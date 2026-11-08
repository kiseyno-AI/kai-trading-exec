from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.broker.base import BrokerConfig
from app.core.config import Settings
from app.services.monitoring_price_source import KisCurrentQuoteMonitoringPriceSource
from test_broker_auth import KIS_PAPER_API_BASE_URL, build_test_app, build_token_grant
from test_monitoring_price_coverage import LiveMarketContextAdapter


@pytest.mark.parametrize("factory_fails", [False, True])
def test_quote_source_recovers_after_adapter_construction_failure(factory_fails) -> None:
    observed_at = datetime(2026, 9, 23, 1, 5, tzinfo=UTC)
    adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"005930": Decimal("70000")},
        current_price_observed_at=observed_at,
    )
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    construction_error = RuntimeError("adapter construction failed")
    calls = 0

    def build_adapter(_settings):
        nonlocal calls
        calls += 1
        if factory_fails and calls == 1:
            raise construction_error
        return adapter

    source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=build_adapter,
    )
    request = dict(symbols=["005930"], observed_at=observed_at, settings=settings)
    try:
        if factory_fails:
            with pytest.raises(RuntimeError) as error:
                source.get_prices(**request)
            assert error.value is construction_error
        else:
            assert [price.symbol for price in source.get_prices(**request)] == ["005930"]

        prices = source.get_prices(**request)
        assert [(price.symbol, price.price) for price in prices] == [
            ("005930", Decimal("70000"))
        ]
        assert source.get_last_diagnostics()["quote_failure_count"] == 0
        assert len(adapter.current_price_calls) == (1 if factory_fails else 2)
    finally:
        source.close()
        engine.dispose()
