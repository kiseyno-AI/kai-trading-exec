from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import UTC, datetime
from decimal import Decimal
import threading

import pytest

from app.broker.base import BrokerConfig
from app.core.config import Settings
from app.services import monitoring_price_source as price_source_module
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


def test_completed_timeout_callback_cannot_unlock_a_later_quote_batch(monkeypatch) -> None:
    old_batch_released = threading.Event()
    next_quote_entered = threading.Event()
    finish_next_quote = threading.Event()
    quote_attempts = 0

    class BlockingSecondQuoteAdapter(LiveMarketContextAdapter):
        def get_current_price(self, access_token, *, symbol, exchange="KRX"):
            nonlocal quote_attempts
            quote_attempts += 1
            if quote_attempts == 2:
                next_quote_entered.set()
                assert finish_next_quote.wait(timeout=5)
            return super().get_current_price(access_token, symbol=symbol, exchange=exchange)

    observed_at = datetime(2026, 9, 23, 1, 5, tzinfo=UTC)
    adapter = BlockingSecondQuoteAdapter(
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
    source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: adapter,
    )
    original_lock = source._quote_batch_lock

    class YieldAfterFirstRelease:
        released_once = False

        def acquire(self, **kwargs):
            return original_lock.acquire(**kwargs)

        def release(self):
            original_lock.release()
            if not self.released_once:
                self.released_once = True
                old_batch_released.set()
                assert next_quote_entered.wait(timeout=5)

    monkeypatch.setattr(source, "_quote_batch_lock", YieldAfterFirstRelease())
    original_as_completed = price_source_module.as_completed
    wait_calls = 0

    def expire_first_wait(futures, *, timeout):
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            # The quote finishes as the first batch's wait expires, before its
            # cleanup callbacks are installed. Future invokes such callbacks
            # synchronously; cleanup also performs an explicit final check.
            for future in futures:
                future.result(timeout=5)
            raise FuturesTimeoutError()
        return original_as_completed(futures, timeout=timeout)

    monkeypatch.setattr(price_source_module, "as_completed", expire_first_wait)
    request = dict(symbols=["005930"], observed_at=observed_at, settings=settings)
    callers = ThreadPoolExecutor(max_workers=2)
    second = None
    try:
        first = callers.submit(source.get_prices, **request)
        assert old_batch_released.wait(timeout=5)
        second = callers.submit(source.get_prices, **request)
        assert next_quote_entered.wait(timeout=5)
        assert first.result(timeout=5) == []

        assert source.get_prices(**request) == []
        assert source.get_last_diagnostics()["quote_batch_in_flight"] is True
        assert quote_attempts == 2
    finally:
        finish_next_quote.set()
        callers.shutdown(wait=True)
        source.close()
        engine.dispose()

    assert second is not None
    assert [price.symbol for price in second.result()] == ["005930"]
