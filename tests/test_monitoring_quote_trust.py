from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings, load_settings
from app.models.runtime import ExecutionPlanItem
from app.services.monitoring import (
    _live_automated_quote_trust_reason,
    _live_watch_buy_veto_applies,
    _post_agent_market_quality_reason,
    _quote_stale_reason,
)


CYCLE_AT = datetime(2026, 9, 6, 14, 0, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_max_quote_age_seconds=30,
        monitoring_max_quote_future_skew_seconds=5,
    )


def test_price_trigger_freshness_rejects_missing_or_naive_timestamp() -> None:
    settings = _settings()

    missing = _quote_stale_reason(
        settings=settings,
        price=MonitoringPriceRequest(symbol="005930", price=Decimal("71000")),
        cycle_at=CYCLE_AT,
    )
    naive = _quote_stale_reason(
        settings=settings,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT.replace(tzinfo=None),
        ),
        cycle_at=CYCLE_AT,
    )

    assert missing == "quote timestamp is missing; freshness cannot be verified"
    assert naive == "quote timestamp has no timezone; freshness cannot be verified"


def test_non_live_quote_without_timestamp_remains_backward_compatible() -> None:
    settings = _settings().model_copy(update={"runtime_mode": "paper"})

    reason = _quote_stale_reason(
        settings=settings,
        price=MonitoringPriceRequest(symbol="005930", price=Decimal("71000")),
        cycle_at=CYCLE_AT,
    )

    assert reason is None


def test_price_trigger_freshness_rejects_excessive_future_clock_skew() -> None:
    reason = _quote_stale_reason(
        settings=_settings(),
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT + timedelta(seconds=6),
        ),
        cycle_at=CYCLE_AT,
    )

    assert reason is not None
    assert "timestamp is in the future" in reason
    assert "allowed clock skew 5s" in reason


def test_price_trigger_freshness_accepts_bounded_skew_and_recent_quote() -> None:
    settings = _settings()

    future = _quote_stale_reason(
        settings=settings,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT + timedelta(seconds=5),
        ),
        cycle_at=CYCLE_AT,
    )
    recent = _quote_stale_reason(
        settings=settings,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT - timedelta(seconds=30),
        ),
        cycle_at=CYCLE_AT,
    )

    assert future is None
    assert recent is None


def test_quote_future_skew_setting_loads_from_environment() -> None:
    settings = load_settings(
        {"KAI_EXEC_MONITORING_MAX_QUOTE_FUTURE_SKEW_SECONDS": "11"}
    )

    assert settings.monitoring_max_quote_future_skew_seconds == 11


def test_live_kis_watch_buy_requires_trusted_exchange_timestamp_and_ask() -> None:
    settings = _settings().model_copy(update={"automation_enabled": True})
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
    )

    untrusted = _live_automated_quote_trust_reason(
        settings=settings,
        item=item,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT,
            freshness_source="transport_received_at_untrusted",
            ask=Decimal("71010"),
        ),
    )
    missing_ask = _live_automated_quote_trust_reason(
        settings=settings,
        item=item,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            observed_at=CYCLE_AT,
            freshness_source="kis_exchange_orderbook_time",
        ),
    )
    trusted = _live_automated_quote_trust_reason(
        settings=settings,
        item=item,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71010"),
            observed_at=CYCLE_AT,
            freshness_source="kis_exchange_orderbook_time",
            ask=Decimal("71010"),
            provenance={
                "field_sources": {
                    "ask": {
                        "freshness_source": "kis_exchange_orderbook_time",
                        "observed_at": CYCLE_AT.isoformat(),
                    }
                }
            },
        ),
    )

    assert untrusted == "live automated KIS buy quote lacks trusted exchange-time provenance"
    assert missing_ask == "live automated KIS buy quote lacks a positive executable ask"
    assert trusted is None


def test_live_watch_buy_veto_follows_runtime_mode_not_static_automation_default() -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
    )

    assert _live_watch_buy_veto_applies(
        settings=_settings().model_copy(update={"automation_enabled": False}),
        item=item,
    ) is True


@pytest.mark.parametrize("route_exchange", ["KRX", "NXT", "SOR", "KOSPI", "KOSDAQ"])
def test_live_watch_buy_veto_covers_every_domestic_order_route(
    route_exchange: str,
) -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue=route_exchange,
        asset_class="stock",
        risk_context={"exchange": route_exchange},
    )

    assert _live_watch_buy_veto_applies(
        settings=_settings(),
        item=item,
    ) is True


def test_live_watch_buy_veto_uses_execution_exchange_before_broker_venue() -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )

    assert _live_watch_buy_veto_applies(
        settings=_settings(),
        item=item,
    ) is True


def test_live_quote_trust_gate_blocks_untrusted_protective_sell_trigger() -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        provider="kis",
    )

    reason = _live_automated_quote_trust_reason(
        settings=_settings().model_copy(update={"automation_enabled": True}),
        item=item,
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("69000"),
            observed_at=CYCLE_AT,
            freshness_source="transport_received_at_untrusted",
        ),
    )

    assert reason == "live automated KIS sell quote lacks trusted exchange-time provenance"


def test_post_agent_sell_requires_trusted_bid_field_provenance() -> None:
    reason = _post_agent_market_quality_reason(
        settings=_settings(),
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("69000"),
            observed_at=CYCLE_AT,
            freshness_source="kis_exchange_orderbook_time",
            bid=Decimal("68990"),
            ask=Decimal("69010"),
            provenance={
                "orderbook": {
                    "freshness_source": "kis_exchange_orderbook_time",
                    "observed_at": CYCLE_AT.isoformat(),
                },
                "field_sources": {
                    "bid": {
                        "freshness_source": "transport_received_at_untrusted",
                        "observed_at": CYCLE_AT.isoformat(),
                    }
                },
            },
        ),
        evaluated_at=CYCLE_AT,
        side="sell",
    )

    assert reason == "fresh broker bid lacks trusted field-level exchange-time provenance"
