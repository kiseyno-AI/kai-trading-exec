from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings, load_settings
from app.services.monitoring import _quote_stale_reason


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
