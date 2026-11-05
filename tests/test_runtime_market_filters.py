from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.models.runtime import RuntimeControlState
from app.services.monitoring import (
    _placement_preserves_current_plan_status,
    _watch_buy_approval_reason_is_transient,
)
from app.services.execution_semantics import resolve_execution_market_scope
from app.services.runtime_controls import (
    automated_buy_submission_block_reason,
    is_transient_market_buy_filter_reason,
    market_scoped_automated_buy_submission_block_reason,
)


def _state(*, context: dict[str, object] | None = None) -> RuntimeControlState:
    return RuntimeControlState(
        id="global",
        mode="live",
        paused=False,
        kill_switch_enabled=False,
        hourly_risk_off_active=False,
        hourly_risk_off_epoch=0,
        hourly_risk_off_context=context,
    )


def test_buy_submission_requires_a_fresh_current_krx_market_observation() -> None:
    settings = Settings(
        hourly_plan_judgment_max_observation_age_seconds=420,
        hourly_plan_judgment_max_future_skew_seconds=5,
    )
    now = datetime(2026, 11, 5, 0, 1, tzinfo=UTC)

    assert market_scoped_automated_buy_submission_block_reason(
        _state(),
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) == "hourly_market_observation_missing"
    assert market_scoped_automated_buy_submission_block_reason(
        _state(
            context={
                "last_market_observation_market_date": "2026-11-04",
                "last_market_observation_received_at": (now - timedelta(seconds=10)).isoformat(),
                "last_market_observation_source_observed_at": (now - timedelta(seconds=10)).isoformat(),
            }
        ),
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) == "hourly_market_observation_stale"
    assert market_scoped_automated_buy_submission_block_reason(
        _state(
            context={
                "last_market_observation_market_date": "2026-11-05",
                "last_market_observation_received_at": (now - timedelta(seconds=30)).isoformat(),
                "last_market_observation_source_observed_at": (now - timedelta(seconds=31)).isoformat(),
            }
        ),
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) is None


def test_krx_market_freshness_does_not_block_us_or_crypto_routes() -> None:
    settings = Settings()
    state = _state()
    now = datetime(2026, 11, 5, 0, 1, tzinfo=UTC)

    assert automated_buy_submission_block_reason(state) is None
    assert market_scoped_automated_buy_submission_block_reason(
        state,
        settings=settings,
        provider="alpaca",
        asset_class="us_equity",
        exchange="NASD",
        now=now,
    ) is None

    state.hourly_risk_off_active = True
    for provider, asset_class, exchange, symbol in (
        ("alpaca", "us_equity", "NASD", "AAPL"),
        ("upbit", "crypto", "UPBIT", "KRW-BTC"),
        ("kis", "us_equity", "NASD", "AAPL"),
    ):
        assert market_scoped_automated_buy_submission_block_reason(
            state,
            settings=settings,
            provider=provider,
            asset_class=asset_class,
            exchange=exchange,
            symbol=symbol,
            now=now,
        ) is None
    assert market_scoped_automated_buy_submission_block_reason(
        state,
        settings=settings,
        provider="upbit",
        asset_class="crypto",
        exchange="UPBIT",
        now=now,
    ) is None


def test_plan_scope_resolver_does_not_confuse_kis_venue_with_exchange() -> None:
    assert resolve_execution_market_scope(
        provider="kis",
        asset_class="stock",
        symbol="0017J0",
        venue="kis",
        risk_context={},
    ) == ("kis", "stock", "KRX")
    assert resolve_execution_market_scope(
        provider="kis",
        asset_class="us_equity",
        symbol="AAPL",
        venue="kis",
        risk_context={},
    ) == ("kis", "us_equity", "US")


def test_plan_scope_resolver_fails_closed_to_krx_for_legacy_kis_short_codes() -> None:
    assert resolve_execution_market_scope(
        provider="kis",
        asset_class=None,
        symbol="0017J0",
        venue="kis",
        risk_context={},
    ) == ("kis", "stock", "KRX")
    assert resolve_execution_market_scope(
        provider="kis",
        asset_class="",
        symbol="005930",
        venue="kis",
        risk_context={},
    ) == ("kis", "stock", "KRX")
    assert resolve_execution_market_scope(
        provider="kis",
        asset_class="us_equity",
        symbol="AAPL",
        venue="kis",
        risk_context={},
    ) == ("kis", "us_equity", "US")


def test_pause_kill_and_active_risk_off_remain_higher_priority_than_freshness() -> None:
    settings = Settings()
    now = datetime(2026, 11, 5, 0, 1, tzinfo=UTC)
    state = _state()
    state.hourly_risk_off_active = True
    assert market_scoped_automated_buy_submission_block_reason(
        state,
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) == "hourly_plan_risk_off"
    state.paused = True
    assert market_scoped_automated_buy_submission_block_reason(
        state,
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) == "paused"
    state.kill_switch_enabled = True
    assert market_scoped_automated_buy_submission_block_reason(
        state,
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        now=now,
    ) == "kill_switch"


def test_dynamic_market_and_core_authority_reasons_are_transient_buy_fences() -> None:
    for reason in (
        "paused",
        "kill_switch",
        "hourly_plan_risk_off",
        "hourly_market_observation_missing",
        "hourly_market_observation_stale",
        "core_buy_authority_missing",
        "core_buy_authority_projection_pending",
        "core_buy_authority_blocked",
        "core_buy_authority_unavailable",
        "core_buy_authority_stale",
    ):
        assert is_transient_market_buy_filter_reason(reason)
    assert not is_transient_market_buy_filter_reason("execution_plan_item_state_changed")


def test_live_regime_downsize_reasons_remain_retryable_watch_blocks() -> None:
    for reason in (
        "market_breadth_incomplete_caution",
        "market_breadth_caution",
        "market_breadth_below_normal",
        "market_breadth_missing_caution",
        "market_breadth_missing_risk_off",
        "sector_weakness_or_crowding_downsize",
    ):
        assert _watch_buy_approval_reason_is_transient(reason)


def test_permanent_broker_rejection_status_is_not_overwritten_by_monitoring() -> None:
    assert _placement_preserves_current_plan_status(
        {
            "block_reason": "permanent_contract_rejection",
            "permanent_rejection": True,
        }
    )
