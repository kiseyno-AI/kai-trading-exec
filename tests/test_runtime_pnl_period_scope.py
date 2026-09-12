from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
)
from app.services import runtime as runtime_service
from app.services.execution_account_route import bind_kis_account_route
from app.services.position_identity import build_position_identity, execution_scope_payload


def _runtime_scope(
    *,
    provider: str,
    exchanges: tuple[str, ...],
) -> runtime_service.RuntimeExecutionScope:
    return runtime_service.RuntimeExecutionScope(
        provider=provider,
        exchanges=exchanges,
        account_scope=f"{provider}-pnl-test-scope",
        status="available",
        source="test",
        reason=None,
    )


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "sqlite:///:memory:",
        "runtime_mode": "paper",
        "execution_position_account_scope": "runtime-pnl-test-account",
        "account_drawdown_recovery_scope_secret": (
            "runtime-pnl-scope-secret-at-least-32-bytes"
        ),
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    (
        "provider",
        "exchanges",
        "timezone_name",
        "day_start",
        "next_day_start",
    ),
    (
        (
            "kis",
            ("KRX",),
            "Asia/Seoul",
            datetime(2026, 3, 8, 15, 0, tzinfo=UTC),
            datetime(2026, 3, 9, 15, 0, tzinfo=UTC),
        ),
        (
            "kis",
            ("NASD", "NYSE", "AMEX"),
            "America/New_York",
            datetime(2026, 3, 8, 5, 0, tzinfo=UTC),
            datetime(2026, 3, 9, 4, 0, tzinfo=UTC),
        ),
        (
            "alpaca",
            ("ALPACA", "US"),
            "America/New_York",
            datetime(2026, 3, 8, 5, 0, tzinfo=UTC),
            datetime(2026, 3, 9, 4, 0, tzinfo=UTC),
        ),
        (
            "upbit",
            ("UPBIT",),
            "Asia/Seoul",
            datetime(2026, 3, 8, 15, 0, tzinfo=UTC),
            datetime(2026, 3, 9, 15, 0, tzinfo=UTC),
        ),
    ),
    ids=("kis_krx", "kis_us", "alpaca", "upbit"),
)
def test_runtime_pnl_calendar_uses_exchange_timezone_and_dst_aware_utc_bounds(
    provider: str,
    exchanges: tuple[str, ...],
    timezone_name: str,
    day_start: datetime,
    next_day_start: datetime,
) -> None:
    now = datetime(2026, 3, 8, 16, 0, tzinfo=UTC)
    settings = _settings()
    runtime_scope = _runtime_scope(provider=provider, exchanges=exchanges)

    timezone = runtime_service._runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )

    assert timezone.key == timezone_name
    assert runtime_service._current_pnl_day_utc_bounds(
        now,
        timezone=timezone,
    ) == (day_start, next_day_start)

    daily = runtime_service._default_daily_pnl_payload(
        settings=settings,
        now=now,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    weekly = runtime_service._default_weekly_pnl_payload(
        settings=settings,
        now=now,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    monthly = runtime_service._default_monthly_pnl_payload(
        settings=settings,
        now=now,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    assert daily["timezone"] == timezone_name
    assert weekly["timezone"] == timezone_name
    assert monthly["timezone"] == timezone_name


def test_new_york_week_and_month_bounds_span_dst_without_fixed_utc_offset() -> None:
    now = datetime(2026, 3, 8, 16, 0, tzinfo=UTC)
    timezone = runtime_service.US_MARKET_TIMEZONE

    assert runtime_service._current_pnl_week_utc_bounds(
        now,
        timezone=timezone,
    ) == (
        datetime(2026, 3, 2, 5, 0, tzinfo=UTC),
        datetime(2026, 3, 9, 4, 0, tzinfo=UTC),
    )
    assert runtime_service._current_pnl_month_utc_bounds(
        now,
        timezone=timezone,
    ) == (
        datetime(2026, 3, 1, 5, 0, tzinfo=UTC),
        datetime(2026, 4, 1, 4, 0, tzinfo=UTC),
    )


def test_kis_dedicated_us_runtime_selects_new_york_for_mixed_provider_scope() -> None:
    mixed_scope = _runtime_scope(
        provider="kis",
        exchanges=("KRX", "AMEX", "NASD", "NYSE"),
    )

    assert runtime_service._runtime_pnl_timezone(
        settings=_settings(kis_us_trading_enabled=True),
        provider="kis",
        runtime_scope=mixed_scope,
    ).key == "America/New_York"
    assert runtime_service._runtime_pnl_timezone(
        settings=_settings(kis_us_trading_enabled=False),
        provider="kis",
        runtime_scope=mixed_scope,
    ).key == "Asia/Seoul"


def test_kis_us_filled_today_uses_new_york_calendar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(kis_us_trading_enabled=True)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 3, 9, 14, 30, tzinfo=UTC)
    original_bounds = runtime_service._current_pnl_day_utc_bounds

    def fixed_bounds(
        _now: datetime | None = None,
        *,
        timezone: ZoneInfo,
    ) -> tuple[datetime, datetime]:
        return original_bounds(now, timezone=timezone)

    monkeypatch.setattr(runtime_service, "_current_pnl_day_utc_bounds", fixed_bounds)
    previous_new_york_day = _order(
        settings=settings,
        key="kis-us-previous-day",
        provider="kis",
        exchange="NASD",
        symbol="PREVIOUS-NY-DAY",
        side="buy",
    )
    current_new_york_day = _order(
        settings=settings,
        key="kis-us-current-day",
        provider="kis",
        exchange="NASD",
        symbol="CURRENT-NY-DAY",
        side="buy",
    )
    db.add_all((previous_new_york_day, current_new_york_day))
    db.flush()
    db.add_all(
        (
            _fill(
                order=previous_new_york_day,
                key="kis-us-previous-day-fill",
                price="100",
                filled_at=datetime(2026, 3, 9, 2, 0, tzinfo=UTC),
            ),
            _fill(
                order=current_new_york_day,
                key="kis-us-current-day-fill",
                price="100",
                filled_at=datetime(2026, 3, 9, 5, 0, tzinfo=UTC),
            ),
        )
    )
    db.commit()

    summary = runtime_service.runtime_filled_today_summary(
        settings=settings,
        db=db,
    )
    symbols = runtime_service.runtime_filled_today_symbols(
        settings=settings,
        db=db,
    )
    symbol_summary = runtime_service.runtime_symbol_summary(
        settings=settings,
        db=db,
    )

    assert summary["by_side"]["buy"] == ["CURRENT-NY-DAY"]
    assert symbols == ["CURRENT-NY-DAY"]
    assert symbol_summary["filled_today"] == ["CURRENT-NY-DAY"]

    db.close()
    engine.dispose()


def test_kis_us_veto_and_late_entry_today_use_new_york_calendar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(kis_us_trading_enabled=True)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 3, 9, 14, 30, tzinfo=UTC)
    original_bounds = runtime_service._current_pnl_day_utc_bounds

    def fixed_bounds(
        _now: datetime | None = None,
        *,
        timezone: ZoneInfo,
    ) -> tuple[datetime, datetime]:
        return original_bounds(now, timezone=timezone)

    monkeypatch.setattr(runtime_service, "_current_pnl_day_utc_bounds", fixed_bounds)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "NASD"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    previous_item = ExecutionPlanItem(
        symbol="PREVIOUS-NY-DAY",
        side="buy",
        intent="watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="NASD",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    current_item = ExecutionPlanItem(
        symbol="CURRENT-NY-DAY",
        side="buy",
        intent="watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="NASD",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    db.add(
        ExecutionPlan(
            external_plan_id="kis-us-today-audit-plan",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            items=[previous_item, current_item],
        )
    )
    db.flush()
    previous_at = datetime(2026, 3, 9, 2, 0, tzinfo=UTC)
    current_at = datetime(2026, 3, 9, 5, 0, tzinfo=UTC)
    db.add_all(
        (
            ExecutionPlanItemAudit(
                execution_plan_item_id=previous_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=previous_at,
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=current_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=current_at,
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=previous_item.id,
                event_type=runtime_service.LATE_ENTRY_GATE_EVENT_TYPE,
                source="test",
                occurred_at=previous_at,
                summary={"mode": "shadow"},
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=current_item.id,
                event_type=runtime_service.LATE_ENTRY_GATE_EVENT_TYPE,
                source="test",
                occurred_at=current_at,
                summary={"mode": "block"},
            ),
        )
    )
    db.commit()
    runtime_scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="kis",
    )

    vetoes = runtime_service.runtime_watch_buy_veto_summary(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )
    late_entry = runtime_service.runtime_late_entry_summary(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )

    assert vetoes == {"today_count": 1, "today_symbols": ["CURRENT-NY-DAY"]}
    assert late_entry["event_count"] == 1
    assert late_entry["shadow_count"] == 0
    assert late_entry["block_count"] == 1
    assert late_entry["symbols"] == ["CURRENT-NY-DAY"]

    db.close()
    engine.dispose()


def test_kis_us_monitored_symbols_resolves_provider_local_today(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(kis_us_trading_enabled=True)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    observed_timezones: list[str] = []

    def observed_bounds(
        _now: datetime | None = None,
        *,
        timezone: ZoneInfo,
    ) -> tuple[datetime, datetime]:
        observed_timezones.append(timezone.key)
        return (
            datetime(2026, 3, 9, 4, 0, tzinfo=UTC),
            datetime(2026, 3, 10, 4, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        runtime_service,
        "_current_pnl_day_utc_bounds",
        observed_bounds,
    )
    runtime_scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="kis",
    )

    assert runtime_service.runtime_monitored_symbols(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    ) == []
    assert observed_timezones == ["America/New_York"]

    db.close()
    engine.dispose()


def _order(
    *,
    settings: Settings | None,
    key: str,
    provider: str,
    exchange: str,
    symbol: str,
    side: str,
) -> BrokerOrder:
    request_metadata = None
    if settings is not None:
        identity = build_position_identity(
            settings=settings,
            provider=provider,
            exchange=exchange,
        )
        request_metadata = {
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue=exchange,
                asset_class="kr_equity" if exchange == "KRX" else "us_equity",
            )
        }
    return BrokerOrder(
        broker_name=provider,
        mode="paper",
        idempotency_key=key,
        broker_order_id=f"broker-{key}",
        symbol=symbol,
        side=side,
        status="filled",
        quantity=1,
        venue=exchange,
        asset_class="kr_equity" if exchange == "KRX" else "us_equity",
        request_metadata=request_metadata,
    )


def _fill(
    *,
    order: BrokerOrder,
    key: str,
    price: str,
    filled_at: datetime,
) -> ExecutionFill:
    return ExecutionFill(
        broker_order=order,
        broker_fill_id=key,
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        price=Decimal(price),
        settlement_currency="KRW",
        transaction_cost=Decimal("0"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test_fixture",
        filled_at=filled_at,
    )


def test_foreign_provider_legacy_fill_does_not_poison_active_provider_pnl() -> None:
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    active_buy = _order(
        settings=settings,
        key="active-kis-buy",
        provider="kis",
        exchange="KRX",
        symbol="ACTIVE-KIS",
        side="buy",
    )
    active_sell = _order(
        settings=settings,
        key="active-kis-sell",
        provider="kis",
        exchange="KRX",
        symbol="ACTIVE-KIS",
        side="sell",
    )
    foreign_legacy = _order(
        settings=None,
        key="legacy-alpaca-fill",
        provider="alpaca",
        exchange="US",
        symbol="FOREIGN-ALPACA",
        side="sell",
    )
    db.add_all((active_buy, active_sell, foreign_legacy))
    db.flush()
    db.add_all(
        (
            _fill(
                order=active_buy,
                key="active-kis-buy-fill",
                price="100",
                filled_at=datetime(2026, 9, 10, 1, 0, tzinfo=UTC),
            ),
            _fill(
                order=active_sell,
                key="active-kis-sell-fill",
                price="120",
                filled_at=datetime(2026, 9, 10, 2, 0, tzinfo=UTC),
            ),
            _fill(
                order=foreign_legacy,
                key="legacy-alpaca-unscoped-fill",
                price="999",
                filled_at=datetime(2026, 9, 10, 2, 30, tzinfo=UTC),
            ),
        )
    )
    db.commit()

    payloads = (
        runtime_service.daily_pnl_payload(settings=settings, db=db, now=now),
        runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now),
        runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now),
    )

    for payload in payloads:
        assert payload["calculation_status"] == "computed"
        assert Decimal(str(payload["gross_realized_pnl_krw"])) == Decimal("20.0000")
        assert payload["timezone"] == "Asia/Seoul"

    db.close()
    engine.dispose()


def test_active_provider_legacy_unscoped_fill_still_fails_closed() -> None:
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    active_legacy = _order(
        settings=None,
        key="legacy-kis-fill",
        provider="kis",
        exchange="KRX",
        symbol="ACTIVE-KIS-UNSCOPED",
        side="sell",
    )
    db.add(active_legacy)
    db.flush()
    db.add(
        _fill(
            order=active_legacy,
            key="legacy-kis-unscoped-fill",
            price="100",
            filled_at=datetime(2026, 9, 10, 2, 30, tzinfo=UTC),
        )
    )
    db.commit()

    payload = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)

    assert payload["calculation_status"] == "not_computed"
    assert payload["calculation_reason"] == "indeterminate_execution_fill_scope"
    assert payload["timezone"] == "Asia/Seoul"

    db.close()
    engine.dispose()


def test_unknown_provider_legacy_unscoped_fill_still_fails_closed() -> None:
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    unknown_legacy = _order(
        settings=None,
        key="legacy-unknown-fill",
        provider="unrecognized-broker",
        exchange="KRX",
        symbol="UNKNOWN-UNSCOPED",
        side="sell",
    )
    db.add(unknown_legacy)
    db.flush()
    db.add(
        _fill(
            order=unknown_legacy,
            key="legacy-unknown-unscoped-fill",
            price="100",
            filled_at=datetime(2026, 9, 10, 2, 30, tzinfo=UTC),
        )
    )
    db.commit()

    payload = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)

    assert payload["calculation_status"] == "not_computed"
    assert payload["calculation_reason"] == "indeterminate_execution_fill_scope"
    assert payload["timezone"] == "Asia/Seoul"

    db.close()
    engine.dispose()


def test_unknown_broker_with_active_provider_scope_still_fails_closed() -> None:
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    conflicting_order = _order(
        settings=settings,
        key="unknown-broker-active-scope",
        provider="kis",
        exchange="KRX",
        symbol="UNKNOWN-BROKER-ACTIVE-SCOPE",
        side="sell",
    )
    conflicting_order.broker_name = "unrecognized-broker"
    db.add(conflicting_order)
    db.flush()
    db.add(
        _fill(
            order=conflicting_order,
            key="unknown-broker-active-scope-fill",
            price="100",
            filled_at=datetime(2026, 9, 10, 2, 30, tzinfo=UTC),
        )
    )
    db.commit()

    payload = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)

    assert payload["calculation_status"] == "not_computed"
    assert payload["calculation_reason"] == "indeterminate_execution_fill_scope"

    db.close()
    engine.dispose()
