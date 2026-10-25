import asyncio
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services import (
    autonomous_runtime,
    monitoring as monitoring_service,
    runtime as runtime_service,
)
from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.autonomous_runtime import (
    AutonomousRuntimeCoordinator,
    CachedMonitoringItem,
    CachedMonitoringPosition,
    MonitoringRuntimeSnapshot,
    _is_krx_symbol,
    _session_active_monitoring_symbols,
    _select_monitoring_symbol_batch,
)
from app.services.monitoring import list_autonomous_monitoring_symbols
from app.services.position_identity import (
    build_position_identity,
    payload_with_position_identity,
)
from app.services.execution_account_route import bind_kis_account_route
from app.services.trigger_time_exec_agent import _run_exec_agent_command
from app.services.trade_outcome_feedback import REPORT_VERSION


async def _wait_for_thread_event(event: threading.Event) -> None:
    async with asyncio.timeout(1):
        while not event.is_set():
            await asyncio.sleep(0.001)


def _core_monitoring_handoff(
    desired_symbols: tuple[str, ...] = (),
    observation_only_symbols: tuple[str, ...] = (),
) -> monitoring_service.CoreMonitoringSymbolHandoff:
    return monitoring_service.CoreMonitoringSymbolHandoff(
        desired_symbols=desired_symbols,
        observation_only_symbols=observation_only_symbols,
    )


def test_price_source_internal_type_error_is_not_retried_with_a_different_signature() -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    observed_at = datetime(2026, 9, 11, 13, 0, tzinfo=UTC)
    settings = Settings(database_url="sqlite:///:memory:")

    def source(*args: object, **kwargs: object) -> object:
        calls.append((args, kwargs))
        raise TypeError("provider failed after starting the request")

    with pytest.raises(TypeError, match="provider failed after starting the request"):
        autonomous_runtime._call_price_source(
            source=source,
            symbols=["005930"],
            observed_at=observed_at,
            settings=settings,
        )

    assert calls == [
        (
            (),
            {
                "symbols": ["005930"],
                "observed_at": observed_at,
                "settings": settings,
            },
        )
    ]


@pytest.mark.parametrize("exchange", ["NXT", "SOR", "KOSPI", "KOSDAQ"])
def test_cached_kis_route_accepts_domestic_order_venues(exchange: str) -> None:
    settings = Settings(
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "cached-domestic-route-secret-at-least-32-bytes"
        ),
    )
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": exchange},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    assert autonomous_runtime._cached_item_matches_active_kis_route(
        settings=settings,
        symbol="005930",
        provider="kis",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    assert risk_context is not None
    assert risk_context["exchange"] == exchange


@pytest.mark.parametrize("exchange", ["NXT", "SOR", "KOSPI", "KOSDAQ"])
def test_cached_domestic_order_venue_matches_canonical_krx_position(
    exchange: str,
) -> None:
    item = CachedMonitoringItem(
        id=f"{exchange.lower()}-stop",
        symbol="005930",
        intent="stop_sell",
        status="stop_sell_monitoring",
        trigger_price=Decimal("50000"),
        provider="kis",
        exchange=exchange,
    )
    position = CachedMonitoringPosition(
        symbol="005930",
        quantity=1,
        provider="kis",
        exchange="KRX",
    )

    assert autonomous_runtime._cached_item_matches_position_route(
        item=item,
        position=position,
    )
    assert item.exchange == exchange


def test_cached_krx_max_holding_due_date_uses_kst_boundary() -> None:
    item = CachedMonitoringItem(
        id="krx-max-holding",
        symbol="005930",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        trigger_price=None,
        close_exit_date=date(2026, 10, 7),
        provider="kis",
        exchange="KRX",
    )

    assert autonomous_runtime._cached_monitoring_item_triggered(
        item=item,
        observed_price=None,
        triggered_at=datetime(2026, 10, 6, 14, 59, tzinfo=UTC),
    ) is False
    assert autonomous_runtime._cached_monitoring_item_triggered(
        item=item,
        observed_price=None,
        triggered_at=datetime(2026, 10, 6, 15, 0, tzinfo=UTC),
    ) is True


@pytest.mark.parametrize("arity", [1, 3])
def test_price_source_legacy_positional_signatures_are_selected_before_invocation(
    arity: int,
) -> None:
    observed_at = datetime(2026, 9, 11, 13, 0, tzinfo=UTC)
    settings = Settings(database_url="sqlite:///:memory:")
    calls: list[tuple[object, ...]] = []

    if arity == 1:

        def source(symbols: list[str]) -> object:
            calls.append((symbols,))
            return []

    else:

        def source(  # type: ignore[no-redef]
            symbols: list[str],
            requested_at: datetime,
            source_settings: Settings,
        ) -> object:
            calls.append((symbols, requested_at, source_settings))
            return []

    assert autonomous_runtime._call_price_source(
        source=source,
        symbols=["005930"],
        observed_at=observed_at,
        settings=settings,
    ) == []
    assert len(calls) == 1


def test_watch_sizing_cycle_refreshes_trade_feedback_before_agent_sizing(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-feedback-runtime.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    calls: list[tuple[str, object]] = []

    def ensure_feedback(  # noqa: ANN001
        *, settings, db, now, provider, account_scope, exchange
    ):
        calls.append(
            (
                "feedback",
                {
                    "provider": provider,
                    "account_scope": account_scope,
                    "exchange": exchange,
                },
            )
        )
        return {
            "status": "generated",
            "version": REPORT_VERSION,
            "generated_at": now.isoformat(),
            "closed_trade_count": 0,
        }

    def run_sizing(**_):
        calls.append(("sizing", None))
        return SimpleNamespace(status_code=200, payload={"idle": True})

    monkeypatch.setattr(autonomous_runtime, "ensure_fresh_trade_outcome_feedback", ensure_feedback)
    monkeypatch.setattr(autonomous_runtime, "run_due_watch_exec_agent_sizing", run_sizing)

    payload = coordinator._watch_exec_agent_sizing_cycle_sync(
        datetime(2026, 6, 12, 0, 1, tzinfo=UTC)
    )

    identity = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    assert calls == [
        (
            "feedback",
            {
                "provider": identity.provider,
                "account_scope": identity.account_scope,
                "exchange": None,
            },
        ),
        ("sizing", None),
    ]
    assert payload["trade_outcome_feedback"]["status"] == "generated"


def test_watch_sizing_cycle_invalidates_monitoring_snapshot_after_price_watch_reopen(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-reopen-runtime.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    invalidations: list[str] = []

    monkeypatch.setattr(
        autonomous_runtime,
        "ensure_fresh_trade_outcome_feedback",
        lambda **_: {"status": "fresh", "version": REPORT_VERSION},
    )
    monkeypatch.setattr(
        autonomous_runtime,
        "run_due_watch_exec_agent_sizing",
        lambda **_: SimpleNamespace(
            status_code=200,
            payload={
                "idle": False,
                "reason": "price_watch_candidates_reopened",
                "price_watch_reopened_item_ids": ["item-1"],
            },
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "invalidate_monitoring_runtime_snapshot",
        lambda *, reason: invalidations.append(reason),
    )

    coordinator._watch_exec_agent_sizing_cycle_sync(
        datetime(2026, 6, 12, 0, 1, tzinfo=UTC)
    )

    assert invalidations == ["watch_exec_agent_sizing_changed_plan_items"]


def test_watch_sizing_cycle_skips_database_feedback_outside_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "watch-sizing-closed-window-secret-at-least-32-bytes"
        ),
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    monkeypatch.setattr(
        autonomous_runtime,
        "ensure_fresh_trade_outcome_feedback",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("closed-window sizing must not access feedback storage")
        ),
    )

    payload = coordinator._watch_exec_agent_sizing_cycle_sync(
        datetime(2026, 9, 22, 18, 1, tzinfo=UTC)
    )

    assert payload["idle"] is True
    assert payload["reason"] == "outside_sizing_window"
    assert payload["trade_outcome_feedback"] == {
        "status": "skipped",
        "version": REPORT_VERSION,
        "reason": "outside_sizing_window",
    }


def test_coordinator_snapshot_stores_exact_canonical_execution_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="autonomous-snapshot-account",
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    coordinator._store_cycle_payload(
        worker_key="monitoring",
        payload={"status_code": 200, "monitored_symbols": ["005930"]},
    )

    identity = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    expected_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": identity.account_scope,
    }
    snapshot = coordinator.snapshot()

    assert snapshot["execution_scope"] == expected_scope
    assert snapshot["monitoring"]["last_cycle"]["execution_scope"] == expected_scope


def test_watch_sizing_feedback_fails_closed_without_stable_live_account_scope(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-feedback-unscoped.db'}",
        runtime_mode="live",
        kis_account_number="12345678",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    def unexpected_feedback(**_):
        raise AssertionError("unscoped live feedback generation must not run")

    monkeypatch.setattr(
        autonomous_runtime,
        "ensure_fresh_trade_outcome_feedback",
        unexpected_feedback,
    )
    monkeypatch.setattr(
        autonomous_runtime,
        "run_due_watch_exec_agent_sizing",
        lambda **_: SimpleNamespace(status_code=200, payload={"idle": True}),
    )

    payload = coordinator._watch_exec_agent_sizing_cycle_sync(
        datetime(2026, 6, 12, 0, 1, tzinfo=UTC)
    )

    assert payload["trade_outcome_feedback"] == {
        "status": "unavailable",
        "version": REPORT_VERSION,
        "reason": "active_kis_account_scope_unavailable",
    }


def test_krx_symbol_filter_excludes_alpaca_symbols() -> None:
    assert _is_krx_symbol("005930") is True
    assert _is_krx_symbol("AFIB") is False
    assert _is_krx_symbol("FRTX/CVR1") is False


def test_monitoring_symbol_batches_keep_all_positions_and_cap_only_rotating_candidates() -> None:
    desired = [
        "030000",
        "001450",
        "002990",
        "003300",
        "003550",
        "003650",
        "005830",
        "006000",
        "006100",
    ]
    cursor = 0
    covered: set[str] = set()

    for _ in range(3):
        batch, cursor = _select_monitoring_symbol_batch(
            desired_symbols=desired,
            priority_symbols={"030000", "001450"},
            batch_size=3,
            cursor=cursor,
        )
        assert batch[:2] == ["030000", "001450"]
        assert len(batch) == 5
        covered.update(batch)

    assert covered == set(desired)


def test_nine_passive_quotes_rotate_across_two_six_symbol_cycles() -> None:
    passive_symbols = [
        "012030",
        "032080",
        "033560",
        "070590",
        "079940",
        "086060",
        "089890",
        "323350",
        "398120",
    ]
    cursor = 0
    covered: set[str] = set()

    for _ in range(2):
        batch, cursor = _select_monitoring_symbol_batch(
            desired_symbols=passive_symbols,
            priority_symbols=set(),
            batch_size=6,
            cursor=cursor,
        )
        assert len(batch) == 6
        covered.update(batch)

    assert covered == set(passive_symbols)


def test_passive_quotes_do_not_displace_held_or_actionable_local_priority() -> None:
    local_held = "005930"
    local_actionable = "000660"
    passive_symbols = [
        "012030",
        "032080",
        "033560",
        "070590",
        "079940",
        "086060",
        "089890",
        "323350",
        "398120",
    ]

    batch, _ = _select_monitoring_symbol_batch(
        desired_symbols=[local_held, local_actionable, *passive_symbols],
        priority_symbols={local_held},
        batch_size=6,
        cursor=0,
    )

    assert batch[:2] == [local_held, local_actionable]
    assert len(batch) == 7


def test_monitoring_cycle_does_not_mix_core_universe_into_active_local_work(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_max_symbols_per_cycle=6,
        monitoring_max_broker_actions_per_cycle=0,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=datetime(2026, 8, 26, 1, 0, tzinfo=UTC),
        symbols=["005930", "AAPL"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    requested: list[str] = []
    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: _core_monitoring_handoff(
            ("100001", "100002", "100003")
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_desired_monitoring_symbols",
        lambda: ["005930", "AAPL"],
    )
    monkeypatch.setattr(coordinator, "_load_local_position_symbols", lambda: [])
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: requested.extend(kwargs["symbols"]) or [],
    )
    monkeypatch.setattr(coordinator, "_refresh_monitoring_snapshot_filled_today", lambda now: None)

    coordinator._monitoring_cycle_sync(datetime(2026, 8, 26, 1, 0, tzinfo=UTC))

    assert requested == ["005930", "AAPL"]


def test_core_desired_krx_symbols_are_passive_quote_work_without_execution_authority(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'passive-core-quotes.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=10,
        monitoring_max_broker_actions_per_cycle=10,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    local_symbols = ["005930", "000660"]
    requested_prices: list[str] = []
    reconciled_symbol_sets: list[set[str]] = []

    monkeypatch.setattr(
        autonomous_runtime,
        "_load_core_monitoring_symbol_handoff",
        lambda **kwargs: monitoring_service.CoreMonitoringSymbolHandoff(
            desired_symbols=(
                "035420",
                "AAPL",
                "12345",
                "１２３４５６",
                "005930",
                "012030",
                "035420",
            ),
            observation_only_symbols=("035420", "012030"),
        ),
    )

    def load_local_symbols(**kwargs) -> list[str]:
        assert kwargs["desired_symbol_priority"] == [
            "035420",
            "AAPL",
            "12345",
            "１２３４５６",
            "005930",
            "012030",
            "035420",
        ]
        return list(local_symbols)

    monkeypatch.setattr(
        autonomous_runtime,
        "list_autonomous_monitoring_symbols",
        load_local_symbols,
    )

    def load_snapshot(*, db, loaded_at, symbols):
        del db
        assert symbols == local_symbols
        return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=list(symbols),
            positions_by_symbol={
                "005930": CachedMonitoringPosition(
                    symbol="005930",
                    quantity=1,
                    provider="kis",
                    exchange="KRX",
                )
            },
            items=[
                CachedMonitoringItem(
                    id="local-actionable",
                    symbol="000660",
                    intent="intraday_watch_buy",
                    status="watch_monitoring_active",
                    trigger_price=Decimal("1"),
                    provider="kis",
                    exchange="KRX",
                )
            ],
            filled_today_symbols=[],
        )

    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols_with_session",
        load_snapshot,
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: (
            requested_prices.extend(kwargs["symbols"])
            or [
                MonitoringPriceRequest(symbol=symbol, price=Decimal("70000"))
                for symbol in kwargs["symbols"]
            ]
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_reconcile_monitoring_orders_if_due",
        lambda **kwargs: (
            reconciled_symbol_sets.append(set(kwargs["symbols"])) or False
        ),
    )
    monkeypatch.setattr(
        autonomous_runtime,
        "run_autonomous_monitoring_cycle",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("passive Core symbols must not enter broker-action evaluation")
        ),
    )

    try:
        snapshot = coordinator._load_monitoring_runtime_snapshot(started_at)
        coordinator._monitoring_runtime_snapshot = snapshot
        payload = coordinator._monitoring_cycle_sync(started_at)
    finally:
        engine.dispose()

    assert snapshot.symbols == ["005930", "000660", "035420", "012030"]
    assert [item.symbol for item in snapshot.items] == ["000660"]
    assert snapshot.passive_quote_symbols == ["035420", "012030"]
    assert requested_prices == ["005930", "000660", "035420", "012030"]
    assert reconciled_symbol_sets == [{"005930", "000660"}]
    assert payload["monitored_symbols"] == ["005930", "000660", "035420", "012030"]
    assert payload["evaluated_item_count"] == 1
    assert payload["triggered_item_count"] == 0
    assert payload["items"] == [
        {
            "execution_plan_item_id": "local-actionable",
            "symbol": "000660",
            "intent": "intraday_watch_buy",
            "status": "watch_monitoring_active",
            "trigger_price": "1",
            "observed_price": "70000",
            "outcome": "waiting",
        }
    ]
    assert payload["submitted_order_count"] == 0


def test_passive_quote_only_cycle_with_missing_price_never_enters_order_evaluation(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'passive-core-no-price.db'}",
        runtime_mode="paper",
        monitoring_max_broker_actions_per_cycle=10,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["012030"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
        passive_quote_symbols=["012030"],
    )
    monkeypatch.setattr(coordinator, "_load_monitoring_prices", lambda **kwargs: [])
    monkeypatch.setattr(
        autonomous_runtime,
        "run_autonomous_monitoring_cycle",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("passive quotes must never enter order evaluation")
        ),
    )

    try:
        payload = coordinator._monitoring_cycle_sync(started_at)
    finally:
        engine.dispose()

    assert payload["monitored_symbols"] == ["012030"]
    assert payload["price_count"] == 0
    assert payload["db_evaluation_symbols"] == []
    assert payload["submitted_order_count"] == 0


def test_triggered_local_item_filters_passive_quotes_from_broker_evaluation(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'passive-trigger-filter.db'}",
        runtime_mode="paper",
        monitoring_max_broker_actions_per_cycle=10,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["000660", "032080"],
        positions_by_symbol={},
        items=[
            CachedMonitoringItem(
                id="local-triggered",
                symbol="000660",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=Decimal("80000"),
                provider="kis",
                exchange="KRX",
            )
        ],
        filled_today_symbols=[],
        passive_quote_symbols=["032080"],
    )
    coordinator._monitoring_runtime_snapshot = snapshot
    broker_evaluations: list[tuple[list[str], list[str]]] = []
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: [
            MonitoringPriceRequest(symbol=symbol, price=Decimal("70000"))
            for symbol in kwargs["symbols"]
        ],
    )
    monkeypatch.setattr(
        coordinator,
        "_reconcile_monitoring_orders_if_due",
        lambda **kwargs: False,
    )

    def evaluate(**kwargs):
        broker_evaluations.append(
            (
                list(kwargs["symbols"]),
                [price.symbol for price in kwargs["prices"]],
            )
        )
        return SimpleNamespace(
            status_code=200,
            payload={
                "triggered_item_count": 1,
                "submitted_order_count": 0,
                "submitted_symbols": [],
                "items": [],
            },
        )

    monkeypatch.setattr(autonomous_runtime, "run_autonomous_monitoring_cycle", evaluate)
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: snapshot,
    )

    try:
        payload = coordinator._monitoring_cycle_sync(started_at)
    finally:
        engine.dispose()

    assert broker_evaluations == [(["000660"], ["000660"])]
    assert payload["monitored_symbols"] == ["000660", "032080"]
    assert payload["broker_action_symbols"] == []


def test_session_active_monitoring_symbols_excludes_us_equities_after_close() -> None:
    now = datetime(2026, 10, 7, 5, 0, tzinfo=UTC)  # 01:00 America/New_York
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["027710", "APTX", "ASPU"],
        positions_by_symbol={
            "027710": CachedMonitoringPosition(
                symbol="027710", quantity=1012, exchange="KRX"
            ),
            "APTX": CachedMonitoringPosition(
                symbol="APTX", quantity=377, exchange="NASD"
            ),
        },
        items=[
            CachedMonitoringItem(
                id="aspu-stop",
                symbol="ASPU",
                intent="stop_sell",
                status="monitoring",
                trigger_price=Decimal("0.70"),
                exchange="NASDAQ",
            )
        ],
        filled_today_symbols=[],
    )

    active, inactive = _session_active_monitoring_symbols(snapshot, now=now)

    assert active == ["027710"]
    assert inactive == ["APTX", "ASPU"]


def test_session_active_monitoring_symbols_excludes_krx_during_us_session() -> None:
    now = datetime(2026, 10, 7, 19, 0, tzinfo=UTC)  # 15:00 America/New_York
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["005930", "AAPL"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930", quantity=1, exchange="KRX"
            ),
            "AAPL": CachedMonitoringPosition(
                symbol="AAPL", quantity=1, exchange="NASD"
            ),
        },
        items=[],
        filled_today_symbols=[],
    )

    active, inactive = _session_active_monitoring_symbols(
        snapshot,
        now=now,
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
    )

    assert active == ["AAPL"]
    assert inactive == ["005930"]


def test_session_active_monitoring_symbols_keeps_markets_independent_when_both_closed() -> None:
    now = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)  # 21:00 Asia/Seoul, 08:00 New York
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["005930", "AAPL"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930", quantity=1, exchange="KRX"
            ),
            "AAPL": CachedMonitoringPosition(
                symbol="AAPL", quantity=1, exchange="NASD"
            ),
        },
        items=[],
        filled_today_symbols=[],
    )

    active, inactive = _session_active_monitoring_symbols(
        snapshot,
        now=now,
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
    )

    assert active == []
    assert inactive == ["005930", "AAPL"]


def test_closed_overseas_market_failure_does_not_block_domestic_readiness() -> None:
    autonomous = {
        "enabled": True,
        "monitoring": {
            "last_cycle": {
                "idle": True,
                "monitored_symbols": [],
                "session_inactive_symbols": ["APTX", "ASPU"],
            },
            "price_source": {
                "configured": True,
                "status": "degraded",
                "reason": "monitoring price source request failed",
                "last_symbol_count": 2,
                "last_price_count": 0,
            },
        },
    }

    assert runtime_service._monitoring_price_source_readiness_reasons(
        autonomous=autonomous,
        symbols={"monitored": ["APTX", "ASPU"]},
    ) == []


def test_monitoring_cycle_does_not_request_us_quotes_outside_regular_session(
    monkeypatch,
) -> None:
    now = datetime(2026, 10, 7, 5, 0, tzinfo=UTC)  # 01:00 America/New_York
    coordinator = AutonomousRuntimeCoordinator(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["APTX"],
        positions_by_symbol={
            "APTX": CachedMonitoringPosition(
                symbol="APTX", quantity=377, exchange="NASD"
            )
        },
        items=[],
        filled_today_symbols=[],
    )
    quote_calls: list[list[str]] = []
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: quote_calls.append(kwargs["symbols"]) or [],
    )

    payload = coordinator._monitoring_cycle_sync(now)

    assert quote_calls == []
    assert payload["idle"] is True
    assert payload["monitored_symbols"] == []
    assert payload["session_active_symbols"] == []
    assert payload["session_inactive_symbols"] == ["APTX"]
    assert payload["market_sessions"]["domestic"]["active"] is True
    assert payload["market_sessions"]["overseas"]["active"] is False


def test_monitoring_cycle_reconciles_closed_session_us_symbols_without_quote_polling(
    monkeypatch,
) -> None:
    now = datetime(2026, 10, 7, 5, 0, tzinfo=UTC)  # 01:00 America/New_York
    coordinator = AutonomousRuntimeCoordinator(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
            monitoring_max_symbols_per_cycle=6,
            monitoring_max_broker_actions_per_cycle=0,
        ),
        session_factory=lambda: None,  # type: ignore[arg-type,return-value]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["005930", "APTX"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930", quantity=1012, exchange="KRX"
            ),
            "APTX": CachedMonitoringPosition(
                symbol="APTX", quantity=377, exchange="NASD"
            ),
        },
        items=[],
        filled_today_symbols=[],
    )
    reconciled_symbols: list[set[str]] = []
    quote_calls: list[list[str]] = []
    monkeypatch.setattr(
        coordinator,
        "_reconcile_and_refresh_monitoring_snapshot_if_due",
        lambda now: True,
    )
    monkeypatch.setattr(
        coordinator,
        "_reconcile_monitoring_orders_if_due",
        lambda **kwargs: reconciled_symbols.append(set(kwargs["symbols"])) or False,
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: quote_calls.append(list(kwargs["symbols"])) or [],
    )

    payload = coordinator._monitoring_cycle_sync(now)

    assert reconciled_symbols == [{"005930", "APTX"}]
    assert quote_calls == [["005930"]]
    assert payload["monitored_symbols"] == ["005930"]


def test_session_active_monitoring_symbols_keeps_only_us_equities_during_us_session() -> None:
    now = datetime(2026, 10, 6, 15, 0, tzinfo=UTC)  # 11:00 America/New_York
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=now,
        symbols=["027710", "APTX"],
        positions_by_symbol={
            "027710": CachedMonitoringPosition(
                symbol="027710", quantity=1012, exchange="KRX"
            ),
            "APTX": CachedMonitoringPosition(
                symbol="APTX", quantity=377, exchange="NASD"
            ),
        },
        items=[],
        filled_today_symbols=[],
    )

    active, inactive = _session_active_monitoring_symbols(snapshot, now=now)

    assert active == ["APTX"]
    assert inactive == ["027710"]


@pytest.mark.parametrize(
    (
        "worker_key",
        "settings_updates",
        "now",
        "expected_mode",
        "expected_delay",
        "expected_next_active",
    ),
    [
        (
            "monitoring",
            {"domestic_closed_market_idle_interval_seconds": 75.0},
            datetime(2026, 9, 22, 11, 1, tzinfo=UTC),
            "domestic_market_closed",
            75.0,
            datetime(2026, 9, 22, 23, 50, tzinfo=UTC),
        ),
        (
            "monitoring",
            {},
            datetime(2026, 9, 23, 1, 0, tzinfo=UTC),
            "market_active",
            1.0,
            None,
        ),
        (
            "monitoring",
            {},
            datetime(2026, 9, 22, 23, 49, 55, tzinfo=UTC),
            "domestic_market_closed",
            5.0,
            datetime(2026, 9, 22, 23, 50, tzinfo=UTC),
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_krx_after_hours_single_price_orders": True,
            },
            datetime(2026, 9, 23, 8, 0, tzinfo=UTC),
            "market_active",
            1.0,
            None,
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_nxt_after_orders": True,
            },
            datetime(2026, 9, 23, 9, 1, tzinfo=UTC),
            "market_active",
            1.0,
            None,
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_nxt_after_orders": True,
                "allow_after_hours_buy_orders": False,
                "allow_after_hours_sell_orders": False,
            },
            datetime(2026, 9, 23, 9, 1, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 23, 23, 50, tzinfo=UTC),
        ),
        (
            "monitoring",
            {"allow_krx_after_hours_single_price_orders": True},
            datetime(2026, 9, 23, 8, 0, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 23, 23, 50, tzinfo=UTC),
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_nxt_pre_orders": True,
            },
            datetime(2026, 9, 22, 22, 59, 55, tzinfo=UTC),
            "domestic_market_closed",
            5.0,
            datetime(2026, 9, 22, 23, 0, tzinfo=UTC),
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_nxt_pre_orders": True,
                "nxt_order_route_preference": "krx_when_regular",
            },
            datetime(2026, 9, 22, 23, 35, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 22, 23, 40, tzinfo=UTC),
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_krx_pre_open_offhours_orders": True,
            },
            datetime(2026, 9, 22, 23, 35, tzinfo=UTC),
            "market_active",
            1.0,
            None,
        ),
        (
            "monitoring",
            {
                "domestic_order_session_routing_enabled": True,
                "allow_krx_after_hours_closing_orders": True,
            },
            datetime(2026, 9, 23, 6, 45, tzinfo=UTC),
            "market_active",
            1.0,
            None,
        ),
        (
            "monitoring",
            {"kis_us_trading_enabled": True},
            datetime(2026, 9, 22, 11, 1, tzinfo=UTC),
            "configured",
            1.0,
            None,
        ),
        (
            "monitoring",
            {"runtime_mode": "live", "runtime_broker_provider": "alpaca"},
            datetime(2026, 9, 22, 11, 1, tzinfo=UTC),
            "configured",
            1.0,
            None,
        ),
        (
            "monitoring",
            {"runtime_mode": "paper"},
            datetime(2026, 9, 22, 11, 1, tzinfo=UTC),
            "configured",
            1.0,
            None,
        ),
        (
            "monitoring",
            {"market_timezone": "America/New_York"},
            datetime(2026, 9, 23, 11, 30, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 23, 23, 50, tzinfo=UTC),
        ),
        (
            "dispatch",
            {"krx_market_holiday_dates": "2026-09-23"},
            datetime(2026, 9, 23, 1, 0, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 23, 23, 50, tzinfo=UTC),
        ),
        (
            "dispatch",
            {},
            datetime(2026, 9, 26, 1, 0, tzinfo=UTC),
            "domestic_market_closed",
            60.0,
            datetime(2026, 9, 27, 23, 50, tzinfo=UTC),
        ),
    ],
    ids=[
        "closed",
        "regular",
        "opening-boundary",
        "krx-after-hours",
        "nxt-late",
        "after-hours-side-disabled",
        "routing-disabled",
        "nxt-pre-boundary",
        "nxt-pre-krx-preference",
        "krx-preopen-close",
        "krx-after-close",
        "kis-us",
        "live-alpaca",
        "paper-kis",
        "krx-timezone-is-fixed",
        "holiday",
        "weekend",
    ],
)
def test_autonomous_runtime_uses_session_aware_cadence(
    worker_key: str,
    settings_updates: dict[str, object],
    now: datetime,
    expected_mode: str,
    expected_delay: float,
    expected_next_active: datetime | None,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
    ).model_copy(update=settings_updates)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    schedule = coordinator._periodic_loop_schedule(
        worker_key=worker_key,
        configured_interval_seconds=1.0,
        now=now,
    )

    assert schedule.cadence_mode == expected_mode
    assert schedule.delay_seconds == expected_delay
    assert schedule.next_market_active_at == expected_next_active
    assert schedule.next_wake_at == now + timedelta(seconds=expected_delay)


def test_closed_market_cadence_never_runs_faster_than_configured_interval() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
        domestic_closed_market_idle_interval_seconds=0.5,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    schedule = coordinator._periodic_loop_schedule(
        worker_key="monitoring",
        configured_interval_seconds=300.0,
        now=datetime(2026, 9, 22, 11, 1, tzinfo=UTC),
    )

    assert schedule.cadence_mode == "domestic_market_closed"
    assert schedule.delay_seconds == 300.0


def test_authoritative_krx_holiday_closes_session_aware_cadence() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    coordinator._krx_trading_days[date(2026, 9, 23)] = False

    schedule = coordinator._periodic_loop_schedule(
        worker_key="monitoring",
        configured_interval_seconds=1.0,
        now=datetime(2026, 9, 23, 1, 0, tzinfo=UTC),
    )

    assert schedule.cadence_mode == "domestic_market_closed"
    assert schedule.next_market_active_at == datetime(2026, 9, 23, 23, 50, tzinfo=UTC)


def test_krx_calendar_refresh_caches_authoritative_broker_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    class CalendarAdapter:
        def is_krx_trading_day(self, access_token: str, *, market_date: date) -> bool:
            assert access_token == "calendar-token"
            assert market_date == date(2026, 9, 23)
            return False

    monkeypatch.setattr(
        autonomous_runtime,
        "ensure_authenticated_broker_session",
        lambda **_: SimpleNamespace(access_token="calendar-token"),
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: CalendarAdapter(),  # type: ignore[arg-type,return-value]
    )
    try:
        coordinator._refresh_current_krx_trading_day(
            datetime(2026, 9, 23, 0, 1, tzinfo=UTC)
        )

        assert coordinator._krx_trading_days == {date(2026, 9, 23): False}
        assert coordinator._krx_calendar_sources == {
            date(2026, 9, 23): "kis_chk_holiday"
        }
    finally:
        engine.dispose()


def test_unknown_krx_calendar_result_fails_open_and_throttles_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    class UnknownCalendarAdapter:
        def is_krx_trading_day(
            self, access_token: str, *, market_date: date
        ) -> None:
            return None

    monkeypatch.setattr(
        autonomous_runtime,
        "ensure_authenticated_broker_session",
        lambda **_: SimpleNamespace(access_token="calendar-token"),
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: UnknownCalendarAdapter(),  # type: ignore[arg-type,return-value]
    )
    checked_at = datetime(2026, 9, 23, 0, 1, tzinfo=UTC)
    try:
        coordinator._refresh_current_krx_trading_day(checked_at)

        assert coordinator._krx_trading_days == {}
        assert coordinator._krx_calendar_refresh_due(
            checked_at + timedelta(seconds=59)
        ) is False
        assert coordinator._krx_calendar_refresh_due(
            checked_at + timedelta(seconds=60)
        ) is True
    finally:
        engine.dispose()


def test_periodic_loop_uses_session_aware_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            runtime_mode="live",
            runtime_broker_provider="kis",
            monitoring_loop_interval_seconds=1.0,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        sleep_delays: list[float] = []
        cycle_calls = 0

        async def cycle(_started_at: datetime) -> dict[str, object]:
            nonlocal cycle_calls
            cycle_calls += 1
            if cycle_calls == 2:
                coordinator._stopping = True
            return {"status_code": 200}

        scheduled_wake = datetime(2026, 9, 22, 11, 2, tzinfo=UTC)
        coordinator._periodic_loop_schedule = (  # type: ignore[method-assign]
            lambda **_: SimpleNamespace(
                delay_seconds=60.0,
                cadence_mode="domestic_market_closed",
                next_wake_at=scheduled_wake,
                next_market_active_at=datetime(2026, 9, 22, 23, 50, tzinfo=UTC),
            )
        )

        async def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        monkeypatch.setattr(autonomous_runtime.asyncio, "sleep", fake_sleep)
        await coordinator._run_periodic_loop(
            worker_key="monitoring",
            interval_seconds=settings.monitoring_loop_interval_seconds,
            cycle=cycle,
        )

        assert sleep_delays == [1.0, 60.0]
        assert cycle_calls == 2
        snapshot = coordinator.snapshot()["monitoring"]
        assert snapshot["cadence_mode"] == "domestic_market_closed"
        assert snapshot["effective_poll_interval_seconds"] == 60.0
        assert snapshot["next_wake_at"] == scheduled_wake.isoformat()

    asyncio.run(scenario())


def test_monitoring_timeout_does_not_block_next_scheduler_tick() -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            monitoring_loop_interval_seconds=0.01,
            monitoring_cycle_timeout_seconds=0.05,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        first_cycle_started = asyncio.Event()
        release_first_cycle = asyncio.Event()
        calls = 0

        async def slow_monitoring_cycle(started_at):  # noqa: ANN001
            nonlocal calls
            calls += 1
            if calls == 1:
                first_cycle_started.set()
                await release_first_cycle.wait()
            return {"status_code": 200, "call": calls}

        task = asyncio.create_task(
            coordinator._run_periodic_loop(
                worker_key="monitoring",
                interval_seconds=settings.monitoring_loop_interval_seconds,
                cycle=slow_monitoring_cycle,
                run_immediately=True,
            )
        )
        try:
            await asyncio.wait_for(first_cycle_started.wait(), timeout=1)
            await asyncio.sleep(settings.monitoring_cycle_timeout_seconds * 2)

            assert calls > 1
            timeout_snapshot = coordinator.snapshot()["monitoring"]
            assert timeout_snapshot["last_cycle"].get("reason") != "monitoring_cycle_timed_out"

            release_first_cycle.set()
            await asyncio.sleep(settings.monitoring_loop_interval_seconds * 3)

            assert calls > 1
            recovered_snapshot = coordinator.snapshot()["monitoring"]
            assert recovered_snapshot["status"] == "running"
            assert recovered_snapshot["last_error"] is None
            assert recovered_snapshot["last_cycle"].get("reason") != "previous_monitoring_cycle_still_running"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_monitoring_timeout_expires_only_the_timed_out_cycle_broker_lease() -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            monitoring_cycle_timeout_seconds=0.02,
            nxt_eligible_sync_enabled=False,
        )
        broker_calls: list[str] = []
        cycle_started = threading.Event()
        release_cycle = threading.Event()

        class Adapter:
            provider = "test"

            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("placed")
                return object()

        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda _settings: Adapter(),  # type: ignore[return-value]
        )

        def delayed_monitoring_cycle(_started_at: datetime) -> dict[str, object]:
            cycle_started.set()
            assert release_cycle.wait(timeout=2)
            coordinator._broker_adapter_for_cycle().place_order("token", object())
            return {"status_code": 200}

        coordinator._monitoring_cycle_sync_guarded = delayed_monitoring_cycle  # type: ignore[method-assign]
        started_at = datetime.now(UTC)
        try:
            awaitable = coordinator._run_monitoring_cycle(started_at)
            result = await coordinator._await_cycle_payload(
                worker_key="monitoring",
                awaitable=awaitable,
            )

            assert result is None
            assert cycle_started.is_set()
            release_cycle.set()
            await coordinator._drain_inflight_cycles(timeout=1)
            assert broker_calls == []
        finally:
            release_cycle.set()
            await coordinator._drain_inflight_cycles(timeout=1)

    asyncio.run(scenario())


def test_stop_drains_monitoring_cycle_that_outlived_scheduler_timeout() -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            monitoring_loop_interval_seconds=0.01,
            monitoring_cycle_timeout_seconds=0.02,
            nxt_eligible_sync_enabled=False,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
            monitoring_price_source=None,
        )
        cycle_entered = threading.Event()
        release_cycle = threading.Event()
        cycle_completed = threading.Event()
        lifecycle_events: list[str] = []
        calls = 0

        class PriceSource:
            closed = False

            def close(self) -> None:
                self.closed = True
                lifecycle_events.append("price_source_closed")

        price_source = PriceSource()
        coordinator.monitoring_price_source = price_source

        def blocking_cycle(_started_at: datetime) -> dict[str, object]:
            nonlocal calls
            calls += 1
            cycle_entered.set()
            try:
                assert release_cycle.wait(timeout=2)
                return {"status_code": 200}
            finally:
                lifecycle_events.append("cycle_completed")
                cycle_completed.set()

        coordinator._monitoring_cycle_sync = blocking_cycle  # type: ignore[method-assign]
        coordinator._monitoring_task = asyncio.create_task(
            coordinator._run_periodic_loop(
                worker_key="monitoring",
                interval_seconds=settings.monitoring_loop_interval_seconds,
                cycle=coordinator._run_monitoring_cycle,
                run_immediately=True,
            )
        )
        stop_task: asyncio.Task[None] | None = None
        restart_task: asyncio.Task[None] | None = None
        try:
            await _wait_for_thread_event(cycle_entered)
            await asyncio.sleep(settings.monitoring_cycle_timeout_seconds * 2)
            stop_task = asyncio.create_task(coordinator.stop())
            await asyncio.sleep(0.02)

            assert stop_task.done() is False
            assert cycle_completed.is_set() is False
            assert price_source.closed is False
            restart_task = asyncio.create_task(coordinator.start())
            await asyncio.sleep(0.02)
            assert restart_task.done() is True
            with pytest.raises(RuntimeError, match="shutting down"):
                await restart_task

            release_cycle.set()
            await asyncio.wait_for(stop_task, timeout=1)
            await asyncio.sleep(settings.monitoring_loop_interval_seconds * 2)

            assert cycle_completed.is_set() is True
            assert calls == 1
            assert coordinator._monitoring_task is None
            assert lifecycle_events == ["cycle_completed", "price_source_closed"]
        finally:
            release_cycle.set()
            if stop_task is not None:
                await asyncio.gather(stop_task, return_exceptions=True)
            if restart_task is not None:
                await asyncio.gather(restart_task, return_exceptions=True)
            await coordinator.stop()

    asyncio.run(scenario())


def test_autonomous_runtime_start_is_idempotent() -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            nxt_eligible_sync_enabled=False,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )

        await coordinator.start()
        first_tasks = coordinator._loop_tasks()
        await coordinator.start()

        assert coordinator._loop_tasks() == first_tasks
        await coordinator.stop()

    asyncio.run(scenario())


def test_monitoring_and_daily_discovery_share_one_reconciliation_flight() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        autonomous_runtime_enabled=True,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    refresh_entered = threading.Event()
    release_refresh = threading.Event()
    counter_lock = threading.Lock()
    now = datetime.now(UTC)
    call_count = 0
    active = 0
    max_active = 0
    results: list[bool] = []

    def blocking_refresh(*, loaded_at: datetime, **_kwargs):  # noqa: ANN202
        nonlocal active, call_count, max_active
        with counter_lock:
            call_count += 1
            active += 1
            max_active = max(max_active, active)
        refresh_entered.set()
        try:
            assert release_refresh.wait(timeout=2)
            coordinator._monitoring_snapshot_last_reconciled_at = loaded_at
        finally:
            with counter_lock:
                active -= 1
        return None

    coordinator._refresh_monitoring_runtime_snapshot = blocking_refresh  # type: ignore[method-assign]
    threads = [
        threading.Thread(
            target=lambda: results.append(
                coordinator._reconcile_and_refresh_monitoring_snapshot_if_due(now)
            )
        )
        for _ in range(2)
    ]
    threads[0].start()
    try:
        assert refresh_entered.wait(timeout=1)
        threads[1].start()
        time.sleep(0.05)

        assert call_count == 1
        assert max_active == 1

        release_refresh.set()
        for thread in threads:
            thread.join(timeout=1)
        assert not any(thread.is_alive() for thread in threads)
        assert results == [True, True]
    finally:
        release_refresh.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=1)


@pytest.mark.parametrize(
    ("task_attribute", "runner_name", "sync_name"),
    [
        ("_dispatch_task", "_run_dispatch_cycle", "_dispatch_cycle_sync"),
        (
            "_watch_exec_agent_sizing_task",
            "_run_watch_exec_agent_sizing_cycle",
            "_watch_exec_agent_sizing_cycle_sync",
        ),
        (
            "_nxt_eligible_sync_task",
            "_run_nxt_eligible_sync_cycle",
            "_nxt_eligible_sync_cycle_sync",
        ),
    ],
)
def test_stop_drains_each_autonomous_threaded_worker(
    task_attribute: str,
    runner_name: str,
    sync_name: str,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        cycle_entered = threading.Event()
        release_cycle = threading.Event()
        calls = 0

        def blocking_cycle(_started_at: datetime) -> dict[str, object]:
            nonlocal calls
            calls += 1
            cycle_entered.set()
            assert release_cycle.wait(timeout=2)
            return {"status_code": 200}

        setattr(coordinator, sync_name, blocking_cycle)
        runner = getattr(coordinator, runner_name)
        setattr(
            coordinator,
            task_attribute,
            asyncio.create_task(runner(datetime.now(UTC))),
        )
        stop_task: asyncio.Task[None] | None = None
        try:
            await _wait_for_thread_event(cycle_entered)
            stop_task = asyncio.create_task(coordinator.stop())
            await asyncio.sleep(0.02)

            assert stop_task.done() is False

            release_cycle.set()
            await asyncio.wait_for(stop_task, timeout=1)
            await asyncio.sleep(0.02)

            assert calls == 1
            assert getattr(coordinator, task_attribute) is None
        finally:
            release_cycle.set()
            if stop_task is not None:
                await asyncio.gather(stop_task, return_exceptions=True)
            await coordinator.stop()

    asyncio.run(scenario())


def test_stop_drains_daily_krx_position_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            runtime_mode="live",
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        discovery_entered = threading.Event()
        release_discovery = threading.Event()
        calls = 0

        def blocking_discovery(_now: datetime) -> bool:
            nonlocal calls
            calls += 1
            discovery_entered.set()
            assert release_discovery.wait(timeout=2)
            return True

        monkeypatch.setattr(autonomous_runtime, "_is_regular_market_session", lambda **_: True)
        monkeypatch.setattr(
            coordinator,
            "_broker_positions_reconciled_on_market_date",
            lambda _now: False,
        )
        coordinator._monitoring_state.last_success_at = datetime.now(UTC)
        coordinator._reconcile_and_refresh_monitoring_snapshot_if_due = (  # type: ignore[method-assign]
            blocking_discovery
        )
        coordinator._daily_krx_position_discovery_task = asyncio.create_task(
            coordinator._run_daily_krx_position_discovery_loop()
        )
        stop_task: asyncio.Task[None] | None = None
        try:
            await _wait_for_thread_event(discovery_entered)
            stop_task = asyncio.create_task(coordinator.stop())
            await asyncio.sleep(0.02)

            assert stop_task.done() is False

            release_discovery.set()
            await asyncio.wait_for(stop_task, timeout=1)
            await asyncio.sleep(0.02)

            assert calls == 1
            assert coordinator._daily_krx_position_discovery_task is None
        finally:
            release_discovery.set()
            if stop_task is not None:
                await asyncio.gather(stop_task, return_exceptions=True)
            await coordinator.stop()

    asyncio.run(scenario())


def test_daily_position_discovery_skips_authoritative_krx_holiday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
            runtime_mode="live",
            runtime_broker_provider="kis",
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        current_market_date = datetime.now(UTC).astimezone(
            autonomous_runtime.KRX_TIMEZONE
        ).date()
        coordinator._krx_trading_days.update(
            {
                current_market_date - timedelta(days=1): False,
                current_market_date: False,
                current_market_date + timedelta(days=1): False,
            }
        )
        coordinator._monitoring_state.last_success_at = datetime.now(UTC)
        reconciliation_calls = 0

        def reconcile(_now: datetime) -> bool:
            nonlocal reconciliation_calls
            reconciliation_calls += 1
            return True

        async def fake_sleep(_delay: float) -> None:
            coordinator._stopping = True

        monkeypatch.setattr(autonomous_runtime, "_is_regular_market_session", lambda **_: True)
        monkeypatch.setattr(autonomous_runtime.asyncio, "sleep", fake_sleep)
        coordinator._reconcile_and_refresh_monitoring_snapshot_if_due = (  # type: ignore[method-assign]
            reconcile
        )

        await coordinator._run_daily_krx_position_discovery_loop()

        assert reconciliation_calls == 0

    asyncio.run(scenario())


def test_monitoring_stale_mutex_skips_until_original_cycle_finishes() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        autonomous_runtime_enabled=True,
        monitoring_cycle_timeout_seconds=0.05,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    first_cycle_entered = threading.Event()
    release_first_cycle = threading.Event()
    calls = 0
    active_cycles = 0
    max_active_cycles = 0
    first_payloads: list[dict[str, object]] = []
    started_at = datetime(2026, 8, 1, 1, 0, tzinfo=UTC)

    def blocking_monitoring_cycle(started_at):  # noqa: ANN001
        nonlocal active_cycles, calls, max_active_cycles
        calls += 1
        active_cycles += 1
        max_active_cycles = max(max_active_cycles, active_cycles)
        try:
            if calls == 1:
                first_cycle_entered.set()
                assert release_first_cycle.wait(timeout=1)
            return {"status_code": 200, "call": calls}
        finally:
            active_cycles -= 1

    coordinator._monitoring_cycle_sync = blocking_monitoring_cycle  # type: ignore[method-assign]

    first_thread = threading.Thread(
        target=lambda: first_payloads.append(
            coordinator._monitoring_cycle_sync_guarded(started_at)
        )
    )
    first_thread.start()
    try:
        assert first_cycle_entered.wait(timeout=1)

        stale_payload = coordinator._monitoring_cycle_sync_guarded(
            started_at + timedelta(seconds=1)
        )

        assert stale_payload["status_code"] == 202
        assert stale_payload["skipped"] is True
        assert stale_payload["reason"] == "previous_monitoring_cycle_still_running"
        assert stale_payload["stale_age_seconds"] == 1
        assert calls == 1
        assert max_active_cycles == 1

        release_first_cycle.set()
        first_thread.join(timeout=1)
        assert not first_thread.is_alive()
        assert first_payloads == [{"status_code": 200, "call": 1}]

        recovered_payload = coordinator._monitoring_cycle_sync_guarded(
            started_at + timedelta(seconds=2)
        )

        assert recovered_payload == {"status_code": 200, "call": 2}
        assert calls == 2
        assert max_active_cycles == 1
    finally:
        release_first_cycle.set()
        first_thread.join(timeout=1)


def test_timed_out_agent_process_releases_monitoring_mutex_within_bounded_margin() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        autonomous_runtime_enabled=True,
        monitoring_cycle_timeout_seconds=4,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    calls = 0
    started_at = datetime(2026, 8, 1, 1, 0, tzinfo=UTC)

    def timed_agent_cycle(_started_at):  # noqa: ANN001
        nonlocal calls
        calls += 1
        if calls == 1:
            with pytest.raises(subprocess.TimeoutExpired):
                _run_exec_agent_command(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    timeout_seconds=1,
                )
        return {"status_code": 200, "call": calls}

    coordinator._monitoring_cycle_sync = timed_agent_cycle  # type: ignore[method-assign]
    started = time.monotonic()

    assert coordinator._monitoring_cycle_sync_guarded(started_at) == {
        "status_code": 200,
        "call": 1,
    }
    assert time.monotonic() - started < 2.5
    assert coordinator._monitoring_cycle_sync_guarded(
        started_at + timedelta(seconds=2)
    ) == {"status_code": 200, "call": 2}


def test_stop_closes_monitoring_price_source_lifecycle() -> None:
    class ClosablePriceSource:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        price_source = ClosablePriceSource()
        settings = Settings(
            database_url="sqlite:///:memory:",
            autonomous_runtime_enabled=True,
        )
        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
            monitoring_price_source=price_source,
        )

        await coordinator.stop()

        assert price_source.closed is True

    asyncio.run(scenario())


def test_monitoring_stale_mutex_exits_when_guard_enabled(monkeypatch) -> None:  # noqa: ANN001
    settings = Settings(
        database_url="sqlite:///:memory:",
        autonomous_runtime_enabled=True,
        monitoring_cycle_timeout_seconds=10,
        monitoring_exit_on_stale_cycle_enabled=True,
        monitoring_stale_cycle_exit_grace_multiplier=2,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    exit_codes: list[int] = []
    monkeypatch.setattr(os, "_exit", lambda code: exit_codes.append(code))
    coordinator._monitoring_cycle_mutex_started_at = datetime(2026, 8, 1, 1, 0, tzinfo=UTC)
    assert coordinator._monitoring_cycle_mutex.acquire(blocking=False)
    try:
        payload = coordinator._monitoring_cycle_sync_guarded(
            datetime(2026, 8, 1, 1, 0, 20, tzinfo=UTC)
        )
    finally:
        coordinator._monitoring_cycle_mutex.release()

    assert payload["reason"] == "previous_monitoring_cycle_still_running"
    assert exit_codes == [75]


def test_sqlite_local_desired_symbols_include_older_active_watch_when_newer_plans_are_non_monitorable(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-symbols.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    accepted_at = datetime(2026, 8, 10, 1, 0, tzinfo=UTC)

    try:
        with session_factory() as session:
            for index in range(70):
                session.add(
                    ExecutionPlan(
                        id=f"newer-plan-{index}",
                        external_plan_id=f"newer-plan-{index}",
                        source="kai-trading-core",
                        market_date=date(2026, 8, 10),
                        mode="paper",
                        status="accepted",
                        accepted_at=accepted_at + timedelta(minutes=index),
                        items=[
                            ExecutionPlanItem(
                                id=f"newer-item-{index}",
                                plan_item_index=index,
                                symbol=f"{index:06d}",
                                side="buy",
                                intent="intraday_watch_buy",
                                status="broker_order_submitted",
                                quantity_policy={"kind": "shares", "quantity": 1},
                                trigger_price=Decimal("1000.0000"),
                                time_in_force="day",
                                updated_at=accepted_at + timedelta(minutes=index),
                            )
                        ],
                    )
                )
            session.add(
                ExecutionPlan(
                    id="older-watch-plan",
                    external_plan_id="older-watch-plan",
                    source="kai-trading-core",
                    market_date=date(2026, 8, 9),
                    mode="paper",
                    status="accepted",
                    accepted_at=accepted_at - timedelta(days=1),
                    items=[
                        ExecutionPlanItem(
                            id="older-watch-item-1",
                            plan_item_index=0,
                            symbol="111111",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                            quantity_policy={"kind": "shares", "quantity": 1},
                            trigger_price=Decimal("1000.0000"),
                            time_in_force="day",
                            updated_at=accepted_at - timedelta(days=1),
                        ),
                        ExecutionPlanItem(
                            id="older-watch-item-2",
                            plan_item_index=1,
                            symbol="222222",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                            quantity_policy={"kind": "shares", "quantity": 1},
                            trigger_price=Decimal("2000.0000"),
                            time_in_force="day",
                            updated_at=accepted_at - timedelta(days=1),
                        ),
                    ],
                )
            )
            session.commit()

        assert coordinator._load_local_desired_monitoring_symbols_from_sqlite()[:2] == [
            "111111",
            "222222",
        ]
    finally:
        engine.dispose()


def test_sqlite_monitoring_snapshot_excludes_terminal_watch_items(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-terminal-items.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    try:
        with session_factory() as session:
            plan = ExecutionPlan(
                external_plan_id="terminal-watch-items",
                source="kai-trading-core",
                market_date=date(2026, 8, 18),
                mode="paper",
                status="accepted",
                items=[
                    ExecutionPlanItem(
                        plan_item_index=index,
                        symbol=symbol,
                        side="buy",
                        intent="intraday_watch_buy",
                        status=status,
                        trigger_price=Decimal("1000"),
                    )
                    for index, (symbol, status) in enumerate(
                        [
                            ("111111", "watch_monitoring_active"),
                            ("222222", "watch_expired"),
                            ("333333", "watch_cancelled"),
                            ("444444", "watch_buy_rejected"),
                            ("555555", "watch_buy_submitted"),
                        ]
                    )
                ],
            )
            plan.items[0].risk_context = {
                "watch_buy_monitoring": {"direction": "at_or_above"},
            }
            session.add(plan)
            session.commit()

        assert coordinator._load_local_desired_monitoring_symbols_from_sqlite() == ["111111"]
        snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
            loaded_at=datetime(2026, 8, 18, 0, 10, tzinfo=UTC),
            symbols=["111111", "222222", "333333", "444444", "555555"],
        )
        assert snapshot is not None
        assert [item.symbol for item in snapshot.items] == ["111111"]
        assert snapshot.items[0].trigger_direction == "at_or_above"
    finally:
        engine.dispose()


def test_monitoring_snapshots_include_only_price_watch_sizing_pending_items(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-price-watch-items.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    price_watch_risk, price_watch_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={
            "exchange": "KRX",
            "reward_risk_max_entry_price": "1000.0000",
            "watch_exec_agent_deferred": {
                "reason_code": "reward_risk_price_watch",
                "reason_family": "reward_risk",
                "disposition": "defer",
                "price_watch_only": True,
            },
        },
        quantity_policy={"type": "exec_agent_sized", "budget_krw": "150000"},
    )
    ordinary_pending_risk, ordinary_pending_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={
            "exchange": "KRX",
            "watch_exec_agent_deferred": {
                "reason_code": "account_snapshot_unavailable",
                "reason_family": "account_snapshot",
                "disposition": "retry",
            },
        },
        quantity_policy={"type": "exec_agent_sized", "budget_krw": "150000"},
    )

    try:
        with session_factory() as session:
            session.add(
                ExecutionPlan(
                    external_plan_id="price-watch-sizing-pending-items",
                    source="kai-trading-core",
                    market_date=date(2026, 10, 2),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            id="price-watch-sizing-pending-item",
                            plan_item_index=0,
                            symbol="111111",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_sizing_pending",
                            trigger_price=Decimal("1000"),
                            provider="kis",
                            risk_context=price_watch_risk,
                            quantity_policy=price_watch_policy,
                        ),
                        ExecutionPlanItem(
                            id="ordinary-sizing-pending-item",
                            plan_item_index=1,
                            symbol="222222",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_sizing_pending",
                            trigger_price=Decimal("2000"),
                            provider="kis",
                            risk_context=ordinary_pending_risk,
                            quantity_policy=ordinary_pending_policy,
                        ),
                    ],
                )
            )
            session.commit()

            assert coordinator._load_local_desired_monitoring_symbols_from_sqlite() == [
                "111111"
            ]
            raw_snapshot = (
                coordinator._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
                    loaded_at=datetime(2026, 10, 2, 1, tzinfo=UTC),
                    symbols=["111111", "222222"],
                )
            )
            orm_snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=session,
                loaded_at=datetime(2026, 10, 2, 1, tzinfo=UTC),
                symbols=["111111", "222222"],
            )

        assert raw_snapshot is not None
        assert [item.id for item in raw_snapshot.items] == [
            "price-watch-sizing-pending-item"
        ]
        assert [item.id for item in orm_snapshot.items] == [
            "price-watch-sizing-pending-item"
        ]
    finally:
        engine.dispose()


def test_monitoring_snapshots_include_live_max_holding_transition_states(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-max-holding-items.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    statuses = (
        "max_holding_close_sell_triggered_blocked",
        "max_holding_close_sell_triggered_pending_dispatch",
    )
    terminal_rows = (
        ("target_sell", "filled"),
        ("stop_sell", "expired"),
        ("max_holding_close_sell", "cancelled"),
    )

    try:
        with session_factory() as session:
            session.add(
                ExecutionPlan(
                    external_plan_id="live-max-holding-transition-items",
                    source="test-fixture",
                    market_date=date(2026, 9, 23),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            id=f"max-holding-{index}",
                            plan_item_index=index,
                            symbol=f"{5930 + index:06d}",
                            side="sell",
                            intent="max_holding_close_sell",
                            status=status,
                            quantity_policy={"kind": "shares", "quantity": 1},
                            risk_context={"exchange": "KRX"},
                        )
                        for index, status in enumerate(statuses)
                    ]
                    + [
                        ExecutionPlanItem(
                            id=f"terminal-sell-{index}",
                            plan_item_index=len(statuses) + index,
                            symbol=f"{6930 + index:06d}",
                            side="sell",
                            intent=intent,
                            status=status,
                            quantity_policy={"kind": "shares", "quantity": 1},
                            risk_context={"exchange": "KRX"},
                        )
                        for index, (intent, status) in enumerate(terminal_rows)
                    ],
                )
            )
            session.commit()
            symbols = ["005930", "005931", "006930", "006931", "006932"]

            raw_snapshot = (
                coordinator._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
                    loaded_at=datetime(2026, 9, 23, 1, tzinfo=UTC),
                    symbols=symbols,
                )
            )
            orm_snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=session,
                loaded_at=datetime(2026, 9, 23, 1, tzinfo=UTC),
                symbols=symbols,
            )

        assert raw_snapshot is not None
        assert {item.status for item in raw_snapshot.items} == set(statuses)
        assert {item.status for item in orm_snapshot.items} == set(statuses)
    finally:
        engine.dispose()


def test_cached_monitoring_loaders_exclude_foreign_and_other_mode_watch_buys(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-account-scope.db'}",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="cached-monitor-route-secret-for-tests",
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    def watch(
        *,
        item_id: str,
        symbol: str,
        risk_context: dict[str, object] | None,
        quantity_policy: dict[str, object] | None,
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan_item_index=0,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=Decimal("100"),
            provider="kis",
            asset_class="stock",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="current-cached-watch-plan",
                        source="test",
                        mode="paper",
                        market_date=date(2026, 9, 8),
                        items=[
                            watch(
                                item_id="current-cached-watch",
                                symbol="111111",
                                risk_context=current_risk,
                                quantity_policy=current_policy,
                            )
                        ],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-cached-watch-plan",
                        source="test",
                        mode="paper",
                        market_date=date(2026, 9, 8),
                        items=[
                            watch(
                                item_id="foreign-cached-watch",
                                symbol="222222",
                                risk_context=foreign_risk,
                                quantity_policy=foreign_policy,
                            )
                        ],
                    ),
                    ExecutionPlan(
                        external_plan_id="other-mode-cached-watch-plan",
                        source="test",
                        mode="live",
                        market_date=date(2026, 9, 8),
                        items=[
                            watch(
                                item_id="other-mode-cached-watch",
                                symbol="333333",
                                risk_context=current_risk,
                                quantity_policy=current_policy,
                            )
                        ],
                    ),
                ]
            )
            db.commit()

            assert coordinator._load_local_desired_monitoring_symbols_from_sqlite() == [
                "111111"
            ]
            raw_snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
                loaded_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
                symbols=["111111", "222222", "333333"],
            )
            orm_snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=db,
                loaded_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
                symbols=["111111", "222222", "333333"],
            )

            assert raw_snapshot is not None
            assert [item.id for item in raw_snapshot.items] == ["current-cached-watch"]
            assert [item.id for item in orm_snapshot.items] == ["current-cached-watch"]
    finally:
        engine.dispose()


def test_cached_cycle_repairs_held_position_monitors_with_desired_symbols(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 8, 18, 0, 10, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=1)
        },
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = snapshot
    prices = [MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))]
    maintenance_calls: list[str] = []

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: _core_monitoring_handoff(),
    )
    monkeypatch.setattr(coordinator, "_load_local_desired_monitoring_symbols", lambda: ["005930"])
    monkeypatch.setattr(coordinator, "_reconcile_monitoring_orders_if_due", lambda **kwargs: None)
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols",
        lambda **kwargs: snapshot,
    )
    monkeypatch.setattr(coordinator, "_refresh_monitoring_snapshot_filled_today", lambda now: None)
    monkeypatch.setattr(coordinator, "_load_monitoring_prices", lambda **kwargs: prices)
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: maintenance_calls.append("called") or (0, 0),
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["status_code"] == 200
    assert maintenance_calls == ["called"]


def test_cached_repair_passes_broker_adapter_to_post_entry_maintenance(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    expected_adapter = object()
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: expected_adapter,  # type: ignore[return-value]
    )
    started_at = datetime(2026, 10, 2, 2, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=1)
        },
        items=[],
        filled_today_symbols=[],
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        autonomous_runtime,
        "_load_positions_by_symbol",
        lambda **kwargs: {"005930": object()},
    )

    def capture_maintenance(**kwargs: object) -> tuple[int, int]:
        captured.update(kwargs)
        return 0, 0

    monkeypatch.setattr(
        autonomous_runtime,
        "_maintain_post_entry_exit_monitors",
        capture_maintenance,
    )

    result = coordinator._repair_post_entry_exit_monitors_for_cached_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))],
        started_at=started_at,
    )

    assert result == (0, 0)
    guarded_adapter = captured["adapter"]
    assert getattr(guarded_adapter, "_adapter") is expected_adapter
    assert callable(captured["krx_trading_day"])
    engine.dispose()


def test_cached_cycle_persists_review_only_unpriced_us_position_state() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="cached-review-test",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    with session_factory() as session:
        session.add(
            ExecutionPosition(
                mode=settings.runtime_mode,
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="OLDCO",
                quantity=7,
                average_price=Decimal("12.50"),
                updated_from="reconciliation",
                raw_payload={
                    "_position_sync": {
                        "asset_class": "us_equity",
                        "exchange": "NASD",
                        "monitoring_eligible": True,
                    }
                },
            )
        )
        session.commit()

    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["OLDCO"],
        positions_by_symbol={
            "OLDCO": CachedMonitoringPosition(
                symbol="OLDCO",
                quantity=7,
                provider="kis",
                exchange="NASD",
            )
        },
        items=[],
        filled_today_symbols=[],
    )

    result = coordinator._repair_post_entry_exit_monitors_for_cached_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))],
        started_at=started_at,
        durably_unquoteable_symbols={"OLDCO"},
    )

    assert result == (0, 1)
    with session_factory() as session:
        position = session.query(ExecutionPosition).filter_by(symbol="OLDCO").one()
        assert position.raw_payload["_position_sync"] == {
            "asset_class": "us_equity",
            "exchange": "NASD",
            "monitoring_eligible": False,
            "manual_review_required": True,
            "manual_review_reason": "unpriced_overseas_holding",
            "manual_review_recorded_at": started_at.isoformat(),
        }
    engine.dispose()


def test_cached_cycle_does_not_relatch_fresh_complete_kis_us_balance_proof() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="fresh-balance-proof-test",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    started_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    raw_payload = payload_with_position_identity(
        {
            "currency": "USD",
            "exchange": "NASD",
            "account_number_masked": "****5678",
            "holdings_coverage": "complete",
            "open_orders_coverage": "complete",
            "balance": {
                "rt_cd": "0",
                "output1": [
                    {
                        "ovrs_pdno": "APTX",
                        "ovrs_excg_cd": "NASD",
                        "ovrs_cblc_qty": "377",
                        "ord_psbl_qty": "377",
                        "pchs_avg_pric": "0.0963",
                        "now_pric2": "0.061",
                    }
                ],
            },
            "open_orders": {"rt_cd": "0", "output": []},
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": True,
            },
        },
        identity=identity,
    )
    with session_factory() as session:
        session.add(
            ExecutionPosition(
                mode=settings.runtime_mode,
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="APTX",
                quantity=377,
                average_price=Decimal("0.0963"),
                updated_from="reconciliation",
                last_reconciled_at=started_at - timedelta(seconds=5),
                raw_payload=raw_payload,
            )
        )
        session.commit()

    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["APTX"],
        positions_by_symbol={
            "APTX": CachedMonitoringPosition(
                symbol="APTX",
                quantity=377,
                provider="kis",
                exchange="NASD",
            )
        },
        items=[],
        filled_today_symbols=[],
    )

    result = coordinator._repair_post_entry_exit_monitors_for_cached_cycle(
        runtime_snapshot=snapshot,
        prices=[],
        started_at=started_at,
        durably_unquoteable_symbols={"APTX"},
    )

    assert result == (0, 0)
    with session_factory() as session:
        position = session.query(ExecutionPosition).filter_by(symbol="APTX").one()
        assert position.raw_payload["_position_sync"] == {
            "asset_class": "us_equity",
            "exchange": "NASD",
            "monitoring_eligible": True,
        }
    engine.dispose()


@pytest.mark.parametrize(
    "invalid_proof",
    [
        "stale",
        "zero_price",
        "incomplete_snapshot",
        "wrong_source_shape",
        "independent_quarantine",
        "reserved_quantity",
        "matching_open_order",
        "account_mask_mismatch",
    ],
)
def test_kis_us_balance_proof_fails_closed(invalid_proof: str) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="invalid-balance-proof-test",
        kis_account_number="12345678",
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    evaluated_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    holding = {
        "ovrs_pdno": "WWBLCM",
        "ovrs_excg_cd": "NASD",
        "ovrs_cblc_qty": "75",
        "ord_psbl_qty": "75",
        "pchs_avg_pric": "0.9164",
        "now_pric2": "0.061",
    }
    sync_context: dict[str, object] = {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": True,
    }
    raw_payload = payload_with_position_identity(
        {
            "currency": "USD",
            "exchange": "NASD",
            "account_number_masked": "****5678",
            "holdings_coverage": "complete",
            "open_orders_coverage": "complete",
            "balance": {"rt_cd": "0", "output1": [holding]},
            "open_orders": {"rt_cd": "0", "output": []},
            "_position_sync": sync_context,
        },
        identity=identity,
    )
    last_reconciled_at = evaluated_at - timedelta(seconds=5)
    if invalid_proof == "stale":
        last_reconciled_at = evaluated_at - timedelta(
            seconds=settings.exec_agent_max_account_snapshot_age_seconds + 1
        )
    elif invalid_proof == "zero_price":
        holding["now_pric2"] = "0"
    elif invalid_proof == "incomplete_snapshot":
        raw_payload["holdings_coverage"] = "incomplete"
    elif invalid_proof == "wrong_source_shape":
        holding["pdno"] = holding.pop("ovrs_pdno")
    elif invalid_proof == "independent_quarantine":
        sync_context["trade_eligible"] = False
        sync_context["route_disposition"] = "manual_review"
    elif invalid_proof == "reserved_quantity":
        holding["ord_psbl_qty"] = "74"
    elif invalid_proof == "matching_open_order":
        raw_payload["open_orders"] = {
            "rt_cd": "0",
            "output": [
                {
                    "pdno": "WWBLCM",
                    "odno": "open-sell-1",
                    "sll_buy_dvsn_cd": "01",
                    "ft_ord_qty": "1",
                    "nccs_qty": "1",
                }
            ],
        }
    elif invalid_proof == "account_mask_mismatch":
        raw_payload["account_number_masked"] = "****9999"

    position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="WWBLCM",
        quantity=75,
        average_price=Decimal("0.9164"),
        updated_from="reconciliation",
        last_reconciled_at=last_reconciled_at,
        raw_payload=raw_payload,
    )

    assert not monitoring_service._has_fresh_complete_kis_us_balance_monitoring_proof(
        settings=settings,
        position=position,
        evaluated_at=evaluated_at,
    )


def test_cached_cycle_removes_reviewed_position_from_reported_monitoring_set(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    initial_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930", "OLDCO"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=1),
            "OLDCO": CachedMonitoringPosition(symbol="OLDCO", quantity=7),
        },
        items=[],
        filled_today_symbols=[],
    )
    reviewed_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=1),
        },
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = initial_snapshot

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: _core_monitoring_handoff(),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_desired_monitoring_symbols",
        lambda: ["005930", "OLDCO"],
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols",
        lambda **kwargs: initial_snapshot,
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_snapshot_filled_today",
        lambda now: None,
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: [
            MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))
        ],
    )
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (0, 1),
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: (
            setattr(coordinator, "_monitoring_runtime_snapshot", reviewed_snapshot)
            or reviewed_snapshot
        ),
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["monitored_symbols"] == ["005930"]
    assert payload["price_count"] == 1
    assert payload["runtime_snapshot"]["position_count"] == 1
    assert payload["post_entry_review_count"] == 1


def test_cached_cycle_returns_review_transition_when_every_symbol_is_removed(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")

    class DurableEmptyPriceSource:
        def get_last_diagnostics(self) -> dict[str, object]:
            return {"durably_unquoteable_symbols": ["OLDCO"]}

    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        monitoring_price_source=DurableEmptyPriceSource(),
    )
    started_at = datetime(2026, 9, 15, 15, 0, tzinfo=UTC)
    initial_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["OLDCO"],
        positions_by_symbol={
            "OLDCO": CachedMonitoringPosition(
                symbol="OLDCO",
                quantity=7,
                provider="kis",
                exchange="NASD",
            )
        },
        items=[],
        filled_today_symbols=[],
    )
    reviewed_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=[],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = initial_snapshot
    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: _core_monitoring_handoff(),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_desired_monitoring_symbols",
        lambda: ["OLDCO"],
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_position_symbols",
        lambda: ["OLDCO"],
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols",
        lambda **kwargs: initial_snapshot,
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_snapshot_filled_today",
        lambda now: None,
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (0, 1),
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: (
            setattr(coordinator, "_monitoring_runtime_snapshot", reviewed_snapshot)
            or reviewed_snapshot
        ),
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["idle"] is True
    assert payload["monitored_symbols"] == []
    assert payload["price_count"] == 0
    assert payload["post_entry_review_count"] == 1
    assert payload["submitted_order_count"] == 0


def test_cached_evaluation_skips_monitoring_disabled_held_position() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["OLDCO"],
        positions_by_symbol={
            "OLDCO": CachedMonitoringPosition(
                symbol="OLDCO",
                quantity=7,
                provider="kis",
                exchange="NASD",
                monitoring_eligible=False,
            )
        },
        items=[
            CachedMonitoringItem(
                id="stop-oldco",
                symbol="OLDCO",
                intent="stop_sell",
                status="stop_monitoring",
                trigger_price=Decimal("10"),
                provider="kis",
                exchange="NASD",
            )
        ],
        filled_today_symbols=[],
    )
    payload = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="OLDCO", price=Decimal("5"))],
        started_at=started_at,
    )

    assert payload["evaluated_item_count"] == 0
    assert payload["triggered_item_count"] == 0
    assert payload["submitted_order_count"] == 0


def test_cached_monitoring_prefilter_uses_executable_quotes_and_watch_direction() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 8, 18, 0, 10, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["111111", "222222", "333333"],
        positions_by_symbol={
            "333333": CachedMonitoringPosition(
                symbol="333333",
                quantity=1,
                provider="kis",
                exchange="KRX",
            ),
        },
        items=[
            CachedMonitoringItem(
                id="pullback",
                symbol="111111",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=Decimal("100"),
                trigger_direction="at_or_below",
            ),
            CachedMonitoringItem(
                id="breakout",
                symbol="222222",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=Decimal("100"),
                trigger_direction="at_or_above",
            ),
            CachedMonitoringItem(
                id="stop",
                symbol="333333",
                intent="stop_sell",
                status="monitoring_active",
                trigger_price=Decimal("100"),
                trigger_direction=None,
            ),
        ],
        filled_today_symbols=[],
    )

    payload = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[
            MonitoringPriceRequest(
                symbol="111111",
                price=Decimal("110"),
                ask=Decimal("99"),
            ),
            MonitoringPriceRequest(
                symbol="222222",
                price=Decimal("90"),
                ask=Decimal("101"),
            ),
            MonitoringPriceRequest(
                symbol="333333",
                price=Decimal("105"),
                bid=Decimal("99"),
            ),
        ],
        started_at=started_at,
    )

    assert payload["triggered_buy_symbols"] == ["111111", "222222"]
    assert payload["triggered_sell_symbols"] == ["333333"]


def test_cached_max_holding_exit_triggers_only_on_close_exit_date() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    close_exit_date = date(2026, 9, 24)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=datetime(2026, 9, 23, 1, 0, tzinfo=UTC),
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[
            CachedMonitoringItem(
                id="max-holding-exit",
                symbol="005930",
                intent="max_holding_close_sell",
                status="max_holding_close_sell_monitoring",
                trigger_price=None,
                close_exit_date=close_exit_date,
                provider="kis",
                exchange="KRX",
            )
        ],
        filled_today_symbols=[],
    )

    before_due = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))],
        started_at=datetime(2026, 9, 23, 14, 59, tzinfo=UTC),
    )
    due = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))],
        started_at=datetime(2026, 9, 23, 15, 0, tzinfo=UTC),
    )

    assert before_due["triggered_item_count"] == 0
    assert before_due["triggered_sell_symbols"] == []
    assert due["triggered_item_count"] == 1
    assert due["triggered_sell_symbols"] == ["005930"]


def test_cached_sell_rearm_requires_an_executable_price() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 24, 1, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[
            CachedMonitoringItem(
                id="blocked-stop-without-bid",
                symbol="005930",
                intent="stop_sell",
                status="stop_sell_triggered_blocked",
                trigger_price=Decimal("100"),
                provider="kis",
                exchange="KRX",
            )
        ],
        filled_today_symbols=[],
    )

    payload = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[MonitoringPriceRequest(symbol="005930", price=Decimal("101"))],
        started_at=started_at,
    )

    assert payload["triggered_item_count"] == 0
    assert payload["rearmable_sell_symbols"] == []


@pytest.mark.parametrize(
    "price",
    [
        MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("101"),
            bid=Decimal("101"),
            observed_at=datetime(2026, 9, 24, 1, 0, tzinfo=UTC),
        ),
        MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("101"),
            bid=Decimal("101"),
            observed_at=datetime(2026, 9, 24, 0, 59, tzinfo=UTC),
            freshness_source="kis_exchange_orderbook_time",
            provenance={
                "field_sources": {
                    "bid": {
                        "freshness_source": "kis_exchange_orderbook_time",
                        "observed_at": "2026-09-24T00:59:00+00:00",
                    }
                }
            },
        ),
    ],
)
def test_cached_sell_rearm_rejects_untrusted_or_stale_live_quote(
    price: MonitoringPriceRequest,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_max_quote_age_seconds=30,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 24, 1, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[
            CachedMonitoringItem(
                id="blocked-stop",
                symbol="005930",
                intent="stop_sell",
                status="stop_sell_triggered_blocked",
                trigger_price=Decimal("100"),
                provider="kis",
                exchange="KRX",
            )
        ],
        filled_today_symbols=[],
    )

    payload = coordinator._evaluate_cached_monitoring_cycle(
        runtime_snapshot=snapshot,
        prices=[price],
        started_at=started_at,
    )

    assert payload["rearmable_sell_symbols"] == []


def test_due_cached_max_holding_exit_reaches_db_backed_monitoring_cycle(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_broker_actions_per_cycle=1,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 24, 1, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[
            CachedMonitoringItem(
                id="due-max-holding-exit",
                symbol="005930",
                intent="max_holding_close_sell",
                status="max_holding_close_sell_monitoring",
                trigger_price=None,
                close_exit_date=started_at.date(),
                provider="kis",
                exchange="KRX",
            )
        ],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = snapshot
    db_calls: list[list[str]] = []
    db_evaluated_at: list[datetime] = []
    quote_freshness_at: list[datetime] = []

    class DummySessionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: [
            MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))
        ],
    )
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (0, 0),
    )
    monkeypatch.setattr(
        autonomous_runtime,
        "run_autonomous_monitoring_cycle",
        lambda **kwargs: (
            db_calls.append(list(kwargs["symbols"]))
            or db_evaluated_at.append(kwargs["now"])
            or quote_freshness_at.append(kwargs["quote_freshness_at"])
            or SimpleNamespace(
                status_code=200,
                payload={
                    "triggered_item_count": 1,
                    "submitted_order_count": 0,
                    "items": [],
                },
            )
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: snapshot,
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert db_calls == [["005930"]]
    assert db_evaluated_at == [started_at]
    assert quote_freshness_at[0] > started_at
    assert payload["db_evaluation_symbols"] == ["005930"]
    assert payload["broker_action_symbols"] == []


def test_recovered_cached_sell_block_reaches_db_backed_monitoring_cycle(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_broker_actions_per_cycle=0,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 24, 1, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[
            CachedMonitoringItem(
                id="recovered-stop",
                symbol="005930",
                intent="stop_sell",
                status="stop_sell_triggered_blocked",
                trigger_price=Decimal("100"),
                provider="kis",
                exchange="KRX",
            ),
            CachedMonitoringItem(
                id="same-symbol-triggered-buy",
                symbol="005930",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=Decimal("100"),
                trigger_direction="at_or_above",
                provider="kis",
                exchange="KRX",
            ),
        ],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = snapshot
    db_calls: list[list[str]] = []
    loaded_prices = [
        MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("101"),
            bid=Decimal("101"),
            ask=Decimal("101"),
        )
    ]
    db_result_payload: dict[str, object] = {
        "triggered_item_count": 0,
        "submitted_order_count": 0,
        "items": [
            {
                "execution_plan_item_id": "recovered-stop",
                "symbol": "005930",
                "intent": "stop_sell",
                "status": "stop_sell_monitoring",
                "outcome": "waiting",
            }
        ],
    }

    class DummySessionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: list(loaded_prices),
    )
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (0, 0),
    )
    monkeypatch.setattr(
        autonomous_runtime,
        "run_autonomous_monitoring_cycle",
        lambda **kwargs: (
            db_calls.append(list(kwargs["symbols"]))
            or SimpleNamespace(
                status_code=200,
                payload=dict(db_result_payload),
            )
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: snapshot,
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert db_calls == [["005930"]]
    assert payload["submitted_order_count"] == 0
    assert payload["items"][0]["status"] == "stop_sell_monitoring"
    assert payload["state_rearm_symbols"] == []
    assert payload["state_rearm_candidate_symbols"] == ["005930"]

    coordinator.settings = settings.model_copy(
        update={"monitoring_max_broker_actions_per_cycle": 1}
    )
    rearm_only_snapshot = replace(snapshot, items=[snapshot.items[0]])
    coordinator._monitoring_runtime_snapshot = rearm_only_snapshot
    monkeypatch.setattr(
        coordinator,
        "_refresh_monitoring_runtime_snapshot",
        lambda **kwargs: rearm_only_snapshot,
    )
    db_calls.clear()
    db_result_payload.clear()
    db_result_payload.update(
        {
            "triggered_item_count": 1,
            "submitted_order_count": 1,
            "submitted_symbols": ["005930"],
            "items": [],
        }
    )
    loaded_prices.append(
        MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("102"),
            bid=Decimal("102"),
            ask=Decimal("102"),
        )
    )

    submitted = coordinator._monitoring_cycle_sync(started_at + timedelta(seconds=1))

    assert db_calls == [["005930"]]
    assert submitted["broker_action_symbols"] == ["005930"]
    assert submitted["broker_action_price_count"] == 1


def test_live_cycle_monitors_local_holdings_without_waiting_for_broker_discovery(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 8, 24, 0, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=7)
        },
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = snapshot

    def load_local_symbols() -> list[str]:
        return ["005930"]

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: _core_monitoring_handoff(),
    )
    monkeypatch.setattr(coordinator, "_load_local_desired_monitoring_symbols", load_local_symbols)
    monkeypatch.setattr(coordinator, "_reconcile_monitoring_orders_if_due", lambda **kwargs: None)
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols",
        lambda **kwargs: snapshot,
    )
    monkeypatch.setattr(coordinator, "_refresh_monitoring_snapshot_filled_today", lambda now: None)
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: [MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))],
    )
    monkeypatch.setattr(
        coordinator,
        "_evaluate_cached_monitoring_cycle",
        lambda **kwargs: {"triggered_item_count": 0, "evaluated_item_count": 0},
    )
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (0, 0),
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["monitored_symbols"] == ["005930"]
    assert payload["runtime_snapshot"]["position_count"] == 1


def test_fresh_monitoring_workset_rotates_without_database_access(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_max_symbols_per_cycle=1,
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930", "000660", "035420"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(
                symbol="005930",
                quantity=1,
                provider="kis",
                exchange="KRX",
            )
        },
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = snapshot
    coordinator._monitoring_snapshot_last_reconciled_at = started_at
    coordinator._monitoring_order_reconcile_last_at = started_at
    selected_batches: list[list[str]] = []
    maintenance_calls: list[list[str]] = []

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_monitoring_symbol_handoff",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("fresh workset must not call Core")
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_desired_monitoring_symbols",
        lambda: (_ for _ in ()).throw(
            AssertionError("fresh workset must not query local desired symbols")
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_position_symbols",
        lambda: (_ for _ in ()).throw(
            AssertionError("fresh workset must not query local positions")
        ),
    )
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot_for_symbols",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("fresh workset must not reload plan items")
        ),
    )

    def load_prices(*, symbols: list[str], observed_at: datetime) -> list[MonitoringPriceRequest]:
        del observed_at
        selected_batches.append(list(symbols))
        return [
            MonitoringPriceRequest(symbol=symbol, price=Decimal("70000"))
            for symbol in symbols
        ]

    monkeypatch.setattr(coordinator, "_load_monitoring_prices", load_prices)
    monkeypatch.setattr(
        coordinator,
        "_repair_post_entry_exit_monitors_for_cached_cycle",
        lambda **kwargs: (
            maintenance_calls.append(list(kwargs["runtime_snapshot"].symbols))
            or (0, 0)
        ),
    )

    first = coordinator._monitoring_cycle_sync(started_at)
    second = coordinator._monitoring_cycle_sync(started_at + timedelta(seconds=1))

    assert first["monitored_symbols"] == ["005930", "000660"]
    assert second["monitored_symbols"] == ["005930", "035420"]
    assert selected_batches == [["005930", "000660"], ["005930", "035420"]]
    assert maintenance_calls == [["005930", "000660"], ["005930", "035420"]]
    assert coordinator._monitoring_runtime_snapshot is snapshot


def test_cached_cycle_tightens_day_two_stop_on_price_crossing_inside_snapshot_ttl(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'cached-cycle-stop-tightening.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=1,
        exec_agent_max_account_snapshot_age_seconds=300,
        exec_agent_max_holding_trading_sessions=5,
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "cached-cycle-stop-tightening-secret-32-bytes"
        ),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider=identity.provider,
        risk_context={
            "exchange": "KRX",
            "target_price": "110.0000",
            "stop_price": "80.0000",
        },
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    started_at = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    try:
        with session_factory() as session:
            plan = ExecutionPlan(
                id="cached-cycle-stop-tightening-plan",
                external_plan_id="cached-cycle-stop-tightening-plan",
                source="kai-trading-core",
                market_date=date(2026, 9, 14),
                mode=settings.runtime_mode,
                status="accepted",
                accepted_at=started_at - timedelta(days=1),
                items=[
                    ExecutionPlanItem(
                        id="cached-cycle-stop-tightening-entry",
                        plan_item_index=0,
                        symbol="005930",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="broker_order_submitted",
                        quantity_policy=quantity_policy,
                        trigger_price=Decimal("100.0000"),
                        limit_price=Decimal("100.0000"),
                        time_in_force="day",
                        risk_context=risk_context,
                        source_plan_item_id="cached-cycle-stop-tightening-entry",
                        source_market_date=date(2026, 9, 14),
                        provider=identity.provider,
                        venue="KRX",
                        asset_class="stock",
                    )
                ],
            )
            session.add(plan)
            session.add(
                ExecutionPosition(
                    mode=settings.runtime_mode,
                    provider=identity.provider,
                    account_scope=identity.account_scope,
                    exchange=identity.exchange,
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("100.0000"),
                    updated_from="reconciliation",
                )
            )
            session.add(
                BrokerOrder(
                    execution_plan_item_id="cached-cycle-stop-tightening-entry",
                    broker_name=identity.provider,
                    mode=settings.runtime_mode,
                    idempotency_key="cached-cycle-stop-tightening-entry-order",
                    broker_order_id="cached-cycle-stop-tightening-entry-order",
                    symbol="005930",
                    side="buy",
                    status="filled",
                    quantity=1,
                    venue="KRX",
                    asset_class="stock",
                    limit_price=Decimal("100.0000"),
                    submitted_at=started_at - timedelta(days=1),
                    request_metadata={
                        "exchange": "KRX",
                        "execution_scope": {
                            "policy_version": 1,
                            "provider": identity.provider,
                            "account_scope": identity.account_scope,
                            "venue": "KRX",
                            "asset_class": "stock",
                            "exchange": identity.exchange,
                        },
                    },
                )
            )
            session.commit()

        coordinator = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=session_factory,
            broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
        )
        snapshot = MonitoringRuntimeSnapshot(
            loaded_at=started_at,
            symbols=["005930"],
            positions_by_symbol={
                "005930": CachedMonitoringPosition(
                    symbol="005930",
                    quantity=1,
                    provider=identity.provider,
                    exchange=identity.exchange,
                )
            },
            items=[],
            filled_today_symbols=[],
            execution_scope={
                "policy_version": 1,
                "provider": identity.provider,
                "account_scope": identity.account_scope,
            },
        )
        coordinator._monitoring_runtime_snapshot = snapshot
        observed_prices = iter(
            [
                [MonitoringPriceRequest(symbol="005930", price=Decimal("101.0000"))],
                [MonitoringPriceRequest(symbol="005930", price=Decimal("100.0000"))],
            ]
        )

        monkeypatch.setattr(
            coordinator,
            "_reconcile_monitoring_orders_if_due",
            lambda **kwargs: False,
        )
        monkeypatch.setattr(
            coordinator,
            "_load_monitoring_prices",
            lambda **kwargs: next(observed_prices),
        )
        monkeypatch.setattr(
            coordinator,
            "_refresh_monitoring_runtime_snapshot",
            lambda **kwargs: snapshot,
        )

        first = coordinator._monitoring_cycle_sync(started_at)
        second = coordinator._monitoring_cycle_sync(started_at + timedelta(seconds=1))

        assert first["post_entry_maintenance_count"] >= 1
        assert second["post_entry_maintenance_count"] >= 1
        with session_factory() as session:
            stop_item = session.query(ExecutionPlanItem).filter_by(
                symbol="005930",
                side="sell",
                intent="stop_sell",
            ).one()
            assert stop_item.trigger_price == Decimal("99.0000")
            assert stop_item.risk_context["post_entry_review"]["reasons"] == [
                "next_day_no_progress"
            ]
    finally:
        engine.dispose()


def test_monitoring_snapshot_refreshes_once_when_cache_ttl_expires(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    loaded_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    initial = MonitoringRuntimeSnapshot(
        loaded_at=loaded_at,
        symbols=["005930"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    coordinator._monitoring_runtime_snapshot = initial
    refreshes: list[tuple[datetime, str]] = []

    def refresh(*, loaded_at: datetime, reason: str, reconcile_broker: bool):
        assert reconcile_broker is False
        refreshes.append((loaded_at, reason))
        refreshed = replace(initial, loaded_at=loaded_at, symbols=["000660"])
        coordinator._monitoring_runtime_snapshot = refreshed
        return refreshed

    monkeypatch.setattr(coordinator, "_refresh_monitoring_runtime_snapshot", refresh)

    assert coordinator._ensure_monitoring_runtime_snapshot(
        loaded_at + timedelta(seconds=299)
    ) is initial
    refreshed = coordinator._ensure_monitoring_runtime_snapshot(
        loaded_at + timedelta(seconds=300)
    )
    reused = coordinator._ensure_monitoring_runtime_snapshot(
        loaded_at + timedelta(seconds=301)
    )

    assert refreshed.symbols == ["000660"]
    assert reused is refreshed
    assert refreshes == [
        (
            loaded_at + timedelta(seconds=300),
            "monitoring_snapshot_ttl_expired",
        )
    ]


def test_monitoring_snapshot_invalidation_forces_one_reload_inside_ttl(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    loaded_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=loaded_at,
        symbols=["005930"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    refresh_count = 0

    def refresh(*, loaded_at: datetime, reason: str, reconcile_broker: bool):
        nonlocal refresh_count
        assert reason == "monitoring_snapshot_missing"
        assert reconcile_broker is False
        refresh_count += 1
        refreshed = MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=["005930", "000660"],
            positions_by_symbol={},
            items=[],
            filled_today_symbols=[],
        )
        coordinator._monitoring_runtime_snapshot = refreshed
        return refreshed

    monkeypatch.setattr(coordinator, "_refresh_monitoring_runtime_snapshot", refresh)

    coordinator.invalidate_monitoring_runtime_snapshot(reason="test_plan_change")
    first = coordinator._ensure_monitoring_runtime_snapshot(
        loaded_at + timedelta(seconds=1)
    )
    second = coordinator._ensure_monitoring_runtime_snapshot(
        loaded_at + timedelta(seconds=2)
    )

    assert first.symbols == ["005930", "000660"]
    assert second is first
    assert refresh_count == 1


def test_monitoring_snapshot_refreshes_publish_in_db_observation_order(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    first_load_entered = threading.Event()
    release_first_load = threading.Event()
    state_lock = threading.Lock()
    database_version = 0
    load_count = 0
    errors: list[Exception] = []

    def load_snapshot(loaded_at: datetime) -> MonitoringRuntimeSnapshot:
        nonlocal load_count
        with state_lock:
            observed_version = database_version
            load_count += 1
            current_load = load_count
        if current_load == 1:
            first_load_entered.set()
            assert release_first_load.wait(timeout=2)
        return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=[f"version-{observed_version}"],
            positions_by_symbol={},
            items=[],
            filled_today_symbols=[],
        )

    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_runtime_snapshot",
        load_snapshot,
    )

    def refresh(loaded_at: datetime) -> None:
        try:
            coordinator._refresh_monitoring_runtime_snapshot(
                loaded_at=loaded_at,
                reason="concurrent_refresh_test",
                reconcile_broker=False,
            )
        except Exception as exc:
            errors.append(exc)

    later_cycle = datetime(2026, 9, 23, 1, 1, tzinfo=UTC)
    earlier_cycle = later_cycle - timedelta(minutes=1)
    stale_refresh = threading.Thread(target=refresh, args=(later_cycle,))
    fresh_refresh = threading.Thread(target=refresh, args=(earlier_cycle,))

    stale_refresh.start()
    assert first_load_entered.wait(timeout=2)
    with state_lock:
        database_version = 1
    fresh_refresh.start()
    release_first_load.set()
    stale_refresh.join(timeout=2)
    fresh_refresh.join(timeout=2)

    assert not stale_refresh.is_alive()
    assert not fresh_refresh.is_alive()
    assert errors == []
    assert load_count == 2
    assert coordinator._monitoring_runtime_snapshot is not None
    assert coordinator._monitoring_runtime_snapshot.symbols == ["version-1"]


def test_dispatch_skips_database_until_idle_rescan_deadline(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(
        "app.services.autonomous_runtime.dispatch_due_execution_jobs",
        lambda **kwargs: SimpleNamespace(
            status_code=200,
            payload={
                "blocked": False,
                "due_job_count": 0,
                "completed_job_count": 0,
                "retrying_job_count": 0,
                "failed_job_count": 0,
                "jobs": [],
                "next_dispatch_at": None,
            },
        ),
    )

    first = coordinator._dispatch_cycle_sync(started_at)
    cached = coordinator._dispatch_cycle_sync(started_at + timedelta(seconds=1))
    rescanned = coordinator._dispatch_cycle_sync(started_at + timedelta(seconds=60))

    assert first["due_job_count"] == 0
    assert cached["db_scan_skipped"] is True
    assert cached["due_job_count"] == 0
    assert rescanned["due_job_count"] == 0
    assert session_open_count == 2


def test_dispatch_reopens_database_when_known_scheduled_job_becomes_due(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    scheduled_for = started_at + timedelta(seconds=30)
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(
        "app.services.autonomous_runtime.dispatch_due_execution_jobs",
        lambda **kwargs: SimpleNamespace(
            status_code=200,
            payload={
                "blocked": False,
                "due_job_count": 0,
                "completed_job_count": 0,
                "retrying_job_count": 0,
                "failed_job_count": 0,
                "jobs": [],
                "next_dispatch_at": scheduled_for.isoformat(),
            },
        ),
    )

    coordinator._dispatch_cycle_sync(started_at)
    cached = coordinator._dispatch_cycle_sync(started_at + timedelta(seconds=29))
    coordinator._dispatch_cycle_sync(scheduled_for)

    assert cached["db_scan_skipped"] is True
    assert cached["next_db_check_at"] == scheduled_for.isoformat()
    assert session_open_count == 2


def test_repeated_no_price_cycles_skip_database_after_first_checkpoint(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(coordinator, "_load_monitoring_prices", lambda **kwargs: [])
    monkeypatch.setattr(
        "app.services.autonomous_runtime.run_autonomous_monitoring_cycle",
        lambda **kwargs: SimpleNamespace(
            status_code=200,
            payload={
                "triggered_at": kwargs["now"].isoformat(),
                "worker_name": "intraday_target_stop_monitor",
                "evaluated_item_count": 0,
                "triggered_item_count": 0,
                "submitted_order_count": 0,
                "blocked_item_count": 0,
                "deferred_item_count": 0,
                "evaluated_symbol_count": 0,
                "triggered_symbol_count": 0,
                "submitted_symbols": [],
                "items": [],
            },
        ),
    )

    first = coordinator._monitoring_cycle_sync(started_at)
    cached = coordinator._monitoring_cycle_sync(
        started_at + timedelta(seconds=1)
    )

    assert first.get("db_scan_skipped") is not True
    assert first["db_evaluation_symbols"] == ["005930"]
    assert first["broker_action_symbols"] == []
    assert first["broker_action_trace_scope"] == "confirmed_submissions"
    assert cached["db_scan_skipped"] is True
    assert session_open_count == 1


def test_no_price_cycle_retries_max_holding_pending_dispatch_without_idle_wait(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={},
        items=[
            CachedMonitoringItem(
                id="max-holding-pending-dispatch",
                symbol="005930",
                intent="max_holding_close_sell",
                status="max_holding_close_sell_triggered_pending_dispatch",
                trigger_price=None,
            )
        ],
        filled_today_symbols=[],
    )
    coordinator._monitoring_next_no_price_db_check_at = started_at + timedelta(
        seconds=60
    )
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(coordinator, "_load_monitoring_prices", lambda **kwargs: [])
    monkeypatch.setattr(
        "app.services.autonomous_runtime.run_autonomous_monitoring_cycle",
        lambda **kwargs: SimpleNamespace(
            status_code=200,
            payload={
                "triggered_at": kwargs["now"].isoformat(),
                "worker_name": "intraday_target_stop_monitor",
                "evaluated_item_count": 1,
                "triggered_item_count": 0,
                "submitted_order_count": 0,
                "blocked_item_count": 0,
                "deferred_item_count": 1,
                "evaluated_symbol_count": 1,
                "triggered_symbol_count": 0,
                "submitted_symbols": [],
                "items": [],
            },
        ),
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload.get("db_scan_skipped") is not True
    assert payload["db_evaluation_symbols"] == ["005930"]
    assert payload["broker_action_symbols"] == []
    assert payload["broker_action_trace_scope"] == "confirmed_submissions"
    assert session_open_count == 1


def test_no_price_cycle_does_not_retry_terminal_sell_before_idle_rescan(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    coordinator._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={},
        items=[
            CachedMonitoringItem(
                id="terminal-max-holding",
                symbol="005930",
                intent="max_holding_close_sell",
                status="cancelled",
                trigger_price=None,
            )
        ],
        filled_today_symbols=[],
    )
    next_scan_at = started_at + timedelta(seconds=60)
    coordinator._monitoring_next_no_price_db_check_at = next_scan_at
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(coordinator, "_load_monitoring_prices", lambda **kwargs: [])

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["db_scan_skipped"] is True
    assert payload["next_db_check_at"] == next_scan_at.isoformat()
    assert session_open_count == 0


def test_dirty_dispatch_schedule_reloads_before_idle_rescan_deadline(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: object(),
    )
    started_at = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)
    session_open_count = 0

    class DummySessionContext:
        def __enter__(self):
            nonlocal session_open_count
            session_open_count += 1
            return object()

        def __exit__(self, exc_type, exc, traceback) -> None:
            del exc_type, exc, traceback

    coordinator._cycle_session_factory = lambda: DummySessionContext()  # type: ignore[method-assign]
    monkeypatch.setattr(
        "app.services.autonomous_runtime.dispatch_due_execution_jobs",
        lambda **kwargs: SimpleNamespace(
            status_code=200,
            payload={
                "blocked": False,
                "due_job_count": 0,
                "completed_job_count": 0,
                "retrying_job_count": 0,
                "failed_job_count": 0,
                "jobs": [],
                "next_dispatch_at": None,
            },
        ),
    )

    coordinator._dispatch_cycle_sync(started_at)
    coordinator.invalidate_dispatch_schedule(reason="execution_plan_created")
    coordinator._dispatch_cycle_sync(started_at + timedelta(seconds=1))

    assert session_open_count == 2


def test_non_broker_snapshot_refresh_does_not_suppress_future_position_reconciliation(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    loaded_at = datetime(2026, 8, 24, 0, 0, tzinfo=UTC)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=loaded_at,
        symbols=[],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=[],
    )
    monkeypatch.setattr(coordinator, "_load_monitoring_runtime_snapshot", lambda now: snapshot)

    coordinator._refresh_monitoring_runtime_snapshot(
        loaded_at=loaded_at,
        reason="plan_only_refresh",
        reconcile_broker=False,
    )

    assert coordinator._monitoring_snapshot_last_reconciled_at is None


def test_live_monitoring_cycle_fails_closed_and_retries_after_position_sync_failure(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: object(),  # type: ignore[arg-type,return-value]
    )
    started_at = datetime(2026, 8, 24, 0, 0, tzinfo=UTC)
    stale_reconciled_at = started_at - timedelta(hours=1)
    snapshot = MonitoringRuntimeSnapshot(
        loaded_at=started_at,
        symbols=["005930"],
        positions_by_symbol={
            "005930": CachedMonitoringPosition(symbol="005930", quantity=7)
        },
        items=[],
        filled_today_symbols=[],
    )
    sync_attempts: list[datetime] = []

    def fail_position_sync(**kwargs) -> None:
        del kwargs
        sync_attempts.append(started_at)
        raise RuntimeError("broker snapshot unavailable")

    coordinator._monitoring_snapshot_last_reconciled_at = stale_reconciled_at
    monkeypatch.setattr(
        "app.services.autonomous_runtime._sync_positions_for_monitoring_routes",
        fail_position_sync,
    )
    monkeypatch.setattr(coordinator, "_load_monitoring_runtime_snapshot", lambda now: snapshot)

    with pytest.raises(RuntimeError, match="live broker position reconciliation failed"):
        coordinator._monitoring_cycle_sync(started_at)

    assert coordinator._monitoring_snapshot_last_reconciled_at == stale_reconciled_at

    with pytest.raises(RuntimeError, match="live broker position reconciliation failed"):
        coordinator._monitoring_cycle_sync(started_at + timedelta(seconds=1))

    assert coordinator._monitoring_snapshot_last_reconciled_at == stale_reconciled_at
    assert len(sync_attempts) == 2
    engine.dispose()


def test_local_desired_symbols_fall_back_to_orm_when_sqlite_fast_path_is_unavailable(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-symbol-fallback.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=4,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    try:
        with session_factory() as session:
            session.add(
                ExecutionPlan(
                    external_plan_id="orm-monitoring-symbol-fallback",
                    source="kai-trading-core",
                    market_date=date(2026, 8, 14),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            plan_item_index=0,
                            symbol="005930",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                        )
                    ],
                )
            )
            session.commit()

        monkeypatch.setattr(
            coordinator,
            "_load_local_desired_monitoring_symbols_from_sqlite",
            lambda: [],
        )

        assert coordinator._load_local_desired_monitoring_symbols() == ["005930"]
    finally:
        engine.dispose()


def test_orm_monitoring_symbols_include_kis_us_and_exclude_alpaca() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=8,
        monitoring_expire_prior_market_watch_buys_enabled=False,
        kis_us_trading_enabled=True,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="mixed-provider-monitoring-symbols",
                source="test",
                market_date=date(2026, 8, 14),
                mode="paper",
                status="accepted",
                items=[
                    ExecutionPlanItem(
                        plan_item_index=0,
                        symbol="005930",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_monitoring_active",
                    ),
                    ExecutionPlanItem(
                        plan_item_index=1,
                        symbol="AAPL",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_monitoring_active",
                        provider="kis",
                        venue="kis",
                        asset_class="us_equity",
                        risk_context={"exchange": "NASD"},
                    ),
                    ExecutionPlanItem(
                        plan_item_index=2,
                        symbol="MSFT",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_monitoring_active",
                        provider="alpaca",
                        venue="alpaca",
                        asset_class="us_equity",
                        risk_context={"exchange": "NASD"},
                    ),
                ],
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("200"),
                updated_from="reconciliation",
                raw_payload={
                    "_position_sync": {
                        "asset_class": "us_equity",
                        "exchange": "NASD",
                        "monitoring_eligible": True,
                    }
                },
            )
        )
        session.commit()

    assert set(coordinator._load_local_desired_monitoring_symbols()) == {"005930", "AAPL"}
    engine.dispose()


def test_live_snapshot_refresh_discovers_enabled_kis_us_holdings(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_us_trading_enabled=True,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: object(),  # type: ignore[return-value]
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "app.services.autonomous_runtime._sync_positions_for_monitoring_routes",
        lambda **kwargs: calls.append(kwargs),
    )

    coordinator._refresh_monitoring_runtime_snapshot(
        loaded_at=datetime(2026, 8, 26, 1, 0, tzinfo=UTC),
        reason="test",
        reconcile_broker=True,
    )

    assert calls[0]["discover_krx_holdings"] is True
    assert calls[0]["discover_us_holdings"] is True
    engine.dispose()


def test_disabled_kis_us_monitoring_skips_us_plan_scan(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_trading_enabled=False,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    monkeypatch.setattr(
        "app.services.monitoring._kis_us_plan_item_filter",
        lambda: (_ for _ in ()).throw(
            AssertionError("US plan scan must be disabled with KIS US trading")
        ),
    )

    with session_factory() as db:
        assert list_autonomous_monitoring_symbols(
            settings=settings,
            db=db,
            effective_at=datetime(2026, 9, 23, 1, 0, tzinfo=UTC),
            include_kis_us_symbols=False,
        ) == []

    engine.dispose()


def test_local_position_symbols_fall_back_to_orm_when_sqlite_fast_path_is_unavailable(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-position-fallback.db'}",
        runtime_mode="live",
        execution_position_account_scope="autonomous-runtime-test",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    try:
        with session_factory() as session:
            session.add_all(
                [
                    ExecutionPosition(
                        mode="live",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="030000",
                        quantity=24,
                    ),
                    ExecutionPosition(
                        mode="live",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=0,
                    ),
                    ExecutionPosition(mode="paper", symbol="000660", quantity=3),
                ]
            )
            session.commit()

        monkeypatch.setattr(
            coordinator,
            "_load_local_position_symbols_from_sqlite",
            lambda: [],
        )

        assert coordinator._load_local_position_symbols() == ["030000"]
    finally:
        engine.dispose()


def test_krx_monitoring_symbols_exclude_us_positions(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'krx-market-isolation.db'}",
        runtime_mode="live",
        execution_position_account_scope="autonomous-market-isolation-test",
        monitoring_max_symbols_per_cycle=6,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other_us = build_position_identity(settings=settings, provider="alpaca", exchange="US")
    watch_risk, watch_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    try:
        with session_factory() as session:
            session.add_all(
                [
                    ExecutionPosition(
                        mode="live",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="030000",
                        quantity=24,
                    ),
                    ExecutionPosition(
                        mode="live",
                        provider=other_us.provider,
                        account_scope=other_us.account_scope,
                        exchange=other_us.exchange,
                        symbol="AFIB",
                        quantity=64,
                    ),
                    ExecutionPosition(
                        mode="live",
                        provider=other_us.provider,
                        account_scope=other_us.account_scope,
                        exchange=other_us.exchange,
                        symbol="FRTX/CVR1",
                        quantity=45,
                    ),
                    ExecutionPlan(
                        external_plan_id="krx-market-isolation",
                        source="kai-trading-core",
                        market_date=date(2026, 8, 26),
                        mode="live",
                        status="accepted",
                        items=[
                            ExecutionPlanItem(
                                plan_item_index=0,
                                symbol="005930",
                                side="buy",
                                    intent="intraday_watch_buy",
                                    status="watch_monitoring_active",
                                    provider="kis",
                                    asset_class="stock",
                                    risk_context=watch_risk,
                                    quantity_policy=watch_policy,
                                )
                        ],
                    ),
                ]
            )
            session.commit()

            symbols = list_autonomous_monitoring_symbols(
                settings=settings,
                db=session,
                current_market_date=date(2026, 8, 26),
            )

        assert symbols == ["030000", "005930"]
    finally:
        engine.dispose()
