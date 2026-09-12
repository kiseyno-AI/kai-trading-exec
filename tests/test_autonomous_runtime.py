import asyncio
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services import autonomous_runtime
from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.autonomous_runtime import (
    AutonomousRuntimeCoordinator,
    CachedMonitoringItem,
    CachedMonitoringPosition,
    MonitoringRuntimeSnapshot,
    _is_krx_symbol,
    _select_monitoring_symbol_batch,
)
from app.services.monitoring import list_autonomous_monitoring_symbols
from app.services.position_identity import build_position_identity
from app.services.execution_account_route import bind_kis_account_route
from app.services.trigger_time_exec_agent import _run_exec_agent_command


async def _wait_for_thread_event(event: threading.Event) -> None:
    async with asyncio.timeout(1):
        while not event.is_set():
            await asyncio.sleep(0.001)


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
            "version": "trade-outcome-feedback-v2",
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
        "version": "trade-outcome-feedback-v2",
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
    requested: list[str] = []
    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_desired_monitoring_symbols",
        lambda settings: ["100001", "100002", "100003"],
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
    prices = [MonitoringPriceRequest(symbol="005930", price=Decimal("70000"))]
    maintenance_calls: list[str] = []

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_desired_monitoring_symbols",
        lambda settings: [],
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
        lambda **kwargs: maintenance_calls.append("called") or 0,
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["status_code"] == 200
    assert maintenance_calls == ["called"]


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

    def load_local_symbols() -> list[str]:
        return ["005930"]

    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_desired_monitoring_symbols",
        lambda settings: [],
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
        lambda **kwargs: 0,
    )

    payload = coordinator._monitoring_cycle_sync(started_at)

    assert payload["monitored_symbols"] == ["005930"]
    assert payload["runtime_snapshot"]["position_count"] == 1


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
