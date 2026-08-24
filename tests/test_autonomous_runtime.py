import asyncio
import os
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services.autonomous_runtime import (
    AutonomousRuntimeCoordinator,
    CachedMonitoringPosition,
    MonitoringRuntimeSnapshot,
)


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
            session.add(plan)
            session.commit()

        assert coordinator._load_local_desired_monitoring_symbols_from_sqlite() == ["111111"]
        snapshot = coordinator._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
            loaded_at=datetime(2026, 8, 18, 0, 10, tzinfo=UTC),
            symbols=["111111", "222222", "333333", "444444", "555555"],
        )
        assert snapshot is not None
        assert [item.symbol for item in snapshot.items] == ["111111"]
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
    monkeypatch.setattr(coordinator, "_load_local_position_symbols", load_local_symbols)
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
