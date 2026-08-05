import asyncio
import os
import threading
from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator


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
