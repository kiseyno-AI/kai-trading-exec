import asyncio

from app.core.config import Settings
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator


def test_monitoring_timeout_waits_for_original_cycle_before_next_start() -> None:
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

            assert calls == 1
            timeout_snapshot = coordinator.snapshot()["monitoring"]
            assert timeout_snapshot["status"] == "error"
            assert timeout_snapshot["last_error"] == "cycle timed out after 0.05s"
            assert timeout_snapshot["last_cycle"]["reason"] == "monitoring_cycle_timed_out"

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
