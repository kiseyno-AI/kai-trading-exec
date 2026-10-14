from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import threading
from types import SimpleNamespace

import pytest
from sqlalchemy import event, select, text

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import (
    _shutdown_runtime_coordinators,
    _stop_runtime_coordinators,
)
from app.models.runtime import RuntimeControlEvent, RuntimeControlState
from app.services import runtime_controls as runtime_controls_service
from app.services.runtime_controls import (
    ControlCommand,
    enable_kill_switch,
    ensure_runtime_control_state,
    pause_runtime,
    recover_legacy_lifespan_shutdown_pause,
    reset_kill_switch,
    resume_runtime,
)
from app.services.alpaca_crypto_runtime import AlpacaCryptoRuntimeCoordinator
from app.services.alpaca_us_equity_runtime import AlpacaUsEquityRuntimeCoordinator
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
from app.services.runtime_shutdown import (
    CommitGuardedSessionFactory,
    GuardedBrokerAdapter,
    RuntimeCycleFence,
)
from app.services.position_identity import EXECUTION_SCOPE_POLICY_VERSION
from app.services.plan_judgment_actions import apply_hourly_plan_judgment_action


def test_lifespan_shutdown_preserves_resumed_runtime_control_state(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'shutdown-pause.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    calls: list[str] = []

    class Coordinator:
        def begin_shutdown(self) -> None:
            calls.append("admission_closed")

        async def stop(self) -> None:
            calls.append("coordinator_stopped")

    try:
        with session_factory() as db:
            ensure_runtime_control_state(settings=settings, db=db)
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )

        asyncio.run(
            _shutdown_runtime_coordinators(
                coordinators=(("runtime", Coordinator()),),
            )
        )

        with session_factory() as db:
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            assert state.paused is False
            assert state.kill_switch_enabled is False
            assert state.last_transition_source == "test"
            shutdown_event = db.scalar(
                select(RuntimeControlEvent)
                .where(RuntimeControlEvent.source == "lifespan_shutdown")
                .order_by(RuntimeControlEvent.occurred_at.desc())
            )
            assert shutdown_event is None
        assert calls == ["admission_closed", "coordinator_stopped"]
    finally:
        engine.dispose()


@pytest.mark.parametrize("initial_state", ["operator_pause", "kill_switch"])
def test_lifespan_shutdown_preserves_operator_control_state(
    initial_state: str,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'shutdown-{initial_state}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            ensure_runtime_control_state(settings=settings, db=db)
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )
            if initial_state == "operator_pause":
                result = pause_runtime(
                    settings=settings,
                    db=db,
                    command=ControlCommand(
                        source="operator_test",
                        reason="prepare operator pause",
                    ),
                )
            else:
                result = enable_kill_switch(
                    settings=settings,
                    db=db,
                    command=ControlCommand(
                        source="operator_test",
                        reason="prepare kill-switch state",
                    ),
                )
            assert result.outcome == "applied"
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            prior_transition_source = state.last_transition_source
            prior_transition_event_id = state.last_transition_event_id

        asyncio.run(
            _shutdown_runtime_coordinators(
                coordinators=(),
            )
        )

        with session_factory() as db:
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            assert state.paused is True
            assert state.kill_switch_enabled is (initial_state == "kill_switch")
            assert state.last_transition_source == prior_transition_source
            assert state.last_transition_event_id == prior_transition_event_id
            shutdown_event = db.scalar(
                select(RuntimeControlEvent)
                .where(RuntimeControlEvent.source == "lifespan_shutdown")
                .order_by(RuntimeControlEvent.occurred_at.desc())
            )
            assert shutdown_event is None
    finally:
        engine.dispose()


def test_startup_recovers_only_legacy_lifespan_shutdown_pause(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'legacy-shutdown-pause.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            state = ensure_runtime_control_state(settings=settings, db=db)
            state.paused = True
            state.kill_switch_enabled = False
            state.last_transition_action = "pause"
            state.last_transition_source = "lifespan_shutdown"
            state.last_transition_reason = "service lifespan shutdown started"
            state.readiness_reason = "paused by operator; monitoring remains active"
            legacy_event_id = "legacy-shutdown-event"
            state.last_transition_event_id = legacy_event_id
            db.add(
                RuntimeControlEvent(
                    id=legacy_event_id,
                    runtime_control_state_id=state.id,
                    action="pause",
                    outcome="applied",
                    actor=settings.service_name,
                    source="lifespan_shutdown",
                    reason="service lifespan shutdown started",
                    message="runtime paused; monitoring remains active",
                    previous_state={"paused": False, "kill_switch_enabled": False},
                    current_state={
                        "paused": True,
                        "kill_switch_enabled": False,
                        "automation_enabled": False,
                        "readiness_reason": "paused by operator; monitoring remains active",
                    },
                    occurred_at=datetime.now(UTC),
                )
            )
            db.commit()

        with session_factory() as db:
            result = recover_legacy_lifespan_shutdown_pause(settings=settings, db=db)
            assert result is not None
            assert result.changed is True
            recovered = db.get(RuntimeControlState, "global")
            assert recovered is not None
            assert recovered.paused is False
            assert recovered.kill_switch_enabled is False
            assert recovered.last_transition_action == "legacy_shutdown_pause_recovery"
            assert recovered.last_transition_source == "startup_recovery"
            assert recovered.last_transition_event_id != legacy_event_id
            event = db.get(RuntimeControlEvent, recovered.last_transition_event_id)
            assert event is not None
            assert event.action == "legacy_shutdown_pause_recovery"
            assert event.outcome == "applied"
            assert event.previous_state["paused"] is True
            assert event.current_state["paused"] is False
    finally:
        engine.dispose()


def test_current_pause_cannot_impersonate_legacy_shutdown_pause(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'current-pause-spoof.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            ensure_runtime_control_state(settings=settings, db=db)
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )
            pause = pause_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(
                    actor=settings.service_name,
                    source="lifespan_shutdown",
                    reason="service lifespan shutdown started",
                ),
            )
            assert pause.outcome == "applied"

        with session_factory() as db:
            recovered = recover_legacy_lifespan_shutdown_pause(
                settings=settings,
                db=db,
            )
            state = db.get(RuntimeControlState, "global")
            assert recovered is None
            assert state is not None
            assert state.paused is True
            assert state.last_transition_action == "pause"
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "tampered_evidence",
    ["message", "readiness_reason", "automation_enabled"],
)
def test_startup_recovery_requires_complete_legacy_shutdown_evidence(
    tmp_path,
    tampered_evidence: str,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'legacy-tampered-{tampered_evidence}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    legacy_readiness = "paused by operator; monitoring remains active"
    try:
        with session_factory() as db:
            state = ensure_runtime_control_state(settings=settings, db=db)
            state.paused = True
            state.kill_switch_enabled = False
            state.readiness_reason = (
                "operator pause must survive restart"
                if tampered_evidence == "readiness_reason"
                else legacy_readiness
            )
            state.last_transition_action = "pause"
            state.last_transition_source = "lifespan_shutdown"
            state.last_transition_reason = "service lifespan shutdown started"
            legacy_event_id = f"legacy-tampered-{tampered_evidence}"
            state.last_transition_event_id = legacy_event_id
            db.add(
                RuntimeControlEvent(
                    id=legacy_event_id,
                    runtime_control_state_id=state.id,
                    action="pause",
                    outcome="applied",
                    actor=settings.service_name,
                    source="lifespan_shutdown",
                    reason="service lifespan shutdown started",
                    message=(
                        "current pause message"
                        if tampered_evidence == "message"
                        else "runtime paused; monitoring remains active"
                    ),
                    previous_state={"paused": False, "kill_switch_enabled": False},
                    current_state={
                        "paused": True,
                        "kill_switch_enabled": False,
                        "automation_enabled": (
                            True if tampered_evidence == "automation_enabled" else False
                        ),
                        "readiness_reason": legacy_readiness,
                    },
                    occurred_at=datetime.now(UTC),
                )
            )
            db.commit()

        with session_factory() as db:
            recovered = recover_legacy_lifespan_shutdown_pause(
                settings=settings,
                db=db,
            )
            state = db.get(RuntimeControlState, "global")
            assert recovered is None
            assert state is not None
            assert state.paused is True
            assert state.last_transition_event_id == legacy_event_id
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("transition_source", "kill_switch_enabled"),
    [("operator", False), ("lifespan_shutdown", True)],
)
def test_startup_recovery_preserves_operator_pause_and_kill_switch(
    tmp_path,
    transition_source: str,
    kill_switch_enabled: bool,
) -> None:
    settings = Settings(
        database_url=(
            f"sqlite:///{tmp_path / f'preserve-{transition_source}-{kill_switch_enabled}.db'}"
        ),
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            state = ensure_runtime_control_state(settings=settings, db=db)
            state.paused = True
            state.kill_switch_enabled = kill_switch_enabled
            state.last_transition_action = "kill_switch" if kill_switch_enabled else "pause"
            state.last_transition_source = transition_source
            state.last_transition_event_id = "preserved-control-event"
            db.commit()

        with session_factory() as db:
            result = recover_legacy_lifespan_shutdown_pause(settings=settings, db=db)
            assert result is None
            preserved = ensure_runtime_control_state(settings=settings, db=db)
            assert preserved.paused is True
            assert preserved.kill_switch_enabled is kill_switch_enabled
            assert preserved.last_transition_source == transition_source
            assert preserved.last_transition_event_id == "preserved-control-event"
            assert db.scalar(
                select(RuntimeControlEvent).where(
                    RuntimeControlEvent.action == "legacy_shutdown_pause_recovery"
                )
            ) is None
    finally:
        engine.dispose()


@pytest.mark.parametrize("racing_transition", ["pause", "resume", "legacy_recovery"])
def test_applied_control_transition_cannot_overwrite_concurrent_kill_switch(
    tmp_path,
    monkeypatch,
    racing_transition: str,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'kill-race-{racing_transition}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    legacy_readiness = "paused by operator; monitoring remains active"
    with session_factory() as db:
        state = ensure_runtime_control_state(settings=settings, db=db)
        if racing_transition == "pause":
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )
        elif racing_transition == "legacy_recovery":
            state.paused = True
            state.kill_switch_enabled = False
            state.readiness_reason = legacy_readiness
            state.last_transition_action = "pause"
            state.last_transition_source = "lifespan_shutdown"
            state.last_transition_reason = "service lifespan shutdown started"
            state.last_transition_event_id = "legacy-race-event"
            db.add(
                RuntimeControlEvent(
                    id="legacy-race-event",
                    runtime_control_state_id=state.id,
                    action="pause",
                    outcome="applied",
                    actor=settings.service_name,
                    source="lifespan_shutdown",
                    reason="service lifespan shutdown started",
                    message="runtime paused; monitoring remains active",
                    previous_state={"paused": False, "kill_switch_enabled": False},
                    current_state={
                        "paused": True,
                        "kill_switch_enabled": False,
                        "automation_enabled": False,
                        "readiness_reason": legacy_readiness,
                    },
                    occurred_at=datetime.now(UTC),
                )
            )
            db.commit()

    stale_snapshot_loaded = threading.Event()
    release_stale_transition = threading.Event()
    original_state_snapshot = runtime_controls_service._state_snapshot
    delayed = False

    def delay_stale_transition(state: RuntimeControlState) -> dict[str, object]:
        nonlocal delayed
        snapshot = original_state_snapshot(state)
        if threading.current_thread().name == "stale-control-transition" and not delayed:
            delayed = True
            stale_snapshot_loaded.set()
            assert release_stale_transition.wait(timeout=5)
        return snapshot

    monkeypatch.setattr(
        runtime_controls_service,
        "_state_snapshot",
        delay_stale_transition,
    )
    transition_errors: list[BaseException] = []

    def apply_stale_transition() -> None:
        try:
            with session_factory() as db:
                if racing_transition == "pause":
                    pause_runtime(
                        settings=settings,
                        db=db,
                        command=ControlCommand(source="test", reason="race pause"),
                    )
                elif racing_transition == "resume":
                    resume_runtime(
                        settings=settings,
                        db=db,
                        command=ControlCommand(source="test", reason="race resume"),
                    )
                else:
                    recover_legacy_lifespan_shutdown_pause(settings=settings, db=db)
        except BaseException as exc:  # pragma: no cover - asserted below
            transition_errors.append(exc)

    thread = threading.Thread(
        target=apply_stale_transition,
        name="stale-control-transition",
    )
    thread.start()
    assert stale_snapshot_loaded.wait(timeout=5)
    with session_factory() as db:
        kill = enable_kill_switch(
            settings=settings,
            db=db,
            command=ControlCommand(source="test", reason="concurrent kill wins"),
        )
    assert kill.outcome == "applied"
    release_stale_transition.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert transition_errors == []
    with session_factory() as db:
        state = db.get(RuntimeControlState, "global")
        assert state is not None
        assert state.kill_switch_enabled is True
        assert state.paused is True
        assert state.last_transition_action == "kill_switch"
        assert state.last_transition_event_id == kill.event_id
        reset = reset_kill_switch(
            settings=settings,
            db=db,
            command=ControlCommand(source="test", reason="verify reset remains possible"),
            expected_current_event_id=kill.event_id,
        )
        assert reset.outcome == "applied"
    engine.dispose()


@pytest.mark.parametrize(
    ("racing_transition", "expected_previous_paused", "expected_previous_reason"),
    [
        (
            "pause",
            True,
            "new entries paused by operator; monitoring and risk-reducing exits remain active",
        ),
        ("resume", False, None),
    ],
)
def test_kill_switch_retries_after_concurrent_control_transition_with_exact_audit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    racing_transition: str,
    expected_previous_paused: bool,
    expected_previous_reason: str | None,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'kill-audit-race-{racing_transition}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        ensure_runtime_control_state(settings=settings, db=db)
        if racing_transition == "pause":
            resumed = resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )
            assert resumed.outcome == "applied"

    stale_snapshot_loaded = threading.Event()
    release_kill_switch = threading.Event()
    original_state_snapshot = runtime_controls_service._state_snapshot
    delayed = False

    def delay_kill_switch_after_snapshot(
        state: RuntimeControlState,
    ) -> dict[str, object]:
        nonlocal delayed
        snapshot = original_state_snapshot(state)
        if threading.current_thread().name == "stale-kill-switch" and not delayed:
            delayed = True
            stale_snapshot_loaded.set()
            assert release_kill_switch.wait(timeout=5)
        return snapshot

    monkeypatch.setattr(
        runtime_controls_service,
        "_state_snapshot",
        delay_kill_switch_after_snapshot,
    )
    kill_results: list[object] = []
    kill_errors: list[BaseException] = []

    def apply_stale_kill_switch() -> None:
        try:
            with session_factory() as db:
                kill_results.append(
                    enable_kill_switch(
                        settings=settings,
                        db=db,
                        command=ControlCommand(
                            source="test",
                            reason="kill must win after concurrent control change",
                        ),
                    )
                )
        except BaseException as exc:  # pragma: no cover - asserted below
            kill_errors.append(exc)

    thread = threading.Thread(
        target=apply_stale_kill_switch,
        name="stale-kill-switch",
    )
    thread.start()
    assert stale_snapshot_loaded.wait(timeout=5)
    with session_factory() as db:
        command = ControlCommand(
            source="test",
            reason=f"concurrent {racing_transition} commits first",
        )
        if racing_transition == "pause":
            raced = pause_runtime(settings=settings, db=db, command=command)
        else:
            raced = resume_runtime(settings=settings, db=db, command=command)
        assert raced.outcome == "applied"
    release_kill_switch.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert kill_errors == []
    assert len(kill_results) == 1
    kill_result = kill_results[0]
    assert kill_result.outcome == "applied"
    with session_factory() as db:
        state = db.get(RuntimeControlState, "global")
        kill_event = db.get(RuntimeControlEvent, kill_result.event_id)
        assert state is not None
        assert kill_event is not None
        assert state.kill_switch_enabled is True
        assert state.paused is True
        assert state.last_transition_action == "kill_switch"
        assert state.last_transition_event_id == kill_result.event_id
        assert kill_event.previous_state == {
            "mode": "paper",
            "paused": expected_previous_paused,
            "kill_switch_enabled": False,
            "automation_enabled": not expected_previous_paused,
            "hourly_risk_off_active": False,
            "hourly_risk_off_epoch": 0,
            "hourly_risk_off_context": None,
            "readiness_reason": expected_previous_reason,
        }
        assert kill_event.current_state["paused"] is True
        assert kill_event.current_state["kill_switch_enabled"] is True
        assert kill_event.current_state["automation_enabled"] is False
        assert kill_event.message == (
            "kill switch enabled; automated order submission is durably blocked"
        )
    engine.dispose()


@pytest.mark.parametrize("transition", ["pause", "kill_switch", "kill_switch_reset"])
def test_control_transition_event_snapshots_include_concurrent_hourly_fence(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    transition: str,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'control-hourly-race-{transition}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active_kill_event_id: str | None = None
    with session_factory() as db:
        ensure_runtime_control_state(settings=settings, db=db)
        resume_runtime(
            settings=settings,
            db=db,
            command=ControlCommand(source="test", reason="prepare running state"),
        )
        if transition == "kill_switch_reset":
            kill = enable_kill_switch(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare reset"),
            )
            assert kill.outcome == "applied"
            active_kill_event_id = kill.event_id

    stale_snapshot_loaded = threading.Event()
    release_transition = threading.Event()
    original_state_snapshot = runtime_controls_service._state_snapshot
    delayed = False

    def delay_transition_after_snapshot(
        state: RuntimeControlState,
    ) -> dict[str, object]:
        nonlocal delayed
        snapshot = original_state_snapshot(state)
        if threading.current_thread().name == "control-hourly-race" and not delayed:
            delayed = True
            stale_snapshot_loaded.set()
            assert release_transition.wait(timeout=5)
        return snapshot

    monkeypatch.setattr(
        runtime_controls_service,
        "_state_snapshot",
        delay_transition_after_snapshot,
    )
    results: list[object] = []
    errors: list[BaseException] = []

    def apply_control_transition() -> None:
        try:
            with session_factory() as db:
                command = ControlCommand(
                    source="test",
                    reason=f"race {transition} with hourly fence",
                )
                if transition == "pause":
                    result = pause_runtime(settings=settings, db=db, command=command)
                elif transition == "kill_switch":
                    result = enable_kill_switch(
                        settings=settings,
                        db=db,
                        command=command,
                    )
                else:
                    assert active_kill_event_id is not None
                    result = reset_kill_switch(
                        settings=settings,
                        db=db,
                        command=command,
                        expected_current_event_id=active_kill_event_id,
                    )
                results.append(result)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(
        target=apply_control_transition,
        name="control-hourly-race",
    )
    thread.start()
    assert stale_snapshot_loaded.wait(timeout=5)
    risk_off = apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=3,
        risk_reasons=["concurrent risk-off"],
    )
    assert risk_off["hourly_risk_off"]["active"] is True
    release_transition.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert len(results) == 1
    result = results[0]
    assert result.outcome == "applied"
    with session_factory() as db:
        state = db.get(RuntimeControlState, "global")
        event_row = db.get(RuntimeControlEvent, result.event_id)
        assert state is not None
        assert event_row is not None
        assert state.hourly_risk_off_active is True
        assert event_row.previous_state["hourly_risk_off_active"] is True
        assert event_row.current_state["hourly_risk_off_active"] is True
        assert event_row.previous_state["hourly_risk_off_epoch"] == (
            state.hourly_risk_off_epoch
        )
        assert event_row.current_state["hourly_risk_off_epoch"] == (
            state.hourly_risk_off_epoch
        )
        assert event_row.previous_state["hourly_risk_off_context"] == (
            state.hourly_risk_off_context
        )
        assert event_row.current_state["hourly_risk_off_context"] == (
            state.hourly_risk_off_context
        )
    engine.dispose()


def test_lifespan_shutdown_closes_admission_before_stopping_any_coordinator() -> None:
    calls: list[str] = []

    class Coordinator:
        def __init__(self, name: str) -> None:
            self.name = name

        def begin_shutdown(self) -> None:
            calls.append(f"admission:{self.name}")

        async def stop(self) -> None:
            expected_calls = ["admission:one", "admission:two"]
            if self.name == "two":
                expected_calls.append("stop:one")
            assert calls == expected_calls
            calls.append(f"stop:{self.name}")

    asyncio.run(
        _shutdown_runtime_coordinators(
            coordinators=(
                ("one", Coordinator("one")),
                ("two", Coordinator("two")),
            ),
        )
    )

    assert calls == ["admission:one", "admission:two", "stop:one", "stop:two"]


def test_lifespan_closes_broker_admission_before_coordinator_drain() -> None:
    async def scenario() -> None:
        stop_started = asyncio.Event()
        release_stop = asyncio.Event()
        fence = RuntimeCycleFence()
        broker_calls: list[str] = []

        class Coordinator:
            def begin_shutdown(self) -> None:
                fence.begin_shutdown()

            async def stop(self) -> None:
                stop_started.set()
                await release_stop.wait()

        class Adapter:
            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("placed")
                return object()

        shutdown_task = asyncio.create_task(
            _shutdown_runtime_coordinators(
                coordinators=(("runtime", Coordinator()),),
            )
        )
        await asyncio.wait_for(stop_started.wait(), timeout=1)

        with pytest.raises(asyncio.CancelledError):
            GuardedBrokerAdapter(Adapter(), fence).place_order("token", object())
        assert broker_calls == []

        release_stop.set()
        await asyncio.wait_for(shutdown_task, timeout=1)

    asyncio.run(scenario())


def test_lifespan_waits_for_real_drain_after_bounded_stop_timeout() -> None:
    async def scenario() -> None:
        release_worker = asyncio.Event()
        drain_started = asyncio.Event()
        calls: list[str] = []

        class TimedOutCoordinator:
            async def stop(self) -> None:
                calls.append("stop:timed_out")
                raise TimeoutError("worker did not drain")

            async def wait_stopped(self) -> None:
                calls.append("wait:timed_out")
                drain_started.set()
                await release_worker.wait()

        class FastCoordinator:
            async def stop(self) -> None:
                calls.append("stop:fast")

        shutdown_task = asyncio.create_task(
            _stop_runtime_coordinators(
                (
                    ("timed_out", TimedOutCoordinator()),
                    ("fast", FastCoordinator()),
                )
            )
        )
        await asyncio.wait_for(drain_started.wait(), timeout=1)

        assert shutdown_task.done() is False
        assert calls == ["stop:timed_out", "stop:fast", "wait:timed_out"]

        release_worker.set()
        with pytest.raises(ExceptionGroup) as exc_info:
            await asyncio.wait_for(shutdown_task, timeout=1)

        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], TimeoutError)

    asyncio.run(scenario())


def test_cancelled_lifespan_finishes_coordinator_drain() -> None:
    async def scenario() -> None:
        stop_started = asyncio.Event()
        release_stop = asyncio.Event()
        calls: list[str] = []

        class Coordinator:
            def begin_shutdown(self) -> None:
                calls.append("admission_closed")

            async def stop(self) -> None:
                calls.append("stop:start")
                stop_started.set()
                await release_stop.wait()
                calls.append("stop:done")

        shutdown_task = asyncio.create_task(
            _shutdown_runtime_coordinators(
                coordinators=(("runtime", Coordinator()),),
            )
        )
        await asyncio.wait_for(stop_started.wait(), timeout=1)
        shutdown_task.cancel()
        await asyncio.sleep(0.02)

        assert calls == ["admission_closed", "stop:start"]

        release_stop.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown_task, timeout=1)
        assert calls == ["admission_closed", "stop:start", "stop:done"]

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity", "autonomous"])
def test_stop_timeout_keeps_runtime_fenced_until_real_worker_completion(
    runtime_kind: str,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
            autonomous_runtime_enabled=runtime_kind == "autonomous",
            nxt_eligible_sync_enabled=False,
        )
        closed: list[str] = []

        class PriceSource:
            def close(self) -> None:
                closed.append("closed")

        if runtime_kind == "crypto":
            runtime = AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        elif runtime_kind == "us_equity":
            runtime = AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        else:
            runtime = AutonomousRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
                broker_adapter_factory=(
                    lambda _settings: None  # type: ignore[arg-type,return-value]
                ),
                monitoring_price_source=PriceSource(),
            )

        entered = threading.Event()
        release = threading.Event()

        def blocking_cycle(*_args: object) -> dict[str, object]:
            entered.set()
            assert release.wait(timeout=2)
            return {"status": "healthy", "status_code": 200}

        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]
        if runtime_kind == "autonomous":
            runtime._dispatch_cycle_sync = blocking_cycle  # type: ignore[method-assign]
            runtime._dispatch_task = asyncio.create_task(
                runtime._run_dispatch_cycle(datetime.now(UTC))
            )
        else:
            runtime.run_cycle = blocking_cycle  # type: ignore[method-assign]
            runtime._task = asyncio.create_task(runtime._run_cycle_in_thread())

        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            assert runtime._stopping is True
            assert closed == []

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            assert runtime._stopping is False
            assert not runtime._inflight_cycles
            if runtime_kind == "autonomous":
                assert closed == ["closed"]
        finally:
            release.set()
            wait_stopped = getattr(runtime, "wait_stopped", None)
            if callable(wait_stopped):
                await wait_stopped()

    asyncio.run(scenario())


def test_shutdown_generation_blocks_broker_mutation_and_terminal_commit(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'cycle-fence.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    generation = fence.capture_generation()
    broker_calls: list[str] = []

    class Adapter:
        provider = "test"

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            broker_calls.append("place_order")
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)
    guarded_sessions = CommitGuardedSessionFactory(
        session_factory,
        fence,
    )

    assert isinstance(adapter, Adapter)

    fence.begin_shutdown()

    with pytest.raises(asyncio.CancelledError):
        fence.checkpoint(generation)
    with pytest.raises(asyncio.CancelledError):
        adapter.place_order("token", object())
    with guarded_sessions() as db:
        db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
        with pytest.raises(asyncio.CancelledError):
            db.commit()
        db.rollback()

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM shutdown_sentinel")) == 0
    assert broker_calls == []
    engine.dispose()


def test_shutdown_preserves_broker_call_that_was_already_admitted() -> None:
    fence = RuntimeCycleFence()
    broker_entered = threading.Event()
    release_broker = threading.Event()
    shutdown_finished = threading.Event()
    calls: list[str] = []

    class Adapter:
        def place_order(self, *_args: object, **_kwargs: object) -> object:
            broker_entered.set()
            assert release_broker.wait(timeout=2)
            calls.append("placed")
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)
    broker_thread = threading.Thread(target=lambda: adapter.place_order("token", object()))
    broker_thread.start()
    assert broker_entered.wait(timeout=1)

    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    shutdown_thread.start()

    assert shutdown_finished.wait(timeout=1) is True
    release_broker.set()
    broker_thread.join(timeout=1)
    shutdown_thread.join(timeout=1)

    assert calls == ["placed"]
    with pytest.raises(asyncio.CancelledError):
        adapter.place_order("token", object())


def test_shutdown_does_not_cancel_commit_that_already_crossed_the_fence(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'commit-linearization.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    commit_entered = threading.Event()
    release_commit = threading.Event()
    shutdown_finished = threading.Event()
    failures: list[BaseException] = []
    db = guarded_sessions()

    def block_after_guard(_session) -> None:  # noqa: ANN001
        commit_entered.set()
        assert release_commit.wait(timeout=2)

    event.listen(db, "before_commit", block_after_guard)
    db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))

    def commit() -> None:
        try:
            db.commit()
        except BaseException as exc:  # pragma: no cover - assertion reports detail
            failures.append(exc)

    commit_thread = threading.Thread(target=commit)
    commit_thread.start()
    assert commit_entered.wait(timeout=1)
    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    shutdown_thread.start()

    assert shutdown_finished.wait(timeout=1) is True
    release_commit.set()
    commit_thread.join(timeout=1)
    shutdown_thread.join(timeout=1)

    assert failures == []
    assert shutdown_finished.is_set() is True
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM shutdown_sentinel")) == 1
    db.close()
    engine.dispose()


def test_commit_admission_never_delays_closing_new_broker_admission() -> None:
    fence = RuntimeCycleFence()
    fence.begin_commit()
    shutdown_finished = threading.Event()
    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    try:
        shutdown_thread.start()

        assert shutdown_finished.wait(timeout=1) is True
        with pytest.raises(asyncio.CancelledError):
            GuardedBrokerAdapter(SimpleNamespace(place_order=lambda: object()), fence).place_order()
    finally:
        fence.finish_commit()
        shutdown_thread.join(timeout=1)


def test_shutdown_blocks_second_broker_call_in_an_already_admitted_cycle() -> None:
    fence = RuntimeCycleFence()
    generation = fence.capture_generation()
    first_completed = threading.Event()
    attempt_second = threading.Event()
    cycle_finished = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def cycle() -> None:
        try:
            adapter.place_order("first")
            first_completed.set()
            assert attempt_second.wait(timeout=2)
            adapter.place_order("second")
        except BaseException as exc:
            errors.append(exc)
        finally:
            cycle_finished.set()

    cycle_thread = threading.Thread(
        target=lambda: fence.run(generation, cycle)
    )
    cycle_thread.start()
    try:
        assert first_completed.wait(timeout=1)
        fence.begin_shutdown()
        attempt_second.set()
        assert cycle_finished.wait(timeout=1)

        assert calls == ["first"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        attempt_second.set()
        cycle_thread.join(timeout=1)


def test_admitted_broker_call_persists_ack_but_cannot_admit_another_call(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'broker-ack-fence.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE broker_ack (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    generation = fence.capture_generation()
    broker_entered = threading.Event()
    release_broker = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            if label == "first":
                broker_entered.set()
                assert release_broker.wait(timeout=2)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def cycle() -> None:
        adapter.place_order("first")
        with guarded_sessions() as db:
            db.execute(text("INSERT INTO broker_ack (value) VALUES (1)"))
            db.commit()
        adapter.place_order("second")

    def run_cycle() -> None:
        try:
            fence.run(generation, cycle)
        except BaseException as exc:
            errors.append(exc)

    cycle_thread = threading.Thread(target=run_cycle)
    cycle_thread.start()
    try:
        assert broker_entered.wait(timeout=1)
        fence.begin_shutdown()
        release_broker.set()
        cycle_thread.join(timeout=1)

        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM broker_ack")) == 1
        assert calls == ["first"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        release_broker.set()
        cycle_thread.join(timeout=1)
        engine.dispose()


def test_expired_cycle_lease_blocks_late_broker_call_without_fencing_other_cycles() -> None:
    fence = RuntimeCycleFence()
    expired_lease = fence.capture_cycle_lease()
    active_lease = fence.capture_cycle_lease()
    release_cycle = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def delayed_cycle() -> None:
        assert release_cycle.wait(timeout=2)
        adapter.place_order("expired")

    def run_delayed_cycle() -> None:
        try:
            fence.run(
                expired_lease.generation,
                delayed_cycle,
                lease=expired_lease,
            )
        except BaseException as exc:
            errors.append(exc)

    cycle_thread = threading.Thread(target=run_delayed_cycle)
    cycle_thread.start()
    try:
        fence.expire_cycle_lease(expired_lease)
        release_cycle.set()
        cycle_thread.join(timeout=1)

        fence.run(
            active_lease.generation,
            lambda: adapter.place_order("active"),
            lease=active_lease,
        )

        assert calls == ["active"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        release_cycle.set()
        cycle_thread.join(timeout=1)


def test_expired_cycle_lease_blocks_late_database_commit(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'expired-cycle.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE cycle_sentinel (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    lease = fence.capture_cycle_lease()
    release_cycle = threading.Event()
    errors: list[BaseException] = []

    def delayed_cycle() -> None:
        assert release_cycle.wait(timeout=2)
        with guarded_sessions() as db:
            db.execute(text("INSERT INTO cycle_sentinel (value) VALUES (1)"))
            db.commit()

    def run_delayed_cycle() -> None:
        try:
            fence.run(lease.generation, delayed_cycle, lease=lease)
        except BaseException as exc:
            errors.append(exc)

    cycle_thread = threading.Thread(target=run_delayed_cycle)
    cycle_thread.start()
    try:
        fence.expire_cycle_lease(lease)
        release_cycle.set()
        cycle_thread.join(timeout=1)

        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM cycle_sentinel")) == 0
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        release_cycle.set()
        cycle_thread.join(timeout=1)
        engine.dispose()


def test_expired_cycle_lease_preserves_already_admitted_broker_outcome(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'expired-cycle-ack.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE broker_ack (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    lease = fence.capture_cycle_lease()
    broker_entered = threading.Event()
    release_broker = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            if label == "first":
                broker_entered.set()
                assert release_broker.wait(timeout=2)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def cycle() -> None:
        adapter.place_order("first")
        with guarded_sessions() as db:
            db.execute(text("INSERT INTO broker_ack (value) VALUES (1)"))
            db.commit()
        adapter.place_order("second")

    def run_cycle() -> None:
        try:
            fence.run(lease.generation, cycle, lease=lease)
        except BaseException as exc:
            errors.append(exc)

    cycle_thread = threading.Thread(target=run_cycle)
    cycle_thread.start()
    try:
        assert broker_entered.wait(timeout=1)
        fence.expire_cycle_lease(lease)
        release_broker.set()
        cycle_thread.join(timeout=1)

        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM broker_ack")) == 1
        assert calls == ["first"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        release_broker.set()
        cycle_thread.join(timeout=1)
        engine.dispose()


def test_nested_transaction_commit_does_not_leak_runtime_fence_lease(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'nested-commit-lease.db'}")
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)

    class CountingFence:
        def __init__(self) -> None:
            self.begins = 0
            self.finishes = 0

        def begin_commit(self) -> None:
            self.begins += 1

        def finish_commit(self) -> None:
            self.finishes += 1

    fence = CountingFence()
    guarded_sessions = CommitGuardedSessionFactory(
        session_factory,
        fence,  # type: ignore[arg-type]
    )
    try:
        with guarded_sessions() as db:
            db.execute(text("SELECT 1"))
            with db.begin_nested():
                db.execute(text("SELECT 1"))
            db.commit()

        assert fence.begins == 1
        assert fence.finishes == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity"])
def test_alpaca_cycle_released_after_stop_timeout_cannot_mutate(
    runtime_kind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url=f"sqlite:///{tmp_path / f'{runtime_kind}-release.db'}",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
            alpaca_crypto_allowed_symbols="BTC/USD",
            alpaca_us_equity_allowed_symbols="SPY",
        )
        engine = create_db_engine(settings)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
        session_factory = create_session_factory(engine)
        entered = threading.Event()
        release = threading.Event()
        broker_boundary_blocked = threading.Event()
        commit_attempted = threading.Event()
        commit_completed = threading.Event()
        broker_calls: list[str] = []

        class Adapter:
            provider = "alpaca"

            def get_live_market_context(self, *_args: object, **_kwargs: object) -> None:
                return None

            def get_market_clock(self, *_args: object, **_kwargs: object) -> dict[str, object]:
                return {"is_open": True}

            def get_stock_live_market_context(
                self,
                *_args: object,
                **_kwargs: object,
            ) -> None:
                return None

            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("place_order")
                return object()

        class Strategy:
            def __init__(self, guarded_session_factory) -> None:  # noqa: ANN001
                self.guarded_session_factory = guarded_session_factory

            def run_cycle(self, *, adapter: object, **_kwargs: object) -> dict[str, object]:
                entered.set()
                assert release.wait(timeout=2)
                try:
                    adapter.place_order("token", object())  # type: ignore[attr-defined]
                except asyncio.CancelledError:
                    broker_boundary_blocked.set()
                else:
                    raise AssertionError("shutdown did not fence broker placement")
                with self.guarded_session_factory() as db:
                    db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
                    commit_attempted.set()
                    db.commit()
                    commit_completed.set()
                return {"status": "healthy"}

            def snapshot(self) -> dict[str, object]:
                return {}

        if runtime_kind == "crypto":
            import app.services.alpaca_crypto_runtime as runtime_module

            runtime = AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=session_factory,
            )
        else:
            import app.services.alpaca_us_equity_runtime as runtime_module

            runtime = AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=session_factory,
            )
        adapter = Adapter()
        monkeypatch.setattr(
            runtime_module,
            "build_broker_adapter",
            lambda *_args, **_kwargs: adapter,
        )
        monkeypatch.setattr(
            runtime_module,
            "ensure_authenticated_broker_session",
            lambda **_kwargs: SimpleNamespace(access_token="token"),
        )
        runtime._strategy = Strategy(  # type: ignore[assignment]
            runtime._cycle_session_factory
        )
        runtime._execution_scope = {
            "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
            "provider": "alpaca",
            "account_scope": "a" * 24,
        }
        runtime._last_reconciled_at = datetime.now(UTC)
        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]

        await runtime.start()
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            with engine.connect() as connection:
                terminal_count = connection.scalar(
                    text("SELECT count(*) FROM shutdown_sentinel")
                )
            assert broker_calls == []
            assert broker_boundary_blocked.is_set() is True
            assert commit_attempted.is_set() is True
            assert commit_completed.is_set() is False
            assert terminal_count == 0
        finally:
            release.set()
            await runtime.wait_stopped()
            engine.dispose()

    asyncio.run(scenario())


def test_autonomous_cycle_released_after_stop_timeout_cannot_mutate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url=f"sqlite:///{tmp_path / 'autonomous-release.db'}",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
            nxt_eligible_sync_enabled=False,
        )
        engine = create_db_engine(settings)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
        session_factory = create_session_factory(engine)
        entered = threading.Event()
        release = threading.Event()
        broker_boundary_blocked = threading.Event()
        commit_attempted = threading.Event()
        commit_completed = threading.Event()
        broker_calls: list[str] = []

        class Adapter:
            provider = "kis"

            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("place_order")
                return object()

        def delayed_dispatch(*, db, adapter, **_kwargs):  # noqa: ANN001, ANN202
            entered.set()
            assert release.wait(timeout=2)
            try:
                adapter.place_order("token", object())
            except asyncio.CancelledError:
                broker_boundary_blocked.set()
            else:
                raise AssertionError("shutdown did not fence broker placement")
            db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
            commit_attempted.set()
            db.commit()
            commit_completed.set()
            return SimpleNamespace(status_code=200, payload={"completed": True})

        monkeypatch.setattr(
            "app.services.autonomous_runtime.dispatch_due_execution_jobs",
            delayed_dispatch,
        )
        runtime = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=session_factory,
            broker_adapter_factory=(
                lambda _settings: Adapter()  # type: ignore[arg-type,return-value]
            ),
        )
        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]
        runtime._dispatch_task = asyncio.create_task(
            runtime._run_dispatch_cycle(datetime.now(UTC))
        )
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            with engine.connect() as connection:
                terminal_count = connection.scalar(
                    text("SELECT count(*) FROM shutdown_sentinel")
                )
            assert broker_calls == []
            assert broker_boundary_blocked.is_set() is True
            assert commit_attempted.is_set() is True
            assert commit_completed.is_set() is False
            assert terminal_count == 0
        finally:
            release.set()
            await runtime.wait_stopped()
            engine.dispose()

    asyncio.run(scenario())
