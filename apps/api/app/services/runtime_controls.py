from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.runtime import RuntimeControlEvent, RuntimeControlState, new_id

RUNTIME_CONTROL_ID = "global"
DEFAULT_CONTROL_SOURCE = "operator"


@dataclass(frozen=True)
class ControlCommand:
    actor: str | None = None
    reason: str | None = None
    source: str = DEFAULT_CONTROL_SOURCE


@dataclass(frozen=True)
class ControlActionResult:
    action: str
    outcome: str
    message: str
    event_id: str
    safety: dict[str, object]

    @property
    def changed(self) -> bool:
        return self.outcome == "applied"

    @property
    def status_code(self) -> int:
        return 409 if self.outcome == "rejected" else 200

    def payload(self) -> dict[str, object]:
        return {
            "action": self.action,
            "outcome": self.outcome,
            "changed": self.changed,
            "message": self.message,
            "event_id": self.event_id,
            "safety": self.safety,
        }


def ensure_runtime_control_state(
    *,
    settings: Settings,
    db: Session,
) -> RuntimeControlState:
    state, changed = _load_runtime_control_state(
        settings=settings,
        db=db,
        sync_mode=True,
    )
    if changed:
        db.commit()
        db.refresh(state)
    return state


def automated_order_submission_allowed(state: RuntimeControlState) -> bool:
    return (
        not state.kill_switch_enabled
        and not state.paused
        and state.automation_enabled
    )


def automated_order_submission_block_reason(state: RuntimeControlState) -> str | None:
    if state.kill_switch_enabled:
        return "kill_switch"
    if state.paused:
        return "paused"
    if not state.automation_enabled:
        return "automation_disabled"
    return None


def last_change_payload(state: RuntimeControlState) -> dict[str, object]:
    return {
        "action": state.last_transition_action,
        "actor": state.last_transition_actor,
        "source": state.last_transition_source,
        "reason": state.last_transition_reason,
        "changed_at": state.last_transition_at.isoformat(),
        "event_id": state.last_transition_event_id,
    }


def runtime_safety_payload(state: RuntimeControlState) -> dict[str, object]:
    return {
        "mode": state.mode,
        "paused": state.paused,
        "kill_switch_enabled": state.kill_switch_enabled,
        "automation_enabled": state.automation_enabled,
        "automated_order_submission_allowed": automated_order_submission_allowed(state),
        "automated_order_submission_block_reason": automated_order_submission_block_reason(state),
        "readiness_reason": state.readiness_reason,
        "updated_at": state.updated_at.isoformat() if state.updated_at else None,
        "last_change": last_change_payload(state),
    }


def pause_runtime(
    *,
    settings: Settings,
    db: Session,
    command: ControlCommand,
) -> ControlActionResult:
    state, _ = _load_runtime_control_state(
        settings=settings,
        db=db,
        sync_mode=True,
    )
    previous_state = _state_snapshot(state)

    if state.kill_switch_enabled:
        message = "kill switch already active; automated order submission remains blocked"
        outcome = "noop"
    elif state.paused and not state.automation_enabled:
        message = "runtime is already paused"
        outcome = "noop"
    else:
        state.mode = settings.runtime_mode
        state.paused = True
        state.automation_enabled = False
        state.readiness_reason = "paused by operator; monitoring remains active"
        message = "runtime paused; monitoring remains active"
        outcome = "applied"

    return _finalize_action(
        db=db,
        state=state,
        command=command,
        action="pause",
        outcome=outcome,
        message=message,
        previous_state=previous_state,
    )


def resume_runtime(
    *,
    settings: Settings,
    db: Session,
    command: ControlCommand,
) -> ControlActionResult:
    state, _ = _load_runtime_control_state(
        settings=settings,
        db=db,
        sync_mode=True,
    )
    previous_state = _state_snapshot(state)

    if state.kill_switch_enabled:
        message = "kill switch is active; explicit reset flow is required before resume"
        outcome = "rejected"
    elif not state.paused and state.automation_enabled:
        message = "runtime is already accepting automated actions"
        outcome = "noop"
    else:
        state.mode = settings.runtime_mode
        state.paused = False
        state.automation_enabled = True
        state.readiness_reason = None
        message = "runtime resumed; automated actions are enabled"
        outcome = "applied"

    return _finalize_action(
        db=db,
        state=state,
        command=command,
        action="resume",
        outcome=outcome,
        message=message,
        previous_state=previous_state,
    )


def enable_kill_switch(
    *,
    settings: Settings,
    db: Session,
    command: ControlCommand,
) -> ControlActionResult:
    state, _ = _load_runtime_control_state(
        settings=settings,
        db=db,
        sync_mode=False,
    )
    previous_state = _state_snapshot(state)

    if state.kill_switch_enabled:
        message = "kill switch is already active"
        outcome = "noop"
        return _finalize_action(
            db=db,
            state=state,
            command=command,
            action="kill_switch",
            outcome=outcome,
            message=message,
            previous_state=previous_state,
        )

    occurred_at = datetime.now(UTC)
    event_id = new_id()
    readiness_reason = (
        "kill switch enabled by operator; automated order submission is blocked"
    )
    current_state = {
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": True,
        "automation_enabled": False,
        "readiness_reason": readiness_reason,
    }
    result = db.execute(
        update(RuntimeControlState)
        .where(
            RuntimeControlState.id == state.id,
            RuntimeControlState.kill_switch_enabled.is_(False),
        )
        .values(
            mode=settings.runtime_mode,
            paused=True,
            kill_switch_enabled=True,
            automation_enabled=False,
            readiness_reason=readiness_reason,
            last_transition_action="kill_switch",
            last_transition_actor=command.actor,
            last_transition_source=command.source,
            last_transition_reason=command.reason,
            last_transition_at=occurred_at,
            last_transition_event_id=event_id,
        ),
    )
    if result.rowcount:
        event = RuntimeControlEvent(
            id=event_id,
            runtime_control_state_id=state.id,
            action="kill_switch",
            outcome="applied",
            actor=command.actor,
            source=command.source,
            reason=command.reason,
            message="kill switch enabled; automated order submission is durably blocked",
            previous_state=previous_state,
            current_state=current_state,
            occurred_at=occurred_at,
        )
        db.add(event)
        db.commit()
        db.refresh(state)
        db.refresh(event)
        return ControlActionResult(
            action="kill_switch",
            outcome="applied",
            message=event.message or "",
            event_id=event.id,
            safety=runtime_safety_payload(state),
        )

    db.refresh(state)
    if state.mode != settings.runtime_mode:
        state.mode = settings.runtime_mode
    return _finalize_action(
        db=db,
        state=state,
        command=command,
        action="kill_switch",
        outcome="noop",
        message="kill switch is already active",
        previous_state=_state_snapshot(state),
    )


def _load_runtime_control_state(
    *,
    settings: Settings,
    db: Session,
    sync_mode: bool,
) -> tuple[RuntimeControlState, bool]:
    state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    created = False
    if state is None:
        created = _insert_runtime_control_state_if_missing(settings=settings, db=db)
        state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    if state is None:
        raise RuntimeError("runtime control state bootstrap failed")

    changed = created
    if sync_mode and state.mode != settings.runtime_mode:
        state.mode = settings.runtime_mode
        changed = True
    return state, changed


def _insert_runtime_control_state_if_missing(
    *,
    settings: Settings,
    db: Session,
) -> bool:
    values = _bootstrap_state_values(settings=settings)
    dialect_name = db.get_bind().dialect.name
    if dialect_name == "sqlite":
        result = db.execute(
            sqlite_insert(RuntimeControlState)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[RuntimeControlState.id]),
        )
        return bool(result.rowcount)
    if dialect_name == "postgresql":
        result = db.execute(
            postgresql_insert(RuntimeControlState)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[RuntimeControlState.id]),
        )
        return bool(result.rowcount)

    db.add(RuntimeControlState(**values))
    db.flush()
    return True


def _bootstrap_state_values(*, settings: Settings) -> dict[str, object]:
    now = datetime.now(UTC)
    readiness_reason = "initialized fail-closed until operator controls are enabled"
    return {
        "id": RUNTIME_CONTROL_ID,
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": False,
        "automation_enabled": False,
        "readiness_reason": readiness_reason,
        "last_transition_action": "bootstrap",
        "last_transition_source": "system",
        "last_transition_reason": readiness_reason,
        "last_transition_at": now,
        "last_transition_event_id": None,
    }


def _state_snapshot(state: RuntimeControlState) -> dict[str, object]:
    return {
        "mode": state.mode,
        "paused": state.paused,
        "kill_switch_enabled": state.kill_switch_enabled,
        "automation_enabled": state.automation_enabled,
        "readiness_reason": state.readiness_reason,
    }


def _finalize_action(
    *,
    db: Session,
    state: RuntimeControlState,
    command: ControlCommand,
    action: str,
    outcome: str,
    message: str,
    previous_state: dict[str, object],
) -> ControlActionResult:
    occurred_at = datetime.now(UTC)
    current_state = _state_snapshot(state)
    event = RuntimeControlEvent(
        runtime_control_state_id=state.id,
        action=action,
        outcome=outcome,
        actor=command.actor,
        source=command.source,
        reason=command.reason,
        message=message,
        previous_state=previous_state,
        current_state=current_state,
        occurred_at=occurred_at,
    )
    db.add(event)
    db.flush()

    if outcome == "applied":
        state.last_transition_action = action
        state.last_transition_actor = command.actor
        state.last_transition_source = command.source
        state.last_transition_reason = command.reason
        state.last_transition_at = occurred_at
        state.last_transition_event_id = event.id

    db.commit()
    db.refresh(state)
    db.refresh(event)

    return ControlActionResult(
        action=action,
        outcome=outcome,
        message=message,
        event_id=event.id,
        safety=runtime_safety_payload(state),
    )
