from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.runtime import RuntimeControlEvent, RuntimeControlState, new_id

RUNTIME_CONTROL_ID = "global"
DEFAULT_CONTROL_SOURCE = "operator"
LEGACY_LIFESPAN_SHUTDOWN_SOURCE = "lifespan_shutdown"
LEGACY_LIFESPAN_SHUTDOWN_REASON = "service lifespan shutdown started"
LEGACY_LIFESPAN_SHUTDOWN_MESSAGE = "runtime paused; monitoring remains active"
LEGACY_LIFESPAN_SHUTDOWN_READINESS_REASON = (
    "paused by operator; monitoring remains active"
)


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


def recover_legacy_lifespan_shutdown_pause(
    *,
    settings: Settings,
    db: Session,
) -> ControlActionResult | None:
    """Release only the exact durable pause written by the retired shutdown path."""

    state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    if (
        state is None
        or state.mode != settings.runtime_mode
        or not state.paused
        or state.kill_switch_enabled
        or state.last_transition_action != "pause"
        or state.last_transition_source != LEGACY_LIFESPAN_SHUTDOWN_SOURCE
        or not state.last_transition_event_id
    ):
        return None

    legacy_event = db.get(RuntimeControlEvent, state.last_transition_event_id)
    current_state = (
        legacy_event.current_state
        if legacy_event is not None and isinstance(legacy_event.current_state, dict)
        else {}
    )
    if (
        legacy_event is None
        or legacy_event.runtime_control_state_id != state.id
        or legacy_event.action != "pause"
        or legacy_event.outcome != "applied"
        or legacy_event.actor != settings.service_name
        or legacy_event.source != LEGACY_LIFESPAN_SHUTDOWN_SOURCE
        or legacy_event.reason != LEGACY_LIFESPAN_SHUTDOWN_REASON
        or legacy_event.message != LEGACY_LIFESPAN_SHUTDOWN_MESSAGE
        or state.readiness_reason != LEGACY_LIFESPAN_SHUTDOWN_READINESS_REASON
        or current_state.get("paused") is not True
        or current_state.get("kill_switch_enabled") is not False
        or current_state.get("automation_enabled") is not False
        or current_state.get("readiness_reason")
        != LEGACY_LIFESPAN_SHUTDOWN_READINESS_REASON
    ):
        return None

    previous_state = _state_snapshot(state)
    return _apply_control_transition_compare_and_swap(
        db=db,
        state=state,
        command=ControlCommand(
            actor=settings.service_name,
            source="startup_recovery",
            reason="release retired lifespan shutdown pause",
        ),
        action="legacy_shutdown_pause_recovery",
        outcome="applied",
        message="legacy lifespan shutdown pause released; runtime entry automation restored",
        previous_state=previous_state,
        expected_state={
            "paused": True,
            "kill_switch_enabled": False,
            "last_transition_action": "pause",
            "last_transition_source": LEGACY_LIFESPAN_SHUTDOWN_SOURCE,
            "last_transition_reason": LEGACY_LIFESPAN_SHUTDOWN_REASON,
            "last_transition_event_id": legacy_event.id,
            "readiness_reason": LEGACY_LIFESPAN_SHUTDOWN_READINESS_REASON,
        },
        state_values={
            "mode": settings.runtime_mode,
            "paused": False,
            "readiness_reason": None,
        },
    )


def effective_automation_enabled(state: RuntimeControlState) -> bool:
    return not state.kill_switch_enabled and not state.paused


def automated_order_submission_allowed(state: RuntimeControlState) -> bool:
    return not state.kill_switch_enabled


def automated_order_submission_block_reason(state: RuntimeControlState) -> str | None:
    if state.kill_switch_enabled:
        return "kill_switch"
    return None


def automated_buy_submission_block_reason(state: RuntimeControlState) -> str | None:
    runtime_reason = automated_order_submission_block_reason(state)
    if runtime_reason is not None:
        return runtime_reason
    if state.paused:
        return "paused"
    if state.hourly_risk_off_active:
        return "hourly_plan_risk_off"
    return None


def automated_buy_submission_allowed(state: RuntimeControlState) -> bool:
    return automated_buy_submission_block_reason(state) is None


def hourly_risk_off_retry_after_at(
    *,
    settings: Settings,
    state: RuntimeControlState,
    now: datetime | None = None,
) -> datetime | None:
    if not state.hourly_risk_off_active:
        return None
    observed_at = _normalize_utc(now or datetime.now(UTC))
    context = (
        state.hourly_risk_off_context
        if isinstance(state.hourly_risk_off_context, dict)
        else {}
    )
    cooldown_until = _parse_utc_datetime(context.get("cooldown_until"))
    last_healthy = _parse_utc_datetime(context.get("last_healthy_observed_at"))
    next_observation_at = (
        last_healthy
        + timedelta(
            seconds=settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
        )
        if last_healthy is not None
        else None
    )
    candidates = [
        candidate
        for candidate in (cooldown_until, next_observation_at)
        if candidate is not None and candidate > observed_at
    ]
    if candidates:
        return max(candidates)
    return observed_at + timedelta(
        seconds=settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
    )


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
        "automation_enabled": effective_automation_enabled(state),
        "automated_order_submission_allowed": automated_order_submission_allowed(state),
        "automated_order_submission_block_reason": automated_order_submission_block_reason(state),
        "automated_buy_submission_allowed": automated_buy_submission_allowed(state),
        "automated_buy_submission_block_reason": automated_buy_submission_block_reason(state),
        "hourly_risk_off": {
            "available": True,
            "active": state.hourly_risk_off_active,
            "epoch": state.hourly_risk_off_epoch,
            "context": state.hourly_risk_off_context,
        },
        "readiness_reason": state.readiness_reason,
        "updated_at": state.updated_at.isoformat() if state.updated_at else None,
        "last_change": last_change_payload(state),
    }


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _normalize_utc(parsed)


def pause_runtime(
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
        message = "kill switch already active; automated order submission remains blocked"
        outcome = "noop"
    elif state.paused:
        message = "runtime is already paused"
        outcome = "noop"
    else:
        readiness_reason = (
            "new entries paused by operator; monitoring and risk-reducing exits remain active"
        )
        message = (
            "new entries paused; monitoring and risk-reducing exits remain active"
        )
        applied = _apply_control_transition_compare_and_swap(
            db=db,
            state=state,
            command=command,
            action="pause",
            outcome="applied",
            message=message,
            previous_state=previous_state,
            expected_state={
                "paused": False,
                "kill_switch_enabled": False,
                "last_transition_event_id": state.last_transition_event_id,
            },
            state_values={
                "mode": settings.runtime_mode,
                "paused": True,
                "readiness_reason": readiness_reason,
            },
        )
        if applied is not None:
            return applied
        state = _reload_runtime_control_state(db=db)
        previous_state = _state_snapshot(state)
        if state.kill_switch_enabled:
            message = "kill switch already active; automated order submission remains blocked"
            outcome = "noop"
        elif state.paused:
            message = "runtime is already paused"
            outcome = "noop"
        else:
            message = "runtime pause rejected; runtime control state changed concurrently"
            outcome = "rejected"

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
        sync_mode=False,
    )
    previous_state = _state_snapshot(state)

    if state.kill_switch_enabled:
        message = "kill switch is active; explicit reset flow is required before resume"
        outcome = "rejected"
    elif not state.paused:
        message = "runtime is already accepting automated actions"
        outcome = "noop"
    else:
        message = "runtime resumed; automated actions are enabled"
        applied = _apply_control_transition_compare_and_swap(
            db=db,
            state=state,
            command=command,
            action="resume",
            outcome="applied",
            message=message,
            previous_state=previous_state,
            expected_state={
                "paused": True,
                "kill_switch_enabled": False,
                "last_transition_event_id": state.last_transition_event_id,
            },
            state_values={
                "mode": settings.runtime_mode,
                "paused": False,
                "readiness_reason": None,
            },
        )
        if applied is not None:
            return applied
        state = _reload_runtime_control_state(db=db)
        previous_state = _state_snapshot(state)
        if state.kill_switch_enabled:
            message = "kill switch is active; explicit reset flow is required before resume"
            outcome = "rejected"
        elif not state.paused:
            message = "runtime is already accepting automated actions"
            outcome = "noop"
        else:
            message = "runtime resume rejected; runtime control state changed concurrently"
            outcome = "rejected"

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
    while True:
        previous_state = _state_snapshot(state)
        if state.kill_switch_enabled:
            return _finalize_action(
                db=db,
                state=state,
                command=command,
                action="kill_switch",
                outcome="noop",
                message="kill switch is already active",
                previous_state=previous_state,
            )

        readiness_reason = (
            "kill switch enabled by operator; automated order submission is blocked"
        )
        applied = _apply_control_transition_compare_and_swap(
            db=db,
            state=state,
            command=command,
            action="kill_switch",
            outcome="applied",
            message=(
                "kill switch enabled; automated order submission is durably blocked"
            ),
            previous_state=previous_state,
            expected_state={
                "mode": state.mode,
                "paused": state.paused,
                "kill_switch_enabled": False,
                "readiness_reason": state.readiness_reason,
                "last_transition_event_id": state.last_transition_event_id,
            },
            state_values={
                "mode": settings.runtime_mode,
                "paused": True,
                "kill_switch_enabled": True,
                "readiness_reason": readiness_reason,
            },
        )
        if applied is not None:
            return applied
        state = _reload_runtime_control_state(db=db)


def reset_kill_switch(
    *,
    settings: Settings,
    db: Session,
    command: ControlCommand,
    expected_current_event_id: str,
) -> ControlActionResult:
    """Clear the kill switch only for the exact transition the operator reviewed."""

    state, _ = _load_runtime_control_state(
        settings=settings,
        db=db,
        sync_mode=False,
    )
    previous_state = _state_snapshot(state)
    if (
        not state.kill_switch_enabled
        or state.last_transition_action != "kill_switch"
        or state.last_transition_event_id != expected_current_event_id
    ):
        return _finalize_action(
            db=db,
            state=state,
            command=command,
            action="kill_switch_reset",
            outcome="rejected",
            message=(
                "kill switch reset rejected; the expected active kill-switch "
                "transition does not match"
            ),
            previous_state=previous_state,
        )

    occurred_at = datetime.now(UTC)
    event_id = new_id()
    readiness_reason = (
        "kill switch reset; new entries remain paused pending explicit operator resume"
    )
    state_values = {
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": False,
        "readiness_reason": readiness_reason,
    }
    result = db.execute(
        update(RuntimeControlState)
        .where(
            RuntimeControlState.id == state.id,
            RuntimeControlState.kill_switch_enabled.is_(True),
            RuntimeControlState.last_transition_action == "kill_switch",
            RuntimeControlState.last_transition_event_id == expected_current_event_id,
        )
        .values(
            **state_values,
            last_transition_action="kill_switch_reset",
            last_transition_actor=command.actor,
            last_transition_source=command.source,
            last_transition_reason=command.reason,
            last_transition_at=occurred_at,
            last_transition_event_id=event_id,
        ),
    )
    if result.rowcount:
        event_previous_state, current_state = _durable_transition_event_snapshots(
            db=db,
            state=state,
            reviewed_previous_state=previous_state,
            state_values=state_values,
        )
        event = RuntimeControlEvent(
            id=event_id,
            runtime_control_state_id=state.id,
            action="kill_switch_reset",
            outcome="applied",
            actor=command.actor,
            source=command.source,
            reason=command.reason,
            message=(
                "kill switch reset; new entries remain paused until explicit resume"
            ),
            previous_state=event_previous_state,
            current_state=current_state,
            occurred_at=occurred_at,
        )
        db.add(event)
        db.commit()
        db.refresh(state)
        db.refresh(event)
        return ControlActionResult(
            action="kill_switch_reset",
            outcome="applied",
            message=event.message or "",
            event_id=event.id,
            safety=runtime_safety_payload(state),
        )

    db.rollback()
    state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    if state is None:
        raise RuntimeError("runtime control state disappeared during kill switch reset")
    return _finalize_action(
        db=db,
        state=state,
        command=command,
        action="kill_switch_reset",
        outcome="rejected",
        message="kill switch reset rejected; runtime control state changed concurrently",
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
        "hourly_risk_off_active": False,
        "hourly_risk_off_epoch": 0,
        "hourly_risk_off_context": None,
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
        "automation_enabled": effective_automation_enabled(state),
        "hourly_risk_off_active": state.hourly_risk_off_active,
        "hourly_risk_off_epoch": state.hourly_risk_off_epoch,
        "hourly_risk_off_context": state.hourly_risk_off_context,
        "readiness_reason": state.readiness_reason,
    }


def _durable_transition_event_snapshots(
    *,
    db: Session,
    state: RuntimeControlState,
    reviewed_previous_state: dict[str, object],
    state_values: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    """Build audit snapshots from the row version actually updated by the CAS."""

    db.refresh(state)
    current_state = _state_snapshot(state)
    previous_state = dict(current_state)
    for key in state_values:
        if key in reviewed_previous_state:
            previous_state[key] = reviewed_previous_state[key]
    previous_state["automation_enabled"] = not bool(
        previous_state.get("paused")
        or previous_state.get("kill_switch_enabled")
    )
    return previous_state, current_state


def _reload_runtime_control_state(*, db: Session) -> RuntimeControlState:
    db.rollback()
    state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    if state is None:
        raise RuntimeError("runtime control state disappeared during control transition")
    db.refresh(state)
    return state


def _apply_control_transition_compare_and_swap(
    *,
    db: Session,
    state: RuntimeControlState,
    command: ControlCommand,
    action: str,
    outcome: str,
    message: str,
    previous_state: dict[str, object],
    expected_state: dict[str, object],
    state_values: dict[str, object],
) -> ControlActionResult | None:
    """Apply an audited transition only while its reviewed state is still current."""

    occurred_at = datetime.now(UTC)
    event_id = new_id()
    predicates = [RuntimeControlState.id == state.id]
    for attribute_name, expected_value in expected_state.items():
        column = getattr(RuntimeControlState, attribute_name)
        predicates.append(
            column.is_(None) if expected_value is None else column == expected_value
        )
    result = db.execute(
        update(RuntimeControlState)
        .where(*predicates)
        .values(
            **state_values,
            last_transition_action=action,
            last_transition_actor=command.actor,
            last_transition_source=command.source,
            last_transition_reason=command.reason,
            last_transition_at=occurred_at,
            last_transition_event_id=event_id,
        )
    )
    if not result.rowcount:
        db.rollback()
        return None

    event_previous_state, current_state = _durable_transition_event_snapshots(
        db=db,
        state=state,
        reviewed_previous_state=previous_state,
        state_values=state_values,
    )
    event = RuntimeControlEvent(
        id=event_id,
        runtime_control_state_id=state.id,
        action=action,
        outcome=outcome,
        actor=command.actor,
        source=command.source,
        reason=command.reason,
        message=message,
        previous_state=event_previous_state,
        current_state=current_state,
        occurred_at=occurred_at,
    )
    db.add(event)
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
