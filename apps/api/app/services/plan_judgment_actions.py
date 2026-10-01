from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

from sqlalchemy import and_, or_, select, text, update
from sqlalchemy.orm import Session, undefer

from app.core.config import Settings
from app.models.runtime import (
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    RuntimeControlEvent,
    RuntimeControlState,
)
from app.services.runtime_controls import RUNTIME_CONTROL_ID, ensure_runtime_control_state

BUY_WATCH_INTENTS = ("intraday_watch_buy", "next_open_buy")
BUY_MUTABLE_STATUSES = (
    "watch_monitoring_active",
    "watch_monitoring_pending",
    "short_term_watcher_pending",
    "short_term_watch_blocked",
    "watch_buy_triggered_blocked",
    "next_open_buy_scheduled",
    "next_open_buy_retry_pending",
    "new_buy_blocked",
)
RISK_OFF_EXPIRABLE_STATUSES = (
    *BUY_MUTABLE_STATUSES,
    "watch_buy_triggered",
    "watch_buy_triggered_pending_dispatch",
    "triggered_pending_agent",
    "agent_delay",
    "agent_approved_pending_submit",
    "next_open_buy_dispatching",
)
ADJUSTMENT_KEY = "hourly_plan_action"
SOURCE = "hourly_runtime_check"
MAX_MUTATIONS_PER_RUN = 20


def _is_core_v4_bound(risk_context: dict[str, object]) -> bool:
    return isinstance(risk_context.get("_core_v4_contract_binding"), dict)


def _database_path(settings: Settings) -> str:
    parsed = urlparse(settings.database_url)
    if parsed.scheme not in {"sqlite", "sqlite+pysqlite"}:
        raise ValueError("hourly plan judgment action currently supports sqlite runtime DB only")
    if settings.database_url.startswith("sqlite:///./"):
        return str(Path(settings.database_url.removeprefix("sqlite:///")).resolve())
    if settings.database_url.startswith("sqlite:///../"):
        return str(Path(settings.database_url.removeprefix("sqlite:///")).resolve())
    if parsed.netloc and parsed.path:
        return unquote(f"/{parsed.netloc}{parsed.path}")
    path = unquote(parsed.path)
    if path.startswith("//"):
        return path[1:]
    if path.startswith("/"):
        return path
    return str(Path(path).resolve())


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(loaded) if isinstance(loaded, dict) else {}
    return {}


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _hourly_risk_off_fence_values(
    *,
    active: bool,
    epoch: int,
    raw_context: object,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
) -> tuple[bool, int, dict[str, object] | None, str | None]:
    """Return the authoritative buy-only risk fence transition."""

    context = _json_object(raw_context)
    required = settings.hourly_plan_judgment_recovery_required_healthy_observations
    cooldown_seconds = settings.hourly_plan_judgment_recovery_cooldown_seconds
    min_interval_seconds = (
        settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
    )
    normalized_epoch = _non_negative_int(epoch)
    stored_last_risk_at = _utc_datetime(context.get("last_risk_at"))
    effective_risk_at = (
        max(now, stored_last_risk_at)
        if active and stored_last_risk_at is not None
        else now
    )

    if risk_flags >= 3:
        next_epoch = normalized_epoch + 1
        activated_at = (
            context.get("activated_at")
            if active and _utc_datetime(context.get("activated_at")) is not None
            else now.isoformat()
        )
        return (
            True,
            next_epoch,
            {
                "status": "active",
                "epoch": next_epoch,
                "activated_at": activated_at,
                "last_risk_at": effective_risk_at.isoformat(),
                "last_risk_flags": risk_flags,
                "last_risk_reasons": list(risk_reasons),
                "activation_risk_flags": risk_flags,
                "activation_risk_reasons": list(risk_reasons),
                "consecutive_healthy_observations": 0,
                "required_healthy_observations": required,
                "cooldown_seconds": cooldown_seconds,
                "cooldown_until": (
                    effective_risk_at + timedelta(seconds=cooldown_seconds)
                ).isoformat(),
                "min_observation_interval_seconds": min_interval_seconds,
                "last_healthy_observed_at": None,
                "projection_pending": True,
                "projection_started_at": effective_risk_at.isoformat(),
                "recovery_ready": False,
            },
            "hourly_risk_off_activate",
        )

    if not active:
        return False, normalized_epoch, context or None, None

    if risk_flags >= 1:
        next_epoch = normalized_epoch + 1
        updated = dict(context)
        updated.update(
            {
                "status": "reset",
                "epoch": next_epoch,
                "last_risk_at": effective_risk_at.isoformat(),
                "last_risk_flags": risk_flags,
                "last_risk_reasons": list(risk_reasons),
                "consecutive_healthy_observations": 0,
                "required_healthy_observations": required,
                "cooldown_seconds": cooldown_seconds,
                "cooldown_until": (
                    effective_risk_at + timedelta(seconds=cooldown_seconds)
                ).isoformat(),
                "min_observation_interval_seconds": min_interval_seconds,
                "last_healthy_observed_at": None,
                "projection_pending": True,
                "projection_started_at": effective_risk_at.isoformat(),
                "recovery_ready": False,
            }
        )
        updated.pop("projection_completed_at", None)
        return True, next_epoch, updated, "hourly_risk_off_reset"

    current_count = _non_negative_int(
        context.get("consecutive_healthy_observations")
    )
    last_risk_at = (
        _utc_datetime(context.get("last_risk_at"))
        or _utc_datetime(context.get("activated_at"))
        or now
    )
    last_healthy_at = _utc_datetime(context.get("last_healthy_observed_at"))
    observation_advanced = (
        now > last_risk_at
        and (
            last_healthy_at is None
            or now
            >= last_healthy_at + timedelta(seconds=min_interval_seconds)
        )
    )
    healthy_count = min(
        required,
        current_count + (1 if observation_advanced else 0),
    )
    cooldown_until = last_risk_at + timedelta(seconds=cooldown_seconds)
    recovery_ready = healthy_count >= required and now >= cooldown_until
    updated = dict(context)
    updated.update(
        {
            "status": "observing",
            "epoch": normalized_epoch,
            "consecutive_healthy_observations": healthy_count,
            "required_healthy_observations": required,
            "cooldown_seconds": cooldown_seconds,
            "cooldown_until": cooldown_until.isoformat(),
            "cooldown_satisfied": now >= cooldown_until,
            "min_observation_interval_seconds": min_interval_seconds,
            "last_risk_flags": 0,
            "last_risk_reasons": [],
            "last_healthy_observed_at": (
                now.isoformat()
                if observation_advanced
                else context.get("last_healthy_observed_at")
            ),
            "projection_pending": True,
            "projection_started_at": now.isoformat(),
            "recovery_ready": recovery_ready,
        }
    )
    updated.pop("projection_completed_at", None)
    return (
        True,
        normalized_epoch,
        updated,
        "hourly_risk_off_observed" if observation_advanced else None,
    )


def _hourly_risk_off_fence_payload(
    *,
    available: bool,
    active: bool,
    epoch: int,
    context: dict[str, object] | None,
    event_type: str | None,
) -> dict[str, object]:
    return {
        "available": available,
        "active": active,
        "epoch": epoch,
        "context": context,
        "transition": event_type,
    }


def _transition_hourly_risk_off_fence_session(
    *,
    db: Session,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
) -> dict[str, object]:
    ensure_runtime_control_state(settings=settings, db=db)
    if db.get_bind().dialect.name == "sqlite":
        # SQLite ignores FOR UPDATE. A no-op write obtains the database's write
        # reservation so broker submission and risk transitions have one
        # unambiguous order.
        db.execute(
            update(RuntimeControlState)
            .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
            .values(
                hourly_risk_off_epoch=RuntimeControlState.hourly_risk_off_epoch
            )
        )
    state = db.scalar(
        select(RuntimeControlState)
        .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
        .with_for_update()
    )
    if state is None:
        raise RuntimeError("runtime control state disappeared during risk transition")
    db.refresh(
        state,
        attribute_names=[
            "hourly_risk_off_active",
            "hourly_risk_off_epoch",
            "hourly_risk_off_context",
        ],
    )
    previous = {
        "active": state.hourly_risk_off_active,
        "epoch": state.hourly_risk_off_epoch,
        "context": state.hourly_risk_off_context,
    }
    active, epoch, context, event_type = _hourly_risk_off_fence_values(
        active=state.hourly_risk_off_active,
        epoch=state.hourly_risk_off_epoch,
        raw_context=state.hourly_risk_off_context,
        settings=settings,
        risk_flags=risk_flags,
        risk_reasons=risk_reasons,
        now=now,
    )
    state_changed = (
        state.hourly_risk_off_active != active
        or state.hourly_risk_off_epoch != epoch
        or _json_object(state.hourly_risk_off_context) != _json_object(context)
    )
    if state_changed:
        state.hourly_risk_off_active = active
        state.hourly_risk_off_epoch = epoch
        state.hourly_risk_off_context = context
    if event_type is None:
        return _hourly_risk_off_fence_payload(
            available=True,
            active=active,
            epoch=epoch,
            context=context,
            event_type=None,
        )
    db.add(
        RuntimeControlEvent(
            runtime_control_state_id=state.id,
            action=event_type,
            outcome="applied",
            source=SOURCE,
            reason=", ".join(risk_reasons) or None,
            message=(
                "hourly buy risk-off fence cleared after confirmed recovery"
                if not active
                else "hourly buy risk-off fence remains active"
            ),
            previous_state=previous,
            current_state={
                "active": active,
                "epoch": epoch,
                "context": context,
            },
            occurred_at=now,
        )
    )
    return _hourly_risk_off_fence_payload(
        available=True,
        active=active,
        epoch=epoch,
        context=context,
        event_type=event_type,
    )


def _sqlite_has_hourly_risk_off_fence(conn: sqlite3.Connection) -> bool:
    try:
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(runtime_control_state)")
        }
    except sqlite3.DatabaseError:
        return False
    return {
        "hourly_risk_off_active",
        "hourly_risk_off_epoch",
        "hourly_risk_off_context",
    }.issubset(columns)


def _transition_hourly_risk_off_fence_sqlite(
    *,
    conn: sqlite3.Connection,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
) -> dict[str, object]:
    if not _sqlite_has_hourly_risk_off_fence(conn):
        return _hourly_risk_off_fence_payload(
            available=False,
            active=False,
            epoch=0,
            context=None,
            event_type=None,
        )
    row = conn.execute(
        """
        select hourly_risk_off_active, hourly_risk_off_epoch,
               hourly_risk_off_context
        from runtime_control_state
        where id = ?
        """,
        (RUNTIME_CONTROL_ID,),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime control state is missing during risk transition")
    previous = {
        "active": bool(row[0]),
        "epoch": _non_negative_int(row[1]),
        "context": _json_object(row[2]) or None,
    }
    active, epoch, context, event_type = _hourly_risk_off_fence_values(
        active=bool(row[0]),
        epoch=_non_negative_int(row[1]),
        raw_context=row[2],
        settings=settings,
        risk_flags=risk_flags,
        risk_reasons=risk_reasons,
        now=now,
    )
    state_changed = (
        previous["active"] != active
        or previous["epoch"] != epoch
        or _json_object(previous["context"]) != _json_object(context)
    )
    if state_changed:
        conn.execute(
            """
            update runtime_control_state
            set hourly_risk_off_active = ?, hourly_risk_off_epoch = ?,
                hourly_risk_off_context = ?, updated_at = ?
            where id = ?
            """,
            (
                1 if active else 0,
                epoch,
                json.dumps(context, ensure_ascii=False),
                now.isoformat(),
                RUNTIME_CONTROL_ID,
            ),
        )
    if event_type is not None:
        conn.execute(
            """
            insert into runtime_control_events (
              id, runtime_control_state_id, action, outcome, actor, source,
              reason, message, previous_state, current_state, occurred_at,
              created_at, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                RUNTIME_CONTROL_ID,
                event_type,
                "applied",
                None,
                SOURCE,
                ", ".join(risk_reasons) or None,
                (
                    "hourly buy risk-off fence cleared after confirmed recovery"
                    if not active
                    else "hourly buy risk-off fence remains active"
                ),
                json.dumps(previous, ensure_ascii=False),
                json.dumps(
                    {"active": active, "epoch": epoch, "context": context},
                    ensure_ascii=False,
                ),
                now.isoformat(),
                now.isoformat(),
                now.isoformat(),
            ),
        )
    return _hourly_risk_off_fence_payload(
        available=True,
        active=active,
        epoch=epoch,
        context=context,
        event_type=event_type,
    )


def _lock_hourly_risk_off_fence_session(db: Session) -> RuntimeControlState:
    if db.get_bind().dialect.name == "sqlite":
        db.execute(
            update(RuntimeControlState)
            .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
            .values(
                hourly_risk_off_epoch=RuntimeControlState.hourly_risk_off_epoch
            )
        )
    state = db.scalar(
        select(RuntimeControlState)
        .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
        .with_for_update()
    )
    if state is None:
        raise RuntimeError("runtime control state disappeared during risk projection")
    db.refresh(
        state,
        attribute_names=[
            "hourly_risk_off_active",
            "hourly_risk_off_epoch",
            "hourly_risk_off_context",
        ],
    )
    return state


def _lock_execution_plan_items_for_hourly_projection(db: Session) -> None:
    """Freeze PostgreSQL plan-item DML through the final scan and fence clear.

    The runtime-control row serializes broker boundaries and risk transitions,
    but an execution-plan insert does not take that row lock.  A table-level
    EXCLUSIVE lock makes the final mutable-buy scan a closed set: an earlier
    writer commits before the scan, while a later writer is linearized after
    the projection transaction clears (or keeps) the global fence. It also
    blocks concurrent ``SELECT FOR UPDATE`` writers before they can hold an
    item row and deadlock against this transaction's later projection update.
    """

    if db.get_bind().dialect.name == "postgresql":
        db.execute(text("LOCK TABLE execution_plan_items IN EXCLUSIVE MODE"))


def _complete_hourly_risk_off_projection_session(
    *,
    db: Session,
    state: RuntimeControlState,
    expected_epoch: int,
    now: datetime,
) -> dict[str, object]:
    context = _json_object(state.hourly_risk_off_context)
    if (
        not state.hourly_risk_off_active
        or state.hourly_risk_off_epoch != expected_epoch
    ):
        return _hourly_risk_off_fence_payload(
            available=True,
            active=state.hourly_risk_off_active,
            epoch=state.hourly_risk_off_epoch,
            context=context or None,
            event_type=None,
        )

    previous = {
        "active": state.hourly_risk_off_active,
        "epoch": state.hourly_risk_off_epoch,
        "context": context or None,
    }
    recovery_ready = bool(context.get("recovery_ready"))
    context["projection_pending"] = False
    context["projection_completed_at"] = now.isoformat()
    if recovery_ready:
        context.update(
            {
                "status": "recovered",
                "recovered_at": now.isoformat(),
                "recovery_ready": False,
            }
        )
    state.hourly_risk_off_active = not recovery_ready
    state.hourly_risk_off_context = context
    event_type = "hourly_risk_off_recovered" if recovery_ready else None
    if event_type is not None:
        db.add(
            RuntimeControlEvent(
                runtime_control_state_id=state.id,
                action=event_type,
                outcome="applied",
                source=SOURCE,
                message="hourly buy risk-off fence cleared after confirmed recovery",
                previous_state=previous,
                current_state={
                    "active": state.hourly_risk_off_active,
                    "epoch": state.hourly_risk_off_epoch,
                    "context": context,
                },
                occurred_at=now,
            )
        )
    return _hourly_risk_off_fence_payload(
        available=True,
        active=state.hourly_risk_off_active,
        epoch=state.hourly_risk_off_epoch,
        context=context,
        event_type=event_type,
    )


def _complete_hourly_risk_off_projection_sqlite(
    *,
    conn: sqlite3.Connection,
    expected_epoch: int,
    now: datetime,
) -> dict[str, object]:
    row = conn.execute(
        """
        select hourly_risk_off_active, hourly_risk_off_epoch,
               hourly_risk_off_context
        from runtime_control_state
        where id = ?
        """,
        (RUNTIME_CONTROL_ID,),
    ).fetchone()
    if row is None:
        raise RuntimeError("runtime control state is missing during risk projection")
    active = bool(row[0])
    epoch = _non_negative_int(row[1])
    context = _json_object(row[2])
    if not active or epoch != expected_epoch:
        return _hourly_risk_off_fence_payload(
            available=True,
            active=active,
            epoch=epoch,
            context=context or None,
            event_type=None,
        )

    previous = {"active": active, "epoch": epoch, "context": context or None}
    recovery_ready = bool(context.get("recovery_ready"))
    context["projection_pending"] = False
    context["projection_completed_at"] = now.isoformat()
    if recovery_ready:
        context.update(
            {
                "status": "recovered",
                "recovered_at": now.isoformat(),
                "recovery_ready": False,
            }
        )
    active = not recovery_ready
    conn.execute(
        """
        update runtime_control_state
        set hourly_risk_off_active = ?, hourly_risk_off_context = ?, updated_at = ?
        where id = ? and hourly_risk_off_epoch = ?
        """,
        (
            1 if active else 0,
            json.dumps(context, ensure_ascii=False),
            now.isoformat(),
            RUNTIME_CONTROL_ID,
            expected_epoch,
        ),
    )
    event_type = "hourly_risk_off_recovered" if recovery_ready else None
    if event_type is not None:
        conn.execute(
            """
            insert into runtime_control_events (
              id, runtime_control_state_id, action, outcome, actor, source,
              reason, message, previous_state, current_state, occurred_at,
              created_at, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                RUNTIME_CONTROL_ID,
                event_type,
                "applied",
                None,
                SOURCE,
                None,
                "hourly buy risk-off fence cleared after confirmed recovery",
                json.dumps(previous, ensure_ascii=False),
                json.dumps(
                    {"active": active, "epoch": epoch, "context": context},
                    ensure_ascii=False,
                ),
                now.isoformat(),
                now.isoformat(),
                now.isoformat(),
            ),
        )
    return _hourly_risk_off_fence_payload(
        available=True,
        active=active,
        epoch=epoch,
        context=context,
        event_type=event_type,
    )


def _price_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN))


def _risk_off_resume_snapshot(
    *,
    previous: dict[str, object],
    prior_adjustment: object,
) -> dict[str, object]:
    resume = dict(previous)
    previous_status = str(previous.get("status") or "")
    if previous_status == "next_open_buy_dispatching":
        # The job is put back into a schedulable state when a dispatch loses
        # the race to risk-off. Never restore an orphaned in-flight label.
        resume["status"] = "next_open_buy_retry_pending"
    elif previous_status in {
        "watch_buy_triggered",
        "watch_buy_triggered_pending_dispatch",
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
    }:
        # Risk-off invalidates the old quote/trigger/agent decision. Recovery
        # returns to fresh monitoring rather than replaying stale approval.
        resume["status"] = "watch_monitoring_active"
    if not isinstance(prior_adjustment, dict):
        return resume
    if prior_adjustment.get("action") != "conservative_price_adjust":
        return resume
    original = prior_adjustment.get("original")
    if not isinstance(original, dict):
        return resume
    for key in ("trigger_price", "limit_price"):
        if key in original:
            resume[key] = original.get(key)
    return resume


def _utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _risk_off_recovery_transition(
    *,
    adjustment: dict[str, Any],
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
    resume_allowed: bool = True,
) -> tuple[dict[str, Any], str, str | None, dict[str, object]]:
    """Advance only the recovery state created by our own risk-off expiry."""

    updated = dict(adjustment)
    previous = updated.get("previous") if isinstance(updated.get("previous"), dict) else {}
    resume = updated.get("resume") if isinstance(updated.get("resume"), dict) else previous
    resume_status = str(resume.get("status") or previous.get("status") or "")
    updated["resume"] = dict(resume)
    recovery = dict(updated.get("recovery")) if isinstance(updated.get("recovery"), dict) else {}
    required = settings.hourly_plan_judgment_recovery_required_healthy_observations
    cooldown_seconds = settings.hourly_plan_judgment_recovery_cooldown_seconds
    applied_at = _utc_datetime(updated.get("applied_at")) or now
    last_risk_at = _utc_datetime(recovery.get("last_risk_at")) or applied_at
    current_count = _non_negative_int(
        recovery.get("consecutive_healthy_observations")
    )

    if risk_flags >= 1:
        had_healthy_progress = current_count > 0 or recovery.get("status") == "observing"
        last_risk_at = max(last_risk_at, now)
        recovery.update(
            {
                "status": "reset",
                "consecutive_healthy_observations": 0,
                "required_healthy_observations": required,
                "cooldown_seconds": cooldown_seconds,
                "cooldown_until": (last_risk_at + timedelta(seconds=cooldown_seconds)).isoformat(),
                "cooldown_satisfied": False,
                "last_risk_at": last_risk_at.isoformat(),
                "last_risk_flags": risk_flags,
                "last_risk_reasons": list(risk_reasons),
                # A risky reading resets the healthy streak; it is not itself a
                # healthy observation anchor.  The first later healthy reading
                # may be counted immediately, while cooldown still prevents an
                # early resume.
                "last_observed_at": None,
            }
        )
        updated["recovery"] = recovery
        return (
            updated,
            "watch_expired",
            "hourly_plan_judgment_buy_watch_recovery_reset" if had_healthy_progress else None,
            {
                "previous_status": resume_status,
                "risk_flags": risk_flags,
                "risk_reasons": list(risk_reasons),
                "consecutive_healthy_observations": 0,
                "cooldown_until": recovery["cooldown_until"],
            },
        )

    last_observed_at = _utc_datetime(recovery.get("last_observed_at"))
    min_observation_interval = timedelta(
        seconds=(
            settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
        )
    )
    observation_advanced = (
        now > last_risk_at
        and (
            last_observed_at is None
            or now >= last_observed_at + min_observation_interval
        )
    )
    healthy_count = min(required, current_count + (1 if observation_advanced else 0))
    cooldown_until = last_risk_at + timedelta(seconds=cooldown_seconds)
    cooldown_satisfied = now >= cooldown_until
    previous_status_is_safe = resume_status in BUY_MUTABLE_STATUSES
    should_resume = (
        resume_allowed
        and previous_status_is_safe
        and healthy_count >= required
        and cooldown_satisfied
    )
    recovery.update(
        {
            "status": "resumed" if should_resume else "observing",
            "consecutive_healthy_observations": healthy_count,
            "required_healthy_observations": required,
            "min_observation_interval_seconds": int(
                min_observation_interval.total_seconds()
            ),
            "cooldown_seconds": cooldown_seconds,
            "cooldown_until": cooldown_until.isoformat(),
            "cooldown_satisfied": cooldown_satisfied,
            "last_risk_at": last_risk_at.isoformat(),
            "last_risk_flags": 0,
            "last_risk_reasons": [],
            "last_observed_at": now.isoformat() if observation_advanced else recovery.get("last_observed_at"),
        }
    )
    if not previous_status_is_safe:
        recovery["status"] = "invalid_previous_status"
    if should_resume:
        recovery["resumed_at"] = now.isoformat()
        updated["action"] = "risk_off_resume"
    updated["recovery"] = recovery
    event_type = (
        "hourly_plan_judgment_buy_watch_resumed"
        if should_resume
        else "hourly_plan_judgment_buy_watch_recovery_observed"
        if observation_advanced
        else None
    )
    return (
        updated,
        resume_status if should_resume else "watch_expired",
        event_type,
        {
            "previous_status": resume_status,
            "restored_trigger_price": resume.get("trigger_price") if should_resume else None,
            "restored_limit_price": resume.get("limit_price") if should_resume else None,
            "consecutive_healthy_observations": healthy_count,
            "required_healthy_observations": required,
            "cooldown_until": cooldown_until.isoformat(),
            "cooldown_satisfied": cooldown_satisfied,
            "resumed": should_resume,
        },
    )


def _initial_risk_off_recovery(
    *,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
) -> dict[str, object]:
    cooldown_seconds = settings.hourly_plan_judgment_recovery_cooldown_seconds
    return {
        "status": "blocked",
        "consecutive_healthy_observations": 0,
        "required_healthy_observations": (
            settings.hourly_plan_judgment_recovery_required_healthy_observations
        ),
        "cooldown_seconds": cooldown_seconds,
        "cooldown_until": (now + timedelta(seconds=cooldown_seconds)).isoformat(),
        "cooldown_satisfied": False,
        "last_risk_at": now.isoformat(),
        "last_risk_flags": risk_flags,
        "last_risk_reasons": list(risk_reasons),
        "last_observed_at": None,
    }


def _risk_off_projection_inputs(
    *,
    fence_context: dict[str, object] | None,
    fallback_risk_flags: int,
    fallback_risk_reasons: list[str],
) -> tuple[int, list[str]]:
    context = fence_context or {}
    risk_flags = max(
        3,
        _non_negative_int(
            context.get("activation_risk_flags")
            or context.get("last_risk_flags")
            or fallback_risk_flags
        ),
    )
    raw_reasons = (
        context.get("activation_risk_reasons")
        or context.get("last_risk_reasons")
        or fallback_risk_reasons
    )
    risk_reasons = (
        [str(reason) for reason in raw_reasons]
        if isinstance(raw_reasons, list)
        else list(fallback_risk_reasons)
    )
    return risk_flags, risk_reasons


def _seed_recovery_from_fence_context(
    *,
    recovery: dict[str, object],
    fence_context: dict[str, object] | None,
    now: datetime,
) -> dict[str, object]:
    if not isinstance(fence_context, dict):
        return recovery
    updated = dict(recovery)
    healthy_count = _non_negative_int(
        fence_context.get("consecutive_healthy_observations")
    )
    updated.update(
        {
            "status": "observing" if healthy_count else "blocked",
            "consecutive_healthy_observations": healthy_count,
            "last_risk_at": fence_context.get("last_risk_at") or now.isoformat(),
            "last_observed_at": fence_context.get("last_healthy_observed_at"),
            "cooldown_until": (
                fence_context.get("cooldown_until") or recovery["cooldown_until"]
            ),
            "cooldown_satisfied": bool(
                fence_context.get("cooldown_satisfied", False)
            ),
        }
    )
    return updated


def _connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(_database_path(settings), timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _load_mutable_buy_items(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    after: tuple[str, str, str] | None = None,
    statuses: tuple[str, ...] = BUY_MUTABLE_STATUSES,
) -> list[sqlite3.Row]:
    placeholders_intents = ",".join("?" for _ in BUY_WATCH_INTENTS)
    placeholders_statuses = ",".join("?" for _ in statuses)
    cursor_clause = ""
    cursor_params: tuple[str, str, str, str, str, str] = ()
    if after is not None:
        symbol, created_at, item_id = after
        cursor_clause = """
              and (
                i.symbol > ?
                or (i.symbol = ? and i.created_at > ?)
                or (i.symbol = ? and i.created_at = ? and i.id > ?)
              )
        """
        cursor_params = (symbol, symbol, created_at, symbol, created_at, item_id)
    return list(
        conn.execute(
            f"""
            select i.id, i.symbol, i.status, i.trigger_price, i.limit_price, i.risk_context, i.created_at
            from execution_plan_items i
            join execution_plans p on p.id = i.execution_plan_id
            where p.mode = ?
              and i.side = 'buy'
              and i.intent in ({placeholders_intents})
              and i.status in ({placeholders_statuses})
              {cursor_clause}
            order by i.symbol asc, i.created_at asc, i.id asc
            limit ?
            """,
            (
                settings.runtime_mode,
                *BUY_WATCH_INTENTS,
                *statuses,
                *cursor_params,
                MAX_MUTATIONS_PER_RUN,
            ),
        )
    )


def _insert_audit(
    conn: sqlite3.Connection,
    *,
    item_id: str,
    event_type: str,
    summary: dict[str, object],
    payload: dict[str, object],
    now: str,
) -> None:
    conn.execute(
        """
        insert into execution_plan_item_audits (
          id, execution_plan_item_id, event_type, source, summary, payload,
          occurred_at, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            item_id,
            event_type,
            SOURCE,
            json.dumps(summary, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            now,
            now,
            now,
        ),
    )


def apply_hourly_plan_judgment_action(
    *,
    settings: Settings,
    db: Session | None = None,
    risk_flags: int,
    risk_reasons: list[str],
) -> dict[str, object]:
    if urlparse(settings.database_url).scheme not in {"sqlite", "sqlite+pysqlite"}:
        if db is None:
            raise ValueError("a database session is required for non-sqlite plan judgment actions")
        return _apply_hourly_plan_judgment_action_session(
            db=db,
            settings=settings,
            risk_flags=risk_flags,
            risk_reasons=risk_reasons,
        )
    fence_persisted = False
    risk_fence: dict[str, object] | None = None
    try:
        with _connect(settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = datetime.now(UTC)
            risk_fence = _transition_hourly_risk_off_fence_sqlite(
                conn=conn,
                settings=settings,
                risk_flags=risk_flags,
                risk_reasons=risk_reasons,
                now=now,
            )
            # The global fence is the authoritative broker-boundary control.
            # Commit it before best-effort item projection so a later item/audit
            # failure cannot roll back a severe market safety decision.
            conn.commit()
            fence_persisted = bool(risk_fence.get("available"))
            expected_epoch = _non_negative_int(risk_fence.get("epoch"))
            conn.execute("BEGIN IMMEDIATE")
            if risk_fence.get("available"):
                current_fence = conn.execute(
                    """
                    select hourly_risk_off_active, hourly_risk_off_epoch,
                           hourly_risk_off_context
                    from runtime_control_state
                    where id = ?
                    """,
                    (RUNTIME_CONTROL_ID,),
                ).fetchone()
                if current_fence is None:
                    raise RuntimeError(
                        "runtime control state is missing during risk projection"
                    )
                fence_active = bool(current_fence[0])
                current_epoch = _non_negative_int(current_fence[1])
                fence_context = _json_object(current_fence[2])
                epoch_matches = current_epoch == expected_epoch
                risk_fence = _hourly_risk_off_fence_payload(
                    available=True,
                    active=fence_active,
                    epoch=current_epoch,
                    context=fence_context or None,
                    event_type=(
                        risk_fence.get("transition") if epoch_matches else None
                    ),
                )
            else:
                fence_active = False
                fence_context = {}
                epoch_matches = True
            projected = 0
            if fence_active:
                projection_risk_flags, projection_risk_reasons = (
                    _risk_off_projection_inputs(
                        fence_context=fence_context,
                        fallback_risk_flags=risk_flags,
                        fallback_risk_reasons=risk_reasons,
                    )
                )
                projected = _expire_buy_watches(
                    conn,
                    settings,
                    projection_risk_flags,
                    projection_risk_reasons,
                )
            if risk_flags >= 3:
                if not fence_active:
                    projected = _expire_buy_watches(
                        conn,
                        settings,
                        risk_flags,
                        risk_reasons,
                    )
                changed = projected
                message = f"risk_off: 신규/추가매수 watch {changed}건 중단"
                action = "risk_off_expire"
            elif risk_flags >= 1:
                adjusted = _adjust_buy_watch_prices(
                    conn,
                    settings,
                    risk_flags,
                    risk_reasons,
                )
                changed = projected + adjusted
                message = f"caution: 신규/추가매수 trigger/limit 1% 보수화 {adjusted}건"
                action = "conservative_price_adjust"
            else:
                restored = _restore_conservative_adjustments(conn, settings)
                changed = projected + restored
                message = f"normal: 보수화 가격선 원복 {restored}건"
                action = "restore_conservative_price_adjust"
            recovery_ready = (
                fence_active
                and epoch_matches
                and bool(fence_context.get("recovery_ready"))
            )
            recovery_counts = _advance_risk_off_recoveries(
                conn,
                settings,
                risk_flags=risk_flags,
                risk_reasons=risk_reasons,
                now=now,
                resume_allowed=(
                    recovery_ready
                    if fence_active
                    else not bool(risk_fence["active"])
                ),
                reconcile_all=fence_active,
            )
            changed += recovery_counts["resumed"]
            if recovery_counts["resumed"]:
                message = f"{message}; risk-off watch {recovery_counts['resumed']}건 재개"
            if fence_active:
                risk_fence = _complete_hourly_risk_off_projection_sqlite(
                    conn=conn,
                    expected_epoch=expected_epoch,
                    now=now,
                )
            conn.commit()
            return {
                "status": "applied",
                "action": action,
                "message": message,
                "changed_count": changed,
                "recovery_observed_count": recovery_counts["observed"],
                "recovery_reset_count": recovery_counts["reset"],
                "recovery_resumed_count": recovery_counts["resumed"],
                "hourly_risk_off": risk_fence,
            }
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            if (
                fence_persisted
                and risk_fence is not None
                and bool(risk_fence.get("active"))
            ):
                return {
                    "status": "applied",
                    "action": (
                        "risk_off_expire"
                        if risk_flags >= 3
                        else "risk_off_reconcile"
                    ),
                    "message": (
                        "hourly risk-off fence is active; per-item projection "
                        "is pending because the runtime database became busy"
                    ),
                    "changed_count": 0,
                    "projection_status": "pending",
                    "hourly_risk_off": risk_fence,
                }
            if risk_flags >= 3:
                return {
                    "status": "retry_required",
                    "action": "db_busy",
                    "message": "hourly risk-off fence was not persisted because the runtime database is busy",
                    "changed_count": 0,
                }
            return {"status": "skipped", "action": "db_busy", "message": "skip(db_busy)", "changed_count": 0}
        raise


def _load_mutable_buy_items_session(
    db: Session,
    settings: Settings,
    *,
    after: tuple[str, datetime, str] | None = None,
    statuses: tuple[str, ...] = BUY_MUTABLE_STATUSES,
) -> list[ExecutionPlanItem]:
    filters = [
        ExecutionPlan.mode == settings.runtime_mode,
        ExecutionPlanItem.side == "buy",
        ExecutionPlanItem.intent.in_(BUY_WATCH_INTENTS),
        ExecutionPlanItem.status.in_(statuses),
    ]
    if after is not None:
        symbol, created_at, item_id = after
        filters.append(
            or_(
                ExecutionPlanItem.symbol > symbol,
                and_(ExecutionPlanItem.symbol == symbol, ExecutionPlanItem.created_at > created_at),
                and_(
                    ExecutionPlanItem.symbol == symbol,
                    ExecutionPlanItem.created_at == created_at,
                    ExecutionPlanItem.id > item_id,
                ),
            )
        )
    return list(
        db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlan, ExecutionPlan.id == ExecutionPlanItem.execution_plan_id)
            .options(undefer(ExecutionPlanItem.risk_context))
            .where(*filters)
            .order_by(ExecutionPlanItem.symbol, ExecutionPlanItem.created_at, ExecutionPlanItem.id)
            .limit(MAX_MUTATIONS_PER_RUN)
        ).all()
    )


def _load_risk_off_expired_buy_items_session(
    db: Session,
    settings: Settings,
    *,
    max_items: int | None = MAX_MUTATIONS_PER_RUN,
) -> list[ExecutionPlanItem]:
    matched: list[ExecutionPlanItem] = []
    after_id = ""
    while max_items is None or len(matched) < max_items:
        rows = list(
            db.scalars(
                select(ExecutionPlanItem)
                .join(ExecutionPlan, ExecutionPlan.id == ExecutionPlanItem.execution_plan_id)
                .options(undefer(ExecutionPlanItem.risk_context))
                .where(
                    ExecutionPlan.mode == settings.runtime_mode,
                    ExecutionPlanItem.side == "buy",
                    ExecutionPlanItem.intent.in_(BUY_WATCH_INTENTS),
                    ExecutionPlanItem.status == "watch_expired",
                    ExecutionPlanItem.id > after_id,
                )
                .order_by(ExecutionPlanItem.id)
                .limit(MAX_MUTATIONS_PER_RUN)
            ).all()
        )
        if not rows:
            break
        for item in rows:
            risk_context = _json_object(item.risk_context)
            adjustment = risk_context.get(ADJUSTMENT_KEY)
            if (
                isinstance(adjustment, dict)
                and adjustment.get("action") == "risk_off_expire"
                and adjustment.get("source") == SOURCE
            ):
                matched.append(item)
                if max_items is not None and len(matched) >= max_items:
                    break
        after_id = rows[-1].id
        if len(rows) < MAX_MUTATIONS_PER_RUN:
            break
    return matched


def _insert_audit_session(
    db: Session,
    *,
    item: ExecutionPlanItem,
    event_type: str,
    summary: dict[str, object],
    payload: dict[str, object],
    now: datetime,
) -> None:
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=event_type,
            source=SOURCE,
            summary=summary,
            payload=payload,
            occurred_at=now,
            created_at=now,
            updated_at=now,
        )
    )


def _advance_risk_off_recoveries_session(
    db: Session,
    settings: Settings,
    *,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
    resume_allowed: bool,
    reconcile_all: bool = False,
) -> dict[str, int]:
    counts = {"observed": 0, "reset": 0, "resumed": 0}
    for item in _load_risk_off_expired_buy_items_session(
        db,
        settings,
        max_items=None if reconcile_all else MAX_MUTATIONS_PER_RUN,
    ):
        risk_context = _json_object(item.risk_context)
        adjustment = risk_context.get(ADJUSTMENT_KEY)
        if not isinstance(adjustment, dict):
            continue
        updated_adjustment, next_status, event_type, event_payload = _risk_off_recovery_transition(
            adjustment=adjustment,
            settings=settings,
            risk_flags=risk_flags,
            risk_reasons=risk_reasons,
            now=now,
            resume_allowed=resume_allowed,
        )
        risk_context[ADJUSTMENT_KEY] = updated_adjustment
        values: dict[str, object] = {
            "status": next_status,
            "risk_context": risk_context,
            "updated_at": now,
        }
        if updated_adjustment["recovery"]["status"] == "resumed":
            resume = updated_adjustment.get("resume")
            if isinstance(resume, dict):
                if "trigger_price" in resume:
                    values["trigger_price"] = _decimal_or_none(
                        resume.get("trigger_price")
                    )
                if "limit_price" in resume:
                    values["limit_price"] = _decimal_or_none(
                        resume.get("limit_price")
                    )
        result = db.execute(
            update(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.id == item.id,
                ExecutionPlanItem.status == "watch_expired",
            )
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        if not result.rowcount:
            continue
        if event_type is None:
            continue
        event_kind = (
            "resumed"
            if event_type.endswith("_resumed")
            else "reset"
            if event_type.endswith("_reset")
            else "observed"
        )
        counts[event_kind] += 1
        _insert_audit_session(
            db,
            item=item,
            event_type=event_type,
            summary={
                "symbol": item.symbol,
                "action": updated_adjustment.get("action"),
                "recovery_status": updated_adjustment["recovery"]["status"],
            },
            payload=event_payload,
            now=now,
        )
    return counts


def _expire_buy_watch_item_session(
    db: Session,
    *,
    item: ExecutionPlanItem,
    risk_context: dict[str, object],
    now: datetime,
) -> bool:
    result = db.execute(
        update(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.id == item.id,
            ExecutionPlanItem.status == item.status,
        )
        .values(status="watch_expired", risk_context=risk_context, updated_at=now)
        .execution_options(synchronize_session=False)
    )
    return bool(result.rowcount)


def project_buy_plan_item_for_hourly_risk_off(
    *,
    db: Session,
    settings: Settings,
    item: ExecutionPlanItem,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
    resume_status: str | None = None,
    fence_context: dict[str, object] | None = None,
) -> bool:
    """Persist the standard recoverable projection for one buy candidate.

    Broker submission uses this when the global fence wins after an order
    intent was committed.  That late candidate must carry the same recovery
    contract as candidates found by the hourly projection scan.
    """

    if item.side != "buy" or item.intent not in BUY_WATCH_INTENTS:
        return False
    risk_context = _json_object(item.risk_context)
    existing = risk_context.get(ADJUSTMENT_KEY)
    if (
        isinstance(existing, dict)
        and existing.get("source") == SOURCE
        and existing.get("action") == "risk_off_expire"
    ):
        if item.status == "watch_expired":
            return False
        # A stale ORM writer may have restored an in-flight status while the
        # durable recovery marker survived. Reassert the marker's projection;
        # never leave an executable status paired with active risk-off state.
        return _expire_buy_watch_item_session(
            db,
            item=item,
            risk_context=risk_context,
            now=now,
        )
    previous = {
        "status": resume_status or item.status,
        "trigger_price": (
            str(item.trigger_price) if item.trigger_price is not None else None
        ),
        "limit_price": str(item.limit_price) if item.limit_price is not None else None,
    }
    recovery = _seed_recovery_from_fence_context(
        recovery=_initial_risk_off_recovery(
            settings=settings,
            risk_flags=risk_flags,
            risk_reasons=risk_reasons,
            now=now,
        ),
        fence_context=fence_context,
        now=now,
    )
    risk_context[ADJUSTMENT_KEY] = {
        "action": "risk_off_expire",
        "source": SOURCE,
        "risk_flags": risk_flags,
        "risk_reasons": list(risk_reasons),
        "previous": previous,
        "resume": _risk_off_resume_snapshot(
            previous=previous,
            prior_adjustment=existing,
        ),
        "applied_at": now.isoformat(),
        "recovery": recovery,
    }
    if not _expire_buy_watch_item_session(
        db,
        item=item,
        risk_context=risk_context,
        now=now,
    ):
        return False
    _insert_audit_session(
        db,
        item=item,
        event_type="hourly_plan_judgment_buy_watch_expired",
        summary={
            "symbol": item.symbol,
            "action": "risk_off_expire",
            "risk_flags": risk_flags,
        },
        payload={"risk_reasons": list(risk_reasons), "previous": previous},
        now=now,
    )
    return True


def _apply_hourly_plan_judgment_action_session(
    *,
    db: Session,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> dict[str, object]:
    changed = 0
    now = datetime.now(UTC)
    risk_fence = _transition_hourly_risk_off_fence_session(
        db=db,
        settings=settings,
        risk_flags=risk_flags,
        risk_reasons=risk_reasons,
        now=now,
    )
    # Persist the authoritative global fence independently from the denormalized
    # per-item projections. A projection failure may delay recovery visibility,
    # but it must never reopen broker submission after severe risk was observed.
    db.commit()
    transition_event = risk_fence.get("transition")
    expected_epoch = _non_negative_int(risk_fence.get("epoch"))
    projection_state = _lock_hourly_risk_off_fence_session(db)
    epoch_matches = projection_state.hourly_risk_off_epoch == expected_epoch
    fence_active = bool(projection_state.hourly_risk_off_active)
    if fence_active:
        _lock_execution_plan_items_for_hourly_projection(db)
    fence_context = _json_object(projection_state.hourly_risk_off_context)
    risk_fence = _hourly_risk_off_fence_payload(
        available=True,
        active=fence_active,
        epoch=projection_state.hourly_risk_off_epoch,
        context=fence_context or None,
        event_type=transition_event if epoch_matches else None,
    )
    if fence_active:
        projection_risk_flags, projection_risk_reasons = _risk_off_projection_inputs(
            fence_context=fence_context,
            fallback_risk_flags=risk_flags,
            fallback_risk_reasons=risk_reasons,
        )
        while True:
            items = _load_mutable_buy_items_session(
                db,
                settings,
                statuses=RISK_OFF_EXPIRABLE_STATUSES,
            )
            if not items:
                break
            for item in items:
                if not project_buy_plan_item_for_hourly_risk_off(
                    db=db,
                    settings=settings,
                    item=item,
                    risk_flags=projection_risk_flags,
                    risk_reasons=projection_risk_reasons,
                    now=now,
                    fence_context=fence_context,
                ):
                    continue
                changed += 1
    if risk_flags >= 3:
        action = "risk_off_expire"
        message = f"risk_off: 신규/추가매수 watch {changed}건 중단"
    elif risk_flags >= 1:
        action = "conservative_price_adjust"
        after: tuple[str, datetime, str] | None = None
        while changed < MAX_MUTATIONS_PER_RUN:
            items = _load_mutable_buy_items_session(db, settings, after=after)
            if not items:
                break
            for item in items:
                risk_context = _json_object(item.risk_context)
                # Core v4 trigger/limit values are covered by the persisted
                # authority contract.  Hourly caution may report the risk but
                # must not rewrite those immutable prices; doing so causes the
                # safety validator to quarantine the entire plan.
                if _is_core_v4_bound(risk_context):
                    continue
                existing = risk_context.get(ADJUSTMENT_KEY)
                if isinstance(existing, dict) and existing.get("action") == action:
                    continue
                trigger_price = _decimal_or_none(item.trigger_price)
                limit_price = _decimal_or_none(item.limit_price)
                if trigger_price is None and limit_price is None:
                    continue
                original = {
                    "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
                    "limit_price": str(item.limit_price) if item.limit_price is not None else None,
                }
                adjusted_trigger = _decimal_or_none(_price_text(trigger_price * Decimal("0.99"))) if trigger_price else None
                adjusted_limit = _decimal_or_none(_price_text(limit_price * Decimal("0.99"))) if limit_price else None
                risk_context[ADJUSTMENT_KEY] = {
                    "action": action,
                    "source": SOURCE,
                    "risk_flags": risk_flags,
                    "risk_reasons": risk_reasons,
                    "original": original,
                    "adjusted": {
                        "trigger_price": _price_text(adjusted_trigger),
                        "limit_price": _price_text(adjusted_limit),
                    },
                    "applied_at": now.isoformat(),
                }
                if adjusted_trigger is not None:
                    item.trigger_price = adjusted_trigger
                if adjusted_limit is not None:
                    item.limit_price = adjusted_limit
                item.risk_context = risk_context
                item.updated_at = now
                _insert_audit_session(
                    db,
                    item=item,
                    event_type="hourly_plan_judgment_buy_watch_price_adjusted",
                    summary={"symbol": item.symbol, "action": action, "risk_flags": risk_flags},
                    payload={
                        "risk_reasons": risk_reasons,
                        "original": original,
                        "adjusted": risk_context[ADJUSTMENT_KEY]["adjusted"],
                    },
                    now=now,
                )
                changed += 1
                if changed >= MAX_MUTATIONS_PER_RUN:
                    break
            if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
                break
            last = items[-1]
            after = (last.symbol, last.created_at, last.id)
        message = f"caution: 신규/추가매수 trigger/limit 1% 보수화 {changed}건"
    else:
        action = "restore_conservative_price_adjust"
        after: tuple[str, datetime, str] | None = None
        while changed < MAX_MUTATIONS_PER_RUN:
            items = _load_mutable_buy_items_session(db, settings, after=after)
            if not items:
                break
            for item in items:
                risk_context = _json_object(item.risk_context)
                adjustment = risk_context.get(ADJUSTMENT_KEY)
                if not isinstance(adjustment, dict) or adjustment.get("action") != "conservative_price_adjust":
                    continue
                original = adjustment.get("original") if isinstance(adjustment.get("original"), dict) else {}
                trigger_price = _decimal_or_none(original.get("trigger_price"))
                limit_price = _decimal_or_none(original.get("limit_price"))
                if trigger_price is not None:
                    item.trigger_price = trigger_price
                if limit_price is not None:
                    item.limit_price = limit_price
                risk_context.pop(ADJUSTMENT_KEY, None)
                item.risk_context = risk_context
                item.updated_at = now
                _insert_audit_session(
                    db,
                    item=item,
                    event_type="hourly_plan_judgment_buy_watch_price_restored",
                    summary={"symbol": item.symbol, "action": action},
                    payload={"restored": original},
                    now=now,
                )
                changed += 1
                if changed >= MAX_MUTATIONS_PER_RUN:
                    break
            if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
                break
            last = items[-1]
            after = (last.symbol, last.created_at, last.id)
        message = f"normal: 보수화 가격선 원복 {changed}건"
    recovery_ready = (
        fence_active
        and epoch_matches
        and bool(fence_context.get("recovery_ready"))
    )
    recovery_counts = _advance_risk_off_recoveries_session(
        db,
        settings,
        risk_flags=risk_flags,
        risk_reasons=risk_reasons,
        now=now,
        resume_allowed=recovery_ready if fence_active else True,
        reconcile_all=fence_active,
    )
    changed += recovery_counts["resumed"]
    if recovery_counts["resumed"]:
        message = f"{message}; risk-off watch {recovery_counts['resumed']}건 재개"
    if fence_active and epoch_matches:
        risk_fence = _complete_hourly_risk_off_projection_session(
            db=db,
            state=projection_state,
            expected_epoch=expected_epoch,
            now=now,
        )
    db.commit()
    return {
        "status": "applied",
        "action": action,
        "message": message,
        "changed_count": changed,
        "recovery_observed_count": recovery_counts["observed"],
        "recovery_reset_count": recovery_counts["reset"],
        "recovery_resumed_count": recovery_counts["resumed"],
        "hourly_risk_off": risk_fence,
    }


def _expire_buy_watches(
    conn: sqlite3.Connection,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    fence_context: dict[str, object] | None = None
    if _sqlite_has_hourly_risk_off_fence(conn):
        row = conn.execute(
            "select hourly_risk_off_context from runtime_control_state where id = ?",
            (RUNTIME_CONTROL_ID,),
        ).fetchone()
        if row is not None:
            fence_context = _json_object(row[0])
    while True:
        items = _load_mutable_buy_items(
            conn,
            settings,
            statuses=RISK_OFF_EXPIRABLE_STATUSES,
        )
        if not items:
            break
        for item in items:
            previous = {
                "status": item["status"],
                "trigger_price": str(item["trigger_price"]) if item["trigger_price"] is not None else None,
                "limit_price": str(item["limit_price"]) if item["limit_price"] is not None else None,
            }
            risk_context = _json_object(item["risk_context"])
            prior_adjustment = risk_context.get(ADJUSTMENT_KEY)
            risk_context[ADJUSTMENT_KEY] = {
                "action": "risk_off_expire",
                "source": SOURCE,
                "risk_flags": risk_flags,
                "risk_reasons": risk_reasons,
                "previous": previous,
                "resume": _risk_off_resume_snapshot(
                    previous=previous,
                    prior_adjustment=prior_adjustment,
                ),
                "applied_at": now,
                "recovery": _seed_recovery_from_fence_context(
                    recovery=_initial_risk_off_recovery(
                        settings=settings,
                        risk_flags=risk_flags,
                        risk_reasons=risk_reasons,
                        now=datetime.fromisoformat(now),
                    ),
                    fence_context=fence_context,
                    now=datetime.fromisoformat(now),
                ),
            }
            cursor = conn.execute(
                """
                update execution_plan_items
                set status = 'watch_expired', risk_context = ?, updated_at = ?
                where id = ? and status = ?
                """,
                (json.dumps(risk_context, ensure_ascii=False), now, item["id"], item["status"]),
            )
            if cursor.rowcount > 0:
                _insert_audit(
                    conn,
                    item_id=item["id"],
                    event_type="hourly_plan_judgment_buy_watch_expired",
                    summary={"symbol": item["symbol"], "action": "risk_off_expire", "risk_flags": risk_flags},
                    payload={"risk_reasons": risk_reasons, "previous": previous},
                    now=now,
                )
                changed += 1
    return changed


def _adjust_buy_watch_prices(
    conn: sqlite3.Connection,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    after: tuple[str, str, str] | None = None
    while changed < MAX_MUTATIONS_PER_RUN:
        items = _load_mutable_buy_items(conn, settings, after=after)
        if not items:
            break
        for item in items:
            risk_context = _json_object(item["risk_context"])
            if _is_core_v4_bound(risk_context):
                continue
            existing = risk_context.get(ADJUSTMENT_KEY)
            if isinstance(existing, dict) and existing.get("action") == "conservative_price_adjust":
                continue
            trigger_price = _decimal_or_none(item["trigger_price"])
            limit_price = _decimal_or_none(item["limit_price"])
            if trigger_price is None and limit_price is None:
                continue
            original = {
                "trigger_price": str(item["trigger_price"]) if item["trigger_price"] is not None else None,
                "limit_price": str(item["limit_price"]) if item["limit_price"] is not None else None,
            }
            adjusted_trigger = _price_text(trigger_price * Decimal("0.99")) if trigger_price is not None else None
            adjusted_limit = _price_text(limit_price * Decimal("0.99")) if limit_price is not None else None
            risk_context[ADJUSTMENT_KEY] = {
                "action": "conservative_price_adjust",
                "source": SOURCE,
                "risk_flags": risk_flags,
                "risk_reasons": risk_reasons,
                "original": original,
                "adjusted": {"trigger_price": adjusted_trigger, "limit_price": adjusted_limit},
                "applied_at": now,
            }
            conn.execute(
                """
                update execution_plan_items
                set trigger_price = coalesce(?, trigger_price),
                    limit_price = coalesce(?, limit_price),
                    risk_context = ?,
                    updated_at = ?
                where id = ?
                """,
                (adjusted_trigger, adjusted_limit, json.dumps(risk_context, ensure_ascii=False), now, item["id"]),
            )
            _insert_audit(
                conn,
                item_id=item["id"],
                event_type="hourly_plan_judgment_buy_watch_price_adjusted",
                summary={"symbol": item["symbol"], "action": "conservative_price_adjust", "risk_flags": risk_flags},
                payload={
                    "risk_reasons": risk_reasons,
                    "original": original,
                    "adjusted": {"trigger_price": adjusted_trigger, "limit_price": adjusted_limit},
                },
                now=now,
            )
            changed += 1
            if changed >= MAX_MUTATIONS_PER_RUN:
                break
        if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
            break
        after = (items[-1]["symbol"], items[-1]["created_at"], items[-1]["id"])
    return changed


def _restore_conservative_adjustments(conn: sqlite3.Connection, settings: Settings) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    after: tuple[str, str, str] | None = None
    while changed < MAX_MUTATIONS_PER_RUN:
        items = _load_mutable_buy_items(conn, settings, after=after)
        if not items:
            break
        for item in items:
            risk_context = _json_object(item["risk_context"])
            adjustment = risk_context.get(ADJUSTMENT_KEY)
            if not isinstance(adjustment, dict) or adjustment.get("action") != "conservative_price_adjust":
                continue
            original = adjustment.get("original") if isinstance(adjustment.get("original"), dict) else {}
            trigger_price = _price_text(_decimal_or_none(original.get("trigger_price")))
            limit_price = _price_text(_decimal_or_none(original.get("limit_price")))
            risk_context.pop(ADJUSTMENT_KEY, None)
            conn.execute(
                """
                update execution_plan_items
                set trigger_price = coalesce(?, trigger_price),
                    limit_price = coalesce(?, limit_price),
                    risk_context = ?,
                    updated_at = ?
                where id = ?
                """,
                (trigger_price, limit_price, json.dumps(risk_context, ensure_ascii=False), now, item["id"]),
            )
            _insert_audit(
                conn,
                item_id=item["id"],
                event_type="hourly_plan_judgment_buy_watch_price_restored",
                summary={"symbol": item["symbol"], "action": "restore_conservative_price_adjust"},
                payload={"restored": original},
                now=now,
            )
            changed += 1
            if changed >= MAX_MUTATIONS_PER_RUN:
                break
        if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
            break
        after = (items[-1]["symbol"], items[-1]["created_at"], items[-1]["id"])
    return changed


def _load_risk_off_expired_buy_items(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    max_items: int | None = MAX_MUTATIONS_PER_RUN,
) -> list[sqlite3.Row]:
    placeholders_intents = ",".join("?" for _ in BUY_WATCH_INTENTS)
    matched: list[sqlite3.Row] = []
    after_id = ""
    while max_items is None or len(matched) < max_items:
        rows = list(
            conn.execute(
                f"""
                select i.id, i.symbol, i.status, i.trigger_price, i.limit_price, i.risk_context
                from execution_plan_items i
                join execution_plans p on p.id = i.execution_plan_id
                where p.mode = ?
                  and i.side = 'buy'
                  and i.intent in ({placeholders_intents})
                  and i.status = 'watch_expired'
                  and i.id > ?
                order by i.id asc
                limit ?
                """,
                (settings.runtime_mode, *BUY_WATCH_INTENTS, after_id, MAX_MUTATIONS_PER_RUN),
            )
        )
        if not rows:
            break
        for item in rows:
            risk_context = _json_object(item["risk_context"])
            adjustment = risk_context.get(ADJUSTMENT_KEY)
            if (
                isinstance(adjustment, dict)
                and adjustment.get("action") == "risk_off_expire"
                and adjustment.get("source") == SOURCE
            ):
                matched.append(item)
                if max_items is not None and len(matched) >= max_items:
                    break
        after_id = rows[-1]["id"]
        if len(rows) < MAX_MUTATIONS_PER_RUN:
            break
    return matched


def _advance_risk_off_recoveries(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    risk_flags: int,
    risk_reasons: list[str],
    now: datetime,
    resume_allowed: bool,
    reconcile_all: bool = False,
) -> dict[str, int]:
    counts = {"observed": 0, "reset": 0, "resumed": 0}
    now_text = now.isoformat()
    for item in _load_risk_off_expired_buy_items(
        conn,
        settings,
        max_items=None if reconcile_all else MAX_MUTATIONS_PER_RUN,
    ):
        risk_context = _json_object(item["risk_context"])
        adjustment = risk_context.get(ADJUSTMENT_KEY)
        if not isinstance(adjustment, dict):
            continue
        updated_adjustment, next_status, event_type, event_payload = _risk_off_recovery_transition(
            adjustment=adjustment,
            settings=settings,
            risk_flags=risk_flags,
            risk_reasons=risk_reasons,
            now=now,
            resume_allowed=resume_allowed,
        )
        risk_context[ADJUSTMENT_KEY] = updated_adjustment
        resume = updated_adjustment.get("resume")
        resumed = updated_adjustment["recovery"]["status"] == "resumed"
        trigger_price = item["trigger_price"]
        limit_price = item["limit_price"]
        if resumed and isinstance(resume, dict):
            if "trigger_price" in resume:
                trigger_price = resume.get("trigger_price")
            if "limit_price" in resume:
                limit_price = resume.get("limit_price")
        cursor = conn.execute(
            """
            update execution_plan_items
            set status = ?, trigger_price = ?, limit_price = ?, risk_context = ?, updated_at = ?
            where id = ? and status = 'watch_expired'
            """,
            (
                next_status,
                trigger_price,
                limit_price,
                json.dumps(risk_context, ensure_ascii=False),
                now_text,
                item["id"],
            ),
        )
        if cursor.rowcount <= 0:
            continue
        if event_type is None:
            continue
        event_kind = (
            "resumed"
            if event_type.endswith("_resumed")
            else "reset"
            if event_type.endswith("_reset")
            else "observed"
        )
        counts[event_kind] += 1
        _insert_audit(
            conn,
            item_id=item["id"],
            event_type=event_type,
            summary={
                "symbol": item["symbol"],
                "action": updated_adjustment.get("action"),
                "recovery_status": updated_adjustment["recovery"]["status"],
            },
            payload=event_payload,
            now=now_text,
        )
    return counts
