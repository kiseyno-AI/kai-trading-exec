import os
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.session import ping_database
from app.models.runtime import (
    BrokerOrder,
    ExecutionJob,
    MonitoringCheckpoint,
)
from app.services.schema import check_runtime_schema
from app.services.runtime_controls import (
    ensure_runtime_control_state,
    runtime_safety_payload,
)

OPEN_ORDER_STATUSES = ("submitted", "open", "partially_filled")
ACTIVE_JOB_STATUSES = ("queued", "scheduled", "running", "retrying")


@dataclass(frozen=True)
class RuntimeMetadata:
    started_at: datetime

    @property
    def process_id(self) -> int:
        return os.getpid()


def liveness_payload(runtime: RuntimeMetadata) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "process_id": runtime.process_id,
        "started_at": runtime.started_at.isoformat(),
        "uptime_seconds": round((now - runtime.started_at).total_seconds(), 3),
    }


def default_safety(settings: Settings, reason: str) -> dict[str, object]:
    return {
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": False,
        "automation_enabled": False,
        "automated_order_submission_allowed": False,
        "automated_order_submission_block_reason": "paused",
        "readiness_reason": reason,
        "updated_at": None,
        "last_change": {
            "action": "bootstrap",
            "actor": None,
            "source": "system",
            "reason": reason,
            "changed_at": None,
            "event_id": None,
        },
    }


def runtime_counts(db: Session) -> dict[str, int]:
    open_orders = db.scalar(
        select(func.count())
        .select_from(BrokerOrder)
        .where(BrokerOrder.status.in_(OPEN_ORDER_STATUSES)),
    )
    active_jobs = db.scalar(
        select(func.count())
        .select_from(ExecutionJob)
        .where(ExecutionJob.status.in_(ACTIVE_JOB_STATUSES)),
    )
    return {
        "open_orders": int(open_orders or 0),
        "active_execution_jobs": int(active_jobs or 0),
    }


def monitoring_payload(db: Session) -> dict[str, object]:
    checkpoints = db.scalars(select(MonitoringCheckpoint)).all()
    if not checkpoints:
        return {
            "status": "not_started",
            "workers": [],
            "last_heartbeat_at": None,
        }

    heartbeat_values = [
        checkpoint.last_heartbeat_at
        for checkpoint in checkpoints
        if checkpoint.last_heartbeat_at is not None
    ]
    last_heartbeat = max(heartbeat_values).isoformat() if heartbeat_values else None
    return {
        "status": "running" if last_heartbeat else "not_started",
        "workers": [
            {
                "name": checkpoint.worker_name,
                "status": checkpoint.status,
                "last_heartbeat_at": (
                    checkpoint.last_heartbeat_at.isoformat()
                    if checkpoint.last_heartbeat_at
                    else None
                ),
                "last_success_at": (
                    checkpoint.last_success_at.isoformat()
                    if checkpoint.last_success_at
                    else None
                ),
            }
            for checkpoint in checkpoints
        ],
        "last_heartbeat_at": last_heartbeat,
    }


def readiness_payload(
    *,
    dependencies_ok: bool,
    safety: dict[str, object],
    counts_available: bool,
) -> dict[str, object]:
    reasons: list[str] = []
    if not dependencies_ok:
        reasons.append("database or runtime schema dependency is not ready")
    if safety["kill_switch_enabled"]:
        reasons.append("kill switch is enabled")
    if safety["paused"]:
        reasons.append("runtime is paused")
    if not safety["automation_enabled"]:
        reasons.append("automation is disabled")
    if not counts_available:
        reasons.append("runtime counters are unavailable")

    return {
        "ready": not reasons,
        "reasons": reasons,
    }


def build_runtime_status(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
) -> dict[str, object]:
    dependencies = {
        "database": "ok",
        "runtime_schema": "ok",
    }
    counts: dict[str, int] = {"open_orders": 0, "active_execution_jobs": 0}
    monitoring: dict[str, object] = {
        "status": "not_started",
        "workers": [],
        "last_heartbeat_at": None,
    }
    counts_available = True

    try:
        ping_database(db)
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["database"] = "error"
        dependencies["runtime_schema"] = "unknown"
        counts_available = False
        safety = default_safety(
            settings,
            f"database unavailable: {exc.__class__.__name__}",
        )
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "liveness": {
                **liveness_payload(runtime),
                "monitoring": monitoring,
            },
            "counts": counts,
        }

    schema = check_runtime_schema(db)
    dependencies["runtime_schema"] = str(schema["status"])
    if schema["status"] != "ok":
        counts_available = False
        missing_tables = schema.get("missing_tables") or []
        if missing_tables:
            schema_reason = (
                "runtime schema incomplete: "
                f"missing {', '.join(str(table) for table in missing_tables)}; run migrations"
            )
        else:
            schema_reason = "runtime schema unavailable; run migrations"
        safety = default_safety(settings, schema_reason)
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "liveness": {
                **liveness_payload(runtime),
                "monitoring": monitoring,
            },
            "counts": counts,
        }

    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
        safety = runtime_safety_payload(state)
        counts = runtime_counts(db)
        monitoring = monitoring_payload(db)
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["runtime_schema"] = "error"
        counts_available = False
        safety = default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )

    readiness = readiness_payload(
        dependencies_ok=all(value == "ok" for value in dependencies.values()),
        safety=safety,
        counts_available=counts_available,
    )
    return {
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": safety["mode"],
        "ready": readiness["ready"],
        "readiness": readiness,
        "safety": safety,
        "dependencies": dependencies,
        "liveness": {
            **liveness_payload(runtime),
            "monitoring": monitoring,
        },
        "counts": counts,
    }
