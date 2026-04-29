from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.session import ping_database
from app.services.schema import check_runtime_schema
from app.services.runtime import RuntimeMetadata, liveness_payload


def check_database(db: Session) -> dict[str, object]:
    started = perf_counter()
    try:
        ping_database(db)
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }
    return {
        "status": "ok",
        "checked_at": datetime.now(UTC).isoformat(),
        "latency_ms": round((perf_counter() - started) * 1000, 3),
    }


def build_health_payload(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
) -> dict[str, object]:
    database = check_database(db)
    schema = check_runtime_schema(db) if database["status"] == "ok" else {
        "status": "skipped",
        "reason": "database dependency is not healthy",
    }
    dependencies = {
        "database": database,
        "runtime_schema": schema,
    }
    status = "ok" if all(dep["status"] == "ok" for dep in dependencies.values()) else "degraded"
    return {
        "status": status,
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": settings.runtime_mode,
        "version": "0.1.0",
        "checked_at": datetime.now(UTC).isoformat(),
        "dependencies": dependencies,
        "liveness": liveness_payload(runtime),
    }
