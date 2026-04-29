from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import EXEC_OWNED_TABLES


def check_runtime_schema(db: Session) -> dict[str, object]:
    started = perf_counter()
    try:
        inspector = inspect(db.get_bind())
        existing = set(inspector.get_table_names())
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }

    missing = [table for table in EXEC_OWNED_TABLES if table not in existing]
    return {
        "status": "ok" if not missing else "missing",
        "checked_at": datetime.now(UTC).isoformat(),
        "latency_ms": round((perf_counter() - started) * 1000, 3),
        "owned_tables": list(EXEC_OWNED_TABLES),
        "missing_tables": missing,
    }
