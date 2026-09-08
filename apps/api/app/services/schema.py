from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import EXEC_OWNED_TABLES


REQUIRED_RUNTIME_COLUMNS = {
    "runtime_control_state": (
        "hourly_risk_off_active",
        "hourly_risk_off_epoch",
        "hourly_risk_off_context",
    ),
}


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
    missing_columns: dict[str, list[str]] = {}
    try:
        for table, required_columns in REQUIRED_RUNTIME_COLUMNS.items():
            if table not in existing:
                continue
            existing_columns = {
                str(column["name"]) for column in inspector.get_columns(table)
            }
            absent = [
                column for column in required_columns if column not in existing_columns
            ]
            if absent:
                missing_columns[table] = absent
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }
    incomplete = bool(missing or missing_columns)
    return {
        "status": "missing" if incomplete else "ok",
        "checked_at": datetime.now(UTC).isoformat(),
        "latency_ms": round((perf_counter() - started) * 1000, 3),
        "owned_tables": list(EXEC_OWNED_TABLES),
        "missing_tables": missing,
        "missing_columns": missing_columns,
    }
