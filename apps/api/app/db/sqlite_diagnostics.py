from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Iterator

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

_LOCK = threading.RLock()
_STATE_LOCK = threading.Lock()
_LOCK_ERROR_COUNT = 0
_WRITE_RETRY_COUNT = 0
_LAST_LOCK_ERROR: dict[str, object] | None = None


def is_sqlite_session(db: Session) -> bool:
    if not hasattr(db, "get_bind"):
        return False
    bind = db.get_bind()
    return bind.dialect.name == "sqlite"


def is_sqlite_engine(engine: Engine) -> bool:
    return engine.dialect.name == "sqlite"


@contextmanager
def sqlite_write_lock(db: Session) -> Iterator[None]:
    if not is_sqlite_session(db):
        yield
        return
    with _LOCK:
        yield


def record_sqlite_lock_error(message: str) -> None:
    global _LOCK_ERROR_COUNT, _LAST_LOCK_ERROR
    with _STATE_LOCK:
        _LOCK_ERROR_COUNT += 1
        _LAST_LOCK_ERROR = {
            "message": message[-500:],
            "observed_at": datetime.now(UTC).isoformat(),
        }


def record_sqlite_write_retry() -> None:
    global _WRITE_RETRY_COUNT
    with _STATE_LOCK:
        _WRITE_RETRY_COUNT += 1


def sqlite_diagnostics_payload() -> dict[str, object]:
    with _STATE_LOCK:
        return {
            "lock_error_count": _LOCK_ERROR_COUNT,
            "write_retry_count": _WRITE_RETRY_COUNT,
            "last_lock_error": dict(_LAST_LOCK_ERROR) if _LAST_LOCK_ERROR else None,
        }
