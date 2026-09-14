from __future__ import annotations

from contextlib import contextmanager
import fcntl
from hashlib import sha256
import os
from pathlib import Path
from tempfile import gettempdir
from threading import Lock, RLock
from typing import Iterator


_ORDER_SCOPE_LOCKS: dict[str, RLock] = {}
_ORDER_SCOPE_LOCKS_GUARD = Lock()


def order_intent_scope_key_for_account(
    *,
    provider: str,
    runtime_mode: str,
    account_scope: str,
    market: str,
) -> str:
    return ":".join(
        (
            "broker-order-intent",
            str(provider).strip().lower(),
            str(runtime_mode).strip().lower(),
            str(account_scope).strip().lower(),
            str(market).strip().upper(),
        )
    )


def order_intent_process_lock(scope: str) -> RLock:
    with _ORDER_SCOPE_LOCKS_GUARD:
        return _ORDER_SCOPE_LOCKS.setdefault(scope, RLock())


@contextmanager
def order_intent_file_lock(
    scope: str,
    *,
    blocking: bool = True,
) -> Iterator[None]:
    digest = sha256(scope.encode("utf-8")).hexdigest()[:24]
    lock_path = Path(gettempdir()) / f"kai-exec-order-intent-{digest}.lock"
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    with os.fdopen(descriptor, "a+", encoding="utf-8") as lock_file:
        os.fchmod(lock_file.fileno(), 0o600)
        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB
        fcntl.flock(lock_file.fileno(), operation)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
