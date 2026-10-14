from __future__ import annotations

import asyncio
from dataclasses import dataclass
import threading
from typing import Any, Callable, TypeVar

from sqlalchemy import event
from sqlalchemy.orm import Session, sessionmaker


_ResultT = TypeVar("_ResultT")


class RuntimeCycleStopped(asyncio.CancelledError):
    """Signal that a worker belongs to a superseded runtime generation."""


@dataclass
class RuntimeCycleLease:
    """One independently expirable unit of work within a runtime generation."""

    generation: int
    expired: bool = False


class RuntimeCycleFence:
    """Thread-safe generation fence for work delegated through ``to_thread``."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._generation = 0
        self._stopping = False
        self._thread_context = threading.local()

    def capture_generation(self) -> int:
        with self._lock:
            if self._stopping:
                raise RuntimeCycleStopped("runtime shutdown is in progress")
            return self._generation

    def capture_cycle_lease(self) -> RuntimeCycleLease:
        with self._lock:
            if self._stopping:
                raise RuntimeCycleStopped("runtime shutdown is in progress")
            return RuntimeCycleLease(generation=self._generation)

    def expire_cycle_lease(self, lease: RuntimeCycleLease) -> None:
        with self._lock:
            lease.expired = True

    def begin_shutdown(self) -> None:
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
            self._generation += 1

    def finish_shutdown(self) -> None:
        with self._lock:
            self._stopping = False

    def checkpoint(self, generation: int | None = None) -> None:
        captured_generation = (
            generation
            if generation is not None
            else getattr(self._thread_context, "generation", None)
        )
        captured_lease = getattr(self._thread_context, "cycle_lease", None)
        with self._lock:
            outcome_persistence_allowed = any(
                bool(getattr(self._thread_context, name, False))
                for name in (
                    "broker_call_in_flight",
                    "broker_outcome_pending",
                    "commit_admitted",
                )
            )
            stale = not outcome_persistence_allowed and (
                self._stopping
                or bool(captured_lease is not None and captured_lease.expired)
                or (
                    captured_generation is not None
                    and captured_generation != self._generation
                )
            )
        if stale:
            raise RuntimeCycleStopped("runtime cycle superseded by shutdown")

    def run(
        self,
        generation: int,
        operation: Callable[..., _ResultT],
        *args: object,
        lease: RuntimeCycleLease | None = None,
    ) -> _ResultT:
        previous_generation = getattr(self._thread_context, "generation", None)
        previous_cycle_lease = getattr(self._thread_context, "cycle_lease", None)
        previous_flags = {
            name: getattr(self._thread_context, name, None)
            for name in (
                "broker_call_in_flight",
                "broker_outcome_pending",
                "commit_admitted",
            )
        }
        self._thread_context.generation = generation
        self._thread_context.cycle_lease = lease
        self._thread_context.broker_call_in_flight = False
        self._thread_context.broker_outcome_pending = False
        self._thread_context.commit_admitted = False
        try:
            self.checkpoint(generation)
            result = operation(*args)
            self.checkpoint(generation)
            return result
        finally:
            if previous_generation is None:
                del self._thread_context.generation
            else:
                self._thread_context.generation = previous_generation
            if previous_cycle_lease is None:
                if hasattr(self._thread_context, "cycle_lease"):
                    del self._thread_context.cycle_lease
            else:
                self._thread_context.cycle_lease = previous_cycle_lease
            for name, previous_value in previous_flags.items():
                if previous_value is None:
                    if hasattr(self._thread_context, name):
                        delattr(self._thread_context, name)
                else:
                    setattr(self._thread_context, name, previous_value)

    def run_broker_call(
        self,
        operation: Callable[..., _ResultT],
        *args: object,
        **kwargs: object,
    ) -> _ResultT:
        """Linearize broker-call admission against shutdown.

        Once admitted, the enclosing cycle may persist the broker outcome even
        if shutdown begins while the network call is in flight. A cycle that
        has not crossed this boundary remains stale and is cancelled.
        """

        had_cycle_context = hasattr(self._thread_context, "generation")
        previous_in_flight = getattr(
            self._thread_context,
            "broker_call_in_flight",
            None,
        )
        with self._lock:
            captured_generation = getattr(self._thread_context, "generation", None)
            captured_lease = getattr(self._thread_context, "cycle_lease", None)
            if self._stopping or (
                captured_generation is not None
                and captured_generation != self._generation
            ) or bool(captured_lease is not None and captured_lease.expired):
                raise RuntimeCycleStopped("runtime cycle superseded by shutdown")
            self._thread_context.broker_call_in_flight = True
        try:
            return operation(*args, **kwargs)
        finally:
            if previous_in_flight is None:
                if hasattr(self._thread_context, "broker_call_in_flight"):
                    del self._thread_context.broker_call_in_flight
            else:
                self._thread_context.broker_call_in_flight = previous_in_flight
            if had_cycle_context:
                self._thread_context.broker_outcome_pending = True

    def begin_commit(self) -> None:
        """Linearize commit admission without holding a lock across database I/O."""

        with self._lock:
            self.checkpoint()
            self._thread_context.commit_admitted = True

    def finish_commit(self) -> None:
        with self._lock:
            self._thread_context.commit_admitted = False
            self._thread_context.broker_outcome_pending = False


class GuardedBrokerAdapter:
    """Proxy that checks a cycle fence at the external order side-effect boundary."""

    def __init__(self, adapter: object, fence: RuntimeCycleFence) -> None:
        self._adapter = adapter
        self._fence = fence

    @property
    def __class__(self) -> type[object]:
        # ``broker_auth`` intentionally routes KIS through stricter shared-token
        # coordination with ``isinstance``. Preserve that runtime identity while
        # interposing only the mutating order boundary.
        return self._adapter.__class__

    def __getattr__(self, name: str) -> Any:
        return getattr(self._adapter, name)

    def place_order(self, *args: object, **kwargs: object) -> object:
        return self._fence.run_broker_call(
            self._adapter.place_order,
            *args,
            **kwargs,
        )


class CommitGuardedSessionFactory:
    """Create sessions whose commits abort when their runtime generation is stale."""

    def __init__(
        self,
        factory: sessionmaker[Session],
        fence: RuntimeCycleFence,
    ) -> None:
        self._factory = factory
        self._fence = fence
        self._lease_key = f"runtime_commit_fence:{id(self)}"

    def __call__(self, *args: object, **kwargs: object) -> Session:
        session = self._factory(*args, **kwargs)

        def check_before_commit(current_session: Session) -> None:
            if current_session.info.get(self._lease_key) is True:
                return
            self._fence.begin_commit()
            current_session.info[self._lease_key] = True

        event.listen(session, "before_commit", check_before_commit)

        def release_after_transaction(
            current_session: Session,
            transaction: object,
        ) -> None:
            if getattr(transaction, "parent", None) is not None:
                return
            if current_session.info.pop(self._lease_key, False):
                self._fence.finish_commit()

        event.listen(session, "after_transaction_end", release_after_transaction)
        return session
