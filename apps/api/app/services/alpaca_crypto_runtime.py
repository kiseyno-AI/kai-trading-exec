from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from sqlalchemy.orm import Session, sessionmaker

from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import BrokerLiveMarketContext
from app.core.config import Settings
from app.services.broker_auth import (
    build_broker_adapter,
    ensure_authenticated_broker_session,
)
from app.services.monitoring_checkpoint_scope import (
    exact_monitoring_execution_scope,
    upsert_scoped_monitoring_checkpoint,
)
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService
from app.services.reconciliation import (
    reconcile_pending_broker_orders,
    sync_execution_positions_from_broker_snapshot,
)
from app.services.runtime_shutdown import (
    CommitGuardedSessionFactory,
    GuardedBrokerAdapter,
    RuntimeCycleFence,
)

LOGGER = logging.getLogger(__name__)
WORKER_NAME = "alpaca_crypto_24h"


@dataclass
class AlpacaCryptoRuntimeCoordinator:
    """Continuously observes, reconciles, and safely executes Alpaca crypto."""

    settings: Settings
    session_factory: sessionmaker[Session]
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _inflight_cycles: set[asyncio.Task[dict[str, object]]] = field(
        default_factory=set,
        init=False,
    )
    _lifecycle_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _stopping: bool = field(default=False, init=False)
    _shutdown_fence: RuntimeCycleFence = field(
        default_factory=RuntimeCycleFence,
        init=False,
        repr=False,
    )
    _cycle_session_factory: sessionmaker[Session] = field(init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _status: str = field(default="disabled", init=False)
    _last_heartbeat_at: datetime | None = field(default=None, init=False)
    _last_success_at: datetime | None = field(default=None, init=False)
    _last_error: str | None = field(default=None, init=False)
    _prices: dict[str, dict[str, str]] = field(default_factory=dict, init=False)
    _last_reconciliation: dict[str, object] | None = field(default=None, init=False)
    _last_reconciled_at: datetime | None = field(default=None, init=False)
    _strategy: AlpacaCryptoStrategyService = field(init=False)
    _last_strategy: dict[str, object] = field(default_factory=dict, init=False)
    _execution_scope: dict[str, object] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._cycle_session_factory = cast(
            sessionmaker[Session],
            CommitGuardedSessionFactory(
                self.session_factory,
                self._shutdown_fence,
            ),
        )
        self._strategy = AlpacaCryptoStrategyService(
            settings=self.settings,
            session_factory=self._cycle_session_factory,
        )
        if self.settings.alpaca_crypto_monitoring_enabled:
            self._status = "idle"

    async def start(self) -> None:
        if not self.settings.alpaca_crypto_monitoring_enabled:
            return
        if self._stopping:
            raise RuntimeError("cannot start Alpaca crypto runtime while shutting down")
        async with self._lifecycle_lock:
            if self._task is not None and not self._task.done():
                return
            if self._inflight_cycles:
                raise RuntimeError(
                    "cannot start Alpaca crypto runtime while an in-flight cycle "
                    "is still draining"
                )
            self._stopping = False
            self._task = asyncio.create_task(self._run_loop())

    def mark_startup_blocked(self, reason: str) -> None:
        if not self.settings.alpaca_crypto_monitoring_enabled:
            return
        with self._lock:
            self._status = "blocked"
            self._last_error = reason

    def begin_shutdown(self) -> None:
        self._stopping = True
        self._shutdown_fence.begin_shutdown()

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            self.begin_shutdown()
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            await self._drain_inflight_cycles(
                timeout=self._shutdown_drain_timeout_seconds()
            )
            self._finish_stop()

    async def wait_stopped(self) -> None:
        """Wait without a timeout so lifespan cannot outlive a worker thread."""

        async with self._lifecycle_lock:
            self.begin_shutdown()
            if self._task is not None and not self._task.done():
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            await self._drain_inflight_cycles(timeout=None)
            self._finish_stop()

    def _finish_stop(self) -> None:
        self._task = None
        self._shutdown_fence.finish_shutdown()
        self._stopping = False

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "enabled": self.settings.alpaca_crypto_monitoring_enabled,
                "provider": "alpaca",
                "mode": self.settings.runtime_mode,
                "live_order_submission_enabled": (
                    self.settings.runtime_mode == "live"
                    and self.settings.alpaca_live_trading_enabled
                ),
                "strategy_enabled": self.settings.alpaca_crypto_strategy_enabled,
                "strategy_live_order_submission_enabled": (
                    self.settings.runtime_mode == "live"
                    and self.settings.alpaca_live_trading_enabled
                    and self.settings.alpaca_crypto_strategy_live_enabled
                ),
                "status": self._status,
                "symbols": list(_allowed_symbols(self.settings)),
                "poll_interval_seconds": self.settings.alpaca_crypto_monitoring_interval_seconds,
                "reconcile_interval_seconds": self.settings.alpaca_crypto_reconcile_interval_seconds,
                "last_heartbeat_at": _iso(self._last_heartbeat_at),
                "last_success_at": _iso(self._last_success_at),
                "last_error": self._last_error,
                "prices": dict(self._prices),
                "last_reconciliation": self._last_reconciliation,
                "strategy": dict(self._last_strategy or self._strategy.snapshot()),
            }

    async def _run_loop(self) -> None:
        try:
            while not self._stopping:
                await self._run_cycle_in_thread()
                if self._stopping:
                    return
                await asyncio.sleep(self.settings.alpaca_crypto_monitoring_interval_seconds)
        except asyncio.CancelledError:
            raise

    async def _run_cycle_in_thread(self) -> dict[str, object]:
        if self._stopping:
            raise asyncio.CancelledError
        generation = self._shutdown_fence.capture_generation()
        task = asyncio.create_task(
            asyncio.to_thread(
                self._shutdown_fence.run,
                generation,
                self.run_cycle,
            )
        )
        self._inflight_cycles.add(task)
        task.add_done_callback(self._inflight_cycle_done)
        return await asyncio.shield(task)

    def _inflight_cycle_done(
        self,
        task: asyncio.Task[dict[str, object]],
    ) -> None:
        self._inflight_cycles.discard(task)
        if task.cancelled():
            return
        task.exception()

    async def _drain_inflight_cycles(self, *, timeout: float | None) -> None:
        tasks = tuple(self._inflight_cycles)
        if not tasks:
            return
        done, pending = await asyncio.wait(
            tasks,
            timeout=timeout,
        )
        if done:
            await asyncio.gather(*done, return_exceptions=True)
        if pending:
            raise TimeoutError(
                "Alpaca crypto in-flight cycle did not drain within the configured "
                "cycle timeout"
            )

    def _shutdown_drain_timeout_seconds(self) -> float:
        return max(
            float(self.settings.alpaca_request_timeout_seconds),
            float(self.settings.monitoring_cycle_timeout_seconds)
            * float(self.settings.monitoring_stale_cycle_exit_grace_multiplier),
        )

    def run_cycle(self) -> dict[str, object]:
        observed_at = datetime.now(UTC)
        self._set_state(status="running", heartbeat=observed_at)
        try:
            result = self._execute_cycle(observed_at=observed_at)
        except Exception as exc:
            self._shutdown_fence.checkpoint()
            message = f"{type(exc).__name__}: {exc}"
            LOGGER.exception("alpaca_crypto_monitoring_cycle_failed")
            # Any failed current-cycle identity check invalidates cached broker
            # authority; do not persist another checkpoint under an old account.
            self._execution_scope = None
            self._last_reconciled_at = None
            self._set_state(status="degraded", heartbeat=observed_at, error=message)
            self._write_checkpoint(
                status="degraded",
                observed_at=observed_at,
                details={
                    "error": message,
                    "symbols": list(_allowed_symbols(self.settings)),
                    "execution_scope": self._execution_scope,
                    "execution_scope_status": (
                        "available" if self._execution_scope is not None else "unavailable"
                    ),
                    "execution_scope_source": (
                        "authenticated_broker_account"
                        if self._execution_scope is not None
                        else None
                    ),
                },
                succeeded=False,
            )
            return {"status": "degraded", "error": message}

        self._shutdown_fence.checkpoint()
        cycle_status = str(result.get("status") or "healthy")
        self._set_state(
            status=cycle_status,
            heartbeat=observed_at,
            success=observed_at,
            error=None,
            prices=result["prices"],
            reconciliation=result["reconciliation"],
            strategy=result["strategy"],
        )
        self._write_checkpoint(
            status=cycle_status,
            observed_at=observed_at,
            details=result,
            succeeded=True,
        )
        return result

    def _execute_cycle(self, *, observed_at: datetime) -> dict[str, object]:
        symbols = set(_allowed_symbols(self.settings))
        adapter = cast(
            AlpacaBrokerAdapter,
            GuardedBrokerAdapter(
                build_broker_adapter(self.settings, provider="alpaca"),
                self._shutdown_fence,
            ),
        )
        should_reconcile = (
            self._last_reconciled_at is None
            or (observed_at - self._last_reconciled_at).total_seconds()
            >= self.settings.alpaca_crypto_reconcile_interval_seconds
        )
        with self._cycle_session_factory() as db:
            session = ensure_authenticated_broker_session(
                settings=self.settings,
                db=db,
                adapter=adapter,
                source=WORKER_NAME,
                reason="24/7 Alpaca crypto monitoring",
            )
            token = session.access_token or ""
            if should_reconcile:
                reconciliation = reconcile_pending_broker_orders(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    symbols=symbols,
                )
                position_sync = sync_execution_positions_from_broker_snapshot(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    symbols=symbols,
                    exchange="ALPACA",
                )
            else:
                reconciliation = {"skipped": 1}
                position_sync = {
                    "skipped": 1,
                    "execution_scope": self._execution_scope,
                    "execution_scope_source": (
                        "authenticated_broker_account"
                        if self._execution_scope is not None
                        else None
                    ),
                }
            db.commit()
        execution_scope = (
            exact_monitoring_execution_scope(
                position_sync.get("execution_scope"),
                provider="alpaca",
            )
            if position_sync.get("execution_scope_source")
            == "authenticated_broker_account"
            else None
        )
        if execution_scope is not None:
            self._execution_scope = execution_scope
        if should_reconcile:
            self._last_reconciled_at = observed_at

        prices: dict[str, dict[str, str]] = {}
        quotes: dict[str, BrokerLiveMarketContext] = {}
        for symbol in sorted(symbols):
            quote = adapter.get_live_market_context(token, symbol=symbol, exchange="ALPACA")
            if quote is None:
                continue
            quotes[symbol] = quote
            midpoint = (
                (quote.bid + quote.ask) / Decimal("2")
                if quote.bid is not None and quote.ask is not None
                else quote.bid or quote.ask
            )
            prices[symbol] = {
                "price": _decimal(midpoint) if midpoint is not None else "",
                "bid": _decimal(quote.bid) if quote.bid is not None else "",
                "ask": _decimal(quote.ask) if quote.ask is not None else "",
                "spread_bps": _decimal(quote.spread_bps) if quote.spread_bps is not None else "",
                "observed_at": quote.observed_at.astimezone(UTC).isoformat(),
            }
        self._shutdown_fence.checkpoint()
        strategy = self._strategy.run_cycle(
            adapter=adapter,
            access_token=token,
            quotes=quotes,
            observed_at=observed_at,
            execution_scope=self._execution_scope,
        )
        strategy_status = str(strategy.get("status") or "healthy")
        if strategy.get("reason") in {
            "authenticated_execution_scope_unavailable",
            "authenticated_execution_scope_mismatch",
        }:
            self._execution_scope = None
            self._last_reconciled_at = None
        cycle_status = (
            "degraded"
            if strategy_status in {"blocked", "degraded", "risk_blocked"}
            or self._execution_scope is None
            else "healthy"
        )
        return {
            "status": cycle_status,
            "observed_at": observed_at.isoformat(),
            "execution_scope": self._execution_scope,
            "execution_scope_status": (
                "available" if self._execution_scope is not None else "unavailable"
            ),
            "execution_scope_source": (
                "authenticated_broker_account"
                if self._execution_scope is not None
                else None
            ),
            "prices": prices,
            "reconciliation": {
                "orders": reconciliation,
                "positions": position_sync,
            },
            "strategy": strategy,
        }

    def _write_checkpoint(
        self,
        *,
        status: str,
        observed_at: datetime,
        details: dict[str, object],
        succeeded: bool,
    ) -> None:
        try:
            self._shutdown_fence.checkpoint()
            with self._cycle_session_factory() as db:
                checkpoint = upsert_scoped_monitoring_checkpoint(
                    db=db,
                    worker_name=WORKER_NAME,
                    mode=self.settings.runtime_mode,
                    status=status,
                    observed_at=observed_at,
                    details=details,
                    succeeded=succeeded,
                    provider="alpaca",
                )
                if checkpoint is None:
                    LOGGER.warning(
                        "alpaca_crypto_checkpoint_scope_unavailable"
                    )
                    return
                db.commit()
        except Exception:
            LOGGER.exception("alpaca_crypto_checkpoint_write_failed")

    def _set_state(
        self,
        *,
        status: str,
        heartbeat: datetime,
        success: datetime | None = None,
        error: str | None = None,
        prices: object | None = None,
        reconciliation: object | None = None,
        strategy: object | None = None,
    ) -> None:
        with self._lock:
            self._status = status
            self._last_heartbeat_at = heartbeat
            if success is not None:
                self._last_success_at = success
            self._last_error = error
            if isinstance(prices, dict):
                self._prices = prices
            if isinstance(reconciliation, dict):
                self._last_reconciliation = reconciliation
            if isinstance(strategy, dict):
                self._last_strategy = strategy


def _allowed_symbols(settings: Settings) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            symbol.strip().upper().replace("-", "/")
            for symbol in settings.alpaca_crypto_allowed_symbols.split(",")
            if symbol.strip()
        )
    )


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")
