from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from sqlalchemy.orm import Session, sessionmaker

from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import BrokerLiveMarketContext
from app.core.config import Settings
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService
from app.services.broker_auth import build_broker_adapter, ensure_authenticated_broker_session
from app.services.monitoring_checkpoint_scope import (
    exact_monitoring_execution_scope,
    upsert_scoped_monitoring_checkpoint,
)
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
WORKER_NAME = "alpaca_us_equity"


class AlpacaUsEquityAdapterFacade:
    """Expose the shared Alpaca adapter through the generic trend-service surface."""

    provider = "alpaca"

    def __init__(self, adapter: AlpacaBrokerAdapter, symbols: tuple[str, ...]) -> None:
        self.adapter = adapter
        self.symbols = symbols

    def __getattr__(self, name: str):  # noqa: ANN204
        return getattr(self.adapter, name)

    def describe(self):  # noqa: ANN201
        return self.adapter.describe()

    def authenticate(self):  # noqa: ANN201
        return self.adapter.authenticate_us_equity()

    def place_order(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        return self.adapter.place_order(*args, **kwargs)

    def reconcile_order(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        return self.adapter.reconcile_order(*args, **kwargs)

    def get_account_snapshot(self, access_token: str, **_kwargs):  # noqa: ANN003, ANN201
        return self.adapter.get_us_equity_account_snapshot(
            access_token,
            symbols=list(self.symbols),
        )

    def get_crypto_daily_bars(self, access_token: str, **kwargs):  # noqa: ANN003, ANN201
        return self.adapter.get_stock_daily_bars(access_token, **kwargs)

    def list_active_crypto_assets(self) -> list[dict[str, Any]]:
        return self.adapter.list_active_us_equity_assets(list(self.symbols))


@dataclass
class AlpacaUsEquityRuntimeCoordinator:
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
    _last_strategy: dict[str, object] = field(default_factory=dict, init=False)
    _last_reconciliation: dict[str, object] | None = field(default=None, init=False)
    _last_reconciled_at: datetime | None = field(default=None, init=False)
    _market_clock: dict[str, object] = field(default_factory=dict, init=False)
    _strategy: AlpacaCryptoStrategyService = field(init=False)
    _execution_scope: dict[str, object] | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._cycle_session_factory = cast(
            sessionmaker[Session],
            CommitGuardedSessionFactory(
                self.session_factory,
                self._shutdown_fence,
            ),
        )
        strategy_settings = self.settings.model_copy(
            update={
                "alpaca_crypto_strategy_enabled": self.settings.alpaca_us_equity_strategy_enabled,
                "alpaca_crypto_strategy_live_enabled": self.settings.alpaca_us_equity_strategy_live_enabled,
                "alpaca_crypto_strategy_version": self.settings.alpaca_us_equity_strategy_version,
                "alpaca_crypto_strategy_sleeve_usd": self.settings.alpaca_us_equity_strategy_sleeve_usd,
                "alpaca_crypto_allowed_symbols": self.settings.alpaca_us_equity_allowed_symbols,
                "alpaca_crypto_max_order_notional_usd": self.settings.alpaca_us_equity_max_order_notional_usd,
            }
        )
        self._strategy = AlpacaCryptoStrategyService(
            settings=strategy_settings,
            session_factory=self._cycle_session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        )
        if self.settings.alpaca_us_equity_monitoring_enabled:
            self._status = "idle"

    async def start(self) -> None:
        if not self.settings.alpaca_us_equity_monitoring_enabled:
            return
        if self._stopping:
            raise RuntimeError("cannot start Alpaca US-equity runtime while shutting down")
        async with self._lifecycle_lock:
            if self._task is not None and not self._task.done():
                return
            if self._inflight_cycles:
                raise RuntimeError(
                    "cannot start Alpaca US-equity runtime while an in-flight "
                    "cycle is still draining"
                )
            self._stopping = False
            self._task = asyncio.create_task(self._run_loop())

    def mark_startup_blocked(self, reason: str) -> None:
        if not self.settings.alpaca_us_equity_monitoring_enabled:
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
                "enabled": self.settings.alpaca_us_equity_monitoring_enabled,
                "provider": "alpaca",
                "asset_class": "us_equity",
                "mode": self.settings.runtime_mode,
                "status": self._status,
                "symbols": list(_allowed_symbols(self.settings)),
                "strategy_enabled": self.settings.alpaca_us_equity_strategy_enabled,
                "strategy_live_order_submission_enabled": (
                    self.settings.runtime_mode == "live"
                    and self.settings.alpaca_live_trading_enabled
                    and self.settings.alpaca_us_equity_strategy_live_enabled
                ),
                "last_heartbeat_at": _iso(self._last_heartbeat_at),
                "last_success_at": _iso(self._last_success_at),
                "last_error": self._last_error,
                "market_clock": dict(self._market_clock),
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
                await asyncio.sleep(self.settings.alpaca_us_equity_monitoring_interval_seconds)
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
                "Alpaca US-equity in-flight cycle did not drain within the "
                "configured cycle timeout"
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
            result = self._execute_cycle(observed_at)
        except Exception as exc:
            self._shutdown_fence.checkpoint()
            message = f"{type(exc).__name__}: {exc}"
            LOGGER.exception("alpaca_us_equity_cycle_failed")
            # An incomplete cycle cannot prove that cached account authority is
            # still current. Fail closed instead of checkpointing the old scope.
            self._execution_scope = None
            self._last_reconciled_at = None
            self._set_state(status="degraded", heartbeat=observed_at, error=message)
            self._write_checkpoint(
                "degraded",
                observed_at,
                {
                    "error": message,
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
                False,
            )
            return {"status": "degraded", "error": message}
        self._shutdown_fence.checkpoint()
        status = str(result["status"])
        self._set_state(
            status=status,
            heartbeat=observed_at,
            success=observed_at,
            error=None,
            prices=result["prices"],
            reconciliation=result["reconciliation"],
            strategy=result["strategy"],
            market_clock=result["market_clock"],
        )
        self._write_checkpoint(status, observed_at, result, True)
        return result

    def _execute_cycle(self, observed_at: datetime) -> dict[str, object]:
        symbols = set(_allowed_symbols(self.settings))
        adapter = cast(
            AlpacaBrokerAdapter,
            GuardedBrokerAdapter(
                build_broker_adapter(self.settings, provider="alpaca"),
                self._shutdown_fence,
            ),
        )
        facade = AlpacaUsEquityAdapterFacade(adapter, tuple(sorted(symbols)))
        should_reconcile = (
            self._last_reconciled_at is None
            or (observed_at - self._last_reconciled_at).total_seconds()
            >= self.settings.alpaca_us_equity_reconcile_interval_seconds
        )
        with self._cycle_session_factory() as db:
            session = ensure_authenticated_broker_session(
                settings=self.settings,
                db=db,
                adapter=facade,  # type: ignore[arg-type]
                source=WORKER_NAME,
                reason="Alpaca US-equity monitoring",
            )
            token = session.access_token or ""
            if should_reconcile:
                reconciliation = reconcile_pending_broker_orders(
                    settings=self.settings,
                    db=db,
                    adapter=facade,  # type: ignore[arg-type]
                    symbols=symbols,
                )
                position_sync = sync_execution_positions_from_broker_snapshot(
                    settings=self.settings,
                    db=db,
                    adapter=facade,  # type: ignore[arg-type]
                    symbols=symbols,
                    exchange="US",
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

        clock = adapter.get_market_clock(token)
        market_open = bool(clock.get("is_open"))
        clock_payload = {
            key: clock.get(key)
            for key in ("is_open", "timestamp", "next_open", "next_close")
        }
        quotes: dict[str, BrokerLiveMarketContext] = {}
        prices: dict[str, dict[str, str]] = {}
        for symbol in sorted(symbols):
            quote = adapter.get_stock_live_market_context(token, symbol=symbol)
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
            adapter=facade,  # type: ignore[arg-type]
            access_token=token,
            quotes=quotes,
            observed_at=observed_at,
            execution_scope=self._execution_scope,
            market_open=market_open,
        )
        strategy_status = str(strategy.get("status") or "healthy")
        if strategy.get("reason") in {
            "authenticated_execution_scope_unavailable",
            "authenticated_execution_scope_mismatch",
        }:
            self._execution_scope = None
            self._last_reconciled_at = None
        status = (
            "degraded"
            if strategy_status in {"blocked", "degraded", "risk_blocked"}
            or self._execution_scope is None
            else "healthy"
        )
        return {
            "status": status,
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
            "market_clock": clock_payload,
            "prices": prices,
            "reconciliation": {"orders": reconciliation, "positions": position_sync},
            "strategy": strategy,
        }

    def _write_checkpoint(
        self,
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
                        "alpaca_us_equity_checkpoint_scope_unavailable"
                    )
                    return
                db.commit()
        except Exception:
            LOGGER.exception("alpaca_us_equity_checkpoint_write_failed")

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
        market_clock: object | None = None,
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
            if isinstance(market_clock, dict):
                self._market_clock = market_clock


def _allowed_symbols(settings: Settings) -> tuple[str, ...]:
    return tuple(dict.fromkeys(
        symbol.strip().upper()
        for symbol in settings.alpaca_us_equity_allowed_symbols.split(",")
        if symbol.strip()
    ))


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")
