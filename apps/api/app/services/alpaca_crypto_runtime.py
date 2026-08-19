from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import BrokerLiveMarketContext
from app.core.config import Settings
from app.models.runtime import MonitoringCheckpoint
from app.services.broker_auth import (
    build_broker_adapter,
    ensure_authenticated_broker_session,
)
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService
from app.services.reconciliation import (
    reconcile_pending_broker_orders,
    sync_execution_positions_from_broker_snapshot,
)

LOGGER = logging.getLogger(__name__)
WORKER_NAME = "alpaca_crypto_24h"


@dataclass
class AlpacaCryptoRuntimeCoordinator:
    """Continuously observes, reconciles, and safely executes Alpaca crypto."""

    settings: Settings
    session_factory: sessionmaker[Session]
    _task: asyncio.Task[None] | None = field(default=None, init=False)
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

    def __post_init__(self) -> None:
        self._strategy = AlpacaCryptoStrategyService(
            settings=self.settings,
            session_factory=self.session_factory,
        )
        if self.settings.alpaca_crypto_monitoring_enabled:
            self._status = "idle"

    async def start(self) -> None:
        if not self.settings.alpaca_crypto_monitoring_enabled:
            return
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

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
            while True:
                await asyncio.to_thread(self.run_cycle)
                await asyncio.sleep(self.settings.alpaca_crypto_monitoring_interval_seconds)
        except asyncio.CancelledError:
            raise

    def run_cycle(self) -> dict[str, object]:
        observed_at = datetime.now(UTC)
        self._set_state(status="running", heartbeat=observed_at)
        try:
            result = self._execute_cycle(observed_at=observed_at)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            LOGGER.exception("alpaca_crypto_monitoring_cycle_failed")
            self._set_state(status="degraded", heartbeat=observed_at, error=message)
            self._write_checkpoint(
                status="degraded",
                observed_at=observed_at,
                details={"error": message, "symbols": list(_allowed_symbols(self.settings))},
                succeeded=False,
            )
            return {"status": "degraded", "error": message}

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
            build_broker_adapter(self.settings, provider="alpaca"),
        )
        should_reconcile = (
            self._last_reconciled_at is None
            or (observed_at - self._last_reconciled_at).total_seconds()
            >= self.settings.alpaca_crypto_reconcile_interval_seconds
        )
        with self.session_factory() as db:
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
                )
            else:
                reconciliation = {"skipped": 1}
                position_sync = {"skipped": 1}
            db.commit()
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
        strategy = self._strategy.run_cycle(
            adapter=adapter,
            access_token=token,
            quotes=quotes,
            observed_at=observed_at,
        )
        strategy_status = str(strategy.get("status") or "healthy")
        cycle_status = "degraded" if strategy_status in {"degraded", "risk_blocked"} else "healthy"
        return {
            "status": cycle_status,
            "observed_at": observed_at.isoformat(),
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
            with self.session_factory() as db:
                checkpoint = db.scalar(
                    select(MonitoringCheckpoint).where(
                        MonitoringCheckpoint.worker_name == WORKER_NAME,
                        MonitoringCheckpoint.mode == self.settings.runtime_mode,
                    )
                )
                if checkpoint is None:
                    checkpoint = MonitoringCheckpoint(
                        worker_name=WORKER_NAME,
                        mode=self.settings.runtime_mode,
                    )
                    db.add(checkpoint)
                checkpoint.status = status
                checkpoint.last_heartbeat_at = observed_at
                if succeeded:
                    checkpoint.last_success_at = observed_at
                checkpoint.details = details
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
