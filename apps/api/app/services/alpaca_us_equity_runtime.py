from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import BrokerLiveMarketContext
from app.core.config import Settings
from app.models.runtime import MonitoringCheckpoint
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService
from app.services.broker_auth import build_broker_adapter, ensure_authenticated_broker_session
from app.services.reconciliation import (
    reconcile_pending_broker_orders,
    sync_execution_positions_from_broker_snapshot,
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
        return self.adapter.authenticate()

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

    def __post_init__(self) -> None:
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
            session_factory=self.session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
        )
        if self.settings.alpaca_us_equity_monitoring_enabled:
            self._status = "idle"

    async def start(self) -> None:
        if self.settings.alpaca_us_equity_monitoring_enabled:
            self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

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
            while True:
                await asyncio.to_thread(self.run_cycle)
                await asyncio.sleep(self.settings.alpaca_us_equity_monitoring_interval_seconds)
        except asyncio.CancelledError:
            raise

    def run_cycle(self) -> dict[str, object]:
        observed_at = datetime.now(UTC)
        self._set_state(status="running", heartbeat=observed_at)
        try:
            result = self._execute_cycle(observed_at)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            LOGGER.exception("alpaca_us_equity_cycle_failed")
            self._set_state(status="degraded", heartbeat=observed_at, error=message)
            self._write_checkpoint("degraded", observed_at, {"error": message}, False)
            return {"status": "degraded", "error": message}
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
        adapter = cast(AlpacaBrokerAdapter, build_broker_adapter(self.settings, provider="alpaca"))
        facade = AlpacaUsEquityAdapterFacade(adapter, tuple(sorted(symbols)))
        should_reconcile = (
            self._last_reconciled_at is None
            or (observed_at - self._last_reconciled_at).total_seconds()
            >= self.settings.alpaca_us_equity_reconcile_interval_seconds
        )
        with self.session_factory() as db:
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
                )
            else:
                reconciliation = {"skipped": 1}
                position_sync = {"skipped": 1}
            db.commit()
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
        strategy = self._strategy.run_cycle(
            adapter=facade,  # type: ignore[arg-type]
            access_token=token,
            quotes=quotes,
            observed_at=observed_at,
            market_open=market_open,
        )
        strategy_status = str(strategy.get("status") or "healthy")
        status = "degraded" if strategy_status in {"degraded", "risk_blocked"} else "healthy"
        return {
            "status": status,
            "observed_at": observed_at.isoformat(),
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
            with self.session_factory() as db:
                checkpoint = db.scalar(select(MonitoringCheckpoint).where(
                    MonitoringCheckpoint.worker_name == WORKER_NAME,
                    MonitoringCheckpoint.mode == self.settings.runtime_mode,
                ))
                if checkpoint is None:
                    checkpoint = MonitoringCheckpoint(worker_name=WORKER_NAME, mode=self.settings.runtime_mode)
                    db.add(checkpoint)
                checkpoint.status = status
                checkpoint.last_heartbeat_at = observed_at
                if succeeded:
                    checkpoint.last_success_at = observed_at
                checkpoint.details = details
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
