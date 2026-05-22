from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.monitoring import (
    list_autonomous_monitoring_symbols,
    run_autonomous_monitoring_cycle,
)
from app.services.watch_exec_agent_sizing import run_due_watch_exec_agent_sizing

LOGGER = logging.getLogger(__name__)

DISPATCH_LOOP_WORKER_NAME = "execution_job_dispatch_loop"
MONITORING_LOOP_WORKER_NAME = "monitoring_dispatch_loop"
WATCH_EXEC_AGENT_SIZING_WORKER_NAME = "watch_exec_agent_sizing_loop"


@dataclass
class AutonomousWorkerState:
    worker_name: str
    poll_interval_seconds: float
    status: str = "idle"
    last_started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_cycle: dict[str, object] | None = None

    def snapshot(self) -> dict[str, object]:
        return {
            "worker_name": self.worker_name,
            "status": self.status,
            "poll_interval_seconds": self.poll_interval_seconds,
            "last_started_at": _serialize_datetime(self.last_started_at),
            "last_heartbeat_at": _serialize_datetime(self.last_heartbeat_at),
            "last_success_at": _serialize_datetime(self.last_success_at),
            "last_error": self.last_error,
            "last_cycle": self.last_cycle,
        }


@dataclass
class MonitoringPriceSourceState:
    configured: bool
    source_name: str | None
    provider: str | None
    status: str
    reason: str | None = None
    last_checked_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    last_symbol_count: int = 0
    last_price_count: int = 0

    def snapshot(self) -> dict[str, object]:
        return {
            "configured": self.configured,
            "source_name": self.source_name,
            "provider": self.provider,
            "status": self.status,
            "reason": self.reason,
            "last_checked_at": _serialize_datetime(self.last_checked_at),
            "last_success_at": _serialize_datetime(self.last_success_at),
            "last_error": self.last_error,
            "last_error_at": _serialize_datetime(self.last_error_at),
            "last_symbol_count": self.last_symbol_count,
            "last_price_count": self.last_price_count,
        }


@dataclass
class AutonomousRuntimeCoordinator:
    settings: Settings
    session_factory: sessionmaker[Session]
    broker_adapter_factory: Callable[[Settings], BrokerAdapter]
    monitoring_price_source: object | None = None
    _dispatch_task: asyncio.Task[None] | None = field(default=None, init=False)
    _monitoring_task: asyncio.Task[None] | None = field(default=None, init=False)
    _watch_exec_agent_sizing_task: asyncio.Task[None] | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _dispatch_state: AutonomousWorkerState = field(init=False)
    _monitoring_state: AutonomousWorkerState = field(init=False)
    _watch_exec_agent_sizing_state: AutonomousWorkerState = field(init=False)
    _monitoring_price_source_state: MonitoringPriceSourceState = field(init=False)

    def __post_init__(self) -> None:
        self._dispatch_state = AutonomousWorkerState(
            worker_name=DISPATCH_LOOP_WORKER_NAME,
            poll_interval_seconds=self.settings.dispatch_loop_interval_seconds,
            status="disabled" if not self.settings.autonomous_runtime_enabled else "idle",
        )
        self._monitoring_state = AutonomousWorkerState(
            worker_name=MONITORING_LOOP_WORKER_NAME,
            poll_interval_seconds=self.settings.monitoring_loop_interval_seconds,
            status="disabled" if not self.settings.autonomous_runtime_enabled else "idle",
        )
        self._watch_exec_agent_sizing_state = AutonomousWorkerState(
            worker_name=WATCH_EXEC_AGENT_SIZING_WORKER_NAME,
            poll_interval_seconds=self.settings.exec_agent_watch_sizing_loop_interval_seconds,
            status="disabled" if not self.settings.autonomous_runtime_enabled else "idle",
        )
        self._monitoring_price_source_state = self._initial_monitoring_price_source_state()

    async def start(self) -> None:
        if not self.settings.autonomous_runtime_enabled:
            return
        self._dispatch_task = asyncio.create_task(
            self._run_periodic_loop(
                worker_key="dispatch",
                interval_seconds=self.settings.dispatch_loop_interval_seconds,
                cycle=self._run_dispatch_cycle,
            )
        )
        self._monitoring_task = asyncio.create_task(
            self._run_periodic_loop(
                worker_key="monitoring",
                interval_seconds=self.settings.monitoring_loop_interval_seconds,
                cycle=self._run_monitoring_cycle,
                run_immediately=_is_regular_market_session(
                    now=datetime.now(UTC),
                    market_timezone=self.settings.market_timezone,
                ),
            )
        )
        self._watch_exec_agent_sizing_task = asyncio.create_task(
            self._run_periodic_loop(
                worker_key="watch_exec_agent_sizing",
                interval_seconds=self.settings.exec_agent_watch_sizing_loop_interval_seconds,
                cycle=self._run_watch_exec_agent_sizing_cycle,
            )
        )

    async def stop(self) -> None:
        tasks = [
            task
            for task in (self._dispatch_task, self._monitoring_task, self._watch_exec_agent_sizing_task)
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "enabled": self.settings.autonomous_runtime_enabled,
                "dispatch": self._dispatch_state.snapshot(),
                "monitoring": {
                    **self._monitoring_state.snapshot(),
                    "price_source_configured": self._monitoring_price_source_state.configured,
                    "price_source": self._monitoring_price_source_state.snapshot(),
                },
                "watch_exec_agent_sizing": self._watch_exec_agent_sizing_state.snapshot(),
            }

    async def _run_periodic_loop(
        self,
        *,
        worker_key: str,
        interval_seconds: float,
        cycle: Callable[[datetime], asyncio.Future | object],
        run_immediately: bool = False,
    ) -> None:
        try:
            first_cycle = True
            while True:
                if first_cycle and run_immediately:
                    first_cycle = False
                else:
                    await asyncio.sleep(interval_seconds)
                    first_cycle = False
                started_at = datetime.now(UTC)
                self._mark_cycle_started(worker_key=worker_key, started_at=started_at)
                try:
                    payload = await cycle(started_at)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.exception(
                        "autonomous_runtime_cycle_failed",
                        extra={
                            "worker_name": self._state_for(worker_key).worker_name,
                            "runtime_mode": self.settings.runtime_mode,
                        },
                    )
                    self._mark_cycle_failed(
                        worker_key=worker_key,
                        heartbeat_at=datetime.now(UTC),
                        error=str(exc),
                    )
                else:
                    self._mark_cycle_succeeded(
                        worker_key=worker_key,
                        heartbeat_at=datetime.now(UTC),
                        payload=payload,
                    )
        except asyncio.CancelledError:
            return

    async def _run_dispatch_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._dispatch_cycle_sync, started_at)

    def _dispatch_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        with self.session_factory() as db:
            adapter = self.broker_adapter_factory(self.settings)
            result = dispatch_due_execution_jobs(
                settings=self.settings,
                db=db,
                adapter=adapter,
                now=started_at,
            )
            payload = {
                "status_code": result.status_code,
                **result.payload,
            }
            self._store_cycle_payload(worker_key="dispatch", payload=payload)
            return payload

    async def _run_monitoring_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._monitoring_cycle_sync, started_at)

    async def _run_watch_exec_agent_sizing_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._watch_exec_agent_sizing_cycle_sync, started_at)

    def _watch_exec_agent_sizing_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        with self.session_factory() as db:
            result = run_due_watch_exec_agent_sizing(
                settings=self.settings,
                db=db,
                now=started_at,
            )
            payload = {
                "status_code": result.status_code,
                **result.payload,
            }
            self._store_cycle_payload(worker_key="watch_exec_agent_sizing", payload=payload)
            return payload

    def _monitoring_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        with self.session_factory() as db:
            symbols = list_autonomous_monitoring_symbols(
                settings=self.settings,
                db=db,
            )
            if not symbols:
                return {
                    "status_code": 200,
                    "monitored_symbols": [],
                    "price_count": 0,
                    "price_source_configured": self._monitoring_price_source_state.configured,
                    "price_source": self._monitoring_price_source_state.snapshot(),
                    "idle": True,
                }

            prices = self._load_monitoring_prices(
                symbols=symbols,
                observed_at=started_at,
            )
            adapter = self.broker_adapter_factory(self.settings)
            result = run_autonomous_monitoring_cycle(
                settings=self.settings,
                db=db,
                adapter=adapter,
                prices=prices,
                now=started_at,
            )
            payload = {
                "status_code": result.status_code,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                **result.payload,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload

    def _load_monitoring_prices(
        self,
        *,
        symbols: list[str],
        observed_at: datetime,
    ) -> list[MonitoringPriceRequest]:
        if self.monitoring_price_source is None:
            self._mark_monitoring_price_source_missing(
                checked_at=observed_at,
                symbol_count=len(symbols),
            )
            return []
        source = self.monitoring_price_source
        source_name, provider = _source_identity(source)
        try:
            if hasattr(source, "get_prices"):
                raw_prices = source.get_prices(
                    symbols=symbols,
                    observed_at=observed_at,
                    settings=self.settings,
                )
            elif callable(source):
                raw_prices = _call_price_source(
                    source=source,
                    symbols=symbols,
                    observed_at=observed_at,
                    settings=self.settings,
                )
            else:
                raise TypeError("monitoring_price_source must be callable or define get_prices()")
            prices = _normalize_monitoring_prices(raw_prices)
        except Exception as exc:
            self._mark_monitoring_price_source_error(
                checked_at=observed_at,
                symbol_count=len(symbols),
                source_name=source_name,
                provider=provider,
                error=str(exc),
            )
            LOGGER.warning(
                "monitoring_price_source_failed",
                extra={
                    "runtime_mode": self.settings.runtime_mode,
                    "source_name": source_name,
                    "provider": provider,
                    "symbol_count": len(symbols),
                },
            )
            return []

        self._mark_monitoring_price_source_result(
            checked_at=observed_at,
            symbol_count=len(symbols),
            source_name=source_name,
            provider=provider,
            price_count=len(prices),
        )
        return prices

    def _mark_cycle_started(
        self,
        *,
        worker_key: str,
        started_at: datetime,
    ) -> None:
        with self._lock:
            state = self._state_for(worker_key)
            state.status = "running"
            state.last_started_at = started_at
            state.last_error = None

    def _mark_cycle_succeeded(
        self,
        *,
        worker_key: str,
        heartbeat_at: datetime,
        payload: dict[str, object],
    ) -> None:
        with self._lock:
            state = self._state_for(worker_key)
            state.status = "running"
            state.last_heartbeat_at = heartbeat_at
            state.last_success_at = heartbeat_at
            state.last_error = None
            state.last_cycle = payload

    def _mark_cycle_failed(
        self,
        *,
        worker_key: str,
        heartbeat_at: datetime,
        error: str,
    ) -> None:
        with self._lock:
            state = self._state_for(worker_key)
            state.status = "error"
            state.last_heartbeat_at = heartbeat_at
            state.last_error = error

    def _store_cycle_payload(
        self,
        *,
        worker_key: str,
        payload: dict[str, object],
    ) -> None:
        with self._lock:
            state = self._state_for(worker_key)
            state.last_cycle = payload

    def _state_for(self, worker_key: str) -> AutonomousWorkerState:
        if worker_key == "dispatch":
            return self._dispatch_state
        if worker_key == "watch_exec_agent_sizing":
            return self._watch_exec_agent_sizing_state
        return self._monitoring_state

    def _initial_monitoring_price_source_state(self) -> MonitoringPriceSourceState:
        if self.monitoring_price_source is None:
            return MonitoringPriceSourceState(
                configured=False,
                source_name=None,
                provider=None,
                status="disabled",
                reason="no monitoring price source is configured",
            )
        source_name, provider = _source_identity(self.monitoring_price_source)
        return MonitoringPriceSourceState(
            configured=True,
            source_name=source_name,
            provider=provider,
            status="configured",
        )

    def _mark_monitoring_price_source_missing(
        self,
        *,
        checked_at: datetime,
        symbol_count: int,
    ) -> None:
        with self._lock:
            state = self._monitoring_price_source_state
            state.configured = False
            state.status = "disabled"
            state.reason = "no monitoring price source is configured"
            state.last_checked_at = checked_at
            state.last_symbol_count = symbol_count
            state.last_price_count = 0
            state.last_error = None
            state.last_error_at = None

    def _mark_monitoring_price_source_result(
        self,
        *,
        checked_at: datetime,
        symbol_count: int,
        source_name: str | None,
        provider: str | None,
        price_count: int,
    ) -> None:
        with self._lock:
            state = self._monitoring_price_source_state
            state.configured = True
            state.source_name = source_name
            state.provider = provider
            state.last_checked_at = checked_at
            state.last_symbol_count = symbol_count
            state.last_price_count = price_count
            if price_count >= symbol_count:
                state.status = "ready"
                state.reason = None
                state.last_success_at = checked_at
                state.last_error = None
                state.last_error_at = None
                return

            state.status = "degraded"
            state.last_error = None
            state.last_error_at = None
            if price_count == 0:
                state.reason = "price source returned no prices for monitored symbols"
            else:
                state.reason = (
                    f"price source returned {price_count} prices for {symbol_count} monitored symbols"
                )
                state.last_success_at = checked_at

    def _mark_monitoring_price_source_error(
        self,
        *,
        checked_at: datetime,
        symbol_count: int,
        source_name: str | None,
        provider: str | None,
        error: str,
    ) -> None:
        with self._lock:
            state = self._monitoring_price_source_state
            state.configured = True
            state.source_name = source_name
            state.provider = provider
            state.status = "degraded"
            state.reason = "monitoring price source request failed"
            state.last_checked_at = checked_at
            state.last_symbol_count = symbol_count
            state.last_price_count = 0
            state.last_error = error
            state.last_error_at = checked_at


def _call_price_source(
    *,
    source: Callable[..., object],
    symbols: list[str],
    observed_at: datetime,
    settings: Settings,
) -> object:
    try:
        return source(symbols=symbols, observed_at=observed_at, settings=settings)
    except TypeError:
        try:
            return source(symbols, observed_at, settings)
        except TypeError:
            return source(symbols)


def _source_identity(source: object) -> tuple[str | None, str | None]:
    if hasattr(source, "describe"):
        description = source.describe()
        if isinstance(description, dict):
            source_name = description.get("source_name")
            provider = description.get("provider")
            return (
                str(source_name) if source_name is not None else source.__class__.__name__,
                str(provider) if provider is not None else None,
            )
    provider = getattr(source, "provider", None)
    return source.__class__.__name__, str(provider) if provider is not None else None


def _normalize_monitoring_prices(raw_prices: object) -> list[MonitoringPriceRequest]:
    if raw_prices is None:
        return []
    if not isinstance(raw_prices, Iterable):
        raise TypeError("monitoring_price_source must return an iterable of prices")

    normalized: list[MonitoringPriceRequest] = []
    for item in raw_prices:
        if isinstance(item, MonitoringPriceRequest):
            normalized.append(item)
            continue
        if isinstance(item, dict):
            normalized.append(MonitoringPriceRequest.model_validate(item))
            continue
        if isinstance(item, tuple) and len(item) == 2:
            symbol, price = item
            normalized.append(
                MonitoringPriceRequest(
                    symbol=str(symbol),
                    price=Decimal(str(price)),
                )
            )
            continue
        raise TypeError(
            "monitoring_price_source items must be MonitoringPriceRequest, dict, or (symbol, price) tuples"
        )
    return normalized


def _is_regular_market_session(*, now: datetime, market_timezone: str) -> bool:
    try:
        local_now = now.astimezone(ZoneInfo(market_timezone))
    except Exception:
        local_now = now.astimezone(ZoneInfo("Asia/Seoul"))
    if local_now.weekday() >= 5:
        return False
    return time(9, 0) <= local_now.time() <= time(15, 30)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()
