from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Callable, Iterable

from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.monitoring import (
    list_autonomous_monitoring_symbols,
    run_autonomous_monitoring_cycle,
)

LOGGER = logging.getLogger(__name__)

DISPATCH_LOOP_WORKER_NAME = "execution_job_dispatch_loop"
MONITORING_LOOP_WORKER_NAME = "monitoring_dispatch_loop"


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
class AutonomousRuntimeCoordinator:
    settings: Settings
    session_factory: sessionmaker[Session]
    broker_adapter_factory: Callable[[Settings], BrokerAdapter]
    monitoring_price_source: object | None = None
    _dispatch_task: asyncio.Task[None] | None = field(default=None, init=False)
    _monitoring_task: asyncio.Task[None] | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _dispatch_state: AutonomousWorkerState = field(init=False)
    _monitoring_state: AutonomousWorkerState = field(init=False)

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
            )
        )

    async def stop(self) -> None:
        tasks = [
            task
            for task in (self._dispatch_task, self._monitoring_task)
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
                    "price_source_configured": self.monitoring_price_source is not None,
                },
            }

    async def _run_periodic_loop(
        self,
        *,
        worker_key: str,
        interval_seconds: float,
        cycle: Callable[[datetime], asyncio.Future | object],
    ) -> None:
        try:
            while True:
                await asyncio.sleep(interval_seconds)
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
            return {
                "status_code": result.status_code,
                **result.payload,
            }

    async def _run_monitoring_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._monitoring_cycle_sync, started_at)

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
                    "price_source_configured": self.monitoring_price_source is not None,
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
            return {
                "status_code": result.status_code,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self.monitoring_price_source is not None,
                **result.payload,
            }

    def _load_monitoring_prices(
        self,
        *,
        symbols: list[str],
        observed_at: datetime,
    ) -> list[MonitoringPriceRequest]:
        if self.monitoring_price_source is None:
            return []
        source = self.monitoring_price_source
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
        return _normalize_monitoring_prices(raw_prices)

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

    def _state_for(self, worker_key: str) -> AutonomousWorkerState:
        if worker_key == "dispatch":
            return self._dispatch_state
        return self._monitoring_state


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


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()
