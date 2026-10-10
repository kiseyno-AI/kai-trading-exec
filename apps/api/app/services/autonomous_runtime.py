from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import sqlite3
import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, TypeVar
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.broker.kis import KIS_OVERSEAS_QUOTE_EMPTY_CODE
from app.core.config import Settings
from app.models.runtime import BrokerOrder
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.domestic_order_router import (
    domestic_market_session,
    domestic_session_has_enabled_route,
)
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.execution_account_route import classify_kis_account_route
from app.services.execution_semantics import (
    KRX_EXCHANGES,
    watch_buy_direction_from_payloads,
)
from app.services.monitoring import (
    ACTIVE_WATCH_MONITORING_STATUSES,
    PENDING_DISPATCH_MONITORING_STATUSES,
    SELL_MONITORED_INTENTS,
    WATCH_BUY_INTENTS,
    _active_kis_position_filters,
    _live_automated_quote_trust_reason_for_side,
    _load_positions_by_symbol,
    _krx_trading_day_resolver,
    _maintain_post_entry_exit_monitors,
    _quote_stale_reason,
    _sync_positions_for_monitoring_routes,
    list_autonomous_monitoring_symbols,
    run_autonomous_monitoring_cycle,
    _load_core_desired_monitoring_symbols,
)
from app.services.monitoring_price_source import is_us_regular_session
from app.services.runtime_transitions import (
    ACTIVE_SELL_MONITORING_STATUSES,
    AGENT_ACTIVE_MONITORING_STATUSES,
    SELL_TRIGGERED_BLOCKED_STATUSES,
    SELL_TRIGGERED_PENDING_DISPATCH_STATUSES,
)
from app.services.nxt_eligibility import refresh_nxt_eligible_symbols
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_position_identity,
)
from app.services.reconciliation import reconcile_pending_broker_orders
from app.services.runtime_shutdown import (
    CommitGuardedSessionFactory,
    GuardedBrokerAdapter,
    RuntimeCycleFence,
)
from app.services.trade_outcome_feedback import (
    REPORT_VERSION as TRADE_OUTCOME_FEEDBACK_VERSION,
    ensure_fresh_trade_outcome_feedback,
)
from app.services.trading_sessions import configured_krx_market_holidays
from app.services.watch_exec_agent_sizing import (
    _inside_sizing_window,
    run_due_watch_exec_agent_sizing,
)

LOGGER = logging.getLogger(__name__)

DISPATCH_LOOP_WORKER_NAME = "execution_job_dispatch_loop"
MONITORING_LOOP_WORKER_NAME = "monitoring_dispatch_loop"
WATCH_EXEC_AGENT_SIZING_WORKER_NAME = "watch_exec_agent_sizing_loop"
NXT_ELIGIBLE_SYNC_WORKER_NAME = "nxt_eligible_sync_loop"
MAX_STATUS_DETAIL_ITEMS = 20
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
US_EASTERN_TIMEZONE = ZoneInfo("America/New_York")
US_EQUITY_EXCHANGES = frozenset(
    {"US", "NAS", "NASD", "NASDAQ", "NYS", "NYSE", "AMS", "AMEX"}
)
DOMESTIC_SESSION_BOUNDARIES = (
    time(8, 0),
    time(8, 30),
    time(8, 40),
    time(8, 50),
    time(9, 0),
    time(15, 30),
    time(15, 40),
    time(16, 0),
    time(18, 0),
)
KRX_CALENDAR_RETRY_INTERVAL = timedelta(seconds=60)
DISPATCH_IDLE_DB_RESCAN_INTERVAL = timedelta(seconds=60)
MONITORING_NO_PRICE_DB_RESCAN_INTERVAL = timedelta(seconds=60)
_CACHED_KIS_ROUTE_EXCHANGES = KRX_EXCHANGES | frozenset({"NASD", "NYSE", "AMEX"})
_CycleResult = TypeVar("_CycleResult")


def _active_kis_position_scope(settings: Settings) -> tuple[str, str] | None:
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return None
    return identity.provider, identity.account_scope


def _active_kis_execution_scope_payload(
    settings: Settings,
) -> dict[str, object] | None:
    active_scope = _active_kis_position_scope(settings)
    if active_scope is None:
        return None
    provider, account_scope = active_scope
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": provider,
        "account_scope": account_scope,
    }


def _cached_positions_by_symbol(
    rows: list[tuple[object, ...]],
) -> dict[str, CachedMonitoringPosition]:
    grouped: dict[str, list[CachedMonitoringPosition]] = {}
    for row in rows:
        symbol = str(row[0] or "").strip()
        quantity = int(row[1] or 0)
        raw_payload = _cached_json_object(row[4]) if len(row) > 4 else {}
        sync_context = raw_payload.get("_position_sync")
        monitoring_eligible = not (
            isinstance(sync_context, dict)
            and sync_context.get("monitoring_eligible") is False
        )
        if symbol and quantity > 0:
            grouped.setdefault(symbol, []).append(
                CachedMonitoringPosition(
                    symbol=symbol,
                    quantity=quantity,
                    exchange=str(row[2] or "").strip().upper() or None,
                    provider=str(row[3] or "").strip().lower() or None,
                    monitoring_eligible=monitoring_eligible,
                )
            )
    return {
        symbol: positions[0]
        for symbol, positions in grouped.items()
        if len(positions) == 1
    }


def _cached_item_exchange(*, symbol: str, risk_context: object) -> str | None:
    context = _cached_json_object(risk_context)
    exchange = str(context.get("exchange") or "").strip().upper()
    if exchange:
        return exchange
    return "KRX" if _is_krx_symbol(symbol) else None


def _cached_item_matches_active_kis_route(
    *,
    settings: Settings,
    symbol: str,
    provider: object,
    risk_context: object,
    quantity_policy: object,
) -> bool:
    if _active_kis_position_scope(settings) is None:
        return False
    normalized_provider = str(provider or ("kis" if _is_krx_symbol(symbol) else "")).strip().lower()
    if normalized_provider in {"korea-investment", "korea_investment"}:
        normalized_provider = "kis"
    exchange = _cached_item_exchange(symbol=symbol, risk_context=risk_context)
    return bool(
        normalized_provider == "kis"
        and exchange in _CACHED_KIS_ROUTE_EXCHANGES
        and classify_kis_account_route(
            settings=settings,
            provider=normalized_provider,
            risk_context=_cached_json_object(risk_context),
            quantity_policy=_cached_json_object(quantity_policy),
        )
        == "active"
    )


def _cached_item_matches_position_route(
    *,
    item: CachedMonitoringItem,
    position: CachedMonitoringPosition,
) -> bool:
    item_provider = str(
        item.provider or ("kis" if _is_krx_symbol(item.symbol) else "")
    ).strip().lower()
    position_provider = str(position.provider or "").strip().lower()
    item_exchange = _cached_position_identity_exchange(
        provider=item_provider,
        exchange=item.exchange or ("KRX" if _is_krx_symbol(item.symbol) else ""),
    )
    position_exchange = _cached_position_identity_exchange(
        provider=position_provider,
        exchange=position.exchange,
    )
    return bool(
        item_provider
        and item_exchange
        and item_provider == position_provider
        and item_exchange == position_exchange
    )


def _cached_position_identity_exchange(*, provider: str, exchange: object) -> str:
    normalized_exchange = str(exchange or "").strip().upper()
    if provider == "kis" and normalized_exchange in KRX_EXCHANGES:
        return "KRX"
    return normalized_exchange


@dataclass
class AutonomousWorkerState:
    worker_name: str
    poll_interval_seconds: float
    status: str = "idle"
    cadence_mode: str = "configured"
    effective_poll_interval_seconds: float | None = None
    next_wake_at: datetime | None = None
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
            "cadence_mode": self.cadence_mode,
            "effective_poll_interval_seconds": (
                self.effective_poll_interval_seconds
                if self.effective_poll_interval_seconds is not None
                else self.poll_interval_seconds
            ),
            "next_wake_at": _serialize_datetime(self.next_wake_at),
            "last_started_at": _serialize_datetime(self.last_started_at),
            "last_heartbeat_at": _serialize_datetime(self.last_heartbeat_at),
            "last_success_at": _serialize_datetime(self.last_success_at),
            "last_error": self.last_error,
            "last_cycle": _compact_cycle_payload(self.last_cycle),
        }


@dataclass(frozen=True)
class PeriodicLoopSchedule:
    delay_seconds: float
    cadence_mode: str
    next_wake_at: datetime
    next_market_active_at: datetime | None = None


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
    diagnostics: dict[str, object] = field(default_factory=dict)

    def snapshot(self) -> dict[str, object]:
        payload = {
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
        if self.diagnostics:
            payload["diagnostics"] = dict(self.diagnostics)
        return payload


@dataclass(frozen=True)
class CachedMonitoringPosition:
    symbol: str
    quantity: int
    provider: str | None = None
    exchange: str | None = None
    monitoring_eligible: bool = True


@dataclass(frozen=True)
class CachedMonitoringItem:
    id: str
    symbol: str
    intent: str
    status: str
    trigger_price: Decimal | None
    trigger_direction: str | None = None
    close_exit_date: date | None = None
    provider: str | None = None
    exchange: str | None = None


@dataclass(frozen=True)
class MonitoringRuntimeSnapshot:
    loaded_at: datetime
    symbols: list[str]
    positions_by_symbol: dict[str, CachedMonitoringPosition]
    items: list[CachedMonitoringItem]
    filled_today_symbols: list[str]
    execution_scope: dict[str, object] | None = None

    def snapshot(self) -> dict[str, object]:
        return {
            "loaded_at": _serialize_datetime(self.loaded_at),
            "symbols": list(self.symbols),
            "symbol_count": len(self.symbols),
            "item_count": len(self.items),
            "position_count": len(self.positions_by_symbol),
            "filled_today_symbols": list(self.filled_today_symbols),
            "execution_scope": (
                dict(self.execution_scope)
                if isinstance(self.execution_scope, dict)
                else None
            ),
        }


@dataclass
class AutonomousRuntimeCoordinator:
    settings: Settings
    session_factory: sessionmaker[Session]
    broker_adapter_factory: Callable[[Settings], BrokerAdapter]
    monitoring_price_source: object | None = None
    _dispatch_task: asyncio.Task[None] | None = field(default=None, init=False)
    _monitoring_task: asyncio.Task[None] | None = field(default=None, init=False)
    _daily_krx_position_discovery_task: asyncio.Task[None] | None = field(default=None, init=False)
    _watch_exec_agent_sizing_task: asyncio.Task[None] | None = field(default=None, init=False)
    _nxt_eligible_sync_task: asyncio.Task[None] | None = field(default=None, init=False)
    _inflight_cycles: set[asyncio.Future[Any]] = field(default_factory=set, init=False)
    _lifecycle_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _stopping: bool = field(default=False, init=False)
    _shutdown_fence: RuntimeCycleFence = field(
        default_factory=RuntimeCycleFence,
        init=False,
        repr=False,
    )
    _cycle_session_factory: sessionmaker[Session] = field(init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _dispatch_state: AutonomousWorkerState = field(init=False)
    _monitoring_state: AutonomousWorkerState = field(init=False)
    _watch_exec_agent_sizing_state: AutonomousWorkerState = field(init=False)
    _nxt_eligible_sync_state: AutonomousWorkerState = field(init=False)
    _monitoring_price_source_state: MonitoringPriceSourceState = field(init=False)
    _monitoring_runtime_snapshot: MonitoringRuntimeSnapshot | None = field(default=None, init=False)
    _monitoring_snapshot_generation: int = field(default=0, init=False)
    _monitoring_snapshot_refresh_mutex: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
    )
    _monitoring_snapshot_last_reconciled_at: datetime | None = field(default=None, init=False)
    _monitoring_order_reconcile_last_at: datetime | None = field(default=None, init=False)
    _monitoring_cycle_mutex: threading.Lock = field(default_factory=threading.Lock, init=False)
    _monitoring_cycle_mutex_started_at: datetime | None = field(default=None, init=False)
    _monitoring_reconciliation_mutex: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
    )
    _monitoring_symbol_cursor: int = field(default=0, init=False)
    _monitoring_next_no_price_db_check_at: datetime | None = field(
        default=None,
        init=False,
    )
    _dispatch_next_db_check_at: datetime | None = field(default=None, init=False)
    _dispatch_schedule_generation: int = field(default=0, init=False)
    _price_source_closed: bool = field(default=False, init=False)
    _krx_trading_days: dict[date, bool] = field(default_factory=dict, init=False)
    _krx_calendar_sources: dict[date, str] = field(default_factory=dict, init=False)
    _krx_calendar_attempts: dict[date, datetime] = field(default_factory=dict, init=False)
    _krx_calendar_last_checked_at: datetime | None = field(default=None, init=False)
    _krx_calendar_last_error: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._cycle_session_factory = CommitGuardedSessionFactory(  # type: ignore[assignment]
            self.session_factory,
            self._shutdown_fence,
        )
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
            status=(
                "disabled"
                if not self.settings.autonomous_runtime_enabled
                or not self.settings.exec_agent_watch_sizing_enabled
                else "idle"
            ),
        )
        self._nxt_eligible_sync_state = AutonomousWorkerState(
            worker_name=NXT_ELIGIBLE_SYNC_WORKER_NAME,
            poll_interval_seconds=self.settings.nxt_eligible_source_ttl_seconds,
            status=(
                "disabled"
                if not self.settings.autonomous_runtime_enabled or not self.settings.nxt_eligible_sync_enabled
                else "idle"
            ),
        )
        self._monitoring_price_source_state = self._initial_monitoring_price_source_state()

    async def start(self) -> None:
        if not self.settings.autonomous_runtime_enabled:
            return
        if self._stopping:
            raise RuntimeError("cannot start autonomous runtime while shutting down")
        async with self._lifecycle_lock:
            if any(task is not None and not task.done() for task in self._loop_tasks()):
                return
            if self._inflight_cycles:
                raise RuntimeError(
                    "cannot start autonomous runtime while an in-flight cycle is "
                    "still draining"
                )
            self._stopping = False
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
                    # Let startup/control initialization finish before the first
                    # DB-backed cycle. A one-interval delay also prevents an
                    # in-memory SQLite test/runtime from sharing one connection
                    # with the first operator request.
                    run_immediately=False,
                )
            )
            if self.settings.runtime_mode == "live":
                self._daily_krx_position_discovery_task = asyncio.create_task(
                    self._run_daily_krx_position_discovery_loop()
                )
            if self.settings.exec_agent_watch_sizing_enabled:
                self._watch_exec_agent_sizing_task = asyncio.create_task(
                    self._run_periodic_loop(
                        worker_key="watch_exec_agent_sizing",
                        interval_seconds=self.settings.exec_agent_watch_sizing_loop_interval_seconds,
                        cycle=self._run_watch_exec_agent_sizing_cycle,
                    )
                )
            if self.settings.nxt_eligible_sync_enabled:
                self._nxt_eligible_sync_task = asyncio.create_task(
                    self._run_periodic_loop(
                        worker_key="nxt_eligible_sync",
                        interval_seconds=self.settings.nxt_eligible_source_ttl_seconds,
                        cycle=self._run_nxt_eligible_sync_cycle,
                        run_immediately=True,
                    )
                )

    def mark_startup_blocked(self, reason: str) -> None:
        with self._lock:
            for state in (
                self._dispatch_state,
                self._monitoring_state,
                self._watch_exec_agent_sizing_state,
                self._nxt_eligible_sync_state,
            ):
                if state.status == "disabled":
                    continue
                state.status = "blocked"
                state.last_error = reason

    def begin_shutdown(self) -> None:
        self._stopping = True
        self._shutdown_fence.begin_shutdown()

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            self.begin_shutdown()
            tasks = [task for task in self._loop_tasks() if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._drain_inflight_cycles(
                timeout=self._shutdown_drain_timeout_seconds()
            )
            self._finish_stop()

    async def wait_stopped(self) -> None:
        """Wait without a timeout so lifespan cannot outlive a worker thread."""

        async with self._lifecycle_lock:
            self.begin_shutdown()
            tasks = [task for task in self._loop_tasks() if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._drain_inflight_cycles(timeout=None)
            self._finish_stop()

    def _finish_stop(self) -> None:
        self._dispatch_task = None
        self._monitoring_task = None
        self._daily_krx_position_discovery_task = None
        self._watch_exec_agent_sizing_task = None
        self._nxt_eligible_sync_task = None
        if not self._price_source_closed:
            close_price_source = getattr(self.monitoring_price_source, "close", None)
            if callable(close_price_source):
                close_price_source()
            self._price_source_closed = True
        self._shutdown_fence.finish_shutdown()
        self._stopping = False

    def _loop_tasks(self) -> tuple[asyncio.Task[None] | None, ...]:
        return (
            self._dispatch_task,
            self._monitoring_task,
            self._daily_krx_position_discovery_task,
            self._watch_exec_agent_sizing_task,
            self._nxt_eligible_sync_task,
        )

    def snapshot(self) -> dict[str, object]:
        krx_calendar = self._krx_calendar_snapshot()
        with self._lock:
            return {
                "enabled": self.settings.autonomous_runtime_enabled,
                "execution_scope": _active_kis_execution_scope_payload(self.settings),
                "dispatch": self._dispatch_state.snapshot(),
                "monitoring": {
                    **self._monitoring_state.snapshot(),
                    "price_source_configured": self._monitoring_price_source_state.configured,
                    "price_source": self._monitoring_price_source_state.snapshot(),
                    "runtime_snapshot": (
                        self._monitoring_runtime_snapshot.snapshot()
                        if self._monitoring_runtime_snapshot is not None
                        else None
                    ),
                },
                "watch_exec_agent_sizing": self._watch_exec_agent_sizing_state.snapshot(),
                "nxt_eligible_sync": self._nxt_eligible_sync_state.snapshot(),
                "krx_calendar": krx_calendar,
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
            while not self._stopping:
                schedule: PeriodicLoopSchedule | None = None
                if first_cycle and run_immediately:
                    first_cycle = False
                else:
                    schedule_now = datetime.now(UTC)
                    if first_cycle:
                        schedule = PeriodicLoopSchedule(
                            delay_seconds=float(interval_seconds),
                            cadence_mode="startup",
                            next_wake_at=schedule_now
                            + timedelta(seconds=float(interval_seconds)),
                        )
                    else:
                        schedule = self._periodic_loop_schedule(
                            worker_key=worker_key,
                            configured_interval_seconds=interval_seconds,
                            now=schedule_now,
                        )
                    self._record_worker_schedule(
                        worker_key=worker_key,
                        schedule=schedule,
                    )
                    await asyncio.sleep(schedule.delay_seconds)
                    first_cycle = False
                if self._stopping:
                    return
                started_at = datetime.now(UTC)
                self._mark_cycle_started(worker_key=worker_key, started_at=started_at)
                try:
                    awaitable = cycle(started_at)
                    payload = await self._await_cycle_payload(
                        worker_key=worker_key,
                        awaitable=awaitable,
                    )
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
                    if payload is None:
                        continue
                    if isinstance(payload, dict) and payload.get("skipped") is True:
                        self._mark_cycle_busy(
                            worker_key=worker_key,
                            heartbeat_at=datetime.now(UTC),
                            payload=payload,
                        )
                        continue
                    self._mark_cycle_succeeded(
                        worker_key=worker_key,
                        heartbeat_at=datetime.now(UTC),
                        payload=payload,
                    )
        except asyncio.CancelledError:
            return

    def _periodic_loop_schedule(
        self,
        *,
        worker_key: str,
        configured_interval_seconds: float,
        now: datetime,
    ) -> PeriodicLoopSchedule:
        delay_seconds = float(configured_interval_seconds)
        cadence_mode = "configured"
        next_market_active_at: datetime | None = None
        if (
            worker_key in {"dispatch", "monitoring"}
            and _domestic_only_live_runtime(self.settings)
        ):
            next_market_active_at = _next_enabled_domestic_session_start(
                settings=self.settings,
                now=now,
                trading_days=self._krx_trading_days_snapshot(),
            )
            if next_market_active_at is None:
                cadence_mode = "market_active"
            else:
                cadence_mode = "domestic_market_closed"
                seconds_until_active = max(
                    (next_market_active_at - now.astimezone(UTC)).total_seconds(),
                    0.001,
                )
                closed_market_interval = max(
                    float(configured_interval_seconds),
                    float(self.settings.domestic_closed_market_idle_interval_seconds),
                )
                delay_seconds = min(
                    closed_market_interval,
                    seconds_until_active,
                )
        next_wake_at = now.astimezone(UTC) + timedelta(seconds=delay_seconds)
        return PeriodicLoopSchedule(
            delay_seconds=delay_seconds,
            cadence_mode=cadence_mode,
            next_wake_at=next_wake_at,
            next_market_active_at=next_market_active_at,
        )

    def _record_worker_schedule(
        self,
        *,
        worker_key: str,
        schedule: PeriodicLoopSchedule,
    ) -> None:
        with self._lock:
            state = self._state_for(worker_key)
            state.cadence_mode = schedule.cadence_mode
            state.effective_poll_interval_seconds = schedule.delay_seconds
            state.next_wake_at = schedule.next_wake_at

    async def _await_cycle_payload(
        self,
        *,
        worker_key: str,
        awaitable: asyncio.Future | object,
    ) -> dict[str, object] | None:
        if worker_key != "monitoring":
            return await awaitable

        task = self._track_inflight_cycle(asyncio.ensure_future(awaitable))
        done, _ = await asyncio.wait(
            {task},
            timeout=self.settings.monitoring_cycle_timeout_seconds,
        )
        if done:
            return task.result()

        timeout_seconds = self.settings.monitoring_cycle_timeout_seconds
        timeout_error = f"cycle timed out after {timeout_seconds:g}s"
        timeout_payload: dict[str, object] = {
            "status_code": 504,
            "timeout": True,
            "reason": "monitoring_cycle_timed_out",
            "timeout_seconds": timeout_seconds,
            "price_source_configured": self._monitoring_price_source_state.configured,
            "price_source": self._monitoring_price_source_state.snapshot(),
        }
        LOGGER.error(
            "autonomous_runtime_cycle_timed_out",
            extra={
                "worker_name": self._state_for(worker_key).worker_name,
                "runtime_mode": self.settings.runtime_mode,
                "timeout_seconds": timeout_seconds,
            },
        )
        self._store_cycle_payload(worker_key=worker_key, payload=timeout_payload)
        self._mark_cycle_failed(
            worker_key=worker_key,
            heartbeat_at=datetime.now(UTC),
            error=timeout_error,
        )

        def _log_late_failure(done_task: asyncio.Future) -> None:
            try:
                done_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                LOGGER.exception(
                    "autonomous_runtime_cycle_failed_after_timeout",
                    extra={
                        "worker_name": self._state_for(worker_key).worker_name,
                        "runtime_mode": self.settings.runtime_mode,
                        "timeout_seconds": timeout_seconds,
                    },
                )

        task.add_done_callback(_log_late_failure)
        return None

    async def _run_dispatch_cycle(self, started_at: datetime) -> dict[str, object]:
        return await self._run_in_thread(self._dispatch_cycle_sync, started_at)

    def _dispatch_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        with self._lock:
            next_db_check_at = self._dispatch_next_db_check_at
            schedule_generation = self._dispatch_schedule_generation
            previous_payload = self._dispatch_state.last_cycle
        if next_db_check_at is not None and started_at < next_db_check_at:
            payload = {
                "status_code": 200,
                "blocked": bool(
                    previous_payload.get("blocked")
                    if isinstance(previous_payload, dict)
                    else False
                ),
                "block_reason": (
                    previous_payload.get("block_reason")
                    if isinstance(previous_payload, dict)
                    else None
                ),
                "triggered_at": started_at.isoformat(),
                "due_job_count": 0,
                "completed_job_count": 0,
                "retrying_job_count": 0,
                "failed_job_count": 0,
                "jobs": [],
                "safety": (
                    previous_payload.get("safety")
                    if isinstance(previous_payload, dict)
                    else None
                ),
                "db_scan_skipped": True,
                "next_db_check_at": next_db_check_at.isoformat(),
            }
            self._store_cycle_payload(worker_key="dispatch", payload=payload)
            return payload

        with self._cycle_session_factory() as db:
            adapter = self._broker_adapter_for_cycle()
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
        next_dispatch_at = _payload_datetime(payload.get("next_dispatch_at"))
        fallback_check_at = started_at + DISPATCH_IDLE_DB_RESCAN_INTERVAL
        next_check_at = (
            min(next_dispatch_at, fallback_check_at)
            if next_dispatch_at is not None
            else fallback_check_at
        )
        with self._lock:
            if self._dispatch_schedule_generation == schedule_generation:
                self._dispatch_next_db_check_at = next_check_at
        payload["next_db_check_at"] = next_check_at.isoformat()
        if _dispatch_payload_changed_runtime_state(payload):
            self._refresh_monitoring_runtime_snapshot(
                loaded_at=started_at,
                reason="dispatch_cycle_changed_runtime_state",
                reconcile_broker=False,
            )
        self._store_cycle_payload(worker_key="dispatch", payload=payload)
        return payload

    def invalidate_dispatch_schedule(self, *, reason: str) -> None:
        with self._lock:
            self._dispatch_schedule_generation += 1
            self._dispatch_next_db_check_at = None
        LOGGER.info(
            "dispatch_schedule_invalidated",
            extra={"reason": reason, "runtime_mode": self.settings.runtime_mode},
        )

    async def _run_monitoring_cycle(self, started_at: datetime) -> dict[str, object]:
        return await self._run_in_thread(self._monitoring_cycle_sync_guarded, started_at)

    def _monitoring_cycle_sync_guarded(self, started_at: datetime) -> dict[str, object]:
        acquired_lock = self._monitoring_cycle_mutex
        if not acquired_lock.acquire(blocking=False):
            stale_age_seconds: float | None = None
            if self._monitoring_cycle_mutex_started_at is not None:
                stale_age_seconds = (started_at - self._monitoring_cycle_mutex_started_at).total_seconds()
            if stale_age_seconds is not None and stale_age_seconds > self.settings.monitoring_cycle_timeout_seconds:
                LOGGER.error(
                    "monitoring_cycle_still_running_after_timeout",
                    extra={
                        "worker_name": MONITORING_LOOP_WORKER_NAME,
                        "runtime_mode": self.settings.runtime_mode,
                        "stale_age_seconds": stale_age_seconds,
                        "timeout_seconds": self.settings.monitoring_cycle_timeout_seconds,
                    },
                )
                self._exit_if_monitoring_cycle_is_stale(stale_age_seconds=stale_age_seconds)
            payload = {
                "status_code": 202,
                "skipped": True,
                "reason": "previous_monitoring_cycle_still_running",
                "stale_age_seconds": stale_age_seconds,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload
        self._monitoring_cycle_mutex_started_at = started_at
        try:
            return self._monitoring_cycle_sync(started_at)
        finally:
            self._monitoring_cycle_mutex_started_at = None
            acquired_lock.release()

    def _exit_if_monitoring_cycle_is_stale(self, *, stale_age_seconds: float) -> None:
        if not self.settings.monitoring_exit_on_stale_cycle_enabled:
            return
        exit_after_seconds = (
            float(self.settings.monitoring_cycle_timeout_seconds)
            * float(self.settings.monitoring_stale_cycle_exit_grace_multiplier)
        )
        if stale_age_seconds < exit_after_seconds:
            return
        LOGGER.critical(
            "monitoring_cycle_stale_exit_requested",
            extra={
                "worker_name": MONITORING_LOOP_WORKER_NAME,
                "runtime_mode": self.settings.runtime_mode,
                "stale_age_seconds": stale_age_seconds,
                "exit_after_seconds": exit_after_seconds,
            },
        )
        os._exit(75)

    async def _run_watch_exec_agent_sizing_cycle(self, started_at: datetime) -> dict[str, object]:
        return await self._run_in_thread(self._watch_exec_agent_sizing_cycle_sync, started_at)

    async def _run_nxt_eligible_sync_cycle(self, started_at: datetime) -> dict[str, object]:
        return await self._run_in_thread(self._nxt_eligible_sync_cycle_sync, started_at)

    async def _run_in_thread(
        self,
        cycle: Callable[..., _CycleResult],
        *args: object,
    ) -> _CycleResult:
        if self._stopping:
            raise asyncio.CancelledError
        generation = self._shutdown_fence.capture_generation()
        task = self._track_inflight_cycle(
            asyncio.create_task(
                asyncio.to_thread(
                    self._shutdown_fence.run,
                    generation,
                    cycle,
                    *args,
                )
            )
        )
        return await asyncio.shield(task)

    def _broker_adapter_for_cycle(self) -> BrokerAdapter:
        return GuardedBrokerAdapter(  # type: ignore[return-value]
            self.broker_adapter_factory(self.settings),
            self._shutdown_fence,
        )

    def _track_inflight_cycle(
        self,
        task: asyncio.Future[_CycleResult],
    ) -> asyncio.Future[_CycleResult]:
        self._inflight_cycles.add(task)
        task.add_done_callback(self._inflight_cycle_done)
        return task

    def _inflight_cycle_done(self, task: asyncio.Future[Any]) -> None:
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
                "autonomous runtime in-flight cycle did not drain within the "
                "configured operation timeout"
            )

    def _shutdown_drain_timeout_seconds(self) -> float:
        return max(
            float(self.settings.monitoring_cycle_timeout_seconds)
            * float(self.settings.monitoring_stale_cycle_exit_grace_multiplier),
            float(self.settings.exec_agent_trigger_time_timeout_seconds),
            float(self.settings.exec_agent_watch_sizing_timeout_seconds),
            float(self.settings.nxt_eligible_source_timeout_seconds),
            float(self.settings.kis_request_timeout_seconds),
            float(self.settings.kis_token_request_timeout_seconds),
        )

    def _watch_exec_agent_sizing_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        if (
            self.settings.exec_agent_watch_sizing_enabled
            and self.settings.exec_agent_enabled
            and not _inside_sizing_window(settings=self.settings, now=started_at)
        ):
            result = run_due_watch_exec_agent_sizing(
                settings=self.settings,
                session_factory=self._cycle_session_factory,
                now=started_at,
            )
            payload = {
                "status_code": result.status_code,
                "trade_outcome_feedback": {
                    "status": "skipped",
                    "version": TRADE_OUTCOME_FEEDBACK_VERSION,
                    "reason": "outside_sizing_window",
                },
                **result.payload,
            }
            self._store_cycle_payload(
                worker_key="watch_exec_agent_sizing",
                payload=payload,
            )
            return payload
        active_scope = _active_kis_position_scope(self.settings)
        if active_scope is None:
            feedback_status = {
                "status": "unavailable",
                "version": TRADE_OUTCOME_FEEDBACK_VERSION,
                "reason": "active_kis_account_scope_unavailable",
            }
        else:
            provider, account_scope = active_scope
            with self._cycle_session_factory() as db:
                feedback_status = ensure_fresh_trade_outcome_feedback(
                    settings=self.settings,
                    db=db,
                    now=started_at,
                    provider=provider,
                    account_scope=account_scope,
                    exchange=None,
                )
        adapter = self._broker_adapter_for_cycle()
        result = run_due_watch_exec_agent_sizing(
            settings=self.settings,
            session_factory=self._cycle_session_factory,
            adapter=adapter,
            now=started_at,
        )
        payload = {
            "status_code": result.status_code,
            "trade_outcome_feedback": feedback_status,
            **result.payload,
        }
        if int(payload.get("approved") or 0) > 0 or int(
            payload.get("rejected") or 0
        ) > 0:
            self.invalidate_monitoring_runtime_snapshot(
                reason="watch_exec_agent_sizing_changed_plan_items"
            )
            self.invalidate_dispatch_schedule(
                reason="watch_exec_agent_sizing_changed_plan_items"
            )
        self._store_cycle_payload(worker_key="watch_exec_agent_sizing", payload=payload)
        return payload

    def _nxt_eligible_sync_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        payload = refresh_nxt_eligible_symbols(settings=self.settings, now=started_at, force=True)
        self._store_cycle_payload(worker_key="nxt_eligible_sync", payload=payload)
        return payload

    def _monitoring_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        if self.settings.runtime_mode == "live" and self.session_factory is not None:
            if not self._reconcile_and_refresh_monitoring_snapshot_if_due(started_at):
                raise RuntimeError("live broker position reconciliation failed")
        workset_snapshot = self._ensure_monitoring_runtime_snapshot(started_at)
        monitoring_workset_symbols = list(workset_snapshot.symbols)
        quote_symbols, session_inactive_symbols = _session_active_monitoring_symbols(
            workset_snapshot,
            now=started_at,
        )
        position_symbols = set(workset_snapshot.positions_by_symbol) & set(quote_symbols)
        symbols, self._monitoring_symbol_cursor = _select_monitoring_symbol_batch(
            desired_symbols=quote_symbols,
            priority_symbols=position_symbols,
            batch_size=max(int(self.settings.monitoring_max_symbols_per_cycle or 0), 1),
            cursor=self._monitoring_symbol_cursor,
        )
        if self.session_factory is not None and self._reconcile_monitoring_orders_if_due(
            now=started_at,
            symbols=set(monitoring_workset_symbols),
        ):
            workset_snapshot = self._ensure_monitoring_runtime_snapshot(started_at)
            monitoring_workset_symbols = list(workset_snapshot.symbols)
            quote_symbols, session_inactive_symbols = _session_active_monitoring_symbols(
                workset_snapshot,
                now=started_at,
            )
            position_symbols = set(workset_snapshot.positions_by_symbol) & set(quote_symbols)
            symbols, self._monitoring_symbol_cursor = _select_monitoring_symbol_batch(
                desired_symbols=quote_symbols,
                priority_symbols=position_symbols,
                batch_size=max(
                    int(self.settings.monitoring_max_symbols_per_cycle or 0),
                    1,
                ),
                cursor=self._monitoring_symbol_cursor,
            )
        if not symbols:
            payload = {
                "status_code": 200,
                "monitored_symbols": [],
                "price_count": 0,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                "session_inactive_symbols": session_inactive_symbols,
                "idle": True,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload

        runtime_snapshot = _monitoring_snapshot_for_symbols(
            workset_snapshot,
            symbols=symbols,
        )

        prices = self._load_monitoring_prices(
            symbols=symbols,
            observed_at=started_at,
        )
        durably_unquoteable_symbols = _durably_unquoteable_symbols(
            self.monitoring_price_source
        )
        monitored_symbols = list(symbols)
        monitored_price_count = len(prices)
        cached_payload = self._evaluate_cached_monitoring_cycle(
            runtime_snapshot=runtime_snapshot,
            prices=prices,
            started_at=started_at,
        )
        post_entry_maintenance_count = 0
        post_entry_review_count = 0
        if prices or durably_unquoteable_symbols:
            (
                post_entry_maintenance_count,
                post_entry_review_count,
            ) = self._repair_post_entry_exit_monitors_for_cached_cycle(
                runtime_snapshot=runtime_snapshot,
                prices=prices,
                started_at=started_at,
                durably_unquoteable_symbols=durably_unquoteable_symbols,
            )
            if post_entry_maintenance_count or post_entry_review_count:
                workset_snapshot = self._refresh_monitoring_runtime_snapshot(
                    loaded_at=started_at,
                    reason="cached_monitoring_post_entry_state_change",
                    reconcile_broker=False,
                )
                workset_symbol_set = set(workset_snapshot.symbols)
                symbols = [symbol for symbol in symbols if symbol in workset_symbol_set]
                desired_symbols, session_inactive_symbols = (
                    _session_active_monitoring_symbols(
                        workset_snapshot,
                        now=started_at,
                    )
                )
                if not symbols and desired_symbols:
                    symbols, self._monitoring_symbol_cursor = _select_monitoring_symbol_batch(
                        desired_symbols=desired_symbols,
                        priority_symbols=(
                            set(workset_snapshot.positions_by_symbol)
                            & set(desired_symbols)
                        ),
                        batch_size=max(
                            int(self.settings.monitoring_max_symbols_per_cycle or 0),
                            1,
                        ),
                        cursor=self._monitoring_symbol_cursor,
                    )
                runtime_snapshot = _monitoring_snapshot_for_symbols(
                    workset_snapshot,
                    symbols=symbols,
                )
                symbol_set = set(symbols)
                prices = [price for price in prices if price.symbol in symbol_set]
                monitored_symbols = list(symbols)
                monitored_price_count = len(prices)
                cached_payload = self._evaluate_cached_monitoring_cycle(
                    runtime_snapshot=runtime_snapshot,
                    prices=prices,
                    started_at=started_at,
                )
                if not symbols:
                    payload = {
                        "status_code": 200,
                        "price_source_configured": (
                            self._monitoring_price_source_state.configured
                        ),
                        "price_source": self._monitoring_price_source_state.snapshot(),
                        "runtime_snapshot": workset_snapshot.snapshot(),
                        "session_inactive_symbols": session_inactive_symbols,
                        **cached_payload,
                        "monitored_symbols": [],
                        "price_count": 0,
                        "broker_action_symbols": [],
                        "broker_action_price_count": 0,
                        "post_entry_review_count": post_entry_review_count,
                        "idle": True,
                    }
                    self._store_cycle_payload(worker_key="monitoring", payload=payload)
                    return payload
        rearmable_sell_symbols = {
            symbol
            for symbol in cached_payload.get("rearmable_sell_symbols", [])
            if isinstance(symbol, str) and symbol
        }
        if (
            cached_payload["triggered_item_count"] == 0
            and prices
            and not rearmable_sell_symbols
        ):
            payload = {
                "status_code": 200,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                "session_inactive_symbols": session_inactive_symbols,
                **cached_payload,
            }
            if post_entry_maintenance_count:
                payload["post_entry_maintenance_count"] = post_entry_maintenance_count
            if post_entry_review_count:
                payload["post_entry_review_count"] = post_entry_review_count
            if self.settings.monitoring_max_broker_actions_per_cycle <= 0:
                payload["broker_actions_disabled"] = True
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload

        if (
            not prices
            and self.settings.monitoring_max_broker_actions_per_cycle <= 0
        ):
            payload = {
                "status_code": 200,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                **cached_payload,
                "monitored_symbols": monitored_symbols,
                "price_count": 0,
                "broker_action_symbols": [],
                "broker_action_price_count": 0,
                "broker_actions_disabled": True,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload

        if not prices:
            # Empty/failed quote cycles still have work to do: persist a
            # checkpoint, surface coverage degradation, and retry previously
            # latched pending-dispatch exits that no longer require a quote.
            pending_without_quote = any(
                item.status in PENDING_DISPATCH_MONITORING_STATUSES
                for item in runtime_snapshot.items
            ) or int(cached_payload.get("triggered_item_count") or 0) > 0
            with self._lock:
                next_no_price_db_check_at = (
                    self._monitoring_next_no_price_db_check_at
                )
            if (
                not pending_without_quote
                and next_no_price_db_check_at is not None
                and started_at < next_no_price_db_check_at
            ):
                payload = {
                    "status_code": 200,
                    "price_source_configured": (
                        self._monitoring_price_source_state.configured
                    ),
                    "price_source": self._monitoring_price_source_state.snapshot(),
                    "runtime_snapshot": workset_snapshot.snapshot(),
                    **cached_payload,
                    "monitored_symbols": monitored_symbols,
                    "price_count": 0,
                    "broker_action_symbols": [],
                    "broker_action_price_count": 0,
                    "db_scan_skipped": True,
                    "next_db_check_at": next_no_price_db_check_at.isoformat(),
                }
                self._store_cycle_payload(worker_key="monitoring", payload=payload)
                return payload
            with self._cycle_session_factory() as db:
                adapter = self._broker_adapter_for_cycle()
                result = run_autonomous_monitoring_cycle(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    prices=[],
                    symbols=symbols,
                    auth_session_factory=self.session_factory,
                    now=started_at,
                )
            with self._lock:
                self._monitoring_next_no_price_db_check_at = (
                    started_at + MONITORING_NO_PRICE_DB_RESCAN_INTERVAL
                )
            submitted_symbols = _submitted_symbols_from_monitoring_payload(
                result.payload
            )
            payload = {
                "status_code": result.status_code,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                **result.payload,
                "monitored_symbols": monitored_symbols,
                "price_count": 0,
                "broker_action_symbols": [
                    symbol for symbol in symbols if symbol in submitted_symbols
                ],
                "broker_action_price_count": 0,
                "broker_action_trace_scope": "confirmed_submissions",
                "db_evaluation_symbols": symbols,
            }
            if post_entry_maintenance_count:
                payload["pre_cached_post_entry_maintenance_count"] = (
                    post_entry_maintenance_count
                )
            if post_entry_review_count:
                payload["pre_cached_post_entry_review_count"] = post_entry_review_count
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            if int(result.payload.get("submitted_order_count") or 0) > 0:
                self._refresh_monitoring_runtime_snapshot(
                    loaded_at=started_at,
                    reason="monitoring_no_price_runtime_state_change",
                    reconcile_broker=False,
                )
            return payload

        triggered_sell_symbols = _triggered_symbols_from_cached_payload(
            cached_payload,
            include_watch_buys=False,
            include_sells=True,
        )
        triggered_buy_symbols = _triggered_symbols_from_cached_payload(
            cached_payload,
            include_watch_buys=True,
            include_sells=False,
        )
        triggered_symbols = triggered_sell_symbols | triggered_buy_symbols
        if (
            triggered_symbols
            and self.settings.monitoring_max_broker_actions_per_cycle <= 0
            and not rearmable_sell_symbols
        ):
            payload = {
                "status_code": 200,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                "broker_actions_disabled": True,
                **cached_payload,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload
        db_evaluation_symbols = triggered_symbols | rearmable_sell_symbols
        if db_evaluation_symbols:
            broker_action_limit = max(int(self.settings.monitoring_max_broker_actions_per_cycle), 1)
            sell_evaluation_symbols = (
                triggered_sell_symbols | rearmable_sell_symbols
            )
            ordered_triggered_sell_symbols = [
                symbol
                for symbol in symbols
                if symbol in sell_evaluation_symbols
            ]
            ordered_triggered_buy_symbols = [
                symbol for symbol in symbols if symbol in triggered_buy_symbols
            ][:broker_action_limit]
            ordered_triggered_symbols = [
                *ordered_triggered_sell_symbols,
                *[
                    symbol
                    for symbol in ordered_triggered_buy_symbols
                    if symbol not in sell_evaluation_symbols
                ],
            ]
            symbols = ordered_triggered_symbols
            prices = [
                price
                for price in prices
                if price.symbol in set(ordered_triggered_symbols)
            ]
            with self._cycle_session_factory() as db:
                adapter = self._broker_adapter_for_cycle()
                result = run_autonomous_monitoring_cycle(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    prices=prices,
                    symbols=symbols,
                    auth_session_factory=self.session_factory,
                    now=started_at,
                )
            reported_rearm_symbols = result.payload.get("rearmed_sell_symbols")
            confirmed_rearm_symbols = {
                symbol
                for symbol in (
                    reported_rearm_symbols
                    if isinstance(reported_rearm_symbols, list)
                    else []
                )
                if isinstance(symbol, str) and symbol
            }
            submitted_symbols = _submitted_symbols_from_monitoring_payload(
                result.payload
            )
            payload = {
                "status_code": result.status_code,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": workset_snapshot.snapshot(),
                **result.payload,
                "monitored_symbols": monitored_symbols,
                "price_count": monitored_price_count,
                "broker_action_symbols": [
                    symbol for symbol in symbols if symbol in submitted_symbols
                ],
                "broker_action_price_count": len(
                    {
                        price.symbol
                        for price in prices
                        if price.symbol in submitted_symbols
                    }
                ),
                "broker_action_trace_scope": "confirmed_submissions",
                "db_evaluation_symbols": symbols,
                "state_rearm_symbols": [
                    symbol for symbol in symbols if symbol in confirmed_rearm_symbols
                ],
                "state_rearm_candidate_symbols": [
                    symbol for symbol in symbols if symbol in rearmable_sell_symbols
                ],
            }
            if self.settings.monitoring_max_broker_actions_per_cycle <= 0:
                payload["broker_actions_disabled"] = True
            if post_entry_maintenance_count:
                payload["pre_cached_post_entry_maintenance_count"] = post_entry_maintenance_count
            if post_entry_review_count:
                payload["pre_cached_post_entry_review_count"] = post_entry_review_count
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            snapshot_refreshed = False
            if int(result.payload.get("submitted_order_count") or 0) > 0:
                snapshot_refreshed = self._reconcile_monitoring_orders_if_due(
                    now=datetime.now(UTC),
                    symbols=set(symbols),
                    force=True,
                )
            if not snapshot_refreshed:
                self._refresh_monitoring_runtime_snapshot(
                    loaded_at=started_at,
                    reason="monitoring_triggered_runtime_state_path",
                    reconcile_broker=False,
                )
            return payload

    def _ensure_monitoring_runtime_snapshot(self, now: datetime) -> MonitoringRuntimeSnapshot:
        with self._lock:
            snapshot = self._monitoring_runtime_snapshot
        refresh_reason: str | None = None
        if snapshot is None:
            refresh_reason = "monitoring_snapshot_missing"
        else:
            max_age_seconds = max(
                int(self.settings.exec_agent_max_account_snapshot_age_seconds),
                1,
            )
            age_seconds = (
                _coerce_utc_datetime(now) - _coerce_utc_datetime(snapshot.loaded_at)
            ).total_seconds()
            if age_seconds >= max_age_seconds:
                refresh_reason = "monitoring_snapshot_ttl_expired"
        if refresh_reason is not None:
            self._refresh_monitoring_runtime_snapshot(
                loaded_at=now,
                reason=refresh_reason,
                reconcile_broker=False,
            )
        with self._lock:
            snapshot = self._monitoring_runtime_snapshot
        assert snapshot is not None
        return snapshot

    def invalidate_monitoring_runtime_snapshot(self, *, reason: str) -> None:
        with self._lock:
            self._monitoring_snapshot_generation += 1
            self._monitoring_runtime_snapshot = None
            self._monitoring_next_no_price_db_check_at = None
        self.invalidate_monitoring_price_runtime_context(reason=reason)
        LOGGER.info(
            "monitoring_runtime_snapshot_invalidated",
            extra={
                "reason": reason,
                "runtime_mode": self.settings.runtime_mode,
            },
        )

    def invalidate_monitoring_price_runtime_context(self, *, reason: str) -> None:
        invalidate = getattr(
            self.monitoring_price_source,
            "invalidate_runtime_context",
            None,
        )
        if callable(invalidate):
            invalidate(reason=reason)

    def _refresh_monitoring_runtime_snapshot(
        self,
        *,
        loaded_at: datetime,
        reason: str,
        reconcile_broker: bool,
    ) -> MonitoringRuntimeSnapshot:
        with self._monitoring_snapshot_refresh_mutex:
            return self._refresh_monitoring_runtime_snapshot_serialized(
                loaded_at=loaded_at,
                reason=reason,
                reconcile_broker=reconcile_broker,
            )

    def _refresh_monitoring_runtime_snapshot_serialized(
        self,
        *,
        loaded_at: datetime,
        reason: str,
        reconcile_broker: bool,
    ) -> MonitoringRuntimeSnapshot:
        with self._lock:
            refresh_generation = self._monitoring_snapshot_generation
        broker_reconciliation_succeeded = False
        if reconcile_broker and self.settings.runtime_mode == "live":
            adapter = self._broker_adapter_for_cycle()
            with self._cycle_session_factory() as db:
                try:
                    _sync_positions_for_monitoring_routes(
                        settings=self.settings,
                        db=db,
                        adapter=adapter,
                        symbols=None,
                        discover_krx_holdings=True,
                        discover_us_holdings=self.settings.kis_us_trading_enabled,
                    )
                    db.commit()
                except Exception:
                    db.rollback()
                    LOGGER.exception(
                        "monitoring_snapshot_broker_reconcile_failed",
                        extra={"reason": reason, "runtime_mode": self.settings.runtime_mode},
                    )
                else:
                    broker_reconciliation_succeeded = True
        snapshot = self._load_monitoring_runtime_snapshot(loaded_at)
        with self._lock:
            if self._monitoring_snapshot_generation != refresh_generation:
                self.invalidate_monitoring_price_runtime_context(
                    reason="monitoring_snapshot_refresh_superseded"
                )
                raise RuntimeError(
                    "monitoring snapshot was invalidated while it was being refreshed"
                )
            self._monitoring_runtime_snapshot = snapshot
            self._monitoring_next_no_price_db_check_at = None
            if broker_reconciliation_succeeded:
                self._monitoring_snapshot_last_reconciled_at = loaded_at
        LOGGER.info(
            "monitoring_runtime_snapshot_refreshed",
            extra={
                "reason": reason,
                "runtime_mode": self.settings.runtime_mode,
                "symbol_count": len(snapshot.symbols),
                "item_count": len(snapshot.items),
                "position_count": len(snapshot.positions_by_symbol),
                "reconcile_broker": reconcile_broker,
                "broker_reconciliation_succeeded": broker_reconciliation_succeeded,
            },
        )
        return snapshot

    def _reconcile_and_refresh_monitoring_snapshot_if_due(self, now: datetime) -> bool:
        with self._monitoring_reconciliation_mutex:
            last_reconciled_at = self._monitoring_snapshot_last_reconciled_at
            if last_reconciled_at is not None:
                elapsed = (now - last_reconciled_at).total_seconds()
                if elapsed < self.settings.exec_agent_max_account_snapshot_age_seconds:
                    return True
            self._refresh_monitoring_runtime_snapshot(
                loaded_at=now,
                reason="periodic_broker_truth_refresh",
                reconcile_broker=True,
            )
            reconciled_at = self._monitoring_snapshot_last_reconciled_at
            return reconciled_at is not None and reconciled_at >= now

    async def _run_daily_krx_position_discovery_loop(self) -> None:
        while not self._stopping:
            now = datetime.now(UTC)
            if (
                _domestic_only_live_runtime(self.settings)
                and self._krx_calendar_refresh_due(now)
            ):
                await self._run_in_thread(
                    self._refresh_current_krx_trading_day,
                    now,
                )
            if _is_regular_market_session(
                now=now,
                market_timezone=str(KRX_TIMEZONE),
            ) and self._krx_trading_days_snapshot().get(
                now.astimezone(KRX_TIMEZONE).date()
            ) is not False and (
                self._monitoring_state.last_success_at is not None
                and not self._broker_positions_reconciled_on_market_date(now)
            ):
                await self._run_in_thread(
                    self._reconcile_and_refresh_monitoring_snapshot_if_due,
                    now,
                )
            if self._stopping:
                return
            await asyncio.sleep(30)

    def _krx_calendar_refresh_due(self, now: datetime) -> bool:
        market_date = now.astimezone(KRX_TIMEZONE).date()
        if market_date.weekday() >= 5:
            return False
        with self._lock:
            if market_date in self._krx_trading_days:
                return False
            last_attempt = self._krx_calendar_attempts.get(market_date)
        return bool(
            last_attempt is None
            or now.astimezone(UTC) - last_attempt >= KRX_CALENDAR_RETRY_INTERVAL
        )

    def _refresh_current_krx_trading_day(self, now: datetime) -> None:
        checked_at = now.astimezone(UTC)
        market_date = checked_at.astimezone(KRX_TIMEZONE).date()
        if market_date.weekday() >= 5:
            return
        configured_holidays = configured_krx_market_holidays(
            self.settings.krx_market_holiday_dates
        )
        with self._lock:
            self._krx_trading_days = {
                cached_date: is_open
                for cached_date, is_open in self._krx_trading_days.items()
                if cached_date == market_date
            }
            self._krx_calendar_sources = {
                cached_date: source
                for cached_date, source in self._krx_calendar_sources.items()
                if cached_date == market_date
            }
            self._krx_calendar_attempts = {
                cached_date: attempted_at
                for cached_date, attempted_at in self._krx_calendar_attempts.items()
                if cached_date == market_date
            }
            if market_date in self._krx_trading_days:
                return
            last_attempt = self._krx_calendar_attempts.get(market_date)
            if (
                last_attempt is not None
                and checked_at - last_attempt < KRX_CALENDAR_RETRY_INTERVAL
            ):
                return
            self._krx_calendar_attempts[market_date] = checked_at
        if market_date in configured_holidays:
            with self._lock:
                self._krx_trading_days[market_date] = False
                self._krx_calendar_sources[market_date] = "configured_override"
                self._krx_calendar_last_checked_at = checked_at
                self._krx_calendar_last_error = None
            return

        try:
            adapter = self._broker_adapter_for_cycle()
            calendar_checker = getattr(adapter, "is_krx_trading_day", None)
            if not callable(calendar_checker):
                raise RuntimeError("broker adapter does not expose the KRX calendar")
            with self._cycle_session_factory() as db:
                broker_session = ensure_authenticated_broker_session(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    source="autonomous_runtime_krx_calendar",
                    reason="resolve the domestic runtime cadence for today's KRX session",
                )
                is_open = calendar_checker(
                    broker_session.access_token or "",
                    market_date=market_date,
                )
            if not isinstance(is_open, bool):
                raise RuntimeError("KIS KRX calendar returned an unknown session state")
        except Exception as exc:
            with self._lock:
                self._krx_calendar_last_checked_at = checked_at
                self._krx_calendar_last_error = str(exc)
            LOGGER.warning(
                "autonomous_runtime_krx_calendar_unavailable",
                extra={"market_date": market_date.isoformat(), "error": str(exc)},
            )
            return

        with self._lock:
            self._krx_trading_days[market_date] = is_open
            self._krx_calendar_sources[market_date] = "kis_chk_holiday"
            self._krx_calendar_last_checked_at = checked_at
            self._krx_calendar_last_error = None

    def _krx_trading_days_snapshot(self) -> dict[date, bool]:
        with self._lock:
            return dict(self._krx_trading_days)

    def _krx_calendar_snapshot(self) -> dict[str, object]:
        market_date = datetime.now(UTC).astimezone(KRX_TIMEZONE).date()
        with self._lock:
            is_open = self._krx_trading_days.get(market_date)
            source = self._krx_calendar_sources.get(market_date)
            attempted_today = market_date in self._krx_calendar_attempts
            last_checked_at = (
                self._krx_calendar_last_checked_at if attempted_today else None
            )
            last_error = self._krx_calendar_last_error if attempted_today else None
        if market_date.weekday() >= 5:
            state = "closed"
            source = "weekend"
        elif market_date in configured_krx_market_holidays(
            self.settings.krx_market_holiday_dates
        ):
            state = "closed"
            source = source or "configured_override"
        elif is_open is True:
            state = "open"
        elif is_open is False:
            state = "closed"
        else:
            state = "unknown"
            source = source or "unknown"
        return {
            "market_date": market_date.isoformat(),
            "state": state,
            "source": source,
            "last_checked_at": _serialize_datetime(last_checked_at),
            "last_error": last_error,
        }

    def _broker_positions_reconciled_on_market_date(self, now: datetime) -> bool:
        reconciled_at = self._monitoring_snapshot_last_reconciled_at
        if reconciled_at is None:
            return False
        return reconciled_at.astimezone(KRX_TIMEZONE).date() == now.astimezone(
            KRX_TIMEZONE
        ).date()

    def _refresh_monitoring_snapshot_filled_today(self, now: datetime) -> None:
        with self._lock:
            snapshot = self._monitoring_runtime_snapshot
        if snapshot is None:
            return
        with self._cycle_session_factory() as db:
            filled_today_symbols = _load_filled_today_symbols(
                settings=self.settings,
                db=db,
                now=now,
            )
        if filled_today_symbols == snapshot.filled_today_symbols:
            return
        with self._lock:
            if self._monitoring_runtime_snapshot is snapshot:
                self._monitoring_runtime_snapshot = replace(
                    snapshot,
                    filled_today_symbols=filled_today_symbols,
                )

    def _reconcile_monitoring_orders_if_due(
        self,
        *,
        now: datetime,
        symbols: set[str],
        force: bool = False,
    ) -> bool:
        if self.settings.runtime_mode != "live":
            return False
        if not symbols:
            return False
        if not force and self._monitoring_order_reconcile_last_at is not None:
            elapsed = (now - self._monitoring_order_reconcile_last_at).total_seconds()
            if elapsed < max(60, self.settings.monitoring_loop_interval_seconds):
                return False
        adapter = self._broker_adapter_for_cycle()
        with self._cycle_session_factory() as db:
            try:
                result = reconcile_pending_broker_orders(
                    settings=self.settings,
                    db=db,
                    adapter=adapter,
                    symbols=symbols,
                )
                if int(result.get("fills_added") or 0) > 0:
                    _sync_positions_for_monitoring_routes(
                        settings=self.settings,
                        db=db,
                        adapter=adapter,
                        symbols=symbols,
                    )
                db.commit()
            except Exception:
                LOGGER.exception(
                    "monitoring_order_reconcile_failed",
                    extra={
                        "runtime_mode": self.settings.runtime_mode,
                        "symbol_count": len(symbols),
                        "force": force,
                    },
                )
                return False
        self._monitoring_order_reconcile_last_at = now
        state_changed = any(
            int(result.get(key) or 0) > 0
            for key in (
                "reconciled_order_count",
                "fills_added",
                "scope_quarantined_order_count",
            )
        )
        if state_changed:
            self.invalidate_dispatch_schedule(
                reason="monitoring_order_reconciliation_changed_state"
            )
            self._refresh_monitoring_runtime_snapshot(
                loaded_at=now,
                reason="monitoring_order_reconciliation_changed_state",
                reconcile_broker=False,
            )
        return state_changed

    def _repair_post_entry_exit_monitors_for_cached_cycle(
        self,
        *,
        runtime_snapshot: MonitoringRuntimeSnapshot,
        prices: list[MonitoringPriceRequest],
        started_at: datetime,
        durably_unquoteable_symbols: set[str] | None = None,
    ) -> tuple[int, int]:
        if not runtime_snapshot.positions_by_symbol:
            return 0, 0
        prices_by_symbol = {price.symbol: price for price in prices}
        adapter = self._broker_adapter_for_cycle()
        krx_trading_day = _krx_trading_day_resolver(
            settings=self.settings,
            adapter=adapter,
            auth_session_factory=self.session_factory,
        )
        with self._cycle_session_factory() as db:
            positions_by_symbol = _load_positions_by_symbol(
                settings=self.settings,
                db=db,
                symbols=set(runtime_snapshot.positions_by_symbol),
                lock_for_update=True,
            )
            maintenance_count, review_count = _maintain_post_entry_exit_monitors(
                settings=self.settings,
                db=db,
                adapter=adapter,
                krx_trading_day=krx_trading_day,
                positions_by_symbol=positions_by_symbol,
                prices=prices_by_symbol,
                triggered_at=started_at,
                durably_unquoteable_symbols=durably_unquoteable_symbols,
            )
            if maintenance_count or review_count:
                db.commit()
            return maintenance_count, review_count

    def _load_monitoring_runtime_snapshot(self, loaded_at: datetime) -> MonitoringRuntimeSnapshot:
        desired_symbol_priority = _load_core_desired_monitoring_symbols(
            settings=self.settings
        )
        with self._cycle_session_factory() as db:
            symbols = list_autonomous_monitoring_symbols(
                settings=self.settings,
                db=db,
                effective_at=loaded_at,
                desired_symbol_priority=desired_symbol_priority,
                include_kis_us_symbols=self.settings.kis_us_trading_enabled,
            )
            snapshot = self._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=db,
                loaded_at=loaded_at,
                symbols=symbols,
            )
        prime_runtime_context = getattr(
            self.monitoring_price_source,
            "prime_runtime_context",
            None,
        )
        if symbols and callable(prime_runtime_context):
            with self._cycle_session_factory() as db:
                try:
                    prime_runtime_context(
                        db=db,
                        symbols=symbols,
                        loaded_at=loaded_at,
                    )
                except Exception:
                    LOGGER.warning(
                        "monitoring_price_runtime_context_prime_failed",
                        extra={
                            "runtime_mode": self.settings.runtime_mode,
                            "symbol_count": len(symbols),
                        },
                        exc_info=True,
                    )
        return snapshot

    def _load_monitoring_runtime_snapshot_for_symbols(
        self,
        *,
        loaded_at: datetime,
        symbols: list[str],
    ) -> MonitoringRuntimeSnapshot:
        raw_snapshot = self._load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
            loaded_at=loaded_at,
            symbols=symbols,
        )
        if raw_snapshot is not None:
            return raw_snapshot
        with self._cycle_session_factory() as db:
            return self._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=db,
                loaded_at=loaded_at,
                symbols=symbols,
            )

    def _load_monitoring_runtime_snapshot_for_symbols_from_sqlite(
        self,
        *,
        loaded_at: datetime,
        symbols: list[str],
    ) -> MonitoringRuntimeSnapshot | None:
        try:
            url = make_url(self.settings.database_url)
        except Exception:
            return None
        if not url.drivername.startswith("sqlite") or not url.database or url.database == ":memory:":
            return None
        if not symbols:
            return MonitoringRuntimeSnapshot(
                loaded_at=loaded_at,
                symbols=[],
                positions_by_symbol={},
                items=[],
                filled_today_symbols=[],
                execution_scope=_active_kis_execution_scope_payload(self.settings),
            )
        symbol_placeholders = ",".join("?" for _ in symbols)
        watch_status_placeholders = ",".join("?" for _ in ACTIVE_WATCH_MONITORING_STATUSES)
        watch_intent_placeholders = ",".join("?" for _ in WATCH_BUY_INTENTS)
        sell_statuses = ACTIVE_SELL_MONITORING_STATUSES
        sell_status_placeholders = ",".join("?" for _ in sell_statuses)
        sell_intent_placeholders = ",".join("?" for _ in SELL_MONITORED_INTENTS)
        active_scope = _active_kis_position_scope(self.settings)
        try:
            with sqlite3.connect(f"file:{url.database}?mode=ro", uri=True, timeout=5) as conn:
                position_rows = (
                    conn.execute(
                        f"""
                        select symbol, quantity, exchange, provider, raw_payload
                        from execution_positions
                        where mode = ?
                          and provider = ?
                          and account_scope = ?
                          and exchange in ('KRX', 'NASD', 'NYSE', 'AMEX')
                          and quantity > 0
                          and symbol in ({symbol_placeholders})
                        """,
                        [
                            self.settings.runtime_mode,
                            active_scope[0],
                            active_scope[1],
                            *symbols,
                        ],
                    ).fetchall()
                    if active_scope is not None
                    else []
                )
                buy_item_rows = conn.execute(
                    f"""
                    select i.id, i.symbol, i.intent, i.status, i.trigger_price,
                           i.risk_context, i.quantity_policy, i.provider
                    from execution_plan_items i
                    join execution_plans p on p.id = i.execution_plan_id
                    where p.mode = ?
                      and p.market_date <= ?
                      and i.side = 'buy'
                      and i.status in ({watch_status_placeholders})
                      and i.intent in ({watch_intent_placeholders})
                      and i.symbol in ({symbol_placeholders})
                    """,
                    [
                        self.settings.runtime_mode,
                        loaded_at.astimezone(KRX_TIMEZONE).date().isoformat(),
                        *ACTIVE_WATCH_MONITORING_STATUSES,
                        *WATCH_BUY_INTENTS,
                        *symbols,
                    ],
                ).fetchall()
                sell_item_rows = conn.execute(
                    f"""
                    select i.id, i.symbol, i.intent, i.status, i.trigger_price,
                           i.risk_context, i.quantity_policy, i.provider
                    from execution_plan_items i
                    join execution_plans p on p.id = i.execution_plan_id
                    where p.mode = ?
                      and i.side = 'sell'
                      and i.status in ({sell_status_placeholders})
                      and i.intent in ({sell_intent_placeholders})
                      and i.symbol in ({symbol_placeholders})
                    """,
                    [
                        self.settings.runtime_mode,
                        *sell_statuses,
                        *SELL_MONITORED_INTENTS,
                        *symbols,
                    ],
                ).fetchall()
        except sqlite3.Error:
            LOGGER.exception(
                "monitoring_sqlite_symbol_snapshot_failed",
                extra={"runtime_mode": self.settings.runtime_mode, "symbol_count": len(symbols)},
            )
            return None
        scoped_item_rows = [
            row
            for row in [*buy_item_rows, *sell_item_rows]
            if _cached_item_matches_active_kis_route(
                settings=self.settings,
                symbol=str(row[1]),
                provider=row[7],
                risk_context=row[5],
                quantity_policy=row[6],
            )
        ]
        cached_items = [
            CachedMonitoringItem(
                id=str(row[0]),
                symbol=str(row[1]),
                intent=str(row[2]),
                status=str(row[3]),
                trigger_price=Decimal(str(row[4])) if row[4] is not None else None,
                trigger_direction=(
                    watch_buy_direction_from_payloads(
                        _cached_json_object(row[6]),
                        _cached_json_object(row[5]),
                    )
                    if str(row[2]) in WATCH_BUY_INTENTS
                    else None
                ),
                close_exit_date=_cached_close_exit_date(row[5]),
                provider=str(row[7] or "").strip().lower() or None,
                exchange=_cached_item_exchange(
                    symbol=str(row[1]),
                    risk_context=row[5],
                ),
            )
            for row in scoped_item_rows
        ]
        return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=symbols,
            positions_by_symbol=_cached_positions_by_symbol(position_rows),
            items=cached_items,
            filled_today_symbols=[],
            execution_scope=_active_kis_execution_scope_payload(self.settings),
        )

    def _load_local_desired_monitoring_symbols_from_sqlite(self) -> list[str]:
        try:
            url = make_url(self.settings.database_url)
        except Exception:
            return []
        if not url.drivername.startswith("sqlite") or not url.database or url.database == ":memory:":
            return []
        # The ORM path performs provider/asset/exchange-aware routing for KIS
        # overseas symbols. The lightweight SQL path intentionally remains
        # KRX-only so it cannot absorb Alpaca positions from a shared database.
        if self.settings.kis_us_trading_enabled:
            return []
        buy_statuses = tuple(ACTIVE_WATCH_MONITORING_STATUSES)
        buy_status_placeholders = ",".join("?" for _ in buy_statuses)
        buy_intent_placeholders = ",".join("?" for _ in WATCH_BUY_INTENTS)
        active_scope = _active_kis_position_scope(self.settings)
        try:
            with sqlite3.connect(f"file:{url.database}?mode=ro", uri=True, timeout=5) as conn:
                position_rows = (
                    conn.execute(
                        """
                        select symbol
                        from execution_positions
                        where mode = ?
                          and provider = ?
                          and account_scope = ?
                          and exchange = 'KRX'
                          and quantity > 0
                        order by symbol asc
                        """,
                        [self.settings.runtime_mode, *active_scope],
                    ).fetchall()
                    if active_scope is not None
                    else []
                )
                item_rows = conn.execute(
                    f"""
                    select
                        i.symbol,
                        p.market_date,
                        p.accepted_at,
                        i.updated_at,
                        i.risk_context,
                        i.quantity_policy,
                        i.provider
                    from execution_plan_items i
                    join execution_plans p on p.id = i.execution_plan_id
                    where p.mode = ?
                      and p.market_date <= ?
                      and i.side = 'buy'
                      and i.status in ({buy_status_placeholders})
                      and i.intent in ({buy_intent_placeholders})
                    order by p.market_date desc, p.accepted_at desc, i.updated_at desc, i.symbol asc
                    """,
                    [
                        self.settings.runtime_mode,
                        datetime.now(KRX_TIMEZONE).date().isoformat(),
                        *buy_statuses,
                        *WATCH_BUY_INTENTS,
                    ],
                ).fetchall()
        except sqlite3.Error:
            LOGGER.exception(
                "monitoring_sqlite_local_symbol_fallback_failed",
                extra={"runtime_mode": self.settings.runtime_mode},
            )
            return []
        ordered: list[str] = []
        seen: set[str] = set()
        for row in position_rows:
            symbol = str(row[0] or "").strip()
            if not _is_krx_symbol(symbol) or symbol in seen:
                continue
            ordered.append(symbol)
            seen.add(symbol)
        for row in item_rows:
            symbol = str(row[0] or "").strip()
            if (
                not _is_krx_symbol(symbol)
                or symbol in seen
                or not _cached_item_matches_active_kis_route(
                    settings=self.settings,
                    symbol=symbol,
                    provider=row[6],
                    risk_context=row[4],
                    quantity_policy=row[5],
                )
                or _cached_item_exchange(symbol=symbol, risk_context=row[4]) != "KRX"
            ):
                continue
            ordered.append(symbol)
            seen.add(symbol)
        return ordered

    def _load_local_position_symbols_from_sqlite(self) -> list[str]:
        try:
            url = make_url(self.settings.database_url)
        except Exception:
            return []
        if not url.drivername.startswith("sqlite") or not url.database or url.database == ":memory:":
            return []
        active_scope = _active_kis_position_scope(self.settings)
        if active_scope is None:
            return []
        try:
            with sqlite3.connect(f"file:{url.database}?mode=ro", uri=True, timeout=5) as conn:
                rows = conn.execute(
                    """
                    select symbol
                    from execution_positions
                    where mode = ?
                      and provider = ?
                      and account_scope = ?
                      and exchange in ('KRX', 'NASD', 'NYSE', 'AMEX')
                      and quantity > 0
                    group by symbol
                    having count(*) = 1
                    order by symbol asc
                    """,
                    [self.settings.runtime_mode, *active_scope],
                ).fetchall()
        except sqlite3.Error:
            LOGGER.exception(
                "monitoring_sqlite_position_symbol_fallback_failed",
                extra={"runtime_mode": self.settings.runtime_mode},
            )
            return []
        return [str(row[0]).strip() for row in rows if str(row[0] or "").strip()]

    def _load_local_position_symbols(self) -> list[str]:
        sqlite_symbols = self._load_local_position_symbols_from_sqlite()
        if sqlite_symbols:
            return sqlite_symbols
        if self.session_factory is None:
            return []
        with self._cycle_session_factory() as db:
            rows = db.execute(
                select(ExecutionPosition.symbol)
                .where(
                    *_active_kis_position_filters(settings=self.settings),
                    ExecutionPosition.quantity > 0,
                )
                .group_by(ExecutionPosition.symbol)
                .having(func.count(ExecutionPosition.id) == 1)
                .order_by(ExecutionPosition.symbol.asc())
            ).scalars()
            return [str(symbol).strip() for symbol in rows if str(symbol or "").strip()]

    def _load_local_desired_monitoring_symbols(self) -> list[str]:
        sqlite_symbols = self._load_local_desired_monitoring_symbols_from_sqlite()
        if sqlite_symbols:
            return sqlite_symbols
        with self._cycle_session_factory() as db:
            return list_autonomous_monitoring_symbols(
                settings=self.settings,
                db=db,
            )

    def _load_monitoring_runtime_snapshot_for_symbols_with_session(
        self,
        *,
        db: Session,
        loaded_at: datetime,
        symbols: list[str],
    ) -> MonitoringRuntimeSnapshot:
        position_rows = db.execute(
            select(
                ExecutionPosition.symbol,
                ExecutionPosition.quantity,
                ExecutionPosition.exchange,
                ExecutionPosition.provider,
                ExecutionPosition.raw_payload,
            ).where(
                *_active_kis_position_filters(settings=self.settings),
                ExecutionPosition.quantity > 0,
                ExecutionPosition.symbol.in_(symbols or [""]),
            )
        ).all()
        buy_item_rows = db.execute(
            select(
                ExecutionPlanItem.id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.provider,
            )
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == self.settings.runtime_mode,
                ExecutionPlan.market_date
                <= loaded_at.astimezone(KRX_TIMEZONE).date(),
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.status.in_(
                    tuple(ACTIVE_WATCH_MONITORING_STATUSES)
                ),
                ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                ExecutionPlanItem.symbol.in_(symbols or [""]),
            )
        ).all()
        sell_item_rows = db.execute(
            select(
                ExecutionPlanItem.id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.provider,
            )
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == self.settings.runtime_mode,
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.status.in_(
                    tuple(ACTIVE_SELL_MONITORING_STATUSES)
                ),
                ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                ExecutionPlanItem.symbol.in_(symbols or [""]),
            )
        ).all()
        item_rows = [
            item
            for item in [*buy_item_rows, *sell_item_rows]
            if _cached_item_matches_active_kis_route(
                settings=self.settings,
                symbol=str(item.symbol),
                provider=item.provider,
                risk_context=item.risk_context,
                quantity_policy=item.quantity_policy,
            )
        ]
        cached_items = [
            CachedMonitoringItem(
                id=str(item.id),
                symbol=str(item.symbol),
                intent=str(item.intent),
                status=str(item.status),
                trigger_price=item.trigger_price,
                trigger_direction=(
                    watch_buy_direction_from_payloads(
                        item.quantity_policy,
                        item.risk_context,
                    )
                    if str(item.intent) in WATCH_BUY_INTENTS
                    else None
                ),
                close_exit_date=_cached_close_exit_date(item.risk_context),
                provider=(
                    str(item.provider).strip().lower()
                    if item.provider is not None
                    else None
                ),
                exchange=_cached_item_exchange(
                    symbol=str(item.symbol),
                    risk_context=item.risk_context,
                ),
            )
            for item in item_rows
        ]
        return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=symbols,
            positions_by_symbol=_cached_positions_by_symbol(position_rows),
            items=cached_items,
            filled_today_symbols=_load_filled_today_symbols(
                settings=self.settings,
                db=db,
                now=loaded_at,
            ),
            execution_scope=_active_kis_execution_scope_payload(self.settings),
        )

    def _evaluate_cached_monitoring_cycle(
        self,
        *,
        runtime_snapshot: MonitoringRuntimeSnapshot,
        prices: list[MonitoringPriceRequest],
        started_at: datetime,
    ) -> dict[str, object]:
        prices_by_symbol = {price.symbol: price for price in prices}
        evaluated_item_count = 0
        triggered_item_count = 0
        blocked_item_count = 0
        evaluated_symbols: set[str] = set()
        triggered_symbols: set[str] = set()
        triggered_sell_symbols: set[str] = set()
        triggered_buy_symbols: set[str] = set()
        rearmable_sell_symbols: set[str] = set()
        results: list[dict[str, object]] = []
        for item in runtime_snapshot.items:
            position = runtime_snapshot.positions_by_symbol.get(item.symbol)
            if position is not None and not position.monitoring_eligible:
                continue
            if (
                item.intent not in WATCH_BUY_INTENTS
                and (
                    position is None
                    or not _cached_item_matches_position_route(
                        item=item,
                        position=position,
                    )
                )
            ):
                continue
            price = prices_by_symbol.get(item.symbol)
            observed_price = _cached_executable_price(
                price=price,
                item=item,
                runtime_mode=self.settings.runtime_mode,
            )
            if price is None and item.intent != "max_holding_close_sell":
                continue
            evaluated_item_count += 1
            evaluated_symbols.add(item.symbol)
            triggered = _cached_monitoring_item_triggered(
                item=item,
                observed_price=observed_price,
                triggered_at=started_at,
            )
            if item.status in AGENT_ACTIVE_MONITORING_STATUSES:
                blocked_item_count += 1
                outcome = "agent_pending"
            elif triggered:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                if item.intent in WATCH_BUY_INTENTS:
                    triggered_buy_symbols.add(item.symbol)
                else:
                    triggered_sell_symbols.add(item.symbol)
                outcome = "triggered"
            else:
                outcome = "waiting"
                quote_rejection_reason = None
                if item.intent in {"stop_sell", "target_sell"}:
                    quote_rejection_reason = _quote_stale_reason(
                        settings=self.settings,
                        price=price,
                        cycle_at=started_at,
                    ) or _live_automated_quote_trust_reason_for_side(
                        settings=self.settings,
                        provider=item.provider or "kis",
                        side="sell",
                        price=price,
                    )
                if (
                    item.intent in {"stop_sell", "target_sell"}
                    and observed_price is not None
                    and quote_rejection_reason is None
                    and item.status
                    in (
                        SELL_TRIGGERED_BLOCKED_STATUSES
                        | SELL_TRIGGERED_PENDING_DISPATCH_STATUSES
                    )
                ):
                    rearmable_sell_symbols.add(item.symbol)
            results.append(
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "intent": item.intent,
                    "status": item.status,
                    "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
                    "observed_price": str(observed_price) if observed_price is not None else None,
                    "outcome": outcome,
                }
            )
        return {
            "triggered_at": started_at.isoformat(),
            "worker_name": MONITORING_LOOP_WORKER_NAME,
            "evaluated_item_count": evaluated_item_count,
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": 0,
            "blocked_item_count": blocked_item_count,
            "deferred_item_count": 0,
            "evaluated_symbol_count": len(evaluated_symbols),
            "triggered_symbol_count": len(triggered_symbols),
            "triggered_symbols": sorted(triggered_symbols),
            "triggered_sell_symbols": sorted(triggered_sell_symbols),
            "triggered_buy_symbols": sorted(triggered_buy_symbols),
            "rearmable_sell_symbols": sorted(rearmable_sell_symbols),
            "submitted_symbols": [],
            "protective_stop_coverage": {
                "status": "cached",
                "unprotected_position_count": 0,
                "position_truth_stale_count": 0,
                "repaired_stop_coverage_count": 0,
                "affected_symbols": [],
                "affected_symbols_count": 0,
                "affected_symbols_truncated": False,
                "missing_quantities": {},
                "issues": [],
                "issues_count": 0,
                "issues_truncated": False,
            },
            "items": results[:MAX_STATUS_DETAIL_ITEMS],
            "items_count": len(results),
            "items_truncated": len(results) > MAX_STATUS_DETAIL_ITEMS,
            "cached_evaluation": True,
        }

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
                diagnostics=_price_source_diagnostics(source),
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
            diagnostics=_price_source_diagnostics(source),
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
            state.last_heartbeat_at = started_at
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
            state.last_cycle = self._scope_cycle_payload(payload)

    def _mark_cycle_busy(
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
            state.last_error = str(payload.get("reason") or "cycle skipped")
            state.last_cycle = self._scope_cycle_payload(payload)

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
            state.last_cycle = self._scope_cycle_payload(payload)

    def _scope_cycle_payload(
        self,
        payload: dict[str, object],
    ) -> dict[str, object]:
        return {
            **payload,
            "execution_scope": _active_kis_execution_scope_payload(self.settings),
        }

    def _state_for(self, worker_key: str) -> AutonomousWorkerState:
        if worker_key == "dispatch":
            return self._dispatch_state
        if worker_key == "watch_exec_agent_sizing":
            return self._watch_exec_agent_sizing_state
        if worker_key == "nxt_eligible_sync":
            return self._nxt_eligible_sync_state
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
            state.diagnostics = {}

    def _mark_monitoring_price_source_result(
        self,
        *,
        checked_at: datetime,
        symbol_count: int,
        source_name: str | None,
        provider: str | None,
        price_count: int,
        diagnostics: dict[str, object],
    ) -> None:
        with self._lock:
            state = self._monitoring_price_source_state
            state.configured = True
            state.source_name = source_name
            state.provider = provider
            state.last_checked_at = checked_at
            state.last_symbol_count = symbol_count
            state.last_price_count = price_count
            state.diagnostics = diagnostics
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
                quote_error = _first_quote_failure_error(diagnostics)
                if quote_error is not None:
                    state.reason = "monitoring price source request failed"
                    state.last_error = quote_error
                    state.last_error_at = checked_at
                else:
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
        diagnostics: dict[str, object],
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
            state.diagnostics = diagnostics


def _call_price_source(
    *,
    source: Callable[..., object],
    symbols: list[str],
    observed_at: datetime,
    settings: Settings,
) -> object:
    call_candidates = (
        ((), {"symbols": symbols, "observed_at": observed_at, "settings": settings}),
        ((symbols, observed_at, settings), {}),
        ((symbols,), {}),
    )
    try:
        source_signature = inspect.signature(source)
    except (TypeError, ValueError):
        args, kwargs = call_candidates[0]
        return source(*args, **kwargs)

    for args, kwargs in call_candidates:
        try:
            source_signature.bind(*args, **kwargs)
        except TypeError:
            continue
        return source(*args, **kwargs)

    raise TypeError(
        "monitoring_price_source must accept (symbols, observed_at, settings) "
        "or the legacy (symbols) signature"
    )


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


def _price_source_diagnostics(source: object) -> dict[str, object]:
    get_last_diagnostics = getattr(source, "get_last_diagnostics", None)
    if get_last_diagnostics is None:
        return {}
    diagnostics = get_last_diagnostics()
    if not isinstance(diagnostics, dict):
        return {}
    return dict(diagnostics)


def _durably_unquoteable_symbols(source: object | None) -> set[str]:
    if source is None:
        return set()
    diagnostics = _price_source_diagnostics(source)
    raw_symbols = diagnostics.get("durably_unquoteable_symbols")
    if not isinstance(raw_symbols, list):
        return set()
    return {
        str(symbol).strip().upper()
        for symbol in raw_symbols
        if str(symbol).strip()
    }


def _first_quote_failure_error(diagnostics: dict[str, object]) -> str | None:
    failures = diagnostics.get("quote_failures")
    if not isinstance(failures, list):
        return None
    for failure in failures:
        if not isinstance(failure, dict):
            continue
        if failure.get("code") == KIS_OVERSEAS_QUOTE_EMPTY_CODE:
            # A successful KIS response with no price rows means this symbol is
            # currently unquoteable. It must stay fail-closed, but it is not a
            # transport/source outage and should not poison global readiness as
            # one.
            continue
        error = failure.get("error")
        if isinstance(error, str) and error.strip():
            return error
    source_failure_count = diagnostics.get("quote_source_failure_count")
    if isinstance(source_failure_count, int) and source_failure_count > 0:
        return "monitoring quote source request failures were truncated"
    return None


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


def _dispatch_payload_changed_runtime_state(payload: dict[str, object]) -> bool:
    for key in ("completed_job_count", "retrying_job_count", "failed_job_count"):
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return True
    jobs = payload.get("jobs")
    return isinstance(jobs, list) and bool(jobs)


def _cached_monitoring_item_triggered(
    *,
    item: CachedMonitoringItem,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> bool:
    if item.intent == "max_holding_close_sell":
        exchange = str(item.exchange or "").strip().upper()
        if exchange in KRX_EXCHANGES:
            market_timezone = KRX_TIMEZONE
        elif exchange in {"US", "NASD", "NYSE", "AMEX"}:
            market_timezone = ZoneInfo("America/New_York")
        else:
            market_timezone = UTC
        return bool(
            item.close_exit_date is not None
            and triggered_at.astimezone(market_timezone).date() >= item.close_exit_date
        )
    if item.intent in WATCH_BUY_INTENTS:
        trigger_price = _cached_watch_buy_trigger_price(item)
        if trigger_price is None or observed_price is None:
            return False
        if _cached_watch_buy_direction(item) == "at_or_below":
            return observed_price <= trigger_price
        return observed_price >= trigger_price
    if item.trigger_price is None or observed_price is None:
        return False
    if item.intent == "target_sell":
        return observed_price >= item.trigger_price
    if item.intent == "stop_sell":
        return observed_price <= item.trigger_price
    return False


def _cached_watch_buy_trigger_price(item: CachedMonitoringItem) -> Decimal | None:
    return item.trigger_price


def _cached_watch_buy_direction(item: CachedMonitoringItem) -> str:
    return item.trigger_direction or "at_or_below"


def _cached_executable_price(
    *,
    price: MonitoringPriceRequest | None,
    item: CachedMonitoringItem,
    runtime_mode: str,
) -> Decimal | None:
    if price is None:
        return None
    executable = price.ask if item.intent in WATCH_BUY_INTENTS else price.bid
    if executable is not None and executable.is_finite() and executable > 0:
        return executable
    if runtime_mode == "live":
        return None
    return price.price


def _cached_json_object(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _cached_close_exit_date(risk_context: object) -> date | None:
    raw_close_exit_date = _cached_json_object(risk_context).get("close_exit_date")
    if not isinstance(raw_close_exit_date, str):
        return None
    try:
        return datetime.strptime(raw_close_exit_date, "%Y-%m-%d").date()
    except ValueError:
        return None


def _triggered_symbols_from_cached_payload(
    payload: dict[str, object],
    *,
    include_watch_buys: bool = True,
    include_sells: bool = True,
) -> set[str]:
    field_name = None
    if include_watch_buys and not include_sells:
        field_name = "triggered_buy_symbols"
    elif include_sells and not include_watch_buys:
        field_name = "triggered_sell_symbols"
    elif include_sells and include_watch_buys:
        field_name = "triggered_symbols"
    if field_name is not None:
        raw_symbols = payload.get(field_name)
        if isinstance(raw_symbols, list):
            return {symbol for symbol in raw_symbols if isinstance(symbol, str) and symbol}

    items = payload.get("items")
    if not isinstance(items, list):
        return set()
    symbols: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("outcome") != "triggered":
            continue
        intent = item.get("intent")
        is_watch_buy = isinstance(intent, str) and intent in WATCH_BUY_INTENTS
        if is_watch_buy and not include_watch_buys:
            continue
        if not is_watch_buy and not include_sells:
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            symbols.add(symbol)
    return symbols


def _submitted_symbols_from_monitoring_payload(
    payload: dict[str, object],
) -> set[str]:
    raw_symbols = payload.get("submitted_symbols")
    if not isinstance(raw_symbols, list):
        return set()
    return {
        symbol
        for symbol in raw_symbols
        if isinstance(symbol, str) and symbol
    }


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _domestic_only_live_runtime(settings: Settings) -> bool:
    return bool(
        settings.runtime_mode == "live"
        and settings.runtime_broker_provider == "kis"
        and not settings.kis_us_trading_enabled
    )


def _next_enabled_domestic_session_start(
    *,
    settings: Settings,
    now: datetime,
    trading_days: dict[date, bool] | None = None,
) -> datetime | None:
    local_now = now.astimezone(KRX_TIMEZONE)
    holidays = configured_krx_market_holidays(settings.krx_market_holiday_dates)
    authoritative_trading_days = trading_days or {}
    if _is_enabled_domestic_session(
        settings=settings,
        local_now=local_now,
        holidays=holidays,
        trading_days=authoritative_trading_days,
    ):
        return None

    for day_offset in range(8):
        market_date = local_now.date() + timedelta(days=day_offset)
        if (
            market_date.weekday() >= 5
            or market_date in holidays
            or authoritative_trading_days.get(market_date) is False
        ):
            continue
        for boundary in DOMESTIC_SESSION_BOUNDARIES:
            candidate = datetime.combine(
                market_date,
                boundary,
                tzinfo=KRX_TIMEZONE,
            )
            if candidate <= local_now:
                continue
            if domestic_session_has_enabled_route(
                settings=settings,
                session=domestic_market_session(candidate),
            ):
                return candidate.astimezone(UTC)
    # The eight-day search always contains a weekday. If an operator marks
    # every candidate as a configured holiday, keep the fast cadence rather
    # than risk sleeping through an unknown reopening.
    return None


def _is_enabled_domestic_session(
    *,
    settings: Settings,
    local_now: datetime,
    holidays: set[date],
    trading_days: dict[date, bool],
) -> bool:
    if (
        local_now.weekday() >= 5
        or local_now.date() in holidays
        or trading_days.get(local_now.date()) is False
    ):
        return False
    return domestic_session_has_enabled_route(
        settings=settings,
        session=domestic_market_session(local_now),
    )


def _is_regular_market_session(*, now: datetime, market_timezone: str) -> bool:
    try:
        local_now = now.astimezone(ZoneInfo(market_timezone))
    except Exception:
        local_now = now.astimezone(ZoneInfo("Asia/Seoul"))
    if local_now.weekday() >= 5:
        return False
    return time(9, 0) <= local_now.time() <= time(15, 30)


def _load_filled_today_symbols(
    *,
    settings: Settings,
    db: Session,
    now: datetime,
) -> list[str]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds(now)
    # Import lazily to avoid a module cycle while sharing the exact canonical
    # execution-scope classifier used by runtime status.
    from app.services.runtime import runtime_filled_today_symbols

    return runtime_filled_today_symbols(
        settings=settings,
        db=db,
        provider="kis",
        today_start_utc=today_start_utc,
        tomorrow_start_utc=tomorrow_start_utc,
    )


def _current_krx_day_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current_krx = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    start_krx = datetime.combine(current_krx.date(), time.min, tzinfo=KRX_TIMEZONE)
    end_krx = datetime.combine(current_krx.date(), time.max, tzinfo=KRX_TIMEZONE)
    return start_krx.astimezone(UTC), end_krx.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _payload_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _coerce_utc_datetime(value)
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _coerce_utc_datetime(parsed)


def _coerce_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _merge_symbol_priority(*symbol_groups: Iterable[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for symbols in symbol_groups:
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol or symbol in seen:
                continue
            ordered.append(symbol)
            seen.add(symbol)
    return ordered


def _monitoring_snapshot_for_symbols(
    snapshot: MonitoringRuntimeSnapshot,
    *,
    symbols: list[str],
) -> MonitoringRuntimeSnapshot:
    selected_symbols = list(dict.fromkeys(symbols))
    selected_symbol_set = set(selected_symbols)
    return replace(
        snapshot,
        symbols=selected_symbols,
        positions_by_symbol={
            symbol: position
            for symbol, position in snapshot.positions_by_symbol.items()
            if symbol in selected_symbol_set
        },
        items=[
            item for item in snapshot.items if item.symbol in selected_symbol_set
        ],
    )


def _session_active_monitoring_symbols(
    snapshot: MonitoringRuntimeSnapshot,
    *,
    now: datetime,
) -> tuple[list[str], list[str]]:
    """Exclude US equities from quote polling outside the US regular session."""
    observed_at = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    if is_us_regular_session(observed_at.astimezone(US_EASTERN_TIMEZONE)):
        return list(snapshot.symbols), []

    exchanges_by_symbol = {
        symbol: str(position.exchange or "").strip().upper()
        for symbol, position in snapshot.positions_by_symbol.items()
        if position.exchange
    }
    for item in snapshot.items:
        if item.exchange and item.symbol not in exchanges_by_symbol:
            exchanges_by_symbol[item.symbol] = str(item.exchange).strip().upper()

    active: list[str] = []
    inactive: list[str] = []
    for symbol in snapshot.symbols:
        if exchanges_by_symbol.get(symbol) in US_EQUITY_EXCHANGES:
            inactive.append(symbol)
        else:
            active.append(symbol)
    return active, inactive


def _is_krx_symbol(symbol: str) -> bool:
    normalized = str(symbol or "").strip()
    return len(normalized) == 6 and normalized.isdigit()


def _select_monitoring_symbol_batch(
    *,
    desired_symbols: list[str],
    priority_symbols: set[str],
    batch_size: int,
    cursor: int,
) -> tuple[list[str], int]:
    limit = max(int(batch_size), 1)
    priority = [symbol for symbol in desired_symbols if symbol in priority_symbols]
    rotating = [symbol for symbol in desired_symbols if symbol not in priority_symbols]
    selected = list(priority)
    if not rotating:
        return selected, cursor % max(len(rotating), 1)
    start = cursor % len(rotating)
    take = min(limit, len(rotating))
    selected.extend(rotating[(start + offset) % len(rotating)] for offset in range(take))
    return selected, (start + take) % len(rotating)


def _compact_cycle_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    if payload is None:
        return None
    compacted = dict(payload)
    coverage = compacted.get("protective_stop_coverage")
    if isinstance(coverage, dict):
        compacted["protective_stop_coverage"] = _compact_detail_lists(coverage)
    target_coverage = compacted.get("profit_target_coverage")
    if isinstance(target_coverage, dict):
        compacted["profit_target_coverage"] = _compact_detail_lists(target_coverage)
    for key in ("items", "jobs"):
        value = compacted.get(key)
        if isinstance(value, list) and len(value) > MAX_STATUS_DETAIL_ITEMS:
            compacted[f"{key}_count"] = len(value)
            compacted[f"{key}_truncated"] = True
            compacted[key] = value[:MAX_STATUS_DETAIL_ITEMS]
    return compacted


def _compact_detail_lists(payload: dict[str, object]) -> dict[str, object]:
    compacted = dict(payload)
    for key in ("affected_symbols", "issues"):
        value = compacted.get(key)
        if isinstance(value, list) and len(value) > MAX_STATUS_DETAIL_ITEMS:
            compacted[f"{key}_count"] = len(value)
            compacted[f"{key}_truncated"] = True
            compacted[key] = value[:MAX_STATUS_DETAIL_ITEMS]
    return compacted
