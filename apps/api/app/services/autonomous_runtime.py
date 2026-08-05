from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import distinct, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionFill
from app.models.runtime import ExecutionPlanItem, ExecutionPosition
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.monitoring import (
    BLOCKED_MONITORING_STATUSES,
    PENDING_DISPATCH_MONITORING_STATUSES,
    SELL_ACTIVE_MONITORING_STATUSES,
    SELL_MONITORED_INTENTS,
    WATCH_BUY_INTENTS,
    WATCH_MONITORING_STATUSES,
    _load_positions_by_symbol,
    _maintain_post_entry_exit_monitors,
    list_autonomous_monitoring_symbols,
    run_autonomous_monitoring_cycle,
    _load_core_desired_monitoring_symbols,
)
from app.services.nxt_eligibility import refresh_nxt_eligible_symbols
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot
from app.services.watch_exec_agent_sizing import run_due_watch_exec_agent_sizing

LOGGER = logging.getLogger(__name__)

DISPATCH_LOOP_WORKER_NAME = "execution_job_dispatch_loop"
MONITORING_LOOP_WORKER_NAME = "monitoring_dispatch_loop"
WATCH_EXEC_AGENT_SIZING_WORKER_NAME = "watch_exec_agent_sizing_loop"
NXT_ELIGIBLE_SYNC_WORKER_NAME = "nxt_eligible_sync_loop"
MAX_STATUS_DETAIL_ITEMS = 20
ACTIONABLE_CACHED_WATCH_STATUSES = frozenset(
    {
        "watch_monitoring_active",
        "agent_delay",
        "watch_buy_triggered_pending_dispatch",
    }
)
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")


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
            "last_cycle": _compact_cycle_payload(self.last_cycle),
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


@dataclass(frozen=True)
class CachedMonitoringItem:
    id: str
    symbol: str
    intent: str
    status: str
    trigger_price: Decimal | None
    trigger_direction: str | None = None
    chase_limit_price: Decimal | None = None


@dataclass(frozen=True)
class MonitoringRuntimeSnapshot:
    loaded_at: datetime
    symbols: list[str]
    positions_by_symbol: dict[str, CachedMonitoringPosition]
    items: list[CachedMonitoringItem]
    filled_today_symbols: list[str]

    def snapshot(self) -> dict[str, object]:
        return {
            "loaded_at": _serialize_datetime(self.loaded_at),
            "symbols": list(self.symbols),
            "symbol_count": len(self.symbols),
            "item_count": len(self.items),
            "position_count": len(self.positions_by_symbol),
            "filled_today_symbols": list(self.filled_today_symbols),
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
    _nxt_eligible_sync_task: asyncio.Task[None] | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _dispatch_state: AutonomousWorkerState = field(init=False)
    _monitoring_state: AutonomousWorkerState = field(init=False)
    _watch_exec_agent_sizing_state: AutonomousWorkerState = field(init=False)
    _nxt_eligible_sync_state: AutonomousWorkerState = field(init=False)
    _monitoring_price_source_state: MonitoringPriceSourceState = field(init=False)
    _monitoring_runtime_snapshot: MonitoringRuntimeSnapshot | None = field(default=None, init=False)
    _monitoring_snapshot_last_reconciled_at: datetime | None = field(default=None, init=False)
    _monitoring_cycle_mutex: threading.Lock = field(default_factory=threading.Lock, init=False)
    _monitoring_cycle_mutex_started_at: datetime | None = field(default=None, init=False)

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

    async def stop(self) -> None:
        tasks = [
            task
            for task in (
                self._dispatch_task,
                self._monitoring_task,
                self._watch_exec_agent_sizing_task,
                self._nxt_eligible_sync_task,
            )
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
                    "runtime_snapshot": (
                        self._monitoring_runtime_snapshot.snapshot()
                        if self._monitoring_runtime_snapshot is not None
                        else None
                    ),
                },
                "watch_exec_agent_sizing": self._watch_exec_agent_sizing_state.snapshot(),
                "nxt_eligible_sync": self._nxt_eligible_sync_state.snapshot(),
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

    async def _await_cycle_payload(
        self,
        *,
        worker_key: str,
        awaitable: asyncio.Future | object,
    ) -> dict[str, object] | None:
        if worker_key != "monitoring":
            return await awaitable

        task = asyncio.ensure_future(awaitable)
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
            if _dispatch_payload_changed_runtime_state(payload):
                self._refresh_monitoring_runtime_snapshot(
                    loaded_at=started_at,
                    reason="dispatch_cycle_changed_runtime_state",
                    reconcile_broker=False,
                )
            self._store_cycle_payload(worker_key="dispatch", payload=payload)
            return payload

    async def _run_monitoring_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._monitoring_cycle_sync_guarded, started_at)

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
        return await asyncio.to_thread(self._watch_exec_agent_sizing_cycle_sync, started_at)

    async def _run_nxt_eligible_sync_cycle(self, started_at: datetime) -> dict[str, object]:
        return await asyncio.to_thread(self._nxt_eligible_sync_cycle_sync, started_at)

    def _watch_exec_agent_sizing_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        adapter = self.broker_adapter_factory(self.settings)
        result = run_due_watch_exec_agent_sizing(
            settings=self.settings,
            session_factory=self.session_factory,
            adapter=adapter,
            now=started_at,
        )
        payload = {
            "status_code": result.status_code,
            **result.payload,
        }
        self._store_cycle_payload(worker_key="watch_exec_agent_sizing", payload=payload)
        return payload

    def _nxt_eligible_sync_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        payload = refresh_nxt_eligible_symbols(settings=self.settings, now=started_at, force=True)
        self._store_cycle_payload(worker_key="nxt_eligible_sync", payload=payload)
        return payload

    def _monitoring_cycle_sync(self, started_at: datetime) -> dict[str, object]:
        desired_symbols = _load_core_desired_monitoring_symbols(settings=self.settings)
        if not desired_symbols:
            desired_symbols = self._load_local_desired_monitoring_symbols_from_sqlite()
        if desired_symbols:
            symbols = desired_symbols[: self.settings.monitoring_max_symbols_per_cycle or len(desired_symbols)]
            if self.settings.monitoring_max_broker_actions_per_cycle <= 0:
                prices = self._load_monitoring_prices(
                    symbols=symbols,
                    observed_at=started_at,
                )
                runtime_snapshot = MonitoringRuntimeSnapshot(
                    loaded_at=started_at,
                    symbols=symbols,
                    positions_by_symbol={},
                    items=[],
                    filled_today_symbols=[],
                )
                payload = {
                    "status_code": 200,
                    "monitored_symbols": symbols,
                    "price_count": len(prices),
                    "price_source_configured": self._monitoring_price_source_state.configured,
                    "price_source": self._monitoring_price_source_state.snapshot(),
                    "runtime_snapshot": runtime_snapshot.snapshot(),
                    "broker_actions_disabled": True,
                    "triggered_at": started_at.isoformat(),
                    "worker_name": MONITORING_LOOP_WORKER_NAME,
                    "evaluated_item_count": 0,
                    "triggered_item_count": 0,
                    "submitted_order_count": 0,
                    "blocked_item_count": 0,
                    "deferred_item_count": 0,
                    "evaluated_symbol_count": len(symbols),
                    "triggered_symbol_count": 0,
                    "submitted_symbols": [],
                    "protective_stop_coverage": {
                        "status": "disabled",
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
                    "profit_target_coverage": {
                        "status": "disabled",
                        "missing_target_coverage_count": 0,
                        "affected_symbols": [],
                        "affected_symbols_count": 0,
                        "affected_symbols_truncated": False,
                        "missing_quantities": {},
                        "issues": [],
                        "issues_count": 0,
                        "issues_truncated": False,
                    },
                    "items": [],
                    "core_desired_symbols_fast_path": True,
                }
                self._store_cycle_payload(worker_key="monitoring", payload=payload)
                with self._lock:
                    self._monitoring_runtime_snapshot = runtime_snapshot
                return payload
            runtime_snapshot = self._load_monitoring_runtime_snapshot_for_symbols(
                loaded_at=started_at,
                symbols=symbols,
            )
            with self._lock:
                self._monitoring_runtime_snapshot = runtime_snapshot
        else:
            runtime_snapshot = self._refresh_monitoring_runtime_snapshot(
                loaded_at=started_at,
                reason="monitoring_cycle_refresh",
                reconcile_broker=False,
            )
        if not desired_symbols:
            self._refresh_monitoring_snapshot_filled_today(started_at)
            runtime_snapshot = self._ensure_monitoring_runtime_snapshot(started_at)
        symbols = runtime_snapshot.symbols
        if not symbols:
            return {
                "status_code": 200,
                "monitored_symbols": [],
                "price_count": 0,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": runtime_snapshot.snapshot(),
                "idle": True,
            }

        prices = self._load_monitoring_prices(
            symbols=symbols,
            observed_at=started_at,
        )
        monitored_symbols = list(symbols)
        monitored_price_count = len(prices)
        cached_payload = self._evaluate_cached_monitoring_cycle(
            runtime_snapshot=runtime_snapshot,
            prices=prices,
            started_at=started_at,
        )
        post_entry_maintenance_count = 0
        if cached_payload["triggered_item_count"] == 0 and prices:
            if not desired_symbols:
                post_entry_maintenance_count = self._repair_post_entry_exit_monitors_for_cached_cycle(
                    runtime_snapshot=runtime_snapshot,
                    prices=prices,
                    started_at=started_at,
                )
                if post_entry_maintenance_count:
                    runtime_snapshot = self._refresh_monitoring_runtime_snapshot(
                        loaded_at=started_at,
                        reason="cached_monitoring_post_entry_maintenance",
                        reconcile_broker=False,
                    )
                    symbols = runtime_snapshot.symbols
                    cached_payload = self._evaluate_cached_monitoring_cycle(
                        runtime_snapshot=runtime_snapshot,
                        prices=prices,
                        started_at=started_at,
                    )
            payload = {
                "status_code": 200,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": runtime_snapshot.snapshot(),
                **cached_payload,
            }
            if post_entry_maintenance_count:
                payload["post_entry_maintenance_count"] = post_entry_maintenance_count
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload

        triggered_symbols = _triggered_symbols_from_cached_payload(cached_payload)
        if cached_payload["triggered_item_count"] > 0 and not triggered_symbols:
            payload = {
                "status_code": 200,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": runtime_snapshot.snapshot(),
                "reason": "cached_triggered_symbols_empty",
                **cached_payload,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload
        if triggered_symbols and self.settings.monitoring_max_broker_actions_per_cycle <= 0:
            payload = {
                "status_code": 200,
                "monitored_symbols": symbols,
                "price_count": len(prices),
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": runtime_snapshot.snapshot(),
                "broker_actions_disabled": True,
                **cached_payload,
            }
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            return payload
        if triggered_symbols:
            broker_action_limit = max(int(self.settings.monitoring_max_broker_actions_per_cycle), 1)
            ordered_triggered_symbols = [
                symbol for symbol in symbols if symbol in triggered_symbols
            ][:broker_action_limit]
            symbols = ordered_triggered_symbols
            selected_prices = [
                price for price in prices if price.symbol in set(ordered_triggered_symbols)
            ]
            if len(prices) > len(ordered_triggered_symbols):
                fresh_action_prices = self._load_monitoring_prices(
                    symbols=ordered_triggered_symbols,
                    observed_at=datetime.now(UTC),
                )
                prices = fresh_action_prices or selected_prices
            else:
                prices = selected_prices
            with self.session_factory() as db:
                adapter = self.broker_adapter_factory(self.settings)
                result = run_autonomous_monitoring_cycle(
                settings=self.settings,
                db=db,
                adapter=adapter,
                prices=prices,
                symbols=symbols,
                now=started_at,
            )
            payload = {
                "status_code": result.status_code,
                "price_source_configured": self._monitoring_price_source_state.configured,
                "price_source": self._monitoring_price_source_state.snapshot(),
                "runtime_snapshot": runtime_snapshot.snapshot(),
                **result.payload,
                "monitored_symbols": monitored_symbols,
                "price_count": monitored_price_count,
                "broker_action_symbols": symbols,
                "broker_action_price_count": len(prices),
            }
            if post_entry_maintenance_count:
                payload["pre_cached_post_entry_maintenance_count"] = post_entry_maintenance_count
            self._store_cycle_payload(worker_key="monitoring", payload=payload)
            if not desired_symbols:
                self._refresh_monitoring_runtime_snapshot(
                    loaded_at=started_at,
                    reason="monitoring_triggered_runtime_state_path",
                    reconcile_broker=False,
                )
            return payload

    def _ensure_monitoring_runtime_snapshot(self, now: datetime) -> MonitoringRuntimeSnapshot:
        if self._monitoring_runtime_snapshot is None:
            self._refresh_monitoring_runtime_snapshot(
                loaded_at=now,
                reason="monitoring_snapshot_missing",
                reconcile_broker=False,
            )
        assert self._monitoring_runtime_snapshot is not None
        return self._monitoring_runtime_snapshot

    def invalidate_monitoring_runtime_snapshot(self, *, reason: str) -> None:
        with self._lock:
            self._monitoring_runtime_snapshot = None
        LOGGER.info(
            "monitoring_runtime_snapshot_invalidated",
            extra={
                "reason": reason,
                "runtime_mode": self.settings.runtime_mode,
            },
        )

    def _refresh_monitoring_runtime_snapshot(
        self,
        *,
        loaded_at: datetime,
        reason: str,
        reconcile_broker: bool,
    ) -> MonitoringRuntimeSnapshot:
        if reconcile_broker and self.settings.runtime_mode == "live":
            adapter = self.broker_adapter_factory(self.settings)
            with self.session_factory() as db:
                try:
                    sync_execution_positions_from_broker_snapshot(
                        settings=self.settings,
                        db=db,
                        adapter=adapter,
                        symbols=None,
                    )
                    db.commit()
                except Exception:
                    LOGGER.exception(
                        "monitoring_snapshot_broker_reconcile_failed",
                        extra={"reason": reason, "runtime_mode": self.settings.runtime_mode},
                    )
        snapshot = self._load_monitoring_runtime_snapshot(loaded_at)
        with self._lock:
            self._monitoring_runtime_snapshot = snapshot
            if reconcile_broker or self._monitoring_snapshot_last_reconciled_at is None:
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
            },
        )
        return snapshot

    def _reconcile_and_refresh_monitoring_snapshot_if_due(self, now: datetime) -> None:
        last_reconciled_at = self._monitoring_snapshot_last_reconciled_at
        if last_reconciled_at is not None:
            elapsed = (now - last_reconciled_at).total_seconds()
            if elapsed < self.settings.exec_agent_max_account_snapshot_age_seconds:
                return
        self._refresh_monitoring_runtime_snapshot(
            loaded_at=now,
            reason="periodic_broker_truth_refresh",
            reconcile_broker=True,
        )

    def _refresh_monitoring_snapshot_filled_today(self, now: datetime) -> None:
        with self._lock:
            snapshot = self._monitoring_runtime_snapshot
        if snapshot is None:
            return
        with self.session_factory() as db:
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

    def _repair_post_entry_exit_monitors_for_cached_cycle(
        self,
        *,
        runtime_snapshot: MonitoringRuntimeSnapshot,
        prices: list[MonitoringPriceRequest],
        started_at: datetime,
    ) -> int:
        if not runtime_snapshot.positions_by_symbol:
            return 0
        prices_by_symbol = {price.symbol: price for price in prices}
        with self.session_factory() as db:
            positions_by_symbol = _load_positions_by_symbol(
                settings=self.settings,
                db=db,
                symbols=set(runtime_snapshot.positions_by_symbol),
            )
            maintenance_count, _review_count = _maintain_post_entry_exit_monitors(
                settings=self.settings,
                db=db,
                positions_by_symbol=positions_by_symbol,
                prices=prices_by_symbol,
                triggered_at=started_at,
            )
            if maintenance_count:
                db.commit()
            return maintenance_count

    def _load_monitoring_runtime_snapshot(self, loaded_at: datetime) -> MonitoringRuntimeSnapshot:
        with self.session_factory() as db:
            symbols = list_autonomous_monitoring_symbols(
                settings=self.settings,
                db=db,
            )
            return self._load_monitoring_runtime_snapshot_for_symbols_with_session(
                db=db,
                loaded_at=loaded_at,
                symbols=symbols,
            )

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
        with self.session_factory() as db:
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
            )
        symbol_placeholders = ",".join("?" for _ in symbols)
        watch_status_placeholders = ",".join("?" for _ in WATCH_MONITORING_STATUSES)
        watch_intent_placeholders = ",".join("?" for _ in WATCH_BUY_INTENTS)
        sell_statuses = (
            SELL_ACTIVE_MONITORING_STATUSES
            | BLOCKED_MONITORING_STATUSES
            | PENDING_DISPATCH_MONITORING_STATUSES
        )
        sell_status_placeholders = ",".join("?" for _ in sell_statuses)
        sell_intent_placeholders = ",".join("?" for _ in SELL_MONITORED_INTENTS)
        try:
            with sqlite3.connect(f"file:{url.database}?mode=ro", uri=True, timeout=5) as conn:
                position_rows = conn.execute(
                    f"""
                    select symbol, quantity
                    from execution_positions
                    where mode = ? and quantity > 0 and symbol in ({symbol_placeholders})
                    """,
                    [self.settings.runtime_mode, *symbols],
                ).fetchall()
                buy_item_rows = conn.execute(
                    f"""
                    select id, symbol, intent, status, trigger_price, risk_context
                    from execution_plan_items
                    where side = 'buy'
                      and status in ({watch_status_placeholders})
                      and intent in ({watch_intent_placeholders})
                      and symbol in ({symbol_placeholders})
                    """,
                    [*WATCH_MONITORING_STATUSES, *WATCH_BUY_INTENTS, *symbols],
                ).fetchall()
                sell_item_rows = conn.execute(
                    f"""
                    select id, symbol, intent, status, trigger_price, risk_context
                    from execution_plan_items
                    where side = 'sell'
                      and status in ({sell_status_placeholders})
                      and intent in ({sell_intent_placeholders})
                      and symbol in ({symbol_placeholders})
                    """,
                    [*sell_statuses, *SELL_MONITORED_INTENTS, *symbols],
                ).fetchall()
        except sqlite3.Error:
            LOGGER.exception(
                "monitoring_sqlite_symbol_snapshot_failed",
                extra={"runtime_mode": self.settings.runtime_mode, "symbol_count": len(symbols)},
            )
            return None
        cached_items = [
            CachedMonitoringItem(
                id=str(row[0]),
                symbol=str(row[1]),
                intent=str(row[2]),
                status=str(row[3]),
                trigger_price=Decimal(str(row[4])) if row[4] is not None else None,
                trigger_direction=_cached_trigger_direction_from_risk_context(row[5]),
                chase_limit_price=_cached_chase_limit_price_from_risk_context(row[5]),
            )
            for row in [*buy_item_rows, *sell_item_rows]
        ]
        return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=symbols,
            positions_by_symbol={
                str(row[0]): CachedMonitoringPosition(symbol=str(row[0]), quantity=int(row[1]))
                for row in position_rows
                if row[1] > 0
            },
            items=cached_items,
            filled_today_symbols=[],
        )

    def _load_local_desired_monitoring_symbols_from_sqlite(self) -> list[str]:
        try:
            url = make_url(self.settings.database_url)
        except Exception:
            return []
        if not url.drivername.startswith("sqlite") or not url.database or url.database == ":memory:":
            return []
        limit = self.settings.monitoring_max_symbols_per_cycle or 128
        buy_statuses = tuple(WATCH_MONITORING_STATUSES)
        sell_statuses = tuple(
            SELL_ACTIVE_MONITORING_STATUSES
            | BLOCKED_MONITORING_STATUSES
            | PENDING_DISPATCH_MONITORING_STATUSES
        )
        buy_status_placeholders = ",".join("?" for _ in buy_statuses)
        sell_status_placeholders = ",".join("?" for _ in sell_statuses)
        buy_intent_placeholders = ",".join("?" for _ in WATCH_BUY_INTENTS)
        sell_intent_placeholders = ",".join("?" for _ in SELL_MONITORED_INTENTS)
        try:
            with sqlite3.connect(f"file:{url.database}?mode=ro", uri=True, timeout=5) as conn:
                position_rows = conn.execute(
                    """
                    select symbol
                    from execution_positions
                    where mode = ? and quantity > 0
                    order by symbol asc
                    limit ?
                    """,
                    [self.settings.runtime_mode, limit],
                ).fetchall()
                item_rows = conn.execute(
                    f"""
                    select distinct symbol
                    from execution_plan_items
                    where (
                        side = 'buy'
                        and status in ({buy_status_placeholders})
                        and intent in ({buy_intent_placeholders})
                    ) or (
                        side = 'sell'
                        and status in ({sell_status_placeholders})
                        and intent in ({sell_intent_placeholders})
                    )
                    order by symbol asc
                    limit ?
                    """,
                    [*buy_statuses, *WATCH_BUY_INTENTS, *sell_statuses, *SELL_MONITORED_INTENTS, limit * 2],
                ).fetchall()
        except sqlite3.Error:
            LOGGER.exception(
                "monitoring_sqlite_local_symbol_fallback_failed",
                extra={"runtime_mode": self.settings.runtime_mode},
            )
            return []
        ordered: list[str] = []
        seen: set[str] = set()
        for row in [*position_rows, *item_rows]:
            symbol = str(row[0] or "").strip()
            if not symbol or symbol in seen:
                continue
            ordered.append(symbol)
            seen.add(symbol)
            if len(ordered) >= limit:
                break
        return ordered

    def _load_monitoring_runtime_snapshot_for_symbols_with_session(
        self,
        *,
        db: Session,
        loaded_at: datetime,
        symbols: list[str],
    ) -> MonitoringRuntimeSnapshot:
            position_rows = db.execute(
                select(ExecutionPosition.symbol, ExecutionPosition.quantity).where(
                    ExecutionPosition.mode == self.settings.runtime_mode,
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
                ).where(
                    ExecutionPlanItem.side == "buy",
                    ExecutionPlanItem.status.in_(tuple(WATCH_MONITORING_STATUSES)),
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
                ).where(
                    ExecutionPlanItem.side == "sell",
                    ExecutionPlanItem.status.in_(
                        tuple(
                            SELL_ACTIVE_MONITORING_STATUSES
                            | BLOCKED_MONITORING_STATUSES
                            | PENDING_DISPATCH_MONITORING_STATUSES
                        )
                    ),
                    ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                    ExecutionPlanItem.symbol.in_(symbols or [""]),
                )
            ).all()
            item_rows = [*buy_item_rows, *sell_item_rows]
            cached_items = [
                CachedMonitoringItem(
                    id=str(item.id),
                    symbol=str(item.symbol),
                    intent=str(item.intent),
                    status=str(item.status),
                    trigger_price=item.trigger_price,
                    trigger_direction=_cached_trigger_direction_from_risk_context(item.risk_context),
                    chase_limit_price=_cached_chase_limit_price_from_risk_context(item.risk_context),
                )
                for item in item_rows
            ]
            return MonitoringRuntimeSnapshot(
            loaded_at=loaded_at,
            symbols=symbols,
            positions_by_symbol={
                str(row.symbol): CachedMonitoringPosition(symbol=str(row.symbol), quantity=int(row.quantity))
                for row in position_rows
                if row.quantity > 0
            },
            items=cached_items,
            filled_today_symbols=[],
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
        deferred_item_count = 0
        evaluated_symbols: set[str] = set()
        triggered_symbols: set[str] = set()
        results: list[dict[str, object]] = []
        for item in runtime_snapshot.items:
            if (
                item.intent not in WATCH_BUY_INTENTS
                and item.symbol not in runtime_snapshot.positions_by_symbol
            ):
                continue
            if item.intent in WATCH_BUY_INTENTS and item.status not in ACTIONABLE_CACHED_WATCH_STATUSES:
                continue
            price = prices_by_symbol.get(item.symbol)
            observed_price = price.price if price is not None else None
            if price is None and item.intent != "max_holding_close_sell":
                continue
            evaluated_item_count += 1
            evaluated_symbols.add(item.symbol)
            triggered = _cached_monitoring_item_triggered(
                item=item,
                observed_price=observed_price,
                triggered_at=started_at,
            )
            if item.status.startswith("triggered_pending_agent"):
                blocked_item_count += 1
                outcome = "agent_pending"
            elif triggered and item.intent == "max_holding_close_sell":
                deferred_item_count += 1
                outcome = "deferred"
            elif triggered and _cached_watch_buy_chase_blocked(item=item, observed_price=observed_price):
                blocked_item_count += 1
                outcome = "blocked"
            elif triggered:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                outcome = "triggered"
            else:
                outcome = "waiting"
            results.append(
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "intent": item.intent,
                    "status": item.status,
                    "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
                    "observed_price": str(observed_price) if observed_price is not None else None,
                    "outcome": outcome,
                    "block_reason": (
                        "price_above_chase_limit"
                        if outcome == "blocked" and item.intent in WATCH_BUY_INTENTS
                        else None
                    ),
                }
            )
        return {
            "triggered_at": started_at.isoformat(),
            "worker_name": MONITORING_LOOP_WORKER_NAME,
            "evaluated_item_count": evaluated_item_count,
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": 0,
            "blocked_item_count": blocked_item_count,
            "deferred_item_count": deferred_item_count,
            "evaluated_symbol_count": len(evaluated_symbols),
            "triggered_symbol_count": len(triggered_symbols),
            "triggered_symbols": sorted(triggered_symbols),
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
            state.last_cycle = payload

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


def _price_source_diagnostics(source: object) -> dict[str, object]:
    get_last_diagnostics = getattr(source, "get_last_diagnostics", None)
    if get_last_diagnostics is None:
        return {}
    diagnostics = get_last_diagnostics()
    if not isinstance(diagnostics, dict):
        return {}
    return dict(diagnostics)


def _first_quote_failure_error(diagnostics: dict[str, object]) -> str | None:
    failures = diagnostics.get("quote_failures")
    if not isinstance(failures, list):
        return None
    for failure in failures:
        if not isinstance(failure, dict):
            continue
        error = failure.get("error")
        if isinstance(error, str) and error.strip():
            return error
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
        return True
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
    if item.trigger_direction in {"at_or_above", "at_or_below"}:
        return item.trigger_direction
    return "at_or_below"


def _cached_watch_buy_chase_blocked(
    *,
    item: CachedMonitoringItem,
    observed_price: Decimal | None,
) -> bool:
    if item.intent not in WATCH_BUY_INTENTS or observed_price is None:
        return False
    chase_limit_price = item.chase_limit_price
    if chase_limit_price is None:
        return False
    if _cached_watch_buy_direction(item) == "at_or_above":
        return observed_price > chase_limit_price
    return observed_price < chase_limit_price


def _cached_chase_limit_price_from_risk_context(raw_context: object) -> Decimal | None:
    context = _decode_cached_risk_context(raw_context)
    if not isinstance(context, dict):
        return None
    sources: list[dict[str, object]] = [context]
    for key in (
        "watch_buy_monitoring",
        "last_monitoring_event",
        "watch_buy_trigger_cross",
        "trigger_cross",
        "monitoring",
    ):
        value = context.get(key)
        if isinstance(value, dict):
            sources.append(value)
    for source in sources:
        value = source.get("chase_limit_price")
        if value is None:
            value = source.get("original_watch_chase_limit_price")
        parsed = _parse_decimal(value)
        if parsed is not None:
            return parsed
    return None


def _cached_trigger_direction_from_risk_context(raw_context: object) -> str | None:
    context = _decode_cached_risk_context(raw_context)
    if not isinstance(context, dict):
        return None
    last_event = context.get("last_monitoring_event")
    if isinstance(last_event, dict) and (
        last_event.get("event") == "watch_buy_pullback_wait"
        or last_event.get("reason") == "price_above_chase_limit"
    ):
        return "at_or_below"
    sources: list[dict[str, object]] = [context]
    for key in (
        "traceability",
        "short_term",
        "handoff",
        "entry_context",
        "monitoring",
        "watch_buy_monitoring",
        "trigger_cross",
    ):
        value = context.get(key)
        if isinstance(value, dict):
            sources.append(value)
    for source in sources:
        for key in ("trigger_direction", "entry_direction", "watch_direction", "direction"):
            value = source.get(key)
            if not isinstance(value, str):
                continue
            normalized = value.strip().lower()
            if normalized in {"up", "above", "breakout", "momentum", ">=", "at_or_above"}:
                return "at_or_above"
            if normalized in {"down", "below", "pullback", "dip", "<=", "at_or_below"}:
                return "at_or_below"
    return None


def _decode_cached_risk_context(raw_context: object) -> dict[str, object] | None:
    if isinstance(raw_context, dict):
        return raw_context
    if isinstance(raw_context, str) and raw_context.strip():
        try:
            decoded = json.loads(raw_context)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, dict) else None
    return None


def _triggered_symbols_from_cached_payload(payload: dict[str, object]) -> set[str]:
    raw_symbols = payload.get("triggered_symbols")
    if isinstance(raw_symbols, list):
        return {
            symbol
            for symbol in raw_symbols
            if isinstance(symbol, str) and symbol
        }
    items = payload.get("items")
    if not isinstance(items, list):
        return set()
    symbols: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("outcome") != "triggered":
            continue
        symbol = item.get("symbol")
        if isinstance(symbol, str) and symbol:
            symbols.add(symbol)
    return symbols


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


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
    symbols = db.scalars(
        select(distinct(ExecutionFill.symbol))
        .select_from(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at >= today_start_utc,
            ExecutionFill.filled_at < tomorrow_start_utc,
        )
    ).all()
    return sorted(str(symbol) for symbol in symbols)


def _current_krx_day_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current_krx = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    start_krx = datetime.combine(current_krx.date(), time.min, tzinfo=KRX_TIMEZONE)
    end_krx = datetime.combine(current_krx.date(), time.max, tzinfo=KRX_TIMEZONE)
    return start_krx.astimezone(UTC), end_krx.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


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
