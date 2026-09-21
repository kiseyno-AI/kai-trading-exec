import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import and_, distinct, func, literal, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import (
    Session,
    aliased,
    contains_eager,
    load_only,
    selectinload,
    undefer,
)

from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.db.sqlite_diagnostics import sqlite_diagnostics_payload
from app.db.session import ping_database
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    MonitoringCheckpoint,
    RuntimeRemediationEvent,
)
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    duplicate_fill_conflicting_fields,
    duplicate_fill_group_candidate_key,
    execution_fill_dedupe_key,
)
from app.services.fill_economics import (
    ClosedTradeEconomics,
    closed_trade_economics,
    execution_fill_economics,
    summarize_closed_trade_economics,
)
from app.services.fifo_inventory import (
    FifoRouteScope,
    ambiguous_fifo_realized_pnl_sell_count,
    fifo_fill_sort_key,
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
    replay_open_long_fill_inventory,
)
from app.services.max_holding_actionability import max_holding_actionability_summary
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
    position_identity_from_columns,
)
from app.services.late_entry_actionability import LATE_ENTRY_GATE_EVENT_TYPE
from app.services.monitoring import (
    ACTIVE_WATCH_MONITORING_STATUSES,
    BLOCKED_MONITORING_STATUSES,
    LIVE_WATCH_BUY_VETO_EVENT_TYPE,
    MONITORING_RUNTIME_TOPOLOGY,
    MONITORING_SELECTION_STRATEGY,
    PENDING_DISPATCH_MONITORING_STATUSES,
    SELL_ACTIVE_MONITORING_STATUSES,
    SELL_PENDING_CONFIG_STATUSES,
    _effective_monitor_plan_filter,
    _is_krx_equity_symbol,
    _monitoring_item_is_effective_at,
    _symbols_with_recent_flat_position_truth,
    _symbols_with_unexited_buy_orders,
    resolve_monitoring_symbol_cycle_limit,
)
from app.services.trigger_time_exec_agent import (
    AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    AGENT_DELAY_STATUS,
    AGENT_PENDING_STATUS,
    AGENT_REJECTED_STATUS,
)
from app.services.broker_auth import build_broker_adapter, build_broker_session_summary
from app.services.account_drawdown_gate import latest_account_drawdown_gate_state
from app.services.broker_order_state import (
    ACTIONABLE_OPEN_ORDER_STATUSES,
    CAPITAL_RESERVING_ORDER_STATUSES,
    MANUAL_REVIEW_ORDER_STATUSES,
    NONTERMINAL_ORDER_STATUSES,
    UNTRUSTED_ACTIVE_ORDER_ERROR_CLASSIFICATIONS,
    active_local_broker_order_filters,
    broker_order_observed_at_expr,
)
from app.services.execution_api import list_runtime_symbols
from app.services.execution_account_route import (
    classify_kis_account_route,
    validate_execution_account_scope,
)
from app.services.execution_semantics import KRX_EXCHANGES, WATCH_BUY_INTENTS
from app.services.schema import check_runtime_schema, execution_position_readiness
from app.services.runtime_controls import (
    ensure_runtime_control_state,
    runtime_safety_payload,
)

OPEN_ORDER_STATUSES = ACTIONABLE_OPEN_ORDER_STATUSES
ACTIVE_JOB_STATUSES = ("queued", "scheduled", "running", "retrying")
MONITORED_INTENTS = ("target_sell", "stop_sell")
SELL_MONITORED_INTENTS = ("target_sell", "stop_sell", "max_holding_close_sell")
WATCH_BUY_INTENTS = tuple(sorted(WATCH_BUY_INTENTS))
KIS_US_EXCHANGES = frozenset({"AMEX", "NASD", "NYSE"})
RUNTIME_MONITORED_SYMBOL_MEMBERSHIP_SCHEMA_VERSION = (
    "runtime_monitored_symbol_membership_v1"
)
RUNTIME_PROVIDER_EXCHANGES = {
    "alpaca": ("ALPACA", "US"),
    "kis": tuple(sorted(KRX_EXCHANGES | KIS_US_EXCHANGES)),
    "upbit": ("UPBIT",),
}
RUNTIME_PROVIDER_BROKER_NAMES = {
    "alpaca": ("alpaca",),
    "kis": ("kis", "korea-investment", "korea_investment"),
    "upbit": ("upbit",),
}
KNOWN_RUNTIME_PROVIDER_BROKER_NAMES = tuple(
    sorted(
        {
            broker_name
            for broker_names in RUNTIME_PROVIDER_BROKER_NAMES.values()
            for broker_name in broker_names
        }
    )
)
RUNTIME_EXECUTION_SCOPE_KEYS = frozenset(
    {
        "policy_version",
        "provider",
        "account_scope",
        "venue",
        "asset_class",
        "exchange",
    }
)
WATCH_BUY_STATUSES = (
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "watch_buy_triggered",
    "watch_buy_triggered_blocked",
    "watch_buy_triggered_pending_dispatch",
    AGENT_PENDING_STATUS,
    AGENT_DELAY_STATUS,
    AGENT_APPROVED_PENDING_SUBMIT_STATUS,
)
WATCHING_AGENT_COUNT_STATUSES = (
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "target_sell_monitoring",
    "stop_sell_monitoring",
    "max_holding_close_sell_monitoring",
)
AGENT_APPROVED_SUBMITTED_STATUSES = (
    AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    "watch_buy_submitted",
    "broker_order_submission_pending",
    "broker_order_submitted",
    "submitted",
    "open",
    "partially_filled",
)
SHORT_TERM_BLOCKED_STATUSES = (
    "short_term_watch_blocked",
    "pending_configuration",
    "watch_buy_triggered_blocked",
    "watch_buy_rejected",
    "watch_cancelled",
    "watch_expired",
)
SHORT_TERM_DECIDER_NAME = "short-term-decider"
SIGNAL_DECIDER_NAME = "signal-decider"
FAILED_ORDER_STATUSES = ("rejected", "failed")
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
US_MARKET_TIMEZONE = ZoneInfo("America/New_York")
UPBIT_MARKET_TIMEZONE = ZoneInfo("Asia/Seoul")
MAX_STATUS_DETAIL_ITEMS = 20


@dataclass(frozen=True)
class RuntimeMetadata:
    started_at: datetime

    @property
    def process_id(self) -> int:
        return os.getpid()


@dataclass(frozen=True)
class DuplicateFillQualityGroup:
    broker_order_pk: str
    broker_fill_id: str | None
    symbol: str
    side: str
    quantity: int
    price: Decimal
    filled_at: datetime
    duplicate_count: int
    quantity_variants: int
    base_quantity_variants: int
    quote_notional_variants: int
    price_variants: int
    economic_conflict: bool
    conflicting_fields: tuple[str, ...]
    candidate_key: str


@dataclass(frozen=True)
class RuntimeFillScopeSelection:
    fills: list[ExecutionFill]
    indeterminate_fill_count: int
    scope_unavailable_reason: str | None = None
    chronology_ambiguous_fill_count: int = 0


@dataclass(frozen=True)
class RuntimeOrderScopeSelection:
    orders: list[BrokerOrder]
    indeterminate_order_count: int
    foreign_order_count: int
    scope_unavailable_reason: str | None = None


@dataclass(frozen=True)
class RuntimePositionEvidenceSelection:
    symbols: set[str]
    indeterminate_order_count: int
    indeterminate_fill_count: int
    foreign_order_count: int
    scope_unavailable_reason: str | None = None


@dataclass(frozen=True)
class RuntimeExecutionScope:
    provider: str
    exchanges: tuple[str, ...]
    account_scope: str | None
    status: str
    source: str | None
    reason: str | None

    @property
    def available(self) -> bool:
        return self.status == "available" and self.account_scope is not None


def liveness_payload(runtime: RuntimeMetadata) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "process_id": runtime.process_id,
        "started_at": runtime.started_at.isoformat(),
        "uptime_seconds": round((now - runtime.started_at).total_seconds(), 3),
    }


def default_safety(settings: Settings, reason: str) -> dict[str, object]:
    return {
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": False,
        "automation_enabled": False,
        "automated_order_submission_allowed": False,
        "automated_order_submission_block_reason": "paused",
        "automated_buy_submission_allowed": False,
        "automated_buy_submission_block_reason": "paused",
        "hourly_risk_off": {
            "available": False,
            "active": None,
            "epoch": None,
            "context": None,
        },
        "readiness_reason": reason,
        "updated_at": None,
        "last_change": {
            "action": "bootstrap",
            "actor": None,
            "source": "system",
            "reason": reason,
            "changed_at": None,
            "event_id": None,
        },
    }


def fast_autonomous_safety(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
    except SQLAlchemyError as exc:
        db.rollback()
        return default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )
    return runtime_safety_payload(state)


def build_runtime_safety(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
    except SQLAlchemyError as exc:
        db.rollback()
        return default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )
    return runtime_safety_payload(state)


def runtime_counts(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, int]:
    candidate_orders = list(
        db.scalars(
            select(BrokerOrder)
            .where(
                *active_local_broker_order_filters(
                    settings=settings,
                    statuses=OPEN_ORDER_STATUSES,
                )
            )
            .options(undefer(BrokerOrder.request_metadata))
        ).all()
    )
    open_orders = (
        _runtime_order_scope_selection(
            settings=settings,
            orders=candidate_orders,
            provider=provider,
            runtime_scope=runtime_scope,
        ).orders
        if runtime_scope is not None
        else candidate_orders
    )
    candidate_jobs = list(
        db.scalars(
            select(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(ACTIVE_JOB_STATUSES),
        )
        .options(selectinload(ExecutionJob.plan_item))
        ).all()
    )
    active_jobs = [
        job
        for job in candidate_jobs
        if runtime_scope is None
        or _active_runtime_plan_item_exchange(
            settings=settings,
            item=job.plan_item,
            provider=provider,
            runtime_scope=runtime_scope,
        )
        is not None
    ]
    return {
        "open_orders": len(open_orders),
        "active_execution_jobs": len(active_jobs),
    }


def runtime_watch_buy_veto_summary(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _runtime_today_utc_bounds(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    rows = db.execute(
        select(ExecutionPlanItemAudit, ExecutionPlanItem)
        .select_from(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type == LIVE_WATCH_BUY_VETO_EVENT_TYPE,
            ExecutionPlanItemAudit.occurred_at >= today_start_utc,
            ExecutionPlanItemAudit.occurred_at < tomorrow_start_utc,
        )
    ).all()
    counts_by_symbol: dict[str, int] = {}
    for _audit, item in rows:
        if (
            runtime_scope is not None
            and _active_runtime_plan_item_exchange(
                settings=settings,
                item=item,
                provider=provider,
                runtime_scope=runtime_scope,
            )
            is None
        ):
            continue
        symbol = str(item.symbol)
        counts_by_symbol[symbol] = counts_by_symbol.get(symbol, 0) + 1
    return {
        "today_count": sum(counts_by_symbol.values()),
        "today_symbols": sorted(counts_by_symbol),
    }


def runtime_late_entry_summary(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _runtime_today_utc_bounds(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    rows = db.execute(
        select(ExecutionPlanItemAudit, ExecutionPlanItem)
        .select_from(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type == LATE_ENTRY_GATE_EVENT_TYPE,
            ExecutionPlanItemAudit.occurred_at >= today_start_utc,
            ExecutionPlanItemAudit.occurred_at < tomorrow_start_utc,
        )
        .order_by(ExecutionPlanItemAudit.occurred_at.asc())
    ).all()
    scoped_rows = [
        (audit, item)
        for audit, item in rows
        if runtime_scope is None
        or _active_runtime_plan_item_exchange(
            settings=settings,
            item=item,
            provider=provider,
            runtime_scope=runtime_scope,
        )
        is not None
    ]
    counts = {"shadow": 0, "block": 0}
    symbols: set[str] = set()
    for audit, item in scoped_rows:
        summary = audit.summary if isinstance(audit.summary, dict) else {}
        gate_mode = summary.get("mode")
        if gate_mode in counts:
            counts[str(gate_mode)] += 1
        symbols.add(str(item.symbol))
    return {
        "event_count": len(scoped_rows),
        "shadow_count": counts["shadow"],
        "block_count": counts["block"],
        "symbols": sorted(symbols),
        "first_observed_at": (
            scoped_rows[0][0].occurred_at.isoformat() if scoped_rows else None
        ),
        "last_observed_at": (
            scoped_rows[-1][0].occurred_at.isoformat() if scoped_rows else None
        ),
        "broker_mutations": False,
    }


def runtime_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    monitoring: dict[str, object],
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    position_truth = position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=observed_at,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    broker_orders = broker_order_freshness_payload(
        settings=settings,
        db=db,
        now=observed_at,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    return {
        "generated_at": observed_at.isoformat(),
        "monitoring": {
            "status": monitoring.get("status"),
            "last_heartbeat_at": monitoring.get("last_heartbeat_at"),
            "last_success_at": monitoring.get("last_success_at"),
            "source": monitoring.get("source") or "database",
        },
        "position_truth": position_truth,
        "broker_orders": broker_orders,
    }


def broker_order_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    latest_order_sync_at: datetime | None = None,
    latest_reconcile_event_at: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    if latest_order_sync_at is None or latest_reconcile_event_at is None:
        scoped_order_sync_at, scoped_reconcile_event_at = (
            _runtime_broker_order_freshness_times(
                settings=settings,
                db=db,
                provider=provider,
                runtime_scope=runtime_scope,
            )
        )
        if latest_order_sync_at is None:
            latest_order_sync_at = scoped_order_sync_at
        if latest_reconcile_event_at is None:
            latest_reconcile_event_at = scoped_reconcile_event_at
    return {
        "latest_broker_sync_at": _serialize_datetime(latest_order_sync_at),
        "latest_reconciliation_event_at": _serialize_datetime(latest_reconcile_event_at),
        "stale_nonterminal": stale_nonterminal_order_freshness_payload(
            settings=settings,
            db=db,
            now=observed_at,
            provider=provider,
            runtime_scope=runtime_scope,
        ),
        "fill_quality": broker_order_fill_quality_payload(
            settings=settings,
            db=db,
            provider=provider,
            runtime_scope=runtime_scope,
        ),
    }


def _runtime_broker_order_freshness_times(
    *,
    settings: Settings,
    db: Session,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> tuple[datetime | None, datetime | None]:
    if runtime_scope is None:
        return (
            db.scalar(
                select(func.max(BrokerOrder.last_broker_sync_at)).where(
                    BrokerOrder.mode == settings.runtime_mode,
                    BrokerOrder.last_broker_sync_at.is_not(None),
                )
            ),
            db.scalar(
                select(func.max(BrokerOrderEvent.observed_at))
                .select_from(BrokerOrderEvent)
                .join(BrokerOrderEvent.broker_order)
                .where(
                    BrokerOrder.mode == settings.runtime_mode,
                    BrokerOrderEvent.event_source == "reconciliation",
                )
            ),
        )
    candidate_orders = list(
        db.scalars(
            select(BrokerOrder)
            .where(BrokerOrder.mode == settings.runtime_mode)
            .options(undefer(BrokerOrder.request_metadata))
        ).all()
    )
    active_orders = _runtime_order_scope_selection(
        settings=settings,
        orders=candidate_orders,
        provider=provider,
        runtime_scope=runtime_scope,
    ).orders
    active_order_ids = [order.id for order in active_orders]
    latest_sync = max(
        (order.last_broker_sync_at for order in active_orders if order.last_broker_sync_at is not None),
        default=None,
    )
    latest_reconcile = (
        db.scalar(
            select(func.max(BrokerOrderEvent.observed_at)).where(
                BrokerOrderEvent.broker_order_pk.in_(active_order_ids),
                BrokerOrderEvent.event_source == "reconciliation",
            )
        )
        if active_order_ids
        else None
    )
    return latest_sync, latest_reconcile


def broker_order_fill_quality_payload(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    scoped_order_selection: RuntimeOrderScopeSelection | None = None
    scoped_order_ids: list[str] | None = None
    if runtime_scope is not None:
        candidate_orders = list(
            db.scalars(
                select(BrokerOrder)
                .where(
                    BrokerOrder.mode == settings.runtime_mode,
                    BrokerOrder.status == "filled",
                )
                .options(undefer(BrokerOrder.request_metadata))
            ).all()
        )
        scoped_order_selection = _runtime_order_scope_selection(
            settings=settings,
            orders=candidate_orders,
            provider=provider,
            runtime_scope=runtime_scope,
        )
        scoped_order_ids = [order.id for order in scoped_order_selection.orders]
    missing_fill_query = (
        select(
            BrokerOrder.id,
            BrokerOrder.symbol,
            BrokerOrder.side,
            BrokerOrder.quantity,
            BrokerOrder.submitted_at,
            BrokerOrder.idempotency_key,
            func.count(ExecutionFill.id).label("fill_count"),
        )
        .outerjoin(ExecutionFill, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status == "filled",
            *(
                (BrokerOrder.id.in_(scoped_order_ids),)
                if scoped_order_ids is not None
                else ()
            ),
        )
        .group_by(BrokerOrder.id)
        .having(func.count(ExecutionFill.id) == 0)
    )
    missing_fill_subquery = missing_fill_query.subquery()
    missing_fill_count = int(
        db.scalar(select(func.count()).select_from(missing_fill_subquery)) or 0
    )
    missing_fill_rows = db.execute(
        missing_fill_query
        .order_by(BrokerOrder.submitted_at.desc(), BrokerOrder.created_at.desc())
        .limit(MAX_STATUS_DETAIL_ITEMS)
    ).all()
    missing_order_rows = db.execute(
        select(missing_fill_subquery.c.id, missing_fill_subquery.c.symbol)
    ).all()
    missing_order_symbols = {
        str(order_id): str(symbol)
        for order_id, symbol in missing_order_rows
    }
    missing_order_ids = list(missing_order_symbols)
    missing_reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == settings.runtime_mode,
            RuntimeRemediationEvent.candidate_type == "missing_execution_fill",
            RuntimeRemediationEvent.outcome == "manual_review_required",
            RuntimeRemediationEvent.subject_id.in_(missing_order_ids or [""]),
        )
    ).all()
    missing_review_by_subject = {str(review.subject_id): review for review in missing_reviews}
    missing_fill_items = []
    for row in missing_fill_rows[:MAX_STATUS_DETAIL_ITEMS]:
        review = missing_review_by_subject.get(str(row.id))
        missing_fill_items.append({
            "broker_order_pk": row.id,
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "submitted_at": _serialize_datetime(row.submitted_at),
            "idempotency_key": row.idempotency_key,
            "review_outcome": review.outcome if review is not None else None,
            "review_actor": review.actor if review is not None else None,
            "review_reason": review.reason if review is not None else None,
            "reviewed_at": _serialize_datetime(review.occurred_at) if review is not None else None,
        })
    missing_manual_review_count = sum(
        1 for order_id in missing_order_ids if order_id in missing_review_by_subject
    )
    missing_unreviewed_symbols = sorted({
        symbol
        for order_id, symbol in missing_order_symbols.items()
        if order_id not in missing_review_by_subject
    })
    missing_manual_review_symbols = sorted({
        symbol
        for order_id, symbol in missing_order_symbols.items()
        if order_id in missing_review_by_subject
    })

    duplicate_fill_groups = _duplicate_fill_quality_groups(
        db=db,
        runtime_mode=settings.runtime_mode,
        broker_order_ids=scoped_order_ids,
    )
    duplicate_fill_count = len(duplicate_fill_groups)
    duplicate_fill_rows = duplicate_fill_groups[:MAX_STATUS_DETAIL_ITEMS]
    duplicate_candidate_keys = [row.candidate_key for row in duplicate_fill_rows]
    duplicate_reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == settings.runtime_mode,
            RuntimeRemediationEvent.candidate_type == "duplicate_fill_group",
            RuntimeRemediationEvent.outcome.in_(
                ["manual_review_required", "non_destructive_isolation"]
            ),
            RuntimeRemediationEvent.candidate_key.in_(duplicate_candidate_keys or [""]),
        )
    ).all()
    duplicate_review_by_key: dict[str, RuntimeRemediationEvent] = {}
    for review in duplicate_reviews:
        candidate_key = str(review.candidate_key)
        if (
            candidate_key not in duplicate_review_by_key
            or review.occurred_at > duplicate_review_by_key[candidate_key].occurred_at
        ):
            duplicate_review_by_key[candidate_key] = review
    duplicate_fill_items = []
    for row in duplicate_fill_rows[:MAX_STATUS_DETAIL_ITEMS]:
        review = duplicate_review_by_key.get(row.candidate_key)
        duplicate_fill_items.append({
            "broker_order_pk": row.broker_order_pk,
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "price": _decimal_text(row.price),
            "filled_at": _serialize_datetime(row.filled_at),
            "broker_fill_id": row.broker_fill_id,
            "duplicate_count": int(row.duplicate_count),
            "economic_conflict": row.economic_conflict,
            "conflicting_fields": list(row.conflicting_fields),
            "review_outcome": review.outcome if review is not None else None,
            "review_actor": review.actor if review is not None else None,
            "review_reason": review.reason if review is not None else None,
        })

    return {
        "scope_status": (
            "unavailable"
            if scoped_order_selection is not None
            and (
                scoped_order_selection.scope_unavailable_reason is not None
                or scoped_order_selection.indeterminate_order_count > 0
            )
            else "ok"
        ),
        "scope_reason": (
            (
                scoped_order_selection.scope_unavailable_reason
                or "indeterminate_execution_scope"
            )
            if scoped_order_selection is not None
            and (
                scoped_order_selection.scope_unavailable_reason is not None
                or scoped_order_selection.indeterminate_order_count > 0
            )
            else None
        ),
        "excluded_indeterminate_order_count": (
            scoped_order_selection.indeterminate_order_count
            if scoped_order_selection is not None
            else 0
        ),
        "excluded_other_route_count": (
            scoped_order_selection.foreign_order_count
            if scoped_order_selection is not None
            else 0
        ),
        "missing_filled_order_fill_count": missing_fill_count,
        "missing_filled_order_fill_unreviewed_count": missing_fill_count - missing_manual_review_count,
        "missing_filled_order_fill_manual_review_count": missing_manual_review_count,
        "missing_filled_order_fill_unreviewed_symbols": missing_unreviewed_symbols,
        "missing_filled_order_fill_manual_review_symbols": missing_manual_review_symbols,
        "missing_filled_order_fill_items": missing_fill_items,
        "missing_filled_order_fill_truncated": missing_fill_count > len(missing_fill_items),
        "duplicate_fill_group_count": duplicate_fill_count,
        "duplicate_fill_groups": duplicate_fill_items,
        "duplicate_fill_groups_truncated": duplicate_fill_count > len(duplicate_fill_items),
    }


def _duplicate_fill_quality_groups(
    *,
    db: Session,
    runtime_mode: str,
    broker_order_ids: list[str] | None = None,
) -> list[DuplicateFillQualityGroup]:
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == runtime_mode,
            *(
                (BrokerOrder.id.in_(broker_order_ids),)
                if broker_order_ids is not None
                else ()
            ),
        )
        .order_by(
            ExecutionFill.broker_order_pk.asc(),
            ExecutionFill.filled_at.asc(),
            ExecutionFill.id.asc(),
        )
    ).all()
    grouped: dict[str, list[ExecutionFill]] = {}
    for fill in fills:
        grouped.setdefault(execution_fill_dedupe_key(fill), []).append(fill)

    duplicate_groups: list[DuplicateFillQualityGroup] = []
    for group_key, group in grouped.items():
        if len(group) <= 1:
            continue
        retained = group[0]
        conflicting_fields = tuple(duplicate_fill_conflicting_fields(group))
        duplicate_groups.append(
            DuplicateFillQualityGroup(
                broker_order_pk=str(retained.broker_order_pk),
                broker_fill_id=retained.broker_fill_id,
                symbol=retained.symbol,
                side=retained.side,
                quantity=retained.quantity,
                price=Decimal(retained.price),
                filled_at=retained.filled_at,
                duplicate_count=len(group),
                quantity_variants=len({int(fill.quantity) for fill in group}),
                base_quantity_variants=len(
                    {
                        _decimal_text(Decimal(fill.base_quantity))
                        if fill.base_quantity is not None
                        else None
                        for fill in group
                    }
                ),
                quote_notional_variants=len(
                    {
                        _decimal_text(Decimal(fill.quote_notional))
                        if fill.quote_notional is not None
                        else None
                        for fill in group
                    }
                ),
                price_variants=len({_decimal_text(Decimal(fill.price)) for fill in group}),
                economic_conflict=bool(conflicting_fields),
                conflicting_fields=conflicting_fields,
                candidate_key=_duplicate_fill_candidate_key(
                    group_key=group_key,
                    fills=group,
                ),
            )
        )
    return sorted(
        duplicate_groups,
        key=lambda group: group.filled_at,
        reverse=True,
    )


def _duplicate_fill_candidate_key(*, group_key: str, fills: list[ExecutionFill]) -> str:
    return duplicate_fill_group_candidate_key(
        group_key=group_key,
        fill_ids=[str(fill.id) for fill in fills if fill.id is not None],
    )


def _active_runtime_kis_position_filters(
    *, settings: Settings
) -> tuple[object, ...]:
    return _active_runtime_position_filters(
        settings=settings,
        provider="kis",
    )


def _active_runtime_position_filters(
    *,
    settings: Settings,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> tuple[object, ...]:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    if not scope.available:
        return (ExecutionPosition.id.is_(None),)
    return (
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.provider == scope.provider,
        ExecutionPosition.account_scope == scope.account_scope,
        ExecutionPosition.exchange.in_(scope.exchanges),
    )


def _normalize_runtime_provider(provider: object) -> str:
    normalized = str(provider or "kis").strip().lower()
    if normalized in {"korea-investment", "korea_investment"}:
        return "kis"
    return normalized


def _runtime_account_drawdown_gate_state(
    *,
    settings: Settings,
    db: Session,
    provider: str,
) -> dict[str, object]:
    normalized_provider = _normalize_runtime_provider(provider)
    if normalized_provider != "kis":
        # The durable legacy gate reader derives KIS recovery fingerprints.
        # Until another provider has an exact-account reader, report the
        # absence of scoped evidence instead of displaying a KIS latch as if
        # it belonged to the active Alpaca or Upbit runtime.
        return {
            "available": False,
            "enabled": settings.account_drawdown_gate_enabled,
            "gated": None,
            "provider": normalized_provider,
            "latest_decision": None,
            "active_recovery_scopes": {},
            "reason": "provider_scoped_gate_state_unavailable",
        }
    return latest_account_drawdown_gate_state(settings=settings, db=db)


def _resolve_runtime_execution_scope(
    *,
    settings: Settings,
    provider: str,
    broker_adapter: BrokerAdapter | None = None,
) -> RuntimeExecutionScope:
    normalized_provider = _normalize_runtime_provider(provider)
    exchanges = RUNTIME_PROVIDER_EXCHANGES.get(normalized_provider, ())
    if not exchanges:
        return RuntimeExecutionScope(
            provider=normalized_provider,
            exchanges=(),
            account_scope=None,
            status="unavailable",
            source=None,
            reason="unsupported_broker_provider",
        )

    broker_account_id: str | None = None
    configured_scope = str(settings.execution_position_account_scope or "").strip()
    if normalized_provider == "alpaca":
        adapter_provider = _normalize_runtime_provider(
            getattr(broker_adapter, "provider", "")
        )
        account_id_loader = getattr(broker_adapter, "get_broker_account_id", None)
        if adapter_provider != normalized_provider or not callable(account_id_loader):
            return RuntimeExecutionScope(
                provider=normalized_provider,
                exchanges=exchanges,
                account_scope=None,
                status="unavailable",
                source=None,
                reason="authenticated_account_identity_unavailable",
            )
        try:
            raw_account_id = account_id_loader("")
        except Exception:
            return RuntimeExecutionScope(
                provider=normalized_provider,
                exchanges=exchanges,
                account_scope=None,
                status="unavailable",
                source=None,
                reason="authenticated_account_identity_unavailable",
            )
        broker_account_id = (
            raw_account_id.strip()
            if isinstance(raw_account_id, str) and raw_account_id.strip()
            else None
        )
        if broker_account_id is None:
            return RuntimeExecutionScope(
                provider=normalized_provider,
                exchanges=exchanges,
                account_scope=None,
                status="unavailable",
                source=None,
                reason="authenticated_account_identity_unavailable",
            )

    try:
        identity = (
            build_authenticated_broker_position_identity(
                settings=settings,
                provider=normalized_provider,
                exchange=exchanges[0],
                broker_account_id=str(broker_account_id or ""),
            )
            if normalized_provider == "alpaca"
            else build_position_identity(
                settings=settings,
                provider=normalized_provider,
                exchange=exchanges[0],
                broker_account_id=broker_account_id,
            )
        )
    except PositionIdentityConflictError:
        return RuntimeExecutionScope(
            provider=normalized_provider,
            exchanges=exchanges,
            account_scope=None,
            status="unavailable",
            source=None,
            reason="stable_account_scope_unavailable",
        )
    if broker_account_id is not None:
        source = "authenticated_broker_account"
    elif configured_scope:
        source = "configured_account_scope"
    elif normalized_provider == "kis" and str(settings.kis_account_number or "").strip():
        source = "configured_broker_account"
    else:
        source = "simulated_provider_route"
    return RuntimeExecutionScope(
        provider=normalized_provider,
        exchanges=exchanges,
        account_scope=identity.account_scope,
        status="available",
        source=source,
        reason=None,
    )


def _runtime_scope_for_provider(
    *,
    settings: Settings,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> RuntimeExecutionScope:
    normalized_provider = _normalize_runtime_provider(provider)
    if runtime_scope is None:
        return _resolve_runtime_execution_scope(
            settings=settings,
            provider=normalized_provider,
        )
    if runtime_scope.provider == normalized_provider:
        return runtime_scope
    return RuntimeExecutionScope(
        provider=normalized_provider,
        exchanges=RUNTIME_PROVIDER_EXCHANGES.get(normalized_provider, ()),
        account_scope=None,
        status="unavailable",
        source=None,
        reason="runtime_scope_provider_mismatch",
    )


def _runtime_pnl_timezone(
    *,
    settings: Settings | None,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> ZoneInfo:
    normalized_provider = _normalize_runtime_provider(provider)
    exchanges = {
        str(exchange).strip().upper()
        for exchange in (runtime_scope.exchanges if runtime_scope is not None else ())
        if str(exchange).strip()
    }
    if normalized_provider == "alpaca":
        return US_MARKET_TIMEZONE
    if normalized_provider == "upbit":
        return UPBIT_MARKET_TIMEZONE
    if normalized_provider == "kis":
        has_krx = "KRX" in exchanges
        has_us = bool(exchanges & KIS_US_EXCHANGES)
        if has_us and not has_krx:
            return US_MARKET_TIMEZONE
        if has_krx and not has_us:
            return KRX_TIMEZONE
        if bool(getattr(settings, "kis_us_trading_enabled", False)):
            return US_MARKET_TIMEZONE
    return KRX_TIMEZONE


def _runtime_pnl_scope(
    *,
    settings: Settings,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> tuple[RuntimeExecutionScope, ZoneInfo]:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    timezone = _runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=scope,
    )
    return scope, timezone


def _runtime_today_utc_bounds(
    *,
    settings: Settings,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> tuple[datetime, datetime]:
    timezone = _runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    return _current_pnl_day_utc_bounds(timezone=timezone)


def _runtime_execution_scope_payload(
    runtime_scope: RuntimeExecutionScope,
) -> dict[str, object]:
    return {
        "provider": runtime_scope.provider,
        "status": runtime_scope.status,
        "source": runtime_scope.source,
        "reason": runtime_scope.reason,
        "exchanges": list(runtime_scope.exchanges),
    }


def _canonical_runtime_snapshot_execution_scope(
    runtime_scope: RuntimeExecutionScope,
) -> dict[str, object] | None:
    if not runtime_scope.available:
        return None
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": runtime_scope.provider,
        "account_scope": runtime_scope.account_scope,
    }


def _exact_runtime_snapshot_scope_matches(
    *,
    value: object,
    runtime_scope: RuntimeExecutionScope,
) -> bool:
    expected = _canonical_runtime_snapshot_execution_scope(runtime_scope)
    return bool(
        expected is not None
        and isinstance(value, dict)
        and set(value) == set(expected)
        and value == expected
    )


def _autonomous_runtime_snapshot_scope_reasons(
    *,
    autonomous: dict[str, object],
    runtime_scope: RuntimeExecutionScope,
) -> list[str]:
    if not _exact_runtime_snapshot_scope_matches(
        value=autonomous.get("execution_scope"),
        runtime_scope=runtime_scope,
    ):
        return ["autonomous runtime snapshot execution scope is unavailable or mismatched"]

    monitoring = autonomous.get("monitoring")
    if not isinstance(monitoring, dict):
        return []
    reasons: list[str] = []
    last_cycle = monitoring.get("last_cycle")
    if isinstance(last_cycle, dict) and not _exact_runtime_snapshot_scope_matches(
        value=last_cycle.get("execution_scope"),
        runtime_scope=runtime_scope,
    ):
        reasons.append(
            "autonomous monitoring cycle execution scope is unavailable or mismatched"
        )
    runtime_snapshot = monitoring.get("runtime_snapshot")
    if isinstance(runtime_snapshot, dict) and not _exact_runtime_snapshot_scope_matches(
        value=runtime_snapshot.get("execution_scope"),
        runtime_scope=runtime_scope,
    ):
        reasons.append(
            "autonomous monitoring data snapshot execution scope is unavailable or mismatched"
        )
    return reasons


def _strip_account_scope(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_account_scope(item)
            for key, item in value.items()
            if key != "account_scope"
        }
    if isinstance(value, list):
        return [_strip_account_scope(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_account_scope(item) for item in value]
    return value


def _public_autonomous_runtime_payload(
    *,
    autonomous: dict[str, object],
    snapshot_trusted: bool,
    scope_reasons: list[str],
) -> dict[str, object]:
    public = _strip_account_scope(autonomous)
    assert isinstance(public, dict)
    public["snapshot_scope_status"] = "available" if snapshot_trusted else "unavailable"
    public["snapshot_scope_reason"] = scope_reasons[0] if scope_reasons else None
    if snapshot_trusted:
        return public
    monitoring = public.get("monitoring")
    if isinstance(monitoring, dict):
        monitoring["status"] = "degraded"
        monitoring["last_cycle"] = None
        monitoring["runtime_snapshot"] = None
    for worker_key in ("dispatch", "watch_exec_agent_sizing", "nxt_eligible_sync"):
        worker = public.get(worker_key)
        if isinstance(worker, dict):
            worker["last_cycle"] = None
    return public


def _active_runtime_plan_item_exchange(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> str | None:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    if not scope.available:
        return None
    active_provider = scope.provider
    if not isinstance(item.provider, str) or not item.provider.strip():
        return None
    item_provider = _normalize_runtime_provider(item.provider)
    if item_provider != active_provider:
        return None
    if active_provider == "kis" and classify_kis_account_route(
        settings=settings,
        provider=item_provider,
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
    ) != "active":
        return None
    if active_provider != "kis" and validate_execution_account_scope(
        provider=item_provider,
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
        current_binding={
            "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
            "provider": scope.provider,
            "account_scope": scope.account_scope,
        },
        require_binding=True,
    ) is not None:
        return None
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = risk_context.get("exchange")
    if not isinstance(exchange, str) and active_provider != "kis":
        exchange = item.venue
    if not isinstance(exchange, str):
        return None
    normalized_exchange = exchange.strip().upper()
    if normalized_exchange not in scope.exchanges:
        return None
    return normalized_exchange


def _active_runtime_order_scope(
    *,
    settings: Settings,
    order: BrokerOrder,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> FifoRouteScope | None:
    classification, route_scope = _runtime_order_scope_classification(
        settings=settings,
        order=order,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    return route_scope if classification == "active" else None


def _runtime_order_scope_classification(
    *,
    settings: Settings,
    order: BrokerOrder,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> tuple[str, FifoRouteScope | None]:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    if not scope.available:
        return "indeterminate", None
    active_provider = scope.provider
    if isinstance(order.broker_name, str) and order.broker_name.strip():
        broker_provider = _normalize_runtime_provider(order.broker_name)
        if (
            broker_provider in RUNTIME_PROVIDER_EXCHANGES
            and broker_provider != active_provider
        ):
            return "foreign", None
    metadata = order.request_metadata
    raw_scope = metadata.get("execution_scope") if isinstance(metadata, dict) else None
    if not isinstance(raw_scope, dict) or set(raw_scope) != RUNTIME_EXECUTION_SCOPE_KEYS:
        return "indeterminate", None
    route_scope = fifo_route_scope_from_order(order)
    if route_scope is None:
        return "indeterminate", None
    if (
        route_scope.provider != active_provider
        or _normalize_runtime_provider(order.broker_name) != active_provider
        or route_scope.exchange not in scope.exchanges
        or order.mode != settings.runtime_mode
    ):
        return "foreign", route_scope
    if (
        route_scope.provider != scope.provider
        or route_scope.account_scope != scope.account_scope
    ):
        return "foreign", route_scope
    return "active", route_scope


def _runtime_order_scope_selection(
    *,
    settings: Settings,
    orders: list[BrokerOrder],
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> RuntimeOrderScopeSelection:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    active_orders: list[BrokerOrder] = []
    indeterminate_order_count = 0
    foreign_order_count = 0
    for order in orders:
        classification, _route_scope = _runtime_order_scope_classification(
            settings=settings,
            order=order,
            provider=provider,
            runtime_scope=scope,
        )
        if classification == "active":
            active_orders.append(order)
        elif classification == "foreign":
            foreign_order_count += 1
        else:
            indeterminate_order_count += 1
    return RuntimeOrderScopeSelection(
        orders=active_orders,
        indeterminate_order_count=indeterminate_order_count,
        foreign_order_count=foreign_order_count,
        scope_unavailable_reason=(
            None if scope.available else scope.reason or "runtime_execution_scope_unavailable"
        ),
    )


def _active_runtime_fills_for_period(
    *,
    settings: Settings,
    db: Session,
    start_at: datetime | None,
    end_at: datetime,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> RuntimeFillScopeSelection:
    scope = _runtime_scope_for_provider(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    if not scope.available:
        return RuntimeFillScopeSelection(
            fills=[],
            indeterminate_fill_count=0,
            scope_unavailable_reason=(
                scope.reason or "runtime_execution_scope_unavailable"
            ),
        )
    period_filters = [
        BrokerOrder.mode == settings.runtime_mode,
        ExecutionFill.filled_at < end_at,
        or_(
            BrokerOrder.broker_name.is_(None),
            func.trim(BrokerOrder.broker_name) == "",
            func.lower(func.trim(BrokerOrder.broker_name)).in_(
                RUNTIME_PROVIDER_BROKER_NAMES.get(scope.provider, (scope.provider,))
            ),
            ~func.lower(func.trim(BrokerOrder.broker_name)).in_(
                KNOWN_RUNTIME_PROVIDER_BROKER_NAMES
            ),
        ),
    ]
    if start_at is not None:
        period_filters.append(ExecutionFill.filled_at >= start_at)
    fills = dedupe_execution_fills(
        db.scalars(
            select(ExecutionFill)
            .select_from(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(*period_filters)
            .options(
                undefer(ExecutionFill.payload),
                selectinload(ExecutionFill.broker_order).load_only(
                    BrokerOrder.id,
                    BrokerOrder.broker_name,
                    BrokerOrder.mode,
                    BrokerOrder.request_metadata,
                    BrokerOrder.venue,
                    BrokerOrder.asset_class,
                ),
            )
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    )
    active_fills: list[ExecutionFill] = []
    indeterminate_fill_count = 0
    for fill in fills:
        classification, _route_scope = _runtime_order_scope_classification(
            settings=settings,
            order=fill.broker_order,
            provider=provider,
            runtime_scope=scope,
        )
        if classification == "active":
            active_fills.append(fill)
        elif classification == "indeterminate":
            indeterminate_fill_count += 1
    return RuntimeFillScopeSelection(
        fills=active_fills,
        indeterminate_fill_count=indeterminate_fill_count,
    )


def position_truth_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    stale_before = observed_at - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    evidence = _position_truth_evidence_symbols(
        settings=settings,
        db=db,
        now=observed_at,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    evidence_symbols = evidence.symbols
    positions = db.scalars(
        select(ExecutionPosition).where(
            *_active_runtime_position_filters(
                settings=settings,
                provider=provider,
                runtime_scope=runtime_scope,
            ),
        )
    ).all()
    positions_by_symbol: dict[str, list[ExecutionPosition]] = {}
    for position in positions:
        positions_by_symbol.setdefault(position.symbol, []).append(position)
    nonzero_position_symbols = {
        position.symbol
        for position in positions
        if _position_has_quantity(position)
    }
    manual_review_reasons: dict[str, str] = {}
    for position in sorted(
        positions,
        key=lambda item: (item.symbol, str(item.exchange or ""), item.id),
    ):
        if not _position_has_quantity(position):
            continue
        raw_payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
        sync_context = raw_payload.get("_position_sync")
        if not (
            isinstance(sync_context, dict)
            and sync_context.get("monitoring_eligible") is False
        ):
            continue
        manual_review_reasons.setdefault(
            position.symbol,
            str(
                sync_context.get("manual_review_reason")
                or "position_monitoring_not_eligible"
            ),
        )
    candidate_symbols = nonzero_position_symbols | evidence_symbols
    trusted_flat_count = sum(
        1
        for symbol, symbol_positions in positions_by_symbol.items()
        if symbol not in evidence_symbols
        and not any(_position_has_quantity(position) for position in symbol_positions)
    )
    stale_symbols: list[str] = []
    oldest_age_seconds: float | None = None
    latest_reconciled_at: datetime | None = None
    oldest_reconciled_at: datetime | None = None
    for position in positions:
        reconciled_at = position.last_reconciled_at
        if reconciled_at is None:
            continue
        normalized_reconciled_at = _as_utc(reconciled_at)
        if latest_reconciled_at is None or normalized_reconciled_at > latest_reconciled_at:
            latest_reconciled_at = normalized_reconciled_at
        if oldest_reconciled_at is None or normalized_reconciled_at < oldest_reconciled_at:
            oldest_reconciled_at = normalized_reconciled_at

    for symbol in sorted(candidate_symbols):
        symbol_positions = positions_by_symbol.get(symbol, [])
        reconciled_times = [
            _as_utc(position.last_reconciled_at)
            for position in symbol_positions
            if position.last_reconciled_at is not None
        ]
        route_is_stale = (
            not symbol_positions
            or len(reconciled_times) != len(symbol_positions)
            or any(reconciled_at < stale_before for reconciled_at in reconciled_times)
        )
        if route_is_stale:
            stale_symbols.append(symbol)
            if not reconciled_times:
                continue
            age_seconds = (observed_at - min(reconciled_times)).total_seconds()
            if oldest_age_seconds is None or age_seconds > oldest_age_seconds:
                oldest_age_seconds = age_seconds

    if evidence.scope_unavailable_reason is not None:
        scope_status = "unavailable"
        scope_reason = evidence.scope_unavailable_reason
    elif evidence.indeterminate_order_count or evidence.indeterminate_fill_count:
        scope_status = "unavailable"
        scope_reason = "indeterminate_execution_scope"
    else:
        scope_status = "ok"
        scope_reason = None
    broker_truth_reachable = bool(
        scope_status == "ok"
        and latest_reconciled_at is not None
        and latest_reconciled_at >= stale_before
    )
    return {
        "scope_status": scope_status,
        "scope_reason": scope_reason,
        "excluded_indeterminate_order_count": evidence.indeterminate_order_count,
        "excluded_indeterminate_fill_count": evidence.indeterminate_fill_count,
        "excluded_other_route_count": evidence.foreign_order_count,
        "stale_count": len(stale_symbols),
        "oldest_age_seconds": round(oldest_age_seconds, 3) if oldest_age_seconds is not None else None,
        "oldest_age": _duration_text(oldest_age_seconds),
        "last_reconciled_at": _serialize_datetime(latest_reconciled_at),
        "oldest_reconciled_at": _serialize_datetime(oldest_reconciled_at),
        "broker_truth_reachable": broker_truth_reachable,
        "stale_symbols": stale_symbols,
        "max_age_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
        "tracked_symbol_count": len(candidate_symbols),
        "evidence_symbol_count": len(evidence_symbols),
        "nonzero_position_count": len(nonzero_position_symbols),
        "manual_review_count": len(manual_review_reasons),
        "manual_review_symbols": sorted(manual_review_reasons),
        "manual_review_reasons": {
            symbol: manual_review_reasons[symbol]
            for symbol in sorted(manual_review_reasons)
        },
        "trusted_flat_count": trusted_flat_count,
    }


def stale_nonterminal_order_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    threshold_seconds = settings.stale_broker_order_reconciliation_threshold_seconds
    stale_before = observed_at - timedelta(seconds=threshold_seconds)
    order_observed_at = broker_order_observed_at_expr()
    candidate_orders = list(db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            or_(
                BrokerOrder.status.in_(MANUAL_REVIEW_ORDER_STATUSES),
                and_(
                    BrokerOrder.status.in_(NONTERMINAL_ORDER_STATUSES),
                    or_(
                        order_observed_at < stale_before,
                        BrokerOrder.last_error_classification.in_(
                            tuple(UNTRUSTED_ACTIVE_ORDER_ERROR_CLASSIFICATIONS)
                        ),
                    ),
                ),
            ),
        )
        .options(undefer(BrokerOrder.request_metadata))
        .order_by(order_observed_at.asc(), BrokerOrder.created_at.asc())
    ).all())
    selection = _runtime_order_scope_selection(
        settings=settings,
        orders=candidate_orders,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    orders = selection.orders
    if selection.scope_unavailable_reason is not None:
        scope_status = "unavailable"
        scope_reason = selection.scope_unavailable_reason
    elif selection.indeterminate_order_count:
        scope_status = "unavailable"
        scope_reason = "indeterminate_execution_scope"
    else:
        scope_status = "ok"
        scope_reason = None
    items = [
        _stale_nonterminal_order_item(order=order)
        for order in orders[:MAX_STATUS_DETAIL_ITEMS]
    ]
    manual_review_count = sum(
        1
        for order in orders
        if _stale_nonterminal_order_action(order) == "manual_review"
    )
    return {
        "scope_status": scope_status,
        "scope_reason": scope_reason,
        "excluded_indeterminate_order_count": selection.indeterminate_order_count,
        "excluded_other_route_count": selection.foreign_order_count,
        "count": len(orders),
        "threshold_seconds": threshold_seconds,
        "stale_before": stale_before.isoformat(),
        "symbols": sorted({order.symbol for order in orders}),
        "needs_reconciliation_count": len(orders) - manual_review_count,
        "manual_review_count": manual_review_count,
        "missing_broker_order_id_count": sum(1 for order in orders if not order.broker_order_id),
        "items": items,
        "truncated": len(orders) > len(items),
    }


def runtime_actionable_alerts_payload(
    *,
    monitoring: dict[str, object],
    freshness: dict[str, object],
) -> dict[str, object]:
    alerts: list[dict[str, object]] = []
    broker_orders = freshness.get("broker_orders")
    stale_nonterminal = (
        broker_orders.get("stale_nonterminal")
        if isinstance(broker_orders, dict)
        else None
    )
    if isinstance(stale_nonterminal, dict):
        if stale_nonterminal.get("scope_status") == "unavailable":
            alerts.append(
                {
                    "kind": "broker_order_execution_scope_unavailable",
                    "severity": "critical",
                    "count": int(
                        stale_nonterminal.get("excluded_indeterminate_order_count")
                        or 0
                    ),
                    "symbols": [],
                    "message": (
                        "broker-order freshness excludes rows whose active execution "
                        "scope cannot be proven"
                    ),
                }
            )
        needs_reconciliation_count = int(stale_nonterminal.get("needs_reconciliation_count") or 0)
        manual_review_count = int(stale_nonterminal.get("manual_review_count") or 0)
        if needs_reconciliation_count > 0:
            alerts.append(
                {
                    "kind": "stale_broker_orders_need_reconciliation",
                    "severity": "warning",
                    "count": needs_reconciliation_count,
                    "symbols": stale_nonterminal.get("symbols") or [],
                    "message": "nonterminal broker orders have stale broker sync and should be reconciled",
                }
            )
        if manual_review_count > 0:
            alerts.append(
                {
                    "kind": "stale_broker_orders_need_manual_review",
                    "severity": "critical",
                    "count": manual_review_count,
                    "symbols": stale_nonterminal.get("symbols") or [],
                    "message": "nonterminal broker orders cannot be reconciled automatically",
                }
            )

    fill_quality = (
        broker_orders.get("fill_quality")
        if isinstance(broker_orders, dict)
        else None
    )
    if isinstance(fill_quality, dict):
        missing_fill_count = int(
            fill_quality.get("missing_filled_order_fill_unreviewed_count")
            if "missing_filled_order_fill_unreviewed_count" in fill_quality
            else fill_quality.get("missing_filled_order_fill_count") or 0
        )
        if missing_fill_count > 0:
            if "missing_filled_order_fill_unreviewed_symbols" in fill_quality:
                missing_fill_symbols = (
                    fill_quality.get("missing_filled_order_fill_unreviewed_symbols") or []
                )
            else:
                missing_fill_symbols = sorted({
                    str(item.get("symbol"))
                    for item in fill_quality.get("missing_filled_order_fill_items") or []
                    if isinstance(item, dict)
                    and item.get("symbol")
                    and item.get("review_outcome") != "manual_review_required"
                })
            alerts.append(
                {
                    "kind": "filled_broker_orders_missing_execution_fills",
                    "severity": "critical",
                    "count": missing_fill_count,
                    "symbols": missing_fill_symbols,
                    "message": (
                        "filled broker orders are missing execution_fills rows, "
                        "so FIFO realized P/L is incomplete"
                    ),
                }
            )
        manual_review_fill_count = int(
            fill_quality.get("missing_filled_order_fill_manual_review_count") or 0
        )
        if manual_review_fill_count > 0:
            if "missing_filled_order_fill_manual_review_symbols" in fill_quality:
                manual_review_fill_symbols = (
                    fill_quality.get("missing_filled_order_fill_manual_review_symbols") or []
                )
            else:
                manual_review_fill_symbols = sorted({
                    str(item.get("symbol"))
                    for item in fill_quality.get("missing_filled_order_fill_items") or []
                    if isinstance(item, dict)
                    and item.get("symbol")
                    and item.get("review_outcome") == "manual_review_required"
                })
            alerts.append(
                {
                    "kind": "filled_broker_orders_missing_fills_manual_review",
                    "severity": "critical",
                    "count": manual_review_fill_count,
                    "symbols": manual_review_fill_symbols,
                    "message": (
                        "missing execution_fills lack authoritative broker evidence and "
                        "have durable manual-review blockers"
                    ),
                }
            )
        duplicate_fill_count = int(fill_quality.get("duplicate_fill_group_count") or 0)
        if duplicate_fill_count > 0:
            alerts.append(
                {
                    "kind": "duplicate_execution_fill_groups",
                    "severity": "warning",
                    "count": duplicate_fill_count,
                    "symbols": sorted({
                        str(item.get("symbol"))
                        for item in fill_quality.get("duplicate_fill_groups") or []
                        if isinstance(item, dict) and item.get("symbol")
                    }),
                    "message": (
                        "duplicate execution_fills groups exist and should be repaired "
                        "before relying on raw fill exports"
                    ),
                }
            )

    position_truth = freshness.get("position_truth")
    if isinstance(position_truth, dict):
        stale_count = int(position_truth.get("stale_count") or 0)
        if stale_count > 0:
            alerts.append(
                {
                    "kind": "position_truth_stale",
                    "severity": "warning",
                    "count": stale_count,
                    "symbols": position_truth.get("stale_symbols") or [],
                    "message": "execution position truth is stale or missing for symbols with live evidence",
                }
            )
        manual_review_count = int(position_truth.get("manual_review_count") or 0)
        if manual_review_count > 0:
            alerts.append(
                {
                    "kind": "position_monitoring_manual_review_required",
                    "severity": "critical",
                    "count": manual_review_count,
                    "symbols": position_truth.get("manual_review_symbols") or [],
                    "message": (
                        "held positions cannot be protected automatically until their "
                        "quote and execution route are reviewed"
                    ),
                }
            )

    last_cycle = monitoring.get("last_cycle")
    if not isinstance(last_cycle, dict):
        last_cycle = {}

    protective_stop_coverage = monitoring.get("protective_stop_coverage")
    if not isinstance(protective_stop_coverage, dict):
        protective_stop_coverage = last_cycle.get("protective_stop_coverage")
    if isinstance(protective_stop_coverage, dict) and protective_stop_coverage.get("status") == "degraded":
        alerts.append(
            {
                "kind": "protective_stop_coverage_degraded",
                "severity": "critical",
                "count": int(protective_stop_coverage.get("unprotected_position_count") or 0),
                "symbols": protective_stop_coverage.get("affected_symbols") or [],
                "message": "held positions are missing full protective stop coverage",
            }
        )

    profit_target_coverage = monitoring.get("profit_target_coverage")
    if not isinstance(profit_target_coverage, dict):
        profit_target_coverage = last_cycle.get("profit_target_coverage")
    if isinstance(profit_target_coverage, dict) and profit_target_coverage.get("status") == "degraded":
        alerts.append(
            {
                "kind": "profit_target_coverage_degraded",
                "severity": "warning",
                "count": int(profit_target_coverage.get("missing_target_coverage_count") or 0),
                "symbols": profit_target_coverage.get("affected_symbols") or [],
                "message": "held positions are missing full profit target coverage",
            }
        )

    severity_order = {"critical": 3, "warning": 2, "info": 1}
    max_severity = max(
        (severity_order.get(str(alert.get("severity")), 0) for alert in alerts),
        default=0,
    )
    return {
        "status": "needs_attention" if alerts else "ok",
        "count": len(alerts),
        "critical_count": sum(1 for alert in alerts if alert.get("severity") == "critical"),
        "warning_count": sum(1 for alert in alerts if alert.get("severity") == "warning"),
        "max_severity": (
            "critical"
            if max_severity >= 3
            else "warning"
            if max_severity == 2
            else "info"
            if max_severity == 1
            else None
        ),
        "alerts": alerts[:MAX_STATUS_DETAIL_ITEMS],
        "alerts_count": len(alerts),
        "alerts_truncated": len(alerts) > MAX_STATUS_DETAIL_ITEMS,
    }


def _default_actionable_alerts_payload() -> dict[str, object]:
    return {
        "status": "unknown",
        "count": 0,
        "critical_count": 0,
        "warning_count": 0,
        "max_severity": None,
        "alerts": [],
        "alerts_count": 0,
        "alerts_truncated": False,
    }


def _stale_nonterminal_order_item(*, order: BrokerOrder) -> dict[str, object]:
    return {
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "symbol": order.symbol,
        "side": order.side,
        "status": order.status,
        "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
        "submitted_at": _serialize_datetime(order.submitted_at),
        "action": _stale_nonterminal_order_action(order),
        "reason": _stale_nonterminal_order_reason(order),
    }


def _stale_nonterminal_order_action(order: BrokerOrder) -> str:
    if (
        order.status in MANUAL_REVIEW_ORDER_STATUSES
        or not order.broker_order_id
        or order.status == "retry_pending"
    ):
        return "manual_review"
    return "reconcile_broker_truth"


def _stale_nonterminal_order_reason(order: BrokerOrder) -> str:
    if order.last_error_classification in UNTRUSTED_ACTIVE_ORDER_ERROR_CLASSIFICATIONS:
        return str(order.last_error_classification)
    if order.status in MANUAL_REVIEW_ORDER_STATUSES:
        return order.last_error_classification or "manual_review_required"
    if not order.broker_order_id:
        return "missing_broker_order_id"
    if order.status == "retry_pending":
        return "local_retry_pending_not_broker_live_state"
    if order.last_broker_sync_at is None:
        return "missing_broker_sync"
    return "stale_broker_sync"


def _position_has_quantity(position: ExecutionPosition) -> bool:
    if position.quantity > 0:
        return True
    if position.base_quantity is None:
        return False
    return Decimal(position.base_quantity) > 0


def _position_truth_evidence_symbols(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> RuntimePositionEvidenceSelection:
    observed_at = _as_utc(now or datetime.now(UTC))
    reserving_orders = list(
        db.scalars(
            select(BrokerOrder)
            .where(
                *active_local_broker_order_filters(
                    settings=settings,
                    statuses=CAPITAL_RESERVING_ORDER_STATUSES,
                    now=now,
                )
            )
            .options(undefer(BrokerOrder.request_metadata))
        ).all()
    )
    filled_orders = list(
        db.scalars(
            select(BrokerOrder)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.side.in_(("buy", "sell")),
                BrokerOrder.status == "filled",
            )
            .options(undefer(BrokerOrder.request_metadata))
        ).all()
    )
    reserving_selection = _runtime_order_scope_selection(
        settings=settings,
        orders=reserving_orders,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    filled_selection = _runtime_order_scope_selection(
        settings=settings,
        orders=filled_orders,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    fill_selection = _active_runtime_fills_for_period(
        settings=settings,
        db=db,
        start_at=None,
        end_at=observed_at + timedelta(microseconds=1),
        provider=provider,
        runtime_scope=runtime_scope,
    )
    symbols = {str(order.symbol) for order in reserving_selection.orders}
    fill_quantity_by_order_id: dict[str, Decimal] = {}
    for fill in fill_selection.fills:
        fill_quantity_by_order_id[fill.broker_order_pk] = (
            fill_quantity_by_order_id.get(fill.broker_order_pk, Decimal("0"))
            + _pnl_fill_quantity(fill)
        )
    incomplete_filled_orders = [
        order
        for order in filled_selection.orders
        if (
            _broker_order_expected_fill_quantity(order) is None
            or fill_quantity_by_order_id.get(order.id, Decimal("0"))
            != _broker_order_expected_fill_quantity(order)
        )
    ]
    # A filled order without complete fill rows is still exposure evidence.  It
    # must not disappear merely because a symbol-level buy/sell set happens to
    # cancel out.
    symbols.update(str(order.symbol) for order in incomplete_filled_orders)
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=observed_at,
    )
    inventory = replay_open_long_fill_inventory(
        fills=fill_selection.fills,
        isolation_cursor=isolation_cursor,
        through=observed_at,
    )
    symbols.update(symbol for (_route_scope, symbol) in inventory.open_quantities)
    return RuntimePositionEvidenceSelection(
        symbols=symbols,
        indeterminate_order_count=(
            reserving_selection.indeterminate_order_count
            + filled_selection.indeterminate_order_count
        ),
        indeterminate_fill_count=(
            fill_selection.indeterminate_fill_count
            + inventory.indeterminate_fill_count
            + len(incomplete_filled_orders)
        ),
        foreign_order_count=(
            reserving_selection.foreign_order_count
            + filled_selection.foreign_order_count
        ),
        scope_unavailable_reason=(
            reserving_selection.scope_unavailable_reason
            or filled_selection.scope_unavailable_reason
            or fill_selection.scope_unavailable_reason
        ),
    )


def _broker_order_expected_fill_quantity(order: BrokerOrder) -> Decimal | None:
    if order.base_quantity is not None and order.base_quantity > 0:
        return Decimal(order.base_quantity)
    if order.quantity is not None and order.quantity > 0:
        return Decimal(order.quantity)
    return None


def _duration_text(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    whole_seconds = max(int(seconds), 0)
    days, remainder = divmod(whole_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days:
        return f"{days}d{hours}h{minutes}m"
    if hours:
        return f"{hours}h{minutes}m"
    if minutes:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"


def runtime_symbol_summary(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _runtime_today_utc_bounds(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    failed_observed_at = func.coalesce(
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.submitted_at,
        BrokerOrder.updated_at,
    )
    monitored_symbols = runtime_monitored_symbols(
        settings=settings,
        db=db,
        today_start_utc=today_start_utc,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    scheduled_items = db.scalars(
        select(ExecutionPlanItem)
        .select_from(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(ACTIVE_JOB_STATUSES),
            ExecutionJob.scheduled_for.is_not(None),
            ExecutionJob.scheduled_for >= today_start_utc,
            ExecutionJob.scheduled_for < tomorrow_start_utc,
        )
        .options(
            load_only(
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
            )
        )
    ).all()
    scheduled_today = sorted(
        {
            item.symbol
            for item in scheduled_items
            if _active_runtime_plan_item_exchange(
                settings=settings,
                item=item,
                provider=provider,
                runtime_scope=runtime_scope,
            )
            is not None
        }
    )
    filled_summary = runtime_filled_today_summary(
        settings=settings,
        db=db,
        today_start_utc=today_start_utc,
        tomorrow_start_utc=tomorrow_start_utc,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    filled_today = sorted(
        {
            *filled_summary["by_side"]["buy"],
            *filled_summary["by_side"]["sell"],
        }
    )
    failed_orders = db.scalars(
        select(BrokerOrder)
        .select_from(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(FAILED_ORDER_STATUSES),
            failed_observed_at.is_not(None),
            failed_observed_at >= today_start_utc,
            failed_observed_at < tomorrow_start_utc,
        )
        .options(
            load_only(
                BrokerOrder.symbol,
                BrokerOrder.broker_name,
                BrokerOrder.mode,
                BrokerOrder.request_metadata,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
            )
        )
    ).all()
    failed_today = sorted(
        {
            order.symbol
            for order in failed_orders
            if _active_runtime_order_scope(
                settings=settings,
                order=order,
                provider=provider,
                runtime_scope=runtime_scope,
            )
            is not None
        }
    )
    position_symbols = _runtime_position_symbols(
        settings=settings,
        db=db,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    return {
        "monitored": monitored_symbols,
        "scheduled_today": scheduled_today,
        "filled_today": filled_today,
        "filled_today_by_side": filled_summary["by_side"],
        "filled_today_counts": filled_summary["counts"],
        "failed_today": failed_today,
        "positions": position_symbols,
    }


def _runtime_position_symbols(
    *,
    settings: Settings,
    db: Session,
    provider: str,
    runtime_scope: RuntimeExecutionScope | None,
) -> list[str]:
    return sorted(
        db.scalars(
            select(distinct(ExecutionPosition.symbol)).where(
                *_active_runtime_position_filters(
                    settings=settings,
                    provider=provider,
                    runtime_scope=runtime_scope,
                ),
                or_(
                    ExecutionPosition.quantity > 0,
                    ExecutionPosition.base_quantity > 0,
                ),
            )
        ).all()
    )


def runtime_monitored_symbols(
    *,
    settings: Settings,
    db: Session,
    today_start_utc: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
    allowed_exchanges: frozenset[str] | None = None,
    include_held_positions: bool = False,
    effective_at: datetime | None = None,
    include_pending_sell_fallback: bool = False,
) -> list[str]:
    if today_start_utc is None:
        today_start_utc, _ = _runtime_today_utc_bounds(
            settings=settings,
            provider=provider,
            runtime_scope=runtime_scope,
        )
    held_position_routes: set[tuple[str, str]] = set()
    held_position_symbols: set[str] = set()
    held_positions = db.execute(
        select(ExecutionPosition.symbol, ExecutionPosition.exchange).where(
            *_active_runtime_position_filters(
                settings=settings,
                provider=provider,
                runtime_scope=runtime_scope,
            ),
            or_(
                ExecutionPosition.quantity > 0,
                ExecutionPosition.base_quantity > 0,
            ),
        )
    ).all()
    for symbol, raw_exchange in held_positions:
        exchange = str(raw_exchange or "").strip().upper()
        if allowed_exchanges is not None and exchange not in allowed_exchanges:
            continue
        normalized_symbol = str(symbol)
        held_position_symbols.add(normalized_symbol)
        held_position_routes.add(
            (
                normalized_symbol,
                _runtime_membership_identity_exchange(
                    provider=provider,
                    exchange=exchange,
                ),
            )
        )
    sell_statuses = tuple(
        SELL_ACTIVE_MONITORING_STATUSES
        | SELL_PENDING_CONFIG_STATUSES
        | BLOCKED_MONITORING_STATUSES
        | PENDING_DISPATCH_MONITORING_STATUSES
    )
    pending_sell_symbol_query = None
    if include_pending_sell_fallback:
        # The scheduler deliberately lets pending protection survive a future
        # plan date. Keep that branch outside the active-watch date predicate.
        fresh_position_truth_after = datetime.now(UTC) - timedelta(
            seconds=settings.exec_agent_max_account_snapshot_age_seconds
        )
        fresh_known_flat_symbols = select(ExecutionPosition.symbol).where(
            *_active_runtime_position_filters(
                settings=settings,
                provider=provider,
                runtime_scope=runtime_scope,
            ),
            ExecutionPosition.quantity <= 0,
            ExecutionPosition.last_reconciled_at.is_not(None),
            ExecutionPosition.last_reconciled_at >= fresh_position_truth_after,
        )
        pending_item = aliased(ExecutionPlanItem)
        pending_plan = aliased(ExecutionPlan)
        pending_sell_symbol_query = (
            select(distinct(pending_item.symbol))
            .join(pending_plan, pending_item.execution_plan_id == pending_plan.id)
            .where(
                pending_plan.mode == settings.runtime_mode,
                pending_item.side == "sell",
                pending_item.intent.in_(SELL_MONITORED_INTENTS),
                pending_item.status.in_(tuple(SELL_PENDING_CONFIG_STATUSES)),
                pending_item.symbol.not_in(fresh_known_flat_symbols),
            )
            .order_by(pending_item.symbol.asc())
            .limit(resolve_monitoring_symbol_cycle_limit(settings=settings) * 4)
        )
    active_watch_filter = and_(
        ExecutionPlanItem.side == "buy",
        ExecutionPlanItem.intent.in_(WATCH_BUY_INTENTS),
        ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
    )
    standard_monitored_filter = (
        active_watch_filter
        if include_pending_sell_fallback
        else or_(
            and_(
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(SELL_MONITORED_INTENTS),
                ExecutionPlanItem.status.in_(sell_statuses),
            ),
            active_watch_filter,
        )
    )
    if effective_at is not None:
        standard_monitored_filter = and_(
            _effective_monitor_plan_filter(
                effective_at=effective_at,
                current_market_date=None,
            ),
            standard_monitored_filter,
        )
    monitored_filter = standard_monitored_filter
    if pending_sell_symbol_query is not None:
        monitored_filter = or_(
            standard_monitored_filter,
            and_(
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(SELL_MONITORED_INTENTS),
                ExecutionPlanItem.status.in_(tuple(SELL_PENDING_CONFIG_STATUSES)),
                ExecutionPlanItem.symbol.in_(pending_sell_symbol_query),
            ),
        )
    monitored_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_filter,
        )
        .options(
            load_only(
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.status,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
            ),
            contains_eager(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
            ),
        )
    ).all()
    monitored_symbols = (
        set(held_position_symbols) if include_held_positions else set()
    )
    pending_sell_symbols: set[str] = set()
    for item in monitored_items:
        if (
            include_pending_sell_fallback
            and item.side == "sell"
            and item.status in SELL_PENDING_CONFIG_STATUSES
        ):
            if _is_krx_equity_symbol(item.symbol):
                pending_sell_symbols.add(str(item.symbol))
            continue
        if (
            effective_at is not None
            and item.side == "buy"
            and not _monitoring_item_is_effective_at(
                item=item,
                effective_at=effective_at,
            )
        ):
            continue
        exchange = _active_runtime_plan_item_exchange(
            settings=settings,
            item=item,
            provider=provider,
            runtime_scope=runtime_scope,
        )
        if exchange is None:
            continue
        if allowed_exchanges is not None and exchange not in allowed_exchanges:
            continue
        if (
            item.side == "sell"
            and settings.runtime_mode == "live"
            and (
                item.symbol,
                _runtime_membership_identity_exchange(
                    provider=provider,
                    exchange=exchange,
                ),
            )
            not in held_position_routes
        ):
            continue
        monitored_symbols.add(item.symbol)
    if include_pending_sell_fallback:
        monitored_symbols.update(
            _runtime_pending_sell_fallback_symbols(
                settings=settings,
                db=db,
                pending_sell_symbols=sorted(pending_sell_symbols),
                existing_symbols=monitored_symbols,
            )
        )
    return sorted(monitored_symbols)


def _runtime_pending_sell_fallback_symbols(
    *,
    settings: Settings,
    db: Session,
    pending_sell_symbols: list[str],
    existing_symbols: set[str],
) -> list[str]:
    symbol_cycle_limit = resolve_monitoring_symbol_cycle_limit(settings=settings)
    if len(existing_symbols) >= symbol_cycle_limit:
        return []
    flat_symbols = (
        _symbols_with_recent_flat_position_truth(
            settings=settings,
            db=db,
            symbols=pending_sell_symbols,
        )
        if settings.runtime_mode == "live"
        else set()
    )
    unexited_buy_order_symbols = _symbols_with_unexited_buy_orders(
        settings=settings,
        db=db,
        symbols=pending_sell_symbols,
    )
    fallback_symbols: list[str] = []
    for symbol in pending_sell_symbols:
        if symbol in existing_symbols or symbol in flat_symbols:
            continue
        if symbol in unexited_buy_order_symbols:
            fallback_symbols.append(symbol)
        if len(existing_symbols) + len(fallback_symbols) >= symbol_cycle_limit:
            break
    return fallback_symbols


def _runtime_membership_identity_exchange(*, provider: str, exchange: str) -> str:
    normalized_exchange = str(exchange or "").strip().upper()
    if (
        _normalize_runtime_provider(provider) == "kis"
        and normalized_exchange in KRX_EXCHANGES
    ):
        return "KRX"
    return normalized_exchange


def _runtime_is_ascii_krx_symbol(symbol: object) -> bool:
    text = str(symbol or "")
    return len(text) == 6 and all("0" <= character <= "9" for character in text)


def _runtime_membership_indeterminate_position_count(
    *,
    settings: Settings,
    db: Session,
    provider: str,
    exchanges: frozenset[str],
) -> int:
    provider_names = RUNTIME_PROVIDER_BROKER_NAMES.get(provider, (provider,))
    count = db.scalar(
        select(func.count(ExecutionPosition.id)).where(
            ExecutionPosition.mode == settings.runtime_mode,
            or_(
                ExecutionPosition.quantity != 0,
                func.coalesce(ExecutionPosition.base_quantity, 0) != 0,
            ),
            or_(
                ExecutionPosition.provider.is_(None),
                func.lower(ExecutionPosition.provider).in_(provider_names),
            ),
            or_(
                ExecutionPosition.exchange.is_(None),
                func.upper(ExecutionPosition.exchange).in_(tuple(exchanges)),
            ),
            or_(
                ExecutionPosition.provider.is_(None),
                ExecutionPosition.account_scope.is_(None),
                ExecutionPosition.exchange.is_(None),
            ),
        )
    )
    return int(count or 0)


def build_runtime_monitored_symbol_membership(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    generated_at = datetime.now(UTC)
    provider = _normalize_runtime_provider(settings.runtime_broker_provider)
    runtime_scope = _resolve_runtime_execution_scope(
        settings=settings,
        provider=provider,
    )
    execution_scope = _runtime_execution_scope_payload(runtime_scope)
    membership_exchanges = frozenset(KRX_EXCHANGES)

    def payload(
        *,
        symbols: list[str],
        membership_complete: bool,
        reason: str | None,
    ) -> dict[str, object]:
        return {
            "schema_version": RUNTIME_MONITORED_SYMBOL_MEMBERSHIP_SCHEMA_VERSION,
            "market_date": generated_at.astimezone(KRX_TIMEZONE).date().isoformat(),
            "generated_at": generated_at.isoformat(),
            "membership_complete": membership_complete,
            "symbols": symbols,
            "membership_scope": {
                "market": "KRX",
                "exchanges": sorted(membership_exchanges),
            },
            "coverage": {
                "status": "complete" if membership_complete else "unavailable",
                "reason": reason,
                "provider": provider,
                "runtime_mode": settings.runtime_mode,
                "execution_scope": execution_scope,
            },
        }

    if provider != "kis":
        return payload(
            symbols=[],
            membership_complete=False,
            reason="runtime_membership_provider_unsupported",
        )

    if not runtime_scope.available:
        return payload(
            symbols=[],
            membership_complete=False,
            reason="runtime_execution_scope_unavailable",
        )

    try:
        symbols = runtime_monitored_symbols(
            settings=settings,
            db=db,
            provider=provider,
            runtime_scope=runtime_scope,
            allowed_exchanges=membership_exchanges,
            include_held_positions=True,
            effective_at=generated_at,
            include_pending_sell_fallback=True,
        )
        invalid_krx_symbols = [
            symbol for symbol in symbols if not _runtime_is_ascii_krx_symbol(symbol)
        ]
        symbols = [
            symbol for symbol in symbols if _runtime_is_ascii_krx_symbol(symbol)
        ]
        indeterminate_position_count = (
            _runtime_membership_indeterminate_position_count(
                settings=settings,
                db=db,
                provider=provider,
                exchanges=membership_exchanges,
            )
        )
    except SQLAlchemyError:
        db.rollback()
        return payload(
            symbols=[],
            membership_complete=False,
            reason="runtime_membership_query_unavailable",
        )

    if invalid_krx_symbols:
        return payload(
            symbols=symbols,
            membership_complete=False,
            reason="runtime_membership_invalid_krx_symbol",
        )
    if indeterminate_position_count:
        return payload(
            symbols=symbols,
            membership_complete=False,
            reason="runtime_membership_scope_blocked",
        )
    return payload(symbols=symbols, membership_complete=True, reason=None)


def runtime_filled_today_summary(
    *,
    settings: Settings,
    db: Session,
    today_start_utc: datetime | None = None,
    tomorrow_start_utc: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    if today_start_utc is None or tomorrow_start_utc is None:
        today_start_utc, tomorrow_start_utc = _runtime_today_utc_bounds(
            settings=settings,
            provider=provider,
            runtime_scope=runtime_scope,
        )
    selection = _active_runtime_fills_for_period(
        settings=settings,
        db=db,
        start_at=today_start_utc,
        end_at=tomorrow_start_utc,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    fills = selection.fills
    by_side: dict[str, list[str]] = {"buy": [], "sell": []}
    counts: dict[str, dict[str, int]] = {
        "all": {"symbols": 0, "fills": 0, "quantity": 0},
        "buy": {"symbols": 0, "fills": 0, "quantity": 0},
        "sell": {"symbols": 0, "fills": 0, "quantity": 0},
    }
    all_symbols: set[str] = set()
    side_symbols: dict[str, set[str]] = {"buy": set(), "sell": set()}
    for fill in sorted(fills, key=fifo_fill_sort_key):
        side = str(fill.side or "").strip().lower()
        symbol = str(fill.symbol or "").strip().upper()
        if side not in {"buy", "sell"} or not symbol:
            continue
        quantity = int(fill.quantity or 0)
        all_symbols.add(symbol)
        side_symbols[side].add(symbol)
        counts["all"]["fills"] += 1
        counts["all"]["quantity"] += quantity
        counts[side]["fills"] += 1
        counts[side]["quantity"] += quantity
    for side, symbols in side_symbols.items():
        by_side[side] = sorted(symbols)
        counts[side]["symbols"] = len(symbols)
    counts["all"]["symbols"] = len(all_symbols)
    return {"by_side": by_side, "counts": counts}


def runtime_filled_today_symbols(
    *,
    settings: Settings,
    db: Session,
    today_start_utc: datetime | None = None,
    tomorrow_start_utc: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> list[str]:
    if today_start_utc is None or tomorrow_start_utc is None:
        today_start_utc, tomorrow_start_utc = _runtime_today_utc_bounds(
            settings=settings,
            provider=provider,
            runtime_scope=runtime_scope,
        )
    selection = _active_runtime_fills_for_period(
        settings=settings,
        db=db,
        start_at=today_start_utc,
        end_at=tomorrow_start_utc,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    return sorted({str(fill.symbol) for fill in selection.fills})


def _current_pnl_day_utc_bounds(
    now: datetime | None = None,
    *,
    timezone: ZoneInfo,
) -> tuple[datetime, datetime]:
    current = (now or datetime.now(timezone)).astimezone(timezone)
    day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day_start = day_start + timedelta(days=1)
    return day_start.astimezone(UTC), next_day_start.astimezone(UTC)


def _current_krx_day_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    return _current_pnl_day_utc_bounds(now, timezone=KRX_TIMEZONE)


def monitoring_payload(
    *,
    settings: Settings,
    db: Session,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    items = list(
        db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(ExecutionPlan.mode == settings.runtime_mode)
        ).all()
    )
    if runtime_scope is not None:
        items = [
            item
            for item in items
            if _active_runtime_plan_item_exchange(
                settings=settings,
                item=item,
                provider=provider,
                runtime_scope=runtime_scope,
            )
            is not None
        ]
    segments = monitoring_segments(settings=settings, db=db, items=items)
    agent_decisions = monitoring_agent_decision_counts(
        settings=settings,
        db=db,
        items=items,
    )
    max_holding_actionability = max_holding_actionability_summary(
        settings=settings,
        db=db,
        mode=settings.runtime_mode,
        provider=(runtime_scope.provider if runtime_scope is not None else None),
        account_scope=(runtime_scope.account_scope if runtime_scope is not None else None),
        exchanges=(runtime_scope.exchanges if runtime_scope is not None else None),
    )
    backlog_items = [
        item
        for item in items
        if item.status in PENDING_DISPATCH_MONITORING_STATUSES
    ]
    backlog_symbols = sorted({item.symbol for item in backlog_items})
    backlog_item_count = len(backlog_items)
    mode_checkpoints = [
        checkpoint
        for checkpoint in db.scalars(select(MonitoringCheckpoint)).all()
        if checkpoint.mode == settings.runtime_mode
    ]
    checkpoints = [
        checkpoint
        for checkpoint in mode_checkpoints
        if runtime_scope is None
        or _monitoring_checkpoint_matches_runtime_scope(
            checkpoint=checkpoint,
            runtime_scope=runtime_scope,
        )
    ]
    excluded_checkpoint_count = len(mode_checkpoints) - len(checkpoints)
    if not checkpoints:
        return {
            "status": "not_started",
            "topology": MONITORING_RUNTIME_TOPOLOGY,
            "scheduler": {
                "selection_strategy": MONITORING_SELECTION_STRATEGY,
                "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
            },
            "dispatcher": {
                "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
                "last_cycle_action_count": 0,
                "backlog_item_count": backlog_item_count,
                "backlog_symbols": backlog_symbols,
            },
            "segments": segments,
            "agent_decisions": agent_decisions,
            "max_holding_actionability": max_holding_actionability,
            "execution_scope": (
                _runtime_execution_scope_payload(runtime_scope)
                if runtime_scope is not None
                else None
            ),
            "excluded_checkpoint_count": excluded_checkpoint_count,
            "workers": [],
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_cycle": None,
        }

    heartbeat_values = [
        checkpoint.last_heartbeat_at
        for checkpoint in checkpoints
        if checkpoint.last_heartbeat_at is not None
    ]
    success_values = [
        checkpoint.last_success_at
        for checkpoint in checkpoints
        if checkpoint.last_success_at is not None
    ]
    last_heartbeat = max(heartbeat_values).isoformat() if heartbeat_values else None
    last_success = max(success_values).isoformat() if success_values else None
    latest_checkpoint = max(
        checkpoints,
        key=lambda checkpoint: (
            checkpoint.last_heartbeat_at or checkpoint.last_success_at or datetime.min.replace(tzinfo=UTC)
        ),
    )
    latest_details = _compact_monitoring_cycle_payload(latest_checkpoint.details or {})
    protective_stop_coverage = latest_details.get("protective_stop_coverage")
    if not isinstance(protective_stop_coverage, dict):
        protective_stop_coverage = _unknown_protective_stop_coverage_payload()
    profit_target_coverage = latest_details.get("profit_target_coverage")
    if not isinstance(profit_target_coverage, dict):
        profit_target_coverage = _unknown_profit_target_coverage_payload()
    if backlog_item_count > 0 or any(checkpoint.status == "degraded" for checkpoint in checkpoints):
        status = "degraded"
    elif last_heartbeat:
        status = "running"
    else:
        status = "not_started"
    public_latest_details = _strip_account_scope(latest_details)
    assert isinstance(public_latest_details, dict)
    public_protective_stop_coverage = _strip_account_scope(protective_stop_coverage)
    public_profit_target_coverage = _strip_account_scope(profit_target_coverage)
    assert isinstance(public_protective_stop_coverage, dict)
    assert isinstance(public_profit_target_coverage, dict)
    return {
        "status": status,
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "scheduler": {
            "selection_strategy": MONITORING_SELECTION_STRATEGY,
            "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        },
        "dispatcher": {
            "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
            "last_cycle_action_count": int(latest_details.get("dispatch_action_count", 0) or 0),
            "backlog_item_count": backlog_item_count,
            "backlog_symbols": backlog_symbols,
        },
        "segments": segments,
        "agent_decisions": agent_decisions,
        "max_holding_actionability": max_holding_actionability,
        "execution_scope": (
            _runtime_execution_scope_payload(runtime_scope)
            if runtime_scope is not None
            else None
        ),
        "excluded_checkpoint_count": excluded_checkpoint_count,
        "workers": [
            {
                "name": checkpoint.worker_name,
                "status": checkpoint.status,
                "last_heartbeat_at": (
                    checkpoint.last_heartbeat_at.isoformat()
                    if checkpoint.last_heartbeat_at
                    else None
                ),
                "last_success_at": (
                    checkpoint.last_success_at.isoformat()
                    if checkpoint.last_success_at
                    else None
                ),
            }
            for checkpoint in checkpoints
        ],
        "last_heartbeat_at": last_heartbeat,
        "last_success_at": last_success,
        "protective_stop_coverage": public_protective_stop_coverage,
        "profit_target_coverage": public_profit_target_coverage,
        "last_cycle": public_latest_details or None,
    }


def _monitoring_checkpoint_matches_runtime_scope(
    *,
    checkpoint: MonitoringCheckpoint,
    runtime_scope: RuntimeExecutionScope,
) -> bool:
    details = checkpoint.details if isinstance(checkpoint.details, dict) else {}
    execution_scope = details.get("execution_scope")
    expected = {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": runtime_scope.provider,
        "account_scope": runtime_scope.account_scope,
    }
    return (
        runtime_scope.available
        and checkpoint.provider == runtime_scope.provider
        and checkpoint.account_scope == runtime_scope.account_scope
        and isinstance(execution_scope, dict)
        and set(execution_scope) == set(expected)
        and execution_scope == expected
    )


def monitoring_agent_decision_counts(
    *,
    settings: Settings,
    db: Session,
    items: list[ExecutionPlanItem] | None = None,
) -> dict[str, int]:
    if items is None:
        items = list(
            db.scalars(
                select(ExecutionPlanItem)
                .join(ExecutionPlanItem.plan)
                .where(ExecutionPlan.mode == settings.runtime_mode)
            ).all()
        )
    counts_by_status: dict[str, int] = {}
    for item in items:
        counts_by_status[item.status] = counts_by_status.get(item.status, 0) + 1
    watching = sum(counts_by_status.get(status, 0) for status in WATCHING_AGENT_COUNT_STATUSES)
    pending = counts_by_status.get(AGENT_PENDING_STATUS, 0)
    delayed = counts_by_status.get(AGENT_DELAY_STATUS, 0)
    rejected = counts_by_status.get(AGENT_REJECTED_STATUS, 0)
    approved_submitted = sum(
        counts_by_status.get(status, 0)
        for status in AGENT_APPROVED_SUBMITTED_STATUSES
    )
    return {
        "watching": watching,
        "pending_agent_decision": pending,
        "agent_delayed": delayed,
        "agent_rejected": rejected,
        "agent_approved_submitted": approved_submitted,
    }


def monitoring_segments(
    *,
    settings: Settings,
    db: Session,
    items: list[ExecutionPlanItem] | None = None,
) -> dict[str, object]:
    if items is None:
        items = list(
            db.scalars(
                select(ExecutionPlanItem)
                .join(ExecutionPlanItem.plan)
                .where(ExecutionPlan.mode == settings.runtime_mode)
            ).all()
        )
    held_sell = [
        item
        for item in items
        if item.side == "sell"
        and item.intent in SELL_MONITORED_INTENTS
        and (
            item.status.endswith("_monitoring")
            or item.status.endswith("_pending_dispatch")
            or item.status.endswith("_triggered_blocked")
        )
    ]
    short_term_watch = [
        item
        for item in items
        if _is_short_term_decider_item(item)
        and item.side == "buy"
        and item.intent in WATCH_BUY_INTENTS
        and item.status in WATCH_BUY_STATUSES
    ]
    signal_next_open = [
        item
        for item in items
        if item.side == "buy"
        and item.intent == "next_open_buy"
        and _normalized_agent(item.decision_agent_name) == SIGNAL_DECIDER_NAME
        and item.status.startswith("next_open_buy")
    ]
    blocked_short_term = [
        item
        for item in items
        if _is_short_term_decider_item(item)
        and item.side == "buy"
        and item.status in SHORT_TERM_BLOCKED_STATUSES
        and not (
            item.status == "watch_buy_triggered_blocked"
            and _risk_context_text(item, "last_monitoring_event", nested_key="reason") == "paused"
        )
    ]
    return {
        "held_position_sell_monitoring": _segment_payload(held_sell),
        "short_term_buy_watch_monitoring": _segment_payload(short_term_watch),
        "signal_decider_next_open_market_buys": _segment_payload(signal_next_open),
        "blocked_short_term_candidates": _segment_payload(blocked_short_term, include_blockers=True),
    }


def _segment_payload(
    items: list[ExecutionPlanItem],
    *,
    include_blockers: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "item_count": len(items),
        "symbols": sorted({item.symbol for item in items}),
    }
    if include_blockers:
        payload["items"] = [
            {
                "execution_plan_item_id": item.id,
                "symbol": item.symbol,
                "status": item.status,
                "tactical_action": _risk_context_text(item, "tactical_action"),
                "blocker_reason": _risk_context_text(item, "short_term_blocker_reason")
                or _risk_context_text(item, "last_monitoring_event", nested_key="reason"),
            }
            for item in sorted(items, key=lambda item: (item.symbol, item.id))
        ]
    return payload


def _is_short_term_decider_item(item: ExecutionPlanItem) -> bool:
    return _normalized_agent(item.decision_agent_name) == SHORT_TERM_DECIDER_NAME


def _normalized_agent(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().lower()


def _risk_context_text(
    item: ExecutionPlanItem,
    key: str,
    *,
    nested_key: str | None = None,
) -> str | None:
    if not isinstance(item.risk_context, dict):
        return None
    value = item.risk_context.get(key)
    if nested_key is not None:
        if not isinstance(value, dict):
            return None
        value = value.get(nested_key)
    if value is None:
        return None
    return str(value)


def readiness_payload(
    *,
    dependencies_ok: bool,
    safety: dict[str, object],
    counts_available: bool,
    extra_reasons: list[str] | None = None,
) -> dict[str, object]:
    reasons: list[str] = []
    if not dependencies_ok:
        reasons.append("database or runtime schema dependency is not ready")
    if safety["kill_switch_enabled"]:
        reasons.append("kill switch is enabled")
    if safety["paused"]:
        reasons.append("runtime is paused")
    if not counts_available:
        reasons.append("runtime counters are unavailable")
    if extra_reasons:
        reasons.extend(extra_reasons)

    return {
        "ready": not reasons,
        "reasons": reasons,
    }


def exec_agent_runtime_configuration(settings: Settings) -> dict[str, object]:
    """Expose configured decision-agent authority without implying liveness."""

    enabled = bool(getattr(settings, "exec_agent_enabled", False))
    trigger_configured = bool(
        getattr(settings, "exec_agent_trigger_time_enabled", False)
    )
    watch_configured = bool(
        getattr(settings, "exec_agent_watch_sizing_enabled", False)
    )
    return {
        "status": "enabled" if enabled else "disabled",
        "enabled": enabled,
        "agent_id": str(getattr(settings, "exec_agent_id", "") or ""),
        "expected_model": str(getattr(settings, "exec_agent_model", "") or ""),
        "provenance_policy": "exact_agent_and_model",
        "trigger_time": {
            "configured": trigger_configured,
            "effective": enabled and trigger_configured,
        },
        "watch_sizing": {
            "configured": watch_configured,
            "effective": enabled and watch_configured,
        },
        "reason_codes": [] if enabled else ["exec_agent_disabled"],
    }


def _blocking_readiness_reason(blocker: dict[str, object]) -> str:
    parts = [str(blocker.get("reason") or "runtime_schema_not_ready")]
    remediation = str(blocker.get("remediation") or "").strip()
    if remediation:
        parts.append(f"remediation={remediation}")
    if blocker.get("restart_required") is True:
        parts.append("restart_required=true")
    return "; ".join(parts)


def build_runtime_status(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    broker_adapter: BrokerAdapter | None = None,
    autonomous_runtime: dict[str, object] | None = None,
    include_expensive_sections: bool = True,
    runtime_scope: RuntimeExecutionScope | None = None,
    runtime_startup_gate: dict[str, object] | None = None,
) -> dict[str, object]:
    runtime_provider = _normalize_runtime_provider(
        getattr(broker_adapter, "provider", "kis")
    )
    active_runtime_scope = _runtime_scope_for_provider(
        settings=settings,
        provider=runtime_provider,
        runtime_scope=(
            runtime_scope
            or _resolve_runtime_execution_scope(
                settings=settings,
                provider=runtime_provider,
                broker_adapter=broker_adapter,
            )
        ),
    )
    runtime_scope_payload = _runtime_execution_scope_payload(active_runtime_scope)
    runtime_scope_reasons = (
        []
        if active_runtime_scope.available
        else [
            "runtime execution scope is unavailable: "
            f"{active_runtime_scope.reason or 'unknown_reason'}"
        ]
    )
    dependencies = {
        "database": "ok",
        "runtime_schema": "ok",
    }
    startup_gate_payload = (
        dict(runtime_startup_gate)
        if isinstance(runtime_startup_gate, dict)
        else {"status": "not_reported", "reason": None}
    )
    startup_gate_blocked = startup_gate_payload.get("status") == "blocked"
    if startup_gate_blocked and startup_gate_payload.get("remediation"):
        startup_gate_reasons = [
            "automated runtime startup is blocked: "
            f"{_blocking_readiness_reason(startup_gate_payload)}"
        ]
    elif startup_gate_blocked:
        startup_gate_reasons = [
            "automated runtime startup is blocked; migrate if needed and restart"
        ]
    else:
        startup_gate_reasons = []
    if isinstance(runtime_startup_gate, dict):
        dependencies["automated_runtime_startup"] = (
            "blocked" if startup_gate_blocked else "ok"
        )
    diagnostics = {
        "sqlite": sqlite_diagnostics_payload(),
    }
    counts: dict[str, int] = {"open_orders": 0, "active_execution_jobs": 0}
    watch_buy_vetoes: dict[str, object] = {"today_count": 0, "today_symbols": []}
    late_entry_gate: dict[str, object] = {
        "event_count": 0,
        "shadow_count": 0,
        "block_count": 0,
        "symbols": [],
    }
    account_drawdown_gate: dict[str, object] = {
        "available": False,
        "enabled": settings.account_drawdown_gate_enabled,
        "gated": None,
        "provider": runtime_provider,
        "latest_decision": None,
        "active_recovery_scopes": {},
        "reason": "account_drawdown_gate_state_unavailable",
    }
    autonomous = (
        autonomous_runtime
        if isinstance(autonomous_runtime, dict)
        else _default_autonomous_runtime_payload(settings)
    )
    autonomous_scope_reasons = (
        _autonomous_runtime_snapshot_scope_reasons(
            autonomous=autonomous,
            runtime_scope=active_runtime_scope,
        )
        if settings.autonomous_runtime_enabled
        else []
    )
    autonomous_snapshot_trusted = not autonomous_scope_reasons
    public_autonomous = _public_autonomous_runtime_payload(
        autonomous=autonomous,
        snapshot_trusted=autonomous_snapshot_trusted,
        scope_reasons=autonomous_scope_reasons,
    )
    monitoring_price_source = _autonomous_monitoring_price_source(autonomous)
    monitoring: dict[str, object] = _monitoring_payload_from_autonomous(
        settings=settings,
        autonomous=autonomous,
        monitoring_price_source=monitoring_price_source,
        snapshot_trusted=autonomous_snapshot_trusted,
        scope_reason=(autonomous_scope_reasons[0] if autonomous_scope_reasons else None),
    )
    symbols: dict[str, object] = {
        "monitored": [],
        "scheduled_today": [],
        "filled_today": [],
        "filled_today_by_side": {"buy": [], "sell": []},
        "filled_today_counts": {
            "all": {"symbols": 0, "fills": 0, "quantity": 0},
            "buy": {"symbols": 0, "fills": 0, "quantity": 0},
            "sell": {"symbols": 0, "fills": 0, "quantity": 0},
        },
        "failed_today": [],
        "positions": [],
    }
    monthly_pnl = _default_monthly_pnl_payload(
        settings=settings,
        provider=runtime_provider,
        runtime_scope=active_runtime_scope,
    )
    daily_pnl = _default_daily_pnl_payload(
        settings=settings,
        provider=runtime_provider,
        runtime_scope=active_runtime_scope,
    )
    weekly_pnl = _default_weekly_pnl_payload(
        settings=settings,
        provider=runtime_provider,
        runtime_scope=active_runtime_scope,
    )
    broker = build_broker_session_summary(
        settings=settings,
        db=None,
        adapter=broker_adapter,
    )
    broker_providers = broker_provider_readiness(settings=settings, db=None)
    freshness: dict[str, object] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "monitoring": {
            "status": monitoring.get("status"),
            "last_heartbeat_at": monitoring.get("last_heartbeat_at"),
            "last_success_at": monitoring.get("last_success_at"),
            "source": monitoring.get("source") or "autonomous_runtime_snapshot",
        },
        "position_truth": {
            "stale_count": 0,
            "oldest_age_seconds": None,
            "oldest_age": None,
            "last_reconciled_at": None,
            "oldest_reconciled_at": None,
            "broker_truth_reachable": False,
            "stale_symbols": [],
            "max_age_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
            "tracked_symbol_count": 0,
            "evidence_symbol_count": 0,
            "nonzero_position_count": 0,
            "manual_review_count": 0,
            "manual_review_symbols": [],
            "manual_review_reasons": {},
            "trusted_flat_count": 0,
        },
        "broker_orders": {
            "latest_broker_sync_at": None,
            "latest_reconciliation_event_at": None,
            "stale_nonterminal": {
                "count": 0,
                "threshold_seconds": settings.stale_broker_order_reconciliation_threshold_seconds,
                "stale_before": None,
                "symbols": [],
                "needs_reconciliation_count": 0,
                "manual_review_count": 0,
                "missing_broker_order_id_count": 0,
                "items": [],
                "truncated": False,
            },
        },
    }
    counts_available = True

    fast_runtime_schema: dict[str, object] | None = None
    if settings.autonomous_runtime_enabled and not include_expensive_sections:
        fast_runtime_schema = check_runtime_schema(db)
        dependencies["runtime_schema"] = str(fast_runtime_schema["status"])

    if (
        settings.autonomous_runtime_enabled
        and not include_expensive_sections
        and fast_runtime_schema is not None
        and fast_runtime_schema["status"] == "ok"
        and not startup_gate_blocked
    ):
        counts = runtime_counts(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        watch_buy_vetoes = runtime_watch_buy_veto_summary(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        late_entry_gate = runtime_late_entry_summary(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        account_drawdown_gate = _runtime_account_drawdown_gate_state(
            settings=settings,
            db=db,
            provider=runtime_provider,
        )
        cached_symbols = (
            _autonomous_runtime_snapshot_symbol_summary(autonomous=autonomous)
            if autonomous_snapshot_trusted
            else {}
        )
        cycle_symbols = (
            _autonomous_runtime_snapshot_symbols(autonomous=autonomous)
            or _autonomous_monitoring_symbols(autonomous=autonomous)
            if autonomous_snapshot_trusted
            else []
        )
        filled_summary = runtime_filled_today_summary(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        symbols = {
            **symbols,
            **cached_symbols,
            "filled_today": runtime_filled_today_symbols(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            ),
            "filled_today_by_side": filled_summary["by_side"],
            "filled_today_counts": filled_summary["counts"],
            "monitored": cycle_symbols,
            "monitoring_cycle_batch": cycle_symbols,
        }
        freshness["position_truth"] = position_truth_freshness_payload(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        freshness["summary_source"] = "fast_autonomous_runtime_snapshot"
        monitoring_reasons = _monitoring_price_source_readiness_reasons(
            autonomous=autonomous,
            symbols=symbols,
        )
        if monitoring_reasons:
            monitoring["status"] = "degraded"
        safety = fast_autonomous_safety(settings=settings, db=db)
        readiness = readiness_payload(
            dependencies_ok=all(value == "ok" for value in dependencies.values()),
            safety=safety,
            counts_available=counts_available,
            extra_reasons=[
                *monitoring_reasons,
                *runtime_scope_reasons,
                *autonomous_scope_reasons,
                *startup_gate_reasons,
            ],
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "execution_scope": runtime_scope_payload,
            "exec_agent": exec_agent_runtime_configuration(settings),
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "runtime_startup_gate": startup_gate_payload,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": public_autonomous,
                "monitoring": monitoring,
            },
            "broker": broker,
            "broker_providers": broker_providers,
            "counts": counts,
            "watch_buy_vetoes": watch_buy_vetoes,
            "late_entry_gate": late_entry_gate,
            "symbols": symbols,
            "freshness": freshness,
            "actionable_alerts": runtime_actionable_alerts_payload(
                monitoring=monitoring,
                freshness=freshness,
            ),
            "monthly_pnl": _default_monthly_pnl_payload(
                settings=settings,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            ),
            "weekly_pnl": _default_weekly_pnl_payload(
                settings=settings,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            ),
            "daily_pnl": daily_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            ),
            "account_drawdown_gate": account_drawdown_gate,
        }

    try:
        ping_database(db)
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["database"] = "error"
        dependencies["runtime_schema"] = "unknown"
        counts_available = False
        safety = default_safety(
            settings,
            f"database unavailable: {exc.__class__.__name__}",
        )
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
            extra_reasons=startup_gate_reasons,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "execution_scope": runtime_scope_payload,
            "exec_agent": exec_agent_runtime_configuration(settings),
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "runtime_startup_gate": startup_gate_payload,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": public_autonomous,
                "monitoring": monitoring,
            },
            "broker": broker,
            "broker_providers": broker_providers,
            "counts": counts,
            "watch_buy_vetoes": watch_buy_vetoes,
            "late_entry_gate": late_entry_gate,
            "symbols": symbols,
            "freshness": freshness,
            "actionable_alerts": _default_actionable_alerts_payload(),
            "monthly_pnl": monthly_pnl,
            "weekly_pnl": weekly_pnl,
            "daily_pnl": daily_pnl,
            "account_drawdown_gate": account_drawdown_gate,
        }

    schema = check_runtime_schema(db)
    dependencies["runtime_schema"] = str(schema["status"])
    if schema["status"] != "ok":
        counts_available = False
        position_readiness = execution_position_readiness(schema)
        missing_tables = schema.get("missing_tables") or []
        missing_columns = schema.get("missing_columns") or {}
        if position_readiness is not None:
            schema_reason = _blocking_readiness_reason(position_readiness)
        elif missing_tables:
            schema_reason = (
                "runtime schema incomplete: "
                f"missing {', '.join(str(table) for table in missing_tables)}; run migrations"
            )
        elif isinstance(missing_columns, dict) and missing_columns:
            missing_column_names = [
                f"{table}.{column}"
                for table, columns in missing_columns.items()
                if isinstance(columns, list)
                for column in columns
            ]
            schema_reason = (
                "runtime schema incomplete: missing "
                f"{', '.join(missing_column_names)}; run migrations"
            )
        else:
            schema_reason = "runtime schema unavailable; run migrations"
        safety = default_safety(settings, schema_reason)
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
            extra_reasons=startup_gate_reasons,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "execution_scope": runtime_scope_payload,
            "exec_agent": exec_agent_runtime_configuration(settings),
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "runtime_startup_gate": startup_gate_payload,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": public_autonomous,
                "monitoring": monitoring,
            },
            "broker": broker,
            "broker_providers": broker_providers,
            "counts": counts,
            "watch_buy_vetoes": watch_buy_vetoes,
            "late_entry_gate": late_entry_gate,
            "symbols": symbols,
            "freshness": freshness,
            "monthly_pnl": monthly_pnl,
            "weekly_pnl": weekly_pnl,
            "daily_pnl": daily_pnl,
            "account_drawdown_gate": account_drawdown_gate,
        }

    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
        safety = runtime_safety_payload(state)
        counts = runtime_counts(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        watch_buy_vetoes = runtime_watch_buy_veto_summary(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        late_entry_gate = runtime_late_entry_summary(
            settings=settings,
            db=db,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        if include_expensive_sections:
            broker = build_broker_session_summary(
                settings=settings,
                db=db,
                adapter=broker_adapter,
            )
            broker_providers = broker_provider_readiness(settings=settings, db=db)
            account_drawdown_gate = _runtime_account_drawdown_gate_state(
                settings=settings,
                db=db,
                provider=runtime_provider,
            )
            symbols = runtime_symbol_summary(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
        if include_expensive_sections:
            monitoring = monitoring_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            monitoring["price_source"] = monitoring_price_source
            freshness = runtime_freshness_payload(
                settings=settings,
                db=db,
                monitoring=monitoring,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            monthly_pnl = monthly_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            weekly_pnl = weekly_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            daily_pnl = daily_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
        elif settings.autonomous_runtime_enabled:
            cached_symbols = _autonomous_runtime_snapshot_symbol_summary(autonomous=autonomous)
            if "filled_today" not in cached_symbols:
                cached_symbols["filled_today"] = runtime_filled_today_symbols(
                    settings=settings,
                    db=db,
                    provider=runtime_provider,
                    runtime_scope=active_runtime_scope,
                )
            if "filled_today_by_side" not in cached_symbols or "filled_today_counts" not in cached_symbols:
                filled_summary = runtime_filled_today_summary(
                    settings=settings,
                    db=db,
                    provider=runtime_provider,
                    runtime_scope=active_runtime_scope,
                )
                cached_symbols["filled_today_by_side"] = filled_summary["by_side"]
                cached_symbols["filled_today_counts"] = filled_summary["counts"]
            symbols = {
                **symbols,
                **cached_symbols,
                "monitored": runtime_monitored_symbols(
                    settings=settings,
                    db=db,
                    provider=runtime_provider,
                    runtime_scope=active_runtime_scope,
                ),
                "monitoring_cycle_batch": _autonomous_monitoring_symbols(autonomous=autonomous),
            }
            freshness["summary_source"] = "fast_autonomous_runtime_snapshot"
            monthly_pnl = _default_monthly_pnl_payload(
                settings=settings,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            weekly_pnl = _default_weekly_pnl_payload(
                settings=settings,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            daily_pnl = _default_daily_pnl_payload(
                settings=settings,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
        else:
            freshness["summary_source"] = "fast_runtime_status"
            monthly_pnl = monthly_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            weekly_pnl = weekly_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
            daily_pnl = daily_pnl_payload(
                settings=settings,
                db=db,
                provider=runtime_provider,
                runtime_scope=active_runtime_scope,
            )
        actionable_alerts = runtime_actionable_alerts_payload(
            monitoring=monitoring,
            freshness=freshness,
        )
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["runtime_schema"] = "error"
        counts_available = False
        safety = default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )
        monthly_pnl = _default_monthly_pnl_payload(
            settings=settings,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        weekly_pnl = _default_weekly_pnl_payload(
            settings=settings,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        daily_pnl = _default_daily_pnl_payload(
            settings=settings,
            provider=runtime_provider,
            runtime_scope=active_runtime_scope,
        )
        actionable_alerts = _default_actionable_alerts_payload()

    monitoring_reasons = _monitoring_price_source_readiness_reasons(
        autonomous=autonomous,
        symbols=symbols,
    )
    if monitoring_reasons:
        monitoring["status"] = "degraded"

    readiness = readiness_payload(
        dependencies_ok=all(value == "ok" for value in dependencies.values()),
        safety=safety,
        counts_available=counts_available,
        extra_reasons=[
            *monitoring_reasons,
            *runtime_scope_reasons,
            *autonomous_scope_reasons,
            *startup_gate_reasons,
        ],
    )
    return {
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": safety["mode"],
        "execution_scope": runtime_scope_payload,
        "exec_agent": exec_agent_runtime_configuration(settings),
        "ready": readiness["ready"],
        "readiness": readiness,
        "safety": safety,
        "dependencies": dependencies,
        "runtime_startup_gate": startup_gate_payload,
        "diagnostics": diagnostics,
        "liveness": {
            **liveness_payload(runtime),
            "autonomous": public_autonomous,
            "monitoring": monitoring,
        },
        "broker": broker,
        "broker_providers": broker_providers,
        "counts": counts,
        "watch_buy_vetoes": watch_buy_vetoes,
        "late_entry_gate": late_entry_gate,
        "symbols": symbols,
        "freshness": freshness,
        "actionable_alerts": actionable_alerts,
        "monthly_pnl": monthly_pnl,
        "weekly_pnl": weekly_pnl,
        "daily_pnl": daily_pnl,
        "account_drawdown_gate": account_drawdown_gate,
    }


def broker_provider_readiness(
    *,
    settings: Settings,
    db: Session | None,
) -> dict[str, object]:
    providers: dict[str, object] = {}
    for provider in ("kis", "upbit", "alpaca"):
        adapter = build_broker_adapter(settings, provider=provider)
        providers[provider] = build_broker_session_summary(
            settings=settings,
            db=db,
            adapter=adapter,
        )
    return providers


def monthly_pnl_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    pnl_scope, pnl_timezone = _runtime_pnl_scope(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    month_start, next_month_start = _current_pnl_month_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    summary = _period_realized_pnl_summary(
        settings=settings,
        db=db,
        start_at=month_start,
        end_at=next_month_start,
        provider=provider,
        runtime_scope=pnl_scope,
    )
    if summary.get("scope_unavailable_reason") or int(
        summary.get("scope_unavailable_fill_count") or 0
    ) > 0:
        return _default_monthly_pnl_payload(
            settings=settings,
            now=now,
            provider=provider,
            runtime_scope=pnl_scope,
            reason=str(
                summary.get("scope_unavailable_reason")
                or "execution_scope_unavailable"
            ),
        )

    target = getattr(settings, "monthly_target_krw", Decimal("500000"))
    economics = _pnl_economics_payload(
        summary["economics_rows"],
        unmatched_sell_quantity_by_currency=summary[
            "unmatched_sell_quantity_by_currency"
        ],
        unknown_currency_unmatched_sell_quantity=summary[
            "unknown_currency_unmatched_sell_quantity"
        ],
    )
    notionals = summary["notionals"]
    monthly_gross = economics["gross_realized_pnl_krw"]
    monthly_net = economics["net_realized_pnl_krw"]
    progress = (
        (Decimal(str(monthly_net)) / target * Decimal("100")).quantize(Decimal("0.01"))
        if monthly_net is not None and target > 0
        else None
    )
    expected_progress = _expected_month_progress_pct(
        now,
        timezone=pnl_timezone,
    )
    recommendation = (
        _monthly_mode_recommendation(
            settings=settings,
            progress_pct=progress,
            expected_progress_pct=expected_progress,
        )
        if progress is not None
        else "insufficient_net_pnl_data"
    )
    return {
        "month": month_start.astimezone(pnl_timezone).strftime("%Y-%m"),
        "timezone": pnl_timezone.key,
        "monthly_realized_pnl": _decimal_text(monthly_gross),
        "monthly_gross_realized_pnl_krw": _decimal_text(monthly_gross),
        "monthly_net_realized_pnl_krw": _decimal_text(monthly_net),
        "monthly_target_krw": _decimal_text(target),
        "target_progress_pct": _decimal_text(progress),
        "expected_calendar_progress_pct": _decimal_text(expected_progress),
        "mode_recommendation": recommendation,
        "monthly_buy_notional": notionals["buy_notional_krw"],
        "monthly_sell_notional": notionals["sell_notional_krw"],
        "matched_sell_quantity": _quantity_value(summary["matched_sell_quantity"]),
        "unmatched_sell_quantity": _quantity_value(summary["unmatched_sell_quantity"]),
        "cost_basis_sources": summary["cost_basis_sources"],
        "unknown_cost_basis_sell_quantity": _quantity_value(
            summary["unknown_cost_basis_sell_quantity"]
        ),
        "stale_fifo_inventory_alerts": summary["stale_fifo_inventory_alerts"],
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills_with_broker_reconciliation_fallback",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def daily_pnl_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    pnl_scope, pnl_timezone = _runtime_pnl_scope(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    day_start, next_day_start = _current_pnl_day_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    selection = _active_runtime_fills_for_period(
        settings=settings,
        db=db,
        start_at=None,
        end_at=next_day_start,
        provider=provider,
        runtime_scope=pnl_scope,
    )
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=next_day_start,
    )
    chronology_ambiguous_fill_count = ambiguous_fifo_realized_pnl_sell_count(
        selection.fills,
        isolation_cursor=isolation_cursor,
        start_at=day_start,
        end_at=next_day_start,
    )
    if (
        selection.scope_unavailable_reason is not None
        or selection.indeterminate_fill_count > 0
        or chronology_ambiguous_fill_count > 0
    ):
        return _default_daily_pnl_payload(
            settings=settings,
            now=now,
            provider=provider,
            runtime_scope=pnl_scope,
            reason=(
                selection.scope_unavailable_reason
                or (
                    "indeterminate_fill_chronology"
                    if chronology_ambiguous_fill_count > 0
                    else "indeterminate_execution_fill_scope"
                )
            ),
        )
    fills = selection.fills

    lots_by_route_symbol: dict[
        tuple[FifoRouteScope, str],
        list[dict[str, object]],
    ] = {}
    realized = Decimal("0")
    buy_notional_by_currency: dict[str, Decimal] = {}
    sell_notional_by_currency: dict[str, Decimal] = {}
    unknown_currency_buy_notional = Decimal("0")
    unknown_currency_sell_notional = Decimal("0")
    matched_sell_quantity = Decimal("0")
    unmatched_sell_quantity = Decimal("0")
    closed_symbols: set[str] = set()
    sold_symbols: set[str] = set()
    unmatched_sell_symbols: set[str] = set()
    unmatched_sell_quantity_by_currency: dict[str, Decimal] = {}
    unknown_currency_unmatched_sell_quantity = Decimal("0")
    economics_rows: list[ClosedTradeEconomics] = []
    for fill in sorted(fills, key=fifo_fill_sort_key):
        filled_at = _as_utc(fill.filled_at)
        side = fill.side.lower()
        symbol = fill.symbol
        quantity = _pnl_fill_quantity(fill)
        notional = _pnl_fill_notional(fill=fill, quantity=quantity)
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        lots = (
            lots_by_route_symbol.setdefault((route_scope, symbol), [])
            if route_scope is not None
            else []
        )
        scope_quality_available = (
            isolation_cursor.apply_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=filled_at,
                lots=lots,
            )
            if route_scope is not None
            else False
        )
        if side == "buy":
            if scope_quality_available:
                lots.append(
                    {
                        "quantity": quantity,
                        "price": fill.price,
                        "filled_at": filled_at,
                        "fill": fill,
                    }
                )
            if filled_at >= day_start:
                currency = execution_fill_economics(fill).settlement_currency
                if currency is None:
                    unknown_currency_buy_notional += notional
                else:
                    buy_notional_by_currency[currency] = (
                        buy_notional_by_currency.get(currency, Decimal("0")) + notional
                    )
            continue
        if side != "sell":
            continue
        if filled_at >= day_start:
            currency = execution_fill_economics(fill).settlement_currency
            if currency is None:
                unknown_currency_sell_notional += notional
            else:
                sell_notional_by_currency[currency] = (
                    sell_notional_by_currency.get(currency, Decimal("0")) + notional
                )
            sold_symbols.add(symbol)
        remaining = quantity
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, lot["quantity"])
            if filled_at >= day_start:
                gross_pnl = ((fill.price - lot["price"]) * matched).quantize(
                    Decimal("0.0001")
                )
                realized += gross_pnl
                economics_rows.append(
                    closed_trade_economics(
                        buy_fill=lot["fill"],
                        sell_fill=fill,
                        matched_quantity=matched,
                        gross_realized_pnl=gross_pnl,
                    )
                )
                matched_sell_quantity += matched
                closed_symbols.add(symbol)
            lot["quantity"] -= matched
            remaining -= matched
            if lot["quantity"] <= 0:
                lots.pop(0)
        if remaining > 0 and filled_at >= day_start:
            unmatched_sell_quantity += remaining
            unmatched_sell_symbols.add(symbol)
            unmatched_currency = execution_fill_economics(fill).settlement_currency
            if unmatched_currency is None:
                unknown_currency_unmatched_sell_quantity += remaining
            else:
                unmatched_sell_quantity_by_currency[unmatched_currency] = (
                    unmatched_sell_quantity_by_currency.get(
                        unmatched_currency, Decimal("0")
                    )
                    + remaining
                )

    economics = _pnl_economics_payload(
        economics_rows,
        unmatched_sell_quantity_by_currency=unmatched_sell_quantity_by_currency,
        unknown_currency_unmatched_sell_quantity=(
            unknown_currency_unmatched_sell_quantity
        ),
    )
    notionals = _pnl_notional_payload(
        buy_by_currency=buy_notional_by_currency,
        sell_by_currency=sell_notional_by_currency,
        unknown_buy=unknown_currency_buy_notional,
        unknown_sell=unknown_currency_sell_notional,
    )
    return {
        "date": day_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "timezone": pnl_timezone.key,
        "daily_realized_pnl": _decimal_text(economics["gross_realized_pnl_krw"]),
        "daily_gross_realized_pnl_krw": _decimal_text(
            economics["gross_realized_pnl_krw"]
        ),
        "daily_net_realized_pnl_krw": _decimal_text(
            economics["net_realized_pnl_krw"]
        ),
        "daily_buy_notional": notionals["buy_notional_krw"],
        "daily_sell_notional": notionals["sell_notional_krw"],
        "matched_sell_quantity": _quantity_value(matched_sell_quantity),
        "unmatched_sell_quantity": _quantity_value(unmatched_sell_quantity),
        "sold_symbols": sorted(sold_symbols),
        "sold_symbol_count": len(sold_symbols),
        "closed_symbols": sorted(closed_symbols),
        "closed_symbol_count": len(closed_symbols),
        "unmatched_sell_symbols": sorted(unmatched_sell_symbols),
        "unmatched_sell_symbol_count": len(unmatched_sell_symbols),
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def weekly_pnl_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    pnl_scope, pnl_timezone = _runtime_pnl_scope(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    week_start, next_week_start = _current_pnl_week_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    summary = _period_realized_pnl_summary(
        settings=settings,
        db=db,
        start_at=week_start,
        end_at=next_week_start,
        provider=provider,
        runtime_scope=pnl_scope,
    )
    if summary.get("scope_unavailable_reason") or int(
        summary.get("scope_unavailable_fill_count") or 0
    ) > 0:
        return _default_weekly_pnl_payload(
            settings=settings,
            now=now,
            provider=provider,
            runtime_scope=pnl_scope,
            reason=str(
                summary.get("scope_unavailable_reason")
                or "execution_scope_unavailable"
            ),
        )

    economics = _pnl_economics_payload(
        summary["economics_rows"],
        unmatched_sell_quantity_by_currency=summary[
            "unmatched_sell_quantity_by_currency"
        ],
        unknown_currency_unmatched_sell_quantity=summary[
            "unknown_currency_unmatched_sell_quantity"
        ],
    )
    notionals = summary["notionals"]
    return {
        "week_start": week_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "week_end_exclusive": next_week_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "timezone": pnl_timezone.key,
        "weekly_realized_pnl": _decimal_text(economics["gross_realized_pnl_krw"]),
        "weekly_gross_realized_pnl_krw": _decimal_text(
            economics["gross_realized_pnl_krw"]
        ),
        "weekly_net_realized_pnl_krw": _decimal_text(
            economics["net_realized_pnl_krw"]
        ),
        "weekly_buy_notional": notionals["buy_notional_krw"],
        "weekly_sell_notional": notionals["sell_notional_krw"],
        "matched_sell_quantity": _quantity_value(summary["matched_sell_quantity"]),
        "unmatched_sell_quantity": _quantity_value(summary["unmatched_sell_quantity"]),
        "closed_trade_count": summary["closed_trade_count"],
        "closed_symbols": summary["closed_symbols"],
        "closed_symbol_count": len(summary["closed_symbols"]),
        "unmatched_sell_symbols": summary["unmatched_sell_symbols"],
        "unmatched_sell_symbol_count": len(summary["unmatched_sell_symbols"]),
        "win_count": summary["win_count"],
        "loss_count": summary["loss_count"],
        "breakeven_count": summary["breakeven_count"],
        "cost_basis_sources": summary["cost_basis_sources"],
        "unknown_cost_basis_sell_quantity": _quantity_value(summary["unknown_cost_basis_sell_quantity"]),
        "stale_fifo_inventory_alerts": summary["stale_fifo_inventory_alerts"],
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills_with_broker_reconciliation_fallback",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def _period_realized_pnl_summary(
    *,
    settings: Settings,
    db: Session,
    start_at: datetime,
    end_at: datetime,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[str, object]:
    selection = _active_runtime_fills_for_period(
        settings=settings,
        db=db,
        start_at=None,
        end_at=end_at,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=end_at,
    )
    chronology_ambiguous_fill_count = ambiguous_fifo_realized_pnl_sell_count(
        selection.fills,
        isolation_cursor=isolation_cursor,
        start_at=start_at,
        end_at=end_at,
    )
    if (
        selection.scope_unavailable_reason is not None
        or selection.indeterminate_fill_count > 0
        or chronology_ambiguous_fill_count > 0
    ):
        return {
            "scope_unavailable_fill_count": (
                selection.indeterminate_fill_count
                + chronology_ambiguous_fill_count
            ),
            "scope_unavailable_reason": (
                selection.scope_unavailable_reason
                or (
                    "indeterminate_fill_chronology"
                    if chronology_ambiguous_fill_count > 0
                    else "indeterminate_execution_fill_scope"
                )
            ),
        }
    fills = selection.fills
    sell_symbols = {
        fill.symbol
        for fill in fills
        if str(fill.side or "").lower() == "sell" and start_at <= _as_utc(fill.filled_at) < end_at
    }
    positions_by_route_symbol = _pnl_positions_by_route_symbol(
        settings=settings,
        db=db,
        symbols=sell_symbols,
        provider=provider,
        runtime_scope=runtime_scope,
    )

    lots_by_route_symbol: dict[
        tuple[FifoRouteScope, str],
        list[dict[str, object]],
    ] = {}
    realized = Decimal("0")
    buy_notional_by_currency: dict[str, Decimal] = {}
    sell_notional_by_currency: dict[str, Decimal] = {}
    unknown_currency_buy_notional = Decimal("0")
    unknown_currency_sell_notional = Decimal("0")
    matched_sell_quantity = Decimal("0")
    unmatched_sell_quantity = Decimal("0")
    closed_symbols: set[str] = set()
    unmatched_sell_symbols: set[str] = set()
    win_count = 0
    loss_count = 0
    breakeven_count = 0
    cost_basis_sources = {"fill_fifo": 0, "broker_reconciliation": 0, "unknown": 0}
    unknown_cost_basis_sell_quantity = Decimal("0")
    unmatched_sell_quantity_by_currency: dict[str, Decimal] = {}
    unknown_currency_unmatched_sell_quantity = Decimal("0")
    economics_rows: list[ClosedTradeEconomics] = []
    for fill in sorted(fills, key=fifo_fill_sort_key):
        filled_at = _as_utc(fill.filled_at)
        side = fill.side.lower()
        symbol = fill.symbol
        quantity = _pnl_fill_quantity(fill)
        notional = _pnl_fill_notional(fill=fill, quantity=quantity)
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        lots = (
            lots_by_route_symbol.setdefault((route_scope, symbol), [])
            if route_scope is not None
            else []
        )
        scope_quality_available = (
            isolation_cursor.apply_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=filled_at,
                lots=lots,
            )
            if route_scope is not None
            else False
        )
        if side == "buy":
            if scope_quality_available:
                lots.append(
                    {
                        "quantity": quantity,
                        "price": fill.price,
                        "filled_at": filled_at,
                        "fill": fill,
                    }
                )
            if filled_at >= start_at:
                currency = execution_fill_economics(fill).settlement_currency
                if currency is None:
                    unknown_currency_buy_notional += notional
                else:
                    buy_notional_by_currency[currency] = (
                        buy_notional_by_currency.get(currency, Decimal("0")) + notional
                    )
            continue
        if side != "sell":
            continue
        in_period = filled_at >= start_at
        next_buy_after_sell = _pnl_next_buy_after(
            fills=fills,
            route_scope=route_scope,
            symbol=symbol,
            after=filled_at,
        )
        if in_period:
            currency = execution_fill_economics(fill).settlement_currency
            if currency is None:
                unknown_currency_sell_notional += notional
            else:
                sell_notional_by_currency[currency] = (
                    sell_notional_by_currency.get(currency, Decimal("0")) + notional
                )
        remaining = quantity
        while remaining > 0 and lots:
            lot = lots[0]
            lot_quantity = Decimal(str(lot["quantity"]))
            matched = min(remaining, lot_quantity)
            cost_price = Decimal(str(lot["price"]))
            cost_source = "fill_fifo"
            position = _pnl_position_for_route(
                positions_by_route_symbol=positions_by_route_symbol,
                route_scope=route_scope,
                symbol=symbol,
            )
            broker_cost_basis = _pnl_broker_reconciliation_cost_basis(
                position=position,
                sold_at=filled_at,
                next_buy_after_sell=next_buy_after_sell,
            )
            if in_period and _as_utc(lot["filled_at"]) < start_at and broker_cost_basis is not None:
                cost_price = broker_cost_basis
                cost_source = "broker_reconciliation"
            pnl = ((fill.price - cost_price) * matched).quantize(Decimal("0.0001"))
            if in_period:
                realized += pnl
                economics_rows.append(
                    closed_trade_economics(
                        buy_fill=(
                            None
                            if cost_source == "broker_reconciliation"
                            else lot["fill"]
                        ),
                        sell_fill=fill,
                        matched_quantity=matched,
                        gross_realized_pnl=pnl,
                    )
                )
                matched_sell_quantity += matched
                closed_symbols.add(symbol)
                cost_basis_sources[cost_source] += 1
                if pnl > 0:
                    win_count += 1
                elif pnl < 0:
                    loss_count += 1
                else:
                    breakeven_count += 1
            lot["quantity"] = lot_quantity - matched
            remaining -= matched
            if Decimal(str(lot["quantity"])) <= 0:
                lots.pop(0)
        if remaining > 0 and in_period:
            position = _pnl_position_for_route(
                positions_by_route_symbol=positions_by_route_symbol,
                route_scope=route_scope,
                symbol=symbol,
            )
            broker_cost_basis = _pnl_broker_reconciliation_cost_basis(
                position=position,
                sold_at=filled_at,
                next_buy_after_sell=next_buy_after_sell,
            )
            if broker_cost_basis is not None:
                pnl = ((fill.price - broker_cost_basis) * remaining).quantize(Decimal("0.0001"))
                realized += pnl
                economics_rows.append(
                    closed_trade_economics(
                        buy_fill=None,
                        sell_fill=fill,
                        matched_quantity=remaining,
                        gross_realized_pnl=pnl,
                    )
                )
                matched_sell_quantity += remaining
                closed_symbols.add(symbol)
                cost_basis_sources["broker_reconciliation"] += 1
                if pnl > 0:
                    win_count += 1
                elif pnl < 0:
                    loss_count += 1
                else:
                    breakeven_count += 1
            else:
                unmatched_sell_quantity += remaining
                unknown_cost_basis_sell_quantity += remaining
                unmatched_sell_symbols.add(symbol)
                cost_basis_sources["unknown"] += 1
                unmatched_currency = execution_fill_economics(fill).settlement_currency
                if unmatched_currency is None:
                    unknown_currency_unmatched_sell_quantity += remaining
                else:
                    unmatched_sell_quantity_by_currency[unmatched_currency] = (
                        unmatched_sell_quantity_by_currency.get(
                            unmatched_currency, Decimal("0")
                        )
                        + remaining
                    )

    for (route_scope, symbol), lots in lots_by_route_symbol.items():
        isolation_cursor.apply_through(
            route_scope=route_scope,
            symbol=symbol,
            observed_at=end_at,
            lots=lots,
        )

    return {
        "scope_unavailable_fill_count": 0,
        "scope_unavailable_reason": None,
        "realized": realized,
        "notionals": _pnl_notional_payload(
            buy_by_currency=buy_notional_by_currency,
            sell_by_currency=sell_notional_by_currency,
            unknown_buy=unknown_currency_buy_notional,
            unknown_sell=unknown_currency_sell_notional,
        ),
        "matched_sell_quantity": matched_sell_quantity,
        "unmatched_sell_quantity": unmatched_sell_quantity,
        "unknown_cost_basis_sell_quantity": unknown_cost_basis_sell_quantity,
        "closed_trade_count": win_count + loss_count + breakeven_count,
        "closed_symbols": sorted(closed_symbols),
        "unmatched_sell_symbols": sorted(unmatched_sell_symbols),
        "win_count": win_count,
        "loss_count": loss_count,
        "breakeven_count": breakeven_count,
        "cost_basis_sources": cost_basis_sources,
        "economics_rows": economics_rows,
        "unmatched_sell_quantity_by_currency": unmatched_sell_quantity_by_currency,
        "unknown_currency_unmatched_sell_quantity": (
            unknown_currency_unmatched_sell_quantity
        ),
        "stale_fifo_inventory_alerts": _stale_fifo_inventory_alerts(
            lots_by_route_symbol=lots_by_route_symbol,
            positions_by_route_symbol=positions_by_route_symbol,
        ),
    }


def _pnl_broker_reconciliation_cost_basis(
    *,
    position: ExecutionPosition | None,
    sold_at: datetime,
    next_buy_after_sell: datetime | None,
) -> Decimal | None:
    if (
        position is not None
        and position.average_price is not None
        and position.average_price > 0
        and position.last_reconciled_at is not None
        and "reconciliation" in str(position.updated_from or "")
        and _position_effective_quantity(position) <= 0
    ):
        reconciled_at = _as_utc(position.last_reconciled_at)
        sold_at = _as_utc(sold_at)
        if reconciled_at >= sold_at and (next_buy_after_sell is None or reconciled_at < next_buy_after_sell):
            return Decimal(position.average_price)
    return None


def _pnl_next_buy_after(
    *,
    fills: list[ExecutionFill],
    route_scope: FifoRouteScope | None,
    symbol: str,
    after: datetime,
) -> datetime | None:
    later_buys = [
        _as_utc(fill.filled_at)
        for fill in fills
        if fill.symbol == symbol
        and fifo_route_scope_from_order(fill.broker_order) == route_scope
        and str(fill.side or "").lower() == "buy"
        and _as_utc(fill.filled_at) > after
    ]
    return min(later_buys) if later_buys else None


def _pnl_position_matches_route(
    *,
    position: ExecutionPosition | None,
    route_scope: FifoRouteScope | None,
) -> bool:
    if position is None or route_scope is None:
        return False
    try:
        identity = position_identity_from_columns(position)
    except PositionIdentityConflictError:
        return False
    return bool(
        identity is not None
        and identity.provider == route_scope.provider
        and identity.account_scope == route_scope.account_scope
        and identity.exchange == route_scope.exchange
    )


PnlPositionRouteKey = tuple[str, str, str, str]


def _pnl_position_route_key(position: ExecutionPosition) -> PnlPositionRouteKey | None:
    try:
        identity = position_identity_from_columns(position)
    except PositionIdentityConflictError:
        return None
    if identity is None:
        return None
    return (
        identity.provider,
        identity.account_scope,
        identity.exchange,
        position.symbol,
    )


def _pnl_positions_by_route_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
    provider: str | None = None,
    runtime_scope: RuntimeExecutionScope | None = None,
) -> dict[PnlPositionRouteKey, ExecutionPosition]:
    if not symbols:
        return {}
    positions_by_route_symbol: dict[PnlPositionRouteKey, ExecutionPosition] = {}
    route_filters = (
        _active_runtime_position_filters(
            settings=settings,
            provider=provider,
            runtime_scope=runtime_scope,
        )
        if provider is not None
        else (
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider.is_not(None),
            ExecutionPosition.account_scope.is_not(None),
            ExecutionPosition.exchange.is_not(None),
        )
    )
    positions = db.scalars(
        select(ExecutionPosition).where(
            *route_filters,
            ExecutionPosition.symbol.in_(sorted(symbols)),
        )
    ).all()
    for position in positions:
        key = _pnl_position_route_key(position)
        if key is None:
            continue
        if key in positions_by_route_symbol:
            raise PositionIdentityConflictError(
                "multiple execution positions exist for one PnL route and symbol"
            )
        positions_by_route_symbol[key] = position
    return positions_by_route_symbol


def _pnl_position_for_route(
    *,
    positions_by_route_symbol: dict[PnlPositionRouteKey, ExecutionPosition],
    route_scope: FifoRouteScope | None,
    symbol: str,
) -> ExecutionPosition | None:
    if route_scope is None:
        return None
    return positions_by_route_symbol.get(
        (
            route_scope.provider,
            route_scope.account_scope,
            route_scope.exchange,
            symbol,
        )
    )


def _stale_fifo_inventory_alerts(
    *,
    lots_by_route_symbol: dict[
        tuple[FifoRouteScope, str],
        list[dict[str, object]],
    ],
    positions_by_route_symbol: dict[PnlPositionRouteKey, ExecutionPosition],
) -> list[dict[str, object]]:
    alerts: list[dict[str, object]] = []
    for (route_scope, symbol), lots in sorted(
        lots_by_route_symbol.items(),
        key=lambda item: (
            item[0][1],
            item[0][0].provider,
            item[0][0].account_scope,
            item[0][0].venue,
            item[0][0].asset_class,
        ),
    ):
        fifo_quantity = sum((Decimal(str(lot["quantity"])) for lot in lots), Decimal("0"))
        if fifo_quantity <= 0:
            continue
        position = _pnl_position_for_route(
            positions_by_route_symbol=positions_by_route_symbol,
            route_scope=route_scope,
            symbol=symbol,
        )
        if (
            position is None
            or position.last_reconciled_at is None
            or not _pnl_position_matches_route(
                position=position,
                route_scope=route_scope,
            )
        ):
            continue
        position_quantity = _position_effective_quantity(position)
        if position_quantity > 0:
            continue
        alerts.append(
            {
                "kind": "stale_fill_fifo_inventory_flat_broker_position",
                "symbol": symbol,
                "execution_scope": route_scope.as_payload(),
                "fifo_open_quantity": _quantity_value(fifo_quantity),
                "broker_position_quantity": _quantity_value(position_quantity),
                "position_last_reconciled_at": _serialize_datetime(position.last_reconciled_at),
                "message": "fill FIFO leaves open quantity but latest broker reconciliation reports flat",
            }
        )
    return alerts


def _position_effective_quantity(position: ExecutionPosition) -> Decimal:
    if position.base_quantity is not None:
        return Decimal(position.base_quantity)
    return Decimal(position.quantity)


def _pnl_fill_quantity(fill: ExecutionFill) -> Decimal:
    if fill.base_quantity is not None and fill.base_quantity > 0:
        return Decimal(fill.base_quantity)
    return Decimal(fill.quantity)


def _pnl_fill_notional(*, fill: ExecutionFill, quantity: Decimal) -> Decimal:
    if fill.quote_notional is not None and fill.quote_notional > 0:
        return Decimal(fill.quote_notional).quantize(Decimal("0.0001"))
    return (fill.price * quantity).quantize(Decimal("0.0001"))


def _pnl_economics_payload(
    rows: list[ClosedTradeEconomics],
    *,
    unmatched_sell_quantity_by_currency: dict[str, Decimal] | None = None,
    unknown_currency_unmatched_sell_quantity: Decimal = Decimal("0"),
) -> dict[str, object]:
    unmatched_by_currency = {
        str(currency): Decimal(str(quantity))
        for currency, quantity in (unmatched_sell_quantity_by_currency or {}).items()
        if Decimal(str(quantity)) > 0
    }
    unknown_unmatched_quantity = Decimal(
        str(unknown_currency_unmatched_sell_quantity)
    )
    summary = summarize_closed_trade_economics(rows)
    raw_by_currency = summary["by_currency"]
    by_currency: dict[str, dict[str, object]] = {}
    if isinstance(raw_by_currency, dict):
        for currency, raw_bucket in raw_by_currency.items():
            if not isinstance(raw_bucket, dict):
                continue
            by_currency[str(currency)] = {
                **raw_bucket,
                "gross_realized_pnl": _decimal_text(
                    raw_bucket.get("gross_realized_pnl")
                ),
                "known_transaction_cost": _decimal_text(
                    raw_bucket.get("known_transaction_cost")
                ),
                "net_realized_pnl": _decimal_text(raw_bucket.get("net_realized_pnl")),
                "covered_net_realized_pnl": _decimal_text(
                    raw_bucket.get("covered_net_realized_pnl")
                ),
            }

    for currency, quantity in sorted(unmatched_by_currency.items()):
        bucket = by_currency.setdefault(
            currency,
            {
                "currency": currency,
                "gross_realized_pnl": None,
                "known_transaction_cost": None,
                "net_realized_pnl": None,
                "covered_net_realized_pnl": None,
                "transaction_cost_coverage": "unknown",
                "closed_lot_count": 0,
                "complete_cost_lot_count": 0,
            },
        )
        bucket["covered_gross_realized_pnl"] = bucket.get(
            "gross_realized_pnl"
        )
        bucket["gross_realized_pnl"] = None
        bucket["net_realized_pnl"] = None
        bucket["pnl_coverage"] = "incomplete_unknown_cost_basis"
        bucket["unmatched_sell_quantity"] = _quantity_value(quantity)

    no_pnl_activity = (
        not rows
        and not unmatched_by_currency
        and unknown_unmatched_quantity <= 0
    )
    krw = raw_by_currency.get("KRW") if isinstance(raw_by_currency, dict) else None
    has_krw = isinstance(krw, dict)
    krw_cost_basis_incomplete = (
        "KRW" in unmatched_by_currency
        or unknown_unmatched_quantity > 0
        or int(summary["unknown_currency_lot_count"]) > 0
    )
    covered_gross_krw = (
        Decimal(str(krw["gross_realized_pnl"])) if has_krw else None
    )
    if no_pnl_activity:
        gross_krw: Decimal | None = Decimal("0")
        net_krw: Decimal | None = Decimal("0")
        status = "no_closed_trades"
        krw_cost_coverage = "complete"
    else:
        gross_krw = (
            covered_gross_krw
            if has_krw and not krw_cost_basis_incomplete
            else None
        )
        net_value = krw.get("net_realized_pnl") if has_krw else None
        net_krw = (
            Decimal(str(net_value))
            if net_value is not None and not krw_cost_basis_incomplete
            else None
        )
        krw_cost_coverage = (
            str(krw.get("transaction_cost_coverage")) if has_krw else "unknown"
        )
        if krw_cost_basis_incomplete:
            krw_cost_coverage = (
                "partial"
                if krw_cost_coverage in {"complete", "partial"}
                else "unknown"
            )
        known_currencies = set(by_currency)
        if (
            int(summary["unknown_currency_lot_count"]) > 0
            or unknown_unmatched_quantity > 0
        ):
            status = "degraded_unknown_currency"
        elif len(known_currencies) > 1:
            status = "degraded_mixed_currency"
        elif unmatched_by_currency:
            status = "degraded_unknown_cost_basis"
        elif krw_cost_coverage != "complete" and has_krw:
            status = "degraded_transaction_cost_coverage"
        elif any(
            isinstance(bucket, dict)
            and bucket.get("transaction_cost_coverage") != "complete"
            for bucket in raw_by_currency.values()
        ):
            status = "degraded_transaction_cost_coverage"
        else:
            status = "complete"

    row_coverages = {row.transaction_cost_coverage for row in rows}
    pnl_cost_basis_incomplete = bool(unmatched_by_currency) or (
        unknown_unmatched_quantity > 0
    )
    cost_coverage = (
        "complete"
        if no_pnl_activity or (
            not pnl_cost_basis_incomplete and row_coverages == {"complete"}
        )
        else (
            "partial"
            if row_coverages & {"complete", "partial"}
            else "unknown"
        )
    )

    return {
        "gross_realized_pnl_krw": gross_krw,
        "covered_gross_realized_pnl_krw": covered_gross_krw,
        "net_realized_pnl_krw": net_krw,
        "realized_pnl_by_currency": by_currency,
        "currency_coverage": (
            "complete"
            if summary["currency_coverage"] == "complete"
            and unknown_unmatched_quantity <= 0
            else "incomplete"
        ),
        "unknown_currency_lot_count": summary["unknown_currency_lot_count"],
        "unknown_currency_unmatched_sell_quantity": _quantity_value(
            unknown_unmatched_quantity
        ),
        "unmatched_sell_quantity_by_currency": {
            currency: _quantity_value(quantity)
            for currency, quantity in sorted(unmatched_by_currency.items())
        },
        "pnl_cost_basis_coverage": (
            "incomplete" if pnl_cost_basis_incomplete else "complete"
        ),
        "unknown_currency_gross_realized_pnl": _decimal_text(
            summary["unknown_currency_gross_realized_pnl"]
        ),
        "transaction_cost_coverage": cost_coverage,
        "krw_transaction_cost_coverage": krw_cost_coverage,
        "profitability_status": status,
        "calculation_status": "computed",
        "calculation_reason": None,
        "legacy_realized_pnl_basis": (
            "gross_before_transaction_costs"
            if gross_krw is not None
            else "unavailable_incomplete_cost_basis_or_currency"
        ),
    }


def _pnl_notional_payload(
    *,
    buy_by_currency: dict[str, Decimal],
    sell_by_currency: dict[str, Decimal],
    unknown_buy: Decimal,
    unknown_sell: Decimal,
) -> dict[str, object]:
    currencies = sorted(set(buy_by_currency) | set(sell_by_currency))
    no_activity = not currencies and unknown_buy == 0 and unknown_sell == 0
    buy_krw = buy_by_currency.get("KRW")
    sell_krw = sell_by_currency.get("KRW")
    if "KRW" in currencies:
        buy_krw = buy_krw or Decimal("0")
        sell_krw = sell_krw or Decimal("0")
    if no_activity:
        buy_krw = sell_krw = Decimal("0")
    return {
        "buy_notional_krw": _decimal_text(buy_krw),
        "sell_notional_krw": _decimal_text(sell_krw),
        "buy_notional_by_currency": {
            currency: _decimal_text(value)
            for currency, value in sorted(buy_by_currency.items())
        },
        "sell_notional_by_currency": {
            currency: _decimal_text(value)
            for currency, value in sorted(sell_by_currency.items())
        },
        "unknown_currency_buy_notional": _decimal_text(unknown_buy),
        "unknown_currency_sell_notional": _decimal_text(unknown_sell),
        "notional_currency_coverage": (
            "complete" if unknown_buy == 0 and unknown_sell == 0 else "incomplete"
        ),
        "notional_status": (
            "no_activity"
            if no_activity
            else (
                "degraded_unknown_currency"
                if unknown_buy > 0 or unknown_sell > 0
                else ("mixed_currency_subtotals" if len(currencies) > 1 else "complete")
            )
        ),
    }


def _unavailable_pnl_economics_payload(*, reason: str) -> dict[str, object]:
    """Represent a skipped/failed calculation without claiming verified zero PnL."""

    return {
        "gross_realized_pnl_krw": None,
        "covered_gross_realized_pnl_krw": None,
        "net_realized_pnl_krw": None,
        "realized_pnl_by_currency": {},
        "currency_coverage": "not_computed",
        "unknown_currency_lot_count": None,
        "unknown_currency_unmatched_sell_quantity": None,
        "unmatched_sell_quantity_by_currency": {},
        "pnl_cost_basis_coverage": "not_computed",
        "unknown_currency_gross_realized_pnl": None,
        "transaction_cost_coverage": "not_computed",
        "krw_transaction_cost_coverage": "not_computed",
        "profitability_status": "degraded_not_computed",
        "calculation_status": "not_computed",
        "calculation_reason": reason,
        "legacy_realized_pnl_basis": "unavailable_not_computed",
    }


def _unavailable_pnl_notional_payload() -> dict[str, object]:
    return {
        "buy_notional_krw": None,
        "sell_notional_krw": None,
        "buy_notional_by_currency": {},
        "sell_notional_by_currency": {},
        "unknown_currency_buy_notional": None,
        "unknown_currency_sell_notional": None,
        "notional_currency_coverage": "not_computed",
        "notional_status": "not_computed",
    }


def _default_monthly_pnl_payload(
    *,
    settings: Settings,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
    reason: str = "monthly_pnl_not_computed",
) -> dict[str, object]:
    target = getattr(settings, "monthly_target_krw", Decimal("500000"))
    economics = _unavailable_pnl_economics_payload(reason=reason)
    notionals = _unavailable_pnl_notional_payload()
    pnl_timezone = _runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    month_start, _next_month_start = _current_pnl_month_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    return {
        "month": month_start.astimezone(pnl_timezone).strftime("%Y-%m"),
        "timezone": pnl_timezone.key,
        "monthly_realized_pnl": None,
        "monthly_gross_realized_pnl_krw": None,
        "monthly_net_realized_pnl_krw": None,
        "monthly_target_krw": _decimal_text(target),
        "target_progress_pct": None,
        "expected_calendar_progress_pct": _decimal_text(
            _expected_month_progress_pct(now, timezone=pnl_timezone)
        ),
        "mode_recommendation": "insufficient_net_pnl_data",
        "monthly_buy_notional": None,
        "monthly_sell_notional": None,
        "matched_sell_quantity": None,
        "unmatched_sell_quantity": None,
        "cost_basis_sources": {"fill_fifo": 0, "broker_reconciliation": 0, "unknown": 0},
        "unknown_cost_basis_sell_quantity": None,
        "stale_fifo_inventory_alerts": [],
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills_with_broker_reconciliation_fallback",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def _default_daily_pnl_payload(
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
    reason: str = "daily_pnl_not_computed",
) -> dict[str, object]:
    economics = _unavailable_pnl_economics_payload(reason=reason)
    notionals = _unavailable_pnl_notional_payload()
    pnl_timezone = _runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    day_start, _next_day_start = _current_pnl_day_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    return {
        "date": day_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "timezone": pnl_timezone.key,
        "daily_realized_pnl": None,
        "daily_gross_realized_pnl_krw": None,
        "daily_net_realized_pnl_krw": None,
        "daily_buy_notional": None,
        "daily_sell_notional": None,
        "matched_sell_quantity": None,
        "unmatched_sell_quantity": None,
        "sold_symbols": [],
        "sold_symbol_count": 0,
        "closed_symbols": [],
        "closed_symbol_count": 0,
        "unmatched_sell_symbols": [],
        "unmatched_sell_symbol_count": 0,
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def _default_weekly_pnl_payload(
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
    provider: str = "kis",
    runtime_scope: RuntimeExecutionScope | None = None,
    reason: str = "weekly_pnl_not_computed",
) -> dict[str, object]:
    pnl_timezone = _runtime_pnl_timezone(
        settings=settings,
        provider=provider,
        runtime_scope=runtime_scope,
    )
    week_start, next_week_start = _current_pnl_week_utc_bounds(
        now,
        timezone=pnl_timezone,
    )
    economics = _unavailable_pnl_economics_payload(reason=reason)
    notionals = _unavailable_pnl_notional_payload()
    return {
        "week_start": week_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "week_end_exclusive": next_week_start.astimezone(pnl_timezone).strftime("%Y-%m-%d"),
        "timezone": pnl_timezone.key,
        "weekly_realized_pnl": None,
        "weekly_gross_realized_pnl_krw": None,
        "weekly_net_realized_pnl_krw": None,
        "weekly_buy_notional": None,
        "weekly_sell_notional": None,
        "matched_sell_quantity": None,
        "unmatched_sell_quantity": None,
        "closed_trade_count": None,
        "closed_symbols": [],
        "closed_symbol_count": 0,
        "unmatched_sell_symbols": [],
        "unmatched_sell_symbol_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "breakeven_count": 0,
        "cost_basis_sources": {"fill_fifo": 0, "broker_reconciliation": 0, "unknown": 0},
        "unknown_cost_basis_sell_quantity": None,
        "stale_fifo_inventory_alerts": [],
        **economics,
        **notionals,
        "pnl_method": "fifo_internal_fills_with_broker_reconciliation_fallback",
        "pnl_economics_method": "gross_by_currency_with_explicit_transaction_cost_coverage",
    }


def _current_pnl_week_utc_bounds(
    now: datetime | None = None,
    *,
    timezone: ZoneInfo,
) -> tuple[datetime, datetime]:
    current = (now or datetime.now(timezone)).astimezone(timezone)
    week_start = (current - timedelta(days=current.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    next_week = week_start + timedelta(days=7)
    return week_start.astimezone(UTC), next_week.astimezone(UTC)


def _current_krx_week_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    return _current_pnl_week_utc_bounds(now, timezone=KRX_TIMEZONE)


def _current_pnl_month_utc_bounds(
    now: datetime | None = None,
    *,
    timezone: ZoneInfo,
) -> tuple[datetime, datetime]:
    current = (now or datetime.now(timezone)).astimezone(timezone)
    month_start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return month_start.astimezone(UTC), next_month.astimezone(UTC)


def _current_krx_month_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    return _current_pnl_month_utc_bounds(now, timezone=KRX_TIMEZONE)


def _expected_month_progress_pct(
    now: datetime | None = None,
    *,
    timezone: ZoneInfo = KRX_TIMEZONE,
) -> Decimal:
    current = (now or datetime.now(timezone)).astimezone(timezone)
    if current.month == 12:
        next_month = current.replace(year=current.year + 1, month=1, day=1)
    else:
        next_month = current.replace(month=current.month + 1, day=1)
    days_in_month = (next_month.date() - current.replace(day=1).date()).days
    return (Decimal(current.day) / Decimal(days_in_month) * Decimal("100")).quantize(Decimal("0.01"))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _monthly_mode_recommendation(
    *,
    settings: Settings,
    progress_pct: Decimal,
    expected_progress_pct: Decimal,
) -> str:
    if getattr(settings, "monthly_capital_preservation_enabled", True) and progress_pct >= Decimal("100"):
        return "capital_preservation"
    lag_ratio = getattr(settings, "monthly_aggressive_lag_ratio", Decimal("0.75"))
    if progress_pct < expected_progress_pct * Decimal(str(lag_ratio)):
        return "aggressive"
    return "normal"


def _decimal_text(value: object) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _quantity_value(value: Decimal) -> int | str:
    if value == value.to_integral_value():
        return int(value)
    return format(value.normalize(), "f")


def build_runtime_daily_board(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    broker_adapter: BrokerAdapter | None = None,
    autonomous_runtime: dict[str, object] | None = None,
    runtime_startup_gate: dict[str, object] | None = None,
) -> dict[str, object]:
    runtime_provider = _normalize_runtime_provider(
        getattr(broker_adapter, "provider", "kis")
    )
    runtime_scope = _resolve_runtime_execution_scope(
        settings=settings,
        provider=runtime_provider,
        broker_adapter=broker_adapter,
    )
    runtime_status = build_runtime_status(
        settings=settings,
        runtime=runtime,
        db=db,
        broker_adapter=broker_adapter,
        autonomous_runtime=autonomous_runtime,
        include_expensive_sections=not settings.autonomous_runtime_enabled,
        runtime_scope=runtime_scope,
        runtime_startup_gate=runtime_startup_gate,
    )
    symbols: list[dict[str, object]] = []
    if runtime_status["dependencies"]["database"] == "ok" and runtime_status["dependencies"]["runtime_schema"] == "ok":
        try:
            monitored_symbols = runtime_status.get("symbols", {}).get("monitored", [])
            status_symbols = runtime_status.get("symbols", {})
            if isinstance(monitored_symbols, list) and isinstance(status_symbols, dict):
                normalized_symbols = set()
                for key in ("monitored", "scheduled_today", "filled_today", "failed_today", "positions"):
                    values = status_symbols.get(key, [])
                    if isinstance(values, list):
                        normalized_symbols.update(str(symbol) for symbol in values)
                if settings.autonomous_runtime_enabled:
                    normalized_symbols.update(
                        _runtime_position_symbols(
                            settings=settings,
                            db=db,
                            provider=runtime_scope.provider,
                            runtime_scope=runtime_scope,
                        )
                    )
                vetoes = runtime_status.get("watch_buy_vetoes", {})
                if isinstance(vetoes, dict) and isinstance(vetoes.get("today_symbols"), list):
                    normalized_symbols.update(str(symbol) for symbol in vetoes["today_symbols"])
                symbols = list_runtime_symbols(
                    settings=settings,
                    db=db,
                    symbols=normalized_symbols,
                    provider=runtime_scope.provider,
                    account_scope=runtime_scope.account_scope,
                    exchanges=runtime_scope.exchanges,
                )
            else:
                symbols = list_runtime_symbols(
                    settings=settings,
                    db=db,
                    provider=runtime_scope.provider,
                    account_scope=runtime_scope.account_scope,
                    exchanges=runtime_scope.exchanges,
                )
        except SQLAlchemyError:
            db.rollback()
            symbols = []

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "market_date": datetime.now(KRX_TIMEZONE).date().isoformat(),
        "mode": settings.runtime_mode,
        "runtime": runtime_status,
        "account_drawdown_gate": _runtime_account_drawdown_gate_state(
            settings=settings,
            db=db,
            provider=runtime_scope.provider,
        ),
        "watch_buy_vetoes": runtime_status.get("watch_buy_vetoes", {"today_count": 0, "today_symbols": []}),
        "totals": {
            "symbol_count": len(symbols),
            "monitored_symbol_count": sum(symbol["monitored"] for symbol in symbols),
            "open_order_symbol_count": sum(symbol["open_order_count"] > 0 for symbol in symbols),
            "filled_symbol_count": sum(symbol["fill_count"] > 0 for symbol in symbols),
            "position_symbol_count": sum(
                symbol["position_quantity"] > 0
                or _positive_decimal_value(symbol.get("position_base_quantity"))
                for symbol in symbols
            ),
        },
        "symbols": symbols,
    }


def _positive_decimal_value(value: object) -> bool:
    if value is None:
        return False
    try:
        return Decimal(str(value)) > 0
    except Exception:
        return False


def _monitoring_payload_from_autonomous(
    *,
    settings: Settings,
    autonomous: dict[str, object],
    monitoring_price_source: dict[str, object],
    snapshot_trusted: bool,
    scope_reason: str | None,
) -> dict[str, object]:
    autonomous_monitoring = autonomous.get("monitoring")
    if not isinstance(autonomous_monitoring, dict):
        autonomous_monitoring = {}
    last_cycle = autonomous_monitoring.get("last_cycle") if snapshot_trusted else None
    if not isinstance(last_cycle, dict):
        last_cycle = None
    else:
        last_cycle = _compact_monitoring_cycle_payload(last_cycle)
        stripped_last_cycle = _strip_account_scope(last_cycle)
        last_cycle = stripped_last_cycle if isinstance(stripped_last_cycle, dict) else None
    status = str(autonomous_monitoring.get("status") or "not_started")
    if status == "disabled":
        status = "not_started"
    if not snapshot_trusted:
        status = "degraded"
    return {
        "status": status,
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "scheduler": {
            "selection_strategy": MONITORING_SELECTION_STRATEGY,
            "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        },
        "dispatcher": {
            "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
            "last_cycle_action_count": int((last_cycle or {}).get("dispatch_action_count", 0) or 0),
            "backlog_item_count": int((last_cycle or {}).get("backlog_item_count", 0) or 0),
            "backlog_symbols": (last_cycle or {}).get("backlog_symbols", []),
        },
        "agent_decisions": {
            "watching": int((last_cycle or {}).get("agent_watching_count", 0) or 0),
            "pending_agent_decision": int((last_cycle or {}).get("agent_pending_decision_count", 0) or 0),
            "agent_delayed": int((last_cycle or {}).get("agent_delayed_count", 0) or 0),
            "agent_rejected": int((last_cycle or {}).get("agent_rejected_count", 0) or 0),
            "agent_approved_submitted": int((last_cycle or {}).get("agent_approved_submitted_count", 0) or 0),
        },
        "workers": [
            {
                "name": autonomous_monitoring.get("worker_name") or "monitoring_dispatch_loop",
                "status": status,
                "last_heartbeat_at": autonomous_monitoring.get("last_heartbeat_at"),
                "last_success_at": autonomous_monitoring.get("last_success_at"),
            }
        ],
        "last_heartbeat_at": autonomous_monitoring.get("last_heartbeat_at"),
        "last_success_at": autonomous_monitoring.get("last_success_at"),
        "last_cycle": last_cycle,
        "execution_scope_status": "available" if snapshot_trusted else "unavailable",
        "execution_scope_reason": scope_reason,
        "price_source": _strip_account_scope(monitoring_price_source),
        "source": "autonomous_runtime_snapshot",
    }


def _compact_monitoring_cycle_payload(payload: dict[str, object]) -> dict[str, object]:
    compacted = dict(payload)
    coverage = compacted.get("protective_stop_coverage")
    if isinstance(coverage, dict):
        compacted["protective_stop_coverage"] = _compact_status_detail_lists(coverage)
    target_coverage = compacted.get("profit_target_coverage")
    if isinstance(target_coverage, dict):
        compacted["profit_target_coverage"] = _compact_status_detail_lists(target_coverage)
    stop_actionability = compacted.get("protective_stop_actionability")
    if isinstance(stop_actionability, dict):
        compacted["protective_stop_actionability"] = _compact_status_detail_lists(stop_actionability)
    for key in ("items", "jobs"):
        value = compacted.get(key)
        if isinstance(value, list) and len(value) > MAX_STATUS_DETAIL_ITEMS:
            compacted[f"{key}_count"] = len(value)
            compacted[f"{key}_truncated"] = True
            compacted[key] = value[:MAX_STATUS_DETAIL_ITEMS]
    return compacted


def _compact_status_detail_lists(payload: dict[str, object]) -> dict[str, object]:
    compacted = dict(payload)
    for key in ("affected_symbols", "issues", "warnings"):
        value = compacted.get(key)
        if isinstance(value, list) and len(value) > MAX_STATUS_DETAIL_ITEMS:
            compacted[f"{key}_count"] = len(value)
            compacted[f"{key}_truncated"] = True
            compacted[key] = value[:MAX_STATUS_DETAIL_ITEMS]
    return compacted


def _unknown_protective_stop_coverage_payload() -> dict[str, object]:
    return {
        "status": "unknown",
        "unprotected_position_count": 0,
        "position_truth_stale_count": 0,
        "repaired_stop_coverage_count": 0,
        "affected_symbols": [],
        "missing_quantities": {},
        "issues": [],
    }


def _unknown_profit_target_coverage_payload() -> dict[str, object]:
    return {
        "status": "unknown",
        "missing_target_coverage_count": 0,
        "affected_symbols": [],
        "missing_quantities": {},
        "issues": [],
    }


def _default_autonomous_runtime_payload(settings: Settings) -> dict[str, object]:
    default_status = "idle" if settings.autonomous_runtime_enabled else "disabled"
    runtime_scope = _resolve_runtime_execution_scope(
        settings=settings,
        provider=_normalize_runtime_provider(settings.runtime_broker_provider),
    )
    return {
        "enabled": settings.autonomous_runtime_enabled,
        "execution_scope": _canonical_runtime_snapshot_execution_scope(runtime_scope),
        "dispatch": {
            "worker_name": "execution_job_dispatch_loop",
            "status": default_status,
            "poll_interval_seconds": settings.dispatch_loop_interval_seconds,
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_cycle": None,
        },
        "monitoring": {
            "worker_name": "monitoring_dispatch_loop",
            "status": default_status,
            "poll_interval_seconds": settings.monitoring_loop_interval_seconds,
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_cycle": None,
            "price_source_configured": False,
            "price_source": {
                "configured": False,
                "source_name": None,
                "provider": None,
                "status": "disabled",
                "reason": "no monitoring price source is configured",
                "last_checked_at": None,
                "last_success_at": None,
                "last_error": None,
                "last_error_at": None,
                "last_symbol_count": 0,
                "last_price_count": 0,
            },
        },
    }


def _autonomous_monitoring_symbols(*, autonomous: dict[str, object]) -> list[str]:
    monitoring = autonomous.get("monitoring")
    if not isinstance(monitoring, dict):
        return []
    last_cycle = monitoring.get("last_cycle")
    if not isinstance(last_cycle, dict):
        return []
    symbols = last_cycle.get("monitored_symbols")
    if not isinstance(symbols, list):
        return []
    return sorted({str(symbol) for symbol in symbols if str(symbol).strip()})


def _autonomous_runtime_snapshot_symbols(*, autonomous: dict[str, object]) -> list[str]:
    monitoring = autonomous.get("monitoring")
    if not isinstance(monitoring, dict):
        return []
    runtime_snapshot = monitoring.get("runtime_snapshot")
    if not isinstance(runtime_snapshot, dict):
        return []
    symbols = runtime_snapshot.get("symbols")
    if not isinstance(symbols, list):
        return []
    return sorted({str(symbol) for symbol in symbols if str(symbol).strip()})


def _autonomous_runtime_snapshot_symbol_summary(*, autonomous: dict[str, object]) -> dict[str, object]:
    monitoring = autonomous.get("monitoring")
    if not isinstance(monitoring, dict):
        return {}
    runtime_snapshot = monitoring.get("runtime_snapshot")
    if not isinstance(runtime_snapshot, dict):
        return {}
    filled_today = runtime_snapshot.get("filled_today_symbols")
    if not isinstance(filled_today, list):
        return {}
    return {
        "filled_today": sorted({str(symbol) for symbol in filled_today if str(symbol).strip()}),
    }


def _autonomous_monitoring_price_source(autonomous: dict[str, object]) -> dict[str, object]:
    monitoring = autonomous.get("monitoring")
    if isinstance(monitoring, dict):
        price_source = monitoring.get("price_source")
        if isinstance(price_source, dict):
            return price_source
        if monitoring.get("price_source_configured") is True:
            return {
                "configured": True,
                "source_name": None,
                "provider": None,
                "status": "configured",
                "reason": None,
                "last_checked_at": None,
                "last_success_at": None,
                "last_error": None,
                "last_error_at": None,
                "last_symbol_count": 0,
                "last_price_count": 0,
            }
    return {
        "configured": False,
        "source_name": None,
        "provider": None,
        "status": "disabled",
        "reason": "no monitoring price source is configured",
        "last_checked_at": None,
        "last_success_at": None,
        "last_error": None,
        "last_error_at": None,
        "last_symbol_count": 0,
        "last_price_count": 0,
    }


def _monitoring_price_source_readiness_reasons(
    *,
    autonomous: dict[str, object],
    symbols: dict[str, list[str]],
) -> list[str]:
    if not autonomous.get("enabled"):
        return []
    monitored_symbols = symbols.get("monitored") or []
    price_source = _autonomous_monitoring_price_source(autonomous)
    last_symbol_count = int(price_source.get("last_symbol_count") or 0)
    if not monitored_symbols and last_symbol_count <= 0:
        return []

    if not price_source.get("configured"):
        return ["monitoring price source is not configured while monitored symbols exist"]

    status = str(price_source.get("status") or "")
    if status in {"configured", "ready"}:
        return []
    last_price_count = int(price_source.get("last_price_count") or 0)
    if status == "degraded" and last_price_count > 0:
        return []

    reason = price_source.get("reason")
    if isinstance(reason, str) and reason.strip():
        return [f"monitoring price source is degraded: {reason}"]
    return ["monitoring price source is degraded while monitored symbols exist"]
