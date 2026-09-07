import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import and_, distinct, func, literal, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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
from app.services.fifo_inventory import load_fifo_isolation_cursor
from app.services.max_holding_actionability import max_holding_actionability_summary
from app.services.late_entry_actionability import late_entry_actionability_summary
from app.services.monitoring import (
    ACTIVE_WATCH_MONITORING_STATUSES,
    BLOCKED_MONITORING_STATUSES,
    LIVE_WATCH_BUY_VETO_EVENT_TYPE,
    MONITORING_RUNTIME_TOPOLOGY,
    MONITORING_SELECTION_STRATEGY,
    PENDING_DISPATCH_MONITORING_STATUSES,
    SELL_ACTIVE_MONITORING_STATUSES,
    SELL_PENDING_CONFIG_STATUSES,
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
from app.services.schema import check_runtime_schema
from app.services.runtime_controls import (
    ensure_runtime_control_state,
    runtime_safety_payload,
)

OPEN_ORDER_STATUSES = ACTIONABLE_OPEN_ORDER_STATUSES
ACTIVE_JOB_STATUSES = ("queued", "scheduled", "running", "retrying")
MONITORED_INTENTS = ("target_sell", "stop_sell")
SELL_MONITORED_INTENTS = ("target_sell", "stop_sell", "max_holding_close_sell")
WATCH_BUY_INTENTS = ("watch_buy", "trigger_buy", "chase_buy", "tactical_buy", "intraday_watch_buy", "next_open_buy")
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
) -> dict[str, int]:
    open_orders = db.scalar(
        select(func.count())
        .select_from(BrokerOrder)
        .where(*active_local_broker_order_filters(settings=settings, statuses=OPEN_ORDER_STATUSES)),
    )
    active_jobs = db.scalar(
        select(func.count())
        .select_from(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(ACTIVE_JOB_STATUSES),
        ),
    )
    return {
        "open_orders": int(open_orders or 0),
        "active_execution_jobs": int(active_jobs or 0),
    }


def runtime_watch_buy_veto_summary(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    rows = db.execute(
        select(ExecutionPlanItem.symbol, func.count())
        .select_from(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type == LIVE_WATCH_BUY_VETO_EVENT_TYPE,
            ExecutionPlanItemAudit.occurred_at >= today_start_utc,
            ExecutionPlanItemAudit.occurred_at < tomorrow_start_utc,
        )
        .group_by(ExecutionPlanItem.symbol)
    ).all()
    symbols = sorted(str(symbol) for symbol, count in rows if int(count or 0) > 0)
    return {
        "today_count": sum(int(count or 0) for _, count in rows),
        "today_symbols": symbols,
    }


def runtime_late_entry_summary(*, settings: Settings, db: Session) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    return late_entry_actionability_summary(
        db=db,
        mode=settings.runtime_mode,
        start_at=today_start_utc,
        end_at=tomorrow_start_utc,
    )


def runtime_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    monitoring: dict[str, object],
    now: datetime | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    position_truth = position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=observed_at,
    )
    latest_order_sync_at = db.scalar(
        select(func.max(BrokerOrder.last_broker_sync_at)).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.last_broker_sync_at.is_not(None),
        )
    )
    latest_reconcile_event_at = db.scalar(
        select(func.max(BrokerOrderEvent.observed_at))
        .select_from(BrokerOrderEvent)
        .join(BrokerOrderEvent.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrderEvent.event_source == "reconciliation",
        )
    )
    broker_orders = broker_order_freshness_payload(
        settings=settings,
        db=db,
        now=observed_at,
        latest_order_sync_at=latest_order_sync_at,
        latest_reconcile_event_at=latest_reconcile_event_at,
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
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    if latest_order_sync_at is None:
        latest_order_sync_at = db.scalar(
            select(func.max(BrokerOrder.last_broker_sync_at)).where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.last_broker_sync_at.is_not(None),
            )
        )
    if latest_reconcile_event_at is None:
        latest_reconcile_event_at = db.scalar(
            select(func.max(BrokerOrderEvent.observed_at))
            .select_from(BrokerOrderEvent)
            .join(BrokerOrderEvent.broker_order)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrderEvent.event_source == "reconciliation",
            )
        )
    return {
        "latest_broker_sync_at": _serialize_datetime(latest_order_sync_at),
        "latest_reconciliation_event_at": _serialize_datetime(latest_reconcile_event_at),
        "stale_nonterminal": stale_nonterminal_order_freshness_payload(
            settings=settings,
            db=db,
            now=observed_at,
        ),
        "fill_quality": broker_order_fill_quality_payload(
            settings=settings,
            db=db,
        ),
    }


def broker_order_fill_quality_payload(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
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
) -> list[DuplicateFillQualityGroup]:
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == runtime_mode)
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


def position_truth_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    stale_before = observed_at - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    evidence_symbols = _position_truth_evidence_symbols(settings=settings, db=db)
    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
        )
    ).all()
    positions_by_symbol = {position.symbol: position for position in positions}
    nonzero_position_symbols = {
        position.symbol
        for position in positions
        if _position_has_quantity(position)
    }
    candidate_symbols = nonzero_position_symbols | evidence_symbols
    trusted_flat_count = sum(
        1
        for symbol, position in positions_by_symbol.items()
        if symbol not in evidence_symbols and not _position_has_quantity(position)
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
        position = positions_by_symbol.get(symbol)
        reconciled_at = position.last_reconciled_at if position is not None else None
        if reconciled_at is None or _as_utc(reconciled_at) < stale_before:
            stale_symbols.append(symbol)
            if reconciled_at is None:
                continue
            age_seconds = (observed_at - _as_utc(reconciled_at)).total_seconds()
            if oldest_age_seconds is None or age_seconds > oldest_age_seconds:
                oldest_age_seconds = age_seconds

    broker_truth_reachable = latest_reconciled_at is not None and latest_reconciled_at >= stale_before
    return {
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
        "trusted_flat_count": trusted_flat_count,
    }


def stale_nonterminal_order_freshness_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    threshold_seconds = settings.stale_broker_order_reconciliation_threshold_seconds
    stale_before = observed_at - timedelta(seconds=threshold_seconds)
    order_observed_at = broker_order_observed_at_expr()
    orders = db.scalars(
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
        .order_by(order_observed_at.asc(), BrokerOrder.created_at.asc())
    ).all()
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
) -> set[str]:
    symbols = set(
        db.scalars(
            select(BrokerOrder.symbol)
            .where(
                *active_local_broker_order_filters(
                    settings=settings,
                    statuses=CAPITAL_RESERVING_ORDER_STATUSES,
                )
            )
            .distinct()
        ).all()
    )
    filled_buy_symbols = set(
        db.scalars(
            select(BrokerOrder.symbol)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.side == "buy",
                BrokerOrder.status == "filled",
            )
            .distinct()
        ).all()
    )
    exited_symbols = set(
        db.scalars(
            select(BrokerOrder.symbol)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.side == "sell",
                BrokerOrder.status == "filled",
            )
            .distinct()
        ).all()
    )
    symbols.update(filled_buy_symbols - exited_symbols)
    return {str(symbol) for symbol in symbols}


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
) -> dict[str, object]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    failed_observed_at = func.coalesce(
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.submitted_at,
        BrokerOrder.updated_at,
    )
    monitored_symbols = runtime_monitored_symbols(
        settings=settings,
        db=db,
        today_start_utc=today_start_utc,
    )
    scheduled_today = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
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
    ).all()
    filled_today = db.scalars(
        select(distinct(ExecutionFill.symbol))
        .select_from(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at >= today_start_utc,
            ExecutionFill.filled_at < tomorrow_start_utc,
        )
    ).all()
    filled_summary = runtime_filled_today_summary(
        settings=settings,
        db=db,
        today_start_utc=today_start_utc,
        tomorrow_start_utc=tomorrow_start_utc,
    )
    failed_today = db.scalars(
        select(distinct(BrokerOrder.symbol))
        .select_from(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(FAILED_ORDER_STATUSES),
            failed_observed_at.is_not(None),
            failed_observed_at >= today_start_utc,
            failed_observed_at < tomorrow_start_utc,
        )
    ).all()
    position_symbols = db.scalars(
        select(distinct(ExecutionPosition.symbol)).where(
            ExecutionPosition.mode == settings.runtime_mode,
            or_(
                ExecutionPosition.quantity > 0,
                ExecutionPosition.base_quantity > 0,
            ),
        )
    ).all()
    return {
        "monitored": monitored_symbols,
        "scheduled_today": sorted(scheduled_today),
        "filled_today": sorted(filled_today),
        "filled_today_by_side": filled_summary["by_side"],
        "filled_today_counts": filled_summary["counts"],
        "failed_today": sorted(failed_today),
        "positions": sorted(position_symbols),
    }


def runtime_monitored_symbols(
    *,
    settings: Settings,
    db: Session,
    today_start_utc: datetime | None = None,
) -> list[str]:
    if today_start_utc is None:
        today_start_utc, _ = _current_krx_day_utc_bounds()
    held_position_symbols = select(ExecutionPosition.symbol).where(
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity > 0,
    )
    sell_monitoring_conditions = [
        ExecutionPlanItem.side == "sell",
        ExecutionPlanItem.intent.in_(SELL_MONITORED_INTENTS),
        ExecutionPlanItem.status.in_(
            tuple(
                SELL_ACTIVE_MONITORING_STATUSES
                | SELL_PENDING_CONFIG_STATUSES
                | BLOCKED_MONITORING_STATUSES
                | PENDING_DISPATCH_MONITORING_STATUSES
            )
        ),
    ]
    if settings.runtime_mode == "live":
        sell_monitoring_conditions.append(ExecutionPlanItem.symbol.in_(held_position_symbols))
    sell_symbols = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            and_(*sell_monitoring_conditions),
        )
    ).all()
    mode_plan_ids = select(ExecutionPlan.id).where(ExecutionPlan.mode == settings.runtime_mode)
    buy_symbols = db.scalars(
        select(distinct(ExecutionPlanItem.symbol)).where(
            ExecutionPlanItem.execution_plan_id.in_(mode_plan_ids),
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(WATCH_BUY_INTENTS),
            ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
        )
    ).all()
    return sorted({*sell_symbols, *buy_symbols})


def runtime_filled_today_summary(
    *,
    settings: Settings,
    db: Session,
    today_start_utc: datetime | None = None,
    tomorrow_start_utc: datetime | None = None,
) -> dict[str, object]:
    if today_start_utc is None or tomorrow_start_utc is None:
        today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    fills = dedupe_execution_fills(
        db.scalars(
            select(ExecutionFill)
            .select_from(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                ExecutionFill.filled_at >= today_start_utc,
                ExecutionFill.filled_at < tomorrow_start_utc,
            )
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    )
    by_side: dict[str, list[str]] = {"buy": [], "sell": []}
    counts: dict[str, dict[str, int]] = {
        "all": {"symbols": 0, "fills": 0, "quantity": 0},
        "buy": {"symbols": 0, "fills": 0, "quantity": 0},
        "sell": {"symbols": 0, "fills": 0, "quantity": 0},
    }
    all_symbols: set[str] = set()
    side_symbols: dict[str, set[str]] = {"buy": set(), "sell": set()}
    for fill in fills:
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
) -> list[str]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    filled_today = db.scalars(
        select(distinct(ExecutionFill.symbol))
        .select_from(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at >= today_start_utc,
            ExecutionFill.filled_at < tomorrow_start_utc,
        )
    ).all()
    return sorted(filled_today)


def _current_krx_day_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current_krx = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    day_start_krx = current_krx.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day_start_krx = day_start_krx + timedelta(days=1)
    return day_start_krx.astimezone(UTC), next_day_start_krx.astimezone(UTC)


def monitoring_payload(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    segments = monitoring_segments(settings=settings, db=db)
    agent_decisions = monitoring_agent_decision_counts(settings=settings, db=db)
    max_holding_actionability = max_holding_actionability_summary(
        db=db,
        mode=settings.runtime_mode,
    )
    backlog_symbols = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.status.in_(tuple(PENDING_DISPATCH_MONITORING_STATUSES)),
        )
        .order_by(ExecutionPlanItem.symbol.asc())
    ).all()
    backlog_item_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.status.in_(tuple(PENDING_DISPATCH_MONITORING_STATUSES)),
            )
        )
        or 0
    )
    checkpoints = db.scalars(select(MonitoringCheckpoint)).all()
    checkpoints = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint.mode == settings.runtime_mode
    ]
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
        "protective_stop_coverage": protective_stop_coverage,
        "profit_target_coverage": profit_target_coverage,
        "last_cycle": latest_details or None,
    }


def monitoring_agent_decision_counts(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, int]:
    rows = db.execute(
        select(ExecutionPlanItem.status, func.count())
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .group_by(ExecutionPlanItem.status)
    ).all()
    counts_by_status = {str(status): int(count or 0) for status, count in rows}
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
) -> dict[str, object]:
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
    ).all()
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


def build_runtime_status(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    autonomous_runtime: dict[str, object] | None = None,
    include_expensive_sections: bool = True,
) -> dict[str, object]:
    dependencies = {
        "database": "ok",
        "runtime_schema": "ok",
    }
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
    account_drawdown_gate: dict[str, object] = {"gated": False, "latest_decision": None}
    autonomous = autonomous_runtime or _default_autonomous_runtime_payload(settings)
    monitoring_price_source = _autonomous_monitoring_price_source(autonomous)
    monitoring: dict[str, object] = _monitoring_payload_from_autonomous(
        settings=settings,
        autonomous=autonomous,
        monitoring_price_source=monitoring_price_source,
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
    monthly_pnl = _default_monthly_pnl_payload(settings=settings)
    daily_pnl = _default_daily_pnl_payload()
    weekly_pnl = _default_weekly_pnl_payload()
    broker = build_broker_session_summary(settings=settings, db=None)
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

    if settings.autonomous_runtime_enabled and not include_expensive_sections:
        late_entry_gate = runtime_late_entry_summary(settings=settings, db=db)
        cached_symbols = _autonomous_runtime_snapshot_symbol_summary(autonomous=autonomous)
        cycle_symbols = _autonomous_runtime_snapshot_symbols(autonomous=autonomous) or _autonomous_monitoring_symbols(
            autonomous=autonomous
        )
        filled_summary = runtime_filled_today_summary(settings=settings, db=db)
        symbols = {
            **symbols,
            **cached_symbols,
            "filled_today": runtime_filled_today_symbols(settings=settings, db=db),
            "filled_today_by_side": filled_summary["by_side"],
            "filled_today_counts": filled_summary["counts"],
            "monitored": cycle_symbols,
            "monitoring_cycle_batch": cycle_symbols,
        }
        freshness["summary_source"] = "fast_autonomous_runtime_snapshot"
        monitoring_reasons = _monitoring_price_source_readiness_reasons(
            autonomous=autonomous,
            symbols=symbols,
        )
        if monitoring_reasons:
            monitoring["status"] = "degraded"
        safety = fast_autonomous_safety(settings=settings, db=db)
        readiness = readiness_payload(
            dependencies_ok=True,
            safety=safety,
            counts_available=counts_available,
            extra_reasons=monitoring_reasons,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": autonomous,
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
            "monthly_pnl": _default_monthly_pnl_payload(settings=settings),
            "weekly_pnl": _default_weekly_pnl_payload(),
            "daily_pnl": daily_pnl_payload(settings=settings, db=db),
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
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": autonomous,
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
        missing_tables = schema.get("missing_tables") or []
        if missing_tables:
            schema_reason = (
                "runtime schema incomplete: "
                f"missing {', '.join(str(table) for table in missing_tables)}; run migrations"
            )
        else:
            schema_reason = "runtime schema unavailable; run migrations"
        safety = default_safety(settings, schema_reason)
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "diagnostics": diagnostics,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": autonomous,
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
        counts = runtime_counts(settings=settings, db=db)
        watch_buy_vetoes = runtime_watch_buy_veto_summary(settings=settings, db=db)
        late_entry_gate = runtime_late_entry_summary(settings=settings, db=db)
        if include_expensive_sections:
            broker = build_broker_session_summary(settings=settings, db=db)
            broker_providers = broker_provider_readiness(settings=settings, db=db)
            account_drawdown_gate = latest_account_drawdown_gate_state(settings=settings, db=db)
            symbols = runtime_symbol_summary(settings=settings, db=db)
        if include_expensive_sections:
            monitoring = monitoring_payload(settings=settings, db=db)
            monitoring["price_source"] = monitoring_price_source
            freshness = runtime_freshness_payload(
                settings=settings,
                db=db,
                monitoring=monitoring,
            )
            monthly_pnl = monthly_pnl_payload(settings=settings, db=db)
            weekly_pnl = weekly_pnl_payload(settings=settings, db=db)
            daily_pnl = daily_pnl_payload(settings=settings, db=db)
        elif settings.autonomous_runtime_enabled:
            cached_symbols = _autonomous_runtime_snapshot_symbol_summary(autonomous=autonomous)
            if "filled_today" not in cached_symbols:
                cached_symbols["filled_today"] = runtime_filled_today_symbols(
                    settings=settings,
                    db=db,
                )
            if "filled_today_by_side" not in cached_symbols or "filled_today_counts" not in cached_symbols:
                filled_summary = runtime_filled_today_summary(settings=settings, db=db)
                cached_symbols["filled_today_by_side"] = filled_summary["by_side"]
                cached_symbols["filled_today_counts"] = filled_summary["counts"]
            symbols = {
                **symbols,
                **cached_symbols,
                "monitored": runtime_monitored_symbols(settings=settings, db=db),
                "monitoring_cycle_batch": _autonomous_monitoring_symbols(autonomous=autonomous),
            }
            freshness["summary_source"] = "fast_autonomous_runtime_snapshot"
            monthly_pnl = _default_monthly_pnl_payload(settings=settings)
            weekly_pnl = _default_weekly_pnl_payload()
            daily_pnl = _default_daily_pnl_payload()
        else:
            freshness["summary_source"] = "fast_runtime_status"
            monthly_pnl = monthly_pnl_payload(settings=settings, db=db)
            weekly_pnl = weekly_pnl_payload(settings=settings, db=db)
            daily_pnl = daily_pnl_payload(settings=settings, db=db)
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
        monthly_pnl = _default_monthly_pnl_payload(settings=settings)
        weekly_pnl = _default_weekly_pnl_payload()
        daily_pnl = _default_daily_pnl_payload()
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
        extra_reasons=monitoring_reasons,
    )
    return {
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": safety["mode"],
        "ready": readiness["ready"],
        "readiness": readiness,
        "safety": safety,
        "dependencies": dependencies,
        "diagnostics": diagnostics,
        "liveness": {
            **liveness_payload(runtime),
            "autonomous": autonomous,
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
) -> dict[str, object]:
    month_start, next_month_start = _current_krx_month_utc_bounds(now)
    summary = _period_realized_pnl_summary(
        settings=settings,
        db=db,
        start_at=month_start,
        end_at=next_month_start,
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
    expected_progress = _expected_month_progress_pct(now)
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
        "month": month_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m"),
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
        "unknown_cost_basis_sell_quantity": _quantity_value(summary["unknown_cost_basis_sell_quantity"]),
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
) -> dict[str, object]:
    day_start, next_day_start = _current_krx_day_utc_bounds(now)
    fills = dedupe_execution_fills(db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at < next_day_start,
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all())

    lots_by_symbol: dict[str, list[dict[str, object]]] = {}
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=next_day_start,
    )
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
    for fill in fills:
        filled_at = _as_utc(fill.filled_at)
        side = fill.side.lower()
        symbol = fill.symbol
        quantity = _pnl_fill_quantity(fill)
        notional = _pnl_fill_notional(fill=fill, quantity=quantity)
        lots = lots_by_symbol.setdefault(symbol, [])
        isolation_cursor.apply_before(symbol=symbol, observed_at=filled_at, lots=lots)
        if side == "buy":
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
        "date": day_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m-%d"),
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
) -> dict[str, object]:
    week_start, next_week_start = _current_krx_week_utc_bounds(now)
    summary = _period_realized_pnl_summary(
        settings=settings,
        db=db,
        start_at=week_start,
        end_at=next_week_start,
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
        "week_start": week_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m-%d"),
        "week_end_exclusive": next_week_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m-%d"),
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
) -> dict[str, object]:
    fills = dedupe_execution_fills(db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at < end_at,
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all())
    sell_symbols = {
        fill.symbol
        for fill in fills
        if str(fill.side or "").lower() == "sell" and start_at <= _as_utc(fill.filled_at) < end_at
    }
    positions_by_symbol = _pnl_positions_by_symbol(settings=settings, db=db, symbols=sell_symbols)

    lots_by_symbol: dict[str, list[dict[str, object]]] = {}
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=end_at,
    )
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
    for fill in fills:
        filled_at = _as_utc(fill.filled_at)
        side = fill.side.lower()
        symbol = fill.symbol
        quantity = _pnl_fill_quantity(fill)
        notional = _pnl_fill_notional(fill=fill, quantity=quantity)
        lots = lots_by_symbol.setdefault(symbol, [])
        isolation_cursor.apply_before(symbol=symbol, observed_at=filled_at, lots=lots)
        if side == "buy":
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
        next_buy_after_sell = _pnl_next_buy_after(fills=fills, symbol=symbol, after=filled_at)
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
            position = positions_by_symbol.get(symbol)
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
            position = positions_by_symbol.get(symbol)
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

    for symbol, lots in lots_by_symbol.items():
        isolation_cursor.apply_through(symbol=symbol, observed_at=end_at, lots=lots)

    return {
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
            lots_by_symbol=lots_by_symbol,
            positions_by_symbol=positions_by_symbol,
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
    symbol: str,
    after: datetime,
) -> datetime | None:
    later_buys = [
        _as_utc(fill.filled_at)
        for fill in fills
        if fill.symbol == symbol
        and str(fill.side or "").lower() == "buy"
        and _as_utc(fill.filled_at) > after
    ]
    return min(later_buys) if later_buys else None


def _pnl_positions_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
) -> dict[str, ExecutionPosition]:
    if not symbols:
        return {}
    return {
        position.symbol: position
        for position in db.scalars(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.symbol.in_(sorted(symbols)),
            )
        ).all()
    }


def _stale_fifo_inventory_alerts(
    *,
    lots_by_symbol: dict[str, list[dict[str, object]]],
    positions_by_symbol: dict[str, ExecutionPosition],
) -> list[dict[str, object]]:
    alerts: list[dict[str, object]] = []
    for symbol, lots in sorted(lots_by_symbol.items()):
        fifo_quantity = sum((Decimal(str(lot["quantity"])) for lot in lots), Decimal("0"))
        if fifo_quantity <= 0:
            continue
        position = positions_by_symbol.get(symbol)
        if position is None or position.last_reconciled_at is None:
            continue
        position_quantity = _position_effective_quantity(position)
        if position_quantity > 0:
            continue
        alerts.append(
            {
                "kind": "stale_fill_fifo_inventory_flat_broker_position",
                "symbol": symbol,
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


def _default_monthly_pnl_payload(*, settings: Settings) -> dict[str, object]:
    target = getattr(settings, "monthly_target_krw", Decimal("500000"))
    economics = _unavailable_pnl_economics_payload(
        reason="monthly_pnl_not_computed"
    )
    notionals = _unavailable_pnl_notional_payload()
    return {
        "month": datetime.now(KRX_TIMEZONE).strftime("%Y-%m"),
        "monthly_realized_pnl": None,
        "monthly_gross_realized_pnl_krw": None,
        "monthly_net_realized_pnl_krw": None,
        "monthly_target_krw": _decimal_text(target),
        "target_progress_pct": None,
        "expected_calendar_progress_pct": _decimal_text(_expected_month_progress_pct()),
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


def _default_daily_pnl_payload() -> dict[str, object]:
    economics = _unavailable_pnl_economics_payload(reason="daily_pnl_not_computed")
    notionals = _unavailable_pnl_notional_payload()
    return {
        "date": datetime.now(KRX_TIMEZONE).strftime("%Y-%m-%d"),
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


def _default_weekly_pnl_payload() -> dict[str, object]:
    week_start, next_week_start = _current_krx_week_utc_bounds()
    economics = _unavailable_pnl_economics_payload(reason="weekly_pnl_not_computed")
    notionals = _unavailable_pnl_notional_payload()
    return {
        "week_start": week_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m-%d"),
        "week_end_exclusive": next_week_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m-%d"),
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


def _current_krx_week_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    week_start = (current - timedelta(days=current.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    next_week = week_start + timedelta(days=7)
    return week_start.astimezone(UTC), next_week.astimezone(UTC)


def _current_krx_month_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    month_start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return month_start.astimezone(UTC), next_month.astimezone(UTC)


def _expected_month_progress_pct(now: datetime | None = None) -> Decimal:
    current = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
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
    autonomous_runtime: dict[str, object] | None = None,
) -> dict[str, object]:
    runtime_status = build_runtime_status(
        settings=settings,
        runtime=runtime,
        db=db,
        autonomous_runtime=autonomous_runtime,
        include_expensive_sections=True,
    )
    symbols: list[dict[str, object]] = []
    if runtime_status["dependencies"]["database"] == "ok" and runtime_status["dependencies"]["runtime_schema"] == "ok":
        try:
            monitored_symbols = runtime_status.get("symbols", {}).get("monitored", [])
            status_symbols = runtime_status.get("symbols", {})
            if isinstance(monitored_symbols, list) and isinstance(status_symbols, dict):
                monitored_symbol_set = {str(symbol) for symbol in monitored_symbols}
                normalized_symbols = set()
                for key in ("monitored", "scheduled_today", "filled_today", "failed_today", "positions"):
                    values = status_symbols.get(key, [])
                    if isinstance(values, list):
                        normalized_symbols.update(str(symbol) for symbol in values)
                vetoes = runtime_status.get("watch_buy_vetoes", {})
                if isinstance(vetoes, dict) and isinstance(vetoes.get("today_symbols"), list):
                    normalized_symbols.update(str(symbol) for symbol in vetoes["today_symbols"])
                if len(normalized_symbols) > 20:
                    symbols = _minimal_runtime_symbol_rows(
                        settings=settings,
                        symbols=normalized_symbols,
                        monitored_symbols=monitored_symbol_set,
                    )
                else:
                    symbols = list_runtime_symbols(
                        settings=settings,
                        db=db,
                        symbols=normalized_symbols,
                    )
            else:
                symbols = list_runtime_symbols(settings=settings, db=db)
        except SQLAlchemyError:
            db.rollback()
            symbols = []

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "market_date": datetime.now(KRX_TIMEZONE).date().isoformat(),
        "mode": settings.runtime_mode,
        "runtime": runtime_status,
        "account_drawdown_gate": latest_account_drawdown_gate_state(settings=settings, db=db),
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


def _minimal_runtime_symbol_rows(
    *,
    settings: Settings,
    symbols: set[str],
    monitored_symbols: set[str],
) -> list[dict[str, object]]:
    return [
        {
            "symbol": symbol,
            "mode": settings.runtime_mode,
            "asset_classes": [],
            "providers": [],
            "venues": [],
            "monitored": symbol in monitored_symbols,
            "monitoring_statuses": [],
            "intents": [],
            "plan_item_count": 0,
            "latest_plan_item_status": None,
            "order_count": 0,
            "open_order_count": 0,
            "fill_count": 0,
            "filled_quantity": 0,
            "filled_base_quantity": "0",
            "filled_quote_notional": "0",
            "position_quantity": 0,
            "position_base_quantity": None,
            "position_average_price": None,
            "last_reconciliation_error": None,
            "latest_activity_at": None,
            "summary_source": "runtime_status_monitored_symbols",
        }
        for symbol in sorted(symbols)
    ]


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
) -> dict[str, object]:
    autonomous_monitoring = autonomous.get("monitoring")
    if not isinstance(autonomous_monitoring, dict):
        autonomous_monitoring = {}
    last_cycle = autonomous_monitoring.get("last_cycle")
    if not isinstance(last_cycle, dict):
        last_cycle = None
    else:
        last_cycle = _compact_monitoring_cycle_payload(last_cycle)
    status = str(autonomous_monitoring.get("status") or "not_started")
    if status == "disabled":
        status = "not_started"
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
        "price_source": monitoring_price_source,
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
    return {
        "enabled": settings.autonomous_runtime_enabled,
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
