from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from functools import partial
from zoneinfo import ZoneInfo

from sqlalchemy import and_, distinct, func, or_, select, tuple_
from sqlalchemy.orm import Session, load_only, selectinload

from app.api.schemas.execution import (
    MonitoringEvaluationRequest,
    MonitoringPriceRequest,
    PlaceOrderRequest,
)
from app.broker.base import BrokerAccountOpenOrder, BrokerAccountSnapshot, BrokerAdapter, BrokerAdapterError
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.reconciliation import (
    reconcile_pending_broker_orders,
    sync_execution_positions_from_broker_snapshot,
)
from app.services.broker_orders import place_broker_order
from app.services.plan_item_identity import serialize_plan_item_source_identity
from app.services.risk_sizing import (
    RiskSizingRejection,
    RiskSizingResult,
    effective_stop_price,
    resolve_risk_based_buy_quantity,
)
from app.services.market_regime_guardrails import market_regime_context_from_item_payloads
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
)
from app.services import trigger_time_exec_agent

SELL_MONITORED_INTENTS = frozenset({"target_sell", "stop_sell", "max_holding_close_sell"})
WATCH_BUY_INTENTS = frozenset({"watch_buy", "trigger_buy", "chase_buy", "tactical_buy", "intraday_watch_buy", "next_open_buy"})
MONITORED_INTENTS = SELL_MONITORED_INTENTS | WATCH_BUY_INTENTS
AGENT_PENDING_STATUS = trigger_time_exec_agent.AGENT_PENDING_STATUS
AGENT_DELAY_STATUS = trigger_time_exec_agent.AGENT_DELAY_STATUS
AGENT_REJECTED_STATUS = trigger_time_exec_agent.AGENT_REJECTED_STATUS
AGENT_APPROVED_PENDING_SUBMIT_STATUS = trigger_time_exec_agent.AGENT_APPROVED_PENDING_SUBMIT_STATUS
AGENT_ACTIVE_STATUSES = trigger_time_exec_agent.AGENT_ACTIVE_STATUSES
MAX_PROTECTIVE_STOP_COVERAGE_DETAILS = 20
WATCH_MONITORING_STATUSES = frozenset(
    {
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "watch_buy_triggered",
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_expired",
        "watch_cancelled",
        "watch_buy_triggered_blocked",
        "watch_buy_triggered_pending_dispatch",
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    }
)
BLOCKED_MONITORING_STATUSES = frozenset(
    {
        "target_sell_triggered_blocked",
        "stop_sell_triggered_blocked",
        "watch_buy_triggered_blocked",
        "watch_cancelled",
    }
)
RETRYABLE_WATCH_BUY_APPROVAL_REASONS = frozenset(
    {
        "account_snapshot_unavailable",
        "exec_agent_sizing_pending",
        "invalid_stop_distance",
    }
)
BLOCKED_WATCH_BUY_APPROVAL_REASONS = frozenset(
    {
        "paused",
        "kill_switch",
        "stale_trigger_reclaim_required",
    }
)
PENDING_DISPATCH_MONITORING_STATUSES = frozenset(
    {
        "target_sell_triggered_pending_dispatch",
        "stop_sell_triggered_pending_dispatch",
        "watch_buy_triggered_pending_dispatch",
    }
)
NON_MONITORABLE_PLAN_ITEM_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "broker_order_rejected",
        "broker_order_failed",
        AGENT_REJECTED_STATUS,
        "submitted",
        "open",
        "partially_filled",
        "filled",
        "cancel_pending",
        "canceled",
        "expired",
        "retry_pending",
        "rejected",
        "failed",
        "superseded",
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_expired",
        "watch_cancelled",
    }
)
RETRYABLE_PROTECTIVE_STOP_STATUSES = frozenset(
    {
        "broker_order_retry_pending",
        "broker_order_rejected",
        "retry_pending",
        "rejected",
    }
)
FALLBACK_PROTECTIVE_STOP_STATUSES = frozenset({"broker_order_retry_pending", "retry_pending"})
SESSION_RETRYABLE_ERROR_CODES = frozenset({"KIOK0320", "APBK0918"})
SESSION_RETRYABLE_MESSAGE_MARKERS = (
    "장운영시간",
    "정규시장 시간 주문불가",
    "market is not open",
    "not regular market",
)
SELL_PENDING_CONFIG_STATUSES = frozenset(
    {
        "target_sell_pending_config",
        "stop_sell_pending_config",
        "max_holding_close_sell_pending_config",
    }
)
SELL_ACTIVE_MONITORING_STATUSES = frozenset(
    {
        "target_sell_monitoring",
        "stop_sell_monitoring",
        "max_holding_close_sell_monitoring",
    }
)
SELL_STALE_FLAT_EXPIRABLE_STATUSES = SELL_PENDING_CONFIG_STATUSES | SELL_ACTIVE_MONITORING_STATUSES
MONITORING_WORKER_NAME = "intraday_target_stop_monitor"
MONITORING_RUNTIME_TOPOLOGY = "shared_symbol_scheduler"
MONITORING_SELECTION_STRATEGY = "round_robin_by_symbol"
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
WATCH_BUY_EXPIRABLE_STATUSES = frozenset(
    {
        "watch_monitoring_active",
        "watch_monitoring_pending",
        "watch_buy_triggered_blocked",
        "watch_buy_triggered_pending_dispatch",
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    }
)
OPEN_BUY_ORDER_STATUSES = frozenset(
    {"submitted", "open", "partially_filled", "submission_pending", "retry_pending"}
)
RESERVED_CASH_EXHAUSTED_REASON = "reserved_cash_exhausted"
ACCOUNT_DRAWDOWN_GATE_RELEASE_SUFFIX = ":account-drawdown-gate-disabled"


@dataclass(frozen=True)
class EvaluateMonitoringResult:
    status_code: int
    payload: dict[str, object]


@dataclass(frozen=True)
class PendingMonitoringAction:
    item: ExecutionPlanItem
    observed_price: Decimal | None
    observed_at: datetime | None = None
    market_quality_context: dict[str, object] | None = None
    trigger_reason: str | None = None
    trigger_metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class WatchBuyOrderApproval:
    quantity: int | None
    reason: str | None = None
    message: str | None = None
    details: dict[str, object] | None = None


@dataclass
class WatchBuyCashReservation:
    orderable_cash: Decimal
    already_reserved_notional: Decimal
    batch_id: str

    @property
    def remaining_cash(self) -> Decimal:
        remaining = self.orderable_cash - self.already_reserved_notional
        return remaining if remaining > 0 else Decimal("0")


@dataclass(frozen=True)
class ProfitProtectionDecision:
    reason: str
    metadata: dict[str, object]


@dataclass(frozen=True)
class TriggerTimeAgentActionResult:
    quantity: int | None
    limit_price: Decimal | None
    result_payload: dict[str, object]


@dataclass(frozen=True)
class ProtectiveStopCoverageIssue:
    symbol: str
    held_quantity: int | None
    active_stop_quantity: int
    missing_quantity: int | None
    latest_stop_item_id: str | None
    latest_stop_status: str | None
    latest_stop_trigger_price: Decimal | None
    outcome: str
    reason: str | None = None
    position_quantity: int | None = None
    position_last_reconciled_at: datetime | None = None
    evidence: list[str] | None = None


PROFIT_PROTECTION_TRIGGER_REASONS = frozenset(
    {"profit_protection_trailing_stop", "aged_position_profit_protection"}
)
PROTECTIVE_STOP_COVERAGE_STATUSES = frozenset(
    {
        "stop_sell_monitoring",
        "stop_sell_triggered_pending_dispatch",
        AGENT_PENDING_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "open",
        "partially_filled",
    }
)
IN_FLIGHT_BROKER_ORDER_ITEM_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "open",
        "partially_filled",
    }
)


SELL_REPLAN_UPDATE_STATUSES = (
    SELL_PENDING_CONFIG_STATUSES
    | SELL_ACTIVE_MONITORING_STATUSES
    | BLOCKED_MONITORING_STATUSES
)


def materialize_monitoring_plan_items(
    plan: ExecutionPlan,
    *,
    db: Session | None = None,
) -> None:
    if db is not None:
        _update_existing_sell_monitors_from_replanned_items(plan=plan, db=db)
    for item in plan.items:
        if item.status == "superseded":
            continue
        if _is_watch_buy_item(item):
            if item.status not in {"pending", "watch_monitoring_pending"}:
                continue
            if _monitoring_item_is_configured(item):
                item.status = "watch_monitoring_active"
                continue
            item.status = "watch_monitoring_pending"
            continue
        if item.intent not in SELL_MONITORED_INTENTS or item.status != "pending":
            continue
        if _monitoring_item_is_configured(
            item
        ) and not _quantity_policy_requires_position_exit(item.quantity_policy):
            item.status = _monitoring_status(item.intent)
            continue
        item.status = _pending_configuration_status(item.intent)


def _update_existing_sell_monitors_from_replanned_items(
    *,
    plan: ExecutionPlan,
    db: Session,
) -> None:
    replanned_items = [
        item
        for item in plan.items
        if (
            item.side == "sell"
            and item.intent in SELL_MONITORED_INTENTS
            and item.source_plan_item_id
        )
    ]
    if not replanned_items:
        return

    existing_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == plan.mode,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.source_plan_item_id.in_(
                [item.source_plan_item_id for item in replanned_items if item.source_plan_item_id]
            ),
            ExecutionPlanItem.status.in_(tuple(SELL_REPLAN_UPDATE_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_plan_item_id,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.updated_at,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
                ExecutionPlan.created_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.created_at.desc(),
            ExecutionPlanItem.updated_at.desc(),
            ExecutionPlanItem.id.desc(),
        )
    ).all()
    existing_by_source: dict[tuple[str, str, str], ExecutionPlanItem] = {}
    for existing in existing_items:
        if not existing.source_plan_item_id:
            continue
        key = (existing.source_plan_item_id, existing.symbol, existing.intent)
        if key not in existing_by_source:
            existing_by_source[key] = existing

    for replanned in replanned_items:
        if not replanned.source_plan_item_id:
            continue
        existing = existing_by_source.get((replanned.source_plan_item_id, replanned.symbol, replanned.intent))
        if existing is None or existing.id == replanned.id:
            continue

        prior_status = existing.status
        prior_trigger_price = existing.trigger_price
        prior_limit_price = existing.limit_price
        prior_quantity_policy = existing.quantity_policy
        prior_decision_snapshot_id = existing.decision_snapshot_id

        existing.trigger_price = replanned.trigger_price
        existing.limit_price = replanned.limit_price
        existing.quantity_policy = replanned.quantity_policy
        existing.time_in_force = replanned.time_in_force
        existing.decision_agent_name = replanned.decision_agent_name
        existing.decision_snapshot_id = replanned.decision_snapshot_id
        existing.decision_run_id = replanned.decision_run_id
        existing.source_market_date = replanned.source_market_date
        existing.provider = replanned.provider
        existing.venue = replanned.venue
        existing.asset_class = replanned.asset_class
        existing.base_quantity = replanned.base_quantity
        existing.quote_notional = replanned.quote_notional
        existing.quantity_precision = replanned.quantity_precision
        existing.price_precision = replanned.price_precision
        existing.status = (
            _monitoring_status(existing.intent)
            if _monitoring_item_is_configured(existing)
            else _pending_configuration_status(existing.intent)
        )
        _record_monitoring_event(
            item=existing,
            event={
                "event": "sell_monitor_replanned_in_place",
                "reason": "same_source_plan_item_replanned",
                "prior_status": prior_status,
                "updated_status": existing.status,
                "prior_trigger_price": _serialize_decimal(prior_trigger_price),
                "updated_trigger_price": _serialize_decimal(existing.trigger_price),
                "prior_limit_price": _serialize_decimal(prior_limit_price),
                "updated_limit_price": _serialize_decimal(existing.limit_price),
                "prior_quantity_policy": prior_quantity_policy,
                "updated_quantity_policy": replanned.quantity_policy,
                "prior_decision_snapshot_id": prior_decision_snapshot_id,
                "updated_decision_snapshot_id": existing.decision_snapshot_id,
                "replanned_execution_plan_id": plan.id,
                "recorded_at": datetime.now(UTC).isoformat(),
            },
        )

        replanned.status = "superseded"
        _record_monitoring_event(
            item=replanned,
            event={
                "event": "replanned_sell_monitor_deduplicated",
                "reason": "existing_monitor_updated_in_place",
                "updated_existing_execution_plan_item_id": existing.id,
                "updated_existing_execution_plan_id": existing.execution_plan_id,
                "recorded_at": datetime.now(UTC).isoformat(),
            },
        )


def evaluate_monitoring(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: MonitoringEvaluationRequest,
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    return _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=adapter,
        prices={price.symbol: price for price in command.prices},
        triggered_at=_normalize_utc(now or command.observed_at or datetime.now(UTC)),
        allow_persisted_triggered_without_price=False,
        update_checkpoint_if_idle=True,
    )


def run_autonomous_monitoring_cycle(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    prices: list[MonitoringPriceRequest] | None = None,
    symbols: list[str] | set[str] | None = None,
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    symbol_scope = _ordered_symbol_scope(symbols)
    return _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=adapter,
        prices={price.symbol: price for price in (prices or [])},
        symbols=symbol_scope or None,
        triggered_at=_normalize_utc(now or datetime.now(UTC)),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=False,
    )


def _ordered_symbol_scope(symbols: list[str] | set[str] | None) -> list[str]:
    if not symbols:
        return []
    source = sorted(symbols) if isinstance(symbols, set) else symbols
    ordered: list[str] = []
    seen: set[str] = set()
    for symbol in source:
        if not symbol or symbol in seen:
            continue
        ordered.append(symbol)
        seen.add(symbol)
    return ordered


def list_autonomous_monitoring_symbols(
    *,
    settings: Settings,
    db: Session,
    current_market_date: date | None = None,
) -> list[str]:
    if settings.monitoring_expire_prior_market_watch_buys_enabled:
        expired_count = expire_prior_market_date_unfilled_watch_buys(
            settings=settings,
            db=db,
            current_market_date=current_market_date or datetime.now(KRX_TIMEZONE).date(),
        )
        if expired_count:
            db.commit()

    held_symbols = set(
        db.scalars(
            select(ExecutionPosition.symbol)
            .where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.quantity > 0,
            )
            .order_by(ExecutionPosition.symbol.asc())
        ).all()
    )
    mode_plan_ids = select(ExecutionPlan.id).where(ExecutionPlan.mode == settings.runtime_mode)
    symbol_candidates: list[str] = []

    def append_candidates(rows: list[str]) -> None:
        for symbol in rows:
            if symbol and symbol not in symbol_candidates:
                symbol_candidates.append(symbol)
            if len(symbol_candidates) >= settings.monitoring_max_symbols_per_cycle:
                return

    if held_symbols:
        held_symbol_rows = db.execute(
            select(
                ExecutionPlanItem.symbol,
                func.min(ExecutionPlan.accepted_at).label("first_accepted_at"),
                func.min(ExecutionPlan.id).label("first_plan_id"),
            )
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.symbol.in_(sorted(held_symbols)),
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                ExecutionPlanItem.status.in_(
                    tuple(
                        SELL_ACTIVE_MONITORING_STATUSES
                        | SELL_PENDING_CONFIG_STATUSES
                        | BLOCKED_MONITORING_STATUSES
                        | PENDING_DISPATCH_MONITORING_STATUSES
                    )
                ),
            )
            .group_by(ExecutionPlanItem.symbol)
            .order_by(
                func.min(ExecutionPlan.accepted_at).asc(),
                func.min(ExecutionPlan.id).asc(),
                ExecutionPlanItem.symbol.asc(),
            )
            .limit(settings.monitoring_max_symbols_per_cycle)
        ).all()
        append_candidates(
            [str(row.symbol) for row in held_symbol_rows]
        )

    if len(symbol_candidates) < settings.monitoring_max_symbols_per_cycle:
        pending_sell_symbols = list(
            db.scalars(
                select(distinct(ExecutionPlanItem.symbol))
                .where(
                    ExecutionPlanItem.execution_plan_id.in_(mode_plan_ids),
                    ExecutionPlanItem.side == "sell",
                    ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                    ExecutionPlanItem.status.in_(tuple(SELL_PENDING_CONFIG_STATUSES)),
                )
                .order_by(ExecutionPlanItem.symbol.asc())
                .limit(settings.monitoring_max_symbols_per_cycle * 4)
            ).all()
        )
        fallback_symbols: list[str] = []
        for symbol in pending_sell_symbols:
            if symbol in symbol_candidates:
                continue
            if settings.runtime_mode == "live" and _has_recent_flat_position_truth_for_symbol(
                settings=settings,
                db=db,
                symbol=symbol,
            ):
                continue
            if _has_unexited_buy_order_for_symbol(
                settings=settings,
                db=db,
                symbol=symbol,
            ):
                fallback_symbols.append(symbol)
            if len(symbol_candidates) + len(fallback_symbols) >= settings.monitoring_max_symbols_per_cycle:
                break
        append_candidates(fallback_symbols)

    if len(symbol_candidates) < settings.monitoring_max_symbols_per_cycle:
        append_candidates(
            list(
                db.scalars(
                    select(distinct(ExecutionPlanItem.symbol))
                    .where(
                        ExecutionPlanItem.execution_plan_id.in_(mode_plan_ids),
                        ExecutionPlanItem.side == "buy",
                        ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                        ExecutionPlanItem.status.in_(tuple(WATCH_MONITORING_STATUSES)),
                    )
                    .order_by(ExecutionPlanItem.symbol.asc())
                    .limit(settings.monitoring_max_symbols_per_cycle - len(symbol_candidates))
                ).all()
            )
        )

    return symbol_candidates


def expire_known_flat_pending_sell_monitors(
    *,
    settings: Settings,
    db: Session,
    expired_at: datetime | None = None,
) -> int:
    expired_at = _normalize_utc(expired_at or datetime.now(UTC))
    held_symbols = select(ExecutionPosition.symbol).where(
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity > 0,
    )
    known_flat_symbols = select(ExecutionPosition.symbol).where(
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity <= 0,
    )
    active_watch_symbols = (
        select(ExecutionPlanItem.symbol)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(WATCH_MONITORING_STATUSES)),
        )
    )
    where = [
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(SELL_STALE_FLAT_EXPIRABLE_STATUSES)),
            ExecutionPlanItem.symbol.in_(known_flat_symbols),
            ExecutionPlanItem.symbol.not_in(held_symbols),
            ExecutionPlanItem.symbol.not_in(active_watch_symbols),
    ]
    where.append(~_unexited_buy_sell_monitor_fallback_filter(settings=settings))
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(*where)
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()

    for item in candidates:
        prior_status = item.status
        item.status = "expired"
        _record_monitoring_event(
            item=item,
            event={
                "event": "known_flat_sell_monitor_expired",
                "reason": "known_flat_position",
                "prior_status": prior_status,
                "recorded_at": expired_at.isoformat(),
            },
        )
    return len(candidates)


def release_disabled_account_drawdown_watch_buy_blocks(
    *,
    settings: Settings,
    db: Session,
    released_at: datetime | None = None,
    symbols: list[str] | set[str] | None = None,
) -> int:
    if settings.account_drawdown_gate_enabled:
        return 0
    released_at = _normalize_utc(released_at or datetime.now(UTC))
    where = [
        ExecutionPlan.mode == settings.runtime_mode,
        BrokerOrder.side == "buy",
        BrokerOrder.status == "rejected",
        BrokerOrder.broker_order_id.is_(None),
        BrokerOrder.last_error_classification == "validation",
        BrokerOrder.response_metadata["account_drawdown_gate"].is_not(None),
        ExecutionPlanItem.side == "buy",
        ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
        ExecutionPlanItem.status.in_(("watch_buy_triggered_blocked", "new_buy_blocked")),
    ]
    if symbols:
        where.append(ExecutionPlanItem.symbol.in_(_ordered_symbol_scope(symbols)))
    orders = db.scalars(
        select(BrokerOrder)
        .join(BrokerOrder.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(*where)
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.idempotency_key,
                BrokerOrder.broker_order_id,
                BrokerOrder.symbol,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.last_error_classification,
                BrokerOrder.response_metadata,
                BrokerOrder.created_at,
            ),
            selectinload(BrokerOrder.plan_item).load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.risk_context,
            ),
        )
        .order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())
    ).all()

    released = 0
    for order in orders:
        item = order.plan_item
        if item is None:
            continue
        if order.idempotency_key.endswith(ACCOUNT_DRAWDOWN_GATE_RELEASE_SUFFIX):
            continue
        prior_status = item.status
        prior_idempotency_key = order.idempotency_key
        order.idempotency_key = f"{order.idempotency_key}{ACCOUNT_DRAWDOWN_GATE_RELEASE_SUFFIX}"
        item.status = "watch_monitoring_active"
        _record_monitoring_event(
            item=item,
            event={
                "event": "account_drawdown_gate_disabled_release",
                "reason": "account_drawdown_gate_disabled",
                "prior_status": prior_status,
                "prior_order_id": order.id,
                "prior_order_idempotency_key": prior_idempotency_key,
                "released_order_idempotency_key": order.idempotency_key,
                "recorded_at": released_at.isoformat(),
            },
        )
        db.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_gate_disabled_release",
                source=MONITORING_WORKER_NAME,
                summary={
                    "reason": "account_drawdown_gate_disabled",
                    "prior_status": prior_status,
                    "prior_order_id": order.id,
                    "prior_order_idempotency_key": prior_idempotency_key,
                    "released_order_idempotency_key": order.idempotency_key,
                },
                payload={
                    "account_drawdown_gate": (order.response_metadata or {}).get("account_drawdown_gate"),
                    "prior_order": {
                        "id": order.id,
                        "status": order.status,
                        "last_error_code": order.last_error_code,
                        "last_error_message": order.last_error_message,
                    },
                },
                occurred_at=released_at,
            )
        )
        released += 1
    return released


def _evaluate_monitoring_cycle(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    prices: dict[str, MonitoringPriceRequest],
    symbols: list[str] | set[str] | None = None,
    triggered_at: datetime,
    allow_persisted_triggered_without_price: bool,
    update_checkpoint_if_idle: bool,
) -> EvaluateMonitoringResult:
    released_drawdown_blocks = release_disabled_account_drawdown_watch_buy_blocks(
        settings=settings,
        db=db,
        released_at=triggered_at,
        symbols=symbols,
    )
    if released_drawdown_blocks:
        db.flush()
    position_truth_refreshed_before_selection = False
    if settings.runtime_mode == "live":
        _refresh_position_truth_if_stale(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
        position_truth_refreshed_before_selection = True
    items = _load_monitored_items(
        settings=settings,
        db=db,
        exclude_pending_config=allow_persisted_triggered_without_price,
        symbols=symbols,
    )
    _reactivate_retryable_protective_stops(
        items=items,
        db=db,
        triggered_at=triggered_at,
    )
    sell_monitored_symbols = {
        item.symbol
        for item in items
        if not _is_watch_buy_item(item)
    }
    _reconcile_before_monitoring(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=sell_monitored_symbols,
        refresh_positions=not position_truth_refreshed_before_selection,
    )
    if not position_truth_refreshed_before_selection:
        _refresh_position_truth_if_stale(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=sell_monitored_symbols,
        )
    positions_by_symbol = _load_positions_by_symbol(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    post_entry_maintenance_count, post_entry_review_count = _maintain_post_entry_exit_monitors(
        settings=settings,
        db=db,
        positions_by_symbol=positions_by_symbol,
        prices=prices,
        triggered_at=triggered_at,
    )
    if post_entry_maintenance_count:
        db.flush()
        items = _load_monitored_items(
            settings=settings,
            db=db,
            exclude_pending_config=allow_persisted_triggered_without_price,
            symbols=symbols,
        )
    coverage_issues, repaired_stop_coverage_count = _repair_unprotected_stop_coverage(
        settings=settings,
        db=db,
        positions_by_symbol=positions_by_symbol,
        symbols=symbols,
        triggered_at=triggered_at,
    )
    if repaired_stop_coverage_count:
        db.flush()
        items = _load_monitored_items(
            settings=settings,
            db=db,
            exclude_pending_config=allow_persisted_triggered_without_price,
            symbols=symbols,
        )
        items = [
            item
            for item in items
            if not _is_same_cycle_cloned_stop_repair(item=item, triggered_at=triggered_at)
        ]
    remaining_quantities_by_symbol = {
        symbol: position.quantity
        for symbol, position in positions_by_symbol.items()
        if position.quantity > 0
    }
    if allow_persisted_triggered_without_price:
        items = _filter_sell_items_to_held_symbols(
            items=items,
            positions_by_symbol=positions_by_symbol,
        )
    monitored_symbols = {item.symbol for item in items}
    _reserve_existing_exit_orders(
        settings=settings,
        db=db,
        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
        triggered_at=triggered_at,
    )
    primary_triggered_symbols = _primary_triggered_sell_symbols(
        items=items,
        prices=prices,
        triggered_at=triggered_at,
    )
    result_by_item_id: dict[str, dict[str, object]] = {}
    evaluated_item_ids: list[str] = []
    actions_by_symbol: dict[str, deque[PendingMonitoringAction]] = {}
    symbol_order: list[str] = []
    evaluated_symbols: set[str] = set()
    triggered_symbols: set[str] = set()
    submitted_symbols: set[str] = set()
    retried_triggered_symbols: set[str] = set()
    triggered_item_count = 0
    submitted_order_count = 0
    blocked_item_count = 0
    deferred_item_count = 0
    dispatch_action_count = 0
    retried_triggered_item_count = 0
    watch_buy_cash_reservation: WatchBuyCashReservation | str | None = None

    for item in items:
        if item.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue

        price = prices.get(item.symbol)
        observed_price = price.price if price is not None else None
        retry_persisted_trigger = (
            allow_persisted_triggered_without_price
            and price is None
            and item.status in (
                BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES
            )
        )
        if price is None and not retry_persisted_trigger and item.intent != "max_holding_close_sell":
            continue

        if (
            not allow_persisted_triggered_without_price
            and not _is_watch_buy_item(item)
            and item.symbol not in positions_by_symbol
            and _monitoring_item_is_configured(item)
        ):
            continue

        evaluated_item_ids.append(item.id)
        evaluated_symbols.add(item.symbol)
        if not _monitoring_item_is_configured(item):
            item.status = _pending_configuration_status(item.intent)
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="invalid_configuration",
                message=(
                    "watch buy monitoring item is missing buy-side trigger or quantity configuration"
                    if _is_watch_buy_item(item)
                    else "target/stop monitoring item is missing sell-side trigger configuration"
                ),
            )
            continue

        retry_blocked_until = _watch_buy_retry_blocked_until(
            settings=settings,
            item=item,
        )
        if retry_blocked_until is not None and triggered_at < retry_blocked_until:
            blocked_item_count += 1
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="blocked",
                block_reason="retry_cooldown",
                message=(
                    "watch buy retry cooldown is active after "
                    "account_snapshot_unavailable"
                ),
            )
            continue

        agent_delay_until = _trigger_time_agent_delay_until(
            settings=settings,
            item=item,
        )
        if agent_delay_until is not None and triggered_at < agent_delay_until:
            blocked_item_count += 1
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="agent_delay",
                block_reason="trigger_time_exec_agent_delay",
                message="trigger-time exec-agent requested delayed reevaluation",
            )
            continue

        if _is_watch_buy_item(item) and not retry_persisted_trigger:
            quote_stale_reason = _quote_stale_reason(
                settings=settings,
                price=price,
                cycle_at=triggered_at,
            )
            if quote_stale_reason is not None:
                blocked_item_count += 1
                if item.status != "watch_monitoring_active":
                    item.status = "watch_monitoring_active"
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "watch_buy_quote_rejected",
                        "reason": "stale_quote",
                        "message": quote_stale_reason,
                        "observed_price": _serialize_decimal(observed_price),
                        "observed_at": _serialize_datetime(_price_observed_at(price, triggered_at)),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="stale_quote",
                    message=quote_stale_reason,
                )
                continue

        trigger_reason: str | None = None
        trigger_metadata: dict[str, object] | None = None
        if retry_persisted_trigger:
            persisted_profit_protection = _persisted_profit_protection_trigger(item)
            if persisted_profit_protection is not None:
                trigger_reason = persisted_profit_protection.reason
                trigger_metadata = persisted_profit_protection.metadata
            else:
                trigger_reason = item.intent
        elif _is_watch_buy_item(item):
            trigger_reason, trigger_metadata = _watch_buy_trigger_reason(
                item=item,
                price=price,
                observed_price=observed_price,
                triggered_at=triggered_at,
            )
        elif _is_triggered(
            item=item,
            observed_price=price.price if price is not None else None,
            triggered_at=triggered_at,
        ):
            trigger_reason = item.intent
        elif not _is_watch_buy_item(item) and item.symbol not in primary_triggered_symbols:
            profit_protection = _evaluate_profit_protection(
                settings=settings,
                item=item,
                position=positions_by_symbol.get(item.symbol),
                observed_price=observed_price,
                triggered_at=triggered_at,
            )
            if profit_protection is not None:
                trigger_reason = profit_protection.reason
                trigger_metadata = profit_protection.metadata

        if _is_watch_buy_item(item) and observed_price is not None:
            chase_limit_price = _watch_buy_chase_limit_price(item)
            if chase_limit_price is not None and observed_price > chase_limit_price:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                blocked_item_count += 1
                item.status = "watch_monitoring_active"
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "watch_buy_pullback_wait",
                        "reason": "price_above_chase_limit",
                        "message": "observed price is above chase_limit_price; keep monitoring for pullback",
                        "trigger_price": _serialize_decimal(_watch_buy_trigger_price(item)),
                        "chase_limit_price": _serialize_decimal(chase_limit_price),
                        "observed_price": _serialize_decimal(observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="blocked",
                    block_reason="price_above_chase_limit",
                    message="observed price is above chase_limit_price; monitoring remains active",
                    trigger_reason=trigger_reason,
                    trigger_metadata=trigger_metadata,
                )
                continue

        if trigger_reason is None:
            if item.status in (
                BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES
            ) or item.status in AGENT_ACTIVE_STATUSES:
                item.status = _monitoring_status(item.intent)
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="waiting",
                message="trigger condition not met",
            )
            continue

        if _is_watch_buy_item(item) and observed_price is not None:
            risk_gate = _watch_buy_high_risk_gate_rejection(
                settings=settings,
                item=item,
                observed_price=observed_price,
                price=price,
            )
            if risk_gate is not None:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                blocked_item_count += 1
                retryable_pullback_gate = _is_retryable_watch_buy_pullback_gate(risk_gate["reason"])
                item.status = "watch_monitoring_active" if retryable_pullback_gate else "watch_buy_rejected"
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "watch_buy_pullback_wait" if retryable_pullback_gate else "watch_buy_rejected",
                        "reason": risk_gate["reason"],
                        "message": risk_gate["message"],
                        "details": risk_gate["details"],
                        "observed_price": _serialize_decimal(observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="blocked" if retryable_pullback_gate else "rejected",
                    block_reason=risk_gate["reason"],
                    message=risk_gate["message"],
                )
                continue

        if retry_persisted_trigger:
            retried_triggered_item_count += 1
            retried_triggered_symbols.add(item.symbol)

        triggered_item_count += 1
        triggered_symbols.add(item.symbol)
        if item.symbol not in actions_by_symbol:
            actions_by_symbol[item.symbol] = deque()
            symbol_order.append(item.symbol)
        actions_by_symbol[item.symbol].append(
            PendingMonitoringAction(
                item=item,
                observed_price=observed_price,
                observed_at=_price_observed_at(price, triggered_at) if price is not None else None,
                market_quality_context=_market_quality_context_from_price(price),
                trigger_reason=trigger_reason,
                trigger_metadata=trigger_metadata,
            )
        )

    max_broker_actions = settings.monitoring_max_broker_actions_per_cycle
    pending_symbols = deque(symbol_order)
    while pending_symbols:
        symbol = pending_symbols.popleft()
        symbol_actions = actions_by_symbol.get(symbol)
        if not symbol_actions:
            continue

        action = symbol_actions.popleft()
        if action.item.status in (
            BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES
        ) or action.item.status in AGENT_ACTIVE_STATUSES:
            action.item.status = _monitoring_status(action.item.intent)
        is_watch_buy = _is_watch_buy_item(action.item)
        limit_price_override: Decimal | None = None
        if is_watch_buy:
            action.item.status = "watch_buy_triggered"
            trigger_time_agent_quantity: int | None = None
            trigger_time_agent_enabled = trigger_time_exec_agent.trigger_time_exec_agent_enabled(
                settings=settings,
                item=action.item,
            )
            if trigger_time_agent_enabled and not _watch_buy_entry_line_bypasses_agent(action):
                if _watch_buy_trigger_cross_pullback_bypasses_agent(action):
                    agent_result = _resolve_trigger_time_quality_gate_delay(
                        settings=settings,
                        db=db,
                        adapter=adapter,
                        action=action,
                        triggered_at=triggered_at,
                    )
                else:
                    agent_result = _resolve_trigger_time_agent_action(
                        settings=settings,
                        db=db,
                        adapter=adapter,
                        action=action,
                        triggered_at=triggered_at,
                        intended_quantity=None,
                        remaining_quantity=None,
                    )
                if agent_result is not None:
                    if agent_result.quantity is None:
                        blocked_item_count += 1
                        result_by_item_id[action.item.id] = agent_result.result_payload
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    trigger_time_agent_quantity = agent_result.quantity
                    limit_price_override = agent_result.limit_price
            approval = _resolve_watch_buy_order(
                settings=settings,
                db=db,
                adapter=adapter,
                item=action.item,
                observed_price=action.observed_price,
                requested_quantity=trigger_time_agent_quantity,
                trigger_time_agent_approved=trigger_time_agent_quantity is not None,
                sizing_source=(
                    "trigger_time_exec_agent_approval"
                    if trigger_time_agent_quantity is not None
                    else None
                ),
            )
            quantity = approval.quantity
            if quantity is None:
                blocked_item_count += 1
                if approval.reason in (
                    BLOCKED_WATCH_BUY_APPROVAL_REASONS | RETRYABLE_WATCH_BUY_APPROVAL_REASONS
                ):
                    action.item.status = "watch_buy_triggered_blocked"
                    event: dict[str, object] = {
                        "event": "watch_buy_triggered_blocked",
                        "reason": approval.reason,
                        "message": approval.message,
                        "observed_price": _serialize_decimal(action.observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    }
                    if approval.details:
                        event["details"] = approval.details
                    if approval.reason in RETRYABLE_WATCH_BUY_APPROVAL_REASONS:
                        event["retry_after_at"] = (
                            triggered_at
                            + timedelta(
                                seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                            )
                        ).isoformat()
                    _record_monitoring_event(
                        item=action.item,
                        event=event,
                    )
                    result_by_item_id[action.item.id] = _monitoring_result_payload(
                        item=action.item,
                        observed_price=action.observed_price,
                        outcome="blocked",
                        block_reason=approval.reason,
                        message=approval.message,
                        trigger_metadata=approval.details,
                    )
                else:
                    action.item.status = "watch_buy_rejected"
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_rejected",
                            "reason": approval.reason or "risk_rejected",
                            "message": approval.message,
                            "details": approval.details or {},
                            "observed_price": _serialize_decimal(action.observed_price),
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                    result_by_item_id[action.item.id] = _monitoring_result_payload(
                        item=action.item,
                        observed_price=action.observed_price,
                        outcome="rejected",
                        message=approval.message,
                    )
                if trigger_time_agent_quantity is not None:
                    _record_trigger_time_watch_buy_sizing_result(
                        item=action.item,
                        approval=approval,
                        requested_quantity=trigger_time_agent_quantity,
                        recorded_at=triggered_at,
                    )
                if symbol_actions:
                    pending_symbols.append(symbol)
                continue
            if trigger_time_agent_quantity is not None:
                _record_trigger_time_watch_buy_sizing_result(
                    item=action.item,
                    approval=approval,
                    requested_quantity=trigger_time_agent_quantity,
                    recorded_at=triggered_at,
                )
        else:
            quantity = _resolve_sell_quantity(
                item=action.item,
                remaining_quantity=remaining_quantities_by_symbol.get(action.item.symbol),
            )
            if quantity is None:
                reconciliation_error = _latest_reconciliation_error_for_symbol(
                    settings=settings,
                    db=db,
                    symbol=action.item.symbol,
                )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome=(
                        "reconciliation_blocked"
                        if reconciliation_error is not None
                        else "no_position"
                    ),
                    message=(
                        "sellable position is unknown because broker reconciliation failed: "
                        f"{reconciliation_error}"
                        if reconciliation_error is not None
                        else "no sellable execution position is available for this trigger"
                    ),
                )
                if symbol_actions:
                    pending_symbols.append(symbol)
                continue
            if trigger_time_exec_agent.trigger_time_exec_agent_enabled(
                settings=settings,
                item=action.item,
            ):
                agent_result = _resolve_trigger_time_agent_action(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    action=action,
                    triggered_at=triggered_at,
                    intended_quantity=quantity,
                    remaining_quantity=remaining_quantities_by_symbol.get(action.item.symbol),
                )
                if agent_result.quantity is None:
                    blocked_item_count += 1
                    result_by_item_id[action.item.id] = agent_result.result_payload
                    if symbol_actions:
                        pending_symbols.append(symbol)
                    continue
                quantity = agent_result.quantity
                limit_price_override = agent_result.limit_price

        if dispatch_action_count >= max_broker_actions:
            symbol_actions.appendleft(action)
            pending_symbols.appendleft(symbol)
            break
        else:
            before_success_commit: Callable[[], None] | None = None
            if not is_watch_buy:
                before_success_commit = partial(
                    _reserve_and_rearm_sell_after_success,
                    settings=settings,
                    item=action.item,
                    position=positions_by_symbol.get(action.item.symbol),
                    remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                    submitted_quantity=quantity,
                    recorded_at=triggered_at,
                )
            else:
                control_state = ensure_runtime_control_state(settings=settings, db=db)
                if automated_order_submission_allowed(control_state):
                    if watch_buy_cash_reservation is None:
                        watch_buy_cash_reservation = _load_watch_buy_cash_reservation(
                            settings=settings,
                            db=db,
                            adapter=adapter,
                            triggered_at=triggered_at,
                        )
                    if isinstance(watch_buy_cash_reservation, str):
                        blocked_item_count += 1
                        action.item.status = "watch_buy_triggered_blocked"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_triggered_blocked",
                                "reason": "account_snapshot_unavailable",
                                "message": watch_buy_cash_reservation,
                                "observed_price": _serialize_decimal(action.observed_price),
                                "recorded_at": triggered_at.isoformat(),
                                "retry_after_at": (
                                    triggered_at
                                    + timedelta(seconds=settings.monitoring_watch_buy_retry_cooldown_seconds)
                                ).isoformat(),
                            },
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="blocked",
                            block_reason="account_snapshot_unavailable",
                            message=watch_buy_cash_reservation,
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    limit_price = limit_price_override or _watch_buy_limit_price(
                        item=action.item,
                        observed_price=action.observed_price,
                    )
                    candidate_notional = _watch_buy_order_notional(
                        quantity=quantity,
                        limit_price=limit_price,
                    )
                    if candidate_notional is None:
                        blocked_item_count += 1
                        action.item.status = "watch_buy_rejected"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_rejected",
                                "reason": RESERVED_CASH_EXHAUSTED_REASON,
                                "message": "watch buy requires limit-price notional for reserved cash guard",
                                "recorded_at": triggered_at.isoformat(),
                            },
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="rejected",
                            block_reason=RESERVED_CASH_EXHAUSTED_REASON,
                            message="watch buy requires limit-price notional for reserved cash guard",
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    reservation_payload = _watch_buy_cash_reservation_payload(
                        reservation=watch_buy_cash_reservation,
                        candidate_notional=candidate_notional,
                        item=action.item,
                        quantity=quantity,
                        limit_price=limit_price,
                        triggered_at=triggered_at,
                    )
                    if candidate_notional > watch_buy_cash_reservation.remaining_cash:
                        blocked_item_count += 1
                        action.item.status = "watch_buy_rejected"
                        _record_reserved_cash_exhausted(
                            db=db,
                            item=action.item,
                            payload=reservation_payload,
                            triggered_at=triggered_at,
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="rejected",
                            block_reason=RESERVED_CASH_EXHAUSTED_REASON,
                            message="watch buy reserved cash budget exhausted before broker submission",
                            trigger_metadata=reservation_payload,
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    watch_buy_cash_reservation.already_reserved_notional += candidate_notional

            dispatch_action_count += 1
            placement = _place_monitoring_order(
                settings=settings,
                db=db,
                adapter=adapter,
                item=action.item,
                quantity=quantity,
                observed_price=action.observed_price,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
                triggered_at=triggered_at,
                limit_price_override=limit_price_override,
                before_success_commit=before_success_commit,
            )
            if placement.status_code == 201:
                if is_watch_buy:
                    action.item.status = "watch_buy_submitted"
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_submitted",
                            "observed_price": _serialize_decimal(action.observed_price),
                            "quantity": quantity,
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                submitted_order_count += 1
                submitted_symbols.add(action.item.symbol)
                trigger_time_exec_agent.attach_order_to_last_trigger_time_interaction(
                    item=action.item,
                    order=placement.payload.get("order") if isinstance(placement.payload.get("order"), dict) else None,
                    new_status=action.item.status,
                    recorded_at=triggered_at,
                )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="submitted",
                    order=placement.payload.get("order"),
                    trigger_reason=action.trigger_reason,
                    trigger_metadata=action.trigger_metadata,
                )
            elif placement.status_code == 202 and placement.payload.get("deferred") is True:
                deferred_item_count += 1
                action.item.status = _pending_dispatch_status(action.item.intent)
                _record_monitoring_event(
                    item=action.item,
                    event={
                        "event": "monitoring_order_deferred",
                        "reason": placement.payload.get("message"),
                        "route": placement.payload.get("route"),
                        "observed_price": _serialize_decimal(action.observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                db.commit()
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="deferred",
                    message=placement.payload.get("message"),
                )
            elif placement.status_code == 409 and "block_reason" in placement.payload:
                blocked_item_count += 1
                action.item.status = (
                    "watch_buy_triggered_blocked"
                    if is_watch_buy
                    else f"{action.item.intent}_triggered_blocked"
                )
                if is_watch_buy:
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_triggered_blocked",
                            "reason": placement.payload.get("block_reason"),
                            "message": placement.payload.get("message"),
                            "observed_price": _serialize_decimal(action.observed_price),
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                db.commit()
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="blocked",
                    block_reason=placement.payload.get("block_reason"),
                    message=placement.payload.get("message"),
                )
            elif placement.payload.get("duplicate") is True:
                duplicate_quantity = _serialized_order_quantity(placement.payload.get("order"))
                if is_watch_buy:
                    action.item.status = "watch_buy_submitted"
                elif duplicate_quantity is not None:
                    _reserve_remaining_quantity(
                        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                        symbol=action.item.symbol,
                        quantity=duplicate_quantity,
                    )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="duplicate",
                    order=placement.payload.get("order"),
                    message=placement.payload.get("message"),
                )
            else:
                if is_watch_buy:
                    action.item.status = "watch_buy_rejected"
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_rejected",
                            "reason": "order_submission_failed",
                            "message": placement.payload.get("message"),
                            "observed_price": _serialize_decimal(action.observed_price),
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="error",
                    message=placement.payload.get("message"),
                )

        if symbol_actions:
            pending_symbols.append(symbol)

    backlog_symbols: set[str] = set()
    for symbol, symbol_actions in actions_by_symbol.items():
        while symbol_actions:
            action = symbol_actions.popleft()
            backlog_symbols.add(symbol)
            deferred_item_count += 1
            action.item.status = _pending_dispatch_status(action.item.intent)
            result_by_item_id[action.item.id] = _monitoring_result_payload(
                item=action.item,
                observed_price=action.observed_price,
                outcome="deferred",
                message="broker action capacity reached for this monitoring cycle",
            )

    results = [
        result_by_item_id[item_id]
        for item_id in evaluated_item_ids
        if item_id in result_by_item_id
    ]
    db.flush()
    agent_status_counts = _trigger_time_agent_status_counts(
        settings=settings,
        db=db,
    )
    checkpoint_details = {
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "selection_strategy": MONITORING_SELECTION_STRATEGY,
        "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        "max_broker_actions_per_cycle": max_broker_actions,
        "dispatch_action_count": dispatch_action_count,
        "evaluated_item_count": len(results),
        "triggered_item_count": triggered_item_count,
        "submitted_order_count": submitted_order_count,
        "blocked_item_count": blocked_item_count,
        "deferred_item_count": deferred_item_count,
        "price_count": len(prices),
        "evaluated_symbol_count": len(evaluated_symbols),
        "triggered_symbol_count": len(triggered_symbols),
        "retried_triggered_item_count": retried_triggered_item_count,
        "retried_triggered_symbols": sorted(retried_triggered_symbols),
        "submitted_symbols": sorted(submitted_symbols),
        "backlog_item_count": deferred_item_count,
        "backlog_symbols": sorted(backlog_symbols),
        "agent_watching_count": agent_status_counts["watching"],
        "agent_pending_decision_count": agent_status_counts["pending_agent_decision"],
        "agent_delayed_count": agent_status_counts["agent_delayed"],
        "agent_rejected_count": agent_status_counts["agent_rejected"],
        "agent_approved_submitted_count": agent_status_counts["agent_approved_submitted"],
        "post_entry_review_count": post_entry_review_count,
        "protective_stop_coverage": _protective_stop_coverage_payload(
            issues=coverage_issues,
            repaired_count=repaired_stop_coverage_count,
        ),
    }

    checkpoint = None
    if items or update_checkpoint_if_idle:
        degraded_coverage_count = sum(1 for issue in coverage_issues if issue.outcome != "rearmed")
        checkpoint = _update_monitoring_checkpoint(
            settings=settings,
            db=db,
            worker_name=MONITORING_WORKER_NAME,
            triggered_at=triggered_at,
            status="degraded" if deferred_item_count > 0 or degraded_coverage_count > 0 else "running",
            details=checkpoint_details,
        )
    return EvaluateMonitoringResult(
        status_code=200,
        payload={
            "triggered_at": triggered_at.isoformat(),
            "worker_name": checkpoint.worker_name if checkpoint is not None else MONITORING_WORKER_NAME,
            "evaluated_item_count": len(results),
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": submitted_order_count,
            "blocked_item_count": blocked_item_count,
            "deferred_item_count": deferred_item_count,
            "evaluated_symbol_count": checkpoint_details["evaluated_symbol_count"],
            "triggered_symbol_count": checkpoint_details["triggered_symbol_count"],
            "submitted_symbols": checkpoint_details["submitted_symbols"],
            "protective_stop_coverage": checkpoint_details["protective_stop_coverage"],
            "items": results,
        },
    )


def expire_prior_market_date_unfilled_watch_buys(
    *,
    settings: Settings,
    db: Session,
    current_market_date: date,
    expired_at: datetime | None = None,
) -> int:
    expired_at = _normalize_utc(expired_at or datetime.now(UTC))
    expiry_market_date = func.coalesce(
        ExecutionPlan.market_date,
        ExecutionPlanItem.source_market_date,
    )
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(WATCH_BUY_EXPIRABLE_STATUSES)),
            expiry_market_date.is_not(None),
            expiry_market_date < current_market_date,
            ~ExecutionPlanItem.broker_orders.any(),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_market_date,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()

    for item in candidates:
        prior_status = item.status
        item.status = "watch_expired"
        item_source_market_date = item.plan.market_date or item.source_market_date
        _record_monitoring_event(
            item=item,
            event={
                "event": "market_date_expired_unfilled",
                "reason": "prior_market_date_unfilled_watch_buy",
                "prior_status": prior_status,
                "source_market_date": _serialize_date(item_source_market_date),
                "current_market_date": current_market_date.isoformat(),
                "recorded_at": expired_at.isoformat(),
            },
        )
    return len(candidates)


def _resolve_trigger_time_agent_action(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    action: PendingMonitoringAction,
    triggered_at: datetime,
    intended_quantity: int | None,
    remaining_quantity: int | None,
) -> TriggerTimeAgentActionResult:
    item = action.item
    previous_status = item.status
    item.status = AGENT_PENDING_STATUS
    _record_monitoring_event(
        item=item,
        event={
            "event": "trigger_time_exec_agent_pending",
            "trigger_reason": action.trigger_reason or item.intent,
            "observed_price": _serialize_decimal(action.observed_price),
            "recorded_at": triggered_at.isoformat(),
        },
    )
    db.commit()

    resolution = trigger_time_exec_agent.resolve_trigger_time_exec_agent_decision(
        settings=settings,
        db=db,
        adapter=adapter,
        item=item,
        observed_price=action.observed_price,
        observed_at=_price_observed_at_from_action(action=action, fallback=triggered_at),
        triggered_at=triggered_at,
        trigger_reason=action.trigger_reason,
        trigger_metadata=action.trigger_metadata,
        live_market_context=action.market_quality_context,
        intended_quantity=intended_quantity,
        remaining_quantity=remaining_quantity,
    )
    decision = resolution.decision
    if decision.decision == "keep_watching":
        item.status = _monitoring_status(item.intent)
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_keep_watching",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "valid_until": _serialize_datetime(decision.valid_until),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="waiting",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    if decision.decision == "delay":
        item.status = AGENT_DELAY_STATUS
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_delay",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "valid_until": _serialize_datetime(decision.valid_until),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="agent_delay",
                block_reason="trigger_time_exec_agent_delay",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    if decision.decision == "reject":
        item.status = AGENT_REJECTED_STATUS
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_rejected",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="rejected",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    item.status = AGENT_APPROVED_PENDING_SUBMIT_STATUS
    _apply_trigger_time_agent_adjustments(item=item, decision=decision)
    _record_monitoring_event(
        item=item,
        event={
            "event": "trigger_time_exec_agent_approved",
            "reason": decision.reason,
            "quantity": decision.quantity,
            "limit_price": _serialize_decimal(decision.limit_price),
            "updated_target_price": _serialize_decimal(decision.updated_target_price),
            "updated_stop_price": _serialize_decimal(decision.updated_stop_price),
            "observed_price": _serialize_decimal(action.observed_price),
            "market_quality_gate": _trigger_time_market_quality_gate(resolution),
            "recorded_at": triggered_at.isoformat(),
        },
    )
    trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
        item=item,
        resolution=resolution,
        previous_status=previous_status,
        new_status=item.status,
        recorded_at=triggered_at,
    )
    return TriggerTimeAgentActionResult(
        quantity=decision.quantity,
        limit_price=decision.limit_price,
        result_payload={},
    )


def _resolve_trigger_time_quality_gate_delay(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    action: PendingMonitoringAction,
    triggered_at: datetime,
) -> TriggerTimeAgentActionResult | None:
    market_context, stale_reason = trigger_time_exec_agent._market_context(
        settings=settings,
        item=action.item,
        observed_price=action.observed_price,
        observed_at=_price_observed_at_from_action(action=action, fallback=triggered_at),
        triggered_at=triggered_at,
        trigger_reason=action.trigger_reason,
        trigger_metadata=action.trigger_metadata,
        live_market_context=action.market_quality_context,
    )
    if stale_reason is None and market_context.get("live_execution_context_available") is True:
        return None
    return _resolve_trigger_time_agent_action(
        settings=settings,
        db=db,
        adapter=adapter,
        action=action,
        triggered_at=triggered_at,
        intended_quantity=None,
        remaining_quantity=None,
    )


def _price_observed_at_from_action(
    *,
    action: PendingMonitoringAction,
    fallback: datetime,
) -> datetime | None:
    if action.observed_price is None:
        return None
    return action.observed_at or fallback


def _market_quality_context_from_price(
    price: MonitoringPriceRequest | None,
) -> dict[str, object] | None:
    if price is None:
        return None
    return {
        "source": "monitoring_price_request",
        "bid": _serialize_decimal(price.bid),
        "ask": _serialize_decimal(price.ask),
        "spread": _serialize_decimal(price.spread),
        "spread_bps": _serialize_decimal(price.spread_bps),
        "orderbook_depth": _serialize_decimal(price.orderbook_depth),
        "orderbook_imbalance": _serialize_decimal(price.orderbook_imbalance),
        "volume": _serialize_decimal(price.volume),
        "turnover": _serialize_decimal(price.turnover),
        "day_high": _serialize_decimal(price.day_high),
        "day_low": _serialize_decimal(price.day_low),
        "session_type": price.session_type,
        "normal_ordering_available": price.normal_ordering_available,
        "provenance": price.provenance or {},
    }


def _trigger_time_market_quality_gate(
    resolution: trigger_time_exec_agent.TriggerTimeExecAgentResolution,
) -> dict[str, object] | None:
    market_context = resolution.request_payload.get("market_context")
    if not isinstance(market_context, dict):
        return None
    gate = market_context.get("live_execution_quality_gate")
    return gate if isinstance(gate, dict) else None


def _apply_trigger_time_agent_adjustments(
    *,
    item: ExecutionPlanItem,
    decision: trigger_time_exec_agent.TriggerTimeExecAgentDecision,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    if decision.limit_price is not None:
        item.limit_price = decision.limit_price
        risk_context["trigger_time_limit_price"] = _serialize_decimal(decision.limit_price)
    if decision.updated_target_price is not None:
        risk_context["target_price"] = _serialize_decimal(decision.updated_target_price)
        risk_context["updated_target_price"] = _serialize_decimal(decision.updated_target_price)
        _update_sibling_trigger_price(
            item=item,
            intent="target_sell",
            trigger_price=decision.updated_target_price,
        )
    if decision.updated_stop_price is not None:
        risk_context["stop_price"] = _serialize_decimal(decision.updated_stop_price)
        risk_context["stop_loss_price"] = _serialize_decimal(decision.updated_stop_price)
        risk_context["updated_stop_price"] = _serialize_decimal(decision.updated_stop_price)
        _update_sibling_trigger_price(
            item=item,
            intent="stop_sell",
            trigger_price=decision.updated_stop_price,
        )
    item.risk_context = risk_context


def _record_trigger_time_watch_buy_sizing_result(
    *,
    item: ExecutionPlanItem,
    approval: WatchBuyOrderApproval,
    requested_quantity: int,
    recorded_at: datetime,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return

    if approval.quantity is not None:
        outcome = "approved"
    elif approval.reason in ({"paused", "kill_switch"} | RETRYABLE_WATCH_BUY_APPROVAL_REASONS):
        outcome = "blocked"
    else:
        outcome = "rejected"

    deterministic_sizing: dict[str, object] = {
        "source": "watch_buy_deterministic_sizing",
        "outcome": outcome,
        "requested_quantity": requested_quantity,
        "approved_quantity": approval.quantity,
        "reason": approval.reason,
        "message": approval.message,
        "details": approval.details or {},
        "recorded_at": recorded_at.isoformat(),
    }
    if approval.quantity is not None and approval.quantity < requested_quantity:
        deterministic_sizing["adjustment"] = "clamped_by_deterministic_risk_sizing"

    updated_interaction = {
        **interaction,
        "deterministic_sizing": deterministic_sizing,
        "new_monitoring_state": item.status,
    }
    interactions = risk_context.get("trigger_time_exec_agent_interactions")
    if isinstance(interactions, list) and interactions:
        interactions = [*interactions[:-1], updated_interaction]
    else:
        interactions = [updated_interaction]
    risk_context["last_trigger_time_exec_agent_interaction"] = updated_interaction
    risk_context["trigger_time_exec_agent_interactions"] = interactions[-20:]
    item.risk_context = risk_context


def _update_sibling_trigger_price(
    *,
    item: ExecutionPlanItem,
    intent: str,
    trigger_price: Decimal,
) -> None:
    plan = item.plan
    if plan is None:
        return
    for sibling in plan.items:
        if sibling.symbol != item.symbol or sibling.intent != intent:
            continue
        if sibling.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue
        sibling.trigger_price = trigger_price


def _load_monitored_items(
    *,
    settings: Settings,
    db: Session,
    exclude_pending_config: bool = False,
    symbols: list[str] | set[str] | None = None,
) -> list[ExecutionPlanItem]:
    monitored_item_filter = _monitored_item_filter()
    if symbols:
        monitored_item_filter = and_(
            monitored_item_filter,
            ExecutionPlanItem.symbol.in_(sorted(symbols)),
        )
    if exclude_pending_config:
        held_symbols = select(ExecutionPosition.symbol).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.quantity > 0,
        )
        if symbols:
            held_symbols = held_symbols.where(ExecutionPosition.symbol.in_(sorted(symbols)))
        sell_symbol_conditions = [
            ExecutionPlanItem.symbol.in_(held_symbols),
            _unexited_buy_sell_monitor_fallback_filter(settings=settings),
        ]
        monitored_item_filter = and_(
            monitored_item_filter,
            or_(
                ExecutionPlanItem.side != "sell",
                or_(*sell_symbol_conditions),
            ),
        )
    if symbols:
        symbol_list = _ordered_symbol_scope(symbols)
        sell_fast_statuses = (
            SELL_ACTIVE_MONITORING_STATUSES
            | BLOCKED_MONITORING_STATUSES
            | PENDING_DISPATCH_MONITORING_STATUSES
            | AGENT_ACTIVE_STATUSES
        )
        if not exclude_pending_config:
            sell_fast_statuses = sell_fast_statuses | SELL_PENDING_CONFIG_STATUSES
        sell_items = db.scalars(
            select(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.symbol.in_(symbol_list),
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(sell_fast_statuses)),
            )
            .options(
                load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.execution_plan_id,
                    ExecutionPlanItem.plan_item_index,
                    ExecutionPlanItem.symbol,
                    ExecutionPlanItem.side,
                    ExecutionPlanItem.intent,
                    ExecutionPlanItem.status,
                    ExecutionPlanItem.quantity_policy,
                    ExecutionPlanItem.limit_price,
                    ExecutionPlanItem.trigger_price,
                    ExecutionPlanItem.time_in_force,
                    ExecutionPlanItem.source_market_date,
                ),
                selectinload(ExecutionPlanItem.plan).load_only(
                    ExecutionPlan.id,
                    ExecutionPlan.mode,
                    ExecutionPlan.market_date,
                    ExecutionPlan.accepted_at,
                )
            )
            .order_by(
                ExecutionPlanItem.symbol.asc(),
                ExecutionPlanItem.plan_item_index.asc(),
                ExecutionPlanItem.id.asc(),
            )
            .limit(settings.monitoring_max_symbols_per_cycle * 16)
        ).all()
        buy_items = db.scalars(
            select(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.symbol.in_(symbol_list),
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(WATCH_MONITORING_STATUSES)),
            )
            .options(
                load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.execution_plan_id,
                    ExecutionPlanItem.plan_item_index,
                    ExecutionPlanItem.symbol,
                    ExecutionPlanItem.side,
                    ExecutionPlanItem.intent,
                    ExecutionPlanItem.status,
                    ExecutionPlanItem.quantity_policy,
                    ExecutionPlanItem.limit_price,
                    ExecutionPlanItem.trigger_price,
                    ExecutionPlanItem.time_in_force,
                    ExecutionPlanItem.source_market_date,
                ),
                selectinload(ExecutionPlanItem.plan).load_only(
                    ExecutionPlan.id,
                    ExecutionPlan.mode,
                    ExecutionPlan.market_date,
                    ExecutionPlan.accepted_at,
                )
            )
            .order_by(
                ExecutionPlanItem.symbol.asc(),
                ExecutionPlanItem.plan_item_index.asc(),
                ExecutionPlanItem.id.asc(),
            )
            .limit(settings.monitoring_max_symbols_per_cycle * 16)
        ).all()
        symbol_order = {symbol: index for index, symbol in enumerate(symbol_list)}
        selected_items = sorted(
            [*sell_items, *buy_items],
            key=lambda item: (
                symbol_order.get(item.symbol, len(symbol_order)),
                item.plan_item_index,
                item.id,
            ),
        )
        return selected_items
    latest_plan_candidates = (
        select(
            ExecutionPlanItem.symbol.label("symbol"),
            ExecutionPlan.id.label("plan_id"),
            ExecutionPlan.accepted_at.label("accepted_at"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlan.market_date.desc(),
                    ExecutionPlan.accepted_at.desc(),
                    ExecutionPlan.id.desc(),
                ),
            )
            .label("symbol_rank"),
        )
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_item_filter,
        )
        .subquery()
    )
    # Bound the working set by distinct symbol, and only evaluate the latest
    # monitorable plan for each symbol so stale prior-day target/stop rows do
    # not submit duplicate exits after a newer plan supersedes them.
    selected_symbol_plans = (
        select(
            latest_plan_candidates.c.symbol,
            latest_plan_candidates.c.plan_id,
        )
        .where(latest_plan_candidates.c.symbol_rank == 1)
        .order_by(
            latest_plan_candidates.c.accepted_at.asc(),
            latest_plan_candidates.c.plan_id.asc(),
        )
        .limit(settings.monitoring_max_symbols_per_cycle)
    )
    selected_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_item_filter,
            tuple_(ExecutionPlanItem.symbol, ExecutionPlanItem.execution_plan_id).in_(selected_symbol_plans),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.source_market_date,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    selected_items = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=selected_items,
        symbols=symbols,
    )
    return _append_held_stop_sell_fallbacks(
        settings=settings,
        db=db,
        monitored_item_filter=monitored_item_filter,
        selected_items=selected_items,
        symbols=symbols,
    )


def _filter_sell_items_to_held_symbols(
    *,
    items: list[ExecutionPlanItem],
    positions_by_symbol: dict[str, ExecutionPosition],
) -> list[ExecutionPlanItem]:
    return [
        item
        for item in items
        if _is_watch_buy_item(item) or item.symbol in positions_by_symbol
    ]


def _trigger_time_agent_status_counts(
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
    watching_statuses = (
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "target_sell_monitoring",
        "stop_sell_monitoring",
        "max_holding_close_sell_monitoring",
    )
    approved_submitted_statuses = (
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
        "watch_buy_submitted",
        "broker_order_submission_pending",
        "broker_order_submitted",
        "submitted",
        "open",
        "partially_filled",
    )
    return {
        "watching": sum(counts_by_status.get(status, 0) for status in watching_statuses),
        "pending_agent_decision": counts_by_status.get(AGENT_PENDING_STATUS, 0),
        "agent_delayed": counts_by_status.get(AGENT_DELAY_STATUS, 0),
        "agent_rejected": counts_by_status.get(AGENT_REJECTED_STATUS, 0),
        "agent_approved_submitted": sum(
            counts_by_status.get(status, 0)
            for status in approved_submitted_statuses
        ),
    }


def _append_held_active_sell_monitor_reconciliations(
    *,
    settings: Settings,
    db: Session,
    selected_items: list[ExecutionPlanItem],
    symbols: set[str] | None = None,
) -> list[ExecutionPlanItem]:
    held_where = [
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity > 0,
    ]
    if symbols:
        held_where.append(ExecutionPosition.symbol.in_(sorted(symbols)))
    held_symbols = set(
        db.scalars(
            select(ExecutionPosition.symbol).where(*held_where)
        ).all()
    )
    if not held_symbols:
        return selected_items

    selected_item_ids = {item.id for item in selected_items}
    selected_symbols = {item.symbol for item in selected_items}
    if not selected_symbols:
        return selected_items
    selected_sell_replacements = {
        (item.symbol, item.intent): item
        for item in selected_items
        if item.side == "sell" and item.intent in SELL_MONITORED_INTENTS
    }
    eligible_symbols = held_symbols & selected_symbols
    if not eligible_symbols:
        return selected_items
    active_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(SELL_ACTIVE_MONITORING_STATUSES)),
            ExecutionPlanItem.symbol.in_(eligible_symbols),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.risk_context,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()

    append_items: list[ExecutionPlanItem] = []
    for item in active_items:
        if item.id in selected_item_ids:
            continue
        replacement = selected_sell_replacements.get((item.symbol, item.intent))
        if replacement is not None:
            prior_status = item.status
            item.status = "superseded"
            _record_monitoring_event(
                item=item,
                event={
                    "event": "active_sell_monitor_superseded",
                    "reason": "newer_same_intent_monitor_selected",
                    "prior_status": prior_status,
                    "superseded_by_execution_plan_item_id": replacement.id,
                    "superseded_by_execution_plan_id": replacement.execution_plan_id,
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
            )
            continue
        append_items.append(item)

    if not append_items:
        return selected_items
    return sorted(
        [*selected_items, *append_items],
        key=lambda item: (
            item.plan.accepted_at,
            item.plan.id,
            item.plan_item_index,
            item.id,
        ),
    )


def _append_held_stop_sell_fallbacks(
    *,
    settings: Settings,
    db: Session,
    monitored_item_filter,
    selected_items: list[ExecutionPlanItem],
    symbols: set[str] | None = None,
) -> list[ExecutionPlanItem]:
    symbols_with_stop = {
        item.symbol
        for item in selected_items
        if item.side == "sell" and item.intent == "stop_sell"
    }
    held_where = [
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity > 0,
    ]
    if symbols:
        held_where.append(ExecutionPosition.symbol.in_(sorted(symbols)))
    held_symbols = set(
        db.scalars(
            select(ExecutionPosition.symbol).where(*held_where)
        ).all()
    )
    fallback_symbols = held_symbols - symbols_with_stop
    if not fallback_symbols:
        return selected_items
    fallback_symbols = sorted(fallback_symbols)

    fallback_candidates = (
        select(
            ExecutionPlanItem.id.label("item_id"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlan.market_date.desc(),
                    ExecutionPlan.accepted_at.desc(),
                    ExecutionPlan.id.desc(),
                    ExecutionPlanItem.plan_item_index.asc(),
                    ExecutionPlanItem.id.asc(),
                ),
            )
            .label("stop_rank"),
        )
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            or_(
                monitored_item_filter,
                and_(
                    ExecutionPlanItem.status.in_(tuple(FALLBACK_PROTECTIVE_STOP_STATUSES)),
                    ExecutionPlanItem.side == "sell",
                    ExecutionPlanItem.intent == "stop_sell",
                ),
            ),
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.symbol.in_(fallback_symbols),
        )
        .subquery()
    )
    fallback_item_ids = select(fallback_candidates.c.item_id).where(
        fallback_candidates.c.stop_rank == 1
    )
    selected_item_ids = {item.id for item in selected_items}
    fallback_items = [
        item
        for item in db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.id.in_(fallback_item_ids),
            )
            .options(
                load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.execution_plan_id,
                    ExecutionPlanItem.plan_item_index,
                    ExecutionPlanItem.symbol,
                    ExecutionPlanItem.side,
                    ExecutionPlanItem.intent,
                    ExecutionPlanItem.status,
                    ExecutionPlanItem.risk_context,
                ),
                selectinload(ExecutionPlanItem.plan).load_only(
                    ExecutionPlan.id,
                    ExecutionPlan.mode,
                    ExecutionPlan.market_date,
                    ExecutionPlan.accepted_at,
                ),
            )
            .order_by(
                ExecutionPlan.accepted_at.asc(),
                ExecutionPlan.id.asc(),
                ExecutionPlanItem.plan_item_index.asc(),
                ExecutionPlanItem.id.asc(),
            )
        ).all()
        if item.id not in selected_item_ids
        and (
            item.status not in RETRYABLE_PROTECTIVE_STOP_STATUSES
            or _latest_order_failure_is_retryable_session_rejection(db=db, item=item)
        )
    ]
    if not fallback_items:
        return selected_items
    return sorted(
        [*selected_items, *fallback_items],
        key=lambda item: (
            item.plan.accepted_at,
            item.plan.id,
            item.plan_item_index,
            item.id,
        ),
    )


def _reactivate_retryable_protective_stops(
    *,
    items: list[ExecutionPlanItem],
    db: Session,
    triggered_at: datetime,
) -> None:
    for item in items:
        if (
            item.side != "sell"
            or item.intent != "stop_sell"
            or item.status not in RETRYABLE_PROTECTIVE_STOP_STATUSES
        ):
            continue
        if not _latest_order_failure_is_retryable_session_rejection(db=db, item=item):
            continue
        prior_status = item.status
        item.status = _monitoring_status(item.intent)
        _record_monitoring_event(
            item=item,
            event={
                "event": "protective_stop_rearmed",
                "reason": "retryable_session_rejection",
                "prior_status": prior_status,
                "recorded_at": triggered_at.isoformat(),
            },
        )


def _repair_unprotected_stop_coverage(
    *,
    settings: Settings,
    db: Session,
    positions_by_symbol: dict[str, ExecutionPosition],
    symbols: set[str] | None = None,
    triggered_at: datetime,
) -> tuple[list[ProtectiveStopCoverageIssue], int]:
    positions = {
        symbol: position
        for symbol, position in positions_by_symbol.items()
        if position.quantity > 0
    }
    if not positions:
        return _stale_position_truth_issues(
            settings=settings,
            db=db,
            trusted_position_symbols=set(),
            symbols=symbols or set(positions_by_symbol) or None,
            triggered_at=triggered_at,
        ), 0

    stop_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(sorted(positions)),
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.decision_snapshot_id,
                ExecutionPlanItem.decision_run_id,
                ExecutionPlanItem.source_plan_item_id,
                ExecutionPlanItem.source_market_date,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    items_by_symbol: dict[str, list[ExecutionPlanItem]] = {symbol: [] for symbol in positions}
    for item in stop_items:
        items_by_symbol.setdefault(item.symbol, []).append(item)

    repaired_count = 0
    issues = _protective_stop_coverage_issues(
        positions=positions,
        items_by_symbol=items_by_symbol,
        repaired_symbols=set(),
    )
    for issue in issues:
        if issue.outcome != "unprotected_position":
            continue
        latest_item = _latest_rearmable_stop_item(items_by_symbol.get(issue.symbol, []))
        if latest_item is None or latest_item.trigger_price is None:
            continue
        prior_status = latest_item.status
        prior_quantity_policy = latest_item.quantity_policy
        held_quantity = positions[issue.symbol].quantity
        rearm_quantity = issue.missing_quantity
        preserve_latest_item = latest_item.status in IN_FLIGHT_BROKER_ORDER_ITEM_STATUSES
        if issue.active_stop_quantity > 0 or preserve_latest_item:
            repair_item = ExecutionPlanItem(
                execution_plan_id=latest_item.execution_plan_id,
                plan_item_index=max(
                    (item.plan_item_index for item in items_by_symbol.get(issue.symbol, [])),
                    default=0,
                )
                + 1,
                symbol=latest_item.symbol,
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                quantity_policy={"kind": "shares", "quantity": rearm_quantity},
                limit_price=latest_item.limit_price,
                trigger_price=latest_item.trigger_price,
                time_in_force=latest_item.time_in_force,
                risk_context=dict(latest_item.risk_context or {}),
                decision_agent_name=latest_item.decision_agent_name,
                decision_snapshot_id=latest_item.decision_snapshot_id,
                decision_run_id=latest_item.decision_run_id,
                source_plan_item_id=latest_item.source_plan_item_id,
                source_market_date=latest_item.source_market_date,
            )
            db.add(repair_item)
            items_by_symbol.setdefault(issue.symbol, []).append(repair_item)
            rearmed_item = repair_item
            cloned_repair = True
        else:
            latest_item.status = "stop_sell_monitoring"
            latest_item.quantity_policy = {"kind": "shares", "quantity": rearm_quantity}
            rearmed_item = latest_item
            cloned_repair = False
        _record_monitoring_event(
            item=rearmed_item,
            event={
                "event": "unprotected_position_stop_rearmed",
                "reason": "active_stop_coverage_below_held_quantity",
                "held_quantity": held_quantity,
                "active_stop_quantity": issue.active_stop_quantity,
                "missing_quantity": issue.missing_quantity,
                "rearmed_quantity": rearm_quantity,
                "prior_item_id": latest_item.id,
                "prior_status": prior_status,
                "prior_quantity_policy": prior_quantity_policy,
                "cloned_repair_item": cloned_repair,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        repaired_count += 1

    if repaired_count:
        issues = _protective_stop_coverage_issues(
            positions=positions,
            items_by_symbol=items_by_symbol,
            repaired_symbols={
                issue.symbol
                for issue in issues
                if _latest_rearmable_stop_item(items_by_symbol.get(issue.symbol, [])) is not None
            },
        )
    issues.extend(
        _stale_position_truth_issues(
            settings=settings,
            db=db,
            trusted_position_symbols=set(positions),
            symbols=symbols or set(positions_by_symbol) or None,
            triggered_at=triggered_at,
        )
    )
    return issues, repaired_count


def _protective_stop_coverage_issues(
    *,
    positions: dict[str, ExecutionPosition],
    items_by_symbol: dict[str, list[ExecutionPlanItem]],
    repaired_symbols: set[str],
) -> list[ProtectiveStopCoverageIssue]:
    issues: list[ProtectiveStopCoverageIssue] = []
    for symbol in sorted(positions):
        held_quantity = positions[symbol].quantity
        items = items_by_symbol.get(symbol, [])
        active_quantity = sum(
            quantity
            for item in items
            if item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            for quantity in [_shares_quantity(item.quantity_policy)]
            if quantity is not None
        )
        if active_quantity >= held_quantity:
            continue
        latest_item = _latest_stop_item(items)
        missing_quantity = held_quantity - active_quantity
        issues.append(
            ProtectiveStopCoverageIssue(
                symbol=symbol,
                held_quantity=held_quantity,
                active_stop_quantity=active_quantity,
                missing_quantity=missing_quantity,
                latest_stop_item_id=latest_item.id if latest_item is not None else None,
                latest_stop_status=latest_item.status if latest_item is not None else None,
                latest_stop_trigger_price=latest_item.trigger_price if latest_item is not None else None,
                outcome=(
                    "rearmed"
                    if symbol in repaired_symbols and latest_item is not None and latest_item.trigger_price is not None
                    else "unprotected_position"
                ),
            )
        )
    return issues


def _stale_position_truth_issues(
    *,
    settings: Settings,
    db: Session,
    trusted_position_symbols: set[str],
    symbols: set[str] | None = None,
    triggered_at: datetime,
) -> list[ProtectiveStopCoverageIssue]:
    evidence_by_symbol = _position_truth_evidence_by_symbol(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    if not evidence_by_symbol:
        return []

    candidate_symbols = sorted(set(evidence_by_symbol) - trusted_position_symbols)
    if not candidate_symbols:
        return []

    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol.in_(candidate_symbols),
        )
    ).all()
    positions_by_symbol = {position.symbol: position for position in positions}
    stale_before = triggered_at - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    issues: list[ProtectiveStopCoverageIssue] = []
    for symbol in candidate_symbols:
        position = positions_by_symbol.get(symbol)
        if position is not None and position.quantity > 0:
            continue
        last_reconciled_at = position.last_reconciled_at if position is not None else None
        if last_reconciled_at is not None and _normalize_utc(last_reconciled_at) >= stale_before:
            continue

        stop_items = _stop_items_for_symbol(settings=settings, db=db, symbol=symbol)
        latest_item = _latest_stop_item(stop_items)
        active_quantity = sum(
            quantity
            for item in stop_items
            if item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            for quantity in [_shares_quantity(item.quantity_policy)]
            if quantity is not None
        )
        issues.append(
            ProtectiveStopCoverageIssue(
                symbol=symbol,
                held_quantity=None,
                active_stop_quantity=active_quantity,
                missing_quantity=None,
                latest_stop_item_id=latest_item.id if latest_item is not None else None,
                latest_stop_status=latest_item.status if latest_item is not None else None,
                latest_stop_trigger_price=latest_item.trigger_price if latest_item is not None else None,
                outcome="position_truth_stale",
                reason="stale_or_missing_execution_position_with_live_evidence",
                position_quantity=position.quantity if position is not None else None,
                position_last_reconciled_at=last_reconciled_at,
                evidence=evidence_by_symbol[symbol],
            )
        )
    return issues


def _position_truth_evidence_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
) -> dict[str, list[str]]:
    evidence_by_symbol: dict[str, set[str]] = {}
    symbol_list = sorted(symbols) if symbols else None

    active_order_where = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.status.in_(("submitted", "open", "partially_filled")),
    ]
    if symbol_list:
        active_order_where.append(BrokerOrder.symbol.in_(symbol_list))
    active_order_rows = db.execute(
        select(BrokerOrder.symbol, BrokerOrder.side, BrokerOrder.status)
        .where(*active_order_where)
    ).all()
    for symbol, side, status in active_order_rows:
        evidence_by_symbol.setdefault(str(symbol), set()).add(f"broker_order:{side}:{status}")

    filled_buy_where = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.side == "buy",
        BrokerOrder.status == "filled",
    ]
    if symbol_list:
        filled_buy_where.append(BrokerOrder.symbol.in_(symbol_list))
    filled_buy_symbols = set(
        db.scalars(
            select(BrokerOrder.symbol)
            .where(*filled_buy_where)
            .distinct()
        ).all()
    )
    for symbol in filled_buy_symbols:
        if _has_unexited_buy_order_for_symbol(settings=settings, db=db, symbol=str(symbol)):
            evidence_by_symbol.setdefault(str(symbol), set()).add("broker_order:buy:filled_unexited")

    sell_where = [
        ExecutionPlan.mode == settings.runtime_mode,
        ExecutionPlanItem.side == "sell",
        ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
        ExecutionPlanItem.status.in_(tuple(SELL_STALE_FLAT_EXPIRABLE_STATUSES | {"expired"})),
    ]
    if symbol_list:
        sell_where.append(ExecutionPlanItem.symbol.in_(symbol_list))
    sell_rows = db.execute(
        select(ExecutionPlanItem.symbol, ExecutionPlanItem.status)
        .join(ExecutionPlanItem.plan)
        .where(*sell_where)
    ).all()
    for symbol, status in sell_rows:
        evidence_by_symbol.setdefault(str(symbol), set()).add(f"sell_monitor:{status}")

    return {
        symbol: sorted(evidence)
        for symbol, evidence in evidence_by_symbol.items()
        if evidence
    }


def _has_unexited_buy_order_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> bool:
    latest_buy_order_at = db.scalar(
        select(func.max(BrokerOrder.created_at)).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
    )
    if latest_buy_order_at is None:
        return False
    latest_sell_order_at = db.scalar(
        select(func.max(BrokerOrder.created_at)).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "sell",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
    )
    return latest_sell_order_at is None or latest_buy_order_at > latest_sell_order_at


def _stop_items_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> list[ExecutionPlanItem]:
    return db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol == symbol,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_market_date,
            )
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()


def _latest_stop_item(items: list[ExecutionPlanItem]) -> ExecutionPlanItem | None:
    return items[0] if items else None


def _latest_rearmable_stop_item(items: list[ExecutionPlanItem]) -> ExecutionPlanItem | None:
    for item in items:
        if item.trigger_price is not None and (
            item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            or item.status not in NON_MONITORABLE_PLAN_ITEM_STATUSES
        ):
            return item
    return None


def _shares_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict) or quantity_policy.get("kind") != "shares":
        return None
    return _parse_positive_int(quantity_policy.get("quantity"))


def _protective_stop_coverage_payload(
    *,
    issues: list[ProtectiveStopCoverageIssue],
    repaired_count: int,
) -> dict[str, object]:
    unprotected = [issue for issue in issues if issue.outcome == "unprotected_position"]
    degraded = [issue for issue in issues if issue.outcome != "rearmed"]
    affected_symbols = [issue.symbol for issue in degraded]
    serialized_issues = [
        {
            "symbol": issue.symbol,
            "held_quantity": issue.held_quantity,
            "active_stop_quantity": issue.active_stop_quantity,
            "missing_quantity": issue.missing_quantity,
            "latest_stop_item_id": issue.latest_stop_item_id,
            "latest_stop_status": issue.latest_stop_status,
            "latest_stop_trigger_price": _serialize_decimal(issue.latest_stop_trigger_price),
            "outcome": issue.outcome,
            "reason": issue.reason,
            "position_quantity": issue.position_quantity,
            "position_last_reconciled_at": _serialize_datetime(issue.position_last_reconciled_at),
            "evidence": issue.evidence or [],
        }
        for issue in issues
    ]
    return {
        "status": "degraded" if degraded else "ok",
        "unprotected_position_count": len(unprotected),
        "position_truth_stale_count": sum(1 for issue in issues if issue.outcome == "position_truth_stale"),
        "repaired_stop_coverage_count": repaired_count,
        "affected_symbols": affected_symbols[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "affected_symbols_count": len(affected_symbols),
        "affected_symbols_truncated": len(affected_symbols) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
        "missing_quantities": {
            issue.symbol: issue.missing_quantity
            for issue in degraded
            if issue.missing_quantity is not None
        },
        "issues": serialized_issues[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "issues_count": len(serialized_issues),
        "issues_truncated": len(serialized_issues) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
    }


def _watch_buy_retry_blocked_until(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> datetime | None:
    if item.status != "watch_buy_triggered_blocked":
        return None
    event = _latest_monitoring_event(item)
    if event is None:
        return None
    reason = str(event.get("reason") or "")
    if reason not in RETRYABLE_WATCH_BUY_APPROVAL_REASONS:
        return None
    recorded_at = _parse_event_datetime(event.get("recorded_at"))
    if recorded_at is None:
        return None
    return recorded_at + timedelta(
        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
    )


def _trigger_time_agent_delay_until(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> datetime | None:
    if item.status != AGENT_DELAY_STATUS:
        return None
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return None
    decision = interaction.get("decision")
    valid_until = None
    if isinstance(decision, dict):
        valid_until = _parse_event_datetime(decision.get("valid_until"))
    if valid_until is not None:
        return valid_until
    recorded_at = _parse_event_datetime(interaction.get("recorded_at"))
    if recorded_at is None:
        return None
    return recorded_at + timedelta(
        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
    )


def _latest_monitoring_event(item: ExecutionPlanItem) -> dict[str, object] | None:
    risk_context = item.risk_context
    if not isinstance(risk_context, dict):
        return None
    event = risk_context.get("last_monitoring_event")
    return event if isinstance(event, dict) else None


def _is_same_cycle_cloned_stop_repair(
    *,
    item: ExecutionPlanItem,
    triggered_at: datetime,
) -> bool:
    event = _latest_monitoring_event(item)
    if event is None:
        return False
    if event.get("event") != "unprotected_position_stop_rearmed":
        return False
    if event.get("cloned_repair_item") is not True:
        return False
    recorded_at = _parse_event_datetime(event.get("recorded_at"))
    return recorded_at == triggered_at


def _parse_event_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _latest_order_failure_is_retryable_session_rejection(
    *,
    db: Session,
    item: ExecutionPlanItem,
) -> bool:
    order = db.scalar(
        select(BrokerOrder)
        .where(BrokerOrder.execution_plan_item_id == item.id)
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.status,
                BrokerOrder.last_error_code,
                BrokerOrder.last_error_classification,
                BrokerOrder.last_error_message,
                BrokerOrder.updated_at,
                BrokerOrder.created_at,
            )
        )
        .order_by(
            BrokerOrder.updated_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
    )
    if order is None:
        return item.status in {"broker_order_retry_pending", "retry_pending"}
    if order.status in {"submitted", "open", "partially_filled", "submission_pending"}:
        return False
    error_code = (order.last_error_code or "").strip().upper()
    if error_code in SESSION_RETRYABLE_ERROR_CODES:
        return True
    classification = (order.last_error_classification or "").strip().lower()
    if classification in {"retryable", "session", "market_session", "market_closed"}:
        return True
    message = (order.last_error_message or "").strip().lower()
    if any(marker.lower() in message for marker in SESSION_RETRYABLE_MESSAGE_MARKERS):
        return True
    for payload in (order.response_metadata, order.raw_broker_payload):
        if not isinstance(payload, dict):
            continue
        raw_code = str(payload.get("msg_cd") or payload.get("code") or "").strip().upper()
        if raw_code in SESSION_RETRYABLE_ERROR_CODES:
            return True
        raw_message = str(payload.get("msg1") or payload.get("message") or "").strip().lower()
        if any(marker.lower() in raw_message for marker in SESSION_RETRYABLE_MESSAGE_MARKERS):
            return True
    return bool(order.last_error_retryable and item.status in {"broker_order_retry_pending", "retry_pending"})


def _monitored_item_filter():
    return and_(
        ~ExecutionPlanItem.status.in_(tuple(NON_MONITORABLE_PLAN_ITEM_STATUSES)),
        or_(
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            and_(
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(WATCH_MONITORING_STATUSES)),
            ),
        ),
    )


def _position_exit_quantity_policy_filter():
    return or_(
        ExecutionPlanItem.quantity_policy["type"].as_string() == "position_exit_from_entry",
        ExecutionPlanItem.quantity_policy["kind"].as_string() == "position_exit_from_entry",
    )


def _has_unexited_buy_order_filter(*, settings: Settings):
    latest_buy_order_at = (
        select(func.max(BrokerOrder.created_at))
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == ExecutionPlanItem.symbol,
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
        .correlate(ExecutionPlanItem)
        .scalar_subquery()
    )
    latest_sell_order_at = (
        select(func.max(BrokerOrder.created_at))
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == ExecutionPlanItem.symbol,
            BrokerOrder.side == "sell",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
        .correlate(ExecutionPlanItem)
        .scalar_subquery()
    )
    return and_(
        latest_buy_order_at.is_not(None),
        or_(latest_sell_order_at.is_(None), latest_buy_order_at > latest_sell_order_at),
    )


def _unexited_buy_sell_monitor_fallback_filter(*, settings: Settings):
    fallback = and_(
        _position_exit_quantity_policy_filter(),
        _has_unexited_buy_order_filter(settings=settings),
    )
    if settings.runtime_mode != "live":
        return fallback
    return and_(
        fallback,
        ~_has_recent_flat_position_truth_filter(settings=settings),
    )


def _has_recent_flat_position_truth_filter(*, settings: Settings):
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return (
        select(ExecutionPosition.id)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol == ExecutionPlanItem.symbol,
            ExecutionPosition.quantity <= 0,
            ExecutionPosition.last_reconciled_at.is_not(None),
            ExecutionPosition.last_reconciled_at >= stale_before,
        )
        .correlate(ExecutionPlanItem)
        .exists()
    )


def _has_recent_flat_position_truth_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> bool:
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return db.scalar(
        select(ExecutionPosition.id)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol == symbol,
            ExecutionPosition.quantity <= 0,
            ExecutionPosition.last_reconciled_at.is_not(None),
            ExecutionPosition.last_reconciled_at >= stale_before,
        )
        .limit(1)
    ) is not None


def _load_positions_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str] | set[str] | None = None,
) -> dict[str, ExecutionPosition]:
    where = [ExecutionPosition.mode == settings.runtime_mode]
    if symbols:
        where.append(ExecutionPosition.symbol.in_(_ordered_symbol_scope(symbols)))
    positions = db.scalars(
        select(ExecutionPosition)
        .where(*where)
        .order_by(ExecutionPosition.symbol.asc())
    ).all()
    return {
        position.symbol: position
        for position in positions
        if position.quantity > 0
    }


def _maintain_post_entry_exit_monitors(
    *,
    settings: Settings,
    db: Session,
    positions_by_symbol: dict[str, ExecutionPosition],
    prices: dict[str, MonitoringPriceRequest],
    triggered_at: datetime,
) -> tuple[int, int]:
    maintenance_count = 0
    review_count = 0
    for symbol, position in positions_by_symbol.items():
        if position.quantity <= 0:
            continue
        entry_order = _latest_filled_buy_order(
            settings=settings,
            db=db,
            symbol=symbol,
        )
        if entry_order is None or entry_order.plan_item is None:
            continue
        entry_item = entry_order.plan_item
        if entry_item.plan is None:
            continue
        entry_price = position.average_price or entry_order.limit_price
        if entry_price is None or entry_price <= 0:
            continue

        observed_price = prices.get(symbol).price if prices.get(symbol) is not None else None
        entry_date = _entry_market_date(entry_item=entry_item, entry_order=entry_order)
        holding_session = _holding_session_age_from_date(
            entry_market_date=entry_date,
            triggered_at=triggered_at,
        )
        initial_stop = _post_entry_initial_stop_price(
            settings=settings,
            entry_item=entry_item,
            entry_price=entry_price,
        )
        stop_item, stop_maintenance_count = _ensure_post_entry_stop_monitor(
            db=db,
            entry_item=entry_item,
            quantity=position.quantity,
            stop_price=initial_stop,
            entry_price=entry_price,
            triggered_at=triggered_at,
        )
        maintenance_count += stop_maintenance_count
        _, max_holding_maintenance_count = _ensure_post_entry_max_holding_monitor(
            db=db,
            entry_item=entry_item,
            quantity=position.quantity,
            entry_market_date=entry_date,
            triggered_at=triggered_at,
            max_holding_sessions=settings.exec_agent_max_holding_trading_sessions,
        )
        maintenance_count += max_holding_maintenance_count

        review = _post_entry_review_state(
            settings=settings,
            entry_price=entry_price,
            observed_price=observed_price,
            holding_session=holding_session,
            stop_price=stop_item.trigger_price,
            triggered_at=triggered_at,
        )
        maintenance_count += _apply_post_entry_review_state(
            item=stop_item,
            review=review,
            triggered_at=triggered_at,
        )
        if review["requires_review"] is True:
            review_count += 1

    return maintenance_count, review_count


def _latest_filled_buy_order(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .join(BrokerOrder.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "buy",
            BrokerOrder.status == "filled",
            ExecutionPlan.mode == settings.runtime_mode,
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.symbol,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.limit_price,
                BrokerOrder.submitted_at,
                BrokerOrder.updated_at,
                BrokerOrder.created_at,
            ),
            selectinload(BrokerOrder.plan_item).load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_market_date,
            ).selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            BrokerOrder.submitted_at.desc(),
            BrokerOrder.updated_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
    )


def _entry_market_date(
    *,
    entry_item: ExecutionPlanItem,
    entry_order: BrokerOrder,
) -> date:
    if entry_item.source_market_date is not None:
        return entry_item.source_market_date
    if entry_item.plan is not None and entry_item.plan.market_date is not None:
        return entry_item.plan.market_date
    if entry_order.submitted_at is not None:
        return _normalize_utc(entry_order.submitted_at).astimezone(KRX_TIMEZONE).date()
    return datetime.now(KRX_TIMEZONE).date()


def _holding_session_age_from_date(
    *,
    entry_market_date: date,
    triggered_at: datetime,
) -> int:
    market_date = triggered_at.astimezone(KRX_TIMEZONE).date()
    return max(1, (market_date - entry_market_date).days + 1)


def _post_entry_initial_stop_price(
    *,
    settings: Settings,
    entry_item: ExecutionPlanItem,
    entry_price: Decimal,
) -> Decimal:
    upstream_stop = _watch_buy_stop_price(entry_item)
    hard_stop = (entry_price * (Decimal("1") - settings.exec_agent_stop_loss_pct)).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if upstream_stop is None:
        return hard_stop
    if upstream_stop >= entry_price:
        return hard_stop
    return effective_stop_price(
        settings=settings,
        entry_price=entry_price,
        upstream_stop_price=upstream_stop,
    )


def _ensure_post_entry_stop_monitor(
    *,
    db: Session,
    entry_item: ExecutionPlanItem,
    quantity: int,
    stop_price: Decimal,
    entry_price: Decimal,
    triggered_at: datetime,
) -> tuple[ExecutionPlanItem, int]:
    plan = entry_item.plan
    assert plan is not None
    maintenance_count = 0
    existing = _active_sibling_monitor(
        entry_item=entry_item,
        intent="stop_sell",
    )
    if existing is None:
        existing = ExecutionPlanItem(
            plan=plan,
            plan_item_index=_next_plan_item_index(plan),
            symbol=entry_item.symbol,
            side="sell",
            intent="stop_sell",
            status="stop_sell_monitoring",
            quantity_policy={"kind": "shares", "quantity": quantity},
            trigger_price=stop_price,
            time_in_force=entry_item.time_in_force or "day",
            risk_context={
                "exchange": _plan_item_exchange(entry_item),
                "entry_context": _post_entry_context(
                    entry_item=entry_item,
                    entry_price=entry_price,
                    quantity=quantity,
                ),
                "deterministic_emergency_stop": True,
                "post_entry_managed": True,
            },
            decision_agent_name=entry_item.decision_agent_name,
            decision_snapshot_id=entry_item.decision_snapshot_id,
            decision_run_id=entry_item.decision_run_id,
            source_plan_item_id=_derived_source_plan_item_id(entry_item, "post-entry-stop"),
            source_market_date=entry_item.source_market_date,
        )
        db.add(existing)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_created",
                "entry_execution_plan_item_id": entry_item.id,
                "trigger_price": _serialize_decimal(stop_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return existing, 1

    if (
        existing.trigger_price is not None
        and existing.trigger_price >= entry_price
        and not _is_breakeven_rearmed_stop(existing)
    ):
        prior_stop = existing.trigger_price
        existing.trigger_price = stop_price
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_rebased",
                "reason": "upstream_stop_at_or_above_entry",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(stop_price),
                "entry_price": _serialize_decimal(entry_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    elif existing.trigger_price is None or stop_price > existing.trigger_price:
        prior_stop = existing.trigger_price
        existing.trigger_price = stop_price
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_tightened",
                "reason": "effective_entry_risk_stop",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(stop_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    risk_context.setdefault("exchange", _plan_item_exchange(entry_item))
    risk_context.setdefault("deterministic_emergency_stop", True)
    risk_context.setdefault("post_entry_managed", True)
    risk_context["entry_context"] = _post_entry_context(
        entry_item=entry_item,
        entry_price=entry_price,
        quantity=quantity,
    )
    existing.risk_context = risk_context
    return existing, maintenance_count


def _ensure_post_entry_max_holding_monitor(
    *,
    db: Session,
    entry_item: ExecutionPlanItem,
    quantity: int,
    entry_market_date: date,
    triggered_at: datetime,
    max_holding_sessions: int,
) -> tuple[ExecutionPlanItem, int]:
    plan = entry_item.plan
    assert plan is not None
    maintenance_count = 0
    existing = _active_sibling_monitor(
        entry_item=entry_item,
        intent="max_holding_close_sell",
    )
    close_exit_date = _add_trading_sessions(entry_market_date, max_holding_sessions)
    if existing is None:
        existing = ExecutionPlanItem(
            plan=plan,
            plan_item_index=_next_plan_item_index(plan),
            symbol=entry_item.symbol,
            side="sell",
            intent="max_holding_close_sell",
            status="max_holding_close_sell_monitoring",
            quantity_policy={"kind": "position_exit_from_entry"},
            time_in_force=entry_item.time_in_force or "day",
            risk_context={
                "exchange": _plan_item_exchange(entry_item),
                "close_exit_date": close_exit_date.isoformat(),
                "max_holding_sessions": max_holding_sessions,
                "entry_context": _post_entry_context(
                    entry_item=entry_item,
                    entry_price=None,
                    quantity=quantity,
                ),
                "post_entry_managed": True,
            },
            decision_agent_name=entry_item.decision_agent_name,
            decision_snapshot_id=entry_item.decision_snapshot_id,
            decision_run_id=entry_item.decision_run_id,
            source_plan_item_id=_derived_source_plan_item_id(entry_item, "post-entry-max-holding"),
            source_market_date=entry_item.source_market_date,
        )
        db.add(existing)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_max_holding_monitor_created",
                "entry_execution_plan_item_id": entry_item.id,
                "close_exit_date": close_exit_date.isoformat(),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return existing, 1

    risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    if risk_context.get("close_exit_date") != close_exit_date.isoformat():
        risk_context["close_exit_date"] = close_exit_date.isoformat()
        risk_context["max_holding_sessions"] = max_holding_sessions
        existing.risk_context = risk_context
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_max_holding_monitor_updated",
                "close_exit_date": close_exit_date.isoformat(),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    risk_context.setdefault("exchange", _plan_item_exchange(entry_item))
    risk_context.setdefault("post_entry_managed", True)
    risk_context["entry_context"] = _post_entry_context(
        entry_item=entry_item,
        entry_price=None,
        quantity=quantity,
    )
    existing.risk_context = risk_context
    return existing, maintenance_count


def _add_trading_sessions(start: date, sessions: int) -> date:
    current = start
    added = 0
    while added < max(1, sessions):
        current = current + timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _active_sibling_monitor(
    *,
    entry_item: ExecutionPlanItem,
    intent: str,
) -> ExecutionPlanItem | None:
    plan = entry_item.plan
    if plan is None:
        return None
    for sibling in plan.items:
        if sibling.symbol != entry_item.symbol or sibling.side != "sell" or sibling.intent != intent:
            continue
        if sibling.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue
        return sibling
    return None


def _is_breakeven_rearmed_stop(item: ExecutionPlanItem) -> bool:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    breakeven_rearmed = risk_context.get("breakeven_rearmed")
    if (
        isinstance(breakeven_rearmed, dict)
        and breakeven_rearmed.get("source") == "partial_take_profit"
    ):
        return True
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    return quantity_policy.get("source") == "partial_take_profit_breakeven_rearm"


def _next_plan_item_index(plan: ExecutionPlan) -> int:
    if not plan.items:
        return 0
    return max(item.plan_item_index for item in plan.items) + 1


def _post_entry_context(
    *,
    entry_item: ExecutionPlanItem,
    entry_price: Decimal | None,
    quantity: int,
) -> dict[str, object]:
    context: dict[str, object] = {
        "source_plan_item_id": entry_item.source_plan_item_id,
        "execution_plan_item_id": entry_item.id,
        "decision_run_id": entry_item.decision_run_id,
        "decision_snapshot_id": entry_item.decision_snapshot_id,
        "entry_quantity": quantity,
    }
    if entry_price is not None:
        context["entry_price"] = _serialize_decimal(entry_price)
    return context


def _derived_source_plan_item_id(entry_item: ExecutionPlanItem, suffix: str) -> str | None:
    source = entry_item.source_plan_item_id
    if not source:
        return None
    return f"{source}:{suffix}"


def _post_entry_review_state(
    *,
    settings: Settings,
    entry_price: Decimal,
    observed_price: Decimal | None,
    holding_session: int,
    stop_price: Decimal | None,
    triggered_at: datetime,
) -> dict[str, object]:
    current_pnl_pct = (
        _profit_pct(current_price=observed_price, average_price=entry_price)
        if observed_price is not None
        else None
    )
    adverse_pct = (
        ((entry_price - observed_price) / entry_price) * Decimal("100")
        if observed_price is not None and observed_price < entry_price
        else Decimal("0")
    )
    reasons: list[str] = []
    tightened_stop = stop_price
    if observed_price is not None:
        risk_threshold_pct = settings.exec_agent_stop_loss_pct * Decimal("100")
        if adverse_pct >= risk_threshold_pct:
            reasons.append("gap_down_breached_risk_threshold")
        if holding_session >= 2 and observed_price <= entry_price:
            reasons.append("next_day_no_progress")
            tightened_stop = max(
                stop_price or Decimal("0"),
                (entry_price * Decimal("0.99")).quantize(Decimal("0.0001"), rounding=ROUND_DOWN),
            )
        if holding_session >= settings.exec_agent_max_holding_trading_sessions and observed_price <= entry_price:
            reasons.append("max_holding_without_progress")
    return {
        "requires_review": bool(reasons),
        "reasons": reasons,
        "entry_price": _serialize_decimal(entry_price),
        "observed_price": _serialize_decimal(observed_price),
        "current_pnl_pct": _serialize_decimal(current_pnl_pct),
        "max_adverse_excursion_pct": _serialize_decimal(adverse_pct),
        "holding_session": holding_session,
        "tightened_stop_price": _serialize_decimal(tightened_stop),
        "recorded_at": triggered_at.isoformat(),
    }


def _apply_post_entry_review_state(
    *,
    item: ExecutionPlanItem,
    review: dict[str, object],
    triggered_at: datetime,
) -> int:
    maintenance_count = 0
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    previous = risk_context.get("post_entry_review")
    risk_context["post_entry_review"] = review
    tightened_stop = _parse_decimal(review.get("tightened_stop_price"))
    if tightened_stop is not None and (item.trigger_price is None or tightened_stop > item.trigger_price):
        prior_stop = item.trigger_price
        item.trigger_price = tightened_stop
        maintenance_count += 1
        item.risk_context = risk_context
        _record_monitoring_event(
            item=item,
            event={
                "event": "post_entry_stop_monitor_tightened",
                "reason": "post_entry_no_progress_review",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(tightened_stop),
                "review_reasons": review.get("reasons") or [],
                "recorded_at": triggered_at.isoformat(),
            },
        )
        risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    item.risk_context = risk_context
    if review["requires_review"] is True and not (
        isinstance(previous, dict) and previous.get("reasons") == review.get("reasons")
    ):
        maintenance_count += 1
        _record_monitoring_event(
            item=item,
            event={
                "event": "post_entry_review_required",
                "reasons": review.get("reasons") or [],
                "holding_session": review.get("holding_session"),
                "max_adverse_excursion_pct": review.get("max_adverse_excursion_pct"),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    return maintenance_count


def _latest_reconciliation_error_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> str | None:
    order = db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == symbol,
            BrokerOrder.last_error_message.is_not(None),
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.symbol,
                BrokerOrder.last_error_message,
                BrokerOrder.last_broker_sync_at,
                BrokerOrder.updated_at,
            )
        )
        .order_by(BrokerOrder.last_broker_sync_at.desc(), BrokerOrder.updated_at.desc())
        .limit(1)
    )
    if order is None:
        return None
    return order.last_error_message


def _reconcile_before_monitoring(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str],
    refresh_positions: bool = True,
) -> None:
    if not symbols:
        return
    if settings.runtime_mode != "live":
        return
    try:
        reconcile_pending_broker_orders(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
    except Exception:
        return

    if not refresh_positions:
        return

    stale_symbols = _positions_with_stale_local_state(
        settings=settings,
        db=db,
        symbols=symbols,
        max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
    )
    if not stale_symbols:
        return
    try:
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=stale_symbols,
        )
    except Exception:
        return


def _positions_with_stale_local_state(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
    max_age_seconds: int,
) -> set[str]:
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol.in_(sorted(symbols)),
        )
    ).all()
    known = {position.symbol: position for position in positions}
    stale: set[str] = set()
    stale_before = datetime.now(UTC) - timedelta(seconds=max_age_seconds)
    for symbol in symbols:
        position = known.get(symbol)
        if position is None or position.quantity <= 0:
            stale.add(symbol)
            continue
        if _is_legacy_seeded_reconciliation_position(position):
            continue
        reconciled_at = position.last_reconciled_at
        if reconciled_at is None:
            stale.add(symbol)
            continue
        if _normalize_utc(reconciled_at) < stale_before:
            stale.add(symbol)
    return stale


def _refresh_position_truth_if_stale(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: list[str] | set[str] | None = None,
) -> None:
    if symbols is not None and not symbols:
        return
    symbol_list = _ordered_symbol_scope(symbols)
    if settings.runtime_mode != "live":
        if not symbol_list:
            return
        existing_symbols = set(
            db.scalars(
                select(ExecutionPosition.symbol).where(
                    ExecutionPosition.mode == settings.runtime_mode,
                    ExecutionPosition.symbol.in_(symbol_list),
                )
            ).all()
        )
        missing_symbols = set(symbol_list) - existing_symbols
        if not missing_symbols:
            return
        try:
            sync_execution_positions_from_broker_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=missing_symbols,
            )
        except Exception:
            return
        return
    where = [
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.quantity > 0,
    ]
    if symbol_list:
        where.append(ExecutionPosition.symbol.in_(symbol_list))
    latest_reconciled_at = db.scalar(
        select(func.max(ExecutionPosition.last_reconciled_at)).where(*where)
    )
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    if latest_reconciled_at is not None and _normalize_utc(latest_reconciled_at) >= stale_before:
        return
    try:
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=set(symbol_list) if symbol_list else None,
        )
    except Exception:
        return


def _is_legacy_seeded_reconciliation_position(position: ExecutionPosition) -> bool:
    if position.updated_from != "reconciliation":
        return False
    raw_payload = position.raw_payload
    return isinstance(raw_payload, dict) and raw_payload == {"source": "broker"}


def _monitoring_item_is_configured(item: ExecutionPlanItem) -> bool:
    if _is_watch_buy_item(item):
        return (
            item.side == "buy"
            and _watch_buy_trigger_price(item) is not None
            and (
                _parse_buy_quantity(item.quantity_policy) is not None
                or _watch_buy_candidate_budget(item) is not None
            )
        )
    if item.side != "sell" or not _quantity_policy_supported(item.quantity_policy):
        return False
    if item.intent == "max_holding_close_sell":
        risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
        close_exit_date = risk_context.get("close_exit_date")
        return isinstance(close_exit_date, str) and bool(close_exit_date.strip())
    return item.trigger_price is not None


def _quantity_policy_supported(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    if _quantity_policy_requires_position_exit(quantity_policy):
        return True

    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = quantity_policy.get("quantity")
        return _parse_positive_int(quantity) is not None
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        return fraction is not None
    return False


def _quantity_policy_requires_position_exit(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    policy_type = quantity_policy.get("type")
    if (
        isinstance(policy_type, str)
        and policy_type.strip().lower() == "position_exit_from_entry"
    ):
        return True
    kind = quantity_policy.get("kind")
    return isinstance(kind, str) and kind.strip().lower() == "position_exit_from_entry"


def _is_triggered(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> bool:
    if item.intent == "max_holding_close_sell":
        return _max_holding_exit_due(item=item, triggered_at=triggered_at)
    trigger_price = (
        _watch_buy_trigger_price(item)
        if _is_watch_buy_item(item)
        else item.trigger_price
    )
    if trigger_price is None or observed_price is None:
        return False
    if item.intent == "target_sell":
        return observed_price >= trigger_price
    if item.intent == "stop_sell":
        return observed_price <= trigger_price
    if _is_watch_buy_item(item):
        if _watch_buy_direction(item) == "at_or_below":
            return observed_price <= trigger_price
        return observed_price >= trigger_price
    return False


def _watch_buy_trigger_reason(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> tuple[str | None, dict[str, object] | None]:
    metadata = _watch_buy_intraday_cross_metadata(
        item=item,
        price=price,
        observed_price=observed_price,
        triggered_at=triggered_at,
    )
    if metadata is not None:
        _record_watch_buy_trigger_cross(item=item, metadata=metadata)
        return str(metadata["reason"]), metadata

    if _is_triggered(
        item=item,
        observed_price=observed_price,
        triggered_at=triggered_at,
    ):
        metadata = _watch_buy_trigger_observation_metadata(
            item=item,
            price=price,
            observed_price=observed_price,
            triggered_at=triggered_at,
            reason=item.intent,
        )
        if metadata is not None:
            _record_watch_buy_trigger_cross(item=item, metadata=metadata)
        return item.intent, metadata

    persisted = _persisted_watch_buy_trigger_cross(item)
    if persisted is not None:
        return str(persisted.get("reason") or "intraday_watch_buy_trigger_cross_seen"), persisted
    return None, None


def _watch_buy_intraday_cross_metadata(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> dict[str, object] | None:
    if price is None:
        return None
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    if not _watch_buy_observed_price_crosses_trigger(item=item, observed_price=observed_price):
        return None
    direction = _watch_buy_direction(item)
    if direction == "at_or_below":
        if price.day_low is None or price.day_low > trigger_price:
            return None
        return _watch_buy_trigger_observation_metadata(
            item=item,
            price=price,
            observed_price=observed_price,
            triggered_at=triggered_at,
            reason="intraday_watch_buy_day_low_cross",
            cross_price=price.day_low,
        )
    if price.day_high is None or price.day_high < trigger_price:
        return None
    return _watch_buy_trigger_observation_metadata(
        item=item,
        price=price,
        observed_price=observed_price,
        triggered_at=triggered_at,
        reason="intraday_watch_buy_day_high_cross",
        cross_price=price.day_high,
    )


def _watch_buy_trigger_observation_metadata(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
    reason: str,
    cross_price: Decimal | None = None,
) -> dict[str, object] | None:
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    observed_at = _price_observed_at(price, triggered_at) if price is not None else triggered_at
    metadata: dict[str, object] = {
        "reason": reason,
        "direction": _watch_buy_direction(item),
        "trigger_price": _serialize_decimal(trigger_price),
        "chase_limit_price": _serialize_decimal(_watch_buy_chase_limit_price(item)),
        "observed_price": _serialize_decimal(observed_price),
        "observed_at": observed_at.isoformat(),
        "recorded_at": triggered_at.isoformat(),
    }
    if cross_price is not None:
        metadata["cross_price"] = _serialize_decimal(cross_price)
    if price is not None:
        metadata["day_high"] = _serialize_decimal(price.day_high)
        metadata["day_low"] = _serialize_decimal(price.day_low)
        metadata["volume"] = _serialize_decimal(price.volume)
        metadata["turnover"] = _serialize_decimal(price.turnover)
    return metadata


def _watch_buy_observed_price_crosses_trigger(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
) -> bool:
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None or observed_price is None:
        return False
    if _watch_buy_direction(item) == "at_or_below":
        return observed_price <= trigger_price
    return observed_price >= trigger_price


def _record_watch_buy_trigger_cross(
    *,
    item: ExecutionPlanItem,
    metadata: dict[str, object],
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    previous = risk_context.get("watch_buy_trigger_cross")
    risk_context["watch_buy_trigger_cross"] = metadata
    risk_context["latest_live_market_context"] = {
        key: metadata[key]
        for key in ("day_high", "day_low", "observed_price", "observed_at", "volume", "turnover")
        if key in metadata
    }
    item.risk_context = risk_context
    if not isinstance(previous, dict):
        _record_monitoring_event(
            item=item,
            event={
                "event": "watch_buy_trigger_cross_seen",
                **metadata,
            },
        )


def _persisted_watch_buy_trigger_cross(item: ExecutionPlanItem) -> dict[str, object] | None:
    risk_context = item.risk_context
    if not isinstance(risk_context, dict):
        return None
    trigger_cross = risk_context.get("watch_buy_trigger_cross")
    if not isinstance(trigger_cross, dict):
        return None
    persisted_direction = trigger_cross.get("direction")
    current_direction = _watch_buy_direction(item)
    if isinstance(persisted_direction, str) and persisted_direction != current_direction:
        return None
    return trigger_cross


def _primary_triggered_sell_symbols(
    *,
    items: list[ExecutionPlanItem],
    prices: dict[str, MonitoringPriceRequest],
    triggered_at: datetime,
) -> set[str]:
    triggered_symbols: set[str] = set()
    for item in items:
        if _is_watch_buy_item(item) or item.intent not in SELL_MONITORED_INTENTS:
            continue
        if not _monitoring_item_is_configured(item):
            continue
        price = prices.get(item.symbol)
        observed_price = price.price if price is not None else None
        if _is_triggered(
            item=item,
            observed_price=observed_price,
            triggered_at=triggered_at,
        ):
            triggered_symbols.add(item.symbol)
    return triggered_symbols


def _evaluate_profit_protection(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> ProfitProtectionDecision | None:
    if item.intent != "target_sell" or observed_price is None:
        return None
    if position is None or position.average_price is None or position.average_price <= 0:
        return None
    current_profit_pct = _profit_pct(
        current_price=observed_price,
        average_price=position.average_price,
    )
    state = _update_profit_protection_state(
        settings=settings,
        item=item,
        observed_price=observed_price,
        current_profit_pct=current_profit_pct,
        triggered_at=triggered_at,
    )
    peak_profit_pct = _parse_decimal(state.get("peak_profit_pct")) or current_profit_pct
    giveback_pct = peak_profit_pct - current_profit_pct
    holding_session = _holding_session_age(item=item, triggered_at=triggered_at)
    metadata = {
        **state,
        "current_profit_pct": _serialize_decimal(current_profit_pct),
        "giveback_pct": _serialize_decimal(giveback_pct),
        "holding_session": holding_session,
        "min_peak_pct": _serialize_decimal(settings.profit_protection_min_peak_pct),
        "giveback_threshold_pct": _serialize_decimal(settings.profit_protection_giveback_pct),
        "aged_session_min": settings.profit_protection_aged_session_min,
        "aged_min_current_profit_pct": _serialize_decimal(
            settings.profit_protection_aged_min_current_profit_pct
        ),
    }
    if (
        peak_profit_pct >= settings.profit_protection_min_peak_pct
        and giveback_pct >= settings.profit_protection_giveback_pct
    ):
        _record_monitoring_event(
            item=item,
            event={
                "event": "profit_protection_triggered",
                "reason": "profit_protection_trailing_stop",
                **metadata,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return ProfitProtectionDecision(
            reason="profit_protection_trailing_stop",
            metadata=metadata,
        )
    if (
        holding_session >= settings.profit_protection_aged_session_min
        and current_profit_pct >= settings.profit_protection_aged_min_current_profit_pct
        and peak_profit_pct > 0
        and giveback_pct >= settings.profit_protection_giveback_pct
    ):
        _record_monitoring_event(
            item=item,
            event={
                "event": "profit_protection_triggered",
                "reason": "aged_position_profit_protection",
                **metadata,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return ProfitProtectionDecision(
            reason="aged_position_profit_protection",
            metadata=metadata,
        )
    return None


def _persisted_profit_protection_trigger(
    item: ExecutionPlanItem,
) -> ProfitProtectionDecision | None:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    last_event = risk_context.get("last_monitoring_event")
    if not isinstance(last_event, dict):
        return None
    reason = last_event.get("reason")
    if reason not in PROFIT_PROTECTION_TRIGGER_REASONS:
        return None
    metadata = {
        key: value
        for key, value in last_event.items()
        if key not in {"event", "reason", "recorded_at"}
    }
    return ProfitProtectionDecision(reason=str(reason), metadata=metadata)


def _update_profit_protection_state(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal,
    current_profit_pct: Decimal,
    triggered_at: datetime,
) -> dict[str, object]:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    raw_state = risk_context.get("profit_protection")
    state = dict(raw_state) if isinstance(raw_state, dict) else {}
    peak_price = _parse_decimal(state.get("peak_price"))
    peak_profit_pct = _parse_decimal(state.get("peak_profit_pct"))
    if peak_price is None or observed_price > peak_price:
        peak_price = observed_price
    if peak_profit_pct is None or current_profit_pct > peak_profit_pct:
        peak_profit_pct = current_profit_pct
    updated_state: dict[str, object] = {
        "peak_price": _serialize_decimal(peak_price),
        "peak_profit_pct": _serialize_decimal(peak_profit_pct),
        "current_profit_pct": _serialize_decimal(current_profit_pct),
        "last_observed_price": _serialize_decimal(observed_price),
        "last_observed_at": triggered_at.isoformat(),
        "min_peak_pct": _serialize_decimal(settings.profit_protection_min_peak_pct),
        "giveback_threshold_pct": _serialize_decimal(settings.profit_protection_giveback_pct),
    }
    risk_context["profit_protection"] = updated_state
    item.risk_context = risk_context
    return updated_state


def _profit_pct(*, current_price: Decimal, average_price: Decimal) -> Decimal:
    return ((current_price - average_price) / average_price) * Decimal("100")


def _holding_session_age(*, item: ExecutionPlanItem, triggered_at: datetime) -> int:
    source_date = item.source_market_date or item.plan.market_date
    if source_date is None:
        return 1
    market_date = triggered_at.astimezone(KRX_TIMEZONE).date()
    return max(1, (market_date - source_date).days + 1)


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _max_holding_exit_due(*, item: ExecutionPlanItem, triggered_at: datetime) -> bool:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    raw_close_exit_date = risk_context.get("close_exit_date")
    if not isinstance(raw_close_exit_date, str):
        return False
    try:
        close_exit_date = datetime.strptime(raw_close_exit_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    return triggered_at.astimezone(UTC).date() >= close_exit_date


def _resolve_sell_quantity(
    *,
    item: ExecutionPlanItem,
    remaining_quantity: int | None,
) -> int | None:
    if remaining_quantity is None or remaining_quantity <= 0:
        return None
    quantity_policy = item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return None
    if _quantity_policy_requires_position_exit(quantity_policy):
        return remaining_quantity

    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = _parse_positive_int(quantity_policy.get("quantity"))
        if quantity is None or quantity > remaining_quantity:
            return None
        return quantity
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        if fraction is None:
            return None
        raw_quantity = (Decimal(remaining_quantity) * fraction).quantize(
            Decimal("1"),
            rounding=ROUND_DOWN,
        )
        quantity = int(raw_quantity)
        if quantity <= 0:
            quantity = 1
        if quantity > remaining_quantity:
            quantity = remaining_quantity
        return quantity
    return None


def _resolve_watch_buy_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    requested_quantity: int | None = None,
    trigger_time_agent_approved: bool = False,
    sizing_source: str | None = None,
) -> WatchBuyOrderApproval:
    decision = _exec_agent_decision(item)
    if isinstance(decision, dict):
        decision_value = str(
            decision.get("decision") or decision.get("action") or ""
        ).strip().lower()
        if decision_value == "rejected" or decision_value == "reject":
            return WatchBuyOrderApproval(
                quantity=None,
                reason=str(decision.get("reason") or "exec_agent_rejected"),
                message="exec-agent/risk policy rejected this watch buy",
            )

    quantity = (
        _parse_positive_int(requested_quantity)
        if requested_quantity is not None
        else _parse_buy_quantity(item.quantity_policy)
    )
    if quantity is None and isinstance(decision, dict):
        quantity = _parse_positive_int(
            decision.get("approved_quantity") or decision.get("quantity")
        )
    if (
        quantity is None
        and _watch_buy_requires_exec_agent_sizing(item)
        and not _watch_buy_exec_agent_sizing_fallback_allowed(
            settings=settings,
            item=item,
        )
    ):
        return WatchBuyOrderApproval(
            quantity=None,
            reason="exec_agent_sizing_pending",
            message="watch buy requires an exec-agent share quantity before order submission",
        )

    control_state = ensure_runtime_control_state(settings=settings, db=db)
    if not automated_order_submission_allowed(control_state):
        if quantity is not None:
            return WatchBuyOrderApproval(quantity=quantity)
        if _watch_buy_candidate_budget(item) is not None:
            block_reason = automated_order_submission_block_reason(control_state)
            return WatchBuyOrderApproval(
                quantity=None,
                reason=block_reason,
                message="automated order submission is blocked by runtime safety controls",
            )
        return WatchBuyOrderApproval(
            quantity=None,
            reason="missing_quantity",
            message="watch buy requires a positive share quantity while runtime is blocked",
        )

    entry_price = observed_price or _watch_buy_trigger_price(item) or item.limit_price
    stale_reclaim_veto = _watch_buy_stale_trigger_reclaim_veto(
        settings=settings,
        item=item,
        current_reference_price=entry_price,
        trigger_time_agent_approved=trigger_time_agent_approved,
    )
    if stale_reclaim_veto is not None:
        return stale_reclaim_veto

    stop_price = _watch_buy_stop_price(item)
    if entry_price is None or stop_price is None:
        return WatchBuyOrderApproval(
            quantity=None,
            reason="missing_stop_or_entry_price",
            message="watch buy requires entry and stop prices for risk-based sizing",
        )

    snapshot = _load_watch_buy_account_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
    )
    if isinstance(snapshot, str):
        return WatchBuyOrderApproval(
            quantity=None,
            reason="account_snapshot_unavailable",
            message=snapshot,
        )

    available_cash = _minimum_decimal(snapshot.available_cash, snapshot.buying_power)
    existing_exposure = entry_price * Decimal(
        _snapshot_symbol_quantity(snapshot=snapshot, symbol=item.symbol)
        + _local_position_quantity(settings=settings, db=db, symbol=item.symbol)
    )
    effective_stop = effective_stop_price(
        settings=settings,
        entry_price=entry_price,
        upstream_stop_price=stop_price,
    )
    candidate_budget = _watch_buy_candidate_budget(item)
    exec_agent_notional_cap = _watch_buy_exec_agent_notional_cap(item)
    candidate_max_budget = _minimum_decimal(candidate_budget, exec_agent_notional_cap)
    if exec_agent_notional_cap is not None:
        cap_quantity = _floor_quantity(exec_agent_notional_cap / entry_price)
        if cap_quantity <= 0:
            details = {
                "entry_price": _serialize_decimal(entry_price),
                "observed_price": _serialize_decimal(observed_price),
                "exec_agent_approved_notional_cap": _serialize_decimal(exec_agent_notional_cap),
                "stored_quantity": quantity,
                "max_quantity_within_cap": cap_quantity,
            }
            return WatchBuyOrderApproval(
                quantity=None,
                reason="exec_agent_approved_notional_exhausted",
                message="exec-agent approved notional cannot buy one share at the current entry price",
                details=details,
            )
    risk_decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=available_cash,
        available_cash=available_cash,
        entry_price=entry_price,
        stop_price=effective_stop,
        confidence=_watch_buy_confidence(item),
        requested_quantity=quantity,
        candidate_max_budget=candidate_max_budget,
        agent_max_quantity=_watch_buy_max_quantity(item),
        existing_symbol_exposure=existing_exposure,
        daily_new_buy_notional_used=_load_daily_new_buy_notional(
            settings=settings,
            db=db,
            market_date=item.plan.market_date,
        ),
        market_regime_context=market_regime_context_from_item_payloads(item.risk_context, item.quantity_policy),
        market_date=item.plan.market_date,
    )
    if isinstance(risk_decision, RiskSizingRejection):
        details = dict(risk_decision.details)
        if requested_quantity is not None:
            details["requested_quantity"] = requested_quantity
        if sizing_source is not None:
            details["sizing_source"] = sizing_source
        return WatchBuyOrderApproval(
            quantity=None,
            reason=risk_decision.reason_code,
            message=risk_decision.message,
            details=details,
        )
    assert isinstance(risk_decision, RiskSizingResult)
    approved_quantity = risk_decision.quantity
    approved_notional = risk_decision.approved_notional
    exec_agent_cap_adjustment: dict[str, object] | None = None
    if exec_agent_notional_cap is not None:
        cap_quantity = _floor_quantity(exec_agent_notional_cap / entry_price)
        if approved_quantity > cap_quantity:
            approved_quantity = cap_quantity
            approved_notional = (entry_price * Decimal(approved_quantity)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_DOWN,
            )
            exec_agent_cap_adjustment = {
                "reason": "exec_agent_approved_notional_cap",
                "entry_price": _serialize_decimal(entry_price),
                "observed_price": _serialize_decimal(observed_price),
                "exec_agent_approved_notional_cap": _serialize_decimal(exec_agent_notional_cap),
                "original_quantity": risk_decision.quantity,
                "adjusted_quantity": approved_quantity,
                "original_notional": _serialize_decimal(risk_decision.approved_notional),
                "adjusted_notional": _serialize_decimal(approved_notional),
            }
    event = {
        "event": "watch_buy_risk_sized",
        "quantity": approved_quantity,
        "approved_notional": _serialize_decimal(approved_notional),
        "risk_pct": _serialize_decimal(risk_decision.risk_pct),
        "risk_budget": _serialize_decimal(risk_decision.risk_budget),
        "stop_price": _serialize_decimal(effective_stop),
        "upstream_stop_price": _serialize_decimal(stop_price),
        "risk_based_quantity": risk_decision.risk_quantity,
        "allocation_quantity": risk_decision.allocation_quantity,
        "cash_quantity": risk_decision.cash_quantity,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    market_regime_guardrail = risk_decision.cap_details.get("market_regime_guardrail")
    if isinstance(market_regime_guardrail, dict):
        event["market_regime_guardrail"] = market_regime_guardrail
    if requested_quantity is not None:
        event["requested_quantity"] = requested_quantity
    if sizing_source is not None:
        event["sizing_source"] = sizing_source
        event["trigger_time_exec_agent_quantity"] = requested_quantity
        if requested_quantity is not None and approved_quantity < requested_quantity:
            event["sizing_adjustment"] = "clamped_by_deterministic_risk_sizing"
    if exec_agent_notional_cap is not None:
        event["exec_agent_approved_notional_cap"] = _serialize_decimal(exec_agent_notional_cap)
    if exec_agent_cap_adjustment is not None:
        event["submission_cap_adjustment"] = exec_agent_cap_adjustment
    if _watch_buy_requires_exec_agent_sizing(item):
        event["sizing_source"] = sizing_source or "risk_sizing_after_exec_agent_attempts_exhausted"
        event["exec_agent_watch_sizing_attempt_count"] = _first_payload_value(
            item,
            "exec_agent_watch_sizing_attempt_count",
        )
    if effective_stop > stop_price:
        _apply_watch_buy_effective_stop(
            item=item,
            effective_stop=effective_stop,
            upstream_stop=stop_price,
        )
        event["stop_monitor_adjustment"] = "raised_to_effective_risk_stop"
    _record_monitoring_event(
        item=item,
        event=event,
    )
    return WatchBuyOrderApproval(quantity=approved_quantity)


def _watch_buy_stale_trigger_reclaim_veto(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    current_reference_price: Decimal | None,
    trigger_time_agent_approved: bool,
) -> WatchBuyOrderApproval | None:
    if current_reference_price is None:
        return None
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    if _watch_buy_direction(item) != "at_or_above":
        return None
    threshold_pct = settings.watch_buy_stale_trigger_reclaim_threshold_pct
    if threshold_pct <= 0:
        return None
    stale_reference_price = (trigger_price * (Decimal("1") - (threshold_pct / Decimal("100")))).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if current_reference_price > stale_reference_price:
        return None
    details = {
        "reason": "stale_trigger_reclaim_required",
        "trigger_price": _serialize_decimal(trigger_price),
        "current_reference_price": _serialize_decimal(current_reference_price),
        "reference_price": _serialize_decimal(current_reference_price),
        "stale_reference_price": _serialize_decimal(stale_reference_price),
        "threshold_pct": _serialize_decimal(threshold_pct),
        "direction": _watch_buy_direction(item),
    }
    if _watch_buy_is_pullback_strategy(item):
        _record_watch_buy_stale_trigger_reclaim_bypass(
            item=item,
            details={**details, "bypass_reason": "pullback_strategy_profile"},
        )
        return None
    if (
        trigger_time_agent_approved
        and _trigger_time_decision_confirms_fresh_pullback_reclaim(item)
    ):
        _record_watch_buy_stale_trigger_reclaim_bypass(
            item=item,
            details={**details, "bypass_reason": "trigger_time_pullback_reclaim_confirmation"},
        )
        return None
    return WatchBuyOrderApproval(
        quantity=None,
        reason="stale_trigger_reclaim_required",
        message="current reference price is materially below the original breakout/reclaim trigger",
        details=details,
    )


def _record_watch_buy_stale_trigger_reclaim_bypass(
    *,
    item: ExecutionPlanItem,
    details: dict[str, object],
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    risk_context["last_stale_trigger_reclaim_check"] = details
    item.risk_context = risk_context


def _watch_buy_is_pullback_strategy(item: ExecutionPlanItem) -> bool:
    for source in _item_payload_sources(item):
        for key in (
            "strategy_profile",
            "strategy_type",
            "setup_profile",
            "setup_type",
            "entry_profile",
            "tactical_action",
            "handoff_intent",
        ):
            value = source.get(key)
            if isinstance(value, str) and "pullback" in value.strip().lower():
                return True
        strategy = source.get("strategy")
        if isinstance(strategy, dict):
            for key in ("profile", "type", "name"):
                value = strategy.get(key)
                if isinstance(value, str) and "pullback" in value.strip().lower():
                    return True
        if source.get("pullback_strategy") is True:
            return True
    return False


def _trigger_time_decision_confirms_fresh_pullback_reclaim(item: ExecutionPlanItem) -> bool:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return False
    decision = interaction.get("decision")
    response = interaction.get("response_payload")
    if not isinstance(decision, dict):
        return False
    if decision.get("decision") != "execute_now":
        return False
    has_updated_stop = _parse_positive_decimal(decision.get("updated_stop_price")) is not None
    has_updated_target = _parse_positive_decimal(decision.get("updated_target_price")) is not None
    has_updated_trigger = any(
        _parse_positive_decimal(payload.get(key)) is not None
        for payload in (decision, response if isinstance(response, dict) else {})
        for key in ("updated_trigger_price", "new_trigger_price", "reclaim_trigger_price")
        if isinstance(payload, dict)
    )
    if not (has_updated_stop and has_updated_target and has_updated_trigger):
        return False
    text = str({"decision": decision, "response_payload": response}).lower()
    return "pullback" in text or "reclaim" in text


def _apply_watch_buy_effective_stop(
    *,
    item: ExecutionPlanItem,
    effective_stop: Decimal,
    upstream_stop: Decimal,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    risk_context["effective_stop_price"] = _serialize_decimal(effective_stop)
    risk_context["upstream_stop_price"] = _serialize_decimal(upstream_stop)
    risk_context["stop_price"] = _serialize_decimal(effective_stop)
    risk_context["stop_loss_price"] = _serialize_decimal(effective_stop)
    risk_context["stop_monitor_adjustment"] = "raised_to_effective_risk_stop"
    item.risk_context = risk_context
    _update_sibling_trigger_price(
        item=item,
        intent="stop_sell",
        trigger_price=effective_stop,
    )


def _parse_buy_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict):
        return None
    if quantity_policy.get("kind") != "shares":
        return None
    return _parse_positive_int(quantity_policy.get("quantity"))


def _watch_buy_requires_exec_agent_sizing(item: ExecutionPlanItem) -> bool:
    quantity_policy = item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return False
    for key in ("kind", "type"):
        value = quantity_policy.get(key)
        if isinstance(value, str) and value.strip().lower() == "exec_agent_sized":
            return True
    return False


def _watch_buy_exec_agent_sizing_fallback_allowed(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> bool:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    if (
        quantity_policy.get("exec_agent_watch_sizing_fallback_allowed") is True
        or risk_context.get("exec_agent_watch_sizing_fallback_allowed") is True
    ):
        return True
    attempts = _parse_positive_int(
        quantity_policy.get("exec_agent_watch_sizing_attempt_count")
        or risk_context.get("exec_agent_watch_sizing_attempt_count")
    ) or 0
    max_attempts = (
        _parse_positive_int(
            quantity_policy.get("exec_agent_watch_sizing_max_attempts")
            or risk_context.get("exec_agent_watch_sizing_max_attempts")
        )
        or settings.exec_agent_watch_sizing_max_attempts
    )
    return attempts >= max_attempts


def _first_payload_value(item: ExecutionPlanItem, key: str) -> object | None:
    for payload in (item.quantity_policy, item.risk_context):
        if isinstance(payload, dict) and key in payload:
            return payload.get(key)
    return None


def _watch_buy_candidate_budget(item: ExecutionPlanItem) -> Decimal | None:
    for source in _item_payload_sources(item):
        for key in (
            "max_budget",
            "max_budget_krw",
            "budget",
            "budget_krw",
            "cash_budget",
            "cash_budget_krw",
            "max_notional",
            "max_notional_krw",
            "max_safe_notional",
            "max_safe_notional_krw",
            "notional",
        ):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    return None


def _watch_buy_exec_agent_notional_cap(item: ExecutionPlanItem) -> Decimal | None:
    caps: list[Decimal] = []
    for source in _item_payload_sources(item):
        source_name = str(source.get("source") or "").strip().lower()
        if source_name == "exec_agent_watch_sizing":
            for key in ("approved_notional", "approved_notional_krw"):
                value = _parse_positive_decimal(source.get(key))
                if value is not None:
                    caps.append(value)
        sizing_adjustment = source.get("sizing_adjustment")
        if isinstance(sizing_adjustment, dict):
            for key in (
                "final_notional",
                "adjusted_notional",
                "approved_notional",
                "cap_notional",
                "agent_approved_notional",
            ):
                value = _parse_positive_decimal(sizing_adjustment.get(key))
                if value is not None:
                    caps.append(value)
    return min(caps) if caps else None


def _watch_buy_max_quantity(item: ExecutionPlanItem) -> int | None:
    for source in _item_payload_sources(item):
        for key in ("max_quantity", "max_shares", "share_cap"):
            quantity = _parse_positive_int(source.get(key))
            if quantity is not None:
                return quantity
    return None


def _watch_buy_confidence(item: ExecutionPlanItem) -> Decimal | None:
    for source in _item_payload_sources(item):
        for key in ("confidence", "score", "probability"):
            value = _parse_positive_decimal(source.get(key))
            if value is None:
                continue
            if value > 1 and value <= 100:
                value = value / Decimal("100")
            if value <= 1:
                return value
    decision = _exec_agent_decision(item)
    if isinstance(decision, dict):
        value = _parse_positive_decimal(decision.get("confidence"))
        if value is not None:
            return value / Decimal("100") if value > 1 and value <= 100 else value
    return None


def _watch_buy_stop_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        (
            "stop_price",
            "stop_sell_price",
            "stop_loss_price",
            "risk_stop_price",
            "stop",
        ),
    )


def _watch_buy_target_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        (
            "target_price",
            "take_profit_price",
            "first_take_profit_price",
            "profit_target_price",
            "target",
        ),
    )


def _load_watch_buy_account_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
) -> BrokerAccountSnapshot | str:
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="watch_buy_risk_sizing",
            reason="resolve account buying power for intraday watch buy sizing",
        )
        return adapter.get_account_snapshot(session_state.access_token or "")
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError) as exc:
        return f"{exc.__class__.__name__}: {exc}"


def _load_watch_buy_cash_reservation(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    triggered_at: datetime,
) -> WatchBuyCashReservation | str:
    snapshot = _load_watch_buy_account_snapshot(settings=settings, db=db, adapter=adapter)
    if isinstance(snapshot, str):
        return snapshot
    orderable_cash = _minimum_decimal(snapshot.available_cash, snapshot.buying_power)
    if orderable_cash is None:
        return "account snapshot did not include orderable cash"
    already_reserved = _snapshot_open_buy_order_notional(snapshot.open_orders)
    already_reserved += _local_open_buy_order_notional(
        settings=settings,
        db=db,
        snapshot_open_orders=snapshot.open_orders,
    )
    return WatchBuyCashReservation(
        orderable_cash=orderable_cash,
        already_reserved_notional=already_reserved,
        batch_id=f"monitoring:{MONITORING_WORKER_NAME}:{triggered_at.isoformat()}",
    )


def _snapshot_open_buy_order_notional(open_orders: list[BrokerAccountOpenOrder]) -> Decimal:
    total = Decimal("0")
    for order in open_orders:
        if order.side != "buy":
            continue
        total += _open_buy_order_notional(
            quantity=order.remaining_quantity or order.quantity,
            limit_price=order.limit_price,
        )
    return total


def _local_open_buy_order_notional(
    *,
    settings: Settings,
    db: Session,
    snapshot_open_orders: list[BrokerAccountOpenOrder],
) -> Decimal:
    snapshot_broker_order_ids = {
        order.broker_order_id
        for order in snapshot_open_orders
        if order.side == "buy" and order.broker_order_id
    }
    snapshot_idempotency_keys_without_broker_id = {
        order.idempotency_key
        for order in snapshot_open_orders
        if order.side == "buy" and not order.broker_order_id and order.idempotency_key
    }
    orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(tuple(OPEN_BUY_ORDER_STATUSES)),
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.idempotency_key,
                BrokerOrder.broker_order_id,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.limit_price,
            )
        )
    ).all()
    total = Decimal("0")
    for order in orders:
        if order.broker_order_id and order.broker_order_id in snapshot_broker_order_ids:
            continue
        if (
            not order.broker_order_id
            and order.idempotency_key
            and order.idempotency_key in snapshot_idempotency_keys_without_broker_id
        ):
            continue
        total += _open_buy_order_notional(
            quantity=order.quantity,
            limit_price=order.limit_price,
        )
    return total


def _open_buy_order_notional(*, quantity: int | None, limit_price: Decimal | None) -> Decimal:
    if quantity is None or quantity <= 0 or limit_price is None:
        return Decimal("0")
    return Decimal(quantity) * limit_price


def _watch_buy_order_notional(*, quantity: int, limit_price: Decimal | None) -> Decimal | None:
    if quantity <= 0 or limit_price is None:
        return None
    return Decimal(quantity) * limit_price


def _watch_buy_cash_reservation_payload(
    *,
    reservation: WatchBuyCashReservation,
    candidate_notional: Decimal,
    item: ExecutionPlanItem,
    quantity: int,
    limit_price: Decimal,
    triggered_at: datetime,
) -> dict[str, object]:
    return {
        "reason": RESERVED_CASH_EXHAUSTED_REASON,
        "batch_id": reservation.batch_id,
        "cycle_id": reservation.batch_id,
        "orderable_cash": _serialize_decimal(reservation.orderable_cash),
        "available_cash": _serialize_decimal(reservation.remaining_cash),
        "already_reserved_notional": _serialize_decimal(reservation.already_reserved_notional),
        "candidate_notional": _serialize_decimal(candidate_notional),
        "quantity": quantity,
        "limit_price": _serialize_decimal(limit_price),
        "symbol": item.symbol,
        "execution_plan_item_id": item.id,
        "recorded_at": triggered_at.isoformat(),
    }


def _record_reserved_cash_exhausted(
    *,
    db: Session,
    item: ExecutionPlanItem,
    payload: dict[str, object],
    triggered_at: datetime,
) -> None:
    _record_monitoring_event(
        item=item,
        event={
            "event": "watch_buy_rejected",
            "reason": RESERVED_CASH_EXHAUSTED_REASON,
            "message": "watch buy reserved cash budget exhausted before broker submission",
            "details": payload,
            "recorded_at": triggered_at.isoformat(),
        },
    )
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=RESERVED_CASH_EXHAUSTED_REASON,
            source=MONITORING_WORKER_NAME,
            summary={
                "reason": RESERVED_CASH_EXHAUSTED_REASON,
                "batch_id": payload.get("batch_id"),
                "symbol": item.symbol,
            },
            payload=payload,
            error_message="watch buy reserved cash budget exhausted before broker submission",
            occurred_at=triggered_at,
        )
    )


def _snapshot_symbol_quantity(*, snapshot: BrokerAccountSnapshot, symbol: str) -> int:
    positions = snapshot.positions or snapshot.holdings
    return sum(position.quantity for position in positions if position.symbol == symbol and position.quantity > 0)


def _local_position_quantity(*, settings: Settings, db: Session, symbol: str) -> int:
    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol == symbol,
        )
    )
    return position.quantity if position is not None and position.quantity > 0 else 0


def _load_daily_new_buy_notional(
    *,
    settings: Settings,
    db: Session,
    market_date,
) -> Decimal:
    if market_date is None:
        return Decimal("0")
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.status.in_(
                (
                    "next_open_buy_scheduled",
                    "next_open_buy_dispatching",
                    "next_open_buy_retry_pending",
                    "watch_buy_submitted",
                    "broker_order_submission_pending",
                    "broker_order_submitted",
                    "broker_order_retry_pending",
                    "submitted",
                    "open",
                    "partially_filled",
                    "filled",
                    "retry_pending",
                )
            ),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.risk_context,
            )
        )
    ).all()
    total = Decimal("0")
    for item in items:
        total += _approved_buy_notional(item)
    return total


def _approved_buy_notional(item: ExecutionPlanItem) -> Decimal:
    for source in _item_payload_sources(item):
        decision = source.get("exec_agent_decision")
        if isinstance(decision, dict):
            value = _parse_positive_decimal(decision.get("approved_notional"))
            if value is not None:
                return value
        for key in ("approved_notional", "notional", "max_notional"):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    quantity = _parse_buy_quantity(item.quantity_policy)
    price = item.limit_price or _watch_buy_trigger_price(item)
    if quantity is None or price is None:
        return Decimal("0")
    return price * Decimal(quantity)


def _minimum_decimal(*values: Decimal | None) -> Decimal | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return min(concrete)


def _floor_quantity(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _monitoring_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_monitoring_active"
    return f"{intent}_monitoring"


def _pending_dispatch_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_buy_triggered_pending_dispatch"
    return f"{intent}_triggered_pending_dispatch"


def _pending_configuration_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_monitoring_pending"
    return f"{intent}_pending_config"


def _reserve_remaining_quantity(
    *,
    remaining_quantities_by_symbol: dict[str, int],
    symbol: str,
    quantity: int,
) -> None:
    remaining_quantity = remaining_quantities_by_symbol.get(symbol)
    if remaining_quantity is None or remaining_quantity <= 0 or quantity <= 0:
        return
    updated_quantity = remaining_quantity - quantity
    if updated_quantity > 0:
        remaining_quantities_by_symbol[symbol] = updated_quantity
        return
    remaining_quantities_by_symbol.pop(symbol, None)


def _reserve_and_rearm_sell_after_success(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    remaining_quantities_by_symbol: dict[str, int],
    submitted_quantity: int,
    recorded_at: datetime,
) -> None:
    _reserve_remaining_quantity(
        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
        symbol=item.symbol,
        quantity=submitted_quantity,
    )
    _rearm_stop_after_partial_take_profit(
        settings=settings,
        item=item,
        position=position,
        remaining_quantity=remaining_quantities_by_symbol.get(item.symbol),
        submitted_quantity=submitted_quantity,
        recorded_at=recorded_at,
    )


def _reserve_existing_exit_orders(
    *,
    settings: Settings,
    db: Session,
    remaining_quantities_by_symbol: dict[str, int],
    triggered_at: datetime,
) -> None:
    if not remaining_quantities_by_symbol:
        return
    day_start = triggered_at.astimezone(KRX_TIMEZONE).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ).astimezone(UTC)
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.side == "sell",
            BrokerOrder.symbol.in_(sorted(remaining_quantities_by_symbol)),
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
            BrokerOrder.updated_at >= day_start,
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.symbol,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.updated_at,
            )
        )
        .order_by(BrokerOrder.updated_at.asc(), BrokerOrder.id.asc())
    ).all()
    for order in orders:
        _reserve_remaining_quantity(
            remaining_quantities_by_symbol=remaining_quantities_by_symbol,
            symbol=order.symbol,
            quantity=order.quantity,
        )


def _rearm_stop_after_partial_take_profit(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    remaining_quantity: int | None,
    submitted_quantity: int,
    recorded_at: datetime,
) -> None:
    if item.intent != "target_sell" or remaining_quantity is None or remaining_quantity <= 0:
        return
    exit_rule = _exit_rule(item)
    if exit_rule.get("kind") != "partial_take_profit":
        return
    if position is None or position.average_price is None or position.average_price <= 0:
        return

    breakeven_stop = _breakeven_stop_price(
        average_price=position.average_price,
        buffer_pct=settings.breakeven_stop_buffer_pct,
    )
    plan = item.plan
    if plan is None:
        return

    for sibling in plan.items:
        if sibling.symbol != item.symbol or sibling.intent != "stop_sell":
            continue
        if sibling.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue

        prior_trigger = sibling.trigger_price
        prior_quantity_policy = sibling.quantity_policy
        if prior_trigger is None or prior_trigger < breakeven_stop:
            sibling.trigger_price = breakeven_stop
        sibling.quantity_policy = {
            "kind": "shares",
            "quantity": remaining_quantity,
            "source": "partial_take_profit_breakeven_rearm",
            "parent_target_item_id": item.id,
        }
        risk_context = dict(sibling.risk_context) if isinstance(sibling.risk_context, dict) else {}
        risk_context["breakeven_rearmed"] = {
            "source": "partial_take_profit",
            "target_item_id": item.id,
            "submitted_take_profit_quantity": submitted_quantity,
            "remaining_quantity": remaining_quantity,
            "average_price": _serialize_decimal(position.average_price),
            "breakeven_stop_price": _serialize_decimal(sibling.trigger_price),
            "buffer_pct": _serialize_decimal(settings.breakeven_stop_buffer_pct),
            "prior_trigger_price": _serialize_decimal(prior_trigger),
            "prior_quantity_policy": prior_quantity_policy,
            "recorded_at": recorded_at.isoformat(),
        }
        sibling.risk_context = risk_context
        _record_monitoring_event(
            item=sibling,
            event={
                "event": "stop_sell_rearmed_after_partial_take_profit",
                "reason": "partial_take_profit_submitted",
                "target_item_id": item.id,
                "submitted_take_profit_quantity": submitted_quantity,
                "remaining_quantity": remaining_quantity,
                "prior_trigger_price": _serialize_decimal(prior_trigger),
                "updated_trigger_price": _serialize_decimal(sibling.trigger_price),
                "average_price": _serialize_decimal(position.average_price),
                "buffer_pct": _serialize_decimal(settings.breakeven_stop_buffer_pct),
                "recorded_at": recorded_at.isoformat(),
            },
        )


def _exit_rule(item: ExecutionPlanItem) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exit_rule = risk_context.get("exit_rule")
    return dict(exit_rule) if isinstance(exit_rule, dict) else {}


def _breakeven_stop_price(*, average_price: Decimal, buffer_pct: Decimal) -> Decimal:
    multiplier = Decimal("1") + (buffer_pct / Decimal("100"))
    return (average_price * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _serialized_order_quantity(order_payload: object) -> int | None:
    if not isinstance(order_payload, dict):
        return None
    return _parse_positive_int(order_payload.get("quantity"))


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _parse_fraction(value: object) -> Decimal | None:
    try:
        fraction = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if fraction <= 0 or fraction > 1:
        return None
    return fraction


def _is_watch_buy_item(item: ExecutionPlanItem) -> bool:
    return item.side == "buy" and (
        item.intent in WATCH_BUY_INTENTS or item.status in WATCH_MONITORING_STATUSES
    )


def _watch_buy_trigger_price(item: ExecutionPlanItem) -> Decimal | None:
    if item.trigger_price is not None and item.trigger_price > 0:
        return item.trigger_price
    return _first_positive_decimal_from_item(
        item,
        (
            "buy_recommendation_price",
            "recommended_entry_price",
            "entry_recommendation_price",
            "recommendation_price",
            "entry_price",
            "watch_trigger_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "original_watch_trigger_price",
        ),
    )


def _watch_buy_chase_limit_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        ("chase_limit_price", "max_chase_limit_price", "entry_limit_price", "original_watch_chase_limit_price"),
    )


def _watch_buy_high_risk_gate_rejection(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal,
    price: MonitoringPriceRequest | None = None,
) -> dict[str, object] | None:
    if not settings.watch_buy_high_risk_gate_enabled:
        return None
    sources = []
    live_market_context = _market_quality_context_from_price(price)
    if live_market_context is not None:
        sources.append(live_market_context)
    sources.extend(_item_payload_sources(item))
    payload = {"sources": sources}
    signal_action = (_recursive_text_feature(payload, {"action"}, parent_key="signal_decider_crosscheck") or "").lower()
    tactical_action = (
        _recursive_text_feature(payload, {"tactical_action"})
        or _recursive_text_feature(payload, {"handoff_intent"})
        or ""
    ).lower()
    if signal_action == "reject":
        return _watch_buy_gate_rejection(
            reason="signal_decider_reject_veto",
            message="same-day signal-decider rejected this setup",
            details={"signal_decider_action": signal_action, "tactical_action": tactical_action},
        )
    if signal_action == "wait" and tactical_action in {"scalp_buy_candidate", "tactical_buy"}:
        return _watch_buy_gate_rejection(
            reason="signal_decider_wait_blocks_tactical_buy",
            message="same-day signal-decider is wait, so tactical buy requires a stricter watch setup",
            details={"signal_decider_action": signal_action, "tactical_action": tactical_action},
        )

    atr_pct = _recursive_decimal_feature(payload, {"atr_pct_14", "atr_pct", "atr14_pct", "atr_percent"})
    ret20 = _recursive_decimal_feature(payload, {"ret20", "ret_20d", "return_20d", "pct_change_20d"})
    ret5 = _recursive_decimal_feature(payload, {"ret5", "ret_5d", "return_5d", "pct_change_5d"})
    trend_state = (_recursive_text_feature(payload, {"trend_state", "technical_trend", "market_regime"}) or "").lower()
    setup_state = (_recursive_text_feature(payload, {"setup_state", "breakdown_state", "pattern_state"}) or "").lower()
    details = {
        "atr_pct_14": _serialize_decimal(atr_pct),
        "ret20": _serialize_decimal(ret20),
        "ret5": _serialize_decimal(ret5),
        "trend_state": trend_state or None,
        "setup_state": setup_state or None,
    }
    if any(marker in f"{trend_state} {setup_state}" for marker in ("downtrend", "bearish", "breakdown_20d", "breakdown")):
        return _watch_buy_gate_rejection(
            reason="short_term_downtrend_or_breakdown_block",
            message="watch buy setup is downtrend/bearish/breakdown",
            details=details,
        )
    if ret20 is not None and ret20 < settings.watch_buy_deep_negative_ret20_threshold_pct:
        return _watch_buy_gate_rejection(
            reason="short_term_ret20_deep_negative_block",
            message="20-day return is deeply negative for a short-term watch buy",
            details=details,
        )
    if (
        atr_pct is not None
        and atr_pct >= settings.watch_buy_high_atr_pct_threshold
        and ret20 is not None
        and ret20 <= settings.watch_buy_weak_ret20_threshold_pct
    ):
        return _watch_buy_gate_rejection(
            reason="short_term_high_atr_weak_trend_block",
            message="ATR is high while 20-day return is weak",
            details=details,
        )
    if (
        atr_pct is not None
        and atr_pct > settings.watch_buy_high_atr_pct_threshold
        and ret5 is not None
        and ret5 > settings.watch_buy_extended_ret5_threshold_pct
    ):
        return _watch_buy_gate_rejection(
            reason="short_term_high_atr_chase_block",
            message="5-day move is extended while ATR is high",
            details=details,
        )

    trigger_price = _watch_buy_trigger_price(item)
    direction = _watch_buy_direction(item)
    pullback_after_cross = (
        trigger_price is not None
        and direction == "at_or_above"
        and (
            observed_price < trigger_price
        )
    )
    day_high = _recursive_decimal_feature(payload, {"day_high", "intraday_high", "high_price", "session_high"})
    day_low = _recursive_decimal_feature(payload, {"day_low", "intraday_low", "low_price", "session_low"})
    if day_high is not None and day_low is not None and day_high > day_low:
        range_position_pct = ((observed_price - day_low) / (day_high - day_low)) * Decimal("100")
        if (
            range_position_pct >= settings.watch_buy_max_intraday_range_position_pct
            and not pullback_after_cross
        ):
            return _watch_buy_gate_rejection(
                reason="watch_buy_intraday_range_chase_block",
                message="triggered price is too high in the current intraday range",
                details={
                    **details,
                    "day_high": _serialize_decimal(day_high),
                    "day_low": _serialize_decimal(day_low),
                    "range_position_pct": _serialize_decimal(range_position_pct),
                    "max_range_position_pct": _serialize_decimal(settings.watch_buy_max_intraday_range_position_pct),
                },
            )
    return None


def _watch_buy_gate_rejection(
    *,
    reason: str,
    message: str,
    details: dict[str, object],
) -> dict[str, object]:
    return {
        "reason": reason,
        "message": message,
        "details": details,
    }


def _is_retryable_watch_buy_pullback_gate(reason: object) -> bool:
    return reason in {
        "watch_buy_intraday_range_chase_block",
    }


def _watch_buy_direction(item: ExecutionPlanItem) -> str:
    for source in _item_payload_sources(item):
        for key in ("trigger_direction", "entry_direction", "watch_direction", "direction"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                normalized = value.strip().lower()
                if normalized in {"up", "above", "breakout", "momentum", ">=", "at_or_above"}:
                    return "at_or_above"
                if normalized in {"down", "below", "pullback", "dip", "<=", "at_or_below"}:
                    return "at_or_below"
    if _watch_buy_tactical_action(item) == "watch_for_trigger":
        return "at_or_above"
    monitoring = _watch_buy_monitoring_context(item)
    direction = monitoring.get("direction")
    if isinstance(direction, str) and direction == "at_or_above":
        return "at_or_above"
    if isinstance(direction, str) and direction == "at_or_below":
        return "at_or_below"
    return "at_or_below"


def _watch_buy_tactical_action(item: ExecutionPlanItem) -> str | None:
    for source in _item_payload_sources(item):
        value = source.get("tactical_action")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    monitoring = _watch_buy_monitoring_context(item)
    value = monitoring.get("tactical_action")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return None


def _first_positive_decimal_from_item(
    item: ExecutionPlanItem,
    keys: tuple[str, ...],
) -> Decimal | None:
    monitoring = _watch_buy_monitoring_context(item)
    for key in keys:
        value = _parse_positive_decimal(monitoring.get(key))
        if value is not None:
            return value
    for source in _item_payload_sources(item):
        for key in keys:
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    return None


def _watch_buy_monitoring_context(item: ExecutionPlanItem) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    monitoring = risk_context.get("watch_buy_monitoring")
    return monitoring if isinstance(monitoring, dict) else {}


def _item_payload_sources(item: ExecutionPlanItem) -> list[dict[str, object]]:
    sources: list[dict[str, object]] = []
    for source in (item.risk_context, item.quantity_policy):
        if isinstance(source, dict):
            sources.append(source)
    return sources


def _parse_positive_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > 0 else None


def _recursive_decimal_feature(payload: object, keys: set[str], *, parent_key: str | None = None) -> Decimal | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized_key = str(key or "").strip().lower()
            if parent_key is not None and normalized_key == parent_key:
                found = _recursive_decimal_feature(value, keys)
                if found is not None:
                    return found
            if parent_key is None and normalized_key in keys:
                parsed = _parse_decimal(value)
                if parsed is not None:
                    return parsed
            found = _recursive_decimal_feature(value, keys, parent_key=parent_key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _recursive_decimal_feature(item, keys, parent_key=parent_key)
            if found is not None:
                return found
    return None


def _recursive_text_feature(payload: object, keys: set[str], *, parent_key: str | None = None) -> str | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized_key = str(key or "").strip().lower()
            if parent_key is not None and normalized_key == parent_key:
                found = _recursive_text_feature(value, keys)
                if found is not None:
                    return found
            if parent_key is None and normalized_key in keys and value is not None:
                text = str(value).strip()
                if text:
                    return text
            found = _recursive_text_feature(value, keys, parent_key=parent_key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _recursive_text_feature(item, keys, parent_key=parent_key)
            if found is not None:
                return found
    return None


def _exec_agent_decision(item: ExecutionPlanItem) -> dict[str, object] | None:
    for source in (item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        for key in (
            "exec_agent_decision",
            "short_term_exec_agent",
            "short_term_exec_agent_decision",
        ):
            value = source.get(key)
            if isinstance(value, dict):
                return value
    return None


def _quote_stale_reason(
    *,
    settings: Settings,
    price: MonitoringPriceRequest | None,
    cycle_at: datetime,
) -> str | None:
    if price is None:
        return "quote is missing for watch buy evaluation"
    observed_at = _price_observed_at(price, cycle_at)
    max_age = timedelta(seconds=settings.monitoring_max_quote_age_seconds)
    age = cycle_at - observed_at
    if age > max_age:
        return (
            "quote is stale: observed_at="
            f"{observed_at.isoformat()} exceeds max age "
            f"{settings.monitoring_max_quote_age_seconds}s"
        )
    return None


def _price_observed_at(
    price: MonitoringPriceRequest | None,
    fallback: datetime,
) -> datetime:
    if price is None or price.observed_at is None:
        return fallback
    return _normalize_utc(price.observed_at)


def _record_monitoring_event(
    *,
    item: ExecutionPlanItem,
    event: dict[str, object],
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    events = risk_context.get("monitoring_events")
    if not isinstance(events, list):
        events = []
    risk_context["last_monitoring_event"] = event
    risk_context["monitoring_events"] = [*events[-9:], event]
    item.risk_context = risk_context


def _plan_item_exchange(item: ExecutionPlanItem) -> str:
    risk_context = item.risk_context
    if not isinstance(risk_context, dict):
        return "KRX"
    exchange = risk_context.get("exchange")
    if not isinstance(exchange, str):
        return "KRX"
    normalized = exchange.strip().upper()
    return normalized or "KRX"


def _update_monitoring_checkpoint(
    *,
    settings: Settings,
    db: Session,
    worker_name: str,
    triggered_at: datetime,
    status: str,
    details: dict[str, object],
) -> MonitoringCheckpoint:
    checkpoint = db.scalar(
        select(MonitoringCheckpoint).where(
            MonitoringCheckpoint.worker_name == worker_name,
            MonitoringCheckpoint.mode == settings.runtime_mode,
        )
    )
    if checkpoint is None:
        checkpoint = MonitoringCheckpoint(
            worker_name=worker_name,
            mode=settings.runtime_mode,
        )
        db.add(checkpoint)
    checkpoint.status = status
    checkpoint.last_heartbeat_at = triggered_at
    checkpoint.last_success_at = triggered_at
    checkpoint.details = details
    db.commit()
    return checkpoint


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _serialize_date(value: date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _place_monitoring_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    quantity: int,
    observed_price: Decimal | None,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    triggered_at: datetime,
    limit_price_override: Decimal | None = None,
    before_success_commit: Callable[[], None] | None = None,
):
    is_watch_buy = _is_watch_buy_item(item)
    limit_price = (
        limit_price_override
        or _watch_buy_limit_price(item=item, observed_price=observed_price)
        if is_watch_buy
        else limit_price_override or item.limit_price
    )
    order_request = PlaceOrderRequest(
        idempotency_key=_monitoring_order_idempotency_key(
            item=item,
            is_watch_buy=is_watch_buy,
            triggered_at=triggered_at,
            db=db,
        ),
        execution_plan_item_id=item.id,
        symbol=item.symbol,
        side="buy" if is_watch_buy else "sell",
        quantity=quantity,
        order_type="limit" if is_watch_buy or limit_price is not None else "market",
        limit_price=limit_price,
        time_in_force=item.time_in_force or "day",
        exchange=_plan_item_exchange(item),
        metadata=_monitoring_order_metadata(
            item=item,
            observed_price=observed_price,
            trigger_reason=trigger_reason,
            trigger_metadata=trigger_metadata,
        ),
    )
    return place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=order_request,
        request_source=f"monitoring:{MONITORING_WORKER_NAME}",
        before_success_commit=before_success_commit,
    )


def _monitoring_order_idempotency_key(
    *,
    item: ExecutionPlanItem,
    is_watch_buy: bool,
    triggered_at: datetime,
    db: Session,
) -> str:
    if is_watch_buy:
        return f"monitoring:watch_buy:{item.id}"
    base = f"monitoring:{item.intent}:{item.id}"
    if (
        item.intent == "stop_sell"
        and _latest_order_failure_is_retryable_session_rejection(db=db, item=item)
    ):
        retry_date = triggered_at.astimezone(KRX_TIMEZONE).date().isoformat()
        return f"{base}:{retry_date}"
    return base


def _monitoring_order_metadata(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "monitoring_worker": MONITORING_WORKER_NAME,
        "trigger_reason": trigger_reason or item.intent,
        "observed_price": _serialize_decimal(observed_price),
        "order_purpose": "protective_stop" if item.intent == "stop_sell" else item.intent,
    }
    if isinstance(item.risk_context, dict):
        for key in ("nxt_eligible", "route_now"):
            if key in item.risk_context:
                metadata[key] = item.risk_context[key]
    if trigger_metadata is not None:
        metadata["trigger"] = trigger_metadata
    return metadata


def _watch_buy_limit_price(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
) -> Decimal:
    reference_price = observed_price or _watch_buy_trigger_price(item) or item.limit_price
    if reference_price is None:
        raise ValueError("watch buy order requires an observed or trigger price")
    chase_limit_price = _watch_buy_chase_limit_price(item)
    if chase_limit_price is None:
        return reference_price
    return min(reference_price, chase_limit_price)


def _watch_buy_trigger_cross_pullback_bypasses_agent(action: PendingMonitoringAction) -> bool:
    if not _is_watch_buy_item(action.item) or action.observed_price is None:
        return False
    metadata = action.trigger_metadata
    if not isinstance(metadata, dict):
        return False
    reason = metadata.get("reason")
    if reason not in {"intraday_watch_buy_day_high_cross", "intraday_watch_buy_day_low_cross"}:
        return False
    trigger_price = _watch_buy_trigger_price(action.item)
    if trigger_price is None:
        return False
    direction = _watch_buy_direction(action.item)
    if direction == "at_or_below":
        return action.observed_price >= trigger_price
    return action.observed_price <= trigger_price


def _watch_buy_entry_line_bypasses_agent(action: PendingMonitoringAction) -> bool:
    if not _is_watch_buy_item(action.item) or action.observed_price is None:
        return False
    if _watch_buy_direction(action.item) != "at_or_below":
        return False
    trigger_price = _watch_buy_trigger_price(action.item)
    return trigger_price is not None and action.observed_price <= trigger_price


def _monitoring_result_payload(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    outcome: str,
    message: str | None = None,
    block_reason: object | None = None,
    order: object | None = None,
    trigger_reason: str | None = None,
    trigger_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "intent": item.intent,
        "status": item.status,
        "trigger_price": _serialize_decimal(item.trigger_price),
        "observed_price": _serialize_decimal(observed_price),
        "outcome": outcome,
        "source_identity": serialize_plan_item_source_identity(item),
    }
    if _is_watch_buy_item(item):
        payload["chase_limit_price"] = _serialize_decimal(_watch_buy_chase_limit_price(item))
        payload["target_price"] = _serialize_decimal(_watch_buy_target_price(item))
        payload["stop_loss_price"] = _serialize_decimal(_watch_buy_stop_price(item))
        payload["current_price"] = _serialize_decimal(observed_price)
        payload["direction"] = _watch_buy_direction(item)
        monitoring = _watch_buy_monitoring_context(item)
        tactical_action = monitoring.get("tactical_action")
        if tactical_action is None and isinstance(item.risk_context, dict):
            tactical_action = item.risk_context.get("tactical_action")
        if tactical_action is not None:
            payload["tactical_action"] = tactical_action
    if message is not None:
        payload["message"] = message
    if block_reason is not None:
        payload["block_reason"] = block_reason
    if trigger_reason is not None:
        payload["trigger_reason"] = trigger_reason
    if trigger_metadata is not None:
        payload["trigger"] = trigger_metadata
    if order is not None:
        payload["order"] = order
    return payload
