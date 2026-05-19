from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import (
    MonitoringEvaluationRequest,
    MonitoringPriceRequest,
    PlaceOrderRequest,
)
from app.broker.base import BrokerAccountSnapshot, BrokerAdapter, BrokerAdapterError
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition, MonitoringCheckpoint
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
from app.services.runtime_controls import automated_order_submission_allowed, ensure_runtime_control_state

SELL_MONITORED_INTENTS = frozenset({"target_sell", "stop_sell", "max_holding_close_sell"})
WATCH_BUY_INTENTS = frozenset({"watch_buy", "trigger_buy", "chase_buy", "tactical_buy", "next_open_buy"})
MONITORED_INTENTS = SELL_MONITORED_INTENTS | WATCH_BUY_INTENTS
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
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_expired",
        "watch_cancelled",
    }
)
MONITORING_WORKER_NAME = "intraday_target_stop_monitor"
MONITORING_RUNTIME_TOPOLOGY = "shared_symbol_scheduler"
MONITORING_SELECTION_STRATEGY = "round_robin_by_symbol"


@dataclass(frozen=True)
class EvaluateMonitoringResult:
    status_code: int
    payload: dict[str, object]


@dataclass(frozen=True)
class PendingMonitoringAction:
    item: ExecutionPlanItem
    observed_price: Decimal | None


@dataclass(frozen=True)
class WatchBuyOrderApproval:
    quantity: int | None
    reason: str | None = None
    message: str | None = None
    details: dict[str, object] | None = None


def materialize_monitoring_plan_items(plan: ExecutionPlan) -> None:
    for item in plan.items:
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
        if _monitoring_item_is_configured(item):
            item.status = _monitoring_status(item.intent)
            continue
        item.status = _pending_configuration_status(item.intent)


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
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    return _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=adapter,
        prices={price.symbol: price for price in (prices or [])},
        triggered_at=_normalize_utc(now or datetime.now(UTC)),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=False,
    )


def list_autonomous_monitoring_symbols(
    *,
    settings: Settings,
    db: Session,
) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for item in _load_monitored_items(settings=settings, db=db):
        if item.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue
        if item.symbol in seen:
            continue
        seen.add(item.symbol)
        symbols.append(item.symbol)
    return symbols


def _evaluate_monitoring_cycle(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    prices: dict[str, MonitoringPriceRequest],
    triggered_at: datetime,
    allow_persisted_triggered_without_price: bool,
    update_checkpoint_if_idle: bool,
) -> EvaluateMonitoringResult:
    items = _load_monitored_items(settings=settings, db=db)
    monitored_symbols = {item.symbol for item in items}
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
    )
    remaining_quantities_by_symbol = _load_remaining_quantities_by_symbol(
        settings=settings,
        db=db,
        symbols=sell_monitored_symbols,
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

        if not retry_persisted_trigger and not _is_triggered(item=item, observed_price=price.price if price is not None else None, triggered_at=triggered_at):
            if item.status in (
                BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES
            ):
                item.status = _monitoring_status(item.intent)
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="waiting",
                message="trigger condition not met",
            )
            continue

        if _is_watch_buy_item(item) and observed_price is not None:
            chase_limit_price = _watch_buy_chase_limit_price(item)
            if chase_limit_price is not None and observed_price > chase_limit_price:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                blocked_item_count += 1
                item.status = "watch_buy_rejected"
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "watch_buy_rejected",
                        "reason": "price_above_chase_limit",
                        "trigger_price": _serialize_decimal(_watch_buy_trigger_price(item)),
                        "chase_limit_price": _serialize_decimal(chase_limit_price),
                        "observed_price": _serialize_decimal(observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="rejected",
                    message="observed price is above chase_limit_price",
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
            PendingMonitoringAction(item=item, observed_price=observed_price)
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
        ):
            action.item.status = _monitoring_status(action.item.intent)
        is_watch_buy = _is_watch_buy_item(action.item)
        if is_watch_buy:
            action.item.status = "watch_buy_triggered"
            approval = _resolve_watch_buy_order(
                settings=settings,
                db=db,
                adapter=adapter,
                item=action.item,
                observed_price=action.observed_price,
            )
            quantity = approval.quantity
            if quantity is None:
                blocked_item_count += 1
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
                continue
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
                continue

        if dispatch_action_count >= max_broker_actions:
            symbol_actions.appendleft(action)
            pending_symbols.appendleft(symbol)
            break
        else:
            dispatch_action_count += 1
            placement = _place_monitoring_order(
                settings=settings,
                db=db,
                adapter=adapter,
                item=action.item,
                quantity=quantity,
                observed_price=action.observed_price,
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
                else:
                    _reserve_remaining_quantity(
                        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                        symbol=action.item.symbol,
                        quantity=quantity,
                    )
                submitted_order_count += 1
                submitted_symbols.add(action.item.symbol)
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="submitted",
                    order=placement.payload.get("order"),
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
    }

    checkpoint = None
    if items or update_checkpoint_if_idle:
        checkpoint = _update_monitoring_checkpoint(
            settings=settings,
            db=db,
            worker_name=MONITORING_WORKER_NAME,
            triggered_at=triggered_at,
            status="degraded" if deferred_item_count > 0 else "running",
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
            "items": results,
        },
    )


def _load_monitored_items(
    *,
    settings: Settings,
    db: Session,
) -> list[ExecutionPlanItem]:
    monitored_item_filter = _monitored_item_filter()
    symbol_candidates = (
        select(
            ExecutionPlanItem.symbol.label("symbol"),
            ExecutionPlan.accepted_at.label("accepted_at"),
            ExecutionPlan.id.label("plan_id"),
            ExecutionPlanItem.plan_item_index.label("plan_item_index"),
            ExecutionPlanItem.id.label("item_id"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlan.accepted_at.asc(),
                    ExecutionPlan.id.asc(),
                    ExecutionPlanItem.plan_item_index.asc(),
                    ExecutionPlanItem.id.asc(),
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
    # Bound the working set by distinct symbol before loading per-symbol items.
    selected_symbols = (
        select(symbol_candidates.c.symbol)
        .where(symbol_candidates.c.symbol_rank == 1)
        .order_by(
            symbol_candidates.c.accepted_at.asc(),
            symbol_candidates.c.plan_id.asc(),
            symbol_candidates.c.plan_item_index.asc(),
            symbol_candidates.c.item_id.asc(),
        )
        .limit(settings.monitoring_max_symbols_per_cycle)
    )
    return db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_item_filter,
            ExecutionPlanItem.symbol.in_(selected_symbols),
        )
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()


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


def _load_remaining_quantities_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
) -> dict[str, int]:
    where = [ExecutionPosition.mode == settings.runtime_mode]
    if symbols:
        where.append(ExecutionPosition.symbol.in_(sorted(symbols)))
    positions = db.scalars(
        select(ExecutionPosition)
        .where(*where)
        .order_by(ExecutionPosition.symbol.asc())
    ).all()
    return {
        position.symbol: position.quantity
        for position in positions
        if position.quantity > 0
    }


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
) -> None:
    if not symbols:
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

    stale_symbols = _positions_with_stale_local_state(
        settings=settings,
        db=db,
        symbols=symbols,
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
    for symbol in symbols:
        position = known.get(symbol)
        if position is None or position.quantity <= 0:
            stale.add(symbol)
    return stale


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
    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = quantity_policy.get("quantity")
        return _parse_positive_int(quantity) is not None
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        return fraction is not None
    return False


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

    quantity = _parse_buy_quantity(item.quantity_policy)
    if quantity is None and isinstance(decision, dict):
        quantity = _parse_positive_int(
            decision.get("approved_quantity") or decision.get("quantity")
        )

    control_state = ensure_runtime_control_state(settings=settings, db=db)
    if not automated_order_submission_allowed(control_state):
        if quantity is not None:
            return WatchBuyOrderApproval(quantity=quantity)
        return WatchBuyOrderApproval(
            quantity=None,
            reason="missing_quantity",
            message="watch buy requires a positive share quantity while runtime is blocked",
        )

    entry_price = observed_price or _watch_buy_trigger_price(item) or item.limit_price
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
    risk_decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=available_cash,
        available_cash=available_cash,
        entry_price=entry_price,
        stop_price=effective_stop,
        confidence=_watch_buy_confidence(item),
        requested_quantity=quantity,
        candidate_max_budget=_watch_buy_candidate_budget(item),
        agent_max_quantity=_watch_buy_max_quantity(item),
        existing_symbol_exposure=existing_exposure,
        daily_new_buy_notional_used=_load_daily_new_buy_notional(
            settings=settings,
            db=db,
            market_date=item.plan.market_date,
        ),
    )
    if isinstance(risk_decision, RiskSizingRejection):
        return WatchBuyOrderApproval(
            quantity=None,
            reason=risk_decision.reason_code,
            message=risk_decision.message,
            details=risk_decision.details,
        )
    assert isinstance(risk_decision, RiskSizingResult)
    _record_monitoring_event(
        item=item,
        event={
            "event": "watch_buy_risk_sized",
            "quantity": risk_decision.quantity,
            "approved_notional": _serialize_decimal(risk_decision.approved_notional),
            "risk_pct": _serialize_decimal(risk_decision.risk_pct),
            "risk_budget": _serialize_decimal(risk_decision.risk_budget),
            "stop_price": _serialize_decimal(effective_stop),
            "upstream_stop_price": _serialize_decimal(stop_price),
            "risk_based_quantity": risk_decision.risk_quantity,
            "allocation_quantity": risk_decision.allocation_quantity,
            "cash_quantity": risk_decision.cash_quantity,
            "recorded_at": datetime.now(UTC).isoformat(),
        },
    )
    return WatchBuyOrderApproval(quantity=risk_decision.quantity)


def _parse_buy_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict):
        return None
    if quantity_policy.get("kind") != "shares":
        return None
    return _parse_positive_int(quantity_policy.get("quantity"))


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
            "notional",
        ):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    return None


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
                    "retry_pending",
                )
            ),
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
        ),
    )


def _watch_buy_chase_limit_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        ("chase_limit_price", "max_chase_limit_price", "entry_limit_price"),
    )


def _watch_buy_direction(item: ExecutionPlanItem) -> str:
    for source in _item_payload_sources(item):
        for key in ("trigger_direction", "entry_direction", "watch_direction", "direction"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                normalized = value.strip().lower()
                if normalized in {"down", "below", "pullback", "dip", "<=", "at_or_below"}:
                    return "at_or_below"
    monitoring = _watch_buy_monitoring_context(item)
    direction = monitoring.get("direction")
    if isinstance(direction, str) and direction == "at_or_below":
        return "at_or_below"
    return "at_or_above"


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
    db.refresh(checkpoint)
    return checkpoint


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


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
):
    is_watch_buy = _is_watch_buy_item(item)
    limit_price = (
        _watch_buy_limit_price(item=item, observed_price=observed_price)
        if is_watch_buy
        else item.limit_price
    )
    order_request = PlaceOrderRequest(
        idempotency_key=(
            f"monitoring:watch_buy:{item.id}"
            if is_watch_buy
            else f"monitoring:{item.intent}:{item.id}"
        ),
        execution_plan_item_id=item.id,
        symbol=item.symbol,
        side="buy" if is_watch_buy else "sell",
        quantity=quantity,
        order_type="limit" if is_watch_buy or limit_price is not None else "market",
        limit_price=limit_price,
        time_in_force=item.time_in_force or "day",
        exchange=_plan_item_exchange(item),
    )
    return place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=order_request,
        request_source=f"monitoring:{MONITORING_WORKER_NAME}",
    )


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


def _monitoring_result_payload(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    outcome: str,
    message: str | None = None,
    block_reason: object | None = None,
    order: object | None = None,
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
        payload["direction"] = _watch_buy_direction(item)
    if message is not None:
        payload["message"] = message
    if block_reason is not None:
        payload["block_reason"] = block_reason
    if order is not None:
        payload["order"] = order
    return payload
