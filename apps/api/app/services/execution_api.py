from dataclasses import dataclass
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import ExecutionPlanItemRequest, ExecutionPlanRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    new_id,
)
from app.services.fill_economics import execution_fill_economics
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.exec_agent import apply_short_term_exec_agent_decisions
from app.services.plan_item_identity import (
    serialize_plan_item_source_identity,
    serialize_symbol_source_identities,
)

MONITORED_INTENTS = frozenset({"target_sell", "stop_sell"})
WATCH_BUY_INTENTS = frozenset({"watch_buy", "trigger_buy", "chase_buy", "tactical_buy", "intraday_watch_buy"})
WATCH_MONITORING_STATUSES = frozenset(
    {
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "watch_buy_triggered",
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_expired",
        "watch_cancelled",
    }
)
AGENT_RUNTIME_MONITORING_STATUSES = frozenset(
    {
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
    }
)
OPEN_ORDER_STATUSES = frozenset({"submitted", "open", "partially_filled"})
SHORT_TERM_DECIDER_NAME = "short-term-decider"
ACTIVE_SHORT_TERM_TACTICAL_ACTIONS = frozenset({"scalp_buy_candidate", "watch_for_trigger"})
INACTIVE_SHORT_TERM_TACTICAL_ACTIONS = frozenset({"avoid_chase", "wait", "reject"})
UNSUPPORTED_BUY_INTENTS = frozenset({"tactical_buy", "trigger_buy", "chase_buy"})
CHASE_BUY_FIELDS = frozenset(
    {
        "chase_limit_price",
        "chase_limit_offset",
        "chase_limit_percent",
        "chase_limit_ticks",
        "max_chase_limit_price",
        "entry_limit_price",
    }
)
BUY_RECOMMENDATION_FIELDS = (
    "buy_recommendation_price",
    "recommended_entry_price",
    "entry_recommendation_price",
    "recommendation_price",
    "entry_price",
)
BUY_TRIGGER_FIELDS = frozenset({"watch_trigger_price", "buy_trigger_price", "entry_trigger_price", "trigger_price", "original_watch_trigger_price"})
BUY_TARGET_FIELDS = frozenset({"target_price", "take_profit_price", "first_take_profit_price", "profit_target_price", "target"})
BUY_STOP_FIELDS = frozenset({"stop_price", "stop_sell_price", "stop_loss_price", "risk_stop_price", "stop"})


@dataclass(frozen=True)
class CreateExecutionPlanResult:
    status_code: int
    payload: dict[str, object]


def create_execution_plan(
    *,
    settings: Settings,
    db: Session,
    command: ExecutionPlanRequest,
    adapter: BrokerAdapter | None = None,
) -> CreateExecutionPlanResult:
    plan = _build_execution_plan(settings=settings, command=command)
    from app.services.execution_jobs import materialize_execution_jobs
    from app.services.monitoring import materialize_monitoring_plan_items

    exec_agent_result = apply_short_term_exec_agent_decisions(
        settings=settings,
        db=db,
        adapter=adapter,
        plan=plan,
    )
    materialize_execution_jobs(plan)
    materialize_monitoring_plan_items(plan, db=db)
    db.add(plan)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return CreateExecutionPlanResult(
            status_code=409,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "plan_id",
                        "message": "execution plan with this plan_id already exists",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {
                    "mode": settings.runtime_mode,
                },
            },
        )

    return CreateExecutionPlanResult(
        status_code=201,
        payload={
            "plan_id": plan.external_plan_id,
            "execution_plan_id": plan.id,
            "accepted_item_count": len(plan.items),
            "rejected_item_count": 0,
            "validation_failures": [],
            "persisted_execution_item_ids": [item.id for item in plan.items],
            "exec_agent": exec_agent_result.payload(),
            "runtime": {
                "mode": settings.runtime_mode,
            },
        },
    )


def preview_execution_plan(
    *,
    settings: Settings,
    db: Session,
    command: ExecutionPlanRequest,
    adapter: BrokerAdapter | None = None,
) -> CreateExecutionPlanResult:
    plan = _build_execution_plan(settings=settings, command=command)
    from app.services.execution_jobs import materialize_execution_jobs, serialize_execution_job
    from app.services.monitoring import materialize_monitoring_plan_items

    exec_agent_result = apply_short_term_exec_agent_decisions(
        settings=settings,
        db=db,
        adapter=adapter,
        plan=plan,
    )
    materialize_execution_jobs(plan)
    materialize_monitoring_plan_items(plan)
    return CreateExecutionPlanResult(
        status_code=200,
        payload={
            "plan_id": plan.external_plan_id,
            "execution_plan_id": plan.id,
            "dry_run": True,
            "accepted_item_count": len(plan.items),
            "rejected_item_count": 0,
            "validation_failures": [],
            "persisted_execution_item_ids": [],
            "items": [_serialize_execution_plan_item(item) for item in plan.items],
            "execution_jobs": [
                serialize_execution_job(job)
                for item in plan.items
                for job in item.jobs
            ],
            "exec_agent": exec_agent_result.payload(),
            "runtime": {
                "mode": settings.runtime_mode,
            },
        },
    )


def _build_execution_plan(
    *,
    settings: Settings,
    command: ExecutionPlanRequest,
) -> ExecutionPlan:
    plan = ExecutionPlan(
        id=new_id(),
        external_plan_id=command.plan_id,
        source=command.source,
        market_date=command.market_date,
        strategy_run_id=command.strategy_run_id,
        mode=settings.runtime_mode,
        status="accepted",
        requested_at=command.requested_at,
        raw_payload=command.model_dump(mode="json"),
        items=[
            _build_execution_plan_item(index=index, item=item)
            for index, item in enumerate(command.items)
        ],
    )
    _normalize_watch_buy_handoff_items(plan)
    return plan


def _build_execution_plan_item(
    *,
    index: int,
    item: ExecutionPlanItemRequest,
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=new_id(),
        plan_item_index=index,
        symbol=item.symbol,
        side=item.side,
        intent=item.intent,
        status=_initial_plan_item_status(item),
        quantity_policy=item.quantity_policy,
        provider=_plan_item_text(item=item, key="provider"),
        venue=_plan_item_text(item=item, key="venue"),
        asset_class=_plan_item_text(item=item, key="asset_class"),
        base_quantity=item.base_quantity,
        quote_notional=item.quote_notional,
        quantity_precision=item.quantity_precision,
        price_precision=item.price_precision,
        limit_price=item.limit_price,
        trigger_price=_plan_item_trigger_price(item),
        time_in_force=item.time_in_force,
        risk_context=_plan_item_risk_context(item),
        decision_agent_name=item.decision_agent_name,
        decision_snapshot_id=item.decision_snapshot_id,
        decision_run_id=item.decision_run_id,
        source_plan_item_id=item.source_plan_item_id,
        source_market_date=item.source_market_date,
    )


def list_execution_plans(
    *,
    settings: Settings,
    db: Session,
    external_plan_id: str | None = None,
) -> list[dict[str, object]]:
    query = (
        select(ExecutionPlan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionPlan.items))
        .order_by(ExecutionPlan.accepted_at.desc(), ExecutionPlan.created_at.desc())
    )
    if external_plan_id:
        query = query.where(ExecutionPlan.external_plan_id == external_plan_id)
    plans = db.scalars(query).all()
    return [_serialize_execution_plan(plan) for plan in plans]


def list_broker_orders(
    *,
    settings: Settings,
    db: Session,
    external_plan_id: str | None = None,
    symbols: set[str] | None = None,
) -> list[dict[str, object]]:
    query = (
        select(BrokerOrder)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .options(
            selectinload(BrokerOrder.events),
            selectinload(BrokerOrder.plan_item).selectinload(ExecutionPlanItem.plan),
        )
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
    )
    if symbols:
        query = query.where(BrokerOrder.symbol.in_(symbols))
    if external_plan_id:
        query = (
            query.join(BrokerOrder.plan_item)
            .join(ExecutionPlanItem.plan)
            .where(ExecutionPlan.external_plan_id == external_plan_id)
        )
    orders = db.scalars(query).all()
    return [serialize_broker_order(order) for order in orders]


def list_execution_fills(
    *,
    settings: Settings,
    db: Session,
    external_plan_id: str | None = None,
    symbols: set[str] | None = None,
) -> list[dict[str, object]]:
    query = (
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .options(
            selectinload(ExecutionFill.broker_order)
            .selectinload(BrokerOrder.plan_item)
            .selectinload(ExecutionPlanItem.plan),
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
    )
    if symbols:
        query = query.where(ExecutionFill.symbol.in_(symbols))
    if external_plan_id:
        query = (
            query.join(BrokerOrder.plan_item)
            .join(ExecutionPlanItem.plan)
            .where(ExecutionPlan.external_plan_id == external_plan_id)
        )
    fills = db.scalars(query).all()
    return [_serialize_execution_fill(fill) for fill in fills]


def list_execution_positions(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
    include_related_plan_items: bool = True,
) -> list[dict[str, object]]:
    query = (
        select(ExecutionPosition)
        .where(ExecutionPosition.mode == settings.runtime_mode)
        .order_by(ExecutionPosition.symbol.asc())
    )
    if symbols:
        query = query.where(ExecutionPosition.symbol.in_(symbols))
    positions = db.scalars(query).all()
    plan_items_by_symbol = (
        _load_plan_items_by_symbol(
            settings=settings,
            db=db,
            symbols={position.symbol for position in positions},
        )
        if include_related_plan_items
        else {}
    )
    return [
        _serialize_execution_position(
            position,
            related_plan_items=plan_items_by_symbol.get(position.symbol, []),
        )
        for position in positions
    ]


def list_runtime_symbols(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
) -> list[dict[str, object]]:
    if symbols is not None and not symbols:
        return []
    state = _load_runtime_symbol_state(settings=settings, db=db, symbols=symbols)
    return [
        _serialize_runtime_symbol_summary(
            settings=settings,
            symbol=symbol,
            plan_items=state["plan_items_by_symbol"].get(symbol, []),
            orders=state["orders_by_symbol"].get(symbol, []),
            fills=state["fills_by_symbol"].get(symbol, []),
            position=state["positions_by_symbol"].get(symbol),
        )
        for symbol in state["symbols"]
    ]


def get_runtime_symbol_detail(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> dict[str, object] | None:
    normalized_symbol = _normalize_symbol(symbol)
    state = _load_runtime_symbol_state(
        settings=settings,
        db=db,
        symbol=normalized_symbol,
    )
    if normalized_symbol not in state["symbols"]:
        return None

    plan_items = state["plan_items_by_symbol"].get(normalized_symbol, [])
    orders = state["orders_by_symbol"].get(normalized_symbol, [])
    fills = state["fills_by_symbol"].get(normalized_symbol, [])
    position = state["positions_by_symbol"].get(normalized_symbol)
    summary = _serialize_runtime_symbol_summary(
        settings=settings,
        symbol=normalized_symbol,
        plan_items=plan_items,
        orders=orders,
        fills=fills,
        position=position,
    )
    monitoring_statuses = sorted(
        {
            item.status
            for item in plan_items
            if _is_runtime_monitored_item(item)
        }
    )
    return {
        "symbol": normalized_symbol,
        "mode": settings.runtime_mode,
        "summary": summary,
        "monitoring": {
            "monitored": any(_is_runtime_monitored_item(item) for item in plan_items),
            "statuses": monitoring_statuses,
            "pending_dispatch_count": sum(
                1
                for item in plan_items
                if item.status.endswith("_pending_dispatch")
            ),
        },
        "position": (
            _serialize_execution_position(position, related_plan_items=plan_items)
            if position is not None
            else None
        ),
        "plan_items": [
            _serialize_execution_plan_item_detail(item)
            for item in _sort_plan_items_desc(plan_items)
        ],
        "orders": [
            serialize_broker_order(order, include_events=True)
            for order in orders
        ],
        "fills": [
            _serialize_execution_fill(fill)
            for fill in fills
        ],
    }


def _serialize_execution_plan(plan: ExecutionPlan) -> dict[str, object]:
    items = sorted(
        plan.items,
        key=lambda item: (
            item.plan_item_index,
            item.created_at,
            item.id,
        ),
    )
    return {
        "execution_plan_id": plan.id,
        "plan_id": plan.external_plan_id,
        "source": plan.source,
        "market_date": _serialize_date(plan.market_date),
        "strategy_run_id": plan.strategy_run_id,
        "mode": plan.mode,
        "status": plan.status,
        "requested_at": _serialize_datetime(plan.requested_at),
        "accepted_at": _serialize_datetime(plan.accepted_at),
        "created_at": _serialize_datetime(plan.created_at),
        "updated_at": _serialize_datetime(plan.updated_at),
        "item_count": len(items),
        "items": [_serialize_execution_plan_item(item) for item in items],
    }


def _serialize_execution_plan_item(item: ExecutionPlanItem) -> dict[str, object]:
    source_identity = serialize_plan_item_source_identity(item)
    tactical_action = _risk_context_value(item, "tactical_action")
    blocker_reason = _risk_context_value(item, "short_term_blocker_reason")
    return {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "side": item.side,
        "intent": item.intent,
        "status": item.status,
        "quantity_policy": item.quantity_policy,
        "provider": item.provider,
        "venue": item.venue,
        "asset_class": item.asset_class,
        "base_quantity": _serialize_decimal(item.base_quantity),
        "quote_notional": _serialize_decimal(item.quote_notional),
        "quantity_precision": item.quantity_precision,
        "price_precision": item.price_precision,
        "limit_price": _serialize_decimal(item.limit_price),
        "trigger_price": _serialize_decimal(item.trigger_price),
        "time_in_force": item.time_in_force,
        "risk_context": item.risk_context,
        "decision_agent_name": source_identity["decision_agent_name"],
        "agent_name": source_identity["agent_name"],
        "decision_snapshot_id": item.decision_snapshot_id,
        "decision_run_id": source_identity["decision_run_id"],
        "source_plan_item_id": source_identity["source_plan_item_id"],
        "core_plan_item_id": source_identity["core_plan_item_id"],
        "source_market_date": source_identity["source_market_date"],
        "source_identity": source_identity,
        "tactical_action": tactical_action,
        "blocker_reason": blocker_reason,
        "created_at": _serialize_datetime(item.created_at),
        "updated_at": _serialize_datetime(item.updated_at),
    }


def _serialize_execution_plan_item_detail(item: ExecutionPlanItem) -> dict[str, object]:
    payload = _serialize_execution_plan_item(item)
    payload.update(
        {
            "execution_plan_id": item.execution_plan_id,
            "plan_id": item.plan.external_plan_id,
            "plan_source": item.plan.source,
            "plan_market_date": _serialize_date(item.plan.market_date),
            "plan_requested_at": _serialize_datetime(item.plan.requested_at),
            "plan_accepted_at": _serialize_datetime(item.plan.accepted_at),
            "plan_status": item.plan.status,
        }
    )
    return payload


def serialize_broker_order(
    order: BrokerOrder,
    *,
    include_events: bool = False,
) -> dict[str, object]:
    source_identity = (
        serialize_plan_item_source_identity(order.plan_item)
        if order.plan_item is not None
        else None
    )
    payload = {
        "broker_order_pk": order.id,
        "execution_plan_item_id": order.execution_plan_item_id,
        "broker_name": order.broker_name,
        "idempotency_key": order.idempotency_key,
        "broker_order_id": order.broker_order_id,
        "symbol": order.symbol,
        "side": order.side,
        "status": order.status,
        "quantity": order.quantity,
        "base_quantity": _serialize_decimal(
            order.base_quantity,
            precision=order.quantity_precision,
        ),
        "quote_notional": _serialize_decimal(order.quote_notional),
        "quantity_precision": order.quantity_precision,
        "price_precision": order.price_precision,
        "venue": order.venue,
        "asset_class": order.asset_class,
        "limit_price": _serialize_decimal(order.limit_price),
        "submitted_at": _serialize_datetime(order.submitted_at),
        "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
        "request_metadata": order.request_metadata,
        "response_metadata": order.response_metadata,
        "last_error_classification": order.last_error_classification,
        "last_error_code": order.last_error_code,
        "last_error_message": order.last_error_message,
        "last_error_retryable": order.last_error_retryable,
        "raw_broker_payload": order.raw_broker_payload,
        "source_identity": source_identity,
        "created_at": _serialize_datetime(order.created_at),
        "updated_at": _serialize_datetime(order.updated_at),
        "event_count": len(order.events),
    }
    if include_events:
        payload["events"] = [
            serialize_broker_order_event(event)
            for event in sorted(
                order.events,
                key=lambda item: (
                    _serialize_datetime(item.observed_at) or "",
                    _serialize_datetime(item.created_at) or "",
                    item.id,
                ),
            )
        ]
    return payload


def serialize_broker_order_event(event) -> dict[str, object]:
    return {
        "broker_order_event_id": event.id,
        "event_type": event.event_type,
        "event_source": event.event_source,
        "normalized_status": event.normalized_status,
        "broker_status": event.broker_status,
        "error_classification": event.error_classification,
        "retryable": event.retryable,
        "message": event.message,
        "request_payload": event.request_payload,
        "response_payload": event.response_payload,
        "payload": event.payload,
        "observed_at": _serialize_datetime(event.observed_at),
        "created_at": _serialize_datetime(event.created_at),
        "updated_at": _serialize_datetime(event.updated_at),
    }


def _serialize_execution_fill(fill: ExecutionFill) -> dict[str, object]:
    source_identity = None
    if fill.broker_order.plan_item is not None:
        source_identity = serialize_plan_item_source_identity(fill.broker_order.plan_item)
    economics = execution_fill_economics(fill)
    return {
        "execution_fill_id": fill.id,
        "broker_order_pk": fill.broker_order_pk,
        "broker_fill_id": fill.broker_fill_id,
        "symbol": fill.symbol,
        "side": fill.side,
        "quantity": fill.quantity,
        "base_quantity": _serialize_decimal(fill.base_quantity),
        "quote_notional": _serialize_decimal(fill.quote_notional),
        "price": _serialize_decimal(fill.price),
        "settlement_currency": economics.settlement_currency,
        "settlement_currency_conflict": economics.settlement_currency_conflict,
        "transaction_cost": _serialize_decimal(economics.transaction_cost),
        "transaction_cost_coverage": economics.transaction_cost_coverage,
        "transaction_cost_source": economics.transaction_cost_source,
        "filled_at": _serialize_datetime(fill.filled_at),
        "payload": fill.payload,
        "source_identity": source_identity,
        "created_at": _serialize_datetime(fill.created_at),
        "updated_at": _serialize_datetime(fill.updated_at),
    }


def _serialize_execution_position(
    position: ExecutionPosition,
    *,
    related_plan_items: list[ExecutionPlanItem] | None = None,
) -> dict[str, object]:
    return {
        "execution_position_id": position.id,
        "symbol": position.symbol,
        "quantity": position.quantity,
        "base_quantity": _serialize_decimal(position.base_quantity),
        "average_price": _serialize_decimal(position.average_price),
        "updated_from": position.updated_from,
        "last_reconciled_at": _serialize_datetime(position.last_reconciled_at),
        "raw_payload": position.raw_payload,
        "source_identities": serialize_symbol_source_identities(related_plan_items or []),
        "created_at": _serialize_datetime(position.created_at),
        "updated_at": _serialize_datetime(position.updated_at),
    }


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_decimal(
    value: Decimal | None,
    *,
    precision: int | None = None,
) -> str | None:
    if value is None:
        return None
    if precision is not None:
        return format(value, f".{precision}f")
    return str(value)


def _risk_context_value(item: ExecutionPlanItem, key: str) -> object | None:
    if not isinstance(item.risk_context, dict):
        return None
    return item.risk_context.get(key)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _load_runtime_symbol_state(
    *,
    settings: Settings,
    db: Session,
    symbol: str | None = None,
    symbols: set[str] | None = None,
) -> dict[str, object]:
    plan_items_query = (
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.created_at.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    )
    orders_query = (
        select(BrokerOrder)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .options(
            selectinload(BrokerOrder.events),
            selectinload(BrokerOrder.plan_item).selectinload(ExecutionPlanItem.plan),
        )
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
    )
    fills_query = (
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .options(
            selectinload(ExecutionFill.broker_order)
            .selectinload(BrokerOrder.plan_item)
            .selectinload(ExecutionPlanItem.plan),
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
    )
    positions_query = (
        select(ExecutionPosition)
        .where(ExecutionPosition.mode == settings.runtime_mode)
        .order_by(ExecutionPosition.symbol.asc())
    )
    symbol_filter = {_normalize_symbol(symbol)} if symbol is not None else symbols
    if symbol_filter is not None:
        normalized_symbols = sorted(_normalize_symbol(item) for item in symbol_filter)
        plan_items_query = plan_items_query.where(ExecutionPlanItem.symbol.in_(normalized_symbols))
        orders_query = orders_query.where(BrokerOrder.symbol.in_(normalized_symbols))
        fills_query = fills_query.where(ExecutionFill.symbol.in_(normalized_symbols))
        positions_query = positions_query.where(ExecutionPosition.symbol.in_(normalized_symbols))

    plan_items = db.scalars(plan_items_query).all()
    orders = db.scalars(orders_query).all()
    fills = dedupe_execution_fills(db.scalars(fills_query).all())
    positions = db.scalars(positions_query).all()

    plan_items_by_symbol: dict[str, list[ExecutionPlanItem]] = defaultdict(list)
    orders_by_symbol: dict[str, list[BrokerOrder]] = defaultdict(list)
    fills_by_symbol: dict[str, list[ExecutionFill]] = defaultdict(list)
    positions_by_symbol: dict[str, ExecutionPosition] = {}
    symbols: set[str] = set()

    for item in plan_items:
        plan_items_by_symbol[item.symbol].append(item)
        symbols.add(item.symbol)
    for order in orders:
        orders_by_symbol[order.symbol].append(order)
        symbols.add(order.symbol)
    for fill in fills:
        fills_by_symbol[fill.symbol].append(fill)
        symbols.add(fill.symbol)
    for position in positions:
        positions_by_symbol[position.symbol] = position
        symbols.add(position.symbol)

    return {
        "symbols": sorted(symbols),
        "plan_items_by_symbol": plan_items_by_symbol,
        "orders_by_symbol": orders_by_symbol,
        "fills_by_symbol": fills_by_symbol,
        "positions_by_symbol": positions_by_symbol,
    }


def _serialize_runtime_symbol_summary(
    *,
    settings: Settings,
    symbol: str,
    plan_items: list[ExecutionPlanItem],
    orders: list[BrokerOrder],
    fills: list[ExecutionFill],
    position: ExecutionPosition | None,
) -> dict[str, object]:
    monitoring_statuses = sorted(
        {
            item.status
            for item in plan_items
            if _is_runtime_monitored_item(item)
        }
    )
    latest_plan_item = _sort_plan_items_desc(plan_items)[0] if plan_items else None
    latest_activity_at = _serialize_datetime(
        _latest_datetime(
            [
                item.updated_at for item in plan_items
            ]
            + [
                order.updated_at for order in orders
            ]
            + [
                fill.filled_at for fill in fills
            ]
            + [
                position.updated_at
                for position in [position]
                if position is not None
            ]
        )
    )
    latest_order_error = _latest_order_reconciliation_error(orders)
    return {
        "symbol": symbol,
        "mode": settings.runtime_mode,
        "asset_classes": sorted(
            {item.asset_class or "stock" for item in plan_items}
            | {order.asset_class or "stock" for order in orders}
        ),
        "providers": sorted(
            {item.provider or "kis" for item in plan_items}
            | {order.broker_name for order in orders}
        ),
        "venues": sorted(
            value
            for value in ({item.venue for item in plan_items} | {order.venue for order in orders})
            if value is not None
        ),
        "monitored": any(_is_runtime_monitored_item(item) for item in plan_items),
        "monitoring_statuses": monitoring_statuses,
        "intents": sorted({item.intent for item in plan_items}),
        "plan_item_count": len(plan_items),
        "latest_plan_item_status": latest_plan_item.status if latest_plan_item is not None else None,
        "order_count": len(orders),
        "open_order_count": sum(order.status in OPEN_ORDER_STATUSES for order in orders),
        "fill_count": len(fills),
        "filled_quantity": sum(fill.quantity for fill in fills),
        "filled_base_quantity": _serialize_decimal(
            sum((fill.base_quantity or Decimal("0")) for fill in fills)
        ),
        "filled_quote_notional": _serialize_decimal(
            sum((fill.quote_notional or Decimal("0")) for fill in fills)
        ),
        "position_quantity": position.quantity if position is not None else 0,
        "position_base_quantity": (
            _serialize_decimal(position.base_quantity)
            if position is not None
            else None
        ),
        "position_average_price": (
            _serialize_decimal(position.average_price)
            if position is not None
            else None
        ),
        "last_reconciliation_error": latest_order_error,
        "latest_activity_at": latest_activity_at,
    }


def _latest_order_reconciliation_error(orders: list[BrokerOrder]) -> dict[str, object] | None:
    errored_orders = [
        order
        for order in orders
        if order.last_error_message and order.last_broker_sync_at is not None
    ]
    if not errored_orders:
        return None
    latest = max(
        errored_orders,
        key=lambda order: (order.last_broker_sync_at or order.updated_at, order.updated_at),
    )
    return {
        "broker_order_id": latest.broker_order_id,
        "status": latest.status,
        "classification": latest.last_error_classification,
        "code": latest.last_error_code,
        "message": latest.last_error_message,
        "retryable": latest.last_error_retryable,
        "last_broker_sync_at": _serialize_datetime(latest.last_broker_sync_at),
    }


def _sort_plan_items_desc(plan_items: list[ExecutionPlanItem]) -> list[ExecutionPlanItem]:
    return sorted(
        plan_items,
        key=lambda item: (
            item.plan.accepted_at,
            item.plan.created_at,
            item.plan_item_index,
            item.id,
        ),
        reverse=True,
    )


def _latest_datetime(values: list[datetime | None]) -> datetime | None:
    concrete_values = [value for value in values if value is not None]
    if not concrete_values:
        return None
    return max(concrete_values)


def _load_plan_items_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
) -> dict[str, list[ExecutionPlanItem]]:
    if not symbols:
        return {}
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(sorted(symbols)),
        )
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.created_at.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    plan_items_by_symbol: dict[str, list[ExecutionPlanItem]] = defaultdict(list)
    for item in items:
        plan_items_by_symbol[item.symbol].append(item)
    return plan_items_by_symbol


def _initial_plan_item_status(item: ExecutionPlanItemRequest) -> str:
    if item.side != "buy":
        return "pending"
    tactical_action = _short_term_tactical_action(item)
    if _decision_agent_name(item) == SHORT_TERM_DECIDER_NAME and tactical_action in INACTIVE_SHORT_TERM_TACTICAL_ACTIONS:
        return "short_term_watch_blocked"
    if _is_watch_buy_request(item):
        if _watch_buy_trigger_price(item) is not None:
            return "watch_monitoring_pending"
        return "pending_configuration"
    if item.intent == "next_open_buy":
        if item.trigger_price is not None or _item_has_buy_chase_or_trigger_fields(item):
            return "pending_configuration"
        if _decision_agent_name(item) == SHORT_TERM_DECIDER_NAME:
            if _short_term_exec_agent_sizing_requested(item.quantity_policy):
                return "pending"
            return "short_term_watcher_pending"
        return "pending"
    if item.intent in UNSUPPORTED_BUY_INTENTS:
        return "unsupported_semantics"
    if _decision_agent_name(item) == SHORT_TERM_DECIDER_NAME:
        return "unsupported_semantics"
    return "pending"


def _normalize_watch_buy_handoff_items(plan: ExecutionPlan) -> None:
    plan_items_by_symbol: dict[str, list[ExecutionPlanItem]] = defaultdict(list)
    for item in plan.items:
        plan_items_by_symbol[item.symbol].append(item)

    for item in plan.items:
        if not _is_watch_buy_plan_item(item):
            continue

        quantity_policy = dict(item.quantity_policy or {})
        quantity = _watch_buy_handoff_quantity(quantity_policy)
        if quantity is not None:
            original_kind = quantity_policy.get("kind") or quantity_policy.get("type")
            quantity_policy["kind"] = "shares"
            quantity_policy["quantity"] = quantity
            quantity_policy.setdefault("source", "watch_buy_handoff_normalizer")
            if original_kind is not None and original_kind != "shares":
                quantity_policy.setdefault("original_policy_type", original_kind)

        entry_price = _first_decimal_from_payloads(
            (quantity_policy, item.risk_context),
            ("entry_price", "recommended_entry_price", "reference_price", "estimated_reference_price"),
        ) or item.trigger_price or item.limit_price
        if entry_price is not None:
            quantity_policy.setdefault("entry_price", _decimal_text(entry_price))
            quantity_policy.setdefault("recommended_entry_price", _decimal_text(entry_price))

        target_price = _first_decimal_from_payloads(
            (quantity_policy, item.risk_context),
            tuple(BUY_TARGET_FIELDS),
        ) or _sibling_trigger_price(plan_items_by_symbol[item.symbol], "target_sell")
        if target_price is not None:
            quantity_policy.setdefault("target_price", _decimal_text(target_price))

        stop_price = _first_decimal_from_payloads(
            (quantity_policy, item.risk_context),
            tuple(BUY_STOP_FIELDS),
        ) or _sibling_trigger_price(plan_items_by_symbol[item.symbol], "stop_sell")
        if stop_price is not None:
            quantity_policy.setdefault("stop_price", _decimal_text(stop_price))
            quantity_policy.setdefault("stop_loss_price", _decimal_text(stop_price))

        item.quantity_policy = quantity_policy or item.quantity_policy
        if (
            item.status in {"pending", "pending_configuration", "next_open_buy_pending_configuration"}
            and item.side == "buy"
            and item.trigger_price is not None
            and _watch_buy_handoff_quantity(item.quantity_policy) is not None
        ):
            item.status = "watch_monitoring_pending"


def _is_watch_buy_plan_item(item: ExecutionPlanItem) -> bool:
    return item.side == "buy" and (
        item.intent in WATCH_BUY_INTENTS
        or item.intent == "next_open_buy"
        or item.status in WATCH_MONITORING_STATUSES
    )


def _watch_buy_handoff_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict):
        return None
    kind = quantity_policy.get("kind") or quantity_policy.get("type")
    if kind in {"shares", "fixed_quantity"}:
        return _parse_positive_int(
            quantity_policy.get("quantity")
            or quantity_policy.get("planned_quantity")
            or quantity_policy.get("computed_quantity")
        )
    if kind == "exec_agent_sized":
        explicit_quantity = _parse_positive_int(
            quantity_policy.get("quantity")
            or quantity_policy.get("planned_quantity")
            or quantity_policy.get("computed_quantity")
        )
        if explicit_quantity is not None:
            return explicit_quantity
        budget = _first_decimal_from_payloads((quantity_policy,), ("budget_krw", "max_budget", "allocation_budget_krw"))
        reference_price = _first_decimal_from_payloads(
            (quantity_policy,),
            ("estimated_reference_price", "reference_price", "entry_reference_price", "entry_price"),
        )
        if budget is None or reference_price is None:
            return None
        quantity = int((budget / reference_price).to_integral_value(rounding=ROUND_DOWN))
        return quantity if quantity > 0 else None
    return None


def _sibling_trigger_price(items: list[ExecutionPlanItem], intent: str) -> Decimal | None:
    for item in items:
        if item.intent == intent and item.trigger_price is not None and item.trigger_price > 0:
            return item.trigger_price
    return None


def _first_decimal_from_payloads(
    payloads: tuple[object, ...],
    keys: tuple[str, ...],
) -> Decimal | None:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in keys:
            value = _parse_positive_decimal(payload.get(key))
            if value is not None:
                return value
    return None


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, Decimal) and value == value.to_integral_value() and value > 0:
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.isdigit() and int(normalized) > 0:
            return int(normalized)
        try:
            parsed = Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None
        if parsed == parsed.to_integral_value() and parsed > 0:
            return int(parsed)
    return None


def _short_term_exec_agent_sizing_requested(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    return (quantity_policy.get("kind") or quantity_policy.get("type")) == "exec_agent_sized"


def _item_has_buy_chase_or_trigger_fields(item: ExecutionPlanItemRequest) -> bool:
    extras = item.model_extra or {}
    payloads = [extras]
    if isinstance(item.risk_context, dict):
        payloads.append(item.risk_context)
    if isinstance(item.quantity_policy, dict):
        payloads.append(item.quantity_policy)
    watch_buy_fields = (
        CHASE_BUY_FIELDS
        | BUY_TRIGGER_FIELDS
        | frozenset(BUY_RECOMMENDATION_FIELDS)
    )
    for payload in payloads:
        for key in watch_buy_fields:
            if payload.get(key) is not None:
                return True
    return False


def _decision_agent_name(item: ExecutionPlanItemRequest) -> str | None:
    value = item.decision_agent_name
    if value is None:
        return None
    return value.strip().lower()


def _is_watch_buy_request(item: ExecutionPlanItemRequest) -> bool:
    if item.side != "buy":
        return False
    if item.intent in WATCH_BUY_INTENTS:
        return True
    if (
        item.intent == "next_open_buy"
        and _decision_agent_name(item) == SHORT_TERM_DECIDER_NAME
        and _exec_monitoring_active(item)
        and _short_term_tactical_action(item) in ACTIVE_SHORT_TERM_TACTICAL_ACTIONS
    ):
        return True
    return item.intent == "next_open_buy" and _item_has_buy_chase_or_trigger_fields(item)


def _plan_item_trigger_price(item: ExecutionPlanItemRequest) -> Decimal | None:
    if not _is_watch_buy_request(item):
        return item.trigger_price
    return _watch_buy_trigger_price(item) or item.trigger_price


def _plan_item_text(*, item: ExecutionPlanItemRequest, key: str) -> str | None:
    value = getattr(item, key, None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    for source in (item.model_extra or {}, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        raw_value = source.get(key)
        if isinstance(raw_value, str) and raw_value.strip():
            return raw_value.strip().lower()
    return None


def _plan_item_risk_context(item: ExecutionPlanItemRequest) -> dict[str, object] | None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else item.risk_context
    normalized = dict(risk_context or {})
    extras = item.model_extra or {}
    for key in ("market", "exchange", "currency", "broker", "session_policy"):
        value = extras.get(key)
        if value is not None and key not in normalized:
            normalized[key] = _json_scalar(value)
    tactical_action = _short_term_tactical_action(item)
    exec_monitoring_active = _exec_monitoring_active(item)
    if tactical_action is not None:
        normalized.setdefault("tactical_action", tactical_action)
    if exec_monitoring_active is not None:
        normalized.setdefault("exec_monitoring_active", exec_monitoring_active)
    if (
        _decision_agent_name(item) == SHORT_TERM_DECIDER_NAME
        and tactical_action in INACTIVE_SHORT_TERM_TACTICAL_ACTIONS
    ):
        normalized.setdefault(
            "short_term_blocker_reason",
            f"tactical_action_{tactical_action}_is_not_active_buy_monitoring",
        )

    if not _is_watch_buy_request(item):
        return normalized or None

    for key in (*BUY_RECOMMENDATION_FIELDS, *BUY_TRIGGER_FIELDS, *CHASE_BUY_FIELDS, *BUY_TARGET_FIELDS, *BUY_STOP_FIELDS):
        value = extras.get(key)
        if value is not None and key not in normalized:
            normalized[key] = _json_scalar(value)

    trigger_source, trigger_price = _watch_buy_trigger_source_and_price(item)
    chase_limit_price = _watch_buy_chase_limit_price(item)
    normalized["watch_buy_monitoring"] = {
        "received_intent": item.intent,
        "trigger_source": trigger_source,
        "trigger_price": _decimal_text(trigger_price),
        "chase_limit_price": _decimal_text(chase_limit_price),
        "direction": _watch_buy_direction(item),
        "tactical_action": tactical_action,
        "exec_monitoring_active": exec_monitoring_active,
    }
    return normalized


def _short_term_tactical_action(item: ExecutionPlanItemRequest) -> str | None:
    value = _first_text_from_request(item, "tactical_action")
    if value is None:
        return None
    return value.strip().lower()


def _exec_monitoring_active(item: ExecutionPlanItemRequest) -> bool | None:
    for source in (item.model_extra or {}, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        value = source.get("exec_monitoring_active")
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
            return value.strip().lower() == "true"
    return None


def _first_text_from_request(item: ExecutionPlanItemRequest, key: str) -> str | None:
    for source in (item.model_extra or {}, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _watch_buy_trigger_price(item: ExecutionPlanItemRequest) -> Decimal | None:
    return _watch_buy_trigger_source_and_price(item)[1]


def _watch_buy_trigger_source_and_price(
    item: ExecutionPlanItemRequest,
) -> tuple[str | None, Decimal | None]:
    for key in BUY_RECOMMENDATION_FIELDS:
        value = _first_decimal_from_request(item, key)
        if value is not None:
            return key, value
    if item.trigger_price is not None and item.trigger_price > 0:
        return "trigger_price", item.trigger_price
    for key in BUY_TRIGGER_FIELDS:
        value = _first_decimal_from_request(item, key)
        if value is not None:
            return key, value
    return None, None


def _watch_buy_chase_limit_price(item: ExecutionPlanItemRequest) -> Decimal | None:
    for key in ("chase_limit_price", "max_chase_limit_price", "entry_limit_price", "original_watch_chase_limit_price"):
        value = _first_decimal_from_request(item, key)
        if value is not None:
            return value
    return None


def _first_decimal_from_request(
    item: ExecutionPlanItemRequest,
    key: str,
) -> Decimal | None:
    sources: list[object] = [item.model_extra or {}, item.risk_context, item.quantity_policy]
    for source in sources:
        if not isinstance(source, dict):
            continue
        value = _parse_positive_decimal(source.get(key))
        if value is not None:
            return value
    return None


def _parse_positive_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > 0 else None


def _watch_buy_direction(item: ExecutionPlanItemRequest) -> str:
    for source in (item.model_extra or {}, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        for key in ("trigger_direction", "entry_direction", "watch_direction", "direction"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                normalized = value.strip().lower()
                if normalized in {"up", "above", "breakout", "momentum", ">=", "at_or_above"}:
                    return "at_or_above"
                if normalized in {"down", "below", "pullback", "dip", "<=", "at_or_below"}:
                    return "at_or_below"
    if _short_term_tactical_action(item) == "watch_for_trigger":
        return "at_or_above"
    return "at_or_below"


def _decimal_text(value: Decimal | None) -> str | None:
    return f"{value:.4f}" if value is not None else None


def _json_scalar(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    return value


def _is_runtime_monitored_item(item: ExecutionPlanItem) -> bool:
    return (
        item.intent in MONITORED_INTENTS
        or item.status in WATCH_MONITORING_STATUSES
        or item.status in AGENT_RUNTIME_MONITORING_STATUSES
    )
