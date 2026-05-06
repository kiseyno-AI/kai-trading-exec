from dataclasses import dataclass
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import ExecutionPlanRequest
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
)

MONITORED_INTENTS = frozenset({"target_sell", "stop_sell"})
OPEN_ORDER_STATUSES = frozenset({"submitted", "open", "partially_filled"})


@dataclass(frozen=True)
class CreateExecutionPlanResult:
    status_code: int
    payload: dict[str, object]


def create_execution_plan(
    *,
    settings: Settings,
    db: Session,
    command: ExecutionPlanRequest,
) -> CreateExecutionPlanResult:
    plan = ExecutionPlan(
        external_plan_id=command.plan_id,
        source=command.source,
        market_date=command.market_date,
        strategy_run_id=command.strategy_run_id,
        mode=settings.runtime_mode,
        status="accepted",
        requested_at=command.requested_at,
        raw_payload=command.model_dump(mode="json"),
        items=[
            ExecutionPlanItem(
                plan_item_index=index,
                symbol=item.symbol,
                side=item.side,
                intent=item.intent,
                status="pending",
                quantity_policy=item.quantity_policy,
                limit_price=item.limit_price,
                trigger_price=item.trigger_price,
                time_in_force=item.time_in_force,
                risk_context=item.risk_context,
                decision_snapshot_id=item.decision_snapshot_id,
            )
            for index, item in enumerate(command.items)
        ],
    )
    from app.services.execution_jobs import materialize_execution_jobs
    from app.services.monitoring import materialize_monitoring_plan_items

    materialize_execution_jobs(plan)
    materialize_monitoring_plan_items(plan)
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
            "runtime": {
                "mode": settings.runtime_mode,
            },
        },
    )


def list_execution_plans(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    plans = db.scalars(
        select(ExecutionPlan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionPlan.items))
        .order_by(ExecutionPlan.accepted_at.desc(), ExecutionPlan.created_at.desc()),
    ).all()
    return [_serialize_execution_plan(plan) for plan in plans]


def list_broker_orders(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    orders = db.scalars(
        select(BrokerOrder)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .options(selectinload(BrokerOrder.events))
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc()),
    ).all()
    return [serialize_broker_order(order) for order in orders]


def list_execution_fills(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc()),
    ).all()
    return [_serialize_execution_fill(fill) for fill in fills]


def list_execution_positions(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    positions = db.scalars(
        select(ExecutionPosition)
        .where(ExecutionPosition.mode == settings.runtime_mode)
        .order_by(ExecutionPosition.symbol.asc()),
    ).all()
    return [_serialize_execution_position(position) for position in positions]


def list_runtime_symbols(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    state = _load_runtime_symbol_state(settings=settings, db=db)
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
            if item.intent in MONITORED_INTENTS
        }
    )
    return {
        "symbol": normalized_symbol,
        "mode": settings.runtime_mode,
        "summary": summary,
        "monitoring": {
            "monitored": any(item.intent in MONITORED_INTENTS for item in plan_items),
            "statuses": monitoring_statuses,
            "pending_dispatch_count": sum(
                1
                for item in plan_items
                if item.status.endswith("_pending_dispatch")
            ),
        },
        "position": _serialize_execution_position(position) if position is not None else None,
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
    return {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "side": item.side,
        "intent": item.intent,
        "status": item.status,
        "quantity_policy": item.quantity_policy,
        "limit_price": _serialize_decimal(item.limit_price),
        "trigger_price": _serialize_decimal(item.trigger_price),
        "time_in_force": item.time_in_force,
        "risk_context": item.risk_context,
        "decision_snapshot_id": item.decision_snapshot_id,
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
    return {
        "execution_fill_id": fill.id,
        "broker_order_pk": fill.broker_order_pk,
        "broker_fill_id": fill.broker_fill_id,
        "symbol": fill.symbol,
        "side": fill.side,
        "quantity": fill.quantity,
        "price": _serialize_decimal(fill.price),
        "filled_at": _serialize_datetime(fill.filled_at),
        "payload": fill.payload,
        "created_at": _serialize_datetime(fill.created_at),
        "updated_at": _serialize_datetime(fill.updated_at),
    }


def _serialize_execution_position(position: ExecutionPosition) -> dict[str, object]:
    return {
        "execution_position_id": position.id,
        "symbol": position.symbol,
        "quantity": position.quantity,
        "average_price": _serialize_decimal(position.average_price),
        "updated_from": position.updated_from,
        "last_reconciled_at": _serialize_datetime(position.last_reconciled_at),
        "raw_payload": position.raw_payload,
        "created_at": _serialize_datetime(position.created_at),
        "updated_at": _serialize_datetime(position.updated_at),
    }


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _load_runtime_symbol_state(
    *,
    settings: Settings,
    db: Session,
    symbol: str | None = None,
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
        .options(selectinload(BrokerOrder.events))
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
    )
    fills_query = (
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == settings.runtime_mode)
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
    )
    positions_query = (
        select(ExecutionPosition)
        .where(ExecutionPosition.mode == settings.runtime_mode)
        .order_by(ExecutionPosition.symbol.asc())
    )
    if symbol is not None:
        plan_items_query = plan_items_query.where(ExecutionPlanItem.symbol == symbol)
        orders_query = orders_query.where(BrokerOrder.symbol == symbol)
        fills_query = fills_query.where(ExecutionFill.symbol == symbol)
        positions_query = positions_query.where(ExecutionPosition.symbol == symbol)

    plan_items = db.scalars(plan_items_query).all()
    orders = db.scalars(orders_query).all()
    fills = db.scalars(fills_query).all()
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
            if item.intent in MONITORED_INTENTS
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
    return {
        "symbol": symbol,
        "mode": settings.runtime_mode,
        "monitored": any(item.intent in MONITORED_INTENTS for item in plan_items),
        "monitoring_statuses": monitoring_statuses,
        "intents": sorted({item.intent for item in plan_items}),
        "plan_item_count": len(plan_items),
        "latest_plan_item_status": latest_plan_item.status if latest_plan_item is not None else None,
        "order_count": len(orders),
        "open_order_count": sum(order.status in OPEN_ORDER_STATUSES for order in orders),
        "fill_count": len(fills),
        "filled_quantity": sum(fill.quantity for fill in fills),
        "position_quantity": position.quantity if position is not None else 0,
        "position_average_price": (
            _serialize_decimal(position.average_price)
            if position is not None
            else None
        ),
        "latest_activity_at": latest_activity_at,
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
