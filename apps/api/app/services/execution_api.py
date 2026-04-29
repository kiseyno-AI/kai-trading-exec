from dataclasses import dataclass
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
            for item in command.items
        ],
    )
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


def list_broker_orders(db: Session) -> list[dict[str, object]]:
    orders = db.scalars(
        select(BrokerOrder)
        .options(selectinload(BrokerOrder.events))
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc()),
    ).all()
    return [serialize_broker_order(order) for order in orders]


def list_execution_fills(db: Session) -> list[dict[str, object]]:
    fills = db.scalars(
        select(ExecutionFill).order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc()),
    ).all()
    return [_serialize_execution_fill(fill) for fill in fills]


def list_execution_positions(db: Session) -> list[dict[str, object]]:
    positions = db.scalars(
        select(ExecutionPosition).order_by(ExecutionPosition.symbol.asc()),
    ).all()
    return [_serialize_execution_position(position) for position in positions]


def _serialize_execution_plan(plan: ExecutionPlan) -> dict[str, object]:
    items = sorted(
        plan.items,
        key=lambda item: (
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
                key=lambda item: (_serialize_datetime(item.observed_at) or "", item.id),
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
