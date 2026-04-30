from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAdapter,
    BrokerAdapterError,
    BrokerEventSource,
    BrokerOrderPlacementRequest,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionPlanItem
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.execution_api import serialize_broker_order
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)


@dataclass(frozen=True)
class PlaceBrokerOrderResult:
    status_code: int
    payload: dict[str, object]


def place_broker_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str = "post:/orders",
) -> PlaceBrokerOrderResult:
    existing = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    if existing is not None:
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(existing, include_events=True),
            },
        )

    control_state = ensure_runtime_control_state(settings=settings, db=db)
    if not automated_order_submission_allowed(control_state):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "message": "automated order submission is blocked by runtime safety controls",
                "block_reason": automated_order_submission_block_reason(control_state),
                "safety": runtime_safety_payload(control_state),
            },
        )

    plan_item = _load_plan_item(db=db, command=command)
    if plan_item is None and command.execution_plan_item_id is not None:
        return PlaceBrokerOrderResult(
            status_code=404,
            payload={
                "message": "execution_plan_item_id was not found",
                "execution_plan_item_id": command.execution_plan_item_id,
            },
        )

    validation_error = _validate_plan_item(plan_item=plan_item, command=command)
    if validation_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={"message": validation_error},
        )

    order = BrokerOrder(
        broker_name=adapter.provider,
        execution_plan_item_id=plan_item.id if plan_item else None,
        mode=settings.runtime_mode,
        idempotency_key=command.idempotency_key,
        symbol=command.symbol,
        side=command.side,
        status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        quantity=command.quantity,
        limit_price=command.limit_price,
        request_metadata={
            "source": request_source,
            "requested_at": datetime.now(UTC).isoformat(),
            "time_in_force": command.time_in_force,
        },
    )
    db.add(order)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        duplicate = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
        if duplicate is None:
            raise
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(duplicate, include_events=True),
            },
        )

    request_event = BrokerOrderEvent(
        broker_order_pk=order.id,
        event_type="submit_requested",
        event_source=BrokerEventSource.SUBMISSION.value,
        normalized_status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        request_payload=_order_command_payload(command),
        payload={"provider": adapter.provider},
        observed_at=datetime.now(UTC),
    )
    db.add(request_event)
    if plan_item is not None:
        plan_item.status = "broker_order_submission_pending"

    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="broker_order_service",
            reason=f"ensure active broker session for {command.idempotency_key}",
        )
        placement = adapter.place_order(
            session_state.access_token or "",
            BrokerOrderPlacementRequest(
                idempotency_key=command.idempotency_key,
                execution_plan_item_id=command.execution_plan_item_id,
                symbol=command.symbol,
                side=command.side,
                quantity=command.quantity,
                order_type=command.order_type,
                time_in_force=command.time_in_force,
                exchange=command.exchange,
                limit_price=command.limit_price,
            ),
        )
    except BrokerAdapterError as exc:
        order.status = exc.normalized_status.value
        order.last_broker_sync_at = exc.observed_at or datetime.now(UTC)
        order.request_metadata = exc.request_metadata or order.request_metadata
        order.response_metadata = exc.response_metadata or None
        order.last_error_classification = exc.classification.value
        order.last_error_code = exc.code
        order.last_error_message = str(exc)
        order.last_error_retryable = exc.retryable
        order.raw_broker_payload = exc.response_payload or None
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="submit_failed",
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=order.status,
                broker_status=exc.code,
                error_classification=exc.classification.value,
                retryable=exc.retryable,
                message=str(exc),
                request_payload=exc.request_payload or _order_command_payload(command),
                response_payload=exc.response_payload or None,
                payload={"provider": adapter.provider},
                observed_at=exc.observed_at or datetime.now(UTC),
            )
        )
        if plan_item is not None:
            plan_item.status = _plan_item_status_for_order_status(order.status)
        db.commit()
        db.refresh(order)
        return PlaceBrokerOrderResult(
            status_code=exc.status_code or 503,
            payload={
                "duplicate": False,
                "message": str(exc),
                "order": serialize_broker_order(
                    _load_order_by_idempotency_key(
                        settings=settings,
                        db=db,
                        idempotency_key=command.idempotency_key,
                    )
                    or order,
                    include_events=True,
                ),
            },
        )

    order.broker_order_id = placement.broker_order_id
    order.status = placement.normalized_status.value
    order.submitted_at = placement.submitted_at
    order.last_broker_sync_at = placement.observed_at
    order.request_metadata = placement.request_metadata
    order.response_metadata = placement.response_metadata
    order.last_error_classification = None
    order.last_error_code = None
    order.last_error_message = None
    order.last_error_retryable = None
    order.raw_broker_payload = placement.response_payload
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submit_acknowledged",
            event_source=BrokerEventSource.SUBMISSION.value,
            normalized_status=order.status,
            broker_status=placement.broker_status,
            message=placement.message,
            request_payload=placement.request_payload,
            response_payload=placement.response_payload,
            payload={"provider": adapter.provider},
            observed_at=placement.observed_at,
        )
    )
    if plan_item is not None:
        plan_item.status = _plan_item_status_for_order_status(order.status)

    db.commit()
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=201,
        payload={
            "duplicate": False,
            "message": placement.message or "broker order submitted",
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _load_order_by_idempotency_key(
    *,
    settings: Settings,
    db: Session,
    idempotency_key: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == idempotency_key,
        )
        .options(selectinload(BrokerOrder.events))
    )


def _load_plan_item(
    *,
    db: Session,
    command: PlaceOrderRequest,
) -> ExecutionPlanItem | None:
    if command.execution_plan_item_id is None:
        return None
    return db.get(ExecutionPlanItem, command.execution_plan_item_id)


def _validate_plan_item(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
) -> str | None:
    if plan_item is None:
        return None
    if plan_item.symbol != command.symbol:
        return "execution plan item symbol does not match order symbol"
    if plan_item.side != command.side:
        return "execution plan item side does not match order side"
    return None


def _order_command_payload(command: PlaceOrderRequest) -> dict[str, object]:
    return command.model_dump(mode="json")


def _plan_item_status_for_order_status(order_status: str) -> str:
    if order_status == NormalizedBrokerOrderStatus.SUBMITTED.value:
        return "broker_order_submitted"
    if order_status == NormalizedBrokerOrderStatus.RETRY_PENDING.value:
        return "broker_order_retry_pending"
    if order_status == NormalizedBrokerOrderStatus.REJECTED.value:
        return "broker_order_rejected"
    if order_status == NormalizedBrokerOrderStatus.FAILED.value:
        return "broker_order_failed"
    return order_status
