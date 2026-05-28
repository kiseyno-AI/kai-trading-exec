from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionPlan, ExecutionPlanItem
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.domestic_order_router import RouteDecision, resolve_domestic_order_route
from app.services.execution_failures import record_actionable_execution_failure
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

    plan_item = _load_plan_item(
        settings=settings,
        db=db,
        command=command,
    )
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

    route_decision = _resolve_route_decision(
        settings=settings,
        command=command,
        plan_item=plan_item,
    )
    if route_decision is not None and route_decision.action != "submit":
        return PlaceBrokerOrderResult(
            status_code=202 if route_decision.action == "defer" else 409,
            payload={
                "duplicate": False,
                "message": route_decision.reason,
                "deferred": route_decision.action == "defer",
                "block_reason": (
                    route_decision.reason
                    if route_decision.action == "block"
                    else None
                ),
                "route": _route_payload(route_decision),
            },
        )
    routed_command = _routed_order_command(
        command=command,
        route_decision=route_decision,
    )

    order = BrokerOrder(
        broker_name=adapter.provider,
        execution_plan_item_id=plan_item.id if plan_item else None,
        mode=settings.runtime_mode,
        idempotency_key=routed_command.idempotency_key,
        symbol=routed_command.symbol,
        side=routed_command.side,
        status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        quantity=routed_command.quantity,
        limit_price=routed_command.limit_price,
        request_metadata={
            "source": request_source,
            "requested_at": datetime.now(UTC).isoformat(),
            "time_in_force": routed_command.time_in_force,
            **({"metadata": routed_command.metadata} if routed_command.metadata is not None else {}),
            **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
        },
    )
    db.add(order)
    request_observed_at = datetime.now(UTC)
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
        request_payload=_order_command_payload(routed_command),
        payload={"provider": adapter.provider},
        observed_at=request_observed_at,
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
                execution_plan_item_id=routed_command.execution_plan_item_id,
                symbol=routed_command.symbol,
                side=routed_command.side,
                quantity=routed_command.quantity,
                order_type=routed_command.order_type,
                time_in_force=routed_command.time_in_force,
                exchange=routed_command.exchange,
                limit_price=routed_command.limit_price,
                order_division=route_decision.ord_dvsn if route_decision is not None else None,
                route_session=route_decision.session if route_decision is not None else None,
                route_reason=route_decision.reason if route_decision is not None else None,
            ),
        )
    except BrokerAdapterError as exc:
        observed_at = exc.observed_at or datetime.now(UTC)
        event_observed_at = _response_event_observed_at(
            observed_at=observed_at,
            request_observed_at=request_observed_at,
        )
        order.status = exc.normalized_status.value
        order.last_broker_sync_at = observed_at
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
                request_payload=exc.request_payload or _order_command_payload(routed_command),
                response_payload=exc.response_payload or None,
                payload={"provider": adapter.provider},
                observed_at=event_observed_at,
            )
        )
        if plan_item is not None:
            plan_item.status = _plan_item_status_for_order_status(order.status)
        record_actionable_execution_failure(
            settings=settings,
            db=db,
            order=order,
            plan_item=plan_item,
            broker_error_classification=exc.classification.value,
            broker_error_code=exc.code,
            broker_error_message=str(exc),
            retryable=exc.retryable,
            observed_at=observed_at,
            request_source=request_source,
        )
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
    order.request_metadata = _merge_order_request_metadata(
        base=placement.request_metadata,
        command=routed_command,
        request_source=request_source,
        route_decision=route_decision,
    )
    order.response_metadata = placement.response_metadata
    order.last_error_classification = None
    order.last_error_code = None
    order.last_error_message = None
    order.last_error_retryable = None
    order.raw_broker_payload = placement.response_payload
    event_observed_at = _response_event_observed_at(
        observed_at=placement.observed_at,
        request_observed_at=request_observed_at,
    )
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
            observed_at=event_observed_at,
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
    settings: Settings,
    db: Session,
    command: PlaceOrderRequest,
) -> ExecutionPlanItem | None:
    if command.execution_plan_item_id is None:
        return None
    return db.scalar(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlanItem.id == command.execution_plan_item_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
    )


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


def _merge_order_request_metadata(
    *,
    base: dict[str, object] | None,
    command: PlaceOrderRequest,
    request_source: str,
    route_decision: RouteDecision | None = None,
) -> dict[str, object]:
    metadata = dict(base or {})
    metadata["source"] = request_source
    metadata["time_in_force"] = command.time_in_force
    if command.metadata is not None:
        metadata["metadata"] = command.metadata
    if route_decision is not None:
        metadata["route"] = _route_payload(route_decision)
    return metadata


def _resolve_route_decision(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> RouteDecision | None:
    if not settings.domestic_order_session_routing_enabled:
        return None
    metadata = dict(command.metadata or {})
    if plan_item is not None and isinstance(plan_item.risk_context, dict):
        for key in ("nxt_eligible", "order_purpose", "purpose"):
            if key in plan_item.risk_context and key not in metadata:
                metadata[key] = plan_item.risk_context[key]
    purpose = _order_purpose(command=command, plan_item=plan_item, metadata=metadata)
    return resolve_domestic_order_route(
        settings=settings,
        now=_route_now(metadata),
        symbol=command.symbol,
        side=command.side,
        purpose=purpose,
        desired_order_type=command.order_type,
        limit_price=command.limit_price,
        requested_exchange=command.exchange,
        metadata=metadata,
    )


def _routed_order_command(
    *,
    command: PlaceOrderRequest,
    route_decision: RouteDecision | None,
) -> PlaceOrderRequest:
    if route_decision is None:
        return command
    metadata = dict(command.metadata or {})
    metadata["route"] = _route_payload(route_decision)
    return command.model_copy(
        update={
            "exchange": route_decision.exchange,
            "order_type": route_decision.effective_order_type,
            "limit_price": command.limit_price,
            "metadata": metadata,
        }
    )


def _route_payload(route_decision: RouteDecision | None) -> dict[str, object] | None:
    if route_decision is None:
        return None
    return {
        "action": route_decision.action,
        "exchange": route_decision.exchange,
        "session": route_decision.session,
        "ord_dvsn": route_decision.ord_dvsn,
        "effective_order_type": route_decision.effective_order_type,
        "price_source": route_decision.price_source,
        "next_retry_at": (
            route_decision.next_retry_at.isoformat()
            if route_decision.next_retry_at is not None
            else None
        ),
        "reason": route_decision.reason,
    }


def _order_purpose(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    metadata: dict[str, object],
) -> str:
    raw_purpose = metadata.get("order_purpose") or metadata.get("purpose")
    if isinstance(raw_purpose, str) and raw_purpose.strip():
        return raw_purpose.strip().lower()
    if plan_item is not None and plan_item.intent == "stop_sell":
        return "protective_stop"
    if plan_item is not None:
        return plan_item.intent
    return "manual_order"


def _route_now(metadata: dict[str, object]) -> datetime:
    raw_value = metadata.get("route_now") or metadata.get("observed_at")
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    return datetime.now(UTC)


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


def _response_event_observed_at(
    *,
    observed_at: datetime,
    request_observed_at: datetime,
) -> datetime:
    minimum = request_observed_at + timedelta(microseconds=1)
    if observed_at >= minimum:
        return observed_at
    return minimum
