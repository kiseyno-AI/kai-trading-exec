from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionFailure, ExecutionPlanItem
from app.services.plan_item_identity import plan_item_decision_agent_name, raw_plan_item_payload

ACTIONABLE_FAILURE_CLASSIFICATIONS = frozenset(
    {
        "insufficient_funds",
        "insufficient_buying_power",
        "insufficient_sellable_quantity",
    }
)
EXECUTION_FAILURE_STATUSES = frozenset({"open", "resolved", "ignored"})

_INSUFFICIENT_FUNDS_TOKENS = (
    "insufficient cash",
    "insufficient funds",
    "cash shortage",
    "cash balance",
    "available cash",
    "deposit shortage",
    "예수금",
    "현금",
    "잔액 부족",
)
_INSUFFICIENT_BUYING_POWER_TOKENS = (
    "insufficient buying power",
    "buying power",
    "orderable amount",
    "orderable quantity",
    "purchasable amount",
    "available to buy",
    "margin shortage",
    "주문가능금액",
    "주문 가능 금액",
    "매수가능금액",
    "매수 가능 금액",
    "매수가능수량",
    "증거금",
)
_INSUFFICIENT_SELLABLE_TOKENS = (
    "insufficient sellable quantity",
    "sellable quantity",
    "available quantity to sell",
    "insufficient position quantity",
    "insufficient holdings",
    "매도가능수량",
    "매도 가능 수량",
    "보유수량 부족",
    "잔고수량 부족",
)
_DECISION_ACTOR_KEYS = (
    "agent_name",
    "decider_name",
    "decision_actor",
    "agent",
    "decider",
)


@dataclass(frozen=True)
class ExecutionFailureStatusUpdateCommand:
    status: str
    actor: str | None
    source: str
    reason: str | None


@dataclass(frozen=True)
class UpdateExecutionFailureStatusResult:
    status_code: int
    payload: dict[str, object]


def record_actionable_execution_failure(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem | None,
    broker_error_classification: str | None,
    broker_error_code: str | None,
    broker_error_message: str,
    retryable: bool,
    observed_at: datetime,
    request_source: str,
) -> ExecutionFailure | None:
    classification = classify_actionable_failure(
        side=order.side,
        broker_error_code=broker_error_code,
        broker_error_message=broker_error_message,
    )
    if classification is None or retryable:
        return None

    failure = db.scalar(
        select(ExecutionFailure).where(
            ExecutionFailure.mode == settings.runtime_mode,
            ExecutionFailure.broker_order_pk == order.id,
        )
    )
    plan = plan_item.plan if plan_item is not None else None
    requested_notional = _requested_notional(order.quantity, order.limit_price)
    account = _account_identifier(settings=settings, order=order)
    decision_actor = _decision_actor(plan_item=plan_item)
    raw_context = {
        "request_source": request_source,
        "response_metadata": order.response_metadata,
        "request_metadata": order.request_metadata,
        "raw_broker_payload": order.raw_broker_payload,
    }

    if failure is None:
        failure = ExecutionFailure(
            mode=settings.runtime_mode,
            broker_order_pk=order.id,
            execution_plan_id=plan.id if plan is not None else None,
            execution_plan_item_id=plan_item.id if plan_item is not None else None,
            plan_id=plan.external_plan_id if plan is not None else None,
            plan_source=plan.source if plan is not None else None,
            strategy_run_id=plan.strategy_run_id if plan is not None else None,
            decision_actor=decision_actor,
            broker_name=order.broker_name,
            account=account,
            idempotency_key=order.idempotency_key,
            symbol=order.symbol,
            side=order.side,
            intent=plan_item.intent if plan_item is not None else None,
            requested_quantity=order.quantity,
            requested_limit_price=order.limit_price,
            requested_notional=requested_notional,
            classification=classification,
            broker_error_classification=broker_error_classification,
            broker_error_code=broker_error_code,
            broker_error_message=broker_error_message,
            retryable=retryable,
            status="open",
            first_failed_at=observed_at,
            latest_failed_at=observed_at,
            raw_context=raw_context,
        )
        db.add(failure)
        return failure

    failure.execution_plan_id = plan.id if plan is not None else failure.execution_plan_id
    failure.execution_plan_item_id = (
        plan_item.id if plan_item is not None else failure.execution_plan_item_id
    )
    failure.plan_id = plan.external_plan_id if plan is not None else failure.plan_id
    failure.plan_source = plan.source if plan is not None else failure.plan_source
    failure.strategy_run_id = plan.strategy_run_id if plan is not None else failure.strategy_run_id
    failure.decision_actor = decision_actor or failure.decision_actor
    failure.account = account or failure.account
    failure.intent = plan_item.intent if plan_item is not None else failure.intent
    failure.requested_quantity = order.quantity
    failure.requested_limit_price = order.limit_price
    failure.requested_notional = requested_notional
    failure.classification = classification
    failure.broker_error_classification = broker_error_classification
    failure.broker_error_code = broker_error_code
    failure.broker_error_message = broker_error_message
    failure.retryable = retryable
    failure.latest_failed_at = observed_at
    failure.raw_context = raw_context
    return failure


def list_execution_failures(
    *,
    settings: Settings,
    db: Session,
    classification: str | None = None,
    status: str | None = None,
) -> list[dict[str, object]]:
    query = select(ExecutionFailure).where(ExecutionFailure.mode == settings.runtime_mode)
    if classification is not None:
        query = query.where(ExecutionFailure.classification == classification)
    if status is not None:
        query = query.where(ExecutionFailure.status == status)
    failures = db.scalars(
        query.order_by(ExecutionFailure.latest_failed_at.desc(), ExecutionFailure.created_at.desc())
    ).all()
    return [serialize_execution_failure(failure) for failure in failures]


def update_execution_failure_status(
    *,
    settings: Settings,
    db: Session,
    failure_id: str,
    command: ExecutionFailureStatusUpdateCommand,
) -> UpdateExecutionFailureStatusResult:
    failure = db.scalar(
        select(ExecutionFailure).where(
            ExecutionFailure.mode == settings.runtime_mode,
            ExecutionFailure.id == failure_id,
        )
    )
    if failure is None:
        return UpdateExecutionFailureStatusResult(
            status_code=404,
            payload={"message": "execution_failure_id was not found"},
        )

    failure.status = command.status
    failure.status_updated_at = datetime.now(UTC)
    failure.status_updated_by = command.actor
    failure.status_updated_source = command.source
    failure.status_updated_reason = command.reason
    db.commit()
    db.refresh(failure)
    return UpdateExecutionFailureStatusResult(
        status_code=200,
        payload=serialize_execution_failure(failure),
    )


def serialize_execution_failure(failure: ExecutionFailure) -> dict[str, object]:
    return {
        "execution_failure_id": failure.id,
        "broker_order_pk": failure.broker_order_pk,
        "execution_plan_id": failure.execution_plan_id,
        "execution_plan_item_id": failure.execution_plan_item_id,
        "plan_id": failure.plan_id,
        "core_plan_id": failure.plan_id,
        "plan_source": failure.plan_source,
        "strategy_run_id": failure.strategy_run_id,
        "decision_actor": failure.decision_actor,
        "broker_name": failure.broker_name,
        "account": failure.account,
        "idempotency_key": failure.idempotency_key,
        "symbol": failure.symbol,
        "side": failure.side,
        "intent": failure.intent,
        "requested_quantity": failure.requested_quantity,
        "requested_limit_price": _serialize_decimal(failure.requested_limit_price),
        "requested_notional": _serialize_decimal(failure.requested_notional),
        "classification": failure.classification,
        "broker_error_classification": failure.broker_error_classification,
        "broker_error_code": failure.broker_error_code,
        "broker_error_message": failure.broker_error_message,
        "retryable": failure.retryable,
        "status": failure.status,
        "first_failed_at": _serialize_datetime(failure.first_failed_at),
        "latest_failed_at": _serialize_datetime(failure.latest_failed_at),
        "retry_decision_status": failure.retry_decision_status,
        "retry_decision_reason": failure.retry_decision_reason,
        "retry_decision_source": failure.retry_decision_source,
        "retry_next_at": _serialize_datetime(failure.retry_next_at),
        "retry_deadline_at": _serialize_datetime(failure.retry_deadline_at),
        "retry_max_attempts": failure.retry_max_attempts,
        "retry_attempt_count": failure.retry_attempt_count,
        "retry_job_id": failure.retry_job_id,
        "retry_order_idempotency_key": failure.retry_order_idempotency_key,
        "retry_exhausted_reason": failure.retry_exhausted_reason,
        "retry_audit_context": failure.retry_audit_context,
        "status_updated_at": _serialize_datetime(failure.status_updated_at),
        "status_updated_by": failure.status_updated_by,
        "status_updated_source": failure.status_updated_source,
        "status_updated_reason": failure.status_updated_reason,
        "created_at": _serialize_datetime(failure.created_at),
        "updated_at": _serialize_datetime(failure.updated_at),
    }


def classify_actionable_failure(
    *,
    side: str,
    broker_error_code: str | None,
    broker_error_message: str,
) -> str | None:
    haystack_lower = f"{broker_error_code or ''} {broker_error_message}".lower()
    haystack = f"{broker_error_code or ''} {broker_error_message}"

    if side == "sell" and _contains_any(haystack_lower, haystack, _INSUFFICIENT_SELLABLE_TOKENS):
        return "insufficient_sellable_quantity"
    if side == "buy" and _contains_any(haystack_lower, haystack, _INSUFFICIENT_FUNDS_TOKENS):
        return "insufficient_funds"
    if side == "buy" and _contains_any(
        haystack_lower,
        haystack,
        _INSUFFICIENT_BUYING_POWER_TOKENS,
    ):
        return "insufficient_buying_power"
    return None


def _contains_any(lower_haystack: str, raw_haystack: str, needles: tuple[str, ...]) -> bool:
    for needle in needles:
        if needle.lower() in lower_haystack:
            return True
        if needle in raw_haystack:
            return True
    return False


def _account_identifier(*, settings: Settings, order: BrokerOrder) -> str | None:
    request_metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    masked = request_metadata.get("account_number_masked")
    if isinstance(masked, str) and masked:
        return masked
    account = settings.kis_account_number
    if not account:
        return None
    if len(account) <= 4:
        return account
    return f"{'*' * (len(account) - 4)}{account[-4:]}"


def _decision_actor(*, plan_item: ExecutionPlanItem | None) -> str | None:
    if plan_item is None:
        return None

    decision_agent_name = plan_item_decision_agent_name(plan_item)
    if decision_agent_name is not None:
        return decision_agent_name

    candidates: list[dict[str, Any]] = []
    if isinstance(plan_item.risk_context, dict):
        candidates.append(plan_item.risk_context)

    raw_plan_item = _raw_plan_item_payload(plan_item)
    if isinstance(raw_plan_item, dict):
        candidates.append(raw_plan_item)

    plan = plan_item.plan
    if isinstance(plan.raw_payload, dict):
        candidates.append(plan.raw_payload)

    for candidate in candidates:
        for key in _DECISION_ACTOR_KEYS:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _raw_plan_item_payload(plan_item: ExecutionPlanItem) -> dict[str, Any] | None:
    return raw_plan_item_payload(plan_item)


def _requested_notional(quantity: int, limit_price: Decimal | None) -> Decimal | None:
    if limit_price is None:
        return None
    return Decimal(quantity) * limit_price


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None
