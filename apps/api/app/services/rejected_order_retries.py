from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, DecimalException
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerEventSource,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionJob,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    new_id,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.execution_failures import classify_actionable_failure
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
)
from app.services.runtime_transitions import (
    SELL_TRIGGERED_BLOCKED_STATUSES,
    apply_broker_order_status,
    set_plan_item_status,
)

BROKER_ORDER_RETRY_JOB_TYPE = "broker_order_retry"
TEMPORARY_MARKET_REJECT_CLASSIFICATION = "temporary_market_state"
UNKNOWN_REJECT_CLASSIFICATION = "unknown_reject"
PERMANENT_REJECT_CLASSIFICATION = "permanent_reject"
RETRY_MAX_ATTEMPTS_PER_ROOT_ORDER = 2
RETRY_DEADLINE = timedelta(hours=1)
GENERIC_TEMPORARY_RETRY_DELAY = timedelta(minutes=5)
TEMPORARY_RETRY_DELAYS = {
    "circuit_breaker": timedelta(minutes=20),
    "sidecar": timedelta(minutes=5),
    "volatility_interruption": timedelta(minutes=2),
    "single_price": timedelta(minutes=5),
}
ACTIVE_ORDER_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        NormalizedBrokerOrderStatus.RETRY_PENDING.value,
    }
)
DUPLICATE_BLOCKING_ORDER_STATUSES = ACTIVE_ORDER_STATUSES - {
    NormalizedBrokerOrderStatus.RETRY_PENDING.value
}
RETRY_COMPATIBLE_PLAN_ITEM_STATUSES = (
    frozenset(
        {
            "broker_order_rejected",
            "broker_order_retry_pending",
            "broker_order_failed",
            "retry_pending",
            "rejected",
            "failed",
            "watch_buy_triggered_blocked",
            "watch_buy_triggered_pending_dispatch",
            "next_open_buy_retry_pending",
            "next_open_buy_failed",
        }
    )
    | SELL_TRIGGERED_BLOCKED_STATUSES
)
NO_RETRY_PLAN_ITEM_STATUSES = frozenset(
    {
        "expired",
        "watch_expired",
        "watch_cancelled",
        "watch_buy_rejected",
        "exec_agent_rejected",
        "superseded",
        "canceled",
        "cancelled",
        "filled",
    }
)
BUY_EXPIRY_KEYS = (
    "signal_expires_at",
    "signal_valid_until",
    "valid_until",
    "expires_at",
    "expiry_at",
    "watch_expires_at",
)
TEMPORARY_REJECT_CODES = frozenset(
    {
        "KIOK0320",
        "APBK0918",
        "MARKET_CLOSED",
        "MARKET_HALTED",
        "MARKET_HALT",
        "CIRCUIT_BREAKER",
        "KOSPI_CIRCUIT_BREAKER",
        "KOSDAQ_SIDECAR",
        "SIDECAR",
        "VI",
        "VOLATILITY_INTERRUPTION",
        "SINGLE_PRICE",
        "TEMPORARY_NOT_ORDERABLE",
        "ORDER_TIME_RESTRICTED",
    }
)
PERMANENT_REJECT_CODES = frozenset(
    {
        "INSUFFICIENT_CASH",
        "INSUFFICIENT_FUNDS",
        "INSUFFICIENT_BUYING_POWER",
        "INSUFFICIENT_SELLABLE_QTY",
        "INSUFFICIENT_HOLDINGS",
        "INVALID_ACCOUNT",
        "INVALID_PRODUCT",
        "INVALID_QUANTITY",
        "INVALID_PRICE_UNIT",
    }
)
TEMPORARY_REJECT_MARKERS = (
    "circuit breaker",
    "sidecar",
    "trading halt",
    "halted",
    "halt",
    "temporary",
    "not orderable",
    "market state",
    "market operation",
    "market closed",
    "not regular market",
    "order time",
    "session",
    "volatility interruption",
    "single price",
    "vi",
    "서킷브레이커",
    "사이드카",
    "매매거래정지",
    "거래정지",
    "매매중단",
    "일시정지",
    "일시중단",
    "주문불가",
    "장운영시간",
    "정규시장 시간 주문불가",
    "변동성완화",
    "단일가",
)
_CANONICAL_EXECUTION_SCOPE_KEYS = frozenset(
    {
        "policy_version",
        "provider",
        "account_scope",
        "venue",
        "asset_class",
        "exchange",
    }
)


@dataclass(frozen=True)
class RejectClassification:
    classification: str
    retryable: bool
    code: str | None
    message: str
    reason: str
    source: str
    next_retry_at: datetime | None
    market_state: dict[str, Any] | None
    temporary_reason: str | None = None


class RetryOrderGuardError(Exception):
    def __init__(
        self,
        message: str,
        *,
        job_status: str = "failed",
        plan_item_status: str | None = None,
    ) -> None:
        super().__init__(message)
        self.job_status = job_status
        self.plan_item_status = plan_item_status


class _RetryCommandIntegrityError(ValueError):
    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class _RetryCommandAuthority:
    command: PlaceOrderRequest
    execution_scope: dict[str, object]


def handle_reconciled_rejected_order(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    payload: dict[str, object],
    observed_at: datetime,
) -> ExecutionFailure:
    classification = classify_rejected_order(order=order, payload=payload)
    order.last_error_classification = classification.classification
    order.last_error_code = classification.code
    order.last_error_message = classification.message
    order.last_error_retryable = classification.retryable

    failure = _upsert_rejected_order_failure(
        settings=settings,
        db=db,
        order=order,
        classification=classification,
        observed_at=observed_at,
        payload=payload,
    )
    decision_before = _failure_decision_signature(failure)
    _apply_retry_decision(
        settings=settings,
        db=db,
        order=order,
        failure=failure,
        classification=classification,
        observed_at=observed_at,
        payload=payload,
    )
    if decision_before != _failure_decision_signature(failure):
        _record_retry_decision_event(
            order=order,
            failure=failure,
            classification=classification,
            payload=payload,
            observed_at=observed_at,
        )
    if not classification.retryable and order.plan_item is not None and order.plan_item.intent == "target_sell":
        existing_audit = db.scalar(
            select(ExecutionPlanItemAudit.id).where(
                ExecutionPlanItemAudit.execution_plan_item_id == order.plan_item.id,
                ExecutionPlanItemAudit.event_type == "nonretryable_sell_monitor_suppressed",
            )
        )
        if existing_audit is None:
            db.add(
                ExecutionPlanItemAudit(
                    execution_plan_item_id=order.plan_item.id,
                    event_type="nonretryable_sell_monitor_suppressed",
                    source="rejected_order_retry",
                    summary={
                        "status": "manual_review_required",
                        "reason": "nonretryable_broker_rejection",
                        "symbol": order.symbol,
                        "intent": order.plan_item.intent,
                    },
                    payload={
                        "broker_code": classification.code,
                        "broker_message": classification.message,
                        "trigger_price": str(order.plan_item.trigger_price) if order.plan_item.trigger_price else None,
                    },
                    error_message=classification.message,
                    occurred_at=observed_at,
                )
            )
    return failure


def classify_rejected_order(
    *,
    order: BrokerOrder,
    payload: dict[str, object],
) -> RejectClassification:
    code = _first_text(
        payload,
        "reject_reason_code",
        "reject_code",
        "reason_code",
        "error_code",
        "msg_cd",
        "broker_status",
    )
    message = _reject_message(payload)
    permanent = classify_actionable_failure(
        side=order.side,
        broker_error_code=code,
        broker_error_message=message,
    )
    if permanent is not None:
        return RejectClassification(
            classification=permanent,
            retryable=False,
            code=code,
            message=message,
            reason=f"permanent_{permanent}",
            source="reconciliation",
            next_retry_at=None,
            market_state=_market_state(payload),
        )
    if code is not None and code.strip().upper() in PERMANENT_REJECT_CODES:
        return RejectClassification(
            classification=PERMANENT_REJECT_CLASSIFICATION,
            retryable=False,
            code=code,
            message=message,
            reason="permanent_reject_code",
            source="reconciliation",
            next_retry_at=None,
            market_state=_market_state(payload),
        )

    market_state = _market_state(payload)
    temporary_reason = _temporary_reason(
        code=code,
        message=message,
        market_state=market_state,
    )
    if temporary_reason is not None:
        return RejectClassification(
            classification=TEMPORARY_MARKET_REJECT_CLASSIFICATION,
            retryable=True,
            code=code,
            message=message,
            reason=f"temporary_{temporary_reason}",
            source="reconciliation",
            next_retry_at=_next_retry_at_from_payload(payload, market_state),
            market_state=market_state,
            temporary_reason=temporary_reason,
        )

    return RejectClassification(
        classification=UNKNOWN_REJECT_CLASSIFICATION,
        retryable=False,
        code=code,
        message=message,
        reason="unclassified_reject",
        source="reconciliation",
        next_retry_at=None,
        market_state=market_state,
    )


def build_rejected_order_retry_request(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job: ExecutionJob,
    triggered_at: datetime,
) -> PlaceOrderRequest:
    failure = _load_failure_for_retry_job(settings=settings, db=db, job=job)
    if failure is None:
        raise RetryOrderGuardError(
            "retry job is not linked to an execution failure",
            plan_item_status="broker_order_failed",
        )
    source_order = db.get(
        BrokerOrder,
        failure.broker_order_pk,
        options=[selectinload(BrokerOrder.plan_item)],
    )
    if source_order is None:
        _mark_failure_retry_skipped(
            failure=failure,
            reason="source_broker_order_missing",
            recorded_at=triggered_at,
        )
        raise RetryOrderGuardError(
            "retry source broker order is missing",
            plan_item_status="broker_order_failed",
        )
    plan_item = source_order.plan_item
    if plan_item is None:
        _mark_failure_retry_skipped(
            failure=failure,
            reason="missing_execution_plan_item",
            recorded_at=triggered_at,
        )
        raise RetryOrderGuardError("retry requires an active execution plan item")
    if failure.retry_decision_status != "scheduled":
        raise RetryOrderGuardError(
            f"retry decision is {failure.retry_decision_status or 'not scheduled'}",
            plan_item_status="broker_order_failed",
        )
    if failure.retry_deadline_at is not None and _normalize_utc(triggered_at) > _normalize_utc(
        failure.retry_deadline_at
    ):
        _mark_failure_retry_exhausted(
            failure=failure,
            reason="retry_deadline_elapsed",
            recorded_at=triggered_at,
        )
        raise RetryOrderGuardError(
            "retry deadline elapsed before dispatch",
            plan_item_status="broker_order_failed",
        )
    if not _plan_item_allows_retry(plan_item=plan_item, order=source_order, at=triggered_at):
        _mark_failure_retry_skipped(
            failure=failure,
            reason="inactive_strategy_intent",
            recorded_at=triggered_at,
        )
        raise RetryOrderGuardError(
            "strategy intent is no longer active for retry",
            plan_item_status=_terminal_plan_item_status(plan_item),
        )
    expected_idempotency_key = _retry_idempotency_key(order=source_order, job=job)
    try:
        retry_authority = _build_retry_command_authority(
            settings=settings,
            db=db,
            adapter=adapter,
            source_order=source_order,
            plan_item=plan_item,
            idempotency_key=expected_idempotency_key,
        )
    except _RetryCommandIntegrityError as exc:
        _quarantine_retry_command_integrity(
            db=db,
            order=source_order,
            failure=failure,
            reason=exc.reason_code,
            recorded_at=triggered_at,
        )
        raise RetryOrderGuardError(
            f"rejected-order retry requires manual review: {exc.reason_code}",
            plan_item_status="manual_review_required",
        ) from exc
    existing_retry_order = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=expected_idempotency_key,
    )
    if existing_retry_order is not None:
        try:
            _validate_existing_retry_order_binding(
                order=existing_retry_order,
                source_order=source_order,
                failure=failure,
                job=job,
                retry_authority=retry_authority,
            )
        except _RetryCommandIntegrityError as exc:
            _quarantine_retry_command_integrity(
                db=db,
                order=source_order,
                failure=failure,
                reason=exc.reason_code,
                recorded_at=triggered_at,
            )
            raise RetryOrderGuardError(
                f"rejected-order retry requires manual review: {exc.reason_code}",
                plan_item_status="manual_review_required",
            ) from exc
    else:
        try:
            active_duplicate = _active_duplicate_order(
                settings=settings,
                db=db,
                source_order=source_order,
                expected_idempotency_key=expected_idempotency_key,
                execution_scope=retry_authority.execution_scope,
            )
        except _RetryCommandIntegrityError as exc:
            _quarantine_retry_command_integrity(
                db=db,
                order=source_order,
                failure=failure,
                reason=exc.reason_code,
                recorded_at=triggered_at,
            )
            raise RetryOrderGuardError(
                f"rejected-order retry requires manual review: {exc.reason_code}",
                plan_item_status="manual_review_required",
            ) from exc
        if active_duplicate is not None:
            _mark_failure_retry_skipped(
                failure=failure,
                reason="active_duplicate_order_exists",
                recorded_at=triggered_at,
                context={"duplicate_broker_order_pk": active_duplicate.id},
            )
            raise RetryOrderGuardError(
                "active duplicate order exists for retry target",
                plan_item_status="broker_order_retry_pending",
            )
        try:
            _validate_account_state_for_retry(
                settings=settings,
                db=db,
                adapter=adapter,
                order=source_order,
                command=retry_authority.command,
                execution_scope=retry_authority.execution_scope,
                observed_at=triggered_at,
            )
        except _RetryCommandIntegrityError as exc:
            _quarantine_retry_command_integrity(
                db=db,
                order=source_order,
                failure=failure,
                reason=exc.reason_code,
                recorded_at=triggered_at,
            )
            raise RetryOrderGuardError(
                f"rejected-order retry requires manual review: {exc.reason_code}",
                plan_item_status="manual_review_required",
            ) from exc
        except RetryOrderGuardError as exc:
            if exc.job_status != "retrying":
                _mark_failure_retry_skipped(
                    failure=failure,
                    reason=str(exc),
                    recorded_at=triggered_at,
                )
            raise
        failure.retry_attempt_count = (failure.retry_attempt_count or 0) + 1
        failure.retry_order_idempotency_key = expected_idempotency_key

    metadata = dict(retry_authority.command.metadata or {})
    metadata["retry"] = {
        "source": "rejected_order_retry",
        "source_broker_order_pk": source_order.id,
        "source_broker_order_id": source_order.broker_order_id,
        "root_broker_order_pk": _root_broker_order_pk(source_order),
        "execution_failure_id": failure.id,
        "retry_job_id": job.id,
        "attempt": failure.retry_attempt_count,
        "max_attempts": failure.retry_max_attempts,
        "decision_reason": failure.retry_decision_reason,
        "scheduled_for": _serialize_datetime(job.scheduled_for),
        "deadline_at": _serialize_datetime(failure.retry_deadline_at),
        "source_execution_scope": dict(retry_authority.execution_scope),
    }
    return retry_authority.command.model_copy(
        update={
            "idempotency_key": expected_idempotency_key,
            "execution_plan_item_id": plan_item.id,
            "metadata": metadata,
        }
    )


def record_retry_dispatch_result(
    *,
    settings: Settings,
    db: Session,
    job: ExecutionJob,
    result_payload: dict[str, object],
    recorded_at: datetime,
) -> None:
    failure = _load_failure_for_retry_job(settings=settings, db=db, job=job)
    if failure is None:
        return
    order_payload = result_payload.get("order")
    if not isinstance(order_payload, dict):
        return
    order_status = order_payload.get("status")
    if isinstance(order_status, str) and order_status in ACTIVE_ORDER_STATUSES:
        failure.retry_decision_status = "submitted"
    retry_context = dict(failure.retry_audit_context or {})
    attempts = retry_context.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
    attempts.append(
        {
            "recorded_at": recorded_at.isoformat(),
            "job_id": job.id,
            "idempotency_key": order_payload.get("idempotency_key"),
            "broker_order_pk": order_payload.get("broker_order_pk"),
            "broker_order_id": order_payload.get("broker_order_id"),
            "status": order_payload.get("status"),
            "duplicate": result_payload.get("duplicate"),
            "message": result_payload.get("message"),
        }
    )
    retry_context["attempts"] = attempts[-10:]
    failure.retry_audit_context = retry_context


def _apply_retry_decision(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    failure: ExecutionFailure,
    classification: RejectClassification,
    observed_at: datetime,
    payload: dict[str, object],
) -> None:
    plan_item = order.plan_item
    root_order_pk = _root_broker_order_pk(order)
    retry_attempt_count = _retry_order_count_for_root(
        settings=settings,
        db=db,
        root_order_pk=root_order_pk,
        plan_item=plan_item,
    )
    audit_context = _retry_audit_context(
        order=order,
        classification=classification,
        payload=payload,
        root_order_pk=root_order_pk,
        retry_attempt_count=retry_attempt_count,
    )
    failure.retry_attempt_count = retry_attempt_count
    failure.retry_max_attempts = RETRY_MAX_ATTEMPTS_PER_ROOT_ORDER
    failure.retry_audit_context = audit_context
    failure.retry_decision_source = "reconciliation"
    existing_retry_job = (
        db.get(ExecutionJob, failure.retry_job_id)
        if failure.retry_job_id
        else None
    )
    if existing_retry_job is not None and existing_retry_job.status == "completed":
        if failure.retry_decision_status not in {"submitted", "completed"}:
            failure.retry_decision_status = "submitted"
        return

    skip_reason = _retry_skip_reason(
        order=order,
        plan_item=plan_item,
        classification=classification,
        retry_attempt_count=retry_attempt_count,
        observed_at=observed_at,
    )
    if skip_reason is not None:
        failure.retry_decision_status = (
            "exhausted"
            if skip_reason == "max_retry_attempts_exhausted"
            else "skipped"
        )
        failure.retry_decision_reason = skip_reason
        failure.retry_next_at = None
        failure.retry_deadline_at = None
        failure.retry_exhausted_reason = (
            skip_reason if failure.retry_decision_status == "exhausted" else None
        )
        return

    next_retry_at = classification.next_retry_at or _fallback_next_retry_at(
        observed_at=observed_at,
        classification=classification,
    )
    deadline_at = observed_at + RETRY_DEADLINE
    pending_existing_retry_job = (
        existing_retry_job is not None
        and existing_retry_job.status in {"queued", "scheduled", "retrying"}
    )
    failure.retry_decision_status = "scheduled"
    if pending_existing_retry_job:
        assert existing_retry_job is not None
        preserved_next_retry_at = (
            failure.retry_next_at
            or existing_retry_job.scheduled_for
            or next_retry_at
        )
        failure.retry_decision_reason = (
            failure.retry_decision_reason or classification.reason
        )
        failure.retry_next_at = preserved_next_retry_at
        failure.retry_deadline_at = failure.retry_deadline_at or deadline_at
        if existing_retry_job.scheduled_for is None:
            existing_retry_job.scheduled_for = preserved_next_retry_at
        if existing_retry_job.last_error is None:
            existing_retry_job.last_error = f"retry scheduled after {classification.reason}"
    else:
        failure.retry_decision_reason = classification.reason
        failure.retry_next_at = next_retry_at
        failure.retry_deadline_at = deadline_at
    failure.retry_exhausted_reason = None
    if failure.retry_job_id is None:
        assert plan_item is not None
        retry_job = ExecutionJob(
            id=new_id(),
            execution_plan_item_id=plan_item.id,
            job_type=BROKER_ORDER_RETRY_JOB_TYPE,
            status="scheduled",
            scheduled_for=next_retry_at,
            last_error=f"retry scheduled after {classification.reason}",
        )
        db.add(retry_job)
        db.flush()
        failure.retry_job_id = retry_job.id
    apply_broker_order_status(order=order, status=NormalizedBrokerOrderStatus.RETRY_PENDING.value)
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="broker_order_retry_pending",
            source="rejected_order_retry",
            reason=classification.reason,
            payload={
                "broker_order_pk": order.id,
                "idempotency_key": order.idempotency_key,
                "retry_job_id": failure.retry_job_id,
                "retry_next_at": _serialize_datetime(failure.retry_next_at),
            },
            occurred_at=observed_at,
        )


def _upsert_rejected_order_failure(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    classification: RejectClassification,
    observed_at: datetime,
    payload: dict[str, object],
) -> ExecutionFailure:
    failure = db.scalar(
        select(ExecutionFailure).where(
            ExecutionFailure.mode == settings.runtime_mode,
            ExecutionFailure.broker_order_pk == order.id,
        )
    )
    plan_item = order.plan_item
    plan = plan_item.plan if plan_item is not None else None
    raw_context = {
        "request_source": "reconciliation",
        "response_metadata": order.response_metadata,
        "request_metadata": order.request_metadata,
        "raw_broker_payload": order.raw_broker_payload,
        "reconcile_payload": _json_safe_value(payload),
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
            decision_actor=(
                plan_item.decision_agent_name
                if plan_item is not None
                else None
            ),
            broker_name=order.broker_name,
            account=_account_identifier(settings=settings, order=order),
            idempotency_key=order.idempotency_key,
            symbol=order.symbol,
            side=order.side,
            intent=plan_item.intent if plan_item is not None else None,
            requested_quantity=order.quantity or 0,
            requested_limit_price=order.limit_price,
            requested_notional=(
                order.quote_notional
                or (
                    order.base_quantity * order.limit_price
                    if order.base_quantity is not None and order.limit_price is not None
                    else None
                )
                or _requested_notional(order.quantity, order.limit_price)
            ),
            classification=classification.classification,
            broker_error_classification=classification.classification,
            broker_error_code=classification.code,
            broker_error_message=classification.message,
            retryable=classification.retryable,
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
    failure.decision_actor = (
        plan_item.decision_agent_name
        if plan_item is not None and plan_item.decision_agent_name is not None
        else failure.decision_actor
    )
    failure.account = _account_identifier(settings=settings, order=order) or failure.account
    failure.intent = plan_item.intent if plan_item is not None else failure.intent
    failure.requested_quantity = order.quantity or 0
    failure.requested_limit_price = order.limit_price
    failure.requested_notional = (
        order.quote_notional
        or (
            order.base_quantity * order.limit_price
            if order.base_quantity is not None and order.limit_price is not None
            else None
        )
        or _requested_notional(order.quantity, order.limit_price)
    )
    failure.classification = classification.classification
    failure.broker_error_classification = classification.classification
    failure.broker_error_code = classification.code
    failure.broker_error_message = classification.message
    failure.retryable = classification.retryable
    failure.latest_failed_at = observed_at
    failure.raw_context = raw_context
    return failure


def _retry_skip_reason(
    *,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem | None,
    classification: RejectClassification,
    retry_attempt_count: int,
    observed_at: datetime,
) -> str | None:
    if not classification.retryable:
        return classification.reason
    if plan_item is None:
        return "missing_execution_plan_item"
    if retry_attempt_count >= RETRY_MAX_ATTEMPTS_PER_ROOT_ORDER:
        return "max_retry_attempts_exhausted"
    if order.side == "buy" and _buy_signal_expired(plan_item=plan_item, at=observed_at):
        return "expired_buy_signal"
    if not _plan_item_allows_retry(plan_item=plan_item, order=order, at=observed_at):
        return "inactive_strategy_intent"
    return None


def _plan_item_allows_retry(
    *,
    plan_item: ExecutionPlanItem,
    order: BrokerOrder,
    at: datetime,
) -> bool:
    if plan_item.status in NO_RETRY_PLAN_ITEM_STATUSES:
        return False
    if order.side == "buy" and _buy_signal_expired(plan_item=plan_item, at=at):
        return False
    if plan_item.status in RETRY_COMPATIBLE_PLAN_ITEM_STATUSES:
        return True
    return plan_item.status.startswith("broker_order_") or plan_item.status.endswith(
        "_pending_dispatch"
    )


def _buy_signal_expired(*, plan_item: ExecutionPlanItem, at: datetime) -> bool:
    candidates: list[object] = []
    for payload in (plan_item.risk_context, plan_item.quantity_policy):
        if not isinstance(payload, dict):
            continue
        for key in BUY_EXPIRY_KEYS:
            candidates.append(payload.get(key))
        watch = payload.get("watch_buy_monitoring")
        if isinstance(watch, dict):
            for key in BUY_EXPIRY_KEYS:
                candidates.append(watch.get(key))
        decision = payload.get("exec_agent_decision")
        if isinstance(decision, dict):
            for key in BUY_EXPIRY_KEYS:
                candidates.append(decision.get(key))
    for value in candidates:
        expires_at = _parse_datetime(value)
        if expires_at is not None and _normalize_utc(at) >= expires_at:
            return True
    return False


def _build_retry_command_authority(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    source_order: BrokerOrder,
    plan_item: ExecutionPlanItem,
    idempotency_key: str,
) -> _RetryCommandAuthority:
    metadata = (
        source_order.request_metadata
        if isinstance(source_order.request_metadata, dict)
        else {}
    )
    execution_scope = _validated_retry_execution_scope(
        metadata.get("execution_scope")
    )
    provider = str(adapter.provider or "").strip().lower()
    if (
        not provider
        or source_order.mode != settings.runtime_mode
        or str(source_order.broker_name or "").strip().lower() != provider
        or execution_scope["provider"] != provider
    ):
        raise _RetryCommandIntegrityError("retry_provider_scope_mismatch")

    command = _load_canonical_source_command(db=db, order=source_order)
    _validate_retry_command_binding(
        command=command,
        order=source_order,
        plan_item=plan_item,
        metadata=metadata,
        execution_scope=execution_scope,
    )
    return _RetryCommandAuthority(
        command=command.model_copy(
            update={
                "idempotency_key": idempotency_key,
                "execution_plan_item_id": plan_item.id,
            }
        ),
        execution_scope=execution_scope,
    )


def _validated_retry_execution_scope(value: object) -> dict[str, object]:
    if (
        not isinstance(value, dict)
        or set(value) != _CANONICAL_EXECUTION_SCOPE_KEYS
        or value.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION
    ):
        raise _RetryCommandIntegrityError("retry_execution_scope_invalid")
    provider = value.get("provider")
    account_scope = value.get("account_scope")
    venue = value.get("venue")
    asset_class = value.get("asset_class")
    exchange = value.get("exchange")
    if (
        not isinstance(provider, str)
        or not provider
        or provider != provider.strip().lower()
        or not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
        or not isinstance(venue, str)
        or not venue
        or venue != venue.strip().upper()
        or not isinstance(asset_class, str)
        or not asset_class
        or asset_class != asset_class.strip().lower()
        or not isinstance(exchange, str)
        or not exchange
        or exchange != exchange.strip().upper()
    ):
        raise _RetryCommandIntegrityError("retry_execution_scope_invalid")
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": provider,
        "account_scope": account_scope,
        "venue": venue,
        "asset_class": asset_class,
        "exchange": exchange,
    }


def _load_canonical_source_command(
    *,
    db: Session,
    order: BrokerOrder,
) -> PlaceOrderRequest:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    final_command = metadata.get("final_broker_command")
    events = db.scalars(
        select(BrokerOrderEvent)
        .where(BrokerOrderEvent.broker_order_pk == order.id)
        .order_by(BrokerOrderEvent.observed_at.asc(), BrokerOrderEvent.id.asc())
    ).all()
    final_event_commands = [
        event.request_payload
        for event in events
        if event.event_type == "submission_price_cap_bound"
        and isinstance(event.request_payload, dict)
    ]
    submit_commands = [
        event.request_payload
        for event in events
        if event.event_type == "submit_requested"
        and isinstance(event.request_payload, dict)
    ]
    if isinstance(final_command, dict):
        command_payloads = [final_command, *final_event_commands]
    elif final_event_commands:
        command_payloads = final_event_commands
    else:
        command_payloads = submit_commands
    if not command_payloads:
        raise _RetryCommandIntegrityError("retry_canonical_command_missing")

    try:
        commands = [PlaceOrderRequest.model_validate(payload) for payload in command_payloads]
    except ValidationError as exc:
        raise _RetryCommandIntegrityError("retry_canonical_command_invalid") from exc
    canonical_payload = commands[0].model_dump(mode="json")
    if any(
        command.model_dump(mode="json") != canonical_payload
        for command in commands[1:]
    ):
        raise _RetryCommandIntegrityError("retry_canonical_command_ambiguous")
    return commands[0]


def _validate_retry_command_binding(
    *,
    command: PlaceOrderRequest,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem,
    metadata: dict[str, object],
    execution_scope: dict[str, object],
) -> None:
    scope_route = (
        execution_scope["provider"],
        execution_scope["venue"],
        execution_scope["asset_class"],
        execution_scope["exchange"],
    )
    command_route = (
        command.provider,
        str(command.venue or "").strip().upper(),
        command.asset_class,
        command.exchange,
    )
    order_route = (
        str(order.broker_name or "").strip().lower(),
        str(order.venue or "").strip().upper(),
        str(order.asset_class or "").strip().lower(),
        _metadata_exchange(metadata),
    )
    if command_route != scope_route or order_route != scope_route:
        raise _RetryCommandIntegrityError("retry_route_scope_mismatch")
    if (
        command.idempotency_key != order.idempotency_key
        or command.execution_plan_item_id != plan_item.id
        or command.symbol != order.symbol
        or command.side != order.side
        or command.quantity != order.quantity
        or command.base_quantity != order.base_quantity
        or command.quote_notional != order.quote_notional
        or command.quantity_precision != order.quantity_precision
        or command.price_precision != order.price_precision
        or command.limit_price != order.limit_price
        or command.order_type
        != ("limit" if order.limit_price is not None else "market")
    ):
        raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    if (
        plan_item.symbol != command.symbol
        or plan_item.side != command.side
        or (
            plan_item.provider is not None
            and str(plan_item.provider).strip().lower() != command.provider
        )
        or (
            plan_item.venue is not None
            and str(plan_item.venue).strip().upper()
            != str(command.venue or "").strip().upper()
        )
        or (
            plan_item.asset_class is not None
            and str(plan_item.asset_class).strip().lower() != command.asset_class
        )
    ):
        raise _RetryCommandIntegrityError("retry_plan_item_scope_mismatch")
    if execution_scope["provider"] != "kis" and (
        plan_item.provider is None
        or plan_item.venue is None
        or plan_item.asset_class is None
    ):
        raise _RetryCommandIntegrityError("retry_plan_item_scope_incomplete")

    time_in_force = metadata.get("time_in_force")
    if (
        not isinstance(time_in_force, str)
        or time_in_force.strip().lower() != command.time_in_force
    ):
        raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    for key, expected in (
        ("provider", command.provider),
        ("venue", command.venue),
        ("asset_class", command.asset_class),
    ):
        value = metadata.get(key)
        if not isinstance(value, str) or value.strip().lower() != str(expected).lower():
            raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    route = metadata.get("route")
    if isinstance(route, dict):
        route_exchange = route.get("exchange")
        if (
            not isinstance(route_exchange, str)
            or route_exchange.strip().upper() != command.exchange
        ):
            raise _RetryCommandIntegrityError("retry_route_scope_mismatch")
        effective_order_type = route.get("effective_order_type")
        if effective_order_type is not None and (
            not isinstance(effective_order_type, str)
            or effective_order_type.strip().lower() != command.order_type
        ):
            raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    _validate_retry_sizing_metadata(metadata.get("sizing"), command=command)


def _validate_existing_retry_order_binding(
    *,
    order: BrokerOrder,
    source_order: BrokerOrder,
    failure: ExecutionFailure,
    job: ExecutionJob,
    retry_authority: _RetryCommandAuthority,
) -> None:
    command = retry_authority.command
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    try:
        execution_scope = _validated_retry_execution_scope(
            metadata.get("execution_scope")
        )
        sizing_matches = _retry_sizing_metadata_matches(
            metadata.get("sizing"),
            command=command,
        )
    except _RetryCommandIntegrityError as exc:
        raise _RetryCommandIntegrityError(
            "retry_existing_order_scope_conflict"
        ) from exc
    if (
        execution_scope != retry_authority.execution_scope
        or not _order_matches_execution_scope(
            order=order,
            metadata=metadata,
            execution_scope=execution_scope,
        )
        or not sizing_matches
        or order.idempotency_key != command.idempotency_key
        or order.execution_plan_item_id != command.execution_plan_item_id
        or order.symbol != command.symbol
        or order.side != command.side
        or order.quantity != command.quantity
        or order.base_quantity != command.base_quantity
        or order.quote_notional != command.quote_notional
        or order.quantity_precision != command.quantity_precision
        or order.price_precision != command.price_precision
        or order.limit_price != command.limit_price
        or metadata.get("time_in_force") != command.time_in_force
    ):
        raise _RetryCommandIntegrityError(
            "retry_existing_order_scope_conflict"
        )
    retry_metadata = _retry_metadata(order)
    if (
        retry_metadata.get("source_broker_order_pk") != source_order.id
        or retry_metadata.get("root_broker_order_pk")
        != _root_broker_order_pk(source_order)
        or retry_metadata.get("execution_failure_id") != failure.id
        or retry_metadata.get("retry_job_id") != job.id
    ):
        raise _RetryCommandIntegrityError(
            "retry_existing_order_scope_conflict"
        )


def _metadata_exchange(metadata: dict[str, object]) -> str:
    direct = metadata.get("exchange")
    if isinstance(direct, str) and direct.strip():
        return direct.strip().upper()
    route = metadata.get("route")
    if isinstance(route, dict):
        exchange = route.get("exchange")
        if isinstance(exchange, str) and exchange.strip():
            return exchange.strip().upper()
    return ""


def _validate_retry_sizing_metadata(
    value: object,
    *,
    command: PlaceOrderRequest,
) -> None:
    if not isinstance(value, dict):
        raise _RetryCommandIntegrityError("retry_canonical_command_incomplete")
    if value.get("quantity") != command.quantity:
        raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    for key, expected in (
        ("base_quantity", command.base_quantity),
        ("quote_notional", command.quote_notional),
        ("limit_price", command.limit_price),
    ):
        if _optional_decimal(value.get(key)) != expected:
            raise _RetryCommandIntegrityError("retry_canonical_command_conflict")
    if (
        value.get("quantity_precision") != command.quantity_precision
        or value.get("price_precision") != command.price_precision
    ):
        raise _RetryCommandIntegrityError("retry_canonical_command_conflict")


def _retry_sizing_metadata_matches(
    value: object,
    *,
    command: PlaceOrderRequest,
) -> bool:
    try:
        _validate_retry_sizing_metadata(value, command=command)
    except _RetryCommandIntegrityError:
        return False
    return True


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise _RetryCommandIntegrityError("retry_canonical_command_invalid")
    try:
        parsed = Decimal(str(value).strip())
    except (DecimalException, TypeError, ValueError) as exc:
        raise _RetryCommandIntegrityError("retry_canonical_command_invalid") from exc
    if not parsed.is_finite():
        raise _RetryCommandIntegrityError("retry_canonical_command_invalid")
    return parsed


def _validate_account_state_for_retry(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object],
    observed_at: datetime,
) -> None:
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="rejected_order_retry",
            reason=f"revalidate account before retrying {order.idempotency_key}",
        )
    except Exception as exc:
        raise RetryOrderGuardError(
            "retry deferred because broker authentication is temporarily unavailable",
            job_status="retrying",
            plan_item_status="broker_order_retry_pending",
        ) from exc
    access_token = session_state.access_token or ""
    provider = str(adapter.provider or "").strip().lower()
    broker_account_id: str | None = None
    if provider == "alpaca":
        account_id_loader = getattr(adapter, "get_broker_account_id", None)
        if not callable(account_id_loader):
            raise _RetryCommandIntegrityError(
                "retry_authenticated_account_identity_unavailable"
            )
        try:
            loaded_account_id = account_id_loader(access_token)
        except Exception as exc:
            raise RetryOrderGuardError(
                "retry deferred because authenticated account identity is temporarily unavailable",
                job_status="retrying",
                plan_item_status="broker_order_retry_pending",
            ) from exc
        broker_account_id = (
            loaded_account_id.strip()
            if isinstance(loaded_account_id, str) and loaded_account_id.strip()
            else None
        )
        if broker_account_id is None:
            raise _RetryCommandIntegrityError(
                "retry_authenticated_account_identity_unavailable"
            )
    try:
        current_identity = (
            build_authenticated_broker_position_identity(
                settings=settings,
                provider=provider,
                exchange=command.exchange,
                broker_account_id=broker_account_id or "",
            )
            if provider == "alpaca"
            else build_position_identity(
                settings=settings,
                provider=provider,
                exchange=command.exchange,
            )
        )
    except PositionIdentityConflictError as exc:
        raise _RetryCommandIntegrityError(
            "retry_authenticated_account_scope_unavailable"
        ) from exc
    if (
        execution_scope["provider"] != current_identity.provider
        or execution_scope["account_scope"] != current_identity.account_scope
        or execution_scope["exchange"] != current_identity.exchange
    ):
        raise _RetryCommandIntegrityError(
            "retry_authenticated_account_scope_mismatch"
        )

    try:
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=[command.symbol],
            exchange=command.exchange,
        )
    except Exception as exc:
        raise RetryOrderGuardError(
            "retry deferred because the broker account snapshot is temporarily unavailable",
            job_status="retrying",
            plan_item_status="broker_order_retry_pending",
        ) from exc
    snapshot_account_id = str(snapshot.broker_account_id or "").strip()
    if (
        provider == "alpaca"
        and snapshot_account_id
        and snapshot_account_id != broker_account_id
    ):
        raise _RetryCommandIntegrityError(
            "retry_authenticated_account_scope_mismatch"
        )
    _validate_snapshot_holdings_coverage(snapshot=snapshot)
    _validate_snapshot_open_order_coverage(
        snapshot=snapshot,
        provider=provider,
    )
    if _has_active_snapshot_open_order(snapshot=snapshot, order=order):
        raise RetryOrderGuardError(
            "retry skipped because broker snapshot has an active duplicate "
            f"{order.side} order",
            plan_item_status="broker_order_retry_pending",
        )
    if order.side == "sell":
        holding = _holding_for_symbol(snapshot=snapshot, symbol=order.symbol)
        available_quantity = _sellable_decimal(holding)
        requested_quantity = _command_base_quantity(command)
        if (
            available_quantity is None
            or requested_quantity is None
            or available_quantity < requested_quantity
        ):
            raise RetryOrderGuardError(
                "retry skipped because current sellable quantity is insufficient",
                plan_item_status="broker_order_failed",
            )
        return

    if order.side == "buy":
        symbol_power = snapshot.symbol_buying_power.get(order.symbol)
        if (
            command.quantity is not None
            and symbol_power is not None
            and symbol_power.buyable_quantity is not None
        ):
            if symbol_power.buyable_quantity < command.quantity:
                raise RetryOrderGuardError(
                    "retry skipped because current buyable quantity is insufficient",
                    plan_item_status="broker_order_failed",
                )
        notional = _command_notional(command)
        available_cash = _minimum_decimal(snapshot.available_cash, snapshot.buying_power)
        if notional is not None and available_cash is not None and available_cash < notional:
            raise RetryOrderGuardError(
                "retry skipped because current cash/buying power is insufficient",
                plan_item_status="broker_order_failed",
            )
        return

    raise RetryOrderGuardError(
        f"retry side {order.side!r} is not supported",
        plan_item_status="broker_order_failed",
    )


def _record_retry_decision_event(
    *,
    order: BrokerOrder,
    failure: ExecutionFailure,
    classification: RejectClassification,
    payload: dict[str, object],
    observed_at: datetime,
) -> None:
    order.events.append(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="retry_decision",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=order.status,
            broker_status=classification.code,
            error_classification=classification.classification,
            retryable=classification.retryable,
            message=failure.retry_decision_reason,
            response_payload=_json_safe_dict(payload),
            payload={
                "execution_failure_id": failure.id,
                "decision_status": failure.retry_decision_status,
                "decision_reason": failure.retry_decision_reason,
                "next_retry_at": _serialize_datetime(failure.retry_next_at),
                "retry_deadline_at": _serialize_datetime(failure.retry_deadline_at),
                "retry_job_id": failure.retry_job_id,
                "retry_attempt_count": failure.retry_attempt_count,
                "retry_max_attempts": failure.retry_max_attempts,
                "market_state": classification.market_state,
                "source": "reconciliation",
            },
            observed_at=observed_at,
        )
    )


def _load_failure_for_retry_job(
    *,
    settings: Settings,
    db: Session,
    job: ExecutionJob,
) -> ExecutionFailure | None:
    return db.scalar(
        select(ExecutionFailure)
        .where(
            ExecutionFailure.mode == settings.runtime_mode,
            ExecutionFailure.retry_job_id == job.id,
        )
        .options(selectinload(ExecutionFailure.broker_order))
    )


def _load_order_by_idempotency_key(
    *,
    settings: Settings,
    db: Session,
    idempotency_key: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == idempotency_key,
        )
    )


def _active_duplicate_order(
    *,
    settings: Settings,
    db: Session,
    source_order: BrokerOrder,
    expected_idempotency_key: str,
    execution_scope: dict[str, object],
) -> BrokerOrder | None:
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol == source_order.symbol,
            BrokerOrder.side == source_order.side,
            BrokerOrder.status.in_(tuple(DUPLICATE_BLOCKING_ORDER_STATUSES)),
        )
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
    ).all()
    exact_duplicate: BrokerOrder | None = None
    for order in orders:
        if order.id == source_order.id:
            continue
        if order.idempotency_key == expected_idempotency_key:
            continue
        candidate_provider = str(order.broker_name or "").strip().lower()
        if candidate_provider != execution_scope["provider"]:
            continue
        metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
        try:
            candidate_scope = _validated_retry_execution_scope(
                metadata.get("execution_scope")
            )
        except _RetryCommandIntegrityError as exc:
            raise _RetryCommandIntegrityError(
                "retry_duplicate_order_scope_invalid"
            ) from exc
        if not _order_matches_execution_scope(
            order=order,
            metadata=metadata,
            execution_scope=candidate_scope,
        ):
            raise _RetryCommandIntegrityError(
                "retry_duplicate_order_scope_invalid"
            )
        if candidate_scope != execution_scope:
            continue
        if exact_duplicate is None:
            exact_duplicate = order
    return exact_duplicate


def _order_matches_execution_scope(
    *,
    order: BrokerOrder,
    metadata: dict[str, object],
    execution_scope: dict[str, object],
) -> bool:
    if (
        str(order.broker_name or "").strip().lower()
        != execution_scope["provider"]
        or str(order.venue or "").strip().upper()
        != execution_scope["venue"]
        or str(order.asset_class or "").strip().lower()
        != execution_scope["asset_class"]
    ):
        return False
    for key, normalization in (
        ("provider", str.lower),
        ("venue", str.upper),
        ("asset_class", str.lower),
        ("exchange", str.upper),
    ):
        if key not in metadata:
            continue
        value = metadata.get(key)
        if (
            not isinstance(value, str)
            or normalization(value.strip()) != execution_scope[key]
        ):
            return False
    route = metadata.get("route")
    if isinstance(route, dict) and "exchange" in route:
        route_exchange = route.get("exchange")
        if (
            not isinstance(route_exchange, str)
            or route_exchange.strip().upper() != execution_scope["exchange"]
        ):
            return False
    return True


def _retry_order_count_for_root(
    *,
    settings: Settings,
    db: Session,
    root_order_pk: str,
    plan_item: ExecutionPlanItem | None,
) -> int:
    if plan_item is None:
        return 0
    orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.execution_plan_item_id == plan_item.id,
        )
    ).all()
    return sum(
        1
        for order in orders
        if _retry_metadata(order).get("root_broker_order_pk") == root_order_pk
    )


def _retry_audit_context(
    *,
    order: BrokerOrder,
    classification: RejectClassification,
    payload: dict[str, object],
    root_order_pk: str,
    retry_attempt_count: int,
) -> dict[str, object]:
    existing_attempts = []
    if order.execution_failure is not None and isinstance(
        order.execution_failure.retry_audit_context,
        dict,
    ):
        attempts = order.execution_failure.retry_audit_context.get("attempts")
        if isinstance(attempts, list):
            existing_attempts = attempts[-10:]
    return {
        "root_broker_order_pk": root_order_pk,
        "source_broker_order_pk": order.id,
        "source_broker_order_id": order.broker_order_id,
        "source_idempotency_key": order.idempotency_key,
        "classification_source": classification.source,
        "classification_reason": classification.reason,
        "temporary_reason": classification.temporary_reason,
        "market_state": classification.market_state,
        "retry_attempt_count": retry_attempt_count,
        "reconcile_payload": _json_safe_value(payload),
        "attempts": existing_attempts,
    }


def _failure_decision_signature(failure: ExecutionFailure) -> tuple[object, ...]:
    return (
        failure.retry_decision_status,
        failure.retry_decision_reason,
        _serialize_datetime(failure.retry_next_at),
        failure.retry_job_id,
        failure.retry_attempt_count,
    )


def _mark_failure_retry_skipped(
    *,
    failure: ExecutionFailure,
    reason: str,
    recorded_at: datetime,
    context: dict[str, object] | None = None,
) -> None:
    failure.retry_decision_status = "skipped"
    failure.retry_decision_reason = reason
    failure.retry_next_at = None
    failure.retry_deadline_at = None
    retry_context = dict(failure.retry_audit_context or {})
    retry_context["latest_skip"] = {
        "reason": reason,
        "recorded_at": recorded_at.isoformat(),
        **(context or {}),
    }
    failure.retry_audit_context = retry_context


def _quarantine_retry_command_integrity(
    *,
    db: Session,
    order: BrokerOrder,
    failure: ExecutionFailure,
    reason: str,
    recorded_at: datetime,
) -> None:
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
    )
    order.last_error_classification = "retry_command_integrity"
    order.last_error_code = reason
    order.last_error_message = (
        "rejected-order retry was blocked because its immutable command or "
        "execution scope could not be proven; manual review is required"
    )
    order.last_error_retryable = False
    failure.retry_decision_status = "manual_review_required"
    failure.retry_decision_reason = reason
    failure.retry_next_at = None
    failure.retry_deadline_at = None
    failure.retry_exhausted_reason = reason
    retry_context = dict(failure.retry_audit_context or {})
    retry_context["integrity_quarantine"] = {
        "reason": reason,
        "recorded_at": recorded_at.isoformat(),
        "source_broker_order_pk": order.id,
    }
    failure.retry_audit_context = retry_context
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="retry_command_integrity_blocked",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=order.status,
            error_classification="retry_command_integrity",
            retryable=False,
            message=order.last_error_message,
            payload={
                "reason": reason,
                "manual_review_required": True,
                "execution_failure_id": failure.id,
            },
            observed_at=recorded_at,
        )
    )
    if order.plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=order.plan_item,
            status="manual_review_required",
            source="rejected_order_retry",
            reason=reason,
            payload={
                "broker_order_pk": order.id,
                "execution_failure_id": failure.id,
                "manual_review_required": True,
            },
            occurred_at=recorded_at,
        )
    db.flush()


def _mark_failure_retry_exhausted(
    *,
    failure: ExecutionFailure,
    reason: str,
    recorded_at: datetime,
) -> None:
    failure.retry_decision_status = "exhausted"
    failure.retry_decision_reason = reason
    failure.retry_exhausted_reason = reason
    failure.retry_next_at = None
    retry_context = dict(failure.retry_audit_context or {})
    retry_context["latest_exhaustion"] = {
        "reason": reason,
        "recorded_at": recorded_at.isoformat(),
    }
    failure.retry_audit_context = retry_context


def _fallback_next_retry_at(
    *,
    observed_at: datetime,
    classification: RejectClassification,
) -> datetime:
    delay = TEMPORARY_RETRY_DELAYS.get(
        classification.temporary_reason or "",
        GENERIC_TEMPORARY_RETRY_DELAY,
    )
    return observed_at + delay


def _temporary_reason(
    *,
    code: str | None,
    message: str,
    market_state: dict[str, Any] | None,
) -> str | None:
    if code is not None:
        normalized_code = code.strip().upper()
        if normalized_code in TEMPORARY_REJECT_CODES:
            return _temporary_reason_from_text(normalized_code) or "orderability_window"
    state_text = _flatten_text(market_state)
    combined = f"{message} {state_text}"
    reason = _temporary_reason_from_text(combined)
    if reason is not None:
        return reason
    if market_state is not None and _has_structured_temporary_orderability(market_state):
        return "orderability_window"
    return None


def _temporary_reason_from_text(value: str) -> str | None:
    normalized = value.strip().lower()
    if not normalized:
        return None
    tokens = set(normalized.replace("_", " ").replace("-", " ").split())
    if "circuit" in normalized or "서킷브레이커" in normalized:
        return "circuit_breaker"
    if "sidecar" in normalized or "사이드카" in normalized:
        return "sidecar"
    if "volatility" in normalized or "vi" in tokens or "변동성완화" in normalized:
        return "volatility_interruption"
    if "single price" in normalized or "단일가" in normalized:
        return "single_price"
    if any(marker.lower() in normalized for marker in TEMPORARY_REJECT_MARKERS):
        return "orderability_window"
    return None


def _has_structured_temporary_orderability(market_state: dict[str, Any]) -> bool:
    for key in ("next_retry_at", "reopen_at", "expected_reopen_at", "orderable_after"):
        if _parse_datetime(market_state.get(key)) is not None:
            return True
    for key in (
        "halted",
        "trading_halt",
        "sidecar_active",
        "circuit_breaker_active",
        "vi_active",
        "single_price_active",
    ):
        value = market_state.get(key)
        if value is True:
            return True
    orderable = market_state.get("normal_ordering_available")
    if orderable is False:
        return True
    return False


def _next_retry_at_from_payload(
    payload: dict[str, object],
    market_state: dict[str, Any] | None,
) -> datetime | None:
    candidates: list[object] = []
    for source in (payload, market_state or {}):
        for key in (
            "next_retry_at",
            "retry_after_at",
            "reopen_at",
            "expected_reopen_at",
            "orderable_after",
        ):
            candidates.append(source.get(key))
    for candidate in candidates:
        parsed = _parse_datetime(candidate)
        if parsed is not None:
            return parsed
    return None


def _market_state(payload: dict[str, object]) -> dict[str, Any] | None:
    candidates = [
        payload.get("market_state"),
        payload.get("market_operation_state"),
        payload.get("orderability"),
        payload.get("market_context"),
    ]
    raw_payload = payload.get("payload")
    if isinstance(raw_payload, dict):
        candidates.extend(
            [
                raw_payload.get("market_state"),
                raw_payload.get("market_operation_state"),
                raw_payload.get("orderability"),
                raw_payload.get("market_context"),
            ]
        )
    for candidate in candidates:
        if isinstance(candidate, dict):
            return _json_safe_dict(candidate)
    return None


def _reject_message(payload: dict[str, object]) -> str:
    parts = [
        _first_text(
            payload,
            "reject_reason_message",
            "reject_message",
            "reason_message",
            "message",
            "msg1",
            "broker_status",
        )
    ]
    raw_payload = payload.get("payload")
    if isinstance(raw_payload, dict):
        parts.append(
            _first_text(
                raw_payload,
                "reject_reason_message",
                "reject_message",
                "reason_message",
                "msg1",
                "message",
                "ord_st_dvsn_cd_name",
                "sll_buy_dvsn_cd_name",
            )
        )
    message = " ".join(part for part in parts if part)
    return message or "broker order rejected during reconciliation"


def _first_text(payload: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raw_payload = payload.get("payload")
    if isinstance(raw_payload, dict):
        for key in keys:
            value = raw_payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _flatten_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(item) for item in value.values())
    if isinstance(value, list | tuple):
        return " ".join(_flatten_text(item) for item in value)
    return str(value)


def _root_broker_order_pk(order: BrokerOrder) -> str:
    retry = _retry_metadata(order)
    root = retry.get("root_broker_order_pk")
    if isinstance(root, str) and root:
        return root
    return order.id


def _retry_metadata(order: BrokerOrder) -> dict[str, object]:
    for payload in (order.request_metadata, order.response_metadata, order.raw_broker_payload):
        if not isinstance(payload, dict):
            continue
        direct = payload.get("retry")
        if isinstance(direct, dict):
            return direct
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            retry = metadata.get("retry")
            if isinstance(retry, dict):
                return retry
    return {}


def _retry_idempotency_key(*, order: BrokerOrder, job: ExecutionJob) -> str:
    return f"retry:{_root_broker_order_pk(order)}:{job.id}"


def _holding_for_symbol(
    *,
    snapshot: BrokerAccountSnapshot,
    symbol: str,
) -> BrokerAccountHolding | None:
    for holding in snapshot.holdings:
        if holding.symbol == symbol:
            return holding
    return None


def _sellable_decimal(holding: BrokerAccountHolding | None) -> Decimal | None:
    if holding is None:
        return None
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    for key in ("sellable_base_quantity", "base_quantity", "balance"):
        if key not in payload:
            continue
        try:
            quantity = Decimal(str(payload[key]).strip())
        except (DecimalException, TypeError, ValueError):
            return None
        if not quantity.is_finite() or quantity < 0:
            return None
        return quantity
    if holding.sellable_quantity is not None:
        return Decimal(holding.sellable_quantity)
    return Decimal(holding.quantity)


def _command_base_quantity(command: PlaceOrderRequest) -> Decimal | None:
    if command.base_quantity is not None:
        return command.base_quantity
    if command.quantity is not None:
        return Decimal(command.quantity)
    return None


def _command_notional(command: PlaceOrderRequest) -> Decimal | None:
    if command.quote_notional is not None:
        return command.quote_notional
    quantity = _command_base_quantity(command)
    if quantity is None or command.limit_price is None:
        return None
    return quantity * command.limit_price


def _has_active_snapshot_open_order(
    *,
    snapshot: BrokerAccountSnapshot,
    order: BrokerOrder,
) -> bool:
    for open_order in snapshot.open_orders:
        if open_order.symbol != order.symbol or open_order.side != order.side:
            continue
        payload = open_order.payload if isinstance(open_order.payload, dict) else {}
        if "remaining_base_quantity" in payload:
            remaining_base_quantity = _nonnegative_decimal(
                payload.get("remaining_base_quantity")
            )
            if remaining_base_quantity is None:
                raise _RetryCommandIntegrityError(
                    "retry_broker_snapshot_order_invalid"
                )
            if remaining_base_quantity <= 0:
                if (open_order.remaining_quantity or 0) > 0:
                    raise _RetryCommandIntegrityError(
                        "retry_broker_snapshot_order_invalid"
                    )
                continue
            return True
        if (open_order.remaining_quantity or 0) < 0:
            raise _RetryCommandIntegrityError(
                "retry_broker_snapshot_order_invalid"
            )
        if (open_order.remaining_quantity or 0) <= 0:
            continue
        return True
    return False


def _validate_snapshot_open_order_coverage(
    *,
    snapshot: BrokerAccountSnapshot,
    provider: str,
) -> None:
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    open_orders_error = payload.get("open_orders_error")
    degraded = (
        isinstance(open_orders_error, dict)
        and open_orders_error.get("degraded") is True
    )
    if not snapshot.open_orders_complete or degraded or (
        provider == "alpaca"
        and payload.get("open_orders_coverage") != "complete"
    ):
        raise _RetryCommandIntegrityError(
            "retry_broker_open_orders_incomplete"
        )


def _validate_snapshot_holdings_coverage(
    *,
    snapshot: BrokerAccountSnapshot,
) -> None:
    if not snapshot.holdings_complete:
        raise _RetryCommandIntegrityError(
            "retry_broker_holdings_incomplete"
        )


def _nonnegative_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value).strip())
    except (DecimalException, TypeError, ValueError):
        return None
    if not result.is_finite() or result < 0:
        return None
    return result


def _terminal_plan_item_status(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.side == "buy":
        return "watch_expired" if plan_item.status == "watch_expired" else "broker_order_failed"
    return "broker_order_failed"


def _time_in_force(order: BrokerOrder) -> str:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    value = metadata.get("time_in_force")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return "day"


def _exchange(order: BrokerOrder) -> str:
    for payload in (order.request_metadata, order.response_metadata, order.raw_broker_payload):
        exchange = _exchange_from_payload(payload)
        if exchange is not None:
            return exchange
    return "KRX"


def _exchange_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    route = payload.get("route")
    if isinstance(route, dict):
        exchange = _normalize_exchange(route.get("exchange"))
        if exchange is not None:
            return exchange
    exchange = _normalize_exchange(payload.get("exchange"))
    if exchange is not None:
        return exchange
    params = payload.get("params")
    if isinstance(params, dict):
        return _normalize_exchange(params.get("EXCG_ID_DVSN_CD"))
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return _exchange_from_payload(metadata)
    return None


def _normalize_exchange(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if normalized in {"KRX", "NXT", "SOR"}:
        return normalized
    return None


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


def _requested_notional(quantity: int | None, limit_price: Decimal | None) -> Decimal | None:
    if quantity is None or limit_price is None:
        return None
    return Decimal(quantity) * limit_price


def _minimum_decimal(*values: Decimal | None) -> Decimal | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return min(concrete)


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _normalize_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _normalize_utc(parsed)


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_utc(value).isoformat()


def _json_safe_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _json_safe_value(value) for key, value in payload.items()}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return _json_safe_dict(value)
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    return value
