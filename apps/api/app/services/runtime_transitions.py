from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.broker.base import NormalizedBrokerOrderStatus
from app.models.runtime import BrokerOrder, ExecutionJob, ExecutionPlanItem, ExecutionPlanItemAudit

BROKER_ORDER_STATUSES = frozenset(status.value for status in NormalizedBrokerOrderStatus)

EXECUTION_JOB_STATUSES = frozenset(
    {
        "queued",
        "scheduled",
        "running",
        "retrying",
        "completed",
        "failed",
    }
)

PLAN_ITEM_STATUSES = frozenset(
    {
        "accepted",
        "cancel_pending",
        "canceled",
        "pending",
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_open",
        "broker_order_partially_filled",
        "broker_order_failed",
        "broker_order_rejected",
        "broker_order_retry_dispatching",
        "broker_order_retry_pending",
        "crypto_paper_queued",
        "crypto_paper_dispatching",
        "crypto_paper_failed",
        "crypto_paper_retry_pending",
        "crypto_paper_rejected",
        "expired",
        "filled",
        "new_buy_blocked",
        "open",
        "partially_filled",
        "next_open_buy_dispatching",
        "next_open_buy_failed",
        "next_open_buy_pending_configuration",
        "next_open_buy_retry_pending",
        "next_open_buy_retrying",
        "next_open_buy_scheduled",
        "stop_sell_monitoring",
        "watch_buy_dispatching",
        "watch_buy_rejected",
        "watch_buy_retrying",
        "watch_buy_triggered",
        "watch_buy_triggered_blocked",
        "watch_buy_triggered_pending_dispatch",
        "watch_expired",
    }
)

BROKER_ORDER_TO_PLAN_ITEM_STATUS = {
    NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value: "broker_order_submission_pending",
    NormalizedBrokerOrderStatus.SUBMITTED.value: "broker_order_submitted",
    NormalizedBrokerOrderStatus.OPEN.value: "open",
    NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value: "partially_filled",
    NormalizedBrokerOrderStatus.FILLED.value: "filled",
    NormalizedBrokerOrderStatus.REJECTED.value: "broker_order_rejected",
    NormalizedBrokerOrderStatus.RETRY_PENDING.value: "broker_order_retry_pending",
    NormalizedBrokerOrderStatus.FAILED.value: "broker_order_failed",
    NormalizedBrokerOrderStatus.EXPIRED.value: "broker_order_failed",
}


def apply_broker_order_status(
    *,
    order: BrokerOrder,
    status: str,
) -> None:
    _ensure_known_status(
        domain="broker order",
        status=status,
        allowed=BROKER_ORDER_STATUSES,
    )
    order.status = status


def plan_item_status_for_broker_order_status(status: str) -> str | None:
    _ensure_known_status(
        domain="broker order",
        status=status,
        allowed=BROKER_ORDER_STATUSES,
    )
    return BROKER_ORDER_TO_PLAN_ITEM_STATUS.get(status)


def set_plan_item_status(
    *,
    db: Session,
    plan_item: ExecutionPlanItem,
    status: str,
    source: str,
    event_type: str = "execution_plan_item_status_transition",
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> bool:
    _ensure_known_status(
        domain="execution plan item",
        status=status,
        allowed=PLAN_ITEM_STATUSES,
    )
    previous_status = plan_item.status
    if previous_status == status:
        return False

    observed_at = occurred_at or datetime.now(UTC)
    plan_item.status = status
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=plan_item.id,
            event_type=event_type,
            source=source,
            summary={
                "previous_status": previous_status,
                "status": status,
                "reason": reason,
            },
            payload=payload,
            error_message=reason,
            occurred_at=observed_at,
        )
    )
    return True


def set_execution_job_status(
    *,
    db: Session,
    job: ExecutionJob,
    status: str,
    source: str,
    last_error: str | None = None,
    locked_at: datetime | None = None,
    increment_attempts: bool = False,
    plan_item_status: str | None = None,
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> None:
    _ensure_known_status(
        domain="execution job",
        status=status,
        allowed=EXECUTION_JOB_STATUSES,
    )
    previous_status = job.status
    job.status = status
    job.locked_at = locked_at
    job.last_error = last_error
    if increment_attempts:
        job.attempts += 1

    if plan_item_status is not None:
        set_plan_item_status(
            db=db,
            plan_item=job.plan_item,
            status=plan_item_status,
            source=source,
            event_type="execution_job_plan_item_status_transition",
            reason=reason,
            payload={
                "execution_job_id": job.id,
                "job_type": job.job_type,
                "previous_job_status": previous_status,
                "job_status": status,
                "last_error": last_error,
                **(payload or {}),
            },
            occurred_at=occurred_at,
        )


def _ensure_known_status(
    *,
    domain: str,
    status: str,
    allowed: frozenset[str],
) -> None:
    if status not in allowed:
        allowed_statuses = ", ".join(sorted(allowed))
        raise ValueError(f"unknown {domain} status {status!r}; expected one of: {allowed_statuses}")
