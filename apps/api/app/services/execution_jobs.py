from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAdapter,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import ExecutionJob, ExecutionPlan, ExecutionPlanItem
from app.services.broker_orders import place_broker_order
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)

KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
NEXT_OPEN_BUY_JOB_TYPE = "next_open_buy"
DISPATCHABLE_JOB_STATUSES = ("queued", "scheduled", "retrying")


@dataclass(frozen=True)
class DispatchExecutionJobsResult:
    status_code: int
    payload: dict[str, object]


def materialize_execution_jobs(plan: ExecutionPlan) -> None:
    for item in plan.items:
        if item.intent != NEXT_OPEN_BUY_JOB_TYPE:
            continue
        if _next_open_buy_quantity(item) is None or plan.market_date is None or item.side != "buy":
            item.status = "next_open_buy_pending_configuration"
            continue
        item.status = "next_open_buy_scheduled"
        item.jobs.append(
            ExecutionJob(
                job_type=NEXT_OPEN_BUY_JOB_TYPE,
                status="scheduled",
                scheduled_for=_next_market_open_utc(plan.market_date),
            )
        )


def list_execution_jobs(
    *,
    settings: Settings,
    db: Session,
) -> list[dict[str, object]]:
    jobs = db.scalars(
        select(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionJob.plan_item))
        .order_by(ExecutionJob.scheduled_for.asc(), ExecutionJob.created_at.asc()),
    ).all()
    return [serialize_execution_job(job) for job in jobs]


def dispatch_due_execution_jobs(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    now: datetime | None = None,
) -> DispatchExecutionJobsResult:
    triggered_at = _normalize_utc(now or datetime.now(UTC))
    control_state = ensure_runtime_control_state(settings=settings, db=db)
    safety = runtime_safety_payload(control_state)
    if not automated_order_submission_allowed(control_state):
        return DispatchExecutionJobsResult(
            status_code=200,
            payload={
                "blocked": True,
                "block_reason": automated_order_submission_block_reason(control_state),
                "triggered_at": triggered_at.isoformat(),
                "due_job_count": 0,
                "completed_job_count": 0,
                "retrying_job_count": 0,
                "failed_job_count": 0,
                "jobs": [],
                "safety": safety,
            },
        )

    jobs = db.scalars(
        select(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(DISPATCHABLE_JOB_STATUSES),
        )
        .options(
            selectinload(ExecutionJob.plan_item).selectinload(ExecutionPlanItem.plan),
        )
        .order_by(ExecutionJob.scheduled_for.asc(), ExecutionJob.created_at.asc())
    ).all()
    due_jobs = [job for job in jobs if _is_due(job, triggered_at)]

    completed = 0
    retrying = 0
    failed = 0
    serialized_jobs: list[dict[str, object]] = []

    for job in due_jobs:
        _claim_job(db=db, job=job, triggered_at=triggered_at)
        result = _dispatch_job(
            settings=settings,
            db=db,
            adapter=adapter,
            job_id=job.id,
        )
        persisted_job = db.get(ExecutionJob, job.id)
        assert persisted_job is not None
        serialized_jobs.append(serialize_execution_job(persisted_job))
        if result == "completed":
            completed += 1
        elif result == "retrying":
            retrying += 1
        elif result == "failed":
            failed += 1

    return DispatchExecutionJobsResult(
        status_code=200,
        payload={
            "blocked": False,
            "block_reason": None,
            "triggered_at": triggered_at.isoformat(),
            "due_job_count": len(due_jobs),
            "completed_job_count": completed,
            "retrying_job_count": retrying,
            "failed_job_count": failed,
            "jobs": serialized_jobs,
            "safety": safety,
        },
    )


def serialize_execution_job(job: ExecutionJob) -> dict[str, object]:
    plan_item = job.plan_item
    return {
        "execution_job_id": job.id,
        "execution_plan_item_id": job.execution_plan_item_id,
        "job_type": job.job_type,
        "status": job.status,
        "scheduled_for": _serialize_datetime(job.scheduled_for),
        "locked_at": _serialize_datetime(job.locked_at),
        "attempts": job.attempts,
        "last_error": job.last_error,
        "symbol": plan_item.symbol,
        "side": plan_item.side,
        "intent": plan_item.intent,
        "created_at": _serialize_datetime(job.created_at),
        "updated_at": _serialize_datetime(job.updated_at),
    }


def _claim_job(
    *,
    db: Session,
    job: ExecutionJob,
    triggered_at: datetime,
) -> None:
    job.status = "running"
    job.locked_at = triggered_at
    job.attempts += 1
    job.last_error = None
    job.plan_item.status = "next_open_buy_dispatching"
    db.commit()


def _dispatch_job(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job_id: str,
) -> str:
    job = db.get(
        ExecutionJob,
        job_id,
        options=[selectinload(ExecutionJob.plan_item)],
    )
    assert job is not None

    try:
        command = _build_place_order_request(job)
    except ValueError as exc:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="failed",
            last_error=str(exc),
            plan_item_status="next_open_buy_failed",
        )

    try:
        result = place_broker_order(
            settings=settings,
            db=db,
            adapter=adapter,
            command=command,
            request_source=f"execution-job:{job.id}",
        )
    except Exception as exc:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="retrying",
            last_error=f"{exc.__class__.__name__}: {exc}",
            plan_item_status="next_open_buy_retry_pending",
        )

    order_payload = result.payload.get("order")
    order_status = None
    if isinstance(order_payload, dict):
        raw_status = order_payload.get("status")
        if isinstance(raw_status, str):
            order_status = raw_status

    if result.status_code == 409 and "block_reason" in result.payload:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="scheduled",
            last_error=str(result.payload.get("message") or "dispatch blocked by safety controls"),
            plan_item_status=None,
        )
    if result.payload.get("duplicate") is True or order_status in {
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        NormalizedBrokerOrderStatus.FILLED.value,
    }:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="completed",
            last_error=None,
            plan_item_status=None,
        )
    if order_status == NormalizedBrokerOrderStatus.RETRY_PENDING.value:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="retrying",
            last_error=str(result.payload.get("message") or "broker retry requested"),
            plan_item_status="next_open_buy_retry_pending",
        )
    return _finish_job(
        db=db,
        job_id=job_id,
        status="failed",
        last_error=str(result.payload.get("message") or "broker order submission failed"),
        plan_item_status="next_open_buy_failed",
    )


def _finish_job(
    *,
    db: Session,
    job_id: str,
    status: str,
    last_error: str | None,
    plan_item_status: str | None,
) -> str:
    job = db.get(
        ExecutionJob,
        job_id,
        options=[selectinload(ExecutionJob.plan_item)],
    )
    assert job is not None
    job.status = status
    job.locked_at = None
    job.last_error = last_error
    if plan_item_status is not None:
        job.plan_item.status = plan_item_status
    db.commit()
    return status


def _build_place_order_request(job: ExecutionJob) -> PlaceOrderRequest:
    plan_item = job.plan_item
    quantity = _next_open_buy_quantity(plan_item)
    if quantity is None:
        raise ValueError("next_open_buy job requires quantity_policy.kind='shares' with a positive integer quantity")
    exchange = _plan_item_exchange(plan_item)
    return PlaceOrderRequest(
        idempotency_key=f"execution-job:{job.id}",
        execution_plan_item_id=plan_item.id,
        symbol=plan_item.symbol,
        side=plan_item.side,
        quantity=quantity,
        order_type="limit" if plan_item.limit_price is not None else "market",
        limit_price=plan_item.limit_price,
        time_in_force=plan_item.time_in_force or "day",
        exchange=exchange,
    )


def _next_open_buy_quantity(plan_item: ExecutionPlanItem) -> int | None:
    quantity_policy = plan_item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return None
    if quantity_policy.get("kind") != "shares":
        return None
    quantity = quantity_policy.get("quantity")
    if isinstance(quantity, bool):
        return None
    if isinstance(quantity, int) and quantity > 0:
        return quantity
    if isinstance(quantity, str) and quantity.isdigit() and int(quantity) > 0:
        return int(quantity)
    return None


def _next_market_open_utc(market_date: date) -> datetime:
    local_open = datetime.combine(
        market_date,
        time(hour=9, minute=0),
        tzinfo=KRX_TIMEZONE,
    )
    return local_open.astimezone(UTC)


def _plan_item_exchange(plan_item: ExecutionPlanItem) -> str:
    risk_context = plan_item.risk_context
    if not isinstance(risk_context, dict):
        return "KRX"
    value = risk_context.get("exchange")
    if not isinstance(value, str):
        return "KRX"
    normalized = value.strip().upper()
    return normalized or "KRX"


def _is_due(job: ExecutionJob, triggered_at: datetime) -> bool:
    scheduled_for = job.scheduled_for
    if scheduled_for is None:
        return True
    return _normalize_utc(scheduled_for) <= triggered_at


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_utc(value).isoformat()
