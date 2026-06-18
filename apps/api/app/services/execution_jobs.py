from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, ROUND_DOWN
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAdapter,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionJob, ExecutionPlan, ExecutionPlanItem, new_id
from app.services.broker_orders import place_broker_order
from app.services.broker_auth import build_broker_adapter
from app.services.rejected_order_retries import (
    BROKER_ORDER_RETRY_JOB_TYPE,
    RetryOrderGuardError,
    build_rejected_order_retry_request,
    record_retry_dispatch_result,
)
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)

LOGGER = logging.getLogger(__name__)

KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
NEXT_OPEN_BUY_JOB_TYPE = "next_open_buy"
CRYPTO_PAPER_ORDER_JOB_TYPE = "crypto_paper_order"
UPBIT_PAPER_ALLOWED_SYMBOLS = frozenset({"KRW-BTC", "KRW-ETH"})
DISPATCHABLE_JOB_STATUSES = ("queued", "scheduled", "retrying")
MIN_STALE_RUNNING_JOB_SECONDS = 30
STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER = 3
COMPLETED_JOB_ORDER_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
        NormalizedBrokerOrderStatus.CANCELED.value,
        NormalizedBrokerOrderStatus.EXPIRED.value,
    }
)
RETRYING_JOB_ORDER_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.RETRY_PENDING.value,
    }
)
FAILED_JOB_ORDER_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.REJECTED.value,
        NormalizedBrokerOrderStatus.FAILED.value,
    }
)


@dataclass(frozen=True)
class DispatchExecutionJobsResult:
    status_code: int
    payload: dict[str, object]


def materialize_execution_jobs(plan: ExecutionPlan) -> None:
    for item in plan.items:
        if _is_crypto_paper_plan_item(plan=plan, item=item):
            _materialize_crypto_paper_job(plan=plan, item=item)
            continue
        if item.intent != NEXT_OPEN_BUY_JOB_TYPE or item.status != "pending":
            continue
        if _next_open_buy_quantity(item) is None or plan.market_date is None or item.side != "buy":
            item.status = "next_open_buy_pending_configuration"
            continue
        item.status = "next_open_buy_scheduled"
        scheduled_for = _next_market_open_utc(plan.market_date)
        item.jobs.append(
            ExecutionJob(
                id=new_id(),
                job_type=NEXT_OPEN_BUY_JOB_TYPE,
                status="scheduled",
                scheduled_for=scheduled_for,
            )
        )
        LOGGER.info(
            "execution_job_created",
            extra={
                "plan_id": plan.external_plan_id,
                "execution_plan_item_id": item.id,
                "symbol": item.symbol,
                "job_type": NEXT_OPEN_BUY_JOB_TYPE,
                "scheduled_for": scheduled_for.isoformat(),
            },
        )


def _materialize_crypto_paper_job(*, plan: ExecutionPlan, item: ExecutionPlanItem) -> None:
    validation_error = _validate_crypto_paper_plan_item(plan=plan, item=item)
    if validation_error is not None:
        item.status = "crypto_paper_rejected"
        item.risk_context = {
            **(item.risk_context if isinstance(item.risk_context, dict) else {}),
            "crypto_paper_block_reason": validation_error,
        }
        return
    item.status = "crypto_paper_queued"
    item.jobs.append(
        ExecutionJob(
            id=new_id(),
            job_type=CRYPTO_PAPER_ORDER_JOB_TYPE,
            status="queued",
            scheduled_for=None,
        )
    )
    LOGGER.info(
        "crypto_paper_execution_job_created",
        extra={
            "plan_id": plan.external_plan_id,
            "execution_plan_item_id": item.id,
            "symbol": item.symbol,
            "provider": item.provider,
            "job_type": CRYPTO_PAPER_ORDER_JOB_TYPE,
        },
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

    _recover_stale_running_jobs(
        settings=settings,
        db=db,
        triggered_at=triggered_at,
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
        "provider": plan_item.provider,
        "asset_class": plan_item.asset_class,
        "venue": plan_item.venue,
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
    job.plan_item.status = _dispatching_plan_item_status(job.job_type)
    db.commit()


def _recover_stale_running_jobs(
    *,
    settings: Settings,
    db: Session,
    triggered_at: datetime,
) -> None:
    running_jobs = db.scalars(
        select(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status == "running",
        )
        .options(selectinload(ExecutionJob.plan_item))
        .order_by(ExecutionJob.locked_at.asc(), ExecutionJob.created_at.asc())
    ).all()

    recovered_any = False
    for job in running_jobs:
        if not _is_stale_running_job(
            settings=settings,
            job=job,
            triggered_at=triggered_at,
        ):
            continue
        job.status = "retrying"
        job.locked_at = None
        job.last_error = "recovered stale running job after interrupted dispatch"
        job.plan_item.status = _retrying_plan_item_status(job.job_type)
        recovered_any = True

    if recovered_any:
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

    effective_adapter = _adapter_for_job(settings=settings, adapter=adapter, job=job)
    try:
        command = _build_place_order_request(
            settings=settings,
            db=db,
            adapter=effective_adapter,
            job=job,
        )
    except RetryOrderGuardError as exc:
        return _finish_job(
            db=db,
            job_id=job_id,
            status=exc.job_status,
            last_error=str(exc),
            plan_item_status=exc.plan_item_status,
        )
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
            adapter=effective_adapter,
            command=command,
            request_source=f"execution-job:{job.id}",
        )
        if job.job_type == BROKER_ORDER_RETRY_JOB_TYPE:
            record_retry_dispatch_result(
                settings=settings,
                db=db,
                job=job,
                result_payload=result.payload,
                recorded_at=datetime.now(UTC),
            )
    except Exception as exc:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="retrying",
            last_error=f"{exc.__class__.__name__}: {exc}",
            plan_item_status=_retrying_plan_item_status(job.job_type),
        )

    order_payload = result.payload.get("order")
    order_status = None
    if isinstance(order_payload, dict):
        raw_status = order_payload.get("status")
        if isinstance(raw_status, str):
            order_status = raw_status

    if result.payload.get("permanent_rejection") is True:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="failed",
            last_error=str(result.payload.get("message") or "broker order rejected permanently"),
            plan_item_status=_failed_plan_item_status(job.job_type),
        )
    if result.status_code == 409 and "block_reason" in result.payload:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="scheduled",
            last_error=str(result.payload.get("message") or "dispatch blocked by safety controls"),
            plan_item_status=None,
        )
    if order_status is None:
        order_status = _load_order_status(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
    finished_from_order_status = _finish_job_from_order_status(
        db=db,
        job_id=job_id,
        job_type=job.job_type,
        payload=result.payload,
        order_status=order_status,
    )
    if finished_from_order_status is not None:
        return finished_from_order_status
    if result.payload.get("duplicate") is True:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="failed",
            last_error="duplicate broker order exists with an unrecognized status",
            plan_item_status=_failed_plan_item_status(job.job_type),
        )
    return _finish_job(
        db=db,
        job_id=job_id,
        status="failed",
        last_error=str(result.payload.get("message") or "broker order submission failed"),
        plan_item_status=_failed_plan_item_status(job.job_type),
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


def _load_order_status(
    *,
    settings: Settings,
    db: Session,
    idempotency_key: str,
) -> str | None:
    return db.scalar(
        select(BrokerOrder.status).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == idempotency_key,
        )
    )


def _finish_job_from_order_status(
    *,
    db: Session,
    job_id: str,
    job_type: str,
    payload: dict[str, object],
    order_status: str | None,
) -> str | None:
    if order_status in COMPLETED_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="completed",
            last_error=None,
            plan_item_status=None,
        )
    if order_status in RETRYING_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="retrying",
            last_error=_retrying_job_message(payload=payload, order_status=order_status),
            plan_item_status=_retrying_plan_item_status(job_type),
        )
    if order_status in FAILED_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            status="failed",
            last_error=_failed_job_message(payload=payload, order_status=order_status),
            plan_item_status=_failed_plan_item_status(job_type),
        )
    return None


def _retrying_job_message(
    *,
    payload: dict[str, object],
    order_status: str,
) -> str:
    duplicate = payload.get("duplicate") is True
    if duplicate:
        if order_status == NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value:
            return "existing broker order submission is still pending"
        return f"existing broker order is {order_status}"
    message = payload.get("message")
    if isinstance(message, str) and message:
        return message
    if order_status == NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value:
        return "broker order submission is still pending"
    return "broker retry requested"


def _failed_job_message(
    *,
    payload: dict[str, object],
    order_status: str,
) -> str:
    duplicate = payload.get("duplicate") is True
    message = payload.get("message")
    if not duplicate and isinstance(message, str) and message:
        return message
    if duplicate:
        return f"existing broker order is {order_status}"
    return f"broker order submission ended in {order_status}"


def _build_place_order_request(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job: ExecutionJob,
) -> PlaceOrderRequest:
    if job.job_type == BROKER_ORDER_RETRY_JOB_TYPE:
        return build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,
            job=job,
            triggered_at=job.locked_at or datetime.now(UTC),
        )
    if job.job_type == CRYPTO_PAPER_ORDER_JOB_TYPE:
        return _build_crypto_paper_order_request(job)
    if job.job_type != NEXT_OPEN_BUY_JOB_TYPE:
        raise ValueError(f"unsupported execution job type: {job.job_type}")

    plan_item = job.plan_item
    quantity = _next_open_buy_quantity(plan_item)
    if quantity is None:
        raise ValueError("next_open_buy job requires quantity_policy.kind='shares' with a positive integer quantity")
    exchange = _plan_item_exchange(plan_item)
    return PlaceOrderRequest(
        idempotency_key=_place_order_idempotency_key(job),
        execution_plan_item_id=plan_item.id,
        symbol=plan_item.symbol,
        side=plan_item.side,
        quantity=quantity,
        order_type="limit" if plan_item.limit_price is not None else "market",
        limit_price=plan_item.limit_price,
        time_in_force=plan_item.time_in_force or "day",
        exchange=exchange,
    )


def _build_crypto_paper_order_request(job: ExecutionJob) -> PlaceOrderRequest:
    plan_item = job.plan_item
    if plan_item.provider != "upbit" or plan_item.asset_class != "crypto" or plan_item.venue != "upbit":
        raise ValueError("crypto paper job requires explicit Upbit crypto provider metadata")
    return PlaceOrderRequest(
        idempotency_key=_place_order_idempotency_key(job),
        execution_plan_item_id=plan_item.id,
        provider="upbit",
        asset_class="crypto",
        venue="upbit",
        symbol=plan_item.symbol,
        side=plan_item.side,
        quantity=None,
        base_quantity=plan_item.base_quantity,
        quote_notional=plan_item.quote_notional,
        quantity_precision=plan_item.quantity_precision,
        price_precision=plan_item.price_precision,
        order_type="limit" if plan_item.limit_price is not None else "market",
        limit_price=plan_item.limit_price,
        time_in_force=plan_item.time_in_force or "day",
        exchange="UPBIT",
        metadata={"paper": True, "simulated": True},
    )


def _adapter_for_job(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    job: ExecutionJob,
) -> BrokerAdapter:
    provider = job.plan_item.provider
    if provider and provider != adapter.provider:
        return build_broker_adapter(settings, provider=provider)
    return adapter


def _place_order_idempotency_key(job: ExecutionJob) -> str:
    quantity_policy = job.plan_item.quantity_policy
    if isinstance(quantity_policy, dict):
        idempotency_key = quantity_policy.get("idempotency_key")
        if isinstance(idempotency_key, str) and idempotency_key.strip():
            return idempotency_key.strip()
    return f"execution-job:{job.id}"


def _next_open_buy_quantity(plan_item: ExecutionPlanItem) -> int | None:
    quantity_policy = plan_item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return None

    kind = quantity_policy.get("kind") or quantity_policy.get("type")
    if kind in {"shares", "fixed_quantity"}:
        return _parse_positive_int(quantity_policy.get("quantity"))

    if kind == "exec_agent_sized":
        explicit_quantity = _parse_positive_int(
            quantity_policy.get("quantity")
            or quantity_policy.get("planned_quantity")
            or quantity_policy.get("computed_quantity")
        )
        if explicit_quantity is not None:
            return explicit_quantity

        budget = _parse_positive_decimal(quantity_policy.get("budget_krw"))
        reference_price = _parse_positive_decimal(
            quantity_policy.get("estimated_reference_price")
            or quantity_policy.get("reference_price")
            or quantity_policy.get("entry_reference_price")
        )
        if budget is None or reference_price is None:
            return None
        quantity = (budget / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN)
        if quantity <= 0:
            return None
        return int(quantity)

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
    return None


def _parse_positive_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value).strip())
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


def _is_crypto_paper_plan_item(*, plan: ExecutionPlan, item: ExecutionPlanItem) -> bool:
    del plan
    return item.provider == "upbit" or item.asset_class == "crypto" or item.venue == "upbit"


def _validate_crypto_paper_plan_item(*, plan: ExecutionPlan, item: ExecutionPlanItem) -> str | None:
    if plan.mode != "paper":
        return "crypto execution plans are paper-only"
    if item.provider != "upbit":
        return "crypto paper execution requires provider='upbit'"
    if item.asset_class != "crypto":
        return "crypto paper execution requires asset_class='crypto'"
    if item.venue != "upbit":
        return "crypto paper execution requires venue='upbit'"
    if item.symbol not in UPBIT_PAPER_ALLOWED_SYMBOLS:
        return "crypto paper execution is limited to KRW-BTC and KRW-ETH"
    if item.side not in {"buy", "sell"}:
        return "crypto paper execution side must be buy or sell"
    sizing_count = sum(
        value is not None
        for value in (item.base_quantity, item.quote_notional)
    )
    if sizing_count != 1:
        return "crypto paper execution requires exactly one of base_quantity or quote_notional"
    if item.quote_notional is not None and item.side != "buy":
        return "quote_notional is only supported for crypto paper buys"
    if item.quote_notional is not None and _crypto_paper_fill_price(item) is None:
        return "quote_notional crypto paper buys require simulated_fill_price or reference_price"
    if (
        item.base_quantity is not None
        and item.side == "buy"
        and item.limit_price is None
        and _crypto_paper_fill_price(item) is None
    ):
        return "base_quantity crypto paper buys require limit_price or simulated_fill_price"
    return None


def _crypto_paper_fill_price(item: ExecutionPlanItem) -> Decimal | None:
    if item.limit_price is not None:
        return item.limit_price
    if not isinstance(item.risk_context, dict):
        return None
    for key in ("simulated_fill_price", "paper_fill_price", "reference_price", "estimated_reference_price"):
        parsed = _parse_positive_decimal(item.risk_context.get(key))
        if parsed is not None:
            return parsed
    provider_metadata = item.risk_context.get("provider_metadata")
    if isinstance(provider_metadata, dict):
        for key in ("simulated_fill_price", "paper_fill_price", "reference_price"):
            parsed = _parse_positive_decimal(provider_metadata.get(key))
            if parsed is not None:
                return parsed
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


def _dispatching_plan_item_status(job_type: str) -> str:
    if job_type == BROKER_ORDER_RETRY_JOB_TYPE:
        return "broker_order_retry_dispatching"
    if job_type == CRYPTO_PAPER_ORDER_JOB_TYPE:
        return "crypto_paper_dispatching"
    return "next_open_buy_dispatching"


def _retrying_plan_item_status(job_type: str) -> str:
    if job_type == BROKER_ORDER_RETRY_JOB_TYPE:
        return "broker_order_retry_pending"
    if job_type == CRYPTO_PAPER_ORDER_JOB_TYPE:
        return "crypto_paper_retry_pending"
    return "next_open_buy_retry_pending"


def _failed_plan_item_status(job_type: str) -> str:
    if job_type == BROKER_ORDER_RETRY_JOB_TYPE:
        return "broker_order_failed"
    if job_type == CRYPTO_PAPER_ORDER_JOB_TYPE:
        return "crypto_paper_failed"
    return "next_open_buy_failed"


def _is_stale_running_job(
    *,
    settings: Settings,
    job: ExecutionJob,
    triggered_at: datetime,
) -> bool:
    if job.locked_at is None:
        return True
    stale_after_seconds = max(
        settings.kis_request_timeout_seconds * STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER,
        MIN_STALE_RUNNING_JOB_SECONDS,
    )
    cutoff = triggered_at - timedelta(seconds=stale_after_seconds)
    return _normalize_utc(job.locked_at) <= cutoff


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_utc(value).isoformat()
