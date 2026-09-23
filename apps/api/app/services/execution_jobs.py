from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from time import monotonic
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAdapter,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    RuntimeControlState,
    new_id,
)
from app.services.broker_orders import place_broker_order
from app.services.exec_agent_approval import is_legacy_unverified_exec_agent_sizing
from app.services.broker_auth import build_broker_adapter, ensure_authenticated_broker_session
from app.services.json_compaction import compact_json_object
from app.services.monitoring_price_source import (
    _is_trusted_exchange_freshness_source,
    is_us_regular_session,
    next_us_regular_open,
)
from app.services.plan_judgment_actions import (
    project_buy_plan_item_for_hourly_risk_off,
)
from app.services.rejected_order_retries import (
    BROKER_ORDER_RETRY_JOB_TYPE,
    RetryOrderGuardError,
    build_rejected_order_retry_request,
    record_retry_dispatch_result,
)
from app.services.runtime_controls import (
    RUNTIME_CONTROL_ID,
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)
from app.services.runtime_transitions import set_execution_job_status

LOGGER = logging.getLogger(__name__)

KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
US_EASTERN_TIMEZONE = ZoneInfo("America/New_York")
NEXT_OPEN_BUY_JOB_TYPE = "next_open_buy"
CRYPTO_PAPER_ORDER_JOB_TYPE = "crypto_paper_order"
UPBIT_PAPER_ALLOWED_SYMBOLS = frozenset({"KRW-BTC", "KRW-ETH"})
DISPATCHABLE_JOB_STATUSES = ("queued", "scheduled", "retrying")
RETRYABLE_ACCOUNT_DRAWDOWN_BLOCK_REASONS = frozenset(
    {
        "account_drawdown_recovery_cooldown",
        "account_drawdown_recovery_disabled",
        "account_drawdown_recovery_observation_interval",
        "account_drawdown_recovery_pending",
        "account_drawdown_recovery_scope_mismatch",
        "account_drawdown_recovery_state_changed",
        "account_drawdown_recovery_fresh_candidate_required",
    }
)
CLAIMABLE_PLAN_ITEM_STATUSES_BY_JOB_TYPE = {
    NEXT_OPEN_BUY_JOB_TYPE: frozenset(
        {
            "next_open_buy_scheduled",
            "next_open_buy_retry_pending",
            "new_buy_blocked",
        }
    ),
    BROKER_ORDER_RETRY_JOB_TYPE: frozenset({"broker_order_retry_pending"}),
    CRYPTO_PAPER_ORDER_JOB_TYPE: frozenset(
        {"crypto_paper_queued", "crypto_paper_retry_pending"}
    ),
}
CLAIMABLE_PLAN_ITEM_STATUSES = frozenset(
    status
    for statuses in CLAIMABLE_PLAN_ITEM_STATUSES_BY_JOB_TYPE.values()
    for status in statuses
)
MIN_STALE_RUNNING_JOB_SECONDS = 30
STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER = 12
STALE_RUNNING_JOB_GRACE_SECONDS = 30
KIS_OVERSEAS_PAGINATION_PAGE_LIMIT = 20
KIS_US_ACCOUNT_SNAPSHOT_PAGINATED_ENDPOINTS = 2
KIS_US_ACCOUNT_SNAPSHOT_NONPAGINATED_REQUESTS = 1
KIS_US_ACCOUNT_GATE_EXCHANGE_COUNT = 3
KIS_US_FINAL_BOUNDARY_REQUEST_ALLOWANCE = 3
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
RECOVERED_COMPLETED_JOB_ORDER_STATUSES = frozenset(
    {
        *COMPLETED_JOB_ORDER_STATUSES,
        NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
    }
)


@dataclass(frozen=True)
class DispatchExecutionJobsResult:
    status_code: int
    payload: dict[str, object]


def materialize_execution_jobs(
    plan: ExecutionPlan,
    *,
    only_item_ids: set[str] | None = None,
) -> None:
    for item in plan.items:
        if only_item_ids is not None and item.id not in only_item_ids:
            continue
        if _is_crypto_paper_plan_item(plan=plan, item=item):
            _materialize_crypto_paper_job(plan=plan, item=item)
            continue
        if item.intent != NEXT_OPEN_BUY_JOB_TYPE or item.status != "pending":
            continue
        existing_job = next(
            (job for job in item.jobs if job.job_type == NEXT_OPEN_BUY_JOB_TYPE),
            None,
        )
        if existing_job is not None:
            item.status = "next_open_buy_scheduled"
            continue
        if (
            _next_open_buy_quantity(item) is None
            or plan.market_date is None
            or item.side != "buy"
            or (item.asset_class == "us_equity" and item.limit_price is None)
        ):
            item.status = "next_open_buy_pending_configuration"
            continue
        item.status = "next_open_buy_scheduled"
        scheduled_for = _next_market_open_utc(plan.market_date, plan_item=item)
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
    dispatch_started = monotonic()
    triggered_at = _normalize_utc(now or datetime.now(UTC))
    from app.services.core_handoff_v4 import (
        reconcile_unproven_legacy_core_buy_authority,
    )

    reconcile_unproven_legacy_core_buy_authority(
        db=db,
        runtime_mode=settings.runtime_mode,
    )
    control_state = ensure_runtime_control_state(settings=settings, db=db)
    _converge_retrying_jobs_from_committed_order_truth(
        settings=settings,
        db=db,
        triggered_at=triggered_at,
    )
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
                "next_dispatch_at": None,
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
            ExecutionPlanItem.status.in_(CLAIMABLE_PLAN_ITEM_STATUSES),
            or_(
                ExecutionJob.scheduled_for.is_(None),
                ExecutionJob.scheduled_for <= triggered_at,
            ),
        )
        .options(
            selectinload(ExecutionJob.plan_item).selectinload(ExecutionPlanItem.plan),
        )
        .order_by(ExecutionJob.scheduled_for.asc(), ExecutionJob.created_at.asc())
    ).all()
    due_jobs = jobs

    completed = 0
    retrying = 0
    failed = 0
    serialized_jobs: list[dict[str, object]] = []
    krx_calendar_state_by_date: dict[date, bool | None] = {}

    for job in due_jobs:
        initial_next_open_guard = _guard_initial_next_open_submission(
            settings=settings,
            db=db,
            adapter=_adapter_for_job(settings=settings, adapter=adapter, job=job),
            job=job,
            triggered_at=triggered_at,
            dispatch_started=dispatch_started,
            calendar_state_by_date=krx_calendar_state_by_date,
        )
        if initial_next_open_guard is not None:
            if initial_next_open_guard == "expired":
                failed += 1
            serialized_jobs.append(serialize_execution_job(job))
            continue
        current_reference_at = _monotonic_cycle_reference_at(
            triggered_at=triggered_at,
            dispatch_started=dispatch_started,
        )
        if _defer_us_next_open_job_outside_session(
            db=db,
            job=job,
            triggered_at=current_reference_at,
        ):
            serialized_jobs.append(serialize_execution_job(job))
            continue
        if not _claim_job(db=db, job=job, triggered_at=current_reference_at):
            serialized_jobs.append(serialize_execution_job(job))
            continue
        result = _dispatch_job(
            settings=settings,
            db=db,
            adapter=adapter,
            job_id=job.id,
            claim_locked_at=current_reference_at,
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

    next_dispatch_at = _next_dispatchable_job_at(
        settings=settings,
        db=db,
        triggered_at=triggered_at,
    )
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
            "next_dispatch_at": _serialize_datetime(next_dispatch_at),
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
) -> bool:
    claimable_plan_item_statuses = CLAIMABLE_PLAN_ITEM_STATUSES_BY_JOB_TYPE.get(
        job.job_type,
        frozenset(),
    )
    claim_state = db.execute(
        select(ExecutionJob.status, ExecutionPlanItem.status)
        .join(ExecutionJob.plan_item)
        .where(ExecutionJob.id == job.id)
        .with_for_update()
    ).one_or_none()
    if (
        claim_state is None
        or claim_state[0] not in DISPATCHABLE_JOB_STATUSES
        or claim_state[1] not in claimable_plan_item_statuses
    ):
        return False
    db.refresh(job)
    db.refresh(job.plan_item, attribute_names=["status"])
    set_execution_job_status(
        db=db,
        job=job,
        status="running",
        source="execution_job_dispatch",
        locked_at=triggered_at,
        increment_attempts=True,
        plan_item_status=_dispatching_plan_item_status(job.job_type),
        reason="execution job claimed for dispatch",
        occurred_at=triggered_at,
    )
    db.commit()
    return True


def _converge_retrying_jobs_from_committed_order_truth(
    *,
    settings: Settings,
    db: Session,
    triggered_at: datetime,
) -> None:
    candidate_ids = db.scalars(
        select(ExecutionJob.id)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status == "retrying",
            ~ExecutionPlanItem.status.in_(CLAIMABLE_PLAN_ITEM_STATUSES),
        )
        .order_by(ExecutionJob.created_at.asc(), ExecutionJob.id.asc())
    ).all()
    if not candidate_ids:
        return

    _lock_hourly_risk_off_fence(db=db)
    terminal_order_statuses = (
        RECOVERED_COMPLETED_JOB_ORDER_STATUSES | FAILED_JOB_ORDER_STATUSES
    )
    for job_id in candidate_ids:
        job = db.scalar(
            select(ExecutionJob)
            .join(ExecutionJob.plan_item)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionJob.id == job_id,
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionJob.status == "retrying",
            )
            .with_for_update()
            .options(selectinload(ExecutionJob.plan_item))
            .execution_options(populate_existing=True)
        )
        if (
            job is None
            or job.plan_item.status in CLAIMABLE_PLAN_ITEM_STATUSES
        ):
            continue
        idempotency_key = _resolved_place_order_idempotency_key(
            settings=settings,
            db=db,
            job=job,
        )
        if idempotency_key is None:
            continue
        order_status = _load_order_status(
            settings=settings,
            db=db,
            idempotency_key=idempotency_key,
        )
        if order_status not in terminal_order_statuses:
            continue
        _recover_stale_job_from_broker_order_truth(
            settings=settings,
            db=db,
            job=job,
            triggered_at=triggered_at,
        )
    db.commit()


def _recover_stale_running_jobs(
    *,
    settings: Settings,
    db: Session,
    triggered_at: datetime,
) -> None:
    running_job_rows = db.execute(
        select(
            ExecutionJob.id,
            ExecutionJob.locked_at,
            ExecutionPlanItem.provider,
            ExecutionPlanItem.asset_class,
            ExecutionPlanItem.side,
        )
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status == "running",
        )
        .order_by(ExecutionJob.locked_at.asc(), ExecutionJob.created_at.asc())
    ).all()
    stale_job_ids = [
        job_id
        for job_id, locked_at, provider, asset_class, side in running_job_rows
        if _is_stale_running_job_lock(
            settings=settings,
            locked_at=locked_at,
            triggered_at=triggered_at,
            timeout_multiplier=_execution_job_timeout_multiplier(
                provider=provider,
                asset_class=asset_class,
                side=side,
            ),
        )
    ]
    if not stale_job_ids:
        return

    control_state = _lock_hourly_risk_off_fence(db=db)

    for job_id in stale_job_ids:
        locked_row = db.execute(
            select(ExecutionJob, ExecutionPlanItem)
            .join(ExecutionJob.plan_item)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionJob.id == job_id,
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionJob.status == "running",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).one_or_none()
        if locked_row is None:
            continue
        job, plan_item = locked_row
        db.refresh(job, attribute_names=["status", "locked_at"])
        db.refresh(
            plan_item,
            attribute_names=["status", "risk_context", "side", "intent"],
        )
        if not _is_stale_running_job(
            settings=settings,
            job=job,
            triggered_at=triggered_at,
        ):
            continue
        if _recover_stale_job_from_broker_order_truth(
            settings=settings,
            db=db,
            job=job,
            triggered_at=triggered_at,
        ):
            continue
        dispatching_status = _dispatching_plan_item_status(job.job_type)
        has_active_risk_off_projection = _plan_item_has_active_hourly_risk_off(
            plan_item
        )
        if (
            plan_item.status != dispatching_status
            and not has_active_risk_off_projection
        ):
            reason = (
                "stale running job cannot resume because plan item is no longer "
                "in its dispatching state"
            )
            set_execution_job_status(
                db=db,
                job=job,
                status="failed",
                source="execution_job_dispatch",
                last_error=reason,
                plan_item_status=None,
                reason=reason,
                occurred_at=triggered_at,
            )
            continue
        preserve_risk_off_projection = False
        if control_state.hourly_risk_off_active:
            risk_flags, risk_reasons, fence_context = (
                _active_hourly_risk_off_projection_inputs(control_state)
            )
            project_buy_plan_item_for_hourly_risk_off(
                db=db,
                settings=settings,
                item=plan_item,
                risk_flags=risk_flags,
                risk_reasons=risk_reasons,
                now=triggered_at,
                resume_status=_retrying_plan_item_status(job.job_type),
                fence_context=fence_context,
            )
            db.refresh(plan_item, attribute_names=["status", "risk_context"])
            preserve_risk_off_projection = _plan_item_has_active_hourly_risk_off(
                plan_item
            )
        elif _plan_item_has_active_hourly_risk_off(plan_item):
            preserve_risk_off_projection = True
        set_execution_job_status(
            db=db,
            job=job,
            status="retrying",
            source="execution_job_dispatch",
            last_error="recovered stale running job after interrupted dispatch",
            plan_item_status=(
                None
                if preserve_risk_off_projection
                else _retrying_plan_item_status(job.job_type)
            ),
            reason="recovered stale running job after interrupted dispatch",
            occurred_at=triggered_at,
        )
    # Release the runtime fence row even if a job changed while we waited for
    # its lock.  Hourly activation follows the same control-row-first order.
    db.commit()


def _recover_stale_job_from_broker_order_truth(
    *,
    settings: Settings,
    db: Session,
    job: ExecutionJob,
    triggered_at: datetime,
) -> bool:
    idempotency_key = _resolved_place_order_idempotency_key(
        settings=settings,
        db=db,
        job=job,
    )
    if idempotency_key is None:
        return False
    order_status = _load_order_status(
        settings=settings,
        db=db,
        idempotency_key=idempotency_key,
    )
    if order_status in RECOVERED_COMPLETED_JOB_ORDER_STATUSES:
        status = "completed"
        last_error = None
    elif order_status in RETRYING_JOB_ORDER_STATUSES:
        status = "retrying"
        last_error = _retrying_job_message(
            payload={},
            order_status=order_status,
        )
    elif order_status in FAILED_JOB_ORDER_STATUSES:
        status = "failed"
        last_error = _failed_job_message(
            payload={},
            order_status=order_status,
        )
    else:
        return False
    set_execution_job_status(
        db=db,
        job=job,
        status=status,
        source="execution_job_recovery",
        last_error=last_error,
        plan_item_status=None,
        reason=(
            "recovered stale running job from committed broker order truth"
        ),
        occurred_at=triggered_at,
    )
    return True


def _lock_hourly_risk_off_fence(*, db: Session) -> RuntimeControlState:
    if db.get_bind().dialect.name == "sqlite":
        # SQLite ignores FOR UPDATE. This no-op write obtains the database's
        # writer reservation before any execution job or plan item is changed.
        db.execute(
            update(RuntimeControlState)
            .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
            .values(
                hourly_risk_off_epoch=RuntimeControlState.hourly_risk_off_epoch
            )
        )
    state = db.scalar(
        select(RuntimeControlState)
        .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if state is None:
        raise RuntimeError("runtime control state disappeared during stale job recovery")
    db.refresh(
        state,
        attribute_names=[
            "hourly_risk_off_active",
            "hourly_risk_off_epoch",
            "hourly_risk_off_context",
        ],
    )
    return state


def _active_hourly_risk_off_projection_inputs(
    state: RuntimeControlState,
) -> tuple[int, list[str], dict[str, object]]:
    context = (
        state.hourly_risk_off_context
        if isinstance(state.hourly_risk_off_context, dict)
        else {}
    )
    raw_flags = context.get(
        "activation_risk_flags",
        context.get("last_risk_flags", 3),
    )
    try:
        risk_flags = max(3, int(raw_flags))
    except (TypeError, ValueError):
        risk_flags = 3
    raw_reasons = context.get(
        "activation_risk_reasons",
        context.get("last_risk_reasons", []),
    )
    risk_reasons = (
        [reason for reason in raw_reasons if isinstance(reason, str) and reason]
        if isinstance(raw_reasons, list)
        else []
    )
    if not risk_reasons:
        risk_reasons = ["active hourly buy risk-off fence"]
    return risk_flags, risk_reasons, context


def _plan_item_has_active_hourly_risk_off(item: ExecutionPlanItem) -> bool:
    if item.status != "watch_expired":
        return False
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    adjustment = risk_context.get("hourly_plan_action")
    return bool(
        isinstance(adjustment, dict)
        and adjustment.get("source") == "hourly_runtime_check"
        and adjustment.get("action") == "risk_off_expire"
    )


def _dispatch_job(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job_id: str,
    claim_locked_at: datetime,
) -> str:
    job = db.get(
        ExecutionJob,
        job_id,
        options=[selectinload(ExecutionJob.plan_item)],
    )
    assert job is not None
    dispatch_started = monotonic()
    dispatch_reference_at = _normalize_utc(claim_locked_at)
    lease_locked_at = dispatch_reference_at

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
            expected_locked_at=lease_locked_at,
            status=exc.job_status,
            last_error=str(exc),
            plan_item_status=exc.plan_item_status,
        )
    except ValueError as exc:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=lease_locked_at,
            status="failed",
            last_error=str(exc),
            plan_item_status=(
                "manual_review_required"
                if job.job_type == BROKER_ORDER_RETRY_JOB_TYPE
                else _failed_plan_item_status(job.job_type)
            ),
        )

    renewed_lease = _renew_job_lease(
        db=db,
        job_id=job_id,
        expected_locked_at=lease_locked_at,
        renewed_at=_monotonic_cycle_reference_at(
            triggered_at=dispatch_reference_at,
            dispatch_started=dispatch_started,
        ),
    )
    if renewed_lease is None:
        return "superseded"
    lease_locked_at = renewed_lease
    next_open_guard_error = _verified_agent_next_open_quote_guard(
        settings=settings,
        db=db,
        adapter=effective_adapter,
        job=job,
        command=command,
        reference_at=(
            _monotonic_cycle_reference_at(
                triggered_at=dispatch_reference_at,
                dispatch_started=dispatch_started,
            )
        ),
    )
    if next_open_guard_error is not None:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=lease_locked_at,
            status="failed",
            last_error=next_open_guard_error,
            plan_item_status="next_open_buy_failed",
        )

    renewed_lease = _renew_job_lease(
        db=db,
        job_id=job_id,
        expected_locked_at=lease_locked_at,
        renewed_at=_monotonic_cycle_reference_at(
            triggered_at=dispatch_reference_at,
            dispatch_started=dispatch_started,
        ),
    )
    if renewed_lease is None:
        return "superseded"
    lease_locked_at = renewed_lease
    submission_job = _lock_job_for_broker_submission(
        db=db,
        job_id=job_id,
        expected_locked_at=lease_locked_at,
    )
    if submission_job is None:
        return "superseded"
    job = submission_job
    try:
        result = place_broker_order(
            settings=settings,
            db=db,
            adapter=effective_adapter,
            command=command,
            request_source=f"execution-job:{job.id}",
            now=dispatch_reference_at,
            freshness_reference_at=_monotonic_cycle_reference_at(
                triggered_at=dispatch_reference_at,
                dispatch_started=dispatch_started,
            ),
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
            expected_locked_at=lease_locked_at,
            status="retrying",
            last_error=f"{exc.__class__.__name__}: {exc}",
            plan_item_status=_retrying_plan_item_status(job.job_type),
        )

    renewed_lease = _renew_job_lease(
        db=db,
        job_id=job_id,
        expected_locked_at=lease_locked_at,
        renewed_at=_monotonic_cycle_reference_at(
            triggered_at=dispatch_reference_at,
            dispatch_started=dispatch_started,
        ),
    )
    if renewed_lease is None:
        return "superseded"
    lease_locked_at = renewed_lease

    order_payload = result.payload.get("order")
    order_status = None
    if isinstance(order_payload, dict):
        raw_status = order_payload.get("status")
        if isinstance(raw_status, str):
            order_status = raw_status

    if (
        job.job_type == BROKER_ORDER_RETRY_JOB_TYPE
        and result.payload.get("manual_reconciliation_required") is True
    ):
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=lease_locked_at,
            status="failed",
            last_error=str(
                result.payload.get("message")
                or "broker retry requires manual reconciliation"
            ),
            plan_item_status="manual_review_required",
        )
    if result.payload.get("permanent_rejection") is True:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=lease_locked_at,
            status="failed",
            last_error=str(result.payload.get("message") or "broker order rejected permanently"),
            plan_item_status=_failed_plan_item_status(job.job_type),
        )
    if result.status_code == 409 and "block_reason" in result.payload:
        retry_after_at = _response_retry_after_at(result.payload)
        block_reason = result.payload.get("block_reason")
        terminal_account_recovery_block = (
            _is_account_drawdown_recovery_block(result.payload)
            and block_reason not in RETRYABLE_ACCOUNT_DRAWDOWN_BLOCK_REASONS
        )
        if block_reason == "hourly_plan_risk_off":
            db.refresh(job.plan_item, attribute_names=["status", "risk_context"])
            plan_item_status = (
                None
                if _plan_item_has_active_hourly_risk_off(job.plan_item)
                else _retrying_plan_item_status(job.job_type)
            )
        elif block_reason in RETRYABLE_ACCOUNT_DRAWDOWN_BLOCK_REASONS:
            plan_item_status = (
                "new_buy_blocked"
                if job.job_type == NEXT_OPEN_BUY_JOB_TYPE
                else _retrying_plan_item_status(job.job_type)
            )
        elif block_reason in {"paused", "kill_switch"}:
            plan_item_status = _retrying_plan_item_status(job.job_type)
        else:
            plan_item_status = None
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=lease_locked_at,
            status="failed" if terminal_account_recovery_block else "scheduled",
            last_error=str(result.payload.get("message") or "dispatch blocked by safety controls"),
            plan_item_status=plan_item_status,
            scheduled_for=retry_after_at,
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
        expected_locked_at=lease_locked_at,
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
            expected_locked_at=lease_locked_at,
            status="failed",
            last_error="duplicate broker order exists with an unrecognized status",
            plan_item_status=_failed_plan_item_status(job.job_type),
        )
    return _finish_job(
        db=db,
        job_id=job_id,
        expected_locked_at=lease_locked_at,
        status="failed",
        last_error=str(result.payload.get("message") or "broker order submission failed"),
        plan_item_status=_failed_plan_item_status(job.job_type),
    )


def _finish_job(
    *,
    db: Session,
    job_id: str,
    expected_locked_at: datetime,
    status: str,
    last_error: str | None,
    plan_item_status: str | None,
    scheduled_for: datetime | None = None,
) -> str:
    job = _lock_current_job_lease(
        db=db,
        job_id=job_id,
        expected_locked_at=expected_locked_at,
    )
    if job is None:
        return "superseded"
    if scheduled_for is not None:
        job.scheduled_for = scheduled_for
    set_execution_job_status(
        db=db,
        job=job,
        status=status,
        source="execution_job_dispatch",
        last_error=last_error,
        plan_item_status=plan_item_status,
        reason=last_error,
        payload={"execution_job_id": job.id},
    )
    db.commit()
    return status


def _renew_job_lease(
    *,
    db: Session,
    job_id: str,
    expected_locked_at: datetime,
    renewed_at: datetime,
) -> datetime | None:
    job = _lock_current_job_lease(
        db=db,
        job_id=job_id,
        expected_locked_at=expected_locked_at,
    )
    if job is None:
        return None
    job.locked_at = _normalize_utc(renewed_at)
    db.commit()
    db.refresh(job, attribute_names=["locked_at"])
    if job.locked_at is None:
        return None
    return _normalize_utc(job.locked_at)


def _lock_current_job_lease(
    *,
    db: Session,
    job_id: str,
    expected_locked_at: datetime,
) -> ExecutionJob | None:
    with db.no_autoflush:
        if db.get_bind().dialect.name == "sqlite":
            reservation = db.execute(
                update(ExecutionJob)
                .where(
                    ExecutionJob.id == job_id,
                    ExecutionJob.status == "running",
                )
                .values(id=ExecutionJob.id)
            )
            if reservation.rowcount != 1:
                db.rollback()
                return None
        job = db.scalar(
            select(ExecutionJob)
            .where(ExecutionJob.id == job_id)
            .with_for_update()
            .options(selectinload(ExecutionJob.plan_item))
            .execution_options(populate_existing=True)
        )
    if (
        job is None
        or job.status != "running"
        or job.locked_at is None
        or _normalize_utc(job.locked_at) != _normalize_utc(expected_locked_at)
    ):
        db.rollback()
        return None
    return job


def _lock_job_for_broker_submission(
    *,
    db: Session,
    job_id: str,
    expected_locked_at: datetime,
) -> ExecutionJob | None:
    """Serialize broker submission against a Core revocation transaction.

    The plan item is locked before the job to match the revocation lock order.
    Whichever transaction wins this boundary decides the outcome: a completed
    revocation makes the worker fail closed, while a worker already inside the
    boundary commits broker truth before revocation can decide it is still safe.
    """
    with db.no_autoflush:
        candidate = db.get(ExecutionJob, job_id)
        if candidate is None:
            db.rollback()
            return None
        from app.services.core_handoff_v4 import (
            fence_unproven_legacy_core_buy_authority,
        )

        if fence_unproven_legacy_core_buy_authority(
            db=db,
            execution_plan_item_id=candidate.execution_plan_item_id,
        ):
            return None
        plan_item = db.scalar(
            select(ExecutionPlanItem)
            .where(ExecutionPlanItem.id == candidate.execution_plan_item_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if plan_item is None:
            db.rollback()
            return None
        job = _lock_current_job_lease(
            db=db,
            job_id=job_id,
            expected_locked_at=expected_locked_at,
        )
        if job is None:
            return None
        if plan_item.status != _dispatching_plan_item_status(job.job_type):
            job.status = "superseded"
            job.locked_at = None
            job.last_error = (
                "broker submission cancelled because the plan item is no longer dispatching"
            )
            db.commit()
            return None
        return job


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
    expected_locked_at: datetime,
    job_type: str,
    payload: dict[str, object],
    order_status: str | None,
) -> str | None:
    if order_status in COMPLETED_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=expected_locked_at,
            status="completed",
            last_error=None,
            plan_item_status=None,
        )
    if order_status in RETRYING_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=expected_locked_at,
            status="retrying",
            last_error=_retrying_job_message(payload=payload, order_status=order_status),
            plan_item_status=_retrying_plan_item_status(job_type),
        )
    if order_status in FAILED_JOB_ORDER_STATUSES:
        return _finish_job(
            db=db,
            job_id=job_id,
            expected_locked_at=expected_locked_at,
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


def _response_retry_after_at(payload: dict[str, object]) -> datetime | None:
    raw_retry_after = payload.get("retry_after_at")
    if raw_retry_after is None:
        recovery = payload.get("account_drawdown_recovery")
        if isinstance(recovery, dict):
            raw_retry_after = recovery.get("retry_after_at")
    if not isinstance(raw_retry_after, str) or not raw_retry_after.strip():
        return None
    try:
        parsed = datetime.fromisoformat(raw_retry_after.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _normalize_utc(parsed)


def _is_account_drawdown_recovery_block(payload: dict[str, object]) -> bool:
    block_reason = payload.get("block_reason")
    return isinstance(block_reason, str) and block_reason.startswith(
        "account_drawdown_recovery_"
    )


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
    if is_legacy_unverified_exec_agent_sizing(plan_item.quantity_policy):
        raise ValueError("unverified_exec_agent_sizing")
    quantity = _next_open_buy_quantity(plan_item)
    if quantity is None:
        raise ValueError("next_open_buy job requires quantity_policy.kind='shares' with a positive integer quantity")
    exchange = _plan_item_exchange(plan_item)
    return PlaceOrderRequest(
        idempotency_key=_place_order_idempotency_key(job),
        execution_plan_item_id=plan_item.id,
        provider=plan_item.provider or "kis",
        asset_class=plan_item.asset_class or "stock",
        venue=plan_item.venue,
        symbol=plan_item.symbol,
        side=plan_item.side,
        quantity=quantity,
        order_type="limit" if plan_item.limit_price is not None else "market",
        limit_price=plan_item.limit_price,
        time_in_force=plan_item.time_in_force or "day",
        exchange=exchange,
    )


def _verified_agent_next_open_quote_guard(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job: ExecutionJob,
    command: PlaceOrderRequest,
    reference_at: datetime,
) -> str | None:
    item = job.plan_item
    if job.job_type != NEXT_OPEN_BUY_JOB_TYPE:
        return None
    policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    if policy.get("source") == "exec_agent_watch_sizing" and not _is_verified_watch_sizing_approval(
        item
    ):
        return "unverified_exec_agent_sizing"
    if not _is_verified_watch_sizing_approval(item):
        return None
    if (
        str(command.provider or "").strip().lower() != "kis"
        or str(adapter.provider or "").strip().lower() != "kis"
    ):
        # The generic non-KIS price methods do not currently provide a
        # timestamped executable stock ask. Do not pretend that their payload
        # satisfies the verified sizing contract.
        return "next_open_verified_provider_quote_unsupported"
    if command.order_type != "limit" or command.limit_price is None:
        return "next_open_unbounded_market_order"

    sizing_price = _parse_positive_decimal(policy.get("sizing_price"))
    approved_notional = _parse_positive_decimal(policy.get("approved_notional"))
    if sizing_price is None or command.limit_price > sizing_price:
        return "next_open_limit_exceeds_sizing_reference"
    if (
        approved_notional is None
        or command.quantity is None
        or command.limit_price * Decimal(command.quantity) > approved_notional
    ):
        return "next_open_limit_exceeds_approved_notional"

    try:
        broker_session = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="next_open_pre_submit_quote_guard",
            reason=f"refresh guarded next-open quote for {item.symbol}",
        )
        quote = adapter.get_current_price(
            broker_session.access_token or "",
            symbol=item.symbol,
            exchange=_plan_item_exchange(item),
        )
    except Exception:
        return "next_open_quote_unavailable"
    if quote is None:
        return "next_open_quote_missing"
    if quote.symbol.strip().upper() != item.symbol.strip().upper():
        return "next_open_quote_identity_mismatch"
    if quote.price <= 0:
        return "next_open_quote_invalid"
    context_getter = getattr(adapter, "get_live_market_context", None)
    if not callable(context_getter):
        return "next_open_executable_quote_unavailable"
    try:
        context = context_getter(
            broker_session.access_token or "",
            symbol=item.symbol,
            exchange=_plan_item_exchange(item),
            reference_price=quote.price,
        )
    except Exception:
        context = None
    if context is None:
        return "next_open_executable_quote_unavailable"
    if context.symbol.strip().upper() != item.symbol.strip().upper():
        return "next_open_quote_identity_mismatch"
    if context.ask is None or context.ask <= 0:
        return "next_open_executable_ask_missing"
    executable_price = context.ask
    observed_at = context.observed_at
    freshness_source = context.freshness_source
    if not _is_trusted_exchange_freshness_source(freshness_source):
        return "next_open_quote_timestamp_untrusted"
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        return "next_open_quote_timestamp_invalid"
    observed_at = _normalize_utc(observed_at)
    reference_at = _normalize_utc(reference_at)
    if observed_at - reference_at > timedelta(
        seconds=settings.monitoring_max_quote_future_skew_seconds
    ):
        return "next_open_quote_future_skew"
    if reference_at - observed_at > timedelta(
        seconds=settings.monitoring_max_quote_age_seconds
    ):
        return "next_open_quote_stale"
    market_date = item.plan.market_date
    if (
        _plan_item_exchange(item) == "KRX"
        and (
            market_date is None
            or observed_at.astimezone(KRX_TIMEZONE).date() != market_date
        )
    ):
        return "next_open_quote_market_date_mismatch"

    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    target_price = _first_positive_decimal(
        risk_context,
        policy,
        keys=("target_price", "take_profit_price", "first_take_profit_price"),
    )
    stop_price = _first_positive_decimal(
        risk_context,
        policy,
        keys=("stop_price", "stop_loss_price", "risk_stop_price"),
    )
    if target_price is None:
        return "next_open_target_missing"
    if stop_price is None:
        return "next_open_stop_missing"
    if executable_price <= stop_price or command.limit_price <= stop_price:
        return "next_open_entry_at_or_below_stop"
    if executable_price >= target_price or command.limit_price >= target_price:
        return "next_open_entry_at_or_above_target"

    item.quantity_policy = compact_json_object({
        **policy,
        "pre_submit_quote": {
            "source": "broker_current_price",
            "symbol": quote.symbol.strip().upper(),
            "exchange": quote.exchange.strip().upper(),
            "price": str(executable_price),
            "observed_at": observed_at.isoformat(),
            "validated_at": reference_at.isoformat(),
            "freshness_source": freshness_source,
        },
    })
    return None


def _is_verified_watch_sizing_approval(item: ExecutionPlanItem) -> bool:
    policy = item.quantity_policy
    if not isinstance(policy, dict) or policy.get("source") != "exec_agent_watch_sizing":
        return False
    provenance = policy.get("approval_provenance")
    decision = policy.get("exec_agent_decision")
    return bool(
        isinstance(provenance, dict)
        and provenance.get("kind") == "verified_exec_agent_watch_sizing"
        and isinstance(decision, dict)
        and str(decision.get("action") or "").strip().lower() == "approve"
        and isinstance(decision.get("runtime_provenance"), dict)
    )


def _first_positive_decimal(
    *payloads: object,
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


def _resolved_place_order_idempotency_key(
    *,
    settings: Settings,
    db: Session,
    job: ExecutionJob,
) -> str | None:
    if job.job_type != BROKER_ORDER_RETRY_JOB_TYPE:
        return _place_order_idempotency_key(job)

    persisted_keys = {
        value.strip()
        for value in db.scalars(
            select(ExecutionFailure.retry_order_idempotency_key).where(
                ExecutionFailure.mode == settings.runtime_mode,
                ExecutionFailure.retry_job_id == job.id,
                ExecutionFailure.retry_order_idempotency_key.is_not(None),
            )
        ).all()
        if isinstance(value, str) and value.strip()
    }
    if len(persisted_keys) == 1:
        return persisted_keys.pop()
    if persisted_keys:
        return None

    legacy_keys = {
        value
        for value in db.scalars(
            select(BrokerOrder.idempotency_key).where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.execution_plan_item_id
                == job.execution_plan_item_id,
                BrokerOrder.idempotency_key.endswith(
                    f":{job.id}",
                    autoescape=True,
                ),
            )
        ).all()
        if value.startswith("retry:")
    }
    if len(legacy_keys) == 1:
        return legacy_keys.pop()
    return None


def _next_open_buy_quantity(plan_item: ExecutionPlanItem) -> int | None:
    quantity_policy = plan_item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return None

    kind = quantity_policy.get("kind") or quantity_policy.get("type")
    if kind in {"shares", "fixed_quantity"}:
        return _parse_positive_int(quantity_policy.get("quantity"))

    if kind == "exec_agent_sized":
        return None

    return None


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int) and value > 0:
        return value
    if (
        isinstance(value, Decimal)
        and value.is_finite()
        and value == value.to_integral_value()
        and value > 0
    ):
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
    if not parsed.is_finite() or parsed <= 0:
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


def _next_market_open_utc(
    market_date: date,
    *,
    plan_item: ExecutionPlanItem | None = None,
) -> datetime:
    if plan_item is not None and plan_item.asset_class == "us_equity":
        local_open = datetime.combine(
            market_date,
            time(hour=9, minute=30),
            tzinfo=US_EASTERN_TIMEZONE,
        )
        return local_open.astimezone(UTC)
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


def _next_dispatchable_job_at(
    *,
    settings: Settings,
    db: Session,
    triggered_at: datetime,
) -> datetime | None:
    row = db.execute(
        select(ExecutionJob.id, ExecutionJob.scheduled_for)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(DISPATCHABLE_JOB_STATUSES),
            ExecutionPlanItem.status.in_(CLAIMABLE_PLAN_ITEM_STATUSES),
        )
        .order_by(
            ExecutionJob.scheduled_for.is_not(None).asc(),
            ExecutionJob.scheduled_for.asc(),
            ExecutionJob.created_at.asc(),
            ExecutionJob.id.asc(),
        )
        .limit(1)
    ).first()
    if row is None:
        return None
    if row.scheduled_for is None:
        return triggered_at
    return _normalize_utc(row.scheduled_for)


def _guard_initial_next_open_submission(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    job: ExecutionJob,
    triggered_at: datetime,
    dispatch_started: float,
    calendar_state_by_date: dict[date, bool | None],
) -> str | None:
    if job.job_type != NEXT_OPEN_BUY_JOB_TYPE:
        return None
    existing_order = db.scalar(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == _place_order_idempotency_key(job),
        ).limit(1)
    )
    if existing_order is not None and _broker_submission_may_have_started(
        db=db,
        order=existing_order,
    ):
        return None
    if job.scheduled_for is None:
        expired = _expire_initial_next_open_job(
            db=db,
            job=job,
            triggered_at=triggered_at,
            last_error="next_open_schedule_missing",
            reason="initial next-open submission requires an explicit schedule",
        )
        return "expired" if expired else "state_changed"
    market_date = job.plan_item.plan.market_date
    is_us_equity = job.plan_item.asset_class == "us_equity"
    market_timezone = US_EASTERN_TIMEZONE if is_us_equity else KRX_TIMEZONE
    local_date = triggered_at.astimezone(market_timezone).date()
    expected_open = (
        _next_market_open_utc(market_date, plan_item=job.plan_item)
        if market_date is not None
        else None
    )
    deadline = (expected_open or _normalize_utc(job.scheduled_for)) + timedelta(
        seconds=settings.next_open_buy_initial_submission_grace_seconds
    )
    within_submission_window = (
        market_date is not None
        and local_date == market_date
        and expected_open is not None
        and expected_open <= triggered_at <= deadline
    )
    if not within_submission_window:
        expired = _expire_initial_next_open_job(
            db=db,
            job=job,
            triggered_at=triggered_at,
            last_error="missed_next_open_submission_window",
            reason="initial next-open submission missed its configured grace window",
        )
        return "expired" if expired else "state_changed"
    if is_us_equity:
        evaluated_at = _monotonic_cycle_reference_at(
            triggered_at=triggered_at,
            dispatch_started=dispatch_started,
        )
        if evaluated_at > deadline:
            expired = _expire_initial_next_open_job(
                db=db,
                job=job,
                triggered_at=evaluated_at,
                last_error="missed_next_open_submission_window",
                reason="initial next-open submission crossed its configured grace window",
            )
            return "expired" if expired else "state_changed"
        if expected_open is None or not is_us_regular_session(
            expected_open.astimezone(US_EASTERN_TIMEZONE)
        ):
            expired = _expire_initial_next_open_job(
                db=db,
                job=job,
                triggered_at=triggered_at,
                last_error="us_session_not_authoritatively_open",
                reason="US calendar did not authorize the requested next-open session",
            )
            return "expired" if expired else "state_changed"
        return None
    if market_date.weekday() >= 5:
        expired = _expire_initial_next_open_job(
            db=db,
            job=job,
            triggered_at=triggered_at,
            last_error="krx_session_not_authoritatively_open",
            reason="KRX calendar did not authorize the requested session date",
        )
        return "expired" if expired else "state_changed"
    if market_date in calendar_state_by_date:
        market_open = calendar_state_by_date[market_date]
    else:
        calendar_checker = getattr(adapter, "is_krx_trading_day", None)
        try:
            if not callable(calendar_checker):
                market_open = None
            else:
                broker_session = ensure_authenticated_broker_session(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    source="next_open_krx_calendar_guard",
                    reason="verify KRX session before first next-open submission",
                )
                market_open = calendar_checker(
                    broker_session.access_token or "",
                    market_date=market_date,
                )
        except Exception:
            market_open = None
        calendar_state_by_date[market_date] = market_open
    evaluated_at = _monotonic_cycle_reference_at(
        triggered_at=triggered_at,
        dispatch_started=dispatch_started,
    )
    if evaluated_at > deadline:
        elapsed_error = (
            "krx_calendar_unavailable_at_deadline"
            if market_open is None
            else (
                "krx_session_not_authoritatively_open"
                if market_open is False
                else "missed_next_open_submission_window"
            )
        )
        expired = _expire_initial_next_open_job(
            db=db,
            job=job,
            triggered_at=evaluated_at,
            last_error=elapsed_error,
            reason=(
                "KRX calendar proof remained unavailable through the grace window"
                if market_open is None
                else (
                    "KRX calendar did not authorize the requested session date"
                    if market_open is False
                    else "initial next-open market proof completed after the grace window"
                )
            ),
        )
        return "expired" if expired else "state_changed"
    if market_open is True:
        return None
    if market_open is None and evaluated_at < deadline:
        if not _lock_initial_next_open_guard_mutation(db=db, job=job):
            return "state_changed"
        set_execution_job_status(
            db=db,
            job=job,
            status="retrying",
            source="execution_job_dispatch",
            last_error="krx_calendar_unavailable",
            plan_item_status="next_open_buy_retry_pending",
            reason="KRX calendar proof is temporarily unavailable",
            occurred_at=evaluated_at,
        )
        job.scheduled_for = min(evaluated_at + timedelta(seconds=15), deadline)
        db.commit()
        return "deferred"
    terminal_error = (
        "krx_calendar_unavailable_at_deadline"
        if market_open is None
        else "krx_session_not_authoritatively_open"
    )
    expired = _expire_initial_next_open_job(
        db=db,
        job=job,
        triggered_at=triggered_at,
        last_error=terminal_error,
        reason="KRX calendar did not authorize the requested session date",
    )
    return "expired" if expired else "state_changed"


def _monotonic_cycle_reference_at(
    *,
    triggered_at: datetime,
    dispatch_started: float,
) -> datetime:
    return triggered_at + timedelta(
        seconds=max(0.0, monotonic() - dispatch_started)
    )


def _lock_initial_next_open_guard_mutation(
    *,
    db: Session,
    job: ExecutionJob,
) -> bool:
    with db.no_autoflush:
        if db.get_bind().dialect.name == "sqlite":
            db.execute(
                update(ExecutionJob)
                .where(ExecutionJob.id == job.id)
                .values(id=ExecutionJob.id)
            )
        state = db.execute(
            select(ExecutionJob.status, ExecutionPlanItem.status)
            .join(ExecutionJob.plan_item)
            .where(ExecutionJob.id == job.id)
            .with_for_update()
        ).one_or_none()
    claimable_statuses = CLAIMABLE_PLAN_ITEM_STATUSES_BY_JOB_TYPE.get(
        NEXT_OPEN_BUY_JOB_TYPE,
        frozenset(),
    )
    if (
        state is None
        or state[0] not in DISPATCHABLE_JOB_STATUSES
        or state[1] not in claimable_statuses
    ):
        db.rollback()
        db.refresh(job)
        db.refresh(job.plan_item, attribute_names=["status"])
        return False
    db.refresh(job)
    db.refresh(job.plan_item, attribute_names=["status"])
    return True


def _expire_initial_next_open_job(
    *,
    db: Session,
    job: ExecutionJob,
    triggered_at: datetime,
    last_error: str,
    reason: str,
) -> bool:
    if not _lock_initial_next_open_guard_mutation(db=db, job=job):
        return False
    set_execution_job_status(
        db=db,
        job=job,
        status="expired",
        source="execution_job_dispatch",
        last_error=last_error,
        plan_item_status="next_open_buy_expired",
        reason=reason,
        occurred_at=triggered_at,
    )
    db.commit()
    return True


def _broker_submission_may_have_started(*, db: Session, order: BrokerOrder) -> bool:
    if order.broker_order_id is not None or order.submitted_at is not None:
        return True
    if order.status in {
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
    }:
        return True
    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.broker_order_pk == order.id)
    ).all()
    return any(
        isinstance(event.payload, dict)
        and event.payload.get("broker_call_started") is True
        for event in events
    )


def _defer_us_next_open_job_outside_session(
    *,
    db: Session,
    job: ExecutionJob,
    triggered_at: datetime,
) -> bool:
    if (
        job.job_type not in {NEXT_OPEN_BUY_JOB_TYPE, BROKER_ORDER_RETRY_JOB_TYPE}
        or job.plan_item.asset_class != "us_equity"
    ):
        return False
    local = triggered_at.astimezone(US_EASTERN_TIMEZONE)
    if is_us_regular_session(local):
        return False
    claimable_plan_item_statuses = CLAIMABLE_PLAN_ITEM_STATUSES_BY_JOB_TYPE.get(
        job.job_type,
        frozenset(),
    )
    defer_state = db.execute(
        select(ExecutionJob.status, ExecutionPlanItem.status)
        .join(ExecutionJob.plan_item)
        .where(ExecutionJob.id == job.id)
        .with_for_update()
    ).one_or_none()
    if defer_state is None or defer_state[0] not in DISPATCHABLE_JOB_STATUSES:
        return False
    db.refresh(job)
    db.refresh(job.plan_item, attribute_names=["status"])
    next_open = next_us_regular_open(triggered_at)
    reason = f"deferred KIS US-equity order outside New York regular session until {next_open.isoformat()}"
    job.scheduled_for = next_open
    if defer_state[1] not in claimable_plan_item_statuses:
        # Risk-off (or another terminal transition) may have changed the plan
        # item after the due-job query.  The job can still be moved out of the
        # hot polling window, but its safety status must remain untouched.
        job.last_error = reason
        db.commit()
        return True
    set_execution_job_status(
        db=db,
        job=job,
        status="scheduled",
        source="execution_job_dispatch",
        last_error=reason,
        plan_item_status=(
            "next_open_buy_scheduled"
            if job.job_type == NEXT_OPEN_BUY_JOB_TYPE
            else _retrying_plan_item_status(job.job_type)
        ),
        reason=reason,
        occurred_at=triggered_at,
    )
    db.commit()
    return True


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
    return _is_stale_running_job_lock(
        settings=settings,
        locked_at=job.locked_at,
        triggered_at=triggered_at,
        timeout_multiplier=_execution_job_timeout_multiplier(
            provider=job.plan_item.provider,
            asset_class=job.plan_item.asset_class,
            side=job.plan_item.side,
        ),
    )


def _execution_job_stale_after_seconds(
    *,
    settings: Settings,
    job: ExecutionJob,
) -> int:
    return _execution_job_stale_after_seconds_for_multiplier(
        settings=settings,
        timeout_multiplier=_execution_job_timeout_multiplier(
            provider=job.plan_item.provider,
            asset_class=job.plan_item.asset_class,
            side=job.plan_item.side,
        ),
    )


def _execution_job_timeout_multiplier(
    *,
    provider: object,
    asset_class: object,
    side: object,
) -> int:
    if (
        str(provider or "").strip().lower() != "kis"
        or str(asset_class or "").strip().lower() != "us_equity"
    ):
        return STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER
    snapshot_requests = (
        KIS_US_ACCOUNT_SNAPSHOT_PAGINATED_ENDPOINTS
        * KIS_OVERSEAS_PAGINATION_PAGE_LIMIT
        + KIS_US_ACCOUNT_SNAPSHOT_NONPAGINATED_REQUESTS
    )
    snapshot_count = (
        KIS_US_ACCOUNT_GATE_EXCHANGE_COUNT
        if str(side or "").strip().lower() == "buy"
        else 1
    )
    return max(
        STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER,
        snapshot_requests * snapshot_count
        + KIS_US_FINAL_BOUNDARY_REQUEST_ALLOWANCE,
    )


def _execution_job_stale_after_seconds_for_multiplier(
    *,
    settings: Settings,
    timeout_multiplier: int,
) -> int:
    return max(
        (
            settings.kis_token_issuance_lease_seconds
            + max(
                settings.kis_request_timeout_seconds,
                settings.alpaca_request_timeout_seconds,
                settings.upbit_request_timeout_seconds,
            )
            * max(timeout_multiplier, STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER)
            + STALE_RUNNING_JOB_GRACE_SECONDS
        ),
        MIN_STALE_RUNNING_JOB_SECONDS,
    )


def _is_stale_running_job_lock(
    *,
    settings: Settings,
    locked_at: datetime | None,
    triggered_at: datetime,
    timeout_multiplier: int = STALE_RUNNING_JOB_TIMEOUT_MULTIPLIER,
) -> bool:
    if locked_at is None:
        return True
    stale_after_seconds = _execution_job_stale_after_seconds_for_multiplier(
        settings=settings,
        timeout_multiplier=timeout_multiplier,
    )
    cutoff = triggered_at - timedelta(seconds=stale_after_seconds)
    return _normalize_utc(locked_at) <= cutoff


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _normalize_utc(value).isoformat()
