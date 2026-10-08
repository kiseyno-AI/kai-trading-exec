from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import (
    get_broker_adapter,
    get_broker_adapter_for_provider,
    get_db_session,
    get_settings,
)
from app.api.schemas.execution import (
    CoreExecutionActivationRequestV4,
    CoreExecutionPlanRequestV4,
    CoreExecutionRevocationRequestV4,
    ExecutionFailureStatusUpdateRequest,
    ExecutionPlanRequest,
    MonitoringEvaluationRequest,
    PlaceOrderRequest,
    ShortTermWatcherDecisionRequest,
    StaleBrokerOrderReconciliationRequest,
)
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_orders import place_broker_order
from app.services.execution_api import (
    ExecutionPositionScopeError,
    create_execution_plan,
    get_runtime_symbol_detail,
    list_broker_orders,
    list_execution_fills,
    list_execution_plans,
    list_execution_positions,
    list_runtime_symbols,
    preview_execution_plan,
)
from app.services.core_handoff_v4 import (
    activate_core_execution_plan_v4,
    create_core_execution_plan_v4,
    list_core_execution_plans_v4,
    revoke_core_execution_plan_v4,
)
from app.services.execution_failures import (
    ExecutionFailureStatusUpdateCommand,
    list_execution_failures,
    update_execution_failure_status,
)
from app.services.execution_jobs import dispatch_due_execution_jobs, list_execution_jobs
from app.services.monitoring import evaluate_monitoring
from app.services.reconciliation import (
    LegacyManualReviewApprovalConflictError,
    reconcile_stale_nonterminal_broker_orders,
)
from app.services.short_term_watcher import evaluate_short_term_watcher_decision

router = APIRouter(tags=["execution"])


def _legacy_core_handoff_rejection(
    payload: ExecutionPlanRequest,
) -> JSONResponse | None:
    extras = payload.model_extra or {}
    metadata = extras.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    core_marker = (
        payload.source == "kai-trading-core"
        or str(metadata.get("schema_version") or "").startswith(
            "signal_decider_exec_plan"
        )
        or str(metadata.get("contract_version") or "").startswith(
            "execution_handoff_contract"
        )
        or str(metadata.get("required_capability") or "").startswith(
            "delegated_final_full_cost_gate_recheck"
        )
    )
    if not core_marker:
        return None
    return JSONResponse(
        status_code=426,
        content={
            "detail": "Core delegated plans require the v4 execution handoff endpoint",
            "required_endpoint": "/v4/execution-plans",
        },
    )


@router.get("/execution-plans")
def execution_plans(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    external_plan_id: str | None = None,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_plans(
        settings=settings,
        db=db,
        external_plan_id=external_plan_id,
    )


@router.post("/execution-plans")
def create_execution_plan_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: ExecutionPlanRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    legacy_rejection = _legacy_core_handoff_rejection(payload)
    if legacy_rejection is not None:
        return legacy_rejection
    result = create_execution_plan(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
    )
    if result.status_code == 201 and result.payload.get("accepted_item_count"):
        _invalidate_execution_runtime(
            request,
            reason="execution_plan_created",
            monitoring=True,
            dispatch=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.get("/v4/execution-plans")
def core_execution_plans_v4(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    external_plan_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_core_execution_plans_v4(
        settings=settings,
        db=db,
        external_plan_id=external_plan_id,
        limit=limit,
        offset=offset,
    )


@router.post("/v4/execution-plans")
def create_core_execution_plan_v4_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: CoreExecutionPlanRequestV4 = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = create_core_execution_plan_v4(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
    )
    if result.status_code == 201 and result.payload.get("item_acks"):
        _invalidate_execution_runtime(
            request,
            reason="core_execution_plan_v4_created",
            monitoring=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/v4/execution-plans/revocations")
def revoke_core_execution_plan_v4_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    payload: CoreExecutionRevocationRequestV4 = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = revoke_core_execution_plan_v4(
        settings=settings,
        db=db,
        command=payload,
    )
    if result.status_code == 200:
        _invalidate_execution_runtime(
            request,
            reason="core_execution_plan_v4_revoked",
            monitoring=True,
            dispatch=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/v4/execution-plans/activations")
def activate_core_execution_plan_v4_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    payload: CoreExecutionActivationRequestV4 = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = activate_core_execution_plan_v4(
        settings=settings,
        db=db,
        command=payload,
    )
    if result.status_code == 200:
        _invalidate_execution_runtime(
            request,
            reason="core_execution_plan_v4_activated",
            monitoring=True,
            dispatch=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/execution-plans/preview")
def preview_execution_plan_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: ExecutionPlanRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    legacy_rejection = _legacy_core_handoff_rejection(payload)
    if legacy_rejection is not None:
        return legacy_rejection
    result = preview_execution_plan(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
    )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.get("/execution-jobs")
def execution_jobs(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_jobs(
        settings=settings,
        db=db,
    )


@router.post("/execution-jobs/dispatch")
def dispatch_execution_jobs(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = dispatch_due_execution_jobs(
        settings=settings,
        db=db,
        adapter=adapter,
    )
    if result.payload.get("jobs"):
        _invalidate_execution_runtime(
            request,
            reason="manual_execution_job_dispatch",
            monitoring=True,
            dispatch=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/short-term-watcher/decisions")
def short_term_watcher_decision(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: ShortTermWatcherDecisionRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = evaluate_short_term_watcher_decision(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
    )
    if not payload.dry_run and result.payload.get("reason_code") in {
        "submitted",
        "submission_not_accepted",
    }:
        _invalidate_execution_runtime(
            request,
            reason="short_term_watcher_decision",
            monitoring=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/monitoring/evaluate")
def monitoring_evaluate(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: MonitoringEvaluationRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = evaluate_monitoring(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
        auth_session_factory=request.app.state.db_session_factory,
    )
    if result.status_code < 500:
        _invalidate_execution_runtime(
            request,
            reason="manual_monitoring_evaluation",
            monitoring=True,
        )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/orders")
def place_order_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    payload: PlaceOrderRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    adapter = get_broker_adapter_for_provider(request, payload.provider)
    result = place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
    )
    _invalidate_execution_runtime(
        request,
        reason="manual_order_submission",
        monitoring=True,
    )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.get("/orders")
def orders(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    external_plan_id: str | None = None,
    symbols: str | None = None,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_broker_orders(
        settings=settings,
        db=db,
        external_plan_id=external_plan_id,
        symbols=_parse_symbols_filter(symbols),
    )


@router.post("/orders/reconcile-stale")
def reconcile_stale_orders(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: StaleBrokerOrderReconciliationRequest | None = Body(default=None),
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    command = payload or StaleBrokerOrderReconciliationRequest()
    if not command.dry_run and (not command.actor or not command.reason):
        raise HTTPException(
            status_code=422,
            detail="stale reconciliation apply requires actor and reason",
        )
    try:
        result = reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=adapter,
            older_than_seconds=command.older_than_seconds,
            symbols=set(command.symbols) if command.symbols is not None else None,
            dry_run=command.dry_run,
            actor=command.actor,
            operator_reason=command.reason,
            manual_review_approvals=command.manual_review_approvals,
            include_legacy_manual_review=True,
        )
    except LegacyManualReviewApprovalConflictError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not command.dry_run:
        _invalidate_execution_runtime(
            request,
            reason="stale_order_reconciliation",
            monitoring=True,
            dispatch=True,
        )
    return result


@router.get("/execution-failures")
def execution_failures(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    classification: str | None = None,
    status: str | None = None,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_failures(
        settings=settings,
        db=db,
        classification=classification,
        status=status,
    )


@router.patch("/execution-failures/{failure_id}")
def update_execution_failure(
    failure_id: str,
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    payload: ExecutionFailureStatusUpdateRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = update_execution_failure_status(
        settings=settings,
        db=db,
        failure_id=failure_id,
        command=ExecutionFailureStatusUpdateCommand(
            status=payload.status,
            actor=payload.actor,
            source=payload.source,
            reason=payload.reason,
        ),
    )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.get("/executions")
def executions(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    external_plan_id: str | None = None,
    symbols: str | None = None,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_fills(
        settings=settings,
        db=db,
        external_plan_id=external_plan_id,
        symbols=_parse_symbols_filter(symbols),
    )


@router.get("/positions")
def positions(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    symbols: str | None = None,
    external_plan_id: str | None = None,
    execution_plan_id: str | None = None,
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    symbol_filter = _parse_symbols_filter(symbols)
    try:
        return list_execution_positions(
            settings=settings,
            db=db,
            symbols=symbol_filter,
            include_related_plan_items=symbol_filter is None,
            external_plan_id=external_plan_id,
            execution_plan_id=execution_plan_id,
        )
    except ExecutionPositionScopeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/symbols")
def symbols(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_runtime_symbols(settings=settings, db=db)


@router.get("/symbols/{symbol}")
def symbol_detail(
    symbol: str,
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    payload = get_runtime_symbol_detail(
        settings=settings,
        db=db,
        symbol=symbol,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="symbol was not found")
    return payload


def _parse_symbols_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    symbols = {
        item.strip().upper()
        for item in value.split(",")
        if item.strip()
    }
    return symbols or None


def _invalidate_execution_runtime(
    request: Request,
    *,
    reason: str,
    monitoring: bool = False,
    dispatch: bool = False,
) -> None:
    runtime = getattr(request.app.state, "autonomous_runtime", None)
    if runtime is None:
        return
    if monitoring:
        invalidate_monitoring = getattr(
            runtime,
            "invalidate_monitoring_runtime_snapshot",
            None,
        )
        if callable(invalidate_monitoring):
            invalidate_monitoring(reason=reason)
    if dispatch:
        invalidate_dispatch = getattr(runtime, "invalidate_dispatch_schedule", None)
        if callable(invalidate_dispatch):
            invalidate_dispatch(reason=reason)
