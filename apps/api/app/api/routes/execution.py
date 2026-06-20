from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import (
    get_broker_adapter,
    get_broker_adapter_for_provider,
    get_db_session,
    get_settings,
)
from app.api.schemas.execution import (
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
    create_execution_plan,
    get_runtime_symbol_detail,
    list_broker_orders,
    list_execution_fills,
    list_execution_plans,
    list_execution_positions,
    list_runtime_symbols,
    preview_execution_plan,
)
from app.services.execution_failures import (
    ExecutionFailureStatusUpdateCommand,
    list_execution_failures,
    update_execution_failure_status,
)
from app.services.execution_jobs import dispatch_due_execution_jobs, list_execution_jobs
from app.services.monitoring import evaluate_monitoring
from app.services.reconciliation import reconcile_stale_nonterminal_broker_orders
from app.services.short_term_watcher import evaluate_short_term_watcher_decision

router = APIRouter(tags=["execution"])


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
    result = create_execution_plan(
        settings=settings,
        db=db,
        adapter=adapter,
        command=payload,
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
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.get("/orders")
def orders(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_broker_orders(settings=settings, db=db)


@router.post("/orders/reconcile-stale")
def reconcile_stale_orders(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: StaleBrokerOrderReconciliationRequest | None = Body(default=None),
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    command = payload or StaleBrokerOrderReconciliationRequest()
    return reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        older_than_seconds=command.older_than_seconds,
        symbols=set(command.symbols) if command.symbols is not None else None,
    )


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
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    symbol_filter = _parse_symbols_filter(symbols)
    return list_execution_positions(
        settings=settings,
        db=db,
        symbols=symbol_filter,
        include_related_plan_items=symbol_filter is None,
    )


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
