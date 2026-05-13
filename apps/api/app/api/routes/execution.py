from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_broker_adapter, get_db_session, get_settings
from app.api.schemas.execution import (
    ExecutionFailureStatusUpdateRequest,
    ExecutionPlanRequest,
    MonitoringEvaluationRequest,
    PlaceOrderRequest,
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
)
from app.services.execution_failures import (
    ExecutionFailureStatusUpdateCommand,
    list_execution_failures,
    update_execution_failure_status,
)
from app.services.execution_jobs import dispatch_due_execution_jobs, list_execution_jobs
from app.services.monitoring import evaluate_monitoring

router = APIRouter(tags=["execution"])


@router.get("/execution-plans")
def execution_plans(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_plans(
        settings=settings,
        db=db,
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
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: PlaceOrderRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
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
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_fills(settings=settings, db=db)


@router.get("/positions")
def positions(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    settings: Settings = get_settings(request)
    return list_execution_positions(settings=settings, db=db)


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
