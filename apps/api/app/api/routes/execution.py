from typing import Annotated

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_broker_adapter, get_db_session, get_settings
from app.api.schemas.execution import ExecutionPlanRequest, PlaceOrderRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_orders import place_broker_order
from app.services.execution_api import (
    create_execution_plan,
    list_broker_orders,
    list_execution_fills,
    list_execution_plans,
    list_execution_positions,
)
from app.services.execution_jobs import dispatch_due_execution_jobs, list_execution_jobs

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
    payload: ExecutionPlanRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = create_execution_plan(
        settings=settings,
        db=db,
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
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    return list_broker_orders(db)


@router.get("/executions")
def executions(
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    return list_execution_fills(db)


@router.get("/positions")
def positions(
    db: Annotated[Session, Depends(get_db_session)],
) -> list[dict[str, object]]:
    return list_execution_positions(db)
