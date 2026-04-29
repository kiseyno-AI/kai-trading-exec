from typing import Annotated

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_settings
from app.api.schemas.execution import ExecutionPlanRequest
from app.core.config import Settings
from app.services.execution_api import (
    create_execution_plan,
    list_broker_orders,
    list_execution_fills,
    list_execution_plans,
    list_execution_positions,
)

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
