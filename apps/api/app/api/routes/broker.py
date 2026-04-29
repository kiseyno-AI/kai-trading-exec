from typing import Annotated

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_broker_adapter, get_db_session, get_settings
from app.api.schemas.broker import BrokerSessionActionRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_auth import (
    BrokerSessionCommand,
    authenticate_broker_session,
    build_broker_session_summary,
    revoke_broker_session,
)

router = APIRouter(prefix="/broker", tags=["broker"])


@router.get("/session")
def broker_session(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    return build_broker_session_summary(
        settings=settings,
        db=db,
        adapter=adapter,
    )


@router.post("/session/authenticate")
def authenticate_broker_session_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: BrokerSessionActionRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = authenticate_broker_session(
        settings=settings,
        db=db,
        command=BrokerSessionCommand(
            actor=payload.actor,
            reason=payload.reason,
            source=payload.source,
            force=payload.force,
        ),
        adapter=adapter,
    )
    return JSONResponse(status_code=result.status_code, content=result.payload)


@router.post("/session/revoke")
def revoke_broker_session_handler(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    payload: BrokerSessionActionRequest = Body(...),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = revoke_broker_session(
        settings=settings,
        db=db,
        command=BrokerSessionCommand(
            actor=payload.actor,
            reason=payload.reason,
            source=payload.source,
            force=payload.force,
        ),
        adapter=adapter,
    )
    return JSONResponse(status_code=result.status_code, content=result.payload)
