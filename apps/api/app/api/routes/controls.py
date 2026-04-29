from typing import Annotated

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_settings
from app.api.schemas.safety import SafetyControlRequest
from app.core.config import Settings
from app.services.runtime_controls import (
    ControlCommand,
    enable_kill_switch,
    pause_runtime,
    resume_runtime,
)

router = APIRouter(prefix="/controls", tags=["controls"])


@router.post("/pause")
def pause_control(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    command: SafetyControlRequest | None = Body(default=None),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = pause_runtime(
        settings=settings,
        db=db,
        command=_command(command),
    )
    return JSONResponse(status_code=result.status_code, content=result.payload())


@router.post("/resume")
def resume_control(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    command: SafetyControlRequest | None = Body(default=None),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = resume_runtime(
        settings=settings,
        db=db,
        command=_command(command),
    )
    return JSONResponse(status_code=result.status_code, content=result.payload())


@router.post("/kill-switch")
def kill_switch_control(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    command: SafetyControlRequest | None = Body(default=None),
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = enable_kill_switch(
        settings=settings,
        db=db,
        command=_command(command),
    )
    return JSONResponse(status_code=result.status_code, content=result.payload())


def _command(command: SafetyControlRequest | None) -> ControlCommand:
    if command is None:
        return ControlCommand()
    return ControlCommand(
        actor=command.actor,
        source=command.source,
        reason=command.reason,
    )
