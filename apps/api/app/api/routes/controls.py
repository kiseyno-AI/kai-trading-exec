from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_settings
from app.api.schemas.safety import KillSwitchResetRequest, SafetyControlRequest
from app.core.config import Settings
from app.services.runtime_controls import (
    ControlCommand,
    LEGACY_LIFESPAN_SHUTDOWN_SOURCE,
    enable_kill_switch,
    pause_runtime,
    reset_kill_switch,
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
    if (
        command is not None
        and command.source.strip().lower() == LEGACY_LIFESPAN_SHUTDOWN_SOURCE
    ):
        raise HTTPException(
            status_code=422,
            detail="source is reserved for historical system-generated shutdown events",
        )
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
    if result.changed:
        runtime = getattr(request.app.state, "autonomous_runtime", None)
        invalidate_dispatch = getattr(
            runtime,
            "invalidate_dispatch_schedule",
            None,
        )
        if callable(invalidate_dispatch):
            invalidate_dispatch(reason="runtime_resumed")
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


@router.post("/kill-switch/reset")
def kill_switch_reset_control(
    request: Request,
    command: KillSwitchResetRequest,
    db: Annotated[Session, Depends(get_db_session)],
) -> JSONResponse:
    settings: Settings = get_settings(request)
    result = reset_kill_switch(
        settings=settings,
        db=db,
        command=_command(command),
        expected_current_event_id=command.expected_current_event_id,
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
