from typing import Annotated

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_runtime_metadata, get_settings
from app.core.config import Settings
from app.services.runtime import RuntimeMetadata, build_runtime_status
from app.services.runtime_controls import ensure_runtime_control_state, runtime_safety_payload

router = APIRouter(prefix="/runtime", tags=["runtime"])


@router.get("/status")
def runtime_status(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    runtime: RuntimeMetadata = get_runtime_metadata(request)
    return build_runtime_status(settings=settings, runtime=runtime, db=db)


@router.get("/safety")
def runtime_safety(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    state = ensure_runtime_control_state(settings=settings, db=db)
    return runtime_safety_payload(state)
