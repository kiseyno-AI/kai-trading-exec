from typing import Annotated

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_runtime_metadata, get_settings
from app.core.config import Settings
from app.db.session import pool_status
from app.services.health import build_health_payload
from app.services.runtime import RuntimeMetadata

router = APIRouter(tags=["health"])


@router.get("/health")
def health(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    runtime: RuntimeMetadata = get_runtime_metadata(request)
    payload = build_health_payload(
        settings=settings,
        runtime=runtime,
        db=db,
        runtime_startup_gate=getattr(
            request.app.state,
            "runtime_startup_gate",
            None,
        ),
    )
    dependencies = payload.get("dependencies")
    if isinstance(dependencies, dict):
        database = dependencies.get("database")
        if isinstance(database, dict):
            database["pool"] = pool_status(request.app.state.db_engine)
    return payload
