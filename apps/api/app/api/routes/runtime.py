from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from app.api.dependencies import get_broker_adapter, get_db_session, get_runtime_metadata, get_settings
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.portfolio_risk import build_portfolio_risk_snapshot
from app.services.plan_judgment_actions import apply_hourly_plan_judgment_action
from app.services.runtime import (
    RuntimeMetadata,
    build_runtime_daily_board,
    build_runtime_safety,
    build_runtime_status,
)

router = APIRouter(prefix="/runtime", tags=["runtime"])


@router.get("/alpaca-crypto")
def alpaca_crypto_runtime_status(request: Request) -> dict[str, object]:
    return request.app.state.alpaca_crypto_runtime.snapshot()


@router.get("/alpaca-us-equity")
def alpaca_us_equity_runtime_status(request: Request) -> dict[str, object]:
    return request.app.state.alpaca_us_equity_runtime.snapshot()


@router.get("/status")
def runtime_status(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    runtime: RuntimeMetadata = get_runtime_metadata(request)
    autonomous_runtime = None
    if hasattr(request.app.state, "autonomous_runtime"):
        autonomous_runtime = request.app.state.autonomous_runtime.snapshot()
    return build_runtime_status(
        settings=settings,
        runtime=runtime,
        db=db,
        autonomous_runtime=autonomous_runtime,
        include_expensive_sections=not settings.autonomous_runtime_enabled,
    )


@router.get("/safety")
def runtime_safety(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    return build_runtime_safety(settings=settings, db=db)


@router.get("/daily-board")
def runtime_daily_board(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    runtime: RuntimeMetadata = get_runtime_metadata(request)
    autonomous_runtime = None
    if hasattr(request.app.state, "autonomous_runtime"):
        autonomous_runtime = request.app.state.autonomous_runtime.snapshot()
    return build_runtime_daily_board(
        settings=settings,
        runtime=runtime,
        db=db,
        autonomous_runtime=autonomous_runtime,
    )


@router.get("/portfolio-risk")
def runtime_portfolio_risk(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    adapter: Annotated[BrokerAdapter, Depends(get_broker_adapter)],
    market: Literal["all", "krx", "us"] = "all",
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    return build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        market_scope=market,
    )


@router.post("/plan-judgment/action")
async def runtime_plan_judgment_action(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
    payload: dict[str, object] = Body(...),
) -> dict[str, object]:
    settings: Settings = get_settings(request)
    risk_reasons_payload = payload.get("risk_reasons")
    risk_reasons = (
        [str(item) for item in risk_reasons_payload if str(item).strip()]
        if isinstance(risk_reasons_payload, list)
        else []
    )
    return apply_hourly_plan_judgment_action(
        settings=settings,
        db=db,
        risk_flags=max(0, int(payload.get("risk_flags") or 0)),
        risk_reasons=risk_reasons,
    )
