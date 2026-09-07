import logging
import faulthandler
from hmac import compare_digest
from ipaddress import ip_address
import signal
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.api.routes.broker import router as broker_router
from app.api.routes.controls import router as controls_router
from app.api.routes.execution import router as execution_router
from app.api.routes.health import router as health_router
from app.api.routes.runtime import router as runtime_router
from app.broker.base import BrokerAdapter
from app.broker.kis import KisBrokerAdapter
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_db_engine, create_session_factory
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
from app.services.alpaca_crypto_runtime import AlpacaCryptoRuntimeCoordinator
from app.services.alpaca_us_equity_runtime import AlpacaUsEquityRuntimeCoordinator
from app.services.monitoring_price_source import build_default_monitoring_price_source
from app.services.runtime import RuntimeMetadata
from app.services.runtime_controls import ensure_runtime_control_state

LOGGER = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | None = None,
    broker_adapter_factory: Callable[[Settings], BrokerAdapter] | None = None,
    monitoring_price_source: object | None = None,
) -> FastAPI:
    app_settings = settings or get_settings()
    _validate_api_security(app_settings)
    configure_logging(app_settings)
    try:
        faulthandler.register(signal.SIGUSR1, all_threads=True, chain=False)
    except Exception:
        LOGGER.exception("faulthandler_signal_registration_failed")

    db_engine = engine or create_db_engine(app_settings)
    db_session_factory = session_factory or create_session_factory(db_engine)
    runtime_metadata = RuntimeMetadata(
        started_at=datetime.now(UTC),
    )
    resolved_broker_adapter_factory = broker_adapter_factory or (
        lambda current_settings: KisBrokerAdapter(settings=current_settings)
    )
    resolved_monitoring_price_source = monitoring_price_source
    if resolved_monitoring_price_source is None:
        resolved_monitoring_price_source = build_default_monitoring_price_source(
            settings=app_settings,
            session_factory=db_session_factory,
            broker_adapter_factory=resolved_broker_adapter_factory,
        )
    autonomous_runtime = AutonomousRuntimeCoordinator(
        settings=app_settings,
        session_factory=db_session_factory,
        broker_adapter_factory=resolved_broker_adapter_factory,
        monitoring_price_source=resolved_monitoring_price_source,
    )
    alpaca_crypto_runtime = AlpacaCryptoRuntimeCoordinator(
        settings=app_settings,
        session_factory=db_session_factory,
    )
    alpaca_us_equity_runtime = AlpacaUsEquityRuntimeCoordinator(
        settings=app_settings,
        session_factory=db_session_factory,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        LOGGER.info(
            "service_starting",
            extra={
                "service": app_settings.service_name,
                "environment": app_settings.environment,
                "runtime_mode": app_settings.runtime_mode,
            },
        )
        if app_settings.autonomous_runtime_enabled:
            # Bootstrap the fail-closed control row before any background
            # worker can race an operator request for the same singleton.
            with db_session_factory() as db:
                ensure_runtime_control_state(settings=app_settings, db=db)
        await autonomous_runtime.start()
        await alpaca_crypto_runtime.start()
        await alpaca_us_equity_runtime.start()
        yield
        await alpaca_us_equity_runtime.stop()
        await alpaca_crypto_runtime.stop()
        await autonomous_runtime.stop()
        LOGGER.info("service_stopping", extra={"service": app_settings.service_name})

    app = FastAPI(
        title=app_settings.service_name,
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = app_settings
    app.state.db_engine = db_engine
    app.state.db_session_factory = db_session_factory
    app.state.runtime_metadata = runtime_metadata
    app.state.broker_adapter_factory = resolved_broker_adapter_factory
    app.state.autonomous_runtime = autonomous_runtime
    app.state.alpaca_crypto_runtime = alpaca_crypto_runtime
    app.state.alpaca_us_equity_runtime = alpaca_us_equity_runtime

    @app.middleware("http")
    async def require_api_bearer_token(request: Request, call_next):  # type: ignore[no-untyped-def]
        configured_token = (app_settings.api_bearer_token or "").strip()
        if request.method == "OPTIONS" or request.url.path == "/health":
            return await call_next(request)
        if configured_token:
            scheme, _, credentials = request.headers.get("Authorization", "").partition(" ")
            authorized = (
                scheme.lower() == "bearer"
                and bool(credentials)
                and compare_digest(credentials, configured_token)
            )
        else:
            # Uvicorn CLI flags can bind more broadly than Settings.api_host.
            # With no token, enforce loopback at request time as a second,
            # independent boundary so a mismatched bind cannot expose writes.
            authorized = _request_is_loopback(request)
        if not authorized:
            return JSONResponse(
                status_code=401,
                content={"detail": "Unauthorized"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await call_next(request)

    app.include_router(health_router)
    app.include_router(runtime_router)
    app.include_router(broker_router)
    app.include_router(controls_router)
    app.include_router(execution_router)
    return app


def _validate_api_security(settings: Settings) -> None:
    if _is_loopback_host(settings.api_host):
        return
    if (settings.api_bearer_token or "").strip():
        return
    raise ValueError(
        "KAI_EXEC_API_BEARER_TOKEN is required when KAI_EXEC_API_HOST is not loopback"
    )


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().strip("[]").lower()
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


def _request_is_loopback(request: Request) -> bool:
    client_host = request.client.host if request.client is not None else ""
    # Starlette's in-process TestClient uses a non-network sentinel address.
    if client_host == "testclient":
        return True
    request_host = request.url.hostname or ""
    return _is_loopback_host(client_host) and _is_loopback_host(request_host)


app = create_app()
