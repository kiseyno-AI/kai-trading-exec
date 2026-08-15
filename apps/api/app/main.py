import logging
import faulthandler
import signal
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Callable

from fastapi import FastAPI
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
from app.services.monitoring_price_source import build_default_monitoring_price_source
from app.services.runtime import RuntimeMetadata

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
        await autonomous_runtime.start()
        yield
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

    app.include_router(health_router)
    app.include_router(runtime_router)
    app.include_router(broker_router)
    app.include_router(controls_router)
    app.include_router(execution_router)
    return app


app = create_app()
