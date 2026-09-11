import asyncio
import logging
import faulthandler
from hmac import compare_digest
from ipaddress import ip_address
import signal
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Awaitable, Callable

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
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import create_db_engine, create_session_factory
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
from app.services.alpaca_crypto_runtime import AlpacaCryptoRuntimeCoordinator
from app.services.alpaca_us_equity_runtime import AlpacaUsEquityRuntimeCoordinator
from app.services.broker_auth import build_broker_adapter
from app.services.monitoring_price_source import build_default_monitoring_price_source
from app.services.runtime import RuntimeMetadata
from app.services.runtime_controls import (
    ControlCommand,
    ensure_runtime_control_state,
    pause_runtime,
)
from app.services.schema import check_runtime_schema

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
    if broker_adapter_factory is None:
        shared_broker_adapter = build_broker_adapter(
            app_settings,
            provider=app_settings.runtime_broker_provider,
        )

        def resolved_broker_adapter_factory(
            _current_settings: Settings,
        ) -> BrokerAdapter:
            return shared_broker_adapter

    else:
        resolved_broker_adapter_factory = broker_adapter_factory
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
                "runtime_broker_provider": app_settings.runtime_broker_provider,
            },
        )
        automated_runtime_requested = _automated_runtime_requested(app_settings)
        runtime_schema: dict[str, object] = {"status": "not_checked"}
        if automated_runtime_requested:
            try:
                with db_session_factory() as db:
                    runtime_schema = check_runtime_schema(db)
            except Exception as exc:
                LOGGER.exception("runtime_schema_startup_check_failed")
                runtime_schema = {
                    "status": "error",
                    "error": type(exc).__name__,
                }
        schema_ready = runtime_schema.get("status") == "ok"
        if automated_runtime_requested and not schema_ready:
            position_readiness = _execution_position_readiness(runtime_schema)
            reason = (
                str(position_readiness["reason"])
                if position_readiness is not None
                else "runtime_schema_not_ready"
            )
            autonomous_runtime.mark_startup_blocked(reason)
            alpaca_crypto_runtime.mark_startup_blocked(reason)
            alpaca_us_equity_runtime.mark_startup_blocked(reason)
            alembic = runtime_schema.get("alembic")
            startup_gate: dict[str, object] = {
                "status": "blocked",
                "reason": reason,
                "runtime_schema_status": runtime_schema.get("status"),
                "alembic_status": (
                    alembic.get("status") if isinstance(alembic, dict) else None
                ),
            }
            if position_readiness is not None:
                startup_gate.update(
                    {
                        "remediation": position_readiness.get("remediation"),
                        "restart_required": True,
                    }
                )
            app.state.runtime_startup_gate = startup_gate
            LOGGER.error(
                "automated_runtime_startup_blocked",
                extra={"runtime_schema": runtime_schema},
            )
        elif automated_runtime_requested:
            app.state.runtime_startup_gate = {
                "status": "ready",
                "reason": None,
                "runtime_schema_status": "ok",
                "alembic_status": "ok",
            }
        else:
            app.state.runtime_startup_gate = {
                "status": "not_required",
                "reason": None,
                "runtime_schema_status": "not_checked",
                "alembic_status": None,
            }
        try:
            if automated_runtime_requested and schema_ready:
                if app_settings.autonomous_runtime_enabled:
                    # Bootstrap the fail-closed control row before any background
                    # worker can race an operator request for the same singleton.
                    with db_session_factory() as db:
                        ensure_runtime_control_state(settings=app_settings, db=db)
                await autonomous_runtime.start()
                await alpaca_crypto_runtime.start()
                await alpaca_us_equity_runtime.start()
            yield
        finally:
            coordinators = (
                ("alpaca_us_equity", alpaca_us_equity_runtime),
                ("alpaca_crypto", alpaca_crypto_runtime),
                ("autonomous", autonomous_runtime),
            )
            if automated_runtime_requested and schema_ready:
                await _shutdown_runtime_coordinators(
                    settings=app_settings,
                    session_factory=db_session_factory,
                    coordinators=coordinators,
                )
            else:
                await _stop_runtime_coordinators(coordinators)
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
    app.state.runtime_startup_gate = {
        "status": (
            "pending"
            if _automated_runtime_requested(app_settings)
            else "not_required"
        ),
        "reason": None,
        "runtime_schema_status": "not_checked",
        "alembic_status": None,
    }

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


def _automated_runtime_requested(settings: Settings) -> bool:
    return bool(
        settings.autonomous_runtime_enabled
        or settings.alpaca_crypto_monitoring_enabled
        or settings.alpaca_us_equity_monitoring_enabled
    )


def _execution_position_readiness(
    runtime_schema: dict[str, object],
) -> dict[str, object] | None:
    if runtime_schema.get("status") != "blocked":
        return None
    blocking_data = runtime_schema.get("blocking_data")
    if not isinstance(blocking_data, dict):
        return None
    position_readiness = blocking_data.get("execution_positions")
    return position_readiness if isinstance(position_readiness, dict) else None


async def _stop_runtime_coordinators(
    coordinators: tuple[tuple[str, object], ...],
) -> None:
    """Stop every coordinator even when an earlier shutdown reports failure."""

    shutdown_errors: list[Exception] = []
    cancellation: asyncio.CancelledError | None = None
    additional_cancellations: list[asyncio.CancelledError] = []
    coordinators_requiring_drain: list[tuple[str, object]] = []
    for name, coordinator in coordinators:
        try:
            await coordinator.stop()  # type: ignore[attr-defined]
        except asyncio.CancelledError as exc:
            coordinators_requiring_drain.append((name, coordinator))
            if cancellation is None:
                cancellation = exc
            else:
                additional_cancellations.append(exc)
            LOGGER.warning(
                "runtime_coordinator_stop_cancelled",
                extra={"runtime_coordinator": name},
            )
        except Exception as exc:
            coordinators_requiring_drain.append((name, coordinator))
            shutdown_errors.append(exc)
            LOGGER.exception(
                "runtime_coordinator_stop_failed",
                extra={"runtime_coordinator": name},
            )
    for name, coordinator in coordinators_requiring_drain:
        wait_stopped = getattr(coordinator, "wait_stopped", None)
        if not callable(wait_stopped):
            continue
        try:
            await wait_stopped()
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
            else:
                additional_cancellations.append(exc)
            LOGGER.warning(
                "runtime_coordinator_drain_cancelled",
                extra={"runtime_coordinator": name},
            )
        except Exception as exc:
            shutdown_errors.append(exc)
            LOGGER.exception(
                "runtime_coordinator_drain_failed",
                extra={"runtime_coordinator": name},
            )
    if cancellation is not None:
        deferred_failures: list[BaseException] = [
            *additional_cancellations,
            *shutdown_errors,
        ]
        if deferred_failures:
            if all(isinstance(exc, Exception) for exc in deferred_failures):
                cause: BaseException = ExceptionGroup(
                    "additional runtime coordinator shutdown failures",
                    [
                        exc
                        for exc in deferred_failures
                        if isinstance(exc, Exception)
                    ],
                )
            else:
                cause = BaseExceptionGroup(
                    "additional runtime coordinator shutdown failures",
                    deferred_failures,
                )
            raise cancellation from cause
        raise cancellation
    if shutdown_errors:
        raise ExceptionGroup("runtime coordinator shutdown failed", shutdown_errors)


async def _pause_runtime_for_shutdown(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
) -> None:
    """Serialize a durable shutdown pause against the final broker boundary."""

    def pause() -> None:
        with session_factory() as db:
            pause_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(
                    actor=settings.service_name,
                    source="lifespan_shutdown",
                    reason="service lifespan shutdown started",
                ),
            )

    await asyncio.to_thread(pause)


async def _shutdown_runtime_coordinators(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
    coordinators: tuple[tuple[str, object], ...],
) -> None:
    """Close broker admission, persist the pause, and drain every coordinator."""

    admission_error: BaseException | None = None
    try:
        _begin_runtime_coordinator_shutdown(coordinators)
    except BaseException as exc:
        admission_error = exc
        LOGGER.error(
            "runtime_shutdown_admission_fence_failed",
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    # In-memory SQLite uses one connection for every thread. Letting the
    # shutdown pause overlap an in-flight worker transaction can make either
    # session roll back the other's work. The database cannot survive process
    # exit anyway, so drain after closing admission before writing its pause.
    drain_before_pause = _shutdown_pause_requires_coordinator_drain(session_factory)
    stop_cancellation: asyncio.CancelledError | None = None
    stop_error: BaseException | None = None
    if drain_before_pause:
        stop_cancellation, stop_error = await _complete_shutdown_step(
            _stop_runtime_coordinators(coordinators)
        )

    pause_cancellation, pause_error = await _complete_shutdown_step(
        _pause_runtime_for_shutdown(
            settings=settings,
            session_factory=session_factory,
        )
    )
    if pause_error is not None:
        LOGGER.error(
            "runtime_shutdown_pause_failed",
            exc_info=(
                type(pause_error),
                pause_error,
                pause_error.__traceback__,
            ),
        )

    if not drain_before_pause:
        stop_cancellation, stop_error = await _complete_shutdown_step(
            _stop_runtime_coordinators(coordinators)
        )

    failures = [
        failure
        for failure in (admission_error, pause_error, stop_error)
        if failure is not None
    ]
    cancellation = pause_cancellation or stop_cancellation
    if not failures and cancellation is None:
        return
    non_cancellation_failures = [
        failure for failure in failures if isinstance(failure, Exception)
    ]
    if cancellation is not None:
        if non_cancellation_failures:
            raise cancellation from ExceptionGroup(
                "runtime shutdown failures",
                non_cancellation_failures,
            )
        raise cancellation
    raise ExceptionGroup("runtime shutdown failed", non_cancellation_failures)


def _shutdown_pause_requires_coordinator_drain(
    session_factory: sessionmaker[Session],
) -> bool:
    """Avoid overlapping transactions on a single shared SQLite connection."""

    factory_options = getattr(session_factory, "kw", None)
    bind = factory_options.get("bind") if isinstance(factory_options, dict) else None
    database = getattr(getattr(bind, "url", None), "database", None)
    return database == ":memory:"


def _begin_runtime_coordinator_shutdown(
    coordinators: tuple[tuple[str, object], ...],
) -> None:
    """Close every in-memory broker admission gate before durable shutdown I/O."""

    failures: list[Exception] = []
    for name, coordinator in coordinators:
        begin_shutdown = getattr(coordinator, "begin_shutdown", None)
        if not callable(begin_shutdown):
            continue
        try:
            begin_shutdown()
        except Exception as exc:
            failures.append(exc)
            LOGGER.exception(
                "runtime_coordinator_admission_fence_failed",
                extra={"runtime_coordinator": name},
            )
    if failures:
        raise ExceptionGroup("runtime coordinator admission fence failed", failures)


async def _complete_shutdown_step(
    awaitable: Awaitable[None],
) -> tuple[asyncio.CancelledError | None, BaseException | None]:
    """Finish one safety-critical shutdown step before propagating cancellation."""

    task = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
    try:
        task.result()
    except asyncio.CancelledError as exc:
        if cancellation is None:
            cancellation = exc
    except BaseException as exc:
        return cancellation, exc
    return cancellation, None


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
