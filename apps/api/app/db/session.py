import logging
from pathlib import Path
import time

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings
from app.db.sqlite_diagnostics import record_sqlite_lock_error

LOGGER = logging.getLogger(__name__)


def ensure_database_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite"):
        return
    if not url.database or url.database == ":memory:":
        return
    Path(url.database).expanduser().parent.mkdir(parents=True, exist_ok=True)


def create_db_engine(settings: Settings) -> Engine:
    ensure_database_parent_dir(settings.database_url)
    url = make_url(settings.database_url)
    kwargs = _build_engine_kwargs(settings)
    engine = create_engine(settings.database_url, **kwargs)
    _configure_pool_diagnostics(engine, settings=settings)
    if url.drivername.startswith("sqlite"):
        _configure_sqlite_engine(engine, settings=settings)
        _initialize_sqlite_journal_mode(engine, database=url.database)
    return engine


def _build_engine_kwargs(settings: Settings) -> dict[str, object]:
    url = make_url(settings.database_url)
    kwargs: dict[str, object] = {
        "future": True,
        "pool_pre_ping": True,
    }
    if url.drivername.startswith("sqlite"):
        kwargs["connect_args"] = {
            "check_same_thread": False,
            "timeout": float(settings.sqlite_busy_timeout_seconds),
        }
        if url.database == ":memory:":
            kwargs["poolclass"] = StaticPool
        else:
            kwargs.update(_queue_pool_kwargs(settings))
    else:
        kwargs.update(_queue_pool_kwargs(settings))
        if url.drivername.startswith("postgresql"):
            kwargs["connect_args"] = {
                "connect_timeout": settings.db_connect_timeout_seconds,
            }
    return kwargs


def _queue_pool_kwargs(settings: Settings) -> dict[str, object]:
    return {
        "pool_timeout": settings.db_pool_timeout_seconds,
        "pool_size": settings.db_pool_size,
        "max_overflow": settings.db_max_overflow,
        "pool_recycle": settings.db_pool_recycle_seconds,
        "pool_use_lifo": settings.db_pool_use_lifo,
    }


def pool_status(engine: Engine) -> dict[str, object]:
    pool = engine.pool
    status: dict[str, object] = {"description": pool.status()}
    for key, method_name in (
        ("pool_size", "size"),
        ("checked_in", "checkedin"),
        ("checked_out", "checkedout"),
        ("overflow", "overflow"),
    ):
        method = getattr(pool, method_name, None)
        if callable(method):
            status[key] = method()
    return status


def _configure_pool_diagnostics(engine: Engine, *, settings: Settings) -> None:
    @event.listens_for(engine, "checkout")
    def _record_checkout(_dbapi_connection, connection_record, _connection_proxy) -> None:  # type: ignore[no-untyped-def]
        connection_record.info["kai_checked_out_at"] = time.monotonic()

    @event.listens_for(engine, "checkin")
    def _log_long_checkout(_dbapi_connection, connection_record) -> None:  # type: ignore[no-untyped-def]
        checked_out_at = connection_record.info.pop("kai_checked_out_at", None)
        if checked_out_at is None:
            return
        elapsed = time.monotonic() - checked_out_at
        if elapsed >= settings.db_long_checkout_warning_seconds:
            LOGGER.warning(
                "database_connection_long_checkout",
                extra={
                    "elapsed_seconds": round(elapsed, 3),
                    "pool": pool_status(engine),
                },
            )

    @event.listens_for(engine, "handle_error")
    def _log_disconnect(exception_context) -> None:  # type: ignore[no-untyped-def]
        if exception_context.is_disconnect:
            LOGGER.warning(
                "database_disconnect_detected",
                extra={
                    "error": str(exception_context.original_exception),
                    "pool": pool_status(engine),
                },
            )


def _configure_sqlite_engine(engine: Engine, *, settings: Settings) -> None:
    busy_timeout_ms = settings.sqlite_busy_timeout_seconds * 1000
    wal_autocheckpoint_pages = settings.sqlite_wal_autocheckpoint_pages

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
            cursor.execute(f"PRAGMA wal_autocheckpoint={wal_autocheckpoint_pages}")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()

    @event.listens_for(engine, "handle_error")
    def _record_sqlite_lock_errors(exception_context) -> None:  # type: ignore[no-untyped-def]
        original = exception_context.original_exception
        message = str(original)
        if "database is locked" in message.lower():
            record_sqlite_lock_error(message)


def _initialize_sqlite_journal_mode(engine: Engine, *, database: str | None) -> None:
    """Enable WAL once during engine startup, never on pooled connection churn.

    ``PRAGMA journal_mode=WAL`` needs a database write lock even when WAL is
    already the desired mode. Running it from the ``connect`` event therefore
    made an unrelated new pooled connection fail whenever another worker held
    a write transaction. WAL is persistent for file-backed SQLite databases,
    so it belongs in startup initialization rather than the per-connection
    pragma hook.
    """
    if not database or database == ":memory:":
        return
    with engine.connect() as connection:
        current_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar_one()
        if str(current_mode).lower() != "wal":
            configured_mode = connection.exec_driver_sql("PRAGMA journal_mode=WAL").scalar_one()
            if str(configured_mode).lower() != "wal":
                raise RuntimeError(
                    f"failed to enable SQLite WAL journal mode: {configured_mode}"
                )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )


def ping_database(db: Session) -> None:
    db.execute(text("SELECT 1"))
