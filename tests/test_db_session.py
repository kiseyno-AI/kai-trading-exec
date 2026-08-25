from sqlalchemy.engine import make_url

from app.core.config import Settings, load_settings
from app.db.session import _build_engine_kwargs, create_db_engine, pool_status
from app.services import health as health_service


def test_postgres_pool_settings_are_configurable_without_connecting() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://user:pass@localhost/kai_exec",
        db_pool_size=7,
        db_max_overflow=3,
        db_pool_timeout_seconds=4,
        db_pool_recycle_seconds=600,
        db_pool_use_lifo=False,
        db_connect_timeout_seconds=6,
    )

    kwargs = _build_engine_kwargs(settings)

    assert make_url(settings.database_url).drivername == "postgresql+psycopg"
    assert kwargs["pool_size"] == 7
    assert kwargs["max_overflow"] == 3
    assert kwargs["pool_timeout"] == 4
    assert kwargs["pool_recycle"] == 600
    assert kwargs["pool_use_lifo"] is False
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["connect_args"] == {"connect_timeout": 6}


def test_sqlite_busy_timeout_and_pool_capacity_are_configurable(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'exec.db'}",
        db_pool_size=3,
        db_max_overflow=2,
        db_pool_timeout_seconds=2,
        sqlite_busy_timeout_seconds=7,
        sqlite_wal_autocheckpoint_pages=1000,
    )
    engine = create_db_engine(settings)
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 7000
            assert connection.exec_driver_sql("PRAGMA wal_autocheckpoint").scalar_one() == 1000
        status = pool_status(engine)
        assert status["pool_size"] == 3
        assert status["checked_out"] == 0
    finally:
        engine.dispose()


def test_sqlite_new_pool_connection_does_not_reapply_wal_under_writer_lock(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'writer-lock.db'}",
        db_pool_size=2,
        db_max_overflow=0,
        sqlite_busy_timeout_seconds=1,
    )
    engine = create_db_engine(settings)
    try:
        with engine.connect() as setup_connection:
            setup_connection.exec_driver_sql("CREATE TABLE lock_probe (id INTEGER PRIMARY KEY)")
            setup_connection.commit()

        with engine.connect() as writer_connection:
            writer_connection.exec_driver_sql("BEGIN IMMEDIATE")
            writer_connection.exec_driver_sql("INSERT INTO lock_probe (id) VALUES (1)")

            # This forces creation of the pool's second DB-API connection while
            # the first connection owns SQLite's write lock. Connection setup
            # must remain read-only and must not execute journal_mode=WAL.
            with engine.connect() as reader_connection:
                assert reader_connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
                assert reader_connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 1000

            writer_connection.rollback()
    finally:
        engine.dispose()


def test_database_pool_environment_fields_are_loaded() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_DB_POOL_SIZE": "8",
            "KAI_EXEC_DB_MAX_OVERFLOW": "4",
            "KAI_EXEC_DB_POOL_TIMEOUT_SECONDS": "3.5",
            "KAI_EXEC_DB_POOL_RECYCLE_SECONDS": "300",
            "KAI_EXEC_DB_POOL_USE_LIFO": "false",
            "KAI_EXEC_DB_CONNECT_TIMEOUT_SECONDS": "9",
            "KAI_EXEC_SQLITE_BUSY_TIMEOUT_SECONDS": "12",
            "KAI_EXEC_SQLITE_WAL_AUTOCHECKPOINT_PAGES": "2000",
        }
    )

    assert settings.db_pool_size == 8
    assert settings.db_max_overflow == 4
    assert settings.db_pool_timeout_seconds == 3.5
    assert settings.db_pool_recycle_seconds == 300
    assert settings.db_pool_use_lifo is False
    assert settings.db_connect_timeout_seconds == 9
    assert settings.sqlite_busy_timeout_seconds == 12
    assert settings.sqlite_wal_autocheckpoint_pages == 2000
def test_database_health_checks_do_not_stack_connection_attempts() -> None:
    lock = health_service._database_healthcheck_lock
    assert lock.acquire(blocking=False) is True
    try:
        result = health_service.check_database(object())  # type: ignore[arg-type]
    finally:
        lock.release()

    assert result["status"] == "error"
    assert result["error"] == "HealthCheckInProgress"


def test_high_volatility_chase_veto_is_enabled_by_default() -> None:
    assert Settings().watch_buy_high_volatility_chase_veto_enabled is True
