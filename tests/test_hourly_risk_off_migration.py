import importlib.util
from pathlib import Path
from unittest.mock import Mock, patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609080001_hourly_buy_risk_off_fence.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_hourly_risk_off_migration_backfills_existing_control_state() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    controls = sa.Table(
        "runtime_control_state",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(controls.insert().values(id="global"))
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    reflected = sa.Table(
        "runtime_control_state",
        sa.MetaData(),
        autoload_with=engine,
    )
    with engine.connect() as connection:
        row = connection.execute(sa.select(reflected)).mappings().one()

    assert row["hourly_risk_off_active"] is False
    assert row["hourly_risk_off_epoch"] == 0
    assert row["hourly_risk_off_context"] is None
    assert reflected.c.hourly_risk_off_active.nullable is False
    assert reflected.c.hourly_risk_off_epoch.nullable is False
    engine.dispose()


def test_hourly_risk_off_migration_refuses_downgrade_while_fence_is_active() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    controls = sa.Table(
        "runtime_control_state",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(controls.insert().values(id="global"))
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
            connection.execute(
                sa.text(
                    "UPDATE runtime_control_state "
                    "SET hourly_risk_off_active = 1 WHERE id = 'global'"
                )
            )
            try:
                migration.downgrade()
            except RuntimeError as exc:
                assert "while it is active" in str(exc)
            else:
                raise AssertionError("active hourly risk-off downgrade must fail closed")

    columns = {
        column["name"] for column in sa.inspect(engine).get_columns("runtime_control_state")
    }
    assert "hourly_risk_off_active" in columns
    assert "hourly_risk_off_epoch" in columns
    assert "hourly_risk_off_context" in columns
    engine.dispose()


def test_hourly_risk_off_downgrade_locks_postgres_transitions_before_check() -> None:
    migration = _load_migration()
    bind = Mock()
    bind.dialect.name = "postgresql"
    bind.execute.return_value.first.return_value = None
    operations = Mock()
    operations.get_bind.return_value = bind

    with patch.object(migration, "op", operations):
        migration.downgrade()

    statements = [str(call.args[0]) for call in bind.execute.call_args_list]
    assert statements[:2] == [
        "LOCK TABLE runtime_control_state IN ACCESS EXCLUSIVE MODE",
        (
            "SELECT 1 FROM runtime_control_state "
            "WHERE hourly_risk_off_active = :active LIMIT 1"
        ),
    ]
    assert operations.drop_column.call_count == 3


def test_hourly_risk_off_migration_downgrades_after_fence_is_inactive() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    sa.Table(
        "runtime_control_state",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
            migration.downgrade()

    columns = {
        column["name"] for column in sa.inspect(engine).get_columns("runtime_control_state")
    }
    assert "hourly_risk_off_active" not in columns
    assert "hourly_risk_off_epoch" not in columns
    assert "hourly_risk_off_context" not in columns
    engine.dispose()
