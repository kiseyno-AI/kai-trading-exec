import importlib.util
from pathlib import Path
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609090001_kis_shared_token_generation.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kis_token_generation_migration_round_trip_sqlite() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    sa.Table(
        "broker_sessions",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("access_token", sa.Text()),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    columns = {column["name"]: column for column in sa.inspect(engine).get_columns("broker_sessions")}
    assert columns["generation"]["nullable"] is True
    assert columns["refresh_after"]["nullable"] is True

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.downgrade()

    assert {column["name"] for column in sa.inspect(engine).get_columns("broker_sessions")} == {
        "id",
        "access_token",
    }
    engine.dispose()


def test_kis_token_generation_migration_uses_portable_column_types() -> None:
    migration = _load_migration()
    migration_op = type("MigrationOp", (), {})()
    added: list[tuple[str, sa.Column]] = []
    removed: list[tuple[str, str]] = []
    migration_op.add_column = lambda table, column: added.append((table, column))
    migration_op.drop_column = lambda table, column: removed.append((table, column))

    with patch.object(migration, "op", migration_op):
        migration.upgrade()
        migration.downgrade()

    assert [(table, column.name, column.nullable) for table, column in added] == [
        ("broker_sessions", "generation", True),
        ("broker_sessions", "refresh_after", True),
    ]
    assert isinstance(added[0][1].type, sa.String)
    assert isinstance(added[1][1].type, sa.DateTime)
    assert removed == [
        ("broker_sessions", "refresh_after"),
        ("broker_sessions", "generation"),
    ]
