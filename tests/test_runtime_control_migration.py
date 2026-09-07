from datetime import UTC, datetime
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
    / "202609060004_runtime_control_transition_not_null.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_control_transition_migration_backfills_before_not_null() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    controls = sa.Table(
        "runtime_control_state",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_transition_action", sa.String(32), nullable=True),
        sa.Column("last_transition_source", sa.String(64), nullable=True),
        sa.Column("last_transition_at", sa.DateTime(timezone=True), nullable=True),
    )
    metadata.create_all(engine)
    recorded_at = datetime(2026, 9, 6, 18, 0, tzinfo=UTC)

    with engine.begin() as connection:
        connection.execute(
            controls.insert().values(
                id="global",
                created_at=recorded_at,
                updated_at=recorded_at,
                last_transition_action=None,
                last_transition_source=None,
                last_transition_at=None,
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    inspector = sa.inspect(engine)
    nullable_by_name = {
        column["name"]: column["nullable"]
        for column in inspector.get_columns("runtime_control_state")
    }
    assert nullable_by_name["last_transition_action"] is False
    assert nullable_by_name["last_transition_source"] is False
    assert nullable_by_name["last_transition_at"] is False

    reflected = sa.Table(
        "runtime_control_state",
        sa.MetaData(),
        autoload_with=engine,
    )
    with engine.connect() as connection:
        row = connection.execute(sa.select(reflected)).mappings().one()
    assert row["last_transition_action"] == "bootstrap"
    assert row["last_transition_source"] == "system"
    assert row["last_transition_at"] is not None
    engine.dispose()
