from __future__ import annotations

import importlib.util
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609110001_monitoring_checkpoint_account_scope.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _legacy_table(engine: sa.Engine) -> sa.Table:
    metadata = sa.MetaData()
    table = sa.Table(
        "monitoring_checkpoints",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("worker_name", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("details", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "worker_name",
            "mode",
            name="uq_monitoring_checkpoints_worker_name_mode",
        ),
    )
    metadata.create_all(engine)
    return table


def _legacy_row(
    *,
    row_id: str,
    worker_name: str,
    details: dict[str, object] | None,
) -> dict[str, object]:
    observed_at = datetime(2026, 9, 11, 12, tzinfo=UTC)
    return {
        "id": row_id,
        "worker_name": worker_name,
        "mode": "live",
        "status": "running",
        "last_heartbeat_at": observed_at,
        "last_success_at": observed_at,
        "details": details,
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def test_monitoring_checkpoint_scope_migration_backfills_only_exact_opaque_scope() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        connection.execute(
            legacy.insert(),
            [
                _legacy_row(
                    row_id="exact",
                    worker_name="exact-monitor",
                    details={
                        "execution_scope": {
                            "policy_version": 1,
                            "provider": "kis",
                            "account_scope": "a" * 24,
                        }
                    },
                ),
                _legacy_row(
                    row_id="legacy",
                    worker_name="legacy-monitor",
                    details={"execution_scope_status": "unavailable"},
                ),
                _legacy_row(
                    row_id="unsafe",
                    worker_name="unsafe-monitor",
                    details={
                        "execution_scope": {
                            "policy_version": 1,
                            "provider": "kis",
                            "account_scope": "12345678-01",
                        }
                    },
                ),
            ],
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    inspector = sa.inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns(legacy.name)}
    assert columns["provider"]["nullable"] is True
    assert columns["account_scope"]["nullable"] is True
    assert {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints(legacy.name)
    } == {("worker_name", "mode", "provider", "account_scope")}
    assert {
        constraint["name"] for constraint in inspector.get_check_constraints(legacy.name)
    } == {"ck_monitoring_checkpoints_scope_complete"}
    indexes = {index["name"]: index for index in inspector.get_indexes(legacy.name)}
    assert indexes["uq_monitoring_checkpoints_legacy_worker_mode"]["unique"] == 1

    scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        rows = {
            row["id"]: row
            for row in connection.execute(sa.select(scoped)).mappings().all()
        }
    assert rows["exact"]["provider"] == "kis"
    assert rows["exact"]["account_scope"] == "a" * 24
    assert rows["legacy"]["provider"] is None
    assert rows["legacy"]["account_scope"] is None
    assert rows["unsafe"]["provider"] is None
    assert rows["unsafe"]["account_scope"] is None

    with pytest.raises(RuntimeError, match="attributed monitoring checkpoints"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
    with engine.begin() as connection:
        connection.execute(scoped.delete().where(scoped.c.id == "exact"))
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.downgrade()
    assert "provider" not in {
        column["name"] for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_monitoring_checkpoint_scope_migration_separates_accounts_and_legacy_rows() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        first = _legacy_row(row_id="scope-a", worker_name="monitor", details={})
        first.update(provider="kis", account_scope="a" * 24)
        second = _legacy_row(row_id="scope-b", worker_name="monitor", details={})
        second.update(provider="kis", account_scope="b" * 24)
        legacy_null = _legacy_row(row_id="legacy-null", worker_name="monitor", details={})
        connection.execute(scoped.insert().values(**first))
        connection.execute(scoped.insert().values(**second))
        connection.execute(scoped.insert().values(**legacy_null))

    duplicate_legacy = _legacy_row(
        row_id="legacy-null-duplicate",
        worker_name="monitor",
        details={},
    )
    with pytest.raises(sa.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(scoped.insert().values(**duplicate_legacy))

    with pytest.raises(RuntimeError, match="colliding scoped monitoring checkpoints"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
    engine.dispose()


def test_monitoring_checkpoint_scope_downgrade_refuses_attributed_rows() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        attributed = _legacy_row(
            row_id="attributed-checkpoint",
            worker_name="monitor",
            details={},
        )
        attributed.update(provider="kis", account_scope="a" * 24)
        connection.execute(scoped.insert().values(**attributed))

    with pytest.raises(RuntimeError, match="attributed monitoring checkpoints"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_monitoring_checkpoint_scope_migration_locks_postgres_before_changes() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind
    migration_op.batch_alter_table.return_value.__enter__.return_value = MagicMock()

    with (
        patch.object(migration, "op", migration_op),
        patch.object(migration, "_backfill_exact_execution_scopes"),
    ):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "LOCK TABLE monitoring_checkpoints IN ACCESS EXCLUSIVE MODE"
    )


@pytest.mark.skipif(
    not os.getenv("KAI_EXEC_TEST_POSTGRES_URL"),
    reason="KAI_EXEC_TEST_POSTGRES_URL is required for isolated PostgreSQL DDL",
)
def test_monitoring_checkpoint_scope_migration_round_trip_isolated_postgres() -> None:
    migration = _load_migration()
    database_url = os.environ["KAI_EXEC_TEST_POSTGRES_URL"]
    database_name = sa.engine.make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("isolated PostgreSQL migration database must end with _test")
    schema_name = f"monitoring_checkpoint_scope_{uuid4().hex[:12]}"
    admin_engine = sa.create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(sa.schema.CreateSchema(schema_name))
    engine = sa.create_engine(
        database_url,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    try:
        legacy = _legacy_table(engine)
        with engine.begin() as connection:
            connection.execute(
                legacy.insert().values(
                    **_legacy_row(
                        row_id="postgres-exact",
                        worker_name="monitor",
                        details={
                            "execution_scope": {
                                "policy_version": 1,
                                "provider": "kis",
                                "account_scope": "a" * 24,
                            }
                        },
                    )
                )
            )
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.upgrade()
            scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
            row = connection.execute(sa.select(scoped)).mappings().one()
            assert row["provider"] == "kis"
            assert row["account_scope"] == "a" * 24
            with pytest.raises(
                RuntimeError,
                match="attributed monitoring checkpoints",
            ):
                operations = Operations(MigrationContext.configure(connection))
                with patch.object(migration, "op", operations):
                    migration.downgrade()
            connection.execute(scoped.delete())
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(sa.schema.DropSchema(schema_name, cascade=True))
        admin_engine.dispose()
