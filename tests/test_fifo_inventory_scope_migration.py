from __future__ import annotations

import importlib.util
import os
from datetime import UTC, datetime
from decimal import Decimal
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
    / "202609090002_fifo_inventory_route_scope.py"
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
        "fifo_inventory_isolations",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("residual_quantity", sa.Numeric(28, 12), nullable=False),
        sa.Column("last_local_fill_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("broker_position_id", sa.String(36), nullable=False),
        sa.Column("broker_position_quantity", sa.Numeric(28, 12), nullable=False),
        sa.Column("broker_reconciled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("isolated_through_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_source", sa.String(64), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("audit_payload", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "mode",
            "symbol",
            "broker_reconciled_at",
            name="uq_fifo_inventory_isolations_mode_symbol_reconciled",
        ),
    )
    sa.Index(
        "ix_fifo_inventory_isolations_mode_symbol_isolated_at",
        table.c.mode,
        table.c.symbol,
        table.c.isolated_through_at,
    )
    metadata.create_all(engine)
    return table


def _legacy_row(*, row_id: str = "isolation-1") -> dict[str, object]:
    observed_at = datetime(2026, 9, 9, 14, 0, tzinfo=UTC)
    return {
        "id": row_id,
        "mode": "live",
        "symbol": "AAPL",
        "residual_quantity": Decimal("1"),
        "last_local_fill_at": observed_at,
        "broker_position_id": f"position-{row_id}",
        "broker_position_quantity": Decimal("0"),
        "broker_reconciled_at": observed_at,
        "isolated_through_at": observed_at,
        "evidence_source": "broker_flat_reconciliation",
        "actor": "operator",
        "reason": "verified flat",
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def test_fifo_inventory_scope_migration_round_trip_sqlite_preserves_legacy_row() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        connection.execute(legacy.insert().values(**_legacy_row()))
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    inspector = sa.inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns(legacy.name)}
    for name in ("provider", "account_scope", "venue", "asset_class", "exchange"):
        assert columns[name]["nullable"] is True
    assert {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints(legacy.name)
    } == {
        (
            "mode",
            "provider",
            "account_scope",
            "venue",
            "asset_class",
            "exchange",
            "symbol",
            "broker_reconciled_at",
        )
    }
    assert {
        constraint["name"] for constraint in inspector.get_check_constraints(legacy.name)
    } == {"ck_fifo_inventory_isolations_scope_complete"}
    indexes = {index["name"]: index for index in inspector.get_indexes(legacy.name)}
    assert indexes["uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled"][
        "unique"
    ] == 1
    scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        row = connection.execute(sa.select(scoped)).mappings().one()
    assert row["id"] == "isolation-1"
    assert row["provider"] is None
    assert row["account_scope"] is None

    duplicate = _legacy_row(row_id="legacy-duplicate")
    with pytest.raises(sa.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(scoped.insert().values(**duplicate))

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.downgrade()

    inspector = sa.inspect(engine)
    assert "account_scope" not in {
        column["name"] for column in inspector.get_columns(legacy.name)
    }
    assert {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints(legacy.name)
    } == {("mode", "symbol", "broker_reconciled_at")}
    assert "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled" not in {
        index["name"] for index in inspector.get_indexes(legacy.name)
    }
    engine.dispose()


def test_fifo_inventory_scope_downgrade_fails_closed_on_colliding_routes() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        base = _legacy_row(row_id="scope-a")
        base.update(
            provider="kis",
            account_scope="account-a",
            venue="KIS-US",
            asset_class="us_equity",
            exchange="NASD",
        )
        second = dict(base)
        second.update(id="scope-b", broker_position_id="position-scope-b")
        second["exchange"] = "NYSE"
        connection.execute(scoped.insert(), [base, second])
        assert connection.scalar(sa.select(sa.func.count()).select_from(scoped)) == 2

    with pytest.raises(RuntimeError, match="colliding scoped FIFO isolation rows"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_fifo_inventory_scope_downgrade_refuses_attributed_rows() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        attributed = _legacy_row(row_id="attributed-isolation")
        attributed.update(
            provider="kis",
            account_scope="a" * 24,
            venue="kis",
            asset_class="stock",
            exchange="KRX",
        )
        connection.execute(scoped.insert().values(**attributed))

    with pytest.raises(RuntimeError, match="attributed FIFO isolation rows"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_fifo_inventory_scope_migration_locks_postgres_before_constraint_changes() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind
    migration_op.batch_alter_table.return_value.__enter__.return_value = MagicMock()

    with patch.object(migration, "op", migration_op):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "LOCK TABLE fifo_inventory_isolations IN ACCESS EXCLUSIVE MODE"
    )


@pytest.mark.skipif(
    not os.getenv("KAI_EXEC_TEST_POSTGRES_URL"),
    reason="KAI_EXEC_TEST_POSTGRES_URL is required for isolated PostgreSQL DDL",
)
def test_fifo_inventory_scope_migration_round_trip_isolated_postgres() -> None:
    migration = _load_migration()
    database_url = os.environ["KAI_EXEC_TEST_POSTGRES_URL"]
    database_name = sa.engine.make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("isolated PostgreSQL migration database must end with _test")
    schema_name = f"fifo_inventory_scope_{uuid4().hex[:12]}"
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
            connection.execute(legacy.insert().values(**_legacy_row()))
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=engine)
        duplicate = _legacy_row(row_id="legacy-duplicate")
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(scoped.insert().values(**duplicate))
        with engine.begin() as connection:
            route_two = _legacy_row(row_id="scope-two")
            route_two.update(
                provider="alpaca",
                account_scope="b" * 24,
                venue="ALPACA",
                asset_class="us_equity",
                exchange="US",
                symbol="MSFT",
            )
            connection.execute(scoped.insert().values(**route_two))
            assert connection.scalar(sa.select(sa.func.count()).select_from(scoped)) == 2
            with pytest.raises(RuntimeError, match="attributed FIFO isolation rows"):
                operations = Operations(MigrationContext.configure(connection))
                with patch.object(migration, "op", operations):
                    migration.downgrade()
            connection.execute(scoped.delete().where(scoped.c.id == "scope-two"))
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
        assert "provider" not in {
            column["name"] for column in sa.inspect(engine).get_columns(legacy.name)
        }
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(sa.schema.DropSchema(schema_name, cascade=True))
        admin_engine.dispose()
