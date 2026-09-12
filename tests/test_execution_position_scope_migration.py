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
    / "202609090003_execution_position_route_scope.py"
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
        "execution_positions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("base_quantity", sa.Numeric(28, 12)),
        sa.Column("average_price", sa.Numeric(18, 4)),
        sa.Column("updated_from", sa.String(64), nullable=False),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True)),
        sa.Column("raw_payload", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "symbol",
            "mode",
            name="uq_execution_positions_symbol_mode",
        ),
    )
    metadata.create_all(engine)
    return table


def _legacy_row(*, row_id: str = "position-1") -> dict[str, object]:
    observed_at = datetime(2026, 9, 9, 14, 0, tzinfo=UTC)
    return {
        "id": row_id,
        "mode": "live",
        "symbol": "AAPL",
        "quantity": 1,
        "base_quantity": None,
        "average_price": Decimal("100"),
        "updated_from": "reconciliation",
        "last_reconciled_at": observed_at,
        "raw_payload": {},
        "created_at": observed_at,
        "updated_at": observed_at,
    }


def test_execution_position_scope_migration_round_trip_preserves_legacy_null_scope() -> None:
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
    for name in ("provider", "account_scope", "exchange"):
        assert columns[name]["nullable"] is True
    assert {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints(legacy.name)
    } == {("mode", "provider", "account_scope", "exchange", "symbol")}
    indexes = {index["name"]: index for index in inspector.get_indexes(legacy.name)}
    assert indexes["uq_execution_positions_legacy_symbol_mode"]["unique"] == 1
    scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        row = connection.execute(sa.select(scoped)).mappings().one()
    assert row["id"] == "position-1"
    assert row["provider"] is None
    assert row["account_scope"] is None
    assert row["exchange"] is None

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.downgrade()

    inspector = sa.inspect(engine)
    assert "provider" not in {
        column["name"] for column in inspector.get_columns(legacy.name)
    }
    assert {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints(legacy.name)
    } == {("symbol", "mode")}
    engine.dispose()


def test_execution_position_scope_downgrade_fails_on_cross_route_symbol_collision() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        first = _legacy_row(row_id="alpaca-a")
        first.update(provider="alpaca", account_scope="a" * 24, exchange="US")
        second = dict(first)
        second.update(id="alpaca-b", account_scope="b" * 24)
        connection.execute(scoped.insert(), [first, second])

    with pytest.raises(RuntimeError, match="colliding scoped execution positions"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_execution_position_scope_downgrade_refuses_attributed_rows() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    legacy = _legacy_table(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        scoped = sa.Table(legacy.name, sa.MetaData(), autoload_with=connection)
        attributed = _legacy_row(row_id="attributed-position")
        attributed.update(
            provider="alpaca",
            account_scope="a" * 24,
            exchange="US",
        )
        connection.execute(scoped.insert().values(**attributed))

    with pytest.raises(RuntimeError, match="attributed execution positions"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns(legacy.name)
    }
    engine.dispose()


def test_execution_position_scope_migration_locks_postgres_before_constraint_change() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind
    migration_op.batch_alter_table.return_value.__enter__.return_value = MagicMock()

    with patch.object(migration, "op", migration_op):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "LOCK TABLE execution_positions IN ACCESS EXCLUSIVE MODE"
    )


@pytest.mark.skipif(
    not os.getenv("KAI_EXEC_TEST_POSTGRES_URL"),
    reason="KAI_EXEC_TEST_POSTGRES_URL is required for isolated PostgreSQL DDL",
)
def test_execution_position_scope_migration_round_trip_isolated_postgres() -> None:
    migration = _load_migration()
    database_url = os.environ["KAI_EXEC_TEST_POSTGRES_URL"]
    database_name = sa.engine.make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("isolated PostgreSQL migration database must end with _test")
    schema_name = f"execution_position_scope_{uuid4().hex[:12]}"
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
        with engine.begin() as connection:
            attributed = _legacy_row(row_id="position-route-two")
            attributed.update(
                provider="alpaca",
                account_scope="b" * 24,
                exchange="US",
                symbol="MSFT",
            )
            connection.execute(
                scoped.insert().values(**attributed)
            )
            assert connection.scalar(sa.select(sa.func.count()).select_from(scoped)) == 2
            with pytest.raises(RuntimeError, match="attributed execution positions"):
                operations = Operations(MigrationContext.configure(connection))
                with patch.object(migration, "op", operations):
                    migration.downgrade()
            connection.execute(
                scoped.delete().where(scoped.c.id == "position-route-two")
            )
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
