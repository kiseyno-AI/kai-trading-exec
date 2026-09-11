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
    / "202609110002_crypto_strategy_account_scope.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _legacy_tables(
    engine: sa.Engine,
    *,
    enforce_legacy_uniqueness: bool = True,
) -> dict[str, sa.Table]:
    metadata = sa.MetaData()
    states = sa.Table(
        "crypto_strategy_states",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
    )
    risks = sa.Table(
        "crypto_strategy_risk_states",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
    )
    decisions = sa.Table(
        "crypto_strategy_decisions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("decision_bar_at", sa.DateTime(timezone=True), nullable=False),
    )
    if enforce_legacy_uniqueness:
        states.append_constraint(
            sa.UniqueConstraint(
                "mode",
                "provider",
                "strategy_version",
                "symbol",
                name="uq_crypto_strategy_states_mode_provider_version_symbol",
            )
        )
        risks.append_constraint(
            sa.UniqueConstraint(
                "mode",
                "provider",
                "strategy_version",
                name="uq_crypto_strategy_risk_states_mode_provider_version",
            )
        )
        decisions.append_constraint(
            sa.UniqueConstraint(
                "mode",
                "provider",
                "strategy_version",
                "symbol",
                "decision_bar_at",
                name="uq_crypto_strategy_decisions_scope_bar",
            )
        )
    metadata.create_all(engine)
    return {table.name: table for table in (states, risks, decisions)}


def _legacy_values(table_name: str, row_id: str) -> dict[str, object]:
    values: dict[str, object] = {
        "id": row_id,
        "mode": "paper",
        "provider": "alpaca",
        "strategy_version": "shared-v1",
    }
    if table_name != "crypto_strategy_risk_states":
        values["symbol"] = "AAPL"
    if table_name == "crypto_strategy_decisions":
        values["decision_bar_at"] = datetime(2026, 9, 11, tzinfo=UTC)
    return values


def test_crypto_strategy_scope_migration_quarantines_legacy_and_separates_scopes() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    tables = _legacy_tables(engine)
    with engine.begin() as connection:
        for table in tables.values():
            connection.execute(
                table.insert().values(**_legacy_values(table.name, f"legacy-{table.name}"))
            )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    inspector = sa.inspect(engine)
    expected_unique = {
        "crypto_strategy_states": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
        ),
        "crypto_strategy_risk_states": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
        ),
        "crypto_strategy_decisions": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
            "decision_bar_at",
        ),
    }
    expected_legacy_indexes = {
        "crypto_strategy_states": (
            "uq_crypto_strategy_states_legacy_mode_provider_version_symbol",
            ("mode", "provider", "strategy_version", "symbol"),
        ),
        "crypto_strategy_risk_states": (
            "uq_crypto_strategy_risk_states_legacy_mode_provider_version",
            ("mode", "provider", "strategy_version"),
        ),
        "crypto_strategy_decisions": (
            "uq_crypto_strategy_decisions_legacy_scope_bar",
            ("mode", "provider", "strategy_version", "symbol", "decision_bar_at"),
        ),
    }
    for table_name, unique_columns in expected_unique.items():
        columns = {
            column["name"]: column
            for column in inspector.get_columns(table_name)
        }
        assert columns["account_scope"]["nullable"] is True
        assert columns["strategy_family"]["nullable"] is True
        assert {
            tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(table_name)
        } == {unique_columns}
        assert len(inspector.get_check_constraints(table_name)) == 1
        legacy_index_name, legacy_columns = expected_legacy_indexes[table_name]
        indexes = {
            str(index["name"]): index for index in inspector.get_indexes(table_name)
        }
        assert indexes[legacy_index_name]["unique"] in (True, 1)
        assert tuple(indexes[legacy_index_name]["column_names"]) == legacy_columns
        scoped = sa.Table(table_name, sa.MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            legacy = connection.execute(
                sa.select(scoped).where(scoped.c.id == f"legacy-{table_name}")
            ).mappings().one()
        assert legacy["account_scope"] is None
        assert legacy["strategy_family"] is None

        with engine.begin() as connection:
            for suffix, account_scope, family in (
                ("a-crypto", "a" * 24, "crypto:ALPACA"),
                ("b-crypto", "b" * 24, "crypto:ALPACA"),
                ("a-equity", "a" * 24, "us_equity:US"),
            ):
                values = _legacy_values(table_name, f"{table_name}-{suffix}")
                values.update(
                    account_scope=account_scope,
                    strategy_family=family,
                )
                connection.execute(scoped.insert().values(**values))

        duplicate = _legacy_values(table_name, f"{table_name}-duplicate")
        duplicate.update(
            account_scope="a" * 24,
            strategy_family="crypto:ALPACA",
        )
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(scoped.insert().values(**duplicate))

        partial = _legacy_values(table_name, f"{table_name}-partial")
        partial.update(account_scope="c" * 24, strategy_family=None)
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(scoped.insert().values(**partial))

        legacy_duplicate = _legacy_values(table_name, f"{table_name}-legacy-duplicate")
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as connection:
                connection.execute(scoped.insert().values(**legacy_duplicate))

    with pytest.raises(RuntimeError, match="colliding scoped crypto strategy rows"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
    engine.dispose()


@pytest.mark.parametrize(
    "duplicate_table",
    [
        "crypto_strategy_states",
        "crypto_strategy_risk_states",
        "crypto_strategy_decisions",
    ],
)
def test_crypto_strategy_scope_upgrade_refuses_duplicate_legacy_rows_before_ddl(
    duplicate_table: str,
) -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    tables = _legacy_tables(engine, enforce_legacy_uniqueness=False)
    with engine.begin() as connection:
        table = tables[duplicate_table]
        connection.execute(
            table.insert(),
            [
                _legacy_values(duplicate_table, "duplicate-a"),
                _legacy_values(duplicate_table, "duplicate-b"),
            ],
        )

    with pytest.raises(
        RuntimeError,
        match=(
            "cannot upgrade: duplicate legacy crypto strategy rows exist in "
            f"{duplicate_table}"
        ),
    ):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.upgrade()

    inspector = sa.inspect(engine)
    for table_name in migration.TABLES:
        assert "account_scope" not in {
            column["name"] for column in inspector.get_columns(table_name)
        }
        assert "strategy_family" not in {
            column["name"] for column in inspector.get_columns(table_name)
        }
    engine.dispose()


def test_crypto_strategy_scope_migration_empty_round_trip() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    _legacy_tables(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
            migration.downgrade()
    for table_name in migration.TABLES:
        assert "account_scope" not in {
            column["name"]
            for column in sa.inspect(engine).get_columns(table_name)
        }
    engine.dispose()


def test_crypto_strategy_scope_downgrade_refuses_attributed_rows() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    _legacy_tables(engine)
    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
        states = sa.Table(
            "crypto_strategy_states",
            sa.MetaData(),
            autoload_with=connection,
        )
        attributed = _legacy_values(
            "crypto_strategy_states",
            "attributed-state",
        )
        attributed.update(
            account_scope="a" * 24,
            strategy_family="crypto:ALPACA",
        )
        connection.execute(states.insert().values(**attributed))

    with pytest.raises(RuntimeError, match="attributed crypto strategy rows"):
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()

    assert "account_scope" in {
        column["name"]
        for column in sa.inspect(engine).get_columns("crypto_strategy_states")
    }
    engine.dispose()


def test_crypto_strategy_scope_migration_locks_postgres_tables_first() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    bind.execute.return_value.first.return_value = None
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind
    migration_op.batch_alter_table.return_value.__enter__.return_value = MagicMock()

    with patch.object(migration, "op", migration_op):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "LOCK TABLE crypto_strategy_states, crypto_strategy_risk_states, "
        "crypto_strategy_decisions IN ACCESS EXCLUSIVE MODE"
    )


@pytest.mark.skipif(
    not os.getenv("KAI_EXEC_TEST_POSTGRES_URL"),
    reason="KAI_EXEC_TEST_POSTGRES_URL is required for isolated PostgreSQL DDL",
)
@pytest.mark.parametrize(
    "duplicate_table",
    [
        "crypto_strategy_states",
        "crypto_strategy_risk_states",
        "crypto_strategy_decisions",
    ],
)
def test_crypto_strategy_scope_upgrade_refuses_duplicate_legacy_rows_postgres(
    duplicate_table: str,
) -> None:
    migration = _load_migration()
    database_url = os.environ["KAI_EXEC_TEST_POSTGRES_URL"]
    database_name = sa.engine.make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("isolated PostgreSQL migration database must end with _test")
    schema_name = f"crypto_strategy_duplicates_{uuid4().hex[:12]}"
    admin_engine = sa.create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(sa.schema.CreateSchema(schema_name))
    engine = sa.create_engine(
        database_url,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    try:
        tables = _legacy_tables(engine, enforce_legacy_uniqueness=False)
        with engine.begin() as connection:
            connection.execute(
                tables[duplicate_table].insert(),
                [
                    _legacy_values(duplicate_table, "duplicate-a"),
                    _legacy_values(duplicate_table, "duplicate-b"),
                ],
            )

        with pytest.raises(
            RuntimeError,
            match=(
                "cannot upgrade: duplicate legacy crypto strategy rows exist in "
                f"{duplicate_table}"
            ),
        ):
            with engine.begin() as connection:
                operations = Operations(MigrationContext.configure(connection))
                with patch.object(migration, "op", operations):
                    migration.upgrade()

        inspector = sa.inspect(engine)
        for table_name in migration.TABLES:
            assert "account_scope" not in {
                column["name"] for column in inspector.get_columns(table_name)
            }
            assert "strategy_family" not in {
                column["name"] for column in inspector.get_columns(table_name)
            }
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(sa.schema.DropSchema(schema_name, cascade=True))
        admin_engine.dispose()


@pytest.mark.skipif(
    not os.getenv("KAI_EXEC_TEST_POSTGRES_URL"),
    reason="KAI_EXEC_TEST_POSTGRES_URL is required for isolated PostgreSQL DDL",
)
def test_crypto_strategy_scope_migration_round_trip_isolated_postgres() -> None:
    migration = _load_migration()
    database_url = os.environ["KAI_EXEC_TEST_POSTGRES_URL"]
    database_name = sa.engine.make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("isolated PostgreSQL migration database must end with _test")
    schema_name = f"crypto_strategy_scope_{uuid4().hex[:12]}"
    admin_engine = sa.create_engine(database_url)
    with admin_engine.begin() as connection:
        connection.execute(sa.schema.CreateSchema(schema_name))
    engine = sa.create_engine(
        database_url,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    try:
        _legacy_tables(engine)
        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.upgrade()
            scoped_tables = {
                table_name: sa.Table(
                    table_name,
                    sa.MetaData(),
                    autoload_with=connection,
                )
                for table_name in migration.TABLES
            }
            for index, (table_name, table) in enumerate(scoped_tables.items()):
                connection.execute(
                    table.insert().values(
                        **_legacy_values(
                            table_name,
                            f"postgres-legacy-{index}",
                        )
                    )
                )
                with pytest.raises(sa.exc.IntegrityError):
                    with connection.begin_nested():
                        connection.execute(
                            table.insert().values(
                                **_legacy_values(
                                    table_name,
                                    f"postgres-legacy-duplicate-{index}",
                                )
                            )
                        )
            states = sa.Table(
                "crypto_strategy_states",
                sa.MetaData(),
                autoload_with=connection,
            )
            attributed = _legacy_values(
                "crypto_strategy_states",
                "postgres-attributed-state",
            )
            attributed.update(
                account_scope="a" * 24,
                strategy_family="crypto:ALPACA",
                strategy_version="attributed-v2",
            )
            connection.execute(states.insert().values(**attributed))
            with pytest.raises(
                RuntimeError,
                match="attributed crypto strategy rows",
            ):
                operations = Operations(MigrationContext.configure(connection))
                with patch.object(migration, "op", operations):
                    migration.downgrade()
            connection.execute(states.delete())
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, "op", operations):
                migration.downgrade()
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(sa.schema.DropSchema(schema_name, cascade=True))
        admin_engine.dispose()
