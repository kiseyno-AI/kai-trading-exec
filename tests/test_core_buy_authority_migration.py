from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609110003_scoped_core_buy_authority.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_scoped_core_buy_authority_migration_upgrade_and_downgrade(
    monkeypatch,
) -> None:
    migration = _migration()
    engine = create_engine("sqlite+pysqlite:///:memory:")

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration, "op", operations)
        migration.upgrade()

        inspector = inspect(connection)
        assert {
            "core_buy_authority_states",
            "core_buy_authority_observations",
        }.issubset(inspector.get_table_names())
        assert {
            column["name"]
            for column in inspector.get_columns("core_buy_authority_states")
        } >= {
            "scope_fingerprint",
            "provider",
            "market",
            "currency",
            "runtime_mode",
            "account_scope",
            "fence_active",
            "projection_pending",
            "source_observed_at",
            "last_block_source_observed_at",
        }
        assert any(
            constraint["name"] == "uq_core_buy_authority_states_scope"
            for constraint in inspector.get_unique_constraints(
                "core_buy_authority_states"
            )
        )
        assert any(
            constraint["name"] == "ck_core_buy_authority_states_krx_scope"
            for constraint in inspector.get_check_constraints(
                "core_buy_authority_states"
            )
        )

        migration.downgrade()
        assert "core_buy_authority_states" not in inspect(connection).get_table_names()
        assert (
            "core_buy_authority_observations"
            not in inspect(connection).get_table_names()
        )

    engine.dispose()
