"""Isolate Alpaca strategy state by account and strategy family.

Revision ID: 202609110002
Revises: 202609110001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609110002"
down_revision: str | None = "202609110001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = {
    "crypto_strategy_states": {
        "legacy_constraint": "uq_crypto_strategy_states_mode_provider_version_symbol",
        "scoped_constraint": "uq_crypto_strategy_states_scoped_symbol",
        "scope_check": "ck_crypto_strategy_states_scope_complete",
        "legacy_index": (
            "uq_crypto_strategy_states_legacy_mode_provider_version_symbol"
        ),
        "legacy_columns": ("mode", "provider", "strategy_version", "symbol"),
        "scoped_columns": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
        ),
    },
    "crypto_strategy_risk_states": {
        "legacy_constraint": "uq_crypto_strategy_risk_states_mode_provider_version",
        "scoped_constraint": "uq_crypto_strategy_risk_states_scope",
        "scope_check": "ck_crypto_strategy_risk_states_scope_complete",
        "legacy_index": (
            "uq_crypto_strategy_risk_states_legacy_mode_provider_version"
        ),
        "legacy_columns": ("mode", "provider", "strategy_version"),
        "scoped_columns": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
        ),
    },
    "crypto_strategy_decisions": {
        "legacy_constraint": "uq_crypto_strategy_decisions_scope_bar",
        "scoped_constraint": "uq_crypto_strategy_decisions_account_family_bar",
        "scope_check": "ck_crypto_strategy_decisions_scope_complete",
        "legacy_index": "uq_crypto_strategy_decisions_legacy_scope_bar",
        "legacy_columns": (
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            "decision_bar_at",
        ),
        "scoped_columns": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
            "decision_bar_at",
        ),
    },
}
LEGACY_SCOPE_PREDICATE = (
    "account_scope IS NULL AND strategy_family IS NULL"
)


def upgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_tables(bind)
    _assert_no_legacy_duplicates(bind)
    for table_name in TABLES:
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.add_column(
                sa.Column("account_scope", sa.String(length=128), nullable=True)
            )
            batch_op.add_column(
                sa.Column("strategy_family", sa.String(length=64), nullable=True)
            )

    # Existing rows deliberately remain NULL/NULL. There is no safe way to
    # infer which authenticated Alpaca account or which shared strategy family
    # produced them, so runtime code quarantines them permanently.
    for table_name, spec in TABLES.items():
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.drop_constraint(
                str(spec["legacy_constraint"]),
                type_="unique",
            )
            batch_op.create_check_constraint(
                str(spec["scope_check"]),
                "(account_scope IS NULL AND strategy_family IS NULL) OR "
                "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)",
            )
            batch_op.create_unique_constraint(
                str(spec["scoped_constraint"]),
                list(spec["scoped_columns"]),
            )
            batch_op.create_index(
                str(spec["legacy_index"]),
                list(spec["legacy_columns"]),
                unique=True,
                sqlite_where=sa.text(LEGACY_SCOPE_PREDICATE),
                postgresql_where=sa.text(LEGACY_SCOPE_PREDICATE),
            )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_tables(bind)
    for table_name, spec in TABLES.items():
        legacy_columns = tuple(str(column) for column in spec["legacy_columns"])
        duplicate = bind.execute(
            sa.text(
                f"SELECT {', '.join(legacy_columns)} FROM {table_name} "
                f"GROUP BY {', '.join(legacy_columns)} "
                "HAVING COUNT(*) > 1 LIMIT 1"
            )
        ).first()
        if duplicate is not None:
            raise RuntimeError(
                "cannot downgrade: colliding scoped crypto strategy rows "
                f"exist in {table_name}"
            )

    for table_name in TABLES:
        attributed = bind.execute(
            sa.text(
                f"SELECT 1 FROM {table_name} "
                "WHERE account_scope IS NOT NULL OR strategy_family IS NOT NULL "
                "LIMIT 1"
            )
        ).first()
        if attributed is not None:
            raise RuntimeError(
                "cannot downgrade: attributed crypto strategy rows would lose "
                f"their account and strategy-family scope in {table_name}"
            )

    for table_name, spec in TABLES.items():
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.drop_index(str(spec["legacy_index"]))
            batch_op.drop_constraint(str(spec["scoped_constraint"]), type_="unique")
            batch_op.drop_constraint(str(spec["scope_check"]), type_="check")
            batch_op.create_unique_constraint(
                str(spec["legacy_constraint"]),
                list(spec["legacy_columns"]),
            )
            batch_op.drop_column("strategy_family")
            batch_op.drop_column("account_scope")


def _assert_no_legacy_duplicates(bind: sa.engine.Connection) -> None:
    for table_name, spec in TABLES.items():
        legacy_columns = tuple(str(column) for column in spec["legacy_columns"])
        duplicate = bind.execute(
            sa.text(
                f"SELECT {', '.join(legacy_columns)} FROM {table_name} "
                f"GROUP BY {', '.join(legacy_columns)} "
                "HAVING COUNT(*) > 1 LIMIT 1"
            )
        ).first()
        if duplicate is not None:
            raise RuntimeError(
                "cannot upgrade: duplicate legacy crypto strategy rows exist in "
                f"{table_name}"
            )


def _lock_postgresql_tables(bind: sa.engine.Connection) -> None:
    if bind.dialect.name == "postgresql":
        bind.execute(
            sa.text(
                "LOCK TABLE crypto_strategy_states, crypto_strategy_risk_states, "
                "crypto_strategy_decisions IN ACCESS EXCLUSIVE MODE"
            )
        )
