"""Scope FIFO inventory isolation records to an exact broker route.

Revision ID: 202609090002
Revises: 202609090001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609090002"
down_revision: str | None = "202609090001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE_NAME = "fifo_inventory_isolations"
LEGACY_CONSTRAINT = "uq_fifo_inventory_isolations_mode_symbol_reconciled"
SCOPED_CONSTRAINT = "uq_fifo_inventory_isolations_route_symbol_reconciled"
LEGACY_INDEX = "ix_fifo_inventory_isolations_mode_symbol_isolated_at"
SCOPED_INDEX = "ix_fifo_inventory_isolations_route_symbol_isolated_at"
LEGACY_SCOPE_INDEX = "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled"
SCOPE_COLUMNS = (
    "provider",
    "account_scope",
    "venue",
    "asset_class",
    "exchange",
)
SCOPE_CHECK = "ck_fifo_inventory_isolations_scope_complete"
LEGACY_SCOPE_PREDICATE = (
    "provider IS NULL AND account_scope IS NULL AND venue IS NULL "
    "AND asset_class IS NULL AND exchange IS NULL"
)


def upgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("provider", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("account_scope", sa.String(length=128), nullable=True)
        )
        batch_op.add_column(sa.Column("venue", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("asset_class", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(sa.Column("exchange", sa.String(length=32), nullable=True))
        batch_op.drop_index(LEGACY_INDEX)
        batch_op.drop_constraint(LEGACY_CONSTRAINT, type_="unique")
        batch_op.create_check_constraint(
            SCOPE_CHECK,
            "(provider IS NULL AND account_scope IS NULL AND venue IS NULL "
            "AND asset_class IS NULL AND exchange IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND venue IS NOT NULL AND asset_class IS NOT NULL "
            "AND exchange IS NOT NULL)",
        )
        batch_op.create_unique_constraint(
            SCOPED_CONSTRAINT,
            [
                "mode",
                *SCOPE_COLUMNS,
                "symbol",
                "broker_reconciled_at",
            ],
        )
        batch_op.create_index(
            SCOPED_INDEX,
            ["mode", *SCOPE_COLUMNS, "symbol", "isolated_through_at"],
            unique=False,
        )
        batch_op.create_index(
            LEGACY_SCOPE_INDEX,
            ["mode", "symbol", "broker_reconciled_at"],
            unique=True,
            sqlite_where=sa.text(LEGACY_SCOPE_PREDICATE),
            postgresql_where=sa.text(LEGACY_SCOPE_PREDICATE),
        )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    duplicate = bind.execute(
        sa.text(
            "SELECT mode, symbol, broker_reconciled_at "
            f"FROM {TABLE_NAME} "
            "GROUP BY mode, symbol, broker_reconciled_at "
            "HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicate is not None:
        raise RuntimeError(
            "cannot downgrade: colliding scoped FIFO isolation rows would violate "
            "the legacy one-row constraint"
        )
    attributed = bind.execute(
        sa.text(
            f"SELECT 1 FROM {TABLE_NAME} WHERE "
            + " OR ".join(f"{column} IS NOT NULL" for column in SCOPE_COLUMNS)
            + " LIMIT 1"
        )
    ).first()
    if attributed is not None:
        raise RuntimeError(
            "cannot downgrade: attributed FIFO isolation rows would lose their "
            "broker inventory route"
        )
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_index(LEGACY_SCOPE_INDEX)
        batch_op.drop_index(SCOPED_INDEX)
        batch_op.drop_constraint(SCOPED_CONSTRAINT, type_="unique")
        batch_op.drop_constraint(SCOPE_CHECK, type_="check")
        batch_op.create_unique_constraint(
            LEGACY_CONSTRAINT,
            ["mode", "symbol", "broker_reconciled_at"],
        )
        batch_op.create_index(
            LEGACY_INDEX,
            ["mode", "symbol", "isolated_through_at"],
            unique=False,
        )
        for column_name in reversed(SCOPE_COLUMNS):
            batch_op.drop_column(column_name)


def _lock_postgresql_table(bind: sa.engine.Connection) -> None:
    if bind.dialect.name == "postgresql":
        bind.execute(
            sa.text(
                "LOCK TABLE fifo_inventory_isolations IN ACCESS EXCLUSIVE MODE"
            )
        )
