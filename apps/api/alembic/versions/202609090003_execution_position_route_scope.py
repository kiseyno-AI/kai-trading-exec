"""Scope execution positions to an exact broker account and exchange route.

Revision ID: 202609090003
Revises: 202609090002
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609090003"
down_revision: str | None = "202609090002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE_NAME = "execution_positions"
LEGACY_CONSTRAINT = "uq_execution_positions_symbol_mode"
SCOPED_CONSTRAINT = "uq_execution_positions_route_symbol"
SCOPE_CHECK = "ck_execution_positions_scope_complete"
LEGACY_NULL_INDEX = "uq_execution_positions_legacy_symbol_mode"
SCOPED_INDEX = "ix_execution_positions_mode_route_symbol"
LEGACY_NULL_PREDICATE = sa.text(
    "provider IS NULL AND account_scope IS NULL AND exchange IS NULL"
)


def upgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("provider", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("account_scope", sa.String(length=128), nullable=True)
        )
        batch_op.add_column(sa.Column("exchange", sa.String(length=32), nullable=True))
        batch_op.drop_constraint(LEGACY_CONSTRAINT, type_="unique")
        batch_op.create_check_constraint(
            SCOPE_CHECK,
            "(provider IS NULL AND account_scope IS NULL AND exchange IS NULL) "
            "OR (provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND exchange IS NOT NULL)",
        )
        batch_op.create_unique_constraint(
            SCOPED_CONSTRAINT,
            ["mode", "provider", "account_scope", "exchange", "symbol"],
        )
        batch_op.create_index(
            LEGACY_NULL_INDEX,
            ["mode", "symbol"],
            unique=True,
            sqlite_where=LEGACY_NULL_PREDICATE,
            postgresql_where=LEGACY_NULL_PREDICATE,
        )
        batch_op.create_index(
            SCOPED_INDEX,
            ["mode", "provider", "account_scope", "exchange", "symbol"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    duplicate = bind.execute(
        sa.text(
            "SELECT mode, symbol FROM execution_positions "
            "GROUP BY mode, symbol HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicate is not None:
        raise RuntimeError(
            "cannot downgrade: colliding scoped execution positions would violate "
            "the legacy one-row constraint"
        )
    attributed = bind.execute(
        sa.text(
            "SELECT 1 FROM execution_positions "
            "WHERE provider IS NOT NULL OR account_scope IS NOT NULL "
            "OR exchange IS NOT NULL LIMIT 1"
        )
    ).first()
    if attributed is not None:
        raise RuntimeError(
            "cannot downgrade: attributed execution positions would lose their "
            "broker account and exchange route"
        )
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_index(SCOPED_INDEX)
        batch_op.drop_index(LEGACY_NULL_INDEX)
        batch_op.drop_constraint(SCOPED_CONSTRAINT, type_="unique")
        batch_op.drop_constraint(SCOPE_CHECK, type_="check")
        batch_op.create_unique_constraint(
            LEGACY_CONSTRAINT,
            ["symbol", "mode"],
        )
        batch_op.drop_column("exchange")
        batch_op.drop_column("account_scope")
        batch_op.drop_column("provider")


def _lock_postgresql_table(bind: sa.engine.Connection) -> None:
    if bind.dialect.name == "postgresql":
        bind.execute(
            sa.text("LOCK TABLE execution_positions IN ACCESS EXCLUSIVE MODE")
        )
