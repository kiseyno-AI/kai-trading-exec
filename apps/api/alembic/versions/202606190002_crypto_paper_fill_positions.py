"""Add decimal crypto fill and position quantities.

Revision ID: 202606190002
Revises: 202606190001
Create Date: 2026-06-19 01:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202606190002"
down_revision: str | None = "202606190001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_fills") as batch_op:
        batch_op.add_column(sa.Column("base_quantity", sa.Numeric(28, 12), nullable=True))
        batch_op.add_column(sa.Column("quote_notional", sa.Numeric(28, 4), nullable=True))

    with op.batch_alter_table("execution_positions") as batch_op:
        batch_op.add_column(sa.Column("base_quantity", sa.Numeric(28, 12), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("execution_positions") as batch_op:
        batch_op.drop_column("base_quantity")

    with op.batch_alter_table("execution_fills") as batch_op:
        batch_op.drop_column("quote_notional")
        batch_op.drop_column("base_quantity")
