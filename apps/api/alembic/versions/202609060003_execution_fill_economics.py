"""Add explicit execution fill economics metadata.

Revision ID: 202609060003
Revises: 202609060002
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609060003"
down_revision: str | None = "202609060002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "execution_fills",
        sa.Column("settlement_currency", sa.String(length=8), nullable=True),
    )
    op.add_column(
        "execution_fills",
        sa.Column("transaction_cost", sa.Numeric(28, 8), nullable=True),
    )
    op.add_column(
        "execution_fills",
        sa.Column("transaction_cost_coverage", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "execution_fills",
        sa.Column("transaction_cost_source", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("execution_fills", "transaction_cost_source")
    op.drop_column("execution_fills", "transaction_cost_coverage")
    op.drop_column("execution_fills", "transaction_cost")
    op.drop_column("execution_fills", "settlement_currency")
