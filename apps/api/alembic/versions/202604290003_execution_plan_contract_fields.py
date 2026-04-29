"""Add execution plan contract fields used by northbound APIs.

Revision ID: 202604290003
Revises: 202604290002
Create Date: 2026-04-29 01:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604290003"
down_revision: str | None = "202604290002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "execution_plans",
        sa.Column("market_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "execution_plans",
        sa.Column("strategy_run_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("limit_price", sa.Numeric(18, 4), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("trigger_price", sa.Numeric(18, 4), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("time_in_force", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("execution_plan_items", "time_in_force")
    op.drop_column("execution_plan_items", "trigger_price")
    op.drop_column("execution_plan_items", "limit_price")
    op.drop_column("execution_plans", "strategy_run_id")
    op.drop_column("execution_plans", "market_date")
