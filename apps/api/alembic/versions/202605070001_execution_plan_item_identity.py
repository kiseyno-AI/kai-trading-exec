"""Add durable execution-plan item identity fields.

Revision ID: 202605070001
Revises: 202605060001
Create Date: 2026-05-07 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202605070001"
down_revision: str | None = "202605060001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "execution_plan_items",
        sa.Column("decision_agent_name", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("decision_run_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("source_plan_item_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "execution_plan_items",
        sa.Column("source_market_date", sa.Date(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("execution_plan_items", "source_market_date")
    op.drop_column("execution_plan_items", "source_plan_item_id")
    op.drop_column("execution_plan_items", "decision_run_id")
    op.drop_column("execution_plan_items", "decision_agent_name")
