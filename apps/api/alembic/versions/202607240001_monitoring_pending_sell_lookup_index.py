"""Add monitoring pending sell lookup index.

Revision ID: 202607240001
Revises: 202607220001
Create Date: 2026-07-24 16:05:00.000000
"""
from collections.abc import Sequence

from alembic import op

revision: str = "202607240001"
down_revision: str | None = "202607220001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_execution_plan_items_side_status_intent_plan_symbol "
        "ON execution_plan_items "
        "(side, status, intent, execution_plan_id, symbol)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_execution_plan_items_side_status_intent_plan_symbol")
