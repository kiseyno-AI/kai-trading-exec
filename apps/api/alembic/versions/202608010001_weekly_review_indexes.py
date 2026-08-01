"""Add weekly review query indexes.

Revision ID: 202608010001
Revises: 202607240001
Create Date: 2026-08-01 10:45:00.000000
"""
from collections.abc import Sequence

from alembic import op

revision: str = "202608010001"
down_revision: str | None = "202607240001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_broker_orders_mode_submitted_at "
        "ON broker_orders (mode, submitted_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_broker_orders_mode_status_submitted_at "
        "ON broker_orders (mode, status, submitted_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_execution_plan_items_status_updated_at "
        "ON execution_plan_items (status, updated_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_execution_plan_items_created_at "
        "ON execution_plan_items (created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "ix_execution_failures_mode_latest_failed_at "
        "ON execution_failures (mode, latest_failed_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_execution_failures_mode_latest_failed_at")
    op.execute("DROP INDEX IF EXISTS ix_execution_plan_items_created_at")
    op.execute("DROP INDEX IF EXISTS ix_execution_plan_items_status_updated_at")
    op.execute("DROP INDEX IF EXISTS ix_broker_orders_mode_status_submitted_at")
    op.execute("DROP INDEX IF EXISTS ix_broker_orders_mode_submitted_at")
