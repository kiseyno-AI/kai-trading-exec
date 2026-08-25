"""Widen execution plan item status for runtime transition names.

Revision ID: 202608250001
Revises: 202608190001
"""

from alembic import op
import sqlalchemy as sa


revision = "202608250001"
down_revision = "202608190001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.alter_column(
            "status",
            existing_type=sa.String(length=32),
            type_=sa.String(length=64),
            existing_nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.alter_column(
            "status",
            existing_type=sa.String(length=64),
            type_=sa.String(length=32),
            existing_nullable=False,
        )
