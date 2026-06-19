"""Add execution plan item audit events.

Revision ID: 202606190003
Revises: 202606190002
Create Date: 2026-06-19 02:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202606190003"
down_revision: str | None = "202606190002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_plan_item_audits",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("execution_plan_item_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_plan_item_id"],
            ["execution_plan_items.id"],
            name=op.f("fk_execution_plan_item_audits_execution_plan_item_id_execution_plan_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_plan_item_audits")),
    )
    op.create_index(
        op.f("ix_execution_plan_item_audits_execution_plan_item_id"),
        "execution_plan_item_audits",
        ["execution_plan_item_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_plan_item_audits_event_type"),
        "execution_plan_item_audits",
        ["event_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_plan_item_audits_occurred_at"),
        "execution_plan_item_audits",
        ["occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_execution_plan_item_audits_occurred_at"), table_name="execution_plan_item_audits")
    op.drop_index(op.f("ix_execution_plan_item_audits_event_type"), table_name="execution_plan_item_audits")
    op.drop_index(
        op.f("ix_execution_plan_item_audits_execution_plan_item_id"),
        table_name="execution_plan_item_audits",
    )
    op.drop_table("execution_plan_item_audits")
