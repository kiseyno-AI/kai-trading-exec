"""Add retry audit fields for rejected broker orders.

Revision ID: 202606180001
Revises: 202605070001
Create Date: 2026-06-18 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202606180001"
down_revision: str | None = "202605070001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_failures") as batch_op:
        batch_op.add_column(
            sa.Column("retry_decision_status", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(sa.Column("retry_decision_reason", sa.Text(), nullable=True))
        batch_op.add_column(
            sa.Column("retry_decision_source", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(
            sa.Column("retry_next_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(
            sa.Column("retry_deadline_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(sa.Column("retry_max_attempts", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "retry_attempt_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            )
        )
        batch_op.add_column(
            sa.Column("retry_job_id", sa.String(length=36), nullable=True)
        )
        batch_op.add_column(
            sa.Column("retry_order_idempotency_key", sa.String(length=128), nullable=True)
        )
        batch_op.add_column(
            sa.Column("retry_exhausted_reason", sa.Text(), nullable=True)
        )
        batch_op.add_column(sa.Column("retry_audit_context", sa.JSON(), nullable=True))

    op.create_index(
        op.f("ix_execution_failures_retry_decision_status"),
        "execution_failures",
        ["retry_decision_status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_failures_retry_next_at"),
        "execution_failures",
        ["retry_next_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_failures_retry_job_id"),
        "execution_failures",
        ["retry_job_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_execution_failures_retry_job_id"),
        table_name="execution_failures",
    )
    op.drop_index(
        op.f("ix_execution_failures_retry_next_at"),
        table_name="execution_failures",
    )
    op.drop_index(
        op.f("ix_execution_failures_retry_decision_status"),
        table_name="execution_failures",
    )
    with op.batch_alter_table("execution_failures") as batch_op:
        batch_op.drop_column("retry_audit_context")
        batch_op.drop_column("retry_exhausted_reason")
        batch_op.drop_column("retry_order_idempotency_key")
        batch_op.drop_column("retry_job_id")
        batch_op.drop_column("retry_attempt_count")
        batch_op.drop_column("retry_max_attempts")
        batch_op.drop_column("retry_deadline_at")
        batch_op.drop_column("retry_next_at")
        batch_op.drop_column("retry_decision_source")
        batch_op.drop_column("retry_decision_reason")
        batch_op.drop_column("retry_decision_status")
