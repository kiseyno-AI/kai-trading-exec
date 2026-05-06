"""Add actionable execution failures table.

Revision ID: 202605060001
Revises: 202604300005
Create Date: 2026-05-06 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202605060001"
down_revision: str | None = "202604300005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_failures",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("broker_order_pk", sa.String(length=36), nullable=False),
        sa.Column("execution_plan_id", sa.String(length=36), nullable=True),
        sa.Column("execution_plan_item_id", sa.String(length=36), nullable=True),
        sa.Column("plan_id", sa.String(length=128), nullable=True),
        sa.Column("plan_source", sa.String(length=64), nullable=True),
        sa.Column("strategy_run_id", sa.String(length=128), nullable=True),
        sa.Column("decision_actor", sa.String(length=128), nullable=True),
        sa.Column("broker_name", sa.String(length=32), nullable=False, server_default="kis"),
        sa.Column("account", sa.String(length=64), nullable=True),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("intent", sa.String(length=64), nullable=True),
        sa.Column("requested_quantity", sa.Integer(), nullable=False),
        sa.Column("requested_limit_price", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("requested_notional", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("classification", sa.String(length=64), nullable=False),
        sa.Column("broker_error_classification", sa.String(length=64), nullable=True),
        sa.Column("broker_error_code", sa.String(length=64), nullable=True),
        sa.Column("broker_error_message", sa.Text(), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="open"),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("latest_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status_updated_by", sa.String(length=128), nullable=True),
        sa.Column("status_updated_source", sa.String(length=64), nullable=True),
        sa.Column("status_updated_reason", sa.Text(), nullable=True),
        sa.Column("raw_context", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["broker_order_pk"], ["broker_orders.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("broker_order_pk", name="uq_execution_failures_broker_order_pk"),
    )
    op.create_index(op.f("ix_execution_failures_mode"), "execution_failures", ["mode"], unique=False)
    op.create_index(
        op.f("ix_execution_failures_broker_order_pk"),
        "execution_failures",
        ["broker_order_pk"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_failures_execution_plan_id"),
        "execution_failures",
        ["execution_plan_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_execution_failures_execution_plan_item_id"),
        "execution_failures",
        ["execution_plan_item_id"],
        unique=False,
    )
    op.create_index(op.f("ix_execution_failures_plan_id"), "execution_failures", ["plan_id"], unique=False)
    op.create_index(op.f("ix_execution_failures_symbol"), "execution_failures", ["symbol"], unique=False)
    op.create_index(
        op.f("ix_execution_failures_classification"),
        "execution_failures",
        ["classification"],
        unique=False,
    )
    op.create_index(op.f("ix_execution_failures_status"), "execution_failures", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_execution_failures_status"), table_name="execution_failures")
    op.drop_index(op.f("ix_execution_failures_classification"), table_name="execution_failures")
    op.drop_index(op.f("ix_execution_failures_symbol"), table_name="execution_failures")
    op.drop_index(op.f("ix_execution_failures_plan_id"), table_name="execution_failures")
    op.drop_index(
        op.f("ix_execution_failures_execution_plan_item_id"),
        table_name="execution_failures",
    )
    op.drop_index(
        op.f("ix_execution_failures_execution_plan_id"),
        table_name="execution_failures",
    )
    op.drop_index(op.f("ix_execution_failures_broker_order_pk"), table_name="execution_failures")
    op.drop_index(op.f("ix_execution_failures_mode"), table_name="execution_failures")
    op.drop_table("execution_failures")
