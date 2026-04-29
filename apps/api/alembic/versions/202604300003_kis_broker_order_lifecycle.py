"""Add KIS adapter audit fields for broker order lifecycle.

Revision ID: 202604300003
Revises: 202604300002
Create Date: 2026-04-30 00:30:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604300003"
down_revision: str | None = "202604300002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.add_column(
            sa.Column(
                "broker_name",
                sa.String(length=32),
                nullable=False,
                server_default="kis",
            )
        )
        batch_op.add_column(sa.Column("request_metadata", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("response_metadata", sa.JSON(), nullable=True))
        batch_op.add_column(
            sa.Column("last_error_classification", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(sa.Column("last_error_code", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("last_error_message", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("last_error_retryable", sa.Boolean(), nullable=True))

    with op.batch_alter_table("broker_order_events") as batch_op:
        batch_op.add_column(sa.Column("event_source", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("normalized_status", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("error_classification", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(sa.Column("retryable", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("message", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("request_payload", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("response_payload", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("broker_order_events") as batch_op:
        batch_op.drop_column("response_payload")
        batch_op.drop_column("request_payload")
        batch_op.drop_column("message")
        batch_op.drop_column("retryable")
        batch_op.drop_column("error_classification")
        batch_op.drop_column("normalized_status")
        batch_op.drop_column("event_source")

    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.drop_column("last_error_retryable")
        batch_op.drop_column("last_error_message")
        batch_op.drop_column("last_error_code")
        batch_op.drop_column("last_error_classification")
        batch_op.drop_column("response_metadata")
        batch_op.drop_column("request_metadata")
        batch_op.drop_column("broker_name")
