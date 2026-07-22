"""Add execution fill date lookup index.

Revision ID: 202607220001
Revises: 202607180001
Create Date: 2026-07-22 12:10:00.000000
"""
from collections.abc import Sequence

from alembic import op

revision: str = "202607220001"
down_revision: str | None = "202607180001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        op.f("ix_execution_fills_filled_at_broker_order_pk"),
        "execution_fills",
        ["filled_at", "broker_order_pk"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_execution_fills_filled_at_broker_order_pk"),
        table_name="execution_fills",
    )
