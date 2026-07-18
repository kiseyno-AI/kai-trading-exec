"""Add execution fill idempotency keys.

Revision ID: 202607180001
Revises: 202606190003
Create Date: 2026-07-18 00:40:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202607180001"
down_revision: str | None = "202606190003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "execution_fills",
        sa.Column("idempotency_key", sa.String(length=80), nullable=True),
    )
    op.create_index(
        op.f("ix_execution_fills_idempotency_key"),
        "execution_fills",
        ["idempotency_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_execution_fills_idempotency_key"), table_name="execution_fills")
    op.drop_column("execution_fills", "idempotency_key")
