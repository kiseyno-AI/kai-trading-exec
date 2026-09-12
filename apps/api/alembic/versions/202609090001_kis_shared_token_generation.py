"""Mirror the shared KIS token generation in broker sessions.

Revision ID: 202609090001
Revises: 202609080002
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609090001"
down_revision: str | None = "202609080002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "broker_sessions",
        sa.Column("generation", sa.String(length=36), nullable=True),
    )
    op.add_column(
        "broker_sessions",
        sa.Column("refresh_after", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("broker_sessions", "refresh_after")
    op.drop_column("broker_sessions", "generation")
