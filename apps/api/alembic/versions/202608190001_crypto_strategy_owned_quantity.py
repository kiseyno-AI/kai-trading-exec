"""Track Alpaca strategy-owned base quantity.

Revision ID: 202608190001
Revises: 202608160001
Create Date: 2026-08-19 16:30:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202608190001"
down_revision: str | None = "202608160001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "crypto_strategy_states",
        sa.Column(
            "strategy_owned_base_quantity",
            sa.Numeric(28, 12),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("crypto_strategy_states", "strategy_owned_base_quantity")
