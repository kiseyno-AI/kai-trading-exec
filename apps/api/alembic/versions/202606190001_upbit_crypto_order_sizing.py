"""Add Upbit provider and crypto order sizing fields.

Revision ID: 202606190001
Revises: 202606180001
Create Date: 2026-06-19 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202606190001"
down_revision: str | None = "202606180001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.add_column(sa.Column("provider", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("venue", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("asset_class", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("base_quantity", sa.Numeric(28, 12), nullable=True))
        batch_op.add_column(sa.Column("quote_notional", sa.Numeric(28, 4), nullable=True))
        batch_op.add_column(sa.Column("quantity_precision", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("price_precision", sa.Integer(), nullable=True))

    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.alter_column("quantity", existing_type=sa.Integer(), nullable=True)
        batch_op.add_column(sa.Column("base_quantity", sa.Numeric(28, 12), nullable=True))
        batch_op.add_column(sa.Column("quote_notional", sa.Numeric(28, 4), nullable=True))
        batch_op.add_column(sa.Column("quantity_precision", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("price_precision", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("venue", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("asset_class", sa.String(length=32), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.drop_column("asset_class")
        batch_op.drop_column("venue")
        batch_op.drop_column("price_precision")
        batch_op.drop_column("quantity_precision")
        batch_op.drop_column("quote_notional")
        batch_op.drop_column("base_quantity")
        batch_op.alter_column("quantity", existing_type=sa.Integer(), nullable=False)

    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.drop_column("price_precision")
        batch_op.drop_column("quantity_precision")
        batch_op.drop_column("quote_notional")
        batch_op.drop_column("base_quantity")
        batch_op.drop_column("asset_class")
        batch_op.drop_column("venue")
        batch_op.drop_column("provider")
