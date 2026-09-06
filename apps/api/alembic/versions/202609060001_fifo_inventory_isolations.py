"""Add durable FIFO inventory isolation records.

Revision ID: 202609060001
Revises: 202608250001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609060001"
down_revision: str | None = "202608250001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fifo_inventory_isolations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("residual_quantity", sa.Numeric(28, 12), nullable=False),
        sa.Column("last_local_fill_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("broker_position_id", sa.String(length=36), nullable=False),
        sa.Column("broker_position_quantity", sa.Numeric(28, 12), nullable=False),
        sa.Column("broker_reconciled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("isolated_through_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_source", sa.String(length=64), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("audit_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fifo_inventory_isolations")),
        sa.UniqueConstraint(
            "mode",
            "symbol",
            "broker_reconciled_at",
            name="uq_fifo_inventory_isolations_mode_symbol_reconciled",
        ),
    )
    op.create_index(
        "ix_fifo_inventory_isolations_broker_position_id",
        "fifo_inventory_isolations",
        ["broker_position_id"],
        unique=False,
    )
    op.create_index(
        "ix_fifo_inventory_isolations_mode_symbol_isolated_at",
        "fifo_inventory_isolations",
        ["mode", "symbol", "isolated_through_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_fifo_inventory_isolations_mode_symbol_isolated_at",
        table_name="fifo_inventory_isolations",
    )
    op.drop_index(
        "ix_fifo_inventory_isolations_broker_position_id",
        table_name="fifo_inventory_isolations",
    )
    op.drop_table("fifo_inventory_isolations")
