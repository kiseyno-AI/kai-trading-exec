"""Persist scoped Core market buy authority observations.

Revision ID: 202609110003
Revises: 202609110002
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609110003"
down_revision: str | None = "202609110002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "core_buy_authority_states",
        sa.Column("scope_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("market", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column("runtime_mode", sa.String(length=16), nullable=False),
        sa.Column("account_scope", sa.String(length=64), nullable=False),
        sa.Column("fence_active", sa.Boolean(), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False),
        sa.Column("delivery_status", sa.String(length=32), nullable=False),
        sa.Column("projection_pending", sa.Boolean(), nullable=False),
        sa.Column("healthy_observation_count", sa.Integer(), nullable=False),
        sa.Column("last_observation_id", sa.String(length=128), nullable=False),
        sa.Column("last_material_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("last_action", sa.String(length=32), nullable=False),
        sa.Column("market_date", sa.Date(), nullable=False),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "last_healthy_source_observed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_block_source_observed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("last_blocked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("context", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "provider = 'kis' AND market = 'KRX' AND currency = 'KRW'",
            name="ck_core_buy_authority_states_krx_scope",
        ),
        sa.PrimaryKeyConstraint("scope_fingerprint"),
        sa.UniqueConstraint(
            "provider",
            "market",
            "currency",
            "runtime_mode",
            "account_scope",
            name="uq_core_buy_authority_states_scope",
        ),
    )
    op.create_table(
        "core_buy_authority_observations",
        sa.Column("observation_id", sa.String(length=128), nullable=False),
        sa.Column("scope_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("material_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("ack_payload", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["scope_fingerprint"],
            ["core_buy_authority_states.scope_fingerprint"],
        ),
        sa.PrimaryKeyConstraint("observation_id"),
    )
    op.create_index(
        "ix_core_buy_authority_observations_scope_received",
        "core_buy_authority_observations",
        ["scope_fingerprint", "received_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_core_buy_authority_observations_scope_received",
        table_name="core_buy_authority_observations",
    )
    op.drop_table("core_buy_authority_observations")
    op.drop_table("core_buy_authority_states")
