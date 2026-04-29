"""Add durable broker session auth state.

Revision ID: 202604300002
Revises: 202604300001
Create Date: 2026-04-30 00:30:00.000000
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "202604300002"
down_revision: str | None = "202604300001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "broker_sessions",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("api_base_url", sa.String(length=255), nullable=True),
        sa.Column("token_type", sa.String(length=32), nullable=True),
        sa.Column("access_token", sa.Text(), nullable=True),
        sa.Column("access_token_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refresh_token", sa.Text(), nullable=True),
        sa.Column("refresh_token_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approval_key", sa.Text(), nullable=True),
        sa.Column("approval_key_issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_authenticated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_broker_sessions")),
    )
    op.create_table(
        "broker_session_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("broker_session_id", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("previous_state", sa.JSON(), nullable=True),
        sa.Column("current_state", sa.JSON(), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
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
            ["broker_session_id"],
            ["broker_sessions.id"],
            name=op.f("fk_broker_session_events_broker_session_id_broker_sessions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_broker_session_events")),
    )
    op.create_index(
        op.f("ix_broker_session_events_broker_session_id"),
        "broker_session_events",
        ["broker_session_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_broker_session_events_broker_session_id"),
        table_name="broker_session_events",
    )
    op.drop_table("broker_session_events")
    op.drop_table("broker_sessions")
