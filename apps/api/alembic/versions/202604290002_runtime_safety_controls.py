"""Add durable runtime safety control metadata and audit events.

Revision ID: 202604290002
Revises: 202604290001
Create Date: 2026-04-29 00:30:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604290002"
down_revision: str | None = "202604290001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runtime_control_state",
        sa.Column(
            "last_transition_action",
            sa.String(length=32),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column("last_transition_actor", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column(
            "last_transition_source",
            sa.String(length=64),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column("last_transition_reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column(
            "last_transition_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column("last_transition_event_id", sa.String(length=36), nullable=True),
    )

    op.execute(
        sa.text(
            """
            UPDATE runtime_control_state
            SET
                last_transition_action = 'bootstrap',
                last_transition_source = 'system',
                last_transition_reason = readiness_reason,
                last_transition_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
            """
        )
    )

    op.create_table(
        "runtime_control_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("runtime_control_state_id", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("previous_state", sa.JSON(), nullable=True),
        sa.Column("current_state", sa.JSON(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["runtime_control_state_id"],
            ["runtime_control_state.id"],
            name=op.f("fk_runtime_control_events_runtime_control_state_id_runtime_control_state"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_control_events")),
    )
    op.create_index(
        op.f("ix_runtime_control_events_runtime_control_state_id"),
        "runtime_control_events",
        ["runtime_control_state_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_runtime_control_events_runtime_control_state_id"),
        table_name="runtime_control_events",
    )
    op.drop_table("runtime_control_events")
    op.drop_column("runtime_control_state", "last_transition_event_id")
    op.drop_column("runtime_control_state", "last_transition_at")
    op.drop_column("runtime_control_state", "last_transition_reason")
    op.drop_column("runtime_control_state", "last_transition_source")
    op.drop_column("runtime_control_state", "last_transition_actor")
    op.drop_column("runtime_control_state", "last_transition_action")
