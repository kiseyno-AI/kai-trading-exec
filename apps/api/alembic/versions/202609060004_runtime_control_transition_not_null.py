"""Enforce required runtime-control transition metadata.

Revision ID: 202609060004
Revises: 202609060003
Create Date: 2026-09-06 19:30:00.000000
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609060004"
down_revision: str | None = "202609060003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE runtime_control_state
            SET
                last_transition_action = COALESCE(last_transition_action, 'bootstrap'),
                last_transition_source = COALESCE(last_transition_source, 'system'),
                last_transition_at = COALESCE(
                    last_transition_at,
                    updated_at,
                    created_at,
                    CURRENT_TIMESTAMP
                )
            WHERE
                last_transition_action IS NULL
                OR last_transition_source IS NULL
                OR last_transition_at IS NULL
            """
        )
    )
    with op.batch_alter_table("runtime_control_state") as batch_op:
        batch_op.alter_column(
            "last_transition_action",
            existing_type=sa.String(length=32),
            nullable=False,
        )
        batch_op.alter_column(
            "last_transition_source",
            existing_type=sa.String(length=64),
            nullable=False,
        )
        batch_op.alter_column(
            "last_transition_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("runtime_control_state") as batch_op:
        batch_op.alter_column(
            "last_transition_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=True,
        )
        batch_op.alter_column(
            "last_transition_source",
            existing_type=sa.String(length=64),
            nullable=True,
        )
        batch_op.alter_column(
            "last_transition_action",
            existing_type=sa.String(length=32),
            nullable=True,
        )
