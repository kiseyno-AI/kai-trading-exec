"""Remove duplicated runtime automation state.

Revision ID: 202610020001
Revises: 202609110003
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202610020001"
down_revision: str | None = "202609110003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("runtime_control_state") as batch_op:
        batch_op.drop_column("automation_enabled")


def downgrade() -> None:
    with op.batch_alter_table("runtime_control_state") as batch_op:
        batch_op.add_column(
            sa.Column(
                "automation_enabled",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
    op.execute(
        sa.text(
            """
            UPDATE runtime_control_state
            SET automation_enabled = CASE
                WHEN paused = false AND kill_switch_enabled = false THEN true
                ELSE false
            END
            """
        )
    )
    with op.batch_alter_table("runtime_control_state") as batch_op:
        batch_op.alter_column(
            "automation_enabled",
            existing_type=sa.Boolean(),
            existing_nullable=False,
            existing_server_default=sa.false(),
            server_default=None,
        )
