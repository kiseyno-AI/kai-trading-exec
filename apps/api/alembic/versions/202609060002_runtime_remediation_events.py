"""Add immutable runtime data-remediation evidence.

Revision ID: 202609060002
Revises: 202609060001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609060002"
down_revision: str | None = "202609060001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "runtime_remediation_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("candidate_type", sa.String(length=64), nullable=False),
        sa.Column("candidate_key", sa.String(length=128), nullable=False),
        sa.Column("subject_id", sa.String(length=128), nullable=True),
        sa.Column("outcome", sa.String(length=64), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("evidence_summary", sa.JSON(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_remediation_events")),
        sa.UniqueConstraint(
            "mode",
            "candidate_type",
            "candidate_key",
            "outcome",
            name="uq_runtime_remediation_events_candidate_outcome",
        ),
    )
    op.create_index(
        "ix_runtime_remediation_events_subject_id",
        "runtime_remediation_events",
        ["subject_id"],
        unique=False,
    )
    op.create_index(
        "ix_runtime_remediation_events_mode_type_occurred",
        "runtime_remediation_events",
        ["mode", "candidate_type", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_runtime_remediation_events_mode_type_occurred",
        table_name="runtime_remediation_events",
    )
    op.drop_index(
        "ix_runtime_remediation_events_subject_id",
        table_name="runtime_remediation_events",
    )
    op.drop_table("runtime_remediation_events")
