"""Add the durable hourly buy-only risk-off fence.

Revision ID: 202609080001
Revises: 202609060004
Create Date: 2026-09-08 01:30:00.000000
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609080001"
down_revision: str | None = "202609060004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "runtime_control_state",
        sa.Column(
            "hourly_risk_off_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column(
            "hourly_risk_off_epoch",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "runtime_control_state",
        sa.Column("hourly_risk_off_context", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # Serialize the inactive check with every transition and hold the lock
        # through the transactional DDL. A plain SELECT (or only a row lock)
        # leaves a TOCTOU window where a concurrent transition can activate the
        # fence immediately before its columns are dropped.
        bind.execute(
            sa.text(
                "LOCK TABLE runtime_control_state IN ACCESS EXCLUSIVE MODE"
            )
        )
    active_fence = bind.execute(
        sa.text(
            "SELECT 1 FROM runtime_control_state "
            "WHERE hourly_risk_off_active = :active LIMIT 1"
        ),
        {"active": True},
    ).first()
    if active_fence is not None:
        raise RuntimeError(
            "cannot remove the hourly risk-off fence while it is active; "
            "reconcile or explicitly clear the fence before downgrading"
        )
    op.drop_column("runtime_control_state", "hourly_risk_off_context")
    op.drop_column("runtime_control_state", "hourly_risk_off_epoch")
    op.drop_column("runtime_control_state", "hourly_risk_off_active")
