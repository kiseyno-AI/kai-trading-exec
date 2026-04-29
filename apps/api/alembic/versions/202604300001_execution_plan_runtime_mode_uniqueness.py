"""Scope execution plan uniqueness by runtime mode.

Revision ID: 202604300001
Revises: 202604290003
Create Date: 2026-04-30 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op

revision: str = "202604300001"
down_revision: str | None = "202604290003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_plans") as batch_op:
        batch_op.drop_constraint(
            "uq_execution_plans_external_plan_id",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_execution_plans_external_plan_id_mode",
            ["external_plan_id", "mode"],
        )


def downgrade() -> None:
    with op.batch_alter_table("execution_plans") as batch_op:
        batch_op.drop_constraint(
            "uq_execution_plans_external_plan_id_mode",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_execution_plans_external_plan_id",
            ["external_plan_id"],
        )
