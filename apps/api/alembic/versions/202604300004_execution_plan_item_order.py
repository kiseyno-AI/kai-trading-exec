"""Persist execution plan item order for restart-safe scheduling.

Revision ID: 202604300004
Revises: 202604300003
Create Date: 2026-04-30 01:15:00.000000
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604300004"
down_revision: str | None = "202604300003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.add_column(
            sa.Column(
                "plan_item_index",
                sa.Integer(),
                nullable=True,
                server_default="0",
            )
        )

    execution_plan_items = sa.table(
        "execution_plan_items",
        sa.column("id", sa.String(length=36)),
        sa.column("execution_plan_id", sa.String(length=36)),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("plan_item_index", sa.Integer()),
    )
    connection = op.get_bind()
    rows = connection.execute(
        sa.select(
            execution_plan_items.c.id,
            execution_plan_items.c.execution_plan_id,
        ).order_by(
            execution_plan_items.c.execution_plan_id.asc(),
            execution_plan_items.c.created_at.asc(),
            execution_plan_items.c.id.asc(),
        )
    ).all()

    current_plan_id: str | None = None
    current_index = 0
    for row in rows:
        if row.execution_plan_id != current_plan_id:
            current_plan_id = row.execution_plan_id
            current_index = 0
        connection.execute(
            execution_plan_items.update()
            .where(execution_plan_items.c.id == row.id)
            .values(plan_item_index=current_index)
        )
        current_index += 1

    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.alter_column(
            "plan_item_index",
            existing_type=sa.Integer(),
            nullable=False,
            server_default=None,
        )


def downgrade() -> None:
    with op.batch_alter_table("execution_plan_items") as batch_op:
        batch_op.drop_column("plan_item_index")
