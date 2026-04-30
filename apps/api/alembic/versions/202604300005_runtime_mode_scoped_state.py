"""Scope runtime order and monitoring state by runtime mode.

Revision ID: 202604300005
Revises: 202604300004
Create Date: 2026-04-30 00:50:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604300005"
down_revision: str | None = "202604300004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.add_column(sa.Column("mode", sa.String(length=16), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE broker_orders
            SET mode = COALESCE(
                (
                    SELECT execution_plans.mode
                    FROM execution_plan_items
                    JOIN execution_plans
                      ON execution_plans.id = execution_plan_items.execution_plan_id
                    WHERE execution_plan_items.id = broker_orders.execution_plan_item_id
                ),
                (
                    SELECT runtime_control_state.mode
                    FROM runtime_control_state
                    WHERE runtime_control_state.id = 'global'
                ),
                'mock'
            )
            WHERE mode IS NULL
            """
        )
    )
    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.alter_column("mode", existing_type=sa.String(length=16), nullable=False)
        batch_op.drop_constraint("uq_broker_orders_idempotency_key", type_="unique")
        batch_op.create_unique_constraint(
            "uq_broker_orders_idempotency_key_mode",
            ["idempotency_key", "mode"],
        )

    with op.batch_alter_table("execution_positions") as batch_op:
        batch_op.add_column(sa.Column("mode", sa.String(length=16), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE execution_positions
            SET mode = COALESCE(
                (
                    SELECT runtime_control_state.mode
                    FROM runtime_control_state
                    WHERE runtime_control_state.id = 'global'
                ),
                'mock'
            )
            WHERE mode IS NULL
            """
        )
    )
    with op.batch_alter_table("execution_positions") as batch_op:
        batch_op.alter_column("mode", existing_type=sa.String(length=16), nullable=False)
        batch_op.drop_constraint("uq_execution_positions_symbol", type_="unique")
        batch_op.create_unique_constraint(
            "uq_execution_positions_symbol_mode",
            ["symbol", "mode"],
        )

    with op.batch_alter_table("monitoring_checkpoints") as batch_op:
        batch_op.add_column(sa.Column("mode", sa.String(length=16), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE monitoring_checkpoints
            SET mode = COALESCE(
                (
                    SELECT runtime_control_state.mode
                    FROM runtime_control_state
                    WHERE runtime_control_state.id = 'global'
                ),
                'mock'
            )
            WHERE mode IS NULL
            """
        )
    )
    with op.batch_alter_table("monitoring_checkpoints") as batch_op:
        batch_op.alter_column("mode", existing_type=sa.String(length=16), nullable=False)
        batch_op.drop_constraint("uq_monitoring_checkpoints_worker_name", type_="unique")
        batch_op.create_unique_constraint(
            "uq_monitoring_checkpoints_worker_name_mode",
            ["worker_name", "mode"],
        )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY idempotency_key
                        ORDER BY created_at ASC, id ASC
                    ) AS row_number
                FROM broker_orders
            )
            DELETE FROM execution_fills
            WHERE broker_order_pk IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY idempotency_key
                        ORDER BY created_at ASC, id ASC
                    ) AS row_number
                FROM broker_orders
            )
            DELETE FROM broker_order_events
            WHERE broker_order_pk IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
            """
        )
    )
    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY idempotency_key
                        ORDER BY created_at ASC, id ASC
                    ) AS row_number
                FROM broker_orders
            )
            DELETE FROM broker_orders
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
            """
        )
    )
    with op.batch_alter_table("broker_orders") as batch_op:
        batch_op.drop_constraint("uq_broker_orders_idempotency_key_mode", type_="unique")
        batch_op.create_unique_constraint(
            "uq_broker_orders_idempotency_key",
            ["idempotency_key"],
        )
        batch_op.drop_column("mode")

    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY created_at ASC, id ASC
                    ) AS row_number
                FROM execution_positions
            )
            DELETE FROM execution_positions
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
            """
        )
    )
    with op.batch_alter_table("execution_positions") as batch_op:
        batch_op.drop_constraint("uq_execution_positions_symbol_mode", type_="unique")
        batch_op.create_unique_constraint(
            "uq_execution_positions_symbol",
            ["symbol"],
        )
        batch_op.drop_column("mode")

    op.execute(
        sa.text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY worker_name
                        ORDER BY created_at ASC, id ASC
                    ) AS row_number
                FROM monitoring_checkpoints
            )
            DELETE FROM monitoring_checkpoints
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
            """
        )
    )
    with op.batch_alter_table("monitoring_checkpoints") as batch_op:
        batch_op.drop_constraint(
            "uq_monitoring_checkpoints_worker_name_mode",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_monitoring_checkpoints_worker_name",
            ["worker_name"],
        )
        batch_op.drop_column("mode")
