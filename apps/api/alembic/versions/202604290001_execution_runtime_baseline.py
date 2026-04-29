"""Create execution runtime baseline schema.

Revision ID: 202604290001
Revises:
Create Date: 2026-04-29 00:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202604290001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "runtime_control_state",
        sa.Column("id", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("paused", sa.Boolean(), nullable=False),
        sa.Column("kill_switch_enabled", sa.Boolean(), nullable=False),
        sa.Column("automation_enabled", sa.Boolean(), nullable=False),
        sa.Column("readiness_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runtime_control_state")),
    )
    runtime_control = sa.table(
        "runtime_control_state",
        sa.column("id", sa.String),
        sa.column("mode", sa.String),
        sa.column("paused", sa.Boolean),
        sa.column("kill_switch_enabled", sa.Boolean),
        sa.column("automation_enabled", sa.Boolean),
        sa.column("readiness_reason", sa.Text),
    )
    op.bulk_insert(
        runtime_control,
        [
            {
                "id": "global",
                "mode": "mock",
                "paused": True,
                "kill_switch_enabled": False,
                "automation_enabled": False,
                "readiness_reason": "initialized fail-closed until operator controls are enabled",
            },
        ],
    )

    op.create_table(
        "execution_plans",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("external_plan_id", sa.String(length=128), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_plans")),
        sa.UniqueConstraint("external_plan_id", name=op.f("uq_execution_plans_external_plan_id")),
    )

    op.create_table(
        "execution_plan_items",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("execution_plan_id", sa.String(length=36), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("intent", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("quantity_policy", sa.JSON(), nullable=True),
        sa.Column("risk_context", sa.JSON(), nullable=True),
        sa.Column("decision_snapshot_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_plan_id"],
            ["execution_plans.id"],
            name=op.f("fk_execution_plan_items_execution_plan_id_execution_plans"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_plan_items")),
    )
    op.create_index(op.f("ix_execution_plan_items_execution_plan_id"), "execution_plan_items", ["execution_plan_id"], unique=False)
    op.create_index(op.f("ix_execution_plan_items_symbol"), "execution_plan_items", ["symbol"], unique=False)

    op.create_table(
        "execution_jobs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("execution_plan_item_id", sa.String(length=36), nullable=False),
        sa.Column("job_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_plan_item_id"],
            ["execution_plan_items.id"],
            name=op.f("fk_execution_jobs_execution_plan_item_id_execution_plan_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_jobs")),
    )
    op.create_index(op.f("ix_execution_jobs_execution_plan_item_id"), "execution_jobs", ["execution_plan_item_id"], unique=False)

    op.create_table(
        "broker_orders",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("execution_plan_item_id", sa.String(length=36), nullable=True),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("broker_order_id", sa.String(length=128), nullable=True),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("limit_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_broker_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_broker_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_plan_item_id"],
            ["execution_plan_items.id"],
            name=op.f("fk_broker_orders_execution_plan_item_id_execution_plan_items"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_broker_orders")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_broker_orders_idempotency_key")),
    )
    op.create_index(op.f("ix_broker_orders_broker_order_id"), "broker_orders", ["broker_order_id"], unique=False)
    op.create_index(op.f("ix_broker_orders_execution_plan_item_id"), "broker_orders", ["execution_plan_item_id"], unique=False)
    op.create_index(op.f("ix_broker_orders_symbol"), "broker_orders", ["symbol"], unique=False)

    op.create_table(
        "broker_order_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("broker_order_pk", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("broker_status", sa.String(length=64), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["broker_order_pk"],
            ["broker_orders.id"],
            name=op.f("fk_broker_order_events_broker_order_pk_broker_orders"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_broker_order_events")),
    )
    op.create_index(op.f("ix_broker_order_events_broker_order_pk"), "broker_order_events", ["broker_order_pk"], unique=False)

    op.create_table(
        "execution_fills",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("broker_order_pk", sa.String(length=36), nullable=False),
        sa.Column("broker_fill_id", sa.String(length=128), nullable=True),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("price", sa.Numeric(18, 4), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["broker_order_pk"],
            ["broker_orders.id"],
            name=op.f("fk_execution_fills_broker_order_pk_broker_orders"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_fills")),
    )
    op.create_index(op.f("ix_execution_fills_broker_fill_id"), "execution_fills", ["broker_fill_id"], unique=False)
    op.create_index(op.f("ix_execution_fills_broker_order_pk"), "execution_fills", ["broker_order_pk"], unique=False)
    op.create_index(op.f("ix_execution_fills_symbol"), "execution_fills", ["symbol"], unique=False)

    op.create_table(
        "execution_positions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("average_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("updated_from", sa.String(length=64), nullable=False),
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_positions")),
        sa.UniqueConstraint("symbol", name=op.f("uq_execution_positions_symbol")),
    )

    op.create_table(
        "monitoring_checkpoints",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("worker_name", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_monitoring_checkpoints")),
        sa.UniqueConstraint("worker_name", name=op.f("uq_monitoring_checkpoints_worker_name")),
    )


def downgrade() -> None:
    op.drop_table("monitoring_checkpoints")
    op.drop_table("execution_positions")
    op.drop_index(op.f("ix_execution_fills_symbol"), table_name="execution_fills")
    op.drop_index(op.f("ix_execution_fills_broker_order_pk"), table_name="execution_fills")
    op.drop_index(op.f("ix_execution_fills_broker_fill_id"), table_name="execution_fills")
    op.drop_table("execution_fills")
    op.drop_index(op.f("ix_broker_order_events_broker_order_pk"), table_name="broker_order_events")
    op.drop_table("broker_order_events")
    op.drop_index(op.f("ix_broker_orders_symbol"), table_name="broker_orders")
    op.drop_index(op.f("ix_broker_orders_execution_plan_item_id"), table_name="broker_orders")
    op.drop_index(op.f("ix_broker_orders_broker_order_id"), table_name="broker_orders")
    op.drop_table("broker_orders")
    op.drop_index(op.f("ix_execution_jobs_execution_plan_item_id"), table_name="execution_jobs")
    op.drop_table("execution_jobs")
    op.drop_index(op.f("ix_execution_plan_items_symbol"), table_name="execution_plan_items")
    op.drop_index(op.f("ix_execution_plan_items_execution_plan_id"), table_name="execution_plan_items")
    op.drop_table("execution_plan_items")
    op.drop_table("execution_plans")
    op.drop_table("runtime_control_state")
