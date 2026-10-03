"""Separate watch sizing from active monitoring.

Revision ID: 202610020002
Revises: 202610020001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202610020002"
down_revision: str | None = "202610020001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _exec_agent_sizing_predicate(dialect_name: str) -> str:
    if dialect_name == "postgresql":
        return """
            COALESCE(quantity_policy ->> 'type', '') = 'exec_agent_sized'
            OR COALESCE(quantity_policy ->> 'kind', '') = 'exec_agent_sized'
        """
    return """
        COALESCE(json_extract(quantity_policy, '$.type'), '') = 'exec_agent_sized'
        OR COALESCE(json_extract(quantity_policy, '$.kind'), '') = 'exec_agent_sized'
    """


def upgrade() -> None:
    sizing_predicate = _exec_agent_sizing_predicate(op.get_bind().dialect.name)
    op.execute(
        sa.text(
            f"""
            UPDATE execution_plan_items
            SET status = 'watch_sizing_pending'
            WHERE side = 'buy'
              AND status IN (
                  'watch_monitoring_pending',
                  'watch_monitoring_active',
                  'watch_buy_triggered',
                  'watch_buy_triggered_blocked',
                  'watch_buy_triggered_pending_dispatch',
                  'triggered_pending_agent',
                  'agent_delay',
                  'agent_approved_pending_submit'
              )
              AND ({sizing_predicate})
            """
        )
    )


def downgrade() -> None:
    sizing_predicate = _exec_agent_sizing_predicate(op.get_bind().dialect.name)
    op.execute(
        sa.text(
            f"""
            UPDATE execution_plan_items
            SET status = 'watch_monitoring_pending'
            WHERE side = 'buy'
              AND status = 'watch_sizing_pending'
              AND ({sizing_predicate})
            """
        )
    )
