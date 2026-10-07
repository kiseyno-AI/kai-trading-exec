"""Make Core delta dispatch batches atomically unique.

Revision ID: 202610070001
Revises: 202610020002
"""

from collections.abc import Sequence
import json

from alembic import op
import sqlalchemy as sa


revision: str = "202610070001"
down_revision: str | None = "202610020002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("execution_plans") as batch_op:
        batch_op.add_column(
            sa.Column("dispatch_batch_key", sa.String(length=128), nullable=True)
        )

    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            """
            SELECT id, source, mode, raw_payload
            FROM execution_plans
            ORDER BY created_at ASC, id ASC
            """
        )
    ).mappings()
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        raw_payload = row["raw_payload"]
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                continue
        if not isinstance(raw_payload, dict):
            continue
        metadata = raw_payload.get("metadata")
        if not isinstance(metadata, dict):
            continue
        batch_key = metadata.get("dispatch_batch_key")
        if (
            metadata.get("dispatch_batch_type") != "delta"
            or not isinstance(batch_key, str)
            or not batch_key.strip()
        ):
            continue
        identity = (str(row["source"]), str(row["mode"]), batch_key.strip())
        if identity in seen:
            continue
        seen.add(identity)
        connection.execute(
            sa.text(
                """
                UPDATE execution_plans
                SET dispatch_batch_key = :dispatch_batch_key
                WHERE id = :execution_plan_id
                """
            ),
            {
                "dispatch_batch_key": identity[2],
                "execution_plan_id": row["id"],
            },
        )

    with op.batch_alter_table("execution_plans") as batch_op:
        batch_op.create_unique_constraint(
            "uq_execution_plans_source_mode_dispatch_batch_key",
            ["source", "mode", "dispatch_batch_key"],
        )


def downgrade() -> None:
    with op.batch_alter_table("execution_plans") as batch_op:
        batch_op.drop_constraint(
            "uq_execution_plans_source_mode_dispatch_batch_key",
            type_="unique",
        )
        batch_op.drop_column("dispatch_batch_key")
