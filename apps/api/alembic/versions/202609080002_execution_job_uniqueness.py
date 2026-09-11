"""Enforce one execution job type per plan item.

Revision ID: 202609080002
Revises: 202609080001
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202609080002"
down_revision: str | None = "202609080001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UNIQUE_CONSTRAINT_NAME = "uq_execution_jobs_plan_item_job_type"
QUARANTINED_JOB_TYPE_PREFIX = "quarantined_duplicate:"
ACTIVE_JOB_STATUSES = frozenset({"queued", "scheduled", "retrying", "running"})


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # Prevent a writer from inserting another duplicate between repair and
        # creation of the durable uniqueness invariant.
        bind.execute(sa.text("LOCK TABLE execution_jobs IN ACCESS EXCLUSIVE MODE"))
    _quarantine_legacy_duplicates(bind)
    with op.batch_alter_table("execution_jobs") as batch_op:
        batch_op.create_unique_constraint(
            UNIQUE_CONSTRAINT_NAME,
            ["execution_plan_item_id", "job_type"],
        )


def downgrade() -> None:
    with op.batch_alter_table("execution_jobs") as batch_op:
        batch_op.drop_constraint(UNIQUE_CONSTRAINT_NAME, type_="unique")
    # Quarantined duplicates stay terminal. A schema rollback must not revive
    # an extra automated order candidate.


def _quarantine_legacy_duplicates(bind: sa.Connection) -> None:
    duplicate_keys = bind.execute(
        sa.text(
            "SELECT execution_plan_item_id, job_type "
            "FROM execution_jobs "
            "GROUP BY execution_plan_item_id, job_type "
            "HAVING COUNT(*) > 1"
        )
    ).mappings().all()
    inspector = sa.inspect(bind)
    has_broker_orders = inspector.has_table("broker_orders")
    has_retry_failure_links = inspector.has_table("execution_failures") and any(
        column["name"] == "retry_job_id"
        for column in inspector.get_columns("execution_failures")
    )
    for duplicate_key in duplicate_keys:
        plan_item_id = duplicate_key["execution_plan_item_id"]
        job_type = duplicate_key["job_type"]
        jobs = bind.execute(
            sa.text(
                "SELECT id, status, created_at FROM execution_jobs "
                "WHERE execution_plan_item_id = :plan_item_id "
                "AND job_type = :job_type"
            ),
            {"plan_item_id": plan_item_id, "job_type": job_type},
        ).mappings().all()
        canonical = min(
            jobs,
            key=lambda job: _canonical_job_priority(
                bind=bind,
                job=job,
                plan_item_id=plan_item_id,
                job_type=job_type,
                has_broker_orders=has_broker_orders,
                has_retry_failure_links=has_retry_failure_links,
            ),
        )
        for job in jobs:
            if job["id"] == canonical["id"]:
                continue
            bind.execute(
                sa.text(
                    "UPDATE execution_jobs "
                    "SET job_type = :quarantined_job_type, "
                    "status = 'failed', "
                    "last_error = :last_error "
                    "WHERE id = :job_id"
                ),
                {
                    "job_id": job["id"],
                    "quarantined_job_type": (
                        f"{QUARANTINED_JOB_TYPE_PREFIX}{job['id']}"
                    ),
                    "last_error": (
                        "duplicate execution job quarantined before uniqueness "
                        "constraint creation"
                    ),
                },
            )


def _canonical_job_priority(
    *,
    bind: sa.Connection,
    job: sa.RowMapping,
    plan_item_id: str,
    job_type: str,
    has_broker_orders: bool,
    has_retry_failure_links: bool,
) -> tuple[int, int, int, int, str, str]:
    has_order_intent = False
    if has_broker_orders:
        has_order_intent = bind.execute(
            sa.text(
                "SELECT 1 FROM broker_orders "
                "WHERE execution_plan_item_id = :plan_item_id "
                "AND (idempotency_key = :idempotency_key "
                "OR (:is_retry_job = 1 "
                "AND idempotency_key LIKE :retry_pattern)) LIMIT 1"
            ),
            {
                "plan_item_id": plan_item_id,
                "idempotency_key": f"execution-job:{job['id']}",
                "is_retry_job": int(job_type == "broker_order_retry"),
                "retry_pattern": f"retry:%:{job['id']}",
            },
        ).first() is not None
    has_failure_link = False
    if has_retry_failure_links and job_type == "broker_order_retry":
        has_failure_link = bind.execute(
            sa.text(
                "SELECT 1 FROM execution_failures "
                "WHERE retry_job_id = :job_id LIMIT 1"
            ),
            {"job_id": job["id"]},
        ).first() is not None
    return (
        0 if has_order_intent else 1,
        0 if has_failure_link else 1,
        0 if job["status"] in ACTIVE_JOB_STATUSES else 1,
        # A missing legacy timestamp cannot prove recency. Preserve it
        # conservatively, then keep the oldest otherwise-equivalent intent.
        0 if job["created_at"] is None else 1,
        str(job["created_at"] or ""),
        str(job["id"]),
    )
