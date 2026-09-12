import importlib.util
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609080002_execution_job_uniqueness.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _legacy_execution_jobs(engine: sa.Engine) -> sa.Table:
    metadata = sa.MetaData()
    jobs = sa.Table(
        "execution_jobs",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("execution_plan_item_id", sa.String(36), nullable=False),
        sa.Column("job_type", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("last_error", sa.Text()),
        sa.Column("created_at", sa.DateTime()),
    )
    metadata.create_all(engine)
    return jobs


def _legacy_broker_orders(engine: sa.Engine) -> sa.Table:
    metadata = sa.MetaData()
    orders = sa.Table(
        "broker_orders",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("execution_plan_item_id", sa.String(36)),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
    )
    metadata.create_all(engine)
    return orders


def _legacy_execution_failures(engine: sa.Engine) -> sa.Table:
    metadata = sa.MetaData()
    failures = sa.Table(
        "execution_failures",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("retry_job_id", sa.String(36)),
    )
    metadata.create_all(engine)
    return failures


def test_execution_job_uniqueness_migration_quarantines_legacy_duplicates() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    jobs = _legacy_execution_jobs(engine)
    with engine.begin() as connection:
        connection.execute(
            jobs.insert(),
            [
                {
                    "id": "job-1",
                    "execution_plan_item_id": "item-1",
                    "job_type": "next_open_buy",
                    "status": "scheduled",
                },
                {
                    "id": "job-2",
                    "execution_plan_item_id": "item-1",
                    "job_type": "next_open_buy",
                    "status": "scheduled",
                },
                {
                    "id": "job-3",
                    "execution_plan_item_id": "item-1",
                    "job_type": "broker_order_retry",
                    "status": "scheduled",
                },
            ],
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    reflected = sa.Table("execution_jobs", sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(reflected).order_by(reflected.c.id)
        ).mappings().all()
    next_open_rows = [row for row in rows if row["job_type"] == "next_open_buy"]
    quarantined_rows = [
        row for row in rows if row["job_type"].startswith("quarantined_duplicate:")
    ]
    assert len(next_open_rows) == 1
    assert len(quarantined_rows) == 1
    assert quarantined_rows[0]["status"] == "failed"
    assert "duplicate execution job" in quarantined_rows[0]["last_error"]
    assert {
        tuple(constraint["column_names"])
        for constraint in sa.inspect(engine).get_unique_constraints("execution_jobs")
    } == {("execution_plan_item_id", "job_type")}

    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                reflected.insert().values(
                    id="job-4",
                    execution_plan_item_id="item-1",
                    job_type="next_open_buy",
                    status="scheduled",
                )
            )
    engine.dispose()


def test_execution_job_uniqueness_migration_downgrade_keeps_quarantine_terminal() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    jobs = _legacy_execution_jobs(engine)
    with engine.begin() as connection:
        connection.execute(
            jobs.insert(),
            [
                {
                    "id": "job-a",
                    "execution_plan_item_id": "item-a",
                    "job_type": "next_open_buy",
                    "status": "scheduled",
                },
                {
                    "id": "job-b",
                    "execution_plan_item_id": "item-a",
                    "job_type": "next_open_buy",
                    "status": "scheduled",
                },
            ],
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
            migration.downgrade()

    assert sa.inspect(engine).get_unique_constraints("execution_jobs") == []
    reflected = sa.Table("execution_jobs", sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        quarantined = connection.execute(
            sa.select(reflected).where(
                reflected.c.job_type.like("quarantined_duplicate:%")
            )
        ).mappings().one()
    assert quarantined["status"] == "failed"
    engine.dispose()


def test_migration_survivor_priority_is_order_link_then_active_then_oldest_id() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    jobs = _legacy_execution_jobs(engine)
    orders = _legacy_broker_orders(engine)
    with engine.begin() as connection:
        connection.execute(
            jobs.insert(),
            [
                # An exact submitted-order intent is authoritative even when inactive/newer.
                {"id": "linked-new", "execution_plan_item_id": "linked", "job_type": "next_open_buy", "status": "failed", "created_at": datetime(2026, 1, 2)},
                {"id": "active-old", "execution_plan_item_id": "linked", "job_type": "next_open_buy", "status": "running", "created_at": datetime(2026, 1, 1)},
                # Without an order intent, active wins and oldest active survives.
                {"id": "active-a", "execution_plan_item_id": "active", "job_type": "next_open_buy", "status": "scheduled", "created_at": datetime(2026, 1, 1)},
                {"id": "active-b", "execution_plan_item_id": "active", "job_type": "next_open_buy", "status": "running", "created_at": datetime(2026, 1, 2)},
                {"id": "inactive-old", "execution_plan_item_id": "active", "job_type": "next_open_buy", "status": "failed", "created_at": datetime(2025, 1, 1)},
                # Missing historical timestamps sort conservatively first, then smallest id.
                {"id": "null-a", "execution_plan_item_id": "nulls", "job_type": "next_open_buy", "status": "failed", "created_at": None},
                {"id": "null-b", "execution_plan_item_id": "nulls", "job_type": "next_open_buy", "status": "failed", "created_at": None},
                {"id": "dated", "execution_plan_item_id": "nulls", "job_type": "next_open_buy", "status": "failed", "created_at": datetime(2020, 1, 1)},
                # Retry orders use a materialized retry:<root>:<job> key.
                {"id": "retry-linked", "execution_plan_item_id": "retry", "job_type": "broker_order_retry", "status": "failed", "created_at": datetime(2026, 1, 2)},
                {"id": "retry-active", "execution_plan_item_id": "retry", "job_type": "broker_order_retry", "status": "running", "created_at": datetime(2026, 1, 1)},
            ],
        )
        connection.execute(
            orders.insert().values(
                id="order-linked",
                execution_plan_item_id="linked",
                idempotency_key="execution-job:linked-new",
            )
        )
        connection.execute(
            orders.insert().values(
                id="order-retry-linked",
                execution_plan_item_id="retry",
                idempotency_key="retry:root-order:retry-linked",
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    reflected = sa.Table("execution_jobs", sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        survivors = connection.execute(
            sa.select(reflected.c.execution_plan_item_id, reflected.c.id).where(
                reflected.c.job_type.in_(("next_open_buy", "broker_order_retry"))
            )
        ).all()
    assert dict(survivors) == {
        "linked": "linked-new",
        "active": "active-a",
        "nulls": "null-a",
        "retry": "retry-linked",
    }
    engine.dispose()


def test_retry_failure_link_survives_duplicate_quarantine_before_order_exists() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    jobs = _legacy_execution_jobs(engine)
    failures = _legacy_execution_failures(engine)
    with engine.begin() as connection:
        connection.execute(
            jobs.insert(),
            [
                {
                    "id": "retry-orphan-old",
                    "execution_plan_item_id": "item-retry-linked",
                    "job_type": "broker_order_retry",
                    "status": "scheduled",
                    "created_at": datetime(2026, 1, 1),
                },
                {
                    "id": "retry-failure-linked-new",
                    "execution_plan_item_id": "item-retry-linked",
                    "job_type": "broker_order_retry",
                    "status": "scheduled",
                    "created_at": datetime(2026, 1, 2),
                },
            ],
        )
        connection.execute(
            failures.insert().values(
                id="failure-1",
                retry_job_id="retry-failure-linked-new",
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    reflected = sa.Table("execution_jobs", sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        surviving = connection.execute(
            sa.select(reflected.c.id).where(
                reflected.c.job_type == "broker_order_retry"
            )
        ).scalar_one()
        quarantined = connection.execute(
            sa.select(reflected.c.id).where(
                reflected.c.job_type.like("quarantined_duplicate:%")
            )
        ).scalar_one()

    assert surviving == "retry-failure-linked-new"
    assert quarantined == "retry-orphan-old"
    engine.dispose()


def test_postgresql_upgrade_locks_table_before_repair() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    batch = MagicMock()
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind
    migration_op.batch_alter_table.return_value.__enter__.return_value = batch

    events: list[str] = []
    bind.execute.side_effect = lambda *_args, **_kwargs: events.append("lock")
    with (
        patch.object(migration, "op", migration_op),
        patch.object(
            migration,
            "_quarantine_legacy_duplicates",
            side_effect=lambda _bind: events.append("repair"),
        ) as quarantine,
    ):
        migration.upgrade()

    lock_sql = str(bind.execute.call_args_list[0].args[0])
    assert lock_sql == "LOCK TABLE execution_jobs IN ACCESS EXCLUSIVE MODE"
    assert events == ["lock", "repair"]
    quarantine.assert_called_once_with(bind)
    batch.create_unique_constraint.assert_called_once_with(
        migration.UNIQUE_CONSTRAINT_NAME,
        ["execution_plan_item_id", "job_type"],
    )
