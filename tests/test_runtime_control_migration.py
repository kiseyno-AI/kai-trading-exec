from __future__ import annotations

from datetime import UTC, datetime
import importlib.util
from pathlib import Path
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text


TRANSITION_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609060004_runtime_control_transition_not_null.py"
)
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202610020001_drop_runtime_control_automation_enabled.py"
)
WATCH_SIZING_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202610020002_watch_sizing_pending_status.py"
)


def _migration(path: Path = MIGRATION_PATH):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_control_transition_migration_backfills_before_not_null() -> None:
    migration = _migration(TRANSITION_MIGRATION_PATH)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    controls = sa.Table(
        "runtime_control_state",
        metadata,
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_transition_action", sa.String(32), nullable=True),
        sa.Column("last_transition_source", sa.String(64), nullable=True),
        sa.Column("last_transition_at", sa.DateTime(timezone=True), nullable=True),
    )
    metadata.create_all(engine)
    recorded_at = datetime(2026, 9, 6, 18, 0, tzinfo=UTC)

    with engine.begin() as connection:
        connection.execute(
            controls.insert().values(
                id="global",
                created_at=recorded_at,
                updated_at=recorded_at,
                last_transition_action=None,
                last_transition_source=None,
                last_transition_at=None,
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    nullable_by_name = {
        column["name"]: column["nullable"]
        for column in inspect(engine).get_columns("runtime_control_state")
    }
    assert nullable_by_name["last_transition_action"] is False
    assert nullable_by_name["last_transition_source"] is False
    assert nullable_by_name["last_transition_at"] is False

    reflected = sa.Table(
        "runtime_control_state",
        sa.MetaData(),
        autoload_with=engine,
    )
    with engine.connect() as connection:
        row = connection.execute(sa.select(reflected)).mappings().one()
    assert row["last_transition_action"] == "bootstrap"
    assert row["last_transition_source"] == "system"
    assert row["last_transition_at"] is not None
    engine.dispose()


def test_runtime_control_automation_enabled_migration_upgrade_and_downgrade(
    monkeypatch,
) -> None:
    migration = _migration()
    engine = create_engine("sqlite+pysqlite:///:memory:")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE runtime_control_state (
                    id VARCHAR(32) PRIMARY KEY NOT NULL,
                    paused BOOLEAN NOT NULL,
                    kill_switch_enabled BOOLEAN NOT NULL,
                    automation_enabled BOOLEAN NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO runtime_control_state (
                    id,
                    paused,
                    kill_switch_enabled,
                    automation_enabled
                ) VALUES
                    ('global', 1, 0, 0),
                    ('resumed', 0, 0, 1),
                    ('killed', 1, 1, 0)
                """
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration, "op", operations)

        migration.upgrade()

        assert "automation_enabled" not in {
            column["name"]
            for column in inspect(connection).get_columns("runtime_control_state")
        }
        assert connection.scalar(
            text("SELECT paused FROM runtime_control_state WHERE id = 'global'")
        ) == 1

        migration.downgrade()

        columns = {
            column["name"]: column
            for column in inspect(connection).get_columns("runtime_control_state")
        }
        assert columns["automation_enabled"]["nullable"] is False
        restored = connection.execute(
            text(
                "SELECT id, automation_enabled FROM runtime_control_state "
                "ORDER BY id"
            )
        ).all()
        assert restored == [("global", 0), ("killed", 0), ("resumed", 1)]

    engine.dispose()


def test_watch_sizing_status_migration_only_reclassifies_unsized_watches(
    monkeypatch,
) -> None:
    migration = _migration(WATCH_SIZING_MIGRATION_PATH)
    engine = create_engine("sqlite+pysqlite:///:memory:")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE execution_plan_items (
                    id VARCHAR(32) PRIMARY KEY NOT NULL,
                    side VARCHAR(8) NOT NULL,
                    status VARCHAR(64) NOT NULL,
                    decision_agent_name VARCHAR(64),
                    quantity_policy JSON
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO execution_plan_items (
                    id,
                    side,
                    status,
                    decision_agent_name,
                    quantity_policy
                )
                VALUES (
                    :id,
                    :side,
                    :status,
                    :decision_agent_name,
                    :quantity_policy
                )
                """
            ),
            [
                {
                    "id": "legacy-pending",
                    "side": "buy",
                    "status": "watch_monitoring_pending",
                    "decision_agent_name": "signal-decider",
                    "quantity_policy": '{"type":"exec_agent_sized"}',
                },
                {
                    "id": "legacy-active",
                    "side": "buy",
                    "status": "watch_monitoring_active",
                    "decision_agent_name": "short-term-decider",
                    "quantity_policy": '{"kind":"exec_agent_sized"}',
                },
                {
                    "id": "legacy-trigger-blocked",
                    "side": "buy",
                    "status": "watch_buy_triggered_blocked",
                    "decision_agent_name": None,
                    "quantity_policy": '{"type":"exec_agent_sized"}',
                },
                {
                    "id": "sized-watch",
                    "side": "buy",
                    "status": "watch_monitoring_active",
                    "decision_agent_name": "signal-decider",
                    "quantity_policy": '{"kind":"shares","quantity":2}',
                },
                {
                    "id": "sell-monitor",
                    "side": "sell",
                    "status": "target_sell_monitoring",
                    "decision_agent_name": None,
                    "quantity_policy": '{"type":"position_exit_from_entry"}',
                },
                {
                    "id": "trigger-time-sized",
                    "side": "buy",
                    "status": "watch_monitoring_active",
                    "decision_agent_name": None,
                    "quantity_policy": '{"type":"trigger_time_exec_agent_sized"}',
                },
            ],
        )
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration, "op", operations)

        migration.upgrade()
        statuses = dict(
            connection.execute(
                text("SELECT id, status FROM execution_plan_items ORDER BY id")
            ).all()
        )
        assert statuses == {
            "legacy-active": "watch_sizing_pending",
            "legacy-pending": "watch_sizing_pending",
            "legacy-trigger-blocked": "watch_sizing_pending",
            "sell-monitor": "target_sell_monitoring",
            "sized-watch": "watch_monitoring_active",
            "trigger-time-sized": "watch_monitoring_active",
        }

        migration.downgrade()
        restored = dict(
            connection.execute(
                text("SELECT id, status FROM execution_plan_items ORDER BY id")
            ).all()
        )
        assert restored["legacy-active"] == "watch_monitoring_pending"
        assert restored["legacy-pending"] == "watch_monitoring_pending"
        assert restored["legacy-trigger-blocked"] == "watch_monitoring_pending"
        assert restored["sized-watch"] == "watch_monitoring_active"
        assert restored["trigger-time-sized"] == "watch_monitoring_active"

    engine.dispose()
