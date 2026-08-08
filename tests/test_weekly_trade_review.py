from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import event, inspect, text

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    MonitoringCheckpoint,
)
from app.services.weekly_trade_review import generate_weekly_trade_review


def _build_db(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def test_weekly_trade_review_exports_bounded_read_only_artifact(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)
    _seed_weekly_review_fixture(db=db, start_at=start_at)

    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany) -> None:  # type: ignore[no-untyped-def]
        statements.append(statement)

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=10,
            max_weekly_orders=10,
            max_execution_failures=10,
            max_linked_events=10,
        )
    finally:
        event.remove(engine, "before_cursor_execute", _capture_sql)

    assert report["counts"]["fill_count"] == 1
    assert report["counts"]["linked_broker_order_count"] == 1
    assert report["counts"]["linked_broker_order_event_count"] == 1
    assert report["counts"]["execution_failure_count"] == 1
    assert report["truncated"] == {
        "execution_failures": False,
        "fills": False,
        "linked_broker_order_events": False,
        "weekly_orders": False,
    }
    assert report["fills"][0]["symbol"] == "005930"
    assert report["linked_broker_orders"][0]["id"] == "weekly-order"
    assert report["weekly_order_status_counts"] == [
        {
            "count": 1,
            "first_submitted_at": "2026-07-26T01:30:00+00:00",
            "last_submitted_at": "2026-07-26T01:30:00+00:00",
            "side": "buy",
            "status": "filled",
        }
    ]
    active_order_summary = report["active_broker_order_summary"]
    assert active_order_summary["missing_submitted_at_nonterminal_count"] == 1
    assert {
        row["status"]: row["count"]
        for row in active_order_summary["nonterminal_status_counts"]
    } == {
        "cancel_pending": 1,
        "open": 1,
        "partially_filled": 1,
        "retry_pending": 1,
        "submission_pending": 1,
        "submitted": 2,
    }
    assert {
        row["status"]
        for row in active_order_summary["stale_nonterminal_samples"]
    } == {
        "cancel_pending",
        "open",
        "partially_filled",
        "retry_pending",
        "submission_pending",
        "submitted",
    }
    assert report["execution_plan_item_summary"]["active_watch_status_counts"][0]["status"] == "watch_monitoring_active"
    assert report["monitoring_checkpoints"][0]["worker_name"] == "monitoring"

    assert "payload" not in report["fills"][0]
    assert "raw_broker_payload" not in report["linked_broker_orders"][0]
    assert "request_payload" not in report["linked_broker_order_events"][0]
    assert "raw_context" not in report["execution_failures"][0]

    mutating_statements = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
    ]
    assert mutating_statements == []
    selected_sql = "\n".join(statements)
    assert "raw_broker_payload" not in selected_sql
    assert "request_metadata" not in selected_sql
    assert "response_metadata" not in selected_sql
    assert "raw_context" not in selected_sql

    artifact_path = tmp_path / "2026-07-25_to_2026-08-01_weekly_trade_review.json"
    latest_path = tmp_path / "latest.json"
    assert artifact_path.exists()
    assert latest_path.exists()
    assert json.loads(artifact_path.read_text(encoding="utf-8"))["counts"]["fill_count"] == 1
    assert json.loads(latest_path.read_text(encoding="utf-8"))["artifact_path"] == str(artifact_path)

    db.close()
    engine.dispose()


def test_weekly_trade_review_cli_fails_before_artifact_when_required_mode_mismatches(tmp_path) -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("KAI_EXEC_")
    }
    env["KAI_EXEC_RUNTIME_MODE"] = "paper"
    env["KAI_EXEC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'unexpected.db'}"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/export_weekly_trade_review.py",
            "--require-runtime-mode",
            "live",
            "--start-at",
            "2026-07-25T00:00:00Z",
            "--end-at",
            "2026-08-01T00:00:00Z",
            "--output-dir",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert "resolved runtime_mode='paper' does not match --require-runtime-mode 'live'" in result.stderr
    assert not (tmp_path / "2026-07-25_to_2026-08-01_weekly_trade_review.json").exists()
    assert not (tmp_path / "latest.json").exists()


def test_weekly_trade_review_queries_have_sqlite_index_paths() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _build_db(settings)
    inspector = inspect(engine)

    assert _index_columns(inspector, "execution_fills")["ix_execution_fills_filled_at_broker_order_pk"] == [
        "filled_at",
        "broker_order_pk",
    ]
    assert _index_columns(inspector, "broker_orders")["ix_broker_orders_mode_submitted_at"] == [
        "mode",
        "submitted_at",
    ]
    assert _index_columns(inspector, "broker_orders")["ix_broker_orders_mode_status_submitted_at"] == [
        "mode",
        "status",
        "submitted_at",
    ]
    assert _index_columns(inspector, "execution_plan_items")["ix_execution_plan_items_status_updated_at"] == [
        "status",
        "updated_at",
    ]
    assert _index_columns(inspector, "execution_plan_items")["ix_execution_plan_items_created_at"] == [
        "created_at",
    ]
    assert _index_columns(inspector, "execution_failures")["ix_execution_failures_mode_latest_failed_at"] == [
        "mode",
        "latest_failed_at",
    ]

    start_at = "2026-07-25T00:00:00+00:00"
    end_at = "2026-08-01T00:00:00+00:00"
    assert "ix_execution_fills_filled_at_broker_order_pk" in _query_plan(
        db,
        """
        SELECT id FROM execution_fills
        WHERE filled_at >= :start_at AND filled_at < :end_at
        ORDER BY filled_at
        """,
        {"start_at": start_at, "end_at": end_at},
    )
    assert "ix_broker_orders_mode_submitted_at" in _query_plan(
        db,
        """
        SELECT id FROM broker_orders
        WHERE mode = :mode AND submitted_at >= :start_at AND submitted_at < :end_at
        ORDER BY submitted_at
        """,
        {"mode": "paper", "start_at": start_at, "end_at": end_at},
    )
    assert "ix_broker_orders_mode_status_submitted_at" in _query_plan(
        db,
        """
        SELECT count(id) FROM broker_orders
        WHERE mode = :mode AND status = 'submitted' AND submitted_at < :end_at
        """,
        {"mode": "paper", "end_at": end_at},
    )
    assert "ix_execution_plan_items_status_updated_at" in _query_plan(
        db,
        """
        SELECT id FROM execution_plan_items
        WHERE status = 'watch_monitoring_active' AND updated_at >= :start_at
        ORDER BY updated_at
        """,
        {"start_at": start_at},
    )
    assert "ix_execution_plan_items_created_at" in _query_plan(
        db,
        """
        SELECT id FROM execution_plan_items
        WHERE created_at >= :start_at AND created_at < :end_at
        ORDER BY created_at
        """,
        {"start_at": start_at, "end_at": end_at},
    )
    assert "ix_execution_failures_mode_latest_failed_at" in _query_plan(
        db,
        """
        SELECT id FROM execution_failures
        WHERE mode = :mode AND latest_failed_at >= :start_at AND latest_failed_at < :end_at
        ORDER BY latest_failed_at
        """,
        {"mode": "paper", "start_at": start_at, "end_at": end_at},
    )

    db.close()
    engine.dispose()


def _seed_weekly_review_fixture(*, db, start_at: datetime) -> None:
    plan = ExecutionPlan(
        id="plan-week",
        external_plan_id="plan-week",
        source="test",
        mode="paper",
        status="accepted",
        accepted_at=start_at + timedelta(hours=1),
        created_at=start_at + timedelta(hours=1),
        updated_at=start_at + timedelta(hours=1),
    )
    weekly_item = ExecutionPlanItem(
        id="weekly-item",
        execution_plan_id="plan-week",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        quantity_policy={"type": "fixed", "quantity": 1},
        created_at=start_at + timedelta(hours=1),
        updated_at=start_at + timedelta(days=1),
    )
    old_item = ExecutionPlanItem(
        id="old-item",
        execution_plan_id="plan-week",
        plan_item_index=1,
        symbol="000660",
        side="buy",
        intent="next_open_buy",
        status="filled",
        quantity_policy={"type": "fixed", "quantity": 1},
        created_at=start_at - timedelta(days=10),
        updated_at=start_at - timedelta(days=10),
    )
    weekly_order = BrokerOrder(
        id="weekly-order",
        execution_plan_item_id="weekly-item",
        broker_name="kis",
        mode="paper",
        idempotency_key="weekly-order-key",
        broker_order_id="kis-weekly-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        limit_price=Decimal("70000"),
        submitted_at=start_at + timedelta(days=1, hours=1, minutes=30),
        request_metadata={"large": "metadata"},
        response_metadata={"large": "metadata"},
        raw_broker_payload={"large": "payload"},
        created_at=start_at + timedelta(days=1, hours=1),
        updated_at=start_at + timedelta(days=1, hours=1),
    )
    old_order = BrokerOrder(
        id="old-order",
        execution_plan_item_id="old-item",
        broker_name="kis",
        mode="paper",
        idempotency_key="old-order-key",
        broker_order_id="kis-old-order",
        symbol="000660",
        side="buy",
        status="filled",
        quantity=1,
        limit_price=Decimal("100000"),
        submitted_at=start_at - timedelta(days=8),
        raw_broker_payload={"large": "old"},
        created_at=start_at - timedelta(days=8),
        updated_at=start_at - timedelta(days=8),
    )
    missing_submitted_order = BrokerOrder(
        id="missing-submitted-order",
        execution_plan_item_id="weekly-item",
        broker_name="kis",
        mode="paper",
        idempotency_key="missing-submitted-key",
        broker_order_id=None,
        symbol="005930",
        side="buy",
        status="submitted",
        quantity=1,
        created_at=start_at + timedelta(days=2),
        updated_at=start_at + timedelta(days=2),
    )
    active_nonterminal_orders = [
        BrokerOrder(
            id=f"stale-{status.replace('_', '-')}-order",
            execution_plan_item_id="weekly-item",
            broker_name="kis",
            mode="paper",
            idempotency_key=f"stale-{status.replace('_', '-')}-key",
            broker_order_id=f"kis-stale-{status.replace('_', '-')}",
            symbol=f"ACTIVE{index}",
            side="buy",
            status=status,
            quantity=1,
            limit_price=Decimal("70000"),
            submitted_at=start_at - timedelta(days=1) + timedelta(minutes=index),
            created_at=start_at - timedelta(days=1) + timedelta(minutes=index),
            updated_at=start_at - timedelta(days=1) + timedelta(minutes=index),
        )
        for index, status in enumerate(
            [
                "submission_pending",
                "submitted",
                "open",
                "partially_filled",
                "cancel_pending",
                "retry_pending",
            ]
        )
    ]
    weekly_fill = ExecutionFill(
        id="weekly-fill",
        broker_order_pk="weekly-order",
        broker_fill_id="fill-1",
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("70000"),
        filled_at=start_at + timedelta(days=1, hours=1, minutes=35),
        payload={"large": "fill-payload"},
        created_at=start_at + timedelta(days=1, hours=1, minutes=35),
        updated_at=start_at + timedelta(days=1, hours=1, minutes=35),
    )
    old_fill = ExecutionFill(
        id="old-fill",
        broker_order_pk="old-order",
        broker_fill_id="fill-old",
        symbol="000660",
        side="buy",
        quantity=1,
        price=Decimal("100000"),
        filled_at=start_at - timedelta(days=8),
        payload={"large": "old-fill-payload"},
        created_at=start_at - timedelta(days=8),
        updated_at=start_at - timedelta(days=8),
    )
    event = BrokerOrderEvent(
        id="weekly-event",
        broker_order_pk="weekly-order",
        event_type="filled",
        event_source="test",
        normalized_status="filled",
        broker_status="filled",
        observed_at=start_at + timedelta(days=1, hours=1, minutes=35),
        request_payload={"large": "request"},
        response_payload={"large": "response"},
        payload={"large": "event"},
        created_at=start_at + timedelta(days=1, hours=1, minutes=35),
        updated_at=start_at + timedelta(days=1, hours=1, minutes=35),
    )
    failure = ExecutionFailure(
        id="weekly-failure",
        mode="paper",
        broker_order_pk="missing-submitted-order",
        execution_plan_id="plan-week",
        execution_plan_item_id="weekly-item",
        plan_id="plan-week",
        plan_source="test",
        broker_name="kis",
        idempotency_key="missing-submitted-key",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        requested_quantity=1,
        classification="broker_rejected",
        broker_error_message="blocked",
        retryable=False,
        status="open",
        first_failed_at=start_at + timedelta(days=2),
        latest_failed_at=start_at + timedelta(days=2),
        raw_context={"large": "failure"},
        created_at=start_at + timedelta(days=2),
        updated_at=start_at + timedelta(days=2),
    )
    checkpoint = MonitoringCheckpoint(
        id="monitoring-checkpoint",
        worker_name="monitoring",
        mode="paper",
        status="running",
        last_heartbeat_at=start_at + timedelta(days=6),
        last_success_at=start_at + timedelta(days=6),
        details={"large": "checkpoint"},
        created_at=start_at,
        updated_at=start_at + timedelta(days=6),
    )
    db.add_all(
        [
            plan,
            weekly_item,
            old_item,
            weekly_order,
            old_order,
            missing_submitted_order,
            *active_nonterminal_orders,
            weekly_fill,
            old_fill,
            event,
            failure,
            checkpoint,
        ]
    )
    db.commit()


def _index_columns(inspector, table_name: str) -> dict[str, list[str]]:
    return {index["name"]: index["column_names"] for index in inspector.get_indexes(table_name)}


def _query_plan(db, sql: str, params: dict[str, object]) -> str:
    rows = db.execute(text(f"EXPLAIN QUERY PLAN {sql}"), params).all()
    return "\n".join(str(row) for row in rows)
