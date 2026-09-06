from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

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
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.weekly_trade_review import WeeklyTradeReviewTimeoutError, generate_weekly_trade_review
from app.services.runtime import runtime_counts


REPO_ROOT = Path(__file__).resolve().parents[1]
WEEKLY_REVIEW_SCRIPT = REPO_ROOT / "scripts" / "export_weekly_trade_review.py"


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

    assert report["counts"]["fill_count"] == 2
    assert report["counts"]["linked_broker_order_count"] == 2
    assert report["counts"]["linked_broker_order_event_count"] == 1
    assert report["counts"]["execution_failure_count"] == 1
    assert report["counts"]["closed_trade_count"] == 1
    assert report["counts"]["closed_sell_fill_count"] == 1
    assert report["counts"]["total_sell_rows"] == 1
    assert report["counts"]["pnl_covered_sell_rows"] == 1
    assert report["counts"]["pnl_uncovered_sell_rows"] == 0
    assert report["counts"]["stale_fifo_inventory_alert_count"] == 1
    assert report["counts"]["protective_stop_actionability_warning_count"] == 1
    assert report["truncated"] == {
        "closed_trades": False,
        "execution_failures": False,
        "fills": False,
        "linked_broker_order_events": False,
        "stale_fifo_inventory_alerts": False,
        "weekly_orders": False,
    }
    assert report["fills"][0]["symbol"] == "005930"
    assert report["closed_trades"][0]["symbol"] == "049430"
    assert report["closed_trades"][0]["cost_basis_source"] == "broker_reconciliation"
    assert report["closed_trades"][0]["realized_pnl"] == "-1800.0000"
    assert report["realized_pnl_coverage"]["status"] == "complete"
    assert report["realized_pnl_coverage"]["truncated"] is False
    assert report["realized_pnl_summary"] == {
        "status": "complete",
        "sell_fill_count": 1,
        "covered_sell_fill_count": 1,
        "win_count": 0,
        "loss_count": 1,
        "flat_count": 0,
        "realized_pnl": "-1800.0000",
        "covered_realized_pnl": "-1800.0000",
        "average_return_pct": "-16.6667",
        "median_return_pct": "-16.6667",
        "best_trade": {
            "sell_fill_id": "legacy-sell-fill",
            "sell_broker_order_pk": "legacy-sell-order",
            "symbol": "049430",
            "sell_filled_at": "2026-07-27T01:05:00+00:00",
            "quantity": 6,
            "cost_basis": "10800.0000",
            "realized_pnl": "-1800.0000",
            "return_pct": "-16.6667",
        },
        "worst_trade": {
            "sell_fill_id": "legacy-sell-fill",
            "sell_broker_order_pk": "legacy-sell-order",
            "symbol": "049430",
            "sell_filled_at": "2026-07-27T01:05:00+00:00",
            "quantity": 6,
            "cost_basis": "10800.0000",
            "realized_pnl": "-1800.0000",
            "return_pct": "-16.6667",
        },
    }
    assert report["stale_fifo_inventory_alerts"][0]["symbol"] == "049430"
    assert report["stale_fifo_inventory_alerts"][0]["fifo_open_quantity"] == 39
    assert report["linked_broker_orders"][0]["id"] == "weekly-order"
    assert report["weekly_order_status_counts"] == [
        {
            "count": 1,
            "first_submitted_at": "2026-07-26T01:30:00+00:00",
            "last_submitted_at": "2026-07-26T01:30:00+00:00",
            "side": "buy",
            "status": "filled",
        },
        {
            "count": 1,
            "first_submitted_at": "2026-07-27T01:00:00+00:00",
            "last_submitted_at": "2026-07-27T01:00:00+00:00",
            "side": "sell",
            "status": "filled",
        }
    ]
    active_order_summary = report["active_broker_order_summary"]
    assert active_order_summary["missing_submitted_at_nonterminal_count"] == 1
    assert active_order_summary["actionable_order_count"] == 0
    assert {
        row["status"]: row["count"]
        for row in active_order_summary["nonterminal_status_counts"]
    } == {}
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
    assert report["protective_stop_actionability"] == {
        "affected_symbols": ["459550"],
        "status": "degraded",
        "warning_count": 1,
        "warnings": [
            {
                "block_reason": None,
                "checkpoint_status": "degraded",
                "execution_plan_item_id": "recovery-stop",
                "last_heartbeat_at": "2026-07-31T00:00:00+00:00",
                "message": "no sellable execution position is available for this trigger",
                "observed_price": "1660.0000",
                "outcome": "no_position",
                "reason": "crossed_stop_without_actionable_outcome",
                "status": "stop_sell_monitoring",
                "stop_trigger": "1872.0000",
                "symbol": "459550",
                "worker_name": "monitoring",
            }
        ],
    }

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
    assert "risk_context" not in selected_sql

    artifact_path = tmp_path / "2026-07-25_to_2026-08-01_weekly_trade_review.json"
    latest_path = tmp_path / "latest.json"
    assert artifact_path.exists()
    assert latest_path.exists()
    assert json.loads(artifact_path.read_text(encoding="utf-8"))["counts"]["fill_count"] == 2
    assert json.loads(latest_path.read_text(encoding="utf-8"))["artifact_path"] == str(artifact_path)

    db.close()
    engine.dispose()


def test_weekly_active_order_summary_matches_runtime_actionable_semantics() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _build_db(settings)
    reference_at = datetime.now(UTC)
    recent_at = reference_at - timedelta(minutes=5)
    stale_at = reference_at - timedelta(hours=2)
    db.add_all(
        [
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="weekly-actionable-recent",
                broker_order_id="recent-1",
                symbol="005930",
                side="buy",
                status="submitted",
                quantity=1,
                submitted_at=recent_at,
                last_broker_sync_at=recent_at,
            ),
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="weekly-nonactionable-stale",
                broker_order_id="stale-1",
                symbol="035810",
                side="buy",
                status="open",
                quantity=1,
                submitted_at=stale_at,
                last_broker_sync_at=reference_at,
            ),
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="weekly-nonactionable-attention",
                broker_order_id="attention-1",
                symbol="004690",
                side="buy",
                status="stale_terminal_unknown",
                quantity=1,
                submitted_at=stale_at,
                last_broker_sync_at=reference_at,
            ),
        ]
    )
    db.commit()

    report = generate_weekly_trade_review(
        settings=settings,
        db=db,
        start_at=reference_at - timedelta(days=1),
        end_at=reference_at,
    )
    summary = report["active_broker_order_summary"]

    assert runtime_counts(settings=settings, db=db)["open_orders"] == 1
    assert summary["actionable_order_count"] == 1
    assert summary["actionable_status_counts"] == [
        {
            "status": "submitted",
            "count": 1,
            "oldest_submitted_at": recent_at.isoformat(),
            "newest_submitted_at": recent_at.isoformat(),
        }
    ]
    assert [row["broker_order_id"] for row in summary["stale_nonterminal_samples"]] == [
        "stale-1"
    ]
    engine.dispose()


def test_weekly_trade_review_bounds_historical_fifo_scans(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)

    orders: list[BrokerOrder] = []
    fills: list[ExecutionFill] = []
    for index in range(5):
        order = BrokerOrder(
            id=f"history-order-{index}",
            broker_name="kis",
            mode="paper",
            idempotency_key=f"history-order-key-{index}",
            symbol=f"HIST{index}",
            side="buy",
            status="filled",
            quantity=1,
            submitted_at=start_at - timedelta(days=10 - index),
        )
        orders.append(order)
        fills.append(
            ExecutionFill(
                id=f"history-fill-{index}",
                broker_order_pk=order.id,
                broker_fill_id=f"history-fill-{index}",
                symbol=f"HIST{index}",
                side="buy",
                quantity=1,
                price=Decimal("1000"),
                filled_at=start_at - timedelta(days=10 - index),
            )
        )
    db.add_all(orders)
    db.flush()
    db.add_all(fills)
    db.commit()

    statements: list[str] = []
    progress: list[dict[str, object]] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany) -> None:  # type: ignore[no-untyped-def]
        statements.append(statement)

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=2,
            progress_callback=progress.append,
        )
    finally:
        event.remove(engine, "before_cursor_execute", _capture_sql)
        db.close()
        engine.dispose()

    assert report["truncated"]["closed_trades"] is True
    assert report["realized_pnl_coverage"]["status"] == "truncated"
    assert report["realized_pnl_coverage"]["truncated"] is True
    assert report["truncated"]["stale_fifo_inventory_alerts"] is True
    assert [event["step"] for event in progress if event["step"] in {"weekly_closed_trades", "stale_fifo_inventory_alerts"}] == [
        "weekly_closed_trades",
        "stale_fifo_inventory_alerts",
    ]
    assert all(
        event["truncated"] is True
        for event in progress
        if event["step"] in {"weekly_closed_trades", "stale_fifo_inventory_alerts"}
    )
    history_queries = [
        statement
        for statement in statements
        if "FROM execution_fills" in statement
        and "execution_fills.filled_at <" in statement
        and "execution_fills.filled_at >=" not in statement
    ]
    assert len(history_queries) == 2
    assert all("ORDER BY execution_fills.filled_at DESC" in statement for statement in history_queries)
    assert all("LIMIT" in statement for statement in history_queries)


def test_weekly_trade_review_keeps_later_repurchase_basis_unknown(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    sell_at = start_at + timedelta(days=1)
    buy_at = start_at + timedelta(days=2)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)
    sell_order = BrokerOrder(
        id="unmatched-sell-order",
        broker_name="kis",
        mode="paper",
        idempotency_key="unmatched-sell-order-key",
        symbol="LATER",
        side="sell",
        status="filled",
        quantity=5,
        submitted_at=sell_at,
    )
    buy_order = BrokerOrder(
        id="later-buy-order",
        broker_name="kis",
        mode="paper",
        idempotency_key="later-buy-order-key",
        symbol="LATER",
        side="buy",
        status="filled",
        quantity=3,
        submitted_at=buy_at,
    )
    db.add_all([sell_order, buy_order])
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id="unmatched-sell-fill",
                broker_order_pk=sell_order.id,
                broker_fill_id="unmatched-sell-fill",
                symbol="LATER",
                side="sell",
                quantity=5,
                price=Decimal("1000"),
                filled_at=sell_at,
            ),
            ExecutionFill(
                id="later-buy-fill",
                broker_order_pk=buy_order.id,
                broker_fill_id="later-buy-fill",
                symbol="LATER",
                side="buy",
                quantity=3,
                price=Decimal("700"),
                filled_at=buy_at,
            ),
            ExecutionPosition(
                id="later-current-position",
                mode="paper",
                symbol="LATER",
                quantity=3,
                average_price=Decimal("700"),
                updated_from="reconciliation",
                last_reconciled_at=buy_at + timedelta(minutes=5),
            ),
        ]
    )
    db.commit()

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=10,
        )
    finally:
        db.close()
        engine.dispose()

    assert report["closed_trades"] == [
        {
            "cost_basis_observed_at": None,
            "cost_basis_price": None,
            "cost_basis_source": "unknown",
            "quantity": 5,
            "realized_pnl": None,
            "sell_broker_order_pk": "unmatched-sell-order",
            "sell_fill_id": "unmatched-sell-fill",
            "sell_filled_at": "2026-07-26T00:00:00+00:00",
            "sell_price": "1000.0000",
            "symbol": "LATER",
        }
    ]
    assert report["realized_pnl_coverage"] == {
        "status": "incomplete",
        "truncated": False,
        "total_sell_rows": 1,
        "covered_sell_rows": 0,
        "uncovered_sell_rows": 1,
        "uncovered_gross_proceeds": "5000.0000",
        "symbols_requiring_repair": ["LATER"],
        "uncovered_sell_fill_ids": ["unmatched-sell-fill"],
    }


def test_weekly_trade_review_pnl_coverage_classifies_each_sell_fill_once(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)

    orders = [
        BrokerOrder(
            id="split-buy-order-1",
            broker_name="kis",
            mode="paper",
            idempotency_key="split-buy-order-1-key",
            symbol="SPLIT",
            side="buy",
            status="filled",
            quantity=2,
            submitted_at=start_at + timedelta(hours=1),
        ),
        BrokerOrder(
            id="split-buy-order-2",
            broker_name="kis",
            mode="paper",
            idempotency_key="split-buy-order-2-key",
            symbol="SPLIT",
            side="buy",
            status="filled",
            quantity=3,
            submitted_at=start_at + timedelta(hours=2),
        ),
        BrokerOrder(
            id="split-sell-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="split-sell-order-key",
            symbol="SPLIT",
            side="sell",
            status="filled",
            quantity=5,
            submitted_at=start_at + timedelta(hours=3),
        ),
        BrokerOrder(
            id="partial-buy-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="partial-buy-order-key",
            symbol="PART",
            side="buy",
            status="filled",
            quantity=2,
            submitted_at=start_at + timedelta(hours=4),
        ),
        BrokerOrder(
            id="partial-sell-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="partial-sell-order-key",
            symbol="PART",
            side="sell",
            status="filled",
            quantity=5,
            submitted_at=start_at + timedelta(hours=5),
        ),
    ]
    db.add_all(orders)
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id="split-buy-fill-1",
                broker_order_pk="split-buy-order-1",
                broker_fill_id="split-buy-fill-1",
                symbol="SPLIT",
                side="buy",
                quantity=2,
                price=Decimal("100"),
                filled_at=start_at + timedelta(hours=1),
            ),
            ExecutionFill(
                id="split-buy-fill-2",
                broker_order_pk="split-buy-order-2",
                broker_fill_id="split-buy-fill-2",
                symbol="SPLIT",
                side="buy",
                quantity=3,
                price=Decimal("110"),
                filled_at=start_at + timedelta(hours=2),
            ),
            ExecutionFill(
                id="split-sell-fill",
                broker_order_pk="split-sell-order",
                broker_fill_id="split-sell-fill",
                symbol="SPLIT",
                side="sell",
                quantity=5,
                price=Decimal("150"),
                filled_at=start_at + timedelta(hours=3),
            ),
            ExecutionFill(
                id="partial-buy-fill",
                broker_order_pk="partial-buy-order",
                broker_fill_id="partial-buy-fill",
                symbol="PART",
                side="buy",
                quantity=2,
                price=Decimal("200"),
                filled_at=start_at + timedelta(hours=4),
            ),
            ExecutionFill(
                id="partial-sell-fill",
                broker_order_pk="partial-sell-order",
                broker_fill_id="partial-sell-fill",
                symbol="PART",
                side="sell",
                quantity=5,
                price=Decimal("250"),
                filled_at=start_at + timedelta(hours=5),
            ),
        ]
    )
    db.commit()

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=10,
        )
    finally:
        db.close()
        engine.dispose()

    assert report["counts"]["closed_trade_count"] == 4
    assert report["counts"]["closed_sell_fill_count"] == 2
    assert report["counts"]["total_sell_rows"] == 2
    assert report["counts"]["pnl_covered_sell_rows"] == 1
    assert report["counts"]["pnl_uncovered_sell_rows"] == 1
    assert report["realized_pnl_coverage"] == {
        "status": "incomplete",
        "truncated": False,
        "total_sell_rows": 2,
        "covered_sell_rows": 1,
        "uncovered_sell_rows": 1,
        "uncovered_gross_proceeds": "750.0000",
        "symbols_requiring_repair": ["PART"],
        "uncovered_sell_fill_ids": ["partial-sell-fill"],
    }
    assert report["realized_pnl_summary"] == {
        "status": "incomplete",
        "sell_fill_count": 2,
        "covered_sell_fill_count": 1,
        "win_count": 1,
        "loss_count": 0,
        "flat_count": 0,
        "realized_pnl": None,
        "covered_realized_pnl": "220.0000",
        "average_return_pct": "41.5094",
        "median_return_pct": "41.5094",
        "best_trade": {
            "sell_fill_id": "split-sell-fill",
            "sell_broker_order_pk": "split-sell-order",
            "symbol": "SPLIT",
            "sell_filled_at": "2026-07-25T03:00:00+00:00",
            "quantity": 5,
            "cost_basis": "530.0000",
            "realized_pnl": "220.0000",
            "return_pct": "41.5094",
        },
        "worst_trade": {
            "sell_fill_id": "split-sell-fill",
            "sell_broker_order_pk": "split-sell-order",
            "symbol": "SPLIT",
            "sell_filled_at": "2026-07-25T03:00:00+00:00",
            "quantity": 5,
            "cost_basis": "530.0000",
            "realized_pnl": "220.0000",
            "return_pct": "41.5094",
        },
    }


def test_weekly_trade_review_realized_summary_dedupes_duplicate_broker_sell_fills(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)

    db.add_all(
        [
            BrokerOrder(
                id="dedupe-broker-buy-order",
                broker_name="kis",
                mode="paper",
                idempotency_key="dedupe-broker-buy-order-key",
                symbol="DEDUP",
                side="buy",
                status="filled",
                quantity=2,
                submitted_at=start_at + timedelta(hours=1),
            ),
            BrokerOrder(
                id="dedupe-broker-sell-order",
                broker_name="kis",
                mode="paper",
                idempotency_key="dedupe-broker-sell-order-key",
                symbol="DEDUP",
                side="sell",
                status="filled",
                quantity=1,
                submitted_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id="dedupe-broker-buy-fill",
                broker_order_pk="dedupe-broker-buy-order",
                broker_fill_id="dedupe-broker-buy-fill",
                symbol="DEDUP",
                side="buy",
                quantity=2,
                price=Decimal("100"),
                filled_at=start_at + timedelta(hours=1),
            ),
            ExecutionFill(
                id="dedupe-broker-sell-fill-a",
                broker_order_pk="dedupe-broker-sell-order",
                broker_fill_id="broker-sell-identity",
                symbol="DEDUP",
                side="sell",
                quantity=1,
                price=Decimal("150"),
                filled_at=start_at + timedelta(hours=2),
            ),
            ExecutionFill(
                id="dedupe-broker-sell-fill-b",
                broker_order_pk="dedupe-broker-sell-order",
                broker_fill_id="broker-sell-identity",
                symbol="DEDUP",
                side="sell",
                quantity=1,
                price=Decimal("150"),
                filled_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.commit()

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=10,
        )
    finally:
        db.close()
        engine.dispose()

    assert report["counts"]["closed_trade_count"] == 1
    assert report["counts"]["closed_sell_fill_count"] == 1
    assert report["realized_pnl_summary"]["status"] == "complete"
    assert report["realized_pnl_summary"]["sell_fill_count"] == 1
    assert report["realized_pnl_summary"]["win_count"] == 1
    assert report["realized_pnl_summary"]["loss_count"] == 0
    assert report["realized_pnl_summary"]["realized_pnl"] == "50.0000"
    assert report["realized_pnl_summary"]["best_trade"]["sell_fill_id"] == "dedupe-broker-sell-fill-a"


def test_weekly_trade_review_realized_summary_dedupes_fallback_sell_fill_identity(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)

    db.add_all(
        [
            BrokerOrder(
                id="dedupe-fallback-buy-order",
                broker_name="kis",
                mode="paper",
                idempotency_key="dedupe-fallback-buy-order-key",
                symbol="FALLBK",
                side="buy",
                status="filled",
                quantity=1,
                submitted_at=start_at + timedelta(hours=1),
            ),
            BrokerOrder(
                id="dedupe-fallback-sell-order",
                broker_name="kis",
                mode="paper",
                idempotency_key="dedupe-fallback-sell-order-key",
                symbol="FALLBK",
                side="sell",
                status="filled",
                quantity=1,
                submitted_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.flush()
    sell_at = start_at + timedelta(hours=2)
    db.add_all(
        [
            ExecutionFill(
                id="dedupe-fallback-buy-fill",
                broker_order_pk="dedupe-fallback-buy-order",
                broker_fill_id=None,
                symbol="FALLBK",
                side="buy",
                quantity=1,
                price=Decimal("100"),
                filled_at=start_at + timedelta(hours=1),
            ),
            ExecutionFill(
                id="dedupe-fallback-sell-fill-a",
                broker_order_pk="dedupe-fallback-sell-order",
                broker_fill_id=None,
                symbol="FALLBK",
                side="sell",
                quantity=1,
                price=Decimal("75"),
                filled_at=sell_at,
            ),
            ExecutionFill(
                id="dedupe-fallback-sell-fill-b",
                broker_order_pk="dedupe-fallback-sell-order",
                broker_fill_id=None,
                symbol="FALLBK",
                side="sell",
                quantity=1,
                price=Decimal("75.0000"),
                filled_at=sell_at,
            ),
        ]
    )
    db.commit()

    try:
        report = generate_weekly_trade_review(
            settings=settings,
            db=db,
            start_at=start_at,
            end_at=end_at,
            max_fills=10,
        )
    finally:
        db.close()
        engine.dispose()

    assert report["counts"]["closed_trade_count"] == 1
    assert report["counts"]["closed_sell_fill_count"] == 1
    assert report["realized_pnl_summary"]["status"] == "complete"
    assert report["realized_pnl_summary"]["sell_fill_count"] == 1
    assert report["realized_pnl_summary"]["win_count"] == 0
    assert report["realized_pnl_summary"]["loss_count"] == 1
    assert report["realized_pnl_summary"]["realized_pnl"] == "-25.0000"
    assert report["realized_pnl_summary"]["worst_trade"]["sell_fill_id"] == "dedupe-fallback-sell-fill-a"


def test_weekly_trade_review_cli_fails_before_artifact_when_required_mode_mismatches(tmp_path) -> None:
    env = _clean_kai_exec_env()
    env["KAI_EXEC_RUNTIME_MODE"] = "paper"
    env["KAI_EXEC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'unexpected.db'}"

    result = subprocess.run(
        [
            sys.executable,
            str(WEEKLY_REVIEW_SCRIPT),
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


def test_weekly_trade_review_cli_requires_explicit_live_database_url_before_opening_default_sqlite(tmp_path) -> None:
    env = _clean_kai_exec_env()
    work_dir = tmp_path / "work"
    output_dir = tmp_path / "artifacts"
    default_sqlite_dir = work_dir / "var"
    default_sqlite_path = default_sqlite_dir / "kai_trading_exec.db"
    default_sqlite_dir.mkdir(parents=True)

    result = subprocess.run(
        [
            sys.executable,
            str(WEEKLY_REVIEW_SCRIPT),
            "--runtime-mode",
            "live",
            "--require-runtime-mode",
            "live",
            "--start-at",
            "2026-07-25T00:00:00Z",
            "--end-at",
            "2026-08-01T00:00:00Z",
            "--output-dir",
            str(output_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=work_dir,
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert "live weekly export requires an explicit database URL" in result.stderr
    assert "--database-url or KAI_EXEC_DATABASE_URL" in result.stderr
    assert not default_sqlite_path.exists()
    assert not (output_dir / "2026-07-25_to_2026-08-01_weekly_trade_review.json").exists()
    assert not (output_dir / "latest.json").exists()


def test_weekly_trade_review_cli_allows_required_live_with_explicit_database_url(tmp_path) -> None:
    env = _clean_kai_exec_env()
    db_path = tmp_path / "live-review.db"
    database_url = f"sqlite:///{db_path}"
    settings = Settings(database_url=database_url, runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            str(WEEKLY_REVIEW_SCRIPT),
            "--runtime-mode",
            "live",
            "--require-runtime-mode",
            "live",
            "--database-url",
            database_url,
            "--start-at",
            "2026-07-25T00:00:00Z",
            "--end-at",
            "2026-08-01T00:00:00Z",
            "--output-dir",
            str(tmp_path / "artifacts"),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["fill_count"] == 0
    assert (tmp_path / "artifacts" / "2026-07-25_to_2026-08-01_weekly_trade_review.json").exists()
    assert (tmp_path / "artifacts" / "latest.json").exists()


def test_weekly_trade_review_timeout_reports_progress_before_artifact(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 7, 25, tzinfo=UTC)
    end_at = datetime(2026, 8, 1, tzinfo=UTC)
    _seed_weekly_review_fixture(db=db, start_at=start_at)
    progress: list[dict[str, object]] = []

    try:
        try:
            generate_weekly_trade_review(
                settings=settings,
                db=db,
                start_at=start_at,
                end_at=end_at,
                output_dir=tmp_path,
                timeout_seconds=-0.001,
                progress_callback=progress.append,
            )
        except WeeklyTradeReviewTimeoutError as exc:
            assert "timeout_seconds=-0.001" in str(exc)
            assert "weekly_fills" in str(exc)
        else:
            raise AssertionError("expected weekly review timeout")
    finally:
        db.close()
        engine.dispose()

    assert progress == []
    assert not (tmp_path / "2026-07-25_to_2026-08-01_weekly_trade_review.json").exists()


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
    legacy_buy_order = BrokerOrder(
        id="legacy-buy-order",
        execution_plan_item_id="old-item",
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-buy-order-key",
        broker_order_id="kis-legacy-buy-order",
        symbol="049430",
        side="buy",
        status="filled",
        quantity=45,
        limit_price=Decimal("2000"),
        submitted_at=start_at - timedelta(days=10),
        created_at=start_at - timedelta(days=10),
        updated_at=start_at - timedelta(days=10),
    )
    legacy_sell_order = BrokerOrder(
        id="legacy-sell-order",
        execution_plan_item_id="weekly-item",
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-sell-order-key",
        broker_order_id="kis-legacy-sell-order",
        symbol="049430",
        side="sell",
        status="filled",
        quantity=6,
        limit_price=Decimal("1500"),
        submitted_at=start_at + timedelta(days=2, hours=1),
        created_at=start_at + timedelta(days=2, hours=1),
        updated_at=start_at + timedelta(days=2, hours=1),
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
    legacy_buy_fill = ExecutionFill(
        id="legacy-buy-fill",
        broker_order_pk="legacy-buy-order",
        broker_fill_id="legacy-buy-fill",
        symbol="049430",
        side="buy",
        quantity=45,
        price=Decimal("2000"),
        filled_at=start_at - timedelta(days=10),
        created_at=start_at - timedelta(days=10),
        updated_at=start_at - timedelta(days=10),
    )
    legacy_sell_fill = ExecutionFill(
        id="legacy-sell-fill",
        broker_order_pk="legacy-sell-order",
        broker_fill_id="legacy-sell-fill",
        symbol="049430",
        side="sell",
        quantity=6,
        price=Decimal("1500"),
        filled_at=start_at + timedelta(days=2, hours=1, minutes=5),
        created_at=start_at + timedelta(days=2, hours=1, minutes=5),
        updated_at=start_at + timedelta(days=2, hours=1, minutes=5),
    )
    flat_position = ExecutionPosition(
        id="flat-legacy-position",
        mode="paper",
        symbol="049430",
        quantity=0,
        average_price=Decimal("1800"),
        updated_from="reconciliation",
        last_reconciled_at=start_at + timedelta(days=2, hours=1, minutes=10),
        created_at=start_at + timedelta(days=2, hours=1, minutes=10),
        updated_at=start_at + timedelta(days=2, hours=1, minutes=10),
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
        status="degraded",
        last_heartbeat_at=start_at + timedelta(days=6),
        last_success_at=start_at + timedelta(days=6),
        details={
            "protective_stop_actionability": {
                "status": "degraded",
                "warning_count": 1,
                "warnings": [
                    {
                        "symbol": "459550",
                        "execution_plan_item_id": "recovery-stop",
                        "status": "stop_sell_monitoring",
                        "observed_price": "1660.0000",
                        "stop_trigger": "1872.0000",
                        "outcome": "no_position",
                        "reason": "crossed_stop_without_actionable_outcome",
                        "message": "no sellable execution position is available for this trigger",
                        "block_reason": None,
                    }
                ],
            },
            "large": "checkpoint",
        },
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
            legacy_buy_order,
            legacy_sell_order,
            missing_submitted_order,
            *active_nonterminal_orders,
            weekly_fill,
            old_fill,
            legacy_buy_fill,
            legacy_sell_fill,
            flat_position,
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


def _clean_kai_exec_env() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("KAI_EXEC_")
    }
