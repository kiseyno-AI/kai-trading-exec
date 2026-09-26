from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import event, inspect, select, text

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    FifoInventoryIsolation,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.weekly_trade_review import (
    WeeklyTradeReviewTimeoutError,
    _weekly_closed_trades,
    _weekly_fill_attribution_coverage,
    _weekly_fills,
    generate_weekly_trade_review,
)
from app.services.runtime import runtime_counts


REPO_ROOT = Path(__file__).resolve().parents[1]
WEEKLY_REVIEW_SCRIPT = REPO_ROOT / "scripts" / "export_weekly_trade_review.py"
WEEKLY_TEST_ACCOUNT_SCOPE = "a" * 24


def _scope_orders(*orders: BrokerOrder) -> None:
    for order in orders:
        order.venue = order.venue or "KRX"
        order.asset_class = order.asset_class or "stock"
        metadata = dict(order.request_metadata or {})
        metadata["broker_account_scope"] = {
            "policy_version": 1,
            "scope": WEEKLY_TEST_ACCOUNT_SCOPE,
        }
        metadata["exchange"] = order.venue
        order.request_metadata = metadata


def _build_db(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _assert_mapping_contains(actual: object, expected: dict[str, object]) -> None:
    assert isinstance(actual, dict)
    for key, expected_value in expected.items():
        assert key in actual
        if isinstance(expected_value, dict):
            _assert_mapping_contains(actual[key], expected_value)
        else:
            assert actual[key] == expected_value


def test_weekly_fills_expose_decision_linkage_and_explicit_unknown_coverage() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _build_db(settings)
    start_at = datetime(2026, 9, 1, tzinfo=UTC)
    end_at = datetime(2026, 9, 8, tzinfo=UTC)
    plan = ExecutionPlan(
        id="linked-fill-plan",
        external_plan_id="linked-fill-plan",
        source="kai-trading-core",
        market_date=datetime(2026, 9, 2, tzinfo=UTC).date(),
        strategy_run_id="decision-run-101",
        mode="paper",
        status="accepted",
    )
    item = ExecutionPlanItem(
        id="linked-fill-item",
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="filled",
        decision_agent_name="short-term-decider",
        decision_snapshot_id="snapshot-101",
        decision_run_id="decision-run-101",
        source_market_date=datetime(2026, 9, 1, tzinfo=UTC).date(),
    )
    linked_order = BrokerOrder(
        id="linked-fill-order",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="linked-fill-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
    )
    unknown_order = BrokerOrder(
        id="unknown-fill-order",
        broker_name="kis",
        mode="paper",
        idempotency_key="unknown-fill-order",
        symbol="000660",
        side="buy",
        status="filled",
        quantity=1,
    )
    db.add_all(
        [
            plan,
            linked_order,
            unknown_order,
            ExecutionFill(
                id="linked-fill",
                broker_order=linked_order,
                symbol="005930",
                side="buy",
                quantity=1,
                price=Decimal("70000"),
                filled_at=start_at + timedelta(hours=1),
            ),
            ExecutionFill(
                id="unknown-fill",
                broker_order=unknown_order,
                symbol="000660",
                side="buy",
                quantity=1,
                price=Decimal("120000"),
                filled_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.commit()

    rows, truncated = _weekly_fills(
        db=db,
        mode="paper",
        start_at=start_at,
        end_at=end_at,
        include_json_payloads=False,
        max_rows=20,
    )
    rows_by_id = {row["id"]: row for row in rows}

    assert truncated is False
    _assert_mapping_contains(
        rows_by_id["linked-fill"],
        {
            "execution_plan_item_id": "linked-fill-item",
            "execution_plan_item_intent": "intraday_watch_buy",
            "decision_agent_name": "short-term-decider",
            "decision_snapshot_id": "snapshot-101",
            "decision_run_id": "decision-run-101",
            "source_market_date": datetime(2026, 9, 1, tzinfo=UTC).date(),
            "target_market_date": datetime(2026, 9, 2, tzinfo=UTC).date(),
            "execution_plan_source": "kai-trading-core",
            "strategy_run_id": "decision-run-101",
        },
    )
    assert rows_by_id["unknown-fill"]["execution_plan_item_id"] is None
    assert rows_by_id["unknown-fill"]["decision_snapshot_id"] is None
    assert _weekly_fill_attribution_coverage(rows, truncated=False) == {
        "status": "incomplete",
        "truncated": False,
        "total_fill_rows": 2,
        "execution_plan_item_linked_fill_rows": 1,
        "execution_plan_item_unlinked_fill_rows": 1,
        "decision_identity_linked_fill_rows": 1,
        "decision_identity_missing_fill_rows": 1,
        "decision_run_mismatch_fill_rows": 0,
        "unlinked_fill_ids": ["unknown-fill"],
        "decision_identity_missing_fill_ids": ["unknown-fill"],
        "decision_run_mismatch_fill_ids": [],
    }
    db.close()
    engine.dispose()


def test_weekly_fill_attribution_rejects_core_decision_run_mismatch() -> None:
    coverage = _weekly_fill_attribution_coverage(
        [
            {
                "id": "mismatched-core-fill",
                "execution_plan_item_id": "item-1",
                "decision_agent_name": "short-term-decider",
                "decision_snapshot_id": "snapshot-1",
                "decision_run_id": "decision-run-1",
                "execution_plan_source": "kai-trading-core",
                "strategy_run_id": "strategy-run-2",
            }
        ],
        truncated=False,
    )

    assert coverage["status"] == "incomplete"
    assert coverage["decision_identity_linked_fill_rows"] == 0
    assert coverage["decision_identity_missing_fill_ids"] == [
        "mismatched-core-fill"
    ]
    assert coverage["decision_run_mismatch_fill_ids"] == [
        "mismatched-core-fill"
    ]


def test_weekly_fifo_never_pairs_same_symbol_across_execution_routes() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _build_db(settings)
    start_at = datetime(2026, 9, 1, tzinfo=UTC)
    end_at = datetime(2026, 9, 8, tzinfo=UTC)

    def order(
        *,
        order_id: str,
        provider: str,
        side: str,
        account_scope: str | None,
        exchange: str,
    ) -> BrokerOrder:
        venue = "KRX" if provider == "kis" else "ALPACA"
        return BrokerOrder(
            id=order_id,
            broker_name=provider,
            mode="paper",
            idempotency_key=order_id,
            symbol="SHARED",
            side=side,
            status="filled",
            quantity=1,
            venue=venue,
            asset_class="stock",
            request_metadata=(
                {
                    "exchange": exchange,
                    "broker_account_scope": {
                        "policy_version": 1,
                        "scope": account_scope,
                    }
                }
                if account_scope is not None
                else None
            ),
        )

    kis_buy = order(
        order_id="weekly-kis-buy",
        provider="kis",
        side="buy",
        account_scope="account-a",
        exchange="KRX",
    )
    alpaca_sell = order(
        order_id="weekly-alpaca-sell",
        provider="alpaca",
        side="sell",
        account_scope="account-a",
        exchange="NASD",
    )
    kis_sell = order(
        order_id="weekly-kis-sell",
        provider="kis",
        side="sell",
        account_scope="account-a",
        exchange="KRX",
    )
    cross_exchange_sell = order(
        order_id="weekly-cross-exchange-sell",
        provider="kis",
        side="sell",
        account_scope="account-a",
        exchange="NYSE",
    )
    unscoped_sell = order(
        order_id="weekly-unscoped-sell",
        provider="kis",
        side="sell",
        account_scope=None,
        exchange="KRX",
    )
    db.add_all(
        [
            kis_buy,
            alpaca_sell,
            kis_sell,
            cross_exchange_sell,
            unscoped_sell,
            ExecutionFill(
                id="weekly-kis-buy-fill",
                broker_order=kis_buy,
                symbol="SHARED",
                side="buy",
                quantity=1,
                price=Decimal("100"),
                filled_at=start_at + timedelta(hours=1),
            ),
            ExecutionFill(
                id="weekly-alpaca-sell-fill",
                broker_order=alpaca_sell,
                symbol="SHARED",
                side="sell",
                quantity=1,
                price=Decimal("200"),
                filled_at=start_at + timedelta(hours=2),
            ),
            ExecutionFill(
                id="weekly-kis-sell-fill",
                broker_order=kis_sell,
                symbol="SHARED",
                side="sell",
                quantity=1,
                price=Decimal("110"),
                filled_at=start_at + timedelta(hours=3),
            ),
            ExecutionFill(
                id="weekly-cross-exchange-sell-fill",
                broker_order=cross_exchange_sell,
                symbol="SHARED",
                side="sell",
                quantity=1,
                price=Decimal("250"),
                filled_at=start_at + timedelta(hours=4),
            ),
            ExecutionFill(
                id="weekly-unscoped-sell-fill",
                broker_order=unscoped_sell,
                symbol="SHARED",
                side="sell",
                quantity=1,
                price=Decimal("300"),
                filled_at=start_at + timedelta(hours=5),
            ),
        ]
    )
    db.commit()

    rows, truncated = _weekly_closed_trades(
        db=db,
        mode="paper",
        start_at=start_at,
        end_at=end_at,
        max_rows=20,
    )

    rows_by_fill = {row["sell_fill_id"]: row for row in rows}
    assert truncated is False
    assert rows_by_fill["weekly-alpaca-sell-fill"]["cost_basis_source"] == "unknown"
    assert rows_by_fill["weekly-alpaca-sell-fill"]["realized_pnl"] is None
    assert rows_by_fill["weekly-kis-sell-fill"]["realized_pnl"] == Decimal("10.0000")
    assert rows_by_fill["weekly-cross-exchange-sell-fill"]["cost_basis_source"] == (
        "unknown"
    )
    assert rows_by_fill["weekly-cross-exchange-sell-fill"]["realized_pnl"] is None
    assert rows_by_fill["weekly-unscoped-sell-fill"]["cost_basis_source"] == (
        "execution_scope_unavailable"
    )
    db.close()
    engine.dispose()


def test_weekly_fifo_starts_clean_generation_after_exact_route_verified_flat() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _build_db(settings)
    start_at = datetime(2026, 9, 1, tzinfo=UTC)
    end_at = datetime(2026, 9, 8, tzinfo=UTC)
    old_at = start_at + timedelta(hours=1)
    boundary_at = old_at + timedelta(seconds=1)

    def order(*, row_id: str, side: str) -> BrokerOrder:
        row = BrokerOrder(
            id=row_id,
            broker_name="kis",
            mode="paper",
            idempotency_key=row_id,
            symbol="005930",
            side=side,
            status="filled",
            quantity=1,
            venue="KRX",
            asset_class="stock",
        )
        _scope_orders(row)
        return row

    old_one = order(row_id="weekly-old-ambiguous-one", side="buy")
    old_two = order(row_id="weekly-old-ambiguous-two", side="buy")
    new_buy = order(row_id="weekly-new-generation-buy", side="buy")
    new_sell = order(row_id="weekly-new-generation-sell", side="sell")
    db.add_all(
        [
            old_one,
            old_two,
            new_buy,
            new_sell,
            FifoInventoryIsolation(
                id="weekly-generation-boundary",
                mode="paper",
                provider="kis",
                account_scope=WEEKLY_TEST_ACCOUNT_SCOPE,
                venue="KRX",
                asset_class="stock",
                exchange="KRX",
                symbol="005930",
                residual_quantity=Decimal("2"),
                last_local_fill_at=old_at,
                broker_position_id="weekly-flat-position",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=boundary_at,
                isolated_through_at=boundary_at,
                evidence_source="broker_flat_reconciliation",
                actor="test",
                reason="exact-route broker-authenticated verified flat",
            ),
            ExecutionFill(
                id="weekly-old-ambiguous-fill-one",
                broker_order=old_one,
                symbol="005930",
                side="buy",
                quantity=1,
                price=Decimal("100"),
                filled_at=old_at,
            ),
            ExecutionFill(
                id="weekly-old-ambiguous-fill-two",
                broker_order=old_two,
                symbol="005930",
                side="buy",
                quantity=1,
                price=Decimal("200"),
                filled_at=old_at,
            ),
            ExecutionFill(
                id="weekly-new-generation-buy-fill",
                broker_order=new_buy,
                symbol="005930",
                side="buy",
                quantity=1,
                price=Decimal("300"),
                filled_at=boundary_at + timedelta(seconds=1),
            ),
            ExecutionFill(
                id="weekly-new-generation-sell-fill",
                broker_order=new_sell,
                symbol="005930",
                side="sell",
                quantity=1,
                price=Decimal("350"),
                filled_at=boundary_at + timedelta(minutes=1),
            ),
        ]
    )
    db.commit()

    rows, truncated = _weekly_closed_trades(
        db=db,
        mode="paper",
        start_at=start_at,
        end_at=end_at,
        max_rows=20,
    )

    assert truncated is False
    assert len(rows) == 1
    assert rows[0]["sell_fill_id"] == "weekly-new-generation-sell-fill"
    assert rows[0]["cost_basis_source"] == "fill_fifo"
    assert rows[0]["realized_pnl"] == Decimal("50.0000")
    db.close()
    engine.dispose()


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
    _assert_mapping_contains(report["realized_pnl_summary"], {
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
    })
    assert report["realized_pnl_summary"]["gross_realized_pnl_by_currency"] == {
        "KRW": "-1800.0000"
    }
    assert report["realized_pnl_summary"]["net_realized_pnl_by_currency"] == {
        "KRW": None
    }
    assert report["realized_pnl_summary"]["transaction_cost_coverage"] == "unknown"
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
    assert "broker_orders.request_metadata" in selected_sql
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


def test_weekly_json_export_recursively_redacts_account_identifiers(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        weekly_trade_review_dir=str(tmp_path),
    )
    engine, db = _build_db(settings)
    start_at = datetime(2026, 9, 1, tzinfo=UTC)
    end_at = datetime(2026, 9, 8, tzinfo=UTC)
    order = BrokerOrder(
        id="weekly-redaction-order",
        broker_name="kis",
        mode="paper",
        idempotency_key="weekly-redaction-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=start_at + timedelta(hours=1),
        request_metadata={
            "account_scope": WEEKLY_TEST_ACCOUNT_SCOPE,
            "nested": {"Account_No": "12345678", "exchange": "KRX"},
        },
        response_metadata={"nested": [{"broker_account_id": "raw-id", "ok": True}]},
        raw_broker_payload={"output": {"CANO": "12345678", "status": "filled"}},
    )
    fill = ExecutionFill(
        id="weekly-redaction-fill",
        broker_order=order,
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("70000"),
        filled_at=start_at + timedelta(hours=2),
        payload={"nested": {"ACNT_PRDT_CD": "01", "marker": "kept"}},
    )
    event_row = BrokerOrderEvent(
        broker_order=order,
        event_type="reconcile_result",
        observed_at=start_at + timedelta(hours=2),
        request_payload={"acct_id": "raw-acct", "request_marker": "kept"},
        response_payload={"account_id": "raw-id", "response_marker": "kept"},
        payload={"acct_no": "raw-acct", "event_marker": "kept"},
    )
    db.add_all([order, fill, event_row])
    db.commit()

    report = generate_weekly_trade_review(
        settings=settings,
        db=db,
        start_at=start_at,
        end_at=end_at,
        include_json_payloads=True,
    )

    serialized = json.dumps(report, sort_keys=True)
    assert "12345678" not in serialized
    assert "raw-id" not in serialized
    assert "raw-acct" not in serialized
    assert "CANO" not in serialized
    assert "ACNT_PRDT_CD" not in serialized
    assert report["linked_broker_orders"][0]["request_metadata"] == {
        "account_scope": WEEKLY_TEST_ACCOUNT_SCOPE,
        "nested": {"exchange": "KRX"},
    }
    assert report["fills"][0]["payload"] == {"nested": {"marker": "kept"}}
    assert report["linked_broker_order_events"][0]["payload"] == {
        "event_marker": "kept"
    }
    assert "12345678" not in (tmp_path / "latest.json").read_text(encoding="utf-8")
    db.close()
    engine.dispose()


def test_weekly_active_order_summary_matches_runtime_actionable_semantics(tmp_path) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
        weekly_trade_review_dir=str(tmp_path),
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
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="weekly-manual-review-required",
                broker_order_id=None,
                symbol="005930",
                side="sell",
                status="manual_review_required",
                quantity=1,
                submitted_at=None,
                last_broker_sync_at=reference_at,
                last_error_classification="submit_outcome_unknown",
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
    assert summary["manual_review_order_count"] == 1
    assert summary["actionable_status_counts"] == [
        {
            "status": "submitted",
            "count": 1,
            "oldest_submitted_at": recent_at.isoformat(),
            "newest_submitted_at": recent_at.isoformat(),
        }
    ]
    assert {row["idempotency_key"] for row in summary["stale_nonterminal_samples"]} == {
        "weekly-nonactionable-stale",
        "weekly-manual-review-required",
    }
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
    _scope_orders(sell_order, buy_order)
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
                    provider="kis",
                    account_scope=WEEKLY_TEST_ACCOUNT_SCOPE,
                    exchange="KRX",
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

    assert len(report["closed_trades"]) == 1
    _assert_mapping_contains(
        report["closed_trades"][0],
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
            "settlement_currency": None,
            "net_realized_pnl": None,
            "transaction_cost_coverage": "unknown",
        },
    )
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
            venue="KRX",
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
            venue="KRX",
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
            venue="KRX",
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
            venue="KRX",
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
            venue="KRX",
            side="sell",
            status="filled",
            quantity=5,
            submitted_at=start_at + timedelta(hours=5),
        ),
    ]
    _scope_orders(*orders)
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
    _assert_mapping_contains(report["realized_pnl_summary"], {
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
    })
    assert report["realized_pnl_summary"]["gross_realized_pnl_by_currency"] == {
        "KRW": "220.0000"
    }
    assert report["realized_pnl_summary"]["net_realized_pnl_by_currency"] == {
        "KRW": None
    }
    assert report["realized_pnl_summary"]["transaction_cost_coverage"] == "unknown"


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
                venue="KRX",
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
                venue="KRX",
                side="sell",
                status="filled",
                quantity=1,
                submitted_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.flush()
    _scope_orders(*db.scalars(select(BrokerOrder)).all())
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
                venue="KRX",
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
                venue="KRX",
                side="sell",
                status="filled",
                quantity=1,
                submitted_at=start_at + timedelta(hours=2),
            ),
        ]
    )
    db.flush()
    _scope_orders(*db.scalars(select(BrokerOrder)).all())
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


def test_weekly_trade_review_cli_rejects_credential_bearing_database_url_argv(
    tmp_path,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(WEEKLY_REVIEW_SCRIPT),
            "--runtime-mode",
            "live",
            "--require-runtime-mode",
            "live",
            "--database-url",
            "postgresql+psycopg://operator:not-a-real-secret@db.invalid/exec",
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
        env=_clean_kai_exec_env(),
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "KAI_EXEC_DATABASE_URL environment variable" in result.stderr
    assert not (tmp_path / "latest.json").exists()


def test_weekly_trade_review_cli_rejects_opaque_database_query_before_db_open(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "must-not-open.db"
    secret = "must-not-leak-weekly-secret"
    database_url = (
        f"sqlite:///{database_path}?connection_string="
        f"password%3D{secret}"
    )
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
        env=_clean_kai_exec_env(),
    )

    assert result.returncode != 0
    assert "KAI_EXEC_DATABASE_URL environment variable" in result.stderr
    assert secret not in result.stdout
    assert secret not in result.stderr
    assert not database_path.exists()
    assert not (tmp_path / "artifacts" / "latest.json").exists()


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
        provider="kis",
        account_scope=WEEKLY_TEST_ACCOUNT_SCOPE,
        exchange="KRX",
        symbol="049430",
        quantity=0,
        average_price=Decimal("1800"),
        updated_from="reconciliation",
        last_reconciled_at=start_at + timedelta(days=2, hours=1, minutes=10),
        created_at=start_at + timedelta(days=2, hours=1, minutes=10),
        updated_at=start_at + timedelta(days=2, hours=1, minutes=10),
    )
    _scope_orders(
        weekly_order,
        old_order,
        legacy_buy_order,
        legacy_sell_order,
        missing_submitted_order,
        *active_nonterminal_orders,
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
