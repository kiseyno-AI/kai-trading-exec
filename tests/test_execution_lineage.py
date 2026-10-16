from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem
from app.services.execution_lineage import (
    EXECUTION_DECISION_LINEAGE_VERSION,
    decision_lineage_for_order,
    decision_lineage_for_plan_item,
    payload_with_order_decision_lineage,
    resolve_decision_lineage_evidence,
)
from app.services.broker_orders import _preserve_decision_lineage
from app.services.reconciliation import _upsert_order_fills
from app.services.fifo_inventory import FifoRouteScope
from app.services.trade_outcome_feedback import (
    OpenLot,
    _closed_trade,
    _feedback_learning_quality_gate,
    _trade_has_learning_lineage,
)
from app.services.weekly_trade_review import (
    _closed_trade_row,
    _weekly_learning_evidence,
)


def _plan_item() -> ExecutionPlanItem:
    plan = ExecutionPlan(
        id="plan-db-id",
        external_plan_id="core-plan-20261016",
        source="kai-trading-core",
        market_date=date(2026, 10, 16),
        strategy_run_id="core-run-1",
        mode="live",
        status="accepted",
    )
    return ExecutionPlanItem(
        id="item-db-id",
        execution_plan_id=plan.id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="entry",
        status="pending",
        decision_agent_name="krx-decision-agent",
        decision_snapshot_id="snapshot-1",
        decision_run_id="core-run-1",
        source_plan_item_id="core-item-1",
        source_market_date=date(2026, 10, 15),
        provider="kis",
        venue="KRX",
        asset_class="stock",
        plan=plan,
    )


def test_decision_lineage_snapshot_is_complete_and_immutable() -> None:
    item = _plan_item()
    lineage = decision_lineage_for_plan_item(item)

    assert lineage == {
        "schema_version": EXECUTION_DECISION_LINEAGE_VERSION,
        "status": "complete",
        "execution_plan_id": "plan-db-id",
        "operational_execution_plan_id": "plan-db-id",
        "execution_plan_item_id": "item-db-id",
        "external_plan_id": "core-plan-20261016",
        "plan_source": "kai-trading-core",
        "strategy_run_id": "core-run-1",
        "target_market_date": "2026-10-16",
        "source_plan_item_id": "core-item-1",
        "decision_agent_name": "krx-decision-agent",
        "decision_snapshot_id": "snapshot-1",
        "decision_run_id": "core-run-1",
        "source_market_date": "2026-10-15",
    }

    order = BrokerOrder(
        id="order-db-id",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="lineage-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={"decision_lineage": lineage},
    )
    item.decision_agent_name = "later-replanned-agent"
    item.decision_snapshot_id = "later-snapshot"
    item.decision_run_id = "later-run"

    persisted = decision_lineage_for_order(order)
    assert persisted is not None
    assert persisted["decision_agent_name"] == "krx-decision-agent"
    assert persisted["decision_snapshot_id"] == "snapshot-1"
    assert persisted["decision_run_id"] == "core-run-1"


def test_legacy_sell_authority_never_falls_back_to_old_parent_identity() -> None:
    item = _plan_item()
    item.risk_context = {
        "sell_monitor_authority_v1": {
            "market_date": "2026-10-17",
            "requested_at": "2026-10-16T01:00:00+00:00",
            "accepted_at": "2026-10-16T01:00:01+00:00",
            "source_plan_id": "newer-plan-with-ambiguous-id-kind",
        }
    }

    lineage = decision_lineage_for_plan_item(item)

    assert lineage is not None
    assert lineage["status"] == "partial"
    assert lineage["execution_plan_id"] is None
    assert lineage["external_plan_id"] is None
    assert lineage["plan_source"] is None
    assert lineage["strategy_run_id"] is None
    assert lineage["target_market_date"] == "2026-10-17"
    assert lineage["operational_execution_plan_id"] == "plan-db-id"
    assert lineage["execution_plan_item_id"] == "item-db-id"


def test_reconciled_fill_carries_order_lineage_without_fabricating_costs() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    item = _plan_item()
    lineage = decision_lineage_for_plan_item(item)
    assert lineage is not None
    order = BrokerOrder(
        id="fill-lineage-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        broker_order_id="broker-order-1",
        mode="live",
        idempotency_key="fill-lineage-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={"decision_lineage": lineage},
    )
    try:
        with Session(engine) as db:
            db.add(item.plan)
            db.add(item)
            db.add(order)
            db.flush()

            added = _upsert_order_fills(
                db=db,
                order=order,
                fills=[
                    {
                        "broker_fill_id": "broker-fill-1",
                        "symbol": "005930",
                        "side": "buy",
                        "quantity": 1,
                        "price": Decimal("70000"),
                        "filled_at": datetime(2026, 10, 16, tzinfo=UTC).isoformat(),
                        "settlement_currency": "KRW",
                        "transaction_cost": None,
                        "transaction_cost_coverage": "unknown",
                        "transaction_cost_source": (
                            "kis:inquire-daily-ccld:fee_tax_not_provided"
                        ),
                    }
                ],
            )
            db.flush()
            fill = db.scalar(select(ExecutionFill))

            assert added == 1
            assert fill is not None
            assert fill.settlement_currency == "KRW"
            assert fill.transaction_cost is None
            assert fill.transaction_cost_coverage == "unknown"
            assert fill.payload is not None
            assert fill.payload["decision_lineage"] == lineage
    finally:
        engine.dispose()


def test_unlinked_order_does_not_gain_decision_identity() -> None:
    order = BrokerOrder(
        id="unlinked-order",
        broker_name="kis",
        mode="live",
        idempotency_key="unlinked-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={},
    )

    assert decision_lineage_for_order(order) is None


def test_lineage_evidence_distinguishes_legacy_partial_invalid_and_conflict() -> None:
    item = _plan_item()
    complete = decision_lineage_for_plan_item(item)
    assert complete is not None
    order = BrokerOrder(
        id="lineage-evidence-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="lineage-evidence-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={},
    )

    legacy = resolve_decision_lineage_evidence(order=order, fill_payload={})
    assert legacy["status"] == "legacy_degraded"
    assert legacy["lineage"] is None

    order.request_metadata = {
        "decision_lineage": {**complete, "status": "partial"}
    }
    partial = resolve_decision_lineage_evidence(order=order, fill_payload={})
    assert partial["status"] == "partial"
    assert partial["lineage"]["decision_snapshot_id"] == "snapshot-1"

    order.request_metadata = {"decision_lineage": {"schema_version": "invalid"}}
    invalid = resolve_decision_lineage_evidence(order=order, fill_payload={})
    assert invalid["status"] == "invalid"
    assert invalid["lineage"] is None

    order.request_metadata = {"decision_lineage": complete}
    conflicting_fill = dict(complete)
    conflicting_fill["decision_snapshot_id"] = "different-snapshot"
    conflict = resolve_decision_lineage_evidence(
        order=order,
        fill_payload={"decision_lineage": conflicting_fill},
    )
    assert conflict["status"] == "conflict"
    assert conflict["lineage"] is None


def test_complete_fill_snapshot_recovers_absent_order_snapshot_without_mutable_fallback() -> None:
    item = _plan_item()
    complete = decision_lineage_for_plan_item(item)
    assert complete is not None
    order = BrokerOrder(
        id="fill-snapshot-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="fill-snapshot-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={},
    )

    evidence = resolve_decision_lineage_evidence(
        order=order,
        fill_payload={"decision_lineage": complete},
    )

    assert evidence["status"] == "complete"
    assert evidence["source"] == "fill_snapshot"
    assert evidence["lineage"] == complete


def test_retry_preserves_the_original_order_lineage_snapshot() -> None:
    item = _plan_item()
    original = decision_lineage_for_plan_item(item)
    assert original is not None
    order = BrokerOrder(
        id="retry-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="retry-order",
        symbol="005930",
        side="buy",
        status="retry_pending",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={"decision_lineage": original},
    )
    replacement = dict(original)
    replacement["decision_run_id"] = "later-run"

    metadata = _preserve_decision_lineage(
        request_metadata={"decision_lineage": replacement},
        existing_order=order,
    )

    assert metadata["decision_lineage"] == original


def test_retry_preserves_legacy_absence_of_order_lineage() -> None:
    item = _plan_item()
    generated = decision_lineage_for_plan_item(item)
    assert generated is not None
    order = BrokerOrder(
        id="legacy-retry-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-retry-order",
        symbol="005930",
        side="buy",
        status="retry_pending",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={"source": "legacy"},
    )

    metadata = _preserve_decision_lineage(
        request_metadata={"decision_lineage": generated},
        existing_order=order,
    )

    assert "decision_lineage" not in metadata


def test_fill_lineage_accepts_semantically_equal_older_v1_snapshot() -> None:
    item = _plan_item()
    lineage = decision_lineage_for_plan_item(item)
    assert lineage is not None
    older_v1_lineage = dict(lineage)
    older_v1_lineage.pop("operational_execution_plan_id")
    order = BrokerOrder(
        id="older-v1-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="older-v1-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={"decision_lineage": older_v1_lineage},
    )

    payload = payload_with_order_decision_lineage(
        {"decision_lineage": older_v1_lineage},
        order=order,
    )

    assert payload["decision_lineage"] == older_v1_lineage
    assert "decision_lineage_conflict" not in payload


def test_weekly_and_feedback_attribution_prefer_the_order_snapshot() -> None:
    item = _plan_item()
    original = decision_lineage_for_plan_item(item)
    assert original is not None
    buy_order = BrokerOrder(
        id="snapshot-buy-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="snapshot-buy-order",
        broker_order_id="snapshot-buy-broker-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="kis",
        asset_class="stock",
        request_metadata={"decision_lineage": original},
        plan_item=item,
    )
    sell_order = BrokerOrder(
        id="snapshot-sell-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="snapshot-sell-order",
        broker_order_id="snapshot-sell-broker-order",
        symbol="005930",
        side="sell",
        status="filled",
        quantity=1,
        venue="kis",
        asset_class="stock",
        request_metadata={"decision_lineage": original},
        plan_item=item,
    )
    buy_fill = ExecutionFill(
        id="snapshot-buy-fill",
        broker_order=buy_order,
        broker_fill_id="snapshot-buy-fill",
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("70000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("1"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test-exact-cost",
        filled_at=datetime(2026, 10, 16, 0, 2, tzinfo=UTC),
    )
    sell_fill = ExecutionFill(
        id="snapshot-sell-fill",
        broker_order=sell_order,
        broker_fill_id="snapshot-sell-fill",
        symbol="005930",
        side="sell",
        quantity=1,
        price=Decimal("71000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("1"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test-exact-cost",
        filled_at=datetime(2026, 10, 16, 1, 2, tzinfo=UTC),
    )
    item.decision_agent_name = "later-agent"
    item.decision_snapshot_id = "later-snapshot"
    item.decision_run_id = "later-run"
    route_scope = FifoRouteScope(
        provider="kis",
        account_scope="a" * 24,
        venue="kis",
        asset_class="stock",
        exchange="KRX",
    )

    weekly = _closed_trade_row(
        fill=sell_fill,
        buy_fill=buy_fill,
        quantity=Decimal("1"),
        buy_price=Decimal("70000"),
        cost_basis_source="fifo_exact",
        cost_basis_observed_at=buy_fill.filled_at,
        route_scope=route_scope,
    )
    feedback = _closed_trade(
        lot=OpenLot(
            quantity=Decimal("1"),
            price=Decimal("70000"),
            filled_at=buy_fill.filled_at,
            fill=buy_fill,
        ),
        sell_fill=sell_fill,
        quantity=Decimal("1"),
        route_scope=route_scope,
    )

    assert weekly["decision_agent_name"] == "krx-decision-agent"
    assert weekly["decision_snapshot_id"] == "snapshot-1"
    assert weekly["decision_run_id"] == "core-run-1"
    assert feedback["decision_agent_name"] == "krx-decision-agent"
    assert feedback["decision_run_id"] == "core-run-1"
    assert weekly["decision_lineage_status"] == "complete"
    assert feedback["decision_lineage_status"] == "complete"


def test_partial_snapshot_blocks_weekly_and_feedback_learning_despite_mutable_identity() -> None:
    item = _plan_item()
    partial = decision_lineage_for_plan_item(item)
    assert partial is not None
    partial["status"] = "partial"
    buy_order = BrokerOrder(
        id="partial-buy-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="partial-buy-order",
        broker_order_id="partial-buy-broker-order",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="kis",
        asset_class="stock",
        request_metadata={"decision_lineage": partial},
        plan_item=item,
    )
    sell_order = BrokerOrder(
        id="partial-sell-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="live",
        idempotency_key="partial-sell-order",
        broker_order_id="partial-sell-broker-order",
        symbol="005930",
        side="sell",
        status="filled",
        quantity=1,
        venue="kis",
        asset_class="stock",
        request_metadata={"decision_lineage": partial},
        plan_item=item,
    )
    buy_fill = ExecutionFill(
        id="partial-buy-fill",
        broker_order=buy_order,
        broker_fill_id="partial-buy-fill",
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("70000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("1"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test-exact-cost",
        filled_at=datetime(2026, 10, 16, 0, 2, tzinfo=UTC),
        payload={"decision_lineage": partial},
    )
    sell_fill = ExecutionFill(
        id="partial-sell-fill",
        broker_order=sell_order,
        broker_fill_id="partial-sell-fill",
        symbol="005930",
        side="sell",
        quantity=1,
        price=Decimal("71000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("1"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test-exact-cost",
        filled_at=datetime(2026, 10, 16, 1, 2, tzinfo=UTC),
        payload={"decision_lineage": partial},
    )
    route_scope = FifoRouteScope(
        provider="kis",
        account_scope="a" * 24,
        venue="kis",
        asset_class="stock",
        exchange="KRX",
    )

    weekly = _closed_trade_row(
        fill=sell_fill,
        buy_fill=buy_fill,
        quantity=Decimal("1"),
        buy_price=Decimal("70000"),
        cost_basis_source="fifo_exact",
        cost_basis_observed_at=buy_fill.filled_at,
        route_scope=route_scope,
    )
    feedback = _closed_trade(
        lot=OpenLot(
            quantity=Decimal("1"),
            price=Decimal("70000"),
            filled_at=buy_fill.filled_at,
            fill=buy_fill,
        ),
        sell_fill=sell_fill,
        quantity=Decimal("1"),
        route_scope=route_scope,
    )

    assert weekly["decision_lineage_status"] == "partial"
    assert feedback["decision_lineage_status"] == "partial"
    assert feedback["profitability_eligible"] is False
    assert _trade_has_learning_lineage(feedback) is False
    feedback_gate = _feedback_learning_quality_gate(
        closed_trades=[feedback],
        candidate_rule_replay={"status": "ready"},
    )
    assert feedback_gate["status"] == "blocked"
    assert "incomplete_decision_lineage_coverage" in feedback_gate["blocked_reasons"]

    weekly_evidence = _weekly_learning_evidence(
        fill_attribution_coverage={"status": "incomplete"},
        realized_pnl_coverage={"status": "complete"},
        realized_pnl_summary={
            "transaction_cost_coverage": "complete",
            "currency_coverage": "complete",
        },
        closed_trades=[weekly],
        weekly_order_status_counts=[],
        execution_failures=[],
        truncated={
            "fills": False,
            "weekly_orders": False,
            "execution_failures": False,
            "closed_trades": False,
        },
    )
    performance = weekly_evidence["performance_feedback"]
    assert performance["status"] == "blocked"
    assert "incomplete_closed_trade_decision_lineage" in performance["blocked_reasons"]
