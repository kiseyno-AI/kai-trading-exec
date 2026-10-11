from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select, text

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPosition,
    FifoInventoryIsolation,
    RuntimeRemediationEvent,
)
from app.services.execution_fill_repair import (
    MISSING_FILL_REPAIR_EVENT,
    analyze_fifo_inventory_repairs,
    analyze_execution_fill_repairs,
    apply_fifo_inventory_repairs,
    apply_execution_fill_repairs,
)
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    duplicate_fill_conflicting_fields,
)
from app.services.position_identity import (
    PositionIdentity,
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
)
from app.services.runtime import broker_order_fill_quality_payload, daily_pnl_payload


FILLED_AT = datetime(2026, 7, 24, 0, 5, 1, tzinfo=UTC)
SOLD_AT = datetime(2026, 7, 24, 1, 0, 0, tzinfo=UTC)
FIFO_TEST_ACCOUNT_SCOPE = "a" * 24
FIFO_FOREIGN_ACCOUNT_SCOPE = "b" * 24


def _fifo_position_payload(*, exchange: str = "KRX") -> dict[str, object]:
    return payload_with_position_identity(
        None,
        identity=PositionIdentity(
            provider="kis",
            account_scope=FIFO_TEST_ACCOUNT_SCOPE,
            exchange=exchange,
        ),
    )


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _settings(database_url: str = "sqlite:///:memory:") -> Settings:
    return Settings(database_url=database_url, runtime_mode="paper")


def _bind_order_to_runtime_scope(
    order: BrokerOrder,
    *,
    settings: Settings,
    exchange: str = "KRX",
) -> None:
    identity = build_position_identity(
        settings=settings,
        provider=order.broker_name,
        exchange=exchange,
    )
    order.request_metadata = {
        "exchange": exchange,
        "execution_scope": execution_scope_payload(
            identity=identity,
            venue=str(order.venue or exchange),
            asset_class=str(order.asset_class or "stock"),
        ),
    }


def _filled_order_missing_fill() -> BrokerOrder:
    return BrokerOrder(
        broker_name="kis",
        venue="KRX",
        asset_class="stock",
        mode="paper",
        idempotency_key="repair-missing-105560-buy",
        broker_order_id="missing-buy-1",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=FILLED_AT,
        request_metadata={
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 2,
                "scope": FIFO_TEST_ACCOUNT_SCOPE,
            }
        },
        raw_broker_payload={
            "order": {
                "odno": "missing-buy-1",
                "pdno": "105560",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "avg_pric": "71000",
                "filled_at": FILLED_AT.isoformat(),
                "ord_dt": "20260724",
                "ord_tmd": "090501",
            }
        },
    )


def _duplicate_fill_order() -> BrokerOrder:
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="repair-duplicate-282330-buy",
        broker_order_id="duplicate-buy-1",
        symbol="282330",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=SOLD_AT,
    )


def test_missing_fill_detection_reports_evidence_and_proposed_fields() -> None:
    settings = _settings()
    engine, db = _session(settings)
    db.add(_filled_order_missing_fill())
    db.commit()

    payload = analyze_execution_fill_repairs(db=db, runtime_mode="paper")

    assert payload["summary"]["missing_filled_order_count"] == 1
    item = payload["missing_filled_orders"][0]
    assert item["symbol"] == "105560"
    assert item["side"] == "buy"
    assert item["quantity"] == 1
    assert item["submitted_at"] == FILLED_AT.replace(tzinfo=None).isoformat()
    assert item["available_repair_evidence"]["source"] == "broker_orders.raw_broker_payload"
    assert item["proposed_fills"] == [
        {
            "broker_fill_id": None,
            "symbol": "105560",
            "side": "buy",
            "quantity": 1,
            "price": "71000.0000",
            "filled_at": FILLED_AT.isoformat(),
        }
    ]
    engine.dispose()


def test_canceled_order_with_broker_reported_fill_is_manual_review_candidate() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        venue="NASD",
        asset_class="us_equity",
        mode="paper",
        idempotency_key="repair-canceled-partial-us-fill",
        broker_order_id="canceled-partial-us-fill",
        symbol="AAPL",
        side="buy",
        status="canceled",
        quantity=10,
        submitted_at=FILLED_AT,
        request_metadata={
            "exchange": "NASD",
            "broker_account_scope": {
                "policy_version": 2,
                "scope": FIFO_TEST_ACCOUNT_SCOPE,
            },
        },
        raw_broker_payload={
            "output": [
                {
                    "ft_ord_qty": "10",
                    "ft_ccld_qty": "1",
                    "nccs_qty": "0",
                }
            ]
        },
    )
    db.add(order)
    db.commit()

    payload = analyze_execution_fill_repairs(db=db, runtime_mode="paper")

    assert payload["summary"]["missing_filled_order_count"] == 1
    item = payload["missing_filled_orders"][0]
    assert item["broker_order_pk"] == order.id
    assert item["candidate_reason"] == "missing_execution_fill"
    assert item["quantity_basis"] == "quantity"
    assert item["expected_quantity"] == 1
    assert item["persisted_quantity"] == 0
    assert item["proposed_fills"] == []
    assert item["repair_action"] == "record_manual_review"
    engine.dispose()


def test_partial_terminal_fill_is_manual_review_candidate_using_base_quantity() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _filled_order_missing_fill()
    order.idempotency_key = "repair-partial-base-quantity"
    order.broker_order_id = "partial-base-quantity"
    order.quantity = 10
    order.base_quantity = Decimal("1.5")
    db.add(order)
    db.flush()
    persisted_fill = ExecutionFill(
        idempotency_key="persisted-partial-base-quantity-fill",
        broker_order_pk=order.id,
        broker_fill_id="partial-base-quantity-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=10,
        base_quantity=Decimal("0.5"),
        price=Decimal("71000"),
        filled_at=FILLED_AT,
    )
    db.add(persisted_fill)
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")

    assert dry_run["summary"]["missing_filled_order_count"] == 1
    item = dry_run["missing_filled_orders"][0]
    assert item["candidate_reason"] == "terminal_fill_quantity_mismatch"
    assert item["quantity_basis"] == "base_quantity"
    assert item["expected_quantity"] == "1.5"
    assert item["persisted_quantity"] == "0.5"
    assert item["persisted_fill_count"] == 1
    assert item["proposed_fills"] == []
    assert item["repair_action"] == "record_manual_review"

    applied = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="partial evidence cannot be completed automatically",
        now=FILLED_AT + timedelta(minutes=10),
    )

    assert applied["summary"]["inserted_fill_count"] == 0
    assert applied["summary"]["manual_review_missing_fill_count"] == 1
    assert db.scalars(
        select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id)
    ).all() == [persisted_fill]
    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "missing_execution_fill",
            RuntimeRemediationEvent.subject_id == order.id,
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["blocker"] == "terminal_fill_quantity_mismatch"
    engine.dispose()


def test_missing_fill_repair_rejects_event_time_as_fill_chronology() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        venue="KRX",
        asset_class="stock",
        mode="paper",
        idempotency_key="repair-missing-without-broker-time",
        broker_order_id="missing-without-broker-time",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=FILLED_AT,
    )
    db.add(order)
    db.flush()
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="reconciliation",
            event_source="reconciliation",
            normalized_status="filled",
            payload={
                "odno": order.broker_order_id,
                "pdno": order.symbol,
                "tot_ccld_qty": "1",
                "avg_pric": "71000",
            },
            observed_at=FILLED_AT + timedelta(minutes=10),
        )
    )
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    item = dry_run["missing_filled_orders"][0]

    assert item["proposed_fills"] == []
    assert item["available_repair_evidence"] is None
    assert item["repair_action"] == "record_manual_review"

    applied = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="do not invent broker chronology",
        now=FILLED_AT + timedelta(minutes=20),
    )

    assert applied["summary"]["inserted_fill_count"] == 0
    assert db.scalar(
        select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id)
    ) is None
    engine.dispose()


def test_missing_fill_repair_rejects_kis_order_time_as_fill_chronology() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _filled_order_missing_fill()
    assert isinstance(order.raw_broker_payload, dict)
    raw_order = order.raw_broker_payload["order"]
    assert isinstance(raw_order, dict)
    raw_order.pop("filled_at")
    db.add(order)
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    item = dry_run["missing_filled_orders"][0]

    assert item["proposed_fills"] == []
    assert item["available_repair_evidence"] is None
    assert item["repair_action"] == "record_manual_review"
    engine.dispose()


def test_missing_explicit_fractional_fill_preserves_fee_and_currency_evidence() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="upbit",
        mode="paper",
        idempotency_key="repair-explicit-crypto-fill",
        broker_order_id="upbit-explicit-fill-1",
        symbol="KRW-BTC",
        side="buy",
        status="filled",
        quantity=None,
        base_quantity=Decimal("0.00123456"),
        quote_notional=Decimal("75000.5000"),
        asset_class="crypto",
        venue="UPBIT",
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "fills": [
                {
                    "broker_fill_id": "upbit-trade-1",
                    "symbol": "KRW-BTC",
                    "side": "buy",
                    "base_quantity": "0.00123456",
                    "quote_notional": "75000.5000",
                    "price": "60750445.4470",
                    "filled_at": FILLED_AT.isoformat(),
                    "settlement_currency": "KRW",
                    "commission": "7.25",
                    "fee": "2.75",
                    "tax": "0",
                    "transaction_cost_coverage": "complete",
                    "transaction_cost_source": "broker_payload:explicit_fee_bundle",
                }
            ]
        },
    )
    db.add(order)
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    proposed = dry_run["missing_filled_orders"][0]["proposed_fills"][0]

    assert proposed["quantity"] == 0
    assert proposed["base_quantity"] == "0.00123456"
    assert proposed["quote_notional"] == "75000.5000"
    assert proposed["settlement_currency"] == "KRW"
    assert proposed["transaction_cost"] == "10.00"
    assert proposed["transaction_cost_coverage"] == "complete"
    assert proposed["transaction_cost_source"] == "broker_payload:explicit_fee_bundle"

    applied = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="restore explicit crypto fill economics",
        now=FILLED_AT,
    )
    repeated = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="restore explicit crypto fill economics",
        now=FILLED_AT,
    )

    fill = db.scalar(select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id))
    assert applied["summary"]["inserted_fill_count"] == 1
    assert repeated["summary"]["inserted_fill_count"] == 0
    assert fill is not None
    assert fill.quantity == 0
    assert fill.base_quantity == Decimal("0.001234560000")
    assert fill.quote_notional == Decimal("75000.5000")
    assert fill.settlement_currency == "KRW"
    assert fill.transaction_cost == Decimal("10.00000000")
    assert fill.transaction_cost_coverage == "complete"
    assert fill.transaction_cost_source == "broker_payload:explicit_fee_bundle"
    assert fill.payload["fill_time_precision"] == "exact"
    assert fill.payload["fill_time_source"] == "broker_payload"
    engine.dispose()


def test_missing_fractional_fill_dedupe_uses_base_quantity_and_quote_notional() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="upbit",
        mode="paper",
        idempotency_key="repair-distinct-fractional-fills",
        broker_order_id="upbit-distinct-fills-1",
        symbol="KRW-ETH",
        side="buy",
        status="filled",
        base_quantity=Decimal("0.003"),
        quote_notional=Decimal("30.0000"),
        asset_class="crypto",
        venue="UPBIT",
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "fills": [
                {
                    "base_quantity": "0.001",
                    "quote_notional": "10.0000",
                    "price": "10000",
                    "filled_at": FILLED_AT.isoformat(),
                },
                {
                    "base_quantity": "0.002",
                    "quote_notional": "20.0000",
                    "price": "10000",
                    "filled_at": FILLED_AT.isoformat(),
                },
            ]
        },
    )
    db.add(order)
    db.commit()

    result = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="retain distinct fractional executions",
        now=FILLED_AT,
    )

    fills = db.scalars(
        select(ExecutionFill)
        .where(ExecutionFill.broker_order_pk == order.id)
        .order_by(ExecutionFill.base_quantity.asc())
    ).all()
    assert result["summary"]["inserted_fill_count"] == 2
    assert [fill.quantity for fill in fills] == [0, 0]
    assert [fill.base_quantity for fill in fills] == [
        Decimal("0.001000000000"),
        Decimal("0.002000000000"),
    ]
    assert [fill.quote_notional for fill in fills] == [
        Decimal("10.0000"),
        Decimal("20.0000"),
    ]
    assert fills[0].idempotency_key != fills[1].idempotency_key
    engine.dispose()


def test_missing_cumulative_fractional_fill_preserves_scoped_total_cost() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="repair-cumulative-fractional-fill",
        broker_order_id="alpaca-cumulative-fill-1",
        symbol="BTC/USD",
        side="buy",
        status="filled",
        quantity=None,
        base_quantity=Decimal("0.0015"),
        quote_notional=Decimal("30.0000"),
        asset_class="crypto",
        venue="ALPACA",
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "broker_fill_id": "alpaca-cumulative-fill-1",
            "filled_base_quantity": "0.0015",
            "quote_notional": "30.0000",
            "average_fill_price": "20000",
            "filled_at": FILLED_AT.isoformat(),
            "settlement_currency": "USD",
            "total_transaction_cost": "0.02000000",
            "transaction_cost_coverage": "complete",
            "transaction_cost_source": "broker_payload:reported_order_total",
            "transaction_cost_scope": "cumulative",
        },
    )
    db.add(order)
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    proposed = dry_run["missing_filled_orders"][0]["proposed_fills"][0]

    assert proposed["quantity"] == 0
    assert proposed["base_quantity"] == "0.0015"
    assert proposed["quote_notional"] == "30.0000"
    assert proposed["transaction_cost_scope"] == "cumulative"
    assert proposed["settlement_currency"] == "USD"
    assert proposed["transaction_cost"] == "0.02000000"
    assert proposed["transaction_cost_coverage"] == "complete"

    result = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="restore cumulative fractional fill",
        now=FILLED_AT,
    )

    fill = db.scalar(select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id))
    assert result["summary"]["inserted_fill_count"] == 1
    assert fill is not None
    assert fill.quantity == 0
    assert fill.base_quantity == Decimal("0.001500000000")
    assert fill.quote_notional == Decimal("30.0000")
    assert fill.settlement_currency == "USD"
    assert fill.transaction_cost == Decimal("0.02000000")
    assert fill.transaction_cost_coverage == "complete"
    assert fill.transaction_cost_source == "broker_payload:reported_order_total"
    engine.dispose()


def test_missing_cumulative_fill_does_not_trust_unscoped_total_cost() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="repair-unscoped-cumulative-cost",
        broker_order_id="alpaca-unscoped-cumulative-1",
        symbol="ETH/USD",
        side="buy",
        status="filled",
        base_quantity=Decimal("0.01"),
        asset_class="crypto",
        venue="ALPACA",
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "filled_base_quantity": "0.01",
            "average_fill_price": "3000",
            "filled_at": FILLED_AT.isoformat(),
            "settlement_currency": "USD",
            "total_transaction_cost": "0.03",
            "transaction_cost_coverage": "complete",
        },
    )
    db.add(order)
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    proposed = dry_run["missing_filled_orders"][0]["proposed_fills"][0]

    assert proposed["transaction_cost"] is None
    assert proposed["transaction_cost_coverage"] == "unknown"
    assert proposed["transaction_cost_source"] == (
        "broker_payload:ambiguous_reconstructed_cumulative_cost_scope"
    )
    engine.dispose()


def test_duplicate_fill_detection_reports_retained_and_deleted_rows() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    first = ExecutionFill(
        id="a-duplicate-retained-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        filled_at=SOLD_AT,
    )
    second = ExecutionFill(
        id="b-duplicate-deleted-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        filled_at=SOLD_AT,
    )
    db.add_all([first, second])
    db.commit()

    payload = analyze_execution_fill_repairs(db=db, runtime_mode="paper")

    assert payload["summary"]["duplicate_fill_group_count"] == 1
    group = payload["duplicate_fill_groups"][0]
    assert group["symbol"] == "282330"
    assert group["retained_fill"]["id"] == first.id
    assert [row["id"] for row in group["duplicate_fills"]] == [second.id]
    engine.dispose()


def test_duplicate_repair_does_not_choose_richer_economics_without_broker_truth() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    retained = ExecutionFill(
        id="a-economics-retained-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        transaction_cost_coverage="unknown",
        filled_at=SOLD_AT,
    )
    richer_duplicate = ExecutionFill(
        id="b-economics-deleted-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("12.34567890"),
        transaction_cost_coverage="complete",
        transaction_cost_source="broker_payload:commission+fee+tax",
        filled_at=SOLD_AT,
    )
    db.add_all([retained, richer_duplicate])
    db.commit()

    result = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="preserve complete economics",
        now=SOLD_AT,
    )

    assert result["summary"]["deleted_fill_count"] == 0
    assert result["summary"]["applied_manual_review_count"] == 1
    remaining = db.scalars(
        select(ExecutionFill)
        .where(ExecutionFill.broker_order_pk == order.id)
        .order_by(ExecutionFill.id.asc())
    ).all()
    assert [fill.id for fill in remaining] == [retained.id, richer_duplicate.id]
    assert retained.settlement_currency is None
    assert retained.transaction_cost is None
    assert retained.transaction_cost_coverage == "unknown"
    assert richer_duplicate.transaction_cost == Decimal("12.34567890")
    assert dedupe_execution_fills(remaining) == []
    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "duplicate_fill_group",
            RuntimeRemediationEvent.subject_id == order.id,
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["conflicting_fields"] == [
        "settlement_currency",
        "transaction_cost",
        "transaction_cost_coverage",
        "transaction_cost_source",
    ]
    assert review.evidence_summary["fills_deleted"] == 0
    engine.dispose()


def test_duplicate_reader_preserves_most_conservative_chronology_evidence() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    unmarked = ExecutionFill(
        id="a-unmarked-chronology-fill",
        broker_order_pk=order.id,
        broker_fill_id="shared-chronology-fill",
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        transaction_cost_coverage="unknown",
        filled_at=SOLD_AT,
        payload={},
    )
    bounded = ExecutionFill(
        id="b-bounded-chronology-fill",
        broker_order_pk=order.id,
        broker_fill_id="shared-chronology-fill",
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        transaction_cost_coverage="unknown",
        filled_at=SOLD_AT,
        payload={
            "fill_time_precision": "observed_upper_bound",
            "fill_time_source": "reconciliation_observed_at",
        },
    )
    db.add_all([unmarked, bounded])
    db.commit()

    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.id.asc())).all()

    assert dedupe_execution_fills(fills) == [bounded]
    engine.dispose()


def test_duplicate_repair_preserves_conflicting_cost_rows_for_manual_review() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    first = ExecutionFill(
        id="a-conflicting-cost-retained-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("10"),
        transaction_cost_coverage="complete",
        transaction_cost_source="broker_payload:total_transaction_cost",
        filled_at=SOLD_AT,
    )
    second = ExecutionFill(
        id="b-conflicting-cost-deleted-fill",
        broker_order_pk=order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("72000"),
        settlement_currency="KRW",
        transaction_cost=Decimal("20"),
        transaction_cost_coverage="complete",
        transaction_cost_source="broker_payload:total_transaction_cost",
        filled_at=SOLD_AT,
    )
    db.add_all([first, second])
    db.commit()

    result = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="reject conflicting economics",
        now=SOLD_AT,
    )

    remaining = db.scalars(
        select(ExecutionFill)
        .where(ExecutionFill.broker_order_pk == order.id)
        .order_by(ExecutionFill.id.asc())
    ).all()
    assert [fill.id for fill in remaining] == [first.id, second.id]
    assert [fill.transaction_cost for fill in remaining] == [Decimal("10"), Decimal("20")]
    assert dedupe_execution_fills(remaining) == []
    assert result["summary"]["deleted_fill_count"] == 0
    assert result["summary"]["applied_manual_review_count"] == 1
    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "duplicate_fill_group",
            RuntimeRemediationEvent.subject_id == order.id,
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["conflicting_fields"] == ["transaction_cost"]
    assert review.evidence_summary["fills_deleted"] == 0
    engine.dispose()


def test_dry_run_command_outputs_missing_and_duplicate_repairs(tmp_path: Path) -> None:
    db_path = tmp_path / "repair.db"
    settings = _settings(f"sqlite:///{db_path}")
    engine, db = _session(settings)
    missing = _filled_order_missing_fill()
    duplicate = _duplicate_fill_order()
    db.add_all([missing, duplicate])
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id="a-dry-run-duplicate-retained-fill",
                broker_order_pk=duplicate.id,
                symbol="282330",
                side="buy",
                quantity=1,
                price=Decimal("72000"),
                filled_at=SOLD_AT,
            ),
            ExecutionFill(
                id="b-dry-run-duplicate-deleted-fill",
                broker_order_pk=duplicate.id,
                symbol="282330",
                side="buy",
                quantity=1,
                price=Decimal("72000"),
                filled_at=SOLD_AT,
            ),
        ]
    )
    db.commit()
    db.close()
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            f"sqlite:///{db_path}",
            "--runtime-mode",
            "paper",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["mode"] == "dry-run"
    assert payload["summary"]["missing_filled_order_count"] == 1
    assert payload["missing_filled_orders"][0]["broker_order_id"] == "missing-buy-1"
    assert payload["summary"]["duplicate_fill_group_count"] == 1
    assert payload["summary"]["proposed_duplicate_fill_isolation_count"] == 1


def test_live_fill_repair_requires_explicit_database_url_source(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--runtime-mode",
            "live",
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "KAI_EXEC_DATABASE_URL": "",
        },
    )

    assert result.returncode != 0
    assert "requires KAI_EXEC_DATABASE_URL or --database-url" in result.stderr
    assert not (tmp_path / "must-not-open.db").exists()


def test_fill_remediation_uses_environment_runtime_and_database_when_cli_values_omitted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "live-fill-remediation.db"
    database_url = f"sqlite:///{db_path}"
    settings = Settings(database_url=database_url, runtime_mode="live")
    engine, db = _session(settings)
    order = _filled_order_missing_fill()
    order.mode = "live"
    db.add(order)
    db.commit()
    db.close()
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "KAI_EXEC_RUNTIME_MODE": "live",
            "KAI_EXEC_DATABASE_URL": database_url,
        },
    )
    payload = json.loads(result.stdout)

    assert payload["runtime_mode"] == "live"
    assert payload["summary"]["missing_filled_order_count"] == 1
    assert payload["missing_filled_orders"][0]["broker_order_id"] == "missing-buy-1"


def test_fill_remediation_rejects_password_in_database_argv(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            "postgresql+psycopg://operator:secret@db.example/exec",
            "--runtime-mode",
            "live",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "KAI_EXEC_DATABASE_URL": ""},
    )

    assert result.returncode != 0
    assert "KAI_EXEC_DATABASE_URL environment variable" in result.stderr
    assert not (tmp_path / "must-not-open.db").exists()


def test_fill_remediation_rejects_query_embedded_database_secret_before_db_open(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "must-not-open.db"
    secret = "must-not-leak-fill-secret"
    database_url = (
        f"sqlite:///{database_path}?auth={secret}"
    )
    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            database_url,
            "--runtime-mode",
            "live",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "KAI_EXEC_DATABASE_URL": ""},
    )

    assert result.returncode != 0
    assert "KAI_EXEC_DATABASE_URL environment variable" in result.stderr
    assert secret not in result.stdout
    assert secret not in result.stderr
    assert not database_path.exists()


@pytest.mark.parametrize(
    ("actor", "reason"),
    [("   ", "valid reason"), ("Peter", "\t  ")],
)
def test_fill_remediation_rejects_blank_audit_metadata_before_opening_database(
    tmp_path: Path,
    actor: str,
    reason: str,
) -> None:
    database_path = tmp_path / "must-not-open.db"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            f"sqlite:///{database_path}",
            "--runtime-mode",
            "paper",
            "--apply",
            "--actor",
            actor,
            "--reason",
            reason,
        ],
        capture_output=True,
        text=True,
        env=os.environ,
    )

    assert result.returncode != 0
    assert "--apply requires --actor and --reason" in result.stderr
    assert not database_path.exists()


def test_fill_remediation_cli_trims_applied_audit_metadata(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'fill-remediation.db'}"
    settings = _settings(database_url)
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="trim-audit-metadata",
        broker_order_id="trim-audit-metadata-1",
        symbol="001800",
        side="buy",
        status="filled",
        quantity=5,
        submitted_at=FILLED_AT,
    )
    db.add(order)
    db.commit()
    db.close()
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            database_url,
            "--runtime-mode",
            "paper",
            "--apply",
            "--actor",
            "  Peter  ",
            "--reason",
            "  authoritative fill evidence unavailable  ",
        ],
        capture_output=True,
        text=True,
        env=os.environ,
    )

    assert result.returncode == 0, result.stderr
    engine = create_db_engine(settings)
    with create_session_factory(engine)() as db:
        event = db.scalar(select(RuntimeRemediationEvent))
        assert event is not None
        assert event.actor == "Peter"
        assert event.reason == "authoritative fill evidence unavailable"
    engine.dispose()


def test_legacy_duplicate_fill_apply_is_disabled(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_duplicate_execution_fills.py",
            str(tmp_path / "must-not-open.db"),
            "--apply",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "destructive duplicate-fill apply is disabled" in result.stderr
    assert not (tmp_path / "must-not-open.db").exists()


def test_apply_is_idempotent_auditable_and_repairs_fifo_pair() -> None:
    settings = _settings()
    engine, db = _session(settings)
    missing_buy = _filled_order_missing_fill()
    sell_order = BrokerOrder(
        broker_name="kis",
        venue="KRX",
        asset_class="stock",
        mode="paper",
        idempotency_key="repair-105560-sell",
        broker_order_id="sell-1",
        symbol="105560",
        side="sell",
        status="filled",
        quantity=1,
        submitted_at=SOLD_AT,
            request_metadata={
                "exchange": "KRX",
                "broker_account_scope": {
                "policy_version": 2,
                "scope": FIFO_TEST_ACCOUNT_SCOPE,
            }
        },
    )
    duplicate_order = _duplicate_fill_order()
    for order in (missing_buy, sell_order, duplicate_order):
        order.venue = order.venue or "KRX"
        order.asset_class = order.asset_class or "stock"
        _bind_order_to_runtime_scope(order, settings=settings)
    db.add_all([missing_buy, sell_order, duplicate_order])
    db.flush()
    sell_fill = ExecutionFill(
        broker_order_pk=sell_order.id,
        symbol="105560",
        side="sell",
        quantity=1,
        price=Decimal("72000"),
        filled_at=SOLD_AT,
    )
    duplicate_one = ExecutionFill(
        id="a-apply-duplicate-retained-fill",
        broker_order_pk=duplicate_order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    duplicate_two = ExecutionFill(
        id="b-apply-duplicate-deleted-fill",
        broker_order_pk=duplicate_order.id,
        symbol="282330",
        side="buy",
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    db.add_all([sell_fill, duplicate_one, duplicate_two])
    db.commit()

    before = daily_pnl_payload(settings=settings, db=db, now=SOLD_AT)
    first = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="issue-194 test repair",
        now=SOLD_AT,
    )
    second = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="issue-194 test repair",
        now=SOLD_AT,
    )
    after = daily_pnl_payload(settings=settings, db=db, now=SOLD_AT)

    assert before["unmatched_sell_quantity"] == 1
    assert first["summary"]["inserted_fill_count"] == 1
    assert first["summary"]["deleted_fill_count"] == 0
    assert first["summary"]["isolated_duplicate_fill_group_count"] == 1
    assert first["summary"]["audit_event_count"] == 2
    assert second["summary"]["inserted_fill_count"] == 0
    assert second["summary"]["deleted_fill_count"] == 0
    assert after["matched_sell_quantity"] == 1
    assert after["unmatched_sell_quantity"] == 0
    assert after["daily_realized_pnl"] == "1000.0000"
    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == MISSING_FILL_REPAIR_EVENT
        )
    ).all()
    assert [event.event_type for event in events] == [MISSING_FILL_REPAIR_EVENT]
    assert len(db.scalars(select(ExecutionFill)).all()) == 4
    duplicate_review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "duplicate_fill_group"
        )
    )
    assert duplicate_review is not None
    assert duplicate_review.outcome == "non_destructive_isolation"
    assert duplicate_review.evidence_summary["fills_deleted"] == 0
    assert duplicate_review.evidence_summary["duplicate_fill_ids"] == [duplicate_two.id]
    engine.dispose()


def test_duplicate_isolation_review_is_versioned_when_group_membership_changes() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    retained = ExecutionFill(
        id="a-membership-retained-fill",
        broker_order_pk=order.id,
        broker_fill_id="membership-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    first_duplicate = ExecutionFill(
        id="b-membership-duplicate-fill",
        broker_order_pk=order.id,
        broker_fill_id="membership-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    db.add_all([retained, first_duplicate])
    db.commit()

    first = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="isolate first duplicate membership",
        now=SOLD_AT,
    )
    first_group = first["duplicate_fill_groups"][0]
    first_candidate_key = first_group["candidate_key"]
    assert first_group["review_outcome"] == "non_destructive_isolation"

    second_duplicate = ExecutionFill(
        id="c-membership-duplicate-fill",
        broker_order_pk=order.id,
        broker_fill_id="membership-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    db.add(second_duplicate)
    db.commit()

    enlarged = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    enlarged_group = enlarged["duplicate_fill_groups"][0]
    assert enlarged_group["candidate_key"] != first_candidate_key
    assert "review_outcome" not in enlarged_group

    second = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="isolate enlarged duplicate membership",
        now=SOLD_AT + timedelta(minutes=1),
    )

    reviews = db.scalars(
        select(RuntimeRemediationEvent)
        .where(RuntimeRemediationEvent.candidate_type == "duplicate_fill_group")
        .order_by(RuntimeRemediationEvent.occurred_at.asc())
    ).all()
    assert second["summary"]["isolated_duplicate_fill_group_count"] == 1
    assert len(reviews) == 2
    assert reviews[0].candidate_key != reviews[1].candidate_key
    assert reviews[0].evidence_summary["duplicate_fill_ids"] == [first_duplicate.id]
    assert reviews[1].evidence_summary["duplicate_fill_ids"] == [
        first_duplicate.id,
        second_duplicate.id,
    ]
    assert len(db.scalars(select(ExecutionFill)).all()) == 3
    engine.dispose()


def test_conflicting_duplicate_broker_fill_requires_manual_review_without_isolation() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    retained = ExecutionFill(
        id="a-conflicting-retained-fill",
        broker_order_pk=order.id,
        broker_fill_id="conflicting-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        base_quantity=Decimal("1.0"),
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    conflicting = ExecutionFill(
        id="b-conflicting-duplicate-fill",
        broker_order_pk=order.id,
        broker_fill_id="conflicting-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=2,
        base_quantity=Decimal("2.0"),
        price=Decimal("74000"),
        filled_at=SOLD_AT,
    )
    db.add_all([retained, conflicting])
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    applied = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="duplicate broker fill has conflicting economics",
        now=SOLD_AT,
    )

    dry_run_group = dry_run["duplicate_fill_groups"][0]
    applied_group = applied["duplicate_fill_groups"][0]
    assert dry_run_group["economic_conflict"] is True
    assert dry_run_group["conflicting_fields"] == [
        "quantity",
        "base_quantity",
        "price",
    ]
    assert dry_run_group["repair_action"] == "record_manual_review"
    assert applied["summary"]["applied_duplicate_isolation_count"] == 0
    assert applied["summary"]["applied_manual_review_count"] == 1
    assert applied["summary"]["isolated_duplicate_fill_group_count"] == 0
    assert applied["summary"]["manual_review_duplicate_fill_group_count"] == 1
    assert applied_group["review_outcome"] == "manual_review_required"
    assert len(db.scalars(select(ExecutionFill)).all()) == 2

    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "duplicate_fill_group"
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["blocker"] == "duplicate_fill_economic_conflict"
    assert review.evidence_summary["fills_deleted"] == 0
    assert review.evidence_summary["fills_fabricated"] == 0
    assert review.evidence_summary["orders_submitted"] == 0
    assert review.evidence_summary["duplicate_fill_ids"] == [conflicting.id]
    engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "conflicting_value"),
    [("symbol", "005930"), ("side", "sell")],
)
def test_duplicate_fill_identity_detects_normalized_route_fact_conflict(
    field_name: str,
    conflicting_value: str,
) -> None:
    order = _duplicate_fill_order()
    first = ExecutionFill(
        broker_order=order,
        broker_fill_id="same-normalized-route-fill",
        symbol=f" {order.symbol.lower()} ",
        side=f" {order.side.upper()} ",
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    second = ExecutionFill(
        broker_order=order,
        broker_fill_id="same-normalized-route-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    setattr(second, field_name, conflicting_value)

    assert duplicate_fill_conflicting_fields([first, second]) == [field_name]


def test_conflicting_duplicate_fill_reader_does_not_silently_canonicalize() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    first = ExecutionFill(
        broker_order_pk=order.id,
        broker_fill_id="reader-conflict-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        base_quantity=Decimal("1.0"),
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    second = ExecutionFill(
        broker_order_pk=order.id,
        broker_fill_id="reader-conflict-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        base_quantity=Decimal("1.0"),
        price=Decimal("74000"),
        filled_at=SOLD_AT,
    )
    db.add_all([first, second])
    db.commit()

    quality = broker_order_fill_quality_payload(settings=settings, db=db)
    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.id.asc())).all()

    assert dedupe_execution_fills(fills) == []
    assert quality["duplicate_fill_group_count"] == 1
    assert quality["duplicate_fill_groups"][0]["economic_conflict"] is True
    assert quality["duplicate_fill_groups"][0]["conflicting_fields"] == ["price"]
    engine.dispose()


def test_quote_notional_conflict_requires_manual_review_and_is_excluded_from_readers() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = _duplicate_fill_order()
    db.add(order)
    db.flush()
    first = ExecutionFill(
        id="a-quote-notional-conflict-fill",
        broker_order_pk=order.id,
        broker_fill_id="quote-notional-conflict-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        base_quantity=Decimal("1.0"),
        quote_notional=None,
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    second = ExecutionFill(
        id="b-quote-notional-conflict-fill",
        broker_order_pk=order.id,
        broker_fill_id="quote-notional-conflict-fill",
        symbol=order.symbol,
        side=order.side,
        quantity=1,
        base_quantity=Decimal("1.0"),
        quote_notional=Decimal("73000"),
        price=Decimal("73000"),
        filled_at=SOLD_AT,
    )
    db.add_all([first, second])
    db.commit()

    dry_run = analyze_execution_fill_repairs(db=db, runtime_mode="paper")
    applied = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="duplicate broker fill has conflicting quote notional",
        now=SOLD_AT,
    )
    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.id.asc())).all()
    quality = broker_order_fill_quality_payload(settings=settings, db=db)

    assert dedupe_execution_fills(fills) == []
    assert dry_run["duplicate_fill_groups"][0]["economic_conflict"] is True
    assert dry_run["duplicate_fill_groups"][0]["conflicting_fields"] == ["quote_notional"]
    assert dry_run["duplicate_fill_groups"][0]["repair_action"] == "record_manual_review"
    assert applied["summary"]["applied_duplicate_isolation_count"] == 0
    assert applied["summary"]["applied_manual_review_count"] == 1
    assert quality["duplicate_fill_groups"][0]["economic_conflict"] is True
    assert quality["duplicate_fill_groups"][0]["conflicting_fields"] == ["quote_notional"]
    engine.dispose()


def test_apply_records_manual_review_when_authoritative_fill_evidence_is_absent() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="repair-missing-no-evidence",
        broker_order_id="missing-no-evidence-1",
        symbol="001800",
        side="buy",
        status="filled",
        quantity=5,
        submitted_at=FILLED_AT,
    )
    db.add(order)
    db.commit()

    first = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="Peter",
        reason="issue #265: authoritative broker fill evidence unavailable",
        now=SOLD_AT,
    )
    second = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="Peter",
        reason="issue #265: authoritative broker fill evidence unavailable",
        now=SOLD_AT,
    )

    assert first["summary"]["manual_review_missing_fill_count"] == 1
    assert first["summary"]["applied_manual_review_count"] == 1
    assert second["summary"]["applied_manual_review_count"] == 0
    assert db.scalars(select(ExecutionFill)).all() == []
    reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "missing_execution_fill"
        )
    ).all()
    assert len(reviews) == 1
    assert reviews[0].actor == "Peter"
    assert reviews[0].outcome == "manual_review_required"
    assert reviews[0].evidence_summary["fills_fabricated"] == 0
    fill_quality = broker_order_fill_quality_payload(settings=settings, db=db)
    assert fill_quality["missing_filled_order_fill_count"] == 1
    assert fill_quality["missing_filled_order_fill_unreviewed_count"] == 0
    assert fill_quality["missing_filled_order_fill_manual_review_count"] == 1
    assert fill_quality["missing_filled_order_fill_items"][0]["review_actor"] == "Peter"
    engine.dispose()


def test_apply_rejects_colliding_no_id_partial_fills_atomically() -> None:
    settings = _settings()
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="repair-colliding-no-id-partials",
        broker_order_id="colliding-no-id-partials-1",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=2,
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "fills": [
                {
                    "symbol": "105560",
                    "side": "buy",
                    "quantity": 1,
                    "price": "71000",
                    "filled_at": FILLED_AT.isoformat(),
                },
                {
                    "symbol": "105560",
                    "side": "buy",
                    "quantity": 1,
                    "price": "71000",
                    "filled_at": FILLED_AT.isoformat(),
                },
            ]
        },
    )
    db.add(order)
    db.commit()

    result = apply_execution_fill_repairs(
        db=db,
        runtime_mode="paper",
        actor="Peter",
        reason="colliding no-ID fills require manual review",
        now=SOLD_AT,
    )

    assert result["summary"]["inserted_fill_count"] == 0
    assert result["summary"]["applied_manual_review_count"] == 1
    assert db.scalars(select(ExecutionFill)).all() == []
    assert db.scalars(select(BrokerOrderEvent)).all() == []
    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "missing_execution_fill"
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["blocker"] == (
        "proposed_fill_set_not_insertable_atomically"
    )
    validation = review.evidence_summary["validation"]
    assert validation["expected_quantity"] == 2
    assert validation["proposed_quantity"] == 2
    assert validation["retained_quantity"] == 1
    assert len(validation["duplicate_signatures"]) == 1
    assert validation["existing_signature_collisions"] == []
    assert review.evidence_summary["fills_fabricated"] == 0
    assert review.evidence_summary["orders_submitted"] == 0
    engine.dispose()


def _add_fill(
    db,
    *,
    symbol: str,
    side: str,
    quantity: int,
    price: str,
    filled_at: datetime,
    suffix: str,
    exchange: str = "KRX",
    settings: Settings | None = None,
) -> ExecutionFill:
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"fifo-{symbol}-{side}-{suffix}",
        broker_order_id=f"fifo-order-{suffix}",
        symbol=symbol,
        side=side,
        status="filled",
        quantity=quantity,
        submitted_at=filled_at,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": exchange,
            "broker_account_scope": {
                "policy_version": 2,
                "scope": FIFO_TEST_ACCOUNT_SCOPE,
            },
        },
    )
    if settings is not None:
        _bind_order_to_runtime_scope(
            order,
            settings=settings,
            exchange=exchange,
        )
    db.add(order)
    db.flush()
    fill = ExecutionFill(
        broker_order_pk=order.id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=Decimal(price),
        filled_at=filled_at,
    )
    db.add(fill)
    return fill


def test_fifo_residual_dry_run_reports_required_broker_flat_evidence() -> None:
    settings = _settings()
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    reconciled_at = fill_at + timedelta(days=1)
    _add_fill(
        db,
        symbol="049430",
        side="buy",
        quantity=45,
        price="1000",
        filled_at=fill_at,
        suffix="dry-run-buy",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider="kis",
            account_scope=FIFO_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="049430",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=reconciled_at,
            raw_payload=_fifo_position_payload(),
        )
    )
    db.commit()

    payload = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")

    assert payload["mode"] == "dry-run"
    assert payload["summary"] == {
        "broker_flat_residual_count": 1,
        "eligible_isolation_count": 1,
        "recency_blocked_count": 0,
        "applied_isolation_count": 0,
        "manual_review_count": 0,
        "unreviewed_recency_blocked_count": 0,
        "applied_blocker_count": 0,
    }
    assert payload["broker_flat_fifo_residuals"][0] == {
        "symbol": "049430",
        "residual_quantity": "45.0000",
        "last_local_fill": fill_at.isoformat(),
        "broker_reconciliation_timestamp": reconciled_at.isoformat(),
        "broker_position_id": db.scalar(select(ExecutionPosition.id)),
        "broker_position_quantity": "0.0000",
            "execution_scope": {
                "provider": "kis",
                "account_scope": FIFO_TEST_ACCOUNT_SCOPE,
                "venue": "KRX",
                "asset_class": "stock",
                "exchange": "KRX",
            },
        "eligible": True,
        "repair_action": "isolate",
        "reason": "broker_flat_evidence_newer_than_last_local_fill",
    }
    assert db.scalars(select(FifoInventoryIsolation)).all() == []
    engine.dispose()


def test_fifo_residual_command_defaults_to_dry_run(tmp_path: Path) -> None:
    db_path = tmp_path / "fifo-repair.db"
    settings = _settings(f"sqlite:///{db_path}")
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    _add_fill(
        db,
        symbol="049430",
        side="buy",
        quantity=45,
        price="1000",
        filled_at=fill_at,
        suffix="command-buy",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider="kis",
            account_scope=FIFO_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="049430",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=fill_at + timedelta(hours=1),
            raw_payload=_fifo_position_payload(),
        )
    )
    db.commit()
    db.close()
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/repair_execution_fill_evidence.py",
            "--database-url",
            f"sqlite:///{db_path}",
            "--runtime-mode",
            "paper",
            "--scope",
            "fifo-residuals",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["mode"] == "dry-run"
    assert payload["summary"]["eligible_isolation_count"] == 1
    assert payload["broker_flat_fifo_residuals"][0]["symbol"] == "049430"


def test_fifo_isolation_preserves_repurchase_after_verified_flat_boundary() -> None:
    settings = _settings()
    engine, db = _session(settings)
    old_buy_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = old_buy_at + timedelta(hours=1)
    _add_fill(
        db,
        symbol="001450",
        side="buy",
        quantity=11,
        price="100",
        filled_at=old_buy_at,
        suffix="old-buy",
        settings=settings,
    )
    active_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=active_identity.provider,
            account_scope=active_identity.account_scope,
            exchange=active_identity.exchange,
            symbol="001450",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=flat_at,
            raw_payload=payload_with_position_identity(
                None,
                identity=active_identity,
            ),
        )
    )
    db.commit()

    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="verified broker flat",
        now=flat_at,
    )
    repeated = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="verified broker flat",
        now=flat_at,
    )
    repurchase_at = flat_at + timedelta(hours=1)
    sell_at = repurchase_at + timedelta(minutes=30)
    _add_fill(
        db,
        symbol="001450",
        side="buy",
        quantity=1,
        price="200",
        filled_at=repurchase_at,
        suffix="repurchase",
        settings=settings,
    )
    _add_fill(
        db,
        symbol="001450",
        side="sell",
        quantity=1,
        price="250",
        filled_at=sell_at,
        suffix="repurchase-sell",
        settings=settings,
    )
    db.commit()

    pnl = daily_pnl_payload(settings=settings, db=db, now=sell_at)
    isolation = db.scalar(select(FifoInventoryIsolation))

    assert applied["summary"]["applied_isolation_count"] == 1
    assert repeated["summary"]["applied_isolation_count"] == 0
    assert isolation is not None
    assert isolation.residual_quantity == Decimal("11")
    assert isolation.audit_payload["fills_deleted"] == 0
    assert isolation.audit_payload["fills_fabricated"] == 0
    assert pnl["matched_sell_quantity"] == 1
    assert pnl["daily_realized_pnl"] == "50.0000"
    assert len(db.scalars(select(ExecutionFill)).all()) == 3
    engine.dispose()


def test_fifo_repair_starts_clean_generation_after_ambiguous_history_and_exact_flat() -> None:
    settings = _settings()
    engine, db = _session(settings)
    ambiguous_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = ambiguous_at + timedelta(hours=1)
    _add_fill(
        db,
        symbol="001451",
        side="buy",
        quantity=1,
        price="100",
        filled_at=ambiguous_at,
        suffix="ambiguous-buy",
        settings=settings,
    )
    _add_fill(
        db,
        symbol="001451",
        side="sell",
        quantity=1,
        price="110",
        filled_at=ambiguous_at,
        suffix="ambiguous-sell",
        settings=settings,
    )
    active_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=active_identity.provider,
            account_scope=active_identity.account_scope,
            exchange=active_identity.exchange,
            symbol="001451",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=flat_at,
            raw_payload=payload_with_position_identity(
                None,
                identity=active_identity,
            ),
        )
    )
    db.commit()

    dry_run = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")
    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="exact broker flat resets ambiguous generation",
        now=flat_at,
    )
    historical = daily_pnl_payload(
        settings=settings,
        db=db,
        now=ambiguous_at + timedelta(minutes=30),
    )

    assert dry_run["summary"]["eligible_isolation_count"] == 1
    candidate = dry_run["broker_flat_fifo_residuals"][0]
    assert candidate["eligible"] is True
    assert candidate["execution_scope"] == {
        "provider": "kis",
        "account_scope": active_identity.account_scope,
        "venue": "KRX",
        "asset_class": "stock",
        "exchange": "KRX",
    }
    assert candidate["reason"] == (
        "broker_flat_evidence_resets_ambiguous_generation"
    )
    assert applied["summary"]["applied_isolation_count"] == 1
    assert historical["daily_realized_pnl"] is None
    assert historical["calculation_reason"] == "indeterminate_fill_chronology"

    repurchase_at = ambiguous_at + timedelta(days=1)
    sell_at = repurchase_at + timedelta(hours=1)
    _add_fill(
        db,
        symbol="001451",
        side="buy",
        quantity=1,
        price="200",
        filled_at=repurchase_at,
        suffix="clean-generation-buy",
        settings=settings,
    )
    _add_fill(
        db,
        symbol="001451",
        side="sell",
        quantity=1,
        price="250",
        filled_at=sell_at,
        suffix="clean-generation-sell",
        settings=settings,
    )
    db.commit()

    current = daily_pnl_payload(
        settings=settings,
        db=db,
        now=sell_at + timedelta(minutes=1),
    )

    assert current["daily_realized_pnl"] == "50.0000"
    assert current["calculation_status"] == "computed"
    engine.dispose()


def test_fifo_isolation_recency_gate_blocks_flat_evidence_older_than_repurchase() -> None:
    settings = _settings()
    engine, db = _session(settings)
    old_buy_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = old_buy_at + timedelta(hours=1)
    repurchase_at = flat_at + timedelta(hours=1)
    _add_fill(
        db,
        symbol="055550",
        side="buy",
        quantity=7,
        price="100",
        filled_at=old_buy_at,
        suffix="pre-flat-buy",
    )
    _add_fill(
        db,
        symbol="055550",
        side="buy",
        quantity=2,
        price="110",
        filled_at=repurchase_at,
        suffix="post-flat-repurchase",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider="kis",
            account_scope=FIFO_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="055550",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=flat_at,
            raw_payload=_fifo_position_payload(),
        )
    )
    db.commit()

    dry_run = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")
    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="must not isolate repurchase",
    )

    item = dry_run["broker_flat_fifo_residuals"][0]
    assert item["residual_quantity"] == "9.0000"
    assert item["last_local_fill"] == repurchase_at.isoformat()
    assert item["eligible"] is False
    assert item["reason"] == "broker_flat_evidence_not_newer_than_last_local_fill"
    assert applied["summary"]["applied_isolation_count"] == 0
    assert applied["summary"]["applied_blocker_count"] == 1
    assert applied["summary"]["manual_review_count"] == 1
    assert db.scalars(select(FifoInventoryIsolation)).all() == []
    review = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type == "fifo_inventory_residual"
        )
    )
    assert review is not None
    assert review.outcome == "manual_review_required"
    assert review.evidence_summary["fills_deleted"] == 0
    engine.dispose()


def test_fifo_isolation_selects_exact_position_route_for_same_symbol() -> None:
    settings = _settings()
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    reconciled_at = fill_at + timedelta(hours=1)
    _add_fill(
        db,
        symbol="049430",
        side="buy",
        quantity=3,
        price="1000",
        filled_at=fill_at,
        suffix="route-isolated-buy",
    )
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                provider="kis",
                account_scope=FIFO_TEST_ACCOUNT_SCOPE,
                exchange="KRX",
                symbol="049430",
                quantity=0,
                updated_from="reconciliation",
                last_reconciled_at=reconciled_at,
                raw_payload=_fifo_position_payload(),
            ),
            ExecutionPosition(
                mode="paper",
                provider="kis",
                account_scope=FIFO_TEST_ACCOUNT_SCOPE,
                exchange="NYSE",
                symbol="049430",
                quantity=10,
                updated_from="reconciliation",
                last_reconciled_at=reconciled_at,
                raw_payload=payload_with_position_identity(
                    None,
                    identity=PositionIdentity(
                        provider="kis",
                        account_scope=FIFO_TEST_ACCOUNT_SCOPE,
                        exchange="NYSE",
                    ),
                ),
            ),
        ]
    )
    db.commit()

    payload = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")

    assert payload["summary"]["eligible_isolation_count"] == 1
    assert payload["broker_flat_fifo_residuals"][0]["execution_scope"] == {
        "provider": "kis",
        "account_scope": FIFO_TEST_ACCOUNT_SCOPE,
        "venue": "KRX",
        "asset_class": "stock",
        "exchange": "KRX",
    }
    assert payload["broker_flat_fifo_residuals"][0]["eligible"] is True
    engine.dispose()


def test_fifo_isolation_repair_fails_closed_for_legacy_unscoped_evidence() -> None:
    settings = _settings()
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = fill_at + timedelta(hours=1)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-unscoped-fifo-buy",
        symbol="009999",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=fill_at,
        venue="KRX",
        asset_class="stock",
    )
    db.add_all(
        [
            order,
            ExecutionPosition(
                mode="paper",
                symbol="009999",
                quantity=0,
                updated_from="reconciliation",
                last_reconciled_at=flat_at,
            ),
        ]
    )
    db.flush()
    db.add(
        ExecutionFill(
            broker_order_pk=order.id,
            symbol="009999",
            side="buy",
            quantity=1,
            price=Decimal("100"),
            filled_at=fill_at,
        )
    )
    db.commit()

    dry_run = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")
    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="scope must be proven",
        now=flat_at,
    )

    item = dry_run["broker_flat_fifo_residuals"][0]
    assert item["eligible"] is False
    assert item["execution_scope"] is None
    assert item["reason"] == "execution_scope_unavailable"
    assert applied["summary"]["applied_isolation_count"] == 0
    assert applied["summary"]["applied_blocker_count"] == 1
    assert db.scalars(select(FifoInventoryIsolation)).all() == []
    engine.dispose()


def test_fifo_repair_starts_exact_krx_generation_after_legacy_flat_rebind() -> None:
    settings = _settings()
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = fill_at + timedelta(hours=1)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-unscoped-before-exact-flat",
        symbol="009997",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=fill_at,
        venue="KRX",
        asset_class="stock",
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    db.add_all(
        [
            order,
            ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="009997",
                quantity=0,
                base_quantity=Decimal("0"),
                updated_from="reconciliation",
                last_reconciled_at=flat_at,
                raw_payload=payload_with_position_identity(None, identity=identity),
            ),
        ]
    )
    db.flush()
    db.add(
        ExecutionFill(
            broker_order_pk=order.id,
            symbol="009997",
            side="buy",
            quantity=1,
            price=Decimal("100"),
            filled_at=fill_at,
        )
    )
    _add_fill(
        db,
        symbol="009997",
        side="buy",
        quantity=1,
        price="101",
        filled_at=fill_at + timedelta(minutes=1),
        suffix="exact-scope-before-flat",
        settings=settings,
    )
    db.commit()

    dry_run = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")
    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="exact current KRX flat truth starts a clean generation",
        now=flat_at,
    )
    repeated = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="exact current KRX flat truth starts a clean generation",
        now=flat_at,
    )
    after = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")

    assert dry_run["summary"]["eligible_isolation_count"] == 2
    item = next(
        row
        for row in dry_run["broker_flat_fifo_residuals"]
        if row["reason"] == "exact_krx_flat_resets_legacy_generation"
    )
    assert item["eligible"] is True
    assert item["reason"] == "exact_krx_flat_resets_legacy_generation"
    assert item["execution_scope"] == {
        "provider": "kis",
        "account_scope": identity.account_scope,
        "venue": "KRX",
        "asset_class": "stock",
        "exchange": "KRX",
    }
    assert applied["summary"]["applied_isolation_count"] == 1
    assert repeated["summary"]["applied_isolation_count"] == 0
    assert after["summary"]["broker_flat_residual_count"] == 0
    isolation = db.scalar(select(FifoInventoryIsolation))
    assert isolation is not None
    assert isolation.symbol == "009997"
    assert isolation.account_scope == identity.account_scope
    assert isolation.residual_quantity == Decimal("2")
    assert isolation.audit_payload["coalesced_candidate_count"] == 2
    assert len(isolation.audit_payload["coalesced_evidence"]) == 2
    assert isolation.audit_payload["fills_deleted"] == 0
    assert isolation.audit_payload["fills_fabricated"] == 0
    engine.dispose()


def test_fifo_repair_reports_duplicate_legacy_position_scope_as_ambiguous() -> None:
    settings = _settings()
    engine, db = _session(settings)
    fill_at = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
    flat_at = fill_at + timedelta(hours=1)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="duplicate-legacy-position-fifo-buy",
        symbol="009998",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=fill_at,
        venue="KRX",
        asset_class="stock",
    )
    # Simulate a pre-constraint/corrupt legacy database so the repair audit can
    # prove it never selects one duplicate by query order.
    db.execute(text("DROP INDEX uq_execution_positions_legacy_symbol_mode"))
    db.add_all(
        [
            order,
            ExecutionPosition(
                id="legacy-position-a",
                mode="paper",
                symbol="009998",
                quantity=0,
                updated_from="reconciliation",
                last_reconciled_at=flat_at,
            ),
            ExecutionPosition(
                id="legacy-position-b",
                mode="paper",
                symbol="009998",
                quantity=0,
                updated_from="reconciliation",
                last_reconciled_at=flat_at + timedelta(seconds=1),
            ),
        ]
    )
    db.flush()
    db.add(
        ExecutionFill(
            broker_order_pk=order.id,
            symbol="009998",
            side="buy",
            quantity=1,
            price=Decimal("100"),
            filled_at=fill_at,
        )
    )
    db.commit()

    dry_run = analyze_fifo_inventory_repairs(db=db, runtime_mode="paper")
    applied = apply_fifo_inventory_repairs(
        db=db,
        runtime_mode="paper",
        actor="test-operator",
        reason="duplicate legacy route needs review",
        now=flat_at + timedelta(hours=1),
    )

    item = dry_run["broker_flat_fifo_residuals"][0]
    assert item["eligible"] is False
    assert item["reason"] == "legacy_position_route_ambiguous"
    assert item["broker_position_ids"] == [
        "legacy-position-a",
        "legacy-position-b",
    ]
    assert applied["summary"]["applied_isolation_count"] == 0
    assert applied["summary"]["applied_blocker_count"] == 1
    assert db.scalars(select(FifoInventoryIsolation)).all() == []
    engine.dispose()
