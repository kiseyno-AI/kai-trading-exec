from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPosition,
    FifoInventoryIsolation,
)
from app.services.execution_fill_repair import (
    DUPLICATE_FILL_REPAIR_EVENT,
    MISSING_FILL_REPAIR_EVENT,
    analyze_fifo_inventory_repairs,
    analyze_execution_fill_repairs,
    apply_fifo_inventory_repairs,
    apply_execution_fill_repairs,
)
from app.services.runtime import daily_pnl_payload


FILLED_AT = datetime(2026, 7, 24, 0, 5, 1, tzinfo=UTC)
SOLD_AT = datetime(2026, 7, 24, 1, 0, 0, tzinfo=UTC)


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _settings(database_url: str = "sqlite:///:memory:") -> Settings:
    return Settings(database_url=database_url, runtime_mode="paper")


def _filled_order_missing_fill() -> BrokerOrder:
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="repair-missing-105560-buy",
        broker_order_id="missing-buy-1",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=FILLED_AT,
        raw_broker_payload={
            "order": {
                "odno": "missing-buy-1",
                "pdno": "105560",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "avg_pric": "71000",
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
    assert [row["id"] for row in group["deleted_fills"]] == [second.id]
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
    assert payload["summary"]["proposed_duplicate_fill_delete_count"] == 1


def test_apply_is_idempotent_auditable_and_repairs_fifo_pair() -> None:
    settings = _settings()
    engine, db = _session(settings)
    missing_buy = _filled_order_missing_fill()
    sell_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="repair-105560-sell",
        broker_order_id="sell-1",
        symbol="105560",
        side="sell",
        status="filled",
        quantity=1,
        submitted_at=SOLD_AT,
    )
    duplicate_order = _duplicate_fill_order()
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
    assert first["summary"]["deleted_fill_count"] == 1
    assert first["summary"]["audit_event_count"] == 2
    assert second["summary"]["inserted_fill_count"] == 0
    assert second["summary"]["deleted_fill_count"] == 0
    assert after["matched_sell_quantity"] == 1
    assert after["unmatched_sell_quantity"] == 0
    assert after["daily_realized_pnl"] == "1000.0000"
    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type.in_(
                [MISSING_FILL_REPAIR_EVENT, DUPLICATE_FILL_REPAIR_EVENT]
            )
        )
    ).all()
    assert sorted(event.event_type for event in events) == [
        DUPLICATE_FILL_REPAIR_EVENT,
        MISSING_FILL_REPAIR_EVENT,
    ]
    assert len(db.scalars(select(ExecutionFill)).all()) == 3
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
            symbol="049430",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=reconciled_at,
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
    }
    assert payload["broker_flat_fifo_residuals"][0] == {
        "symbol": "049430",
        "residual_quantity": "45.0000",
        "last_local_fill": fill_at.isoformat(),
        "broker_reconciliation_timestamp": reconciled_at.isoformat(),
        "broker_position_id": db.scalar(select(ExecutionPosition.id)),
        "broker_position_quantity": "0.0000",
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
            symbol="049430",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=fill_at + timedelta(hours=1),
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
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="001450",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=flat_at,
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
    )
    _add_fill(
        db,
        symbol="001450",
        side="sell",
        quantity=1,
        price="250",
        filled_at=sell_at,
        suffix="repurchase-sell",
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
            symbol="055550",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=flat_at,
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
    assert db.scalars(select(FifoInventoryIsolation)).all() == []
    engine.dispose()
