from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
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
    RuntimeRemediationEvent,
)
from app.services.execution_fill_repair import (
    MISSING_FILL_REPAIR_EVENT,
    analyze_fifo_inventory_repairs,
    analyze_execution_fill_repairs,
    apply_fifo_inventory_repairs,
    apply_execution_fill_repairs,
)
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.runtime import broker_order_fill_quality_payload, daily_pnl_payload


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
    assert [row["id"] for row in group["duplicate_fills"]] == [second.id]
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


def test_live_fill_repair_requires_database_url_argument(tmp_path: Path) -> None:
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
            "KAI_EXEC_DATABASE_URL": f"sqlite:///{tmp_path / 'must-not-open.db'}",
        },
    )

    assert result.returncode != 0
    assert "requires --database-url explicitly" in result.stderr
    assert not (tmp_path / "must-not-open.db").exists()


def test_fill_remediation_uses_environment_runtime_mode_when_cli_mode_omitted(
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
            "--database-url",
            database_url,
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "KAI_EXEC_RUNTIME_MODE": "live",
        },
    )
    payload = json.loads(result.stdout)

    assert payload["runtime_mode"] == "live"
    assert payload["summary"]["missing_filled_order_count"] == 1
    assert payload["missing_filled_orders"][0]["broker_order_id"] == "missing-buy-1"


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
