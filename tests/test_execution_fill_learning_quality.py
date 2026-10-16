from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    RuntimeRemediationEvent,
)
from app.services.execution_fill_economics_import import (
    EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
    _record_candidate_key,
    analyze_execution_fill_economics_import,
    apply_execution_fill_economics_import,
)
from app.services.execution_fill_repair import (
    analyze_execution_fill_metadata_repairs,
    apply_execution_fill_metadata_repairs,
)


def _canonical_scope() -> dict[str, object]:
    return {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": "a" * 24,
        "venue": "kis",
        "asset_class": "stock",
        "exchange": "KRX",
    }


def _order(
    *,
    order_id: str,
    broker_order_id: str,
    request_metadata: dict[str, object] | None,
) -> BrokerOrder:
    return BrokerOrder(
        id=order_id,
        broker_name="kis",
        broker_order_id=broker_order_id,
        mode="live",
        idempotency_key=order_id,
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="kis",
        asset_class="stock",
        submitted_at=datetime(2026, 10, 16, 0, 1, tzinfo=UTC),
        request_metadata=request_metadata,
    )


def _fill(*, fill_id: str, order: BrokerOrder, broker_fill_id: str) -> ExecutionFill:
    return ExecutionFill(
        id=fill_id,
        idempotency_key=f"fill:{fill_id}",
        broker_order=order,
        broker_fill_id=broker_fill_id,
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("70000"),
        settlement_currency=None,
        transaction_cost=None,
        transaction_cost_coverage="unknown",
        transaction_cost_source=None,
        filled_at=datetime(2026, 10, 16, 0, 2, tzinfo=UTC),
        payload={},
    )


def _session() -> tuple[Session, object]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine), engine


def test_fill_metadata_repair_uses_verified_scope_and_leaves_unknown_unmodified() -> None:
    db, engine = _session()
    try:
        canonical = _order(
            order_id="canonical-order",
            broker_order_id="10001",
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _canonical_scope(),
            },
        )
        canonical_fill = _fill(
            fill_id="canonical-fill",
            order=canonical,
            broker_fill_id="canonical-broker-fill",
        )
        event_order = _order(
            order_id="event-order",
            broker_order_id="10002",
            request_metadata={},
        )
        event_fill = _fill(
            fill_id="event-fill",
            order=event_order,
            broker_fill_id="event-broker-fill",
        )
        event_order.events.append(
            BrokerOrderEvent(
                id="event-evidence",
                event_type="reconcile_result",
                event_source="reconciliation",
                normalized_status="filled",
                response_payload={
                    "order": {
                        "odno": "10002",
                        "pdno": "005930",
                        "ord_dt": "20261016",
                        "excg_id_dvsn_cd": "KRX",
                    }
                },
                observed_at=datetime(2026, 10, 16, 0, 3, tzinfo=UTC),
            )
        )
        unknown = _order(
            order_id="unknown-order",
            broker_order_id="10003",
            request_metadata={},
        )
        unknown_fill = _fill(
            fill_id="unknown-fill",
            order=unknown,
            broker_fill_id="unknown-broker-fill",
        )
        db.add_all([canonical_fill, event_fill, unknown_fill])
        db.commit()

        dry_run = analyze_execution_fill_metadata_repairs(
            db=db,
            runtime_mode="live",
        )
        assert dry_run["summary"] == {
            "missing_settlement_currency_fill_count": 3,
            "deterministic_krw_candidate_count": 2,
            "unresolved_settlement_currency_fill_count": 1,
            "applied_settlement_currency_count": 0,
            "transaction_cost_repair_count": 0,
            "audit_event_count": 0,
        }

        applied = apply_execution_fill_metadata_repairs(
            db=db,
            runtime_mode="live",
            actor="test-operator",
            reason="verified historical KRX scope",
            now=datetime(2026, 10, 16, 1, tzinfo=UTC),
        )
        assert applied["summary"]["applied_settlement_currency_count"] == 2
        assert canonical_fill.settlement_currency == "KRW"
        assert event_fill.settlement_currency == "KRW"
        assert unknown_fill.settlement_currency is None
        assert canonical_fill.transaction_cost is None
        assert event_fill.transaction_cost is None
        assert unknown_fill.transaction_cost is None
        assert len(db.scalars(select(RuntimeRemediationEvent)).all()) == 2

        repeated = apply_execution_fill_metadata_repairs(
            db=db,
            runtime_mode="live",
            actor="test-operator",
            reason="idempotency check",
        )
        assert repeated["summary"]["applied_settlement_currency_count"] == 0
        assert repeated["summary"]["audit_event_count"] == 0
    finally:
        db.close()
        engine.dispose()


def test_fill_metadata_repair_normalizes_blank_currency_during_locked_apply() -> None:
    db, engine = _session()
    try:
        order = _order(
            order_id="blank-currency-order",
            broker_order_id="blank-currency-broker-order",
            request_metadata={"execution_scope": _canonical_scope()},
        )
        fill = _fill(
            fill_id="blank-currency-fill",
            order=order,
            broker_fill_id="blank-currency-broker-fill",
        )
        fill.settlement_currency = "   "
        db.add(fill)
        db.commit()

        dry_run = analyze_execution_fill_metadata_repairs(
            db=db,
            runtime_mode="live",
        )
        assert dry_run["summary"]["deterministic_krw_candidate_count"] == 1

        applied = apply_execution_fill_metadata_repairs(
            db=db,
            runtime_mode="live",
            actor="test-operator",
            reason="normalize verified blank currency",
            now=datetime(2026, 10, 16, 1, tzinfo=UTC),
        )

        assert applied["summary"]["applied_settlement_currency_count"] == 1
        assert fill.settlement_currency == "KRW"
        event = db.scalar(select(RuntimeRemediationEvent))
        assert event is not None
        assert event.evidence_summary["settlement_currency_before"] == "   "
    finally:
        db.close()
        engine.dispose()


def _economics_document(
    *,
    broker_order_id: str = "20001",
    broker_fill_id: str = "broker-fill-20001",
    scope: str = "per_fill",
) -> dict[str, object]:
    return {
        "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
        "records": [
            {
                "source_reference": "kis-confirmation-20261016.json",
                "broker_record_id": "record-20001",
                "broker_name": "kis",
                "broker_order_id": broker_order_id,
                "broker_fill_id": broker_fill_id,
                "symbol": "005930",
                "side": "buy",
                "scope": scope,
                "settlement_currency": "KRW",
                "fee": "10.25",
                "tax": "2.75",
            }
        ],
    }


def test_exact_per_fill_economics_import_is_audited_and_idempotent() -> None:
    db, engine = _session()
    try:
        order = _order(
            order_id="economics-order",
            broker_order_id="20001",
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _canonical_scope(),
            },
        )
        fill = _fill(
            fill_id="economics-fill",
            order=order,
            broker_fill_id="broker-fill-20001",
        )
        fill.transaction_cost_source = (
            "kis:inquire-daily-ccld:fee_tax_not_provided"
        )
        db.add(fill)
        db.commit()
        document = _economics_document()

        dry_run = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=document,
        )
        assert dry_run["summary"]["eligible_record_count"] == 1
        assert dry_run["summary"]["aggregate_allocation_count"] == 0
        assert fill.transaction_cost is None

        applied = apply_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=document,
            actor="test-operator",
            reason="exact KIS fill confirmation",
            now=datetime(2026, 10, 16, 1, tzinfo=UTC),
        )
        assert applied["summary"]["applied_record_count"] == 1
        assert applied["summary"]["already_applied_record_count"] == 1
        assert fill.settlement_currency == "KRW"
        assert fill.transaction_cost == Decimal("13.00000000")
        assert fill.transaction_cost_coverage == "complete"
        assert fill.transaction_cost_source == "broker_economics_import:fee+tax"

        repeated = apply_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=document,
            actor="test-operator",
            reason="idempotency check",
        )
        assert repeated["summary"]["applied_record_count"] == 0
        assert repeated["summary"]["already_applied_record_count"] == 1
        assert repeated["summary"]["audit_event_count"] == 0
    finally:
        db.close()
        engine.dispose()


def test_economics_import_rejects_aggregate_unknown_and_ambiguous_records() -> None:
    db, engine = _session()
    try:
        for suffix in ("a", "b"):
            order = _order(
                order_id=f"ambiguous-order-{suffix}",
                broker_order_id="20001",
                request_metadata={
                    "exchange": "KRX",
                    "execution_scope": _canonical_scope(),
                },
            )
            db.add(
                _fill(
                    fill_id=f"ambiguous-fill-{suffix}",
                    order=order,
                    broker_fill_id="broker-fill-20001",
                )
            )
        db.commit()

        ambiguous = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=_economics_document(),
        )
        assert ambiguous["items"][0]["reason"] == "ambiguous_broker_fill_identity"

        aggregate = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=_economics_document(scope="cumulative_order"),
        )
        assert (
            aggregate["items"][0]["reason"]
            == "aggregate_or_ambiguous_cost_scope_rejected"
        )

        missing_tax = _economics_document()
        records = missing_tax["records"]
        assert isinstance(records, list) and isinstance(records[0], dict)
        del records[0]["tax"]
        unknown = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=missing_tax,
        )
        assert unknown["items"][0]["status"] == "rejected"
        assert all(fill.transaction_cost is None for fill in db.scalars(select(ExecutionFill)))
    finally:
        db.close()
        engine.dispose()


def test_economics_import_binds_external_provenance_to_one_exact_fill() -> None:
    db, engine = _session()
    try:
        first_order = _order(
            order_id="provenance-order-a",
            broker_order_id="30001",
            request_metadata={"execution_scope": _canonical_scope()},
        )
        second_order = _order(
            order_id="provenance-order-b",
            broker_order_id="30002",
            request_metadata={"execution_scope": _canonical_scope()},
        )
        first_fill = _fill(
            fill_id="provenance-fill-a",
            order=first_order,
            broker_fill_id="broker-fill-30001",
        )
        second_fill = _fill(
            fill_id="provenance-fill-b",
            order=second_order,
            broker_fill_id="broker-fill-30002",
        )
        db.add_all([first_fill, second_fill])
        db.commit()

        first_document = _economics_document(
            broker_order_id="30001",
            broker_fill_id="broker-fill-30001",
        )
        applied = apply_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=first_document,
            actor="test-operator",
            reason="bind exact external ledger row",
        )
        assert applied["summary"]["applied_record_count"] == 1

        retargeted = _economics_document(
            broker_order_id="30002",
            broker_fill_id="broker-fill-30002",
        )
        retargeted_result = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=retargeted,
        )
        assert retargeted_result["items"][0]["reason"] == (
            "external_broker_provenance_conflict"
        )
        assert second_fill.transaction_cost is None

        changed_amount = _economics_document(
            broker_order_id="30001",
            broker_fill_id="broker-fill-30001",
        )
        changed_records = changed_amount["records"]
        assert isinstance(changed_records, list)
        assert isinstance(changed_records[0], dict)
        changed_records[0]["fee"] = "11.25"
        changed_result = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=changed_amount,
        )
        assert changed_result["items"][0]["reason"] == (
            "external_broker_provenance_conflict"
        )
        assert len(db.scalars(select(RuntimeRemediationEvent)).all()) == 1
    finally:
        db.close()
        engine.dispose()


def test_economics_import_honors_legacy_full_record_audit_binding() -> None:
    db, engine = _session()
    try:
        first_order = _order(
            order_id="legacy-provenance-order-a",
            broker_order_id="31001",
            request_metadata={"execution_scope": _canonical_scope()},
        )
        second_order = _order(
            order_id="legacy-provenance-order-b",
            broker_order_id="31002",
            request_metadata={"execution_scope": _canonical_scope()},
        )
        first_fill = _fill(
            fill_id="legacy-provenance-fill-a",
            order=first_order,
            broker_fill_id="broker-fill-31001",
        )
        second_fill = _fill(
            fill_id="legacy-provenance-fill-b",
            order=second_order,
            broker_fill_id="broker-fill-31002",
        )
        original_document = _economics_document(
            broker_order_id="31001",
            broker_fill_id="broker-fill-31001",
        )
        original_record = original_document["records"][0]
        assert isinstance(original_record, dict)
        first_fill.settlement_currency = "KRW"
        first_fill.transaction_cost = Decimal("13")
        first_fill.transaction_cost_coverage = "complete"
        first_fill.transaction_cost_source = "broker_economics_import:fee+tax"
        first_fill.payload = {
            "broker_economics_import": {
                "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
                "source_reference": original_record["source_reference"],
                "broker_record_id": original_record["broker_record_id"],
                "scope": "per_fill",
                "fee": "10.25",
                "tax": "2.75",
                "transaction_cost": "13.00",
                "settlement_currency": "KRW",
                "record_sha256": _record_candidate_key(original_record),
            }
        }
        db.add_all([first_fill, second_fill])
        db.flush()
        db.add(
            RuntimeRemediationEvent(
                mode="live",
                candidate_type="execution_fill_economics_import",
                candidate_key=_record_candidate_key(original_record),
                subject_id=first_fill.id,
                outcome="exact_economics_attached",
                actor="legacy-importer",
                reason="legacy full-record binding",
                occurred_at=datetime(2026, 10, 16, 1, tzinfo=UTC),
                evidence_summary={
                    "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
                    **original_record,
                    "transaction_cost": "13.00",
                    "identity_match_count": 1,
                    "aggregate_allocation_performed": False,
                    "record_sha256": _record_candidate_key(original_record),
                },
            )
        )
        db.commit()

        original_result = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=original_document,
        )
        assert original_result["items"][0]["status"] == "already_applied"

        retargeted = _economics_document(
            broker_order_id="31002",
            broker_fill_id="broker-fill-31002",
        )
        retargeted_result = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=retargeted,
        )

        assert retargeted_result["items"][0]["reason"] == (
            "external_broker_provenance_conflict"
        )
        assert second_fill.transaction_cost is None
    finally:
        db.close()
        engine.dispose()


def test_economics_import_rejects_duplicate_external_provenance_in_batch() -> None:
    db, engine = _session()
    try:
        for suffix, broker_order_id in (("a", "40001"), ("b", "40002")):
            order = _order(
                order_id=f"batch-provenance-order-{suffix}",
                broker_order_id=broker_order_id,
                request_metadata={"execution_scope": _canonical_scope()},
            )
            db.add(
                _fill(
                    fill_id=f"batch-provenance-fill-{suffix}",
                    order=order,
                    broker_fill_id=f"broker-fill-{broker_order_id}",
                )
            )
        db.commit()
        first = _economics_document(
            broker_order_id="40001",
            broker_fill_id="broker-fill-40001",
        )["records"][0]
        second = _economics_document(
            broker_order_id="40002",
            broker_fill_id="broker-fill-40002",
        )["records"][0]
        document = {
            "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
            "records": [first, second],
        }

        result = analyze_execution_fill_economics_import(
            db=db,
            runtime_mode="live",
            document=document,
        )

        assert result["summary"]["eligible_record_count"] == 0
        assert {
            item["reason"] for item in result["items"]
        } == {"duplicate_input_external_provenance"}
    finally:
        db.close()
        engine.dispose()
