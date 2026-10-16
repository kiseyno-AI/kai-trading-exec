from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.runtime import BrokerOrder, ExecutionFill, RuntimeRemediationEvent
from app.services.execution_fill_repair import (
    _record_remediation_event,
    _verified_kis_krw_evidence,
)
from app.services.execution_lineage import payload_with_order_decision_lineage


EXECUTION_FILL_ECONOMICS_IMPORT_VERSION = "execution-fill-economics-import-v1"
ECONOMICS_IMPORT_CANDIDATE_TYPE = "execution_fill_economics_import"
_ECONOMICS_IMPORT_OUTCOME = "exact_economics_attached"
_RECORD_FIELDS = frozenset(
    {
        "source_reference",
        "broker_record_id",
        "broker_name",
        "broker_order_id",
        "broker_fill_id",
        "symbol",
        "side",
        "scope",
        "settlement_currency",
        "fee",
        "tax",
    }
)
_MAX_COST_COMPONENT = Decimal("99999999999999999999.99999999")
_COST_QUANTUM = Decimal("0.00000001")
_REPLACEABLE_UNKNOWN_COST_SOURCES = frozenset(
    {"", "kis:inquire-daily-ccld:fee_tax_not_provided"}
)


def analyze_execution_fill_economics_import(
    *,
    db: Session,
    runtime_mode: str,
    document: object,
) -> dict[str, object]:
    """Validate exact broker economics without changing local runtime data."""

    records = _validated_document_records(document)
    duplicate_identities = _duplicate_input_identities(records)
    duplicate_provenances = _duplicate_input_provenances(records)
    items = [
        _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
            duplicate_provenances=duplicate_provenances,
        )
        for index, record in enumerate(records)
    ]
    return _result_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        items=items,
        applied_count=0,
        audit_event_count=0,
    )


def apply_execution_fill_economics_import(
    *,
    db: Session,
    runtime_mode: str,
    document: object,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Attach exact per-fill fee/tax evidence, rejecting any allocation guess."""

    imported_at = now or datetime.now(UTC)
    records = _validated_document_records(document)
    duplicate_identities = _duplicate_input_identities(records)
    duplicate_provenances = _duplicate_input_provenances(records)
    initial_items = [
        _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
            duplicate_provenances=duplicate_provenances,
        )
        for index, record in enumerate(records)
    ]
    applied_count = 0
    audit_event_count = 0
    for index, (record, initial_item) in enumerate(zip(records, initial_items, strict=True)):
        if initial_item["status"] != "eligible":
            continue
        current = _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
            duplicate_provenances=duplicate_provenances,
            lock=True,
        )
        if current["status"] != "eligible":
            continue
        fill = db.get(ExecutionFill, str(current["execution_fill_id"]))
        if fill is None or fill.broker_order is None:
            continue
        fee = _exact_cost(record["fee"])
        tax = _exact_cost(record["tax"])
        assert fee is not None and tax is not None
        total = fee + tax
        record_sha256 = _record_candidate_key(record)
        provenance_candidate_key = _external_provenance_candidate_key(record)
        evidence_summary = {
            "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
            "source_reference": record["source_reference"],
            "broker_record_id": record["broker_record_id"],
            "broker_name": record["broker_name"],
            "broker_order_id": record["broker_order_id"],
            "broker_fill_id": record["broker_fill_id"],
            "symbol": record["symbol"],
            "side": record["side"],
            "scope": "per_fill",
            "settlement_currency": "KRW",
            "fee": _decimal_text(fee),
            "tax": _decimal_text(tax),
            "transaction_cost": _decimal_text(total),
            "identity_match_count": 1,
            "aggregate_allocation_performed": False,
            "external_provenance_sha256": provenance_candidate_key,
            "record_sha256": record_sha256,
            "fills_deleted": 0,
            "fills_fabricated": 0,
            "orders_submitted": 0,
        }
        event_created = False
        try:
            # Reserve the provenance before changing the fill. The durable unique
            # key serializes concurrent imports, while the savepoint lets a loser
            # re-read the winning binding without poisoning the outer transaction.
            with db.begin_nested():
                event_created = _record_remediation_event(
                    db=db,
                    runtime_mode=runtime_mode,
                    candidate_type=ECONOMICS_IMPORT_CANDIDATE_TYPE,
                    candidate_key=provenance_candidate_key,
                    subject_id=fill.id,
                    outcome=_ECONOMICS_IMPORT_OUTCOME,
                    actor=actor,
                    reason=reason,
                    occurred_at=imported_at,
                    evidence_summary=evidence_summary,
                )
                if event_created:
                    db.flush()
        except IntegrityError:
            raced = _evaluate_record(
                db=db,
                runtime_mode=runtime_mode,
                record=record,
                record_index=index,
                duplicate_identities=duplicate_identities,
                duplicate_provenances=duplicate_provenances,
                lock=True,
            )
            if raced["status"] == "eligible":
                raise
            continue
        if not event_created:
            continue
        payload = payload_with_order_decision_lineage(
            fill.payload if isinstance(fill.payload, dict) else {},
            order=fill.broker_order,
        )
        payload["broker_economics_import"] = {
            "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
            "source_reference": record["source_reference"],
            "broker_record_id": record["broker_record_id"],
            "scope": "per_fill",
            "fee": _decimal_text(fee),
            "tax": _decimal_text(tax),
            "transaction_cost": _decimal_text(total),
            "settlement_currency": "KRW",
            "imported_at": imported_at.isoformat(),
            "actor": actor,
            "reason": reason,
            "external_provenance_sha256": provenance_candidate_key,
            "record_sha256": record_sha256,
        }
        fill.settlement_currency = "KRW"
        fill.transaction_cost = total
        fill.transaction_cost_coverage = "complete"
        fill.transaction_cost_source = "broker_economics_import:fee+tax"
        fill.payload = payload
        db.add(fill)
        audit_event_count += 1
        applied_count += 1
    db.commit()
    final_items = [
        _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
            duplicate_provenances=duplicate_provenances,
        )
        for index, record in enumerate(records)
    ]
    return _result_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        items=final_items,
        applied_count=applied_count,
        audit_event_count=audit_event_count,
    )


def _validated_document_records(document: object) -> list[dict[str, object]]:
    if not isinstance(document, dict):
        raise ValueError("economics import document must be a JSON object")
    if set(document) != {"schema_version", "records"}:
        raise ValueError("economics import document fields are unsupported")
    if document.get("schema_version") != EXECUTION_FILL_ECONOMICS_IMPORT_VERSION:
        raise ValueError("economics import schema_version is unsupported")
    records = document.get("records")
    if not isinstance(records, list):
        raise ValueError("economics import records must be a JSON array")
    if len(records) > 10_000:
        raise ValueError("economics import exceeds the 10000-record safety limit")
    return [dict(record) if isinstance(record, dict) else {} for record in records]


def _evaluate_record(
    *,
    db: Session,
    runtime_mode: str,
    record: dict[str, object],
    record_index: int,
    duplicate_identities: set[tuple[str, str, str]],
    duplicate_provenances: set[tuple[str, str, str]],
    lock: bool = False,
) -> dict[str, object]:
    base = {
        "record_index": record_index,
        "source_reference": _bounded_text(record.get("source_reference"), 256),
        "broker_record_id": _bounded_text(record.get("broker_record_id"), 256),
        "broker_name": _normalized_text(record.get("broker_name"), lower=True),
        "broker_order_id": _bounded_text(record.get("broker_order_id"), 128),
        "broker_fill_id": _bounded_text(record.get("broker_fill_id"), 128),
        "symbol": _normalized_text(record.get("symbol"), upper=True),
        "side": _normalized_text(record.get("side"), lower=True),
        "scope": _normalized_text(record.get("scope"), lower=True),
        "settlement_currency": _normalized_text(
            record.get("settlement_currency"), upper=True
        ),
    }
    validation_error = _record_validation_error(record, base=base)
    external_provenance = _canonical_external_provenance(record)
    if external_provenance in duplicate_provenances:
        return {
            **base,
            "status": "rejected",
            "reason": "duplicate_input_external_provenance",
        }
    binding_events = (
        _external_provenance_binding_events(
            db=db,
            runtime_mode=runtime_mode,
            candidate_key=_external_provenance_candidate_key(record),
            record=record,
            lock=lock,
        )
        if external_provenance is not None
        else []
    )
    if any(
        not _binding_event_matches_record(
            binding_event,
            record=record,
            base=base,
        )
        for binding_event in binding_events
    ):
        return {
            **base,
            "status": "rejected",
            "reason": "external_broker_provenance_conflict",
        }
    if validation_error is not None:
        return {**base, "status": "rejected", "reason": validation_error}
    identity = (
        str(base["broker_name"]),
        str(base["broker_order_id"]),
        str(base["broker_fill_id"]),
    )
    if identity in duplicate_identities:
        return {
            **base,
            "status": "rejected",
            "reason": "duplicate_input_broker_fill_identity",
        }

    query = (
        select(ExecutionFill)
        .join(BrokerOrder, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == runtime_mode,
            BrokerOrder.broker_name == base["broker_name"],
            BrokerOrder.broker_order_id == base["broker_order_id"],
            ExecutionFill.broker_fill_id == base["broker_fill_id"],
        )
        .order_by(ExecutionFill.id.asc())
    )
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    matches = db.scalars(query).all()
    if not matches:
        return {**base, "status": "rejected", "reason": "broker_fill_not_found"}
    if len(matches) != 1:
        return {
            **base,
            "status": "rejected",
            "reason": "ambiguous_broker_fill_identity",
            "identity_match_count": len(matches),
        }
    fill = matches[0]
    order = fill.broker_order
    if order is None:
        return {**base, "status": "rejected", "reason": "broker_order_not_found"}
    if fill.symbol.strip().upper() != base["symbol"]:
        return {**base, "status": "rejected", "reason": "fill_symbol_mismatch"}
    if fill.side.strip().lower() != base["side"]:
        return {**base, "status": "rejected", "reason": "fill_side_mismatch"}
    fill_payload = fill.payload if isinstance(fill.payload, dict) else {}
    if (
        fill_payload.get("price_derivation") == "cumulative_notional_delta"
        or str(fill.broker_fill_id or "").startswith(
            f"{order.broker_order_id}:cumulative:"
        )
    ):
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "broker_fill_identity_not_broker_authoritative",
        }
    scope_evidence = _verified_kis_krw_evidence(order)
    if scope_evidence is None:
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "verified_krx_scope_unavailable",
        }
    if lock and not binding_events:
        binding_events = _external_provenance_binding_events(
            db=db,
            runtime_mode=runtime_mode,
            candidate_key=_external_provenance_candidate_key(record),
            record=record,
            lock=True,
        )
    if any(
        not _binding_event_matches_record(
            binding_event,
            record=record,
            base=base,
            execution_fill_id=fill.id,
        )
        for binding_event in binding_events
    ):
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "external_broker_provenance_conflict",
        }
    stored_currency = str(fill.settlement_currency or "").strip().upper()
    if stored_currency and stored_currency != "KRW":
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "settlement_currency_conflict",
        }
    fee = _exact_cost(record["fee"])
    tax = _exact_cost(record["tax"])
    assert fee is not None and tax is not None
    total = fee + tax
    persisted_cost = _exact_cost(fill.transaction_cost)
    persisted_coverage = str(fill.transaction_cost_coverage or "").strip().lower()
    persisted_source = str(fill.transaction_cost_source or "").strip()
    if (
        persisted_cost == total
        and persisted_coverage == "complete"
        and persisted_source
        and stored_currency == "KRW"
        and _persisted_import_matches_record(fill_payload, record=record)
    ):
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "already_applied",
            "reason": None,
            "fee": _decimal_text(fee),
            "tax": _decimal_text(tax),
            "transaction_cost": _decimal_text(total),
            "scope_evidence": scope_evidence,
        }
    if binding_events:
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "external_broker_provenance_conflict",
        }
    if (
        fill.transaction_cost is not None
        or persisted_coverage not in {"", "unknown"}
        or persisted_source not in _REPLACEABLE_UNKNOWN_COST_SOURCES
    ):
        return {
            **base,
            "execution_fill_id": fill.id,
            "status": "rejected",
            "reason": "existing_transaction_cost_evidence_conflict",
        }
    return {
        **base,
        "execution_fill_id": fill.id,
        "status": "eligible",
        "reason": None,
        "fee": _decimal_text(fee),
        "tax": _decimal_text(tax),
        "transaction_cost": _decimal_text(total),
        "scope_evidence": scope_evidence,
    }


def _record_validation_error(
    record: dict[str, object],
    *,
    base: dict[str, object],
) -> str | None:
    if set(record) != _RECORD_FIELDS:
        return "record_fields_are_unsupported_or_missing"
    required_text = (
        "source_reference",
        "broker_record_id",
        "broker_name",
        "broker_order_id",
        "broker_fill_id",
        "symbol",
        "side",
        "scope",
        "settlement_currency",
    )
    if any(base.get(field) is None for field in required_text):
        return "record_identity_or_provenance_is_missing"
    if any(record.get(field) != base.get(field) for field in required_text):
        return "record_identity_or_provenance_is_not_canonical"
    if base["broker_name"] != "kis":
        return "unsupported_broker"
    if base["scope"] != "per_fill":
        return "aggregate_or_ambiguous_cost_scope_rejected"
    if base["side"] not in {"buy", "sell"}:
        return "invalid_side"
    if base["settlement_currency"] != "KRW":
        return "settlement_currency_not_verified_for_krx"
    fee = _exact_cost(record.get("fee"))
    tax = _exact_cost(record.get("tax"))
    if fee is None or tax is None:
        return "fee_and_tax_must_be_explicit_nonnegative_exact_values"
    if fee + tax > _MAX_COST_COMPONENT:
        return "transaction_cost_exceeds_storage_precision"
    return None


def _persisted_import_matches_record(
    payload: dict[str, Any],
    *,
    record: dict[str, object],
) -> bool:
    imported = payload.get("broker_economics_import")
    if not isinstance(imported, dict):
        return False
    fee = _exact_cost(record.get("fee"))
    tax = _exact_cost(record.get("tax"))
    if fee is None or tax is None:
        return False
    return (
        imported.get("schema_version") == EXECUTION_FILL_ECONOMICS_IMPORT_VERSION
        and imported.get("source_reference") == record.get("source_reference")
        and imported.get("broker_record_id") == record.get("broker_record_id")
        and imported.get("scope") == "per_fill"
        and imported.get("settlement_currency") == "KRW"
        and _exact_cost(imported.get("fee")) == fee
        and _exact_cost(imported.get("tax")) == tax
        and _exact_cost(imported.get("transaction_cost")) == fee + tax
    )


def _canonical_external_provenance(
    record: dict[str, object],
) -> tuple[str, str, str] | None:
    if set(record) != _RECORD_FIELDS:
        return None
    broker_name = _normalized_text(record.get("broker_name"), lower=True)
    source_reference = _bounded_text(record.get("source_reference"), 256)
    broker_record_id = _bounded_text(record.get("broker_record_id"), 256)
    if (
        broker_name is None
        or source_reference is None
        or broker_record_id is None
        or record.get("broker_name") != broker_name
        or record.get("source_reference") != source_reference
        or record.get("broker_record_id") != broker_record_id
    ):
        return None
    return broker_name, source_reference, broker_record_id


def _external_provenance_candidate_key(record: dict[str, object]) -> str:
    provenance = _canonical_external_provenance(record)
    if provenance is None:
        raise ValueError("external broker provenance is not canonical")
    broker_name, source_reference, broker_record_id = provenance
    canonical = json.dumps(
        {
            "broker_name": broker_name,
            "broker_record_id": broker_record_id,
            "source_reference": source_reference,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _external_provenance_binding_events(
    *,
    db: Session,
    runtime_mode: str,
    candidate_key: str,
    record: dict[str, object],
    lock: bool,
) -> list[RuntimeRemediationEvent]:
    common_predicates = (
        RuntimeRemediationEvent.mode == runtime_mode,
        RuntimeRemediationEvent.candidate_type == ECONOMICS_IMPORT_CANDIDATE_TYPE,
        RuntimeRemediationEvent.outcome == _ECONOMICS_IMPORT_OUTCOME,
    )
    query = select(RuntimeRemediationEvent).where(
        *common_predicates,
        RuntimeRemediationEvent.candidate_key == candidate_key,
    )
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    events = list(db.scalars(query).all())

    # The first release keyed audits by the SHA of the entire broker record.
    # Locate those durable records by their exact external provenance so an
    # upgrade cannot reuse a previously consumed ledger row on another fill.
    provenance = _canonical_external_provenance(record)
    if provenance is None:
        return events
    broker_name, source_reference, broker_record_id = provenance
    legacy_query = select(RuntimeRemediationEvent).where(
        *common_predicates,
        RuntimeRemediationEvent.candidate_key != candidate_key,
        RuntimeRemediationEvent.evidence_summary["broker_name"].as_string()
        == broker_name,
        RuntimeRemediationEvent.evidence_summary["source_reference"].as_string()
        == source_reference,
        RuntimeRemediationEvent.evidence_summary["broker_record_id"].as_string()
        == broker_record_id,
    )
    if lock:
        legacy_query = legacy_query.with_for_update().execution_options(
            populate_existing=True
        )
    events.extend(db.scalars(legacy_query).all())
    return events


def _binding_event_matches_record(
    event: RuntimeRemediationEvent,
    *,
    record: dict[str, object],
    base: dict[str, object],
    execution_fill_id: str | None = None,
) -> bool:
    evidence = event.evidence_summary
    fee = _exact_cost(record.get("fee"))
    tax = _exact_cost(record.get("tax"))
    if not isinstance(evidence, dict) or fee is None or tax is None:
        return False
    if execution_fill_id is not None and event.subject_id != execution_fill_id:
        return False
    return (
        evidence.get("schema_version") == EXECUTION_FILL_ECONOMICS_IMPORT_VERSION
        and evidence.get("source_reference") == base.get("source_reference")
        and evidence.get("broker_record_id") == base.get("broker_record_id")
        and evidence.get("broker_name") == base.get("broker_name")
        and evidence.get("broker_order_id") == base.get("broker_order_id")
        and evidence.get("broker_fill_id") == base.get("broker_fill_id")
        and evidence.get("symbol") == base.get("symbol")
        and evidence.get("side") == base.get("side")
        and evidence.get("scope") == base.get("scope")
        and evidence.get("settlement_currency") == base.get("settlement_currency")
        and _exact_cost(evidence.get("fee")) == fee
        and _exact_cost(evidence.get("tax")) == tax
        and _exact_cost(evidence.get("transaction_cost")) == fee + tax
    )


def _duplicate_input_identities(
    records: list[dict[str, object]],
) -> set[tuple[str, str, str]]:
    counts: dict[tuple[str, str, str], int] = {}
    for record in records:
        identity = (
            str(record.get("broker_name") or "").strip().lower(),
            str(record.get("broker_order_id") or "").strip(),
            str(record.get("broker_fill_id") or "").strip(),
        )
        counts[identity] = counts.get(identity, 0) + 1
    return {identity for identity, count in counts.items() if count > 1}


def _duplicate_input_provenances(
    records: list[dict[str, object]],
) -> set[tuple[str, str, str]]:
    counts: dict[tuple[str, str, str], int] = {}
    for record in records:
        provenance = _canonical_external_provenance(record)
        if provenance is None:
            continue
        counts[provenance] = counts.get(provenance, 0) + 1
    return {provenance for provenance, count in counts.items() if count > 1}


def _record_candidate_key(record: dict[str, object]) -> str:
    canonical = json.dumps(
        record,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def _result_payload(
    *,
    mode: str,
    runtime_mode: str,
    items: list[dict[str, object]],
    applied_count: int,
    audit_event_count: int,
) -> dict[str, object]:
    return {
        "schema_version": EXECUTION_FILL_ECONOMICS_IMPORT_VERSION,
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "record_count": len(items),
            "eligible_record_count": sum(
                1 for item in items if item["status"] == "eligible"
            ),
            "already_applied_record_count": sum(
                1 for item in items if item["status"] == "already_applied"
            ),
            "rejected_record_count": sum(
                1 for item in items if item["status"] == "rejected"
            ),
            "applied_record_count": applied_count,
            "audit_event_count": audit_event_count,
            "aggregate_allocation_count": 0,
        },
        "items": items,
    }


def _exact_cost(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool | float):
        return None
    if not isinstance(value, str | int | Decimal):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0 or parsed > _MAX_COST_COMPONENT:
        return None
    try:
        if parsed != parsed.quantize(_COST_QUANTUM):
            return None
    except InvalidOperation:
        return None
    return parsed


def _bounded_text(value: object, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        return None
    return normalized


def _normalized_text(
    value: object,
    *,
    upper: bool = False,
    lower: bool = False,
) -> str | None:
    normalized = _bounded_text(value, 128)
    if normalized is None:
        return None
    if upper:
        return normalized.upper()
    if lower:
        return normalized.lower()
    return normalized


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")
