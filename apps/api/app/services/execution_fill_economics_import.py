from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import BrokerOrder, ExecutionFill
from app.services.execution_fill_repair import (
    _record_remediation_event,
    _verified_kis_krw_evidence,
)
from app.services.execution_lineage import payload_with_order_decision_lineage


EXECUTION_FILL_ECONOMICS_IMPORT_VERSION = "execution-fill-economics-import-v1"
ECONOMICS_IMPORT_CANDIDATE_TYPE = "execution_fill_economics_import"
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
    items = [
        _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
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
    initial_items = [
        _evaluate_record(
            db=db,
            runtime_mode=runtime_mode,
            record=record,
            record_index=index,
            duplicate_identities=duplicate_identities,
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
            "record_sha256": record_sha256,
        }
        fill.settlement_currency = "KRW"
        fill.transaction_cost = total
        fill.transaction_cost_coverage = "complete"
        fill.transaction_cost_source = "broker_economics_import:fee+tax"
        fill.payload = payload
        db.add(fill)
        if _record_remediation_event(
            db=db,
            runtime_mode=runtime_mode,
            candidate_type=ECONOMICS_IMPORT_CANDIDATE_TYPE,
            candidate_key=record_sha256,
            subject_id=fill.id,
            outcome="exact_economics_attached",
            actor=actor,
            reason=reason,
            occurred_at=imported_at,
            evidence_summary={
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
                "record_sha256": record_sha256,
                "fills_deleted": 0,
                "fills_fabricated": 0,
                "orders_submitted": 0,
            },
        ):
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
        and imported.get("record_sha256") == _record_candidate_key(record)
        and imported.get("scope") == "per_fill"
        and imported.get("settlement_currency") == "KRW"
        and _exact_cost(imported.get("fee")) == fee
        and _exact_cost(imported.get("tax")) == tax
        and _exact_cost(imported.get("transaction_cost")) == fee + tax
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
