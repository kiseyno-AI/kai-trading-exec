from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPosition,
    FifoInventoryIsolation,
    RuntimeRemediationEvent,
)
from app.services.fifo_inventory import load_fifo_isolation_cursor
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    duplicate_fill_economic_conflict,
    execution_fill_dedupe_key,
    execution_fill_idempotency_key,
)


REPAIR_EVENT_SOURCE = "operator_repair"
MISSING_FILL_REPAIR_EVENT = "execution_fill_missing_repair"
FIFO_INVENTORY_ISOLATION_SOURCE = "broker_flat_reconciliation"
MISSING_FILL_CANDIDATE_TYPE = "missing_execution_fill"
DUPLICATE_FILL_CANDIDATE_TYPE = "duplicate_fill_group"
FIFO_RESIDUAL_CANDIDATE_TYPE = "fifo_inventory_residual"


def analyze_fifo_inventory_repairs(
    *,
    db: Session,
    runtime_mode: str,
) -> dict[str, object]:
    items = _fifo_inventory_repair_items(db=db, runtime_mode=runtime_mode)
    return _fifo_inventory_repair_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        items=items,
        applied_count=0,
    )


def apply_fifo_inventory_repairs(
    *,
    db: Session,
    runtime_mode: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    isolated_at = now or datetime.now(UTC)
    items = _fifo_inventory_repair_items(db=db, runtime_mode=runtime_mode)
    applied_items: list[dict[str, object]] = []
    applied_blocker_count = 0
    for item in items:
        if item["eligible"] is not True:
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=FIFO_RESIDUAL_CANDIDATE_TYPE,
                candidate_key=_fifo_candidate_key(item),
                subject_id=str(item["broker_position_id"]),
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=isolated_at,
                evidence_summary={
                    "symbol": item["symbol"],
                    "residual_quantity": item["residual_quantity"],
                    "last_local_fill": item["last_local_fill"],
                    "broker_reconciliation_timestamp": item[
                        "broker_reconciliation_timestamp"
                    ],
                    "blocker": item["reason"],
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_blocker_count += 1
            continue
        broker_reconciled_at = _parse_timestamp(
            item["broker_reconciliation_timestamp"],
            fallback=isolated_at,
        )
        existing = db.scalar(
            select(FifoInventoryIsolation).where(
                FifoInventoryIsolation.mode == runtime_mode,
                FifoInventoryIsolation.symbol == item["symbol"],
                FifoInventoryIsolation.broker_reconciled_at == broker_reconciled_at,
            )
        )
        if existing is not None:
            continue
        isolation = FifoInventoryIsolation(
            mode=runtime_mode,
            symbol=str(item["symbol"]),
            residual_quantity=_parse_decimal(item["residual_quantity"]) or Decimal("0"),
            last_local_fill_at=_parse_timestamp(item["last_local_fill"], fallback=isolated_at),
            broker_position_id=str(item["broker_position_id"]),
            broker_position_quantity=_parse_decimal(item["broker_position_quantity"])
            or Decimal("0"),
            broker_reconciled_at=broker_reconciled_at,
            isolated_through_at=broker_reconciled_at,
            evidence_source=FIFO_INVENTORY_ISOLATION_SOURCE,
            actor=actor,
            reason=reason,
            audit_payload={
                "action": "isolate_fifo_residual",
                "actor": actor,
                "reason": reason,
                "isolated_at": isolated_at.isoformat(),
                "runtime_mode": runtime_mode,
                "evidence": item,
                "fills_deleted": 0,
                "fills_fabricated": 0,
            },
        )
        db.add(isolation)
        applied_items.append(item)
    db.commit()
    items = _annotate_fifo_reviews(
        db=db,
        runtime_mode=runtime_mode,
        items=items,
    )
    return _fifo_inventory_repair_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        items=items,
        applied_count=len(applied_items),
        applied_items=applied_items,
        applied_blocker_count=applied_blocker_count,
    )


def _fifo_inventory_repair_payload(
    *,
    mode: str,
    runtime_mode: str,
    items: list[dict[str, object]],
    applied_count: int,
    applied_items: list[dict[str, object]] | None = None,
    applied_blocker_count: int = 0,
) -> dict[str, object]:
    eligible_count = sum(1 for item in items if item["eligible"] is True)
    manual_review_count = sum(
        1 for item in items if item.get("review_outcome") == "manual_review_required"
    )
    return {
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "broker_flat_residual_count": len(items),
            "eligible_isolation_count": eligible_count,
            "recency_blocked_count": len(items) - eligible_count,
            "applied_isolation_count": applied_count,
            "manual_review_count": manual_review_count,
            "unreviewed_recency_blocked_count": (
                len(items) - eligible_count - manual_review_count
            ),
            "applied_blocker_count": applied_blocker_count,
        },
        "broker_flat_fifo_residuals": items,
        "applied_isolations": applied_items or [],
    }


def _fifo_inventory_repair_items(
    *,
    db: Session,
    runtime_mode: str,
) -> list[dict[str, object]]:
    fills = dedupe_execution_fills(list(
        db.scalars(
            select(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(BrokerOrder.mode == runtime_mode)
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    ))
    isolation_cursor = load_fifo_isolation_cursor(db=db, runtime_mode=runtime_mode)
    lots_by_symbol: dict[str, list[Decimal]] = {}
    last_fill_at_by_symbol: dict[str, datetime] = {}
    for fill in fills:
        symbol = fill.symbol
        filled_at = _parse_timestamp(fill.filled_at, fallback=datetime.now(UTC))
        lots = lots_by_symbol.setdefault(symbol, [])
        isolation_cursor.apply_before(symbol=symbol, observed_at=filled_at, lots=lots)
        quantity = (
            Decimal(fill.base_quantity)
            if fill.base_quantity is not None and fill.base_quantity > 0
            else Decimal(fill.quantity)
        )
        if str(fill.side or "").lower() == "buy":
            lots.append(quantity)
        elif str(fill.side or "").lower() == "sell":
            remaining = quantity
            while remaining > 0 and lots:
                matched = min(remaining, lots[0])
                lots[0] -= matched
                remaining -= matched
                if lots[0] <= 0:
                    lots.pop(0)
        last_fill_at_by_symbol[symbol] = filled_at

    for symbol, lots in lots_by_symbol.items():
        isolation_cursor.apply_through(
            symbol=symbol,
            observed_at=datetime.max.replace(tzinfo=UTC),
            lots=lots,
        )

    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == runtime_mode,
            ExecutionPosition.last_reconciled_at.is_not(None),
        )
    ).all()
    position_by_symbol = {position.symbol: position for position in positions}
    items: list[dict[str, object]] = []
    for symbol, lots in sorted(lots_by_symbol.items()):
        residual_quantity = sum(lots, Decimal("0"))
        if residual_quantity <= 0:
            continue
        position = position_by_symbol.get(symbol)
        if (
            position is None
            or "reconciliation" not in str(position.updated_from or "")
            or _position_quantity(position) > 0
            or position.last_reconciled_at is None
        ):
            continue
        last_fill_at = last_fill_at_by_symbol.get(symbol)
        if last_fill_at is None:
            continue
        broker_reconciled_at = _parse_timestamp(
            position.last_reconciled_at,
            fallback=datetime.min.replace(tzinfo=UTC),
        )
        eligible = broker_reconciled_at > last_fill_at
        items.append(
            {
                "symbol": symbol,
                "residual_quantity": _decimal_text(residual_quantity),
                "last_local_fill": last_fill_at.isoformat(),
                "broker_reconciliation_timestamp": broker_reconciled_at.isoformat(),
                "broker_position_id": position.id,
                "broker_position_quantity": _decimal_text(_position_quantity(position)),
                "eligible": eligible,
                "repair_action": "isolate" if eligible else "skip",
                "reason": (
                    "broker_flat_evidence_newer_than_last_local_fill"
                    if eligible
                    else "broker_flat_evidence_not_newer_than_last_local_fill"
                ),
            }
        )
    return _annotate_fifo_reviews(db=db, runtime_mode=runtime_mode, items=items)


def _fifo_candidate_key(item: dict[str, object]) -> str:
    return "|".join(
        (
            str(item["symbol"]),
            str(item["last_local_fill"]),
            str(item["broker_reconciliation_timestamp"]),
        )
    )


def _annotate_fifo_reviews(
    *,
    db: Session,
    runtime_mode: str,
    items: list[dict[str, object]],
) -> list[dict[str, object]]:
    candidate_keys = {_fifo_candidate_key(item) for item in items}
    if not candidate_keys:
        return items
    reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == FIFO_RESIDUAL_CANDIDATE_TYPE,
            RuntimeRemediationEvent.candidate_key.in_(sorted(candidate_keys)),
        )
    ).all()
    review_by_key = {review.candidate_key: review for review in reviews}
    for item in items:
        review = review_by_key.get(_fifo_candidate_key(item))
        if review is not None:
            item["review_outcome"] = review.outcome
            item["review_actor"] = review.actor
            item["review_reason"] = review.reason
            item["reviewed_at"] = _serialize_datetime(review.occurred_at)
    return items


def _position_quantity(position: ExecutionPosition) -> Decimal:
    if position.base_quantity is not None:
        return Decimal(position.base_quantity)
    return Decimal(position.quantity)


def analyze_execution_fill_repairs(
    *,
    db: Session,
    runtime_mode: str,
) -> dict[str, object]:
    missing = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=MISSING_FILL_CANDIDATE_TYPE,
        items=_missing_fill_items(db=db, runtime_mode=runtime_mode),
    )
    duplicates = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
        items=_duplicate_fill_groups(db=db, runtime_mode=runtime_mode),
    )
    return _repair_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        missing=missing,
        duplicates=duplicates,
        inserted_fill_count=0,
        deleted_fill_count=0,
        audit_event_count=0,
        applied_manual_review_count=0,
        applied_duplicate_isolation_count=0,
    )


def apply_execution_fill_repairs(
    *,
    db: Session,
    runtime_mode: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    repaired_at = now or datetime.now(UTC)
    inserted_fill_count = 0
    audit_event_count = 0
    applied_manual_review_count = 0
    applied_duplicate_isolation_count = 0

    missing = _missing_fill_items(db=db, runtime_mode=runtime_mode)
    existing_keys = {
        execution_fill_dedupe_key(fill)
        for fill in db.scalars(select(ExecutionFill)).all()
    }
    for item in missing:
        order = db.get(BrokerOrder, item["broker_order_pk"])
        if order is None or order.fills:
            continue
        proposed_fills = item.get("proposed_fills")
        if not isinstance(proposed_fills, list) or not proposed_fills:
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=MISSING_FILL_CANDIDATE_TYPE,
                candidate_key=str(order.id),
                subject_id=order.id,
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=repaired_at,
                evidence_summary={
                    "symbol": order.symbol,
                    "side": order.side,
                    "broker_order_id": order.broker_order_id,
                    "idempotency_key": order.idempotency_key,
                    "blocker": "authoritative_broker_fill_evidence_unavailable",
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_manual_review_count += 1
                audit_event_count += 1
            continue
        validated_fills, validation_blocker = _validated_repair_fill_proposals(
            order=order,
            proposed_fills=proposed_fills,
            existing_keys=existing_keys,
        )
        if validation_blocker is not None:
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=MISSING_FILL_CANDIDATE_TYPE,
                candidate_key=str(order.id),
                subject_id=order.id,
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=repaired_at,
                evidence_summary={
                    "symbol": order.symbol,
                    "side": order.side,
                    "broker_order_id": order.broker_order_id,
                    "idempotency_key": order.idempotency_key,
                    "blocker": validation_blocker["code"],
                    "validation": validation_blocker,
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_manual_review_count += 1
                audit_event_count += 1
            continue
        inserted_for_order: list[dict[str, object]] = []
        for proposed, signature in validated_fills:
            fill = ExecutionFill(
                idempotency_key=signature,
                broker_order_pk=order.id,
                broker_fill_id=_optional_str(proposed.get("broker_fill_id")),
                symbol=str(proposed["symbol"]),
                side=str(proposed["side"]),
                quantity=int(proposed["quantity"]),
                price=_parse_decimal(proposed["price"]) or Decimal("0"),
                filled_at=_parse_timestamp(proposed.get("filled_at"), fallback=repaired_at),
                payload={
                    "source": "operator_fill_evidence_repair",
                    "actor": actor,
                    "reason": reason,
                    "repair_evidence": item.get("available_repair_evidence"),
                    "proposed_fill": _json_safe_value(proposed),
                },
            )
            db.add(fill)
            existing_keys.add(signature)
            inserted_fill_count += 1
            inserted_for_order.append(_fill_row_payload(fill, id_override=None))
        if inserted_for_order:
            db.add(
                BrokerOrderEvent(
                    broker_order_pk=order.id,
                    event_type=MISSING_FILL_REPAIR_EVENT,
                    event_source=REPAIR_EVENT_SOURCE,
                    normalized_status=order.status,
                    broker_status=None,
                    message="operator repaired missing execution fill evidence",
                    payload={
                        "actor": actor,
                        "reason": reason,
                        "repaired_at": repaired_at.isoformat(),
                        "runtime_mode": runtime_mode,
                        "broker_order_id": order.broker_order_id,
                        "idempotency_key": order.idempotency_key,
                        "inserted_fills": inserted_for_order,
                        "dry_run_item": item,
                    },
                    observed_at=repaired_at,
                )
            )
            audit_event_count += 1

    db.flush()
    duplicates = _duplicate_fill_groups(db=db, runtime_mode=runtime_mode)
    for group in duplicates:
        retained = group.get("retained_fill")
        duplicate_fills = group.get("duplicate_fills")
        if (
            not isinstance(retained, dict)
            or not isinstance(duplicate_fills, list)
            or not duplicate_fills
        ):
            continue
        conflict = group.get("economic_conflict") is True
        outcome = "manual_review_required" if conflict else "non_destructive_isolation"
        evidence_summary = {
            "symbol": group.get("symbol"),
            "side": group.get("side"),
            "broker_order_id": group.get("broker_order_id"),
            "retained_fill_id": retained.get("id"),
            "duplicate_fill_ids": [
                row.get("id") for row in duplicate_fills if isinstance(row, dict)
            ],
            "duplicate_group_key": group.get("duplicate_group_key"),
            "group_membership_version": _duplicate_group_membership_version(group),
            "fills_deleted": 0,
            "fills_fabricated": 0,
            "orders_submitted": 0,
        }
        if conflict:
            evidence_summary["blocker"] = "duplicate_fill_economic_conflict"
            evidence_summary["conflicting_fields"] = group.get("conflicting_fields")
            evidence_summary["dedupe_behavior"] = (
                "application_readers_preserve_conflicting_duplicate_identity_rows"
            )
        else:
            evidence_summary["dedupe_behavior"] = (
                "application_readers_ignore_duplicate_identity"
            )
        if _record_remediation_event(
            db=db,
            runtime_mode=runtime_mode,
            candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
            candidate_key=_duplicate_group_candidate_key(group),
            subject_id=str(group["broker_order_pk"]),
            outcome=outcome,
            actor=actor,
            reason=reason,
            occurred_at=repaired_at,
            evidence_summary=evidence_summary,
        ):
            if conflict:
                applied_manual_review_count += 1
            else:
                applied_duplicate_isolation_count += 1
            audit_event_count += 1

    db.commit()
    remaining_missing = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=MISSING_FILL_CANDIDATE_TYPE,
        items=_missing_fill_items(db=db, runtime_mode=runtime_mode),
    )
    remaining_duplicates = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
        items=_duplicate_fill_groups(db=db, runtime_mode=runtime_mode),
    )
    return _repair_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        missing=remaining_missing,
        duplicates=remaining_duplicates,
        inserted_fill_count=inserted_fill_count,
        deleted_fill_count=0,
        audit_event_count=audit_event_count,
        applied_manual_review_count=applied_manual_review_count,
        applied_duplicate_isolation_count=applied_duplicate_isolation_count,
    )


def _repair_payload(
    *,
    mode: str,
    runtime_mode: str,
    missing: list[dict[str, object]],
    duplicates: list[dict[str, object]],
    inserted_fill_count: int,
    deleted_fill_count: int,
    audit_event_count: int,
    applied_manual_review_count: int,
    applied_duplicate_isolation_count: int,
) -> dict[str, object]:
    proposed_missing = sum(
        len(item.get("proposed_fills") or [])
        for item in missing
        if isinstance(item.get("proposed_fills"), list)
    )
    manual_review_count = sum(
        1 for item in missing if item.get("review_outcome") == "manual_review_required"
    )
    isolated_duplicate_count = sum(
        1
        for group in duplicates
        if group.get("review_outcome") == "non_destructive_isolation"
    )
    manual_review_duplicate_count = sum(
        1
        for group in duplicates
        if group.get("review_outcome") == "manual_review_required"
    )
    return {
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "missing_filled_order_count": len(missing),
            "proposed_missing_fill_insert_count": proposed_missing,
            "duplicate_fill_group_count": len(duplicates),
            "proposed_manual_review_count": sum(
                1
                for item in missing
                if not item.get("proposed_fills") and not item.get("review_outcome")
            ),
            "proposed_duplicate_fill_isolation_count": sum(
                1
                for group in duplicates
                if group.get("repair_action") == "isolate_non_destructively"
                and not group.get("review_outcome")
            ),
            "proposed_duplicate_fill_manual_review_count": sum(
                1
                for group in duplicates
                if group.get("repair_action") == "record_manual_review"
                and not group.get("review_outcome")
            ),
            "manual_review_missing_fill_count": manual_review_count,
            "unreviewed_missing_fill_count": len(missing) - manual_review_count,
            "isolated_duplicate_fill_group_count": isolated_duplicate_count,
            "manual_review_duplicate_fill_group_count": manual_review_duplicate_count,
            "unreviewed_duplicate_fill_group_count": (
                len(duplicates) - isolated_duplicate_count - manual_review_duplicate_count
            ),
            "inserted_fill_count": inserted_fill_count,
            "deleted_fill_count": deleted_fill_count,
            "audit_event_count": audit_event_count,
            "applied_manual_review_count": applied_manual_review_count,
            "applied_duplicate_isolation_count": applied_duplicate_isolation_count,
        },
        "missing_filled_orders": missing,
        "duplicate_fill_groups": duplicates,
    }


def _missing_fill_items(*, db: Session, runtime_mode: str) -> list[dict[str, object]]:
    rows = db.scalars(
        select(BrokerOrder)
        .outerjoin(ExecutionFill, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(BrokerOrder.mode == runtime_mode, BrokerOrder.status == "filled")
        .group_by(BrokerOrder.id)
        .having(func.count(ExecutionFill.id) == 0)
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.created_at.asc())
    ).all()
    return [_missing_fill_item(db=db, order=order) for order in rows]


def _missing_fill_item(*, db: Session, order: BrokerOrder) -> dict[str, object]:
    evidence = _repair_evidence_for_order(db=db, order=order)
    proposed_fills = evidence.get("proposed_fills")
    return {
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "symbol": order.symbol,
        "side": order.side,
        "quantity": order.quantity,
        "submitted_at": _serialize_datetime(order.submitted_at),
        "available_repair_evidence": evidence.get("available_repair_evidence"),
        "proposed_fills": proposed_fills if isinstance(proposed_fills, list) else [],
    }


def _repair_evidence_for_order(*, db: Session, order: BrokerOrder) -> dict[str, object]:
    candidates: list[tuple[str, dict[str, object], datetime | None, str | None]] = []
    if isinstance(order.raw_broker_payload, dict):
        candidates.append(("broker_orders.raw_broker_payload", order.raw_broker_payload, order.last_broker_sync_at, None))
    events = db.scalars(
        select(BrokerOrderEvent)
        .where(BrokerOrderEvent.broker_order_pk == order.id)
        .order_by(BrokerOrderEvent.observed_at.desc(), BrokerOrderEvent.created_at.desc())
    ).all()
    for event in events:
        if str(event.event_source or "").lower() not in {"reconciliation", "realtime"}:
            continue
        if isinstance(event.payload, dict):
            candidates.append(("broker_order_events.payload", event.payload, event.observed_at, event.id))
            normalized_payload = event.payload.get("normalized_payload")
            if isinstance(normalized_payload, dict):
                candidates.append(("broker_order_events.payload.normalized_payload", normalized_payload, event.observed_at, event.id))
        if isinstance(event.response_payload, dict):
            candidates.append(("broker_order_events.response_payload", event.response_payload, event.observed_at, event.id))

    for source, payload, observed_at, event_id in candidates:
        fills = _proposed_fills_from_payload(
            payload=payload,
            order=order,
            observed_at=observed_at or order.submitted_at or datetime.now(UTC),
        )
        if fills and _fills_match_filled_order(order=order, fills=fills):
            return {
                "available_repair_evidence": {
                    "source": source,
                    "evidence_kind": "persisted_broker_payload",
                    "broker_order_event_id": event_id,
                    "observed_at": _serialize_datetime(observed_at),
                    "fields": sorted(_evidence_fields(payload)),
                    "validation": "exact_quantity_symbol_and_side_match",
                },
                "proposed_fills": fills,
            }
    return {
        "available_repair_evidence": None,
        "proposed_fills": [],
    }


def _fills_match_filled_order(
    *,
    order: BrokerOrder,
    fills: list[dict[str, object]],
) -> bool:
    if order.quantity is None or order.quantity <= 0:
        return False
    total_quantity = 0
    for fill in fills:
        if str(fill.get("symbol") or "").strip().upper() != order.symbol.strip().upper():
            return False
        if str(fill.get("side") or "").strip().lower() != order.side.strip().lower():
            return False
        quantity = _parse_int(fill.get("quantity"))
        price = _parse_decimal(fill.get("price"))
        if quantity is None or quantity <= 0 or price is None or price <= 0:
            return False
        total_quantity += quantity
    return total_quantity == order.quantity


def _proposed_fills_from_payload(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
    observed_at: datetime,
) -> list[dict[str, object]]:
    fills = payload.get("fills")
    if isinstance(fills, list):
        parsed = [
            parsed_fill
            for item in fills
            if isinstance(item, dict)
            for parsed_fill in [_normalize_explicit_fill(item, order=order, observed_at=observed_at)]
            if parsed_fill is not None
        ]
        if parsed:
            return parsed

    nested_order = payload.get("order")
    if isinstance(nested_order, dict):
        nested = _proposed_fills_from_payload(payload=nested_order, order=order, observed_at=observed_at)
        if nested:
            return nested

    rows = payload.get("output1")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            if _optional_str(row.get("odno")) not in {None, order.broker_order_id}:
                continue
            parsed = _cumulative_fill_from_payload(payload=row, order=order, observed_at=observed_at)
            if parsed:
                return parsed

    return _cumulative_fill_from_payload(payload=payload, order=order, observed_at=observed_at)


def _normalize_explicit_fill(
    fill: dict[str, object],
    *,
    order: BrokerOrder,
    observed_at: datetime,
) -> dict[str, object] | None:
    quantity = _parse_int(fill.get("quantity"))
    price = _parse_decimal(fill.get("price"))
    if quantity is None or quantity <= 0 or price is None:
        return None
    filled_at = _parse_timestamp(fill.get("filled_at"), fallback=observed_at)
    return {
        "broker_fill_id": _optional_str(fill.get("broker_fill_id")),
        "symbol": _optional_str(fill.get("symbol")) or order.symbol,
        "side": _optional_str(fill.get("side")) or order.side,
        "quantity": quantity,
        "price": _decimal_text(price),
        "filled_at": filled_at.isoformat(),
    }


def _cumulative_fill_from_payload(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
    observed_at: datetime,
) -> list[dict[str, object]]:
    quantity = _parse_int(payload.get("filled_quantity"))
    if quantity is None:
        quantity = _parse_int(payload.get("tot_ccld_qty"))
    if quantity is None or quantity <= 0:
        return []
    price = (
        _parse_decimal(payload.get("average_fill_price"))
        or _parse_decimal(payload.get("avg_upr"))
        or _parse_decimal(payload.get("trd_prc"))
        or _parse_decimal(payload.get("avg_pric"))
        or _parse_decimal(payload.get("avg_prvs"))
        or _parse_decimal(payload.get("ord_unpr"))
    )
    if price is None or price <= 0:
        return []
    filled_at = _parse_timestamp(
        payload.get("filled_at") or _kis_order_timestamp(payload),
        fallback=observed_at,
    )
    return [
        {
            "broker_fill_id": _optional_str(payload.get("broker_fill_id")),
            "symbol": _optional_str(payload.get("symbol")) or _optional_str(payload.get("pdno")) or order.symbol,
            "side": _optional_str(payload.get("side")) or order.side,
            "quantity": quantity,
            "price": _decimal_text(price),
            "filled_at": filled_at.isoformat(),
        }
    ]


def _duplicate_fill_groups(*, db: Session, runtime_mode: str) -> list[dict[str, object]]:
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == runtime_mode)
        .order_by(ExecutionFill.broker_order_pk.asc(), ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()
    groups: dict[str, list[ExecutionFill]] = defaultdict(list)
    for fill in fills:
        groups[execution_fill_dedupe_key(fill)].append(fill)
    duplicate_groups: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) <= 1:
            continue
        retained = group[0]
        duplicate_fills = group[1:]
        economic_conflict = duplicate_fill_economic_conflict(group)
        conflicting_fields = _duplicate_group_conflicting_fields(group)
        duplicate_groups.append(
            {
                "duplicate_group_key": key,
                "broker_order_pk": retained.broker_order_pk,
                "broker_order_id": retained.broker_order.broker_order_id if retained.broker_order is not None else None,
                "symbol": retained.symbol,
                "side": retained.side,
                "quantity": retained.quantity,
                "price": _decimal_text(Decimal(retained.price)),
                "filled_at": _serialize_datetime(retained.filled_at),
                "base_quantity": (
                    _decimal_text(Decimal(retained.base_quantity))
                    if retained.base_quantity is not None
                    else None
                ),
                "economic_conflict": economic_conflict,
                "conflicting_fields": conflicting_fields,
                "retained_fill": _fill_row_payload(retained),
                "duplicate_fills": [_fill_row_payload(fill) for fill in duplicate_fills],
            }
        )
    return duplicate_groups


def _fill_signature(*, order: BrokerOrder, fill: dict[str, object]) -> str | None:
    quantity = _parse_int(fill.get("quantity"))
    price = _parse_decimal(fill.get("price"))
    symbol = _optional_str(fill.get("symbol"))
    side = _optional_str(fill.get("side"))
    if quantity is None or quantity <= 0 or price is None or not symbol or not side:
        return None
    return execution_fill_idempotency_key(
        broker_order_pk=order.id,
        broker_fill_id=_optional_str(fill.get("broker_fill_id")),
        symbol=symbol,
        side=side,
        filled_at=_parse_timestamp(fill.get("filled_at"), fallback=datetime.now(UTC)),
        price=price,
        quantity=quantity,
    )


def _validated_repair_fill_proposals(
    *,
    order: BrokerOrder,
    proposed_fills: list[object],
    existing_keys: set[str],
) -> tuple[list[tuple[dict[str, object], str]], dict[str, object] | None]:
    """Preflight every fill so one order is repaired completely or not at all."""
    validated: list[tuple[dict[str, object], str]] = []
    signatures: list[str] = []
    proposed_quantity = 0
    for index, proposed in enumerate(proposed_fills):
        if not isinstance(proposed, dict):
            return [], {
                "code": "invalid_proposed_fill",
                "invalid_fill_index": index,
            }
        signature = _fill_signature(order=order, fill=proposed)
        quantity = _parse_int(proposed.get("quantity"))
        if signature is None or quantity is None or quantity <= 0:
            return [], {
                "code": "invalid_proposed_fill",
                "invalid_fill_index": index,
            }
        validated.append((proposed, signature))
        signatures.append(signature)
        proposed_quantity += quantity

    signature_counts: dict[str, int] = defaultdict(int)
    for signature in signatures:
        signature_counts[signature] += 1
    duplicate_signatures = sorted(
        signature for signature, count in signature_counts.items() if count > 1
    )
    existing_signature_collisions = sorted(set(signatures) & existing_keys)
    retained_quantity = 0
    retained_signatures: set[str] = set()
    for proposed, signature in validated:
        if signature in existing_signature_collisions or signature in retained_signatures:
            continue
        retained_signatures.add(signature)
        retained_quantity += _parse_int(proposed.get("quantity")) or 0
    expected_quantity = int(order.quantity or 0)
    if (
        duplicate_signatures
        or existing_signature_collisions
        or proposed_quantity != expected_quantity
        or retained_quantity != expected_quantity
    ):
        return [], {
            "code": "proposed_fill_set_not_insertable_atomically",
            "expected_quantity": expected_quantity,
            "proposed_quantity": proposed_quantity,
            "retained_quantity": retained_quantity,
            "duplicate_signatures": duplicate_signatures,
            "existing_signature_collisions": existing_signature_collisions,
        }
    return validated, None


def _annotate_remediation_reviews(
    *,
    db: Session,
    runtime_mode: str,
    candidate_type: str,
    items: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not items:
        return items
    candidate_keys = {
        str(item["broker_order_pk"])
        if candidate_type == MISSING_FILL_CANDIDATE_TYPE
        else _duplicate_group_candidate_key(item)
        for item in items
    }
    reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == candidate_type,
            RuntimeRemediationEvent.candidate_key.in_(sorted(candidate_keys)),
        )
    ).all()
    review_by_key = {review.candidate_key: review for review in reviews}
    for item in items:
        candidate_key = (
            str(item["broker_order_pk"])
            if candidate_type == MISSING_FILL_CANDIDATE_TYPE
            else _duplicate_group_candidate_key(item)
        )
        review = review_by_key.get(candidate_key)
        item["candidate_key"] = candidate_key
        item["repair_action"] = (
            "insert_authoritative_fill"
            if candidate_type == MISSING_FILL_CANDIDATE_TYPE and item.get("proposed_fills")
            else "record_manual_review"
            if (
                candidate_type == MISSING_FILL_CANDIDATE_TYPE
                or item.get("economic_conflict") is True
            )
            else "isolate_non_destructively"
        )
        if review is not None:
            item["review_outcome"] = review.outcome
            item["review_actor"] = review.actor
            item["review_reason"] = review.reason
            item["reviewed_at"] = _serialize_datetime(review.occurred_at)
    return items


def _duplicate_group_candidate_key(item: dict[str, object]) -> str:
    return ":".join(
        (
            str(item["duplicate_group_key"]),
            _duplicate_group_membership_version(item),
        )
    )


def _duplicate_group_membership_version(item: dict[str, object]) -> str:
    member_ids: list[str] = []
    retained = item.get("retained_fill")
    if isinstance(retained, dict) and retained.get("id") is not None:
        member_ids.append(str(retained["id"]))
    duplicate_fills = item.get("duplicate_fills")
    if isinstance(duplicate_fills, list):
        member_ids.extend(
            str(fill["id"])
            for fill in duplicate_fills
            if isinstance(fill, dict) and fill.get("id") is not None
        )
    membership = "\0".join(sorted(member_ids))
    return sha256(membership.encode("utf-8")).hexdigest()[:32]


def _duplicate_group_conflicting_fields(group: list[ExecutionFill]) -> list[str]:
    fields: list[str] = []
    if len({int(fill.quantity) for fill in group}) > 1:
        fields.append("quantity")
    if (
        len(
            {
                _decimal_text(Decimal(fill.base_quantity))
                if fill.base_quantity is not None
                else None
                for fill in group
            }
        )
        > 1
    ):
        fields.append("base_quantity")
    if len({_decimal_text(Decimal(fill.price)) for fill in group}) > 1:
        fields.append("price")
    return fields


def _record_remediation_event(
    *,
    db: Session,
    runtime_mode: str,
    candidate_type: str,
    candidate_key: str,
    subject_id: str | None,
    outcome: str,
    actor: str,
    reason: str,
    occurred_at: datetime,
    evidence_summary: dict[str, object],
) -> bool:
    existing = db.scalar(
        select(RuntimeRemediationEvent.id).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == candidate_type,
            RuntimeRemediationEvent.candidate_key == candidate_key,
            RuntimeRemediationEvent.outcome == outcome,
        )
    )
    if existing is not None:
        return False
    db.add(
        RuntimeRemediationEvent(
            mode=runtime_mode,
            candidate_type=candidate_type,
            candidate_key=candidate_key,
            subject_id=subject_id,
            outcome=outcome,
            actor=actor,
            reason=reason,
            evidence_summary=evidence_summary,
            occurred_at=occurred_at,
        )
    )
    return True


def _fill_row_payload(fill: ExecutionFill, *, id_override: str | None = "__fill_id__") -> dict[str, object]:
    payload = {
        "id": fill.id if id_override == "__fill_id__" else id_override,
        "idempotency_key": fill.idempotency_key,
        "broker_order_pk": fill.broker_order_pk,
        "broker_fill_id": fill.broker_fill_id,
        "symbol": fill.symbol,
        "side": fill.side,
        "quantity": fill.quantity,
        "base_quantity": (
            _decimal_text(Decimal(fill.base_quantity))
            if fill.base_quantity is not None
            else None
        ),
        "price": _decimal_text(Decimal(fill.price)),
        "filled_at": _serialize_datetime(fill.filled_at),
        "created_at": _serialize_datetime(fill.created_at),
        "updated_at": _serialize_datetime(fill.updated_at),
    }
    if payload["id"] is None:
        payload.pop("id")
    return payload


def _evidence_fields(payload: dict[str, object]) -> set[str]:
    fields = set(payload.keys())
    nested = payload.get("order")
    if isinstance(nested, dict):
        fields.update(f"order.{key}" for key in nested.keys())
    return fields


def _kis_order_timestamp(payload: dict[str, object]) -> str | None:
    date_value = _optional_str(payload.get("ord_dt"))
    time_value = _optional_str(payload.get("ord_tmd"))
    if not date_value or not time_value:
        return None
    # KIS domestic order inquiry fields are Korea-local wall-clock values.
    # Include the offset so repair fills do not reinterpret them as UTC.
    return f"{date_value}{time_value}+0900"


def _parse_timestamp(value: object, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        text = value.strip()
        for candidate in (text, text.replace(" ", "T"), text.replace("Z", "+00:00")):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y%m%d%H%M%S%z",
                "%Y%m%d%H%M%S",
            ):
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    return (
                        parsed.astimezone(UTC)
                        if parsed.tzinfo is not None
                        else parsed.replace(tzinfo=UTC)
                    )
                except (TypeError, ValueError):
                    pass
        try:
            parsed = datetime.fromisoformat(text)
            return (
                parsed.astimezone(UTC)
                if parsed.tzinfo is not None
                else parsed.replace(tzinfo=UTC)
            )
        except ValueError:
            pass
    return fallback


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float):
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            return Decimal(value.strip())
        except (InvalidOperation, ValueError):
            return None
    return None


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(Decimal(value.strip()))
        except (InvalidOperation, ValueError):
            return None
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _decimal_text(value: Decimal) -> str:
    return f"{value:.4f}"


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value
