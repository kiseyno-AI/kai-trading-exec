from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill
from app.services.fill_idempotency import (
    execution_fill_dedupe_key,
    execution_fill_idempotency_key,
)


REPAIR_EVENT_SOURCE = "operator_repair"
MISSING_FILL_REPAIR_EVENT = "execution_fill_missing_repair"
DUPLICATE_FILL_REPAIR_EVENT = "execution_fill_duplicate_repair"


def analyze_execution_fill_repairs(
    *,
    db: Session,
    runtime_mode: str,
) -> dict[str, object]:
    missing = _missing_fill_items(db=db, runtime_mode=runtime_mode)
    duplicates = _duplicate_fill_groups(db=db, runtime_mode=runtime_mode)
    return _repair_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        missing=missing,
        duplicates=duplicates,
        inserted_fill_count=0,
        deleted_fill_count=0,
        audit_event_count=0,
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
    deleted_fill_count = 0
    audit_event_count = 0

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
            continue
        inserted_for_order: list[dict[str, object]] = []
        for proposed in proposed_fills:
            if not isinstance(proposed, dict):
                continue
            signature = _fill_signature(order=order, fill=proposed)
            if signature is None or signature in existing_keys:
                continue
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
        deleted = group.get("deleted_fills")
        if not isinstance(retained, dict) or not isinstance(deleted, list) or not deleted:
            continue
        retained_id = str(retained["id"])
        retained_fill = db.get(ExecutionFill, retained_id)
        if retained_fill is None:
            continue
        duplicate_ids = [str(row["id"]) for row in deleted if isinstance(row, dict)]
        duplicate_rows = db.scalars(
            select(ExecutionFill).where(ExecutionFill.id.in_(duplicate_ids))
        ).all()
        if not duplicate_rows:
            continue
        order = retained_fill.broker_order
        db.add(
            BrokerOrderEvent(
                broker_order_pk=retained_fill.broker_order_pk,
                event_type=DUPLICATE_FILL_REPAIR_EVENT,
                event_source=REPAIR_EVENT_SOURCE,
                normalized_status=order.status if order is not None else None,
                broker_status=None,
                message="operator repaired duplicate execution fill evidence",
                payload={
                    "actor": actor,
                    "reason": reason,
                    "repaired_at": repaired_at.isoformat(),
                    "runtime_mode": runtime_mode,
                    "retained_fill": retained,
                    "deleted_fills": [_fill_row_payload(row) for row in duplicate_rows],
                    "duplicate_group_key": group.get("duplicate_group_key"),
                },
                observed_at=repaired_at,
            )
        )
        audit_event_count += 1
        for duplicate in duplicate_rows:
            db.delete(duplicate)
            deleted_fill_count += 1
        if retained_fill.idempotency_key is None:
            retained_fill.idempotency_key = execution_fill_dedupe_key(retained_fill)

    db.commit()
    remaining_missing = _missing_fill_items(db=db, runtime_mode=runtime_mode)
    remaining_duplicates = _duplicate_fill_groups(db=db, runtime_mode=runtime_mode)
    return _repair_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        missing=remaining_missing,
        duplicates=remaining_duplicates,
        inserted_fill_count=inserted_fill_count,
        deleted_fill_count=deleted_fill_count,
        audit_event_count=audit_event_count,
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
) -> dict[str, object]:
    proposed_missing = sum(
        len(item.get("proposed_fills") or [])
        for item in missing
        if isinstance(item.get("proposed_fills"), list)
    )
    proposed_deleted = sum(
        len(group.get("deleted_fills") or [])
        for group in duplicates
        if isinstance(group.get("deleted_fills"), list)
    )
    return {
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "missing_filled_order_count": len(missing),
            "proposed_missing_fill_insert_count": proposed_missing,
            "duplicate_fill_group_count": len(duplicates),
            "proposed_duplicate_fill_delete_count": proposed_deleted,
            "inserted_fill_count": inserted_fill_count,
            "deleted_fill_count": deleted_fill_count,
            "audit_event_count": audit_event_count,
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
        if fills:
            return {
                "available_repair_evidence": {
                    "source": source,
                    "broker_order_event_id": event_id,
                    "observed_at": _serialize_datetime(observed_at),
                    "fields": sorted(_evidence_fields(payload)),
                },
                "proposed_fills": fills,
            }
    return {
        "available_repair_evidence": None,
        "proposed_fills": [],
    }


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
        deleted = group[1:]
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
                "retained_fill": _fill_row_payload(retained),
                "deleted_fills": [_fill_row_payload(fill) for fill in deleted],
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


def _fill_row_payload(fill: ExecutionFill, *, id_override: str | None = "__fill_id__") -> dict[str, object]:
    payload = {
        "id": fill.id if id_override == "__fill_id__" else id_override,
        "idempotency_key": fill.idempotency_key,
        "broker_order_pk": fill.broker_order_pk,
        "broker_fill_id": fill.broker_fill_id,
        "symbol": fill.symbol,
        "side": fill.side,
        "quantity": fill.quantity,
        "price": _decimal_text(Decimal(fill.price)),
        "filled_at": _serialize_datetime(fill.filled_at),
        "created_at": _serialize_datetime(fill.created_at),
        "updated_at": _serialize_datetime(fill.updated_at),
        "payload": _json_safe_value(fill.payload),
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
