from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models.runtime import ExecutionFill


def execution_fill_idempotency_key(
    *,
    broker_order_pk: str,
    broker_fill_id: str | None,
    symbol: str,
    side: str,
    filled_at: datetime,
    price: Decimal,
    quantity: int,
    base_quantity: Decimal | None = None,
    quote_notional: Decimal | None = None,
) -> str:
    if broker_fill_id:
        parts = ("broker", broker_order_pk, broker_fill_id.strip())
    else:
        parts = (
            "fallback",
            broker_order_pk,
            symbol.strip().upper(),
            side.strip().lower(),
            _timestamp_key(filled_at),
            _decimal_key(price),
            str(int(quantity)),
            _optional_decimal_key(base_quantity),
            _optional_decimal_key(quote_notional),
        )
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"execution_fill:{digest}"


def execution_fill_dedupe_key(fill: ExecutionFill) -> str:
    if fill.idempotency_key:
        return fill.idempotency_key
    broker_order_pk = fill.broker_order_pk
    if not broker_order_pk and fill.broker_order is not None:
        broker_order_pk = fill.broker_order.id
    if not broker_order_pk:
        # A transient fill without an order identity cannot be compared safely
        # with fills from another transient order. Keep its scope unique rather
        # than collapsing unrelated broker evidence under a shared None key.
        broker_order_pk = f"unpersisted:{fill.id or id(fill)}"
    return execution_fill_idempotency_key(
        broker_order_pk=str(broker_order_pk),
        broker_fill_id=fill.broker_fill_id,
        symbol=fill.symbol,
        side=fill.side,
        filled_at=fill.filled_at,
        price=Decimal(fill.price),
        quantity=int(fill.quantity),
        base_quantity=(Decimal(fill.base_quantity) if fill.base_quantity is not None else None),
        quote_notional=(Decimal(fill.quote_notional) if fill.quote_notional is not None else None),
    )


def duplicate_fill_group_candidate_key(*, group_key: str, fill_ids: list[str]) -> str:
    membership = "\0".join(sorted(fill_ids))
    membership_version = sha256(membership.encode("utf-8")).hexdigest()[:32]
    return f"{group_key}:{membership_version}"


def dedupe_execution_fills(fills: list[ExecutionFill]) -> list[ExecutionFill]:
    groups: dict[str, list[ExecutionFill]] = {}
    for fill in fills:
        groups.setdefault(execution_fill_dedupe_key(fill), []).append(fill)

    conflicting_keys = {
        key for key, group in groups.items() if duplicate_fill_economic_conflict(group)
    }

    seen: set[str] = set()
    unique: list[ExecutionFill] = []
    for fill in fills:
        key = execution_fill_dedupe_key(fill)
        if key in conflicting_keys:
            continue
        if key in seen:
            continue
        seen.add(key)
        unique.append(fill)
    return unique


def duplicate_fill_economic_conflict(fills: list[ExecutionFill]) -> bool:
    return bool(duplicate_fill_conflicting_fields(fills))


def duplicate_fill_conflicting_fields(fills: list[ExecutionFill]) -> list[str]:
    """Return every persisted fact that disagrees inside one fill identity.

    Duplicate broker identities are safe to collapse only when both the trade
    and its economics agree.  Choosing one currency, fee, coverage state, or
    provenance would silently manufacture P/L evidence, so readers exclude the
    whole group and remediation records it for manual review instead.
    """

    field_values = (
        ("quantity", {int(fill.quantity) for fill in fills}),
        (
            "base_quantity",
            {
                _optional_decimal_key(
                    Decimal(fill.base_quantity)
                    if fill.base_quantity is not None
                    else None
                )
                for fill in fills
            },
        ),
        (
            "quote_notional",
            {
                _optional_decimal_key(
                    Decimal(fill.quote_notional)
                    if fill.quote_notional is not None
                    else None
                )
                for fill in fills
            },
        ),
        ("price", {_decimal_key(Decimal(fill.price)) for fill in fills}),
        (
            "settlement_currency",
            {
                str(fill.settlement_currency or "").strip().upper()
                for fill in fills
            },
        ),
        (
            "transaction_cost",
            {
                _optional_decimal_key(
                    Decimal(fill.transaction_cost)
                    if fill.transaction_cost is not None
                    else None
                )
                for fill in fills
            },
        ),
        (
            "transaction_cost_coverage",
            {
                str(fill.transaction_cost_coverage or "unknown").strip().lower()
                for fill in fills
            },
        ),
        (
            "transaction_cost_source",
            {
                str(fill.transaction_cost_source or "").strip()
                for fill in fills
            },
        ),
        (
            "settlement_currency_conflict",
            {_stored_currency_conflict(fill) for fill in fills},
        ),
    )
    return [name for name, values in field_values if len(values) > 1]


def _stored_currency_conflict(fill: ExecutionFill) -> bool:
    payload = fill.payload if isinstance(fill.payload, dict) else {}
    nested = payload.get("economics")
    return bool(
        payload.get("settlement_currency_conflict") is True
        or (
            isinstance(nested, dict)
            and nested.get("settlement_currency_conflict") is True
        )
    )


def _timestamp_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _decimal_key(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _optional_decimal_key(value: Decimal | None) -> str:
    return "" if value is None else _decimal_key(value)
