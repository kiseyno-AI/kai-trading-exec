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
    return execution_fill_idempotency_key(
        broker_order_pk=fill.broker_order_pk,
        broker_fill_id=fill.broker_fill_id,
        symbol=fill.symbol,
        side=fill.side,
        filled_at=fill.filled_at,
        price=Decimal(fill.price),
        quantity=int(fill.quantity),
        base_quantity=(Decimal(fill.base_quantity) if fill.base_quantity is not None else None),
        quote_notional=(Decimal(fill.quote_notional) if fill.quote_notional is not None else None),
    )


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
            unique.append(fill)
            continue
        if key in seen:
            continue
        seen.add(key)
        unique.append(fill)
    return unique


def duplicate_fill_economic_conflict(fills: list[ExecutionFill]) -> bool:
    signatures = {
        (
            int(fill.quantity),
            _optional_decimal_key(
                Decimal(fill.base_quantity) if fill.base_quantity is not None else None
            ),
            _decimal_key(Decimal(fill.price)),
        )
        for fill in fills
    }
    return len(signatures) > 1


def _timestamp_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _decimal_key(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _optional_decimal_key(value: Decimal | None) -> str:
    return "" if value is None else _decimal_key(value)
