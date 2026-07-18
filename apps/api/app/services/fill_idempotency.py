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
    )


def dedupe_execution_fills(fills: list[ExecutionFill]) -> list[ExecutionFill]:
    seen: set[str] = set()
    unique: list[ExecutionFill] = []
    for fill in fills:
        key = execution_fill_dedupe_key(fill)
        if key in seen:
            continue
        seen.add(key)
        unique.append(fill)
    return unique


def _timestamp_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _decimal_key(value: Decimal) -> str:
    return format(value.normalize(), "f")
