from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models.runtime import ExecutionFill


TERMINAL_FILL_EVIDENCE_STATUSES = (
    "filled",
    "canceled",
    "rejected",
    "failed",
    "expired",
)
BROKER_FILLED_QUANTITY_KEYS = frozenset(
    {
        "filled_quantity",
        "ft_ccld_qty",
        "tot_ccld_qty",
        "ccld_qty",
    }
)


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
    conservative_representatives = {
        key: max(group, key=_fill_chronology_risk_rank)
        for key, group in groups.items()
        if key not in conflicting_keys
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
        unique.append(conservative_representatives[key])
    return unique


def duplicate_fill_economic_conflict(fills: list[ExecutionFill]) -> bool:
    return bool(duplicate_fill_conflicting_fields(fills))


def execution_fills_have_conflicting_identity(fills: list[ExecutionFill]) -> bool:
    groups: dict[str, list[ExecutionFill]] = {}
    for fill in fills:
        groups.setdefault(execution_fill_dedupe_key(fill), []).append(fill)
    return any(
        len(group) > 1 and duplicate_fill_economic_conflict(group)
        for group in groups.values()
    )


def execution_fills_have_relevant_conflicting_identity(
    fills: list[ExecutionFill],
    *,
    is_relevant: Callable[[ExecutionFill], bool],
) -> bool:
    """Whether a conflicting fill identity has any relevant observation.

    Consumers commonly isolate old inventory with a verified-flat boundary or
    bound a report to a review period.  Filtering rows before grouping is not
    safe: a broker identity can have one observation before the boundary and a
    conflicting observation after it.  Group the complete scoped evidence
    first, then ignore a conflict only when every member is outside the
    consumer's relevant generation/period.
    """

    groups: dict[str, list[ExecutionFill]] = {}
    for fill in fills:
        groups.setdefault(execution_fill_dedupe_key(fill), []).append(fill)
    return any(
        len(group) > 1
        and duplicate_fill_economic_conflict(group)
        and any(is_relevant(fill) for fill in group)
        for group in groups.values()
    )


def duplicate_fill_conflicting_fields(fills: list[ExecutionFill]) -> list[str]:
    """Return every persisted fact that disagrees inside one fill identity.

    Duplicate broker identities are safe to collapse only when both the trade
    and its economics agree.  Choosing one currency, fee, coverage state, or
    provenance would silently manufacture P/L evidence, so readers exclude the
    whole group and remediation records it for manual review instead.
    """

    field_values = (
        (
            "filled_at",
            {_timestamp_key(fill.filled_at) for fill in fills},
        ),
        (
            "symbol",
            {str(fill.symbol or "").strip().upper() for fill in fills},
        ),
        (
            "side",
            {str(fill.side or "").strip().lower() for fill in fills},
        ),
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


def _fill_chronology_risk_rank(fill: ExecutionFill) -> int:
    """Prefer the duplicate carrying the most conservative chronology proof."""

    payload = fill.payload if isinstance(fill.payload, dict) else {}
    precision = payload.get("fill_time_precision")
    if precision == "observed_upper_bound":
        return 3
    if precision == "exact":
        return 0
    if payload.get("source") == "operator_fill_evidence_repair":
        return 2
    if payload.get("price_derivation") == "cumulative_notional_delta" or any(
        key in payload
        for key in (
            "cumulative_quantity",
            "cumulative_base_quantity",
            "cumulative_quote_notional",
        )
    ):
        return 2
    return 1


def _timestamp_key(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _decimal_key(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _optional_decimal_key(value: Decimal | None) -> str:
    return "" if value is None else _decimal_key(value)


def broker_reported_filled_quantity(order: object) -> Decimal | None:
    """Return conservative positive integer fill evidence from broker payloads."""

    evidence_payloads: list[object] = [
        getattr(order, "response_metadata", None),
        getattr(order, "raw_broker_payload", None),
    ]
    for event in getattr(order, "events", ()):
        if str(getattr(event, "event_source", "") or "").strip().lower() not in {
            "reconciliation",
            "realtime",
        }:
            continue
        evidence_payloads.extend(
            (
                getattr(event, "payload", None),
                getattr(event, "response_payload", None),
            )
        )
    quantities = [
        quantity
        for payload in evidence_payloads
        for key, value in _nested_payload_items(payload)
        if key.strip().lower() in BROKER_FILLED_QUANTITY_KEYS
        for quantity in [_positive_integral_decimal(value)]
        if quantity is not None
    ]
    return max(quantities) if quantities else None


def _nested_payload_items(payload: object) -> list[tuple[str, object]]:
    pending: list[object] = [payload]
    items: list[tuple[str, object]] = []
    visited: set[int] = set()
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            identity = id(value)
            if identity in visited:
                continue
            visited.add(identity)
            for key, nested in value.items():
                items.append((str(key), nested))
                if isinstance(nested, dict | list):
                    pending.append(nested)
        elif isinstance(value, list):
            identity = id(value)
            if identity in visited:
                continue
            visited.add(identity)
            pending.extend(value)
    return items


def _positive_integral_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None
    if (
        not parsed.is_finite()
        or parsed <= 0
        or parsed != parsed.to_integral_value()
    ):
        return None
    return parsed
