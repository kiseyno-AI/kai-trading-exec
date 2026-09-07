from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from app.models.runtime import BrokerOrder, ExecutionFill


COST_COVERAGE_COMPLETE = "complete"
COST_COVERAGE_PARTIAL = "partial"
COST_COVERAGE_UNKNOWN = "unknown"
_COST_COVERAGE_VALUES = {
    COST_COVERAGE_COMPLETE,
    COST_COVERAGE_PARTIAL,
    COST_COVERAGE_UNKNOWN,
}
_COST_QUANTUM = Decimal("0.00000001")


@dataclass(frozen=True)
class FillEconomics:
    settlement_currency: str | None
    transaction_cost: Decimal | None
    transaction_cost_coverage: str
    transaction_cost_source: str | None
    settlement_currency_conflict: bool = False


@dataclass(frozen=True)
class ClosedTradeEconomics:
    settlement_currency: str | None
    buy_settlement_currency: str | None
    sell_settlement_currency: str | None
    gross_realized_pnl: Decimal
    buy_transaction_cost: Decimal | None
    sell_transaction_cost: Decimal | None
    known_transaction_cost: Decimal | None
    net_realized_pnl: Decimal | None
    transaction_cost_coverage: str


def normalize_fill_economics(
    payload: object,
    *,
    order: BrokerOrder,
) -> FillEconomics:
    """Normalize explicit broker costs and deterministic settlement currency.

    A missing fee is not a zero fee. Callers must not publish a net result
    unless every contributing fill has complete transaction-cost coverage.
    """

    source_payload = _economics_payload(payload)
    reported_currency = _normalized_currency(
        _first_present(
            source_payload,
            "settlement_currency",
            "quote_currency",
            "currency",
        )
    )
    expected_currency = settlement_currency_for_order(order)
    currency_conflict = (
        reported_currency is not None
        and expected_currency is not None
        and reported_currency != expected_currency
    )
    currency = None if currency_conflict else (reported_currency or expected_currency)

    explicit_coverage = _normalized_coverage(
        _first_present(
            source_payload,
            "transaction_cost_coverage",
            "cost_coverage",
        )
    )
    explicit_source = _optional_text(
        _first_present(
            source_payload,
            "transaction_cost_source",
            "cost_source",
        )
    )

    total_key, total_value = _first_decimal(
        source_payload,
        "total_transaction_cost",
        "transaction_cost",
    )
    if total_value is not None and total_value >= 0:
        return FillEconomics(
            settlement_currency=currency,
            transaction_cost=total_value,
            transaction_cost_coverage=explicit_coverage or COST_COVERAGE_COMPLETE,
            transaction_cost_source=explicit_source or f"broker_payload:{total_key}",
            settlement_currency_conflict=currency_conflict,
        )

    component_values: list[Decimal] = []
    component_keys: list[str] = []
    component_groups = (
        ("commission", "commission_amount"),
        ("fee", "fee_amount"),
        ("tax", "tax_amount"),
    )
    complete_components = True
    for aliases in component_groups:
        key, value = _first_decimal(source_payload, *aliases)
        if value is None or value < 0:
            complete_components = False
            continue
        component_keys.append(key)
        component_values.append(value)

    if component_values:
        coverage = explicit_coverage or (
            COST_COVERAGE_COMPLETE if complete_components else COST_COVERAGE_PARTIAL
        )
        return FillEconomics(
            settlement_currency=currency,
            transaction_cost=sum(component_values, Decimal("0")),
            transaction_cost_coverage=coverage,
            transaction_cost_source=explicit_source
            or "broker_payload:" + "+".join(component_keys),
            settlement_currency_conflict=currency_conflict,
        )

    return FillEconomics(
        settlement_currency=currency,
        transaction_cost=None,
        transaction_cost_coverage=(
            COST_COVERAGE_PARTIAL
            if explicit_coverage == COST_COVERAGE_PARTIAL
            else COST_COVERAGE_UNKNOWN
        ),
        transaction_cost_source=explicit_source,
        settlement_currency_conflict=currency_conflict,
    )


def execution_fill_economics(fill: ExecutionFill) -> FillEconomics:
    payload = fill.payload if isinstance(fill.payload, dict) else {}
    nested_payload = payload.get("economics")
    nested_currency_conflict = (
        isinstance(nested_payload, dict)
        and nested_payload.get("settlement_currency_conflict") is True
    )
    stored_currency = _normalized_currency(fill.settlement_currency)
    expected_currency = (
        settlement_currency_for_order(fill.broker_order)
        if fill.broker_order is not None
        else None
    )
    currency_conflict = (
        payload.get("settlement_currency_conflict") is True
        or nested_currency_conflict
        or (
            stored_currency is not None
            and expected_currency is not None
            and stored_currency != expected_currency
        )
    )
    currency = None if currency_conflict else (stored_currency or expected_currency)
    cost = _decimal(fill.transaction_cost)
    if cost is not None and cost < 0:
        cost = None
    coverage = _normalized_coverage(fill.transaction_cost_coverage)
    if coverage is None:
        coverage = COST_COVERAGE_UNKNOWN
    source = _optional_text(fill.transaction_cost_source)
    if coverage == COST_COVERAGE_COMPLETE and (cost is None or source is None):
        coverage = COST_COVERAGE_UNKNOWN
    return FillEconomics(
        settlement_currency=currency,
        transaction_cost=cost,
        transaction_cost_coverage=coverage,
        transaction_cost_source=source,
        settlement_currency_conflict=currency_conflict,
    )


def allocated_transaction_cost(
    fill: ExecutionFill,
    *,
    matched_quantity: Decimal,
) -> tuple[Decimal | None, str]:
    economics = execution_fill_economics(fill)
    if economics.transaction_cost is None:
        return None, economics.transaction_cost_coverage
    fill_quantity = effective_fill_quantity(fill)
    if fill_quantity <= 0 or matched_quantity < 0 or matched_quantity > fill_quantity:
        return None, COST_COVERAGE_UNKNOWN
    allocated = (
        economics.transaction_cost * matched_quantity / fill_quantity
    ).quantize(_COST_QUANTUM)
    return allocated, economics.transaction_cost_coverage


def closed_trade_economics(
    *,
    buy_fill: ExecutionFill | None,
    sell_fill: ExecutionFill,
    matched_quantity: Decimal,
    gross_realized_pnl: Decimal,
) -> ClosedTradeEconomics:
    buy_economics = execution_fill_economics(buy_fill) if buy_fill is not None else None
    sell_economics = execution_fill_economics(sell_fill)
    buy_currency = buy_economics.settlement_currency if buy_economics is not None else None
    sell_currency = sell_economics.settlement_currency
    currency = (
        sell_currency
        if buy_fill is None or (buy_currency is not None and buy_currency == sell_currency)
        else None
    )

    if buy_fill is None:
        buy_cost, buy_coverage = None, COST_COVERAGE_UNKNOWN
    else:
        buy_cost, buy_coverage = allocated_transaction_cost(
            buy_fill,
            matched_quantity=matched_quantity,
        )
    sell_cost, sell_coverage = allocated_transaction_cost(
        sell_fill,
        matched_quantity=matched_quantity,
    )
    known_cost_values = [cost for cost in (buy_cost, sell_cost) if cost is not None]
    known_cost = (
        sum(known_cost_values, Decimal("0")).quantize(_COST_QUANTUM)
        if currency is not None and known_cost_values
        else None
    )
    coverage = _combined_cost_coverage(buy_coverage, sell_coverage)
    if currency is None:
        coverage = COST_COVERAGE_UNKNOWN
    net = (
        (gross_realized_pnl - known_cost).quantize(_COST_QUANTUM)
        if coverage == COST_COVERAGE_COMPLETE and known_cost is not None
        else None
    )
    return ClosedTradeEconomics(
        settlement_currency=currency,
        buy_settlement_currency=buy_currency,
        sell_settlement_currency=sell_currency,
        gross_realized_pnl=gross_realized_pnl.quantize(Decimal("0.0001")),
        buy_transaction_cost=buy_cost,
        sell_transaction_cost=sell_cost,
        known_transaction_cost=known_cost,
        net_realized_pnl=net,
        transaction_cost_coverage=coverage,
    )


def summarize_closed_trade_economics(
    rows: list[ClosedTradeEconomics],
) -> dict[str, object]:
    by_currency: dict[str, dict[str, object]] = {}
    unknown_currency_count = 0
    unknown_currency_gross = Decimal("0")
    for row in rows:
        currency = row.settlement_currency
        if currency is None:
            unknown_currency_count += 1
            unknown_currency_gross += row.gross_realized_pnl
            continue
        bucket = by_currency.setdefault(
            currency,
            {
                "gross_realized_pnl": Decimal("0"),
                "known_transaction_cost": Decimal("0"),
                "known_transaction_cost_lot_count": 0,
                "covered_net_realized_pnl": Decimal("0"),
                "closed_lot_count": 0,
                "complete_cost_lot_count": 0,
            },
        )
        bucket["gross_realized_pnl"] = Decimal(
            str(bucket["gross_realized_pnl"])
        ) + row.gross_realized_pnl
        if row.known_transaction_cost is not None:
            bucket["known_transaction_cost"] = Decimal(
                str(bucket["known_transaction_cost"])
            ) + row.known_transaction_cost
            bucket["known_transaction_cost_lot_count"] = int(
                bucket["known_transaction_cost_lot_count"]
            ) + 1
        bucket["closed_lot_count"] = int(bucket["closed_lot_count"]) + 1
        if row.net_realized_pnl is not None:
            bucket["covered_net_realized_pnl"] = Decimal(
                str(bucket["covered_net_realized_pnl"])
            ) + row.net_realized_pnl
            bucket["complete_cost_lot_count"] = int(
                bucket["complete_cost_lot_count"]
            ) + 1

    serialized: dict[str, dict[str, object]] = {}
    for currency, bucket in sorted(by_currency.items()):
        lot_count = int(bucket["closed_lot_count"])
        complete_count = int(bucket["complete_cost_lot_count"])
        known_cost_count = int(bucket["known_transaction_cost_lot_count"])
        known_cost = Decimal(str(bucket["known_transaction_cost"])).quantize(
            _COST_QUANTUM
        )
        gross = Decimal(str(bucket["gross_realized_pnl"])).quantize(Decimal("0.0001"))
        covered_net = Decimal(str(bucket["covered_net_realized_pnl"])).quantize(
            _COST_QUANTUM
        )
        currency_rows = [row for row in rows if row.settlement_currency == currency]
        coverage = (
            COST_COVERAGE_COMPLETE
            if complete_count == lot_count
            else (
                COST_COVERAGE_PARTIAL
                if any(
                    row.transaction_cost_coverage
                    in {COST_COVERAGE_COMPLETE, COST_COVERAGE_PARTIAL}
                    for row in currency_rows
                )
                else COST_COVERAGE_UNKNOWN
            )
        )
        serialized[currency] = {
            "currency": currency,
            "gross_realized_pnl": gross,
            "known_transaction_cost": known_cost if known_cost_count > 0 else None,
            "net_realized_pnl": covered_net if coverage == COST_COVERAGE_COMPLETE else None,
            "covered_net_realized_pnl": covered_net,
            "transaction_cost_coverage": coverage,
            "closed_lot_count": lot_count,
            "complete_cost_lot_count": complete_count,
        }

    return {
        "by_currency": serialized,
        "currency_coverage": "complete" if unknown_currency_count == 0 else "incomplete",
        "unknown_currency_lot_count": unknown_currency_count,
        "unknown_currency_gross_realized_pnl": unknown_currency_gross.quantize(
            Decimal("0.0001")
        ),
    }


def effective_fill_quantity(fill: ExecutionFill) -> Decimal:
    if fill.base_quantity is not None and fill.base_quantity > 0:
        return Decimal(fill.base_quantity)
    return Decimal(fill.quantity)


def settlement_currency_for_order(order: BrokerOrder) -> str | None:
    asset_class = str(order.asset_class or "").strip().lower()
    provider = str(order.broker_name or "").strip().lower()
    venue = str(order.venue or "").strip().upper()
    symbol = str(order.symbol or "").strip().upper()

    if asset_class == "us_equity":
        return "USD"
    if provider == "alpaca":
        return "USD"
    if provider == "upbit":
        quote = symbol.split("-", 1)[0] if "-" in symbol else ""
        return _normalized_currency(quote)
    if provider == "kis":
        if venue in {"NASD", "NYSE", "AMEX", "US"}:
            return "USD"
        if venue in {"KRX", "NXT", "SOR"}:
            return "KRW"
        if asset_class in {"stock", "kr_equity", "domestic_equity"}:
            return "KRW"
        if len(symbol) == 6 and symbol.isdigit():
            return "KRW"
        # Legacy KIS rows without market metadata are not safely attributable
        # to either the domestic or overseas settlement currency.
        return None
    return None


def economics_payload(economics: FillEconomics) -> dict[str, object]:
    return {
        "settlement_currency": economics.settlement_currency,
        "transaction_cost": economics.transaction_cost,
        "transaction_cost_coverage": economics.transaction_cost_coverage,
        "transaction_cost_source": economics.transaction_cost_source,
        "settlement_currency_conflict": economics.settlement_currency_conflict,
    }


def _economics_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    economics = payload.get("economics")
    if isinstance(economics, dict):
        return {**payload, **economics}
    return payload


def _first_present(payload: dict[str, Any], *keys: str) -> object:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _first_decimal(payload: dict[str, Any], *keys: str) -> tuple[str, Decimal | None]:
    for key in keys:
        if key not in payload:
            continue
        value = _decimal(payload[key])
        if value is not None:
            return key, value
    return "", None


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _normalized_currency(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if not normalized or len(normalized) > 8 or not normalized.isalnum():
        return None
    return normalized


def _normalized_coverage(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in _COST_COVERAGE_VALUES else None


def _combined_cost_coverage(*values: str) -> str:
    if values and all(value == COST_COVERAGE_COMPLETE for value in values):
        return COST_COVERAGE_COMPLETE
    if any(value in {COST_COVERAGE_COMPLETE, COST_COVERAGE_PARTIAL} for value in values):
        return COST_COVERAGE_PARTIAL
    return COST_COVERAGE_UNKNOWN


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None
