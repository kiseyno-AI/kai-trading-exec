from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, TypeVar

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.models.runtime import ExecutionPosition
from app.services.position_identity import (
    PositionIdentityConflictError,
    position_identity_from_columns,
)


@dataclass(frozen=True)
class BrokerRawHolding:
    symbol: str
    quantity: Decimal
    average_price: Decimal | None
    current_price: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None
    unrealized_pnl_pct: Decimal | None
    sellable_quantity: Decimal | None
    name: str | None
    raw_payload: dict[str, object]


HoldingT = TypeVar("HoldingT", BrokerAccountHolding, BrokerRawHolding)


def broker_raw_balance_is_valid(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    balance = payload.get("balance")
    if not isinstance(balance, dict):
        return False
    return str(balance.get("rt_cd", "")).strip() == "0"


def broker_raw_balance_is_fresh(
    *,
    observed_at: datetime | None,
    as_of: datetime,
    max_age_seconds: int,
) -> bool:
    if observed_at is None:
        return False
    observed = _normalize_utc(observed_at)
    return observed <= as_of and observed >= as_of - timedelta(seconds=max_age_seconds)


def broker_raw_holdings_from_payload(payload: dict[str, Any] | None) -> list[BrokerRawHolding]:
    if not broker_raw_balance_is_valid(payload):
        return []
    balance = payload.get("balance")
    if not isinstance(balance, dict):
        return []
    holdings: list[BrokerRawHolding] = []
    for row in _payload_rows(balance.get("output1")):
        symbol = str(row.get("pdno") or row.get("symbol") or "").strip().upper()
        quantity = _decimal_value(row.get("hldg_qty"))
        if not symbol or quantity is None or quantity <= 0:
            continue
        holdings.append(
            BrokerRawHolding(
                symbol=symbol,
                quantity=quantity,
                average_price=_decimal_value(row.get("pchs_avg_pric")),
                current_price=_decimal_value(row.get("prpr") or row.get("now_pric") or row.get("cur_prc")),
                market_value=_decimal_value(row.get("evlu_amt")),
                unrealized_pnl=_decimal_value(row.get("evlu_pfls_amt")),
                unrealized_pnl_pct=_decimal_value(row.get("evlu_pfls_rt")),
                sellable_quantity=_decimal_value(row.get("ord_psbl_qty")),
                name=_optional_text(row.get("prdt_name") or row.get("prdt_name120")),
                raw_payload=dict(row),
            )
        )
    return sorted(holdings, key=lambda row: row.symbol)


def broker_holdings_from_raw_balance(payload: dict[str, Any] | None) -> list[BrokerAccountHolding]:
    return [
        BrokerAccountHolding(
            symbol=holding.symbol,
            quantity=int(holding.quantity),
            average_price=holding.average_price,
            sellable_quantity=int(holding.sellable_quantity) if holding.sellable_quantity is not None else None,
            unrealized_pnl=holding.unrealized_pnl,
            unrealized_pnl_pct=holding.unrealized_pnl_pct,
            payload=holding.raw_payload,
        )
        for holding in broker_raw_holdings_from_payload(payload)
    ]


def snapshot_with_raw_balance_holdings(
    snapshot: BrokerAccountSnapshot,
    *,
    as_of: datetime,
    max_age_seconds: int,
) -> BrokerAccountSnapshot:
    if not broker_raw_balance_is_fresh(
        observed_at=snapshot.observed_at,
        as_of=as_of,
        max_age_seconds=max_age_seconds,
    ):
        return snapshot
    raw_holdings = broker_holdings_from_raw_balance(snapshot.payload)
    if not raw_holdings:
        return snapshot
    by_symbol = _broker_holdings_by_symbol(snapshot.holdings)
    by_symbol.update(_broker_holdings_by_symbol(raw_holdings))
    return BrokerAccountSnapshot(
        available_cash=snapshot.available_cash,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        buying_power=snapshot.buying_power,
        total_asset_value=snapshot.total_asset_value,
        previous_total_asset_value=snapshot.previous_total_asset_value,
        asset_change_amount=snapshot.asset_change_amount,
        unrealized_pnl=snapshot.unrealized_pnl,
        holdings=sorted(by_symbol.values(), key=lambda row: row.symbol),
        positions=snapshot.positions,
        open_orders=snapshot.open_orders,
        symbol_buying_power=snapshot.symbol_buying_power,
        payload=snapshot.payload,
        holdings_complete=snapshot.holdings_complete,
        open_orders_complete=snapshot.open_orders_complete,
        diagnostics=dict(snapshot.diagnostics),
        broker_account_id=snapshot.broker_account_id,
    )


def raw_vs_normalized_position_drift(
    *,
    raw_holdings: list[BrokerRawHolding],
    positions: list[ExecutionPosition],
) -> list[dict[str, object]]:
    position_by_symbol = _positions_by_symbol_for_exact_route(positions)
    raw_by_symbol = _broker_holdings_by_symbol(raw_holdings)
    rows: list[dict[str, object]] = []
    for symbol in sorted(set(position_by_symbol) | set(raw_by_symbol)):
        raw = raw_by_symbol.get(symbol)
        position = position_by_symbol.get(symbol)
        raw_quantity = raw.quantity if raw is not None else Decimal("0")
        local_quantity = _position_quantity(position)
        raw_average = raw.average_price if raw is not None else None
        local_average = Decimal(position.average_price) if position is not None and position.average_price is not None else None
        if raw_quantity == local_quantity and _same_decimal(raw_average, local_average):
            continue
        rows.append(
            {
                "symbol": symbol,
                "raw_quantity": _decimal_text(raw_quantity),
                "normalized_quantity": _decimal_text(local_quantity),
                "raw_average_price": _decimal_text(raw_average),
                "normalized_average_price": _decimal_text(local_average),
                "raw_current_price": _decimal_text(raw.current_price if raw is not None else None),
                "raw_market_value_krw": _decimal_text(raw.market_value if raw is not None else None),
                "raw_unrealized_pnl_krw": _decimal_text(raw.unrealized_pnl if raw is not None else None),
                "raw_unrealized_pnl_pct": _decimal_text(raw.unrealized_pnl_pct if raw is not None else None),
                "raw_sellable_quantity": _decimal_text(raw.sellable_quantity if raw is not None else None),
                "normalized_last_reconciled_at": position.last_reconciled_at.isoformat()
                if position is not None and position.last_reconciled_at is not None
                else None,
                "reason": _drift_reason(raw_quantity=raw_quantity, local_quantity=local_quantity),
                "raw_payload": _json_safe(raw.raw_payload) if raw is not None else None,
            }
        )
    return rows


def _positions_by_symbol_for_exact_route(
    positions: list[ExecutionPosition],
) -> dict[str, ExecutionPosition]:
    expected_route: tuple[str, str, str] | None = None
    positions_by_symbol: dict[str, ExecutionPosition] = {}
    for position in positions:
        try:
            identity = position_identity_from_columns(position)
        except PositionIdentityConflictError as exc:
            raise ValueError(
                "normalized position drift requires one exact route"
            ) from exc
        if identity is None:
            raise ValueError("normalized position drift requires one exact route")
        route = (identity.provider, identity.account_scope, identity.exchange)
        if expected_route is None:
            expected_route = route
        elif route != expected_route:
            raise ValueError("normalized position drift requires one exact route")
        symbol = position.symbol.upper()
        if symbol in positions_by_symbol:
            raise ValueError(
                "normalized position drift requires unique symbols within one exact route"
            )
        positions_by_symbol[symbol] = position
    return positions_by_symbol


def _broker_holdings_by_symbol(
    holdings: list[HoldingT],
) -> dict[str, HoldingT]:
    holdings_by_symbol: dict[str, HoldingT] = {}
    for holding in holdings:
        symbol = holding.symbol.upper()
        if symbol in holdings_by_symbol:
            raise ValueError(
                "broker holdings must contain unique symbols from one exact route"
            )
        holdings_by_symbol[symbol] = holding
    return holdings_by_symbol


def broker_raw_holding_payload(holding: BrokerRawHolding) -> dict[str, object]:
    return {
        "symbol": holding.symbol,
        "name": holding.name,
        "quantity": _decimal_text(holding.quantity),
        "average_price": _decimal_text(holding.average_price),
        "current_price": _decimal_text(holding.current_price),
        "market_value_krw": _decimal_text(holding.market_value),
        "unrealized_pnl_krw": _decimal_text(holding.unrealized_pnl),
        "unrealized_pnl_pct": _decimal_text(holding.unrealized_pnl_pct),
        "sellable_quantity": _decimal_text(holding.sellable_quantity),
        "raw_payload": _json_safe(holding.raw_payload),
    }


def _payload_rows(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _decimal_value(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _position_quantity(position: ExecutionPosition | None) -> Decimal:
    if position is None:
        return Decimal("0")
    if position.quantity > 0:
        return Decimal(position.quantity)
    base = Decimal(position.base_quantity) if position.base_quantity is not None else Decimal("0")
    return base if base > 0 else Decimal("0")


def _same_decimal(left: Decimal | None, right: Decimal | None) -> bool:
    if left is None or right is None:
        return left is right
    return left == right


def _drift_reason(*, raw_quantity: Decimal, local_quantity: Decimal) -> str:
    if raw_quantity > 0 and local_quantity <= 0:
        return "broker_raw_open_missing_from_normalized"
    if raw_quantity <= 0 and local_quantity > 0:
        return "normalized_open_missing_from_broker_raw"
    return "broker_raw_normalized_quantity_or_price_mismatch"


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
