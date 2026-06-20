from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from collections.abc import Iterable
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.broker.base import BrokerAccountHolding, BrokerAccountPosition, BrokerAccountSnapshot, BrokerAdapter
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerSession, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.runtime import OPEN_ORDER_STATUSES, position_truth_freshness_payload

TOP_SYMBOL_ALLOCATION_WARNING_PCT = Decimal("0.30")
TOP_THREE_ALLOCATION_WARNING_PCT = Decimal("0.60")
TOP_ITEM_LIMIT = 5
STOP_MONITOR_INTENTS = {"stop_sell", "target_sell", "max_holding_close_sell"}
ACTIVE_MONITORING_STATUSES = (
    "target_sell_monitoring",
    "target_sell_pending_dispatch",
    "target_sell_triggered_blocked",
    "stop_sell_monitoring",
    "stop_sell_pending_dispatch",
    "stop_sell_triggered_blocked",
    "max_holding_close_sell_monitoring",
    "max_holding_close_sell_pending_dispatch",
    "max_holding_close_sell_triggered_blocked",
)


@dataclass(frozen=True)
class HoldingRiskRow:
    symbol: str
    quantity: int
    base_quantity: Decimal | None
    average_price: Decimal | None
    market_value: Decimal | None
    source: str


@dataclass(frozen=True)
class OpenOrderRiskRow:
    symbol: str
    side: str
    quantity: int | None
    base_quantity: Decimal | None
    remaining_quantity: Decimal | None
    limit_price: Decimal | None
    notional: Decimal | None
    status: str
    broker_order_id: str | None
    idempotency_key: str | None
    source: str


def build_portfolio_risk_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or datetime.now(UTC)
    local_positions = _load_local_holdings(settings=settings, db=db)
    local_open_orders = _load_local_open_orders(settings=settings, db=db)
    snapshot, account_status = _load_account_snapshot_if_available(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=sorted({row.symbol for row in [*local_positions, *local_open_orders]}),
    )

    holdings = _holdings_from_snapshot(snapshot) if snapshot is not None else local_positions
    open_orders = _merge_open_orders(
        _open_orders_from_snapshot(snapshot) if snapshot is not None else [],
        local_open_orders,
    )
    total_market_value = _sum_known(row.market_value for row in holdings)
    total_securities_value = _decimal_from_payload(snapshot.payload, ("total_market_value", "securities_value", "total_securities_value")) if snapshot is not None else None
    if total_securities_value is None:
        total_securities_value = total_market_value

    holdings_payload = _top_holdings_payload(holdings=holdings, total_market_value=total_market_value)
    open_orders_payload = _top_open_orders_payload(open_orders=open_orders)
    exposures_payload = _exposures_payload(
        holdings=holdings,
        open_orders=open_orders,
        total_market_value=total_market_value,
    )
    stop_metadata = _load_stop_metadata(settings=settings, db=db)
    stop_risk_payload = _estimated_stop_risk_payload(holdings=holdings, stop_metadata=stop_metadata)
    freshness = position_truth_freshness_payload(settings=settings, db=db, now=generated_at)
    stale_order_warnings = _stale_order_warnings(settings=settings, db=db, now=generated_at)
    concentration_warnings = _concentration_warnings(
        holdings_payload=holdings_payload,
        exposures_payload=exposures_payload,
        open_orders=open_orders,
        stop_risk_payload=stop_risk_payload,
        freshness=freshness,
        stale_order_warnings=stale_order_warnings,
    )

    source_status = _source_status(
        account_status=account_status,
        holdings=holdings,
        open_orders=open_orders,
    )
    return {
        "generated_at": generated_at.isoformat(),
        "mode": settings.runtime_mode,
        "source_status": source_status["status"],
        "source_reason": source_status["reason"],
        "account": {
            "source": snapshot.source if snapshot is not None else "local_exec_state",
            "observed_at": snapshot.observed_at.astimezone(UTC).isoformat() if snapshot is not None else None,
            "available_cash": _decimal_text(snapshot.available_cash) if snapshot is not None else None,
            "buying_power": _decimal_text(snapshot.buying_power) if snapshot is not None else None,
            "total_market_value": _decimal_text(total_market_value),
            "total_securities_value": _decimal_text(total_securities_value),
        },
        "holdings": {
            "open_count": len(holdings),
            "top_by_market_value": holdings_payload,
        },
        "open_orders": {
            "open_count": len(open_orders),
            "top_by_notional": open_orders_payload,
        },
        "exposures": {
            "by_symbol": exposures_payload,
        },
        "estimated_stop_risk": {
            "positions": stop_risk_payload,
            "total_estimated_risk": _decimal_text(_sum_known(_decimal_or_none(row.get("estimated_risk")) for row in stop_risk_payload)),
        },
        "warnings": {
            "concentration": concentration_warnings,
            "stale_positions": {
                "stale_count": freshness["stale_count"],
                "stale_symbols": freshness["stale_symbols"],
                "oldest_age_seconds": freshness["oldest_age_seconds"],
                "max_age_seconds": freshness["max_age_seconds"],
            },
            "stale_orders": stale_order_warnings,
        },
    }


def _load_account_snapshot_if_available(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None,
    symbols: list[str],
) -> tuple[BrokerAccountSnapshot | None, dict[str, str | None]]:
    if adapter is None:
        return None, {"status": "missing", "reason": "broker adapter unavailable"}
    try:
        config = adapter.describe()
    except Exception as exc:
        return None, {"status": "unavailable", "reason": f"{exc.__class__.__name__}: {exc}"}
    if not config.configured:
        return None, {"status": "unavailable", "reason": config.reason or "broker provider is not configured"}
    session = db.get(BrokerSession, f"{config.provider}:{config.mode}")
    if session is None or not session.access_token:
        return None, {"status": "missing", "reason": "no reusable broker access token is stored"}
    if session.access_token_expires_at is not None:
        expires_at = _as_utc(session.access_token_expires_at)
        if expires_at <= datetime.now(UTC) + timedelta(minutes=5):
            return None, {"status": "missing", "reason": "stored broker access token is expired or near expiry"}
    try:
        return (
            adapter.get_account_snapshot(
                session.access_token,
                symbols=symbols or None,
                exchange="KRX",
            ),
            {"status": "ok", "reason": None},
        )
    except TypeError:
        try:
            return adapter.get_account_snapshot(session.access_token), {"status": "ok", "reason": None}
        except Exception as exc:
            return None, {"status": "unavailable", "reason": f"{exc.__class__.__name__}: {exc}"}
    except Exception as exc:
        return None, {"status": "unavailable", "reason": f"{exc.__class__.__name__}: {exc}"}


def _source_status(
    *,
    account_status: dict[str, str | None],
    holdings: list[HoldingRiskRow],
    open_orders: list[OpenOrderRiskRow],
) -> dict[str, str | None]:
    if account_status["status"] == "ok":
        return {"status": "ok", "reason": None}
    if holdings or open_orders:
        return {
            "status": "partial",
            "reason": account_status["reason"] or "using local exec state without broker account snapshot",
        }
    return {
        "status": "unavailable",
        "reason": account_status["reason"] or "no portfolio source data is available",
    }


def _load_local_holdings(*, settings: Settings, db: Session) -> list[HoldingRiskRow]:
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            or_(ExecutionPosition.quantity > 0, ExecutionPosition.base_quantity > 0),
        )
        .order_by(ExecutionPosition.symbol.asc())
    ).all()
    return [
        HoldingRiskRow(
            symbol=position.symbol,
            quantity=position.quantity,
            base_quantity=position.base_quantity,
            average_price=position.average_price,
            market_value=_position_market_value(position),
            source="local_execution_positions",
        )
        for position in positions
    ]


def _position_market_value(position: ExecutionPosition) -> Decimal | None:
    payload_value = _decimal_from_payload(position.raw_payload, ("market_value", "valuation", "evaluated_amount"))
    if payload_value is not None:
        return payload_value
    if position.average_price is None:
        return None
    units = position.base_quantity if position.base_quantity and position.base_quantity > 0 else Decimal(position.quantity)
    if units <= 0:
        return None
    return units * position.average_price


def _holdings_from_snapshot(snapshot: BrokerAccountSnapshot) -> list[HoldingRiskRow]:
    if not snapshot.holdings:
        return _positions_from_snapshot(snapshot)
    rows: list[HoldingRiskRow] = []
    for holding in snapshot.holdings:
        base_quantity = _decimal_from_payload(holding.payload, ("base_quantity", "balance"))
        if holding.quantity <= 0 and (base_quantity is None or base_quantity <= 0):
            continue
        rows.append(
            HoldingRiskRow(
                symbol=holding.symbol,
                quantity=holding.quantity,
                base_quantity=base_quantity,
                average_price=holding.average_price,
                market_value=_holding_market_value(holding, base_quantity=base_quantity),
                source=snapshot.source,
            )
        )
    return rows


def _positions_from_snapshot(snapshot: BrokerAccountSnapshot) -> list[HoldingRiskRow]:
    rows: list[HoldingRiskRow] = []
    for position in snapshot.positions:
        base_quantity = _decimal_from_payload(position.payload, ("base_quantity", "balance"))
        if position.quantity <= 0 and (base_quantity is None or base_quantity <= 0):
            continue
        rows.append(
            HoldingRiskRow(
                symbol=position.symbol,
                quantity=position.quantity,
                base_quantity=base_quantity,
                average_price=position.average_price,
                market_value=_position_snapshot_market_value(position, base_quantity=base_quantity),
                source=snapshot.source,
            )
        )
    return rows


def _holding_market_value(holding: BrokerAccountHolding, *, base_quantity: Decimal | None) -> Decimal | None:
    payload_value = _decimal_from_payload(holding.payload, ("market_value", "valuation", "evaluated_amount"))
    if payload_value is not None:
        return payload_value
    if holding.average_price is None:
        return None
    units = base_quantity if base_quantity and base_quantity > 0 else Decimal(holding.quantity)
    if units <= 0:
        return None
    return units * holding.average_price


def _position_snapshot_market_value(position: BrokerAccountPosition, *, base_quantity: Decimal | None) -> Decimal | None:
    payload_value = _decimal_from_payload(position.payload, ("market_value", "valuation", "evaluated_amount"))
    if payload_value is not None:
        return payload_value
    if position.average_price is None:
        return None
    units = base_quantity if base_quantity and base_quantity > 0 else Decimal(position.quantity)
    if units <= 0:
        return None
    return units * position.average_price


def _load_local_open_orders(*, settings: Settings, db: Session) -> list[OpenOrderRiskRow]:
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(tuple(OPEN_ORDER_STATUSES)),
        )
        .order_by(BrokerOrder.symbol.asc(), BrokerOrder.updated_at.desc())
    ).all()
    return [
        OpenOrderRiskRow(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            base_quantity=order.base_quantity,
            remaining_quantity=order.base_quantity if order.base_quantity and order.base_quantity > 0 else _decimal_or_none(order.quantity),
            limit_price=order.limit_price,
            notional=_order_notional(
                quantity=_decimal_or_none(order.quantity),
                base_quantity=order.base_quantity,
                limit_price=order.limit_price,
                quote_notional=order.quote_notional,
            ),
            status=order.status,
            broker_order_id=order.broker_order_id,
            idempotency_key=order.idempotency_key,
            source="local_broker_orders",
        )
        for order in orders
    ]


def _open_orders_from_snapshot(snapshot: BrokerAccountSnapshot) -> list[OpenOrderRiskRow]:
    return [
        OpenOrderRiskRow(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            base_quantity=_decimal_from_payload(order.payload, ("base_quantity", "remaining_volume")),
            remaining_quantity=_decimal_or_none(order.remaining_quantity),
            limit_price=order.limit_price,
            notional=_order_notional(
                quantity=_decimal_or_none(order.remaining_quantity),
                base_quantity=_decimal_from_payload(order.payload, ("base_quantity", "remaining_volume")),
                limit_price=order.limit_price,
                quote_notional=_decimal_from_payload(order.payload, ("quote_notional", "notional")),
            ),
            status=order.status,
            broker_order_id=order.broker_order_id,
            idempotency_key=order.idempotency_key,
            source=snapshot.source,
        )
        for order in snapshot.open_orders
    ]


def _merge_open_orders(
    snapshot_orders: list[OpenOrderRiskRow],
    local_orders: list[OpenOrderRiskRow],
) -> list[OpenOrderRiskRow]:
    merged: list[OpenOrderRiskRow] = []
    seen_broker_order_ids: set[str] = set()
    seen_idempotency_keys_without_broker_id: set[str] = set()
    for order in [*snapshot_orders, *local_orders]:
        if order.broker_order_id:
            if order.broker_order_id in seen_broker_order_ids:
                continue
            seen_broker_order_ids.add(order.broker_order_id)
        elif order.idempotency_key:
            if order.idempotency_key in seen_idempotency_keys_without_broker_id:
                continue
            seen_idempotency_keys_without_broker_id.add(order.idempotency_key)
        merged.append(order)
    return merged


def _order_notional(
    *,
    quantity: Decimal | None,
    base_quantity: Decimal | None,
    limit_price: Decimal | None,
    quote_notional: Decimal | None,
) -> Decimal | None:
    if quote_notional is not None:
        return quote_notional
    if limit_price is None:
        return None
    units = base_quantity if base_quantity and base_quantity > 0 else quantity
    if units is None:
        return None
    return units * limit_price


def _top_holdings_payload(
    *,
    holdings: list[HoldingRiskRow],
    total_market_value: Decimal | None,
) -> list[dict[str, object]]:
    rows = sorted(holdings, key=lambda row: row.market_value or Decimal("0"), reverse=True)
    return [
        {
            "symbol": row.symbol,
            "quantity": row.quantity,
            "base_quantity": _decimal_text(row.base_quantity),
            "average_price": _decimal_text(row.average_price),
            "market_value": _decimal_text(row.market_value),
            "allocation_pct": _decimal_text(_pct(row.market_value, total_market_value)),
            "source": row.source,
        }
        for row in rows[:TOP_ITEM_LIMIT]
    ]


def _top_open_orders_payload(*, open_orders: list[OpenOrderRiskRow]) -> list[dict[str, object]]:
    rows = sorted(open_orders, key=lambda row: row.notional or Decimal("0"), reverse=True)
    return [
        {
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "base_quantity": _decimal_text(row.base_quantity),
            "remaining_quantity": _decimal_text(row.remaining_quantity),
            "limit_price": _decimal_text(row.limit_price),
            "notional": _decimal_text(row.notional),
            "status": row.status,
            "broker_order_id": row.broker_order_id,
            "idempotency_key": row.idempotency_key,
            "source": row.source,
        }
        for row in rows[:TOP_ITEM_LIMIT]
    ]


def _exposures_payload(
    *,
    holdings: list[HoldingRiskRow],
    open_orders: list[OpenOrderRiskRow],
    total_market_value: Decimal | None,
) -> list[dict[str, object]]:
    by_symbol: dict[str, dict[str, Decimal | str | None]] = {}
    for holding in holdings:
        entry = by_symbol.setdefault(holding.symbol, {"symbol": holding.symbol, "holding_value": Decimal("0"), "open_buy_notional": Decimal("0")})
        if holding.market_value is not None:
            entry["holding_value"] = _decimal_or_zero(entry["holding_value"]) + holding.market_value
    for order in open_orders:
        if order.side != "buy":
            continue
        entry = by_symbol.setdefault(order.symbol, {"symbol": order.symbol, "holding_value": Decimal("0"), "open_buy_notional": Decimal("0")})
        if order.notional is not None:
            entry["open_buy_notional"] = _decimal_or_zero(entry["open_buy_notional"]) + order.notional
    rows = []
    exposure_basis = total_market_value
    for symbol, entry in by_symbol.items():
        holding_value = _decimal_or_zero(entry["holding_value"])
        open_buy_notional = _decimal_or_zero(entry["open_buy_notional"])
        total_exposure = holding_value + open_buy_notional
        rows.append(
            {
                "symbol": symbol,
                "holding_value": _decimal_text(holding_value),
                "open_buy_notional": _decimal_text(open_buy_notional),
                "total_exposure": _decimal_text(total_exposure),
                "allocation_pct": _decimal_text(_pct(total_exposure, exposure_basis)),
            }
        )
    return sorted(rows, key=lambda row: _decimal_or_none(row["total_exposure"]) or Decimal("0"), reverse=True)


def _load_stop_metadata(*, settings: Settings, db: Session) -> dict[str, ExecutionPlanItem]:
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.intent.in_(STOP_MONITOR_INTENTS),
            ExecutionPlanItem.status.in_(ACTIVE_MONITORING_STATUSES),
        )
        .order_by(ExecutionPlanItem.updated_at.desc())
    ).all()
    by_symbol: dict[str, ExecutionPlanItem] = {}
    for item in items:
        by_symbol.setdefault(item.symbol, item)
    return by_symbol


def _estimated_stop_risk_payload(
    *,
    holdings: list[HoldingRiskRow],
    stop_metadata: dict[str, ExecutionPlanItem],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for holding in holdings:
        item = stop_metadata.get(holding.symbol)
        stop_price = _stop_price(item)
        units = holding.base_quantity if holding.base_quantity and holding.base_quantity > 0 else Decimal(holding.quantity)
        estimated_risk = None
        if stop_price is not None and holding.average_price is not None and units > 0:
            estimated_risk = max((holding.average_price - stop_price) * units, Decimal("0"))
        rows.append(
            {
                "symbol": holding.symbol,
                "stop_price": _decimal_text(stop_price),
                "estimated_risk": _decimal_text(estimated_risk),
                "metadata_status": "present" if stop_price is not None else "missing",
                "plan_item_id": item.id if item is not None else None,
            }
        )
    return rows


def _stop_price(item: ExecutionPlanItem | None) -> Decimal | None:
    if item is None:
        return None
    for source in (item.risk_context or {}, item.quantity_policy or {}):
        value = _decimal_from_payload(source, ("effective_stop_price", "stop_price", "stop_loss_price"))
        if value is not None:
            return value
    if item.intent == "stop_sell":
        return item.trigger_price
    return None


def _stale_order_warnings(*, settings: Settings, db: Session, now: datetime) -> list[dict[str, object]]:
    stale_before = now - timedelta(seconds=settings.stale_broker_order_reconciliation_threshold_seconds)
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(tuple(OPEN_ORDER_STATUSES)),
            or_(BrokerOrder.last_broker_sync_at.is_(None), BrokerOrder.last_broker_sync_at < stale_before),
        )
        .order_by(BrokerOrder.symbol.asc())
    ).all()
    return [
        {
            "symbol": order.symbol,
            "side": order.side,
            "status": order.status,
            "broker_order_id": order.broker_order_id,
            "idempotency_key": order.idempotency_key,
            "last_broker_sync_at": order.last_broker_sync_at.astimezone(UTC).isoformat() if order.last_broker_sync_at else None,
            "reason": "missing_broker_sync" if order.last_broker_sync_at is None else "stale_broker_sync",
        }
        for order in orders
    ]


def _concentration_warnings(
    *,
    holdings_payload: list[dict[str, object]],
    exposures_payload: list[dict[str, object]],
    open_orders: list[OpenOrderRiskRow],
    stop_risk_payload: list[dict[str, object]],
    freshness: dict[str, object],
    stale_order_warnings: list[dict[str, object]],
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    if holdings_payload:
        top = holdings_payload[0]
        top_pct = _decimal_or_none(top.get("allocation_pct"))
        if top_pct is not None and top_pct >= TOP_SYMBOL_ALLOCATION_WARNING_PCT * Decimal("100"):
            warnings.append({"code": "top_symbol_concentration", "symbol": top["symbol"], "allocation_pct": top["allocation_pct"]})
        top_three_pct = _sum_known(_decimal_or_none(row.get("allocation_pct")) for row in holdings_payload[:3])
        if top_three_pct is not None and top_three_pct >= TOP_THREE_ALLOCATION_WARNING_PCT * Decimal("100"):
            warnings.append({"code": "top_three_concentration", "allocation_pct": _decimal_text(top_three_pct)})
    held_symbols = {str(row["symbol"]) for row in holdings_payload}
    open_buy_symbols = {order.symbol for order in open_orders if order.side == "buy"}
    for symbol in sorted(held_symbols & open_buy_symbols):
        warnings.append({"code": "holding_plus_open_buy_order", "symbol": symbol})
    for row in stop_risk_payload:
        if row.get("metadata_status") == "missing":
            warnings.append({"code": "missing_stop_metadata", "symbol": row["symbol"]})
    for symbol in freshness.get("stale_symbols", []):
        warnings.append({"code": "stale_position_truth", "symbol": symbol})
    for order in stale_order_warnings:
        warnings.append({"code": "stale_open_order", "symbol": order["symbol"], "idempotency_key": order["idempotency_key"], "reason": order["reason"]})
    for row in exposures_payload:
        exposure_pct = _decimal_or_none(row.get("allocation_pct"))
        if exposure_pct is not None and exposure_pct >= TOP_SYMBOL_ALLOCATION_WARNING_PCT * Decimal("100"):
            warnings.append({"code": "symbol_total_exposure_concentration", "symbol": row["symbol"], "allocation_pct": row["allocation_pct"]})
    return warnings


def _decimal_from_payload(payload: dict[str, Any] | None, keys: tuple[str, ...]) -> Decimal | None:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key not in payload:
            continue
        value = _decimal_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_or_zero(value: object) -> Decimal:
    return _decimal_or_none(value) or Decimal("0")


def _sum_known(values: Iterable[Decimal | None]) -> Decimal | None:
    total = Decimal("0")
    found = False
    for value in values:
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _pct(value: Decimal | None, basis: Decimal | None) -> Decimal | None:
    if value is None or basis is None or basis <= 0:
        return None
    return (value / basis) * Decimal("100")


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
