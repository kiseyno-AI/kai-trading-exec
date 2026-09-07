from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, load_only

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountPosition,
    BrokerAccountSnapshot,
    BrokerAdapter,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerSession, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_raw_holdings import (
    broker_raw_balance_is_fresh,
    broker_raw_balance_is_valid,
    broker_raw_holding_payload,
    broker_raw_holdings_from_payload,
    raw_vs_normalized_position_drift,
    snapshot_with_raw_balance_holdings,
)
from app.services.broker_order_state import (
    CAPITAL_RESERVING_ORDER_STATUSES,
    MANUAL_REVIEW_ORDER_STATUSES,
    active_local_broker_order_filters,
)
from app.services.fill_economics import settlement_currency_for_order

TOP_SYMBOL_ALLOCATION_WARNING_PCT = Decimal("0.30")
TOP_THREE_ALLOCATION_WARNING_PCT = Decimal("0.60")
TOP_ITEM_LIMIT = 5
UNKNOWN_CURRENCY_KEY = "UNKNOWN"
CURRENCY_HINT_HISTORY_PER_SYMBOL = 3
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
    settlement_currency: str | None
    currency_source: str | None
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
    settlement_currency: str | None
    currency_source: str | None
    status: str
    broker_order_id: str | None
    idempotency_key: str | None
    source: str


@dataclass(frozen=True)
class SymbolRiskMetadataHint:
    settlement_currency: str | None
    currency_source: str | None
    account_exchange: str | None
    exchange_source: str | None
    position_market_value: Decimal | None


def infer_settlement_currency(
    *,
    symbol: str,
    payloads: Iterable[tuple[str, object]] = (),
    provider: object = None,
    venue: object = None,
    asset_class: object = None,
) -> tuple[str | None, str | None]:
    payload_list = list(payloads)
    explicit_candidates: dict[str, str] = {}
    for label, payload in payload_list:
        explicit = _explicit_currency_from_payload(payload)
        if explicit is not None:
            explicit_candidates.setdefault(explicit, label)

    route_provider = _normalized_provider(provider)
    route_venue = _normalized_venue(venue)
    route_asset_class = _normalized_asset_class(asset_class)
    route_source = "route_metadata"
    for label, payload in payload_list:
        payload_provider, payload_venue, payload_asset_class = _route_metadata(payload)
        if route_provider is None and payload_provider is not None:
            route_provider = payload_provider
            route_source = label
        if route_venue is None and payload_venue is not None:
            route_venue = payload_venue
            route_source = label
        if route_asset_class is None and payload_asset_class is not None:
            route_asset_class = payload_asset_class
            route_source = label

    route_currency = _currency_from_route(
        symbol=symbol,
        provider=route_provider,
        venue=route_venue,
        asset_class=route_asset_class,
    )
    if len(explicit_candidates) > 1:
        return None, "conflicting_explicit_currency_metadata"
    if explicit_candidates:
        explicit_currency, explicit_source = next(iter(explicit_candidates.items()))
        if route_currency is not None and route_currency != explicit_currency:
            return None, "conflicting_currency_and_route_metadata"
        return explicit_currency, f"{explicit_source}:explicit_currency"
    if route_currency is not None:
        return route_currency, f"{route_source}:route_metadata"

    normalized_symbol = str(symbol or "").strip().upper()
    if "-" in normalized_symbol:
        quote_currency = normalized_symbol.split("-", 1)[0]
        if quote_currency in {
            "AUD",
            "BTC",
            "CAD",
            "CHF",
            "CNY",
            "EUR",
            "GBP",
            "HKD",
            "JPY",
            "KRW",
            "SGD",
            "USD",
            "USDT",
        }:
            return quote_currency, "symbol_quote_currency"
    if len(normalized_symbol) == 6 and normalized_symbol.isdigit():
        return "KRW", "krx_symbol_format"
    return None, None


def infer_order_settlement_currency(order: BrokerOrder) -> tuple[str | None, str | None]:
    payloads = (
        ("broker_order.request_metadata", order.request_metadata),
        ("broker_order.response_metadata", order.response_metadata),
        ("broker_order.raw_broker_payload", order.raw_broker_payload),
    )
    inferred, source = infer_settlement_currency(
        symbol=order.symbol,
        payloads=payloads,
        provider=order.broker_name,
        venue=order.venue,
        asset_class=order.asset_class,
    )
    expected = settlement_currency_for_order(order)
    if inferred is not None and expected is not None and inferred != expected:
        return None, "conflicting_order_currency_metadata"
    if inferred is not None:
        return inferred, source
    if expected is not None:
        return expected, "broker_order_route"
    item = order.plan_item
    if item is None:
        return None, source
    return infer_settlement_currency(
        symbol=order.symbol,
        payloads=(
            ("execution_plan_item.risk_context", item.risk_context),
            ("execution_plan_item.quantity_policy", item.quantity_policy),
        ),
        provider=item.provider,
        venue=item.venue,
        asset_class=item.asset_class,
    )


def infer_account_snapshot_exchange(
    *,
    symbol: str,
    payloads: Iterable[tuple[str, object]] = (),
    provider: object = None,
    venue: object = None,
    asset_class: object = None,
) -> tuple[str | None, str | None]:
    payload_list = list(payloads)
    route_provider = _normalized_provider(provider)
    route_venue = _normalized_venue(venue)
    route_asset_class = _normalized_asset_class(asset_class)
    route_source = "route_metadata"
    for label, payload in payload_list:
        payload_provider, payload_venue, payload_asset_class = _route_metadata(payload)
        if route_provider is None and payload_provider is not None:
            route_provider = payload_provider
            route_source = label
        if route_venue is None and payload_venue is not None:
            route_venue = payload_venue
            route_source = label
        if route_asset_class is None and payload_asset_class is not None:
            route_asset_class = payload_asset_class
            route_source = label

    if route_provider == "alpaca":
        return "ALPACA", f"{route_source}:route_metadata"
    if route_provider == "upbit":
        return "UPBIT", f"{route_source}:route_metadata"
    if route_venue in {"AMEX", "NASD", "NYSE"}:
        return route_venue, f"{route_source}:route_metadata"
    if route_venue in {"KRX", "NXT", "SOR"}:
        return "KRX", f"{route_source}:route_metadata"
    normalized_symbol = str(symbol or "").strip().upper()
    if len(normalized_symbol) == 6 and normalized_symbol.isdigit():
        return "KRX", "krx_symbol_format"
    if route_asset_class in {"stock", "kr_equity", "domestic_equity"}:
        return "KRX", f"{route_source}:route_metadata"
    return None, None


def load_symbol_risk_metadata_hints(
    *,
    settings: Settings,
    db: Session,
    symbols: Iterable[str],
) -> dict[str, SymbolRiskMetadataHint]:
    normalized_symbols = sorted(
        {str(symbol).strip() for symbol in symbols if str(symbol).strip()}
    )
    if not normalized_symbols:
        return {}

    currency_candidates: dict[str, dict[str, str]] = {
        symbol: {} for symbol in normalized_symbols
    }
    exchange_candidates: dict[str, dict[str, str]] = {
        symbol: {} for symbol in normalized_symbols
    }
    position_market_values: dict[str, Decimal | None] = {}

    for symbol in normalized_symbols:
        _record_symbol_metadata_candidates(
            symbol=symbol,
            currency_candidates=currency_candidates,
            exchange_candidates=exchange_candidates,
        )

    position_rows = db.execute(
        select(ExecutionPosition.symbol, ExecutionPosition.raw_payload)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol.in_(normalized_symbols),
        )
        .order_by(ExecutionPosition.symbol.asc())
        .limit(len(normalized_symbols))
    ).all()
    for symbol, raw_payload in position_rows:
        _record_symbol_metadata_candidates(
            symbol=symbol,
            payloads=(("execution_position.raw_payload", raw_payload),),
            currency_candidates=currency_candidates,
            exchange_candidates=exchange_candidates,
        )
        position_market_values[symbol] = _decimal_from_payload(
            raw_payload,
            ("market_value", "valuation", "evaluated_amount", "evlu_amt"),
        )

    history_symbols = _symbols_needing_metadata_fallback(
        symbols=normalized_symbols,
        currency_candidates=currency_candidates,
        exchange_candidates=exchange_candidates,
    )
    for order in _bounded_recent_broker_orders(
        settings=settings,
        db=db,
        symbols=history_symbols,
    ):
        _record_symbol_metadata_candidates(
            symbol=order.symbol,
            payloads=(
                ("broker_order.request_metadata", order.request_metadata),
                ("broker_order.response_metadata", order.response_metadata),
                ("broker_order.raw_broker_payload", order.raw_broker_payload),
            ),
            provider=order.broker_name,
            venue=order.venue,
            asset_class=order.asset_class,
            currency_candidates=currency_candidates,
            exchange_candidates=exchange_candidates,
        )

    plan_symbols = _symbols_needing_metadata_fallback(
        symbols=history_symbols,
        currency_candidates=currency_candidates,
        exchange_candidates=exchange_candidates,
    )
    for item in _bounded_recent_plan_items(
        settings=settings,
        db=db,
        symbols=plan_symbols,
    ):
        _record_symbol_metadata_candidates(
            symbol=item.symbol,
            payloads=(
                ("execution_plan_item.risk_context", item.risk_context),
                ("execution_plan_item.quantity_policy", item.quantity_policy),
            ),
            provider=item.provider,
            venue=item.venue,
            asset_class=item.asset_class,
            currency_candidates=currency_candidates,
            exchange_candidates=exchange_candidates,
        )

    return {
        symbol: SymbolRiskMetadataHint(
            settlement_currency=_single_candidate(currency_candidates[symbol]),
            currency_source=_candidate_source(
                currency_candidates[symbol],
                conflict_source="conflicting_settlement_currency_metadata",
            ),
            account_exchange=_single_candidate(exchange_candidates[symbol]),
            exchange_source=_candidate_source(
                exchange_candidates[symbol],
                conflict_source="conflicting_account_exchange_metadata",
            ),
            position_market_value=position_market_values.get(symbol),
        )
        for symbol in normalized_symbols
    }


def load_symbol_settlement_currency_hints(
    *,
    settings: Settings,
    db: Session,
    symbols: Iterable[str],
) -> dict[str, tuple[str, str]]:
    metadata = load_symbol_risk_metadata_hints(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    return {
        symbol: (
            hint.settlement_currency,
            hint.currency_source or "inferred_settlement_currency",
        )
        for symbol, hint in metadata.items()
        if hint.settlement_currency is not None
    }


def _bounded_recent_broker_orders(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> list[BrokerOrder]:
    if not symbols:
        return []
    ranked_order_ids = (
        select(
            BrokerOrder.id.label("order_id"),
            func.row_number()
            .over(
                partition_by=BrokerOrder.symbol,
                order_by=(
                    BrokerOrder.updated_at.desc(),
                    BrokerOrder.created_at.desc(),
                    BrokerOrder.id.desc(),
                ),
            )
            .label("symbol_rank"),
        )
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.symbol.in_(symbols),
        )
        .subquery()
    )
    return list(
        db.scalars(
            select(BrokerOrder)
            .join(
                ranked_order_ids,
                BrokerOrder.id == ranked_order_ids.c.order_id,
            )
            .where(
                ranked_order_ids.c.symbol_rank
                <= CURRENCY_HINT_HISTORY_PER_SYMBOL
            )
            .options(
                load_only(
                    BrokerOrder.id,
                    BrokerOrder.symbol,
                    BrokerOrder.broker_name,
                    BrokerOrder.venue,
                    BrokerOrder.asset_class,
                    BrokerOrder.request_metadata,
                    BrokerOrder.response_metadata,
                    BrokerOrder.raw_broker_payload,
                )
            )
            .order_by(
                BrokerOrder.symbol.asc(),
                ranked_order_ids.c.symbol_rank.asc(),
            )
            .limit(len(symbols) * CURRENCY_HINT_HISTORY_PER_SYMBOL)
        )
    )


def _bounded_recent_plan_items(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> list[ExecutionPlanItem]:
    if not symbols:
        return []
    ranked_item_ids = (
        select(
            ExecutionPlanItem.id.label("item_id"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlanItem.updated_at.desc(),
                    ExecutionPlanItem.created_at.desc(),
                    ExecutionPlanItem.id.desc(),
                ),
            )
            .label("symbol_rank"),
        )
        .where(
            ExecutionPlanItem.symbol.in_(symbols),
            ExecutionPlanItem.plan.has(mode=settings.runtime_mode),
        )
        .subquery()
    )
    return list(
        db.scalars(
            select(ExecutionPlanItem)
            .join(
                ranked_item_ids,
                ExecutionPlanItem.id == ranked_item_ids.c.item_id,
            )
            .where(
                ranked_item_ids.c.symbol_rank
                <= CURRENCY_HINT_HISTORY_PER_SYMBOL
            )
            .options(
                load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.symbol,
                    ExecutionPlanItem.provider,
                    ExecutionPlanItem.venue,
                    ExecutionPlanItem.asset_class,
                    ExecutionPlanItem.risk_context,
                    ExecutionPlanItem.quantity_policy,
                )
            )
            .order_by(
                ExecutionPlanItem.symbol.asc(),
                ranked_item_ids.c.symbol_rank.asc(),
            )
            .limit(len(symbols) * CURRENCY_HINT_HISTORY_PER_SYMBOL)
        )
    )


def _record_symbol_metadata_candidates(
    *,
    symbol: str,
    currency_candidates: dict[str, dict[str, str]],
    exchange_candidates: dict[str, dict[str, str]],
    payloads: Iterable[tuple[str, object]] = (),
    provider: object = None,
    venue: object = None,
    asset_class: object = None,
) -> None:
    payload_list = list(payloads)
    currency, currency_source = infer_settlement_currency(
        symbol=symbol,
        payloads=payload_list,
        provider=provider,
        venue=venue,
        asset_class=asset_class,
    )
    if currency is not None:
        currency_candidates[symbol].setdefault(
            currency,
            currency_source or "inferred_settlement_currency",
        )
    exchange, exchange_source = infer_account_snapshot_exchange(
        symbol=symbol,
        payloads=payload_list,
        provider=provider,
        venue=venue,
        asset_class=asset_class,
    )
    if exchange is not None:
        exchange_candidates[symbol].setdefault(
            exchange,
            exchange_source or "inferred_account_exchange",
        )


def _symbols_needing_metadata_fallback(
    *,
    symbols: Iterable[str],
    currency_candidates: dict[str, dict[str, str]],
    exchange_candidates: dict[str, dict[str, str]],
) -> list[str]:
    return [
        symbol
        for symbol in symbols
        if len(currency_candidates[symbol]) != 1
        or len(exchange_candidates[symbol]) != 1
    ]


def _single_candidate(candidates: dict[str, str]) -> str | None:
    if len(candidates) != 1:
        return None
    return next(iter(candidates))


def _candidate_source(
    candidates: dict[str, str],
    *,
    conflict_source: str,
) -> str | None:
    if len(candidates) > 1:
        return conflict_source
    if not candidates:
        return None
    return next(iter(candidates.values()))


def currency_aggregation_state(
    currencies: Iterable[str | None],
) -> dict[str, object]:
    values = list(currencies)
    known = sorted({currency for currency in values if currency is not None})
    unknown_count = sum(1 for currency in values if currency is None)
    if not values:
        status = "unavailable"
        reason = "no_currency_denominated_rows"
        aggregate_currency = None
    elif unknown_count and len(known) > 1:
        status = "degraded"
        reason = "mixed_and_unknown_settlement_currencies_without_fx_conversion"
        aggregate_currency = None
    elif unknown_count:
        status = "degraded"
        reason = "unknown_settlement_currency_without_fx_conversion"
        aggregate_currency = None
    elif len(known) > 1:
        status = "degraded"
        reason = "mixed_settlement_currencies_without_fx_conversion"
        aggregate_currency = None
    else:
        status = "available"
        reason = None
        aggregate_currency = known[0]
    return {
        "status": status,
        "reason": reason,
        "aggregate_currency": aggregate_currency,
        "currencies": known,
        "unknown_currency_count": unknown_count,
        "fx_conversion_source": None,
    }


def aggregation_state_with_value_coverage(
    currency_state: dict[str, object],
    *,
    total_count: int,
    known_value_count: int,
    incomplete_reason: str,
) -> dict[str, object]:
    state = dict(currency_state)
    missing_value_count = max(total_count - known_value_count, 0)
    state["total_value_count"] = total_count
    state["known_value_count"] = known_value_count
    state["missing_value_count"] = missing_value_count
    if missing_value_count <= 0:
        return state
    existing_reason = str(state.get("reason") or "").strip()
    state["status"] = "degraded"
    state["reason"] = (
        f"{existing_reason};{incomplete_reason}"
        if existing_reason
        else incomplete_reason
    )
    return state


def _degrade_for_partial_snapshot_scope(
    state: dict[str, object],
    *,
    account_status: dict[str, str | None],
) -> dict[str, object]:
    if account_status.get("status") != "partial":
        return state
    degraded = dict(state)
    existing_reason = str(degraded.get("reason") or "").strip()
    degraded["status"] = "degraded"
    degraded["reason"] = (
        f"{existing_reason};incomplete_broker_snapshot_scope"
        if existing_reason
        else "incomplete_broker_snapshot_scope"
    )
    return degraded


def build_portfolio_risk_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    generated_at = now or datetime.now(UTC)
    all_positions = _load_positions(settings=settings, db=db)
    active_positions = [
        position
        for position in all_positions
        if position.quantity > 0
        or (position.base_quantity is not None and position.base_quantity > 0)
    ]
    local_open_orders = _load_local_open_orders(
        settings=settings,
        db=db,
        now=generated_at,
    )
    local_symbols = sorted(
        {
            *(position.symbol for position in active_positions),
            *(order.symbol for order in local_open_orders),
        }
    )
    metadata_hints = load_symbol_risk_metadata_hints(
        settings=settings,
        db=db,
        symbols=local_symbols,
    )
    local_positions = _load_local_holdings(
        positions=active_positions,
        metadata_hints=metadata_hints,
    )
    snapshot, account_status = _load_account_snapshot_if_available(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=local_symbols,
        metadata_hints=metadata_hints,
    )
    raw_balance_freshness_as_of = now or datetime.now(UTC)
    raw_balance_valid = broker_raw_balance_is_valid(snapshot.payload if snapshot is not None else None)
    raw_balance_fresh = (
        broker_raw_balance_is_fresh(
            observed_at=snapshot.observed_at,
            as_of=raw_balance_freshness_as_of,
            max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
        )
        if snapshot is not None
        else False
    )
    raw_holdings = (
        broker_raw_holdings_from_payload(snapshot.payload)
        if snapshot is not None and raw_balance_valid and raw_balance_fresh
        else []
    )
    if snapshot is not None and raw_holdings:
        snapshot = snapshot_with_raw_balance_holdings(
            snapshot,
            as_of=raw_balance_freshness_as_of,
            max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
        )
    raw_drift = raw_vs_normalized_position_drift(
        raw_holdings=raw_holdings,
        positions=all_positions,
    )

    snapshot_holdings = _holdings_from_snapshot(snapshot) if snapshot is not None else []
    holdings = (
        _merge_holdings(snapshot_holdings, local_positions)
        if account_status["status"] == "partial"
        else snapshot_holdings
        if snapshot is not None
        else local_positions
    )
    open_orders = _merge_open_orders(
        _open_orders_from_snapshot(snapshot) if snapshot is not None else [],
        local_open_orders,
    )
    holding_currency_state = aggregation_state_with_value_coverage(
        currency_aggregation_state(
            row.settlement_currency for row in holdings
        ),
        total_count=len(holdings),
        known_value_count=sum(
            row.market_value is not None for row in holdings
        ),
        incomplete_reason="incomplete_market_value_coverage",
    )
    holding_currency_state = _degrade_for_partial_snapshot_scope(
        holding_currency_state,
        account_status=account_status,
    )
    open_buy_orders = [order for order in open_orders if order.side == "buy"]
    exposure_currency_state = aggregation_state_with_value_coverage(
        currency_aggregation_state(
            [
                *(row.settlement_currency for row in holdings),
                *(row.settlement_currency for row in open_buy_orders),
            ]
        ),
        total_count=len(holdings) + len(open_buy_orders),
        known_value_count=sum(
            row.market_value is not None for row in holdings
        )
        + sum(order.notional is not None for order in open_buy_orders),
        incomplete_reason="incomplete_exposure_value_coverage",
    )
    exposure_currency_state = _degrade_for_partial_snapshot_scope(
        exposure_currency_state,
        account_status=account_status,
    )
    overall_currency_state = aggregation_state_with_value_coverage(
        currency_aggregation_state(
            [
                *(row.settlement_currency for row in holdings),
                *(row.settlement_currency for row in open_orders),
            ]
        ),
        total_count=len(holdings) + len(open_orders),
        known_value_count=sum(row.market_value is not None for row in holdings)
        + sum(order.notional is not None for order in open_orders),
        incomplete_reason="incomplete_portfolio_value_coverage",
    )
    overall_currency_state = _degrade_for_partial_snapshot_scope(
        overall_currency_state,
        account_status=account_status,
    )
    unconverted_market_value = _sum_known(row.market_value for row in holdings)
    total_market_value = (
        unconverted_market_value
        if holding_currency_state["status"] == "available"
        else None
    )
    total_securities_value = (
        _decimal_from_payload(
            snapshot.payload,
            ("total_market_value", "securities_value", "total_securities_value"),
        )
        if snapshot is not None and holding_currency_state["status"] == "available"
        else None
    )
    if total_securities_value is None and holding_currency_state["status"] == "available":
        total_securities_value = total_market_value

    holdings_payload = _top_holdings_payload(
        holdings=holdings,
        total_market_value=total_market_value,
        aggregation_available=holding_currency_state["status"] == "available",
    )
    open_orders_payload = _top_open_orders_payload(open_orders=open_orders)
    exposures_payload = _exposures_payload(
        holdings=holdings,
        open_orders=open_orders,
        total_market_value=total_market_value,
        aggregation_available=exposure_currency_state["status"] == "available",
    )
    stop_metadata = _load_stop_metadata(settings=settings, db=db)
    stop_risk_payload = _estimated_stop_risk_payload(holdings=holdings, stop_metadata=stop_metadata)
    known_stop_risk_count = sum(
        _decimal_or_none(row.get("estimated_risk")) is not None
        for row in stop_risk_payload
    )
    stop_currency_state = aggregation_state_with_value_coverage(
        currency_aggregation_state(
            row.settlement_currency for row in holdings
        ),
        total_count=len(stop_risk_payload),
        known_value_count=known_stop_risk_count,
        incomplete_reason="incomplete_stop_risk_coverage",
    )
    stop_currency_state = _degrade_for_partial_snapshot_scope(
        stop_currency_state,
        account_status=account_status,
    )
    total_estimated_stop_risk = (
        _sum_known(
            _decimal_or_none(row.get("estimated_risk"))
            for row in stop_risk_payload
        )
        if stop_currency_state["status"] == "available"
        else None
    )
    # Imported lazily to keep the currency inference helpers reusable by the
    # feedback service without creating runtime -> monitoring -> feedback cycles.
    from app.services.runtime import position_truth_freshness_payload

    freshness = position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=generated_at,
    )
    stale_order_warnings = _stale_order_warnings(settings=settings, db=db, now=generated_at)
    concentration_warnings = _concentration_warnings(
        holdings_payload=holdings_payload,
        exposures_payload=exposures_payload,
        open_orders=open_orders,
        stop_risk_payload=stop_risk_payload,
        freshness=freshness,
        stale_order_warnings=stale_order_warnings,
        raw_drift=raw_drift,
        currency_aggregation=overall_currency_state,
        holding_aggregation=holding_currency_state,
        stop_risk_aggregation=stop_currency_state,
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
        "aggregation_status": overall_currency_state["status"],
        "aggregation_reason": overall_currency_state["reason"],
        "aggregate_currency": overall_currency_state["aggregate_currency"],
        "currency_aggregation": overall_currency_state,
        "account": {
            "source": snapshot.source if snapshot is not None else "local_exec_state",
            "observed_at": snapshot.observed_at.astimezone(UTC).isoformat() if snapshot is not None else None,
            "available_cash": _decimal_text(snapshot.available_cash) if snapshot is not None else None,
            "buying_power": _decimal_text(snapshot.buying_power) if snapshot is not None else None,
            "total_asset_value": _decimal_text(snapshot.total_asset_value) if snapshot is not None else None,
            "total_market_value": _decimal_text(total_market_value),
            "total_securities_value": _decimal_text(total_securities_value),
            "settlement_currency": holding_currency_state["aggregate_currency"],
            "aggregation_status": holding_currency_state["status"],
            "aggregation_reason": holding_currency_state["reason"],
            "snapshot_status": account_status["status"],
            "snapshot_reason": account_status["reason"],
            "snapshot_exchange": account_status.get("exchange"),
        },
        "holdings": {
            "open_count": len(holdings),
            "top_by_market_value": holdings_payload,
            "by_currency": _holding_subtotals_by_currency(holdings),
            "currency_aggregation": holding_currency_state,
            "broker_raw": {
                "available": bool(raw_holdings),
                "valid": raw_balance_valid,
                "fresh": raw_balance_fresh,
                "freshness_threshold_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
                "source": "broker_raw.balance.output1" if raw_holdings else None,
                "holdings": [broker_raw_holding_payload(holding) for holding in raw_holdings],
                "normalized_drift_count": len(raw_drift),
                "normalized_drift": raw_drift,
            },
        },
        "open_orders": {
            "open_count": len(open_orders),
            "top_by_notional": open_orders_payload,
            "by_currency": _open_order_subtotals_by_currency(open_orders),
        },
        "exposures": {
            "by_symbol": exposures_payload,
            "by_currency": _exposure_subtotals_by_currency(
                holdings=holdings,
                open_orders=open_orders,
            ),
            "currency_aggregation": exposure_currency_state,
        },
        "estimated_stop_risk": {
            "positions": stop_risk_payload,
            "total_estimated_risk": _decimal_text(total_estimated_stop_risk),
            "settlement_currency": stop_currency_state["aggregate_currency"],
            "by_currency": _stop_risk_subtotals_by_currency(stop_risk_payload),
            "currency_aggregation": stop_currency_state,
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
    metadata_hints: dict[str, SymbolRiskMetadataHint],
) -> tuple[BrokerAccountSnapshot | None, dict[str, str | None]]:
    if adapter is None:
        return None, {
            "status": "missing",
            "reason": "broker adapter unavailable",
            "exchange": None,
        }
    try:
        config = adapter.describe()
    except Exception as exc:
        return None, {
            "status": "unavailable",
            "reason": f"{exc.__class__.__name__}: {exc}",
            "exchange": None,
        }
    if not config.configured:
        return None, {
            "status": "unavailable",
            "reason": config.reason or "broker provider is not configured",
            "exchange": None,
        }
    session = db.get(BrokerSession, f"{config.provider}:{config.mode}")
    if session is None or not session.access_token:
        return None, {
            "status": "missing",
            "reason": "no reusable broker access token is stored",
            "exchange": None,
        }
    if session.access_token_expires_at is not None:
        expires_at = _as_utc(session.access_token_expires_at)
        if expires_at <= datetime.now(UTC) + timedelta(minutes=5):
            return None, {
                "status": "missing",
                "reason": "stored broker access token is expired or near expiry",
                "exchange": None,
            }
    if _normalized_provider(config.provider) == "kis":
        exchanges, scope_reason = _kis_account_snapshot_exchanges(
            settings=settings,
            symbols=symbols,
            metadata_hints=metadata_hints,
        )
        if not exchanges:
            return None, {
                "status": "incomplete_scope",
                "reason": scope_reason,
                "exchange": None,
            }
        return _load_kis_account_snapshots(
            adapter=adapter,
            access_token=session.access_token,
            exchanges=exchanges,
            symbols=symbols,
            metadata_hints=metadata_hints,
        )

    exchange, scope_reason = _account_snapshot_scope(
        provider=config.provider,
        symbols=symbols,
        metadata_hints=metadata_hints,
    )
    if exchange is None:
        return None, {
            "status": "incomplete_scope",
            "reason": scope_reason,
            "exchange": None,
        }
    try:
        return (
            adapter.get_account_snapshot(
                session.access_token,
                symbols=symbols or None,
                exchange=exchange,
            ),
            {"status": "ok", "reason": None, "exchange": exchange},
        )
    except TypeError as exc:
        if exchange not in {"ALPACA", "KRX", "UPBIT"}:
            return None, {
                "status": "unavailable",
                "reason": (
                    f"adapter does not support scoped {exchange} account snapshots: "
                    f"{exc.__class__.__name__}: {exc}"
                ),
                "exchange": exchange,
            }
        try:
            return adapter.get_account_snapshot(session.access_token), {
                "status": "ok",
                "reason": None,
                "exchange": exchange,
            }
        except Exception as fallback_exc:
            return None, {
                "status": "unavailable",
                "reason": (
                    f"{fallback_exc.__class__.__name__}: {fallback_exc}"
                ),
                "exchange": exchange,
            }
    except Exception as exc:
        return None, {
            "status": "unavailable",
            "reason": f"{exc.__class__.__name__}: {exc}",
            "exchange": exchange,
        }


def _kis_account_snapshot_exchanges(
    *,
    settings: Settings,
    symbols: list[str],
    metadata_hints: dict[str, SymbolRiskMetadataHint],
) -> tuple[list[str], str | None]:
    if not symbols:
        exchanges = {"KRX"}
        if settings.kis_us_trading_enabled:
            exchanges.update({"AMEX", "NASD", "NYSE"})
        return sorted(exchanges), None

    unresolved_symbols = [
        symbol
        for symbol in symbols
        if metadata_hints.get(symbol) is None
        or metadata_hints[symbol].account_exchange is None
    ]
    if unresolved_symbols:
        return [], (
            "broker account snapshot skipped because account exchange is unresolved "
            f"for symbols={','.join(unresolved_symbols)}; local portfolio truth preserved"
        )
    exchanges = {
        hint.account_exchange
        for symbol in symbols
        if (hint := metadata_hints[symbol]).account_exchange is not None
    }
    if settings.kis_us_trading_enabled or exchanges & {"AMEX", "NASD", "NYSE"}:
        # KIS overseas balance/order endpoints are exchange-scoped. Querying
        # only the exchange of a tracked symbol silently omits holdings routed
        # through the other US exchanges.
        exchanges.update({"AMEX", "NASD", "NYSE"})
    return sorted(exchanges), None


def _load_kis_account_snapshots(
    *,
    adapter: BrokerAdapter,
    access_token: str,
    exchanges: list[str],
    symbols: list[str],
    metadata_hints: dict[str, SymbolRiskMetadataHint],
) -> tuple[BrokerAccountSnapshot | None, dict[str, str | None]]:
    snapshots: list[tuple[str, BrokerAccountSnapshot]] = []
    failures: list[str] = []
    us_symbols = [
        symbol
        for symbol in symbols
        if metadata_hints.get(symbol) is not None
        and metadata_hints[symbol].account_exchange in {"AMEX", "NASD", "NYSE"}
    ]
    krx_symbols = [
        symbol
        for symbol in symbols
        if metadata_hints.get(symbol) is not None
        and metadata_hints[symbol].account_exchange == "KRX"
    ]
    for exchange in exchanges:
        scoped_symbols = us_symbols if exchange in {"AMEX", "NASD", "NYSE"} else krx_symbols
        try:
            snapshot = adapter.get_account_snapshot(
                access_token,
                symbols=scoped_symbols or None,
                exchange=exchange,
            )
        except Exception as exc:
            failures.append(f"{exchange}:{exc.__class__.__name__}:{exc}")
            continue
        snapshots.append((exchange, snapshot))

    exchange_label = ",".join(exchanges)
    if not snapshots:
        return None, {
            "status": "unavailable",
            "reason": "all scoped KIS account snapshots failed: " + "; ".join(failures),
            "exchange": exchange_label,
        }
    merged = _merge_kis_account_snapshots(snapshots)
    if failures:
        return merged, {
            "status": "partial",
            "reason": "some scoped KIS account snapshots failed: " + "; ".join(failures),
            "exchange": exchange_label,
        }
    return merged, {"status": "ok", "reason": None, "exchange": exchange_label}


def _merge_kis_account_snapshots(
    snapshots: list[tuple[str, BrokerAccountSnapshot]],
) -> BrokerAccountSnapshot:
    if len(snapshots) == 1:
        return snapshots[0][1]
    holdings: dict[tuple[str, str], BrokerAccountHolding] = {}
    positions: dict[tuple[str, str], BrokerAccountPosition] = {}
    open_orders: dict[tuple[str, str], BrokerAccountOpenOrder] = {}
    currencies: set[str] = set()
    for exchange, snapshot in snapshots:
        currency = str(snapshot.payload.get("currency") or ("KRW" if exchange == "KRX" else "USD"))
        currencies.add(currency)
        for holding in snapshot.holdings:
            key = (_snapshot_row_exchange(holding.payload, fallback=exchange), holding.symbol)
            holdings.setdefault(key, holding)
        for position in snapshot.positions:
            key = (_snapshot_row_exchange(position.payload, fallback=exchange), position.symbol)
            positions.setdefault(key, position)
        for order in snapshot.open_orders:
            identity = order.broker_order_id or order.idempotency_key or (
                f"{order.symbol}:{order.side}:{order.quantity}:{order.limit_price}"
            )
            open_orders.setdefault(
                (_snapshot_row_exchange(order.payload, fallback=exchange), identity),
                order,
            )

    same_currency = len(currencies) == 1
    source_snapshots = [snapshot for _exchange, snapshot in snapshots]
    available_cash_values = [
        snapshot.available_cash
        for snapshot in source_snapshots
        if snapshot.available_cash is not None
    ]
    buying_power_values = [
        snapshot.buying_power
        for snapshot in source_snapshots
        if snapshot.buying_power is not None
    ]
    return BrokerAccountSnapshot(
        available_cash=(min(available_cash_values) if same_currency and available_cash_values else None),
        buying_power=(min(buying_power_values) if same_currency and buying_power_values else None),
        total_asset_value=(
            source_snapshots[0].total_asset_value
            if len(source_snapshots) == 1
            else None
        ),
        previous_total_asset_value=(
            source_snapshots[0].previous_total_asset_value
            if len(source_snapshots) == 1
            else None
        ),
        asset_change_amount=(
            source_snapshots[0].asset_change_amount
            if len(source_snapshots) == 1
            else None
        ),
        unrealized_pnl=(
            sum(
                (snapshot.unrealized_pnl or Decimal("0") for snapshot in source_snapshots),
                Decimal("0"),
            )
            if same_currency
            else None
        ),
        observed_at=min(snapshot.observed_at for snapshot in source_snapshots),
        source="kis:scoped_account_snapshot_aggregate",
        holdings=list(holdings.values()),
        positions=list(positions.values()),
        open_orders=list(open_orders.values()),
        symbol_buying_power={
            symbol: buying_power
            for snapshot in source_snapshots
            for symbol, buying_power in snapshot.symbol_buying_power.items()
        },
        payload={
            **({"currency": next(iter(currencies))} if same_currency else {}),
            "snapshot_exchanges": [exchange for exchange, _snapshot in snapshots],
            "snapshot_sources": [snapshot.source for snapshot in source_snapshots],
            "currency_aggregation": "single_currency" if same_currency else "mixed_without_fx",
        },
    )


def _snapshot_row_exchange(payload: object, *, fallback: str) -> str:
    if not isinstance(payload, dict):
        return fallback
    for key in ("exchange", "ovrs_excg_cd", "OVRS_EXCG_CD"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return fallback


def _account_snapshot_scope(
    *,
    provider: str,
    symbols: list[str],
    metadata_hints: dict[str, SymbolRiskMetadataHint],
) -> tuple[str | None, str | None]:
    normalized_provider = _normalized_provider(provider)
    if normalized_provider == "alpaca":
        return "ALPACA", None
    if normalized_provider == "upbit":
        return "UPBIT", None
    if not symbols:
        return "KRX", None

    unresolved_symbols = [
        symbol
        for symbol in symbols
        if metadata_hints.get(symbol) is None
        or metadata_hints[symbol].account_exchange is None
    ]
    if unresolved_symbols:
        return None, (
            "broker account snapshot skipped because account exchange is unresolved "
            f"for symbols={','.join(unresolved_symbols)}; local portfolio truth preserved"
        )
    exchanges = sorted(
        {
            hint.account_exchange
            for symbol in symbols
            if (hint := metadata_hints[symbol]).account_exchange is not None
        }
    )
    if len(exchanges) != 1:
        return None, (
            "broker account snapshot skipped because mixed account exchanges require "
            f"an FX-aware merge (exchanges={','.join(exchanges)}); "
            "local portfolio truth preserved"
        )
    return exchanges[0], None


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


def _load_local_holdings(
    *,
    positions: list[ExecutionPosition],
    metadata_hints: dict[str, SymbolRiskMetadataHint],
) -> list[HoldingRiskRow]:
    rows: list[HoldingRiskRow] = []
    for position in positions:
        if position.quantity <= 0 and (
            position.base_quantity is None or position.base_quantity <= 0
        ):
            continue
        hint = metadata_hints.get(position.symbol)
        if hint is not None and hint.settlement_currency is not None:
            currency = hint.settlement_currency
            currency_source = hint.currency_source
        else:
            currency, currency_source = infer_settlement_currency(
                symbol=position.symbol,
            )
        rows.append(
            HoldingRiskRow(
                symbol=position.symbol,
                quantity=position.quantity,
                base_quantity=position.base_quantity,
                average_price=position.average_price,
                market_value=_position_market_value(
                    position,
                    raw_market_value=(
                        hint.position_market_value if hint is not None else None
                    ),
                ),
                settlement_currency=currency,
                currency_source=currency_source,
                source="local_execution_positions",
            )
        )
    return rows


def _load_positions(*, settings: Settings, db: Session) -> list[ExecutionPosition]:
    return list(
        db.scalars(
            select(ExecutionPosition)
            .where(ExecutionPosition.mode == settings.runtime_mode)
            .order_by(ExecutionPosition.symbol.asc())
        )
    )

def _position_market_value(
    position: ExecutionPosition,
    *,
    raw_market_value: Decimal | None,
) -> Decimal | None:
    if raw_market_value is not None:
        return raw_market_value
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
        settlement_currency, currency_source = infer_settlement_currency(
            symbol=holding.symbol,
            payloads=(
                ("broker_holding.payload", holding.payload),
                ("broker_snapshot.payload", snapshot.payload),
            ),
        )
        rows.append(
            HoldingRiskRow(
                symbol=holding.symbol,
                quantity=holding.quantity,
                base_quantity=base_quantity,
                average_price=holding.average_price,
                market_value=_holding_market_value(holding, base_quantity=base_quantity),
                settlement_currency=settlement_currency,
                currency_source=currency_source,
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
        settlement_currency, currency_source = infer_settlement_currency(
            symbol=position.symbol,
            payloads=(
                ("broker_position.payload", position.payload),
                ("broker_snapshot.payload", snapshot.payload),
            ),
        )
        rows.append(
            HoldingRiskRow(
                symbol=position.symbol,
                quantity=position.quantity,
                base_quantity=base_quantity,
                average_price=position.average_price,
                market_value=_position_snapshot_market_value(position, base_quantity=base_quantity),
                settlement_currency=settlement_currency,
                currency_source=currency_source,
                source=snapshot.source,
            )
        )
    return rows


def _holding_market_value(holding: BrokerAccountHolding, *, base_quantity: Decimal | None) -> Decimal | None:
    payload_value = _decimal_from_payload(
        holding.payload,
        ("market_value", "valuation", "evaluated_amount", "evlu_amt"),
    )
    if payload_value is not None:
        return payload_value
    current_price = _decimal_from_payload(holding.payload, ("current_price", "prpr", "now_pric", "cur_prc"))
    if holding.average_price is None:
        if current_price is None:
            return None
        units = base_quantity if base_quantity and base_quantity > 0 else Decimal(holding.quantity)
        return units * current_price if units > 0 else None
    units = base_quantity if base_quantity and base_quantity > 0 else Decimal(holding.quantity)
    if units <= 0:
        return None
    if current_price is not None:
        return units * current_price
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


def _load_local_open_orders(*, settings: Settings, db: Session, now: datetime | None = None) -> list[OpenOrderRiskRow]:
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            *active_local_broker_order_filters(
                settings=settings,
                statuses=CAPITAL_RESERVING_ORDER_STATUSES,
                now=now,
            ),
        )
        .order_by(BrokerOrder.symbol.asc(), BrokerOrder.updated_at.desc())
    ).all()
    rows: list[OpenOrderRiskRow] = []
    for order in orders:
        settlement_currency, currency_source = infer_order_settlement_currency(order)
        rows.append(
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
            settlement_currency=settlement_currency,
            currency_source=currency_source,
            status=order.status,
            broker_order_id=order.broker_order_id,
            idempotency_key=order.idempotency_key,
            source="local_broker_orders",
        )
        )
    return rows


def _open_orders_from_snapshot(snapshot: BrokerAccountSnapshot) -> list[OpenOrderRiskRow]:
    rows: list[OpenOrderRiskRow] = []
    for order in snapshot.open_orders:
        settlement_currency, currency_source = infer_settlement_currency(
            symbol=order.symbol,
            payloads=(
                ("broker_open_order.payload", order.payload),
                ("broker_snapshot.payload", snapshot.payload),
            ),
        )
        rows.append(
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
            settlement_currency=settlement_currency,
            currency_source=currency_source,
            status=order.status,
            broker_order_id=order.broker_order_id,
            idempotency_key=order.idempotency_key,
            source=snapshot.source,
        )
        )
    return rows


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


def _merge_holdings(
    snapshot_holdings: list[HoldingRiskRow],
    local_holdings: list[HoldingRiskRow],
) -> list[HoldingRiskRow]:
    """Prefer broker rows while retaining symbols omitted by a partial snapshot."""
    merged: list[HoldingRiskRow] = []
    seen_symbols: set[str] = set()
    for holding in [*snapshot_holdings, *local_holdings]:
        if holding.symbol in seen_symbols:
            continue
        seen_symbols.add(holding.symbol)
        merged.append(holding)
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
    aggregation_available: bool,
) -> list[dict[str, object]]:
    totals_by_currency = _known_holding_totals(holdings)
    rows = (
        sorted(
            holdings,
            key=lambda row: row.market_value or Decimal("0"),
            reverse=True,
        )
        if aggregation_available
        else sorted(
            holdings,
            key=lambda row: _currency_group_sort_key(
                currency=row.settlement_currency,
                value=row.market_value,
            ),
        )
    )
    return [
        {
            "symbol": row.symbol,
            "quantity": row.quantity,
            "base_quantity": _decimal_text(row.base_quantity),
            "average_price": _decimal_text(row.average_price),
            "market_value": _decimal_text(row.market_value),
            "settlement_currency": row.settlement_currency,
            "currency_source": row.currency_source,
            "allocation_pct": _decimal_text(
                _pct(row.market_value, total_market_value)
                if aggregation_available
                else None
            ),
            "currency_allocation_pct": _decimal_text(
                _pct(
                    row.market_value,
                    totals_by_currency.get(row.settlement_currency),
                )
                if row.settlement_currency is not None
                else None
            ),
            "source": row.source,
        }
        for row in rows[:TOP_ITEM_LIMIT]
    ]


def _top_open_orders_payload(*, open_orders: list[OpenOrderRiskRow]) -> list[dict[str, object]]:
    aggregation = currency_aggregation_state(
        row.settlement_currency for row in open_orders
    )
    rows = (
        sorted(
            open_orders,
            key=lambda row: row.notional or Decimal("0"),
            reverse=True,
        )
        if aggregation["status"] == "available"
        else sorted(
            open_orders,
            key=lambda row: _currency_group_sort_key(
                currency=row.settlement_currency,
                value=row.notional,
            ),
        )
    )
    return [
        {
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "base_quantity": _decimal_text(row.base_quantity),
            "remaining_quantity": _decimal_text(row.remaining_quantity),
            "limit_price": _decimal_text(row.limit_price),
            "notional": _decimal_text(row.notional),
            "settlement_currency": row.settlement_currency,
            "currency_source": row.currency_source,
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
    aggregation_available: bool,
) -> list[dict[str, object]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    for holding in holdings:
        entry = by_symbol.setdefault(
            holding.symbol,
            {
                "symbol": holding.symbol,
                "holding_value": Decimal("0"),
                "open_buy_notional": Decimal("0"),
                "holding_count": 0,
                "known_holding_value_count": 0,
                "open_buy_order_count": 0,
                "known_open_buy_notional_count": 0,
                "currencies": set(),
                "unknown_currency": False,
            },
        )
        entry["holding_count"] = int(entry["holding_count"]) + 1
        _track_entry_currency(entry, holding.settlement_currency)
        if holding.market_value is not None:
            entry["known_holding_value_count"] = (
                int(entry["known_holding_value_count"]) + 1
            )
            entry["holding_value"] = _decimal_or_zero(entry["holding_value"]) + holding.market_value
    for order in open_orders:
        if order.side != "buy":
            continue
        entry = by_symbol.setdefault(
            order.symbol,
            {
                "symbol": order.symbol,
                "holding_value": Decimal("0"),
                "open_buy_notional": Decimal("0"),
                "holding_count": 0,
                "known_holding_value_count": 0,
                "open_buy_order_count": 0,
                "known_open_buy_notional_count": 0,
                "currencies": set(),
                "unknown_currency": False,
            },
        )
        entry["open_buy_order_count"] = int(entry["open_buy_order_count"]) + 1
        _track_entry_currency(entry, order.settlement_currency)
        if order.notional is not None:
            entry["known_open_buy_notional_count"] = (
                int(entry["known_open_buy_notional_count"]) + 1
            )
            entry["open_buy_notional"] = _decimal_or_zero(entry["open_buy_notional"]) + order.notional
    rows = []
    holding_totals_by_currency = _known_holding_totals(holdings)
    for symbol, entry in by_symbol.items():
        holding_value = _decimal_or_zero(entry["holding_value"])
        open_buy_notional = _decimal_or_zero(entry["open_buy_notional"])
        currencies = entry["currencies"]
        settlement_currency = (
            next(iter(currencies))
            if len(currencies) == 1 and entry["unknown_currency"] is False
            else None
        )
        values_complete = (
            int(entry["holding_count"])
            == int(entry["known_holding_value_count"])
            and int(entry["open_buy_order_count"])
            == int(entry["known_open_buy_notional_count"])
        )
        total_exposure = (
            holding_value + open_buy_notional
            if settlement_currency is not None and values_complete
            else None
        )
        rows.append(
            {
                "symbol": symbol,
                "holding_value": _decimal_text(
                    holding_value
                    if int(entry["holding_count"])
                    == int(entry["known_holding_value_count"])
                    else None
                ),
                "open_buy_notional": _decimal_text(
                    open_buy_notional
                    if int(entry["open_buy_order_count"])
                    == int(entry["known_open_buy_notional_count"])
                    else None
                ),
                "total_exposure": _decimal_text(total_exposure),
                "settlement_currency": settlement_currency,
                "allocation_pct": _decimal_text(
                    _pct(total_exposure, total_market_value)
                    if aggregation_available
                    else None
                ),
                "currency_allocation_pct": _decimal_text(
                    _pct(
                        total_exposure,
                        holding_totals_by_currency.get(settlement_currency),
                    )
                    if settlement_currency is not None
                    else None
                ),
            }
        )
    if aggregation_available:
        return sorted(
            rows,
            key=lambda row: _decimal_or_none(row["total_exposure"])
            or Decimal("0"),
            reverse=True,
        )
    return sorted(
        rows,
        key=lambda row: _currency_group_sort_key(
            currency=(
                str(row["settlement_currency"])
                if row["settlement_currency"] is not None
                else None
            ),
            value=_decimal_or_none(row["total_exposure"]),
        ),
    )


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
                "settlement_currency": holding.settlement_currency,
                "currency_source": holding.currency_source,
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
            BrokerOrder.status.in_(tuple(CAPITAL_RESERVING_ORDER_STATUSES)),
            or_(
                BrokerOrder.status.in_(MANUAL_REVIEW_ORDER_STATUSES),
                BrokerOrder.last_broker_sync_at.is_(None),
                BrokerOrder.last_broker_sync_at < stale_before,
            ),
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
    raw_drift: list[dict[str, object]],
    currency_aggregation: dict[str, object],
    holding_aggregation: dict[str, object],
    stop_risk_aggregation: dict[str, object],
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    if currency_aggregation["status"] == "degraded":
        warnings.append(
            {
                "code": "currency_aggregation_degraded",
                "reason": currency_aggregation["reason"],
                "currencies": currency_aggregation["currencies"],
                "unknown_currency_count": currency_aggregation[
                    "unknown_currency_count"
                ],
            }
        )
    if int(holding_aggregation.get("missing_value_count") or 0) > 0:
        warnings.append(
            {
                "code": "incomplete_market_value_coverage",
                "reason": holding_aggregation["reason"],
                "known_value_count": holding_aggregation["known_value_count"],
                "missing_value_count": holding_aggregation["missing_value_count"],
            }
        )
    if int(stop_risk_aggregation.get("missing_value_count") or 0) > 0:
        warnings.append(
            {
                "code": "incomplete_stop_risk_coverage",
                "reason": stop_risk_aggregation["reason"],
                "known_value_count": stop_risk_aggregation["known_value_count"],
                "missing_value_count": stop_risk_aggregation["missing_value_count"],
            }
        )
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
    for row in raw_drift:
        warnings.append(
            {
                "code": "broker_raw_normalized_position_drift",
                "symbol": row.get("symbol"),
                "reason": row.get("reason"),
            }
        )
    for row in exposures_payload:
        exposure_pct = _decimal_or_none(row.get("allocation_pct"))
        if exposure_pct is not None and exposure_pct >= TOP_SYMBOL_ALLOCATION_WARNING_PCT * Decimal("100"):
            warnings.append({"code": "symbol_total_exposure_concentration", "symbol": row["symbol"], "allocation_pct": row["allocation_pct"]})
    return warnings


def _holding_subtotals_by_currency(
    holdings: list[HoldingRiskRow],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for holding in holdings:
        key = _currency_key(holding.settlement_currency)
        entry = stats.setdefault(
            key,
            {
                "holding_count": 0,
                "known_market_value_count": 0,
                "market_values": [],
            },
        )
        entry["holding_count"] = int(entry["holding_count"]) + 1
        if holding.market_value is not None:
            entry["known_market_value_count"] = (
                int(entry["known_market_value_count"]) + 1
            )
            entry["market_values"].append(holding.market_value)
    return {
        key: {
            "holding_count": entry["holding_count"],
            "known_market_value_count": entry["known_market_value_count"],
            "known_market_value_subtotal": (
                _decimal_text(sum(entry["market_values"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY and entry["market_values"]
                else None
            ),
            "total_market_value": (
                _decimal_text(sum(entry["market_values"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY
                and entry["market_values"]
                and entry["known_market_value_count"] == entry["holding_count"]
                else None
            ),
        }
        for key, entry in sorted(stats.items())
    }


def _open_order_subtotals_by_currency(
    open_orders: list[OpenOrderRiskRow],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for order in open_orders:
        key = _currency_key(order.settlement_currency)
        entry = stats.setdefault(
            key,
            {"order_count": 0, "known_notional_count": 0, "notionals": []},
        )
        entry["order_count"] = int(entry["order_count"]) + 1
        if order.notional is not None:
            entry["known_notional_count"] = int(entry["known_notional_count"]) + 1
            entry["notionals"].append(order.notional)
    return {
        key: {
            "order_count": entry["order_count"],
            "known_notional_count": entry["known_notional_count"],
            "known_notional_subtotal": (
                _decimal_text(sum(entry["notionals"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY and entry["notionals"]
                else None
            ),
            "total_notional": (
                _decimal_text(sum(entry["notionals"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY
                and entry["notionals"]
                and entry["known_notional_count"] == entry["order_count"]
                else None
            ),
        }
        for key, entry in sorted(stats.items())
    }


def _exposure_subtotals_by_currency(
    *,
    holdings: list[HoldingRiskRow],
    open_orders: list[OpenOrderRiskRow],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for holding in holdings:
        key = _currency_key(holding.settlement_currency)
        entry = stats.setdefault(
            key,
            {
                "holding_count": 0,
                "known_holding_value_count": 0,
                "open_buy_order_count": 0,
                "known_open_buy_notional_count": 0,
                "holding_values": [],
                "open_buy_notionals": [],
            },
        )
        entry["holding_count"] = int(entry["holding_count"]) + 1
        if holding.market_value is not None:
            entry["known_holding_value_count"] = (
                int(entry["known_holding_value_count"]) + 1
            )
            entry["holding_values"].append(holding.market_value)
    for order in open_orders:
        if order.side != "buy":
            continue
        key = _currency_key(order.settlement_currency)
        entry = stats.setdefault(
            key,
            {
                "holding_count": 0,
                "known_holding_value_count": 0,
                "open_buy_order_count": 0,
                "known_open_buy_notional_count": 0,
                "holding_values": [],
                "open_buy_notionals": [],
            },
        )
        entry["open_buy_order_count"] = int(entry["open_buy_order_count"]) + 1
        if order.notional is not None:
            entry["known_open_buy_notional_count"] = (
                int(entry["known_open_buy_notional_count"]) + 1
            )
            entry["open_buy_notionals"].append(order.notional)
    payload: dict[str, dict[str, object]] = {}
    for key, entry in sorted(stats.items()):
        if key == UNKNOWN_CURRENCY_KEY:
            holding_value = None
            open_buy_notional = None
            total_exposure = None
            known_holding_value_subtotal = None
            known_open_buy_notional_subtotal = None
        else:
            holding_value_decimal = sum(entry["holding_values"], Decimal("0"))
            open_buy_notional_decimal = sum(
                entry["open_buy_notionals"], Decimal("0")
            )
            holding_values_complete = (
                entry["holding_count"] == entry["known_holding_value_count"]
            )
            open_buy_values_complete = (
                entry["open_buy_order_count"]
                == entry["known_open_buy_notional_count"]
            )
            known_holding_value_subtotal = _decimal_text(holding_value_decimal)
            known_open_buy_notional_subtotal = _decimal_text(
                open_buy_notional_decimal
            )
            holding_value = (
                known_holding_value_subtotal if holding_values_complete else None
            )
            open_buy_notional = (
                known_open_buy_notional_subtotal
                if open_buy_values_complete
                else None
            )
            total_exposure = (
                _decimal_text(holding_value_decimal + open_buy_notional_decimal)
                if holding_values_complete and open_buy_values_complete
                else None
            )
        payload[key] = {
            "holding_count": entry["holding_count"],
            "known_holding_value_count": entry["known_holding_value_count"],
            "open_buy_order_count": entry["open_buy_order_count"],
            "known_open_buy_notional_count": entry[
                "known_open_buy_notional_count"
            ],
            "known_holding_value_subtotal": known_holding_value_subtotal,
            "known_open_buy_notional_subtotal": (
                known_open_buy_notional_subtotal
            ),
            "holding_value": holding_value,
            "open_buy_notional": open_buy_notional,
            "total_exposure": total_exposure,
        }
    return payload


def _stop_risk_subtotals_by_currency(
    rows: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for row in rows:
        currency = (
            str(row["settlement_currency"])
            if row.get("settlement_currency") is not None
            else None
        )
        key = _currency_key(currency)
        entry = stats.setdefault(
            key,
            {"position_count": 0, "known_estimated_risk_count": 0, "risks": []},
        )
        entry["position_count"] = int(entry["position_count"]) + 1
        risk = _decimal_or_none(row.get("estimated_risk"))
        if risk is not None:
            entry["known_estimated_risk_count"] = (
                int(entry["known_estimated_risk_count"]) + 1
            )
            entry["risks"].append(risk)
    return {
        key: {
            "position_count": entry["position_count"],
            "known_estimated_risk_count": entry["known_estimated_risk_count"],
            "total_estimated_risk": (
                _decimal_text(sum(entry["risks"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY
                and entry["risks"]
                and entry["known_estimated_risk_count"]
                == entry["position_count"]
                else None
            ),
            "known_estimated_risk_subtotal": (
                _decimal_text(sum(entry["risks"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY and entry["risks"]
                else None
            ),
        }
        for key, entry in sorted(stats.items())
    }


def _known_holding_totals(
    holdings: list[HoldingRiskRow],
) -> dict[str, Decimal]:
    values: dict[str, list[Decimal]] = {}
    counts: dict[str, int] = {}
    for holding in holdings:
        if holding.settlement_currency is None:
            continue
        counts[holding.settlement_currency] = (
            counts.get(holding.settlement_currency, 0) + 1
        )
        if holding.market_value is not None:
            values.setdefault(holding.settlement_currency, []).append(
                holding.market_value
            )
    return {
        currency: sum(currency_values, Decimal("0"))
        for currency, currency_values in values.items()
        if len(currency_values) == counts[currency]
    }


def _track_entry_currency(entry: dict[str, Any], currency: str | None) -> None:
    if currency is None:
        entry["unknown_currency"] = True
        return
    entry["currencies"].add(currency)


def _currency_group_sort_key(
    *,
    currency: str | None,
    value: Decimal | None,
) -> tuple[bool, str, Decimal]:
    return (
        currency is None,
        currency or UNKNOWN_CURRENCY_KEY,
        -(value or Decimal("0")),
    )


def _currency_key(currency: str | None) -> str:
    return currency or UNKNOWN_CURRENCY_KEY


def _explicit_currency_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in (
        "settlement_currency",
        "quote_currency",
        "currency",
        "crcy_cd",
        "tr_crcy_cd",
    ):
        currency = _normalized_currency(payload.get(key))
        if currency is not None:
            return currency
    for key in ("economics", "instrument", "holding", "order", "_position_sync"):
        currency = _explicit_currency_from_payload(payload.get(key))
        if currency is not None:
            return currency
    return None


def _route_metadata(payload: object) -> tuple[str | None, str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None, None
    candidates = [payload]
    candidates.extend(
        nested
        for key in ("instrument", "route", "_position_sync")
        if isinstance((nested := payload.get(key)), dict)
    )
    provider = None
    venue = None
    asset_class = None
    for candidate in candidates:
        if provider is None:
            provider = _normalized_provider(
                candidate.get("provider") or candidate.get("broker_name")
            )
        if venue is None:
            venue = _normalized_venue(
                candidate.get("venue")
                or candidate.get("exchange")
                or candidate.get("ovrs_excg_cd")
                or candidate.get("excg_cd")
            )
        if asset_class is None:
            asset_class = _normalized_asset_class(candidate.get("asset_class"))
    return provider, venue, asset_class


def _currency_from_route(
    *,
    symbol: str,
    provider: str | None,
    venue: str | None,
    asset_class: str | None,
) -> str | None:
    normalized_symbol = str(symbol or "").strip().upper()
    if asset_class == "us_equity" or provider == "alpaca":
        return "USD"
    if asset_class in {"stock", "kr_equity", "domestic_equity"}:
        return "KRW"
    if provider == "upbit" and "-" in normalized_symbol:
        return _normalized_currency(normalized_symbol.split("-", 1)[0])
    if venue in {"AMEX", "NASD", "NYSE", "US"}:
        return "USD"
    if venue in {"KRX", "NXT", "SOR"}:
        return "KRW"
    if provider == "kis" and len(normalized_symbol) == 6 and normalized_symbol.isdigit():
        return "KRW"
    return None


def _normalized_currency(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if not (3 <= len(normalized) <= 8) or not normalized.isalnum():
        return None
    return normalized


def _normalized_provider(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().lower().split(":", 1)[0]


def _normalized_venue(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().upper()
    return {
        "AMS": "AMEX",
        "NAS": "NASD",
        "NASDAQ": "NASD",
        "NYS": "NYSE",
    }.get(normalized, normalized)


def _normalized_asset_class(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().lower()


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
        return Decimal(str(value).replace(",", "").strip())
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
