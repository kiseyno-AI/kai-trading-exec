from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.broker.base import (
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.json_compaction import compact_json_object

SHORT_TERM_DECIDER_NAME = "short-term-decider"
NEXT_OPEN_BUY_INTENT = "next_open_buy"
EXEC_AGENT_NAME = "short_term_exec_agent_v1"
EXEC_AGENT_DECISION_KEY = "exec_agent_decision"
ORIGINAL_QUANTITY_POLICY_KEY = "original_quantity_policy"
OPEN_BUY_ORDER_STATUSES = frozenset(CAPITAL_RESERVING_ORDER_STATUSES)
ACTIVE_SOURCE_STATUSES = frozenset(
    {
        "pending",
        "next_open_buy_scheduled",
        "next_open_buy_dispatching",
        "next_open_buy_retry_pending",
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "submitted",
        "open",
        "partially_filled",
        "retry_pending",
    }
)
BUDGET_KEYS = (
    "max_budget",
    "max_budget_krw",
    "budget",
    "budget_krw",
    "cash_budget",
    "cash_budget_krw",
    "max_notional",
    "max_notional_krw",
    "notional",
)
MAX_QUANTITY_KEYS = ("max_quantity", "max_shares", "share_cap")
REFERENCE_PRICE_KEYS = (
    "reference_price",
    "entry_reference_price",
    "entry_price",
    "last_price",
    "current_price",
    "price",
)
TARGET_PRICE_KEYS = (
    "target_price",
    "target_sell_price",
    "take_profit_price",
    "profit_target_price",
    "target",
)
STOP_PRICE_KEYS = (
    "stop_price",
    "stop_sell_price",
    "stop_loss_price",
    "risk_stop_price",
    "stop",
)


@dataclass(frozen=True)
class LocalAccountState:
    positions_by_symbol: dict[str, int]
    open_buy_order_symbols: set[str]
    active_source_keys: set[str]


@dataclass(frozen=True)
class TargetStopContext:
    target_price: Decimal | None
    stop_price: Decimal | None
    target_items: list[ExecutionPlanItem]
    stop_items: list[ExecutionPlanItem]

    @property
    def complete(self) -> bool:
        target_ready = self.target_price is not None or any(
            item.trigger_price is not None for item in self.target_items
        )
        stop_ready = self.stop_price is not None or any(
            item.trigger_price is not None for item in self.stop_items
        )
        return target_ready and stop_ready


@dataclass(frozen=True)
class PreparedCandidate:
    item: ExecutionPlanItem
    reference_price: Decimal
    target_stop: TargetStopContext
    max_budget: Decimal
    requested_shares: int | None
    max_quantity: int | None
    confidence: Decimal | None
    conviction: str | None
    budget_multiplier: Decimal
    source_key: str | None


def apply_short_term_exec_agent_decisions(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    plan: ExecutionPlan,
) -> None:
    candidates = [
        item
        for item in plan.items
        if _is_short_term_next_open_buy(item)
    ]
    if not candidates:
        return

    local_state = _load_local_account_state(settings=settings, db=db)
    seen_source_keys: set[str] = set()
    prepared_candidates: list[PreparedCandidate] = []
    for item in candidates:
        if item.status != "pending":
            _record_decision(
                item=item,
                decision="rejected",
                reason="unsupported_buy_semantics",
                details={"status": item.status},
            )
            continue

        target_stop = _target_stop_context(plan=plan, item=item)
        if not target_stop.complete:
            _reject(item=item, reason="missing_target_or_stop")
            continue

        reference_price = _reference_price(item)
        if reference_price is None:
            _reject(item=item, reason="missing_reference_price")
            continue

        budget_result = _candidate_budget(item=item, reference_price=reference_price)
        if isinstance(budget_result, str):
            _reject(item=item, reason=budget_result)
            continue
        max_budget, requested_shares, max_quantity = budget_result

        confidence = _candidate_confidence(item)
        conviction = _candidate_conviction(item)
        budget_multiplier = _budget_multiplier(confidence=confidence, conviction=conviction)
        if budget_multiplier is None:
            _reject(
                item=item,
                reason="confidence_below_minimum",
                details={"confidence": _decimal_text(confidence), "conviction": conviction},
            )
            continue

        source_key = _source_candidate_key(item)
        if source_key is not None:
            if source_key in seen_source_keys or source_key in local_state.active_source_keys:
                _reject(item=item, reason="duplicate_source_candidate", details={"source_key": source_key})
                continue
            seen_source_keys.add(source_key)

        prepared_candidates.append(
            PreparedCandidate(
                item=item,
                reference_price=reference_price,
                target_stop=target_stop,
                max_budget=max_budget,
                requested_shares=requested_shares,
                max_quantity=max_quantity,
                confidence=confidence,
                conviction=conviction,
                budget_multiplier=budget_multiplier,
                source_key=source_key,
            )
        )

    if not prepared_candidates:
        return

    reference_prices = {
        candidate.item.symbol: candidate.reference_price
        for candidate in prepared_candidates
    }
    try:
        account_snapshot = _load_broker_account_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=[candidate.item.symbol for candidate in prepared_candidates],
            reference_prices=reference_prices,
        )
    except (BrokerAdapterError, RuntimeError, ValueError) as exc:
        for candidate in prepared_candidates:
            _reject(
                item=candidate.item,
                reason="account_state_unavailable",
                details={"message": str(exc)},
            )
        return

    remaining_cash = _minimum_decimal(
        account_snapshot.available_cash,
        account_snapshot.buying_power,
    )
    if remaining_cash is None or remaining_cash <= 0:
        for candidate in prepared_candidates:
            _reject(
                item=candidate.item,
                reason="buying_power_unavailable",
                details={
                    "available_cash": _decimal_text(account_snapshot.available_cash),
                    "buying_power": _decimal_text(account_snapshot.buying_power),
                },
            )
        return

    approved_symbols: set[str] = set()
    for candidate in prepared_candidates:
        item = candidate.item
        broker_position_quantity = _broker_position_quantity(
            account_snapshot=account_snapshot,
            symbol=item.symbol,
        )
        local_position_quantity = local_state.positions_by_symbol.get(item.symbol, 0)
        if broker_position_quantity > 0 or local_position_quantity > 0:
            _reject(
                item=item,
                reason="existing_position",
                details={
                    "broker_position_quantity": broker_position_quantity,
                    "local_position_quantity": local_position_quantity,
                },
            )
            continue

        if (
            item.symbol in approved_symbols
            or item.symbol in local_state.open_buy_order_symbols
            or _broker_has_open_buy_order(account_snapshot=account_snapshot, symbol=item.symbol)
        ):
            _reject(item=item, reason="existing_open_buy_order")
            continue

        symbol_power = account_snapshot.symbol_buying_power.get(item.symbol)
        symbol_cash = _minimum_decimal(
            symbol_power.available_cash if symbol_power is not None else None,
            symbol_power.buying_power if symbol_power is not None else None,
        )
        cash_cap = _minimum_decimal(remaining_cash, symbol_cash)
        if cash_cap is None or cash_cap <= 0:
            _reject(item=item, reason="insufficient_cash")
            continue

        quantity = _approved_quantity(
            candidate=candidate,
            cash_cap=cash_cap,
            symbol_buyable_quantity=(
                symbol_power.buyable_quantity if symbol_power is not None else None
            ),
        )
        if quantity is None:
            _reject(
                item=item,
                reason="insufficient_cash",
                details={
                    "available_cash": _decimal_text(cash_cap),
                    "reference_price": _decimal_text(candidate.reference_price),
                    "max_budget": _decimal_text(candidate.max_budget),
                },
            )
            continue

        notional = candidate.reference_price * Decimal(quantity)
        remaining_cash -= notional
        approved_symbols.add(item.symbol)
        _approve(candidate=candidate, quantity=quantity, notional=notional)
        _ensure_monitoring_items(
            plan=plan,
            candidate=candidate,
            quantity=quantity,
        )


def _load_broker_account_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: list[str],
    reference_prices: dict[str, Decimal],
) -> BrokerAccountSnapshot:
    session_state = ensure_authenticated_broker_session(
        settings=settings,
        db=db,
        adapter=adapter,
        source="short_term_exec_agent",
        reason="resolve account buying power for short-term candidate sizing",
    )
    return adapter.get_account_snapshot(
        session_state.access_token or "",
        symbols=symbols,
        reference_prices=reference_prices,
    )


def _load_local_account_state(
    *,
    settings: Settings,
    db: Session,
) -> LocalAccountState:
    positions = db.scalars(
        select(ExecutionPosition).where(ExecutionPosition.mode == settings.runtime_mode)
    ).all()
    orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(tuple(OPEN_BUY_ORDER_STATUSES)),
        )
    ).all()
    plan_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.intent == NEXT_OPEN_BUY_INTENT,
            ExecutionPlanItem.decision_agent_name == SHORT_TERM_DECIDER_NAME,
            ExecutionPlanItem.status.in_(tuple(ACTIVE_SOURCE_STATUSES)),
        )
    ).all()
    return LocalAccountState(
        positions_by_symbol={
            position.symbol: position.quantity
            for position in positions
            if position.quantity > 0
        },
        open_buy_order_symbols={order.symbol for order in orders},
        active_source_keys={
            source_key
            for source_key in (_source_candidate_key(item) for item in plan_items)
            if source_key is not None
        },
    )


def _is_short_term_next_open_buy(item: ExecutionPlanItem) -> bool:
    return (
        item.side == "buy"
        and item.intent == NEXT_OPEN_BUY_INTENT
        and (item.decision_agent_name or "").strip().lower() == SHORT_TERM_DECIDER_NAME
    )


def _target_stop_context(
    *,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
) -> TargetStopContext:
    sibling_items = [
        sibling
        for sibling in plan.items
        if sibling is not item
        and sibling.symbol == item.symbol
        and sibling.side == "sell"
        and sibling.intent in {"target_sell", "stop_sell"}
    ]
    target_items = [sibling for sibling in sibling_items if sibling.intent == "target_sell"]
    stop_items = [sibling for sibling in sibling_items if sibling.intent == "stop_sell"]
    return TargetStopContext(
        target_price=_first_decimal_from_item(item, TARGET_PRICE_KEYS),
        stop_price=_first_decimal_from_item(item, STOP_PRICE_KEYS),
        target_items=target_items,
        stop_items=stop_items,
    )


def _reference_price(item: ExecutionPlanItem) -> Decimal | None:
    if item.limit_price is not None and item.limit_price > 0:
        return item.limit_price
    return _first_decimal_from_item(item, REFERENCE_PRICE_KEYS)


def _candidate_budget(
    *,
    item: ExecutionPlanItem,
    reference_price: Decimal,
) -> tuple[Decimal, int | None, int | None] | str:
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    kind = quantity_policy.get("kind")
    requested_shares = None
    if kind == "shares":
        requested_shares = _parse_positive_int(quantity_policy.get("quantity"))
        if requested_shares is None:
            return "invalid_share_quantity"
        max_budget = reference_price * Decimal(requested_shares)
    elif kind in {"candidate", "budget", "cash_budget", "notional"}:
        max_budget = _first_decimal(quantity_policy, BUDGET_KEYS)
        if max_budget is None:
            max_budget = _first_decimal(item.risk_context, BUDGET_KEYS)
        if max_budget is None or max_budget <= 0:
            return "missing_candidate_budget"
    else:
        max_budget = _first_decimal(quantity_policy, BUDGET_KEYS)
        if max_budget is None:
            max_budget = _first_decimal(item.risk_context, BUDGET_KEYS)
        if max_budget is None or max_budget <= 0:
            return "unsupported_quantity_policy"

    max_quantity = _first_positive_int(quantity_policy, MAX_QUANTITY_KEYS)
    if max_quantity is None:
        max_quantity = _first_positive_int(item.risk_context, MAX_QUANTITY_KEYS)
    return max_budget, requested_shares, max_quantity


def _approved_quantity(
    *,
    candidate: PreparedCandidate,
    cash_cap: Decimal,
    symbol_buyable_quantity: int | None,
) -> int | None:
    if candidate.requested_shares is not None:
        quantity = candidate.requested_shares
    else:
        sizing_budget = min(candidate.max_budget, cash_cap) * candidate.budget_multiplier
        quantity = int(
            (sizing_budget / candidate.reference_price).quantize(
                Decimal("1"),
                rounding=ROUND_DOWN,
            )
        )
    if candidate.max_quantity is not None:
        quantity = min(quantity, candidate.max_quantity)
    if symbol_buyable_quantity is not None:
        quantity = min(quantity, symbol_buyable_quantity)
    if quantity <= 0:
        return None
    notional = candidate.reference_price * Decimal(quantity)
    if notional > cash_cap or notional > candidate.max_budget:
        return None
    return quantity


def _candidate_confidence(item: ExecutionPlanItem) -> Decimal | None:
    value = _first_decimal(item.risk_context, ("confidence", "score", "probability"))
    if value is None:
        value = _first_decimal(item.quantity_policy, ("confidence",))
    if value is None:
        return None
    if value > 1 and value <= 100:
        value = value / Decimal("100")
    if value < 0 or value > 1:
        return None
    return value


def _candidate_conviction(item: ExecutionPlanItem) -> str | None:
    for source in (item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        value = source.get("conviction")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return None


def _budget_multiplier(
    *,
    confidence: Decimal | None,
    conviction: str | None,
) -> Decimal | None:
    if confidence is not None:
        if confidence < Decimal("0.50"):
            return None
        if confidence >= Decimal("0.80"):
            return Decimal("1.00")
        if confidence >= Decimal("0.65"):
            return Decimal("0.75")
        return Decimal("0.50")
    if conviction in {"very_high", "high", "strong", "highest"}:
        return Decimal("1.00")
    if conviction in {"medium", "moderate", "normal"}:
        return Decimal("0.75")
    if conviction in {"low", "weak"}:
        return Decimal("0.50")
    return Decimal("0.50")


def _broker_position_quantity(
    *,
    account_snapshot: BrokerAccountSnapshot,
    symbol: str,
) -> int:
    positions = account_snapshot.positions or account_snapshot.holdings
    return sum(
        position.quantity
        for position in positions
        if position.symbol == symbol and position.quantity > 0
    )


def _broker_has_open_buy_order(
    *,
    account_snapshot: BrokerAccountSnapshot,
    symbol: str,
) -> bool:
    return any(
        order.symbol == symbol
        and order.side == "buy"
        and order.quantity > 0
        and order.status.lower() in OPEN_BUY_ORDER_STATUSES
        for order in account_snapshot.open_orders
    )


def _approve(
    *,
    candidate: PreparedCandidate,
    quantity: int,
    notional: Decimal,
) -> None:
    item = candidate.item
    original_policy = _json_safe(item.quantity_policy)
    item.quantity_policy = {
        "kind": "shares",
        "quantity": quantity,
        "source": EXEC_AGENT_NAME,
    }
    risk_context = _risk_context(item)
    risk_context[ORIGINAL_QUANTITY_POLICY_KEY] = original_policy
    risk_context[EXEC_AGENT_DECISION_KEY] = _decision_payload(
        decision="approved",
        reason="approved_by_deterministic_rails",
        candidate=candidate,
        quantity=quantity,
        notional=notional,
    )
    item.risk_context = compact_json_object(risk_context)


def _reject(
    *,
    item: ExecutionPlanItem,
    reason: str,
    details: dict[str, object] | None = None,
) -> None:
    item.status = "exec_agent_rejected"
    _record_decision(item=item, decision="rejected", reason=reason, details=details)


def _record_decision(
    *,
    item: ExecutionPlanItem,
    decision: str,
    reason: str,
    details: dict[str, object] | None = None,
) -> None:
    risk_context = _risk_context(item)
    risk_context.setdefault(ORIGINAL_QUANTITY_POLICY_KEY, _json_safe(item.quantity_policy))
    risk_context[EXEC_AGENT_DECISION_KEY] = {
        "agent": EXEC_AGENT_NAME,
        "decision": decision,
        "reason": reason,
        "details": _json_safe(details or {}),
    }
    item.risk_context = compact_json_object(risk_context)


def _decision_payload(
    *,
    decision: str,
    reason: str,
    candidate: PreparedCandidate,
    quantity: int,
    notional: Decimal,
) -> dict[str, object]:
    return {
        "agent": EXEC_AGENT_NAME,
        "decision": decision,
        "reason": reason,
        "approved_quantity": quantity,
        "reference_price": _decimal_text(candidate.reference_price),
        "approved_notional": _decimal_text(notional),
        "candidate_max_budget": _decimal_text(candidate.max_budget),
        "budget_multiplier": _decimal_text(candidate.budget_multiplier),
        "confidence": _decimal_text(candidate.confidence),
        "conviction": candidate.conviction,
        "source_key": candidate.source_key,
    }


def _ensure_monitoring_items(
    *,
    plan: ExecutionPlan,
    candidate: PreparedCandidate,
    quantity: int,
) -> None:
    target_items = list(candidate.target_stop.target_items)
    stop_items = list(candidate.target_stop.stop_items)
    if not target_items and candidate.target_stop.target_price is not None:
        target_items.append(
            _new_monitoring_item(
                plan=plan,
                entry_item=candidate.item,
                intent="target_sell",
                trigger_price=candidate.target_stop.target_price,
                quantity=quantity,
            )
        )
    if not stop_items and candidate.target_stop.stop_price is not None:
        stop_items.append(
            _new_monitoring_item(
                plan=plan,
                entry_item=candidate.item,
                intent="stop_sell",
                trigger_price=candidate.target_stop.stop_price,
                quantity=quantity,
            )
        )

    for monitor_item in target_items + stop_items:
        trigger_price = (
            candidate.target_stop.target_price
            if monitor_item.intent == "target_sell"
            else candidate.target_stop.stop_price
        )
        _link_monitoring_item(
            monitor_item=monitor_item,
            entry_item=candidate.item,
            quantity=quantity,
            trigger_price=trigger_price,
        )


def _new_monitoring_item(
    *,
    plan: ExecutionPlan,
    entry_item: ExecutionPlanItem,
    intent: str,
    trigger_price: Decimal,
    quantity: int,
) -> ExecutionPlanItem:
    next_index = max((item.plan_item_index for item in plan.items), default=-1) + 1
    source_suffix = "target" if intent == "target_sell" else "stop"
    item = ExecutionPlanItem(
        plan_item_index=next_index,
        symbol=entry_item.symbol,
        side="sell",
        intent=intent,
        status="pending",
        quantity_policy={
            "kind": "shares",
            "quantity": quantity,
            "source": EXEC_AGENT_NAME,
        },
        trigger_price=trigger_price,
        time_in_force=entry_item.time_in_force,
        risk_context={
            "exchange": _exchange(entry_item),
            "generated_by": EXEC_AGENT_NAME,
        },
        decision_agent_name=entry_item.decision_agent_name,
        decision_snapshot_id=entry_item.decision_snapshot_id,
        decision_run_id=entry_item.decision_run_id,
        source_plan_item_id=_derived_source_plan_item_id(entry_item, source_suffix),
        source_market_date=entry_item.source_market_date,
    )
    plan.items.append(item)
    return item


def _link_monitoring_item(
    *,
    monitor_item: ExecutionPlanItem,
    entry_item: ExecutionPlanItem,
    quantity: int,
    trigger_price: Decimal | None,
) -> None:
    if monitor_item.trigger_price is None and trigger_price is not None:
        monitor_item.trigger_price = trigger_price
    if not _monitor_quantity_supported(monitor_item.quantity_policy):
        monitor_item.quantity_policy = {
            "kind": "shares",
            "quantity": quantity,
            "source": EXEC_AGENT_NAME,
        }
    elif isinstance(monitor_item.quantity_policy, dict) and monitor_item.quantity_policy.get("kind") == "shares":
        monitor_quantity = _parse_positive_int(monitor_item.quantity_policy.get("quantity"))
        if monitor_quantity is not None and monitor_quantity > quantity:
            monitor_item.quantity_policy = {
                **monitor_item.quantity_policy,
                "quantity": quantity,
                "capped_by": EXEC_AGENT_NAME,
            }

    risk_context = _risk_context(monitor_item)
    risk_context.setdefault("exchange", _exchange(entry_item))
    risk_context["entry_context"] = {
        "source_plan_item_id": entry_item.source_plan_item_id,
        "decision_run_id": entry_item.decision_run_id,
        "decision_snapshot_id": entry_item.decision_snapshot_id,
        "entry_quantity": quantity,
        "linked_by": EXEC_AGENT_NAME,
    }
    monitor_item.risk_context = compact_json_object(risk_context)


def _monitor_quantity_supported(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    kind = quantity_policy.get("kind")
    if kind == "shares":
        return _parse_positive_int(quantity_policy.get("quantity")) is not None
    if kind == "percent":
        fraction = _parse_decimal(quantity_policy.get("fraction"))
        return fraction is not None and fraction > 0 and fraction <= 1
    return False


def _risk_context(item: ExecutionPlanItem) -> dict[str, object]:
    if isinstance(item.risk_context, dict):
        return dict(item.risk_context)
    return {}


def _exchange(item: ExecutionPlanItem) -> str:
    risk_context = _risk_context(item)
    exchange = risk_context.get("exchange")
    if isinstance(exchange, str) and exchange.strip():
        return exchange.strip().upper()
    return "KRX"


def _source_candidate_key(item: ExecutionPlanItem) -> str | None:
    if not item.source_plan_item_id:
        return None
    source_market_date = _date_text(item.source_market_date)
    return "|".join(
        [
            (item.decision_agent_name or "").strip().lower(),
            item.symbol,
            item.intent,
            item.decision_run_id or "",
            item.source_plan_item_id,
            source_market_date or "",
        ]
    )


def _derived_source_plan_item_id(entry_item: ExecutionPlanItem, suffix: str) -> str | None:
    source = entry_item.source_plan_item_id
    if not source:
        return None
    derived = f"{source}:{suffix}"
    if len(derived) <= 128:
        return derived
    return derived[:128]


def _first_decimal_from_item(item: ExecutionPlanItem, keys: tuple[str, ...]) -> Decimal | None:
    value = _first_decimal(item.risk_context, keys)
    if value is not None:
        return value
    return _first_decimal(item.quantity_policy, keys)


def _first_decimal(source: object, keys: tuple[str, ...]) -> Decimal | None:
    if not isinstance(source, dict):
        return None
    for key in keys:
        value = _parse_decimal(source.get(key))
        if value is not None and value > 0:
            return value
    return None


def _first_positive_int(source: object, keys: tuple[str, ...]) -> int | None:
    if not isinstance(source, dict):
        return None
    for key in keys:
        value = _parse_positive_int(source.get(key))
        if value is not None:
            return value
    return None


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _minimum_decimal(*values: Decimal | None) -> Decimal | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return min(concrete)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _date_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
