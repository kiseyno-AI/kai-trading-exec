from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN

from app.core.config import Settings


@dataclass(frozen=True)
class RiskSizingResult:
    quantity: int
    approved_notional: Decimal
    risk_pct: Decimal
    risk_budget: Decimal
    stop_distance: Decimal
    risk_quantity: int
    allocation_budget: Decimal
    allocation_quantity: int
    cash_quantity: int
    max_position_budget: Decimal
    max_symbol_exposure: Decimal
    existing_symbol_exposure: Decimal
    exposure_after_trade: Decimal
    daily_budget_remaining: Decimal
    cap_details: dict[str, str | int | None]


@dataclass(frozen=True)
class RiskSizingRejection:
    reason_code: str
    message: str
    details: dict[str, str | int | None]


RiskSizingDecision = RiskSizingResult | RiskSizingRejection


def resolve_risk_based_buy_quantity(
    *,
    settings: Settings,
    account_basis: Decimal | None,
    available_cash: Decimal | None,
    entry_price: Decimal | None,
    stop_price: Decimal | None,
    confidence: Decimal | None = None,
    requested_quantity: int | None = None,
    candidate_max_budget: Decimal | None = None,
    agent_max_quantity: int | None = None,
    symbol_buyable_quantity: int | None = None,
    existing_symbol_exposure: Decimal = Decimal("0"),
    daily_new_buy_notional_used: Decimal = Decimal("0"),
) -> RiskSizingDecision:
    if account_basis is None or account_basis <= 0:
        return _reject("account_basis_unavailable", "account equity/cash basis is unavailable")
    if available_cash is None or available_cash <= 0:
        return _reject("insufficient_cash", "available cash/buying power is unavailable")
    if entry_price is None or entry_price <= 0:
        return _reject("missing_reference_price", "entry/reference price is required for risk sizing")
    if stop_price is None or stop_price <= 0:
        return _reject("missing_stop_price", "stop price is required for risk sizing")

    stop_distance = entry_price - stop_price
    if stop_distance <= 0:
        return _reject(
            "invalid_stop_distance",
            "stop price must be below entry price for long-side risk sizing",
            entry_price=entry_price,
            stop_price=stop_price,
        )

    risk_pct = _risk_pct(settings=settings, confidence=confidence)
    risk_budget = (account_basis * risk_pct).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    risk_quantity = _floor_quantity(risk_budget / stop_distance)
    if risk_quantity <= 0:
        return _reject(
            "risk_budget_too_small",
            "risk budget cannot buy one share at the configured stop distance",
            risk_pct=risk_pct,
            risk_budget=risk_budget,
            stop_distance=stop_distance,
        )

    max_position_budget = (account_basis * _decimal_setting(
        settings,
        "exec_agent_max_position_allocation_pct",
        Decimal("0.10"),
    )).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    max_symbol_exposure = min(
        account_basis
        * _decimal_setting(
            settings,
            "exec_agent_max_symbol_total_exposure_pct",
            _decimal_setting(settings, "exec_agent_max_symbol_exposure_pct", Decimal("0.20")),
        ),
        _decimal_setting(settings, "exec_agent_max_symbol_budget", Decimal("1000000")),
    ).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    symbol_exposure_remaining = max_symbol_exposure - existing_symbol_exposure
    if symbol_exposure_remaining <= 0:
        return _reject(
            "symbol_exposure_cap_exceeded",
            "existing symbol exposure already reaches the configured cap",
            max_symbol_exposure=max_symbol_exposure,
            existing_symbol_exposure=existing_symbol_exposure,
        )

    daily_cap = _decimal_setting(settings, "exec_agent_max_daily_new_buy_notional", Decimal("500000"))
    daily_budget_remaining = (daily_cap - daily_new_buy_notional_used).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if daily_budget_remaining <= 0:
        return _reject(
            "daily_new_buy_cap_reached",
            "daily new-buy notional cap is already used",
            daily_new_buy_notional_used=daily_new_buy_notional_used,
            daily_cap=daily_cap,
        )
    if daily_budget_remaining < entry_price:
        return _reject(
            "daily_new_buy_cap_reached",
            "daily new-buy notional cap cannot fund one share at the entry price",
            daily_new_buy_notional_used=daily_new_buy_notional_used,
            daily_cap=daily_cap,
            daily_budget_remaining=daily_budget_remaining,
            entry_price=entry_price,
        )

    budget_candidates = [
        available_cash,
        max_position_budget,
        symbol_exposure_remaining,
        daily_budget_remaining,
        _decimal_setting(settings, "exec_agent_max_symbol_budget", Decimal("1000000")),
    ]
    if candidate_max_budget is not None:
        budget_candidates.append(candidate_max_budget)
    allocation_budget = min(budget_candidates).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    allocation_quantity = _floor_quantity(allocation_budget / entry_price)
    cash_quantity = _floor_quantity(available_cash / entry_price)

    quantity_caps = [risk_quantity, allocation_quantity, cash_quantity]
    if requested_quantity is not None:
        quantity_caps.append(requested_quantity)
    if agent_max_quantity is not None:
        quantity_caps.append(agent_max_quantity)
    if symbol_buyable_quantity is not None:
        quantity_caps.append(symbol_buyable_quantity)

    quantity = min(quantity_caps)
    if quantity <= 0:
        return _reject(
            "insufficient_cash",
            "risk, allocation, cash, or quantity caps cannot buy one share",
            risk_quantity=risk_quantity,
            allocation_quantity=allocation_quantity,
            cash_quantity=cash_quantity,
            requested_quantity=requested_quantity,
            agent_max_quantity=agent_max_quantity,
            symbol_buyable_quantity=symbol_buyable_quantity,
        )

    approved_notional = (entry_price * Decimal(quantity)).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    exposure_after_trade = existing_symbol_exposure + approved_notional
    return RiskSizingResult(
        quantity=quantity,
        approved_notional=approved_notional,
        risk_pct=risk_pct,
        risk_budget=risk_budget,
        stop_distance=stop_distance,
        risk_quantity=risk_quantity,
        allocation_budget=allocation_budget,
        allocation_quantity=allocation_quantity,
        cash_quantity=cash_quantity,
        max_position_budget=max_position_budget,
        max_symbol_exposure=max_symbol_exposure,
        existing_symbol_exposure=existing_symbol_exposure,
        exposure_after_trade=exposure_after_trade,
        daily_budget_remaining=daily_budget_remaining,
        cap_details={
            "requested_quantity": requested_quantity,
            "agent_max_quantity": agent_max_quantity,
            "symbol_buyable_quantity": symbol_buyable_quantity,
            "candidate_max_budget": _text(candidate_max_budget),
            "daily_new_buy_notional_used": _text(daily_new_buy_notional_used),
        },
    )


def effective_stop_price(
    *,
    settings: Settings,
    entry_price: Decimal,
    upstream_stop_price: Decimal,
) -> Decimal:
    hard_stop_pct = _decimal_setting(settings, "exec_agent_stop_loss_pct", Decimal("0.04"))
    hard_stop_price = (entry_price * (Decimal("1") - hard_stop_pct)).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    return max(upstream_stop_price, hard_stop_price)


def high_confidence_for_aggressive_risk(*, settings: Settings, confidence: Decimal | None) -> bool:
    if confidence is None:
        return False
    threshold = _decimal_setting(settings, "exec_agent_aggressive_confidence_threshold", Decimal("0.90"))
    return confidence >= threshold


def _risk_pct(*, settings: Settings, confidence: Decimal | None) -> Decimal:
    base = _decimal_setting(settings, "exec_agent_base_risk_pct", Decimal("0.005"))
    aggressive = _decimal_setting(settings, "exec_agent_aggressive_risk_pct", Decimal("0.015"))
    if high_confidence_for_aggressive_risk(settings=settings, confidence=confidence):
        return min(aggressive, Decimal("0.015"))
    return min(base, Decimal("0.010"))


def _floor_quantity(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _decimal_setting(settings: Settings, field_name: str, default: Decimal) -> Decimal:
    value = getattr(settings, field_name, default)
    try:
        parsed = Decimal(str(value))
    except Exception:
        return default
    if parsed <= 0:
        return default
    return parsed


def _reject(
    reason_code: str,
    message: str,
    **details: Decimal | int | None,
) -> RiskSizingRejection:
    return RiskSizingRejection(
        reason_code=reason_code,
        message=message,
        details={key: _text(value) for key, value in details.items()},
    )


def _text(value: Decimal | int | None) -> str | int | None:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    return value
