from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from math import sqrt
from typing import Any

from app.core.config import Settings
from app.services.market_regime_guardrails import evaluate_market_regime_guardrail


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
    cap_details: dict[str, object]


@dataclass(frozen=True)
class RiskSizingRejection:
    reason_code: str
    message: str
    details: dict[str, object]


RiskSizingDecision = RiskSizingResult | RiskSizingRejection


def apply_vol_corr_quantity_cap(
    *,
    settings: Settings,
    baseline_quantity: int,
    context: dict[str, object] | None,
) -> dict[str, str | int | None] | None:
    return _vol_corr_cap_details(
        settings=settings,
        baseline_quantity=baseline_quantity,
        context=context,
    )


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
    vol_corr_context: dict[str, object] | None = None,
    market_regime_context: dict[str, object] | None = None,
    market_date: object | None = None,
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

    daily_cap = _decimal_setting(settings, "exec_agent_max_daily_new_buy_notional", Decimal("0"))
    daily_budget_remaining: Decimal | None = None
    if daily_cap > 0:
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
        _decimal_setting(settings, "exec_agent_max_symbol_budget", Decimal("1000000")),
    ]
    if daily_budget_remaining is not None:
        budget_candidates.append(daily_budget_remaining)
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

    baseline_quantity = min(quantity_caps)
    quantity = baseline_quantity
    vol_corr_cap_details = apply_vol_corr_quantity_cap(
        settings=settings,
        baseline_quantity=baseline_quantity,
        context=vol_corr_context,
    )
    if vol_corr_cap_details is not None:
        quantity = int(vol_corr_cap_details["capped_quantity"])
    quantity_before_market_regime = quantity
    market_regime_guardrail = evaluate_market_regime_guardrail(
        settings=settings,
        context=market_regime_context,
        market_date=market_date,
        original_quantity=quantity,
        original_notional=entry_price * Decimal(quantity),
        reference_price=entry_price,
    )
    if market_regime_guardrail.rejected:
        return _reject(
            market_regime_guardrail.reason_code,
            "market-regime guardrail rejected this automated buy",
            market_regime_guardrail=market_regime_guardrail.audit,
        )
    if market_regime_guardrail.action == "downsize":
        adjusted_notional = market_regime_guardrail.audit.get("adjusted_notional_krw")
        adjusted_quantity = _floor_quantity(Decimal(str(adjusted_notional)) / entry_price) if adjusted_notional else 0
        quantity = min(quantity, adjusted_quantity)
    if quantity <= 0:
        if baseline_quantity > 0 and quantity_before_market_regime > 0 and market_regime_guardrail.action == "downsize":
            return _reject(
                "market_regime_min_lot_rejected",
                "market-regime guardrail downsized this buy below one share",
                risk_quantity=risk_quantity,
                allocation_quantity=allocation_quantity,
                cash_quantity=cash_quantity,
                requested_quantity=requested_quantity,
                agent_max_quantity=agent_max_quantity,
                symbol_buyable_quantity=symbol_buyable_quantity,
                baseline_quantity=baseline_quantity,
                quantity_before_market_regime=quantity_before_market_regime,
                market_regime_guardrail=market_regime_guardrail.audit,
            )
        if baseline_quantity > 0 and vol_corr_cap_details is not None:
            return _reject(
                "vol_corr_min_lot_rejected",
                "volatility/correlation sizing cap downsized this buy below one share",
                risk_quantity=risk_quantity,
                allocation_quantity=allocation_quantity,
                cash_quantity=cash_quantity,
                requested_quantity=requested_quantity,
                agent_max_quantity=agent_max_quantity,
                symbol_buyable_quantity=symbol_buyable_quantity,
                baseline_quantity=baseline_quantity,
                vol_corr_cap_details=vol_corr_cap_details,
            )
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
            "baseline_quantity": baseline_quantity,
            **(vol_corr_cap_details or {}),
            "market_regime_guardrail": market_regime_guardrail.audit,
        },
    )


def effective_stop_price(
    *,
    settings: Settings,
    entry_price: Decimal,
    upstream_stop_price: Decimal,
) -> Decimal:
    hard_stop_pct = max(_decimal_setting(settings, "exec_agent_stop_loss_pct", Decimal("0.04")), Decimal("0.04"))
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
    max_trade_risk = _decimal_setting(settings, "exec_agent_max_trade_risk_pct", Decimal("0.015"))
    if high_confidence_for_aggressive_risk(settings=settings, confidence=confidence):
        return min(aggressive, max_trade_risk)
    return min(base, max_trade_risk)


def _vol_corr_cap_details(
    *,
    settings: Settings,
    baseline_quantity: int,
    context: dict[str, object] | None,
) -> dict[str, str | int | None] | None:
    if not bool(getattr(settings, "vol_corr_sizing_enabled", False)):
        return None
    if baseline_quantity <= 0:
        return None

    volatility_pct, volatility_source, volatility_observations = _resolve_volatility_pct(
        settings=settings,
        context=context,
    )
    volatility_multiplier, volatility_reason = _volatility_multiplier(
        settings=settings,
        volatility_pct=volatility_pct,
    )

    correlation, correlation_symbol, correlation_source, correlation_observations = _resolve_correlation(
        settings=settings,
        context=context,
    )
    correlation_multiplier, correlation_reason = _correlation_multiplier(
        settings=settings,
        correlation=correlation,
    )

    missing_reasons: list[str] = []
    if volatility_pct is None:
        missing_reasons.append("missing_volatility")
    if correlation is None and _has_correlation_holdings(context):
        missing_reasons.append("missing_correlation")
    missing_multiplier = Decimal("1")
    if missing_reasons and str(getattr(settings, "vol_corr_missing_data_behavior", "neutral")).lower() == "conservative":
        missing_multiplier = _decimal_setting(settings, "vol_corr_missing_data_multiplier", Decimal("0.75"))

    combined_multiplier = min(Decimal("1"), volatility_multiplier) * min(Decimal("1"), correlation_multiplier)
    combined_multiplier *= min(Decimal("1"), missing_multiplier)
    combined_multiplier = min(Decimal("1"), combined_multiplier)
    capped_quantity = _floor_quantity(Decimal(baseline_quantity) * combined_multiplier)
    if capped_quantity <= 0 and baseline_quantity > 0:
        capped_quantity = 1
    capped_quantity = min(baseline_quantity, capped_quantity)

    cap_reasons = [
        reason
        for reason, multiplier in (
            (volatility_reason, volatility_multiplier),
            (correlation_reason, correlation_multiplier),
        )
        if multiplier < Decimal("1")
    ]
    if missing_multiplier < Decimal("1"):
        cap_reasons.append("missing_data_conservative")
    final_reason = "neutral"
    if cap_reasons:
        final_reason = "+".join(cap_reasons)
    elif missing_reasons:
        final_reason = "missing_data_neutral"

    return {
        "vol_corr_enabled": "true",
        "baseline_quantity": baseline_quantity,
        "volatility_pct": _text(volatility_pct),
        "volatility_source": volatility_source,
        "volatility_observations": volatility_observations,
        "volatility_multiplier": _text(volatility_multiplier),
        "volatility_reason": volatility_reason,
        "correlation": _text(correlation),
        "correlation_symbol": correlation_symbol,
        "correlation_source": correlation_source,
        "correlation_observations": correlation_observations,
        "correlation_multiplier": _text(correlation_multiplier),
        "correlation_reason": correlation_reason,
        "missing_data_behavior": str(getattr(settings, "vol_corr_missing_data_behavior", "neutral")),
        "missing_data_reasons": ",".join(missing_reasons) if missing_reasons else None,
        "missing_data_multiplier": _text(missing_multiplier),
        "vol_corr_multiplier": _text(combined_multiplier),
        "vol_corr_cap_reason": final_reason,
        "capped_quantity": capped_quantity,
    }


def _resolve_volatility_pct(
    *,
    settings: Settings,
    context: dict[str, object] | None,
) -> tuple[Decimal | None, str | None, int | None]:
    if not isinstance(context, dict):
        return None, None, None
    for key in (
        "realized_volatility_pct",
        "volatility_pct",
        "atr_pct",
        "recent_realized_volatility_pct",
    ):
        value = _positive_decimal(context.get(key))
        if value is not None:
            return value, key, None
    returns = _decimal_series(
        context.get("daily_return_pcts")
        or context.get("return_pcts")
        or context.get("candidate_return_pcts")
    )
    returns_source = "daily_return_pcts"
    if not returns:
        fraction_returns = _decimal_series(
            context.get("daily_returns")
            or context.get("returns")
            or context.get("candidate_returns")
        )
        if fraction_returns:
            returns = [value * Decimal("100") for value in fraction_returns]
            returns_source = "daily_returns"
    lookback = int(getattr(settings, "vol_corr_volatility_lookback_days", 20))
    returns = returns[-lookback:]
    min_observations = int(getattr(settings, "vol_corr_min_return_observations", 10))
    if len(returns) < min_observations:
        return None, returns_source if returns else None, len(returns)
    volatility = _sample_stdev(returns)
    if volatility is None:
        return None, returns_source, len(returns)
    return volatility, returns_source, len(returns)


def _volatility_multiplier(*, settings: Settings, volatility_pct: Decimal | None) -> tuple[Decimal, str]:
    if volatility_pct is None:
        return Decimal("1"), "missing_volatility"
    very_high = _decimal_setting(settings, "vol_corr_very_high_volatility_threshold_pct", Decimal("7.0"))
    high = _decimal_setting(settings, "vol_corr_high_volatility_threshold_pct", Decimal("4.0"))
    if volatility_pct >= very_high:
        return (
            _decimal_setting(settings, "vol_corr_very_high_volatility_multiplier", Decimal("0.50")),
            "very_high_volatility",
        )
    if volatility_pct >= high:
        return (
            _decimal_setting(settings, "vol_corr_high_volatility_multiplier", Decimal("0.75")),
            "high_volatility",
        )
    return Decimal("1"), "normal_volatility"


def _resolve_correlation(
    *,
    settings: Settings,
    context: dict[str, object] | None,
) -> tuple[Decimal | None, str | None, str | None, int | None]:
    if not isinstance(context, dict):
        return None, None, None, None
    direct = _first_decimal_value(
        context,
        keys=("max_holding_correlation", "holding_correlation", "correlation"),
    )
    if direct is not None:
        return direct, _text_value(context.get("correlation_symbol")), "direct", None

    candidate_returns = _returns_from_context(context)
    if not candidate_returns:
        return None, None, None, None
    holding_returns = context.get("holding_return_pcts_by_symbol") or context.get("holding_returns_by_symbol")
    if not isinstance(holding_returns, dict):
        return None, None, "holding_returns_by_symbol", len(candidate_returns)

    min_observations = int(getattr(settings, "vol_corr_min_return_observations", 10))
    best_correlation: Decimal | None = None
    best_symbol: str | None = None
    best_observations: int | None = None
    for raw_symbol, raw_returns in holding_returns.items():
        series = _decimal_series(raw_returns)
        if not series:
            continue
        if "holding_returns_by_symbol" in context and "holding_return_pcts_by_symbol" not in context:
            series = [value * Decimal("100") for value in series]
        overlap = min(len(candidate_returns), len(series))
        if overlap < min_observations:
            continue
        correlation = _sample_correlation(candidate_returns[-overlap:], series[-overlap:])
        if correlation is None:
            continue
        if best_correlation is None or correlation > best_correlation:
            best_correlation = correlation
            best_symbol = str(raw_symbol)
            best_observations = overlap
    return best_correlation, best_symbol, "computed_returns", best_observations


def _correlation_multiplier(*, settings: Settings, correlation: Decimal | None) -> tuple[Decimal, str]:
    if correlation is None:
        return Decimal("1"), "missing_correlation"
    threshold = _decimal_setting(settings, "vol_corr_high_correlation_threshold", Decimal("0.75"))
    if correlation >= threshold:
        return (
            _decimal_setting(settings, "vol_corr_high_correlation_multiplier", Decimal("0.80")),
            "high_correlation",
        )
    return Decimal("1"), "normal_correlation"


def _has_correlation_holdings(context: dict[str, object] | None) -> bool:
    if not isinstance(context, dict):
        return False
    holdings = context.get("holding_symbols") or context.get("holdings")
    if isinstance(holdings, list) and holdings:
        return True
    returns = context.get("holding_return_pcts_by_symbol") or context.get("holding_returns_by_symbol")
    return isinstance(returns, dict) and bool(returns)


def _returns_from_context(context: dict[str, object]) -> list[Decimal]:
    returns = _decimal_series(
        context.get("daily_return_pcts")
        or context.get("return_pcts")
        or context.get("candidate_return_pcts")
    )
    if returns:
        return returns
    fraction_returns = _decimal_series(
        context.get("daily_returns")
        or context.get("returns")
        or context.get("candidate_returns")
    )
    return [value * Decimal("100") for value in fraction_returns]


def _decimal_series(value: object) -> list[Decimal]:
    if not isinstance(value, list):
        return []
    series: list[Decimal] = []
    for item in value:
        parsed = _decimal_value(item)
        if parsed is not None:
            series.append(parsed)
    return series


def _sample_stdev(values: list[Decimal]) -> Decimal | None:
    if len(values) < 2:
        return None
    mean = sum(values) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values) - 1)
    return Decimal(str(sqrt(float(variance)))).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _sample_correlation(left: list[Decimal], right: list[Decimal]) -> Decimal | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = sum(left) / Decimal(len(left))
    right_mean = sum(right) / Decimal(len(right))
    covariance = sum((left_item - left_mean) * (right_item - right_mean) for left_item, right_item in zip(left, right))
    left_variance = sum((left_item - left_mean) ** 2 for left_item in left)
    right_variance = sum((right_item - right_mean) ** 2 for right_item in right)
    denominator = sqrt(float(left_variance * right_variance))
    if denominator == 0:
        return None
    return Decimal(str(float(covariance) / denominator)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


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


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _first_decimal_value(payload: dict[str, object], *, keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        if key not in payload:
            continue
        parsed = _decimal_value(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _positive_decimal(value: Any) -> Decimal | None:
    parsed = _decimal_value(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _text_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _reject(
    reason_code: str,
    message: str,
    **details: Any,
) -> RiskSizingRejection:
    return RiskSizingRejection(
        reason_code=reason_code,
        message=message,
        details={key: _text(value) if isinstance(value, Decimal) or isinstance(value, int) or value is None else value for key, value in details.items()},
    )


def _text(value: Decimal | int | None) -> str | int | None:
    if isinstance(value, Decimal):
        return f"{value:.4f}"
    return value
