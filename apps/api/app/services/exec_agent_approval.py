from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import (
    Decimal,
    DecimalException,
    InvalidOperation,
    ROUND_CEILING,
    ROUND_DOWN,
)

from app.services.execution_semantics import canonical_execution_asset_class
from app.services.openclaw_runtime_contract import (
    persisted_runtime_contract_proof_shape_is_valid,
)


_ROUND_TRIP_COST_KEYS = frozenset(
    {
        "estimated_round_trip_cost_bps",
        "round_trip_cost_bps",
        "fee_slippage_bps",
        "total_cost_bps",
        "transaction_cost_bps",
    }
)
_FEE_COST_KEYS = frozenset(
    {"fee_bps", "commission_bps", "tax_bps", "round_trip_fee_bps"}
)
_SLIPPAGE_COST_KEYS = frozenset(
    {
        "expected_slippage_bps",
        "slippage_bps",
        "spread_bps",
        "bid_ask_spread_bps",
        "orderbook_spread_bps",
    }
)

DELEGATED_LIQUIDITY_GATE_CODE = (
    "short_term_liquidity_floor_block_delegated_to_exec_watch"
)
DELEGATED_REWARD_RISK_GATE_CODE = (
    "short_term_reward_risk_ratio_block_delegated_to_exec_watch"
)
DELEGATED_LIVE_COST_GATE_CODE = (
    "transaction_cost_evidence_pending_live_delegated_to_exec_watch"
)
SHORT_TERM_LIVE_MIN_TURNOVER_KRW = Decimal("3000000000")
SHORT_TERM_LIVE_MIN_NET_REWARD_RISK = Decimal("1.80")
DELEGATED_BUY_GATE_CODES = frozenset(
    {
        DELEGATED_LIQUIDITY_GATE_CODE,
        DELEGATED_REWARD_RISK_GATE_CODE,
        DELEGATED_LIVE_COST_GATE_CODE,
    }
)
_DELEGATED_GATE_POLICY_VERSION = "core_delegated_gate_policy_v1"
_DELEGATED_GATE_KEYS = frozenset(
    {
        "code",
        "observed_value",
        "field_path",
        "comparator",
        "threshold",
        "policy_version",
        "observed_at",
        "recheck_source",
        "expires_at",
    }
)
_DELEGATED_GATE_SPECS: dict[str, dict[str, object]] = {
    DELEGATED_LIQUIDITY_GATE_CODE: {
        "field_path": "risk_context.validation.short_term_entry_quality.raw_trading_value_krw",
        "comparator": "gte",
        "threshold": "3000000000",
        "recheck_source": "exec_trigger_time_market_liquidity",
        "observed_value_required": True,
    },
    DELEGATED_REWARD_RISK_GATE_CODE: {
        "field_path": "risk_context.validation.short_term_entry_quality.reward_risk_ratio",
        "comparator": "gte",
        "threshold": "1.80",
        "recheck_source": "exec_trigger_time_price_geometry_and_cost_model",
        "observed_value_required": True,
    },
    DELEGATED_LIVE_COST_GATE_CODE: {
        "field_path": "risk_context.execution_feasibility_context.expected_slippage_bps",
        "comparator": "finite_non_negative_with_basis",
        "threshold": "0",
        "recheck_source": "exec_trigger_time_quote_and_cost_model",
        "observed_value_required": False,
    },
}
_EXPECTED_SLIPPAGE_BASIS = "quote_spread_bps"
_COST_EVIDENCE_SOURCE_KEY = "cost_evidence_source"
_LIVE_QUOTE_SPREAD_REQUIRED_KEY = "live_quote_spread_required"
_LIVE_QUOTE_SPREAD_PRESENT_KEY = "live_quote_spread_present"
_DOMESTIC_KIS_EXCHANGES = frozenset({"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"})
_US_EQUITY_EXCHANGES = frozenset({"NASD", "NYSE", "AMEX", "US"})


@dataclass(frozen=True)
class BuyEconomicsAssessment:
    reason: str | None
    minimum_reward_risk: Decimal
    gross_reward_risk: Decimal | None
    net_reward_risk: Decimal | None
    gross_expected_edge_bps: Decimal | None
    net_expected_edge_bps: Decimal | None
    estimated_round_trip_cost_bps: Decimal | None
    cost_evidence_source: str | None


def configured_execution_cost_evidence(
    *,
    settings: object,
    provider: object,
    asset_class: object,
    exchange: object,
    live_quote_spread_bps: object | None,
    require_live_quote_spread: bool,
    live_quote_spread_basis: object | None = _EXPECTED_SLIPPAGE_BASIS,
) -> dict[str, object]:
    """Build server-owned cost evidence for one provider/market route.

    Persisted plan values and agent output are deliberately not inputs.  The
    non-spread component is an operator-controlled Settings policy and the only
    variable component is an explicitly selected executable quote spread.
    """

    normalized_provider = str(provider or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().upper()
    try:
        normalized_asset_class = canonical_execution_asset_class(
            asset_class,
            provider=normalized_provider,
            exchange=normalized_exchange,
        )
    except ValueError:
        normalized_asset_class = str(asset_class or "").strip().lower()
    setting_name: str | None = None
    if (
        normalized_provider == "kis"
        and normalized_asset_class == "stock"
        and normalized_exchange in _DOMESTIC_KIS_EXCHANGES
    ):
        setting_name = "kis_krx_round_trip_non_spread_cost_bps"
    elif (
        normalized_provider == "kis"
        and normalized_asset_class == "us_equity"
        and normalized_exchange in _US_EQUITY_EXCHANGES
    ):
        setting_name = "kis_us_round_trip_non_spread_cost_bps"
    elif (
        normalized_provider == "alpaca"
        and normalized_asset_class == "us_equity"
        and normalized_exchange in _US_EQUITY_EXCHANGES
    ):
        setting_name = "alpaca_us_equity_round_trip_non_spread_cost_bps"

    context: dict[str, object] = {}
    if setting_name is not None:
        raw_non_spread_cost = getattr(settings, setting_name, None)
        if raw_non_spread_cost is not None:
            context["round_trip_fee_bps"] = raw_non_spread_cost
            context[_COST_EVIDENCE_SOURCE_KEY] = f"settings:{setting_name}"

    policy_is_configured = _COST_EVIDENCE_SOURCE_KEY in context
    if require_live_quote_spread and policy_is_configured:
        context[_LIVE_QUOTE_SPREAD_REQUIRED_KEY] = True
        context[_LIVE_QUOTE_SPREAD_PRESENT_KEY] = live_quote_spread_bps is not None
    if policy_is_configured and live_quote_spread_bps is not None:
        context["expected_slippage_bps"] = live_quote_spread_bps
        context["expected_slippage_basis"] = live_quote_spread_basis
        source = context.get(_COST_EVIDENCE_SOURCE_KEY)
        if isinstance(source, str):
            context[_COST_EVIDENCE_SOURCE_KEY] = (
                f"{source}+live_quote_spread_bps"
            )
    return {"live_execution_cost_context": context}


def assess_buy_economics(
    *,
    reference_price: Decimal | None,
    target_price: Decimal | None,
    stop_price: Decimal | None,
    minimum_reward_risk: Decimal,
    evidence: object,
    require_cost_evidence: bool,
) -> BuyEconomicsAssessment:
    """Apply an agent-independent reward/risk and supplied-cost floor."""

    invalid_geometry = bool(
        reference_price is None
        or target_price is None
        or stop_price is None
        or not reference_price.is_finite()
        or not target_price.is_finite()
        or not stop_price.is_finite()
        or reference_price <= 0
        or target_price <= reference_price
        or stop_price <= 0
        or stop_price >= reference_price
    )
    if invalid_geometry:
        return BuyEconomicsAssessment(
            reason="invalid_target_stop_geometry",
            minimum_reward_risk=minimum_reward_risk,
            gross_reward_risk=None,
            net_reward_risk=None,
            gross_expected_edge_bps=None,
            net_expected_edge_bps=None,
            estimated_round_trip_cost_bps=None,
            cost_evidence_source=None,
        )

    assert reference_price is not None
    assert target_price is not None
    assert stop_price is not None
    try:
        reward = target_price - reference_price
        risk = reference_price - stop_price
        gross_reward_risk = reward / risk
        gross_edge_bps = reward / reference_price * Decimal("10000")
    except DecimalException:
        return BuyEconomicsAssessment(
            reason="economic_value_out_of_range",
            minimum_reward_risk=minimum_reward_risk,
            gross_reward_risk=None,
            net_reward_risk=None,
            gross_expected_edge_bps=None,
            net_expected_edge_bps=None,
            estimated_round_trip_cost_bps=None,
            cost_evidence_source=None,
        )
    cost_bps, cost_source, invalid_cost = _supplied_round_trip_cost_bps(evidence)
    reason: str | None = None
    if gross_reward_risk < minimum_reward_risk:
        reason = "reward_risk_below_minimum"
    elif invalid_cost:
        reason = "transaction_cost_evidence_invalid"
    elif cost_bps is None and require_cost_evidence:
        reason = "transaction_cost_evidence_missing"

    effective_cost_bps = cost_bps or Decimal("0")
    try:
        net_edge_bps = gross_edge_bps - effective_cost_bps
        round_trip_cost = reference_price * effective_cost_bps / Decimal("10000")
        net_reward = reward - round_trip_cost
        # Costs reduce target-side proceeds and deepen a stop-side loss.  Charging
        # the same supplied round-trip estimate to both outcomes is conservative
        # and prevents a thin gross ratio from passing as profitable after costs.
        net_reward_risk = net_reward / (risk + round_trip_cost)
    except DecimalException:
        reason = reason or "economic_value_out_of_range"
        net_edge_bps = None
        net_reward_risk = None
    if reason is None and net_edge_bps is not None and net_edge_bps <= 0:
        reason = "net_expected_edge_nonpositive"
    elif reason is None and net_reward_risk is not None and net_reward_risk < minimum_reward_risk:
        reason = "net_reward_risk_below_minimum"
    gross_reward_risk_rounded = _four_places(gross_reward_risk)
    net_reward_risk_rounded = _four_places(net_reward_risk)
    gross_edge_bps_rounded = _four_places(gross_edge_bps)
    net_edge_bps_rounded = _four_places(net_edge_bps)
    cost_bps_rounded = _four_places(cost_bps)
    if reason is None and any(
        value is None
        for value in (
            gross_reward_risk_rounded,
            net_reward_risk_rounded,
            gross_edge_bps_rounded,
            net_edge_bps_rounded,
        )
    ):
        reason = "economic_value_out_of_range"
    return BuyEconomicsAssessment(
        reason=reason,
        minimum_reward_risk=minimum_reward_risk,
        gross_reward_risk=gross_reward_risk_rounded,
        net_reward_risk=net_reward_risk_rounded,
        gross_expected_edge_bps=gross_edge_bps_rounded,
        net_expected_edge_bps=net_edge_bps_rounded,
        estimated_round_trip_cost_bps=cost_bps_rounded,
        cost_evidence_source=cost_source,
    )


def maximum_entry_price_for_buy_economics(
    *,
    target_price: Decimal | None,
    upstream_stop_price: Decimal | None,
    stop_loss_pct: Decimal,
    minimum_reward_risk: Decimal,
    evidence: object,
    require_cost_evidence: bool,
) -> Decimal | None:
    """Return the highest entry that can satisfy the server cost/RR contract.

    The stop is the tighter of the upstream stop and the configured percentage
    stop.  This is the same effective-stop rule used for deterministic sizing
    and final broker submission.
    """

    values = (
        target_price,
        upstream_stop_price,
        stop_loss_pct,
        minimum_reward_risk,
    )
    if any(
        value is None or not value.is_finite() or value <= 0
        for value in values
    ):
        return None
    assert target_price is not None
    assert upstream_stop_price is not None
    if (
        upstream_stop_price >= target_price
        or stop_loss_pct >= 1
    ):
        return None

    cost_bps, _cost_source, invalid_cost = _supplied_round_trip_cost_bps(
        evidence
    )
    if invalid_cost or (cost_bps is None and require_cost_evidence):
        return None
    cost_rate = (cost_bps or Decimal("0")) / Decimal("10000")
    try:
        stop_transition_price = upstream_stop_price / (
            Decimal("1") - stop_loss_pct
        )
        upstream_stop_cap = (
            target_price
            + minimum_reward_risk * upstream_stop_price
        ) / (
            (Decimal("1") + minimum_reward_risk)
            * (Decimal("1") + cost_rate)
        )
        percentage_stop_cap = target_price / (
            Decimal("1")
            + cost_rate
            + minimum_reward_risk * (stop_loss_pct + cost_rate)
        )
        cap = (
            percentage_stop_cap
            if percentage_stop_cap >= stop_transition_price
            else upstream_stop_cap
        )
    except DecimalException:
        return None
    cap = _four_places(cap)
    if cap is None or cap <= upstream_stop_price or cap >= target_price:
        return None
    price_tick = Decimal("0.0001")
    rounding_adjustment_bound = (
        minimum_reward_risk
        / (
            Decimal("1")
            + cost_rate * (Decimal("1") + minimum_reward_risk)
        )
    ).to_integral_value(rounding=ROUND_CEILING)
    max_adjustment_ticks = min(32, max(1, int(rounding_adjustment_bound)))
    for adjustment_ticks in range(max_adjustment_ticks + 1):
        candidate = cap - price_tick * adjustment_ticks
        if candidate <= upstream_stop_price:
            return None
        try:
            configured_stop = (
                candidate * (Decimal("1") - stop_loss_pct)
            ).quantize(price_tick, rounding=ROUND_DOWN)
        except DecimalException:
            return None
        assessment = assess_buy_economics(
            reference_price=candidate,
            target_price=target_price,
            stop_price=max(upstream_stop_price, configured_stop),
            minimum_reward_risk=minimum_reward_risk,
            evidence=evidence,
            require_cost_evidence=require_cost_evidence,
        )
        if assessment.reason is None:
            return candidate
        if assessment.reason != "net_reward_risk_below_minimum":
            return None
    return None


def _supplied_round_trip_cost_bps(
    evidence: object,
) -> tuple[Decimal | None, str | None, bool]:
    found: dict[str, list[object]] = {}
    invalid, source_override = _collect_trusted_cost_values(
        evidence=evidence,
        found=found,
    )

    def parsed_values(keys: frozenset[str]) -> list[tuple[str, Decimal]]:
        nonlocal invalid
        values: list[tuple[str, Decimal]] = []
        for key in sorted(keys):
            for raw_value in found.get(key, []):
                parsed = _nonnegative_finite_decimal(raw_value)
                if parsed is None:
                    invalid = True
                    continue
                values.append((key, parsed))
        return values

    full_costs = parsed_values(_ROUND_TRIP_COST_KEYS)
    fee_costs = parsed_values(_FEE_COST_KEYS)
    slippage_costs = parsed_values(_SLIPPAGE_COST_KEYS)
    candidates: list[tuple[str, Decimal]] = list(full_costs)
    if fee_costs or slippage_costs:
        fee_key, fee = max(fee_costs, key=lambda pair: pair[1]) if fee_costs else ("", Decimal("0"))
        slippage_key, slippage = (
            max(slippage_costs, key=lambda pair: pair[1])
            if slippage_costs
            else ("", Decimal("0"))
        )
        component_keys = "+".join(key for key in (fee_key, slippage_key) if key)
        candidates.append((component_keys, fee + slippage))
    if not candidates:
        return None, None, invalid
    source, value = max(candidates, key=lambda pair: pair[1])
    if value < 0:
        return None, source or None, True
    return value, source_override or source or None, invalid


def _collect_trusted_cost_values(
    *,
    evidence: object,
    found: dict[str, list[object]],
) -> tuple[bool, str | None]:
    """Read only explicit server-selected cost contexts, never arbitrary nested agent data."""

    roots = evidence if isinstance(evidence, list) else [evidence]
    invalid = False
    ordinary_contexts: list[dict[str, object]] = []
    live_contexts: list[dict[str, object]] = []
    for root in roots:
        if not isinstance(root, dict):
            continue
        ordinary_contexts.append(root)
        feasibility = root.get("execution_feasibility_context")
        if isinstance(feasibility, dict):
            ordinary_contexts.append(feasibility)
        live_context = root.get("live_execution_cost_context")
        if isinstance(live_context, dict):
            live_contexts.append(live_context)

    contexts = live_contexts or ordinary_contexts
    expected_slippage_values: list[tuple[object, object]] = []
    source_values: list[str] = []
    keys = _ROUND_TRIP_COST_KEYS | _FEE_COST_KEYS | _SLIPPAGE_COST_KEYS
    for context in contexts:
        if (
            context.get(_LIVE_QUOTE_SPREAD_REQUIRED_KEY) is True
            and context.get(_LIVE_QUOTE_SPREAD_PRESENT_KEY) is not True
        ):
            invalid = True
        source_value = context.get(_COST_EVIDENCE_SOURCE_KEY)
        if source_value is not None:
            if (
                not isinstance(source_value, str)
                or not source_value.startswith("settings:")
                or source_value.strip() != source_value
            ):
                invalid = True
            else:
                source_values.append(source_value)
        for key in keys:
            if key not in context:
                continue
            nested = context[key]
            if key == "expected_slippage_bps":
                basis = context.get("expected_slippage_basis")
                expected_slippage_values.append((nested, basis))
                if basis != _EXPECTED_SLIPPAGE_BASIS:
                    invalid = True
                    continue
            found.setdefault(key, []).append(nested)
    if expected_slippage_values and any(
        value != expected_slippage_values[0]
        for value in expected_slippage_values[1:]
    ):
        invalid = True
    if source_values and any(value != source_values[0] for value in source_values[1:]):
        invalid = True
    return invalid, source_values[0] if source_values else None


def _nonnegative_finite_decimal(value: object) -> Decimal | None:
    parsed = _finite_decimal(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def _finite_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _four_places(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    except (InvalidOperation, ValueError):
        return None


def delegated_buy_gate_rejections(
    evidence: object,
    *,
    evaluated_at: datetime | None = None,
    live_market_context: object | None = None,
    economics: BuyEconomicsAssessment | None = None,
    require_live_recheck: bool = False,
) -> list[str]:
    """Validate and, at execution time, recheck Core-delegated buy gates."""

    contexts = _delegated_gate_contexts(evidence)
    if not contexts:
        return []
    if any(context != contexts[0] for context in contexts[1:]):
        return ["delegated_gate_contract_conflict"]
    context = contexts[0]
    raw_codes = context.get("delegated_gate_codes")
    confirmation = context.get("delegated_gate_confirmation_required")
    raw_gates = context.get("delegated_gates")
    if not isinstance(raw_codes, list) or any(
        not isinstance(code, str) or not code or code.strip() != code
        for code in raw_codes
    ):
        return ["delegated_gate_codes_invalid"]
    codes = list(raw_codes)
    if len(set(codes)) != len(codes):
        return ["delegated_gate_codes_invalid"]
    if not codes:
        if confirmation is True or raw_gates not in (None, []):
            return ["delegated_gate_confirmation_invalid"]
        return []
    unknown = sorted(set(codes).difference(DELEGATED_BUY_GATE_CODES))
    if unknown:
        return [f"delegated_gate_unknown:{code}" for code in unknown]
    if confirmation is not True:
        return ["delegated_gate_confirmation_missing"]
    if not isinstance(raw_gates, list) or len(raw_gates) != len(codes):
        return ["delegated_gate_contract_missing"]

    gate_codes: list[str] = []
    now = _aware_utc(evaluated_at or datetime.now(UTC))
    if now is None:
        return ["delegated_gate_evaluation_time_invalid"]
    gates: list[dict[str, object]] = []
    for raw_gate in raw_gates:
        reason, gate = _validated_delegated_gate(raw_gate, evaluated_at=now)
        if reason is not None:
            return [reason]
        assert gate is not None
        gate_codes.append(str(gate["code"]))
        gates.append(gate)
    if gate_codes != codes:
        return ["delegated_gate_contract_mismatch"]
    if not require_live_recheck:
        return []

    failures: list[str] = []
    for gate in gates:
        code = str(gate["code"])
        threshold = _finite_decimal(gate.get("threshold"))
        passed = False
        if code == DELEGATED_LIQUIDITY_GATE_CODE:
            turnover = _trusted_live_market_decimal(
                live_market_context,
                field="turnover",
                evaluated_at=now,
            )
            passed = bool(
                turnover is not None
                and threshold is not None
                and turnover >= threshold
            )
        elif code == DELEGATED_REWARD_RISK_GATE_CODE:
            passed = bool(
                economics is not None
                and economics.net_reward_risk is not None
                and threshold is not None
                and economics.net_reward_risk >= threshold
            )
        elif code == DELEGATED_LIVE_COST_GATE_CODE:
            passed = bool(
                economics is not None
                and economics.estimated_round_trip_cost_bps is not None
                and economics.cost_evidence_source is not None
                and economics.cost_evidence_source.startswith("settings:")
                and economics.cost_evidence_source.endswith(
                    "+live_quote_spread_bps"
                )
            )
        if not passed:
            failures.append(f"delegated_gate_failed:{code}")
    return failures


def trusted_live_market_decimal(
    context: object,
    *,
    field: str,
    evaluated_at: datetime,
) -> Decimal | None:
    normalized_evaluated_at = _aware_utc(evaluated_at)
    if normalized_evaluated_at is None:
        return None
    return _trusted_live_market_decimal(
        context,
        field=field,
        evaluated_at=normalized_evaluated_at,
    )


def _delegated_gate_contexts(value: object) -> list[dict[str, object]]:
    roots = value if isinstance(value, list) else [value]
    contexts: list[dict[str, object]] = []
    for root in roots:
        if not isinstance(root, dict):
            continue
        if any(
            key in root
            for key in (
                "delegated_gate_codes",
                "delegated_gate_confirmation_required",
                "delegated_gates",
            )
        ):
            contexts.append(
                {
                    "delegated_gate_codes": root.get("delegated_gate_codes"),
                    "delegated_gate_confirmation_required": root.get(
                        "delegated_gate_confirmation_required"
                    ),
                    "delegated_gates": root.get("delegated_gates"),
                }
            )
    return contexts


def _validated_delegated_gate(
    value: object,
    *,
    evaluated_at: datetime,
) -> tuple[str | None, dict[str, object] | None]:
    if not isinstance(value, dict) or frozenset(value) != _DELEGATED_GATE_KEYS:
        return "delegated_gate_schema_invalid", None
    code = value.get("code")
    if not isinstance(code, str) or code not in _DELEGATED_GATE_SPECS:
        return f"delegated_gate_unknown:{code}", None
    spec = _DELEGATED_GATE_SPECS[code]
    for key in ("field_path", "comparator", "threshold", "recheck_source"):
        if value.get(key) != spec[key]:
            return f"delegated_gate_contract_mismatch:{code}:{key}", None
    if value.get("policy_version") != _DELEGATED_GATE_POLICY_VERSION:
        return f"delegated_gate_contract_mismatch:{code}:policy_version", None
    observed_value = value.get("observed_value")
    observed_value_required = spec["observed_value_required"] is True
    parsed_observed_value = _finite_decimal(observed_value)
    if observed_value_required:
        if parsed_observed_value is None or parsed_observed_value < 0:
            return f"delegated_gate_observed_value_invalid:{code}", None
        threshold = _finite_decimal(spec["threshold"])
        if threshold is None or parsed_observed_value >= threshold:
            return f"delegated_gate_observed_value_inconsistent:{code}", None
    elif observed_value is not None:
        return f"delegated_gate_observed_value_invalid:{code}", None

    observed_at = _strict_aware_iso_datetime(value.get("observed_at"))
    expires_at = _strict_aware_iso_datetime(value.get("expires_at"))
    if observed_at is None:
        return f"delegated_gate_observed_at_invalid:{code}", None
    if expires_at is None:
        return f"delegated_gate_expires_at_invalid:{code}", None
    if (
        expires_at.utcoffset() != timedelta(hours=9)
        or expires_at.timetz().replace(tzinfo=None) != time(hour=15, minute=30)
    ):
        return f"delegated_gate_expires_at_invalid:{code}", None
    if observed_at > expires_at or observed_at > evaluated_at:
        return f"delegated_gate_time_order_invalid:{code}", None
    if evaluated_at > expires_at.astimezone(UTC):
        return f"delegated_gate_expired:{code}", None
    return None, value


def _trusted_live_market_decimal(
    context: object,
    *,
    field: str,
    evaluated_at: datetime,
) -> Decimal | None:
    if not isinstance(context, dict):
        return None
    if context.get("live_execution_context_available") is not True:
        return None
    value = _finite_decimal(context.get(field))
    freshness = context.get("field_freshness")
    field_freshness = freshness.get(field) if isinstance(freshness, dict) else None
    if value is None or not isinstance(field_freshness, dict):
        return None
    if field_freshness.get("valid") is not True:
        return None
    source = field_freshness.get("source")
    if source not in {
        "kis_exchange_orderbook_time",
        "kis_exchange_intraday_minute_time",
        "kis_overseas_exchange_bar_time",
        "alpaca_exchange_trade_time",
        "alpaca_exchange_quote_time",
        "broker_realtime_exchange_time",
    }:
        return None
    observed_at = _strict_aware_iso_datetime(field_freshness.get("observed_at"))
    max_age = _nonnegative_finite_decimal(field_freshness.get("max_age_seconds"))
    max_future_skew = _nonnegative_finite_decimal(
        field_freshness.get("max_future_skew_seconds")
    )
    if observed_at is None or max_age is None or max_future_skew is None:
        return None
    age = Decimal(str((evaluated_at - observed_at).total_seconds()))
    if age > max_age or age < -max_future_skew:
        return None
    return value


def _strict_aware_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value or value.strip() != value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _aware_utc(value: datetime) -> datetime | None:
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC)


def load_strict_json_object(text: str) -> dict[str, object]:
    """Load one unambiguous JSON object without coercion or salvage."""

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key: {key}")
            result[key] = value
        return result

    def reject_nonfinite_number(value: str) -> object:
        raise ValueError(f"non-finite JSON number is not allowed: {value}")

    parsed = json.loads(
        text,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_nonfinite_number,
    )
    if not isinstance(parsed, dict):
        raise ValueError("exec-agent response JSON must be an object")
    _validate_finite_json_numbers(parsed)
    return parsed


def _validate_finite_json_numbers(value: object) -> None:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite JSON number is not allowed")
        return
    if isinstance(value, dict):
        for nested in value.values():
            _validate_finite_json_numbers(nested)
        return
    if isinstance(value, list):
        for nested in value:
            _validate_finite_json_numbers(nested)


def is_legacy_unverified_exec_agent_sizing(quantity_policy: object) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    source = str(quantity_policy.get("source") or "").strip()
    if source == "exec_agent_watch_sizing":
        return not persisted_verified_watch_sizing_provenance(quantity_policy)
    if source != "watch_buy_handoff_normalizer":
        return False
    if str(quantity_policy.get("original_policy_type") or "").strip() != "exec_agent_sized":
        return False
    return not persisted_verified_watch_sizing_provenance(quantity_policy)


def persisted_verified_watch_sizing_provenance(
    quantity_policy: dict[str, object],
) -> bool:
    approval_provenance = quantity_policy.get("approval_provenance")
    decision = quantity_policy.get("exec_agent_decision")
    if not (
        isinstance(approval_provenance, dict)
        and set(approval_provenance) == {"kind", "runtime"}
        and approval_provenance.get("kind")
        == "verified_exec_agent_watch_sizing"
        and isinstance(decision, dict)
        and decision == quantity_policy.get("short_term_exec_agent_decision")
        and decision.get("source") == "openclaw_exec_agent_watch_sizing"
        and decision.get("action") == "approve"
        and decision.get("decision") == "approved"
    ):
        return False
    quantity = quantity_policy.get("quantity")
    sizing_price = _finite_decimal(quantity_policy.get("sizing_price"))
    decision_sizing_price = _finite_decimal(decision.get("sizing_price"))
    approved_notional = _finite_decimal(
        quantity_policy.get("approved_notional")
    )
    decision_notional = _finite_decimal(
        decision.get("approved_notional_krw")
    )
    adjustment = quantity_policy.get("sizing_adjustment")
    adjustment_notional = _finite_decimal(
        adjustment.get("final_notional")
        if isinstance(adjustment, dict)
        else None
    )
    if not (
        isinstance(quantity, int)
        and not isinstance(quantity, bool)
        and quantity > 0
        and decision.get("quantity") == quantity
        and decision.get("approved_quantity") == quantity
        and sizing_price is not None
        and sizing_price > 0
        and sizing_price == decision_sizing_price
        and approved_notional is not None
        and approved_notional > 0
        and approved_notional == decision_notional
        and approved_notional == sizing_price * Decimal(quantity)
        and isinstance(adjustment, dict)
        and adjustment.get("final_quantity") == quantity
        and adjustment_notional == approved_notional
    ):
        return False
    runtime = approval_provenance.get("runtime")
    if not (
        isinstance(runtime, dict)
        and runtime == decision.get("runtime_provenance")
        and runtime.get("agent_id_metadata_malformed") is not True
    ):
        return False
    configured_agent_id = _persisted_nonempty_text(
        runtime.get("configured_agent_id")
    )
    raw_actual_agent_id = runtime.get("actual_agent_id")
    actual_agent_id = _persisted_nonempty_text(raw_actual_agent_id)
    if configured_agent_id is None:
        return False
    if actual_agent_id is not None:
        if (
            actual_agent_id != configured_agent_id
            or runtime.get("agent_id") != actual_agent_id
        ):
            return False
        verified_agent_id = actual_agent_id
    else:
        if raw_actual_agent_id is not None and not (
            isinstance(raw_actual_agent_id, str)
            and not raw_actual_agent_id.strip()
        ):
            return False
        verified_agent_id = _persisted_nonempty_text(
            runtime.get("verified_agent_id")
        )
        if not (
            verified_agent_id == configured_agent_id
            and runtime.get("agent_identity_source")
            == "signed_runtime_contract"
            and runtime.get("agent_id") in {None, ""}
        ):
            return False
    configured_model = _persisted_nonempty_text(
        runtime.get("configured_model_hint")
    )
    actual_provider = _persisted_nonempty_text(runtime.get("actual_provider"))
    actual_model = _persisted_nonempty_text(runtime.get("actual_model"))
    actual_model_ref = _persisted_nonempty_text(runtime.get("actual_model_ref"))
    session_id = _persisted_nonempty_text(runtime.get("session_id"))
    requested_session_key = _persisted_nonempty_text(
        runtime.get("requested_session_key")
    )
    request_sha256 = _persisted_nonempty_text(runtime.get("request_sha256"))
    if not (
        configured_model is not None
        and actual_provider is not None
        and actual_model is not None
        and actual_model_ref == f"{actual_provider}/{actual_model}"
        and actual_model_ref == configured_model
        and session_id is not None
        and requested_session_key is not None
        and request_sha256 is not None
    ):
        return False
    return persisted_runtime_contract_proof_shape_is_valid(
        runtime.get("runtime_contract"),
        agent_id=verified_agent_id,
        expected_model_ref=configured_model,
        requested_session_key=requested_session_key,
        reported_session_id=session_id,
        request_sha256=request_sha256,
    )


def _persisted_nonempty_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
