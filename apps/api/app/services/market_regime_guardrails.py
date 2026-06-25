from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from app.core.config import Settings


@dataclass(frozen=True)
class MarketRegimeGuardrailResult:
    action: str
    posture: str
    reason_code: str
    multiplier: Decimal
    cap_notional: Decimal | None
    audit: dict[str, object]

    @property
    def rejected(self) -> bool:
        return self.action == "reject"


def evaluate_market_regime_guardrail(
    *,
    settings: Settings,
    context: dict[str, object] | None,
    market_date: date | str | None = None,
    original_notional: Decimal | None = None,
    original_quantity: int | None = None,
    reference_price: Decimal | None = None,
    account_context: dict[str, object] | None = None,
    now: datetime | None = None,
) -> MarketRegimeGuardrailResult:
    evaluated_at = now or datetime.now(UTC)
    enabled = bool(getattr(settings, "market_regime_guard_enabled", True))
    payload = context if isinstance(context, dict) else {}
    breadth, breadth_source = _extract_market_breadth(payload)
    sector, sector_source = _extract_sector_context(payload)
    market = _text(_first_present(payload, ("market", "exchange", "market_name"))) or _text(
        breadth.get("market") if isinstance(breadth, dict) else None
    ) or "KRX"
    warnings: list[str] = []

    if not enabled:
        audit = _audit_payload(
            settings=settings,
            enabled=False,
            action="allow",
            posture="normal",
            reason_code="market_regime_guard_disabled",
            market=market,
            breadth=breadth,
            breadth_source=breadth_source,
            sector=sector,
            sector_source=sector_source,
            warnings=warnings,
            original_notional=original_notional,
            adjusted_notional=original_notional,
            original_quantity=original_quantity,
            adjusted_quantity=original_quantity,
            multiplier=Decimal("1"),
            cap_notional=None,
            evaluated_at=evaluated_at,
            market_date=market_date,
            account_context=account_context,
        )
        return MarketRegimeGuardrailResult(
            action="allow",
            posture="normal",
            reason_code="market_regime_guard_disabled",
            multiplier=Decimal("1"),
            cap_notional=None,
            audit=audit,
        )

    posture, reason_code = _market_posture(
        settings=settings,
        breadth=breadth,
        market_date=market_date,
        evaluated_at=evaluated_at,
        warnings=warnings,
    )
    sector_action, sector_reason, sector_multiplier = _sector_adjustment(
        settings=settings,
        sector=sector,
        account_context=account_context,
        warnings=warnings,
    )
    action = "allow"
    multiplier = Decimal("1")
    cap_notional = None
    if posture == "risk_off":
        action = "reject"
    elif posture in {"caution", "stale"}:
        action = "downsize"
        multiplier = min(multiplier, Decimal(str(settings.market_regime_caution_sizing_multiplier)))
        cap_notional = _positive_decimal(settings.market_regime_caution_max_notional_krw)
    if sector_action == "reject":
        action = "reject"
        reason_code = sector_reason
    elif sector_action == "downsize":
        if action == "allow":
            action = "downsize"
        multiplier = min(multiplier, sector_multiplier)
        cap_notional = _min_positive(
            cap_notional,
            _positive_decimal(settings.market_regime_sector_weak_max_notional_krw),
        )
        if reason_code in {"market_breadth_normal", "market_breadth_missing_neutral"}:
            reason_code = sector_reason
    adjusted_notional = _adjusted_notional(
        original_notional=original_notional,
        reference_price=reference_price,
        original_quantity=original_quantity,
        multiplier=multiplier,
        cap_notional=cap_notional,
        reject=action == "reject",
    )
    adjusted_quantity = _adjusted_quantity(
        original_quantity=original_quantity,
        reference_price=reference_price,
        adjusted_notional=adjusted_notional,
        reject=action == "reject",
    )
    audit = _audit_payload(
        settings=settings,
        enabled=True,
        action=action,
        posture=posture,
        reason_code=reason_code,
        market=market,
        breadth=breadth,
        breadth_source=breadth_source,
        sector=sector,
        sector_source=sector_source,
        warnings=warnings,
        original_notional=original_notional,
        adjusted_notional=adjusted_notional,
        original_quantity=original_quantity,
        adjusted_quantity=adjusted_quantity,
        multiplier=Decimal("0") if action == "reject" else multiplier,
        cap_notional=cap_notional,
        evaluated_at=evaluated_at,
        market_date=market_date,
        account_context=account_context,
    )
    return MarketRegimeGuardrailResult(
        action=action,
        posture=posture,
        reason_code=reason_code,
        multiplier=Decimal("0") if action == "reject" else multiplier,
        cap_notional=cap_notional,
        audit=audit,
    )


def market_regime_context_from_item_payloads(*payloads: object) -> dict[str, object]:
    merged: dict[str, object] = {}
    for payload in payloads:
        if isinstance(payload, dict):
            _merge_context(merged, payload)
    return merged


def _market_posture(
    *,
    settings: Settings,
    breadth: dict[str, object] | None,
    market_date: date | str | None,
    evaluated_at: datetime,
    warnings: list[str],
) -> tuple[str, str]:
    if breadth is None:
        behavior = str(settings.market_regime_missing_breadth_behavior).lower()
        if settings.runtime_mode == "live" and bool(settings.automation_enabled):
            behavior = str(settings.market_regime_live_missing_breadth_behavior).lower()
        warnings.append("market_breadth_missing")
        if behavior == "caution":
            return "caution", "market_breadth_missing_caution"
        if behavior == "risk_off":
            return "risk_off", "market_breadth_missing_risk_off"
        return "unavailable", "market_breadth_missing_neutral"

    if _breadth_stale(settings=settings, breadth=breadth, market_date=market_date, evaluated_at=evaluated_at):
        warnings.append("market_breadth_stale")
        behavior = str(settings.market_regime_stale_breadth_behavior).lower()
        if behavior == "risk_off":
            return "risk_off", "market_breadth_stale_risk_off"
        return "stale", "market_breadth_stale_caution"

    up_ratio = _ratio_value(_first_present(breadth, ("up_ratio", "advance_ratio", "advancing_ratio")))
    ad_ratio = _decimal_value(
        _first_present(
            breadth,
            ("advance_decline_ratio", "adv_dec_ratio", "advance_to_decline_ratio"),
        )
    )
    up_count = _decimal_value(_first_present(breadth, ("up_count", "advance_count", "advancers")))
    down_count = _decimal_value(_first_present(breadth, ("down_count", "decline_count", "decliners")))
    if ad_ratio is None and up_count is not None and down_count is not None:
        ad_ratio = up_count / down_count if down_count > 0 else Decimal("99")
    if up_ratio is None and up_count is not None and down_count is not None:
        total = up_count + down_count
        if total > 0:
            up_ratio = up_count / total

    if up_ratio is None and ad_ratio is None:
        warnings.append("market_breadth_incomplete")
        return "caution", "market_breadth_incomplete_caution"
    if (
        up_ratio is not None
        and up_ratio < Decimal(str(settings.market_regime_risk_off_up_ratio_threshold))
    ) or (
        ad_ratio is not None
        and ad_ratio < Decimal(str(settings.market_regime_risk_off_advance_decline_threshold))
    ):
        return "risk_off", "market_breadth_risk_off"
    if (
        up_ratio is not None
        and up_ratio < Decimal(str(settings.market_regime_caution_up_ratio_threshold))
    ) or (
        ad_ratio is not None
        and ad_ratio < Decimal(str(settings.market_regime_caution_advance_decline_threshold))
    ):
        return "caution", "market_breadth_caution"
    return "normal", "market_breadth_normal"


def _sector_adjustment(
    *,
    settings: Settings,
    sector: dict[str, object] | None,
    account_context: dict[str, object] | None,
    warnings: list[str],
) -> tuple[str, str, Decimal]:
    if sector is None:
        warnings.append("sector_context_missing")
        return "allow", "sector_context_missing_warning", Decimal("1")
    exposure_count = _sector_exposure_count(sector=sector, account_context=account_context)
    weak = _sector_is_weak(settings=settings, sector=sector)
    if exposure_count > 0:
        warnings.append("same_sector_exposure_present")
    if not weak and exposure_count <= 0:
        return "allow", "sector_context_neutral", Decimal("1")
    behavior = str(settings.market_regime_sector_weak_behavior).lower()
    if weak and behavior == "reject":
        return "reject", "sector_weakness_reject", Decimal("0")
    return (
        "downsize",
        "sector_weakness_or_crowding_downsize",
        Decimal(str(settings.market_regime_sector_weak_multiplier)),
    )


def _sector_is_weak(*, settings: Settings, sector: dict[str, object]) -> bool:
    state = _text(_first_present(sector, ("relative_state", "sector_relative_state", "state", "trend_state")))
    if state is not None and any(token in state.lower() for token in ("weak", "lag", "underperform", "bear")):
        return True
    percentile = _decimal_value(_first_present(sector, ("relative_percentile", "sector_percentile", "percentile")))
    if percentile is not None and percentile <= Decimal(str(settings.market_regime_sector_weak_percentile_threshold)):
        return True
    rel_return = _decimal_value(
        _first_present(sector, ("relative_return_pct", "sector_relative_return_pct", "market_relative_return_pct"))
    )
    if rel_return is not None and rel_return <= Decimal(str(settings.market_regime_sector_weak_relative_return_pct)):
        return True
    return False


def _extract_market_breadth(payload: dict[str, object]) -> tuple[dict[str, object] | None, str | None]:
    for path in (
        ("market_breadth",),
        ("market_context", "market_breadth"),
        ("decider_handoff_context", "market_breadth"),
        ("decider_handoff_context", "decider_input_context", "market_breadth"),
        ("decider_handoff_context", "decider_input_context", "request_payload", "market_breadth"),
        ("pre_decider_enrichment_context", "market_breadth"),
        ("decider_handoff_context", "decider_input_context", "pre_decider_enrichment_context", "market_breadth"),
    ):
        value = _nested(payload, path)
        if isinstance(value, dict):
            return value, ".".join(path)
    return None, None


def _extract_sector_context(payload: dict[str, object]) -> tuple[dict[str, object] | None, str | None]:
    for path in (
        ("sector_context",),
        ("sector_peer_context",),
        ("sector_peers_context",),
        ("market_relative_context",),
        ("decider_flow_short_sector_context", "flow_short_sector_context", "sector_peers"),
        ("decider_handoff_context", "sector_peer_context"),
        ("decider_handoff_context", "decider_input_context", "sector_peer_context"),
        ("decider_handoff_context", "decider_input_context", "request_payload", "sector_peer_context"),
        ("pre_decider_enrichment_context", "sector_peer_context"),
        ("decider_handoff_context", "decider_input_context", "pre_decider_enrichment_context", "sector_peer_context"),
    ):
        value = _nested(payload, path)
        if isinstance(value, dict):
            return value, ".".join(path)
    return None, None


def _merge_context(target: dict[str, object], payload: dict[str, object]) -> None:
    for key in (
        "market",
        "exchange",
        "market_breadth",
        "market_context",
        "sector_context",
        "sector_peer_context",
        "sector_peers_context",
        "market_relative_context",
        "pre_decider_enrichment_context",
        "decider_handoff_context",
        "decider_flow_short_sector_context",
    ):
        if key in payload and key not in target:
            target[key] = payload[key]


def _breadth_stale(
    *,
    settings: Settings,
    breadth: dict[str, object],
    market_date: date | str | None,
    evaluated_at: datetime,
) -> bool:
    observed_date = _date_value(_first_present(breadth, ("market_date", "date", "as_of_date", "trading_date")))
    expected_date = _date_value(market_date) or evaluated_at.date()
    if observed_date is not None and observed_date < expected_date:
        return True
    observed_at = _datetime_value(_first_present(breadth, ("observed_at", "as_of", "created_at", "updated_at")))
    if observed_at is None:
        return False
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    max_age = int(settings.market_regime_breadth_max_age_seconds)
    return (evaluated_at - observed_at.astimezone(UTC)).total_seconds() > max_age


def _audit_payload(
    *,
    settings: Settings,
    enabled: bool,
    action: str,
    posture: str,
    reason_code: str,
    market: str,
    breadth: dict[str, object] | None,
    breadth_source: str | None,
    sector: dict[str, object] | None,
    sector_source: str | None,
    warnings: list[str],
    original_notional: Decimal | None,
    adjusted_notional: Decimal | None,
    original_quantity: int | None,
    adjusted_quantity: int | None,
    multiplier: Decimal,
    cap_notional: Decimal | None,
    evaluated_at: datetime,
    market_date: date | str | None,
    account_context: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "schema_version": "market_regime_guardrail_v1",
        "enabled": enabled,
        "market_regime_posture": posture,
        "market_regime_reason_code": reason_code,
        "market": market,
        "market_date": market_date.isoformat() if isinstance(market_date, date) else market_date,
        "action": action,
        "multiplier": _decimal_text(multiplier),
        "cap_notional_krw": _decimal_text(cap_notional),
        "original_notional_krw": _decimal_text(original_notional),
        "adjusted_notional_krw": _decimal_text(adjusted_notional),
        "original_quantity": original_quantity,
        "adjusted_quantity": adjusted_quantity,
        "breadth": _breadth_audit(breadth),
        "breadth_source": breadth_source,
        "breadth_freshness": _freshness_audit(breadth),
        "sector": _sector_audit(sector, account_context=account_context),
        "sector_source": sector_source,
        "warnings": warnings,
        "settings": {
            "missing_breadth_behavior": settings.market_regime_missing_breadth_behavior,
            "live_missing_breadth_behavior": settings.market_regime_live_missing_breadth_behavior,
            "stale_breadth_behavior": settings.market_regime_stale_breadth_behavior,
            "caution_multiplier": _decimal_text(settings.market_regime_caution_sizing_multiplier),
            "sector_weak_behavior": settings.market_regime_sector_weak_behavior,
            "sector_weak_multiplier": _decimal_text(settings.market_regime_sector_weak_multiplier),
        },
        "evaluated_at": evaluated_at.isoformat(),
    }


def _breadth_audit(breadth: dict[str, object] | None) -> dict[str, object] | None:
    if breadth is None:
        return None
    up_count = _decimal_value(_first_present(breadth, ("up_count", "advance_count", "advancers")))
    down_count = _decimal_value(_first_present(breadth, ("down_count", "decline_count", "decliners")))
    flat_count = _decimal_value(_first_present(breadth, ("flat_count", "unchanged_count", "flats")))
    symbol_count = _decimal_value(_first_present(breadth, ("market_symbol_count", "symbol_count", "total_count")))
    up_ratio = _ratio_value(_first_present(breadth, ("up_ratio", "advance_ratio", "advancing_ratio")))
    ad_ratio = _decimal_value(
        _first_present(breadth, ("advance_decline_ratio", "adv_dec_ratio", "advance_to_decline_ratio"))
    )
    if symbol_count is None:
        parts = [value for value in (up_count, down_count, flat_count) if value is not None]
        symbol_count = sum(parts, Decimal("0")) if parts else None
    if up_ratio is None and up_count is not None and symbol_count is not None and symbol_count > 0:
        up_ratio = up_count / symbol_count
    if ad_ratio is None and up_count is not None and down_count is not None:
        ad_ratio = up_count / down_count if down_count > 0 else Decimal("99")
    return {
        "market_symbol_count": _int_or_text(symbol_count),
        "up_count": _int_or_text(up_count),
        "down_count": _int_or_text(down_count),
        "flat_count": _int_or_text(flat_count),
        "up_ratio": _decimal_text(up_ratio),
        "advance_decline_ratio": _decimal_text(ad_ratio),
    }


def _freshness_audit(breadth: dict[str, object] | None) -> dict[str, object] | None:
    if breadth is None:
        return None
    return {
        "source": _text(_first_present(breadth, ("source", "provider"))),
        "market_date": _text(_first_present(breadth, ("market_date", "date", "as_of_date", "trading_date"))),
        "observed_at": _text(_first_present(breadth, ("observed_at", "as_of", "created_at", "updated_at"))),
    }


def _sector_audit(sector: dict[str, object] | None, *, account_context: dict[str, object] | None) -> dict[str, object] | None:
    if sector is None:
        return None
    exposure_count = _sector_exposure_count(sector=sector, account_context=account_context)
    return {
        "sector_name": _text(_first_present(sector, ("sector_name", "name", "category", "industry"))),
        "relative_state": _text(_first_present(sector, ("relative_state", "sector_relative_state", "state", "trend_state"))),
        "relative_percentile": _decimal_text(
            _decimal_value(_first_present(sector, ("relative_percentile", "sector_percentile", "percentile")))
        ),
        "relative_return_pct": _decimal_text(
            _decimal_value(
                _first_present(sector, ("relative_return_pct", "sector_relative_return_pct", "market_relative_return_pct"))
            )
        ),
        "same_sector_exposure_count": exposure_count,
    }


def _sector_exposure_count(*, sector: dict[str, object], account_context: dict[str, object] | None) -> int:
    direct = _decimal_value(
        _first_present(
            sector,
            (
                "same_sector_exposure_count",
                "current_exposure_count",
                "open_buy_count",
                "same_sector_open_buy_count",
            ),
        )
    )
    if direct is not None:
        return max(0, int(direct))
    if not isinstance(account_context, dict):
        return 0
    sector_name = _text(_first_present(sector, ("sector_name", "name", "category", "industry")))
    if sector_name is None:
        return 0
    count = 0
    for key in ("holdings", "open_orders"):
        rows = account_context.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, dict) and _text(row.get("sector_name") or row.get("sector")) == sector_name:
                count += 1
    return count


def _adjusted_notional(
    *,
    original_notional: Decimal | None,
    reference_price: Decimal | None,
    original_quantity: int | None,
    multiplier: Decimal,
    cap_notional: Decimal | None,
    reject: bool,
) -> Decimal | None:
    if reject:
        return Decimal("0")
    base = original_notional
    if base is None and reference_price is not None and original_quantity is not None:
        base = reference_price * Decimal(original_quantity)
    if base is None:
        return None
    adjusted = (base * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if cap_notional is not None:
        adjusted = min(adjusted, cap_notional)
    return adjusted


def _adjusted_quantity(
    *,
    original_quantity: int | None,
    reference_price: Decimal | None,
    adjusted_notional: Decimal | None,
    reject: bool,
) -> int | None:
    if reject:
        return 0
    if original_quantity is None or reference_price is None or reference_price <= 0 or adjusted_notional is None:
        return original_quantity
    return min(original_quantity, int((adjusted_notional / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN)))


def _nested(payload: dict[str, object], path: tuple[str, ...]) -> object:
    current: object = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_present(payload: dict[str, object], keys: tuple[str, ...]) -> object:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _ratio_value(value: object) -> Decimal | None:
    parsed = _decimal_value(value)
    if parsed is None:
        return None
    if parsed > 1:
        return parsed / Decimal("100")
    return parsed


def _positive_decimal(value: object) -> Decimal | None:
    parsed = _decimal_value(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _min_positive(left: Decimal | None, right: Decimal | None) -> Decimal | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _decimal_value(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _date_value(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _datetime_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.0001'), rounding=ROUND_DOWN):.4f}"


def _int_or_text(value: Decimal | None) -> int | str | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return _decimal_text(value)
