from __future__ import annotations


CANONICAL_INTRADAY_WATCH_BUY_INTENT = "intraday_watch_buy"
INTRADAY_WATCH_BUY_INTENTS = frozenset(
    {
        "watch_buy",
        "trigger_buy",
        "chase_buy",
        "tactical_buy",
        CANONICAL_INTRADAY_WATCH_BUY_INTENT,
    }
)
WATCH_BUY_INTENTS = INTRADAY_WATCH_BUY_INTENTS | frozenset({"next_open_buy"})

KRX_EXCHANGES = frozenset({"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"})
KRX_ASSET_CLASS_ALIASES = frozenset(
    {"stock", "equity", "kr_equity", "domestic_equity", "korean_equity"}
)
WATCH_BUY_DIRECTION_FIELDS = (
    "trigger_direction",
    "entry_direction",
    "watch_direction",
    "direction",
)
WATCH_BUY_AT_OR_ABOVE_ALIASES = frozenset(
    {"up", "above", "breakout", "momentum", ">=", "at_or_above"}
)
WATCH_BUY_AT_OR_BELOW_ALIASES = frozenset(
    {"down", "below", "pullback", "dip", "<=", "at_or_below"}
)


def canonical_watch_buy_intent(intent: object, *, side: object = "buy") -> str:
    normalized = str(intent or "").strip().lower()
    if str(side or "").strip().lower() == "buy" and normalized in INTRADAY_WATCH_BUY_INTENTS:
        return CANONICAL_INTRADAY_WATCH_BUY_INTENT
    return normalized


def is_short_term_watch_buy_intent(intent: object) -> bool:
    return str(intent or "").strip().lower() in INTRADAY_WATCH_BUY_INTENTS


def is_watch_buy_intent(intent: object) -> bool:
    return str(intent or "").strip().lower() in WATCH_BUY_INTENTS


def watch_buy_direction_from_payloads(*payloads: object) -> str:
    """Resolve the persisted watch direction without assuming every watch is a breakout."""

    sources = [payload for payload in payloads if isinstance(payload, dict)]
    for source in sources:
        for key in WATCH_BUY_DIRECTION_FIELDS:
            direction = _canonical_watch_buy_direction(source.get(key))
            if direction is not None:
                return direction

    for source in sources:
        tactical_action = source.get("tactical_action")
        if (
            isinstance(tactical_action, str)
            and tactical_action.strip().lower() == "watch_for_trigger"
        ):
            return "at_or_above"

    for source in sources:
        monitoring = source.get("watch_buy_monitoring")
        if not isinstance(monitoring, dict):
            continue
        direction = _canonical_watch_buy_direction(monitoring.get("direction"))
        if direction is not None:
            return direction
        tactical_action = monitoring.get("tactical_action")
        if (
            isinstance(tactical_action, str)
            and tactical_action.strip().lower() == "watch_for_trigger"
        ):
            return "at_or_above"
    return "at_or_below"


def _canonical_watch_buy_direction(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized in WATCH_BUY_AT_OR_ABOVE_ALIASES:
        return "at_or_above"
    if normalized in WATCH_BUY_AT_OR_BELOW_ALIASES:
        return "at_or_below"
    return None


def canonical_krx_asset_class(asset_class: object) -> str | None:
    normalized = str(asset_class or "").strip().lower()
    if normalized in KRX_ASSET_CLASS_ALIASES:
        return "stock"
    return None


def canonical_execution_asset_class(
    asset_class: object,
    *,
    provider: object,
    exchange: object,
) -> str:
    normalized_provider = str(provider or "kis").strip().lower()
    normalized_exchange = str(exchange or "KRX").strip().upper()
    normalized_asset_class = str(asset_class or "stock").strip().lower()
    if normalized_provider == "kis" and normalized_exchange in KRX_EXCHANGES:
        canonical = canonical_krx_asset_class(normalized_asset_class)
        if canonical is None:
            raise ValueError(
                f"unsupported KRX asset_class: {normalized_asset_class or '<missing>'}"
            )
        return canonical
    return normalized_asset_class
