from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.core.config import Settings

LOGGER = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, object] = {
    "loaded_at": None,
    "context": None,
    "error": None,
}


def load_live_market_regime_context(*, settings: Settings, now: datetime | None = None) -> dict[str, object]:
    if not settings.market_context_enabled or not settings.market_context_base_url:
        return {}

    checked_at = now or datetime.now(UTC)
    with _CACHE_LOCK:
        loaded_at = _CACHE.get("loaded_at")
        context = _CACHE.get("context")
        if isinstance(loaded_at, datetime) and isinstance(context, dict):
            ttl = timedelta(seconds=int(settings.market_context_cache_ttl_seconds))
            if checked_at - loaded_at < ttl:
                return dict(context)

    try:
        payload = _fetch_market_snapshot(settings=settings)
        context = market_regime_context_from_market_snapshot(payload, fetched_at=checked_at)
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
        LOGGER.warning("live_market_context_fetch_failed", extra={"error": str(exc)})
        context = {
            "market_context_status": "unavailable",
            "market_context_error": str(exc),
        }

    with _CACHE_LOCK:
        _CACHE["loaded_at"] = checked_at
        _CACHE["context"] = dict(context)
        _CACHE["error"] = context.get("market_context_error")
    return context


def market_regime_context_from_market_snapshot(
    payload: dict[str, Any],
    *,
    fetched_at: datetime | None = None,
) -> dict[str, object]:
    markets = payload.get("markets")
    if not isinstance(markets, dict):
        return {"market_context_status": str(payload.get("status") or "unavailable")}

    fetched = fetched_at or datetime.now(UTC)
    context: dict[str, object] = {
        "market_context_status": str(payload.get("status") or "unknown"),
        "market_context_as_of": payload.get("as_of"),
        "market_context_fetched_at": fetched.isoformat(),
        "market_regime_context": {
            "source": "kai-trading-core:/market/snapshot",
            "markets": {},
        },
    }
    if payload.get("stale_reason"):
        context["market_context_stale_reason"] = str(payload.get("stale_reason"))

    regime_context = context["market_regime_context"]
    assert isinstance(regime_context, dict)
    by_market = regime_context["markets"]
    assert isinstance(by_market, dict)

    preferred_breadth: dict[str, object] | None = None
    for key, raw_market in markets.items():
        if not isinstance(raw_market, dict):
            continue
        breadth = _market_breadth_from_snapshot_market(raw_market, fetched_at=fetched)
        by_market[str(key).upper()] = breadth
        if preferred_breadth is None:
            preferred_breadth = breadth
        label = str(raw_market.get("label") or key).upper()
        if label == "KOSPI":
            preferred_breadth = breadth

    if preferred_breadth is not None:
        context["market_breadth"] = preferred_breadth
        regime_context["market_breadth"] = preferred_breadth
    return context


def merge_live_market_regime_context(
    base_context: dict[str, object] | None,
    live_context: dict[str, object] | None,
) -> dict[str, object]:
    merged = dict(base_context) if isinstance(base_context, dict) else {}
    if not isinstance(live_context, dict) or not live_context:
        return merged

    if isinstance(live_context.get("market_regime_context"), dict):
        existing = merged.get("market_regime_context")
        combined = dict(existing) if isinstance(existing, dict) else {}
        for key, value in live_context["market_regime_context"].items():
            if key in {"market_breadth", "markets", "source"}:
                combined[key] = value
            else:
                combined.setdefault(key, value)
        merged["market_regime_context"] = combined

    for key, value in live_context.items():
        if key == "market_regime_context":
            continue
        if key == "market_breadth":
            merged[key] = value
        else:
            merged.setdefault(key, value)
    return merged


def _fetch_market_snapshot(*, settings: Settings) -> dict[str, Any]:
    base_url = str(settings.market_context_base_url or "").rstrip("/")
    if not base_url:
        raise ValueError("market_context_base_url is required")
    request = Request(
        f"{base_url}/market/snapshot",
        headers=_request_headers(settings),
        method="GET",
    )
    with urlopen(request, timeout=float(settings.market_context_timeout_seconds)) as response:
        body = response.read()
    decoded = json.loads(body.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("market snapshot response must be a JSON object")
    return decoded


def _request_headers(settings: Settings) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    token = settings.market_context_bearer_token
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _market_breadth_from_snapshot_market(raw_market: dict[str, Any], *, fetched_at: datetime) -> dict[str, object]:
    advancers = _int_value(raw_market.get("advancers"))
    decliners = _int_value(raw_market.get("decliners"))
    unchanged = _int_value(raw_market.get("unchanged")) or 0
    total = (advancers or 0) + (decliners or 0) + unchanged
    label = str(raw_market.get("label") or raw_market.get("market") or "KRX").upper()
    breadth: dict[str, object] = {
        "market": label,
        "market_date": fetched_at.date().isoformat(),
        "observed_at": str(raw_market.get("as_of") or fetched_at.isoformat()),
        "source": "kai-trading-core:/market/snapshot",
        "up_count": advancers,
        "down_count": decliners,
        "flat_count": unchanged,
        "market_symbol_count": total if total > 0 else None,
        "index_change_percent": raw_market.get("change_percent"),
        "foreign_net_value": raw_market.get("foreign_net_value"),
        "institution_net_value": raw_market.get("institution_net_value"),
    }
    if total > 0 and advancers is not None:
        breadth["up_ratio"] = f"{advancers / total:.6f}"
    if decliners and advancers is not None:
        breadth["advance_decline_ratio"] = f"{advancers / decliners:.6f}"
    return breadth


def _int_value(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
