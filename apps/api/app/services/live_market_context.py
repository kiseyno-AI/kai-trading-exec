from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import UTC, datetime, time, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from app.core.config import Settings

LOGGER = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, object] = {
    "loaded_at": None,
    "context": None,
    "error": None,
    "identity": None,
}

_AUTHORITATIVE_MARKET_CONTEXT_STATUSES = frozenset({"ready", "partial"})
_MARKET_SNAPSHOT_MAX_FUTURE_SKEW_SECONDS = 30
_MARKET_PROVIDER_OBSERVATION_MAX_AGE_SECONDS = 360
_MARKET_INDEX_OBSERVATION_SOURCE = "kis_index_intraday_bar"
_MARKET_BREADTH_MIN_SYMBOL_COUNT = 20
_MARKET_BREADTH_BUCKET_NAMES = (
    "upper_limit_count",
    "advancers",
    "unchanged",
    "decliners",
    "lower_limit_count",
)
_MARKET_BREADTH_LIMIT_BUCKET_SEMANTICS = frozenset(
    {
        "included_in_direction_counts",
        "disjoint_from_direction_counts",
    }
)
_KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
_KRX_REGULAR_SESSION_OPEN = time(9, 0)
_KRX_REGULAR_SESSION_CLOSE = time(15, 30)
_MARKET_IDENTIFIER_ALLOWLIST = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}
_OPTIONAL_MARKET_CONTEXT_MISSING_FIELDS = frozenset(
    {
        "markets.investor_flow",
        "usd_krw.rate",
        "usd_krw.effective_date",
    }
)


def load_live_market_regime_context(*, settings: Settings, now: datetime | None = None) -> dict[str, object]:
    if not settings.market_context_enabled or not settings.market_context_base_url:
        return {}

    checked_at = now or datetime.now(UTC)
    cache_identity = _market_context_cache_identity(settings)
    with _CACHE_LOCK:
        loaded_at = _CACHE.get("loaded_at")
        context = _CACHE.get("context")
        if (
            _CACHE.get("identity") == cache_identity
            and isinstance(loaded_at, datetime)
            and isinstance(context, dict)
        ):
            ttl = timedelta(seconds=int(settings.market_context_cache_ttl_seconds))
            age = checked_at - loaded_at
            if (
                timedelta(0) <= age < ttl
                and _cached_market_evidence_is_current(context, checked_at=checked_at)
            ):
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
        _CACHE["identity"] = cache_identity
    return context


def live_market_breadth_is_current(
    breadth: dict[str, object],
    *,
    checked_at: datetime,
) -> bool:
    """Recheck Core breadth at the execution boundary, independent of fetch/cache age."""
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        return False
    if breadth.get("source") != "kai-trading-core:/market/snapshot":
        return False
    market = _normalized_market(breadth.get("market"))
    if market is None:
        return False
    observed_at = _aware_datetime(breadth.get("observed_at"))
    if observed_at is None:
        return False
    observed_date = observed_at.astimezone(_KRX_TIMEZONE).date()
    if breadth.get("market_date") != observed_date.isoformat():
        return False
    return _market_observation_is_current(observed_at, checked_at=checked_at)


def _market_context_cache_identity(settings: Settings) -> tuple[str, str | None]:
    base_url = str(settings.market_context_base_url or "").rstrip("/")
    token = settings.market_context_bearer_token
    token_fingerprint = hashlib.sha256(token.encode("utf-8")).hexdigest() if token else None
    return base_url, token_fingerprint


def market_regime_context_from_market_snapshot(
    payload: dict[str, Any],
    *,
    fetched_at: datetime | None = None,
) -> dict[str, object]:
    fetched = fetched_at or datetime.now(UTC)
    status = str(payload.get("status") or "unavailable").strip().lower()
    context: dict[str, object] = {
        "market_context_status": status,
        "market_context_as_of": payload.get("as_of"),
        "market_context_fetched_at": fetched.isoformat(),
        "market_regime_context": {
            "source": "kai-trading-core:/market/snapshot",
            "markets": {},
        },
    }
    snapshot_is_authoritative = _snapshot_index_is_authoritative(
        payload=payload,
        status=status,
    )
    if payload.get("stale_reason"):
        reason_key = (
            "market_context_component_stale_reason"
            if snapshot_is_authoritative and status == "partial"
            else "market_context_stale_reason"
        )
        context[reason_key] = str(payload.get("stale_reason"))

    markets = payload.get("markets")
    if (
        not isinstance(markets, dict)
        or status not in _AUTHORITATIVE_MARKET_CONTEXT_STATUSES
        or not snapshot_is_authoritative
    ):
        return context

    regime_context = context["market_regime_context"]
    assert isinstance(regime_context, dict)
    by_market = regime_context["markets"]
    assert isinstance(by_market, dict)

    seen_market_keys: set[str] = set()
    ambiguous_market_keys: set[str] = set()
    for key, raw_market in markets.items():
        key_market = _normalized_market(key)
        if key_market is None:
            continue
        if key_market in seen_market_keys:
            ambiguous_market_keys.add(key_market)
            by_market.pop(key_market, None)
            continue
        seen_market_keys.add(key_market)
        if not isinstance(raw_market, dict):
            continue
        label_market = _normalized_market(raw_market.get("label"))
        if label_market != key_market:
            continue
        observed_at = _authoritative_market_observed_at(
            raw_market,
            fetched_at=fetched,
        )
        if observed_at is None:
            continue
        breadth = _market_breadth_from_snapshot_market(
            raw_market,
            market=key_market,
            observed_at=observed_at,
        )
        if breadth is not None and key_market not in ambiguous_market_keys:
            by_market[key_market] = breadth
    return context


def _snapshot_index_is_authoritative(
    *,
    payload: dict[str, Any],
    status: str,
) -> bool:
    if status not in _AUTHORITATIVE_MARKET_CONTEXT_STATUSES:
        return False
    legs = payload.get("legs")
    index_leg = legs.get("index") if isinstance(legs, dict) else None
    if not isinstance(index_leg, dict):
        return False
    index_status = str(index_leg.get("status") or "").strip().lower()
    missing_fields = payload.get("missing_fields")
    if not isinstance(missing_fields, list):
        return False
    if any(not isinstance(field, str) or not field.strip() for field in missing_fields):
        return False
    normalized_missing_fields = {
        field.strip()
        for field in missing_fields
    }
    if not _snapshot_as_of_matches_provider_observations(payload):
        return False
    if status == "ready":
        return (
            not payload.get("stale_reason")
            and index_status == "ready"
            and not normalized_missing_fields
        )
    return (
        index_status in {"ready", "partial"}
        and normalized_missing_fields <= _OPTIONAL_MARKET_CONTEXT_MISSING_FIELDS
    )


def _snapshot_as_of_matches_provider_observations(payload: dict[str, Any]) -> bool:
    snapshot_as_of = _aware_datetime(payload.get("as_of"))
    markets = payload.get("markets")
    if snapshot_as_of is None or not isinstance(markets, dict):
        return False
    provider_observations = [
        observed_at
        for raw_market in markets.values()
        if isinstance(raw_market, dict)
        and (
            observed_at := _aware_datetime(raw_market.get("provider_observed_at"))
        )
        is not None
    ]
    return bool(provider_observations) and snapshot_as_of == max(provider_observations)


def merge_live_market_regime_context(
    base_context: dict[str, object] | None,
    live_context: dict[str, object] | None,
) -> dict[str, object]:
    merged = dict(base_context) if isinstance(base_context, dict) else {}
    if not isinstance(live_context, dict) or not live_context:
        return merged

    live_status = str(live_context.get("market_context_status") or "").strip().lower()
    live_regime = live_context.get("market_regime_context")
    if isinstance(live_regime, dict):
        existing = merged.get("market_regime_context")
        combined = dict(existing) if isinstance(existing, dict) else {}
        for key, value in live_regime.items():
            if key in {"markets", "source"}:
                combined[key] = value
            elif key == "market_breadth":
                continue
            else:
                combined.setdefault(key, value)
        merged["market_regime_context"] = combined

    for key, value in live_context.items():
        if key in {"market_regime_context", "market_breadth"}:
            continue
        if key.startswith("market_context_"):
            merged[key] = value
        else:
            merged.setdefault(key, value)

    combined_regime = merged.get("market_regime_context")
    if not isinstance(combined_regime, dict):
        combined_regime = {}
        merged["market_regime_context"] = combined_regime

    status_is_authoritative = (
        not live_status
        or live_status in _AUTHORITATIVE_MARKET_CONTEXT_STATUSES
    )
    if not status_is_authoritative or live_context.get("market_context_stale_reason"):
        merged.pop("market_breadth", None)
        combined_regime.pop("market_breadth", None)
        return merged

    selected_breadth = _select_market_breadth(
        base_context=merged,
        live_context=live_context,
    )
    if selected_breadth is None:
        merged.pop("market_breadth", None)
        combined_regime.pop("market_breadth", None)
    else:
        merged["market_breadth"] = selected_breadth
        combined_regime["market_breadth"] = selected_breadth
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


def _market_breadth_from_snapshot_market(
    raw_market: dict[str, Any],
    *,
    market: str,
    observed_at: datetime,
) -> dict[str, object] | None:
    verified_counts = _verified_market_breadth_counts(
        raw_market,
        observed_at=observed_at,
    )
    if verified_counts is None:
        return None
    up_count, down_count, unchanged, total, raw_buckets, semantics = verified_counts
    breadth: dict[str, object] = {
        "market": market,
        "market_date": observed_at.astimezone(_KRX_TIMEZONE).date().isoformat(),
        "observed_at": observed_at.isoformat(),
        "source": "kai-trading-core:/market/snapshot",
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": unchanged,
        "market_symbol_count": total,
        "raw_buckets": raw_buckets,
        "limit_bucket_semantics": semantics,
        "index_change_percent": raw_market.get("change_percent"),
        "foreign_net_value": raw_market.get("foreign_net_value"),
        "institution_net_value": raw_market.get("institution_net_value"),
    }
    breadth["up_ratio"] = f"{up_count / total:.6f}"
    if down_count > 0:
        breadth["advance_decline_ratio"] = f"{up_count / down_count:.6f}"
    return breadth


def _verified_market_breadth_counts(
    raw_market: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[int, int, int, int, dict[str, int], str] | None:
    raw_payload = raw_market.get("breadth_raw_buckets")
    completeness = raw_market.get("breadth_completeness")
    if not isinstance(raw_payload, dict) or not isinstance(completeness, dict):
        return None
    if str(completeness.get("status") or "").strip().lower() != "verified":
        return None

    raw_buckets: dict[str, int] = {}
    for name in _MARKET_BREADTH_BUCKET_NAMES:
        market_value = _nonnegative_int(raw_market.get(name))
        raw_value = _nonnegative_int(raw_payload.get(name))
        if market_value is None or raw_value is None or market_value != raw_value:
            return None
        raw_buckets[name] = market_value

    expected_count = _nonnegative_int(completeness.get("expected_symbol_count"))
    observed_count = _nonnegative_int(completeness.get("observed_symbol_count"))
    semantics = str(completeness.get("limit_bucket_semantics") or "").strip().lower()
    if (
        expected_count is None
        or expected_count < _MARKET_BREADTH_MIN_SYMBOL_COUNT
        or observed_count != expected_count
        or semantics not in _MARKET_BREADTH_LIMIT_BUCKET_SEMANTICS
    ):
        return None

    directional_count = (
        raw_buckets["advancers"]
        + raw_buckets["unchanged"]
        + raw_buckets["decliners"]
    )
    five_bucket_count = sum(raw_buckets.values())
    calculated_count = (
        directional_count
        if semantics == "included_in_direction_counts"
        else five_bucket_count
    )
    if calculated_count != observed_count:
        return None

    expected_universe = completeness.get("expected_universe")
    if not _expected_universe_is_verified(
        expected_universe,
        expected_count=expected_count,
        observed_at=observed_at,
    ):
        return None

    if semantics == "disjoint_from_direction_counts":
        up_count = raw_buckets["upper_limit_count"] + raw_buckets["advancers"]
        down_count = raw_buckets["decliners"] + raw_buckets["lower_limit_count"]
    else:
        up_count = raw_buckets["advancers"]
        down_count = raw_buckets["decliners"]
    return (
        up_count,
        down_count,
        raw_buckets["unchanged"],
        expected_count,
        raw_buckets,
        semantics,
    )


def _expected_universe_is_verified(
    value: object,
    *,
    expected_count: int,
    observed_at: datetime,
) -> bool:
    if not isinstance(value, dict) or value.get("provider") != "kis_master":
        return False
    if value.get("market_date") != observed_at.astimezone(_KRX_TIMEZONE).date().isoformat():
        return False
    if _nonnegative_int(value.get("symbol_count")) != expected_count:
        return False
    completed_at = _aware_datetime(value.get("completed_at"))
    if completed_at is None:
        return False
    membership_sha256 = value.get("membership_sha256")
    return bool(
        isinstance(membership_sha256, str)
        and len(membership_sha256) == 64
        and all(character in "0123456789abcdef" for character in membership_sha256)
    )


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _authoritative_market_observed_at(
    raw_market: dict[str, Any],
    *,
    fetched_at: datetime,
) -> datetime | None:
    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        return None

    index_leg = raw_market.get("index_leg")
    if not isinstance(index_leg, dict):
        return None
    if str(index_leg.get("status") or "").strip().lower() != "ready":
        return None
    if index_leg.get("observation_source") != _MARKET_INDEX_OBSERVATION_SOURCE:
        return None

    observed_at = _aware_datetime(raw_market.get("provider_observed_at"))
    index_observed_at = _aware_datetime(index_leg.get("provider_observed_at"))
    if observed_at is None or index_observed_at is None or observed_at != index_observed_at:
        return None

    observed_local = observed_at.astimezone(_KRX_TIMEZONE)
    observed_date = observed_local.date()
    observed_time = observed_local.time().replace(tzinfo=None)
    if raw_market.get("provider_business_date") != observed_date.isoformat():
        return None
    if raw_market.get("provider_session_time") != observed_time.strftime("%H:%M:%S"):
        return None
    if not _market_observation_is_current(observed_at, checked_at=fetched_at):
        return None
    return observed_at


def _cached_market_evidence_is_current(
    context: dict[str, object],
    *,
    checked_at: datetime,
) -> bool:
    regime = context.get("market_regime_context")
    markets = regime.get("markets") if isinstance(regime, dict) else None
    if not isinstance(markets, dict) or not markets:
        return True
    for breadth in markets.values():
        if not isinstance(breadth, dict):
            return False
        observed_at = _aware_datetime(breadth.get("observed_at"))
        if observed_at is None or not _market_observation_is_current(
            observed_at,
            checked_at=checked_at,
        ):
            return False
    return True


def _market_observation_is_current(
    observed_at: datetime,
    *,
    checked_at: datetime,
) -> bool:
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        return False
    observed_local = observed_at.astimezone(_KRX_TIMEZONE)
    checked_local = checked_at.astimezone(_KRX_TIMEZONE)
    observed_date = observed_local.date()
    observed_time = observed_local.time().replace(tzinfo=None)
    if observed_date != checked_local.date() or observed_date.weekday() >= 5:
        return False
    if not _KRX_REGULAR_SESSION_OPEN <= observed_time <= _KRX_REGULAR_SESSION_CLOSE:
        return False
    age_seconds = (checked_at.astimezone(UTC) - observed_at.astimezone(UTC)).total_seconds()
    return (
        age_seconds >= -_MARKET_SNAPSHOT_MAX_FUTURE_SKEW_SECONDS
        and age_seconds <= _MARKET_PROVIDER_OBSERVATION_MAX_AGE_SECONDS
    )


def _select_market_breadth(
    *,
    base_context: dict[str, object],
    live_context: dict[str, object],
) -> dict[str, object] | None:
    live_regime = live_context.get("market_regime_context")
    markets = live_regime.get("markets") if isinstance(live_regime, dict) else None
    normalized_markets = {
        str(key).strip().upper(): value
        for key, value in markets.items()
        if isinstance(value, dict)
    } if isinstance(markets, dict) else {}
    target_markets = _target_markets(base_context)
    if len(target_markets) > 1:
        return None
    target_market = next(iter(target_markets), None)
    if target_market is not None:
        selected = normalized_markets.get(target_market)
        if isinstance(selected, dict):
            return dict(selected)
        fallback = live_context.get("market_breadth")
        if isinstance(fallback, dict) and _normalized_market(fallback.get("market")) == target_market:
            return dict(fallback)
        return None
    if len(normalized_markets) == 1:
        return dict(next(iter(normalized_markets.values())))
    fallback = live_context.get("market_breadth")
    return dict(fallback) if isinstance(fallback, dict) else None


def _target_market(context: dict[str, object]) -> str | None:
    markets = _target_markets(context)
    return next(iter(markets)) if len(markets) == 1 else None


def _target_markets(context: dict[str, object]) -> set[str]:
    candidates: list[object] = [context.get("market")]
    breadth = context.get("market_breadth")
    if isinstance(breadth, dict):
        candidates.append(breadth.get("market"))
    regime = context.get("market_regime_context")
    if isinstance(regime, dict):
        candidates.append(regime.get("market"))
        regime_breadth = regime.get("market_breadth")
        if isinstance(regime_breadth, dict):
            candidates.append(regime_breadth.get("market"))
    return {
        normalized
        for candidate in candidates
        if (normalized := _normalized_market(candidate)) is not None
    }


def _normalized_market(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return _MARKET_IDENTIFIER_ALLOWLIST.get(value.strip().upper())


def _nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value
