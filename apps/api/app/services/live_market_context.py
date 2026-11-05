from __future__ import annotations

import copy
import hashlib
import json
import logging
import threading
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from app.broker.base import BrokerAccountSnapshot
from app.core.config import Settings

LOGGER = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, object] = {
    "loaded_at": None,
    "context": None,
    "error": None,
    "identity": None,
}
_SECTOR_CACHE_LOCK = threading.Lock()
_SECTOR_CACHE: dict[tuple[object, ...], dict[str, object]] = {}

_AUTHORITATIVE_MARKET_CONTEXT_STATUSES = frozenset({"ready", "partial"})
_MARKET_SNAPSHOT_MAX_FUTURE_SKEW_SECONDS = 30
_MARKET_PROVIDER_OBSERVATION_MAX_AGE_SECONDS = 360
_LIVE_SECTOR_PROJECTION_SCHEMA_VERSION = "live_sector_projection_v1"
_LIVE_SECTOR_PROJECTION_SOURCE = "core_verified_pykrx_sector_plus_kis_current_session"
_LIVE_SECTOR_CONTEXT_SOURCE = "kai-trading-core:/market/live-sector-projection"
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


def load_cached_live_market_regime_context(
    *,
    settings: Settings,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read current cached market authority without performing network I/O.

    The broker's serialized final boundary must not make another provider call
    after its account/order locks.  Monitoring has already populated this
    cache; if that proof has aged out while the locked checks run, return an
    explicit unavailable snapshot so stale plan-day breadth/sector fields are
    removed by the merge and the configured live fail-closed policy applies.
    """

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
    return {
        "market_context_status": "unavailable",
        "market_context_stale_reason": "final_boundary_cached_evidence_unavailable",
        "market_regime_context": {},
    }


def load_live_sector_regime_context(
    *,
    settings: Settings,
    symbol: str,
    source_market_date: date | str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Load current-session sector performance plus verified sector identity.

    The identity can be used for account-crowding classification before the
    market opens.  Performance is exposed as ``sector_context`` only when Core
    proves a current-session projection.  Pending or unavailable responses
    therefore remove stale plan-day sector performance without throwing away a
    verified mapping that is still useful for exposure counting.
    """

    checked_at = _aware_checked_at(now or datetime.now(UTC))
    normalized_symbol = _normalize_symbol(symbol)
    normalized_source_date = _date_value(source_market_date)
    if not settings.market_context_enabled or not settings.market_context_base_url:
        return _unavailable_sector_context(
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
            fetched_at=checked_at,
            reason="live_sector_context_disabled",
        )
    if not normalized_symbol or normalized_source_date is None:
        return _unavailable_sector_context(
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
            fetched_at=checked_at,
            reason="live_sector_context_identity_missing",
        )

    cache_key = _sector_context_cache_key(
        settings=settings,
        symbol=normalized_symbol,
        source_market_date=normalized_source_date,
    )
    cached = _load_cached_sector_context(
        settings=settings,
        cache_key=cache_key,
        checked_at=checked_at,
    )
    if cached is not None:
        return cached

    try:
        payload = _fetch_live_sector_projection(
            settings=settings,
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
        )
        context = live_sector_regime_context_from_projection(
            payload,
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
            fetched_at=checked_at,
        )
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
        LOGGER.warning(
            "live_sector_context_fetch_failed",
            extra={"symbol": normalized_symbol, "error": str(exc)},
        )
        context = _unavailable_sector_context(
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
            fetched_at=checked_at,
            reason="live_sector_context_fetch_failed",
            error=str(exc),
        )

    with _SECTOR_CACHE_LOCK:
        _SECTOR_CACHE[cache_key] = {
            "loaded_at": checked_at,
            "context": dict(context),
        }
    return context


def load_cached_live_sector_regime_context(
    *,
    settings: Settings,
    symbol: str,
    source_market_date: date | str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Read current cached sector authority without network I/O."""

    checked_at = _aware_checked_at(now or datetime.now(UTC))
    normalized_symbol = _normalize_symbol(symbol)
    normalized_source_date = _date_value(source_market_date)
    if not normalized_symbol or normalized_source_date is None:
        return _unavailable_sector_context(
            symbol=normalized_symbol,
            source_market_date=normalized_source_date,
            fetched_at=checked_at,
            reason="final_boundary_cached_sector_identity_missing",
        )
    cache_key = _sector_context_cache_key(
        settings=settings,
        symbol=normalized_symbol,
        source_market_date=normalized_source_date,
    )
    cached = _load_cached_sector_context(
        settings=settings,
        cache_key=cache_key,
        checked_at=checked_at,
    )
    if cached is not None:
        return cached
    return _unavailable_sector_context(
        symbol=normalized_symbol,
        source_market_date=normalized_source_date,
        fetched_at=checked_at,
        reason="final_boundary_cached_sector_evidence_unavailable",
    )


def live_sector_regime_context_from_projection(
    payload: dict[str, Any],
    *,
    symbol: str,
    source_market_date: date,
    fetched_at: datetime | None = None,
) -> dict[str, object]:
    fetched = _aware_checked_at(fetched_at or datetime.now(UTC))
    normalized_symbol = _normalize_symbol(symbol)
    base = _unavailable_sector_context(
        symbol=normalized_symbol,
        source_market_date=source_market_date,
        fetched_at=fetched,
        reason="live_sector_projection_invalid",
    )
    if payload.get("schema_version") != _LIVE_SECTOR_PROJECTION_SCHEMA_VERSION:
        base["sector_context_reason"] = "live_sector_projection_schema_invalid"
        return base
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"ready", "pending_current_session", "unavailable"}:
        base["sector_context_reason"] = "live_sector_projection_status_invalid"
        return base
    if payload.get("source") != _LIVE_SECTOR_PROJECTION_SOURCE:
        base["sector_context_reason"] = "live_sector_projection_source_invalid"
        return base
    if _normalize_symbol(payload.get("symbol")) != normalized_symbol:
        base["sector_context_reason"] = "live_sector_projection_symbol_mismatch"
        return base
    if _date_value(payload.get("source_market_date")) != source_market_date:
        base["sector_context_reason"] = "live_sector_projection_mapping_date_mismatch"
        return base

    market = _normalized_market(payload.get("market"))
    market_date = _date_value(payload.get("market_date"))
    identity = _verified_live_sector_identity(
        payload.get("sector_identity"),
        expected_market=market,
        source_market_date=source_market_date,
    )
    base.update(
        {
            "sector_context_status": status,
            "sector_context_reason": _nonempty_text(payload.get("reason")),
            "sector_context_market_date": (
                market_date.isoformat() if market_date is not None else None
            ),
            "sector_context_missing_fields": _string_list(payload.get("missing_fields")),
        }
    )
    if identity is not None:
        base["sector_identity"] = identity

    if status != "ready":
        return base
    if market is None or market_date != fetched.astimezone(_KRX_TIMEZONE).date():
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_market_date_invalid"
        return base
    if identity is None:
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_identity_unverified"
        return base
    observed_at = _aware_datetime(payload.get("observed_at"))
    if (
        observed_at is None
        or observed_at.astimezone(_KRX_TIMEZONE).date() != market_date
        or not _market_observation_is_current(observed_at, checked_at=fetched)
    ):
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_observation_stale"
        return base
    projection = payload.get("projection")
    cohort = payload.get("cohort")
    if not isinstance(projection, dict) or projection.get("status") != "ready":
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_payload_invalid"
        return base
    relative_return = _finite_number(projection.get("relative_return_pct"))
    sector_return = _finite_number(projection.get("sector_return_pct"))
    benchmark_return = _finite_number(projection.get("benchmark_return_pct"))
    live_coverage = _finite_number(projection.get("live_market_cap_coverage_ratio"))
    if any(
        value is None
        for value in (relative_return, sector_return, benchmark_return, live_coverage)
    ):
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_metrics_invalid"
        return base
    membership_sha = (
        cohort.get("membership_sha256") if isinstance(cohort, dict) else None
    )
    if not _sha256_text(membership_sha):
        base["sector_context_status"] = "unavailable"
        base["sector_context_reason"] = "live_sector_projection_cohort_unverified"
        return base

    sector_context: dict[str, object] = {
        "schema_version": _LIVE_SECTOR_PROJECTION_SCHEMA_VERSION,
        "source": _LIVE_SECTOR_CONTEXT_SOURCE,
        "symbol": normalized_symbol,
        "market": market,
        "market_date": market_date.isoformat(),
        "source_market_date": source_market_date.isoformat(),
        "observed_at": observed_at.isoformat(),
        "sector_name": identity["sector_name"],
        "sector_return_pct": projection.get("sector_return_pct"),
        "benchmark_return_pct": projection.get("benchmark_return_pct"),
        "relative_return_pct": projection.get("relative_return_pct"),
        "live_market_cap_coverage_ratio": projection.get(
            "live_market_cap_coverage_ratio"
        ),
        "cohort_membership_sha256": membership_sha,
        "sector_identity": identity,
    }
    base["sector_context"] = sector_context
    base["market_regime_context"] = {"sector_context": sector_context}
    return base


def merge_live_sector_regime_context(
    base_context: dict[str, object] | None,
    live_sector_context: dict[str, object] | None,
) -> dict[str, object]:
    """Replace sector authority without disturbing independently loaded breadth."""

    merged = dict(base_context) if isinstance(base_context, dict) else {}
    if not isinstance(live_sector_context, dict) or not live_sector_context:
        return merged
    regime = merged.get("market_regime_context")
    combined = dict(regime) if isinstance(regime, dict) else {}
    for key in ("sector_context", "sector", "sector_peer_context", "sector_peers_context"):
        merged.pop(key, None)
        combined.pop(key, None)
    for key, value in live_sector_context.items():
        if key.startswith("sector_context_") or key == "sector_identity":
            merged[key] = value
    current = live_sector_context.get("sector_context")
    if isinstance(current, dict) and live_sector_context.get("sector_context_status") == "ready":
        current_copy = dict(current)
        merged["sector_context"] = current_copy
        combined["sector_context"] = current_copy
    merged["market_regime_context"] = combined
    return merged


def enrich_account_context_with_live_sector_identity(
    *,
    settings: Settings,
    account_context: dict[str, object] | None,
    source_market_date: date | str | None,
    now: datetime | None = None,
    cached_only: bool = False,
) -> dict[str, object]:
    """Attach verified sector names to held and open-buy symbols.

    Missing account coverage or one missing identity is represented explicitly;
    callers must not reinterpret an incomplete mapping as zero sector exposure.
    """

    checked_at = _aware_checked_at(now or datetime.now(UTC))
    normalized_source_date = _date_value(source_market_date)
    enriched = copy.deepcopy(account_context) if isinstance(account_context, dict) else {}
    holdings = _dict_rows(enriched.get("holdings"))
    open_orders = _dict_rows(enriched.get("open_orders"))
    enriched["holdings"] = holdings
    enriched["open_orders"] = open_orders
    holdings_complete = enriched.get("holdings_complete") is True
    open_orders_complete = enriched.get("open_orders_complete") is True

    required_symbols: set[str] = set()
    for row in holdings:
        symbol = _normalize_symbol(row.get("symbol"))
        quantity = _finite_number(row.get("quantity"))
        if symbol and (quantity is None or quantity > 0):
            required_symbols.add(symbol)
    for row in open_orders:
        symbol = _normalize_symbol(row.get("symbol"))
        side = str(row.get("side") or "").strip().lower()
        remaining = _finite_number(
            row.get("remaining_quantity", row.get("quantity"))
        )
        if symbol and side == "buy" and (remaining is None or remaining > 0):
            required_symbols.add(symbol)

    identities: dict[str, dict[str, object]] = {}
    missing_symbols: list[str] = []
    if normalized_source_date is None:
        missing_symbols = sorted(required_symbols)
    else:
        loader = (
            load_cached_live_sector_regime_context
            if cached_only
            else load_live_sector_regime_context
        )
        for symbol in sorted(required_symbols):
            context = loader(
                settings=settings,
                symbol=symbol,
                source_market_date=normalized_source_date,
                now=checked_at,
            )
            identity = context.get("sector_identity")
            if isinstance(identity, dict) and _nonempty_text(identity.get("sector_name")):
                identities[symbol] = dict(identity)
            else:
                missing_symbols.append(symbol)

    def attach(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        for row in rows:
            updated = dict(row)
            identity = identities.get(_normalize_symbol(row.get("symbol")))
            if identity is not None:
                updated["sector_name"] = identity["sector_name"]
                updated["sector_identity"] = identity
            else:
                updated.pop("sector_name", None)
                updated.pop("sector", None)
                updated.pop("sector_identity", None)
            result.append(updated)
        return result

    enriched["holdings"] = attach(holdings)
    enriched["open_orders"] = attach(open_orders)
    mapping_complete = bool(
        normalized_source_date is not None
        and holdings_complete
        and open_orders_complete
        and not missing_symbols
    )
    enriched.update(
        {
            "sector_mapping_complete": mapping_complete,
            "sector_mapping_status": "ready" if mapping_complete else "incomplete",
            "sector_mapping_source": _LIVE_SECTOR_CONTEXT_SOURCE,
            "sector_mapping_source_market_date": (
                normalized_source_date.isoformat()
                if normalized_source_date is not None
                else None
            ),
            "sector_mapping_checked_at": checked_at.isoformat(),
            "sector_mapping_missing_symbols": sorted(set(missing_symbols)),
            "sector_mapping_required_symbol_count": len(required_symbols),
            "sector_mapping_resolved_symbol_count": len(identities),
            "holdings_complete": holdings_complete,
            "open_orders_complete": open_orders_complete,
        }
    )
    return enriched


def market_regime_account_context_from_snapshot(
    snapshot: BrokerAccountSnapshot,
) -> dict[str, object]:
    """Project only current broker truth needed by sector-crowding filters."""

    holdings = [
        {
            "symbol": holding.symbol,
            "quantity": holding.quantity,
        }
        for holding in snapshot.holdings
        if holding.quantity > 0
    ]
    open_orders = []
    for order in snapshot.open_orders:
        remaining_quantity = (
            order.remaining_quantity
            if order.remaining_quantity is not None
            else order.quantity
        )
        if remaining_quantity is None or remaining_quantity <= 0:
            continue
        open_orders.append(
            {
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "remaining_quantity": remaining_quantity,
                "status": order.status,
            }
        )
    return {
        "available": True,
        "observed_at": snapshot.observed_at.isoformat(),
        "holdings_complete": snapshot.holdings_complete,
        "open_orders_complete": snapshot.open_orders_complete,
        "holdings": holdings,
        "open_orders": open_orders,
    }


def live_sector_context_is_current(
    sector: dict[str, object],
    *,
    checked_at: datetime,
) -> bool:
    if sector.get("source") != _LIVE_SECTOR_CONTEXT_SOURCE:
        return False
    market = _normalized_market(sector.get("market"))
    observed_at = _aware_datetime(sector.get("observed_at"))
    market_date = _date_value(sector.get("market_date"))
    if market is None or observed_at is None or market_date is None:
        return False
    if market_date != checked_at.astimezone(_KRX_TIMEZONE).date():
        return False
    return _market_observation_is_current(observed_at, checked_at=checked_at)


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
        # A fetched live snapshot replaces execution-time market authority.
        # Never carry a plan-day sector judgment through merely because the
        # current provider does not publish a sector update.
        for key in ("sector_context", "sector", "sector_peer_context", "sector_peers_context"):
            combined.pop(key, None)
        for key, value in live_regime.items():
            if key in {
                "markets",
                "source",
                "sector_context",
                "sector",
                "sector_peer_context",
                "sector_peers_context",
            }:
                combined[key] = value
            elif key == "market_breadth":
                continue
            else:
                combined.setdefault(key, value)
        merged["market_regime_context"] = combined

    for key in (
        "sector_context",
        "sector_peer_context",
        "sector_peers_context",
        "market_relative_context",
        "decider_flow_short_sector_context",
    ):
        merged.pop(key, None)
    for key, value in live_context.items():
        if key in {"market_regime_context", "market_breadth"}:
            continue
        if key.startswith("market_context_") or key in {
            "sector_context",
            "sector_peer_context",
            "sector_peers_context",
            "market_relative_context",
            "decider_flow_short_sector_context",
        }:
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


def _fetch_live_sector_projection(
    *,
    settings: Settings,
    symbol: str,
    source_market_date: date,
) -> dict[str, Any]:
    base_url = str(settings.market_context_base_url or "").rstrip("/")
    if not base_url:
        raise ValueError("market_context_base_url is required")
    query = urlencode(
        {
            "symbol": symbol,
            "source_market_date": source_market_date.isoformat(),
        }
    )
    request = Request(
        f"{base_url}/market/live-sector-projection?{query}",
        headers=_request_headers(settings),
        method="GET",
    )
    with urlopen(request, timeout=float(settings.market_context_timeout_seconds)) as response:
        body = response.read()
    decoded = json.loads(body.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("live sector projection response must be a JSON object")
    return decoded


def _sector_context_cache_key(
    *,
    settings: Settings,
    symbol: str,
    source_market_date: date,
) -> tuple[object, ...]:
    return (
        *_market_context_cache_identity(settings),
        symbol,
        source_market_date.isoformat(),
    )


def _load_cached_sector_context(
    *,
    settings: Settings,
    cache_key: tuple[object, ...],
    checked_at: datetime,
) -> dict[str, object] | None:
    with _SECTOR_CACHE_LOCK:
        entry = _SECTOR_CACHE.get(cache_key)
        if not isinstance(entry, dict):
            return None
        loaded_at = entry.get("loaded_at")
        context = entry.get("context")
        if not isinstance(loaded_at, datetime) or not isinstance(context, dict):
            return None
        age = checked_at - loaded_at
        ttl = timedelta(seconds=int(settings.market_context_cache_ttl_seconds))
        if age < timedelta(0) or age >= ttl:
            return None
        status = str(context.get("sector_context_status") or "").strip().lower()
        if status == "ready":
            sector = context.get("sector_context")
            if not isinstance(sector, dict) or not live_sector_context_is_current(
                sector,
                checked_at=checked_at,
            ):
                return None
        else:
            market_date = _date_value(context.get("sector_context_market_date"))
            if (
                market_date is not None
                and market_date != checked_at.astimezone(_KRX_TIMEZONE).date()
            ):
                return None
        return copy.deepcopy(context)


def _unavailable_sector_context(
    *,
    symbol: str,
    source_market_date: date | None,
    fetched_at: datetime,
    reason: str,
    error: str | None = None,
) -> dict[str, object]:
    context: dict[str, object] = {
        "sector_context_status": "unavailable",
        "sector_context_reason": reason,
        "sector_context_symbol": symbol or None,
        "sector_context_source_market_date": (
            source_market_date.isoformat() if source_market_date is not None else None
        ),
        "sector_context_market_date": None,
        "sector_context_fetched_at": fetched_at.isoformat(),
        "sector_context_missing_fields": [],
        "market_regime_context": {},
    }
    if error:
        context["sector_context_error"] = error
    return context


def _verified_live_sector_identity(
    value: object,
    *,
    expected_market: str | None,
    source_market_date: date,
) -> dict[str, object] | None:
    if not isinstance(value, dict) or expected_market is None:
        return None
    sector_name = _nonempty_text(value.get("sector_name"))
    if (
        value.get("status") != "available"
        or not sector_name
        or _normalized_market(value.get("market")) != expected_market
        or _date_value(value.get("mapping_date")) != source_market_date
        or value.get("mapping_source") != "pykrx"
        or value.get("universe_market_complete") is not True
        or _nonnegative_int(value.get("universe_reference_count")) in {None, 0}
        or not _sha256_text(value.get("universe_reference_sha256"))
        or value.get("universe_reference_source") != "pykrx_market_ticker_list"
    ):
        return None
    return copy.deepcopy(value)


def _aware_checked_at(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(UTC)


def _date_value(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _normalize_symbol(value: object) -> str:
    text = str(value or "").strip().upper()
    return text.zfill(6) if text.isdigit() else text


def _nonempty_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _sha256_text(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _finite_number(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _dict_rows(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, dict)]


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
        return False
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


def market_regime_target_market(
    context: dict[str, object] | None,
) -> str | None:
    """Return one unambiguous KOSPI/KOSDAQ identity from persisted context."""

    return _target_market(context) if isinstance(context, dict) else None


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
