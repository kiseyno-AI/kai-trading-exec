from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from app.core.config import Settings

LOGGER = logging.getLogger(__name__)


@dataclass
class NxtEligibleSymbolCache:
    symbols: frozenset[str] = frozenset()
    fetched_at: datetime | None = None
    source_url: str | None = None
    set_time: str | None = None
    record_count: int | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "symbol_count": len(self.symbols),
                "fetched_at": self.fetched_at.isoformat() if self.fetched_at is not None else None,
                "source_url": self.source_url,
                "set_time": self.set_time,
                "record_count": self.record_count,
                "last_error": self.last_error,
                "last_error_at": self.last_error_at.isoformat() if self.last_error_at is not None else None,
            }


_CACHE = NxtEligibleSymbolCache()


def is_nxt_eligible_symbol(*, settings: Settings, symbol: str, now: datetime | None = None) -> bool:
    normalized_symbol = normalize_nxt_symbol(symbol)
    if not normalized_symbol:
        return False

    override_symbols = parse_symbol_list(settings.nxt_eligible_symbols)
    if normalized_symbol in override_symbols:
        return True

    if not settings.nxt_eligible_sync_enabled:
        return False

    refresh_nxt_eligible_symbols(settings=settings, now=now)
    return normalized_symbol in _CACHE.symbols


def refresh_nxt_eligible_symbols(
    *,
    settings: Settings,
    now: datetime | None = None,
    force: bool = False,
) -> dict[str, object]:
    checked_at = now or datetime.now(UTC)
    if not settings.nxt_eligible_sync_enabled:
        return _CACHE.snapshot()
    if not force and not _cache_is_stale(settings=settings, now=checked_at):
        return _CACHE.snapshot()

    try:
        payload = _fetch_nxt_eligible_payload(settings=settings)
        symbols, record_count, set_time = parse_nxt_eligible_payload(payload)
    except Exception as exc:
        _store_refresh_error(error=str(exc), now=checked_at)
        LOGGER.warning(
            "nxt_eligible_symbol_refresh_failed",
            extra={"source_url": settings.nxt_eligible_source_url},
        )
        return _CACHE.snapshot()

    with _CACHE._lock:
        _CACHE.symbols = frozenset(symbols)
        _CACHE.fetched_at = checked_at
        _CACHE.source_url = settings.nxt_eligible_source_url
        _CACHE.set_time = set_time
        _CACHE.record_count = record_count
        _CACHE.last_error = None
        _CACHE.last_error_at = None
    LOGGER.info(
        "nxt_eligible_symbol_cache_refreshed",
        extra={
            "source_url": settings.nxt_eligible_source_url,
            "symbol_count": len(symbols),
            "record_count": record_count,
            "set_time": set_time,
        },
    )
    return _CACHE.snapshot()


def get_nxt_eligible_cache_snapshot() -> dict[str, object]:
    return _CACHE.snapshot()


def parse_symbol_list(raw_symbols: str) -> frozenset[str]:
    return frozenset(
        normalized
        for token in raw_symbols.replace(";", ",").split(",")
        if (normalized := normalize_nxt_symbol(token))
    )


def parse_nxt_eligible_payload(payload: dict[str, Any]) -> tuple[frozenset[str], int | None, str | None]:
    rows = payload.get("brdinfoTimeList")
    if not isinstance(rows, list):
        raise ValueError("NXT eligibility payload is missing brdinfoTimeList")

    symbols: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_symbol = row.get("isuSrdCd")
        if raw_symbol is None:
            raw_symbol = row.get("isuCd")
        normalized = normalize_nxt_symbol(str(raw_symbol)) if raw_symbol is not None else ""
        if normalized:
            symbols.add(normalized)

    if not symbols:
        raise ValueError("NXT eligibility payload did not include any symbols")

    record_count = payload.get("records")
    if not isinstance(record_count, int):
        record_count = payload.get("totalCnt")
    if not isinstance(record_count, int):
        record_count = None

    set_time = payload.get("setTime")
    if not isinstance(set_time, str):
        set_time = None
    return frozenset(symbols), record_count, set_time


def normalize_nxt_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized.startswith("A") and len(normalized) == 7:
        normalized = normalized[1:]
    if len(normalized) != 6 or not normalized.isalnum():
        return ""
    return normalized


def _cache_is_stale(*, settings: Settings, now: datetime) -> bool:
    fetched_at = _CACHE.fetched_at
    if fetched_at is None:
        return True
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    return now - fetched_at >= timedelta(seconds=settings.nxt_eligible_source_ttl_seconds)


def _fetch_nxt_eligible_payload(*, settings: Settings) -> dict[str, Any]:
    request = Request(
        settings.nxt_eligible_source_url,
        headers={
            "Accept": "*/*",
            "Referer": "https://www.nextrade.co.kr/menu/marketData/menuList.do",
            "User-Agent": "curl/8.0.0",
        },
    )
    try:
        with urlopen(request, timeout=settings.nxt_eligible_source_timeout_seconds) as response:
            raw_body = response.read()
    except URLError as exc:
        raise RuntimeError(f"NXT eligibility source request failed: {exc}") from exc

    payload = json.loads(raw_body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("NXT eligibility source returned non-object JSON")
    return payload


def _store_refresh_error(*, error: str, now: datetime) -> None:
    with _CACHE._lock:
        _CACHE.last_error = error
        _CACHE.last_error_at = now
