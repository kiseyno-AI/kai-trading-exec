import logging
from datetime import UTC, datetime, timedelta

import pytest

from app.core.config import Settings
from app.services import nxt_eligibility
from app.services.nxt_eligibility import (
    is_nxt_eligible_symbol,
    normalize_nxt_symbol,
    parse_nxt_eligible_payload,
    parse_symbol_list,
    refresh_nxt_eligible_symbols,
)


@pytest.fixture(autouse=True)
def reset_nxt_cache() -> None:
    _reset_nxt_cache()
    yield
    _reset_nxt_cache()


def _reset_nxt_cache() -> None:
    with nxt_eligibility._CACHE._lock:
        nxt_eligibility._CACHE.symbols = frozenset()
        nxt_eligibility._CACHE.fetched_at = None
        nxt_eligibility._CACHE.last_refresh_attempt_at = None
        nxt_eligibility._CACHE.source_url = None
        nxt_eligibility._CACHE.set_time = None
        nxt_eligibility._CACHE.record_count = None
        nxt_eligibility._CACHE.last_error = None
        nxt_eligibility._CACHE.last_error_at = None


def test_parse_nxt_payload_uses_short_codes_without_a_prefix() -> None:
    symbols, record_count, set_time = parse_nxt_eligible_payload(
        {
            "records": 2,
            "setTime": "2026-06-05 07:31",
            "brdinfoTimeList": [
                {"isuSrdCd": "A005930"},
                {"isuSrdCd": "A0008Z0"},
            ],
        }
    )

    assert symbols == frozenset({"005930", "0008Z0"})
    assert record_count == 2
    assert set_time == "2026-06-05 07:31"


def test_normalize_nxt_symbol_accepts_korean_alphanumeric_short_codes() -> None:
    assert normalize_nxt_symbol("A005930") == "005930"
    assert normalize_nxt_symbol("005930") == "005930"
    assert normalize_nxt_symbol("A0008Z0") == "0008Z0"
    assert normalize_nxt_symbol("bad") == ""


def test_env_symbol_list_still_overrides_official_cache() -> None:
    assert parse_symbol_list("A005930; 034730,0008z0") == frozenset({"005930", "034730", "0008Z0"})


def test_is_nxt_eligible_symbol_uses_official_cache_when_env_list_is_empty(monkeypatch) -> None:
    settings = Settings(
        nxt_eligible_symbols="",
        nxt_eligible_source_ttl_seconds=3600,
        nxt_eligible_sync_enabled=True,
    )

    monkeypatch.setattr(
        nxt_eligibility,
        "_fetch_nxt_eligible_payload",
        lambda *, settings: {
            "records": 1,
            "setTime": "2026-06-05 07:31",
            "brdinfoTimeList": [{"isuSrdCd": "A005930"}],
        },
    )
    refresh_nxt_eligible_symbols(
        settings=settings,
        now=datetime(2026, 6, 5, 0, 0, tzinfo=UTC),
        force=True,
    )

    assert is_nxt_eligible_symbol(
        settings=settings,
        symbol="005930",
        now=datetime(2026, 6, 5, 0, 1, tzinfo=UTC),
    )
    assert not is_nxt_eligible_symbol(
        settings=settings,
        symbol="033530",
        now=datetime(2026, 6, 5, 0, 1, tzinfo=UTC),
    )


def test_refresh_keeps_stale_cache_when_official_source_fails(monkeypatch, caplog) -> None:
    settings = Settings(nxt_eligible_sync_enabled=True)

    monkeypatch.setattr(
        nxt_eligibility,
        "_fetch_nxt_eligible_payload",
        lambda *, settings: {
            "records": 1,
            "brdinfoTimeList": [{"isuSrdCd": "A005930"}],
        },
    )
    refresh_nxt_eligible_symbols(
        settings=settings,
        now=datetime(2026, 6, 5, 0, 0, tzinfo=UTC),
        force=True,
    )

    def fail_fetch(*, settings):
        raise RuntimeError("source unavailable")

    monkeypatch.setattr(nxt_eligibility, "_fetch_nxt_eligible_payload", fail_fetch)
    with caplog.at_level(logging.WARNING, logger=nxt_eligibility.LOGGER.name):
        refresh_nxt_eligible_symbols(
            settings=settings,
            now=datetime(2026, 6, 5, 2, 0, tzinfo=UTC),
            force=True,
        )

    assert is_nxt_eligible_symbol(
        settings=settings,
        symbol="005930",
        now=datetime(2026, 6, 5, 2, 0, tzinfo=UTC) + timedelta(seconds=1),
    )
    assert nxt_eligibility.get_nxt_eligible_cache_snapshot()["last_error"] == "source unavailable"
    failure_record = next(
        record for record in caplog.records if record.message == "nxt_eligible_symbol_refresh_failed"
    )
    assert failure_record.error_class == "RuntimeError"
    assert failure_record.error_message == "source unavailable"
    assert failure_record.cached_symbol_count == 1
    assert failure_record.fallback_used is True
    assert failure_record.last_success_at == "2026-06-05T00:00:00+00:00"
    assert failure_record.last_error_at == "2026-06-05T02:00:00+00:00"


def test_failed_cold_cache_refresh_backs_off_immediate_lookups(monkeypatch) -> None:
    settings = Settings(
        nxt_eligible_symbols="",
        nxt_eligible_source_ttl_seconds=3600,
        nxt_eligible_sync_enabled=True,
    )
    calls = 0

    def fetch(*, settings):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("source unavailable")
        return {
            "records": 1,
            "setTime": "2026-06-05 07:31",
            "brdinfoTimeList": [{"isuSrdCd": "A005930"}],
        }

    monkeypatch.setattr(nxt_eligibility, "_fetch_nxt_eligible_payload", fetch)
    failed_at = datetime(2026, 6, 5, 0, 0, tzinfo=UTC)

    assert not is_nxt_eligible_symbol(settings=settings, symbol="005930", now=failed_at)
    assert not is_nxt_eligible_symbol(
        settings=settings,
        symbol="005930",
        now=failed_at + timedelta(seconds=1),
    )
    snapshot = nxt_eligibility.get_nxt_eligible_cache_snapshot()
    assert calls == 1
    assert snapshot["fetched_at"] is None
    assert snapshot["last_refresh_attempt_at"] == failed_at.isoformat()
    assert snapshot["last_error"] == "source unavailable"

    assert is_nxt_eligible_symbol(
        settings=settings,
        symbol="005930",
        now=failed_at + nxt_eligibility.FAILED_REFRESH_BACKOFF,
    )
    assert calls == 2
