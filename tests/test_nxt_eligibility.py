from datetime import UTC, datetime, timedelta

from app.core.config import Settings
from app.services import nxt_eligibility
from app.services.nxt_eligibility import (
    is_nxt_eligible_symbol,
    normalize_nxt_symbol,
    parse_nxt_eligible_payload,
    parse_symbol_list,
    refresh_nxt_eligible_symbols,
)


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


def test_refresh_keeps_stale_cache_when_official_source_fails(monkeypatch) -> None:
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
