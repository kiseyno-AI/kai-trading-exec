from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings
from app.services import live_market_context
from app.services.live_market_context import (
    load_live_market_regime_context,
    market_regime_context_from_market_snapshot,
    merge_live_market_regime_context,
)


def _ready_market(
    label: str,
    observed_at: str,
    **overrides: object,
) -> dict[str, object]:
    observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    observed_local = observed.astimezone(ZoneInfo("Asia/Seoul"))
    row: dict[str, object] = {
        "label": label,
        "provider_business_date": observed_local.date().isoformat(),
        "provider_session_time": observed_local.strftime("%H:%M:%S"),
        "provider_observed_at": observed_at,
        "index_leg": {
            "status": "ready",
            "observation_source": "kis_index_intraday_bar",
            "provider_observed_at": observed_at,
        },
        "advancers": 500,
        "unchanged": 10,
        "decliners": 400,
        "upper_limit_count": 0,
        "lower_limit_count": 0,
    }
    row.update(overrides)
    raw_buckets = {
        "upper_limit_count": row["upper_limit_count"],
        "advancers": row["advancers"],
        "unchanged": row["unchanged"],
        "decliners": row["decliners"],
        "lower_limit_count": row["lower_limit_count"],
    }
    limit_buckets_are_disjoint = bool(
        raw_buckets["upper_limit_count"] or raw_buckets["lower_limit_count"]
    )
    observed_symbol_count = (
        sum(int(value) for value in raw_buckets.values())
        if limit_buckets_are_disjoint
        else int(raw_buckets["advancers"])
        + int(raw_buckets["unchanged"])
        + int(raw_buckets["decliners"])
    )
    row.setdefault("breadth_raw_buckets", raw_buckets)
    row.setdefault(
        "breadth_completeness",
        {
            "status": "verified",
            "reason": None,
            "expected_symbol_count": observed_symbol_count,
            "observed_symbol_count": observed_symbol_count,
            "limit_bucket_semantics": (
                "disjoint_from_direction_counts"
                if limit_buckets_are_disjoint
                else "included_in_direction_counts"
            ),
            "expected_universe": {
                "provider": "kis_master",
                "market_date": observed_local.date().isoformat(),
                "completed_at": observed_at,
                "symbol_count": observed_symbol_count,
                "membership_sha256": "0" * 64,
            },
        },
    )
    return row


def test_market_snapshot_converts_to_guardrail_breadth_context() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T13:38:49+09:00",
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T13:38:49+09:00",
                    change_percent=-3.46,
                    advancers=114,
                    unchanged=12,
                    decliners=787,
                    foreign_net_value=158312000000,
                    institution_net_value=-207487000000,
                ),
                "kosdaq": _ready_market(
                    "KOSDAQ",
                    "2026-07-20T13:38:49+09:00",
                    change_percent=-4.63,
                    advancers=190,
                    unchanged=36,
                    decliners=1513,
                ),
            },
        },
        fetched_at=datetime(2026, 7, 20, 4, 40, tzinfo=UTC),
    )

    breadth = context["market_regime_context"]["markets"]["KOSPI"]
    assert breadth["market"] == "KOSPI"
    assert breadth["up_count"] == 114
    assert breadth["down_count"] == 787
    assert breadth["up_ratio"] == "0.124863"
    assert breadth["advance_decline_ratio"] == "0.144854"
    assert breadth["index_change_percent"] == -3.46
    assert breadth["market_symbol_count"] == 913
    assert breadth["limit_bucket_semantics"] == "included_in_direction_counts"
    assert breadth["observed_at"] == "2026-07-20T13:38:49+09:00"
    assert breadth["market_date"] == "2026-07-20"


def test_stale_core_snapshot_cannot_be_relabelled_fresh_by_transport_time() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "stale",
            "as_of": "2026-07-20T09:00:00+09:00",
            "stale_reason": "provider snapshot exceeded max age",
            "markets": {
                "kospi": {
                    "label": "KOSPI",
                    "advancers": 800,
                    "unchanged": 10,
                    "decliners": 90,
                }
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 0, tzinfo=UTC),
    )

    assert context["market_context_status"] == "stale"
    assert context["market_context_fetched_at"] == "2026-07-20T05:00:00+00:00"
    assert "market_breadth" not in context
    assert context["market_regime_context"]["markets"] == {}


def test_market_snapshot_rejects_top_as_of_that_conflicts_with_provider_max() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-21T00:01:00+09:00",
            "markets": {
                "kosdaq": _ready_market(
                    "KOSDAQ",
                    "2026-07-20T14:59:00+09:00",
                    advancers=400,
                    unchanged=10,
                    decliners=500,
                )
            },
        },
        fetched_at=datetime(2026, 7, 20, 6, 0, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


@pytest.mark.parametrize(
    ("legs", "missing_fields"),
    [
        (None, []),
        ({"index": {"status": "unverified"}}, []),
        ({"index": {"status": "ready"}}, None),
        ({"index": {"status": "ready"}}, ["markets.investor_flow"]),
    ],
)
def test_ready_snapshot_requires_complete_top_index_contract(
    legs: object,
    missing_fields: object,
) -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "as_of": "2026-07-20T14:59:00+09:00",
            "legs": legs,
            "missing_fields": missing_fields,
            "markets": {
                "kosdaq": _ready_market("KOSDAQ", "2026-07-20T14:59:00+09:00")
            },
        },
        fetched_at=datetime(2026, 7, 20, 6, 0, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


def test_market_snapshot_derives_market_date_in_krx_timezone() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T00:01:00+00:00",
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T00:01:00+00:00",
                    advancers=400,
                    unchanged=10,
                    decliners=500,
                )
            },
        },
        fetched_at=datetime(2026, 7, 20, 0, 2, tzinfo=UTC),
    )

    breadth = context["market_regime_context"]["markets"]["KOSPI"]
    assert breadth["observed_at"] == "2026-07-20T00:01:00+00:00"
    assert breadth["market_date"] == "2026-07-20"


def test_partial_snapshot_keeps_fresh_index_breadth_when_only_optional_legs_fail() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "partial",
            "as_of": "2026-07-20T13:38:49+09:00",
            "stale_reason": "koreaexim_upstream_error",
            "missing_fields": ["markets.investor_flow", "usd_krw.rate"],
            "legs": {
                "index": {"status": "ready"},
                "flow": {"status": "unavailable"},
                "fx": {"status": "missing"},
            },
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T13:38:49+09:00",
                )
            },
        },
        fetched_at=datetime(2026, 7, 20, 4, 40, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"]["KOSPI"]["up_count"] == 500
    assert context["market_context_component_stale_reason"] == "koreaexim_upstream_error"


def test_partial_snapshot_rejects_breadth_when_index_leg_is_not_ready() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "partial",
            "as_of": "2026-07-20T13:38:49+09:00",
            "missing_fields": ["markets.index"],
            "legs": {"index": {"status": "unverified"}},
            "markets": {
                "kospi": {
                    "label": "KOSPI",
                    "advancers": 800,
                    "unchanged": 10,
                    "decliners": 90,
                }
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 0, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


def test_partial_snapshot_keeps_each_ready_market_when_another_index_leg_is_unverified() -> None:
    unverified_kosdaq = _ready_market("KOSDAQ", "2026-07-20T13:38:49+09:00")
    unverified_kosdaq["index_leg"] = {
        "status": "unverified",
        "observation_source": "kis_index_intraday_bar",
        "provider_observed_at": "2026-07-20T13:38:49+09:00",
    }
    context = market_regime_context_from_market_snapshot(
        {
            "status": "partial",
            "as_of": "2026-07-20T13:38:49+09:00",
            "missing_fields": [],
            "legs": {"index": {"status": "partial"}},
            "markets": {
                "kospi": _ready_market("KOSPI", "2026-07-20T13:38:49+09:00"),
                "kosdaq": unverified_kosdaq,
            },
        },
        fetched_at=datetime(2026, 7, 20, 4, 40, tzinfo=UTC),
    )

    assert set(context["market_regime_context"]["markets"]) == {"KOSPI"}


def test_snapshot_rejects_market_key_and_label_mismatch() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T13:38:49+09:00",
            "markets": {
                "kospi": {
                    "label": "KOSDAQ",
                    "advancers": 800,
                    "unchanged": 10,
                    "decliners": 90,
                }
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 0, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


def test_merge_selects_kosdaq_breadth_for_kosdaq_order_context() -> None:
    live = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T13:38:49+09:00",
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T13:38:49+09:00",
                    advancers=800,
                    unchanged=10,
                    decliners=90,
                ),
                "kosdaq": _ready_market(
                    "KOSDAQ",
                    "2026-07-20T13:38:49+09:00",
                    advancers=100,
                    unchanged=10,
                    decliners=800,
                ),
            },
        },
        fetched_at=datetime(2026, 7, 20, 4, 40, tzinfo=UTC),
    )

    merged = merge_live_market_regime_context(
        {"market": "KOSDAQ", "market_breadth": {"market": "KOSDAQ", "up_ratio": "0.90"}},
        live,
    )

    assert merged["market_breadth"]["market"] == "KOSDAQ"
    assert merged["market_breadth"]["up_ratio"] == "0.109890"


def test_merge_rejects_conflicting_base_market_identity() -> None:
    merged = merge_live_market_regime_context(
        {
            "market": "KOSPI",
            "market_breadth": {"market": "KOSDAQ", "up_ratio": "0.90"},
            "market_regime_context": {
                "market": "KOSDAQ",
                "market_breadth": {"market": "KOSDAQ", "up_ratio": "0.90"},
            },
        },
        {
            "market_context_status": "ready",
            "market_regime_context": {
                "source": "kai-trading-core:/market/snapshot",
                "markets": {
                    "KOSPI": {"market": "KOSPI", "up_ratio": "0.80"},
                    "KOSDAQ": {"market": "KOSDAQ", "up_ratio": "0.10"},
                },
            },
        },
    )

    assert "market_breadth" not in merged
    assert "market_breadth" not in merged["market_regime_context"]


def test_live_context_overrides_stale_breadth_but_preserves_sector_context() -> None:
    merged = merge_live_market_regime_context(
        {
            "market": "KOSPI",
            "market_regime_context": {
                "market_breadth": {"market": "KOSPI", "up_ratio": "0.80"},
                "sector_context": {"sector_name": "semiconductor", "relative_state": "weak"},
            }
        },
        {
            "market_breadth": {"market": "KOSPI", "up_ratio": "0.12"},
            "market_regime_context": {
                "market_breadth": {"market": "KOSPI", "up_ratio": "0.12"},
                "source": "kai-trading-core:/market/snapshot",
            },
        },
    )

    regime = merged["market_regime_context"]
    assert regime["market_breadth"]["up_ratio"] == "0.12"
    assert regime["sector_context"]["relative_state"] == "weak"
    assert merged["market_breadth"]["up_ratio"] == "0.12"


def test_live_market_context_cache_isolated_by_endpoint_and_auth(monkeypatch) -> None:
    calls: list[tuple[str | None, str | None]] = []

    def fake_fetch(*, settings: Settings) -> dict[str, object]:
        calls.append((settings.market_context_base_url, settings.market_context_bearer_token))
        return {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T05:00:00+00:00",
            "markets": {
                "kospi": {
                    "label": "KOSPI",
                    "advancers": len(calls),
                    "decliners": 10,
                }
            },
        }

    monkeypatch.setattr(live_market_context, "_fetch_market_snapshot", fake_fetch)
    with live_market_context._CACHE_LOCK:
        live_market_context._CACHE.update(
            {"loaded_at": None, "context": None, "error": None, "identity": None}
        )

    checked_at = datetime(2026, 7, 20, 5, 0, 1, tzinfo=UTC)
    common = {
        "market_context_enabled": True,
        "market_context_cache_ttl_seconds": 60,
    }
    first = Settings(
        **common,
        market_context_base_url="http://core-a.test",
        market_context_bearer_token="token-a",
    )
    second_endpoint = first.model_copy(
        update={"market_context_base_url": "http://core-b.test"}
    )
    second_auth = second_endpoint.model_copy(
        update={"market_context_bearer_token": "token-b"}
    )

    load_live_market_regime_context(settings=first, now=checked_at)
    load_live_market_regime_context(settings=first, now=checked_at)
    load_live_market_regime_context(settings=second_endpoint, now=checked_at)
    load_live_market_regime_context(settings=second_auth, now=checked_at)

    assert calls == [
        ("http://core-a.test", "token-a"),
        ("http://core-b.test", "token-a"),
        ("http://core-b.test", "token-b"),
    ]


def test_live_market_context_cache_never_outlives_provider_observation(monkeypatch) -> None:
    calls = 0

    def fake_fetch(*, settings: Settings) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:00:00+09:00",
            "markets": {
                "kospi": _ready_market("KOSPI", "2026-07-20T14:00:00+09:00")
            },
        }

    monkeypatch.setattr(live_market_context, "_fetch_market_snapshot", fake_fetch)
    with live_market_context._CACHE_LOCK:
        live_market_context._CACHE.update(
            {"loaded_at": None, "context": None, "error": None, "identity": None}
        )
    settings = Settings(
        market_context_enabled=True,
        market_context_base_url="http://core.test",
        market_context_cache_ttl_seconds=3600,
    )

    first = load_live_market_regime_context(
        settings=settings,
        now=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )
    second = load_live_market_regime_context(
        settings=settings,
        now=datetime(2026, 7, 20, 5, 6, 1, tzinfo=UTC),
    )

    assert first["market_regime_context"]["markets"]["KOSPI"]["up_count"] == 500
    assert second["market_regime_context"]["markets"] == {}
    assert calls == 2


def test_snapshot_uses_each_markets_provider_observation_timestamp() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T14:04:50+09:00",
                    advancers=700,
                    decliners=200,
                ),
                "kosdaq": _ready_market(
                    "KOSDAQ",
                    "2026-07-20T13:50:00+09:00",
                    advancers=100,
                    decliners=800,
                ),
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    markets = context["market_regime_context"]["markets"]
    assert set(markets) == {"KOSPI"}
    assert markets["KOSPI"]["observed_at"] == "2026-07-20T14:04:50+09:00"


@pytest.mark.parametrize(
    "invalidity",
    [
        "missing_completeness",
        "unverified_completeness",
        "count_mismatch",
        "raw_bucket_mismatch",
    ],
)
def test_snapshot_excludes_only_market_without_verified_breadth_completeness(
    invalidity: str,
) -> None:
    invalid_kospi = _ready_market("KOSPI", "2026-07-20T14:04:50+09:00")
    if invalidity == "missing_completeness":
        invalid_kospi.pop("breadth_completeness")
    elif invalidity == "unverified_completeness":
        invalid_kospi["breadth_completeness"] = {
            **invalid_kospi["breadth_completeness"],
            "status": "unverified",
        }
    elif invalidity == "count_mismatch":
        invalid_kospi["breadth_completeness"] = {
            **invalid_kospi["breadth_completeness"],
            "observed_symbol_count": 909,
        }
    else:
        invalid_kospi["breadth_raw_buckets"] = {
            **invalid_kospi["breadth_raw_buckets"],
            "advancers": 499,
        }

    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                "kospi": invalid_kospi,
                "kosdaq": _ready_market("KOSDAQ", "2026-07-20T14:04:50+09:00"),
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert set(context["market_regime_context"]["markets"]) == {"KOSDAQ"}


def test_snapshot_counts_disjoint_limit_buckets_in_market_breadth() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                "kospi": _ready_market(
                    "KOSPI",
                    "2026-07-20T14:04:50+09:00",
                    upper_limit_count=2,
                    advancers=100,
                    unchanged=10,
                    decliners=80,
                    lower_limit_count=3,
                )
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    breadth = context["market_regime_context"]["markets"]["KOSPI"]
    assert breadth["market_symbol_count"] == 195
    assert breadth["up_count"] == 102
    assert breadth["down_count"] == 83
    assert breadth["up_ratio"] == "0.523077"
    assert breadth["advance_decline_ratio"] == "1.228916"


@pytest.mark.parametrize(
    ("row_observed_at", "leg_observed_at"),
    [
        (None, "2026-07-20T14:04:50+09:00"),
        ("not-a-timestamp", "2026-07-20T14:04:50+09:00"),
        ("2026-07-20T14:04:50", "2026-07-20T14:04:50+09:00"),
        ("2026-07-20T14:04:50+09:00", None),
        ("2026-07-20T14:04:50+09:00", "2026-07-20T14:04:49+09:00"),
    ],
)
def test_snapshot_rejects_missing_invalid_or_conflicting_market_observation(
    row_observed_at: object,
    leg_observed_at: object,
) -> None:
    row = _ready_market("KOSPI", "2026-07-20T14:04:50+09:00")
    row["provider_observed_at"] = row_observed_at
    row["index_leg"] = {
        "status": "ready",
        "observation_source": "kis_index_intraday_bar",
        "provider_observed_at": leg_observed_at,
    }

    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {"kospi": row},
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


@pytest.mark.parametrize(
    "index_leg",
    [
        None,
        {},
        {"status": "unverified"},
        {
            "status": "ready",
            "observation_source": "untrusted_source",
            "provider_observed_at": "2026-07-20T14:04:50+09:00",
        },
    ],
)
def test_snapshot_requires_ready_provider_bound_index_leg(index_leg: object) -> None:
    row = _ready_market("KOSPI", "2026-07-20T14:04:50+09:00")
    row["index_leg"] = index_leg

    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {"kospi": row},
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_business_date", None),
        ("provider_business_date", "2026-07-19"),
        ("provider_session_time", None),
        ("provider_session_time", "14:04:49"),
    ],
)
def test_snapshot_rejects_incoherent_provider_date_or_session_metadata(
    field: str,
    value: object,
) -> None:
    row = _ready_market("KOSPI", "2026-07-20T14:04:50+09:00")
    row[field] = value

    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {"kospi": row},
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


@pytest.mark.parametrize(
    ("observed_at", "fetched_at"),
    [
        (
            "2026-07-20T14:05:31+09:00",
            datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
        ),
        (
            "2026-07-19T14:04:50+09:00",
            datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
        ),
        (
            "2026-07-20T15:31:00+09:00",
            datetime(2026, 7, 20, 6, 31, 30, tzinfo=UTC),
        ),
        (
            "2026-07-20T13:58:59+09:00",
            datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
        ),
        (
            "2026-07-18T10:00:00+09:00",
            datetime(2026, 7, 18, 1, 1, tzinfo=UTC),
        ),
    ],
)
def test_snapshot_rejects_future_wrong_date_out_of_session_stale_or_weekend_observations(
    observed_at: str,
    fetched_at: datetime,
) -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": observed_at,
            "markets": {"kospi": _ready_market("KOSPI", observed_at)},
        },
        fetched_at=fetched_at,
    )

    assert context["market_regime_context"]["markets"] == {}


@pytest.mark.parametrize(
    ("market_key", "market_label"),
    [
        ("kospi200", "KOSPI"),
        ("kospi/kosdaq", "KOSDAQ"),
        ("kospi", "KOSPI composite"),
        ("unknown", "KOSPI"),
    ],
)
def test_snapshot_rejects_ambiguous_or_unknown_market_identifiers(
    market_key: str,
    market_label: str,
) -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                market_key: _ready_market(
                    market_label,
                    "2026-07-20T14:04:50+09:00",
                )
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


def test_snapshot_rejects_duplicate_keys_for_the_same_canonical_market() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                "kospi": _ready_market("KOSPI", "2026-07-20T14:04:50+09:00"),
                "KOSPI": _ready_market("KOSPI", "2026-07-20T14:04:49+09:00"),
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}


def test_snapshot_rejects_duplicate_canonical_key_even_when_one_row_is_invalid() -> None:
    invalid_duplicate = _ready_market("KOSPI", "2026-07-20T14:04:49+09:00")
    invalid_duplicate["index_leg"] = None
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "missing_fields": [],
            "legs": {"index": {"status": "ready"}},
            "as_of": "2026-07-20T14:04:50+09:00",
            "markets": {
                "kospi": _ready_market("KOSPI", "2026-07-20T14:04:50+09:00"),
                "KOSPI": invalid_duplicate,
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 5, tzinfo=UTC),
    )

    assert context["market_regime_context"]["markets"] == {}
