from datetime import UTC, datetime

from app.services.live_market_context import (
    market_regime_context_from_market_snapshot,
    merge_live_market_regime_context,
)


def test_market_snapshot_converts_to_guardrail_breadth_context() -> None:
    context = market_regime_context_from_market_snapshot(
        {
            "status": "ready",
            "as_of": "2026-07-20T13:38:49+09:00",
            "markets": {
                "kospi": {
                    "label": "KOSPI",
                    "change_percent": -3.46,
                    "advancers": 114,
                    "unchanged": 12,
                    "decliners": 787,
                    "foreign_net_value": 158312000000,
                    "institution_net_value": -207487000000,
                },
                "kosdaq": {
                    "label": "KOSDAQ",
                    "change_percent": -4.63,
                    "advancers": 190,
                    "unchanged": 36,
                    "decliners": 1513,
                },
            },
        },
        fetched_at=datetime(2026, 7, 20, 5, 0, tzinfo=UTC),
    )

    breadth = context["market_breadth"]
    assert breadth["market"] == "KOSPI"
    assert breadth["up_count"] == 114
    assert breadth["down_count"] == 787
    assert breadth["up_ratio"] == "0.124863"
    assert breadth["advance_decline_ratio"] == "0.144854"
    assert breadth["index_change_percent"] == -3.46


def test_live_context_overrides_stale_breadth_but_preserves_sector_context() -> None:
    merged = merge_live_market_regime_context(
        {
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
