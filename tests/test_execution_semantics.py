from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.api.schemas.execution import ExecutionPlanItemRequest, PlaceOrderRequest
from app.services.execution_semantics import (
    INTRADAY_WATCH_BUY_INTENTS,
    canonical_krx_asset_class,
    is_exec_agent_sizing_policy,
    is_short_term_watch_buy_intent,
    is_trigger_time_exec_agent_sizing_policy,
    watch_buy_direction_from_payloads,
)
from app.core.config import Settings
from app.services.exec_agent_approval import configured_execution_cost_evidence


@pytest.mark.parametrize("intent", sorted(INTRADAY_WATCH_BUY_INTENTS))
def test_execution_plan_intake_canonicalizes_every_short_term_watch_buy_alias(
    intent: str,
) -> None:
    item = ExecutionPlanItemRequest(
        symbol="005930",
        side="buy",
        intent=intent,
        provider="kis",
        venue="krx",
        asset_class="stock",
    )

    assert item.intent == "intraday_watch_buy"
    assert is_short_term_watch_buy_intent(intent) is True


@pytest.mark.parametrize(
    "asset_class",
    ["stock", "equity", "kr_equity", "domestic_equity", "korean_equity"],
)
def test_krx_asset_class_aliases_have_one_canonical_value(asset_class: str) -> None:
    item = ExecutionPlanItemRequest(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        venue="krx",
        asset_class=asset_class,
    )
    order = PlaceOrderRequest(
        idempotency_key=f"asset-{asset_class}",
        provider="kis",
        asset_class=asset_class,
        exchange="krx",
        symbol="005930",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price="70000",
    )

    assert item.asset_class == "stock"
    assert order.asset_class == "stock"
    assert canonical_krx_asset_class(asset_class) == "stock"


def test_unknown_kis_krx_asset_class_fails_closed() -> None:
    with pytest.raises(ValidationError, match="unsupported KRX asset_class"):
        PlaceOrderRequest(
            idempotency_key="bad-asset",
            provider="kis",
            asset_class="stcok",
            exchange="krx",
            symbol="005930",
            side="buy",
            quantity=1,
            order_type="limit",
            limit_price="70000",
        )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"trigger_direction": "breakout"}, "at_or_above"),
        ({"entry_direction": "pullback"}, "at_or_below"),
        ({"tactical_action": "watch_for_trigger"}, "at_or_above"),
        ({"watch_buy_monitoring": {"direction": "at_or_above"}}, "at_or_above"),
        ({}, "at_or_below"),
    ],
)
def test_watch_buy_direction_is_resolved_consistently_from_persisted_payloads(
    payload: dict[str, object],
    expected: str,
) -> None:
    assert watch_buy_direction_from_payloads(payload) == expected


def test_pre_monitor_sizing_wins_over_conflicting_trigger_time_alias() -> None:
    policy = {
        "type": "trigger_time_exec_agent_sized",
        "kind": "exec_agent_sized",
    }

    assert is_exec_agent_sizing_policy(policy) is True
    assert is_trigger_time_exec_agent_sizing_policy(policy) is False


@pytest.mark.parametrize(
    "asset_class",
    ["stock", "equity", "kr_equity", "domestic_equity", "korean_equity"],
)
def test_krx_asset_aliases_use_identical_server_owned_cost_evidence(
    asset_class: str,
) -> None:
    evidence = configured_execution_cost_evidence(
        settings=Settings(runtime_mode="live"),
        provider="kis",
        asset_class=asset_class,
        exchange="KRX",
        live_quote_spread_bps="1.25",
        require_live_quote_spread=True,
    )

    context = evidence["live_execution_cost_context"]
    assert context["round_trip_fee_bps"] == 50
    assert context["expected_slippage_bps"] == "1.25"
    assert context["cost_evidence_source"] == (
        "settings:kis_krx_round_trip_non_spread_cost_bps"
        "+live_quote_spread_bps"
    )
