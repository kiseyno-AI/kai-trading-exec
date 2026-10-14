from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import BrokerIntradayLiquiditySnapshot, BrokerLiveMarketContext
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services import broker_orders, monitoring, trigger_time_exec_agent
from app.services.exec_agent_approval import (
    assess_buy_economics,
    configured_execution_cost_evidence,
    trusted_live_market_decimal,
)
from app.services.execution_semantics import INTRADAY_WATCH_BUY_INTENTS


EVALUATED_AT = datetime(2026, 6, 5, 0, 30, tzinfo=UTC)
ORDERBOOK_SOURCE = {
    "source": "broker_live_market_context",
    "freshness_source": "kis_exchange_orderbook_time",
    "observed_at": EVALUATED_AT.isoformat(),
}
LIQUIDITY_SOURCE = {
    "source": "broker_intraday_liquidity_snapshot",
    "freshness_source": "kis_exchange_intraday_minute_time",
    "observed_at": EVALUATED_AT.isoformat(),
    "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
    "tr_id": "FHKST03010200",
}


def _live_context() -> dict[str, object]:
    return {
        "symbol": "005930",
        "observed_at": EVALUATED_AT.isoformat(),
        "freshness_source": "kis_exchange_orderbook_time",
        "bid": "71045",
        "ask": "71055",
        "spread": "10",
        "spread_bps": "1.41",
        "orderbook_depth": "450000000",
        "orderbook_imbalance": "0.08",
        "volume": "1250000",
        "turnover": "3500000000",
        "day_high": "71200",
        "day_low": "70500",
        "session_type": "regular",
        "normal_ordering_available": True,
        "provenance": {
            "field_sources": {
                **{
                    field: dict(ORDERBOOK_SOURCE)
                    for field in (
                        "latest_price",
                        "bid",
                        "ask",
                        "spread",
                        "spread_bps",
                        "orderbook",
                        "orderbook_depth",
                        "orderbook_imbalance",
                    )
                },
                **{
                    field: dict(LIQUIDITY_SOURCE)
                    for field in ("volume", "turnover", "day_high", "day_low")
                },
            }
        },
    }


def _market_context(
    live_context: dict[str, object],
    *,
    side: str = "buy",
) -> tuple[dict[str, object], str | None]:
    plan = ExecutionPlan(
        id="plan",
        external_plan_id="plan",
        source="test",
        mode="live",
    )
    item = ExecutionPlanItem(
        id="item",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="005930",
        side=side,
        intent="intraday_watch_buy" if side == "buy" else "stop_sell",
        provider="kis",
        asset_class="stock",
        venue="KRX",
        risk_context={
            "market": "KOSPI",
            "exchange": "KRX",
            "session_policy": {
                "session": "regular",
                "timezone": "Asia/Seoul",
                "allow_extended_hours": False,
            },
        },
    )
    return trigger_time_exec_agent._market_context(
        settings=Settings(
            runtime_mode="live",
            monitoring_max_quote_age_seconds=2,
            monitoring_max_quote_future_skew_seconds=1,
        ),
        item=item,
        observed_price=Decimal("71055"),
        observed_at=EVALUATED_AT,
        triggered_at=EVALUATED_AT,
        trigger_reason="test",
        trigger_metadata=None,
        live_market_context=live_context,
    )


@pytest.mark.parametrize("field", ["day_high", "day_low"])
@pytest.mark.parametrize(
    ("tamper_key", "tamper_value"),
    [
        ("bound_endpoint", None),
        ("bound_tr_id", "WRONG"),
        ("binding", None),
    ],
)
def test_bound_current_price_day_range_keeps_exact_intraday_freshness_proof(
    field: str,
    tamper_key: str,
    tamper_value: str | None,
) -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
    )
    evidence = {
        "available": True,
        "source": "kis_exchange_intraday_minute_time",
        "provider_source": "broker_intraday_liquidity_snapshot",
        "observed_at": EVALUATED_AT.isoformat(),
        "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-price",
        "tr_id": "FHKST01010100",
        "bound_endpoint": (
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        ),
        "bound_tr_id": "FHKST03010200",
        "binding": (
            "same_symbol_monotonic_cumulative_totals_and_containing_minute_range"
        ),
    }

    assert (
        trigger_time_exec_agent._live_buy_field_evidence_reason(
            settings=Settings(runtime_mode="live"),
            item=item,
            field=field,
            evidence=evidence,
            evaluated_at=EVALUATED_AT,
        )
        is None
    )

    tampered_evidence = dict(evidence)
    if tamper_value is None:
        tampered_evidence.pop(tamper_key)
    else:
        tampered_evidence[tamper_key] = tamper_value
    assert trigger_time_exec_agent._live_buy_field_evidence_reason(
        settings=Settings(runtime_mode="live"),
        item=item,
        field=field,
        evidence=tampered_evidence,
        evaluated_at=EVALUATED_AT,
    ) == f"{field} evidence source identity is not trusted"


def _core_live_breadth_context(
    *,
    up_count: int,
    down_count: int,
    status: str = "ready",
    observed_at: datetime = EVALUATED_AT,
) -> dict[str, object]:
    flat_count = 50
    symbol_count = up_count + down_count + flat_count
    return {
        "market_context_status": status,
        "market_context_as_of": observed_at.isoformat(),
        "market_context_fetched_at": EVALUATED_AT.isoformat(),
        "market_regime_context": {
            "source": "kai-trading-core:/market/snapshot",
            "markets": {
                "KOSPI": {
                    "market": "KOSPI",
                    "market_date": observed_at.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat(),
                    "observed_at": observed_at.isoformat(),
                    "source": "kai-trading-core:/market/snapshot",
                    "up_count": up_count,
                    "down_count": down_count,
                    "flat_count": flat_count,
                    "market_symbol_count": symbol_count,
                    "up_ratio": f"{up_count / symbol_count:.6f}",
                    "advance_decline_ratio": f"{up_count / down_count:.6f}",
                }
            },
        },
    }


@pytest.fixture(autouse=True)
def _fresh_normal_core_market_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: _core_live_breadth_context(up_count=650, down_count=250),
    )


def test_live_trigger_context_accepts_field_specific_exchange_evidence() -> None:
    context, _reason = _market_context(_live_context())

    assert context["live_execution_context_available"] is True
    assert context["invalid_critical_fields"] == []
    assert context["field_freshness"]["turnover"]["source"] == (
        "kis_exchange_intraday_minute_time"
    )
    assert context["field_freshness"]["day_high"]["valid"] is True


@pytest.mark.parametrize(
    ("field", "override"),
    [
        ("bid", {"freshness_source": "kis_exchange_intraday_minute_time"}),
        ("turnover", {"freshness_source": "kis_exchange_orderbook_time"}),
        (
            "volume",
            {"observed_at": (EVALUATED_AT - timedelta(seconds=3)).isoformat()},
        ),
        ("day_high", {"freshness_source": "transport_received_at_untrusted"}),
    ],
)
def test_live_trigger_context_rejects_wrong_or_stale_per_field_evidence(
    field: str,
    override: dict[str, object],
) -> None:
    live_context = _live_context()
    field_sources = live_context["provenance"]["field_sources"]
    field_sources[field] = {**field_sources[field], **override}

    context, _reason = _market_context(live_context)

    assert context["live_execution_context_available"] is False
    assert field in context["invalid_critical_fields"]
    assert context["field_freshness"][field]["valid"] is False


def test_live_trigger_context_does_not_use_orderbook_time_for_intraday_range() -> None:
    live_context = _live_context()
    live_context["provenance"]["field_sources"].pop("day_low")

    context, _reason = _market_context(live_context)

    assert context["live_execution_context_available"] is False
    assert "day_low" in context["invalid_critical_fields"]


@pytest.mark.parametrize("session_type", ["regular", "krx_regular", "nxt_main"])
def test_trigger_time_buy_blocks_fresh_live_risk_off_breadth(
    monkeypatch: pytest.MonkeyPatch,
    session_type: str,
) -> None:
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: _core_live_breadth_context(up_count=200, down_count=700),
    )

    live_context = _live_context()
    live_context["session_type"] = session_type
    context, reason = _market_context(live_context)

    assert reason == "fresh live market breadth is risk_off for trigger-time buy"
    assert context["live_market_buy_authority_gate"]["status"] == "blocked"
    assert context["live_market_buy_authority_gate"]["reason_code"] == (
        "market_breadth_risk_off"
    )
    assert context["live_execution_context_available"] is False


@pytest.mark.parametrize(
    ("core_context", "expected_reason"),
    [
        (
            _core_live_breadth_context(up_count=200, down_count=700),
            "fresh live market breadth is risk_off for trigger-time buy",
        ),
        (
            {"market_context_status": "unavailable"},
            "fresh live market breadth is unavailable for trigger-time buy",
        ),
        (
            {
                "market_context_status": "stale",
                "market_context_stale_reason": "provider observation expired",
            },
            "fresh live market breadth is unavailable for trigger-time buy",
        ),
    ],
)
def test_trigger_time_resolution_quarantines_unsafe_buy_before_agent_or_account_load(
    monkeypatch: pytest.MonkeyPatch,
    core_context: dict[str, object],
    expected_reason: str,
) -> None:
    class EmptySession:
        @staticmethod
        def scalars(_statement: object) -> object:
            return type("EmptyScalarResult", (), {"all": lambda self: []})()

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: core_context,
    )
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "_request_exec_agent_decision",
        lambda **_kwargs: pytest.fail("risk-off BUY must not reach the exec agent"),
    )
    plan = ExecutionPlan(
        id="resolution-plan",
        external_plan_id="resolution-plan",
        source="test",
        mode="live",
    )
    item = ExecutionPlanItem(
        id="resolution-item",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        venue="KRX",
        risk_context={
            "market": "KOSPI",
            "exchange": "KRX",
            "session_policy": {
                "session": "regular",
                "timezone": "Asia/Seoul",
                "allow_extended_hours": False,
            },
        },
    )

    resolution = trigger_time_exec_agent.resolve_trigger_time_exec_agent_decision(
        settings=Settings(runtime_mode="live"),
        db=EmptySession(),
        adapter=object(),
        item=item,
        observed_price=Decimal("71055"),
        observed_at=EVALUATED_AT,
        triggered_at=EVALUATED_AT,
        trigger_reason="test",
        trigger_metadata=None,
        intended_quantity=1,
        live_market_context=_live_context(),
    )

    assert resolution.decision.decision == "delay"
    assert resolution.source == "exec_runtime_context_gate"
    assert resolution.decision.context_staleness_ok is False
    assert resolution.decision.reason == expected_reason


@pytest.mark.parametrize("status", ["unavailable", "stale"])
def test_trigger_time_buy_fails_closed_without_fresh_live_breadth(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: {
            "market_context_status": status,
            "market_context_stale_reason": (
                "provider observation expired" if status == "stale" else None
            ),
        },
    )

    context, reason = _market_context(_live_context())

    assert reason == "fresh live market breadth is unavailable for trigger-time buy"
    assert context["live_market_buy_authority_gate"]["status"] == "blocked"
    assert context["live_market_buy_authority_gate"]["reason_code"] == (
        "live_market_breadth_unavailable"
    )
    assert context["live_execution_context_available"] is False


def test_trigger_time_buy_fails_closed_when_ready_live_breadth_is_too_old(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: _core_live_breadth_context(
            up_count=650,
            down_count=250,
            status="ready",
            observed_at=EVALUATED_AT - timedelta(minutes=10),
        ),
    )

    context, reason = _market_context(_live_context())

    assert reason == "fresh live market breadth is unavailable for trigger-time buy"
    assert context["live_market_buy_authority_gate"]["status"] == "blocked"
    assert context["live_market_buy_authority_gate"]["reason_code"] == (
        "live_market_breadth_unavailable"
    )
    assert context["live_execution_context_available"] is False


def test_trigger_time_protective_sell_ignores_live_breadth_unavailability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **_kwargs: {"market_context_status": "unavailable"},
    )

    context, reason = _market_context(_live_context(), side="sell")

    assert reason is None
    assert context["live_market_buy_authority_gate"]["status"] == "not_applicable"
    assert context["live_execution_context_available"] is True


@pytest.mark.parametrize(
    ("target", "expected_reason"),
    [
        ("119.399", "net_reward_risk_below_minimum"),
        ("119.400", None),
    ],
)
def test_live_short_term_net_reward_risk_floor_is_cost_adjusted(
    target: str,
    expected_reason: str | None,
) -> None:
    settings = Settings(runtime_mode="live")
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
    )

    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal(target),
        stop_price=Decimal("90"),
        minimum_reward_risk=monitoring._minimum_reward_risk_for_watch_buy(
            settings=settings,
            item=item,
        ),
        evidence=configured_execution_cost_evidence(
            settings=settings,
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            live_quote_spread_bps="0",
            require_live_quote_spread=True,
        ),
        require_cost_evidence=True,
    )

    assert assessment.reason == expected_reason
    assert assessment.minimum_reward_risk == Decimal("1.80")


@pytest.mark.parametrize("tamper", ["stale", "symbol"])
def test_final_broker_turnover_proof_rejects_stale_or_mismatched_identity(
    tamper: str,
) -> None:
    observed_at = (
        EVALUATED_AT - timedelta(seconds=3)
        if tamper == "stale"
        else EVALUATED_AT
    )
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
        quantity_policy={
            "exec_trigger_time_liquidity_evidence_v1": {
                "version": 1,
                "provider": "kis",
                "asset_class": "stock",
                "exchange": "KRX",
                "symbol": "OTHER" if tamper == "symbol" else "005930",
                "observed_at": observed_at.isoformat(),
                "freshness_source": "kis_exchange_intraday_minute_time",
                "endpoint": (
                    "/uapi/domestic-stock/v1/quotations/"
                    "inquire-time-itemchartprice"
                ),
                "tr_id": "FHKST03010200",
                "turnover": "3500000000",
            }
        },
    )
    context = broker_orders._broker_delegated_gate_market_context(
        settings=Settings(
            runtime_mode="live",
            monitoring_max_quote_age_seconds=2,
            monitoring_max_quote_future_skew_seconds=1,
        ),
        evaluated_at=EVALUATED_AT,
        plan_item=item,
    )

    assert trusted_live_market_decimal(
        context,
        field="turnover",
        evaluated_at=EVALUATED_AT,
    ) is None


def test_final_broker_never_promotes_caller_persisted_liquidity_claim() -> None:
    item = ExecutionPlanItem(
        id="forged-liquidity-item",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
        quantity_policy={
            "exec_trigger_time_liquidity_evidence_v1": {
                "version": 1,
                "provider": "kis",
                "asset_class": "stock",
                "exchange": "KRX",
                "symbol": "005930",
                "observed_at": EVALUATED_AT.isoformat(),
                "freshness_source": "kis_exchange_intraday_minute_time",
                "endpoint": (
                    "/uapi/domestic-stock/v1/quotations/"
                    "inquire-time-itemchartprice"
                ),
                "tr_id": "FHKST03010200",
                "turnover": "999999999999",
            }
        },
    )

    context = broker_orders._broker_delegated_gate_market_context(
        settings=Settings(runtime_mode="live"),
        evaluated_at=EVALUATED_AT,
        plan_item=item,
        liquidity_snapshot=None,
    )

    assert trusted_live_market_decimal(
        context,
        field="turnover",
        evaluated_at=EVALUATED_AT,
    ) is None


@pytest.mark.parametrize("intent", sorted(INTRADAY_WATCH_BUY_INTENTS))
def test_every_legacy_short_term_buy_alias_hits_final_turnover_floor(
    intent: str,
) -> None:
    plan = ExecutionPlan(
        id=f"legacy-{intent}-plan",
        external_plan_id=f"legacy-{intent}-plan",
        source="test",
        mode="live",
        market_date=EVALUATED_AT.astimezone(ZoneInfo("Asia/Seoul")).date(),
    )
    item = ExecutionPlanItem(
        id=f"legacy-{intent}",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="005930",
        side="buy",
        intent=intent,
        provider="kis",
        asset_class="stock",
        trigger_price=Decimal("100"),
        risk_context={
            "trigger_direction": "at_or_above",
            "target_price": "120",
            "stop_price": "90",
        },
    )
    command = PlaceOrderRequest(
        idempotency_key=f"legacy-{intent}",
        execution_plan_item_id=item.id,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price="100",
    )
    evidence = broker_orders.AutomatedKisBuyMarketEvidence(
        is_domestic=True,
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=EVALUATED_AT,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            normal_ordering_available=True,
            freshness_source="kis_exchange_orderbook_time",
        ),
        liquidity_snapshot=BrokerIntradayLiquiditySnapshot(
            symbol="005930",
            exchange="KRX",
            observed_at=EVALUATED_AT,
            volume=Decimal("10"),
            turnover=Decimal("2999999999"),
            day_high=Decimal("101"),
            day_low=Decimal("99"),
            payload={
                "provider": "kis",
                "endpoint": (
                    "/uapi/domestic-stock/v1/quotations/"
                    "inquire-time-itemchartprice"
                ),
                "tr_id": "FHKST03010200",
            },
            freshness_source="kis_exchange_intraday_minute_time",
        ),
    )

    failure = broker_orders._automated_kis_buy_market_evidence_failure(
        settings=Settings(runtime_mode="live"),
        command=command,
        plan_item=item,
        evidence=evidence,
        reference_at=EVALUATED_AT,
    )

    assert failure is not None
    assert failure[0] == "live_buy_short_term_turnover_below_minimum"


def test_intraday_buy_final_gate_rejects_future_plan_market_date() -> None:
    local_market_date = EVALUATED_AT.astimezone(ZoneInfo("Asia/Seoul")).date()
    plan = ExecutionPlan(
        id="future-intraday-plan",
        external_plan_id="future-intraday-plan",
        source="kai-trading-core",
        mode="live",
        market_date=local_market_date + timedelta(days=1),
    )
    item = ExecutionPlanItem(
        id="future-intraday-item",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        trigger_price=Decimal("100"),
        risk_context={
            "trigger_direction": "at_or_above",
            "target_price": "120",
            "stop_price": "90",
        },
    )
    command = PlaceOrderRequest(
        idempotency_key="future-intraday-final-gate",
        execution_plan_item_id=item.id,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price="100",
    )
    evidence = broker_orders.AutomatedKisBuyMarketEvidence(
        is_domestic=True,
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=EVALUATED_AT,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            normal_ordering_available=True,
            freshness_source="kis_exchange_orderbook_time",
        ),
        liquidity_snapshot=None,
    )

    failure = broker_orders._automated_kis_buy_market_evidence_failure(
        settings=Settings(runtime_mode="live"),
        command=command,
        plan_item=item,
        evidence=evidence,
        reference_at=EVALUATED_AT,
    )

    assert failure == (
        "live_buy_plan_market_date_mismatch",
        "automated KIS buy plan is not authorized for the current market date",
    )


def test_intraday_buy_final_gate_rejects_expired_plan_market_date() -> None:
    local_market_date = EVALUATED_AT.astimezone(ZoneInfo("Asia/Seoul")).date()
    plan = ExecutionPlan(
        id="expired-intraday-plan",
        external_plan_id="expired-intraday-plan",
        source="kai-trading-core",
        mode="live",
        market_date=local_market_date - timedelta(days=1),
    )
    item = ExecutionPlanItem(
        id="expired-intraday-item",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        trigger_price=Decimal("100"),
        risk_context={
            "trigger_direction": "at_or_above",
            "target_price": "120",
            "stop_price": "90",
        },
    )
    command = PlaceOrderRequest(
        idempotency_key="expired-intraday-final-gate",
        execution_plan_item_id=item.id,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price="100",
    )
    evidence = broker_orders.AutomatedKisBuyMarketEvidence(
        is_domestic=True,
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=EVALUATED_AT,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            normal_ordering_available=True,
            freshness_source="kis_exchange_orderbook_time",
        ),
        liquidity_snapshot=None,
    )

    failure = broker_orders._automated_kis_buy_market_evidence_failure(
        settings=Settings(runtime_mode="live"),
        command=command,
        plan_item=item,
        evidence=evidence,
        reference_at=EVALUATED_AT,
    )

    assert failure == (
        "live_buy_plan_market_date_mismatch",
        "automated KIS buy plan is not authorized for the current market date",
    )


def test_kis_us_buy_final_gate_rejects_future_new_york_market_date() -> None:
    evaluated_at = datetime(2026, 9, 11, 15, 30, tzinfo=UTC)
    plan = ExecutionPlan(
        id="future-us-plan",
        external_plan_id="future-us-plan",
        source="kai-trading-core",
        mode="live",
        market_date=date(2026, 9, 12),
    )
    item = ExecutionPlanItem(
        id="future-us-item",
        execution_plan_id=plan.id,
        plan=plan,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        risk_context={
            "exchange": "NASD",
            "trigger_direction": "at_or_above",
            "target_price": "120",
            "stop_price": "90",
        },
    )
    command = PlaceOrderRequest(
        idempotency_key="future-us-final-gate",
        execution_plan_item_id=item.id,
        provider="kis",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price="100",
    )
    evidence = broker_orders.AutomatedKisBuyMarketEvidence(
        is_domestic=False,
        context=BrokerLiveMarketContext(
            symbol="AAPL",
            exchange="NASD",
            observed_at=evaluated_at,
            bid=Decimal("99.99"),
            ask=Decimal("100"),
            normal_ordering_available=True,
            freshness_source="kis_overseas_exchange_orderbook_time",
        ),
        liquidity_snapshot=None,
    )

    failure = broker_orders._automated_kis_buy_market_evidence_failure(
        settings=Settings(runtime_mode="live"),
        command=command,
        plan_item=item,
        evidence=evidence,
        reference_at=evaluated_at,
    )

    assert evaluated_at.astimezone(ZoneInfo("Asia/Seoul")).date() == date(2026, 9, 12)
    assert evaluated_at.astimezone(ZoneInfo("America/New_York")).date() == date(
        2026, 9, 11
    )
    assert failure == (
        "live_buy_plan_market_date_mismatch",
        "automated KIS buy plan is not authorized for the current market date",
    )
