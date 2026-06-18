from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.broker.base import BrokerAdapterError, BrokerConfig
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition, MonitoringCheckpoint
from app.services import trigger_time_exec_agent
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StubBrokerAdapter,
    build_order_placement,
    build_test_app,
    build_token_grant,
)


def _build_trigger_agent_app(
    *,
    monkeypatch: pytest.MonkeyPatch,
    agent_responses: list[dict[str, object]] | None = None,
    broker_adapter: StubBrokerAdapter | None = None,
):
    captured_requests: list[dict[str, object]] = []
    responses = list(agent_responses or [])

    def fake_agent_request(*, settings: Settings, payload: dict[str, object]) -> dict[str, object]:
        del settings
        captured_requests.append(payload)
        if not responses:
            raise AssertionError("unexpected trigger-time exec-agent call")
        return responses.pop(0)

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "_request_exec_agent_decision",
        fake_agent_request,
    )
    adapter = broker_adapter or StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-trigger-agent-1"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            exec_agent_enabled=True,
            exec_agent_trigger_time_enabled=True,
            exec_agent_trigger_time_sell_enabled=True,
            exec_agent_max_position_allocation_pct=Decimal("0.30"),
            exec_agent_max_symbol_total_exposure_pct=Decimal("0.30"),
        ),
        broker_adapter=adapter,
    )
    client = TestClient(app)
    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable trigger-time agent tests"},
    )
    assert resume.status_code == 200
    return app, engine, client, adapter, captured_requests


def _create_intraday_watch_buy(
    client: TestClient,
    *,
    plan_id: str,
    max_safe_notional_krw: str = "300000",
    risk_context_extra: dict[str, object] | None = None,
) -> str:
    risk_context = {
        "exchange": "KRX",
        "target_price": "72420.00",
        "stop_price": "70000.00",
        "trigger_direction": "at_or_above",
        "decider_output": {"rationale": "trigger-time test candidate"},
    }
    if risk_context_extra:
        risk_context.update(risk_context_extra)
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": plan_id,
            "source": "kai-trading-core",
            "market_date": "2026-06-05",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "quantity_policy": {
                        "type": "exec_agent_sized",
                        "budget_krw": "0",
                        "max_safe_notional_krw": max_safe_notional_krw,
                    },
                    "trigger_price": "71000.00",
                    "time_in_force": "day",
                    "risk_context": risk_context,
                }
            ],
        },
    )
    assert response.status_code == 201
    return response.json()["persisted_execution_item_ids"][0]


def _live_price_payload(
    price: str = "71050.00",
    *,
    day_low: str | None = None,
    day_high: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "005930",
        "price": price,
        "bid": "71045.00",
        "ask": "71055.00",
        "spread": "10.00",
        "spread_bps": "1.41",
        "orderbook_depth": "450000000",
        "orderbook_imbalance": "0.08",
        "volume": "1250000",
        "turnover": "88750000000",
        "session_type": "regular",
        "normal_ordering_available": True,
        "provenance": {"source": "test_live_market_quality"},
    }
    if day_low is not None:
        payload["day_low"] = day_low
    if day_high is not None:
        payload["day_high"] = day_high
    return payload


def test_trigger_time_buy_agent_context_approval_quantity_drives_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "buy",
                "quantity": 3,
                "limit_price": "71075.00",
                "updated_target_price": "72500.00",
                "updated_stop_price": "69950.00",
                "reason": "fresh trigger context supports a small buy",
                "risk_notes": ["spread acceptable"],
                "confidence": "0.72",
                "context_staleness_ok": True,
            }
        ],
    )
    item_id = _create_intraday_watch_buy(client, plan_id="plan-trigger-agent-buy-approval")

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger buy with agent",
            "prices": [_live_price_payload()],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert len(captured) == 1
    request = captured[0]
    assert request["signal_context"]["side"] == "buy"
    assert request["signal_context"]["execution_plan_item_id"] == item_id
    assert request["market_context"]["latest_price"] == "71050.0000"
    assert request["market_context"]["context_staleness_ok"] is True
    assert request["market_context"]["live_execution_context_available"] is True
    assert request["market_context"]["missing_critical_fields"] == []
    assert request["market_context"]["bid"] == "71045.0000"
    assert request["market_context"]["ask"] == "71055.0000"
    assert request["market_context"]["orderbook_depth"] == "450000000.0000"
    assert request["market_context"]["volume"] == "1250000.0000"
    assert request["market_context"]["turnover"] == "88750000000.0000"
    assert request["account_context"]["orderable_cash_krw"] == "1000000.0000"
    assert request["execution_context"]["intended_quantity"] is None

    assert len(broker_adapter.order_calls) == 1
    order_call = broker_adapter.order_calls[0]
    assert order_call.execution_plan_item_id == item_id
    assert order_call.side == "buy"
    assert order_call.quantity == 3
    assert order_call.limit_price == Decimal("71075.00")

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_submitted"
        assert item.limit_price == Decimal("71075.0000")
        assert item.risk_context["target_price"] == "72500.0000"
        assert item.risk_context["stop_price"] == "69950.0000"
        approved_events = [
            event
            for event in item.risk_context["monitoring_events"]
            if event["event"] == "trigger_time_exec_agent_approved"
        ]
        approved_event = approved_events[-1]
        assert approved_event["event"] == "trigger_time_exec_agent_approved"
        assert approved_event["market_quality_gate"]["available"] is True
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["decision"]["decision"] == "execute_now"
        assert interaction["order"]["quantity"] == 3

    engine.dispose()


def test_trigger_time_buy_agent_is_bypassed_for_default_entry_line_buy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-default-entry-line-bypass",
            "source": "kai-trading-core",
            "market_date": "2026-06-05",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 2},
                    "trigger_price": "71000.00",
                    "chase_limit_price": "71200.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "72420.00",
                        "stop_price": "70000.00",
                        "trigger_direction": "at_or_below",
                    },
                }
            ],
        },
    )
    assert response.status_code == 201
    item_id = response.json()["persisted_execution_item_ids"][0]

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "entry line buy should not call trigger-time agent",
            "prices": [_live_price_payload(price="70900.00")],
        },
    )

    assert result.status_code == 200
    payload = result.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["direction"] == "at_or_below"
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].quantity == 2
    assert broker_adapter.order_calls[0].limit_price == Decimal("70900.00")

    engine.dispose()


def test_trigger_time_buy_agent_is_bypassed_for_intraday_pullback_below_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-pullback-bypass",
        risk_context_extra={"exec_agent_watch_sizing_fallback_allowed": True},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross above chase persists before pullback",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="70000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0
    assert crossed.json()["items"][0]["block_reason"] in {
        "price_above_chase_limit",
        "watch_buy_intraday_range_chase_block",
    }

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high crossed trigger and pulled back below target buy",
            "prices": [
                _live_price_payload(
                    price="70900.00",
                    day_low="70000.00",
                    day_high="71400.00",
                )
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["trigger_reason"] == "intraday_watch_buy_day_high_cross"
    assert captured == []

    assert len(broker_adapter.order_calls) == 1
    order_call = broker_adapter.order_calls[0]
    assert order_call.execution_plan_item_id == item_id
    assert order_call.side == "buy"
    assert order_call.limit_price == Decimal("70900.00")

    engine.dispose()


def test_trigger_time_pullback_bypass_still_delays_without_live_market_quality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-pullback-bypass-missing-quality",
        risk_context_extra={"exec_agent_watch_sizing_fallback_allowed": True},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross above chase persists before missing-quality pullback",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="70000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "pullback lacks live quality even though trigger cross is persisted",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "70900.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "agent_delay"
    assert captured == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "agent_delay"
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["source"] == "exec_runtime_market_quality_gate"
        assert interaction["decision"]["decision"] == "delay"
        assert "bid" in interaction["request_payload"]["market_context"]["missing_critical_fields"]

    engine.dispose()


def test_trigger_time_buy_missing_live_market_quality_delays_without_agent_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "buy",
                "quantity": 3,
                "reason": "should not be called through missing live quality gate",
                "context_staleness_ok": True,
            }
        ],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-buy-missing-live-quality",
        risk_context_extra={
            "bid": "71044.00",
            "ask": "71056.00",
            "orderbook_depth": "999999999",
            "volume": "987654",
            "turnover": "70123456789",
            "session_type": "regular",
            "normal_ordering_available": True,
            "market_data": {
                "bid": "71043.00",
                "ask": "71057.00",
                "orderbook_depth": "888888888",
                "volume": "876543",
                "turnover": "61123456789",
                "session_type": "regular",
                "normal_ordering_available": True,
            },
        },
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "missing live quality should delay",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "agent_delay"
    assert captured == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "agent_delay"
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["source"] == "exec_runtime_market_quality_gate"
        assert interaction["decision"]["decision"] == "delay"
        market_context = interaction["request_payload"]["market_context"]
        assert market_context["live_execution_context_available"] is False
        assert set(market_context["missing_critical_fields"]) == {
            "bid",
            "ask",
            "orderbook",
            "volume",
            "turnover",
            "session_type",
            "normal_ordering_available",
        }
        assert market_context["bid"] is None
        assert market_context["ask"] is None
        assert market_context["orderbook_depth"] is None
        assert market_context["volume"] is None
        assert market_context["turnover"] is None
        assert market_context["field_freshness"]["bid"]["source"] is None
        assert market_context["raw_market_payload"]["bid"] == "71043.00"
        delay_events = [
            event
            for event in item.risk_context["monitoring_events"]
            if event["event"] == "trigger_time_exec_agent_delay"
        ]
        assert delay_events[-1]["market_quality_gate"]["available"] is False
        assert "bid" in delay_events[-1]["market_quality_gate"]["missing_fields"]

    engine.dispose()


def test_trigger_time_buy_invalid_agent_reason_is_bounded_and_delays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    long_reason = "agent timeout " + ("x" * 2000)
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "delay",
                "side": "buy",
                "reason": long_reason,
                "context_staleness_ok": True,
            }
        ],
    )
    item_id = _create_intraday_watch_buy(client, plan_id="plan-trigger-agent-long-reason-delay")

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "agent response has overlong reason",
            "prices": [_live_price_payload()],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "agent_delay"
    assert len(captured) == 1
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "agent_delay"
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["source"] == "exec_runtime_context_gate"
        assert interaction["decision"]["decision"] == "delay"
        assert len(interaction["decision"]["reason"]) <= 1000
        assert "String should have at most 1000 characters" in interaction["error"]

    engine.dispose()


def test_trigger_time_buy_agent_quantity_is_clamped_by_deterministic_sizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "buy",
                "quantity": 10,
                "limit_price": "71075.00",
                "reason": "agent wants a larger tactical entry",
                "risk_notes": ["operator should still apply deterministic caps"],
                "confidence": "0.72",
                "context_staleness_ok": True,
            }
        ],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-buy-deterministic-clamp",
        max_safe_notional_krw="150000",
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "oversized trigger-time agent buy should be risk clamped",
            "prices": [_live_price_payload()],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert len(broker_adapter.order_calls) == 1
    order_call = broker_adapter.order_calls[0]
    assert order_call.execution_plan_item_id == item_id
    assert order_call.quantity == 2

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_submitted"
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["decision"]["quantity"] == 10
        assert interaction["order"]["quantity"] == 2
        deterministic_sizing = interaction["deterministic_sizing"]
        assert deterministic_sizing["requested_quantity"] == 10
        assert deterministic_sizing["approved_quantity"] == 2
        assert deterministic_sizing["adjustment"] == "clamped_by_deterministic_risk_sizing"
        sizing_events = [
            event
            for event in item.risk_context["monitoring_events"]
            if event["event"] == "watch_buy_risk_sized"
        ]
        assert sizing_events[-1]["sizing_source"] == "trigger_time_exec_agent_approval"
        assert sizing_events[-1]["trigger_time_exec_agent_quantity"] == 10
        assert sizing_events[-1]["quantity"] == 2

    engine.dispose()


def test_trigger_time_buy_effective_risk_stop_updates_stop_monitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "buy",
                "quantity": 10,
                "limit_price": "71075.00",
                "updated_stop_price": "60000.00",
                "reason": "agent stop is too wide but buy can proceed with deterministic risk stop",
                "risk_notes": ["deterministic sizing should tighten the actual monitor stop"],
                "confidence": "0.72",
                "context_staleness_ok": True,
            }
        ],
    )
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-effective-stop-monitor",
            "source": "kai-trading-core",
            "market_date": "2026-06-05",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "quantity_policy": {
                        "type": "exec_agent_sized",
                        "budget_krw": "0",
                        "max_safe_notional_krw": "300000",
                    },
                    "trigger_price": "71000.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "72420.00",
                        "stop_price": "60000.00",
                        "trigger_direction": "at_or_above",
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "60000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, stop_item_id = response.json()["persisted_execution_item_ids"]

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "effective stop should be propagated before broker submission",
            "prices": [_live_price_payload()],
        },
    )

    assert result.status_code == 200
    assert result.json()["submitted_order_count"] == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == buy_item_id

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert buy_item is not None
        assert stop_item is not None
        assert stop_item.trigger_price == Decimal("68208.0000")
        assert buy_item.risk_context["effective_stop_price"] == "68208.0000"
        assert buy_item.risk_context["upstream_stop_price"] == "60000.0000"
        assert buy_item.risk_context["stop_monitor_adjustment"] == "raised_to_effective_risk_stop"
        sizing_events = [
            event
            for event in buy_item.risk_context["monitoring_events"]
            if event["event"] == "watch_buy_risk_sized"
        ]
        assert sizing_events[-1]["stop_monitor_adjustment"] == "raised_to_effective_risk_stop"

    engine.dispose()


@pytest.mark.parametrize(
    ("agent_response", "expected_status", "expected_outcome", "expected_count_key"),
    [
        (
            {
                "decision": "delay",
                "side": "buy",
                "valid_until": "2026-06-05T01:05:00Z",
                "reason": "spread widened after trigger",
                "risk_notes": ["wait for spread"],
                "confidence": "0.40",
                "context_staleness_ok": True,
            },
            "agent_delay",
            "agent_delay",
            "agent_delayed",
        ),
        (
            {
                "decision": "reject",
                "side": "buy",
                "reason": "liquidity disappeared",
                "risk_notes": ["thin book"],
                "confidence": "0.20",
                "context_staleness_ok": True,
            },
            "agent_rejected",
            "rejected",
            "agent_rejected",
        ),
    ],
)
def test_trigger_time_buy_delay_or_reject_does_not_submit_and_is_audited(
    monkeypatch: pytest.MonkeyPatch,
    agent_response: dict[str, object],
    expected_status: str,
    expected_outcome: str,
    expected_count_key: str,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[agent_response],
    )
    item_id = _create_intraday_watch_buy(client, plan_id=f"plan-trigger-agent-{expected_status}")

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger buy agent no-submit",
            "prices": [_live_price_payload()],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == expected_outcome
    assert broker_adapter.order_calls == []
    assert len(captured) == 1

    runtime_status = client.get("/runtime/status").json()
    assert runtime_status["liveness"]["monitoring"]["agent_decisions"][expected_count_key] == 1

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == expected_status
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["decision"]["decision"] == agent_response["decision"]
        assert interaction["order"] is None if "order" in interaction else True

    engine.dispose()


def test_trigger_time_missing_account_context_delays_without_agent_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-never-used"),
        account_snapshot_error=BrokerAdapterError("account unavailable"),
    )
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        broker_adapter=broker_adapter,
    )
    item_id = _create_intraday_watch_buy(client, plan_id="plan-trigger-agent-account-delay")

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "missing account should delay",
            "prices": [_live_price_payload()],
        },
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["outcome"] == "agent_delay"
    assert captured == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["source"] == "exec_runtime_context_gate"
        assert interaction["decision"]["decision"] == "delay"
        assert interaction["request_payload"]["account_context"]["available"] is False

    engine.dispose()


def test_trigger_time_sell_agent_can_adjust_quantity_limit_and_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "sell",
                "quantity": 2,
                "limit_price": "125500.00",
                "updated_stop_price": "119000.00",
                "reason": "take partial profit and raise stop",
                "risk_notes": ["lock gains"],
                "confidence": "0.80",
                "context_staleness_ok": True,
            }
        ],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-sell-adjust",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "118000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "target sell with trigger-time agent",
            "prices": [{"symbol": "005930", "price": "125100.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1
    assert len(captured) == 1
    assert captured[0]["signal_context"]["side"] == "sell"
    assert captured[0]["execution_context"]["intended_quantity"] == 5
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == target_item_id
    assert broker_adapter.order_calls[0].quantity == 2
    assert broker_adapter.order_calls[0].limit_price == Decimal("125500.00")

    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "broker_order_submitted"
        assert stop_item.trigger_price == Decimal("119000.0000")
        assert target_item.risk_context["updated_stop_price"] == "119000.0000"

    engine.dispose()


def test_trigger_time_agent_preserves_deterministic_emergency_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-emergency-stop-bypass",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "118000.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "deterministic_emergency_stop": True,
                    },
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    stop_item_id = plan_response.json()["persisted_execution_item_ids"][0]
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "emergency stop should remain deterministic",
            "prices": [{"symbol": "005930", "price": "117900.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == stop_item_id
    assert broker_adapter.order_calls[0].quantity == 5

    engine.dispose()


def test_runtime_status_exposes_trigger_time_agent_state_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, _broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-agent-counts",
            source="test",
            mode="paper",
            items=[
                ExecutionPlanItem(symbol="000001", side="buy", intent="intraday_watch_buy", status="watch_monitoring_active"),
                ExecutionPlanItem(symbol="000002", side="buy", intent="intraday_watch_buy", status="triggered_pending_agent"),
                ExecutionPlanItem(symbol="000003", side="buy", intent="intraday_watch_buy", status="agent_delay"),
                ExecutionPlanItem(symbol="000004", side="buy", intent="intraday_watch_buy", status="agent_rejected"),
                ExecutionPlanItem(symbol="000005", side="buy", intent="intraday_watch_buy", status="watch_buy_submitted"),
            ],
        )
        session.add(plan)
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    counts = response.json()["liveness"]["monitoring"]["agent_decisions"]
    assert counts == {
        "watching": 1,
        "pending_agent_decision": 1,
        "agent_delayed": 1,
        "agent_rejected": 1,
        "agent_approved_submitted": 1,
    }

    with app.state.db_session_factory() as session:
        assert session.scalar(select(BrokerOrder)) is None

    engine.dispose()


def test_monitoring_rearms_partial_protective_stop_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, _broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-partial-protective-stop-coverage",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status="broker_order_submitted",
                    quantity_policy={"kind": "shares", "quantity": 1},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        session.add(plan)
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "repair partial protective stop coverage",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 1
    assert coverage["affected_symbols"] == []
    assert coverage["issues"] == []

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_coverage = runtime_status.json()["liveness"]["monitoring"]["protective_stop_coverage"]
    assert runtime_coverage["status"] == "ok"
    assert runtime_coverage["repaired_stop_coverage_count"] == 1

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        stop_items = session.scalars(
            select(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.symbol == "005930",
                ExecutionPlanItem.intent == "stop_sell",
            )
            .order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        checkpoint = session.scalar(select(MonitoringCheckpoint))
        assert stop_item is not None
        assert checkpoint is not None
        assert stop_item.status == "broker_order_submitted"
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 1}
        assert len(stop_items) == 2
        repair_item = stop_items[1]
        assert repair_item.status == "stop_sell_monitoring"
        assert repair_item.quantity_policy == {"kind": "shares", "quantity": 4}
        events = repair_item.risk_context["monitoring_events"]
        assert events[-1]["event"] == "unprotected_position_stop_rearmed"
        assert events[-1]["held_quantity"] == 5
        assert events[-1]["active_stop_quantity"] == 1
        assert events[-1]["missing_quantity"] == 4
        assert events[-1]["rearmed_quantity"] == 4
        assert events[-1]["prior_item_id"] == stop_item_id
        assert checkpoint.status == "running"
        assert checkpoint.details["protective_stop_coverage"]["repaired_stop_coverage_count"] == 1

    engine.dispose()


@pytest.mark.parametrize("stop_status", ["open", "partially_filled"])
def test_monitoring_counts_broker_truth_live_stop_as_protective_coverage(
    monkeypatch: pytest.MonkeyPatch,
    stop_status: str,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id=f"plan-live-stop-coverage-{stop_status}",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status=stop_status,
                    quantity_policy={"kind": "shares", "quantity": 5},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        session.add(plan)
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "verify broker truth protective stop coverage",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 0
    assert coverage["affected_symbols"] == []
    assert coverage["issues"] == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.status == stop_status
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 5}
        assert "monitoring_events" not in stop_item.risk_context

    engine.dispose()
