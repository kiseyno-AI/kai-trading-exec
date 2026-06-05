from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.broker.base import BrokerAdapterError, BrokerConfig
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
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
) -> str:
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
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "72420.00",
                        "stop_price": "70000.00",
                        "decider_output": {"rationale": "trigger-time test candidate"},
                    },
                }
            ],
        },
    )
    assert response.status_code == 201
    return response.json()["persisted_execution_item_ids"][0]


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
            "prices": [{"symbol": "005930", "price": "71050.00"}],
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
        interaction = item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert interaction["decision"]["decision"] == "execute_now"
        assert interaction["order"]["quantity"] == 3

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
            "prices": [{"symbol": "005930", "price": "71050.00"}],
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
            "prices": [{"symbol": "005930", "price": "71050.00"}],
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
            "prices": [{"symbol": "005930", "price": "71050.00"}],
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
