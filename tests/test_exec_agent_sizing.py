from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, ExecutionJob, ExecutionPlan, ExecutionPlanItem


class StubBrokerAdapter:
    provider = "kis"

    def __init__(
        self,
        *,
        mode: str = "live",
        available_cash: Decimal = Decimal("500000"),
        holdings: list[BrokerAccountHolding] | None = None,
        open_orders: list[BrokerAccountOpenOrder] | None = None,
    ) -> None:
        self.mode = mode
        self.snapshot = BrokerAccountSnapshot(
            available_cash=available_cash,
            observed_at=datetime(2026, 5, 13, 9, 0, tzinfo=UTC),
            source="stub:account_snapshot",
            holdings=holdings or [],
            open_orders=open_orders or [],
        )
        self.authenticate_calls = 0
        self.snapshot_calls = 0
        self.order_calls: list[BrokerOrderPlacementRequest] = []

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider=self.provider,
            mode=self.mode,  # type: ignore[arg-type]
            configured=True,
            api_base_url="https://stub.kis.local",
        )

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        return BrokerTokenGrant(
            access_token="stub-token",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        assert access_token
        return BrokerRealtimeApproval(
            approval_key="stub-approval",
            issued_at=datetime.now(UTC),
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        assert access_token
        return BrokerTokenRevocation(message="revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        assert access_token
        self.order_calls.append(request)
        observed_at = datetime(2026, 5, 13, 9, 1, tzinfo=UTC)
        return BrokerOrderPlacement(
            broker_order_id="kis-short-term-1",
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="0",
            submitted_at=observed_at,
            observed_at=observed_at,
            request_metadata={
                "provider": self.provider,
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "tr_id": "TTTC0012U",
            },
            response_metadata={"status_code": 200, "msg_cd": "0"},
            request_payload={
                "PDNO": request.symbol,
                "ORD_QTY": str(request.quantity),
                "ORD_UNPR": "0",
            },
            response_payload={
                "rt_cd": "0",
                "msg_cd": "0",
                "output": {"ODNO": "kis-short-term-1"},
            },
            message="accepted",
        )

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        assert access_token
        return BrokerCurrentPrice(
            symbol=symbol,
            exchange=exchange,
            price=Decimal("50000"),
            observed_at=datetime.now(UTC),
            payload={},
        )

    def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
        assert access_token
        self.snapshot_calls += 1
        return self.snapshot

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(self, access_token: str, broker_order_id: str) -> dict[str, object]:
        return {"access_token_present": bool(access_token), "broker_order_id": broker_order_id}

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        del transaction_id, hashkey
        return {"authorization": f"Bearer {access_token}"}


def build_test_app(adapter: StubBrokerAdapter, *, runtime_mode: str = "live"):
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=runtime_mode,  # type: ignore[arg-type]
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_max_symbol_budget=Decimal("300000"),
        exec_agent_max_total_budget=Decimal("600000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: adapter,
        monitoring_price_source=None,
    )
    return app, engine


def short_term_plan_payload(
    *,
    plan_id: str = "plan-short-term-agent-1",
    source_plan_item_id: str = "watcher-candidate-1",
    exec_agent_decision: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "plan_id": plan_id,
        "source": "kai-trading-core",
        "market_date": "2026-05-13",
        "items": [
            {
                "symbol": "005930",
                "side": "buy",
                "intent": "next_open_buy",
                "time_in_force": "day",
                "decision_agent_name": "short-term-decider",
                "decision_snapshot_id": "snapshot-1",
                "decision_run_id": "run-1",
                "source_plan_item_id": source_plan_item_id,
                "risk_context": {
                    "exec_agent_decision": exec_agent_decision
                    or {
                        "action": "approve",
                        "confidence": "0.82",
                        "max_budget": "260000",
                        "reference_price": "50000",
                        "target_price": "56000",
                        "stop_price": "47000",
                        "feedback_ids": ["fb-1"],
                        "context": {"watcher_signal_id": "sig-1"},
                    }
                },
            }
        ],
    }


def test_short_term_exec_agent_approves_market_open_buy_and_materializes_monitoring() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    payload = response.json()
    assert payload["exec_agent"]["approved_count"] == 1
    assert payload["exec_agent"]["rejected_count"] == 0
    assert len(payload["persisted_execution_item_ids"]) == 3

    plans = client.get("/execution-plans").json()
    items_by_intent = {item["intent"]: item for item in plans[0]["items"]}
    assert items_by_intent["next_open_buy"]["status"] == "next_open_buy_scheduled"
    assert items_by_intent["next_open_buy"]["quantity_policy"]["quantity"] == 5
    assert items_by_intent["target_sell"]["status"] == "target_sell_monitoring"
    assert items_by_intent["target_sell"]["trigger_price"] == "56000.0000"
    assert items_by_intent["stop_sell"]["status"] == "stop_sell_monitoring"
    assert items_by_intent["stop_sell"]["trigger_price"] == "47000.0000"

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["completed_job_count"] == 1
    assert len(adapter.order_calls) == 1
    order_call = adapter.order_calls[0]
    assert order_call.quantity == 5
    assert order_call.order_type == "market"
    assert order_call.idempotency_key == (
        "short-term-exec-agent:plan-short-term-agent-1:watcher-candidate-1"
    )

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob))
        assert job is not None
        assert job.status == "completed"
        order = session.scalar(select(BrokerOrder))
        assert order is not None
        assert order.idempotency_key == order_call.idempotency_key
        assert order.broker_order_id == "kis-short-term-1"
        buy_item = session.scalar(
            select(ExecutionPlanItem).where(ExecutionPlanItem.intent == "next_open_buy")
        )
        assert buy_item is not None
        audit = buy_item.risk_context["exec_agent_decision"]
        assert audit["feedback_ids"] == ["fb-1"]
        assert audit["agent_contract"]["context"] == {"watcher_signal_id": "sig-1"}
        assert audit["account_snapshot"]["available_cash"] == "500000.0000"

    engine.dispose()


def test_short_term_exec_agent_rejects_insufficient_cash() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("40000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "insufficient_cash"
    assert client.get("/execution-jobs").json() == []
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_existing_holding_without_explicit_approval() -> None:
    adapter = StubBrokerAdapter(
        available_cash=Decimal("500000"),
        holdings=[BrokerAccountHolding(symbol="005930", quantity=3)],
    )
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "existing_holding"
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_duplicate_open_buy_order() -> None:
    duplicate_key = "short-term-exec-agent:plan-short-term-agent-1:watcher-candidate-1"
    adapter = StubBrokerAdapter(
        available_cash=Decimal("500000"),
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=5,
                remaining_quantity=5,
                status="open",
                idempotency_key=duplicate_key,
            )
        ],
    )
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == (
        "duplicate_open_buy_order"
    )
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_invalid_agent_output() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(
            exec_agent_decision={
                "action": "approve",
                "confidence": "0.9",
                "max_budget": "260000",
            }
        ),
    )

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "invalid_agent_output"
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_preview_does_not_persist_or_submit() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans/preview", json=short_term_plan_payload())

    assert response.status_code == 200
    payload = response.json()
    assert payload["dry_run"] is True
    assert payload["exec_agent"]["approved_count"] == 1
    assert payload["execution_jobs"][0]["job_type"] == "next_open_buy"
    assert payload["items"][0]["quantity_policy"]["quantity"] == 5
    assert {item["intent"] for item in payload["items"]} == {
        "next_open_buy",
        "target_sell",
        "stop_sell",
    }
    assert adapter.order_calls == []

    with app.state.db_session_factory() as session:
        assert session.scalars(select(ExecutionPlan)).all() == []
        assert session.scalars(select(BrokerOrder)).all() == []

    engine.dispose()


def test_runtime_safety_controls_still_block_live_short_term_order_submission() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    plan_response = client.post("/execution-plans", json=short_term_plan_payload())
    assert plan_response.status_code == 201

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["blocked"] is True
    assert dispatch.json()["block_reason"] == "paused"
    assert adapter.order_calls == []
    assert client.get("/orders").json() == []

    engine.dispose()
