from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from app.broker.base import (
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerTokenGrant,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis import KisBrokerAdapter
from app.broker.upbit import UpbitBrokerAdapter
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlanItem,
    ExecutionPosition,
)


def build_test_app(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
    )
    return app, engine


def resume_runtime(client: TestClient) -> None:
    response = client.post(
        "/controls/resume",
        json={"actor": "test", "source": "test", "reason": "enable test order flow"},
    )
    assert response.status_code == 200


def assert_digest_only(value: object) -> None:
    assert isinstance(value, str)
    assert value.startswith("broker_error_details_redacted[sha256=")


def test_upbit_stub_snapshot_never_claims_authoritative_empty_truth() -> None:
    adapter = UpbitBrokerAdapter(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    )

    snapshot = adapter.get_account_snapshot("paper-token")

    assert snapshot.holdings == []
    assert snapshot.open_orders == []
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.diagnostics == {
        "reason": "account_snapshot_not_implemented",
        "provider": "upbit",
    }


def test_upbit_paper_market_buy_preserves_krw_quote_notional() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-quote-buy-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "50000.1234",
            "price_precision": 0,
        },
    )

    assert response.status_code == 201
    order = response.json()["order"]
    assert order["broker_name"] == "upbit"
    assert order["quantity"] is None
    assert order["base_quantity"] is None
    assert order["quote_notional"] == "50000.1234"
    assert order["asset_class"] == "crypto"
    assert order["request_metadata"]["sizing"]["quote_notional"] == "50000.1234"
    assert order["request_metadata"]["execution_scope"]["provider"] == "upbit"
    assert order["request_metadata"]["execution_scope"]["venue"] == "UPBIT"
    assert order["request_metadata"]["execution_scope"]["asset_class"] == "crypto"
    assert order["request_metadata"]["execution_scope"]["exchange"] == "UPBIT"
    assert len(order["request_metadata"]["execution_scope"]["account_scope"]) == 24
    assert order["events"][0]["request_payload"]["quote_notional"] == "50000.1234"
    assert order["events"][1]["request_payload"]["price"] == "50000.1234"

    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="upbit-quote-buy-1").one()
        assert persisted.quantity is None
        assert persisted.quote_notional == Decimal("50000.1234")


def test_upbit_paper_buys_reserve_configured_total_budget_across_symbols() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            exec_agent_max_symbol_budget=Decimal("60000"),
            exec_agent_max_total_budget=Decimal("75000"),
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    def payload(*, symbol: str, key: str) -> dict[str, object]:
        return {
            "idempotency_key": key,
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": symbol,
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "50000",
            "price_precision": 0,
        }

    first = client.post(
        "/orders",
        json=payload(symbol="KRW-BTC", key="upbit-budget-first"),
    )
    second = client.post(
        "/orders",
        json=payload(symbol="KRW-ETH", key="upbit-budget-second"),
    )

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json()["block_reason"] == "buy_capital_reservation"
    with create_session_factory(engine)() as session:
        assert session.query(BrokerOrder).filter_by(status="submitted").count() == 1
    engine.dispose()


def test_paused_runtime_blocks_direct_order_without_persisting_broker_action() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-paused-direct-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "50000",
            "price_precision": 0,
        },
    )

    assert response.status_code == 409
    payload = response.json()
    assert payload["block_reason"] == "paused"
    with create_session_factory(engine)() as session:
        assert session.query(BrokerOrder).count() == 0
        assert session.query(BrokerOrderEvent).count() == 0


def test_kill_switch_blocks_resume_and_direct_order_without_broker_action() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    kill = client.post(
        "/controls/kill-switch",
        json={"actor": "ops", "source": "test", "reason": "test kill switch"},
    )
    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "should stay blocked"},
    )
    order = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-kill-switch-direct-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "50000",
            "price_precision": 0,
        },
    )

    assert kill.status_code == 200
    assert kill.json()["safety"]["kill_switch_enabled"] is True
    assert resume.status_code == 409
    assert resume.json()["outcome"] == "rejected"
    assert order.status_code == 409
    assert order.json()["block_reason"] == "kill_switch"
    with create_session_factory(engine)() as session:
        assert session.query(BrokerOrder).count() == 0
        assert session.query(BrokerOrderEvent).count() == 0


def test_order_idempotent_replay_returns_existing_state_without_duplicate_events() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)
    request_payload = {
        "idempotency_key": "upbit-idempotent-replay-1",
        "provider": "upbit",
        "asset_class": "crypto",
        "venue": "upbit",
        "symbol": "KRW-BTC",
        "side": "buy",
        "order_type": "market",
        "time_in_force": "day",
        "exchange": "UPBIT",
        "quote_notional": "50000",
        "price_precision": 0,
    }

    first = client.post("/orders", json=request_payload)
    second = client.post("/orders", json=request_payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["order"]["status"] == "submitted"
    with create_session_factory(engine)() as session:
        order = session.query(BrokerOrder).filter_by(idempotency_key="upbit-idempotent-replay-1").one()
        assert session.query(BrokerOrder).count() == 1
        assert session.query(BrokerOrderEvent).filter_by(broker_order_pk=order.id).count() == 2
        assert session.query(ExecutionFill).filter_by(broker_order_pk=order.id).count() == 0


def test_upbit_paper_sell_preserves_fractional_base_quantity() -> None:
    app, _engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-base-sell-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "sell",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "base_quantity": "0.00123456",
            "quantity_precision": 8,
        },
    )

    assert response.status_code == 201
    order = response.json()["order"]
    assert order["quantity"] is None
    assert order["base_quantity"] == "0.00123456"
    assert order["quote_notional"] is None
    assert order["request_metadata"]["sizing"]["base_quantity"] == "0.00123456"
    assert order["events"][1]["request_payload"]["volume"] == "0.00123456"


def test_upbit_paper_limit_order_preserves_limit_price_payloads() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-limit-sell-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "sell",
            "order_type": "limit",
            "limit_price": "150000000.1234",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "base_quantity": "0.00123456",
            "quantity_precision": 8,
            "price_precision": 4,
        },
    )

    assert response.status_code == 201
    order = response.json()["order"]
    assert order["limit_price"] == "150000000.1234"
    assert order["request_metadata"]["sizing"]["limit_price"] == "150000000.1234"
    assert order["events"][1]["request_payload"]["price"] == "150000000.1234"
    assert order["events"][1]["response_payload"]["price"] == "150000000.1234"

    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="upbit-limit-sell-1").one()
        assert persisted.limit_price == Decimal("150000000.1234")
        assert persisted.raw_broker_payload["price"] == "150000000.1234"


def test_upbit_rejects_crypto_integer_quantity() -> None:
    app, _engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-invalid-int-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quantity": 1,
        },
    )

    assert response.status_code == 422
    assert "base_quantity or quote_notional" in response.text or "integer quantity" in response.text


def test_upbit_live_disabled_fails_closed_even_with_keys() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
            automation_enabled=True,
            upbit_access_key="present",
            upbit_secret_key="present",
            upbit_live_trading_enabled=False,
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-live-disabled-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "10000",
        },
    )

    assert response.status_code == 409
    payload = response.json()
    assert payload["duplicate"] is False
    assert "paper-only" in payload["message"].lower()


def test_runtime_status_reports_upbit_readiness_separately() -> None:
    app, _engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["broker"]["provider"] == "kis"
    assert payload["broker_providers"]["kis"]["provider"] == "kis"
    assert payload["broker_providers"]["upbit"]["provider"] == "upbit"
    assert payload["broker_providers"]["upbit"]["configured"] is True
    assert payload["broker_providers"]["upbit"]["status"] == "uninitialized"


def test_execution_plan_accepts_crypto_sizing_fields() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            execution_position_account_scope="test-upbit-paper-account",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-plan-1",
            "source": "test",
            "requested_at": datetime(2026, 6, 19, tzinfo=UTC).isoformat(),
            "items": [
                {
                    "symbol": "KRW-BTC",
                    "side": "buy",
                    "intent": "manual_crypto_buy",
                    "provider": "upbit",
                    "venue": "upbit",
                    "asset_class": "crypto",
                    "quote_notional": "75000.50",
                    "price_precision": 0,
                    "quantity_policy": {"kind": "quote_notional", "quote_notional": "75000.50"},
                }
            ],
        },
    )

    assert response.status_code == 201
    item_id = response.json()["persisted_execution_item_ids"][0]
    with create_session_factory(engine)() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.provider == "upbit"
        assert item.asset_class == "crypto"
        assert item.quote_notional == Decimal("75000.5000")


def test_crypto_paper_plan_dispatch_simulates_fill_and_position() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            execution_position_account_scope="test-upbit-paper-account",
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-paper-lifecycle-1",
            "source": "test",
            "market_date": datetime.now(UTC).date().isoformat(),
            "requested_at": datetime(2026, 6, 19, tzinfo=UTC).isoformat(),
            "items": [
                {
                    "symbol": "KRW-BTC",
                    "side": "buy",
                    "intent": "paper_crypto_buy",
                    "provider": "upbit",
                    "venue": "upbit",
                    "asset_class": "crypto",
                    "quote_notional": "60000.0000",
                    "price_precision": 0,
                    "quantity_policy": {
                        "kind": "quote_notional",
                        "quote_notional": "60000.0000",
                        "idempotency_key": "upbit-paper-lifecycle-order-1",
                    },
                    "risk_context": {
                        "provider_metadata": {"provider": "upbit", "venue": "UPBIT"},
                        "simulated_fill_price": "120000000",
                    },
                }
            ],
        },
    )

    assert plan_response.status_code == 201
    item_id = plan_response.json()["persisted_execution_item_ids"][0]
    jobs_response = client.get("/execution-jobs")
    assert jobs_response.status_code == 200
    assert jobs_response.json()[0]["job_type"] == "crypto_paper_order"
    assert jobs_response.json()[0]["provider"] == "upbit"

    dispatch_response = client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["completed_job_count"] == 1
    order = client.get("/symbols/KRW-BTC").json()["orders"][0]
    assert order["broker_name"] == "upbit"
    assert order["status"] == "filled"
    assert order["asset_class"] == "crypto"
    assert order["quantity"] is None
    assert order["quote_notional"] == "60000.0000"
    assert order["response_metadata"]["paper"] is True
    assert [event["event_type"] for event in order["events"]] == [
        "submit_requested",
        "submit_acknowledged",
        "paper_fill_simulated",
    ]
    assert order["events"][2]["payload"]["simulated"] is True

    symbol = client.get("/symbols/KRW-BTC").json()
    assert symbol["summary"]["providers"] == ["upbit"]
    assert symbol["summary"]["asset_classes"] == ["crypto"]
    assert symbol["summary"]["fill_count"] == 1
    assert symbol["summary"]["filled_base_quantity"] == "0.000500000000"
    assert symbol["summary"]["position_base_quantity"] == "0.000500000000"

    with create_session_factory(engine)() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "filled"
        fill = session.query(ExecutionFill).filter_by(symbol="KRW-BTC").one()
        assert fill.quantity == 0
        assert fill.base_quantity == Decimal("0.000500000000")
        assert fill.payload["simulated"] is True
        position = session.query(ExecutionPosition).filter_by(symbol="KRW-BTC").one()
        assert position.quantity == 0
        assert position.base_quantity == Decimal("0.000500000000")
        event = session.query(BrokerOrderEvent).filter_by(event_type="paper_fill_simulated").one()
        assert event.payload["paper"] is True


def test_crypto_paper_dispatch_fails_permanent_budget_guardrail() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            exec_agent_max_symbol_budget=Decimal("100000"),
            execution_position_account_scope="test-upbit-paper-account",
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-paper-budget-dispatch-1",
            "source": "test",
            "requested_at": datetime(2026, 6, 19, tzinfo=UTC).isoformat(),
            "items": [
                {
                    "symbol": "KRW-BTC",
                    "side": "buy",
                    "intent": "paper_crypto_buy",
                    "provider": "upbit",
                    "venue": "upbit",
                    "asset_class": "crypto",
                    "quote_notional": "100001",
                    "quantity_policy": {
                        "kind": "quote_notional",
                        "quote_notional": "100001",
                        "idempotency_key": "upbit-paper-budget-dispatch-order-1",
                    },
                    "risk_context": {
                        "provider_metadata": {"provider": "upbit", "venue": "UPBIT"},
                        "simulated_fill_price": "120000000",
                    },
                }
            ],
        },
    )

    assert plan_response.status_code == 201
    item_id = plan_response.json()["persisted_execution_item_ids"][0]

    dispatch_response = client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    payload = dispatch_response.json()
    assert payload["completed_job_count"] == 0
    assert payload["retrying_job_count"] == 0
    assert payload["failed_job_count"] == 1
    assert payload["jobs"][0]["status"] == "failed"
    assert "budget" in payload["jobs"][0]["last_error"]
    assert client.get("/orders").json() == []

    with create_session_factory(engine)() as session:
        job = session.query(ExecutionJob).one()
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert job.status == "failed"
        assert item.status == "crypto_paper_failed"


def test_crypto_paper_position_preserves_weighted_average_cost() -> None:
    app, engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    orders = [
        ("upbit-cost-buy-1", "buy", "0.5", "10000"),
        ("upbit-cost-buy-2", "buy", "0.5", "20000"),
        ("upbit-cost-sell-1", "sell", "0.25", "30000"),
    ]
    for idempotency_key, side, base_quantity, limit_price in orders:
        response = client.post(
            "/orders",
            json={
                "idempotency_key": idempotency_key,
                "provider": "upbit",
                "asset_class": "crypto",
                "venue": "upbit",
                "symbol": "KRW-BTC",
                "side": side,
                "order_type": "limit",
                "time_in_force": "day",
                "exchange": "UPBIT",
                "base_quantity": base_quantity,
                "limit_price": limit_price,
                "quantity_precision": 8,
                "price_precision": 0,
            },
        )
        assert response.status_code == 201

    with create_session_factory(engine)() as session:
        position = session.query(ExecutionPosition).filter_by(symbol="KRW-BTC").one()
        assert position.base_quantity == Decimal("0.750000000000")
        assert position.average_price == Decimal("15000.0000")


def test_crypto_paper_dispatch_blocked_by_pause() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            execution_position_account_scope="test-upbit-paper-account",
        )
    )
    client = TestClient(app)

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-paper-paused-1",
            "source": "test",
            "items": [
                {
                    "symbol": "KRW-ETH",
                    "side": "buy",
                    "intent": "paper_crypto_buy",
                    "provider": "upbit",
                    "venue": "upbit",
                    "asset_class": "crypto",
                    "quote_notional": "50000",
                    "risk_context": {"simulated_fill_price": "5000000"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201

    dispatch_response = client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    payload = dispatch_response.json()
    assert payload["blocked"] is True
    assert payload["block_reason"] == "paused"
    assert client.get("/orders").json() == []


def test_crypto_paper_plan_rejects_unwhitelisted_symbol() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            execution_position_account_scope="test-upbit-paper-account",
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-paper-altcoin-1",
            "source": "test",
            "items": [
                {
                    "symbol": "KRW-XRP",
                    "side": "buy",
                    "intent": "paper_crypto_buy",
                    "provider": "upbit",
                    "venue": "upbit",
                    "asset_class": "crypto",
                    "quote_notional": "50000",
                    "risk_context": {"simulated_fill_price": "1000"},
                }
            ],
        },
    )

    assert response.status_code == 201
    plans = client.get("/execution-plans").json()
    item = plans[0]["items"][0]
    assert item["status"] == "crypto_paper_rejected"
    assert "KRW-BTC and KRW-ETH" in item["risk_context"]["crypto_paper_block_reason"]
    assert client.get("/execution-jobs").json() == []


def test_crypto_paper_order_rejects_budget_breach() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            exec_agent_max_symbol_budget=Decimal("100000"),
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "upbit-budget-breach-1",
            "provider": "upbit",
            "asset_class": "crypto",
            "venue": "upbit",
            "symbol": "KRW-BTC",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "UPBIT",
            "quote_notional": "100001",
        },
    )

    assert response.status_code == 409
    payload = response.json()
    assert "budget" in payload["message"]
    assert payload["permanent_rejection"] is True
    assert payload["rejection_reason"] == payload["message"]
    assert "block_reason" not in payload


def test_kis_stock_order_preserves_integer_quantity_on_adapter_rejection(
    monkeypatch,
    tmp_path,
) -> None:
    sensitive_value = "nested-error-account-value"
    monkeypatch.setattr(
        KisBrokerAdapter,
        "authenticate",
            lambda _self: BrokerTokenGrant(
                access_token="test-token",
                token_type="Bearer",
                access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
    )
    monkeypatch.setattr(
        KisBrokerAdapter,
        "get_account_snapshot",
        lambda _self, _access_token, **_kwargs: BrokerAccountSnapshot(
            available_cash=Decimal("100000.00"),
            buying_power=Decimal("100000.00"),
            observed_at=datetime.now(UTC),
            source="test:sufficient-buying-power",
        ),
    )

    def reject_order(_self, _access_token, _request):  # type: ignore[no-untyped-def]
        raise BrokerAdapterError(
            "deterministic adapter rejection",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            request_metadata={
                "adapter_marker": "preserved",
                "nested": {"CANO": sensitive_value, "safe_request_marker": "kept"},
                "execution_scope": {
                    "policy_version": 1,
                    "provider": "attacker",
                    "account_scope": "f" * 24,
                    "venue": "OTHER",
                    "asset_class": "other",
                    "exchange": "OTHER",
                },
            },
            response_metadata={
                "nested": {
                    "ACNT_PRDT_CD": sensitive_value,
                    "safe_response_marker": "kept",
                }
            },
            request_payload={
                "nested": {"account_number": sensitive_value, "safe_request_payload": "kept"}
            },
            response_payload={
                "nested": {"acct_id": sensitive_value, "safe_broker_payload": "kept"}
            },
        )

    monkeypatch.setattr(KisBrokerAdapter, "place_order", reject_order)
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            account_drawdown_gate_enabled=False,
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            kis_token_cache_directory=str(tmp_path / "kis-token-cache"),
        )
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "kis-stock-int-quantity-1",
            "symbol": "005930",
            "side": "buy",
            "order_type": "limit",
            "quantity": 1,
            "limit_price": "71000",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert response.status_code == 422
    order = response.json()["order"]
    assert order["broker_name"] == "kis"
    assert order["status"] == "rejected"
    assert order["quantity"] == 1
    assert order["base_quantity"] is None
    assert order["quote_notional"] is None
    assert order["request_metadata"]["adapter_marker"] == "preserved"
    assert order["request_metadata"]["execution_scope"]["provider"] == "kis"
    assert order["request_metadata"]["execution_scope"]["venue"] == "KIS"
    assert order["request_metadata"]["execution_scope"]["asset_class"] == "stock"
    assert order["request_metadata"]["execution_scope"]["exchange"] == "KRX"
    assert len(order["request_metadata"]["execution_scope"]["account_scope"]) == 24
    assert order["request_metadata"]["nested"] == {"safe_request_marker": "kept"}
    assert_digest_only(
        order["response_metadata"]["nested"]["safe_response_marker"]
    )
    assert_digest_only(
        order["raw_broker_payload"]["nested"]["safe_broker_payload"]
    )
    failure_event = order["events"][-1]
    assert failure_event["request_payload"]["nested"] == {
        "safe_request_payload": "kept"
    }
    assert_digest_only(
        failure_event["response_payload"]["nested"]["safe_broker_payload"]
    )
    assert sensitive_value not in str(order)
    assert "12345678" not in str(order["request_metadata"])
    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(
            idempotency_key="kis-stock-int-quantity-1"
        ).one()
        persisted_event = session.query(BrokerOrderEvent).filter_by(
            broker_order_pk=persisted.id,
            event_type="submit_failed",
        ).one()
        assert persisted.request_metadata["nested"] == {"safe_request_marker": "kept"}
        assert_digest_only(
            persisted.response_metadata["nested"]["safe_response_marker"]
        )
        assert_digest_only(
            persisted.raw_broker_payload["nested"]["safe_broker_payload"]
        )
        assert persisted_event.request_payload["nested"] == {
            "safe_request_payload": "kept"
        }
        assert_digest_only(
            persisted_event.response_payload["nested"]["safe_broker_payload"]
        )
        assert sensitive_value not in str(
            {
                "request_metadata": persisted.request_metadata,
                "response_metadata": persisted.response_metadata,
                "raw_broker_payload": persisted.raw_broker_payload,
                "event_request": persisted_event.request_payload,
                "event_response": persisted_event.response_payload,
                "event_payload": persisted_event.payload,
            }
        )


def test_stock_order_still_requires_integer_quantity() -> None:
    app, _engine = build_test_app(
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "kis-stock-missing-quantity-1",
            "symbol": "005930",
            "side": "buy",
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert response.status_code == 422
    assert "stock orders require quantity" in response.text
