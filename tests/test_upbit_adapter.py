from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill, ExecutionPlanItem, ExecutionPosition


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
    assert order["events"][0]["request_payload"]["quote_notional"] == "50000.1234"
    assert order["events"][1]["request_payload"]["price"] == "50000.1234"

    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="upbit-quote-buy-1").one()
        assert persisted.quantity is None
        assert persisted.quote_notional == Decimal("50000.1234")


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
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
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
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
    )
    client = TestClient(app)
    resume_runtime(client)

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "upbit-paper-lifecycle-1",
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

    board = client.get("/runtime/daily-board").json()
    assert "KRW-BTC" in [row["symbol"] for row in board["symbols"]]
    assert board["totals"]["filled_symbol_count"] == 1
    assert board["totals"]["position_symbol_count"] == 1

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


def test_crypto_paper_dispatch_blocked_by_pause() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
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
        Settings(database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True)
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
    assert "budget" in response.json()["message"]


def test_kis_stock_order_preserves_integer_quantity_on_adapter_rejection() -> None:
    app, _engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
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
