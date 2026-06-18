from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, ExecutionPlanItem


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
    app, engine = build_test_app(
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
    assert "disabled" in payload["message"].lower()
    order = payload["order"]
    assert order["broker_name"] == "upbit"
    assert order["status"] == "failed"
    assert order["quote_notional"] == "10000.0000"

    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="upbit-live-disabled-1").one()
        assert persisted.broker_order_id is None
        assert persisted.quote_notional == Decimal("10000.0000")


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
