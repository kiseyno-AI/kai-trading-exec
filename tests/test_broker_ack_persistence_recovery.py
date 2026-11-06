"""A broker acknowledgement must survive a failure in the local bookkeeping that follows it."""

from fastapi.testclient import TestClient

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, BrokerOrderEvent
from app.services import broker_orders as broker_orders_service


def _build_client() -> tuple[TestClient, object]:
    settings = Settings(
        database_url="sqlite:///:memory:", runtime_mode="paper", automation_enabled=True
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=create_session_factory(engine),
    )
    client = TestClient(app, raise_server_exceptions=False)
    resumed = client.post(
        "/controls/resume",
        json={"actor": "test", "source": "test", "reason": "enable test order flow"},
    )
    assert resumed.status_code == 200
    return client, engine


def _order_payload(key: str) -> dict:
    return {
        "idempotency_key": key,
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


def test_ack_is_recorded_when_post_ack_bookkeeping_fails(monkeypatch) -> None:
    client, engine = _build_client()

    def boom(**_kwargs):
        raise RuntimeError("post-acknowledgement bookkeeping failed")

    # Runs after place_order() returned, in the same unit of work as the ACK.
    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", boom)

    response = client.post("/orders", json=_order_payload("ack-recovery-1"))

    assert response.status_code == 201, response.text
    order = response.json()["order"]
    assert order["status"] == "submitted"
    assert order["broker_order_id"]
    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="ack-recovery-1").one()
        assert persisted.broker_order_id
        assert persisted.status == "submitted"
        assert persisted.submitted_at is not None
        events = (
            session.query(BrokerOrderEvent)
            .filter_by(broker_order_pk=persisted.id)
            .order_by(BrokerOrderEvent.id)
            .all()
        )
        assert [event.event_type for event in events][-1] == "submit_acknowledged_degraded"
        assert persisted.broker_order_id == events[-1].payload.get("broker_order_id")


def test_replay_after_recovered_ack_does_not_place_a_second_order(monkeypatch) -> None:
    client, engine = _build_client()

    def boom(**_kwargs):
        raise RuntimeError("post-acknowledgement bookkeeping failed")

    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", boom)
    first = client.post("/orders", json=_order_payload("ack-recovery-2"))
    second = client.post("/orders", json=_order_payload("ack-recovery-2"))

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    with create_session_factory(engine)() as session:
        assert session.query(BrokerOrder).filter_by(idempotency_key="ack-recovery-2").count() == 1


def test_normal_acknowledgement_path_is_unchanged() -> None:
    client, engine = _build_client()

    response = client.post("/orders", json=_order_payload("ack-normal-1"))

    assert response.status_code == 201
    with create_session_factory(engine)() as session:
        persisted = session.query(BrokerOrder).filter_by(idempotency_key="ack-normal-1").one()
        types = [
            event.event_type
            for event in session.query(BrokerOrderEvent)
            .filter_by(broker_order_pk=persisted.id)
            .order_by(BrokerOrderEvent.id)
            .all()
        ]
        assert "submit_acknowledged_degraded" not in types
        assert "submit_acknowledged" in types
