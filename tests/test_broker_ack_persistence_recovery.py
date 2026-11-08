"""A broker acknowledgement must survive a failure in the local bookkeeping that follows it."""

from decimal import Decimal

from fastapi.testclient import TestClient
import pytest

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
        degraded = (
            session.query(BrokerOrderEvent)
            .filter_by(broker_order_pk=persisted.id, event_type="submit_acknowledged_degraded")
            .one()
        )
        assert persisted.broker_order_id == degraded.payload.get("broker_order_id")


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


# --------------------------------------------------------------------------
# Review cases: the degraded path must never undo progress or a revocation.

from datetime import UTC, date, datetime  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from sqlalchemy.orm import Session  # noqa: E402

from app.broker.base import NormalizedBrokerOrderStatus  # noqa: E402
from app.models.runtime import (  # noqa: E402
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
)
from app.api.schemas.execution import PlaceOrderRequest  # noqa: E402


def _filled_payload(key: str) -> dict:
    return _order_payload(key)


def _enable_paper_fills(monkeypatch) -> None:
    """The simulator needs a reference price; pin one so paper orders fill."""

    from decimal import Decimal

    monkeypatch.setattr(
        broker_orders_service, "_simulated_fill_price", lambda **_kwargs: Decimal("120000000")
    )


def _fills(engine, key: str) -> int:
    with create_session_factory(engine)() as session:
        order = session.query(BrokerOrder).filter_by(idempotency_key=key).one()
        return session.query(ExecutionFill).filter_by(broker_order_pk=order.id).count()


def _status(engine, key: str) -> str:
    with create_session_factory(engine)() as session:
        return session.query(BrokerOrder).filter_by(idempotency_key=key).one().status


def test_the_filled_payload_is_filled_on_the_normal_path(monkeypatch) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    response = client.post("/orders", json=_filled_payload("ack-filled-0"))
    assert response.status_code == 201, response.text
    assert _status(engine, "ack-filled-0") == "filled"
    assert _fills(engine, "ack-filled-0") == 1


def test_a_failure_after_the_full_commit_does_not_regress_a_filled_order(monkeypatch) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    real_refresh = Session.refresh
    state = {"raised": False}

    def refresh_fails_once_on_the_filled_order(self, instance, *args, **kwargs):
        if (
            not state["raised"]
            and isinstance(instance, BrokerOrder)
            and not args
            and not kwargs
            and instance.status == "filled"
        ):
            state["raised"] = True
            raise RuntimeError("lookup right after the commit failed")
        return real_refresh(self, instance, *args, **kwargs)

    monkeypatch.setattr(Session, "refresh", refresh_fails_once_on_the_filled_order)
    response = client.post("/orders", json=_filled_payload("ack-filled-1"))

    assert state["raised"] is True
    assert response.status_code == 201, response.text
    assert _status(engine, "ack-filled-1") == "filled"
    assert _fills(engine, "ack-filled-1") == 1
    with create_session_factory(engine)() as session:
        order = session.query(BrokerOrder).filter_by(idempotency_key="ack-filled-1").one()
        types = [e.event_type for e in session.query(BrokerOrderEvent).filter_by(broker_order_pk=order.id)]
        assert "submit_acknowledged_degraded" not in types


def test_the_paper_fill_is_retried_by_the_degraded_path(monkeypatch) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    real = broker_orders_service._record_upbit_paper_simulated_fill
    calls = {"n": 0}

    def fails_once(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulator hiccup")
        return real(**kwargs)

    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", fails_once)
    response = client.post("/orders", json=_filled_payload("ack-paper-1"))

    assert response.status_code == 201, response.text
    assert _status(engine, "ack-paper-1") == "filled"
    assert _fills(engine, "ack-paper-1") == 1


def test_a_replay_recovers_a_paper_fill_the_degraded_path_could_not_write(monkeypatch) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    real = broker_orders_service._record_upbit_paper_simulated_fill
    calls = {"n": 0}

    def fails_twice(**kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("simulator down")
        return real(**kwargs)

    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", fails_twice)
    first = client.post("/orders", json=_filled_payload("ack-paper-2"))
    assert first.status_code == 201
    assert _status(engine, "ack-paper-2") == "submitted"
    assert _fills(engine, "ack-paper-2") == 0

    replay = client.post("/orders", json=_filled_payload("ack-paper-2"))
    again = client.post("/orders", json=_filled_payload("ack-paper-2"))

    assert replay.status_code == 200 and replay.json()["duplicate"] is True
    assert again.status_code == 200
    assert _status(engine, "ack-paper-2") == "filled"
    assert _fills(engine, "ack-paper-2") == 1  # idempotent across replays
    with create_session_factory(engine)() as session:
        assert session.query(BrokerOrder).filter_by(idempotency_key="ack-paper-2").count() == 1


@pytest.mark.parametrize("stored_command", ["valid", "missing", "invalid"])
def test_degraded_replay_never_uses_replacement_order_economics(
    monkeypatch, stored_command: str
) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    real = broker_orders_service._record_upbit_paper_simulated_fill

    def unavailable(**_kwargs):
        raise RuntimeError("simulator unavailable")

    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", unavailable)
    key = "ack-original-economics"
    first = client.post("/orders", json=_filled_payload(key))
    assert first.status_code == 201, first.text
    assert _fills(engine, key) == 0
    if stored_command != "valid":
        with create_session_factory(engine)() as session:
            order = session.query(BrokerOrder).filter_by(idempotency_key=key).one()
            metadata = dict(order.request_metadata)
            metadata["original_command"] = None if stored_command == "missing" else {}
            order.request_metadata = metadata
            session.commit()
    monkeypatch.setattr(broker_orders_service, "_record_upbit_paper_simulated_fill", real)

    replacement = {**_filled_payload(key), "quote_notional": "500000"}
    replay = client.post("/orders", json=replacement)

    assert replay.status_code == 200 and replay.json()["duplicate"] is True
    with create_session_factory(engine)() as session:
        order = session.query(BrokerOrder).filter_by(idempotency_key=key).one()
        fills = session.query(ExecutionFill).filter_by(broker_order_pk=order.id).all()
        assert order.quote_notional == Decimal("50000")
        if stored_command == "valid":
            assert len(fills) == 1
            assert fills[0].quote_notional == Decimal("50000")
        else:
            assert fills == []
            assert order.status == "submitted"


def test_degraded_replay_cannot_simulate_a_fill_for_another_provider(monkeypatch) -> None:
    _enable_paper_fills(monkeypatch)
    client, engine = _build_client()
    key = "ack-alpaca-replayed-as-upbit"
    with create_session_factory(engine)() as session:
        order = BrokerOrder(
            broker_name="alpaca", mode="paper", idempotency_key=key,
            symbol="BTC/USD", side="buy", status="submitted",
            broker_order_id="alpaca-ack", asset_class="crypto", venue="alpaca",
            quote_notional=Decimal("100"),
        )
        session.add(order)
        session.flush()
        session.add(BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type=broker_orders_service.DEGRADED_ACK_EVENT_TYPE,
            event_source="submission", normalized_status="submitted",
        ))
        session.commit()

    replay = client.post("/orders", json=_filled_payload(key))

    assert replay.status_code == 200 and replay.json()["duplicate"] is True
    assert _status(engine, key) == "submitted"
    assert _fills(engine, key) == 0


def _degraded_setup(*, change_plan_item):
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    plan = ExecutionPlan(
        external_plan_id="plan-degraded", source="kai-trading-core",
        market_date=date(2026, 11, 9), mode="live", status="accepted",
    )
    item = ExecutionPlanItem(
        plan=plan, symbol="005930", side="buy", intent="next_open_buy",
        status="broker_order_submission_pending", provider="kis", venue="kis",
        asset_class="stock", risk_context={"exchange": "KRX", "currency": "KRW"},
    )
    db.add_all([plan, item])
    db.flush()
    order = BrokerOrder(
        broker_name="kis", mode="live", idempotency_key="degraded-live-1",
        symbol="005930", side="buy", status="submission_pending",
        execution_plan_item_id=item.id,
    )
    db.add(order)
    db.commit()
    before = broker_orders_service._plan_item_cas_state(item)
    if change_plan_item:
        # Core's revocation lands between the broker ACK and the degraded write.
        item.status = "cancelled"
        item.risk_context = {**item.risk_context, "core_revocation": {"revoked": True}}
        db.commit()
    placement = SimpleNamespace(
        broker_order_id="KIS-0001",
        normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
        submitted_at=datetime(2026, 11, 9, 0, 1, tzinfo=UTC),
        observed_at=datetime(2026, 11, 9, 0, 1, tzinfo=UTC),
        broker_status="accepted",
    )
    command = PlaceOrderRequest.model_validate(
        {
            "idempotency_key": "degraded-live-1", "provider": "kis", "asset_class": "stock",
            "venue": "kis", "symbol": "005930", "side": "buy", "order_type": "limit",
            "time_in_force": "day", "exchange": "KRX", "quantity": "1", "limit_price": "70000",
        }
    )
    result = broker_orders_service._persist_degraded_broker_acknowledgement(
        settings=settings, db=db, idempotency_key="degraded-live-1", plan_item_id=item.id,
        placement=placement, failure=RuntimeError("bookkeeping failed"), command=command,
        order_status_before_ack="submission_pending", plan_item_state_before_ack=before,
    )
    db.expire_all()
    return db, result, db.get(ExecutionPlanItem, item.id)


def test_a_core_revocation_that_landed_meanwhile_is_not_overwritten() -> None:
    db, order, item = _degraded_setup(change_plan_item=True)
    assert order.broker_order_id == "KIS-0001"  # the broker id is never lost
    assert item.status == "cancelled"
    assert item.risk_context["core_revocation"] == {"revoked": True}
    db.close()


def test_an_unchanged_plan_item_still_follows_the_acknowledgement() -> None:
    db, order, item = _degraded_setup(change_plan_item=False)
    assert order.broker_order_id == "KIS-0001"
    assert order.status == "submitted"
    assert item.status != "broker_order_submission_pending"
    db.close()
