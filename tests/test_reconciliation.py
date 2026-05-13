from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.broker.base import (
    BrokerAccountHolding,
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
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPosition
from app.services.reconciliation import reconcile_broker_order, sync_execution_positions_from_broker_snapshot


OBSERVED_AT = datetime(2026, 5, 13, 9, 5, tzinfo=UTC)


class ReconciliationStubBroker:
    provider = "kis"

    def __init__(
        self,
        *,
        reconcile_payloads: list[dict[str, object]] | None = None,
        account_snapshot: BrokerAccountSnapshot | None = None,
        order_placement: BrokerOrderPlacement | None = None,
    ) -> None:
        self.reconcile_payloads = reconcile_payloads or []
        self.account_snapshot = account_snapshot or BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
        )
        self.order_placement = order_placement or BrokerOrderPlacement(
            broker_order_id="sell-order-1",
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="0",
            submitted_at=OBSERVED_AT,
            observed_at=OBSERVED_AT,
            request_metadata={"provider": "kis"},
            response_metadata={"provider": "kis"},
            request_payload={},
            response_payload={"rt_cd": "0"},
        )
        self.reconcile_calls: list[str] = []
        self.order_calls: list[BrokerOrderPlacementRequest] = []
        self.authenticate_calls = 0

    def describe(self) -> BrokerConfig:
        return BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url="https://stub.kis.local")

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        return BrokerTokenGrant(
            access_token="top-secret-token",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        assert access_token
        return BrokerRealtimeApproval(approval_key="approval", issued_at=OBSERVED_AT)

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        assert access_token
        return BrokerTokenRevocation(message="revoked")

    def place_order(self, access_token: str, request: BrokerOrderPlacementRequest) -> BrokerOrderPlacement:
        assert access_token
        self.order_calls.append(request)
        return self.order_placement

    def get_current_price(self, access_token: str, *, symbol: str, exchange: str = "KRX") -> BrokerCurrentPrice | None:
        assert access_token
        return BrokerCurrentPrice(symbol=symbol, exchange=exchange, price=Decimal("125100"), observed_at=OBSERVED_AT, payload={})

    def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
        assert access_token
        return self.account_snapshot

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="shared_realtime_subscription")

    def reconcile_order(self, access_token: str, broker_order_id: str) -> dict[str, object]:
        assert access_token
        self.reconcile_calls.append(broker_order_id)
        if not self.reconcile_payloads:
            return {"broker_order_id": broker_order_id, "status": "open", "observed_at": OBSERVED_AT.isoformat()}
        return self.reconcile_payloads[min(len(self.reconcile_calls) - 1, len(self.reconcile_payloads) - 1)]

    def build_headers(self, access_token: str, *, transaction_id: str | None = None, hashkey: str | None = None) -> dict[str, str]:
        return {"authorization": f"Bearer {access_token}"}


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _order(*, status: str = "submitted", broker_order_id: str = "0033196600") -> BrokerOrder:
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"test:{broker_order_id}",
        broker_order_id=broker_order_id,
        symbol="005930",
        side="buy",
        status=status,
        quantity=2,
        submitted_at=OBSERVED_AT,
    )


def test_reconcile_full_fill_is_idempotent_and_updates_position_snapshot() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order()
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "0033196600",
                "status": "filled",
                "broker_status": "체결",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "fills": [
                    {
                        "broker_fill_id": "0033196600:1",
                        "symbol": "005930",
                        "side": "buy",
                        "quantity": 2,
                        "price": "71000",
                        "filled_at": OBSERVED_AT.isoformat(),
                    }
                ],
                "observed_at": OBSERVED_AT.isoformat(),
                "payload": {"odno": "0033196600"},
            }
        ],
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("500000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[BrokerAccountHolding(symbol="005930", quantity=2, average_price=Decimal("71000"), sellable_quantity=2)],
        ),
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    position_result = sync_execution_positions_from_broker_snapshot(settings=settings, db=db, adapter=broker, symbols={"005930"})

    fills = db.scalars(select(ExecutionFill)).all()
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "005930"))
    assert first["status"] == "filled"
    assert first["fills_added"] == 1
    assert second["fills_added"] == 0
    assert len(fills) == 1
    assert fills[0].quantity == 2
    assert position_result["created"] == 1
    assert position is not None
    assert position.quantity == 2
    assert position.average_price == Decimal("71000.0000")
    engine.dispose()


def test_reconcile_partial_fill_and_open_order_states() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    partial_order = _order(broker_order_id="partial-1")
    open_order = _order(broker_order_id="open-1")
    db.add_all([partial_order, open_order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "partial-1",
                "status": "partially_filled",
                "filled_quantity": 1,
                "remaining_quantity": 1,
                "average_fill_price": "70000",
                "fills": [
                    {"broker_fill_id": "partial-1:1", "quantity": 1, "price": "70000", "filled_at": OBSERVED_AT.isoformat()}
                ],
                "observed_at": OBSERVED_AT.isoformat(),
            },
            {
                "broker_order_id": "open-1",
                "status": "open",
                "filled_quantity": 0,
                "remaining_quantity": 2,
                "observed_at": OBSERVED_AT.isoformat(),
            },
        ]
    )

    partial = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=partial_order)
    opened = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=open_order)

    assert partial["status"] == "partially_filled"
    assert partial["fills_added"] == 1
    assert opened["status"] == "open"
    assert opened["fills_added"] == 0
    assert len(db.scalars(select(ExecutionFill)).all()) == 1
    engine.dispose()


def test_monitoring_reconciles_stale_missing_position_from_broker_holdings_before_sell() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[BrokerAccountHolding(symbol="005930", quantity=1, average_price=Decimal("70000"), sellable_quantity=1)],
        )
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(settings=settings, engine=engine, session_factory=session_factory, broker_adapter_factory=lambda _settings: broker)
    client = TestClient(app)

    assert client.post("/controls/resume", json={"actor": "ops", "source": "test", "reason": "enable"}).status_code == 200
    plan = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-monitoring-reconcile-position",
            "source": "test",
            "market_date": "2026-05-13",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000",
                    "limit_price": "125000",
                    "time_in_force": "day",
                    "source_plan_item_id": "target-1",
                }
            ],
        },
    )
    assert plan.status_code == 201

    evaluation = client.post(
        "/monitoring/evaluate",
        json={"observed_at": OBSERVED_AT.isoformat(), "prices": [{"symbol": "005930", "price": "125100"}]},
    )

    assert evaluation.status_code == 200
    assert evaluation.json()["submitted_order_count"] == 1
    assert broker.order_calls[0].symbol == "005930"
    assert broker.order_calls[0].side == "sell"
    with session_factory() as db:
        position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "005930"))
        assert position is not None
        assert position.quantity == 1
        assert position.updated_from == "reconciliation"
    engine.dispose()
