from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerErrorClassification,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis import JsonHttpResponse, KisBrokerAdapter
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill, ExecutionPosition
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
        self.reconcile_calls: list[tuple[str, str]] = []
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

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
    ) -> dict[str, object]:
        assert access_token
        self.reconcile_calls.append((broker_order_id, exchange))
        if not self.reconcile_payloads:
            return {"broker_order_id": broker_order_id, "status": "open", "observed_at": OBSERVED_AT.isoformat()}
        return self.reconcile_payloads[min(len(self.reconcile_calls) - 1, len(self.reconcile_payloads) - 1)]

    def build_headers(self, access_token: str, *, transaction_id: str | None = None, hashkey: str | None = None) -> dict[str, str]:
        return {"authorization": f"Bearer {access_token}"}


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _order(
    *,
    status: str = "submitted",
    broker_order_id: str = "0033196600",
    request_metadata: dict[str, object] | None = None,
) -> BrokerOrder:
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
        request_metadata=request_metadata,
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


def test_account_snapshot_sync_zeroes_local_positions_absent_from_broker_snapshot() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="000660",
            quantity=3,
            average_price=Decimal("95000"),
            updated_from="test",
        )
    )
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[],
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
    )

    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "000660"))
    assert result["tracked_symbol_count"] == 1
    assert result["zeroed"] == 1
    assert position is not None
    assert position.quantity == 0
    assert position.last_reconciled_at is not None
    assert position.last_reconciled_at.replace(tzinfo=UTC) == OBSERVED_AT
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
                    "quantity_policy": {"type": "position_exit_from_entry", "linked_entry_item_key": "entry-1"},
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


def test_reconcile_cumulative_filled_quantity_adds_only_delta() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="cumulative-1")
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "cumulative-1",
                "status": "partially_filled",
                "filled_quantity": 1,
                "remaining_quantity": 1,
                "average_fill_price": "70000",
                "filled_at": OBSERVED_AT.isoformat(),
                "observed_at": OBSERVED_AT.isoformat(),
            },
            {
                "broker_order_id": "cumulative-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "filled_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
                "observed_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
            },
        ]
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.filled_at.asc())).all()
    assert first["fills_added"] == 1
    assert second["fills_added"] == 1
    assert [fill.quantity for fill in fills] == [1, 1]
    assert sum(fill.quantity for fill in fills) == 2
    engine.dispose()


class KisReconcileHttpClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def get_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout_seconds": timeout_seconds})
        return JsonHttpResponse(status_code=200, headers={}, payload=self.payload)

    def post_json(self, **_kwargs: object) -> JsonHttpResponse:
        raise AssertionError("reconciliation must not place or post orders")


def test_kis_reconcile_order_reports_cumulative_quantity_without_synthetic_fill() -> None:
    settings = Settings(
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
        kis_account_product_code="01",
    )
    http_client = KisReconcileHttpClient(
        {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "output1": [
                {
                    "odno": "0033196600",
                    "pdno": "005930",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "2",
                    "tot_ccld_qty": "2",
                    "rmn_qty": "0",
                    "avg_pric": "71000",
                    "ord_dt": "20260513",
                    "ord_tmd": "090501",
                    "ord_st_dvsn_cd_name": "체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order("top-secret-token", "0033196600")

    assert payload is not None
    assert payload["status"] == "filled"
    assert payload["filled_quantity"] == 2
    assert "fills" not in payload
    assert http_client.calls[0]["params"]["ODNO"] == "0033196600"


@pytest.mark.parametrize(
    ("runtime_mode", "expected_tr_id"),
    [("live", "TTTC0081R"), ("paper", "VTTC0081R")],
)
def test_kis_reconcile_order_uses_daily_ccld_endpoint_and_mode_tr_id(
    runtime_mode: str,
    expected_tr_id: str,
) -> None:
    settings = Settings(
        runtime_mode=runtime_mode,
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
        kis_account_product_code="01",
    )
    http_client = KisReconcileHttpClient(
        {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "output1": [
                {
                    "odno": "0033196600",
                    "pdno": "005930",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "1",
                    "tot_ccld_qty": "0",
                    "rmn_qty": "1",
                    "avg_pric": "0",
                    "ord_dt": "20260513",
                    "ord_tmd": "090501",
                    "ord_st_dvsn_cd_name": "미체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order("top-secret-token", "0033196600")

    assert payload is not None
    assert payload["status"] == "open"
    assert http_client.calls[0]["url"].endswith("/uapi/domestic-stock/v1/trading/inquire-daily-ccld")
    assert http_client.calls[0]["headers"]["tr_id"] == expected_tr_id
    assert http_client.calls[0]["params"]["EXCG_ID_DVSN_CD"] == "KRX"
    assert http_client.calls[0]["params"]["ODNO"] == "0033196600"


def test_kis_reconcile_order_uses_requested_exchange() -> None:
    settings = Settings(
        runtime_mode="live",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
        kis_account_product_code="01",
    )
    http_client = KisReconcileHttpClient(
        {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "output1": [
                {
                    "odno": "0042705600",
                    "pdno": "034730",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "1",
                    "tot_ccld_qty": "1",
                    "rmn_qty": "0",
                    "avg_pric": "668000",
                    "ord_dt": "20260602",
                    "ord_tmd": "161400",
                    "ord_st_dvsn_cd_name": "체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0042705600",
        exchange="NXT",
    )

    assert payload is not None
    assert payload["status"] == "filled"
    assert http_client.calls[0]["params"]["EXCG_ID_DVSN_CD"] == "NXT"


def test_reconcile_order_uses_route_exchange_from_order_metadata() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(
        broker_order_id="nxt-1",
        request_metadata={"route": {"exchange": "NXT", "session": "nxt_after"}},
    )
    db.add(order)
    db.commit()

    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "nxt-1",
                "status": "open",
                "observed_at": OBSERVED_AT.isoformat(),
                "request_metadata": {
                    "provider": "kis",
                    "params": {"EXCG_ID_DVSN_CD": "NXT"},
                },
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["status"] == "open"
    assert broker.reconcile_calls == [("nxt-1", "NXT")]
    assert order.request_metadata["route"]["exchange"] == "NXT"
    assert order.request_metadata["params"]["EXCG_ID_DVSN_CD"] == "NXT"
    engine.dispose()


def test_reconcile_repeated_identical_broker_failures_are_deduped() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="failed-1")
    db.add(order)
    db.commit()

    class FailingBroker(ReconciliationStubBroker):
        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
        ) -> dict[str, object]:
            assert access_token
            raise BrokerAdapterError(
                "KIS request failed",
                status_code=422,
                classification=BrokerErrorClassification.BROKER_REJECTED,
                retryable=False,
                code="404",
                request_metadata={
                    "provider": "kis",
                    "endpoint": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                    "tr_id": "VTTC0081R",
                },
                response_metadata={"status_code": 404},
                response_payload={},
                observed_at=OBSERVED_AT,
            )

    broker = FailingBroker()
    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "reconcile_failed")
    ).all()
    assert first["message"] == "KIS request failed"
    assert second["message"] == "KIS request failed"
    assert len(events) == 1
    assert events[0].payload is not None
    assert events[0].payload["request_metadata"]["tr_id"] == "VTTC0081R"
    assert order.last_error_message == "KIS request failed"
    engine.dispose()


def test_reconcile_repeated_generic_failures_are_deduped_with_unknown_classification() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="generic-failed-1")
    db.add(order)
    db.commit()

    class BuggyBroker(ReconciliationStubBroker):
        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
        ) -> dict[str, object]:  # type: ignore[override]
            assert access_token
            return "not-a-dict"  # type: ignore[return-value]

    broker = BuggyBroker()
    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "reconcile_failed")
    ).all()
    assert first["message"].startswith("ValueError: reconcile_order adapter returned")
    assert second["message"] == first["message"]
    assert len(events) == 1
    assert events[0].error_classification == "unknown"
    engine.dispose()
