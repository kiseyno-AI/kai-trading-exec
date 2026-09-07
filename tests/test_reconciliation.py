from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.api.schemas.execution import StaleBrokerOrderReconciliationRequest
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
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services import reconciliation as reconciliation_service
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.fill_idempotency import (
    duplicate_fill_group_candidate_key,
    execution_fill_dedupe_key,
)
from app.services.reconciliation import (
    reconcile_broker_order,
    reconcile_stale_nonterminal_broker_orders,
    sync_execution_positions_from_broker_snapshot,
)
from app.services.runtime import (
    RuntimeMetadata,
    build_runtime_daily_board,
    build_runtime_status,
)
from app.services.runtime_controls import ControlCommand, resume_runtime


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
        self.reconcile_calls: list[tuple[str, str, datetime | None]] = []
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
        submitted_at: datetime | None = None,
    ) -> dict[str, object]:
        assert access_token
        self.reconcile_calls.append((broker_order_id, exchange, submitted_at))
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
    side: str = "buy",
    request_metadata: dict[str, object] | None = None,
) -> BrokerOrder:
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"test:{broker_order_id}",
        broker_order_id=broker_order_id,
        symbol="005930",
        side=side,
        status=status,
        quantity=2,
        submitted_at=OBSERVED_AT,
        request_metadata=request_metadata,
    )


def _plan_backed_order(
    *,
    plan_id: str,
    side: str,
    intent: str,
    symbol: str = "005930",
    quantity: int = 2,
    broker_order_id: str = "reject-1",
    status: str = "submitted",
    risk_context: dict[str, object] | None = None,
) -> tuple[ExecutionPlan, ExecutionPlanItem, BrokerOrder]:
    plan = ExecutionPlan(
        external_plan_id=plan_id,
        source="test",
        mode="paper",
        market_date=OBSERVED_AT.date(),
    )
    item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol=symbol,
        side=side,
        intent=intent,
        status="broker_order_submitted",
        quantity_policy={"kind": "shares", "quantity": quantity},
        limit_price=Decimal("71000"),
        time_in_force="day",
        risk_context=risk_context,
    )
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"test:{broker_order_id}",
        broker_order_id=broker_order_id,
        symbol=symbol,
        side=side,
        status=status,
        quantity=quantity,
        limit_price=Decimal("71000"),
        submitted_at=OBSERVED_AT,
        request_metadata={
            "source": "test",
            "time_in_force": "day",
            "route": {"exchange": "KRX"},
        },
    )
    order.plan_item = item
    return plan, item, order


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
    events = db.scalars(
        select(BrokerOrderEvent)
        .where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_result",
        )
        .order_by(BrokerOrderEvent.observed_at.asc())
    ).all()
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "005930"))
    runtime_payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )
    assert first["status"] == "filled"
    assert first["fills_added"] == 1
    assert second["fills_added"] == 0
    assert len(fills) == 1
    assert len(events) == 2
    assert [event.normalized_status for event in events] == ["filled", "filled"]
    assert [event.payload["provider"] for event in events] == ["kis", "kis"]
    assert [event.payload["status"] for event in events] == ["filled", "filled"]
    assert events[0].payload["normalized_payload"] == {"odno": "0033196600"}
    assert events[1].payload["normalized_payload"] == {"odno": "0033196600"}
    assert fills[0].quantity == 2
    assert position_result["created"] == 1
    assert position is not None
    assert position.quantity == 2
    assert position.average_price == Decimal("71000.0000")
    assert runtime_payload["symbols"]["positions"] == ["005930"]
    engine.dispose()


def test_us_holding_discovery_preserves_position_truth_across_exchange_queries() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)

    class RoutedSnapshotBroker(ReconciliationStubBroker):
        def __init__(self) -> None:
            super().__init__()
            self.snapshot_calls: list[tuple[list[str] | None, str]] = []

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            self.snapshot_calls.append((symbols, exchange))
            holdings = (
                [
                    BrokerAccountHolding(
                        symbol="AAPL",
                        quantity=2,
                        average_price=Decimal("200"),
                        sellable_quantity=2,
                        payload={"ovrs_excg_cd": "NASD"},
                    )
                ]
                if exchange == "NASD"
                else []
            )
            return BrokerAccountSnapshot(
                available_cash=Decimal("1000"),
                observed_at=OBSERVED_AT,
                source=f"stub:{exchange}",
                holdings=holdings,
                payload={"currency": "USD", "exchange": exchange},
            )

    broker = RoutedSnapshotBroker()
    nasd_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
    )
    nyse_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NYSE",
        discover_holdings_only=True,
    )

    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "AAPL"))
    assert broker.snapshot_calls == [(None, "NASD"), (None, "NYSE")]
    assert nasd_result["created"] == 1
    assert nyse_result["zeroed"] == 0
    assert position is not None
    assert position.quantity == 2
    assert position.average_price == Decimal("200.0000")
    assert position.raw_payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": True,
    }
    engine.dispose()


def test_reconcile_repeated_fallback_fill_payload_is_idempotent() -> None:
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
                        "symbol": "005930",
                        "side": "buy",
                        "quantity": 2,
                        "price": "71000.0000",
                        "filled_at": OBSERVED_AT.isoformat(),
                    }
                ],
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ],
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(select(ExecutionFill)).all()
    assert first["fills_added"] == 1
    assert second["fills_added"] == 0
    assert len(fills) == 1
    assert fills[0].idempotency_key is not None
    engine.dispose()


def test_reconcile_does_not_duplicate_legacy_fill_without_idempotency_key() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order()
    order.fills.append(
        ExecutionFill(
            broker_order_pk=order.id,
            broker_fill_id=None,
            symbol="005930",
            side="buy",
            quantity=2,
            price=Decimal("71000.0000"),
            filled_at=OBSERVED_AT,
            payload={"source": "legacy"},
        )
    )
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
                        "symbol": "005930",
                        "side": "buy",
                        "quantity": 2,
                        "price": "71000",
                        "filled_at": OBSERVED_AT.isoformat(),
                    }
                ],
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ],
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["fills_added"] == 0
    assert len(db.scalars(select(ExecutionFill)).all()) == 1
    engine.dispose()


def test_reconcile_filled_order_marks_plan_item_filled() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    plan = ExecutionPlan(
        external_plan_id="plan-reconcile-filled-item",
        source="test",
        mode="paper",
    )
    item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="broker_order_submitted",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    order = _order()
    order.plan_item = item
    db.add(plan)
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
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ],
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    audit = db.scalar(
        select(ExecutionPlanItemAudit).where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "execution_plan_item_status_transition",
        )
    )
    assert result["status"] == "filled"
    assert item.status == "filled"
    assert audit is not None
    assert audit.source == "reconciliation"
    assert audit.summary["previous_status"] == "broker_order_submitted"
    assert audit.summary["status"] == "filled"
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
    assert [fill.price for fill in fills] == [Decimal("70000"), Decimal("72000")]
    assert [fill.quote_notional for fill in fills] == [Decimal("70000"), Decimal("72000")]
    assert sum(fill.quantity for fill in fills) == 2
    engine.dispose()


def test_reconcile_cumulative_share_fill_dedupes_preserved_existing_evidence() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="cumulative-preserved-duplicate-share")
    db.add(order)
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id=f"{prefix}-preserved-share-fill",
                broker_order_pk=order.id,
                broker_fill_id="actual-share-fill-1",
                symbol=order.symbol,
                side=order.side,
                quantity=1,
                quote_notional=Decimal("70000"),
                price=Decimal("70000"),
                filled_at=OBSERVED_AT,
            )
            for prefix in ("a", "b")
        ]
    )
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": order.broker_order_id,
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "filled_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
                "observed_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(
        select(ExecutionFill).order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()
    assert result["fills_added"] == 1
    assert len(fills) == 3
    assert fills[-1].quantity == 1
    assert fills[-1].quote_notional == Decimal("72000")
    assert fills[-1].price == Decimal("72000")
    assert fills[-1].payload["previous_quantity"] == 1
    assert fills[-1].payload["previous_quote_notional"] == "70000.0000"
    engine.dispose()


def test_reconcile_cumulative_base_fill_derives_incremental_price_from_notional_delta() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="alpaca-cumulative-base-1")
    order.symbol = "BTC/USD"
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "alpaca-cumulative-base-1",
                "status": "partially_filled",
                "filled_base_quantity": "1",
                "average_fill_price": "100",
                "filled_at": OBSERVED_AT.isoformat(),
                "observed_at": OBSERVED_AT.isoformat(),
            },
            {
                "broker_order_id": "alpaca-cumulative-base-1",
                "status": "filled",
                "filled_base_quantity": "2",
                "average_fill_price": "150",
                "filled_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
                "observed_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
            },
        ]
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    repeated = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.filled_at.asc())).all()
    assert first["fills_added"] == 1
    assert second["fills_added"] == 1
    assert repeated["fills_added"] == 0
    assert [fill.quantity for fill in fills] == [0, 0]
    assert [fill.base_quantity for fill in fills] == [Decimal("1"), Decimal("1")]
    assert [fill.quote_notional for fill in fills] == [Decimal("100"), Decimal("200")]
    assert [fill.price for fill in fills] == [Decimal("100"), Decimal("200")]
    assert fills[1].payload["price_derivation"] == "cumulative_notional_delta"
    assert fills[1].payload["previous_quote_notional"] == "100.0000"
    engine.dispose()


def test_reconcile_cumulative_base_fill_dedupes_preserved_existing_evidence() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="cumulative-preserved-duplicate-base")
    order.symbol = "BTC/USD"
    db.add(order)
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                id=f"{prefix}-preserved-base-fill",
                broker_order_pk=order.id,
                broker_fill_id="actual-base-fill-1",
                symbol=order.symbol,
                side=order.side,
                quantity=0,
                base_quantity=Decimal("1"),
                quote_notional=Decimal("100"),
                price=Decimal("100"),
                filled_at=OBSERVED_AT,
            )
            for prefix in ("a", "b")
        ]
    )
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": order.broker_order_id,
                "status": "filled",
                "filled_base_quantity": "2",
                "average_fill_price": "150",
                "filled_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
                "observed_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(
        select(ExecutionFill).order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()
    assert result["fills_added"] == 1
    assert len(fills) == 3
    assert fills[-1].base_quantity == Decimal("1")
    assert fills[-1].quote_notional == Decimal("200")
    assert fills[-1].price == Decimal("200")
    assert fills[-1].payload["previous_base_quantity"] == "1.000000000000"
    assert fills[-1].payload["previous_quote_notional"] == "100.0000"
    engine.dispose()


def test_reconcile_cumulative_base_fill_skips_non_positive_incremental_notional_with_audit() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="alpaca-cumulative-anomaly-1")
    order.symbol = "BTC/USD"
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "alpaca-cumulative-anomaly-1",
                "status": "partially_filled",
                "filled_base_quantity": "1",
                "average_fill_price": "100",
                "filled_at": OBSERVED_AT.isoformat(),
                "observed_at": OBSERVED_AT.isoformat(),
            },
            {
                "broker_order_id": "alpaca-cumulative-anomaly-1",
                "status": "partially_filled",
                "filled_base_quantity": "2",
                "average_fill_price": "40",
                "filled_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
                "observed_at": (OBSERVED_AT + timedelta(minutes=1)).isoformat(),
            },
        ]
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    fills = db.scalars(select(ExecutionFill).order_by(ExecutionFill.filled_at.asc())).all()
    events = db.scalars(
        select(BrokerOrderEvent)
        .where(BrokerOrderEvent.event_type == "reconcile_result")
        .order_by(BrokerOrderEvent.observed_at.asc(), BrokerOrderEvent.created_at.asc())
    ).all()
    assert first["fills_added"] == 1
    assert second["fills_added"] == 0
    assert len(fills) == 1
    anomalies = events[-1].payload["fill_delta_anomalies"]
    assert anomalies[0]["reason"] == "non_positive_incremental_notional"
    assert anomalies[0]["quantity_basis"] == "base_quantity"
    assert anomalies[0]["broker_order_id"] == "alpaca-cumulative-anomaly-1"
    engine.dispose()


def test_reconcile_stale_nonterminal_order_marks_closed_against_broker_truth() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    order = _order(broker_order_id="stale-filled-1")
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = stale_submitted_at
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-filled-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "stale_order_reconcile_classification"
        )
    ).all()
    assert result["candidate_order_count"] == 1
    assert result["counts"]["closed"] == 1
    assert result["items"][0]["classification"] == "closed"
    assert order.status == "filled"
    assert broker.reconcile_calls == [("stale-filled-1", "KRX", stale_submitted_at)]
    assert len(events) == 1
    assert events[0].error_classification == "closed"
    repair = db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    )
    assert repair is not None
    assert repair.payload["status_before"] == "submitted"
    assert repair.payload["status_after"] == "filled"
    engine.dispose()


def test_reconcile_stale_order_commits_operator_attribution_with_inner_transition(
    monkeypatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'stale-reconcile-attribution.db'}",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    db = session_factory()
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    order = _order(broker_order_id="stale-filled-attributed-1")
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = stale_submitted_at
    db.add(order)
    db.commit()
    order_id = order.id
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-filled-attributed-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    def raise_before_outer_commit(**kwargs) -> None:
        raise RuntimeError("interrupted before outer audit commit")

    monkeypatch.setattr(
        "app.services.reconciliation._record_stale_order_classification_event",
        raise_before_outer_commit,
    )

    with pytest.raises(RuntimeError, match="interrupted before outer audit commit"):
        reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            actor="Peter",
            operator_reason="operator requested read-only broker truth reconciliation",
        )
    db.rollback()
    db.close()

    with session_factory() as verification_db:
        persisted_order = verification_db.get(BrokerOrder, order_id)
        reconcile_event = verification_db.scalar(
            select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "reconcile_result")
        )
        outer_events = verification_db.scalars(
            select(BrokerOrderEvent).where(
                BrokerOrderEvent.event_type.in_(
                    ["stale_order_repair", "stale_order_reconcile_classification"]
                )
            )
        ).all()
        assert persisted_order is not None
        assert persisted_order.status == "filled"
        assert reconcile_event is not None
        assert reconcile_event.payload["actor"] == "Peter"
        assert (
            reconcile_event.payload["operator_reason"]
            == "operator requested read-only broker truth reconciliation"
        )
        assert outer_events == []
    engine.dispose()


def test_reconcile_stale_order_uses_submission_age_despite_recent_sync() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    order = _order(broker_order_id="stale-recently-synced-1")
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = datetime.now(UTC)
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-recently-synced-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    assert result["candidate_order_count"] == 1
    assert result["counts"]["closed"] == 1
    assert order.status == "filled"
    assert broker.reconcile_calls == [
        ("stale-recently-synced-1", "KRX", stale_submitted_at)
    ]
    engine.dispose()


def test_successful_current_reconciliation_terminalizes_absent_prior_session_order() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    submitted_at = OBSERVED_AT - timedelta(days=2)
    order = _order(status="open", broker_order_id="prior-session-absent-1")
    order.submitted_at = submitted_at
    order.last_broker_sync_at = submitted_at
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": order.broker_order_id,
                "status": "unknown",
                "broker_status": "unknown",
                "message": "matching order was not found in broker order query",
                "observed_at": OBSERVED_AT.isoformat(),
                "response_metadata": {"status_code": 200, "msg_cd": "0"},
                "payload": {"output": {"rt_cd": "0", "output1": []}},
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    repair = db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    )
    assert result["status"] == "stale_terminal_unknown"
    assert result["status_changed"] is True
    assert result["fills_added"] == 0
    assert order.status == "stale_terminal_unknown"
    assert order.fills == []
    assert repair is not None
    assert repair.payload["status_before"] == "open"
    assert repair.payload["status_after"] == "stale_terminal_unknown"
    assert repair.payload["observed_broker_code"] == "unknown"
    assert repair.payload["reason"] == "prior_session_order_absent_after_bounded_threshold"
    assert repair.payload["result"]["status_code"] == 200
    assert repair.payload["result"]["response_metadata"]["msg_cd"] == "0"
    engine.dispose()


def test_successful_absent_reconciliation_keeps_order_open_before_threshold() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    submitted_at = OBSERVED_AT - timedelta(minutes=30)
    order = _order(status="open", broker_order_id="recent-absent-1")
    order.submitted_at = submitted_at
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": order.broker_order_id,
                "status": "unknown",
                "broker_status": "unknown",
                "message": "matching order was not found in broker order query",
                "observed_at": OBSERVED_AT.isoformat(),
                "response_metadata": {"status_code": 200, "msg_cd": "0"},
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["status"] == "unknown"
    assert result["status_changed"] is False
    assert order.status == "open"
    assert db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    ) is None
    engine.dispose()


def test_reconcile_stale_submitted_kier_lookup_window_response_repairs_to_window_expired() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=10)
    order = _order(broker_order_id="kier2570-old-1")
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = stale_submitted_at
    db.add(order)
    db.commit()
    db.add(
        ExecutionFailure(
            mode=settings.runtime_mode,
            broker_order_pk=order.id,
            broker_name="kis",
            idempotency_key=order.idempotency_key,
            symbol=order.symbol,
            side=order.side,
            requested_quantity=order.quantity or 0,
            classification="broker_rejected",
            broker_error_classification="broker_rejected",
            broker_error_code="KIER2570",
            broker_error_message="조회일자를 확인하십시오",
            retryable=False,
            status="open",
            first_failed_at=stale_submitted_at,
            latest_failed_at=stale_submitted_at,
        )
    )
    db.commit()

    class LookupWindowBroker(ReconciliationStubBroker):
        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
            submitted_at: datetime | None = None,
        ) -> dict[str, object]:
            assert access_token
            self.reconcile_calls.append((broker_order_id, exchange, submitted_at))
            raise BrokerAdapterError(
                "조회일자를 확인하십시오",
                status_code=422,
                classification=BrokerErrorClassification.BROKER_REJECTED,
                retryable=False,
                code="KIER2570",
                request_metadata={
                    "provider": "kis",
                    "endpoint": "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                    "tr_id": "VTTC0081R",
                },
                response_payload={"msg_cd": "KIER2570", "msg1": "조회일자를 확인하십시오"},
                observed_at=OBSERVED_AT,
            )

    broker = LookupWindowBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        actor="Peter",
        operator_reason="issue #265: reconcile stale order with read-only broker inquiry",
    )

    repair = db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    )
    assert result["candidate_order_count"] == 1
    assert result["repaired_order_count"] == 1
    assert result["counts"]["reconcile-window-expired"] == 1
    assert result["items"][0]["classification"] == "reconcile-window-expired"
    assert result["items"][0]["observed_broker_code"] == "KIER2570"
    assert order.status == "reconcile_window_expired"
    assert order.last_error_classification == "broker_rejected"
    assert order.last_error_code == "KIER2570"
    assert broker.reconcile_calls == [("kier2570-old-1", "KRX", stale_submitted_at)]
    assert broker.order_calls == []
    assert repair is not None
    assert repair.broker_status == "KIER2570"
    assert repair.error_classification == "reconcile_window_expired"
    assert repair.payload["status_before"] == "submitted"
    assert repair.payload["status_after"] == "reconcile_window_expired"
    assert repair.payload["observed_broker_code"] == "KIER2570"
    assert repair.payload["classification"] == "reconcile_window_expired"
    assert repair.payload["reason"] == "broker_lookup_window_expired"
    assert repair.payload["actor"] == "Peter"
    assert repair.payload["operator_reason"].startswith("issue #265")
    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    assert failure is not None
    assert failure.classification == "broker_rejected"
    assert failure.broker_error_classification == "broker_rejected"
    assert failure.broker_error_code == "KIER2570"
    assert failure.latest_failed_at is not None
    assert failure.status == "resolved"
    assert failure.status_updated_by == "system"
    assert failure.status_updated_source == "stale_order_reconciliation"
    assert failure.status_updated_reason == "broker_lookup_window_expired"
    engine.dispose()


def test_reconcile_resolves_preexisting_open_failure_for_already_expired_order() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    expired_at = datetime.now(UTC) - timedelta(days=2)
    order = _order(status="reconcile_window_expired", broker_order_id="kier2570-expired-1")
    order.submitted_at = expired_at
    order.last_broker_sync_at = expired_at
    order.last_error_classification = "broker_rejected"
    order.last_error_code = "KIER2570"
    order.last_error_message = "조회일자를 확인하십시오"
    db.add(order)
    db.commit()
    failure = ExecutionFailure(
        mode=settings.runtime_mode,
        broker_order_pk=order.id,
        broker_name="kis",
        idempotency_key=order.idempotency_key,
        symbol=order.symbol,
        side=order.side,
        requested_quantity=order.quantity or 0,
        classification="broker_rejected",
        broker_error_classification="broker_rejected",
        broker_error_code="KIER2570",
        broker_error_message="조회일자를 확인하십시오",
        retryable=False,
        status="open",
        first_failed_at=expired_at,
        latest_failed_at=expired_at,
    )
    db.add(failure)
    db.commit()
    broker = ReconciliationStubBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    db.refresh(failure)
    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "expired_reconciliation_failure_resolved"
        )
    ).all()
    assert result["candidate_order_count"] == 0
    assert result["expired_failure_cleanup"]["candidate_failure_count"] == 1
    assert result["expired_failure_cleanup"]["resolved_failure_count"] == 1
    assert result["expired_failure_cleanup"]["items"][0]["action"] == "resolved"
    assert broker.reconcile_calls == []
    assert order.status == "reconcile_window_expired"
    assert failure.status == "resolved"
    assert failure.status_updated_by == "system"
    assert failure.status_updated_source == "stale_order_reconciliation_expired_order_cleanup"
    assert failure.status_updated_reason == "broker_lookup_window_expired"
    assert len(events) == 1
    assert events[0].broker_order_pk == order.id
    assert events[0].payload["execution_failure_id"] == failure.id
    assert events[0].payload["failure_status_before"] == "open"
    assert events[0].payload["failure_status_after"] == "resolved"

    second = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    events_after_second_run = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "expired_reconciliation_failure_resolved"
        )
    ).all()
    assert second["expired_failure_cleanup"]["candidate_failure_count"] == 0
    assert len(events_after_second_run) == 1
    assert broker.reconcile_calls == []
    engine.dispose()


def test_reconcile_does_not_resolve_unrelated_open_failure_for_expired_order() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    failed_at = datetime.now(UTC) - timedelta(days=2)
    order = _order(status="reconcile_window_expired", broker_order_id="unrelated-expired-1")
    order.submitted_at = failed_at
    order.last_broker_sync_at = failed_at
    order.last_error_classification = "broker_rejected"
    order.last_error_code = "INSUFFICIENT_CASH"
    order.last_error_message = "insufficient cash available for order"
    db.add(order)
    db.commit()
    failure = ExecutionFailure(
        mode=settings.runtime_mode,
        broker_order_pk=order.id,
        broker_name="kis",
        idempotency_key=order.idempotency_key,
        symbol=order.symbol,
        side=order.side,
        requested_quantity=order.quantity or 0,
        classification="insufficient_funds",
        broker_error_classification="broker_rejected",
        broker_error_code="INSUFFICIENT_CASH",
        broker_error_message="insufficient cash available for order",
        retryable=False,
        status="open",
        first_failed_at=failed_at,
        latest_failed_at=failed_at,
    )
    db.add(failure)
    db.commit()
    broker = ReconciliationStubBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    db.refresh(failure)
    cleanup = result["expired_failure_cleanup"]
    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "expired_reconciliation_failure_resolved"
        )
    ).all()
    assert result["candidate_order_count"] == 0
    assert cleanup["candidate_failure_count"] == 1
    assert cleanup["resolved_failure_count"] == 0
    assert cleanup["items"][0]["action"] == "skipped"
    assert cleanup["items"][0]["reason"] == "missing_lookup_window_expired_evidence"
    assert broker.reconcile_calls == []
    assert failure.status == "open"
    assert failure.status_updated_at is None
    assert events == []
    engine.dispose()


def test_reconcile_repeated_unknown_status_after_stale_threshold_repairs_to_unknown_terminal() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_observed_at = datetime.now(UTC) - timedelta(days=5)
    order = _order(status="open", broker_order_id="stale-unknown-repeat-1")
    order.submitted_at = stale_observed_at
    order.last_broker_sync_at = stale_observed_at
    order.last_error_classification = "unknown_status"
    order.last_error_code = "unknown"
    order.last_error_message = "unrecognized broker status"
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-unknown-repeat-1",
                "status": "not-in-open-order-window",
                "broker_status": "unknown",
                "message": "matching order was not found in broker order query",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    repair = db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    )
    unrecognized = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "reconcile_result_unrecognized_status"
        )
    )
    assert result["candidate_order_count"] == 1
    assert result["repaired_order_count"] == 1
    assert result["counts"]["stale-terminal-unknown"] == 1
    assert result["items"][0]["classification"] == "stale-terminal-unknown"
    assert result["items"][0]["reason"] == "repeated_unknown_status_after_stale_threshold"
    assert order.status == "stale_terminal_unknown"
    assert order.last_error_classification == "unknown_status"
    assert repair is not None
    assert repair.payload["status_before"] == "open"
    assert repair.payload["status_after"] == "stale_terminal_unknown"
    assert repair.payload["observed_broker_code"] == "not-in-open-order-window"
    assert unrecognized is not None
    engine.dispose()


def test_reconcile_stale_filled_order_syncs_execution_positions() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    order = _order(broker_order_id="stale-sell-filled-1", side="sell")
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = stale_submitted_at
    db.add(order)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="005930",
            quantity=2,
            average_price=Decimal("70000"),
            updated_from="reconciliation",
            last_reconciled_at=stale_submitted_at,
        )
    )
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-sell-filled-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ],
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[],
        ),
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "005930",
        )
    )
    assert result["counts"]["closed"] == 1
    assert result["position_sync"]["tracked_symbol_count"] == 1
    assert result["position_sync"]["zeroed"] == 1
    assert position is not None
    assert position.quantity == 0
    assert position.last_reconciled_at == OBSERVED_AT.replace(tzinfo=None)
    engine.dispose()


def test_reconcile_stale_nonterminal_orders_skips_wrong_broker_provider() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    kis_order = _order(broker_order_id="stale-kis-filled-1")
    kis_order.submitted_at = stale_submitted_at
    kis_order.last_broker_sync_at = stale_submitted_at
    upbit_order = BrokerOrder(
        broker_name="upbit",
        mode="paper",
        idempotency_key="upbit:stale-submitted-1",
        broker_order_id="upbit-stale-submitted-1",
        symbol="KRW-BTC",
        side="buy",
        status="submitted",
        quantity=0,
        base_quantity=Decimal("0.001"),
        quote_notional=Decimal("100000"),
        submitted_at=stale_submitted_at,
        last_broker_sync_at=stale_submitted_at,
        request_metadata={"provider": "upbit", "venue": "upbit", "asset_class": "crypto"},
    )
    db.add_all([kis_order, upbit_order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-kis-filled-1",
                "status": "filled",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    upbit_events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.broker_order_pk == upbit_order.id)
    ).all()
    assert result["candidate_order_count"] == 1
    assert result["counts"]["closed"] == 1
    assert result["items"][0]["broker_order_id"] == "stale-kis-filled-1"
    assert broker.reconcile_calls == [("stale-kis-filled-1", "KRX", stale_submitted_at)]
    assert kis_order.status == "filled"
    assert upbit_order.status == "submitted"
    assert upbit_order.last_error_classification is None
    assert upbit_order.last_error_message is None
    assert upbit_order.last_broker_sync_at == stale_submitted_at
    assert upbit_events == []
    engine.dispose()


def test_reconcile_stale_retry_pending_without_broker_id_is_failed_to_reconcile() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=5)
    plan, item, live_order = _plan_backed_order(
        plan_id="plan-stale-duplicate-retry",
        side="buy",
        intent="intraday_watch_buy",
        broker_order_id="live-open-1",
        status="open",
    )
    live_order.submitted_at = datetime.now(UTC)
    live_order.last_broker_sync_at = datetime.now(UTC)
    retry_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="retry:duplicate-local",
        broker_order_id=None,
        symbol=item.symbol,
        side="buy",
        status="retry_pending",
        quantity=2,
        submitted_at=stale_submitted_at,
        last_broker_sync_at=stale_submitted_at,
    )
    retry_order.plan_item = item
    db.add_all([plan, retry_order])
    db.commit()
    broker = ReconciliationStubBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "stale_order_reconcile_classification"
        )
    ).all()
    audit = db.scalar(
        select(ExecutionPlanItemAudit).where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "execution_plan_item_status_transition",
        )
    )
    assert result["candidate_order_count"] == 1
    assert result["repaired_order_count"] == 1
    assert result["counts"]["manual-review-required"] == 1
    assert result["items"][0]["classification"] == "manual-review-required"
    assert result["items"][0]["reason"] == "missing_broker_order_id"
    assert retry_order.status == "manual_review_required"
    assert item.status == "broker_order_failed"
    assert retry_order.last_error_classification == "missing_broker_order_id"
    assert broker.reconcile_calls == []
    assert len(events) == 1
    assert events[0].error_classification == "manual_review_required"
    assert audit is not None
    assert audit.source == "reconciliation"
    assert audit.summary["previous_status"] == "broker_order_submitted"
    assert audit.summary["status"] == "broker_order_failed"
    assert audit.payload["status"] == "manual_review_required"
    assert audit.payload["classification"] == "manual_review_required"
    repair = db.scalar(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "stale_order_repair")
    )
    assert repair is not None
    assert repair.payload["status_before"] == "retry_pending"
    assert repair.payload["status_after"] == "manual_review_required"
    engine.dispose()


def test_reconcile_stale_orders_dry_run_summarizes_repairs_without_mutating() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=5)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="dry-run-retry-missing-id",
        broker_order_id=None,
        symbol="031330",
        side="sell",
        status="retry_pending",
        quantity=2,
        submitted_at=stale_submitted_at,
        last_broker_sync_at=stale_submitted_at,
    )
    with session_factory() as db:
        db.add(order)
        db.commit()
        order_pk = order.id
    broker = ReconciliationStubBroker()
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: broker,
    )
    client = TestClient(app)

    response = client.post("/orders/reconcile-stale", json={"dry_run": True})
    unaudited_apply = client.post("/orders/reconcile-stale", json={})

    assert response.status_code == 200
    assert unaudited_apply.status_code == 422
    assert unaudited_apply.json()["detail"] == (
        "stale reconciliation apply requires actor and reason"
    )
    result = response.json()
    with session_factory() as db:
        persisted = db.get(BrokerOrder, order_pk)
        events = db.scalars(select(BrokerOrderEvent)).all()
        assert persisted is not None
        assert persisted.status == "retry_pending"
        assert persisted.last_error_classification is None
    assert result["dry_run"] is True
    assert result["candidate_order_count"] == 1
    assert result["would_repair_order_count"] == 1
    assert result["repaired_order_count"] == 0
    assert result["items"][0]["repair_action"] == "would_repair"
    assert result["items"][0]["status_before"] == "retry_pending"
    assert result["items"][0]["status_after"] == "manual_review_required"
    assert broker.reconcile_calls == []
    assert events == []
    engine.dispose()


def test_reconcile_adapter_error_event_keeps_attribution_after_outer_interruption(
    tmp_path, monkeypatch
) -> None:
    database_url = f"sqlite:///{tmp_path / 'reconcile-attribution.db'}"
    settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=5)
    with session_factory() as db:
        order = _order(broker_order_id="adapter-error-attribution")
        order.submitted_at = stale_submitted_at
        order.last_broker_sync_at = stale_submitted_at
        db.add(order)
        db.commit()
        order_pk = order.id

    class FailingReconcileBroker(ReconciliationStubBroker):
        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
            submitted_at: datetime | None = None,
        ) -> dict[str, object]:
            raise BrokerAdapterError(
                "broker lookup unavailable",
                status_code=503,
                classification=BrokerErrorClassification.NETWORK,
                retryable=True,
                code="lookup_timeout",
                observed_at=OBSERVED_AT,
            )

    def interrupt_before_outer_classification_commit(**kwargs) -> None:
        raise RuntimeError("interrupted before classification commit")

    monkeypatch.setattr(
        reconciliation_service,
        "_record_stale_order_classification_event",
        interrupt_before_outer_classification_commit,
    )

    with pytest.raises(RuntimeError, match="interrupted before classification commit"):
        with session_factory() as db:
            reconcile_stale_nonterminal_broker_orders(
                settings=settings,
                db=db,
                adapter=FailingReconcileBroker(),
                actor="ops-desk",
                operator_reason="investigate stale broker lookup",
            )

    with session_factory() as db:
        event = db.scalar(
            select(BrokerOrderEvent).where(
                BrokerOrderEvent.broker_order_pk == order_pk,
                BrokerOrderEvent.event_type == "reconcile_failed",
            )
        )
        classifications = db.scalars(
            select(BrokerOrderEvent).where(
                BrokerOrderEvent.broker_order_pk == order_pk,
                BrokerOrderEvent.event_type == "stale_order_reconcile_classification",
            )
        ).all()
        assert event is not None
        assert event.payload["actor"] == "ops-desk"
        assert event.payload["operator_reason"] == "investigate stale broker lookup"
        assert event.error_classification == "network"
        assert classifications == []
    engine.dispose()


@pytest.mark.parametrize("field", ["actor", "reason"])
def test_stale_reconciliation_schema_strips_and_rejects_whitespace_audit_fields(
    field: str,
) -> None:
    with pytest.raises(ValidationError, match="field must not be empty"):
        StaleBrokerOrderReconciliationRequest.model_validate({field: "   "})

    command = StaleBrokerOrderReconciliationRequest.model_validate(
        {"actor": "  operator  ", "reason": "  reconcile broker truth  "}
    )

    assert command.actor == "operator"
    assert command.reason == "reconcile broker truth"


@pytest.mark.parametrize("field", ["actor", "reason"])
def test_reconcile_stale_orders_api_rejects_whitespace_audit_fields(field: str) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: ReconciliationStubBroker(),
    )
    client = TestClient(app)
    payload = {
        "actor": "operator",
        "reason": "reconcile broker truth",
        field: "   ",
    }

    response = client.post("/orders/reconcile-stale", json=payload)

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["loc"] == ["body", field]
    assert error["type"] == "value_error"
    engine.dispose()


def test_runtime_and_daily_board_open_order_counts_exclude_repaired_stale_states() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    observed_at = datetime.now(UTC)
    plan = ExecutionPlan(
        external_plan_id="plan-repaired-open-counts",
        source="test",
        mode="paper",
        market_date=observed_at.date(),
    )
    statuses_by_symbol = {
        "005930": "submitted",
        "078930": "reconcile_window_expired",
        "031330": "stale_terminal_unknown",
        "001450": "manual_review_required",
    }
    for index, (symbol, status) in enumerate(statuses_by_symbol.items()):
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=index,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        order = BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key=f"open-count:{symbol}",
            broker_order_id=f"order-{symbol}",
            symbol=symbol,
            side="buy",
            status=status,
            quantity=1,
            submitted_at=observed_at,
            last_broker_sync_at=observed_at,
        )
        order.plan_item = item
    db.add(plan)
    db.commit()

    runtime_payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )
    board = build_runtime_daily_board(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    symbols = {row["symbol"]: row for row in board["symbols"]}
    assert runtime_payload["counts"]["open_orders"] == 1
    assert board["totals"]["open_order_symbol_count"] == 1
    assert symbols["005930"]["open_order_count"] == 1
    assert symbols["078930"]["open_order_count"] == 0
    assert symbols["031330"]["open_order_count"] == 0
    assert symbols["001450"]["open_order_count"] == 0
    engine.dispose()


def test_runtime_status_reports_position_truth_freshness_details() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine, db = _session(settings)
    stale_reconciled_at = datetime.now(UTC) - timedelta(hours=2)
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="filled-buy-freshness",
            broker_order_id="filled-buy-freshness",
            symbol="319660",
            side="buy",
            status="filled",
            quantity=4,
            submitted_at=stale_reconciled_at + timedelta(minutes=5),
        )
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="319660",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=stale_reconciled_at,
        )
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    freshness = payload["freshness"]["position_truth"]
    assert freshness["stale_count"] == 1
    assert freshness["stale_symbols"] == ["319660"]
    assert freshness["oldest_age_seconds"] is not None
    assert freshness["last_reconciled_at"] == stale_reconciled_at.astimezone(UTC).isoformat()
    assert freshness["broker_truth_reachable"] is False
    engine.dispose()


def test_runtime_status_does_not_count_historical_flat_positions_as_stale() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine, db = _session(settings)
    stale_reconciled_at = datetime.now(UTC) - timedelta(days=30)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="000660",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=stale_reconciled_at,
            raw_payload={"source": "broker"},
        )
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    freshness = payload["freshness"]["position_truth"]
    assert freshness["stale_count"] == 0
    assert freshness["stale_symbols"] == []
    assert freshness["trusted_flat_count"] == 1
    assert freshness["tracked_symbol_count"] == 0
    engine.dispose()


def test_runtime_status_reports_actionable_stale_nonterminal_orders() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_observed_at = datetime.now(UTC) - timedelta(days=3)
    recent_observed_at = datetime.now(UTC)
    db.add_all(
        [
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="stale-submitted-with-broker-id",
                broker_order_id="stale-submitted-1",
                symbol="005930",
                side="buy",
                status="submitted",
                quantity=1,
                submitted_at=stale_observed_at,
                last_broker_sync_at=stale_observed_at,
            ),
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="stale-retry-missing-broker-id",
                broker_order_id=None,
                symbol="031330",
                side="sell",
                status="retry_pending",
                quantity=2,
                submitted_at=stale_observed_at,
                last_broker_sync_at=stale_observed_at,
            ),
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="recent-submitted-order",
                broker_order_id="recent-submitted-1",
                symbol="035420",
                side="buy",
                status="submitted",
                quantity=1,
                submitted_at=recent_observed_at,
                last_broker_sync_at=recent_observed_at,
            ),
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="recent-unknown-status-order",
                broker_order_id="recent-unknown-1",
                symbol="005380",
                side="buy",
                status="submitted",
                quantity=1,
                submitted_at=recent_observed_at,
                last_broker_sync_at=recent_observed_at,
                last_error_classification="unknown_status",
                last_error_code="unknown",
                last_error_message="unrecognized broker status",
            ),
            BrokerOrder(
                broker_name="kis",
                mode="live",
                idempotency_key="live-stale-submitted-order",
                broker_order_id="live-stale-1",
                symbol="000660",
                side="buy",
                status="submitted",
                quantity=1,
                submitted_at=stale_observed_at,
                last_broker_sync_at=stale_observed_at,
            ),
        ]
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    stale_orders = payload["freshness"]["broker_orders"]["stale_nonterminal"]
    assert stale_orders["count"] == 3
    assert stale_orders["symbols"] == ["005380", "005930", "031330"]
    assert stale_orders["needs_reconciliation_count"] == 2
    assert stale_orders["manual_review_count"] == 1
    assert stale_orders["missing_broker_order_id_count"] == 1
    assert payload["counts"]["open_orders"] == 1
    items_by_key = {item["idempotency_key"]: item for item in stale_orders["items"]}
    assert items_by_key["stale-submitted-with-broker-id"]["action"] == "reconcile_broker_truth"
    assert items_by_key["stale-retry-missing-broker-id"]["action"] == "manual_review"
    assert items_by_key["stale-retry-missing-broker-id"]["reason"] == "missing_broker_order_id"
    assert items_by_key["recent-unknown-status-order"]["action"] == "reconcile_broker_truth"
    assert items_by_key["recent-unknown-status-order"]["reason"] == "unknown_status"
    alerts = payload["actionable_alerts"]
    assert alerts["status"] == "needs_attention"
    assert alerts["critical_count"] == 1
    assert alerts["warning_count"] >= 1
    alert_kinds = {alert["kind"] for alert in alerts["alerts"]}
    assert "stale_broker_orders_need_reconciliation" in alert_kinds
    assert "stale_broker_orders_need_manual_review" in alert_kinds
    engine.dispose()


def test_runtime_status_immediately_surfaces_unknown_submit_for_manual_review() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    observed_at = datetime.now(UTC)
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="unknown-submit-visible",
            broker_order_id=None,
            symbol="005930",
            side="buy",
            status="manual_review_required",
            quantity=1,
            limit_price=Decimal("70000"),
            submitted_at=None,
            last_broker_sync_at=observed_at,
            last_error_classification="submit_outcome_unknown",
            last_error_code="SUBMIT_OUTCOME_UNKNOWN",
            last_error_message="manual broker reconciliation is required",
            last_error_retryable=False,
        )
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=observed_at),
        db=db,
    )

    attention = payload["freshness"]["broker_orders"]["stale_nonterminal"]
    assert attention["count"] == 1
    assert attention["manual_review_count"] == 1
    assert attention["needs_reconciliation_count"] == 0
    assert attention["items"][0]["action"] == "manual_review"
    assert attention["items"][0]["reason"] == "submit_outcome_unknown"
    alerts = payload["actionable_alerts"]
    assert alerts["critical_count"] == 1
    assert alerts["alerts"][0]["kind"] == "stale_broker_orders_need_manual_review"
    engine.dispose()


def test_runtime_status_reports_pnl_fill_quality_alerts() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    engine, db = _session(settings)
    filled_at = datetime.now(UTC)
    missing_fill_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-missing-fill",
        broker_order_id="missing-fill-1",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=filled_at,
    )
    duplicate_fill_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-duplicate-fill",
        broker_order_id="duplicate-fill-1",
        symbol="282330",
        side="buy",
        status="filled",
        quantity=2,
        submitted_at=filled_at,
    )
    distinct_broker_fill_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-distinct-broker-fills",
        broker_order_id="distinct-broker-fills-1",
        symbol="000660",
        side="buy",
        status="filled",
        quantity=2,
        submitted_at=filled_at,
    )
    db.add_all([missing_fill_order, duplicate_fill_order, distinct_broker_fill_order])
    db.flush()
    db.add_all(
        [
            ExecutionFill(
                broker_order_pk=duplicate_fill_order.id,
                symbol="282330",
                side="buy",
                quantity=2,
                price=Decimal("125400.0000"),
                filled_at=filled_at,
            ),
            ExecutionFill(
                broker_order_pk=distinct_broker_fill_order.id,
                broker_fill_id="distinct-fill-1",
                symbol="000660",
                side="buy",
                quantity=2,
                price=Decimal("125400.0000"),
                filled_at=filled_at,
            ),
            ExecutionFill(
                broker_order_pk=distinct_broker_fill_order.id,
                broker_fill_id="distinct-fill-2",
                symbol="000660",
                side="buy",
                quantity=2,
                price=Decimal("125400.0000"),
                filled_at=filled_at,
            ),
            ExecutionFill(
                broker_order_pk=duplicate_fill_order.id,
                symbol="282330",
                side="buy",
                quantity=2,
                price=Decimal("125400.0000"),
                filled_at=filled_at,
            ),
        ]
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    fill_quality = payload["freshness"]["broker_orders"]["fill_quality"]
    assert fill_quality["missing_filled_order_fill_count"] == 1
    assert fill_quality["missing_filled_order_fill_items"][0]["symbol"] == "105560"
    assert fill_quality["duplicate_fill_group_count"] == 1
    assert fill_quality["duplicate_fill_groups"][0]["symbol"] == "282330"

    alerts = payload["actionable_alerts"]
    assert alerts["status"] == "needs_attention"
    assert alerts["critical_count"] == 1
    assert alerts["warning_count"] >= 1
    alerts_by_kind = {alert["kind"]: alert for alert in alerts["alerts"]}
    assert alerts_by_kind["filled_broker_orders_missing_execution_fills"]["symbols"] == ["105560"]
    assert alerts_by_kind["duplicate_execution_fill_groups"]["symbols"] == ["282330"]
    engine.dispose()


def test_runtime_status_matches_duplicate_fill_reviews_by_candidate_key() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    engine, db = _session(settings)
    filled_at = datetime.now(UTC)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-two-duplicate-fill-groups",
        broker_order_id="duplicate-fill-shared-order",
        symbol="282330",
        side="buy",
        status="filled",
        quantity=4,
        submitted_at=filled_at,
    )
    db.add(order)
    db.flush()
    first_group = [
        ExecutionFill(
            id="a-shared-order-duplicate-fill-one",
            broker_order_pk=order.id,
            broker_fill_id="shared-fill-one",
            symbol="282330",
            side="buy",
            quantity=1,
            price=Decimal("125400.0000"),
            filled_at=filled_at,
        ),
        ExecutionFill(
            id="b-shared-order-duplicate-fill-one",
            broker_order_pk=order.id,
            broker_fill_id="shared-fill-one",
            symbol="282330",
            side="buy",
            quantity=1,
            price=Decimal("125400.0000"),
            filled_at=filled_at,
        ),
    ]
    second_group = [
        ExecutionFill(
            id="a-shared-order-duplicate-fill-two",
            broker_order_pk=order.id,
            broker_fill_id="shared-fill-two",
            symbol="282330",
            side="buy",
            quantity=1,
            price=Decimal("125500.0000"),
            filled_at=filled_at + timedelta(seconds=1),
        ),
        ExecutionFill(
            id="b-shared-order-duplicate-fill-two",
            broker_order_pk=order.id,
            broker_fill_id="shared-fill-two",
            symbol="282330",
            side="buy",
            quantity=1,
            price=Decimal("125500.0000"),
            filled_at=filled_at + timedelta(seconds=1),
        ),
    ]
    db.add_all([*first_group, *second_group])
    db.flush()
    db.add_all(
        [
            RuntimeRemediationEvent(
                mode="paper",
                candidate_type="duplicate_fill_group",
                candidate_key=duplicate_fill_group_candidate_key(
                    group_key=execution_fill_dedupe_key(first_group[0]),
                    fill_ids=[fill.id for fill in first_group],
                ),
                subject_id=order.id,
                outcome="non_destructive_isolation",
                actor="first-reviewer",
                reason="first group isolated",
                evidence_summary={"fills_deleted": 0},
                occurred_at=filled_at,
            ),
            RuntimeRemediationEvent(
                mode="paper",
                candidate_type="duplicate_fill_group",
                candidate_key=duplicate_fill_group_candidate_key(
                    group_key=execution_fill_dedupe_key(second_group[0]),
                    fill_ids=[fill.id for fill in second_group],
                ),
                subject_id=order.id,
                outcome="manual_review_required",
                actor="second-reviewer",
                reason="second group needs review",
                evidence_summary={"fills_deleted": 0},
                occurred_at=filled_at,
            ),
            RuntimeRemediationEvent(
                mode="paper",
                candidate_type="duplicate_fill_group",
                candidate_key=duplicate_fill_group_candidate_key(
                    group_key="execution_fill:stale-order-level-key",
                    fill_ids=[fill.id for fill in first_group],
                ),
                subject_id=order.id,
                outcome="manual_review_required",
                actor="wrong-actor",
                reason="wrong group review must not leak",
                evidence_summary={"fills_deleted": 0},
                occurred_at=filled_at + timedelta(minutes=1),
            ),
            RuntimeRemediationEvent(
                mode="paper",
                candidate_type="duplicate_fill_group",
                candidate_key=duplicate_fill_group_candidate_key(
                    group_key="execution_fill:stale-order-level-key",
                    fill_ids=[fill.id for fill in second_group],
                ),
                subject_id=order.id,
                outcome="manual_review_required",
                actor="also-wrong",
                reason="another wrong group review must not leak",
                evidence_summary={"fills_deleted": 0},
                occurred_at=filled_at + timedelta(minutes=2),
            ),
        ]
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    groups = payload["freshness"]["broker_orders"]["fill_quality"]["duplicate_fill_groups"]
    assert len(groups) == 2
    groups_by_fill_id = {group["broker_fill_id"]: group for group in groups}
    assert groups_by_fill_id["shared-fill-one"]["review_outcome"] == "non_destructive_isolation"
    assert groups_by_fill_id["shared-fill-one"]["review_actor"] == "first-reviewer"
    assert groups_by_fill_id["shared-fill-one"]["review_reason"] == "first group isolated"
    assert groups_by_fill_id["shared-fill-two"]["review_outcome"] == "manual_review_required"
    assert groups_by_fill_id["shared-fill-two"]["review_actor"] == "second-reviewer"
    assert groups_by_fill_id["shared-fill-two"]["review_reason"] == "second group needs review"
    engine.dispose()


def test_missing_fill_alert_symbols_exclude_reviewed_and_ignore_sample_limit() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    engine, db = _session(settings)
    submitted_at = datetime.now(UTC)
    reviewed_orders = [
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key=f"reviewed-missing-fill-{index}",
            broker_order_id=f"reviewed-missing-fill-{index}",
            symbol=f"REVIEWED{index:02d}",
            side="buy",
            status="filled",
            quantity=1,
            submitted_at=submitted_at + timedelta(minutes=index),
        )
        for index in range(20)
    ]
    unreviewed_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="older-actionable-missing-fill",
        broker_order_id="older-actionable-missing-fill",
        symbol="ACTIONABLE",
        side="buy",
        status="filled",
        quantity=1,
        submitted_at=submitted_at - timedelta(days=1),
    )
    db.add_all([*reviewed_orders, unreviewed_order])
    db.flush()
    db.add_all([
        RuntimeRemediationEvent(
            mode="paper",
            candidate_type="missing_execution_fill",
            candidate_key=f"{order.id}:missing",
            subject_id=order.id,
            outcome="manual_review_required",
            actor="Peter",
            reason="authoritative fill evidence unavailable",
            evidence_summary={"fills_fabricated": 0},
            occurred_at=submitted_at,
        )
        for order in reviewed_orders
    ])
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
    )

    fill_quality = payload["freshness"]["broker_orders"]["fill_quality"]
    assert fill_quality["missing_filled_order_fill_count"] == 21
    assert fill_quality["missing_filled_order_fill_unreviewed_count"] == 1
    assert fill_quality["missing_filled_order_fill_manual_review_count"] == 20
    assert fill_quality["missing_filled_order_fill_truncated"] is True
    assert all(
        item["review_outcome"] == "manual_review_required"
        for item in fill_quality["missing_filled_order_fill_items"]
    )
    assert fill_quality["missing_filled_order_fill_unreviewed_symbols"] == ["ACTIONABLE"]
    alerts_by_kind = {
        alert["kind"]: alert for alert in payload["actionable_alerts"]["alerts"]
    }
    assert alerts_by_kind["filled_broker_orders_missing_execution_fills"] == {
        "kind": "filled_broker_orders_missing_execution_fills",
        "severity": "critical",
        "count": 1,
        "symbols": ["ACTIONABLE"],
        "message": (
            "filled broker orders are missing execution_fills rows, "
            "so FIFO realized P/L is incomplete"
        ),
    }
    engine.dispose()


def test_fast_autonomous_runtime_status_skips_fill_quality_alerts() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        autonomous_runtime_enabled=True,
    )
    engine, db = _session(settings)
    filled_at = datetime.now(UTC)
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="fast-filled-order-missing-fill",
            broker_order_id="fast-missing-fill-1",
            symbol="105560",
            side="buy",
            status="filled",
            quantity=1,
            submitted_at=filled_at,
        )
    )
    db.commit()

    payload = build_runtime_status(
        settings=settings,
        runtime=RuntimeMetadata(started_at=datetime.now(UTC)),
        db=db,
        include_expensive_sections=False,
    )

    assert "fill_quality" not in payload["freshness"]["broker_orders"]
    alerts_by_kind = {alert["kind"]: alert for alert in payload["actionable_alerts"]["alerts"]}
    assert "filled_broker_orders_missing_execution_fills" not in alerts_by_kind
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
    assert payload["filled_at"] == "2026-05-13T00:05:01+00:00"
    assert "fills" not in payload
    assert http_client.calls[0]["params"]["ODNO"] == "0033196600"


def test_kis_reconcile_order_uses_avg_prvs_as_average_fill_price() -> None:
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
                    "odno": "0033196600",
                    "pdno": "005930",
                    "sll_buy_dvsn_cd": "01",
                    "ord_qty": "2",
                    "tot_ccld_qty": "2",
                    "rmn_qty": "0",
                    "avg_prvs": "71000",
                    "ord_unpr": "0",
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
    assert payload["average_fill_price"] == Decimal("71000")


def test_kis_reconcile_order_treats_sell_rejection_kind_as_rejected() -> None:
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
                    "odno": "0007266500",
                    "pdno": "000815",
                    "sll_buy_dvsn_cd": "01",
                    "sll_buy_dvsn_cd_name": "매도거부",
                    "ord_qty": "1",
                    "tot_ccld_qty": "0",
                    "rmn_qty": "0",
                    "avg_prvs": "0",
                    "ord_unpr": "394000",
                    "ord_dt": "20260608",
                    "ord_tmd": "090717",
                    "ord_st_dvsn_cd_name": "체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order("top-secret-token", "0007266500")

    assert payload is not None
    assert payload["status"] == "rejected"
    assert payload["broker_status"] == "거부"
    assert payload["filled_quantity"] == 0
    assert "fills" not in payload


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


def test_kis_reconcile_order_uses_submitted_date_as_inquiry_start() -> None:
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
                    "odno": "0002761000",
                    "pdno": "105560",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "1",
                    "tot_ccld_qty": "1",
                    "rmn_qty": "0",
                    "avg_pric": "175000",
                    "ord_dt": "20260723",
                    "ord_tmd": "090320",
                    "ord_st_dvsn_cd_name": "체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0002761000",
        submitted_at=datetime(2026, 7, 23, 9, 3, 20, tzinfo=UTC),
    )

    assert payload is not None
    assert payload["status"] == "filled"
    assert http_client.calls[0]["params"]["INQR_STRT_DT"] == "20260723"
    assert http_client.calls[0]["params"]["INQR_END_DT"] >= "20260723"


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
    assert broker.reconcile_calls == [("nxt-1", "NXT", OBSERVED_AT)]
    assert order.request_metadata["route"]["exchange"] == "NXT"
    assert order.request_metadata["params"]["EXCG_ID_DVSN_CD"] == "NXT"
    engine.dispose()


def test_reconcile_order_preserves_kis_us_exchange_from_plan_context() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    plan, _item, order = _plan_backed_order(
        plan_id="plan-us-reconcile",
        side="buy",
        intent="next_open_buy",
        symbol="AAPL",
        broker_order_id="us-order-1",
        risk_context={"exchange": "NASD", "asset_class": "us_equity"},
    )
    order.asset_class = "us_equity"
    order.venue = "NASD"
    order.request_metadata = {"source": "test"}
    db.add(plan)
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "us-order-1",
                "status": "open",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["status"] == "open"
    assert broker.reconcile_calls == [("us-order-1", "NASD", OBSERVED_AT)]
    engine.dispose()


def test_reconcile_order_fails_closed_for_us_equity_without_exchange() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(broker_order_id="us-order-missing-exchange")
    order.symbol = "AAPL"
    order.asset_class = "us_equity"
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker()

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["status_code"] == 503
    assert result["status"] == "submitted"
    assert broker.reconcile_calls == []
    assert "explicit NASD, NYSE, or AMEX" in str(result["message"])
    engine.dispose()


def test_reconcile_order_passes_order_submitted_at_to_adapter() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    submitted_at = datetime(2026, 7, 23, 9, 3, 20, tzinfo=UTC)
    order = _order(
        broker_order_id="0002761000",
        request_metadata={"route": {"exchange": "KRX"}},
    )
    order.submitted_at = submitted_at
    db.add(order)
    db.commit()

    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "0002761000",
                "status": "filled",
                "filled_quantity": 1,
                "average_fill_price": "175000",
                "observed_at": OBSERVED_AT.isoformat(),
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    assert result["status"] == "filled"
    assert broker.reconcile_calls == [("0002761000", "KRX", submitted_at)]
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


def test_reconcile_kospi_circuit_breaker_sell_reject_schedules_retry() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-kospi-circuit-breaker-retry",
        side="sell",
        intent="stop_sell",
        symbol="009180",
        quantity=325,
        broker_order_id="kospi-cb-1",
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "kospi-cb-1",
                "status": "rejected",
                "broker_status": "매도거부",
                "message": "KOSPI circuit breaker trading halt",
                "observed_at": OBSERVED_AT.isoformat(),
                "market_state": {
                    "state": "circuit_breaker",
                    "reason": "KOSPI circuit breaker trading halt",
                    "normal_ordering_available": False,
                },
                "payload": {"odno": "kospi-cb-1", "sll_buy_dvsn_cd_name": "매도거부"},
            }
        ]
    )

    result = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id))
    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "retry_decision")
    ).all()
    audits = db.scalars(
        select(ExecutionPlanItemAudit)
        .where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "execution_plan_item_status_transition",
        )
        .order_by(ExecutionPlanItemAudit.occurred_at.asc())
    ).all()
    assert result["status"] == "retry_pending"
    assert order.status == "retry_pending"
    assert item.status == "broker_order_retry_pending"
    assert failure is not None
    assert failure.classification == "temporary_market_state"
    assert failure.retry_decision_status == "scheduled"
    assert failure.retry_decision_reason == "temporary_circuit_breaker"
    assert failure.retry_next_at is not None
    assert _utc(failure.retry_next_at) == OBSERVED_AT + timedelta(minutes=20)
    assert job is not None
    assert job.job_type == "broker_order_retry"
    assert job.status == "scheduled"
    assert events and events[0].retryable is True
    assert audits
    assert audits[-1].source == "rejected_order_retry"
    assert audits[-1].summary["status"] == "broker_order_retry_pending"
    engine.dispose()


def test_reconcile_kosdaq_sidecar_sell_reject_schedules_delayed_retry() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-kosdaq-sidecar-retry",
        side="sell",
        intent="target_sell",
        symbol="086790",
        quantity=4,
        broker_order_id="kosdaq-sidecar-1",
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "kosdaq-sidecar-1",
                "status": "rejected",
                "broker_status": "매도거부",
                "message": "KOSDAQ sell sidecar temporary orderability window",
                "observed_at": OBSERVED_AT.isoformat(),
                "market_operation_state": {
                    "state": "sidecar",
                    "reason": "KOSDAQ sidecar",
                    "normal_ordering_available": False,
                },
                "payload": {"odno": "kosdaq-sidecar-1"},
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id))
    assert failure is not None
    assert failure.classification == "temporary_market_state"
    assert failure.retry_decision_status == "scheduled"
    assert failure.retry_decision_reason == "temporary_sidecar"
    assert failure.retry_next_at is not None
    assert _utc(failure.retry_next_at) == OBSERVED_AT + timedelta(minutes=5)
    assert job is not None
    assert _utc(job.scheduled_for) == OBSERVED_AT + timedelta(minutes=5)
    engine.dispose()


def test_reconcile_repeat_temp_reject_does_not_slide_pending_retry_job() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-repeat-temp-reject-stable-retry",
        side="sell",
        intent="target_sell",
        symbol="086790",
        quantity=4,
        broker_order_id="repeat-sidecar-1",
    )
    db.add_all([plan, order])
    db.commit()
    second_observed_at = OBSERVED_AT + timedelta(minutes=1)
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "repeat-sidecar-1",
                "status": "rejected",
                "broker_status": "매도거부",
                "message": "KOSDAQ sidecar temporary orderability window",
                "observed_at": OBSERVED_AT.isoformat(),
                "market_operation_state": {
                    "state": "sidecar",
                    "reason": "KOSDAQ sidecar",
                    "normal_ordering_available": False,
                },
                "payload": {"odno": "repeat-sidecar-1"},
            },
            {
                "broker_order_id": "repeat-sidecar-1",
                "status": "rejected",
                "broker_status": "매도거부",
                "message": "KOSDAQ sidecar temporary orderability window",
                "observed_at": second_observed_at.isoformat(),
                "market_operation_state": {
                    "state": "sidecar",
                    "reason": "KOSDAQ sidecar",
                    "normal_ordering_available": False,
                },
                "payload": {"odno": "repeat-sidecar-1"},
            },
        ]
    )

    first = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    job = db.scalar(
        select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id)
    )
    assert first["status"] == "retry_pending"
    assert failure is not None
    assert job is not None
    assert failure.retry_next_at is not None
    assert failure.retry_deadline_at is not None
    assert job.scheduled_for is not None
    initial_retry_next_at = _utc(failure.retry_next_at)
    initial_deadline_at = _utc(failure.retry_deadline_at)
    initial_scheduled_for = _utc(job.scheduled_for)

    second = reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    db.refresh(failure)
    db.refresh(job)
    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "retry_decision")
    ).all()
    assert second["status"] == "retry_pending"
    assert failure.retry_decision_status == "scheduled"
    assert failure.retry_decision_reason == "temporary_sidecar"
    assert failure.retry_next_at is not None
    assert failure.retry_deadline_at is not None
    assert job.scheduled_for is not None
    assert _utc(failure.retry_next_at) == initial_retry_next_at
    assert _utc(failure.retry_deadline_at) == initial_deadline_at
    assert _utc(job.scheduled_for) == initial_scheduled_for
    assert initial_retry_next_at == OBSERVED_AT + timedelta(minutes=5)
    assert second_observed_at + timedelta(minutes=5) > initial_scheduled_for
    assert len(events) == 1
    engine.dispose()


def test_reconcile_insufficient_holdings_sell_reject_does_not_retry() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-insufficient-holdings-no-retry",
        side="sell",
        intent="stop_sell",
        quantity=3,
        broker_order_id="sell-insufficient-1",
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "sell-insufficient-1",
                "status": "rejected",
                "broker_status": "INSUFFICIENT_SELLABLE_QTY",
                "message": "insufficient sellable quantity",
                "observed_at": OBSERVED_AT.isoformat(),
                "payload": {
                    "msg_cd": "INSUFFICIENT_SELLABLE_QTY",
                    "msg1": "insufficient sellable quantity",
                },
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    audit = db.scalar(
        select(ExecutionPlanItemAudit).where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "execution_plan_item_status_transition",
        )
    )
    assert failure is not None
    assert failure.classification == "insufficient_sellable_quantity"
    assert failure.retry_decision_status == "skipped"
    assert failure.retry_decision_reason == "permanent_insufficient_sellable_quantity"
    assert (
        db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id))
        is None
    )
    assert order.status == "rejected"
    assert item.status == "broker_order_rejected"
    assert audit is not None
    assert audit.source == "reconciliation"
    assert audit.summary["status"] == "broker_order_rejected"
    engine.dispose()


def test_reconcile_insufficient_cash_buy_reject_does_not_retry() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-insufficient-cash-no-retry",
        side="buy",
        intent="next_open_buy",
        quantity=5,
        broker_order_id="buy-insufficient-1",
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "buy-insufficient-1",
                "status": "rejected",
                "broker_status": "INSUFFICIENT_CASH",
                "message": "insufficient cash available for order",
                "observed_at": OBSERVED_AT.isoformat(),
                "payload": {
                    "msg_cd": "INSUFFICIENT_CASH",
                    "msg1": "insufficient cash available for order",
                },
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    assert failure is not None
    assert failure.classification == "insufficient_funds"
    assert failure.retry_decision_status == "skipped"
    assert failure.retry_decision_reason == "permanent_insufficient_funds"
    assert (
        db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id))
        is None
    )
    engine.dispose()


def test_reconcile_expired_buy_signal_temp_reject_does_not_retry() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-expired-buy-signal-no-retry",
        side="buy",
        intent="next_open_buy",
        quantity=5,
        broker_order_id="buy-expired-temp-1",
        risk_context={"signal_expires_at": (OBSERVED_AT - timedelta(minutes=1)).isoformat()},
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "buy-expired-temp-1",
                "status": "rejected",
                "broker_status": "MARKET_CLOSED",
                "message": "temporary market closed orderability window",
                "observed_at": OBSERVED_AT.isoformat(),
                "market_state": {
                    "normal_ordering_available": False,
                    "reopen_at": (OBSERVED_AT + timedelta(minutes=5)).isoformat(),
                },
                "payload": {"msg_cd": "MARKET_CLOSED"},
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    assert failure is not None
    assert failure.classification == "temporary_market_state"
    assert failure.retry_decision_status == "skipped"
    assert failure.retry_decision_reason == "expired_buy_signal"
    assert (
        db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id))
        is None
    )
    engine.dispose()


def test_retry_job_dispatch_and_reconciliation_rerun_do_not_duplicate_retry_order() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        plan_id="plan-retry-rerun-idempotent",
        side="sell",
        intent="stop_sell",
        symbol="024110",
        quantity=20,
        broker_order_id="retry-rerun-source-1",
    )
    db.add_all([plan, order])
    db.commit()
    retry_at = OBSERVED_AT + timedelta(minutes=1)
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "retry-rerun-source-1",
                "status": "rejected",
                "broker_status": "매도거부",
                "message": "temporary trading halt",
                "observed_at": OBSERVED_AT.isoformat(),
                "market_state": {
                    "state": "halt",
                    "reason": "trading halt",
                    "normal_ordering_available": False,
                    "reopen_at": retry_at.isoformat(),
                },
                "payload": {"odno": "retry-rerun-source-1"},
            }
        ],
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="024110",
                    quantity=20,
                    average_price=Decimal("71000"),
                    sellable_quantity=20,
                )
            ],
        ),
        order_placement=BrokerOrderPlacement(
            broker_order_id="retry-child-1",
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="0",
            submitted_at=retry_at,
            observed_at=retry_at,
            request_metadata={"provider": "kis"},
            response_metadata={"provider": "kis"},
            request_payload={},
            response_payload={"rt_cd": "0"},
            message="accepted",
        ),
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    resume_runtime(
        settings=settings,
        db=db,
        command=ControlCommand(actor="ops", source="test", reason="enable retry dispatch"),
    )
    first_dispatch = dispatch_due_execution_jobs(
        settings=settings,
        db=db,
        adapter=broker,
        now=retry_at + timedelta(seconds=1),
    )
    assert first_dispatch.payload["completed_job_count"] == 1, first_dispatch.payload
    assert len(broker.order_calls) == 1

    db.refresh(order)
    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
    second_dispatch = dispatch_due_execution_jobs(
        settings=settings,
        db=db,
        adapter=broker,
        now=retry_at + timedelta(seconds=2),
    )

    jobs = db.scalars(
        select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id)
    ).all()
    orders = db.scalars(
        select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == item.id)
    ).all()
    retry_orders = [candidate for candidate in orders if candidate.id != order.id]
    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    assert second_dispatch.payload["due_job_count"] == 0
    assert len(broker.order_calls) == 1
    assert len(jobs) == 1
    assert len(retry_orders) == 1
    assert retry_orders[0].idempotency_key.startswith(f"retry:{order.id}:")
    assert failure is not None
    assert failure.retry_decision_status == "submitted"
    assert item.status == "broker_order_submitted"
    engine.dispose()
