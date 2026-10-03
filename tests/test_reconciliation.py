from __future__ import annotations

import hmac
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError
from sqlalchemy import event, func, select

from app.api.schemas.execution import PlaceOrderRequest, StaleBrokerOrderReconciliationRequest
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
from app.services import monitoring as monitoring_service
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.execution_account_route import bind_kis_account_route
from app.services.execution_api import _serialize_execution_fill
from app.services.fill_idempotency import (
    duplicate_fill_group_candidate_key,
    execution_fill_dedupe_key,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
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
from app.services.watch_exec_agent_sizing import _reconcile_stale_sizing_orders


OBSERVED_AT = datetime(2026, 5, 13, 9, 5, tzinfo=UTC)


def test_execution_fill_serialization_recursively_redacts_account_references() -> None:
    order = _order(broker_order_id="redacted-fill-order")
    fill = ExecutionFill(
        broker_order=order,
        broker_fill_id="redacted-fill",
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("71000"),
        filled_at=OBSERVED_AT,
        payload={
            "nested": {
                "CANO": "12345678",
                "ACNT_PRDT_CD": "01",
                "marker": "preserved",
            }
        },
    )

    payload = _serialize_execution_fill(fill)

    persisted_text = str(payload["payload"]).lower()
    assert "12345678" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert payload["payload"]["nested"]["marker"] == "preserved"


def test_existing_fill_economics_rewrite_redacts_legacy_account_identifiers() -> None:
    order = _order(broker_order_id="redacted-fill-revision-order")
    fill = ExecutionFill(
        broker_order=order,
        broker_fill_id="redacted-fill-revision",
        symbol="005930",
        side="buy",
        quantity=1,
        price=Decimal("71000"),
        filled_at=OBSERVED_AT,
        payload={
            "nested": {
                "account_id": "raw-broker-account",
                "ACNT_PRDT_CD": "01",
                "marker": "preserved",
            }
        },
    )

    reconciliation_service._enrich_existing_fill_economics(
        existing=fill,
        incoming={
            "settlement_currency": "KRW",
            "transaction_cost": "10",
            "transaction_cost_coverage": "complete",
            "transaction_cost_source": "broker_payload:test",
        },
    )

    persisted_text = str(fill.payload).lower()
    assert "raw-broker-account" not in persisted_text
    assert "account_id" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert fill.payload["nested"] == {"marker": "preserved"}
    assert fill.payload["economics_revision"]["settlement_currency"] == "KRW"


def test_reconciliation_metadata_cannot_replace_canonical_execution_scope() -> None:
    canonical_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": "a" * 24,
        "venue": "KIS-US",
        "asset_class": "us_equity",
        "exchange": "NASD",
    }

    merged = reconciliation_service._merge_reconcile_request_metadata(
        existing={
            "source": "post:/orders",
            "exchange": "NASD",
            "execution_scope": canonical_scope,
        },
        incoming={
            "broker_marker": "preserved",
            "exchange": "NYSE",
            "execution_scope": {
                **canonical_scope,
                "account_scope": "b" * 24,
                "exchange": "NYSE",
            },
        },
    )

    assert merged["broker_marker"] == "preserved"
    assert merged["exchange"] == "NASD"
    assert merged["execution_scope"] == canonical_scope


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
        self.reconcile_expected_symbols: list[str | None] = []
        self.order_calls: list[BrokerOrderPlacementRequest] = []
        self.account_snapshot_calls: list[tuple[list[str] | None, str]] = []
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

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        assert access_token
        self.account_snapshot_calls.append((symbols, exchange))
        del reference_prices
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
        expected_symbol: str | None = None,
    ) -> dict[str, object]:
        assert access_token
        self.reconcile_calls.append((broker_order_id, exchange, submitted_at))
        self.reconcile_expected_symbols.append(expected_symbol)
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
    settings: Settings | None = None,
    status: str = "submitted",
    broker_order_id: str = "0033196600",
    side: str = "buy",
    request_metadata: dict[str, object] | None = None,
) -> BrokerOrder:
    effective_settings = settings or Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    metadata = dict(request_metadata or {})
    route = metadata.get("route")
    route = route if isinstance(route, dict) else {}
    exchange = str(metadata.get("exchange") or route.get("exchange") or "KRX").upper()
    venue = exchange
    asset_class = "stock"
    metadata["execution_scope"] = execution_scope_payload(
        identity=build_position_identity(
            settings=effective_settings,
            provider="kis",
            exchange=exchange,
        ),
        venue=venue,
        asset_class=asset_class,
    )
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"test:{broker_order_id}",
        broker_order_id=broker_order_id,
        symbol="005930",
        side=side,
        status=status,
        quantity=2,
        venue=venue,
        asset_class=asset_class,
        submitted_at=OBSERVED_AT,
        request_metadata=metadata,
    )


def _plan_backed_order(
    *,
    settings: Settings | None = None,
    plan_id: str,
    side: str,
    intent: str,
    symbol: str = "005930",
    quantity: int = 2,
    broker_order_id: str = "reject-1",
    status: str = "submitted",
    risk_context: dict[str, object] | None = None,
) -> tuple[ExecutionPlan, ExecutionPlanItem, BrokerOrder]:
    effective_settings = settings or Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    item_risk_context = dict(risk_context or {})
    exchange = str(item_risk_context.get("exchange") or "KRX").upper()
    asset_class = str(item_risk_context.get("asset_class") or "stock").lower()
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
        risk_context=item_risk_context,
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
        venue=exchange,
        asset_class=asset_class,
        limit_price=Decimal("71000"),
        submitted_at=OBSERVED_AT,
        request_metadata={
            "source": "test",
            "time_in_force": "day",
            "route": {"exchange": exchange},
            "execution_scope": execution_scope_payload(
                identity=build_position_identity(
                    settings=effective_settings,
                    provider="kis",
                    exchange=exchange,
                ),
                venue=exchange,
                asset_class=asset_class,
            ),
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
    order = _order(settings=settings)
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
    assert broker.reconcile_expected_symbols == ["005930", "005930"]
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


@pytest.mark.parametrize(
    "identity_patch",
    [
        {"broker_order_id": "another-order"},
        {"provider": "alpaca"},
        {"symbol": "000660"},
        {"symbol": 5930},
    ],
)
def test_reconcile_rejects_mismatched_top_level_broker_identity_before_mutation(
    identity_patch: dict[str, object],
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    order = _order(settings=settings, broker_order_id="identity-guard-order")
    db.add(order)
    db.commit()
    payload: dict[str, object] = {
        "broker_order_id": "identity-guard-order",
        "provider": "kis",
        "symbol": "005930",
        "status": "filled",
        "filled_quantity": 2,
        "remaining_quantity": 0,
        "average_fill_price": "71000",
        "fills": [
            {
                "broker_fill_id": "identity-guard-fill",
                "symbol": "005930",
                "side": "buy",
                "quantity": 2,
                "price": "71000",
                "filled_at": OBSERVED_AT.isoformat(),
            }
        ],
        "observed_at": OBSERVED_AT.isoformat(),
    }
    payload.update(identity_patch)
    broker = ReconciliationStubBroker(reconcile_payloads=[payload])

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["status_code"] == 502
    assert result["status"] == "submitted"
    assert result["status_changed"] is False
    assert result["fills_added"] == 0
    assert db.scalars(select(ExecutionFill)).all() == []
    assert db.scalar(
        select(func.count(BrokerOrderEvent.id)).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_result",
        )
    ) == 0
    assert broker.reconcile_expected_symbols == ["005930"]
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


def test_us_position_sync_preserves_durable_monitoring_manual_review() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="manual-review-sync-test",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    raw_payload = payload_with_position_identity({}, identity=identity)
    raw_payload["_position_sync"] = {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
    }
    db.add(
        ExecutionPosition(
            mode=settings.runtime_mode,
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="OLDCO",
            quantity=7,
            average_price=Decimal("12.50"),
            updated_from="reconciliation",
            raw_payload=raw_payload,
        )
    )
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            observed_at=OBSERVED_AT,
            source="stub:nasd",
            holdings=[
                BrokerAccountHolding(
                    symbol="OLDCO",
                    quantity=7,
                    average_price=Decimal("12.25"),
                    sellable_quantity=7,
                    payload={"ovrs_excg_cd": "NASD"},
                )
            ],
            payload={"currency": "USD", "exchange": "NASD"},
        )
    )

    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
        authenticated_access_token="access-token",
    )

    position = db.scalar(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "OLDCO")
    )
    assert position is not None
    assert position.average_price == Decimal("12.2500")
    assert position.raw_payload["currency"] == "USD"
    assert position.raw_payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
    }
    engine.dispose()


def test_us_position_sync_preserves_unpriced_latch_when_holding_is_absent() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="missing-holding-latch-test",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    raw_payload = payload_with_position_identity({}, identity=identity)
    raw_payload["_position_sync"] = {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
    }
    position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="WWBLCM",
        quantity=75,
        average_price=Decimal("0.9164"),
        updated_from="reconciliation",
        raw_payload=raw_payload,
    )
    db.add(position)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:us_equity_account_snapshot",
            holdings=[],
            holdings_complete=True,
            open_orders_complete=True,
            payload={"currency": "USD", "exchange": "NASD"},
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"WWBLCM"},
        exchange="NASD",
        authenticated_access_token="access-token",
    )

    db.refresh(position)
    assert result["zeroed"] == 1
    assert result["monitoring_recovered_count"] == 0
    assert position.quantity == 0
    assert position.raw_payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
    }
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    engine.dispose()


def test_us_position_sync_recovers_unpriced_holding_from_fresh_complete_broker_evidence() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="manual-review-recovery-test",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    raw_payload = payload_with_position_identity({}, identity=identity)
    raw_payload["_position_sync"] = {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
    }
    position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="APTX",
        quantity=377,
        average_price=Decimal("0.0963"),
        updated_from="reconciliation",
        raw_payload=raw_payload,
    )
    db.add(position)
    db.commit()
    observed_at = datetime.now(UTC) - timedelta(seconds=5)
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            observed_at=observed_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holdings=[
                BrokerAccountHolding(
                    symbol="APTX",
                    quantity=377,
                    average_price=Decimal("0.0963"),
                    sellable_quantity=377,
                    payload={"ovrs_excg_cd": "NASD", "now_pric2": "0.061"},
                )
            ],
            payload={"currency": "USD", "exchange": "NASD"},
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
        authenticated_access_token="access-token",
    )

    db.refresh(position)
    event = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type
            == "position_monitoring_manual_review",
            RuntimeRemediationEvent.subject_id == position.id,
            RuntimeRemediationEvent.outcome == "monitoring_reenabled",
        )
    )
    assert result["monitoring_recovered_count"] == 1
    assert position.raw_payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": True,
    }
    assert event is not None
    assert event.actor == "system"
    assert event.reason == "fresh_complete_kis_holding_is_priceable_and_sellable"
    assert event.evidence_summary["snapshot_source"] == "kis:us_equity_account_snapshot"
    assert event.evidence_summary["current_price"] == "0.061"
    assert event.evidence_summary["sellable_quantity"] == 377
    assert event.evidence_summary["prior_manual_review_reason"] == (
        "unpriced_overseas_holding"
    )
    assert event.evidence_summary["manual_review_recorded_at"] == (
        "2026-09-15T03:55:00+00:00"
    )
    assert event.evidence_summary["orders_submitted"] == 0
    assert "APTX" in monitoring_service.list_autonomous_monitoring_symbols(
        settings=settings,
        db=db,
        effective_at=observed_at,
    )
    relatched_at = observed_at + timedelta(seconds=1)
    monitoring_service._mark_position_monitoring_manual_review(
        position=position,
        reason="unpriced_overseas_holding",
        triggered_at=relatched_at,
    )
    db.commit()
    assert position.raw_payload["_position_sync"]["monitoring_eligible"] is False
    assert position.raw_payload["_position_sync"]["manual_review_recorded_at"] == (
        relatched_at.isoformat()
    )
    assert "APTX" not in monitoring_service.list_autonomous_monitoring_symbols(
        settings=settings,
        db=db,
        effective_at=relatched_at,
    )
    broker.account_snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        observed_at=relatched_at + timedelta(seconds=1),
        source="kis:us_equity_account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holdings=broker.account_snapshot.holdings,
        payload={"currency": "USD", "exchange": "NASD"},
    )
    second_recovery = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
        authenticated_access_token="access-token",
    )
    recovery_events = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type
            == "position_monitoring_manual_review",
            RuntimeRemediationEvent.subject_id == position.id,
            RuntimeRemediationEvent.outcome == "monitoring_reenabled",
        )
    ).all()
    db.refresh(position)
    assert second_recovery["monitoring_recovered_count"] == 1
    assert position.raw_payload["_position_sync"]["monitoring_eligible"] is True
    assert len(recovery_events) == 2
    engine.dispose()


def test_unpriced_holding_recovery_preserves_independent_trade_quarantine() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    observed_at = datetime.now(UTC)
    snapshot = BrokerAccountSnapshot(
        observed_at=observed_at,
        source="kis:us_equity_account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        payload={"exchange": "NASD"},
    )
    holding = BrokerAccountHolding(
        symbol="APTX",
        quantity=377,
        average_price=Decimal("0.0963"),
        sellable_quantity=377,
        payload={"ovrs_excg_cd": "NASD", "now_pric2": "0.061"},
    )

    evidence = reconciliation_service._unpriced_kis_us_holding_recovery_evidence(
        settings=settings,
        snapshot=snapshot,
        holding=holding,
        exchange="NASD",
        prior_payload={
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": False,
                "manual_review_required": True,
                "manual_review_reason": "unpriced_overseas_holding",
                "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
                "trade_eligible": False,
                "route_disposition": "manual_review",
            }
        },
        evaluated_at=observed_at,
    )

    assert evidence is None


def test_us_position_sync_preserves_unpriced_latch_for_zero_current_price() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="zero-price-latch-test",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    recorded_at = "2026-09-15T03:55:00+00:00"
    raw_payload = payload_with_position_identity({}, identity=identity)
    raw_payload["_position_sync"] = {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unpriced_overseas_holding",
        "manual_review_recorded_at": recorded_at,
    }
    position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="WWBLCM",
        quantity=75,
        average_price=Decimal("0.9164"),
        updated_from="reconciliation",
        raw_payload=raw_payload,
    )
    db.add(position)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC) - timedelta(seconds=1),
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holdings=[
                BrokerAccountHolding(
                    symbol="WWBLCM",
                    quantity=75,
                    average_price=Decimal("0.9164"),
                    sellable_quantity=75,
                    payload={"ovrs_excg_cd": "NASD", "now_pric2": "0"},
                )
            ],
            payload={"currency": "USD", "exchange": "NASD"},
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"WWBLCM"},
        exchange="NASD",
        authenticated_access_token="access-token",
    )

    db.refresh(position)
    sync_payload = position.raw_payload["_position_sync"]
    assert result["monitoring_recovered_count"] == 0
    assert sync_payload["monitoring_eligible"] is False
    assert sync_payload["manual_review_required"] is True
    assert sync_payload["manual_review_reason"] == "unpriced_overseas_holding"
    assert sync_payload["manual_review_recorded_at"] == recorded_at
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    engine.dispose()


def test_unpriced_holding_recovery_rejects_missing_average_price() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    observed_at = datetime.now(UTC)

    evidence = reconciliation_service._unpriced_kis_us_holding_recovery_evidence(
        settings=settings,
        snapshot=BrokerAccountSnapshot(
            observed_at=observed_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={"exchange": "NASD"},
        ),
        holding=BrokerAccountHolding(
            symbol="APTX",
            quantity=377,
            average_price=None,
            sellable_quantity=377,
            payload={"ovrs_excg_cd": "NASD", "now_pric2": "0.061"},
        ),
        exchange="NASD",
        prior_payload={
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": False,
                "manual_review_required": True,
                "manual_review_reason": "unpriced_overseas_holding",
                "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
            }
        },
        evaluated_at=observed_at,
    )

    assert evidence is None


def test_unpriced_holding_recovery_rejects_conflicting_current_price_fields() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    observed_at = datetime.now(UTC)

    evidence = reconciliation_service._unpriced_kis_us_holding_recovery_evidence(
        settings=settings,
        snapshot=BrokerAccountSnapshot(
            observed_at=observed_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={"exchange": "NASD"},
        ),
        holding=BrokerAccountHolding(
            symbol="APTX",
            quantity=377,
            average_price=Decimal("0.0963"),
            sellable_quantity=377,
            payload={
                "ovrs_excg_cd": "NASD",
                "now_pric2": "0.061",
                "current_price": "0",
            },
        ),
        exchange="NASD",
        prior_payload={
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": False,
                "manual_review_required": True,
                "manual_review_reason": "unpriced_overseas_holding",
                "manual_review_recorded_at": "2026-09-15T03:55:00+00:00",
            }
        },
        evaluated_at=observed_at,
    )

    assert evidence is None


def test_unpriced_holding_recovery_requires_snapshot_newer_than_current_latch() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    observed_at = datetime.now(UTC)

    evidence = reconciliation_service._unpriced_kis_us_holding_recovery_evidence(
        settings=settings,
        snapshot=BrokerAccountSnapshot(
            observed_at=observed_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={"exchange": "NASD"},
        ),
        holding=BrokerAccountHolding(
            symbol="APTX",
            quantity=377,
            average_price=Decimal("0.0963"),
            sellable_quantity=377,
            payload={"ovrs_excg_cd": "NASD", "now_pric2": "0.061"},
        ),
        exchange="NASD",
        prior_payload={
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": False,
                "manual_review_required": True,
                "manual_review_reason": "unpriced_overseas_holding",
                "manual_review_recorded_at": (
                    observed_at + timedelta(seconds=1)
                ).isoformat(),
            }
        },
        evaluated_at=observed_at + timedelta(seconds=2),
    )

    assert evidence is None


def test_us_position_sync_does_not_preserve_non_durable_monitoring_disable() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="non-durable-review-sync-test",
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    payload = reconciliation_service._position_snapshot_payload(
        snapshot_payload={"currency": "USD"},
        holding=BrokerAccountHolding(
            symbol="AAPL",
            quantity=1,
            average_price=Decimal("200"),
            sellable_quantity=1,
            payload={"ovrs_excg_cd": "NASD"},
        ),
        exchange="NASD",
        identity=identity,
        prior_payload={
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": False,
                "manual_review_reason": "transient_quote_failure",
            }
        },
    )

    assert payload is not None
    assert payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "NASD",
        "monitoring_eligible": True,
    }


def test_us_holding_discovery_attributes_only_reported_exchange_route() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
        execution_position_account_scope="kis-primary-live",
    )
    engine, db = _session(settings)

    class AggregateLiveSnapshotBroker(ReconciliationStubBroker):
        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            del symbols
            holding = BrokerAccountHolding(
                symbol="IBM",
                quantity=3,
                average_price=Decimal("250"),
                sellable_quantity=3,
                payload={"ovrs_excg_cd": "NYSE"},
            )
            return BrokerAccountSnapshot(
                available_cash=Decimal("1000"),
                observed_at=OBSERVED_AT,
                source=f"stub:{exchange}",
                holdings=[holding] if exchange in {"NASD", "NYSE"} else [],
                payload={"currency": "USD", "exchange": exchange},
            )

    broker = AggregateLiveSnapshotBroker()
    nasdaq_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
    )

    assert nasdaq_result["created"] == 0
    assert db.scalars(select(ExecutionPosition)).all() == []

    nyse_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NYSE",
        discover_holdings_only=True,
    )

    positions = db.scalars(select(ExecutionPosition)).all()
    assert nyse_result["created"] == 1
    assert len(positions) == 1
    assert positions[0].symbol == "IBM"
    assert positions[0].exchange == "NYSE"
    assert positions[0].quantity == 3
    engine.dispose()


def test_us_position_sync_rejects_holding_without_exchange_proof_before_mutation() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            observed_at=OBSERVED_AT,
            source="stub:missing-exchange",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=1,
                    average_price=Decimal("200"),
                    sellable_quantity=1,
                    payload={},
                )
            ],
        )
    )

    with pytest.raises(BrokerAdapterError, match="lacks broker exchange proof"):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=broker,
            symbols=None,
            exchange="NASD",
            discover_holdings_only=True,
        )

    assert db.scalars(select(ExecutionPosition)).all() == []
    engine.dispose()


def test_us_position_sync_rejects_conflicting_duplicate_holdings_before_mutation() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    original = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="AAPL",
        quantity=5,
        average_price=Decimal("500"),
        updated_from="test_seed",
        raw_payload=payload_with_position_identity(
            {"snapshot": "trusted-before-conflict"},
            identity=identity,
        ),
    )
    db.add(original)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            observed_at=OBSERVED_AT,
            source="stub:conflicting-duplicates",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=1,
                    average_price=Decimal("100"),
                    sellable_quantity=1,
                    unrealized_pnl=Decimal("1"),
                    unrealized_pnl_pct=Decimal("1"),
                    payload={
                        "ovrs_excg_cd": "NASD",
                        "ovrs_stck_evlu_amt": "101",
                    },
                ),
                BrokerAccountHolding(
                    symbol="aapl",
                    quantity=9,
                    average_price=Decimal("900"),
                    sellable_quantity=9,
                    unrealized_pnl=Decimal("9"),
                    unrealized_pnl_pct=Decimal("1"),
                    payload={
                        "ovrs_excg_cd": "nasdaq",
                        "ovrs_stck_evlu_amt": "8109",
                    },
                ),
            ],
        )
    )

    with pytest.raises(BrokerAdapterError, match="conflicting duplicate broker holding"):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=broker,
            symbols=None,
            exchange="NASD",
            discover_holdings_only=True,
        )

    db.expire_all()
    persisted = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "AAPL",
        )
    )
    assert persisted is not None
    assert persisted.quantity == 5
    assert persisted.average_price == Decimal("500.0000")
    assert persisted.updated_from == "test_seed"
    assert persisted.raw_payload["snapshot"] == "trusted-before-conflict"
    engine.dispose()


def test_us_position_sync_dedupes_equivalent_normalized_holdings() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            observed_at=OBSERVED_AT,
            source="stub:equivalent-duplicates",
            holdings=[
                BrokerAccountHolding(
                    symbol=" AAPL ",
                    quantity=1,
                    average_price=Decimal("100.0"),
                    sellable_quantity=1,
                    unrealized_pnl=Decimal("1.0"),
                    unrealized_pnl_pct=Decimal("1.00"),
                    payload={
                        "ovrs_excg_cd": "NASD",
                        "ovrs_stck_evlu_amt": "101.0",
                    },
                ),
                BrokerAccountHolding(
                    symbol="aapl",
                    quantity=1,
                    average_price=Decimal("100.00"),
                    sellable_quantity=1,
                    unrealized_pnl=Decimal("1.00"),
                    unrealized_pnl_pct=Decimal("1.0"),
                    payload={
                        "ovrs_excg_cd": "nasdaq",
                        "ovrs_stck_evlu_amt": "101.00",
                    },
                ),
            ],
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        exchange="NASD",
        discover_holdings_only=True,
    )

    positions = db.scalars(select(ExecutionPosition)).all()
    assert result["tracked_symbol_count"] == 1
    assert len(positions) == 1
    assert positions[0].symbol == "AAPL"
    assert positions[0].quantity == 1
    assert positions[0].average_price == Decimal("100.0000")
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
            raw_payload=payload_with_position_identity(
                None,
                identity=build_position_identity(
                    settings=settings,
                    provider="kis",
                    exchange="KRX",
                ),
            ),
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


def test_reconciliation_redacts_kis_account_fields_from_persisted_payloads() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    order = _order(settings=settings)
    canonical_scope = order.request_metadata["execution_scope"]
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
                "request_metadata": {
                    "route": {"CANO": "12345678", "exchange": "KRX"},
                    "request_marker": "preserved",
                },
                "response_metadata": {
                    "nested": {"ACNT_PRDT_CD": "01", "status": "ok"},
                    "response_marker": "preserved",
                },
                "request_payload": {
                    "CANO": "12345678",
                    "nested": [{"ACNT_PRDT_CD": "01", "PDNO": "005930"}],
                },
                "payload": {
                    "account_reference": "KIS:12345678-01",
                    "output": {
                        "cano": "12345678",
                        "acnt_prdt_cd": "01",
                        "odno": "0033196600",
                    },
                },
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    db.refresh(order)
    event = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_result",
        )
    )
    assert event is not None
    persisted_payloads = {
        "order_request_metadata": order.request_metadata,
        "order_response_metadata": order.response_metadata,
        "order_raw_broker_payload": order.raw_broker_payload,
        "event_request_payload": event.request_payload,
        "event_response_payload": event.response_payload,
        "event_payload": event.payload,
    }
    persisted_text = str(persisted_payloads).lower()
    assert "12345678" not in persisted_text
    assert "account_reference" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert order.request_metadata == {
        "route": {"exchange": "KRX"},
        "request_marker": "preserved",
        "execution_scope": canonical_scope,
    }
    assert order.response_metadata == {
        "nested": {"status": "ok"},
        "response_marker": "preserved",
    }
    assert order.raw_broker_payload == {"output": {"odno": "0033196600"}}
    assert event.request_payload == {"nested": [{"PDNO": "005930"}]}
    assert event.payload["normalized_payload"] == {
        "output": {"odno": "0033196600"}
    }
    engine.dispose()


def test_reconciliation_error_redacts_kis_account_fields_from_persisted_event() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    order = _order(settings=settings, broker_order_id="redaction-error-1")
    db.add(order)
    db.commit()

    class FailingReconciliationBroker(ReconciliationStubBroker):
        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
            submitted_at: datetime | None = None,
        ) -> dict[str, object]:
            del access_token, broker_order_id, exchange, submitted_at
            raise BrokerAdapterError(
                "broker rejected reconciliation",
                status_code=502,
                classification=BrokerErrorClassification.BROKER_REJECTED,
                code="RECONCILE_REJECTED",
                request_metadata={
                    "route": {"CANO": "12345678", "exchange": "KRX"},
                    "request_marker": "preserved",
                },
                response_metadata={
                    "nested": {"ACNT_PRDT_CD": "01", "status": "rejected"},
                    "response_marker": "preserved",
                },
                request_payload={
                    "CANO": "12345678",
                    "nested": {"PDNO": "005930"},
                },
                response_payload={
                    "account_reference": "KIS:12345678-01",
                    "output": {"acnt_prdt_cd": "01", "msg_cd": "REJECTED"},
                },
                observed_at=OBSERVED_AT,
            )

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=FailingReconciliationBroker(),
        order=order,
    )

    event = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_failed",
        )
    )
    assert result["status_code"] == 502
    assert event is not None
    persisted_payloads = {
        "order_request_metadata": order.request_metadata,
        "order_response_metadata": order.response_metadata,
        "order_raw_broker_payload": order.raw_broker_payload,
        "event_request_payload": event.request_payload,
        "event_response_payload": event.response_payload,
        "event_payload": event.payload,
    }
    persisted_text = str(persisted_payloads).lower()
    assert "12345678" not in persisted_text
    assert "account_reference" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert event.request_payload == {"nested": {"PDNO": "005930"}}
    assert event.response_payload == {"output": {"msg_cd": "REJECTED"}}
    assert event.payload["request_metadata"]["request_marker"] == "preserved"
    assert event.payload["response_metadata"]["response_marker"].startswith(
        "broker_error_details_redacted["
    )
    engine.dispose()


def test_absent_order_repair_redacts_kis_account_fields_from_plan_audit() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
        stale_broker_order_reconciliation_threshold_seconds=60,
    )
    engine, db = _session(settings)
    plan, item, order = _plan_backed_order(
        settings=settings,
        plan_id="plan-absent-repair-redaction",
        side="buy",
        intent="next_open_buy",
        broker_order_id="absent-redaction-1",
    )
    order.submitted_at = OBSERVED_AT - timedelta(days=1)
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "absent-redaction-1",
                "status": "unknown",
                "broker_status": "NOT_FOUND",
                "message": "not found in broker order query",
                "observed_at": OBSERVED_AT.isoformat(),
                "payload": {
                    "request": {"CANO": "12345678", "ACNT_PRDT_CD": "01"},
                    "result_marker": "preserved",
                },
            }
        ]
    )

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    audit = db.scalar(
        select(ExecutionPlanItemAudit).where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "execution_plan_item_status_transition",
        )
    )
    assert result["status"] == "stale_terminal_unknown"
    assert audit is not None
    persisted_text = str(audit.payload).lower()
    assert "12345678" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert audit.payload["broker_result"]["payload"]["result_marker"] == "preserved"
    engine.dispose()


def test_rejected_order_reconciliation_redacts_downstream_failure_context() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    plan, _item, order = _plan_backed_order(
        settings=settings,
        plan_id="plan-reject-redaction",
        side="buy",
        intent="next_open_buy",
        broker_order_id="reject-redaction-1",
    )
    db.add_all([plan, order])
    db.commit()
    broker = ReconciliationStubBroker(
        reconcile_payloads=[
            {
                "broker_order_id": "reject-redaction-1",
                "status": "rejected",
                "broker_status": "INSUFFICIENT_CASH",
                "message": "insufficient cash available for order",
                "observed_at": OBSERVED_AT.isoformat(),
                "request_payload": {
                    "CANO": "12345678",
                    "ACNT_PRDT_CD": "01",
                    "PDNO": "005930",
                },
                "payload": {
                    "account_reference": "KIS:12345678-01",
                    "output": {
                        "cano": "12345678",
                        "acnt_prdt_cd": "01",
                        "msg_cd": "INSUFFICIENT_CASH",
                    },
                    "result_marker": "preserved",
                },
            }
        ]
    )

    reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

    failure = db.scalar(
        select(ExecutionFailure).where(ExecutionFailure.broker_order_pk == order.id)
    )
    events = db.scalars(
        select(BrokerOrderEvent).where(BrokerOrderEvent.broker_order_pk == order.id)
    ).all()
    assert failure is not None
    persisted_payloads = {
        "failure_raw_context": failure.raw_context,
        "failure_retry_audit_context": failure.retry_audit_context,
        "events": [
            {
                "request_payload": event.request_payload,
                "response_payload": event.response_payload,
                "payload": event.payload,
            }
            for event in events
        ],
    }
    persisted_text = str(persisted_payloads).lower()
    assert "12345678" not in persisted_text
    assert "account_reference" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert failure.raw_context["reconcile_payload"]["payload"]["result_marker"] == (
        "preserved"
    )
    engine.dispose()


def test_position_reconciliation_redacts_kis_account_fields_from_raw_snapshot() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("71000"),
                    sellable_quantity=2,
                )
            ],
            payload={
                "balance": {
                    "request": {"CANO": "12345678", "ACNT_PRDT_CD": "01"},
                    "output": [{"PDNO": "005930", "hldg_qty": "2"}],
                },
                "account_reference": "KIS:12345678-01",
                "snapshot_marker": "preserved",
            },
        )
    )

    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"005930"},
    )

    position = db.scalar(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "005930")
    )
    assert position is not None
    persisted_text = str(position.raw_payload).lower()
    assert "12345678" not in persisted_text
    assert "account_reference" not in persisted_text
    assert "cano" not in persisted_text
    assert "acnt_prdt_cd" not in persisted_text
    assert position.raw_payload["snapshot_marker"] == "preserved"
    assert position.raw_payload["balance"]["output"] == [
        {"PDNO": "005930", "hldg_qty": "2"}
    ]
    assert position.raw_payload["_position_identity"]["provider"] == "kis"
    engine.dispose()


def test_position_snapshot_from_other_provider_uses_isolated_symbol_row() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_trading_enabled=True,
        alpaca_us_equity_monitoring_enabled=True,
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
        alpaca_api_key="paper-alpaca-key",
        alpaca_api_secret="paper-alpaca-secret",
    )
    engine, db = _session(settings)
    kis_broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:kis",
            holdings=[
                BrokerAccountHolding(
                    symbol="SHARED",
                    quantity=2,
                    average_price=Decimal("100"),
                    sellable_quantity=2,
                )
            ],
        )
    )
    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=kis_broker,
        symbols={"SHARED"},
    )

    class AlpacaSnapshotBroker(ReconciliationStubBroker):
        provider = "alpaca"

        def describe(self) -> BrokerConfig:
            return BrokerConfig(
                provider="alpaca",
                mode="paper",
                configured=True,
                api_base_url="https://stub.alpaca.local",
            )

    alpaca_broker = AlpacaSnapshotBroker(
        account_snapshot=BrokerAccountSnapshot(
            broker_account_id="alpaca-shared-account",
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            source="stub:alpaca",
            holdings=[
                BrokerAccountHolding(
                    symbol="SHARED",
                    quantity=7,
                    average_price=Decimal("200"),
                    sellable_quantity=7,
                )
            ],
        )
    )

    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=alpaca_broker,
        symbols={"SHARED"},
    )

    positions = db.scalars(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "SHARED")
    ).all()
    assert len(positions) == 2
    positions_by_provider = {position.provider: position for position in positions}
    assert positions_by_provider["kis"].quantity == 2
    assert positions_by_provider["kis"].average_price == Decimal("100.0000")
    assert positions_by_provider["alpaca"].quantity == 7
    assert positions_by_provider["alpaca"].average_price == Decimal("200.0000")
    engine.dispose()


def test_alpaca_position_identity_survives_rotation_and_isolates_other_account() -> None:
    original_settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        alpaca_api_key="rotating-key-v1",
        alpaca_api_secret="rotating-secret-v1",
        account_drawdown_recovery_scope_secret="stable-position-scope-secret",
    )
    rotated_settings = original_settings.model_copy(
        update={
            "alpaca_api_key": "rotating-key-v2",
            "alpaca_api_secret": "rotating-secret-v2",
        }
    )
    engine, db = _session(original_settings)

    class AlpacaSnapshotBroker(ReconciliationStubBroker):
        provider = "alpaca"

        def describe(self) -> BrokerConfig:
            return BrokerConfig(
                provider="alpaca",
                mode="paper",
                configured=True,
                api_base_url="https://stub.alpaca.local",
            )

    def snapshot(*, account_id: str, quantity: int, observed_at: datetime) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            broker_account_id=account_id,
            available_cash=Decimal("1000"),
            observed_at=observed_at,
            source="stub:alpaca",
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=quantity,
                    average_price=Decimal("60000"),
                    sellable_quantity=quantity,
                )
            ],
        )

    first_broker = AlpacaSnapshotBroker(
        account_snapshot=snapshot(
            account_id="alpaca-account-stable-id",
            quantity=2,
            observed_at=OBSERVED_AT,
        )
    )
    sync_execution_positions_from_broker_snapshot(
        settings=original_settings,
        db=db,
        adapter=first_broker,
        symbols={"BTC/USD"},
        exchange="ALPACA",
    )
    position = db.scalar(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "BTC/USD")
    )
    assert position is not None
    original_identity = position.raw_payload["_position_identity"]
    assert "alpaca-account-stable-id" not in str(position.raw_payload)

    rotated_broker = AlpacaSnapshotBroker(
        account_snapshot=snapshot(
            account_id="alpaca-account-stable-id",
            quantity=3,
            observed_at=OBSERVED_AT + timedelta(seconds=1),
        )
    )
    result = sync_execution_positions_from_broker_snapshot(
        settings=rotated_settings,
        db=db,
        adapter=rotated_broker,
        symbols={"BTC/USD"},
        exchange="ALPACA",
    )
    db.refresh(position)
    assert result["updated"] == 1
    assert position.quantity == 3
    assert position.raw_payload["_position_identity"] == original_identity

    other_account_broker = AlpacaSnapshotBroker(
        account_snapshot=snapshot(
            account_id="alpaca-other-account-id",
            quantity=7,
            observed_at=OBSERVED_AT + timedelta(seconds=2),
        )
    )
    sync_execution_positions_from_broker_snapshot(
        settings=rotated_settings,
        db=db,
        adapter=other_account_broker,
        symbols={"BTC/USD"},
        exchange="ALPACA",
    )
    db.refresh(position)
    assert position.quantity == 3
    assert db.scalar(
        select(func.count()).select_from(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "BTC/USD",
        )
    ) == 2
    other_position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "BTC/USD",
            ExecutionPosition.account_scope != position.account_scope,
        )
    )
    assert other_position is not None
    assert other_position.quantity == 7
    engine.dispose()


def test_alpaca_position_sync_rejects_missing_authenticated_account_id() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="operator-label-is-not-account-truth",
        account_drawdown_recovery_scope_secret="stable-position-scope-secret",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
    )
    engine, db = _session(settings)

    class AlpacaSnapshotBroker(ReconciliationStubBroker):
        provider = "alpaca"

    broker = AlpacaSnapshotBroker(
        account_snapshot=BrokerAccountSnapshot(
            broker_account_id=None,
            observed_at=OBSERVED_AT,
            source="stub:alpaca-missing-account-id",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=1,
                    average_price=Decimal("100"),
                )
            ],
        )
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="authenticated broker account identity is unavailable",
    ):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=broker,
            symbols={"AAPL"},
            exchange="US",
            authenticated_access_token="validated",
        )

    assert db.scalar(select(func.count()).select_from(ExecutionPosition)) == 0
    db.rollback()
    engine.dispose()


def test_upbit_position_identity_uses_stable_configured_scope_not_credentials() -> None:
    original_settings = Settings(
        runtime_mode="paper",
        upbit_access_key="rotating-access-v1",
        upbit_secret_key="rotating-secret-v1",
        execution_position_account_scope="primary-upbit-account",
    )
    rotated_settings = Settings(
        runtime_mode="paper",
        upbit_access_key="rotating-access-v2",
        upbit_secret_key="rotating-secret-v2",
        execution_position_account_scope="primary-upbit-account",
    )
    other_scope_settings = rotated_settings.model_copy(
        update={"execution_position_account_scope": "secondary-upbit-account"}
    )

    original_identity = build_position_identity(
        settings=original_settings,
        provider="upbit",
        exchange="UPBIT",
    )
    rotated_identity = build_position_identity(
        settings=rotated_settings,
        provider="upbit",
        exchange="UPBIT",
    )
    other_scope_identity = build_position_identity(
        settings=other_scope_settings,
        provider="upbit",
        exchange="UPBIT",
    )

    assert rotated_identity == original_identity
    assert other_scope_identity != original_identity
    persisted_identity = original_identity.as_payload()
    assert "rotating-access" not in str(persisted_identity)
    assert "rotating-secret" not in str(persisted_identity)
    assert "primary-upbit-account" not in str(persisted_identity)


def test_live_raw_broker_account_scope_is_hmac_keyed_and_rotation_stable() -> None:
    stable_secret = "stable-position-scope-secret-32-bytes"
    original_settings = Settings(
        runtime_mode="live",
        alpaca_api_key="rotating-key-v1",
        alpaca_api_secret="rotating-secret-v1",
        account_drawdown_recovery_scope_secret=stable_secret,
    )
    rotated_settings = original_settings.model_copy(
        update={
            "alpaca_api_key": "rotating-key-v2",
            "alpaca_api_secret": "rotating-secret-v2",
        }
    )
    account_id = "stable-alpaca-account-id"

    original_identity = build_position_identity(
        settings=original_settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id=account_id,
    )
    rotated_identity = build_position_identity(
        settings=rotated_settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id=account_id,
    )

    message = (
        "execution-position-route-v2\0alpaca\0"
        f"broker-account-id\0{account_id}"
    ).encode("utf-8")
    expected_scope = hmac.new(
        stable_secret.encode("utf-8"),
        message,
        sha256,
    ).hexdigest()[:24]
    legacy_unkeyed_scope = sha256(message).hexdigest()[:24]
    assert original_identity == rotated_identity
    assert original_identity.account_scope == expected_scope
    assert original_identity.account_scope != legacy_unkeyed_scope
    assert account_id not in str(original_identity.as_payload())


def test_live_raw_account_identity_fails_closed_without_stable_hmac_secret() -> None:
    settings = Settings(
        runtime_mode="live",
        alpaca_api_key="configured-key",
        alpaca_api_secret="configured-secret",
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET",
    ):
        build_position_identity(
            settings=settings,
            provider="alpaca",
            exchange="ALPACA",
            broker_account_id="raw-account-id",
        )


def test_live_raw_account_identity_rejects_short_hmac_secret() -> None:
    settings = Settings(
        runtime_mode="live",
        account_drawdown_recovery_scope_secret="short-secret",
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="32 bytes",
    ):
        build_position_identity(
            settings=settings,
            provider="alpaca",
            exchange="NASD",
            broker_account_id="raw-account-id",
        )


def test_live_kis_account_scope_is_keyed_and_requires_stable_hmac_secret() -> None:
    account_number = "12345678"
    product_code = "01"
    stable_secret = "stable-kis-position-scope-secret"
    settings = Settings(
        runtime_mode="live",
        kis_account_number=account_number,
        kis_account_product_code=product_code,
        account_drawdown_recovery_scope_secret=stable_secret,
    )

    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )

    message = (
        "execution-position-route-v2\0kis\0configured-kis-account\0"
        f"{account_number}\0{product_code}"
    ).encode("utf-8")
    assert identity.account_scope == hmac.new(
        stable_secret.encode("utf-8"),
        message,
        sha256,
    ).hexdigest()[:24]
    assert identity.account_scope != sha256(message).hexdigest()[:24]
    assert account_number not in str(identity.as_payload())

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET",
    ):
        build_position_identity(
            settings=settings.model_copy(
                update={"account_drawdown_recovery_scope_secret": None}
            ),
            provider="kis",
            exchange="KRX",
        )


def test_live_kis_account_scope_prefers_account_material_over_operator_label() -> None:
    stable_secret = "stable-kis-position-scope-secret-at-least-32-bytes"
    shared = {
        "runtime_mode": "live",
        "kis_account_product_code": "01",
        "account_drawdown_recovery_scope_secret": stable_secret,
        "execution_position_account_scope": "primary-kis-label",
    }
    account_a = Settings(**shared, kis_account_number="11111111")
    account_b = Settings(**shared, kis_account_number="22222222")
    relabeled_a = account_a.model_copy(
        update={"execution_position_account_scope": "renamed-kis-label"}
    )

    identity_a = build_position_identity(
        settings=account_a,
        provider="kis",
        exchange="KRX",
    )
    identity_b = build_position_identity(
        settings=account_b,
        provider="kis",
        exchange="KRX",
    )
    relabeled_identity_a = build_position_identity(
        settings=relabeled_a,
        provider="kis",
        exchange="KRX",
    )

    assert identity_a != identity_b
    assert relabeled_identity_a == identity_a
    assert "11111111" not in str(identity_a.as_payload())
    assert "primary-kis-label" not in str(identity_a.as_payload())


def test_live_configured_non_secret_position_scope_does_not_require_hmac_key() -> None:
    settings = Settings(
        runtime_mode="live",
        execution_position_account_scope="primary-upbit-account",
    )

    identity = build_position_identity(
        settings=settings,
        provider="upbit",
        exchange="UPBIT",
    )

    assert len(identity.account_scope) == 24
    assert "primary-upbit-account" not in str(identity.as_payload())


def test_live_alpaca_authenticated_scope_rejects_short_key_despite_operator_label() -> None:
    settings = Settings(
        runtime_mode="live",
        account_drawdown_recovery_scope_secret="short-secret",
        execution_position_account_scope="primary-alpaca-account",
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET",
    ):
        build_position_identity(
            settings=settings,
            provider="alpaca",
            exchange="NASD",
            broker_account_id="raw-account-id",
        )


def test_live_alpaca_authenticated_id_is_hmac_material_when_label_is_available() -> None:
    stable_secret = "stable-position-scope-secret-32-bytes"
    configured_scope = "primary-alpaca-account"
    settings = Settings(
        runtime_mode="live",
        account_drawdown_recovery_scope_secret=stable_secret,
        execution_position_account_scope=configured_scope,
    )

    identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="NASD",
        broker_account_id="raw-account-id",
    )

    configured_message = (
        "execution-position-route-v2\0alpaca\0"
        f"configured-account-scope\0{configured_scope}"
    ).encode("utf-8")
    raw_message = (
        "execution-position-route-v2\0alpaca\0"
        "broker-account-id\0raw-account-id"
    ).encode("utf-8")
    assert identity.account_scope == hmac.new(
        stable_secret.encode("utf-8"),
        raw_message,
        sha256,
    ).hexdigest()[:24]
    assert identity.account_scope != hmac.new(
        stable_secret.encode("utf-8"),
        configured_message,
        sha256,
    ).hexdigest()[:24]


def test_position_sync_uses_separate_row_for_configured_account_scope() -> None:
    primary_settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="primary-upbit-account",
    )
    secondary_settings = primary_settings.model_copy(
        update={"execution_position_account_scope": "secondary-upbit-account"}
    )
    engine, db = _session(primary_settings)

    class UpbitSnapshotBroker(ReconciliationStubBroker):
        provider = "upbit"

        def describe(self) -> BrokerConfig:
            return BrokerConfig(
                provider="upbit",
                mode="paper",
                configured=True,
                api_base_url="https://stub.upbit.local",
            )

    primary_broker = UpbitSnapshotBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=OBSERVED_AT,
            source="stub:upbit",
            holdings=[
                BrokerAccountHolding(
                    symbol="KRW-BTC",
                    quantity=0,
                    average_price=Decimal("100000000"),
                    payload={"base_quantity": "0.001"},
                )
            ],
        )
    )
    sync_execution_positions_from_broker_snapshot(
        settings=primary_settings,
        db=db,
        adapter=primary_broker,
        symbols={"KRW-BTC"},
        exchange="UPBIT",
    )

    secondary_broker = UpbitSnapshotBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=OBSERVED_AT + timedelta(seconds=1),
            source="stub:upbit",
            holdings=[
                BrokerAccountHolding(
                    symbol="KRW-BTC",
                    quantity=0,
                    average_price=Decimal("110000000"),
                    payload={"base_quantity": "0.002"},
                )
            ],
        )
    )
    sync_execution_positions_from_broker_snapshot(
        settings=secondary_settings,
        db=db,
        adapter=secondary_broker,
        symbols={"KRW-BTC"},
        exchange="UPBIT",
    )

    positions = db.scalars(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "KRW-BTC")
    ).all()
    assert {position.base_quantity for position in positions} == {
        Decimal("0.001000000000"),
        Decimal("0.002000000000"),
    }
    assert db.scalar(
        select(func.count()).select_from(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "KRW-BTC",
        )
    ) == 2
    engine.dispose()


def test_position_row_lookup_fails_closed_when_route_symbol_invariant_is_violated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="upbit",
        exchange="UPBIT",
    )
    duplicate_rows = [
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="KRW-BTC",
        ),
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="KRW-BTC",
        ),
    ]

    class DuplicateScalarResult:
        def all(self) -> list[ExecutionPosition]:
            return duplicate_rows

    monkeypatch.setattr(db, "scalars", lambda _statement: DuplicateScalarResult())

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="one-row-per-route-symbol",
    ):
        reconciliation_service._lock_execution_position_rows(
            db=db,
            mode="paper",
            symbols={"KRW-BTC"},
            identity=identity,
        )
    db.rollback()
    engine.dispose()


def test_live_position_identity_without_broker_id_or_configured_scope_fails_closed() -> None:
    settings = Settings(
        runtime_mode="live",
        upbit_access_key="live-access-key",
        upbit_secret_key="live-secret-key",
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="stable broker account ID or EXECUTION_POSITION_ACCOUNT_SCOPE",
    ):
        build_position_identity(
            settings=settings,
            provider="upbit",
            exchange="UPBIT",
        )


def test_older_account_snapshot_cannot_overwrite_newer_position_truth() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="***",
        kis_app_secret="***",
        kis_account_number="12345678",
    )
    engine, db = _session(settings)
    newer_observed_at = OBSERVED_AT + timedelta(minutes=1)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="005930",
            quantity=2,
            average_price=Decimal("71000"),
            updated_from="reconciliation",
            last_reconciled_at=newer_observed_at,
            raw_payload=payload_with_position_identity(
                {"snapshot": "newer"},
                identity=build_position_identity(
                    settings=settings,
                    provider="kis",
                    exchange="KRX",
                ),
            ),
        )
    )
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:delayed_account_snapshot",
            holdings=[],
            payload={"snapshot": "older"},
        )
    )

    result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"005930"},
    )

    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "005930",
        )
    )
    assert result["zeroed"] == 0
    assert position is not None
    assert position.quantity == 2
    assert position.average_price == Decimal("71000.0000")
    assert _utc(position.last_reconciled_at) == newer_observed_at
    assert position.raw_payload["snapshot"] == "newer"
    assert position.raw_payload["_position_identity"]["provider"] == "kis"
    engine.dispose()


def test_matching_broker_values_cannot_bind_nonflat_legacy_position_to_current_account() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="22222222",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-position-scope-secret-at-least-32-bytes",
    )
    engine, db = _session(settings)
    legacy_position = ExecutionPosition(
        mode="paper",
        symbol="005930",
        quantity=1,
        average_price=Decimal("71000"),
        updated_from="legacy_import",
        raw_payload={"legacy_marker": "unknown-account"},
    )
    db.add(legacy_position)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:matching_other_account",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("71000"),
                    sellable_quantity=1,
                )
            ],
        )
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="legacy non-flat position requires explicit account mapping",
    ):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=broker,
            symbols={"005930"},
        )

    db.rollback()
    db.refresh(legacy_position)
    assert legacy_position.provider is None
    assert legacy_position.account_scope is None
    assert legacy_position.exchange is None
    assert legacy_position.raw_payload == {"legacy_marker": "unknown-account"}
    assert db.scalar(
        select(func.count()).select_from(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "005930",
        )
    ) == 1
    engine.dispose()


def test_scoped_position_sync_cannot_ignore_parallel_nonflat_legacy_row() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="22222222",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-position-scope-secret-at-least-32-bytes",
    )
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    scoped_position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=1,
        average_price=Decimal("70000"),
        updated_from="reconciliation",
        raw_payload=payload_with_position_identity({}, identity=identity),
    )
    legacy_position = ExecutionPosition(
        mode="paper",
        symbol="005930",
        quantity=2,
        average_price=Decimal("71000"),
        updated_from="legacy_import",
        raw_payload={"legacy_marker": "unknown-account"},
    )
    db.add_all([scoped_position, legacy_position])
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="stub:current_account",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=3,
                    average_price=Decimal("72000"),
                    sellable_quantity=3,
                )
            ],
        )
    )

    with pytest.raises(
        reconciliation_service.PositionIdentityConflictError,
        match="legacy position requires explicit account cleanup",
    ):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=broker,
            symbols={"005930"},
        )

    db.rollback()
    db.refresh(scoped_position)
    db.refresh(legacy_position)
    assert scoped_position.quantity == 1
    assert scoped_position.average_price == Decimal("70000.0000")
    assert legacy_position.provider is None
    assert legacy_position.quantity == 2
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
    snapshot_observed_at = stale_submitted_at + timedelta(days=7, minutes=1)
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
            raw_payload=payload_with_position_identity(
                None,
                identity=build_position_identity(
                    settings=settings,
                    provider="kis",
                    exchange="KRX",
                ),
            ),
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
            observed_at=snapshot_observed_at,
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
    assert position.last_reconciled_at == snapshot_observed_at.replace(tzinfo=None)
    engine.dispose()


@pytest.mark.parametrize("order_exchange", ["NXT", "SOR"])
def test_reconcile_stale_domestic_fill_queries_order_route_but_stores_krx_position(
    order_exchange: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=7)
    order = _order(
        settings=settings,
        broker_order_id=f"stale-{order_exchange.lower()}-filled-1",
        request_metadata={"route": {"exchange": order_exchange}},
    )
    order.submitted_at = stale_submitted_at
    order.last_broker_sync_at = stale_submitted_at
    db.add(order)
    db.commit()

    class RouteRecordingBroker(ReconciliationStubBroker):
        def __init__(self) -> None:
            super().__init__(
                reconcile_payloads=[
                    {
                        "broker_order_id": order.broker_order_id,
                        "status": "filled",
                        "filled_quantity": 2,
                        "remaining_quantity": 0,
                        "average_fill_price": "71000",
                        "observed_at": OBSERVED_AT.isoformat(),
                    }
                ],
                account_snapshot=BrokerAccountSnapshot(
                    available_cash=Decimal("1000000"),
                    observed_at=OBSERVED_AT + timedelta(minutes=1),
                    source="stub:account_snapshot",
                    holdings=[
                        BrokerAccountHolding(
                            symbol="005930",
                            quantity=2,
                            average_price=Decimal("70000"),
                        )
                    ],
                ),
            )
            self.snapshot_calls: list[tuple[list[str] | None, str]] = []

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            del reference_prices
            self.snapshot_calls.append((symbols, exchange))
            return self.account_snapshot

    broker = RouteRecordingBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
    )

    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == "paper",
            ExecutionPosition.symbol == "005930",
        )
    ).all()
    assert result["counts"]["closed"] == 1
    assert broker.reconcile_calls == [
        (order.broker_order_id, order_exchange, stale_submitted_at)
    ]
    assert broker.snapshot_calls == [(["005930"], order_exchange)]
    assert len(positions) == 1
    assert positions[0].exchange == "KRX"
    assert positions[0].quantity == 2
    assert all(position.exchange not in {"NXT", "SOR"} for position in positions)
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


def test_reconcile_stale_missing_id_does_not_downgrade_filled_plan_sibling() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_submitted_at = datetime.now(UTC) - timedelta(days=5)
    plan, item, filled_order = _plan_backed_order(
        settings=settings,
        plan_id="plan-stale-retry-with-filled-sibling",
        side="sell",
        intent="stop_sell",
        broker_order_id="filled-retry-successor",
        status="filled",
    )
    item.status = "filled"
    stale_retry = BrokerOrder(
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="stale-retry-before-filled-successor",
        broker_order_id=None,
        symbol=item.symbol,
        side="sell",
        status="retry_pending",
        quantity=2,
        submitted_at=stale_submitted_at,
        last_broker_sync_at=stale_submitted_at,
        request_metadata=filled_order.request_metadata,
    )
    db.add_all([plan, stale_retry])
    db.commit()

    reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=ReconciliationStubBroker(),
    )

    db.refresh(item)
    db.refresh(stale_retry)
    assert stale_retry.status == "manual_review_required"
    assert item.status == "filled"
    engine.dispose()


@pytest.mark.parametrize("scope_drift", ["mode", "broker", "account"])
def test_authoritative_plan_sibling_outcome_rejects_scope_drift(
    scope_drift: str,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, db = _session(settings)
    plan, item, sibling = _plan_backed_order(
        settings=settings,
        plan_id=f"sibling-outcome-{scope_drift}-drift",
        side="sell",
        intent="stop_sell",
        status="filled",
    )
    candidate_metadata = dict(sibling.request_metadata)
    candidate_scope = dict(candidate_metadata["execution_scope"])
    candidate_metadata["execution_scope"] = candidate_scope
    sibling_scope = dict(candidate_scope)
    sibling.request_metadata = {
        **sibling.request_metadata,
        "execution_scope": sibling_scope,
    }
    if scope_drift == "mode":
        sibling.mode = "live"
    elif scope_drift == "broker":
        sibling.broker_name = "alpaca"
        sibling_scope["provider"] = "alpaca"
    else:
        sibling_scope["account_scope"] = "b" * 24
    candidate = BrokerOrder(
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key=f"scope-drift-candidate-{scope_drift}",
        symbol="005930",
        side="sell",
        status="retry_pending",
        quantity=2,
        venue=sibling.venue,
        asset_class=sibling.asset_class,
        request_metadata=candidate_metadata,
    )
    db.add_all([plan, candidate, sibling])
    db.commit()

    outcome = reconciliation_service._authoritative_plan_sibling_outcome_status(
        db=db,
        order=candidate,
    )

    assert outcome is None
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


def test_reconcile_stale_dry_run_surfaces_fingerprinted_legacy_manual_review_candidate() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-manual-review-dry-run",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=2,
        created_at=stale_at,
        submitted_at=None,
        last_broker_sync_at=stale_at,
        last_error_classification="missing_broker_order_id",
        last_error_code="IGW00007",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings=[],
            open_orders=[],
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    db.refresh(order)
    resolution = result["legacy_manual_review_resolution"]
    assert resolution["candidate_order_count"] == 1
    assert resolution["eligible_order_count"] == 1
    assert resolution["would_resolve_order_count"] == 1
    assert resolution["resolved_order_count"] == 0
    assert len(resolution["items"][0]["evidence_fingerprint"]) == 64
    assert resolution["items"][0]["repair_action"] == "approval_required"
    assert resolution["items"][0]["broker_position_match"] is False
    assert resolution["items"][0]["broker_open_order_match"] is False
    assert resolution["items"][0]["account_product_code_proof"] == (
        "legacy_missing"
    )
    assert resolution["items"][0]["snapshot_route_product_code"] == "01"
    assert order.status == "manual_review_required"
    assert db.scalars(select(BrokerOrderEvent)).all() == []
    assert broker.order_calls == []
    engine.dispose()


def test_reconcile_stale_never_surfaces_legacy_manual_review_younger_than_thirty_days() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=60,
    )
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-manual-review-too-young",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=2,
        created_at=datetime.now(UTC) - timedelta(days=29),
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        older_than_seconds=60,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    resolution = result["legacy_manual_review_resolution"]
    assert resolution["minimum_age_seconds"] == 30 * 24 * 60 * 60
    assert resolution["candidate_order_count"] == 0
    assert broker.account_snapshot_calls == []
    engine.dispose()


def test_sizing_stale_reconciliation_never_discovers_legacy_manual_review() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-manual-review-must-not-run-during-sizing",
            broker_order_id=None,
            symbol="052420",
            side="buy",
            status="manual_review_required",
            quantity=1,
            created_at=datetime.now(UTC) - timedelta(days=45),
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0012U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )
    )
    db.commit()
    broker = ReconciliationStubBroker()

    result = _reconcile_stale_sizing_orders(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"052420"},
    )

    assert result["legacy_manual_review_resolution"]["candidate_order_count"] == 0
    assert broker.account_snapshot_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("submitted_at", datetime(2026, 6, 1, tzinfo=UTC)),
        ("venue", "KRX"),
        ("asset_class", "stock"),
    ],
)
def test_reconcile_stale_excludes_nonlegacy_manual_review_shape_and_rejects_approval(
    field: str,
    value: object,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key=f"legacy-shape-exclusion-{field}",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=1,
        created_at=stale_at,
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
        **{field: value},
    )
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker()

    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    assert dry_run["legacy_manual_review_resolution"]["candidate_order_count"] == 0
    assert broker.account_snapshot_calls == []
    with pytest.raises(
        reconciliation_service.LegacyManualReviewApprovalConflictError,
        match="target is no longer a candidate",
    ):
        reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            actor="ops-desk",
            operator_reason="stale excluded-shape approval",
            manual_review_approvals={order.id: "a" * 64},
            include_legacy_manual_review=True,
        )
    db.rollback()
    db.refresh(order)
    assert order.status == "manual_review_required"
    assert db.scalars(select(BrokerOrderEvent)).all() == []
    engine.dispose()


def test_reconcile_stale_blocks_conflicting_legacy_account_product_code() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-account-product-code-mismatch",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=1,
        created_at=datetime.now(UTC) - timedelta(days=45),
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "account_product_code": "02",
            "route": {"exchange": "KRX"},
        },
    )
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    item = result["legacy_manual_review_resolution"]["items"][0]
    assert item["eligible"] is False
    assert item["reason"] == "legacy_account_product_code_proof_invalid"
    assert item["account_product_code_proof"] is None
    assert item["snapshot_route_product_code"] == "01"
    assert item["evidence_fingerprint"] is None
    engine.dispose()


def test_reconcile_stale_applies_explicit_legacy_manual_review_approval_without_broker_mutation() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-manual-review-apply",
        broker_order_id=None,
        symbol="033530",
        side="sell",
        status="manual_review_required",
        quantity=5,
        created_at=stale_at,
        last_broker_sync_at=stale_at,
        last_error_classification="missing_broker_order_id",
        last_error_code="APBK0918",
        last_error_message="legacy broker submission outcome unknown",
        last_error_retryable=False,
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0011U",
            "account_number_masked": "****2504",
            "account_product_code": "01",
            "route": {"exchange": "KRX"},
        },
        response_metadata={"status_code": 500, "rt_cd": "7"},
        raw_broker_payload={"rt_cd": "7", "msg_cd": "APBK0918"},
    )
    db.add(order)
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings=[],
            open_orders=[],
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )
    item = dry_run["legacy_manual_review_resolution"]["items"][0]

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=False,
        actor="ops-desk",
        operator_reason="verified absent from complete broker truth",
        manual_review_approvals={order.id: item["evidence_fingerprint"]},
        include_legacy_manual_review=True,
    )

    db.refresh(order)
    repair = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "stale_order_repair",
        )
    )
    remediation = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.candidate_type
            == "legacy_manual_review_order",
            RuntimeRemediationEvent.subject_id == order.id,
            RuntimeRemediationEvent.outcome == "stale_terminal_unknown",
        )
    )
    resolution = result["legacy_manual_review_resolution"]
    assert resolution["resolved_order_count"] == 1
    assert resolution["items"][0]["repair_action"] == "repaired"
    assert order.status == "stale_terminal_unknown"
    assert order.last_error_classification == "missing_broker_order_id"
    assert order.last_error_code == "APBK0918"
    assert order.last_error_message == "legacy broker submission outcome unknown"
    assert order.last_error_retryable is False
    assert order.last_broker_sync_at == stale_at.replace(tzinfo=None)
    assert order.response_metadata == {"status_code": 500, "rt_cd": "7"}
    assert order.raw_broker_payload == {"rt_cd": "7", "msg_cd": "APBK0918"}
    assert order.fills == []
    assert broker.order_calls == []
    assert repair is not None
    assert repair.payload["actor"] == "ops-desk"
    assert repair.payload["operator_reason"] == (
        "verified absent from complete broker truth"
    )
    assert repair.payload["result"]["evidence_fingerprint"] == item[
        "evidence_fingerprint"
    ]
    assert repair.payload["result"]["broker_mutations"] == 0
    assert repair.payload["result"]["fills_written"] == 0
    assert repair.payload["result"]["account_product_code_proof"] == (
        "exact_match"
    )
    assert repair.payload["result"]["snapshot_route_product_code"] == "01"
    assert remediation is not None
    assert remediation.actor == "ops-desk"
    assert remediation.reason == "verified absent from complete broker truth"
    assert remediation.evidence_summary["evidence_fingerprint"] == item[
        "evidence_fingerprint"
    ]
    assert remediation.evidence_summary["account_product_code_proof"] == (
        "exact_match"
    )
    assert remediation.evidence_summary["snapshot_route_product_code"] == "01"
    assert broker.account_snapshot_calls == [
        (None, "KRX"),
        (None, "KRX"),
        (None, "KRX"),
    ]
    engine.dispose()


def test_reconcile_stale_api_rejects_stale_manual_review_fingerprint_after_row_drift() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as db:
        order = BrokerOrder(
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-manual-review-drift",
            broker_order_id=None,
            symbol="069960",
            side="sell",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_broker_sync_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0011U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )
        db.add(order)
        db.commit()
        order_id = order.id
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: broker,
    )
    client = TestClient(app)
    dry_run = client.post("/orders/reconcile-stale", json={"dry_run": True})
    fingerprint = dry_run.json()["legacy_manual_review_resolution"]["items"][0][
        "evidence_fingerprint"
    ]
    with session_factory() as db:
        persisted = db.get(BrokerOrder, order_id)
        assert persisted is not None
        persisted.response_metadata = {"status_code": 500, "rt_cd": "7"}
        db.commit()

    response = client.post(
        "/orders/reconcile-stale",
        json={
            "actor": "ops-desk",
            "reason": "approve exact reviewed row",
            "manual_review_approvals": {order_id: fingerprint},
        },
    )

    assert response.status_code == 409
    assert "fingerprint is stale" in response.json()["detail"]
    with session_factory() as db:
        persisted = db.get(BrokerOrder, order_id)
        repair_events = db.scalars(
            select(BrokerOrderEvent).where(
                BrokerOrderEvent.broker_order_pk == order_id,
                BrokerOrderEvent.event_type == "stale_order_repair",
            )
        ).all()
        assert persisted is not None
        assert persisted.status == "manual_review_required"
        assert repair_events == []
    engine.dispose()


def test_reconcile_stale_requires_atomic_approval_for_manual_review_siblings() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    plan = ExecutionPlan(
        external_plan_id="legacy-sibling-plan",
        source="test",
        mode="live",
        market_date=stale_at.date(),
    )
    plan_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="033530",
        side="sell",
        intent="stop_sell",
        status="manual_review_required",
        quantity_policy={"kind": "shares", "quantity": 5},
    )

    def sibling(suffix: str) -> BrokerOrder:
        return BrokerOrder(
            plan_item=plan_item,
            broker_name="kis",
            mode="live",
            idempotency_key=f"legacy-manual-review-sibling-{suffix}",
            broker_order_id=None,
            symbol="033530",
            side="sell",
            status="manual_review_required",
            quantity=5,
            created_at=stale_at,
            last_broker_sync_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0011U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )

    first = sibling("first")
    second = sibling("second")
    db.add_all([plan, first, second])
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )
    approvals = {
        item["broker_order_pk"]: item["evidence_fingerprint"]
        for item in dry_run["legacy_manual_review_resolution"]["items"]
    }

    with pytest.raises(
        reconciliation_service.LegacyManualReviewApprovalConflictError,
        match="sibling group requires atomic approval",
    ):
        reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            actor="ops-desk",
            operator_reason="approve reviewed sibling",
            manual_review_approvals={first.id: approvals[first.id]},
            include_legacy_manual_review=True,
        )
    db.rollback()
    db.refresh(first)
    db.refresh(second)
    assert first.status == "manual_review_required"
    assert second.status == "manual_review_required"
    assert plan_item.status == "manual_review_required"

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        actor="ops-desk",
        operator_reason="approve complete reviewed sibling group",
        manual_review_approvals=approvals,
        include_legacy_manual_review=True,
    )

    db.refresh(first)
    db.refresh(second)
    db.refresh(plan_item)
    audits = db.scalars(
        select(ExecutionPlanItemAudit).where(
            ExecutionPlanItemAudit.execution_plan_item_id == plan_item.id,
            ExecutionPlanItemAudit.source == "operator_remediation",
        )
    ).all()
    assert result["legacy_manual_review_resolution"]["resolved_order_count"] == 2
    assert first.status == "stale_terminal_unknown"
    assert second.status == "stale_terminal_unknown"
    assert plan_item.status == "broker_order_failed"
    assert len(audits) == 1
    engine.dispose()


def test_reconcile_stale_blocks_legacy_manual_review_with_failure_or_active_job() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    plan = ExecutionPlan(
        external_plan_id="legacy-active-job-plan",
        source="test",
        mode="live",
        market_date=stale_at.date(),
    )
    plan_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="073560",
        side="sell",
        intent="stop_sell",
        status="broker_order_failed",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    job = ExecutionJob(
        plan_item=plan_item,
        job_type="broker_order_submit",
        status="queued",
    )

    def legacy_order(symbol: str, suffix: str) -> BrokerOrder:
        return BrokerOrder(
            broker_name="kis",
            mode="live",
            idempotency_key=f"legacy-manual-review-blocked-{suffix}",
            broker_order_id=None,
            symbol=symbol,
            side="sell",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_broker_sync_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0011U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )

    job_order = legacy_order("073560", "job")
    job_order.plan_item = plan_item
    failure_order = legacy_order("069960", "failure")
    db.add_all([plan, job, job_order, failure_order])
    db.commit()
    db.add(
        ExecutionFailure(
            mode="live",
            broker_order_pk=failure_order.id,
            broker_name="kis",
            idempotency_key=failure_order.idempotency_key,
            symbol=failure_order.symbol,
            side=failure_order.side,
            requested_quantity=1,
            classification="legacy_submission_unknown",
            broker_error_message="legacy broker submission outcome unknown",
            retryable=False,
            status="resolved",
            first_failed_at=stale_at,
            latest_failed_at=stale_at,
        )
    )
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    items = {
        item["symbol"]: item
        for item in result["legacy_manual_review_resolution"]["items"]
    }
    assert items["069960"]["reason"] == "linked_execution_failure_present"
    assert items["069960"]["linked_execution_failure_count"] == 1
    assert items["069960"]["evidence_fingerprint"] is None
    assert items["073560"]["reason"] == "active_execution_job_present"
    assert items["073560"]["active_execution_job_count"] == 1
    assert items["073560"]["evidence_fingerprint"] is None
    engine.dispose()


def test_legacy_manual_review_job_lock_query_reads_all_plan_jobs() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine, db = _session(settings)
    plan = ExecutionPlan(
        external_plan_id="legacy-all-job-lock-plan",
        source="test",
        mode="live",
        market_date=datetime.now(UTC).date(),
    )
    plan_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="073560",
        side="sell",
        intent="stop_sell",
        status="broker_order_failed",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    completed_job = ExecutionJob(
        plan_item=plan_item,
        job_type="completed_submit",
        status="completed",
    )
    queued_job = ExecutionJob(
        plan_item=plan_item,
        job_type="queued_submit",
        status="queued",
    )
    db.add_all([plan, completed_job, queued_job])
    db.commit()
    statements: list[str] = []

    def capture_statement(
        _connection,
        _cursor,
        statement: str,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_statement)
    try:
        jobs_by_item = (
            reconciliation_service._load_legacy_manual_review_active_jobs(
                db=db,
                plan_item_ids={plan_item.id},
                for_update=True,
            )
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture_statement)

    job_selects = [
        statement.lower()
        for statement in statements
        if "from execution_jobs" in statement.lower()
    ]
    assert len(job_selects) == 1
    assert "execution_jobs.status in" not in job_selects[0]
    assert [job.id for job in jobs_by_item[plan_item.id]] == [queued_job.id]
    engine.dispose()


def test_legacy_manual_review_postgres_position_truth_fence_blocks_dml() -> None:
    class PostgreSQLDialect:
        name = "postgresql"

    class PostgreSQLBind:
        dialect = PostgreSQLDialect()

    class RecordingSession:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def get_bind(self):
            return PostgreSQLBind()

        def execute(self, statement):
            self.statements.append(str(statement))

    db = RecordingSession()

    reconciliation_service._acquire_legacy_manual_review_position_truth_fence(
        db=db,
    )

    assert db.statements == [
        "LOCK TABLE execution_positions IN SHARE MODE NOWAIT"
    ]


@pytest.mark.parametrize(
    ("sibling_status", "with_fill", "sibling_broker", "sibling_mode"),
    [
        ("filled", False, "kis", "live"),
        ("partially_filled", False, "kis", "live"),
        ("failed", True, "alpaca", "paper"),
    ],
)
def test_reconcile_stale_blocks_plan_group_with_outcome_bearing_sibling(
    sibling_status: str,
    with_fill: bool,
    sibling_broker: str,
    sibling_mode: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    plan = ExecutionPlan(
        external_plan_id=f"legacy-outcome-sibling-{sibling_status}-{with_fill}",
        source="test",
        mode="live",
        market_date=stale_at.date(),
    )
    plan_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="033530",
        side="sell",
        intent="stop_sell",
        status="manual_review_required",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    candidate = BrokerOrder(
        plan_item=plan_item,
        broker_name="kis",
        mode="live",
        idempotency_key=f"legacy-outcome-candidate-{sibling_status}-{with_fill}",
        broker_order_id=None,
        symbol="033530",
        side="sell",
        status="manual_review_required",
        quantity=1,
        created_at=stale_at,
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0011U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    sibling = BrokerOrder(
        plan_item=plan_item,
        broker_name=sibling_broker,
        mode=sibling_mode,
        idempotency_key=f"legacy-outcome-sibling-order-{sibling_status}-{with_fill}",
        broker_order_id="historical-outcome-sibling",
        symbol="033530",
        side="sell",
        status=sibling_status,
        quantity=1,
        created_at=stale_at,
        submitted_at=datetime.now(UTC),
        last_broker_sync_at=datetime.now(UTC),
    )
    if with_fill:
        sibling.fills.append(
            ExecutionFill(
                broker_fill_id="historical-outcome-fill",
                symbol="033530",
                side="sell",
                quantity=1,
                price=Decimal("1000"),
                filled_at=stale_at,
            )
        )
    db.add_all([plan, candidate, sibling])
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )

    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )

    item = dry_run["legacy_manual_review_resolution"]["items"][0]
    assert item["broker_order_pk"] == candidate.id
    assert item["eligible"] is False
    assert item["reason"] == "outcome_bearing_plan_sibling_present"
    assert item["outcome_bearing_sibling_count"] == 1
    assert item["evidence_fingerprint"] is None
    with pytest.raises(
        reconciliation_service.LegacyManualReviewApprovalConflictError,
        match="fingerprint is stale or ineligible",
    ):
        reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            actor="ops-desk",
            operator_reason="must not overwrite historical outcome",
            manual_review_approvals={candidate.id: "a" * 64},
            include_legacy_manual_review=True,
        )
    db.rollback()
    db.refresh(candidate)
    db.refresh(plan_item)
    assert candidate.status == "manual_review_required"
    assert plan_item.status == "manual_review_required"
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    engine.dispose()


def _verified_retry_successor_settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )


def _persist_verified_retry_successor(db):
    candidate_at = datetime(2026, 5, 28, tzinfo=UTC)
    rearmed_at = datetime(2026, 5, 29, 2, 57, tzinfo=UTC)
    successor_at = datetime(2026, 5, 29, 3, tzinfo=UTC)
    plan = ExecutionPlan(
        external_plan_id="legacy-verified-retry-successor-plan",
        source="test",
        mode="live",
        market_date=candidate_at.date(),
    )
    plan_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="033530",
        side="sell",
        intent="stop_sell",
        status="broker_order_failed",
        quantity_policy={"kind": "shares", "quantity": 58},
        risk_context={
            "monitoring_events": [
                {
                    "event": "protective_stop_rearmed",
                    "reason": "retryable_session_rejection",
                    "prior_status": "broker_order_retry_pending",
                    "recorded_at": rearmed_at.isoformat(),
                }
            ]
        },
    )
    db.add(plan)
    db.flush()
    candidate_key = f"monitoring:stop_sell:{plan_item.id}"
    candidate = BrokerOrder(
        plan_item=plan_item,
        broker_name="kis",
        mode="live",
        idempotency_key=candidate_key,
        symbol="033530",
        side="sell",
        status="manual_review_required",
        quantity=58,
        created_at=candidate_at,
        last_broker_sync_at=candidate_at,
        last_error_classification="missing_broker_order_id",
        last_error_code="APBK0918",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0011U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    candidate.events.append(
        BrokerOrderEvent(
            event_type="submit_failed",
            normalized_status="retry_pending",
            broker_status="APBK0918",
            error_classification="transient",
            retryable=True,
            observed_at=candidate_at + timedelta(seconds=1),
        )
    )
    successor = BrokerOrder(
        plan_item=plan_item,
        broker_name="kis",
        mode="live",
        idempotency_key=f"{candidate_key}:2026-05-29",
        broker_order_id="0023934900",
        symbol="033530",
        side="sell",
        status="filled",
        quantity=58,
        submitted_at=successor_at,
        created_at=successor_at,
        request_metadata={"provider": "kis"},
        response_metadata={"account_number_masked": "****2504"},
    )
    successor.fills.append(
        ExecutionFill(
            broker_fill_id="verified-retry-successor-fill",
            symbol="033530",
            side="sell",
            quantity=58,
            price=Decimal("7540"),
            filled_at=successor_at,
        )
    )
    db.add_all([candidate, successor])
    db.commit()
    return plan_item, candidate, successor


def test_reconcile_stale_resolves_exact_filled_retry_successor_and_restores_plan_outcome() -> None:
    settings = _verified_retry_successor_settings()
    engine, db = _session(settings)
    plan_item, candidate, successor = _persist_verified_retry_successor(db)
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )

    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )
    item = dry_run["legacy_manual_review_resolution"]["items"][0]
    assert item["eligible"] is True
    assert item["resolution_context"] == "verified_filled_retry_successor"
    assert item["verified_retry_successor"]["broker_order_pk"] == successor.id

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        actor="ops-desk",
        operator_reason="retire exact legacy retry predecessor",
        manual_review_approvals={candidate.id: item["evidence_fingerprint"]},
        include_legacy_manual_review=True,
    )

    db.refresh(candidate)
    db.refresh(plan_item)
    remediation = db.scalar(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.subject_id == candidate.id
        )
    )
    assert result["legacy_manual_review_resolution"]["resolved_order_count"] == 1
    assert candidate.status == "stale_terminal_unknown"
    assert plan_item.status == "filled"
    assert remediation is not None
    assert remediation.evidence_summary["historical_outcome"] == "unknown"
    assert remediation.evidence_summary["verified_retry_successor"][
        "broker_order_pk"
    ] == successor.id
    assert broker.order_calls == []
    engine.dispose()


def test_verified_retry_successor_rejects_mismatched_failure_code() -> None:
    settings = _verified_retry_successor_settings()
    engine, db = _session(settings)
    plan_item, candidate, successor = _persist_verified_retry_successor(db)
    failure = candidate.events[0]
    failure.broker_status = "DIFFERENT_ERROR"

    result = reconciliation_service._verified_legacy_manual_review_retry_successor(
        settings=settings,
        order=candidate,
        plan_item=plan_item,
        outcome_bearing_siblings=[successor],
        fills_by_order_id={successor.id: list(successor.fills)},
        events_by_order_id={candidate.id: [failure]},
    )

    assert result is None
    engine.dispose()


@pytest.mark.parametrize(
    "evidence_drift",
    [
        "account_mask",
        "provider",
        "idempotency_date",
        "rearm_chronology",
        "fill_sum",
        "nonpositive_fill",
        "multiple_outcome_siblings",
    ],
)
def test_verified_retry_successor_rejects_lineage_drift(
    evidence_drift: str,
) -> None:
    settings = _verified_retry_successor_settings()
    engine, db = _session(settings)
    plan_item, candidate, successor = _persist_verified_retry_successor(db)
    siblings = [successor]
    if evidence_drift == "account_mask":
        successor.response_metadata = {"account_number_masked": "****9999"}
    elif evidence_drift == "provider":
        successor.request_metadata = {"provider": "alpaca"}
    elif evidence_drift == "idempotency_date":
        successor.idempotency_key = (
            f"monitoring:stop_sell:{plan_item.id}:2026-05-30"
        )
    elif evidence_drift == "rearm_chronology":
        plan_item.risk_context = {
            "monitoring_events": [
                {
                    "event": "protective_stop_rearmed",
                    "reason": "retryable_session_rejection",
                    "prior_status": "broker_order_retry_pending",
                    "recorded_at": datetime(
                        2026,
                        5,
                        29,
                        3,
                        1,
                        tzinfo=UTC,
                    ).isoformat(),
                }
            ]
        }
    elif evidence_drift == "fill_sum":
        successor.fills[0].quantity = 57
    elif evidence_drift == "nonpositive_fill":
        successor.fills[0].quantity = 59
        successor.fills.append(
            ExecutionFill(
                broker_fill_id="invalid-negative-fill",
                symbol="033530",
                side="sell",
                quantity=-1,
                price=Decimal("7540"),
                filled_at=successor.submitted_at,
            )
        )
    elif evidence_drift == "multiple_outcome_siblings":
        siblings = [successor, successor]

    result = reconciliation_service._verified_legacy_manual_review_retry_successor(
        settings=settings,
        order=candidate,
        plan_item=plan_item,
        outcome_bearing_siblings=siblings,
        fills_by_order_id={successor.id: list(successor.fills)},
        events_by_order_id={candidate.id: list(candidate.events)},
    )

    assert result is None
    db.rollback()
    engine.dispose()


def test_reconcile_stale_rejects_approval_when_job_appears_before_post_lock_reload(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'legacy-job-drift.db'}",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as setup_db:
        plan = ExecutionPlan(
            external_plan_id="legacy-job-drift-plan",
            source="test",
            mode="live",
            market_date=stale_at.date(),
        )
        plan_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="073560",
            side="sell",
            intent="stop_sell",
            status="manual_review_required",
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        order = BrokerOrder(
            plan_item=plan_item,
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-job-drift-order",
            broker_order_id=None,
            symbol="073560",
            side="sell",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0011U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )
        setup_db.add_all([plan, order])
        setup_db.commit()
        order_id = order.id
        plan_item_id = plan_item.id

    snapshot = BrokerAccountSnapshot(
        observed_at=datetime.now(UTC),
        source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        payload={
            "exchange": "KRX",
            "account_number_masked": "****2504",
        },
    )

    class JobDriftBroker(ReconciliationStubBroker):
        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            result = super().get_account_snapshot(
                access_token,
                symbols=symbols,
                reference_prices=reference_prices,
                exchange=exchange,
            )
            if len(self.account_snapshot_calls) == 2:
                with session_factory() as concurrent_db:
                    concurrent_db.add(
                        ExecutionJob(
                            execution_plan_item_id=plan_item_id,
                            job_type="broker_order_submit",
                            status="queued",
                        )
                    )
                    concurrent_db.commit()
            return result

    broker = JobDriftBroker(account_snapshot=snapshot)
    with session_factory() as db:
        dry_run = reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            dry_run=True,
            include_legacy_manual_review=True,
        )
        fingerprint = dry_run["legacy_manual_review_resolution"]["items"][0][
            "evidence_fingerprint"
        ]

        with pytest.raises(
            reconciliation_service.LegacyManualReviewApprovalConflictError,
            match="fingerprint is stale or ineligible",
        ):
            reconcile_stale_nonterminal_broker_orders(
                settings=settings,
                db=db,
                adapter=broker,
                actor="ops-desk",
                operator_reason="reject concurrent job drift",
                manual_review_approvals={order_id: fingerprint},
                include_legacy_manual_review=True,
            )
        db.rollback()
        persisted = db.get(BrokerOrder, order_id)
        assert persisted is not None
        assert persisted.status == "manual_review_required"
        assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    assert broker.account_snapshot_calls == [
        (None, "KRX"),
        (None, "KRX"),
        (None, "KRX"),
    ]
    engine.dispose()


def test_reconcile_stale_rejects_approval_when_plan_item_outcome_status_drifts() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="legacy-plan-item-outcome-drift",
            source="test",
            mode="live",
            market_date=stale_at.date(),
        )
        plan_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="073560",
            side="sell",
            intent="stop_sell",
            status="manual_review_required",
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        order = BrokerOrder(
            plan_item=plan_item,
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-plan-item-outcome-drift-order",
            broker_order_id=None,
            symbol="073560",
            side="sell",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0011U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )
        db.add_all([plan, order])
        db.commit()
        order_id = order.id
        plan_item_id = plan_item.id
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: broker,
    )
    client = TestClient(app)
    dry_run = client.post("/orders/reconcile-stale", json={"dry_run": True})
    assert dry_run.status_code == 200
    item = dry_run.json()["legacy_manual_review_resolution"]["items"][0]
    assert item["plan_item_status"] == "manual_review_required"
    fingerprint = item["evidence_fingerprint"]
    with session_factory() as db:
        persisted_plan_item = db.get(ExecutionPlanItem, plan_item_id)
        assert persisted_plan_item is not None
        persisted_plan_item.status = "filled"
        db.commit()

    response = client.post(
        "/orders/reconcile-stale",
        json={
            "actor": "ops-desk",
            "reason": "reject plan outcome drift",
            "manual_review_approvals": {order_id: fingerprint},
        },
    )

    assert response.status_code == 409
    assert "fingerprint is stale or ineligible" in response.json()["detail"]
    with session_factory() as db:
        persisted_order = db.get(BrokerOrder, order_id)
        persisted_plan_item = db.get(ExecutionPlanItem, plan_item_id)
        assert persisted_order is not None
        assert persisted_plan_item is not None
        assert persisted_order.status == "manual_review_required"
        assert persisted_plan_item.status == "filled"
        assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    engine.dispose()


def test_reconcile_stale_sqlite_apply_fails_closed_on_competing_writer(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'legacy-writer-reservation.db'}",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
        sqlite_busy_timeout_seconds=1,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as setup_db:
        order = BrokerOrder(
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-sqlite-writer-reservation",
            broker_order_id=None,
            symbol="052420",
            side="buy",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_error_classification="missing_broker_order_id",
            request_metadata={
                "provider": "kis",
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "method": "POST",
                "tr_id": "TTTC0012U",
                "account_number_masked": "****2504",
                "route": {"exchange": "KRX"},
            },
        )
        setup_db.add(order)
        setup_db.commit()
        order_id = order.id

    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    with session_factory() as dry_run_db:
        dry_run = reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=dry_run_db,
            adapter=broker,
            dry_run=True,
            include_legacy_manual_review=True,
        )
        fingerprint = dry_run["legacy_manual_review_resolution"]["items"][0][
            "evidence_fingerprint"
        ]
        dry_run_db.commit()

    with session_factory() as competing_db:
        competing_order = competing_db.get(BrokerOrder, order_id)
        assert competing_order is not None
        competing_order.last_error_message = "uncommitted competing writer"
        competing_db.flush()

        with session_factory() as apply_db:
            with pytest.raises(
                reconciliation_service.LegacyManualReviewApprovalConflictError,
                match="database is locked",
            ):
                reconcile_stale_nonterminal_broker_orders(
                    settings=settings,
                    db=apply_db,
                    adapter=broker,
                    actor="ops-desk",
                    operator_reason="require exclusive sqlite writer reservation",
                    manual_review_approvals={order_id: fingerprint},
                    include_legacy_manual_review=True,
                )
            apply_db.rollback()
        competing_db.rollback()

    with session_factory() as verify_db:
        persisted = verify_db.get(BrokerOrder, order_id)
        assert persisted is not None
        assert persisted.status == "manual_review_required"
        assert persisted.last_error_message is None
        assert verify_db.scalars(select(RuntimeRemediationEvent)).all() == []
    assert broker.account_snapshot_calls == [(None, "KRX"), (None, "KRX")]
    engine.dispose()


def test_reconcile_stale_sqlite_preserves_ordinary_repair_with_legacy_approval() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    now = datetime.now(UTC)
    ordinary = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="ordinary-stale-repair-before-legacy-reservation",
        broker_order_id=None,
        symbol="031330",
        side="buy",
        status="retry_pending",
        quantity=1,
        created_at=now - timedelta(days=5),
        submitted_at=now - timedelta(days=5),
        last_broker_sync_at=now - timedelta(days=5),
    )
    legacy = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-approval-after-ordinary-stale-repair",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=1,
        created_at=now - timedelta(days=45),
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    db.add_all([ordinary, legacy])
    db.commit()
    broker = ReconciliationStubBroker(
        account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )
    )
    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )
    fingerprint = dry_run["legacy_manual_review_resolution"]["items"][0][
        "evidence_fingerprint"
    ]

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        actor="ops-desk",
        operator_reason="preserve ordinary and legacy repairs atomically",
        manual_review_approvals={legacy.id: fingerprint},
        include_legacy_manual_review=True,
    )

    db.refresh(ordinary)
    db.refresh(legacy)
    ordinary_repairs = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == ordinary.id,
            BrokerOrderEvent.event_type == "stale_order_repair",
        )
    ).all()
    assert result["repaired_order_count"] == 1
    assert ordinary.status == "manual_review_required"
    assert ordinary.last_error_classification == "missing_broker_order_id"
    assert len(ordinary_repairs) == 1
    assert legacy.status == "stale_terminal_unknown"
    engine.dispose()


def test_legacy_manual_review_locked_reload_refreshes_relationship_drift(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'legacy-relationship-drift.db'}",
        runtime_mode="live",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as db:
        order = BrokerOrder(
            broker_name="kis",
            mode="live",
            idempotency_key="legacy-manual-review-relationship-drift",
            broker_order_id=None,
            symbol="069960",
            side="sell",
            status="manual_review_required",
            quantity=1,
            created_at=stale_at,
            last_error_classification="missing_broker_order_id",
        )
        db.add(order)
        db.commit()
        order_id = order.id

    cutoff = datetime.now(UTC) - timedelta(days=30)
    with session_factory() as primary_db:
        initially_loaded = reconciliation_service._load_legacy_manual_review_orders(
            settings=settings,
            db=primary_db,
            broker_name="kis",
            stale_before=cutoff,
            symbols=None,
            for_update=False,
        )
        assert len(initially_loaded) == 1
        assert initially_loaded[0].execution_failure is None
        with session_factory() as concurrent_db:
            concurrent_db.add(
                ExecutionFailure(
                    mode="live",
                    broker_order_pk=order_id,
                    broker_name="kis",
                    idempotency_key="legacy-manual-review-relationship-drift",
                    symbol="069960",
                    side="sell",
                    requested_quantity=1,
                    classification="legacy_submission_unknown",
                    broker_error_message="concurrent failure evidence",
                    retryable=False,
                    status="open",
                    first_failed_at=stale_at,
                    latest_failed_at=stale_at,
                )
            )
            concurrent_db.commit()

        locked_reload = reconciliation_service._load_legacy_manual_review_orders(
            settings=settings,
            db=primary_db,
            broker_name="kis",
            stale_before=cutoff,
            symbols=None,
            for_update=True,
        )

        assert locked_reload[0] is initially_loaded[0]
        assert locked_reload[0].execution_failure is not None
        assert locked_reload[0].execution_failure.status == "open"
    engine.dispose()


def test_reconcile_stale_rejects_approval_when_post_lock_broker_truth_drifts() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12342504",
        kis_account_product_code="01",
        stale_broker_order_reconciliation_threshold_seconds=3600,
    )
    engine, db = _session(settings)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    order = BrokerOrder(
        broker_name="kis",
        mode="live",
        idempotency_key="legacy-manual-review-broker-drift",
        broker_order_id=None,
        symbol="052420",
        side="buy",
        status="manual_review_required",
        quantity=2,
        created_at=stale_at,
        last_error_classification="missing_broker_order_id",
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "method": "POST",
            "tr_id": "TTTC0012U",
            "account_number_masked": "****2504",
            "route": {"exchange": "KRX"},
        },
    )
    db.add(order)
    db.commit()

    def snapshot(*, with_holding: bool) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            observed_at=datetime.now(UTC),
            source="kis:account_snapshot",
            holdings=(
                [
                    BrokerAccountHolding(
                        symbol="052420",
                        quantity=2,
                        average_price=Decimal("5000"),
                        sellable_quantity=2,
                    )
                ]
                if with_holding
                else []
            ),
            holdings_complete=True,
            open_orders_complete=True,
            payload={
                "exchange": "KRX",
                "account_number_masked": "****2504",
            },
        )

    class DriftingSnapshotBroker(ReconciliationStubBroker):
        def __init__(self) -> None:
            super().__init__(account_snapshot=snapshot(with_holding=False))
            self.snapshots = [
                snapshot(with_holding=False),
                snapshot(with_holding=False),
                snapshot(with_holding=True),
            ]

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            del reference_prices
            self.account_snapshot_calls.append((symbols, exchange))
            return self.snapshots[len(self.account_snapshot_calls) - 1]

    broker = DriftingSnapshotBroker()
    dry_run = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        dry_run=True,
        include_legacy_manual_review=True,
    )
    fingerprint = dry_run["legacy_manual_review_resolution"]["items"][0][
        "evidence_fingerprint"
    ]

    with pytest.raises(
        reconciliation_service.LegacyManualReviewApprovalConflictError,
        match="fingerprint is stale or ineligible",
    ):
        reconcile_stale_nonterminal_broker_orders(
            settings=settings,
            db=db,
            adapter=broker,
            actor="ops-desk",
            operator_reason="approve reviewed broker absence",
            manual_review_approvals={order.id: fingerprint},
            include_legacy_manual_review=True,
        )

    db.rollback()
    db.refresh(order)
    assert order.status == "manual_review_required"
    assert broker.account_snapshot_calls == [
        (None, "KRX"),
        (None, "KRX"),
        (None, "KRX"),
    ]
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


def test_stale_reconciliation_schema_normalizes_and_validates_manual_review_approvals() -> None:
    command = StaleBrokerOrderReconciliationRequest.model_validate(
        {"manual_review_approvals": {"  order-id  ": "A" * 64}}
    )

    assert command.manual_review_approvals == {"order-id": "a" * 64}
    with pytest.raises(ValidationError, match="64-character hexadecimal"):
        StaleBrokerOrderReconciliationRequest.model_validate(
            {"manual_review_approvals": {"order-id": "not-a-fingerprint"}}
        )
    for prefixed_fingerprint in ("+" + "a" * 63, "0x" + "a" * 62):
        with pytest.raises(ValidationError, match="64-character hexadecimal"):
            StaleBrokerOrderReconciliationRequest.model_validate(
                {
                    "manual_review_approvals": {
                        "order-id": prefixed_fingerprint
                    }
                }
            )
    with pytest.raises(ValidationError, match="order id must not be empty"):
        StaleBrokerOrderReconciliationRequest.model_validate(
            {"manual_review_approvals": {"   ": "a" * 64}}
        )


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
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    for index, (symbol, status) in enumerate(statuses_by_symbol.items()):
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=index,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            quantity_policy={"kind": "shares", "quantity": 1},
            provider="kis",
            venue="KIS",
            asset_class="stock",
            risk_context={"exchange": "KRX"},
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
            venue="KIS",
            asset_class="stock",
            submitted_at=observed_at,
            last_broker_sync_at=observed_at,
            request_metadata={
                "exchange": "KRX",
                "execution_scope": execution_scope_payload(
                    identity=identity,
                    venue="KIS",
                    asset_class="stock",
                ),
            },
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
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    order_scope = execution_scope_payload(
        identity=identity,
        venue="KRX",
        asset_class="stock",
    )
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
            venue="KRX",
            asset_class="stock",
            submitted_at=stale_reconciled_at + timedelta(minutes=5),
            request_metadata={"execution_scope": order_scope},
        )
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="319660",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=stale_reconciled_at,
            raw_payload=payload_with_position_identity(None, identity=identity),
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
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="000660",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=stale_reconciled_at,
            raw_payload=payload_with_position_identity(
                {"source": "broker"},
                identity=identity,
            ),
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
    order_scope = execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        venue="KRX",
        asset_class="stock",
    )
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
                venue="KRX",
                asset_class="stock",
                submitted_at=stale_observed_at,
                last_broker_sync_at=stale_observed_at,
                request_metadata={"execution_scope": order_scope},
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
                venue="KRX",
                asset_class="stock",
                submitted_at=stale_observed_at,
                last_broker_sync_at=stale_observed_at,
                request_metadata={"execution_scope": order_scope},
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
                venue="KRX",
                asset_class="stock",
                submitted_at=recent_observed_at,
                last_broker_sync_at=recent_observed_at,
                request_metadata={"execution_scope": order_scope},
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
                venue="KRX",
                asset_class="stock",
                submitted_at=recent_observed_at,
                last_broker_sync_at=recent_observed_at,
                last_error_classification="unknown_status",
                last_error_code="unknown",
                last_error_message="unrecognized broker status",
                request_metadata={"execution_scope": order_scope},
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
    order_scope = execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        venue="KRX",
        asset_class="stock",
    )
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
            venue="KRX",
            asset_class="stock",
            limit_price=Decimal("70000"),
            submitted_at=None,
            last_broker_sync_at=observed_at,
            last_error_classification="submit_outcome_unknown",
            last_error_code="SUBMIT_OUTCOME_UNKNOWN",
            last_error_message="manual broker reconciliation is required",
            last_error_retryable=False,
            request_metadata={"execution_scope": order_scope},
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
    order_scope = execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        venue="KRX",
        asset_class="stock",
    )
    missing_fill_order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-missing-fill",
        broker_order_id="missing-fill-1",
        symbol="105560",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        submitted_at=filled_at,
        request_metadata={"execution_scope": dict(order_scope)},
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
        venue="KRX",
        asset_class="stock",
        submitted_at=filled_at,
        request_metadata={"execution_scope": dict(order_scope)},
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
        venue="KRX",
        asset_class="stock",
        submitted_at=filled_at,
        request_metadata={"execution_scope": dict(order_scope)},
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
    order_scope = execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        venue="KRX",
        asset_class="stock",
    )
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="filled-order-two-duplicate-fill-groups",
        broker_order_id="duplicate-fill-shared-order",
        symbol="282330",
        side="buy",
        status="filled",
        quantity=4,
        venue="KRX",
        asset_class="stock",
        submitted_at=filled_at,
        request_metadata={"execution_scope": order_scope},
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
    order_scope = execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        venue="KRX",
        asset_class="stock",
    )
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
            venue="KRX",
            asset_class="stock",
            submitted_at=submitted_at + timedelta(minutes=index),
            request_metadata={"execution_scope": dict(order_scope)},
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
        venue="KRX",
        asset_class="stock",
        submitted_at=submitted_at - timedelta(days=1),
        request_metadata={"execution_scope": dict(order_scope)},
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
        return JsonHttpResponse(
            status_code=200,
            headers={"tr_cont": "D"},
            payload=self.payload,
        )

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
                        "excg_id_dvsn_Cd": "KRX",
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

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0033196600",
        submitted_at=datetime(2026, 5, 13, tzinfo=UTC),
        expected_symbol="005930",
    )

    assert payload is not None
    assert payload["status"] == "filled"
    assert payload["filled_quantity"] == 2
    assert payload["filled_at"] == payload["observed_at"]
    assert payload["fill_time_precision"] == "observed_upper_bound"
    assert payload["fill_time_source"] == "reconciliation_observed_at"
    assert payload["payload"]["order"]["ord_tmd"] == "090501"
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
                        "excg_id_dvsn_Cd": "KRX",
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

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0033196600",
        submitted_at=datetime(2026, 5, 13, tzinfo=UTC),
        expected_symbol="005930",
    )

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
                        "excg_id_dvsn_Cd": "KRX",
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

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0007266500",
        submitted_at=datetime(2026, 6, 8, tzinfo=UTC),
        expected_symbol="000815",
    )

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
    submitted_at = datetime.now(UTC)
    submitted_krx_date = (submitted_at + timedelta(hours=9)).strftime("%Y%m%d")
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
                    "excg_id_dvsn_Cd": "KRX",
                    "sll_buy_dvsn_cd": "02",
                    "ord_qty": "1",
                    "tot_ccld_qty": "0",
                    "rmn_qty": "1",
                    "avg_pric": "0",
                    "ord_dt": submitted_krx_date,
                    "ord_tmd": "090501",
                    "ord_st_dvsn_cd_name": "미체결",
                }
            ],
        }
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=http_client)

    payload = adapter.reconcile_order(
        "top-secret-token",
        "0033196600",
        submitted_at=submitted_at,
        expected_symbol="005930",
    )

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
                        "excg_id_dvsn_Cd": "NXT",
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
        submitted_at=datetime(2026, 6, 2, tzinfo=UTC),
        expected_symbol="034730",
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
                        "excg_id_dvsn_Cd": "KRX",
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
        expected_symbol="105560",
    )

    assert payload is not None
    assert payload["status"] == "filled"
    assert http_client.calls[0]["params"]["INQR_STRT_DT"] == "20260723"
    assert http_client.calls[0]["params"]["INQR_END_DT"] == "20260723"


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
    order.request_metadata = {**(order.request_metadata or {}), "source": "test"}
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

    assert result["status_code"] == 409
    assert result["status"] == "manual_review_required"
    assert result["scope_classification"] == "invalid_execution_scope"
    assert broker.reconcile_calls == []
    assert result["message"] == "canonical_execution_scope_conflicts_with_order"
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
    assert first["message"].startswith("broker_error_details_redacted[")
    assert second["message"] == first["message"]
    assert len(events) == 1
    assert events[0].payload is not None
    assert events[0].payload["request_metadata"]["tr_id"] == "VTTC0081R"
    assert order.last_error_message == first["message"]
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
    assert first["message"].startswith("broker_error_details_redacted[")
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
        plan_id="plan-retry-rerun-idempotent",
        side="sell",
        intent="stop_sell",
        symbol="024110",
        quantity=20,
        broker_order_id="retry-rerun-source-1",
    )
    item.risk_context, item.quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
    )
    db.add_all([plan, order])
    db.flush()
    canonical_source_command = PlaceOrderRequest(
        idempotency_key=order.idempotency_key,
        execution_plan_item_id=item.id,
        provider="kis",
        venue="KRX",
        asset_class="stock",
        symbol=order.symbol,
        side=order.side,
        quantity=order.quantity,
        order_type="limit",
        limit_price=order.limit_price,
        time_in_force="day",
        exchange="KRX",
    )
    order.request_metadata = {
        **(order.request_metadata or {}),
        "provider": "kis",
        "venue": "KRX",
        "asset_class": "stock",
        "exchange": "KRX",
        "time_in_force": "day",
        "sizing": {
            "quantity": order.quantity,
            "base_quantity": None,
            "quote_notional": None,
            "limit_price": str(order.limit_price),
            "quantity_precision": None,
            "price_precision": None,
        },
    }
    order.events.append(
        BrokerOrderEvent(
            event_type="submit_requested",
            event_source="submission",
            normalized_status="submission_pending",
            request_payload=canonical_source_command.model_dump(mode="json"),
            payload={"provider": "kis"},
            observed_at=OBSERVED_AT,
        )
    )
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
