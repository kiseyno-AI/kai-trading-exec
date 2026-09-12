from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.broker.base import (
    BrokerConfig,
    BrokerReconnectHandle,
    BrokerTokenGrant,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
)
from app.services.monitoring import _reserve_existing_exit_orders
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
)
from app.services.reconciliation import (
    reconcile_broker_order,
    reconcile_pending_broker_orders,
    reconcile_stale_nonterminal_broker_orders,
)


OBSERVED_AT = datetime(2026, 9, 10, 1, tzinfo=UTC)


class ScopeReconciliationBroker:
    provider = "kis"

    def __init__(self, *, mode: str = "paper", symbol: str = "005930") -> None:
        self.mode = mode
        self.symbol = symbol
        self.authenticate_calls = 0
        self.reconcile_calls: list[tuple[str, str]] = []

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider=self.provider,
            mode=self.mode,
            configured=True,
            api_base_url="https://scope-test.invalid",
        )

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        return BrokerTokenGrant(
            access_token="scope-test-token",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=False, strategy="none")

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
        submitted_at: datetime | None = None,
    ) -> dict[str, object]:
        assert access_token
        self.reconcile_calls.append((broker_order_id, exchange))
        return {
            "broker_order_id": broker_order_id,
            "status": "filled",
            "filled_quantity": 1,
            "average_fill_price": "71000",
            "fills": [
                {
                    "broker_fill_id": f"{broker_order_id}:fill",
                    "symbol": self.symbol,
                    "side": "buy",
                    "quantity": 1,
                    "price": "71000",
                    "filled_at": OBSERVED_AT.isoformat(),
                }
            ],
            "observed_at": OBSERVED_AT.isoformat(),
        }


class AlpacaScopeReconciliationBroker(ScopeReconciliationBroker):
    provider = "alpaca"

    def __init__(self, *, broker_account_id: str, mode: str = "paper") -> None:
        super().__init__(mode=mode)
        self.broker_account_id = broker_account_id
        self.account_identity_calls = 0

    def get_broker_account_id(self, access_token: str) -> str:
        assert access_token
        self.account_identity_calls += 1
        return self.broker_account_id


def _settings(*, account_number: str, mode: str = "paper") -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=mode,
        kis_app_key="scope-test-key",
        kis_app_secret="scope-test-secret-at-least-32-bytes",
        kis_account_number=account_number,
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="scope-hmac-secret-at-least-32-bytes",
    )


def _alpaca_settings(*, mode: str = "paper") -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=mode,
        alpaca_api_key="alpaca-scope-test-key",
        alpaca_api_secret="alpaca-scope-test-secret",
        execution_position_account_scope="operator-label-must-not-be-authority",
        account_drawdown_recovery_scope_secret="scope-hmac-secret-at-least-32-bytes",
    )


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)()


def _scoped_order(
    *,
    settings: Settings,
    broker_order_id: str,
    idempotency_key: str,
    exchange: str = "KRX",
    status: str = "submitted",
    symbol: str = "005930",
    venue: str | None = None,
    asset_class: str = "stock",
    include_route_hints: bool = True,
) -> BrokerOrder:
    resolved_venue = venue or exchange
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange=exchange,
    )
    return BrokerOrder(
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=idempotency_key,
        broker_order_id=broker_order_id,
        symbol=symbol,
        side="buy",
        status=status,
        quantity=1,
        venue=resolved_venue,
        asset_class=asset_class,
        submitted_at=OBSERVED_AT,
        request_metadata={
            **(
                {"exchange": exchange, "route": {"exchange": exchange}}
                if include_route_hints
                else {}
            ),
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue=resolved_venue,
                asset_class=asset_class,
            ),
        },
    )


def test_direct_reconcile_rejects_foreign_kis_account_before_broker_lookup() -> None:
    account_a = _settings(account_number="11111111")
    account_b = _settings(account_number="22222222")
    engine, db = _session(account_b)
    order = _scoped_order(
        settings=account_a,
        broker_order_id="same-broker-order-id",
        idempotency_key="account-a-order",
    )
    db.add(order)
    db.commit()
    broker = ScopeReconciliationBroker()

    result = reconcile_broker_order(
        settings=account_b,
        db=db,
        adapter=broker,
        order=order,
    )
    repeated = reconcile_broker_order(
        settings=account_b,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["scope_classification"] == "foreign_execution_scope"
    assert repeated["scope_classification"] == "foreign_execution_scope"
    assert broker.authenticate_calls == 0
    assert broker.reconcile_calls == []
    db.refresh(order)
    assert order.status == "submitted"
    assert db.scalar(
        select(func.count()).select_from(ExecutionFill).where(
            ExecutionFill.broker_order_pk == order.id
        )
    ) == 0
    event = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_scope_rejected",
        )
    )
    assert event is not None
    assert event.error_classification == "foreign_execution_scope"
    assert db.scalar(
        select(func.count()).select_from(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_scope_rejected",
        )
    ) == 1
    engine.dispose()


def test_direct_reconcile_quarantines_unscoped_order_without_broker_lookup() -> None:
    settings = _settings(account_number="22222222")
    engine, db = _session(settings)
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-unscoped-order",
        broker_order_id="legacy-order-id",
        symbol="005930",
        side="buy",
        status="submitted",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        submitted_at=OBSERVED_AT,
        request_metadata={"exchange": "KRX", "route": {"exchange": "KRX"}},
    )
    db.add(order)
    db.commit()
    broker = ScopeReconciliationBroker()

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["scope_classification"] == "invalid_execution_scope"
    assert result["status"] == "manual_review_required"
    assert result["status_changed"] is True
    assert broker.authenticate_calls == 0
    assert broker.reconcile_calls == []
    db.refresh(order)
    assert order.status == "manual_review_required"
    assert order.last_error_classification == "invalid_execution_scope"
    assert order.last_error_retryable is False
    assert db.scalar(
        select(func.count()).select_from(ExecutionFill).where(
            ExecutionFill.broker_order_pk == order.id
        )
    ) == 0
    event = db.scalar(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_scope_rejected",
        )
    )
    assert event is not None
    assert event.error_classification == "invalid_execution_scope"
    engine.dispose()


def test_alpaca_reconcile_rechecks_authenticated_account_before_broker_lookup() -> None:
    settings = _alpaca_settings()
    engine, db = _session(settings)
    account_a_identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="NASD",
        broker_account_id="alpaca-account-a",
    )
    order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="alpaca-account-a-order",
        broker_order_id="same-alpaca-broker-order-id",
        symbol="AAPL",
        side="buy",
        status="submitted",
        quantity=1,
        venue="NASD",
        asset_class="us_equity",
        submitted_at=OBSERVED_AT,
        request_metadata={
            "exchange": "NASD",
            "route": {"exchange": "NASD"},
            "execution_scope": execution_scope_payload(
                identity=account_a_identity,
                venue="NASD",
                asset_class="us_equity",
            ),
        },
    )
    db.add(order)
    db.commit()
    broker = AlpacaScopeReconciliationBroker(
        broker_account_id="alpaca-account-b"
    )

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["scope_classification"] == "foreign_execution_scope"
    assert broker.authenticate_calls == 1
    assert broker.account_identity_calls == 1
    assert broker.reconcile_calls == []
    db.refresh(order)
    assert order.status == "submitted"
    assert db.scalar(
        select(func.count()).select_from(ExecutionFill).where(
            ExecutionFill.broker_order_pk == order.id
        )
    ) == 0
    engine.dispose()


def test_alpaca_reconcile_preserves_order_when_authenticated_identity_is_unavailable() -> None:
    settings = _alpaca_settings()
    engine, db = _session(settings)
    persisted_identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="NASD",
        broker_account_id="alpaca-account-a",
    )
    order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="alpaca-missing-current-identity",
        broker_order_id="alpaca-missing-current-identity",
        symbol="AAPL",
        side="buy",
        status="submitted",
        quantity=1,
        venue="NASD",
        asset_class="us_equity",
        submitted_at=OBSERVED_AT,
        request_metadata={
            "exchange": "NASD",
            "execution_scope": execution_scope_payload(
                identity=persisted_identity,
                venue="NASD",
                asset_class="us_equity",
            ),
        },
    )
    db.add(order)
    db.commit()
    broker = AlpacaScopeReconciliationBroker(broker_account_id="")

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["scope_classification"] == "authenticated_execution_scope_unavailable"
    assert broker.authenticate_calls == 1
    assert broker.account_identity_calls == 1
    assert broker.reconcile_calls == []
    db.refresh(order)
    assert order.status == "submitted"
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0
    engine.dispose()


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_matching_kis_scope_reconciles_us_fill_from_canonical_exchange(
    mode: str,
) -> None:
    settings = _settings(account_number="22222222", mode=mode)
    engine, db = _session(settings)
    order = _scoped_order(
        settings=settings,
        broker_order_id=f"matching-{mode}-order",
        idempotency_key=f"matching-{mode}-order",
        exchange="NASD",
        symbol="AAPL",
        venue="KIS-US",
        asset_class="us_equity",
        include_route_hints=False,
    )
    db.add(order)
    db.commit()
    broker = ScopeReconciliationBroker(mode=mode, symbol="AAPL")

    result = reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )

    assert result["status"] == "filled"
    assert result["fills_added"] == 1
    assert broker.reconcile_calls == [(f"matching-{mode}-order", "NASD")]
    db.refresh(order)
    assert order.status == "filled"
    fill = db.scalar(
        select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id)
    )
    assert fill is not None
    assert fill.symbol == "AAPL"
    assert fill.quantity == 1
    engine.dispose()


def test_pending_reconciliation_isolates_same_broker_id_by_account_scope() -> None:
    account_a = _settings(account_number="11111111")
    account_b = _settings(account_number="22222222")
    engine, db = _session(account_b)
    foreign = _scoped_order(
        settings=account_a,
        broker_order_id="shared-kis-order-id",
        idempotency_key="pending-account-a",
    )
    matching = _scoped_order(
        settings=account_b,
        broker_order_id="shared-kis-order-id",
        idempotency_key="pending-account-b",
    )
    legacy = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="pending-legacy",
        broker_order_id="legacy-pending-id",
        symbol="005930",
        side="buy",
        status="submitted",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        submitted_at=OBSERVED_AT,
        request_metadata={"exchange": "KRX"},
    )
    db.add_all([foreign, legacy, matching])
    db.commit()
    broker = ScopeReconciliationBroker()

    result = reconcile_pending_broker_orders(
        settings=account_b,
        db=db,
        adapter=broker,
    )

    assert result["candidate_order_count"] == 3
    assert result["scope_rejected_order_count"] == 2
    assert result["scope_quarantined_order_count"] == 1
    assert broker.reconcile_calls == [("shared-kis-order-id", "KRX")]
    db.refresh(foreign)
    db.refresh(legacy)
    db.refresh(matching)
    assert foreign.status == "submitted"
    assert legacy.status == "manual_review_required"
    assert matching.status == "filled"
    assert db.scalar(
        select(func.count()).select_from(ExecutionFill).where(
            ExecutionFill.broker_order_pk == foreign.id
        )
    ) == 0
    assert db.scalar(
        select(func.count()).select_from(ExecutionFill).where(
            ExecutionFill.broker_order_pk == matching.id
        )
    ) == 1
    engine.dispose()


def test_stale_reconciliation_dry_run_surfaces_scope_actions_without_mutation() -> None:
    account_a = _settings(account_number="11111111")
    account_b = _settings(account_number="22222222")
    engine, db = _session(account_b)
    foreign = _scoped_order(
        settings=account_a,
        broker_order_id="stale-foreign-id",
        idempotency_key="stale-account-a",
    )
    matching = _scoped_order(
        settings=account_b,
        broker_order_id="stale-matching-id",
        idempotency_key="stale-account-b",
    )
    legacy = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="stale-legacy",
        broker_order_id="stale-legacy-id",
        symbol="005930",
        side="buy",
        status="submitted",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        submitted_at=OBSERVED_AT,
        request_metadata={"exchange": "KRX"},
    )
    db.add_all([foreign, legacy, matching])
    db.commit()
    broker = ScopeReconciliationBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=account_b,
        db=db,
        adapter=broker,
        older_than_seconds=1,
        dry_run=True,
    )

    assert result["candidate_order_count"] == 3
    assert result["counts"]["live"] == 1
    assert result["counts"]["failed-to-reconcile"] == 1
    assert result["counts"]["manual-review-required"] == 1
    items = {item["idempotency_key"]: item for item in result["items"]}
    assert items["stale-account-a"]["reason"] == "authenticated_account_scope_mismatch"
    assert items["stale-account-a"]["status_after"] == "submitted"
    assert items["stale-legacy"]["status_after"] == "manual_review_required"
    assert items["stale-legacy"]["repair_action"] == "would_quarantine"
    assert broker.authenticate_calls == 0
    assert broker.reconcile_calls == []
    for order in (foreign, legacy, matching):
        db.refresh(order)
        assert order.status == "submitted"
    assert db.scalar(select(func.count()).select_from(BrokerOrderEvent)) == 0
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0
    engine.dispose()


def test_stale_reconciliation_quarantines_legacy_and_never_queries_foreign() -> None:
    account_a = _settings(account_number="11111111")
    account_b = _settings(account_number="22222222")
    engine, db = _session(account_b)
    foreign = _scoped_order(
        settings=account_a,
        broker_order_id="stale-live-foreign-id",
        idempotency_key="stale-live-account-a",
    )
    legacy = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="stale-live-legacy",
        broker_order_id="stale-live-legacy-id",
        symbol="005930",
        side="buy",
        status="submitted",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        submitted_at=OBSERVED_AT,
        request_metadata={"exchange": "KRX"},
    )
    db.add_all([foreign, legacy])
    db.commit()
    broker = ScopeReconciliationBroker()

    result = reconcile_stale_nonterminal_broker_orders(
        settings=account_b,
        db=db,
        adapter=broker,
        older_than_seconds=1,
        dry_run=False,
    )

    assert result["counts"]["failed-to-reconcile"] == 1
    assert result["counts"]["manual-review-required"] == 1
    items = {item["idempotency_key"]: item for item in result["items"]}
    assert items["stale-live-account-a"]["repair_action"] == "none"
    assert items["stale-live-legacy"]["repair_action"] == "quarantined"
    assert broker.authenticate_calls == 0
    assert broker.reconcile_calls == []
    db.refresh(foreign)
    db.refresh(legacy)
    assert foreign.status == "submitted"
    assert legacy.status == "manual_review_required"
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0
    engine.dispose()


def test_stale_alpaca_dry_run_does_not_claim_unverified_account_match() -> None:
    settings = _alpaca_settings()
    engine, db = _session(settings)
    identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="NASD",
        broker_account_id="alpaca-account-a",
    )
    order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="stale-alpaca-dry-run",
        broker_order_id="stale-alpaca-dry-run",
        symbol="AAPL",
        side="buy",
        status="submitted",
        quantity=1,
        venue="NASD",
        asset_class="us_equity",
        submitted_at=OBSERVED_AT,
        request_metadata={
            "exchange": "NASD",
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue="NASD",
                asset_class="us_equity",
            ),
        },
    )
    db.add(order)
    db.commit()
    broker = AlpacaScopeReconciliationBroker(
        broker_account_id="alpaca-account-a"
    )

    result = reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=broker,
        older_than_seconds=1,
        dry_run=True,
    )

    assert result["counts"]["live"] == 0
    assert result["counts"]["failed-to-reconcile"] == 1
    assert result["items"][0]["reason"] == (
        "dry_run_cannot_verify_authenticated_account_identity"
    )
    assert broker.authenticate_calls == 0
    assert broker.account_identity_calls == 0
    assert broker.reconcile_calls == []
    db.refresh(order)
    assert order.status == "submitted"
    assert db.scalar(select(func.count()).select_from(BrokerOrderEvent)) == 0
    engine.dispose()


def test_quarantined_legacy_sell_remains_a_replacement_quantity_blocker() -> None:
    settings = _settings(account_number="22222222")
    engine, db = _session(settings)
    plan = ExecutionPlan(
        external_plan_id="legacy-sell-plan",
        source="legacy-test",
        mode="paper",
        status="accepted",
        accepted_at=OBSERVED_AT,
    )
    item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="broker_order_submitted",
        quantity_policy={"kind": "shares", "quantity": 2},
        trigger_price=Decimal("68000"),
    )
    order = BrokerOrder(
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-unscoped-sell",
        broker_order_id="legacy-unscoped-sell",
        symbol="005930",
        side="sell",
        status="submitted",
        quantity=2,
        venue="KRX",
        asset_class="stock",
        submitted_at=OBSERVED_AT,
        request_metadata={"exchange": "KRX"},
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=2,
        average_price=Decimal("71000"),
        updated_from="test",
        raw_payload=payload_with_position_identity(None, identity=identity),
    )
    db.add_all([plan, order, position])
    db.commit()
    broker = ScopeReconciliationBroker()

    reconcile_broker_order(
        settings=settings,
        db=db,
        adapter=broker,
        order=order,
    )
    remaining = {"005930": 2}
    _reserve_existing_exit_orders(
        settings=settings,
        db=db,
        remaining_quantities_by_symbol=remaining,
        positions_by_symbol={"005930": position},
        rearmed_plan_item_ids=set(),
    )

    db.refresh(order)
    db.refresh(item)
    assert order.status == "manual_review_required"
    assert item.status == "broker_order_failed"
    assert remaining == {}
    assert broker.reconcile_calls == []
    engine.dispose()
