from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from app.broker.base import (
    BrokerAccountPosition,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, BrokerSession, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.portfolio_risk import build_portfolio_risk_snapshot


def _session(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return engine, session_factory, session_factory()


class PortfolioRiskStubAdapter:
    provider = "kis"

    def __init__(
        self,
        *,
        configured: bool = True,
        snapshot: BrokerAccountSnapshot | None = None,
        snapshot_error: Exception | None = None,
    ) -> None:
        self.configured = configured
        self.snapshot = snapshot
        self.snapshot_error = snapshot_error
        self.snapshot_calls = 0

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider="kis",
            mode="paper",
            configured=self.configured,
            api_base_url=None,
            reason=None if self.configured else "missing test credentials",
        )

    def authenticate(self) -> BrokerTokenGrant:
        raise AssertionError("portfolio risk snapshot must not authenticate")

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        raise AssertionError("portfolio risk snapshot must not request realtime approval")

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        raise AssertionError("portfolio risk snapshot must not revoke")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        raise AssertionError("portfolio risk snapshot must not place orders")

    def get_account_snapshot(self, access_token: str, **_kwargs: object) -> BrokerAccountSnapshot:
        assert access_token == "stored-token"
        self.snapshot_calls += 1
        if self.snapshot_error is not None:
            raise self.snapshot_error
        return self.snapshot or BrokerAccountSnapshot(
            available_cash=Decimal("5000000"),
            observed_at=datetime(2026, 6, 20, tzinfo=UTC),
            source="stub:account_snapshot",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(self, access_token: str, broker_order_id: str, *, exchange: str = "KRX") -> dict[str, object] | None:
        raise AssertionError("portfolio risk snapshot must not reconcile orders")

    def build_headers(self, access_token: str, **_kwargs: object) -> dict[str, str]:
        return {}


def _add_broker_session(db, *, expires_at: datetime | None = None) -> None:
    db.add(
        BrokerSession(
            id="kis:paper",
            provider="kis",
            mode="paper",
            access_token="stored-token",
            token_type="Bearer",
            access_token_expires_at=expires_at or datetime(2026, 6, 21, tzinfo=UTC),
        )
    )


def _add_plan_with_stop(db, *, symbol: str, stop_price: str = "90000.00") -> None:
    plan = ExecutionPlan(
        external_plan_id=f"plan-{symbol}",
        source="test",
        mode="paper",
        status="accepted",
    )
    item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol=symbol,
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        trigger_price=Decimal(stop_price),
        risk_context={"stop_price": stop_price},
    )
    db.add_all([plan, item])


def test_portfolio_risk_snapshot_reports_holdings_and_stored_stop_risk() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100000.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=5,
                average_price=Decimal("50000.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
        ]
    )
    _add_plan_with_stop(db, symbol="005930", stop_price="90000.00")
    db.commit()

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(),
        now=now,
    )

    assert payload["source_status"] == "partial"
    assert payload["account"]["total_market_value"] == "1250000.0000"
    assert payload["holdings"]["open_count"] == 2
    assert payload["holdings"]["top_by_market_value"][0]["symbol"] == "005930"
    assert payload["holdings"]["top_by_market_value"][0]["allocation_pct"] == "80.0000"
    stop_rows = {row["symbol"]: row for row in payload["estimated_stop_risk"]["positions"]}
    assert stop_rows["005930"]["estimated_risk"] == "100000.0000"
    assert stop_rows["005930"]["metadata_status"] == "present"
    engine.dispose()


def test_portfolio_risk_snapshot_combines_holdings_and_open_buy_order_exposure() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="005930",
            quantity=10,
            average_price=Decimal("100000.00"),
            updated_from="reconciliation",
            last_reconciled_at=now,
        )
    )
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="buy-005930",
            broker_order_id="broker-buy-005930",
            symbol="005930",
            side="buy",
            status="open",
            quantity=2,
            limit_price=Decimal("110000.00"),
            last_broker_sync_at=now,
        )
    )
    db.commit()

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(),
        now=now,
    )

    exposure = payload["exposures"]["by_symbol"][0]
    assert exposure["symbol"] == "005930"
    assert exposure["holding_value"] == "1000000.0000"
    assert exposure["open_buy_notional"] == "220000.0000"
    assert exposure["total_exposure"] == "1220000.0000"
    warning_codes = {warning["code"] for warning in payload["warnings"]["concentration"]}
    assert "holding_plus_open_buy_order" in warning_codes
    engine.dispose()


def test_portfolio_risk_snapshot_returns_unavailable_without_broker_or_local_data() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(configured=False),
        now=datetime(2026, 6, 20, 3, 0, tzinfo=UTC),
    )

    assert payload["source_status"] == "unavailable"
    assert "missing test credentials" in payload["source_reason"]
    assert payload["holdings"]["open_count"] == 0
    engine.dispose()


def test_portfolio_risk_snapshot_warns_on_missing_stop_metadata_and_concentration() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=90,
                average_price=Decimal("10000.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=10,
                average_price=Decimal("10000.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
        ]
    )
    db.commit()

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(),
        now=now,
    )

    warning_codes = {warning["code"] for warning in payload["warnings"]["concentration"]}
    assert "top_symbol_concentration" in warning_codes
    assert "top_three_concentration" in warning_codes
    assert "missing_stop_metadata" in warning_codes
    engine.dispose()


def test_portfolio_risk_snapshot_includes_upbit_paper_base_quantity_position() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="KRW-BTC",
            quantity=0,
            base_quantity=Decimal("0.125"),
            average_price=Decimal("80000000.00"),
            updated_from="upbit_paper",
            last_reconciled_at=now,
        )
    )
    db.commit()

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(),
        now=now,
    )

    assert payload["holdings"]["open_count"] == 1
    holding = payload["holdings"]["top_by_market_value"][0]
    assert holding["symbol"] == "KRW-BTC"
    assert holding["base_quantity"] == "0.1250"
    assert holding["market_value"] == "10000000.0000"
    engine.dispose()


def test_portfolio_risk_endpoint_returns_partial_not_500_for_account_snapshot_gap() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, session_factory, db = _session(settings)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="005930",
            quantity=1,
            average_price=Decimal("70000.00"),
            updated_from="reconciliation",
            last_reconciled_at=datetime.now(UTC),
        )
    )
    db.commit()
    db.close()
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: PortfolioRiskStubAdapter(snapshot_error=RuntimeError("account unavailable")),
    )

    response = TestClient(app).get("/runtime/portfolio-risk")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_status"] == "partial"
    assert "no reusable broker access token" in payload["source_reason"]
    engine.dispose()


def test_portfolio_risk_endpoint_uses_stored_token_for_cash_snapshot() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, session_factory, db = _session(settings)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.commit()
    db.close()
    adapter = PortfolioRiskStubAdapter(
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("123456.78"),
            buying_power=Decimal("234567.89"),
            observed_at=datetime(2026, 6, 20, tzinfo=UTC),
            source="stub:account_snapshot",
        )
    )
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: adapter,
    )

    response = TestClient(app).get("/runtime/portfolio-risk")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_status"] == "ok"
    assert payload["account"]["available_cash"] == "123456.7800"
    assert payload["account"]["buying_power"] == "234567.8900"
    assert adapter.snapshot_calls == 1
    engine.dispose()


def test_portfolio_risk_snapshot_uses_broker_positions_when_holdings_are_empty() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.commit()
    snapshot = BrokerAccountSnapshot(
        observed_at=datetime(2026, 6, 20, tzinfo=UTC),
        source="stub:account_snapshot",
        positions=[
            BrokerAccountPosition(
                symbol="KRW-ETH",
                quantity=0,
                average_price=Decimal("4000000.00"),
                payload={"base_quantity": "0.50"},
            )
        ],
    )

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(snapshot=snapshot),
        now=datetime(2026, 6, 20, 3, 0, tzinfo=UTC),
    )

    assert payload["source_status"] == "ok"
    assert payload["holdings"]["open_count"] == 1
    holding = payload["holdings"]["top_by_market_value"][0]
    assert holding["symbol"] == "KRW-ETH"
    assert holding["base_quantity"] == "0.5000"
    assert holding["market_value"] == "2000000.0000"
    engine.dispose()
