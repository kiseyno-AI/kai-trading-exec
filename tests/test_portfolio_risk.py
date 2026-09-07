from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import event

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountPosition,
    BrokerAccountSnapshot,
    BrokerAccountOpenOrder,
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


def test_portfolio_risk_snapshot_deduplicates_snapshot_and_local_open_order_by_broker_order_id() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.add(
        BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="local-idempotency-key",
            broker_order_id="kis-open-order-1",
            symbol="005930",
            side="buy",
            status="open",
            quantity=2,
            limit_price=Decimal("70000.00"),
            last_broker_sync_at=now,
        )
    )
    db.commit()
    snapshot = BrokerAccountSnapshot(
        observed_at=now,
        source="stub:account_snapshot",
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=2,
                remaining_quantity=2,
                status="open",
                broker_order_id="kis-open-order-1",
                idempotency_key=None,
                limit_price=Decimal("70000.00"),
            )
        ],
    )

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(snapshot=snapshot),
        now=now,
    )

    assert payload["source_status"] == "ok"
    assert payload["open_orders"]["open_count"] == 1
    assert payload["open_orders"]["top_by_notional"] == [
        {
            "symbol": "005930",
            "side": "buy",
            "quantity": 2,
            "base_quantity": None,
            "remaining_quantity": "2.0000",
                "limit_price": "70000.0000",
                "notional": "140000.0000",
                "settlement_currency": "KRW",
                "currency_source": "krx_symbol_format",
                "status": "open",
            "broker_order_id": "kis-open-order-1",
            "idempotency_key": None,
            "source": "stub:account_snapshot",
        }
    ]
    exposure = payload["exposures"]["by_symbol"][0]
    assert exposure["symbol"] == "005930"
    assert exposure["open_buy_notional"] == "140000.0000"
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


def test_portfolio_risk_snapshot_degrades_cross_currency_aggregates_without_fx() -> None:
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
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("200.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
                raw_payload={
                    "_position_sync": {
                        "asset_class": "us_equity",
                        "exchange": "NASD",
                    }
                },
            ),
        ]
    )
    _add_plan_with_stop(db, symbol="005930", stop_price="90000.00")
    _add_plan_with_stop(db, symbol="AAPL", stop_price="190.00")
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.commit()

    snapshots = {
        "KRX": BrokerAccountSnapshot(
            observed_at=now,
            source="kis:krx",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=10,
                    average_price=Decimal("100000"),
                    payload={"market_value": "1000000", "currency": "KRW"},
                )
            ],
            payload={"currency": "KRW", "exchange": "KRX"},
        ),
        "NASD": BrokerAccountSnapshot(
            observed_at=now,
            source="kis:nasd",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={
                        "market_value": "400",
                        "currency": "USD",
                        "ovrs_excg_cd": "NASD",
                    },
                )
            ],
            payload={"currency": "USD", "exchange": "NASD"},
        ),
        "AMEX": BrokerAccountSnapshot(observed_at=now, source="kis:amex", payload={"currency": "USD"}),
        "NYSE": BrokerAccountSnapshot(observed_at=now, source="kis:nyse", payload={"currency": "USD"}),
    }

    class MixedSnapshotAdapter(PortfolioRiskStubAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []

        def get_account_snapshot(self, access_token: str, **kwargs: object) -> BrokerAccountSnapshot:
            assert access_token == "stored-token"
            exchange = str(kwargs["exchange"])
            self.calls.append(exchange)
            return snapshots[exchange]

    adapter = MixedSnapshotAdapter()
    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        now=now,
    )

    assert payload["source_status"] == "ok"
    assert payload["source_reason"] is None
    assert adapter.calls == ["AMEX", "KRX", "NASD", "NYSE"]
    assert payload["aggregation_status"] == "degraded"
    assert (
        payload["aggregation_reason"]
        == "mixed_settlement_currencies_without_fx_conversion"
    )
    assert payload["aggregate_currency"] is None
    assert payload["account"]["total_market_value"] is None
    assert payload["account"]["total_securities_value"] is None
    assert payload["holdings"]["by_currency"] == {
        "KRW": {
            "holding_count": 1,
            "known_market_value_count": 1,
            "known_market_value_subtotal": "1000000.0000",
            "total_market_value": "1000000.0000",
        },
        "USD": {
            "holding_count": 1,
            "known_market_value_count": 1,
            "known_market_value_subtotal": "400.0000",
            "total_market_value": "400.0000",
        },
    }
    holdings = {
        row["symbol"]: row for row in payload["holdings"]["top_by_market_value"]
    }
    assert holdings["005930"]["settlement_currency"] == "KRW"
    assert holdings["AAPL"]["settlement_currency"] == "USD"
    assert holdings["005930"]["allocation_pct"] is None
    assert holdings["AAPL"]["allocation_pct"] is None
    assert holdings["005930"]["currency_allocation_pct"] == "100.0000"
    assert holdings["AAPL"]["currency_allocation_pct"] == "100.0000"
    assert payload["estimated_stop_risk"]["total_estimated_risk"] is None
    assert payload["estimated_stop_risk"]["by_currency"] == {
        "KRW": {
            "position_count": 1,
            "known_estimated_risk_count": 1,
            "total_estimated_risk": "100000.0000",
            "known_estimated_risk_subtotal": "100000.0000",
        },
        "USD": {
            "position_count": 1,
            "known_estimated_risk_count": 1,
            "total_estimated_risk": "20.0000",
            "known_estimated_risk_subtotal": "20.0000",
        },
    }
    engine.dispose()


def test_portfolio_risk_snapshot_preserves_unknown_currency_diagnostics() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="LEGACY",
            quantity=2,
            average_price=Decimal("10.00"),
            updated_from="legacy_import",
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

    assert payload["aggregation_status"] == "degraded"
    assert (
        payload["aggregation_reason"]
        == "unknown_settlement_currency_without_fx_conversion"
    )
    assert payload["account"]["total_market_value"] is None
    assert payload["holdings"]["by_currency"]["UNKNOWN"] == {
        "holding_count": 1,
        "known_market_value_count": 1,
        "known_market_value_subtotal": None,
        "total_market_value": None,
    }
    holding = payload["holdings"]["top_by_market_value"][0]
    assert holding["market_value"] == "20.0000"
    assert holding["settlement_currency"] is None
    assert holding["allocation_pct"] is None
    engine.dispose()


def test_portfolio_risk_snapshot_queries_kis_us_exchange_for_aapl() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="AAPL",
            quantity=2,
            average_price=Decimal("200.00"),
            updated_from="broker_reconciliation",
            last_reconciled_at=now,
            raw_payload={
                "_position_sync": {
                    "provider": "kis",
                    "asset_class": "us_equity",
                    "exchange": "NASD",
                    "settlement_currency": "USD",
                }
            },
        )
    )
    db.commit()

    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        observed_at=now,
        source="kis:us_equity_account_snapshot",
        holdings=[
            BrokerAccountHolding(
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("210.00"),
                payload={
                    "currency": "USD",
                    "ovrs_excg_cd": "NASD",
                    "ovrs_stck_evlu_amt": "420.00",
                    "market_value": "420.00",
                },
            )
        ],
        payload={"currency": "USD", "exchange": "NASD"},
    )

    class KisUsSnapshotAdapter(PortfolioRiskStubAdapter):
        def __init__(self) -> None:
            super().__init__(snapshot=snapshot)
            self.calls: list[tuple[list[str] | None, str | None]] = []

        def get_account_snapshot(
            self,
            access_token: str,
            **kwargs: object,
        ) -> BrokerAccountSnapshot:
            self.calls.append(
                (
                    kwargs.get("symbols"),
                    kwargs.get("exchange"),
                )
            )
            return super().get_account_snapshot(access_token, **kwargs)

    adapter = KisUsSnapshotAdapter()
    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        now=now,
    )

    assert adapter.calls == [(["AAPL"], "AMEX"), (["AAPL"], "NASD"), (["AAPL"], "NYSE")]
    assert payload["source_status"] == "ok"
    assert payload["account"]["snapshot_exchange"] == "AMEX,NASD,NYSE"
    holding = payload["holdings"]["top_by_market_value"][0]
    assert holding["symbol"] == "AAPL"
    assert holding["settlement_currency"] == "USD"
    assert holding["market_value"] == "420.0000"
    engine.dispose()


def test_portfolio_risk_snapshot_discovers_holdings_across_all_kis_us_exchanges() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="AAPL",
            quantity=1,
            average_price=Decimal("200"),
            updated_from="broker_reconciliation",
            last_reconciled_at=now,
            raw_payload={"_position_sync": {"asset_class": "us_equity", "exchange": "NASD"}},
        )
    )
    db.commit()
    snapshots = {
        "AMEX": BrokerAccountSnapshot(observed_at=now, source="kis:amex", payload={"currency": "USD"}),
        "NASD": BrokerAccountSnapshot(
            observed_at=now,
            source="kis:nasd",
            holdings=[BrokerAccountHolding(symbol="AAPL", quantity=1, average_price=Decimal("200"), payload={"currency": "USD", "market_value": "200", "ovrs_excg_cd": "NASD"})],
            payload={"currency": "USD"},
        ),
        "NYSE": BrokerAccountSnapshot(
            observed_at=now,
            source="kis:nyse",
            holdings=[BrokerAccountHolding(symbol="MSFT", quantity=2, average_price=Decimal("300"), payload={"currency": "USD", "market_value": "600", "ovrs_excg_cd": "NYSE"})],
            payload={"currency": "USD"},
        ),
    }

    class AllUsExchangesAdapter(PortfolioRiskStubAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []

        def get_account_snapshot(self, access_token: str, **kwargs: object) -> BrokerAccountSnapshot:
            assert access_token == "stored-token"
            exchange = str(kwargs["exchange"])
            self.calls.append(exchange)
            return snapshots[exchange]

    adapter = AllUsExchangesAdapter()
    payload = build_portfolio_risk_snapshot(settings=settings, db=db, adapter=adapter, now=now)

    assert adapter.calls == ["AMEX", "NASD", "NYSE"]
    assert payload["source_status"] == "ok"
    assert {row["symbol"] for row in payload["holdings"]["top_by_market_value"]} == {"AAPL", "MSFT"}
    assert payload["holdings"]["by_currency"]["USD"]["total_market_value"] == "800.0000"
    engine.dispose()


def test_portfolio_risk_empty_local_scope_queries_krx_and_us_when_enabled() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_trading_enabled=True,
    )
    engine, _factory, db = _session(settings)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.commit()

    class DiscoveryAdapter(PortfolioRiskStubAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[str] = []

        def get_account_snapshot(self, access_token: str, **kwargs: object) -> BrokerAccountSnapshot:
            assert access_token == "stored-token"
            exchange = str(kwargs["exchange"])
            self.calls.append(exchange)
            currency = "KRW" if exchange == "KRX" else "USD"
            return BrokerAccountSnapshot(
                observed_at=datetime(2026, 6, 20, tzinfo=UTC),
                source=f"kis:{exchange.lower()}",
                payload={"currency": currency},
            )

    adapter = DiscoveryAdapter()
    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        now=datetime(2026, 6, 20, 3, 0, tzinfo=UTC),
    )

    assert adapter.calls == ["AMEX", "KRX", "NASD", "NYSE"]
    assert payload["source_status"] == "ok"
    assert payload["account"]["snapshot_exchange"] == "AMEX,KRX,NASD,NYSE"
    engine.dispose()


def test_portfolio_risk_partial_exchange_failure_preserves_local_holdings_and_degrades_totals() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                symbol="AAPL",
                quantity=1,
                average_price=Decimal("200"),
                updated_from="broker_reconciliation",
                last_reconciled_at=now,
                raw_payload={"_position_sync": {"asset_class": "us_equity", "exchange": "NASD"}},
            ),
            ExecutionPosition(
                mode="paper",
                symbol="MSFT",
                quantity=2,
                average_price=Decimal("300"),
                updated_from="broker_reconciliation",
                last_reconciled_at=now,
                raw_payload={"_position_sync": {"asset_class": "us_equity", "exchange": "NYSE"}},
            ),
        ]
    )
    db.commit()

    class PartialUsAdapter(PortfolioRiskStubAdapter):
        def get_account_snapshot(self, access_token: str, **kwargs: object) -> BrokerAccountSnapshot:
            assert access_token == "stored-token"
            exchange = str(kwargs["exchange"])
            if exchange == "NYSE":
                raise RuntimeError("NYSE temporarily unavailable")
            holdings = (
                [BrokerAccountHolding(symbol="AAPL", quantity=1, average_price=Decimal("210"), payload={"currency": "USD", "market_value": "210", "ovrs_excg_cd": "NASD"})]
                if exchange == "NASD"
                else []
            )
            return BrokerAccountSnapshot(
                observed_at=now,
                source=f"kis:{exchange.lower()}",
                holdings=holdings,
                payload={"currency": "USD"},
            )

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PartialUsAdapter(),
        now=now,
    )

    assert payload["source_status"] == "partial"
    assert "NYSE" in payload["source_reason"]
    assert {row["symbol"] for row in payload["holdings"]["top_by_market_value"]} == {"AAPL", "MSFT"}
    assert payload["account"]["total_market_value"] is None
    assert payload["holdings"]["currency_aggregation"]["status"] == "degraded"
    assert "incomplete_broker_snapshot_scope" in payload["holdings"]["currency_aggregation"]["reason"]
    engine.dispose()


def test_portfolio_risk_snapshot_hides_partial_same_currency_market_value_total() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100.00"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=5,
                average_price=None,
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

    assert payload["aggregation_status"] == "degraded"
    assert "incomplete_portfolio_value_coverage" in payload["aggregation_reason"]
    assert payload["account"]["total_market_value"] is None
    assert payload["account"]["aggregation_status"] == "degraded"
    assert payload["account"]["aggregation_reason"] == (
        "incomplete_market_value_coverage"
    )
    assert payload["holdings"]["by_currency"]["KRW"] == {
        "holding_count": 2,
        "known_market_value_count": 1,
        "known_market_value_subtotal": "1000.0000",
        "total_market_value": None,
    }
    assert all(
        row["allocation_pct"] is None
        and row["currency_allocation_pct"] is None
        for row in payload["holdings"]["top_by_market_value"]
    )
    warning_codes = {
        warning["code"] for warning in payload["warnings"]["concentration"]
    }
    assert "incomplete_market_value_coverage" in warning_codes
    engine.dispose()


def test_portfolio_risk_snapshot_hides_partial_stop_risk_total() -> None:
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

    stop_risk = payload["estimated_stop_risk"]
    assert stop_risk["total_estimated_risk"] is None
    assert stop_risk["currency_aggregation"]["status"] == "degraded"
    assert stop_risk["currency_aggregation"]["reason"] == (
        "incomplete_stop_risk_coverage"
    )
    assert stop_risk["by_currency"]["KRW"] == {
        "position_count": 2,
        "known_estimated_risk_count": 1,
        "total_estimated_risk": None,
        "known_estimated_risk_subtotal": "100000.0000",
    }
    warning_codes = {
        warning["code"] for warning in payload["warnings"]["concentration"]
    }
    assert "incomplete_stop_risk_coverage" in warning_codes
    engine.dispose()


def test_portfolio_risk_currency_history_fallback_reads_are_bounded() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 20, 3, 0, tzinfo=UTC)
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                symbol="ORDERMETA",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
            ExecutionPosition(
                mode="paper",
                symbol="PLANMETA",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="reconciliation",
                last_reconciled_at=now,
            ),
        ]
    )
    for index in range(12):
        db.add(
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key=f"history-order-{index}",
                symbol="ORDERMETA",
                side="buy",
                status="filled",
                quantity=1,
                limit_price=Decimal("100"),
                asset_class="us_equity",
                venue="NASD",
            )
        )
        plan = ExecutionPlan(
            external_plan_id=f"history-plan-{index}",
            source="test",
            mode="paper",
            status="accepted",
        )
        db.add_all(
            [
                plan,
                ExecutionPlanItem(
                    plan=plan,
                    plan_item_index=0,
                    symbol="PLANMETA",
                    side="buy",
                    intent="entry_buy",
                    status="planned",
                    provider="kis",
                    venue="NYSE",
                    asset_class="us_equity",
                ),
            ]
        )
    db.commit()
    statements: list[str] = []

    def capture_sql(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_sql)
    try:
        payload = build_portfolio_risk_snapshot(
            settings=settings,
            db=db,
            adapter=None,
            now=now,
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture_sql)

    assert payload["holdings"]["open_count"] == 2
    history_reads = [
        statement
        for statement in statements
        if "ROW_NUMBER() OVER" in statement.upper()
        and (
            "FROM BROKER_ORDERS" in statement.upper()
            or "FROM EXECUTION_PLAN_ITEMS" in statement.upper()
        )
    ]
    assert len(history_reads) == 2
    assert all("LIMIT" in statement.upper() for statement in history_reads)
    raw_payload_reads = [
        statement
        for statement in statements
        if "execution_positions.raw_payload" in statement
        and "FROM execution_positions" in statement
    ]
    assert raw_payload_reads
    assert all("LIMIT" in statement.upper() for statement in raw_payload_reads)
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
            total_asset_value=Decimal("987654.32"),
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
    assert payload["account"]["total_asset_value"] == "987654.3200"
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


def test_portfolio_risk_snapshot_uses_fresh_broker_raw_holdings_and_surfaces_drift() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine, _factory, db = _session(settings)
    now = datetime(2026, 6, 27, 4, 2, tzinfo=UTC)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="088910",
            quantity=74,
            average_price=Decimal("3000.00"),
            updated_from="reconciliation",
            last_reconciled_at=now,
        )
    )
    db.commit()
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=now,
        source="stub:account_snapshot",
        holdings=[],
        payload={
            "balance": {
                "rt_cd": "0",
                "output1": [
                    {
                        "pdno": "002380",
                        "prdt_name": "KCC",
                        "hldg_qty": "1",
                        "ord_psbl_qty": "1",
                        "pchs_avg_pric": "350000",
                        "prpr": "333500",
                        "evlu_amt": "333,500",
                        "evlu_pfls_amt": "-16500",
                        "evlu_pfls_rt": "-4.7143",
                    },
                    {
                        "pdno": "031330",
                        "prdt_name": "SAMT",
                        "hldg_qty": "35",
                        "ord_psbl_qty": "35",
                        "pchs_avg_pric": "6000",
                        "prpr": "1830",
                        "evlu_amt": "64050",
                        "evlu_pfls_amt": "-145950",
                        "evlu_pfls_rt": "-69.5000",
                    },
                    {
                        "pdno": "088910",
                        "prdt_name": "Dongwoo Farm To Table",
                        "hldg_qty": "74",
                        "ord_psbl_qty": "74",
                        "pchs_avg_pric": "3000",
                        "prpr": "2585",
                        "evlu_amt": "191290",
                        "evlu_pfls_amt": "-30710",
                        "evlu_pfls_rt": "-13.8333",
                    },
                ],
            }
        },
    )

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=PortfolioRiskStubAdapter(snapshot=snapshot),
        now=now,
    )

    assert payload["source_status"] == "ok"
    assert payload["account"]["total_market_value"] == "588840.0000"
    assert payload["holdings"]["open_count"] == 3
    by_symbol = {row["symbol"]: row for row in payload["exposures"]["by_symbol"]}
    assert by_symbol["002380"]["holding_value"] == "333500.0000"
    assert by_symbol["031330"]["holding_value"] == "64050.0000"
    broker_raw = payload["holdings"]["broker_raw"]
    assert broker_raw["available"] is True
    assert broker_raw["valid"] is True
    assert broker_raw["fresh"] is True
    assert broker_raw["normalized_drift_count"] == 2
    drift_by_symbol = {row["symbol"]: row for row in broker_raw["normalized_drift"]}
    assert drift_by_symbol["002380"]["reason"] == "broker_raw_open_missing_from_normalized"
    assert drift_by_symbol["002380"]["raw_current_price"] == "333500.0000"
    assert drift_by_symbol["002380"]["raw_unrealized_pnl_krw"] == "-16500.0000"
    assert drift_by_symbol["031330"]["raw_unrealized_pnl_krw"] == "-145950.0000"
    warning_codes = {warning["code"] for warning in payload["warnings"]["concentration"]}
    assert "broker_raw_normalized_position_drift" in warning_codes
    engine.dispose()


def test_portfolio_risk_snapshot_accepts_raw_balance_observed_milliseconds_after_generated_at() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine, _factory, db = _session(settings)
    _add_broker_session(db, expires_at=datetime.now(UTC) + timedelta(hours=1))
    db.commit()

    class FutureObservedAdapter(PortfolioRiskStubAdapter):
        def get_account_snapshot(self, access_token: str, **_kwargs: object) -> BrokerAccountSnapshot:
            assert access_token == "stored-token"
            self.snapshot_calls += 1
            return BrokerAccountSnapshot(
                observed_at=datetime.now(UTC),
                source="stub:account_snapshot",
                holdings=[],
                payload={
                    "balance": {
                        "rt_cd": "0",
                        "output1": [
                            {
                                "pdno": "005930",
                                "prdt_name": "Samsung Electronics",
                                "hldg_qty": "2",
                                "ord_psbl_qty": "2",
                                "pchs_avg_pric": "70000",
                                "prpr": "71000",
                                "evlu_amt": "142000",
                            }
                        ],
                    }
                },
            )

    adapter = FutureObservedAdapter()

    payload = build_portfolio_risk_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
    )

    assert payload["generated_at"] < payload["account"]["observed_at"]
    broker_raw = payload["holdings"]["broker_raw"]
    assert broker_raw["valid"] is True
    assert broker_raw["fresh"] is True
    assert broker_raw["available"] is True
    assert payload["holdings"]["open_count"] == 1
    assert payload["holdings"]["top_by_market_value"][0]["symbol"] == "005930"
    assert adapter.snapshot_calls == 1
    engine.dispose()
