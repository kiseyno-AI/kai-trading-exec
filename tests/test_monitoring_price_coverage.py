from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient

from app.broker.base import (
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerCurrentPrice,
    BrokerLiveMarketContext,
)
from app.core.config import Settings
from app.models.runtime import ExecutionPlanItem, ExecutionPosition
from app.services.monitoring_price_source import KisCurrentQuoteMonitoringPriceSource
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StubBrokerAdapter,
    build_order_placement,
    build_test_app,
    build_token_grant,
    wait_for,
)


class SymbolScopedQuoteErrorAdapter(StubBrokerAdapter):
    def __init__(
        self,
        *,
        current_price_errors: dict[str, Exception],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.current_price_errors = current_price_errors

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        symbol_error = self.current_price_errors.get(symbol)
        if symbol_error is not None:
            self.current_price_calls.append((symbol, exchange))
            raise symbol_error
        return super().get_current_price(access_token, symbol=symbol, exchange=exchange)


class LiveMarketContextAdapter(StubBrokerAdapter):
    def __init__(
        self,
        *,
        live_contexts: dict[str, BrokerLiveMarketContext] | None = None,
        live_context_errors: dict[str, Exception] | None = None,
        current_price_observed_at: datetime | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.live_contexts = live_contexts or {}
        self.live_context_errors = live_context_errors or {}
        self.current_price_observed_at = current_price_observed_at
        self.live_context_calls: list[tuple[str, str]] = []

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        quote = super().get_current_price(access_token, symbol=symbol, exchange=exchange)
        if quote is None or self.current_price_observed_at is None:
            return quote
        return BrokerCurrentPrice(
            symbol=quote.symbol,
            exchange=quote.exchange,
            price=quote.price,
            observed_at=self.current_price_observed_at,
            payload={
                "output": {
                    "stck_prpr": str(quote.price),
                    "acml_vol": "112278",
                    "acml_tr_pbmn": "175344817",
                }
            },
        )

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext | None:
        del reference_price
        assert access_token
        self.live_context_calls.append((symbol, exchange))
        symbol_error = self.live_context_errors.get(symbol)
        if symbol_error is not None:
            raise symbol_error
        return self.live_contexts.get(symbol)


def test_kis_price_source_enriches_monitoring_price_with_live_market_context() -> None:
    observed_at = datetime(2026, 6, 9, 1, 5, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"073560": Decimal("1583.00")},
        current_price_observed_at=observed_at,
        live_contexts={
            "073560": BrokerLiveMarketContext(
                symbol="073560",
                exchange="KRX",
                observed_at=observed_at,
                bid=Decimal("1582"),
                ask=Decimal("1584"),
                spread=Decimal("2"),
                spread_bps=Decimal("12.63423878648136449778900821"),
                orderbook_depth=Decimal("2500"),
                orderbook_imbalance=Decimal("0.2"),
                payload={
                    "provider": "kis",
                    "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                    "tr_id": "FHKST01010200",
                },
            )
        },
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            domestic_order_session_routing_enabled=True,
        ),
        broker_adapter=broker_adapter,
    )
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    prices = price_source.get_prices(
        symbols=["073560"],
        observed_at=observed_at,
        settings=app.state.settings,
    )

    assert len(prices) == 1
    price = prices[0]
    assert price.symbol == "073560"
    assert price.bid == Decimal("1582")
    assert price.ask == Decimal("1584")
    assert price.spread == Decimal("2")
    assert price.orderbook_depth == Decimal("2500")
    assert price.orderbook_imbalance == Decimal("0.2")
    assert price.session_type == "krx_regular"
    assert price.normal_ordering_available is True
    assert price.provenance is not None
    assert price.provenance["orderbook"]["endpoint"] == (
        "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
    )
    assert price_source.get_last_diagnostics()["context_failure_count"] == 0
    assert broker_adapter.live_context_calls == [("073560", "KRX")]
    engine.dispose()


def test_kis_price_source_keeps_quote_and_reports_context_enrichment_failure() -> None:
    observed_at = datetime(2026, 6, 9, 1, 5, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"073560": Decimal("1583.00")},
        current_price_observed_at=observed_at,
        live_context_errors={
            "073560": BrokerAdapterError(
                "KIS orderbook endpoint timed out",
                status_code=503,
                classification=BrokerErrorClassification.TRANSIENT,
                retryable=True,
            )
        },
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            domestic_order_session_routing_enabled=True,
        ),
        broker_adapter=broker_adapter,
    )
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    prices = price_source.get_prices(
        symbols=["073560"],
        observed_at=observed_at,
        settings=app.state.settings,
    )

    assert len(prices) == 1
    price = prices[0]
    assert price.price == Decimal("1583.00")
    assert price.bid is None
    assert price.ask is None
    assert price.orderbook_depth is None
    assert price.session_type == "krx_regular"
    assert price.normal_ordering_available is True
    assert price.provenance is not None
    assert price.provenance["orderbook"]["status"] == "failed"
    assert price.provenance["orderbook"]["reason"] == "KIS orderbook endpoint timed out"
    assert price.provenance["missing_fields"] == ["bid", "ask", "orderbook"]
    diagnostics = price_source.get_last_diagnostics()
    assert diagnostics["priced_symbol_count"] == 1
    assert diagnostics["context_failure_count"] == 1
    assert diagnostics["context_failures"] == [
        {
            "symbol": "073560",
            "endpoint": "broker_live_market_context",
            "reason": "KIS orderbook endpoint timed out",
        }
    ]
    engine.dispose()


def test_autonomous_monitoring_keeps_partial_prices_when_one_live_quote_fails() -> None:
    broker_adapter = SymbolScopedQuoteErrorAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-auto-monitor-partial"),
        current_prices={"005930": Decimal("125100.00")},
        current_price_errors={
            "000660": BrokerAdapterError(
                "EGW00201",
                status_code=429,
                classification=BrokerErrorClassification.RATE_LIMIT,
                retryable=True,
            )
        },
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            dispatch_loop_interval_seconds=60,
            monitoring_loop_interval_seconds=0.05,
        ),
        broker_adapter=broker_adapter,
        autonomous_runtime_enabled=True,
    )
    with TestClient(app) as client:
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable autonomous runtime"},
        )
        assert resume.status_code == 200

        plan_response = client.post(
            "/execution-plans",
            json={
                "plan_id": "plan-autonomous-monitoring-partial-quotes",
                "source": "kai-trading-core",
                "items": [
                    {
                        "symbol": "005930",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "130000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    },
                    {
                        "symbol": "000660",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "90000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    },
                ],
            },
        )
        assert plan_response.status_code == 201
        first_item_id, second_item_id = plan_response.json()["persisted_execution_item_ids"]

        with app.state.db_session_factory() as session:
            session.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        symbol="005930",
                        quantity=1,
                        average_price=Decimal("120000.0000"),
                        updated_from="reconciliation",
                        last_reconciled_at=datetime.now(UTC),
                        raw_payload={"source": "broker"},
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="000660",
                        quantity=1,
                        average_price=Decimal("88000.0000"),
                        updated_from="reconciliation",
                        last_reconciled_at=datetime.now(UTC),
                        raw_payload={"source": "broker"},
                    ),
                ]
            )
            session.commit()

        wait_for(
            lambda: (
                client.get("/runtime/status")
                .json()["liveness"]["autonomous"]["monitoring"]["price_source"]["last_price_count"]
                == 1
            )
        )

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        autonomous = runtime_status.json()["liveness"]["autonomous"]["monitoring"]
        price_source = autonomous["price_source"]
        diagnostics = price_source["diagnostics"]

        assert autonomous["last_cycle"]["price_count"] == 1
        assert autonomous["last_cycle"]["evaluated_item_count"] == 1
        assert price_source["status"] == "degraded"
        assert price_source["last_symbol_count"] == 2
        assert price_source["last_price_count"] == 1
        assert diagnostics["requested_symbol_count"] == 2
        assert diagnostics["priced_symbol_count"] == 1
        assert diagnostics["missing_symbol_count"] == 1
        assert diagnostics["missing_symbols"] == ["000660"]
        assert diagnostics["quote_failure_count"] == 1
        assert diagnostics["quote_failures"] == [{"symbol": "000660", "error": "EGW00201"}]
        assert ("005930", "KRX") in broker_adapter.current_price_calls
        assert ("000660", "KRX") in broker_adapter.current_price_calls
        assert broker_adapter.order_calls == []

        with app.state.db_session_factory() as session:
            first_item = session.get(ExecutionPlanItem, first_item_id)
            second_item = session.get(ExecutionPlanItem, second_item_id)
            assert first_item is not None
            assert second_item is not None
            assert first_item.status == "target_sell_monitoring"
            assert second_item.status == "target_sell_monitoring"

    engine.dispose()
