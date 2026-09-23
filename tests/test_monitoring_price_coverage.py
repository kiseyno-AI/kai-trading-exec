from datetime import UTC, datetime, timedelta
from decimal import Decimal
import threading

from fastapi.testclient import TestClient

from app.broker.base import (
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerCurrentPrice,
    BrokerLiveMarketContext,
)
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.broker.kis import KIS_OVERSEAS_QUOTE_EMPTY_CODE
from app.services import monitoring_price_source as monitoring_price_source_module
from app.services.monitoring_price_source import KisCurrentQuoteMonitoringPriceSource
from app.services.monitoring_price_source import _KisMonitoringRuntimeContext
from app.services.monitoring_price_source import _derive_orderability_context
from app.services.monitoring_price_source import monitoring_price_from_broker_quote
from app.services.monitoring_price_source import next_us_regular_open
from app.services.position_identity import build_position_identity
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


def test_us_orderability_uses_new_york_regular_session_while_krx_is_closed() -> None:
    context = _derive_orderability_context(
        settings=Settings(domestic_order_session_routing_enabled=True),
        symbol="AAPL",
        price=Decimal("201.25"),
        observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
        exchange="NASD",
    )

    assert context["session_type"] == "us_regular"
    assert context["normal_ordering_available"] is True
    assert context["orderability_source"] == "us_regular_session_clock"


def test_terminal_newer_route_cannot_redirect_active_protective_quote() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    app, engine = build_test_app(settings=settings)
    with app.state.db_session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="route-authority-plan",
            source="test",
            market_date=datetime(2026, 9, 9, tzinfo=UTC).date(),
            mode="paper",
            status="accepted",
            items=[
                ExecutionPlanItem(
                    id="active-nasd-stop",
                    plan_item_index=0,
                    symbol="AAPL",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_monitoring",
                    provider="kis",
                    venue="kis",
                    asset_class="us_equity",
                    trigger_price=Decimal("220"),
                    risk_context={"exchange": "NASD"},
                    updated_at=datetime(2026, 9, 9, 13, 0, tzinfo=UTC),
                ),
                ExecutionPlanItem(
                    id="newer-rejected-nyse-target",
                    plan_item_index=1,
                    symbol="AAPL",
                    side="sell",
                    intent="target_sell",
                    status="agent_rejected",
                    provider="kis",
                    venue="kis",
                    asset_class="us_equity",
                    trigger_price=Decimal("240"),
                    risk_context={"exchange": "NYSE"},
                    updated_at=datetime(2026, 9, 9, 13, 5, tzinfo=UTC),
                ),
            ],
        )
        db.add(plan)
        db.commit()

        contexts = (
            monitoring_price_source_module._load_monitoring_execution_context_by_symbol(
                settings=Settings(runtime_mode="paper"),
                db=db,
                symbols=["AAPL"],
            )
        )

    assert contexts["AAPL"]["exchange"] == "NASD"
    assert contexts["AAPL"]["provider"] == "kis"
    assert contexts["AAPL"]["order_contexts"] == [
        {"side": "sell", "purpose": "protective_stop"}
    ]
    engine.dispose()


def test_us_orderability_closes_on_nyse_holiday() -> None:
    context = _derive_orderability_context(
        settings=Settings(),
        symbol="AAPL",
        price=Decimal("201.25"),
        observed_at=datetime(2026, 7, 3, 15, 0, tzinfo=UTC),
        exchange="NASD",
    )

    assert context["session_type"] == "us_closed"
    assert context["normal_ordering_available"] is False


def test_us_orderability_closes_after_black_friday_early_close() -> None:
    observed_at = datetime(2026, 11, 27, 18, 30, tzinfo=UTC)
    context = _derive_orderability_context(
        settings=Settings(),
        symbol="AAPL",
        price=Decimal("201.25"),
        observed_at=observed_at,
        exchange="NASD",
    )

    assert context["session_type"] == "us_closed"
    assert context["normal_ordering_available"] is False
    assert next_us_regular_open(observed_at) == datetime(2026, 11, 30, 14, 30, tzinfo=UTC)


def test_us_orderability_applies_independence_day_eve_early_close_when_open() -> None:
    context = _derive_orderability_context(
        settings=Settings(),
        symbol="AAPL",
        price=Decimal("201.25"),
        observed_at=datetime(2028, 7, 3, 17, 30, tzinfo=UTC),
        exchange="NASD",
    )

    assert context["session_type"] == "us_closed"
    assert context["normal_ordering_available"] is False


def test_us_orderability_closes_after_christmas_eve_early_close() -> None:
    context = _derive_orderability_context(
        settings=Settings(),
        symbol="AAPL",
        price=Decimal("201.25"),
        observed_at=datetime(2026, 12, 24, 18, 30, tzinfo=UTC),
        exchange="NASD",
    )

    assert context["session_type"] == "us_closed"
    assert context["normal_ordering_available"] is False


def test_krx_orderability_uses_actual_buy_side_and_entry_purpose() -> None:
    context = _derive_orderability_context(
        settings=Settings(),
        symbol="005930",
        price=Decimal("71000"),
        observed_at=datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
        exchange="KRX",
        side="buy",
        purpose="intraday_watch_buy",
    )

    assert context["orderability_action"] == "block"
    assert context["orderability_reason"] == "closed_not_orderable"


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


def test_kis_price_source_reuses_primed_runtime_context_without_db_checkout() -> None:
    observed_at = datetime(2026, 9, 23, 1, 5, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"005930": Decimal("70000")},
        current_price_observed_at=observed_at,
    )
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    app, engine = build_test_app(settings=settings, broker_adapter=broker_adapter)
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )
    with app.state.db_session_factory() as db:
        price_source.prime_runtime_context(
            db=db,
            symbols=["005930"],
            loaded_at=observed_at,
        )

    def fail_session_checkout():
        raise AssertionError("primed quote cycles must not open a DB session")

    price_source.session_factory = fail_session_checkout  # type: ignore[assignment]

    first = price_source.get_prices(
        symbols=["005930"],
        observed_at=observed_at,
        settings=settings,
    )
    second = price_source.get_prices(
        symbols=["005930"],
        observed_at=observed_at + timedelta(seconds=1),
        settings=settings,
    )

    assert [price.symbol for price in first] == ["005930"]
    assert [price.symbol for price in second] == ["005930"]
    assert broker_adapter.current_price_calls == [
        ("005930", "KRX"),
        ("005930", "KRX"),
    ]
    price_source.close()
    engine.dispose()


def test_kis_price_source_discards_cached_context_after_auth_rejection() -> None:
    observed_at = datetime(2026, 9, 23, 1, 5, tzinfo=UTC)
    broker_adapter = SymbolScopedQuoteErrorAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_price_errors={
            "005930": BrokerAdapterError(
                "token rejected",
                status_code=422,
                classification=BrokerErrorClassification.AUTH,
                response_metadata={"status_code": 401},
            )
        },
    )
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    app, engine = build_test_app(settings=settings, broker_adapter=broker_adapter)
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )
    with app.state.db_session_factory() as db:
        price_source.prime_runtime_context(
            db=db,
            symbols=["005930"],
            loaded_at=observed_at,
        )

    assert price_source.get_prices(
        symbols=["005930"],
        observed_at=observed_at,
        settings=settings,
    ) == []
    assert price_source._cached_runtime_context(symbols=["005930"]) is None

    price_source.close()
    engine.dispose()


def test_kis_price_source_invalidation_wins_over_inflight_prime(monkeypatch) -> None:
    loaded_at = datetime(2026, 9, 23, 1, 5, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
    )
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=lambda: None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda _settings: broker_adapter,
    )
    entered = threading.Event()
    release = threading.Event()
    errors: list[Exception] = []

    def build_context(**kwargs) -> _KisMonitoringRuntimeContext:
        del kwargs
        entered.set()
        assert release.wait(timeout=2)
        return _KisMonitoringRuntimeContext(
            cached_at=loaded_at,
            refresh_before=loaded_at + timedelta(minutes=5),
            access_token="stale-token",
            covered_symbols=frozenset({"005930"}),
            execution_context_by_symbol={},
        )

    monkeypatch.setattr(price_source, "_build_runtime_context", build_context)

    def prime() -> None:
        try:
            price_source.prime_runtime_context(
                db=object(),  # type: ignore[arg-type]
                symbols=["005930"],
                loaded_at=loaded_at,
            )
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=prime)
    worker.start()
    assert entered.wait(timeout=2)
    price_source.invalidate_runtime_context(reason="plan_route_changed")
    release.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert "invalidated during refresh" in str(errors[0])
    assert price_source._cached_runtime_context(symbols=["005930"]) is None
    price_source.close()


def test_kis_price_source_requires_repeated_empty_quotes_before_durable_review() -> None:
    observed_at = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    broker_adapter = SymbolScopedQuoteErrorAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={},
        current_price_errors={
            "OLDCO": BrokerAdapterError(
                "KIS overseas minute price response is missing output2 payload",
                status_code=502,
                code=KIS_OVERSEAS_QUOTE_EMPTY_CODE,
            ),
            "RETRY": BrokerAdapterError(
                "temporary rate limit",
                status_code=429,
                classification=BrokerErrorClassification.RATE_LIMIT,
                retryable=True,
                code="EGW00201",
            ),
        },
    )
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
        execution_position_account_scope="durable-empty-quote-test",
    )
    app, engine = build_test_app(settings=settings, broker_adapter=broker_adapter)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    with app.state.db_session_factory() as db:
        for symbol in ("OLDCO", "RETRY"):
            db.add(
                ExecutionPosition(
                    mode=settings.runtime_mode,
                    provider=identity.provider,
                    account_scope=identity.account_scope,
                    exchange=identity.exchange,
                    symbol=symbol,
                    quantity=1,
                    average_price=Decimal("1.00"),
                    updated_from="reconciliation",
                    raw_payload={
                        "_position_sync": {
                            "asset_class": "us_equity",
                            "exchange": "NASD",
                            "monitoring_eligible": True,
                        }
                    },
                )
            )
        db.commit()
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    assert price_source.get_prices(
        symbols=["OLDCO", "RETRY"],
        observed_at=observed_at,
        settings=settings,
    ) == []
    assert price_source.get_last_diagnostics()[
        "empty_quote_consecutive_counts"
    ] == {"OLDCO": 1}

    broker_adapter.current_price_errors["OLDCO"] = BrokerAdapterError(
        "temporary rate limit",
        status_code=429,
        classification=BrokerErrorClassification.RATE_LIMIT,
        retryable=True,
        code="EGW00201",
    )
    assert price_source.get_prices(
        symbols=["OLDCO", "RETRY"],
        observed_at=observed_at,
        settings=settings,
    ) == []
    assert price_source.get_last_diagnostics()[
        "empty_quote_consecutive_counts"
    ] == {}

    broker_adapter.current_price_errors["OLDCO"] = BrokerAdapterError(
        "KIS overseas minute price response contains no output2 rows",
        status_code=502,
        code=KIS_OVERSEAS_QUOTE_EMPTY_CODE,
    )
    for expected_count in (1, 2):
        assert price_source.get_prices(
            symbols=["OLDCO", "RETRY"],
            observed_at=observed_at,
            settings=settings,
        ) == []
        diagnostics = price_source.get_last_diagnostics()
        assert diagnostics["durably_unquoteable_symbols"] == []
        assert diagnostics["empty_quote_consecutive_counts"] == {
            "OLDCO": expected_count
        }
    assert price_source.get_prices(
        symbols=["OLDCO", "RETRY"],
        observed_at=observed_at,
        settings=settings,
    ) == []
    diagnostics = price_source.get_last_diagnostics()
    assert diagnostics["durably_unquoteable_symbols"] == ["OLDCO"]
    assert diagnostics["empty_quote_consecutive_counts"] == {"OLDCO": 3}
    assert next(
        failure
        for failure in diagnostics["quote_failures"]
        if failure["symbol"] == "OLDCO"
    )["code"] == KIS_OVERSEAS_QUOTE_EMPTY_CODE

    broker_adapter.current_price_errors.pop("OLDCO")
    broker_adapter.current_prices["OLDCO"] = Decimal("1.25")
    assert [
        price.symbol
        for price in price_source.get_prices(
            symbols=["OLDCO"],
            observed_at=observed_at,
            settings=settings,
        )
    ] == ["OLDCO"]
    diagnostics = price_source.get_last_diagnostics()
    assert diagnostics["durably_unquoteable_symbols"] == []
    assert diagnostics["empty_quote_consecutive_counts"] == {}
    price_source.close()
    engine.dispose()


def test_fresh_partial_orderbook_does_not_relabel_untrusted_bid_as_fresh() -> None:
    stale_transport_at = datetime(2026, 9, 9, 0, 0, tzinfo=UTC)
    fresh_exchange_at = datetime(2026, 9, 9, 0, 1, tzinfo=UTC)

    price = monitoring_price_from_broker_quote(
        settings=Settings(runtime_mode="live"),
        quote=BrokerCurrentPrice(
            symbol="005930",
            exchange="KRX",
            price=Decimal("71000"),
            observed_at=stale_transport_at,
            freshness_source="transport_received_at_untrusted",
            payload={"output": {"bidp1": "70990"}},
        ),
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=fresh_exchange_at,
            ask=Decimal("71010"),
            freshness_source="kis_exchange_orderbook_time",
            payload={"endpoint": "orderbook"},
        ),
        context_failure=None,
        side="sell",
        purpose="target_sell",
    )

    assert price.observed_at == fresh_exchange_at
    assert price.ask == Decimal("71010")
    assert price.bid is None
    assert price.provenance is not None
    assert "bid" not in price.provenance["field_sources"]


def test_kis_price_source_derives_orderability_from_actual_watch_buy() -> None:
    observed_at = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"005930": Decimal("71000")},
        current_price_observed_at=observed_at,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    with app.state.db_session_factory() as db:
        db.add(
            ExecutionPlan(
                external_plan_id="plan-buy-orderability",
                source="test",
                mode="paper",
                items=[
                    ExecutionPlanItem(
                        symbol="005930",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_monitoring_active",
                        trigger_price=Decimal("71000"),
                        risk_context={"exchange": "KRX"},
                    )
                ],
            )
        )
        db.commit()
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    prices = price_source.get_prices(
        symbols=["005930"],
        observed_at=observed_at,
        settings=app.state.settings,
    )

    assert len(prices) == 1
    assert prices[0].normal_ordering_available is False
    assert prices[0].provenance is not None
    assert prices[0].provenance["session"]["action"] == "block"
    assert prices[0].provenance["session"]["reason"] == "closed_not_orderable"
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


def test_kis_price_source_enriches_contexts_concurrently_with_deterministic_order() -> None:
    observed_at = datetime(2026, 6, 9, 1, 5, tzinfo=UTC)
    active_context_calls = 0
    max_active_context_calls = 0
    concurrent_calls_entered = threading.Event()
    call_lock = threading.Lock()

    class ConcurrentContextAdapter(LiveMarketContextAdapter):
        def get_live_market_context(
            self,
            access_token: str,
            *,
            symbol: str,
            exchange: str = "KRX",
            reference_price: Decimal | None = None,
        ) -> BrokerLiveMarketContext | None:
            nonlocal active_context_calls, max_active_context_calls
            del reference_price
            assert access_token
            with call_lock:
                active_context_calls += 1
                max_active_context_calls = max(max_active_context_calls, active_context_calls)
                if active_context_calls >= 2:
                    concurrent_calls_entered.set()
            try:
                assert concurrent_calls_entered.wait(timeout=1)
                return BrokerLiveMarketContext(
                    symbol=symbol,
                    exchange=exchange,
                    observed_at=observed_at,
                    bid=Decimal("99"),
                    ask=Decimal("101"),
                    orderbook_depth=Decimal("1000"),
                )
            finally:
                with call_lock:
                    active_context_calls -= 1

    symbols = ["000003", "000001", "000002"]
    broker_adapter = ConcurrentContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={symbol: Decimal("100") for symbol in symbols},
        current_price_observed_at=observed_at,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    prices = price_source.get_prices(
        symbols=symbols,
        observed_at=observed_at,
        settings=app.state.settings,
    )

    assert [price.symbol for price in prices] == symbols
    assert 2 <= max_active_context_calls <= monitoring_price_source_module.MAX_QUOTE_WORKERS
    assert price_source.get_last_diagnostics()["context_failure_count"] == 0
    engine.dispose()


def test_kis_price_source_ignores_zero_intraday_high_low_from_quote_payload() -> None:
    observed_at = datetime(2026, 6, 29, 0, 1, tzinfo=UTC)
    broker_adapter = LiveMarketContextAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"088910": Decimal("2520.00")},
        current_price_observed_at=observed_at,
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
    original_get_current_price = broker_adapter.get_current_price

    def get_current_price_with_zero_high_low(
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        quote = original_get_current_price(access_token, symbol=symbol, exchange=exchange)
        assert quote is not None
        return BrokerCurrentPrice(
            symbol=quote.symbol,
            exchange=quote.exchange,
            price=quote.price,
            observed_at=quote.observed_at,
            payload={
                "output": {
                    "stck_prpr": str(quote.price),
                    "stck_hgpr": "0",
                    "stck_lwpr": "0",
                    "acml_vol": "0",
                    "acml_tr_pbmn": "0",
                }
            },
        )

    broker_adapter.get_current_price = get_current_price_with_zero_high_low  # type: ignore[method-assign]
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )

    prices = price_source.get_prices(
        symbols=["088910"],
        observed_at=observed_at,
        settings=app.state.settings,
    )

    assert len(prices) == 1
    price = prices[0]
    assert price.symbol == "088910"
    assert price.price == Decimal("2520.00")
    assert price.day_high is None
    assert price.day_low is None
    diagnostics = price_source.get_last_diagnostics()
    assert diagnostics["priced_symbol_count"] == 1
    assert diagnostics["missing_symbol_count"] == 0
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
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
        dispatch_loop_interval_seconds=60,
        monitoring_loop_interval_seconds=0.05,
    )
    app, engine = build_test_app(
        settings=settings,
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
                "source": "test-fixture",
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
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )

        with app.state.db_session_factory() as session:
            session.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol="005930",
                        quantity=1,
                        average_price=Decimal("120000.0000"),
                        updated_from="reconciliation",
                        last_reconciled_at=datetime.now(UTC),
                        raw_payload={"source": "broker"},
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
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

        def monitoring_cycle_covers_both_positions() -> bool:
            response = client.get("/runtime/status")
            if response.status_code != 200:
                return False
            monitoring = response.json()["liveness"]["autonomous"]["monitoring"]
            last_cycle = monitoring.get("last_cycle")
            return bool(
                monitoring["price_source"]["last_price_count"] == 1
                and isinstance(last_cycle, dict)
                and last_cycle.get("evaluated_item_count") == 2
            )

        wait_for(monitoring_cycle_covers_both_positions)

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        autonomous = runtime_status.json()["liveness"]["autonomous"]["monitoring"]
        price_source = autonomous["price_source"]
        diagnostics = price_source["diagnostics"]

        assert autonomous["last_cycle"]["price_count"] == 1
        # The missing-quote item is still evaluated into a safe no-action result;
        # partial price failure must not make it disappear from cycle coverage.
        assert autonomous["last_cycle"]["evaluated_item_count"] == 2
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


def test_kis_price_source_skips_new_quote_batch_while_timed_out_workers_are_still_running(
    monkeypatch,
) -> None:
    entered_quote = threading.Event()
    release_quote = threading.Event()
    quote_attempts = 0

    class BlockingQuoteAdapter(LiveMarketContextAdapter):
        def get_current_price(
            self,
            access_token: str,
            *,
            symbol: str,
            exchange: str = "KRX",
        ) -> BrokerCurrentPrice | None:
            nonlocal quote_attempts
            quote_attempts += 1
            entered_quote.set()
            assert release_quote.wait(timeout=5)
            return super().get_current_price(access_token, symbol=symbol, exchange=exchange)

    monkeypatch.setattr(
        monitoring_price_source_module,
        "QUOTE_BATCH_TIMEOUT_BUFFER_SECONDS",
        0.01,
    )
    broker_adapter = BlockingQuoteAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_prices={"005930": Decimal("125100.00")},
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            kis_request_timeout_seconds=1,
        ),
        broker_adapter=broker_adapter,
    )
    price_source = KisCurrentQuoteMonitoringPriceSource(
        settings=app.state.settings,
        session_factory=app.state.db_session_factory,
        broker_adapter_factory=lambda _settings: broker_adapter,
    )
    first_prices: list[MonitoringPriceRequest] = []
    first_thread = threading.Thread(
        target=lambda: first_prices.extend(
            price_source.get_prices(
                symbols=["005930"],
                observed_at=datetime(2026, 8, 10, 1, 5, tzinfo=UTC),
                settings=app.state.settings,
            )
        )
    )
    first_thread.start()
    try:
        assert entered_quote.wait(timeout=1)
        first_thread.join(timeout=2)
        assert not first_thread.is_alive()
        assert first_prices == []
        assert price_source.get_last_diagnostics()["quote_batch_timed_out"] is True

        second_prices = price_source.get_prices(
            symbols=["005930"],
            observed_at=datetime(2026, 8, 10, 1, 6, tzinfo=UTC),
            settings=app.state.settings,
        )

        assert second_prices == []
        diagnostics = price_source.get_last_diagnostics()
        assert diagnostics["quote_batch_in_flight"] is True
        assert diagnostics["quote_failure_count"] == 1
        assert quote_attempts == 1
    finally:
        release_quote.set()
        first_thread.join(timeout=1)
        price_source.close()
        engine.dispose()
