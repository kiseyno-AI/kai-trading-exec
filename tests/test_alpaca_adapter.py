from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import app.api.dependencies as api_dependencies
import app.services.alpaca_crypto_runtime as alpaca_runtime_module
import app.services.alpaca_us_equity_runtime as us_equity_runtime_module
from app.api.schemas.execution import PlaceOrderRequest
from app.broker.alpaca import AlpacaBrokerAdapter, AlpacaHttpResponse
from app.broker.base import (
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerOrderPlacementRequest,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import BrokerOrder, MonitoringCheckpoint
from app.services.alpaca_crypto_runtime import AlpacaCryptoRuntimeCoordinator
from app.services.alpaca_us_equity_runtime import AlpacaUsEquityRuntimeCoordinator
from app.services.broker_orders import _validate_provider_capability
from app.services.reconciliation import _parse_fills_from_payload, _upsert_order_fills


class StubAlpacaHttpClient:
    def __init__(self, responses: list[AlpacaHttpResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def request_json(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "runtime_mode": "paper",
        "alpaca_api_key": "paper-key",
        "alpaca_api_secret": "paper-secret",
    }
    values.update(overrides)
    return Settings(
        **values,  # type: ignore[arg-type]
    )


def _order(**overrides: object) -> BrokerOrderPlacementRequest:
    values: dict[str, object] = {
        "idempotency_key": "alpaca-paper-buy-1",
        "symbol": "BTC/USD",
        "side": "buy",
        "quantity": None,
        "base_quantity": None,
        "quote_notional": Decimal("25.50"),
        "order_type": "market",
        "time_in_force": "gtc",
        "exchange": "ALPACA",
        "provider": "alpaca",
        "venue": "alpaca",
        "asset_class": "crypto",
    }
    values.update(overrides)
    return BrokerOrderPlacementRequest(**values)  # type: ignore[arg-type]


def test_authentication_uses_paper_endpoint_and_validates_crypto_status() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "status": "ACTIVE",
                "crypto_status": "ACTIVE",
                "account_blocked": False,
                "trading_blocked": False,
                "trade_suspended_by_user": False,
            },
        )
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    grant = adapter.authenticate()

    assert grant.token_type == "ApiKey"
    assert client.calls[0]["url"] == "https://paper-api.alpaca.markets/v2/account"
    assert client.calls[0]["headers"]["APCA-API-KEY-ID"] == "paper-key"  # type: ignore[index]


def test_market_buy_submits_decimal_notional_and_client_order_id() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={"x-request-id": "request-1"},
            payload={
                "id": "broker-order-1",
                "status": "accepted",
                "submitted_at": datetime(2026, 8, 16, tzinfo=UTC).isoformat(),
            },
        )
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    placement = adapter.place_order("validated", _order())

    assert placement.broker_order_id == "broker-order-1"
    assert client.calls[0]["payload"] == {
        "symbol": "BTC/USD",
        "side": "buy",
        "type": "market",
        "time_in_force": "gtc",
        "client_order_id": "alpaca-paper-buy-1",
        "notional": "25.50",
    }


def test_us_equity_market_buy_uses_fractional_notional_and_day_tif() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(200, {}, {"id": "stock-order-1", "status": "accepted"})
    ])
    settings = _settings(alpaca_us_equity_allowed_symbols="SPY,QQQ")
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=client)

    placement = adapter.place_order(
        "validated",
        _order(
            symbol="SPY",
            asset_class="us_equity",
            quote_notional=Decimal("50"),
            time_in_force="day",
            exchange="US",
        ),
    )

    assert placement.broker_order_id == "stock-order-1"
    assert placement.message == "Alpaca US-equity order accepted"
    assert client.calls[0]["payload"] == {
        "symbol": "SPY",
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "client_order_id": "alpaca-paper-buy-1",
        "notional": "50",
    }


@pytest.mark.parametrize(
    ("side", "sizing"),
    [
        ("buy", {"quote_notional": Decimal("50")}),
        ("sell", {"base_quantity": Decimal("0.25")}),
    ],
)
def test_us_equity_place_order_request_accepts_alpaca_fractional_sizing(
    side: str,
    sizing: dict[str, Decimal],
) -> None:
    request = PlaceOrderRequest(
        idempotency_key=f"alpaca-paper-us-equity-{side}-1",
        provider="alpaca",
        asset_class="us_equity",
        venue="alpaca",
        symbol="SPY",
        side=side,
        order_type="market",
        time_in_force="day",
        exchange="US",
        **sizing,
    )

    assert request.exchange == "US"
    assert request.asset_class == "us_equity"


def test_alpaca_provider_capability_accepts_us_equity_orders() -> None:
    adapter = AlpacaBrokerAdapter(settings=_settings())
    command = PlaceOrderRequest(
        idempotency_key="alpaca-us-equity-capability-test",
        provider="alpaca",
        asset_class="us_equity",
        venue="alpaca",
        exchange="US",
        symbol="SPY",
        side="buy",
        order_type="market",
        quote_notional=Decimal("50"),
        time_in_force="day",
    )

    assert _validate_provider_capability(adapter=adapter, command=command) is None


def test_duplicate_client_id_response_recovers_existing_order() -> None:
    client = StubAlpacaHttpClient([
        BrokerAdapterError(
            "client order id must be unique",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            code="42210000",
        ),
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "id": "broker-order-recovered",
                "status": "accepted",
                "client_order_id": "alpaca-paper-buy-1",
            },
        ),
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    placement = adapter.place_order("validated", _order())

    assert placement.broker_order_id == "broker-order-recovered"
    assert placement.response_metadata["recovered_after_uncertain_submit"] is True
    assert client.calls[1]["params"] == {"client_order_id": "alpaca-paper-buy-1"}


def test_reconcile_exposes_fractional_cumulative_fill() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "id": "broker-order-1",
                "status": "filled",
                "filled_qty": "0.00045678",
                "filled_avg_price": "67234.12",
                "filled_at": "2026-08-16T12:01:00Z",
            },
        )
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    payload = adapter.reconcile_order("validated", "broker-order-1")

    assert payload is not None
    assert payload["status"] == "filled"
    assert payload["filled_base_quantity"] == "0.00045678"
    assert payload["average_fill_price"] == "67234.12"


def test_daily_crypto_bars_paginates_symbol_keyed_response() -> None:
    later_btc_bar = {
        "t": "2026-08-15T00:00:00Z",
        "o": 65000,
        "h": 66000,
        "l": 64000,
        "c": 65500,
        "v": 12.5,
        "n": 200,
        "vw": 65250,
    }
    current_eth_bar = {
        "t": "2026-08-16T00:00:00Z",
        "o": 3100,
        "h": 3250,
        "l": 3050,
        "c": 3200,
        "v": 125.5,
        "n": 350,
        "vw": 3160,
    }
    earlier_btc_bar = {
        "t": "2026-08-14T00:00:00Z",
        "o": 65500,
        "h": 66500,
        "l": 64500,
        "c": 66000,
        "v": 13.5,
        "n": 210,
        "vw": 65750,
    }
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "bars": {
                    "BTC/USD": [later_btc_bar],
                    "ETH/USD": [current_eth_bar],
                },
                "next_page_token": "next-page",
            },
        ),
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "bars": {"BTC/USD": [earlier_btc_bar]},
                "next_page_token": None,
            },
        ),
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    bars = adapter.get_crypto_daily_bars(
        "validated",
        symbols=["btc-usd", "ETH/USD", "BTC/USD"],
        start=datetime(2026, 8, 14, tzinfo=UTC),
        end=datetime(2026, 8, 17, tzinfo=UTC),
        page_limit=2,
    )

    assert bars == {
        "BTC/USD": [earlier_btc_bar, later_btc_bar],
        "ETH/USD": [current_eth_bar],
    }
    assert client.calls[0]["url"] == (
        "https://data.alpaca.markets/v1beta3/crypto/us/bars"
    )
    assert client.calls[0]["params"] == {
        "symbols": "BTC/USD,ETH/USD",
        "timeframe": "1Day",
        "start": "2026-08-14T00:00:00Z",
        "end": "2026-08-17T00:00:00Z",
        "limit": 2,
        "sort": "asc",
        "page_token": None,
    }
    assert client.calls[1]["params"] == {
        **client.calls[0]["params"],  # type: ignore[dict-item]
        "page_token": "next-page",
    }
    assert bars["ETH/USD"][0]["t"] == "2026-08-16T00:00:00Z"


def test_stock_bars_and_latest_quote_use_iex_feed() -> None:
    bar = {"t": "2026-08-14T04:00:00Z", "o": 640, "h": 645, "l": 638, "c": 642, "v": 1000}
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(200, {}, {"bars": {"SPY": [bar]}, "next_page_token": None}),
        AlpacaHttpResponse(200, {}, {"quotes": {"SPY": {"bp": "641.9", "ap": "642.1", "t": "2026-08-14T19:59:59Z"}}}),
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    bars = adapter.get_stock_daily_bars(
        "validated",
        symbols=["spy"],
        start=datetime(2026, 8, 14, tzinfo=UTC),
        end=datetime(2026, 8, 15, tzinfo=UTC),
    )
    quote = adapter.get_stock_live_market_context("validated", symbol="spy")

    assert bars == {"SPY": [bar]}
    assert client.calls[0]["params"]["feed"] == "iex"  # type: ignore[index]
    assert quote is not None
    assert quote.bid == Decimal("641.9")
    assert quote.ask == Decimal("642.1")
    assert client.calls[1]["params"] == {"symbols": "SPY", "feed": "iex"}


def test_daily_crypto_bars_rejects_repeated_page_token() -> None:
    repeated_page = AlpacaHttpResponse(
        status_code=200,
        headers={},
        payload={"bars": {"BTC/USD": []}, "next_page_token": "same-page"},
    )
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient([repeated_page, repeated_page]),
    )

    with pytest.raises(BrokerAdapterError, match="repeated a page token"):
        adapter.get_crypto_daily_bars(
            "validated",
            symbols=["BTC/USD"],
            start=datetime(2026, 8, 14, tzinfo=UTC),
            end=datetime(2026, 8, 17, tzinfo=UTC),
        )


def test_live_order_is_fail_closed_until_explicitly_enabled() -> None:
    adapter = AlpacaBrokerAdapter(
        settings=_settings(runtime_mode="live"),
        http_client=StubAlpacaHttpClient([]),
    )

    with pytest.raises(BrokerAdapterError, match="live trading is disabled"):
        adapter.place_order("validated", _order())


def test_order_api_persists_guarded_alpaca_crypto_order(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        automation_enabled=True,
        account_drawdown_gate_enabled=False,
    )
    http_client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "status": "ACTIVE",
                "crypto_status": "ACTIVE",
                "account_blocked": False,
                "trading_blocked": False,
                "trade_suspended_by_user": False,
            },
        ),
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={"id": "broker-order-api-1", "status": "accepted"},
        ),
    ])
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=http_client)
    monkeypatch.setattr(
        api_dependencies,
        "build_broker_adapter",
        lambda _settings, *, provider: adapter,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(settings=settings, engine=engine, session_factory=session_factory)

    with TestClient(app) as client:
        assert client.post(
            "/controls/resume",
            json={"actor": "test", "source": "test", "reason": "paper order test"},
        ).status_code == 200
        response = client.post(
            "/orders",
            json={
                "idempotency_key": "alpaca-api-buy-1",
                "provider": "alpaca",
                "asset_class": "crypto",
                "venue": "alpaca",
                "symbol": "BTC/USD",
                "side": "buy",
                "order_type": "market",
                "time_in_force": "gtc",
                "exchange": "ALPACA",
                "quote_notional": "25.50",
            },
        )

    assert response.status_code == 201
    assert response.json()["order"]["broker_name"] == "alpaca"
    with session_factory() as db:
        order = db.query(BrokerOrder).filter_by(idempotency_key="alpaca-api-buy-1").one()
        assert order.quote_notional == Decimal("25.5000")
        assert order.broker_order_id == "broker-order-api-1"


def test_fractional_fill_is_persisted_without_integer_rounding() -> None:
    settings = _settings(database_url="sqlite:///:memory:")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    observed_at = datetime(2026, 8, 16, 12, 1, tzinfo=UTC)

    with session_factory() as db:
        order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="fractional-fill-1",
            broker_order_id="broker-fractional-1",
            symbol="BTC/USD",
            side="buy",
            status="submitted",
            quote_notional=Decimal("25.50"),
            venue="alpaca",
            asset_class="crypto",
        )
        db.add(order)
        db.flush()
        fills = _parse_fills_from_payload(
            payload={
                "filled_base_quantity": "0.00037925",
                "average_fill_price": "67234.12",
                "filled_at": observed_at.isoformat(),
            },
            order=order,
            observed_at=observed_at,
        )

        assert _upsert_order_fills(db=db, order=order, fills=fills) == 1
        db.commit()
        db.refresh(order)
        assert order.fills[0].quantity == 0
        assert order.fills[0].base_quantity == Decimal("0.000379250000")
        assert order.fills[0].quote_notional == Decimal("25.4985")


def test_24h_runtime_reconciles_and_checkpoints_without_market_hours_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        alpaca_crypto_monitoring_enabled=True,
    )
    responses = [
        # Session authentication.
        AlpacaHttpResponse(200, {}, {
            "status": "ACTIVE",
            "crypto_status": "ACTIVE",
            "account_blocked": False,
            "trading_blocked": False,
            "trade_suspended_by_user": False,
        }),
        # Position synchronization account, positions, and open orders.
        AlpacaHttpResponse(200, {}, {
            "status": "ACTIVE",
            "crypto_status": "ACTIVE",
            "account_blocked": False,
            "trading_blocked": False,
            "trade_suspended_by_user": False,
            "cash": "1000",
            "non_marginable_buying_power": "1000",
            "portfolio_value": "1000",
        }),
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
        # Quote polling is intentionally independent of exchange hours/weekends.
        AlpacaHttpResponse(200, {}, {"quotes": {"BTC/USD": {"bp": "66990", "ap": "67010", "t": "2026-08-16T12:00:00Z"}}}),
        AlpacaHttpResponse(200, {}, {"quotes": {"ETH/USD": {"bp": "3199", "ap": "3201", "t": "2026-08-16T12:00:01Z"}}}),
    ]
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=StubAlpacaHttpClient(responses))
    monkeypatch.setattr(
        alpaca_runtime_module,
        "build_broker_adapter",
        lambda _settings, *, provider: adapter,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    runtime = AlpacaCryptoRuntimeCoordinator(settings=settings, session_factory=session_factory)

    result = runtime.run_cycle()

    assert result["status"] == "healthy"
    assert result["prices"]["BTC/USD"]["price"] == "67000"  # type: ignore[index]
    assert runtime.snapshot()["status"] == "healthy"
    with session_factory() as db:
        checkpoint = db.query(MonitoringCheckpoint).filter_by(
            worker_name="alpaca_crypto_24h",
            mode="paper",
        ).one()
        assert checkpoint.status == "healthy"
        assert checkpoint.last_success_at is not None


def test_us_equity_runtime_monitors_while_market_closed_without_trading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        alpaca_us_equity_monitoring_enabled=True,
        alpaca_us_equity_strategy_enabled=True,
    )
    responses = [
        AlpacaHttpResponse(200, {}, {
            "status": "ACTIVE", "crypto_status": "ACTIVE", "account_blocked": False,
            "trading_blocked": False, "trade_suspended_by_user": False,
        }),
        AlpacaHttpResponse(200, {}, {
            "status": "ACTIVE", "account_blocked": False, "trading_blocked": False,
            "trade_suspended_by_user": False, "cash": "100000",
            "non_marginable_buying_power": "100000", "portfolio_value": "100000",
            "last_equity": "100000",
        }),
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, {
            "is_open": False, "timestamp": "2026-08-16T12:00:00Z",
            "next_open": "2026-08-17T09:30:00-04:00", "next_close": "2026-08-17T16:00:00-04:00",
        }),
        AlpacaHttpResponse(200, {}, {"quotes": {"QQQ": {"bp": "570", "ap": "571", "t": "2026-08-15T19:59:59Z"}}}),
        AlpacaHttpResponse(200, {}, {"quotes": {"SPY": {"bp": "640", "ap": "641", "t": "2026-08-15T19:59:59Z"}}}),
    ]
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=StubAlpacaHttpClient(responses))
    monkeypatch.setattr(us_equity_runtime_module, "build_broker_adapter", lambda *_args, **_kwargs: adapter)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    runtime = AlpacaUsEquityRuntimeCoordinator(
        settings=settings,
        session_factory=create_session_factory(engine),
    )

    result = runtime.run_cycle()

    assert result["status"] == "healthy"
    assert result["market_clock"]["is_open"] is False  # type: ignore[index]
    assert result["strategy"]["status"] == "market_closed"  # type: ignore[index]
    assert set(result["prices"]) == {"SPY", "QQQ"}
    assert runtime.snapshot()["strategy_live_order_submission_enabled"] is False
