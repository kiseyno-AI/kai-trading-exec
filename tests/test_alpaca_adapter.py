from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import threading

import pytest
from fastapi.testclient import TestClient

from app.broker import alpaca as alpaca_module
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
from app.services.broker_orders import (
    _validate_alpaca_crypto_guardrails,
    _validate_provider_capability,
    place_broker_order,
)
from app.services.position_identity import (
    build_authenticated_broker_position_identity,
    execution_scope_payload,
)
from app.services.reconciliation import (
    _parse_fills_from_payload,
    _upsert_order_fills,
    reconcile_broker_order,
)
from app.services.runtime import RuntimeExecutionScope, monitoring_payload


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


async def _wait_for_thread_event(event: threading.Event) -> None:
    async with asyncio.timeout(1):
        while not event.is_set():
            await asyncio.sleep(0.001)


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


def _active_account_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": "stable-alpaca-account-id",
        "status": "ACTIVE",
        "crypto_status": "ACTIVE",
        "account_blocked": False,
        "trading_blocked": False,
        "trade_suspended_by_user": False,
        "cash": "1000",
        "non_marginable_buying_power": "1000",
        "portfolio_value": "1000",
        "last_equity": "1000",
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    "raw_body",
    [
        (
            '{"code":"40110000","message":'
            '"APCA-API-SECRET-KEY=upstream-secret-marker",'
            '"authorization":"Bearer upstream-secret-marker"}'
        ),
        "<html>APCA-API-SECRET-KEY=upstream-secret-marker</html>",
    ],
)
def test_alpaca_http_error_never_retains_untrusted_error_text(raw_body: str) -> None:
    parsed = alpaca_module._parse_json(raw_body)
    error = alpaca_module._alpaca_http_error(
        status_code=401,
        headers={
            "x-request-id": "upstream-secret-marker",
            "set-cookie": "session=upstream-secret-marker",
        },
        payload=parsed,
    )

    durable_shape = {
        "message": str(error),
        "response_metadata": error.response_metadata,
        "response_payload": error.response_payload,
    }
    assert "upstream-secret-marker" not in repr(durable_shape)
    assert str(error).startswith("broker_error_details_redacted[")
    assert error.classification == BrokerErrorClassification.AUTH
    assert error.code == ("40110000" if raw_body.startswith("{") else None)
    assert error.response_metadata["provider"] == "alpaca"
    assert error.response_metadata["request_id"].startswith(
        "broker_error_details_redacted["
    )


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


def test_us_equity_authentication_does_not_require_active_crypto_status() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(
            status_code=200,
            headers={},
            payload={
                "status": "ACTIVE",
                "crypto_status": "INACTIVE",
                "account_blocked": False,
                "trading_blocked": False,
                "trade_suspended_by_user": False,
            },
        )
    ])
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    grant = adapter.authenticate_us_equity()

    assert grant.token_type == "ApiKey"
    assert grant.access_token == "alpaca:paper:validated"


def test_account_snapshot_exposes_transient_account_id_but_redacts_payload() -> None:
    client = StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload={
                    "id": "stable-alpaca-account-id",
                    "account_number": "raw-alpaca-account-number",
                    "status": "ACTIVE",
                    "crypto_status": "ACTIVE",
                    "account_blocked": False,
                    "trading_blocked": False,
                    "trade_suspended_by_user": False,
                    "cash": "1000",
                    "non_marginable_buying_power": "1000",
                    "portfolio_value": "1000",
                    "last_equity": "1000",
                },
            ),
            AlpacaHttpResponse(status_code=200, headers={}, payload=[]),
            AlpacaHttpResponse(status_code=200, headers={}, payload=[]),
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    snapshot = adapter.get_account_snapshot("validated")

    assert snapshot.broker_account_id == "stable-alpaca-account-id"
    assert "stable-alpaca-account-id" not in repr(snapshot)
    assert "stable-alpaca-account-id" not in str(snapshot.payload)
    assert "raw-alpaca-account-number" not in str(snapshot.payload)


def test_generic_account_snapshot_routes_us_equity_truth_by_exchange() -> None:
    client = StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload={
                    "id": "stable-alpaca-account-id",
                    "status": "ACTIVE",
                    "crypto_status": "INACTIVE",
                    "account_blocked": False,
                    "trading_blocked": False,
                    "trade_suspended_by_user": False,
                    "cash": "1000",
                    "non_marginable_buying_power": "1000",
                    "portfolio_value": "1000",
                    "last_equity": "1000",
                },
            ),
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload=[
                    {
                        "symbol": "AAPL",
                        "asset_class": "us_equity",
                        "qty": "1.5",
                        "avg_entry_price": "200",
                    },
                    {
                        "symbol": "BTCUSD",
                        "asset_class": "crypto",
                        "qty": "0.1",
                        "avg_entry_price": "60000",
                    },
                ],
            ),
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload=[
                    {
                        "id": "aapl-open-buy",
                        "symbol": "AAPL",
                        "asset_class": "us_equity",
                        "side": "buy",
                        "qty": "1",
                        "filled_qty": "0",
                        "limit_price": "190",
                    },
                    {
                        "id": "btc-open-buy",
                        "symbol": "BTCUSD",
                        "asset_class": "crypto",
                        "side": "buy",
                        "qty": "0.1",
                        "filled_qty": "0",
                        "limit_price": "59000",
                    },
                ],
            ),
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    snapshot = adapter.get_account_snapshot(
        "validated",
        symbols=["AAPL"],
        exchange="US",
    )

    assert [holding.symbol for holding in snapshot.holdings] == ["AAPL"]
    assert [order.symbol for order in snapshot.open_orders] == ["AAPL"]
    assert snapshot.payload["asset_class"] == "us_equity"
    assert client.calls[2]["params"] == {
        "status": "open",
        "asset_class": "us_equity",
        "limit": 500,
    }


def test_snapshot_explicit_asset_class_prevents_cross_class_duplicates() -> None:
    positions = [
        {"symbol": "P/USD", "asset_class": "us_equity", "qty": "1"},
        {"symbol": "AAPL", "asset_class": "crypto", "qty": "0.5"},
    ]
    orders = [
        {
            "id": "explicit-equity",
            "symbol": "P/USD",
            "asset_class": "us_equity",
            "side": "buy",
            "status": "new",
            "qty": "1",
            "filled_qty": "0",
        },
        {
            "id": "explicit-crypto",
            "symbol": "AAPL",
            "asset_class": "crypto",
            "side": "buy",
            "status": "new",
            "qty": "0.5",
            "filled_qty": "0",
        },
    ]

    crypto_holdings, _, crypto_holdings_complete = alpaca_module._snapshot_holdings(
        positions,
        asset_class="crypto",
        allowed=None,
    )
    equity_holdings, _, equity_holdings_complete = alpaca_module._snapshot_holdings(
        positions,
        asset_class="us_equity",
        allowed=None,
    )
    crypto_orders, _, crypto_orders_complete = alpaca_module._snapshot_open_orders(
        orders,
        asset_class="crypto",
        allowed=None,
    )
    equity_orders, _, equity_orders_complete = alpaca_module._snapshot_open_orders(
        orders,
        asset_class="us_equity",
        allowed=None,
    )

    assert [holding.symbol for holding in crypto_holdings] == ["AAPL"]
    assert [holding.symbol for holding in equity_holdings] == ["P/USD"]
    assert [order.broker_order_id for order in crypto_orders] == ["explicit-crypto"]
    assert [order.broker_order_id for order in equity_orders] == ["explicit-equity"]
    assert all(
        (
            crypto_holdings_complete,
            equity_holdings_complete,
            crypto_orders_complete,
            equity_orders_complete,
        )
    )


def test_us_equity_snapshot_preserves_ticker_ending_in_usd() -> None:
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(200, {}, _active_account_payload()),
                AlpacaHttpResponse(200, {}, []),
                AlpacaHttpResponse(
                    200,
                    {},
                    [
                        {
                            "id": "pusd-open-buy",
                            "symbol": "PUSD",
                            "asset_class": "us_equity",
                            "side": "buy",
                            "status": "new",
                            "qty": "1",
                            "filled_qty": "0",
                        }
                    ],
                ),
            ]
        ),
    )

    snapshot = adapter.get_account_snapshot(
        "validated",
        symbols=["PUSD"],
        exchange="US",
    )

    assert snapshot.open_orders_complete is True
    assert [order.symbol for order in snapshot.open_orders] == ["PUSD"]


@pytest.mark.parametrize(
    ("exchange", "asset_class", "symbol"),
    [
        ("US", "us_equity", "AAPL"),
        ("ALPACA", "crypto", "BTCUSD"),
    ],
)
def test_account_snapshot_marks_open_order_page_limit_as_incomplete(
    exchange: str,
    asset_class: str,
    symbol: str,
) -> None:
    account = {
        "id": "stable-alpaca-account-id",
        "status": "ACTIVE",
        "crypto_status": "ACTIVE",
        "account_blocked": False,
        "trading_blocked": False,
        "trade_suspended_by_user": False,
        "cash": "1000",
        "non_marginable_buying_power": "1000",
        "portfolio_value": "1000",
        "last_equity": "1000",
    }
    raw_orders = [
        {
            "id": f"open-order-{index}",
            "symbol": symbol,
            "asset_class": asset_class,
            "side": "buy",
            "qty": "0.1",
            "filled_qty": "0",
            "limit_price": "100",
        }
        for index in range(500)
    ]
    client = StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(status_code=200, headers={}, payload=account),
            AlpacaHttpResponse(status_code=200, headers={}, payload=[]),
            AlpacaHttpResponse(status_code=200, headers={}, payload=raw_orders),
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    snapshot = adapter.get_account_snapshot("validated", exchange=exchange)

    assert len(snapshot.open_orders) == 500
    assert snapshot.payload["open_orders_coverage"] == "incomplete"
    assert snapshot.payload["open_orders_error"] == {
        "code": "alpaca_open_orders_page_limit_reached",
        "degraded": True,
        "limit": 500,
        "returned_count": 500,
    }


def test_account_snapshot_marks_corrupt_duplicate_and_malformed_rows_incomplete() -> None:
    positions: list[object] = [
        {"symbol": "BTCUSD", "asset_class": "crypto", "qty": "0.5"},
        {"symbol": "BTC/USD", "asset_class": "crypto", "qty": "0.25"},
        "corrupt-position-row",
        {"asset_class": "crypto", "qty": "0.1"},
        {"symbol": "ETHUSD", "asset_class": "crypto", "qty": "not-a-number"},
        {"symbol": "DOGEUSD", "qty": "1"},
        {"symbol": "SOLUSD", "asset_class": "crypto", "qty": "0"},
    ]
    orders: list[object] = [
        {
            "id": "open-order-1",
            "symbol": "BTCUSD",
            "asset_class": "crypto",
            "side": "buy",
            "status": "new",
            "qty": "0.2",
            "filled_qty": "0.1",
        },
        {
            "id": "open-order-1",
            "symbol": "BTC/USD",
            "asset_class": "crypto",
            "side": "buy",
            "status": "partially_filled",
            "qty": "0.3",
            "filled_qty": "0.1",
        },
        "corrupt-order-row",
        {
            "symbol": "ETHUSD",
            "asset_class": "crypto",
            "side": "sell",
            "status": "new",
            "qty": "1",
            "filled_qty": "0",
        },
        {
            "id": "invalid-quantity",
            "symbol": "ETHUSD",
            "asset_class": "crypto",
            "side": "sell",
            "status": "new",
            "qty": "not-a-number",
            "filled_qty": "0",
        },
        {
            "id": "filled-order",
            "symbol": "ETHUSD",
            "asset_class": "crypto",
            "side": "sell",
            "status": "filled",
            "qty": "1",
            "filled_qty": "1",
        },
        {
            "id": "missing-asset-class",
            "symbol": "DOGEUSD",
            "side": "buy",
            "status": "new",
            "qty": "1",
            "filled_qty": "0",
        },
        {
            "id": "zero-order",
            "symbol": "ETHUSD",
            "asset_class": "crypto",
            "side": "sell",
            "status": "new",
            "qty": "0",
            "filled_qty": "0",
        },
    ]
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(200, {}, _active_account_payload()),
                AlpacaHttpResponse(200, {}, positions),
                AlpacaHttpResponse(200, {}, orders),
            ]
        ),
    )

    snapshot = adapter.get_account_snapshot("validated")

    assert [holding.symbol for holding in snapshot.holdings] == [
        "BTC/USD",
        "BTC/USD",
    ]
    assert [order.broker_order_id for order in snapshot.open_orders] == [
        "open-order-1",
        "open-order-1",
    ]
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.payload["holdings_coverage"] == "incomplete"
    assert snapshot.payload["open_orders_coverage"] == "incomplete"
    assert snapshot.diagnostics["holdings_parse"] == {
        "raw_row_count": 7,
        "parsed_row_count": 2,
        "omitted_row_count": 1,
        "invalid_row_count": 4,
        "duplicate_identity_count": 1,
    }
    assert snapshot.diagnostics["open_orders_parse"] == {
        "raw_row_count": 8,
        "parsed_row_count": 2,
        "omitted_row_count": 2,
        "invalid_row_count": 4,
        "duplicate_identity_count": 1,
    }


@pytest.mark.parametrize("account_id", [None, "", 123, {"unexpected": "object"}])
def test_account_snapshot_marks_malformed_account_identity_incomplete(
    account_id: object,
) -> None:
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(
                    200,
                    {},
                    _active_account_payload(id=account_id),
                ),
                AlpacaHttpResponse(200, {}, []),
                AlpacaHttpResponse(200, {}, []),
            ]
        ),
    )

    snapshot = adapter.get_account_snapshot("validated")

    assert snapshot.broker_account_id is None
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.diagnostics["account_identity"] == {
        "complete": False,
        "reason": "missing_or_malformed_account_id",
    }


def test_broker_account_id_uses_only_the_minimal_account_endpoint() -> None:
    client = StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload={"id": "stable-alpaca-account-id"},
            )
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=_settings(), http_client=client)

    account_id = adapter.get_broker_account_id("validated")

    assert account_id == "stable-alpaca-account-id"
    assert [call["url"] for call in client.calls] == [
        "https://paper-api.alpaca.markets/v2/account"
    ]


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


def test_us_equity_order_never_uses_crypto_pair_symbol_normalization() -> None:
    client = StubAlpacaHttpClient([
        AlpacaHttpResponse(200, {}, {"id": "pusd-stock-order", "status": "accepted"})
    ])
    adapter = AlpacaBrokerAdapter(
        settings=_settings(alpaca_us_equity_allowed_symbols="PUSD"),
        http_client=client,
    )

    placement = adapter.place_order(
        "validated",
        _order(
            symbol="PUSD",
            asset_class="us_equity",
            quote_notional=Decimal("50"),
            time_in_force="day",
            exchange="US",
        ),
    )

    assert placement.broker_order_id == "pusd-stock-order"
    assert client.calls[0]["payload"]["symbol"] == "PUSD"


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


def test_live_us_equity_manual_buy_requires_internal_strategy_authority() -> None:
    settings = _settings(
        runtime_mode="live",
        alpaca_live_trading_enabled=True,
        alpaca_us_equity_strategy_live_enabled=False,
    )
    adapter = AlpacaBrokerAdapter(settings=settings)
    command = PlaceOrderRequest(
        idempotency_key="alpaca-us-equity-manual-smoke-test",
        provider="alpaca",
        asset_class="us_equity",
        venue="alpaca",
        exchange="US",
        symbol="SPY",
        side="buy",
        order_type="market",
        quote_notional=Decimal("1"),
        time_in_force="day",
    )

    assert _validate_alpaca_crypto_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=None,
        request_source="post:/orders",
    ) == "Alpaca buys require internal strategy risk authority"


def test_live_us_equity_strategy_gate_blocks_trusted_strategy_order() -> None:
    settings = _settings(
        runtime_mode="live",
        alpaca_live_trading_enabled=True,
        alpaca_us_equity_strategy_live_enabled=False,
    )
    adapter = AlpacaBrokerAdapter(settings=settings)
    command = PlaceOrderRequest(
        idempotency_key="alpaca-us-equity-strategy-test",
        provider="alpaca",
        asset_class="us_equity",
        venue="alpaca",
        exchange="US",
        symbol="SPY",
        side="buy",
        order_type="market",
        quote_notional=Decimal("1"),
        time_in_force="day",
        metadata={
            "order_origin": "alpaca_us_equity_strategy",
            "autonomous_strategy_order": True,
        },
    )

    assert _validate_alpaca_crypto_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=None,
        request_source="alpaca_us_equity_strategy:daily_rebalance",
    ) == "Alpaca US-equity strategy live trading is disabled by configuration"


def test_alpaca_strategy_order_fails_closed_without_trusted_origin_metadata() -> None:
    settings = _settings(
        runtime_mode="live",
        alpaca_live_trading_enabled=True,
        alpaca_us_equity_strategy_live_enabled=True,
    )
    adapter = AlpacaBrokerAdapter(settings=settings)
    command = PlaceOrderRequest(
        idempotency_key="alpaca-us-equity-strategy-missing-origin",
        provider="alpaca",
        asset_class="us_equity",
        venue="alpaca",
        exchange="US",
        symbol="SPY",
        side="buy",
        order_type="market",
        quote_notional=Decimal("1"),
        time_in_force="day",
    )

    assert _validate_alpaca_crypto_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=None,
        request_source="alpaca_us_equity_strategy:daily_rebalance",
    ) == "Alpaca autonomous strategy orders must carry explicit strategy origin metadata"


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


@pytest.mark.parametrize(
    ("response_id", "response_symbol", "expected_symbol"),
    [
        (None, "BTCUSD", "BTC/USD"),
        ("different-order", "BTCUSD", "BTC/USD"),
        ("broker-order-1", "ETHUSD", "BTC/USD"),
        ("broker-order-1", None, "BTC/USD"),
    ],
)
def test_reconcile_rejects_mismatched_order_identity(
    response_id: object,
    response_symbol: object,
    expected_symbol: str,
) -> None:
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(
                    status_code=200,
                    headers={},
                    payload={
                        "id": response_id,
                        "symbol": response_symbol,
                        "status": "filled",
                        "filled_qty": "0.1",
                    },
                )
            ]
        ),
    )

    with pytest.raises(BrokerAdapterError) as exc_info:
        adapter.reconcile_order(
            "validated",
            "broker-order-1",
            expected_symbol=expected_symbol,
        )

    assert exc_info.value.code == "alpaca_reconcile_identity_mismatch"
    assert exc_info.value.retryable is True


def test_reconcile_us_equity_never_aliases_a_crypto_pair_symbol() -> None:
    adapter = AlpacaBrokerAdapter(
        settings=_settings(),
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(
                    status_code=200,
                    headers={},
                    payload={
                        "id": "broker-stock-order-1",
                        "symbol": "P/USD",
                        "status": "filled",
                        "filled_qty": "1",
                    },
                )
            ]
        ),
    )

    with pytest.raises(BrokerAdapterError) as exc_info:
        adapter.reconcile_order(
            "validated",
            "broker-stock-order-1",
            exchange="US",
            expected_symbol="PUSD",
        )

    assert exc_info.value.code == "alpaca_reconcile_identity_mismatch"


@pytest.mark.parametrize(
    ("persisted_symbol", "broker_symbol"),
    [
        ("BTCUSD", "BTC/USD"),
        ("BTC-USD", "BTCUSD"),
        ("BTC/USD", "BTC-USD"),
    ],
)
def test_reconciliation_service_accepts_same_alpaca_crypto_instrument_aliases(
    persisted_symbol: str,
    broker_symbol: str,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        account_drawdown_recovery_scope_secret="stable-test-scope-secret",
    )
    account_id = "stable-alpaca-account-id"
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id=account_id,
    )
    adapter = AlpacaBrokerAdapter(
        settings=settings,
        http_client=StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(200, {}, _active_account_payload(id=account_id)),
                AlpacaHttpResponse(200, {}, _active_account_payload(id=account_id)),
                AlpacaHttpResponse(
                    200,
                    {},
                    {
                        "id": "broker-crypto-alias-order",
                        "symbol": broker_symbol,
                        "status": "new",
                        "filled_qty": "0",
                    },
                ),
            ]
        ),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as db:
        order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key=f"crypto-alias:{persisted_symbol}",
            broker_order_id="broker-crypto-alias-order",
            symbol=persisted_symbol,
            side="buy",
            status="submitted",
            base_quantity=Decimal("0.1"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
            request_metadata={
                "execution_scope": execution_scope_payload(
                    identity=identity,
                    venue="alpaca",
                    asset_class="crypto",
                )
            },
        )
        db.add(order)
        db.commit()

        result = reconcile_broker_order(
            settings=settings,
            db=db,
            adapter=adapter,
            order=order,
        )

        assert result["status_code"] == 200
        assert result["status"] == "open"
        assert order.last_error_code is None

    engine.dispose()


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


def test_internal_strategy_persists_guarded_alpaca_crypto_order() -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        automation_enabled=True,
        account_drawdown_gate_enabled=False,
    )
    account_payload = {
        "id": "stable-alpaca-account-id",
        "status": "ACTIVE",
        "crypto_status": "ACTIVE",
        "account_blocked": False,
        "trading_blocked": False,
        "trade_suspended_by_user": False,
        "cash": "1000",
        "non_marginable_buying_power": "1000",
        "portfolio_value": "1000",
        "last_equity": "1000",
    }
    http_client = StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(200, {}, account_payload),
            AlpacaHttpResponse(
                200,
                {},
                {"id": "stable-alpaca-account-id"},
            ),
            AlpacaHttpResponse(200, {}, account_payload),
            AlpacaHttpResponse(200, {}, []),
            AlpacaHttpResponse(200, {}, []),
            AlpacaHttpResponse(
                200,
                {},
                {"id": "stable-alpaca-account-id"},
            ),
            AlpacaHttpResponse(
                200,
                {},
                {"id": "broker-order-api-1", "status": "accepted"},
            ),
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=http_client)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(settings=settings, engine=engine, session_factory=session_factory)
    expected_scope = execution_scope_payload(
        identity=build_authenticated_broker_position_identity(
            settings=settings,
            provider="alpaca",
            exchange="ALPACA",
            broker_account_id="stable-alpaca-account-id",
        ),
        venue="alpaca",
        asset_class="crypto",
    )
    command = PlaceOrderRequest(
        idempotency_key="alpaca-api-buy-1",
        provider="alpaca",
        asset_class="crypto",
        venue="alpaca",
        symbol="BTC/USD",
        side="buy",
        order_type="market",
        time_in_force="gtc",
        exchange="ALPACA",
        quote_notional=Decimal("25.50"),
        metadata={
            "autonomous_strategy_order": True,
            "order_origin": "alpaca_crypto_strategy",
            "expected_execution_scope": expected_scope,
        },
    )

    with TestClient(app) as client:
        assert client.post(
            "/controls/resume",
            json={"actor": "test", "source": "test", "reason": "paper order test"},
        ).status_code == 200
        with session_factory() as db:
            result = place_broker_order(
                settings=settings,
                db=db,
                adapter=adapter,
                command=command,
                request_source="alpaca_crypto_strategy:daily_rebalance",
            )

    assert result.status_code == 201
    assert result.payload["order"]["broker_name"] == "alpaca"
    with session_factory() as db:
        order = db.query(BrokerOrder).filter_by(idempotency_key="alpaca-api-buy-1").one()
        assert order.quote_notional == Decimal("25.5000")
        assert order.broker_order_id == "broker-order-api-1"
        assert order.request_metadata["execution_scope"] == {
            "policy_version": 1,
            "provider": "alpaca",
            "account_scope": order.request_metadata["execution_scope"]["account_scope"],
            "venue": "ALPACA",
            "asset_class": "crypto",
            "exchange": "ALPACA",
        }
        assert len(order.request_metadata["execution_scope"]["account_scope"]) == 24
        assert "stable-alpaca-account-id" not in str(order.request_metadata)
    assert [call["url"] for call in http_client.calls] == [
        "https://paper-api.alpaca.markets/v2/account",
        "https://paper-api.alpaca.markets/v2/account",
        "https://paper-api.alpaca.markets/v2/account",
        "https://paper-api.alpaca.markets/v2/positions",
        "https://paper-api.alpaca.markets/v2/orders",
        "https://paper-api.alpaca.markets/v2/account",
        "https://paper-api.alpaca.markets/v2/orders",
    ]


def test_live_order_fails_before_placement_when_account_scope_is_unavailable(
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        automation_enabled=True,
        account_drawdown_gate_enabled=False,
        alpaca_live_trading_enabled=True,
        account_drawdown_recovery_scope_secret=(
            "stable-alpaca-execution-scope-secret-32-bytes"
        ),
    )
    http_client = StubAlpacaHttpClient(
        [
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
            AlpacaHttpResponse(status_code=200, headers={}, payload={"id": "  "}),
            AlpacaHttpResponse(status_code=200, headers={}, payload={"id": "  "}),
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=http_client)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(settings=settings, engine=engine, session_factory=session_factory)
    expected_scope = execution_scope_payload(
        identity=build_authenticated_broker_position_identity(
            settings=settings,
            provider="alpaca",
            exchange="ALPACA",
            broker_account_id="stable-alpaca-account-id",
        ),
        venue="alpaca",
        asset_class="crypto",
    )
    command = PlaceOrderRequest(
        idempotency_key="alpaca-live-missing-scope-1",
        provider="alpaca",
        asset_class="crypto",
        venue="alpaca",
        symbol="BTC/USD",
        side="buy",
        order_type="market",
        time_in_force="gtc",
        exchange="ALPACA",
        quote_notional=Decimal("25.50"),
        metadata={
            "autonomous_strategy_order": True,
            "order_origin": "alpaca_crypto_strategy",
            "expected_execution_scope": expected_scope,
        },
    )

    with TestClient(app) as client:
        assert client.post(
            "/controls/resume",
            json={"actor": "test", "source": "test", "reason": "live scope test"},
        ).status_code == 200
        with session_factory() as db:
            result = place_broker_order(
                settings=settings,
                db=db,
                adapter=adapter,
                command=command,
                request_source="alpaca_crypto_strategy:daily_rebalance",
            )
            persisted = db.query(BrokerOrder).filter_by(
                idempotency_key=command.idempotency_key
            ).one_or_none()

    assert result.status_code == 409
    assert result.payload["block_reason"] == "execution_scope_indeterminate"
    assert persisted is None
    assert [call["url"] for call in http_client.calls] == [
        "https://api.alpaca.markets/v2/account",
        "https://api.alpaca.markets/v2/account",
        "https://api.alpaca.markets/v2/account",
    ]


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
        fills, anomalies = _parse_fills_from_payload(
            payload={
                "filled_base_quantity": "0.00037925",
                "average_fill_price": "67234.12",
                "filled_at": observed_at.isoformat(),
            },
            order=order,
            observed_at=observed_at,
        )

        assert anomalies == []
        assert _upsert_order_fills(db=db, order=order, fills=fills) == 1
        db.commit()
        db.refresh(order)
        assert order.fills[0].quantity == 0
        assert order.fills[0].base_quantity == Decimal("0.000379250000")
        assert order.fills[0].quote_notional == Decimal("25.4985")


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity"])
def test_alpaca_runtime_start_is_idempotent(runtime_kind: str) -> None:
    async def scenario() -> None:
        settings = _settings(
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
        )
        runtime = (
            AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
            if runtime_kind == "crypto"
            else AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        )
        runtime.run_cycle = lambda: {"status": "healthy"}  # type: ignore[method-assign]

        await runtime.start()
        first_task = runtime._task
        await runtime.start()

        assert first_task is not None
        assert runtime._task is first_task
        await runtime.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity"])
def test_alpaca_runtime_stop_drains_inflight_cycle_and_prevents_another_cycle(
    runtime_kind: str,
) -> None:
    async def scenario() -> None:
        settings = _settings(
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
        )
        runtime = (
            AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
            if runtime_kind == "crypto"
            else AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        )
        cycle_entered = threading.Event()
        release_cycle = threading.Event()
        cycle_completed = threading.Event()
        calls = 0

        def blocking_cycle() -> dict[str, object]:
            nonlocal calls
            calls += 1
            cycle_entered.set()
            try:
                assert release_cycle.wait(timeout=2)
                return {"status": "healthy"}
            finally:
                cycle_completed.set()

        runtime.run_cycle = blocking_cycle  # type: ignore[method-assign]
        await runtime.start()
        stop_task: asyncio.Task[None] | None = None
        restart_task: asyncio.Task[None] | None = None
        try:
            await _wait_for_thread_event(cycle_entered)
            stop_task = asyncio.create_task(runtime.stop())
            await asyncio.sleep(0.02)

            assert stop_task.done() is False
            assert cycle_completed.is_set() is False
            restart_task = asyncio.create_task(runtime.start())
            await asyncio.sleep(0.02)
            assert restart_task.done() is True
            with pytest.raises(RuntimeError, match="shutting down"):
                await restart_task

            release_cycle.set()
            await asyncio.wait_for(stop_task, timeout=1)
            await asyncio.sleep(0.02)

            assert cycle_completed.is_set() is True
            assert calls == 1
            assert runtime._task is None
        finally:
            release_cycle.set()
            if stop_task is not None:
                await asyncio.gather(stop_task, return_exceptions=True)
            if restart_task is not None:
                await asyncio.gather(restart_task, return_exceptions=True)
            await runtime.stop()

    asyncio.run(scenario())


def test_alpaca_runtime_stop_timeout_keeps_shutdown_fence_until_cycle_drains() -> None:
    async def scenario() -> None:
        settings = _settings(alpaca_crypto_monitoring_enabled=True)
        runtime = AlpacaCryptoRuntimeCoordinator(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
        )
        cycle_entered = threading.Event()
        release_cycle = threading.Event()

        def blocking_cycle() -> dict[str, object]:
            cycle_entered.set()
            assert release_cycle.wait(timeout=2)
            return {"status": "healthy"}

        runtime.run_cycle = blocking_cycle  # type: ignore[method-assign]
        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]
        await runtime.start()
        try:
            await _wait_for_thread_event(cycle_entered)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            assert release_cycle.is_set() is False
            assert runtime._stopping is True
            with pytest.raises(RuntimeError, match="shutting down"):
                await runtime.start()

            release_cycle.set()
            await runtime.wait_stopped()
            assert runtime._task is None
            assert runtime._stopping is False
        finally:
            release_cycle.set()
            await runtime.stop()

    asyncio.run(scenario())


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
            "id": "stable-alpaca-account-id",
            "status": "ACTIVE",
            "crypto_status": "ACTIVE",
            "account_blocked": False,
            "trading_blocked": False,
            "trade_suspended_by_user": False,
        }),
        # Position synchronization account, positions, and open orders.
        AlpacaHttpResponse(200, {}, {
            "id": "stable-alpaca-account-id",
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
        assert checkpoint.provider == "alpaca"
        assert checkpoint.account_scope is not None
        assert len(checkpoint.account_scope) == 24
        assert checkpoint.details["execution_scope"] == {
            "policy_version": 1,
            "provider": "alpaca",
            "account_scope": checkpoint.account_scope,
        }
        monitoring = monitoring_payload(
            settings=settings,
            db=db,
            provider="alpaca",
            runtime_scope=RuntimeExecutionScope(
                provider="alpaca",
                exchanges=("ALPACA", "US"),
                account_scope=checkpoint.account_scope,
                status="available",
                source="authenticated_broker_account",
                reason=None,
            ),
        )
        assert [worker["name"] for worker in monitoring["workers"]] == [
            "alpaca_crypto_24h"
        ]


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
            "id": "stable-alpaca-us-account-id",
            "status": "ACTIVE", "crypto_status": "INACTIVE", "account_blocked": False,
            "trading_blocked": False, "trade_suspended_by_user": False,
        }),
        AlpacaHttpResponse(200, {}, {
            "id": "stable-alpaca-us-account-id",
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
        # The strategy independently authenticates current account identity even
        # while the market is closed before accepting a scoped success.
        AlpacaHttpResponse(200, {}, {
            "id": "stable-alpaca-us-account-id",
            "status": "ACTIVE", "account_blocked": False, "trading_blocked": False,
            "trade_suspended_by_user": False, "cash": "100000",
            "non_marginable_buying_power": "100000", "portfolio_value": "100000",
            "last_equity": "100000",
        }),
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
    ]
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=StubAlpacaHttpClient(responses))
    monkeypatch.setattr(us_equity_runtime_module, "build_broker_adapter", lambda *_args, **_kwargs: adapter)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    runtime = AlpacaUsEquityRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
    )

    result = runtime.run_cycle()

    assert result["status"] == "healthy"
    assert result["market_clock"]["is_open"] is False  # type: ignore[index]
    assert result["strategy"]["status"] == "market_closed"  # type: ignore[index]
    assert set(result["prices"]) == {"SPY", "QQQ"}
    assert runtime.snapshot()["strategy_live_order_submission_enabled"] is False
    with session_factory() as db:
        checkpoint = db.query(MonitoringCheckpoint).filter_by(
            worker_name="alpaca_us_equity",
            mode="paper",
        ).one()
        assert checkpoint.provider == "alpaca"
        assert checkpoint.account_scope is not None
        assert checkpoint.details["execution_scope"] == {
            "policy_version": 1,
            "provider": "alpaca",
            "account_scope": checkpoint.account_scope,
        }
        monitoring = monitoring_payload(
            settings=settings,
            db=db,
            provider="alpaca",
            runtime_scope=RuntimeExecutionScope(
                provider="alpaca",
                exchanges=("ALPACA", "US"),
                account_scope=checkpoint.account_scope,
                status="available",
                source="authenticated_broker_account",
                reason=None,
            ),
        )
        assert [worker["name"] for worker in monitoring["workers"]] == [
            "alpaca_us_equity"
        ]


def test_us_equity_runtime_clears_cached_scope_when_closed_account_rotates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        alpaca_us_equity_monitoring_enabled=True,
        alpaca_us_equity_strategy_enabled=True,
        alpaca_us_equity_reconcile_interval_seconds=3600,
    )
    account_a = {
        "id": "alpaca-account-a",
        "status": "ACTIVE",
        "crypto_status": "INACTIVE",
        "account_blocked": False,
        "trading_blocked": False,
        "trade_suspended_by_user": False,
        "cash": "100000",
        "non_marginable_buying_power": "100000",
        "portfolio_value": "100000",
        "last_equity": "100000",
    }
    account_b = {**account_a, "id": "alpaca-account-b"}
    closed_clock = AlpacaHttpResponse(200, {}, {
        "is_open": False,
        "timestamp": "2026-08-16T12:00:00Z",
        "next_open": "2026-08-17T09:30:00-04:00",
        "next_close": "2026-08-17T16:00:00-04:00",
    })
    qqq_quote = AlpacaHttpResponse(
        200,
        {},
        {"quotes": {"QQQ": {"bp": "570", "ap": "571", "t": "2026-08-15T19:59:59Z"}}},
    )
    spy_quote = AlpacaHttpResponse(
        200,
        {},
        {"quotes": {"SPY": {"bp": "640", "ap": "641", "t": "2026-08-15T19:59:59Z"}}},
    )
    responses = [
        AlpacaHttpResponse(200, {}, account_a),  # authenticate
        AlpacaHttpResponse(200, {}, account_a),  # scoped position sync
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
        closed_clock,
        qqq_quote,
        spy_quote,
        AlpacaHttpResponse(200, {}, account_a),  # strategy identity check
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
        closed_clock,
        qqq_quote,
        spy_quote,
        AlpacaHttpResponse(200, {}, account_b),  # credentials now resolve to B
        AlpacaHttpResponse(200, {}, []),
        AlpacaHttpResponse(200, {}, []),
    ]
    adapter = AlpacaBrokerAdapter(
        settings=settings,
        http_client=StubAlpacaHttpClient(responses),
    )
    monkeypatch.setattr(
        us_equity_runtime_module,
        "build_broker_adapter",
        lambda *_args, **_kwargs: adapter,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    runtime = AlpacaUsEquityRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
    )

    first = runtime.run_cycle()
    second = runtime.run_cycle()

    assert first["status"] == "healthy"
    assert first["strategy"]["status"] == "market_closed"  # type: ignore[index]
    assert first["execution_scope"] is not None
    assert second["status"] == "degraded"
    assert second["strategy"] == {  # type: ignore[index]
        "enabled": True,
        "status": "blocked",
        "reason": "authenticated_execution_scope_mismatch",
    }
    assert second["execution_scope"] is None
    assert second["execution_scope_status"] == "unavailable"
    assert runtime._execution_scope is None
