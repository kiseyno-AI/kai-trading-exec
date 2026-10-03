from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerLiveMarketContext,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.broker.error_safety import (
    safe_broker_error_code,
    safe_broker_error_message,
    sanitize_broker_error_payload,
)

ALPACA_PROVIDER = "alpaca"
ALPACA_LIVE_API_BASE_URL = "https://api.alpaca.markets"
ALPACA_PAPER_API_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
ALPACA_ACCOUNT_PATH = "/v2/account"
ALPACA_ASSETS_PATH = "/v2/assets"
ALPACA_ORDERS_PATH = "/v2/orders"
ALPACA_POSITIONS_PATH = "/v2/positions"
ALPACA_CRYPTO_BARS_PATH = "/v1beta3/crypto/us/bars"
ALPACA_LATEST_TRADES_PATH = "/v1beta3/crypto/us/latest/trades"
ALPACA_LATEST_QUOTES_PATH = "/v1beta3/crypto/us/latest/quotes"
ALPACA_STOCK_BARS_PATH = "/v2/stocks/bars"
ALPACA_STOCK_LATEST_QUOTES_PATH = "/v2/stocks/quotes/latest"
ALPACA_CLOCK_PATH = "/v2/clock"
ALPACA_OPEN_ORDERS_PAGE_LIMIT = 500


@dataclass(frozen=True)
class AlpacaHttpResponse:
    status_code: int
    headers: dict[str, str]
    payload: object | None


class AlpacaHttpClient(Protocol):
    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, object] | None = None,
        payload: dict[str, object] | None = None,
        timeout_seconds: float,
    ) -> AlpacaHttpResponse: ...


class UrllibAlpacaHttpClient:
    def __init__(self, *, ssl_context: ssl.SSLContext | None = None) -> None:
        self._ssl_context = ssl_context or ssl.create_default_context()

    def request_json(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, object] | None = None,
        payload: dict[str, object] | None = None,
        timeout_seconds: float,
    ) -> AlpacaHttpResponse:
        query = urlencode(
            {
                key: str(value)
                for key, value in (params or {}).items()
                if value is not None
            },
            doseq=True,
        )
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            url=f"{url}?{query}" if query else url,
            data=body,
            headers=headers,
            method=method.upper(),
        )
        try:
            with urlopen(
                request,
                timeout=timeout_seconds,
                context=self._ssl_context,
            ) as response:
                raw_body = response.read().decode("utf-8")
                return AlpacaHttpResponse(
                    status_code=response.status,
                    headers={str(key).lower(): str(value) for key, value in response.headers.items()},
                    payload=_parse_json(raw_body),
                )
        except HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            raise _alpaca_http_error(
                status_code=exc.code,
                headers={str(key).lower(): str(value) for key, value in exc.headers.items()},
                payload=_parse_json(raw_body),
            ) from exc
        except URLError as exc:
            raise BrokerAdapterError(
                safe_broker_error_message(exc.reason),
                status_code=503,
                classification=BrokerErrorClassification.NETWORK,
                retryable=True,
                normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            ) from exc


class AlpacaBrokerAdapter:
    provider = ALPACA_PROVIDER

    def __init__(
        self,
        *,
        settings: Settings,
        http_client: AlpacaHttpClient | None = None,
    ) -> None:
        self.settings = settings
        self.http_client = http_client or UrllibAlpacaHttpClient()

    def describe(self) -> BrokerConfig:
        base_url = _trading_base_url(self.settings)
        configured = bool(self.settings.alpaca_api_key and self.settings.alpaca_api_secret)
        reason = None if configured else "Alpaca API key and secret are not configured"
        if self.settings.runtime_mode == "mock":
            configured = False
            reason = "runtime mode mock does not connect to Alpaca"
        return BrokerConfig(
            provider=self.provider,
            mode=self.settings.runtime_mode,
            configured=configured,
            api_base_url=base_url,
            reason=reason,
            websocket_approval_path=None,
            shared_realtime_subscriptions_supported=True,
            reconnect_supported=True,
            bounded_rest_reconciliation_supported=True,
        )

    def authenticate(self) -> BrokerTokenGrant:
        config = self._configured()
        response = self._request(
            method="GET",
            url=f"{config.api_base_url}{ALPACA_ACCOUNT_PATH}",
        )
        account = _dict_payload(response.payload, context="Alpaca account")
        _validate_account_ready(account)
        return self._token_grant()

    def authenticate_us_equity(self) -> BrokerTokenGrant:
        config = self._configured()
        response = self._request(
            method="GET",
            url=f"{config.api_base_url}{ALPACA_ACCOUNT_PATH}",
        )
        account = _dict_payload(response.payload, context="Alpaca account")
        _validate_account_ready(account, require_crypto=False)
        return self._token_grant()

    def _token_grant(self) -> BrokerTokenGrant:
        return BrokerTokenGrant(
            access_token=f"alpaca:{self.settings.runtime_mode}:validated",
            token_type="ApiKey",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=12),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        del access_token
        return BrokerRealtimeApproval(
            approval_key="alpaca-api-key-headers",
            issued_at=datetime.now(UTC),
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        del access_token
        return BrokerTokenRevocation(message="Alpaca local broker session revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        del access_token
        self._configured()
        self._validate_order_request(request)
        observed_at = datetime.now(UTC)
        request_payload = _order_request_payload(request)
        request_metadata = self._request_metadata(request)
        if self.settings.runtime_mode == "live" and not self.settings.alpaca_live_trading_enabled:
            raise BrokerAdapterError(
                "Alpaca live trading is disabled by configuration",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                code="alpaca_live_trading_disabled",
                request_metadata=request_metadata,
                response_metadata={"provider": self.provider, "mode": self.settings.runtime_mode},
                request_payload=request_payload,
                response_payload={"error": "alpaca_live_trading_disabled"},
                observed_at=observed_at,
            )

        try:
            response = self._request(
                method="POST",
                url=f"{_trading_base_url(self.settings)}{ALPACA_ORDERS_PATH}",
                payload=request_payload,
            )
        except BrokerAdapterError as exc:
            # A timeout, disconnect, or duplicate-client-id response can happen
            # after Alpaca accepted the order but before this process committed
            # its local acknowledgement. Always look up the deterministic
            # client id before deciding that submission failed.
            recovered = self._recover_order_by_client_id(request.idempotency_key)
            if recovered is not None:
                return self._placement_from_payload(
                    recovered,
                    request=request,
                    request_payload=request_payload,
                    recovered_after_uncertain_submit=True,
                )
            raise _order_error(exc, request=request, request_payload=request_payload) from exc

        order = _dict_payload(response.payload, context="Alpaca order")
        return self._placement_from_payload(
            order,
            request=request,
            request_payload=request_payload,
            recovered_after_uncertain_submit=False,
        )

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "ALPACA",
    ) -> BrokerCurrentPrice | None:
        del access_token
        normalized_symbol = _normalize_symbol(symbol)
        response = self._request(
            method="GET",
            url=f"{self.settings.alpaca_data_base_url}{ALPACA_LATEST_TRADES_PATH}",
            params={"symbols": normalized_symbol},
        )
        payload = _dict_payload(response.payload, context="Alpaca latest trades")
        trades = payload.get("trades")
        if not isinstance(trades, dict):
            return None
        trade = trades.get(normalized_symbol)
        if not isinstance(trade, dict):
            return None
        price = _optional_decimal(trade.get("p") or trade.get("price"))
        if price is None or price <= 0:
            return None
        exchange_observed_at = _parse_timestamp(trade.get("t") or trade.get("timestamp"))
        observed_at = exchange_observed_at or datetime.now(UTC)
        return BrokerCurrentPrice(
            symbol=normalized_symbol,
            exchange=exchange,
            price=price,
            observed_at=observed_at,
            payload={"provider": self.provider, "endpoint": ALPACA_LATEST_TRADES_PATH, **trade},
            freshness_source=(
                "alpaca_exchange_trade_time"
                if exchange_observed_at is not None
                else "transport_received_at_untrusted"
            ),
        )

    def get_crypto_daily_bars(
        self,
        access_token: str,
        *,
        symbols: list[str],
        start: datetime,
        end: datetime,
        page_limit: int = 10_000,
    ) -> dict[str, list[dict[str, Any]]]:
        del access_token
        normalized_symbols = list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))
        if not normalized_symbols:
            raise _validation_error("Alpaca crypto bars require at least one symbol")
        if not 1 <= page_limit <= 10_000:
            raise _validation_error("Alpaca crypto bars page_limit must be between 1 and 10000")
        normalized_start = _utc_datetime(start)
        normalized_end = _utc_datetime(end)
        if normalized_start >= normalized_end:
            raise _validation_error("Alpaca crypto bars start must be before end")

        bars_by_symbol: dict[str, list[dict[str, Any]]] = {
            symbol: [] for symbol in normalized_symbols
        }
        requested_symbols = set(normalized_symbols)
        page_token: str | None = None
        seen_page_tokens: set[str] = set()
        while True:
            response = self._request(
                method="GET",
                url=f"{self.settings.alpaca_data_base_url}{ALPACA_CRYPTO_BARS_PATH}",
                params={
                    "symbols": ",".join(normalized_symbols),
                    "timeframe": "1Day",
                    "start": _rfc3339(normalized_start),
                    "end": _rfc3339(normalized_end),
                    "limit": page_limit,
                    "sort": "asc",
                    "page_token": page_token,
                },
            )
            payload = _dict_payload(response.payload, context="Alpaca crypto bars")
            page_bars = payload.get("bars")
            if not isinstance(page_bars, dict):
                raise BrokerAdapterError(
                    "Alpaca crypto bars response did not contain a symbol-keyed bars object",
                    status_code=502,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
            for response_symbol, raw_bars in page_bars.items():
                normalized_symbol = _normalize_symbol(str(response_symbol))
                if normalized_symbol not in requested_symbols:
                    continue
                if not isinstance(raw_bars, list):
                    raise BrokerAdapterError(
                        f"Alpaca crypto bars for {normalized_symbol} were not a list",
                        status_code=502,
                        classification=BrokerErrorClassification.TRANSIENT,
                        retryable=True,
                    )
                bars_by_symbol[normalized_symbol].extend(
                    bar for bar in raw_bars if isinstance(bar, dict)
                )

            next_page_token = _optional_string(payload.get("next_page_token"))
            if next_page_token is None:
                for symbol_bars in bars_by_symbol.values():
                    symbol_bars.sort(key=_crypto_bar_sort_key)
                return bars_by_symbol
            if next_page_token in seen_page_tokens:
                raise BrokerAdapterError(
                    "Alpaca crypto bars pagination repeated a page token",
                    status_code=502,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token

    def get_stock_daily_bars(
        self,
        access_token: str,
        *,
        symbols: list[str],
        start: datetime,
        end: datetime,
        page_limit: int = 10_000,
        feed: str = "iex",
    ) -> dict[str, list[dict[str, Any]]]:
        del access_token
        normalized_symbols = list(dict.fromkeys(_normalize_equity_symbol(symbol) for symbol in symbols))
        if not normalized_symbols:
            raise _validation_error("Alpaca stock bars require at least one symbol")
        normalized_start = _utc_datetime(start)
        normalized_end = _utc_datetime(end)
        if normalized_start >= normalized_end:
            raise _validation_error("Alpaca stock bars start must be before end")
        bars_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in normalized_symbols}
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            response = self._request(
                method="GET",
                url=f"{self.settings.alpaca_data_base_url}{ALPACA_STOCK_BARS_PATH}",
                params={
                    "symbols": ",".join(normalized_symbols),
                    "timeframe": "1Day",
                    "start": _rfc3339(normalized_start),
                    "end": _rfc3339(normalized_end),
                    "limit": page_limit,
                    "sort": "asc",
                    "feed": feed,
                    "adjustment": "all",
                    "page_token": page_token,
                },
            )
            payload = _dict_payload(response.payload, context="Alpaca stock bars")
            page_bars = payload.get("bars")
            if not isinstance(page_bars, dict):
                raise BrokerAdapterError(
                    "Alpaca stock bars response did not contain a symbol-keyed bars object",
                    status_code=502,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
            for symbol, rows in page_bars.items():
                normalized = _normalize_equity_symbol(str(symbol))
                if normalized in bars_by_symbol and isinstance(rows, list):
                    bars_by_symbol[normalized].extend(row for row in rows if isinstance(row, dict))
            next_token = _optional_string(payload.get("next_page_token"))
            if next_token is None:
                for rows in bars_by_symbol.values():
                    rows.sort(key=_crypto_bar_sort_key)
                return bars_by_symbol
            if next_token in seen_tokens:
                raise BrokerAdapterError(
                    "Alpaca stock bars pagination repeated a page token",
                    status_code=502,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
            seen_tokens.add(next_token)
            page_token = next_token

    def get_stock_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "US",
        feed: str = "iex",
    ) -> BrokerLiveMarketContext | None:
        del access_token
        normalized_symbol = _normalize_equity_symbol(symbol)
        response = self._request(
            method="GET",
            url=f"{self.settings.alpaca_data_base_url}{ALPACA_STOCK_LATEST_QUOTES_PATH}",
            params={"symbols": normalized_symbol, "feed": feed},
        )
        payload = _dict_payload(response.payload, context="Alpaca latest stock quotes")
        quotes = payload.get("quotes")
        if not isinstance(quotes, dict) or not isinstance(quotes.get(normalized_symbol), dict):
            return None
        quote_payload = quotes[normalized_symbol]
        bid = _optional_decimal(quote_payload.get("bp") or quote_payload.get("bid_price"))
        ask = _optional_decimal(quote_payload.get("ap") or quote_payload.get("ask_price"))
        spread = ask - bid if bid is not None and ask is not None and ask >= bid else None
        midpoint = (ask + bid) / 2 if bid is not None and ask is not None and ask + bid > 0 else None
        spread_bps = (spread / midpoint * Decimal("10000")) if spread is not None and midpoint else None
        exchange_observed_at = _parse_timestamp(
            quote_payload.get("t") or quote_payload.get("timestamp")
        )
        observed_at = exchange_observed_at or datetime.now(UTC)
        return BrokerLiveMarketContext(
            symbol=normalized_symbol,
            exchange=exchange,
            observed_at=observed_at,
            bid=bid,
            ask=ask,
            spread=spread,
            spread_bps=spread_bps,
            session_type="us_equity_regular",
            payload={"provider": self.provider, "endpoint": ALPACA_STOCK_LATEST_QUOTES_PATH, **quote_payload},
            freshness_source=(
                "alpaca_exchange_quote_time"
                if exchange_observed_at is not None
                else "transport_received_at_untrusted"
            ),
        )

    def get_market_clock(self, access_token: str) -> dict[str, Any]:
        del access_token
        response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_CLOCK_PATH}",
        )
        return _dict_payload(response.payload, context="Alpaca market clock")

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "ALPACA",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext | None:
        del access_token, reference_price
        normalized_symbol = _normalize_symbol(symbol)
        response = self._request(
            method="GET",
            url=f"{self.settings.alpaca_data_base_url}{ALPACA_LATEST_QUOTES_PATH}",
            params={"symbols": normalized_symbol},
        )
        payload = _dict_payload(response.payload, context="Alpaca latest quotes")
        quotes = payload.get("quotes")
        if not isinstance(quotes, dict) or not isinstance(quotes.get(normalized_symbol), dict):
            return None
        quote_payload = quotes[normalized_symbol]
        bid = _optional_decimal(quote_payload.get("bp") or quote_payload.get("bid_price"))
        ask = _optional_decimal(quote_payload.get("ap") or quote_payload.get("ask_price"))
        spread = ask - bid if bid is not None and ask is not None and ask >= bid else None
        midpoint = (ask + bid) / 2 if bid is not None and ask is not None and ask + bid > 0 else None
        spread_bps = (spread / midpoint * Decimal("10000")) if spread is not None and midpoint else None
        exchange_observed_at = _parse_timestamp(
            quote_payload.get("t") or quote_payload.get("timestamp")
        )
        observed_at = exchange_observed_at or datetime.now(UTC)
        return BrokerLiveMarketContext(
            symbol=normalized_symbol,
            exchange=exchange,
            observed_at=observed_at,
            bid=bid,
            ask=ask,
            spread=spread,
            spread_bps=spread_bps,
            session_type="crypto_24_7",
            normal_ordering_available=True,
            payload={"provider": self.provider, "endpoint": ALPACA_LATEST_QUOTES_PATH, **quote_payload},
            freshness_source=(
                "alpaca_exchange_quote_time"
                if exchange_observed_at is not None
                else "transport_received_at_untrusted"
            ),
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "ALPACA",
    ) -> BrokerAccountSnapshot:
        normalized_exchange = str(exchange or "").strip().upper()
        if normalized_exchange in {"US", "AMEX", "NASD", "NASDAQ", "NYSE"}:
            return self.get_us_equity_account_snapshot(
                access_token,
                symbols=symbols,
            )
        del access_token, reference_prices
        account_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ACCOUNT_PATH}",
        )
        account = _dict_payload(account_response.payload, context="Alpaca account")
        _validate_account_ready(account)
        positions_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_POSITIONS_PATH}",
        )
        orders_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ORDERS_PATH}",
            params={
                "status": "open",
                "asset_class": "crypto",
                "limit": ALPACA_OPEN_ORDERS_PAGE_LIMIT,
            },
        )
        allowed = {_normalize_symbol(value) for value in symbols} if symbols else None
        holdings, holdings_parse, holdings_parse_complete = _snapshot_holdings(
            positions_response.payload,
            asset_class="crypto",
            allowed=allowed,
        )
        open_orders, open_orders_parse, open_orders_parse_complete = (
            _snapshot_open_orders(
                orders_response.payload,
                asset_class="crypto",
                allowed=allowed,
            )
        )
        raw_open_orders = (
            orders_response.payload if isinstance(orders_response.payload, list) else []
        )
        open_orders_coverage = _open_orders_coverage_payload(raw_open_orders)
        account_id = _snapshot_identity(account.get("id"))
        account_identity = _account_identity_diagnostics(account_id)
        holdings_complete = account_id is not None and holdings_parse_complete
        open_orders_complete = (
            account_id is not None
            and open_orders_parse_complete
            and open_orders_coverage["open_orders_coverage"] == "complete"
        )
        return BrokerAccountSnapshot(
            broker_account_id=account_id,
            available_cash=_optional_decimal(account.get("cash")),
            buying_power=_optional_decimal(account.get("non_marginable_buying_power")),
            total_asset_value=_optional_decimal(account.get("portfolio_value")),
            previous_total_asset_value=_optional_decimal(account.get("last_equity")),
            observed_at=datetime.now(UTC),
            source=f"alpaca:{self.settings.runtime_mode}",
            holdings=holdings,
            open_orders=open_orders,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
            diagnostics={
                "account_identity": account_identity,
                "holdings_parse": holdings_parse,
                "open_orders_parse": open_orders_parse,
            },
            payload={
                "provider": self.provider,
                "mode": self.settings.runtime_mode,
                "asset_class": "crypto",
                "account": _redacted_account_payload(account),
                **open_orders_coverage,
                "holdings_coverage": (
                    "complete" if holdings_complete else "incomplete"
                ),
                "open_orders_coverage": (
                    "complete" if open_orders_complete else "incomplete"
                ),
            },
        )

    def get_broker_account_id(self, access_token: str) -> str | None:
        """Return Alpaca's stable account id without loading holdings or orders."""
        del access_token
        response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ACCOUNT_PATH}",
        )
        account = _dict_payload(response.payload, context="Alpaca account identity")
        return _snapshot_identity(account.get("id"))

    def get_us_equity_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
    ) -> BrokerAccountSnapshot:
        del access_token
        account_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ACCOUNT_PATH}",
        )
        account = _dict_payload(account_response.payload, context="Alpaca account")
        _validate_account_ready(account, require_crypto=False)
        positions_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_POSITIONS_PATH}",
        )
        orders_response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ORDERS_PATH}",
            params={
                "status": "open",
                "asset_class": "us_equity",
                "limit": ALPACA_OPEN_ORDERS_PAGE_LIMIT,
            },
        )
        allowed = {_normalize_equity_symbol(value) for value in symbols} if symbols else None
        holdings, holdings_parse, holdings_parse_complete = _snapshot_holdings(
            positions_response.payload,
            asset_class="us_equity",
            allowed=allowed,
        )
        open_orders, open_orders_parse, open_orders_parse_complete = (
            _snapshot_open_orders(
                orders_response.payload,
                asset_class="us_equity",
                allowed=allowed,
            )
        )
        raw_open_orders = (
            orders_response.payload if isinstance(orders_response.payload, list) else []
        )
        open_orders_coverage = _open_orders_coverage_payload(raw_open_orders)
        account_id = _snapshot_identity(account.get("id"))
        account_identity = _account_identity_diagnostics(account_id)
        holdings_complete = account_id is not None and holdings_parse_complete
        open_orders_complete = (
            account_id is not None
            and open_orders_parse_complete
            and open_orders_coverage["open_orders_coverage"] == "complete"
        )
        return BrokerAccountSnapshot(
            broker_account_id=account_id,
            available_cash=_optional_decimal(account.get("cash")),
            buying_power=_optional_decimal(account.get("non_marginable_buying_power")),
            total_asset_value=_optional_decimal(account.get("portfolio_value")),
            previous_total_asset_value=_optional_decimal(account.get("last_equity")),
            observed_at=datetime.now(UTC),
            source=f"alpaca:{self.settings.runtime_mode}:us_equity",
            holdings=holdings,
            open_orders=open_orders,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
            diagnostics={
                "account_identity": account_identity,
                "holdings_parse": holdings_parse,
                "open_orders_parse": open_orders_parse,
            },
            payload={
                "provider": self.provider,
                "mode": self.settings.runtime_mode,
                "asset_class": "us_equity",
                "account": _redacted_account_payload(account),
                **open_orders_coverage,
                "holdings_coverage": (
                    "complete" if holdings_complete else "incomplete"
                ),
                "open_orders_coverage": (
                    "complete" if open_orders_complete else "incomplete"
                ),
            },
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(
            supported=True,
            strategy="rest_reconcile_then_resubscribe",
            details={
                "provider": self.provider,
                "market": "crypto_24_7",
                "trade_updates": f"{_trading_base_url(self.settings).replace('https://', 'wss://')}/stream",
                "market_data": "wss://stream.data.alpaca.markets/v1beta3/crypto/us",
            },
        )

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "ALPACA",
        submitted_at: datetime | None = None,
        expected_symbol: str | None = None,
    ) -> dict[str, Any] | None:
        del access_token, submitted_at
        normalized_exchange = str(exchange or "").strip().upper()
        normalize_reconcile_symbol = (
            _normalize_symbol
            if normalized_exchange == "ALPACA"
            else _normalize_equity_symbol
        )
        response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ORDERS_PATH}/{quote(broker_order_id, safe='')}",
        )
        order = _dict_payload(response.payload, context="Alpaca order")
        response_order_id = _snapshot_identity(order.get("id"))
        response_symbol = _snapshot_identity(order.get("symbol"))
        normalized_response_symbol = (
            normalize_reconcile_symbol(response_symbol)
            if response_symbol is not None
            else None
        )
        identity_mismatch = response_order_id != broker_order_id
        if expected_symbol is not None:
            identity_mismatch = identity_mismatch or (
                normalized_response_symbol is None
                or normalized_response_symbol
                != normalize_reconcile_symbol(expected_symbol)
            )
        if identity_mismatch:
            raise BrokerAdapterError(
                "Alpaca reconciliation response identity did not match the requested order",
                status_code=502,
                classification=BrokerErrorClassification.TRANSIENT,
                retryable=True,
                code="alpaca_reconcile_identity_mismatch",
                response_metadata=_safe_error_payload(
                    {
                        "provider": self.provider,
                        "request_id": response.headers.get("x-request-id"),
                    }
                ),
                response_payload={"identity_match": False},
                observed_at=datetime.now(UTC),
            )
        status = _normalized_order_status(str(order.get("status") or ""))
        filled_base_quantity = _optional_decimal(order.get("filled_qty")) or Decimal("0")
        average_fill_price = _optional_decimal(order.get("filled_avg_price"))
        return {
            "provider": self.provider,
            "broker_order_id": response_order_id,
            "symbol": normalized_response_symbol,
            "status": status.value,
            "broker_status": str(order.get("status") or ""),
            "filled_base_quantity": str(filled_base_quantity),
            "average_fill_price": str(average_fill_price) if average_fill_price is not None else None,
            "filled_at": order.get("filled_at") or order.get("updated_at"),
            "observed_at": datetime.now(UTC).isoformat(),
            "request_metadata": {
                "provider": self.provider,
                "exchange": normalized_exchange,
                "endpoint": f"{ALPACA_ORDERS_PATH}/{{order_id}}",
            },
            "response_metadata": {
                "provider": self.provider,
                "request_id": response.headers.get("x-request-id"),
            },
            "payload": order,
        }

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        del access_token, transaction_id, hashkey
        return self._headers()

    def list_active_crypto_assets(self) -> list[dict[str, Any]]:
        response = self._request(
            method="GET",
            url=f"{_trading_base_url(self.settings)}{ALPACA_ASSETS_PATH}",
            params={"status": "active", "asset_class": "crypto"},
        )
        return [item for item in _list_payload(response.payload, context="Alpaca assets") if item.get("tradable") is True]

    def list_active_us_equity_assets(self, symbols: list[str]) -> list[dict[str, Any]]:
        assets: list[dict[str, Any]] = []
        for symbol in dict.fromkeys(_normalize_equity_symbol(item) for item in symbols):
            response = self._request(
                method="GET",
                url=f"{_trading_base_url(self.settings)}{ALPACA_ASSETS_PATH}/{quote(symbol, safe='')}",
            )
            asset = _dict_payload(response.payload, context=f"Alpaca asset {symbol}")
            if asset.get("status") == "active" and asset.get("tradable") is True:
                assets.append({
                    **asset,
                    "symbol": symbol,
                    "min_trade_increment": "0.000001" if asset.get("fractionable") is True else "1",
                })
        return assets

    def _configured(self) -> BrokerConfig:
        config = self.describe()
        if not config.configured or not config.api_base_url:
            raise BrokerAdapterError(
                config.reason or "Alpaca is not configured",
                status_code=409,
                classification=BrokerErrorClassification.AUTH,
            )
        return config

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.settings.alpaca_api_key or "",
            "APCA-API-SECRET-KEY": self.settings.alpaca_api_secret or "",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "kai-trading-exec/0.1",
        }

    def _request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, object] | None = None,
        payload: dict[str, object] | None = None,
    ) -> AlpacaHttpResponse:
        self._configured()
        return self.http_client.request_json(
            method=method,
            url=url,
            headers=self._headers(),
            params=params,
            payload=payload,
            timeout_seconds=self.settings.alpaca_request_timeout_seconds,
        )

    def _recover_order_by_client_id(self, client_order_id: str) -> dict[str, Any] | None:
        try:
            response = self._request(
                method="GET",
                url=f"{_trading_base_url(self.settings)}{ALPACA_ORDERS_PATH}:by_client_order_id",
                params={"client_order_id": client_order_id},
            )
        except BrokerAdapterError:
            return None
        return response.payload if isinstance(response.payload, dict) else None

    def _placement_from_payload(
        self,
        order: dict[str, Any],
        *,
        request: BrokerOrderPlacementRequest,
        request_payload: dict[str, object],
        recovered_after_uncertain_submit: bool,
    ) -> BrokerOrderPlacement:
        observed_at = datetime.now(UTC)
        broker_status = str(order.get("status") or "accepted")
        normalized_status = _normalized_order_status(broker_status)
        return BrokerOrderPlacement(
            broker_order_id=_optional_string(order.get("id")),
            normalized_status=normalized_status,
            broker_status=broker_status,
            submitted_at=_parse_timestamp(order.get("submitted_at") or order.get("created_at")) or observed_at,
            observed_at=observed_at,
            request_metadata=self._request_metadata(request),
            response_metadata={
                "provider": self.provider,
                "mode": self.settings.runtime_mode,
                "paper": self.settings.runtime_mode == "paper",
                "recovered_after_uncertain_submit": recovered_after_uncertain_submit,
            },
            request_payload=request_payload,
            response_payload=order,
            message=(
                "Alpaca crypto order accepted"
                if request.asset_class == "crypto"
                else "Alpaca US-equity order accepted"
            ),
        )

    def _validate_order_request(self, request: BrokerOrderPlacementRequest) -> None:
        if request.provider not in {None, self.provider} or request.asset_class not in {"crypto", "us_equity"}:
            raise _validation_error("Alpaca adapter accepts provider='alpaca' crypto or US-equity orders")
        if request.quantity is not None:
            raise _validation_error("Alpaca orders must use base_quantity or quote_notional")
        if (request.base_quantity is None) == (request.quote_notional is None):
            raise _validation_error("Alpaca orders require exactly one of base_quantity or quote_notional")
        if request.order_type not in {"market", "limit"}:
            raise _validation_error("Alpaca order_type must be market or limit")
        allowed_tif = {"gtc", "ioc"} if request.asset_class == "crypto" else {"day"}
        if request.time_in_force not in allowed_tif:
            raise _validation_error(f"Alpaca {request.asset_class} time_in_force must be one of {sorted(allowed_tif)}")
        if request.order_type == "limit" and request.limit_price is None:
            raise _validation_error("Alpaca limit orders require limit_price")
        if request.quote_notional is not None and not (request.side == "buy" and request.order_type == "market"):
            raise _validation_error("Alpaca quote_notional is only supported for market buys")
        normalized_symbol = (
            _normalize_symbol(request.symbol)
            if request.asset_class == "crypto"
            else _normalize_equity_symbol(request.symbol)
        )
        allowed_symbols = (
            _allowed_symbols(self.settings)
            if request.asset_class == "crypto"
            else _allowed_us_equity_symbols(self.settings)
        )
        if normalized_symbol not in allowed_symbols:
            raise _validation_error(f"Alpaca {request.asset_class} symbol is not in the configured allowlist")

    def _request_metadata(self, request: BrokerOrderPlacementRequest) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "venue": request.venue or "alpaca",
            "asset_class": request.asset_class,
            "exchange": request.exchange,
            "mode": self.settings.runtime_mode,
            "paper": self.settings.runtime_mode == "paper",
            "client_order_id": request.idempotency_key,
            "sizing": {
                "base_quantity": _decimal_text(request.base_quantity),
                "quote_notional": _decimal_text(request.quote_notional),
                "limit_price": _decimal_text(request.limit_price),
            },
        }


def _trading_base_url(settings: Settings) -> str:
    if settings.runtime_mode == "paper":
        return settings.alpaca_paper_base_url or ALPACA_PAPER_API_BASE_URL
    return settings.alpaca_live_base_url or ALPACA_LIVE_API_BASE_URL


def _allowed_symbols(settings: Settings) -> set[str]:
    return {
        _normalize_symbol(value)
        for value in settings.alpaca_crypto_allowed_symbols.split(",")
        if value.strip()
    }


def _allowed_us_equity_symbols(settings: Settings) -> set[str]:
    return {
        _normalize_equity_symbol(value)
        for value in settings.alpaca_us_equity_allowed_symbols.split(",")
        if value.strip()
    }


def _normalize_symbol(value: str) -> str:
    normalized = value.strip().upper().replace("-", "/")
    if "/" not in normalized and normalized.endswith("USD") and len(normalized) > 3:
        normalized = f"{normalized[:-3]}/USD"
    return normalized


def _normalize_equity_symbol(value: str) -> str:
    return value.strip().upper()


def _order_request_payload(request: BrokerOrderPlacementRequest) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": (
            _normalize_symbol(request.symbol)
            if request.asset_class == "crypto"
            else _normalize_equity_symbol(request.symbol)
        ),
        "side": request.side,
        "type": request.order_type,
        "time_in_force": request.time_in_force,
        "client_order_id": request.idempotency_key,
    }
    if request.base_quantity is not None:
        payload["qty"] = _decimal_text(request.base_quantity) or "0"
    if request.quote_notional is not None:
        payload["notional"] = _decimal_text(request.quote_notional) or "0"
    if request.limit_price is not None:
        payload["limit_price"] = _decimal_text(request.limit_price) or "0"
    return payload


def _snapshot_identity(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _account_identity_diagnostics(account_id: str | None) -> dict[str, object]:
    if account_id is not None:
        return {"complete": True}
    return {
        "complete": False,
        "reason": "missing_or_malformed_account_id",
    }


def _snapshot_symbol(payload: dict[str, Any], *, asset_class: str) -> str | None:
    raw_symbol = _snapshot_identity(payload.get("symbol"))
    if raw_symbol is None:
        return None
    if asset_class == "crypto":
        return _normalize_symbol(raw_symbol)
    return _normalize_equity_symbol(raw_symbol)


def _snapshot_asset_class_match(
    payload: dict[str, Any],
    *,
    asset_class: str,
) -> bool | None:
    reported_asset_class = str(payload.get("asset_class") or "").strip().lower()
    if reported_asset_class in {"crypto", "equity", "us_equity"}:
        reported_class = (
            "crypto" if reported_asset_class == "crypto" else "us_equity"
        )
        return reported_class == asset_class
    if "/" in str(payload.get("symbol") or ""):
        return asset_class == "crypto"
    return None


def _snapshot_parse_diagnostics(
    *,
    raw_row_count: int,
    parsed_identities: list[str],
    omitted_row_count: int,
    invalid_row_count: int,
) -> dict[str, int]:
    return {
        "raw_row_count": raw_row_count,
        "parsed_row_count": len(parsed_identities),
        "omitted_row_count": omitted_row_count,
        "invalid_row_count": invalid_row_count,
        "duplicate_identity_count": len(parsed_identities)
        - len(set(parsed_identities)),
    }


def _snapshot_parse_complete(
    *,
    response_is_list: bool,
    diagnostics: dict[str, int],
) -> bool:
    return bool(
        response_is_list
        and diagnostics["raw_row_count"]
        == diagnostics["parsed_row_count"] + diagnostics["omitted_row_count"]
        + diagnostics["invalid_row_count"]
        and diagnostics["invalid_row_count"] == 0
        and diagnostics["duplicate_identity_count"] == 0
    )


def _snapshot_holdings(
    value: object | None,
    *,
    asset_class: str,
    allowed: set[str] | None,
) -> tuple[list[BrokerAccountHolding], dict[str, int], bool]:
    response_is_list = isinstance(value, list)
    raw_rows = value if response_is_list else []
    holdings: list[BrokerAccountHolding] = []
    identities: list[str] = []
    omitted_row_count = 0
    invalid_row_count = 0 if response_is_list else 1
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            invalid_row_count += 1
            continue
        asset_class_match = _snapshot_asset_class_match(
            raw_row,
            asset_class=asset_class,
        )
        if asset_class_match is False:
            omitted_row_count += 1
            continue
        if asset_class_match is None:
            invalid_row_count += 1
            continue
        symbol = _snapshot_symbol(raw_row, asset_class=asset_class)
        quantity = _optional_decimal(raw_row.get("qty"))
        if symbol is None or quantity is None or quantity < 0:
            invalid_row_count += 1
            continue
        if quantity == 0 or (allowed is not None and symbol not in allowed):
            omitted_row_count += 1
            continue
        holdings.append(
            _holding_from_position(raw_row, asset_class=asset_class)
        )
        identities.append(symbol)
    diagnostics = _snapshot_parse_diagnostics(
        raw_row_count=len(raw_rows),
        parsed_identities=identities,
        omitted_row_count=omitted_row_count,
        invalid_row_count=invalid_row_count,
    )
    return (
        holdings,
        diagnostics,
        _snapshot_parse_complete(
            response_is_list=response_is_list,
            diagnostics=diagnostics,
        ),
    )


_ALPACA_TERMINAL_ORDER_STATUSES = frozenset(
    {
        "canceled",
        "cancelled",
        "done_for_day",
        "expired",
        "filled",
        "rejected",
        "replaced",
    }
)


def _snapshot_open_orders(
    value: object | None,
    *,
    asset_class: str,
    allowed: set[str] | None,
) -> tuple[list[BrokerAccountOpenOrder], dict[str, int], bool]:
    response_is_list = isinstance(value, list)
    raw_rows = value if response_is_list else []
    open_orders: list[BrokerAccountOpenOrder] = []
    identities: list[str] = []
    omitted_row_count = 0
    invalid_row_count = 0 if response_is_list else 1
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            invalid_row_count += 1
            continue
        asset_class_match = _snapshot_asset_class_match(
            raw_row,
            asset_class=asset_class,
        )
        if asset_class_match is False:
            omitted_row_count += 1
            continue
        if asset_class_match is None:
            invalid_row_count += 1
            continue
        status = str(raw_row.get("status") or "").strip().lower()
        if status in _ALPACA_TERMINAL_ORDER_STATUSES:
            omitted_row_count += 1
            continue
        symbol = _snapshot_symbol(raw_row, asset_class=asset_class)
        broker_order_id = _snapshot_identity(raw_row.get("id"))
        side = str(raw_row.get("side") or "").strip().lower()
        quantity = _optional_decimal(raw_row.get("qty"))
        filled_quantity = _optional_decimal(raw_row.get("filled_qty"))
        if (
            symbol is None
            or broker_order_id is None
            or side not in {"buy", "sell"}
            or quantity is None
            or filled_quantity is None
            or quantity < 0
            or filled_quantity < 0
            or filled_quantity > quantity
        ):
            invalid_row_count += 1
            continue
        if (
            quantity == 0
            or filled_quantity == quantity
            or (allowed is not None and symbol not in allowed)
        ):
            omitted_row_count += 1
            continue
        open_orders.append(
            _open_order_from_payload(raw_row, asset_class=asset_class)
        )
        identities.append(broker_order_id)
    diagnostics = _snapshot_parse_diagnostics(
        raw_row_count=len(raw_rows),
        parsed_identities=identities,
        omitted_row_count=omitted_row_count,
        invalid_row_count=invalid_row_count,
    )
    return (
        open_orders,
        diagnostics,
        _snapshot_parse_complete(
            response_is_list=response_is_list,
            diagnostics=diagnostics,
        ),
    )


def _holding_from_position(
    payload: dict[str, Any],
    *,
    asset_class: str,
) -> BrokerAccountHolding:
    base_quantity = _optional_decimal(payload.get("qty")) or Decimal("0")
    symbol = (
        _normalize_symbol(str(payload.get("symbol") or ""))
        if asset_class == "crypto"
        else _normalize_equity_symbol(str(payload.get("symbol") or ""))
    )
    return BrokerAccountHolding(
        symbol=symbol,
        quantity=0,
        average_price=_optional_decimal(payload.get("avg_entry_price")),
        sellable_quantity=0,
        unrealized_pnl=_optional_decimal(payload.get("unrealized_pl")),
        unrealized_pnl_pct=_optional_decimal(payload.get("unrealized_plpc")),
        payload={
            "provider": ALPACA_PROVIDER,
            "asset_class": str(payload.get("asset_class") or "unknown").lower(),
            "base_quantity": _decimal_text(base_quantity),
            "market_value": payload.get("market_value"),
            "current_price": payload.get("current_price"),
            "raw": payload,
        },
    )


def _open_order_from_payload(
    payload: dict[str, Any],
    *,
    asset_class: str,
) -> BrokerAccountOpenOrder:
    base_quantity = _optional_decimal(payload.get("qty")) or Decimal("0")
    filled = _optional_decimal(payload.get("filled_qty")) or Decimal("0")
    remaining = max(base_quantity - filled, Decimal("0"))
    return BrokerAccountOpenOrder(
        symbol=(
            _normalize_symbol(str(payload.get("symbol") or ""))
            if asset_class == "crypto"
            else _normalize_equity_symbol(str(payload.get("symbol") or ""))
        ),
        side=str(payload.get("side") or "").lower(),
        quantity=0,
        remaining_quantity=0,
        status=str(payload.get("status") or "open"),
        broker_order_id=_optional_string(payload.get("id")),
        idempotency_key=_optional_string(payload.get("client_order_id")),
        limit_price=_optional_decimal(payload.get("limit_price")),
        payload={
            "provider": ALPACA_PROVIDER,
            "asset_class": str(payload.get("asset_class") or "unknown").lower(),
            "base_quantity": _decimal_text(base_quantity),
            "remaining_base_quantity": _decimal_text(remaining),
            "raw": payload,
        },
    )


def _validate_account_ready(account: dict[str, Any], *, require_crypto: bool = True) -> None:
    if str(account.get("status") or "").upper() != "ACTIVE":
        raise BrokerAdapterError(
            "Alpaca account is not active",
            status_code=409,
            classification=BrokerErrorClassification.AUTH,
            code="alpaca_account_not_active",
        )
    crypto_status = str(account.get("crypto_status") or "ACTIVE").upper()
    if require_crypto and crypto_status != "ACTIVE":
        raise BrokerAdapterError(
            "Alpaca crypto trading is not active for this account",
            status_code=409,
            classification=BrokerErrorClassification.AUTH,
            code="alpaca_crypto_not_active",
        )
    blocking_fields = ("account_blocked", "trading_blocked", "trade_suspended_by_user")
    blocked = [field for field in blocking_fields if account.get(field) is True]
    if blocked:
        raise BrokerAdapterError(
            f"Alpaca account is blocked for trading: {', '.join(blocked)}",
            status_code=409,
            classification=BrokerErrorClassification.AUTH,
            code="alpaca_account_blocked",
        )


def _normalized_order_status(value: str) -> NormalizedBrokerOrderStatus:
    normalized = value.strip().lower()
    if normalized == "filled":
        return NormalizedBrokerOrderStatus.FILLED
    if normalized == "partially_filled":
        return NormalizedBrokerOrderStatus.PARTIALLY_FILLED
    if normalized in {"canceled", "replaced"}:
        return NormalizedBrokerOrderStatus.CANCELED
    if normalized == "expired":
        return NormalizedBrokerOrderStatus.EXPIRED
    if normalized in {"rejected", "suspended", "stopped"}:
        return NormalizedBrokerOrderStatus.REJECTED
    if normalized in {"pending_cancel", "pending_replace"}:
        return NormalizedBrokerOrderStatus.CANCEL_PENDING
    if normalized in {"new", "accepted_for_bidding"}:
        return NormalizedBrokerOrderStatus.OPEN
    return NormalizedBrokerOrderStatus.SUBMITTED


def _validation_error(message: str) -> BrokerAdapterError:
    return BrokerAdapterError(
        message,
        status_code=422,
        classification=BrokerErrorClassification.VALIDATION,
        normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        retryable=False,
    )


def _order_error(
    exc: BrokerAdapterError,
    *,
    request: BrokerOrderPlacementRequest,
    request_payload: dict[str, object],
) -> BrokerAdapterError:
    return BrokerAdapterError(
        str(exc),
        status_code=exc.status_code,
        classification=exc.classification,
        retryable=exc.retryable,
        code=exc.code,
        normalized_status=exc.normalized_status,
        request_metadata={
            "provider": ALPACA_PROVIDER,
            "client_order_id": request.idempotency_key,
        },
        response_metadata=exc.response_metadata,
        request_payload=request_payload,
        response_payload=exc.response_payload,
        observed_at=exc.observed_at or datetime.now(UTC),
    )


def _alpaca_http_error(
    *,
    status_code: int,
    headers: dict[str, str],
    payload: object | None,
) -> BrokerAdapterError:
    body = payload if isinstance(payload, dict) else {}
    raw_message = str(
        body.get("message") or body.get("error") or f"Alpaca HTTP {status_code}"
    )
    code = _optional_string(body.get("code"))
    if status_code in {401, 403}:
        classification = BrokerErrorClassification.AUTH
        retryable = False
        normalized = NormalizedBrokerOrderStatus.REJECTED
    elif status_code == 429:
        classification = BrokerErrorClassification.RATE_LIMIT
        retryable = True
        normalized = NormalizedBrokerOrderStatus.RETRY_PENDING
    elif status_code >= 500:
        classification = BrokerErrorClassification.TRANSIENT
        retryable = True
        normalized = NormalizedBrokerOrderStatus.RETRY_PENDING
    elif status_code in {400, 404, 422}:
        classification = BrokerErrorClassification.BROKER_REJECTED
        retryable = False
        normalized = NormalizedBrokerOrderStatus.REJECTED
    else:
        classification = BrokerErrorClassification.UNKNOWN
        retryable = False
        normalized = NormalizedBrokerOrderStatus.FAILED
    return BrokerAdapterError(
        safe_broker_error_message(raw_message),
        status_code=status_code,
        classification=classification,
        retryable=retryable,
        code=safe_broker_error_code(code, provider=ALPACA_PROVIDER),
        normalized_status=normalized,
        response_metadata=_safe_error_payload(
            {
                "provider": ALPACA_PROVIDER,
                "request_id": headers.get("x-request-id"),
            }
        ),
        response_payload=_safe_error_payload(body),
        observed_at=datetime.now(UTC),
    )


def _dict_payload(value: object | None, *, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BrokerAdapterError(
            f"{context} response was not an object",
            status_code=502,
            classification=BrokerErrorClassification.TRANSIENT,
            retryable=True,
        )
    return value


def _list_payload(value: object | None, *, context: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise BrokerAdapterError(
            f"{context} response was not a list",
            status_code=502,
            classification=BrokerErrorClassification.TRANSIENT,
            retryable=True,
        )
    return [item for item in value if isinstance(item, dict)]


def _open_orders_coverage_payload(
    raw_open_orders: list[object],
) -> dict[str, object]:
    if len(raw_open_orders) < ALPACA_OPEN_ORDERS_PAGE_LIMIT:
        return {"open_orders_coverage": "complete"}
    # Alpaca's open-order response may be truncated at the requested limit.
    # Until pagination is proven complete, consumers must reserve
    # conservatively and block new exposure/exit orders.
    return {
        "open_orders_coverage": "incomplete",
        "open_orders_error": {
            "code": "alpaca_open_orders_page_limit_reached",
            "degraded": True,
            "limit": ALPACA_OPEN_ORDERS_PAGE_LIMIT,
            "returned_count": len(raw_open_orders),
        },
    }


def _parse_json(raw_body: str) -> object | None:
    if not raw_body.strip():
        return None
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError:
        return {"body_diagnostic": safe_broker_error_message(raw_body)}


def _safe_error_payload(payload: object) -> dict[str, Any]:
    sanitized = sanitize_broker_error_payload(payload)
    return dict(sanitized) if isinstance(sanitized, dict) else {}


def _optional_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _rfc3339(value: datetime) -> str:
    return _utc_datetime(value).isoformat().replace("+00:00", "Z")


def _crypto_bar_sort_key(bar: dict[str, Any]) -> datetime:
    return (
        _parse_timestamp(bar.get("t") or bar.get("timestamp"))
        or datetime.max.replace(tzinfo=UTC)
    )


def _redacted_account_payload(account: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in account.items()
        if key not in {"account_number", "id"}
    }
