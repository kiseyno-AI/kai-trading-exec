from __future__ import annotations

import json
import ssl
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.broker.base import (
    BrokerAdapter,
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

KIS_PROVIDER = "kis"
KIS_LIVE_API_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_PAPER_API_BASE_URL = "https://openapivts.koreainvestment.com:29443"
KIS_WEBSOCKET_APPROVAL_PATH = "/oauth2/Approval"
KIS_TOKEN_PATH = "/oauth2/tokenP"
KIS_REVOKE_PATH = "/oauth2/revokeP"
KIS_HASHKEY_PATH = "/uapi/hashkey"
KIS_ORDER_CASH_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
KIS_CURRENT_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
KIS_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_OPEN_ORDERS_PATH = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
KIS_DAILY_CCLD_PATH = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
KIS_ASKING_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
KIS_CURRENT_PRICE_TR_ID = "FHKST01010100"
KIS_ASKING_PRICE_TR_ID = "FHKST01010200"
KIS_TIMEZONE = timezone(timedelta(hours=9))
_DEFAULT_SSL_CONTEXT: ssl.SSLContext | None = None
_DEFAULT_SSL_CONTEXT_LOCK = threading.Lock()


def _default_ssl_context() -> ssl.SSLContext:
    global _DEFAULT_SSL_CONTEXT
    if _DEFAULT_SSL_CONTEXT is not None:
        return _DEFAULT_SSL_CONTEXT
    with _DEFAULT_SSL_CONTEXT_LOCK:
        if _DEFAULT_SSL_CONTEXT is None:
            _DEFAULT_SSL_CONTEXT = ssl.create_default_context()
        return _DEFAULT_SSL_CONTEXT


@dataclass(frozen=True)
class JsonHttpResponse:
    status_code: int
    headers: dict[str, str]
    payload: dict[str, object]


class JsonHttpClient(Protocol):
    def get_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse: ...

    def post_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse: ...


class UrllibJsonHttpClient:
    def __init__(self, *, ssl_context: ssl.SSLContext | None = None) -> None:
        self._ssl_context = ssl_context or _default_ssl_context()

    def get_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        query = urlencode(
            {
                key: str(value)
                for key, value in params.items()
                if value is not None
            }
        )
        request = Request(
            url=f"{url}?{query}" if query else url,
            headers=headers,
            method="GET",
        )
        return self._request_json(request=request, timeout_seconds=timeout_seconds)

    def post_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        request = Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        return self._request_json(request=request, timeout_seconds=timeout_seconds)

    def _request_json(
        self,
        *,
        request: Request,
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        try:
            with urlopen(
                request,
                timeout=timeout_seconds,
                context=self._ssl_context,
            ) as response:
                raw_body = response.read().decode("utf-8")
                return JsonHttpResponse(
                    status_code=response.status,
                    headers={
                        str(key).lower(): str(value)
                        for key, value in response.headers.items()
                    },
                    payload=_parse_json_payload(raw_body),
                )
        except HTTPError as exc:
            raise _http_error(
                status_code=exc.code,
                headers={str(key).lower(): str(value) for key, value in exc.headers.items()},
                raw_body=exc.read().decode("utf-8", errors="replace"),
            ) from exc
        except URLError as exc:
            raise BrokerAdapterError(
                f"KIS request failed: {exc.reason}",
                status_code=503,
                classification=BrokerErrorClassification.NETWORK,
                retryable=True,
                normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            ) from exc


class KisBrokerAdapter(BrokerAdapter):
    provider = KIS_PROVIDER

    def __init__(
        self,
        *,
        settings: Settings,
        http_client: JsonHttpClient | None = None,
    ) -> None:
        self.settings = settings
        self.http_client = http_client or UrllibJsonHttpClient()

    def describe(self) -> BrokerConfig:
        api_base_url = _resolve_api_base_url(self.settings)
        if self.settings.runtime_mode == "mock":
            return BrokerConfig(
                provider=self.provider,
                mode=self.settings.runtime_mode,
                configured=False,
                api_base_url=None,
                reason="runtime mode mock does not use KIS broker authentication",
                websocket_approval_path=KIS_WEBSOCKET_APPROVAL_PATH,
            )

        if not self.settings.kis_app_key or not self.settings.kis_app_secret:
            return BrokerConfig(
                provider=self.provider,
                mode=self.settings.runtime_mode,
                configured=False,
                api_base_url=api_base_url,
                reason="KIS app key and app secret are not configured",
                websocket_approval_path=KIS_WEBSOCKET_APPROVAL_PATH,
            )

        return BrokerConfig(
            provider=self.provider,
            mode=self.settings.runtime_mode,
            configured=True,
            api_base_url=api_base_url,
            websocket_approval_path=KIS_WEBSOCKET_APPROVAL_PATH,
        )

    def authenticate(self) -> BrokerTokenGrant:
        config = self._configured()
        response = self.http_client.post_json(
            url=f"{config.api_base_url}{KIS_TOKEN_PATH}",
            headers=_json_headers(),
            payload={
                "grant_type": "client_credentials",
                "appkey": self.settings.kis_app_key or "",
                "appsecret": self.settings.kis_app_secret or "",
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        access_token = _required_string(response.payload, "access_token")
        token_type = str(response.payload.get("token_type") or "Bearer")
        access_token_expires_at = _parse_expiration(
            response.payload.get("access_token_token_expired"),
            response.payload.get("expires_in"),
        )
        refresh_token = _optional_string(response.payload.get("refresh_token"))
        refresh_token_expires_at = _parse_optional_expiration(
            response.payload.get("refresh_token_token_expired"),
            response.payload.get("refresh_token_expires_in"),
        )
        return BrokerTokenGrant(
            access_token=access_token,
            token_type=token_type,
            access_token_expires_at=access_token_expires_at,
            refresh_token=refresh_token,
            refresh_token_expires_at=refresh_token_expires_at,
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        config = self._configured()
        response = self.http_client.post_json(
            url=f"{config.api_base_url}{KIS_WEBSOCKET_APPROVAL_PATH}",
            headers=_json_headers(),
            payload={
                "grant_type": "client_credentials",
                "appkey": self.settings.kis_app_key or "",
                "secretkey": self.settings.kis_app_secret or "",
                "token": access_token,
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        approval_key = _required_string(response.payload, "approval_key")
        return BrokerRealtimeApproval(
            approval_key=approval_key,
            issued_at=datetime.now(UTC),
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        config = self._configured()
        response = self.http_client.post_json(
            url=f"{config.api_base_url}{KIS_REVOKE_PATH}",
            headers=_json_headers(),
            payload={
                "appkey": self.settings.kis_app_key or "",
                "appsecret": self.settings.kis_app_secret or "",
                "token": access_token,
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        message = _required_string(response.payload, "message")
        return BrokerTokenRevocation(message=message)

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        config = self._configured()
        if not self.settings.kis_account_number:
            raise BrokerAdapterError(
                "KIS account number is not configured",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )
        tr_id = _transaction_id(self.settings.runtime_mode, request.side)
        request_payload = {
            "CANO": self.settings.kis_account_number or "",
            "ACNT_PRDT_CD": self.settings.kis_account_product_code,
            "PDNO": request.symbol,
            "ORD_DVSN": _order_division(request.order_type, request.order_division),
            "ORD_QTY": str(request.quantity),
            "ORD_UNPR": _order_price(
                request.order_type,
                request.limit_price,
                request.order_division,
            ),
            "EXCG_ID_DVSN_CD": request.exchange,
            "SLL_TYPE": "01" if request.side == "sell" else "",
            "CNDT_PRIC": "",
        }
        hashkey = self._issue_hashkey(access_token, request_payload)
        headers = self.build_headers(
            access_token,
            transaction_id=tr_id,
            hashkey=hashkey,
        )
        headers["tr_cont"] = ""
        headers["custtype"] = self.settings.kis_customer_type

        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_ORDER_CASH_PATH,
            "method": "POST",
            "tr_id": tr_id,
            "requested_at": datetime.now(UTC).isoformat(),
            "account_number_masked": _mask_account(self.settings.kis_account_number),
            "account_product_code": self.settings.kis_account_product_code,
            "route": {
                "exchange": request.exchange,
                "session": request.route_session,
                "ord_dvsn": request.order_division,
                "reason": request.route_reason,
            },
        }

        try:
            response = self.http_client.post_json(
                url=f"{config.api_base_url}{KIS_ORDER_CASH_PATH}",
                headers=headers,
                payload=request_payload,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
        except BrokerAdapterError as exc:
            raise BrokerAdapterError(
                str(exc),
                status_code=exc.status_code or 503,
                classification=exc.classification,
                retryable=exc.retryable,
                code=exc.code,
                normalized_status=exc.normalized_status,
                request_metadata=request_metadata,
                response_metadata=exc.response_metadata,
                request_payload=request_payload,
                response_payload=exc.response_payload,
                observed_at=exc.observed_at,
            ) from exc

        body = response.payload
        if _optional_string(body.get("rt_cd")) != "0":
            raise _business_error(
                payload=body,
                request_metadata=request_metadata,
                request_payload=request_payload,
            )

        output = body.get("output")
        if not isinstance(output, dict):
            raise BrokerAdapterError(
                "KIS order response is missing output payload",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                request_metadata=request_metadata,
                request_payload=request_payload,
                response_payload=body,
            )

        observed_at = datetime.now(UTC)
        response_metadata = {
            "status_code": response.status_code,
            "headers": _sanitize_headers(response.headers),
            "rt_cd": body.get("rt_cd"),
            "msg_cd": body.get("msg_cd"),
            "msg1": body.get("msg1"),
            "received_at": observed_at.isoformat(),
        }
        return BrokerOrderPlacement(
            broker_order_id=_optional_string(output.get("ODNO")),
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status=_optional_string(body.get("msg_cd")),
            submitted_at=_submitted_at(output.get("ORD_TMD"), observed_at),
            observed_at=observed_at,
            request_metadata=request_metadata,
            response_metadata=response_metadata,
            request_payload=request_payload,
            response_payload=body,
            message=_optional_string(body.get("msg1")),
        )

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        config = self._configured()
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_CURRENT_PRICE_PATH,
            "method": "GET",
            "tr_id": KIS_CURRENT_PRICE_TR_ID,
            "requested_at": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "exchange": exchange,
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": _quote_market_division(exchange),
            "FID_INPUT_ISCD": symbol,
        }
        try:
            response = self.http_client.get_json(
                url=f"{config.api_base_url}{KIS_CURRENT_PRICE_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=KIS_CURRENT_PRICE_TR_ID,
                ),
                params=params,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
        except BrokerAdapterError as exc:
            raise BrokerAdapterError(
                str(exc),
                status_code=exc.status_code or 503,
                classification=exc.classification,
                retryable=exc.retryable,
                code=exc.code,
                request_metadata=request_metadata,
                response_metadata=exc.response_metadata,
                response_payload=exc.response_payload,
                observed_at=exc.observed_at,
            ) from exc

        body = response.payload
        if _optional_string(body.get("rt_cd")) != "0":
            raise BrokerAdapterError(
                _error_message(body),
                status_code=422,
                classification=_classify_error(
                    status_code=400,
                    code=_optional_string(body.get("msg_cd")),
                    message=_error_message(body),
                ),
                retryable=False,
                code=_optional_string(body.get("msg_cd")),
                request_metadata=request_metadata,
                response_payload=body,
                observed_at=datetime.now(UTC),
            )

        output = body.get("output")
        if not isinstance(output, dict):
            raise BrokerAdapterError(
                "KIS current price response is missing output payload",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                request_metadata=request_metadata,
                response_payload=body,
            )

        price = _parse_decimal(output.get("stck_prpr"))
        if price is None or price <= 0:
            return None
        return BrokerCurrentPrice(
            symbol=symbol,
            exchange=exchange,
            price=price,
            observed_at=datetime.now(UTC),
            payload=body,
        )

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext | None:
        config = self._configured()
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_ASKING_PRICE_PATH,
            "method": "GET",
            "tr_id": KIS_ASKING_PRICE_TR_ID,
            "requested_at": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "exchange": exchange,
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": _quote_market_division(exchange),
            "FID_INPUT_ISCD": symbol,
        }
        try:
            response = self.http_client.get_json(
                url=f"{config.api_base_url}{KIS_ASKING_PRICE_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=KIS_ASKING_PRICE_TR_ID,
                ),
                params=params,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
        except BrokerAdapterError as exc:
            raise BrokerAdapterError(
                str(exc),
                status_code=exc.status_code or 503,
                classification=exc.classification,
                retryable=exc.retryable,
                code=exc.code,
                request_metadata=request_metadata,
                response_metadata=exc.response_metadata,
                response_payload=exc.response_payload,
                observed_at=exc.observed_at,
            ) from exc

        body = response.payload
        if _optional_string(body.get("rt_cd")) != "0":
            raise BrokerAdapterError(
                _error_message(body),
                status_code=422,
                classification=_classify_error(
                    status_code=400,
                    code=_optional_string(body.get("msg_cd")),
                    message=_error_message(body),
                ),
                retryable=False,
                code=_optional_string(body.get("msg_cd")),
                request_metadata=request_metadata,
                response_payload=body,
                observed_at=datetime.now(UTC),
            )

        output = _first_payload_object(body, "output", "output1")
        if output is None:
            raise BrokerAdapterError(
                "KIS asking price response is missing output payload",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                request_metadata=request_metadata,
                response_payload=body,
            )

        bid = _first_decimal(output, "bid", "bid_price", "best_bid", "bidp1", "bidp")
        ask = _first_decimal(output, "ask", "ask_price", "best_ask", "askp1", "askp")
        if bid is None and ask is None:
            return None
        spread = _spread(bid=bid, ask=ask)
        mid_price = _mid_price(bid=bid, ask=ask) or reference_price
        ask_depth = _first_decimal(output, "total_askp_rsqn", "askp_rsqn_tvol", "ask_depth")
        bid_depth = _first_decimal(output, "total_bidp_rsqn", "bidp_rsqn_tvol", "bid_depth")
        if ask_depth is None:
            ask_depth = _sum_numbered_decimal(output, "askp_rsqn", 10)
        if bid_depth is None:
            bid_depth = _sum_numbered_decimal(output, "bidp_rsqn", 10)
        orderbook_depth = _sum_optional_decimals(ask_depth, bid_depth)
        return BrokerLiveMarketContext(
            symbol=symbol,
            exchange=exchange,
            observed_at=datetime.now(UTC),
            bid=bid,
            ask=ask,
            spread=spread,
            spread_bps=_spread_bps(spread=spread, mid_price=mid_price),
            orderbook_depth=orderbook_depth,
            orderbook_imbalance=_orderbook_imbalance(bid_depth=bid_depth, ask_depth=ask_depth),
            payload={
                "provider": self.provider,
                "endpoint": KIS_ASKING_PRICE_PATH,
                "tr_id": KIS_ASKING_PRICE_TR_ID,
                "response_metadata": {
                    "status_code": response.status_code,
                    "msg_cd": _optional_string(body.get("msg_cd")),
                    "msg1": _optional_string(body.get("msg1")),
                    "received_at": datetime.now(UTC).isoformat(),
                },
                "output": output,
            },
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del symbols, reference_prices
        if exchange != "KRX":
            raise BrokerAdapterError(
                f"unsupported exchange for account snapshot: {exchange}",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
            )
        config = self._configured()
        if not self.settings.kis_account_number:
            raise BrokerAdapterError(
                "KIS account number is not configured",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
            )

        observed_at = datetime.now(UTC)
        balance_response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_BALANCE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_balance_transaction_id(self.settings.runtime_mode),
            ),
            params=_balance_params(self.settings),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        open_orders_response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_OPEN_ORDERS_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_open_orders_transaction_id(self.settings.runtime_mode),
            ),
            params=_open_orders_params(self.settings, observed_at),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )

        _raise_if_business_error(balance_response.payload)
        _raise_if_business_error(open_orders_response.payload)
        available_cash = _parse_available_cash(balance_response.payload)
        if available_cash is None:
            raise BrokerAdapterError(
                "KIS account balance response is missing available cash",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                response_payload=balance_response.payload,
                observed_at=observed_at,
            )

        balance_summary = _parse_balance_summary(balance_response.payload)
        return BrokerAccountSnapshot(
            available_cash=available_cash,
            observed_at=observed_at,
            source=f"{self.provider}:account_snapshot",
            total_asset_value=balance_summary.get("total_asset_value"),
            previous_total_asset_value=balance_summary.get("previous_total_asset_value"),
            asset_change_amount=balance_summary.get("asset_change_amount"),
            unrealized_pnl=balance_summary.get("unrealized_pnl"),
            holdings=_parse_holdings(balance_response.payload),
            open_orders=_parse_open_orders(open_orders_response.payload),
            payload={
                "balance": balance_response.payload,
                "open_orders": open_orders_response.payload,
                "account_number_masked": _mask_account(self.settings.kis_account_number),
            },
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(
            supported=True,
            strategy="shared_realtime_subscription",
            details={
                "approval_path": KIS_WEBSOCKET_APPROVAL_PATH,
                "rest_fallback": "bounded_reconciliation_only",
            },
        )

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
        submitted_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        config = self._configured()
        observed_at = datetime.now(UTC)
        transaction_id = _daily_ccld_transaction_id(self.settings.runtime_mode)
        params = _reconcile_order_params(
            self.settings,
            broker_order_id,
            observed_at,
            exchange=exchange,
            submitted_at=submitted_at,
        )
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_DAILY_CCLD_PATH,
            "tr_id": transaction_id,
            "params": params,
        }
        try:
            response = self.http_client.get_json(
                url=f"{config.api_base_url}{KIS_DAILY_CCLD_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=transaction_id,
                ),
                params=params,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
            _raise_if_business_error(response.payload)
        except BrokerAdapterError as exc:
            if exc.request_metadata is None:
                exc.request_metadata = request_metadata
            raise
        normalized_order = _normalize_reconcile_order(
            response.payload,
            broker_order_id,
            observed_at=observed_at,
        )
        if normalized_order is None:
            return {
                "provider": self.provider,
                "broker_order_id": broker_order_id,
                "status": "unknown",
                "broker_status": "unknown",
                "observed_at": observed_at.isoformat(),
                "request_metadata": request_metadata,
                "response_metadata": {
                    "status_code": response.status_code,
                    "msg_cd": _optional_string(response.payload.get("msg_cd")),
                    "account_number_masked": _mask_account(self.settings.kis_account_number),
                },
                "message": "matching order was not found in broker order query",
                "payload": {"output": _safe_reconcile_payload(response.payload)},
            }

        return {
            "provider": self.provider,
            "broker_order_id": broker_order_id,
            "status": normalized_order["status"],
            "broker_status": normalized_order["broker_status"],
            "requested_quantity": normalized_order["requested_quantity"],
            "remaining_quantity": normalized_order["remaining_quantity"],
            "filled_quantity": normalized_order["filled_quantity"],
            "average_fill_price": normalized_order["average_fill_price"],
            "filled_at": normalized_order["filled_at"].isoformat(),
            "observed_at": normalized_order["observed_at"].isoformat(),
            "request_metadata": request_metadata,
            "response_metadata": {
                "status_code": response.status_code,
                "msg_cd": _optional_string(response.payload.get("msg_cd")),
                "account_number_masked": _mask_account(self.settings.kis_account_number),
            },
            "payload": {
                "order": normalized_order["raw"],
            },
            "message": normalized_order["message"],
        }

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {access_token}",
            "appkey": self.settings.kis_app_key or "",
            "appsecret": self.settings.kis_app_secret or "",
        }
        if transaction_id is not None:
            headers["tr_id"] = transaction_id
        if hashkey is not None:
            headers["hashkey"] = hashkey
        return headers

    def _configured(self) -> BrokerConfig:
        config = self.describe()
        if not config.configured or config.api_base_url is None:
            raise BrokerAdapterError(
                config.reason or "KIS configuration is unavailable",
                status_code=409,
            )
        return config

    def _issue_hashkey(
        self,
        access_token: str,
        request_payload: dict[str, object],
    ) -> str | None:
        if not self.settings.kis_use_hashkey:
            return None

        config = self._configured()
        response = self.http_client.post_json(
            url=f"{config.api_base_url}{KIS_HASHKEY_PATH}",
            headers=self.build_headers(access_token),
            payload=request_payload,
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        hashkey = _optional_string(response.payload.get("HASH"))
        if not hashkey:
            raise BrokerAdapterError(
                "KIS hashkey response is missing HASH",
                status_code=502,
                classification=BrokerErrorClassification.AUTH,
                normalized_status=NormalizedBrokerOrderStatus.FAILED,
                request_payload=request_payload,
                response_payload=response.payload,
            )
        return hashkey

def _reconcile_order_params(
    settings: Settings,
    broker_order_id: str,
    observed_at: datetime,
    *,
    exchange: str = "KRX",
    submitted_at: datetime | None = None,
) -> dict[str, object]:
    inquiry_end_date = observed_at.astimezone(KIS_TIMEZONE).date()
    inquiry_start_date = (
        submitted_at.astimezone(KIS_TIMEZONE).date()
        if submitted_at is not None
        else inquiry_end_date
    )
    if inquiry_start_date > inquiry_end_date:
        inquiry_start_date = inquiry_end_date
    normalized_exchange = exchange.strip().upper() or "KRX"
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "INQR_STRT_DT": inquiry_start_date.strftime("%Y%m%d"),
        "INQR_END_DT": inquiry_end_date.strftime("%Y%m%d"),
        "SLL_BUY_DVSN_CD": "00",
        "PDNO": "",
        "CCLD_DVSN": "00",
        "INQR_DVSN": "00",
        "INQR_DVSN_3": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": broker_order_id,
        "INQR_DVSN_1": "",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
        "EXCG_ID_DVSN_CD": normalized_exchange,
    }


def _normalize_reconcile_order(
    payload: dict[str, object],
    broker_order_id: str,
    *,
    observed_at: datetime,
) -> dict[str, object] | None:
    rows = _payload_rows(payload.get("output1"))
    for row in rows:
        row_order_id = _optional_string(row.get("odno")) or _optional_string(
            row.get("ord_no")
        )
        if row_order_id != broker_order_id:
            continue
        return _extract_reconcile_order_info(
            row,
            observed_at=observed_at,
            raw_status=(
                _rejection_status_from_order_kind(row)
                or _optional_string(row.get("ord_ccld_status_name"))
                or _optional_string(row.get("ord_st_dvsn_cd_name"))
                or _optional_string(row.get("ord_sts_name"))
            ),
        )
    return None


def _extract_reconcile_order_info(
    row: dict[str, object],
    *,
    observed_at: datetime,
    raw_status: str | None,
) -> dict[str, object] | None:
    requested_quantity = _parse_int(row.get("ord_qty"))
    if requested_quantity is None or requested_quantity <= 0:
        return None
    filled_quantity = _parse_int(row.get("tot_ccld_qty")) or 0
    remaining_quantity = _remaining_order_quantity(row, requested_quantity)
    status = _normalize_reconcile_status(
        raw_status=raw_status,
        requested_quantity=requested_quantity,
        remaining_quantity=remaining_quantity,
        filled_quantity=filled_quantity,
    )
    average_fill_price = (
        _parse_decimal(row.get("avg_upr"))
        or _parse_decimal(row.get("trd_prc"))
        or _parse_decimal(row.get("avg_pric"))
        or _parse_decimal(row.get("avg_prvs"))
        or _parse_decimal(row.get("ord_unpr"))
    )
    filled_at = _reconcile_filled_timestamp(row, observed_at=observed_at)
    return {
        "status": status,
        "broker_status": raw_status or status,
        "requested_quantity": requested_quantity,
        "remaining_quantity": remaining_quantity,
        "filled_quantity": filled_quantity,
        "average_fill_price": average_fill_price,
        "filled_at": filled_at,
        "raw": _safe_reconcile_payload(row),
        "observed_at": observed_at,
        "message": None,
    }


def _normalize_reconcile_status(
    *,
    raw_status: str | None,
    requested_quantity: int,
    remaining_quantity: int | None,
    filled_quantity: int,
) -> str:
    normalized = (raw_status or "").strip().lower().replace("-", "_").replace(" ", "_")
    status_lookup: dict[str, str] = {
        "미체결": "open",
        "부분체결": "partially_filled",
        "체결완료": "filled",
        "체결": "filled",
        "취소": "canceled",
        "거부": "rejected",
        "실패": "failed",
        "만료": "expired",
        "open": "open",
        "partially_filled": "partially_filled",
        "filled": "filled",
        "canceled": "canceled",
        "rejected": "rejected",
        "failed": "failed",
        "expired": "expired",
        "submission_pending": "submission_pending",
        "retry_pending": "retry_pending",
    }
    if normalized in status_lookup:
        return status_lookup[normalized]
    if remaining_quantity is not None and remaining_quantity <= 0 and filled_quantity <= 0:
        return "canceled"
    if remaining_quantity is not None and remaining_quantity <= 0:
        return "filled"
    if filled_quantity > 0:
        return "partially_filled"
    if requested_quantity > 0:
        return "open"
    return "unknown"


def _rejection_status_from_order_kind(row: dict[str, object]) -> str | None:
    raw = " ".join(
        str(value)
        for value in (
            row.get("sll_buy_dvsn_cd_name"),
            row.get("sll_buy_dvsn_name"),
            row.get("ord_dvsn_name"),
        )
        if value is not None
    )
    normalized = raw.strip().lower()
    if not normalized:
        return None
    if "거부" in normalized or "reject" in normalized:
        return "거부"
    if "실패" in normalized or "fail" in normalized:
        return "실패"
    if "취소" in normalized or "cancel" in normalized:
        return "취소"
    return None


def _reconcile_filled_timestamp(
    row: dict[str, object],
    *,
    observed_at: datetime,
) -> datetime:
    for time_value, date_value in (
        (row.get("ord_tmd"), row.get("ord_dt")),
        (row.get("ord_time"), row.get("ord_dt")),
        (row.get("ord_dtm"), None),
        (row.get("trad_dt"), row.get("ord_dt")),
    ):
        if not isinstance(time_value, str):
            continue
        timestamp = _parse_reconcile_datetime(
            time_value=time_value,
            date_value=_optional_string(date_value),
            observed_at=observed_at,
        )
        if timestamp is not None:
            return timestamp
    return observed_at


def _parse_reconcile_datetime(
    *,
    time_value: str,
    date_value: str | None,
    observed_at: datetime,
) -> datetime | None:
    normalized_time = time_value.replace(":", "").replace("-", "").replace(" ", "")
    if date_value is not None:
        candidate = f"{date_value}{normalized_time}".replace(" ", "")
        for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M%S%f"):
            try:
                return datetime.strptime(candidate, fmt).replace(tzinfo=UTC)
            except ValueError:
                pass
    for fmt in ("%H%M%S", "%H%M%S%f"):
        try:
            parsed = datetime.strptime(normalized_time, fmt)
            return observed_at.replace(
                hour=parsed.hour,
                minute=parsed.minute,
                second=parsed.second,
                microsecond=parsed.microsecond,
            )
        except ValueError:
            pass
    return None


def _safe_reconcile_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    output = dict(payload)
    output.pop("CANO", None)
    output.pop("ACNT_PRDT_CD", None)
    output.pop("appkey", None)
    output.pop("appsecret", None)
    return output


def _resolve_api_base_url(settings: Settings) -> str | None:
    if settings.runtime_mode == "live":
        return settings.kis_base_url
    if settings.runtime_mode == "paper":
        return settings.kis_mock_base_url
    return None


def _json_headers() -> dict[str, str]:
    return {
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BrokerAdapterError(f"KIS response is missing '{key}'", status_code=502)
    return value


def _optional_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _parse_optional_expiration(explicit: object, seconds: object) -> datetime | None:
    if explicit is None and seconds is None:
        return None
    return _parse_expiration(explicit, seconds)


def _parse_expiration(explicit: object, seconds: object) -> datetime:
    if isinstance(explicit, str) and explicit.strip():
        return datetime.strptime(explicit, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KIS_TIMEZONE)

    if isinstance(seconds, (int, float)):
        return datetime.now(UTC) + timedelta(seconds=float(seconds))

    raise BrokerAdapterError("KIS response is missing access token expiry metadata", status_code=502)


def _parse_json_payload(raw_body: str) -> dict[str, object]:
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise BrokerAdapterError("KIS returned invalid JSON", status_code=502) from exc
    if not isinstance(payload, dict):
        raise BrokerAdapterError("KIS returned a non-object JSON payload", status_code=502)
    return payload


def _error_message(payload: dict[str, object]) -> str:
    for key in ("message", "msg1", "error_description", "detail"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return "KIS request failed"


def _http_error(
    *,
    status_code: int,
    headers: dict[str, str],
    raw_body: str,
) -> BrokerAdapterError:
    payload = _parse_http_payload(raw_body)
    classification = _classify_error(
        status_code=status_code,
        code=_optional_string(payload.get("msg_cd")),
        message=_error_message(payload),
    )
    retryable = classification in {
        BrokerErrorClassification.NETWORK,
        BrokerErrorClassification.RATE_LIMIT,
        BrokerErrorClassification.TRANSIENT,
    }
    normalized_status = (
        NormalizedBrokerOrderStatus.RETRY_PENDING
        if retryable
        else NormalizedBrokerOrderStatus.REJECTED
    )
    return BrokerAdapterError(
        _error_message(payload),
        status_code=503 if retryable else 422,
        classification=classification,
        retryable=retryable,
        code=_optional_string(payload.get("msg_cd")),
        normalized_status=normalized_status,
        response_metadata={
            "status_code": status_code,
            "headers": _sanitize_headers(headers),
        },
        response_payload=payload,
        observed_at=datetime.now(UTC),
    )


def _parse_http_payload(raw_body: str) -> dict[str, object]:
    if not raw_body:
        return {}
    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError:
        return {"detail": raw_body}
    if isinstance(parsed, dict):
        return parsed
    return {"detail": raw_body}


def _classify_error(
    *,
    status_code: int,
    code: str | None,
    message: str,
) -> BrokerErrorClassification:
    message_lower = message.lower()
    code_lower = (code or "").lower()

    if status_code in {401, 403}:
        return BrokerErrorClassification.AUTH
    if status_code == 429:
        return BrokerErrorClassification.RATE_LIMIT
    if status_code in {408, 425, 500, 502, 503, 504}:
        return BrokerErrorClassification.TRANSIENT
    if code == "KIOK0320" or "장운영시간" in message:
        return BrokerErrorClassification.TRANSIENT

    if any(token in code_lower for token in ("auth", "token")) or any(
        token in message_lower for token in ("auth", "token", "인증")
    ):
        return BrokerErrorClassification.AUTH
    if "limit" in message_lower or "rate" in message_lower or "호출 제한" in message:
        return BrokerErrorClassification.RATE_LIMIT
    if any(token in message_lower for token in ("timeout", "tempor", "retry")):
        return BrokerErrorClassification.TRANSIENT
    if any(token in message_lower for token in ("quantity", "price", "input", "validation")):
        return BrokerErrorClassification.VALIDATION
    if any(token in message for token in ("수량", "가격", "입력")):
        return BrokerErrorClassification.VALIDATION
    if 400 <= status_code < 500:
        return BrokerErrorClassification.BROKER_REJECTED
    return BrokerErrorClassification.UNKNOWN


def _order_division(order_type: str, order_division: str | None = None) -> str:
    if order_division is not None:
        return order_division
    if order_type == "limit":
        return "00"
    if order_type == "market":
        return "01"
    raise BrokerAdapterError(
        f"unsupported order_type: {order_type}",
        status_code=422,
        classification=BrokerErrorClassification.VALIDATION,
        normalized_status=NormalizedBrokerOrderStatus.REJECTED,
    )


def _order_price(order_type: str, limit_price: Any, order_division: str | None = None) -> str:
    if order_division in {"05", "06"} and limit_price is None:
        return "0"
    if order_type == "market":
        return "0"
    if limit_price is None:
        raise BrokerAdapterError(
            "limit orders require limit_price",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    return str(limit_price)


def _quote_market_division(exchange: str) -> str:
    if exchange != "KRX":
        raise BrokerAdapterError(
            f"unsupported exchange for current price quotes: {exchange}",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
        )
    return "J"


def _first_payload_object(payload: dict[str, object], *keys: str) -> dict[str, object] | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return None


def _first_decimal(payload: dict[str, object], *keys: str) -> Decimal | None:
    for key in keys:
        value = _parse_decimal(payload.get(key))
        if value is not None:
            return value
    return None


def _sum_numbered_decimal(payload: dict[str, object], prefix: str, levels: int) -> Decimal | None:
    total = Decimal("0")
    found = False
    for level in range(1, levels + 1):
        value = _parse_decimal(payload.get(f"{prefix}{level}"))
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _sum_optional_decimals(*values: Decimal | None) -> Decimal | None:
    total = Decimal("0")
    found = False
    for value in values:
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _spread(*, bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    spread = ask - bid
    return spread if spread >= 0 else None


def _mid_price(*, bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / Decimal("2")
    return mid if mid > 0 else None


def _spread_bps(*, spread: Decimal | None, mid_price: Decimal | None) -> Decimal | None:
    if spread is None or mid_price is None or mid_price <= 0:
        return None
    return (spread / mid_price) * Decimal("10000")


def _orderbook_imbalance(
    *,
    bid_depth: Decimal | None,
    ask_depth: Decimal | None,
) -> Decimal | None:
    if bid_depth is None or ask_depth is None:
        return None
    total = bid_depth + ask_depth
    if total <= 0:
        return None
    return (bid_depth - ask_depth) / total


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _transaction_id(runtime_mode: str, side: str) -> str:
    live = runtime_mode == "live"
    if side == "sell":
        return "TTTC0011U" if live else "VTTC0011U"
    return "TTTC0012U" if live else "VTTC0012U"


def _balance_transaction_id(runtime_mode: str) -> str:
    return "TTTC8434R" if runtime_mode == "live" else "VTTC8434R"


def _open_orders_transaction_id(runtime_mode: str) -> str:
    return "TTTC8001R" if runtime_mode == "live" else "VTTC8001R"


def _daily_ccld_transaction_id(runtime_mode: str, *, period_division: str = "inner") -> str:
    before_period = period_division.strip().lower() == "before"
    if runtime_mode == "live":
        return "CTSC9215R" if before_period else "TTTC0081R"
    return "VTSC9215R" if before_period else "VTTC0081R"


def _balance_params(settings: Settings) -> dict[str, object]:
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }


def _open_orders_params(settings: Settings, observed_at: datetime) -> dict[str, object]:
    inquiry_date = observed_at.astimezone(KIS_TIMEZONE).strftime("%Y%m%d")
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "INQR_STRT_DT": inquiry_date,
        "INQR_END_DT": inquiry_date,
        "SLL_BUY_DVSN_CD": "00",
        "INQR_DVSN": "00",
        "PDNO": "",
        "CCLD_DVSN": "02",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }


def _raise_if_business_error(payload: dict[str, object]) -> None:
    if _optional_string(payload.get("rt_cd")) in {None, "0"}:
        return
    raise BrokerAdapterError(
        _error_message(payload),
        status_code=422,
        classification=_classify_error(
            status_code=400,
            code=_optional_string(payload.get("msg_cd")),
            message=_error_message(payload),
        ),
        retryable=False,
        code=_optional_string(payload.get("msg_cd")),
        response_payload=payload,
        observed_at=datetime.now(UTC),
    )


def _parse_available_cash(payload: dict[str, object]) -> Decimal | None:
    for row in _payload_rows(payload.get("output2")):
        for key in (
            "ord_psbl_cash",
            "ord_psbl_amt",
            "dnca_tot_amt",
            "nxdy_excc_amt",
            "prvs_rcdl_excc_amt",
            "tot_evlu_amt",
        ):
            amount = _parse_decimal(row.get(key))
            if amount is not None and amount >= 0:
                return amount
    return None


def _parse_balance_summary(payload: dict[str, object]) -> dict[str, Decimal | None]:
    for row in _payload_rows(payload.get("output2")):
        return {
            "total_asset_value": _first_decimal(row, "tot_evlu_amt", "nass_amt"),
            "previous_total_asset_value": _parse_decimal(row.get("bfdy_tot_asst_evlu_amt")),
            "asset_change_amount": _parse_decimal(row.get("asst_icdc_amt")),
            "unrealized_pnl": _parse_decimal(row.get("evlu_pfls_smtl_amt")),
        }
    return {
        "total_asset_value": None,
        "previous_total_asset_value": None,
        "asset_change_amount": None,
        "unrealized_pnl": None,
    }


def _first_decimal(row: dict[str, object], *keys: str) -> Decimal | None:
    for key in keys:
        parsed = _parse_decimal(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_holdings(payload: dict[str, object]) -> list[BrokerAccountHolding]:
    holdings: list[BrokerAccountHolding] = []
    for row in _payload_rows(payload.get("output1")):
        symbol = _optional_string(row.get("pdno"))
        quantity = _parse_int(row.get("hldg_qty"))
        if symbol is None or quantity is None or quantity <= 0:
            continue
        holdings.append(
            BrokerAccountHolding(
                symbol=symbol.strip().upper(),
                quantity=quantity,
                average_price=_parse_decimal(row.get("pchs_avg_pric")),
                sellable_quantity=_parse_int(row.get("ord_psbl_qty")),
                unrealized_pnl=_parse_decimal(row.get("evlu_pfls_amt")),
                unrealized_pnl_pct=_parse_decimal(row.get("evlu_pfls_rt")),
                payload=dict(row),
            )
        )
    return holdings


def _parse_open_orders(payload: dict[str, object]) -> list[BrokerAccountOpenOrder]:
    open_orders: list[BrokerAccountOpenOrder] = []
    for row in _payload_rows(payload.get("output1")):
        symbol = _optional_string(row.get("pdno"))
        quantity = _parse_int(row.get("ord_qty"))
        remaining_quantity = _remaining_order_quantity(row, quantity)
        if symbol is None or quantity is None or quantity <= 0 or remaining_quantity <= 0:
            continue
        open_orders.append(
            BrokerAccountOpenOrder(
                symbol=symbol.strip().upper(),
                side=_parse_order_side(row),
                quantity=quantity,
                remaining_quantity=remaining_quantity,
                status=_optional_string(row.get("ord_ccld_status_name")) or "open",
                broker_order_id=_optional_string(row.get("odno")),
                limit_price=_parse_decimal(row.get("ord_unpr")),
                payload=dict(row),
            )
        )
    return open_orders


def _payload_rows(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(Decimal(value.replace(",", "")))
        except (InvalidOperation, ValueError):
            return None
    return None


def _remaining_order_quantity(row: dict[str, object], quantity: int | None) -> int:
    for key in ("rmn_qty", "ord_uncc_qty", "nccs_qty"):
        value = _parse_int(row.get(key))
        if value is not None:
            return max(value, 0)
    filled = _parse_int(row.get("tot_ccld_qty"))
    if quantity is not None and filled is not None:
        return max(quantity - filled, 0)
    return quantity or 0


def _parse_order_side(row: dict[str, object]) -> str:
    raw = " ".join(
        str(value)
        for value in (
            row.get("sll_buy_dvsn_cd"),
            row.get("sll_buy_dvsn_cd_name"),
            row.get("sll_buy_dvsn_name"),
        )
        if value is not None
    )
    if "02" in raw or "매수" in raw or "buy" in raw.lower():
        return "buy"
    if "01" in raw or "매도" in raw or "sell" in raw.lower():
        return "sell"
    return "unknown"


def _submitted_at(order_time: object, fallback: datetime) -> datetime:
    if not isinstance(order_time, str) or len(order_time) != 6:
        return fallback
    try:
        return fallback.replace(
            hour=int(order_time[0:2]),
            minute=int(order_time[2:4]),
            second=int(order_time[4:6]),
            microsecond=0,
        )
    except ValueError:
        return fallback


def _mask_account(account_number: str | None) -> str | None:
    if not account_number:
        return None
    if len(account_number) <= 4:
        return account_number
    return f"{'*' * (len(account_number) - 4)}{account_number[-4:]}"


def _sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key, value in headers.items():
        key_lower = key.lower()
        if key_lower in {"authorization", "appsecret", "hashkey"}:
            continue
        sanitized[key_lower] = value
    return sanitized


def _business_error(
    *,
    payload: dict[str, object],
    request_metadata: dict[str, object],
    request_payload: dict[str, object],
) -> BrokerAdapterError:
    code = _optional_string(payload.get("msg_cd"))
    message = _error_message(payload)
    classification = _classify_error(status_code=400, code=code, message=message)
    retryable = classification in {
        BrokerErrorClassification.RATE_LIMIT,
        BrokerErrorClassification.TRANSIENT,
    }
    return BrokerAdapterError(
        message,
        status_code=503 if retryable else 422,
        classification=classification,
        retryable=retryable,
        code=code,
        normalized_status=(
            NormalizedBrokerOrderStatus.RETRY_PENDING
            if retryable
            else NormalizedBrokerOrderStatus.REJECTED
        ),
        request_metadata=dict(request_metadata),
        request_payload=dict(request_payload),
        response_payload=dict(payload),
        observed_at=datetime.now(UTC),
    )
