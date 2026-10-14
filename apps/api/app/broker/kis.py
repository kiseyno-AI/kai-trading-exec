from __future__ import annotations

import json
import re
import ssl
import threading
import time
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Protocol
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from app.broker.base import (
    BrokerAdapter,
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerSymbolBuyingPower,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerIntradayLiquiditySnapshot,
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
from app.broker.kis_token_coordinator import (
    invalidate_shared_kis_token,
)
from app.broker.error_safety import (
    safe_broker_error_code,
    safe_broker_error_message,
    sanitize_broker_error_payload,
)

KIS_PROVIDER = "kis"
KIS_DOMESTIC_EXCHANGES = frozenset({"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"})
KIS_US_EXCHANGES = frozenset({"NASD", "NYSE", "AMEX"})
KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE = "kis_us_all_holdings_only_v1"
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
KIS_INTRADAY_MINUTE_PRICE_PATH = (
    "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
)
KIS_HOLIDAY_CALENDAR_PATH = "/uapi/domestic-stock/v1/quotations/chk-holiday"
KIS_OVERSEAS_ORDER_PATH = "/uapi/overseas-stock/v1/trading/order"
KIS_OVERSEAS_MINUTE_PRICE_PATH = (
    "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
)
KIS_OVERSEAS_LAST_PRICE_PATH = "/uapi/overseas-price/v1/quotations/price"
KIS_OVERSEAS_ASKING_PRICE_PATH = (
    "/uapi/overseas-price/v1/quotations/inquire-asking-price"
)
# Backward-compatible name for callers that treat this as the adapter's
# current-price endpoint. The canonical implementation requires timestamped
# minute data rather than the untimestamped legacy `/price` response.
KIS_OVERSEAS_CURRENT_PRICE_PATH = KIS_OVERSEAS_MINUTE_PRICE_PATH
KIS_OVERSEAS_BALANCE_PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
KIS_OVERSEAS_OPEN_ORDERS_PATH = "/uapi/overseas-stock/v1/trading/inquire-nccs"
KIS_OVERSEAS_CCLD_PATH = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
KIS_OVERSEAS_BUYING_POWER_PATH = "/uapi/overseas-stock/v1/trading/inquire-psamount"
KIS_CURRENT_PRICE_TR_ID = "FHKST01010100"
KIS_ASKING_PRICE_TR_ID = "FHKST01010200"
KIS_INTRADAY_MINUTE_PRICE_TR_ID = "FHKST03010200"
KIS_HOLIDAY_CALENDAR_TR_ID = "CTCA0903R"
KIS_OVERSEAS_MINUTE_PRICE_TR_ID = "HHDFS76950200"
KIS_OVERSEAS_LAST_PRICE_TR_ID = "HHDFS00000300"
KIS_OVERSEAS_ASKING_PRICE_TR_ID = "HHDFS76200100"
KIS_OVERSEAS_QUOTE_EMPTY_CODE = "KIS_OVERSEAS_QUOTE_EMPTY"
KIS_TIMEZONE = timezone(timedelta(hours=9))
US_EASTERN_TIMEZONE = ZoneInfo("America/New_York")
_KIS_TOKEN_EXPIRY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
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
    pagination: dict[str, object] = field(default_factory=dict)


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
    def __init__(
        self,
        *,
        ssl_context: ssl.SSLContext | None = None,
        min_request_interval_seconds: float = 0.0,
        get_network_retry_attempts: int = 0,
        get_network_retry_backoff_seconds: float = 0.0,
    ) -> None:
        self._ssl_context = ssl_context or _default_ssl_context()
        self._min_request_interval_seconds = max(0.0, min_request_interval_seconds)
        self._get_network_retry_attempts = max(0, get_network_retry_attempts)
        self._get_network_retry_backoff_seconds = max(
            0.0, get_network_retry_backoff_seconds
        )
        self._request_lock = threading.Lock()
        self._last_request_started_at: float | None = None

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
        for attempt in range(self._get_network_retry_attempts + 1):
            try:
                return self._request_json(
                    request=request,
                    timeout_seconds=timeout_seconds,
                )
            except BrokerAdapterError as exc:
                should_retry = (
                    exc.retryable
                    and exc.classification == BrokerErrorClassification.NETWORK
                    and attempt < self._get_network_retry_attempts
                )
                if not should_retry:
                    raise
                if self._get_network_retry_backoff_seconds > 0:
                    time.sleep(
                        self._get_network_retry_backoff_seconds * (attempt + 1)
                    )
        raise AssertionError("unreachable KIS GET retry loop")

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
        self._wait_for_request_slot()
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
                safe_broker_error_message(exc.reason),
                status_code=503,
                classification=BrokerErrorClassification.NETWORK,
                retryable=True,
                normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            ) from exc
        except TimeoutError as exc:
            raise BrokerAdapterError(
                safe_broker_error_message(exc),
                status_code=503,
                classification=BrokerErrorClassification.NETWORK,
                retryable=True,
                normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            ) from exc

    def _wait_for_request_slot(self) -> None:
        if self._min_request_interval_seconds <= 0:
            return
        with self._request_lock:
            now = time.monotonic()
            if self._last_request_started_at is not None:
                remaining = (
                    self._min_request_interval_seconds
                    - (now - self._last_request_started_at)
                )
                if remaining > 0:
                    time.sleep(remaining)
                    now = time.monotonic()
            self._last_request_started_at = now


class _TokenInvalidatingJsonHttpClient:
    def __init__(self, *, settings: Settings, delegate: JsonHttpClient) -> None:
        self._settings = settings
        self._delegate = delegate

    def get_json(self, **kwargs: object) -> JsonHttpResponse:
        return self._request(self._delegate.get_json, kwargs)

    def post_json(self, **kwargs: object) -> JsonHttpResponse:
        return self._request(self._delegate.post_json, kwargs)

    def _request(self, operation, kwargs: dict[str, object]) -> JsonHttpResponse:
        headers = kwargs.get("headers")
        access_token = _bearer_token(headers if isinstance(headers, dict) else {})
        try:
            response = operation(**kwargs)
        except BrokerAdapterError as exc:
            raw_status = exc.response_metadata.get("status_code")
            if access_token is not None and raw_status == 401:
                invalidate_shared_kis_token(
                    settings=self._settings,
                    access_token=access_token,
                )
            raise
        if (
            access_token is not None
            and response.status_code == 200
            and response.payload.get("msg_cd") == "EGW00123"
        ):
            invalidate_shared_kis_token(
                settings=self._settings,
                access_token=access_token,
            )
        return response


class KisBrokerAdapter(BrokerAdapter):
    provider = KIS_PROVIDER

    def __init__(
        self,
        *,
        settings: Settings,
        http_client: JsonHttpClient | None = None,
    ) -> None:
        self.settings = settings
        delegate = http_client or UrllibJsonHttpClient(
            min_request_interval_seconds=settings.kis_min_request_interval_seconds,
            get_network_retry_attempts=settings.kis_get_network_retry_attempts,
            get_network_retry_backoff_seconds=(
                settings.kis_get_network_retry_backoff_seconds
            ),
        )
        self.http_client = _TokenInvalidatingJsonHttpClient(
            settings=settings,
            delegate=delegate,
        )
        self._krx_calendar_cache: dict[date, tuple[bool, datetime]] = {}
        self._krx_calendar_cache_lock = threading.Lock()

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
        issuance_started_at = datetime.now(UTC)
        response = self.http_client.post_json(
            url=f"{config.api_base_url}{KIS_TOKEN_PATH}",
            headers=_json_headers(),
            payload={
                "grant_type": "client_credentials",
                "appkey": self.settings.kis_app_key or "",
                "appsecret": self.settings.kis_app_secret or "",
            },
            timeout_seconds=self.settings.kis_token_request_timeout_seconds,
        )
        access_token = _required_string(response.payload, "access_token")
        token_type = str(response.payload.get("token_type") or "Bearer")
        access_token_expires_at = _parse_expiration(
            response.payload.get("access_token_token_expired"),
            response.payload.get("expires_in"),
            reference_at=issuance_started_at,
            require_seconds=True,
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
        if request.asset_class == "us_equity":
            return self._place_us_equity_order(access_token, request)
        tr_id = _transaction_id(self.settings.runtime_mode, request.side)
        request_payload = {
            "CANO": self.settings.kis_account_number or "",
            "ACNT_PRDT_CD": self.settings.kis_account_product_code,
            "PDNO": request.symbol,
            "ORD_DVSN": _order_division(
                request.order_type,
                request.order_division,
                request.time_in_force,
            ),
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
                response_payload=_safe_error_payload(body),
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

    def _place_us_equity_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        config = self._configured()
        _validate_us_equity_order_request(request)
        if request.side == "buy" and self.settings.kis_us_preflight_buying_power_enabled:
            self._validate_us_equity_buying_power(access_token, request)
        tr_id = _overseas_order_transaction_id(self.settings.runtime_mode, request.side)
        request_payload = {
            "CANO": self.settings.kis_account_number or "",
            "ACNT_PRDT_CD": self.settings.kis_account_product_code,
            "OVRS_EXCG_CD": request.exchange,
            "PDNO": request.symbol,
            "ORD_QTY": str(request.quantity),
            "OVRS_ORD_UNPR": str(request.limit_price),
            "CTAC_TLNO": "",
            "MGCO_APTM_ODNO": "",
            "SLL_TYPE": "00" if request.side == "sell" else "",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",
        }
        hashkey = self._issue_hashkey(access_token, request_payload)
        headers = self.build_headers(access_token, transaction_id=tr_id, hashkey=hashkey)
        headers["tr_cont"] = ""
        headers["custtype"] = self.settings.kis_customer_type
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_OVERSEAS_ORDER_PATH,
            "method": "POST",
            "tr_id": tr_id,
            "requested_at": datetime.now(UTC).isoformat(),
            "account_number_masked": _mask_account(self.settings.kis_account_number),
            "account_product_code": self.settings.kis_account_product_code,
            "asset_class": "us_equity",
            "currency": "USD",
            "route": {
                "exchange": request.exchange,
                "session": request.route_session or "regular",
                "ord_dvsn": "00",
                "reason": request.route_reason,
            },
        }
        try:
            response = self.http_client.post_json(
                url=f"{config.api_base_url}{KIS_OVERSEAS_ORDER_PATH}",
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
                "KIS overseas order response is missing output payload",
                status_code=502,
                request_metadata=request_metadata,
                request_payload=request_payload,
                response_payload=_safe_error_payload(body),
            )
        observed_at = datetime.now(UTC)
        return BrokerOrderPlacement(
            broker_order_id=_optional_string(output.get("ODNO")),
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status=_optional_string(body.get("msg_cd")),
            submitted_at=_submitted_at(output.get("ORD_TMD"), observed_at),
            observed_at=observed_at,
            request_metadata=request_metadata,
            response_metadata={
                "status_code": response.status_code,
                "headers": _sanitize_headers(response.headers),
                "rt_cd": body.get("rt_cd"),
                "msg_cd": body.get("msg_cd"),
                "msg1": body.get("msg1"),
                "received_at": observed_at.isoformat(),
            },
            request_payload=request_payload,
            response_payload=body,
            message=_optional_string(body.get("msg1")),
        )

    def _validate_us_equity_buying_power(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> None:
        assert request.limit_price is not None and request.quantity is not None
        config = self._configured()
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_OVERSEAS_BUYING_POWER_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_overseas_buying_power_transaction_id(self.settings.runtime_mode),
            ),
            params=_overseas_buying_power_params(
                self.settings,
                exchange=request.exchange,
                symbol=request.symbol,
                price=request.limit_price,
            ),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        buying_power = _parse_overseas_buying_power(request.symbol, response.payload)
        if buying_power is None or buying_power.buyable_quantity is None:
            raise BrokerAdapterError(
                "KIS overseas buying-power response is incomplete; buy blocked fail-closed",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                response_payload=response.payload,
            )
        if request.quantity > buying_power.buyable_quantity:
            raise BrokerAdapterError(
                "KIS US-equity buy quantity exceeds broker-reported buying power",
                status_code=422,
                classification=BrokerErrorClassification.BROKER_REJECTED,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                response_payload=response.payload,
            )

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        if exchange in {"NASD", "NYSE", "AMEX"}:
            return self._get_us_equity_current_price(
                access_token,
                symbol=symbol,
                exchange=exchange,
            )
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
            raw_error_message = _error_message(body)
            raise BrokerAdapterError(
                safe_broker_error_message(raw_error_message),
                status_code=422,
                classification=_classify_error(
                    status_code=400,
                    code=_optional_string(body.get("msg_cd")),
                    message=raw_error_message,
                ),
                retryable=False,
                code=safe_broker_error_code(
                    body.get("msg_cd"),
                    provider=KIS_PROVIDER,
                ),
                request_metadata=request_metadata,
                response_payload=_safe_error_payload(body),
                observed_at=datetime.now(UTC),
            )

        output = body.get("output")
        if not isinstance(output, dict):
            raise BrokerAdapterError(
                "KIS current price response is missing output payload",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                request_metadata=request_metadata,
                response_payload=_safe_error_payload(body),
            )

        price = _parse_decimal(output.get("stck_prpr"))
        if price is None or price <= 0:
            return None
        received_at = datetime.now(UTC)
        return BrokerCurrentPrice(
            symbol=symbol,
            exchange=exchange,
            price=price,
            observed_at=received_at,
            payload=body,
            freshness_source="transport_received_at_untrusted",
        )

    def is_krx_trading_day(
        self,
        access_token: str,
        *,
        market_date: date,
    ) -> bool | None:
        now = datetime.now(UTC)
        with self._krx_calendar_cache_lock:
            cached = self._krx_calendar_cache.get(market_date)
            if cached is not None and cached[1] > now:
                return cached[0]
        config = self._configured()
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_HOLIDAY_CALENDAR_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=KIS_HOLIDAY_CALENDAR_TR_ID,
            ),
            params={
                "BASS_DT": market_date.strftime("%Y%m%d"),
                "CTX_AREA_NK": "",
                "CTX_AREA_FK": "",
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        output = response.payload.get("output")
        rows = output if isinstance(output, list) else [output]
        expected_date = market_date.strftime("%Y%m%d")
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_date = _optional_string(
                row.get("bass_dt")
                or row.get("BASS_DT")
                or row.get("base_dt")
                or row.get("BASE_DT")
            )
            if row_date != expected_date:
                continue
            open_flag = _optional_string(row.get("opnd_yn") or row.get("OPND_YN"))
            if open_flag is None:
                return None
            normalized = open_flag.strip().upper()
            if normalized in {"Y", "YES", "TRUE", "1"}:
                result = True
                break
            if normalized in {"N", "NO", "FALSE", "0"}:
                result = False
                break
            return None
        else:
            return None
        with self._krx_calendar_cache_lock:
            self._krx_calendar_cache[market_date] = (
                result,
                now + timedelta(hours=6),
            )
        return result

    def _get_us_equity_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str,
    ) -> BrokerCurrentPrice | None:
        config = self._configured()
        params = {
            "AUTH": "",
            "EXCD": _overseas_quote_exchange(exchange),
            "SYMB": symbol,
            "NMIN": "1",
            "PINC": "0",
            "NEXT": "",
            "NREC": "1",
            "FILL": "",
            "KEYB": "",
        }
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_OVERSEAS_MINUTE_PRICE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=KIS_OVERSEAS_MINUTE_PRICE_TR_ID,
            ),
            params=params,
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        raw_output_rows = response.payload.get("output2")
        output_rows = _payload_rows(raw_output_rows)
        if raw_output_rows == []:
            fallback_quote = self._get_us_equity_last_price(
                access_token,
                symbol=symbol,
                exchange=exchange,
            )
            if fallback_quote is not None:
                return fallback_quote
            raise BrokerAdapterError(
                "KIS overseas minute price response contains no output2 rows",
                status_code=502,
                code=KIS_OVERSEAS_QUOTE_EMPTY_CODE,
                response_payload=response.payload,
                observed_at=datetime.now(UTC),
            )
        if not output_rows:
            raise BrokerAdapterError(
                "KIS overseas minute price response is missing output2 payload",
                status_code=502,
                response_payload=response.payload,
                observed_at=datetime.now(UTC),
            )
        for output in output_rows:
            price = _first_decimal(output, "last", "stck_prpr", "clos")
            observed_at = _parse_overseas_bar_observed_at(output, exchange=exchange)
            if price is None or price <= 0 or observed_at is None:
                continue
            return BrokerCurrentPrice(
                symbol=symbol,
                exchange=exchange,
                price=price,
                observed_at=observed_at,
                payload=response.payload,
                freshness_source="kis_overseas_exchange_bar_time",
            )
        return None

    def _get_us_equity_last_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str,
    ) -> BrokerCurrentPrice | None:
        """Read the official current-price endpoint after a valid empty minute response.

        The endpoint has no authoritative exchange timestamp, so its value is only
        a reference price. Monitoring still requires the separately fetched,
        timestamped order book before a live order can become executable.
        """
        config = self._configured()
        quote_exchange = _overseas_quote_exchange(exchange)
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_OVERSEAS_LAST_PRICE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=KIS_OVERSEAS_LAST_PRICE_TR_ID,
            ),
            params={"AUTH": "", "EXCD": quote_exchange, "SYMB": symbol},
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        rows = _payload_rows(response.payload.get("output"))
        if len(rows) != 1:
            return None
        output = rows[0]
        expected_symbol = symbol.strip().upper()
        expected_rsym = f"D{quote_exchange}{expected_symbol}"
        response_symbol = _optional_string(
            output.get("symb")
            or output.get("SYMB")
            or output.get("code")
            or output.get("CODE")
        )
        response_rsym = _optional_string(output.get("rsym") or output.get("RSYM"))
        if (response_symbol is None and response_rsym is None) or (
            response_symbol is not None
            and response_symbol.strip().upper() != expected_symbol
        ) or (
            response_rsym is not None
            and response_rsym.strip().upper() != expected_rsym
        ):
            return None
        price = _first_decimal(output, "last")
        if price is None or price <= 0:
            return None
        received_at = datetime.now(UTC)
        payload = dict(response.payload)
        payload.update(
            {
                "provider": self.provider,
                "endpoint": KIS_OVERSEAS_LAST_PRICE_PATH,
                "tr_id": KIS_OVERSEAS_LAST_PRICE_TR_ID,
                "requested_exchange": exchange,
                "quote_exchange": quote_exchange,
                "fallback_reason": "overseas_minute_rows_empty",
            }
        )
        return BrokerCurrentPrice(
            symbol=expected_symbol,
            exchange=exchange,
            price=price,
            observed_at=received_at,
            payload=payload,
            freshness_source="transport_received_at_untrusted",
        )

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext | None:
        if exchange.strip().upper() in {"NASD", "NYSE", "AMEX"}:
            return self._get_us_equity_live_market_context(
                access_token,
                symbol=symbol,
                exchange=exchange.strip().upper(),
                reference_price=reference_price,
            )
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
            raw_error_message = _error_message(body)
            raise BrokerAdapterError(
                safe_broker_error_message(raw_error_message),
                status_code=422,
                classification=_classify_error(
                    status_code=400,
                    code=_optional_string(body.get("msg_cd")),
                    message=raw_error_message,
                ),
                retryable=False,
                code=safe_broker_error_code(
                    body.get("msg_cd"),
                    provider=KIS_PROVIDER,
                ),
                request_metadata=request_metadata,
                response_payload=_safe_error_payload(body),
                observed_at=datetime.now(UTC),
            )

        output = _first_payload_object(body, "output", "output1")
        if output is None:
            raise BrokerAdapterError(
                "KIS asking price response is missing output payload",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                request_metadata=request_metadata,
                response_payload=_safe_error_payload(body),
            )

        bid = _first_decimal(output, "bid", "bid_price", "best_bid", "bidp1", "bidp")
        ask = _first_decimal(output, "ask", "ask_price", "best_ask", "askp1", "askp")
        if bid is None and ask is None:
            return None
        received_at = datetime.now(UTC)
        exchange_observed_at = _parse_krx_orderbook_observed_at(
            output,
            received_at=received_at,
        )
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
            observed_at=exchange_observed_at or received_at,
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
                "field_sources": {
                    "bid": {
                        "field": _first_present_key(
                            output, "bid", "bid_price", "best_bid", "bidp1", "bidp"
                        ),
                        "endpoint": KIS_ASKING_PRICE_PATH,
                        "tr_id": KIS_ASKING_PRICE_TR_ID,
                    },
                    "ask": {
                        "field": _first_present_key(
                            output, "ask", "ask_price", "best_ask", "askp1", "askp"
                        ),
                        "endpoint": KIS_ASKING_PRICE_PATH,
                        "tr_id": KIS_ASKING_PRICE_TR_ID,
                    },
                },
                "response_metadata": {
                    "status_code": response.status_code,
                    "msg_cd": _optional_string(body.get("msg_cd")),
                    "msg1": _optional_string(body.get("msg1")),
                    "received_at": received_at.isoformat(),
                },
                "output": output,
            },
            freshness_source=(
                "kis_exchange_orderbook_time"
                if exchange_observed_at is not None
                else "transport_received_at_untrusted"
            ),
        )

    def _get_us_equity_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str,
        reference_price: Decimal | None,
    ) -> BrokerLiveMarketContext | None:
        config = self._configured()
        quote_exchange = _overseas_quote_exchange(exchange)
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_OVERSEAS_ASKING_PRICE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=KIS_OVERSEAS_ASKING_PRICE_TR_ID,
            ),
            params={"AUTH": "", "EXCD": quote_exchange, "SYMB": symbol},
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        rows = [
            row
            for key in ("output1", "output2", "output3", "output")
            for row in _payload_rows(response.payload.get(key))
        ]
        expected_symbol = symbol.strip().upper()
        expected_rsym = f"D{quote_exchange}{expected_symbol}"
        identified_rows = [
            row
            for row in rows
            if _optional_string(row.get("code") or row.get("CODE")) is not None
            or _optional_string(row.get("rsym") or row.get("RSYM")) is not None
        ]
        identity_rows = [
            row
            for row in identified_rows
            if (
                (code := _optional_string(row.get("code") or row.get("CODE")))
                is None
                or code.strip().upper() == expected_symbol
            )
            and (
                (rsym := _optional_string(row.get("rsym") or row.get("RSYM")))
                is None
                or rsym.strip().upper() == expected_rsym
            )
        ]
        if not identity_rows or len(identity_rows) != len(identified_rows):
            return None
        bid, bid_field = _first_decimal_from_rows(
            rows, "pbid1", "PBID1", "bidp1", "BIDP1"
        )
        ask, ask_field = _first_decimal_from_rows(
            rows, "pask1", "PASK1", "askp1", "ASKP1"
        )
        observed_at = next(
            (
                parsed
                for row in identity_rows
                if (parsed := _parse_overseas_orderbook_observed_at(row, exchange=exchange))
                is not None
            ),
            None,
        )
        if observed_at is None or (bid is None and ask is None):
            return None
        spread = _spread(bid=bid, ask=ask)
        mid_price = _mid_price(bid=bid, ask=ask) or reference_price
        local_observed = observed_at.astimezone(US_EASTERN_TIMEZONE)
        local_time = local_observed.time()
        normal_ordering_available = bool(
            local_observed.weekday() < 5
            and local_time >= datetime.min.time().replace(hour=9, minute=30)
            and local_time < datetime.min.time().replace(hour=16)
        )
        return BrokerLiveMarketContext(
            symbol=expected_symbol,
            exchange=exchange,
            observed_at=observed_at,
            bid=bid,
            ask=ask,
            spread=spread,
            spread_bps=_spread_bps(spread=spread, mid_price=mid_price),
            normal_ordering_available=normal_ordering_available,
            payload={
                "provider": self.provider,
                "endpoint": KIS_OVERSEAS_ASKING_PRICE_PATH,
                "tr_id": KIS_OVERSEAS_ASKING_PRICE_TR_ID,
                "requested_exchange": exchange,
                "quote_exchange": quote_exchange,
                "field_sources": {
                    "bid": {
                        "field": bid_field,
                        "endpoint": KIS_OVERSEAS_ASKING_PRICE_PATH,
                        "tr_id": KIS_OVERSEAS_ASKING_PRICE_TR_ID,
                    },
                    "ask": {
                        "field": ask_field,
                        "endpoint": KIS_OVERSEAS_ASKING_PRICE_PATH,
                        "tr_id": KIS_OVERSEAS_ASKING_PRICE_TR_ID,
                    },
                },
                "response_metadata": {
                    "status_code": response.status_code,
                    "msg_cd": _optional_string(response.payload.get("msg_cd")),
                    "msg1": _optional_string(response.payload.get("msg1")),
                },
                "output1": response.payload.get("output1"),
                "output2": response.payload.get("output2"),
                "output3": response.payload.get("output3"),
            },
            freshness_source="kis_overseas_exchange_orderbook_time",
        )

    def get_intraday_liquidity_snapshot(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_at: datetime,
    ) -> BrokerIntradayLiquiditySnapshot | None:
        normalized_exchange = exchange.strip().upper()
        if normalized_exchange not in {"KRX", "KOSPI", "KOSDAQ"}:
            raise BrokerAdapterError(
                "KIS intraday liquidity supports Korean equity exchanges only",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_unsupported_exchange",
            )
        reference_utc = (
            reference_at.astimezone(UTC)
            if reference_at.tzinfo is not None and reference_at.utcoffset() is not None
            else None
        )
        if reference_utc is None:
            raise BrokerAdapterError(
                "KIS intraday liquidity requires a timezone-aware reference",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_reference_timestamp_invalid",
            )
        local_reference = reference_utc.astimezone(KIS_TIMEZONE)
        config = self._configured()
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_INTRADAY_MINUTE_PRICE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=KIS_INTRADAY_MINUTE_PRICE_TR_ID,
            ),
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": local_reference.strftime("%H%M%S"),
                "FID_PW_DATA_INCU_YN": "N",
                "FID_ETC_CLS_CODE": "",
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
        _raise_if_business_error(response.payload)
        output1 = _first_payload_object(response.payload, "output1")
        output2 = _payload_rows(response.payload.get("output2"))
        if output1 is None or not output2 or not isinstance(output2[0], dict):
            raise BrokerAdapterError(
                "KIS intraday liquidity response is empty",
                status_code=502,
                classification=BrokerErrorClassification.TRANSIENT,
                retryable=True,
                code="kis_intraday_liquidity_empty_response",
                response_metadata={
                    "status_code": response.status_code,
                    "msg_cd": _optional_string(response.payload.get("msg_cd")),
                },
            )
        latest = output2[0]
        observed_at = _parse_krx_dated_observed_at(
            latest.get("stck_bsop_date") or latest.get("STCK_BSOP_DATE"),
            latest.get("stck_cntg_hour") or latest.get("STCK_CNTG_HOUR"),
        )
        if observed_at is None:
            raise BrokerAdapterError(
                "KIS intraday liquidity timestamp is invalid",
                status_code=502,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_timestamp_invalid",
            )
        if observed_at.astimezone(KIS_TIMEZONE).date() != local_reference.date():
            raise BrokerAdapterError(
                "KIS intraday liquidity is not for the requested market date",
                status_code=502,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_wrong_market_date",
            )
        minute_volume = _parse_decimal(latest.get("cntg_vol"))
        turnover = _parse_decimal(latest.get("acml_tr_pbmn"))
        volume = _parse_decimal(output1.get("acml_vol"))
        day_high = _parse_decimal(output1.get("stck_hgpr"))
        day_low = _parse_decimal(output1.get("stck_lwpr"))
        if any(
            value is None or value < 0
            for value in (minute_volume, turnover, volume)
        ) or any(
            value is None or value <= 0
            for value in (day_high, day_low)
        ):
            raise BrokerAdapterError(
                "KIS intraday liquidity numeric evidence is invalid",
                status_code=502,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_numeric_evidence_invalid",
            )
        if day_low > day_high:
            raise BrokerAdapterError(
                "KIS intraday liquidity day range is invalid",
                status_code=502,
                classification=BrokerErrorClassification.VALIDATION,
                code="kis_intraday_liquidity_day_range_invalid",
            )
        assert turnover is not None
        assert volume is not None
        assert day_high is not None
        assert day_low is not None
        return BrokerIntradayLiquiditySnapshot(
            symbol=symbol,
            exchange="KRX",
            observed_at=observed_at,
            volume=volume,
            turnover=turnover,
            day_high=day_high,
            day_low=day_low,
            payload={
                "provider": self.provider,
                "endpoint": KIS_INTRADAY_MINUTE_PRICE_PATH,
                "tr_id": KIS_INTRADAY_MINUTE_PRICE_TR_ID,
                "response_metadata": {
                    "status_code": response.status_code,
                },
            },
            freshness_source="kis_exchange_intraday_minute_time",
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        normalized_exchange = exchange.strip().upper()
        if normalized_exchange in KIS_US_EXCHANGES:
            return self._get_us_equity_account_snapshot(
                access_token,
                symbols=symbols,
                reference_prices=reference_prices,
                exchange=normalized_exchange,
            )
        del symbols, reference_prices
        if normalized_exchange not in KIS_DOMESTIC_EXCHANGES:
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
        balance_response = _get_domestic_paginated_json(
            http_client=self.http_client,
            url=f"{config.api_base_url}{KIS_BALANCE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_balance_transaction_id(self.settings.runtime_mode),
            ),
            params=_balance_params(self.settings),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
            row_keys=("output1", "output2"),
        )
        _raise_if_business_error(balance_response.payload)
        open_orders_error = None
        try:
            open_orders_response = _get_domestic_paginated_json(
                http_client=self.http_client,
                url=f"{config.api_base_url}{KIS_OPEN_ORDERS_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=_open_orders_transaction_id(self.settings.runtime_mode),
                ),
                params=_open_orders_params(
                    self.settings,
                    observed_at,
                ),
                timeout_seconds=self.settings.kis_request_timeout_seconds,
                row_keys=("output1",),
            )
        except BrokerAdapterError as exc:
            # KIS can reject the same-day open-order lookup with KIER2570 even
            # while the balance endpoint is healthy. Holdings and sellable
            # quantities remain authoritative and must still refresh so a
            # protective sell is not blocked by stale local position truth.
            if exc.code != "KIER2570":
                raise
            open_orders_error = {
                "code": exc.code,
                "message": str(exc),
                "degraded": True,
            }
            open_orders_response = JsonHttpResponse(
                status_code=exc.status_code or 422,
                headers={},
                payload=dict(exc.response_payload),
                pagination={
                    "complete": False,
                    "page_count": None,
                    "cursor_width": 100,
                    "reason": "broker_rejected_daily_order_query",
                },
            )
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
        holdings = _parse_holdings(balance_response.payload)
        open_orders = (
            []
            if open_orders_error is not None
            else _parse_open_orders(open_orders_response.payload)
        )
        holdings_parse = _collection_parse_diagnostics(
            payload=balance_response.payload,
            row_key="output1",
            parsed_identities=[holding.symbol for holding in holdings],
            assess_row=_assess_domestic_holding_row,
        )
        open_orders_parse = _collection_parse_diagnostics(
            payload=open_orders_response.payload,
            row_key="output1",
            parsed_identities=[order.broker_order_id for order in open_orders],
            assess_row=_assess_domestic_open_order_row,
        )
        holdings_complete = bool(
            balance_response.pagination.get("complete") is True
        ) and _collection_parse_complete(
            payload=balance_response.payload,
            row_key="output1",
            diagnostics=holdings_parse,
        )
        open_orders_complete = (
            open_orders_error is None
            and open_orders_response.pagination.get("complete") is True
            and _collection_parse_complete(
                payload=open_orders_response.payload,
                row_key="output1",
                diagnostics=open_orders_parse,
            )
        )
        return BrokerAccountSnapshot(
            available_cash=available_cash,
            observed_at=observed_at,
            source=f"{self.provider}:account_snapshot",
            total_asset_value=balance_summary.get("total_asset_value"),
            previous_total_asset_value=balance_summary.get("previous_total_asset_value"),
            asset_change_amount=balance_summary.get("asset_change_amount"),
            unrealized_pnl=balance_summary.get("unrealized_pnl"),
            holdings=holdings,
            open_orders=open_orders,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
            diagnostics={
                "requested_exchange": normalized_exchange,
                "holdings_pagination": dict(balance_response.pagination),
                "open_orders_pagination": dict(open_orders_response.pagination),
                "holdings_parse": holdings_parse,
                "open_orders_parse": open_orders_parse,
            },
            payload={
                "exchange": normalized_exchange,
                "balance": balance_response.payload,
                "open_orders": open_orders_response.payload,
                "open_orders_error": open_orders_error,
                "holdings_coverage": (
                    "complete" if holdings_complete else "incomplete"
                ),
                "open_orders_coverage": (
                    "complete" if open_orders_complete else "incomplete"
                ),
                "account_number_masked": _mask_account(self.settings.kis_account_number),
            },
        )

    def get_legacy_us_holdings_snapshot(
        self,
        access_token: str,
    ) -> BrokerAccountSnapshot:
        """Read the complete all-US balance without opening trading surfaces."""
        config = self._configured()
        if self.settings.runtime_mode != "live":
            raise BrokerAdapterError(
                "KIS all-US holdings evidence is available only in live mode",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
            )
        if not self.settings.kis_account_number:
            raise BrokerAdapterError(
                "KIS account number is not configured",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
            )

        params = _overseas_balance_params(self.settings, "NASD")
        params["OVRS_EXCG_CD"] = "NASD"
        balance_response = _get_overseas_paginated_json(
            http_client=self.http_client,
            url=f"{config.api_base_url}{KIS_OVERSEAS_BALANCE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_overseas_balance_transaction_id(
                    self.settings.runtime_mode
                ),
            ),
            params=params,
            timeout_seconds=self.settings.kis_request_timeout_seconds,
            row_key="output1",
        )
        _raise_if_business_error(balance_response.payload)
        holdings = _parse_overseas_holdings(balance_response.payload)
        holdings_parse = _collection_parse_diagnostics(
            payload=balance_response.payload,
            row_key="output1",
            parsed_identities=[holding.symbol for holding in holdings],
            assess_row=_assess_overseas_holding_row,
        )
        holdings_complete = bool(
            balance_response.pagination.get("complete") is True
        ) and _collection_parse_complete(
            payload=balance_response.payload,
            row_key="output1",
            diagnostics=holdings_parse,
        )
        observed_at = datetime.now(UTC)
        return BrokerAccountSnapshot(
            observed_at=observed_at,
            source=f"{self.provider}:legacy_us_holdings_snapshot",
            holdings=holdings,
            open_orders=[],
            holdings_complete=holdings_complete,
            open_orders_complete=False,
            diagnostics={
                "query_scope": KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE,
                "holdings_pagination": dict(balance_response.pagination),
                "holdings_parse": holdings_parse,
            },
            payload={
                "currency": "USD",
                "exchange": "US_ALL",
                "query_scope": KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE,
                "evidence_type": "holdings_only",
                "balance": balance_response.payload,
                "holdings_coverage": (
                    "complete" if holdings_complete else "incomplete"
                ),
                "open_orders_coverage": "not_queried",
                "account_number_masked": _mask_account(
                    self.settings.kis_account_number
                ),
            },
        )

    def _get_us_equity_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None,
        reference_prices: dict[str, Decimal] | None,
        exchange: str,
    ) -> BrokerAccountSnapshot:
        config = self._configured()
        if not self.settings.kis_account_number:
            raise BrokerAdapterError(
                "KIS account number is not configured",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
            )
        observed_at = datetime.now(UTC)
        balance_response = _get_overseas_paginated_json(
            http_client=self.http_client,
            url=f"{config.api_base_url}{KIS_OVERSEAS_BALANCE_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_overseas_balance_transaction_id(self.settings.runtime_mode),
            ),
            params=_overseas_balance_params(self.settings, exchange),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
            row_key="output1",
        )
        open_orders_response = _get_overseas_paginated_json(
            http_client=self.http_client,
            url=f"{config.api_base_url}{KIS_OVERSEAS_OPEN_ORDERS_PATH}",
            headers=self.build_headers(
                access_token,
                transaction_id=_overseas_open_orders_transaction_id(self.settings.runtime_mode),
            ),
            params=_overseas_open_orders_params(self.settings, exchange),
            timeout_seconds=self.settings.kis_request_timeout_seconds,
            row_key="output",
        )
        _raise_if_business_error(balance_response.payload)
        _raise_if_business_error(open_orders_response.payload)

        symbol_buying_power = {}
        prices = reference_prices or {}
        for symbol in list(dict.fromkeys(symbols or []))[:20]:
            reference_price = prices.get(symbol)
            if reference_price is None or reference_price <= 0:
                continue
            buying_power_response = self.http_client.get_json(
                url=f"{config.api_base_url}{KIS_OVERSEAS_BUYING_POWER_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=_overseas_buying_power_transaction_id(self.settings.runtime_mode),
                ),
                params=_overseas_buying_power_params(
                    self.settings,
                    exchange=exchange,
                    symbol=symbol,
                    price=reference_price,
                ),
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
            _require_kis_business_success(
                buying_power_response.payload,
                context="overseas account buying-power inquiry",
            )
            buying_power = _parse_overseas_buying_power(symbol, buying_power_response.payload)
            if buying_power is not None:
                symbol_buying_power[symbol] = buying_power

        available_cash = next(
            (
                value.available_cash
                for value in symbol_buying_power.values()
                if value.available_cash is not None
            ),
            _parse_overseas_available_cash(balance_response.payload),
        )
        holdings = _parse_overseas_holdings(balance_response.payload)
        open_orders = _parse_overseas_open_orders(open_orders_response.payload)
        holdings_parse = _collection_parse_diagnostics(
            payload=balance_response.payload,
            row_key="output1",
            parsed_identities=[holding.symbol for holding in holdings],
            assess_row=_assess_overseas_holding_row,
        )
        open_orders_parse = _collection_parse_diagnostics(
            payload=open_orders_response.payload,
            row_key="output",
            parsed_identities=[order.broker_order_id for order in open_orders],
            assess_row=_assess_overseas_open_order_row,
        )
        holdings_complete = bool(
            balance_response.pagination.get("complete") is True
        ) and _collection_parse_complete(
            payload=balance_response.payload,
            row_key="output1",
            diagnostics=holdings_parse,
        )
        open_orders_complete = bool(
            open_orders_response.pagination.get("complete") is True
        ) and _collection_parse_complete(
            payload=open_orders_response.payload,
            row_key="output",
            diagnostics=open_orders_parse,
        )
        unrealized_pnl = (
            None
            if any(holding.unrealized_pnl is None for holding in holdings)
            else sum(
                (holding.unrealized_pnl for holding in holdings),
                Decimal("0"),
            )
        )
        securities_value = sum(
            (
                _parse_decimal(holding.payload.get("ovrs_stck_evlu_amt")) or Decimal("0")
                for holding in holdings
            ),
            Decimal("0"),
        )
        total_asset_value = securities_value + available_cash if available_cash is not None else None
        return BrokerAccountSnapshot(
            available_cash=available_cash,
            buying_power=available_cash,
            total_asset_value=total_asset_value,
            unrealized_pnl=unrealized_pnl,
            observed_at=observed_at,
            source=f"{self.provider}:us_equity_account_snapshot",
            holdings=holdings,
            open_orders=open_orders,
            symbol_buying_power=symbol_buying_power,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
            diagnostics={
                "holdings_pagination": dict(balance_response.pagination),
                "open_orders_pagination": dict(open_orders_response.pagination),
                "holdings_parse": holdings_parse,
                "open_orders_parse": open_orders_parse,
            },
            payload={
                "currency": "USD",
                "exchange": exchange,
                "balance": balance_response.payload,
                "open_orders": open_orders_response.payload,
                "holdings_coverage": (
                    "complete" if holdings_complete else "incomplete"
                ),
                "open_orders_coverage": (
                    "complete" if open_orders_complete else "incomplete"
                ),
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
        expected_symbol: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_submitted_at = (
            _normalize_utc_datetime(submitted_at)
            if submitted_at is not None
            else None
        )
        if exchange in {"NASD", "NYSE", "AMEX"}:
            return self._reconcile_us_equity_order(
                access_token,
                broker_order_id,
                exchange=exchange,
                submitted_at=normalized_submitted_at,
                expected_symbol=expected_symbol,
            )
        config = self._configured()
        observed_at = datetime.now(UTC)
        transaction_id = _daily_ccld_transaction_id(
            self.settings.runtime_mode,
            period_division=_daily_ccld_period_division(
                submitted_at=normalized_submitted_at,
                observed_at=observed_at,
            ),
        )
        params = _reconcile_order_params(
            self.settings,
            broker_order_id,
            observed_at,
            exchange=exchange,
            submitted_at=normalized_submitted_at,
            expected_symbol=expected_symbol,
        )
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_DAILY_CCLD_PATH,
            "tr_id": transaction_id,
            "params": params,
        }
        try:
            response = _get_domestic_paginated_json(
                http_client=self.http_client,
                url=f"{config.api_base_url}{KIS_DAILY_CCLD_PATH}",
                headers=self.build_headers(
                    access_token,
                    transaction_id=transaction_id,
                ),
                params=params,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
                row_keys=("output1",),
            )
        except BrokerAdapterError as exc:
            if not exc.request_metadata:
                exc.request_metadata = request_metadata
            raise
        normalized_order = _normalize_reconcile_order(
            response.payload,
            broker_order_id,
            observed_at=observed_at,
            expected_symbol=expected_symbol,
            expected_submitted_date=(
                normalized_submitted_at.astimezone(KIS_TIMEZONE).date()
                if normalized_submitted_at is not None
                else observed_at.astimezone(KIS_TIMEZONE).date()
            ),
            expected_exchange=exchange,
        )
        if normalized_order is None:
            return {
                "provider": self.provider,
                "broker_order_id": broker_order_id,
                "symbol": expected_symbol,
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
            "symbol": normalized_order["symbol"],
            "status": normalized_order["status"],
            "broker_status": normalized_order["broker_status"],
            "requested_quantity": normalized_order["requested_quantity"],
            "remaining_quantity": normalized_order["remaining_quantity"],
            "filled_quantity": normalized_order["filled_quantity"],
            "average_fill_price": normalized_order["average_fill_price"],
            "filled_at": normalized_order["filled_at"].isoformat(),
            "fill_time_precision": normalized_order["fill_time_precision"],
            "fill_time_source": normalized_order["fill_time_source"],
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

    def _reconcile_us_equity_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str,
        submitted_at: datetime | None,
        expected_symbol: str | None,
    ) -> dict[str, Any]:
        config = self._configured()
        observed_at = datetime.now(UTC)
        params = _overseas_reconcile_params(
            self.settings,
            exchange=exchange,
            submitted_at=submitted_at,
            observed_at=observed_at,
        )
        transaction_id = _overseas_ccld_transaction_id(self.settings.runtime_mode)
        request_metadata = {
            "provider": self.provider,
            "endpoint": KIS_OVERSEAS_CCLD_PATH,
            "tr_id": transaction_id,
            "params": params,
        }
        response = _get_overseas_paginated_json(
            http_client=self.http_client,
            url=f"{config.api_base_url}{KIS_OVERSEAS_CCLD_PATH}",
            headers=self.build_headers(access_token, transaction_id=transaction_id),
            params=params,
            timeout_seconds=self.settings.kis_request_timeout_seconds,
            row_key="output",
        )
        _raise_if_business_error(response.payload)
        normalized_submitted_at = (
            _normalize_utc_datetime(submitted_at)
            if submitted_at is not None
            else None
        )
        normalized_order = _normalize_overseas_reconcile_order(
            response.payload,
            broker_order_id,
            observed_at=observed_at,
            expected_symbol=expected_symbol,
            expected_submitted_date=(
                normalized_submitted_at.astimezone(US_EASTERN_TIMEZONE).date()
                if normalized_submitted_at is not None
                else observed_at.astimezone(US_EASTERN_TIMEZONE).date()
            ),
            expected_exchange=exchange,
        )
        if normalized_order is None:
            return {
                "provider": self.provider,
                "broker_order_id": broker_order_id,
                "symbol": expected_symbol,
                "status": "unknown",
                "broker_status": "unknown",
                "observed_at": observed_at.isoformat(),
                "request_metadata": request_metadata,
                "response_metadata": {"status_code": response.status_code},
                "message": "matching overseas order was not found in broker order query",
                "payload": {"output": _safe_reconcile_payload(response.payload)},
            }
        return {
            "provider": self.provider,
            "broker_order_id": broker_order_id,
            "symbol": normalized_order["symbol"],
            "status": normalized_order["status"],
            "broker_status": normalized_order["broker_status"],
            "requested_quantity": normalized_order["requested_quantity"],
            "remaining_quantity": normalized_order["remaining_quantity"],
            "filled_quantity": normalized_order["filled_quantity"],
            "average_fill_price": normalized_order["average_fill_price"],
            "filled_at": normalized_order["filled_at"].isoformat(),
            "fill_time_precision": normalized_order["fill_time_precision"],
            "fill_time_source": normalized_order["fill_time_source"],
            "observed_at": observed_at.isoformat(),
            "request_metadata": request_metadata,
            "response_metadata": {"status_code": response.status_code},
            "payload": {"order": normalized_order["raw"]},
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
    expected_symbol: str | None = None,
) -> dict[str, object]:
    inquiry_date = (
        _normalize_utc_datetime(submitted_at).astimezone(KIS_TIMEZONE).date()
        if submitted_at is not None
        else observed_at.astimezone(KIS_TIMEZONE).date()
    )
    normalized_exchange = exchange.strip().upper() or "KRX"
    normalized_symbol = (expected_symbol or "").strip().upper()
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "INQR_STRT_DT": inquiry_date.strftime("%Y%m%d"),
        "INQR_END_DT": inquiry_date.strftime("%Y%m%d"),
        "SLL_BUY_DVSN_CD": "00",
        "PDNO": normalized_symbol,
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
    expected_symbol: str | None,
    expected_submitted_date: date,
    expected_exchange: str,
) -> dict[str, object] | None:
    rows = _payload_rows(payload.get("output1"))
    normalized_expected_symbol = (expected_symbol or "").strip().upper()
    normalized_expected_exchange = expected_exchange.strip().upper()
    expected_date = expected_submitted_date.strftime("%Y%m%d")
    if not normalized_expected_symbol or not normalized_expected_exchange:
        return None
    for row in rows:
        row_order_id = _optional_string(
            _case_insensitive_value(row, "odno", "ord_no")
        )
        if row_order_id != broker_order_id:
            continue
        row_symbol = _optional_string(_case_insensitive_value(row, "pdno"))
        row_date = _optional_string(_case_insensitive_value(row, "ord_dt"))
        row_exchange = _optional_string(
            _case_insensitive_value(row, "excg_id_dvsn_cd")
        )
        if (
            row_symbol is None
            or row_symbol.strip().upper() != normalized_expected_symbol
            or row_date != expected_date
            or row_exchange is None
            or row_exchange.strip().upper() != normalized_expected_exchange
        ):
            continue
        normalized = _extract_reconcile_order_info(
            row,
            observed_at=observed_at,
            raw_status=(
                _rejection_status_from_order_kind(row)
                or _optional_string(row.get("ord_ccld_status_name"))
                or _optional_string(row.get("ord_st_dvsn_cd_name"))
                or _optional_string(row.get("ord_sts_name"))
            ),
        )
        if normalized is None:
            return None
        return {
            **normalized,
            "symbol": row_symbol.strip().upper(),
            "exchange": row_exchange.strip().upper(),
            "submitted_date": row_date,
        }
    return None


def _normalize_overseas_reconcile_order(
    payload: dict[str, object],
    broker_order_id: str,
    *,
    observed_at: datetime,
    expected_symbol: str | None,
    expected_submitted_date: date,
    expected_exchange: str,
) -> dict[str, object] | None:
    normalized_expected_symbol = (expected_symbol or "").strip().upper()
    normalized_expected_exchange = _normalized_kis_us_exchange(expected_exchange)
    expected_date = expected_submitted_date.strftime("%Y%m%d")
    if not normalized_expected_symbol or normalized_expected_exchange is None:
        return None
    for row in _payload_rows(payload.get("output")):
        row_order_id = _optional_string(
            _case_insensitive_value(row, "odno", "ord_no")
        )
        if row_order_id != broker_order_id:
            continue
        row_symbol = _optional_string(_case_insensitive_value(row, "pdno"))
        row_date = _optional_string(_case_insensitive_value(row, "ord_dt"))
        row_exchange = _normalized_kis_us_exchange(
            _case_insensitive_value(
                row,
                "ovrs_excg_cd",
                "excg_cd",
                "excg_id_dvsn_cd",
            )
        )
        if (
            row_symbol is None
            or row_symbol.strip().upper() != normalized_expected_symbol
            or row_date != expected_date
            or row_exchange != normalized_expected_exchange
        ):
            continue
        quantities = _extract_reconcile_quantities(
            row,
            requested_keys=("ft_ord_qty", "ord_qty"),
            filled_keys=("ft_ccld_qty",),
            remaining_keys=("nccs_qty",),
        )
        if quantities is None:
            return None
        requested_quantity, filled_quantity, remaining_quantity = quantities
        raw_status = (
            _rejection_status_from_order_kind(row)
            or _optional_string(row.get("prcs_stat_name"))
            or _optional_string(row.get("rvse_cncl_dvsn_name"))
        )
        status = _normalize_reconcile_status(
            raw_status=raw_status,
            requested_quantity=requested_quantity,
            remaining_quantity=remaining_quantity,
            filled_quantity=filled_quantity,
        )
        if not _reconcile_status_matches_quantities(
            status=status,
            requested_quantity=requested_quantity,
            filled_quantity=filled_quantity,
            remaining_quantity=remaining_quantity,
        ):
            return None
        return {
            "symbol": row_symbol.strip().upper(),
            "exchange": row_exchange,
            "submitted_date": row_date,
            "status": status,
            "broker_status": raw_status or status,
            "requested_quantity": requested_quantity,
            "remaining_quantity": remaining_quantity,
            "filled_quantity": filled_quantity,
            "average_fill_price": _first_decimal(
                row,
                "ft_ccld_unpr3",
                "avg_pric",
                "ft_ord_unpr3",
            ),
            # This cumulative order row exposes order time, not execution time.
            # Use the poll timestamp as an upper bound and preserve the weaker
            # chronology explicitly so risk gates cannot treat it as exact.
            "filled_at": observed_at,
            "fill_time_precision": "observed_upper_bound",
            "fill_time_source": "reconciliation_observed_at",
            "raw": _safe_reconcile_payload(row),
            "observed_at": observed_at,
            "message": None,
        }
    return None


def _normalized_kis_us_exchange(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    canonical = {
        "NAS": "NASD",
        "NASDAQ": "NASD",
        "NYS": "NYSE",
        "NEWYORK": "NYSE",
        "AMS": "AMEX",
    }.get(normalized, normalized)
    return canonical if canonical in KIS_US_EXCHANGES else None


def _extract_reconcile_order_info(
    row: dict[str, object],
    *,
    observed_at: datetime,
    raw_status: str | None,
) -> dict[str, object] | None:
    quantities = _extract_reconcile_quantities(
        row,
        requested_keys=("ord_qty",),
        filled_keys=("tot_ccld_qty",),
        remaining_keys=("rmn_qty", "ord_uncc_qty", "nccs_qty"),
    )
    if quantities is None:
        return None
    requested_quantity, filled_quantity, remaining_quantity = quantities
    status = _normalize_reconcile_status(
        raw_status=raw_status,
        requested_quantity=requested_quantity,
        remaining_quantity=remaining_quantity,
        filled_quantity=filled_quantity,
    )
    if not _reconcile_status_matches_quantities(
        status=status,
        requested_quantity=requested_quantity,
        filled_quantity=filled_quantity,
        remaining_quantity=remaining_quantity,
    ):
        return None
    average_fill_price = (
        _parse_decimal(row.get("avg_upr"))
        or _parse_decimal(row.get("trd_prc"))
        or _parse_decimal(row.get("avg_pric"))
        or _parse_decimal(row.get("avg_prvs"))
        or _parse_decimal(row.get("ord_unpr"))
    )
    return {
        "status": status,
        "broker_status": raw_status or status,
        "requested_quantity": requested_quantity,
        "remaining_quantity": remaining_quantity,
        "filled_quantity": filled_quantity,
        "average_fill_price": average_fill_price,
        # KIS daily cumulative order history does not prove individual fill
        # time. ord_tmd/ord_dt describe submission and remain only in raw.
        "filled_at": observed_at,
        "fill_time_precision": "observed_upper_bound",
        "fill_time_source": "reconciliation_observed_at",
        "raw": _safe_reconcile_payload(row),
        "observed_at": observed_at,
        "message": None,
    }


def _extract_reconcile_quantities(
    row: dict[str, object],
    *,
    requested_keys: tuple[str, ...],
    filled_keys: tuple[str, ...],
    remaining_keys: tuple[str, ...],
) -> tuple[int, int, int] | None:
    requested_quantity, requested_supplied = _first_integral_evidence(
        row,
        requested_keys,
    )
    if (
        not requested_supplied
        or requested_quantity is None
        or requested_quantity <= 0
    ):
        return None

    filled_quantity, filled_supplied = _first_integral_evidence(row, filled_keys)
    if filled_supplied and filled_quantity is None:
        return None
    if filled_quantity is None:
        filled_quantity = 0

    remaining_quantity, remaining_supplied = _first_integral_evidence(
        row,
        remaining_keys,
    )
    if remaining_supplied and remaining_quantity is None:
        return None
    if remaining_quantity is None:
        remaining_quantity = requested_quantity - filled_quantity

    if not (
        0 <= filled_quantity <= requested_quantity
        and 0 <= remaining_quantity <= requested_quantity
        and filled_quantity + remaining_quantity <= requested_quantity
    ):
        return None
    return requested_quantity, filled_quantity, remaining_quantity


def _reconcile_status_matches_quantities(
    *,
    status: str,
    requested_quantity: int,
    filled_quantity: int,
    remaining_quantity: int,
) -> bool:
    if status == "filled":
        return filled_quantity == requested_quantity and remaining_quantity == 0
    if status == "partially_filled":
        return (
            0 < filled_quantity < requested_quantity
            and filled_quantity + remaining_quantity == requested_quantity
        )
    if status == "open":
        return filled_quantity == 0 and remaining_quantity == requested_quantity
    if status in {"submission_pending", "retry_pending"}:
        return filled_quantity == 0 and remaining_quantity == requested_quantity
    if status in {"canceled", "rejected", "failed", "expired"}:
        return remaining_quantity == 0
    return False


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


def _bearer_token(headers: dict[str, object]) -> str | None:
    authorization = next(
        (
            value
            for key, value in headers.items()
            if str(key).lower() == "authorization"
        ),
        None,
    )
    if not isinstance(authorization, str) or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ")
    return token or None


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BrokerAdapterError(f"KIS response is missing '{key}'", status_code=502)
    return value


def _optional_string(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _case_insensitive_value(
    payload: dict[str, object],
    *expected_keys: str,
) -> object | None:
    expected = {key.casefold() for key in expected_keys}
    matches = [
        value
        for key, value in payload.items()
        if str(key).casefold() in expected
    ]
    if not matches or any(value != matches[0] for value in matches[1:]):
        return None
    return matches[0]


def _parse_krx_orderbook_observed_at(
    output: dict[str, object],
    *,
    received_at: datetime,
) -> datetime | None:
    raw_time = _optional_string(
        output.get("aspr_acpt_hour") or output.get("ASPR_ACPT_HOUR")
    )
    if (
        raw_time is None
        or len(raw_time) != 6
        or not raw_time.isascii()
        or not raw_time.isdigit()
    ):
        return None
    try:
        parsed_time = datetime.strptime(raw_time, "%H%M%S").time()
    except ValueError:
        return None
    normalized_received_at = (
        received_at if received_at.tzinfo is not None else received_at.replace(tzinfo=UTC)
    )
    krx_date = normalized_received_at.astimezone(KIS_TIMEZONE).date()
    return datetime.combine(
        krx_date,
        parsed_time,
        tzinfo=KIS_TIMEZONE,
    ).astimezone(UTC)


def _parse_krx_dated_observed_at(
    raw_date: object,
    raw_time: object,
) -> datetime | None:
    if (
        not isinstance(raw_date, str)
        or not isinstance(raw_time, str)
        or len(raw_date) != 8
        or len(raw_time) != 6
        or not raw_date.isascii()
        or not raw_time.isascii()
        or not raw_date.isdigit()
        or not raw_time.isdigit()
    ):
        return None
    try:
        local = datetime.strptime(
            f"{raw_date}{raw_time}",
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=KIS_TIMEZONE)
    except ValueError:
        return None
    return local.astimezone(UTC)


def _parse_overseas_bar_observed_at(
    output: dict[str, object],
    *,
    exchange: str,
) -> datetime | None:
    raw_date = _optional_string(output.get("xymd") or output.get("XYMD"))
    raw_time = _optional_string(output.get("xhms") or output.get("XHMS"))
    if raw_date is None or raw_time is None:
        return None
    if (
        len(raw_date) != 8
        or not raw_date.isascii()
        or not raw_date.isdigit()
        or len(raw_time) != 6
        or not raw_time.isascii()
        or not raw_time.isdigit()
    ):
        return None
    try:
        local = datetime.strptime(
            f"{raw_date}{raw_time}",
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=US_EASTERN_TIMEZONE)
    except ValueError:
        return None
    if exchange not in {"NASD", "NYSE", "AMEX"}:
        return None
    return local.astimezone(UTC)


def _parse_overseas_orderbook_observed_at(
    output: dict[str, object],
    *,
    exchange: str,
) -> datetime | None:
    if exchange not in {"NASD", "NYSE", "AMEX"}:
        return None
    raw_date = _optional_string(output.get("dymd") or output.get("DYMD"))
    raw_time = _optional_string(output.get("dhms") or output.get("DHMS"))
    if (
        raw_date is None
        or raw_time is None
        or len(raw_date) != 8
        or len(raw_time) != 6
        or not raw_date.isascii()
        or not raw_time.isascii()
        or not raw_date.isdigit()
        or not raw_time.isdigit()
    ):
        return None
    try:
        local = datetime.strptime(
            f"{raw_date}{raw_time}",
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=US_EASTERN_TIMEZONE)
    except ValueError:
        return None
    return local.astimezone(UTC)


def _parse_optional_expiration(explicit: object, seconds: object) -> datetime | None:
    if explicit is None and seconds is None:
        return None
    return _parse_expiration(explicit, seconds)


def _parse_expiration(
    explicit: object,
    seconds: object,
    *,
    reference_at: datetime | None = None,
    require_seconds: bool = False,
) -> datetime:
    parsed_seconds = _parse_token_expiry_seconds(seconds)
    if require_seconds and parsed_seconds is None:
        raise BrokerAdapterError(
            "KIS response contains invalid access token expiry metadata",
            status_code=502,
        )
    expires_at = (
        _as_utc_datetime(reference_at or datetime.now(UTC))
        + timedelta(seconds=parsed_seconds)
        if parsed_seconds is not None
        else None
    )
    if explicit is not None:
        parsed = _parse_kis_token_expiry(explicit)
        if parsed is None:
            raise BrokerAdapterError(
                "KIS response contains invalid access token expiry metadata",
                status_code=502,
            )
        expires_at = parsed if expires_at is None else min(expires_at, parsed)

    if expires_at is not None:
        return expires_at

    raise BrokerAdapterError("KIS response is missing access token expiry metadata", status_code=502)


def _parse_token_expiry_seconds(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = value
    elif (
        isinstance(value, str)
        and len(value) <= 6
        and value.isascii()
        and value.isdigit()
    ):
        parsed = int(value)
    else:
        return None
    return parsed if 0 < parsed <= 172_800 else None


def _parse_kis_token_expiry(value: object) -> datetime | None:
    if (
        not isinstance(value, str)
        or _KIS_TOKEN_EXPIRY_PATTERN.fullmatch(value) is None
    ):
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except (OverflowError, ValueError):
        return None
    if parsed.strftime("%Y-%m-%d %H:%M:%S") != value:
        return None
    return parsed.replace(tzinfo=KIS_TIMEZONE).astimezone(UTC)


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


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
    raw_error_message = _error_message(payload)
    classification = _classify_error(
        status_code=status_code,
        code=safe_broker_error_code(payload.get("msg_cd"), provider=KIS_PROVIDER),
        message=raw_error_message,
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
        safe_broker_error_message(raw_error_message),
        status_code=503 if retryable else 422,
        classification=classification,
        retryable=retryable,
        code=safe_broker_error_code(payload.get("msg_cd"), provider=KIS_PROVIDER),
        normalized_status=normalized_status,
        response_metadata=_safe_error_payload(
            {
                "status_code": status_code,
                "headers": _sanitize_headers(headers),
            }
        ),
        response_payload=_safe_error_payload(payload),
        observed_at=datetime.now(UTC),
    )


def _parse_http_payload(raw_body: str) -> dict[str, object]:
    if not raw_body:
        return {}
    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError:
        return {"body_diagnostic": safe_broker_error_message(raw_body)}
    if isinstance(parsed, dict):
        return parsed
    return {"body_diagnostic": safe_broker_error_message(raw_body)}


def _safe_error_payload(payload: object) -> dict[str, object]:
    sanitized = sanitize_broker_error_payload(payload)
    return dict(sanitized) if isinstance(sanitized, dict) else {}


def _classify_error(
    *,
    status_code: int,
    code: str | None,
    message: str,
) -> BrokerErrorClassification:
    message_lower = message.lower()
    code_lower = (code or "").lower()

    # KIS reports an app-key/runtime-mode mismatch as HTTP 500 even though the
    # request cannot succeed until credentials are changed.  Treat it as a
    # permanent configuration validation error before the generic 5xx rule so
    # autonomous dispatch does not hammer the broker with identical retries.
    if code == "EGW02007":
        return BrokerErrorClassification.VALIDATION
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


def _order_division(
    order_type: str,
    order_division: str | None = None,
    time_in_force: str = "day",
) -> str:
    if order_division is not None:
        return order_division
    if order_type == "limit" and time_in_force == "ioc":
        # KIS domestic cash-order contract: 11 = IOC limit
        # (immediate execution, cancel any remainder).
        return "11"
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


def _first_present_key(payload: dict[str, object], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if key in payload and value is not None and value != "":
            return key
    return None


def _first_decimal_from_rows(
    rows: list[dict[str, object]],
    *keys: str,
) -> tuple[Decimal | None, str | None]:
    for row in rows:
        for key in keys:
            value = _parse_decimal(row.get(key))
            if value is not None:
                return value, key
    return None, None


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
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _transaction_id(runtime_mode: str, side: str) -> str:
    live = runtime_mode == "live"
    if side == "sell":
        return "TTTC0011U" if live else "VTTC0011U"
    return "TTTC0012U" if live else "VTTC0012U"


def _validate_us_equity_order_request(request: BrokerOrderPlacementRequest) -> None:
    if request.exchange not in {"NASD", "NYSE", "AMEX"}:
        raise BrokerAdapterError(
            "KIS US-equity order requires NASD, NYSE, or AMEX exchange code",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    if request.order_type != "limit" or request.time_in_force != "day":
        raise BrokerAdapterError(
            "KIS US-equity order supports day limit orders only",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    if request.quantity is None or request.quantity <= 0 or request.limit_price is None:
        raise BrokerAdapterError(
            "KIS US-equity order requires whole-share quantity and limit price",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )


def _overseas_order_transaction_id(runtime_mode: str, side: str) -> str:
    if runtime_mode == "live":
        return "TTTT1006U" if side == "sell" else "TTTT1002U"
    return "VTTT1001U" if side == "sell" else "VTTT1002U"


def _overseas_quote_exchange(exchange: str) -> str:
    try:
        return {"NASD": "NAS", "NYSE": "NYS", "AMEX": "AMS"}[exchange]
    except KeyError as exc:
        raise BrokerAdapterError(
            f"unsupported KIS overseas quote exchange: {exchange}",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
        ) from exc


def _overseas_balance_transaction_id(runtime_mode: str) -> str:
    return "TTTS3012R" if runtime_mode == "live" else "VTTS3012R"


def _overseas_buying_power_transaction_id(runtime_mode: str) -> str:
    return "TTTS3007R" if runtime_mode == "live" else "VTTS3007R"


def _overseas_ccld_transaction_id(runtime_mode: str) -> str:
    return "TTTS3035R" if runtime_mode == "live" else "VTTS3035R"


def _overseas_open_orders_transaction_id(runtime_mode: str) -> str:
    return "TTTS3018R" if runtime_mode == "live" else "VTTS3018R"


def _get_kis_paginated_json(
    *,
    http_client: JsonHttpClient,
    url: str,
    headers: dict[str, str],
    params: dict[str, object],
    timeout_seconds: int,
    row_keys: tuple[str, ...],
    cursor_width: int,
    market_label: str,
    max_pages: int = 20,
) -> JsonHttpResponse:
    request_params = dict(params)
    request_headers = dict(headers)
    aggregate: dict[str, object] | None = None
    last_response: JsonHttpResponse | None = None
    seen_cursors: set[tuple[str, str]] = set()
    for page_index in range(max_pages):
        response = http_client.get_json(
            url=url,
            headers=request_headers,
            params=request_params,
            timeout_seconds=timeout_seconds,
        )
        _require_kis_business_success(
            response.payload,
            context=f"{market_label} paginated inquiry",
        )
        last_response = response
        if aggregate is None:
            aggregate = dict(response.payload)
        else:
            for row_key in row_keys:
                page_rows = _payload_collection_items(response.payload.get(row_key))
                if not page_rows:
                    continue
                existing_rows = _payload_collection_items(aggregate.get(row_key))
                aggregate[row_key] = [*existing_rows, *page_rows]

        continuation = str(
            response.headers.get("tr_cont")
            or response.headers.get("TR_CONT")
            or ""
        ).strip().upper()
        if continuation in {"D", "E"}:
            return JsonHttpResponse(
                status_code=response.status_code,
                headers=response.headers,
                payload=aggregate,
                pagination={
                    "complete": True,
                    "page_count": page_index + 1,
                    "cursor_width": cursor_width,
                },
            )
        if continuation not in {"F", "M"}:
            continuation_label = continuation or "missing"
            raise BrokerAdapterError(
                f"KIS {market_label} pagination invalid continuation state: "
                f"{continuation_label}",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                response_payload=response.payload,
            )
        cursor = _pagination_cursor(response.payload, cursor_width=cursor_width)
        if cursor is None:
            raise BrokerAdapterError(
                f"KIS {market_label} response requested continuation without cursor fields",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                response_payload=response.payload,
            )
        if cursor in seen_cursors:
            raise BrokerAdapterError(
                f"KIS {market_label} pagination repeated continuation cursor",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                response_payload=response.payload,
            )
        seen_cursors.add(cursor)
        request_params[f"CTX_AREA_FK{cursor_width}"], request_params[
            f"CTX_AREA_NK{cursor_width}"
        ] = cursor
        request_headers["tr_cont"] = "N"
        if page_index == max_pages - 1:
            raise BrokerAdapterError(
                f"KIS {market_label} pagination exceeded {max_pages} pages",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
            )
    if aggregate is None or last_response is None:
        raise BrokerAdapterError(
            f"KIS {market_label} pagination returned no response",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
        )
    return JsonHttpResponse(last_response.status_code, last_response.headers, aggregate)


def _get_domestic_paginated_json(
    *,
    http_client: JsonHttpClient,
    url: str,
    headers: dict[str, str],
    params: dict[str, object],
    timeout_seconds: int,
    row_keys: tuple[str, ...],
    max_pages: int = 20,
) -> JsonHttpResponse:
    return _get_kis_paginated_json(
        http_client=http_client,
        url=url,
        headers=headers,
        params=params,
        timeout_seconds=timeout_seconds,
        row_keys=row_keys,
        cursor_width=100,
        market_label="domestic",
        max_pages=max_pages,
    )


def _get_overseas_paginated_json(
    *,
    http_client: JsonHttpClient,
    url: str,
    headers: dict[str, str],
    params: dict[str, object],
    timeout_seconds: int,
    row_key: str,
    max_pages: int = 20,
) -> JsonHttpResponse:
    return _get_kis_paginated_json(
        http_client=http_client,
        url=url,
        headers=headers,
        params=params,
        timeout_seconds=timeout_seconds,
        row_keys=(row_key,),
        cursor_width=200,
        market_label="overseas",
        max_pages=max_pages,
    )


def _pagination_cursor(
    payload: dict[str, object],
    *,
    cursor_width: int,
) -> tuple[str, str] | None:
    candidates: list[dict[str, object]] = [payload]
    for value in payload.values():
        if isinstance(value, dict):
            candidates.append(value)
        elif isinstance(value, list):
            candidates.extend(row for row in value if isinstance(row, dict))
    for candidate in candidates:
        lowered = {str(key).lower(): value for key, value in candidate.items()}
        fk = str(lowered.get(f"ctx_area_fk{cursor_width}") or "").strip()
        nk = str(lowered.get(f"ctx_area_nk{cursor_width}") or "").strip()
        if fk or nk:
            return fk, nk
    return None


def _overseas_balance_params(settings: Settings, exchange: str) -> dict[str, object]:
    # KIS assigns different meanings to NASD on the live balance endpoint:
    # live NASD is an aggregate of all US markets, while NAS is Nasdaq-only.
    # Keep canonical order/quote routing as NASD and narrow only this inquiry.
    balance_exchange = (
        "NAS"
        if settings.runtime_mode == "live" and exchange.strip().upper() == "NASD"
        else exchange
    )
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "OVRS_EXCG_CD": balance_exchange,
        "TR_CRCY_CD": "USD",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }


def _overseas_open_orders_params(settings: Settings, exchange: str) -> dict[str, object]:
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "OVRS_EXCG_CD": exchange,
        "SORT_SQN": "DS",
        "CTX_AREA_FK200": "",
        "CTX_AREA_NK200": "",
    }


def _overseas_buying_power_params(
    settings: Settings,
    *,
    exchange: str,
    symbol: str,
    price: Decimal,
) -> dict[str, object]:
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "OVRS_EXCG_CD": exchange,
        "OVRS_ORD_UNPR": str(price),
        "ITEM_CD": symbol,
    }


def _overseas_reconcile_params(
    settings: Settings,
    *,
    exchange: str,
    submitted_at: datetime | None,
    observed_at: datetime,
) -> dict[str, object]:
    end_date = observed_at.astimezone(US_EASTERN_TIMEZONE).date()
    start_date = (
        _normalize_utc_datetime(submitted_at)
        .astimezone(US_EASTERN_TIMEZONE)
        .date()
        if submitted_at
        else end_date
    )
    if start_date > end_date:
        start_date = end_date
    is_paper = settings.runtime_mode != "live"
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        "PDNO": "" if is_paper else "%",
        "ORD_STRT_DT": start_date.strftime("%Y%m%d"),
        "ORD_END_DT": end_date.strftime("%Y%m%d"),
        "SLL_BUY_DVSN": "00",
        "CCLD_NCCS_DVSN": "00",
        "OVRS_EXCG_CD": "" if is_paper else exchange,
        "SORT_SQN": "DS",
        "ORD_DT": "",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "CTX_AREA_NK200": "",
        "CTX_AREA_FK200": "",
    }


def _balance_transaction_id(runtime_mode: str) -> str:
    return "TTTC8434R" if runtime_mode == "live" else "VTTC8434R"


def _open_orders_transaction_id(runtime_mode: str) -> str:
    return "TTTC0081R" if runtime_mode == "live" else "VTTC0081R"


def _daily_ccld_transaction_id(runtime_mode: str, *, period_division: str = "inner") -> str:
    before_period = period_division.strip().lower() == "before"
    if runtime_mode == "live":
        return "CTSC9215R" if before_period else "TTTC0081R"
    return "VTSC9215R" if before_period else "VTTC0081R"


def _daily_ccld_period_division(
    *,
    submitted_at: datetime | None,
    observed_at: datetime,
) -> str:
    if submitted_at is None:
        return "inner"
    submitted_date = (
        _normalize_utc_datetime(submitted_at).astimezone(KIS_TIMEZONE).date()
    )
    observed_date = observed_at.astimezone(KIS_TIMEZONE).date()
    # KIS documents this split as "3 months" rather than a fixed day count.
    # Interpreting that as calendar months is deliberate; month-end dates clamp
    # to the final day of the target month so the boundary itself stays inner.
    inner_period_start = _subtract_calendar_months(observed_date, months=3)
    return "before" if submitted_date < inner_period_start else "inner"


def _subtract_calendar_months(value: date, *, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 - months
    target_year, zero_based_month = divmod(month_index, 12)
    target_month = zero_based_month + 1
    target_day = min(value.day, monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


def _normalize_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _balance_params(settings: Settings) -> dict[str, object]:
    return {
        "CANO": settings.kis_account_number or "",
        "ACNT_PRDT_CD": settings.kis_account_product_code,
        # This endpoint returns account-wide domestic inventory. ``X`` only
        # changes the valuation-price source to NXT; it does not filter lots.
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


def _open_orders_params(
    settings: Settings,
    observed_at: datetime,
) -> dict[str, object]:
    inquiry_date = observed_at.astimezone(KIS_TIMEZONE).strftime("%Y%m%d")
    # Capital reservation must see every domestic venue in live trading.
    # The paper API only supports KRX for this inquiry contract.
    query_exchange = "ALL" if settings.runtime_mode == "live" else "KRX"
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
        "EXCG_ID_DVSN_CD": query_exchange,
    }


def _raise_if_business_error(payload: dict[str, object]) -> None:
    if _optional_string(payload.get("rt_cd")) in {None, "0"}:
        return
    raw_error_message = _error_message(payload)
    raise BrokerAdapterError(
        safe_broker_error_message(raw_error_message),
        status_code=422,
        classification=_classify_error(
            status_code=400,
            code=_optional_string(payload.get("msg_cd")),
            message=raw_error_message,
        ),
        retryable=False,
        code=safe_broker_error_code(payload.get("msg_cd"), provider=KIS_PROVIDER),
        response_payload=_safe_error_payload(payload),
        observed_at=datetime.now(UTC),
    )


def _require_kis_business_success(
    payload: dict[str, object],
    *,
    context: str,
) -> None:
    raw_code = payload.get("rt_cd")
    if not isinstance(raw_code, str) or not raw_code.strip():
        raise BrokerAdapterError(
            f"KIS {context} response is missing a valid string rt_cd",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
            retryable=True,
            code="kis_response_business_code_invalid",
            response_payload=_safe_error_payload(payload),
            observed_at=datetime.now(UTC),
        )
    _raise_if_business_error(payload)


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


def _parse_holdings(payload: dict[str, object]) -> list[BrokerAccountHolding]:
    holdings: list[BrokerAccountHolding] = []
    for row in _payload_rows(payload.get("output1")):
        assessment, _identity = _assess_domestic_holding_row(row)
        if assessment != "parsed":
            continue
        symbol = _optional_string(row.get("pdno"))
        quantity = _parse_int(row.get("hldg_qty"))
        assert symbol is not None
        assert quantity is not None and quantity > 0
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
        assessment, _identity = _assess_domestic_open_order_row(row)
        if assessment != "parsed":
            continue
        symbol = _optional_string(row.get("pdno"))
        quantity = _snapshot_integer(row, ("ord_qty",))
        remaining_quantity = _snapshot_remaining_quantity(
            row,
            quantity=quantity,
            remaining_keys=("rmn_qty", "ord_uncc_qty", "nccs_qty"),
            filled_keys=("tot_ccld_qty",),
        )
        assert symbol is not None
        assert quantity is not None
        assert remaining_quantity is not None
        open_orders.append(
            BrokerAccountOpenOrder(
                symbol=symbol.strip().upper(),
                side=_parse_order_side(row),
                quantity=quantity,
                remaining_quantity=remaining_quantity,
                status=_optional_string(row.get("ord_ccld_status_name")) or "open",
                broker_order_id=(
                    _optional_string(row.get("odno"))
                    or _optional_string(row.get("ord_no"))
                ),
                limit_price=_parse_decimal(row.get("ord_unpr")),
                payload=dict(row),
            )
        )
    return open_orders


def _parse_overseas_holdings(payload: dict[str, object]) -> list[BrokerAccountHolding]:
    holdings: list[BrokerAccountHolding] = []
    for row in _payload_rows(payload.get("output1")):
        assessment, _identity = _assess_overseas_holding_row(row)
        if assessment != "parsed":
            continue
        symbol = _optional_string(row.get("ovrs_pdno")) or _optional_string(row.get("pdno"))
        quantity = _parse_int(row.get("ovrs_cblc_qty"))
        assert symbol is not None
        assert quantity is not None and quantity > 0
        holdings.append(
            BrokerAccountHolding(
                symbol=symbol.strip().upper(),
                quantity=quantity,
                average_price=_parse_decimal(row.get("pchs_avg_pric")),
                sellable_quantity=_parse_int(row.get("ord_psbl_qty")),
                unrealized_pnl=_parse_decimal(row.get("frcr_evlu_pfls_amt")),
                unrealized_pnl_pct=_parse_decimal(row.get("evlu_pfls_rt")),
                payload=dict(row),
            )
        )
    return holdings


def _parse_overseas_open_orders(payload: dict[str, object]) -> list[BrokerAccountOpenOrder]:
    open_orders: list[BrokerAccountOpenOrder] = []
    for row in _payload_rows(payload.get("output")):
        assessment, _identity = _assess_overseas_open_order_row(row)
        if assessment != "parsed":
            continue
        symbol = _optional_string(row.get("pdno")) or _optional_string(
            row.get("ovrs_pdno")
        )
        quantity = _snapshot_integer(row, ("ft_ord_qty", "ord_qty"))
        remaining_quantity = _snapshot_remaining_quantity(
            row,
            quantity=quantity,
            remaining_keys=("nccs_qty",),
            filled_keys=("ft_ccld_qty",),
        )
        assert symbol is not None
        assert quantity is not None
        assert remaining_quantity is not None
        open_orders.append(
            BrokerAccountOpenOrder(
                symbol=symbol.strip().upper(),
                side=_parse_order_side(row),
                quantity=quantity,
                remaining_quantity=remaining_quantity,
                status=_optional_string(row.get("prcs_stat_name")) or "open",
                broker_order_id=(
                    _optional_string(row.get("odno"))
                    or _optional_string(row.get("ord_no"))
                ),
                limit_price=_first_decimal(row, "ft_ord_unpr3", "ovrs_ord_unpr"),
                payload=dict(row),
            )
        )
    return open_orders


def _parse_overseas_available_cash(payload: dict[str, object]) -> Decimal | None:
    for row in _payload_rows(payload.get("output2")):
        value = _first_decimal(
            row,
            "ord_psbl_frcr_amt",
            "frcr_ord_psbl_amt1",
            "frcr_dncl_amt_2",
            "frcr_dncl_amt",
        )
        if value is not None and value >= 0:
            return value
    return None


def _parse_overseas_buying_power(
    symbol: str,
    payload: dict[str, object],
) -> BrokerSymbolBuyingPower | None:
    output = _first_payload_object(payload, "output", "output1")
    if output is None:
        return None
    available_cash = _first_decimal(
        output,
        "ord_psbl_frcr_amt",
        "frcr_ord_psbl_amt1",
        "ovrs_ord_psbl_amt",
    )
    buying_power = _first_decimal(output, "ovrs_ord_psbl_amt", "ord_psbl_frcr_amt")
    buyable_quantity = _parse_int(output.get("max_ord_psbl_qty")) or _parse_int(
        output.get("ovrs_max_ord_psbl_qty")
    )
    return BrokerSymbolBuyingPower(
        symbol=symbol,
        available_cash=available_cash,
        buying_power=buying_power,
        buyable_quantity=buyable_quantity,
        payload=dict(output),
    )


def _payload_rows(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _payload_collection_items(value: object) -> list[object]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return [value]
    if value is None:
        return []
    return [value]


def _snapshot_integer(
    row: dict[str, object],
    keys: tuple[str, ...],
) -> int | None:
    for key in keys:
        if key not in row:
            continue
        raw_value = row.get(key)
        if raw_value is None or (
            isinstance(raw_value, str) and not raw_value.strip()
        ):
            continue
        return _parse_int(raw_value)
    return None


def _first_integral_evidence(
    row: dict[str, object],
    keys: tuple[str, ...],
) -> tuple[int | None, bool]:
    for key in keys:
        if key not in row:
            continue
        raw_value = row.get(key)
        if raw_value is None or (
            isinstance(raw_value, str) and not raw_value.strip()
        ):
            continue
        return _parse_int(raw_value), True
    return None, False


def _snapshot_remaining_quantity(
    row: dict[str, object],
    *,
    quantity: int | None,
    remaining_keys: tuple[str, ...],
    filled_keys: tuple[str, ...],
) -> int | None:
    remaining = _snapshot_integer(row, remaining_keys)
    if remaining is not None:
        return remaining
    filled = _snapshot_integer(row, filled_keys)
    if quantity is None or filled is None:
        return None
    return quantity - filled


def _assess_holding_row(
    row: dict[str, object],
    *,
    symbol_keys: tuple[str, ...],
    quantity_keys: tuple[str, ...],
    sellable_quantity_keys: tuple[str, ...],
) -> tuple[str, str | None]:
    symbol = next(
        (
            candidate
            for key in symbol_keys
            if (candidate := _optional_string(row.get(key))) is not None
        ),
        None,
    )
    quantity = _snapshot_integer(row, quantity_keys)
    if symbol is None or quantity is None or quantity < 0:
        return "invalid", None
    sellable_quantity, sellable_quantity_supplied = _first_integral_evidence(
        row,
        sellable_quantity_keys,
    )
    if sellable_quantity_supplied and (
        sellable_quantity is None
        or sellable_quantity < 0
        or sellable_quantity > quantity
    ):
        return "invalid", None
    identity = symbol.strip().upper()
    if quantity == 0:
        return "omitted", identity
    return "parsed", identity


def _assess_domestic_holding_row(row: dict[str, object]) -> tuple[str, str | None]:
    return _assess_holding_row(
        row,
        symbol_keys=("pdno",),
        quantity_keys=("hldg_qty",),
        sellable_quantity_keys=("ord_psbl_qty",),
    )


def _assess_overseas_holding_row(row: dict[str, object]) -> tuple[str, str | None]:
    return _assess_holding_row(
        row,
        symbol_keys=("ovrs_pdno", "pdno"),
        quantity_keys=("ovrs_cblc_qty",),
        sellable_quantity_keys=("ord_psbl_qty",),
    )


def _assess_open_order_row(
    row: dict[str, object],
    *,
    symbol_keys: tuple[str, ...],
    quantity_keys: tuple[str, ...],
    remaining_keys: tuple[str, ...],
    filled_keys: tuple[str, ...],
) -> tuple[str, str | None]:
    symbol = next(
        (
            candidate
            for key in symbol_keys
            if (candidate := _optional_string(row.get(key))) is not None
        ),
        None,
    )
    broker_order_id = _optional_string(row.get("odno")) or _optional_string(
        row.get("ord_no")
    )
    side = _parse_order_side(row)
    quantity = _snapshot_integer(row, quantity_keys)
    if (
        symbol is None
        or broker_order_id is None
        or side not in {"buy", "sell"}
        or quantity is None
        or quantity < 0
    ):
        return "invalid", None
    if quantity == 0:
        return "omitted", broker_order_id
    remaining = _snapshot_remaining_quantity(
        row,
        quantity=quantity,
        remaining_keys=remaining_keys,
        filled_keys=filled_keys,
    )
    if remaining is None or remaining < 0 or remaining > quantity:
        return "invalid", None
    if remaining == 0:
        return "omitted", broker_order_id
    return "parsed", broker_order_id


def _assess_domestic_open_order_row(
    row: dict[str, object],
) -> tuple[str, str | None]:
    return _assess_open_order_row(
        row,
        symbol_keys=("pdno",),
        quantity_keys=("ord_qty",),
        remaining_keys=("rmn_qty", "ord_uncc_qty", "nccs_qty"),
        filled_keys=("tot_ccld_qty",),
    )


def _assess_overseas_open_order_row(
    row: dict[str, object],
) -> tuple[str, str | None]:
    return _assess_open_order_row(
        row,
        symbol_keys=("pdno", "ovrs_pdno"),
        quantity_keys=("ft_ord_qty", "ord_qty"),
        remaining_keys=("nccs_qty",),
        filled_keys=("ft_ccld_qty",),
    )


def _collection_parse_diagnostics(
    *,
    payload: dict[str, object],
    row_key: str,
    parsed_identities: list[str | None],
    assess_row: Callable[[dict[str, object]], tuple[str, str | None]],
) -> dict[str, int]:
    raw_items = _payload_collection_items(payload.get(row_key))
    invalid_row_count = 0
    omitted_row_count = 0
    expected_parsed_identities: list[str] = []
    for item in raw_items:
        if not isinstance(item, dict):
            invalid_row_count += 1
            continue
        assessment, identity = assess_row(item)
        if assessment == "invalid":
            invalid_row_count += 1
        elif assessment == "omitted":
            omitted_row_count += 1
        elif identity is not None:
            expected_parsed_identities.append(identity)
        else:
            invalid_row_count += 1
    identities = [identity for identity in parsed_identities if identity]
    if expected_parsed_identities != identities:
        invalid_row_count += max(
            abs(len(expected_parsed_identities) - len(identities)),
            1,
        )
    return {
        "raw_row_count": len(raw_items),
        "parsed_row_count": len(parsed_identities),
        "omitted_row_count": omitted_row_count,
        "invalid_row_count": invalid_row_count,
        "duplicate_identity_count": len(identities) - len(set(identities)),
    }


def _collection_parse_complete(
    *,
    payload: dict[str, object],
    row_key: str,
    diagnostics: dict[str, int],
) -> bool:
    return bool(
        row_key in payload
        and isinstance(payload.get(row_key), (dict, list))
        and diagnostics["raw_row_count"]
        == diagnostics["parsed_row_count"] + diagnostics["omitted_row_count"]
        and diagnostics["invalid_row_count"] == 0
        and diagnostics["duplicate_identity_count"] == 0
    )


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = Decimal(value.replace(",", ""))
            if not parsed.is_finite() or parsed != parsed.to_integral_value():
                return None
            return int(parsed)
        except (InvalidOperation, OverflowError, ValueError):
            return None
    return None


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
        fallback_local = fallback.astimezone(KIS_TIMEZONE)
        return fallback_local.replace(
            hour=int(order_time[0:2]),
            minute=int(order_time[2:4]),
            second=int(order_time[4:6]),
            microsecond=0,
        ).astimezone(UTC)
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
        normalized_key = "".join(
            character for character in key_lower if character.isalnum()
        )
        if key_lower in {"authorization", "appsecret", "hashkey"} or any(
            normalized_key.endswith(suffix)
            for suffix in (
                "authorization",
                "cookie",
                "cookieheader",
                "sessionid",
                "token",
                "secret",
                "credential",
                "hashkey",
                "appkey",
                "apikey",
            )
        ):
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
    raw_error_message = _error_message(payload)
    classification = _classify_error(
        status_code=400,
        code=code,
        message=raw_error_message,
    )
    retryable = classification in {
        BrokerErrorClassification.RATE_LIMIT,
        BrokerErrorClassification.TRANSIENT,
    }
    return BrokerAdapterError(
        safe_broker_error_message(raw_error_message),
        status_code=503 if retryable else 422,
        classification=classification,
        retryable=retryable,
        code=safe_broker_error_code(code, provider=KIS_PROVIDER),
        normalized_status=(
            NormalizedBrokerOrderStatus.RETRY_PENDING
            if retryable
            else NormalizedBrokerOrderStatus.REJECTED
        ),
        request_metadata=dict(request_metadata),
        request_payload=dict(request_payload),
        response_payload=_safe_error_payload(payload),
        observed_at=datetime.now(UTC),
    )
