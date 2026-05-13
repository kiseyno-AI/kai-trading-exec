from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.broker.base import (
    BrokerAccountPosition,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerSymbolBuyingPower,
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
KIS_INQUIRE_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_INQUIRE_PSBL_ORDER_PATH = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
KIS_CURRENT_PRICE_TR_ID = "FHKST01010100"
KIS_TIMEZONE = timezone(timedelta(hours=9))


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
            with urlopen(request, timeout=timeout_seconds) as response:
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
            "ORD_DVSN": _order_division(request.order_type),
            "ORD_QTY": str(request.quantity),
            "ORD_UNPR": _order_price(request.order_type, request.limit_price),
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

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str],
        reference_prices: dict[str, Decimal],
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        config = self._configured()
        if not self.settings.kis_account_number:
            raise BrokerAdapterError(
                "KIS account number is not configured",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
            )
        if exchange != "KRX":
            raise BrokerAdapterError(
                f"unsupported exchange for account snapshot: {exchange}",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
            )

        observed_at = datetime.now(UTC)
        balance_payloads = self._fetch_domestic_balance(
            config=config,
            access_token=access_token,
        )
        positions = _parse_balance_positions(balance_payloads)
        balance_cash = _parse_balance_cash(balance_payloads)

        symbol_buying_power: dict[str, BrokerSymbolBuyingPower] = {}
        buying_power_values: list[Decimal] = []
        available_cash_values: list[Decimal] = []
        for symbol in _unique_symbols(symbols):
            reference_price = reference_prices.get(symbol)
            if reference_price is None or reference_price <= 0:
                continue
            buying_power_payload = self._fetch_symbol_buying_power(
                config=config,
                access_token=access_token,
                symbol=symbol,
                reference_price=reference_price,
            )
            buying_power = _parse_symbol_buying_power(symbol, buying_power_payload)
            symbol_buying_power[symbol] = buying_power
            if buying_power.buying_power is not None:
                buying_power_values.append(buying_power.buying_power)
            if buying_power.available_cash is not None:
                available_cash_values.append(buying_power.available_cash)

        available_cash = balance_cash
        if available_cash is None and available_cash_values:
            available_cash = min(available_cash_values)
        buying_power = min(buying_power_values) if buying_power_values else available_cash
        return BrokerAccountSnapshot(
            observed_at=observed_at,
            available_cash=available_cash,
            buying_power=buying_power,
            positions=positions,
            open_orders=[],
            symbol_buying_power=symbol_buying_power,
            payload={
                "provider": self.provider,
                "balance": balance_payloads,
                "symbol_buying_power": {
                    symbol: power.payload
                    for symbol, power in symbol_buying_power.items()
                },
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
    ) -> dict[str, Any] | None:
        del access_token
        return {
            "provider": self.provider,
            "broker_order_id": broker_order_id,
            "status": "not_implemented",
            "strategy": "bounded_rest_fallback",
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
            raise BrokerAdapterError(config.reason or "KIS configuration is unavailable", status_code=409)
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

    def _fetch_domestic_balance(
        self,
        *,
        config: BrokerConfig,
        access_token: str,
    ) -> list[dict[str, object]]:
        tr_id = "TTTC8434R" if self.settings.runtime_mode == "live" else "VTTC8434R"
        params = {
            "CANO": self.settings.kis_account_number or "",
            "ACNT_PRDT_CD": self.settings.kis_account_product_code,
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
        payloads: list[dict[str, object]] = []
        tr_cont = ""
        for _ in range(10):
            headers = self.build_headers(access_token, transaction_id=tr_id)
            headers["tr_cont"] = tr_cont
            headers["custtype"] = self.settings.kis_customer_type
            response = self.http_client.get_json(
                url=f"{config.api_base_url}{KIS_INQUIRE_BALANCE_PATH}",
                headers=headers,
                params=params,
                timeout_seconds=self.settings.kis_request_timeout_seconds,
            )
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
                    code=_optional_string(body.get("msg_cd")),
                    response_metadata={
                        "status_code": response.status_code,
                        "headers": _sanitize_headers(response.headers),
                    },
                    response_payload=body,
                    observed_at=datetime.now(UTC),
                )
            payloads.append(body)

            next_tr_cont = response.headers.get("tr_cont", "")
            if next_tr_cont not in {"M", "F"}:
                break
            params["CTX_AREA_FK100"] = str(body.get("ctx_area_fk100") or "")
            params["CTX_AREA_NK100"] = str(body.get("ctx_area_nk100") or "")
            tr_cont = "N"
        return payloads

    def _fetch_symbol_buying_power(
        self,
        *,
        config: BrokerConfig,
        access_token: str,
        symbol: str,
        reference_price: Decimal,
    ) -> dict[str, object]:
        tr_id = "TTTC8908R" if self.settings.runtime_mode == "live" else "VTTC8908R"
        headers = self.build_headers(access_token, transaction_id=tr_id)
        headers["custtype"] = self.settings.kis_customer_type
        response = self.http_client.get_json(
            url=f"{config.api_base_url}{KIS_INQUIRE_PSBL_ORDER_PATH}",
            headers=headers,
            params={
                "CANO": self.settings.kis_account_number or "",
                "ACNT_PRDT_CD": self.settings.kis_account_product_code,
                "PDNO": symbol,
                "ORD_UNPR": _kis_whole_krw(reference_price),
                "ORD_DVSN": "01",
                "CMA_EVLU_AMT_ICLD_YN": "Y",
                "OVRS_ICLD_YN": "N",
            },
            timeout_seconds=self.settings.kis_request_timeout_seconds,
        )
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
                code=_optional_string(body.get("msg_cd")),
                response_metadata={
                    "status_code": response.status_code,
                    "headers": _sanitize_headers(response.headers),
                },
                response_payload=body,
                observed_at=datetime.now(UTC),
            )
        return body


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


def _order_division(order_type: str) -> str:
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


def _order_price(order_type: str, limit_price: Any) -> str:
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


def _kis_whole_krw(value: Decimal) -> str:
    whole = value.quantize(Decimal("1"), rounding=ROUND_DOWN)
    if whole <= 0:
        raise BrokerAdapterError(
            "reference price must be positive for KIS buying power inquiry",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
        )
    return str(whole)


def _quote_market_division(exchange: str) -> str:
    if exchange != "KRX":
        raise BrokerAdapterError(
            f"unsupported exchange for current price quotes: {exchange}",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
        )
    return "J"


def _parse_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: object) -> int | None:
    decimal_value = _parse_decimal(value)
    if decimal_value is None:
        return None
    return int(decimal_value.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _payload_output_list(payload: dict[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _payload_output_object(payload: dict[str, object], key: str) -> dict[str, object] | None:
    value = payload.get(key)
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                return item
    return None


def _parse_balance_positions(payloads: list[dict[str, object]]) -> list[BrokerAccountPosition]:
    positions: list[BrokerAccountPosition] = []
    for payload in payloads:
        for row in _payload_output_list(payload, "output1"):
            symbol = _first_string(row, ("pdno", "PDNO", "stck_shrn_iscd"))
            if symbol is None:
                continue
            quantity = _parse_int(_first_value(row, ("hldg_qty", "HLDG_QTY", "ord_psbl_qty")))
            if quantity is None or quantity <= 0:
                continue
            positions.append(
                BrokerAccountPosition(
                    symbol=symbol.strip().upper(),
                    quantity=quantity,
                    average_price=_parse_decimal(
                        _first_value(row, ("pchs_avg_pric", "avg_prvs", "pchs_avg_pric_amt"))
                    ),
                    payload=dict(row),
                )
            )
    return positions


def _parse_balance_cash(payloads: list[dict[str, object]]) -> Decimal | None:
    cash_values: list[Decimal] = []
    for payload in payloads:
        output = _payload_output_object(payload, "output2")
        if output is None:
            continue
        for key in (
            "dnca_tot_amt",
            "prvs_rcdl_excc_amt",
            "ord_psbl_cash",
            "nrcvb_buy_amt",
            "max_buy_amt",
        ):
            value = _parse_decimal(output.get(key))
            if value is not None and value >= 0:
                cash_values.append(value)
                break
    if not cash_values:
        return None
    return min(cash_values)


def _parse_symbol_buying_power(
    symbol: str,
    payload: dict[str, object],
) -> BrokerSymbolBuyingPower:
    output = _payload_output_object(payload, "output") or {}
    available_cash = _first_decimal(
        output,
        ("ord_psbl_cash", "nrcvb_buy_amt", "max_buy_amt"),
    )
    buying_power = _first_decimal(output, ("nrcvb_buy_amt", "ord_psbl_cash", "max_buy_amt"))
    buyable_quantity = _parse_int(
        _first_value(output, ("nrcvb_buy_qty", "ord_psbl_qty", "max_buy_qty"))
    )
    return BrokerSymbolBuyingPower(
        symbol=symbol,
        available_cash=available_cash,
        buying_power=buying_power,
        buyable_quantity=buyable_quantity if buyable_quantity and buyable_quantity > 0 else None,
        payload=dict(output),
    )


def _first_value(payload: dict[str, object], keys: tuple[str, ...]) -> object | None:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def _first_string(payload: dict[str, object], keys: tuple[str, ...]) -> str | None:
    value = _first_value(payload, keys)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _first_decimal(payload: dict[str, object], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        value = _parse_decimal(payload.get(key))
        if value is not None:
            return value
    return None


def _transaction_id(runtime_mode: str, side: str) -> str:
    live = runtime_mode == "live"
    if side == "sell":
        return "TTTC0011U" if live else "VTTC0011U"
    return "TTTC0012U" if live else "VTTC0012U"


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
