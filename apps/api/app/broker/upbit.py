from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from app.broker.base import (
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings

UPBIT_PROVIDER = "upbit"
UPBIT_API_BASE_URL = "https://api.upbit.com"


class UpbitBrokerAdapter:
    provider = UPBIT_PROVIDER

    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings

    def describe(self) -> BrokerConfig:
        configured, reason = self._configured_state()
        return BrokerConfig(
            provider=self.provider,
            mode=self.settings.runtime_mode,
            configured=configured,
            api_base_url=self.settings.upbit_base_url or UPBIT_API_BASE_URL,
            reason=reason,
            websocket_approval_path=None,
            shared_realtime_subscriptions_supported=False,
            reconnect_supported=False,
            bounded_rest_reconciliation_supported=False,
        )

    def authenticate(self) -> BrokerTokenGrant:
        config = self.describe()
        if not config.configured:
            raise BrokerAdapterError(
                config.reason or "Upbit execution is not configured",
                status_code=409,
                classification=BrokerErrorClassification.AUTH,
            )
        if self.settings.runtime_mode == "live" and not self.settings.upbit_live_trading_enabled:
            raise BrokerAdapterError(
                "Upbit live trading is disabled by configuration",
                status_code=409,
                classification=BrokerErrorClassification.AUTH,
            )
        return BrokerTokenGrant(
            access_token=f"{self.provider}:{self.settings.runtime_mode}:paper-token",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(days=365),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        raise BrokerAdapterError(
            "Upbit realtime approval is not supported by this adapter skeleton",
            status_code=501,
            classification=BrokerErrorClassification.VALIDATION,
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        return BrokerTokenRevocation(message="Upbit paper/mock session revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        del access_token
        observed_at = datetime.now(UTC)
        self._validate_order_request(request)
        if self.settings.runtime_mode == "live" and not self.settings.upbit_live_trading_enabled:
            raise BrokerAdapterError(
                "Upbit live order submission is disabled by configuration",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                retryable=False,
                code="upbit_live_trading_disabled",
                request_metadata=self._request_metadata(request),
                response_metadata={"provider": self.provider, "mode": self.settings.runtime_mode},
                request_payload=self._request_payload(request),
                response_payload={"error": "upbit_live_trading_disabled"},
                observed_at=observed_at,
            )
        if self.settings.runtime_mode == "live":
            raise BrokerAdapterError(
                "Upbit live order submission is not implemented",
                status_code=501,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                retryable=False,
                code="upbit_live_not_implemented",
                request_metadata=self._request_metadata(request),
                response_metadata={"provider": self.provider, "mode": self.settings.runtime_mode},
                request_payload=self._request_payload(request),
                response_payload={"error": "upbit_live_not_implemented"},
                observed_at=observed_at,
            )

        broker_order_id = f"upbit-paper-{request.idempotency_key}"
        return BrokerOrderPlacement(
            broker_order_id=broker_order_id,
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="paper_submitted",
            submitted_at=observed_at,
            observed_at=observed_at,
            request_metadata=self._request_metadata(request),
            response_metadata={
                "provider": self.provider,
                "mode": self.settings.runtime_mode,
                "paper": True,
            },
            request_payload=self._request_payload(request),
            response_payload={
                "uuid": broker_order_id,
                "market": request.symbol,
                "side": request.side,
                "ord_type": request.order_type,
                "state": "paper_submitted",
                "volume": str(request.base_quantity) if request.base_quantity is not None else None,
                "price": _order_price(request),
            },
            message="Upbit paper/mock order accepted",
        )

    def get_current_price(self, access_token: str, *, symbol: str, exchange: str = "UPBIT"):
        return None

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "UPBIT",
        reference_price: Decimal | None = None,
    ):
        return None

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "UPBIT",
    ) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            available_cash=None,
            observed_at=datetime.now(UTC),
            source="upbit:paper",
            payload={"provider": self.provider, "mode": self.settings.runtime_mode},
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(
            supported=False,
            strategy="not_supported",
            details={"provider": self.provider},
        )

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "UPBIT",
        submitted_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        del submitted_at
        return None

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        return {"authorization": f"Bearer {access_token}"}

    def _configured_state(self) -> tuple[bool, str | None]:
        if self.settings.runtime_mode in {"mock", "paper"}:
            return True, None
        if not self.settings.upbit_live_trading_enabled:
            return False, "Upbit live trading is disabled by default"
        if not self.settings.upbit_access_key or not self.settings.upbit_secret_key:
            return False, "Upbit access key and secret key are required for live mode"
        return True, None

    def _validate_order_request(self, request: BrokerOrderPlacementRequest) -> None:
        if request.asset_class != "crypto":
            raise BrokerAdapterError(
                "Upbit adapter only accepts crypto orders",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )
        if request.quantity is not None:
            raise BrokerAdapterError(
                "Upbit orders must not use integer stock quantity",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )
        if (request.base_quantity is None) == (request.quote_notional is None):
            raise BrokerAdapterError(
                "Upbit orders require exactly one of base_quantity or quote_notional",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )
        if request.quote_notional is not None and not (
            request.side == "buy" and request.order_type == "market"
        ):
            raise BrokerAdapterError(
                "Upbit quote_notional is only supported for market buys",
                status_code=422,
                classification=BrokerErrorClassification.VALIDATION,
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )

    def _request_metadata(self, request: BrokerOrderPlacementRequest) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "venue": request.venue or "upbit",
            "asset_class": request.asset_class,
            "market": request.symbol,
            "mode": self.settings.runtime_mode,
            "paper": self.settings.runtime_mode != "live",
            "sizing": {
                "base_quantity": str(request.base_quantity) if request.base_quantity is not None else None,
                "quote_notional": str(request.quote_notional) if request.quote_notional is not None else None,
                "limit_price": str(request.limit_price) if request.limit_price is not None else None,
                "quantity_precision": request.quantity_precision,
                "price_precision": request.price_precision,
            },
        }

    def _request_payload(self, request: BrokerOrderPlacementRequest) -> dict[str, Any]:
        return {
            "market": request.symbol,
            "side": request.side,
            "ord_type": request.order_type,
            "volume": str(request.base_quantity) if request.base_quantity is not None else None,
            "price": _order_price(request),
            "identifier": request.idempotency_key,
        }


def _order_price(request: BrokerOrderPlacementRequest) -> str | None:
    if request.order_type == "limit":
        return str(request.limit_price) if request.limit_price is not None else None
    return str(request.quote_notional) if request.quote_notional is not None else None
