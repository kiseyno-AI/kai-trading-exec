from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from app.core.config import RuntimeMode


class BrokerErrorClassification(StrEnum):
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    TRANSIENT = "transient"
    VALIDATION = "validation"
    BROKER_REJECTED = "broker_rejected"
    NETWORK = "network"
    UNKNOWN = "unknown"


class BrokerEventSource(StrEnum):
    SUBMISSION = "submission"
    REALTIME = "realtime"
    RECONCILIATION = "reconciliation"
    OPERATOR = "operator"
    SYSTEM = "system"


class NormalizedBrokerOrderStatus(StrEnum):
    SUBMISSION_PENDING = "submission_pending"
    SUBMITTED = "submitted"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCEL_PENDING = "cancel_pending"
    CANCELED = "canceled"
    REJECTED = "rejected"
    RETRY_PENDING = "retry_pending"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class BrokerConfig:
    provider: str
    mode: RuntimeMode
    configured: bool
    api_base_url: str | None
    reason: str | None = None
    websocket_approval_path: str | None = None
    shared_realtime_subscriptions_supported: bool = True
    reconnect_supported: bool = True
    bounded_rest_reconciliation_supported: bool = True


@dataclass(frozen=True)
class BrokerTokenGrant:
    access_token: str
    token_type: str
    access_token_expires_at: datetime
    refresh_token: str | None = None
    refresh_token_expires_at: datetime | None = None


@dataclass(frozen=True)
class BrokerRealtimeApproval:
    approval_key: str
    issued_at: datetime


@dataclass(frozen=True)
class BrokerCurrentPrice:
    symbol: str
    exchange: str
    price: Decimal
    observed_at: datetime
    payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerLiveMarketContext:
    symbol: str
    exchange: str
    observed_at: datetime
    bid: Decimal | None = None
    ask: Decimal | None = None
    spread: Decimal | None = None
    spread_bps: Decimal | None = None
    orderbook_depth: Decimal | None = None
    orderbook_imbalance: Decimal | None = None
    session_type: str | None = None
    normal_ordering_available: bool | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerAccountPosition:
    symbol: str
    quantity: int
    average_price: Decimal | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerAccountHolding:
    symbol: str
    quantity: int
    average_price: Decimal | None = None
    sellable_quantity: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerAccountOpenOrder:
    symbol: str
    side: str
    quantity: int
    remaining_quantity: int | None = None
    status: str = "open"
    broker_order_id: str | None = None
    idempotency_key: str | None = None
    source_candidate_id: str | None = None
    limit_price: Decimal | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.remaining_quantity is None:
            object.__setattr__(self, "remaining_quantity", self.quantity)


@dataclass(frozen=True)
class BrokerSymbolBuyingPower:
    symbol: str
    available_cash: Decimal | None
    buying_power: Decimal | None
    buyable_quantity: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerAccountSnapshot:
    available_cash: Decimal | None = None
    observed_at: datetime = field(default_factory=datetime.utcnow)
    source: str = "compat:unknown"
    buying_power: Decimal | None = None
    holdings: list[BrokerAccountHolding] = field(default_factory=list)
    positions: list[BrokerAccountPosition] = field(default_factory=list)
    open_orders: list[BrokerAccountOpenOrder] = field(default_factory=list)
    symbol_buying_power: dict[str, BrokerSymbolBuyingPower] = field(default_factory=dict)
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.positions and self.holdings:
            object.__setattr__(
                self,
                "positions",
                [
                    BrokerAccountPosition(
                        symbol=holding.symbol,
                        quantity=holding.quantity,
                        average_price=holding.average_price,
                        payload=holding.payload,
                    )
                    for holding in self.holdings
                ],
            )
        if self.buying_power is None:
            object.__setattr__(self, "buying_power", self.available_cash)


@dataclass(frozen=True)
class BrokerTokenRevocation:
    message: str


@dataclass(frozen=True)
class BrokerOrderPlacementRequest:
    idempotency_key: str
    symbol: str
    side: str
    quantity: int
    order_type: str
    time_in_force: str
    exchange: str
    limit_price: Decimal | None = None
    execution_plan_item_id: str | None = None
    order_division: str | None = None
    route_session: str | None = None
    route_reason: str | None = None


@dataclass(frozen=True)
class BrokerOrderPlacement:
    broker_order_id: str | None
    normalized_status: NormalizedBrokerOrderStatus
    broker_status: str | None
    submitted_at: datetime | None
    observed_at: datetime
    request_metadata: dict[str, Any]
    response_metadata: dict[str, Any]
    request_payload: dict[str, Any]
    response_payload: dict[str, Any]
    message: str | None = None


class BrokerAdapterError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        classification: BrokerErrorClassification = BrokerErrorClassification.UNKNOWN,
        retryable: bool = False,
        code: str | None = None,
        normalized_status: NormalizedBrokerOrderStatus = NormalizedBrokerOrderStatus.FAILED,
        request_metadata: dict[str, Any] | None = None,
        response_metadata: dict[str, Any] | None = None,
        request_payload: dict[str, Any] | None = None,
        response_payload: dict[str, Any] | None = None,
        observed_at: datetime | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.classification = classification
        self.retryable = retryable
        self.code = code
        self.normalized_status = normalized_status
        self.request_metadata = request_metadata or {}
        self.response_metadata = response_metadata or {}
        self.request_payload = request_payload or {}
        self.response_payload = response_payload or {}
        self.observed_at = observed_at


@dataclass(frozen=True)
class BrokerReconnectHandle:
    supported: bool
    strategy: str
    details: dict[str, Any] = field(default_factory=dict)


class BrokerAdapter(Protocol):
    provider: str

    def describe(self) -> BrokerConfig: ...

    def authenticate(self) -> BrokerTokenGrant: ...

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval: ...

    def revoke(self, access_token: str) -> BrokerTokenRevocation: ...

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement: ...

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None: ...

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext | None: ...

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot: ...

    def reconnect_handle(self) -> BrokerReconnectHandle: ...

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
    ) -> dict[str, Any] | None: ...

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]: ...
