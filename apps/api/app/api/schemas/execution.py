from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


class ExecutionPlanItemRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    intent: str = Field(min_length=1, max_length=64)
    quantity_policy: dict[str, Any] | None = None
    provider: str | None = Field(default=None, max_length=32)
    venue: str | None = Field(default=None, max_length=32)
    asset_class: str | None = Field(default=None, max_length=32)
    base_quantity: Decimal | None = Field(default=None, gt=0)
    quote_notional: Decimal | None = Field(default=None, gt=0)
    quantity_precision: int | None = Field(default=None, ge=0, le=18)
    price_precision: int | None = Field(default=None, ge=0, le=18)
    limit_price: Decimal | None = None
    trigger_price: Decimal | None = None
    time_in_force: str | None = Field(default=None, max_length=32)
    risk_context: dict[str, Any] | None = None
    decision_agent_name: str | None = Field(default=None, max_length=128)
    decision_snapshot_id: str | None = Field(default=None, max_length=128)
    decision_run_id: str | None = Field(default=None, max_length=128)
    source_plan_item_id: str | None = Field(default=None, max_length=128)
    source_market_date: date | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_identity_aliases(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        _copy_alias_value(
            normalized,
            destination="provider",
            aliases=("provider", "broker"),
        )
        _copy_alias_value(
            normalized,
            destination="venue",
            aliases=("venue", "broker"),
        )
        _copy_alias_value(
            normalized,
            destination="decision_agent_name",
            aliases=("decision_agent_name", "agent_name"),
        )
        _copy_alias_value(
            normalized,
            destination="decision_run_id",
            aliases=("decision_run_id", "run_id"),
        )
        _copy_alias_value(
            normalized,
            destination="source_plan_item_id",
            aliases=("source_plan_item_id", "core_item_key", "core_plan_item_id", "plan_item_id"),
        )
        _copy_alias_value(
            normalized,
            destination="source_market_date",
            aliases=("source_market_date", "strategy_market_date", "market_date"),
        )
        return normalized

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("side", "intent", "time_in_force", "provider", "venue", "asset_class")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized.lower()

    @field_validator("decision_agent_name", "decision_snapshot_id", "decision_run_id", "source_plan_item_id")
    @classmethod
    def normalize_identity_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


class ExecutionPlanRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    plan_id: str = Field(
        min_length=1,
        max_length=128,
        validation_alias=AliasChoices("plan_id", "external_plan_id"),
    )
    source: str = Field(min_length=1, max_length=64)
    market_date: date | None = None
    strategy_run_id: str | None = Field(default=None, max_length=128)
    requested_at: datetime | None = None
    items: list[ExecutionPlanItemRequest] = Field(min_length=1)

    @field_validator("plan_id", "source", "strategy_run_id")
    @classmethod
    def normalize_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @model_validator(mode="after")
    def require_us_kis_routing(self) -> "ExecutionPlanRequest":
        for item in self.items:
            extras = item.model_extra or {}
            risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
            market = str(extras.get("market") or risk_context.get("market") or "").strip().upper()
            currency = str(extras.get("currency") or risk_context.get("currency") or "").strip().upper()
            is_us_equity = item.asset_class == "us_equity" or market == "US" or currency == "USD"
            if not is_us_equity:
                continue
            if item.provider is None:
                raise ValueError("US equity execution requires an explicit provider")
            if item.provider != "kis":
                continue
            if item.venue != "kis":
                raise ValueError("US equity execution requires provider=kis and venue=kis")
            if item.source_plan_item_id is None:
                raise ValueError("US equity execution requires source_plan_item_id")
            exchange = str(extras.get("exchange") or risk_context.get("exchange") or "").strip().upper()
            if exchange not in {"NASD", "NYSE", "AMEX"}:
                raise ValueError("US equity execution requires NASD, NYSE, or AMEX exchange")
        return self


class PlaceOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    execution_plan_item_id: str | None = Field(default=None, max_length=36)
    provider: str = Field(default="kis", max_length=32)
    asset_class: str = Field(default="stock", max_length=32)
    venue: str | None = Field(default=None, max_length=32)
    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    quantity: int | None = Field(default=None, gt=0)
    base_quantity: Decimal | None = Field(default=None, gt=0)
    quote_notional: Decimal | None = Field(default=None, gt=0)
    quantity_precision: int | None = Field(default=None, ge=0, le=18)
    price_precision: int | None = Field(default=None, ge=0, le=18)
    order_type: str = Field(default="limit", max_length=16)
    limit_price: Decimal | None = None
    time_in_force: str = Field(default="day", max_length=32)
    exchange: str = Field(default="KRX", max_length=16)
    metadata: dict[str, Any] | None = None

    @field_validator("idempotency_key", "execution_plan_item_id", "venue")
    @classmethod
    def normalize_identifiers(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("symbol")
    @classmethod
    def normalize_order_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("provider", "asset_class", "side", "order_type", "time_in_force")
    @classmethod
    def normalize_order_text_fields(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("exchange must not be empty")
        return normalized

    @field_validator("limit_price")
    @classmethod
    def validate_limit_price(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("limit_price must be greater than zero")
        return value

    @field_validator("order_type")
    @classmethod
    def validate_order_type(cls, value: str) -> str:
        if value not in {"limit", "market"}:
            raise ValueError("order_type must be 'limit' or 'market'")
        return value

    @field_validator("side")
    @classmethod
    def validate_side(cls, value: str) -> str:
        if value not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        return value

    @field_validator("time_in_force")
    @classmethod
    def validate_time_in_force(cls, value: str) -> str:
        if value not in {"day", "gtc", "ioc"}:
            raise ValueError("time_in_force must be 'day', 'gtc', or 'ioc'")
        return value

    @field_validator("exchange")
    @classmethod
    def validate_exchange(cls, value: str) -> str:
        if value not in {"KRX", "NXT", "SOR", "UPBIT", "ALPACA", "US", "NASD", "NYSE", "AMEX"}:
            raise ValueError(
                "exchange must be 'KRX', 'NXT', 'SOR', 'UPBIT', 'ALPACA', 'US', 'NASD', 'NYSE', or 'AMEX'"
            )
        return value

    @model_validator(mode="after")
    def validate_sizing(self) -> "PlaceOrderRequest":
        if self.order_type == "limit" and self.limit_price is None:
            raise ValueError("limit_price is required for limit orders")
        if self.order_type == "market" and self.limit_price is not None:
            raise ValueError("limit_price must be omitted for market orders")
        if self.asset_class == "us_equity":
            if self.provider not in {"alpaca", "kis"}:
                raise ValueError("US-equity orders require provider='alpaca' or provider='kis'")
            if self.provider == "kis":
                if self.quantity is None or self.base_quantity is not None or self.quote_notional is not None:
                    raise ValueError("KIS US-equity orders require integer quantity only")
                if self.order_type != "limit":
                    raise ValueError("KIS US-equity orders support limit orders only")
                if self.exchange not in {"NASD", "NYSE", "AMEX"}:
                    raise ValueError("KIS US-equity orders require exchange='NASD', 'NYSE', or 'AMEX'")
                if self.time_in_force != "day":
                    raise ValueError("KIS US-equity orders require time_in_force='day'")
                return self
            sizing_fields = [
                self.quantity is not None,
                self.base_quantity is not None,
                self.quote_notional is not None,
            ]
            if sum(sizing_fields) != 1:
                raise ValueError(
                    "Alpaca US-equity orders require exactly one of base_quantity or quote_notional"
                )
            if self.quantity is not None:
                raise ValueError(
                    "Alpaca US-equity orders must use base_quantity or quote_notional, not integer quantity"
                )
            if self.quote_notional is not None and not (
                self.side == "buy" and self.order_type == "market"
            ):
                raise ValueError("quote_notional is only supported for Alpaca US-equity market buys")
            if self.exchange != "US":
                raise ValueError("Alpaca US-equity orders require exchange='US'")
            if self.time_in_force != "day":
                raise ValueError("Alpaca US-equity orders require time_in_force='day'")
            return self
        if self.asset_class == "crypto" or self.provider in {"upbit", "alpaca"}:
            if self.provider not in {"upbit", "alpaca"}:
                raise ValueError("crypto orders require provider='upbit' or provider='alpaca'")
            if self.asset_class != "crypto":
                raise ValueError("crypto providers require asset_class='crypto'")
            sizing_fields = [
                self.quantity is not None,
                self.base_quantity is not None,
                self.quote_notional is not None,
            ]
            if sum(sizing_fields) != 1:
                raise ValueError(
                    "crypto orders require exactly one of quantity, base_quantity, or quote_notional"
                )
            if self.quantity is not None:
                raise ValueError("crypto orders must use base_quantity or quote_notional, not integer quantity")
            if self.quote_notional is not None and not (self.side == "buy" and self.order_type == "market"):
                raise ValueError("quote_notional is only supported for crypto market buys")
            if self.provider == "upbit":
                if self.exchange != "UPBIT" or self.time_in_force != "day":
                    raise ValueError("Upbit crypto orders require exchange='UPBIT' and time_in_force='day'")
            if self.provider == "alpaca":
                if self.exchange != "ALPACA":
                    raise ValueError("Alpaca crypto orders require exchange='ALPACA'")
                if self.time_in_force not in {"gtc", "ioc"}:
                    raise ValueError("Alpaca crypto orders require time_in_force='gtc' or 'ioc'")
            return self
        if self.provider != "kis":
            raise ValueError("provider must be 'kis', 'upbit', or 'alpaca'")
        if self.time_in_force != "day":
            raise ValueError("KIS stock orders require time_in_force='day'")
        if self.quantity is None:
            raise ValueError("stock orders require quantity")
        if self.base_quantity is not None or self.quote_notional is not None:
            raise ValueError("stock orders must not include base_quantity or quote_notional")
        return self


class ShortTermWatcherAgentOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: str = Field(min_length=1, max_length=16)
    order_type: str = Field(default="market", max_length=16)
    quantity: int | None = Field(default=None, gt=0)
    max_notional: Decimal | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    reason_code: str | None = Field(default=None, max_length=64)
    rationale: str | None = Field(default=None, max_length=512)

    @field_validator("decision", "order_type", "reason_code")
    @classmethod
    def normalize_agent_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("decision")
    @classmethod
    def validate_decision(cls, value: str) -> str:
        if value not in {"approve", "reject"}:
            raise ValueError("decision must be 'approve' or 'reject'")
        return value

    @field_validator("order_type")
    @classmethod
    def validate_short_term_order_type(cls, value: str) -> str:
        if value not in {"market"}:
            raise ValueError("order_type must be 'market'")
        return value


class ShortTermWatcherAccountContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available_cash: Decimal = Field(gt=0)
    last_price: Decimal = Field(gt=0)
    holdings: dict[str, int] = Field(default_factory=dict)
    open_order_symbols: list[str] = Field(default_factory=list)

    @field_validator("holdings")
    @classmethod
    def normalize_holdings(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for symbol, quantity in value.items():
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("holding symbol must not be empty")
            if quantity < 0:
                raise ValueError("holding quantity must be non-negative")
            normalized[normalized_symbol] = quantity
        return normalized

    @field_validator("open_order_symbols")
    @classmethod
    def normalize_open_order_symbols(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for symbol in value:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("open order symbol must not be empty")
            normalized.append(normalized_symbol)
        return normalized


class ShortTermWatcherDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_plan_item_id: str = Field(min_length=1, max_length=36)
    dry_run: bool = True
    agent_output: ShortTermWatcherAgentOutput
    account_context: ShortTermWatcherAccountContext

    def account_context_from_snapshot(self, snapshot: object) -> ShortTermWatcherAccountContext:
        holdings = {
            holding.symbol: holding.quantity
            for holding in getattr(snapshot, "holdings", [])
            if getattr(holding, "quantity", 0) > 0
        }
        open_order_symbols = [
            order.symbol
            for order in getattr(snapshot, "open_orders", [])
            if getattr(order, "side", "").lower() == "buy" and getattr(order, "remaining_quantity", 0) > 0
        ]
        return ShortTermWatcherAccountContext(
            available_cash=getattr(snapshot, "available_cash"),
            last_price=self.account_context.last_price,
            holdings=holdings,
            open_order_symbols=open_order_symbols,
        )

    @field_validator("execution_plan_item_id")
    @classmethod
    def normalize_candidate_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("execution_plan_item_id must not be empty")
        return normalized


class StaleBrokerOrderReconciliationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    older_than_seconds: int | None = Field(default=None, ge=60, le=31536000)
    symbols: list[str] | None = None
    dry_run: bool = False

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized: list[str] = []
        for symbol in value:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("symbol must not be empty")
            normalized.append(normalized_symbol)
        return normalized


class MonitoringPriceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    price: Decimal = Field(gt=0)
    observed_at: datetime | None = None
    bid: Decimal | None = Field(default=None, gt=0)
    ask: Decimal | None = Field(default=None, gt=0)
    spread: Decimal | None = Field(default=None, ge=0)
    spread_bps: Decimal | None = Field(default=None, ge=0)
    orderbook_depth: Decimal | None = Field(default=None, ge=0)
    orderbook_imbalance: Decimal | None = None
    volume: Decimal | None = Field(default=None, ge=0)
    turnover: Decimal | None = Field(default=None, ge=0)
    day_high: Decimal | None = Field(default=None, gt=0)
    day_low: Decimal | None = Field(default=None, gt=0)
    session_type: str | None = Field(default=None, max_length=64)
    normal_ordering_available: bool | None = None
    provenance: dict[str, object] | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_monitoring_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized


class MonitoringEvaluationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", max_length=64)
    reason: str | None = None
    observed_at: datetime | None = None
    prices: list[MonitoringPriceRequest] = Field(min_length=1)

    @field_validator("actor", "source")
    @classmethod
    def normalize_monitoring_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


class ExecutionFailureStatusUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = Field(min_length=1, max_length=16)
    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", max_length=64)
    reason: str | None = None

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("status must not be empty")
        if normalized not in {"open", "resolved", "ignored"}:
            raise ValueError("status must be 'open', 'resolved', or 'ignored'")
        return normalized

    @field_validator("actor", "source")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


def _copy_alias_value(
    payload: dict[str, Any],
    *,
    destination: str,
    aliases: tuple[str, ...],
) -> None:
    if payload.get(destination) is not None:
        return
    for alias in aliases:
        value = payload.get(alias)
        if value is not None:
            payload[destination] = value
            return
