from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


class ExecutionPlanItemRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    intent: str = Field(min_length=1, max_length=64)
    quantity_policy: dict[str, Any] | None = None
    limit_price: Decimal | None = None
    trigger_price: Decimal | None = None
    time_in_force: str | None = Field(default=None, max_length=32)
    risk_context: dict[str, Any] | None = None
    decision_snapshot_id: str | None = Field(default=None, max_length=128)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("side", "intent", "time_in_force")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized.lower()


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


class PlaceOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    execution_plan_item_id: str | None = Field(default=None, max_length=36)
    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    quantity: int = Field(gt=0)
    order_type: str = Field(default="limit", max_length=16)
    limit_price: Decimal | None = None
    time_in_force: str = Field(default="day", max_length=32)
    exchange: str = Field(default="KRX", max_length=16)

    @field_validator("idempotency_key", "execution_plan_item_id")
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

    @field_validator("side", "order_type", "time_in_force")
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
        if value not in {"day"}:
            raise ValueError("time_in_force must be 'day'")
        return value

    @field_validator("exchange")
    @classmethod
    def validate_exchange(cls, value: str) -> str:
        if value not in {"KRX", "SOR"}:
            raise ValueError("exchange must be 'KRX' or 'SOR'")
        return value

    def model_post_init(self, __context: Any) -> None:
        if self.order_type == "limit" and self.limit_price is None:
            raise ValueError("limit_price is required for limit orders")
        if self.order_type == "market" and self.limit_price is not None:
            raise ValueError("limit_price must be omitted for market orders")


class MonitoringPriceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    price: Decimal = Field(gt=0)

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
