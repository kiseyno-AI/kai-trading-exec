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
