from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PlanJudgmentActionRequest(BaseModel):
    """Typed input for the global hourly buy-risk observation."""

    model_config = ConfigDict(extra="forbid")

    risk_flags: int = Field(strict=True, ge=0)
    risk_reasons: list[str] = Field(max_length=32)
    observation_id: str | None = Field(default=None, max_length=128)
    market_date: date | None = None
    source_observed_at: datetime | None = None

    @field_validator("risk_reasons")
    @classmethod
    def normalize_risk_reasons(cls, value: list[str]) -> list[str]:
        return [reason.strip() for reason in value if reason.strip()]

    @field_validator("observation_id")
    @classmethod
    def normalize_observation_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("observation_id must not be blank")
        return normalized

    @field_validator("source_observed_at")
    @classmethod
    def require_aware_source_observed_at(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("source_observed_at must include a timezone offset")
        return value

    @model_validator(mode="after")
    def require_healthy_observation_identity(self) -> "PlanJudgmentActionRequest":
        if self.risk_flags != 0:
            return self
        missing = [
            name
            for name, value in (
                ("observation_id", self.observation_id),
                ("market_date", self.market_date),
                ("source_observed_at", self.source_observed_at),
            )
            if value is None
        ]
        if missing:
            raise ValueError(
                "healthy observation requires " + ", ".join(missing)
            )
        return self
