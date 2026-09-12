from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BrokerSessionActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", max_length=64)
    reason: str | None = Field(default=None, max_length=512)
    force: bool = False

    @field_validator("actor", "source", "reason")
    @classmethod
    def normalize_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


class BrokerSessionActionResponse(BaseModel):
    """Public result contract for broker authentication and revocation actions."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["authenticate", "revoke"]
    outcome: Literal["applied", "noop", "rejected"]
    changed: bool
    message: str
    event_id: str
    session: dict[str, object]
    outcome_unknown: bool = False
    retry_required: bool = False
    error_code: str | None = None
    session_authoritative: bool = True
