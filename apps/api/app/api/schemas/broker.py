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
