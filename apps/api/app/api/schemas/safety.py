from pydantic import BaseModel, Field


class SafetyControlRequest(BaseModel):
    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=500)
