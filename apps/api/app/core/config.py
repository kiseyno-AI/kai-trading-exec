import os
from functools import lru_cache
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

RuntimeMode = Literal["mock", "paper", "live"]
LogFormat = Literal["json", "plain"]


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    service_name: str = "kai-trading-exec"
    environment: str = "local"
    runtime_mode: RuntimeMode = "mock"
    database_url: str = "sqlite:///./var/kai_trading_exec.db"
    log_level: str = "INFO"
    log_format: LogFormat = "json"
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8010, ge=1, le=65535)
    automation_enabled: bool = False

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return normalized


_ENV_FIELDS = {
    "SERVICE_NAME": "service_name",
    "ENVIRONMENT": "environment",
    "RUNTIME_MODE": "runtime_mode",
    "DATABASE_URL": "database_url",
    "LOG_LEVEL": "log_level",
    "LOG_FORMAT": "log_format",
    "API_HOST": "api_host",
    "API_PORT": "api_port",
    "AUTOMATION_ENABLED": "automation_enabled",
}


def load_settings(
    environ: Mapping[str, str] | None = None,
    *,
    prefix: str = "KAI_EXEC_",
) -> Settings:
    source = os.environ if environ is None else environ
    values: dict[str, str] = {}
    for env_key, field_name in _ENV_FIELDS.items():
        full_key = f"{prefix}{env_key}"
        if full_key in source:
            values[field_name] = source[full_key]
    return Settings(**values)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()

