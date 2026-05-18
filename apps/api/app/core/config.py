import os
from decimal import Decimal
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
    autonomous_runtime_enabled: bool = False
    dispatch_loop_interval_seconds: float = Field(default=1.0, gt=0, le=3600)
    monitoring_loop_interval_seconds: float = Field(default=1.0, gt=0, le=3600)
    monitoring_max_symbols_per_cycle: int = Field(default=128, ge=1, le=10000)
    monitoring_max_broker_actions_per_cycle: int = Field(default=32, ge=1, le=10000)
    exec_agent_enabled: bool = False
    exec_agent_id: str = "exec-agent"
    exec_agent_model: str = "openai-codex/gpt-5.5"
    exec_agent_max_symbol_budget: Decimal = Field(default=Decimal("1000000"), gt=0)
    exec_agent_max_total_budget: Decimal = Field(default=Decimal("3000000"), gt=0)
    exec_agent_max_account_snapshot_age_seconds: int = Field(default=300, ge=1, le=86400)
    exec_agent_max_holding_trading_sessions: int = Field(default=3, ge=1, le=20)
    openclaw_base_url: str | None = None
    openclaw_gateway_token: str | None = None
    openclaw_agent_timeout_seconds: int = Field(default=120, ge=1, le=600)
    kis_app_key: str | None = None
    kis_app_secret: str | None = None
    kis_account_number: str | None = None
    kis_account_product_code: str = "01"
    kis_hts_id: str | None = None
    kis_base_url: str = "https://openapi.koreainvestment.com:9443"
    kis_mock_base_url: str = "https://openapivts.koreainvestment.com:29443"
    kis_customer_type: str = "P"
    kis_use_hashkey: bool = True
    kis_request_timeout_seconds: int = Field(default=10, ge=1, le=120)

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
    "AUTONOMOUS_RUNTIME_ENABLED": "autonomous_runtime_enabled",
    "DISPATCH_LOOP_INTERVAL_SECONDS": "dispatch_loop_interval_seconds",
    "MONITORING_LOOP_INTERVAL_SECONDS": "monitoring_loop_interval_seconds",
    "MONITORING_MAX_SYMBOLS_PER_CYCLE": "monitoring_max_symbols_per_cycle",
    "MONITORING_MAX_BROKER_ACTIONS_PER_CYCLE": "monitoring_max_broker_actions_per_cycle",
    "EXEC_AGENT_ENABLED": "exec_agent_enabled",
    "EXEC_AGENT_ID": "exec_agent_id",
    "EXEC_AGENT_MODEL": "exec_agent_model",
    "EXEC_AGENT_MAX_SYMBOL_BUDGET": "exec_agent_max_symbol_budget",
    "EXEC_AGENT_MAX_TOTAL_BUDGET": "exec_agent_max_total_budget",
    "EXEC_AGENT_MAX_ACCOUNT_SNAPSHOT_AGE_SECONDS": "exec_agent_max_account_snapshot_age_seconds",
    "EXEC_AGENT_MAX_HOLDING_TRADING_SESSIONS": "exec_agent_max_holding_trading_sessions",
    "OPENCLAW_BASE_URL": "openclaw_base_url",
    "OPENCLAW_GATEWAY_TOKEN": "openclaw_gateway_token",
    "OPENCLAW_AGENT_TIMEOUT_SECONDS": "openclaw_agent_timeout_seconds",
    "KIS_APP_KEY": "kis_app_key",
    "KIS_APP_SECRET": "kis_app_secret",
    "KIS_ACCOUNT_NUMBER": "kis_account_number",
    "KIS_ACCOUNT_PRODUCT_CODE": "kis_account_product_code",
    "KIS_HTS_ID": "kis_hts_id",
    "KIS_BASE_URL": "kis_base_url",
    "KIS_MOCK_BASE_URL": "kis_mock_base_url",
    "KIS_CUSTOMER_TYPE": "kis_customer_type",
    "KIS_USE_HASHKEY": "kis_use_hashkey",
    "KIS_REQUEST_TIMEOUT_SECONDS": "kis_request_timeout_seconds",
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
