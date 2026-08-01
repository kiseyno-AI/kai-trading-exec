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
    monitoring_cycle_timeout_seconds: float = Field(default=20.0, gt=0, le=3600)
    monitoring_expire_prior_market_watch_buys_enabled: bool = True
    monitoring_max_symbols_per_cycle: int = Field(default=128, ge=1, le=10000)
    monitoring_max_broker_actions_per_cycle: int = Field(default=32, ge=1, le=10000)
    monitoring_max_quote_age_seconds: int = Field(default=30, ge=1, le=3600)
    monitoring_watch_buy_retry_cooldown_seconds: int = Field(default=5, ge=1, le=3600)
    stale_broker_order_reconciliation_threshold_seconds: int = Field(default=86400, ge=60, le=31536000)
    watch_buy_high_risk_gate_enabled: bool = True
    watch_buy_high_atr_pct_threshold: Decimal = Field(default=Decimal("9.0"), ge=0, le=Decimal("100"))
    watch_buy_weak_ret20_threshold_pct: Decimal = Field(default=Decimal("0.0"), ge=Decimal("-100"), le=Decimal("100"))
    watch_buy_deep_negative_ret20_threshold_pct: Decimal = Field(default=Decimal("-10.0"), ge=Decimal("-100"), le=Decimal("0"))
    watch_buy_extended_ret5_threshold_pct: Decimal = Field(default=Decimal("15.0"), ge=0, le=Decimal("100"))
    watch_buy_max_intraday_range_position_pct: Decimal = Field(default=Decimal("75.0"), ge=0, le=Decimal("100"))
    watch_buy_stale_trigger_reclaim_threshold_pct: Decimal = Field(default=Decimal("3.0"), ge=0, le=Decimal("100"))
    profit_protection_min_peak_pct: Decimal = Field(default=Decimal("3.0"), gt=0, le=Decimal("100"))
    profit_protection_giveback_pct: Decimal = Field(default=Decimal("1.5"), gt=0, le=Decimal("100"))
    profit_protection_aged_session_min: int = Field(default=2, ge=1, le=20)
    profit_protection_aged_min_current_profit_pct: Decimal = Field(default=Decimal("1.0"), ge=0, le=Decimal("100"))
    breakeven_stop_buffer_pct: Decimal = Field(default=Decimal("0.2"), ge=0, le=Decimal("5"))
    market_timezone: str = "Asia/Seoul"
    exec_agent_enabled: bool = False
    exec_agent_id: str = "exec-agent"
    exec_agent_model: str = "openai-codex/gpt-5.5"
    exec_agent_max_symbol_budget: Decimal = Field(default=Decimal("3000000"), gt=0)
    exec_agent_max_total_budget: Decimal = Field(default=Decimal("9000000"), gt=0)
    exec_agent_base_risk_pct: Decimal = Field(default=Decimal("0.015"), gt=0, le=Decimal("0.015"))
    exec_agent_aggressive_risk_pct: Decimal = Field(default=Decimal("0.015"), gt=0, le=Decimal("0.015"))
    exec_agent_aggressive_confidence_threshold: Decimal = Field(default=Decimal("0.90"), gt=0, le=1)
    exec_agent_max_position_allocation_pct: Decimal = Field(default=Decimal("0.30"), gt=0, le=1)
    exec_agent_max_symbol_exposure_pct: Decimal = Field(default=Decimal("0.30"), gt=0, le=1)
    exec_agent_max_symbol_total_exposure_pct: Decimal = Field(default=Decimal("0.30"), gt=0, le=1)
    exec_agent_max_daily_new_buy_notional: Decimal = Field(default=Decimal("0"), ge=0)
    exec_agent_max_trade_risk_pct: Decimal = Field(default=Decimal("0.015"), gt=0, le=Decimal("0.015"))
    exec_agent_stop_loss_pct: Decimal = Field(default=Decimal("0.025"), gt=0, le=Decimal("0.10"))
    exec_agent_max_entry_premium_pct: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("0.10"))
    exec_agent_min_reward_risk: Decimal = Field(default=Decimal("1.30"), gt=0)
    exec_agent_relaxed_min_reward_risk: Decimal = Field(default=Decimal("1.10"), gt=0)
    exec_agent_absolute_min_reward_risk: Decimal = Field(default=Decimal("1.00"), gt=0)
    exec_agent_tactical_scalp_min_target_pct: Decimal = Field(default=Decimal("0.02"), gt=0, le=1)
    exec_agent_tactical_scalp_max_stop_pct: Decimal = Field(default=Decimal("0.015"), gt=0, le=1)
    exec_agent_max_account_snapshot_age_seconds: int = Field(default=300, ge=1, le=86400)
    exec_agent_max_holding_trading_sessions: int = Field(default=2, ge=1, le=20)
    exec_agent_first_take_profit_pct: Decimal = Field(default=Decimal("0.025"), gt=0, le=Decimal("0.10"))
    exec_agent_final_take_profit_pct: Decimal = Field(default=Decimal("0.055"), gt=0, le=Decimal("0.20"))
    exec_agent_partial_take_profit_fraction: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)
    exec_agent_trigger_time_enabled: bool = True
    exec_agent_trigger_time_sell_enabled: bool = True
    exec_agent_trigger_time_timeout_seconds: int = Field(default=900, ge=10, le=3600)
    monthly_target_krw: Decimal = Field(default=Decimal("500000"), gt=0)
    monthly_capital_preservation_enabled: bool = True
    monthly_aggressive_lag_ratio: Decimal = Field(default=Decimal("0.75"), gt=0, le=1)
    exec_agent_watch_sizing_enabled: bool = False
    exec_agent_watch_sizing_loop_interval_seconds: float = Field(default=60.0, gt=0, le=3600)
    exec_agent_watch_sizing_batch_size: int = Field(default=3, ge=1, le=10)
    exec_agent_watch_sizing_max_batches_per_cycle: int = Field(default=3, ge=1, le=100)
    exec_agent_watch_sizing_max_attempts: int = Field(default=3, ge=1, le=10)
    exec_agent_watch_sizing_start_local_time: str = "08:00"
    exec_agent_watch_sizing_end_local_time: str = "20:00"
    exec_agent_watch_sizing_timeout_seconds: int = Field(default=900, ge=30, le=3600)
    exec_agent_watch_sizing_min_notional_krw: Decimal = Field(default=Decimal("1000000"), ge=0)
    exec_agent_watch_sizing_high_conviction_min_notional_krw: Decimal = Field(default=Decimal("1500000"), ge=0)
    exec_agent_watch_sizing_high_conviction_threshold: Decimal = Field(default=Decimal("0.65"), ge=0, le=1)
    exec_agent_watch_sizing_fallback_account_basis_krw: Decimal = Field(default=Decimal("10000000"), gt=0)
    account_drawdown_gate_enabled: bool = True
    account_daily_total_asset_drawdown_pct: Decimal = Field(default=Decimal("1.0"), ge=0, le=Decimal("100"))
    account_daily_total_asset_drawdown_krw: Decimal = Field(default=Decimal("100000"), ge=0)
    account_weekly_total_asset_drawdown_pct: Decimal = Field(default=Decimal("2.0"), ge=0, le=Decimal("100"))
    account_weekly_total_asset_baseline_krw: Decimal = Field(default=Decimal("0"), ge=0)
    account_weekly_total_asset_baseline_at: str | None = None
    account_unrealized_loss_abs_krw: Decimal = Field(default=Decimal("100000"), ge=0)
    account_unrealized_loss_pct: Decimal = Field(default=Decimal("1.0"), ge=0, le=Decimal("100"))
    account_severe_holding_loss_pct: Decimal = Field(default=Decimal("7.0"), ge=0, le=Decimal("100"))
    account_severe_holding_loss_abs_krw: Decimal = Field(default=Decimal("0"), ge=0)
    vol_corr_sizing_enabled: bool = False
    vol_corr_volatility_lookback_days: int = Field(default=20, ge=2, le=252)
    vol_corr_min_return_observations: int = Field(default=10, ge=2, le=252)
    vol_corr_high_volatility_threshold_pct: Decimal = Field(default=Decimal("4.0"), gt=0, le=Decimal("100"))
    vol_corr_very_high_volatility_threshold_pct: Decimal = Field(default=Decimal("7.0"), gt=0, le=Decimal("100"))
    vol_corr_high_volatility_multiplier: Decimal = Field(default=Decimal("0.75"), gt=0, le=1)
    vol_corr_very_high_volatility_multiplier: Decimal = Field(default=Decimal("0.50"), gt=0, le=1)
    vol_corr_high_correlation_threshold: Decimal = Field(default=Decimal("0.75"), gt=0, le=1)
    vol_corr_high_correlation_multiplier: Decimal = Field(default=Decimal("0.80"), gt=0, le=1)
    vol_corr_missing_data_behavior: str = "neutral"
    vol_corr_missing_data_multiplier: Decimal = Field(default=Decimal("0.75"), gt=0, le=1)
    market_regime_guard_enabled: bool = True
    market_regime_caution_up_ratio_threshold: Decimal = Field(default=Decimal("0.45"), ge=0, le=1)
    market_regime_normal_up_ratio_threshold: Decimal = Field(default=Decimal("0.50"), ge=0, le=1)
    market_regime_risk_off_up_ratio_threshold: Decimal = Field(default=Decimal("0.35"), ge=0, le=1)
    market_regime_caution_advance_decline_threshold: Decimal = Field(default=Decimal("1.00"), ge=0)
    market_regime_risk_off_advance_decline_threshold: Decimal = Field(default=Decimal("0.70"), ge=0)
    market_regime_breadth_max_age_seconds: int = Field(default=86400, ge=60, le=604800)
    market_regime_missing_breadth_behavior: str = "neutral_with_warning"
    market_regime_live_missing_breadth_behavior: str = "caution"
    market_regime_stale_breadth_behavior: str = "caution"
    market_regime_risk_off_sizing_multiplier: Decimal = Field(default=Decimal("0.15"), gt=0, le=1)
    market_regime_risk_off_max_notional_krw: Decimal = Field(default=Decimal("500000"), ge=0)
    market_regime_caution_sizing_multiplier: Decimal = Field(default=Decimal("0.33"), gt=0, le=1)
    market_regime_caution_max_notional_krw: Decimal = Field(default=Decimal("1000000"), ge=0)
    market_regime_sector_weak_behavior: str = "downsize"
    market_regime_sector_weak_multiplier: Decimal = Field(default=Decimal("0.33"), ge=0, le=1)
    market_regime_sector_weak_max_notional_krw: Decimal = Field(default=Decimal("750000"), ge=0)
    market_regime_sector_weak_percentile_threshold: Decimal = Field(default=Decimal("0.30"), ge=0, le=1)
    market_regime_sector_weak_relative_return_pct: Decimal = Field(default=Decimal("-2.0"), ge=Decimal("-100"), le=Decimal("100"))
    market_context_enabled: bool = False
    market_context_base_url: str | None = None
    market_context_bearer_token: str | None = None
    market_context_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    market_context_cache_ttl_seconds: int = Field(default=60, ge=1, le=3600)
    trade_outcome_feedback_enabled: bool = True
    trade_outcome_feedback_dir: str = "var/analytics/trade_outcome_feedback"
    trade_outcome_feedback_lookback_days: int = Field(default=20, ge=1, le=365)
    trade_outcome_feedback_max_lessons: int = Field(default=6, ge=1, le=25)
    weekly_trade_review_dir: str = "var/analytics/weekly_trade_review"
    openclaw_cli_path: str = "openclaw"
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
    upbit_access_key: str | None = None
    upbit_secret_key: str | None = None
    upbit_base_url: str = "https://api.upbit.com"
    upbit_live_trading_enabled: bool = False
    upbit_request_timeout_seconds: int = Field(default=10, ge=1, le=120)
    domestic_order_session_routing_enabled: bool = False
    allow_krx_pre_open_offhours_orders: bool = False
    allow_krx_after_hours_closing_orders: bool = False
    allow_krx_after_hours_single_price_orders: bool = False
    allow_nxt_pre_orders: bool = False
    allow_nxt_main_orders: bool = False
    allow_nxt_after_orders: bool = False
    nxt_order_route_preference: str = "krx_when_regular"
    allow_after_hours_buy_orders: bool = False
    allow_after_hours_sell_orders: bool = True
    nxt_eligible_symbols: str = ""
    nxt_eligible_sync_enabled: bool = True
    nxt_eligible_source_url: str = "https://www.nextrade.co.kr/brdinfoTime/brdinfoTimeList.do?pageIndex=1&pageUnit=1000"
    nxt_eligible_source_timeout_seconds: int = Field(default=5, ge=1, le=60)
    nxt_eligible_source_ttl_seconds: int = Field(default=3600, ge=60, le=86400)

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return normalized

    @field_validator("vol_corr_missing_data_behavior")
    @classmethod
    def normalize_vol_corr_missing_data_behavior(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"neutral", "conservative"}
        if normalized not in allowed:
            raise ValueError(f"vol_corr_missing_data_behavior must be one of {sorted(allowed)}")
        return normalized

    @field_validator(
        "market_regime_missing_breadth_behavior",
        "market_regime_live_missing_breadth_behavior",
        "market_regime_stale_breadth_behavior",
    )
    @classmethod
    def normalize_market_regime_missing_behavior(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"neutral_with_warning", "caution", "risk_off"}
        if normalized not in allowed:
            raise ValueError(f"market regime missing/stale behavior must be one of {sorted(allowed)}")
        return normalized

    @field_validator("market_regime_sector_weak_behavior")
    @classmethod
    def normalize_market_regime_sector_weak_behavior(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"downsize", "reject"}
        if normalized not in allowed:
            raise ValueError(f"market_regime_sector_weak_behavior must be one of {sorted(allowed)}")
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
    "MONITORING_CYCLE_TIMEOUT_SECONDS": "monitoring_cycle_timeout_seconds",
    "MONITORING_EXPIRE_PRIOR_MARKET_WATCH_BUYS_ENABLED": "monitoring_expire_prior_market_watch_buys_enabled",
    "MONITORING_MAX_SYMBOLS_PER_CYCLE": "monitoring_max_symbols_per_cycle",
    "MONITORING_MAX_BROKER_ACTIONS_PER_CYCLE": "monitoring_max_broker_actions_per_cycle",
    "MONITORING_MAX_QUOTE_AGE_SECONDS": "monitoring_max_quote_age_seconds",
    "MONITORING_WATCH_BUY_RETRY_COOLDOWN_SECONDS": "monitoring_watch_buy_retry_cooldown_seconds",
    "STALE_BROKER_ORDER_RECONCILIATION_THRESHOLD_SECONDS": "stale_broker_order_reconciliation_threshold_seconds",
    "WATCH_BUY_HIGH_RISK_GATE_ENABLED": "watch_buy_high_risk_gate_enabled",
    "WATCH_BUY_HIGH_ATR_PCT_THRESHOLD": "watch_buy_high_atr_pct_threshold",
    "WATCH_BUY_WEAK_RET20_THRESHOLD_PCT": "watch_buy_weak_ret20_threshold_pct",
    "WATCH_BUY_DEEP_NEGATIVE_RET20_THRESHOLD_PCT": "watch_buy_deep_negative_ret20_threshold_pct",
    "WATCH_BUY_EXTENDED_RET5_THRESHOLD_PCT": "watch_buy_extended_ret5_threshold_pct",
    "WATCH_BUY_MAX_INTRADAY_RANGE_POSITION_PCT": "watch_buy_max_intraday_range_position_pct",
    "WATCH_BUY_STALE_TRIGGER_RECLAIM_THRESHOLD_PCT": "watch_buy_stale_trigger_reclaim_threshold_pct",
    "PROFIT_PROTECTION_MIN_PEAK_PCT": "profit_protection_min_peak_pct",
    "PROFIT_PROTECTION_GIVEBACK_PCT": "profit_protection_giveback_pct",
    "PROFIT_PROTECTION_AGED_SESSION_MIN": "profit_protection_aged_session_min",
    "PROFIT_PROTECTION_AGED_MIN_CURRENT_PROFIT_PCT": "profit_protection_aged_min_current_profit_pct",
    "BREAKEVEN_STOP_BUFFER_PCT": "breakeven_stop_buffer_pct",
    "MARKET_TIMEZONE": "market_timezone",
    "EXEC_AGENT_ENABLED": "exec_agent_enabled",
    "EXEC_AGENT_ID": "exec_agent_id",
    "EXEC_AGENT_MODEL": "exec_agent_model",
    "EXEC_AGENT_MAX_SYMBOL_BUDGET": "exec_agent_max_symbol_budget",
    "EXEC_AGENT_MAX_TOTAL_BUDGET": "exec_agent_max_total_budget",
    "EXEC_AGENT_BASE_RISK_PCT": "exec_agent_base_risk_pct",
    "EXEC_AGENT_AGGRESSIVE_RISK_PCT": "exec_agent_aggressive_risk_pct",
    "EXEC_AGENT_AGGRESSIVE_CONFIDENCE_THRESHOLD": "exec_agent_aggressive_confidence_threshold",
    "EXEC_AGENT_MAX_POSITION_ALLOCATION_PCT": "exec_agent_max_position_allocation_pct",
    "EXEC_AGENT_MAX_SYMBOL_EXPOSURE_PCT": "exec_agent_max_symbol_exposure_pct",
    "EXEC_AGENT_MAX_SYMBOL_TOTAL_EXPOSURE_PCT": "exec_agent_max_symbol_total_exposure_pct",
    "EXEC_AGENT_MAX_DAILY_NEW_BUY_NOTIONAL": "exec_agent_max_daily_new_buy_notional",
    "EXEC_AGENT_MAX_TRADE_RISK_PCT": "exec_agent_max_trade_risk_pct",
    "EXEC_AGENT_STOP_LOSS_PCT": "exec_agent_stop_loss_pct",
    "EXEC_AGENT_MAX_ENTRY_PREMIUM_PCT": "exec_agent_max_entry_premium_pct",
    "EXEC_AGENT_MIN_REWARD_RISK": "exec_agent_min_reward_risk",
    "EXEC_AGENT_RELAXED_MIN_REWARD_RISK": "exec_agent_relaxed_min_reward_risk",
    "EXEC_AGENT_ABSOLUTE_MIN_REWARD_RISK": "exec_agent_absolute_min_reward_risk",
    "EXEC_AGENT_TACTICAL_SCALP_MIN_TARGET_PCT": "exec_agent_tactical_scalp_min_target_pct",
    "EXEC_AGENT_TACTICAL_SCALP_MAX_STOP_PCT": "exec_agent_tactical_scalp_max_stop_pct",
    "EXEC_AGENT_MAX_ACCOUNT_SNAPSHOT_AGE_SECONDS": "exec_agent_max_account_snapshot_age_seconds",
    "EXEC_AGENT_MAX_HOLDING_TRADING_SESSIONS": "exec_agent_max_holding_trading_sessions",
    "EXEC_AGENT_FIRST_TAKE_PROFIT_PCT": "exec_agent_first_take_profit_pct",
    "EXEC_AGENT_FINAL_TAKE_PROFIT_PCT": "exec_agent_final_take_profit_pct",
    "EXEC_AGENT_PARTIAL_TAKE_PROFIT_FRACTION": "exec_agent_partial_take_profit_fraction",
    "EXEC_AGENT_TRIGGER_TIME_ENABLED": "exec_agent_trigger_time_enabled",
    "EXEC_AGENT_TRIGGER_TIME_SELL_ENABLED": "exec_agent_trigger_time_sell_enabled",
    "EXEC_AGENT_TRIGGER_TIME_TIMEOUT_SECONDS": "exec_agent_trigger_time_timeout_seconds",
    "MONTHLY_TARGET_KRW": "monthly_target_krw",
    "MONTHLY_CAPITAL_PRESERVATION_ENABLED": "monthly_capital_preservation_enabled",
    "MONTHLY_AGGRESSIVE_LAG_RATIO": "monthly_aggressive_lag_ratio",
    "EXEC_AGENT_WATCH_SIZING_ENABLED": "exec_agent_watch_sizing_enabled",
    "EXEC_AGENT_WATCH_SIZING_LOOP_INTERVAL_SECONDS": "exec_agent_watch_sizing_loop_interval_seconds",
    "EXEC_AGENT_WATCH_SIZING_BATCH_SIZE": "exec_agent_watch_sizing_batch_size",
    "EXEC_AGENT_WATCH_SIZING_MAX_BATCHES_PER_CYCLE": "exec_agent_watch_sizing_max_batches_per_cycle",
    "EXEC_AGENT_WATCH_SIZING_MAX_ATTEMPTS": "exec_agent_watch_sizing_max_attempts",
    "EXEC_AGENT_WATCH_SIZING_START_LOCAL_TIME": "exec_agent_watch_sizing_start_local_time",
    "EXEC_AGENT_WATCH_SIZING_END_LOCAL_TIME": "exec_agent_watch_sizing_end_local_time",
    "EXEC_AGENT_WATCH_SIZING_TIMEOUT_SECONDS": "exec_agent_watch_sizing_timeout_seconds",
    "EXEC_AGENT_WATCH_SIZING_MIN_NOTIONAL_KRW": "exec_agent_watch_sizing_min_notional_krw",
    "EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_MIN_NOTIONAL_KRW": "exec_agent_watch_sizing_high_conviction_min_notional_krw",
    "EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_THRESHOLD": "exec_agent_watch_sizing_high_conviction_threshold",
    "EXEC_AGENT_WATCH_SIZING_FALLBACK_ACCOUNT_BASIS_KRW": "exec_agent_watch_sizing_fallback_account_basis_krw",
    "ACCOUNT_DRAWDOWN_GATE_ENABLED": "account_drawdown_gate_enabled",
    "ACCOUNT_DAILY_TOTAL_ASSET_DRAWDOWN_PCT": "account_daily_total_asset_drawdown_pct",
    "ACCOUNT_DAILY_TOTAL_ASSET_DRAWDOWN_KRW": "account_daily_total_asset_drawdown_krw",
    "ACCOUNT_WEEKLY_TOTAL_ASSET_DRAWDOWN_PCT": "account_weekly_total_asset_drawdown_pct",
    "ACCOUNT_WEEKLY_TOTAL_ASSET_BASELINE_KRW": "account_weekly_total_asset_baseline_krw",
    "ACCOUNT_WEEKLY_TOTAL_ASSET_BASELINE_AT": "account_weekly_total_asset_baseline_at",
    "ACCOUNT_UNREALIZED_LOSS_ABS_KRW": "account_unrealized_loss_abs_krw",
    "ACCOUNT_UNREALIZED_LOSS_PCT": "account_unrealized_loss_pct",
    "ACCOUNT_SEVERE_HOLDING_LOSS_PCT": "account_severe_holding_loss_pct",
    "ACCOUNT_SEVERE_HOLDING_LOSS_ABS_KRW": "account_severe_holding_loss_abs_krw",
    "VOL_CORR_SIZING_ENABLED": "vol_corr_sizing_enabled",
    "VOL_CORR_VOLATILITY_LOOKBACK_DAYS": "vol_corr_volatility_lookback_days",
    "VOL_CORR_MIN_RETURN_OBSERVATIONS": "vol_corr_min_return_observations",
    "VOL_CORR_HIGH_VOLATILITY_THRESHOLD_PCT": "vol_corr_high_volatility_threshold_pct",
    "VOL_CORR_VERY_HIGH_VOLATILITY_THRESHOLD_PCT": "vol_corr_very_high_volatility_threshold_pct",
    "VOL_CORR_HIGH_VOLATILITY_MULTIPLIER": "vol_corr_high_volatility_multiplier",
    "VOL_CORR_VERY_HIGH_VOLATILITY_MULTIPLIER": "vol_corr_very_high_volatility_multiplier",
    "VOL_CORR_HIGH_CORRELATION_THRESHOLD": "vol_corr_high_correlation_threshold",
    "VOL_CORR_HIGH_CORRELATION_MULTIPLIER": "vol_corr_high_correlation_multiplier",
    "VOL_CORR_MISSING_DATA_BEHAVIOR": "vol_corr_missing_data_behavior",
    "VOL_CORR_MISSING_DATA_MULTIPLIER": "vol_corr_missing_data_multiplier",
    "MARKET_REGIME_GUARD_ENABLED": "market_regime_guard_enabled",
    "MARKET_REGIME_CAUTION_UP_RATIO_THRESHOLD": "market_regime_caution_up_ratio_threshold",
    "MARKET_REGIME_NORMAL_UP_RATIO_THRESHOLD": "market_regime_normal_up_ratio_threshold",
    "MARKET_REGIME_RISK_OFF_UP_RATIO_THRESHOLD": "market_regime_risk_off_up_ratio_threshold",
    "MARKET_REGIME_CAUTION_ADVANCE_DECLINE_THRESHOLD": "market_regime_caution_advance_decline_threshold",
    "MARKET_REGIME_RISK_OFF_ADVANCE_DECLINE_THRESHOLD": "market_regime_risk_off_advance_decline_threshold",
    "MARKET_REGIME_BREADTH_MAX_AGE_SECONDS": "market_regime_breadth_max_age_seconds",
    "MARKET_REGIME_MISSING_BREADTH_BEHAVIOR": "market_regime_missing_breadth_behavior",
    "MARKET_REGIME_LIVE_MISSING_BREADTH_BEHAVIOR": "market_regime_live_missing_breadth_behavior",
    "MARKET_REGIME_STALE_BREADTH_BEHAVIOR": "market_regime_stale_breadth_behavior",
    "MARKET_REGIME_RISK_OFF_SIZING_MULTIPLIER": "market_regime_risk_off_sizing_multiplier",
    "MARKET_REGIME_RISK_OFF_MAX_NOTIONAL_KRW": "market_regime_risk_off_max_notional_krw",
    "MARKET_REGIME_CAUTION_SIZING_MULTIPLIER": "market_regime_caution_sizing_multiplier",
    "MARKET_REGIME_CAUTION_MAX_NOTIONAL_KRW": "market_regime_caution_max_notional_krw",
    "MARKET_REGIME_SECTOR_WEAK_BEHAVIOR": "market_regime_sector_weak_behavior",
    "MARKET_REGIME_SECTOR_WEAK_MULTIPLIER": "market_regime_sector_weak_multiplier",
    "MARKET_REGIME_SECTOR_WEAK_MAX_NOTIONAL_KRW": "market_regime_sector_weak_max_notional_krw",
    "MARKET_REGIME_SECTOR_WEAK_PERCENTILE_THRESHOLD": "market_regime_sector_weak_percentile_threshold",
    "MARKET_REGIME_SECTOR_WEAK_RELATIVE_RETURN_PCT": "market_regime_sector_weak_relative_return_pct",
    "MARKET_CONTEXT_ENABLED": "market_context_enabled",
    "MARKET_CONTEXT_BASE_URL": "market_context_base_url",
    "MARKET_CONTEXT_BEARER_TOKEN": "market_context_bearer_token",
    "MARKET_CONTEXT_TIMEOUT_SECONDS": "market_context_timeout_seconds",
    "MARKET_CONTEXT_CACHE_TTL_SECONDS": "market_context_cache_ttl_seconds",
    "TRADE_OUTCOME_FEEDBACK_ENABLED": "trade_outcome_feedback_enabled",
    "TRADE_OUTCOME_FEEDBACK_DIR": "trade_outcome_feedback_dir",
    "TRADE_OUTCOME_FEEDBACK_LOOKBACK_DAYS": "trade_outcome_feedback_lookback_days",
    "TRADE_OUTCOME_FEEDBACK_MAX_LESSONS": "trade_outcome_feedback_max_lessons",
    "WEEKLY_TRADE_REVIEW_DIR": "weekly_trade_review_dir",
    "OPENCLAW_CLI_PATH": "openclaw_cli_path",
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
    "UPBIT_ACCESS_KEY": "upbit_access_key",
    "UPBIT_SECRET_KEY": "upbit_secret_key",
    "UPBIT_BASE_URL": "upbit_base_url",
    "UPBIT_LIVE_TRADING_ENABLED": "upbit_live_trading_enabled",
    "UPBIT_REQUEST_TIMEOUT_SECONDS": "upbit_request_timeout_seconds",
    "DOMESTIC_ORDER_SESSION_ROUTING_ENABLED": "domestic_order_session_routing_enabled",
    "ALLOW_KRX_PRE_OPEN_OFFHOURS_ORDERS": "allow_krx_pre_open_offhours_orders",
    "ALLOW_KRX_AFTER_HOURS_CLOSING_ORDERS": "allow_krx_after_hours_closing_orders",
    "ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS": "allow_krx_after_hours_single_price_orders",
    "ALLOW_NXT_PRE_ORDERS": "allow_nxt_pre_orders",
    "ALLOW_NXT_MAIN_ORDERS": "allow_nxt_main_orders",
    "ALLOW_NXT_AFTER_ORDERS": "allow_nxt_after_orders",
    "NXT_ORDER_ROUTE_PREFERENCE": "nxt_order_route_preference",
    "ALLOW_AFTER_HOURS_BUY_ORDERS": "allow_after_hours_buy_orders",
    "ALLOW_AFTER_HOURS_SELL_ORDERS": "allow_after_hours_sell_orders",
    "NXT_ELIGIBLE_SYMBOLS": "nxt_eligible_symbols",
    "NXT_ELIGIBLE_SYNC_ENABLED": "nxt_eligible_sync_enabled",
    "NXT_ELIGIBLE_SOURCE_URL": "nxt_eligible_source_url",
    "NXT_ELIGIBLE_SOURCE_TIMEOUT_SECONDS": "nxt_eligible_source_timeout_seconds",
    "NXT_ELIGIBLE_SOURCE_TTL_SECONDS": "nxt_eligible_source_ttl_seconds",
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
