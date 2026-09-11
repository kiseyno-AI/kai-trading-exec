import os
from datetime import time as wall_time
from decimal import Decimal
from functools import lru_cache
from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

RuntimeMode = Literal["mock", "paper", "live"]
RuntimeBrokerProvider = Literal["kis", "alpaca", "upbit"]
LogFormat = Literal["json", "plain"]


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    service_name: str = "kai-trading-exec"
    environment: str = "local"
    runtime_mode: RuntimeMode = "mock"
    runtime_broker_provider: RuntimeBrokerProvider = "kis"
    database_url: str = Field(
        default="sqlite:///./var/kai_trading_exec.db",
        repr=False,
    )
    db_pool_size: int = Field(default=10, ge=1, le=200)
    db_max_overflow: int = Field(default=10, ge=0, le=200)
    db_pool_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    db_pool_recycle_seconds: int = Field(default=900, ge=0, le=86_400)
    db_pool_use_lifo: bool = True
    db_connect_timeout_seconds: int = Field(default=5, ge=1, le=120)
    db_long_checkout_warning_seconds: float = Field(default=10.0, gt=0, le=3600)
    sqlite_busy_timeout_seconds: int = Field(default=30, ge=1, le=300)
    sqlite_wal_autocheckpoint_pages: int = Field(default=0, ge=0, le=1_000_000)
    log_level: str = "INFO"
    log_format: LogFormat = "json"
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8010, ge=1, le=65535)
    api_bearer_token: str | None = Field(default=None, repr=False)
    automation_enabled: bool = False
    autonomous_runtime_enabled: bool = False
    dispatch_loop_interval_seconds: float = Field(default=1.0, gt=0, le=3600)
    monitoring_loop_interval_seconds: float = Field(default=1.0, gt=0, le=3600)
    monitoring_cycle_timeout_seconds: float = Field(default=20.0, gt=0, le=3600)
    monitoring_expire_prior_market_watch_buys_enabled: bool = True
    monitoring_watch_buy_max_age_trading_sessions: int = Field(default=2, ge=1, le=10)
    monitoring_max_symbols_per_cycle: int = Field(default=128, ge=0, le=10000)
    monitoring_auto_symbol_batch_target_seconds: float = Field(default=12.0, gt=0, le=300)
    monitoring_max_broker_actions_per_cycle: int = Field(default=32, ge=0, le=10000)
    monitoring_max_quote_age_seconds: int = Field(default=30, ge=1, le=3600)
    monitoring_max_quote_future_skew_seconds: int = Field(default=5, ge=0, le=300)
    next_open_buy_initial_submission_grace_seconds: int = Field(
        default=300,
        ge=0,
        le=3600,
    )
    krx_market_holiday_dates: str = ""
    monitoring_watch_buy_retry_cooldown_seconds: int = Field(default=5, ge=1, le=3600)
    monitoring_exit_on_stale_cycle_enabled: bool = False
    monitoring_stale_cycle_exit_grace_multiplier: float = Field(default=2.0, ge=1.0, le=20.0)
    stale_broker_order_reconciliation_threshold_seconds: int = Field(default=86400, ge=60, le=31536000)
    watch_buy_high_risk_gate_enabled: bool = True
    watch_buy_high_atr_pct_threshold: Decimal = Field(default=Decimal("9.0"), ge=0, le=Decimal("100"))
    watch_buy_weak_ret20_threshold_pct: Decimal = Field(default=Decimal("0.0"), ge=Decimal("-100"), le=Decimal("100"))
    watch_buy_deep_negative_ret20_threshold_pct: Decimal = Field(default=Decimal("-10.0"), ge=Decimal("-100"), le=Decimal("0"))
    watch_buy_extended_ret5_threshold_pct: Decimal = Field(default=Decimal("15.0"), ge=0, le=Decimal("100"))
    watch_buy_high_volatility_chase_veto_enabled: bool = True
    watch_buy_high_volatility_chase_atr_pct_threshold: Decimal = Field(default=Decimal("15.0"), ge=0, le=Decimal("100"))
    watch_buy_high_volatility_chase_ret5_threshold_pct: Decimal = Field(default=Decimal("30.0"), ge=0, le=Decimal("100"))
    watch_buy_high_volatility_chase_max_context_age_days: int = Field(default=1, ge=0, le=30)
    watch_buy_max_intraday_range_position_pct: Decimal = Field(default=Decimal("75.0"), ge=0, le=Decimal("100"))
    watch_buy_stale_trigger_reclaim_threshold_pct: Decimal = Field(default=Decimal("3.0"), ge=0, le=Decimal("100"))
    watch_buy_late_entry_mode: str = "shadow"
    watch_buy_late_entry_cutoff_krx: str = "10:30"
    watch_buy_live_market_quality_veto_enabled: bool = True
    watch_buy_live_market_regime_downsize_allowed: bool = False
    watch_buy_live_min_trigger_turnover_krw: Decimal = Field(default=Decimal("100000000"), ge=0)
    profit_protection_min_peak_pct: Decimal = Field(default=Decimal("3.0"), gt=0, le=Decimal("100"))
    profit_protection_giveback_pct: Decimal = Field(default=Decimal("1.5"), gt=0, le=Decimal("100"))
    profit_protection_aged_session_min: int = Field(default=2, ge=1, le=20)
    profit_protection_aged_min_current_profit_pct: Decimal = Field(default=Decimal("1.0"), ge=0, le=Decimal("100"))
    breakeven_stop_buffer_pct: Decimal = Field(default=Decimal("0.2"), ge=0, le=Decimal("5"))
    market_timezone: str = "Asia/Seoul"
    exec_agent_enabled: bool = False
    exec_agent_id: str = "exec-agent"
    exec_agent_model: str = "openai/gpt-5.6-sol"
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
    kis_krx_round_trip_non_spread_cost_bps: Decimal = Field(
        default=Decimal("50"),
        ge=0,
    )
    kis_us_round_trip_non_spread_cost_bps: Decimal | None = Field(
        default=None,
        ge=0,
    )
    alpaca_us_equity_round_trip_non_spread_cost_bps: Decimal = Field(
        default=Decimal("0"),
        ge=0,
    )
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
    account_drawdown_recovery_enabled: bool = True
    account_drawdown_recovery_threshold_ratio: Decimal = Field(
        default=Decimal("0.80"),
        gt=0,
        le=1,
    )
    account_drawdown_recovery_required_healthy_observations: int = Field(
        default=2,
        ge=2,
        le=24,
    )
    account_drawdown_recovery_cooldown_seconds: int = Field(
        default=300,
        ge=0,
        le=86400,
    )
    account_drawdown_recovery_min_observation_interval_seconds: int = Field(
        default=60,
        ge=1,
        le=3600,
    )
    account_drawdown_recovery_max_snapshot_future_skew_seconds: int = Field(
        default=30,
        ge=0,
        le=300,
    )
    account_drawdown_recovery_scope_secret: str | None = Field(
        default=None,
        repr=False,
    )
    execution_position_account_scope: str | None = Field(default=None, repr=False)
    hourly_plan_judgment_recovery_required_healthy_observations: int = Field(
        default=2,
        ge=2,
        le=24,
    )
    hourly_plan_judgment_recovery_cooldown_seconds: int = Field(
        default=900,
        ge=0,
        le=86400,
    )
    hourly_plan_judgment_recovery_min_observation_interval_seconds: int = Field(
        default=300,
        ge=1,
        le=3600,
    )
    core_buy_authority_max_observation_age_seconds: int = Field(
        default=420,
        ge=60,
        le=3600,
    )
    core_buy_authority_max_future_skew_seconds: int = Field(
        default=5,
        ge=0,
        le=300,
    )
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
    market_context_bearer_token: str | None = Field(default=None, repr=False)
    market_context_timeout_seconds: float = Field(default=5.0, gt=0, le=120)
    market_context_cache_ttl_seconds: int = Field(default=60, ge=1, le=3600)
    trade_outcome_feedback_enabled: bool = True
    trade_outcome_feedback_dir: str = "var/analytics/trade_outcome_feedback"
    trade_outcome_feedback_lookback_days: int = Field(default=20, ge=1, le=365)
    trade_outcome_feedback_max_lessons: int = Field(default=6, ge=1, le=25)
    trade_outcome_feedback_max_age_seconds: int = Field(
        default=172800,
        ge=60,
        le=31536000,
    )
    weekly_trade_review_dir: str = "var/analytics/weekly_trade_review"
    openclaw_cli_path: str = "openclaw"
    kis_app_key: str | None = Field(default=None, repr=False)
    kis_app_secret: str | None = Field(default=None, repr=False)
    kis_account_number: str | None = Field(default=None, repr=False)
    kis_account_product_code: str = "01"
    kis_hts_id: str | None = Field(default=None, repr=False)
    kis_base_url: str = "https://openapi.koreainvestment.com:9443"
    kis_mock_base_url: str = "https://openapivts.koreainvestment.com:29443"
    kis_customer_type: str = "P"
    kis_use_hashkey: bool = True
    kis_request_timeout_seconds: int = Field(default=10, ge=1, le=120)
    kis_min_request_interval_seconds: float = Field(default=0.12, ge=0, le=10)
    kis_token_refresh_margin_seconds: int = Field(default=300, ge=0, le=172_799)
    kis_token_failure_cooldown_seconds: int = Field(default=60, ge=60, le=3600)
    kis_token_request_timeout_seconds: int = Field(default=15, ge=1, le=120)
    kis_token_issuance_lease_seconds: int = Field(default=90, ge=75, le=3600)
    kis_token_min_issue_interval_seconds: int = Field(default=60, ge=60, le=3600)
    kis_token_cache_directory: str = "~/.cache/kai-trading/kis-oauth-tokens"
    kis_us_trading_enabled: bool = False
    kis_us_live_trading_enabled: bool = False
    kis_us_live_confirmation_token: str | None = Field(default=None, repr=False)
    kis_us_account_allowlist: str = Field(default="", repr=False)
    kis_us_max_order_notional_usd: Decimal = Field(default=Decimal("100"), gt=0)
    kis_us_max_total_exposure_usd: Decimal = Field(default=Decimal("1000"), gt=0)
    kis_us_daily_realized_loss_limit_usd: Decimal = Field(default=Decimal("25"), gt=0)
    kis_us_weekly_realized_loss_limit_usd: Decimal = Field(default=Decimal("50"), gt=0)
    kis_us_unrealized_loss_limit_usd: Decimal = Field(default=Decimal("50"), gt=0)
    kis_us_unrealized_loss_pct: Decimal = Field(default=Decimal("5"), gt=0, le=Decimal("100"))
    kis_us_severe_holding_loss_pct: Decimal = Field(default=Decimal("10"), gt=0, le=Decimal("100"))
    kis_us_allow_degraded_realized_pnl_coverage: bool = False
    kis_us_preflight_buying_power_enabled: bool = True
    upbit_access_key: str | None = Field(default=None, repr=False)
    upbit_secret_key: str | None = Field(default=None, repr=False)
    upbit_base_url: str = "https://api.upbit.com"
    upbit_live_trading_enabled: bool = False
    upbit_request_timeout_seconds: int = Field(default=10, ge=1, le=120)
    alpaca_api_key: str | None = Field(default=None, repr=False)
    alpaca_api_secret: str | None = Field(default=None, repr=False)
    alpaca_live_base_url: str = "https://api.alpaca.markets"
    alpaca_paper_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_data_base_url: str = "https://data.alpaca.markets"
    alpaca_live_trading_enabled: bool = False
    alpaca_request_timeout_seconds: int = Field(default=10, ge=1, le=120)
    alpaca_crypto_monitoring_enabled: bool = False
    alpaca_crypto_monitoring_interval_seconds: float = Field(default=15.0, ge=3.0, le=3600)
    alpaca_crypto_reconcile_interval_seconds: float = Field(default=30.0, ge=5.0, le=3600)
    alpaca_crypto_allowed_symbols: str = "BTC/USD,ETH/USD"
    alpaca_crypto_max_order_notional_usd: Decimal = Field(default=Decimal("1000"), gt=0)
    alpaca_crypto_strategy_enabled: bool = False
    alpaca_crypto_strategy_live_enabled: bool = False
    alpaca_crypto_strategy_version: str = "crypto_trend_v1"
    alpaca_crypto_strategy_decision_hour_utc: int = Field(default=0, ge=0, le=23)
    alpaca_crypto_strategy_decision_minute_utc: int = Field(default=10, ge=0, le=59)
    alpaca_crypto_strategy_history_days: int = Field(default=500, ge=260, le=3650)
    alpaca_crypto_strategy_sleeve_usd: Decimal = Field(default=Decimal("1000"), gt=0)
    alpaca_crypto_strategy_max_invested_fraction: Decimal = Field(
        default=Decimal("0.60"),
        gt=0,
        le=1,
    )
    alpaca_crypto_strategy_target_volatility: Decimal = Field(
        default=Decimal("0.10"),
        gt=0,
        le=1,
    )
    alpaca_crypto_strategy_max_position_risk_fraction: Decimal = Field(
        default=Decimal("0.0025"),
        gt=0,
        le=Decimal("0.02"),
    )
    alpaca_crypto_strategy_min_order_notional_usd: Decimal = Field(default=Decimal("25"), gt=0)
    alpaca_crypto_strategy_min_rebalance_fraction: Decimal = Field(
        default=Decimal("0.05"),
        ge=0,
        le=1,
    )
    alpaca_crypto_strategy_quote_max_age_seconds: int = Field(default=60, ge=5, le=3600)
    alpaca_crypto_strategy_annualization_days: Decimal = Field(
        default=Decimal("365"),
        gt=0,
        le=Decimal("366"),
    )
    alpaca_crypto_strategy_max_closed_bar_age_days: int = Field(
        default=1,
        ge=1,
        le=7,
    )
    alpaca_crypto_strategy_max_spread_bps: Decimal = Field(default=Decimal("100"), gt=0)
    alpaca_crypto_strategy_stop_atr_multiple: Decimal = Field(default=Decimal("3"), gt=0, le=10)
    alpaca_crypto_strategy_max_stop_distance_fraction: Decimal = Field(
        default=Decimal("0.12"),
        gt=0,
        le=Decimal("0.50"),
    )
    alpaca_crypto_strategy_daily_drawdown_fraction: Decimal = Field(
        default=Decimal("0.01"),
        gt=0,
        le=Decimal("0.50"),
    )
    alpaca_crypto_strategy_weekly_drawdown_fraction: Decimal = Field(
        default=Decimal("0.03"),
        gt=0,
        le=Decimal("0.50"),
    )
    alpaca_crypto_strategy_peak_drawdown_fraction: Decimal = Field(
        default=Decimal("0.08"),
        gt=0,
        le=Decimal("0.90"),
    )
    alpaca_us_equity_monitoring_enabled: bool = False
    alpaca_us_equity_monitoring_interval_seconds: float = Field(default=15.0, ge=3.0, le=3600)
    alpaca_us_equity_reconcile_interval_seconds: float = Field(default=30.0, ge=5.0, le=3600)
    alpaca_us_equity_allowed_symbols: str = "SPY,QQQ"
    alpaca_us_equity_max_order_notional_usd: Decimal = Field(default=Decimal("100"), gt=0)
    alpaca_us_equity_strategy_enabled: bool = False
    alpaca_us_equity_strategy_live_enabled: bool = False
    alpaca_us_equity_strategy_version: str = "us_equity_trend_v1"
    alpaca_us_equity_strategy_sleeve_usd: Decimal = Field(default=Decimal("1000"), gt=0)
    alpaca_us_equity_strategy_annualization_days: Decimal = Field(
        default=Decimal("252"),
        gt=0,
        le=Decimal("366"),
    )
    alpaca_us_equity_strategy_max_closed_bar_age_days: int = Field(
        default=4,
        ge=1,
        le=10,
    )
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

    @model_validator(mode="after")
    def validate_kis_token_issuance_timing(self) -> "Settings":
        required_lease_seconds = (
            max(
                self.kis_token_request_timeout_seconds,
                self.kis_request_timeout_seconds,
            )
            + self.kis_token_min_issue_interval_seconds
        )
        if self.kis_token_issuance_lease_seconds < required_lease_seconds:
            raise ValueError(
                "KIS_TOKEN_ISSUANCE_LEASE_SECONDS must be at least "
                "the larger of KIS_TOKEN_REQUEST_TIMEOUT_SECONDS and "
                "KIS_REQUEST_TIMEOUT_SECONDS, plus "
                "KIS_TOKEN_MIN_ISSUE_INTERVAL_SECONDS"
            )
        return self

    @model_validator(mode="after")
    def validate_autonomous_runtime_provider(self) -> "Settings":
        if (
            self.autonomous_runtime_enabled
            and self.runtime_broker_provider != "kis"
        ):
            raise ValueError(
                "autonomous runtime currently supports only the KIS provider"
            )
        return self

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        normalized = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return normalized

    @field_validator("runtime_broker_provider", mode="before")
    @classmethod
    def normalize_runtime_broker_provider(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

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

    @field_validator("watch_buy_late_entry_mode")
    @classmethod
    def normalize_watch_buy_late_entry_mode(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"disabled", "shadow", "block"}
        if normalized not in allowed:
            raise ValueError(f"watch_buy_late_entry_mode must be one of {sorted(allowed)}")
        return normalized

    @field_validator("watch_buy_late_entry_cutoff_krx")
    @classmethod
    def validate_watch_buy_late_entry_cutoff_krx(cls, value: str) -> str:
        normalized = value.strip()
        if len(normalized) != 5:
            raise ValueError("watch_buy_late_entry_cutoff_krx must use HH:MM")
        try:
            parsed = wall_time.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("watch_buy_late_entry_cutoff_krx must use HH:MM") from exc
        if parsed.second or parsed.microsecond:
            raise ValueError("watch_buy_late_entry_cutoff_krx must use HH:MM")
        return parsed.strftime("%H:%M")


_ENV_FIELDS = {
    "SERVICE_NAME": "service_name",
    "ENVIRONMENT": "environment",
    "RUNTIME_MODE": "runtime_mode",
    "RUNTIME_BROKER_PROVIDER": "runtime_broker_provider",
    "DATABASE_URL": "database_url",
    "DB_POOL_SIZE": "db_pool_size",
    "DB_MAX_OVERFLOW": "db_max_overflow",
    "DB_POOL_TIMEOUT_SECONDS": "db_pool_timeout_seconds",
    "DB_POOL_RECYCLE_SECONDS": "db_pool_recycle_seconds",
    "DB_POOL_USE_LIFO": "db_pool_use_lifo",
    "DB_CONNECT_TIMEOUT_SECONDS": "db_connect_timeout_seconds",
    "DB_LONG_CHECKOUT_WARNING_SECONDS": "db_long_checkout_warning_seconds",
    "SQLITE_BUSY_TIMEOUT_SECONDS": "sqlite_busy_timeout_seconds",
    "SQLITE_WAL_AUTOCHECKPOINT_PAGES": "sqlite_wal_autocheckpoint_pages",
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
    "MONITORING_WATCH_BUY_MAX_AGE_TRADING_SESSIONS": "monitoring_watch_buy_max_age_trading_sessions",
    "MONITORING_MAX_SYMBOLS_PER_CYCLE": "monitoring_max_symbols_per_cycle",
    "MONITORING_AUTO_SYMBOL_BATCH_TARGET_SECONDS": "monitoring_auto_symbol_batch_target_seconds",
    "MONITORING_MAX_BROKER_ACTIONS_PER_CYCLE": "monitoring_max_broker_actions_per_cycle",
    "MONITORING_MAX_QUOTE_AGE_SECONDS": "monitoring_max_quote_age_seconds",
    "MONITORING_MAX_QUOTE_FUTURE_SKEW_SECONDS": "monitoring_max_quote_future_skew_seconds",
    "NEXT_OPEN_BUY_INITIAL_SUBMISSION_GRACE_SECONDS": "next_open_buy_initial_submission_grace_seconds",
    "KRX_MARKET_HOLIDAY_DATES": "krx_market_holiday_dates",
    "MONITORING_WATCH_BUY_RETRY_COOLDOWN_SECONDS": "monitoring_watch_buy_retry_cooldown_seconds",
    "MONITORING_EXIT_ON_STALE_CYCLE_ENABLED": "monitoring_exit_on_stale_cycle_enabled",
    "MONITORING_STALE_CYCLE_EXIT_GRACE_MULTIPLIER": "monitoring_stale_cycle_exit_grace_multiplier",
    "STALE_BROKER_ORDER_RECONCILIATION_THRESHOLD_SECONDS": "stale_broker_order_reconciliation_threshold_seconds",
    "WATCH_BUY_HIGH_RISK_GATE_ENABLED": "watch_buy_high_risk_gate_enabled",
    "WATCH_BUY_HIGH_ATR_PCT_THRESHOLD": "watch_buy_high_atr_pct_threshold",
    "WATCH_BUY_WEAK_RET20_THRESHOLD_PCT": "watch_buy_weak_ret20_threshold_pct",
    "WATCH_BUY_DEEP_NEGATIVE_RET20_THRESHOLD_PCT": "watch_buy_deep_negative_ret20_threshold_pct",
    "WATCH_BUY_EXTENDED_RET5_THRESHOLD_PCT": "watch_buy_extended_ret5_threshold_pct",
    "WATCH_BUY_HIGH_VOLATILITY_CHASE_VETO_ENABLED": "watch_buy_high_volatility_chase_veto_enabled",
    "WATCH_BUY_HIGH_VOLATILITY_CHASE_ATR_PCT_THRESHOLD": "watch_buy_high_volatility_chase_atr_pct_threshold",
    "WATCH_BUY_HIGH_VOLATILITY_CHASE_RET5_THRESHOLD_PCT": "watch_buy_high_volatility_chase_ret5_threshold_pct",
    "WATCH_BUY_HIGH_VOLATILITY_CHASE_MAX_CONTEXT_AGE_DAYS": "watch_buy_high_volatility_chase_max_context_age_days",
    "WATCH_BUY_MAX_INTRADAY_RANGE_POSITION_PCT": "watch_buy_max_intraday_range_position_pct",
    "WATCH_BUY_STALE_TRIGGER_RECLAIM_THRESHOLD_PCT": "watch_buy_stale_trigger_reclaim_threshold_pct",
    "WATCH_BUY_LATE_ENTRY_MODE": "watch_buy_late_entry_mode",
    "WATCH_BUY_LATE_ENTRY_CUTOFF_KRX": "watch_buy_late_entry_cutoff_krx",
    "WATCH_BUY_LIVE_MARKET_QUALITY_VETO_ENABLED": "watch_buy_live_market_quality_veto_enabled",
    "WATCH_BUY_LIVE_MARKET_REGIME_DOWNSIZE_ALLOWED": "watch_buy_live_market_regime_downsize_allowed",
    "WATCH_BUY_LIVE_MIN_TRIGGER_TURNOVER_KRW": "watch_buy_live_min_trigger_turnover_krw",
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
    "KIS_KRX_ROUND_TRIP_NON_SPREAD_COST_BPS": "kis_krx_round_trip_non_spread_cost_bps",
    "KIS_US_ROUND_TRIP_NON_SPREAD_COST_BPS": "kis_us_round_trip_non_spread_cost_bps",
    "ALPACA_US_EQUITY_ROUND_TRIP_NON_SPREAD_COST_BPS": "alpaca_us_equity_round_trip_non_spread_cost_bps",
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
    "ACCOUNT_DRAWDOWN_RECOVERY_ENABLED": "account_drawdown_recovery_enabled",
    "ACCOUNT_DRAWDOWN_RECOVERY_THRESHOLD_RATIO": "account_drawdown_recovery_threshold_ratio",
    "ACCOUNT_DRAWDOWN_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS": "account_drawdown_recovery_required_healthy_observations",
    "ACCOUNT_DRAWDOWN_RECOVERY_COOLDOWN_SECONDS": "account_drawdown_recovery_cooldown_seconds",
    "ACCOUNT_DRAWDOWN_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS": "account_drawdown_recovery_min_observation_interval_seconds",
    "ACCOUNT_DRAWDOWN_RECOVERY_MAX_SNAPSHOT_FUTURE_SKEW_SECONDS": "account_drawdown_recovery_max_snapshot_future_skew_seconds",
    "ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET": "account_drawdown_recovery_scope_secret",
    "EXECUTION_POSITION_ACCOUNT_SCOPE": "execution_position_account_scope",
    "HOURLY_PLAN_JUDGMENT_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS": "hourly_plan_judgment_recovery_required_healthy_observations",
    "HOURLY_PLAN_JUDGMENT_RECOVERY_COOLDOWN_SECONDS": "hourly_plan_judgment_recovery_cooldown_seconds",
    "HOURLY_PLAN_JUDGMENT_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS": "hourly_plan_judgment_recovery_min_observation_interval_seconds",
    "CORE_BUY_AUTHORITY_MAX_OBSERVATION_AGE_SECONDS": "core_buy_authority_max_observation_age_seconds",
    "CORE_BUY_AUTHORITY_MAX_FUTURE_SKEW_SECONDS": "core_buy_authority_max_future_skew_seconds",
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
    "API_BEARER_TOKEN": "api_bearer_token",
    "TRADE_OUTCOME_FEEDBACK_ENABLED": "trade_outcome_feedback_enabled",
    "TRADE_OUTCOME_FEEDBACK_DIR": "trade_outcome_feedback_dir",
    "TRADE_OUTCOME_FEEDBACK_LOOKBACK_DAYS": "trade_outcome_feedback_lookback_days",
    "TRADE_OUTCOME_FEEDBACK_MAX_LESSONS": "trade_outcome_feedback_max_lessons",
    "TRADE_OUTCOME_FEEDBACK_MAX_AGE_SECONDS": "trade_outcome_feedback_max_age_seconds",
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
    "KIS_MIN_REQUEST_INTERVAL_SECONDS": "kis_min_request_interval_seconds",
    "KIS_TOKEN_REFRESH_MARGIN_SECONDS": "kis_token_refresh_margin_seconds",
    "KIS_TOKEN_FAILURE_COOLDOWN_SECONDS": "kis_token_failure_cooldown_seconds",
    "KIS_TOKEN_REQUEST_TIMEOUT_SECONDS": "kis_token_request_timeout_seconds",
    "KIS_TOKEN_ISSUANCE_LEASE_SECONDS": "kis_token_issuance_lease_seconds",
    "KIS_TOKEN_MIN_ISSUE_INTERVAL_SECONDS": "kis_token_min_issue_interval_seconds",
    "KIS_TOKEN_CACHE_DIRECTORY": "kis_token_cache_directory",
    "KIS_US_TRADING_ENABLED": "kis_us_trading_enabled",
    "KIS_US_LIVE_TRADING_ENABLED": "kis_us_live_trading_enabled",
    "KIS_US_LIVE_CONFIRMATION_TOKEN": "kis_us_live_confirmation_token",
    "KIS_US_ACCOUNT_ALLOWLIST": "kis_us_account_allowlist",
    "KIS_US_MAX_ORDER_NOTIONAL_USD": "kis_us_max_order_notional_usd",
    "KIS_US_MAX_TOTAL_EXPOSURE_USD": "kis_us_max_total_exposure_usd",
    "KIS_US_DAILY_REALIZED_LOSS_LIMIT_USD": "kis_us_daily_realized_loss_limit_usd",
    "KIS_US_WEEKLY_REALIZED_LOSS_LIMIT_USD": "kis_us_weekly_realized_loss_limit_usd",
    "KIS_US_UNREALIZED_LOSS_LIMIT_USD": "kis_us_unrealized_loss_limit_usd",
    "KIS_US_UNREALIZED_LOSS_PCT": "kis_us_unrealized_loss_pct",
    "KIS_US_SEVERE_HOLDING_LOSS_PCT": "kis_us_severe_holding_loss_pct",
    "KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE": "kis_us_allow_degraded_realized_pnl_coverage",
    "KIS_US_PREFLIGHT_BUYING_POWER_ENABLED": "kis_us_preflight_buying_power_enabled",
    "UPBIT_ACCESS_KEY": "upbit_access_key",
    "UPBIT_SECRET_KEY": "upbit_secret_key",
    "UPBIT_BASE_URL": "upbit_base_url",
    "UPBIT_LIVE_TRADING_ENABLED": "upbit_live_trading_enabled",
    "UPBIT_REQUEST_TIMEOUT_SECONDS": "upbit_request_timeout_seconds",
    "ALPACA_API_KEY": "alpaca_api_key",
    "ALPACA_API_SECRET": "alpaca_api_secret",
    "ALPACA_LIVE_BASE_URL": "alpaca_live_base_url",
    "ALPACA_PAPER_BASE_URL": "alpaca_paper_base_url",
    "ALPACA_DATA_BASE_URL": "alpaca_data_base_url",
    "ALPACA_LIVE_TRADING_ENABLED": "alpaca_live_trading_enabled",
    "ALPACA_REQUEST_TIMEOUT_SECONDS": "alpaca_request_timeout_seconds",
    "ALPACA_CRYPTO_MONITORING_ENABLED": "alpaca_crypto_monitoring_enabled",
    "ALPACA_CRYPTO_MONITORING_INTERVAL_SECONDS": "alpaca_crypto_monitoring_interval_seconds",
    "ALPACA_CRYPTO_RECONCILE_INTERVAL_SECONDS": "alpaca_crypto_reconcile_interval_seconds",
    "ALPACA_CRYPTO_ALLOWED_SYMBOLS": "alpaca_crypto_allowed_symbols",
    "ALPACA_CRYPTO_MAX_ORDER_NOTIONAL_USD": "alpaca_crypto_max_order_notional_usd",
    "ALPACA_CRYPTO_STRATEGY_ENABLED": "alpaca_crypto_strategy_enabled",
    "ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED": "alpaca_crypto_strategy_live_enabled",
    "ALPACA_CRYPTO_STRATEGY_VERSION": "alpaca_crypto_strategy_version",
    "ALPACA_CRYPTO_STRATEGY_DECISION_HOUR_UTC": "alpaca_crypto_strategy_decision_hour_utc",
    "ALPACA_CRYPTO_STRATEGY_DECISION_MINUTE_UTC": "alpaca_crypto_strategy_decision_minute_utc",
    "ALPACA_CRYPTO_STRATEGY_HISTORY_DAYS": "alpaca_crypto_strategy_history_days",
    "ALPACA_CRYPTO_STRATEGY_SLEEVE_USD": "alpaca_crypto_strategy_sleeve_usd",
    "ALPACA_CRYPTO_STRATEGY_MAX_INVESTED_FRACTION": "alpaca_crypto_strategy_max_invested_fraction",
    "ALPACA_CRYPTO_STRATEGY_TARGET_VOLATILITY": "alpaca_crypto_strategy_target_volatility",
    "ALPACA_CRYPTO_STRATEGY_MAX_POSITION_RISK_FRACTION": "alpaca_crypto_strategy_max_position_risk_fraction",
    "ALPACA_CRYPTO_STRATEGY_MIN_ORDER_NOTIONAL_USD": "alpaca_crypto_strategy_min_order_notional_usd",
    "ALPACA_CRYPTO_STRATEGY_MIN_REBALANCE_FRACTION": "alpaca_crypto_strategy_min_rebalance_fraction",
    "ALPACA_CRYPTO_STRATEGY_QUOTE_MAX_AGE_SECONDS": "alpaca_crypto_strategy_quote_max_age_seconds",
    "ALPACA_CRYPTO_STRATEGY_ANNUALIZATION_DAYS": "alpaca_crypto_strategy_annualization_days",
    "ALPACA_CRYPTO_STRATEGY_MAX_CLOSED_BAR_AGE_DAYS": "alpaca_crypto_strategy_max_closed_bar_age_days",
    "ALPACA_CRYPTO_STRATEGY_MAX_SPREAD_BPS": "alpaca_crypto_strategy_max_spread_bps",
    "ALPACA_CRYPTO_STRATEGY_STOP_ATR_MULTIPLE": "alpaca_crypto_strategy_stop_atr_multiple",
    "ALPACA_CRYPTO_STRATEGY_MAX_STOP_DISTANCE_FRACTION": "alpaca_crypto_strategy_max_stop_distance_fraction",
    "ALPACA_CRYPTO_STRATEGY_DAILY_DRAWDOWN_FRACTION": "alpaca_crypto_strategy_daily_drawdown_fraction",
    "ALPACA_CRYPTO_STRATEGY_WEEKLY_DRAWDOWN_FRACTION": "alpaca_crypto_strategy_weekly_drawdown_fraction",
    "ALPACA_CRYPTO_STRATEGY_PEAK_DRAWDOWN_FRACTION": "alpaca_crypto_strategy_peak_drawdown_fraction",
    "ALPACA_US_EQUITY_MONITORING_ENABLED": "alpaca_us_equity_monitoring_enabled",
    "ALPACA_US_EQUITY_MONITORING_INTERVAL_SECONDS": "alpaca_us_equity_monitoring_interval_seconds",
    "ALPACA_US_EQUITY_RECONCILE_INTERVAL_SECONDS": "alpaca_us_equity_reconcile_interval_seconds",
    "ALPACA_US_EQUITY_ALLOWED_SYMBOLS": "alpaca_us_equity_allowed_symbols",
    "ALPACA_US_EQUITY_MAX_ORDER_NOTIONAL_USD": "alpaca_us_equity_max_order_notional_usd",
    "ALPACA_US_EQUITY_STRATEGY_ENABLED": "alpaca_us_equity_strategy_enabled",
    "ALPACA_US_EQUITY_STRATEGY_LIVE_ENABLED": "alpaca_us_equity_strategy_live_enabled",
    "ALPACA_US_EQUITY_STRATEGY_VERSION": "alpaca_us_equity_strategy_version",
    "ALPACA_US_EQUITY_STRATEGY_SLEEVE_USD": "alpaca_us_equity_strategy_sleeve_usd",
    "ALPACA_US_EQUITY_STRATEGY_ANNUALIZATION_DAYS": "alpaca_us_equity_strategy_annualization_days",
    "ALPACA_US_EQUITY_STRATEGY_MAX_CLOSED_BAR_AGE_DAYS": "alpaca_us_equity_strategy_max_closed_bar_age_days",
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
            value = source[full_key]
            if field_name == "monitoring_max_symbols_per_cycle" and value.strip().lower() == "auto":
                request_timeout = float(
                    source.get(f"{prefix}KIS_REQUEST_TIMEOUT_SECONDS", Settings().kis_request_timeout_seconds)
                )
                target_seconds = float(
                    source.get(
                        f"{prefix}MONITORING_AUTO_SYMBOL_BATCH_TARGET_SECONDS",
                        Settings().monitoring_auto_symbol_batch_target_seconds,
                    )
                )
                quote_workers = 3
                estimated_limit = int((max(target_seconds, request_timeout) / max(request_timeout, 1.0)) * quote_workers)
                value = str(max(3, min(20, estimated_limit)))
            values[field_name] = value
    return Settings(**values)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()
