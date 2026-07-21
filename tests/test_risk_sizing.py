from decimal import Decimal

from app.core.config import Settings, load_settings
from app.services.risk_sizing import RiskSizingRejection, RiskSizingResult, resolve_risk_based_buy_quantity


def test_default_exec_sizing_uses_three_x_short_term_budget_caps() -> None:
    settings = Settings()

    assert settings.exec_agent_max_symbol_budget == Decimal("3000000")
    assert settings.exec_agent_max_total_budget == Decimal("9000000")
    assert settings.exec_agent_base_risk_pct == Decimal("0.015")
    assert settings.exec_agent_max_position_allocation_pct == Decimal("0.30")
    assert settings.exec_agent_max_symbol_total_exposure_pct == Decimal("0.30")


def test_settings_loads_market_context_controls() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_MARKET_CONTEXT_ENABLED": "true",
            "KAI_EXEC_MARKET_CONTEXT_BASE_URL": "http://127.0.0.1:8000",
            "KAI_EXEC_MARKET_CONTEXT_BEARER_TOKEN": "token",
            "KAI_EXEC_MARKET_CONTEXT_TIMEOUT_SECONDS": "7",
            "KAI_EXEC_MARKET_CONTEXT_CACHE_TTL_SECONDS": "45",
        }
    )

    assert settings.market_context_enabled is True
    assert settings.market_context_base_url == "http://127.0.0.1:8000"
    assert settings.market_context_bearer_token == "token"
    assert settings.market_context_timeout_seconds == 7
    assert settings.market_context_cache_ttl_seconds == 45


def test_default_downside_guardrails_are_conservative_after_loss_week() -> None:
    settings = Settings()

    assert settings.exec_agent_stop_loss_pct == Decimal("0.025")
    assert settings.exec_agent_max_holding_trading_sessions == 2
    assert settings.account_drawdown_gate_enabled is True
    assert settings.exec_agent_trigger_time_timeout_seconds == 900
    assert settings.exec_agent_watch_sizing_timeout_seconds == 900


def test_risk_sizing_allows_three_x_notional_when_stop_risk_and_caps_allow() -> None:
    settings = Settings()

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.risk_pct == Decimal("0.015")
    assert decision.risk_budget == Decimal("150000.0000")
    assert decision.max_position_budget == Decimal("3000000.0000")
    assert decision.max_symbol_exposure == Decimal("3000000.0000")
    assert decision.quantity == 30
    assert decision.approved_notional == Decimal("3000000.0000")


def test_risk_sizing_clamps_three_x_notional_to_candidate_budget() -> None:
    settings = Settings()

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        candidate_max_budget=Decimal("1200000"),
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 12
    assert decision.approved_notional == Decimal("1200000.0000")


def test_risk_sizing_vol_corr_disabled_preserves_baseline_quantity() -> None:
    settings = Settings(vol_corr_sizing_enabled=False)

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        vol_corr_context={"realized_volatility_pct": "10", "max_holding_correlation": "0.95"},
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 30
    assert "vol_corr_multiplier" not in decision.cap_details


def test_risk_sizing_high_volatility_reduces_quantity_when_enabled() -> None:
    settings = Settings(vol_corr_sizing_enabled=True)

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        vol_corr_context={"realized_volatility_pct": "5.0"},
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 22
    assert decision.cap_details["baseline_quantity"] == 30
    assert decision.cap_details["volatility_multiplier"] == "0.7500"
    assert decision.cap_details["correlation_multiplier"] == "1.0000"
    assert decision.cap_details["vol_corr_cap_reason"] == "high_volatility"


def test_risk_sizing_high_correlation_reduces_quantity_when_enabled() -> None:
    settings = Settings(vol_corr_sizing_enabled=True)

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        vol_corr_context={
            "realized_volatility_pct": "2.0",
            "max_holding_correlation": "0.82",
            "correlation_symbol": "000660",
        },
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 24
    assert decision.quantity <= decision.cap_details["baseline_quantity"]
    assert decision.cap_details["correlation"] == "0.8200"
    assert decision.cap_details["correlation_symbol"] == "000660"
    assert decision.cap_details["vol_corr_cap_reason"] == "high_correlation"


def test_risk_sizing_missing_data_can_be_conservative() -> None:
    settings = Settings(
        vol_corr_sizing_enabled=True,
        vol_corr_missing_data_behavior="conservative",
        vol_corr_missing_data_multiplier=Decimal("0.50"),
    )

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        vol_corr_context={"holding_symbols": ["000660"]},
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 15
    assert decision.cap_details["missing_data_behavior"] == "conservative"
    assert decision.cap_details["missing_data_reasons"] == "missing_volatility,missing_correlation"
    assert decision.cap_details["vol_corr_cap_reason"] == "missing_data_conservative"


def test_risk_sizing_computes_volatility_and_correlation_from_returns() -> None:
    settings = Settings(
        vol_corr_sizing_enabled=True,
        vol_corr_high_volatility_threshold_pct=Decimal("3.0"),
        vol_corr_high_correlation_threshold=Decimal("0.70"),
    )

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        vol_corr_context={
            "daily_return_pcts": ["-5", "4", "-4", "5", "-5", "4", "-4", "5", "-5", "4"],
            "holding_return_pcts_by_symbol": {
                "000660": ["-4", "3", "-3", "4", "-4", "3", "-3", "4", "-4", "3"]
            },
        },
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 18
    assert decision.cap_details["volatility_source"] == "daily_return_pcts"
    assert decision.cap_details["volatility_observations"] == 10
    assert decision.cap_details["correlation_source"] == "computed_returns"
    assert decision.cap_details["correlation_symbol"] == "000660"
    assert decision.cap_details["vol_corr_multiplier"] == "0.6000"


def test_risk_sizing_normal_market_breadth_preserves_baseline_quantity() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market": "KOSPI",
                "market_date": "2026-06-25",
                "up_count": 520,
                "down_count": 430,
                "flat_count": 50,
                "up_ratio": "0.52",
                "advance_decline_ratio": "1.21",
                "source": "test",
            }
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 30
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "normal"
    assert guardrail["action"] == "allow"


def test_risk_sizing_risk_off_market_breadth_downsizes_quantity() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(market_regime_risk_off_sizing_multiplier=Decimal("0.20")),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market": "KOSDAQ",
                "market_date": "2026-06-25",
                "up_count": 350,
                "down_count": 600,
                "flat_count": 50,
                "up_ratio": "0.35",
            }
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 5
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "risk_off"
    assert guardrail["market_regime_reason_code"] == "market_breadth_risk_off"
    assert guardrail["action"] == "downsize"
    assert guardrail["multiplier"] == "0.2000"


def test_risk_sizing_below_normal_market_breadth_downsizes_quantity() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market": "KOSPI",
                "market_date": "2026-06-25",
                "up_ratio": "0.42",
                "advance_decline_ratio": "1.10",
            }
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 9
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "caution"
    assert guardrail["market_regime_reason_code"] == "market_breadth_caution"
    assert guardrail["action"] == "downsize"
    assert guardrail["multiplier"] == "0.3300"


def test_risk_sizing_risk_off_market_breadth_downsizes_buy() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market": "KOSPI",
                "market_date": "2026-06-25",
                "up_count": 250,
                "down_count": 700,
                "flat_count": 50,
                "up_ratio": "0.25",
            }
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 4
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "risk_off"
    assert guardrail["market_regime_reason_code"] == "market_breadth_risk_off"
    assert guardrail["action"] == "downsize"


def test_risk_sizing_missing_market_breadth_in_live_automation_downsizes() -> None:
    settings = Settings(
        runtime_mode="live",
        automation_enabled=True,
        market_regime_caution_sizing_multiplier=Decimal("0.50"),
    )

    decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={},
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 10
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "caution"
    assert guardrail["market_regime_reason_code"] == "market_breadth_missing_caution"
    assert "market_breadth_missing" in guardrail["warnings"]


def test_risk_sizing_nested_decider_handoff_market_regime_rejects_when_downsized_below_one_share() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(runtime_mode="live", automation_enabled=True),
        account_basis=Decimal("8517680"),
        available_cash=Decimal("8517680"),
        entry_price=Decimal("140700"),
        stop_price=Decimal("137182.5"),
        confidence=Decimal("0.52"),
        requested_quantity=1,
        candidate_max_budget=Decimal("144700"),
        agent_max_quantity=2,
        market_regime_context={
            "decider_handoff_context": {
                "market_regime_context": {
                    "market": "KOSDAQ",
                    "market_breadth": {
                        "market": "KOSDAQ",
                        "market_date": "2026-07-14",
                        "up_count": 213,
                        "down_count": 1423,
                        "flat_count": 134,
                        "up_ratio": "0.1203",
                        "advance_decline_ratio": "0.149684",
                    },
                    "sector_context": {
                        "sector_name": "기계·장비",
                        "relative_state": "weak",
                    },
                },
            },
        },
        market_date="2026-07-14",
    )

    assert isinstance(decision, RiskSizingRejection)
    assert decision.reason_code == "market_regime_min_lot_rejected"
    guardrail = decision.details["market_regime_guardrail"]
    assert guardrail["breadth"]["up_count"] == 213
    assert guardrail["market_regime_posture"] == "risk_off"
    assert guardrail["action"] == "downsize"


def test_risk_sizing_market_regime_downsize_below_one_share_has_precise_reason() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(
            runtime_mode="live",
            automation_enabled=True,
            market_regime_caution_sizing_multiplier=Decimal("0.33"),
        ),
        account_basis=Decimal("8517680"),
        available_cash=Decimal("8517680"),
        entry_price=Decimal("140700"),
        stop_price=Decimal("137182.5"),
        confidence=Decimal("0.52"),
        requested_quantity=1,
        candidate_max_budget=Decimal("144700"),
        agent_max_quantity=2,
        market_regime_context={},
        market_date="2026-07-14",
    )

    assert isinstance(decision, RiskSizingRejection)
    assert decision.reason_code == "market_regime_min_lot_rejected"
    assert decision.details["cash_quantity"] == 60
    assert decision.details["baseline_quantity"] == 1
    guardrail = decision.details["market_regime_guardrail"]
    assert guardrail["market_regime_reason_code"] == "market_breadth_missing_caution"
    assert guardrail["adjusted_quantity"] == 0


def test_risk_sizing_stale_market_breadth_neutral_behavior_preserves_quantity() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(market_regime_stale_breadth_behavior="neutral_with_warning"),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market": "KOSPI",
                "market_date": "2026-06-24",
                "up_ratio": "0.25",
            }
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 30
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "unavailable"
    assert guardrail["market_regime_reason_code"] == "market_breadth_stale_neutral"
    assert guardrail["action"] == "allow"
    assert "market_breadth_stale" in guardrail["warnings"]


def test_risk_sizing_sector_weakness_downsizes_buy() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(
            market_regime_sector_weak_multiplier=Decimal("0.50"),
            market_regime_sector_weak_max_notional_krw=Decimal("3000000"),
        ),
        account_basis=Decimal("10000000"),
        available_cash=Decimal("10000000"),
        entry_price=Decimal("100000"),
        stop_price=Decimal("95000"),
        confidence=Decimal("0.70"),
        market_regime_context={
            "market_breadth": {
                "market_date": "2026-06-25",
                "up_ratio": "0.55",
            },
            "sector_peer_context": {
                "sector_name": "Semiconductors",
                "relative_state": "weak",
                "relative_percentile": "0.20",
            },
        },
        market_date="2026-06-25",
    )

    assert isinstance(decision, RiskSizingResult)
    assert decision.quantity == 15
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_reason_code"] == "sector_weakness_or_crowding_downsize"
    assert guardrail["sector"]["sector_name"] == "Semiconductors"
