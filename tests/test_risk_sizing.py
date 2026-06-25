from decimal import Decimal

from app.core.config import Settings
from app.services.risk_sizing import RiskSizingRejection, RiskSizingResult, resolve_risk_based_buy_quantity


def test_default_exec_sizing_uses_three_x_short_term_budget_caps() -> None:
    settings = Settings()

    assert settings.exec_agent_max_symbol_budget == Decimal("3000000")
    assert settings.exec_agent_max_total_budget == Decimal("9000000")
    assert settings.exec_agent_base_risk_pct == Decimal("0.015")
    assert settings.exec_agent_max_position_allocation_pct == Decimal("0.30")
    assert settings.exec_agent_max_symbol_total_exposure_pct == Decimal("0.30")


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


def test_risk_sizing_caution_market_breadth_downsizes_quantity() -> None:
    decision = resolve_risk_based_buy_quantity(
        settings=Settings(market_regime_caution_sizing_multiplier=Decimal("0.50")),
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
    assert decision.quantity == 15
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "caution"
    assert guardrail["action"] == "downsize"
    assert guardrail["multiplier"] == "0.5000"


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
    assert decision.quantity == 15
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "caution"
    assert guardrail["market_regime_reason_code"] == "market_breadth_below_normal"
    assert guardrail["action"] == "downsize"
    assert guardrail["multiplier"] == "0.5000"


def test_risk_sizing_risk_off_market_breadth_rejects_buy() -> None:
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

    assert isinstance(decision, RiskSizingRejection)
    assert decision.reason_code == "market_breadth_risk_off"
    assert decision.details["market_regime_guardrail"]["action"] == "reject"


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
    assert decision.quantity == 15
    guardrail = decision.cap_details["market_regime_guardrail"]
    assert guardrail["market_regime_posture"] == "caution"
    assert guardrail["market_regime_reason_code"] == "market_breadth_missing_caution"
    assert "market_breadth_missing" in guardrail["warnings"]


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
