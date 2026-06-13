from decimal import Decimal

from app.core.config import Settings
from app.services.risk_sizing import RiskSizingResult, resolve_risk_based_buy_quantity


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
