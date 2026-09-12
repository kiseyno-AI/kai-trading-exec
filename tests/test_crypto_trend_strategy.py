from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.crypto_trend_strategy import (
    DailyBar,
    TrendFeatures,
    allocate_inverse_volatility,
    annualized_realized_volatility,
    atr,
    ema,
    evaluate_symbol,
    filter_closed_bars,
    replay_strategy,
    total_return,
)


def _bars(
    symbol: str,
    *,
    count: int,
    start: date = date(2025, 1, 1),
    rising: bool = True,
) -> list[DailyBar]:
    rows = []
    for offset in range(count):
        if rising:
            close = Decimal("100") + Decimal(offset) + (Decimal(offset % 3) / Decimal("10"))
        else:
            close = Decimal("100")
        rows.append(
            DailyBar(
                symbol=symbol,
                bar_date=start + timedelta(days=offset),
                open=close - Decimal("0.5"),
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("10.25"),
            )
        )
    return rows


def _features(
    symbol: str,
    *,
    volatility: str,
    eligible: bool = True,
) -> TrendFeatures:
    return TrendFeatures(
        symbol=symbol,
        as_of_date=date(2026, 1, 1),
        closed_bar_count=250,
        latest_bar_date=date(2025, 12, 31),
        latest_close=Decimal("100"),
        ema50=Decimal("95"),
        ema200=Decimal("80"),
        total_return_90d=Decimal("0.20"),
        atr14=Decimal("3"),
        realized_volatility_30d=Decimal(volatility),
        eligible=eligible,
        reasons=("eligible_long_trend",) if eligible else ("close_not_above_ema200",),
    )


def test_daily_bar_requires_valid_decimal_ohlc() -> None:
    with pytest.raises(ValueError, match="high/low"):
        DailyBar(
            symbol="BTC/USD",
            bar_date=date(2026, 1, 1),
            open=Decimal("100"),
            high=Decimal("99"),
            low=Decimal("90"),
            close=Decimal("98"),
        )


def test_filter_closed_bars_excludes_current_and_explicitly_open_bars() -> None:
    rows = [
        DailyBar("btc/usd", date(2026, 1, 2), "101", "102", "100", "101"),
        DailyBar("BTC/USD", date(2026, 1, 1), "100", "101", "99", "100"),
        DailyBar("BTC/USD", date(2026, 1, 3), "102", "103", "101", "102"),
        DailyBar("BTC/USD", date(2025, 12, 31), "99", "100", "98", "99", is_closed=False),
    ]

    selected = filter_closed_bars(
        rows,
        symbol="BTC/USD",
        as_of=datetime(2026, 1, 3, 0, 10, tzinfo=UTC),
    )

    assert [bar.bar_date for bar in selected] == [date(2026, 1, 1), date(2026, 1, 2)]
    assert all(bar.symbol == "BTC/USD" for bar in selected)


def test_filter_closed_bars_rejects_duplicate_symbol_date() -> None:
    row = DailyBar("BTC/USD", date(2026, 1, 1), "100", "101", "99", "100")

    with pytest.raises(ValueError, match="duplicate daily bar"):
        filter_closed_bars([row, row], as_of=date(2026, 1, 2))


def test_indicator_primitives_have_deterministic_decimal_results() -> None:
    assert ema([Decimal("1"), Decimal("2"), Decimal("3")], 2) == Decimal("2.500000000000")
    assert total_return([Decimal("100"), Decimal("110"), Decimal("121")], 2) == Decimal(
        "0.210000000000"
    )

    rows = [
        DailyBar("BTC/USD", date(2026, 1, 1) + timedelta(days=index), "100", "102", "99", "100")
        for index in range(15)
    ]
    assert atr(rows, 14) == Decimal("3.000000000000")

    values = [Decimal("100") + Decimal(index % 2) + Decimal(index) for index in range(31)]
    volatility = annualized_realized_volatility(values)
    assert isinstance(volatility, Decimal)
    assert volatility is not None and volatility > 0


def test_atr_uses_wilders_smoothing_after_its_seed_window() -> None:
    rows = [
        DailyBar("BTC/USD", date(2026, 1, 1), "10", "11", "9", "10"),
        DailyBar("BTC/USD", date(2026, 1, 2), "11", "13", "10", "12"),
        DailyBar("BTC/USD", date(2026, 1, 3), "13", "14", "12", "13"),
        DailyBar("BTC/USD", date(2026, 1, 4), "14", "18", "13", "17"),
    ]

    assert atr(rows, 2) == Decimal("3.750000000000")


def test_evaluate_symbol_uses_only_closed_history_and_marks_bull_trend_eligible() -> None:
    rows = _bars("BTC/USD", count=230)
    current_day = rows[-1].bar_date + timedelta(days=1)
    rows.append(
        DailyBar(
            "BTC/USD",
            current_day,
            "1",
            "2",
            "0.5",
            "1",
            is_closed=True,
        )
    )

    result = evaluate_symbol("BTC/USD", rows, as_of=current_day)

    assert result.closed_bar_count == 230
    assert result.latest_bar_date == current_day - timedelta(days=1)
    assert result.ema50 is not None and result.ema200 is not None
    assert result.ema50 > result.ema200
    assert result.latest_close is not None and result.latest_close > result.ema200
    assert result.total_return_90d is not None and result.total_return_90d > 0
    assert result.atr14 is not None and result.atr14 > 0
    assert result.realized_volatility_30d is not None and result.realized_volatility_30d > 0
    assert result.eligible is True
    assert result.reasons == ("eligible_long_trend",)


def test_evaluate_symbol_uses_family_specific_annualization_basis() -> None:
    rows = _bars("AAPL", count=230)
    as_of = rows[-1].bar_date + timedelta(days=1)

    crypto_basis = evaluate_symbol(
        "AAPL",
        rows,
        as_of=as_of,
        annualization_days=Decimal("365"),
    )
    equity_basis = evaluate_symbol(
        "AAPL",
        rows,
        as_of=as_of,
        annualization_days=Decimal("252"),
    )

    assert crypto_basis.realized_volatility_30d is not None
    assert equity_basis.realized_volatility_30d is not None
    assert crypto_basis.realized_volatility_30d > equity_basis.realized_volatility_30d


def test_evaluate_symbol_fails_closed_on_short_or_flat_history() -> None:
    short = evaluate_symbol(
        "BTC/USD",
        _bars("BTC/USD", count=199),
        as_of=date(2026, 1, 1),
    )
    flat_rows = _bars("ETH/USD", count=230, rising=False)
    flat = evaluate_symbol(
        "ETH/USD",
        flat_rows,
        as_of=flat_rows[-1].bar_date + timedelta(days=1),
    )

    assert short.eligible is False
    assert "insufficient_closed_bars:199/200" in short.reasons
    assert flat.eligible is False
    assert "close_not_above_ema200" in flat.reasons
    assert "ema50_not_above_ema200" in flat.reasons
    assert "non_positive_90_day_return" in flat.reasons
    assert "realized_volatility_unavailable" in flat.reasons


def test_inverse_volatility_targets_apply_exposure_cash_and_asset_caps() -> None:
    allocation = allocate_inverse_volatility(
        [
            _features("BTC/USD", volatility="0.20"),
            _features("ETH/USD", volatility="0.40"),
        ]
    )

    targets = {target.symbol: target.target_weight for target in allocation.targets}
    assert targets == {
        "BTC/USD": Decimal("0.400000"),
        "ETH/USD": Decimal("0.200000"),
    }
    assert allocation.invested_weight == Decimal("0.600000")
    assert allocation.cash_weight == Decimal("0.400000")


def test_inverse_volatility_leaves_unallocatable_budget_in_cash() -> None:
    allocation = allocate_inverse_volatility(
        [
            _features("BTC/USD", volatility="0.20"),
            _features("ETH/USD", volatility="0.40", eligible=False),
        ]
    )

    targets = {target.symbol: target.target_weight for target in allocation.targets}
    assert targets["BTC/USD"] == Decimal("0.400000")
    assert targets["ETH/USD"] == Decimal("0")
    assert allocation.cash_weight == Decimal("0.600000")


def test_replay_uses_prior_bars_and_reports_costs_and_equity_curve() -> None:
    btc = _bars("BTC/USD", count=230)
    eth = _bars("ETH/USD", count=230)

    result = replay_strategy(
        {"BTC/USD": btc, "ETH/USD": eth},
        initial_equity=Decimal("10000"),
        fee_bps=Decimal("25"),
        slippage_bps=Decimal("5"),
    )

    assert result.equity_curve
    assert result.equity_curve[0].trade_date == btc[200].bar_date
    assert result.trade_count > 0
    assert result.total_fees > 0
    assert result.final_equity > result.initial_equity
    assert result.total_return > 0
    assert result.max_drawdown <= 0
    assert all(isinstance(point.equity, Decimal) for point in result.equity_curve)


def test_replay_of_flat_market_stays_in_cash_without_trades() -> None:
    result = replay_strategy(
        {"BTC/USD": _bars("BTC/USD", count=205, rising=False)},
        initial_equity=Decimal("2500"),
    )

    assert result.trade_count == 0
    assert result.total_fees == Decimal("0E-12")
    assert result.final_equity == Decimal("2500.000000000000")
    assert result.total_return == Decimal("0E-12")
