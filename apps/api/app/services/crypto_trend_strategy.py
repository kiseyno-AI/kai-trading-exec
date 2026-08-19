from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_DOWN, localcontext
from typing import Iterable, Mapping, Sequence


ZERO = Decimal("0")
ONE = Decimal("1")
METRIC_QUANTUM = Decimal("0.000000000001")
WEIGHT_QUANTUM = Decimal("0.000001")
CRYPTO_ANNUALIZATION_DAYS = Decimal("365")


def _decimal(value: Decimal | int | str) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _quantize_metric(value: Decimal | None) -> Decimal | None:
    return value.quantize(METRIC_QUANTUM) if value is not None else None


def _as_of_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(UTC).date()
    return value


@dataclass(frozen=True)
class DailyBar:
    """One UTC crypto day.

    ``bar_date`` names the UTC day that the candle opened. It becomes usable by
    a strategy only on a later UTC date, even when an upstream payload marks it
    closed. That prevents a still-forming daily candle from leaking into a
    signal.
    """

    symbol: str
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = ZERO
    is_closed: bool = True

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("symbol is required")
        object.__setattr__(self, "symbol", symbol)
        for field_name in ("open", "high", "low", "close", "volume"):
            object.__setattr__(self, field_name, _decimal(getattr(self, field_name)))
        if min(self.open, self.high, self.low, self.close) <= ZERO:
            raise ValueError("OHLC prices must be positive")
        if self.volume < ZERO:
            raise ValueError("volume must not be negative")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("bar high/low do not contain open and close")


@dataclass(frozen=True)
class TrendFeatures:
    symbol: str
    as_of_date: date
    closed_bar_count: int
    latest_bar_date: date | None
    latest_close: Decimal | None
    ema50: Decimal | None
    ema200: Decimal | None
    total_return_90d: Decimal | None
    atr14: Decimal | None
    realized_volatility_30d: Decimal | None
    eligible: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PortfolioTarget:
    symbol: str
    target_weight: Decimal
    eligible: bool
    reason: str


@dataclass(frozen=True)
class PortfolioAllocation:
    as_of_date: date
    targets: tuple[PortfolioTarget, ...]
    invested_weight: Decimal
    cash_weight: Decimal


@dataclass(frozen=True)
class ReplayPoint:
    trade_date: date
    equity: Decimal
    cash: Decimal
    invested_value: Decimal
    target_weights: tuple[tuple[str, Decimal], ...]


@dataclass(frozen=True)
class ReplayResult:
    initial_equity: Decimal
    final_equity: Decimal
    total_return: Decimal
    max_drawdown: Decimal
    trade_count: int
    total_fees: Decimal
    equity_curve: tuple[ReplayPoint, ...]


def filter_closed_bars(
    bars: Iterable[DailyBar],
    *,
    as_of: date | datetime,
    symbol: str | None = None,
) -> tuple[DailyBar, ...]:
    """Return validated, unique, fully closed bars in chronological order."""

    cutoff = _as_of_date(as_of)
    normalized_symbol = symbol.strip().upper() if symbol else None
    selected = [
        bar
        for bar in bars
        if bar.is_closed
        and bar.bar_date < cutoff
        and (normalized_symbol is None or bar.symbol == normalized_symbol)
    ]
    selected.sort(key=lambda bar: (bar.bar_date, bar.symbol))
    seen: set[tuple[str, date]] = set()
    for bar in selected:
        key = (bar.symbol, bar.bar_date)
        if key in seen:
            raise ValueError(f"duplicate daily bar: {bar.symbol} {bar.bar_date.isoformat()}")
        seen.add(key)
    return tuple(selected)


def ema(values: Sequence[Decimal], period: int) -> Decimal | None:
    """EMA seeded with the first period's simple average."""

    if period <= 0:
        raise ValueError("period must be positive")
    if len(values) < period:
        return None
    normalized = [_decimal(value) for value in values]
    with localcontext() as context:
        context.prec = 42
        current = sum(normalized[:period], ZERO) / Decimal(period)
        multiplier = Decimal(2) / Decimal(period + 1)
        for value in normalized[period:]:
            current = ((value - current) * multiplier) + current
        return _quantize_metric(current)


def total_return(values: Sequence[Decimal], periods: int) -> Decimal | None:
    if periods <= 0:
        raise ValueError("periods must be positive")
    if len(values) <= periods:
        return None
    start = _decimal(values[-(periods + 1)])
    end = _decimal(values[-1])
    if start <= ZERO:
        return None
    return _quantize_metric((end / start) - ONE)


def atr(bars: Sequence[DailyBar], period: int = 14) -> Decimal | None:
    if period <= 0:
        raise ValueError("period must be positive")
    if len(bars) <= period:
        return None
    true_ranges: list[Decimal] = []
    for previous, current in zip(bars, bars[1:]):
        true_ranges.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    current_atr = sum(true_ranges[:period], ZERO) / Decimal(period)
    for true_range in true_ranges[period:]:
        current_atr = ((current_atr * Decimal(period - 1)) + true_range) / Decimal(period)
    return _quantize_metric(current_atr)


def annualized_realized_volatility(
    values: Sequence[Decimal],
    *,
    periods: int = 30,
    annualization_days: Decimal = CRYPTO_ANNUALIZATION_DAYS,
) -> Decimal | None:
    """Sample standard deviation of daily log returns, annualized for 24/7 crypto."""

    if periods < 2:
        raise ValueError("periods must be at least 2")
    annualization_days = _decimal(annualization_days)
    if annualization_days <= ZERO:
        raise ValueError("annualization_days must be positive")
    if len(values) <= periods:
        return None
    selected = [_decimal(value) for value in values[-(periods + 1) :]]
    if any(value <= ZERO for value in selected):
        return None
    with localcontext() as context:
        context.prec = 42
        log_returns = [(current / previous).ln() for previous, current in zip(selected, selected[1:])]
        average = sum(log_returns, ZERO) / Decimal(len(log_returns))
        variance = sum(((value - average) ** 2 for value in log_returns), ZERO) / Decimal(
            len(log_returns) - 1
        )
        annualized = variance.sqrt() * annualization_days.sqrt()
        return _quantize_metric(annualized)


def evaluate_symbol(
    symbol: str,
    bars: Iterable[DailyBar],
    *,
    as_of: date | datetime,
) -> TrendFeatures:
    """Evaluate the V1 long/cash trend rule from closed UTC daily bars only."""

    normalized_symbol = symbol.strip().upper()
    closed = filter_closed_bars(bars, as_of=as_of, symbol=normalized_symbol)
    closes = [bar.close for bar in closed]
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200)
    return90 = total_return(closes, 90)
    atr14 = atr(closed, 14)
    volatility30 = annualized_realized_volatility(closes, periods=30)

    reasons: list[str] = []
    if len(closed) < 200:
        reasons.append(f"insufficient_closed_bars:{len(closed)}/200")
    if not closes:
        reasons.append("latest_close_unavailable")
    elif ema200 is not None and closes[-1] <= ema200:
        reasons.append("close_not_above_ema200")
    if ema50 is not None and ema200 is not None and ema50 <= ema200:
        reasons.append("ema50_not_above_ema200")
    if return90 is not None and return90 <= ZERO:
        reasons.append("non_positive_90_day_return")
    if volatility30 is None or volatility30 <= ZERO:
        reasons.append("realized_volatility_unavailable")

    eligible = bool(
        len(closed) >= 200
        and closes
        and ema50 is not None
        and ema200 is not None
        and closes[-1] > ema200
        and ema50 > ema200
        and return90 is not None
        and return90 > ZERO
        and atr14 is not None
        and volatility30 is not None
        and volatility30 > ZERO
    )
    if eligible:
        reasons.append("eligible_long_trend")

    return TrendFeatures(
        symbol=normalized_symbol,
        as_of_date=_as_of_date(as_of),
        closed_bar_count=len(closed),
        latest_bar_date=closed[-1].bar_date if closed else None,
        latest_close=_quantize_metric(closes[-1]) if closes else None,
        ema50=ema50,
        ema200=ema200,
        total_return_90d=return90,
        atr14=atr14,
        realized_volatility_30d=volatility30,
        eligible=eligible,
        reasons=tuple(reasons),
    )


def allocate_inverse_volatility(
    features: Iterable[TrendFeatures],
    *,
    total_exposure: Decimal = Decimal("0.60"),
    per_asset_cap: Decimal = Decimal("0.40"),
    cash_floor: Decimal = Decimal("0.40"),
) -> PortfolioAllocation:
    """Allocate eligible assets by inverse volatility with hard exposure caps."""

    total_exposure = _decimal(total_exposure)
    per_asset_cap = _decimal(per_asset_cap)
    cash_floor = _decimal(cash_floor)
    if not ZERO <= total_exposure <= ONE:
        raise ValueError("total_exposure must be between 0 and 1")
    if not ZERO < per_asset_cap <= ONE:
        raise ValueError("per_asset_cap must be greater than 0 and at most 1")
    if not ZERO <= cash_floor <= ONE:
        raise ValueError("cash_floor must be between 0 and 1")

    ordered = sorted(features, key=lambda item: item.symbol)
    if not ordered:
        raise ValueError("at least one feature set is required")
    if len({item.symbol for item in ordered}) != len(ordered):
        raise ValueError("feature symbols must be unique")
    as_of_dates = {item.as_of_date for item in ordered}
    if len(as_of_dates) != 1:
        raise ValueError("all feature sets must share one as_of_date")

    budget = min(total_exposure, ONE - cash_floor)
    candidates = {
        item.symbol: ONE / item.realized_volatility_30d
        for item in ordered
        if item.eligible
        and item.realized_volatility_30d is not None
        and item.realized_volatility_30d > ZERO
    }
    allocations: dict[str, Decimal] = {}
    remaining_budget = budget
    remaining = dict(candidates)
    while remaining and remaining_budget > ZERO:
        score_total = sum(remaining.values(), ZERO)
        proposed = {
            symbol: remaining_budget * score / score_total
            for symbol, score in remaining.items()
        }
        capped = [symbol for symbol, weight in proposed.items() if weight > per_asset_cap]
        if not capped:
            allocations.update(proposed)
            break
        for symbol in sorted(capped):
            allocations[symbol] = per_asset_cap
            remaining_budget -= per_asset_cap
            remaining.pop(symbol)
        remaining_budget = max(remaining_budget, ZERO)

    rounded_allocations = {
        symbol: max(ZERO, weight).quantize(WEIGHT_QUANTUM, rounding=ROUND_DOWN)
        for symbol, weight in allocations.items()
    }
    invested = sum(rounded_allocations.values(), ZERO)
    targets = tuple(
        PortfolioTarget(
            symbol=item.symbol,
            target_weight=rounded_allocations.get(item.symbol, ZERO),
            eligible=item.eligible,
            reason=(
                "trend_eligible_inverse_volatility"
                if item.symbol in rounded_allocations
                else ";".join(item.reasons) or "not_eligible"
            ),
        )
        for item in ordered
    )
    return PortfolioAllocation(
        as_of_date=ordered[0].as_of_date,
        targets=targets,
        invested_weight=invested,
        cash_weight=(ONE - invested).quantize(WEIGHT_QUANTUM),
    )


def replay_strategy(
    bars_by_symbol: Mapping[str, Sequence[DailyBar]],
    *,
    initial_equity: Decimal = Decimal("10000"),
    total_exposure: Decimal = Decimal("0.60"),
    per_asset_cap: Decimal = Decimal("0.40"),
    cash_floor: Decimal = Decimal("0.40"),
    fee_bps: Decimal = Decimal("25"),
    slippage_bps: Decimal = Decimal("5"),
) -> ReplayResult:
    """Small deterministic next-open replay for validation, not a fill simulator.

    Each UTC day uses only earlier daily bars for its signal, rebalances at that
    day's open, and marks equity at that day's close. Fees and adverse slippage
    are applied on both buys and sells.
    """

    initial_equity = _decimal(initial_equity)
    fee_bps = _decimal(fee_bps)
    slippage_bps = _decimal(slippage_bps)
    if initial_equity <= ZERO:
        raise ValueError("initial_equity must be positive")
    if fee_bps < ZERO or slippage_bps < ZERO:
        raise ValueError("fee_bps and slippage_bps must not be negative")
    if not bars_by_symbol:
        raise ValueError("bars_by_symbol must not be empty")

    normalized: dict[str, tuple[DailyBar, ...]] = {}
    by_date: dict[str, dict[date, DailyBar]] = {}
    for raw_symbol, raw_bars in bars_by_symbol.items():
        symbol = raw_symbol.strip().upper()
        if not symbol:
            raise ValueError("bar mapping symbols must not be empty")
        if symbol in normalized:
            raise ValueError(f"duplicate normalized symbol: {symbol}")
        ordered = tuple(sorted(raw_bars, key=lambda bar: bar.bar_date))
        if not ordered:
            raise ValueError(f"at least one daily bar is required for {symbol}")
        if any(bar.symbol != symbol for bar in ordered):
            raise ValueError(f"bar symbol mismatch for {symbol}")
        if len({bar.bar_date for bar in ordered}) != len(ordered):
            raise ValueError(f"duplicate daily bar for {symbol}")
        normalized[symbol] = ordered
        by_date[symbol] = {bar.bar_date: bar for bar in ordered if bar.is_closed}

    shared_dates = sorted(set.intersection(*(set(rows) for rows in by_date.values())))
    cash = initial_equity
    quantities = {symbol: ZERO for symbol in normalized}
    fee_rate = fee_bps / Decimal("10000")
    slippage_rate = slippage_bps / Decimal("10000")
    total_fees = ZERO
    trade_count = 0
    curve: list[ReplayPoint] = []

    for trade_date in shared_dates:
        if any(sum(1 for bar in bars if bar.bar_date < trade_date and bar.is_closed) < 200 for bars in normalized.values()):
            continue
        features = [
            evaluate_symbol(symbol, bars, as_of=trade_date)
            for symbol, bars in normalized.items()
        ]
        allocation = allocate_inverse_volatility(
            features,
            total_exposure=total_exposure,
            per_asset_cap=per_asset_cap,
            cash_floor=cash_floor,
        )
        target_weights = {target.symbol: target.target_weight for target in allocation.targets}
        open_equity = cash + sum(
            quantities[symbol] * by_date[symbol][trade_date].open
            for symbol in normalized
        )
        desired_quantities = {
            symbol: (open_equity * target_weights.get(symbol, ZERO)) / by_date[symbol][trade_date].open
            for symbol in normalized
        }

        for symbol in sorted(normalized):
            difference = desired_quantities[symbol] - quantities[symbol]
            if difference >= ZERO:
                continue
            quantity = -difference
            execution_price = by_date[symbol][trade_date].open * (ONE - slippage_rate)
            proceeds = quantity * execution_price
            fee = proceeds * fee_rate
            cash += proceeds - fee
            quantities[symbol] -= quantity
            total_fees += fee
            trade_count += 1

        for symbol in sorted(normalized):
            difference = desired_quantities[symbol] - quantities[symbol]
            if difference <= ZERO:
                continue
            execution_price = by_date[symbol][trade_date].open * (ONE + slippage_rate)
            unit_cost = execution_price * (ONE + fee_rate)
            quantity = min(difference, cash / unit_cost if unit_cost > ZERO else ZERO)
            if quantity <= ZERO:
                continue
            notional = quantity * execution_price
            fee = notional * fee_rate
            cash -= notional + fee
            quantities[symbol] += quantity
            total_fees += fee
            trade_count += 1

        invested_value = sum(
            quantities[symbol] * by_date[symbol][trade_date].close
            for symbol in normalized
        )
        equity = cash + invested_value
        curve.append(
            ReplayPoint(
                trade_date=trade_date,
                equity=_quantize_metric(equity) or ZERO,
                cash=_quantize_metric(cash) or ZERO,
                invested_value=_quantize_metric(invested_value) or ZERO,
                target_weights=tuple(sorted(target_weights.items())),
            )
        )

    final_equity = curve[-1].equity if curve else initial_equity
    peak = initial_equity
    max_drawdown = ZERO
    for point in curve:
        peak = max(peak, point.equity)
        if peak > ZERO:
            max_drawdown = min(max_drawdown, (point.equity / peak) - ONE)
    return ReplayResult(
        initial_equity=_quantize_metric(initial_equity) or ZERO,
        final_equity=_quantize_metric(final_equity) or ZERO,
        total_return=_quantize_metric((final_equity / initial_equity) - ONE) or ZERO,
        max_drawdown=_quantize_metric(max_drawdown) or ZERO,
        trade_count=trade_count,
        total_fees=_quantize_metric(total_fees) or ZERO,
        equity_curve=tuple(curve),
    )
