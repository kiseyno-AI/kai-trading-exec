from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot, BrokerLiveMarketContext
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    CryptoDailyBar,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
)
from app.services.broker_orders import place_broker_order
from app.services.crypto_trend_strategy import (
    DailyBar,
    PortfolioAllocation,
    TrendFeatures,
    allocate_inverse_volatility,
    evaluate_symbol,
)


PROVIDER = "alpaca"
EXCHANGE = "ALPACA"
TERMINAL_DECISION_STATUSES = frozenset(
    {"no_action", "submitted", "duplicate", "blocked_risk", "rejected"}
)


class AlpacaCryptoStrategyService:
    """Deterministic long/cash strategy executed through the guarded order service."""

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session],
        asset_class: str = "crypto",
        exchange: str = EXCHANGE,
        time_in_force: str = "gtc",
        bar_source: str = "alpaca_crypto_bars_v1beta3",
        order_origin: str = "alpaca_crypto_strategy",
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.asset_class = asset_class
        self.exchange = exchange
        self.time_in_force = time_in_force
        self.bar_source = bar_source
        self.order_origin = order_origin
        self._last_result: dict[str, object] = {
            "enabled": settings.alpaca_crypto_strategy_enabled,
            "status": "not_started" if settings.alpaca_crypto_strategy_enabled else "disabled",
        }

    def snapshot(self) -> dict[str, object]:
        return dict(self._last_result)

    def run_cycle(
        self,
        *,
        adapter: AlpacaBrokerAdapter,
        access_token: str,
        quotes: dict[str, BrokerLiveMarketContext],
        observed_at: datetime,
        market_open: bool = True,
    ) -> dict[str, object]:
        if not self.settings.alpaca_crypto_strategy_enabled:
            return self._remember({"enabled": False, "status": "disabled"})
        if self.settings.runtime_mode == "live" and not self.settings.alpaca_crypto_strategy_live_enabled:
            return self._remember({
                "enabled": True,
                "status": "blocked",
                "reason": "alpaca_crypto_strategy_live_enabled_is_false",
            })
        if not market_open:
            return self._remember({
                "enabled": True,
                "status": "market_closed",
                "strategy_version": self.settings.alpaca_crypto_strategy_version,
                "asset_class": self.asset_class,
                "observed_at": observed_at.isoformat(),
            })

        symbols = _allowed_symbols(self.settings)
        quote_health = _quote_health(
            quotes=quotes,
            symbols=symbols,
            observed_at=observed_at,
            max_age_seconds=self.settings.alpaca_crypto_strategy_quote_max_age_seconds,
            max_spread_bps=self.settings.alpaca_crypto_strategy_max_spread_bps,
        )
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=list(symbols),
            exchange=self.exchange,
        )
        with self.session_factory() as db:
            risk = _update_risk_state(
                settings=self.settings,
                db=db,
                snapshot=snapshot,
                observed_at=observed_at,
            )
            states = _load_strategy_states(settings=self.settings, db=db, symbols=symbols)
            _refresh_strategy_owned_quantities(
                settings=self.settings,
                db=db,
                states=states,
                snapshot=snapshot,
                asset_class=self.asset_class,
                order_origin=self.order_origin,
            )
            protective_actions = self._run_protective_exits(
                db=db,
                adapter=adapter,
                snapshot=snapshot,
                quotes=quotes,
                quote_health=quote_health,
                states=states,
                observed_at=observed_at,
            )
            decision_result = self._run_daily_decision_if_due(
                db=db,
                adapter=adapter,
                access_token=access_token,
                snapshot=snapshot,
                quotes=quotes,
                quote_health=quote_health,
                states=states,
                risk=risk,
                observed_at=observed_at,
            )
            db.commit()

        status = "healthy"
        reasons: list[str] = []
        if quote_health["blocked_symbols"]:
            status = "degraded"
            reasons.append("quotes_not_actionable")
        if risk.risk_status != "healthy":
            status = "risk_blocked"
            reasons.extend(str(reason) for reason in (risk.risk_reasons or []))
        return self._remember({
            "enabled": True,
            "status": status,
            "strategy_version": self.settings.alpaca_crypto_strategy_version,
            "asset_class": self.asset_class,
            "mode": self.settings.runtime_mode,
            "observed_at": observed_at.isoformat(),
            "quote_health": quote_health,
            "risk": _risk_payload(risk),
            "protective_actions": protective_actions,
            "daily_decision": decision_result,
            "reasons": list(dict.fromkeys(reasons)),
        })

    def _run_protective_exits(
        self,
        *,
        db: Session,
        adapter: AlpacaBrokerAdapter,
        snapshot: BrokerAccountSnapshot,
        quotes: dict[str, BrokerLiveMarketContext],
        quote_health: dict[str, object],
        states: dict[str, CryptoStrategyState],
        observed_at: datetime,
    ) -> list[dict[str, object]]:
        holdings = _holdings_by_symbol(snapshot)
        open_symbols = {order.symbol for order in snapshot.open_orders}
        blocked_symbols = set(quote_health["blocked_symbols"])
        actions: list[dict[str, object]] = []
        for symbol, state in states.items():
            quote = quotes.get(symbol)
            holding = holdings.get(symbol)
            broker_base_quantity = _holding_base_quantity(holding)
            owned_base_quantity = _strategy_owned_base_quantity(state)
            base_quantity = min(broker_base_quantity, owned_base_quantity)
            stop = _decimal_or_none(state.protective_stop_price)
            bid = quote.bid if quote is not None else None
            if (
                broker_base_quantity <= 0
                or owned_base_quantity <= 0
                or stop is None
                or bid is None
                or bid > stop
                or symbol in blocked_symbols
                or symbol in open_symbols
            ):
                continue
            increment = _state_quantity_increment(state)
            if increment is None:
                actions.append({"symbol": symbol, "action": "blocked", "reason": "trade_increment_unavailable"})
                continue
            sell_quantity = _floor_to_increment(base_quantity, increment)
            if sell_quantity <= 0:
                actions.append({"symbol": symbol, "action": "blocked", "reason": "below_trade_increment"})
                continue
            idempotency_key = _order_key(
                version=self.settings.alpaca_crypto_strategy_version,
                decision_date=observed_at.date(),
                symbol=symbol,
                action="protective-sell",
            )
            result = place_broker_order(
                settings=self.settings,
                db=db,
                adapter=adapter,
                command=_sell_command(
                    symbol=symbol,
                    base_quantity=sell_quantity,
                    idempotency_key=idempotency_key,
                    metadata={
                        "strategy_version": self.settings.alpaca_crypto_strategy_version,
                        "order_origin": self.order_origin,
                        "autonomous_strategy_order": True,
                        "decision_type": "protective_exit",
                        "protective_stop_price": _decimal_text(stop),
                        "trigger_bid": _decimal_text(bid),
                        "risk_reduction": True,
                        "strategy_owned_base_quantity": _decimal_text(owned_base_quantity),
                        "broker_base_quantity": _decimal_text(broker_base_quantity),
                    },
                    asset_class=self.asset_class,
                    exchange=self.exchange,
                    time_in_force=self.time_in_force,
                ),
                request_source=f"{self.order_origin}:protective_exit",
            )
            outcome = _order_outcome(result.status_code, result.payload)
            state.last_action = "protective_sell"
            state.last_action_at = observed_at
            actions.append({
                "symbol": symbol,
                "action": "protective_sell",
                "quantity": _decimal_text(sell_quantity),
                "stop_price": _decimal_text(stop),
                "trigger_bid": _decimal_text(bid),
                "order": outcome,
            })
        return actions

    def _run_daily_decision_if_due(
        self,
        *,
        db: Session,
        adapter: AlpacaBrokerAdapter,
        access_token: str,
        snapshot: BrokerAccountSnapshot,
        quotes: dict[str, BrokerLiveMarketContext],
        quote_health: dict[str, object],
        states: dict[str, CryptoStrategyState],
        risk: CryptoStrategyRiskState,
        observed_at: datetime,
    ) -> dict[str, object]:
        if not _after_decision_time(self.settings, observed_at):
            return {"status": "not_due", "next_decision_after": _decision_time_text(self.settings)}

        symbols = tuple(states)
        start = observed_at - timedelta(days=self.settings.alpaca_crypto_strategy_history_days)
        raw_bars = adapter.get_crypto_daily_bars(
            access_token,
            symbols=list(symbols),
            start=start,
            end=observed_at,
        )
        bars_by_symbol = _normalize_and_persist_bars(
            db=db,
            raw_bars=raw_bars,
            observed_at=observed_at,
            source=self.bar_source,
        )
        features = {
            symbol: evaluate_symbol(symbol, bars_by_symbol.get(symbol, ()), as_of=observed_at)
            for symbol in symbols
        }
        if any(feature.latest_bar_date is None for feature in features.values()):
            return {"status": "blocked", "reason": "closed_daily_bars_unavailable"}
        latest_dates = {feature.latest_bar_date for feature in features.values()}
        if len(latest_dates) != 1:
            return {
                "status": "blocked",
                "reason": "symbols_do_not_share_latest_closed_bar",
                "latest_bar_dates": sorted(item.isoformat() for item in latest_dates if item is not None),
            }
        decision_date = next(iter(latest_dates))
        assert decision_date is not None
        decision_bar_at = datetime.combine(decision_date, datetime.min.time(), tzinfo=UTC)
        if all(_decision_exists(db=db, settings=self.settings, symbol=symbol, bar_at=decision_bar_at) for symbol in symbols):
            return {"status": "already_processed", "decision_bar_at": decision_bar_at.isoformat()}

        allocation = allocate_inverse_volatility(
            features.values(),
            total_exposure=self.settings.alpaca_crypto_strategy_max_invested_fraction,
            per_asset_cap=Decimal("0.40"),
            cash_floor=Decimal("1") - self.settings.alpaca_crypto_strategy_max_invested_fraction,
        )
        target_weights = _volatility_scaled_targets(
            allocation=allocation,
            features=features,
            target_volatility=self.settings.alpaca_crypto_strategy_target_volatility,
        )
        assets = adapter.list_active_crypto_assets()
        increments = _asset_increments(assets)
        holdings = _holdings_by_symbol(snapshot)
        open_symbols = {order.symbol for order in snapshot.open_orders}
        equity = snapshot.total_asset_value or Decimal("0")
        strategy_capital = min(self.settings.alpaca_crypto_strategy_sleeve_usd, max(equity, Decimal("0")))
        target_weights = _risk_capped_targets(
            settings=self.settings,
            features=features,
            target_weights=target_weights,
            account_equity=equity,
            strategy_capital=strategy_capital,
        )
        available_cash = max(snapshot.available_cash or Decimal("0"), Decimal("0"))
        minimum_delta = max(
            self.settings.alpaca_crypto_strategy_min_order_notional_usd,
            strategy_capital * self.settings.alpaca_crypto_strategy_min_rebalance_fraction,
        )
        actions: list[dict[str, object]] = []
        all_terminal = True
        blocked_symbols = set(quote_health["blocked_symbols"])
        for symbol in symbols:
            if _decision_exists(db=db, settings=self.settings, symbol=symbol, bar_at=decision_bar_at):
                actions.append({"symbol": symbol, "status": "already_processed"})
                continue
            if symbol in blocked_symbols or symbol not in quotes:
                all_terminal = False
                actions.append({"symbol": symbol, "status": "waiting", "reason": "quote_not_actionable"})
                continue
            feature = features[symbol]
            quote = quotes[symbol]
            target_weight = target_weights.get(symbol, Decimal("0"))
            holding = holdings.get(symbol)
            broker_base_quantity = _holding_base_quantity(holding)
            owned_base_quantity = _strategy_owned_base_quantity(states[symbol])
            base_quantity = min(broker_base_quantity, owned_base_quantity)
            current_value = base_quantity * (quote.bid or Decimal("0"))
            target_value = strategy_capital * target_weight
            delta = target_value - current_value
            state = states[symbol]
            increment = increments.get(symbol)
            if increment is None or increment <= 0:
                all_terminal = False
                actions.append({"symbol": symbol, "status": "waiting", "reason": "trade_increment_unavailable"})
                continue
            state.eligible = feature.eligible
            state.target_fraction = target_weight
            state.metrics = {
                **_feature_payload(feature),
                "quantity_increment": _decimal_text(increment),
                "strategy_capital": _decimal_text(strategy_capital),
            }
            _update_protective_stop(
                settings=self.settings,
                state=state,
                feature=feature,
                has_position=owned_base_quantity > 0 or (feature.eligible and target_weight > 0),
                entry_reference_price=(
                    quote.ask
                    if owned_base_quantity <= 0 and feature.eligible and target_weight > 0
                    else None
                ),
            )

            action = "hold"
            status = "no_action"
            reasons = list(feature.reasons)
            order_key: str | None = None
            requested_base: Decimal | None = None
            requested_notional: Decimal | None = None
            order_payload: dict[str, object] | None = None

            if symbol in open_symbols:
                all_terminal = False
                actions.append({"symbol": symbol, "status": "waiting", "reason": "unresolved_open_order"})
                continue

            force_exit = not feature.eligible and owned_base_quantity > 0
            if (delta <= -minimum_delta or force_exit) and base_quantity > 0:
                requested_base = _floor_to_increment(
                    base_quantity if force_exit else min(base_quantity, (-delta) / quote.bid),
                    increment,
                )
                if requested_base > 0:
                    action = "sell"
                    order_key = _order_key(
                        version=self.settings.alpaca_crypto_strategy_version,
                        decision_date=decision_date,
                        symbol=symbol,
                        action="daily-sell",
                    )
                    result = place_broker_order(
                        settings=self.settings,
                        db=db,
                        adapter=adapter,
                        command=_sell_command(
                            symbol=symbol,
                            base_quantity=requested_base,
                            idempotency_key=order_key,
                            metadata=_daily_order_metadata(
                                self.settings.alpaca_crypto_strategy_version,
                                feature,
                                target_weight,
                                decision_bar_at,
                                order_origin=self.order_origin,
                                owned_base_quantity=owned_base_quantity,
                                broker_base_quantity=broker_base_quantity,
                                force_exit=force_exit,
                            ),
                            asset_class=self.asset_class,
                            exchange=self.exchange,
                            time_in_force=self.time_in_force,
                        ),
                        request_source=f"{self.order_origin}:daily_rebalance",
                    )
                    order_payload = _order_outcome(result.status_code, result.payload)
                    status = _decision_status(result.status_code, result.payload)
            elif delta >= minimum_delta and feature.eligible:
                if risk.risk_status != "healthy":
                    action = "buy"
                    status = "blocked_risk"
                    reasons.extend(str(item) for item in (risk.risk_reasons or []))
                else:
                    requested_notional = min(
                        delta,
                        available_cash,
                        self.settings.alpaca_crypto_max_order_notional_usd,
                    ).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                    if requested_notional >= self.settings.alpaca_crypto_strategy_min_order_notional_usd:
                        action = "buy"
                        order_key = _order_key(
                            version=self.settings.alpaca_crypto_strategy_version,
                            decision_date=decision_date,
                            symbol=symbol,
                            action="daily-buy",
                        )
                        result = place_broker_order(
                            settings=self.settings,
                            db=db,
                            adapter=adapter,
                            command=_buy_command(
                                symbol=symbol,
                                quote_notional=requested_notional,
                                idempotency_key=order_key,
                                metadata=_daily_order_metadata(
                                    self.settings.alpaca_crypto_strategy_version,
                                    feature,
                                    target_weight,
                                    decision_bar_at,
                                    order_origin=self.order_origin,
                                    owned_base_quantity=owned_base_quantity,
                                    broker_base_quantity=broker_base_quantity,
                                ),
                                asset_class=self.asset_class,
                                exchange=self.exchange,
                                time_in_force=self.time_in_force,
                            ),
                            request_source=f"{self.order_origin}:daily_rebalance",
                        )
                        order_payload = _order_outcome(result.status_code, result.payload)
                        status = _decision_status(result.status_code, result.payload)
                        if status in {"submitted", "duplicate"}:
                            available_cash -= requested_notional

            decision = CryptoStrategyDecision(
                mode=self.settings.runtime_mode,
                provider=PROVIDER,
                strategy_version=self.settings.alpaca_crypto_strategy_version,
                symbol=symbol,
                decision_bar_at=decision_bar_at,
                decided_at=observed_at,
                action=action,
                decision_status=status,
                eligible=feature.eligible,
                target_fraction=target_weight,
                reference_price=quote.ask if action == "buy" else quote.bid,
                requested_base_quantity=requested_base,
                requested_quote_notional=requested_notional,
                high_water_price=state.high_water_price,
                protective_stop_price=state.protective_stop_price,
                idempotency_key=order_key,
                reasons=reasons,
                metrics=_feature_payload(feature),
                risk_context=_risk_payload(risk),
                payload=order_payload,
            )
            db.add(decision)
            state.last_processed_bar_at = decision_bar_at
            state.last_action = action
            state.last_action_at = observed_at
            actions.append({
                "symbol": symbol,
                "action": action,
                "status": status,
                "target_fraction": _decimal_text(target_weight),
                "current_value": _decimal_text(current_value),
                "target_value": _decimal_text(target_value),
                "strategy_owned_base_quantity": _decimal_text(owned_base_quantity),
                "broker_base_quantity": _decimal_text(broker_base_quantity),
                "requested_base_quantity": _decimal_text(requested_base),
                "requested_quote_notional": _decimal_text(requested_notional),
                "order": order_payload,
            })
            all_terminal = all_terminal and status in TERMINAL_DECISION_STATUSES

        return {
            "status": "processed" if all_terminal else "partial",
            "decision_bar_at": decision_bar_at.isoformat(),
            "strategy_capital": _decimal_text(strategy_capital),
            "allocation": {
                "invested_fraction": _decimal_text(sum(target_weights.values(), Decimal("0"))),
                "cash_fraction": _decimal_text(Decimal("1") - sum(target_weights.values(), Decimal("0"))),
                "targets": {symbol: _decimal_text(weight) for symbol, weight in target_weights.items()},
            },
            "actions": actions,
        }

    def _remember(self, payload: dict[str, object]) -> dict[str, object]:
        self._last_result = payload
        return payload


def _normalize_and_persist_bars(
    *,
    db: Session,
    raw_bars: dict[str, list[dict[str, Any]]],
    observed_at: datetime,
    source: str = "alpaca_crypto_bars_v1beta3",
) -> dict[str, tuple[DailyBar, ...]]:
    normalized: dict[str, tuple[DailyBar, ...]] = {}
    for symbol, payloads in raw_bars.items():
        daily_rows: list[DailyBar] = []
        for payload in payloads:
            timestamp = _parse_timestamp(payload.get("t") or payload.get("timestamp"))
            if timestamp is None or timestamp.date() >= observed_at.astimezone(UTC).date():
                continue
            try:
                daily = DailyBar(
                    symbol=symbol,
                    bar_date=timestamp.date(),
                    open=_required_decimal(payload.get("o") or payload.get("open")),
                    high=_required_decimal(payload.get("h") or payload.get("high")),
                    low=_required_decimal(payload.get("l") or payload.get("low")),
                    close=_required_decimal(payload.get("c") or payload.get("close")),
                    volume=_decimal_or_none(payload.get("v") or payload.get("volume")) or Decimal("0"),
                )
            except (ValueError, InvalidOperation):
                continue
            daily_rows.append(daily)
            existing = db.scalar(
                select(CryptoDailyBar).where(
                    CryptoDailyBar.provider == PROVIDER,
                    CryptoDailyBar.symbol == daily.symbol,
                    CryptoDailyBar.timeframe == "1Day",
                    CryptoDailyBar.bar_open_at == timestamp,
                )
            )
            if existing is None:
                existing = CryptoDailyBar(
                    provider=PROVIDER,
                    symbol=daily.symbol,
                    timeframe="1Day",
                    bar_open_at=timestamp,
                    bar_close_at=timestamp + timedelta(days=1),
                    open_price=daily.open,
                    high_price=daily.high,
                    low_price=daily.low,
                    close_price=daily.close,
                    volume=daily.volume,
                    source=source,
                )
                db.add(existing)
            existing.open_price = daily.open
            existing.high_price = daily.high
            existing.low_price = daily.low
            existing.close_price = daily.close
            existing.volume = daily.volume
            existing.trade_count = _int_or_none(payload.get("n") or payload.get("trade_count"))
            existing.vwap = _decimal_or_none(payload.get("vw") or payload.get("vwap"))
            existing.raw_payload = payload
        normalized[symbol] = tuple(sorted(daily_rows, key=lambda item: item.bar_date))
    return normalized


def _load_strategy_states(
    *,
    settings: Settings,
    db: Session,
    symbols: tuple[str, ...],
) -> dict[str, CryptoStrategyState]:
    existing = {
        state.symbol: state
        for state in db.scalars(
            select(CryptoStrategyState).where(
                CryptoStrategyState.mode == settings.runtime_mode,
                CryptoStrategyState.provider == PROVIDER,
                CryptoStrategyState.strategy_version == settings.alpaca_crypto_strategy_version,
                CryptoStrategyState.symbol.in_(symbols),
            )
        ).all()
    }
    for symbol in symbols:
        if symbol not in existing:
            state = CryptoStrategyState(
                mode=settings.runtime_mode,
                provider=PROVIDER,
                strategy_version=settings.alpaca_crypto_strategy_version,
                symbol=symbol,
                eligible=False,
                target_fraction=Decimal("0"),
            )
            db.add(state)
            existing[symbol] = state
    db.flush()
    return {symbol: existing[symbol] for symbol in symbols}


def _refresh_strategy_owned_quantities(
    *,
    settings: Settings,
    db: Session,
    states: dict[str, CryptoStrategyState],
    snapshot: BrokerAccountSnapshot,
    asset_class: str,
    order_origin: str,
) -> None:
    symbols = tuple(states)
    if not symbols:
        return
    orders = db.scalars(
        select(BrokerOrder)
        .options(selectinload(BrokerOrder.fills))
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == PROVIDER,
            BrokerOrder.asset_class == asset_class,
            BrokerOrder.symbol.in_(symbols),
        )
    ).all()
    totals = {symbol: Decimal("0") for symbol in symbols}
    for order in orders:
        if not _is_strategy_attributable_order(order=order, order_origin=order_origin):
            continue
        if order.side == "buy":
            direction = Decimal("1")
        elif order.side == "sell":
            direction = Decimal("-1")
        else:
            direction = Decimal("0")
        if direction == 0:
            continue
        for fill in order.fills:
            base_quantity = Decimal(fill.base_quantity or 0)
            if base_quantity <= 0:
                continue
            totals[order.symbol] += direction * base_quantity
    holdings = _holdings_by_symbol(snapshot)
    for symbol, state in states.items():
        ledger_quantity = max(totals.get(symbol, Decimal("0")), Decimal("0"))
        broker_quantity = _holding_base_quantity(holdings.get(symbol))
        state.strategy_owned_base_quantity = min(ledger_quantity, broker_quantity)


def _is_strategy_attributable_order(*, order: BrokerOrder, order_origin: str) -> bool:
    request_metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    source = str(request_metadata.get("source") or "")
    return source == order_origin or source.startswith(f"{order_origin}:")


def _update_risk_state(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
    observed_at: datetime,
) -> CryptoStrategyRiskState:
    state = db.scalar(
        select(CryptoStrategyRiskState).where(
            CryptoStrategyRiskState.mode == settings.runtime_mode,
            CryptoStrategyRiskState.provider == PROVIDER,
            CryptoStrategyRiskState.strategy_version == settings.alpaca_crypto_strategy_version,
        )
    )
    if state is None:
        state = CryptoStrategyRiskState(
            mode=settings.runtime_mode,
            provider=PROVIDER,
            strategy_version=settings.alpaca_crypto_strategy_version,
        )
        db.add(state)

    equity = snapshot.total_asset_value
    if equity is None or equity <= 0:
        state.risk_status = "blocked_entries"
        state.risk_reasons = ["account_equity_unavailable"]
        state.current_equity = equity
        state.current_equity_at = observed_at
        return state

    daily_date = observed_at.astimezone(UTC).date()
    week_start = daily_date - timedelta(days=daily_date.weekday())
    if state.daily_baseline_at is None or _utc_date(state.daily_baseline_at) != daily_date:
        state.daily_baseline_equity = equity
        state.daily_baseline_at = observed_at
    if state.weekly_baseline_at is None or _utc_date(state.weekly_baseline_at) != week_start:
        state.weekly_baseline_equity = equity
        state.weekly_baseline_at = datetime.combine(week_start, datetime.min.time(), tzinfo=UTC)
    if state.peak_equity is None or equity > state.peak_equity:
        state.peak_equity = equity
        state.peak_equity_at = observed_at
    state.current_equity = equity
    state.current_equity_at = observed_at

    daily_drawdown = _drawdown_fraction(state.daily_baseline_equity, equity)
    weekly_drawdown = _drawdown_fraction(state.weekly_baseline_equity, equity)
    peak_drawdown = _drawdown_fraction(state.peak_equity, equity)
    reasons: list[str] = []
    if daily_drawdown >= settings.alpaca_crypto_strategy_daily_drawdown_fraction:
        reasons.append("daily_drawdown_limit")
    if weekly_drawdown >= settings.alpaca_crypto_strategy_weekly_drawdown_fraction:
        reasons.append("weekly_drawdown_limit")
    if peak_drawdown >= settings.alpaca_crypto_strategy_peak_drawdown_fraction:
        reasons.append("peak_drawdown_limit")
    state.risk_status = "blocked_entries" if reasons else "healthy"
    state.risk_reasons = reasons
    state.metrics = {
        "daily_drawdown_fraction": _decimal_text(daily_drawdown),
        "weekly_drawdown_fraction": _decimal_text(weekly_drawdown),
        "peak_drawdown_fraction": _decimal_text(peak_drawdown),
    }
    return state


def _update_protective_stop(
    *,
    settings: Settings,
    state: CryptoStrategyState,
    feature: TrendFeatures,
    has_position: bool,
    entry_reference_price: Decimal | None = None,
) -> None:
    if not has_position or feature.latest_close is None or feature.atr14 is None:
        if not has_position:
            state.high_water_price = None
            state.protective_stop_price = None
        return
    reference_price = max(feature.latest_close, entry_reference_price or feature.latest_close)
    high_water = max(state.high_water_price or reference_price, reference_price)
    atr_distance = (settings.alpaca_crypto_strategy_stop_atr_multiple * feature.atr14) / reference_price
    distance = min(atr_distance, settings.alpaca_crypto_strategy_max_stop_distance_fraction)
    proposed = high_water * (Decimal("1") - distance)
    state.high_water_price = high_water
    state.protective_stop_price = max(state.protective_stop_price or proposed, proposed)


def _volatility_scaled_targets(
    *,
    allocation: PortfolioAllocation,
    features: dict[str, TrendFeatures],
    target_volatility: Decimal,
) -> dict[str, Decimal]:
    weights = {target.symbol: target.target_weight for target in allocation.targets}
    conservative_volatility = sum(
        weight * (features[symbol].realized_volatility_30d or Decimal("0"))
        for symbol, weight in weights.items()
    )
    scale = min(Decimal("1"), target_volatility / conservative_volatility) if conservative_volatility > 0 else Decimal("0")
    result: dict[str, Decimal] = {}
    for symbol, weight in weights.items():
        cap = Decimal("0.30") if symbol == "ETH/USD" else Decimal("0.40")
        result[symbol] = min(weight * scale, cap).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    return result


def _risk_capped_targets(
    *,
    settings: Settings,
    features: dict[str, TrendFeatures],
    target_weights: dict[str, Decimal],
    account_equity: Decimal,
    strategy_capital: Decimal,
) -> dict[str, Decimal]:
    if account_equity <= 0 or strategy_capital <= 0:
        return {symbol: Decimal("0") for symbol in target_weights}
    risk_budget = account_equity * settings.alpaca_crypto_strategy_max_position_risk_fraction
    capped: dict[str, Decimal] = {}
    for symbol, target_weight in target_weights.items():
        feature = features[symbol]
        if (
            target_weight <= 0
            or feature.latest_close is None
            or feature.latest_close <= 0
            or feature.atr14 is None
        ):
            capped[symbol] = Decimal("0")
            continue
        atr_fraction = (
            settings.alpaca_crypto_strategy_stop_atr_multiple * feature.atr14
        ) / feature.latest_close
        stop_fraction = min(
            atr_fraction,
            settings.alpaca_crypto_strategy_max_stop_distance_fraction,
        )
        if stop_fraction <= 0:
            capped[symbol] = Decimal("0")
            continue
        max_position_value = risk_budget / stop_fraction
        max_weight = max_position_value / strategy_capital
        capped[symbol] = min(target_weight, max_weight).quantize(
            Decimal("0.000001"),
            rounding=ROUND_DOWN,
        )
    return capped


def _quote_health(
    *,
    quotes: dict[str, BrokerLiveMarketContext],
    symbols: tuple[str, ...],
    observed_at: datetime,
    max_age_seconds: int,
    max_spread_bps: Decimal,
) -> dict[str, object]:
    details: dict[str, dict[str, object]] = {}
    blocked: list[str] = []
    for symbol in symbols:
        quote = quotes.get(symbol)
        reasons: list[str] = []
        if quote is None:
            reasons.append("missing_quote")
            age = None
        else:
            quote_observed_at = _as_utc(quote.observed_at)
            age = max(
                Decimal("0"),
                Decimal(str((_as_utc(observed_at) - quote_observed_at).total_seconds())),
            )
            if age > max_age_seconds:
                reasons.append("stale_quote")
            if quote.bid is None or quote.ask is None or quote.bid <= 0 or quote.ask <= 0:
                reasons.append("nonpositive_quote")
            elif quote.ask < quote.bid:
                reasons.append("crossed_quote")
            if quote.spread_bps is None or quote.spread_bps > max_spread_bps:
                reasons.append("spread_too_wide")
        if reasons:
            blocked.append(symbol)
        details[symbol] = {
            "actionable": not reasons,
            "reasons": reasons,
            "age_seconds": _decimal_text(age),
            "bid": _decimal_text(quote.bid if quote else None),
            "ask": _decimal_text(quote.ask if quote else None),
            "spread_bps": _decimal_text(quote.spread_bps if quote else None),
            "observed_at": quote.observed_at.isoformat() if quote else None,
        }
    return {"blocked_symbols": blocked, "symbols": details}


def _asset_increments(assets: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for asset in assets:
        symbol = str(asset.get("symbol") or "").strip().upper().replace("-", "/")
        increment = _decimal_or_none(asset.get("min_trade_increment") or asset.get("min_order_size"))
        if symbol and increment is not None and increment > 0:
            result[symbol] = increment
    return result


def _state_quantity_increment(state: CryptoStrategyState) -> Decimal | None:
    metrics = state.metrics if isinstance(state.metrics, dict) else {}
    increment = _decimal_or_none(metrics.get("quantity_increment"))
    return increment if increment is not None and increment > 0 else None


def _feature_payload(feature: TrendFeatures) -> dict[str, object]:
    payload = asdict(feature)
    return {
        key: (
            value.isoformat()
            if isinstance(value, date)
            else _decimal_text(value)
            if isinstance(value, Decimal)
            else list(value)
            if isinstance(value, tuple)
            else value
        )
        for key, value in payload.items()
    }


def _daily_order_metadata(
    strategy_version: str,
    feature: TrendFeatures,
    target_weight: Decimal,
    decision_bar_at: datetime,
    *,
    order_origin: str,
    owned_base_quantity: Decimal,
    broker_base_quantity: Decimal,
    force_exit: bool = False,
) -> dict[str, object]:
    return {
        "strategy_version": strategy_version,
        "order_origin": order_origin,
        "autonomous_strategy_order": True,
        "decision_type": "daily_target_rebalance",
        "decision_bar_at": decision_bar_at.isoformat(),
        "target_fraction": _decimal_text(target_weight),
        "strategy_owned_base_quantity": _decimal_text(owned_base_quantity),
        "broker_base_quantity": _decimal_text(broker_base_quantity),
        "force_exit": force_exit,
        "signal": _feature_payload(feature),
    }


def _buy_command(
    *,
    symbol: str,
    quote_notional: Decimal,
    idempotency_key: str,
    metadata: dict[str, object],
    asset_class: str = "crypto",
    exchange: str = EXCHANGE,
    time_in_force: str = "gtc",
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        idempotency_key=idempotency_key,
        provider=PROVIDER,
        asset_class=asset_class,
        venue=PROVIDER,
        symbol=symbol,
        side="buy",
        quote_notional=quote_notional,
        order_type="market",
        time_in_force=time_in_force,
        exchange=exchange,
        metadata=metadata,
    )


def _sell_command(
    *,
    symbol: str,
    base_quantity: Decimal,
    idempotency_key: str,
    metadata: dict[str, object],
    asset_class: str = "crypto",
    exchange: str = EXCHANGE,
    time_in_force: str = "gtc",
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        idempotency_key=idempotency_key,
        provider=PROVIDER,
        asset_class=asset_class,
        venue=PROVIDER,
        symbol=symbol,
        side="sell",
        base_quantity=base_quantity,
        order_type="market",
        time_in_force=time_in_force,
        exchange=exchange,
        metadata=metadata,
    )


def _decision_exists(*, db: Session, settings: Settings, symbol: str, bar_at: datetime) -> bool:
    return db.scalar(
        select(CryptoStrategyDecision.id).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            CryptoStrategyDecision.strategy_version == settings.alpaca_crypto_strategy_version,
            CryptoStrategyDecision.symbol == symbol,
            CryptoStrategyDecision.decision_bar_at == bar_at,
        )
    ) is not None


def _holdings_by_symbol(snapshot: BrokerAccountSnapshot) -> dict[str, BrokerAccountHolding]:
    return {holding.symbol: holding for holding in snapshot.holdings}


def _holding_base_quantity(holding: BrokerAccountHolding | None) -> Decimal:
    if holding is None or not isinstance(holding.payload, dict):
        return Decimal("0")
    return _decimal_or_none(holding.payload.get("base_quantity")) or Decimal("0")


def _strategy_owned_base_quantity(state: CryptoStrategyState) -> Decimal:
    return max(Decimal(state.strategy_owned_base_quantity or 0), Decimal("0"))


def _floor_to_increment(value: Decimal, increment: Decimal) -> Decimal:
    if value <= 0 or increment <= 0:
        return Decimal("0")
    return (value / increment).to_integral_value(rounding=ROUND_DOWN) * increment


def _drawdown_fraction(baseline: Decimal | None, current: Decimal) -> Decimal:
    if baseline is None or baseline <= 0 or current >= baseline:
        return Decimal("0")
    return (baseline - current) / baseline


def _after_decision_time(settings: Settings, observed_at: datetime) -> bool:
    utc = observed_at.astimezone(UTC)
    return (utc.hour, utc.minute) >= (
        settings.alpaca_crypto_strategy_decision_hour_utc,
        settings.alpaca_crypto_strategy_decision_minute_utc,
    )


def _decision_time_text(settings: Settings) -> str:
    return (
        f"{settings.alpaca_crypto_strategy_decision_hour_utc:02d}:"
        f"{settings.alpaca_crypto_strategy_decision_minute_utc:02d} UTC"
    )


def _order_key(*, version: str, decision_date: date, symbol: str, action: str) -> str:
    version_slug = "".join(character for character in version.lower() if character.isalnum())[:24]
    symbol_slug = "".join(character for character in symbol.lower() if character.isalnum())[:16]
    action_slug = "".join(character for character in action.lower() if character.isalnum())[:24]
    return f"{version_slug}-{decision_date:%Y%m%d}-{symbol_slug}-{action_slug}"[:128]


def _order_outcome(status_code: int, payload: dict[str, object]) -> dict[str, object]:
    order = payload.get("order") if isinstance(payload.get("order"), dict) else {}
    return {
        "status_code": status_code,
        "duplicate": bool(payload.get("duplicate")),
        "message": payload.get("message"),
        "order_id": order.get("id"),
        "broker_order_id": order.get("broker_order_id"),
        "status": order.get("status"),
    }


def _decision_status(status_code: int, payload: dict[str, object]) -> str:
    if payload.get("duplicate"):
        return "duplicate"
    if 200 <= status_code < 300:
        return "submitted"
    return "rejected"


def _risk_payload(state: CryptoStrategyRiskState) -> dict[str, object]:
    return {
        "status": state.risk_status,
        "reasons": list(state.risk_reasons or []),
        "current_equity": _decimal_text(state.current_equity),
        "daily_baseline_equity": _decimal_text(state.daily_baseline_equity),
        "weekly_baseline_equity": _decimal_text(state.weekly_baseline_equity),
        "peak_equity": _decimal_text(state.peak_equity),
        "metrics": dict(state.metrics or {}),
    }


def _allowed_symbols(settings: Settings) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            item.strip().upper().replace("-", "/")
            for item in settings.alpaca_crypto_allowed_symbols.split(",")
            if item.strip()
        )
    )


def _required_decimal(value: object) -> Decimal:
    parsed = _decimal_or_none(value)
    if parsed is None:
        raise ValueError("decimal value is required")
    return parsed


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _int_or_none(value: object) -> int | None:
    try:
        return None if value is None or value == "" else int(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _utc_date(value: datetime) -> date:
    return value.replace(tzinfo=UTC).date() if value.tzinfo is None else value.astimezone(UTC).date()


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
