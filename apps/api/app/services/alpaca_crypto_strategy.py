from __future__ import annotations

import hmac
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from hashlib import sha256
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerLiveMarketContext,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    CryptoDailyBar,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
    RuntimeRemediationEvent,
)
from app.services.broker_orders import place_broker_order
from app.services.alpaca_legacy_strategy_recovery import (
    has_active_alpaca_us_legacy_strategy_recovery,
    legacy_strategy_authority_fingerprint,
    legacy_strategy_authority_orders,
    recovered_alpaca_us_strategy_inventory,
)
from app.services.crypto_trend_strategy import (
    DailyBar,
    PortfolioAllocation,
    TrendFeatures,
    allocate_inverse_volatility,
    evaluate_symbol,
)
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.fifo_inventory import FifoRouteScope, fifo_route_scope_from_order
from app.services.monitoring_checkpoint_scope import exact_monitoring_execution_scope
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
)


PROVIDER = "alpaca"
EXCHANGE = "ALPACA"
TERMINAL_DECISION_STATUSES = frozenset(
    {"no_action", "submitted", "duplicate", "rejected"}
)
ALPACA_TRUSTED_QUOTE_FRESHNESS_SOURCES = frozenset(
    {"alpaca_exchange_quote_time"}
)
ALPACA_QUOTE_MAX_FUTURE_SKEW_SECONDS = 5
ALPACA_ACCOUNT_SNAPSHOT_MAX_AGE_SECONDS = 300
PROTECTIVE_EXIT_TERMINAL_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.CANCELED.value,
        NormalizedBrokerOrderStatus.REJECTED.value,
        NormalizedBrokerOrderStatus.FAILED.value,
        NormalizedBrokerOrderStatus.EXPIRED.value,
    }
)
PROTECTIVE_EXIT_ACTIVE_ACK_STATUSES = frozenset(
    {
        "accepted",
        "new",
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
    }
)
ALPACA_ACTIVE_OPEN_ORDER_STATUSES = frozenset(
    {"accepted", "new", "open", "partially_filled", "pending_new"}
)
DAILY_DATA_INTEGRITY_BLOCK_REASONS = frozenset(
    {
        "closed_daily_bars_unavailable",
        "symbols_do_not_share_latest_closed_bar",
        "daily_bars_stale",
        "broker_open_orders_truth_unavailable",
    }
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
        self.strategy_family = _strategy_family(
            asset_class=asset_class,
            exchange=exchange,
        )
        if str(asset_class).strip().lower() == "us_equity":
            self.annualization_days = (
                settings.alpaca_us_equity_strategy_annualization_days
            )
            self.max_closed_bar_age_days = (
                settings.alpaca_us_equity_strategy_max_closed_bar_age_days
            )
        else:
            self.annualization_days = (
                settings.alpaca_crypto_strategy_annualization_days
            )
            self.max_closed_bar_age_days = (
                settings.alpaca_crypto_strategy_max_closed_bar_age_days
            )
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
        execution_scope: object,
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
        symbols = _allowed_symbols(self.settings)
        coordinator_scope = exact_monitoring_execution_scope(
            execution_scope,
            provider=PROVIDER,
        )
        if coordinator_scope is None:
            return self._remember({
                "enabled": True,
                "status": "blocked",
                "reason": "authenticated_execution_scope_unavailable",
            })
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=list(symbols),
            exchange=self.exchange,
        )
        try:
            snapshot_identity = build_authenticated_broker_position_identity(
                settings=self.settings,
                provider=PROVIDER,
                exchange=self.exchange,
                broker_account_id=str(snapshot.broker_account_id or ""),
            )
        except PositionIdentityConflictError:
            return self._remember({
                "enabled": True,
                "status": "blocked",
                "reason": "authenticated_execution_scope_unavailable",
            })
        account_scope = str(coordinator_scope["account_scope"])
        if not hmac.compare_digest(account_scope, snapshot_identity.account_scope):
            return self._remember({
                "enabled": True,
                "status": "blocked",
                "reason": "authenticated_execution_scope_mismatch",
            })
        snapshot_integrity_reason = _snapshot_inventory_integrity_reason(snapshot)
        if snapshot_integrity_reason is not None:
            return self._remember({
                "enabled": True,
                "status": "blocked",
                "reason": snapshot_integrity_reason,
            })
        if not market_open:
            return self._remember({
                "enabled": True,
                "status": "market_closed",
                "strategy_version": self.settings.alpaca_crypto_strategy_version,
                "strategy_family": self.strategy_family,
                "asset_class": self.asset_class,
                "observed_at": observed_at.isoformat(),
            })
        quote_health = _quote_health(
            quotes=quotes,
            symbols=symbols,
            observed_at=observed_at,
            max_age_seconds=self.settings.alpaca_crypto_strategy_quote_max_age_seconds,
            max_spread_bps=self.settings.alpaca_crypto_strategy_max_spread_bps,
            expected_exchange=self.exchange,
        )
        route_scope = FifoRouteScope(
            provider=PROVIDER,
            account_scope=account_scope,
            venue=PROVIDER,
            asset_class=self.asset_class,
            exchange=self.exchange,
        )
        with self.session_factory() as db:
            legacy_strategy_authority = _has_legacy_strategy_authority(
                settings=self.settings,
                db=db,
                snapshot=snapshot,
                observed_at=observed_at,
                symbols=symbols,
                asset_class=self.asset_class,
                order_origin=self.order_origin,
                strategy_family=self.strategy_family,
                route_scope=route_scope,
            )
            states = _load_strategy_states(
                settings=self.settings,
                db=db,
                symbols=symbols,
                account_scope=account_scope,
                strategy_family=self.strategy_family,
                create_missing=not legacy_strategy_authority,
            )
            inventory_integrity_reason = _refresh_strategy_owned_quantities(
                settings=self.settings,
                db=db,
                states=states,
                snapshot=snapshot,
                asset_class=self.asset_class,
                order_origin=self.order_origin,
                strategy_family=self.strategy_family,
                route_scope=route_scope,
                legacy_strategy_authority=legacy_strategy_authority,
                observed_at=observed_at,
            )
            if inventory_integrity_reason is not None:
                db.rollback()
                return self._remember({
                    "enabled": True,
                    "status": "blocked",
                    "reason": inventory_integrity_reason,
                    "strategy_version": self.settings.alpaca_crypto_strategy_version,
                    "strategy_family": self.strategy_family,
                    "asset_class": self.asset_class,
                    "mode": self.settings.runtime_mode,
                    "observed_at": observed_at.isoformat(),
                    "quote_health": quote_health,
                    "protective_actions": [],
                    "daily_decision": {
                        "status": "blocked",
                        "reason": inventory_integrity_reason,
                    },
                })
            protective_actions = self._run_protective_exits(
                db=db,
                adapter=adapter,
                snapshot=snapshot,
                quotes=quotes,
                quote_health=quote_health,
                states=states,
                observed_at=observed_at,
                route_scope=route_scope,
            )
            if legacy_strategy_authority:
                db.commit()
                return self._remember({
                    "enabled": True,
                    "status": "blocked",
                    "reason": "legacy_strategy_scope_requires_manual_review",
                    "strategy_version": self.settings.alpaca_crypto_strategy_version,
                    "strategy_family": self.strategy_family,
                    "asset_class": self.asset_class,
                    "mode": self.settings.runtime_mode,
                    "observed_at": observed_at.isoformat(),
                    "quote_health": quote_health,
                    "protective_actions": protective_actions,
                    "daily_decision": {
                        "status": "blocked",
                        "reason": "legacy_strategy_scope_requires_manual_review",
                    },
                })
            risk = _update_risk_state(
                settings=self.settings,
                db=db,
                snapshot=snapshot,
                observed_at=observed_at,
                account_scope=account_scope,
                strategy_family=self.strategy_family,
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
                account_scope=account_scope,
                route_scope=route_scope,
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
        daily_decision_reason = decision_result.get("reason")
        if daily_decision_reason in DAILY_DATA_INTEGRITY_BLOCK_REASONS:
            status = "degraded"
            reasons.append(str(daily_decision_reason))
        rebalance_attention_reason = _daily_rebalance_attention_reason(
            decision_result
        )
        if rebalance_attention_reason is not None:
            status = "degraded"
            reasons.append(rebalance_attention_reason)
        if any(_protective_action_requires_attention(action) for action in protective_actions):
            status = "degraded"
            reasons.append("protection_blocked")
        return self._remember({
            "enabled": True,
            "status": status,
            "strategy_version": self.settings.alpaca_crypto_strategy_version,
            "strategy_family": self.strategy_family,
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
        route_scope: FifoRouteScope,
    ) -> list[dict[str, object]]:
        holdings = _holdings_by_symbol(snapshot)
        open_orders_by_symbol: dict[str, list[BrokerAccountOpenOrder]] = {}
        for order in snapshot.open_orders:
            open_orders_by_symbol.setdefault(order.symbol, []).append(order)
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
            ):
                continue
            if bid is None or symbol in blocked_symbols:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "protective_sell_blocked",
                        "reason": "quote_not_actionable",
                    }
                )
                continue
            if bid > stop:
                continue
            if not _open_orders_coverage_is_complete(snapshot):
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "protective_sell_blocked",
                        "reason": "open_orders_coverage_degraded",
                    }
                )
                continue
            symbol_open_orders = open_orders_by_symbol.get(symbol, [])
            conflicting_sides = {
                str(order.side or "").strip().lower()
                for order in symbol_open_orders
                if str(order.side or "").strip().lower() != "sell"
            }
            if conflicting_sides:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "protective_sell_blocked",
                        "reason": (
                            "conflicting_open_buy"
                            if "buy" in conflicting_sides
                            else "conflicting_open_order"
                        ),
                    }
                )
                continue
            increment = _state_quantity_increment(state)
            if increment is None:
                actions.append({
                    "symbol": symbol,
                    "action": "protective_sell_blocked",
                    "reason": "trade_increment_unavailable",
                })
                continue
            desired_sell_quantity = _floor_to_increment(base_quantity, increment)
            if desired_sell_quantity <= 0:
                actions.append({
                    "symbol": symbol,
                    "action": "protective_sell_blocked",
                    "reason": "below_trade_increment",
                })
                continue
            active_coverage, matched_protective_order_ids = (
                _verified_active_protective_sell_coverage(
                    settings=self.settings,
                    db=db,
                    symbol=symbol,
                    open_orders=symbol_open_orders,
                    desired_stop=stop,
                    order_origin=self.order_origin,
                    strategy_family=self.strategy_family,
                    route_scope=route_scope,
                )
            )
            sell_quantity = _floor_to_increment(
                max(desired_sell_quantity - active_coverage, Decimal("0")),
                increment,
            )
            if sell_quantity <= 0:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "protective_sell_active",
                        "reason": "verified_active_protective_sell",
                        "covered_quantity": _decimal_text(active_coverage),
                    }
                )
                continue
            idempotency_key = _protective_exit_order_key(
                settings=self.settings,
                db=db,
                symbol=symbol,
                observed_at=observed_at,
                order_origin=self.order_origin,
                strategy_family=self.strategy_family,
                route_scope=route_scope,
                advance_after_order_ids=matched_protective_order_ids,
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
                        "strategy_family": self.strategy_family,
                        "order_origin": self.order_origin,
                        "expected_execution_scope": _execution_scope_payload(route_scope),
                        "autonomous_strategy_order": True,
                        "decision_type": "protective_exit",
                        "protective_stop_price": _decimal_text(stop),
                        "trigger_bid": _decimal_text(bid),
                        "risk_reduction": True,
                        "strategy_owned_base_quantity": _decimal_text(owned_base_quantity),
                        "broker_base_quantity": _decimal_text(broker_base_quantity),
                        "active_protective_base_quantity": _decimal_text(active_coverage),
                        "desired_protective_base_quantity": _decimal_text(
                            desired_sell_quantity
                        ),
                    },
                    asset_class=self.asset_class,
                    exchange=self.exchange,
                    time_in_force=self.time_in_force,
                ),
                request_source=f"{self.order_origin}:protective_exit",
            )
            outcome = _order_outcome(result.status_code, result.payload)
            action_payload: dict[str, object] = {
                "symbol": symbol,
                "quantity": _decimal_text(sell_quantity),
                "stop_price": _decimal_text(stop),
                "trigger_bid": _decimal_text(bid),
                "order": outcome,
            }
            if _protective_order_acknowledged(result.status_code, result.payload):
                state.last_action = "protective_sell"
                state.last_action_at = observed_at
                action_payload["action"] = "protective_sell"
            else:
                action_payload.update(
                    {
                        "action": (
                            "protective_sell_failed"
                            if result.status_code >= 500
                            else "protective_sell_blocked"
                        ),
                        "reason": _protective_order_failure_reason(
                            result.status_code,
                            result.payload,
                        ),
                        "status_code": result.status_code,
                    }
                )
            actions.append(action_payload)
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
        account_scope: str,
        route_scope: FifoRouteScope,
    ) -> dict[str, object]:
        if not _after_decision_time(self.settings, observed_at):
            return {"status": "not_due", "next_decision_after": _decision_time_text(self.settings)}
        if not snapshot.open_orders_complete:
            return {
                "status": "blocked",
                "reason": "broker_open_orders_truth_unavailable",
            }

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
            symbol: evaluate_symbol(
                symbol,
                bars_by_symbol.get(symbol, ()),
                as_of=observed_at,
                annualization_days=self.annualization_days,
            )
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
        if not _latest_closed_bar_is_fresh(
            latest_bar_date=decision_date,
            observed_at=observed_at,
            asset_class=self.asset_class,
            max_age_days=self.max_closed_bar_age_days,
        ):
            return {
                "status": "blocked",
                "reason": "daily_bars_stale",
                "latest_bar_date": decision_date.isoformat(),
                "max_age_days": self.max_closed_bar_age_days,
            }
        decision_bar_at = datetime.combine(decision_date, datetime.min.time(), tzinfo=UTC)
        if all(
            _decision_exists(
                db=db,
                settings=self.settings,
                symbol=symbol,
                bar_at=decision_bar_at,
                account_scope=account_scope,
                strategy_family=self.strategy_family,
            )
            for symbol in symbols
        ):
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
            if _decision_exists(
                db=db,
                settings=self.settings,
                symbol=symbol,
                bar_at=decision_bar_at,
                account_scope=account_scope,
                strategy_family=self.strategy_family,
            ):
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
            prior_state_metrics = (
                state.metrics if isinstance(state.metrics, dict) else {}
            )
            state.metrics = {
                **prior_state_metrics,
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
                    order_key = _daily_rebalance_order_key(
                        settings=self.settings,
                        db=db,
                        decision_date=decision_date,
                        decision_bar_at=decision_bar_at,
                        symbol=symbol,
                        action="daily-sell",
                        order_origin=self.order_origin,
                        strategy_family=self.strategy_family,
                        route_scope=route_scope,
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
                                strategy_family=self.strategy_family,
                                route_scope=route_scope,
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
                        order_key = _daily_rebalance_order_key(
                            settings=self.settings,
                            db=db,
                            decision_date=decision_date,
                            decision_bar_at=decision_bar_at,
                            symbol=symbol,
                            action="daily-buy",
                            order_origin=self.order_origin,
                            strategy_family=self.strategy_family,
                            route_scope=route_scope,
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
                                    strategy_family=self.strategy_family,
                                    route_scope=route_scope,
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

            decision = _load_strategy_decision(
                db=db,
                settings=self.settings,
                symbol=symbol,
                bar_at=decision_bar_at,
                account_scope=account_scope,
                strategy_family=self.strategy_family,
            )
            if decision is None:
                decision = CryptoStrategyDecision(
                    mode=self.settings.runtime_mode,
                    provider=PROVIDER,
                    account_scope=account_scope,
                    strategy_family=self.strategy_family,
                    strategy_version=self.settings.alpaca_crypto_strategy_version,
                    symbol=symbol,
                    decision_bar_at=decision_bar_at,
                    decided_at=observed_at,
                    action=action,
                    decision_status=status,
                    eligible=feature.eligible,
                    target_fraction=target_weight,
                )
                db.add(decision)
            decision.decided_at = observed_at
            decision.action = action
            decision.decision_status = status
            decision.eligible = feature.eligible
            decision.target_fraction = target_weight
            decision.reference_price = quote.ask if action == "buy" else quote.bid
            decision.requested_base_quantity = requested_base
            decision.requested_quote_notional = requested_notional
            decision.high_water_price = state.high_water_price
            decision.protective_stop_price = state.protective_stop_price
            decision.idempotency_key = order_key
            decision.reasons = reasons
            decision.metrics = _feature_payload(feature)
            decision.risk_context = _risk_payload(risk)
            decision.payload = order_payload
            if status in TERMINAL_DECISION_STATUSES:
                state.last_processed_bar_at = decision_bar_at
                if status in {"no_action", "submitted", "duplicate"}:
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
    account_scope: str,
    strategy_family: str,
    create_missing: bool,
) -> dict[str, CryptoStrategyState]:
    existing = {
        state.symbol: state
        for state in db.scalars(
            select(CryptoStrategyState).where(
                CryptoStrategyState.mode == settings.runtime_mode,
                CryptoStrategyState.provider == PROVIDER,
                CryptoStrategyState.account_scope == account_scope,
                CryptoStrategyState.strategy_family == strategy_family,
                CryptoStrategyState.strategy_version == settings.alpaca_crypto_strategy_version,
                CryptoStrategyState.symbol.in_(symbols),
            )
        ).all()
    }
    if create_missing:
        for symbol in symbols:
            if symbol in existing:
                continue
            state = CryptoStrategyState(
                mode=settings.runtime_mode,
                provider=PROVIDER,
                account_scope=account_scope,
                strategy_family=strategy_family,
                strategy_version=settings.alpaca_crypto_strategy_version,
                symbol=symbol,
                eligible=False,
                target_fraction=Decimal("0"),
            )
            db.add(state)
            existing[symbol] = state
    db.flush()
    return {
        symbol: existing[symbol]
        for symbol in symbols
        if symbol in existing
    }


def _has_legacy_strategy_authority(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
    observed_at: datetime,
    symbols: tuple[str, ...],
    asset_class: str,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> bool:
    # A strategy-version bump cannot restore the account/family attribution
    # missing from quarantined rows, so legacy authority remains version-agnostic.
    legacy_states = db.scalars(
        select(CryptoStrategyState).where(
            CryptoStrategyState.mode == settings.runtime_mode,
            CryptoStrategyState.provider == PROVIDER,
            or_(
                CryptoStrategyState.account_scope.is_(None),
                CryptoStrategyState.strategy_family.is_(None),
            ),
            CryptoStrategyState.symbol.in_(symbols),
        )
    ).all()
    legacy_decisions = db.scalars(
        select(CryptoStrategyDecision).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            or_(
                CryptoStrategyDecision.account_scope.is_(None),
                CryptoStrategyDecision.strategy_family.is_(None),
            ),
            CryptoStrategyDecision.symbol.in_(symbols),
        )
    ).all()
    relevant_legacy_versions = {
        *(row.strategy_version for row in legacy_states),
        *(row.strategy_version for row in legacy_decisions),
    }
    risk_query = select(CryptoStrategyRiskState).where(
        CryptoStrategyRiskState.mode == settings.runtime_mode,
        CryptoStrategyRiskState.provider == PROVIDER,
        or_(
            CryptoStrategyRiskState.account_scope.is_(None),
            CryptoStrategyRiskState.strategy_family.is_(None),
        ),
    )
    if relevant_legacy_versions:
        risk_query = risk_query.where(
            CryptoStrategyRiskState.strategy_version.in_(
                relevant_legacy_versions
            )
        )
    legacy_risk_states = db.scalars(risk_query).all()

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
    legacy_orders = legacy_strategy_authority_orders(
        orders=orders,
        order_origin=order_origin,
        strategy_family=strategy_family,
        route_scope=route_scope,
    )

    if not (
        legacy_states
        or legacy_risk_states
        or legacy_decisions
        or legacy_orders
    ):
        return False

    evidence_fingerprint = legacy_strategy_authority_fingerprint(
        states=legacy_states,
        risk_states=legacy_risk_states,
        decisions=legacy_decisions,
        orders=legacy_orders,
    )
    candidate_key = _legacy_strategy_authority_candidate_key(
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
        evidence_fingerprint=evidence_fingerprint,
    )
    retired = db.scalar(
        select(RuntimeRemediationEvent.id).where(
            RuntimeRemediationEvent.mode == settings.runtime_mode,
            RuntimeRemediationEvent.candidate_type
            == "legacy_strategy_authority",
            RuntimeRemediationEvent.candidate_key == candidate_key,
            RuntimeRemediationEvent.outcome
            == "verified_flat_authority_retired",
        )
    )
    if retired is not None:
        return False

    if has_active_alpaca_us_legacy_strategy_recovery(
        settings=settings,
        db=db,
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
        symbols=symbols,
        authority_fingerprint=evidence_fingerprint,
    ):
        return False

    if not _broker_truth_proves_legacy_strategy_flat(
        snapshot=snapshot,
        observed_at=observed_at,
        symbols=symbols,
        legacy_orders=legacy_orders,
    ):
        return True

    db.add(
        RuntimeRemediationEvent(
            mode=settings.runtime_mode,
            candidate_type="legacy_strategy_authority",
            candidate_key=candidate_key,
            subject_id=route_scope.account_scope,
            outcome="verified_flat_authority_retired",
            actor="alpaca_strategy_recovery",
            reason=(
                "complete authenticated broker truth proved every strategy "
                "symbol flat with no open orders"
            ),
            evidence_summary={
                "evidence_fingerprint": evidence_fingerprint,
                "provider": route_scope.provider,
                "account_scope": route_scope.account_scope,
                "strategy_family": strategy_family,
                "symbols": sorted(symbols),
                "broker_snapshot_observed_at": snapshot.observed_at.isoformat(),
                "holdings_complete": snapshot.holdings_complete,
                "open_orders_complete": snapshot.open_orders_complete,
                "legacy_state_ids": sorted(row.id for row in legacy_states),
                "legacy_risk_state_ids": sorted(
                    row.id for row in legacy_risk_states
                ),
                "legacy_decision_ids": sorted(
                    row.id for row in legacy_decisions
                ),
                "legacy_order_ids": sorted(row.id for row in legacy_orders),
            },
        )
    )
    db.flush()
    return False


def _broker_truth_proves_legacy_strategy_flat(
    *,
    snapshot: BrokerAccountSnapshot,
    observed_at: datetime,
    symbols: tuple[str, ...],
    legacy_orders: list[BrokerOrder],
) -> bool:
    snapshot_age_seconds = (
        _as_utc(observed_at) - _as_utc(snapshot.observed_at)
    ).total_seconds()
    if not (
        -ALPACA_QUOTE_MAX_FUTURE_SKEW_SECONDS
        <= snapshot_age_seconds
        <= ALPACA_ACCOUNT_SNAPSHOT_MAX_AGE_SECONDS
    ):
        return False
    if not snapshot.holdings_complete or not snapshot.open_orders_complete:
        return False
    if not _open_orders_coverage_is_complete(snapshot):
        return False
    canonical_symbols = {_canonical_snapshot_symbol(symbol) for symbol in symbols}
    holdings = _holdings_by_symbol(snapshot)
    if any(
        _holding_base_quantity(holdings.get(symbol)) != 0
        for symbol in canonical_symbols
    ):
        return False
    if any(
        _canonical_snapshot_symbol(order.symbol) in canonical_symbols
        for order in snapshot.open_orders
    ):
        return False
    return all(
        str(order.status or "").strip().lower()
        in PROTECTIVE_EXIT_TERMINAL_STATUSES
        for order in legacy_orders
    )


def _legacy_strategy_authority_candidate_key(
    *,
    account_scope: str,
    strategy_family: str,
    evidence_fingerprint: str,
) -> str:
    scope_digest = sha256(
        f"{account_scope}|{strategy_family}".encode("utf-8")
    ).hexdigest()[:24]
    return f"{scope_digest}:{evidence_fingerprint}"


def _refresh_strategy_owned_quantities(
    *,
    settings: Settings,
    db: Session,
    states: dict[str, CryptoStrategyState],
    snapshot: BrokerAccountSnapshot,
    asset_class: str,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
    legacy_strategy_authority: bool,
    observed_at: datetime,
) -> str | None:
    symbols = tuple(states)
    if not symbols:
        return None
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
    recovered_quantities, recovery_reason = (
        recovered_alpaca_us_strategy_inventory(
            settings=settings,
            db=db,
            states=states,
            snapshot=snapshot,
            orders=orders,
            route_scope=route_scope,
            order_origin=order_origin,
            strategy_family=strategy_family,
            authority_released=not legacy_strategy_authority,
            observed_at=observed_at,
        )
    )
    if recovery_reason is not None:
        return recovery_reason
    if recovered_quantities is not None:
        for symbol, state in states.items():
            state.strategy_owned_base_quantity = recovered_quantities[symbol]
        return None
    totals = {symbol: Decimal("0") for symbol in symbols}
    for order in orders:
        if not _is_strategy_attributable_order(
            order=order,
            order_origin=order_origin,
            strategy_family=strategy_family,
            route_scope=route_scope,
        ):
            continue
        if order.side == "buy":
            direction = Decimal("1")
        elif order.side == "sell":
            direction = Decimal("-1")
        else:
            direction = Decimal("0")
        if direction == 0:
            continue
        for fill in dedupe_execution_fills(order.fills):
            base_quantity = Decimal(fill.base_quantity or 0)
            if base_quantity <= 0:
                continue
            totals[order.symbol] += direction * base_quantity
    holdings = _holdings_by_symbol(snapshot)
    for symbol, state in states.items():
        ledger_quantity = max(totals.get(symbol, Decimal("0")), Decimal("0"))
        broker_quantity = _holding_base_quantity(holdings.get(symbol))
        state.strategy_owned_base_quantity = min(ledger_quantity, broker_quantity)
    return None


def _is_strategy_attributable_order(
    *,
    order: BrokerOrder,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> bool:
    request_metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    source = str(request_metadata.get("source") or "")
    command_metadata = request_metadata.get("metadata")
    command_metadata = command_metadata if isinstance(command_metadata, dict) else {}
    return (
        source == order_origin or source.startswith(f"{order_origin}:")
    ) and (
        command_metadata.get("strategy_family") == strategy_family
        and command_metadata.get("expected_execution_scope")
        == _execution_scope_payload(route_scope)
        and fifo_route_scope_from_order(order) == route_scope
    )


def _update_risk_state(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
    observed_at: datetime,
    account_scope: str,
    strategy_family: str,
) -> CryptoStrategyRiskState:
    state = db.scalar(
        select(CryptoStrategyRiskState).where(
            CryptoStrategyRiskState.mode == settings.runtime_mode,
            CryptoStrategyRiskState.provider == PROVIDER,
            CryptoStrategyRiskState.account_scope == account_scope,
            CryptoStrategyRiskState.strategy_family == strategy_family,
            CryptoStrategyRiskState.strategy_version == settings.alpaca_crypto_strategy_version,
        )
    )
    is_new_state = state is None
    if state is None:
        state = CryptoStrategyRiskState(
            mode=settings.runtime_mode,
            provider=PROVIDER,
            account_scope=account_scope,
            strategy_family=strategy_family,
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

    previous_equity = _authenticated_previous_equity(
        snapshot=snapshot,
        observed_at=observed_at,
    )
    daily_date = observed_at.astimezone(UTC).date()
    week_start = daily_date - timedelta(days=daily_date.weekday())
    if state.daily_baseline_at is None or _utc_date(state.daily_baseline_at) != daily_date:
        state.daily_baseline_equity = previous_equity
        state.daily_baseline_at = datetime.combine(
            daily_date,
            datetime.min.time(),
            tzinfo=UTC,
        )
    if state.weekly_baseline_at is None or _utc_date(state.weekly_baseline_at) != week_start:
        state.weekly_baseline_at = datetime.combine(
            week_start,
            datetime.min.time(),
            tzinfo=UTC,
        )
        state.weekly_baseline_equity = (
            previous_equity if daily_date == week_start else None
        )
    peak_candidate = max(
        equity,
        previous_equity or equity,
    )
    if state.peak_equity is None or peak_candidate > state.peak_equity:
        state.peak_equity = peak_candidate
        state.peak_equity_at = observed_at
    state.current_equity = equity
    state.current_equity_at = observed_at

    daily_drawdown = _drawdown_fraction(state.daily_baseline_equity, equity)
    weekly_drawdown = _drawdown_fraction(state.weekly_baseline_equity, equity)
    peak_drawdown = _drawdown_fraction(state.peak_equity, equity)
    reasons: list[str] = []
    if state.daily_baseline_equity is None:
        reasons.append("daily_baseline_warmup")
    if state.weekly_baseline_equity is None:
        reasons.append("weekly_baseline_warmup")
    if daily_drawdown >= settings.alpaca_crypto_strategy_daily_drawdown_fraction:
        reasons.append("daily_drawdown_limit")
    if weekly_drawdown >= settings.alpaca_crypto_strategy_weekly_drawdown_fraction:
        reasons.append("weekly_drawdown_limit")
    if peak_drawdown >= settings.alpaca_crypto_strategy_peak_drawdown_fraction:
        reasons.append("peak_drawdown_limit")
    state.risk_status = "blocked_entries" if reasons else "healthy"
    state.risk_reasons = reasons
    prior_metrics = state.metrics if isinstance(state.metrics, dict) else {}
    state.metrics = {
        **prior_metrics,
        "daily_drawdown_fraction": _decimal_text(daily_drawdown),
        "weekly_drawdown_fraction": _decimal_text(weekly_drawdown),
        "peak_drawdown_fraction": _decimal_text(peak_drawdown),
        "daily_baseline_source": (
            "authenticated_previous_equity"
            if state.daily_baseline_equity is not None
            else "warmup"
        ),
        "weekly_baseline_source": (
            "authenticated_previous_equity"
            if state.weekly_baseline_equity is not None
            else "warmup"
        ),
        "cold_start": is_new_state,
    }
    return state


def _authenticated_previous_equity(
    *,
    snapshot: BrokerAccountSnapshot,
    observed_at: datetime,
) -> Decimal | None:
    """Return broker session evidence only when it belongs to this live snapshot."""

    previous_equity = snapshot.previous_total_asset_value
    if (
        previous_equity is None
        or previous_equity <= 0
        or not str(snapshot.broker_account_id or "").strip()
        or not str(snapshot.source or "").strip()
    ):
        return None
    age_seconds = (_as_utc(observed_at) - _as_utc(snapshot.observed_at)).total_seconds()
    if not (
        -ALPACA_QUOTE_MAX_FUTURE_SKEW_SECONDS
        <= age_seconds
        <= ALPACA_ACCOUNT_SNAPSHOT_MAX_AGE_SECONDS
    ):
        return None
    return previous_equity


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
    max_future_skew_seconds: int = ALPACA_QUOTE_MAX_FUTURE_SKEW_SECONDS,
    expected_exchange: str | None = None,
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
            age = Decimal(
                str((_as_utc(observed_at) - quote_observed_at).total_seconds())
            )
            if quote.freshness_source not in ALPACA_TRUSTED_QUOTE_FRESHNESS_SOURCES:
                reasons.append("untrusted_quote_timestamp")
            if age < -Decimal(max_future_skew_seconds):
                reasons.append("future_quote_timestamp")
            elif age > max_age_seconds:
                reasons.append("stale_quote")
            if (
                expected_exchange is not None
                and str(quote.exchange or "").strip().upper()
                != str(expected_exchange).strip().upper()
            ):
                reasons.append("quote_exchange_mismatch")
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
    duplicate_symbols: set[str] = set()
    for asset in assets:
        symbol = _canonical_snapshot_symbol(str(asset.get("symbol") or ""))
        increment = _decimal_or_none(asset.get("min_trade_increment") or asset.get("min_order_size"))
        if not symbol or increment is None or increment <= 0:
            continue
        if symbol in result:
            duplicate_symbols.add(symbol)
            continue
        result[symbol] = increment
    for symbol in duplicate_symbols:
        result.pop(symbol, None)
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
    strategy_family: str,
    route_scope: FifoRouteScope,
    owned_base_quantity: Decimal,
    broker_base_quantity: Decimal,
    force_exit: bool = False,
) -> dict[str, object]:
    return {
        "strategy_version": strategy_version,
        "strategy_family": strategy_family,
        "order_origin": order_origin,
        "expected_execution_scope": _execution_scope_payload(route_scope),
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


def _decision_exists(
    *,
    db: Session,
    settings: Settings,
    symbol: str,
    bar_at: datetime,
    account_scope: str,
    strategy_family: str,
) -> bool:
    return db.scalar(
        select(CryptoStrategyDecision.id).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            CryptoStrategyDecision.account_scope == account_scope,
            CryptoStrategyDecision.strategy_family == strategy_family,
            CryptoStrategyDecision.strategy_version == settings.alpaca_crypto_strategy_version,
            CryptoStrategyDecision.symbol == symbol,
            CryptoStrategyDecision.decision_bar_at == bar_at,
            CryptoStrategyDecision.decision_status.in_(
                tuple(TERMINAL_DECISION_STATUSES)
            ),
        )
    ) is not None


def _load_strategy_decision(
    *,
    db: Session,
    settings: Settings,
    symbol: str,
    bar_at: datetime,
    account_scope: str,
    strategy_family: str,
) -> CryptoStrategyDecision | None:
    return db.scalar(
        select(CryptoStrategyDecision).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            CryptoStrategyDecision.account_scope == account_scope,
            CryptoStrategyDecision.strategy_family == strategy_family,
            CryptoStrategyDecision.strategy_version
            == settings.alpaca_crypto_strategy_version,
            CryptoStrategyDecision.symbol == symbol,
            CryptoStrategyDecision.decision_bar_at == bar_at,
        )
    )


def _snapshot_inventory_integrity_reason(
    snapshot: BrokerAccountSnapshot,
) -> str | None:
    if not snapshot.holdings_complete:
        return "broker_holdings_truth_unavailable"
    try:
        _holdings_by_symbol(snapshot)
    except ValueError as exc:
        return str(exc)
    return None


def _canonical_snapshot_symbol(value: str) -> str:
    normalized = value.strip().upper().replace("-", "/")
    if "/" not in normalized and normalized.endswith("USD") and len(normalized) > 3:
        return f"{normalized[:-3]}/USD"
    return normalized


def _holdings_by_symbol(snapshot: BrokerAccountSnapshot) -> dict[str, BrokerAccountHolding]:
    result: dict[str, BrokerAccountHolding] = {}
    for holding in snapshot.holdings:
        symbol = _canonical_snapshot_symbol(holding.symbol)
        if not symbol:
            raise ValueError("malformed_broker_holding_identity")
        if symbol in result:
            raise ValueError("duplicate_broker_holding_identity")
        result[symbol] = holding
    return result


def _holding_base_quantity(holding: BrokerAccountHolding | None) -> Decimal:
    if holding is None or not isinstance(holding.payload, dict):
        return Decimal("0")
    return _decimal_or_none(holding.payload.get("base_quantity")) or Decimal("0")


def _strategy_owned_base_quantity(state: CryptoStrategyState) -> Decimal:
    return max(Decimal(state.strategy_owned_base_quantity or 0), Decimal("0"))


def _open_orders_coverage_is_complete(snapshot: BrokerAccountSnapshot) -> bool:
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    error = payload.get("open_orders_error")
    return payload.get("open_orders_coverage") == "complete" and not (
        isinstance(error, dict) and error.get("degraded") is True
    )


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


def _latest_closed_bar_is_fresh(
    *,
    latest_bar_date: date,
    observed_at: datetime,
    asset_class: str,
    max_age_days: int,
) -> bool:
    normalized_asset_class = str(asset_class or "").strip().lower()
    if normalized_asset_class not in {"crypto", "us_equity"}:
        return False
    calendar_age_days = (
        _as_utc(observed_at).date() - latest_bar_date
    ).days
    return 1 <= calendar_age_days <= max_age_days


def _decision_time_text(settings: Settings) -> str:
    return (
        f"{settings.alpaca_crypto_strategy_decision_hour_utc:02d}:"
        f"{settings.alpaca_crypto_strategy_decision_minute_utc:02d} UTC"
    )


def _order_key(
    *,
    version: str,
    decision_date: date,
    symbol: str,
    action: str,
    account_scope: str,
    strategy_family: str,
) -> str:
    version_slug = "".join(character for character in version.lower() if character.isalnum())[:24]
    symbol_slug = "".join(character for character in symbol.lower() if character.isalnum())[:16]
    action_slug = "".join(character for character in action.lower() if character.isalnum())[:40]
    scope_digest = sha256(
        f"{account_scope}\0{strategy_family}".encode("utf-8")
    ).hexdigest()[:20]
    return (
        f"{version_slug}-{scope_digest}-{decision_date:%Y%m%d}-"
        f"{symbol_slug}-{action_slug}"
    )[:128]


def _protective_exit_order_key(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    observed_at: datetime,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
    advance_after_order_ids: tuple[str, ...] = (),
) -> str:
    protective_orders = _strategy_protective_orders(
        settings=settings,
        db=db,
        symbol=symbol,
        order_origin=order_origin,
        strategy_family=strategy_family,
        route_scope=route_scope,
    )
    unresolved = [
        order
        for order in protective_orders
        if str(order.status or "").strip().lower()
        not in PROTECTIVE_EXIT_TERMINAL_STATUSES
    ]
    generation_anchor_ids = frozenset(advance_after_order_ids)
    if generation_anchor_ids:
        unobserved_unresolved = [
            order for order in unresolved if str(order.id) not in generation_anchor_ids
        ]
        if unobserved_unresolved:
            return max(
                unobserved_unresolved,
                key=_protective_order_sort_key,
            ).idempotency_key
        prior = max(protective_orders, key=_protective_order_sort_key)
        generation_material = "\0".join(
            sorted(str(order.id) for order in protective_orders)
        )
        generation = sha256(generation_material.encode("utf-8")).hexdigest()[:16]
        anchor = prior.submitted_at or prior.created_at or observed_at
        return _order_key(
            version=settings.alpaca_crypto_strategy_version,
            decision_date=_as_utc(anchor).date(),
            symbol=symbol,
            action=f"protective-sell-supplement-{generation}",
            account_scope=route_scope.account_scope,
            strategy_family=strategy_family,
        )

    if unresolved:
        return max(unresolved, key=_protective_order_sort_key).idempotency_key

    if protective_orders:
        prior = max(protective_orders, key=_protective_order_sort_key)
        generation = sha256(str(prior.id).encode("utf-8")).hexdigest()[:16]
        anchor = prior.submitted_at or prior.created_at or observed_at
        return _order_key(
            version=settings.alpaca_crypto_strategy_version,
            decision_date=_as_utc(anchor).date(),
            symbol=symbol,
            action=f"protective-sell-{generation}",
            account_scope=route_scope.account_scope,
            strategy_family=strategy_family,
        )

    return _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=_as_utc(observed_at).date(),
        symbol=symbol,
        action="protective-sell",
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
    )


def _daily_rebalance_order_key(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    decision_date: date,
    decision_bar_at: datetime,
    action: str,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> str:
    base_key = _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=decision_date,
        symbol=symbol,
        action=action,
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
    )
    decision = _load_strategy_decision(
        db=db,
        settings=settings,
        symbol=symbol,
        bar_at=decision_bar_at,
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
    )
    if (
        decision is None
        or decision.decision_status != "retryable"
        or not decision.idempotency_key
    ):
        return base_key
    prior_key = decision.idempotency_key
    prior_order = db.scalar(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == prior_key,
        )
    )
    if prior_order is None:
        return prior_key
    if not _is_strategy_attributable_order(
        order=prior_order,
        order_origin=order_origin,
        strategy_family=strategy_family,
        route_scope=route_scope,
    ):
        return prior_key
    request_metadata = (
        prior_order.request_metadata
        if isinstance(prior_order.request_metadata, dict)
        else {}
    )
    command_metadata = request_metadata.get("metadata")
    command_metadata = command_metadata if isinstance(command_metadata, dict) else {}
    if (
        command_metadata.get("decision_type") != "daily_target_rebalance"
        or command_metadata.get("decision_bar_at") != decision_bar_at.isoformat()
    ):
        return prior_key
    prior_status = str(prior_order.status or "").strip().lower()
    if prior_status not in PROTECTIVE_EXIT_TERMINAL_STATUSES:
        return prior_key
    if prior_status == NormalizedBrokerOrderStatus.FILLED.value:
        return prior_key
    generation = sha256(str(prior_order.id).encode("utf-8")).hexdigest()[:16]
    return _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=decision_date,
        symbol=symbol,
        action=f"{action}-retry-{generation}",
        account_scope=route_scope.account_scope,
        strategy_family=strategy_family,
    )


def _strategy_protective_orders(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> list[BrokerOrder]:
    orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == PROVIDER,
            BrokerOrder.asset_class == route_scope.asset_class,
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "sell",
        )
    ).all()
    protective_orders: list[BrokerOrder] = []
    for order in orders:
        request_metadata = (
            order.request_metadata
            if isinstance(order.request_metadata, dict)
            else {}
        )
        command_metadata = request_metadata.get("metadata")
        if not isinstance(command_metadata, dict):
            continue
        if command_metadata.get("decision_type") != "protective_exit":
            continue
        if not _is_strategy_attributable_order(
            order=order,
            order_origin=order_origin,
            strategy_family=strategy_family,
            route_scope=route_scope,
        ):
            continue
        protective_orders.append(order)
    return protective_orders


def _verified_active_protective_sell_coverage(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    open_orders: list[BrokerAccountOpenOrder],
    desired_stop: Decimal,
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> tuple[Decimal, tuple[str, ...]]:
    exact_orders = _strategy_protective_orders(
        settings=settings,
        db=db,
        symbol=symbol,
        order_origin=order_origin,
        strategy_family=strategy_family,
        route_scope=route_scope,
    )
    by_idempotency_key = {
        order.idempotency_key: order for order in exact_orders
    }
    by_broker_order_id = {
        order.broker_order_id: order
        for order in exact_orders
        if order.broker_order_id
    }
    matched_order_ids: set[str] = set()
    counted_order_ids: set[str] = set()
    covered_quantity = Decimal("0")
    for open_order in open_orders:
        if str(open_order.side or "").strip().lower() != "sell":
            continue
        if (
            str(open_order.status or "").strip().lower()
            not in ALPACA_ACTIVE_OPEN_ORDER_STATUSES
        ):
            continue
        local_order = None
        if open_order.idempotency_key:
            local_order = by_idempotency_key.get(open_order.idempotency_key)
        if local_order is None and open_order.broker_order_id:
            local_order = by_broker_order_id.get(open_order.broker_order_id)
        if local_order is None:
            continue
        local_order_id = str(local_order.id)
        matched_order_ids.add(local_order_id)
        if local_order_id in counted_order_ids:
            continue
        if (
            str(local_order.status or "").strip().lower()
            not in PROTECTIVE_EXIT_ACTIVE_ACK_STATUSES
            or local_order.limit_price is not None
        ):
            continue
        request_metadata = (
            local_order.request_metadata
            if isinstance(local_order.request_metadata, dict)
            else {}
        )
        command_metadata = request_metadata.get("metadata")
        command_metadata = (
            command_metadata if isinstance(command_metadata, dict) else {}
        )
        protective_stop = _decimal_or_none(
            command_metadata.get("protective_stop_price")
        )
        trigger_bid = _decimal_or_none(command_metadata.get("trigger_bid"))
        open_payload = (
            open_order.payload if isinstance(open_order.payload, dict) else {}
        )
        remaining_quantity = _decimal_or_none(
            open_payload.get("remaining_base_quantity")
        )
        if (
            protective_stop is None
            or protective_stop < desired_stop
            or trigger_bid is None
            or trigger_bid > protective_stop
            or remaining_quantity is None
            or remaining_quantity <= 0
        ):
            continue
        counted_order_ids.add(local_order_id)
        covered_quantity += remaining_quantity
    return covered_quantity, tuple(sorted(matched_order_ids))


def _protective_order_sort_key(order: BrokerOrder) -> tuple[datetime, str]:
    observed_at = order.submitted_at or order.created_at
    return (
        _as_utc(observed_at) if observed_at is not None else datetime.min.replace(tzinfo=UTC),
        str(order.id),
    )


def _strategy_family(*, asset_class: str, exchange: str) -> str:
    normalized_asset_class = str(asset_class or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().upper()
    if not normalized_asset_class or not normalized_exchange:
        raise ValueError("strategy family requires asset class and exchange")
    value = f"{normalized_asset_class}:{normalized_exchange}"
    if len(value) > 64:
        raise ValueError("strategy family exceeds persistence limit")
    return value


def _execution_scope_payload(route_scope: FifoRouteScope) -> dict[str, object]:
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        **route_scope.as_payload(),
    }


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


def _protective_order_acknowledged(
    status_code: int,
    payload: dict[str, object],
) -> bool:
    if not 200 <= status_code < 300:
        return False
    order = payload.get("order")
    order = order if isinstance(order, dict) else {}
    status = str(order.get("status") or "").strip().lower()
    if bool(payload.get("duplicate")):
        return status in PROTECTIVE_EXIT_ACTIVE_ACK_STATUSES
    return (
        status in PROTECTIVE_EXIT_ACTIVE_ACK_STATUSES
        or status == NormalizedBrokerOrderStatus.FILLED.value
    )


def _protective_order_failure_reason(
    status_code: int,
    payload: dict[str, object],
) -> str:
    order = payload.get("order")
    order = order if isinstance(order, dict) else {}
    status = str(order.get("status") or "").strip().lower()
    if bool(payload.get("duplicate")):
        return (
            f"terminal_duplicate:{status}"
            if status in PROTECTIVE_EXIT_TERMINAL_STATUSES
            else "duplicate_not_active"
        )
    for key in ("block_reason", "rejection_reason"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if status_code >= 500:
        return "broker_order_submission_failed"
    return "broker_order_not_acknowledged"


def _protective_action_requires_attention(action: dict[str, object]) -> bool:
    return action.get("action") in {
        "protective_sell_blocked",
        "protective_sell_failed",
    }


def _daily_rebalance_attention_reason(
    decision_result: dict[str, object],
) -> str | None:
    actions = decision_result.get("actions")
    if not isinstance(actions, list):
        return None
    statuses = {
        str(action.get("status") or "")
        for action in actions
        if isinstance(action, dict)
    }
    if "retryable" in statuses:
        return "rebalance_retryable_failure"
    if "rejected" in statuses:
        return "rebalance_rejected"
    return None


def _decision_status(status_code: int, payload: dict[str, object]) -> str:
    order = payload.get("order")
    order = order if isinstance(order, dict) else {}
    order_status = str(order.get("status") or "").strip().lower()
    duplicate = bool(payload.get("duplicate"))
    acknowledged_statuses = {
        *PROTECTIVE_EXIT_ACTIVE_ACK_STATUSES,
        NormalizedBrokerOrderStatus.FILLED.value,
    }
    if 200 <= status_code < 300 and order_status in acknowledged_statuses:
        return "duplicate" if duplicate else "submitted"
    if payload.get("permanent_rejection") is True:
        return "rejected"
    return "retryable"


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
