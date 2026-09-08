from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.orm import Session, aliased, undefer

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
)
from app.services.fifo_inventory import load_fifo_isolation_cursor
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.fill_economics import (
    ClosedTradeEconomics,
    closed_trade_economics,
    effective_fill_quantity,
)
from app.services.account_drawdown_recovery import (
    POLICY_DISABLED_RELEASE_EVENT_TYPE,
    RECOVERABLE_LOSS_GATE_NAMES,
    RECOVERY_RELEASED_EVENT_TYPE,
    recovery_account_fingerprint,
)


BLOCK_EVENT_TYPE = "account_drawdown_new_buy_blocked"
US_EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class AccountDrawdownDecision:
    allowed: bool
    gate_name: str | None
    reason: str | None
    current_metric: Decimal | None
    threshold: Decimal | None
    baseline_value: Decimal | None
    baseline_timestamp: str | None
    sells_risk_reduction_allowed: bool
    summary: dict[str, object]
    payload: dict[str, object]


@dataclass(frozen=True)
class PeriodRealizedPnl:
    gross: Decimal | None
    covered_gross: Decimal
    known_transaction_cost: Decimal | None
    covered_loss_floor: Decimal | None
    net: Decimal | None
    transaction_cost_coverage: str
    pnl_coverage: str
    unmatched_sell_quantity: Decimal


def evaluate_account_drawdown_gate(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    order_idempotency_key: str,
    execution_plan_item_id: str | None,
    now: datetime | None = None,
) -> AccountDrawdownDecision:
    observed_at = now or datetime.now(UTC)
    metrics = _metrics(snapshot)
    baseline_timestamp = snapshot.observed_at.astimezone(UTC).isoformat()
    gates = [
        _daily_total_asset_pct_gate(settings=settings, metrics=metrics),
        _daily_total_asset_abs_gate(settings=settings, metrics=metrics),
        _weekly_total_asset_gate(settings=settings, metrics=metrics),
        _unrealized_abs_gate(settings=settings, metrics=metrics),
        _unrealized_pct_gate(settings=settings, metrics=metrics),
        _severe_holding_gate(settings=settings, snapshot=snapshot),
    ]
    blockers = [gate for gate in gates if gate is not None]
    if blockers:
        gate = blockers[0]
        summary = {
            "gate_name": gate["gate_name"],
            "reason": gate["reason"],
            "current_metric": _decimal_text(gate.get("current_metric")),
            "threshold": _decimal_text(gate.get("threshold")),
            "baseline_value": _decimal_text(gate.get("baseline_value")),
            "baseline_timestamp": gate.get("baseline_timestamp") or baseline_timestamp,
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "sells_risk_reduction_allowed": True,
        }
        return AccountDrawdownDecision(
            allowed=False,
            gate_name=str(gate["gate_name"]),
            reason=str(gate["reason"]),
            current_metric=_decimal_or_none(gate.get("current_metric")),
            threshold=_decimal_or_none(gate.get("threshold")),
            baseline_value=_decimal_or_none(gate.get("baseline_value")),
            baseline_timestamp=str(summary["baseline_timestamp"]),
            sells_risk_reduction_allowed=True,
            summary=summary,
            payload={
                "blocked": True,
                "evaluated_at": observed_at.isoformat(),
                "order_idempotency_key": order_idempotency_key,
                "execution_plan_item_id": execution_plan_item_id,
                "decision": summary,
                "all_blockers": [_json_safe(gate) for gate in blockers],
                "metrics": _json_safe(metrics),
                "raw_balance_payload": snapshot.payload.get("balance") if isinstance(snapshot.payload, dict) else None,
                "holdings": [_holding_payload(holding) for holding in snapshot.holdings],
            },
        )

    return AccountDrawdownDecision(
        allowed=True,
        gate_name=None,
        reason=None,
        current_metric=None,
        threshold=None,
        baseline_value=metrics.get("daily_baseline"),
        baseline_timestamp=baseline_timestamp,
        sells_risk_reduction_allowed=True,
        summary={
            "blocked": False,
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
        },
        payload={
            "blocked": False,
            "evaluated_at": observed_at.isoformat(),
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "metrics": _json_safe(metrics),
        },
    )


def evaluate_us_equity_account_gate(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
    order_notional: Decimal,
    order_idempotency_key: str,
    execution_plan_item_id: str | None,
    additional_open_buy_notional: Decimal = Decimal("0"),
    additional_open_buy_unknown_count: int = 0,
    now: datetime | None = None,
) -> AccountDrawdownDecision:
    observed_at = now or datetime.now(UTC)
    exposure = sum((_holding_market_value(holding) for holding in snapshot.holdings), Decimal("0"))
    open_buy_notional, unknown_open_buy_count = _open_buy_notional(snapshot)
    open_buy_notional += additional_open_buy_notional
    unknown_open_buy_count += additional_open_buy_unknown_count
    projected_exposure = exposure + open_buy_notional + order_notional
    daily_realized, weekly_realized = _us_realized_pnl(
        settings=settings,
        db=db,
        now=observed_at,
    )
    unrealized = snapshot.unrealized_pnl or sum(
        (holding.unrealized_pnl or Decimal("0") for holding in snapshot.holdings),
        Decimal("0"),
    )
    unrealized_loss_pct = (
        (-unrealized / exposure * Decimal("100")) if exposure > 0 and unrealized < 0 else Decimal("0")
    )
    worst_holding_loss_pct = max(
        (
            -(holding.unrealized_pnl_pct or Decimal("0"))
            for holding in snapshot.holdings
            if (holding.unrealized_pnl_pct or Decimal("0")) < 0
        ),
        default=Decimal("0"),
    )
    metrics = {
        "currency": "USD",
        "current_exposure_usd": exposure,
        "open_buy_notional_usd": open_buy_notional,
        "open_buy_notional_unknown_count": unknown_open_buy_count,
        "projected_exposure_usd": projected_exposure,
        "daily_realized_pnl_usd": daily_realized.net,
        "weekly_realized_pnl_usd": weekly_realized.net,
        "daily_gross_realized_pnl_usd": daily_realized.gross,
        "weekly_gross_realized_pnl_usd": weekly_realized.gross,
        "daily_covered_gross_realized_pnl_usd": daily_realized.covered_gross,
        "weekly_covered_gross_realized_pnl_usd": weekly_realized.covered_gross,
        "daily_known_transaction_cost_usd": daily_realized.known_transaction_cost,
        "weekly_known_transaction_cost_usd": weekly_realized.known_transaction_cost,
        "daily_covered_realized_pnl_loss_floor_usd": daily_realized.covered_loss_floor,
        "weekly_covered_realized_pnl_loss_floor_usd": weekly_realized.covered_loss_floor,
        "daily_transaction_cost_coverage": daily_realized.transaction_cost_coverage,
        "weekly_transaction_cost_coverage": weekly_realized.transaction_cost_coverage,
        "daily_realized_pnl_coverage": daily_realized.pnl_coverage,
        "weekly_realized_pnl_coverage": weekly_realized.pnl_coverage,
        "daily_unmatched_sell_quantity": daily_realized.unmatched_sell_quantity,
        "weekly_unmatched_sell_quantity": weekly_realized.unmatched_sell_quantity,
        "daily_realized_loss_gate_basis": (
            "net_after_transaction_costs"
            if daily_realized.net is not None
            else (
                "degraded_gross_minus_known_transaction_cost"
                if daily_realized.gross is not None
                else (
                    "covered_loss_floor_gross_minus_known_transaction_cost"
                    if daily_realized.covered_loss_floor is not None
                    else "unavailable_unknown_cost_basis_or_currency"
                )
            )
        ),
        "weekly_realized_loss_gate_basis": (
            "net_after_transaction_costs"
            if weekly_realized.net is not None
            else (
                "degraded_gross_minus_known_transaction_cost"
                if weekly_realized.gross is not None
                else (
                    "covered_loss_floor_gross_minus_known_transaction_cost"
                    if weekly_realized.covered_loss_floor is not None
                    else "unavailable_unknown_cost_basis_or_currency"
                )
            )
        ),
        "realized_pnl_source": "local_execution_fills_fifo_explicit_costs",
        "realized_pnl_coverage": (
            snapshot.payload.get("realized_pnl_coverage")
            if isinstance(snapshot.payload, dict)
            else "degraded_local_execution_fills_only"
        ) or "degraded_local_execution_fills_only",
        "unrealized_pnl_usd": unrealized,
        "unrealized_loss_pct": unrealized_loss_pct,
        "worst_holding_loss_pct": worst_holding_loss_pct,
    }
    realized_pnl_coverage = str(metrics["realized_pnl_coverage"])
    transaction_cost_coverage_complete = (
        daily_realized.transaction_cost_coverage == "complete"
        and weekly_realized.transaction_cost_coverage == "complete"
    )
    daily_loss_metric = _period_loss_metric(daily_realized)
    weekly_loss_metric = _period_loss_metric(weekly_realized)
    gates = [
        (
            "us_realized_pnl_coverage_unconfirmed",
            Decimal("1"),
            Decimal("0"),
            (
                realized_pnl_coverage != "broker_confirmed"
                and not settings.kis_us_allow_degraded_realized_pnl_coverage
            ),
            "KIS US-equity realized PnL coverage is not broker-confirmed",
        ),
        (
            "us_transaction_cost_coverage_unconfirmed",
            Decimal("1"),
            Decimal("0"),
            (
                not transaction_cost_coverage_complete
                and not settings.kis_us_allow_degraded_realized_pnl_coverage
            ),
            "KIS US-equity realized PnL transaction-cost coverage is incomplete",
        ),
        (
            "us_open_buy_notional_unknown",
            Decimal(unknown_open_buy_count),
            Decimal("0"),
            unknown_open_buy_count > 0,
            "KIS US-equity open buy notional could not be determined",
        ),
        (
            "us_total_exposure_usd",
            projected_exposure,
            settings.kis_us_max_total_exposure_usd,
            projected_exposure > settings.kis_us_max_total_exposure_usd,
            "projected KIS US-equity exposure exceeds the USD cap",
        ),
        (
            "us_daily_realized_loss_usd",
            -daily_loss_metric if daily_loss_metric is not None else None,
            settings.kis_us_daily_realized_loss_limit_usd,
            daily_loss_metric is not None
            and daily_loss_metric <= -settings.kis_us_daily_realized_loss_limit_usd,
            "KIS US-equity daily realized loss limit reached",
        ),
        (
            "us_weekly_realized_loss_usd",
            -weekly_loss_metric if weekly_loss_metric is not None else None,
            settings.kis_us_weekly_realized_loss_limit_usd,
            weekly_loss_metric is not None
            and weekly_loss_metric <= -settings.kis_us_weekly_realized_loss_limit_usd,
            "KIS US-equity weekly realized loss limit reached",
        ),
        (
            "us_unrealized_loss_usd",
            -unrealized,
            settings.kis_us_unrealized_loss_limit_usd,
            unrealized <= -settings.kis_us_unrealized_loss_limit_usd,
            "KIS US-equity unrealized loss limit reached",
        ),
        (
            "us_unrealized_loss_pct",
            unrealized_loss_pct,
            settings.kis_us_unrealized_loss_pct,
            unrealized_loss_pct >= settings.kis_us_unrealized_loss_pct,
            "KIS US-equity unrealized loss percent limit reached",
        ),
        (
            "us_severe_holding_loss_pct",
            worst_holding_loss_pct,
            settings.kis_us_severe_holding_loss_pct,
            worst_holding_loss_pct >= settings.kis_us_severe_holding_loss_pct,
            "KIS US-equity severe holding loss limit reached",
        ),
    ]
    blocker = next((gate for gate in gates if gate[3]), None)
    if blocker is None:
        return AccountDrawdownDecision(
            allowed=True,
            gate_name=None,
            reason=None,
            current_metric=None,
            threshold=None,
            baseline_value=exposure,
            baseline_timestamp=snapshot.observed_at.astimezone(UTC).isoformat(),
            sells_risk_reduction_allowed=True,
            summary={
                "blocked": False,
                "currency": "USD",
                "order_idempotency_key": order_idempotency_key,
                "execution_plan_item_id": execution_plan_item_id,
            },
            payload={
                "blocked": False,
                "evaluated_at": observed_at.isoformat(),
                "metrics": _json_safe(metrics),
            },
        )
    gate_name, current_metric, threshold, _, reason = blocker
    summary = {
        "gate_name": gate_name,
        "reason": reason,
        "currency": "USD",
        "current_metric": _decimal_text(current_metric),
        "threshold": _decimal_text(threshold),
        "baseline_value": _decimal_text(exposure),
        "baseline_timestamp": snapshot.observed_at.astimezone(UTC).isoformat(),
        "order_idempotency_key": order_idempotency_key,
        "execution_plan_item_id": execution_plan_item_id,
        "sells_risk_reduction_allowed": True,
    }
    return AccountDrawdownDecision(
        allowed=False,
        gate_name=gate_name,
        reason=reason,
        current_metric=current_metric,
        threshold=threshold,
        baseline_value=exposure,
        baseline_timestamp=summary["baseline_timestamp"],
        sells_risk_reduction_allowed=True,
        summary=summary,
        payload={
            "blocked": True,
            "evaluated_at": observed_at.isoformat(),
            "decision": summary,
            "metrics": _json_safe(metrics),
            "holdings": [_holding_payload(holding) for holding in snapshot.holdings],
        },
    )


def _holding_market_value(holding: BrokerAccountHolding) -> Decimal:
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    for key in ("ovrs_stck_evlu_amt", "market_value", "evaluation_amount"):
        parsed = _decimal_or_none(payload.get(key))
        if parsed is not None and parsed >= 0:
            return parsed
    if holding.average_price is not None:
        return holding.average_price * Decimal(holding.quantity)
    return Decimal("0")


def _open_buy_notional(snapshot: BrokerAccountSnapshot) -> tuple[Decimal, int]:
    total = Decimal("0")
    unknown_count = 0
    for order in snapshot.open_orders:
        remaining = order.remaining_quantity or 0
        if order.side.lower() != "buy" or remaining <= 0:
            continue
        if order.limit_price is None or order.limit_price <= 0:
            unknown_count += 1
            continue
        total += Decimal(remaining) * order.limit_price
    return total, unknown_count


def _us_realized_pnl(
    *,
    settings: Settings,
    db: Session,
    now: datetime,
) -> tuple[PeriodRealizedPnl, PeriodRealizedPnl]:
    local_now = now.astimezone(US_EASTERN)
    day_start = datetime.combine(local_now.date(), time.min, tzinfo=US_EASTERN).astimezone(UTC)
    week_start = datetime.combine(
        local_now.date() - timedelta(days=local_now.weekday()),
        time.min,
        tzinfo=US_EASTERN,
    ).astimezone(UTC)
    fills = dedupe_execution_fills(
        db.scalars(
            select(ExecutionFill)
            .join(BrokerOrder)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.broker_name == "kis",
                BrokerOrder.asset_class == "us_equity",
                ExecutionFill.filled_at <= now,
            )
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    )
    lots: dict[str, list[list[object]]] = {}
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=now,
    )
    daily_rows: list[ClosedTradeEconomics] = []
    weekly_rows: list[ClosedTradeEconomics] = []
    daily_unmatched_sell_quantity = Decimal("0")
    weekly_unmatched_sell_quantity = Decimal("0")
    for fill in fills:
        quantity = effective_fill_quantity(fill)
        filled_at = fill.filled_at
        if filled_at.tzinfo is None:
            filled_at = filled_at.replace(tzinfo=UTC)
        symbol_lots = lots.setdefault(fill.symbol, [])
        isolation_cursor.apply_before(
            symbol=fill.symbol,
            observed_at=fill.filled_at,
            lots=symbol_lots,
        )
        if fill.side.lower() == "buy":
            symbol_lots.append([quantity, Decimal(fill.price), fill])
            continue
        if fill.side.lower() != "sell":
            continue
        remaining = quantity
        while remaining > 0 and symbol_lots:
            lot = symbol_lots[0]
            matched = min(remaining, lot[0])
            pnl = ((Decimal(fill.price) - Decimal(str(lot[1]))) * matched).quantize(
                Decimal("0.0001")
            )
            economics = closed_trade_economics(
                buy_fill=lot[2],
                sell_fill=fill,
                matched_quantity=matched,
                gross_realized_pnl=pnl,
            )
            if filled_at >= day_start:
                daily_rows.append(economics)
            if filled_at >= week_start:
                weekly_rows.append(economics)
            lot[0] -= matched
            remaining -= matched
            if lot[0] <= 0:
                symbol_lots.pop(0)
        if remaining > 0:
            if filled_at >= day_start:
                daily_unmatched_sell_quantity += remaining
            if filled_at >= week_start:
                weekly_unmatched_sell_quantity += remaining
    return (
        _period_realized_pnl(
            daily_rows,
            unmatched_sell_quantity=daily_unmatched_sell_quantity,
        ),
        _period_realized_pnl(
            weekly_rows,
            unmatched_sell_quantity=weekly_unmatched_sell_quantity,
        ),
    )


def _period_realized_pnl(
    rows: list[ClosedTradeEconomics],
    *,
    unmatched_sell_quantity: Decimal = Decimal("0"),
) -> PeriodRealizedPnl:
    covered_gross = sum((row.gross_realized_pnl for row in rows), Decimal("0")).quantize(
        Decimal("0.0001")
    )
    currency_complete = all(row.settlement_currency == "USD" for row in rows)
    pnl_complete = currency_complete and unmatched_sell_quantity <= 0
    gross = covered_gross if pnl_complete else None
    known_cost_values = [
        row.known_transaction_cost
        for row in rows
        if row.known_transaction_cost is not None
    ]
    known_cost = (
        sum(known_cost_values, Decimal("0")).quantize(Decimal("0.00000001"))
        if known_cost_values and currency_complete
        else (
            Decimal("0")
            if not rows and unmatched_sell_quantity <= 0
            else None
        )
    )
    if not rows and unmatched_sell_quantity <= 0:
        coverage = "complete"
    elif pnl_complete and all(
        row.transaction_cost_coverage == "complete"
        and row.settlement_currency == "USD"
        for row in rows
    ):
        coverage = "complete"
    elif known_cost is not None or any(
        row.transaction_cost_coverage in {"complete", "partial"} for row in rows
    ):
        coverage = "partial"
    else:
        coverage = "unknown"
    net = (
        (gross - known_cost).quantize(Decimal("0.00000001"))
        if coverage == "complete" and gross is not None and known_cost is not None
        else None
    )
    covered_loss_floor = (
        (
            covered_gross
            - sum(known_cost_values, Decimal("0"))
        ).quantize(Decimal("0.00000001"))
        if rows and currency_complete
        else None
    )
    return PeriodRealizedPnl(
        gross=gross,
        covered_gross=covered_gross,
        known_transaction_cost=known_cost,
        covered_loss_floor=covered_loss_floor,
        net=net,
        transaction_cost_coverage=coverage,
        pnl_coverage="complete" if pnl_complete else "incomplete",
        unmatched_sell_quantity=unmatched_sell_quantity,
    )


def _period_loss_metric(period: PeriodRealizedPnl) -> Decimal | None:
    if period.net is not None:
        return period.net
    if period.gross is None:
        return (
            period.covered_loss_floor
            if period.covered_loss_floor is not None
            and period.covered_loss_floor < 0
            else None
        )
    return period.gross - (period.known_transaction_cost or Decimal("0"))


def record_account_drawdown_block(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    decision: AccountDrawdownDecision,
    occurred_at: datetime | None = None,
) -> None:
    if execution_plan_item_id is None:
        return
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=execution_plan_item_id,
            event_type=BLOCK_EVENT_TYPE,
            source="broker_order_service",
            summary=decision.summary,
            payload=decision.payload,
            error_message=decision.reason,
            occurred_at=occurred_at or datetime.now(UTC),
        )
    )


def account_drawdown_uncertain_decision(
    *,
    order_idempotency_key: str,
    execution_plan_item_id: str | None,
    reason: str,
    now: datetime | None = None,
) -> AccountDrawdownDecision:
    observed_at = now or datetime.now(UTC)
    summary = {
        "gate_name": "account_drawdown_snapshot_unavailable",
        "reason": reason,
        "current_metric": None,
        "threshold": None,
        "baseline_value": None,
        "baseline_timestamp": observed_at.isoformat(),
        "order_idempotency_key": order_idempotency_key,
        "execution_plan_item_id": execution_plan_item_id,
        "sells_risk_reduction_allowed": True,
    }
    return AccountDrawdownDecision(
        allowed=False,
        gate_name="account_drawdown_snapshot_unavailable",
        reason=reason,
        current_metric=None,
        threshold=None,
        baseline_value=None,
        baseline_timestamp=observed_at.isoformat(),
        sells_risk_reduction_allowed=True,
        summary=summary,
        payload={
            "blocked": True,
            "evaluated_at": observed_at.isoformat(),
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "decision": summary,
            "metrics": {},
        },
    )


def latest_account_drawdown_gate_state(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    if not settings.account_drawdown_gate_enabled:
        return {
            "gated": False,
            "enabled": False,
            "latest_decision": None,
            "active_recovery_scopes": {},
        }

    release_event_types = (
        RECOVERY_RELEASED_EVENT_TYPE,
        POLICY_DISABLED_RELEASE_EVENT_TYPE,
    )
    account_fingerprint = recovery_account_fingerprint(settings)
    current_recovery_scopes = (
        f"kis:{account_fingerprint}:krw",
        f"kis:{account_fingerprint}:usd",
    )
    source_audit_released = (
        exists(
            select(1).where(
                ExecutionPlanItemAudit.execution_plan_item_id
                == BrokerOrder.execution_plan_item_id,
                ExecutionPlanItemAudit.event_type.in_(release_event_types),
                ExecutionPlanItemAudit.summary[
                    "source_broker_order_pk"
                ].as_string()
                == BrokerOrder.id,
            )
        )
        .correlate(BrokerOrder)
    )
    source_order_event_released = (
        exists(
            select(1).where(
                BrokerOrderEvent.broker_order_pk == BrokerOrder.id,
                BrokerOrderEvent.event_type.in_(release_event_types),
            )
        )
        .correlate(BrokerOrder)
    )
    source_released = or_(source_audit_released, source_order_event_released)
    visible_recovery_scopes = (
        current_recovery_scopes
        if settings.account_drawdown_recovery_scope_secret
        else None
    )
    recovery_sources = db.scalars(
        select(BrokerOrder)
        .where(
            _recoverable_recovery_source_predicate(
                BrokerOrder,
                runtime_mode=settings.runtime_mode,
                recovery_scopes=visible_recovery_scopes,
            ),
            ~source_released,
        )
        .options(
            undefer(BrokerOrder.request_metadata),
            undefer(BrokerOrder.response_metadata),
            undefer(BrokerOrder.raw_broker_payload),
        )
        .order_by(
            BrokerOrder.last_broker_sync_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
    ).all()
    active_recovery_scopes: dict[str, dict[str, object]] = {}
    latest_active_recovery: tuple[datetime, dict[str, object]] | None = None
    for source_order in recovery_sources:
        policy = (
            source_order.request_metadata.get("account_drawdown_recovery_policy")
            if isinstance(source_order.request_metadata, dict)
            else None
        )
        recovery_scope = (
            str(policy.get("recovery_scope"))
            if isinstance(policy, dict) and policy.get("recovery_scope")
            else None
        )
        if recovery_scope is None or recovery_scope in active_recovery_scopes:
            continue
        decision = _broker_order_gate_state(source_order, recovery_scope=recovery_scope)
        active_recovery_scopes[recovery_scope] = decision
        occurred_at = source_order.last_broker_sync_at or source_order.created_at
        if latest_active_recovery is None or _timestamp_for_ordering(
            occurred_at
        ) > _timestamp_for_ordering(latest_active_recovery[0]):
            latest_active_recovery = (occurred_at, decision)

    recovery_source_alias = aliased(BrokerOrder)
    matching_recovery_source = (
        exists(
            select(1).where(
                _recoverable_recovery_source_predicate(
                    recovery_source_alias,
                    runtime_mode=settings.runtime_mode,
                ),
                recovery_source_alias.execution_plan_item_id
                == ExecutionPlanItemAudit.execution_plan_item_id,
                recovery_source_alias.idempotency_key
                == ExecutionPlanItemAudit.summary[
                    "order_idempotency_key"
                ].as_string(),
            )
        )
        .correlate(ExecutionPlanItemAudit)
    )
    policy_source_alias = aliased(BrokerOrder)
    policy_source_policy = policy_source_alias.request_metadata[
        "account_drawdown_recovery_policy"
    ]
    matching_foreign_policy_source = (
        exists(
            select(1).where(
                policy_source_alias.mode == settings.runtime_mode,
                policy_source_alias.execution_plan_item_id
                == ExecutionPlanItemAudit.execution_plan_item_id,
                policy_source_alias.idempotency_key
                == ExecutionPlanItemAudit.summary[
                    "order_idempotency_key"
                ].as_string(),
                func.coalesce(
                    policy_source_policy["policy_version"].as_integer(),
                    0,
                )
                == 1,
                func.coalesce(
                    policy_source_policy["recovery_scope"].as_string(),
                    "",
                )
                != "",
                policy_source_policy["recovery_scope"].as_string().not_in(
                    current_recovery_scopes
                ),
            )
        )
        .correlate(ExecutionPlanItemAudit)
    )
    released_legacy_source_alias = aliased(BrokerOrder)
    legacy_release_audit_alias = aliased(ExecutionPlanItemAudit)
    matching_released_source = (
        exists(
            select(1)
            .select_from(released_legacy_source_alias)
            .join(
                legacy_release_audit_alias,
                and_(
                    legacy_release_audit_alias.execution_plan_item_id
                    == released_legacy_source_alias.execution_plan_item_id,
                    legacy_release_audit_alias.event_type.in_(release_event_types),
                    legacy_release_audit_alias.summary[
                        "source_broker_order_pk"
                    ].as_string()
                    == released_legacy_source_alias.id,
                ),
            )
            .where(
                released_legacy_source_alias.mode == settings.runtime_mode,
                released_legacy_source_alias.execution_plan_item_id
                == ExecutionPlanItemAudit.execution_plan_item_id,
                released_legacy_source_alias.idempotency_key
                == ExecutionPlanItemAudit.summary[
                    "order_idempotency_key"
                ].as_string(),
            )
        )
        .correlate(ExecutionPlanItemAudit)
    )
    audit = db.scalar(
        select(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type == BLOCK_EVENT_TYPE,
            ~matching_recovery_source,
            ~matching_released_source,
            *(
                (~matching_foreign_policy_source,)
                if settings.account_drawdown_recovery_scope_secret
                else ()
            ),
        )
        .options(
            undefer(ExecutionPlanItemAudit.summary),
            undefer(ExecutionPlanItemAudit.payload),
        )
        .order_by(
            ExecutionPlanItemAudit.occurred_at.desc(),
            ExecutionPlanItemAudit.created_at.desc(),
            ExecutionPlanItemAudit.id.desc(),
        )
        .limit(1)
    )
    order_policy = BrokerOrder.request_metadata["account_drawdown_recovery_policy"]
    order_gate_summary = BrokerOrder.response_metadata["account_drawdown_gate"]
    visible_order_scope = or_(
        order_policy["recovery_scope"].as_string().in_(current_recovery_scopes),
        and_(
            func.coalesce(order_policy["recovery_scope"].as_string(), "") == "",
            or_(
                order_gate_summary["blocked"].as_boolean().is_(True),
                func.coalesce(order_gate_summary["gate_name"].as_string(), "")
                != "",
            ),
        ),
    )
    order = db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            or_(
                order_gate_summary["blocked"].as_boolean().is_not(None),
                func.coalesce(order_gate_summary["gate_name"].as_string(), "")
                != "",
                func.coalesce(
                    order_gate_summary["order_idempotency_key"].as_string(),
                    "",
                )
                != "",
            ),
            ~_recoverable_recovery_source_predicate(
                BrokerOrder,
                runtime_mode=settings.runtime_mode,
            ),
            ~source_released,
            *(
                (visible_order_scope,)
                if settings.account_drawdown_recovery_scope_secret
                else ()
            ),
        )
        .options(
            undefer(BrokerOrder.response_metadata),
            undefer(BrokerOrder.raw_broker_payload),
        )
        .order_by(
            BrokerOrder.last_broker_sync_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
        .limit(1)
    )
    released_source_alias = aliased(BrokerOrder)
    released_policy = released_source_alias.request_metadata[
        "account_drawdown_recovery_policy"
    ]
    matching_visible_released_source = (
        exists(
            select(1).where(
                released_source_alias.mode == settings.runtime_mode,
                released_source_alias.last_error_code.in_(
                    tuple(RECOVERABLE_LOSS_GATE_NAMES)
                ),
                released_source_alias.id
                == ExecutionPlanItemAudit.summary[
                    "source_broker_order_pk"
                ].as_string(),
                or_(
                    _recoverable_recovery_source_predicate(
                        released_source_alias,
                        runtime_mode=settings.runtime_mode,
                        recovery_scopes=current_recovery_scopes,
                    ),
                    func.coalesce(
                        released_policy["policy_version"].as_integer(),
                        0,
                    )
                    != 1,
                    func.coalesce(
                        released_policy["recovery_scope"].as_string(),
                        "",
                    )
                    == "",
                ),
            )
        )
        .correlate(ExecutionPlanItemAudit)
    )
    latest_release = db.scalar(
        select(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type.in_(release_event_types),
            matching_visible_released_source,
        )
        .options(
            undefer(ExecutionPlanItemAudit.summary),
            undefer(ExecutionPlanItemAudit.payload),
        )
        .order_by(
            ExecutionPlanItemAudit.occurred_at.desc(),
            ExecutionPlanItemAudit.created_at.desc(),
            ExecutionPlanItemAudit.id.desc(),
        )
        .limit(1)
    )
    released_event_source_alias = aliased(BrokerOrder)
    latest_order_release = db.scalar(
        select(BrokerOrderEvent)
        .join(
            released_event_source_alias,
            released_event_source_alias.id == BrokerOrderEvent.broker_order_pk,
        )
        .where(
            BrokerOrderEvent.event_type.in_(release_event_types),
            _recoverable_recovery_source_predicate(
                released_event_source_alias,
                runtime_mode=settings.runtime_mode,
                recovery_scopes=visible_recovery_scopes,
            ),
        )
        .options(undefer(BrokerOrderEvent.response_payload))
        .order_by(
            BrokerOrderEvent.observed_at.desc(),
            BrokerOrderEvent.created_at.desc(),
            BrokerOrderEvent.id.desc(),
        )
        .limit(1)
    )

    legacy_decision: tuple[datetime, bool, dict[str, object]] | None = None
    order_occurred_at = (
        (order.last_broker_sync_at or order.created_at) if order is not None else None
    )
    if audit is not None and (
        order_occurred_at is None
        or _timestamp_for_ordering(audit.occurred_at) >= _timestamp_for_ordering(order_occurred_at)
    ):
        legacy_decision = (
            audit.occurred_at,
            True,
            {
                "source": "execution_plan_item_audit",
                "occurred_at": audit.occurred_at.isoformat(),
                "execution_plan_item_id": audit.execution_plan_item_id,
                "summary": audit.summary or {},
                "payload": audit.payload or {},
            },
        )
    elif order is not None and order_occurred_at is not None:
        order_decision = _broker_order_gate_state(order)
        legacy_decision = (
            order_occurred_at,
            _account_drawdown_decision_is_blocked(
                summary=order_decision["summary"],
                payload=order_decision["payload"],
            ),
            order_decision,
        )

    gated_decisions: list[tuple[datetime, dict[str, object]]] = []
    if latest_active_recovery is not None:
        gated_decisions.append(latest_active_recovery)
    if legacy_decision is not None and legacy_decision[1]:
        gated_decisions.append((legacy_decision[0], legacy_decision[2]))
    if gated_decisions:
        latest_decision = max(
            gated_decisions,
            key=lambda candidate: _timestamp_for_ordering(candidate[0]),
        )[1]
        return {
            "gated": True,
            "latest_decision": latest_decision,
            "active_recovery_scopes": active_recovery_scopes,
        }

    informational_decisions: list[tuple[datetime, dict[str, object]]] = []
    if legacy_decision is not None:
        informational_decisions.append((legacy_decision[0], legacy_decision[2]))
    if latest_release is not None:
        informational_decisions.append(
            (
                latest_release.occurred_at,
                {
                    "source": "account_drawdown_recovery_audit",
                    "event_type": latest_release.event_type,
                    "occurred_at": latest_release.occurred_at.isoformat(),
                    "execution_plan_item_id": latest_release.execution_plan_item_id,
                    "summary": latest_release.summary or {},
                    "payload": latest_release.payload or {},
                },
            )
        )
    if latest_order_release is not None:
        informational_decisions.append(
            (
                latest_order_release.observed_at,
                {
                    "source": "broker_order_recovery_event",
                    "event_type": latest_order_release.event_type,
                    "occurred_at": latest_order_release.observed_at.isoformat(),
                    "broker_order_pk": latest_order_release.broker_order_pk,
                    "execution_plan_item_id": None,
                    "summary": latest_order_release.payload or {},
                    "payload": latest_order_release.response_payload or {},
                },
            )
        )
    latest_decision = (
        max(
            informational_decisions,
            key=lambda candidate: _timestamp_for_ordering(candidate[0]),
        )[1]
        if informational_decisions
        else None
    )
    return {
        "gated": False,
        "latest_decision": latest_decision,
        "active_recovery_scopes": active_recovery_scopes,
    }


def _broker_order_gate_state(
    order: BrokerOrder,
    *,
    recovery_scope: str | None = None,
) -> dict[str, object]:
    state: dict[str, object] = {
        "source": "broker_order",
        "occurred_at": (order.last_broker_sync_at or order.created_at).isoformat(),
        "order_id": order.id,
        "order_idempotency_key": order.idempotency_key,
        "execution_plan_item_id": order.execution_plan_item_id,
        "summary": (order.response_metadata or {}).get("account_drawdown_gate") or {},
        "payload": order.raw_broker_payload or {},
    }
    if recovery_scope is not None:
        state["recovery_scope"] = recovery_scope
    return state


def _recoverable_recovery_source_predicate(
    order_model: Any,
    *,
    runtime_mode: str,
    recovery_scopes: tuple[str, ...] | None = None,
) -> Any:
    policy = order_model.request_metadata["account_drawdown_recovery_policy"]
    recovery_scope = policy["recovery_scope"].as_string()
    conditions = [
        order_model.mode == runtime_mode,
        order_model.side == "buy",
        order_model.status == "rejected",
        func.coalesce(order_model.last_error_classification == "validation", False),
        func.coalesce(
            order_model.last_error_code.in_(tuple(RECOVERABLE_LOSS_GATE_NAMES)),
            False,
        ),
        order_model.broker_order_id.is_(None),
        order_model.submitted_at.is_(None),
        func.coalesce(policy["policy_version"].as_integer(), 0) == 1,
        func.coalesce(recovery_scope, "") != "",
    ]
    if recovery_scopes is not None:
        conditions.append(recovery_scope.in_(recovery_scopes))
    return and_(*conditions)


def account_drawdown_gate_report_lines(gate_state: dict[str, object]) -> list[str]:
    latest = gate_state.get("latest_decision")
    if not gate_state.get("gated") or not isinstance(latest, dict):
        return ["Account drawdown gate: not currently gated by the latest audited decision."]
    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    return [
        (
            "Account drawdown gate: gated="
            f"{gate_state.get('gated')}, gate={summary.get('gate_name')}, "
            f"metric={summary.get('current_metric')}, threshold={summary.get('threshold')}, "
            f"baseline={summary.get('baseline_value')}, "
            f"sells/risk-reduction allowed={summary.get('sells_risk_reduction_allowed')}"
        )
    ]


def account_drawdown_gate_prompt_summary(gate_state: dict[str, object]) -> str:
    latest = gate_state.get("latest_decision")
    if not gate_state.get("gated") or not isinstance(latest, dict):
        return "Account drawdown gate is not active in the latest audited decision."
    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    return (
        f"Account drawdown gate active: {summary.get('gate_name')} "
        f"metric={summary.get('current_metric')} threshold={summary.get('threshold')} "
        f"baseline={summary.get('baseline_value')}; sells and risk-reduction remain allowed."
    )


def _account_drawdown_decision_is_blocked(
    *,
    summary: object,
    payload: object,
) -> bool:
    if isinstance(summary, dict) and isinstance(summary.get("blocked"), bool):
        return bool(summary["blocked"])
    if isinstance(payload, dict) and isinstance(payload.get("blocked"), bool):
        return bool(payload["blocked"])
    if isinstance(payload, dict):
        decision = payload.get("decision")
        if isinstance(decision, dict) and isinstance(decision.get("blocked"), bool):
            return bool(decision["blocked"])
    if isinstance(summary, dict) and summary.get("gate_name"):
        return True
    return False


def _timestamp_for_ordering(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _field_or_payload_decimal(
    field_value: Decimal | None,
    payload: dict[str, Any],
    keys: tuple[str, ...],
) -> Decimal | None:
    if field_value is not None:
        return field_value
    return _payload_decimal(payload, keys)


def _metrics(snapshot: BrokerAccountSnapshot) -> dict[str, Decimal | None]:
    total_asset = _field_or_payload_decimal(
        snapshot.total_asset_value,
        snapshot.payload,
        ("total_asset_value", "tot_evlu_amt", "nass_amt"),
    )
    daily_baseline = _field_or_payload_decimal(
        snapshot.previous_total_asset_value,
        snapshot.payload,
        ("previous_total_asset_value", "bfdy_tot_asst_evlu_amt"),
    )
    asset_change = _field_or_payload_decimal(
        snapshot.asset_change_amount,
        snapshot.payload,
        ("asset_change_amount", "asst_icdc_amt"),
    )
    unrealized_pnl = _field_or_payload_decimal(
        snapshot.unrealized_pnl,
        snapshot.payload,
        ("unrealized_pnl", "evlu_pfls_smtl_amt"),
    )
    daily_drawdown = None
    daily_drawdown_pct = None
    if total_asset is not None and daily_baseline is not None and daily_baseline > 0:
        daily_drawdown = daily_baseline - total_asset
        daily_drawdown_pct = (daily_drawdown / daily_baseline * Decimal("100")).quantize(Decimal("0.0001"))
    if daily_drawdown is None and asset_change is not None and asset_change < 0:
        daily_drawdown = -asset_change
    unrealized_loss_pct = None
    if unrealized_pnl is not None and total_asset is not None and total_asset > 0 and unrealized_pnl < 0:
        unrealized_loss_pct = ((-unrealized_pnl) / total_asset * Decimal("100")).quantize(Decimal("0.0001"))
    return {
        "total_asset": total_asset,
        "daily_baseline": daily_baseline,
        "daily_drawdown": daily_drawdown,
        "daily_drawdown_pct": daily_drawdown_pct,
        "asset_change_amount": asset_change,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_loss_pct": unrealized_loss_pct,
    }


def _daily_total_asset_pct_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("daily_drawdown_pct")
    threshold = settings.account_daily_total_asset_drawdown_pct
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "daily_total_asset_drawdown_pct",
            "reason": "daily total asset drawdown percent threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("daily_baseline"),
        }
    return None


def _daily_total_asset_abs_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("daily_drawdown")
    threshold = settings.account_daily_total_asset_drawdown_krw
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "daily_total_asset_drawdown_krw",
            "reason": "daily total asset drawdown amount threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("daily_baseline"),
        }
    return None


def _weekly_total_asset_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    baseline = settings.account_weekly_total_asset_baseline_krw
    current_asset = metrics.get("total_asset")
    threshold = settings.account_weekly_total_asset_drawdown_pct
    if baseline <= 0 or current_asset is None or threshold <= 0:
        return None
    drawdown_pct = ((baseline - current_asset) / baseline * Decimal("100")).quantize(Decimal("0.0001"))
    if drawdown_pct >= threshold:
        return {
            "gate_name": "weekly_total_asset_drawdown_pct",
            "reason": "weekly total asset drawdown percent threshold reached",
            "current_metric": drawdown_pct,
            "threshold": threshold,
            "baseline_value": baseline,
            "baseline_timestamp": settings.account_weekly_total_asset_baseline_at,
        }
    return None


def _unrealized_abs_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    unrealized = metrics.get("unrealized_pnl")
    threshold = settings.account_unrealized_loss_abs_krw
    if unrealized is not None and threshold > 0 and unrealized <= -threshold:
        return {
            "gate_name": "unrealized_portfolio_loss_krw",
            "reason": "unrealized portfolio loss amount threshold reached",
            "current_metric": unrealized,
            "threshold": -threshold,
            "baseline_value": metrics.get("total_asset"),
        }
    return None


def _unrealized_pct_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("unrealized_loss_pct")
    threshold = settings.account_unrealized_loss_pct
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "unrealized_portfolio_loss_pct",
            "reason": "unrealized portfolio loss percent threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("total_asset"),
        }
    return None


def _severe_holding_gate(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
) -> dict[str, object] | None:
    pct_threshold = settings.account_severe_holding_loss_pct
    abs_threshold = settings.account_severe_holding_loss_abs_krw
    for holding in snapshot.holdings:
        holding_loss = -holding.unrealized_pnl if holding.unrealized_pnl is not None else None
        holding_loss_pct = _holding_loss_pct(holding)
        if holding_loss_pct is not None and pct_threshold > 0 and holding_loss_pct >= pct_threshold:
            return {
                "gate_name": "severe_holding_unrealized_loss_pct",
                "reason": "severe holding unrealized loss threshold reached",
                "current_metric": holding_loss_pct,
                "threshold": pct_threshold,
                "baseline_value": holding.average_price,
                "holding_symbol": holding.symbol,
            }
        if holding_loss is not None and abs_threshold > 0 and holding_loss >= abs_threshold:
            return {
                "gate_name": "severe_holding_unrealized_loss_krw",
                "reason": "severe holding unrealized loss amount threshold reached",
                "current_metric": holding.unrealized_pnl,
                "threshold": -abs_threshold,
                "baseline_value": holding.average_price,
                "holding_symbol": holding.symbol,
            }
    return None


def _holding_loss_pct(holding: BrokerAccountHolding) -> Decimal | None:
    if holding.unrealized_pnl_pct is not None:
        value = holding.unrealized_pnl_pct
        return -value if value < 0 else None
    raw = holding.payload
    for key in ("evlu_pfls_rt", "evlu_erng_rt", "profit_loss_pct"):
        parsed = _parse_decimal(raw.get(key))
        if parsed is not None:
            return -parsed if parsed < 0 else None
    return None


def _payload_decimal(payload: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        parsed = _parse_decimal(payload.get(key))
        if parsed is not None:
            return parsed
    balance = payload.get("balance")
    if isinstance(balance, dict):
        output2 = balance.get("output2")
        rows = output2 if isinstance(output2, list) else [output2]
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in keys:
                parsed = _parse_decimal(row.get(key))
                if parsed is not None:
                    return parsed
    return None


def _holding_payload(holding: BrokerAccountHolding) -> dict[str, object]:
    return {
        "symbol": holding.symbol,
        "quantity": holding.quantity,
        "average_price": _decimal_text(holding.average_price),
        "unrealized_pnl": _decimal_text(holding.unrealized_pnl),
        "unrealized_pnl_pct": _decimal_text(holding.unrealized_pnl_pct),
        "raw": holding.payload,
    }


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except Exception:
        return None


def _decimal_or_none(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    return _parse_decimal(value)


def _decimal_text(value: object) -> str | None:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None
    return f"{parsed:.4f}"


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
