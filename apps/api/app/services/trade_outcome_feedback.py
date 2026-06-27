from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from app.core.config import Settings, load_settings
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlanItem, ExecutionPosition

REPORT_VERSION = "trade-outcome-feedback-v1"
MIN_CANDIDATE_RULE_SAMPLE_SIZE = 3
BROKER_BALANCE_OUTPUT2_FIELDS = (
    "tot_evlu_amt",
    "nass_amt",
    "bfdy_tot_asst_evlu_amt",
    "asst_icdc_amt",
    "evlu_pfls_smtl_amt",
    "pchs_amt_smtl_amt",
    "evlu_amt_smtl_amt",
    "dnca_tot_amt",
)


@dataclass(frozen=True)
class CandidateRule:
    rule_id: str
    name: str
    action: str
    size_multiplier: Decimal
    threshold: Decimal
    evaluator: Callable[[dict[str, object], "CandidateRule"], tuple[bool, dict[str, object]]]


@dataclass
class OpenLot:
    quantity: int
    price: Decimal
    filled_at: datetime
    fill: ExecutionFill


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    position: ExecutionPosition
    balance: dict[str, object]
    output2: dict[str, object]
    observed_at: datetime | None


def generate_trade_outcome_feedback(
    *,
    settings: Settings,
    db: Session,
    lookback_days: int | None = None,
    as_of: datetime | None = None,
    output_dir: Path | None = None,
) -> dict[str, object]:
    now = _normalize_utc(as_of or datetime.now(UTC))
    days = max(1, int(lookback_days or settings.trade_outcome_feedback_lookback_days))
    started_at = now - timedelta(days=days)
    fills = _load_fills(settings=settings, db=db)
    closed_trades = [
        trade
        for trade in _fifo_closed_trades(fills)
        if _parse_datetime(str((trade.get("sell") or {}).get("filled_at"))) >= started_at
    ]
    broker_asset_performance = _broker_asset_performance(
        settings=settings,
        db=db,
        closed_trades=closed_trades,
        lookback_days=days,
        started_at=started_at,
    )
    summary = _summary(closed_trades, broker_asset_performance=broker_asset_performance)
    lessons = _lessons(closed_trades, max_lessons=settings.trade_outcome_feedback_max_lessons)
    portfolio_risk = _portfolio_risk(
        settings=settings,
        db=db,
        closed_trades=closed_trades,
        as_of=now,
    )
    candidate_rule_replay = _candidate_rule_replay(settings=settings, closed_trades=closed_trades)
    snippet = _snippet(
        summary=summary,
        lessons=lessons,
        portfolio_risk=portfolio_risk,
        candidate_rule_replay=candidate_rule_replay,
        broker_asset_performance=broker_asset_performance,
    )
    report_date = now.astimezone(UTC).date()
    report: dict[str, object] = {
        "version": REPORT_VERSION,
        "mode": settings.runtime_mode,
        "generated_at": now.isoformat(),
        "lookback_days": days,
        "started_at": started_at.isoformat(),
        "closed_trade_count": len(closed_trades),
        "summary": summary,
        "broker_asset_performance": broker_asset_performance,
        "portfolio_risk": portfolio_risk,
        "closed_trades": closed_trades,
        "lessons": lessons,
        "candidate_rule_replay": candidate_rule_replay,
        "prompt_snippet": snippet,
    }
    target_dir = output_dir or Path(settings.trade_outcome_feedback_dir)
    _write_artifacts(report=report, report_date=report_date, output_dir=target_dir)
    return report


def load_latest_trade_outcome_feedback_snippet(settings: Settings) -> dict[str, object] | None:
    if not settings.trade_outcome_feedback_enabled:
        return None
    latest_path = Path(settings.trade_outcome_feedback_dir) / "latest.json"
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("mode") != settings.runtime_mode:
        return None
    snippet = payload.get("prompt_snippet")
    if not isinstance(snippet, dict):
        return None
    return {
        "version": payload.get("version"),
        "mode": payload.get("mode"),
        "generated_at": payload.get("generated_at"),
        "lookback_days": payload.get("lookback_days"),
        "closed_trade_count": payload.get("closed_trade_count"),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        "snippet": snippet,
    }


def _load_fills(*, settings: Settings, db: Session) -> list[ExecutionFill]:
    return list(
        db.scalars(
            select(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
            )
            .options(
                selectinload(ExecutionFill.broker_order)
                .selectinload(BrokerOrder.plan_item)
                .selectinload(ExecutionPlanItem.plan)
            )
            .order_by(ExecutionFill.symbol.asc(), ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        )
    )


def _fifo_closed_trades(fills: list[ExecutionFill]) -> list[dict[str, object]]:
    lots_by_symbol: dict[str, list[OpenLot]] = {}
    closed: list[dict[str, object]] = []
    for fill in sorted(fills, key=lambda row: (row.symbol, _normalize_utc(row.filled_at), row.id)):
        side = str(fill.side or "").lower()
        if side == "buy":
            lots_by_symbol.setdefault(fill.symbol, []).append(
                OpenLot(
                    quantity=int(fill.quantity),
                    price=Decimal(fill.price),
                    filled_at=_normalize_utc(fill.filled_at),
                    fill=fill,
                )
            )
            continue
        if side != "sell":
            continue
        remaining = int(fill.quantity)
        lots = lots_by_symbol.setdefault(fill.symbol, [])
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, lot.quantity)
            closed.append(_closed_trade(lot=lot, sell_fill=fill, quantity=matched))
            lot.quantity -= matched
            remaining -= matched
            if lot.quantity <= 0:
                lots.pop(0)
    return closed


def _closed_trade(*, lot: OpenLot, sell_fill: ExecutionFill, quantity: int) -> dict[str, object]:
    buy_order = lot.fill.broker_order
    sell_order = sell_fill.broker_order
    buy_item = buy_order.plan_item if buy_order is not None else None
    sell_item = sell_order.plan_item if sell_order is not None else None
    sell_price = Decimal(sell_fill.price)
    buy_notional = lot.price * Decimal(quantity)
    sell_notional = sell_price * Decimal(quantity)
    pnl = sell_notional - buy_notional
    return_pct = (pnl / buy_notional * Decimal("100")) if buy_notional > 0 else Decimal("0")
    hold_minutes = int((_normalize_utc(sell_fill.filled_at) - lot.filled_at).total_seconds() // 60)
    source_item = buy_item or sell_item
    return {
        "symbol": sell_fill.symbol,
        "quantity": quantity,
        "buy": _fill_payload(fill=lot.fill, order=buy_order, item=buy_item, quantity=quantity),
        "sell": _fill_payload(fill=sell_fill, order=sell_order, item=sell_item, quantity=quantity),
        "realized_pnl_krw": _decimal_text(pnl),
        "return_pct": _decimal_text(return_pct),
        "hold_minutes": hold_minutes,
        "hold_hours": _decimal_text(Decimal(hold_minutes) / Decimal("60")),
        "source_execution_plan_item_id": source_item.id if source_item is not None else None,
        "decision_run_id": source_item.decision_run_id if source_item is not None else None,
        "decision_agent_name": source_item.decision_agent_name if source_item is not None else None,
        "exec_agent_decision": _exec_agent_decision(source_item),
        "risk_sizing": _risk_sizing_context(source_item),
        "trigger_time": _trigger_time_context(source_item),
        "market_enrichment": _market_enrichment_context(source_item),
        "candidate_rule_context": _candidate_rule_context(item=source_item, buy_fill=lot.fill),
        "outcome_bucket": _outcome_bucket(pnl),
    }


def _fill_payload(
    *,
    fill: ExecutionFill,
    order: BrokerOrder | None,
    item: ExecutionPlanItem | None,
    quantity: int,
) -> dict[str, object]:
    return {
        "filled_at": _normalize_utc(fill.filled_at).isoformat(),
        "quantity": quantity,
        "price": _decimal_text(Decimal(fill.price)),
        "broker_fill_id": fill.broker_fill_id,
        "broker_order_id": order.broker_order_id if order is not None else None,
        "execution_plan_item_id": item.id if item is not None else None,
        "intent": item.intent if item is not None else None,
        "source_plan_item_id": item.source_plan_item_id if item is not None else None,
    }


def _summary(
    closed_trades: list[dict[str, object]],
    *,
    broker_asset_performance: dict[str, object] | None = None,
) -> dict[str, object]:
    pnl_values = [_decimal_value(row.get("realized_pnl_krw")) or Decimal("0") for row in closed_trades]
    winners = [value for value in pnl_values if value > 0]
    losers = [value for value in pnl_values if value < 0]
    total = sum(pnl_values, Decimal("0"))
    summary: dict[str, object] = {
        "primary_performance_metric": "broker_total_asset_change"
        if broker_asset_performance and broker_asset_performance.get("available")
        else "fifo_realized_pnl",
        "realized_pnl_krw": _decimal_text(total),
        "win_count": len(winners),
        "loss_count": len(losers),
        "win_rate_pct": _decimal_text((Decimal(len(winners)) / Decimal(len(pnl_values)) * Decimal("100")) if pnl_values else Decimal("0")),
        "average_win_krw": _decimal_text(sum(winners, Decimal("0")) / Decimal(len(winners)) if winners else Decimal("0")),
        "average_loss_krw": _decimal_text(sum(losers, Decimal("0")) / Decimal(len(losers)) if losers else Decimal("0")),
        "worst_loss_krw": _decimal_text(min(pnl_values) if pnl_values else Decimal("0")),
    }
    if broker_asset_performance and broker_asset_performance.get("available"):
        summary.update(
            {
                "broker_total_asset_baseline_krw": broker_asset_performance.get("baseline_total_asset_krw"),
                "broker_latest_total_asset_krw": broker_asset_performance.get("latest_total_asset_krw"),
                "broker_latest_net_asset_krw": broker_asset_performance.get("latest_net_asset_krw"),
                "broker_total_asset_change_krw": broker_asset_performance.get("asset_change_krw"),
                "broker_total_asset_change_pct": broker_asset_performance.get("asset_change_pct"),
                "broker_unrealized_pnl_krw": broker_asset_performance.get("broker_unrealized_pnl_krw"),
            }
        )
    return summary


def _broker_asset_performance(
    *,
    settings: Settings,
    db: Session,
    closed_trades: list[dict[str, object]],
    lookback_days: int,
    started_at: datetime,
) -> dict[str, object]:
    positions = _load_all_positions(settings=settings, db=db)
    snapshots = _broker_balance_snapshots(positions)
    if not snapshots:
        return _broker_asset_unavailable(
            f"execution_positions raw broker balance payload unavailable for mode={settings.runtime_mode}; "
            "cannot headline broker total asset change"
        )

    if lookback_days <= 1:
        snapshot = snapshots[0]
        latest_total_asset, latest_net_asset, baseline_total_asset, asset_change = _previous_day_asset_metrics(snapshot.output2)
        baseline_observed_at = None
        source_window = "previous_day"
    else:
        snapshot, baseline_snapshot, reason = _multi_day_broker_asset_snapshots(
            snapshots=snapshots,
            started_at=started_at,
        )
        if snapshot is None or baseline_snapshot is None:
            return _broker_asset_unavailable(reason)
        latest_total_asset = _decimal_value(snapshot.output2.get("tot_evlu_amt"))
        latest_net_asset = _decimal_value(snapshot.output2.get("nass_amt"))
        baseline_total_asset = _decimal_value(baseline_snapshot.output2.get("tot_evlu_amt"))
        asset_change = (
            latest_total_asset - baseline_total_asset
            if latest_total_asset is not None and baseline_total_asset is not None
            else None
        )
        baseline_observed_at = baseline_snapshot.observed_at
        source_window = "report_lookback_snapshots"

    if asset_change is None or latest_total_asset is None or baseline_total_asset is None:
        return _broker_asset_unavailable(
            "broker balance output2 missing parseable total/baseline/change asset fields; "
            "cannot headline broker total asset change"
        )

    position = snapshot.position
    balance = snapshot.balance
    output2 = snapshot.output2
    raw_fields = {key: output2.get(key) for key in BROKER_BALANCE_OUTPUT2_FIELDS if key in output2}
    asset_change_pct = (
        asset_change / baseline_total_asset * Decimal("100")
        if asset_change is not None and baseline_total_asset is not None and baseline_total_asset != 0
        else None
    )
    realized_pnl = sum(
        (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0"))
        for row in closed_trades
    )
    divergence = _asset_fifo_divergence(asset_change=asset_change, realized_pnl=realized_pnl)
    holdings = _broker_holdings_from_output1(balance.get("output1"))
    if not holdings:
        holdings = [_position_holding_fallback(position=row) for row in positions if _position_effective_quantity(row) > 0]
    return {
        "available": True,
        "source": "execution_positions.raw_payload.balance.output2",
        "source_window": source_window,
        "holdings_source": "execution_positions.raw_payload.balance.output1"
        if _payload_rows(balance.get("output1"))
        else "execution_positions",
        "observed_at": _normalize_utc(position.last_reconciled_at).isoformat()
        if position.last_reconciled_at is not None
        else None,
        "baseline_observed_at": baseline_observed_at.isoformat() if baseline_observed_at is not None else None,
        "source_position_symbol": position.symbol,
        "baseline_total_asset_krw": _decimal_text(baseline_total_asset),
        "latest_total_asset_krw": _decimal_text(latest_total_asset),
        "latest_net_asset_krw": _decimal_text(latest_net_asset),
        "asset_change_krw": _decimal_text(asset_change),
        "asset_change_pct": _decimal_text(asset_change_pct),
        "broker_unrealized_pnl_krw": _decimal_text(_decimal_value(output2.get("evlu_pfls_smtl_amt"))),
        "broker_purchase_amount_krw": _decimal_text(_decimal_value(output2.get("pchs_amt_smtl_amt"))),
        "broker_securities_value_krw": _decimal_text(_decimal_value(output2.get("evlu_amt_smtl_amt"))),
        "broker_cash_krw": _decimal_text(_decimal_value(output2.get("dnca_tot_amt"))),
        "fifo_realized_pnl_krw": _decimal_text(realized_pnl),
        "divergence_warning": divergence,
        "holdings": _json_safe(holdings),
        "raw_output2_fields": _json_safe(raw_fields),
    }


def _load_all_positions(*, settings: Settings, db: Session) -> list[ExecutionPosition]:
    return list(
        db.scalars(
            select(ExecutionPosition)
            .where(ExecutionPosition.mode == settings.runtime_mode)
            .order_by(ExecutionPosition.last_reconciled_at.desc().nulls_last(), ExecutionPosition.updated_at.desc())
        )
    )


def _broker_asset_unavailable(reason: str | None) -> dict[str, object]:
    return {
        "available": False,
        "source": "execution_positions.raw_payload.balance.output2",
        "source_failed": reason or "broker asset performance unavailable",
    }


def _broker_balance_snapshots(positions: list[ExecutionPosition]) -> list[BrokerBalanceSnapshot]:
    snapshots = []
    for position in positions:
        payload = position.raw_payload if isinstance(position.raw_payload, dict) else None
        if payload is None:
            continue
        balance = payload.get("balance")
        if not isinstance(balance, dict):
            continue
        output2 = _first_payload_row(balance.get("output2"))
        if output2 is None:
            continue
        observed_at = _normalize_utc(position.last_reconciled_at) if position.last_reconciled_at is not None else None
        snapshots.append(
            BrokerBalanceSnapshot(
                position=position,
                balance=balance,
                output2=output2,
                observed_at=observed_at,
            )
        )
    return sorted(
        snapshots,
        key=lambda row: (
            row.observed_at or datetime.min.replace(tzinfo=UTC),
            _normalize_utc(row.position.updated_at) if row.position.updated_at is not None else datetime.min.replace(tzinfo=UTC),
        ),
        reverse=True,
    )


def _previous_day_asset_metrics(output2: dict[str, object]) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    latest_total_asset = _decimal_value(output2.get("tot_evlu_amt"))
    latest_net_asset = _decimal_value(output2.get("nass_amt"))
    baseline_total_asset = _decimal_value(output2.get("bfdy_tot_asst_evlu_amt"))
    asset_change = _decimal_value(output2.get("asst_icdc_amt"))
    if asset_change is None and latest_total_asset is not None and baseline_total_asset is not None:
        asset_change = latest_total_asset - baseline_total_asset
    if baseline_total_asset is None and latest_total_asset is not None and asset_change is not None:
        baseline_total_asset = latest_total_asset - asset_change
    return latest_total_asset, latest_net_asset, baseline_total_asset, asset_change


def _multi_day_broker_asset_snapshots(
    *,
    snapshots: list[BrokerBalanceSnapshot],
    started_at: datetime,
) -> tuple[BrokerBalanceSnapshot | None, BrokerBalanceSnapshot | None, str | None]:
    started_at = _normalize_utc(started_at)
    latest_snapshot = next(
        (
            snapshot
            for snapshot in snapshots
            if snapshot.observed_at is not None
            and snapshot.observed_at >= started_at
            and _decimal_value(snapshot.output2.get("tot_evlu_amt")) is not None
        ),
        None,
    )
    baseline_snapshot = next(
        (
            snapshot
            for snapshot in snapshots
            if snapshot.observed_at is not None
            and snapshot.observed_at <= started_at
            and _decimal_value(snapshot.output2.get("tot_evlu_amt")) is not None
        ),
        None,
    )
    if (
        latest_snapshot is None
        or baseline_snapshot is None
        or latest_snapshot.observed_at is None
        or baseline_snapshot.observed_at is None
        or latest_snapshot.observed_at <= baseline_snapshot.observed_at
    ):
        return (
            None,
            None,
            "multi-day broker asset performance unavailable: need execution_positions raw balance snapshots "
            f"spanning started_at={started_at.isoformat()}; latest broker output2 previous-day fields "
            "bfdy_tot_asst_evlu_amt/asst_icdc_amt do not cover the report lookback",
        )
    return latest_snapshot, baseline_snapshot, None


def _first_payload_row(value: object) -> dict[str, object] | None:
    rows = _payload_rows(value)
    return rows[0] if rows else None


def _payload_rows(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _broker_holdings_from_output1(value: object) -> list[dict[str, object]]:
    holdings = []
    for row in _payload_rows(value):
        symbol = str(row.get("pdno") or row.get("symbol") or "").strip().upper()
        quantity = _decimal_value(row.get("hldg_qty"))
        if not symbol or quantity is None or quantity <= 0:
            continue
        holdings.append(
            {
                "symbol": symbol,
                "name": row.get("prdt_name") or row.get("prdt_name120"),
                "quantity": _decimal_text(quantity),
                "average_price": _decimal_text(_decimal_value(row.get("pchs_avg_pric"))),
                "purchase_amount_krw": _decimal_text(_decimal_value(row.get("pchs_amt"))),
                "market_value_krw": _decimal_text(_decimal_value(row.get("evlu_amt"))),
                "unrealized_pnl_krw": _decimal_text(_decimal_value(row.get("evlu_pfls_amt"))),
                "unrealized_pnl_pct": _decimal_text(_decimal_value(row.get("evlu_pfls_rt"))),
                "sellable_quantity": _decimal_text(_decimal_value(row.get("ord_psbl_qty"))),
                "raw_payload": _json_safe(row),
            }
        )
    return sorted(holdings, key=lambda row: _decimal_value(row.get("market_value_krw")) or Decimal("0"), reverse=True)


def _position_holding_fallback(*, position: ExecutionPosition) -> dict[str, object]:
    effective_quantity = _position_effective_quantity(position)
    market_value = Decimal(position.average_price) * effective_quantity if position.average_price is not None else None
    return {
        "symbol": position.symbol,
        "name": None,
        "quantity": _quantity_text(effective_quantity),
        "average_price": _decimal_text(Decimal(position.average_price)) if position.average_price is not None else None,
        "purchase_amount_krw": None,
        "market_value_krw": _decimal_text(market_value),
        "unrealized_pnl_krw": None,
        "unrealized_pnl_pct": None,
        "sellable_quantity": None,
        "raw_payload": None,
    }


def _asset_fifo_divergence(*, asset_change: Decimal | None, realized_pnl: Decimal) -> dict[str, object] | None:
    if asset_change is None:
        return None
    difference = asset_change - realized_pnl
    threshold = max(Decimal("50000"), abs(asset_change) * Decimal("0.10"))
    if abs(difference) < threshold:
        return None
    return {
        "type": "broker_asset_change_fifo_realized_pnl_divergence",
        "asset_change_krw": _decimal_text(asset_change),
        "fifo_realized_pnl_krw": _decimal_text(realized_pnl),
        "difference_krw": _decimal_text(difference),
        "threshold_krw": _decimal_text(threshold),
        "reason": "broker total asset movement includes open-position unrealized PnL, cash, fees, and broker reconciliation effects; FIFO realized PnL is closed-trade detail only",
    }


def _portfolio_risk(
    *,
    settings: Settings,
    db: Session,
    closed_trades: list[dict[str, object]],
    as_of: datetime,
) -> dict[str, object]:
    positions = _load_positions(settings=settings, db=db)
    open_orders = _load_open_orders(settings=settings, db=db)
    stale_before = as_of - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    position_rows = [
        _position_risk_row(position=position, sector=_sector_for_symbol(settings=settings, db=db, symbol=position.symbol))
        for position in positions
    ]
    rows_with_value = [
        row
        for row in position_rows
        if _decimal_value(row.get("market_value_krw")) is not None
    ]
    total_market_value = sum(
        (_decimal_value(row.get("market_value_krw")) or Decimal("0"))
        for row in rows_with_value
    )
    top_concentration = sorted(
        rows_with_value,
        key=lambda row: _decimal_value(row.get("market_value_krw")) or Decimal("0"),
        reverse=True,
    )[:5]
    for row in top_concentration:
        value = _decimal_value(row.get("market_value_krw")) or Decimal("0")
        row["portfolio_weight_pct"] = _decimal_text(
            (value / total_market_value * Decimal("100")) if total_market_value > 0 else Decimal("0")
        )
    stale_positions = [
        {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "base_quantity": _position_base_quantity_text(position),
            "effective_quantity": _position_effective_quantity_text(position),
            "last_reconciled_at": _normalize_utc(position.last_reconciled_at).isoformat()
            if position.last_reconciled_at is not None
            else None,
            "reason": "missing_last_reconciled_at"
            if position.last_reconciled_at is None
            else "last_reconciled_at_older_than_max_age",
        }
        for position in positions
        if position.last_reconciled_at is None or _normalize_utc(position.last_reconciled_at) < stale_before
    ]
    open_order_rows = [_open_order_risk_row(order=order, as_of=as_of, stale_before=stale_before) for order in open_orders]
    stale_orders = [row for row in open_order_rows if row.get("stale") is True]
    normal_open_orders = [row for row in open_order_rows if row.get("stale") is not True]
    stale_state_blockers = [_stale_state_blocker_row(row) for row in stale_orders]
    account_source_failed = None
    if not position_rows:
        account_source_failed = (
            f"execution_positions returned no positive local positions for mode={settings.runtime_mode}; "
            "broker account snapshots are not persisted by trade_outcome_feedback"
        )
    contributions = _realized_contributions(closed_trades)
    sector_concentration = _sector_concentration(position_rows) if any(row.get("sector") for row in position_rows) else []
    monday_checks = _monday_pre_market_checks(
        positions=position_rows,
        stale_positions=stale_positions,
        open_orders=open_order_rows,
    )
    return {
        "available": bool(position_rows),
        "source": "execution_positions",
        "source_failed": account_source_failed,
        "as_of": as_of.isoformat(),
        "stale_threshold_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
        "open_holding_count": len(position_rows) if position_rows else None,
        "total_market_value_krw": _decimal_text(total_market_value) if position_rows and len(rows_with_value) == len(position_rows) else None,
        "positions_missing_market_value_count": len(position_rows) - len(rows_with_value),
        "top_position_concentration": _json_safe(top_concentration),
        "sector_concentration": _json_safe(sector_concentration),
        "realized_pnl_by_symbol": contributions["by_symbol"],
        "sell_reason_contributions": contributions["by_sell_reason"],
        "stop_sell_loss_contribution": contributions["stop_sell_loss"],
        "target_sell_profit_contribution": contributions["target_sell_profit"],
        "max_holding_close_contribution": contributions["max_holding_close"],
        "open_orders": _json_safe(open_order_rows),
        "normal_open_orders": _json_safe(normal_open_orders),
        "stale_state_blockers": _json_safe(stale_state_blockers),
        "stale_open_order_warnings": _json_safe(stale_orders),
        "stale_local_position_warnings": _json_safe(stale_positions),
        "monday_pre_market_checks": _json_safe(monday_checks),
    }


def _load_positions(*, settings: Settings, db: Session) -> list[ExecutionPosition]:
    return list(
        db.scalars(
            select(ExecutionPosition)
            .where(
                ExecutionPosition.mode == settings.runtime_mode,
                or_(
                    ExecutionPosition.quantity > 0,
                    ExecutionPosition.base_quantity > 0,
                ),
            )
            .order_by(ExecutionPosition.symbol.asc())
        )
    )


def _load_open_orders(*, settings: Settings, db: Session) -> list[BrokerOrder]:
    terminal_statuses = {
        "filled",
        "canceled",
        "cancelled",
        "rejected",
        "expired",
        "failed",
        "closed",
    }
    return list(
        db.scalars(
            select(BrokerOrder)
            .where(BrokerOrder.mode == settings.runtime_mode)
            .where(BrokerOrder.status.not_in(terminal_statuses))
            .options(selectinload(BrokerOrder.plan_item), selectinload(BrokerOrder.fills))
            .order_by(BrokerOrder.symbol.asc(), BrokerOrder.submitted_at.asc().nulls_last(), BrokerOrder.id.asc())
        )
    )


def _position_risk_row(*, position: ExecutionPosition, sector: str | None) -> dict[str, object]:
    effective_quantity = _position_effective_quantity(position)
    market_value = None
    if position.average_price is not None and effective_quantity > 0:
        market_value = Decimal(position.average_price) * effective_quantity
    return {
        "symbol": position.symbol,
        "quantity": position.quantity,
        "base_quantity": _position_base_quantity_text(position),
        "effective_quantity": _position_effective_quantity_text(position),
        "average_price": _decimal_text(Decimal(position.average_price)) if position.average_price is not None else None,
        "market_value_krw": _decimal_text(market_value),
        "last_reconciled_at": _normalize_utc(position.last_reconciled_at).isoformat()
        if position.last_reconciled_at is not None
        else None,
        "updated_from": position.updated_from,
        "sector": sector,
    }


def _position_effective_quantity(position: ExecutionPosition) -> Decimal:
    if position.quantity > 0:
        return Decimal(position.quantity)
    base_quantity = Decimal(position.base_quantity) if position.base_quantity is not None else Decimal("0")
    return base_quantity if base_quantity > 0 else Decimal("0")


def _position_base_quantity_text(position: ExecutionPosition) -> str | None:
    if position.base_quantity is None:
        return None
    return _quantity_text(Decimal(position.base_quantity))


def _position_effective_quantity_text(position: ExecutionPosition) -> str:
    if position.quantity > 0:
        return _decimal_text(Decimal(position.quantity)) or "0.0000"
    return _quantity_text(_position_effective_quantity(position))


def _open_order_risk_row(*, order: BrokerOrder, as_of: datetime, stale_before: datetime) -> dict[str, object]:
    reference_at = order.last_broker_sync_at or order.submitted_at or order.updated_at or order.created_at
    normalized_reference = _normalize_utc(reference_at) if reference_at is not None else None
    remaining_quantity = _remaining_order_quantity(order)
    is_stale = normalized_reference is None or normalized_reference < stale_before
    return {
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "execution_plan_item_id": order.execution_plan_item_id,
        "symbol": order.symbol,
        "side": order.side,
        "intent": order.plan_item.intent if order.plan_item is not None else None,
        "status": order.status,
        "quantity": order.quantity,
        "remaining_quantity": remaining_quantity,
        "limit_price": _decimal_text(Decimal(order.limit_price)) if order.limit_price is not None else None,
        "submitted_at": _normalize_utc(order.submitted_at).isoformat() if order.submitted_at is not None else None,
        "last_broker_sync_at": _normalize_utc(order.last_broker_sync_at).isoformat()
        if order.last_broker_sync_at is not None
        else None,
        "age_seconds": int((as_of - normalized_reference).total_seconds()) if normalized_reference is not None else None,
        "stale": is_stale,
        "stale_reason": "missing_order_timestamp" if normalized_reference is None else ("older_than_max_age" if is_stale else None),
        "last_error_classification": order.last_error_classification,
        "last_error_code": order.last_error_code,
        "last_error_message": order.last_error_message,
    }


def _stale_state_blocker_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "type": "stale_open_order_broker_truth",
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "status": row.get("status"),
        "broker_order_id": row.get("broker_order_id"),
        "broker_order_pk": row.get("broker_order_pk"),
        "stale_reason": row.get("stale_reason"),
        "last_broker_sync_at": row.get("last_broker_sync_at"),
        "age_seconds": row.get("age_seconds"),
        "last_error_classification": row.get("last_error_classification"),
        "last_error_code": row.get("last_error_code"),
        "last_error_message": row.get("last_error_message"),
        "reason": "refresh or resolve broker truth before new automation for this symbol/account",
    }


def _remaining_order_quantity(order: BrokerOrder) -> int | None:
    if order.quantity is None:
        return None
    filled_quantity = sum(int(fill.quantity) for fill in order.fills)
    return max(0, int(order.quantity) - filled_quantity)


def _realized_contributions(closed_trades: list[dict[str, object]]) -> dict[str, object]:
    by_symbol: dict[str, list[Decimal]] = {}
    by_reason: dict[str, list[Decimal]] = {}
    for trade in closed_trades:
        symbol = str(trade.get("symbol") or "unknown")
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        reason = _sell_reason(trade)
        by_symbol.setdefault(symbol, []).append(pnl)
        by_reason.setdefault(reason, []).append(pnl)
    return {
        "by_symbol": _contribution_rows(by_symbol),
        "by_sell_reason": _contribution_rows(by_reason),
        "stop_sell_loss": _reason_filtered_contribution(closed_trades, reason="stop_sell", pnl_filter="loss"),
        "target_sell_profit": _reason_filtered_contribution(closed_trades, reason="target_sell", pnl_filter="profit"),
        "max_holding_close": _reason_filtered_contribution(closed_trades, reason="max_holding_close_sell", pnl_filter=None),
    }


def _contribution_rows(stats: dict[str, list[Decimal]]) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        rows.append(
            {
                "name": name,
                "trade_count": len(values),
                "realized_pnl_krw": _decimal_text(total),
            }
        )
    return sorted(rows, key=lambda row: Decimal(str(row["realized_pnl_krw"])))


def _reason_filtered_contribution(
    closed_trades: list[dict[str, object]],
    *,
    reason: str,
    pnl_filter: str | None,
) -> dict[str, object]:
    values = []
    symbols: dict[str, list[Decimal]] = {}
    for trade in closed_trades:
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        if _sell_reason(trade) != reason:
            continue
        if pnl_filter == "loss" and pnl >= 0:
            continue
        if pnl_filter == "profit" and pnl <= 0:
            continue
        values.append(pnl)
        symbols.setdefault(str(trade.get("symbol") or "unknown"), []).append(pnl)
    return {
        "trade_count": len(values),
        "realized_pnl_krw": _decimal_text(sum(values, Decimal("0"))),
        "by_symbol": _contribution_rows(symbols),
    }


def _sell_reason(trade: dict[str, object]) -> str:
    sell = trade.get("sell") if isinstance(trade.get("sell"), dict) else {}
    intent = str(sell.get("intent") or "").strip()
    if intent:
        return intent
    decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
    reason = str(decision.get("reason_code") or "").strip()
    return reason or "unknown"


def _sector_for_symbol(*, settings: Settings, db: Session, symbol: str) -> str | None:
    items = db.scalars(
        select(ExecutionPlanItem)
        .where(ExecutionPlanItem.symbol == symbol, ExecutionPlanItem.plan.has(mode=settings.runtime_mode))
        .order_by(ExecutionPlanItem.updated_at.desc().nulls_last(), ExecutionPlanItem.created_at.desc())
        .limit(20)
    )
    for item in items:
        if not isinstance(item.risk_context, dict):
            continue
        for key in ("sector", "sector_name", "industry", "theme"):
            value = item.risk_context.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _sector_concentration(position_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    stats: dict[str, list[Decimal]] = {}
    for row in position_rows:
        sector = str(row.get("sector") or "").strip()
        value = _decimal_value(row.get("market_value_krw"))
        if sector and value is not None:
            stats.setdefault(sector, []).append(value)
    total = sum((sum(values, Decimal("0")) for values in stats.values()), Decimal("0"))
    rows = []
    for sector, values in stats.items():
        value = sum(values, Decimal("0"))
        rows.append(
            {
                "sector": sector,
                "position_count": len(values),
                "market_value_krw": _decimal_text(value),
                "portfolio_weight_pct": _decimal_text((value / total * Decimal("100")) if total > 0 else Decimal("0")),
            }
        )
    return sorted(rows, key=lambda row: Decimal(str(row["market_value_krw"])), reverse=True)


def _monday_pre_market_checks(
    *,
    positions: list[dict[str, object]],
    stale_positions: list[dict[str, object]],
    open_orders: list[dict[str, object]],
) -> list[dict[str, object]]:
    checks = []
    for order in open_orders:
        checks.append(
            {
                "type": "open_order",
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "status": order.get("status"),
                "reason": "confirm broker open-order truth before Monday pre-market",
            }
        )
    stale_symbols = {str(row.get("symbol")) for row in stale_positions}
    for position in positions:
        if str(position.get("symbol")) in stale_symbols:
            checks.append(
                {
                    "type": "stale_local_position",
                    "symbol": position.get("symbol"),
                    "quantity": position.get("quantity"),
                    "base_quantity": position.get("base_quantity"),
                    "effective_quantity": position.get("effective_quantity"),
                    "reason": "refresh local position against broker truth before Monday pre-market",
                }
            )
        else:
            checks.append(
                {
                    "type": "open_position",
                    "symbol": position.get("symbol"),
                    "quantity": position.get("quantity"),
                    "base_quantity": position.get("base_quantity"),
                    "effective_quantity": position.get("effective_quantity"),
                    "market_value_krw": position.get("market_value_krw"),
                    "reason": "review concentration and exit monitors before Monday pre-market",
                }
            )
    return checks


def _lessons(closed_trades: list[dict[str, object]], *, max_lessons: int) -> dict[str, object]:
    worst_losses = sorted(
        closed_trades,
        key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"),
    )[:max_lessons]
    profile_stats: dict[str, list[Decimal]] = {}
    reason_stats: dict[str, list[Decimal]] = {}
    for trade in closed_trades:
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
        profile = str(decision.get("target_profile") or "unknown")
        reason = str(decision.get("reason_code") or "unknown")
        profile_stats.setdefault(profile, []).append(pnl)
        reason_stats.setdefault(reason, []).append(pnl)
    return {
        "worst_losses": [_lesson_trade(row) for row in worst_losses],
        "patterns_to_downsize_or_reject": _negative_patterns(profile_stats, max_lessons=max_lessons),
        "positive_expectancy_patterns": _positive_patterns(profile_stats, max_lessons=max_lessons),
        "negative_reason_codes": _negative_patterns(reason_stats, max_lessons=max_lessons),
    }


def _candidate_rules(settings: Settings) -> list[CandidateRule]:
    return [
        CandidateRule(
            rule_id="stale_trigger_drift_veto",
            name="Stale trigger drift veto",
            action="veto",
            size_multiplier=Decimal("0"),
            threshold=Decimal(settings.watch_buy_stale_trigger_reclaim_threshold_pct),
            evaluator=_rule_stale_trigger_drift,
        ),
        CandidateRule(
            rule_id="high_volatility_downsize",
            name="High volatility downsize",
            action="downsize",
            size_multiplier=Decimal("0.50"),
            threshold=Decimal(settings.watch_buy_high_atr_pct_threshold),
            evaluator=_rule_high_volatility,
        ),
        CandidateRule(
            rule_id="high_correlation_downsize",
            name="High correlation downsize",
            action="downsize",
            size_multiplier=Decimal("0.50"),
            threshold=Decimal("0.75"),
            evaluator=_rule_high_correlation,
        ),
        CandidateRule(
            rule_id="weak_confidence_reject",
            name="Weak confidence reject",
            action="reject",
            size_multiplier=Decimal("0"),
            threshold=Decimal("0.50"),
            evaluator=_rule_weak_confidence,
        ),
    ]


def _candidate_rule_replay(*, settings: Settings, closed_trades: list[dict[str, object]]) -> dict[str, object]:
    baseline_pnls = [_decimal_value(row.get("realized_pnl_krw")) or Decimal("0") for row in closed_trades]
    baseline_returns = [_decimal_value(row.get("return_pct")) or Decimal("0") for row in closed_trades]
    baseline_gross = sum(baseline_pnls, Decimal("0"))
    baseline_win_rate = _win_rate_pct(baseline_pnls)
    baseline_avg_return = _average_decimal(baseline_returns)
    fee_slippage_bps = _fee_slippage_bps(closed_trades)
    return {
        "source": "execution_fills+broker_orders+execution_plan_items.risk_context",
        "baseline_closed_trade_count": len(closed_trades),
        "baseline_gross_pnl_krw": _decimal_text(baseline_gross),
        "baseline_win_rate_pct": _decimal_text(baseline_win_rate),
        "baseline_average_return_pct": _decimal_text(baseline_avg_return),
        "fee_slippage_assumption_bps": _decimal_text(fee_slippage_bps),
        "rules": [
            _candidate_rule_result(
                rule=rule,
                closed_trades=closed_trades,
                baseline_pnls=baseline_pnls,
                baseline_returns=baseline_returns,
                baseline_gross=baseline_gross,
                baseline_win_rate=baseline_win_rate,
                baseline_avg_return=baseline_avg_return,
                fee_slippage_bps=fee_slippage_bps,
            )
            for rule in _candidate_rules(settings)
        ],
    }


def _candidate_rule_result(
    *,
    rule: CandidateRule,
    closed_trades: list[dict[str, object]],
    baseline_pnls: list[Decimal],
    baseline_returns: list[Decimal],
    baseline_gross: Decimal,
    baseline_win_rate: Decimal,
    baseline_avg_return: Decimal,
    fee_slippage_bps: Decimal | None,
) -> dict[str, object]:
    examples: list[dict[str, object]] = []
    candidate_pnls: list[Decimal] = []
    candidate_returns: list[Decimal] = []
    skipped_entries = 0
    skipped_winners = 0
    avoided_losers = 0
    affected_count = 0
    estimated_cost_savings = Decimal("0")
    for trade, baseline_pnl, baseline_return in zip(closed_trades, baseline_pnls, baseline_returns, strict=False):
        matched, evidence = rule.evaluator(trade, rule)
        multiplier = rule.size_multiplier if matched else Decimal("1")
        if matched:
            affected_count += 1
            if multiplier <= 0:
                skipped_entries += 1
                if baseline_pnl > 0:
                    skipped_winners += 1
            if baseline_pnl < 0:
                avoided_losers += 1
            examples.append(_candidate_rule_example(trade=trade, baseline_pnl=baseline_pnl, evidence=evidence))
            if fee_slippage_bps is not None:
                estimated_cost_savings += _trade_round_trip_cost(trade=trade, fee_slippage_bps=fee_slippage_bps) * (Decimal("1") - multiplier)
        candidate_pnls.append(baseline_pnl * multiplier)
        candidate_returns.append(baseline_return * multiplier)
    candidate_gross = sum(candidate_pnls, Decimal("0"))
    gross_delta = candidate_gross - baseline_gross
    win_rate_delta = _win_rate_pct(candidate_pnls) - baseline_win_rate
    avg_return_delta = _average_decimal(candidate_returns) - baseline_avg_return
    max_drawdown_proxy = _max_drawdown_proxy(candidate_pnls)
    net_delta = gross_delta + estimated_cost_savings if fee_slippage_bps is not None else None
    return {
        "rule_id": rule.rule_id,
        "rule_name": rule.name,
        "action": rule.action,
        "threshold": _decimal_text(rule.threshold),
        "size_multiplier": _decimal_text(rule.size_multiplier),
        "baseline_closed_trade_count": len(closed_trades),
        "candidate_closed_trade_count": len(candidate_pnls) - skipped_entries,
        "matched_trade_count": affected_count,
        "skipped_entries": skipped_entries,
        "skipped_winners": skipped_winners,
        "avoided_losers": avoided_losers,
        "gross_pnl_delta_krw": _decimal_text(gross_delta),
        "estimated_net_pnl_delta_krw": _decimal_text(net_delta),
        "win_rate_delta_pct": _decimal_text(win_rate_delta),
        "average_return_delta_pct": _decimal_text(avg_return_delta),
        "max_drawdown_proxy_krw": _decimal_text(max_drawdown_proxy),
        "recommendation": _candidate_rule_recommendation(
            matched_trade_count=affected_count,
            gross_delta=gross_delta,
            skipped_winners=skipped_winners,
        ),
        "top_skipped_winners": _top_candidate_examples(examples, outcome="win")[:5],
        "top_avoided_losers": _top_candidate_examples(examples, outcome="loss")[:5],
    }


def _candidate_rule_recommendation(*, matched_trade_count: int, gross_delta: Decimal, skipped_winners: int) -> str:
    if matched_trade_count < MIN_CANDIDATE_RULE_SAMPLE_SIZE:
        return "needs_more_data"
    if gross_delta > 0 and skipped_winners == 0:
        return "promote_to_issue"
    if gross_delta < 0:
        return "reject_candidate"
    return "needs_more_data"


def _candidate_rule_example(
    *,
    trade: dict[str, object],
    baseline_pnl: Decimal,
    evidence: dict[str, object],
) -> dict[str, object]:
    buy = trade.get("buy") if isinstance(trade.get("buy"), dict) else {}
    return {
        "symbol": trade.get("symbol"),
        "realized_pnl_krw": _decimal_text(baseline_pnl),
        "return_pct": trade.get("return_pct"),
        "buy_filled_at": buy.get("filled_at"),
        "execution_plan_item_id": buy.get("execution_plan_item_id") or trade.get("source_execution_plan_item_id"),
        "decision_run_id": trade.get("decision_run_id"),
        "evidence": _json_safe(evidence),
    }


def _top_candidate_examples(examples: list[dict[str, object]], *, outcome: str) -> list[dict[str, object]]:
    if outcome == "win":
        winners = [row for row in examples if (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0")) > 0]
        return sorted(winners, key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"), reverse=True)
    losers = [row for row in examples if (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0")) < 0]
    return sorted(losers, key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"))


def _rule_stale_trigger_drift(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    trigger_price = _candidate_context_decimal(trade, keys=("trigger_price", "watch_trigger_price", "buy_trigger_price", "entry_trigger_price"))
    buy_price = _nested_decimal(trade, ("buy", "price"))
    if trigger_price is None or trigger_price <= 0 or buy_price is None:
        return False, {"reason": "missing_trigger_or_buy_price"}
    drift_pct = (buy_price - trigger_price) / trigger_price * Decimal("100")
    return drift_pct >= rule.threshold, {"trigger_price": _decimal_text(trigger_price), "buy_price": _decimal_text(buy_price), "drift_pct": _decimal_text(drift_pct)}


def _rule_high_volatility(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "atr_pct",
            "atr_pct_14",
            "atr14_pct",
            "atr_percent",
            "volatility_pct",
            "realized_volatility_pct",
            "intraday_range_pct",
            "daily_range_pct",
        ),
    )
    if value is None:
        return False, {"reason": "missing_volatility_context"}
    return value >= rule.threshold, {"field": key, "value_pct": _decimal_text(value)}


def _rule_high_correlation(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "portfolio_correlation",
            "max_correlation",
            "correlation",
            "sector_correlation",
            "market_correlation",
        ),
    )
    if value is None:
        return False, {"reason": "missing_correlation_context"}
    return value >= rule.threshold, {"field": key, "value": _decimal_text(value)}


def _rule_weak_confidence(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "confidence",
            "confidence_score",
            "confidence_score_10",
            "probability",
            "score",
        ),
    )
    if value is None:
        return False, {"reason": "missing_confidence_context"}
    normalized = value / Decimal("10") if key == "confidence_score_10" or value > 1 else value
    return normalized < rule.threshold, {"field": key, "confidence": _decimal_text(normalized)}


def _candidate_context_decimal(trade: dict[str, object], *, keys: tuple[str, ...]) -> Decimal | None:
    return _candidate_context_first_decimal(trade, keys=keys)[0]


def _candidate_context_first_decimal(trade: dict[str, object], *, keys: tuple[str, ...]) -> tuple[Decimal | None, str | None]:
    contexts = [
        trade.get("candidate_rule_context"),
        trade.get("exec_agent_decision"),
        trade.get("risk_sizing"),
        trade.get("trigger_time"),
        trade.get("market_enrichment"),
    ]
    buy = trade.get("buy") if isinstance(trade.get("buy"), dict) else {}
    contexts.append(buy)
    for context in contexts:
        value, key = _first_decimal_deep(context, keys=keys)
        if value is not None:
            return value, key
    return None, None


def _first_decimal_deep(value: object, *, keys: tuple[str, ...]) -> tuple[Decimal | None, str | None]:
    if isinstance(value, dict):
        for key in keys:
            parsed = _decimal_value(value.get(key))
            if parsed is not None:
                return parsed, key
        for nested in value.values():
            parsed, key = _first_decimal_deep(nested, keys=keys)
            if parsed is not None:
                return parsed, key
    if isinstance(value, list):
        for nested in value:
            parsed, key = _first_decimal_deep(nested, keys=keys)
            if parsed is not None:
                return parsed, key
    return None, None


def _nested_decimal(value: dict[str, object], path: tuple[str, ...]) -> Decimal | None:
    current: object = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _decimal_value(current)


def _fee_slippage_bps(closed_trades: list[dict[str, object]]) -> Decimal | None:
    values: list[Decimal] = []
    for trade in closed_trades:
        value, _key = _candidate_context_first_decimal(
            trade,
            keys=(
                "fee_slippage_bps",
                "round_trip_cost_bps",
                "estimated_round_trip_cost_bps",
                "fee_bps",
                "slippage_bps",
            ),
        )
        if value is not None and value >= 0:
            values.append(value)
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _trade_round_trip_cost(*, trade: dict[str, object], fee_slippage_bps: Decimal) -> Decimal:
    buy_notional = (_nested_decimal(trade, ("buy", "price")) or Decimal("0")) * (_nested_decimal(trade, ("buy", "quantity")) or Decimal("0"))
    sell_notional = (_nested_decimal(trade, ("sell", "price")) or Decimal("0")) * (_nested_decimal(trade, ("sell", "quantity")) or Decimal("0"))
    return (buy_notional + sell_notional) * fee_slippage_bps / Decimal("10000")


def _win_rate_pct(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return Decimal(len([value for value in values if value > 0])) / Decimal(len(values)) * Decimal("100")


def _average_decimal(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _max_drawdown_proxy(values: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    max_drawdown = Decimal("0")
    for value in values:
        equity += value
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _candidate_rule_replay_snippet_lines(candidate_rule_replay: dict[str, object]) -> list[str]:
    rows = candidate_rule_replay.get("rules")
    if not isinstance(rows, list):
        return []
    lines = []
    for row in rows[:6]:
        if isinstance(row, dict):
            lines.append(
                f"{row.get('rule_id')}: recommendation={row.get('recommendation')}, "
                f"gross_delta={row.get('gross_pnl_delta_krw')} KRW, "
                f"skipped_winners={row.get('skipped_winners')}, avoided_losers={row.get('avoided_losers')}."
            )
    return lines


def _snippet(
    *,
    summary: dict[str, object],
    lessons: dict[str, object],
    portfolio_risk: dict[str, object],
    candidate_rule_replay: dict[str, object],
    broker_asset_performance: dict[str, object],
) -> dict[str, object]:
    hard_gates = []
    for row in lessons.get("patterns_to_downsize_or_reject", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Downsize/reject target_profile={row.get('name')} until evidence improves; avg_pnl={row.get('avg_pnl_krw')} over {row.get('count')} closed trades."
            )
    for row in lessons.get("worst_losses", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Recent worst loss {row.get('symbol')} {row.get('return_pct')}% ({row.get('realized_pnl_krw')} KRW), profile={row.get('target_profile')}, reason={row.get('reason_code')}."
            )
    if broker_asset_performance.get("available"):
        snippet_summary = (
            f"Broker total asset change: {broker_asset_performance.get('asset_change_krw')} KRW "
            f"({broker_asset_performance.get('asset_change_pct')}%), "
            f"baseline={broker_asset_performance.get('baseline_total_asset_krw')} KRW, "
            f"latest_total={broker_asset_performance.get('latest_total_asset_krw')} KRW, "
            f"net_asset={broker_asset_performance.get('latest_net_asset_krw')} KRW. "
            f"FIFO closed-trade realized PnL: {summary.get('realized_pnl_krw')} KRW."
        )
    else:
        snippet_summary = (
            f"Recent closed trades: pnl={summary.get('realized_pnl_krw')} KRW, "
            f"win_rate={summary.get('win_rate_pct')}%, worst_loss={summary.get('worst_loss_krw')} KRW."
        )
    return {
        "summary": snippet_summary,
        "patterns_to_downsize_or_reject": hard_gates[:6],
        "positive_expectancy_patterns": lessons.get("positive_expectancy_patterns", []),
        "broker_asset_warnings": _broker_asset_warning_lines(broker_asset_performance),
        "top_portfolio_risks": _portfolio_risk_snippet_lines(portfolio_risk),
        "monday_pre_market_checks": _monday_check_snippet_lines(portfolio_risk),
        "candidate_rule_replay": _candidate_rule_replay_snippet_lines(candidate_rule_replay),
        "required_hard_gates": [
            "Do not increase size for profiles/reason_codes listed as negative without fresh superior live quality.",
            "Prefer delay/reject over execute_now when recent worst-loss conditions resemble the current candidate.",
        ],
    }


def _broker_asset_warning_lines(broker_asset_performance: dict[str, object]) -> list[str]:
    if not broker_asset_performance.get("available"):
        return [f"Broker asset performance unavailable: {broker_asset_performance.get('source_failed')}"]
    lines = []
    divergence = broker_asset_performance.get("divergence_warning")
    if isinstance(divergence, dict):
        lines.append(
            f"Divergence warning: broker asset change={divergence.get('asset_change_krw')} KRW, "
            f"FIFO realized={divergence.get('fifo_realized_pnl_krw')} KRW, "
            f"difference={divergence.get('difference_krw')} KRW."
        )
    unrealized = broker_asset_performance.get("broker_unrealized_pnl_krw")
    if unrealized is not None:
        lines.append(f"Broker unrealized PnL summary={unrealized} KRW.")
    holdings = broker_asset_performance.get("holdings")
    if isinstance(holdings, list):
        for row in holdings[:3]:
            if isinstance(row, dict):
                lines.append(
                    f"Holding {row.get('symbol')}: unrealized={row.get('unrealized_pnl_krw')} KRW, "
                    f"market_value={row.get('market_value_krw')} KRW."
                )
    return lines[:6]


def _portfolio_risk_snippet_lines(portfolio_risk: dict[str, object]) -> list[str]:
    if not portfolio_risk.get("available"):
        return [f"Portfolio risk unavailable: {portfolio_risk.get('source_failed')}"]
    lines = [
        (
            f"Open holdings={portfolio_risk.get('open_holding_count')}, "
            f"market_value={portfolio_risk.get('total_market_value_krw')} KRW."
        )
    ]
    top_positions = portfolio_risk.get("top_position_concentration")
    if isinstance(top_positions, list):
        for row in top_positions[:3]:
            if isinstance(row, dict):
                lines.append(
                    f"Top concentration {row.get('symbol')}: {row.get('market_value_krw')} KRW ({row.get('portfolio_weight_pct')}%)."
                )
    stale_orders = portfolio_risk.get("stale_open_order_warnings")
    stale_blockers = portfolio_risk.get("stale_state_blockers")
    stale_positions = portfolio_risk.get("stale_local_position_warnings")
    if isinstance(stale_blockers, list) and stale_blockers:
        lines.append(f"Stale-state blockers need broker-truth refresh: {len(stale_blockers)}.")
    if isinstance(stale_orders, list) and stale_orders:
        lines.append(f"Stale open orders need review: {len(stale_orders)}.")
    if isinstance(stale_positions, list) and stale_positions:
        lines.append(f"Stale local positions need broker-truth refresh: {len(stale_positions)}.")
    return lines[:6]


def _monday_check_snippet_lines(portfolio_risk: dict[str, object]) -> list[str]:
    checks = portfolio_risk.get("monday_pre_market_checks")
    if not isinstance(checks, list) or not checks:
        if portfolio_risk.get("source_failed"):
            return [f"Confirm missing account/position source before Monday: {portfolio_risk.get('source_failed')}"]
        return []
    lines = []
    for row in checks[:8]:
        if isinstance(row, dict):
            lines.append(f"{row.get('type')} {row.get('symbol')}: {row.get('reason')}")
    return lines


def _write_artifacts(*, report: dict[str, object], report_date: date, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_date.isoformat()}.json"
    md_path = output_dir / f"{report_date.isoformat()}.md"
    payload = _json_safe(report)
    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    json_path.write_text(json_text + "\n", encoding="utf-8")
    (output_dir / "latest.json").write_text(json_text + "\n", encoding="utf-8")
    md_path.write_text(_markdown_report(payload), encoding="utf-8")
    (output_dir / "latest.md").write_text(_markdown_report(payload), encoding="utf-8")


def _markdown_report(report: dict[str, object]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    snippet = report.get("prompt_snippet") if isinstance(report.get("prompt_snippet"), dict) else {}
    broker_asset = report.get("broker_asset_performance") if isinstance(report.get("broker_asset_performance"), dict) else {}
    portfolio_risk = report.get("portfolio_risk") if isinstance(report.get("portfolio_risk"), dict) else {}
    candidate_rule_replay = report.get("candidate_rule_replay") if isinstance(report.get("candidate_rule_replay"), dict) else {}
    lines = [
        f"# Trade Outcome Feedback {str(report.get('generated_at'))[:10]}",
        "",
        f"- Mode: {report.get('mode')}",
        f"- Closed trades: {report.get('closed_trade_count')}",
        f"- Primary performance metric: {summary.get('primary_performance_metric')}",
        f"- Broker total asset change: {summary.get('broker_total_asset_change_krw')} KRW ({summary.get('broker_total_asset_change_pct')}%)"
        if broker_asset.get("available")
        else f"- Broker total asset change: unavailable ({broker_asset.get('source_failed')})",
        f"- FIFO realized P/L: {summary.get('realized_pnl_krw')} KRW",
        f"- Win rate: {summary.get('win_rate_pct')}%",
        f"- Worst loss: {summary.get('worst_loss_krw')} KRW",
        "",
        "## 이번주 분석 결과",
        "",
        f"- 브로커 총자산 변화: {summary.get('broker_total_asset_change_krw')} KRW ({summary.get('broker_total_asset_change_pct')}%)"
        if broker_asset.get("available")
        else f"- 브로커 총자산 변화: 사용 불가 ({broker_asset.get('source_failed')})",
        f"- FIFO 실현손익(보조 지표): {summary.get('realized_pnl_krw')} KRW",
        f"- 승/패: {summary.get('win_count')}승 / {summary.get('loss_count')}패",
        "- 근거: execution_positions.raw_payload.balance.output2, execution_positions.raw_payload.balance.output1, execution_fills, broker_orders",
        "",
        "### Broker Total Asset Performance",
        "",
    ]
    if broker_asset.get("available"):
        lines.extend(
            [
                f"- Baseline total asset: {broker_asset.get('baseline_total_asset_krw')} KRW",
                f"- Latest total asset: {broker_asset.get('latest_total_asset_krw')} KRW",
                f"- Latest net asset: {broker_asset.get('latest_net_asset_krw')} KRW",
                f"- Asset change: {broker_asset.get('asset_change_krw')} KRW ({broker_asset.get('asset_change_pct')}%)",
                f"- Broker unrealized P/L: {broker_asset.get('broker_unrealized_pnl_krw')} KRW",
                f"- Securities value: {broker_asset.get('broker_securities_value_krw')} KRW",
                f"- Cash: {broker_asset.get('broker_cash_krw')} KRW",
                f"- FIFO realized P/L detail: {broker_asset.get('fifo_realized_pnl_krw')} KRW",
            ]
        )
        divergence = broker_asset.get("divergence_warning")
        if isinstance(divergence, dict):
            lines.append(
                f"- Divergence warning: asset change {divergence.get('asset_change_krw')} KRW vs "
                f"FIFO realized {divergence.get('fifo_realized_pnl_krw')} KRW "
                f"(difference {divergence.get('difference_krw')} KRW)"
            )
        holdings = broker_asset.get("holdings")
        if isinstance(holdings, list) and holdings:
            lines.append("- Current holdings from broker output1:")
            for row in holdings[:10]:
                if isinstance(row, dict):
                    lines.append(
                        f"  - {row.get('symbol')}: qty={row.get('quantity')}, "
                        f"value={row.get('market_value_krw')} KRW, unrealized={row.get('unrealized_pnl_krw')} KRW"
                    )
    else:
        lines.append(f"- 사용 불가: {broker_asset.get('source_failed')}")
    lines.extend(
        [
            "",
            "### 포트폴리오 리스크",
            "",
        ]
    )
    if portfolio_risk.get("available"):
        lines.extend(
            [
                f"- 보유 종목 수: {portfolio_risk.get('open_holding_count')}",
                f"- 총 평가 금액: {portfolio_risk.get('total_market_value_krw')} KRW",
                f"- 평가 금액 누락 포지션: {portfolio_risk.get('positions_missing_market_value_count')}",
            ]
        )
        top_positions = portfolio_risk.get("top_position_concentration")
        if isinstance(top_positions, list) and top_positions:
            lines.append("- 상위 집중:")
            for row in top_positions[:5]:
                if isinstance(row, dict):
                    lines.append(
                        f"  - {row.get('symbol')}: {row.get('market_value_krw')} KRW ({row.get('portfolio_weight_pct')}%)"
                    )
    else:
        lines.append(f"- 사용 불가: {portfolio_risk.get('source_failed')}")
    for label, key in (
        ("종목별 실현손익", "realized_pnl_by_symbol"),
        ("매도 사유별 실현손익", "sell_reason_contributions"),
    ):
        rows = portfolio_risk.get(key)
        if isinstance(rows, list) and rows:
            lines.append(f"- {label}:")
            for row in rows:
                if isinstance(row, dict):
                    lines.append(f"  - {row.get('name')}: {row.get('realized_pnl_krw')} KRW ({row.get('trade_count')}건)")
    stale_orders = portfolio_risk.get("stale_open_order_warnings")
    stale_blockers = portfolio_risk.get("stale_state_blockers")
    stale_positions = portfolio_risk.get("stale_local_position_warnings")
    if isinstance(stale_blockers, list) and stale_blockers:
        lines.append(f"- 오래된 주문 상태 차단: {len(stale_blockers)}건")
    if isinstance(stale_orders, list) and stale_orders:
        lines.append(f"- 오래된 미체결 주문 경고: {len(stale_orders)}건")
    if isinstance(stale_positions, list) and stale_positions:
        lines.append(f"- 오래된 로컬 포지션 경고: {len(stale_positions)}건")
    lines.extend(
        [
            "",
            "### Candidate Rule Replay",
            "",
            f"- Baseline closed trades: {candidate_rule_replay.get('baseline_closed_trade_count')}",
            f"- Baseline gross P/L: {candidate_rule_replay.get('baseline_gross_pnl_krw')} KRW",
        ]
    )
    replay_rules = candidate_rule_replay.get("rules")
    if isinstance(replay_rules, list) and replay_rules:
        for row in replay_rules:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {row.get('rule_id')} ({row.get('rule_name')}): "
                f"recommendation={row.get('recommendation')}, "
                f"gross_delta={row.get('gross_pnl_delta_krw')} KRW, "
                f"net_delta={row.get('estimated_net_pnl_delta_krw')} KRW, "
                f"win_rate_delta={row.get('win_rate_delta_pct')}%, "
                f"avg_return_delta={row.get('average_return_delta_pct')}%, "
                f"drawdown_proxy={row.get('max_drawdown_proxy_krw')} KRW, "
                f"skipped={row.get('skipped_entries')}, skipped_winners={row.get('skipped_winners')}, avoided_losers={row.get('avoided_losers')}"
            )
            avoided = row.get("top_avoided_losers")
            if isinstance(avoided, list) and avoided:
                lines.append("  - Top avoided losers:")
                for example in avoided[:5]:
                    if isinstance(example, dict):
                        lines.append(
                            f"    - {example.get('symbol')}: {example.get('realized_pnl_krw')} KRW ({example.get('return_pct')}%)"
                        )
            skipped = row.get("top_skipped_winners")
            if isinstance(skipped, list) and skipped:
                lines.append("  - Top skipped winners:")
                for example in skipped[:5]:
                    if isinstance(example, dict):
                        lines.append(
                            f"    - {example.get('symbol')}: {example.get('realized_pnl_krw')} KRW ({example.get('return_pct')}%)"
                        )
    else:
        lines.append("- No configured candidate rules produced replay output.")
    lines.extend(
        [
            "",
            "## 개선 사항",
            "",
            "- 최근 손실 패턴은 아래 Gates에 반영됩니다.",
            "",
            "## 검증",
            "",
            "- 생성 JSON에는 portfolio_risk가 별도 machine-readable 객체로 포함됩니다.",
            "",
            "## 월요일 전 우선순위",
            "",
        ]
    )
    monday_checks = portfolio_risk.get("monday_pre_market_checks")
    if isinstance(monday_checks, list) and monday_checks:
        for row in monday_checks[:8]:
            if isinstance(row, dict):
                lines.append(f"- {row.get('symbol')}: {row.get('reason')}")
    elif portfolio_risk.get("source_failed"):
        lines.append(f"- 계좌/포지션 소스 확인: {portfolio_risk.get('source_failed')}")
    else:
        lines.append("- 특이 미체결/포지션 점검 항목 없음")
    lines.extend(
        [
            "",
            "## Prompt Snippet",
            "",
            str(snippet.get("summary") or ""),
            "",
            "## Portfolio Risk Summary",
        ]
    )
    for item in snippet.get("broker_asset_warnings", []):
        lines.append(f"- {item}")
    for item in snippet.get("top_portfolio_risks", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Monday Pre-Market Checks",
        ]
    )
    for item in snippet.get("monday_pre_market_checks", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Candidate Rule Replay",
        ]
    )
    for item in snippet.get("candidate_rule_replay", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Gates",
        ]
    )
    for item in snippet.get("patterns_to_downsize_or_reject", []):
        lines.append(f"- {item}")
    for item in snippet.get("required_hard_gates", []):
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def _negative_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg < 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])))[:max_lessons]


def _positive_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg > 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])), reverse=True)[:max_lessons]


def _lesson_trade(trade: dict[str, object]) -> dict[str, object]:
    decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
    return {
        "symbol": trade.get("symbol"),
        "realized_pnl_krw": trade.get("realized_pnl_krw"),
        "return_pct": trade.get("return_pct"),
        "hold_hours": trade.get("hold_hours"),
        "target_profile": decision.get("target_profile"),
        "reason_code": decision.get("reason_code"),
        "decision_run_id": trade.get("decision_run_id"),
    }


def _exec_agent_decision(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    for source in (item.risk_context, item.quantity_policy):
        if isinstance(source, dict):
            for key in ("exec_agent_decision", "short_term_exec_agent_decision", "last_trigger_time_exec_agent_interaction"):
                value = source.get(key)
                if isinstance(value, dict):
                    return _json_safe(value)
    return {}


def _risk_sizing_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    payload: dict[str, object] = {}
    for source in (item.quantity_policy, item.risk_context):
        if not isinstance(source, dict):
            continue
        for key in (
            "requested_quantity",
            "approved_quantity",
            "stop_price",
            "effective_stop_price",
            "sizing_adjustment",
            "max_safe_notional_krw",
            "budget_krw",
        ):
            if key in source and key not in payload:
                payload[key] = source[key]
    return _json_safe(payload)


def _trigger_time_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    interaction = item.risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return {}
    decision = interaction.get("decision") if isinstance(interaction.get("decision"), dict) else {}
    request = interaction.get("request_payload") if isinstance(interaction.get("request_payload"), dict) else {}
    market = request.get("market_context") if isinstance(request.get("market_context"), dict) else {}
    return _json_safe(
        {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "risk_notes": decision.get("risk_notes"),
            "context_staleness_ok": decision.get("context_staleness_ok"),
            "market_quality_gate": market.get("live_execution_quality_gate"),
        }
    )


def _market_enrichment_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    payload: dict[str, object] = {}
    for key in (
        "liquidity_tier",
        "trading_value",
        "short_ratio",
        "investor_flow",
        "market_breadth",
        "data_quality_flags",
        "data_quality_warnings",
    ):
        if key in item.risk_context:
            payload[key] = item.risk_context[key]
    return _json_safe(payload)


def _candidate_rule_context(*, item: ExecutionPlanItem | None, buy_fill: ExecutionFill) -> dict[str, object]:
    if item is None:
        return {}
    payload: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "trigger_price": _decimal_text(Decimal(item.trigger_price)) if item.trigger_price is not None else None,
        "limit_price": _decimal_text(Decimal(item.limit_price)) if item.limit_price is not None else None,
        "buy_fill_price": _decimal_text(Decimal(buy_fill.price)),
        "intent": item.intent,
    }
    for source in (item.quantity_policy, item.risk_context):
        if not isinstance(source, dict):
            continue
        for key in (
            "trigger_price",
            "watch_trigger_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "confidence",
            "confidence_score",
            "confidence_score_10",
            "probability",
            "score",
            "atr_pct",
            "atr_pct_14",
            "atr14_pct",
            "atr_percent",
            "volatility_pct",
            "realized_volatility_pct",
            "intraday_range_pct",
            "daily_range_pct",
            "portfolio_correlation",
            "max_correlation",
            "correlation",
            "sector_correlation",
            "market_correlation",
            "fee_slippage_bps",
            "round_trip_cost_bps",
            "estimated_round_trip_cost_bps",
            "fee_bps",
            "slippage_bps",
        ):
            if key in source and key not in payload:
                payload[key] = source[key]
        for nested_key in ("exec_agent_decision", "short_term_exec_agent_decision", "market_data", "quote", "decider_output"):
            nested = source.get(nested_key)
            if isinstance(nested, dict):
                for key in (
                    "confidence",
                    "confidence_score",
                    "confidence_score_10",
                    "atr_pct",
                    "atr_pct_14",
                    "atr14_pct",
                    "atr_percent",
                    "volatility_pct",
                    "portfolio_correlation",
                    "max_correlation",
                    "correlation",
                    "fee_slippage_bps",
                    "round_trip_cost_bps",
                    "estimated_round_trip_cost_bps",
                ):
                    if key in nested and key not in payload:
                        payload[key] = nested[key]
    return _json_safe({key: value for key, value in payload.items() if value is not None})


def _outcome_bucket(pnl: Decimal) -> str:
    if pnl < 0:
        return "loss"
    if pnl > 0:
        return "win"
    return "flat"


def _decimal_value(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _quantity_text(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP))


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime:
    return _normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _json_safe(value: object) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _normalize_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate exec-agent trade outcome feedback artifacts.")
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    settings = load_settings()
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        report = generate_trade_outcome_feedback(
            settings=settings,
            db=db,
            lookback_days=args.lookback_days,
            output_dir=args.output_dir,
        )
    print(json.dumps(_json_safe({"status": "ok", "closed_trade_count": report["closed_trade_count"], "output_dir": str(args.output_dir or settings.trade_outcome_feedback_dir)})))


if __name__ == "__main__":
    main()
