from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import Settings, load_settings
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.broker_order_state import (
    NONTERMINAL_ORDER_STATUSES,
)


class WeeklyTradeReviewTimeoutError(TimeoutError):
    """Raised when a bounded weekly export exceeds its configured deadline."""


WATCH_REVIEW_STATUSES = (
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "watch_buy_triggered",
    "watch_buy_triggered_blocked",
    "watch_buy_triggered_pending_dispatch",
    "watch_buy_submitted",
    "watch_buy_rejected",
    "watch_expired",
    "watch_cancelled",
    "short_term_watcher_pending",
    "short_term_watch_blocked",
    "new_buy_blocked",
)


def generate_weekly_trade_review(
    *,
    settings: Settings,
    db: Session,
    start_at: datetime,
    end_at: datetime,
    output_dir: Path | None = None,
    include_json_payloads: bool = False,
    max_range_days: int = 14,
    max_fills: int = 5000,
    max_weekly_orders: int = 1000,
    max_execution_failures: int = 1000,
    max_linked_events: int = 2000,
    max_stale_order_samples: int = 100,
    timeout_seconds: float | None = None,
    progress_callback: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Write a bounded, read-only weekly review artifact from exec-owned tables."""

    start_at = _normalize_utc(start_at)
    end_at = _normalize_utc(end_at)
    if end_at <= start_at:
        raise ValueError("end_at must be after start_at")
    range_days = (end_at - start_at).total_seconds() / 86400
    if range_days > max_range_days:
        raise ValueError(f"review range is {range_days:.2f} days; max_range_days is {max_range_days}")

    target_dir = output_dir or Path(settings.weekly_trade_review_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    started_monotonic = time.monotonic()

    def progress(step: str, **fields: object) -> None:
        elapsed = time.monotonic() - started_monotonic
        if timeout_seconds is not None and elapsed > timeout_seconds:
            raise WeeklyTradeReviewTimeoutError(
                f"weekly trade review export exceeded timeout_seconds={timeout_seconds:.3f} "
                f"after step={step!r} elapsed_seconds={elapsed:.3f}"
            )
        if progress_callback is not None:
            progress_callback(
                {
                    "step": step,
                    "elapsed_seconds": round(elapsed, 3),
                    **fields,
                }
            )

    fills, fills_truncated = _weekly_fills(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
        include_json_payloads=include_json_payloads,
        max_rows=max_fills,
    )
    progress("weekly_fills", row_count=len(fills), truncated=fills_truncated)
    broker_order_pks = sorted(
        {
            str(row["broker_order_pk"])
            for row in fills
            if row.get("broker_order_pk") is not None
        }
    )
    linked_orders = _linked_broker_orders(
        db=db,
        mode=settings.runtime_mode,
        broker_order_pks=broker_order_pks,
        include_json_payloads=include_json_payloads,
    )
    progress("linked_broker_orders", row_count=len(linked_orders))
    linked_events, linked_events_truncated = _linked_broker_order_events(
        db=db,
        broker_order_pks=broker_order_pks,
        include_json_payloads=include_json_payloads,
        max_rows=max_linked_events,
    )
    progress("linked_broker_order_events", row_count=len(linked_events), truncated=linked_events_truncated)
    weekly_order_counts = _weekly_order_status_counts(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
    )
    progress("weekly_order_status_counts", row_count=len(weekly_order_counts))
    weekly_orders, weekly_orders_truncated = _weekly_order_samples(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
        include_json_payloads=include_json_payloads,
        max_rows=max_weekly_orders,
    )
    progress("weekly_order_samples", row_count=len(weekly_orders), truncated=weekly_orders_truncated)
    failures, failures_truncated = _weekly_execution_failures(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
        include_json_payloads=include_json_payloads,
        max_rows=max_execution_failures,
    )
    progress("weekly_execution_failures", row_count=len(failures), truncated=failures_truncated)
    closed_trades, closed_trades_truncated = _weekly_closed_trades(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
        max_rows=max_fills,
    )
    progress("weekly_closed_trades", row_count=len(closed_trades), truncated=closed_trades_truncated)
    pnl_coverage = _weekly_pnl_coverage(closed_trades, truncated=closed_trades_truncated)
    realized_pnl_summary = _weekly_realized_pnl_summary(
        closed_trades,
        truncated=closed_trades_truncated,
    )
    stale_fifo_inventory_alerts, stale_fifo_inventory_alerts_truncated = _stale_fifo_inventory_alerts(
        db=db,
        mode=settings.runtime_mode,
        end_at=end_at,
        max_rows=max_fills,
    )
    progress(
        "stale_fifo_inventory_alerts",
        row_count=len(stale_fifo_inventory_alerts),
        truncated=stale_fifo_inventory_alerts_truncated,
    )
    active_order_summary = _active_broker_order_summary(
        db=db,
        mode=settings.runtime_mode,
        reference_at=end_at,
        stale_threshold_seconds=settings.stale_broker_order_reconciliation_threshold_seconds,
        max_stale_order_samples=max_stale_order_samples,
    )
    progress("active_broker_order_summary")
    plan_item_summary = _plan_item_review_summary(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
    )
    monitoring_checkpoints = _monitoring_checkpoints(
        db=db,
        mode=settings.runtime_mode,
        include_json_payloads=include_json_payloads,
    )
    protective_stop_actionability = _monitoring_protective_stop_actionability(
        db=db,
        mode=settings.runtime_mode,
    )
    progress("monitoring_sections", checkpoint_count=len(monitoring_checkpoints))

    report: dict[str, object] = {
        "generated_at": datetime.now(UTC),
        "runtime_mode": settings.runtime_mode,
        "range": {
            "start_at": start_at,
            "end_at": end_at,
            "end_exclusive": True,
        },
        "bounds": {
            "max_range_days": max_range_days,
            "max_fills": max_fills,
            "max_weekly_orders": max_weekly_orders,
            "max_execution_failures": max_execution_failures,
            "max_linked_events": max_linked_events,
            "max_stale_order_samples": max_stale_order_samples,
            "include_json_payloads": include_json_payloads,
        },
        "query_shape": {
            "fills": "execution_fills.filled_at range joined to broker_orders.mode",
            "linked_orders": "broker_orders.id IN weekly fill broker_order_pk",
            "linked_events": "broker_order_events.broker_order_pk IN weekly fill broker_order_pk",
            "weekly_orders": "broker_orders.mode/submitted_at range",
            "stale_orders": "broker_orders.mode/status/submitted_at",
            "plan_items": "execution_plan_items status/created_at/updated_at aggregates",
            "deferred_json_payloads": "omitted unless include_json_payloads=true",
            "broker_mutations": False,
        },
        "counts": {
            "fill_count": len(fills),
            "linked_broker_order_count": len(linked_orders),
            "linked_broker_order_event_count": len(linked_events),
            "weekly_order_sample_count": len(weekly_orders),
            "execution_failure_count": len(failures),
            "monitoring_checkpoint_count": len(monitoring_checkpoints),
            "protective_stop_actionability_warning_count": protective_stop_actionability["warning_count"],
            "closed_trade_count": len(closed_trades),
            "closed_sell_fill_count": realized_pnl_summary["sell_fill_count"],
            "total_sell_rows": pnl_coverage["total_sell_rows"],
            "pnl_covered_sell_rows": pnl_coverage["covered_sell_rows"],
            "pnl_uncovered_sell_rows": pnl_coverage["uncovered_sell_rows"],
            "stale_fifo_inventory_alert_count": len(stale_fifo_inventory_alerts),
        },
        "truncated": {
            "fills": fills_truncated,
            "linked_broker_order_events": linked_events_truncated,
            "weekly_orders": weekly_orders_truncated,
            "execution_failures": failures_truncated,
            "closed_trades": closed_trades_truncated,
            "stale_fifo_inventory_alerts": stale_fifo_inventory_alerts_truncated,
        },
        "fills": fills,
        "linked_broker_orders": linked_orders,
        "linked_broker_order_events": linked_events,
        "weekly_order_status_counts": weekly_order_counts,
        "weekly_order_samples": weekly_orders,
        "execution_failures": failures,
        "closed_trades": closed_trades,
        "realized_pnl_coverage": pnl_coverage,
        "realized_pnl_summary": realized_pnl_summary,
        "stale_fifo_inventory_alerts": stale_fifo_inventory_alerts,
        "active_broker_order_summary": active_order_summary,
        "execution_plan_item_summary": plan_item_summary,
        "monitoring_checkpoints": monitoring_checkpoints,
        "protective_stop_actionability": protective_stop_actionability,
    }

    artifact_path = target_dir / _artifact_filename(start_at=start_at, end_at=end_at)
    report["artifact_path"] = str(artifact_path)
    payload = _json_safe(report)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    artifact_path.write_text(text + "\n", encoding="utf-8")
    (target_dir / "latest.json").write_text(text + "\n", encoding="utf-8")
    progress("write_artifact", artifact_path=str(artifact_path))
    return payload


def _weekly_fills(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
    include_json_payloads: bool,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    columns = [
        ExecutionFill.id,
        ExecutionFill.broker_order_pk,
        ExecutionFill.broker_fill_id,
        ExecutionFill.symbol,
        ExecutionFill.side,
        ExecutionFill.quantity,
        ExecutionFill.base_quantity,
        ExecutionFill.quote_notional,
        ExecutionFill.price,
        ExecutionFill.filled_at,
        BrokerOrder.broker_name,
        BrokerOrder.mode,
        BrokerOrder.broker_order_id,
        BrokerOrder.status.label("broker_order_status"),
        BrokerOrder.submitted_at.label("broker_order_submitted_at"),
        BrokerOrder.idempotency_key,
    ]
    if include_json_payloads:
        columns.append(ExecutionFill.payload)
    rows = db.execute(
        select(*columns)
        .join(BrokerOrder, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == mode,
            ExecutionFill.filled_at >= start_at,
            ExecutionFill.filled_at < end_at,
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        .limit(max_rows + 1)
    ).all()
    return _bounded_rows(rows, max_rows)


def _linked_broker_orders(
    *,
    db: Session,
    mode: str,
    broker_order_pks: Sequence[str],
    include_json_payloads: bool,
) -> list[dict[str, object]]:
    if not broker_order_pks:
        return []
    columns = [
        BrokerOrder.id,
        BrokerOrder.execution_plan_item_id,
        BrokerOrder.broker_name,
        BrokerOrder.mode,
        BrokerOrder.idempotency_key,
        BrokerOrder.broker_order_id,
        BrokerOrder.symbol,
        BrokerOrder.side,
        BrokerOrder.status,
        BrokerOrder.quantity,
        BrokerOrder.base_quantity,
        BrokerOrder.quote_notional,
        BrokerOrder.limit_price,
        BrokerOrder.submitted_at,
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.last_error_classification,
        BrokerOrder.last_error_code,
        BrokerOrder.last_error_retryable,
        BrokerOrder.created_at,
        BrokerOrder.updated_at,
    ]
    if include_json_payloads:
        columns.extend(
            [
                BrokerOrder.request_metadata,
                BrokerOrder.response_metadata,
                BrokerOrder.last_error_message,
                BrokerOrder.raw_broker_payload,
            ]
        )
    rows = db.execute(
        select(*columns)
        .where(BrokerOrder.mode == mode, BrokerOrder.id.in_(broker_order_pks))
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.id.asc())
    ).all()
    return _rows_to_dicts(rows)


def _linked_broker_order_events(
    *,
    db: Session,
    broker_order_pks: Sequence[str],
    include_json_payloads: bool,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    if not broker_order_pks:
        return [], False
    columns = [
        BrokerOrderEvent.id,
        BrokerOrderEvent.broker_order_pk,
        BrokerOrderEvent.event_type,
        BrokerOrderEvent.event_source,
        BrokerOrderEvent.normalized_status,
        BrokerOrderEvent.broker_status,
        BrokerOrderEvent.error_classification,
        BrokerOrderEvent.retryable,
        BrokerOrderEvent.observed_at,
        BrokerOrderEvent.created_at,
    ]
    if include_json_payloads:
        columns.extend(
            [
                BrokerOrderEvent.message,
                BrokerOrderEvent.request_payload,
                BrokerOrderEvent.response_payload,
                BrokerOrderEvent.payload,
            ]
        )
    rows = db.execute(
        select(*columns)
        .where(BrokerOrderEvent.broker_order_pk.in_(broker_order_pks))
        .order_by(
            BrokerOrderEvent.broker_order_pk.asc(),
            BrokerOrderEvent.observed_at.asc(),
            BrokerOrderEvent.id.asc(),
        )
        .limit(max_rows + 1)
    ).all()
    return _bounded_rows(rows, max_rows)


def _weekly_order_status_counts(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, object]]:
    rows = db.execute(
        select(
            BrokerOrder.status,
            BrokerOrder.side,
            func.count(BrokerOrder.id).label("count"),
            func.min(BrokerOrder.submitted_at).label("first_submitted_at"),
            func.max(BrokerOrder.submitted_at).label("last_submitted_at"),
        )
        .where(
            BrokerOrder.mode == mode,
            BrokerOrder.submitted_at >= start_at,
            BrokerOrder.submitted_at < end_at,
        )
        .group_by(BrokerOrder.status, BrokerOrder.side)
        .order_by(BrokerOrder.status.asc(), BrokerOrder.side.asc())
    ).all()
    return _rows_to_dicts(rows)


def _weekly_order_samples(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
    include_json_payloads: bool,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    columns = [
        BrokerOrder.id,
        BrokerOrder.execution_plan_item_id,
        BrokerOrder.broker_name,
        BrokerOrder.idempotency_key,
        BrokerOrder.broker_order_id,
        BrokerOrder.symbol,
        BrokerOrder.side,
        BrokerOrder.status,
        BrokerOrder.quantity,
        BrokerOrder.base_quantity,
        BrokerOrder.quote_notional,
        BrokerOrder.limit_price,
        BrokerOrder.submitted_at,
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.last_error_classification,
        BrokerOrder.last_error_code,
        BrokerOrder.last_error_retryable,
    ]
    if include_json_payloads:
        columns.extend(
            [
                BrokerOrder.request_metadata,
                BrokerOrder.response_metadata,
                BrokerOrder.last_error_message,
                BrokerOrder.raw_broker_payload,
            ]
        )
    rows = db.execute(
        select(*columns)
        .where(
            BrokerOrder.mode == mode,
            BrokerOrder.submitted_at >= start_at,
            BrokerOrder.submitted_at < end_at,
        )
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.id.asc())
        .limit(max_rows + 1)
    ).all()
    return _bounded_rows(rows, max_rows)


def _weekly_execution_failures(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
    include_json_payloads: bool,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    columns = [
        ExecutionFailure.id,
        ExecutionFailure.broker_order_pk,
        ExecutionFailure.execution_plan_id,
        ExecutionFailure.execution_plan_item_id,
        ExecutionFailure.plan_id,
        ExecutionFailure.plan_source,
        ExecutionFailure.strategy_run_id,
        ExecutionFailure.decision_actor,
        ExecutionFailure.broker_name,
        ExecutionFailure.idempotency_key,
        ExecutionFailure.symbol,
        ExecutionFailure.side,
        ExecutionFailure.intent,
        ExecutionFailure.requested_quantity,
        ExecutionFailure.requested_limit_price,
        ExecutionFailure.requested_notional,
        ExecutionFailure.classification,
        ExecutionFailure.broker_error_classification,
        ExecutionFailure.broker_error_code,
        ExecutionFailure.retryable,
        ExecutionFailure.status,
        ExecutionFailure.first_failed_at,
        ExecutionFailure.latest_failed_at,
        ExecutionFailure.status_updated_at,
        ExecutionFailure.retry_decision_status,
        ExecutionFailure.retry_next_at,
        ExecutionFailure.retry_attempt_count,
    ]
    if include_json_payloads:
        columns.extend(
            [
                ExecutionFailure.broker_error_message,
                ExecutionFailure.retry_decision_reason,
                ExecutionFailure.retry_exhausted_reason,
                ExecutionFailure.raw_context,
            ]
        )
    rows = db.execute(
        select(*columns)
        .where(
            ExecutionFailure.mode == mode,
            ExecutionFailure.latest_failed_at >= start_at,
            ExecutionFailure.latest_failed_at < end_at,
        )
        .order_by(ExecutionFailure.latest_failed_at.asc(), ExecutionFailure.id.asc())
        .limit(max_rows + 1)
    ).all()
    return _bounded_rows(rows, max_rows)


def _active_broker_order_summary(
    *,
    db: Session,
    mode: str,
    reference_at: datetime,
    stale_threshold_seconds: int,
    max_stale_order_samples: int,
) -> dict[str, object]:
    stale_before = reference_at - timedelta(seconds=stale_threshold_seconds)
    nonterminal_counts = db.execute(
        select(
            BrokerOrder.status,
            func.count(BrokerOrder.id).label("count"),
            func.min(BrokerOrder.submitted_at).label("oldest_submitted_at"),
            func.max(BrokerOrder.submitted_at).label("newest_submitted_at"),
        )
        .where(
            BrokerOrder.mode == mode,
            BrokerOrder.status.in_(NONTERMINAL_ORDER_STATUSES),
        )
        .group_by(BrokerOrder.status)
        .order_by(BrokerOrder.status.asc())
    ).all()
    stale_rows = db.execute(
        select(
            BrokerOrder.id,
            BrokerOrder.broker_order_id,
            BrokerOrder.symbol,
            BrokerOrder.side,
            BrokerOrder.status,
            BrokerOrder.quantity,
            BrokerOrder.limit_price,
            BrokerOrder.submitted_at,
            BrokerOrder.last_broker_sync_at,
        )
        .where(
            BrokerOrder.mode == mode,
            BrokerOrder.status.in_(NONTERMINAL_ORDER_STATUSES),
            BrokerOrder.submitted_at.is_not(None),
            BrokerOrder.submitted_at < stale_before,
        )
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.id.asc())
        .limit(max_stale_order_samples + 1)
    ).all()
    stale_samples, stale_truncated = _bounded_rows(stale_rows, max_stale_order_samples)
    missing_submitted_at_count = db.scalar(
        select(func.count(BrokerOrder.id)).where(
            BrokerOrder.mode == mode,
            BrokerOrder.status.in_(NONTERMINAL_ORDER_STATUSES),
            BrokerOrder.submitted_at.is_(None),
        )
    )
    return {
        "stale_before": stale_before,
        "nonterminal_status_counts": _rows_to_dicts(nonterminal_counts),
        "stale_nonterminal_samples": stale_samples,
        "stale_nonterminal_samples_truncated": stale_truncated,
        "missing_submitted_at_nonterminal_count": int(missing_submitted_at_count or 0),
    }


def _plan_item_review_summary(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, object]:
    active_watch_counts = db.execute(
        select(
            ExecutionPlanItem.status,
            ExecutionPlanItem.intent,
            func.count(ExecutionPlanItem.id).label("count"),
            func.min(ExecutionPlanItem.updated_at).label("oldest_updated_at"),
            func.max(ExecutionPlanItem.updated_at).label("newest_updated_at"),
        )
        .join(ExecutionPlan, ExecutionPlanItem.execution_plan_id == ExecutionPlan.id)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItem.status.in_(WATCH_REVIEW_STATUSES),
        )
        .group_by(ExecutionPlanItem.status, ExecutionPlanItem.intent)
        .order_by(ExecutionPlanItem.status.asc(), ExecutionPlanItem.intent.asc())
    ).all()
    created_counts = db.execute(
        select(
            ExecutionPlanItem.status,
            ExecutionPlanItem.intent,
            func.count(ExecutionPlanItem.id).label("count"),
        )
        .join(ExecutionPlan, ExecutionPlanItem.execution_plan_id == ExecutionPlan.id)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItem.created_at >= start_at,
            ExecutionPlanItem.created_at < end_at,
        )
        .group_by(ExecutionPlanItem.status, ExecutionPlanItem.intent)
        .order_by(ExecutionPlanItem.status.asc(), ExecutionPlanItem.intent.asc())
    ).all()
    updated_counts = db.execute(
        select(
            ExecutionPlanItem.status,
            ExecutionPlanItem.intent,
            func.count(ExecutionPlanItem.id).label("count"),
        )
        .join(ExecutionPlan, ExecutionPlanItem.execution_plan_id == ExecutionPlan.id)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItem.updated_at >= start_at,
            ExecutionPlanItem.updated_at < end_at,
        )
        .group_by(ExecutionPlanItem.status, ExecutionPlanItem.intent)
        .order_by(ExecutionPlanItem.status.asc(), ExecutionPlanItem.intent.asc())
    ).all()
    return {
        "active_watch_status_counts": _rows_to_dicts(active_watch_counts),
        "weekly_created_status_counts": _rows_to_dicts(created_counts),
        "weekly_updated_status_counts": _rows_to_dicts(updated_counts),
    }


def _monitoring_checkpoints(
    *,
    db: Session,
    mode: str,
    include_json_payloads: bool,
) -> list[dict[str, object]]:
    columns = [
        MonitoringCheckpoint.id,
        MonitoringCheckpoint.worker_name,
        MonitoringCheckpoint.mode,
        MonitoringCheckpoint.status,
        MonitoringCheckpoint.last_heartbeat_at,
        MonitoringCheckpoint.last_success_at,
        MonitoringCheckpoint.created_at,
        MonitoringCheckpoint.updated_at,
    ]
    if include_json_payloads:
        columns.append(MonitoringCheckpoint.details)
    rows = db.execute(
        select(*columns)
        .where(MonitoringCheckpoint.mode == mode)
        .order_by(MonitoringCheckpoint.worker_name.asc())
    ).all()
    return _rows_to_dicts(rows)


def _weekly_closed_trades(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    newest_fills = db.scalars(
        select(ExecutionFill)
        .join(BrokerOrder, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == mode,
            ExecutionFill.filled_at < end_at,
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
        .limit(max_rows + 1)
    ).all()
    truncated = len(newest_fills) > max_rows
    fills = list(reversed(newest_fills[:max_rows]))
    sell_symbols = {
        fill.symbol
        for fill in fills
        if str(fill.side or "").lower() == "sell" and start_at <= _normalize_utc(fill.filled_at) < end_at
    }
    positions_by_symbol = _positions_by_symbol(db=db, mode=mode, symbols=sell_symbols)

    lots_by_symbol: dict[str, list[dict[str, object]]] = {}
    closed: list[dict[str, object]] = []
    for fill in fills:
        side = str(fill.side or "").lower()
        quantity = _fill_quantity(fill)
        if side == "buy":
            lots_by_symbol.setdefault(fill.symbol, []).append(
                {
                    "quantity": quantity,
                    "price": Decimal(fill.price),
                    "filled_at": _normalize_utc(fill.filled_at),
                    "fill_id": fill.id,
                }
            )
            continue
        if side != "sell":
            continue
        filled_at = _normalize_utc(fill.filled_at)
        in_period = start_at <= filled_at < end_at
        next_buy_after_sell = _next_buy_after(fills=fills, symbol=fill.symbol, after=filled_at)
        remaining = quantity
        lots = lots_by_symbol.setdefault(fill.symbol, [])
        while remaining > 0 and lots:
            lot = lots[0]
            lot_quantity = Decimal(str(lot["quantity"]))
            matched = min(remaining, lot_quantity)
            if in_period:
                buy_price = Decimal(str(lot["price"]))
                cost_basis_source = "fill_fifo"
                position = positions_by_symbol.get(fill.symbol)
                broker_cost_basis = _broker_reconciliation_cost_basis(
                    position=position,
                    sold_at=filled_at,
                    next_buy_after_sell=next_buy_after_sell,
                )
                if _normalize_utc(lot["filled_at"]) < start_at and broker_cost_basis is not None:
                    buy_price = broker_cost_basis
                    cost_basis_source = "broker_reconciliation"
                    cost_basis_observed_at = (
                        position.last_reconciled_at if position is not None else None
                    )
                else:
                    cost_basis_observed_at = lot["filled_at"]
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        quantity=matched,
                        buy_price=buy_price,
                        cost_basis_source=cost_basis_source,
                        cost_basis_observed_at=cost_basis_observed_at,
                    )
                )
            lot["quantity"] = lot_quantity - matched
            remaining -= matched
            if Decimal(str(lot["quantity"])) <= 0:
                lots.pop(0)
        if remaining > 0 and in_period:
            position = positions_by_symbol.get(fill.symbol)
            broker_cost_basis = _broker_reconciliation_cost_basis(
                position=position,
                sold_at=filled_at,
                next_buy_after_sell=next_buy_after_sell,
            )
            if broker_cost_basis is not None:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        quantity=remaining,
                        buy_price=broker_cost_basis,
                        cost_basis_source="broker_reconciliation",
                        cost_basis_observed_at=position.last_reconciled_at if position is not None else None,
                    )
                )
            else:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        quantity=remaining,
                        buy_price=None,
                        cost_basis_source="unknown",
                        cost_basis_observed_at=None,
                    )
                )
    return closed, truncated


def _broker_reconciliation_cost_basis(
    *,
    position: ExecutionPosition | None,
    sold_at: datetime,
    next_buy_after_sell: datetime | None,
) -> Decimal | None:
    if (
        position is not None
        and position.average_price is not None
        and position.average_price > 0
        and position.last_reconciled_at is not None
        and "reconciliation" in str(position.updated_from or "")
        and _position_quantity(position) <= 0
    ):
        reconciled_at = _normalize_utc(position.last_reconciled_at)
        sold_at = _normalize_utc(sold_at)
        if reconciled_at >= sold_at and (next_buy_after_sell is None or reconciled_at < next_buy_after_sell):
            return Decimal(position.average_price)
    return None


def _weekly_pnl_coverage(
    closed_trades: Sequence[dict[str, object]],
    *,
    truncated: bool,
) -> dict[str, object]:
    rows_by_fill_id: dict[str, list[dict[str, object]]] = {}
    for row in closed_trades:
        rows_by_fill_id.setdefault(str(row["sell_fill_id"]), []).append(row)
    uncovered_rows = [row for row in closed_trades if row.get("cost_basis_source") == "unknown"]
    uncovered_fill_ids = {
        fill_id
        for fill_id, rows in rows_by_fill_id.items()
        if any(row.get("cost_basis_source") == "unknown" for row in rows)
    }
    covered_fill_ids = set(rows_by_fill_id) - uncovered_fill_ids
    uncovered_proceeds = sum(
        (
            Decimal(str(row["sell_price"])) * Decimal(str(row["quantity"]))
            for row in uncovered_rows
        ),
        Decimal("0"),
    ).quantize(Decimal("0.0001"))
    return {
        "status": "truncated" if truncated else ("complete" if not uncovered_fill_ids else "incomplete"),
        "truncated": truncated,
        "total_sell_rows": len(rows_by_fill_id),
        "covered_sell_rows": len(covered_fill_ids),
        "uncovered_sell_rows": len(uncovered_fill_ids),
        "uncovered_gross_proceeds": uncovered_proceeds,
        "symbols_requiring_repair": sorted({str(row["symbol"]) for row in uncovered_rows}),
        "uncovered_sell_fill_ids": sorted(uncovered_fill_ids),
    }


def _weekly_realized_pnl_summary(
    closed_trades: Sequence[dict[str, object]],
    *,
    truncated: bool,
) -> dict[str, object]:
    """Aggregate FIFO lot rows into one outcome per sell fill."""

    rows_by_fill_id: dict[str, list[dict[str, object]]] = {}
    for row in closed_trades:
        rows_by_fill_id.setdefault(str(row["sell_fill_id"]), []).append(row)

    outcomes: list[dict[str, object]] = []
    for fill_id, rows in rows_by_fill_id.items():
        if any(
            row.get("cost_basis_price") is None or row.get("realized_pnl") is None
            for row in rows
        ):
            continue
        cost_basis = sum(
            (
                Decimal(str(row["cost_basis_price"])) * Decimal(str(row["quantity"]))
                for row in rows
            ),
            Decimal("0"),
        )
        realized_pnl = sum(
            (Decimal(str(row["realized_pnl"])) for row in rows),
            Decimal("0"),
        ).quantize(Decimal("0.0001"))
        return_pct = (
            (realized_pnl / cost_basis * Decimal("100")).quantize(Decimal("0.0001"))
            if cost_basis > 0
            else None
        )
        outcomes.append(
            {
                "sell_fill_id": fill_id,
                "sell_broker_order_pk": rows[0]["sell_broker_order_pk"],
                "symbol": rows[0]["symbol"],
                "sell_filled_at": rows[0]["sell_filled_at"],
                "quantity": _quantity_value(
                    sum((Decimal(str(row["quantity"])) for row in rows), Decimal("0"))
                ),
                "cost_basis": cost_basis.quantize(Decimal("0.0001")),
                "realized_pnl": realized_pnl,
                "return_pct": return_pct,
            }
        )

    outcomes.sort(key=lambda row: (Decimal(str(row["realized_pnl"])), str(row["sell_fill_id"])))
    returns = sorted(
        Decimal(str(row["return_pct"]))
        for row in outcomes
        if row["return_pct"] is not None
    )
    average_return_pct = (
        (sum(returns, Decimal("0")) / Decimal(len(returns))).quantize(Decimal("0.0001"))
        if returns
        else None
    )
    median_return_pct: Decimal | None = None
    if returns:
        midpoint = len(returns) // 2
        if len(returns) % 2:
            median_return_pct = returns[midpoint]
        else:
            median_return_pct = (
                (returns[midpoint - 1] + returns[midpoint]) / Decimal("2")
            ).quantize(Decimal("0.0001"))

    coverage_complete = len(outcomes) == len(rows_by_fill_id)
    status = "truncated" if truncated else ("complete" if coverage_complete else "incomplete")
    covered_realized_pnl = sum(
        (Decimal(str(row["realized_pnl"])) for row in outcomes),
        Decimal("0"),
    ).quantize(Decimal("0.0001"))
    return {
        "status": status,
        "sell_fill_count": len(rows_by_fill_id),
        "covered_sell_fill_count": len(outcomes),
        "win_count": sum(1 for row in outcomes if Decimal(str(row["realized_pnl"])) > 0),
        "loss_count": sum(1 for row in outcomes if Decimal(str(row["realized_pnl"])) < 0),
        "flat_count": sum(1 for row in outcomes if Decimal(str(row["realized_pnl"])) == 0),
        "realized_pnl": covered_realized_pnl if status == "complete" else None,
        "covered_realized_pnl": covered_realized_pnl,
        "average_return_pct": average_return_pct,
        "median_return_pct": median_return_pct,
        "best_trade": outcomes[-1] if outcomes else None,
        "worst_trade": outcomes[0] if outcomes else None,
    }


def _next_buy_after(
    *,
    fills: Sequence[ExecutionFill],
    symbol: str,
    after: datetime,
) -> datetime | None:
    later_buys = [
        _normalize_utc(fill.filled_at)
        for fill in fills
        if fill.symbol == symbol
        and str(fill.side or "").lower() == "buy"
        and _normalize_utc(fill.filled_at) > after
    ]
    return min(later_buys) if later_buys else None


def _closed_trade_row(
    *,
    fill: ExecutionFill,
    quantity: Decimal,
    buy_price: Decimal | None,
    cost_basis_source: str,
    cost_basis_observed_at: object,
) -> dict[str, object]:
    sell_price = Decimal(fill.price)
    pnl = ((sell_price - buy_price) * quantity).quantize(Decimal("0.0001")) if buy_price is not None else None
    return {
        "symbol": fill.symbol,
        "sell_fill_id": fill.id,
        "sell_broker_order_pk": fill.broker_order_pk,
        "sell_filled_at": _normalize_utc(fill.filled_at),
        "quantity": _quantity_value(quantity),
        "sell_price": sell_price,
        "cost_basis_price": buy_price,
        "cost_basis_source": cost_basis_source,
        "cost_basis_observed_at": cost_basis_observed_at,
        "realized_pnl": pnl,
    }


def _stale_fifo_inventory_alerts(
    *,
    db: Session,
    mode: str,
    end_at: datetime,
    max_rows: int,
) -> tuple[list[dict[str, object]], bool]:
    newest_fills = db.scalars(
        select(ExecutionFill)
        .join(BrokerOrder, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == mode,
            ExecutionFill.filled_at < end_at,
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
        .limit(max_rows + 1)
    ).all()
    truncated = len(newest_fills) > max_rows
    fills = list(reversed(newest_fills[:max_rows]))
    quantities: dict[str, Decimal] = {}
    first_fill_at: dict[str, datetime] = {}
    last_fill_at: dict[str, datetime] = {}
    for fill in fills:
        observed_at = _normalize_utc(fill.filled_at)
        first_fill_at[fill.symbol] = min(first_fill_at.get(fill.symbol, observed_at), observed_at)
        last_fill_at[fill.symbol] = max(last_fill_at.get(fill.symbol, observed_at), observed_at)
        quantity = _fill_quantity(fill)
        if str(fill.side or "").lower() == "buy":
            quantities[fill.symbol] = quantities.get(fill.symbol, Decimal("0")) + quantity
        elif str(fill.side or "").lower() == "sell":
            quantities[fill.symbol] = quantities.get(fill.symbol, Decimal("0")) - quantity
    candidate_symbols = {symbol for symbol, quantity in quantities.items() if quantity > 0}
    positions_by_symbol = _positions_by_symbol(db=db, mode=mode, symbols=candidate_symbols)
    alerts: list[dict[str, object]] = []
    for symbol in sorted(candidate_symbols):
        position = positions_by_symbol.get(symbol)
        if position is None or position.last_reconciled_at is None:
            continue
        broker_quantity = _position_quantity(position)
        if broker_quantity > 0:
            continue
        alerts.append(
            {
                "kind": "stale_fill_fifo_inventory_flat_broker_position",
                "symbol": symbol,
                "fifo_open_quantity": _quantity_value(quantities[symbol]),
                "broker_position_quantity": _quantity_value(broker_quantity),
                "position_last_reconciled_at": position.last_reconciled_at,
                "fifo_scan_first_fill_at": first_fill_at.get(symbol),
                "fifo_scan_last_fill_at": last_fill_at.get(symbol),
                "repair_action": "reconcile broker execution history before retiring local FIFO lots",
                "message": "fill FIFO leaves open quantity but latest broker reconciliation reports flat",
            }
        )
    return alerts, truncated


def _positions_by_symbol(
    *,
    db: Session,
    mode: str,
    symbols: set[str],
) -> dict[str, ExecutionPosition]:
    if not symbols:
        return {}
    return {
        position.symbol: position
        for position in db.scalars(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == mode,
                ExecutionPosition.symbol.in_(sorted(symbols)),
            )
        ).all()
    }


def _fill_quantity(fill: ExecutionFill) -> Decimal:
    if fill.base_quantity is not None and fill.base_quantity > 0:
        return Decimal(fill.base_quantity)
    return Decimal(fill.quantity)


def _position_quantity(position: ExecutionPosition) -> Decimal:
    if position.base_quantity is not None:
        return Decimal(position.base_quantity)
    return Decimal(position.quantity)


def _quantity_value(value: Decimal) -> int | str:
    if value == value.to_integral_value():
        return int(value)
    return str(value.normalize())


def _monitoring_protective_stop_actionability(
    *,
    db: Session,
    mode: str,
) -> dict[str, object]:
    rows = db.execute(
        select(
            MonitoringCheckpoint.worker_name,
            MonitoringCheckpoint.status,
            MonitoringCheckpoint.last_heartbeat_at,
            MonitoringCheckpoint.details,
        )
        .where(MonitoringCheckpoint.mode == mode)
        .order_by(MonitoringCheckpoint.worker_name.asc())
    ).all()
    warnings: list[dict[str, object]] = []
    for row in rows:
        details = row.details if isinstance(row.details, dict) else {}
        actionability = details.get("protective_stop_actionability")
        if not isinstance(actionability, dict):
            continue
        checkpoint_warnings = actionability.get("warnings")
        if not isinstance(checkpoint_warnings, list):
            continue
        for warning in checkpoint_warnings:
            if not isinstance(warning, dict):
                continue
            warnings.append(
                {
                    "worker_name": row.worker_name,
                    "checkpoint_status": row.status,
                    "last_heartbeat_at": row.last_heartbeat_at,
                    "symbol": warning.get("symbol"),
                    "execution_plan_item_id": warning.get("execution_plan_item_id"),
                    "status": warning.get("status"),
                    "observed_price": warning.get("observed_price"),
                    "stop_trigger": warning.get("stop_trigger"),
                    "outcome": warning.get("outcome"),
                    "reason": warning.get("reason"),
                    "message": warning.get("message"),
                    "block_reason": warning.get("block_reason"),
                }
            )
    return {
        "status": "degraded" if warnings else "ok",
        "warning_count": len(warnings),
        "affected_symbols": sorted(
            {
                str(warning["symbol"])
                for warning in warnings
                if warning.get("symbol") is not None
            }
        ),
        "warnings": warnings,
    }


def _bounded_rows(rows: Sequence[object], max_rows: int) -> tuple[list[dict[str, object]], bool]:
    truncated = len(rows) > max_rows
    return _rows_to_dicts(rows[:max_rows]), truncated


def _rows_to_dicts(rows: Iterable[object]) -> list[dict[str, object]]:
    return [dict(row._mapping) for row in rows]  # type: ignore[attr-defined]


def _artifact_filename(*, start_at: datetime, end_at: datetime) -> str:
    return (
        f"{start_at.date().isoformat()}_to_"
        f"{end_at.date().isoformat()}_weekly_trade_review.json"
    )


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return _normalize_utc(parsed)


def _json_safe(value: object) -> Any:
    if isinstance(value, Decimal):
        return str(value)
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
    parser = argparse.ArgumentParser(
        description=(
            "Export bounded weekly trade-review evidence from the local exec "
            "runtime database. This command is read-only and does not call broker APIs."
        ),
    )
    parser.add_argument(
        "--start-at",
        required=True,
        help=(
            "Inclusive start timestamp or date. Naive values are interpreted as UTC; "
            "use an explicit offset such as +09:00 for KST trading-date bounds."
        ),
    )
    parser.add_argument(
        "--end-at",
        required=True,
        help=(
            "Exclusive end timestamp or date. Naive values are interpreted as UTC; "
            "use an explicit offset such as +09:00 for KST trading-date bounds."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--runtime-mode", choices=("mock", "paper", "live"), default=None)
    parser.add_argument(
        "--require-runtime-mode",
        choices=("mock", "paper", "live"),
        default=None,
        help=(
            "Fail before connecting to the database unless loaded settings resolve "
            "to this runtime mode. Use this for live cron exports."
        ),
    )
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--include-json-payloads", action="store_true")
    parser.add_argument("--max-range-days", type=int, default=14)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Fail with a specific timeout once query-step progress exceeds this deadline.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Write JSON progress events to stderr as each bounded query step completes.",
    )
    args = parser.parse_args()

    settings_kwargs: dict[str, object] = {}
    if args.database_url:
        settings_kwargs["database_url"] = args.database_url
    if args.runtime_mode:
        settings_kwargs["runtime_mode"] = args.runtime_mode
    settings = load_settings().model_copy(update=settings_kwargs)
    if args.require_runtime_mode and settings.runtime_mode != args.require_runtime_mode:
        parser.error(
            "resolved runtime_mode="
            f"{settings.runtime_mode!r} does not match --require-runtime-mode "
            f"{args.require_runtime_mode!r}; source the service .env or pass "
            "explicit --runtime-mode and --database-url values"
        )
    explicit_database_url = bool(args.database_url) or bool(
        os.environ.get("KAI_EXEC_DATABASE_URL", "").strip()
    )
    if args.require_runtime_mode == "live" and settings.runtime_mode == "live" and not explicit_database_url:
        parser.error(
            "live weekly export requires an explicit database URL via "
            "--database-url or KAI_EXEC_DATABASE_URL when --require-runtime-mode "
            "is 'live'; source the service .env or pass --database-url"
        )

    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            try:
                report = generate_weekly_trade_review(
                    settings=settings,
                    db=db,
                    start_at=_parse_datetime(args.start_at),
                    end_at=_parse_datetime(args.end_at),
                    output_dir=args.output_dir,
                    include_json_payloads=args.include_json_payloads,
                    max_range_days=args.max_range_days,
                    timeout_seconds=args.timeout_seconds,
                    progress_callback=_stderr_progress if args.progress else None,
                )
            except WeeklyTradeReviewTimeoutError as exc:
                print(
                    json.dumps(
                        {
                            "status": "timeout",
                            "error": str(exc),
                            "timeout_seconds": args.timeout_seconds,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                )
                raise SystemExit(124) from exc
        print(
            json.dumps(
                {
                    "status": "ok",
                    "artifact_path": report["artifact_path"],
                    "fill_count": report["counts"]["fill_count"],  # type: ignore[index]
                    "execution_failure_count": report["counts"]["execution_failure_count"],  # type: ignore[index]
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    finally:
        engine.dispose()


def _stderr_progress(event: dict[str, object]) -> None:
    print(json.dumps({"status": "progress", **event}, ensure_ascii=False, sort_keys=True), file=sys.stderr)


if __name__ == "__main__":
    main()
