from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, selectinload, undefer

from app.core.config import Settings, load_settings
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.broker_order_state import (
    ACTIONABLE_OPEN_ORDER_STATUSES,
    MANUAL_REVIEW_ORDER_STATUSES,
    NONTERMINAL_ORDER_STATUSES,
    active_local_broker_order_filters,
)
from app.services.artifact_integrity import (
    artifact_integrity_is_valid,
    attach_artifact_integrity,
    load_strict_json_object,
)
from app.services.database_cli_security import reject_secret_database_url_argument
from app.services.execution_account_route import redact_account_references
from app.services.fifo_inventory import (
    FifoRouteScope,
    ambiguous_fifo_chronology_buckets,
    fifo_chronology_is_ambiguous_at,
    fifo_fill_sort_key,
    fifo_route_scope_from_order,
    legacy_kis_krx_event_is_isolated,
    load_fifo_isolation_cursor,
)
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.fill_economics import closed_trade_economics
from app.services.runtime_transitions import WATCH_SIZING_PENDING_STATUS
from app.services.max_holding_actionability import max_holding_actionability_summary
from app.services.late_entry_actionability import late_entry_actionability_summary
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
)


class WeeklyTradeReviewTimeoutError(TimeoutError):
    """Raised when a bounded weekly export exceeds its configured deadline."""


WEEKLY_TRADE_REVIEW_SCHEMA_VERSION = "weekly-trade-review-v2"
WEEKLY_LEARNING_EVIDENCE_SCHEMA_VERSION = "weekly-learning-evidence-v1"


ACTIVE_WATCH_REVIEW_STATUSES = (
    WATCH_SIZING_PENDING_STATUS,
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "watch_buy_triggered",
    "watch_buy_triggered_blocked",
    "watch_buy_triggered_pending_dispatch",
    "short_term_watcher_pending",
    "new_buy_blocked",
)


def load_weekly_trade_review_artifact(path: Path) -> dict[str, object]:
    """Load a weekly artifact only when its schema and checksum are valid."""

    try:
        payload = load_strict_json_object(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("weekly trade review artifact is unreadable") from exc
    if payload.get("schema_version") != WEEKLY_TRADE_REVIEW_SCHEMA_VERSION:
        raise ValueError("weekly trade review artifact schema is unsupported")
    if not artifact_integrity_is_valid(payload):
        raise ValueError("weekly trade review artifact integrity check failed")
    return payload


ACTIVE_NEXT_OPEN_JOB_STATUSES = frozenset(
    {"queued", "scheduled", "running", "retrying"}
)
ACTIVE_NEXT_OPEN_PLAN_ITEM_STATUSES = frozenset(
    {
        "new_buy_blocked",
        "next_open_buy_scheduled",
        "next_open_buy_retry_pending",
        "next_open_buy_dispatching",
    }
)
TERMINAL_AUDIT_PLAN_ITEM_STATUSES = frozenset(
    {
        "broker_order_rejected",
        "exec_agent_rejected",
        "expired",
        "next_open_buy_expired",
        "next_open_buy_failed",
        "rejected",
        "short_term_watch_blocked",
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_cancelled",
        "watch_expired",
    }
)


def _weekly_exact_krx_fifo_scope(settings: Settings) -> FifoRouteScope | None:
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return None
    return FifoRouteScope(
        provider=identity.provider,
        account_scope=identity.account_scope,
        venue="KRX",
        asset_class="stock",
        exchange=identity.exchange,
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
    generated_at = datetime.now(UTC)

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
    fill_attribution_coverage = _weekly_fill_attribution_coverage(
        fills,
        truncated=fills_truncated,
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
        settings=settings,
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
        settings=settings,
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
        settings=settings,
        reference_at=generated_at,
        max_stale_order_samples=max_stale_order_samples,
    )
    progress("active_broker_order_summary")
    plan_item_summary = _plan_item_review_summary(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
    )
    monitoring_checkpoints, monitoring_checkpoint_summary = _monitoring_checkpoints(
        db=db,
        mode=settings.runtime_mode,
        active_since=start_at,
        include_json_payloads=include_json_payloads,
    )
    protective_stop_actionability = _monitoring_protective_stop_actionability(
        db=db,
        mode=settings.runtime_mode,
        active_since=start_at,
    )
    max_holding_actionability = max_holding_actionability_summary(
        settings=settings,
        db=db,
        mode=settings.runtime_mode,
        reference_at=end_at,
    )
    late_entry_actionability = late_entry_actionability_summary(
        db=db,
        mode=settings.runtime_mode,
        start_at=start_at,
        end_at=end_at,
    )
    progress("monitoring_sections", checkpoint_count=len(monitoring_checkpoints))

    learning_evidence = _weekly_learning_evidence(
        fill_attribution_coverage=fill_attribution_coverage,
        realized_pnl_coverage=pnl_coverage,
        realized_pnl_summary=realized_pnl_summary,
        closed_trades=closed_trades,
        weekly_order_status_counts=weekly_order_counts,
        execution_failures=failures,
        truncated={
            "fills": fills_truncated,
            "weekly_orders": weekly_orders_truncated,
            "execution_failures": failures_truncated,
            "closed_trades": closed_trades_truncated,
        },
    )

    report: dict[str, object] = {
        "schema_version": WEEKLY_TRADE_REVIEW_SCHEMA_VERSION,
        "generated_at": generated_at,
        "runtime_mode": settings.runtime_mode,
        "provenance": {
            "producer": "kai-trading-exec",
            "artifact_type": "weekly_trade_review",
            "query_contract": WEEKLY_TRADE_REVIEW_SCHEMA_VERSION,
            "source_tables": [
                "execution_fills",
                "broker_orders",
                "broker_order_events",
                "execution_failures",
                "execution_plan_items",
                "execution_plans",
                "execution_positions",
                "monitoring_checkpoints",
            ],
            "runtime_mode": settings.runtime_mode,
            "generated_at": generated_at,
        },
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
            "fills": (
                "execution_fills.filled_at range joined to broker_orders.mode with "
                "optional local execution_plan_item/decision identity"
            ),
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
            "decision_linked_fill_count": fill_attribution_coverage[
                "decision_identity_linked_fill_rows"
            ],
            "decision_identity_missing_fill_count": fill_attribution_coverage[
                "decision_identity_missing_fill_rows"
            ],
            "decision_run_mismatch_fill_count": fill_attribution_coverage[
                "decision_run_mismatch_fill_rows"
            ],
            "linked_broker_order_count": len(linked_orders),
            "linked_broker_order_event_count": len(linked_events),
            "weekly_order_sample_count": len(weekly_orders),
            "execution_failure_count": len(failures),
            "monitoring_checkpoint_count": len(monitoring_checkpoints),
            "protective_stop_actionability_warning_count": protective_stop_actionability["warning_count"],
            "due_max_holding_item_count": max_holding_actionability["due_item_count"],
            "late_entry_shadow_count": late_entry_actionability["shadow_count"],
            "late_entry_block_count": late_entry_actionability["block_count"],
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
        "fill_attribution_coverage": fill_attribution_coverage,
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
        "monitoring_checkpoint_summary": monitoring_checkpoint_summary,
        "protective_stop_actionability": protective_stop_actionability,
        "max_holding_actionability": max_holding_actionability,
        "late_entry_actionability": late_entry_actionability,
        "learning_evidence": learning_evidence,
    }

    artifact_path = target_dir / _artifact_filename(start_at=start_at, end_at=end_at)
    report["artifact_path"] = str(artifact_path)
    payload = attach_artifact_integrity(_json_safe(report))
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    _atomic_write_text(artifact_path, text + "\n")
    _atomic_write_text(target_dir / "latest.json", text + "\n")
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
        BrokerOrder.execution_plan_item_id,
        ExecutionPlanItem.intent.label("execution_plan_item_intent"),
        ExecutionPlanItem.decision_agent_name,
        ExecutionPlanItem.decision_snapshot_id,
        ExecutionPlanItem.decision_run_id,
        ExecutionPlanItem.source_market_date,
        ExecutionPlan.market_date.label("target_market_date"),
        ExecutionPlan.source.label("execution_plan_source"),
        ExecutionPlan.strategy_run_id,
    ]
    if include_json_payloads:
        columns.append(ExecutionFill.payload)
    rows = db.execute(
        select(*columns)
        .join(BrokerOrder, ExecutionFill.broker_order_pk == BrokerOrder.id)
        .outerjoin(
            ExecutionPlanItem,
            BrokerOrder.execution_plan_item_id == ExecutionPlanItem.id,
        )
        .outerjoin(
            ExecutionPlan,
            ExecutionPlanItem.execution_plan_id == ExecutionPlan.id,
        )
        .where(
            BrokerOrder.mode == mode,
            ExecutionFill.filled_at >= start_at,
            ExecutionFill.filled_at < end_at,
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        .limit(max_rows + 1)
    ).all()
    return _bounded_rows(rows, max_rows)


def _weekly_fill_attribution_coverage(
    fills: Sequence[dict[str, object]],
    *,
    truncated: bool,
) -> dict[str, object]:
    unlinked_fill_ids = sorted(
        str(row["id"])
        for row in fills
        if row.get("execution_plan_item_id") is None
    )
    decision_run_mismatch_ids = sorted(
        str(row["id"])
        for row in fills
        if row.get("execution_plan_source") == "kai-trading-core"
        and (
            row.get("strategy_run_id") is None
            or row.get("decision_run_id") != row.get("strategy_run_id")
        )
    )
    missing_decision_ids = sorted(
        str(row["id"])
        for row in fills
        if row.get("decision_snapshot_id") is None
        or row.get("decision_agent_name") is None
        or row.get("decision_run_id") is None
        or str(row["id"]) in decision_run_mismatch_ids
    )
    if truncated:
        status = "truncated"
    elif not fills:
        status = "empty"
    elif not unlinked_fill_ids and not missing_decision_ids:
        status = "complete"
    else:
        status = "incomplete"
    return {
        "status": status,
        "truncated": truncated,
        "total_fill_rows": len(fills),
        "execution_plan_item_linked_fill_rows": len(fills) - len(unlinked_fill_ids),
        "execution_plan_item_unlinked_fill_rows": len(unlinked_fill_ids),
        "decision_identity_linked_fill_rows": len(fills) - len(missing_decision_ids),
        "decision_identity_missing_fill_rows": len(missing_decision_ids),
        "decision_run_mismatch_fill_rows": len(decision_run_mismatch_ids),
        "unlinked_fill_ids": unlinked_fill_ids,
        "decision_identity_missing_fill_ids": missing_decision_ids,
        "decision_run_mismatch_fill_ids": decision_run_mismatch_ids,
    }


def _weekly_learning_evidence(
    *,
    fill_attribution_coverage: dict[str, object],
    realized_pnl_coverage: dict[str, object],
    realized_pnl_summary: dict[str, object],
    closed_trades: Sequence[dict[str, object]],
    weekly_order_status_counts: Sequence[dict[str, object]],
    execution_failures: Sequence[dict[str, object]],
    truncated: dict[str, bool],
) -> dict[str, object]:
    """Build a stable, fail-closed contract for downstream learning consumers."""

    closed_trade_lineage_missing_ids = sorted(
        {
            str(row.get("sell_fill_id"))
            for row in closed_trades
            if row.get("source_execution_plan_item_id") is None
            or row.get("decision_snapshot_id") is None
            or row.get("decision_run_id") is None
            or row.get("decision_agent_name") is None
        }
    )
    performance_blocked_reasons: list[str] = []
    if not closed_trades:
        performance_status = "no_data"
        performance_blocked_reasons.append("no_closed_trades")
    else:
        if truncated.get("fills") or truncated.get("closed_trades"):
            performance_blocked_reasons.append("source_rows_truncated")
        if fill_attribution_coverage.get("status") != "complete":
            performance_blocked_reasons.append("incomplete_fill_decision_lineage")
        if realized_pnl_coverage.get("status") != "complete":
            performance_blocked_reasons.append("incomplete_cost_basis_coverage")
        if realized_pnl_summary.get("transaction_cost_coverage") != "complete":
            performance_blocked_reasons.append("incomplete_transaction_cost_coverage")
        if realized_pnl_summary.get("currency_coverage") != "complete":
            performance_blocked_reasons.append("incomplete_settlement_currency_coverage")
        if closed_trade_lineage_missing_ids:
            performance_blocked_reasons.append("incomplete_closed_trade_decision_lineage")
        performance_status = "blocked" if performance_blocked_reasons else "ready"

    failure_rows_with_lineage = sum(
        1
        for row in execution_failures
        if row.get("execution_plan_item_id") is not None
        and row.get("strategy_run_id") is not None
        and row.get("decision_actor") is not None
    )
    failure_status_counts = _count_values(execution_failures, "status")
    failure_classification_counts = _count_values(execution_failures, "classification")
    failure_retryability_counts = {
        "retryable": sum(row.get("retryable") is True for row in execution_failures),
        "not_retryable": sum(row.get("retryable") is False for row in execution_failures),
        "unknown": sum(row.get("retryable") is None for row in execution_failures),
    }
    terminal_order_outcomes = {
        f"{str(row.get('side') or 'unknown')}:{str(row.get('status') or 'unknown')}": int(
            row.get("count") or 0
        )
        for row in weekly_order_status_counts
        if _is_failure_or_rejection_status(str(row.get("status") or ""))
    }
    failure_evidence_truncated = bool(
        truncated.get("weekly_orders") or truncated.get("execution_failures")
    )
    return {
        "schema_version": WEEKLY_LEARNING_EVIDENCE_SCHEMA_VERSION,
        "performance_feedback": {
            "status": performance_status,
            "consumer_eligible": performance_status == "ready",
            "blocked_reasons": performance_blocked_reasons,
            "closed_trade_count": len(closed_trades),
            "profitability_eligible_sell_fill_count": realized_pnl_summary.get(
                "covered_sell_fill_count"
            ),
            "cost_basis_coverage": realized_pnl_coverage.get("status"),
            "transaction_cost_coverage": realized_pnl_summary.get(
                "transaction_cost_coverage"
            ),
            "settlement_currency_coverage": realized_pnl_summary.get(
                "currency_coverage"
            ),
            "fill_decision_lineage_coverage": fill_attribution_coverage.get("status"),
            "closed_trade_decision_lineage_missing_count": len(
                closed_trade_lineage_missing_ids
            ),
            "closed_trade_decision_lineage_missing_sell_fill_ids": (
                closed_trade_lineage_missing_ids
            ),
        },
        "failure_and_rejection_feedback": {
            "status": (
                "truncated"
                if failure_evidence_truncated
                else ("complete" if execution_failures or terminal_order_outcomes else "no_data")
            ),
            "consumer_eligible": not failure_evidence_truncated
            and bool(execution_failures or terminal_order_outcomes),
            "execution_failure_count": len(execution_failures),
            "lineage_linked_execution_failure_count": failure_rows_with_lineage,
            "lineage_missing_execution_failure_count": (
                len(execution_failures) - failure_rows_with_lineage
            ),
            "failure_status_counts": failure_status_counts,
            "failure_classification_counts": failure_classification_counts,
            "failure_retryability_counts": failure_retryability_counts,
            "terminal_order_outcome_counts": terminal_order_outcomes,
        },
    }


def _count_values(
    rows: Sequence[dict[str, object]],
    key: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _is_failure_or_rejection_status(status: str) -> bool:
    normalized = status.strip().lower()
    return any(
        marker in normalized
        for marker in ("failed", "rejected", "cancelled", "canceled", "manual_review")
    )


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
    settings: Settings,
    reference_at: datetime,
    max_stale_order_samples: int,
) -> dict[str, object]:
    stale_before = reference_at - timedelta(
        seconds=settings.stale_broker_order_reconciliation_threshold_seconds
    )
    actionable_counts = db.execute(
        select(
            BrokerOrder.status,
            func.count(BrokerOrder.id).label("count"),
            func.min(BrokerOrder.submitted_at).label("oldest_submitted_at"),
            func.max(BrokerOrder.submitted_at).label("newest_submitted_at"),
        )
        .where(*active_local_broker_order_filters(
            settings=settings,
            statuses=ACTIONABLE_OPEN_ORDER_STATUSES,
            now=reference_at,
        ))
        .group_by(BrokerOrder.status)
        .order_by(BrokerOrder.status.asc())
    ).all()
    stale_rows = db.execute(
        select(
            BrokerOrder.id,
            BrokerOrder.broker_order_id,
            BrokerOrder.idempotency_key,
            BrokerOrder.symbol,
            BrokerOrder.side,
            BrokerOrder.status,
            BrokerOrder.quantity,
            BrokerOrder.limit_price,
            BrokerOrder.submitted_at,
            BrokerOrder.last_broker_sync_at,
        )
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            or_(
                BrokerOrder.status.in_(MANUAL_REVIEW_ORDER_STATUSES),
                and_(
                    BrokerOrder.status.in_(NONTERMINAL_ORDER_STATUSES),
                    BrokerOrder.submitted_at.is_not(None),
                    BrokerOrder.submitted_at < stale_before,
                ),
            ),
        )
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.id.asc())
        .limit(max_stale_order_samples + 1)
    ).all()
    stale_samples, stale_truncated = _bounded_rows(stale_rows, max_stale_order_samples)
    missing_submitted_at_count = db.scalar(
        select(func.count(BrokerOrder.id)).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(
                (*NONTERMINAL_ORDER_STATUSES, *MANUAL_REVIEW_ORDER_STATUSES)
            ),
            BrokerOrder.submitted_at.is_(None),
        )
    )
    manual_review_order_count = db.scalar(
        select(func.count(BrokerOrder.id)).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(MANUAL_REVIEW_ORDER_STATUSES),
        )
    )
    return {
        "stale_before": stale_before,
        "actionable_status_counts": _rows_to_dicts(actionable_counts),
        # Kept for existing artifact consumers; this now intentionally has the
        # same actionable semantics as runtime counts.
        "nonterminal_status_counts": _rows_to_dicts(actionable_counts),
        "actionable_order_count": sum(int(row.count) for row in actionable_counts),
        "manual_review_order_count": int(manual_review_order_count or 0),
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
    active_next_open_counts = db.execute(
        select(
            ExecutionPlanItem.status,
            ExecutionPlanItem.intent,
            func.count(ExecutionPlanItem.id).label("count"),
            func.min(ExecutionPlanItem.updated_at).label("oldest_updated_at"),
            func.max(ExecutionPlanItem.updated_at).label("newest_updated_at"),
        )
        .select_from(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionJob.job_type == "next_open_buy",
            ExecutionJob.status.in_(ACTIVE_NEXT_OPEN_JOB_STATUSES),
            ExecutionPlanItem.status.in_(ACTIVE_NEXT_OPEN_PLAN_ITEM_STATUSES),
        )
        .group_by(ExecutionPlanItem.status, ExecutionPlanItem.intent)
        .order_by(ExecutionPlanItem.status.asc(), ExecutionPlanItem.intent.asc())
    ).all()
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
            ExecutionPlanItem.status.in_(ACTIVE_WATCH_REVIEW_STATUSES),
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
    terminal_audit_counts = db.execute(
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
            ExecutionPlanItem.status.in_(TERMINAL_AUDIT_PLAN_ITEM_STATUSES),
        )
        .group_by(ExecutionPlanItem.status, ExecutionPlanItem.intent)
        .order_by(ExecutionPlanItem.status.asc(), ExecutionPlanItem.intent.asc())
    ).all()
    return {
        "active_next_open_status_counts": _rows_to_dicts(active_next_open_counts),
        "active_watch_status_counts": _rows_to_dicts(active_watch_counts),
        "terminal_audit_status_counts": _rows_to_dicts(terminal_audit_counts),
        "weekly_created_status_counts": _rows_to_dicts(created_counts),
        "weekly_updated_status_counts": _rows_to_dicts(updated_counts),
        "weekly_status_count_semantics": "historical_audit_activity_not_active_work",
    }


def _monitoring_checkpoints(
    *,
    db: Session,
    mode: str,
    active_since: datetime,
    include_json_payloads: bool,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    columns = [
        MonitoringCheckpoint.id,
        MonitoringCheckpoint.worker_name,
        MonitoringCheckpoint.mode,
        MonitoringCheckpoint.provider,
        MonitoringCheckpoint.account_scope,
        MonitoringCheckpoint.status,
        MonitoringCheckpoint.last_heartbeat_at,
        MonitoringCheckpoint.last_success_at,
        MonitoringCheckpoint.created_at,
        MonitoringCheckpoint.updated_at,
    ]
    if include_json_payloads:
        columns.append(MonitoringCheckpoint.details)
    activity_at = func.coalesce(
        MonitoringCheckpoint.last_heartbeat_at,
        MonitoringCheckpoint.last_success_at,
        MonitoringCheckpoint.updated_at,
    )
    rows = db.execute(
        select(*columns)
        .where(
            MonitoringCheckpoint.mode == mode,
            MonitoringCheckpoint.provider.is_not(None),
            MonitoringCheckpoint.account_scope.is_not(None),
            activity_at >= active_since,
        )
        .order_by(MonitoringCheckpoint.worker_name.asc())
    ).all()
    excluded_legacy_count = int(
        db.scalar(
            select(func.count(MonitoringCheckpoint.id)).where(
                MonitoringCheckpoint.mode == mode,
                MonitoringCheckpoint.provider.is_(None),
                MonitoringCheckpoint.account_scope.is_(None),
            )
        )
        or 0
    )
    excluded_stale_scoped_count = int(
        db.scalar(
            select(func.count(MonitoringCheckpoint.id)).where(
                MonitoringCheckpoint.mode == mode,
                MonitoringCheckpoint.provider.is_not(None),
                MonitoringCheckpoint.account_scope.is_not(None),
                or_(activity_at < active_since, activity_at.is_(None)),
            )
        )
        or 0
    )
    current_rows = _rows_to_dicts(rows)
    return current_rows, {
        "current_scoped_checkpoint_count": len(current_rows),
        "excluded_legacy_unscoped_checkpoint_count": excluded_legacy_count,
        "excluded_stale_scoped_checkpoint_count": excluded_stale_scoped_count,
        "active_since": active_since,
        "semantics": "only_recent_account_scoped_checkpoints_are_current_instances",
    }


def _weekly_closed_trades(
    *,
    db: Session,
    settings: Settings,
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
        .options(
            undefer(ExecutionFill.payload),
            selectinload(ExecutionFill.broker_order).load_only(
                BrokerOrder.id,
                BrokerOrder.broker_name,
                BrokerOrder.request_metadata,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
            ),
            selectinload(ExecutionFill.broker_order)
            .selectinload(BrokerOrder.plan_item)
            .load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.decision_snapshot_id,
                ExecutionPlanItem.decision_run_id,
            ),
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
        .limit(max_rows + 1)
    ).all()
    truncated = len(newest_fills) > max_rows
    fills = dedupe_execution_fills(list(reversed(newest_fills[:max_rows])))
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    sell_route_symbols = {
        (route_scope, fill.symbol)
        for fill in fills
        if (route_scope := fifo_route_scope_from_order(fill.broker_order)) is not None
        if str(fill.side or "").lower() == "sell" and start_at <= _normalize_utc(fill.filled_at) < end_at
    }
    positions_by_route_symbol = _positions_by_route_symbol(
        db=db,
        mode=mode,
        route_symbols=sell_route_symbols,
    )
    isolation_cursor = load_fifo_isolation_cursor(db=db, runtime_mode=mode, through=end_at)
    exact_krx_scope = _weekly_exact_krx_fifo_scope(settings)

    lots_by_route_symbol: dict[
        tuple[FifoRouteScope, str],
        list[dict[str, object]],
    ] = {}
    closed: list[dict[str, object]] = []
    for fill in sorted(fills, key=fifo_fill_sort_key):
        side = str(fill.side or "").lower()
        quantity = _fill_quantity(fill)
        fill_time = _normalize_utc(fill.filled_at)
        in_period = start_at <= fill_time < end_at
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        if route_scope is None:
            if legacy_kis_krx_event_is_isolated(
                order=fill.broker_order,
                event_symbol=fill.symbol,
                event_at=fill_time,
                through=start_at,
                isolation_cursor=isolation_cursor,
                exact_route_scope=exact_krx_scope,
            ):
                continue
            if side == "sell" and in_period:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        buy_fill=None,
                        quantity=quantity,
                        buy_price=None,
                        cost_basis_source="execution_scope_unavailable",
                        cost_basis_observed_at=None,
                        route_scope=None,
                    )
                )
            continue
        route_symbol = (route_scope, fill.symbol)
        lots = lots_by_route_symbol.setdefault(route_symbol, [])
        scope_quality_available = isolation_cursor.apply_before(
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=fill_time,
            lots=lots,
        )
        if fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=fill_time,
        ):
            if side == "sell" and in_period:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        buy_fill=None,
                        quantity=quantity,
                        buy_price=None,
                        cost_basis_source="fill_chronology_ambiguous",
                        cost_basis_observed_at=None,
                        route_scope=route_scope,
                    )
                )
            continue
        if not scope_quality_available:
            if side == "sell" and in_period:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        buy_fill=None,
                        quantity=quantity,
                        buy_price=None,
                        cost_basis_source="execution_scope_unavailable",
                        cost_basis_observed_at=None,
                        route_scope=route_scope,
                    )
                )
            continue
        if side == "buy":
            lots.append(
                {
                    "quantity": quantity,
                    "price": Decimal(fill.price),
                    "filled_at": fill_time,
                    "fill_id": fill.id,
                    "fill": fill,
                }
            )
            continue
        if side != "sell":
            continue
        filled_at = fill_time
        next_buy_after_sell = _next_buy_after(
            fills=fills,
            route_scope=route_scope,
            symbol=fill.symbol,
            after=filled_at,
        )
        remaining = quantity
        while remaining > 0 and lots:
            lot = lots[0]
            lot_quantity = Decimal(str(lot["quantity"]))
            matched = min(remaining, lot_quantity)
            if in_period:
                buy_price = Decimal(str(lot["price"]))
                cost_basis_source = "fill_fifo"
                position = positions_by_route_symbol.get(route_symbol)
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
                        buy_fill=(
                            None
                            if cost_basis_source == "broker_reconciliation"
                            else lot["fill"]
                        ),
                        quantity=matched,
                        buy_price=buy_price,
                        cost_basis_source=cost_basis_source,
                        cost_basis_observed_at=cost_basis_observed_at,
                        route_scope=route_scope,
                    )
                )
            lot["quantity"] = lot_quantity - matched
            remaining -= matched
            if Decimal(str(lot["quantity"])) <= 0:
                lots.pop(0)
        if remaining > 0 and in_period:
            position = positions_by_route_symbol.get(route_symbol)
            broker_cost_basis = _broker_reconciliation_cost_basis(
                position=position,
                sold_at=filled_at,
                next_buy_after_sell=next_buy_after_sell,
            )
            if broker_cost_basis is not None:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        buy_fill=None,
                        quantity=remaining,
                        buy_price=broker_cost_basis,
                        cost_basis_source="broker_reconciliation",
                        cost_basis_observed_at=position.last_reconciled_at if position is not None else None,
                        route_scope=route_scope,
                    )
                )
            else:
                closed.append(
                    _closed_trade_row(
                        fill=fill,
                        buy_fill=None,
                        quantity=remaining,
                        buy_price=None,
                        cost_basis_source="unknown",
                        cost_basis_observed_at=None,
                        route_scope=route_scope,
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
    uncovered_rows = [
        row
        for row in closed_trades
        if row.get("cost_basis_source")
        not in {"fill_fifo", "broker_reconciliation"}
    ]
    uncovered_fill_ids = {
        fill_id
        for fill_id, rows in rows_by_fill_id.items()
        if any(
            row.get("cost_basis_source")
            not in {"fill_fifo", "broker_reconciliation"}
            for row in rows
        )
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
        currencies = {
            str(row["settlement_currency"])
            for row in rows
            if row.get("settlement_currency")
        }
        currency = (
            next(iter(currencies))
            if len(currencies) == 1
            and all(row.get("settlement_currency") for row in rows)
            else None
        )
        known_cost_values = [
            Decimal(str(row["known_transaction_cost"]))
            for row in rows
            if row.get("known_transaction_cost") is not None
        ]
        known_transaction_cost = (
            sum(known_cost_values, Decimal("0")).quantize(Decimal("0.0001"))
            if known_cost_values
            else None
        )
        net_complete = currency is not None and all(
            row.get("transaction_cost_coverage") == "complete"
            and row.get("net_realized_pnl") is not None
            for row in rows
        )
        net_realized_pnl = (
            sum(
                (Decimal(str(row["net_realized_pnl"])) for row in rows),
                Decimal("0"),
            ).quantize(Decimal("0.0001"))
            if net_complete
            else None
        )
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
                "realized_pnl": realized_pnl if currency == "KRW" else None,
                "gross_realized_pnl": realized_pnl,
                "known_transaction_cost": known_transaction_cost,
                "net_realized_pnl": net_realized_pnl,
                "settlement_currency": currency,
                "transaction_cost_coverage": "complete" if net_complete else (
                    "partial"
                    if any(
                        row.get("transaction_cost_coverage") in {"complete", "partial"}
                        for row in rows
                    )
                    else "unknown"
                ),
                "legacy_realized_pnl_basis": "gross_before_transaction_costs",
                "return_pct": return_pct,
            }
        )

    outcomes.sort(
        key=lambda row: (
            Decimal(str(row["gross_realized_pnl"])),
            str(row["sell_fill_id"]),
        )
    )
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
        (
            Decimal(str(row["gross_realized_pnl"]))
            for row in outcomes
            if row.get("settlement_currency") == "KRW"
        ),
        Decimal("0"),
    ).quantize(Decimal("0.0001"))
    all_krw = all(row.get("settlement_currency") == "KRW" for row in outcomes)
    outcome_currencies_known = all(
        row.get("settlement_currency") for row in outcomes
    )
    currency_coverage_complete = (
        len(outcomes) == len(rows_by_fill_id) and outcome_currencies_known
    )
    gross_by_currency: dict[str, Decimal] = {}
    net_by_currency: dict[str, Decimal | None] = {}
    best_trade_by_currency: dict[str, dict[str, object]] = {}
    worst_trade_by_currency: dict[str, dict[str, object]] = {}
    for currency in sorted(
        {str(row["settlement_currency"]) for row in outcomes if row.get("settlement_currency")}
    ):
        currency_rows = [row for row in outcomes if row.get("settlement_currency") == currency]
        gross_by_currency[currency] = sum(
            (Decimal(str(row["gross_realized_pnl"])) for row in currency_rows),
            Decimal("0"),
        ).quantize(Decimal("0.0001"))
        net_by_currency[currency] = (
            sum(
                (Decimal(str(row["net_realized_pnl"])) for row in currency_rows),
                Decimal("0"),
            ).quantize(Decimal("0.0001"))
            if all(row.get("net_realized_pnl") is not None for row in currency_rows)
            else None
        )
        best_trade_by_currency[currency] = max(
            currency_rows,
            key=lambda row: Decimal(str(row["gross_realized_pnl"])),
        )
        worst_trade_by_currency[currency] = min(
            currency_rows,
            key=lambda row: Decimal(str(row["gross_realized_pnl"])),
        )
    transaction_cost_coverage = (
        "complete"
        if not rows_by_fill_id
        or (
            outcomes
            and len(outcomes) == len(rows_by_fill_id)
            and all(
                row.get("transaction_cost_coverage") == "complete"
                for row in outcomes
            )
        )
        else (
            "partial"
            if any(
                row.get("transaction_cost_coverage") in {"complete", "partial"}
                for row in outcomes
            )
            else "unknown"
        )
    )
    return {
        "status": status,
        "sell_fill_count": len(rows_by_fill_id),
        "covered_sell_fill_count": len(outcomes),
        "win_count": sum(1 for row in outcomes if Decimal(str(row["gross_realized_pnl"])) > 0),
        "loss_count": sum(1 for row in outcomes if Decimal(str(row["gross_realized_pnl"])) < 0),
        "flat_count": sum(1 for row in outcomes if Decimal(str(row["gross_realized_pnl"])) == 0),
        "win_count_basis": "gross_before_transaction_costs",
        "return_pct_basis": "gross_before_transaction_costs",
        "best_worst_trade_basis": "gross_before_transaction_costs_within_currency",
        "net_win_count": sum(
            1
            for row in outcomes
            if row.get("net_realized_pnl") is not None
            and Decimal(str(row["net_realized_pnl"])) > 0
        ),
        "net_loss_count": sum(
            1
            for row in outcomes
            if row.get("net_realized_pnl") is not None
            and Decimal(str(row["net_realized_pnl"])) < 0
        ),
        "realized_pnl": covered_realized_pnl
        if status == "complete" and all_krw
        else None,
        "covered_realized_pnl": covered_realized_pnl if all_krw else None,
        "gross_realized_pnl_by_currency": gross_by_currency,
        "net_realized_pnl_by_currency": net_by_currency,
        "transaction_cost_coverage": transaction_cost_coverage,
        "currency_coverage": (
            "complete"
            if currency_coverage_complete
            else "incomplete"
        ),
        "average_return_pct": average_return_pct,
        "median_return_pct": median_return_pct,
        "best_trade": outcomes[-1]
        if outcomes and len(gross_by_currency) == 1 and outcome_currencies_known
        else None,
        "worst_trade": outcomes[0]
        if outcomes and len(gross_by_currency) == 1 and outcome_currencies_known
        else None,
        "best_trade_by_currency": best_trade_by_currency,
        "worst_trade_by_currency": worst_trade_by_currency,
    }


def _next_buy_after(
    *,
    fills: Sequence[ExecutionFill],
    route_scope: FifoRouteScope,
    symbol: str,
    after: datetime,
) -> datetime | None:
    later_buys = [
        _normalize_utc(fill.filled_at)
        for fill in fills
        if fill.symbol == symbol
        and fifo_route_scope_from_order(fill.broker_order) == route_scope
        and str(fill.side or "").lower() == "buy"
        and _normalize_utc(fill.filled_at) > after
    ]
    return min(later_buys) if later_buys else None


def _closed_trade_row(
    *,
    fill: ExecutionFill,
    buy_fill: ExecutionFill | None,
    quantity: Decimal,
    buy_price: Decimal | None,
    cost_basis_source: str,
    cost_basis_observed_at: object,
    route_scope: FifoRouteScope | None,
) -> dict[str, object]:
    sell_price = Decimal(fill.price)
    buy_item = (
        buy_fill.broker_order.plan_item
        if buy_fill is not None and buy_fill.broker_order is not None
        else None
    )
    sell_item = fill.broker_order.plan_item if fill.broker_order is not None else None
    source_item = buy_item or sell_item
    pnl = ((sell_price - buy_price) * quantity).quantize(Decimal("0.0001")) if buy_price is not None else None
    economics = (
        closed_trade_economics(
            buy_fill=buy_fill,
            sell_fill=fill,
            matched_quantity=quantity,
            gross_realized_pnl=pnl,
        )
        if pnl is not None
        else None
    )
    return {
        "symbol": fill.symbol,
        "execution_scope": (
            route_scope.as_payload() if route_scope is not None else None
        ),
        "sell_fill_id": fill.id,
        "sell_broker_order_pk": fill.broker_order_pk,
        "sell_filled_at": _normalize_utc(fill.filled_at),
        "quantity": _quantity_value(quantity),
        "sell_price": sell_price,
        "cost_basis_price": buy_price,
        "cost_basis_source": cost_basis_source,
        "cost_basis_observed_at": cost_basis_observed_at,
        "realized_pnl": pnl,
        "realized_pnl_basis": "gross_before_transaction_costs" if pnl is not None else None,
        "settlement_currency": economics.settlement_currency if economics is not None else None,
        "buy_settlement_currency": (
            economics.buy_settlement_currency if economics is not None else None
        ),
        "sell_settlement_currency": (
            economics.sell_settlement_currency if economics is not None else None
        ),
        "gross_realized_pnl": economics.gross_realized_pnl if economics is not None else None,
        "buy_transaction_cost": economics.buy_transaction_cost if economics is not None else None,
        "sell_transaction_cost": economics.sell_transaction_cost if economics is not None else None,
        "known_transaction_cost": economics.known_transaction_cost if economics is not None else None,
        "net_realized_pnl": economics.net_realized_pnl if economics is not None else None,
        "transaction_cost_coverage": (
            economics.transaction_cost_coverage if economics is not None else "unknown"
        ),
        "source_execution_plan_item_id": (
            source_item.id if source_item is not None else None
        ),
        "decision_snapshot_id": (
            source_item.decision_snapshot_id if source_item is not None else None
        ),
        "decision_run_id": (
            source_item.decision_run_id if source_item is not None else None
        ),
        "decision_agent_name": (
            source_item.decision_agent_name if source_item is not None else None
        ),
    }


def _stale_fifo_inventory_alerts(
    *,
    db: Session,
    settings: Settings,
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
        .options(
            undefer(ExecutionFill.payload),
            selectinload(ExecutionFill.broker_order).load_only(
                BrokerOrder.id,
                BrokerOrder.broker_name,
                BrokerOrder.request_metadata,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
            )
        )
        .order_by(ExecutionFill.filled_at.desc(), ExecutionFill.id.desc())
        .limit(max_rows + 1)
    ).all()
    truncated = len(newest_fills) > max_rows
    fills = dedupe_execution_fills(list(reversed(newest_fills[:max_rows])))
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    isolation_cursor = load_fifo_isolation_cursor(db=db, runtime_mode=mode, through=end_at)
    exact_krx_scope = _weekly_exact_krx_fifo_scope(settings)
    lots_by_route_symbol: dict[tuple[FifoRouteScope, str], list[Decimal]] = {}
    first_fill_at: dict[tuple[FifoRouteScope, str], datetime] = {}
    last_fill_at: dict[tuple[FifoRouteScope, str], datetime] = {}
    unavailable_scope_fill_count: dict[str, int] = {}
    ambiguous_chronology_fill_count: dict[tuple[FifoRouteScope, str], int] = {}
    generation_by_route_symbol: dict[
        tuple[FifoRouteScope, str], datetime | None
    ] = {}
    for fill in sorted(fills, key=fifo_fill_sort_key):
        observed_at = _normalize_utc(fill.filled_at)
        quantity = _fill_quantity(fill)
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        if route_scope is None:
            if legacy_kis_krx_event_is_isolated(
                order=fill.broker_order,
                event_symbol=fill.symbol,
                event_at=observed_at,
                through=end_at,
                isolation_cursor=isolation_cursor,
                exact_route_scope=exact_krx_scope,
            ):
                continue
            unavailable_scope_fill_count[fill.symbol] = (
                unavailable_scope_fill_count.get(fill.symbol, 0) + 1
            )
            continue
        route_symbol = (route_scope, fill.symbol)
        lots = lots_by_route_symbol.setdefault(route_symbol, [])
        generation_boundary = isolation_cursor.latest_boundary_before(
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=observed_at,
        )
        if (
            route_symbol in generation_by_route_symbol
            and generation_by_route_symbol[route_symbol] != generation_boundary
        ):
            lots.clear()
            ambiguous_chronology_fill_count.pop(route_symbol, None)
            first_fill_at.pop(route_symbol, None)
            last_fill_at.pop(route_symbol, None)
        generation_by_route_symbol[route_symbol] = generation_boundary
        scope_quality_available = isolation_cursor.apply_before(
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=observed_at,
            lots=lots,
        )
        if fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=observed_at,
        ):
            ambiguous_chronology_fill_count[route_symbol] = (
                ambiguous_chronology_fill_count.get(route_symbol, 0) + 1
            )
            lots.clear()
            continue
        first_fill_at[route_symbol] = min(
            first_fill_at.get(route_symbol, observed_at),
            observed_at,
        )
        last_fill_at[route_symbol] = max(
            last_fill_at.get(route_symbol, observed_at),
            observed_at,
        )
        if not scope_quality_available:
            unavailable_scope_fill_count[fill.symbol] = (
                unavailable_scope_fill_count.get(fill.symbol, 0) + 1
            )
            continue
        if str(fill.side or "").lower() == "buy":
            lots.append(quantity)
        elif str(fill.side or "").lower() == "sell":
            remaining = quantity
            while remaining > 0 and lots:
                matched = min(remaining, lots[0])
                lots[0] -= matched
                remaining -= matched
                if lots[0] <= 0:
                    lots.pop(0)
    quantities: dict[tuple[FifoRouteScope, str], Decimal] = {}
    for (route_scope, symbol), lots in lots_by_route_symbol.items():
        route_symbol = (route_scope, symbol)
        final_boundary = isolation_cursor.latest_boundary_through(
            route_scope=route_scope,
            symbol=symbol,
            observed_at=end_at,
        )
        if (
            final_boundary is not None
            and final_boundary != generation_by_route_symbol.get(route_symbol)
        ):
            ambiguous_chronology_fill_count.pop(route_symbol, None)
            first_fill_at.pop(route_symbol, None)
            last_fill_at.pop(route_symbol, None)
        isolation_cursor.apply_through(
            route_scope=route_scope,
            symbol=symbol,
            observed_at=end_at,
            lots=lots,
        )
        quantities[route_symbol] = sum(lots, Decimal("0"))
    candidate_route_symbols = {
        route_symbol
        for route_symbol, quantity in quantities.items()
        if quantity > 0
    }
    positions_by_route_symbol = _positions_by_route_symbol(
        db=db,
        mode=mode,
        route_symbols=candidate_route_symbols,
    )
    alerts: list[dict[str, object]] = [
        {
            "kind": "fifo_execution_scope_unavailable",
            "symbol": symbol,
            "execution_scope": None,
            "unscoped_fill_count": fill_count,
            "repair_action": "bind fills to an exact broker execution scope",
            "message": "FIFO inventory was excluded because execution scope is unavailable",
        }
        for symbol, fill_count in sorted(unavailable_scope_fill_count.items())
    ]
    alerts.extend(
        {
            "kind": "fifo_fill_chronology_ambiguous",
            "symbol": symbol,
            "execution_scope": route_scope.as_payload(),
            "ambiguous_fill_count": fill_count,
            "repair_action": "obtain broker fill sequence evidence before FIFO replay",
            "message": "materially order-sensitive fills share one broker timestamp and were excluded from FIFO",
        }
        for (route_scope, symbol), fill_count in sorted(
            ambiguous_chronology_fill_count.items(),
            key=lambda item: (*item[0][0].as_payload().values(), item[0][1]),
        )
    )
    for route_scope, symbol in sorted(
        candidate_route_symbols,
        key=lambda item: (*item[0].as_payload().values(), item[1]),
    ):
        route_symbol = (route_scope, symbol)
        position = positions_by_route_symbol.get(route_symbol)
        if position is None or position.last_reconciled_at is None:
            continue
        broker_quantity = _position_quantity(position)
        if broker_quantity > 0:
            continue
        alerts.append(
            {
                "kind": "stale_fill_fifo_inventory_flat_broker_position",
                "symbol": symbol,
                "execution_scope": route_scope.as_payload(),
                "fifo_open_quantity": _quantity_value(quantities[route_symbol]),
                "broker_position_quantity": _quantity_value(broker_quantity),
                "position_last_reconciled_at": position.last_reconciled_at,
                "fifo_scan_first_fill_at": first_fill_at.get(route_symbol),
                "fifo_scan_last_fill_at": last_fill_at.get(route_symbol),
                "repair_action": "reconcile broker execution history before retiring local FIFO lots",
                "message": "fill FIFO leaves open quantity but latest broker reconciliation reports flat",
            }
        )
    return alerts, truncated


def _positions_by_route_symbol(
    *,
    db: Session,
    mode: str,
    route_symbols: set[tuple[FifoRouteScope, str]],
) -> dict[tuple[FifoRouteScope, str], ExecutionPosition]:
    if not route_symbols:
        return {}
    route_predicates = [
        and_(
            ExecutionPosition.provider == route_scope.provider,
            ExecutionPosition.account_scope == route_scope.account_scope,
            ExecutionPosition.exchange == route_scope.exchange,
            ExecutionPosition.symbol == symbol,
        )
        for route_scope, symbol in sorted(
            route_symbols,
            key=lambda item: (*item[0].as_payload().values(), item[1]),
        )
    ]
    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == mode,
            or_(*route_predicates),
        )
    ).all()
    positions_by_route: dict[
        tuple[str, str, str, str],
        list[ExecutionPosition],
    ] = {}
    for position in positions:
        if (
            position.provider is None
            or position.account_scope is None
            or position.exchange is None
        ):
            continue
        key = (
            position.provider.strip().lower(),
            position.account_scope,
            position.exchange.strip().upper(),
            position.symbol,
        )
        positions_by_route.setdefault(key, []).append(position)
    return {
        (route_scope, symbol): matches[0]
        for route_scope, symbol in route_symbols
        if len(
            matches := positions_by_route.get(
                (
                    route_scope.provider,
                    route_scope.account_scope,
                    route_scope.exchange,
                    symbol,
                ),
                [],
            )
        )
        == 1
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
    active_since: datetime,
) -> dict[str, object]:
    activity_at = func.coalesce(
        MonitoringCheckpoint.last_heartbeat_at,
        MonitoringCheckpoint.last_success_at,
        MonitoringCheckpoint.updated_at,
    )
    rows = db.execute(
        select(
            MonitoringCheckpoint.worker_name,
            MonitoringCheckpoint.status,
            MonitoringCheckpoint.last_heartbeat_at,
            MonitoringCheckpoint.details,
        )
        .where(
            MonitoringCheckpoint.mode == mode,
            MonitoringCheckpoint.provider.is_not(None),
            MonitoringCheckpoint.account_scope.is_not(None),
            activity_at >= active_since,
        )
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
        redacted = redact_account_references(value)
        if not isinstance(redacted, dict):
            return {}
        return {str(key): _json_safe(item) for key, item in redacted.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _atomic_write_text(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


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
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "Explicit non-secret SQLAlchemy URL. Supply credential-bearing URLs "
            "through KAI_EXEC_DATABASE_URL so they are not exposed in argv."
        ),
    )
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

    reject_secret_database_url_argument(args.database_url)

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
            "is 'live'; source the service .env or pass a non-secret --database-url"
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
