import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import distinct, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.session import ping_database
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    MonitoringCheckpoint,
)
from app.services.monitoring import (
    MONITORING_RUNTIME_TOPOLOGY,
    MONITORING_SELECTION_STRATEGY,
    PENDING_DISPATCH_MONITORING_STATUSES,
)
from app.services.broker_auth import build_broker_session_summary
from app.services.execution_api import list_runtime_symbols
from app.services.schema import check_runtime_schema
from app.services.runtime_controls import (
    ensure_runtime_control_state,
    runtime_safety_payload,
)

OPEN_ORDER_STATUSES = ("submitted", "open", "partially_filled")
ACTIVE_JOB_STATUSES = ("queued", "scheduled", "running", "retrying")
MONITORED_INTENTS = ("target_sell", "stop_sell")
SELL_MONITORED_INTENTS = ("target_sell", "stop_sell", "max_holding_close_sell")
WATCH_BUY_INTENTS = ("watch_buy", "trigger_buy", "chase_buy", "tactical_buy", "intraday_watch_buy", "next_open_buy")
WATCH_BUY_STATUSES = (
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "watch_buy_triggered",
    "watch_buy_triggered_blocked",
    "watch_buy_triggered_pending_dispatch",
    "watch_buy_submitted",
    "watch_buy_rejected",
)
SHORT_TERM_BLOCKED_STATUSES = (
    "short_term_watch_blocked",
    "pending_configuration",
    "watch_buy_rejected",
    "watch_cancelled",
    "watch_expired",
)
SHORT_TERM_DECIDER_NAME = "short-term-decider"
SIGNAL_DECIDER_NAME = "signal-decider"
FAILED_ORDER_STATUSES = ("rejected", "failed")
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class RuntimeMetadata:
    started_at: datetime

    @property
    def process_id(self) -> int:
        return os.getpid()


def liveness_payload(runtime: RuntimeMetadata) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "process_id": runtime.process_id,
        "started_at": runtime.started_at.isoformat(),
        "uptime_seconds": round((now - runtime.started_at).total_seconds(), 3),
    }


def default_safety(settings: Settings, reason: str) -> dict[str, object]:
    return {
        "mode": settings.runtime_mode,
        "paused": True,
        "kill_switch_enabled": False,
        "automation_enabled": False,
        "automated_order_submission_allowed": False,
        "automated_order_submission_block_reason": "paused",
        "readiness_reason": reason,
        "updated_at": None,
        "last_change": {
            "action": "bootstrap",
            "actor": None,
            "source": "system",
            "reason": reason,
            "changed_at": None,
            "event_id": None,
        },
    }


def build_runtime_safety(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
    except SQLAlchemyError as exc:
        db.rollback()
        return default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )
    return runtime_safety_payload(state)


def runtime_counts(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, int]:
    open_orders = db.scalar(
        select(func.count())
        .select_from(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(OPEN_ORDER_STATUSES),
        ),
    )
    active_jobs = db.scalar(
        select(func.count())
        .select_from(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(ACTIVE_JOB_STATUSES),
        ),
    )
    return {
        "open_orders": int(open_orders or 0),
        "active_execution_jobs": int(active_jobs or 0),
    }


def runtime_symbol_summary(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, list[str]]:
    today_start_utc, tomorrow_start_utc = _current_krx_day_utc_bounds()
    failed_observed_at = func.coalesce(
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.submitted_at,
        BrokerOrder.updated_at,
    )
    monitored_symbols = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            or_(
                ExecutionPlanItem.intent.in_(MONITORED_INTENTS),
                ExecutionPlanItem.status.like("watch_%"),
            ),
        )
    ).all()
    scheduled_today = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
        .select_from(ExecutionJob)
        .join(ExecutionJob.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionJob.status.in_(ACTIVE_JOB_STATUSES),
            ExecutionJob.scheduled_for.is_not(None),
            ExecutionJob.scheduled_for >= today_start_utc,
            ExecutionJob.scheduled_for < tomorrow_start_utc,
        )
    ).all()
    filled_today = db.scalars(
        select(distinct(ExecutionFill.symbol))
        .select_from(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at >= today_start_utc,
            ExecutionFill.filled_at < tomorrow_start_utc,
        )
    ).all()
    failed_today = db.scalars(
        select(distinct(BrokerOrder.symbol))
        .select_from(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(FAILED_ORDER_STATUSES),
            failed_observed_at.is_not(None),
            failed_observed_at >= today_start_utc,
            failed_observed_at < tomorrow_start_utc,
        )
    ).all()

    return {
        "monitored": sorted(monitored_symbols),
        "scheduled_today": sorted(scheduled_today),
        "filled_today": sorted(filled_today),
        "failed_today": sorted(failed_today),
    }


def _current_krx_day_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current_krx = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    day_start_krx = current_krx.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day_start_krx = day_start_krx + timedelta(days=1)
    return day_start_krx.astimezone(UTC), next_day_start_krx.astimezone(UTC)


def monitoring_payload(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    segments = monitoring_segments(settings=settings, db=db)
    backlog_symbols = db.scalars(
        select(distinct(ExecutionPlanItem.symbol))
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.status.in_(tuple(PENDING_DISPATCH_MONITORING_STATUSES)),
        )
        .order_by(ExecutionPlanItem.symbol.asc())
    ).all()
    backlog_item_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.status.in_(tuple(PENDING_DISPATCH_MONITORING_STATUSES)),
            )
        )
        or 0
    )
    checkpoints = db.scalars(select(MonitoringCheckpoint)).all()
    checkpoints = [
        checkpoint
        for checkpoint in checkpoints
        if checkpoint.mode == settings.runtime_mode
    ]
    if not checkpoints:
        return {
            "status": "not_started",
            "topology": MONITORING_RUNTIME_TOPOLOGY,
            "scheduler": {
                "selection_strategy": MONITORING_SELECTION_STRATEGY,
                "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
            },
            "dispatcher": {
                "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
                "last_cycle_action_count": 0,
                "backlog_item_count": backlog_item_count,
                "backlog_symbols": backlog_symbols,
            },
            "segments": segments,
            "workers": [],
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_cycle": None,
        }

    heartbeat_values = [
        checkpoint.last_heartbeat_at
        for checkpoint in checkpoints
        if checkpoint.last_heartbeat_at is not None
    ]
    success_values = [
        checkpoint.last_success_at
        for checkpoint in checkpoints
        if checkpoint.last_success_at is not None
    ]
    last_heartbeat = max(heartbeat_values).isoformat() if heartbeat_values else None
    last_success = max(success_values).isoformat() if success_values else None
    latest_checkpoint = max(
        checkpoints,
        key=lambda checkpoint: (
            checkpoint.last_heartbeat_at or checkpoint.last_success_at or datetime.min.replace(tzinfo=UTC)
        ),
    )
    latest_details = latest_checkpoint.details or {}
    if backlog_item_count > 0 or any(checkpoint.status == "degraded" for checkpoint in checkpoints):
        status = "degraded"
    elif last_heartbeat:
        status = "running"
    else:
        status = "not_started"
    return {
        "status": status,
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "scheduler": {
            "selection_strategy": MONITORING_SELECTION_STRATEGY,
            "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        },
        "dispatcher": {
            "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
            "last_cycle_action_count": int(latest_details.get("dispatch_action_count", 0) or 0),
            "backlog_item_count": backlog_item_count,
            "backlog_symbols": backlog_symbols,
        },
        "segments": segments,
        "workers": [
            {
                "name": checkpoint.worker_name,
                "status": checkpoint.status,
                "last_heartbeat_at": (
                    checkpoint.last_heartbeat_at.isoformat()
                    if checkpoint.last_heartbeat_at
                    else None
                ),
                "last_success_at": (
                    checkpoint.last_success_at.isoformat()
                    if checkpoint.last_success_at
                    else None
                ),
            }
            for checkpoint in checkpoints
        ],
        "last_heartbeat_at": last_heartbeat,
        "last_success_at": last_success,
        "last_cycle": latest_details or None,
    }


def monitoring_segments(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
    ).all()
    held_sell = [
        item
        for item in items
        if item.side == "sell"
        and item.intent in SELL_MONITORED_INTENTS
        and (
            item.status.endswith("_monitoring")
            or item.status.endswith("_pending_dispatch")
            or item.status.endswith("_triggered_blocked")
        )
    ]
    short_term_watch = [
        item
        for item in items
        if _is_short_term_decider_item(item)
        and item.side == "buy"
        and item.intent in WATCH_BUY_INTENTS
        and item.status in WATCH_BUY_STATUSES
    ]
    signal_next_open = [
        item
        for item in items
        if item.side == "buy"
        and item.intent == "next_open_buy"
        and _normalized_agent(item.decision_agent_name) == SIGNAL_DECIDER_NAME
        and item.status.startswith("next_open_buy")
    ]
    blocked_short_term = [
        item
        for item in items
        if _is_short_term_decider_item(item)
        and item.side == "buy"
        and item.status in SHORT_TERM_BLOCKED_STATUSES
    ]
    return {
        "held_position_sell_monitoring": _segment_payload(held_sell),
        "short_term_buy_watch_monitoring": _segment_payload(short_term_watch),
        "signal_decider_next_open_market_buys": _segment_payload(signal_next_open),
        "blocked_short_term_candidates": _segment_payload(blocked_short_term, include_blockers=True),
    }


def _segment_payload(
    items: list[ExecutionPlanItem],
    *,
    include_blockers: bool = False,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "item_count": len(items),
        "symbols": sorted({item.symbol for item in items}),
    }
    if include_blockers:
        payload["items"] = [
            {
                "execution_plan_item_id": item.id,
                "symbol": item.symbol,
                "status": item.status,
                "tactical_action": _risk_context_text(item, "tactical_action"),
                "blocker_reason": _risk_context_text(item, "short_term_blocker_reason")
                or _risk_context_text(item, "last_monitoring_event", nested_key="reason"),
            }
            for item in sorted(items, key=lambda item: (item.symbol, item.id))
        ]
    return payload


def _is_short_term_decider_item(item: ExecutionPlanItem) -> bool:
    return _normalized_agent(item.decision_agent_name) == SHORT_TERM_DECIDER_NAME


def _normalized_agent(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().lower()


def _risk_context_text(
    item: ExecutionPlanItem,
    key: str,
    *,
    nested_key: str | None = None,
) -> str | None:
    if not isinstance(item.risk_context, dict):
        return None
    value = item.risk_context.get(key)
    if nested_key is not None:
        if not isinstance(value, dict):
            return None
        value = value.get(nested_key)
    if value is None:
        return None
    return str(value)


def readiness_payload(
    *,
    dependencies_ok: bool,
    safety: dict[str, object],
    counts_available: bool,
    extra_reasons: list[str] | None = None,
) -> dict[str, object]:
    reasons: list[str] = []
    if not dependencies_ok:
        reasons.append("database or runtime schema dependency is not ready")
    if safety["kill_switch_enabled"]:
        reasons.append("kill switch is enabled")
    if safety["paused"]:
        reasons.append("runtime is paused")
    if not counts_available:
        reasons.append("runtime counters are unavailable")
    if extra_reasons:
        reasons.extend(extra_reasons)

    return {
        "ready": not reasons,
        "reasons": reasons,
    }


def build_runtime_status(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    autonomous_runtime: dict[str, object] | None = None,
) -> dict[str, object]:
    dependencies = {
        "database": "ok",
        "runtime_schema": "ok",
    }
    counts: dict[str, int] = {"open_orders": 0, "active_execution_jobs": 0}
    autonomous = autonomous_runtime or _default_autonomous_runtime_payload(settings)
    monitoring_price_source = _autonomous_monitoring_price_source(autonomous)
    monitoring: dict[str, object] = {
        "status": "not_started",
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "scheduler": {
            "selection_strategy": MONITORING_SELECTION_STRATEGY,
            "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        },
        "dispatcher": {
            "max_broker_actions_per_cycle": settings.monitoring_max_broker_actions_per_cycle,
            "last_cycle_action_count": 0,
            "backlog_item_count": 0,
            "backlog_symbols": [],
        },
        "workers": [],
        "last_heartbeat_at": None,
        "last_success_at": None,
        "last_cycle": None,
        "price_source": monitoring_price_source,
    }
    symbols: dict[str, list[str]] = {
        "monitored": [],
        "scheduled_today": [],
        "filled_today": [],
        "failed_today": [],
    }
    broker = build_broker_session_summary(settings=settings, db=None)
    counts_available = True

    try:
        ping_database(db)
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["database"] = "error"
        dependencies["runtime_schema"] = "unknown"
        counts_available = False
        safety = default_safety(
            settings,
            f"database unavailable: {exc.__class__.__name__}",
        )
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": autonomous,
                "monitoring": monitoring,
            },
            "broker": broker,
            "counts": counts,
            "symbols": symbols,
        }

    schema = check_runtime_schema(db)
    dependencies["runtime_schema"] = str(schema["status"])
    if schema["status"] != "ok":
        counts_available = False
        missing_tables = schema.get("missing_tables") or []
        if missing_tables:
            schema_reason = (
                "runtime schema incomplete: "
                f"missing {', '.join(str(table) for table in missing_tables)}; run migrations"
            )
        else:
            schema_reason = "runtime schema unavailable; run migrations"
        safety = default_safety(settings, schema_reason)
        readiness = readiness_payload(
            dependencies_ok=False,
            safety=safety,
            counts_available=counts_available,
        )
        return {
            "service": settings.service_name,
            "environment": settings.environment,
            "mode": safety["mode"],
            "ready": readiness["ready"],
            "readiness": readiness,
            "safety": safety,
            "dependencies": dependencies,
            "liveness": {
                **liveness_payload(runtime),
                "autonomous": autonomous,
                "monitoring": monitoring,
            },
            "broker": broker,
            "counts": counts,
            "symbols": symbols,
        }

    try:
        state = ensure_runtime_control_state(settings=settings, db=db)
        safety = runtime_safety_payload(state)
        counts = runtime_counts(settings=settings, db=db)
        monitoring = monitoring_payload(settings=settings, db=db)
        monitoring["price_source"] = monitoring_price_source
        broker = build_broker_session_summary(settings=settings, db=db)
        symbols = runtime_symbol_summary(settings=settings, db=db)
        monthly_pnl = monthly_pnl_payload(settings=settings, db=db)
    except SQLAlchemyError as exc:
        db.rollback()
        dependencies["runtime_schema"] = "error"
        counts_available = False
        safety = default_safety(
            settings,
            f"runtime state unavailable: {exc.__class__.__name__}; run migrations",
        )
        monthly_pnl = _default_monthly_pnl_payload(settings=settings)

    monitoring_reasons = _monitoring_price_source_readiness_reasons(
        autonomous=autonomous,
        symbols=symbols,
    )
    if monitoring_reasons:
        monitoring["status"] = "degraded"

    readiness = readiness_payload(
        dependencies_ok=all(value == "ok" for value in dependencies.values()),
        safety=safety,
        counts_available=counts_available,
        extra_reasons=monitoring_reasons,
    )
    return {
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": safety["mode"],
        "ready": readiness["ready"],
        "readiness": readiness,
        "safety": safety,
        "dependencies": dependencies,
        "liveness": {
            **liveness_payload(runtime),
            "autonomous": autonomous,
            "monitoring": monitoring,
        },
        "broker": broker,
        "counts": counts,
        "symbols": symbols,
        "monthly_pnl": monthly_pnl,
    }


def monthly_pnl_payload(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> dict[str, object]:
    month_start, next_month_start = _current_krx_month_utc_bounds(now)
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            ExecutionFill.filled_at < next_month_start,
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()

    lots_by_symbol: dict[str, list[dict[str, Decimal]]] = {}
    monthly_realized = Decimal("0")
    monthly_sell_notional = Decimal("0")
    monthly_buy_notional = Decimal("0")
    matched_sell_quantity = 0
    unmatched_sell_quantity = 0
    for fill in fills:
        filled_at = _as_utc(fill.filled_at)
        side = fill.side.lower()
        symbol = fill.symbol
        quantity = Decimal(fill.quantity)
        notional = (fill.price * quantity).quantize(Decimal("0.0001"))
        if side == "buy":
            lots_by_symbol.setdefault(symbol, []).append(
                {"quantity": quantity, "price": fill.price}
            )
            if filled_at >= month_start:
                monthly_buy_notional += notional
            continue
        if side != "sell":
            continue
        if filled_at >= month_start:
            monthly_sell_notional += notional
        remaining = quantity
        lots = lots_by_symbol.setdefault(symbol, [])
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, lot["quantity"])
            if filled_at >= month_start:
                monthly_realized += ((fill.price - lot["price"]) * matched).quantize(Decimal("0.0001"))
                matched_sell_quantity += int(matched)
            lot["quantity"] -= matched
            remaining -= matched
            if lot["quantity"] <= 0:
                lots.pop(0)
        if remaining > 0 and filled_at >= month_start:
            unmatched_sell_quantity += int(remaining)

    target = getattr(settings, "monthly_target_krw", Decimal("500000"))
    progress = (monthly_realized / target * Decimal("100")).quantize(Decimal("0.01")) if target > 0 else Decimal("0")
    expected_progress = _expected_month_progress_pct(now)
    recommendation = _monthly_mode_recommendation(
        settings=settings,
        progress_pct=progress,
        expected_progress_pct=expected_progress,
    )
    return {
        "month": month_start.astimezone(KRX_TIMEZONE).strftime("%Y-%m"),
        "monthly_realized_pnl": _decimal_text(monthly_realized),
        "monthly_target_krw": _decimal_text(target),
        "target_progress_pct": _decimal_text(progress),
        "expected_calendar_progress_pct": _decimal_text(expected_progress),
        "mode_recommendation": recommendation,
        "monthly_buy_notional": _decimal_text(monthly_buy_notional),
        "monthly_sell_notional": _decimal_text(monthly_sell_notional),
        "matched_sell_quantity": matched_sell_quantity,
        "unmatched_sell_quantity": unmatched_sell_quantity,
        "pnl_method": "fifo_internal_fills",
    }


def _default_monthly_pnl_payload(*, settings: Settings) -> dict[str, object]:
    target = getattr(settings, "monthly_target_krw", Decimal("500000"))
    return {
        "month": datetime.now(KRX_TIMEZONE).strftime("%Y-%m"),
        "monthly_realized_pnl": "0.0000",
        "monthly_target_krw": _decimal_text(target),
        "target_progress_pct": "0.00",
        "expected_calendar_progress_pct": _decimal_text(_expected_month_progress_pct()),
        "mode_recommendation": "normal",
        "monthly_buy_notional": "0.0000",
        "monthly_sell_notional": "0.0000",
        "matched_sell_quantity": 0,
        "unmatched_sell_quantity": 0,
        "pnl_method": "fifo_internal_fills",
    }


def _current_krx_month_utc_bounds(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    month_start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)
    return month_start.astimezone(UTC), next_month.astimezone(UTC)


def _expected_month_progress_pct(now: datetime | None = None) -> Decimal:
    current = (now or datetime.now(KRX_TIMEZONE)).astimezone(KRX_TIMEZONE)
    if current.month == 12:
        next_month = current.replace(year=current.year + 1, month=1, day=1)
    else:
        next_month = current.replace(month=current.month + 1, day=1)
    days_in_month = (next_month.date() - current.replace(day=1).date()).days
    return (Decimal(current.day) / Decimal(days_in_month) * Decimal("100")).quantize(Decimal("0.01"))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _monthly_mode_recommendation(
    *,
    settings: Settings,
    progress_pct: Decimal,
    expected_progress_pct: Decimal,
) -> str:
    if getattr(settings, "monthly_capital_preservation_enabled", True) and progress_pct >= Decimal("100"):
        return "capital_preservation"
    lag_ratio = getattr(settings, "monthly_aggressive_lag_ratio", Decimal("0.75"))
    if progress_pct < expected_progress_pct * Decimal(str(lag_ratio)):
        return "aggressive"
    return "normal"


def _decimal_text(value: Decimal) -> str:
    return f"{value:.4f}"


def build_runtime_daily_board(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    autonomous_runtime: dict[str, object] | None = None,
) -> dict[str, object]:
    runtime_status = build_runtime_status(
        settings=settings,
        runtime=runtime,
        db=db,
        autonomous_runtime=autonomous_runtime,
    )
    symbols: list[dict[str, object]] = []
    if runtime_status["dependencies"]["database"] == "ok" and runtime_status["dependencies"]["runtime_schema"] == "ok":
        try:
            symbols = list_runtime_symbols(settings=settings, db=db)
        except SQLAlchemyError:
            db.rollback()
            symbols = []

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "market_date": datetime.now(KRX_TIMEZONE).date().isoformat(),
        "mode": settings.runtime_mode,
        "runtime": runtime_status,
        "totals": {
            "symbol_count": len(symbols),
            "monitored_symbol_count": sum(symbol["monitored"] for symbol in symbols),
            "open_order_symbol_count": sum(symbol["open_order_count"] > 0 for symbol in symbols),
            "filled_symbol_count": sum(symbol["fill_count"] > 0 for symbol in symbols),
            "position_symbol_count": sum(symbol["position_quantity"] > 0 for symbol in symbols),
        },
        "symbols": symbols,
    }


def _default_autonomous_runtime_payload(settings: Settings) -> dict[str, object]:
    default_status = "idle" if settings.autonomous_runtime_enabled else "disabled"
    return {
        "enabled": settings.autonomous_runtime_enabled,
        "dispatch": {
            "worker_name": "execution_job_dispatch_loop",
            "status": default_status,
            "poll_interval_seconds": settings.dispatch_loop_interval_seconds,
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_cycle": None,
        },
        "monitoring": {
            "worker_name": "monitoring_dispatch_loop",
            "status": default_status,
            "poll_interval_seconds": settings.monitoring_loop_interval_seconds,
            "last_started_at": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_error": None,
            "last_cycle": None,
            "price_source_configured": False,
            "price_source": {
                "configured": False,
                "source_name": None,
                "provider": None,
                "status": "disabled",
                "reason": "no monitoring price source is configured",
                "last_checked_at": None,
                "last_success_at": None,
                "last_error": None,
                "last_error_at": None,
                "last_symbol_count": 0,
                "last_price_count": 0,
            },
        },
    }


def _autonomous_monitoring_price_source(autonomous: dict[str, object]) -> dict[str, object]:
    monitoring = autonomous.get("monitoring")
    if isinstance(monitoring, dict):
        price_source = monitoring.get("price_source")
        if isinstance(price_source, dict):
            return price_source
        if monitoring.get("price_source_configured") is True:
            return {
                "configured": True,
                "source_name": None,
                "provider": None,
                "status": "configured",
                "reason": None,
                "last_checked_at": None,
                "last_success_at": None,
                "last_error": None,
                "last_error_at": None,
                "last_symbol_count": 0,
                "last_price_count": 0,
            }
    return {
        "configured": False,
        "source_name": None,
        "provider": None,
        "status": "disabled",
        "reason": "no monitoring price source is configured",
        "last_checked_at": None,
        "last_success_at": None,
        "last_error": None,
        "last_error_at": None,
        "last_symbol_count": 0,
        "last_price_count": 0,
    }


def _monitoring_price_source_readiness_reasons(
    *,
    autonomous: dict[str, object],
    symbols: dict[str, list[str]],
) -> list[str]:
    if not autonomous.get("enabled"):
        return []
    monitored_symbols = symbols.get("monitored") or []
    if not monitored_symbols:
        return []

    price_source = _autonomous_monitoring_price_source(autonomous)
    if not price_source.get("configured"):
        return ["monitoring price source is not configured while monitored symbols exist"]

    status = str(price_source.get("status") or "")
    if status in {"configured", "ready"}:
        return []

    reason = price_source.get("reason")
    if isinstance(reason, str) and reason.strip():
        return [f"monitoring price source is degraded: {reason}"]
    return ["monitoring price source is degraded while monitored symbols exist"]
