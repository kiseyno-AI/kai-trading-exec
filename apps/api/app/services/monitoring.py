from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from functools import partial
from hashlib import sha256
import hmac
import json
import logging
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from sqlalchemy import and_, distinct, func, or_, select, tuple_, update
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, load_only, selectinload
from sqlalchemy.orm.attributes import set_committed_value

from app.api.schemas.execution import (
    MonitoringEvaluationRequest,
    MonitoringPriceRequest,
    PlaceOrderRequest,
)
from app.broker.base import (
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerIntradayLiquiditySnapshot,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    MonitoringCheckpoint,
    new_id,
)
from app.services.account_drawdown_recovery import snapshot_is_fresh_for_recovery
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.fill_economics import effective_fill_quantity
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.fifo_inventory import (
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
    replay_open_long_fill_inventory,
)
from app.services.execution_account_route import (
    EXECUTION_ACCOUNT_SCOPE_KEY,
    KIS_ACCOUNT_ROUTE_BINDING_KEY,
    bind_kis_account_route,
    classify_kis_account_route,
    configured_kis_route_fingerprint,
    execution_account_scope_binding_from_payloads,
    kis_account_route_binding_from_payloads,
    validate_kis_account_route,
)
from app.services.execution_semantics import (
    KRX_ASSET_CLASS_ALIASES,
    KRX_EXCHANGES,
    WATCH_BUY_INTENTS,
    canonical_execution_asset_class,
    is_delegated_exec_agent_sizing_policy,
    is_exec_agent_sizing_policy,
    is_short_term_watch_buy_intent,
    watch_buy_direction_from_payloads,
)
from app.services.reconciliation import (
    reconcile_pending_broker_orders,
    sync_execution_positions_from_broker_snapshot,
)
from app.services.broker_orders import (
    ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES,
    PLAN_ITEM_NONCLAIMABLE_STATUSES,
    _current_buy_plan_item_state,
    _plan_item_state_changed_result,
    inspect_account_drawdown_recovery,
    place_broker_order,
)
from app.services.json_compaction import compact_json_object, json_payload_hash
from app.services.plan_item_identity import (
    plan_item_entry_family_key,
    select_plan_item_family_members,
    serialize_plan_item_source_identity,
)
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_position_identity,
    position_identity_from_columns,
    position_identity_from_payload,
    stable_account_scope_hmac_key,
)
from app.services.risk_sizing import (
    RiskSizingRejection,
    RiskSizingResult,
    effective_stop_price,
    resolve_risk_based_buy_quantity,
)
from app.services.market_regime_guardrails import evaluate_market_regime_guardrail, market_regime_context_from_item_payloads
from app.services.live_market_context import load_live_market_regime_context, merge_live_market_regime_context
from app.services.monitoring_price_source import (
    _fetch_live_market_context,
    _is_trusted_exchange_freshness_source,
    monitoring_orderability_key,
    monitoring_price_from_broker_quote,
)
from app.services.runtime_controls import (
    automated_buy_submission_allowed,
    automated_buy_submission_block_reason,
    ensure_runtime_control_state,
)
from app.services.runtime_transitions import (
    ACTIVE_SELL_MONITORING_STATUSES,
    ACTIVE_WATCH_MONITORING_STATUSES,
    LIVE_WATCH_ENTRY_STATUSES,
    SELL_ACTIVE_MONITORING_STATUSES,
    SELL_TRIGGERED_BLOCKED_STATUSES,
    SELL_TRIGGERED_PENDING_DISPATCH_STATUSES,
    WATCH_LIFECYCLE_STATUSES,
    WATCH_SIZING_PENDING_STATUS,
    apply_plan_item_status,
    plan_item_status_for_broker_order_status,
)
from app.services import trigger_time_exec_agent
from app.services.exec_agent_approval import (
    BuyEconomicsAssessment,
    DELEGATED_LIQUIDITY_GATE_CODE,
    SHORT_TERM_LIVE_MIN_NET_REWARD_RISK,
    SHORT_TERM_LIVE_MIN_TURNOVER_KRW,
    assess_buy_economics,
    configured_execution_cost_evidence,
    delegated_buy_gate_rejections,
    is_legacy_unverified_exec_agent_sizing,
    trusted_live_market_decimal,
)

logger = logging.getLogger(__name__)

_CORE_DESIRED_MONITORING_SYMBOL_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}
_CORE_DESIRED_MONITORING_SYMBOL_WARNING_AT: dict[str, float] = {}

SELL_MONITORED_INTENTS = frozenset({"target_sell", "stop_sell", "max_holding_close_sell"})
MONITORED_KIS_POSITION_EXCHANGES = tuple(
    sorted(KRX_EXCHANGES | {"AMEX", "NASD", "NYSE"})
)
PRICE_TRIGGERED_MONITORED_INTENTS = WATCH_BUY_INTENTS | frozenset({"target_sell", "stop_sell"})
MONITORED_INTENTS = SELL_MONITORED_INTENTS | WATCH_BUY_INTENTS
AGENT_PENDING_STATUS = trigger_time_exec_agent.AGENT_PENDING_STATUS
AGENT_DELAY_STATUS = trigger_time_exec_agent.AGENT_DELAY_STATUS
AGENT_REJECTED_STATUS = trigger_time_exec_agent.AGENT_REJECTED_STATUS
AGENT_APPROVED_PENDING_SUBMIT_STATUS = trigger_time_exec_agent.AGENT_APPROVED_PENDING_SUBMIT_STATUS
AGENT_ACTIVE_STATUSES = trigger_time_exec_agent.AGENT_ACTIVE_STATUSES
MAX_PROTECTIVE_STOP_COVERAGE_DETAILS = 20
TRIGGER_TIME_LIQUIDITY_EVIDENCE_KEY = "exec_trigger_time_liquidity_evidence_v1"
TRIGGER_TIME_FULL_COST_ECONOMICS_KEY = "exec_trigger_time_full_cost_economics_v1"
TRIGGER_TIME_FULL_COST_ECONOMICS_PROVENANCE_VERSION = (
    "exec-trigger-time-full-cost-economics-provenance-v1"
)
SELL_MONITOR_AUTHORITY_KEY = "sell_monitor_authority_v1"
BLOCKED_MONITORING_STATUSES = SELL_TRIGGERED_BLOCKED_STATUSES | frozenset(
    {
        "watch_buy_triggered_blocked",
        "watch_cancelled",
    }
)
RETRYABLE_WATCH_BUY_APPROVAL_REASONS = frozenset(
    {
        "account_snapshot_unavailable",
        "exec_agent_sizing_pending",
        "invalid_stop_distance",
        "krx_calendar_unavailable",
        "krx_market_closed",
        "live_buy_orderbook_unavailable",
        "live_buy_orderbook_identity_mismatch",
        "live_buy_quote_untrusted",
        "live_buy_quote_timestamp_invalid",
        "live_buy_quote_future_skew",
        "live_buy_quote_stale",
        "live_buy_quote_wrong_market_date",
        "live_buy_ordering_unavailable",
        "live_buy_ask_unavailable",
        "live_buy_target_missing",
        "live_buy_stop_missing",
        "live_buy_target_geometry_invalid",
        "live_buy_stop_geometry_invalid",
        "live_market_breadth_unavailable",
        "market_breadth_risk_off",
        "sector_weakness_reject",
        "pre_submit_quote_stale",
        "trigger_time_liquidity_unavailable",
    }
)
BLOCKED_WATCH_BUY_APPROVAL_REASONS = frozenset(
    {
        "paused",
        "kill_switch",
        "hourly_plan_risk_off",
        "stale_trigger_reclaim_required",
        "late_entry_cutoff",
        "watch_buy_very_weak_liquidity",
    }
)
PENDING_DISPATCH_MONITORING_STATUSES = (
    SELL_TRIGGERED_PENDING_DISPATCH_STATUSES
    | frozenset(
        {
            "watch_buy_triggered_pending_dispatch",
        }
    )
)
NON_MONITORABLE_PLAN_ITEM_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "broker_order_rejected",
        "broker_order_failed",
        AGENT_REJECTED_STATUS,
        "submitted",
        "open",
        "partially_filled",
        "filled",
        "cancel_pending",
        "canceled",
        "cancelled",
        "contract_quarantined",
        "core_activation_pending",
        "expired",
        "retry_pending",
        "rejected",
        "revoked",
        "failed",
        "superseded",
        "watch_buy_submitted",
        "watch_buy_rejected",
        "watch_expired",
        "watch_cancelled",
        "manual_review_required",
    }
)
RETRYABLE_PROTECTIVE_STOP_STATUSES = frozenset(
    {
        "broker_order_retry_pending",
        "broker_order_rejected",
        "retry_pending",
        "rejected",
    }
)
FALLBACK_PROTECTIVE_STOP_STATUSES = frozenset({"broker_order_retry_pending", "retry_pending"})
SESSION_RETRYABLE_ERROR_CODES = frozenset({"KIOK0320", "APBK0918"})
SESSION_RETRYABLE_MESSAGE_MARKERS = (
    "장운영시간",
    "정규시장 시간 주문불가",
    "market is not open",
    "not regular market",
)
PROTECTIVE_STOP_SESSION_RETRY_MAX_ATTEMPTS = 2
ACTIVE_PROTECTIVE_STOP_ORDER_STATUSES = frozenset(CAPITAL_RESERVING_ORDER_STATUSES) - {
    "retry_pending"
}
SELL_PENDING_CONFIG_STATUSES = frozenset(
    {
        "target_sell_pending_config",
        "target_sell_pending_configuration",
        "stop_sell_pending_config",
        "stop_sell_pending_configuration",
        "max_holding_close_sell_pending_config",
    }
)
LOADABLE_SELL_MONITORING_STATUSES = (
    ACTIVE_SELL_MONITORING_STATUSES | SELL_PENDING_CONFIG_STATUSES
)
SELL_STALE_FLAT_EXPIRABLE_STATUSES = SELL_PENDING_CONFIG_STATUSES | SELL_ACTIVE_MONITORING_STATUSES
MONITORING_WORKER_NAME = "intraday_target_stop_monitor"
MONITORING_RUNTIME_TOPOLOGY = "shared_symbol_scheduler"
MONITORING_SELECTION_STRATEGY = "round_robin_by_symbol"
KRX_TIMEZONE = ZoneInfo("Asia/Seoul")
WATCH_BUY_HANDOFF_LOOKBACK_DAYS = 3
WATCH_BUY_MIN_HARD_CALENDAR_TTL_DAYS = 7
WATCH_BUY_EXPIRABLE_STATUSES = frozenset(
    {
        WATCH_SIZING_PENDING_STATUS,
        "watch_monitoring_active",
        "watch_monitoring_pending",
        "watch_buy_triggered",
        "watch_buy_triggered_blocked",
        "watch_buy_triggered_pending_dispatch",
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    }
)
OPEN_BUY_ORDER_STATUSES = frozenset(CAPITAL_RESERVING_ORDER_STATUSES)
WATCH_BUY_CONFIRMED_ORDER_STATUSES = frozenset(
    {"submitted", "open", "partially_filled", "filled"}
)
RESERVED_CASH_EXHAUSTED_REASON = "reserved_cash_exhausted"
LIVE_WATCH_BUY_VETO_EVENT_TYPE = "live_watch_buy_submission_veto"
LATE_ENTRY_GATE_EVENT_TYPE = "watch_buy_late_entry_gate"


@dataclass(frozen=True)
class EvaluateMonitoringResult:
    status_code: int
    payload: dict[str, object]


@dataclass(frozen=True)
class PendingMonitoringAction:
    item: ExecutionPlanItem
    observed_price: Decimal | None
    price: MonitoringPriceRequest | None = None
    observed_at: datetime | None = None
    market_quality_context: dict[str, object] | None = None
    trigger_reason: str | None = None
    trigger_metadata: dict[str, object] | None = None


@dataclass(frozen=True)
class WatchBuyOrderApproval:
    quantity: int | None
    reason: str | None = None
    message: str | None = None
    details: dict[str, object] | None = None


@dataclass
class WatchBuyCashReservation:
    orderable_cash: Decimal
    already_reserved_notional: Decimal
    batch_id: str

    @property
    def remaining_cash(self) -> Decimal:
        remaining = self.orderable_cash - self.already_reserved_notional
        return remaining if remaining > 0 else Decimal("0")


@dataclass(frozen=True)
class ProfitProtectionDecision:
    reason: str
    metadata: dict[str, object]


@dataclass(frozen=True)
class TriggerTimeAgentActionResult:
    quantity: int | None
    limit_price: Decimal | None
    result_payload: dict[str, object]
    refreshed_price: MonitoringPriceRequest | None = None


@dataclass(frozen=True)
class TriggerTimeAgentClaim:
    claim_id: str
    previous_status: str
    identity: tuple[object, ...]


@dataclass(frozen=True)
class ProtectiveStopCoverageIssue:
    symbol: str
    held_quantity: int | None
    active_stop_quantity: int
    missing_quantity: int | None
    latest_stop_item_id: str | None
    latest_stop_status: str | None
    latest_stop_trigger_price: Decimal | None
    outcome: str
    reason: str | None = None
    position_quantity: int | None = None
    position_last_reconciled_at: datetime | None = None
    evidence: list[str] | None = None


@dataclass(frozen=True)
class ProfitTargetCoverageIssue:
    symbol: str
    held_quantity: int
    active_target_quantity: int
    missing_quantity: int
    latest_target_item_id: str | None
    latest_target_status: str | None
    latest_target_trigger_price: Decimal | None
    outcome: str
    reason: str | None = None


@dataclass(frozen=True)
class ProtectiveStopActionabilityWarning:
    symbol: str
    execution_plan_item_id: str
    status: str
    observed_price: Decimal
    stop_trigger: Decimal
    outcome: str
    reason: str
    message: str | None = None
    block_reason: object | None = None


PROFIT_PROTECTION_TRIGGER_REASONS = frozenset(
    {"profit_protection_trailing_stop", "aged_position_profit_protection"}
)
PROTECTIVE_STOP_COVERAGE_STATUSES = frozenset(
    status
    for status in ACTIVE_SELL_MONITORING_STATUSES
    if status in AGENT_ACTIVE_STATUSES or status.startswith("stop_sell_")
) | frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "open",
        "partially_filled",
    }
)
PROTECTIVE_STOP_REPAIR_CANDIDATE_STATUSES = (
    PROTECTIVE_STOP_COVERAGE_STATUSES
    | RETRYABLE_PROTECTIVE_STOP_STATUSES
    | FALLBACK_PROTECTIVE_STOP_STATUSES
    | {"stop_sell_pending_config"}
)
PROFIT_TARGET_COVERAGE_STATUSES = frozenset(
    status
    for status in ACTIVE_SELL_MONITORING_STATUSES
    if status in AGENT_ACTIVE_STATUSES or status.startswith("target_sell_")
) | frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "open",
        "partially_filled",
    }
)
IN_FLIGHT_BROKER_ORDER_ITEM_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "open",
        "partially_filled",
    }
)


SELL_REPLAN_UPDATE_STATUSES = (
    SELL_PENDING_CONFIG_STATUSES
    | SELL_ACTIVE_MONITORING_STATUSES
    | BLOCKED_MONITORING_STATUSES
)


def materialize_monitoring_plan_items(
    plan: ExecutionPlan,
    *,
    db: Session | None = None,
    effective_at: datetime | None = None,
) -> None:
    resolved_effective_at = effective_at or datetime.now(UTC)
    if db is not None:
        _update_existing_sell_monitors_from_replanned_items(
            plan=plan,
            db=db,
            effective_at=resolved_effective_at,
        )
    for item in plan.items:
        if item.status == "superseded":
            continue
        if _is_watch_buy_item(item):
            if item.status not in {
                "pending",
                "pending_configuration",
                "next_open_buy_pending_configuration",
                "watch_monitoring_pending",
                WATCH_SIZING_PENDING_STATUS,
            }:
                continue
            quantity_policy = item.quantity_policy
            if is_exec_agent_sizing_policy(quantity_policy):
                # This is an outstanding sizing delegation, not executable
                # quantity.  Estimated entry data may make the generic watch
                # geometry look configured, but only the sizing worker may turn
                # it into a share policy and scheduled order.
                item.status = WATCH_SIZING_PENDING_STATUS
                continue
            if _monitoring_item_is_configured(item):
                item.status = "watch_monitoring_active"
                continue
            item.status = "watch_monitoring_pending"
            continue
        if item.intent not in SELL_MONITORED_INTENTS or item.status != "pending":
            continue
        if plan.market_date is not None and not _replanned_sell_plan_is_effective(
            plan=plan,
            item=item,
            effective_at=resolved_effective_at,
        ):
            # A future exit belongs to its future entry/session. Persist it as
            # dormant plan material, but never activate it against today's lot.
            continue
        if _monitoring_item_is_configured(
            item
        ) and not _quantity_policy_requires_position_exit(item.quantity_policy):
            item.status = _monitoring_status(item.intent)
            continue
        item.status = _pending_configuration_status(item.intent)


def _update_existing_sell_monitors_from_replanned_items(
    *,
    plan: ExecutionPlan,
    db: Session,
    effective_at: datetime,
) -> None:
    replanned_items = [
        item
        for item in plan.items
        if (
            item.side == "sell"
            and item.intent in SELL_MONITORED_INTENTS
            and item.source_plan_item_id
            and _replanned_sell_plan_is_effective(
                plan=plan,
                item=item,
                effective_at=effective_at,
            )
        )
    ]
    if not replanned_items:
        return

    existing_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == plan.mode,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.source_plan_item_id.in_(
                [item.source_plan_item_id for item in replanned_items if item.source_plan_item_id]
            ),
            ExecutionPlanItem.status.in_(tuple(SELL_REPLAN_UPDATE_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_plan_item_id,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.updated_at,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
                ExecutionPlan.created_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.created_at.desc(),
            ExecutionPlanItem.updated_at.desc(),
            ExecutionPlanItem.id.desc(),
        )
    ).all()
    existing_by_source: dict[
        tuple[str, str, str, tuple[str, str, str, str]],
        ExecutionPlanItem,
    ] = {}
    for existing in existing_items:
        if not existing.source_plan_item_id:
            continue
        route_key = _sell_monitor_route_key(existing)
        if route_key is None:
            continue
        key = (
            existing.source_plan_item_id,
            existing.symbol,
            existing.intent,
            route_key,
        )
        current = existing_by_source.get(key)
        if current is None or _sell_item_is_preferred_authority(existing, current):
            existing_by_source[key] = existing

    for replanned in replanned_items:
        if not replanned.source_plan_item_id:
            continue
        route_key = _sell_monitor_route_key(replanned)
        if route_key is None:
            continue
        existing = existing_by_source.get(
            (
                replanned.source_plan_item_id,
                replanned.symbol,
                replanned.intent,
                route_key,
            )
        )
        if existing is None or existing.id == replanned.id:
            continue
        authority_comparison = _compare_sell_plan_to_item_authority(plan, existing)
        if authority_comparison < 0:
            replanned.status = "superseded"
            _record_monitoring_event(
                item=replanned,
                event={
                    "event": "replanned_sell_monitor_rejected",
                    "reason": "older_source_authority",
                    "preserved_execution_plan_item_id": existing.id,
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
            )
            continue
        if authority_comparison == 0:
            if _sell_monitor_material(replanned) != _sell_monitor_material(existing):
                replanned.status = "manual_review_required"
                _record_monitoring_event(
                    item=replanned,
                    event={
                        "event": "replanned_sell_monitor_authority_conflict",
                        "reason": "equal_source_authority_with_divergent_material",
                        "preserved_execution_plan_item_id": existing.id,
                        "recorded_at": datetime.now(UTC).isoformat(),
                    },
                )
            else:
                replanned.status = "superseded"
                _record_monitoring_event(
                    item=replanned,
                    event={
                        "event": "replanned_sell_monitor_deduplicated",
                        "reason": "equal_source_authority_with_identical_material",
                        "preserved_execution_plan_item_id": existing.id,
                        "recorded_at": datetime.now(UTC).isoformat(),
                    },
                )
            continue

        prior_status = existing.status
        prior_trigger_price = existing.trigger_price
        prior_limit_price = existing.limit_price
        prior_quantity_policy = existing.quantity_policy
        prior_decision_snapshot_id = existing.decision_snapshot_id

        existing.trigger_price = replanned.trigger_price
        existing.limit_price = replanned.limit_price
        existing.quantity_policy = replanned.quantity_policy
        existing.time_in_force = replanned.time_in_force
        existing.decision_agent_name = replanned.decision_agent_name
        existing.decision_snapshot_id = replanned.decision_snapshot_id
        existing.decision_run_id = replanned.decision_run_id
        existing.source_market_date = replanned.source_market_date
        existing.provider = replanned.provider
        existing.venue = replanned.venue
        existing.asset_class = replanned.asset_class
        existing.base_quantity = replanned.base_quantity
        existing.quote_notional = replanned.quote_notional
        existing.quantity_precision = replanned.quantity_precision
        existing.price_precision = replanned.price_precision
        existing.status = (
            _monitoring_status(existing.intent)
            if _monitoring_item_is_configured(existing)
            else _pending_configuration_status(existing.intent)
        )
        existing_risk_context = dict(
            _loaded_risk_context(existing, allow_file_backed_load=True)
        )
        existing_risk_context[SELL_MONITOR_AUTHORITY_KEY] = (
            _sell_plan_authority_payload(plan)
        )
        existing.risk_context = compact_json_object(existing_risk_context)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "sell_monitor_replanned_in_place",
                "reason": "same_source_plan_item_replanned",
                "prior_status": prior_status,
                "updated_status": existing.status,
                "prior_trigger_price": _serialize_decimal(prior_trigger_price),
                "updated_trigger_price": _serialize_decimal(existing.trigger_price),
                "prior_limit_price": _serialize_decimal(prior_limit_price),
                "updated_limit_price": _serialize_decimal(existing.limit_price),
                "prior_quantity_policy": prior_quantity_policy,
                "updated_quantity_policy": replanned.quantity_policy,
                "prior_decision_snapshot_id": prior_decision_snapshot_id,
                "updated_decision_snapshot_id": existing.decision_snapshot_id,
                "replanned_execution_plan_id": plan.id,
                "authority_market_date": _serialize_date(plan.market_date),
                "authority_requested_at": (
                    _normalize_utc(plan.requested_at).isoformat()
                    if plan.requested_at is not None
                    else None
                ),
                "recorded_at": datetime.now(UTC).isoformat(),
            },
        )
        replanned.status = "superseded"
        _record_monitoring_event(
            item=replanned,
            event={
                "event": "replanned_sell_monitor_deduplicated",
                "reason": "existing_monitor_updated_in_place",
                "updated_existing_execution_plan_item_id": existing.id,
                "updated_existing_execution_plan_id": existing.execution_plan_id,
                "recorded_at": datetime.now(UTC).isoformat(),
            },
        )


def _replanned_sell_plan_is_effective(
    *,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    effective_at: datetime,
) -> bool:
    if plan.market_date is None:
        return False
    return plan.market_date <= _plan_item_market_date(
        item=item,
        effective_at=effective_at,
    )


def _plan_item_market_timezone(item: ExecutionPlanItem) -> tzinfo | None:
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    risk_exchange = (
        str(risk_context.get("exchange") or "").strip().upper()
        if isinstance(risk_context, dict)
        else ""
    )
    venue_exchange = str(item.venue or "").strip().upper()
    recognized_exchanges = KRX_EXCHANGES | {
        "US",
        "NASD",
        "NYSE",
        "AMEX",
        "UPBIT",
    }
    exchange = next(
        (
            candidate
            for candidate in (risk_exchange, venue_exchange)
            if candidate in recognized_exchanges
        ),
        "",
    )
    asset_class = str(item.asset_class or "").strip().lower()
    if asset_class == "crypto" or exchange == "UPBIT":
        return UTC
    if asset_class == "us_equity" or exchange in {"US", "NASD", "NYSE", "AMEX"}:
        return ZoneInfo("America/New_York")
    if (
        asset_class in KRX_ASSET_CLASS_ALIASES
        or exchange in KRX_EXCHANGES
        or _is_krx_equity_symbol(item.symbol)
    ):
        return KRX_TIMEZONE
    return None


def _plan_item_market_date(
    *,
    item: ExecutionPlanItem,
    effective_at: datetime,
) -> date:
    normalized_effective_at = _normalize_utc(effective_at)
    market_timezone = _plan_item_market_timezone(item)
    if market_timezone is not None:
        return normalized_effective_at.astimezone(market_timezone).date()
    return min(
        normalized_effective_at.astimezone(KRX_TIMEZONE).date(),
        normalized_effective_at.astimezone(ZoneInfo("America/New_York")).date(),
        normalized_effective_at.date(),
    )


def _monitoring_item_is_effective_at(
    *,
    item: ExecutionPlanItem,
    effective_at: datetime,
) -> bool:
    if item.plan.market_date is None:
        return item.side == "sell"
    return item.plan.market_date <= _plan_item_market_date(
        item=item,
        effective_at=effective_at,
    )


def _effective_monitor_plan_filter(
    *,
    effective_at: datetime | None,
    current_market_date: date | None,
):
    if effective_at is None and current_market_date is not None:
        return or_(
            ExecutionPlan.market_date <= current_market_date,
            and_(
                ExecutionPlan.market_date.is_(None),
                ExecutionPlanItem.side == "sell",
            ),
        )
    resolved_effective_at = _normalize_utc(effective_at or datetime.now(UTC))
    krx_date = resolved_effective_at.astimezone(KRX_TIMEZONE).date()
    us_date = resolved_effective_at.astimezone(
        ZoneInfo("America/New_York")
    ).date()
    utc_date = resolved_effective_at.date()
    normalized_asset_class = func.lower(
        func.trim(func.coalesce(ExecutionPlanItem.asset_class, ""))
    )
    risk_exchange = func.upper(
        func.trim(
            func.coalesce(ExecutionPlanItem.risk_context["exchange"].as_string(), "")
        )
    )
    venue_exchange = func.upper(
        func.trim(func.coalesce(ExecutionPlanItem.venue, ""))
    )
    recognized_exchanges = tuple(
        sorted(KRX_EXCHANGES | {"US", "NASD", "NYSE", "AMEX", "UPBIT"})
    )

    def exchange_matches(values: tuple[str, ...]):
        return or_(
            risk_exchange.in_(values),
            and_(
                ~risk_exchange.in_(recognized_exchanges),
                venue_exchange.in_(values),
            ),
        )

    crypto_market = or_(
        normalized_asset_class == "crypto",
        exchange_matches(("UPBIT",)),
    )
    us_market = and_(
        ~crypto_market,
        or_(
            normalized_asset_class == "us_equity",
            exchange_matches(("US", "NASD", "NYSE", "AMEX")),
        ),
    )
    krx_symbol = and_(
        func.length(ExecutionPlanItem.symbol) == 6,
        *(
            func.substr(ExecutionPlanItem.symbol, index, 1).between("0", "9")
            for index in range(1, 7)
        ),
    )
    krx_market = and_(
        ~crypto_market,
        ~us_market,
        or_(
            normalized_asset_class.in_(tuple(KRX_ASSET_CLASS_ALIASES)),
            exchange_matches(tuple(KRX_EXCHANGES)),
            krx_symbol,
        ),
    )
    unknown_market = and_(~crypto_market, ~us_market, ~krx_market)
    return or_(
        and_(
            ExecutionPlan.market_date.is_(None),
            ExecutionPlanItem.side == "sell",
        ),
        and_(
            us_market,
            ExecutionPlan.market_date <= us_date,
        ),
        and_(
            crypto_market,
            ExecutionPlan.market_date <= utc_date,
        ),
        and_(
            krx_market,
            ExecutionPlan.market_date <= krx_date,
        ),
        and_(
            unknown_market,
            ExecutionPlan.market_date <= min(krx_date, us_date, utc_date),
        ),
    )


def _sell_plan_authority_payload(plan: ExecutionPlan) -> dict[str, str | None]:
    return {
        "market_date": _serialize_date(plan.market_date),
        "requested_at": (
            _normalize_utc(plan.requested_at).isoformat()
            if plan.requested_at is not None
            else None
        ),
        "accepted_at": (
            _normalize_utc(plan.accepted_at).isoformat()
            if plan.accepted_at is not None
            else None
        ),
        "source_plan_id": str(plan.external_plan_id or plan.id or "") or None,
    }


def _sell_item_authority(
    item: ExecutionPlanItem,
) -> tuple[date, datetime | None, datetime | None]:
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    authority = (
        risk_context.get(SELL_MONITOR_AUTHORITY_KEY)
        if isinstance(risk_context, dict)
        else None
    )
    if isinstance(authority, dict):
        market_date = _parse_date_or_datetime(authority.get("market_date"))
        requested_at = _parse_event_datetime(authority.get("requested_at"))
        accepted_at = _parse_event_datetime(authority.get("accepted_at"))
        if market_date is not None and (requested_at is not None or accepted_at is not None):
            return market_date, requested_at, accepted_at
    plan = item.plan
    return (
        plan.market_date or date.min,
        _normalize_utc(plan.requested_at) if plan.requested_at is not None else None,
        _normalize_utc(plan.accepted_at) if plan.accepted_at is not None else None,
    )


def _compare_sell_authorities(
    candidate: tuple[date, datetime | None, datetime | None],
    current: tuple[date, datetime | None, datetime | None],
) -> int:
    candidate_market_date, candidate_requested_at, candidate_accepted_at = candidate
    current_market_date, current_requested_at, current_accepted_at = current
    if candidate_market_date != current_market_date:
        return 1 if candidate_market_date > current_market_date else -1
    if candidate_requested_at is not None and current_requested_at is not None:
        if candidate_requested_at == current_requested_at:
            return 0
        return 1 if candidate_requested_at > current_requested_at else -1
    if candidate_accepted_at is None or current_accepted_at is None:
        return 0
    if candidate_accepted_at == current_accepted_at:
        return 0
    return 1 if candidate_accepted_at > current_accepted_at else -1


def _compare_sell_plan_to_item_authority(
    candidate: ExecutionPlan,
    current: ExecutionPlanItem,
) -> int:
    return _compare_sell_authorities(
        (
            candidate.market_date or date.min,
            _normalize_utc(candidate.requested_at)
            if candidate.requested_at is not None
            else None,
            _normalize_utc(candidate.accepted_at)
            if candidate.accepted_at is not None
            else None,
        ),
        _sell_item_authority(current),
    )


def _sell_item_is_strictly_newer(
    candidate: ExecutionPlanItem,
    current: ExecutionPlanItem,
) -> bool:
    return _compare_sell_authorities(
        _sell_item_authority(candidate),
        _sell_item_authority(current),
    ) > 0


def _sell_item_is_preferred_authority(
    candidate: ExecutionPlanItem,
    current: ExecutionPlanItem,
) -> bool:
    comparison = _compare_sell_authorities(
        _sell_item_authority(candidate),
        _sell_item_authority(current),
    )
    if comparison != 0:
        return comparison > 0
    candidate_accepted_at = _sell_item_authority(candidate)[2]
    current_accepted_at = _sell_item_authority(current)[2]
    if candidate_accepted_at is not None and current_accepted_at is not None:
        if candidate_accepted_at != current_accepted_at:
            # Equal Core authority means arrival order cannot authorize a
            # replacement. Preserve the first accepted incumbent.
            return candidate_accepted_at < current_accepted_at
    return str(candidate.id) < str(current.id)


def _sell_monitor_material(item: ExecutionPlanItem) -> tuple[object, ...]:
    return (
        item.trigger_price,
        item.limit_price,
        item.quantity_policy,
        item.time_in_force,
        item.decision_agent_name,
        item.decision_snapshot_id,
        item.decision_run_id,
        item.source_market_date,
        item.provider,
        item.venue,
        item.asset_class,
        item.base_quantity,
        item.quote_notional,
        item.quantity_precision,
        item.price_precision,
    )


def _sell_monitor_route_key(
    item: ExecutionPlanItem,
) -> tuple[str, str, str, str] | None:
    provider = str(item.provider or "kis").strip().lower()
    if provider in {"korea-investment", "korea_investment"}:
        provider = "kis"
    exchange = _plan_item_exchange(item)
    asset_class = str(item.asset_class or "stock").strip().lower()
    if provider != "kis":
        binding = execution_account_scope_binding_from_payloads(
            _loaded_risk_context(item, allow_file_backed_load=True),
            item.quantity_policy,
        )
        if binding is None or binding.get("provider") != provider:
            return None
        return provider, exchange, asset_class, str(binding["account_scope"])
    binding = kis_account_route_binding_from_payloads(
        _loaded_risk_context(item, allow_file_backed_load=True),
        item.quantity_policy,
    )
    if binding is None:
        return None
    return (
        provider,
        exchange,
        asset_class,
        binding["configured_route_fingerprint"],
    )


def evaluate_monitoring(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: MonitoringEvaluationRequest,
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    prices_by_symbol = {price.symbol: price for price in command.prices}
    symbol_scope = _ordered_symbol_scope(list(prices_by_symbol))[
        : settings.monitoring_max_symbols_per_cycle
    ]
    return _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=adapter,
        prices=prices_by_symbol,
        symbols=symbol_scope,
        triggered_at=_normalize_utc(now or command.observed_at or datetime.now(UTC)),
        allow_persisted_triggered_without_price=False,
        update_checkpoint_if_idle=True,
    )


def run_autonomous_monitoring_cycle(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    prices: list[MonitoringPriceRequest] | None = None,
    symbols: list[str] | set[str] | None = None,
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    symbol_scope = _ordered_symbol_scope(symbols)
    return _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=adapter,
        prices={price.symbol: price for price in (prices or [])},
        symbols=symbol_scope or None,
        triggered_at=_normalize_utc(now or datetime.now(UTC)),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=False,
    )


def _ordered_symbol_scope(symbols: list[str] | set[str] | None) -> list[str]:
    if not symbols:
        return []
    source = sorted(symbols) if isinstance(symbols, set) else symbols
    ordered: list[str] = []
    seen: set[str] = set()
    for symbol in source:
        if not symbol or symbol in seen:
            continue
        ordered.append(symbol)
        seen.add(symbol)
    return ordered


def _kis_plan_item_provider_filter():
    """Select only rows owned by the KIS monitoring coordinator.

    Null/blank providers are retained for pre-provider legacy plans because
    this module historically treated them as KIS. Explicit foreign providers
    must never be inspected or mutated by the KIS runtime.
    """

    normalized_provider = func.lower(func.trim(ExecutionPlanItem.provider))
    return or_(
        ExecutionPlanItem.provider.is_(None),
        func.trim(ExecutionPlanItem.provider) == "",
        normalized_provider.in_(("kis", "korea-investment", "korea_investment")),
    )


def _kis_us_plan_item_filter():
    us_exchanges = ("US", "NASD", "NYSE", "AMEX")
    return or_(
        func.lower(func.trim(func.coalesce(ExecutionPlanItem.asset_class, "")))
        == "us_equity",
        func.upper(
            func.trim(
                func.coalesce(
                    ExecutionPlanItem.risk_context["exchange"].as_string(),
                    "",
                )
            )
        ).in_(us_exchanges),
        func.upper(func.trim(func.coalesce(ExecutionPlanItem.venue, ""))).in_(
            us_exchanges
        ),
    )


def _active_kis_position_filters(*, settings: Settings) -> tuple[object, ...]:
    """Bind local position authority to this runtime's configured KIS account."""

    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        # A missing stable account identity must behave as no authoritative
        # local position truth, never as permission to consume legacy rows.
        return (ExecutionPosition.id.is_(None),)
    return (
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.provider == identity.provider,
        ExecutionPosition.account_scope == identity.account_scope,
        ExecutionPosition.exchange.in_(MONITORED_KIS_POSITION_EXCHANGES),
    )


def _monitoring_checkpoint_execution_scope(
    *,
    settings: Settings,
) -> dict[str, object] | None:
    """Return the canonical account authority attached to liveness evidence."""

    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return None
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
    }


def _active_kis_order_filters(
    *,
    settings: Settings,
    exchange: str | None = None,
) -> tuple[object, ...]:
    """Require a complete canonical order scope for the configured KIS account."""

    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange=exchange or "KRX",
        )
    except PositionIdentityConflictError:
        return (BrokerOrder.id.is_(None),)
    scope = BrokerOrder.request_metadata["execution_scope"]
    scope_exchange = scope["exchange"].as_string()
    filters: list[object] = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == identity.provider,
        scope["policy_version"].as_integer() == 1,
        scope["provider"].as_string() == identity.provider,
        scope["account_scope"].as_string() == identity.account_scope,
        scope["venue"].as_string() == func.upper(BrokerOrder.venue),
        scope["asset_class"].as_string() == func.lower(BrokerOrder.asset_class),
        or_(
            BrokerOrder.request_metadata["exchange"].as_string().is_(None),
            BrokerOrder.request_metadata["exchange"].as_string()
            == scope_exchange,
        ),
    ]
    if exchange is None:
        filters.append(scope_exchange.in_(MONITORED_KIS_POSITION_EXCHANGES))
    else:
        filters.append(scope_exchange == str(exchange).strip().upper())
    return tuple(filters)


def _active_kis_order_exchange(
    *,
    settings: Settings,
    order: BrokerOrder,
) -> str | None:
    status, exchange = _kis_order_scope_classification(
        settings=settings,
        order=order,
    )
    return exchange if status == "active" else None


def _kis_position_identity_exchange(exchange: object) -> str:
    normalized = str(exchange or "").strip().upper()
    return "KRX" if normalized in KRX_EXCHANGES else normalized


def _kis_order_scope_classification(
    *,
    settings: Settings,
    order: BrokerOrder,
) -> tuple[str, str | None]:
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return "invalid", None
    route_scope = fifo_route_scope_from_order(order)
    if (
        route_scope is None
        or route_scope.provider != identity.provider
        or route_scope.exchange not in MONITORED_KIS_POSITION_EXCHANGES
    ):
        return "invalid", None
    if route_scope.account_scope != identity.account_scope:
        return "foreign", route_scope.exchange
    return "active", route_scope.exchange


def _active_kis_order_matches_position(
    *,
    settings: Settings,
    order: BrokerOrder,
    position: ExecutionPosition | None,
) -> bool:
    if position is None:
        return False
    try:
        position_identity = position_identity_from_columns(position)
        expected_identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange=(
                position_identity.exchange if position_identity is not None else ""
            ),
        )
    except PositionIdentityConflictError:
        return False
    return bool(
        position_identity is not None
        and position_identity == expected_identity
        and _kis_position_identity_exchange(
            _active_kis_order_exchange(settings=settings, order=order)
        )
        == position_identity.exchange
    )


def _unique_positions_by_symbol(
    positions: list[ExecutionPosition],
) -> dict[str, ExecutionPosition]:
    positions_by_symbol: dict[str, ExecutionPosition] = {}
    for position in positions:
        if position.symbol in positions_by_symbol:
            raise PositionIdentityConflictError(
                "active KIS execution position route is ambiguous for symbol "
                f"{position.symbol}"
            )
        positions_by_symbol[position.symbol] = position
    return positions_by_symbol


def _position_for_monitoring_item(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    positions_by_symbol: dict[str, ExecutionPosition],
) -> ExecutionPosition | None:
    position = positions_by_symbol.get(item.symbol)
    if position is None:
        return None
    if _position_monitoring_is_disabled(position):
        return None
    symbol = str(item.symbol or "").strip().upper()
    provider = str(
        item.provider or ("kis" if _is_krx_equity_symbol(symbol) else "")
    ).strip().lower()
    if provider in {"korea-investment", "korea_investment"}:
        provider = "kis"
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    raw_exchange = risk_context.get("exchange") if isinstance(risk_context, dict) else None
    exchange = str(raw_exchange or "").strip().upper()
    if not exchange and _is_krx_equity_symbol(symbol):
        exchange = "KRX"
    if not provider or not exchange:
        return None
    if provider == "kis" and _kis_monitoring_route_rejection(item=item, settings=settings):
        return None
    identity_exchange = (
        _kis_position_identity_exchange(exchange) if provider == "kis" else exchange
    )
    try:
        expected = build_position_identity(
            settings=settings,
            provider=provider,
            exchange=identity_exchange,
        )
        actual = position_identity_from_columns(position)
    except PositionIdentityConflictError:
        return None
    return position if actual == expected else None


def _kis_monitoring_route_rejection(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> tuple[str, str | None] | None:
    provider = str(item.provider or "kis").strip().lower()
    if provider in {"korea-investment", "korea_investment"}:
        provider = "kis"
    if provider != "kis":
        return None
    mismatch = validate_kis_account_route(
        settings=settings,
        provider=provider,
        risk_context=_loaded_risk_context(item, allow_file_backed_load=True),
        quantity_policy=item.quantity_policy,
        require_binding=True,
    )
    if mismatch is not None:
        return mismatch.reason_code, mismatch.provided_route_fingerprint
    if (
        configured_kis_route_fingerprint(settings) is not None
        and kis_account_route_binding_from_payloads(
            _loaded_risk_context(item, allow_file_backed_load=True),
            item.quantity_policy,
        )
        is None
    ):
        return "account_route_binding_missing", None
    return None


def _quarantine_invalid_sell_monitor_routes(
    *,
    settings: Settings,
    items: list[ExecutionPlanItem],
    recorded_at: datetime,
    positions_by_symbol: dict[str, ExecutionPosition] | None = None,
) -> list[ExecutionPlanItem]:
    eligible: list[ExecutionPlanItem] = []
    for item in items:
        if item.side != "sell" or item.intent not in SELL_MONITORED_INTENTS:
            eligible.append(item)
            continue
        provider = str(item.provider or "kis").strip().lower()
        if provider in {"korea-investment", "korea_investment"}:
            provider = "kis"
        if provider != "kis":
            # This coordinator owns only KIS. Leave another provider's
            # protective state untouched for its dedicated runtime.
            continue
        if provider == "kis" and classify_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context=_loaded_risk_context(
                item,
                allow_file_backed_load=True,
            ),
            quantity_policy=item.quantity_policy,
        ) == "foreign":
            # Another configured account owns this still-live protection.
            # Ignore it in the current account without destroying its status.
            continue
        rejection = _kis_monitoring_route_rejection(settings=settings, item=item)
        if (
            rejection is None
            and positions_by_symbol is not None
            and item.symbol in positions_by_symbol
        ):
            if (
                _position_for_monitoring_item(
                    settings=settings,
                    item=item,
                    positions_by_symbol=positions_by_symbol,
                )
                is None
            ):
                rejection = ("position_route_mismatch", None)
        if rejection is None:
            eligible.append(item)
            continue
        reason, provided_route_fingerprint = rejection
        prior_status = item.status
        item.status = "manual_review_required"
        _record_monitoring_event(
            item=item,
            event={
                "event": "sell_monitor_account_route_quarantined",
                "reason": reason,
                "prior_status": prior_status,
                "provided_route_fingerprint": provided_route_fingerprint,
                "recorded_at": _normalize_utc(recorded_at).isoformat(),
            },
        )
    return eligible


def _is_krx_equity_symbol(symbol: object) -> bool:
    normalized = str(symbol or "").strip()
    return len(normalized) == 6 and normalized.isdigit()


def list_autonomous_monitoring_symbols(
    *,
    settings: Settings,
    db: Session,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
    desired_symbol_priority: list[str] | None = None,
    include_kis_us_symbols: bool = True,
) -> list[str]:
    resolved_effective_at = _normalize_utc(effective_at or datetime.now(UTC))
    resolved_market_date = current_market_date or resolved_effective_at.astimezone(
        KRX_TIMEZONE
    ).date()
    effective_plan_filter = _effective_monitor_plan_filter(
        effective_at=(
            resolved_effective_at
            if effective_at is not None or current_market_date is None
            else None
        ),
        current_market_date=current_market_date,
    )
    symbol_cycle_limit = resolve_monitoring_symbol_cycle_limit(settings=settings)
    changed_watch_count = 0
    max_age_trading_sessions = _watch_buy_max_age_trading_sessions(settings)
    if max_age_trading_sessions is not None:
        expired_watch_count = expire_prior_market_date_unfilled_watch_buys(
            settings=settings,
            db=db,
            current_market_date=resolved_market_date,
            effective_at=(
                resolved_effective_at
                if effective_at is not None or current_market_date is None
                else None
            ),
            max_age_trading_sessions=max_age_trading_sessions,
        )
        changed_watch_count += expired_watch_count
        if expired_watch_count:
            db.flush()
    changed_watch_count += supersede_duplicate_active_watch_buys(
        settings=settings,
        db=db,
        current_market_date=resolved_market_date,
        effective_at=(
            resolved_effective_at
            if effective_at is not None or current_market_date is None
            else None
        ),
    )
    if changed_watch_count:
        db.commit()

    mode_plan_ids = select(ExecutionPlan.id).where(ExecutionPlan.mode == settings.runtime_mode)
    kis_us_symbols: set[str] = set()
    if include_kis_us_symbols:
        kis_us_symbols = {
            item.symbol
            for item in db.scalars(
                select(ExecutionPlanItem)
                .join(ExecutionPlanItem.plan)
                .where(
                    ExecutionPlan.mode == settings.runtime_mode,
                    _kis_plan_item_provider_filter(),
                    _kis_us_plan_item_filter(),
                )
            ).all()
            if _plan_item_exchange(item) in {"NASD", "NYSE", "AMEX"}
        }
    held_positions = db.scalars(
        select(ExecutionPosition)
        .where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.quantity > 0,
        )
        .order_by(ExecutionPosition.symbol.asc())
    ).all()
    monitoring_disabled_held_symbols = {
        position.symbol
        for position in held_positions
        if _position_monitoring_is_disabled(position)
    }
    for position in held_positions:
        raw_payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
        sync_context = raw_payload.get("_position_sync")
        if (
            isinstance(sync_context, dict)
            and sync_context.get("asset_class") == "us_equity"
            and sync_context.get("monitoring_eligible") is True
            and str(sync_context.get("exchange") or "").strip().upper()
            in {"NASD", "NYSE", "AMEX"}
        ):
            kis_us_symbols.add(position.symbol)
    held_symbols = {
        position.symbol
        for position in held_positions
        if position.symbol not in monitoring_disabled_held_symbols
        and (
            _is_krx_equity_symbol(position.symbol)
            or position.symbol in kis_us_symbols
        )
    }
    fresh_position_truth_after = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    fresh_known_flat_symbols = select(ExecutionPosition.symbol).where(
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity <= 0,
        ExecutionPosition.last_reconciled_at.is_not(None),
        ExecutionPosition.last_reconciled_at >= fresh_position_truth_after,
    )
    symbol_candidates: list[str] = []

    def append_candidates(rows: list[str], *, enforce_limit: bool = True) -> None:
        for symbol in rows:
            if symbol in monitoring_disabled_held_symbols:
                continue
            if not _is_krx_equity_symbol(symbol) and symbol not in kis_us_symbols:
                continue
            if enforce_limit and len(symbol_candidates) >= symbol_cycle_limit:
                return
            if symbol and symbol not in symbol_candidates:
                symbol_candidates.append(symbol)
            if enforce_limit and len(symbol_candidates) >= symbol_cycle_limit:
                return

    if held_symbols:
        held_symbol_rows = db.execute(
            select(
                ExecutionPlanItem.symbol,
                func.min(ExecutionPlan.accepted_at).label("first_accepted_at"),
                func.min(ExecutionPlan.id).label("first_plan_id"),
            )
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.symbol.in_(sorted(held_symbols)),
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(LOADABLE_SELL_MONITORING_STATUSES)),
            )
            .group_by(ExecutionPlanItem.symbol)
            .order_by(
                func.min(ExecutionPlan.accepted_at).asc(),
                func.min(ExecutionPlan.id).asc(),
                ExecutionPlanItem.symbol.asc(),
            )
        ).all()
        append_candidates([str(row.symbol) for row in held_symbol_rows], enforce_limit=False)
        append_candidates(
            sorted(symbol for symbol in held_symbols if symbol not in symbol_candidates),
            enforce_limit=False,
        )

    active_watch_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            effective_plan_filter,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.status,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
            ),
        )
        .order_by(ExecutionPlanItem.symbol.asc(), ExecutionPlanItem.id.asc())
    ).all()
    if effective_at is not None or current_market_date is None:
        active_watch_items = [
            item
            for item in active_watch_items
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=resolved_effective_at,
            )
        ]
    active_watch_items = [
        item
        for item in active_watch_items
        if not is_exec_agent_sizing_policy(item.quantity_policy)
        and classify_kis_account_route(
            settings=settings,
            provider=str(item.provider or "kis").strip().lower(),
            risk_context=_loaded_risk_context(
                item,
                allow_file_backed_load=True,
            ),
            quantity_policy=item.quantity_policy,
        )
        == "active"
    ]
    desired_symbols = (
        _load_core_desired_monitoring_symbols(settings=settings)
        if desired_symbol_priority is None
        else list(desired_symbol_priority)
    )
    desired_rank = {symbol: index for index, symbol in enumerate(desired_symbols)}
    base_ordered_watch_items = sorted(
        active_watch_items,
        key=lambda item: (
            desired_rank.get(str(item.symbol), len(desired_rank)),
            str(item.symbol),
            str(item.id),
        ),
    )
    domestic_positions = [
        index
        for index, item in enumerate(base_ordered_watch_items)
        if _plan_item_market_timezone(item) == KRX_TIMEZONE
    ]
    domestic_watch_items = sorted(
        (base_ordered_watch_items[index] for index in domestic_positions),
        key=lambda item: _watch_buy_queue_priority_key(
            settings=settings,
            item=item,
            evaluated_at=resolved_effective_at,
            desired_rank=desired_rank.get(str(item.symbol), len(desired_rank)),
        ),
    )
    ordered_watch_items = list(base_ordered_watch_items)
    for index, item in zip(domestic_positions, domestic_watch_items, strict=True):
        ordered_watch_items[index] = item
    buy_symbol_rows: list[str] = []
    for item in ordered_watch_items:
        symbol = str(item.symbol)
        if symbol not in buy_symbol_rows:
            buy_symbol_rows.append(symbol)
    # Every persisted active watch item is executable work. Keep the full
    # bounded set here and let the runtime round-robin scheduler batch it;
    # truncating to one quote batch starves the remaining active items.
    append_candidates(buy_symbol_rows, enforce_limit=False)

    if len(symbol_candidates) < symbol_cycle_limit:
        pending_sell_symbols = list(
            db.scalars(
                select(distinct(ExecutionPlanItem.symbol))
                    .where(
                        ExecutionPlanItem.execution_plan_id.in_(mode_plan_ids),
                        ExecutionPlanItem.side == "sell",
                        ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                        ExecutionPlanItem.status.in_(tuple(SELL_PENDING_CONFIG_STATUSES)),
                        ExecutionPlanItem.symbol.not_in(fresh_known_flat_symbols),
                    )
                .order_by(ExecutionPlanItem.symbol.asc())
                .limit(symbol_cycle_limit * 4)
            ).all()
        )
        flat_symbols = _symbols_with_recent_flat_position_truth(
            settings=settings,
            db=db,
            symbols=pending_sell_symbols,
        ) if settings.runtime_mode == "live" else set()
        unexited_buy_order_symbols = _symbols_with_unexited_buy_orders(
            settings=settings,
            db=db,
            symbols=pending_sell_symbols,
        )
        fallback_symbols: list[str] = []
        for symbol in pending_sell_symbols:
            if symbol in symbol_candidates:
                continue
            if symbol in flat_symbols:
                continue
            if symbol in unexited_buy_order_symbols:
                fallback_symbols.append(symbol)
            if len(symbol_candidates) + len(fallback_symbols) >= symbol_cycle_limit:
                break
        append_candidates(fallback_symbols)

    return symbol_candidates


def resolve_monitoring_symbol_cycle_limit(*, settings: Settings) -> int:
    configured_limit = int(settings.monitoring_max_symbols_per_cycle or 0)
    if configured_limit > 0:
        return configured_limit
    request_timeout = max(float(settings.kis_request_timeout_seconds), 1.0)
    target_seconds = max(float(settings.monitoring_auto_symbol_batch_target_seconds), request_timeout)
    quote_workers = 3
    estimated_limit = int((target_seconds / request_timeout) * quote_workers)
    return max(3, min(20, estimated_limit))


def _watch_buy_queue_priority_key(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    evaluated_at: datetime,
    desired_rank: int,
) -> tuple[object, ...]:
    trigger_rank = _watch_buy_queue_trigger_rank(item)
    turnover, liquidity_observed_at = _watch_buy_queue_verified_turnover(
        settings=settings,
        item=item,
        evaluated_at=evaluated_at,
    )
    if turnover is None:
        liquidity_rank = 1
    elif turnover >= SHORT_TERM_LIVE_MIN_TURNOVER_KRW:
        liquidity_rank = 0
    else:
        liquidity_rank = 2

    net_reward_risk, economics_observed_at = _watch_buy_queue_full_cost_rr(
        settings=settings,
        item=item,
        evaluated_at=evaluated_at,
    )
    required_reward_risk = _minimum_reward_risk_for_watch_buy(
        settings=settings,
        item=item,
    )
    if net_reward_risk is None:
        reward_risk_rank = 1
    elif net_reward_risk >= required_reward_risk:
        reward_risk_rank = 0
    else:
        reward_risk_rank = 2

    evidence_times = [
        observed_at
        for observed_at in (liquidity_observed_at, economics_observed_at)
        if observed_at is not None
    ]
    oldest_evidence_age = max(
        (
            max(
                0.0,
                (_normalize_utc(evaluated_at) - observed_at).total_seconds(),
            )
            for observed_at in evidence_times
        ),
        default=float("inf"),
    )
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    return (
        trigger_rank,
        liquidity_rank,
        reward_risk_rank,
        _watch_buy_queue_alignment_rank(risk_context),
        oldest_evidence_age,
        _watch_buy_queue_warning_count(risk_context),
        -(turnover or Decimal("0")),
        -(net_reward_risk or Decimal("0")),
        desired_rank,
        str(item.symbol),
        str(item.id),
    )


def _watch_buy_queue_trigger_rank(item: ExecutionPlanItem) -> int:
    if item.status in {
        "watch_buy_triggered_pending_dispatch",
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    }:
        return 0
    if item.status in {
        "watch_buy_triggered",
        "watch_buy_triggered_blocked",
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
    } or _persisted_watch_buy_trigger_cross(item) is not None:
        return 1
    if item.status == "watch_monitoring_active":
        return 2
    return 3


def _watch_buy_queue_verified_turnover(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    evaluated_at: datetime,
) -> tuple[Decimal | None, datetime | None]:
    policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    evidence = policy.get(TRIGGER_TIME_LIQUIDITY_EVIDENCE_KEY)
    if not isinstance(evidence, dict) or evidence.get("version") != 1:
        return None, None
    if (
        str(evidence.get("provider") or "").strip().lower() != "kis"
        or str(evidence.get("asset_class") or "").strip().lower() != "stock"
        or str(evidence.get("exchange") or "").strip().upper()
        != _plan_item_exchange(item).strip().upper()
        or str(evidence.get("symbol") or "").strip().upper()
        != str(item.symbol).strip().upper()
        or evidence.get("freshness_source") != "kis_exchange_intraday_minute_time"
        or evidence.get("endpoint")
        != "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        or evidence.get("tr_id") != "FHKST03010200"
    ):
        return None, None
    observed_at = _strict_queue_evidence_datetime(evidence.get("observed_at"))
    if not _watch_queue_evidence_is_fresh(
        settings=settings,
        observed_at=observed_at,
        evaluated_at=evaluated_at,
    ):
        return None, None
    turnover = _parse_decimal(evidence.get("turnover"))
    volume = _parse_decimal(evidence.get("volume"))
    day_high = _parse_decimal(evidence.get("day_high"))
    day_low = _parse_decimal(evidence.get("day_low"))
    if (
        turnover is None
        or turnover < 0
        or volume is None
        or volume < 0
        or day_high is None
        or day_high <= 0
        or day_low is None
        or day_low <= 0
        or day_low > day_high
    ):
        return None, None
    return turnover, observed_at


def _watch_buy_queue_full_cost_rr(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    evaluated_at: datetime,
) -> tuple[Decimal | None, datetime | None]:
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    evidence = risk_context.get(TRIGGER_TIME_FULL_COST_ECONOMICS_KEY)
    if not isinstance(evidence, dict) or evidence.get("version") != 1:
        return None, None
    evidence_symbol = str(evidence.get("symbol") or "").strip().upper()
    if evidence_symbol != str(item.symbol).strip().upper():
        return None, None
    observed_at = _strict_queue_evidence_datetime(evidence.get("evaluated_at"))
    if not _watch_queue_evidence_is_fresh(
        settings=settings,
        observed_at=observed_at,
        evaluated_at=evaluated_at,
    ):
        return None, None
    if not _trigger_time_full_cost_economics_provenance_is_valid(
        settings=settings,
        item=item,
        evidence=evidence,
        evaluated_at=evaluated_at,
    ):
        return None, None
    net_reward_risk = _parse_decimal(evidence.get("net_reward_risk"))
    minimum_reward_risk = _parse_decimal(evidence.get("minimum_reward_risk"))
    round_trip_cost_bps = _parse_decimal(
        evidence.get("estimated_round_trip_cost_bps")
    )
    assessment_reason = evidence.get("assessment_reason")
    required_minimum_reward_risk = max(
        minimum_reward_risk or Decimal("0"),
        _minimum_reward_risk_for_watch_buy(settings=settings, item=item),
    )
    if (
        net_reward_risk is None
        or net_reward_risk < 0
        or minimum_reward_risk is None
        or minimum_reward_risk
        < _minimum_reward_risk_for_watch_buy(settings=settings, item=item)
        or net_reward_risk < required_minimum_reward_risk
        or round_trip_cost_bps is None
        or round_trip_cost_bps < 0
        or not str(evidence.get("cost_evidence_source") or "").strip()
        or (
            assessment_reason is not None
            and str(assessment_reason).strip()
        )
    ):
        return None, None
    return net_reward_risk, observed_at


def _trigger_time_full_cost_economics_provenance_is_valid(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    evidence: dict[str, object],
    evaluated_at: datetime,
) -> bool:
    provenance = evidence.get("provenance")
    if not isinstance(provenance, dict):
        return False
    if set(provenance) != {
        "version",
        "source",
        "route_fingerprint",
        "liquidity_evidence_hash",
        "material_hash",
        "binding",
    }:
        return False
    if (
        provenance.get("version")
        != TRIGGER_TIME_FULL_COST_ECONOMICS_PROVENANCE_VERSION
        or provenance.get("source") != "kai-trading-exec.monitoring"
    ):
        return False
    route_fingerprint = configured_kis_route_fingerprint(settings)
    if not isinstance(route_fingerprint, str) or not hmac.compare_digest(
        str(provenance.get("route_fingerprint") or ""),
        route_fingerprint,
    ):
        return False
    key = stable_account_scope_hmac_key(settings)
    if key is None:
        return False
    verified_turnover, _ = _watch_buy_queue_verified_turnover(
        settings=settings,
        item=item,
        evaluated_at=evaluated_at,
    )
    if verified_turnover is None:
        return False
    liquidity_evidence = _trigger_time_liquidity_evidence_payload(item)
    if liquidity_evidence is None:
        return False
    liquidity_hash = json_payload_hash(
        _trigger_time_liquidity_binding_material(liquidity_evidence)
    )
    if not hmac.compare_digest(
        str(provenance.get("liquidity_evidence_hash") or ""),
        liquidity_hash,
    ):
        return False
    material = _trigger_time_full_cost_economics_binding_material(
        item=item,
        evidence=evidence,
        route_fingerprint=route_fingerprint,
        liquidity_evidence_hash=liquidity_hash,
    )
    material_hash = json_payload_hash(material)
    if not hmac.compare_digest(
        str(provenance.get("material_hash") or ""),
        material_hash,
    ):
        return False
    expected_binding = hmac.new(
        key,
        material_hash.encode("utf-8"),
        sha256,
    ).hexdigest()
    binding = provenance.get("binding")
    return isinstance(binding, str) and hmac.compare_digest(
        binding,
        expected_binding,
    )


def _trigger_time_full_cost_economics_provenance(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    evidence: dict[str, object],
) -> dict[str, object] | None:
    route_fingerprint = configured_kis_route_fingerprint(settings)
    key = stable_account_scope_hmac_key(settings)
    liquidity_evidence = _trigger_time_liquidity_evidence_payload(item)
    if (
        not isinstance(route_fingerprint, str)
        or key is None
        or liquidity_evidence is None
    ):
        return None
    liquidity_hash = json_payload_hash(
        _trigger_time_liquidity_binding_material(liquidity_evidence)
    )
    material = _trigger_time_full_cost_economics_binding_material(
        item=item,
        evidence=evidence,
        route_fingerprint=route_fingerprint,
        liquidity_evidence_hash=liquidity_hash,
    )
    material_hash = json_payload_hash(material)
    return {
        "version": TRIGGER_TIME_FULL_COST_ECONOMICS_PROVENANCE_VERSION,
        "source": "kai-trading-exec.monitoring",
        "route_fingerprint": route_fingerprint,
        "liquidity_evidence_hash": liquidity_hash,
        "material_hash": material_hash,
        "binding": hmac.new(
            key,
            material_hash.encode("utf-8"),
            sha256,
        ).hexdigest(),
    }


def _trigger_time_full_cost_economics_binding_material(
    *,
    item: ExecutionPlanItem,
    evidence: dict[str, object],
    route_fingerprint: str,
    liquidity_evidence_hash: str,
) -> dict[str, object]:
    return {
        "version": TRIGGER_TIME_FULL_COST_ECONOMICS_PROVENANCE_VERSION,
        "source": "kai-trading-exec.monitoring",
        "execution_plan_id": str(item.execution_plan_id or ""),
        "execution_plan_item_id": str(item.id or ""),
        "plan_item_index": item.plan_item_index,
        "provider": str(item.provider or "kis").strip().lower(),
        "venue": str(item.venue or "").strip().upper(),
        "asset_class": str(item.asset_class or "stock").strip().lower(),
        "exchange": _plan_item_exchange(item).strip().upper(),
        "symbol": str(item.symbol or "").strip().upper(),
        "intent": str(item.intent or "").strip(),
        "side": str(item.side or "").strip().lower(),
        "route_fingerprint": route_fingerprint,
        "liquidity_evidence_hash": liquidity_evidence_hash,
        "evaluated_at": str(evidence.get("evaluated_at") or ""),
        "gross_reward_risk": str(evidence.get("gross_reward_risk") or ""),
        "net_reward_risk": str(evidence.get("net_reward_risk") or ""),
        "minimum_reward_risk": str(evidence.get("minimum_reward_risk") or ""),
        "estimated_round_trip_cost_bps": str(
            evidence.get("estimated_round_trip_cost_bps") or ""
        ),
        "cost_evidence_source": str(evidence.get("cost_evidence_source") or ""),
        "assessment_reason": (
            None
            if evidence.get("assessment_reason") is None
            else str(evidence.get("assessment_reason"))
        ),
    }


def _trigger_time_liquidity_evidence_payload(
    item: ExecutionPlanItem,
) -> dict[str, object] | None:
    policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    evidence = policy.get(TRIGGER_TIME_LIQUIDITY_EVIDENCE_KEY)
    return evidence if isinstance(evidence, dict) else None


def _trigger_time_liquidity_binding_material(
    evidence: dict[str, object],
) -> dict[str, object]:
    return {
        "version": evidence.get("version"),
        "provider": str(evidence.get("provider") or "").strip().lower(),
        "asset_class": str(evidence.get("asset_class") or "").strip().lower(),
        "exchange": str(evidence.get("exchange") or "").strip().upper(),
        "symbol": str(evidence.get("symbol") or "").strip().upper(),
        "observed_at": str(evidence.get("observed_at") or ""),
        "freshness_source": str(evidence.get("freshness_source") or ""),
        "endpoint": str(evidence.get("endpoint") or ""),
        "tr_id": str(evidence.get("tr_id") or ""),
        "volume": str(evidence.get("volume") or ""),
        "turnover": str(evidence.get("turnover") or ""),
        "day_high": str(evidence.get("day_high") or ""),
        "day_low": str(evidence.get("day_low") or ""),
    }


def _watch_queue_evidence_is_fresh(
    *,
    settings: Settings,
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> bool:
    if observed_at is None:
        return False
    evaluated_at = _normalize_utc(evaluated_at)
    if observed_at.astimezone(KRX_TIMEZONE).date() != evaluated_at.astimezone(
        KRX_TIMEZONE
    ).date():
        return False
    age = evaluated_at - observed_at
    return (
        age <= timedelta(seconds=settings.monitoring_max_quote_age_seconds)
        and age
        >= -timedelta(seconds=settings.monitoring_max_quote_future_skew_seconds)
    )


def _strict_queue_evidence_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _watch_buy_queue_alignment_rank(risk_context: dict[str, object]) -> int:
    gate = risk_context.get("alignment_gate")
    gate = gate if isinstance(gate, dict) else {}
    values = [
        risk_context.get("signal_decider_alignment"),
        risk_context.get("alignment_gate_action"),
        gate.get("signal_decider_alignment"),
        gate.get("alignment_gate_action"),
        gate.get("signal_decider_action"),
    ]
    normalized = " ".join(
        str(value).strip().lower()
        for value in values
        if isinstance(value, str) and value.strip()
    )
    if any(marker in normalized for marker in ("conflict", "block", "reject", "wait")):
        return 2
    if any(marker in normalized for marker in ("buy_aligned", "aligned_buy", "confirm", "support")):
        return 0
    return 1


def _watch_buy_queue_warning_count(risk_context: dict[str, object]) -> int:
    warnings = risk_context.get("data_quality_warnings")
    count = len(warnings) if isinstance(warnings, list) else 0
    gate = risk_context.get("alignment_gate")
    if isinstance(gate, dict):
        signal_context = gate.get("signal_decider_context")
        nested = (
            signal_context.get("data_quality_warnings")
            if isinstance(signal_context, dict)
            else None
        )
        if isinstance(nested, list):
            count += len(nested)
    return count


def _load_core_desired_monitoring_symbols(*, settings: Settings) -> list[str]:
    env_priority = os.getenv("KAI_EXEC_MONITORING_SYMBOL_PRIORITY", "")
    if env_priority.strip():
        return _parse_symbol_priority(env_priority)
    if not settings.market_context_enabled or not settings.market_context_base_url:
        return []
    base_url = str(settings.market_context_base_url).rstrip("/")
    # Core owns the daily read model. Exec only consumes the cached handoff here;
    # forcing a rebuild on every workset refresh turns a priority hint into a
    # cross-service DB polling loop.
    request = Request(f"{base_url}/execution/runtime/monitored-symbols?refresh=false")
    if settings.market_context_bearer_token:
        request.add_header("Authorization", f"Bearer {settings.market_context_bearer_token}")
    try:
        with urlopen(request, timeout=min(float(settings.market_context_timeout_seconds), 3.0)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Core monitored-symbol response must be a JSON object")
        realtime_subscription = payload.get("realtime_subscription")
        if not isinstance(realtime_subscription, dict):
            raise ValueError("Core monitored-symbol response is missing realtime_subscription")
        desired_symbols = realtime_subscription.get("desired_symbols")
        if not isinstance(desired_symbols, list):
            raise ValueError("Core monitored-symbol response is missing desired_symbols")
        symbols = _parse_symbol_priority(",".join(str(symbol) for symbol in desired_symbols))
    except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
        cached = _cached_core_desired_monitoring_symbols(settings=settings, cache_key=base_url)
        _log_core_desired_monitoring_symbol_failure(
            settings=settings,
            cache_key=base_url,
            error=exc,
            fallback_symbols=cached,
        )
        return cached
    _CORE_DESIRED_MONITORING_SYMBOL_CACHE[base_url] = (time.monotonic(), tuple(symbols))
    return symbols


def _cached_core_desired_monitoring_symbols(*, settings: Settings, cache_key: str) -> list[str]:
    cached = _CORE_DESIRED_MONITORING_SYMBOL_CACHE.get(cache_key)
    if cached is None:
        return []
    cached_at, symbols = cached
    ttl_seconds = max(float(settings.market_context_cache_ttl_seconds), 1.0)
    if time.monotonic() - cached_at > ttl_seconds:
        _CORE_DESIRED_MONITORING_SYMBOL_CACHE.pop(cache_key, None)
        return []
    return list(symbols)


def _log_core_desired_monitoring_symbol_failure(
    *,
    settings: Settings,
    cache_key: str,
    error: Exception,
    fallback_symbols: list[str],
) -> None:
    now = time.monotonic()
    warning_interval = max(float(settings.market_context_cache_ttl_seconds), 30.0)
    last_warning_at = _CORE_DESIRED_MONITORING_SYMBOL_WARNING_AT.get(cache_key)
    if last_warning_at is not None and now - last_warning_at < warning_interval:
        return
    _CORE_DESIRED_MONITORING_SYMBOL_WARNING_AT[cache_key] = now
    logger.warning(
        "core_desired_monitoring_symbols_unavailable",
        extra={
            "core_base_url": cache_key,
            "error_type": error.__class__.__name__,
            "fallback_symbol_count": len(fallback_symbols),
            "fallback_cache_hit": bool(fallback_symbols),
        },
    )


def _parse_symbol_priority(value: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for symbol in value.split(","):
        normalized = str(symbol).strip()
        if not normalized or normalized in seen:
            continue
        ordered.append(normalized)
        seen.add(normalized)
    return ordered


def expire_known_flat_pending_sell_monitors(
    *,
    settings: Settings,
    db: Session,
    expired_at: datetime | None = None,
) -> int:
    expired_at = _normalize_utc(expired_at or datetime.now(UTC))
    effective_plan_filter = _effective_monitor_plan_filter(
        effective_at=expired_at,
        current_market_date=None,
    )
    held_symbols = select(ExecutionPosition.symbol).where(
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity > 0,
    )
    known_flat_symbols = select(ExecutionPosition.symbol).where(
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity <= 0,
    )
    active_watch_symbols = (
        select(ExecutionPlanItem.symbol)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(LIVE_WATCH_ENTRY_STATUSES)),
            effective_plan_filter,
        )
    )
    where = [
        ExecutionPlan.mode == settings.runtime_mode,
        ExecutionPlanItem.side == "sell",
        ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
        ExecutionPlanItem.status.in_(tuple(SELL_STALE_FLAT_EXPIRABLE_STATUSES)),
        ExecutionPlanItem.symbol.in_(known_flat_symbols),
        ExecutionPlanItem.symbol.not_in(held_symbols),
        ExecutionPlanItem.symbol.not_in(active_watch_symbols),
        effective_plan_filter,
    ]
    where.append(~_unexited_buy_sell_monitor_fallback_filter(settings=settings))
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(*where)
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()

    candidates = [
        item
        for item in candidates
        if _monitoring_item_is_effective_at(item=item, effective_at=expired_at)
    ]
    candidates = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=candidates,
        recorded_at=expired_at,
    )

    for item in candidates:
        prior_status = item.status
        item.status = "expired"
        _record_monitoring_event(
            item=item,
            event={
                "event": "known_flat_sell_monitor_expired",
                "reason": "known_flat_position",
                "prior_status": prior_status,
                "recorded_at": expired_at.isoformat(),
            },
        )
    return len(candidates)


def _evaluate_monitoring_cycle(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    prices: dict[str, MonitoringPriceRequest],
    symbols: list[str] | set[str] | None = None,
    triggered_at: datetime,
    allow_persisted_triggered_without_price: bool,
    update_checkpoint_if_idle: bool,
) -> EvaluateMonitoringResult:
    cycle_started = time.monotonic()
    checkpoint_execution_scope = _monitoring_checkpoint_execution_scope(
        settings=settings,
    )
    checkpoint_scope_status = (
        "available" if checkpoint_execution_scope is not None else "unavailable"
    )
    checkpoint_scope_reason = (
        None
        if checkpoint_execution_scope is not None
        else "stable_account_scope_unavailable"
    )

    def _log_cycle_step(step: str, **extra: object) -> None:
        logger.info(
            "monitoring_cycle_step",
            extra={
                "step": step,
                "elapsed_ms": round((time.monotonic() - cycle_started) * 1000, 2),
                **extra,
            },
        )

    skip_pre_cycle_broker_reconciliation = (
        allow_persisted_triggered_without_price and bool(symbols)
    )
    _log_cycle_step(
        "drawdown_release",
        released_drawdown_blocks=0,
        skip_pre_cycle_broker_reconciliation=skip_pre_cycle_broker_reconciliation,
        symbol_count=len(symbols or []),
        price_count=len(prices),
    )
    position_truth_refreshed_before_selection = False
    if settings.runtime_mode == "live" and not skip_pre_cycle_broker_reconciliation:
        _refresh_position_truth_if_stale(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
        position_truth_refreshed_before_selection = True
    _log_cycle_step("position_truth_before_selection")
    items = _load_monitored_items(
        settings=settings,
        db=db,
        exclude_pending_config=allow_persisted_triggered_without_price,
        symbols=symbols,
        current_market_date=triggered_at.astimezone(KRX_TIMEZONE).date(),
        effective_at=triggered_at,
    )
    _log_cycle_step("load_monitored_items", item_count=len(items))
    rearmed_protective_stop_item_ids = _reactivate_retryable_protective_stops(
        settings=settings,
        items=items,
        db=db,
        triggered_at=triggered_at,
    )
    db.flush()
    sell_monitored_symbols = {
        item.symbol
        for item in items
        if not _is_watch_buy_item(item)
    }
    pending_broker_order_symbols = _pending_broker_order_symbols(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    broker_reconciliation_symbols = sell_monitored_symbols | pending_broker_order_symbols
    if not skip_pre_cycle_broker_reconciliation:
        _reconcile_before_monitoring(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=broker_reconciliation_symbols,
            refresh_positions=not position_truth_refreshed_before_selection,
        )
    _log_cycle_step(
        "reconcile_before_monitoring",
        sell_symbol_count=len(sell_monitored_symbols),
        pending_broker_order_symbol_count=len(pending_broker_order_symbols),
        broker_reconciliation_symbol_count=len(broker_reconciliation_symbols),
    )
    if not position_truth_refreshed_before_selection and not skip_pre_cycle_broker_reconciliation:
        _refresh_position_truth_if_stale(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=broker_reconciliation_symbols,
        )
    _log_cycle_step("position_truth_after_selection")
    positions_by_symbol = _load_positions_by_symbol(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    _log_cycle_step("load_positions", position_count=len(positions_by_symbol))
    if skip_pre_cycle_broker_reconciliation:
        post_entry_maintenance_count = 0
        post_entry_review_count = 0
    else:
        post_entry_maintenance_count, post_entry_review_count = _maintain_post_entry_exit_monitors(
            settings=settings,
            db=db,
            positions_by_symbol=positions_by_symbol,
            prices=prices,
            triggered_at=triggered_at,
        )
    _log_cycle_step(
        "post_entry_maintenance",
        post_entry_maintenance_count=post_entry_maintenance_count,
        post_entry_review_count=post_entry_review_count,
    )
    if post_entry_maintenance_count:
        db.flush()
        items = _load_monitored_items(
            settings=settings,
            db=db,
            exclude_pending_config=allow_persisted_triggered_without_price,
            symbols=symbols,
            current_market_date=triggered_at.astimezone(KRX_TIMEZONE).date(),
            effective_at=triggered_at,
        )
    if skip_pre_cycle_broker_reconciliation:
        coverage_issues = []
        repaired_stop_coverage_count = 0
    else:
        coverage_issues, repaired_stop_coverage_count = _repair_unprotected_stop_coverage(
            settings=settings,
            db=db,
            positions_by_symbol=positions_by_symbol,
            symbols=symbols,
            triggered_at=triggered_at,
        )
    profit_target_coverage_issues = _profit_target_coverage_issues(
        settings=settings,
        db=db,
        positions_by_symbol=positions_by_symbol,
        evaluated_at=triggered_at,
    )
    _log_cycle_step(
        "stop_coverage",
        coverage_issue_count=len(coverage_issues),
        repaired_stop_coverage_count=repaired_stop_coverage_count,
    )
    _log_cycle_step(
        "profit_target_coverage",
        coverage_issue_count=len(profit_target_coverage_issues),
    )
    if repaired_stop_coverage_count:
        db.flush()
        items = _load_monitored_items(
            settings=settings,
            db=db,
            exclude_pending_config=allow_persisted_triggered_without_price,
            symbols=symbols,
            current_market_date=triggered_at.astimezone(KRX_TIMEZONE).date(),
            effective_at=triggered_at,
        )
        items = _prefer_same_cycle_cloned_stop_repairs(
            items=items,
            triggered_at=triggered_at,
        )
    remaining_quantities_by_symbol = {
        symbol: position.quantity
        for symbol, position in positions_by_symbol.items()
        if position.quantity > 0
    }
    if allow_persisted_triggered_without_price:
        items = _prepare_due_max_holding_items(
            settings=settings,
            db=db,
            items=items,
            positions_by_symbol=positions_by_symbol,
            triggered_at=triggered_at,
        )
        items = _filter_sell_items_to_held_symbols(
            settings=settings,
            items=items,
            positions_by_symbol=positions_by_symbol,
        )
    monitored_symbols = {item.symbol for item in items}
    _reserve_existing_exit_orders(
        settings=settings,
        db=db,
        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
        positions_by_symbol=positions_by_symbol,
        rearmed_plan_item_ids=rearmed_protective_stop_item_ids,
    )
    primary_triggered_symbols = _primary_triggered_sell_symbols(
        settings=settings,
        items=items,
        prices=prices,
        triggered_at=triggered_at,
    )
    result_by_item_id: dict[str, dict[str, object]] = {}
    evaluated_item_ids: list[str] = []
    actions_by_symbol: dict[str, deque[PendingMonitoringAction]] = {}
    symbol_order: list[str] = []
    evaluated_symbols: set[str] = set()
    triggered_symbols: set[str] = set()
    submitted_symbols: set[str] = set()
    retried_triggered_symbols: set[str] = set()
    triggered_item_count = 0
    submitted_order_count = 0
    blocked_item_count = 0
    deferred_item_count = 0
    dispatch_action_count = 0
    retried_triggered_item_count = 0
    rearmed_sell_monitor_count = 0
    rearmed_sell_symbols: set[str] = set()
    watch_buy_cash_reservation: WatchBuyCashReservation | str | None = None
    drawdown_recovery_sources_by_scope: dict[str, BrokerOrder | None] = {}

    for item in items:
        if item.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue
        if _is_watch_buy_item(item) and _position_monitoring_is_disabled(
            positions_by_symbol.get(item.symbol)
        ):
            continue

        price = prices.get(item.symbol)
        observed_price = _observed_price_for_item(price=price, item=item)
        retry_persisted_trigger = (
            allow_persisted_triggered_without_price
            and price is None
            and not _is_watch_buy_item(item)
            and item.status in (
                BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES
            )
        )
        if price is None and not retry_persisted_trigger and item.intent != "max_holding_close_sell":
            continue

        item_position = _position_for_monitoring_item(
            settings=settings,
            item=item,
            positions_by_symbol=positions_by_symbol,
        )
        if (
            not allow_persisted_triggered_without_price
            and not _is_watch_buy_item(item)
            and item_position is None
            and _monitoring_item_is_configured(item)
        ):
            continue

        evaluated_item_ids.append(item.id)
        evaluated_symbols.add(item.symbol)
        if not _monitoring_item_is_configured(item):
            item.status = _pending_configuration_status(item.intent)
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="invalid_configuration",
                message=(
                    "watch buy monitoring item is missing buy-side trigger or quantity configuration"
                    if _is_watch_buy_item(item)
                    else "target/stop monitoring item is missing sell-side trigger configuration"
                ),
            )
            continue

        retry_blocked_until = _watch_buy_retry_blocked_until(
            settings=settings,
            item=item,
        )
        if retry_blocked_until is not None and triggered_at < retry_blocked_until:
            blocked_item_count += 1
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="blocked",
                block_reason="retry_cooldown",
                message=(
                    "watch buy retry cooldown is active after "
                    "account_snapshot_unavailable"
                ),
            )
            continue

        agent_delay_until = _trigger_time_agent_delay_until(
            settings=settings,
            item=item,
        )
        if agent_delay_until is not None and triggered_at < agent_delay_until:
            blocked_item_count += 1
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="agent_delay",
                block_reason="trigger_time_exec_agent_delay",
                message="trigger-time exec-agent requested delayed reevaluation",
            )
            continue

        agent_recovery_reason = _trigger_time_agent_recovery_reason(
            settings=settings,
            item=item,
            now=triggered_at,
            delay_until=agent_delay_until,
        )
        if agent_recovery_reason is not None:
            previous_status = item.status
            item.status = _monitoring_status(item.intent)
            _record_monitoring_event(
                item=item,
                event={
                    "event": (
                        "trigger_time_exec_agent_pending_recovered"
                        if previous_status == AGENT_PENDING_STATUS
                        else "trigger_time_exec_agent_state_recovered"
                    ),
                    "reason": agent_recovery_reason,
                    "message": (
                        "expired trigger-time exec-agent state was returned to monitoring "
                        "for a fresh decision"
                    ),
                    "previous_status": previous_status,
                    "recorded_at": triggered_at.isoformat(),
                },
            )

        if item.status in AGENT_ACTIVE_STATUSES:
            blocked_item_count += 1
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="agent_pending",
                block_reason="trigger_time_exec_agent_active",
                message="trigger-time exec-agent decision is already pending for this item",
            )
            continue

        if item.intent in PRICE_TRIGGERED_MONITORED_INTENTS and not retry_persisted_trigger:
            quote_stale_reason = _quote_stale_reason(
                settings=settings,
                price=price,
                cycle_at=triggered_at,
            )
            quote_trust_reason = _live_automated_quote_trust_reason(
                settings=settings,
                item=item,
                price=price,
            )
            quote_rejection_reason = quote_stale_reason or quote_trust_reason
            if quote_rejection_reason is not None:
                blocked_item_count += 1
                persisted_profit_protection = (
                    _persisted_profit_protection_trigger(item)
                    if item.status
                    in (
                        BLOCKED_MONITORING_STATUSES
                        | PENDING_DISPATCH_MONITORING_STATUSES
                    )
                    else None
                )
                monitoring_status = _monitoring_status(item.intent)
                if (
                    item.status
                    not in (
                        BLOCKED_MONITORING_STATUSES
                        | PENDING_DISPATCH_MONITORING_STATUSES
                    )
                    and item.status != monitoring_status
                ):
                    item.status = monitoring_status
                rejection_event: dict[str, object] = {
                    "event": (
                        "watch_buy_quote_rejected"
                        if _is_watch_buy_item(item)
                        else "sell_quote_rejected"
                    ),
                    "reason": (
                        "stale_quote"
                        if quote_stale_reason is not None
                        else "untrusted_quote_provenance"
                    ),
                    "message": quote_rejection_reason,
                    "observed_price": _serialize_decimal(observed_price),
                    "observed_at": _serialize_datetime(
                        _provided_price_observed_at(price)
                    ),
                    "recorded_at": triggered_at.isoformat(),
                }
                if persisted_profit_protection is not None:
                    rejection_event["persisted_profit_protection_trigger"] = {
                        "reason": persisted_profit_protection.reason,
                        "metadata": persisted_profit_protection.metadata,
                    }
                _record_monitoring_event(item=item, event=rejection_event)
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome=(
                        "stale_quote"
                        if quote_stale_reason is not None
                        else "untrusted_quote_provenance"
                    ),
                    message=quote_rejection_reason,
                )
                continue

        drawdown_recovery = None
        if _is_watch_buy_item(item):
            drawdown_recovery = inspect_account_drawdown_recovery(
                settings=settings,
                db=db,
                adapter=adapter,
                provider=item.provider or "kis",
                asset_class=item.asset_class or "stock",
                exchange=_plan_item_exchange(item),
                idempotency_key=f"monitoring:watch_buy:{item.id}",
                execution_plan_item_id=item.id,
                symbol=item.symbol,
                now=triggered_at,
                scope_source_cache=drawdown_recovery_sources_by_scope,
            )

        trigger_reason: str | None = None
        trigger_metadata: dict[str, object] | None = None
        if retry_persisted_trigger:
            persisted_profit_protection = _persisted_profit_protection_trigger(item)
            if persisted_profit_protection is not None:
                trigger_reason = persisted_profit_protection.reason
                trigger_metadata = persisted_profit_protection.metadata
            else:
                trigger_reason = item.intent
        elif _is_watch_buy_item(item):
            trigger_reason, trigger_metadata = _watch_buy_trigger_reason(
                item=item,
                price=price,
                observed_price=observed_price,
                triggered_at=triggered_at,
                allow_persisted_trigger=not (
                    drawdown_recovery is not None and drawdown_recovery.active
                ),
            )
        elif _is_triggered(
            item=item,
            observed_price=observed_price,
            triggered_at=triggered_at,
        ):
            trigger_reason = item.intent
        elif not _is_watch_buy_item(item) and item.symbol not in primary_triggered_symbols:
            profit_protection = _evaluate_profit_protection(
                settings=settings,
                item=item,
                position=item_position,
                observed_price=observed_price,
                triggered_at=triggered_at,
            )
            if profit_protection is not None:
                trigger_reason = profit_protection.reason
                trigger_metadata = profit_protection.metadata

        if _is_watch_buy_item(item) and observed_price is not None:
            chase_limit_price = _watch_buy_chase_limit_price(item)
            if chase_limit_price is not None and observed_price > chase_limit_price:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                blocked_item_count += 1
                item.status = "watch_monitoring_active"
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "watch_buy_pullback_wait",
                        "reason": "price_above_chase_limit",
                        "message": "observed price is above chase_limit_price; keep monitoring for pullback",
                        "trigger_price": _serialize_decimal(_watch_buy_trigger_price(item)),
                        "chase_limit_price": _serialize_decimal(chase_limit_price),
                        "observed_price": _serialize_decimal(observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="blocked",
                    block_reason="price_above_chase_limit",
                    message="observed price is above chase_limit_price; monitoring remains active",
                    trigger_reason=trigger_reason,
                    trigger_metadata=trigger_metadata,
                )
                continue

        if trigger_reason is None:
            previous_status = item.status
            rearmable_sell_status = (
                item.intent in SELL_MONITORED_INTENTS
                and previous_status
                in (
                    SELL_TRIGGERED_BLOCKED_STATUSES
                    | SELL_TRIGGERED_PENDING_DISPATCH_STATUSES
                )
            )
            if (
                previous_status
                in (BLOCKED_MONITORING_STATUSES | PENDING_DISPATCH_MONITORING_STATUSES)
                and previous_status != "watch_buy_triggered_blocked"
            ):
                item.status = _monitoring_status(item.intent)
            if rearmable_sell_status and item.status != previous_status:
                rearmed_sell_monitor_count += 1
                rearmed_sell_symbols.add(item.symbol)
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": "sell_monitor_rearmed",
                        "reason": "trigger_condition_cleared",
                        "previous_status": previous_status,
                        "observed_price": _serialize_decimal(observed_price),
                        "trigger_price": _serialize_decimal(item.trigger_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
            waiting_result = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="waiting",
                message="trigger condition not met",
            )
            if rearmable_sell_status and item.status != previous_status:
                waiting_result["state_transition"] = "sell_monitor_rearmed"
                waiting_result["previous_status"] = previous_status
            result_by_item_id[item.id] = waiting_result
            continue

        if _is_watch_buy_item(item) and observed_price is not None:
            risk_gate = _watch_buy_high_risk_gate_rejection(
                settings=settings,
                item=item,
                observed_price=observed_price,
                price=price,
                trigger_reason=trigger_reason,
                trigger_metadata=trigger_metadata,
                triggered_at=triggered_at,
            )
            if risk_gate is not None:
                triggered_item_count += 1
                triggered_symbols.add(item.symbol)
                blocked_item_count += 1
                recovery_source_must_remain_retryable = bool(
                    drawdown_recovery is not None and drawdown_recovery.active
                )
                retryable_pullback_gate = _is_retryable_watch_buy_pullback_gate(
                    risk_gate["reason"]
                )
                retryable_gate = (
                    retryable_pullback_gate or recovery_source_must_remain_retryable
                )
                item.status = (
                    "watch_buy_triggered_blocked"
                    if recovery_source_must_remain_retryable
                    else "watch_monitoring_active"
                    if retryable_pullback_gate
                    else "watch_buy_rejected"
                )
                _record_monitoring_event(
                    item=item,
                    event={
                        "event": (
                            "watch_buy_triggered_blocked"
                            if recovery_source_must_remain_retryable
                            else "watch_buy_pullback_wait"
                            if retryable_pullback_gate
                            else "watch_buy_rejected"
                        ),
                        "reason": risk_gate["reason"],
                        "message": risk_gate["message"],
                        "details": risk_gate["details"],
                        "observed_price": _serialize_decimal(observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    },
                )
                result_by_item_id[item.id] = _monitoring_result_payload(
                    item=item,
                    observed_price=observed_price,
                    outcome="blocked" if retryable_gate else "rejected",
                    block_reason=risk_gate["reason"],
                    message=risk_gate["message"],
                    risk_decision=risk_gate["details"],
                )
                continue

        if (
            drawdown_recovery is not None
            and drawdown_recovery.block_reason is not None
        ):
            blocked_item_count += 1
            current_plan_state = _current_buy_plan_item_state(
                db=db,
                item_id=item.id,
                lock=True,
            )
            preserve_current_plan_status = bool(
                current_plan_state is None
                or current_plan_state[0]
                in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
            )
            if current_plan_state is not None:
                db.refresh(item, attribute_names=["status", "risk_context"])
            if not preserve_current_plan_status:
                item.status = "watch_buy_triggered_blocked"
                event: dict[str, object] = {
                    "event": "watch_buy_triggered_blocked",
                    "reason": drawdown_recovery.block_reason,
                    "message": drawdown_recovery.message,
                    "source_broker_order_pk": drawdown_recovery.source_broker_order_pk,
                    "observed_price": _serialize_decimal(observed_price),
                    "recorded_at": triggered_at.isoformat(),
                }
                if drawdown_recovery.retry_after_at is not None:
                    event["retry_after_at"] = (
                        drawdown_recovery.retry_after_at.isoformat()
                    )
                _record_monitoring_event(item=item, event=event)
            result_by_item_id[item.id] = _monitoring_result_payload(
                item=item,
                observed_price=observed_price,
                outcome="blocked",
                block_reason=drawdown_recovery.block_reason,
                message=drawdown_recovery.message,
                trigger_metadata={
                    "source_broker_order_pk": drawdown_recovery.source_broker_order_pk,
                    "retry_after_at": (
                        drawdown_recovery.retry_after_at.isoformat()
                        if drawdown_recovery.retry_after_at is not None
                        else None
                    ),
                },
            )
            continue

        if retry_persisted_trigger:
            retried_triggered_item_count += 1
            retried_triggered_symbols.add(item.symbol)

        triggered_item_count += 1
        triggered_symbols.add(item.symbol)
        if item.symbol not in actions_by_symbol:
            actions_by_symbol[item.symbol] = deque()
            symbol_order.append(item.symbol)
        actions_by_symbol[item.symbol].append(
            PendingMonitoringAction(
                item=item,
                observed_price=observed_price,
                price=price,
                observed_at=_price_observed_at(price, triggered_at) if price is not None else None,
                market_quality_context=_market_quality_context_from_price(
                    price,
                    item=item,
                ),
                trigger_reason=trigger_reason,
                trigger_metadata=trigger_metadata,
            )
        )

    max_broker_actions = settings.monitoring_max_broker_actions_per_cycle
    _prioritize_sell_actions_by_symbol(actions_by_symbol)
    pending_symbols = deque(
        _prioritized_monitoring_symbol_order(
            symbol_order=symbol_order,
            actions_by_symbol=actions_by_symbol,
        )
    )
    if max_broker_actions <= 0:
        backlog_symbols: set[str] = set()
        for symbol, symbol_actions in actions_by_symbol.items():
            while symbol_actions:
                action = symbol_actions.popleft()
                backlog_symbols.add(symbol)
                deferred_item_count += 1
                action.item.status = _pending_dispatch_status(action.item.intent)
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="deferred",
                    message="broker action capacity is disabled for this monitoring cycle",
                )
        results = [
            result_by_item_id[item_id]
            for item_id in evaluated_item_ids
            if item_id in result_by_item_id
        ]
        stop_actionability_warnings = _protective_stop_actionability_warnings(
            items=items,
            prices=prices,
            results=results,
        )
        db.flush()
        agent_status_counts = _trigger_time_agent_status_counts(items=items)
        checkpoint_details = {
            "topology": MONITORING_RUNTIME_TOPOLOGY,
            "selection_strategy": MONITORING_SELECTION_STRATEGY,
            "execution_scope": checkpoint_execution_scope,
            "execution_scope_status": checkpoint_scope_status,
            "execution_scope_reason": checkpoint_scope_reason,
            "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
            "max_broker_actions_per_cycle": max_broker_actions,
            "dispatch_action_count": 0,
            "evaluated_item_count": len(results),
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": 0,
            "blocked_item_count": blocked_item_count,
            "deferred_item_count": deferred_item_count,
            "price_count": len(prices),
            "evaluated_symbol_count": len(evaluated_symbols),
            "triggered_symbol_count": len(triggered_symbols),
            "retried_triggered_item_count": retried_triggered_item_count,
            "retried_triggered_symbols": sorted(retried_triggered_symbols),
            "rearmed_sell_monitor_count": rearmed_sell_monitor_count,
            "rearmed_sell_symbols": sorted(rearmed_sell_symbols),
            "submitted_symbols": [],
            "backlog_item_count": deferred_item_count,
            "backlog_symbols": sorted(backlog_symbols),
            "agent_count_scope": "selected_cycle_items",
            "agent_count_item_count": len(items),
            "agent_watching_count": agent_status_counts["watching"],
            "agent_pending_decision_count": agent_status_counts["pending_agent_decision"],
            "agent_delayed_count": agent_status_counts["agent_delayed"],
            "agent_rejected_count": agent_status_counts["agent_rejected"],
            "agent_approved_submitted_count": agent_status_counts["agent_approved_submitted"],
            "post_entry_review_count": post_entry_review_count,
            "protective_stop_coverage": _protective_stop_coverage_payload(
                issues=coverage_issues,
                repaired_count=repaired_stop_coverage_count,
                actionability_warnings=stop_actionability_warnings,
            ),
            "protective_stop_actionability": _protective_stop_actionability_payload(
                warnings=stop_actionability_warnings,
            ),
            "profit_target_coverage": _profit_target_coverage_payload(
                issues=profit_target_coverage_issues,
            ),
        }
        checkpoint = _update_monitoring_checkpoint(
            settings=settings,
            db=db,
            worker_name=MONITORING_WORKER_NAME,
            triggered_at=triggered_at,
            status="degraded" if deferred_item_count > 0 else "running",
            details=checkpoint_details,
        )
        _log_cycle_step("broker_actions_disabled", deferred_item_count=deferred_item_count)
        return EvaluateMonitoringResult(
            status_code=200,
            payload={
                "triggered_at": triggered_at.isoformat(),
                "worker_name": (
                    checkpoint.worker_name
                    if checkpoint is not None
                    else MONITORING_WORKER_NAME
                ),
                "evaluated_item_count": len(results),
                "triggered_item_count": triggered_item_count,
                "submitted_order_count": 0,
                "blocked_item_count": blocked_item_count,
                "deferred_item_count": deferred_item_count,
                "evaluated_symbol_count": checkpoint_details["evaluated_symbol_count"],
                "triggered_symbol_count": checkpoint_details["triggered_symbol_count"],
                "rearmed_sell_monitor_count": checkpoint_details[
                    "rearmed_sell_monitor_count"
                ],
                "rearmed_sell_symbols": checkpoint_details["rearmed_sell_symbols"],
                "submitted_symbols": [],
                "protective_stop_coverage": checkpoint_details["protective_stop_coverage"],
                "protective_stop_actionability": checkpoint_details["protective_stop_actionability"],
                "profit_target_coverage": checkpoint_details["profit_target_coverage"],
                "items": results,
            },
        )
    while pending_symbols:
        pending_symbols = deque(
            _prioritized_monitoring_symbol_order(
                symbol_order=list(pending_symbols),
                actions_by_symbol=actions_by_symbol,
            )
        )
        symbol = pending_symbols.popleft()
        symbol_actions = actions_by_symbol.get(symbol)
        if not symbol_actions:
            continue

        action = symbol_actions.popleft()
        if action.item.status in AGENT_ACTIVE_STATUSES:
            blocked_item_count += 1
            result_by_item_id[action.item.id] = _monitoring_result_payload(
                item=action.item,
                observed_price=action.observed_price,
                outcome="agent_pending",
                block_reason="trigger_time_exec_agent_active",
                message="trigger-time exec-agent decision is already pending for this item",
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            )
            if symbol_actions:
                pending_symbols.append(symbol)
            continue
        is_watch_buy = _is_watch_buy_item(action.item)
        deterministic_sell_quantity_cap: int | None = None
        limit_price_override: Decimal | None = None
        if is_watch_buy:
            action.item.status = "watch_buy_triggered"
            trigger_time_agent_quantity: int | None = None
            control_state = ensure_runtime_control_state(settings=settings, db=db)
            entry_block_reason = automated_buy_submission_block_reason(control_state)
            if entry_block_reason is not None:
                approval = WatchBuyOrderApproval(
                    quantity=None,
                    reason=entry_block_reason,
                    message=(
                        "automated buy submission is blocked by runtime safety controls"
                    ),
                )
            else:
                trigger_time_agent_enabled = (
                    trigger_time_exec_agent.trigger_time_exec_agent_enabled(
                        settings=settings,
                        item=action.item,
                    )
                )
                if (
                    trigger_time_agent_enabled
                    and not _watch_buy_entry_line_bypasses_agent(action)
                ):
                    if _watch_buy_trigger_cross_pullback_bypasses_agent(action):
                        agent_result = _resolve_trigger_time_quality_gate_delay(
                            settings=settings,
                            db=db,
                            adapter=adapter,
                            action=action,
                            triggered_at=triggered_at,
                            cycle_started=cycle_started,
                        )
                    else:
                        agent_result = _resolve_trigger_time_agent_action(
                            settings=settings,
                            db=db,
                            adapter=adapter,
                            action=action,
                            triggered_at=triggered_at,
                            cycle_started=cycle_started,
                            intended_quantity=None,
                            remaining_quantity=None,
                        )
                    if agent_result is not None:
                        if agent_result.refreshed_price is not None:
                            action = _action_with_refreshed_price(
                                action=action,
                                price=agent_result.refreshed_price,
                            )
                            prices[action.item.symbol] = agent_result.refreshed_price
                        if agent_result.quantity is None:
                            blocked_item_count += 1
                            result_by_item_id[action.item.id] = agent_result.result_payload
                            if symbol_actions:
                                pending_symbols.append(symbol)
                            continue
                        trigger_time_agent_quantity = agent_result.quantity
                        limit_price_override = agent_result.limit_price
                approval = _resolve_watch_buy_order(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    item=action.item,
                    observed_price=action.observed_price,
                    price=prices.get(action.item.symbol),
                    execution_price_override=limit_price_override,
                    requested_quantity=trigger_time_agent_quantity,
                    trigger_time_agent_approved=trigger_time_agent_quantity is not None,
                    sizing_source=(
                        "trigger_time_exec_agent_approval"
                        if trigger_time_agent_quantity is not None
                        else None
                    ),
                )
            quantity = approval.quantity
            if quantity is None:
                blocked_item_count += 1
                if approval.reason in (
                    BLOCKED_WATCH_BUY_APPROVAL_REASONS | RETRYABLE_WATCH_BUY_APPROVAL_REASONS
                ):
                    action.item.status = "watch_buy_triggered_blocked"
                    event: dict[str, object] = {
                        "event": "watch_buy_triggered_blocked",
                        "reason": approval.reason,
                        "message": approval.message,
                        "observed_price": _serialize_decimal(action.observed_price),
                        "recorded_at": triggered_at.isoformat(),
                    }
                    if approval.details:
                        event["details"] = approval.details
                    if approval.reason in RETRYABLE_WATCH_BUY_APPROVAL_REASONS:
                        event["retry_after_at"] = (
                            triggered_at
                            + timedelta(
                                seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                            )
                        ).isoformat()
                    _record_monitoring_event(
                        item=action.item,
                        event=event,
                    )
                    result_by_item_id[action.item.id] = _monitoring_result_payload(
                        item=action.item,
                        observed_price=action.observed_price,
                        outcome="blocked",
                        block_reason=approval.reason,
                        message=approval.message,
                        trigger_metadata=approval.details,
                    )
                else:
                    action.item.status = "watch_buy_rejected"
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_rejected",
                            "reason": approval.reason or "risk_rejected",
                            "message": approval.message,
                            "details": approval.details or {},
                            "observed_price": _serialize_decimal(action.observed_price),
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                    result_by_item_id[action.item.id] = _monitoring_result_payload(
                        item=action.item,
                        observed_price=action.observed_price,
                        outcome="rejected",
                        message=approval.message,
                    )
                if trigger_time_agent_quantity is not None:
                    _record_trigger_time_watch_buy_sizing_result(
                        item=action.item,
                        approval=approval,
                        requested_quantity=trigger_time_agent_quantity,
                        recorded_at=triggered_at,
                    )
                if symbol_actions:
                    pending_symbols.append(symbol)
                continue
            if trigger_time_agent_quantity is not None:
                _record_trigger_time_watch_buy_sizing_result(
                    item=action.item,
                    approval=approval,
                    requested_quantity=trigger_time_agent_quantity,
                    recorded_at=triggered_at,
                )
        else:
            quantity = _resolve_sell_quantity(
                item=action.item,
                remaining_quantity=remaining_quantities_by_symbol.get(action.item.symbol),
            )
            if quantity is None:
                reconciliation_error = _latest_reconciliation_error_for_symbol(
                    settings=settings,
                    db=db,
                    symbol=action.item.symbol,
                    exchange=_plan_item_exchange(action.item),
                )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome=(
                        "reconciliation_blocked"
                        if reconciliation_error is not None
                        else "no_position"
                    ),
                    message=(
                        "sellable position is unknown because broker reconciliation failed: "
                        f"{reconciliation_error}"
                        if reconciliation_error is not None
                        else "no sellable execution position is available for this trigger"
                    ),
                )
                if symbol_actions:
                    pending_symbols.append(symbol)
                continue
            deterministic_sell_quantity_cap = quantity
            if trigger_time_exec_agent.trigger_time_exec_agent_enabled(
                settings=settings,
                item=action.item,
            ):
                agent_result = _resolve_trigger_time_agent_action(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    action=action,
                    triggered_at=triggered_at,
                    cycle_started=cycle_started,
                    intended_quantity=quantity,
                    remaining_quantity=remaining_quantities_by_symbol.get(action.item.symbol),
                )
                if agent_result.refreshed_price is not None:
                    action = _action_with_refreshed_price(
                        action=action,
                        price=agent_result.refreshed_price,
                    )
                    prices[action.item.symbol] = agent_result.refreshed_price
                if agent_result.quantity is None:
                    blocked_item_count += 1
                    result_by_item_id[action.item.id] = agent_result.result_payload
                    if symbol_actions:
                        pending_symbols.append(symbol)
                    continue
                quantity = agent_result.quantity
                limit_price_override = agent_result.limit_price

        if dispatch_action_count >= max_broker_actions:
            symbol_actions.appendleft(action)
            pending_symbols.appendleft(symbol)
            break
        else:
            before_success_commit: Callable[[], None] | None = None
            if not is_watch_buy:
                before_success_commit = partial(
                    _reserve_and_rearm_sell_after_success,
                    settings=settings,
                    item=action.item,
                    position=_position_for_monitoring_item(
                        settings=settings,
                        item=action.item,
                        positions_by_symbol=positions_by_symbol,
                    ),
                    remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                    submitted_quantity=quantity,
                    recorded_at=triggered_at,
                )
            else:
                control_state = ensure_runtime_control_state(settings=settings, db=db)
                if automated_buy_submission_allowed(control_state):
                    if watch_buy_cash_reservation is None:
                        watch_buy_cash_reservation = _load_watch_buy_cash_reservation(
                            settings=settings,
                            db=db,
                            adapter=adapter,
                            triggered_at=triggered_at,
                        )
                    if isinstance(watch_buy_cash_reservation, str):
                        blocked_item_count += 1
                        action.item.status = "watch_buy_triggered_blocked"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_triggered_blocked",
                                "reason": "account_snapshot_unavailable",
                                "message": watch_buy_cash_reservation,
                                "observed_price": _serialize_decimal(action.observed_price),
                                "recorded_at": triggered_at.isoformat(),
                                "retry_after_at": (
                                    triggered_at
                                    + timedelta(seconds=settings.monitoring_watch_buy_retry_cooldown_seconds)
                                ).isoformat(),
                            },
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="blocked",
                            block_reason="account_snapshot_unavailable",
                            message=watch_buy_cash_reservation,
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    limit_price = limit_price_override or _watch_buy_limit_price(
                        item=action.item,
                        observed_price=action.observed_price,
                    )
                    candidate_notional = _watch_buy_order_notional(
                        quantity=quantity,
                        limit_price=limit_price,
                    )
                    if candidate_notional is None:
                        blocked_item_count += 1
                        action.item.status = "watch_buy_rejected"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_rejected",
                                "reason": RESERVED_CASH_EXHAUSTED_REASON,
                                "message": "watch buy requires limit-price notional for reserved cash guard",
                                "recorded_at": triggered_at.isoformat(),
                            },
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="rejected",
                            block_reason=RESERVED_CASH_EXHAUSTED_REASON,
                            message="watch buy requires limit-price notional for reserved cash guard",
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    reservation_payload = _watch_buy_cash_reservation_payload(
                        reservation=watch_buy_cash_reservation,
                        candidate_notional=candidate_notional,
                        item=action.item,
                        quantity=quantity,
                        limit_price=limit_price,
                        triggered_at=triggered_at,
                    )
                    if candidate_notional > watch_buy_cash_reservation.remaining_cash:
                        blocked_item_count += 1
                        action.item.status = "watch_buy_rejected"
                        _record_reserved_cash_exhausted(
                            db=db,
                            item=action.item,
                            payload=reservation_payload,
                            triggered_at=triggered_at,
                        )
                        result_by_item_id[action.item.id] = _monitoring_result_payload(
                            item=action.item,
                            observed_price=action.observed_price,
                            outcome="rejected",
                            block_reason=RESERVED_CASH_EXHAUSTED_REASON,
                            message="watch buy reserved cash budget exhausted before broker submission",
                            trigger_metadata=reservation_payload,
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    pre_submit_at = triggered_at + timedelta(
                        seconds=max(0.0, time.monotonic() - cycle_started)
                    )
                    pre_submit_price = prices.get(action.item.symbol)
                    pre_submit_quote_reason = (
                        _quote_stale_reason(
                            settings=settings,
                            price=pre_submit_price,
                            cycle_at=pre_submit_at,
                        )
                        if settings.runtime_mode == "live"
                        else None
                    )
                    pre_submit_quote_reason = pre_submit_quote_reason or (
                        _live_automated_quote_trust_reason(
                            settings=settings,
                            item=action.item,
                            price=pre_submit_price,
                        )
                    )
                    if pre_submit_quote_reason is not None:
                        blocked_item_count += 1
                        action.item.status = "watch_buy_triggered_blocked"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_triggered_blocked",
                                "reason": "pre_submit_quote_stale",
                                "message": pre_submit_quote_reason,
                                "observed_price": _serialize_decimal(
                                    action.observed_price
                                ),
                                "observed_at": _serialize_datetime(
                                    _provided_price_observed_at(pre_submit_price)
                                ),
                                "retry_after_at": (
                                    pre_submit_at
                                    + timedelta(
                                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                                    )
                                ).isoformat(),
                                "recorded_at": pre_submit_at.isoformat(),
                            },
                        )
                        result_by_item_id[action.item.id] = (
                            _monitoring_result_payload(
                                item=action.item,
                                observed_price=action.observed_price,
                                outcome="blocked",
                                block_reason="pre_submit_quote_stale",
                                message=pre_submit_quote_reason,
                            )
                        )
                        if symbol_actions:
                            pending_symbols.append(symbol)
                        continue
                    watch_buy_cash_reservation.already_reserved_notional += candidate_notional

            dispatch_action_count += 1
            placement = _place_monitoring_order(
                settings=settings,
                db=db,
                adapter=adapter,
                item=action.item,
                quantity=quantity,
                observed_price=action.observed_price,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
                triggered_at=triggered_at,
                freshness_reference_at=(
                    triggered_at
                    + timedelta(seconds=max(0.0, time.monotonic() - cycle_started))
                ),
                limit_price_override=limit_price_override,
                before_success_commit=before_success_commit,
                deterministic_sell_quantity_cap=deterministic_sell_quantity_cap,
            )
            if placement.status_code == 201:
                if is_watch_buy:
                    action.item.status = "watch_buy_submitted"
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_submitted",
                            "observed_price": _serialize_decimal(action.observed_price),
                            "quantity": quantity,
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                submitted_order_count += 1
                submitted_symbols.add(action.item.symbol)
                trigger_time_exec_agent.attach_order_to_last_trigger_time_interaction(
                    item=action.item,
                    order=placement.payload.get("order") if isinstance(placement.payload.get("order"), dict) else None,
                    new_status=action.item.status,
                    recorded_at=triggered_at,
                )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="submitted",
                    order=placement.payload.get("order"),
                    trigger_reason=action.trigger_reason,
                    trigger_metadata=action.trigger_metadata,
                )
            elif placement.status_code == 202 and placement.payload.get("deferred") is True:
                deferred_item_count += 1
                action.item.status = _pending_dispatch_status(action.item.intent)
                deferred_event: dict[str, object] = {
                    "event": "monitoring_order_deferred",
                    "reason": placement.payload.get("message"),
                    "route": placement.payload.get("route"),
                    "observed_price": _serialize_decimal(action.observed_price),
                    "recorded_at": triggered_at.isoformat(),
                }
                if (
                    action.trigger_reason in PROFIT_PROTECTION_TRIGGER_REASONS
                    and isinstance(action.trigger_metadata, dict)
                ):
                    deferred_event["persisted_profit_protection_trigger"] = {
                        "reason": action.trigger_reason,
                        "metadata": dict(action.trigger_metadata),
                    }
                _record_monitoring_event(item=action.item, event=deferred_event)
                db.commit()
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="deferred",
                    message=placement.payload.get("message"),
                )
            elif placement.status_code == 409 and "block_reason" in placement.payload:
                blocked_item_count += 1
                preserve_current_plan_status = (
                    _placement_preserves_current_plan_status(placement.payload)
                )
                hourly_risk_off_blocked = (
                    placement.payload.get("block_reason") == "hourly_plan_risk_off"
                )
                if hourly_risk_off_blocked:
                    db.refresh(action.item, attribute_names=["status", "risk_context"])
                    if action.item.status != "watch_expired":
                        action.item.status = (
                            "watch_buy_triggered_blocked"
                            if is_watch_buy
                            else f"{action.item.intent}_triggered_blocked"
                        )
                elif preserve_current_plan_status:
                    db.refresh(action.item, attribute_names=["status", "risk_context"])
                else:
                    action.item.status = (
                        "watch_buy_triggered_blocked"
                        if is_watch_buy
                        else f"{action.item.intent}_triggered_blocked"
                    )
                if is_watch_buy and not preserve_current_plan_status:
                    _record_monitoring_event(
                        item=action.item,
                        event={
                            "event": "watch_buy_triggered_blocked",
                            "reason": placement.payload.get("block_reason"),
                            "message": placement.payload.get("message"),
                            "retry_after_at": placement.payload.get("retry_after_at"),
                            "observed_price": _serialize_decimal(action.observed_price),
                            "recorded_at": triggered_at.isoformat(),
                        },
                    )
                db.commit()
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="blocked",
                    block_reason=placement.payload.get("block_reason"),
                    message=placement.payload.get("message"),
                )
            elif placement.payload.get("duplicate") is True:
                dispatch_action_count -= 1
                duplicate_quantity = _serialized_order_quantity(placement.payload.get("order"))
                if is_watch_buy:
                    duplicate_status = _serialized_order_status(
                        placement.payload.get("order")
                    )
                    if duplicate_status in WATCH_BUY_CONFIRMED_ORDER_STATUSES:
                        action.item.status = "watch_buy_submitted"
                    else:
                        broker_truth_status = _plan_item_status_from_order_payload(
                            placement.payload.get("order")
                        )
                        if broker_truth_status is not None:
                            action.item.status = broker_truth_status
                else:
                    duplicate_status = _serialized_order_status(
                        placement.payload.get("order")
                    )
                    if (
                        duplicate_status in CAPITAL_RESERVING_ORDER_STATUSES
                        or duplicate_status
                        == NormalizedBrokerOrderStatus.FILLED.value
                    ):
                        broker_truth_status = _plan_item_status_from_order_payload(
                            placement.payload.get("order")
                        )
                        if broker_truth_status is not None:
                            action.item.status = broker_truth_status
                        if duplicate_quantity is not None:
                            _reserve_remaining_quantity(
                                remaining_quantities_by_symbol=(
                                    remaining_quantities_by_symbol
                                ),
                                symbol=action.item.symbol,
                                quantity=duplicate_quantity,
                            )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="duplicate",
                    order=placement.payload.get("order"),
                    message=placement.payload.get("message"),
                )
            else:
                if is_watch_buy:
                    broker_truth_status = _plan_item_status_from_order_payload(
                        placement.payload.get("order")
                    )
                    if broker_truth_status is not None:
                        action.item.status = broker_truth_status
                    else:
                        action.item.status = "watch_buy_rejected"
                        _record_monitoring_event(
                            item=action.item,
                            event={
                                "event": "watch_buy_rejected",
                                "reason": "order_submission_failed",
                                "message": placement.payload.get("message"),
                                "observed_price": _serialize_decimal(action.observed_price),
                                "recorded_at": triggered_at.isoformat(),
                            },
                        )
                result_by_item_id[action.item.id] = _monitoring_result_payload(
                    item=action.item,
                    observed_price=action.observed_price,
                    outcome="error",
                    message=placement.payload.get("message"),
                )

        if symbol_actions:
            pending_symbols.append(symbol)

    _log_cycle_step(
        "evaluate_items",
        evaluated_item_count=len(evaluated_item_ids),
        triggered_item_count=triggered_item_count,
        dispatch_action_count=dispatch_action_count,
    )
    backlog_symbols: set[str] = set()
    for symbol, symbol_actions in actions_by_symbol.items():
        while symbol_actions:
            action = symbol_actions.popleft()
            backlog_symbols.add(symbol)
            deferred_item_count += 1
            action.item.status = _pending_dispatch_status(action.item.intent)
            result_by_item_id[action.item.id] = _monitoring_result_payload(
                item=action.item,
                observed_price=action.observed_price,
                outcome="deferred",
                message="broker action capacity reached for this monitoring cycle",
            )

    results = [
        result_by_item_id[item_id]
        for item_id in evaluated_item_ids
        if item_id in result_by_item_id
    ]
    stop_actionability_warnings = _protective_stop_actionability_warnings(
        items=items,
        prices=prices,
        results=results,
    )
    db.flush()
    _log_cycle_step("flush_results", result_count=len(results))
    agent_status_counts = _trigger_time_agent_status_counts(items=items)
    _log_cycle_step("agent_status_counts")
    checkpoint_details = {
        "topology": MONITORING_RUNTIME_TOPOLOGY,
        "selection_strategy": MONITORING_SELECTION_STRATEGY,
        "execution_scope": checkpoint_execution_scope,
        "execution_scope_status": checkpoint_scope_status,
        "execution_scope_reason": checkpoint_scope_reason,
        "max_symbols_per_cycle": settings.monitoring_max_symbols_per_cycle,
        "max_broker_actions_per_cycle": max_broker_actions,
        "dispatch_action_count": dispatch_action_count,
        "evaluated_item_count": len(results),
        "triggered_item_count": triggered_item_count,
        "submitted_order_count": submitted_order_count,
        "blocked_item_count": blocked_item_count,
        "deferred_item_count": deferred_item_count,
        "price_count": len(prices),
        "evaluated_symbol_count": len(evaluated_symbols),
        "triggered_symbol_count": len(triggered_symbols),
        "retried_triggered_item_count": retried_triggered_item_count,
        "retried_triggered_symbols": sorted(retried_triggered_symbols),
        "rearmed_sell_monitor_count": rearmed_sell_monitor_count,
        "rearmed_sell_symbols": sorted(rearmed_sell_symbols),
        "submitted_symbols": sorted(submitted_symbols),
        "backlog_item_count": deferred_item_count,
        "backlog_symbols": sorted(backlog_symbols),
        "agent_count_scope": "selected_cycle_items",
        "agent_count_item_count": len(items),
        "agent_watching_count": agent_status_counts["watching"],
        "agent_pending_decision_count": agent_status_counts["pending_agent_decision"],
        "agent_delayed_count": agent_status_counts["agent_delayed"],
        "agent_rejected_count": agent_status_counts["agent_rejected"],
        "agent_approved_submitted_count": agent_status_counts["agent_approved_submitted"],
        "post_entry_review_count": post_entry_review_count,
        "protective_stop_coverage": _protective_stop_coverage_payload(
            issues=coverage_issues,
            repaired_count=repaired_stop_coverage_count,
            actionability_warnings=stop_actionability_warnings,
        ),
        "protective_stop_actionability": _protective_stop_actionability_payload(
            warnings=stop_actionability_warnings,
        ),
        "profit_target_coverage": _profit_target_coverage_payload(
            issues=profit_target_coverage_issues,
        ),
    }

    checkpoint = None
    if items or update_checkpoint_if_idle:
        degraded_coverage_count = sum(1 for issue in coverage_issues if issue.outcome != "rearmed")
        degraded_target_coverage_count = len(profit_target_coverage_issues)
        checkpoint = _update_monitoring_checkpoint(
            settings=settings,
            db=db,
            worker_name=MONITORING_WORKER_NAME,
            triggered_at=triggered_at,
            status=(
                "degraded"
                if deferred_item_count > 0
                or submitted_order_count > 0
                or degraded_coverage_count > 0
                or stop_actionability_warnings
                or degraded_target_coverage_count > 0
                else "running"
            ),
            details=checkpoint_details,
        )
    _log_cycle_step("checkpoint_update")
    return EvaluateMonitoringResult(
        status_code=200,
        payload={
            "triggered_at": triggered_at.isoformat(),
            "worker_name": checkpoint.worker_name if checkpoint is not None else MONITORING_WORKER_NAME,
            "evaluated_item_count": len(results),
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": submitted_order_count,
            "blocked_item_count": blocked_item_count,
            "deferred_item_count": deferred_item_count,
            "evaluated_symbol_count": checkpoint_details["evaluated_symbol_count"],
            "triggered_symbol_count": checkpoint_details["triggered_symbol_count"],
            "rearmed_sell_monitor_count": checkpoint_details[
                "rearmed_sell_monitor_count"
            ],
            "rearmed_sell_symbols": checkpoint_details["rearmed_sell_symbols"],
            "submitted_symbols": checkpoint_details["submitted_symbols"],
            "protective_stop_coverage": checkpoint_details["protective_stop_coverage"],
            "protective_stop_actionability": checkpoint_details["protective_stop_actionability"],
            "profit_target_coverage": checkpoint_details["profit_target_coverage"],
            "items": results,
        },
    )


def _prioritize_sell_actions_by_symbol(
    actions_by_symbol: dict[str, deque[PendingMonitoringAction]],
) -> None:
    for symbol, actions in actions_by_symbol.items():
        if len(actions) <= 1:
            continue
        ordered_actions = sorted(
            actions,
            key=lambda action: (
                _monitoring_action_priority(action),
                action.item.plan_item_index,
                action.item.id,
            ),
        )
        actions_by_symbol[symbol] = deque(ordered_actions)


def _monitoring_action_priority(action: PendingMonitoringAction) -> int:
    if action.item.side == "sell" and action.item.intent == "stop_sell":
        return 0
    if action.item.side == "sell":
        return 1
    return 2


def _prioritized_monitoring_symbol_order(
    *,
    symbol_order: list[str],
    actions_by_symbol: dict[str, deque[PendingMonitoringAction]],
) -> list[str]:
    """Keep every protective stop ahead of other sells, and every sell ahead of buys."""

    original_position = {symbol: index for index, symbol in enumerate(symbol_order)}
    return sorted(
        symbol_order,
        key=lambda symbol: (
            _monitoring_action_priority(actions_by_symbol[symbol][0])
            if actions_by_symbol.get(symbol)
            else 3,
            original_position[symbol],
        ),
    )


def expire_prior_market_date_unfilled_watch_buys(
    *,
    settings: Settings,
    db: Session,
    current_market_date: date,
    expired_at: datetime | None = None,
    max_age_trading_sessions: int = 1,
    effective_at: datetime | None = None,
) -> int:
    expired_at = _normalize_utc(expired_at or datetime.now(UTC))
    resolved_effective_at = (
        _normalize_utc(effective_at) if effective_at is not None else None
    )
    authority_projection = ExecutionPlanItem.risk_context["core_buy_authority"]
    authority_previous = authority_projection["previous"]
    authority_blocked_watch = and_(
        ExecutionPlanItem.status == "watch_expired",
        authority_projection["schema_version"].as_string()
        == "core_buy_authority_projection_v1",
        authority_projection["source"].as_string()
        == "core_live_market_buy_authority",
        authority_projection["action"].as_string() == "block_new_buy",
        authority_previous["status"].as_string().in_(
            tuple(WATCH_BUY_EXPIRABLE_STATUSES)
        ),
    )
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            or_(
                ExecutionPlanItem.status.in_(tuple(WATCH_BUY_EXPIRABLE_STATUSES)),
                authority_blocked_watch,
            ),
            ~ExecutionPlanItem.broker_orders.any(),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.source_market_date,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    scoped_candidates: list[ExecutionPlanItem] = []
    quarantined_route_count = 0
    for item in candidates:
        risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
        if item.status == "watch_expired":
            authority = (
                risk_context.get("core_buy_authority")
                if isinstance(risk_context, dict)
                else None
            )
            previous = (
                authority.get("previous")
                if isinstance(authority, dict)
                else None
            )
            if (
                not isinstance(authority, dict)
                or authority.get("schema_version")
                != "core_buy_authority_projection_v1"
                or authority.get("source") != "core_live_market_buy_authority"
                or authority.get("action") != "block_new_buy"
                or not isinstance(previous, dict)
                or previous.get("status") not in WATCH_BUY_EXPIRABLE_STATUSES
            ):
                continue
        if resolved_effective_at is not None and not _monitoring_item_is_effective_at(
            item=item,
            effective_at=resolved_effective_at,
        ):
            continue
        provider = str(item.provider or "kis").strip().lower()
        if provider in {"korea-investment", "korea_investment"}:
            provider = "kis"
        if provider != "kis":
            continue
        route_classification = classify_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context=risk_context,
            quantity_policy=item.quantity_policy,
        )
        if route_classification == "active":
            scoped_candidates.append(item)
        elif route_classification == "indeterminate":
            if not _quarantine_invalid_kis_watch_routes(
                settings=settings,
                items=[item],
                recorded_at=expired_at,
            ):
                quarantined_route_count += 1
    candidates = scoped_candidates
    if quarantined_route_count:
        # Session factories deliberately disable autoflush. Persist quarantine
        # before the duplicate-selection query can observe stale active rows.
        db.flush()

    observed_market_dates = sorted(
        market_date
        for market_date in db.scalars(
            select(distinct(ExecutionPlan.market_date)).where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlan.market_date.is_not(None),
                ExecutionPlan.market_date <= current_market_date,
            )
        ).all()
        if market_date is not None
    )
    hard_max_calendar_days = max(
        WATCH_BUY_MIN_HARD_CALENDAR_TTL_DAYS,
        max_age_trading_sessions * 4,
    )
    expired: list[tuple[ExecutionPlanItem, date, date, int, int, str]] = []
    for item in candidates:
        item_current_market_date = (
            _plan_item_market_date(item=item, effective_at=resolved_effective_at)
            if resolved_effective_at is not None
            else current_market_date
        )
        source_market_date = _watch_buy_source_market_date(item)
        age_trading_sessions = sum(
            source_market_date < market_date <= item_current_market_date
            for market_date in observed_market_dates
        )
        age_calendar_days = (item_current_market_date - source_market_date).days
        if _short_term_watch_target_session_elapsed(
            item=item,
            current_market_date=item_current_market_date,
            effective_at=resolved_effective_at,
        ):
            expiry_reason = "target_session_elapsed"
        elif source_market_date >= item_current_market_date:
            continue
        elif age_trading_sessions >= max_age_trading_sessions:
            expiry_reason = "trading_session_ttl_elapsed"
        elif age_calendar_days >= hard_max_calendar_days:
            expiry_reason = "calendar_safety_ttl_elapsed"
        else:
            continue
        expired.append(
            (
                item,
                source_market_date,
                item_current_market_date,
                age_trading_sessions,
                age_calendar_days,
                expiry_reason,
            )
        )

    for (
        item,
        source_market_date,
        item_current_market_date,
        age_trading_sessions,
        age_calendar_days,
        expiry_reason,
    ) in expired:
        prior_status = item.status
        item.status = "watch_expired"
        risk_context = dict(
            _loaded_risk_context(item, allow_file_backed_load=True)
        )
        authority = risk_context.get("core_buy_authority")
        if (
            isinstance(authority, dict)
            and authority.get("schema_version")
            == "core_buy_authority_projection_v1"
            and authority.get("source") == "core_live_market_buy_authority"
            and authority.get("action") == "block_new_buy"
        ):
            authority = dict(authority)
            previous = (
                dict(authority.get("previous"))
                if isinstance(authority.get("previous"), dict)
                else {}
            )
            previous["status"] = "watch_expired"
            authority["previous"] = previous
            authority["recovery"] = {
                "status": "not_restored_terminal",
                "reason": expiry_reason,
                "expired_at": expired_at.isoformat(),
            }
            risk_context["core_buy_authority"] = authority
            item.risk_context = risk_context
        legacy_expiry = (
            settings.runtime_mode != "live"
            and max_age_trading_sessions == 1
            and expiry_reason == "trading_session_ttl_elapsed"
        )
        _record_monitoring_event(
            item=item,
            event={
                "event": (
                    "market_date_expired_unfilled"
                    if legacy_expiry
                    else f"watch_buy_{expiry_reason}"
                ),
                "reason": (
                    "prior_market_date_unfilled_watch_buy"
                    if legacy_expiry
                    else expiry_reason
                ),
                "prior_status": prior_status,
                "source_market_date": source_market_date.isoformat(),
                "target_market_date": (
                    item.plan.market_date.isoformat()
                    if item.plan.market_date is not None
                    else None
                ),
                "current_market_date": item_current_market_date.isoformat(),
                "age_trading_sessions": age_trading_sessions,
                "max_age_trading_sessions": max_age_trading_sessions,
                "age_calendar_days": age_calendar_days,
                "hard_max_calendar_days": hard_max_calendar_days,
                "recorded_at": expired_at.isoformat(),
            },
        )
    return len(expired)


def _watch_buy_max_age_trading_sessions(settings: Settings) -> int | None:
    if settings.runtime_mode == "live":
        return settings.monitoring_watch_buy_max_age_trading_sessions
    if settings.monitoring_expire_prior_market_watch_buys_enabled:
        return 1
    return None


def _short_term_watch_target_session_elapsed(
    *,
    item: ExecutionPlanItem,
    current_market_date: date,
    effective_at: datetime | None,
) -> bool:
    target_market_date = item.plan.market_date
    if (
        target_market_date is None
        or not is_short_term_watch_buy_intent(item.intent)
        or _plan_item_market_timezone(item) != KRX_TIMEZONE
    ):
        return False
    if current_market_date > target_market_date:
        return True
    if current_market_date < target_market_date or effective_at is None:
        return False
    target_session_close = datetime.combine(
        target_market_date,
        datetime.min.time().replace(hour=15, minute=30),
        tzinfo=KRX_TIMEZONE,
    )
    return _normalize_utc(effective_at) >= target_session_close.astimezone(UTC)


def _watch_buy_source_market_date(item: ExecutionPlanItem) -> date:
    if item.plan.market_date is not None:
        return item.plan.market_date
    if item.source_market_date is not None:
        return item.source_market_date
    accepted_at = _normalize_utc(item.plan.accepted_at)
    return accepted_at.astimezone(KRX_TIMEZONE).date()


def supersede_duplicate_active_watch_buys(
    *,
    settings: Settings,
    db: Session,
    superseded_at: datetime | None = None,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
) -> int:
    resolved_effective_at = _normalize_utc(effective_at or datetime.now(UTC))
    effective_plan_filter = _effective_monitor_plan_filter(
        effective_at=(
            resolved_effective_at
            if effective_at is not None or current_market_date is None
            else None
        ),
        current_market_date=current_market_date,
    )
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            effective_plan_filter,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(LIVE_WATCH_ENTRY_STATUSES)),
            ~ExecutionPlanItem.broker_orders.any(),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
            ),
        )
    ).all()
    if effective_at is not None or current_market_date is None:
        candidates = [
            item
            for item in candidates
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=resolved_effective_at,
            )
        ]
    prior_statuses = {item.id: item.status for item in candidates}
    eligible_candidates = _quarantine_invalid_kis_watch_routes(
        settings=settings,
        items=candidates,
        recorded_at=_normalize_utc(superseded_at or datetime.now(UTC)),
    )
    quarantined_count = sum(
        item.status != prior_statuses[item.id]
        for item in candidates
    )
    ordered = sorted(eligible_candidates, key=_active_watch_recency_key, reverse=True)
    canonical_by_route: dict[tuple[str, str, str, str], ExecutionPlanItem] = {}
    superseded: list[tuple[ExecutionPlanItem, ExecutionPlanItem]] = []
    for item in ordered:
        route_key = _active_watch_route_key(item)
        canonical = canonical_by_route.get(route_key)
        if canonical is None:
            canonical_by_route[route_key] = item
            continue
        superseded.append((item, canonical))

    recorded_at = _normalize_utc(superseded_at or datetime.now(UTC)).isoformat()
    for item, canonical in superseded:
        prior_status = item.status
        item.status = "superseded"
        _record_monitoring_event(
            item=item,
            event={
                "event": "duplicate_watch_buy_superseded",
                "reason": "newer_active_watch_buy_exists",
                "prior_status": prior_status,
                "superseded_by_execution_plan_item_id": canonical.id,
                "recorded_at": recorded_at,
            },
        )
    return quarantined_count + len(superseded)


def _active_watch_route_key(item: ExecutionPlanItem) -> tuple[str, str, str, str]:
    symbol = str(item.symbol or "").strip().upper()
    is_krx_equity = _is_krx_equity_symbol(symbol)
    provider = str(item.provider or ("kis" if is_krx_equity else "unknown")).strip().lower()
    if provider in {"korea-investment", "korea_investment"}:
        provider = "kis"
    raw_asset_class = item.asset_class or ("stock" if is_krx_equity else "unknown")
    try:
        asset_class = canonical_execution_asset_class(
            raw_asset_class,
            provider=provider,
            exchange=_plan_item_exchange(item) if is_krx_equity else "unspecified",
        )
    except ValueError:
        asset_class = str(raw_asset_class).strip().lower()
    exchange = _plan_item_exchange(item)
    # A single account must never carry two simultaneous new-buy authorities
    # for the same market instrument. Entry-family identity still scopes
    # protective exits, but it must not allow duplicate BUYs to race each other.
    return provider, asset_class, exchange, symbol


def _active_watch_recency_key(
    item: ExecutionPlanItem,
) -> tuple[str, str, str, str, int, str]:
    market_date = item.plan.market_date or item.source_market_date or date.min
    requested_at = item.plan.requested_at
    accepted_at = item.plan.accepted_at
    return (
        market_date.isoformat(),
        _normalize_utc(requested_at).isoformat() if requested_at is not None else "",
        accepted_at.isoformat() if accepted_at is not None else "",
        item.execution_plan_id,
        -item.plan_item_index,
        item.id,
    )


def _claim_trigger_time_agent_action(
    *,
    db: Session,
    action: PendingMonitoringAction,
    triggered_at: datetime,
) -> TriggerTimeAgentClaim | TriggerTimeAgentActionResult:
    item = action.item
    local_status = item.status
    local_risk_context = dict(_loaded_risk_context(item))
    local_quantity_policy = item.quantity_policy
    item_state = sa_inspect(item)
    prior_risk_context = _attribute_value_before_local_changes(
        item_state.attrs.risk_context.history,
        fallback=item.risk_context,
    )
    prior_quantity_policy = _attribute_value_before_local_changes(
        item_state.attrs.quantity_policy.history,
        fallback=item.quantity_policy,
    )
    expected_identity = _trigger_time_agent_claim_identity(
        item,
        risk_context=prior_risk_context,
        quantity_policy=prior_quantity_policy,
    )
    current_state = _current_buy_plan_item_state(
        db=db,
        item_id=item.id,
        lock=True,
    )
    if current_state is None:
        db.rollback()
        return _trigger_time_agent_claim_lost_result(item=item, action=action)
    current_status, current_risk_context = current_state
    with db.no_autoflush:
        db.refresh(
            item,
            attribute_names=[
                "execution_plan_id",
                "status",
                "provider",
                "venue",
                "asset_class",
                "symbol",
                "side",
                "intent",
                "limit_price",
                "trigger_price",
                "time_in_force",
                "base_quantity",
                "quote_notional",
                "quantity_precision",
                "price_precision",
                "risk_context",
                "quantity_policy",
            ],
        )
    claimable_statuses = {
        "watch_monitoring_pending",
        _monitoring_status(item.intent),
        _pending_dispatch_status(item.intent),
        "watch_buy_triggered",
        "watch_buy_triggered_blocked",
        *SELL_TRIGGERED_BLOCKED_STATUSES,
    }
    local_recovery_event = local_risk_context.get("last_monitoring_event")
    recovered_agent_state_is_claimable = bool(
        current_status in AGENT_ACTIVE_STATUSES
        and local_status == _monitoring_status(item.intent)
        and isinstance(local_recovery_event, dict)
        and local_recovery_event.get("event")
        in {
            "trigger_time_exec_agent_pending_recovered",
            "trigger_time_exec_agent_state_recovered",
        }
        and local_recovery_event.get("previous_status") == current_status
        and json_payload_hash(current_risk_context)
        == json_payload_hash(
            prior_risk_context if isinstance(prior_risk_context, dict) else {}
        )
    )
    refreshed_identity = _trigger_time_agent_claim_identity(item)
    if (
        (
            current_status not in claimable_statuses
            and not recovered_agent_state_is_claimable
        )
        or refreshed_identity != expected_identity
    ):
        db.rollback()
        with db.no_autoflush:
            db.refresh(item)
        return _trigger_time_agent_claim_lost_result(item=item, action=action)

    durable_risk_context = dict(_loaded_risk_context(item))
    durable_risk_context.update(local_risk_context)
    durable_events = durable_risk_context.get("monitoring_events")
    local_events = local_risk_context.get("monitoring_events")
    combined_events: list[object] = []
    for event in (
        *(durable_events if isinstance(durable_events, list) else []),
        *(local_events if isinstance(local_events, list) else []),
    ):
        if event not in combined_events:
            combined_events.append(event)
    if combined_events:
        durable_risk_context["monitoring_events"] = combined_events[-19:]
    item.risk_context = compact_json_object(durable_risk_context)
    item.quantity_policy = local_quantity_policy
    claim_id = new_id()
    item.status = AGENT_PENDING_STATUS
    _record_monitoring_event(
        item=item,
        event={
            "event": "trigger_time_exec_agent_pending",
            "claim_id": claim_id,
            "previous_status": current_status,
            "trigger_reason": action.trigger_reason or item.intent,
            "observed_price": _serialize_decimal(action.observed_price),
            "recorded_at": triggered_at.isoformat(),
        },
    )
    db.commit()
    return TriggerTimeAgentClaim(
        claim_id=claim_id,
        previous_status=current_status,
        identity=_trigger_time_agent_claim_identity(item),
    )


def _trigger_time_agent_claim_is_current(
    *,
    db: Session,
    item: ExecutionPlanItem,
    claim: TriggerTimeAgentClaim,
) -> bool:
    current_state = _current_buy_plan_item_state(
        db=db,
        item_id=item.id,
        lock=True,
    )
    if current_state is None:
        db.rollback()
        return False
    with db.no_autoflush:
        db.refresh(
            item,
            attribute_names=[
                "execution_plan_id",
                "status",
                "provider",
                "venue",
                "asset_class",
                "symbol",
                "side",
                "intent",
                "limit_price",
                "trigger_price",
                "time_in_force",
                "base_quantity",
                "quote_notional",
                "quantity_precision",
                "price_precision",
                "risk_context",
                "quantity_policy",
            ],
        )
    risk_context = dict(_loaded_risk_context(item))
    latest_event = risk_context.get("last_monitoring_event")
    valid = bool(
        item.status == AGENT_PENDING_STATUS
        and _trigger_time_agent_claim_identity(item) == claim.identity
        and isinstance(latest_event, dict)
        and latest_event.get("event") == "trigger_time_exec_agent_pending"
        and latest_event.get("claim_id") == claim.claim_id
    )
    if valid:
        return True
    db.rollback()
    with db.no_autoflush:
        db.refresh(item)
    return False


_TRIGGER_TIME_IDENTITY_UNSET = object()


def _trigger_time_agent_claim_identity(
    item: ExecutionPlanItem,
    *,
    risk_context: object = _TRIGGER_TIME_IDENTITY_UNSET,
    quantity_policy: object = _TRIGGER_TIME_IDENTITY_UNSET,
) -> tuple[object, ...]:
    resolved_risk_context = (
        _loaded_risk_context(item)
        if risk_context is _TRIGGER_TIME_IDENTITY_UNSET
        else risk_context
    )
    safety_context = (
        dict(resolved_risk_context)
        if isinstance(resolved_risk_context, dict)
        else {}
    )
    for transient_key in (
        "monitoring_events",
        "last_monitoring_event",
        "watch_buy_trigger_cross",
        "last_stale_trigger_reclaim_check",
        "latest_live_market_context",
    ):
        safety_context.pop(transient_key, None)
    resolved_quantity_policy = (
        item.quantity_policy
        if quantity_policy is _TRIGGER_TIME_IDENTITY_UNSET
        else quantity_policy
    )
    return (
        item.execution_plan_id,
        item.provider,
        item.venue,
        item.asset_class,
        item.symbol,
        item.side,
        item.intent,
        item.limit_price,
        item.trigger_price,
        item.time_in_force,
        item.base_quantity,
        item.quote_notional,
        item.quantity_precision,
        item.price_precision,
        json_payload_hash(resolved_quantity_policy),
        json_payload_hash(safety_context),
    )


def _attribute_value_before_local_changes(
    history: object,
    *,
    fallback: object,
) -> object:
    deleted = getattr(history, "deleted", ())
    return deleted[0] if deleted else fallback


def _trigger_time_agent_claim_lost_result(
    *,
    item: ExecutionPlanItem,
    action: PendingMonitoringAction,
) -> TriggerTimeAgentActionResult:
    return TriggerTimeAgentActionResult(
        quantity=None,
        limit_price=None,
        result_payload=_monitoring_result_payload(
            item=item,
            observed_price=action.observed_price,
            outcome="blocked",
            block_reason="trigger_time_exec_agent_claim_lost",
            message=(
                "trigger-time exec-agent result was discarded because the durable "
                "plan item changed while the decision was pending"
            ),
            trigger_reason=action.trigger_reason,
            trigger_metadata=action.trigger_metadata,
        ),
    )


def _cycle_bounded_trigger_agent_settings(
    *,
    settings: Settings,
    cycle_started: float,
) -> Settings | None:
    """Leave enough cycle budget to terminate a timed-out agent subprocess."""

    elapsed_seconds = max(0.0, time.monotonic() - cycle_started)
    remaining_agent_seconds = int(
        settings.monitoring_cycle_timeout_seconds - elapsed_seconds - 2.0
    )
    if remaining_agent_seconds < 1:
        return None
    return settings.model_copy(
        update={
            "exec_agent_trigger_time_timeout_seconds": min(
                settings.exec_agent_trigger_time_timeout_seconds,
                remaining_agent_seconds,
            )
        }
    )


def _resolve_trigger_time_agent_action(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    action: PendingMonitoringAction,
    triggered_at: datetime,
    cycle_started: float,
    intended_quantity: int | None,
    remaining_quantity: int | None,
) -> TriggerTimeAgentActionResult:
    item = action.item
    claim = _claim_trigger_time_agent_action(
        db=db,
        action=action,
        triggered_at=triggered_at,
    )
    if isinstance(claim, TriggerTimeAgentActionResult):
        return claim
    previous_status = claim.previous_status
    if settings.runtime_mode == "live" and _requires_trigger_time_liquidity_snapshot(item):
        action, liquidity_failure = _refresh_trigger_time_action_liquidity(
            settings=settings,
            db=db,
            adapter=adapter,
            action=action,
            reference_at=triggered_at
            + timedelta(seconds=max(0.0, time.monotonic() - cycle_started)),
        )
        if liquidity_failure is not None:
            return _trigger_time_agent_preflight_failure(
                settings=settings,
                db=db,
                claim=claim,
                action=action,
                previous_status=previous_status,
                recorded_at=triggered_at,
                reason=liquidity_failure,
            )
    bounded_settings = _cycle_bounded_trigger_agent_settings(
        settings=settings,
        cycle_started=cycle_started,
    )
    if bounded_settings is None:
        return _trigger_time_agent_preflight_failure(
            settings=settings,
            db=db,
            claim=claim,
            action=action,
            previous_status=previous_status,
            recorded_at=triggered_at,
            reason="monitoring cycle has insufficient time remaining for an agent decision",
        )
    resolution = trigger_time_exec_agent.resolve_trigger_time_exec_agent_decision(
        settings=bounded_settings,
        db=db,
        adapter=adapter,
        item=item,
        observed_price=action.observed_price,
        observed_at=_price_observed_at_from_action(action=action, fallback=triggered_at),
        triggered_at=triggered_at,
        trigger_reason=action.trigger_reason,
        trigger_metadata=action.trigger_metadata,
        live_market_context=action.market_quality_context,
        intended_quantity=intended_quantity,
        remaining_quantity=remaining_quantity,
    )
    decision = resolution.decision
    if decision.decision == "keep_watching":
        if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
            return _trigger_time_agent_claim_lost_result(item=item, action=action)
        _persist_pre_agent_liquidity_evidence(
            settings=settings,
            item=item,
            action=action,
        )
        item.status = _monitoring_status(item.intent)
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_keep_watching",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "valid_until": _serialize_datetime(decision.valid_until),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        db.commit()
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="waiting",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    if decision.decision == "delay":
        if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
            return _trigger_time_agent_claim_lost_result(item=item, action=action)
        _persist_pre_agent_liquidity_evidence(
            settings=settings,
            item=item,
            action=action,
        )
        item.status = AGENT_DELAY_STATUS
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_delay",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "valid_until": _serialize_datetime(decision.valid_until),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        db.commit()
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="agent_delay",
                block_reason="trigger_time_exec_agent_delay",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    if decision.decision == "reject":
        if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
            return _trigger_time_agent_claim_lost_result(item=item, action=action)
        _persist_pre_agent_liquidity_evidence(
            settings=settings,
            item=item,
            action=action,
        )
        item.status = AGENT_REJECTED_STATUS
        _record_monitoring_event(
            item=item,
            event={
                "event": "trigger_time_exec_agent_rejected",
                "reason": decision.reason,
                "observed_price": _serialize_decimal(action.observed_price),
                "market_quality_gate": _trigger_time_market_quality_gate(resolution),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
            item=item,
            resolution=resolution,
            previous_status=previous_status,
            new_status=item.status,
            recorded_at=triggered_at,
        )
        db.commit()
        return TriggerTimeAgentActionResult(
            quantity=None,
            limit_price=None,
            result_payload=_monitoring_result_payload(
                item=item,
                observed_price=action.observed_price,
                outcome="rejected",
                message=decision.reason,
                trigger_reason=action.trigger_reason,
                trigger_metadata=action.trigger_metadata,
            ),
        )

    refreshed_price: MonitoringPriceRequest | None = None
    post_agent_limit_adjustment: dict[str, object] | None = None
    post_agent_market_quality_context = action.market_quality_context
    request_market_context = resolution.request_payload.get("market_context")
    post_agent_delegated_context = (
        request_market_context if isinstance(request_market_context, dict) else None
    )
    post_agent_evaluated_at = triggered_at
    if settings.runtime_mode == "live":
        refresh_started_at = triggered_at + timedelta(
            seconds=max(0.0, time.monotonic() - cycle_started)
        )
        refreshed_price, refresh_error = _refresh_trigger_time_market_price(
            settings=settings,
            db=db,
            adapter=adapter,
            item=item,
            reference_at=refresh_started_at,
            trigger_liquidity_price=action.price,
        )
        revalidated_at = triggered_at + timedelta(
            seconds=max(0.0, time.monotonic() - cycle_started)
        )
        if refresh_error is not None or refreshed_price is None:
            return _post_agent_revalidation_failure(
                settings=settings,
                db=db,
                claim=claim,
                item=item,
                action=action,
                resolution=resolution,
                previous_status=previous_status,
                recorded_at=revalidated_at,
                reason="post_agent_quote_unavailable",
                message=refresh_error or "broker quote refresh returned no quote",
                outcome="agent_delay",
                new_status=AGENT_DELAY_STATUS,
            )
        fresh_quality = _market_quality_context_from_price(
            refreshed_price,
            item=item,
        )
        post_agent_market_quality_context = fresh_quality
        fresh_market_context, stale_reason = trigger_time_exec_agent._market_context(
            settings=settings,
            item=item,
            observed_price=_observed_price_for_item(price=refreshed_price, item=item),
            observed_at=refreshed_price.observed_at,
            triggered_at=revalidated_at,
            trigger_reason=action.trigger_reason,
            trigger_metadata=action.trigger_metadata,
            live_market_context=fresh_quality,
        )
        post_agent_delegated_context = fresh_market_context
        post_agent_evaluated_at = revalidated_at
        quote_reason = _quote_stale_reason(
            settings=settings,
            price=refreshed_price,
            cycle_at=revalidated_at,
        )
        quality_reason = _post_agent_market_quality_reason(
            settings=settings,
            price=refreshed_price,
            evaluated_at=revalidated_at,
            side=item.side,
        )
        if (
            quote_reason is not None
            or stale_reason is not None
            or fresh_market_context.get("live_execution_context_available") is not True
            or quality_reason is not None
        ):
            missing_fields = fresh_market_context.get("missing_critical_fields")
            detail = quote_reason or stale_reason or quality_reason
            if detail is None and isinstance(missing_fields, list):
                detail = f"missing live market fields: {', '.join(map(str, missing_fields))}"
            return _post_agent_revalidation_failure(
                settings=settings,
                db=db,
                claim=claim,
                item=item,
                action=action,
                resolution=resolution,
                previous_status=previous_status,
                recorded_at=revalidated_at,
                reason="post_agent_market_quality_unavailable",
                message=detail or "fresh live market context failed the execution quality gate",
                outcome="agent_delay",
                new_status=AGENT_DELAY_STATUS,
                refreshed_price=refreshed_price,
                market_quality_gate=fresh_market_context.get("live_execution_quality_gate"),
            )
        if not _post_agent_trigger_still_valid(
            item=item,
            price=refreshed_price,
            decision=decision,
            triggered_at=revalidated_at,
        ):
            return _post_agent_revalidation_failure(
                settings=settings,
                db=db,
                claim=claim,
                item=item,
                action=action,
                resolution=resolution,
                previous_status=previous_status,
                recorded_at=revalidated_at,
                reason="post_agent_trigger_not_met",
                message="fresh broker quote no longer satisfies the monitored trigger",
                outcome="waiting",
                new_status=_monitoring_status(item.intent),
                refreshed_price=refreshed_price,
                market_quality_gate=fresh_market_context.get("live_execution_quality_gate"),
            )
        if _is_watch_buy_item(item):
            chase_limit_price = _watch_buy_chase_limit_price(item)
            refreshed_observed_price = _observed_price_for_item(
                price=refreshed_price,
                item=item,
            )
            if (
                chase_limit_price is not None
                and refreshed_observed_price is not None
                and refreshed_observed_price > chase_limit_price
            ):
                return _post_agent_revalidation_failure(
                    settings=settings,
                    db=db,
                    claim=claim,
                    item=item,
                    action=action,
                    resolution=resolution,
                    previous_status=previous_status,
                    recorded_at=revalidated_at,
                    reason="post_agent_chase_limit_exceeded",
                    message="fresh broker quote is above the configured chase limit",
                    outcome="waiting",
                    new_status=_monitoring_status(item.intent),
                    refreshed_price=refreshed_price,
                    market_quality_gate=fresh_market_context.get("live_execution_quality_gate"),
                )
            if decision.limit_price is not None and refreshed_price.ask is not None:
                fresh_limit_cap = refreshed_price.ask
                if chase_limit_price is not None:
                    fresh_limit_cap = min(fresh_limit_cap, chase_limit_price)
                if decision.limit_price > fresh_limit_cap:
                    original_limit_price = decision.limit_price
                    decision.limit_price = fresh_limit_cap
                    post_agent_limit_adjustment = {
                        "reason": "fresh_ask_limit_cap",
                        "agent_limit_price": _serialize_decimal(original_limit_price),
                        "fresh_ask": _serialize_decimal(refreshed_price.ask),
                        "chase_limit_price": _serialize_decimal(chase_limit_price),
                        "adjusted_limit_price": _serialize_decimal(fresh_limit_cap),
                    }

    geometry_reference_price = (
        _observed_price_for_item(price=refreshed_price, item=item)
        if refreshed_price is not None
        else action.observed_price
    )
    enforced_stop = _high_volatility_chase_override_stop_to_enforce(
        item=item,
        decision=decision,
    )
    geometry_decision = decision
    geometry_error: str | None = None
    if enforced_stop is not None:
        if geometry_reference_price is None or geometry_reference_price <= 0:
            geometry_error = "buy enforced stop requires a positive fresh reference price"
        elif enforced_stop >= geometry_reference_price:
            geometry_error = (
                "buy enforced stop must be below fresh reference price: "
                f"{enforced_stop}>={geometry_reference_price}"
            )
        else:
            geometry_decision = decision.model_copy(
                update={"updated_stop_price": enforced_stop}
            )
    if geometry_error is None:
        geometry_error = trigger_time_exec_agent.trigger_time_decision_geometry_error(
            decision=geometry_decision,
            reference_price=geometry_reference_price,
        )
    economics_reason: str | None = None
    economics_message: str | None = None
    economics: BuyEconomicsAssessment | None = None
    if geometry_error is None and _is_watch_buy_item(item):
        effective_target = (
            decision.updated_target_price or _watch_buy_target_price(item)
        )
        effective_stop = (
            enforced_stop
            or decision.updated_stop_price
            or _watch_buy_stop_price(item)
        )
        execution_geometry_decision = geometry_decision.model_copy(
            update={
                "updated_trigger_price": None,
                "updated_target_price": effective_target,
                "updated_stop_price": effective_stop,
            }
        )
        if effective_target is None:
            geometry_error = "buy effective target requires a positive price"
        else:
            geometry_error = (
                trigger_time_exec_agent.trigger_time_decision_geometry_error(
                    decision=execution_geometry_decision,
                    reference_price=geometry_reference_price,
                )
            )
        if geometry_error is None and effective_target is not None:
            target_entry_reference = geometry_reference_price
            if decision.limit_price is not None and (
                target_entry_reference is None
                or decision.limit_price > target_entry_reference
            ):
                target_entry_reference = decision.limit_price
            target_geometry_decision = execution_geometry_decision.model_copy(
                update={"limit_price": None, "updated_stop_price": None}
            )
            geometry_error = (
                trigger_time_exec_agent.trigger_time_decision_geometry_error(
                    decision=target_geometry_decision,
                    reference_price=target_entry_reference,
                )
            )
        if geometry_error is None:
            economics_stop = effective_stop_price(
                settings=settings,
                entry_price=target_entry_reference,
                upstream_stop_price=effective_stop,
            )
            economics = assess_buy_economics(
                reference_price=target_entry_reference,
                target_price=effective_target,
                stop_price=economics_stop,
                minimum_reward_risk=_minimum_reward_risk_for_watch_buy(
                    settings=settings,
                    item=item,
                ),
                evidence=_post_agent_live_cost_evidence(
                    settings=settings,
                    item=item,
                    market_context=post_agent_market_quality_context,
                ),
                require_cost_evidence=settings.runtime_mode == "live",
            )
            if economics.reason is not None:
                economics_reason = f"post_agent_{economics.reason}"
                economics_message = (
                    "trigger-time approval failed deterministic buy economics: "
                    f"{economics.reason}; gross_reward_risk="
                    f"{_serialize_decimal(economics.gross_reward_risk)}; "
                    f"net_reward_risk={_serialize_decimal(economics.net_reward_risk)}; "
                    f"minimum_reward_risk={_serialize_decimal(economics.minimum_reward_risk)}; "
                    f"estimated_round_trip_cost_bps="
                    f"{_serialize_decimal(economics.estimated_round_trip_cost_bps)}"
                )
            elif delegated_rejections := delegated_buy_gate_rejections(
                [_loaded_risk_context(item), item.quantity_policy],
                evaluated_at=post_agent_evaluated_at,
                live_market_context=post_agent_delegated_context,
                economics=economics,
                require_live_recheck=True,
            ):
                economics_reason = f"post_agent_{delegated_rejections[0]}"
                economics_message = (
                    "trigger-time approval failed deterministic delegated entry gates: "
                    + ",".join(delegated_rejections)
                )
            elif settings.runtime_mode == "live" and is_short_term_watch_buy_intent(item.intent):
                live_turnover = trusted_live_market_decimal(
                    post_agent_delegated_context,
                    field="turnover",
                    evaluated_at=post_agent_evaluated_at,
                )
                if live_turnover is None:
                    economics_reason = "post_agent_short_term_live_turnover_unavailable"
                    economics_message = (
                        "short-term buy requires timestamped trigger-time turnover evidence"
                    )
                elif live_turnover < SHORT_TERM_LIVE_MIN_TURNOVER_KRW:
                    economics_reason = "post_agent_short_term_live_turnover_below_minimum"
                    economics_message = (
                        "short-term buy trigger-time turnover is below the deterministic "
                        f"{_serialize_decimal(SHORT_TERM_LIVE_MIN_TURNOVER_KRW)} KRW floor"
                    )
    if geometry_error is not None:
        return _post_agent_revalidation_failure(
            settings=settings,
            db=db,
            claim=claim,
            item=item,
            action=action,
            resolution=resolution,
            previous_status=previous_status,
            recorded_at=(
                refreshed_price.observed_at
                if refreshed_price is not None
                and refreshed_price.observed_at is not None
                else triggered_at
            ),
            reason="post_agent_invalid_target_stop_geometry",
            message=geometry_error,
            outcome="agent_delay",
            new_status=AGENT_DELAY_STATUS,
            refreshed_price=refreshed_price,
            market_quality_gate=_trigger_time_market_quality_gate(resolution),
            liquidity_evidence_price=refreshed_price,
            economics=economics,
        )
    if economics_reason is not None:
        return _post_agent_revalidation_failure(
            settings=settings,
            db=db,
            claim=claim,
            item=item,
            action=action,
            resolution=resolution,
            previous_status=previous_status,
            recorded_at=(
                refreshed_price.observed_at
                if refreshed_price is not None
                and refreshed_price.observed_at is not None
                else triggered_at
            ),
            reason=economics_reason,
            message=economics_message or "trigger-time approval failed deterministic buy economics",
            outcome="agent_delay",
            new_status=AGENT_DELAY_STATUS,
            refreshed_price=refreshed_price,
            market_quality_gate=_trigger_time_market_quality_gate(resolution),
            liquidity_evidence_price=refreshed_price,
            economics=economics,
        )

    if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
        return _trigger_time_agent_claim_lost_result(item=item, action=action)
    if item.side == "sell" and decision.updated_stop_price is not None:
        locked_stop = _locked_effective_protective_stop_price(
            db=db,
            item=item,
        )
        if locked_stop is not None and decision.updated_stop_price < locked_stop:
            return _post_agent_revalidation_failure(
                settings=settings,
                db=db,
                claim=claim,
                item=item,
                action=action,
                resolution=resolution,
                previous_status=previous_status,
                recorded_at=triggered_at,
                reason="post_agent_protective_stop_relaxation",
                message=(
                    "trigger-time sell approval would loosen the current protective "
                    f"stop: {decision.updated_stop_price}<{locked_stop}"
                ),
                outcome="agent_delay",
                new_status=AGENT_DELAY_STATUS,
                refreshed_price=refreshed_price,
                market_quality_gate=_trigger_time_market_quality_gate(resolution),
            )
    item.status = AGENT_APPROVED_PENDING_SUBMIT_STATUS
    _apply_trigger_time_agent_adjustments(item=item, decision=decision)
    if refreshed_price is not None and _requires_trigger_time_liquidity_snapshot(item):
        _persist_trigger_time_liquidity_evidence(
            item=item,
            price=refreshed_price,
        )
    if economics is not None:
        _persist_trigger_time_full_cost_economics(
            settings=settings,
            item=item,
            economics=economics,
            evaluated_at=post_agent_evaluated_at,
        )
    if post_agent_limit_adjustment is not None:
        risk_context = dict(_loaded_risk_context(item))
        risk_context["trigger_time_limit_price_revalidation"] = post_agent_limit_adjustment
        item.risk_context = compact_json_object(risk_context)
    _record_monitoring_event(
        item=item,
        event={
            "event": "trigger_time_exec_agent_approved",
            "reason": decision.reason,
            "quantity": decision.quantity,
            "limit_price": _serialize_decimal(decision.limit_price),
            "updated_target_price": _serialize_decimal(decision.updated_target_price),
            "updated_stop_price": _serialize_decimal(decision.updated_stop_price),
            "observed_price": _serialize_decimal(action.observed_price),
            "post_agent_observed_price": _serialize_decimal(
                _observed_price_for_item(price=refreshed_price, item=item)
                if refreshed_price is not None
                else None
            ),
            "post_agent_observed_at": _serialize_datetime(
                refreshed_price.observed_at if refreshed_price is not None else None
            ),
            "post_agent_limit_adjustment": post_agent_limit_adjustment,
            "market_quality_gate": _trigger_time_market_quality_gate(resolution),
            "recorded_at": triggered_at.isoformat(),
        },
    )
    trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
        item=item,
        resolution=resolution,
        previous_status=previous_status,
        new_status=item.status,
        recorded_at=triggered_at,
    )
    db.commit()
    return TriggerTimeAgentActionResult(
        quantity=decision.quantity,
        limit_price=decision.limit_price,
        result_payload={},
        refreshed_price=refreshed_price,
    )


def _locked_effective_protective_stop_price(
    *,
    db: Session,
    item: ExecutionPlanItem,
) -> Decimal | None:
    terminal_statuses = {
        "agent_rejected",
        "broker_order_failed",
        "canceled",
        "cancelled",
        "expired",
        "failed",
        "filled",
        "rejected",
        "superseded",
    }
    stop_items = db.scalars(
        select(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.execution_plan_id == item.execution_plan_id,
            ExecutionPlanItem.symbol == item.symbol,
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.status.not_in(terminal_statuses),
        )
        .execution_options(populate_existing=True)
        .with_for_update()
    ).all()
    stop_items = select_plan_item_family_members(
        anchor=item,
        candidates=stop_items,
    )
    candidates = [
        stop_item.trigger_price
        for stop_item in stop_items
        if stop_item.trigger_price is not None
        and stop_item.trigger_price.is_finite()
        and stop_item.trigger_price > 0
    ]
    for payload in (item.risk_context, item.quantity_policy):
        if not isinstance(payload, dict):
            continue
        for key in ("updated_stop_price", "stop_price", "stop_loss_price"):
            candidate = _parse_positive_decimal(payload.get(key))
            if candidate is not None:
                candidates.append(candidate)
    return max(candidates) if candidates else None


def _post_agent_live_cost_evidence(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    market_context: dict[str, object],
) -> dict[str, object]:
    """Build the only cost context trusted for a live trigger-time buy."""

    risk_context = _loaded_risk_context(item)
    return configured_execution_cost_evidence(
        settings=settings,
        provider=item.provider or "kis",
        asset_class=item.asset_class or "stock",
        exchange=item.venue or risk_context.get("exchange") or "KRX",
        live_quote_spread_bps=market_context.get("spread_bps"),
        require_live_quote_spread=settings.runtime_mode == "live",
    )


def _refresh_trigger_time_action_liquidity(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    action: PendingMonitoringAction,
    reference_at: datetime,
) -> tuple[PendingMonitoringAction, str | None]:
    if action.price is None:
        return action, "trigger-time price is unavailable for liquidity binding"
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="trigger_time_liquidity_refresh",
            reason=f"refresh timestamped intraday liquidity for {action.item.symbol}",
        )
        quote = adapter.get_current_price(
            session_state.access_token or "",
            symbol=action.item.symbol,
            exchange=_plan_item_exchange(action.item),
        )
        if quote is None:
            return action, "trigger-time broker current-price endpoint returned no quote"
        if quote.symbol != action.item.symbol:
            return action, "trigger-time broker quote symbol identity mismatch"
        context, context_failure = _fetch_live_market_context(
            broker_adapter=adapter,
            access_token=session_state.access_token or "",
            quote=quote,
        )
        price = monitoring_price_from_broker_quote(
            settings=settings,
            quote=quote,
            context=context,
            context_failure=context_failure,
            side=action.item.side,
            purpose=action.item.intent,
        )
        price, failure = _merge_trigger_time_liquidity_snapshot(
            settings=settings,
            adapter=adapter,
            access_token=session_state.access_token or "",
            item=action.item,
            price=price,
            reference_at=reference_at,
        )
    except Exception as exc:
        return action, (
            "intraday liquidity refresh failed: "
            f"{exc.__class__.__name__}: {_bounded_text(str(exc), max_length=240)}"
        )
    if failure is not None:
        return action, failure
    return _action_with_refreshed_price(action=action, price=price), None


def _trigger_time_agent_preflight_failure(
    *,
    settings: Settings,
    db: Session,
    claim: TriggerTimeAgentClaim,
    action: PendingMonitoringAction,
    previous_status: str,
    recorded_at: datetime,
    reason: str,
) -> TriggerTimeAgentActionResult:
    item = action.item
    if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
        return _trigger_time_agent_claim_lost_result(item=item, action=action)
    item.status = "watch_buy_triggered_blocked"
    retry_after_at = recorded_at + timedelta(
        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
    )
    _record_monitoring_event(
        item=item,
        event={
            "event": "trigger_time_liquidity_unavailable",
            "reason": "trigger_time_liquidity_unavailable",
            "message": reason,
            "previous_status": previous_status,
            "retry_after_at": retry_after_at.isoformat(),
            "recorded_at": recorded_at.isoformat(),
        },
    )
    db.commit()
    return TriggerTimeAgentActionResult(
        quantity=None,
        limit_price=None,
        result_payload=_monitoring_result_payload(
            item=item,
            observed_price=action.observed_price,
            outcome="blocked",
            block_reason="trigger_time_liquidity_unavailable",
            message=reason,
            trigger_reason=action.trigger_reason,
            trigger_metadata=action.trigger_metadata,
        ),
    )


def _resolve_trigger_time_quality_gate_delay(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    action: PendingMonitoringAction,
    triggered_at: datetime,
    cycle_started: float,
) -> TriggerTimeAgentActionResult | None:
    market_context, stale_reason = trigger_time_exec_agent._market_context(
        settings=settings,
        item=action.item,
        observed_price=action.observed_price,
        observed_at=_price_observed_at_from_action(action=action, fallback=triggered_at),
        triggered_at=triggered_at,
        trigger_reason=action.trigger_reason,
        trigger_metadata=action.trigger_metadata,
        live_market_context=action.market_quality_context,
    )
    if stale_reason is None and market_context.get("live_execution_context_available") is True:
        return None
    return _resolve_trigger_time_agent_action(
        settings=settings,
        db=db,
        adapter=adapter,
        action=action,
        triggered_at=triggered_at,
        cycle_started=cycle_started,
        intended_quantity=None,
        remaining_quantity=None,
    )


def _price_observed_at_from_action(
    *,
    action: PendingMonitoringAction,
    fallback: datetime,
) -> datetime | None:
    if action.observed_price is None:
        return None
    return action.observed_at or fallback


def _market_quality_context_from_price(
    price: MonitoringPriceRequest | None,
    *,
    item: ExecutionPlanItem | None = None,
) -> dict[str, object] | None:
    if price is None:
        return None
    provenance = dict(price.provenance or {})
    session_type = price.session_type
    normal_ordering_available = price.normal_ordering_available
    if item is not None:
        orderability_by_item = provenance.get("orderability_by_item")
        purpose = "protective_stop" if item.intent == "stop_sell" else item.intent
        orderability_key = monitoring_orderability_key(
            side=item.side,
            purpose=purpose,
        )
        selected_orderability = (
            orderability_by_item.get(orderability_key)
            if isinstance(orderability_by_item, dict)
            else None
        )
        if isinstance(selected_orderability, dict):
            selected_session = selected_orderability.get("session_type")
            if isinstance(selected_session, str) and selected_session.strip():
                session_type = selected_session
            route_available = selected_orderability.get(
                "normal_ordering_available"
            )
            broker_orderability = provenance.get("broker_orderability")
            broker_available = (
                broker_orderability.get("normal_ordering_available")
                if isinstance(broker_orderability, dict)
                else None
            )
            if isinstance(route_available, bool):
                normal_ordering_available = (
                    False if broker_available is False else route_available
                )
            provenance["selected_orderability"] = {
                "key": orderability_key,
                **selected_orderability,
            }
    spread_bps = price.spread_bps
    if (
        spread_bps is None
        and price.bid is not None
        and price.ask is not None
        and price.ask >= price.bid
    ):
        midpoint = (price.ask + price.bid) / Decimal("2")
        if midpoint > 0:
            spread_bps = (price.ask - price.bid) / midpoint * Decimal("10000")
    return {
        "source": "monitoring_price_request",
        "symbol": price.symbol,
        "observed_at": (
            price.observed_at.isoformat() if price.observed_at is not None else None
        ),
        "freshness_source": price.freshness_source,
        "bid": _serialize_decimal(price.bid),
        "ask": _serialize_decimal(price.ask),
        "spread": _serialize_decimal(price.spread),
        "spread_bps": _serialize_decimal(spread_bps),
        "orderbook_depth": _serialize_decimal(price.orderbook_depth),
        "orderbook_imbalance": _serialize_decimal(price.orderbook_imbalance),
        "volume": _serialize_decimal(price.volume),
        "turnover": _serialize_decimal(price.turnover),
        "day_high": _serialize_decimal(price.day_high),
        "day_low": _serialize_decimal(price.day_low),
        "session_type": session_type,
        "normal_ordering_available": normal_ordering_available,
        "provenance": provenance,
    }


def _refresh_trigger_time_market_price(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    reference_at: datetime,
    trigger_liquidity_price: MonitoringPriceRequest | None,
) -> tuple[MonitoringPriceRequest | None, str | None]:
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="trigger_time_exec_agent_post_approval",
            reason=f"refresh broker market context for {item.side} {item.symbol}",
        )
        access_token = session_state.access_token or ""
        quote = adapter.get_current_price(
            access_token,
            symbol=item.symbol,
            exchange=_plan_item_exchange(item),
        )
        if quote is None:
            return None, "broker current-price endpoint returned no quote"
        if quote.symbol.strip().upper() != item.symbol.strip().upper():
            return None, (
                "broker current-price symbol mismatch: "
                f"expected {item.symbol}, received {quote.symbol}"
            )
        context, context_failure = _fetch_live_market_context(
            broker_adapter=adapter,
            access_token=access_token,
            quote=quote,
        )
        price = monitoring_price_from_broker_quote(
            settings=settings,
            quote=quote,
            context=context,
            context_failure=context_failure,
            side=item.side,
            purpose="protective_stop" if item.intent == "stop_sell" else item.intent,
        )
        if _requires_trigger_time_liquidity_snapshot(item):
            if trigger_liquidity_price is None:
                return None, "timestamped trigger-time liquidity evidence is unavailable"
            price = _copy_trigger_time_liquidity_fields(
                destination=price,
                source=trigger_liquidity_price,
            )
        return price, None
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"


def _requires_trigger_time_liquidity_snapshot(item: ExecutionPlanItem) -> bool:
    if item.side == "buy" and is_short_term_watch_buy_intent(item.intent):
        return True
    for context in (_loaded_risk_context(item), item.quantity_policy):
        if not isinstance(context, dict):
            continue
        codes = context.get("delegated_gate_codes")
        if isinstance(codes, list) and DELEGATED_LIQUIDITY_GATE_CODE in codes:
            return True
    return False


def _minimum_reward_risk_for_watch_buy(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> Decimal:
    configured = Decimal(str(settings.exec_agent_min_reward_risk))
    if settings.runtime_mode == "live" and is_short_term_watch_buy_intent(item.intent):
        return max(configured, SHORT_TERM_LIVE_MIN_NET_REWARD_RISK)
    return configured


def _merge_trigger_time_liquidity_snapshot(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    access_token: str,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest,
    reference_at: datetime,
) -> tuple[MonitoringPriceRequest, str | None]:
    loader = getattr(adapter, "get_intraday_liquidity_snapshot", None)
    if not callable(loader):
        return price, "timestamped intraday liquidity endpoint is unavailable"
    try:
        snapshot = loader(
            access_token,
            symbol=item.symbol,
            exchange=_plan_item_exchange(item),
            reference_at=reference_at,
        )
    except Exception as exc:
        return price, f"intraday liquidity refresh failed: {exc.__class__.__name__}"
    reason = _trigger_time_liquidity_snapshot_reason(
        settings=settings,
        adapter=adapter,
        item=item,
        snapshot=snapshot,
        reference_at=reference_at,
    )
    if reason is not None:
        return price, reason
    assert isinstance(snapshot, BrokerIntradayLiquiditySnapshot)
    observed_at = _normalize_utc(snapshot.observed_at)
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    field_source = {
        "source": "broker_intraday_liquidity_snapshot",
        "freshness_source": snapshot.freshness_source,
        "observed_at": observed_at.isoformat(),
        "endpoint": payload.get("endpoint"),
        "tr_id": payload.get("tr_id"),
    }
    provenance = dict(price.provenance or {})
    field_sources = dict(provenance.get("field_sources") or {})
    for field in ("volume", "turnover", "day_high", "day_low"):
        field_sources[field] = dict(field_source)
    provenance["field_sources"] = field_sources
    provenance["intraday_liquidity"] = {
        **field_source,
        "symbol": snapshot.symbol,
        "exchange": snapshot.exchange,
    }
    return (
        price.model_copy(
            update={
                "volume": snapshot.volume,
                "turnover": snapshot.turnover,
                "day_high": snapshot.day_high,
                "day_low": snapshot.day_low,
                "provenance": provenance,
            }
        ),
        None,
    )


def _copy_trigger_time_liquidity_fields(
    *,
    destination: MonitoringPriceRequest,
    source: MonitoringPriceRequest,
) -> MonitoringPriceRequest:
    source_provenance = source.provenance if isinstance(source.provenance, dict) else {}
    source_fields = source_provenance.get("field_sources")
    intraday = source_provenance.get("intraday_liquidity")
    if not isinstance(source_fields, dict) or not isinstance(intraday, dict):
        return destination
    destination_provenance = dict(destination.provenance or {})
    destination_fields = dict(destination_provenance.get("field_sources") or {})
    for field in ("volume", "turnover", "day_high", "day_low"):
        field_source = source_fields.get(field)
        if isinstance(field_source, dict):
            destination_fields[field] = dict(field_source)
    destination_provenance["field_sources"] = destination_fields
    destination_provenance["intraday_liquidity"] = dict(intraday)
    return destination.model_copy(
        update={
            "volume": source.volume,
            "turnover": source.turnover,
            "day_high": source.day_high,
            "day_low": source.day_low,
            "provenance": destination_provenance,
        }
    )


def _trigger_time_liquidity_snapshot_reason(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    snapshot: object,
    reference_at: datetime,
) -> str | None:
    if not isinstance(snapshot, BrokerIntradayLiquiditySnapshot):
        return "timestamped intraday liquidity endpoint returned no snapshot"
    if str(adapter.provider or "").strip().lower() != "kis":
        return "intraday liquidity provider identity mismatch"
    if snapshot.symbol != item.symbol:
        return "intraday liquidity symbol identity mismatch"
    expected_exchange = _plan_item_exchange(item).strip().upper()
    canonical_exchange = (
        "KRX" if expected_exchange in {"KRX", "KOSPI", "KOSDAQ"} else expected_exchange
    )
    if snapshot.exchange != canonical_exchange:
        return "intraday liquidity exchange identity mismatch"
    if snapshot.freshness_source != "kis_exchange_intraday_minute_time":
        return "intraday liquidity lacks trusted exchange-time provenance"
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    if (
        payload.get("provider") != "kis"
        or payload.get("endpoint")
        != "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        or payload.get("tr_id") != "FHKST03010200"
    ):
        return "intraday liquidity endpoint identity mismatch"
    if snapshot.observed_at.tzinfo is None or snapshot.observed_at.utcoffset() is None:
        return "intraday liquidity timestamp is not timezone-aware"
    observed_at = _normalize_utc(snapshot.observed_at)
    evaluated_at = _normalize_utc(reference_at)
    if observed_at.astimezone(ZoneInfo(settings.market_timezone)).date() != (
        evaluated_at.astimezone(ZoneInfo(settings.market_timezone)).date()
    ):
        return "intraday liquidity trading date mismatch"
    age = evaluated_at - observed_at
    if age > timedelta(seconds=settings.monitoring_max_quote_age_seconds):
        return "intraday liquidity snapshot is stale"
    if age < -timedelta(
        seconds=settings.monitoring_max_quote_future_skew_seconds
    ):
        return "intraday liquidity timestamp exceeds allowed future clock skew"
    values = (snapshot.volume, snapshot.turnover, snapshot.day_high, snapshot.day_low)
    if any(
        (parsed := _parse_decimal(value)) is None or parsed < 0
        for value in values[:2]
    ) or any(
        (parsed := _parse_decimal(value)) is None or parsed <= 0
        for value in values[2:]
    ):
        return "intraday liquidity snapshot contains invalid numeric evidence"
    if snapshot.day_low > snapshot.day_high:
        return "intraday liquidity day range is invalid"
    return None


def _persist_pre_agent_liquidity_evidence(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    action: PendingMonitoringAction,
) -> None:
    if (
        settings.runtime_mode != "live"
        or action.price is None
        or not _requires_trigger_time_liquidity_snapshot(item)
    ):
        return
    _persist_trigger_time_liquidity_evidence(
        item=item,
        price=action.price,
    )


def _persist_trigger_time_liquidity_evidence(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest,
) -> None:
    quantity_policy = (
        dict(item.quantity_policy) if isinstance(item.quantity_policy, dict) else {}
    )
    provenance = price.provenance if isinstance(price.provenance, dict) else {}
    field_sources = provenance.get("field_sources")
    turnover_source = (
        field_sources.get("turnover") if isinstance(field_sources, dict) else None
    )
    if not isinstance(turnover_source, dict):
        return
    quantity_policy[TRIGGER_TIME_LIQUIDITY_EVIDENCE_KEY] = {
        "version": 1,
        "provider": str(item.provider or "kis").strip().lower(),
        "asset_class": str(item.asset_class or "stock").strip().lower(),
        "exchange": _plan_item_exchange(item).strip().upper(),
        "symbol": item.symbol,
        "observed_at": turnover_source.get("observed_at"),
        "freshness_source": turnover_source.get("freshness_source"),
        "endpoint": turnover_source.get("endpoint"),
        "tr_id": turnover_source.get("tr_id"),
        "volume": _serialize_decimal(price.volume),
        "turnover": _serialize_decimal(price.turnover),
        "day_high": _serialize_decimal(price.day_high),
        "day_low": _serialize_decimal(price.day_low),
    }
    item.quantity_policy = compact_json_object(quantity_policy)


def _persist_trigger_time_full_cost_economics(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    economics: BuyEconomicsAssessment,
    evaluated_at: datetime,
) -> None:
    risk_context = dict(_loaded_risk_context(item))
    evidence = {
        "version": 1,
        "symbol": item.symbol,
        "evaluated_at": _normalize_utc(evaluated_at).isoformat(),
        "gross_reward_risk": _serialize_decimal(economics.gross_reward_risk),
        "net_reward_risk": _serialize_decimal(economics.net_reward_risk),
        "minimum_reward_risk": _serialize_decimal(
            economics.minimum_reward_risk
        ),
        "estimated_round_trip_cost_bps": _serialize_decimal(
            economics.estimated_round_trip_cost_bps
        ),
        "cost_evidence_source": economics.cost_evidence_source,
        "assessment_reason": economics.reason,
    }
    provenance = _trigger_time_full_cost_economics_provenance(
        settings=settings,
        item=item,
        evidence=evidence,
    )
    if provenance is not None:
        evidence["provenance"] = provenance
    risk_context[TRIGGER_TIME_FULL_COST_ECONOMICS_KEY] = evidence
    item.risk_context = compact_json_object(risk_context)
    if isinstance(item.quantity_policy, dict):
        item.quantity_policy = compact_json_object(item.quantity_policy)


def _post_agent_market_quality_reason(
    *,
    settings: Settings,
    price: MonitoringPriceRequest,
    evaluated_at: datetime,
    side: str,
) -> str | None:
    if price.bid is not None and price.ask is not None and price.bid > price.ask:
        return "fresh broker quote has a crossed bid/ask spread"
    if price.orderbook_depth is not None and price.orderbook_depth <= 0:
        return "fresh broker orderbook has no positive displayed depth"
    provenance = price.provenance if isinstance(price.provenance, dict) else {}
    orderbook = provenance.get("orderbook")
    if not isinstance(orderbook, dict):
        return "fresh broker orderbook provenance is unavailable"
    if orderbook.get("status") == "failed":
        return f"fresh broker orderbook refresh failed: {orderbook.get('reason') or 'unknown'}"
    if not _is_trusted_exchange_freshness_source(
        orderbook.get("freshness_source")
    ):
        return "fresh broker orderbook lacks trusted exchange-time provenance"
    observed_at = _parse_event_datetime(orderbook.get("observed_at"))
    if observed_at is None:
        return "fresh broker orderbook timestamp is unavailable"
    normalized_side = side.strip().lower()
    executable_field = "ask" if normalized_side == "buy" else "bid"
    executable_price = getattr(price, executable_field)
    if executable_price is None or executable_price <= 0:
        return f"fresh broker quote lacks a positive executable {executable_field}"
    field_sources = provenance.get("field_sources")
    field_source = (
        field_sources.get(executable_field)
        if isinstance(field_sources, dict)
        else None
    )
    if not isinstance(field_source, dict) or not _is_trusted_exchange_freshness_source(
        field_source.get("freshness_source")
    ):
        return (
            f"fresh broker {executable_field} lacks trusted field-level "
            "exchange-time provenance"
        )
    field_observed_at = _parse_event_datetime(field_source.get("observed_at"))
    if field_observed_at is None:
        return f"fresh broker {executable_field} timestamp is unavailable"
    if _normalize_utc(field_observed_at) != _normalize_utc(observed_at):
        return (
            f"fresh broker {executable_field} timestamp does not match the "
            "orderbook snapshot"
        )
    normalized_evaluated_at = _normalize_utc(evaluated_at)
    future_skew = observed_at - normalized_evaluated_at
    if future_skew > timedelta(seconds=settings.monitoring_max_quote_future_skew_seconds):
        return "fresh broker orderbook timestamp exceeds the allowed future clock skew"
    age = normalized_evaluated_at - observed_at
    if age > timedelta(seconds=settings.monitoring_max_quote_age_seconds):
        return (
            "fresh broker orderbook is stale: "
            f"{age.total_seconds():.3f}s>{settings.monitoring_max_quote_age_seconds}s"
        )
    return None


def _post_agent_trigger_still_valid(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest,
    decision: trigger_time_exec_agent.TriggerTimeExecAgentDecision,
    triggered_at: datetime,
) -> bool:
    if _is_triggered(
        item=item,
        observed_price=_observed_price_for_item(price=price, item=item),
        triggered_at=triggered_at,
    ):
        return True
    if not _is_watch_buy_item(item):
        return False
    if _watch_buy_is_pullback_strategy(item):
        return _persisted_watch_buy_trigger_cross(item) is not None
    updated_trigger = decision.updated_trigger_price
    if (
        updated_trigger is None
        or decision.updated_target_price is None
        or decision.updated_stop_price is None
    ):
        return False
    rationale = f"{decision.reason} {' '.join(decision.risk_notes)}".lower()
    if "pullback" not in rationale and "reclaim" not in rationale:
        return False
    if _watch_buy_direction(item) == "at_or_below":
        observed_price = _observed_price_for_item(price=price, item=item)
        return observed_price is not None and observed_price <= updated_trigger
    observed_price = _observed_price_for_item(price=price, item=item)
    return observed_price is not None and observed_price >= updated_trigger


def _post_agent_revalidation_failure(
    *,
    settings: Settings,
    db: Session,
    claim: TriggerTimeAgentClaim,
    item: ExecutionPlanItem,
    action: PendingMonitoringAction,
    resolution: trigger_time_exec_agent.TriggerTimeExecAgentResolution,
    previous_status: str,
    recorded_at: datetime,
    reason: str,
    message: str,
    outcome: str,
    new_status: str,
    refreshed_price: MonitoringPriceRequest | None = None,
    market_quality_gate: object | None = None,
    liquidity_evidence_price: MonitoringPriceRequest | None = None,
    economics: BuyEconomicsAssessment | None = None,
) -> TriggerTimeAgentActionResult:
    if not _trigger_time_agent_claim_is_current(db=db, item=item, claim=claim):
        return _trigger_time_agent_claim_lost_result(item=item, action=action)
    if (
        liquidity_evidence_price is not None
        and _requires_trigger_time_liquidity_snapshot(item)
    ):
        _persist_trigger_time_liquidity_evidence(
            item=item,
            price=liquidity_evidence_price,
        )
    if economics is not None:
        _persist_trigger_time_full_cost_economics(
            settings=settings,
            item=item,
            economics=economics,
            evaluated_at=recorded_at,
        )
    item.status = new_status
    observed_price = (
        _observed_price_for_item(price=refreshed_price, item=item)
        if refreshed_price is not None
        else action.observed_price
    )
    event: dict[str, object] = {
        "event": "trigger_time_exec_agent_post_approval_revalidation_failed",
        "reason": reason,
        "message": message,
        "initial_observed_price": _serialize_decimal(action.observed_price),
        "observed_price": _serialize_decimal(observed_price),
        "observed_at": _serialize_datetime(
            refreshed_price.observed_at if refreshed_price is not None else None
        ),
        "recorded_at": recorded_at.isoformat(),
    }
    if market_quality_gate is not None:
        event["market_quality_gate"] = market_quality_gate
    _record_monitoring_event(item=item, event=event)
    trigger_time_exec_agent.record_trigger_time_exec_agent_interaction(
        item=item,
        resolution=resolution,
        previous_status=previous_status,
        new_status=item.status,
        recorded_at=recorded_at,
    )
    db.commit()
    return TriggerTimeAgentActionResult(
        quantity=None,
        limit_price=None,
        result_payload=_monitoring_result_payload(
            item=item,
            observed_price=observed_price,
            outcome=outcome,
            block_reason=reason,
            message=message,
            trigger_reason=action.trigger_reason,
            trigger_metadata=action.trigger_metadata,
        ),
        refreshed_price=refreshed_price,
    )


def _action_with_refreshed_price(
    *,
    action: PendingMonitoringAction,
    price: MonitoringPriceRequest,
) -> PendingMonitoringAction:
    return PendingMonitoringAction(
        item=action.item,
        observed_price=_observed_price_for_item(price=price, item=action.item),
        price=price,
        observed_at=price.observed_at,
        market_quality_context=_market_quality_context_from_price(
            price,
            item=action.item,
        ),
        trigger_reason=action.trigger_reason,
        trigger_metadata=action.trigger_metadata,
    )


def _trigger_time_market_quality_gate(
    resolution: trigger_time_exec_agent.TriggerTimeExecAgentResolution,
) -> dict[str, object] | None:
    market_context = resolution.request_payload.get("market_context")
    if not isinstance(market_context, dict):
        return None
    gate = market_context.get("live_execution_quality_gate")
    return gate if isinstance(gate, dict) else None


def _apply_trigger_time_agent_adjustments(
    *,
    item: ExecutionPlanItem,
    decision: trigger_time_exec_agent.TriggerTimeExecAgentDecision,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    quantity_policy = dict(item.quantity_policy) if isinstance(item.quantity_policy, dict) else None
    enforced_stop = _high_volatility_chase_override_stop_to_enforce(item=item, decision=decision)
    agent_updated_stop = decision.updated_stop_price
    if enforced_stop is not None:
        decision.updated_stop_price = enforced_stop
    if decision.limit_price is not None:
        item.limit_price = decision.limit_price
        risk_context["trigger_time_limit_price"] = _serialize_decimal(decision.limit_price)
    if decision.updated_target_price is not None:
        risk_context["target_price"] = _serialize_decimal(decision.updated_target_price)
        risk_context["updated_target_price"] = _serialize_decimal(decision.updated_target_price)
        if quantity_policy is not None:
            quantity_policy["target_price"] = _serialize_decimal(decision.updated_target_price)
            quantity_policy["updated_target_price"] = _serialize_decimal(decision.updated_target_price)
        _update_sibling_trigger_price(
            item=item,
            intent="target_sell",
            trigger_price=decision.updated_target_price,
        )
    stop_price = enforced_stop or decision.updated_stop_price
    if stop_price is not None:
        risk_context["stop_price"] = _serialize_decimal(stop_price)
        risk_context["stop_loss_price"] = _serialize_decimal(stop_price)
        risk_context["updated_stop_price"] = _serialize_decimal(stop_price)
        if quantity_policy is not None:
            quantity_policy["stop_price"] = _serialize_decimal(stop_price)
            quantity_policy["stop_loss_price"] = _serialize_decimal(stop_price)
            quantity_policy["updated_stop_price"] = _serialize_decimal(stop_price)
        if enforced_stop is not None:
            risk_context["trigger_time_override_stop_enforcement"] = {
                "reason": "high_volatility_chase_override_stop_preserved",
                "agent_updated_stop_price": _serialize_decimal(agent_updated_stop),
                "enforced_stop_price": _serialize_decimal(enforced_stop),
            }
        _update_sibling_trigger_price(
            item=item,
            intent="stop_sell",
            trigger_price=stop_price,
        )
    item.risk_context = compact_json_object(risk_context)
    if quantity_policy is not None:
        item.quantity_policy = compact_json_object(quantity_policy)


def _high_volatility_chase_override_stop_to_enforce(
    *,
    item: ExecutionPlanItem,
    decision: trigger_time_exec_agent.TriggerTimeExecAgentDecision,
) -> Decimal | None:
    if item.side != "buy" or decision.decision != "execute_now":
        return None
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    high_volatility_decision = risk_context.get("last_high_volatility_chase_decision")
    if not isinstance(high_volatility_decision, dict) or high_volatility_decision.get("outcome") != "override":
        return None
    override = high_volatility_decision.get("override")
    if not isinstance(override, dict):
        return None
    override_stop = _parse_positive_decimal(override.get("tighter_stop_price"))
    if override_stop is None:
        return None
    if decision.updated_stop_price is None or decision.updated_stop_price >= override_stop:
        return None
    return override_stop


def _record_trigger_time_watch_buy_sizing_result(
    *,
    item: ExecutionPlanItem,
    approval: WatchBuyOrderApproval,
    requested_quantity: int,
    recorded_at: datetime,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return

    if approval.quantity is not None:
        outcome = "approved"
    elif approval.reason in ({"paused", "kill_switch"} | RETRYABLE_WATCH_BUY_APPROVAL_REASONS):
        outcome = "blocked"
    else:
        outcome = "rejected"

    deterministic_sizing: dict[str, object] = {
        "source": "watch_buy_deterministic_sizing",
        "outcome": outcome,
        "requested_quantity": requested_quantity,
        "approved_quantity": approval.quantity,
        "reason": approval.reason,
        "message": approval.message,
        "details": approval.details or {},
        "recorded_at": recorded_at.isoformat(),
    }
    if approval.quantity is not None and approval.quantity < requested_quantity:
        deterministic_sizing["adjustment"] = "clamped_by_deterministic_risk_sizing"

    updated_interaction = {
        **interaction,
        "deterministic_sizing": deterministic_sizing,
        "new_monitoring_state": item.status,
    }
    interactions = risk_context.get("trigger_time_exec_agent_interactions")
    if isinstance(interactions, list) and interactions:
        interactions = [*interactions[:-1], updated_interaction]
    else:
        interactions = [updated_interaction]
    risk_context["last_trigger_time_exec_agent_interaction"] = compact_json_object(updated_interaction)
    risk_context["trigger_time_exec_agent_interactions"] = [
        compact_json_object(interaction, max_depth=3, max_list_items=5)
        for interaction in interactions[-3:]
    ]
    item.risk_context = compact_json_object(risk_context)


def _update_sibling_trigger_price(
    *,
    item: ExecutionPlanItem,
    intent: str,
    trigger_price: Decimal,
) -> None:
    plan = item.plan
    if plan is None:
        return
    candidates = [
        sibling
        for sibling in plan.items
        if sibling.symbol == item.symbol
        and sibling.intent == intent
        and sibling.status not in NON_MONITORABLE_PLAN_ITEM_STATUSES
    ]
    for sibling in select_plan_item_family_members(
        anchor=item,
        candidates=candidates,
    ):
        sibling.trigger_price = trigger_price


def _load_monitored_items(
    *,
    settings: Settings,
    db: Session,
    exclude_pending_config: bool = False,
    symbols: list[str] | set[str] | None = None,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
) -> list[ExecutionPlanItem]:
    resolved_effective_at = _normalize_utc(effective_at or datetime.now(UTC))
    use_effective_at = effective_at is not None or current_market_date is None
    resolved_market_date = current_market_date or resolved_effective_at.astimezone(
        KRX_TIMEZONE
    ).date()
    provider_filter = _kis_plan_item_provider_filter()
    effective_plan_filter = _effective_monitor_plan_filter(
        effective_at=resolved_effective_at if use_effective_at else None,
        current_market_date=current_market_date,
    )
    monitored_item_filter = and_(
        _monitored_item_filter(),
        provider_filter,
        effective_plan_filter,
    )
    if symbols:
        monitored_item_filter = and_(
            monitored_item_filter,
            ExecutionPlanItem.symbol.in_(sorted(symbols)),
        )
    route_candidate_where = [
        ExecutionPlan.mode == settings.runtime_mode,
        ExecutionPlanItem.side == "sell",
        ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
        ExecutionPlanItem.status.in_(tuple(LOADABLE_SELL_MONITORING_STATUSES)),
        provider_filter,
        effective_plan_filter,
    ]
    if symbols:
        route_candidate_where.append(
            ExecutionPlanItem.symbol.in_(_ordered_symbol_scope(symbols))
        )
    route_candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(*route_candidate_where)
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
            )
        )
    ).all()
    if use_effective_at:
        route_candidates = [
            item
            for item in route_candidates
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=resolved_effective_at,
            )
        ]
    eligible_route_candidates = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=route_candidates,
        positions_by_symbol=_load_positions_by_symbol(
            settings=settings,
            db=db,
            symbols=symbols,
        ),
        recorded_at=datetime.now(UTC),
    )
    if len(eligible_route_candidates) != len(route_candidates):
        db.flush()
    if exclude_pending_config:
        held_symbols = select(ExecutionPosition.symbol).where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.quantity > 0,
        )
        if symbols:
            held_symbols = held_symbols.where(ExecutionPosition.symbol.in_(sorted(symbols)))
        sell_symbol_conditions = [
            ExecutionPlanItem.symbol.in_(held_symbols),
            _unexited_buy_sell_monitor_fallback_filter(settings=settings),
        ]
        monitored_item_filter = and_(
            monitored_item_filter,
            or_(
                ExecutionPlanItem.side != "sell",
                or_(*sell_symbol_conditions),
            ),
        )
    if symbols:
        symbol_list = _ordered_symbol_scope(symbols)
        sell_fast_statuses = ACTIVE_SELL_MONITORING_STATUSES
        if exclude_pending_config:
            sell_fast_statuses = sell_fast_statuses | {
                "max_holding_close_sell_pending_config"
            }
        if not exclude_pending_config:
            sell_fast_statuses = sell_fast_statuses | SELL_PENDING_CONFIG_STATUSES
        sell_load_options = (
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.decision_snapshot_id,
                ExecutionPlanItem.decision_run_id,
                ExecutionPlanItem.source_plan_item_id,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
            ),
        )
        if exclude_pending_config:
            sell_items = []
            for symbol in symbol_list:
                sell_items.extend(
                    db.scalars(
                        select(ExecutionPlanItem)
                        .join(ExecutionPlanItem.plan)
                        .where(
                            ExecutionPlan.mode == settings.runtime_mode,
                            ExecutionPlanItem.symbol == symbol,
                            ExecutionPlanItem.side == "sell",
                            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                            ExecutionPlanItem.status.in_(tuple(sell_fast_statuses)),
                            provider_filter,
                            effective_plan_filter,
                        )
                        .options(
                            *sell_load_options,
                            selectinload(ExecutionPlanItem.plan).load_only(
                                ExecutionPlan.id,
                                ExecutionPlan.mode,
                                ExecutionPlan.market_date,
                                ExecutionPlan.requested_at,
                                ExecutionPlan.accepted_at,
                            ),
                        )
                        .order_by(
                            ExecutionPlanItem.plan_item_index.asc(),
                            ExecutionPlanItem.id.asc(),
                        )
                    ).all()
                )
            buy_items = db.scalars(
                select(ExecutionPlanItem)
                .join(ExecutionPlanItem.plan)
                .where(
                    ExecutionPlan.mode == settings.runtime_mode,
                    ExecutionPlanItem.symbol.in_(symbol_list),
                    ExecutionPlanItem.side == "buy",
                    ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                    ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
                    provider_filter,
                    effective_plan_filter,
                )
                .options(
                    load_only(
                        ExecutionPlanItem.id,
                        ExecutionPlanItem.execution_plan_id,
                        ExecutionPlanItem.plan_item_index,
                        ExecutionPlanItem.symbol,
                        ExecutionPlanItem.side,
                        ExecutionPlanItem.intent,
                        ExecutionPlanItem.status,
                        ExecutionPlanItem.quantity_policy,
                        ExecutionPlanItem.limit_price,
                        ExecutionPlanItem.trigger_price,
                        ExecutionPlanItem.time_in_force,
                        ExecutionPlanItem.risk_context,
                        ExecutionPlanItem.decision_agent_name,
                        ExecutionPlanItem.decision_snapshot_id,
                        ExecutionPlanItem.decision_run_id,
                        ExecutionPlanItem.source_plan_item_id,
                        ExecutionPlanItem.source_market_date,
                        ExecutionPlanItem.provider,
                        ExecutionPlanItem.asset_class,
                    ),
                    selectinload(ExecutionPlanItem.plan).load_only(
                        ExecutionPlan.id,
                        ExecutionPlan.mode,
                        ExecutionPlan.market_date,
                        ExecutionPlan.requested_at,
                        ExecutionPlan.accepted_at,
                    ),
                )
                .order_by(
                    ExecutionPlanItem.symbol.asc(),
                    ExecutionPlanItem.plan_item_index.asc(),
                    ExecutionPlanItem.id.asc(),
                )
            ).all()
            symbol_order = {symbol: index for index, symbol in enumerate(symbol_list)}
            return _canonicalize_loaded_active_watch_buys(
                sorted(
                    [*sell_items, *buy_items],
                    key=lambda item: (
                        symbol_order.get(item.symbol, len(symbol_order)),
                        item.plan_item_index,
                        item.id,
                    ),
                ),
                settings=settings,
                current_market_date=resolved_market_date,
                effective_at=resolved_effective_at if use_effective_at else None,
            )
        sell_items = db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.symbol.in_(symbol_list),
                ExecutionPlanItem.side == "sell",
                ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(sell_fast_statuses)),
                provider_filter,
                effective_plan_filter,
            )
            .options(
                *sell_load_options,
                selectinload(ExecutionPlanItem.plan).load_only(
                    ExecutionPlan.id,
                    ExecutionPlan.mode,
                    ExecutionPlan.market_date,
                    ExecutionPlan.requested_at,
                    ExecutionPlan.accepted_at,
                )
            )
            .order_by(
                ExecutionPlanItem.symbol.asc(),
                ExecutionPlanItem.plan_item_index.asc(),
                ExecutionPlanItem.id.asc(),
            )
            .limit(settings.monitoring_max_symbols_per_cycle * 16)
        ).all()
        buy_items = db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.symbol.in_(symbol_list),
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
                ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
                provider_filter,
                effective_plan_filter,
            )
            .options(
                load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.execution_plan_id,
                    ExecutionPlanItem.plan_item_index,
                    ExecutionPlanItem.symbol,
                    ExecutionPlanItem.side,
                    ExecutionPlanItem.intent,
                    ExecutionPlanItem.status,
                    ExecutionPlanItem.quantity_policy,
                    ExecutionPlanItem.limit_price,
                    ExecutionPlanItem.trigger_price,
                    ExecutionPlanItem.time_in_force,
                    ExecutionPlanItem.risk_context,
                    ExecutionPlanItem.decision_agent_name,
                    ExecutionPlanItem.source_market_date,
                    ExecutionPlanItem.provider,
                    ExecutionPlanItem.asset_class,
                ),
                selectinload(ExecutionPlanItem.plan).load_only(
                    ExecutionPlan.id,
                    ExecutionPlan.mode,
                    ExecutionPlan.market_date,
                    ExecutionPlan.requested_at,
                    ExecutionPlan.accepted_at,
                )
            )
            .order_by(
                ExecutionPlanItem.symbol.asc(),
                ExecutionPlanItem.plan_item_index.asc(),
                ExecutionPlanItem.id.asc(),
            )
        ).all()
        symbol_order = {symbol: index for index, symbol in enumerate(symbol_list)}
        selected_items = sorted(
            [*sell_items, *buy_items],
            key=lambda item: (
                symbol_order.get(item.symbol, len(symbol_order)),
                item.plan_item_index,
                item.id,
            ),
        )
        return _canonicalize_loaded_active_watch_buys(
            selected_items,
            settings=settings,
            current_market_date=resolved_market_date,
            effective_at=resolved_effective_at if use_effective_at else None,
        )
    latest_plan_candidates = (
        select(
            ExecutionPlanItem.symbol.label("symbol"),
            ExecutionPlan.id.label("plan_id"),
            ExecutionPlan.accepted_at.label("accepted_at"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlan.market_date.desc(),
                    ExecutionPlan.requested_at.desc().nullslast(),
                    ExecutionPlan.accepted_at.desc(),
                    ExecutionPlan.id.desc(),
                ),
            )
            .label("symbol_rank"),
        )
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_item_filter,
            effective_plan_filter,
        )
        .subquery()
    )
    # Bound the working set by distinct symbol, and only evaluate the latest
    # monitorable plan for each symbol so stale prior-day target/stop rows do
    # not submit duplicate exits after a newer plan supersedes them.
    selected_symbol_plans = (
        select(
            latest_plan_candidates.c.symbol,
            latest_plan_candidates.c.plan_id,
        )
        .where(latest_plan_candidates.c.symbol_rank == 1)
        .order_by(
            latest_plan_candidates.c.accepted_at.asc(),
            latest_plan_candidates.c.plan_id.asc(),
        )
        .limit(settings.monitoring_max_symbols_per_cycle)
    )
    selected_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            monitored_item_filter,
            tuple_(ExecutionPlanItem.symbol, ExecutionPlanItem.execution_plan_id).in_(selected_symbol_plans),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    selected_items = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=selected_items,
        symbols=symbols,
        current_market_date=resolved_market_date,
        effective_at=resolved_effective_at if use_effective_at else None,
    )
    selected_items = _append_held_stop_sell_fallbacks(
        settings=settings,
        db=db,
        monitored_item_filter=monitored_item_filter,
        selected_items=selected_items,
        symbols=symbols,
        current_market_date=resolved_market_date,
        effective_at=resolved_effective_at if use_effective_at else None,
    )
    return _canonicalize_loaded_active_watch_buys(
        selected_items,
        settings=settings,
        current_market_date=resolved_market_date,
        effective_at=resolved_effective_at if use_effective_at else None,
    )


def _canonicalize_loaded_active_watch_buys(
    items: list[ExecutionPlanItem],
    *,
    settings: Settings,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
) -> list[ExecutionPlanItem]:
    items = [
        item
        for item in items
        if not (
            item.side == "buy"
            and item.intent in WATCH_BUY_INTENTS
            and is_exec_agent_sizing_policy(item.quantity_policy)
        )
    ]
    resolved_market_date = current_market_date or datetime.now(KRX_TIMEZONE).date()
    if effective_at is not None:
        items = [
            item
            for item in items
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=effective_at,
            )
        ]
    else:
        items = [
            item
            for item in items
            if not (
                item.plan.market_date is not None
                and item.plan.market_date > resolved_market_date
            )
            and not (
                item.side == "buy"
                and item.intent in WATCH_BUY_INTENTS
                and item.plan.market_date is None
            )
        ]
    items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=items,
        recorded_at=datetime.now(UTC),
    )
    items = _quarantine_invalid_kis_watch_routes(
        settings=settings,
        items=items,
        recorded_at=datetime.now(UTC),
    )
    items = _canonicalize_loaded_sell_monitors(items)
    canonical_by_route: dict[tuple[str, str, str, str], ExecutionPlanItem] = {}
    for item in items:
        if (
            item.side != "buy"
            or item.intent not in WATCH_BUY_INTENTS
            or item.status not in ACTIVE_WATCH_MONITORING_STATUSES
        ):
            continue
        route_key = _active_watch_route_key(item)
        current = canonical_by_route.get(route_key)
        if (
            current is None
            or _active_watch_recency_key(item) > _active_watch_recency_key(current)
        ):
            canonical_by_route[route_key] = item
    canonical_ids = {item.id for item in canonical_by_route.values()}
    return [
        item
        for item in items
        if item.side != "buy"
        or item.intent not in WATCH_BUY_INTENTS
        or item.status not in ACTIVE_WATCH_MONITORING_STATUSES
        or item.id in canonical_ids
    ]


def _canonicalize_loaded_sell_monitors(
    items: list[ExecutionPlanItem],
) -> list[ExecutionPlanItem]:
    canonical_plan_by_authority: dict[
        tuple[str, str, str, tuple[str, str, str, str]],
        ExecutionPlanItem,
    ] = {}
    for item in items:
        if item.side != "sell" or item.intent not in SELL_MONITORED_INTENTS:
            continue
        family_key = plan_item_entry_family_key(item)
        if family_key is None and item.source_plan_item_id:
            family_key = f"source:{item.source_plan_item_id}"
        route_key = _sell_monitor_route_key(item)
        if family_key is None or route_key is None:
            # Unlinked legacy protection is intentionally retained; symbol-only
            # deduplication cannot prove that two exits protect the same entry.
            continue
        authority_key = (item.symbol, item.intent, family_key, route_key)
        current = canonical_plan_by_authority.get(authority_key)
        if current is None or _sell_item_is_preferred_authority(item, current):
            canonical_plan_by_authority[authority_key] = item

    retained: list[ExecutionPlanItem] = []
    for item in items:
        if item.side != "sell" or item.intent not in SELL_MONITORED_INTENTS:
            retained.append(item)
            continue
        family_key = plan_item_entry_family_key(item)
        if family_key is None and item.source_plan_item_id:
            family_key = f"source:{item.source_plan_item_id}"
        route_key = _sell_monitor_route_key(item)
        if family_key is None or route_key is None:
            retained.append(item)
            continue
        authority_key = (item.symbol, item.intent, family_key, route_key)
        canonical = canonical_plan_by_authority.get(authority_key)
        if canonical is None or item.execution_plan_id == canonical.execution_plan_id:
            retained.append(item)
            continue
        if _compare_sell_authorities(
            _sell_item_authority(item),
            _sell_item_authority(canonical),
        ) == 0:
            prior_status = item.status
            if _sell_monitor_material(item) == _sell_monitor_material(canonical):
                item.status = "superseded"
                reason = "equal_source_authority_with_identical_material"
            else:
                item.status = "manual_review_required"
                reason = "equal_source_authority_with_divergent_material"
            _record_monitoring_event(
                item=item,
                event={
                    "event": "sell_monitor_authority_conflict_resolved",
                    "reason": reason,
                    "prior_status": prior_status,
                    "preserved_execution_plan_item_id": canonical.id,
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
            )
    return retained


def _record_ambiguous_entry_family_review(
    *,
    item: ExecutionPlanItem,
    event: str,
    competing_item_ids: list[str],
    recorded_at: datetime,
) -> None:
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    last_event = risk_context.get("last_monitoring_event")
    if (
        isinstance(last_event, dict)
        and last_event.get("event") == event
        and last_event.get("reason") == "ambiguous_entry_family"
        and last_event.get("competing_execution_plan_item_ids")
        == competing_item_ids
    ):
        return
    _record_monitoring_event(
        item=item,
        event={
            "event": event,
            "reason": "ambiguous_entry_family",
            "action": "manual_review",
            "competing_execution_plan_item_ids": competing_item_ids,
            "recorded_at": _normalize_utc(recorded_at).isoformat(),
        },
    )


def _quarantine_invalid_kis_watch_routes(
    *,
    settings: Settings,
    items: list[ExecutionPlanItem],
    recorded_at: datetime,
) -> list[ExecutionPlanItem]:
    eligible: list[ExecutionPlanItem] = []
    for item in items:
        if (
            item.side != "buy"
            or item.intent not in WATCH_BUY_INTENTS
            or item.status not in ACTIVE_WATCH_MONITORING_STATUSES
        ):
            eligible.append(item)
            continue
        provider = str(item.provider or "kis").strip().lower()
        if provider in {"korea-investment", "korea_investment"}:
            provider = "kis"
        if provider != "kis":
            # This coordinator owns only KIS. Another provider's active state
            # must remain untouched for its dedicated runtime.
            continue
        if classify_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context=_loaded_risk_context(item, allow_file_backed_load=True),
            quantity_policy=item.quantity_policy,
        ) == "foreign":
            # Another account owns this active entry candidate. Ignore it in
            # the current runtime without destroying the owning route's state.
            continue
        rejection = _kis_monitoring_route_rejection(
            settings=settings,
            item=item,
        )
        if rejection is None:
            eligible.append(item)
            continue
        reason, provided_route_fingerprint = rejection
        prior_status = item.status
        apply_plan_item_status(plan_item=item, status="watch_buy_rejected")
        _record_monitoring_event(
            item=item,
            event={
                "event": "watch_buy_account_route_rejected",
                "reason": reason,
                "prior_status": prior_status,
                "provided_route_fingerprint": provided_route_fingerprint,
                "recorded_at": _normalize_utc(recorded_at).isoformat(),
            },
        )
    return eligible


def _filter_sell_items_to_held_symbols(
    *,
    settings: Settings,
    items: list[ExecutionPlanItem],
    positions_by_symbol: dict[str, ExecutionPosition],
) -> list[ExecutionPlanItem]:
    return [
        item
        for item in items
        if _is_watch_buy_item(item)
        or _position_for_monitoring_item(
            settings=settings,
            item=item,
            positions_by_symbol=positions_by_symbol,
        )
        is not None
    ]


def _prepare_due_max_holding_items(
    *,
    settings: Settings,
    db: Session,
    items: list[ExecutionPlanItem],
    positions_by_symbol: dict[str, ExecutionPosition],
    triggered_at: datetime,
) -> list[ExecutionPlanItem]:
    """Promote one safe due max-holding exit per explicit entry family."""
    candidates_by_symbol: dict[str, list[ExecutionPlanItem]] = {}
    passthrough: list[ExecutionPlanItem] = []
    candidate_statuses = {
        "max_holding_close_sell_pending_config",
        "max_holding_close_sell_monitoring",
    }
    for item in items:
        if (
            item.intent == "max_holding_close_sell"
            and item.status in candidate_statuses
            and _monitoring_item_is_configured(item)
            and _max_holding_exit_due(item=item, triggered_at=triggered_at)
        ):
            candidates_by_symbol.setdefault(item.symbol, []).append(item)
        else:
            passthrough.append(item)

    stale_before = triggered_at - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    for symbol, candidates in sorted(candidates_by_symbol.items()):
        position = positions_by_symbol.get(symbol)
        route_matched_candidates = [
            item
            for item in candidates
            if _position_for_monitoring_item(
                settings=settings,
                item=item,
                positions_by_symbol=positions_by_symbol,
            )
            is not None
        ]
        for item in candidates:
            if item in route_matched_candidates:
                continue
            _record_max_holding_state_event(
                db=db,
                item=item,
                event="max_holding_close_blocked",
                reason="missing_exact_route_position_truth",
                triggered_at=triggered_at,
                position=None,
            )
        candidates = route_matched_candidates
        if not candidates:
            continue
        position_reconciled_at = (
            _normalize_utc(position.last_reconciled_at)
            if position is not None and position.last_reconciled_at is not None
            else None
        )
        fresh = (
            position is not None
            and position.quantity > 0
            and (
                settings.runtime_mode != "live"
                or (
                    position_reconciled_at is not None
                    and position_reconciled_at >= stale_before
                )
            )
        )
        if not fresh:
            for item in candidates:
                _record_max_holding_state_event(
                    db=db,
                    item=item,
                    event="max_holding_close_blocked",
                    reason="stale_or_missing_position_truth",
                    triggered_at=triggered_at,
                    position=position,
                )
            continue

        candidates_by_family: dict[str, list[ExecutionPlanItem]] = {}
        unresolved_candidates: list[ExecutionPlanItem] = []
        for item in candidates:
            family_key = plan_item_entry_family_key(item)
            if family_key is None:
                unresolved_candidates.append(item)
            else:
                candidates_by_family.setdefault(family_key, []).append(item)

        if unresolved_candidates:
            if len(candidates) == 1:
                # Preserve the single-entry legacy contract. There is no
                # competing family that could make the symbol fallback unsafe.
                item = unresolved_candidates[0]
                candidates_by_family[f"legacy-single:{item.id}"] = [item]
            else:
                # Never guess which same-symbol entry an unlinked close owns.
                # Keep its durable status intact and leave a manual-review
                # event instead of superseding or promoting it.
                for item in unresolved_candidates:
                    _record_max_holding_state_event(
                        db=db,
                        item=item,
                        event="max_holding_close_blocked",
                        reason="ambiguous_entry_family",
                        triggered_at=triggered_at,
                        position=position,
                    )

        for family_candidates in candidates_by_family.values():
            family_candidates.sort(key=_max_holding_canonical_rank, reverse=True)
            canonical = family_candidates[0]
            if canonical.status != "max_holding_close_sell_monitoring":
                canonical.status = "max_holding_close_sell_monitoring"
                _record_max_holding_state_event(
                    db=db,
                    item=canonical,
                    event="max_holding_close_promoted",
                    reason="due_with_fresh_sellable_position",
                    triggered_at=triggered_at,
                    position=position,
                )
            passthrough.append(canonical)
            for duplicate in family_candidates[1:]:
                duplicate.status = "superseded"
                _record_max_holding_state_event(
                    db=db,
                    item=duplicate,
                    event="max_holding_close_superseded",
                    reason="newer_canonical_due_exit_selected",
                    triggered_at=triggered_at,
                    position=position,
                    canonical_item_id=canonical.id,
                )
    return passthrough


def _max_holding_canonical_rank(item: ExecutionPlanItem) -> tuple[str, str, str]:
    market_date = item.source_market_date or (item.plan.market_date if item.plan else None)
    accepted_at = item.plan.accepted_at if item.plan else None
    return (
        market_date.isoformat() if market_date is not None else "",
        _normalize_utc(accepted_at).isoformat() if accepted_at is not None else "",
        item.id,
    )


def _record_max_holding_state_event(
    *,
    db: Session,
    item: ExecutionPlanItem,
    event: str,
    reason: str,
    triggered_at: datetime,
    position: ExecutionPosition | None,
    canonical_item_id: str | None = None,
) -> None:
    previous = _loaded_risk_context(item).get("last_monitoring_event")
    position_reconciled_at = (
        _serialize_datetime(position.last_reconciled_at) if position is not None else None
    )
    if (
        isinstance(previous, dict)
        and previous.get("event") == event
        and previous.get("reason") == reason
        and previous.get("position_last_reconciled_at") == position_reconciled_at
        and previous.get("canonical_item_id") == canonical_item_id
    ):
        return
    payload = {
        "event": event,
        "reason": reason,
        "symbol": item.symbol,
        "position_quantity": position.quantity if position is not None else None,
        "position_last_reconciled_at": position_reconciled_at,
        "canonical_item_id": canonical_item_id,
        "recorded_at": triggered_at.isoformat(),
    }
    _record_monitoring_event(item=item, event=payload)
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=event,
            source=MONITORING_WORKER_NAME,
            summary={
                "reason": reason,
                "symbol": item.symbol,
                "canonical_item_id": canonical_item_id,
            },
            payload=payload,
            occurred_at=triggered_at,
        )
    )


def _trigger_time_agent_status_counts(
    *,
    items: list[ExecutionPlanItem],
) -> dict[str, int]:
    counts_by_status: dict[str, int] = {}
    for item in items:
        status = str(item.status)
        counts_by_status[status] = counts_by_status.get(status, 0) + 1
    watching_statuses = (
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "target_sell_monitoring",
        "stop_sell_monitoring",
        "max_holding_close_sell_monitoring",
    )
    approved_submitted_statuses = (
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
        "watch_buy_submitted",
        "broker_order_submission_pending",
        "broker_order_submitted",
        "submitted",
        "open",
        "partially_filled",
    )
    return {
        "watching": sum(counts_by_status.get(status, 0) for status in watching_statuses),
        "pending_agent_decision": counts_by_status.get(AGENT_PENDING_STATUS, 0),
        "agent_delayed": counts_by_status.get(AGENT_DELAY_STATUS, 0),
        "agent_rejected": counts_by_status.get(AGENT_REJECTED_STATUS, 0),
        "agent_approved_submitted": sum(
            counts_by_status.get(status, 0)
            for status in approved_submitted_statuses
        ),
    }


def _append_held_active_sell_monitor_reconciliations(
    *,
    settings: Settings,
    db: Session,
    selected_items: list[ExecutionPlanItem],
    symbols: set[str] | None = None,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
) -> list[ExecutionPlanItem]:
    resolved_market_date = current_market_date or datetime.now(KRX_TIMEZONE).date()
    selected_items = [
        item
        for item in selected_items
        if (
            _monitoring_item_is_effective_at(item=item, effective_at=effective_at)
            if effective_at is not None
            else item.plan.market_date is None
            or item.plan.market_date <= resolved_market_date
        )
    ]
    requested_symbols = {item.symbol for item in selected_items}
    held_where = [
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity > 0,
    ]
    if symbols:
        held_where.append(ExecutionPosition.symbol.in_(sorted(symbols)))
    positions_by_symbol = _unique_positions_by_symbol(
        db.scalars(select(ExecutionPosition).where(*held_where)).all()
    )
    selected_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=selected_items,
        positions_by_symbol=positions_by_symbol,
        recorded_at=datetime.now(UTC),
    )
    held_symbols = set(positions_by_symbol)
    if not held_symbols:
        return selected_items

    selected_item_ids = {item.id for item in selected_items}
    if not requested_symbols:
        return selected_items
    selected_sell_replacements: dict[
        tuple[str, str, str], ExecutionPlanItem
    ] = {}
    for item in selected_items:
        if item.side != "sell" or item.intent not in SELL_MONITORED_INTENTS:
            continue
        family_key = plan_item_entry_family_key(item)
        if family_key is None:
            # An unlinked legacy monitor cannot prove that it replaces any
            # other same-symbol protection row.
            continue
        replacement_key = (item.symbol, item.intent, family_key)
        current = selected_sell_replacements.get(replacement_key)
        if current is None or _sell_item_is_preferred_authority(item, current):
            selected_sell_replacements[replacement_key] = item
    eligible_symbols = held_symbols & requested_symbols
    if not eligible_symbols:
        return selected_items
    active_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(ACTIVE_SELL_MONITORING_STATUSES)),
            ExecutionPlanItem.symbol.in_(eligible_symbols),
            _kis_plan_item_provider_filter(),
            or_(
                ExecutionPlan.market_date.is_(None),
                ExecutionPlan.market_date <= resolved_market_date,
            ),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.asc(),
            ExecutionPlan.requested_at.asc().nullsfirst(),
            ExecutionPlan.accepted_at.asc(),
            ExecutionPlan.id.asc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    if effective_at is not None:
        active_items = [
            item
            for item in active_items
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=effective_at,
            )
        ]
    active_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=active_items,
        positions_by_symbol=positions_by_symbol,
        recorded_at=datetime.now(UTC),
    )

    append_items: list[ExecutionPlanItem] = []
    for item in active_items:
        if item.id in selected_item_ids:
            continue
        family_key = plan_item_entry_family_key(item)
        replacement = (
            selected_sell_replacements.get((item.symbol, item.intent, family_key))
            if family_key is not None
            else None
        )
        if replacement is not None and _sell_item_is_strictly_newer(
            replacement,
            item,
        ):
            prior_status = item.status
            item.status = "superseded"
            _record_monitoring_event(
                item=item,
                event={
                    "event": "active_sell_monitor_superseded",
                    "reason": "newer_same_intent_monitor_selected",
                    "prior_status": prior_status,
                    "superseded_by_execution_plan_item_id": replacement.id,
                    "superseded_by_execution_plan_id": replacement.execution_plan_id,
                    "entry_family_key": family_key,
                    "recorded_at": datetime.now(UTC).isoformat(),
                },
            )
            continue
        append_items.append(item)

    reconciled_items = [*selected_items, *append_items]
    active_sell_items_by_route: dict[tuple[str, str], list[ExecutionPlanItem]] = {}
    for item in reconciled_items:
        if (
            item.side == "sell"
            and item.intent in SELL_MONITORED_INTENTS
            and item.status in ACTIVE_SELL_MONITORING_STATUSES
        ):
            active_sell_items_by_route.setdefault(
                (item.symbol, item.intent), []
            ).append(item)
    recorded_at = datetime.now(UTC)
    for route_items in active_sell_items_by_route.values():
        unresolved = [
            item
            for item in route_items
            if plan_item_entry_family_key(item) is None
        ]
        if not unresolved or len(route_items) == 1:
            continue
        competing_ids = sorted(str(item.id) for item in route_items)
        for item in unresolved:
            _record_ambiguous_entry_family_review(
                item=item,
                event="active_sell_monitor_manual_review",
                competing_item_ids=competing_ids,
                recorded_at=recorded_at,
            )

    if not append_items:
        return selected_items
    return sorted(
        reconciled_items,
        key=lambda item: (
            item.plan.accepted_at,
            item.plan.id,
            item.plan_item_index,
            item.id,
        ),
    )


def _append_held_stop_sell_fallbacks(
    *,
    settings: Settings,
    db: Session,
    monitored_item_filter,
    selected_items: list[ExecutionPlanItem],
    symbols: set[str] | None = None,
    current_market_date: date | None = None,
    effective_at: datetime | None = None,
) -> list[ExecutionPlanItem]:
    resolved_market_date = current_market_date or datetime.now(KRX_TIMEZONE).date()
    selected_items = [
        item
        for item in selected_items
        if (
            _monitoring_item_is_effective_at(item=item, effective_at=effective_at)
            if effective_at is not None
            else item.plan.market_date is None
            or item.plan.market_date <= resolved_market_date
        )
    ]
    held_where = [
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity > 0,
    ]
    if symbols:
        held_where.append(ExecutionPosition.symbol.in_(sorted(symbols)))
    positions_by_symbol = _unique_positions_by_symbol(
        db.scalars(select(ExecutionPosition).where(*held_where)).all()
    )
    selected_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=selected_items,
        positions_by_symbol=positions_by_symbol,
        recorded_at=datetime.now(UTC),
    )
    held_symbols = set(positions_by_symbol)
    if not held_symbols:
        return selected_items

    selected_stops_by_symbol: dict[str, list[ExecutionPlanItem]] = {}
    selected_families_by_symbol: dict[str, set[str]] = {}
    for item in selected_items:
        if item.side != "sell" or item.intent != "stop_sell":
            continue
        selected_stops_by_symbol.setdefault(item.symbol, []).append(item)
        family_key = plan_item_entry_family_key(item)
        if family_key is not None:
            selected_families_by_symbol.setdefault(item.symbol, set()).add(family_key)

    selected_item_ids = {item.id for item in selected_items}
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            or_(
                monitored_item_filter,
                and_(
                    ExecutionPlanItem.status.in_(
                        tuple(FALLBACK_PROTECTIVE_STOP_STATUSES)
                    ),
                    ExecutionPlanItem.side == "sell",
                    ExecutionPlanItem.intent == "stop_sell",
                ),
            ),
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.symbol.in_(sorted(held_symbols)),
            _kis_plan_item_provider_filter(),
            or_(
                ExecutionPlan.market_date.is_(None),
                ExecutionPlan.market_date <= resolved_market_date,
            ),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.requested_at.desc().nullslast(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    if effective_at is not None:
        candidates = [
            item
            for item in candidates
            if _monitoring_item_is_effective_at(
                item=item,
                effective_at=effective_at,
            )
        ]
    candidates = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=candidates,
        positions_by_symbol=positions_by_symbol,
        recorded_at=datetime.now(UTC),
    )
    fallback_by_family: dict[tuple[str, str], ExecutionPlanItem] = {}
    ambiguous_candidates_by_symbol: dict[str, list[ExecutionPlanItem]] = {}
    for item in candidates:
        if item.id in selected_item_ids:
            continue
        if (
            item.status in RETRYABLE_PROTECTIVE_STOP_STATUSES
            and not _latest_order_failure_is_retryable_session_rejection(
                settings=settings,
                db=db,
                item=item,
            )
        ):
            continue
        family_key = plan_item_entry_family_key(item)
        if family_key is None:
            ambiguous_candidates_by_symbol.setdefault(item.symbol, []).append(item)
            continue
        if family_key in selected_families_by_symbol.get(item.symbol, set()):
            continue
        fallback_key = (item.symbol, family_key)
        current_fallback = fallback_by_family.get(fallback_key)
        if current_fallback is None or _sell_item_is_preferred_authority(
            item,
            current_fallback,
        ):
            fallback_by_family[fallback_key] = item

    fallback_items = list(fallback_by_family.values())
    recorded_at = datetime.now(UTC)
    for symbol, ambiguous_candidates in ambiguous_candidates_by_symbol.items():
        selected_stops = selected_stops_by_symbol.get(symbol, [])
        explicit_fallbacks = [
            item
            for (candidate_symbol, _family_key), item in fallback_by_family.items()
            if candidate_symbol == symbol
        ]
        if not selected_stops and not explicit_fallbacks and len(ambiguous_candidates) == 1:
            # Retain the single-family legacy behavior only when there is no
            # evidence of another same-symbol protective family.
            fallback_items.append(ambiguous_candidates[0])
            continue
        competing_ids = sorted(
            str(item.id)
            for item in [*selected_stops, *explicit_fallbacks, *ambiguous_candidates]
        )
        for item in ambiguous_candidates:
            _record_ambiguous_entry_family_review(
                item=item,
                event="protective_stop_fallback_manual_review",
                competing_item_ids=competing_ids,
                recorded_at=recorded_at,
            )
    if not fallback_items:
        return selected_items
    return sorted(
        [*selected_items, *fallback_items],
        key=lambda item: (
            item.plan.accepted_at,
            item.plan.id,
            item.plan_item_index,
            item.id,
        ),
    )


def _reactivate_retryable_protective_stops(
    *,
    settings: Settings,
    items: list[ExecutionPlanItem],
    db: Session,
    triggered_at: datetime,
) -> set[str]:
    rearmed_item_ids: set[str] = set()
    for item in items:
        if (
            item.side != "sell"
            or item.intent != "stop_sell"
            or item.status not in RETRYABLE_PROTECTIVE_STOP_STATUSES
        ):
            continue
        source_order = _retryable_session_rejection_source_order(
            settings=settings,
            db=db,
            item=item,
        )
        if source_order is None:
            continue
        prior_status = item.status
        retry_generation = _protective_stop_retry_generation(
            settings=settings,
            db=db,
            item=item,
            source_order=source_order,
        )
        if retry_generation >= PROTECTIVE_STOP_SESSION_RETRY_MAX_ATTEMPTS:
            item.status = "broker_order_failed"
            _record_monitoring_event(
                item=item,
                event={
                    "event": "protective_stop_retry_exhausted",
                    "reason": "retryable_session_rejection_attempts_exhausted",
                    "prior_status": prior_status,
                    "source_broker_order_pk": source_order.id,
                    "retry_attempt_count": retry_generation,
                    "retry_max_attempts": PROTECTIVE_STOP_SESSION_RETRY_MAX_ATTEMPTS,
                    "recorded_at": triggered_at.isoformat(),
                },
            )
            continue
        if _quantity_policy_requires_position_exit(item.quantity_policy):
            broker_quantity = db.scalar(
                select(BrokerOrder.quantity)
                .where(
                    *_active_kis_order_filters(
                        settings=settings,
                        exchange=_plan_item_exchange(item),
                    ),
                    BrokerOrder.execution_plan_item_id == item.id,
                )
                .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
                .limit(1)
            )
            if broker_quantity is not None and int(broker_quantity) > 0:
                item.quantity_policy = _fixed_shares_quantity_policy(
                    item=item,
                    quantity=int(broker_quantity),
                )
        item.status = _monitoring_status(item.intent)
        _record_monitoring_event(
            item=item,
            event={
                "event": "protective_stop_rearmed",
                "reason": "retryable_session_rejection",
                "prior_status": prior_status,
                "source_broker_order_pk": source_order.id,
                "retry_attempt_count": retry_generation,
                "retry_max_attempts": PROTECTIVE_STOP_SESSION_RETRY_MAX_ATTEMPTS,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        rearmed_item_ids.add(item.id)
    return rearmed_item_ids


def _repair_unprotected_stop_coverage(
    *,
    settings: Settings,
    db: Session,
    positions_by_symbol: dict[str, ExecutionPosition],
    symbols: set[str] | None = None,
    triggered_at: datetime,
) -> tuple[list[ProtectiveStopCoverageIssue], int]:
    positions = {
        symbol: position
        for symbol, position in positions_by_symbol.items()
        if position.quantity > 0
    }
    if not positions:
        return _stale_position_truth_issues(
            settings=settings,
            db=db,
            trusted_position_symbols=set(),
            symbols=symbols or set(positions_by_symbol) or None,
            triggered_at=triggered_at,
        ), 0

    stop_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(sorted(positions)),
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.status.in_(tuple(PROTECTIVE_STOP_REPAIR_CANDIDATE_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.decision_agent_name,
                ExecutionPlanItem.decision_snapshot_id,
                ExecutionPlanItem.decision_run_id,
                ExecutionPlanItem.source_plan_item_id,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.quantity_precision,
                ExecutionPlanItem.price_precision,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.requested_at,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.requested_at.desc().nullslast(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    stop_items = [
        item
        for item in stop_items
        if item.plan.market_date is None
        or _replanned_sell_plan_is_effective(
            plan=item.plan,
            item=item,
            effective_at=triggered_at,
        )
    ]
    stop_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=stop_items,
        positions_by_symbol=positions,
        recorded_at=triggered_at,
    )
    items_by_symbol: dict[str, list[ExecutionPlanItem]] = {symbol: [] for symbol in positions}
    for item in stop_items:
        if (
            _protective_stop_reserves_quantity(item)
            and _quantity_policy_requires_position_exit(item.quantity_policy)
        ):
            broker_quantity = db.scalar(
                select(BrokerOrder.quantity)
                .where(
                    *_active_kis_order_filters(
                        settings=settings,
                        exchange=_plan_item_exchange(item),
                    ),
                    BrokerOrder.execution_plan_item_id == item.id,
                )
                .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
                .limit(1)
            )
            if broker_quantity is not None and int(broker_quantity) > 0:
                item.quantity_policy = _fixed_shares_quantity_policy(
                    item=item,
                    quantity=int(broker_quantity),
                )
        items_by_symbol.setdefault(item.symbol, []).append(item)

    repaired_count = 0
    issues = _protective_stop_coverage_issues(
        positions=positions,
        items_by_symbol=items_by_symbol,
        repaired_symbols=set(),
    )
    for issue in issues:
        if issue.outcome != "unprotected_position":
            continue
        latest_item = _latest_rearmable_stop_item(items_by_symbol.get(issue.symbol, []))
        if latest_item is None or latest_item.trigger_price is None:
            continue
        prior_status = latest_item.status
        prior_quantity_policy = latest_item.quantity_policy
        held_quantity = positions[issue.symbol].quantity
        rearm_quantity = issue.missing_quantity
        repair_trigger_price = latest_item.trigger_price
        average_price = positions[issue.symbol].average_price
        if (
            repair_trigger_price is not None
            and average_price is not None
            and average_price > 0
            and repair_trigger_price >= average_price
            and latest_item.status in RETRYABLE_PROTECTIVE_STOP_STATUSES
        ):
            repair_trigger_price = (
                average_price * (Decimal("1") - settings.exec_agent_stop_loss_pct)
            ).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        preserve_latest_item = (
            _protective_stop_reserves_quantity(latest_item)
            or latest_item.status in RETRYABLE_PROTECTIVE_STOP_STATUSES
        )
        if issue.active_stop_quantity > 0 or preserve_latest_item:
            repair_item = ExecutionPlanItem(
                execution_plan_id=latest_item.execution_plan_id,
                plan_item_index=max(
                    (item.plan_item_index for item in items_by_symbol.get(issue.symbol, [])),
                    default=0,
                )
                + 1,
                symbol=latest_item.symbol,
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                quantity_policy=_fixed_shares_quantity_policy(
                    item=latest_item,
                    quantity=rearm_quantity,
                ),
                limit_price=latest_item.limit_price,
                trigger_price=repair_trigger_price,
                time_in_force=latest_item.time_in_force,
                risk_context=dict(latest_item.risk_context or {}),
                decision_agent_name=(
                    latest_item.decision_agent_name or "post-entry-monitor-repair"
                ),
                decision_snapshot_id=latest_item.decision_snapshot_id,
                decision_run_id=latest_item.decision_run_id,
                source_plan_item_id=latest_item.source_plan_item_id,
                source_market_date=latest_item.source_market_date,
                provider=latest_item.provider,
                venue=latest_item.venue,
                asset_class=latest_item.asset_class,
                quantity_precision=latest_item.quantity_precision,
                price_precision=latest_item.price_precision,
            )
            db.add(repair_item)
            items_by_symbol.setdefault(issue.symbol, []).append(repair_item)
            rearmed_item = repair_item
            cloned_repair = True
        else:
            latest_item.status = "stop_sell_monitoring"
            latest_item.quantity_policy = _fixed_shares_quantity_policy(
                item=latest_item,
                quantity=rearm_quantity,
            )
            latest_item.trigger_price = repair_trigger_price
            rearmed_item = latest_item
            cloned_repair = False
        _record_monitoring_event(
            item=rearmed_item,
            event={
                "event": "unprotected_position_stop_rearmed",
                "reason": "active_stop_coverage_below_held_quantity",
                "held_quantity": held_quantity,
                "active_stop_quantity": issue.active_stop_quantity,
                "missing_quantity": issue.missing_quantity,
                "rearmed_quantity": rearm_quantity,
                "prior_item_id": latest_item.id,
                "prior_status": prior_status,
                "prior_quantity_policy": prior_quantity_policy,
                "cloned_repair_item": cloned_repair,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        repaired_count += 1

    if repaired_count:
        issues = _protective_stop_coverage_issues(
            positions=positions,
            items_by_symbol=items_by_symbol,
            repaired_symbols={
                issue.symbol
                for issue in issues
                if _latest_rearmable_stop_item(items_by_symbol.get(issue.symbol, [])) is not None
            },
        )
    issues.extend(
        _stale_position_truth_issues(
            settings=settings,
            db=db,
            trusted_position_symbols=set(positions),
            symbols=symbols or set(positions_by_symbol) or None,
            triggered_at=triggered_at,
        )
    )
    return issues, repaired_count


def _protective_stop_coverage_issues(
    *,
    positions: dict[str, ExecutionPosition],
    items_by_symbol: dict[str, list[ExecutionPlanItem]],
    repaired_symbols: set[str],
) -> list[ProtectiveStopCoverageIssue]:
    issues: list[ProtectiveStopCoverageIssue] = []
    for symbol in sorted(positions):
        held_quantity = positions[symbol].quantity
        items = items_by_symbol.get(symbol, [])
        coverage_items = _current_stop_generation_items(items)
        active_quantity = sum(
            quantity
            for item in coverage_items
            if item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            for quantity in [_shares_quantity(item.quantity_policy)]
            if quantity is not None
        )
        if active_quantity >= held_quantity:
            continue
        latest_item = _latest_stop_item(items)
        missing_quantity = held_quantity - active_quantity
        issues.append(
            ProtectiveStopCoverageIssue(
                symbol=symbol,
                held_quantity=held_quantity,
                active_stop_quantity=active_quantity,
                missing_quantity=missing_quantity,
                latest_stop_item_id=latest_item.id if latest_item is not None else None,
                latest_stop_status=latest_item.status if latest_item is not None else None,
                latest_stop_trigger_price=latest_item.trigger_price if latest_item is not None else None,
                outcome=(
                    "rearmed"
                    if symbol in repaired_symbols and latest_item is not None and latest_item.trigger_price is not None
                    else "unprotected_position"
                ),
            )
        )
    return issues


def _stale_position_truth_issues(
    *,
    settings: Settings,
    db: Session,
    trusted_position_symbols: set[str],
    symbols: set[str] | None = None,
    triggered_at: datetime,
) -> list[ProtectiveStopCoverageIssue]:
    evidence_by_symbol = _position_truth_evidence_by_symbol(
        settings=settings,
        db=db,
        symbols=symbols,
        evaluated_at=triggered_at,
    )
    if not evidence_by_symbol:
        return []

    candidate_symbols = sorted(set(evidence_by_symbol) - trusted_position_symbols)
    if not candidate_symbols:
        return []

    positions = db.scalars(
        select(ExecutionPosition).where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.symbol.in_(candidate_symbols),
        )
    ).all()
    positions_by_symbol = _unique_positions_by_symbol(positions)
    stale_before = triggered_at - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    issues: list[ProtectiveStopCoverageIssue] = []
    for symbol in candidate_symbols:
        position = positions_by_symbol.get(symbol)
        if position is not None and position.quantity > 0:
            continue
        last_reconciled_at = position.last_reconciled_at if position is not None else None
        if last_reconciled_at is not None and _normalize_utc(last_reconciled_at) >= stale_before:
            continue

        stop_items = _stop_items_for_symbol(
            settings=settings,
            db=db,
            symbol=symbol,
            evaluated_at=triggered_at,
        )
        latest_item = _latest_stop_item(stop_items)
        active_quantity = sum(
            quantity
            for item in stop_items
            if item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            for quantity in [_shares_quantity(item.quantity_policy)]
            if quantity is not None
        )
        issues.append(
            ProtectiveStopCoverageIssue(
                symbol=symbol,
                held_quantity=None,
                active_stop_quantity=active_quantity,
                missing_quantity=None,
                latest_stop_item_id=latest_item.id if latest_item is not None else None,
                latest_stop_status=latest_item.status if latest_item is not None else None,
                latest_stop_trigger_price=latest_item.trigger_price if latest_item is not None else None,
                outcome="position_truth_stale",
                reason="stale_or_missing_execution_position_with_live_evidence",
                position_quantity=position.quantity if position is not None else None,
                position_last_reconciled_at=last_reconciled_at,
                evidence=evidence_by_symbol[symbol],
            )
        )
    return issues


def _position_truth_evidence_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
    evaluated_at: datetime | None = None,
) -> dict[str, list[str]]:
    resolved_evaluated_at = _normalize_utc(evaluated_at or datetime.now(UTC))
    evidence_by_symbol: dict[str, set[str]] = {}
    symbol_list = sorted(symbols) if symbols else None

    order_where = [
        *_active_kis_order_filters(settings=settings),
        BrokerOrder.side.in_(("buy", "sell")),
        BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
    ]
    if symbol_list:
        order_where.append(BrokerOrder.symbol.in_(symbol_list))
    orders = db.scalars(
        select(BrokerOrder).where(*order_where)
    ).all()
    order_evidence_by_route: dict[tuple[str, str], set[str]] = {}
    for order in orders:
        exchange = _active_kis_order_exchange(settings=settings, order=order)
        if exchange is None:
            continue
        symbol = str(order.symbol)
        route = (symbol, exchange)
        if order.status in {"submitted", "open", "partially_filled"}:
            order_evidence_by_route.setdefault(route, set()).add(
                f"broker_order:{order.side}:{order.status}"
            )
    open_fill_routes = _unexited_buy_routes_from_fills(
        settings=settings,
        db=db,
        symbols=symbol_list,
    )
    for route in open_fill_routes:
        order_evidence_by_route.setdefault(route, set()).add(
            "broker_order:buy:filled_unexited"
        )
    routes_by_symbol: dict[str, set[str]] = {}
    for symbol, exchange in order_evidence_by_route:
        routes_by_symbol.setdefault(symbol, set()).add(exchange)
    for symbol, exchanges in routes_by_symbol.items():
        if len(exchanges) == 1:
            route = (symbol, next(iter(exchanges)))
            evidence_by_symbol.setdefault(symbol, set()).update(
                order_evidence_by_route[route]
            )

    sell_where = [
        ExecutionPlan.mode == settings.runtime_mode,
        _effective_monitor_plan_filter(
            effective_at=resolved_evaluated_at,
            current_market_date=None,
        ),
        ExecutionPlanItem.side == "sell",
        ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
        ExecutionPlanItem.status.in_(tuple(SELL_STALE_FLAT_EXPIRABLE_STATUSES | {"expired"})),
    ]
    if symbol_list:
        sell_where.append(ExecutionPlanItem.symbol.in_(symbol_list))
    sell_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(*sell_where)
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.quantity_policy,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
            ),
        )
    ).all()
    sell_items = [
        item
        for item in sell_items
        if _monitoring_item_is_effective_at(
            item=item,
            effective_at=resolved_evaluated_at,
        )
    ]
    sell_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=sell_items,
        recorded_at=datetime.now(UTC),
    )
    sell_evidence_by_route: dict[tuple[str, str], set[str]] = {}
    for item in sell_items:
        provider = str(item.provider or "kis").strip().lower()
        if provider in {"korea-investment", "korea_investment"}:
            provider = "kis"
        if provider != "kis" or classify_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context=_loaded_risk_context(item, allow_file_backed_load=True),
            quantity_policy=item.quantity_policy,
        ) != "active":
            continue
        exchange = _plan_item_exchange(item)
        if exchange not in MONITORED_KIS_POSITION_EXCHANGES:
            continue
        sell_evidence_by_route.setdefault((str(item.symbol), exchange), set()).add(
            f"sell_monitor:{item.status}"
        )
    sell_routes_by_symbol: dict[str, set[str]] = {}
    for symbol, exchange in sell_evidence_by_route:
        sell_routes_by_symbol.setdefault(symbol, set()).add(exchange)
    for symbol, exchanges in sell_routes_by_symbol.items():
        if len(exchanges) != 1:
            continue
        route = (symbol, next(iter(exchanges)))
        evidence_by_symbol.setdefault(symbol, set()).update(
            sell_evidence_by_route[route]
        )

    return {
        symbol: sorted(evidence)
        for symbol, evidence in evidence_by_symbol.items()
        if evidence
    }


def _has_unexited_buy_order_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> bool:
    return symbol in _unexited_buy_symbols_from_fills(
        settings=settings,
        db=db,
        symbols=[symbol],
    )


def _symbols_with_unexited_buy_orders(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> set[str]:
    if not symbols:
        return set()
    ordered_symbols = _ordered_symbol_scope(symbols)
    return _unexited_buy_symbols_from_fills(
        settings=settings,
        db=db,
        symbols=ordered_symbols,
    )


def _unexited_buy_symbols_from_fills(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> set[str]:
    routes = _unexited_buy_routes_from_fills(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    exchanges_by_symbol: dict[str, set[str]] = {}
    for symbol, exchange in routes:
        exchanges_by_symbol.setdefault(symbol, set()).add(exchange)
    return {
        symbol
        for symbol, _exchange in routes
        if len(exchanges_by_symbol.get(symbol, set())) == 1
    }


def _unexited_buy_routes_from_fills(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str] | None,
) -> set[tuple[str, str]]:
    observed_through = datetime.now(UTC)
    where = [
        *_active_kis_order_filters(settings=settings),
        ExecutionFill.filled_at <= observed_through,
    ]
    if symbols:
        where.append(ExecutionFill.symbol.in_(symbols))
    fills = db.scalars(
        select(ExecutionFill)
        .join(BrokerOrder, BrokerOrder.id == ExecutionFill.broker_order_pk)
        .where(*where)
        .options(
            selectinload(ExecutionFill.broker_order).load_only(
                BrokerOrder.id,
                BrokerOrder.broker_name,
                BrokerOrder.symbol,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
                BrokerOrder.request_metadata,
            )
        )
        .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()
    filled_order_ids = {fill.broker_order_pk for fill in fills}
    uncertain_order_where = [
        *_active_kis_order_filters(settings=settings),
        BrokerOrder.side == "buy",
        BrokerOrder.status.in_(("filled", "partially_filled")),
        BrokerOrder.id.not_in(filled_order_ids),
    ]
    if symbols:
        uncertain_order_where.append(BrokerOrder.symbol.in_(symbols))
    uncertain_buy_orders = db.scalars(
        select(BrokerOrder).where(*uncertain_order_where)
    ).all()
    replay = replay_open_long_fill_inventory(
        fills=fills,
        isolation_cursor=load_fifo_isolation_cursor(
            db=db,
            runtime_mode=settings.runtime_mode,
            through=observed_through,
        ),
        through=observed_through,
    )
    open_routes = {
        (symbol, route_scope.exchange)
        for (route_scope, symbol), quantity in replay.open_quantities.items()
        if route_scope.provider == "kis" and quantity > 0
    }
    if replay.indeterminate_fill_count:
        # Incomplete canonical history must never be interpreted as proof that
        # a route is flat. Preserve protection for every affected fill route.
        for fill in fills:
            route_scope = fifo_route_scope_from_order(fill.broker_order)
            symbol = str(fill.symbol or "").strip().upper()
            if route_scope is not None and route_scope.provider == "kis" and symbol:
                open_routes.add((symbol, route_scope.exchange))
    for order in uncertain_buy_orders:
        route_scope = fifo_route_scope_from_order(order)
        symbol = str(order.symbol or "").strip().upper()
        if route_scope is not None and route_scope.provider == "kis" and symbol:
            open_routes.add((symbol, route_scope.exchange))
    return open_routes


def _stop_items_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    evaluated_at: datetime | None = None,
) -> list[ExecutionPlanItem]:
    resolved_evaluated_at = _normalize_utc(evaluated_at or datetime.now(UTC))
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            _effective_monitor_plan_filter(
                effective_at=resolved_evaluated_at,
                current_market_date=None,
            ),
            ExecutionPlanItem.symbol == symbol,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.status.in_(tuple(PROTECTIVE_STOP_REPAIR_CANDIDATE_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.source_market_date,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    items = [
        item
        for item in items
        if _monitoring_item_is_effective_at(
            item=item,
            effective_at=resolved_evaluated_at,
        )
    ]
    return _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=items,
        positions_by_symbol=_load_positions_by_symbol(
            settings=settings,
            db=db,
            symbols=[symbol],
        ),
        recorded_at=datetime.now(UTC),
    )


def _profit_target_coverage_issues(
    *,
    settings: Settings,
    db: Session,
    positions_by_symbol: dict[str, ExecutionPosition],
    evaluated_at: datetime | None = None,
) -> list[ProfitTargetCoverageIssue]:
    positions = {
        symbol: position
        for symbol, position in positions_by_symbol.items()
        if position.quantity > 0
    }
    if not positions:
        return []

    target_items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(sorted(positions)),
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == "target_sell",
            ExecutionPlanItem.status.in_(tuple(PROFIT_TARGET_COVERAGE_STATUSES)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.asset_class,
            )
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.id.desc(),
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    resolved_evaluated_at = evaluated_at or datetime.now(UTC)
    target_items = [
        item
        for item in target_items
        if item.plan.market_date is None
        or _replanned_sell_plan_is_effective(
            plan=item.plan,
            item=item,
            effective_at=resolved_evaluated_at,
        )
    ]
    target_items = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=target_items,
        positions_by_symbol=positions,
        recorded_at=datetime.now(UTC),
    )
    items_by_symbol: dict[str, list[ExecutionPlanItem]] = {symbol: [] for symbol in positions}
    for item in target_items:
        items_by_symbol.setdefault(item.symbol, []).append(item)

    issues: list[ProfitTargetCoverageIssue] = []
    for symbol in sorted(positions):
        held_quantity = positions[symbol].quantity
        items = items_by_symbol.get(symbol, [])
        active_quantity = sum(
            quantity
            for item in items
            if item.status in PROFIT_TARGET_COVERAGE_STATUSES
            for quantity in [_coverage_quantity(item.quantity_policy, held_quantity=held_quantity)]
            if quantity is not None
        )
        if active_quantity >= held_quantity:
            continue
        latest_item = _latest_target_item(items)
        entry_order = _latest_filled_buy_order(
            settings=settings,
            db=db,
            symbol=symbol,
            position=positions[symbol],
        )
        entry_target_price = (
            _watch_buy_target_price(entry_order.plan_item)
            if entry_order is not None and entry_order.plan_item is not None
            else None
        )
        if latest_item is None and entry_target_price is None:
            continue
        issues.append(
            ProfitTargetCoverageIssue(
                symbol=symbol,
                held_quantity=held_quantity,
                active_target_quantity=active_quantity,
                missing_quantity=held_quantity - active_quantity,
                latest_target_item_id=latest_item.id if latest_item is not None else None,
                latest_target_status=latest_item.status if latest_item is not None else None,
                latest_target_trigger_price=(
                    latest_item.trigger_price if latest_item is not None else entry_target_price
                ),
                outcome="missing_profit_target_coverage",
                reason="active_target_sell_coverage_below_held_quantity",
            )
        )
    return issues


def _latest_stop_item(items: list[ExecutionPlanItem]) -> ExecutionPlanItem | None:
    return items[0] if items else None


def _current_stop_generation_items(
    items: list[ExecutionPlanItem],
) -> list[ExecutionPlanItem]:
    if not items:
        return []
    latest_plan_id = items[0].execution_plan_id
    return [
        item
        for item in items
        if item.execution_plan_id == latest_plan_id
        or _protective_stop_reserves_quantity(item)
        or _is_session_reactivated_stop(item)
    ]


def _protective_stop_reserves_quantity(item: ExecutionPlanItem) -> bool:
    return item.status in (
        IN_FLIGHT_BROKER_ORDER_ITEM_STATUSES | FALLBACK_PROTECTIVE_STOP_STATUSES
    )


def _is_session_reactivated_stop(item: ExecutionPlanItem) -> bool:
    event = _latest_monitoring_event(item) or {}
    return (
        event.get("event") == "protective_stop_rearmed"
        and event.get("reason") == "retryable_session_rejection"
    )


def _latest_target_item(items: list[ExecutionPlanItem]) -> ExecutionPlanItem | None:
    return items[0] if items else None


def _latest_rearmable_stop_item(items: list[ExecutionPlanItem]) -> ExecutionPlanItem | None:
    for item in items:
        if item.trigger_price is not None and (
            item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
            or item.status in RETRYABLE_PROTECTIVE_STOP_STATUSES
            or item.status not in NON_MONITORABLE_PLAN_ITEM_STATUSES
        ):
            return item
    return None


def _fixed_shares_quantity_policy(
    *,
    item: ExecutionPlanItem,
    quantity: int,
    source: str | None = None,
) -> dict[str, object]:
    """Materialize shares without detaching the exit from its authority."""

    prior_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    policy: dict[str, object] = {
        "kind": "shares",
        "quantity": quantity,
    }
    linked_entry_item_key = prior_policy.get("linked_entry_item_key")
    if isinstance(linked_entry_item_key, str) and linked_entry_item_key.strip():
        policy["linked_entry_item_key"] = linked_entry_item_key.strip()
    for authority_key in (
        KIS_ACCOUNT_ROUTE_BINDING_KEY,
        EXECUTION_ACCOUNT_SCOPE_KEY,
    ):
        authority = prior_policy.get(authority_key)
        if isinstance(authority, dict):
            policy[authority_key] = dict(authority)
    if source is not None:
        policy["source"] = source
    return policy


def _shares_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict) or quantity_policy.get("kind") != "shares":
        return None
    return _parse_positive_int(quantity_policy.get("quantity"))


def _coverage_quantity(
    quantity_policy: dict[str, object] | None,
    *,
    held_quantity: int,
) -> int | None:
    if _quantity_policy_requires_position_exit(quantity_policy):
        return held_quantity
    return _shares_quantity(quantity_policy)


def _protective_stop_coverage_payload(
    *,
    issues: list[ProtectiveStopCoverageIssue],
    repaired_count: int,
    actionability_warnings: list[ProtectiveStopActionabilityWarning] | None = None,
) -> dict[str, object]:
    actionability_warnings = actionability_warnings or []
    unprotected = [issue for issue in issues if issue.outcome == "unprotected_position"]
    degraded = [issue for issue in issues if issue.outcome != "rearmed"]
    degraded_symbols = {issue.symbol for issue in degraded}
    affected_symbols = [issue.symbol for issue in degraded] + [
        warning.symbol
        for warning in actionability_warnings
        if warning.symbol not in degraded_symbols
    ]
    serialized_issues = [
        {
            "symbol": issue.symbol,
            "held_quantity": issue.held_quantity,
            "active_stop_quantity": issue.active_stop_quantity,
            "missing_quantity": issue.missing_quantity,
            "latest_stop_item_id": issue.latest_stop_item_id,
            "latest_stop_status": issue.latest_stop_status,
            "latest_stop_trigger_price": _serialize_decimal(issue.latest_stop_trigger_price),
            "outcome": issue.outcome,
            "reason": issue.reason,
            "position_quantity": issue.position_quantity,
            "position_last_reconciled_at": _serialize_datetime(issue.position_last_reconciled_at),
            "evidence": issue.evidence or [],
        }
        for issue in issues
    ]
    return {
        "status": "degraded" if degraded or actionability_warnings else "ok",
        "unprotected_position_count": len(unprotected),
        "position_truth_stale_count": sum(1 for issue in issues if issue.outcome == "position_truth_stale"),
        "repaired_stop_coverage_count": repaired_count,
        "actionability_warning_count": len(actionability_warnings),
        "affected_symbols": affected_symbols[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "affected_symbols_count": len(affected_symbols),
        "affected_symbols_truncated": len(affected_symbols) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
        "missing_quantities": {
            issue.symbol: issue.missing_quantity
            for issue in degraded
            if issue.missing_quantity is not None
        },
        "issues": serialized_issues[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "issues_count": len(serialized_issues),
        "issues_truncated": len(serialized_issues) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
    }


def _protective_stop_actionability_warnings(
    *,
    items: list[ExecutionPlanItem],
    prices: dict[str, MonitoringPriceRequest],
    results: list[dict[str, object]],
) -> list[ProtectiveStopActionabilityWarning]:
    warnings: list[ProtectiveStopActionabilityWarning] = []
    results_by_item_id = {
        str(result.get("execution_plan_item_id")): result
        for result in results
        if result.get("execution_plan_item_id") is not None
    }
    seen: set[str] = set()
    for item in items:
        if item.side != "sell" or item.intent != "stop_sell":
            continue
        price = prices.get(item.symbol)
        observed_price = _observed_price_for_item(price=price, item=item)
        if observed_price is None or item.trigger_price is None or observed_price > item.trigger_price:
            continue
        result = results_by_item_id.get(item.id)
        if result is not None and _protective_stop_result_has_actionable_outcome(result):
            continue
        seen.add(item.id)
        warnings.append(
            ProtectiveStopActionabilityWarning(
                symbol=item.symbol,
                execution_plan_item_id=item.id,
                status=str(result.get("status") if result is not None else item.status),
                observed_price=observed_price,
                stop_trigger=item.trigger_price,
                outcome=str(result.get("outcome") if result is not None else "not_evaluated"),
                reason="crossed_stop_without_actionable_outcome",
                message=(
                    str(result["message"])
                    if result is not None and result.get("message") is not None
                    else "stop trigger was crossed but no monitor result was recorded"
                ),
                block_reason=result.get("block_reason") if result is not None else None,
            )
        )
    for result in results:
        if result.get("intent") != "stop_sell":
            continue
        item_id = str(result.get("execution_plan_item_id") or "")
        if item_id in seen:
            continue
        observed_price = _parse_decimal(result.get("observed_price"))
        stop_trigger = _parse_decimal(result.get("trigger_price"))
        if observed_price is None or stop_trigger is None or observed_price > stop_trigger:
            continue
        if _protective_stop_result_has_actionable_outcome(result):
            continue
        warnings.append(
            ProtectiveStopActionabilityWarning(
                symbol=str(result.get("symbol") or ""),
                execution_plan_item_id=item_id,
                status=str(result.get("status") or ""),
                observed_price=observed_price,
                stop_trigger=stop_trigger,
                outcome=str(result.get("outcome") or "unknown"),
                reason="crossed_stop_without_actionable_outcome",
                message=str(result["message"]) if result.get("message") is not None else None,
                block_reason=result.get("block_reason"),
            )
        )
    return warnings


def _protective_stop_result_has_actionable_outcome(result: dict[str, object]) -> bool:
    outcome = str(result.get("outcome") or "")
    status = str(result.get("status") or "")
    explicit_failure_statuses = RETRYABLE_PROTECTIVE_STOP_STATUSES | {
        "broker_order_failed",
        "failed",
    }
    if outcome in {"submitted", "duplicate"}:
        return True
    if outcome == "deferred" and status.endswith("_pending_dispatch"):
        return True
    if outcome in {"blocked", "agent_delay", "agent_pending", "rejected"}:
        return (
            status.endswith("_triggered_blocked")
            or status in AGENT_ACTIVE_STATUSES
            or status == AGENT_REJECTED_STATUS
            or status in explicit_failure_statuses
        )
    if outcome == "error":
        return status in explicit_failure_statuses
    return False


def _protective_stop_actionability_payload(
    *,
    warnings: list[ProtectiveStopActionabilityWarning],
) -> dict[str, object]:
    serialized_warnings = [
        {
            "symbol": warning.symbol,
            "execution_plan_item_id": warning.execution_plan_item_id,
            "status": warning.status,
            "observed_price": _serialize_decimal(warning.observed_price),
            "stop_trigger": _serialize_decimal(warning.stop_trigger),
            "outcome": warning.outcome,
            "reason": warning.reason,
            "message": warning.message,
            "block_reason": warning.block_reason,
        }
        for warning in warnings
    ]
    return {
        "status": "degraded" if warnings else "ok",
        "warning_count": len(warnings),
        "affected_symbols": [
            warning.symbol for warning in warnings[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS]
        ],
        "affected_symbols_count": len(warnings),
        "affected_symbols_truncated": len(warnings) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
        "warnings": serialized_warnings[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "warnings_count": len(serialized_warnings),
        "warnings_truncated": len(serialized_warnings) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
    }


def _profit_target_coverage_payload(
    *,
    issues: list[ProfitTargetCoverageIssue],
) -> dict[str, object]:
    affected_symbols = [issue.symbol for issue in issues]
    serialized_issues = [
        {
            "symbol": issue.symbol,
            "held_quantity": issue.held_quantity,
            "active_target_quantity": issue.active_target_quantity,
            "missing_quantity": issue.missing_quantity,
            "latest_target_item_id": issue.latest_target_item_id,
            "latest_target_status": issue.latest_target_status,
            "latest_target_trigger_price": _serialize_decimal(issue.latest_target_trigger_price),
            "outcome": issue.outcome,
            "reason": issue.reason,
        }
        for issue in issues
    ]
    return {
        "status": "degraded" if issues else "ok",
        "missing_target_coverage_count": len(issues),
        "affected_symbols": affected_symbols[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "affected_symbols_count": len(affected_symbols),
        "affected_symbols_truncated": len(affected_symbols) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
        "missing_quantities": {
            issue.symbol: issue.missing_quantity
            for issue in issues
        },
        "issues": serialized_issues[:MAX_PROTECTIVE_STOP_COVERAGE_DETAILS],
        "issues_count": len(serialized_issues),
        "issues_truncated": len(serialized_issues) > MAX_PROTECTIVE_STOP_COVERAGE_DETAILS,
    }


def _watch_buy_retry_blocked_until(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> datetime | None:
    if item.status != "watch_buy_triggered_blocked":
        return None
    event = _latest_monitoring_event(item)
    if event is None:
        return None
    reason = str(event.get("reason") or "")
    if reason not in RETRYABLE_WATCH_BUY_APPROVAL_REASONS:
        return None
    retry_after_at = _parse_event_datetime(event.get("retry_after_at"))
    if retry_after_at is not None:
        return retry_after_at
    recorded_at = _parse_event_datetime(event.get("recorded_at"))
    if recorded_at is None:
        return None
    return recorded_at + timedelta(
        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
    )


def _trigger_time_agent_delay_until(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> datetime | None:
    if item.status != AGENT_DELAY_STATUS:
        return None
    risk_context = _loaded_risk_context(item)
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return None
    decision = interaction.get("decision")
    valid_until = None
    if isinstance(decision, dict):
        valid_until = _parse_event_datetime(decision.get("valid_until"))
    if valid_until is not None:
        return valid_until
    recorded_at = _parse_event_datetime(interaction.get("recorded_at"))
    if recorded_at is None:
        return None
    return recorded_at + timedelta(
        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
    )


def _trigger_time_agent_pending_is_stale(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    now: datetime,
) -> bool:
    if item.status != AGENT_PENDING_STATUS:
        return False
    risk_context = _loaded_risk_context(item)
    latest_event = risk_context.get("last_monitoring_event")
    recorded_at = None
    if isinstance(latest_event, dict) and latest_event.get("event") == "trigger_time_exec_agent_pending":
        recorded_at = _parse_event_datetime(latest_event.get("recorded_at"))
    if recorded_at is None:
        interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
        if isinstance(interaction, dict):
            recorded_at = _parse_event_datetime(interaction.get("recorded_at"))
    if recorded_at is None:
        return True
    timeout_seconds = int(getattr(settings, "exec_agent_trigger_time_timeout_seconds", 90))
    return now >= recorded_at + timedelta(seconds=timeout_seconds)


def _trigger_time_agent_approved_is_stale(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    now: datetime,
) -> bool:
    if item.status != AGENT_APPROVED_PENDING_SUBMIT_STATUS:
        return False
    risk_context = _loaded_risk_context(item)
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    recorded_at = None
    if isinstance(interaction, dict):
        recorded_at = _parse_event_datetime(interaction.get("recorded_at"))
    if recorded_at is None:
        latest_event = risk_context.get("last_monitoring_event")
        if (
            isinstance(latest_event, dict)
            and latest_event.get("event") == "trigger_time_exec_agent_approved"
        ):
            recorded_at = _parse_event_datetime(latest_event.get("recorded_at"))
    if recorded_at is None:
        return True
    timeout_seconds = int(getattr(settings, "exec_agent_trigger_time_timeout_seconds", 90))
    return now >= recorded_at + timedelta(seconds=timeout_seconds)


def _trigger_time_agent_recovery_reason(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    now: datetime,
    delay_until: datetime | None,
) -> str | None:
    if item.status == AGENT_PENDING_STATUS:
        if _trigger_time_agent_pending_is_stale(settings=settings, item=item, now=now):
            return "trigger_time_exec_agent_pending_timeout"
        return None
    if item.status == AGENT_DELAY_STATUS:
        if delay_until is None or now >= delay_until:
            return "trigger_time_exec_agent_delay_expired"
        return None
    if item.status == AGENT_APPROVED_PENDING_SUBMIT_STATUS:
        if _trigger_time_agent_approved_is_stale(settings=settings, item=item, now=now):
            return "trigger_time_exec_agent_approved_pending_submit_timeout"
    return None


def _latest_monitoring_event(item: ExecutionPlanItem) -> dict[str, object] | None:
    risk_context = _loaded_risk_context(item)
    if not isinstance(risk_context, dict):
        return None
    event = risk_context.get("last_monitoring_event")
    return event if isinstance(event, dict) else None


def _is_same_cycle_cloned_stop_repair(
    *,
    item: ExecutionPlanItem,
    triggered_at: datetime,
) -> bool:
    event = _latest_monitoring_event(item)
    if event is None:
        return False
    if event.get("event") != "unprotected_position_stop_rearmed":
        return False
    if event.get("cloned_repair_item") is not True:
        return False
    recorded_at = _parse_event_datetime(event.get("recorded_at"))
    return recorded_at == triggered_at


def _prefer_same_cycle_cloned_stop_repairs(
    *,
    items: list[ExecutionPlanItem],
    triggered_at: datetime,
) -> list[ExecutionPlanItem]:
    repairs_by_symbol = {
        item.symbol: item
        for item in items
        if _is_same_cycle_cloned_stop_repair(item=item, triggered_at=triggered_at)
    }
    if not repairs_by_symbol:
        return items

    def keep_existing_stop(item: ExecutionPlanItem) -> bool:
        repair_item = repairs_by_symbol.get(item.symbol)
        if repair_item is None or item.intent != "stop_sell":
            return True
        repair_event = _latest_monitoring_event(repair_item) or {}
        active_stop_quantity = int(repair_event.get("active_stop_quantity") or 0)
        if _protective_stop_reserves_quantity(item):
            return True
        if active_stop_quantity > 0:
            return item.id != repair_item.id and item.status in PROTECTIVE_STOP_COVERAGE_STATUSES
        return item.id == repair_item.id

    return [
        item
        for item in items
        if keep_existing_stop(item)
    ]


def _parse_event_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _latest_order_failure_is_retryable_session_rejection(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
) -> bool:
    order = _latest_plan_item_submission_attempt(
        settings=settings,
        db=db,
        item=item,
    )
    if order is None:
        return item.status in {"broker_order_retry_pending", "retry_pending"}
    return _order_is_retryable_session_rejection(order=order, item=item)


def _latest_plan_item_submission_attempt(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
) -> BrokerOrder | None:
    """Return the latest durable submission attempt for a plan item.

    SQLite's server-generated timestamps have one-second precision and broker
    order IDs are UUIDs, so neither can order back-to-back attempts reliably.
    ``submit_requested.observed_at`` is application-generated with microsecond
    precision and is persisted before the broker boundary. Legacy rows without
    that event retain the timestamp fallback.
    """

    order = db.scalar(
        select(BrokerOrder)
        .join(BrokerOrderEvent, BrokerOrderEvent.broker_order_pk == BrokerOrder.id)
        .where(
            *_active_kis_order_filters(
                settings=settings,
                exchange=_plan_item_exchange(item),
            ),
            BrokerOrder.execution_plan_item_id == item.id,
        )
        .where(BrokerOrderEvent.event_type == "submit_requested")
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.idempotency_key,
                BrokerOrder.status,
                BrokerOrder.last_error_code,
                BrokerOrder.last_error_classification,
                BrokerOrder.last_error_message,
                BrokerOrder.last_error_retryable,
                BrokerOrder.response_metadata,
                BrokerOrder.raw_broker_payload,
                BrokerOrder.updated_at,
                BrokerOrder.created_at,
            )
        )
        .order_by(
            BrokerOrderEvent.observed_at.desc(),
            BrokerOrderEvent.created_at.desc(),
            BrokerOrderEvent.id.desc(),
        )
        .limit(1)
    )
    if order is not None:
        return order
    return db.scalar(
        select(BrokerOrder)
        .where(
            *_active_kis_order_filters(
                settings=settings,
                exchange=_plan_item_exchange(item),
            ),
            BrokerOrder.execution_plan_item_id == item.id,
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.idempotency_key,
                BrokerOrder.status,
                BrokerOrder.last_error_code,
                BrokerOrder.last_error_classification,
                BrokerOrder.last_error_message,
                BrokerOrder.last_error_retryable,
                BrokerOrder.response_metadata,
                BrokerOrder.raw_broker_payload,
                BrokerOrder.updated_at,
                BrokerOrder.created_at,
            )
        )
        .order_by(
            BrokerOrder.updated_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
        .limit(1)
    )


def _order_is_retryable_session_rejection(
    *,
    order: BrokerOrder,
    item: ExecutionPlanItem,
) -> bool:
    if order.status in {"submitted", "open", "partially_filled", "submission_pending"}:
        return False
    error_code = (order.last_error_code or "").strip().upper()
    if error_code in SESSION_RETRYABLE_ERROR_CODES:
        return True
    classification = (order.last_error_classification or "").strip().lower()
    if classification in {"retryable", "session", "market_session", "market_closed"}:
        return True
    message = (order.last_error_message or "").strip().lower()
    if any(marker.lower() in message for marker in SESSION_RETRYABLE_MESSAGE_MARKERS):
        return True
    for payload in (order.response_metadata, order.raw_broker_payload):
        if not isinstance(payload, dict):
            continue
        raw_code = str(payload.get("msg_cd") or payload.get("code") or "").strip().upper()
        if raw_code in SESSION_RETRYABLE_ERROR_CODES:
            return True
        raw_message = str(payload.get("msg1") or payload.get("message") or "").strip().lower()
        if any(marker.lower() in raw_message for marker in SESSION_RETRYABLE_MESSAGE_MARKERS):
            return True
    return bool(order.last_error_retryable and item.status in {"broker_order_retry_pending", "retry_pending"})


def _retryable_session_rejection_source_order(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
) -> BrokerOrder | None:
    active_order_id = db.scalar(
        select(BrokerOrder.id)
        .where(
            *_active_kis_order_filters(
                settings=settings,
                exchange=_plan_item_exchange(item),
            ),
            BrokerOrder.execution_plan_item_id == item.id,
            BrokerOrder.status.in_(tuple(ACTIVE_PROTECTIVE_STOP_ORDER_STATUSES)),
        )
        .limit(1)
    )
    if active_order_id is not None:
        return None
    order = _latest_plan_item_submission_attempt(
        settings=settings,
        db=db,
        item=item,
    )
    if order is None or not _order_is_retryable_session_rejection(order=order, item=item):
        return None
    return order


def _protective_stop_retry_generation(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    source_order: BrokerOrder,
) -> int:
    """Count the persisted monitoring retry lineage ending at ``source_order``."""

    generation = 0
    current = source_order
    visited: set[str] = set()
    while current.id not in visited:
        visited.add(current.id)
        event = db.scalar(
            select(BrokerOrderEvent)
            .where(
                BrokerOrderEvent.broker_order_pk == current.id,
                BrokerOrderEvent.event_type == "submit_requested",
            )
            .order_by(
                BrokerOrderEvent.observed_at.desc(),
                BrokerOrderEvent.created_at.desc(),
                BrokerOrderEvent.id.desc(),
            )
            .limit(1)
        )
        request_payload = event.request_payload if event is not None else None
        command_metadata = (
            request_payload.get("metadata") if isinstance(request_payload, dict) else None
        )
        retry = command_metadata.get("retry") if isinstance(command_metadata, dict) else None
        source_order_pk = (
            retry.get("source_broker_order_pk")
            if isinstance(retry, dict) and retry.get("source") == "rejected_order_retry"
            else None
        )
        expected_key = (
            f"monitoring:stop_sell:{item.id}:retry:{source_order_pk}"
            if isinstance(source_order_pk, str) and source_order_pk
            else None
        )
        if expected_key is None or current.idempotency_key != expected_key:
            break
        parent = db.get(BrokerOrder, source_order_pk)
        if (
            parent is None
            or parent.execution_plan_item_id != item.id
            or _active_kis_order_exchange(settings=settings, order=parent)
            != _plan_item_exchange(item)
        ):
            break
        generation += 1
        current = parent
    return generation


def _monitored_item_filter():
    return or_(
        and_(
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent.in_(tuple(SELL_MONITORED_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(LOADABLE_SELL_MONITORING_STATUSES)),
        ),
        and_(
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(ACTIVE_WATCH_MONITORING_STATUSES)),
        ),
    )


def _position_exit_quantity_policy_filter():
    return or_(
        ExecutionPlanItem.quantity_policy["type"].as_string() == "position_exit_from_entry",
        ExecutionPlanItem.quantity_policy["kind"].as_string() == "position_exit_from_entry",
    )


def _has_unexited_buy_order_filter(*, settings: Settings):
    scope_exchange = BrokerOrder.request_metadata["execution_scope"][
        "exchange"
    ].as_string()
    item_exchange = func.upper(
        func.coalesce(
            ExecutionPlanItem.risk_context["exchange"].as_string(),
            "KRX",
        )
    )
    latest_buy_order_at = (
        select(func.max(BrokerOrder.created_at))
        .where(
            *_active_kis_order_filters(settings=settings),
            BrokerOrder.symbol == ExecutionPlanItem.symbol,
            scope_exchange == item_exchange,
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
        .correlate(ExecutionPlanItem)
        .scalar_subquery()
    )
    latest_sell_order_at = (
        select(func.max(BrokerOrder.created_at))
        .where(
            *_active_kis_order_filters(settings=settings),
            BrokerOrder.symbol == ExecutionPlanItem.symbol,
            scope_exchange == item_exchange,
            BrokerOrder.side == "sell",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled", "filled")),
        )
        .correlate(ExecutionPlanItem)
        .scalar_subquery()
    )
    return and_(
        latest_buy_order_at.is_not(None),
        or_(latest_sell_order_at.is_(None), latest_buy_order_at > latest_sell_order_at),
    )


def _unexited_buy_sell_monitor_fallback_filter(*, settings: Settings):
    fallback = and_(
        _position_exit_quantity_policy_filter(),
        _has_unexited_buy_order_filter(settings=settings),
    )
    if settings.runtime_mode != "live":
        return fallback
    return and_(
        fallback,
        ~_has_recent_flat_position_truth_filter(settings=settings),
    )


def _has_recent_flat_position_truth_filter(*, settings: Settings):
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return (
        select(ExecutionPosition.id)
        .where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.symbol == ExecutionPlanItem.symbol,
            ExecutionPosition.quantity <= 0,
            ExecutionPosition.last_reconciled_at.is_not(None),
            ExecutionPosition.last_reconciled_at >= stale_before,
        )
        .correlate(ExecutionPlanItem)
        .exists()
    )


def _has_recent_flat_position_truth_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
) -> bool:
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return db.scalar(
        select(ExecutionPosition.id)
        .where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.symbol == symbol,
            ExecutionPosition.quantity <= 0,
            ExecutionPosition.last_reconciled_at.is_not(None),
            ExecutionPosition.last_reconciled_at >= stale_before,
        )
        .limit(1)
    ) is not None


def _symbols_with_recent_flat_position_truth(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> set[str]:
    if not symbols:
        return set()
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return {
        str(symbol)
        for symbol in db.scalars(
            select(ExecutionPosition.symbol)
            .where(
                *_active_kis_position_filters(settings=settings),
                ExecutionPosition.symbol.in_(_ordered_symbol_scope(symbols)),
                ExecutionPosition.quantity <= 0,
                ExecutionPosition.last_reconciled_at.is_not(None),
                ExecutionPosition.last_reconciled_at >= stale_before,
            )
        ).all()
    }


def _load_positions_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str] | set[str] | None = None,
    lock_for_update: bool = False,
) -> dict[str, ExecutionPosition]:
    where = list(_active_kis_position_filters(settings=settings))
    if symbols:
        where.append(ExecutionPosition.symbol.in_(_ordered_symbol_scope(symbols)))
    statement = (
        select(ExecutionPosition)
        .where(*where)
        .order_by(ExecutionPosition.symbol.asc())
    )
    if lock_for_update:
        statement = statement.with_for_update().execution_options(populate_existing=True)
    positions = db.scalars(statement).all()
    return {
        symbol: position
        for symbol, position in _unique_positions_by_symbol(positions).items()
        if position.quantity > 0
    }


def _maintain_post_entry_exit_monitors(
    *,
    settings: Settings,
    db: Session,
    positions_by_symbol: dict[str, ExecutionPosition],
    prices: dict[str, MonitoringPriceRequest],
    triggered_at: datetime,
    durably_unquoteable_symbols: set[str] | None = None,
) -> tuple[int, int]:
    maintenance_count = 0
    review_count = 0
    for symbol, position in positions_by_symbol.items():
        if position.quantity <= 0:
            continue
        entry_order = _latest_filled_buy_order(
            settings=settings,
            db=db,
            symbol=symbol,
            position=position,
        )
        if entry_order is None or entry_order.plan_item is None:
            existing_maintenance_count, requires_review = _ensure_existing_position_default_exit_monitors(
                settings=settings,
                db=db,
                position=position,
                observed_price=prices.get(symbol),
                triggered_at=triggered_at,
                quote_unavailable_is_durable=(
                    symbol in (durably_unquoteable_symbols or set())
                ),
            )
            maintenance_count += existing_maintenance_count
            if requires_review:
                review_count += 1
            continue
        entry_item = entry_order.plan_item
        if entry_item.plan is None:
            continue
        entry_price = position.average_price or entry_order.limit_price
        if entry_price is None or entry_price <= 0:
            continue

        observed_price = _observed_price_for_side(
            price=prices.get(symbol),
            side="sell",
            runtime_mode=settings.runtime_mode,
        )
        entry_date = _entry_market_date(entry_item=entry_item, entry_order=entry_order)
        holding_session = _holding_session_age_from_date(
            entry_market_date=entry_date,
            triggered_at=triggered_at,
        )
        initial_stop = _post_entry_initial_stop_price(
            settings=settings,
            entry_item=entry_item,
            entry_price=entry_price,
        )
        stop_item, stop_maintenance_count = _ensure_post_entry_stop_monitor(
            db=db,
            entry_item=entry_item,
            quantity=position.quantity,
            stop_price=initial_stop,
            entry_price=entry_price,
            triggered_at=triggered_at,
        )
        maintenance_count += stop_maintenance_count
        _, target_maintenance_count = _ensure_post_entry_target_monitor(
            settings=settings,
            db=db,
            entry_item=entry_item,
            quantity=position.quantity,
            entry_price=entry_price,
            triggered_at=triggered_at,
        )
        maintenance_count += target_maintenance_count
        _, max_holding_maintenance_count = _ensure_post_entry_max_holding_monitor(
            db=db,
            entry_item=entry_item,
            quantity=position.quantity,
            entry_market_date=entry_date,
            triggered_at=triggered_at,
            max_holding_sessions=settings.exec_agent_max_holding_trading_sessions,
        )
        maintenance_count += max_holding_maintenance_count

        review = _post_entry_review_state(
            settings=settings,
            entry_price=entry_price,
            observed_price=observed_price,
            holding_session=holding_session,
            stop_price=stop_item.trigger_price,
            triggered_at=triggered_at,
        )
        maintenance_count += _apply_post_entry_review_state(
            item=stop_item,
            review=review,
            triggered_at=triggered_at,
        )
        if review["requires_review"] is True:
            review_count += 1

    return maintenance_count, review_count


def _latest_filled_buy_order(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    position: ExecutionPosition,
) -> BrokerOrder | None:
    try:
        actual_identity = position_identity_from_columns(position)
        expected_identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange=actual_identity.exchange if actual_identity is not None else "",
        )
    except PositionIdentityConflictError:
        return None
    if (
        actual_identity is None
        or actual_identity != expected_identity
        or actual_identity.exchange not in MONITORED_KIS_POSITION_EXCHANGES
    ):
        return None
    candidate_order_exchanges = (
        sorted(KRX_EXCHANGES)
        if actual_identity.exchange == "KRX"
        else [actual_identity.exchange]
    )
    active_order_route = or_(
        *(
            and_(
                *_active_kis_order_filters(
                    settings=settings,
                    exchange=route_exchange,
                )
            )
            for route_exchange in candidate_order_exchanges
        )
    )
    return db.scalar(
        select(BrokerOrder)
        .join(BrokerOrder.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            active_order_route,
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "buy",
            BrokerOrder.status == "filled",
            ExecutionPlan.mode == settings.runtime_mode,
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.execution_plan_item_id,
                BrokerOrder.symbol,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.limit_price,
                BrokerOrder.submitted_at,
                BrokerOrder.updated_at,
                BrokerOrder.created_at,
            ),
            selectinload(BrokerOrder.plan_item).load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.plan_item_index,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.time_in_force,
                ExecutionPlanItem.source_market_date,
            ).selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.mode,
                ExecutionPlan.market_date,
                ExecutionPlan.accepted_at,
            ),
        )
        .order_by(
            BrokerOrder.submitted_at.desc().nullslast(),
            BrokerOrder.updated_at.desc().nullslast(),
            BrokerOrder.created_at.desc().nullslast(),
            BrokerOrder.id.desc(),
        )
    )


def _ensure_existing_position_default_exit_monitors(
    *,
    settings: Settings,
    db: Session,
    position: ExecutionPosition,
    observed_price: MonitoringPriceRequest | None,
    triggered_at: datetime,
    quote_unavailable_is_durable: bool = False,
) -> tuple[int, bool]:
    if position.quantity <= 0 or position.average_price is None or position.average_price <= 0:
        return 0, False
    route = _existing_position_monitoring_route(
        settings=settings,
        db=db,
        position=position,
    )
    if route["manual_review_reason"] is not None:
        _mark_position_monitoring_manual_review(
            position=position,
            reason=str(route["manual_review_reason"]),
            triggered_at=triggered_at,
        )
        return 0, True
    if route["asset_class"] == "us_equity" and (
        _observed_price_for_side(
            price=observed_price,
            side="sell",
            runtime_mode=settings.runtime_mode,
        )
        is None
    ):
        if not quote_unavailable_is_durable:
            return 0, False
        if not _has_fresh_complete_kis_us_balance_monitoring_proof(
            settings=settings,
            position=position,
            evaluated_at=triggered_at,
        ):
            _mark_position_monitoring_manual_review(
                position=position,
                reason="unpriced_overseas_holding",
                triggered_at=triggered_at,
            )
            return 0, True
    market_timezone = (
        ZoneInfo("America/New_York")
        if route["asset_class"] == "us_equity"
        else KRX_TIMEZONE
    )
    plan = _latest_runtime_plan(
        settings=settings,
        db=db,
        current_market_date=_normalize_utc(triggered_at).astimezone(market_timezone).date(),
    )
    if plan is None:
        return 0, False

    maintenance_count = 0
    if not _has_active_sell_monitor(
        db=db,
        settings=settings,
        position=position,
        intent="stop_sell",
        evaluated_at=triggered_at,
    ):
        stop_price = (position.average_price * (Decimal("1") - settings.exec_agent_stop_loss_pct)).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
        db.add(
            _new_existing_position_exit_monitor(
                settings=settings,
                plan=plan,
                position=position,
                intent="stop_sell",
                trigger_price=stop_price,
                source="post_entry_missing_order_stop_repair",
                exchange=str(route["exchange"]),
                venue=str(route["venue"]),
                asset_class=str(route["asset_class"]),
                triggered_at=triggered_at,
            )
        )
        maintenance_count += 1

    if not _has_active_sell_monitor(
        db=db,
        settings=settings,
        position=position,
        intent="target_sell",
        evaluated_at=triggered_at,
    ):
        target_price = (position.average_price * (Decimal("1") + settings.exec_agent_first_take_profit_pct)).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
        db.add(
            _new_existing_position_exit_monitor(
                settings=settings,
                plan=plan,
                position=position,
                intent="target_sell",
                trigger_price=target_price,
                source="post_entry_missing_order_target_repair",
                exchange=str(route["exchange"]),
                venue=str(route["venue"]),
                asset_class=str(route["asset_class"]),
                triggered_at=triggered_at,
            )
        )
        maintenance_count += 1

    return maintenance_count, False


def _has_fresh_complete_kis_us_balance_monitoring_proof(
    *,
    settings: Settings,
    position: ExecutionPosition,
    evaluated_at: datetime,
) -> bool:
    """Keep position truth distinct from transient executable-quote coverage.

    A KIS overseas balance reference price is not an executable bid.  This
    proof therefore prevents only a durable manual-review relatch; the live
    quote trust gates still require exchange-timestamped bid evidence before
    any sell can be submitted.
    """

    if position.updated_from != "reconciliation":
        return False
    try:
        column_identity = position_identity_from_columns(position)
        payload_identity = position_identity_from_payload(position.raw_payload)
    except PositionIdentityConflictError:
        return False
    if (
        column_identity is None
        or payload_identity is None
        or column_identity != payload_identity
        or column_identity.provider != "kis"
        or column_identity.exchange not in {"NASD", "NYSE", "AMEX"}
    ):
        return False

    raw_payload = position.raw_payload
    if not isinstance(raw_payload, dict):
        return False
    sync_context = raw_payload.get("_position_sync")
    if not isinstance(sync_context, dict) or not (
        sync_context.get("asset_class") == "us_equity"
        and str(sync_context.get("exchange") or "").strip().upper()
        == column_identity.exchange
        and sync_context.get("monitoring_eligible") is True
        and sync_context.get("trade_eligible") is not False
        and sync_context.get("route_disposition") is None
    ):
        return False
    if (
        str(raw_payload.get("currency") or "").strip().upper() != "USD"
        or str(raw_payload.get("exchange") or "").strip().upper()
        != column_identity.exchange
        or raw_payload.get("holdings_coverage") != "complete"
        or raw_payload.get("open_orders_coverage") != "complete"
    ):
        return False
    masked_account = raw_payload.get("account_number_masked")
    if not isinstance(masked_account, str) or not masked_account.startswith("****"):
        return False
    configured_account = str(settings.kis_account_number or "").strip()
    if configured_account:
        expected_mask = (
            configured_account
            if len(configured_account) <= 4
            else f"{'*' * (len(configured_account) - 4)}{configured_account[-4:]}"
        )
        if masked_account != expected_mask:
            return False

    balance = raw_payload.get("balance")
    open_orders = raw_payload.get("open_orders")
    if (
        not isinstance(balance, dict)
        or str(balance.get("rt_cd") or "").strip() != "0"
        or not isinstance(open_orders, dict)
        or str(open_orders.get("rt_cd") or "").strip() != "0"
    ):
        return False
    raw_open_rows = open_orders.get("output")
    open_rows = (
        [raw_open_rows]
        if isinstance(raw_open_rows, dict)
        else [row for row in raw_open_rows if isinstance(row, dict)]
        if isinstance(raw_open_rows, list)
        else None
    )
    if open_rows is None:
        return False
    for row in open_rows:
        open_symbol = str(
            row.get("pdno") or row.get("ovrs_pdno") or ""
        ).strip().upper()
        if open_symbol != position.symbol:
            continue
        remaining_value = row.get("nccs_qty")
        if remaining_value is None:
            remaining_value = row.get("ord_uncc_qty")
        remaining_quantity = _parse_decimal(remaining_value)
        if remaining_quantity is None or remaining_quantity > 0:
            return False

    observed_at = position.last_reconciled_at
    if not isinstance(observed_at, datetime):
        return False
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    if not snapshot_is_fresh_for_recovery(
        observed_at=observed_at,
        now=evaluated_at,
        settings=settings,
    ):
        return False

    raw_rows = balance.get("output1")
    rows = (
        [raw_rows]
        if isinstance(raw_rows, dict)
        else [row for row in raw_rows if isinstance(row, dict)]
        if isinstance(raw_rows, list)
        else []
    )
    matches = [
        row
        for row in rows
        if str(row.get("ovrs_pdno") or "").strip().upper() == position.symbol
        and str(row.get("ovrs_excg_cd") or "").strip().upper()
        == column_identity.exchange
    ]
    if len(matches) != 1:
        return False
    holding = matches[0]
    holding_quantity = _parse_decimal(holding.get("ovrs_cblc_qty"))
    sellable_quantity = _parse_decimal(holding.get("ord_psbl_qty"))
    average_price = _parse_decimal(holding.get("pchs_avg_pric"))
    current_price = _parse_decimal(holding.get("now_pric2"))
    if (
        holding_quantity is None
        or holding_quantity != Decimal(position.quantity)
        or holding_quantity <= 0
        or position.base_quantity not in {None, Decimal("0")}
        or sellable_quantity is None
        or sellable_quantity != holding_quantity
        or average_price is None
        or position.average_price is None
        or average_price != Decimal(position.average_price)
        or average_price <= 0
        or current_price is None
        or current_price <= 0
    ):
        return False
    for field in ("now_pric", "current_price", "prpr", "last_price"):
        if field not in holding:
            continue
        corroborating_price = _parse_decimal(holding[field])
        if corroborating_price is None or corroborating_price != current_price:
            return False
    return True


def _existing_position_monitoring_route(
    *,
    settings: Settings,
    db: Session,
    position: ExecutionPosition,
) -> dict[str, str | None]:
    try:
        position_identity = position_identity_from_columns(position)
        if position_identity is None:
            raise PositionIdentityConflictError("position route is missing")
        expected_identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange=position_identity.exchange,
        )
    except PositionIdentityConflictError:
        return {
            "exchange": None,
            "venue": None,
            "asset_class": None,
            "manual_review_reason": "position_route_unknown",
        }
    if position_identity != expected_identity:
        return {
            "exchange": None,
            "venue": None,
            "asset_class": None,
            "manual_review_reason": "position_route_mismatch",
        }

    exchange = position_identity.exchange
    raw_payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    sync_context = raw_payload.get("_position_sync")
    if exchange in {"NASD", "NYSE", "AMEX"}:
        eligible = not (
            isinstance(sync_context, dict)
            and sync_context.get("monitoring_eligible") is False
        )
        reason = (
            sync_context.get("manual_review_reason")
            if isinstance(sync_context, dict)
            else None
        )
        if not eligible:
            reason = reason or "overseas_holding_not_verified_orderable"
        return {
            "exchange": exchange,
            "venue": "kis",
            "asset_class": "us_equity",
            "manual_review_reason": str(reason) if reason else None,
        }
    if exchange == "KRX" and position.symbol.isdigit() and len(position.symbol) == 6:
        return {
            "exchange": "KRX",
            "venue": "krx",
            "asset_class": "stock",
            "manual_review_reason": None,
        }
    return {
        "exchange": None,
        "venue": None,
        "asset_class": None,
        "manual_review_reason": "position_route_unknown",
    }


def _mark_position_monitoring_manual_review(
    *,
    position: ExecutionPosition,
    reason: str,
    triggered_at: datetime,
) -> None:
    raw_payload = dict(position.raw_payload) if isinstance(position.raw_payload, dict) else {}
    sync_context = dict(raw_payload.get("_position_sync") or {})
    sync_context["monitoring_eligible"] = False
    sync_context["manual_review_required"] = True
    sync_context["manual_review_reason"] = reason
    sync_context["manual_review_recorded_at"] = triggered_at.isoformat()
    raw_payload["_position_sync"] = sync_context
    position.raw_payload = raw_payload


def _position_monitoring_is_disabled(position: ExecutionPosition | None) -> bool:
    if position is None:
        return False
    raw_payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    sync_context = raw_payload.get("_position_sync")
    return bool(
        isinstance(sync_context, dict)
        and sync_context.get("monitoring_eligible") is False
    )


def _latest_runtime_plan(
    *,
    settings: Settings,
    db: Session,
    current_market_date: date,
) -> ExecutionPlan | None:
    return db.scalar(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            or_(
                ExecutionPlan.market_date.is_(None),
                ExecutionPlan.market_date <= current_market_date,
            ),
        )
        .order_by(
            ExecutionPlan.market_date.desc(),
            ExecutionPlan.requested_at.desc().nullslast(),
            ExecutionPlan.accepted_at.desc(),
            ExecutionPlan.created_at.desc(),
            ExecutionPlan.id.desc(),
        )
        .limit(1)
    )


def _has_active_sell_monitor(
    *,
    db: Session,
    settings: Settings,
    position: ExecutionPosition,
    intent: str,
    evaluated_at: datetime,
) -> bool:
    candidate_statuses = (
        ACTIVE_SELL_MONITORING_STATUSES
        | IN_FLIGHT_BROKER_ORDER_ITEM_STATUSES
        | SELL_PENDING_CONFIG_STATUSES
    )
    if intent == "stop_sell":
        candidate_statuses = candidate_statuses | FALLBACK_PROTECTIVE_STOP_STATUSES
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol == position.symbol,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == intent,
            ExecutionPlanItem.status.in_(tuple(candidate_statuses)),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.side,
                ExecutionPlanItem.intent,
                ExecutionPlanItem.status,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.venue,
                ExecutionPlanItem.asset_class,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.trigger_price,
                ExecutionPlanItem.risk_context,
            ),
            selectinload(ExecutionPlanItem.plan).load_only(
                ExecutionPlan.id,
                ExecutionPlan.market_date,
            ),
        )
        .order_by(
            ExecutionPlanItem.updated_at.desc(),
            ExecutionPlanItem.id.desc(),
        )
    ).all()
    candidates = [
        item
        for item in candidates
        if item.plan.market_date is None
        or _replanned_sell_plan_is_effective(
            plan=item.plan,
            item=item,
            effective_at=evaluated_at,
        )
    ]
    eligible_candidates = _quarantine_invalid_sell_monitor_routes(
        settings=settings,
        items=candidates,
        recorded_at=datetime.now(UTC),
        positions_by_symbol={position.symbol: position},
    )
    for item in eligible_candidates:
        if item.status in SELL_PENDING_CONFIG_STATUSES and _is_future_entry_exit_template(item):
            # Only a demonstrably linked future-entry template may yield to
            # broker-position fallback protection.  An explicit shares-based
            # pending exit belongs to the current holding and must retain
            # precedence, including when its threshold is intentionally
            # missing so coverage remains fail-closed/degraded.
            continue
        return True
    return False


def _is_future_entry_exit_template(item: ExecutionPlanItem) -> bool:
    policy = item.quantity_policy
    if not _quantity_policy_requires_position_exit(policy):
        return False
    linked_entry_item_key = policy.get("linked_entry_item_key") if isinstance(policy, dict) else None
    return isinstance(linked_entry_item_key, str) and bool(linked_entry_item_key.strip())


def _new_existing_position_exit_monitor(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    position: ExecutionPosition,
    intent: str,
    trigger_price: Decimal,
    source: str,
    exchange: str,
    venue: str,
    asset_class: str,
    triggered_at: datetime,
) -> ExecutionPlanItem:
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={
            "exchange": exchange,
            "post_entry_managed": True,
            "entry_context": {
                "entry_price": _serialize_decimal(position.average_price),
                "entry_quantity": position.quantity,
            },
        },
        quantity_policy={
            "kind": "shares",
            "quantity": position.quantity,
            "source": source,
        },
    )
    item = ExecutionPlanItem(
        id=new_id(),
        plan=plan,
        plan_item_index=_next_plan_item_index(plan),
        symbol=position.symbol,
        side="sell",
        intent=intent,
        status=_monitoring_status(intent),
        quantity_policy=quantity_policy,
        trigger_price=trigger_price,
        time_in_force="day",
        risk_context=risk_context,
        decision_agent_name="post-entry-monitor-repair",
        source_plan_item_id=f"existing-position-{position.symbol}-{intent}",
        source_market_date=triggered_at.astimezone(
            ZoneInfo("America/New_York") if asset_class == "us_equity" else KRX_TIMEZONE
        ).date(),
        provider="kis",
        venue=venue,
        asset_class=asset_class,
    )
    _record_monitoring_event(
        item=item,
        event={
            "event": "existing_position_exit_monitor_created",
            "reason": source,
            "trigger_price": _serialize_decimal(trigger_price),
            "recorded_at": triggered_at.isoformat(),
        },
    )
    return item


def _entry_market_date(
    *,
    entry_item: ExecutionPlanItem,
    entry_order: BrokerOrder,
) -> date:
    if entry_item.source_market_date is not None:
        return entry_item.source_market_date
    if entry_item.plan is not None and entry_item.plan.market_date is not None:
        return entry_item.plan.market_date
    if entry_order.submitted_at is not None:
        return _normalize_utc(entry_order.submitted_at).astimezone(KRX_TIMEZONE).date()
    return datetime.now(KRX_TIMEZONE).date()


def _holding_session_age_from_date(
    *,
    entry_market_date: date,
    triggered_at: datetime,
) -> int:
    market_date = triggered_at.astimezone(KRX_TIMEZONE).date()
    return max(1, (market_date - entry_market_date).days + 1)


def _post_entry_initial_stop_price(
    *,
    settings: Settings,
    entry_item: ExecutionPlanItem,
    entry_price: Decimal,
) -> Decimal:
    upstream_stop = _watch_buy_stop_price(entry_item)
    hard_stop = (entry_price * (Decimal("1") - settings.exec_agent_stop_loss_pct)).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if upstream_stop is None:
        return hard_stop
    if upstream_stop >= entry_price:
        return hard_stop
    return effective_stop_price(
        settings=settings,
        entry_price=entry_price,
        upstream_stop_price=upstream_stop,
    )


def _ensure_post_entry_stop_monitor(
    *,
    db: Session,
    entry_item: ExecutionPlanItem,
    quantity: int,
    stop_price: Decimal,
    entry_price: Decimal,
    triggered_at: datetime,
) -> tuple[ExecutionPlanItem, int]:
    plan = entry_item.plan
    assert plan is not None
    maintenance_count = 0
    existing = _active_sibling_monitor(
        entry_item=entry_item,
        intent="stop_sell",
    )
    if existing is None:
        existing = ExecutionPlanItem(
            plan=plan,
            plan_item_index=_next_plan_item_index(plan),
            symbol=entry_item.symbol,
            side="sell",
            intent="stop_sell",
            status="stop_sell_monitoring",
            quantity_policy={"kind": "shares", "quantity": quantity},
            trigger_price=stop_price,
            time_in_force=entry_item.time_in_force or "day",
            risk_context={
                "exchange": _plan_item_exchange(entry_item),
                **_inherited_kis_account_route_context(entry_item),
                "entry_context": _post_entry_context(
                    entry_item=entry_item,
                    entry_price=entry_price,
                    quantity=quantity,
                ),
                "deterministic_emergency_stop": True,
                "post_entry_managed": True,
            },
            decision_agent_name=entry_item.decision_agent_name,
            decision_snapshot_id=entry_item.decision_snapshot_id,
            decision_run_id=entry_item.decision_run_id,
            source_plan_item_id=_derived_source_plan_item_id(entry_item, "post-entry-stop"),
            source_market_date=entry_item.source_market_date,
            provider=entry_item.provider,
            venue=entry_item.venue,
            asset_class=entry_item.asset_class,
        )
        db.add(existing)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_created",
                "entry_execution_plan_item_id": entry_item.id,
                "trigger_price": _serialize_decimal(stop_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return existing, 1

    if (
        existing.trigger_price is not None
        and existing.trigger_price >= entry_price
        and not _is_breakeven_rearmed_stop(existing)
    ):
        prior_stop = existing.trigger_price
        existing.trigger_price = stop_price
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_rebased",
                "reason": "upstream_stop_at_or_above_entry",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(stop_price),
                "entry_price": _serialize_decimal(entry_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    elif existing.trigger_price is None or stop_price > existing.trigger_price:
        prior_stop = existing.trigger_price
        existing.trigger_price = stop_price
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_tightened",
                "reason": "effective_entry_risk_stop",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(stop_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    risk_context.setdefault("exchange", _plan_item_exchange(entry_item))
    risk_context.update(_inherited_kis_account_route_context(entry_item))
    risk_context.setdefault("deterministic_emergency_stop", True)
    risk_context.setdefault("post_entry_managed", True)
    risk_context["entry_context"] = _post_entry_context(
        entry_item=entry_item,
        entry_price=entry_price,
        quantity=quantity,
    )
    existing.risk_context = risk_context
    if not _quantity_policy_supported(existing.quantity_policy):
        existing.quantity_policy = {"kind": "shares", "quantity": quantity}
        maintenance_count += 1
    elif _quantity_policy_requires_position_exit(existing.quantity_policy):
        existing.quantity_policy = {
            **dict(existing.quantity_policy or {}),
            "kind": "shares",
            "quantity": quantity,
            "source": "post_entry_position_sync",
        }
        maintenance_count += 1
    if (
        existing.status in SELL_PENDING_CONFIG_STATUSES
        and _monitoring_item_is_configured(existing)
    ):
        prior_status = existing.status
        existing.status = "stop_sell_monitoring"
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_stop_monitor_activated",
                "reason": "repaired_pending_stop_configuration",
                "prior_status": prior_status,
                "updated_status": existing.status,
                "recorded_at": triggered_at.isoformat(),
            },
        )
    return existing, maintenance_count


def _ensure_post_entry_target_monitor(
    *,
    settings: Settings,
    db: Session,
    entry_item: ExecutionPlanItem,
    quantity: int,
    entry_price: Decimal,
    triggered_at: datetime,
) -> tuple[ExecutionPlanItem | None, int]:
    target_price = _watch_buy_target_price(entry_item)
    if target_price is None or target_price <= entry_price:
        target_price = (
            entry_price * (Decimal("1") + settings.exec_agent_first_take_profit_pct)
        ).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
    plan = entry_item.plan
    assert plan is not None
    maintenance_count = 0
    existing = _active_sibling_monitor(
        entry_item=entry_item,
        intent="target_sell",
    )
    if existing is None:
        if _nonretryable_sibling_monitor_suppresses_recreation(
            entry_item=entry_item,
            intent="target_sell",
            trigger_price=target_price,
        ):
            return None, 0
        existing = ExecutionPlanItem(
            plan=plan,
            plan_item_index=_next_plan_item_index(plan),
            symbol=entry_item.symbol,
            side="sell",
            intent="target_sell",
            status="target_sell_monitoring",
            quantity_policy={"kind": "shares", "quantity": quantity},
            trigger_price=target_price,
            time_in_force=entry_item.time_in_force or "day",
            risk_context={
                "exchange": _plan_item_exchange(entry_item),
                **_inherited_kis_account_route_context(entry_item),
                "entry_context": _post_entry_context(
                    entry_item=entry_item,
                    entry_price=entry_price,
                    quantity=quantity,
                ),
                "post_entry_managed": True,
            },
            decision_agent_name=entry_item.decision_agent_name,
            decision_snapshot_id=entry_item.decision_snapshot_id,
            decision_run_id=entry_item.decision_run_id,
            source_plan_item_id=_derived_source_plan_item_id(entry_item, "post-entry-target"),
            source_market_date=entry_item.source_market_date,
            provider=entry_item.provider,
            venue=entry_item.venue,
            asset_class=entry_item.asset_class,
        )
        db.add(existing)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_target_monitor_created",
                "entry_execution_plan_item_id": entry_item.id,
                "trigger_price": _serialize_decimal(target_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return existing, 1

    if existing.trigger_price != target_price:
        prior_target = existing.trigger_price
        existing.trigger_price = target_price
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_target_monitor_updated",
                "prior_trigger_price": _serialize_decimal(prior_target),
                "trigger_price": _serialize_decimal(target_price),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    if not _quantity_policy_supported(existing.quantity_policy):
        existing.quantity_policy = {"kind": "shares", "quantity": quantity}
        maintenance_count += 1
    elif _quantity_policy_requires_position_exit(existing.quantity_policy):
        existing.quantity_policy = {
            **dict(existing.quantity_policy or {}),
            "kind": "shares",
            "quantity": quantity,
            "source": "post_entry_position_sync",
        }
        maintenance_count += 1
    if (
        existing.status in SELL_PENDING_CONFIG_STATUSES
        and _monitoring_item_is_configured(existing)
    ):
        prior_status = existing.status
        existing.status = "target_sell_monitoring"
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_target_monitor_activated",
                "reason": "repaired_pending_target_configuration",
                "prior_status": prior_status,
                "updated_status": existing.status,
                "recorded_at": triggered_at.isoformat(),
            },
        )

    risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    risk_context.setdefault("exchange", _plan_item_exchange(entry_item))
    risk_context.update(_inherited_kis_account_route_context(entry_item))
    risk_context.setdefault("post_entry_managed", True)
    risk_context["entry_context"] = _post_entry_context(
        entry_item=entry_item,
        entry_price=entry_price,
        quantity=quantity,
    )
    existing.risk_context = risk_context
    return existing, maintenance_count


def _nonretryable_sibling_monitor_suppresses_recreation(
    *,
    entry_item: ExecutionPlanItem,
    intent: str,
    trigger_price: Decimal,
) -> bool:
    plan = entry_item.plan
    if plan is None:
        return False
    expected_source = _derived_source_plan_item_id(entry_item, "post-entry-target")
    for sibling in plan.items:
        if (
            sibling.symbol != entry_item.symbol
            or sibling.side != "sell"
            or sibling.intent != intent
            or sibling.source_plan_item_id != expected_source
            or sibling.trigger_price != trigger_price
        ):
            continue
        if any(order.last_error_retryable is False for order in sibling.broker_orders):
            return True
    return False


def _ensure_post_entry_max_holding_monitor(
    *,
    db: Session,
    entry_item: ExecutionPlanItem,
    quantity: int,
    entry_market_date: date,
    triggered_at: datetime,
    max_holding_sessions: int,
) -> tuple[ExecutionPlanItem, int]:
    plan = entry_item.plan
    assert plan is not None
    maintenance_count = 0
    existing = _active_sibling_monitor(
        entry_item=entry_item,
        intent="max_holding_close_sell",
    )
    close_exit_date = _add_trading_sessions(entry_market_date, max_holding_sessions)
    if existing is None:
        existing = ExecutionPlanItem(
            plan=plan,
            plan_item_index=_next_plan_item_index(plan),
            symbol=entry_item.symbol,
            side="sell",
            intent="max_holding_close_sell",
            status="max_holding_close_sell_monitoring",
            quantity_policy={"kind": "position_exit_from_entry"},
            time_in_force=entry_item.time_in_force or "day",
            risk_context={
                "exchange": _plan_item_exchange(entry_item),
                **_inherited_kis_account_route_context(entry_item),
                "close_exit_date": close_exit_date.isoformat(),
                "max_holding_sessions": max_holding_sessions,
                "entry_context": _post_entry_context(
                    entry_item=entry_item,
                    entry_price=None,
                    quantity=quantity,
                ),
                "post_entry_managed": True,
            },
            decision_agent_name=entry_item.decision_agent_name,
            decision_snapshot_id=entry_item.decision_snapshot_id,
            decision_run_id=entry_item.decision_run_id,
            source_plan_item_id=_derived_source_plan_item_id(entry_item, "post-entry-max-holding"),
            source_market_date=entry_item.source_market_date,
            provider=entry_item.provider,
            venue=entry_item.venue,
            asset_class=entry_item.asset_class,
        )
        db.add(existing)
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_max_holding_monitor_created",
                "entry_execution_plan_item_id": entry_item.id,
                "close_exit_date": close_exit_date.isoformat(),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return existing, 1

    risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    if risk_context.get("close_exit_date") != close_exit_date.isoformat():
        risk_context["close_exit_date"] = close_exit_date.isoformat()
        risk_context["max_holding_sessions"] = max_holding_sessions
        existing.risk_context = risk_context
        maintenance_count += 1
        _record_monitoring_event(
            item=existing,
            event={
                "event": "post_entry_max_holding_monitor_updated",
                "close_exit_date": close_exit_date.isoformat(),
                "recorded_at": triggered_at.isoformat(),
            },
        )
        risk_context = dict(existing.risk_context) if isinstance(existing.risk_context, dict) else {}
    risk_context.setdefault("exchange", _plan_item_exchange(entry_item))
    risk_context.update(_inherited_kis_account_route_context(entry_item))
    risk_context.setdefault("post_entry_managed", True)
    risk_context["entry_context"] = _post_entry_context(
        entry_item=entry_item,
        entry_price=None,
        quantity=quantity,
    )
    existing.risk_context = risk_context
    return existing, maintenance_count


def _add_trading_sessions(start: date, sessions: int) -> date:
    current = start
    added = 0
    while added < max(1, sessions):
        current = current + timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _active_sibling_monitor(
    *,
    entry_item: ExecutionPlanItem,
    intent: str,
) -> ExecutionPlanItem | None:
    plan = entry_item.plan
    if plan is None:
        return None
    candidates = [
        sibling
        for sibling in plan.items
        if sibling.symbol == entry_item.symbol
        and sibling.side == "sell"
        and sibling.intent == intent
        and sibling.status not in NON_MONITORABLE_PLAN_ITEM_STATUSES
    ]
    selected = select_plan_item_family_members(
        anchor=entry_item,
        candidates=candidates,
    )
    if selected:
        return selected[0]
    session = sa_inspect(entry_item).session
    if session is None:
        return None
    persisted_candidates = session.scalars(
        select(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.execution_plan_id == entry_item.execution_plan_id,
            ExecutionPlanItem.symbol == entry_item.symbol,
            ExecutionPlanItem.side == "sell",
            ExecutionPlanItem.intent == intent,
            ~ExecutionPlanItem.status.in_(tuple(NON_MONITORABLE_PLAN_ITEM_STATUSES)),
        )
        .order_by(
            ExecutionPlanItem.plan_item_index.asc(),
            ExecutionPlanItem.id.asc(),
        )
    ).all()
    selected = select_plan_item_family_members(
        anchor=entry_item,
        candidates=persisted_candidates,
    )
    return selected[0] if selected else None


def _is_breakeven_rearmed_stop(item: ExecutionPlanItem) -> bool:
    risk_context = _loaded_risk_context(item)
    breakeven_rearmed = risk_context.get("breakeven_rearmed")
    if (
        isinstance(breakeven_rearmed, dict)
        and breakeven_rearmed.get("source") == "partial_take_profit"
    ):
        return True
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    return quantity_policy.get("source") == "partial_take_profit_breakeven_rearm"


def _next_plan_item_index(plan: ExecutionPlan) -> int:
    if not plan.items:
        return 0
    return max(item.plan_item_index for item in plan.items) + 1


def _post_entry_context(
    *,
    entry_item: ExecutionPlanItem,
    entry_price: Decimal | None,
    quantity: int,
) -> dict[str, object]:
    context: dict[str, object] = {
        "source_plan_item_id": entry_item.source_plan_item_id,
        "execution_plan_item_id": entry_item.id,
        "decision_run_id": entry_item.decision_run_id,
        "decision_snapshot_id": entry_item.decision_snapshot_id,
        "entry_quantity": quantity,
    }
    if entry_price is not None:
        context["entry_price"] = _serialize_decimal(entry_price)
    return context


def _inherited_kis_account_route_context(
    entry_item: ExecutionPlanItem,
) -> dict[str, object]:
    binding = kis_account_route_binding_from_payloads(
        entry_item.risk_context,
        entry_item.quantity_policy,
    )
    if binding is None:
        return {}
    return {KIS_ACCOUNT_ROUTE_BINDING_KEY: binding}


def _derived_source_plan_item_id(entry_item: ExecutionPlanItem, suffix: str) -> str | None:
    source = entry_item.source_plan_item_id
    if not source:
        return None
    return f"{source}:{suffix}"


def _post_entry_review_state(
    *,
    settings: Settings,
    entry_price: Decimal,
    observed_price: Decimal | None,
    holding_session: int,
    stop_price: Decimal | None,
    triggered_at: datetime,
) -> dict[str, object]:
    current_pnl_pct = (
        _profit_pct(current_price=observed_price, average_price=entry_price)
        if observed_price is not None
        else None
    )
    adverse_pct = (
        ((entry_price - observed_price) / entry_price) * Decimal("100")
        if observed_price is not None and observed_price < entry_price
        else Decimal("0")
    )
    reasons: list[str] = []
    tightened_stop = stop_price
    if observed_price is not None:
        risk_threshold_pct = settings.exec_agent_stop_loss_pct * Decimal("100")
        if adverse_pct >= risk_threshold_pct:
            reasons.append("gap_down_breached_risk_threshold")
        if holding_session >= 2 and observed_price <= entry_price:
            reasons.append("next_day_no_progress")
            tightened_stop = max(
                stop_price or Decimal("0"),
                (entry_price * Decimal("0.99")).quantize(Decimal("0.0001"), rounding=ROUND_DOWN),
            )
        if holding_session >= settings.exec_agent_max_holding_trading_sessions and observed_price <= entry_price:
            reasons.append("max_holding_without_progress")
    return {
        "requires_review": bool(reasons),
        "reasons": reasons,
        "entry_price": _serialize_decimal(entry_price),
        "observed_price": _serialize_decimal(observed_price),
        "current_pnl_pct": _serialize_decimal(current_pnl_pct),
        "max_adverse_excursion_pct": _serialize_decimal(adverse_pct),
        "holding_session": holding_session,
        "tightened_stop_price": _serialize_decimal(tightened_stop),
        "recorded_at": triggered_at.isoformat(),
    }


def _apply_post_entry_review_state(
    *,
    item: ExecutionPlanItem,
    review: dict[str, object],
    triggered_at: datetime,
) -> int:
    maintenance_count = 0
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    previous = risk_context.get("post_entry_review")
    risk_context["post_entry_review"] = review
    tightened_stop = _parse_decimal(review.get("tightened_stop_price"))
    if tightened_stop is not None and (item.trigger_price is None or tightened_stop > item.trigger_price):
        prior_stop = item.trigger_price
        item.trigger_price = tightened_stop
        maintenance_count += 1
        item.risk_context = compact_json_object(risk_context)
        _record_monitoring_event(
            item=item,
            event={
                "event": "post_entry_stop_monitor_tightened",
                "reason": "post_entry_no_progress_review",
                "prior_trigger_price": _serialize_decimal(prior_stop),
                "trigger_price": _serialize_decimal(tightened_stop),
                "review_reasons": review.get("reasons") or [],
                "recorded_at": triggered_at.isoformat(),
            },
        )
        risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    item.risk_context = compact_json_object(risk_context)
    if review["requires_review"] is True and not (
        isinstance(previous, dict) and previous.get("reasons") == review.get("reasons")
    ):
        maintenance_count += 1
        _record_monitoring_event(
            item=item,
            event={
                "event": "post_entry_review_required",
                "reasons": review.get("reasons") or [],
                "holding_session": review.get("holding_session"),
                "max_adverse_excursion_pct": review.get("max_adverse_excursion_pct"),
                "recorded_at": triggered_at.isoformat(),
            },
        )
    return maintenance_count


def _latest_reconciliation_error_for_symbol(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    exchange: str,
) -> str | None:
    order = db.scalar(
        select(BrokerOrder)
        .where(
            *_active_kis_order_filters(settings=settings, exchange=exchange),
            BrokerOrder.symbol == symbol,
            BrokerOrder.side == "sell",
            BrokerOrder.status.in_(("submitted", "open", "partially_filled")),
            BrokerOrder.last_error_message.is_not(None),
        )
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.symbol,
                BrokerOrder.last_error_message,
                BrokerOrder.last_broker_sync_at,
                BrokerOrder.updated_at,
            )
        )
        .order_by(BrokerOrder.last_broker_sync_at.desc(), BrokerOrder.updated_at.desc())
        .limit(1)
    )
    if order is None:
        return None
    return order.last_error_message


def _reconcile_before_monitoring(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str],
    refresh_positions: bool = True,
) -> None:
    if not symbols:
        return
    if settings.runtime_mode != "live":
        return
    try:
        reconciliation = reconcile_pending_broker_orders(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
    except PositionIdentityConflictError:
        raise
    except Exception:
        return

    # A position that was fresh immediately before reconciliation can become
    # stale as soon as a fill is learned. Refresh it even inside the normal
    # account-snapshot TTL so the next protective order uses post-fill truth.
    if (
        int(reconciliation.get("fills_added", 0)) > 0
        or int(reconciliation.get("reconciled_order_count", 0)) > 0
    ):
        try:
            _sync_positions_for_monitoring_routes(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=symbols,
            )
        except PositionIdentityConflictError:
            raise
        except Exception:
            # The fill-aware reservation pass below still removes executions
            # newer than local position truth if the broker snapshot is down.
            pass
        return

    if not refresh_positions:
        return

    stale_symbols = _positions_with_stale_local_state(
        settings=settings,
        db=db,
        symbols=symbols,
        max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
    )
    if not stale_symbols:
        return
    try:
        _sync_positions_for_monitoring_routes(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=stale_symbols,
        )
    except PositionIdentityConflictError:
        raise
    except Exception:
        return


def _pending_broker_order_symbols(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str] | None = None,
) -> set[str]:
    where = [
        *_active_kis_order_filters(settings=settings),
        BrokerOrder.broker_order_id.is_not(None),
        BrokerOrder.status.in_(
            (
                "submission_pending",
                "submitted",
                "open",
                "partially_filled",
                "cancel_pending",
                "retry_pending",
            )
        ),
    ]
    if symbols:
        where.append(BrokerOrder.symbol.in_(sorted(symbols)))
    return {
        str(symbol)
        for symbol in db.scalars(
            select(distinct(BrokerOrder.symbol)).where(*where)
        ).all()
        if str(symbol).strip()
    }


def _positions_with_stale_local_state(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
    max_age_seconds: int,
) -> set[str]:
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            *_active_kis_position_filters(settings=settings),
            ExecutionPosition.symbol.in_(sorted(symbols)),
        )
    ).all()
    known = _unique_positions_by_symbol(positions)
    stale: set[str] = set()
    stale_before = datetime.now(UTC) - timedelta(seconds=max_age_seconds)
    for symbol in symbols:
        position = known.get(symbol)
        if position is None or position.quantity <= 0:
            stale.add(symbol)
            continue
        if _is_legacy_seeded_reconciliation_position(position):
            continue
        reconciled_at = position.last_reconciled_at
        if reconciled_at is None:
            stale.add(symbol)
            continue
        if _normalize_utc(reconciled_at) < stale_before:
            stale.add(symbol)
    return stale


def _refresh_position_truth_if_stale(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: list[str] | set[str] | None = None,
) -> None:
    if symbols is not None and not symbols:
        return
    symbol_list = _ordered_symbol_scope(symbols)
    if settings.runtime_mode != "live":
        if not symbol_list:
            return
        existing_symbols = set(
            db.scalars(
                select(ExecutionPosition.symbol).where(
                    *_active_kis_position_filters(settings=settings),
                    ExecutionPosition.symbol.in_(symbol_list),
                )
            ).all()
        )
        missing_symbols = set(symbol_list) - existing_symbols
        if not missing_symbols:
            return
        try:
            _sync_positions_for_monitoring_routes(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=missing_symbols,
            )
        except PositionIdentityConflictError:
            raise
        except Exception:
            return
        return
    if symbol_list:
        stale_symbols = _positions_with_stale_local_state(
            settings=settings,
            db=db,
            symbols=set(symbol_list),
            max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
        )
        if not stale_symbols:
            return
        _sync_positions_for_monitoring_routes(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=stale_symbols,
        )
        return
    where = [
        *_active_kis_position_filters(settings=settings),
        ExecutionPosition.quantity > 0,
    ]
    latest_reconciled_at = db.scalar(
        select(func.max(ExecutionPosition.last_reconciled_at)).where(*where)
    )
    stale_before = datetime.now(UTC) - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    if latest_reconciled_at is not None and _normalize_utc(latest_reconciled_at) >= stale_before:
        return
    _sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=None,
    )


def _sync_positions_for_monitoring_routes(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str] | None,
    discover_us_holdings: bool = False,
    discover_krx_holdings: bool = False,
) -> None:
    if str(getattr(adapter, "provider", "")).strip().lower() != "kis":
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
        return

    tracked_symbols = set(symbols or ())
    if not tracked_symbols:
        tracked_symbols.update(
            str(symbol)
            for symbol in db.scalars(
                select(ExecutionPosition.symbol).where(
                    *_active_kis_position_filters(settings=settings),
                )
            ).all()
            if str(symbol).strip()
        )

    discovery_exchanges = set()
    if discover_krx_holdings:
        discovery_exchanges.add("KRX")
    if discover_us_holdings:
        discovery_exchanges.update(("NASD", "NYSE", "AMEX"))
    if discovery_exchanges:
        for exchange in sorted(discovery_exchanges):
            sync_execution_positions_from_broker_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=None,
                exchange=exchange,
                discover_holdings_only=True,
            )
    if not tracked_symbols:
        return

    positions_by_symbol = _unique_positions_by_symbol(
        list(
            db.scalars(
                select(ExecutionPosition).where(
                    *_active_kis_position_filters(settings=settings),
                    ExecutionPosition.symbol.in_(sorted(tracked_symbols)),
                )
            ).all()
        )
    )
    latest_items: dict[str, ExecutionPlanItem] = {}
    rows = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(sorted(tracked_symbols)),
            ExecutionPlanItem.provider == "kis",
        )
        .order_by(ExecutionPlanItem.created_at.desc(), ExecutionPlanItem.plan_item_index.desc())
    ).all()
    for item in rows:
        if classify_kis_account_route(
            settings=settings,
            provider=item.provider,
            risk_context=item.risk_context,
            quantity_policy=item.quantity_policy,
        ) == "active":
            latest_items.setdefault(item.symbol, item)

    symbols_by_exchange: dict[str, set[str]] = {}
    for symbol in sorted(tracked_symbols):
        position = positions_by_symbol.get(symbol)
        position_exchange = _position_sync_exchange(position)
        if position_exchange in MONITORED_KIS_POSITION_EXCHANGES:
            symbols_by_exchange.setdefault(position_exchange, set()).add(symbol)
            continue
        if position is not None and position.quantity > 0:
            _mark_position_monitoring_manual_review(
                position=position,
                reason="position_route_unknown",
                triggered_at=datetime.now(UTC),
            )
            continue

        item = latest_items.get(symbol)
        if item is None:
            if symbol.isdigit() and len(symbol) == 6:
                exchange = "KRX"
            else:
                continue
        elif item.asset_class == "us_equity":
            exchange = _plan_item_exchange(item)
            if exchange not in {"NASD", "NYSE", "AMEX"}:
                raise ValueError(
                    f"KIS US-equity position sync requires an explicit overseas exchange for {symbol}"
                )
        else:
            exchange = "KRX"
        symbols_by_exchange.setdefault(exchange, set()).add(symbol)

    for exchange, routed_symbols in sorted(symbols_by_exchange.items()):
        sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=routed_symbols,
            exchange=exchange,
        )


def _position_sync_exchange(position: ExecutionPosition | None) -> str | None:
    if position is None:
        return None
    try:
        identity = position_identity_from_columns(position)
    except PositionIdentityConflictError:
        return None
    if identity is None:
        return None
    return identity.exchange


def _is_legacy_seeded_reconciliation_position(position: ExecutionPosition) -> bool:
    if position.updated_from != "reconciliation":
        return False
    raw_payload = position.raw_payload
    return isinstance(raw_payload, dict) and raw_payload == {"source": "broker"}


def _monitoring_item_is_configured(item: ExecutionPlanItem) -> bool:
    if _is_watch_buy_item(item):
        return (
            item.side == "buy"
            and _watch_buy_trigger_price(item) is not None
            and (
                _parse_buy_quantity(item.quantity_policy) is not None
                or _watch_buy_candidate_budget(item) is not None
            )
        )
    if item.side != "sell" or not _quantity_policy_supported(item.quantity_policy):
        return False
    if item.intent == "max_holding_close_sell":
        risk_context = _loaded_risk_context(item)
        close_exit_date = risk_context.get("close_exit_date")
        return isinstance(close_exit_date, str) and bool(close_exit_date.strip())
    return item.trigger_price is not None


def _quantity_policy_supported(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    if _quantity_policy_requires_position_exit(quantity_policy):
        return True

    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = quantity_policy.get("quantity")
        return _parse_positive_int(quantity) is not None
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        return fraction is not None
    return False


def _quantity_policy_requires_position_exit(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    policy_type = quantity_policy.get("type")
    if (
        isinstance(policy_type, str)
        and policy_type.strip().lower() == "position_exit_from_entry"
    ):
        return True
    kind = quantity_policy.get("kind")
    return isinstance(kind, str) and kind.strip().lower() == "position_exit_from_entry"


def _is_triggered(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> bool:
    if item.intent == "max_holding_close_sell":
        return _max_holding_exit_due(item=item, triggered_at=triggered_at)
    trigger_price = (
        _watch_buy_trigger_price(item)
        if _is_watch_buy_item(item)
        else item.trigger_price
    )
    if trigger_price is None or observed_price is None:
        return False
    if item.intent == "target_sell":
        return observed_price >= trigger_price
    if item.intent == "stop_sell":
        return observed_price <= trigger_price
    if _is_watch_buy_item(item):
        if _watch_buy_direction(item) == "at_or_below":
            return observed_price <= trigger_price
        return observed_price >= trigger_price
    return False


def _watch_buy_trigger_reason(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
    allow_persisted_trigger: bool = True,
) -> tuple[str | None, dict[str, object] | None]:
    metadata = _watch_buy_intraday_cross_metadata(
        item=item,
        price=price,
        observed_price=observed_price,
        triggered_at=triggered_at,
    )
    if metadata is not None:
        _record_watch_buy_trigger_cross(item=item, metadata=metadata)
        return str(metadata["reason"]), metadata

    if _is_triggered(
        item=item,
        observed_price=observed_price,
        triggered_at=triggered_at,
    ):
        metadata = _watch_buy_trigger_observation_metadata(
            item=item,
            price=price,
            observed_price=observed_price,
            triggered_at=triggered_at,
            reason=item.intent,
        )
        if metadata is not None:
            _record_watch_buy_trigger_cross(item=item, metadata=metadata)
        return item.intent, metadata

    if allow_persisted_trigger:
        persisted = _persisted_watch_buy_trigger_cross(item)
        if persisted is not None:
            return str(persisted.get("reason") or "intraday_watch_buy_trigger_cross_seen"), persisted
    return None, None


def _watch_buy_intraday_cross_metadata(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> dict[str, object] | None:
    if price is None:
        return None
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    if not _watch_buy_observed_price_crosses_trigger(item=item, observed_price=observed_price):
        return None
    direction = _watch_buy_direction(item)
    if direction == "at_or_below":
        if price.day_low is None or price.day_low > trigger_price:
            return None
        return _watch_buy_trigger_observation_metadata(
            item=item,
            price=price,
            observed_price=observed_price,
            triggered_at=triggered_at,
            reason="intraday_watch_buy_day_low_cross",
            cross_price=price.day_low,
        )
    if price.day_high is None or price.day_high < trigger_price:
        return None
    return _watch_buy_trigger_observation_metadata(
        item=item,
        price=price,
        observed_price=observed_price,
        triggered_at=triggered_at,
        reason="intraday_watch_buy_day_high_cross",
        cross_price=price.day_high,
    )


def _watch_buy_trigger_observation_metadata(
    *,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
    reason: str,
    cross_price: Decimal | None = None,
) -> dict[str, object] | None:
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    observed_at = _price_observed_at(price, triggered_at) if price is not None else triggered_at
    metadata: dict[str, object] = {
        "reason": reason,
        "direction": _watch_buy_direction(item),
        "trigger_price": _serialize_decimal(trigger_price),
        "chase_limit_price": _serialize_decimal(_watch_buy_chase_limit_price(item)),
        "observed_price": _serialize_decimal(observed_price),
        "observed_at": observed_at.isoformat(),
        "recorded_at": triggered_at.isoformat(),
    }
    if cross_price is not None:
        metadata["cross_price"] = _serialize_decimal(cross_price)
    if price is not None:
        metadata["day_high"] = _serialize_decimal(price.day_high)
        metadata["day_low"] = _serialize_decimal(price.day_low)
        metadata["volume"] = _serialize_decimal(price.volume)
        metadata["turnover"] = _serialize_decimal(price.turnover)
    return metadata


def _watch_buy_observed_price_crosses_trigger(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
) -> bool:
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None or observed_price is None:
        return False
    if _watch_buy_direction(item) == "at_or_below":
        return observed_price <= trigger_price
    return observed_price >= trigger_price


def _record_watch_buy_trigger_cross(
    *,
    item: ExecutionPlanItem,
    metadata: dict[str, object],
) -> None:
    risk_context = dict(_loaded_risk_context(item))
    previous = risk_context.get("watch_buy_trigger_cross")
    risk_context["watch_buy_trigger_cross"] = metadata
    risk_context["latest_live_market_context"] = {
        key: metadata[key]
        for key in ("day_high", "day_low", "observed_price", "observed_at", "volume", "turnover")
        if key in metadata
    }
    item.risk_context = compact_json_object(risk_context)
    if not isinstance(previous, dict):
        _record_monitoring_event(
            item=item,
            event={
                "event": "watch_buy_trigger_cross_seen",
                **metadata,
            },
        )


def _persisted_watch_buy_trigger_cross(item: ExecutionPlanItem) -> dict[str, object] | None:
    risk_context = _loaded_risk_context(item)
    trigger_cross = risk_context.get("watch_buy_trigger_cross")
    if not isinstance(trigger_cross, dict):
        return None
    persisted_direction = trigger_cross.get("direction")
    current_direction = _watch_buy_direction(item)
    if isinstance(persisted_direction, str) and persisted_direction != current_direction:
        return None
    return trigger_cross


def _primary_triggered_sell_symbols(
    *,
    settings: Settings,
    items: list[ExecutionPlanItem],
    prices: dict[str, MonitoringPriceRequest],
    triggered_at: datetime,
) -> set[str]:
    triggered_symbols: set[str] = set()
    for item in items:
        if _is_watch_buy_item(item) or item.intent not in SELL_MONITORED_INTENTS:
            continue
        if not _monitoring_item_is_configured(item):
            continue
        price = prices.get(item.symbol)
        if _quote_stale_reason(
            settings=settings,
            price=price,
            cycle_at=triggered_at,
        ) is not None:
            continue
        observed_price = _observed_price_for_item(price=price, item=item)
        if _is_triggered(
            item=item,
            observed_price=observed_price,
            triggered_at=triggered_at,
        ):
            triggered_symbols.add(item.symbol)
    return triggered_symbols


def _evaluate_profit_protection(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    observed_price: Decimal | None,
    triggered_at: datetime,
) -> ProfitProtectionDecision | None:
    if item.intent != "target_sell" or observed_price is None:
        return None
    if position is None or position.average_price is None or position.average_price <= 0:
        return None
    current_profit_pct = _profit_pct(
        current_price=observed_price,
        average_price=position.average_price,
    )
    state = _update_profit_protection_state(
        settings=settings,
        item=item,
        observed_price=observed_price,
        current_profit_pct=current_profit_pct,
        triggered_at=triggered_at,
    )
    peak_profit_pct = _parse_decimal(state.get("peak_profit_pct")) or current_profit_pct
    giveback_pct = peak_profit_pct - current_profit_pct
    holding_session = _holding_session_age(item=item, triggered_at=triggered_at)
    metadata = {
        **state,
        "current_profit_pct": _serialize_decimal(current_profit_pct),
        "giveback_pct": _serialize_decimal(giveback_pct),
        "holding_session": holding_session,
        "min_peak_pct": _serialize_decimal(settings.profit_protection_min_peak_pct),
        "giveback_threshold_pct": _serialize_decimal(settings.profit_protection_giveback_pct),
        "aged_session_min": settings.profit_protection_aged_session_min,
        "aged_min_current_profit_pct": _serialize_decimal(
            settings.profit_protection_aged_min_current_profit_pct
        ),
    }
    if (
        peak_profit_pct >= settings.profit_protection_min_peak_pct
        and giveback_pct >= settings.profit_protection_giveback_pct
    ):
        _record_monitoring_event(
            item=item,
            event={
                "event": "profit_protection_triggered",
                "reason": "profit_protection_trailing_stop",
                **metadata,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return ProfitProtectionDecision(
            reason="profit_protection_trailing_stop",
            metadata=metadata,
        )
    if (
        holding_session >= settings.profit_protection_aged_session_min
        and current_profit_pct >= settings.profit_protection_aged_min_current_profit_pct
        and peak_profit_pct > 0
        and giveback_pct >= settings.profit_protection_giveback_pct
    ):
        _record_monitoring_event(
            item=item,
            event={
                "event": "profit_protection_triggered",
                "reason": "aged_position_profit_protection",
                **metadata,
                "recorded_at": triggered_at.isoformat(),
            },
        )
        return ProfitProtectionDecision(
            reason="aged_position_profit_protection",
            metadata=metadata,
        )
    return None


def _persisted_profit_protection_trigger(
    item: ExecutionPlanItem,
) -> ProfitProtectionDecision | None:
    risk_context = _loaded_risk_context(item)
    last_event = risk_context.get("last_monitoring_event")
    if not isinstance(last_event, dict):
        return None
    persisted_trigger = last_event.get("persisted_profit_protection_trigger")
    if isinstance(persisted_trigger, dict):
        persisted_reason = persisted_trigger.get("reason")
        persisted_metadata = persisted_trigger.get("metadata")
        if (
            persisted_reason in PROFIT_PROTECTION_TRIGGER_REASONS
            and isinstance(persisted_metadata, dict)
        ):
            return ProfitProtectionDecision(
                reason=str(persisted_reason),
                metadata=dict(persisted_metadata),
            )
    reason = last_event.get("reason")
    if reason not in PROFIT_PROTECTION_TRIGGER_REASONS:
        return None
    metadata = {
        key: value
        for key, value in last_event.items()
        if key not in {"event", "reason", "recorded_at"}
    }
    return ProfitProtectionDecision(reason=str(reason), metadata=metadata)


def _update_profit_protection_state(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal,
    current_profit_pct: Decimal,
    triggered_at: datetime,
) -> dict[str, object]:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    raw_state = risk_context.get("profit_protection")
    state = dict(raw_state) if isinstance(raw_state, dict) else {}
    peak_price = _parse_decimal(state.get("peak_price"))
    peak_profit_pct = _parse_decimal(state.get("peak_profit_pct"))
    if peak_price is None or observed_price > peak_price:
        peak_price = observed_price
    if peak_profit_pct is None or current_profit_pct > peak_profit_pct:
        peak_profit_pct = current_profit_pct
    updated_state: dict[str, object] = {
        "peak_price": _serialize_decimal(peak_price),
        "peak_profit_pct": _serialize_decimal(peak_profit_pct),
        "current_profit_pct": _serialize_decimal(current_profit_pct),
        "last_observed_price": _serialize_decimal(observed_price),
        "last_observed_at": triggered_at.isoformat(),
        "min_peak_pct": _serialize_decimal(settings.profit_protection_min_peak_pct),
        "giveback_threshold_pct": _serialize_decimal(settings.profit_protection_giveback_pct),
    }
    risk_context["profit_protection"] = updated_state
    item.risk_context = compact_json_object(risk_context)
    return updated_state


def _profit_pct(*, current_price: Decimal, average_price: Decimal) -> Decimal:
    return ((current_price - average_price) / average_price) * Decimal("100")


def _holding_session_age(*, item: ExecutionPlanItem, triggered_at: datetime) -> int:
    source_date = item.source_market_date or item.plan.market_date
    if source_date is None:
        return 1
    market_date = triggered_at.astimezone(KRX_TIMEZONE).date()
    return max(1, (market_date - source_date).days + 1)


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _max_holding_exit_due(*, item: ExecutionPlanItem, triggered_at: datetime) -> bool:
    risk_context = _loaded_risk_context(item)
    raw_close_exit_date = risk_context.get("close_exit_date")
    if not isinstance(raw_close_exit_date, str):
        return False
    try:
        close_exit_date = datetime.strptime(raw_close_exit_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    return triggered_at.astimezone(UTC).date() >= close_exit_date


def _resolve_sell_quantity(
    *,
    item: ExecutionPlanItem,
    remaining_quantity: int | None,
) -> int | None:
    if remaining_quantity is None or remaining_quantity <= 0:
        return None
    quantity_policy = item.quantity_policy
    if not isinstance(quantity_policy, dict):
        return None
    if _quantity_policy_requires_position_exit(quantity_policy):
        return remaining_quantity

    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = _parse_positive_int(quantity_policy.get("quantity"))
        if quantity is None:
            return None
        if quantity > remaining_quantity:
            if item.intent in {"stop_sell", "max_holding_close_sell"}:
                return remaining_quantity
            return None
        return quantity
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        if fraction is None:
            return None
        raw_quantity = (Decimal(remaining_quantity) * fraction).quantize(
            Decimal("1"),
            rounding=ROUND_DOWN,
        )
        quantity = int(raw_quantity)
        if quantity <= 0:
            quantity = 1
        if quantity > remaining_quantity:
            quantity = remaining_quantity
        return quantity
    return None


def _resolve_watch_buy_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    price: MonitoringPriceRequest | None = None,
    execution_price_override: Decimal | None = None,
    requested_quantity: int | None = None,
    trigger_time_agent_approved: bool = False,
    sizing_source: str | None = None,
) -> WatchBuyOrderApproval:
    if is_legacy_unverified_exec_agent_sizing(item.quantity_policy):
        return WatchBuyOrderApproval(
            quantity=None,
            reason="unverified_exec_agent_sizing",
            message="legacy inferred shares lack verified exec-agent approval provenance",
        )
    liquidity_tier = _watch_buy_liquidity_tier(item)
    if liquidity_tier == "very_weak" and _watch_buy_liquidity_override(item) is None:
        approval = WatchBuyOrderApproval(
            quantity=None,
            reason="watch_buy_very_weak_liquidity",
            message="very-weak liquidity watch buys require an explicit operator override",
            details={"liquidity_tier": liquidity_tier},
        )
        _record_live_watch_buy_submission_veto(
            db=db,
            item=item,
            approval=approval,
            recorded_at=datetime.now(UTC),
        )
        return approval
    decision = _exec_agent_decision(item)
    if isinstance(decision, dict):
        decision_value = str(
            decision.get("decision") or decision.get("action") or ""
        ).strip().lower()
        if decision_value == "rejected" or decision_value == "reject":
            return WatchBuyOrderApproval(
                quantity=None,
                reason=str(decision.get("reason") or "exec_agent_rejected"),
                message="exec-agent/risk policy rejected this watch buy",
            )

    quantity = (
        _parse_positive_int(requested_quantity)
        if requested_quantity is not None
        else _parse_buy_quantity(item.quantity_policy)
    )
    if quantity is None and isinstance(decision, dict):
        quantity = _parse_positive_int(
            decision.get("approved_quantity") or decision.get("quantity")
        )
    if (
        quantity is None
        and _watch_buy_requires_exec_agent_sizing(item)
        and not _watch_buy_exec_agent_sizing_fallback_allowed(
            settings=settings,
            item=item,
        )
    ):
        return WatchBuyOrderApproval(
            quantity=None,
            reason="exec_agent_sizing_pending",
            message="watch buy requires an exec-agent share quantity before order submission",
        )

    entry_price = (
        execution_price_override
        or observed_price
        or _watch_buy_trigger_price(item)
        or item.limit_price
    )
    stale_reclaim_veto = _watch_buy_stale_trigger_reclaim_veto(
        settings=settings,
        item=item,
        current_reference_price=entry_price,
        trigger_time_agent_approved=trigger_time_agent_approved,
    )
    if stale_reclaim_veto is not None:
        return stale_reclaim_veto

    late_entry_veto = _watch_buy_late_entry_gate(
        settings=settings,
        db=db,
        item=item,
        price=price,
    )
    if late_entry_veto is not None:
        return late_entry_veto

    live_veto_recorded_at = _price_observed_at(price, datetime.now(UTC)) if price is not None else datetime.now(UTC)
    live_veto = _live_watch_buy_submission_veto(
        settings=settings,
        item=item,
        observed_price=entry_price,
        price=price,
        quantity=quantity,
        recorded_at=live_veto_recorded_at,
    )
    if live_veto is not None:
        _record_live_watch_buy_submission_veto(
            db=db,
            item=item,
            approval=live_veto,
            recorded_at=live_veto_recorded_at,
        )
        return live_veto

    control_state = ensure_runtime_control_state(settings=settings, db=db)
    block_reason = automated_buy_submission_block_reason(control_state)
    if block_reason is not None:
        return WatchBuyOrderApproval(
            quantity=None,
            reason=block_reason,
            message="automated buy submission is blocked by runtime safety controls",
        )

    stop_price = _watch_buy_stop_price(item)
    if entry_price is None or stop_price is None:
        return WatchBuyOrderApproval(
            quantity=None,
            reason="missing_stop_or_entry_price",
            message="watch buy requires entry and stop prices for risk-based sizing",
        )

    snapshot = _load_watch_buy_account_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
    )
    if isinstance(snapshot, str):
        return WatchBuyOrderApproval(
            quantity=None,
            reason="account_snapshot_unavailable",
            message=snapshot,
        )

    available_cash = _minimum_decimal(snapshot.available_cash, snapshot.buying_power)
    existing_exposure = entry_price * Decimal(
        _snapshot_symbol_quantity(snapshot=snapshot, symbol=item.symbol)
        + _local_position_quantity(settings=settings, db=db, item=item)
    )
    effective_stop = effective_stop_price(
        settings=settings,
        entry_price=entry_price,
        upstream_stop_price=stop_price,
    )
    candidate_budget = _watch_buy_candidate_budget(item)
    exec_agent_notional_cap = _watch_buy_exec_agent_notional_cap(item)
    candidate_max_budget = _minimum_decimal(candidate_budget, exec_agent_notional_cap)
    if exec_agent_notional_cap is not None:
        cap_quantity = _floor_quantity(exec_agent_notional_cap / entry_price)
        if cap_quantity <= 0:
            details = {
                "entry_price": _serialize_decimal(entry_price),
                "observed_price": _serialize_decimal(observed_price),
                "exec_agent_approved_notional_cap": _serialize_decimal(exec_agent_notional_cap),
                "stored_quantity": quantity,
                "max_quantity_within_cap": cap_quantity,
            }
            return WatchBuyOrderApproval(
                quantity=None,
                reason="exec_agent_approved_notional_exhausted",
                message="exec-agent approved notional cannot buy one share at the current entry price",
                details=details,
            )
    market_regime_context = market_regime_context_from_item_payloads(_loaded_risk_context(item), item.quantity_policy)
    market_regime_context = merge_live_market_regime_context(
        market_regime_context,
        load_live_market_regime_context(settings=settings),
    )
    risk_decision = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=available_cash,
        available_cash=available_cash,
        entry_price=entry_price,
        stop_price=effective_stop,
        confidence=_watch_buy_confidence(item),
        requested_quantity=quantity,
        candidate_max_budget=candidate_max_budget,
        agent_max_quantity=_watch_buy_max_quantity(item),
        existing_symbol_exposure=existing_exposure,
        daily_new_buy_notional_used=_load_daily_new_buy_notional(
            settings=settings,
            db=db,
            market_date=item.plan.market_date,
        ),
        market_regime_context=market_regime_context,
        market_date=item.plan.market_date,
    )
    if isinstance(risk_decision, RiskSizingRejection):
        details = dict(risk_decision.details)
        if requested_quantity is not None:
            details["requested_quantity"] = requested_quantity
        if sizing_source is not None:
            details["sizing_source"] = sizing_source
        return WatchBuyOrderApproval(
            quantity=None,
            reason=risk_decision.reason_code,
            message=risk_decision.message,
            details=details,
        )
    assert isinstance(risk_decision, RiskSizingResult)
    approved_quantity = risk_decision.quantity
    approved_notional = risk_decision.approved_notional
    exec_agent_cap_adjustment: dict[str, object] | None = None
    safe_quantity_cap = _watch_buy_safe_quantity_cap(item)
    safe_quantity_adjustment: dict[str, object] | None = None
    if safe_quantity_cap is not None and approved_quantity > safe_quantity_cap:
        original_quantity = approved_quantity
        original_notional = approved_notional
        approved_quantity = safe_quantity_cap
        approved_notional = (entry_price * Decimal(approved_quantity)).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
        safe_quantity_adjustment = {
            "reason": "max_safe_quantity_cap",
            "original_quantity": original_quantity,
            "adjusted_quantity": approved_quantity,
            "max_safe_quantity": safe_quantity_cap,
            "original_notional": _serialize_decimal(original_notional),
            "adjusted_notional": _serialize_decimal(approved_notional),
        }
    if exec_agent_notional_cap is not None:
        cap_quantity = _floor_quantity(exec_agent_notional_cap / entry_price)
        if approved_quantity > cap_quantity:
            approved_quantity = cap_quantity
            approved_notional = (entry_price * Decimal(approved_quantity)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_DOWN,
            )
            exec_agent_cap_adjustment = {
                "reason": "exec_agent_approved_notional_cap",
                "entry_price": _serialize_decimal(entry_price),
                "observed_price": _serialize_decimal(observed_price),
                "exec_agent_approved_notional_cap": _serialize_decimal(exec_agent_notional_cap),
                "original_quantity": risk_decision.quantity,
                "adjusted_quantity": approved_quantity,
                "original_notional": _serialize_decimal(risk_decision.approved_notional),
                "adjusted_notional": _serialize_decimal(approved_notional),
            }
    event = {
        "event": "watch_buy_risk_sized",
        "quantity": approved_quantity,
        "approved_notional": _serialize_decimal(approved_notional),
        "risk_pct": _serialize_decimal(risk_decision.risk_pct),
        "risk_budget": _serialize_decimal(risk_decision.risk_budget),
        "stop_price": _serialize_decimal(effective_stop),
        "upstream_stop_price": _serialize_decimal(stop_price),
        "risk_based_quantity": risk_decision.risk_quantity,
        "allocation_quantity": risk_decision.allocation_quantity,
        "cash_quantity": risk_decision.cash_quantity,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    market_regime_guardrail = risk_decision.cap_details.get("market_regime_guardrail")
    if isinstance(market_regime_guardrail, dict):
        event["market_regime_guardrail"] = market_regime_guardrail
    if requested_quantity is not None:
        event["requested_quantity"] = requested_quantity
    if sizing_source is not None:
        event["sizing_source"] = sizing_source
        event["trigger_time_exec_agent_quantity"] = requested_quantity
        if requested_quantity is not None and approved_quantity < requested_quantity:
            event["sizing_adjustment"] = "clamped_by_deterministic_risk_sizing"
    if exec_agent_notional_cap is not None:
        event["exec_agent_approved_notional_cap"] = _serialize_decimal(exec_agent_notional_cap)
    if exec_agent_cap_adjustment is not None:
        event["submission_cap_adjustment"] = exec_agent_cap_adjustment
    if safe_quantity_adjustment is not None:
        event["safe_quantity_cap_adjustment"] = safe_quantity_adjustment
        policy = dict(item.quantity_policy or {})
        explanation = dict(policy.get("sizing_explanation") or {})
        explanation["runtime_safe_quantity_cap"] = safe_quantity_adjustment
        policy["sizing_explanation"] = explanation
        policy["quantity"] = approved_quantity
        item.quantity_policy = compact_json_object(policy)
        db.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="watch_buy_safe_quantity_capped",
                source=MONITORING_WORKER_NAME,
                summary={"status": "capped", "reason": "max_safe_quantity_cap", "symbol": item.symbol},
                payload={"safe_quantity_cap_adjustment": safe_quantity_adjustment},
                occurred_at=datetime.now(UTC),
            )
        )
    if _watch_buy_requires_exec_agent_sizing(item):
        event["sizing_source"] = sizing_source or "risk_sizing_after_exec_agent_attempts_exhausted"
        event["exec_agent_watch_sizing_attempt_count"] = _first_payload_value(
            item,
            "exec_agent_watch_sizing_attempt_count",
        )
    if effective_stop > stop_price:
        _apply_watch_buy_effective_stop(
            item=item,
            effective_stop=effective_stop,
            upstream_stop=stop_price,
        )
        event["stop_monitor_adjustment"] = "raised_to_effective_risk_stop"
    _record_monitoring_event(
        item=item,
        event=event,
    )
    return WatchBuyOrderApproval(quantity=approved_quantity)


def _live_watch_buy_submission_veto(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    price: MonitoringPriceRequest | None,
    quantity: int | None,
    recorded_at: datetime,
) -> WatchBuyOrderApproval | None:
    if not _live_watch_buy_veto_applies(settings=settings, item=item):
        return None
    market_context = market_regime_context_from_item_payloads(_loaded_risk_context(item), item.quantity_policy)
    market_context = merge_live_market_regime_context(
        market_context,
        load_live_market_regime_context(settings=settings, now=recorded_at),
    )
    live_market_authority, authority_reason = (
        trigger_time_exec_agent.evaluate_live_market_buy_authority_gate(
            settings=settings,
            item=item,
            live_payload=market_context,
            session_type=price.session_type if price is not None else None,
            evaluated_at=recorded_at,
        )
    )
    if authority_reason is not None:
        authority_reason_code = str(
            live_market_authority.get("reason_code")
            or "live_market_buy_authority_blocked"
        )
        skipped_notional = (
            observed_price * Decimal(quantity)
            if observed_price is not None and quantity is not None
            else None
        )
        return WatchBuyOrderApproval(
            quantity=None,
            reason=authority_reason_code,
            message=authority_reason,
            details=compact_json_object(
                {
                    "event": LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                    "schema_version": "live_watch_buy_submission_veto_v1",
                    "symbol": item.symbol,
                    "execution_plan_item_id": item.id,
                    "reasons": [authority_reason_code],
                    "primary_reason": authority_reason_code,
                    "runtime_mode": settings.runtime_mode,
                    "provider": item.provider or "kis",
                    "asset_class": item.asset_class or "stock",
                    "venue": item.venue or _plan_item_exchange(item),
                    "skipped_quantity": quantity,
                    "skipped_notional_krw": _serialize_decimal(skipped_notional),
                    "observed_price": _serialize_decimal(observed_price),
                    "live_market_buy_authority_gate": live_market_authority,
                    "recorded_at": recorded_at.isoformat(),
                }
            ),
        )
    skipped_notional = observed_price * Decimal(quantity) if observed_price is not None and quantity is not None else None
    market_guardrail = evaluate_market_regime_guardrail(
        settings=settings,
        context=market_context,
        market_date=item.plan.market_date,
        original_notional=skipped_notional,
        original_quantity=quantity,
        reference_price=observed_price,
        now=recorded_at,
    )
    reasons: list[str] = []
    if market_guardrail.reason_code == "market_breadth_risk_off":
        if _live_watch_buy_market_regime_downsize_veto_applies(settings=settings, action=market_guardrail.action):
            reasons.append("market_breadth_risk_off")
    elif (
        market_guardrail.reason_code.startswith("market_breadth_stale")
        or "market_breadth_stale" in market_guardrail.audit.get("warnings", [])
    ):
        if (
            not settings.watch_buy_live_market_regime_downsize_allowed
            and _live_watch_buy_breadth_override(item=item) is None
        ):
            reasons.append("market_breadth_stale")
    elif market_guardrail.posture == "risk_off":
        if _live_watch_buy_market_regime_downsize_veto_applies(settings=settings, action=market_guardrail.action):
            reasons.append(market_guardrail.reason_code or "market_breadth_risk_off")
    elif _live_watch_buy_market_regime_downsize_veto_applies(
        settings=settings,
        action=market_guardrail.action,
    ):
        reasons.append(market_guardrail.reason_code)

    if settings.watch_buy_live_market_quality_veto_enabled:
        trigger_quality = _live_watch_buy_trigger_quality(price=price)
        quote_trust_reason = _live_automated_quote_trust_reason(
            settings=settings,
            item=item,
            price=price,
        )
        if quote_trust_reason is not None:
            reasons.append("untrusted_quote_provenance")
        if trigger_quality["volume"] is not None and trigger_quality["volume"] <= 0:
            reasons.append("zero_trigger_volume")
        if trigger_quality["turnover"] is not None and trigger_quality["turnover"] <= 0:
            reasons.append("zero_trigger_turnover")
        if trigger_quality["day_high"] is None or trigger_quality["day_low"] is None:
            reasons.append("missing_day_high_low")
        turnover = trigger_quality["turnover"]
        min_turnover = settings.watch_buy_live_min_trigger_turnover_krw
        if turnover is not None and turnover > 0 and min_turnover > 0 and turnover < min_turnover:
            reasons.append("low_trigger_turnover")
    else:
        trigger_quality = _live_watch_buy_trigger_quality(price=price)

    if not reasons:
        return None
    deduped_reasons = list(dict.fromkeys(reasons))
    payload = {
        "event": LIVE_WATCH_BUY_VETO_EVENT_TYPE,
        "schema_version": "live_watch_buy_submission_veto_v1",
        "symbol": item.symbol,
        "execution_plan_item_id": item.id,
        "reasons": deduped_reasons,
        "primary_reason": deduped_reasons[0],
        "runtime_mode": settings.runtime_mode,
        "provider": item.provider or "kis",
        "asset_class": item.asset_class or "stock",
        "venue": item.venue or _plan_item_exchange(item),
        "skipped_quantity": quantity,
        "skipped_notional_krw": _serialize_decimal(skipped_notional),
        "observed_price": _serialize_decimal(observed_price),
        "trigger_quality": {
            "volume": _serialize_decimal(trigger_quality["volume"]),
            "turnover": _serialize_decimal(trigger_quality["turnover"]),
            "day_high": _serialize_decimal(trigger_quality["day_high"]),
            "day_low": _serialize_decimal(trigger_quality["day_low"]),
            "min_turnover_krw": _serialize_decimal(settings.watch_buy_live_min_trigger_turnover_krw),
            "price_observed_at": _serialize_datetime(_price_observed_at(price, recorded_at) if price is not None else None),
            "provenance": price.provenance if price is not None else {},
        },
        "market_regime_guardrail": market_guardrail.audit,
        "live_market_buy_authority_gate": live_market_authority,
        "market_regime_reason_code": market_guardrail.reason_code,
        "market_regime_posture": market_guardrail.posture,
        "market_regime_action": market_guardrail.action,
        "override": _live_watch_buy_breadth_override(item=item),
        "recorded_at": recorded_at.isoformat(),
    }
    return WatchBuyOrderApproval(
        quantity=None,
        reason=deduped_reasons[0],
        message="live automated watch buy vetoed before broker submission",
        details=compact_json_object(payload),
    )


def _live_watch_buy_veto_applies(*, settings: Settings, item: ExecutionPlanItem) -> bool:
    if settings.runtime_mode != "live":
        return False
    provider = str(item.provider or "kis").strip().lower()
    if provider not in {"kis", "korea_investment", "korea-investment"}:
        return False
    try:
        asset_class = canonical_execution_asset_class(
            item.asset_class or "stock",
            provider=provider,
            exchange=item.venue or _plan_item_exchange(item) or "KRX",
        )
    except ValueError:
        return False
    if asset_class != "stock":
        return False
    venue = str(item.venue or _plan_item_exchange(item) or "KRX").strip().upper()
    return venue in KRX_EXCHANGES


def _live_automated_quote_trust_reason(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
) -> str | None:
    if not (_is_watch_buy_item(item) or item.side == "sell"):
        return None
    provider = str(item.provider or "kis").strip().lower()
    side = "buy" if _is_watch_buy_item(item) else "sell"
    return _live_automated_quote_trust_reason_for_side(
        settings=settings,
        provider=provider,
        side=side,
        price=price,
    )


def _live_automated_quote_trust_reason_for_side(
    *,
    settings: Settings,
    provider: str,
    side: str,
    price: MonitoringPriceRequest | None,
) -> str | None:
    if settings.runtime_mode != "live":
        return None
    normalized_provider = provider.strip().lower()
    if normalized_provider not in {"kis", "korea_investment", "korea-investment"}:
        return None
    normalized_side = side.strip().lower()
    if normalized_side not in {"buy", "sell"}:
        return "live automated KIS quote side is invalid"
    if price is None:
        return f"live automated KIS {normalized_side} requires a fresh exchange quote"
    if not _is_trusted_exchange_freshness_source(price.freshness_source):
        return (
            f"live automated KIS {normalized_side} quote lacks trusted exchange-time "
            "provenance"
        )
    executable_field = "ask" if normalized_side == "buy" else "bid"
    executable_price = getattr(price, executable_field)
    if executable_price is None or executable_price <= 0:
        return (
            f"live automated KIS {normalized_side} quote lacks a positive executable "
            f"{executable_field}"
        )
    provenance = price.provenance if isinstance(price.provenance, dict) else {}
    field_sources = provenance.get("field_sources")
    field_source = (
        field_sources.get(executable_field)
        if isinstance(field_sources, dict)
        else None
    )
    if not isinstance(field_source, dict) or not _is_trusted_exchange_freshness_source(
        field_source.get("freshness_source")
    ):
        return (
            f"live automated KIS {normalized_side} {executable_field} lacks trusted "
            "field-level exchange-time provenance"
        )
    field_observed_at = _parse_event_datetime(field_source.get("observed_at"))
    if field_observed_at is None or price.observed_at is None:
        return (
            f"live automated KIS {normalized_side} {executable_field} lacks a verifiable "
            "field timestamp"
        )
    if _normalize_utc(field_observed_at) != _normalize_utc(price.observed_at):
        return (
            f"live automated KIS {normalized_side} {executable_field} timestamp does not "
            "match the quote freshness boundary"
        )
    return None


def _live_watch_buy_trigger_quality(*, price: MonitoringPriceRequest | None) -> dict[str, Decimal | None]:
    if price is None:
        return {"volume": None, "turnover": None, "day_high": None, "day_low": None}
    return {
        "volume": price.volume,
        "turnover": price.turnover,
        "day_high": price.day_high,
        "day_low": price.day_low,
    }


def _live_watch_buy_market_regime_downsize_veto_applies(
    *,
    settings: Settings,
    action: str,
) -> bool:
    if settings.watch_buy_live_market_regime_downsize_allowed:
        return False
    return action in {"downsize", "caution"}


def _live_watch_buy_breadth_override(*, item: ExecutionPlanItem) -> dict[str, object] | None:
    for source in _item_payload_sources(item):
        override = source.get("live_watch_buy_breadth_override") or source.get("market_breadth_override")
        if not isinstance(override, dict) or override.get("enabled") is not True:
            continue
        actor = str(override.get("actor") or override.get("operator") or "").strip()
        reason = str(override.get("reason") or override.get("rationale") or "").strip()
        if len(actor) < 2 or len(reason) < 20:
            continue
        return {
            "enabled": True,
            "actor": _bounded_text(actor, max_length=120),
            "reason": _bounded_text(reason, max_length=240),
            "recorded_at": str(override.get("recorded_at") or override.get("approved_at") or ""),
        }
    return None


def _record_live_watch_buy_submission_veto(
    *,
    db: Session,
    item: ExecutionPlanItem,
    approval: WatchBuyOrderApproval,
    recorded_at: datetime,
) -> None:
    payload = approval.details or {}
    _record_monitoring_event(
        item=item,
        event={
            "event": "watch_buy_rejected",
            "reason": approval.reason or "live_watch_buy_submission_veto",
            "message": approval.message,
            "details": payload,
            "recorded_at": recorded_at.isoformat(),
        },
    )
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=LIVE_WATCH_BUY_VETO_EVENT_TYPE,
            source=MONITORING_WORKER_NAME,
            summary={
                "status": "watch_buy_rejected",
                "reason": approval.reason,
                "symbol": item.symbol,
                "skipped_quantity": payload.get("skipped_quantity"),
                "skipped_notional_krw": payload.get("skipped_notional_krw"),
            },
            payload=payload,
            error_message=approval.message,
            occurred_at=recorded_at,
        )
    )


def _watch_buy_late_entry_gate(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    price: MonitoringPriceRequest | None,
) -> WatchBuyOrderApproval | None:
    mode = settings.watch_buy_late_entry_mode
    if mode == "disabled":
        return None
    observed_at = _price_observed_at(price, datetime.now(UTC))
    observed_krx = observed_at.astimezone(KRX_TIMEZONE)
    cutoff = datetime.strptime(settings.watch_buy_late_entry_cutoff_krx, "%H:%M").time()
    if observed_krx.time().replace(tzinfo=None) <= cutoff:
        return None

    evidence = {
        "mode": mode,
        "reason": "late_entry_cutoff",
        "cutoff_krx": settings.watch_buy_late_entry_cutoff_krx,
        "observed_at": observed_at.isoformat(),
        "observed_at_krx": observed_krx.isoformat(),
        "market_date_krx": observed_krx.date().isoformat(),
        "symbol": item.symbol,
        "broker_mutations": False,
    }
    previous = _loaded_risk_context(item).get("last_monitoring_event")
    duplicate = (
        isinstance(previous, dict)
        and previous.get("event") == LATE_ENTRY_GATE_EVENT_TYPE
        and previous.get("mode") == mode
        and previous.get("market_date_krx") == evidence["market_date_krx"]
        and previous.get("cutoff_krx") == evidence["cutoff_krx"]
    )
    if not duplicate:
        day_start_krx = observed_krx.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end_krx = day_start_krx + timedelta(days=1)
        prior_audits = db.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == item.id,
                ExecutionPlanItemAudit.event_type == LATE_ENTRY_GATE_EVENT_TYPE,
                ExecutionPlanItemAudit.occurred_at >= day_start_krx.astimezone(UTC),
                ExecutionPlanItemAudit.occurred_at < day_end_krx.astimezone(UTC),
            )
        ).all()
        duplicate = any(
            isinstance(audit.summary, dict)
            and audit.summary.get("mode") == mode
            and audit.summary.get("market_date_krx") == evidence["market_date_krx"]
            for audit in prior_audits
        )
    if not duplicate:
        _record_monitoring_event(
            item=item,
            event={"event": LATE_ENTRY_GATE_EVENT_TYPE, **evidence},
        )
        db.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type=LATE_ENTRY_GATE_EVENT_TYPE,
                source=MONITORING_WORKER_NAME,
                summary={
                    "mode": mode,
                    "reason": "late_entry_cutoff",
                    "symbol": item.symbol,
                    "market_date_krx": evidence["market_date_krx"],
                },
                payload=evidence,
                occurred_at=observed_at,
            )
        )
    if mode == "shadow":
        return None
    return WatchBuyOrderApproval(
        quantity=None,
        reason="late_entry_cutoff",
        message=(
            "watch buy is after the configured KRX entry cutoff "
            f"{settings.watch_buy_late_entry_cutoff_krx}"
        ),
        details=evidence,
    )


def _watch_buy_stale_trigger_reclaim_veto(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    current_reference_price: Decimal | None,
    trigger_time_agent_approved: bool,
) -> WatchBuyOrderApproval | None:
    if current_reference_price is None:
        return None
    trigger_price = _watch_buy_trigger_price(item)
    if trigger_price is None:
        return None
    if _watch_buy_direction(item) != "at_or_above":
        return None
    threshold_pct = settings.watch_buy_stale_trigger_reclaim_threshold_pct
    if threshold_pct <= 0:
        return None
    stale_reference_price = (trigger_price * (Decimal("1") - (threshold_pct / Decimal("100")))).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if current_reference_price > stale_reference_price:
        return None
    details = {
        "reason": "stale_trigger_reclaim_required",
        "trigger_price": _serialize_decimal(trigger_price),
        "current_reference_price": _serialize_decimal(current_reference_price),
        "reference_price": _serialize_decimal(current_reference_price),
        "stale_reference_price": _serialize_decimal(stale_reference_price),
        "threshold_pct": _serialize_decimal(threshold_pct),
        "direction": _watch_buy_direction(item),
    }
    if _watch_buy_is_pullback_strategy(item):
        _record_watch_buy_stale_trigger_reclaim_bypass(
            item=item,
            details={**details, "bypass_reason": "pullback_strategy_profile"},
        )
        return None
    if (
        trigger_time_agent_approved
        and _trigger_time_decision_confirms_fresh_pullback_reclaim(item)
    ):
        _record_watch_buy_stale_trigger_reclaim_bypass(
            item=item,
            details={**details, "bypass_reason": "trigger_time_pullback_reclaim_confirmation"},
        )
        return None
    return WatchBuyOrderApproval(
        quantity=None,
        reason="stale_trigger_reclaim_required",
        message="current reference price is materially below the original breakout/reclaim trigger",
        details=details,
    )


def _record_watch_buy_stale_trigger_reclaim_bypass(
    *,
    item: ExecutionPlanItem,
    details: dict[str, object],
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    risk_context["last_stale_trigger_reclaim_check"] = details
    item.risk_context = compact_json_object(risk_context)


def _watch_buy_is_pullback_strategy(item: ExecutionPlanItem) -> bool:
    for source in _item_payload_sources(item):
        for key in (
            "strategy_profile",
            "strategy_type",
            "setup_profile",
            "setup_type",
            "entry_profile",
            "tactical_action",
            "handoff_intent",
        ):
            value = source.get(key)
            if isinstance(value, str) and "pullback" in value.strip().lower():
                return True
        strategy = source.get("strategy")
        if isinstance(strategy, dict):
            for key in ("profile", "type", "name"):
                value = strategy.get(key)
                if isinstance(value, str) and "pullback" in value.strip().lower():
                    return True
        if source.get("pullback_strategy") is True:
            return True
    return False


def _trigger_time_decision_confirms_fresh_pullback_reclaim(item: ExecutionPlanItem) -> bool:
    risk_context = _loaded_risk_context(item)
    interaction = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return False
    decision = interaction.get("decision")
    if not isinstance(decision, dict):
        return False
    if decision.get("decision") != "execute_now":
        return False
    has_updated_stop = _parse_positive_decimal(decision.get("updated_stop_price")) is not None
    has_updated_target = _parse_positive_decimal(decision.get("updated_target_price")) is not None
    has_updated_trigger = (
        _parse_positive_decimal(decision.get("updated_trigger_price")) is not None
    )
    if not (has_updated_stop and has_updated_target and has_updated_trigger):
        return False
    text = str(decision).lower()
    return "pullback" in text or "reclaim" in text


def _apply_watch_buy_effective_stop(
    *,
    item: ExecutionPlanItem,
    effective_stop: Decimal,
    upstream_stop: Decimal,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    risk_context["effective_stop_price"] = _serialize_decimal(effective_stop)
    risk_context["upstream_stop_price"] = _serialize_decimal(upstream_stop)
    risk_context["stop_price"] = _serialize_decimal(effective_stop)
    risk_context["stop_loss_price"] = _serialize_decimal(effective_stop)
    risk_context["stop_monitor_adjustment"] = "raised_to_effective_risk_stop"
    item.risk_context = compact_json_object(risk_context)
    _update_sibling_trigger_price(
        item=item,
        intent="stop_sell",
        trigger_price=effective_stop,
    )


def _parse_buy_quantity(quantity_policy: dict[str, object] | None) -> int | None:
    if not isinstance(quantity_policy, dict):
        return None
    if quantity_policy.get("kind") != "shares":
        return None
    return _parse_positive_int(quantity_policy.get("quantity"))


def _watch_buy_requires_exec_agent_sizing(item: ExecutionPlanItem) -> bool:
    return is_delegated_exec_agent_sizing_policy(item.quantity_policy)


def _watch_buy_exec_agent_sizing_fallback_allowed(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> bool:
    # An exhausted agent retry budget is not approval provenance. Arithmetic
    # budget/reference fallback previously converted delegated sizing into a
    # live buy without a verified decision, so delegated policies stay closed.
    del settings, item
    return False


def _first_payload_value(item: ExecutionPlanItem, key: str) -> object | None:
    for payload in (item.quantity_policy, _loaded_risk_context(item)):
        if isinstance(payload, dict) and key in payload:
            return payload.get(key)
    return None


def _watch_buy_candidate_budget(item: ExecutionPlanItem) -> Decimal | None:
    for source in _item_payload_sources(item):
        for key in (
            "max_budget",
            "max_budget_krw",
            "budget",
            "budget_krw",
            "cash_budget",
            "cash_budget_krw",
            "max_notional",
            "max_notional_krw",
            "max_safe_notional",
            "max_safe_notional_krw",
            "notional",
        ):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    return None


def _watch_buy_exec_agent_notional_cap(item: ExecutionPlanItem) -> Decimal | None:
    caps: list[Decimal] = []
    for source in _item_payload_sources(item):
        source_name = str(source.get("source") or "").strip().lower()
        if source_name == "exec_agent_watch_sizing":
            for key in ("approved_notional", "approved_notional_krw"):
                value = _parse_positive_decimal(source.get(key))
                if value is not None:
                    caps.append(value)
        sizing_adjustment = source.get("sizing_adjustment")
        if isinstance(sizing_adjustment, dict):
            for key in (
                "final_notional",
                "adjusted_notional",
                "approved_notional",
                "cap_notional",
                "agent_approved_notional",
            ):
                value = _parse_positive_decimal(sizing_adjustment.get(key))
                if value is not None:
                    caps.append(value)
    return min(caps) if caps else None


def _watch_buy_max_quantity(item: ExecutionPlanItem) -> int | None:
    for source in _item_payload_sources(item):
        for key in ("max_quantity", "max_shares", "share_cap"):
            quantity = _parse_positive_int(source.get(key))
            if quantity is not None:
                return quantity
    return None


def _watch_buy_safe_quantity_cap(item: ExecutionPlanItem) -> int | None:
    for source in _item_payload_sources(item):
        candidates = [source]
        feasibility = source.get("execution_feasibility_context")
        if isinstance(feasibility, dict):
            candidates.append(feasibility)
        for candidate in candidates:
            if candidate.get("budget_exceeds_safe_notional") is not True:
                continue
            quantity = _parse_positive_int(candidate.get("max_safe_quantity"))
            if quantity is not None:
                return quantity
    return None


def _watch_buy_liquidity_tier(item: ExecutionPlanItem) -> str | None:
    for source in _item_payload_sources(item):
        candidates = [source]
        feasibility = source.get("execution_feasibility_context")
        if isinstance(feasibility, dict):
            candidates.append(feasibility)
        for candidate in candidates:
            value = str(candidate.get("liquidity_tier") or "").strip().lower()
            if value:
                return value
    return None


def _watch_buy_liquidity_override(item: ExecutionPlanItem) -> dict[str, object] | None:
    for source in _item_payload_sources(item):
        override = source.get("watch_buy_liquidity_override")
        if not isinstance(override, dict) or override.get("enabled") is not True:
            continue
        actor = str(override.get("actor") or override.get("operator") or "").strip()
        reason = str(override.get("reason") or override.get("rationale") or "").strip()
        if actor and reason:
            return {"enabled": True, "actor": actor, "reason": reason}
    return None


def _watch_buy_confidence(item: ExecutionPlanItem) -> Decimal | None:
    for source in _item_payload_sources(item):
        for key in ("confidence", "score", "probability"):
            value = _parse_positive_decimal(source.get(key))
            if value is None:
                continue
            if value > 1 and value <= 100:
                value = value / Decimal("100")
            if value <= 1:
                return value
    decision = _exec_agent_decision(item)
    if isinstance(decision, dict):
        value = _parse_positive_decimal(decision.get("confidence"))
        if value is not None:
            return value / Decimal("100") if value > 1 and value <= 100 else value
    return None


def _watch_buy_stop_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        (
            "stop_price",
            "stop_sell_price",
            "stop_loss_price",
            "risk_stop_price",
            "stop",
        ),
    )


def _watch_buy_target_price(item: ExecutionPlanItem) -> Decimal | None:
    return _first_positive_decimal_from_item(
        item,
        (
            "target_price",
            "take_profit_price",
            "first_take_profit_price",
            "profit_target_price",
            "target",
        ),
    )


def _load_watch_buy_account_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
) -> BrokerAccountSnapshot | str:
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="watch_buy_risk_sizing",
            reason="resolve account buying power for intraday watch buy sizing",
        )
        return adapter.get_account_snapshot(session_state.access_token or "")
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError) as exc:
        return f"{exc.__class__.__name__}: {exc}"


def _load_watch_buy_cash_reservation(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    triggered_at: datetime,
) -> WatchBuyCashReservation | str:
    snapshot = _load_watch_buy_account_snapshot(settings=settings, db=db, adapter=adapter)
    if isinstance(snapshot, str):
        return snapshot
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    open_orders_error = snapshot_payload.get("open_orders_error")
    if settings.runtime_mode == "live" and not snapshot.open_orders_complete:
        return "broker open-order truth is incomplete; live watch-buy sizing is blocked"
    if (
        settings.runtime_mode == "live"
        and isinstance(open_orders_error, dict)
        and open_orders_error.get("degraded") is True
    ):
        return "broker open-order truth is degraded; live watch-buy sizing is blocked"
    orderable_cash = _minimum_decimal(snapshot.available_cash, snapshot.buying_power)
    if orderable_cash is None:
        return "account snapshot did not include orderable cash"
    already_reserved, local_reservation_complete = _local_open_buy_order_notional(
        settings=settings,
        db=db,
        snapshot_open_orders=snapshot.open_orders,
    )
    if settings.runtime_mode == "live" and not local_reservation_complete:
        return (
            "an earlier buy submission has unknown broker outcome and no defensible "
            "capital reservation"
        )
    return WatchBuyCashReservation(
        orderable_cash=orderable_cash,
        already_reserved_notional=already_reserved,
        batch_id=f"monitoring:{MONITORING_WORKER_NAME}:{triggered_at.isoformat()}",
    )


def _local_open_buy_order_notional(
    *,
    settings: Settings,
    db: Session,
    snapshot_open_orders: list[BrokerAccountOpenOrder],
) -> tuple[Decimal, bool]:
    valued_snapshot_orders: list[BrokerAccountOpenOrder] = []
    unvalued_snapshot_orders: list[BrokerAccountOpenOrder] = []
    total = Decimal("0")
    for snapshot_order in snapshot_open_orders:
        if snapshot_order.side != "buy":
            continue
        remaining_quantity = snapshot_order.remaining_quantity or snapshot_order.quantity
        if remaining_quantity <= 0:
            continue
        snapshot_notional = _open_buy_order_notional(
            quantity=remaining_quantity,
            limit_price=snapshot_order.limit_price,
        )
        if snapshot_notional is None:
            unvalued_snapshot_orders.append(snapshot_order)
            continue
        total += snapshot_notional
        valued_snapshot_orders.append(snapshot_order)
    candidate_orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == "kis",
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(tuple(OPEN_BUY_ORDER_STATUSES)),
        )
    ).all()
    complete = True
    orders: list[BrokerOrder] = []
    for order in candidate_orders:
        scope_status, _exchange = _kis_order_scope_classification(
            settings=settings,
            order=order,
        )
        if scope_status == "active":
            orders.append(order)
        elif scope_status == "invalid":
            # A legacy KIS order could belong to the active account. Its
            # reservation cannot be safely ignored in live sizing.
            complete = False
    for order in orders:
        if any(
            _same_open_order_identity(local=order, snapshot=snapshot_order)
            for snapshot_order in valued_snapshot_orders
        ):
            continue
        reserved_notional = _local_broker_order_buy_notional(order)
        if reserved_notional is None:
            complete = False
            continue
        total += reserved_notional
        unvalued_snapshot_orders = [
            snapshot_order
            for snapshot_order in unvalued_snapshot_orders
            if not _same_open_order_identity(local=order, snapshot=snapshot_order)
        ]
    return total, complete and not unvalued_snapshot_orders


def _same_open_order_identity(
    *,
    local: BrokerOrder,
    snapshot: BrokerAccountOpenOrder,
) -> bool:
    if local.broker_order_id and snapshot.broker_order_id:
        return local.broker_order_id == snapshot.broker_order_id
    return bool(
        local.idempotency_key
        and snapshot.idempotency_key
        and local.idempotency_key == snapshot.idempotency_key
    )


def _local_broker_order_buy_notional(order: BrokerOrder) -> Decimal | None:
    if order.quote_notional is not None and order.quote_notional > 0:
        return Decimal(order.quote_notional)
    if order.limit_price is not None and order.limit_price > 0:
        if order.base_quantity is not None and order.base_quantity > 0:
            return Decimal(order.base_quantity) * Decimal(order.limit_price)
        if order.quantity is not None and order.quantity > 0:
            return Decimal(order.quantity) * Decimal(order.limit_price)
    if order.plan_item is not None:
        approved_notional = _approved_buy_notional(order.plan_item)
        if approved_notional > 0:
            return approved_notional
    return None


def _open_buy_order_notional(
    *,
    quantity: int | None,
    limit_price: Decimal | None,
) -> Decimal | None:
    if quantity is None or quantity <= 0 or limit_price is None:
        return None
    if limit_price <= 0:
        return None
    return Decimal(quantity) * limit_price


def _watch_buy_order_notional(*, quantity: int, limit_price: Decimal | None) -> Decimal | None:
    if quantity <= 0 or limit_price is None:
        return None
    return Decimal(quantity) * limit_price


def _watch_buy_cash_reservation_payload(
    *,
    reservation: WatchBuyCashReservation,
    candidate_notional: Decimal,
    item: ExecutionPlanItem,
    quantity: int,
    limit_price: Decimal,
    triggered_at: datetime,
) -> dict[str, object]:
    return {
        "reason": RESERVED_CASH_EXHAUSTED_REASON,
        "batch_id": reservation.batch_id,
        "cycle_id": reservation.batch_id,
        "orderable_cash": _serialize_decimal(reservation.orderable_cash),
        "available_cash": _serialize_decimal(reservation.remaining_cash),
        "already_reserved_notional": _serialize_decimal(reservation.already_reserved_notional),
        "candidate_notional": _serialize_decimal(candidate_notional),
        "quantity": quantity,
        "limit_price": _serialize_decimal(limit_price),
        "symbol": item.symbol,
        "execution_plan_item_id": item.id,
        "recorded_at": triggered_at.isoformat(),
    }


def _record_reserved_cash_exhausted(
    *,
    db: Session,
    item: ExecutionPlanItem,
    payload: dict[str, object],
    triggered_at: datetime,
) -> None:
    _record_monitoring_event(
        item=item,
        event={
            "event": "watch_buy_rejected",
            "reason": RESERVED_CASH_EXHAUSTED_REASON,
            "message": "watch buy reserved cash budget exhausted before broker submission",
            "details": payload,
            "recorded_at": triggered_at.isoformat(),
        },
    )
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=RESERVED_CASH_EXHAUSTED_REASON,
            source=MONITORING_WORKER_NAME,
            summary={
                "reason": RESERVED_CASH_EXHAUSTED_REASON,
                "batch_id": payload.get("batch_id"),
                "symbol": item.symbol,
            },
            payload=payload,
            error_message="watch buy reserved cash budget exhausted before broker submission",
            occurred_at=triggered_at,
        )
    )


def _snapshot_symbol_quantity(*, snapshot: BrokerAccountSnapshot, symbol: str) -> int:
    positions = snapshot.positions or snapshot.holdings
    return sum(position.quantity for position in positions if position.symbol == symbol and position.quantity > 0)


def _local_position_quantity(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
) -> int:
    position = _position_for_monitoring_item(
        settings=settings,
        item=item,
        positions_by_symbol=_load_positions_by_symbol(
            settings=settings,
            db=db,
            symbols={item.symbol},
        ),
    )
    return position.quantity if position is not None and position.quantity > 0 else 0


def _load_daily_new_buy_notional(
    *,
    settings: Settings,
    db: Session,
    market_date,
) -> Decimal:
    if market_date is None:
        return Decimal("0")
    uncertain_statuses = ("submission_pending", "manual_review_required")
    uncertain_order_candidates = db.scalars(
        select(BrokerOrder)
        .join(
            ExecutionPlanItem,
            BrokerOrder.execution_plan_item_id == ExecutionPlanItem.id,
        )
        .join(ExecutionPlan, ExecutionPlanItem.execution_plan_id == ExecutionPlan.id)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            BrokerOrder.broker_name == "kis",
            BrokerOrder.side == "buy",
            BrokerOrder.status.in_(uncertain_statuses),
        )
    ).all()
    # An unscoped legacy KIS order cannot be attributed to another account, so
    # excluding it could understate committed capital. Count active and
    # indeterminate rows conservatively; exclude only a positively identified
    # foreign account scope.
    uncertain_orders = [
        order
        for order in uncertain_order_candidates
        if _kis_order_scope_classification(settings=settings, order=order)[0]
        != "foreign"
    ]
    uncertain_item_ids = {
        order.execution_plan_item_id
        for order in uncertain_orders
        if order.execution_plan_item_id is not None
    }
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.status.in_(
                (
                    "next_open_buy_scheduled",
                    "next_open_buy_dispatching",
                    "next_open_buy_retry_pending",
                    "watch_buy_submitted",
                    "broker_order_submission_pending",
                    "broker_order_submitted",
                    "broker_order_retry_pending",
                    "submitted",
                    "open",
                    "partially_filled",
                    "filled",
                    "retry_pending",
                )
            ),
        )
        .options(
            load_only(
                ExecutionPlanItem.id,
                ExecutionPlanItem.execution_plan_id,
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.side,
                ExecutionPlanItem.status,
                ExecutionPlanItem.quantity_policy,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.provider,
                ExecutionPlanItem.limit_price,
                ExecutionPlanItem.trigger_price,
            )
        )
    ).all()
    total = Decimal("0")
    for item in items:
        if item.id in uncertain_item_ids:
            continue
        if (
            classify_kis_account_route(
                settings=settings,
                provider=item.provider,
                risk_context=item.risk_context,
                quantity_policy=item.quantity_policy,
            )
            == "foreign"
        ):
            continue
        total += _approved_buy_notional(item)
    for order in uncertain_orders:
        reserved_notional = _local_broker_order_buy_notional(order)
        if reserved_notional is not None:
            total += reserved_notional
    return total


def _approved_buy_notional(item: ExecutionPlanItem) -> Decimal:
    for source in (item.quantity_policy,):
        if not isinstance(source, dict):
            continue
        decision = source.get("exec_agent_decision")
        if isinstance(decision, dict):
            value = _parse_positive_decimal(decision.get("approved_notional"))
            if value is not None:
                return value
        for key in ("approved_notional", "notional", "max_notional"):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    quantity = _parse_buy_quantity(item.quantity_policy)
    price = item.limit_price or _watch_buy_trigger_price(item)
    if quantity is None or price is None:
        return Decimal("0")
    return price * Decimal(quantity)


def _minimum_decimal(*values: Decimal | None) -> Decimal | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return min(concrete)


def _floor_quantity(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _monitoring_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_monitoring_active"
    return f"{intent}_monitoring"


def _pending_dispatch_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_buy_triggered_pending_dispatch"
    return f"{intent}_triggered_pending_dispatch"


def _pending_configuration_status(intent: str) -> str:
    if intent in WATCH_BUY_INTENTS:
        return "watch_monitoring_pending"
    return f"{intent}_pending_config"


def _reserve_remaining_quantity(
    *,
    remaining_quantities_by_symbol: dict[str, int],
    symbol: str,
    quantity: int,
) -> None:
    remaining_quantity = remaining_quantities_by_symbol.get(symbol)
    if remaining_quantity is None or remaining_quantity <= 0 or quantity <= 0:
        return
    updated_quantity = remaining_quantity - quantity
    if updated_quantity > 0:
        remaining_quantities_by_symbol[symbol] = updated_quantity
        return
    remaining_quantities_by_symbol.pop(symbol, None)


def _reserve_and_rearm_sell_after_success(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    remaining_quantities_by_symbol: dict[str, int],
    submitted_quantity: int,
    recorded_at: datetime,
) -> None:
    _reserve_remaining_quantity(
        remaining_quantities_by_symbol=remaining_quantities_by_symbol,
        symbol=item.symbol,
        quantity=submitted_quantity,
    )
    _rearm_stop_after_partial_take_profit(
        settings=settings,
        item=item,
        position=position,
        remaining_quantity=remaining_quantities_by_symbol.get(item.symbol),
        submitted_quantity=submitted_quantity,
        recorded_at=recorded_at,
    )


def _reserve_existing_exit_orders(
    *,
    settings: Settings,
    db: Session,
    remaining_quantities_by_symbol: dict[str, int],
    positions_by_symbol: dict[str, ExecutionPosition],
    rearmed_plan_item_ids: set[str],
) -> None:
    if not remaining_quantities_by_symbol:
        return
    scoped_symbols = sorted(remaining_quantities_by_symbol)
    active_sell_candidates = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == "kis",
            BrokerOrder.side == "sell",
            BrokerOrder.symbol.in_(scoped_symbols),
            BrokerOrder.status.in_(tuple(CAPITAL_RESERVING_ORDER_STATUSES)),
        )
    ).all()
    for order in active_sell_candidates:
        scope_status, _exchange = _kis_order_scope_classification(
            settings=settings,
            order=order,
        )
        if scope_status == "invalid":
            # A legacy active sell may belong to the position account. Do not
            # submit replacement quantity until its route can be established.
            remaining_quantities_by_symbol.pop(order.symbol, None)
    if not remaining_quantities_by_symbol:
        return
    where = [
        *_active_kis_order_filters(settings=settings),
        BrokerOrder.side == "sell",
        BrokerOrder.symbol.in_(sorted(remaining_quantities_by_symbol)),
        BrokerOrder.status.in_(tuple(CAPITAL_RESERVING_ORDER_STATUSES)),
    ]
    if rearmed_plan_item_ids:
        # Its retry-pending row represents the stop being rearmed, not a second
        # concurrent exit. Other active rows for the same item still reserve.
        where.append(
            or_(
                BrokerOrder.execution_plan_item_id.is_(None),
                BrokerOrder.execution_plan_item_id.not_in(rearmed_plan_item_ids),
                BrokerOrder.status != "retry_pending",
            )
        )
    orders = db.scalars(
        select(BrokerOrder)
        .where(*where)
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.symbol,
                BrokerOrder.side,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.broker_name,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
                BrokerOrder.request_metadata,
                BrokerOrder.last_broker_sync_at,
                BrokerOrder.updated_at,
            )
        )
        .order_by(BrokerOrder.updated_at.asc(), BrokerOrder.id.asc())
    ).all()
    orders = [
        order
        for order in orders
        if _active_kis_order_matches_position(
            settings=settings,
            order=order,
            position=positions_by_symbol.get(order.symbol),
        )
    ]
    symbols = sorted(remaining_quantities_by_symbol)
    terminal_where = [
        *_active_kis_order_filters(settings=settings),
        BrokerOrder.side == "sell",
        BrokerOrder.symbol.in_(symbols),
        BrokerOrder.status == "filled",
    ]
    reconciled_times = [
        _normalize_utc(position.last_reconciled_at)
        for symbol in symbols
        if (position := positions_by_symbol.get(symbol)) is not None
        and position.last_reconciled_at is not None
    ]
    if len(reconciled_times) == len(symbols):
        earliest_reconciled_at = min(reconciled_times)
        terminal_where.append(
            or_(
                BrokerOrder.last_broker_sync_at > earliest_reconciled_at,
                and_(
                    BrokerOrder.last_broker_sync_at.is_(None),
                    BrokerOrder.updated_at > earliest_reconciled_at,
                ),
            )
        )
    terminal_filled_orders = db.scalars(
        select(BrokerOrder)
        .where(*terminal_where)
        .options(
            load_only(
                BrokerOrder.id,
                BrokerOrder.symbol,
                BrokerOrder.status,
                BrokerOrder.quantity,
                BrokerOrder.broker_name,
                BrokerOrder.venue,
                BrokerOrder.asset_class,
                BrokerOrder.request_metadata,
                BrokerOrder.last_broker_sync_at,
                BrokerOrder.updated_at,
            )
        )
    ).all()
    terminal_filled_orders = [
        order
        for order in terminal_filled_orders
        if _active_kis_order_matches_position(
            settings=settings,
            order=order,
            position=positions_by_symbol.get(order.symbol),
        )
    ]
    relevant_order_ids = [
        order.id for order in [*orders, *terminal_filled_orders]
    ]
    fill_scope = [ExecutionFill.broker_order_pk.in_(relevant_order_ids)]
    for symbol in symbols:
        position = positions_by_symbol.get(symbol)
        reconciled_at = position.last_reconciled_at if position is not None else None
        fill_scope.append(
            ExecutionFill.symbol == symbol
            if reconciled_at is None
            else and_(
                ExecutionFill.symbol == symbol,
                ExecutionFill.filled_at > _normalize_utc(reconciled_at),
            )
        )
    fills = dedupe_execution_fills(
        db.scalars(
            select(ExecutionFill)
            .join(BrokerOrder, BrokerOrder.id == ExecutionFill.broker_order_pk)
            .where(
                *_active_kis_order_filters(settings=settings),
                BrokerOrder.side == "sell",
                ExecutionFill.side == "sell",
                or_(*fill_scope),
            )
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    )
    fills = [
        fill
        for fill in fills
        if _active_kis_order_matches_position(
            settings=settings,
            order=fill.broker_order,
            position=positions_by_symbol.get(fill.symbol),
        )
    ]
    filled_quantities: dict[str, Decimal] = {}
    fills_before_position_by_order: dict[str, Decimal] = {}
    unreflected_by_symbol: dict[str, Decimal] = {}
    uncertain_order_ids = {
        order.id
        for order in orders
        if order.status in {"submission_pending", "manual_review_required"}
    }
    for fill in fills:
        quantity = effective_fill_quantity(fill)
        filled_quantities[fill.broker_order_pk] = (
            filled_quantities.get(fill.broker_order_pk, Decimal("0")) + quantity
        )
        position = positions_by_symbol.get(fill.symbol)
        reconciled_at = position.last_reconciled_at if position is not None else None
        if reconciled_at is not None and _normalize_utc(fill.filled_at) <= _normalize_utc(
            reconciled_at
        ):
            fills_before_position_by_order[fill.broker_order_pk] = (
                fills_before_position_by_order.get(fill.broker_order_pk, Decimal("0"))
                + quantity
            )
            continue
        if fill.broker_order_pk in uncertain_order_ids:
            # Full original quantity is reserved below for ambiguous outcomes.
            continue
        unreflected_by_symbol[fill.symbol] = (
            unreflected_by_symbol.get(fill.symbol, Decimal("0")) + quantity
        )

    # Some broker status payloads prove a full fill without itemized fills. If
    # that proof is newer than position truth, reserve the missing executed
    # quantity as well; otherwise a TTL-fresh pre-fill position could produce
    # an oversized replacement stop.
    for order in terminal_filled_orders:
        if order.quantity is None or order.quantity <= 0:
            continue
        position = positions_by_symbol.get(order.symbol)
        reconciled_at = position.last_reconciled_at if position is not None else None
        broker_observed_at = order.last_broker_sync_at or order.updated_at
        if (
            reconciled_at is not None
            and broker_observed_at is not None
            and _normalize_utc(broker_observed_at) <= _normalize_utc(reconciled_at)
        ):
            continue
        known_before = fills_before_position_by_order.get(order.id, Decimal("0"))
        known_after = sum(
            (
                effective_fill_quantity(fill)
                for fill in fills
                if fill.broker_order_pk == order.id
                and (
                    reconciled_at is None
                    or _normalize_utc(fill.filled_at) > _normalize_utc(reconciled_at)
                )
            ),
            Decimal("0"),
        )
        implied_after = max(Decimal(order.quantity) - known_before, Decimal("0"))
        missing_after = max(implied_after - known_after, Decimal("0"))
        if missing_after > 0:
            unreflected_by_symbol[order.symbol] = (
                unreflected_by_symbol.get(order.symbol, Decimal("0")) + missing_after
            )

    for symbol, quantity in unreflected_by_symbol.items():
        _reserve_remaining_quantity(
            remaining_quantities_by_symbol=remaining_quantities_by_symbol,
            symbol=symbol,
            quantity=max(int(quantity), 0),
        )
    for order in orders:
        if order.quantity is None or order.quantity <= 0:
            # Unknown active sell quantity is not safely replaceable.
            remaining_quantities_by_symbol.pop(order.symbol, None)
            continue
        if order.status in {
            "submission_pending",
            "manual_review_required",
        }:
            reserved_quantity = order.quantity
        else:
            reserved_quantity = max(
                int(Decimal(order.quantity) - filled_quantities.get(order.id, Decimal("0"))),
                0,
            )
        _reserve_remaining_quantity(
            remaining_quantities_by_symbol=remaining_quantities_by_symbol,
            symbol=order.symbol,
            quantity=reserved_quantity,
        )


def _rearm_stop_after_partial_take_profit(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    position: ExecutionPosition | None,
    remaining_quantity: int | None,
    submitted_quantity: int,
    recorded_at: datetime,
) -> None:
    if item.intent != "target_sell" or remaining_quantity is None or remaining_quantity <= 0:
        return
    exit_rule = _exit_rule(item)
    if exit_rule.get("kind") != "partial_take_profit":
        return
    if position is None or position.average_price is None or position.average_price <= 0:
        return

    breakeven_stop = _breakeven_stop_price(
        average_price=position.average_price,
        buffer_pct=settings.breakeven_stop_buffer_pct,
    )
    plan = item.plan
    if plan is None:
        return

    stop_candidates = [
        sibling
        for sibling in plan.items
        if sibling.symbol == item.symbol
        and sibling.intent == "stop_sell"
        and sibling.status not in NON_MONITORABLE_PLAN_ITEM_STATUSES
    ]
    for sibling in select_plan_item_family_members(
        anchor=item,
        candidates=stop_candidates,
    ):

        prior_trigger = sibling.trigger_price
        prior_quantity_policy = sibling.quantity_policy
        if prior_trigger is None or prior_trigger < breakeven_stop:
            sibling.trigger_price = breakeven_stop
        sibling.quantity_policy = {
            **(
                dict(prior_quantity_policy)
                if isinstance(prior_quantity_policy, dict)
                else {}
            ),
            "kind": "shares",
            "quantity": remaining_quantity,
            "source": "partial_take_profit_breakeven_rearm",
            "parent_target_item_id": item.id,
        }
        risk_context = dict(sibling.risk_context) if isinstance(sibling.risk_context, dict) else {}
        risk_context["breakeven_rearmed"] = {
            "source": "partial_take_profit",
            "target_item_id": item.id,
            "submitted_take_profit_quantity": submitted_quantity,
            "remaining_quantity": remaining_quantity,
            "average_price": _serialize_decimal(position.average_price),
            "breakeven_stop_price": _serialize_decimal(sibling.trigger_price),
            "buffer_pct": _serialize_decimal(settings.breakeven_stop_buffer_pct),
            "prior_trigger_price": _serialize_decimal(prior_trigger),
            "prior_quantity_policy": prior_quantity_policy,
            "recorded_at": recorded_at.isoformat(),
        }
        sibling.risk_context = risk_context
        _record_monitoring_event(
            item=sibling,
            event={
                "event": "stop_sell_rearmed_after_partial_take_profit",
                "reason": "partial_take_profit_submitted",
                "target_item_id": item.id,
                "submitted_take_profit_quantity": submitted_quantity,
                "remaining_quantity": remaining_quantity,
                "prior_trigger_price": _serialize_decimal(prior_trigger),
                "updated_trigger_price": _serialize_decimal(sibling.trigger_price),
                "average_price": _serialize_decimal(position.average_price),
                "buffer_pct": _serialize_decimal(settings.breakeven_stop_buffer_pct),
                "recorded_at": recorded_at.isoformat(),
            },
        )


def _exit_rule(item: ExecutionPlanItem) -> dict[str, object]:
    risk_context = _loaded_risk_context(item)
    exit_rule = risk_context.get("exit_rule")
    return dict(exit_rule) if isinstance(exit_rule, dict) else {}


def _breakeven_stop_price(*, average_price: Decimal, buffer_pct: Decimal) -> Decimal:
    multiplier = Decimal("1") + (buffer_pct / Decimal("100"))
    return (average_price * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _serialized_order_quantity(order_payload: object) -> int | None:
    if not isinstance(order_payload, dict):
        return None
    return _parse_positive_int(order_payload.get("quantity"))


def _serialized_order_status(order_payload: object) -> str | None:
    if not isinstance(order_payload, dict):
        return None
    status = order_payload.get("status")
    if not isinstance(status, str) or not status.strip():
        return None
    return status.strip().lower()


def _plan_item_status_from_order_payload(order_payload: object) -> str | None:
    status = _serialized_order_status(order_payload)
    if status not in {value.value for value in NormalizedBrokerOrderStatus}:
        return None
    return plan_item_status_for_broker_order_status(status)


def _placement_preserves_current_plan_status(payload: dict[str, object]) -> bool:
    return payload.get("block_reason") in {
        "execution_plan_item_state_changed",
        "account_drawdown_recovery_candidate_ineligible",
        "account_drawdown_recovery_lineage_invalid",
        "account_drawdown_recovery_state_changed",
    }


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if (
        not parsed.is_finite()
        or parsed <= 0
        or parsed != parsed.to_integral_value()
    ):
        return None
    return int(parsed)


def _parse_fraction(value: object) -> Decimal | None:
    try:
        fraction = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not fraction.is_finite() or fraction <= 0 or fraction > 1:
        return None
    return fraction


def _is_watch_buy_item(item: ExecutionPlanItem) -> bool:
    return item.side == "buy" and (
        item.intent in WATCH_BUY_INTENTS or item.status in WATCH_LIFECYCLE_STATUSES
    )


def _watch_buy_trigger_price(item: ExecutionPlanItem) -> Decimal | None:
    if item.trigger_price is not None and item.trigger_price > 0:
        return item.trigger_price
    return _first_positive_decimal_from_item(
        item,
        (
            "buy_recommendation_price",
            "recommended_entry_price",
            "entry_recommendation_price",
            "recommendation_price",
            "entry_price",
            "watch_trigger_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "original_watch_trigger_price",
        ),
    )


def _watch_buy_chase_limit_price(item: ExecutionPlanItem) -> Decimal | None:
    configured = _first_positive_decimal_from_item(
        item,
        ("chase_limit_price", "max_chase_limit_price", "entry_limit_price", "original_watch_chase_limit_price"),
    )
    context = item.risk_context if isinstance(item.risk_context, dict) else {}
    reward_risk_cap = _parse_decimal(context.get("reward_risk_max_entry_price"))
    if reward_risk_cap is None or reward_risk_cap <= 0:
        return configured
    return min(configured, reward_risk_cap) if configured is not None else reward_risk_cap


def _watch_buy_high_risk_gate_rejection(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal,
    price: MonitoringPriceRequest | None = None,
    trigger_reason: str | None = None,
    trigger_metadata: dict[str, object] | None = None,
    triggered_at: datetime | None = None,
) -> dict[str, object] | None:
    if not settings.watch_buy_high_risk_gate_enabled:
        return None
    sources = []
    live_market_context = _market_quality_context_from_price(
        price,
        item=item,
    )
    if live_market_context is not None:
        sources.append(live_market_context)
    sources.extend(_item_payload_sources(item))
    payload = {"sources": sources}
    signal_action = (_recursive_text_feature(payload, {"action"}, parent_key="signal_decider_crosscheck") or "").lower()
    tactical_action = (
        _recursive_text_feature(payload, {"tactical_action"})
        or _recursive_text_feature(payload, {"handoff_intent"})
        or ""
    ).lower()
    if signal_action == "reject":
        return _watch_buy_gate_rejection(
            reason="signal_decider_reject_veto",
            message="same-day signal-decider rejected this setup",
            details={"signal_decider_action": signal_action, "tactical_action": tactical_action},
        )
    if signal_action == "wait" and tactical_action in {"scalp_buy_candidate", "tactical_buy"}:
        return _watch_buy_gate_rejection(
            reason="signal_decider_wait_blocks_tactical_buy",
            message="same-day signal-decider is wait, so tactical buy requires a stricter watch setup",
            details={"signal_decider_action": signal_action, "tactical_action": tactical_action},
        )

    atr_pct = _recursive_decimal_feature(payload, {"atr_pct_14", "atr_pct", "atr14_pct", "atr_percent"})
    ret20 = _recursive_decimal_feature(payload, {"ret20", "ret_20d", "return_20d", "pct_change_20d"})
    ret5 = _recursive_decimal_feature(payload, {"ret5", "ret_5d", "return_5d", "pct_change_5d"})
    trend_state = (_recursive_text_feature(payload, {"trend_state", "technical_trend", "market_regime"}) or "").lower()
    setup_state = (_recursive_text_feature(payload, {"setup_state", "breakdown_state", "pattern_state"}) or "").lower()
    details = {
        "atr_pct_14": _serialize_decimal(atr_pct),
        "ret20": _serialize_decimal(ret20),
        "ret5": _serialize_decimal(ret5),
        "trend_state": trend_state or None,
        "setup_state": setup_state or None,
    }
    trigger_price = _watch_buy_trigger_price(item)
    direction = _watch_buy_direction(item)
    pullback_after_cross = (
        trigger_price is not None
        and direction == "at_or_above"
        and observed_price < trigger_price
    )
    chase_decision = None
    if not pullback_after_cross:
        chase_decision = _watch_buy_high_volatility_chase_decision(
            settings=settings,
            item=item,
            observed_price=observed_price,
            trigger_reason=trigger_reason,
            trigger_metadata=trigger_metadata,
            payload=payload,
            atr_pct=atr_pct,
            ret5=ret5,
            triggered_at=triggered_at,
        )
    if chase_decision is not None and chase_decision["outcome"] in {"veto", "legacy_veto"}:
        return _watch_buy_gate_rejection(
            reason=str(chase_decision["reason"]),
            message=str(chase_decision["message"]),
            details=chase_decision,
        )
    if any(marker in f"{trend_state} {setup_state}" for marker in ("downtrend", "bearish", "breakdown_20d", "breakdown")):
        return _watch_buy_gate_rejection(
            reason="short_term_downtrend_or_breakdown_block",
            message="watch buy setup is downtrend/bearish/breakdown",
            details=details,
        )
    if ret20 is not None and ret20 < settings.watch_buy_deep_negative_ret20_threshold_pct:
        return _watch_buy_gate_rejection(
            reason="short_term_ret20_deep_negative_block",
            message="20-day return is deeply negative for a short-term watch buy",
            details=details,
        )
    if (
        atr_pct is not None
        and atr_pct >= settings.watch_buy_high_atr_pct_threshold
        and ret20 is not None
        and ret20 <= settings.watch_buy_weak_ret20_threshold_pct
    ):
        return _watch_buy_gate_rejection(
            reason="short_term_high_atr_weak_trend_block",
            message="ATR is high while 20-day return is weak",
            details=details,
        )
    day_high = _recursive_decimal_feature(payload, {"day_high", "intraday_high", "high_price", "session_high"})
    day_low = _recursive_decimal_feature(payload, {"day_low", "intraday_low", "low_price", "session_low"})
    if day_high is not None and day_low is not None and day_high > day_low:
        range_position_pct = ((observed_price - day_low) / (day_high - day_low)) * Decimal("100")
        if (
            range_position_pct > settings.watch_buy_max_intraday_range_position_pct
            and not pullback_after_cross
        ):
            return _watch_buy_gate_rejection(
                reason="watch_buy_intraday_range_chase_block",
                message="triggered price is too high in the current intraday range",
                details={
                    **details,
                    "day_high": _serialize_decimal(day_high),
                    "day_low": _serialize_decimal(day_low),
                    "range_position_pct": _serialize_decimal(range_position_pct),
                    "max_range_position_pct": _serialize_decimal(settings.watch_buy_max_intraday_range_position_pct),
                },
            )
    return None


def _watch_buy_high_volatility_chase_decision(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    payload: dict[str, object],
    atr_pct: Decimal | None,
    ret5: Decimal | None,
    triggered_at: datetime | None = None,
) -> dict[str, object] | None:
    if not _watch_buy_is_chase_trigger(
        item=item,
        trigger_reason=trigger_reason,
        trigger_metadata=trigger_metadata,
    ):
        return None
    context_state = _watch_buy_technical_context_state(
        settings=settings,
        item=item,
        payload=payload,
        triggered_at=triggered_at,
    )
    legacy_matched = (
        atr_pct is not None
        and atr_pct >= settings.watch_buy_high_atr_pct_threshold
        and ret5 is not None
        and ret5 >= settings.watch_buy_extended_ret5_threshold_pct
    )
    details: dict[str, object] = {
        "rule_id": "high_volatility_chase_veto",
        "trigger_reason": trigger_reason,
        "atr_pct_14": _serialize_decimal(atr_pct),
        "return_5d": _serialize_decimal(ret5),
        "atr_threshold_pct": _serialize_decimal(settings.watch_buy_high_volatility_chase_atr_pct_threshold),
        "return_5d_threshold_pct": _serialize_decimal(settings.watch_buy_high_volatility_chase_ret5_threshold_pct),
        "legacy_atr_threshold_pct": _serialize_decimal(settings.watch_buy_high_atr_pct_threshold),
        "legacy_return_5d_threshold_pct": _serialize_decimal(settings.watch_buy_extended_ret5_threshold_pct),
        "technical_context": context_state,
        "observed_price": _serialize_decimal(observed_price),
        "live_veto_enabled": settings.watch_buy_high_volatility_chase_veto_enabled,
    }
    missing_or_stale = context_state["status"] != "fresh"
    matched = (
        atr_pct is not None
        and atr_pct > settings.watch_buy_high_volatility_chase_atr_pct_threshold
        and ret5 is not None
        and ret5 > settings.watch_buy_high_volatility_chase_ret5_threshold_pct
    )
    if not settings.watch_buy_high_volatility_chase_veto_enabled:
        if legacy_matched:
            decision = {
                **details,
                "outcome": "legacy_veto",
                "reason": "short_term_high_atr_extended_ret5_block",
                "message": "5-day move is extended while ATR is high for a chase buy",
            }
            _persist_watch_buy_high_volatility_chase_decision(item=item, decision=decision)
            return decision
        if matched:
            decision = {
                **details,
                "outcome": "shadow_veto",
                "reason": "short_term_high_volatility_chase_block",
                "message": "5-day move is extended while ATR is extreme for a chase buy",
            }
            _persist_watch_buy_high_volatility_chase_decision(item=item, decision=decision)
        return None

    if not matched:
        return None

    override = _watch_buy_high_volatility_chase_override(
        item=item,
        observed_price=observed_price,
    )
    if override is not None:
        decision = {
            **details,
            "outcome": "override",
            "reason": "high_volatility_chase_override",
            "message": "explicit stronger-signal override accepted with tighter stop",
            "override": override,
        }
        _persist_watch_buy_high_volatility_chase_decision(item=item, decision=decision)
        return decision

    reason = (
        "stale_or_missing_technical_context_for_chase"
        if missing_or_stale
        else "short_term_high_volatility_chase_block"
    )
    message = (
        "fresh technical context is required before allowing a chase buy"
        if missing_or_stale
        else "5-day move is extended while ATR is extreme for a chase buy"
    )
    decision = {
        **details,
        "outcome": "veto" if settings.watch_buy_high_volatility_chase_veto_enabled else "shadow_veto",
        "reason": reason,
        "message": message,
    }
    _persist_watch_buy_high_volatility_chase_decision(item=item, decision=decision)
    return decision if settings.watch_buy_high_volatility_chase_veto_enabled else None


def _watch_buy_is_chase_trigger(
    *,
    item: ExecutionPlanItem,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
) -> bool:
    reasons = {
        str(trigger_reason or "").strip().lower(),
        str((trigger_metadata or {}).get("reason") or "").strip().lower(),
    }
    if reasons & {
        "intraday_watch_buy_day_high_cross",
        "intraday_watch_buy_breakout_cross",
        "intraday_watch_buy_momentum_cross",
    }:
        return True
    return _watch_buy_direction(item) == "at_or_above" and _watch_buy_tactical_action(item) in {
        "watch_for_trigger",
        "breakout_buy",
        "momentum_buy",
        "tactical_buy",
    }


def _watch_buy_technical_context_state(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    payload: dict[str, object],
    triggered_at: datetime | None = None,
) -> dict[str, object]:
    as_of = _recursive_text_feature(
        payload,
        {"technical_context_as_of", "core_technical_as_of", "snapshot_at", "as_of", "market_date"},
    )
    if as_of is None and item.source_market_date is not None:
        as_of = item.source_market_date.isoformat()
    parsed = _parse_date_or_datetime(as_of)
    if parsed is None:
        return {"status": "missing", "as_of": as_of, "max_age_days": settings.watch_buy_high_volatility_chase_max_context_age_days}
    reference_date = (
        triggered_at.astimezone(KRX_TIMEZONE).date()
        if triggered_at is not None
        else datetime.now(UTC).astimezone(KRX_TIMEZONE).date()
    )
    age_days = (reference_date - parsed).days
    status = "fresh" if age_days <= settings.watch_buy_high_volatility_chase_max_context_age_days else "stale"
    return {
        "status": status,
        "as_of": parsed.isoformat(),
        "age_days": age_days,
        "max_age_days": settings.watch_buy_high_volatility_chase_max_context_age_days,
    }


def _parse_date_or_datetime(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.astimezone(KRX_TIMEZONE).date() if value.tzinfo is not None else value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).astimezone(KRX_TIMEZONE).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _watch_buy_high_volatility_chase_override(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal,
) -> dict[str, object] | None:
    risk_context = _loaded_risk_context(item)
    override = risk_context.get("high_volatility_chase_override")
    if not isinstance(override, dict) or override.get("enabled") is not True:
        return None
    reason = str(override.get("reason") or override.get("rationale") or "").strip()
    tighter_stop = _parse_positive_decimal(
        override.get("tighter_stop_price")
        or override.get("updated_stop_price")
        or override.get("stop_price")
    )
    current_stop = _watch_buy_stop_price(item)
    if len(reason) < 20 or tighter_stop is None or current_stop is None:
        return None
    if not (current_stop < tighter_stop < observed_price):
        return None
    updated_context = dict(risk_context)
    updated_context["stop_price"] = _serialize_decimal(tighter_stop)
    updated_context["stop_loss_price"] = _serialize_decimal(tighter_stop)
    item.risk_context = compact_json_object(updated_context)
    if isinstance(item.quantity_policy, dict):
        updated_policy = dict(item.quantity_policy)
        updated_policy["stop_price"] = _serialize_decimal(tighter_stop)
        updated_policy["stop_loss_price"] = _serialize_decimal(tighter_stop)
        item.quantity_policy = compact_json_object(updated_policy)
    return {
        "reason": _bounded_text(reason, max_length=240),
        "prior_stop_price": _serialize_decimal(current_stop),
        "tighter_stop_price": _serialize_decimal(tighter_stop),
    }


def _persist_watch_buy_high_volatility_chase_decision(
    *,
    item: ExecutionPlanItem,
    decision: dict[str, object],
) -> None:
    risk_context = dict(_loaded_risk_context(item))
    compact_decision = compact_json_object(decision, max_depth=5, max_list_items=5)
    risk_context["last_high_volatility_chase_decision"] = compact_decision
    item.risk_context = compact_json_object(risk_context)


def _bounded_text(value: str, *, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3].rstrip() + "..."


def _watch_buy_gate_rejection(
    *,
    reason: str,
    message: str,
    details: dict[str, object],
) -> dict[str, object]:
    return {
        "reason": reason,
        "message": message,
        "details": details,
    }


def _is_retryable_watch_buy_pullback_gate(reason: object) -> bool:
    return reason in {
        "watch_buy_intraday_range_chase_block",
    }


def _watch_buy_direction(item: ExecutionPlanItem) -> str:
    return watch_buy_direction_from_payloads(
        item.quantity_policy,
        _loaded_risk_context(item),
    )


def _watch_buy_tactical_action(item: ExecutionPlanItem) -> str | None:
    for source in _item_payload_sources(item):
        value = source.get("tactical_action")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    monitoring = _watch_buy_monitoring_context(item)
    value = monitoring.get("tactical_action")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return None


def _first_positive_decimal_from_item(
    item: ExecutionPlanItem,
    keys: tuple[str, ...],
) -> Decimal | None:
    monitoring = _watch_buy_monitoring_context(item)
    for key in keys:
        value = _parse_positive_decimal(monitoring.get(key))
        if value is not None:
            return value
    for source in _item_payload_sources(item):
        for key in keys:
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value
    return None


def _watch_buy_monitoring_context(item: ExecutionPlanItem) -> dict[str, object]:
    risk_context = _loaded_risk_context(item)
    monitoring = risk_context.get("watch_buy_monitoring")
    return monitoring if isinstance(monitoring, dict) else {}


def _item_payload_sources(item: ExecutionPlanItem) -> list[dict[str, object]]:
    sources: list[dict[str, object]] = []
    for source in (item.quantity_policy, _loaded_risk_context(item)):
        if isinstance(source, dict):
            sources.append(source)
    return sources


def _loaded_risk_context(
    item: ExecutionPlanItem,
    *,
    allow_file_backed_load: bool = False,
) -> dict[str, object]:
    item_state = sa_inspect(item)
    if "risk_context" in item_state.unloaded:
        session = item_state.session
        bind = session.get_bind() if session is not None else None
        database = getattr(getattr(bind, "url", None), "database", None)
        if database in {None, "", ":memory:"} or allow_file_backed_load:
            risk_context = item.risk_context
            return risk_context if isinstance(risk_context, dict) else {}
        return {}
    risk_context = item.risk_context
    return risk_context if isinstance(risk_context, dict) else {}


def _parse_positive_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _recursive_decimal_feature(payload: object, keys: set[str], *, parent_key: str | None = None) -> Decimal | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized_key = str(key or "").strip().lower()
            if parent_key is not None and normalized_key == parent_key:
                found = _recursive_decimal_feature(value, keys)
                if found is not None:
                    return found
            if parent_key is None and normalized_key in keys:
                parsed = _parse_decimal(value)
                if parsed is not None:
                    return parsed
            found = _recursive_decimal_feature(value, keys, parent_key=parent_key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _recursive_decimal_feature(item, keys, parent_key=parent_key)
            if found is not None:
                return found
    return None


def _recursive_text_feature(payload: object, keys: set[str], *, parent_key: str | None = None) -> str | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized_key = str(key or "").strip().lower()
            if parent_key is not None and normalized_key == parent_key:
                found = _recursive_text_feature(value, keys)
                if found is not None:
                    return found
            if parent_key is None and normalized_key in keys and value is not None:
                text = str(value).strip()
                if text:
                    return text
            found = _recursive_text_feature(value, keys, parent_key=parent_key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _recursive_text_feature(item, keys, parent_key=parent_key)
            if found is not None:
                return found
    return None


def _exec_agent_decision(item: ExecutionPlanItem) -> dict[str, object] | None:
    for source in (_loaded_risk_context(item), item.quantity_policy):
        if not isinstance(source, dict):
            continue
        for key in (
            "exec_agent_decision",
            "short_term_exec_agent",
            "short_term_exec_agent_decision",
        ):
            value = source.get(key)
            if isinstance(value, dict):
                return value
    return None


def _quote_stale_reason(
    *,
    settings: Settings,
    price: MonitoringPriceRequest | None,
    cycle_at: datetime,
) -> str | None:
    if price is None:
        return "quote is missing for watch buy evaluation"
    if price.observed_at is None:
        if settings.runtime_mode == "live":
            return "quote timestamp is missing; freshness cannot be verified"
        return None
    if price.observed_at.tzinfo is None or price.observed_at.utcoffset() is None:
        if settings.runtime_mode == "live":
            return "quote timestamp has no timezone; freshness cannot be verified"
    observed_at = _price_observed_at(price, cycle_at)
    future_skew = observed_at - cycle_at
    max_future_skew = timedelta(
        seconds=settings.monitoring_max_quote_future_skew_seconds
    )
    if future_skew > max_future_skew:
        return (
            "quote timestamp is in the future: observed_at="
            f"{observed_at.isoformat()} exceeds allowed clock skew "
            f"{settings.monitoring_max_quote_future_skew_seconds}s"
        )
    max_age = timedelta(seconds=settings.monitoring_max_quote_age_seconds)
    age = cycle_at - observed_at
    if age > max_age:
        return (
            "quote is stale: observed_at="
            f"{observed_at.isoformat()} exceeds max age "
            f"{settings.monitoring_max_quote_age_seconds}s"
        )
    return None


def _provided_price_observed_at(
    price: MonitoringPriceRequest | None,
) -> datetime | None:
    if price is None or price.observed_at is None:
        return None
    return _normalize_utc(price.observed_at)


def _observed_price_for_side(
    *,
    price: MonitoringPriceRequest | None,
    side: str,
    runtime_mode: str = "live",
) -> Decimal | None:
    if price is None:
        return None
    executable_price = price.ask if side.strip().lower() == "buy" else price.bid
    if (
        executable_price is not None
        and executable_price.is_finite()
        and executable_price > 0
    ):
        return executable_price
    if runtime_mode == "live":
        return None
    return price.price


def _observed_price_for_item(
    *,
    price: MonitoringPriceRequest | None,
    item: ExecutionPlanItem,
    runtime_mode: str | None = None,
) -> Decimal | None:
    plan = getattr(item, "plan", None)
    resolved_runtime_mode = runtime_mode or str(
        getattr(plan, "mode", None) or "live"
    )
    return _observed_price_for_side(
        price=price,
        side=item.side,
        runtime_mode=resolved_runtime_mode,
    )


def _price_observed_at(
    price: MonitoringPriceRequest | None,
    fallback: datetime,
) -> datetime:
    if price is None or price.observed_at is None:
        return fallback
    return _normalize_utc(price.observed_at)


def _record_monitoring_event(
    *,
    item: ExecutionPlanItem,
    event: dict[str, object],
) -> None:
    risk_context = dict(_loaded_risk_context(item))
    core_v4_contract = _core_v4_monitoring_contract_fields(
        item=item,
        risk_context=risk_context,
    )
    core_v4_proof = {
        key: risk_context[key]
        for key in (
            "_core_v4_contract_binding",
            "_core_activation",
            "_core_activation_state",
        )
        if key in risk_context
    }
    monitoring_events = risk_context.get("monitoring_events")
    if not isinstance(monitoring_events, list):
        monitoring_events = []
    # A single trigger can legitimately emit quality, sizing, approval, and
    # submission events. Keep enough bounded history for the approval/audit
    # trail to survive the rest of that same decision cycle.
    risk_context["monitoring_events"] = [*monitoring_events[-19:], event]
    risk_context["last_monitoring_event"] = event
    if core_v4_proof.get("_core_v4_contract_binding") is not None:
        # A Core v4 item is itself a persisted cryptographic contract.  The
        # complete pre-existing context can participate in later validation,
        # including runtime sizing fields not present in the original command.
        # Compacting the whole object is therefore unsafe.  History is already
        # bounded above, so retain the contract verbatim and only add the
        # bounded monitoring fields.
        item.risk_context = risk_context
        return
    compacted = compact_json_object(risk_context)
    # These fields are executable-authority proofs, not optional diagnostic
    # detail. JSON compaction may discard deep underscore-prefixed objects;
    # losing one after a routine monitoring event makes a valid v4 plan look
    # legacy and causes the safety fence to quarantine the BUY watch.
    compacted.update(core_v4_contract)
    compacted.update(core_v4_proof)
    item.risk_context = compacted


def _core_v4_monitoring_contract_fields(
    *,
    item: ExecutionPlanItem,
    risk_context: dict[str, object],
) -> dict[str, object]:
    """Return the immutable v4 payload fields that monitoring must not compact.

    The persisted-contract validator proves the complete original risk payload,
    not only the binding and activation markers.  Rebuild that immutable slice
    from the plan command so runtime monitoring fields may still be compacted
    without turning a valid watch into a legacy/unproven contract.
    """
    if not isinstance(risk_context.get("_core_v4_contract_binding"), dict):
        return {}
    try:
        raw_payload = item.plan.raw_payload
    except (AttributeError, RuntimeError):
        return {}
    if not isinstance(raw_payload, dict):
        return {}
    raw_items = raw_payload.get("items")
    if not isinstance(raw_items, list):
        return {}
    source_key = str(item.source_plan_item_id or "").strip()
    raw_item = next(
        (
            candidate
            for candidate in raw_items
            if isinstance(candidate, dict)
            and candidate.get("core_item_key") == source_key
        ),
        None,
    )
    if not isinstance(raw_item, dict):
        return {}
    raw_risk = raw_item.get("risk_context")
    if not isinstance(raw_risk, dict):
        return {}
    preserved = dict(raw_risk)
    for key in (
        "market",
        "exchange",
        "currency",
        "broker",
        "session_policy",
        "handoff_intent",
        "raw_action",
        "tactical_action",
        "generic_action",
    ):
        if key in raw_item:
            preserved[key] = raw_item[key]
    return preserved


def _plan_item_exchange(item: ExecutionPlanItem) -> str:
    risk_context = _loaded_risk_context(item, allow_file_backed_load=True)
    if not isinstance(risk_context, dict):
        return "KRX"
    exchange = risk_context.get("exchange")
    if isinstance(exchange, str) and exchange.strip():
        return exchange.strip().upper()
    venue = str(item.venue or "").strip().upper()
    if venue in set(MONITORED_KIS_POSITION_EXCHANGES) | {"US", "UPBIT"}:
        return venue
    return "KRX"


def _update_monitoring_checkpoint(
    *,
    settings: Settings,
    db: Session,
    worker_name: str,
    triggered_at: datetime,
    status: str,
    details: dict[str, object],
) -> MonitoringCheckpoint | None:
    execution_scope = details.get("execution_scope")
    expected_scope = _monitoring_checkpoint_execution_scope(settings=settings)
    if (
        expected_scope is None
        or not isinstance(execution_scope, dict)
        or set(execution_scope) != {
            "policy_version",
            "provider",
            "account_scope",
        }
        or type(execution_scope.get("policy_version")) is not int
        or execution_scope != expected_scope
    ):
        # Liveness without exact account authority is not safe evidence. In
        # particular, never let an unscoped process overwrite the current
        # account's last-known-good heartbeat.
        return None
    provider = str(execution_scope["provider"])
    account_scope = str(execution_scope["account_scope"])
    checkpoint = db.scalar(
        select(MonitoringCheckpoint).where(
            MonitoringCheckpoint.worker_name == worker_name,
            MonitoringCheckpoint.mode == settings.runtime_mode,
            MonitoringCheckpoint.provider == provider,
            MonitoringCheckpoint.account_scope == account_scope,
        )
    )
    if checkpoint is None:
        checkpoint = MonitoringCheckpoint(
            worker_name=worker_name,
            mode=settings.runtime_mode,
            provider=provider,
            account_scope=account_scope,
        )
        db.add(checkpoint)
    details = _preserve_checkpoint_retry_evidence(
        previous_details=checkpoint.details,
        details=details,
        triggered_at=triggered_at,
    )
    checkpoint.status = status
    checkpoint.last_heartbeat_at = triggered_at
    checkpoint.last_success_at = triggered_at
    checkpoint.details = details
    db.commit()
    return checkpoint


def _preserve_checkpoint_retry_evidence(
    *,
    previous_details: object,
    details: dict[str, object],
    triggered_at: datetime,
) -> dict[str, object]:
    current_retry_count = _nonnegative_int_value(
        details.get("retried_triggered_item_count")
    )
    merged = dict(details)
    if current_retry_count > 0:
        merged["last_retried_triggered_at"] = triggered_at.isoformat()
        return merged
    if _nonnegative_int_value(details.get("price_count")) != 0:
        return merged
    if not isinstance(previous_details, dict):
        return merged
    previous_retry_count = _nonnegative_int_value(
        previous_details.get("retried_triggered_item_count")
    )
    if previous_retry_count <= 0:
        return merged
    merged["retried_triggered_item_count"] = previous_retry_count
    previous_symbols = previous_details.get("retried_triggered_symbols")
    if isinstance(previous_symbols, list) and not merged.get("retried_triggered_symbols"):
        merged["retried_triggered_symbols"] = [
            symbol for symbol in previous_symbols if isinstance(symbol, str)
        ]
    previous_retry_at = previous_details.get("last_retried_triggered_at")
    if isinstance(previous_retry_at, str):
        merged["last_retried_triggered_at"] = previous_retry_at
    return merged


def _nonnegative_int_value(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int) and value > 0:
        return value
    return 0


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _serialize_date(value: date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _place_monitoring_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    quantity: int,
    observed_price: Decimal | None,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    triggered_at: datetime,
    freshness_reference_at: datetime | None = None,
    limit_price_override: Decimal | None = None,
    before_success_commit: Callable[[], None] | None = None,
    deterministic_sell_quantity_cap: int | None = None,
):
    item_state = sa_inspect(item)
    expected_status = str(
        _attribute_value_before_local_changes(
            item_state.attrs.status.history,
            fallback=item.status,
        )
    )
    expected_risk_context = dict(
        _attribute_value_before_local_changes(
            item_state.attrs.risk_context.history,
            fallback=_loaded_risk_context(item, allow_file_backed_load=True),
        )
        or {}
    )
    is_watch_buy = _is_watch_buy_item(item)
    limit_price = (
        limit_price_override
        or _watch_buy_limit_price(item=item, observed_price=observed_price)
        if is_watch_buy
        else limit_price_override or item.limit_price
    )
    if item.asset_class == "us_equity" and limit_price is None:
        limit_price = observed_price
    order_metadata = _monitoring_order_metadata(
        item=item,
        observed_price=observed_price,
        trigger_reason=trigger_reason,
        trigger_metadata=trigger_metadata,
        triggered_at=triggered_at,
        deterministic_sell_quantity_cap=deterministic_sell_quantity_cap,
    )
    retry_source_order = (
        _retryable_session_rejection_source_order(
            settings=settings,
            db=db,
            item=item,
        )
        if item.intent == "stop_sell"
        else None
    )
    retry_source_order_pk = retry_source_order.id if retry_source_order is not None else None
    if retry_source_order_pk is not None:
        order_metadata["retry"] = {
            "source": "rejected_order_retry",
            "source_broker_order_pk": retry_source_order_pk,
        }
    order_request = PlaceOrderRequest(
        idempotency_key=_monitoring_order_idempotency_key(
            item=item,
            is_watch_buy=is_watch_buy,
            retry_source_order_pk=retry_source_order_pk,
        ),
        execution_plan_item_id=item.id,
        provider=item.provider or "kis",
        asset_class=item.asset_class or "stock",
        venue=item.venue,
        symbol=item.symbol,
        side="buy" if is_watch_buy else "sell",
        quantity=quantity,
        order_type="limit" if is_watch_buy or limit_price is not None else "market",
        limit_price=limit_price,
        time_in_force=item.time_in_force or "day",
        exchange=_plan_item_exchange(item),
        metadata=order_metadata,
    )
    authority_context = dict(_loaded_risk_context(item))
    authority_context["monitoring_order_authority"] = {
        "policy_version": 1,
        "source": f"monitoring:{MONITORING_WORKER_NAME}",
        "command": order_request.model_dump(mode="json"),
    }
    item.risk_context = compact_json_object(authority_context)
    # The cycle has already staged a trigger status on this ORM item. Persist
    # that status and its command authority only while the durable row remains
    # claimable; an unconditional flush can resurrect a stale item over a Core
    # activation hold or operator terminal transition committed after selection.
    with db.no_autoflush:
        guarded_update = db.execute(
            update(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.id == item.id,
                ExecutionPlanItem.status == expected_status,
                ExecutionPlanItem.status.not_in(
                    tuple(PLAN_ITEM_NONCLAIMABLE_STATUSES)
                ),
            )
            .values(
                status=item.status,
            )
            .execution_options(synchronize_session=False)
        )
    if guarded_update.rowcount != 1:
        current_plan_state = _current_buy_plan_item_state(
            db=db,
            item_id=item.id,
            lock=False,
        )
        if current_plan_state is not None:
            set_committed_value(item, "status", current_plan_state[0])
            set_committed_value(item, "risk_context", current_plan_state[1])
        return _plan_item_state_changed_result(side=order_request.side)
    current_plan_state = _current_buy_plan_item_state(
        db=db,
        item_id=item.id,
        lock=False,
    )
    if (
        current_plan_state is None
        or current_plan_state[1] != expected_risk_context
    ):
        if current_plan_state is not None:
            restored_update = db.execute(
                update(ExecutionPlanItem)
                .where(
                    ExecutionPlanItem.id == item.id,
                    ExecutionPlanItem.status == item.status,
                )
                .values(status=expected_status)
                .execution_options(synchronize_session=False)
            )
            if restored_update.rowcount == 1:
                set_committed_value(item, "status", expected_status)
                set_committed_value(
                    item,
                    "risk_context",
                    current_plan_state[1],
                )
        return _plan_item_state_changed_result(side=order_request.side)
    persisted_risk_context = db.execute(
        update(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.id == item.id,
            ExecutionPlanItem.status == item.status,
        )
        .values(risk_context=item.risk_context)
        .execution_options(synchronize_session=False)
    )
    if persisted_risk_context.rowcount != 1:
        return _plan_item_state_changed_result(side=order_request.side)
    set_committed_value(item, "status", item.status)
    set_committed_value(item, "risk_context", item.risk_context)
    return place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=order_request,
        request_source=f"monitoring:{MONITORING_WORKER_NAME}",
        before_success_commit=before_success_commit,
        now=triggered_at,
        freshness_reference_at=freshness_reference_at or triggered_at,
    )


def _monitoring_order_idempotency_key(
    *,
    item: ExecutionPlanItem,
    is_watch_buy: bool,
    retry_source_order_pk: str | None,
) -> str:
    if is_watch_buy:
        return f"monitoring:watch_buy:{item.id}"
    base = f"monitoring:{item.intent}:{item.id}"
    if item.intent == "stop_sell" and retry_source_order_pk is not None:
        # One deterministic generation per confirmed terminal rejection.
        # Replays against the same rejected source remain idempotent; a new
        # generation is possible only after a new rejection row is durable.
        return f"{base}:retry:{retry_source_order_pk}"
    return base


def _monitoring_order_metadata(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    triggered_at: datetime,
    deterministic_sell_quantity_cap: int | None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "monitoring_worker": MONITORING_WORKER_NAME,
        "trigger_reason": trigger_reason or item.intent,
        "observed_price": _serialize_decimal(observed_price),
        "order_purpose": "protective_stop" if item.intent == "stop_sell" else item.intent,
        "route_now": triggered_at.isoformat(),
    }
    if item.side == "sell":
        metadata["deterministic_sell_quantity_cap"] = (
            deterministic_sell_quantity_cap
        )
        trigger_binding = _monitoring_sell_trigger_binding(
            item=item,
            observed_price=observed_price,
            trigger_reason=trigger_reason,
            trigger_metadata=trigger_metadata,
        )
        if trigger_binding is not None:
            metadata["monitoring_sell_trigger_binding"] = trigger_binding
    risk_context = _loaded_risk_context(item)
    if isinstance(risk_context, dict):
        for key in ("nxt_eligible", "route_now"):
            if key in risk_context:
                metadata[key] = risk_context[key]
    if trigger_metadata is not None:
        metadata["trigger"] = trigger_metadata
    return metadata


def _monitoring_sell_trigger_binding(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
) -> dict[str, object] | None:
    reason = trigger_reason or item.intent
    if item.intent == "stop_sell":
        direction = "at_or_below"
        threshold = item.trigger_price
    elif item.intent == "target_sell":
        if reason in PROFIT_PROTECTION_TRIGGER_REASONS:
            # Profit-protection target rows behave as a trailing protective
            # exit. Bind them to the crossed bid rather than treating them as
            # an ordinary upside target at the final broker boundary.
            direction = "at_or_below"
            threshold = observed_price or _parse_decimal(
                (trigger_metadata or {}).get("last_observed_price")
            )
        else:
            direction = "at_or_above"
            threshold = item.trigger_price
    else:
        return None
    if threshold is None or not threshold.is_finite() or threshold <= 0:
        return None
    return {
        "version": 1,
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "intent": item.intent,
        "trigger_reason": reason,
        "direction": direction,
        "trigger_price": _serialize_decimal(threshold),
    }


def _watch_buy_limit_price(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
) -> Decimal:
    reference_price = observed_price or _watch_buy_trigger_price(item) or item.limit_price
    if reference_price is None:
        raise ValueError("watch buy order requires an observed or trigger price")
    chase_limit_price = _watch_buy_chase_limit_price(item)
    if chase_limit_price is None:
        return reference_price
    return min(reference_price, chase_limit_price)


def _watch_buy_trigger_cross_pullback_bypasses_agent(action: PendingMonitoringAction) -> bool:
    if not _is_watch_buy_item(action.item) or action.observed_price is None:
        return False
    metadata = action.trigger_metadata
    if not isinstance(metadata, dict):
        return False
    reason = metadata.get("reason")
    if reason not in {"intraday_watch_buy_day_high_cross", "intraday_watch_buy_day_low_cross"}:
        return False
    trigger_price = _watch_buy_trigger_price(action.item)
    if trigger_price is None:
        return False
    direction = _watch_buy_direction(action.item)
    if direction == "at_or_below":
        return action.observed_price >= trigger_price
    return action.observed_price <= trigger_price


def _watch_buy_entry_line_bypasses_agent(action: PendingMonitoringAction) -> bool:
    if not _is_watch_buy_item(action.item) or action.observed_price is None:
        return False
    if _watch_buy_direction(action.item) != "at_or_below":
        return False
    trigger_price = _watch_buy_trigger_price(action.item)
    return trigger_price is not None and action.observed_price <= trigger_price


def _monitoring_result_payload(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    outcome: str,
    message: str | None = None,
    block_reason: object | None = None,
    risk_decision: object | None = None,
    order: object | None = None,
    trigger_reason: str | None = None,
    trigger_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "intent": item.intent,
        "status": item.status,
        "trigger_price": _serialize_decimal(item.trigger_price),
        "observed_price": _serialize_decimal(observed_price),
        "outcome": outcome,
        "source_identity": serialize_plan_item_source_identity(item),
    }
    if _is_watch_buy_item(item):
        payload["chase_limit_price"] = _serialize_decimal(_watch_buy_chase_limit_price(item))
        payload["target_price"] = _serialize_decimal(_watch_buy_target_price(item))
        payload["stop_loss_price"] = _serialize_decimal(_watch_buy_stop_price(item))
        payload["current_price"] = _serialize_decimal(observed_price)
        payload["direction"] = _watch_buy_direction(item)
        monitoring = _watch_buy_monitoring_context(item)
        tactical_action = monitoring.get("tactical_action")
        if tactical_action is None:
            tactical_action = _loaded_risk_context(item).get("tactical_action")
        if tactical_action is not None:
            payload["tactical_action"] = tactical_action
    if message is not None:
        payload["message"] = message
    if block_reason is not None:
        payload["block_reason"] = block_reason
    if risk_decision is not None:
        payload["risk_decision"] = risk_decision
    if trigger_reason is not None:
        payload["trigger_reason"] = trigger_reason
    if trigger_metadata is not None:
        payload["trigger"] = trigger_metadata
    if order is not None:
        payload["order"] = order
    return payload
