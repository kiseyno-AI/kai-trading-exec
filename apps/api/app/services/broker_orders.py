from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, DecimalException, ROUND_DOWN
from hashlib import sha256
import hmac
from time import monotonic
from typing import Iterator
from zoneinfo import ZoneInfo

from sqlalchemy import case, exists, func, or_, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, contains_eager, selectinload, undefer

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerEventSource,
    BrokerIntradayLiquiditySnapshot,
    BrokerLiveMarketContext,
    BrokerOrderPlacementRequest,
    NormalizedBrokerOrderStatus,
)
from app.broker.error_safety import (
    safe_broker_error_code,
    safe_broker_error_message,
    sanitize_broker_error_payload,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    BrokerSession,
    ExecutionFailure,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    RuntimeControlState,
)
from app.models.runtime import ExecutionFill, ExecutionPosition, new_id
from app.services.account_drawdown_gate import (
    AccountDrawdownDecision,
    account_drawdown_uncertain_decision,
    evaluate_account_drawdown_gate,
    evaluate_us_equity_account_gate,
    record_account_drawdown_block,
)
from app.services.account_drawdown_recovery import (
    RECOVERABLE_LOSS_GATE_NAMES,
    record_healthy_observation,
    record_recovery_release,
    record_recovery_reset,
    recovery_account_fingerprint,
    recovery_cooldown_until,
    recovery_gate_settings,
    recovery_idempotency_key,
    recovery_policy_fingerprint,
    recovery_state,
    recovery_state_is_current_release,
    scope_recovery_release_idempotency_key,
    snapshot_is_fresh_for_recovery,
)
from app.services.broker_raw_holdings import snapshot_with_raw_balance_holdings
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.domestic_order_router import RouteDecision, resolve_domestic_order_route
from app.services.execution_failures import record_actionable_execution_failure
from app.services.execution_lineage import (
    SELL_MONITOR_AUTHORITY_KEY,
    decision_lineage_for_plan_item,
    payload_with_order_decision_lineage,
)
from app.services.exec_agent_approval import (
    SHORT_TERM_LIVE_MIN_NET_REWARD_RISK,
    SHORT_TERM_LIVE_MIN_TURNOVER_KRW,
    assess_buy_economics,
    configured_execution_cost_evidence,
    delegated_buy_gate_rejections,
    persisted_verified_watch_sizing_provenance,
    trusted_live_market_decimal,
)
from app.services.execution_account_route import (
    AccountRouteMismatch,
    ExecutionAccountScopeMismatch,
    build_execution_account_scope_binding,
    execution_account_scope_binding_from_payloads,
    redact_account_references,
    validate_execution_account_scope,
    validate_kis_account_route,
)
from app.services.execution_semantics import (
    KRX_EXCHANGES,
    canonical_execution_asset_class,
    is_short_term_watch_buy_intent,
    is_watch_buy_intent,
    watch_buy_direction_from_payloads,
)
from app.services.execution_api import serialize_broker_order
from app.services.core_buy_authority import (
    core_buy_authority_block_reason,
    project_plan_item_for_core_buy_authority,
)
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    execution_fill_idempotency_key,
    execution_fills_have_relevant_conflicting_identity,
)
from app.services.fill_economics import (
    COST_COVERAGE_UNKNOWN,
    effective_fill_quantity,
    settlement_currency_for_order,
)
from app.services.fifo_inventory import (
    FifoRouteScope,
    ambiguous_fifo_chronology_buckets,
    canonical_fifo_inventory_route_scope,
    fifo_chronology_is_ambiguous_at,
    fifo_fill_sort_key,
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
)
from app.services.json_compaction import PLAN_ITEM_SUBMISSION_RISK_KEYS, json_payload_hash
from app.services.monitoring_price_source import is_us_regular_session, next_us_regular_open
from app.services.order_intent_serialization import (
    alpaca_pre_auth_order_intent_account_scope,
    order_intent_file_lock,
    order_intent_process_lock,
    order_intent_scope_key_for_account,
)
from app.services.plan_item_identity import (
    plan_item_decision_agent_name,
    plan_item_entry_family_key,
    plan_item_execution_route_key,
    plan_item_inventory_scope_key,
    select_plan_item_family_members,
)
from app.services.plan_judgment_actions import project_buy_plan_item_for_hourly_risk_off
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    KIS_US_POSITION_QUARANTINE_EXCHANGES,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
    position_identity_from_columns,
    position_identity_from_payload,
    stable_account_scope_hmac_key,
)
from app.services.risk_sizing import effective_stop_price
from app.services.runtime_controls import (
    RUNTIME_CONTROL_ID,
    automated_buy_submission_block_reason,
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    hourly_risk_off_retry_after_at,
    runtime_safety_payload,
)
from app.services.runtime_transitions import (
    apply_broker_order_status,
    plan_item_status_for_broker_order_status,
    set_plan_item_status,
)

UPBIT_PAPER_ALLOWED_SYMBOLS = frozenset({"KRW-BTC", "KRW-ETH"})
RUNTIME_SAFETY_CONTROL_CLASSIFICATION = "runtime_safety_control"
ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_open",
        "broker_order_partially_filled",
        "broker_order_rejected",
        "broker_order_failed",
        "cancel_pending",
        "canceled",
        "cancelled",
        "exec_agent_rejected",
        "expired",
        "failed",
        "filled",
        "open",
        "partially_filled",
        "rejected",
        "submitted",
        "superseded",
        "watch_buy_rejected",
        "watch_buy_submitted",
        "watch_cancelled",
        "watch_expired",
        "next_open_buy_expired",
        "next_open_buy_failed",
    }
)
PLAN_ITEM_NONCLAIMABLE_STATUSES = (
    ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
    | frozenset(
        {
            "core_activation_pending",
            "contract_quarantined",
            "manual_review_required",
            "revoked",
        }
    )
)
ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES = (
    "monitoring:",
    "execution-job:",
    "short-term-watcher:",
)
STRATEGY_ENTRY_FAMILY_METADATA_KEY = "strategy_entry_family_v1"
STRATEGY_ENTRY_FAMILY_POLICY_VERSION = 1
SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY = "symbol_exposure_reservation_v1"
AUTOMATED_MONITORING_SOURCE = "monitoring:intraday_target_stop_monitor"
AUTOMATED_FAMILY_EXIT_INTENTS = frozenset(
    {"target_sell", "stop_sell", "max_holding_close_sell"}
)
CONCLUSIVE_MONITORING_SESSION_REJECTION_CODES = frozenset(
    {"KIOK0320", "APBK0918"}
)


@dataclass(frozen=True)
class PlaceBrokerOrderResult:
    status_code: int
    payload: dict[str, object]


@dataclass(frozen=True)
class AccountDrawdownRecoveryPreflight:
    active: bool
    block_reason: str | None = None
    message: str | None = None
    retry_after_at: datetime | None = None
    source_broker_order_pk: str | None = None
    source_idempotency_key: str | None = None


@dataclass(frozen=True)
class AutomatedKisBuyMarketEvidence:
    context: BrokerLiveMarketContext | None
    is_domestic: bool
    liquidity_snapshot: BrokerIntradayLiquiditySnapshot | None = None
    failure: tuple[str, str] | None = None


@dataclass(frozen=True)
class AutomatedKisSellMarketEvidence:
    context: BrokerLiveMarketContext | None
    is_domestic: bool
    failure: tuple[str, str] | None = None


@dataclass(frozen=True)
class AutomatedKisBuySymbolExposureFailure:
    block_reason: str
    message: str
    metrics: dict[str, object]


@dataclass(frozen=True)
class AutomatedKisOppositeOrderFailure:
    block_reason: str
    message: str
    metrics: dict[str, object]


@dataclass(frozen=True)
class ScopedLocalOrderSelection:
    matching_orders: tuple[BrokerOrder, ...]
    indeterminate_order: BrokerOrder | None = None


@dataclass(frozen=True)
class OpenBuyReservationResolution:
    broker_open_notional: Decimal
    local_unmatched_notional: Decimal
    broker_unknown_count: int
    local_unknown_count: int


@dataclass(frozen=True)
class AutomatedBuyLaneInventoryFailure:
    block_reason: str
    message: str
    manual_review_required: bool
    metrics: dict[str, object]


@dataclass(frozen=True)
class StrategyFamilyLedgerReplay:
    family_balances: dict[tuple[str, str], Decimal]
    family_bought: dict[tuple[str, str], Decimal]
    family_sold: dict[tuple[str, str], Decimal]
    total_position: Decimal
    error_kind: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class RejectedOrderRetryLineageProof:
    job: ExecutionJob
    failure: ExecutionFailure
    source_order: BrokerOrder
    root_order: BrokerOrder
    source_scope: dict[str, object]
    source_route: FifoRouteScope
    automated_monitoring_family_identity: dict[str, object] | None = None
    automated_monitoring_family_error: str | None = None


def _normalized_strategy_lane(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    lane = value.strip().casefold()
    return lane or None


def _strategy_entry_family_identity(
    plan_item: ExecutionPlanItem | None,
) -> dict[str, object] | None:
    """Return the exact strategy-owned inventory identity for an order.

    The broker account remains aggregate, but automated entry duplication and
    exits must be resolved inside the originating decision lane and entry
    family.  Missing lane or family evidence is never inferred from symbol.
    """

    if plan_item is None:
        return None
    family_key = plan_item_entry_family_key(plan_item)
    decision_lane = _normalized_strategy_lane(
        plan_item_decision_agent_name(plan_item)
    )
    if family_key is None or decision_lane is None:
        return None
    return {
        "policy_version": STRATEGY_ENTRY_FAMILY_POLICY_VERSION,
        "entry_family_key": family_key,
        "decision_lane": decision_lane,
    }


def _canonical_strategy_entry_family_identity(
    value: object,
) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    family_key = value.get("entry_family_key")
    lane = value.get("decision_lane")
    if (
        value.get("policy_version") != STRATEGY_ENTRY_FAMILY_POLICY_VERSION
        or not isinstance(family_key, str)
        or not family_key.strip()
        or not isinstance(lane, str)
    ):
        return None
    normalized_lane = _normalized_strategy_lane(lane)
    if normalized_lane is None:
        return None
    return {
        "policy_version": STRATEGY_ENTRY_FAMILY_POLICY_VERSION,
        "entry_family_key": family_key.strip(),
        "decision_lane": normalized_lane,
    }


def _persisted_order_strategy_family_identity(
    order: BrokerOrder,
) -> tuple[dict[str, object] | None, str | None]:
    """Read family ownership only when both immutable command copies agree."""

    request_metadata = (
        order.request_metadata if isinstance(order.request_metadata, dict) else {}
    )
    routed_metadata = request_metadata.get("metadata")
    original_command = request_metadata.get("original_command")
    original_metadata = (
        original_command.get("metadata")
        if isinstance(original_command, dict)
        else None
    )
    routed_identity = (
        routed_metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
        if isinstance(routed_metadata, dict)
        else None
    )
    original_identity = (
        original_metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
        if isinstance(original_metadata, dict)
        else None
    )
    routed_canonical = _canonical_strategy_entry_family_identity(routed_identity)
    original_canonical = _canonical_strategy_entry_family_identity(original_identity)
    if routed_canonical is None or original_canonical is None:
        return None, "strategy_family_persisted_identity_unavailable"
    if (
        routed_canonical != original_canonical
        or routed_identity != routed_canonical
        or original_identity != original_canonical
    ):
        return None, "strategy_family_persisted_identity_mismatch"
    return routed_canonical, None


def _validated_order_strategy_family_identity(
    order: BrokerOrder,
) -> tuple[dict[str, object] | None, str | None]:
    """Cross-check immutable command ownership against the current plan row."""

    persisted_identity, persisted_error = (
        _persisted_order_strategy_family_identity(order)
    )
    current_identity = _strategy_entry_family_identity(order.plan_item)
    if persisted_error is not None:
        return None, persisted_error
    if current_identity is None or current_identity != persisted_identity:
        return None, "strategy_family_persisted_identity_mismatch"
    return persisted_identity, None


def _is_automated_monitoring_order(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> bool:
    if request_source != AUTOMATED_MONITORING_SOURCE or plan_item is None:
        return False
    if command.execution_plan_item_id != plan_item.id:
        return False
    if (
        command.provider != "kis"
        or command.asset_class != "stock"
        or command.exchange not in KRX_EXCHANGES
    ):
        return False
    return bool(
        (command.side == "buy" and is_watch_buy_intent(plan_item.intent))
        or (
            command.side == "sell"
            and plan_item.intent in AUTOMATED_FAMILY_EXIT_INTENTS
        )
    )


def _is_trusted_automated_watch_buy(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> bool:
    """Identify internal KRX watch-buy paths that require the final lane gate."""

    return bool(
        adapter.provider == "kis"
        and command.provider == "kis"
        and command.side == "buy"
        and command.asset_class == "stock"
        and command.exchange in KRX_EXCHANGES
        and plan_item is not None
        and command.execution_plan_item_id == plan_item.id
        and is_watch_buy_intent(plan_item.intent)
        and any(
            request_source.startswith(prefix)
            for prefix in ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
        )
    )


def _bind_server_owned_watch_buy_identity(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> tuple[PlaceOrderRequest, tuple[str, str] | None]:
    """Persist exact plan-owned lane identity for every internal watch-buy source."""

    if not _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return command, None
    expected_identity = _strategy_entry_family_identity(plan_item)
    if expected_identity is None:
        return command, (
            "strategy_family_lineage_unavailable",
            "automated watch buy lacks exact strategy lane lineage",
        )
    metadata = dict(command.metadata or {})
    supplied_identity = metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
    if supplied_identity is not None and supplied_identity != expected_identity:
        return command, (
            "strategy_family_identity_mismatch",
            "automated watch buy strategy identity conflicts with its plan item",
        )
    metadata[STRATEGY_ENTRY_FAMILY_METADATA_KEY] = expected_identity
    return command.model_copy(update={"metadata": metadata}), None


def _automated_strategy_identity_error(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
) -> tuple[str, str] | None:
    if not _is_trusted_automated_family_sell(
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=retry_lineage_proof,
    ):
        return None
    expected = _strategy_entry_family_identity(plan_item)
    if expected is None:
        return (
            "strategy_family_lineage_unavailable",
            "automated monitoring order lacks exact strategy family lineage",
        )
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    supplied = metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
    if supplied != expected:
        return (
            "strategy_family_identity_mismatch",
            "automated monitoring order strategy family identity does not match its plan item",
        )
    return None


def _manual_review_strategy_boundary_result(
    *,
    db: Session,
    plan_item: ExecutionPlanItem | None,
    block_reason: str,
    message: str,
) -> PlaceBrokerOrderResult:
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="manual_review_required",
            source="broker_order_service",
            event_type="strategy_family_boundary_manual_review",
            reason=message,
            payload={
                "block_reason": block_reason,
                "broker_call_started": False,
            },
        )
        db.commit()
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": message,
            "block_reason": block_reason,
            "manual_review_required": True,
            "permanent_rejection": True,
        },
    )


def inspect_account_drawdown_recovery(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    provider: str,
    asset_class: str,
    exchange: str,
    idempotency_key: str,
    execution_plan_item_id: str,
    symbol: str,
    now: datetime,
    scope_source_cache: dict[str, BrokerOrder | None] | None = None,
) -> AccountDrawdownRecoveryPreflight:
    """Return the durable buy-recovery fence without querying the broker.

    Monitoring uses this before risk sizing so a known cooldown cannot trigger
    redundant account snapshots. The order service repeats the same checks
    under its serialized intent lock before any broker mutation.
    """

    if adapter.provider != "kis" or provider != "kis":
        return AccountDrawdownRecoveryPreflight(active=False)
    recovery_scope = _account_drawdown_recovery_scope(
        settings=settings,
        provider=adapter.provider,
        asset_class=asset_class,
        exchange=exchange,
    )
    direct_source = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=idempotency_key,
    )
    direct_source_matches = (
        direct_source is not None
        and _account_drawdown_recovery_preflight_source_matches(
            settings=settings,
            adapter=adapter,
            order=direct_source,
            recovery_scope=recovery_scope,
            idempotency_key=idempotency_key,
            execution_plan_item_id=execution_plan_item_id,
            symbol=symbol,
        )
    )
    if direct_source_matches:
        source_order = direct_source
    elif scope_source_cache is not None and recovery_scope in scope_source_cache:
        source_order = scope_source_cache[recovery_scope]
    else:
        source_order = _load_active_account_drawdown_recovery_source_for_scope(
            settings=settings,
            db=db,
            provider=adapter.provider,
            recovery_scope=recovery_scope,
        )
        if scope_source_cache is not None:
            scope_source_cache[recovery_scope] = source_order
    if source_order is None:
        return AccountDrawdownRecoveryPreflight(active=False)
    common = {
        "active": True,
        "source_broker_order_pk": source_order.id,
        "source_idempotency_key": source_order.idempotency_key,
    }
    source_scope = _account_drawdown_recovery_order_scope(source_order)
    if source_scope != recovery_scope:
        return AccountDrawdownRecoveryPreflight(
            **common,
            block_reason="account_drawdown_recovery_scope_mismatch",
            message=(
                "an active account drawdown latch belongs to a different opaque "
                "account scope; restore the original stable scope secret or "
                "complete reviewed operator remediation"
            ),
            retry_after_at=(
                _normalize_utc_datetime(now)
                + timedelta(
                    seconds=settings.account_drawdown_recovery_min_observation_interval_seconds
                )
            ),
        )
    if settings.account_drawdown_gate_enabled and not settings.account_drawdown_recovery_enabled:
        return AccountDrawdownRecoveryPreflight(
            **common,
            block_reason="account_drawdown_recovery_disabled",
            message="account drawdown recovery is disabled by configuration",
        )
    state = recovery_state(
        db=db,
        execution_plan_item_id=source_order.execution_plan_item_id,
        source_broker_order_pk=source_order.id,
    )
    if state.released:
        # A released child can still be retryable when a runtime safety fence
        # stopped it before the broker call. Keep forcing a fresh trigger in
        # monitoring, but do not restart the already-completed cooldown.
        return AccountDrawdownRecoveryPreflight(**common)
    blocked_at = source_order.last_broker_sync_at or source_order.created_at
    cooldown_until = recovery_cooldown_until(
        blocked_at=blocked_at,
        settings=settings,
        last_unhealthy_at=state.last_unhealthy_at,
    )
    if settings.account_drawdown_gate_enabled and _normalize_utc_datetime(now) < cooldown_until:
        return AccountDrawdownRecoveryPreflight(
            **common,
            block_reason="account_drawdown_recovery_cooldown",
            message="account drawdown recovery cooldown has not elapsed",
            retry_after_at=cooldown_until,
        )
    if state.last_snapshot_observed_at is not None:
        next_observation_at = _normalize_utc_datetime(
            state.last_snapshot_observed_at
        ) + timedelta(
            seconds=settings.account_drawdown_recovery_min_observation_interval_seconds
        )
        if _normalize_utc_datetime(now) < next_observation_at:
            return AccountDrawdownRecoveryPreflight(
                **common,
                block_reason="account_drawdown_recovery_observation_interval",
                message="account drawdown recovery is waiting for the next distinct snapshot",
                retry_after_at=next_observation_at,
            )
    return AccountDrawdownRecoveryPreflight(**common)


@contextmanager
def _serialized_order_intent_scope(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> Iterator[None]:
    """Serialize account-capital checks through the durable intent commit."""
    scope = _order_intent_scope_key(
        settings=settings,
        adapter=adapter,
        command=command,
    )
    process_lock = order_intent_process_lock(scope)
    with process_lock:
        database = make_url(settings.database_url)
        if database.drivername.startswith("postgresql"):
            # Cross-process locking is acquired on the order transaction just
            # before reservation reads. The process lock still protects local
            # in-memory adapters and SQLite-style test engines here.
            yield
            return
        if (
            database.drivername.startswith("sqlite")
            and database.database
            and database.database != ":memory:"
        ):
            with order_intent_file_lock(scope):
                yield
            return
        yield


def _order_intent_scope_key(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None = None,
) -> str:
    provider = str(getattr(adapter, "provider", None) or command.provider or "unknown").lower()
    exchange = str(command.exchange or command.venue or "").upper()
    market = (
        "ACCOUNT"
        if provider == "alpaca"
        else "USD"
        if command.asset_class == "us_equity" or exchange in {"AMEX", "NASD", "NYSE"}
        else command.symbol.split("-", 1)[0]
        if "-" in command.symbol
        else "KRW"
    )
    scoped_provider = (
        str(execution_scope.get("provider") or "").strip().lower()
        if isinstance(execution_scope, dict)
        else ""
    )
    scoped_account = (
        str(execution_scope.get("account_scope") or "").strip()
        if isinstance(execution_scope, dict)
        else ""
    )
    account_fingerprint = (
        scoped_account
        if scoped_provider == provider and scoped_account
        else _order_intent_account_fingerprint(
            settings=settings,
            provider=provider,
        )
    )
    return order_intent_scope_key_for_account(
        provider=provider,
        runtime_mode=settings.runtime_mode,
        account_scope=account_fingerprint,
        market=market,
    )


def _order_intent_account_fingerprint(
    *,
    settings: Settings,
    provider: str,
) -> str:
    if provider == "kis":
        return recovery_account_fingerprint(settings)

    configured_scope = str(settings.execution_position_account_scope or "").strip()
    if provider == "alpaca":
        # Alpaca's stable broker account ID is fetched only after this outer
        # lock is acquired. Labels and API keys can both change across rolling
        # workers, so serialize the pre-auth provider conservatively. The
        # PostgreSQL transaction lock is narrowed to the authenticated opaque
        # account scope after authentication.
        return alpaca_pre_auth_order_intent_account_scope(
            runtime_mode=settings.runtime_mode,
        )
    credential_material = (
        settings.upbit_access_key
        if provider == "upbit"
        else settings.alpaca_api_key
        if provider == "alpaca"
        else None
    )
    material = (
        f"configured-account-scope\0{configured_scope}"
        if configured_scope
        else f"provider-account\0{credential_material or 'unavailable'}"
    ).encode("utf-8")
    key = stable_account_scope_hmac_key(settings)
    if key is not None:
        return hmac.new(key, material, sha256).hexdigest()[:24]
    if configured_scope:
        return sha256(material).hexdigest()[:24]
    if settings.runtime_mode == "live":
        # The final broker boundary rejects this unsafe account configuration.
        # Use one broad opaque lock scope meanwhile so concurrency cannot be
        # weakened and no low-entropy or rotating credential hash is exposed.
        return sha256(f"{provider}\0live-scope-unavailable".encode()).hexdigest()[:24]
    paper_key = (
        settings.upbit_secret_key
        if provider == "upbit"
        else settings.alpaca_api_secret
        if provider == "alpaca"
        else None
    )
    if paper_key:
        return hmac.new(paper_key.encode("utf-8"), material, sha256).hexdigest()[:24]
    return sha256(f"{provider}\0{settings.runtime_mode}\0simulated".encode()).hexdigest()[:24]


def _acquire_cross_process_order_intent_lock(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
) -> None:
    if not make_url(settings.database_url).drivername.startswith("postgresql"):
        return
    scope = _order_intent_scope_key(
        settings=settings,
        adapter=adapter,
        command=command,
        execution_scope=execution_scope,
    )
    # Transaction-scoped lock releases with the durable submission-intent
    # commit. It needs no second pool connection and prevents two processes
    # from evaluating the same account reserves concurrently.
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:scope))"),
        {"scope": scope},
    )


def place_broker_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str = "post:/orders",
    before_success_commit: Callable[[], None] | None = None,
    now: datetime | None = None,
    freshness_reference_at: datetime | None = None,
) -> PlaceBrokerOrderResult:
    with _serialized_order_intent_scope(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
    ):
        return _place_broker_order_serialized(
            settings=settings,
            db=db,
            adapter=adapter,
            command=command,
            request_source=request_source,
            before_success_commit=before_success_commit,
            now=now,
            freshness_reference_at=freshness_reference_at,
        )


def _place_broker_order_serialized(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
    before_success_commit: Callable[[], None] | None,
    now: datetime | None,
    freshness_reference_at: datetime | None,
) -> PlaceBrokerOrderResult:
    submission_started = monotonic()
    logical_command = command
    observed_at = _normalize_utc_datetime(now or datetime.now(UTC))
    snapshot_freshness_reference_at = _normalize_utc_datetime(
        freshness_reference_at or observed_at
    )
    existing = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    retryable_pre_submit_failure_order = (
        existing
        if existing is not None
        and _retryable_pre_submit_failure_matches_command(
            settings=settings,
            adapter=adapter,
            order=existing,
            command=command,
            request_source=request_source,
        )
        else None
    )
    retrying_proven_pre_submit_failure = (
        retryable_pre_submit_failure_order is not None
    )
    retryable_safety_fenced_order = (
        existing
        if existing is not None
        and _runtime_safety_fenced_order_matches_command(
            settings=settings,
            adapter=adapter,
            order=existing,
            command=command,
        )
        else retryable_pre_submit_failure_order
    )
    if retryable_safety_fenced_order is not None:
        if retryable_pre_submit_failure_order is not None or not _runtime_safety_order_allows_resized_retrigger(
            retryable_safety_fenced_order
        ):
            command = _runtime_safety_replay_command(
                retryable_safety_fenced_order
            ) or command
    direct_drawdown_recovery_source_order = (
        existing
        if existing is not None
        and _account_drawdown_recovery_source_matches_command(
            settings=settings,
            adapter=adapter,
            order=existing,
            command=logical_command,
            request_source=request_source,
        )
        else None
    )
    active_drawdown_recovery_source_order = (
        _load_active_account_drawdown_recovery_source(
            settings=settings,
            db=db,
            adapter=adapter,
            command=logical_command,
        )
        if direct_drawdown_recovery_source_order is None
        else None
    )
    if (
        existing is not None
        and retryable_safety_fenced_order is None
        and direct_drawdown_recovery_source_order is None
    ):
        current_recovery_scope = _account_drawdown_recovery_scope(
            settings=settings,
            provider=adapter.provider,
            asset_class=logical_command.asset_class,
            exchange=logical_command.exchange,
        )
        if (
            not settings.account_drawdown_recovery_scope_secret
            and active_drawdown_recovery_source_order is existing
            and _account_drawdown_recovery_order_scope(existing)
            != current_recovery_scope
        ):
            # Without a dedicated stable scope key, rotating the KIS secret
            # changes the opaque account fingerprint. Treat an exact replay of
            # that orphaned same-currency loss row as the active fail-closed
            # latch instead of downgrading it to an ordinary rejected duplicate.
            return _account_drawdown_recovery_blocked_result(
                source_order=existing,
                block_reason="account_drawdown_recovery_scope_mismatch",
                message=(
                    "an active account drawdown latch belongs to a different "
                    "opaque account scope; restore the original stable scope "
                    "secret or complete reviewed operator remediation"
                ),
            )
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(existing, include_events=True),
            },
        )
    drawdown_recovery_source_order = (
        direct_drawdown_recovery_source_order
        or active_drawdown_recovery_source_order
    )
    control_state = ensure_runtime_control_state(settings=settings, db=db)
    submission_block_reason = automated_order_submission_block_reason(control_state)
    if (
        submission_block_reason is None
        and command.side == "buy"
        and _is_new_buy_order(command=command, plan_item=None)
    ):
        submission_block_reason = automated_buy_submission_block_reason(control_state)
    if submission_block_reason is not None:
        block_reason = submission_block_reason
        retry_after_at = (
            hourly_risk_off_retry_after_at(
                settings=settings,
                state=control_state,
                now=observed_at,
            )
            if block_reason == "hourly_plan_risk_off"
            else None
        )
        if retryable_safety_fenced_order is not None:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": True,
                    "message": (
                        "automated order submission remains blocked by runtime safety controls"
                    ),
                    "block_reason": block_reason,
                    "retry_after_at": (
                        retry_after_at.isoformat() if retry_after_at is not None else None
                    ),
                    "safety": runtime_safety_payload(control_state),
                    "order": serialize_broker_order(
                        retryable_safety_fenced_order,
                        include_events=True,
                    ),
                },
            )
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "message": (
                    "automated order submission is blocked by runtime safety controls: "
                    f"{block_reason}"
                ),
                "block_reason": block_reason,
                "retry_after_at": (
                    retry_after_at.isoformat() if retry_after_at is not None else None
                ),
                "safety": runtime_safety_payload(control_state),
            },
        )

    provider_error = _validate_provider_capability(adapter=adapter, command=command)
    if provider_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={
                "duplicate": False,
                "message": provider_error,
                "permanent_rejection": True,
                "rejection_reason": provider_error,
            },
        )

    plan_item = _load_plan_item(
        settings=settings,
        db=db,
        command=command,
    )
    if plan_item is None and command.execution_plan_item_id is not None:
        return PlaceBrokerOrderResult(
            status_code=404,
            payload={
                "message": "execution_plan_item_id was not found",
                "execution_plan_item_id": command.execution_plan_item_id,
            },
        )

    if plan_item is not None and command.side == "buy":
        from app.services.core_handoff_v4 import (
            fence_unproven_legacy_core_buy_authority,
        )

        if fence_unproven_legacy_core_buy_authority(
            db=db,
            execution_plan_item_id=plan_item.id,
            settings=settings,
        ):
            plan_item = _load_plan_item(
                settings=settings,
                db=db,
                command=command,
            )

    plan_state_error = _plan_item_state_error(
        plan_item=plan_item,
        command=command,
    )
    if plan_state_error is not None:
        return plan_state_error
    if (
        retryable_safety_fenced_order is not None
        and not retrying_proven_pre_submit_failure
        and _fresh_monitoring_sell_retrigger_is_authorized(
            plan_item=plan_item,
            command=logical_command,
            request_source=request_source,
        )
    ):
        # Runtime-safety rows prove that the prior attempt never crossed the
        # broker boundary, but their original monitoring metadata is stale by
        # definition on a later trigger.  The stable command/account identity
        # check above still gates reuse of the row; only an exact, newly
        # persisted monitoring authority may replace the old observed price,
        # route time, and trigger binding.
        command = logical_command
    initial_plan_item_identity = _plan_item_submission_identity(plan_item)

    account_route_mismatch = _plan_item_account_route_mismatch(
        settings=settings,
        plan_item=plan_item,
        provider=adapter.provider,
    )
    if account_route_mismatch is not None:
        return _account_route_blocked_result(account_route_mismatch)

    scoped_authority_reason = (
        core_buy_authority_block_reason(
            settings=settings,
            db=db,
            plan_item=plan_item,
            now=observed_at,
        )
        if command.side == "buy"
        and _is_new_buy_order(command=command, plan_item=plan_item)
        else None
    )
    if scoped_authority_reason is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": retryable_safety_fenced_order is not None,
                "blocked": True,
                "message": (
                    "new Core-v4 KRX buy requires a fresh acknowledged scoped "
                    "market authority observation"
                ),
                "block_reason": scoped_authority_reason,
                "permanent_rejection": False,
            },
        )

    validation_error = _validate_plan_item(
        plan_item=plan_item,
        command=command,
        request_source=request_source,
    )
    if validation_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={"message": validation_error},
        )
    initial_server_owned_command_authority = bool(
        plan_item is not None
        and _server_owned_plan_command_matches(
            risk_context=(
                plan_item.risk_context
                if isinstance(plan_item.risk_context, dict)
                else {}
            ),
            command=logical_command,
            request_source=request_source,
        )
    )
    rejected_retry_lineage_proof = _rejected_order_retry_lineage_proof(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    )

    if (
        command.side == "buy"
        and command.order_type == "market"
        and isinstance(getattr(plan_item, "risk_context", None), dict)
        and "exec_agent_decision" in plan_item.risk_context
        and _verified_exec_agent_market_notional(
            command=command,
            plan_item=plan_item,
        )
        is None
    ):
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={
                "duplicate": False,
                "message": "market buy requires an internally consistent exec-agent approval",
                "block_reason": "exec_agent_approval_invalid",
                "permanent_rejection": True,
            },
        )

    monitoring_sell_cap_error = _monitoring_sell_quantity_cap_error(
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=rejected_retry_lineage_proof,
    )
    if monitoring_sell_cap_error is not None:
        block_reason, message = monitoring_sell_cap_error
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": message,
                "block_reason": block_reason,
                "permanent_rejection": True,
                "rejection_reason": message,
            },
        )

    strategy_identity_error = _automated_strategy_identity_error(
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=rejected_retry_lineage_proof,
    )
    if strategy_identity_error is not None:
        block_reason, message = strategy_identity_error
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason=block_reason,
            message=message,
        )

    if command.side == "buy" and not _is_new_buy_order(
        command=command,
        plan_item=plan_item,
    ):
        # Preserve the existing risk-reduction/protective-hedge exemption from
        # loss-threshold gating. Cash and exposure reservation checks below
        # remain mandatory for these orders.
        drawdown_recovery_source_order = None
        direct_drawdown_recovery_source_order = None
    if (
        drawdown_recovery_source_order is not None
        and plan_item is not None
        and request_source.startswith(ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES)
    ):
        # A monitoring/job candidate can be cancelled while it is waiting on
        # an account-wide cooldown or scope check. Refresh and lock its durable
        # state before every early recovery response so the caller cannot
        # overwrite a terminal operator decision with a retryable status.
        recovery_candidate_state_error = _account_recovery_candidate_state_error(
            db=db,
            plan_item=plan_item,
            source_order=drawdown_recovery_source_order,
            request_source=request_source,
            retryable_safety_fenced_order=retryable_safety_fenced_order,
            lock=False,
        )
        if recovery_candidate_state_error is not None:
            return recovery_candidate_state_error
    if (
        drawdown_recovery_source_order is not None
        and _account_drawdown_recovery_order_scope(drawdown_recovery_source_order)
        != _account_drawdown_recovery_scope(
            settings=settings,
            provider=adapter.provider,
            asset_class=logical_command.asset_class,
            exchange=logical_command.exchange,
        )
    ):
        recovery_candidate_state_error = _account_recovery_candidate_state_error(
            db=db,
            plan_item=plan_item,
            source_order=drawdown_recovery_source_order,
            request_source=request_source,
            retryable_safety_fenced_order=retryable_safety_fenced_order,
            lock=True,
        )
        if recovery_candidate_state_error is not None:
            return recovery_candidate_state_error
        return _account_drawdown_recovery_blocked_result(
            source_order=drawdown_recovery_source_order,
            block_reason="account_drawdown_recovery_scope_mismatch",
            message=(
                "an active account drawdown latch belongs to a different opaque "
                "account scope; restore the original stable scope secret or "
                "complete reviewed operator remediation"
            ),
            retry_after_at=(
                observed_at
                + timedelta(
                    seconds=settings.account_drawdown_recovery_min_observation_interval_seconds
                )
            ),
        )
    if (
        drawdown_recovery_source_order is not None
        and settings.account_drawdown_gate_enabled
        and not settings.account_drawdown_recovery_enabled
    ):
        recovery_candidate_state_error = _account_recovery_candidate_state_error(
            db=db,
            plan_item=plan_item,
            source_order=drawdown_recovery_source_order,
            request_source=request_source,
            retryable_safety_fenced_order=retryable_safety_fenced_order,
            lock=True,
        )
        if recovery_candidate_state_error is not None:
            return recovery_candidate_state_error
        return _account_drawdown_recovery_blocked_result(
            source_order=drawdown_recovery_source_order,
            block_reason="account_drawdown_recovery_disabled",
            message="account drawdown recovery is disabled by configuration",
        )

    if (
        command.side == "buy"
        and _is_new_buy_order(command=command, plan_item=plan_item)
        and control_state.hourly_risk_off_active
    ):
        recovery_candidate_state_error = _account_recovery_candidate_state_error(
            db=db,
            plan_item=plan_item,
            source_order=drawdown_recovery_source_order,
            request_source=request_source,
            retryable_safety_fenced_order=retryable_safety_fenced_order,
            lock=True,
        )
        if recovery_candidate_state_error is not None:
            return recovery_candidate_state_error
        retry_after_at = hourly_risk_off_retry_after_at(
            settings=settings,
            state=control_state,
            now=observed_at,
        )
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": retryable_safety_fenced_order is not None,
                "message": "new buy submission is blocked by the active hourly risk-off fence",
                "block_reason": "hourly_plan_risk_off",
                "retry_after_at": (
                    retry_after_at.isoformat() if retry_after_at is not None else None
                ),
                "safety": runtime_safety_payload(control_state),
                **(
                    {
                        "order": serialize_broker_order(
                            retryable_safety_fenced_order,
                            include_events=True,
                        )
                    }
                    if retryable_safety_fenced_order is not None
                    else {}
                ),
            },
        )

    recovery_generation: int | None = None
    recovery_already_released = False
    recovery_candidate_can_advance = False
    scope_recovery_observer = False
    if drawdown_recovery_source_order is not None:
        if drawdown_recovery_source_order.execution_plan_item_id is None:
            recovery_candidate_can_advance = bool(
                direct_drawdown_recovery_source_order is not None
                and plan_item is None
                and logical_command.execution_plan_item_id is None
                and request_source == "post:/orders"
                and logical_command.idempotency_key
                == drawdown_recovery_source_order.idempotency_key
                and logical_command.symbol == drawdown_recovery_source_order.symbol
                and logical_command.side == drawdown_recovery_source_order.side == "buy"
            )
        else:
            recovery_candidate_can_advance = bool(
                direct_drawdown_recovery_source_order is not None
                and plan_item is not None
                and request_source.startswith(
                    ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
                )
                and plan_item.id
                == drawdown_recovery_source_order.execution_plan_item_id
                and logical_command.execution_plan_item_id
                == drawdown_recovery_source_order.execution_plan_item_id
                and logical_command.idempotency_key
                == drawdown_recovery_source_order.idempotency_key
                and logical_command.symbol == drawdown_recovery_source_order.symbol
                and logical_command.side == drawdown_recovery_source_order.side == "buy"
            )
        scope_recovery_observer = bool(
            not recovery_candidate_can_advance
            and plan_item is not None
            and request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and plan_item.id
            != drawdown_recovery_source_order.execution_plan_item_id
            and logical_command.execution_plan_item_id == plan_item.id
            and logical_command.side == "buy"
            and _is_new_buy_order(command=logical_command, plan_item=plan_item)
        )
        recovery_generation = 1
        state = recovery_state(
            db=db,
            execution_plan_item_id=(
                drawdown_recovery_source_order.execution_plan_item_id
            ),
            source_broker_order_pk=drawdown_recovery_source_order.id,
        )
        blocked_at = (
            drawdown_recovery_source_order.last_broker_sync_at
            or drawdown_recovery_source_order.created_at
        )
        cooldown_until = recovery_cooldown_until(
            blocked_at=blocked_at,
            settings=settings,
            last_unhealthy_at=state.last_unhealthy_at,
        )
        if settings.account_drawdown_gate_enabled and observed_at < cooldown_until:
            recovery_candidate_state_error = _account_recovery_candidate_state_error(
                db=db,
                plan_item=plan_item,
                source_order=drawdown_recovery_source_order,
                request_source=request_source,
                retryable_safety_fenced_order=retryable_safety_fenced_order,
                lock=True,
            )
            if recovery_candidate_state_error is not None:
                return recovery_candidate_state_error
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_cooldown",
                message="account drawdown recovery cooldown has not elapsed",
                retry_after_at=cooldown_until,
            )
        recovery_key = recovery_idempotency_key(
            base_key=logical_command.idempotency_key,
            source_broker_order_pk=drawdown_recovery_source_order.id,
            generation=recovery_generation,
        )
        recovery_already_released = recovery_state_is_current_release(
            settings=settings,
            state=state,
            source_broker_order_pk=drawdown_recovery_source_order.id,
            source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
            recovery_generation=recovery_generation,
        )
        recovery_metadata = dict(logical_command.metadata or {})
        recovery_metadata["account_drawdown_recovery"] = {
            "policy_version": 1,
            "recovery_scope": _account_drawdown_recovery_scope(
                settings=settings,
                provider=adapter.provider,
                asset_class=logical_command.asset_class,
                exchange=logical_command.exchange,
            ),
            "source_broker_order_pk": drawdown_recovery_source_order.id,
            "source_idempotency_key": drawdown_recovery_source_order.idempotency_key,
            "recovery_generation": recovery_generation,
        }
        if not scope_recovery_observer:
            command = logical_command.model_copy(
                update={
                    "idempotency_key": recovery_key,
                    "metadata": recovery_metadata,
                }
            )
            existing_recovery_order = _load_order_by_idempotency_key(
                settings=settings,
                db=db,
                idempotency_key=recovery_key,
            )
            if existing_recovery_order is not None:
                recovery_pre_submit_failure_order = (
                    existing_recovery_order
                    if _retryable_pre_submit_failure_matches_command(
                        settings=settings,
                        adapter=adapter,
                        order=existing_recovery_order,
                        command=command,
                        request_source=request_source,
                    )
                    else None
                )
                if recovery_pre_submit_failure_order is not None or _runtime_safety_fenced_order_matches_command(
                    settings=settings,
                    adapter=adapter,
                    order=existing_recovery_order,
                    command=command,
                ):
                    retryable_safety_fenced_order = existing_recovery_order
                    retrying_proven_pre_submit_failure = (
                        recovery_pre_submit_failure_order is not None
                    )
                    if recovery_pre_submit_failure_order is not None or not _runtime_safety_order_allows_resized_retrigger(
                        existing_recovery_order
                    ):
                        command = _runtime_safety_replay_command(
                            existing_recovery_order
                        ) or command
                else:
                    return PlaceBrokerOrderResult(
                        status_code=200,
                        payload={
                            "duplicate": True,
                            "message": "account drawdown recovery order already exists",
                            "order": serialize_broker_order(
                                existing_recovery_order,
                                include_events=True,
                            ),
                        },
                    )
        if (
            plan_item is not None
            and not _plan_status_allows_automated_retry(
                status=plan_item.status,
                retryable_safety_fenced_order=retryable_safety_fenced_order,
            )
        ):
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_candidate_ineligible",
                message=(
                    "execution plan item is no longer eligible for automatic "
                    "account drawdown recovery"
                ),
            )

    upbit_guard_error = _validate_upbit_paper_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    )
    if upbit_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": upbit_guard_error,
                "permanent_rejection": True,
                "rejection_reason": upbit_guard_error,
            },
        )

    alpaca_guard_error = _validate_alpaca_crypto_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    )
    if alpaca_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": alpaca_guard_error,
                **(
                    {"block_reason": "strategy_risk_authority_required"}
                    if alpaca_guard_error
                    == "Alpaca buys require internal strategy risk authority"
                    else {}
                ),
                "permanent_rejection": True,
                "rejection_reason": alpaca_guard_error,
            },
        )

    kis_us_guard_error = _validate_kis_us_equity_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    )
    if kis_us_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": kis_us_guard_error,
                "permanent_rejection": True,
                "rejection_reason": kis_us_guard_error,
            },
        )

    kis_us_session_route = _kis_us_session_route(command)
    if kis_us_session_route is not None:
        return PlaceBrokerOrderResult(
            status_code=202,
            payload={
                "duplicate": False,
                "message": str(kis_us_session_route["reason"]),
                "deferred": True,
                "block_reason": None,
                "route": kis_us_session_route,
            },
        )

    domestic_market_date_block = _domestic_equity_market_date_block(
        settings=settings,
        adapter=adapter,
        command=command,
        now=now,
    )
    if domestic_market_date_block is not None:
        return PlaceBrokerOrderResult(
            status_code=202,
            payload={
                "duplicate": False,
                "message": domestic_market_date_block["message"],
                "deferred": True,
                "block_reason": domestic_market_date_block["reason"],
                "market": domestic_market_date_block,
            },
        )

    route_decision = _resolve_route_decision(
        settings=settings,
        command=command,
        plan_item=plan_item,
    )
    routed_command = _routed_order_command(
        command=command,
        route_decision=route_decision,
    )
    rejected_retry_route_guard = _rejected_order_retry_route_guard(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        routed_command=routed_command,
        request_source=request_source,
        observed_at=observed_at,
        retry_lineage_proof=rejected_retry_lineage_proof,
    )
    if rejected_retry_route_guard is not None:
        return rejected_retry_route_guard
    if route_decision is not None and route_decision.action != "submit":
        return PlaceBrokerOrderResult(
            status_code=202 if route_decision.action == "defer" else 409,
            payload={
                "duplicate": False,
                "message": route_decision.reason,
                "deferred": route_decision.action == "defer",
                "block_reason": (
                    route_decision.reason
                    if route_decision.action == "block"
                    else None
                ),
                "route": _route_payload(route_decision),
            },
        )
    if routed_command.venue is None:
        routed_command = routed_command.model_copy(
            update={"venue": adapter.provider}
        )
    routed_command, watch_buy_identity_error = (
        _bind_server_owned_watch_buy_identity(
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        )
    )
    if watch_buy_identity_error is not None:
        block_reason, message = watch_buy_identity_error
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason=block_reason,
            message=message,
        )
    if _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=routed_command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        original_metadata = dict(command.metadata or {})
        original_metadata[STRATEGY_ENTRY_FAMILY_METADATA_KEY] = (
            routed_command.metadata or {}
        )[STRATEGY_ENTRY_FAMILY_METADATA_KEY]
        command = command.model_copy(update={"metadata": original_metadata})
    strategy_execution_scope_required = _is_trusted_alpaca_strategy_source(
        request_source
    )
    strategy_expected_execution_scope = _trusted_strategy_execution_scope(
        command=routed_command,
        request_source=request_source,
    )
    if (
        strategy_execution_scope_required
        and strategy_expected_execution_scope is None
    ):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "Alpaca strategy order lacks a valid immutable execution scope"
                ),
                "block_reason": "strategy_execution_scope_unavailable",
            },
        )

    prepared_broker_session: BrokerSession | None = None
    prepared_broker_session_error: str | None = None
    prepared_broker_session_exception: Exception | None = None
    prepared_authenticated_account_id_resolved = False
    prepared_authenticated_broker_account_id: str | None = None
    prepared_execution_scope: dict[str, object] | None = None
    # Broker authentication can commit token bookkeeping. Do it before the
    # PostgreSQL transaction-scoped account lock so the commit cannot silently
    # release serialization between exact-scope reservation evaluation and the
    # durable submission-intent commit.
    try:
        prepared_broker_session = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="order_reservation_scope",
            reason=(
                "prepare authenticated execution scope before serialized "
                f"reservation checks for {routed_command.idempotency_key}"
            ),
        )
    except Exception as exc:
        prepared_broker_session_exception = exc
        prepared_broker_session_error = safe_broker_error_message(exc)
    if prepared_broker_session is not None:
        (
            prepared_authenticated_account_id_resolved,
            prepared_authenticated_broker_account_id,
        ) = _load_authenticated_broker_account_id(
            adapter=adapter,
            access_token=prepared_broker_session.access_token or "",
        )
        try:
            prepared_execution_scope = _canonical_execution_scope(
                settings=settings,
                adapter=adapter,
                access_token=prepared_broker_session.access_token or "",
                command=routed_command,
                broker_account_id=prepared_authenticated_broker_account_id,
                broker_account_id_resolved=(
                    prepared_authenticated_account_id_resolved
                ),
            )
        except Exception:
            prepared_execution_scope = None
        if (
            prepared_execution_scope is not None
            and strategy_expected_execution_scope is not None
            and prepared_execution_scope != strategy_expected_execution_scope
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": (
                        "authenticated account does not match the strategy "
                        "decision scope"
                    ),
                    "block_reason": "strategy_execution_scope_changed",
                },
            )
    elif (
        prepared_broker_session_exception is not None
        and adapter.provider != "alpaca"
    ):
        # KIS and Upbit account scope is derived from stable configured route
        # authority rather than the transient access token. Persist it even
        # when authentication failed before any broker call so retryable orders
        # remain attributable and reserve only their proven account/currency.
        try:
            prepared_execution_scope = _canonical_execution_scope(
                settings=settings,
                adapter=adapter,
                access_token="",
                command=routed_command,
                broker_account_id_resolved=True,
            )
        except BrokerAdapterError:
            prepared_execution_scope = None
    elif (
        prepared_broker_session_exception is not None
        and adapter.provider == "alpaca"
        and strategy_expected_execution_scope is not None
    ):
        # The trusted strategy already bound this decision to authenticated
        # account truth. Preserve that immutable scope on a retryable pre-call
        # failure so the pending reservation remains attributable, while the
        # failure path still prevents any broker submission.
        prepared_execution_scope = dict(strategy_expected_execution_scope)

    if (
        prepared_broker_session is not None
        and prepared_execution_scope is None
    ):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "authenticated execution scope is indeterminate; "
                    "submission is blocked"
                ),
                "block_reason": "execution_scope_indeterminate",
            },
        )
    if (
        prepared_broker_session_exception is not None
        and prepared_execution_scope is None
    ):
        return PlaceBrokerOrderResult(
            status_code=503,
            payload={
                "duplicate": False,
                "message": prepared_broker_session_error
                or "broker authentication failed before submission",
                "block_reason": "execution_scope_indeterminate",
                "manual_reconciliation_required": False,
            },
        )

    _acquire_cross_process_order_intent_lock(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        execution_scope=prepared_execution_scope,
    )
    lane_boundary_block = _evaluate_automated_buy_lane_boundary(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        plan_item=plan_item,
        request_source=request_source,
        execution_scope=prepared_execution_scope,
        exclude_order_id=(
            retryable_safety_fenced_order.id
            if retryable_safety_fenced_order is not None
            else None
        ),
    )
    if lane_boundary_block is not None:
        return lane_boundary_block
    quarantine_block = _kis_us_otc_quarantine_block(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        execution_scope=prepared_execution_scope,
        lock=True,
    )
    if quarantine_block is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": retryable_safety_fenced_order is not None,
                "blocked": True,
                "permanent_rejection": False,
                **quarantine_block,
            },
        )

    if plan_item is not None:
        pending_risk_context = _json_dict(plan_item.risk_context)
        current_plan_state = _current_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=False,
            refresh_authority=True,
        )
        if current_plan_state is None:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "execution plan item disappeared before order submission",
                    "block_reason": "execution_plan_item_state_changed",
                },
            )
        current_status, current_risk_context = current_plan_state
        if (
            _plan_item_submission_identity(plan_item)
            != initial_plan_item_identity
            or _validate_plan_item(
                plan_item=plan_item,
                command=logical_command,
                request_source=request_source,
            )
            is not None
        ):
            return _plan_item_identity_changed_result()
        if (
            routed_command.side == "buy"
            and _risk_context_has_active_hourly_risk_off(current_risk_context)
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "buy submission is blocked by the active hourly risk-off fence",
                    "block_reason": "hourly_plan_risk_off",
                },
            )
        if current_status in PLAN_ITEM_NONCLAIMABLE_STATUSES:
            return _plan_item_state_changed_result(side=routed_command.side)
        if (
            routed_command.side == "buy"
            and request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and not _plan_status_allows_automated_retry(
                status=current_status,
                retryable_safety_fenced_order=retryable_safety_fenced_order,
            )
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "execution plan item is no longer eligible for automated buy submission",
                    "block_reason": "execution_plan_item_state_changed",
                },
            )
        if (
            routed_command.side == "buy"
            and drawdown_recovery_source_order is not None
            and not _plan_status_allows_automated_retry(
                status=current_status,
                retryable_safety_fenced_order=retryable_safety_fenced_order,
            )
        ):
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_candidate_ineligible",
                message=(
                    "execution plan item became ineligible while waiting for "
                    "the account lock"
                ),
            )
        # The caller may hold an unflushed monitoring transition while another
        # transaction has already revoked/quarantined the durable row. Always
        # continue from the latest durable status; never write the caller's
        # stale status back over it. A row lock is taken again immediately
        # before the intent commit after the network-backed safety checks.
        plan_item.status = current_status
        plan_item.risk_context = _merge_pending_plan_item_risk_context(
            persisted=current_risk_context,
            pending=pending_risk_context,
        )

    # The first latch lookup happens before PostgreSQL's account-scoped
    # advisory lock.  A competing request can commit a new loss block while
    # this transaction is waiting, so repeat the lookup after acquiring the
    # lock.  Returning for a fresh evaluation is deliberately fail-closed: a
    # command prepared before the latch existed must never become its release
    # child or submit with stale recovery state.
    locked_active_recovery_source = _load_active_account_drawdown_recovery_source(
        settings=settings,
        db=db,
        adapter=adapter,
        command=logical_command,
    )
    if locked_active_recovery_source is not None and (
        drawdown_recovery_source_order is None
        or locked_active_recovery_source.id != drawdown_recovery_source_order.id
    ):
        return _account_drawdown_recovery_blocked_result(
            source_order=locked_active_recovery_source,
            block_reason="account_drawdown_recovery_state_changed",
            message=(
                "account drawdown recovery state changed while waiting for the "
                "account lock; reevaluate this candidate"
            ),
        )

    if drawdown_recovery_source_order is not None and retryable_safety_fenced_order is None:
        concurrent_recovery_order = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=routed_command.idempotency_key,
        )
        if concurrent_recovery_order is not None:
            return PlaceBrokerOrderResult(
                status_code=200,
                payload={
                    "duplicate": True,
                    "message": "account drawdown recovery order already exists",
                    "order": serialize_broker_order(
                        concurrent_recovery_order,
                        include_events=True,
                    ),
                },
            )

    if drawdown_recovery_source_order is not None:
        locked_recovery_state = recovery_state(
            db=db,
            execution_plan_item_id=(
                drawdown_recovery_source_order.execution_plan_item_id
            ),
            source_broker_order_pk=drawdown_recovery_source_order.id,
        )
        locked_recovery_release_is_current = recovery_state_is_current_release(
            settings=settings,
            state=locked_recovery_state,
            source_broker_order_pk=drawdown_recovery_source_order.id,
            source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
            recovery_generation=recovery_generation or 1,
        )
        if locked_recovery_state.released and not locked_recovery_release_is_current:
            # Another process released this account-wide latch while this
            # candidate waited on the account intent lock, or the durable
            # release summary is incomplete/stale. Never infer release from the
            # event type alone.
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_state_changed",
                message=(
                    "account drawdown recovery was released by another order; "
                    "reevaluate this candidate against fresh account state"
                ),
            )
        if locked_recovery_release_is_current and not recovery_already_released:
            # Do not let two candidates both cross a release that committed
            # while this transaction waited on the account-scoped intent lock.
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_state_changed",
                message=(
                    "account drawdown recovery was released by another order; "
                    "reevaluate this candidate against fresh account state"
                ),
            )

    unresolved_selection = _load_unresolved_prior_submission(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        execution_scope=prepared_execution_scope,
        plan_item=plan_item,
        request_source=request_source,
    )
    if unresolved_selection.indeterminate_order is not None:
        return _reservation_scope_indeterminate_result(
            unresolved_selection.indeterminate_order
        )
    unresolved_prior = next(iter(unresolved_selection.matching_orders), None)
    if unresolved_prior is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "a prior submission for this symbol and side has unresolved broker truth; "
                    "reconcile it before creating a replacement order"
                ),
                "block_reason": "unresolved_prior_submission",
                "prior_order_id": unresolved_prior.id,
                "prior_order_status": unresolved_prior.status,
            },
        )

    prepared_sell_snapshot: BrokerAccountSnapshot | None = None
    prepared_sell_snapshot_error: str | None = None
    if routed_command.side == "sell" and adapter.provider == "kis":
        if prepared_broker_session_exception is not None or prepared_broker_session is None:
            prepared_sell_snapshot_error = (
                prepared_broker_session_error
                or "broker authentication failed before position verification"
            )
        else:
            try:
                prepared_sell_snapshot = adapter.get_account_snapshot(
                    prepared_broker_session.access_token or "",
                    symbols=[routed_command.symbol],
                    exchange=routed_command.exchange,
                )
            except Exception as exc:
                prepared_sell_snapshot_error = safe_broker_error_message(exc)

    family_sell_reservation_block = _evaluate_automated_family_sell_boundary(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        plan_item=plan_item,
        request_source=request_source,
        execution_scope=prepared_execution_scope,
        account_snapshot=prepared_sell_snapshot,
        account_snapshot_error=prepared_sell_snapshot_error,
        retry_lineage_proof=rejected_retry_lineage_proof,
        exclude_order_id=(
            retryable_safety_fenced_order.id
            if retrying_proven_pre_submit_failure
            and retryable_safety_fenced_order is not None
            else None
        ),
    )
    if family_sell_reservation_block is not None:
        return family_sell_reservation_block

    sell_reservation_block = (
        None
        if prepared_broker_session_exception is not None
        else _evaluate_sell_reservation_gate(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            request_source=request_source,
            execution_scope=prepared_execution_scope,
            session_state=prepared_broker_session,
            session_error=prepared_broker_session_error,
            account_snapshot=prepared_sell_snapshot,
            account_snapshot_error=prepared_sell_snapshot_error,
            retry_lineage_proof=rejected_retry_lineage_proof,
            exclude_order_id=(
                retryable_safety_fenced_order.id
                if retrying_proven_pre_submit_failure
                and retryable_safety_fenced_order is not None
                else None
            ),
        )
    )
    if sell_reservation_block is not None:
        return sell_reservation_block

    if (
        drawdown_recovery_source_order is not None
        and settings.account_drawdown_gate_enabled
        and not recovery_candidate_can_advance
        and not scope_recovery_observer
    ):
        return _account_drawdown_recovery_blocked_result(
            source_order=drawdown_recovery_source_order,
            block_reason="account_drawdown_recovery_pending",
            message=(
                "account drawdown recovery awaits the original stopped buy "
                "candidate"
            ),
        )

    account_gate_settings = (
        recovery_gate_settings(settings)
        if drawdown_recovery_source_order is not None
        and settings.account_drawdown_gate_enabled
        else settings
    )
    account_gate_decision = (
        None
        if prepared_broker_session_exception is not None
        else _evaluate_new_buy_account_gate(
            settings=account_gate_settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            now=observed_at,
            freshness_reference_at=snapshot_freshness_reference_at,
            session_state=prepared_broker_session,
            session_error=prepared_broker_session_error,
            execution_scope=prepared_execution_scope,
            require_fresh_snapshot=drawdown_recovery_source_order is not None,
            exclude_order_id=(
                retryable_safety_fenced_order.id
                if retrying_proven_pre_submit_failure
                and retryable_safety_fenced_order is not None
                else None
            ),
        )
    )
    if account_gate_decision is not None and not account_gate_decision.allowed:
        if drawdown_recovery_source_order is not None:
            if not recovery_already_released and (
                recovery_candidate_can_advance or scope_recovery_observer
            ):
                record_recovery_reset(
                    db=db,
                    execution_plan_item_id=(
                        drawdown_recovery_source_order.execution_plan_item_id
                    ),
                    source_broker_order_pk=drawdown_recovery_source_order.id,
                    source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
                    recovery_generation=recovery_generation or 1,
                    decision=account_gate_decision,
                    settings=settings,
                    occurred_at=observed_at,
                )
                db.commit()
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_pending",
                message=(
                    "account drawdown metrics have not satisfied recovery thresholds"
                ),
                decision=account_gate_decision,
            )
        return _persist_account_gate_block(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            original_command=logical_command,
            request_source=request_source,
            plan_item=plan_item,
            route_decision=route_decision,
            decision=account_gate_decision,
            existing_order=retryable_safety_fenced_order,
            observed_at=observed_at,
        )

    if plan_item is not None:
        pending_risk_context = _json_dict(plan_item.risk_context)
        current_plan_state = _current_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=True,
            refresh_authority=True,
        )
        if current_plan_state is None:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "execution plan item disappeared before order intent commit",
                    "block_reason": "execution_plan_item_state_changed",
                },
            )
        current_status, current_risk_context = current_plan_state
        if (
            _plan_item_submission_identity(plan_item)
            != initial_plan_item_identity
            or _validate_plan_item(
                plan_item=plan_item,
                command=logical_command,
                request_source=request_source,
            )
            is not None
        ):
            return _plan_item_identity_changed_result()
        if (
            routed_command.side == "buy"
            and _risk_context_has_active_hourly_risk_off(current_risk_context)
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "buy submission is blocked by the active hourly risk-off fence",
                    "block_reason": "hourly_plan_risk_off",
                },
            )
        if current_status in PLAN_ITEM_NONCLAIMABLE_STATUSES:
            return _plan_item_state_changed_result(side=routed_command.side)
        if (
            routed_command.side == "buy"
            and request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and not _plan_status_allows_automated_retry(
                status=current_status,
                retryable_safety_fenced_order=retryable_safety_fenced_order,
            )
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "execution plan item changed while account safety checks were running",
                    "block_reason": "execution_plan_item_state_changed",
                },
            )
        plan_item.status = current_status
        plan_item.risk_context = _merge_pending_plan_item_risk_context(
            persisted=current_risk_context,
            pending=pending_risk_context,
        )

    if drawdown_recovery_source_order is not None:
        healthy_count = 0
        recovery_evaluation_available = (
            not settings.account_drawdown_gate_enabled
            or prepared_broker_session_exception is None
        )
        if settings.account_drawdown_gate_enabled and recovery_evaluation_available:
            assert account_gate_decision is not None
            if recovery_already_released:
                healthy_count = settings.account_drawdown_recovery_required_healthy_observations
            else:
                healthy_count, observation_recorded = record_healthy_observation(
                    db=db,
                    execution_plan_item_id=(
                        drawdown_recovery_source_order.execution_plan_item_id
                    ),
                    source_broker_order_pk=drawdown_recovery_source_order.id,
                    source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
                    recovery_generation=recovery_generation or 1,
                    decision=account_gate_decision,
                    settings=settings,
                    occurred_at=observed_at,
                )
                if (
                    not observation_recorded
                    or healthy_count
                    < settings.account_drawdown_recovery_required_healthy_observations
                ):
                    db.commit()
                    snapshot_observed_at = _normalize_utc_datetime(
                        account_gate_decision.baseline_timestamp
                        if isinstance(account_gate_decision.baseline_timestamp, datetime)
                        else datetime.fromisoformat(
                            str(account_gate_decision.baseline_timestamp).replace(
                                "Z", "+00:00"
                            )
                        )
                    )
                    retry_anchor = max(
                        observed_at,
                        snapshot_observed_at,
                        *(
                            [_normalize_utc_datetime(state.last_snapshot_observed_at)]
                            if state.last_snapshot_observed_at is not None
                            else []
                        ),
                    )
                    return _account_drawdown_recovery_blocked_result(
                        source_order=drawdown_recovery_source_order,
                        block_reason="account_drawdown_recovery_pending",
                        message="account drawdown recovery requires more healthy observations",
                        decision=account_gate_decision,
                        healthy_observation_count=healthy_count,
                        retry_after_at=(
                            retry_anchor
                            + timedelta(
                                seconds=settings.account_drawdown_recovery_min_observation_interval_seconds
                            )
                        ),
                    )
        if not recovery_already_released and recovery_evaluation_available:
            release_idempotency_key = (
                scope_recovery_release_idempotency_key(
                    source_broker_order_pk=drawdown_recovery_source_order.id,
                    generation=recovery_generation or 1,
                )
                if scope_recovery_observer
                else routed_command.idempotency_key
            )
            record_recovery_release(
                db=db,
                execution_plan_item_id=(
                    drawdown_recovery_source_order.execution_plan_item_id
                ),
                source_broker_order_pk=drawdown_recovery_source_order.id,
                source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
                recovery_idempotency_key=release_idempotency_key,
                recovery_generation=recovery_generation or 1,
                healthy_observation_count=healthy_count,
                settings=settings,
                occurred_at=observed_at,
                gate_disabled=not settings.account_drawdown_gate_enabled,
            )
        if scope_recovery_observer and recovery_evaluation_available:
            db.commit()
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_fresh_candidate_required",
                message=(
                    "account drawdown latch was released without an order; "
                    "submit only after a fresh candidate evaluation"
                ),
                decision=account_gate_decision,
                healthy_observation_count=healthy_count,
                retry_after_at=observed_at,
            )

    plan_execution_account_scope = execution_account_scope_binding_from_payloads(
        plan_item.risk_context if plan_item is not None else None,
        plan_item.quantity_policy if plan_item is not None else None,
    )
    decision_lineage = decision_lineage_for_plan_item(plan_item)
    request_metadata = {
        "source": request_source,
        "trusted_order_origin": _trusted_order_origin(request_source),
        "requested_at": datetime.now(UTC).isoformat(),
        "original_command": _order_command_payload(command),
        "time_in_force": routed_command.time_in_force,
        "provider": routed_command.provider,
        "asset_class": routed_command.asset_class,
        "venue": routed_command.venue,
        "sizing": _sizing_payload(routed_command),
        **(
            {"plan_item_status_before_submission": plan_item.status}
            if plan_item is not None
            else {}
        ),
        **(
            {"decision_lineage": decision_lineage}
            if decision_lineage is not None
            else {}
        ),
        **(
            {"metadata": redact_account_references(routed_command.metadata)}
            if routed_command.metadata is not None
            else {}
        ),
        **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
        **(
            {"plan_execution_account_scope": plan_execution_account_scope}
            if plan_execution_account_scope is not None
            else {}
        ),
        **(
            {
                "execution_scope": dict(prepared_execution_scope),
                "exchange": prepared_execution_scope["exchange"],
            }
            if prepared_execution_scope is not None
            else {}
        ),
        **(
            {
                "broker_account_scope": {
                    "policy_version": 1,
                    "scope": _account_drawdown_recovery_scope(
                        settings=settings,
                        provider=adapter.provider,
                        asset_class=routed_command.asset_class,
                        exchange=routed_command.exchange,
                    ),
                }
            }
            if adapter.provider == "kis"
            else {}
        ),
    }
    symbol_exposure_reservation = (
        _automated_kis_symbol_exposure_reservation_payload(
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        )
    )
    if symbol_exposure_reservation is not None:
        request_metadata[SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY] = (
            symbol_exposure_reservation
        )
    request_metadata = _preserve_canonical_execution_scope(
        request_metadata=request_metadata,
        existing_order=(
            None
            if retrying_proven_pre_submit_failure
            else retryable_safety_fenced_order
        ),
    )
    request_metadata = _preserve_decision_lineage(
        request_metadata=request_metadata,
        existing_order=retryable_safety_fenced_order,
    )
    request_metadata = _redacted_payload(request_metadata) or {}
    pre_submit_failure = prepared_broker_session_exception is not None
    pre_submit_error_classification: str | None = None
    pre_submit_error_code: str | None = None
    pre_submit_error_retryable: bool | None = None
    pre_submit_error_message: str | None = None
    if prepared_broker_session_exception is not None:
        if isinstance(prepared_broker_session_exception, BrokerAdapterError):
            pre_submit_error_classification = (
                prepared_broker_session_exception.classification.value
            )
            pre_submit_error_code = safe_broker_error_code(
                prepared_broker_session_exception.code,
                provider=adapter.provider,
            )
            pre_submit_error_retryable = bool(
                prepared_broker_session_exception.retryable
            )
            pre_submit_error_message = safe_broker_error_message(
                prepared_broker_session_exception
            )
        else:
            pre_submit_error_classification = "pre_submit_failure"
            pre_submit_error_code = "PRE_SUBMIT_FAILURE"
            pre_submit_error_retryable = False
            pre_submit_error_message = (
                f"{prepared_broker_session_exception.__class__.__name__}: broker "
                "submission failed before the broker order call started"
            )
    initial_order_status = (
        (
            NormalizedBrokerOrderStatus.RETRY_PENDING.value
            if pre_submit_error_retryable
            else NormalizedBrokerOrderStatus.FAILED.value
        )
        if pre_submit_failure
        else NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value
    )
    if retryable_safety_fenced_order is None:
        order = BrokerOrder(
            broker_name=adapter.provider,
            execution_plan_item_id=plan_item.id if plan_item else None,
            mode=settings.runtime_mode,
            idempotency_key=routed_command.idempotency_key,
            symbol=routed_command.symbol,
            side=routed_command.side,
            status=initial_order_status,
            quantity=routed_command.quantity,
            base_quantity=routed_command.base_quantity,
            quote_notional=routed_command.quote_notional,
            quantity_precision=routed_command.quantity_precision,
            price_precision=routed_command.price_precision,
            venue=routed_command.venue,
            asset_class=routed_command.asset_class,
            limit_price=routed_command.limit_price,
            request_metadata=request_metadata,
            last_error_classification=pre_submit_error_classification,
            last_error_code=pre_submit_error_code,
            last_error_message=pre_submit_error_message,
            last_error_retryable=pre_submit_error_retryable,
        )
    else:
        order = retryable_safety_fenced_order
        apply_broker_order_status(
            order=order,
            status=initial_order_status,
        )
        order.quantity = routed_command.quantity
        order.base_quantity = routed_command.base_quantity
        order.quote_notional = routed_command.quote_notional
        order.quantity_precision = routed_command.quantity_precision
        order.price_precision = routed_command.price_precision
        order.venue = routed_command.venue
        order.asset_class = routed_command.asset_class
        order.limit_price = routed_command.limit_price
        order.request_metadata = request_metadata
        order.response_metadata = None
        order.last_error_classification = pre_submit_error_classification
        order.last_error_code = pre_submit_error_code
        order.last_error_message = pre_submit_error_message
        order.last_error_retryable = pre_submit_error_retryable
        order.raw_broker_payload = None
    db.add(order)
    request_observed_at = datetime.now(UTC)
    try:
        db.flush()
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type=(
                    "submit_failed_before_broker_call"
                    if pre_submit_failure
                    else "submit_requested"
                ),
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=initial_order_status,
                error_classification=pre_submit_error_classification,
                retryable=pre_submit_error_retryable,
                message=pre_submit_error_message,
                request_payload=_redacted_payload(
                    _order_command_payload(routed_command)
                ),
                payload=_redacted_payload(
                    {
                        "provider": adapter.provider,
                        **(
                            {
                                "exception_type": prepared_broker_session_exception.__class__.__name__,
                                "broker_call_started": False,
                                "manual_reconciliation_required": False,
                                "reported_error_classification": (
                                    pre_submit_error_classification
                                ),
                                "reported_retryable": pre_submit_error_retryable,
                            }
                            if prepared_broker_session_exception is not None
                            else {}
                        ),
                    }
                ),
                observed_at=request_observed_at,
            )
        )
        if plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status=(
                    _plan_item_status_for_order_status(initial_order_status)
                    if pre_submit_failure
                    else "broker_order_submission_pending"
                ),
                source="broker_order_service",
                reason=(
                    pre_submit_error_message
                    if pre_submit_failure
                    else f"broker order submission requested for {routed_command.idempotency_key}"
                ),
                payload={
                    "broker_order_pk": order.id,
                    "idempotency_key": order.idempotency_key,
                    **(
                        {
                            "event_type": "submit_failed_before_broker_call",
                            "broker_call_started": False,
                            "manual_reconciliation_required": False,
                        }
                        if pre_submit_failure
                        else {}
                    ),
                },
            )

        # Claim the idempotency key durably before crossing the broker boundary.
        # If the process loses the broker response, a retry observes this pending
        # intent and must reconcile it instead of submitting a second order.
        db.commit()
    except IntegrityError:
        db.rollback()
        duplicate = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
        if duplicate is None:
            raise
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(duplicate, include_events=True),
            },
        )
    db.refresh(order)

    if pre_submit_failure:
        return PlaceBrokerOrderResult(
            status_code=503,
            payload={
                "duplicate": False,
                "message": pre_submit_error_message,
                "manual_reconciliation_required": False,
                "order": serialize_broker_order(order, include_events=True),
            },
        )

    broker_call_started = False
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="broker_order_service",
            reason=f"ensure active broker session for {command.idempotency_key}",
        )
        (
            authenticated_account_id_resolved,
            authenticated_broker_account_id,
        ) = _load_authenticated_broker_account_id(
            adapter=adapter,
            access_token=session_state.access_token or "",
        )
        current_execution_scope = _canonical_execution_scope(
            settings=settings,
            adapter=adapter,
            access_token=session_state.access_token or "",
            command=routed_command,
            broker_account_id=authenticated_broker_account_id,
            broker_account_id_resolved=authenticated_account_id_resolved,
        )
        if (
            prepared_execution_scope is not None
            and current_execution_scope != prepared_execution_scope
        ):
            raise BrokerAdapterError(
                "authenticated execution scope changed after reservation checks",
                status_code=409,
                classification=BrokerErrorClassification.VALIDATION,
                retryable=False,
                code="execution_scope_changed",
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            )
        _bind_canonical_execution_scope(
            settings=settings,
            db=db,
            adapter=adapter,
            access_token=session_state.access_token or "",
            order=order,
            command=routed_command,
            broker_account_id=authenticated_broker_account_id,
            broker_account_id_resolved=authenticated_account_id_resolved,
        )
        current_execution_account_scope = _current_execution_account_scope_binding(
            settings=settings,
            adapter=adapter,
            command=routed_command,
            broker_account_id=authenticated_broker_account_id,
            broker_account_id_resolved=authenticated_account_id_resolved,
        )
        # Broker market evidence is network I/O. Load it before taking the final
        # runtime/plan locks so pause, kill, and cancellation controls remain
        # responsive. The evidence is revalidated against a monotonic-adjusted
        # clock after those locks are acquired and no outbound I/O occurs between
        # that final state check and ``place_order``.
        db.commit()
        market_evidence = _load_automated_kis_buy_market_evidence(
            settings=settings,
            adapter=adapter,
            access_token=session_state.access_token or "",
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            reference_at=(
                snapshot_freshness_reference_at
                + timedelta(seconds=max(0.0, monotonic() - submission_started))
            ),
        )
        sell_market_evidence = _load_automated_kis_sell_market_evidence(
            settings=settings,
            adapter=adapter,
            access_token=session_state.access_token or "",
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        )
        (
            symbol_exposure_snapshot,
            symbol_exposure_snapshot_error,
        ) = _load_automated_kis_symbol_exposure_snapshot(
            adapter=adapter,
            access_token=session_state.access_token or "",
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        )
        if _requires_automated_kis_symbol_exposure_boundary(
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        ):
            opposite_order_snapshot = symbol_exposure_snapshot
            opposite_order_snapshot_error = symbol_exposure_snapshot_error
        else:
            (
                opposite_order_snapshot,
                opposite_order_snapshot_error,
            ) = _load_automated_kis_opposite_order_snapshot(
                adapter=adapter,
                access_token=session_state.access_token or "",
                command=routed_command,
                plan_item=plan_item,
                request_source=request_source,
                retry_lineage_proof=rejected_retry_lineage_proof,
            )
        # Loading broker evidence happens after the durable intent commit,
        # which releases transaction-scoped PostgreSQL locks. Reacquire the
        # exact account/currency scope before any final authority checks.
        _acquire_cross_process_order_intent_lock(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            execution_scope=prepared_execution_scope,
        )
        control_state = _lock_runtime_control_for_broker_boundary(
            db=db,
            control_state=control_state,
        )
        if plan_item is not None:
            current_plan_state = _current_plan_item_state(
                db=db,
                item_id=plan_item.id,
                lock=True,
                refresh_authority=True,
            )
            if (
                current_plan_state is None
                or current_plan_state[0] != "broker_order_submission_pending"
            ):
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override="execution_plan_item_state_changed",
                    message_override=(
                        "execution plan item changed while final market evidence was loading"
                    ),
                    preserve_plan_item_status=True,
                )
            db.refresh(
                plan_item,
                attribute_names=[
                    "status",
                    "execution_plan_id",
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
                    "decision_agent_name",
                    "decision_snapshot_id",
                    "decision_run_id",
                    "source_plan_item_id",
                    "source_market_date",
                ],
            )
            if (
                initial_server_owned_command_authority
                and not _server_owned_plan_command_matches(
                    risk_context=(
                        plan_item.risk_context
                        if isinstance(plan_item.risk_context, dict)
                        else {}
                    ),
                    command=logical_command,
                    request_source=request_source,
                )
            ):
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override="execution_plan_item_identity_changed",
                    message_override=(
                        "execution plan item order authority changed while final "
                        "market evidence was loading"
                    ),
                    retryable_override=False,
                    permanent_rejection=True,
                )
            if _plan_item_submission_identity(plan_item) != initial_plan_item_identity:
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override="execution_plan_item_identity_changed",
                    message_override=(
                        "execution plan item identity changed while final market "
                        "evidence was loading"
                    ),
                    retryable_override=False,
                    permanent_rejection=True,
                )
        boundary_block_reason = (
            automated_buy_submission_block_reason(control_state)
            if routed_command.side == "buy"
            and _is_new_buy_order(command=routed_command, plan_item=plan_item)
            else automated_order_submission_block_reason(control_state)
        )
        if boundary_block_reason is not None:
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=boundary_block_reason,
                risk_off_fence=(boundary_block_reason == "hourly_plan_risk_off"),
            )
        account_route_mismatch = _plan_item_account_route_mismatch(
            settings=settings,
            plan_item=plan_item,
            provider=adapter.provider,
        )
        if account_route_mismatch is not None:
            return _persist_account_route_fence(
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                mismatch=account_route_mismatch,
            )
        authority_checked_at = _normalize_utc_datetime(
            now or datetime.now(UTC)
        )
        scoped_authority_reason = (
            core_buy_authority_block_reason(
                settings=settings,
                db=db,
                plan_item=plan_item,
                now=authority_checked_at,
                lock=True,
            )
            if routed_command.side == "buy"
            and _is_new_buy_order(command=routed_command, plan_item=plan_item)
            else None
        )
        if scoped_authority_reason is not None:
            if plan_item is not None:
                project_plan_item_for_core_buy_authority(
                    settings=settings,
                    db=db,
                    plan_item=plan_item,
                    now=authority_checked_at,
                )
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=scoped_authority_reason,
                message_override=(
                    "broker submission was fenced because the scoped Core KRX "
                    "buy authority is missing, blocked, or stale"
                ),
                preserve_plan_item_status=True,
            )
        execution_account_scope_mismatch = (
            _plan_item_execution_account_scope_mismatch(
                plan_item=plan_item,
                provider=adapter.provider,
                current_binding=current_execution_account_scope,
            )
            if plan_item is not None
            and adapter.provider != "kis"
            else None
        )
        if execution_account_scope_mismatch is not None:
            return _persist_execution_account_scope_fence(
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                mismatch=execution_account_scope_mismatch,
            )
        sell_stop_integrity_failure = _final_target_sell_stop_integrity_failure(
            db=db,
            command=routed_command,
            plan_item=plan_item,
        )
        if sell_stop_integrity_failure is not None:
            block_reason, message = sell_stop_integrity_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                plan_item_status_override=(
                    f"{plan_item.intent}_monitoring"
                    if plan_item is not None
                    else None
                ),
            )
        final_market_reference_at = (
            snapshot_freshness_reference_at
            + timedelta(seconds=max(0.0, monotonic() - submission_started))
        )
        next_open_window_failure = _automated_next_open_submission_window_failure(
            settings=settings,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            reference_at=final_market_reference_at,
        )
        if next_open_window_failure is not None:
            block_reason, message = next_open_window_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                plan_item_status_override="next_open_buy_expired",
                retryable_override=False,
                permanent_rejection=True,
            )
        provider_buy_capability_failure = _automated_provider_buy_capability_failure(
            settings=settings,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
        )
        if provider_buy_capability_failure is not None:
            block_reason, message = provider_buy_capability_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                plan_item_status_override=(
                    "next_open_buy_failed"
                    if plan_item is not None and plan_item.intent == "next_open_buy"
                    else "watch_buy_triggered_blocked"
                ),
                retryable_override=False,
                permanent_rejection=True,
            )
        market_evidence_failure = _automated_kis_buy_market_evidence_failure(
            settings=settings,
            command=routed_command,
            plan_item=plan_item,
            evidence=market_evidence,
            reference_at=final_market_reference_at,
        )
        if market_evidence_failure is not None:
            block_reason, message = market_evidence_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    "next_open_buy_retry_pending"
                    if plan_item is not None and plan_item.intent == "next_open_buy"
                    else "watch_buy_triggered_blocked"
                ),
            )
        sell_market_evidence_failure = _automated_kis_sell_market_evidence_failure(
            settings=settings,
            command=routed_command,
            plan_item=plan_item,
            evidence=sell_market_evidence,
            reference_at=final_market_reference_at,
        )
        if sell_market_evidence_failure is not None:
            block_reason, message = sell_market_evidence_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    f"{plan_item.intent}_monitoring"
                    if plan_item is not None
                    else None
                ),
            )
        if sell_market_evidence is not None:
            routed_command = _bind_automated_kis_sell_limit(
                command=routed_command,
                evidence=sell_market_evidence,
            )
            sell_context = sell_market_evidence.context
            if sell_context is not None:
                order.limit_price = routed_command.limit_price
                current_metadata = (
                    dict(order.request_metadata)
                    if isinstance(order.request_metadata, dict)
                    else {}
                )
                order.request_metadata = {
                    **current_metadata,
                    "final_broker_command": _order_command_payload(routed_command),
                    "final_sell_quote_evidence": {
                        "provider": "kis",
                        "symbol": sell_context.symbol,
                        "exchange": sell_context.exchange,
                        "observed_at": sell_context.observed_at.isoformat(),
                        "freshness_source": sell_context.freshness_source,
                        "executable_bid": str(sell_context.bid),
                    },
                }
                db.add(
                    BrokerOrderEvent(
                        broker_order_pk=order.id,
                        event_type="submission_sell_quote_revalidated",
                        event_source=BrokerEventSource.SUBMISSION.value,
                        normalized_status=order.status,
                        message=(
                            "automated monitoring sell revalidated against a fresh "
                            "executable bid"
                        ),
                        request_payload=_redacted_payload(
                            _order_command_payload(routed_command)
                        ),
                        payload=_redacted_payload(
                            {
                                "provider": "kis",
                                "symbol": sell_context.symbol,
                                "exchange": sell_context.exchange,
                                "observed_at": sell_context.observed_at.isoformat(),
                                "freshness_source": sell_context.freshness_source,
                                "executable_bid": str(sell_context.bid),
                            }
                        ),
                        observed_at=final_market_reference_at,
                    )
                )
                db.flush()
        if market_evidence is not None and market_evidence.is_domestic:
            route_decision = _final_automated_domestic_buy_route(
                settings=settings,
                command=routed_command,
                plan_item=plan_item,
                reference_at=final_market_reference_at,
            )
        (
            routed_command,
            route_decision,
            bounded_submission_failure,
        ) = _bind_automated_watch_buy_ioc_limit(
            command=routed_command,
            plan_item=plan_item,
            evidence=market_evidence,
            route_decision=route_decision,
        )
        if bounded_submission_failure is not None:
            block_reason, message = bounded_submission_failure
            return _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=block_reason,
                message_override=message,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    "next_open_buy_retry_pending"
                    if plan_item is not None and plan_item.intent == "next_open_buy"
                    else "watch_buy_triggered_blocked"
                ),
            )
        if (
            market_evidence is not None
            and market_evidence.is_domestic
            and plan_item is not None
            and is_watch_buy_intent(plan_item.intent)
        ):
            order.limit_price = routed_command.limit_price
            current_metadata = (
                dict(order.request_metadata)
                if isinstance(order.request_metadata, dict)
                else {}
            )
            order.request_metadata = {
                **current_metadata,
                "time_in_force": routed_command.time_in_force,
                "route": _route_payload(route_decision),
                "final_broker_command": _order_command_payload(routed_command),
            }
            db.add(
                BrokerOrderEvent(
                    broker_order_pk=order.id,
                    event_type="submission_price_cap_bound",
                    event_source=BrokerEventSource.SUBMISSION.value,
                    normalized_status=order.status,
                    message=(
                        "automated watch buy bound to the fresh executable ask "
                        "as an IOC limit"
                    ),
                    request_payload=_redacted_payload(
                        _order_command_payload(routed_command)
                    ),
                    payload=_redacted_payload(
                        {
                            "provider": adapter.provider,
                            "price_cap_source": "final_executable_ask",
                            "ord_dvsn": route_decision.ord_dvsn
                            if route_decision is not None
                            else None,
                        }
                    ),
                    observed_at=final_market_reference_at,
                )
            )
            db.flush()
        final_command_metadata = (
            dict(order.request_metadata)
            if isinstance(order.request_metadata, dict)
            else {}
        )
        if "final_broker_command" in final_command_metadata:
            canonical_scope = final_command_metadata.get("execution_scope")
            final_command_metadata = _merge_order_request_metadata(
                base=final_command_metadata,
                command=routed_command,
                request_source=request_source,
                route_decision=route_decision,
                canonical_execution_scope=(
                    canonical_scope if isinstance(canonical_scope, dict) else None
                ),
            )
            final_command_metadata["final_broker_command"] = (
                _order_command_payload(routed_command)
            )
            final_symbol_exposure_reservation = (
                _automated_kis_symbol_exposure_reservation_payload(
                    adapter=adapter,
                    command=routed_command,
                    plan_item=plan_item,
                    request_source=request_source,
                )
            )
            if (
                _requires_automated_kis_symbol_exposure_boundary(
                    adapter=adapter,
                    command=routed_command,
                    plan_item=plan_item,
                    request_source=request_source,
                )
                and final_symbol_exposure_reservation is None
            ):
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override="buy_capital_reservation",
                    message_override=(
                        "final automated buy price cannot be converted into an "
                        "exact durable cash reservation"
                    ),
                )
            if final_symbol_exposure_reservation is not None:
                final_command_metadata[SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY] = (
                    final_symbol_exposure_reservation
                )
            order.request_metadata = _redacted_payload(final_command_metadata) or {}
            order.limit_price = routed_command.limit_price
            db.add(order)
            db.flush()
            # The final broker command can differ materially from the durable
            # submission intent (for example, a KRX IOC ask cap or a KIS-US
            # executable-bid sell limit). Persist that exact command before the
            # external side-effect boundary so process loss cannot erase the
            # route that the broker actually received.
            db.commit()

            # The commit above releases PostgreSQL transaction advisory locks
            # and all row locks. Reacquire every serialization/authority fence
            # and validate the committed intent before calling the broker.
            (
                symbol_exposure_snapshot,
                symbol_exposure_snapshot_error,
            ) = _load_automated_kis_symbol_exposure_snapshot(
                adapter=adapter,
                access_token=session_state.access_token or "",
                command=routed_command,
                plan_item=plan_item,
                request_source=request_source,
            )
            if _requires_automated_kis_symbol_exposure_boundary(
                adapter=adapter,
                command=routed_command,
                plan_item=plan_item,
                request_source=request_source,
            ):
                opposite_order_snapshot = symbol_exposure_snapshot
                opposite_order_snapshot_error = symbol_exposure_snapshot_error
            else:
                (
                    opposite_order_snapshot,
                    opposite_order_snapshot_error,
                ) = _load_automated_kis_opposite_order_snapshot(
                    adapter=adapter,
                    access_token=session_state.access_token or "",
                    command=routed_command,
                    plan_item=plan_item,
                    request_source=request_source,
                    retry_lineage_proof=rejected_retry_lineage_proof,
                )
            _acquire_cross_process_order_intent_lock(
                settings=settings,
                db=db,
                adapter=adapter,
                command=routed_command,
                execution_scope=prepared_execution_scope,
            )
            control_state = _lock_runtime_control_for_broker_boundary(
                db=db,
                control_state=control_state,
            )
            if plan_item is not None:
                current_plan_state = _current_plan_item_state(
                    db=db,
                    item_id=plan_item.id,
                    lock=True,
                    refresh_authority=True,
                )
                if (
                    current_plan_state is None
                    or current_plan_state[0] != "broker_order_submission_pending"
                ):
                    return _persist_runtime_safety_fence(
                        settings=settings,
                        db=db,
                        order=order,
                        plan_item=plan_item,
                        command=routed_command,
                        control_state=control_state,
                        block_reason_override="execution_plan_item_state_changed",
                        message_override=(
                            "execution plan item changed while the final broker "
                            "command was committed"
                        ),
                        preserve_plan_item_status=True,
                    )
                db.refresh(
                    plan_item,
                    attribute_names=[
                        "status",
                        "execution_plan_id",
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
                        "decision_agent_name",
                        "decision_snapshot_id",
                        "decision_run_id",
                        "source_plan_item_id",
                        "source_market_date",
                    ],
                )
                if (
                    initial_server_owned_command_authority
                    and not _server_owned_plan_command_matches(
                        risk_context=(
                            plan_item.risk_context
                            if isinstance(plan_item.risk_context, dict)
                            else {}
                        ),
                        command=logical_command,
                        request_source=request_source,
                    )
                ):
                    return _persist_runtime_safety_fence(
                        settings=settings,
                        db=db,
                        order=order,
                        plan_item=plan_item,
                        command=routed_command,
                        control_state=control_state,
                        block_reason_override="execution_plan_item_identity_changed",
                        message_override=(
                            "execution plan item order authority changed while "
                            "the final broker command was committed"
                        ),
                        retryable_override=False,
                        permanent_rejection=True,
                    )
                if _plan_item_submission_identity(plan_item) != initial_plan_item_identity:
                    return _persist_runtime_safety_fence(
                        settings=settings,
                        db=db,
                        order=order,
                        plan_item=plan_item,
                        command=routed_command,
                        control_state=control_state,
                        block_reason_override="execution_plan_item_identity_changed",
                        message_override=(
                            "execution plan item identity changed while the final "
                            "broker command was committed"
                        ),
                        retryable_override=False,
                        permanent_rejection=True,
                    )

            # Core revocation locks plan items before their broker orders. Keep
            # that lock order here too so a revoke racing this second phase
            # cannot deadlock with the final intent check.
            locked_order = db.scalar(
                select(BrokerOrder)
                .where(
                    BrokerOrder.mode == settings.runtime_mode,
                    BrokerOrder.idempotency_key == command.idempotency_key,
                )
                .options(selectinload(BrokerOrder.events))
                .with_for_update()
            )
            locked_metadata = (
                locked_order.request_metadata
                if locked_order is not None
                and isinstance(locked_order.request_metadata, dict)
                else {}
            )
            if (
                locked_order is None
                or locked_order.id != order.id
                or locked_order.status
                != NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value
                or locked_order.broker_order_id is not None
                or locked_order.submitted_at is not None
                or locked_metadata.get("final_broker_command")
                != _order_command_payload(routed_command)
            ):
                return PlaceBrokerOrderResult(
                    status_code=409,
                    payload={
                        "duplicate": False,
                        "message": (
                            "durable order intent changed before broker submission"
                        ),
                        "block_reason": "broker_order_intent_state_changed",
                    },
                )
            order = locked_order

            boundary_block_reason = (
                automated_buy_submission_block_reason(control_state)
                if routed_command.side == "buy"
                and _is_new_buy_order(command=routed_command, plan_item=plan_item)
                else automated_order_submission_block_reason(control_state)
            )
            if boundary_block_reason is not None:
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=boundary_block_reason,
                    risk_off_fence=(boundary_block_reason == "hourly_plan_risk_off"),
                )
            account_route_mismatch = _plan_item_account_route_mismatch(
                settings=settings,
                plan_item=plan_item,
                provider=adapter.provider,
            )
            if account_route_mismatch is not None:
                return _persist_account_route_fence(
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    mismatch=account_route_mismatch,
                )
            authority_checked_at = _normalize_utc_datetime(
                now or datetime.now(UTC)
            )
            scoped_authority_reason = (
                core_buy_authority_block_reason(
                    settings=settings,
                    db=db,
                    plan_item=plan_item,
                    now=authority_checked_at,
                    lock=True,
                )
                if routed_command.side == "buy"
                and _is_new_buy_order(command=routed_command, plan_item=plan_item)
                else None
            )
            if scoped_authority_reason is not None:
                if plan_item is not None:
                    project_plan_item_for_core_buy_authority(
                        settings=settings,
                        db=db,
                        plan_item=plan_item,
                        now=authority_checked_at,
                    )
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=scoped_authority_reason,
                    message_override=(
                        "broker submission was fenced because the scoped Core KRX "
                        "buy authority is missing, blocked, or stale"
                    ),
                    preserve_plan_item_status=True,
                )
            execution_account_scope_mismatch = (
                _plan_item_execution_account_scope_mismatch(
                    plan_item=plan_item,
                    provider=adapter.provider,
                    current_binding=current_execution_account_scope,
                )
                if plan_item is not None and adapter.provider != "kis"
                else None
            )
            if execution_account_scope_mismatch is not None:
                return _persist_execution_account_scope_fence(
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    mismatch=execution_account_scope_mismatch,
                )
            sell_stop_integrity_failure = _final_target_sell_stop_integrity_failure(
                db=db,
                command=routed_command,
                plan_item=plan_item,
            )
            if sell_stop_integrity_failure is not None:
                block_reason, message = sell_stop_integrity_failure
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=block_reason,
                    message_override=message,
                    plan_item_status_override=(
                        f"{plan_item.intent}_monitoring"
                        if plan_item is not None
                        else None
                    ),
                )

            final_market_reference_at = (
                snapshot_freshness_reference_at
                + timedelta(seconds=max(0.0, monotonic() - submission_started))
            )
            next_open_window_failure = _automated_next_open_submission_window_failure(
                settings=settings,
                command=routed_command,
                plan_item=plan_item,
                request_source=request_source,
                reference_at=final_market_reference_at,
            )
            if next_open_window_failure is not None:
                block_reason, message = next_open_window_failure
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=block_reason,
                    message_override=message,
                    plan_item_status_override="next_open_buy_expired",
                    retryable_override=False,
                    permanent_rejection=True,
                )
            market_evidence_failure = _automated_kis_buy_market_evidence_failure(
                settings=settings,
                command=routed_command,
                plan_item=plan_item,
                evidence=market_evidence,
                reference_at=final_market_reference_at,
            )
            if market_evidence_failure is not None:
                block_reason, message = market_evidence_failure
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=block_reason,
                    message_override=message,
                    retry_after_at_override=(
                        datetime.now(UTC)
                        + timedelta(
                            seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                        )
                    ),
                    plan_item_status_override=(
                        "next_open_buy_retry_pending"
                        if plan_item is not None and plan_item.intent == "next_open_buy"
                        else "watch_buy_triggered_blocked"
                    ),
                )
            sell_market_evidence_failure = _automated_kis_sell_market_evidence_failure(
                settings=settings,
                command=routed_command,
                plan_item=plan_item,
                evidence=sell_market_evidence,
                reference_at=final_market_reference_at,
            )
            if sell_market_evidence_failure is not None:
                block_reason, message = sell_market_evidence_failure
                return _persist_runtime_safety_fence(
                    settings=settings,
                    db=db,
                    order=order,
                    plan_item=plan_item,
                    command=routed_command,
                    control_state=control_state,
                    block_reason_override=block_reason,
                    message_override=message,
                    retry_after_at_override=(
                        datetime.now(UTC)
                        + timedelta(
                            seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                        )
                    ),
                    plan_item_status_override=(
                        f"{plan_item.intent}_monitoring"
                        if plan_item is not None
                        else None
                    ),
                )
        opposite_order_failure = _automated_kis_opposite_order_boundary_failure(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            retry_lineage_proof=rejected_retry_lineage_proof,
            execution_scope=current_execution_scope,
            snapshot=opposite_order_snapshot,
            snapshot_error=opposite_order_snapshot_error,
            exclude_order_id=order.id,
            observed_at=datetime.now(UTC),
            operation_reference_at=(
                snapshot_freshness_reference_at
                + timedelta(seconds=max(0.0, monotonic() - submission_started))
            ),
        )
        if opposite_order_failure is not None:
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=opposite_order_failure.block_reason,
                message_override=opposite_order_failure.message,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    (
                        "next_open_buy_retry_pending"
                        if plan_item is not None
                        and plan_item.intent == "next_open_buy"
                        else "watch_buy_triggered_blocked"
                    )
                    if routed_command.side == "buy"
                    else (
                        f"{plan_item.intent}_monitoring"
                        if plan_item is not None
                        else None
                    )
                ),
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "opposite_order_boundary": opposite_order_failure.metrics,
                },
            )
        final_family_sell_failure = _evaluate_automated_family_sell_boundary(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            execution_scope=current_execution_scope,
            account_snapshot=opposite_order_snapshot,
            account_snapshot_error=opposite_order_snapshot_error,
            retry_lineage_proof=rejected_retry_lineage_proof,
            exclude_order_id=order.id,
        )
        if final_family_sell_failure is not None:
            manual_review_required = bool(
                final_family_sell_failure.payload.get("manual_review_required")
            )
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=str(
                    final_family_sell_failure.payload.get("block_reason")
                    or "strategy_family_inventory_indeterminate"
                ),
                message_override=str(
                    final_family_sell_failure.payload.get("message")
                    or "strategy family inventory changed before broker submission"
                ),
                retry_after_at_override=(
                    None
                    if manual_review_required
                    else datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    None
                    if manual_review_required
                    else (
                        f"{plan_item.intent}_monitoring"
                        if plan_item is not None
                        else None
                    )
                ),
                preserve_plan_item_status=manual_review_required,
                retryable_override=not manual_review_required,
                permanent_rejection=manual_review_required,
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "manual_review_required": manual_review_required,
                    "strategy_family_inventory": (
                        final_family_sell_failure.payload
                    ),
                },
            )
        final_cash_decision = _evaluate_final_automated_buy_cash_boundary(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            execution_scope=current_execution_scope,
            snapshot=symbol_exposure_snapshot,
            snapshot_error=symbol_exposure_snapshot_error,
            exclude_order_id=order.id,
            observed_at=final_market_reference_at,
        )
        if final_cash_decision is not None and not final_cash_decision.allowed:
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=final_cash_decision.gate_name,
                message_override=final_cash_decision.reason,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    "next_open_buy_retry_pending"
                    if plan_item is not None and plan_item.intent == "next_open_buy"
                    else "watch_buy_triggered_blocked"
                ),
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "buy_capital_reservation": final_cash_decision.payload,
                },
            )

        lane_inventory_failure = _automated_buy_lane_inventory_failure(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            execution_scope=current_execution_scope,
            account_snapshot=symbol_exposure_snapshot,
            account_snapshot_error=symbol_exposure_snapshot_error,
        )
        if lane_inventory_failure is not None:
            manual_review_required = (
                lane_inventory_failure.manual_review_required
            )
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=lane_inventory_failure.block_reason,
                message_override=lane_inventory_failure.message,
                retry_after_at_override=(
                    None
                    if manual_review_required
                    else datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    "manual_review_required"
                    if manual_review_required
                    else (
                        "next_open_buy_retry_pending"
                        if plan_item is not None
                        and plan_item.intent == "next_open_buy"
                        else "watch_buy_triggered_blocked"
                    )
                ),
                retryable_override=not manual_review_required,
                permanent_rejection=manual_review_required,
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "manual_review_required": manual_review_required,
                    "strategy_lane_inventory": lane_inventory_failure.metrics,
                },
            )

        symbol_exposure_failure = _evaluate_automated_kis_buy_symbol_exposure_boundary(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            plan_item=plan_item,
            request_source=request_source,
            execution_scope=current_execution_scope,
            snapshot=symbol_exposure_snapshot,
            snapshot_error=symbol_exposure_snapshot_error,
            exclude_order_id=order.id,
            # This snapshot is fetched synchronously at the final broker
            # boundary, so its freshness must be judged against wall-clock
            # receipt time as well as the operation's monotonic-adjusted
            # reference (paper/replay dispatches can intentionally be dated).
            observed_at=datetime.now(UTC),
            operation_reference_at=(
                snapshot_freshness_reference_at
                + timedelta(seconds=max(0.0, monotonic() - submission_started))
            ),
        )
        if symbol_exposure_failure is not None:
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=symbol_exposure_failure.block_reason,
                message_override=symbol_exposure_failure.message,
                retry_after_at_override=(
                    datetime.now(UTC)
                    + timedelta(
                        seconds=settings.monitoring_watch_buy_retry_cooldown_seconds
                    )
                ),
                plan_item_status_override=(
                    "next_open_buy_retry_pending"
                    if plan_item is not None and plan_item.intent == "next_open_buy"
                    else "watch_buy_triggered_blocked"
                ),
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "symbol_exposure": symbol_exposure_failure.metrics,
                },
            )

        quarantine_block = _kis_us_otc_quarantine_block(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            execution_scope=current_execution_scope,
            lock=True,
        )
        if quarantine_block is not None:
            fenced = _persist_runtime_safety_fence(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                command=routed_command,
                control_state=control_state,
                block_reason_override=str(quarantine_block["block_reason"]),
                message_override=str(quarantine_block["message"]),
            )
            return PlaceBrokerOrderResult(
                status_code=fenced.status_code,
                payload={
                    **fenced.payload,
                    "quarantined_position_count": quarantine_block[
                        "quarantined_position_count"
                    ],
                },
            )
        broker_call_started = True
        placement = adapter.place_order(
            session_state.access_token or "",
            BrokerOrderPlacementRequest(
                idempotency_key=command.idempotency_key,
                execution_plan_item_id=routed_command.execution_plan_item_id,
                symbol=routed_command.symbol,
                side=routed_command.side,
                quantity=routed_command.quantity,
                base_quantity=routed_command.base_quantity,
                quote_notional=routed_command.quote_notional,
                quantity_precision=routed_command.quantity_precision,
                price_precision=routed_command.price_precision,
                provider=routed_command.provider,
                venue=routed_command.venue,
                asset_class=routed_command.asset_class,
                order_type=routed_command.order_type,
                time_in_force=routed_command.time_in_force,
                exchange=routed_command.exchange,
                limit_price=routed_command.limit_price,
                order_division=route_decision.ord_dvsn if route_decision is not None else None,
                route_session=route_decision.session if route_decision is not None else None,
                route_reason=route_decision.reason if route_decision is not None else None,
            ),
        )
    except BrokerAdapterError as exc:
        observed_at = exc.observed_at or datetime.now(UTC)
        event_observed_at = _response_event_observed_at(
            observed_at=observed_at,
            request_observed_at=request_observed_at,
        )
        rejection_acknowledged = _broker_error_has_rejection_acknowledgement(exc)
        outcome_unknown = broker_call_started and not rejection_acknowledged
        if outcome_unknown:
            status = NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value
            classification = "submit_outcome_unknown"
            error_code = "SUBMIT_OUTCOME_UNKNOWN"
            event_type = "submit_outcome_unknown"
            retryable = False
            error_message = (
                "broker submission outcome is unknown; manual broker reconciliation is required"
            )
        else:
            status = exc.normalized_status.value
            classification = exc.classification.value
            error_code = safe_broker_error_code(
                exc.code,
                provider=adapter.provider,
            )
            event_type = "submit_failed"
            retryable = exc.retryable
            error_message = safe_broker_error_message(exc)
        apply_broker_order_status(order=order, status=status)
        order.last_broker_sync_at = observed_at
        if exc.request_metadata is not None:
            pre_error_request_metadata = (
                dict(order.request_metadata)
                if isinstance(order.request_metadata, dict)
                else {}
            )
            sanitized_request_metadata = redact_account_references(
                exc.request_metadata
            )
            error_request_metadata = (
                dict(sanitized_request_metadata)
                if isinstance(sanitized_request_metadata, dict)
                else {}
            )
            merged_error_metadata = _merge_order_request_metadata(
                base={**pre_error_request_metadata, **error_request_metadata},
                command=routed_command,
                request_source=request_source,
                route_decision=route_decision,
                canonical_execution_scope=(
                    pre_error_request_metadata.get("execution_scope")
                    if isinstance(
                        pre_error_request_metadata.get("execution_scope"), dict
                    )
                    else None
                ),
            )
            merged_error_metadata = _preserve_decision_lineage(
                request_metadata=merged_error_metadata,
                existing_order=order,
            )
            order.request_metadata = _redacted_payload(merged_error_metadata) or {}
        order.response_metadata = _redacted_error_payload(exc.response_metadata) or None
        order.last_error_classification = classification
        order.last_error_code = error_code
        order.last_error_message = error_message
        order.last_error_retryable = retryable
        order.raw_broker_payload = _redacted_error_payload(exc.response_payload) or None
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type=event_type,
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=order.status,
                broker_status=error_code,
                error_classification=classification,
                retryable=retryable,
                message=error_message,
                request_payload=_redacted_payload(
                    exc.request_payload
                    if exc.request_payload is not None
                    else _order_command_payload(routed_command)
                ),
                response_payload=_redacted_error_payload(exc.response_payload) or None,
                payload=_redacted_payload(
                    {
                        "provider": adapter.provider,
                        "broker_call_started": broker_call_started,
                        "broker_rejection_acknowledged": rejection_acknowledged,
                        "reported_error_classification": exc.classification.value,
                        "reported_retryable": exc.retryable,
                        "manual_reconciliation_required": outcome_unknown,
                    }
                ),
                observed_at=event_observed_at,
            )
        )
        if plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status=_plan_item_status_for_order_status(order.status),
                source="broker_order_service",
                reason=error_message,
                payload={
                    "broker_order_pk": order.id,
                    "idempotency_key": order.idempotency_key,
                    "broker_status": error_code,
                    "event_type": event_type,
                    "manual_reconciliation_required": outcome_unknown,
                },
            )
        if not outcome_unknown:
            record_actionable_execution_failure(
                settings=settings,
                db=db,
                order=order,
                plan_item=plan_item,
                broker_error_classification=exc.classification.value,
                broker_error_code=error_code,
                broker_error_message=error_message,
                retryable=exc.retryable,
                observed_at=observed_at,
                request_source=request_source,
            )
        db.commit()
        db.refresh(order)
        return PlaceBrokerOrderResult(
            status_code=503 if outcome_unknown else exc.status_code or 503,
            payload={
                "duplicate": False,
                "message": error_message,
                "manual_reconciliation_required": outcome_unknown,
                "order": serialize_broker_order(
                    _load_order_by_idempotency_key(
                        settings=settings,
                        db=db,
                        idempotency_key=command.idempotency_key,
                    )
                    or order,
                    include_events=True,
                ),
            },
        )
    except Exception as exc:
        # Authentication and other pre-submit failures have not crossed the
        # external side-effect boundary. Once place_order starts, however, an
        # arbitrary transport/runtime exception can mean the broker accepted
        # the order while its acknowledgement was lost. Never retry that state
        # automatically or leave it looking like an ordinary pending request.
        observed_at = datetime.now(UTC)
        event_observed_at = _response_event_observed_at(
            observed_at=observed_at,
            request_observed_at=request_observed_at,
        )
        outcome_unknown = broker_call_started
        if outcome_unknown:
            status = NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value
            classification = "submit_outcome_unknown"
            error_code = "SUBMIT_OUTCOME_UNKNOWN"
            event_type = "submit_outcome_unknown"
            error_message = (
                f"{exc.__class__.__name__}: broker submission outcome is unknown; "
                "manual broker reconciliation is required"
            )
        else:
            status = NormalizedBrokerOrderStatus.FAILED.value
            classification = "pre_submit_failure"
            error_code = "PRE_SUBMIT_FAILURE"
            event_type = "submit_failed_before_broker_call"
            error_message = (
                f"{exc.__class__.__name__}: broker submission failed before the "
                "broker order call started"
            )
        apply_broker_order_status(order=order, status=status)
        order.last_error_classification = classification
        order.last_error_code = error_code
        order.last_error_message = error_message
        order.last_error_retryable = False
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type=event_type,
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=order.status,
                error_classification=classification,
                retryable=False,
                message=error_message,
                request_payload=_redacted_payload(
                    _order_command_payload(routed_command)
                ),
                payload=_redacted_payload(
                    {
                        "provider": adapter.provider,
                        "exception_type": exc.__class__.__name__,
                        "broker_call_started": broker_call_started,
                        "manual_reconciliation_required": outcome_unknown,
                    }
                ),
                observed_at=event_observed_at,
            )
        )
        if plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status=_plan_item_status_for_order_status(order.status),
                source="broker_order_service",
                reason=error_message,
                payload={
                    "broker_order_pk": order.id,
                    "idempotency_key": order.idempotency_key,
                    "event_type": event_type,
                    "broker_call_started": broker_call_started,
                    "manual_reconciliation_required": outcome_unknown,
                },
            )
        db.commit()
        db.refresh(order)
        return PlaceBrokerOrderResult(
            status_code=503,
            payload={
                "duplicate": False,
                "message": error_message,
                "manual_reconciliation_required": outcome_unknown,
                "order": serialize_broker_order(
                    _load_order_by_idempotency_key(
                        settings=settings,
                        db=db,
                        idempotency_key=command.idempotency_key,
                    )
                    or order,
                    include_events=True,
                ),
            },
        )

    order.broker_order_id = placement.broker_order_id
    apply_broker_order_status(order=order, status=placement.normalized_status.value)
    order.submitted_at = placement.submitted_at
    order.last_broker_sync_at = placement.observed_at
    pre_ack_request_metadata = (
        dict(order.request_metadata)
        if isinstance(order.request_metadata, dict)
        else {}
    )
    placement_request_metadata = (
        dict(placement.request_metadata)
        if isinstance(placement.request_metadata, dict)
        else {}
    )
    order_request_metadata = _merge_order_request_metadata(
        base={**pre_ack_request_metadata, **placement_request_metadata},
        command=routed_command,
        request_source=request_source,
        route_decision=route_decision,
        canonical_execution_scope=(
            pre_ack_request_metadata.get("execution_scope")
            if isinstance(pre_ack_request_metadata.get("execution_scope"), dict)
            else None
        ),
    )
    order_request_metadata = _preserve_decision_lineage(
        request_metadata=order_request_metadata,
        existing_order=order,
    )
    stable_account_scope = pre_ack_request_metadata.get("broker_account_scope")
    if isinstance(stable_account_scope, dict):
        # Broker acknowledgements may omit or echo arbitrary request metadata.
        # The account scope was derived locally before submission and is an
        # immutable attribution boundary for fills, PnL, and feedback.
        order_request_metadata["broker_account_scope"] = stable_account_scope
    stable_symbol_exposure_reservation = pre_ack_request_metadata.get(
        SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY
    )
    if isinstance(stable_symbol_exposure_reservation, dict):
        # This locally-derived valuation is part of the durable pre-submit
        # reservation. A broker echo must not replace it after acknowledgement.
        order_request_metadata[SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY] = (
            stable_symbol_exposure_reservation
        )
    if account_gate_decision is not None:
        order_request_metadata["account_drawdown_recovery_policy"] = {
            "policy_version": 1,
            "recovery_scope": _account_drawdown_recovery_scope(
                settings=settings,
                provider=adapter.provider,
                asset_class=logical_command.asset_class,
                exchange=logical_command.exchange,
            ),
            "policy_fingerprint": recovery_policy_fingerprint(settings),
        }
    order.request_metadata = _redacted_payload(order_request_metadata) or {}
    response_metadata = _redacted_payload(placement.response_metadata) or {}
    if account_gate_decision is not None:
        response_metadata["account_drawdown_gate"] = account_gate_decision.summary
    order.response_metadata = _redacted_payload(response_metadata) or {}
    order.last_error_classification = None
    order.last_error_code = None
    order.last_error_message = None
    order.last_error_retryable = None
    order.raw_broker_payload = _redacted_payload(placement.response_payload)
    event_observed_at = _response_event_observed_at(
        observed_at=placement.observed_at,
        request_observed_at=request_observed_at,
    )
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submit_acknowledged",
            event_source=BrokerEventSource.SUBMISSION.value,
            normalized_status=order.status,
            broker_status=placement.broker_status,
            message=placement.message,
            request_payload=_redacted_payload(placement.request_payload),
            response_payload=_redacted_payload(placement.response_payload),
            payload=_redacted_payload({"provider": adapter.provider}),
            observed_at=event_observed_at,
        )
    )
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status=_plan_item_status_for_order_status(order.status),
            source="broker_order_service",
            reason=placement.message,
            payload={
                "broker_order_pk": order.id,
                "broker_order_id": placement.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "event_type": "submit_acknowledged",
            },
        )

    _record_upbit_paper_simulated_fill(
        settings=settings,
        db=db,
        order=order,
        command=routed_command,
        plan_item=plan_item,
        observed_at=event_observed_at + timedelta(microseconds=1),
    )

    # Persist the broker acknowledgement before optional post-submit bookkeeping.
    # That bookkeeping can be retried or repaired, while a lost broker order id
    # can otherwise turn a harmless retry into a duplicate external order.
    db.commit()
    db.refresh(order)

    if before_success_commit is not None:
        before_success_commit()
        db.commit()
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=201,
        payload={
            "duplicate": False,
            "message": placement.message or "broker order submitted",
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _broker_error_has_rejection_acknowledgement(exc: BrokerAdapterError) -> bool:
    """Return whether structured broker evidence proves that no order was accepted."""
    response_payload = exc.response_payload if isinstance(exc.response_payload, dict) else {}
    rt_cd = response_payload.get("rt_cd")
    if rt_cd is not None:
        return str(rt_cd).strip() not in {"", "0"}

    if (
        exc.normalized_status == NormalizedBrokerOrderStatus.REJECTED
        and exc.classification
        in {
            BrokerErrorClassification.AUTH,
            BrokerErrorClassification.VALIDATION,
            BrokerErrorClassification.BROKER_REJECTED,
        }
    ):
        return True

    response_metadata = exc.response_metadata if isinstance(exc.response_metadata, dict) else {}
    response_status = response_metadata.get("status_code")
    try:
        response_status_code = int(response_status) if response_status is not None else None
    except (TypeError, ValueError):
        response_status_code = None
    if (
        response_status_code is not None
        and 400 <= response_status_code < 500
        and response_status_code not in {408, 425}
    ):
        return True

    # Some adapters retain the broker's structured rejection body without its
    # transport metadata. A rate-limit code plus a broker message is still an
    # acknowledgement; a bare network/timeout exception is not.
    response_code = response_payload.get("msg_cd") or response_payload.get("code")
    response_message = (
        response_payload.get("msg1")
        or response_payload.get("message")
        or response_payload.get("detail")
    )
    return (
        exc.classification == BrokerErrorClassification.RATE_LIMIT
        and bool(str(response_code or "").strip())
        and bool(str(response_message or "").strip())
    )


def _plan_item_account_route_mismatch(
    *,
    settings: Settings,
    plan_item: ExecutionPlanItem | None,
    provider: str | None,
) -> AccountRouteMismatch | None:
    if plan_item is None:
        return None
    return validate_kis_account_route(
        settings=settings,
        provider=provider,
        risk_context=plan_item.risk_context,
        quantity_policy=plan_item.quantity_policy,
        require_binding=True,
    )


def _plan_item_execution_account_scope_mismatch(
    *,
    plan_item: ExecutionPlanItem,
    provider: str,
    current_binding: object,
) -> ExecutionAccountScopeMismatch | None:
    return validate_execution_account_scope(
        provider=provider,
        risk_context=plan_item.risk_context,
        quantity_policy=plan_item.quantity_policy,
        current_binding=current_binding,
        require_binding=True,
    )


def _load_authenticated_broker_account_id(
    *,
    adapter: BrokerAdapter,
    access_token: str,
) -> tuple[bool, str | None]:
    """Load Alpaca's authenticated account id without leaking it downstream.

    Providers without an account endpoint use their startup-validated opaque
    configured scope instead. A failed Alpaca lookup is represented as
    unavailable so every plan-linked order can be quarantined before
    ``place_order`` instead of mutating an unproven account.
    """

    if str(adapter.provider or "").strip().lower() != "alpaca":
        return True, None
    account_id_loader = getattr(adapter, "get_broker_account_id", None)
    if not callable(account_id_loader):
        return False, None
    try:
        loaded_account_id = account_id_loader(access_token)
    except Exception:
        return False, None
    broker_account_id = (
        loaded_account_id.strip()
        if isinstance(loaded_account_id, str) and loaded_account_id.strip()
        else None
    )
    return broker_account_id is not None, broker_account_id


def _current_execution_account_scope_binding(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    broker_account_id: str | None,
    broker_account_id_resolved: bool,
) -> dict[str, object] | None:
    provider = str(adapter.provider or "").strip().lower()
    if provider == "kis" or not provider:
        return None
    if provider == "alpaca" and (
        not broker_account_id_resolved or broker_account_id is None
    ):
        return None
    try:
        return build_execution_account_scope_binding(
            settings=settings,
            provider=provider,
            exchange=command.exchange,
            broker_account_id=broker_account_id,
        )
    except PositionIdentityConflictError:
        return None


def _plan_item_submission_identity(
    plan_item: ExecutionPlanItem | None,
) -> tuple[object, ...] | None:
    if plan_item is None:
        return None
    risk_context = (
        plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
    )
    exchange = risk_context.get("exchange")
    return (
        plan_item.provider,
        plan_item.asset_class,
        plan_item.venue,
        plan_item.symbol,
        plan_item.side,
        plan_item.intent,
        exchange,
        plan_item.limit_price,
        plan_item.trigger_price,
        plan_item.time_in_force,
        plan_item.base_quantity,
        plan_item.quote_notional,
        plan_item.quantity_precision,
        plan_item.price_precision,
        json_payload_hash(plan_item.quantity_policy),
        json_payload_hash(
            {
                key: risk_context[key]
                for key in PLAN_ITEM_SUBMISSION_RISK_KEYS
                if key in risk_context
            }
        ),
        json_payload_hash(decision_lineage_for_plan_item(plan_item)),
    )


def _account_route_blocked_result(
    mismatch: AccountRouteMismatch,
) -> PlaceBrokerOrderResult:
    return PlaceBrokerOrderResult(
        status_code=422,
        payload={
            "duplicate": False,
            "message": mismatch.reason_code,
            "block_reason": mismatch.reason_code,
            "permanent_rejection": True,
            "provided_route_fingerprint": mismatch.provided_route_fingerprint,
        },
    )


def _persist_account_route_fence(
    *,
    db: Session,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
    mismatch: AccountRouteMismatch,
) -> PlaceBrokerOrderResult:
    observed_at = datetime.now(UTC)
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
    )
    order.last_error_classification = "validation"
    order.last_error_code = mismatch.reason_code
    order.last_error_message = mismatch.reason_code
    order.last_error_retryable = False
    order.response_metadata = {
        "account_route_fence": {
            "reason_code": mismatch.reason_code,
            "provided_route_fingerprint": mismatch.provided_route_fingerprint,
        }
    }
    order.raw_broker_payload = None
    request_metadata = dict(order.request_metadata or {})
    request_metadata["account_route_fence"] = {
        "reason_code": mismatch.reason_code,
        "provided_route_fingerprint": mismatch.provided_route_fingerprint,
        "observed_at": observed_at.isoformat(),
        "broker_call_started": False,
    }
    order.request_metadata = request_metadata
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submission_blocked",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED.value,
            broker_status=mismatch.reason_code,
            error_classification="validation",
            retryable=False,
            message=mismatch.reason_code,
            request_payload=_order_command_payload(command),
            payload={
                "block_reason": mismatch.reason_code,
                "provided_route_fingerprint": mismatch.provided_route_fingerprint,
                "broker_call_started": False,
            },
            observed_at=observed_at,
        )
    )
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="broker_order_rejected",
            source="broker_order_service",
            reason=mismatch.reason_code,
            payload={
                "broker_order_pk": order.id,
                "idempotency_key": order.idempotency_key,
                "event_type": "submission_blocked",
                "block_reason": mismatch.reason_code,
                "provided_route_fingerprint": mismatch.provided_route_fingerprint,
            },
        )
    db.commit()
    db.refresh(order)
    return PlaceBrokerOrderResult(
        status_code=422,
        payload={
            "duplicate": False,
            "message": mismatch.reason_code,
            "block_reason": mismatch.reason_code,
            "permanent_rejection": True,
            "order": serialize_broker_order(order, include_events=True),
        },
    )


def _persist_execution_account_scope_fence(
    *,
    db: Session,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem,
    command: PlaceOrderRequest,
    mismatch: ExecutionAccountScopeMismatch,
) -> PlaceBrokerOrderResult:
    observed_at = datetime.now(UTC)
    reason_code = mismatch.reason_code
    message = (
        "non-KIS buy account authority changed or is unavailable; "
        "operator review is required"
    )
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
    )
    order.last_broker_sync_at = observed_at
    order.last_error_classification = "account_authority"
    order.last_error_code = reason_code
    order.last_error_message = message
    order.last_error_retryable = False
    fence = {
        "policy_version": 1,
        "reason_code": reason_code,
        "broker_call_started": False,
    }
    metadata = dict(order.request_metadata or {})
    metadata["account_authority_fence"] = fence
    order.request_metadata = metadata
    order.response_metadata = {"account_authority_fence": fence}
    order.raw_broker_payload = None
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submission_account_scope_quarantined",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
            broker_status=reason_code,
            error_classification="account_authority",
            retryable=False,
            message=message,
            request_payload=_redacted_payload(_order_command_payload(command)),
            payload=dict(fence),
            observed_at=observed_at,
        )
    )
    set_plan_item_status(
        db=db,
        plan_item=plan_item,
        status="manual_review_required",
        source="broker_order_service",
        reason=message,
        payload={
            "broker_order_pk": order.id,
            "idempotency_key": order.idempotency_key,
            "event_type": "submission_account_scope_quarantined",
            "block_reason": reason_code,
            "broker_call_started": False,
        },
    )
    db.commit()
    db.refresh(order)
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": message,
            "block_reason": reason_code,
            "manual_review_required": True,
            "permanent_rejection": True,
            "order": serialize_broker_order(order, include_events=True),
        },
    )


def _persist_runtime_safety_fence(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
    control_state: RuntimeControlState,
    block_reason_override: str | None = None,
    risk_off_fence: bool = False,
    message_override: str | None = None,
    retry_after_at_override: datetime | None = None,
    plan_item_status_override: str | None = None,
    preserve_plan_item_status: bool = False,
    retryable_override: bool = True,
    permanent_rejection: bool = False,
) -> PlaceBrokerOrderResult:
    observed_at = datetime.now(UTC)
    block_reason = (
        block_reason_override
        or automated_order_submission_block_reason(control_state)
        or "runtime_control"
    )
    message = message_override or (
        "broker submission was fenced before the broker call because runtime safety controls "
        f"changed to {block_reason}"
    )
    retry_after_at = retry_after_at_override
    if retry_after_at is None and risk_off_fence:
        retry_after_at = hourly_risk_off_retry_after_at(
            settings=settings,
            state=control_state,
            now=observed_at,
        )
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
    )
    order.last_error_classification = RUNTIME_SAFETY_CONTROL_CLASSIFICATION
    order.last_error_code = block_reason
    order.last_error_message = message
    order.last_error_retryable = retryable_override
    order.response_metadata = {"runtime_safety_fence": runtime_safety_payload(control_state)}
    order.raw_broker_payload = None
    request_metadata = dict(order.request_metadata or {})
    request_metadata["runtime_safety_fence"] = {
        "block_reason": block_reason,
        "observed_at": observed_at.isoformat(),
        "broker_call_started": False,
    }
    order.request_metadata = request_metadata
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submission_blocked",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED.value,
            broker_status=block_reason,
            error_classification=RUNTIME_SAFETY_CONTROL_CLASSIFICATION,
            retryable=retryable_override,
            message=message,
            request_payload=_order_command_payload(command),
            payload={
                "block_reason": block_reason,
                "broker_call_started": False,
                "safety": runtime_safety_payload(control_state),
            },
            observed_at=observed_at,
        )
    )
    if plan_item is not None and not preserve_plan_item_status:
        if risk_off_fence:
            db.refresh(plan_item, attribute_names=["status", "risk_context"])
            fence_context = (
                control_state.hourly_risk_off_context
                if isinstance(control_state.hourly_risk_off_context, dict)
                else {}
            )
            original_status = request_metadata.get(
                "plan_item_status_before_submission"
            )
            if not isinstance(original_status, str) or not original_status:
                original_status = (
                    "next_open_buy_retry_pending"
                    if plan_item.intent == "next_open_buy"
                    else "watch_monitoring_active"
                )
            projected = project_buy_plan_item_for_hourly_risk_off(
                db=db,
                settings=settings,
                item=plan_item,
                risk_flags=int(
                    fence_context.get("activation_risk_flags")
                    or fence_context.get("last_risk_flags")
                    or 3
                ),
                risk_reasons=[
                    str(reason)
                    for reason in (
                        fence_context.get("activation_risk_reasons")
                        or fence_context.get("last_risk_reasons")
                        or ["active hourly risk-off fence"]
                    )
                ],
                now=observed_at,
                resume_status=original_status,
                fence_context=fence_context,
            )
            if (
                not projected
                and not _plan_item_has_active_hourly_risk_off(plan_item)
            ):
                set_plan_item_status(
                    db=db,
                    plan_item=plan_item,
                    status="broker_order_rejected",
                    source="broker_order_service",
                    reason=message,
                    payload={
                        "broker_order_pk": order.id,
                        "idempotency_key": order.idempotency_key,
                        "block_reason": block_reason,
                        "broker_call_started": False,
                    },
                )
        else:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status=plan_item_status_override or "broker_order_rejected",
                source="broker_order_service",
                reason=message,
                payload={
                    "broker_order_pk": order.id,
                    "idempotency_key": order.idempotency_key,
                    "block_reason": block_reason,
                    "broker_call_started": False,
                },
            )
    db.commit()
    db.expire(order, ["events"])
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=order.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "blocked": True,
            "message": message,
            "block_reason": block_reason,
            "permanent_rejection": permanent_rejection,
            "retry_after_at": (
                retry_after_at.isoformat() if retry_after_at is not None else None
            ),
            "safety": runtime_safety_payload(control_state),
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _automated_next_open_submission_window_failure(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    reference_at: datetime,
) -> tuple[str, str] | None:
    if settings.runtime_mode != "live":
        return None
    if (
        plan_item is None
        or plan_item.intent != "next_open_buy"
        or command.side != "buy"
        or not _is_new_buy_order(command=command, plan_item=plan_item)
        or not request_source.startswith(ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES)
    ):
        return None
    market_date = plan_item.plan.market_date if plan_item.plan is not None else None
    if market_date is None:
        return (
            "missed_next_open_submission_window",
            "automated next-open buy requires an explicit current market date",
        )
    asset_class = str(command.asset_class or plan_item.asset_class or "stock").strip().lower()
    if asset_class == "us_equity":
        market_timezone = ZoneInfo("America/New_York")
        local_open = datetime.combine(
            market_date,
            datetime.min.time().replace(hour=9, minute=30),
            tzinfo=market_timezone,
        ).astimezone(UTC)
    elif asset_class == "stock":
        market_timezone = ZoneInfo(settings.market_timezone)
        local_open = datetime.combine(
            market_date,
            datetime.min.time().replace(hour=9),
            tzinfo=market_timezone,
        ).astimezone(UTC)
    else:
        return (
            "missed_next_open_submission_window",
            "automated next-open buy has no supported market-open clock",
        )
    evaluated_at = _normalize_utc_datetime(reference_at)
    deadline = local_open + timedelta(
        seconds=settings.next_open_buy_initial_submission_grace_seconds
    )
    if (
        evaluated_at.astimezone(market_timezone).date() != market_date
        or evaluated_at < local_open
        or evaluated_at > deadline
    ):
        return (
            "missed_next_open_submission_window",
            "automated next-open buy crossed its final submission grace window",
        )
    return None


def _automated_provider_buy_capability_failure(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> tuple[str, str] | None:
    if settings.runtime_mode != "live":
        return None
    if (
        command.provider != "alpaca"
        or command.asset_class != "us_equity"
        or command.side != "buy"
        or not _is_new_buy_order(command=command, plan_item=plan_item)
        or not request_source.startswith(ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES)
    ):
        return None
    return (
        "live_automated_alpaca_buy_quote_unsupported",
        "automated Alpaca US-equity buy requires a trusted executable quote source",
    )


def _lock_runtime_control_for_broker_boundary(
    *,
    db: Session,
    control_state: RuntimeControlState,
) -> RuntimeControlState:
    if db.get_bind().dialect.name == "sqlite":
        # SQLite ignores FOR UPDATE. This no-op write obtains its single-writer
        # reservation and keeps a concurrent risk transition ordered behind the
        # broker call (or vice versa).
        db.execute(
            update(RuntimeControlState)
            .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
            .values(hourly_risk_off_epoch=RuntimeControlState.hourly_risk_off_epoch)
        )
    locked = db.scalar(
        select(RuntimeControlState)
        .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
        .with_for_update()
    )
    if locked is None:
        raise RuntimeError("runtime control state disappeared before broker submission")
    db.refresh(
        locked,
        attribute_names=[
            "paused",
            "kill_switch_enabled",
            "hourly_risk_off_active",
            "hourly_risk_off_epoch",
            "hourly_risk_off_context",
        ],
    )
    return locked


def _load_order_by_idempotency_key(
    *,
    settings: Settings,
    db: Session,
    idempotency_key: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == idempotency_key,
        )
        .options(selectinload(BrokerOrder.events))
    )


def _runtime_safety_fenced_order_matches_command(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    command: PlaceOrderRequest,
) -> bool:
    """Allow a known pre-broker safety fence to resume under the same key.

    The row is reusable only because the runtime fence is recorded before
    ``adapter.place_order`` starts. Economic fields and durable approval
    lineage must still match, while monitoring timestamps/prices are allowed
    to change on the later fresh trigger. The persisted command is replayed
    after this check so volatile metadata can never mutate the fenced order.
    """

    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    original_command = metadata.get("original_command")
    account_scope_policy = metadata.get("broker_account_scope")
    account_scope_matches = adapter.provider != "kis" or bool(
        isinstance(account_scope_policy, dict)
        and account_scope_policy.get("policy_version") == 1
        and account_scope_policy.get("scope")
        == _account_drawdown_recovery_scope(
            settings=settings,
            provider=adapter.provider,
            asset_class=command.asset_class,
            exchange=command.exchange,
        )
    )
    return bool(
        order.status == NormalizedBrokerOrderStatus.REJECTED.value
        and order.last_error_classification == RUNTIME_SAFETY_CONTROL_CLASSIFICATION
        and order.last_error_retryable is True
        and order.broker_order_id is None
        and order.submitted_at is None
        and order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and account_scope_matches
        and isinstance(original_command, dict)
        and _runtime_safety_command_identity(original_command)
        == _runtime_safety_command_identity(_order_command_payload(command))
    )


def _retryable_pre_submit_failure_matches_command(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    """Retry only an exact order proven not to have crossed the broker boundary."""

    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    original_command = metadata.get("original_command")
    stored_scope = metadata.get("execution_scope")
    stored_route = _execution_scope_route(stored_scope)
    expected_venue = str(command.venue or adapter.provider).strip().upper()
    route_matches = bool(
        stored_route is not None
        and stored_route.provider == str(adapter.provider or "").strip().lower()
        and stored_route.venue == expected_venue
        and stored_route.asset_class == str(command.asset_class or "").strip().lower()
    )
    events = list(order.events or [])
    event_proof_matches = bool(events)
    for event in events:
        event_payload = event.payload if isinstance(event.payload, dict) else {}
        event_request = (
            event.request_payload
            if isinstance(event.request_payload, dict)
            else None
        )
        if (
            event.event_type != "submit_failed_before_broker_call"
            or event.normalized_status
            != NormalizedBrokerOrderStatus.RETRY_PENDING.value
            or event.retryable is not True
            or event_payload.get("broker_call_started") is not False
            or event_payload.get("manual_reconciliation_required") is not False
            or not isinstance(original_command, dict)
            or not _pre_submit_routed_request_matches_original(
                adapter=adapter,
                routed_request=event_request,
                original_request=original_command,
            )
            or not _pre_submit_event_scope_matches(
                settings=settings,
                adapter=adapter,
                routed_request=event_request,
                stored_scope=stored_scope,
            )
        ):
            event_proof_matches = False
            break
    return bool(
        order.status == NormalizedBrokerOrderStatus.RETRY_PENDING.value
        and order.last_error_retryable is True
        and order.broker_order_id is None
        and order.submitted_at is None
        and order.last_broker_sync_at is None
        and order.response_metadata is None
        and order.raw_broker_payload is None
        and order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and order.execution_plan_item_id == command.execution_plan_item_id
        and metadata.get("source") == request_source
        and isinstance(original_command, dict)
        and _pre_submit_request_with_default_venue(
            adapter=adapter,
            request=original_command,
        )
        == _pre_submit_request_with_default_venue(
            adapter=adapter,
            request=_order_command_payload(command),
        )
        and route_matches
        and event_proof_matches
    )


def _pre_submit_request_with_default_venue(
    *,
    adapter: BrokerAdapter,
    request: dict[str, object],
) -> dict[str, object]:
    canonical = dict(request)
    canonical["venue"] = str(
        canonical.get("venue") or adapter.provider
    ).strip().lower()
    return canonical


def _pre_submit_routed_request_matches_original(
    *,
    adapter: BrokerAdapter,
    routed_request: dict[str, object] | None,
    original_request: dict[str, object],
) -> bool:
    if not isinstance(routed_request, dict):
        return False
    if _pre_submit_request_with_default_venue(
        adapter=adapter,
        request=routed_request,
    ) == _pre_submit_request_with_default_venue(
        adapter=adapter,
        request=original_request,
    ):
        return True
    try:
        original = PlaceOrderRequest.model_validate(original_request)
    except ValueError:
        return False
    routed = _parse_pre_submit_routed_request(routed_request)
    if routed is None:
        return False
    original = original.model_copy(
        update={"venue": original.venue or adapter.provider}
    )
    routed = routed.model_copy(
        update={"venue": routed.venue or adapter.provider}
    )
    if (
        str(adapter.provider or "").strip().lower() != "kis"
        or original.provider != routed.provider
        or original.provider != "kis"
        or original.asset_class != routed.asset_class
        or original.asset_class not in {"stock", "kr_equity", "domestic_equity"}
        or original.exchange not in KRX_EXCHANGES
        or routed.exchange not in KRX_EXCHANGES
    ):
        return False
    invariant_fields = (
        "idempotency_key",
        "execution_plan_item_id",
        "provider",
        "venue",
        "asset_class",
        "symbol",
        "side",
        "quantity",
        "base_quantity",
        "quote_notional",
        "quantity_precision",
        "price_precision",
        "limit_price",
        "time_in_force",
    )
    if any(
        getattr(original, field) != getattr(routed, field)
        for field in invariant_fields
    ):
        return False
    original_metadata = dict(original.metadata or {})
    routed_metadata = dict(routed.metadata or {})
    route = routed_metadata.pop("route", None)
    if routed_metadata != original_metadata or not isinstance(route, dict):
        return False
    session = str(route.get("session") or "").strip()
    reason = str(route.get("reason") or "").strip()
    effective_order_type = str(route.get("effective_order_type") or "").strip()
    regular_division = "01" if routed.order_type == "market" else "00"
    regular_price_source = (
        "market" if routed.order_type == "market" else "explicit_limit"
    )
    permitted_route_shapes: dict[str, tuple[set[str], set[str], str, set[str]]] = {
        "krx_regular": (
            {"KRX", "SOR"},
            {"krx_regular_session"},
            regular_division,
            {regular_price_source},
        ),
        "nxt_main": (
            {"NXT", "SOR"},
            {"nxt_main_preferred"},
            regular_division,
            {regular_price_source},
        ),
        "krx_opening_call_auction": (
            {"KRX"},
            {"krx_opening_call_auction"},
            "00",
            {"explicit_limit"},
        ),
        "nxt_pre": (
            {"NXT"},
            {"nxt_pre_session", "nxt_pre_preferred"},
            "00",
            {"explicit_limit"},
        ),
        "krx_preopen_close": (
            {"KRX"},
            {"krx_preopen_close_enabled"},
            "05",
            {"closing_price_session"},
        ),
        "nxt_after": (
            {"NXT"},
            {"nxt_after_session", "nxt_after_preferred_for_nxt_symbol"},
            "00",
            {"explicit_limit"},
        ),
        "krx_after_close": (
            {"KRX"},
            {"krx_after_hours_closing_enabled"},
            "06",
            {"closing_price_session"},
        ),
        "krx_after_single_price": (
            {"KRX"},
            {"krx_after_hours_single_price_enabled"},
            "07",
            {"single_price_session"},
        ),
    }
    route_shape = permitted_route_shapes.get(session)
    if route_shape is None:
        return False
    exchanges, reasons, order_division, price_sources = route_shape
    if original.order_type != routed.order_type and not (
        original.order_type == "market"
        and routed.order_type == "limit"
        and session
        in {
            "krx_preopen_close",
            "krx_after_close",
            "krx_after_single_price",
        }
    ):
        return False
    return bool(
        route.get("action") == "submit"
        and route.get("exchange") == routed.exchange
        and routed.exchange in exchanges
        and effective_order_type == routed.order_type
        and route.get("ord_dvsn") == order_division
        and route.get("price_source") in price_sources
        and route.get("next_retry_at") is None
        and reason in reasons
    )


def _parse_pre_submit_routed_request(
    routed_request: dict[str, object],
) -> PlaceOrderRequest | None:
    try:
        return PlaceOrderRequest.model_validate(routed_request)
    except ValueError:
        # KRX closing-price and single-price sessions encode a logical market
        # request as a broker-side limit command without an explicit price.
        # ``model_copy`` intentionally creates that server-owned wire shape,
        # which cannot be parsed as a public limit request. Validate every
        # caller-owned field through the public market schema before restoring
        # only this narrowly permitted server-owned effective order type.
        metadata = routed_request.get("metadata")
        route = metadata.get("route") if isinstance(metadata, dict) else None
        if not (
            routed_request.get("order_type") == "limit"
            and routed_request.get("limit_price") is None
            and isinstance(route, dict)
            and route.get("effective_order_type") == "limit"
            and route.get("session")
            in {
                "krx_preopen_close",
                "krx_after_close",
                "krx_after_single_price",
            }
        ):
            return None
        projected_request = dict(routed_request)
        projected_request["order_type"] = "market"
        try:
            return PlaceOrderRequest.model_validate(projected_request).model_copy(
                update={"order_type": "limit"}
            )
        except ValueError:
            return None


def _pre_submit_event_scope_matches(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    routed_request: dict[str, object] | None,
    stored_scope: object,
) -> bool:
    if not isinstance(routed_request, dict) or not isinstance(stored_scope, dict):
        return False
    stored_route = _execution_scope_route(stored_scope)
    if stored_route is None:
        return False
    routed = _parse_pre_submit_routed_request(routed_request)
    if routed is None:
        return False
    if str(adapter.provider or "").strip().lower() == "alpaca":
        return bool(
            stored_route.provider == routed.provider
            and stored_route.venue
            == str(routed.venue or adapter.provider).strip().upper()
            and stored_route.asset_class == routed.asset_class
            and stored_route.exchange == routed.exchange
        )
    try:
        current_scope = _canonical_execution_scope(
            settings=settings,
            adapter=adapter,
            access_token="",
            command=routed.model_copy(
                update={"venue": routed.venue or adapter.provider}
            ),
            broker_account_id_resolved=True,
        )
    except BrokerAdapterError:
        return False
    return bool(
        current_scope.get("provider") == stored_scope.get("provider")
        and current_scope.get("account_scope") == stored_scope.get("account_scope")
        and current_scope.get("venue")
        == str(routed.venue or adapter.provider).strip().upper()
        and current_scope.get("asset_class") == routed.asset_class
        and current_scope.get("exchange") == routed.exchange
    )


def _plan_status_allows_automated_retry(
    *,
    status: str,
    retryable_safety_fenced_order: BrokerOrder | None,
) -> bool:
    """Never let a reusable pre-call fence revive a terminal plan decision."""

    if status not in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES:
        return True
    return bool(
        status == "broker_order_rejected"
        and retryable_safety_fenced_order is not None
    )


def _runtime_safety_command_identity(payload: dict[str, object]) -> dict[str, object]:
    volatile_retrigger_fields = {
        "base_quantity",
        "limit_price",
        "price_precision",
        "quantity",
        "quantity_precision",
        "quote_notional",
    }
    allow_resized_retrigger = str(payload.get("idempotency_key") or "").startswith(
        "monitoring:watch_buy:"
    )
    identity = {
        key: value
        for key, value in payload.items()
        if key != "metadata"
        and not (allow_resized_retrigger and key in volatile_retrigger_fields)
    }
    raw_metadata = payload.get("metadata")
    metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
    stable_metadata_keys = (
        "account_drawdown_recovery",
        "monitoring_worker",
        "order_purpose",
        "paper",
        "retry",
        "simulated",
    )
    identity["metadata"] = {
        key: metadata[key] for key in stable_metadata_keys if key in metadata
    }
    return identity


def _runtime_safety_order_allows_resized_retrigger(order: BrokerOrder) -> bool:
    return bool(
        order.execution_plan_item_id
        and order.side == "buy"
        and order.idempotency_key.startswith("monitoring:watch_buy:")
    )


def _runtime_safety_replay_command(order: BrokerOrder) -> PlaceOrderRequest | None:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    original_command = metadata.get("original_command")
    if not isinstance(original_command, dict):
        return None
    try:
        return PlaceOrderRequest.model_validate(original_command)
    except ValueError:
        return None


def _fresh_monitoring_sell_retrigger_is_authorized(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    if (
        plan_item is None
        or command.side != "sell"
        or plan_item.intent not in {"stop_sell", "target_sell"}
        or command.execution_plan_item_id != plan_item.id
    ):
        return False
    risk_context = (
        plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
    )
    return _server_owned_plan_command_matches(
        risk_context=risk_context,
        command=command,
        request_source=request_source,
    )


def _account_drawdown_recovery_source_matches_command(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    response_metadata = (
        order.response_metadata if isinstance(order.response_metadata, dict) else {}
    )
    gate_summary = response_metadata.get("account_drawdown_gate")
    gate_name = (
        order.last_error_code
        or (gate_summary.get("gate_name") if isinstance(gate_summary, dict) else None)
    )
    request_metadata = (
        order.request_metadata if isinstance(order.request_metadata, dict) else {}
    )
    recovery_policy = request_metadata.get("account_drawdown_recovery_policy")
    policy_is_current = (
        isinstance(recovery_policy, dict)
        and recovery_policy.get("policy_version") == 1
        and recovery_policy.get("recovery_scope")
        == _account_drawdown_recovery_scope(
            settings=settings,
            provider=adapter.provider,
            asset_class=command.asset_class,
            exchange=command.exchange,
        )
    )
    if not policy_is_current:
        # Gate disablement may bypass health thresholds only after the source
        # proves it belongs to this exact account lane. Legacy/unscoped and
        # foreign-policy rows cannot prove that identity and remain frozen.
        return False
    source_shape_matches = bool(
        order.status == NormalizedBrokerOrderStatus.REJECTED.value
        and order.last_error_classification == "validation"
        and gate_name in RECOVERABLE_LOSS_GATE_NAMES
        and order.broker_order_id is None
        and order.submitted_at is None
        and order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and order.idempotency_key == command.idempotency_key
        and order.symbol == command.symbol
        and order.side == command.side
    )
    if not source_shape_matches:
        return False
    if order.execution_plan_item_id is not None:
        return bool(
            request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and order.execution_plan_item_id == command.execution_plan_item_id
        )
    original_command = request_metadata.get("original_command")
    return bool(
        request_source == "post:/orders"
        and command.execution_plan_item_id is None
        and isinstance(original_command, dict)
        # An unlinked API caller may choose an idempotency key that resembles
        # an internal monitoring key. Direct recovery has no trusted plan
        # lineage, so every normalized command field must remain identical;
        # the monitoring-only resized-retrigger identity is unsafe here.
        and original_command == _order_command_payload(command)
    )


def _account_drawdown_recovery_preflight_source_matches(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    recovery_scope: str,
    idempotency_key: str,
    execution_plan_item_id: str,
    symbol: str,
) -> bool:
    request_metadata = (
        order.request_metadata if isinstance(order.request_metadata, dict) else {}
    )
    policy = request_metadata.get("account_drawdown_recovery_policy")
    return bool(
        isinstance(policy, dict)
        and policy.get("policy_version") == 1
        and policy.get("recovery_scope") == recovery_scope
        and order.status == NormalizedBrokerOrderStatus.REJECTED.value
        and order.last_error_classification == "validation"
        and order.last_error_code in RECOVERABLE_LOSS_GATE_NAMES
        and order.broker_order_id is None
        and order.submitted_at is None
        and order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and order.idempotency_key == idempotency_key
        and order.execution_plan_item_id == execution_plan_item_id
        and order.symbol == symbol
        and order.side == "buy"
    )


def _load_active_account_drawdown_recovery_source(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> BrokerOrder | None:
    if command.side != "buy" or adapter.provider != "kis" or command.provider != "kis":
        return None
    recovery_scope = _account_drawdown_recovery_scope(
        settings=settings,
        provider=adapter.provider,
        asset_class=command.asset_class,
        exchange=command.exchange,
    )
    return _load_active_account_drawdown_recovery_source_for_scope(
        settings=settings,
        db=db,
        provider=adapter.provider,
        recovery_scope=recovery_scope,
    )


def _load_active_account_drawdown_recovery_source_for_scope(
    *,
    settings: Settings,
    db: Session,
    provider: str,
    recovery_scope: str,
) -> BrokerOrder | None:
    scope_expression = BrokerOrder.request_metadata[
        "account_drawdown_recovery_policy"
    ]["recovery_scope"].as_string()
    currency_scope = recovery_scope.rsplit(":", 1)[-1]
    scope_predicate = scope_expression == recovery_scope
    if not (
        str(settings.account_drawdown_recovery_scope_secret or "").strip()
        or str(settings.execution_position_account_scope or "").strip()
    ):
        # Without either stable authority, a broker-credential rotation changes
        # the opaque account fingerprint. Conservatively keep a same-currency
        # orphan latch closed. A secret or explicit opaque route label makes
        # account identity deterministic, so another scope would be a false block.
        scope_predicate = or_(
            scope_predicate,
            scope_expression.like(f"{provider}:%:{currency_scope}"),
        )
    candidates = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == provider,
            BrokerOrder.side == "buy",
            BrokerOrder.status == NormalizedBrokerOrderStatus.REJECTED.value,
            BrokerOrder.last_error_classification == "validation",
            BrokerOrder.last_error_code.in_(tuple(RECOVERABLE_LOSS_GATE_NAMES)),
            BrokerOrder.broker_order_id.is_(None),
            BrokerOrder.submitted_at.is_(None),
            BrokerOrder.request_metadata["account_drawdown_recovery_policy"][
                "policy_version"
            ].as_integer()
            == 1,
            scope_predicate,
        )
        .options(
            undefer(BrokerOrder.request_metadata),
            undefer(BrokerOrder.response_metadata),
            selectinload(BrokerOrder.events),
        )
        .order_by(
            case((scope_expression == recovery_scope, 0), else_=1),
            BrokerOrder.last_broker_sync_at.desc(),
            BrokerOrder.created_at.desc(),
            BrokerOrder.id.desc(),
        )
    ).all()
    for source_order in candidates:
        state = recovery_state(
            db=db,
            execution_plan_item_id=source_order.execution_plan_item_id,
            source_broker_order_pk=source_order.id,
        )
        if recovery_state_is_current_release(
            settings=settings,
            state=state,
            source_broker_order_pk=source_order.id,
            source_idempotency_key=source_order.idempotency_key,
            recovery_generation=1,
        ):
            continue
        return source_order
    return None


def _account_drawdown_recovery_order_scope(order: BrokerOrder) -> str | None:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    policy = metadata.get("account_drawdown_recovery_policy")
    if not isinstance(policy, dict):
        return None
    scope = policy.get("recovery_scope")
    return str(scope) if isinstance(scope, str) and scope else None


def _account_drawdown_recovery_scope(
    *,
    settings: Settings,
    provider: str,
    asset_class: str,
    exchange: str,
) -> str:
    currency_scope = (
        "usd"
        if asset_class == "us_equity" or exchange in {"NASD", "NYSE", "AMEX", "US"}
        else "krw"
    )
    account_fingerprint = recovery_account_fingerprint(settings)
    return f"{provider}:{account_fingerprint}:{currency_scope}"


def _account_drawdown_recovery_blocked_result(
    *,
    source_order: BrokerOrder,
    block_reason: str,
    message: str,
    retry_after_at: datetime | None = None,
    decision: AccountDrawdownDecision | None = None,
    healthy_observation_count: int | None = None,
) -> PlaceBrokerOrderResult:
    recovery: dict[str, object] = {
        "source_broker_order_pk": source_order.id,
        "source_idempotency_key": source_order.idempotency_key,
    }
    if retry_after_at is not None:
        recovery["retry_after_at"] = _normalize_utc_datetime(retry_after_at).isoformat()
    if healthy_observation_count is not None:
        recovery["healthy_observation_count"] = healthy_observation_count
    if decision is not None:
        recovery["decision"] = decision.summary
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": True,
            "blocked": True,
            "message": message,
            "block_reason": block_reason,
            "retry_after_at": recovery.get("retry_after_at"),
            "account_drawdown_recovery": recovery,
            "order": serialize_broker_order(source_order, include_events=True),
        },
    )


def _load_plan_item(
    *,
    settings: Settings,
    db: Session,
    command: PlaceOrderRequest,
) -> ExecutionPlanItem | None:
    if command.execution_plan_item_id is None:
        return None
    return db.scalar(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlanItem.id == command.execution_plan_item_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
    )


def _json_dict(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _risk_context_has_active_hourly_risk_off(risk_context: object) -> bool:
    risk_context = _json_dict(risk_context)
    adjustment = risk_context.get("hourly_plan_action")
    return bool(
        isinstance(adjustment, dict)
        and adjustment.get("source") == "hourly_runtime_check"
        and adjustment.get("action") == "risk_off_expire"
    )


def _plan_item_has_active_hourly_risk_off(plan_item: ExecutionPlanItem) -> bool:
    return plan_item.status == "watch_expired" and _risk_context_has_active_hourly_risk_off(
        plan_item.risk_context
    )


def _current_plan_item_state(
    *,
    db: Session,
    item_id: str,
    lock: bool,
    refresh_authority: bool = False,
) -> tuple[str, dict[str, object]] | None:
    # Do not autoflush a monitoring cycle's stale status before reading the
    # current durable row. In SQLite the no-op write establishes the same
    # single-writer ordering that SELECT FOR UPDATE provides on PostgreSQL.
    with db.no_autoflush:
        if lock and db.get_bind().dialect.name == "sqlite":
            db.execute(
                text(
                    "UPDATE execution_plan_items SET id = id WHERE id = :item_id"
                ),
                {"item_id": item_id},
            )
        if refresh_authority:
            statement = (
                select(ExecutionPlanItem)
                .join(ExecutionPlanItem.plan)
                .options(
                    contains_eager(ExecutionPlanItem.plan),
                    undefer(ExecutionPlanItem.risk_context),
                )
                .where(ExecutionPlanItem.id == item_id)
                .execution_options(populate_existing=True)
            )
        else:
            statement = select(
                ExecutionPlanItem.status,
                ExecutionPlanItem.risk_context,
            ).where(ExecutionPlanItem.id == item_id)
        if lock:
            statement = statement.with_for_update()
        row = db.execute(statement).one_or_none()
    if row is None:
        return None
    if refresh_authority:
        refreshed_item = row[0]
        return str(refreshed_item.status), _json_dict(refreshed_item.risk_context)
    return str(row[0]), _json_dict(row[1])


def _current_buy_plan_item_state(
    *,
    db: Session,
    item_id: str,
    lock: bool,
) -> tuple[str, dict[str, object]] | None:
    """Compatibility wrapper for monitoring callers that only evaluate BUYs."""
    return _current_plan_item_state(db=db, item_id=item_id, lock=lock)


def _account_recovery_candidate_state_error(
    *,
    db: Session,
    plan_item: ExecutionPlanItem | None,
    source_order: BrokerOrder | None,
    request_source: str,
    retryable_safety_fenced_order: BrokerOrder | None,
    lock: bool,
) -> PlaceBrokerOrderResult | None:
    if (
        source_order is None
        or plan_item is None
        or not request_source.startswith(
            ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
        )
    ):
        return None
    current = _current_buy_plan_item_state(
        db=db,
        item_id=plan_item.id,
        lock=lock,
    )
    if current is not None and _plan_status_allows_automated_retry(
        status=current[0],
        retryable_safety_fenced_order=retryable_safety_fenced_order,
    ):
        return None
    return _account_drawdown_recovery_blocked_result(
        source_order=source_order,
        block_reason="account_drawdown_recovery_candidate_ineligible",
        message=(
            "execution plan item is no longer eligible for automatic "
            "account drawdown recovery"
        ),
    )


def _merge_pending_plan_item_risk_context(
    *,
    persisted: dict[str, object],
    pending: dict[str, object],
) -> dict[str, object]:
    merged = dict(persisted)
    for key, value in pending.items():
        if key in {"hourly_plan_action", SELL_MONITOR_AUTHORITY_KEY} and key in persisted:
            continue
        merged[key] = value
    persisted_events = persisted.get("monitoring_events")
    pending_events = pending.get("monitoring_events")
    if isinstance(persisted_events, list) or isinstance(pending_events, list):
        combined: list[object] = []
        for event in (
            *(persisted_events if isinstance(persisted_events, list) else []),
            *(pending_events if isinstance(pending_events, list) else []),
        ):
            if event not in combined:
                combined.append(event)
        merged["monitoring_events"] = combined[-20:]
    return merged


def _plan_item_identity_changed_result() -> PlaceBrokerOrderResult:
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "blocked": True,
            "message": "execution plan item identity changed before order intent commit",
            "block_reason": "execution_plan_item_identity_changed",
            "permanent_rejection": True,
        },
    )


def _validate_plan_item(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
    request_source: str = "post:/orders",
) -> str | None:
    if plan_item is None:
        return None
    if plan_item.symbol != command.symbol:
        return "execution plan item symbol does not match order symbol"
    if plan_item.side != command.side:
        return "execution plan item side does not match order side"
    persisted_provider = str(plan_item.provider or "kis").strip().lower()
    if persisted_provider != command.provider:
        return "execution plan item provider does not match order provider"
    persisted_asset_class = str(plan_item.asset_class or "stock").strip().lower()
    try:
        plan_asset_class = canonical_execution_asset_class(
            persisted_asset_class,
            provider=persisted_provider,
            exchange=command.exchange,
        )
    except ValueError:
        return "execution plan item asset_class is unsupported for its route"
    if plan_asset_class != command.asset_class:
        return "execution plan item asset_class does not match order asset_class"
    if plan_item.venue and (
        command.venue is None
        or str(plan_item.venue).strip().lower()
        != str(command.venue).strip().lower()
    ):
        return "execution plan item venue does not match order venue"
    if (
        not plan_item.venue
        and command.venue is not None
        and str(command.venue).strip().lower() not in {"kis", "krx"}
    ):
        return "execution plan item venue does not match order venue"
    risk_context = (
        plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
    )
    server_owned_command_transform = _server_owned_plan_command_matches(
        risk_context=risk_context,
        command=command,
        request_source=request_source,
    )
    server_owned_price_transform = bool(
        server_owned_command_transform
        or _persisted_monitoring_buy_reprice_authority_matches(
            plan_item=plan_item,
            command=command,
            request_source=request_source,
        )
    )
    expected_time_in_force = plan_item.time_in_force or "day"
    if (
        expected_time_in_force != command.time_in_force
        and not server_owned_command_transform
    ):
        return "execution plan item time_in_force does not match order time_in_force"
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    raw_purpose = metadata.get("order_purpose") or metadata.get("purpose")
    if (
        isinstance(raw_purpose, str)
        and raw_purpose.strip()
        and not _plan_item_order_purpose_matches(
            intent=plan_item.intent,
            purpose=raw_purpose,
        )
    ):
        return "execution plan item intent does not match order purpose"
    expected_exchange = risk_context.get("exchange")
    if isinstance(expected_exchange, str) and expected_exchange.strip():
        normalized_expected_exchange = expected_exchange.strip().upper()
    elif persisted_provider == "alpaca" and plan_asset_class == "us_equity":
        normalized_expected_exchange = "US"
    elif persisted_provider == "alpaca" and plan_asset_class == "crypto":
        normalized_expected_exchange = "ALPACA"
    elif persisted_provider == "upbit" and plan_asset_class == "crypto":
        normalized_expected_exchange = "UPBIT"
    elif persisted_provider == "kis" and plan_asset_class == "stock":
        # Legacy domestic plans predate explicit route fields. Their only
        # canonical interpretation is the historical KRX default; they cannot
        # authorize an NXT/SOR/overseas route supplied by the order caller.
        normalized_expected_exchange = "KRX"
    else:
        return "execution plan item exchange authority is unavailable"
    if normalized_expected_exchange != command.exchange:
        return "execution plan item exchange does not match order exchange"
    quantity_policy = (
        plan_item.quantity_policy
        if isinstance(plan_item.quantity_policy, dict)
        else {}
    )
    expected_quantity = _plan_item_fixed_quantity(quantity_policy)
    expected_base_quantity = getattr(plan_item, "base_quantity", None)
    expected_quote_notional = getattr(plan_item, "quote_notional", None)
    expected_quantity_precision = getattr(plan_item, "quantity_precision", None)
    expected_price_precision = getattr(plan_item, "price_precision", None)
    quantity_matches = bool(
        expected_quantity is not None
        and (
            command.quantity == expected_quantity
            or (
                server_owned_price_transform
                and request_source == "monitoring:intraday_target_stop_monitor"
                and command.side == "buy"
                and command.quantity is not None
                and 0 < command.quantity <= expected_quantity
            )
            or (
                persisted_provider == "alpaca"
                and plan_asset_class == "us_equity"
                and command.quantity is None
                and command.base_quantity == Decimal(expected_quantity)
            )
        )
    )
    if (
        quantity_policy.get("kind") in {"shares", "fixed_quantity"}
        and expected_quantity is None
        and not server_owned_command_transform
    ):
        return "execution plan item sizing authority is invalid"
    if (
        expected_quantity is None
        and expected_base_quantity is None
        and expected_quote_notional is None
        and not server_owned_command_transform
    ):
        return "execution plan item sizing authority is unavailable"
    if (
        expected_quantity is not None
        and not quantity_matches
        and not server_owned_command_transform
    ):
        return "execution plan item quantity does not match order quantity"
    if expected_quantity is None and not server_owned_command_transform and (
        expected_base_quantity != command.base_quantity
        or expected_quote_notional != command.quote_notional
    ):
        return "execution plan item fractional sizing does not match order sizing"
    if not server_owned_command_transform and (
        expected_quantity_precision != command.quantity_precision
        or expected_price_precision != command.price_precision
    ):
        return "execution plan item precision does not match order precision"
    expected_limit_price = plan_item.limit_price
    if (
        expected_limit_price is None
        and (
            (command.side == "sell" and plan_item.intent in {"stop_sell", "target_sell"})
            or (
                command.side == "buy"
                and is_watch_buy_intent(plan_item.intent)
            )
        )
        and getattr(plan_item, "trigger_price", None) is not None
    ):
        expected_limit_price = plan_item.trigger_price
    expected_order_type = "limit" if expected_limit_price is not None else "market"
    if (
        expected_order_type != command.order_type
        and not server_owned_price_transform
    ):
        return "execution plan item order_type does not match order order_type"
    if (
        expected_limit_price != command.limit_price
        and not server_owned_price_transform
    ):
        return "execution plan item limit_price does not match order limit_price"
    return None


def _server_owned_plan_command_matches(
    *,
    risk_context: dict[str, object],
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    authority_key = {
        "monitoring:intraday_target_stop_monitor": "monitoring_order_authority",
        "short-term-watcher:decision": "short_term_watcher_order_authority",
    }.get(request_source)
    if authority_key is None:
        return False
    authority = risk_context.get(authority_key)
    return bool(
        isinstance(authority, dict)
        and authority.get("policy_version") == 1
        and authority.get("source") == request_source
        and authority.get("command") == command.model_dump(mode="json")
    )


def _persisted_monitoring_buy_reprice_authority_matches(
    *,
    plan_item: ExecutionPlanItem,
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    if (
        request_source != "monitoring:intraday_target_stop_monitor"
        or command.side != "buy"
        or not is_watch_buy_intent(plan_item.intent)
    ):
        return False
    policy = (
        plan_item.quantity_policy
        if isinstance(plan_item.quantity_policy, dict)
        else {}
    )
    evidence = policy.get("exec_trigger_time_liquidity_evidence_v1")
    if isinstance(evidence, dict) and evidence.get("version") == 1:
        return bool(
            str(evidence.get("provider") or "").strip().lower()
            == command.provider
            and str(evidence.get("asset_class") or "").strip().lower()
            == command.asset_class
            and str(evidence.get("exchange") or "").strip().upper()
            == command.exchange
            and str(evidence.get("symbol") or "").strip().upper()
            == command.symbol
            and isinstance(evidence.get("observed_at"), str)
            and str(evidence.get("freshness_source") or "").strip()
        )

    risk_context = (
        plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
    )
    limit_price = command.limit_price
    trigger_price = _positive_decimal(
        getattr(plan_item, "trigger_price", None)
        or risk_context.get("watch_trigger_price")
        or risk_context.get("buy_trigger_price")
    )
    chase_limit = _positive_decimal(
        risk_context.get("chase_limit_price")
        or policy.get("chase_limit_price")
    )
    if (
        command.order_type != "limit"
        or limit_price is None
        or trigger_price is None
        or chase_limit is None
    ):
        return False
    direction = str(risk_context.get("trigger_direction") or "at_or_above").strip().lower()
    if direction == "at_or_below":
        price_in_trigger_envelope = limit_price <= trigger_price
    else:
        price_in_trigger_envelope = trigger_price <= limit_price <= chase_limit
    stop_price = _positive_decimal(risk_context.get("stop_price"))
    target_price = _positive_decimal(risk_context.get("target_price"))
    return bool(
        price_in_trigger_envelope
        and (stop_price is None or limit_price > stop_price)
        and (target_price is None or limit_price < target_price)
    )


def _plan_item_fixed_quantity(quantity_policy: object) -> int | None:
    if not isinstance(quantity_policy, dict):
        return None
    if quantity_policy.get("kind") not in {"shares", "fixed_quantity"}:
        return None
    value = quantity_policy.get("quantity")
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (DecimalException, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0 or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _plan_item_order_purpose_matches(*, intent: str, purpose: str) -> bool:
    normalized_intent = str(intent or "").strip().lower()
    normalized_purpose = str(purpose or "").strip().lower()
    if normalized_purpose == normalized_intent:
        return True
    if normalized_intent == "stop_sell" and normalized_purpose == "protective_stop":
        return True
    return normalized_intent in {"stop_sell", "target_sell"} and normalized_purpose in {
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
    }


def _plan_item_state_error(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
) -> PlaceBrokerOrderResult | None:
    if (
        plan_item is None
        or plan_item.status not in PLAN_ITEM_NONCLAIMABLE_STATUSES
    ):
        return None
    return _plan_item_state_changed_result(side=command.side)


def _plan_item_state_changed_result(*, side: str) -> PlaceBrokerOrderResult:
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": (
                "execution plan item is no longer eligible for "
                f"{str(side or 'order').strip().lower()} submission"
            ),
            "block_reason": "execution_plan_item_state_changed",
            "preserve_plan_item_status": True,
        },
    )


def _validate_provider_capability(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> str | None:
    if command.provider != adapter.provider:
        return (
            f"order provider '{command.provider}' does not match adapter "
            f"provider '{adapter.provider}'"
        )
    if command.provider == "upbit" and command.asset_class != "crypto":
        return "upbit adapter accepts crypto orders only"
    if command.provider == "alpaca" and command.asset_class not in {"crypto", "us_equity"}:
        return "alpaca adapter accepts crypto and US-equity orders only"
    return None


def _domestic_equity_market_date_block(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    now: datetime | None = None,
) -> dict[str, object] | None:
    if settings.runtime_mode != "live":
        return None
    if adapter.provider != "kis" or command.asset_class != "stock":
        return None
    if command.exchange not in {"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"}:
        return None

    local_now = (now or datetime.now(UTC)).astimezone(ZoneInfo(settings.market_timezone))
    if local_now.weekday() < 5:
        return None

    return {
        "reason": "domestic_equity_market_closed_weekend",
        "message": "domestic equity order deferred because the market date is a weekend",
        "market_date": local_now.date().isoformat(),
        "weekday": local_now.strftime("%A").lower(),
        "timezone": settings.market_timezone,
        "provider": adapter.provider,
        "exchange": command.exchange,
    }


def _load_automated_kis_buy_market_evidence(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    access_token: str,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    reference_at: datetime,
) -> AutomatedKisBuyMarketEvidence | None:
    if settings.runtime_mode != "live":
        return None
    if adapter.provider != "kis" or command.provider != "kis":
        return None
    if command.side != "buy" or not _is_new_buy_order(command=command, plan_item=plan_item):
        return None
    if not request_source.startswith(ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES):
        return None

    exchange = str(command.exchange or "KRX").strip().upper()
    try:
        canonical_asset_class = canonical_execution_asset_class(
            command.asset_class,
            provider=command.provider,
            exchange=exchange,
        )
    except ValueError:
        canonical_asset_class = str(command.asset_class or "").strip().lower()
    is_domestic = (
        canonical_asset_class == "stock"
        and exchange in {"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"}
    )
    if is_domestic:
        calendar_getter = getattr(adapter, "is_krx_trading_day", None)
        if not callable(calendar_getter):
            return AutomatedKisBuyMarketEvidence(
                context=None,
                is_domestic=True,
                failure=(
                    "krx_calendar_unavailable",
                    "automated KRX buy requires authoritative trading-day evidence",
                ),
            )
        local_date = reference_at.astimezone(
            ZoneInfo(settings.market_timezone)
        ).date()
        try:
            trading_day = calendar_getter(access_token, market_date=local_date)
        except Exception:
            trading_day = None
        if trading_day is False:
            return AutomatedKisBuyMarketEvidence(
                context=None,
                is_domestic=True,
                failure=(
                    "krx_market_closed",
                    "automated KRX buy is blocked by authoritative closed-market evidence",
                ),
            )
        if trading_day is not True:
            return AutomatedKisBuyMarketEvidence(
                context=None,
                is_domestic=True,
                failure=(
                    "krx_calendar_unavailable",
                    "automated KRX buy requires authoritative trading-day evidence",
                ),
            )

    context_getter = getattr(adapter, "get_live_market_context", None)
    if not callable(context_getter):
        return AutomatedKisBuyMarketEvidence(
            context=None,
            is_domestic=is_domestic,
            failure=(
                "live_buy_orderbook_unavailable",
                "automated KIS buy requires timestamped executable orderbook evidence",
            ),
        )
    try:
        context = context_getter(
            access_token,
            symbol=command.symbol,
            exchange=exchange,
            reference_price=command.limit_price,
        )
    except Exception:
        context = None
    if context is None:
        return AutomatedKisBuyMarketEvidence(
            context=None,
            is_domestic=is_domestic,
            failure=(
                "live_buy_orderbook_unavailable",
                "automated KIS buy requires timestamped executable orderbook evidence",
            ),
        )
    liquidity_snapshot: BrokerIntradayLiquiditySnapshot | None = None
    if (
        is_domestic
        and plan_item is not None
        and is_short_term_watch_buy_intent(plan_item.intent)
    ):
        liquidity_getter = getattr(adapter, "get_intraday_liquidity_snapshot", None)
        if callable(liquidity_getter):
            try:
                candidate_snapshot = liquidity_getter(
                    access_token,
                    symbol=command.symbol,
                    exchange=exchange,
                    reference_at=reference_at,
                )
            except Exception:
                candidate_snapshot = None
            if isinstance(candidate_snapshot, BrokerIntradayLiquiditySnapshot):
                liquidity_snapshot = candidate_snapshot
    return AutomatedKisBuyMarketEvidence(
        context=context,
        is_domestic=is_domestic,
        liquidity_snapshot=liquidity_snapshot,
    )


def _load_automated_kis_sell_market_evidence(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    access_token: str,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> AutomatedKisSellMarketEvidence | None:
    if settings.runtime_mode != "live":
        return None
    if adapter.provider != "kis" or command.provider != "kis":
        return None
    if (
        command.side != "sell"
        or plan_item is None
        or plan_item.intent not in {"target_sell", "stop_sell", "max_holding_close_sell"}
        or not request_source.startswith("monitoring:")
    ):
        return None
    exchange = str(command.exchange or "KRX").strip().upper()
    try:
        canonical_asset_class = canonical_execution_asset_class(
            command.asset_class,
            provider=command.provider,
            exchange=exchange,
        )
    except ValueError:
        canonical_asset_class = str(command.asset_class or "").strip().lower()
    is_domestic = bool(
        canonical_asset_class == "stock"
        and exchange in {"KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"}
    )
    supported = is_domestic or (
        canonical_asset_class == "us_equity"
        and exchange in {"NASD", "NYSE", "AMEX"}
    )
    if not supported:
        return AutomatedKisSellMarketEvidence(
            context=None,
            is_domestic=is_domestic,
            failure=(
                "live_sell_quote_route_unsupported",
                "automated KIS sell has no trusted executable quote route",
            ),
        )
    context_getter = getattr(adapter, "get_live_market_context", None)
    if not callable(context_getter):
        return AutomatedKisSellMarketEvidence(
            context=None,
            is_domestic=is_domestic,
            failure=(
                "live_sell_orderbook_unavailable",
                "automated KIS sell requires timestamped executable bid evidence",
            ),
        )
    try:
        context = context_getter(
            access_token,
            symbol=command.symbol,
            exchange=exchange,
            reference_price=command.limit_price,
        )
    except Exception:
        context = None
    if context is None:
        return AutomatedKisSellMarketEvidence(
            context=None,
            is_domestic=is_domestic,
            failure=(
                "live_sell_orderbook_unavailable",
                "automated KIS sell requires timestamped executable bid evidence",
            ),
        )
    return AutomatedKisSellMarketEvidence(
        context=context,
        is_domestic=is_domestic,
    )


def _automated_kis_sell_market_evidence_failure(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    evidence: AutomatedKisSellMarketEvidence | None,
    reference_at: datetime,
) -> tuple[str, str] | None:
    if evidence is None:
        return None
    if evidence.failure is not None:
        return evidence.failure
    context = evidence.context
    if context is None:  # pragma: no cover - constructor invariant
        return (
            "live_sell_orderbook_unavailable",
            "automated KIS sell requires timestamped executable bid evidence",
        )
    exchange = str(command.exchange or "KRX").strip().upper()
    if context.symbol.strip().upper() != command.symbol.strip().upper() or (
        context.exchange.strip().upper() != exchange
    ):
        return (
            "live_sell_orderbook_identity_mismatch",
            "automated KIS sell orderbook identity does not match the order route",
        )
    expected_source = (
        "kis_exchange_orderbook_time"
        if evidence.is_domestic
        else "kis_overseas_exchange_orderbook_time"
    )
    expected_endpoint = (
        "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        if evidence.is_domestic
        else "/uapi/overseas-price/v1/quotations/inquire-asking-price"
    )
    expected_tr_id = "FHKST01010200" if evidence.is_domestic else "HHDFS76200100"
    payload = context.payload if isinstance(context.payload, dict) else {}
    field_sources = payload.get("field_sources")
    bid_source = field_sources.get("bid") if isinstance(field_sources, dict) else None
    allowed_bid_fields = (
        {"bid", "bid_price", "best_bid", "bidp1", "bidp"}
        if evidence.is_domestic
        else {"pbid1", "PBID1", "bidp1", "BIDP1"}
    )
    if (
        context.freshness_source != expected_source
        or payload.get("provider") != "kis"
        or payload.get("endpoint") != expected_endpoint
        or payload.get("tr_id") != expected_tr_id
        or not isinstance(bid_source, dict)
        or bid_source.get("field") not in allowed_bid_fields
        or bid_source.get("endpoint") != expected_endpoint
        or bid_source.get("tr_id") != expected_tr_id
    ):
        return (
            "live_sell_quote_untrusted",
            "automated KIS sell bid lacks trusted field and endpoint provenance",
        )
    if context.observed_at.tzinfo is None or context.observed_at.utcoffset() is None:
        return (
            "live_sell_quote_timestamp_invalid",
            "automated KIS sell orderbook timestamp is not timezone-aware",
        )
    evaluated_at = _normalize_utc_datetime(reference_at)
    observed_at = _normalize_utc_datetime(context.observed_at)
    if observed_at - evaluated_at > timedelta(
        seconds=settings.monitoring_max_quote_future_skew_seconds
    ):
        return (
            "live_sell_quote_future_skew",
            "automated KIS sell bid timestamp exceeds allowed future skew",
        )
    if evaluated_at - observed_at > timedelta(
        seconds=settings.monitoring_max_quote_age_seconds
    ):
        return (
            "live_sell_quote_stale",
            "automated KIS sell bid is stale before submission",
        )
    market_timezone = ZoneInfo(
        settings.market_timezone if evidence.is_domestic else "America/New_York"
    )
    if observed_at.astimezone(market_timezone).date() != evaluated_at.astimezone(
        market_timezone
    ).date():
        return (
            "live_sell_quote_wrong_market_date",
            "automated KIS sell bid is not from the current market date",
        )
    if plan_item is not None and plan_item.plan is not None:
        plan_market_date = plan_item.plan.market_date
        if (
            plan_market_date is not None
            and plan_market_date > evaluated_at.astimezone(market_timezone).date()
        ):
            return (
                "live_sell_plan_market_date_mismatch",
                "automated KIS sell plan is not authorized for the current market date",
            )
    if context.normal_ordering_available is False:
        return (
            "live_sell_ordering_unavailable",
            "automated KIS sell orderbook reports normal ordering unavailable",
        )
    bid = context.bid
    if bid is None or not bid.is_finite() or bid <= 0:
        return (
            "live_sell_bid_unavailable",
            "automated KIS sell requires a positive executable bid",
        )
    trigger_failure = _final_monitoring_sell_trigger_failure(
        command=command,
        plan_item=plan_item,
        executable_bid=bid,
    )
    if trigger_failure is not None:
        return trigger_failure
    return _final_trigger_time_sell_agent_validity_failure(
        plan_item=plan_item,
        reference_at=evaluated_at,
    )


def _final_monitoring_sell_trigger_failure(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    executable_bid: Decimal,
) -> tuple[str, str] | None:
    if plan_item is None or plan_item.intent == "max_holding_close_sell":
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    binding = metadata.get("monitoring_sell_trigger_binding")
    if not isinstance(binding, dict):
        return (
            "live_sell_trigger_binding_missing",
            "automated monitoring sell is missing its persisted trigger binding",
        )
    direction = binding.get("direction")
    try:
        trigger_price = Decimal(str(binding.get("trigger_price")))
    except (DecimalException, ValueError):
        trigger_price = None
    if (
        binding.get("version") != 1
        or binding.get("execution_plan_item_id") != plan_item.id
        or str(binding.get("symbol") or "").strip().upper()
        != plan_item.symbol.strip().upper()
        or binding.get("intent") != plan_item.intent
        or direction not in {"at_or_above", "at_or_below"}
        or trigger_price is None
        or not trigger_price.is_finite()
        or trigger_price <= 0
    ):
        return (
            "live_sell_trigger_binding_invalid",
            "automated monitoring sell trigger binding does not match the plan item",
        )
    trigger_met = (
        executable_bid >= trigger_price
        if direction == "at_or_above"
        else executable_bid <= trigger_price
    )
    if not trigger_met:
        return (
            "live_sell_trigger_not_met",
            "fresh executable bid no longer satisfies the persisted sell trigger",
        )
    return None


def _final_target_sell_stop_integrity_failure(
    *,
    db: Session,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> tuple[str, str] | None:
    if (
        plan_item is None
        or command.side != "sell"
        or plan_item.intent != "target_sell"
        or not isinstance(plan_item.risk_context, dict)
    ):
        return None
    interaction = plan_item.risk_context.get("last_trigger_time_exec_agent_interaction")
    decision = interaction.get("decision") if isinstance(interaction, dict) else None
    if (
        not isinstance(decision, dict)
        or decision.get("decision") != "execute_now"
        or decision.get("side") != "sell"
    ):
        return None
    try:
        proposed_stop = Decimal(str(decision.get("updated_stop_price")))
    except (DecimalException, ValueError):
        return None
    if not proposed_stop.is_finite() or proposed_stop <= 0:
        return None
    target_route = plan_item_execution_route_key(plan_item)
    target_inventory_scope = plan_item_inventory_scope_key(plan_item)
    if target_route is None or target_inventory_scope is None:
        return (
            "live_sell_protective_stop_route_unavailable",
            "trigger-time target sell approval has no exact execution route",
        )
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
    sibling_stops = db.scalars(
        select(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.execution_plan_id == plan_item.execution_plan_id,
            ExecutionPlanItem.symbol == plan_item.symbol,
            ExecutionPlanItem.intent == "stop_sell",
            ExecutionPlanItem.status.not_in(terminal_statuses),
        )
        .options(undefer(ExecutionPlanItem.risk_context))
        .with_for_update()
    ).all()
    sibling_stops = select_plan_item_family_members(
        anchor=plan_item,
        candidates=sibling_stops,
    )
    sibling_stops = [
        sibling
        for sibling in sibling_stops
        if plan_item_inventory_scope_key(sibling) == target_inventory_scope
    ]
    effective_stop = max(
        (
            sibling.trigger_price
            for sibling in sibling_stops
            if sibling.trigger_price is not None
            and sibling.trigger_price.is_finite()
            and sibling.trigger_price > 0
        ),
        default=None,
    )
    if effective_stop is not None and proposed_stop < effective_stop:
        return (
            "live_sell_protective_stop_relaxation",
            "trigger-time target sell approval would loosen the current protective stop",
        )
    return None


def _final_trigger_time_sell_agent_validity_failure(
    *,
    plan_item: ExecutionPlanItem | None,
    reference_at: datetime,
) -> tuple[str, str] | None:
    if plan_item is None or not isinstance(plan_item.risk_context, dict):
        return None
    interaction = plan_item.risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return None
    decision = interaction.get("decision")
    if (
        not isinstance(decision, dict)
        or decision.get("decision") != "execute_now"
        or decision.get("side") != "sell"
    ):
        return None
    raw_valid_until = decision.get("valid_until")
    try:
        valid_until = datetime.fromisoformat(str(raw_valid_until).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        valid_until = None
    if valid_until is None or valid_until.tzinfo is None or valid_until.utcoffset() is None:
        return (
            "live_sell_agent_valid_until_missing",
            "trigger-time sell approval requires a timezone-aware valid_until boundary",
        )
    if reference_at >= _normalize_utc_datetime(valid_until):
        return (
            "live_sell_agent_approval_expired",
            "trigger-time sell approval expired before broker submission",
        )
    return None


def _bind_automated_kis_sell_limit(
    *,
    command: PlaceOrderRequest,
    evidence: AutomatedKisSellMarketEvidence,
) -> PlaceOrderRequest:
    """Keep price-required KIS protective sells executable at the trusted bid.

    KIS-US accepts day limit orders only. Rebinding to the fresh best bid avoids
    leaving a stop stranded at the earlier monitoring quote while retaining a
    finite price boundary. KRX single-price ``07`` is also a limit-price wire
    contract even when the logical monitoring order was market; bind only that
    exact server-owned route. Other domestic sessions keep their existing
    market/closing-price semantics.
    """

    context = evidence.context
    if context is None or context.bid is None or context.bid <= 0:
        return command
    metadata = dict(command.metadata or {})
    if evidence.is_domestic:
        route = metadata.get("route")
        if not (
            command.provider == "kis"
            and command.exchange == "KRX"
            and command.order_type == "limit"
            and command.limit_price is None
            and isinstance(route, dict)
            and route.get("action") == "submit"
            and route.get("exchange") == "KRX"
            and route.get("session") == "krx_after_single_price"
            and route.get("ord_dvsn") == "07"
            and route.get("effective_order_type") == "limit"
            and route.get("price_source") == "single_price_session"
            and route.get("next_retry_at") is None
            and route.get("reason") == "krx_after_hours_single_price_enabled"
        ):
            return command
    metadata["final_exit_price_bound"] = {
        "source": "fresh_executable_bid",
        "limit_price": str(context.bid),
        "time_in_force": "day",
    }
    return command.model_copy(
        update={
            "order_type": "limit",
            "limit_price": context.bid,
            "time_in_force": "day",
            "metadata": metadata,
        }
    )


def _automated_kis_buy_market_evidence_failure(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    evidence: AutomatedKisBuyMarketEvidence | None,
    reference_at: datetime,
) -> tuple[str, str] | None:
    if evidence is None:
        return None
    if evidence.failure is not None:
        return evidence.failure
    context = evidence.context
    if context is None:  # pragma: no cover - constructor invariant
        return (
            "live_buy_orderbook_unavailable",
            "automated KIS buy requires timestamped executable orderbook evidence",
        )
    exchange = str(command.exchange or "KRX").strip().upper()
    is_domestic = evidence.is_domestic
    if context.symbol != command.symbol or context.exchange.strip().upper() != exchange:
        return (
            "live_buy_orderbook_identity_mismatch",
            "automated KIS buy orderbook identity does not match the order",
        )
    expected_source = (
        "kis_exchange_orderbook_time"
        if is_domestic
        else "kis_overseas_exchange_orderbook_time"
    )
    if context.freshness_source != expected_source:
        return (
            "live_buy_quote_untrusted",
            "automated KIS buy orderbook lacks trusted exchange-time provenance",
        )
    if context.observed_at.tzinfo is None or context.observed_at.utcoffset() is None:
        return (
            "live_buy_quote_timestamp_invalid",
            "automated KIS buy orderbook timestamp is not timezone-aware",
        )
    evaluated_at = _normalize_utc_datetime(reference_at)
    observed_at = _normalize_utc_datetime(context.observed_at)
    if observed_at - evaluated_at > timedelta(
        seconds=settings.monitoring_max_quote_future_skew_seconds
    ):
        return (
            "live_buy_quote_future_skew",
            "automated KIS buy orderbook timestamp exceeds allowed future skew",
        )
    if evaluated_at - observed_at > timedelta(
        seconds=settings.monitoring_max_quote_age_seconds
    ):
        return (
            "live_buy_quote_stale",
            "automated KIS buy orderbook evidence is stale before submission",
        )
    market_timezone = ZoneInfo(
        settings.market_timezone if is_domestic else "America/New_York"
    )
    if observed_at.astimezone(market_timezone).date() != evaluated_at.astimezone(
        market_timezone
    ).date():
        return (
            "live_buy_quote_wrong_market_date",
            "automated KIS buy orderbook evidence is not from the current market date",
        )
    if plan_item is not None:
        plan_market_date = (
            plan_item.plan.market_date if plan_item.plan is not None else None
        )
        current_market_date = evaluated_at.astimezone(market_timezone).date()
        if (
            plan_market_date is None
            or plan_market_date > current_market_date
            or (
                is_domestic
                and is_short_term_watch_buy_intent(plan_item.intent)
                and plan_market_date != current_market_date
            )
        ):
            return (
                "live_buy_plan_market_date_mismatch",
                "automated KIS buy plan is not authorized for the current market date",
            )
    if (
        is_domestic
        and plan_item is not None
        and plan_item.intent == "next_open_buy"
    ):
        market_date = plan_item.plan.market_date if plan_item.plan is not None else None
        local_tz = ZoneInfo(settings.market_timezone)
        if market_date is None:
            return (
                "missed_next_open_submission_window",
                "next-open buy requires a current market date at final submission",
            )
        local_open = datetime.combine(
            market_date,
            datetime.min.time().replace(hour=9),
            tzinfo=local_tz,
        ).astimezone(UTC)
        deadline = local_open + timedelta(
            seconds=settings.next_open_buy_initial_submission_grace_seconds
        )
        if (
            evaluated_at.astimezone(local_tz).date() != market_date
            or evaluated_at < local_open
            or evaluated_at > deadline
        ):
            return (
                "missed_next_open_submission_window",
                "next-open buy crossed its initial submission grace window",
            )
    if is_domestic:
        final_route = _final_automated_domestic_buy_route(
            settings=settings,
            command=command,
            plan_item=plan_item,
            reference_at=evaluated_at,
        )
        if (
            final_route.action != "submit"
            or final_route.exchange != exchange
            or final_route.effective_order_type != command.order_type
        ):
            return (
                "live_buy_ordering_unavailable",
                "automated KRX buy is outside its final orderable session route",
            )
    if context.normal_ordering_available is False:
        return (
            "live_buy_ordering_unavailable",
            "automated KIS buy orderbook reports normal ordering unavailable",
        )
    executable_ask = context.ask
    if executable_ask is None or executable_ask <= 0:
        return (
            "live_buy_ask_unavailable",
            "automated KIS buy requires a positive executable ask",
        )
    entry_signal_failure = _final_watch_buy_entry_signal_failure(
        plan_item=plan_item,
        executable_ask=executable_ask,
    )
    if entry_signal_failure is not None:
        return entry_signal_failure
    live_spread_bps = _executable_quote_spread_bps(context)
    if live_spread_bps is None:
        return (
            "live_buy_transaction_cost_evidence_missing",
            "automated KIS buy requires a valid executable bid/ask spread for cost validation",
        )
    execution_reference = max(
        executable_ask,
        command.limit_price or executable_ask,
    )
    if plan_item is not None and is_watch_buy_intent(plan_item.intent):
        target = _first_positive_plan_item_price(
            plan_item,
            "updated_target_price",
            "target_price",
            "take_profit_price",
        )
        stop = _final_watch_buy_effective_stop(
            settings=settings,
            plan_item=plan_item,
            execution_reference=execution_reference,
        )
        if target is None:
            return (
                "live_buy_target_missing",
                "automated KIS buy requires a positive target price",
            )
        if stop is None:
            return (
                "live_buy_stop_missing",
                "automated KIS buy requires a positive stop price",
            )
        if target <= execution_reference:
            return (
                "live_buy_target_geometry_invalid",
                "automated KIS buy target must exceed the executable entry reference",
            )
        if stop >= execution_reference:
            return (
                "live_buy_stop_geometry_invalid",
                "automated KIS buy stop must remain below the executable entry reference",
            )
        economics = assess_buy_economics(
            reference_price=execution_reference,
            target_price=target,
            stop_price=stop,
            minimum_reward_risk=(
                max(
                    Decimal(str(settings.exec_agent_min_reward_risk)),
                    SHORT_TERM_LIVE_MIN_NET_REWARD_RISK,
                )
                if plan_item is not None
                and is_short_term_watch_buy_intent(plan_item.intent)
                else Decimal(str(settings.exec_agent_min_reward_risk))
            ),
            evidence=configured_execution_cost_evidence(
                settings=settings,
                provider=command.provider,
                asset_class=command.asset_class,
                exchange=exchange,
                live_quote_spread_bps=live_spread_bps,
                require_live_quote_spread=True,
            ),
            require_cost_evidence=True,
        )
        if economics.reason is not None:
            return (
                f"live_buy_{economics.reason}",
                "automated KIS buy failed deterministic final execution economics",
            )
        delegated_market_context = _broker_delegated_gate_market_context(
            settings=settings,
            evaluated_at=evaluated_at,
            plan_item=plan_item,
            liquidity_snapshot=evidence.liquidity_snapshot,
        )
        if plan_item is not None and is_short_term_watch_buy_intent(plan_item.intent):
            live_turnover = trusted_live_market_decimal(
                delegated_market_context,
                field="turnover",
                evaluated_at=evaluated_at,
            )
            if live_turnover is None:
                return (
                    "live_buy_short_term_turnover_unavailable",
                    "short-term automated buy requires timestamped turnover evidence",
                )
            if live_turnover < SHORT_TERM_LIVE_MIN_TURNOVER_KRW:
                return (
                    "live_buy_short_term_turnover_below_minimum",
                    "short-term automated buy turnover is below the deterministic floor",
                )
        delegated_rejections = delegated_buy_gate_rejections(
            [plan_item.risk_context, plan_item.quantity_policy],
            evaluated_at=evaluated_at,
            live_market_context=delegated_market_context,
            economics=economics,
            require_live_recheck=True,
        )
        if delegated_rejections:
            return (
                f"live_buy_{delegated_rejections[0]}",
                "automated KIS buy failed a delegated Core gate at the final broker boundary",
            )
    return None


def _final_watch_buy_entry_signal_failure(
    *,
    plan_item: ExecutionPlanItem | None,
    executable_ask: Decimal,
) -> tuple[str, str] | None:
    if plan_item is None or not is_short_term_watch_buy_intent(plan_item.intent):
        return None
    chase_limit = _first_positive_plan_item_price(
        plan_item,
        "chase_limit_price",
        "max_chase_limit_price",
        "entry_limit_price",
        "original_watch_chase_limit_price",
    )
    if chase_limit is not None and executable_ask > chase_limit:
        return (
            "live_buy_chase_limit_exceeded",
            "automated watch buy executable ask exceeds the configured chase limit",
        )
    trigger_price = (
        plan_item.trigger_price
        if plan_item.trigger_price is not None
        and plan_item.trigger_price.is_finite()
        and plan_item.trigger_price > 0
        else _first_positive_plan_item_price(
            plan_item,
            "buy_recommendation_price",
            "recommended_entry_price",
            "entry_recommendation_price",
            "recommendation_price",
            "entry_price",
            "watch_trigger_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "original_watch_trigger_price",
        )
    )
    if trigger_price is None:
        return (
            "live_buy_trigger_missing",
            "automated watch buy requires a positive trigger price at submission",
        )
    direction = watch_buy_direction_from_payloads(
        plan_item.quantity_policy,
        plan_item.risk_context,
    )
    trigger_met = (
        executable_ask <= trigger_price
        if direction == "at_or_below"
        else executable_ask >= trigger_price
    )
    if not trigger_met:
        return (
            "live_buy_trigger_not_met",
            "automated watch buy executable ask no longer satisfies the monitored trigger",
        )
    return None


def _bind_automated_watch_buy_ioc_limit(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    evidence: AutomatedKisBuyMarketEvidence | None,
    route_decision: RouteDecision | None,
) -> tuple[
    PlaceOrderRequest,
    RouteDecision | None,
    tuple[str, str] | None,
]:
    """Turn a point-in-time KRX watch decision into a non-lingering price cap."""

    if (
        evidence is None
        or not evidence.is_domestic
        or plan_item is None
        or not is_watch_buy_intent(plan_item.intent)
    ):
        return command, route_decision, None
    context = evidence.context
    executable_ask = context.ask if context is not None else None
    if executable_ask is None or not executable_ask.is_finite() or executable_ask <= 0:
        return command, route_decision, (
            "live_buy_ask_unavailable",
            "automated KIS buy requires a positive executable ask",
        )
    requested_cap = command.limit_price
    if (
        requested_cap is not None
        and requested_cap.is_finite()
        and requested_cap > 0
        and executable_ask > requested_cap
    ):
        return command, route_decision, (
            "live_buy_entry_limit_exceeded",
            "automated watch buy executable ask exceeds its pre-approved entry cap",
        )
    if (
        route_decision is None
        or route_decision.action != "submit"
        or route_decision.exchange != "KRX"
        or route_decision.session != "krx_regular"
    ):
        return command, route_decision, (
            "live_buy_ioc_route_unavailable",
            "automated watch buy requires the regular KRX IOC-limit route",
        )

    bounded_route = RouteDecision(
        action="submit",
        exchange="KRX",
        session="krx_regular",
        ord_dvsn="11",
        effective_order_type="limit",
        price_source="fresh_executable_ask_ioc_cap",
        next_retry_at=None,
        reason=f"{route_decision.reason}:bounded_ioc_limit",
    )
    metadata = dict(command.metadata or {})
    metadata["route"] = _route_payload(bounded_route)
    metadata["final_entry_price_cap"] = {
        "source": "fresh_executable_ask",
        "limit_price": str(executable_ask),
        "time_in_force": "ioc",
        "ord_dvsn": "11",
    }
    bounded_command = command.model_copy(
        update={
            "order_type": "limit",
            "limit_price": executable_ask,
            "time_in_force": "ioc",
            "exchange": "KRX",
            "metadata": metadata,
        }
    )
    return bounded_command, bounded_route, None


def _executable_quote_spread_bps(
    context: BrokerLiveMarketContext,
) -> Decimal | None:
    bid = context.bid
    ask = context.ask
    if (
        bid is None
        or ask is None
        or not bid.is_finite()
        or not ask.is_finite()
        or bid <= 0
        or ask <= 0
        or ask < bid
    ):
        return None
    try:
        midpoint = (bid + ask) / Decimal("2")
        if midpoint <= 0:
            return None
        spread_bps = (ask - bid) / midpoint * Decimal("10000")
    except DecimalException:
        return None
    return spread_bps if spread_bps.is_finite() and spread_bps >= 0 else None


def _broker_delegated_gate_market_context(
    *,
    settings: Settings,
    evaluated_at: datetime,
    plan_item: ExecutionPlanItem | None,
    liquidity_snapshot: BrokerIntradayLiquiditySnapshot | None = None,
) -> dict[str, object]:
    snapshot = liquidity_snapshot
    payload = snapshot.payload if isinstance(snapshot, BrokerIntradayLiquiditySnapshot) and isinstance(snapshot.payload, dict) else {}
    expected_exchange = "KRX"
    if plan_item is not None:
        risk_context = plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
        raw_exchange = str(risk_context.get("exchange") or plan_item.venue or "KRX").strip().upper()
        expected_exchange = (
            "KRX" if raw_exchange in {"KRX", "KOSPI", "KOSDAQ"} else raw_exchange
        )
    observed_at = (
        snapshot.observed_at
        if isinstance(snapshot, BrokerIntradayLiquiditySnapshot)
        else None
    )
    normalized_observed_at = (
        _normalize_utc_datetime(observed_at)
        if isinstance(observed_at, datetime)
        and observed_at.tzinfo is not None
        and observed_at.utcoffset() is not None
        else None
    )
    normalized_evaluated_at = _normalize_utc_datetime(evaluated_at)
    valid_proof = bool(
        isinstance(snapshot, BrokerIntradayLiquiditySnapshot)
        and plan_item is not None
        and snapshot.symbol == plan_item.symbol
        and snapshot.exchange.strip().upper() == expected_exchange
        and snapshot.freshness_source == "kis_exchange_intraday_minute_time"
        and payload.get("provider") == "kis"
        and payload.get("endpoint")
        == "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        and payload.get("tr_id") == "FHKST03010200"
        and normalized_observed_at is not None
        and normalized_observed_at.astimezone(ZoneInfo(settings.market_timezone)).date()
        == normalized_evaluated_at.astimezone(ZoneInfo(settings.market_timezone)).date()
        and normalized_observed_at
        <= normalized_evaluated_at
        + timedelta(seconds=settings.monitoring_max_quote_future_skew_seconds)
        and normalized_observed_at
        >= normalized_evaluated_at
        - timedelta(seconds=settings.monitoring_max_quote_age_seconds)
    )
    turnover = snapshot.turnover if valid_proof and snapshot is not None else None
    return {
        "live_execution_context_available": True,
        "turnover": turnover,
        "field_freshness": {
            "turnover": {
                "valid": valid_proof and turnover is not None,
                "source": snapshot.freshness_source if valid_proof and snapshot is not None else None,
                "provider_source": "broker_intraday_liquidity_snapshot" if valid_proof else None,
                "endpoint": payload.get("endpoint") if valid_proof else None,
                "tr_id": payload.get("tr_id") if valid_proof else None,
                "observed_at": normalized_observed_at.isoformat() if valid_proof and normalized_observed_at is not None else None,
                "max_age_seconds": settings.monitoring_max_quote_age_seconds,
                "max_future_skew_seconds": settings.monitoring_max_quote_future_skew_seconds,
            }
        },
    }


def _final_automated_domestic_buy_route(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    reference_at: datetime,
) -> RouteDecision:
    metadata = dict(command.metadata or {})
    return resolve_domestic_order_route(
        settings=settings,
        now=reference_at,
        symbol=command.symbol,
        side=command.side,
        purpose=_order_purpose(
            command=command,
            plan_item=plan_item,
            metadata=metadata,
        ),
        desired_order_type=command.order_type,
        limit_price=command.limit_price,
        requested_exchange=str(command.exchange or "KRX").strip().upper(),
        metadata=metadata,
    )


def _first_positive_plan_item_price(
    plan_item: ExecutionPlanItem,
    *keys: str,
) -> Decimal | None:
    for payload in (plan_item.risk_context, plan_item.quantity_policy):
        if not isinstance(payload, dict):
            continue
        for key in keys:
            raw_value = payload.get(key)
            if raw_value is None:
                continue
            try:
                value = Decimal(str(raw_value))
            except (ArithmeticError, ValueError):
                continue
            if value.is_finite() and value > 0:
                return value
    return None


def _final_watch_buy_effective_stop(
    *,
    settings: Settings,
    plan_item: ExecutionPlanItem,
    execution_reference: Decimal,
) -> Decimal | None:
    policy = (
        plan_item.quantity_policy
        if isinstance(plan_item.quantity_policy, dict)
        else {}
    )
    risk_context = (
        plan_item.risk_context
        if isinstance(plan_item.risk_context, dict)
        else {}
    )
    baseline: Decimal | None = None
    if policy.get("effective_stop_policy_version") == "server_effective_stop_v1":
        envelope = policy.get("allowed_action_envelope")
        effective = _positive_decimal(policy.get("effective_stop_price"))
        upstream = _positive_decimal(policy.get("upstream_stop_price"))
        sizing_price = _positive_decimal(policy.get("sizing_price"))
        if not (
            policy.get("source") == "exec_agent_watch_sizing"
            and persisted_verified_watch_sizing_provenance(policy)
            and isinstance(envelope, dict)
            and effective is not None
            and upstream is not None
            and sizing_price is not None
            and upstream <= effective < sizing_price
            and _positive_decimal(policy.get("stop_price")) == effective
            and _positive_decimal(policy.get("stop_loss_price")) == effective
            and _positive_decimal(envelope.get("upstream_stop_price")) == upstream
            and _positive_decimal(envelope.get("effective_stop_price")) == effective
            and _positive_decimal(envelope.get("stop_price")) == effective
        ):
            return None
        current_stop_candidates = [effective]
        for key in (
            "updated_stop_price",
            "effective_stop_price",
            "upstream_stop_price",
            "stop_price",
            "stop_loss_price",
        ):
            candidate = _positive_decimal(risk_context.get(key))
            if candidate is not None:
                current_stop_candidates.append(candidate)
        baseline = max(current_stop_candidates)
    else:
        baseline = _first_positive_plan_item_price(
            plan_item,
            "updated_stop_price",
            "upstream_stop_price",
            "stop_price",
            "stop_loss_price",
        )
    if baseline is None:
        return None
    return effective_stop_price(
        settings=settings,
        entry_price=execution_reference,
        upstream_stop_price=baseline,
    )


def _validate_upbit_paper_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> str | None:
    if command.provider != "upbit" and adapter.provider != "upbit":
        return None
    if command.provider != "upbit" or adapter.provider != "upbit":
        return "Upbit crypto paper execution requires provider='upbit'"
    if command.asset_class != "crypto":
        return "Upbit crypto paper execution requires asset_class='crypto'"
    if (command.venue or "").lower() != "upbit":
        return "Upbit crypto paper execution requires venue='upbit'"
    if command.exchange != "UPBIT":
        return "Upbit crypto paper execution requires exchange='UPBIT'"
    if command.symbol not in UPBIT_PAPER_ALLOWED_SYMBOLS:
        return "Upbit crypto paper execution is limited to KRW-BTC and KRW-ETH"
    if settings.runtime_mode != "paper":
        return "Upbit execution is paper-only in this runtime"
    config = adapter.describe()
    if config.provider != "upbit" or not config.configured:
        return config.reason or "Upbit provider readiness is uncertain"
    if plan_item is not None:
        if plan_item.provider != "upbit" or plan_item.asset_class != "crypto" or plan_item.venue != "upbit":
            return "execution plan item must explicitly declare Upbit crypto provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and notional is None:
        return "Upbit crypto paper buy requires deterministic quote_notional or limit-price notional"
    if notional is not None and notional > settings.exec_agent_max_symbol_budget:
        return "Upbit crypto paper order exceeds per-symbol execution budget"
    return None


def _validate_alpaca_crypto_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> str | None:
    if command.provider != "alpaca" and adapter.provider != "alpaca":
        return None
    if command.provider != "alpaca" or adapter.provider != "alpaca":
        return "Alpaca crypto execution requires provider='alpaca'"
    if command.asset_class not in {"crypto", "us_equity"}:
        return "Alpaca execution requires asset_class='crypto' or 'us_equity'"
    expected_exchange = "ALPACA" if command.asset_class == "crypto" else "US"
    if (command.venue or "").lower() != "alpaca" or command.exchange != expected_exchange:
        return f"Alpaca {command.asset_class} execution requires venue='alpaca' and exchange='{expected_exchange}'"
    configured_symbols = (
        settings.alpaca_crypto_allowed_symbols
        if command.asset_class == "crypto"
        else settings.alpaca_us_equity_allowed_symbols
    )
    allowed_symbols = {
        value.strip().upper().replace("-", "/")
        for value in configured_symbols.split(",")
        if value.strip()
    }
    if command.symbol not in allowed_symbols:
        return f"Alpaca {command.asset_class} symbol is not in its configured allowlist"
    if settings.runtime_mode == "mock":
        return "Alpaca execution requires paper or live runtime mode"
    if settings.runtime_mode == "live" and not settings.alpaca_live_trading_enabled:
        return "Alpaca live trading is disabled by configuration"
    expected_strategy_origin = (
        "alpaca_crypto_strategy"
        if command.asset_class == "crypto"
        else "alpaca_us_equity_strategy"
    )
    strategy_authorized_buy = (
        request_source == expected_strategy_origin
        or request_source.startswith(f"{expected_strategy_origin}:")
    )
    plan_authorized_buy = bool(
        plan_item is not None
        and (
            request_source.startswith("execution-job:")
            or request_source == "monitoring:intraday_target_stop_monitor"
        )
    )
    if command.side == "buy" and not (
        strategy_authorized_buy or plan_authorized_buy
    ):
        return "Alpaca buys require internal strategy risk authority"
    order_origin_error = _validate_trusted_order_origin(command=command, request_source=request_source)
    if order_origin_error is not None:
        return order_origin_error
    if (
        command.asset_class == "us_equity"
        and settings.runtime_mode == "live"
        and _is_alpaca_us_equity_strategy_order(request_source)
        and not settings.alpaca_us_equity_strategy_live_enabled
    ):
        return "Alpaca US-equity strategy live trading is disabled by configuration"
    config = adapter.describe()
    if not config.configured:
        return config.reason or "Alpaca provider readiness is uncertain"
    if plan_item is not None and (
        plan_item.provider != "alpaca"
        or plan_item.asset_class != command.asset_class
        or plan_item.venue != "alpaca"
    ):
        return "execution plan item must explicitly declare matching Alpaca provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and notional is None:
        return f"Alpaca {command.asset_class} buys require quote_notional or base_quantity with limit_price"
    max_notional = (
        settings.alpaca_crypto_max_order_notional_usd
        if command.asset_class == "crypto"
        else settings.alpaca_us_equity_max_order_notional_usd
    )
    if notional is not None and notional > max_notional:
        return f"Alpaca {command.asset_class} order exceeds its configured maximum notional"
    return None


def _validate_kis_us_equity_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> str | None:
    if command.provider != "kis" or command.asset_class != "us_equity":
        return None
    if adapter.provider != "kis":
        return "KIS US-equity execution requires provider='kis'"
    if not settings.kis_us_trading_enabled:
        return "KIS US-equity trading is disabled by configuration"
    if (command.venue or "").lower() != "kis":
        return "KIS US-equity execution requires venue='kis'"
    if command.exchange not in {"NASD", "NYSE", "AMEX"}:
        return "KIS US-equity execution requires NASD, NYSE, or AMEX exchange code"
    if command.order_type != "limit" or command.time_in_force != "day":
        return "KIS US-equity execution is limited to day limit orders"
    if command.quantity is None or command.limit_price is None:
        return "KIS US-equity execution requires whole-share quantity and limit_price"
    if plan_item is None:
        return "KIS US-equity execution requires an approved execution plan item"
    if plan_item.provider != "kis" or plan_item.asset_class != "us_equity" or plan_item.venue != "kis":
        return "execution plan item must explicitly declare KIS US-equity provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and (
        notional is None or notional > settings.kis_us_max_order_notional_usd
    ):
        return "KIS US-equity order exceeds its configured maximum USD notional"
    if settings.runtime_mode == "live":
        if not settings.kis_us_live_trading_enabled:
            return "KIS US-equity live trading is disabled by configuration"
        if settings.kis_us_live_confirmation_token != "ENABLE_KIS_US_LIVE":
            return "KIS US-equity live confirmation token is missing"
        allowed_accounts = {
            value.strip()
            for value in settings.kis_us_account_allowlist.split(",")
            if value.strip()
        }
        if not settings.kis_account_number or settings.kis_account_number not in allowed_accounts:
            return "KIS US-equity live account is not in the configured allowlist"
    return None


def _kis_us_session_route(command: PlaceOrderRequest) -> dict[str, object] | None:
    if command.provider != "kis" or command.asset_class != "us_equity":
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    observed_at = _route_now(metadata)
    local = observed_at.astimezone(ZoneInfo("America/New_York"))
    if is_us_regular_session(local):
        return None
    next_open = next_us_regular_open(observed_at)
    return {
        "action": "defer",
        "exchange": command.exchange,
        "session": "us_closed",
        "ord_dvsn": None,
        "effective_order_type": command.order_type,
        "price_source": "command_limit_price",
        "next_retry_at": next_open.isoformat(),
        "reason": (
            "deferred KIS US-equity order outside New York regular session "
            f"until {next_open.isoformat()}"
        ),
    }


def _validate_trusted_order_origin(*, command: PlaceOrderRequest, request_source: str) -> str | None:
    if not request_source.startswith("alpaca_"):
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    expected_origin = request_source.split(":", 1)[0]
    if expected_origin not in {"alpaca_crypto_strategy", "alpaca_us_equity_strategy"}:
        return f"unrecognized Alpaca autonomous order source: {expected_origin}"
    if metadata.get("autonomous_strategy_order") is not True:
        return "Alpaca autonomous strategy orders must carry explicit strategy origin metadata"
    if metadata.get("order_origin") != expected_origin:
        return "Alpaca autonomous strategy order origin metadata does not match request source"
    if command.asset_class == "us_equity" and expected_origin != "alpaca_us_equity_strategy":
        return "Alpaca US-equity strategy orders must use the US-equity strategy origin"
    if command.asset_class == "crypto" and expected_origin != "alpaca_crypto_strategy":
        return "Alpaca crypto strategy orders must use the crypto strategy origin"
    return None


def _is_alpaca_us_equity_strategy_order(request_source: str) -> bool:
    return request_source == "alpaca_us_equity_strategy" or request_source.startswith(
        "alpaca_us_equity_strategy:"
    )


def _is_trusted_alpaca_strategy_source(request_source: str) -> bool:
    return any(
        request_source == origin or request_source.startswith(f"{origin}:")
        for origin in ("alpaca_crypto_strategy", "alpaca_us_equity_strategy")
    )


_CANONICAL_EXECUTION_SCOPE_KEYS = frozenset(
    {
        "policy_version",
        "provider",
        "account_scope",
        "venue",
        "asset_class",
        "exchange",
    }
)


def _execution_scope_route(payload: object) -> FifoRouteScope | None:
    if not isinstance(payload, dict) or set(payload) != _CANONICAL_EXECUTION_SCOPE_KEYS:
        return None
    if payload.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION:
        return None
    account_scope = payload.get("account_scope")
    if (
        not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
    ):
        return None
    route_values = {
        key: payload.get(key)
        for key in ("provider", "venue", "asset_class", "exchange")
    }
    if any(
        not isinstance(value, str) or not value.strip()
        for value in route_values.values()
    ):
        return None
    try:
        route = FifoRouteScope(
            provider=str(route_values["provider"]),
            account_scope=account_scope,
            venue=str(route_values["venue"]),
            asset_class=str(route_values["asset_class"]),
            exchange=str(route_values["exchange"]),
        )
    except ValueError:
        return None
    if any(
        payload[field_name] != getattr(route, field_name)
        for field_name in ("provider", "account_scope", "venue", "asset_class", "exchange")
    ):
        return None
    return route


def _trusted_strategy_execution_scope(
    *,
    command: PlaceOrderRequest,
    request_source: str,
) -> dict[str, object] | None:
    """Return only a trusted strategy's exact, route-consistent scope claim."""
    if not _is_trusted_alpaca_strategy_source(request_source):
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    expected_origin = request_source.split(":", 1)[0]
    if (
        metadata.get("autonomous_strategy_order") is not True
        or metadata.get("order_origin") != expected_origin
    ):
        return None
    payload = metadata.get("expected_execution_scope")
    route = _execution_scope_route(payload)
    if route is None:
        return None
    expected_route = {
        "provider": str(command.provider or "").strip().lower(),
        "venue": str(command.venue or "").strip().upper(),
        "asset_class": str(command.asset_class or "").strip().lower(),
        "exchange": str(command.exchange or "").strip().upper(),
    }
    if (
        route.provider != "alpaca"
        or any(
            getattr(route, field_name) != expected_value
            for field_name, expected_value in expected_route.items()
        )
    ):
        return None
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": route.provider,
        "account_scope": route.account_scope,
        "venue": route.venue,
        "asset_class": route.asset_class,
        "exchange": route.exchange,
    }


def _select_local_orders_for_execution_scope(
    *,
    orders: list[BrokerOrder],
    execution_scope: dict[str, object] | None,
    settlement_currency: str | None = None,
) -> ScopedLocalOrderSelection:
    expected_scope = _execution_scope_route(execution_scope)
    matching_orders: list[BrokerOrder] = []
    for order in orders:
        metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
        stored_scope = _execution_scope_route(metadata.get("execution_scope"))
        persisted_scope = fifo_route_scope_from_order(order)
        if (
            expected_scope is None
            or stored_scope is None
            or persisted_scope is None
            or stored_scope != persisted_scope
        ):
            return ScopedLocalOrderSelection(
                matching_orders=tuple(matching_orders),
                indeterminate_order=order,
            )
        if settlement_currency is None:
            scope_matches = stored_scope == expected_scope
        elif (
            stored_scope.provider == expected_scope.provider
            and stored_scope.account_scope == expected_scope.account_scope
        ):
            order_currency = settlement_currency_for_order(order)
            if order_currency is None:
                return ScopedLocalOrderSelection(
                    matching_orders=tuple(matching_orders),
                    indeterminate_order=order,
                )
            scope_matches = order_currency == settlement_currency
        else:
            scope_matches = False
        if scope_matches:
            matching_orders.append(order)
    return ScopedLocalOrderSelection(matching_orders=tuple(matching_orders))


def _select_local_orders_for_fifo_inventory_scope(
    *,
    orders: list[BrokerOrder],
    execution_scope: dict[str, object] | None,
) -> ScopedLocalOrderSelection:
    """Select physical-inventory peers without relaxing persisted route proof.

    Each order must still contain one internally consistent exact execution
    scope.  Only after that proof succeeds do KIS domestic order routes fold
    into their shared account inventory.  Non-KIS and overseas routes retain
    exact-route matching.
    """

    expected_exact_scope = _execution_scope_route(execution_scope)
    expected_inventory_scope = (
        canonical_fifo_inventory_route_scope(expected_exact_scope)
        if expected_exact_scope is not None
        else None
    )
    matching_orders: list[BrokerOrder] = []
    for order in orders:
        metadata = (
            order.request_metadata
            if isinstance(order.request_metadata, dict)
            else {}
        )
        stored_exact_scope = _execution_scope_route(metadata.get("execution_scope"))
        persisted_exact_scope = fifo_route_scope_from_order(order)
        if (
            expected_exact_scope is None
            or expected_inventory_scope is None
            or stored_exact_scope is None
            or persisted_exact_scope is None
            or stored_exact_scope != persisted_exact_scope
        ):
            return ScopedLocalOrderSelection(
                matching_orders=tuple(matching_orders),
                indeterminate_order=order,
            )
        if (
            canonical_fifo_inventory_route_scope(stored_exact_scope)
            == expected_inventory_scope
        ):
            matching_orders.append(order)
    return ScopedLocalOrderSelection(matching_orders=tuple(matching_orders))


def _fifo_inventory_position_exchanges(
    route_scope: FifoRouteScope,
) -> tuple[str, ...]:
    inventory_scope = canonical_fifo_inventory_route_scope(route_scope)
    if (
        inventory_scope.provider == "kis"
        and inventory_scope.asset_class == "stock"
        and inventory_scope.exchange == "KRX"
        and route_scope.exchange in KRX_EXCHANGES
    ):
        return tuple(sorted(KRX_EXCHANGES))
    return (route_scope.exchange,)


def _reservation_scope_indeterminate_result(
    order: BrokerOrder,
) -> PlaceBrokerOrderResult:
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": (
                "an active local order lacks a proven exact execution scope; "
                "reconcile or quarantine it before creating another order"
            ),
            "block_reason": "order_reservation_scope_indeterminate",
            "prior_order_id": order.id,
            "prior_order_status": order.status,
        },
    )


def _load_unresolved_prior_submission(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
    plan_item: ExecutionPlanItem | None = None,
    request_source: str = "post:/orders",
) -> ScopedLocalOrderSelection:
    """Keep a new key from bypassing an order whose broker outcome is unknown."""
    orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == adapter.provider,
            BrokerOrder.symbol == command.symbol,
            BrokerOrder.side == command.side,
            BrokerOrder.status.in_(
                (
                    NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
                    NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
                )
            ),
        )
        .options(selectinload(BrokerOrder.plan_item))
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
    ).all()
    selection = _select_local_orders_for_execution_scope(
        orders=list(orders),
        execution_scope=execution_scope,
    )
    if selection.indeterminate_order is not None:
        return selection
    return ScopedLocalOrderSelection(
        matching_orders=tuple(
            order
            for order in selection.matching_orders
            if _unresolved_order_conflicts_with_strategy_identity(
                adapter=adapter,
                candidate_plan_item=plan_item,
                prior_order=order,
                command=command,
                request_source=request_source,
            )
        )
    )


def _unresolved_order_conflicts_with_strategy_identity(
    *,
    adapter: BrokerAdapter,
    candidate_plan_item: ExecutionPlanItem | None,
    prior_order: BrokerOrder,
    command: PlaceOrderRequest,
    request_source: str,
) -> bool:
    if _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=command,
        plan_item=candidate_plan_item,
        request_source=request_source,
    ):
        candidate_identity = _strategy_entry_family_identity(candidate_plan_item)
        prior_identity, prior_identity_error = (
            _validated_order_strategy_family_identity(prior_order)
        )
        if (
            candidate_identity is None
            or prior_identity_error is not None
            or prior_identity is None
        ):
            return True
        return bool(
            prior_identity["decision_lane"]
            == candidate_identity["decision_lane"]
        )
    if not _is_automated_monitoring_order(
        command=command,
        plan_item=candidate_plan_item,
        request_source=request_source,
    ):
        return True
    candidate_identity = _strategy_entry_family_identity(candidate_plan_item)
    prior_identity = _strategy_entry_family_identity(prior_order.plan_item)
    if candidate_identity is None or prior_identity is None:
        return True
    return prior_identity == candidate_identity


def _local_capital_reserving_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    side: str,
    execution_scope: dict[str, object] | None,
    symbol: str | None = None,
    settlement_currency: str | None = None,
    exclude_order_id: int | None = None,
    fifo_inventory_scope: bool = False,
) -> ScopedLocalOrderSelection:
    query = select(BrokerOrder).where(
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == adapter.provider,
        BrokerOrder.side == side,
        BrokerOrder.status.in_(tuple(CAPITAL_RESERVING_ORDER_STATUSES)),
    )
    if symbol is not None:
        query = query.where(BrokerOrder.symbol == symbol)
    if exclude_order_id is not None:
        query = query.where(BrokerOrder.id != exclude_order_id)
    orders = db.scalars(query.order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())).all()
    if fifo_inventory_scope:
        if settlement_currency is not None:
            raise ValueError(
                "FIFO inventory order selection cannot use currency-only scope"
            )
        return _select_local_orders_for_fifo_inventory_scope(
            orders=list(orders),
            execution_scope=execution_scope,
        )
    return _select_local_orders_for_execution_scope(
        orders=list(orders),
        execution_scope=execution_scope,
        settlement_currency=settlement_currency,
    )


def _command_settlement_currency(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> str | None:
    provider = str(adapter.provider or command.provider or "").strip().lower()
    asset_class = str(command.asset_class or "").strip().lower()
    exchange = str(command.exchange or command.venue or "").strip().upper()
    symbol = str(command.symbol or "").strip().upper()
    if provider == "alpaca" or asset_class == "us_equity":
        return "USD"
    if provider == "upbit":
        quote = symbol.split("-", 1)[0] if "-" in symbol else ""
        return quote or None
    if provider == "kis":
        if exchange in {"AMEX", "NASD", "NYSE", "US"}:
            return "USD"
        if exchange in {"KRX", "NXT", "SOR"}:
            return "KRW"
        if asset_class in {"stock", "kr_equity", "domestic_equity"}:
            return "KRW"
    return None


def _same_order_identity(
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


def _local_order_notional(order: BrokerOrder) -> Decimal | None:
    if order.quote_notional is not None and order.quote_notional > 0:
        return Decimal(order.quote_notional)
    if order.limit_price is None or order.limit_price <= 0:
        return None
    if order.base_quantity is not None and order.base_quantity > 0:
        return Decimal(order.base_quantity) * Decimal(order.limit_price)
    if order.quantity is not None and order.quantity > 0:
        return Decimal(order.quantity) * Decimal(order.limit_price)
    return None


def _local_unmatched_buy_reservation(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    snapshot: BrokerAccountSnapshot,
    request_source: str,
    execution_scope: dict[str, object] | None,
    exclude_order_id: int | None = None,
) -> tuple[Decimal, int, BrokerOrder | None]:
    resolution, indeterminate_order = _scoped_open_buy_reservation(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        snapshot=snapshot,
        request_source=request_source,
        execution_scope=execution_scope,
        exclude_order_id=exclude_order_id,
    )
    if indeterminate_order is not None:
        return Decimal("0"), 0, indeterminate_order
    assert resolution is not None
    return (
        resolution.local_unmatched_notional,
        resolution.local_unknown_count,
        None,
    )


def _scoped_open_buy_reservation(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    snapshot: BrokerAccountSnapshot,
    request_source: str,
    execution_scope: dict[str, object] | None,
    exclude_order_id: int | None = None,
) -> tuple[OpenBuyReservationResolution | None, BrokerOrder | None]:
    retry_source_order_pk = _trusted_retry_source_order_pk(
        command=command,
        request_source=request_source,
    )
    selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="buy",
        execution_scope=execution_scope,
        settlement_currency=_command_settlement_currency(
            adapter=adapter,
            command=command,
        ),
        exclude_order_id=exclude_order_id,
    )
    if selection.indeterminate_order is not None:
        return None, selection.indeterminate_order
    local_orders = tuple(
        order
        for order in selection.matching_orders
        if order.id != retry_source_order_pk
    )
    return (
        _resolve_open_buy_reservations(
            snapshot=snapshot,
            local_orders=local_orders,
        ),
        None,
    )


def _requires_automated_kis_symbol_exposure_boundary(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> bool:
    return _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    )


def _load_automated_kis_symbol_exposure_snapshot(
    *,
    adapter: BrokerAdapter,
    access_token: str,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> tuple[BrokerAccountSnapshot | None, str | None]:
    if not _requires_automated_kis_symbol_exposure_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None, None
    try:
        return (
            adapter.get_account_snapshot(
                access_token,
                symbols=[command.symbol],
                exchange=command.exchange or "KRX",
            ),
            None,
        )
    except Exception as exc:
        return None, safe_broker_error_message(exc)


def _requires_automated_kis_opposite_order_boundary(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None,
) -> bool:
    if adapter.provider != "kis":
        return False
    return bool(
        _is_trusted_automated_watch_buy(
            adapter=adapter,
            command=command,
            plan_item=plan_item,
            request_source=request_source,
        )
        or _is_trusted_automated_family_sell(
            command=command,
            plan_item=plan_item,
            request_source=request_source,
            retry_lineage_proof=retry_lineage_proof,
        )
    )


def _load_automated_kis_opposite_order_snapshot(
    *,
    adapter: BrokerAdapter,
    access_token: str,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None,
) -> tuple[BrokerAccountSnapshot | None, str | None]:
    if not _requires_automated_kis_opposite_order_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=retry_lineage_proof,
    ):
        return None, None
    try:
        return (
            adapter.get_account_snapshot(
                access_token,
                symbols=[command.symbol],
                exchange=command.exchange or "KRX",
            ),
            None,
        )
    except Exception as exc:
        return None, safe_broker_error_message(exc)


def _holding_current_market_value(holding: BrokerAccountHolding) -> Decimal | None:
    if isinstance(holding.quantity, bool):
        return None
    if holding.quantity <= 0:
        return Decimal("0") if holding.quantity == 0 else None
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    for key in ("market_value", "valuation", "evaluated_amount", "evlu_amt"):
        value = _positive_decimal(payload.get(key))
        if value is not None:
            return value
    for key in ("current_price", "prpr", "now_pric", "cur_prc", "stck_prpr"):
        price = _positive_decimal(payload.get(key))
        if price is not None:
            return Decimal(holding.quantity) * price
    return None


def _snapshot_symbol_holding_exposure(
    *,
    snapshot: BrokerAccountSnapshot,
    symbol: str,
) -> tuple[Decimal, int]:
    normalized_symbol = symbol.strip().upper()
    holdings_value = Decimal("0")
    unknown_count = 0
    for holding in snapshot.holdings:
        if holding.symbol.strip().upper() != normalized_symbol:
            continue
        value = _holding_current_market_value(holding)
        if value is None or not value.is_finite() or value < 0:
            unknown_count += 1
        else:
            holdings_value += value
    return holdings_value, unknown_count


def _automated_kis_symbol_exposure_reservation_payload(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> dict[str, object] | None:
    if not _requires_automated_kis_symbol_exposure_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None
    quantity = command.quantity
    notional = _order_notional(command=command, plan_item=plan_item)
    if (
        isinstance(quantity, bool)
        or quantity is None
        or quantity <= 0
        or notional is None
        or not notional.is_finite()
        or notional <= 0
    ):
        return None
    unit_price = notional / Decimal(quantity)
    if not unit_price.is_finite() or unit_price <= 0:
        return None
    return {
        "policy_version": 1,
        "provider": command.provider,
        "exchange": command.exchange,
        "symbol": command.symbol,
        "side": command.side,
        "execution_plan_item_id": command.execution_plan_item_id,
        "idempotency_key": command.idempotency_key,
        "quantity": quantity,
        "unit_price": str(unit_price),
        "reserved_notional": str(notional),
    }


def _persisted_local_buy_reservation_unit_price(order: BrokerOrder) -> Decimal | None:
    request_metadata = (
        order.request_metadata if isinstance(order.request_metadata, dict) else {}
    )
    reservation = request_metadata.get(SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY)
    if not isinstance(reservation, dict) or reservation.get("policy_version") != 1:
        return None
    quantity = reservation.get("quantity")
    if (
        reservation.get("provider") != order.broker_name
        or str(reservation.get("symbol") or "").strip().upper()
        != order.symbol.strip().upper()
        or reservation.get("side") != "buy"
        or reservation.get("execution_plan_item_id") != order.execution_plan_item_id
        or reservation.get("idempotency_key") != order.idempotency_key
        or isinstance(quantity, bool)
        or not isinstance(quantity, int)
        or order.quantity != quantity
        or quantity <= 0
    ):
        return None
    unit_price = _positive_decimal(reservation.get("unit_price"))
    notional = _positive_decimal(reservation.get("reserved_notional"))
    if (
        unit_price is None
        or notional is None
        or unit_price * Decimal(quantity) != notional
    ):
        return None
    return unit_price


def _local_order_remaining_buy_notional(order: BrokerOrder) -> Decimal | None:
    requested_quote_notional = _positive_decimal(order.quote_notional)
    if (
        requested_quote_notional is not None
        and order.quantity is None
        and order.base_quantity is None
    ):
        filled_quote_notional = Decimal("0")
        for fill in dedupe_execution_fills(list(order.fills)):
            if str(fill.side or "").strip().lower() != "buy":
                return None
            fill_quote_notional = _positive_decimal(fill.quote_notional)
            if fill_quote_notional is None:
                return None
            filled_quote_notional += fill_quote_notional
        remaining_quote_notional = (
            requested_quote_notional - filled_quote_notional
        )
        if remaining_quote_notional < 0:
            return None
        return remaining_quote_notional

    requested: Decimal | None = None
    require_integral_quantity = False
    if order.quantity is not None and order.quantity > 0:
        requested = Decimal(order.quantity)
        require_integral_quantity = True
    elif order.base_quantity is not None and order.base_quantity > 0:
        requested = Decimal(order.base_quantity)
    if requested is None or not requested.is_finite() or requested <= 0:
        return None
    if (
        require_integral_quantity
        and requested != requested.to_integral_value(rounding=ROUND_DOWN)
    ):
        return None
    filled = Decimal("0")
    for fill in dedupe_execution_fills(list(order.fills)):
        if str(fill.side or "").strip().lower() != "buy":
            return None
        quantity = effective_fill_quantity(fill)
        if not quantity.is_finite() or quantity <= 0:
            return None
        if (
            require_integral_quantity
            and quantity != quantity.to_integral_value(rounding=ROUND_DOWN)
        ):
            return None
        filled += quantity
    remaining = requested - filled
    if remaining < 0:
        return None
    if remaining == 0:
        return Decimal("0")
    persisted_price = _persisted_local_buy_reservation_unit_price(order)
    price = _positive_decimal(order.limit_price)
    if price is not None and persisted_price is not None and price != persisted_price:
        return None
    if price is None:
        price = persisted_price
    if price is None:
        return None
    return remaining * price


def _snapshot_open_order_remaining_quantity(
    order: BrokerAccountOpenOrder,
) -> Decimal | None:
    payload = order.payload if isinstance(order.payload, dict) else {}
    raw_quantity: object
    if "remaining_base_quantity" in payload:
        raw_quantity = payload.get("remaining_base_quantity")
    else:
        raw_quantity = (
            order.remaining_quantity
            if order.remaining_quantity is not None
            else order.quantity
        )
    if isinstance(raw_quantity, bool):
        return None
    try:
        remaining = Decimal(str(raw_quantity).strip())
    except (DecimalException, TypeError, ValueError):
        return None
    if not remaining.is_finite() or remaining < 0:
        return None
    return remaining


def _order_identity_claims_overlap(
    *,
    local: BrokerOrder,
    snapshot: BrokerAccountOpenOrder,
) -> bool:
    return bool(
        (
            local.broker_order_id
            and snapshot.broker_order_id
            and local.broker_order_id == snapshot.broker_order_id
        )
        or (
            local.idempotency_key
            and snapshot.idempotency_key
            and local.idempotency_key == snapshot.idempotency_key
        )
    )


def _order_identity_claims_conflict(
    *,
    local: BrokerOrder,
    snapshot: BrokerAccountOpenOrder,
) -> bool:
    return bool(
        (
            local.broker_order_id
            and snapshot.broker_order_id
            and local.broker_order_id != snapshot.broker_order_id
        )
        or (
            local.idempotency_key
            and snapshot.idempotency_key
            and local.idempotency_key != snapshot.idempotency_key
        )
    )


def _matched_local_open_buy_reservation_unit_price(
    *,
    local: BrokerOrder,
    snapshot: BrokerAccountOpenOrder,
    remaining: Decimal,
) -> Decimal | None:
    if not _order_identity_claims_overlap(local=local, snapshot=snapshot):
        return None
    if _order_identity_claims_conflict(local=local, snapshot=snapshot):
        return None
    if (
        str(local.symbol or "").strip().upper()
        != str(snapshot.symbol or "").strip().upper()
        or str(local.side or "").strip().lower() != "buy"
        or str(snapshot.side or "").strip().lower() != "buy"
        or isinstance(snapshot.quantity, bool)
        or not isinstance(snapshot.quantity, int)
        or snapshot.quantity <= 0
        or local.quantity != snapshot.quantity
        or remaining != remaining.to_integral_value(rounding=ROUND_DOWN)
        or remaining > Decimal(snapshot.quantity)
    ):
        return None
    persisted_price = _persisted_local_buy_reservation_unit_price(local)
    if persisted_price is None:
        return None
    local_limit_price = _positive_decimal(local.limit_price)
    if local_limit_price is not None and local_limit_price != persisted_price:
        return None
    return persisted_price


def _resolve_open_buy_reservations(
    *,
    snapshot: BrokerAccountSnapshot,
    local_orders: tuple[BrokerOrder, ...],
    symbol: str | None = None,
) -> OpenBuyReservationResolution:
    normalized_symbol = symbol.strip().upper() if symbol is not None else None
    broker_notional = Decimal("0")
    local_notional = Decimal("0")
    broker_unknown_count = 0
    local_unknown_count = 0
    matched_local_ids: set[str] = set()

    for broker_order in snapshot.open_orders:
        if str(broker_order.side or "").strip().lower() != "buy":
            continue
        if (
            normalized_symbol is not None
            and str(broker_order.symbol or "").strip().upper()
            != normalized_symbol
        ):
            continue
        matching_local_orders = [
            local
            for local in local_orders
            if _order_identity_claims_overlap(
                local=local,
                snapshot=broker_order,
            )
        ]
        identity_claims_conflict = any(
            _order_identity_claims_conflict(
                local=local,
                snapshot=broker_order,
            )
            for local in matching_local_orders
        )
        reuses_matched_local = any(
            order.id in matched_local_ids for order in matching_local_orders
        )
        remaining = _snapshot_open_order_remaining_quantity(broker_order)
        if identity_claims_conflict:
            if remaining is None or remaining > 0:
                broker_unknown_count += 1
            continue
        matched_local_ids.update(order.id for order in matching_local_orders)
        if remaining is None:
            broker_unknown_count += 1
            continue
        if len(matching_local_orders) > 1 or reuses_matched_local:
            broker_unknown_count += 1
            continue
        matched_local = (
            matching_local_orders[0] if matching_local_orders else None
        )
        if remaining == 0:
            continue

        broker_price = _positive_decimal(broker_order.limit_price)
        if broker_price is None:
            if matched_local is None:
                broker_unknown_count += 1
                continue
            broker_price = _matched_local_open_buy_reservation_unit_price(
                local=matched_local,
                snapshot=broker_order,
                remaining=remaining,
            )
            if broker_price is None:
                broker_unknown_count += 1
                continue
        elif matched_local is not None:
            local_price = _positive_decimal(matched_local.limit_price)
            persisted_price = _persisted_local_buy_reservation_unit_price(
                matched_local
            )
            if (
                (local_price is not None and local_price != broker_price)
                or (
                    persisted_price is not None
                    and persisted_price != broker_price
                )
            ):
                broker_unknown_count += 1
                continue
        broker_notional += remaining * broker_price

    for local_order in local_orders:
        if local_order.id in matched_local_ids:
            continue
        notional = _local_order_remaining_buy_notional(local_order)
        if notional is None:
            local_unknown_count += 1
        else:
            local_notional += notional

    return OpenBuyReservationResolution(
        broker_open_notional=broker_notional,
        local_unmatched_notional=local_notional,
        broker_unknown_count=broker_unknown_count,
        local_unknown_count=local_unknown_count,
    )


def _self_cross_local_order_remaining_quantity(
    order: BrokerOrder,
) -> Decimal | None:
    requested = order.quantity
    if (
        isinstance(requested, bool)
        or not isinstance(requested, int)
        or requested <= 0
    ):
        return None
    raw_fills = list(order.fills)
    if execution_fills_have_relevant_conflicting_identity(
        raw_fills,
        is_relevant=lambda _fill: True,
    ):
        return None
    filled = Decimal("0")
    try:
        for fill in dedupe_execution_fills(raw_fills):
            if str(fill.side or "").strip().lower() != order.side:
                return None
            quantity = effective_fill_quantity(fill)
            if (
                not quantity.is_finite()
                or quantity <= 0
                or quantity != quantity.to_integral_value(rounding=ROUND_DOWN)
            ):
                return None
            filled += quantity
    except (DecimalException, TypeError, ValueError):
        return None
    remaining = Decimal(requested) - filled
    if (
        remaining < 0
        or remaining != remaining.to_integral_value(rounding=ROUND_DOWN)
    ):
        return None
    return remaining


def _automated_kis_opposite_order_boundary_failure(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None,
    execution_scope: dict[str, object] | None,
    snapshot: BrokerAccountSnapshot | None,
    snapshot_error: str | None,
    exclude_order_id: str,
    observed_at: datetime,
    operation_reference_at: datetime,
) -> AutomatedKisOppositeOrderFailure | None:
    """Fence only opposite orders that can trade against this automated order.

    The strategy lanes own inventory independently, but the broker account can
    still self-match two orders for the same physical KIS domestic position.
    This final boundary therefore folds only KRX/NXT/SOR inventory routes for
    the authenticated account, then permits a proven non-crossing pair while
    failing closed on crossing, market, or indeterminate opposite-order truth.
    """

    if not _requires_automated_kis_opposite_order_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=retry_lineage_proof,
    ):
        return None

    def indeterminate(
        message: str,
        **metrics: object,
    ) -> AutomatedKisOppositeOrderFailure:
        return AutomatedKisOppositeOrderFailure(
            block_reason="opposite_order_truth_indeterminate",
            message=message,
            metrics={"truth_complete": False, **metrics},
        )

    if snapshot_error is not None or snapshot is None:
        return indeterminate(
            "automated KIS order cannot verify opposite-side broker orders",
            snapshot_error=snapshot_error,
        )
    fresh_at_receipt = snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=observed_at,
        settings=settings,
    )
    fresh_at_operation = snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=operation_reference_at,
        settings=settings,
    )
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    aggregated_open_order_errors = snapshot_payload.get("open_orders_errors")
    if not (fresh_at_receipt or fresh_at_operation):
        return indeterminate(
            "automated KIS opposite-order snapshot is stale or future-dated"
        )
    if (
        not snapshot.open_orders_complete
        or (
            isinstance(snapshot_payload.get("open_orders_error"), dict)
            and snapshot_payload["open_orders_error"].get("degraded") is True
        )
        or (
            isinstance(aggregated_open_order_errors, list)
            and any(
                isinstance(error, dict) and error.get("degraded") is True
                for error in aggregated_open_order_errors
            )
        )
    ):
        return indeterminate(
            "automated KIS order requires complete broker open-order truth"
        )

    opposite_side = "sell" if command.side == "buy" else "buy"
    selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side=opposite_side,
        execution_scope=execution_scope,
        symbol=command.symbol,
        exclude_order_id=exclude_order_id,
        fifo_inventory_scope=True,
    )
    if selection.indeterminate_order is not None:
        return indeterminate(
            "an active opposite-side order lacks a proven account inventory scope",
            prior_order_id=selection.indeterminate_order.id,
        )
    local_orders = selection.matching_orders
    normalized_symbol = command.symbol.strip().upper()
    broker_orders = tuple(
        broker_order
        for broker_order in snapshot.open_orders
        if str(broker_order.side or "").strip().lower() == opposite_side
        and str(broker_order.symbol or "").strip().upper() == normalized_symbol
    )

    candidate_price = _positive_decimal(command.limit_price)
    candidate_is_market = str(command.order_type or "").strip().lower() == "market"
    if not candidate_is_market and candidate_price is None and (
        broker_orders or local_orders
    ):
        return indeterminate(
            "the final automated KIS order has no exact executable price"
        )

    matched_local_ids: set[str] = set()
    seen_broker_identity_claims: set[tuple[str, str]] = set()
    crossing_order_count = 0
    market_or_unknown_price_count = 0
    active_broker_order_count = 0
    active_local_only_order_count = 0

    def prices_cross(opposite_price: Decimal | None) -> bool:
        if candidate_is_market or opposite_price is None:
            return True
        assert candidate_price is not None
        if command.side == "buy":
            return candidate_price >= opposite_price
        return opposite_price >= candidate_price

    for broker_order in broker_orders:
        remaining = _snapshot_open_order_remaining_quantity(broker_order)
        if (
            isinstance(broker_order.quantity, bool)
            or not isinstance(broker_order.quantity, int)
            or broker_order.quantity <= 0
            or remaining is None
            or remaining != remaining.to_integral_value(rounding=ROUND_DOWN)
            or remaining > Decimal(broker_order.quantity)
        ):
            return indeterminate(
                "an opposite-side broker order has unverifiable remaining quantity"
            )
        if remaining == 0:
            continue
        active_broker_order_count += 1
        identity_claims = {
            ("broker_order_id", str(broker_order.broker_order_id).strip())
            for _present in [broker_order.broker_order_id]
            if _present and str(_present).strip()
        } | {
            ("idempotency_key", str(broker_order.idempotency_key).strip())
            for _present in [broker_order.idempotency_key]
            if _present and str(_present).strip()
        }
        if not identity_claims:
            return indeterminate(
                "an opposite-side broker order has no durable identity"
            )
        if seen_broker_identity_claims.intersection(identity_claims):
            return indeterminate(
                "opposite-side broker order identity is duplicated"
            )
        seen_broker_identity_claims.update(identity_claims)

        matching_local_orders = [
            local
            for local in local_orders
            if _order_identity_claims_overlap(local=local, snapshot=broker_order)
        ]
        if len(matching_local_orders) > 1 or any(
            local.id in matched_local_ids for local in matching_local_orders
        ):
            return indeterminate(
                "opposite-side broker identity cannot be matched one-to-one locally"
            )
        matching_local = (
            matching_local_orders[0] if matching_local_orders else None
        )
        broker_price = _positive_decimal(broker_order.limit_price)
        if matching_local is not None:
            if (
                (
                    matching_local.broker_order_id
                    and broker_order.broker_order_id
                    and matching_local.broker_order_id
                    != broker_order.broker_order_id
                )
                or (
                    matching_local.idempotency_key
                    and broker_order.idempotency_key
                    and matching_local.idempotency_key
                    != broker_order.idempotency_key
                )
            ):
                return indeterminate(
                    "opposite-side broker and local identity claims conflict"
                )
            local_remaining = _self_cross_local_order_remaining_quantity(
                matching_local
            )
            if local_remaining is None or local_remaining != remaining:
                return indeterminate(
                    "opposite-side broker and local remaining quantities conflict"
                )
            local_price = _positive_decimal(matching_local.limit_price)
            if (
                broker_price is not None
                and local_price is not None
                and broker_price != local_price
            ):
                return indeterminate(
                    "opposite-side broker and local executable prices conflict"
                )
            matched_local_ids.add(matching_local.id)

        if broker_price is None:
            market_or_unknown_price_count += 1
        if prices_cross(broker_price):
            crossing_order_count += 1

    for local_order in local_orders:
        if local_order.id in matched_local_ids:
            continue
        if not str(local_order.idempotency_key or "").strip():
            return indeterminate(
                "an unmatched local opposite-side order has no durable identity"
            )
        remaining = _self_cross_local_order_remaining_quantity(local_order)
        if remaining is None:
            return indeterminate(
                "an unmatched local opposite-side order has unverifiable remaining quantity",
                prior_order_id=local_order.id,
            )
        if remaining == 0:
            continue
        active_local_only_order_count += 1
        local_price = _positive_decimal(local_order.limit_price)
        if local_price is None:
            market_or_unknown_price_count += 1
        if prices_cross(local_price):
            crossing_order_count += 1

    metrics: dict[str, object] = {
        "truth_complete": True,
        "candidate_side": command.side,
        "opposite_side": opposite_side,
        "broker_active_order_count": active_broker_order_count,
        "local_unmatched_active_order_count": active_local_only_order_count,
        "crossing_order_count": crossing_order_count,
        "market_or_unknown_price_count": market_or_unknown_price_count,
    }
    if crossing_order_count == 0:
        return None
    return AutomatedKisOppositeOrderFailure(
        block_reason="opposite_order_self_cross_risk",
        message=(
            "automated KIS order was deferred because an active opposite-side "
            "order could self-match in the same account"
        ),
        metrics=metrics,
    )


def _evaluate_automated_kis_buy_symbol_exposure_boundary(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    execution_scope: dict[str, object] | None,
    snapshot: BrokerAccountSnapshot | None,
    snapshot_error: str | None,
    exclude_order_id: str,
    observed_at: datetime,
    operation_reference_at: datetime,
) -> AutomatedKisBuySymbolExposureFailure | None:
    if not _requires_automated_kis_symbol_exposure_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None

    def indeterminate(message: str, **metrics: object) -> AutomatedKisBuySymbolExposureFailure:
        return AutomatedKisBuySymbolExposureFailure(
            block_reason="symbol_exposure_indeterminate",
            message=message,
            metrics={"unknown_exposure": True, **metrics},
        )

    if snapshot_error is not None or snapshot is None:
        return indeterminate(
            "automated KIS buy cannot verify final symbol exposure from the broker",
            snapshot_error=snapshot_error,
        )
    fresh_at_receipt = snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=observed_at,
        settings=settings,
    )
    fresh_at_operation = snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=operation_reference_at,
        settings=settings,
    )
    if not (fresh_at_receipt or fresh_at_operation):
        return indeterminate("automated KIS buy account snapshot is stale or future-dated")
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    aggregated_open_order_errors = snapshot_payload.get("open_orders_errors")
    if (
        not snapshot.holdings_complete
        or not snapshot.open_orders_complete
        or (
            isinstance(snapshot_payload.get("open_orders_error"), dict)
            and snapshot_payload["open_orders_error"].get("degraded") is True
        )
        or (
            isinstance(aggregated_open_order_errors, list)
            and any(
                isinstance(error, dict) and error.get("degraded") is True
                for error in aggregated_open_order_errors
            )
        )
    ):
        return indeterminate(
            "automated KIS buy requires complete holdings and open-order truth"
        )

    selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="buy",
        execution_scope=execution_scope,
        symbol=command.symbol,
        settlement_currency="KRW",
        exclude_order_id=exclude_order_id,
    )
    if selection.indeterminate_order is not None:
        return indeterminate(
            "an active symbol buy lacks a proven exact execution scope",
            prior_order_id=selection.indeterminate_order.id,
        )
    reservation = _resolve_open_buy_reservations(
        snapshot=snapshot,
        local_orders=selection.matching_orders,
        symbol=command.symbol,
    )
    holdings_value, holdings_unknown_count = _snapshot_symbol_holding_exposure(
        snapshot=snapshot,
        symbol=command.symbol,
    )
    broker_open_value = reservation.broker_open_notional
    local_open_value = reservation.local_unmatched_notional
    snapshot_unknown_count = (
        holdings_unknown_count + reservation.broker_unknown_count
    )
    local_unknown_count = reservation.local_unknown_count

    candidate_value = _order_notional(command=command, plan_item=plan_item)
    if (
        candidate_value is None
        or not candidate_value.is_finite()
        or candidate_value <= 0
        or snapshot_unknown_count > 0
        or local_unknown_count > 0
    ):
        return indeterminate(
            "automated KIS buy cannot prove every symbol exposure component",
            snapshot_unknown_count=snapshot_unknown_count,
            local_unknown_count=local_unknown_count,
        )

    account_basis = _positive_decimal(snapshot.total_asset_value)
    account_basis_source = "broker_total_asset_value"
    if account_basis is None:
        orderable_cash = [
            value
            for value in (
                _positive_decimal(snapshot.available_cash),
                _positive_decimal(snapshot.buying_power),
            )
            if value is not None
        ]
        if not orderable_cash:
            return indeterminate(
                "automated KIS buy cannot verify the broker account allocation basis"
            )
        account_basis = min(orderable_cash)
        account_basis_source = "broker_orderable_cash"
    cap_candidates = {
        "configured_symbol_budget": Decimal(settings.exec_agent_max_symbol_budget),
        "position_allocation_cap": (
            account_basis * Decimal(settings.exec_agent_max_position_allocation_pct)
        ),
        "symbol_exposure_cap": (
            account_basis * Decimal(settings.exec_agent_max_symbol_exposure_pct)
        ),
        "symbol_total_exposure_cap": (
            account_basis * Decimal(settings.exec_agent_max_symbol_total_exposure_pct)
        ),
    }
    if any(not value.is_finite() or value <= 0 for value in cap_candidates.values()):
        return indeterminate("automated KIS buy has an invalid configured symbol cap")
    symbol_cap = min(cap_candidates.values())
    projected_value = (
        holdings_value + broker_open_value + local_open_value + candidate_value
    )
    metrics: dict[str, object] = {
        "account_basis": str(account_basis),
        "account_basis_source": account_basis_source,
        "holdings_current_value": str(holdings_value),
        "broker_open_buy_remaining_notional": str(broker_open_value),
        "local_unmatched_buy_remaining_notional": str(local_open_value),
        "candidate_notional": str(candidate_value),
        "projected_symbol_exposure": str(projected_value),
        "symbol_exposure_cap": str(symbol_cap),
        "cap_candidates": {key: str(value) for key, value in cap_candidates.items()},
        "broker_open_orders_counted_in_symbol_exposure": True,
        "broker_open_orders_reapplied_to_buying_power": False,
    }
    if projected_value <= symbol_cap:
        return None
    return AutomatedKisBuySymbolExposureFailure(
        block_reason="symbol_exposure_cap_exceeded",
        message=(
            "automated KIS buy would exceed the aggregate per-symbol exposure cap"
        ),
        metrics=metrics,
    )


def _evaluate_final_automated_buy_cash_boundary(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    execution_scope: dict[str, object] | None,
    snapshot: BrokerAccountSnapshot | None,
    snapshot_error: str | None,
    exclude_order_id: str,
    observed_at: datetime,
) -> AccountDrawdownDecision | None:
    """Recheck aggregate cash after the final executable price is durable."""

    if not _requires_automated_kis_symbol_exposure_boundary(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None
    if snapshot_error is not None or snapshot is None:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason=(
                "final buy capital reservation cannot verify the broker account "
                f"snapshot: {snapshot_error or 'snapshot unavailable'}"
            ),
            now=observed_at,
        )
    reservation, indeterminate_order = _scoped_open_buy_reservation(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        snapshot=snapshot,
        request_source=request_source,
        execution_scope=execution_scope,
        exclude_order_id=exclude_order_id,
    )
    if indeterminate_order is not None:
        return _reservation_scope_indeterminate_decision(
            command=command,
            plan_item=plan_item,
            order=indeterminate_order,
            observed_at=observed_at,
        )
    assert reservation is not None
    return _buy_capital_reservation_decision(
        settings=settings,
        snapshot=snapshot,
        command=command,
        plan_item=plan_item,
        local_open_buy_notional=reservation.local_unmatched_notional,
        local_open_buy_unknown_count=reservation.local_unknown_count,
        observed_at=observed_at,
        open_buy_reservation=reservation,
    )


def _snapshot_open_buy_reservation(snapshot: BrokerAccountSnapshot) -> tuple[Decimal, int]:
    resolution = _resolve_open_buy_reservations(
        snapshot=snapshot,
        local_orders=(),
    )
    return resolution.broker_open_notional, resolution.broker_unknown_count


def _buy_capital_reservation_decision(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    local_open_buy_notional: Decimal,
    local_open_buy_unknown_count: int,
    observed_at: datetime,
    open_buy_reservation: OpenBuyReservationResolution | None = None,
) -> AccountDrawdownDecision | None:
    if settings.runtime_mode == "live" and not snapshot.holdings_complete:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason=(
                "live buy capital reservation is incomplete because holdings "
                "cannot be verified"
            ),
        )
    candidate_notional = _order_notional(command=command, plan_item=plan_item)
    if open_buy_reservation is None:
        snapshot_notional, snapshot_unknown_count = (
            _snapshot_open_buy_reservation(snapshot)
        )
    else:
        snapshot_notional = open_buy_reservation.broker_open_notional
        snapshot_unknown_count = open_buy_reservation.broker_unknown_count
        local_open_buy_notional = (
            open_buy_reservation.local_unmatched_notional
        )
        local_open_buy_unknown_count = (
            open_buy_reservation.local_unknown_count
        )
    provider = str(command.provider or "").strip().lower()
    buying_power_is_net_of_broker_open_orders = provider == "alpaca"
    unknown_count = local_open_buy_unknown_count
    if not buying_power_is_net_of_broker_open_orders:
        unknown_count += snapshot_unknown_count
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    open_orders_error = snapshot_payload.get("open_orders_error")
    configured_paper_budget_requires_exact_reservations = bool(
        settings.runtime_mode == "paper"
        and command.provider == "upbit"
        and settings.exec_agent_max_total_budget > 0
    )
    incomplete_reservation = candidate_notional is None or unknown_count > 0
    if settings.runtime_mode == "live":
        incomplete_reservation = incomplete_reservation or bool(
            not buying_power_is_net_of_broker_open_orders
            and (
                not snapshot.open_orders_complete
                or (
                    isinstance(open_orders_error, dict)
                    and open_orders_error.get("degraded") is True
                )
            )
        )
    if (
        settings.runtime_mode == "live"
        or configured_paper_budget_requires_exact_reservations
    ) and incomplete_reservation:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason=(
                "live buy capital reservation is incomplete because candidate or open-order "
                "notional cannot be verified"
            ),
        )
    if candidate_notional is None:
        return None

    cash_candidates = [
        value
        for value in (snapshot.available_cash, snapshot.buying_power)
        if value is not None
    ]
    if not cash_candidates:
        if (
            settings.runtime_mode == "paper"
            and command.provider == "upbit"
            and settings.exec_agent_max_total_budget > 0
        ):
            cash_candidates = [settings.exec_agent_max_total_budget]
        elif settings.runtime_mode != "live":
            return None
        else:
            return account_drawdown_uncertain_decision(
                order_idempotency_key=command.idempotency_key,
                execution_plan_item_id=(
                    plan_item.id
                    if plan_item is not None
                    else command.execution_plan_item_id
                ),
                reason="live buy capital reservation requires broker orderable cash",
            )

    orderable_cash = min(cash_candidates)
    broker_open_buy_reservation = (
        Decimal("0")
        if buying_power_is_net_of_broker_open_orders
        else snapshot_notional
    )
    reserved_notional = broker_open_buy_reservation + local_open_buy_notional
    projected_notional = reserved_notional + candidate_notional
    if projected_notional <= orderable_cash:
        return None

    execution_plan_item_id = (
        plan_item.id if plan_item is not None else command.execution_plan_item_id
    )
    reason = "projected buy notional exceeds unreserved broker buying power"
    summary = {
        "blocked": True,
        "gate_name": "buy_capital_reservation",
        "reason": reason,
        "current_metric": str(projected_notional),
        "threshold": str(orderable_cash),
        "baseline_value": str(orderable_cash),
        "baseline_timestamp": snapshot.observed_at.astimezone(UTC).isoformat(),
        "order_idempotency_key": command.idempotency_key,
        "execution_plan_item_id": execution_plan_item_id,
        "sells_risk_reduction_allowed": True,
    }
    return AccountDrawdownDecision(
        allowed=False,
        gate_name="buy_capital_reservation",
        reason=reason,
        current_metric=projected_notional,
        threshold=orderable_cash,
        baseline_value=orderable_cash,
        baseline_timestamp=str(summary["baseline_timestamp"]),
        sells_risk_reduction_allowed=True,
        summary=summary,
        payload={
            "blocked": True,
            "evaluated_at": observed_at.isoformat(),
            "decision": summary,
            "metrics": {
                "orderable_cash": str(orderable_cash),
                "snapshot_open_buy_notional": str(snapshot_notional),
                "snapshot_open_buy_reservation_applied": str(
                    broker_open_buy_reservation
                ),
                "buying_power_is_net_of_broker_open_orders": (
                    buying_power_is_net_of_broker_open_orders
                ),
                "local_unmatched_open_buy_notional": str(local_open_buy_notional),
                "candidate_notional": str(candidate_notional),
                "projected_buy_notional": str(projected_notional),
                "unknown_open_buy_count": unknown_count,
            },
        },
    )


def _reservation_scope_indeterminate_decision(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    order: BrokerOrder,
    observed_at: datetime,
) -> AccountDrawdownDecision:
    execution_plan_item_id = (
        plan_item.id if plan_item is not None else command.execution_plan_item_id
    )
    reason = (
        "an active local buy order lacks a proven exact execution scope; "
        "reconcile or quarantine it before reserving more capital"
    )
    summary: dict[str, object] = {
        "blocked": True,
        "gate_name": "order_reservation_scope_indeterminate",
        "reason": reason,
        "current_metric": None,
        "threshold": None,
        "baseline_value": None,
        "baseline_timestamp": observed_at.isoformat(),
        "order_idempotency_key": command.idempotency_key,
        "execution_plan_item_id": execution_plan_item_id,
        "prior_order_id": order.id,
        "prior_order_status": order.status,
        "sells_risk_reduction_allowed": True,
    }
    return AccountDrawdownDecision(
        allowed=False,
        gate_name="order_reservation_scope_indeterminate",
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
            "order_idempotency_key": command.idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "decision": summary,
            "metrics": {},
        },
    )


def _replay_exact_strategy_family_ledger(
    *,
    orders: tuple[BrokerOrder, ...],
    fills_by_order: dict[str, list[ExecutionFill]],
) -> StrategyFamilyLedgerReplay:
    """Replay every exact family on one global, broker-time chronology."""

    empty = StrategyFamilyLedgerReplay(
        family_balances={},
        family_bought={},
        family_sold={},
        total_position=Decimal("0"),
    )
    replay_events: list[
        tuple[ExecutionFill, tuple[str, str], str, Decimal]
    ] = []
    for order in orders:
        replay_fills = fills_by_order.get(order.id, [])
        if not replay_fills:
            continue
        order_identity, identity_error = _validated_order_strategy_family_identity(
            order
        )
        if identity_error is not None or order_identity is None:
            return StrategyFamilyLedgerReplay(
                **{
                    **empty.__dict__,
                    "error_kind": "persisted_identity",
                    "error_message": (
                        "filled symbol inventory conflicts with its immutable "
                        "strategy family command identity"
                    ),
                }
            )
        family_key = (
            str(order_identity["entry_family_key"]),
            str(order_identity["decision_lane"]),
        )
        plan_side = str(
            order.plan_item.side if order.plan_item is not None else ""
        ).strip().lower()
        for fill in replay_fills:
            quantity = effective_fill_quantity(fill)
            if (
                not quantity.is_finite()
                or quantity <= 0
                or quantity != quantity.to_integral_value(rounding=ROUND_DOWN)
            ):
                return StrategyFamilyLedgerReplay(
                    **{
                        **empty.__dict__,
                        "error_kind": "invalid_quantity",
                        "error_message": (
                            "strategy family fill quantity is not a positive "
                            "whole-share value"
                        ),
                    }
                )
            side = str(fill.side or "").strip().lower()
            if side not in {"buy", "sell"} or side != order.side or side != plan_side:
                return StrategyFamilyLedgerReplay(
                    **{
                        **empty.__dict__,
                        "error_kind": "side_conflict",
                        "error_message": (
                            "strategy family fill side conflicts with its plan item"
                        ),
                    }
                )
            replay_events.append((fill, family_key, side, quantity))

    balances: dict[tuple[str, str], Decimal] = {}
    bought: dict[tuple[str, str], Decimal] = {}
    sold: dict[tuple[str, str], Decimal] = {}
    total_position = Decimal("0")
    for _fill, family_key, side, quantity in sorted(
        replay_events,
        key=lambda event: fifo_fill_sort_key(event[0]),
    ):
        signed_quantity = quantity if side == "buy" else -quantity
        balance = balances.get(family_key, Decimal("0")) + signed_quantity
        total_position += signed_quantity
        if side == "buy":
            bought[family_key] = bought.get(family_key, Decimal("0")) + quantity
        else:
            sold[family_key] = sold.get(family_key, Decimal("0")) + quantity
        if balance < 0:
            return StrategyFamilyLedgerReplay(
                family_balances=balances,
                family_bought=bought,
                family_sold=sold,
                total_position=total_position,
                error_kind="family_overdraw",
                error_message=(
                    "a strategy family has an overdrawn current-generation ledger"
                ),
            )
        balances[family_key] = balance

    return StrategyFamilyLedgerReplay(
        family_balances=balances,
        family_bought=bought,
        family_sold=sold,
        total_position=total_position,
    )


def _evaluate_automated_buy_lane_boundary(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    execution_scope: dict[str, object] | None,
    exclude_order_id: str | None = None,
) -> PlaceBrokerOrderResult | None:
    if not _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None
    candidate_identity = _strategy_entry_family_identity(plan_item)
    if candidate_identity is None:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_lineage_unavailable",
            message="automated watch buy lacks exact strategy lane lineage",
        )
    selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="buy",
        execution_scope=execution_scope,
        symbol=command.symbol,
        exclude_order_id=exclude_order_id,
        fifo_inventory_scope=True,
    )
    if selection.indeterminate_order is not None:
        return _reservation_scope_indeterminate_result(
            selection.indeterminate_order
        )
    candidate_lane = candidate_identity["decision_lane"]
    for order in selection.matching_orders:
        if order.idempotency_key == command.idempotency_key:
            continue
        prior_identity, prior_identity_error = (
            _validated_order_strategy_family_identity(order)
        )
        if prior_identity_error is not None or prior_identity is None:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_persisted_identity_mismatch",
                message=(
                    "an active automated buy lacks a matching immutable "
                    "strategy lane identity"
                ),
            )
        prior_lane = prior_identity["decision_lane"]
        if prior_lane != candidate_lane:
            continue
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "the same decision lane already has a durable active buy "
                    "reservation for this symbol"
                ),
                "block_reason": "same_lane_active_buy",
                "prior_order_id": order.id,
                "decision_lane": candidate_lane,
            },
        )
    return None


def _automated_buy_lane_inventory_failure(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    execution_scope: dict[str, object] | None,
    account_snapshot: BrokerAccountSnapshot | None,
    account_snapshot_error: str | None,
) -> AutomatedBuyLaneInventoryFailure | None:
    """Prove that the candidate lane does not already own current inventory."""

    if not _is_trusted_automated_watch_buy(
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    ):
        return None

    def failure(
        *,
        block_reason: str,
        message: str,
        manual_review_required: bool = True,
        **metrics: object,
    ) -> AutomatedBuyLaneInventoryFailure:
        return AutomatedBuyLaneInventoryFailure(
            block_reason=block_reason,
            message=message,
            manual_review_required=manual_review_required,
            metrics=metrics,
        )

    candidate_identity = _strategy_entry_family_identity(plan_item)
    route_scope = _execution_scope_route(execution_scope)
    if candidate_identity is None or route_scope is None:
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message=(
                "automated watch buy cannot prove its exact strategy lane "
                "and execution route"
            ),
        )
    if account_snapshot_error is not None or account_snapshot is None:
        return failure(
            block_reason="strategy_lane_position_truth_unavailable",
            message=(
                "automated watch buy cannot verify current broker position truth"
            ),
            snapshot_error=account_snapshot_error,
        )

    normalized_symbol = command.symbol.strip().upper()
    broker_positions = [
        position
        for position in (account_snapshot.holdings or account_snapshot.positions)
        if position.symbol.strip().upper() == normalized_symbol
    ]
    if len(broker_positions) > 1:
        return failure(
            block_reason="strategy_lane_position_truth_unavailable",
            message="broker position truth contains duplicate symbol rows",
        )
    broker_position_quantity: Decimal | None = None
    if broker_positions:
        raw_broker_quantity = broker_positions[0].quantity
        if (
            isinstance(raw_broker_quantity, bool)
            or raw_broker_quantity < 0
            or Decimal(raw_broker_quantity)
            != Decimal(raw_broker_quantity).to_integral_value(rounding=ROUND_DOWN)
        ):
            return failure(
                block_reason="strategy_lane_position_truth_unavailable",
                message="broker position truth has an invalid share quantity",
            )
        broker_position_quantity = Decimal(raw_broker_quantity)
    elif account_snapshot.holdings_complete:
        broker_position_quantity = Decimal("0")

    local_positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == route_scope.provider,
            ExecutionPosition.account_scope == route_scope.account_scope,
            ExecutionPosition.exchange.in_(
                _fifo_inventory_position_exchanges(route_scope)
            ),
            ExecutionPosition.symbol == command.symbol,
        )
    ).all()
    if len(local_positions) > 1 or any(
        position.quantity < 0
        or Decimal(position.quantity)
        != Decimal(position.quantity).to_integral_value(rounding=ROUND_DOWN)
        for position in local_positions
    ):
        return failure(
            block_reason="strategy_lane_position_truth_unavailable",
            message="local position truth has an invalid inventory-scope quantity",
        )
    local_position_quantity = (
        Decimal(local_positions[0].quantity) if local_positions else None
    )
    if (
        local_position_quantity is not None
        and broker_positions
        and local_position_quantity != broker_position_quantity
    ):
        return failure(
            block_reason="strategy_lane_position_mismatch",
            message="local and broker position truth disagree for this inventory scope",
        )
    if settings.runtime_mode == "live":
        if not account_snapshot.holdings_complete or broker_position_quantity is None:
            return failure(
                block_reason="strategy_lane_position_truth_unavailable",
                message="live automated buy requires complete broker position truth",
            )
        if (
            local_position_quantity is not None
            and local_position_quantity != broker_position_quantity
        ):
            return failure(
                block_reason="strategy_lane_position_mismatch",
                message="local and broker position truth disagree for this inventory scope",
            )
        authoritative_position_quantity = broker_position_quantity
    elif local_position_quantity is not None:
        authoritative_position_quantity = local_position_quantity
    elif broker_position_quantity is not None:
        authoritative_position_quantity = broker_position_quantity
    else:
        return failure(
            block_reason="strategy_lane_position_truth_unavailable",
            message=(
                "automated watch buy cannot reconcile strategy inventory to "
                "broker or exact local position truth"
            ),
        )

    ledger_through = datetime.now(UTC)
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=ledger_through,
    )
    if not isolation_cursor.apply_through(
        route_scope=route_scope,
        symbol=normalized_symbol,
        observed_at=ledger_through,
        lots=[],
    ):
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message="strategy lane inventory has unresolved flat-boundary evidence",
        )
    latest_flat_boundary = isolation_cursor.latest_boundary_through(
        route_scope=route_scope,
        symbol=normalized_symbol,
        observed_at=ledger_through,
    )

    family_orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == adapter.provider,
            BrokerOrder.symbol == command.symbol,
        )
        .options(
            selectinload(BrokerOrder.plan_item),
            selectinload(BrokerOrder.fills),
        )
        .order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())
        .with_for_update()
    ).all()
    raw_fills_by_order = {
        order.id: list(order.fills)
        for order in family_orders
        if order.fills
    }

    scoped_orders = _select_local_orders_for_fifo_inventory_scope(
        orders=[
            order
            for order in family_orders
            if order.id in raw_fills_by_order
        ],
        execution_scope=execution_scope,
    )
    if scoped_orders.indeterminate_order is not None:
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message=(
                "automated watch buy cannot prove account inventory-scoped "
                "strategy inventory"
            ),
        )
    all_scoped_raw_fills = [
        fill
        for order in scoped_orders.matching_orders
        for fill in raw_fills_by_order[order.id]
    ]
    def is_current_generation_fill(fill: ExecutionFill) -> bool:
        return (
            latest_flat_boundary is None
            or _normalize_utc_datetime(fill.filled_at) > latest_flat_boundary
        )
    scoped_raw_fills = [
        fill for fill in all_scoped_raw_fills if is_current_generation_fill(fill)
    ]
    if any(
        _normalize_utc_datetime(fill.filled_at) > ledger_through
        for fill in scoped_raw_fills
    ):
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message="strategy lane inventory contains a future-dated fill",
        )
    if execution_fills_have_relevant_conflicting_identity(
        all_scoped_raw_fills,
        is_relevant=is_current_generation_fill,
    ):
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message=(
                "current-generation strategy inventory contains conflicting "
                "duplicate fill evidence"
            ),
        )
    post_boundary_fills_by_order = {
        order.id: canonical_fills
        for order in scoped_orders.matching_orders
        if (
            canonical_fills := dedupe_execution_fills(
                [
                    fill
                    for fill in raw_fills_by_order[order.id]
                    if is_current_generation_fill(fill)
                ]
            )
        )
    }
    scoped_fills = [
        fill
        for order in scoped_orders.matching_orders
        for fill in post_boundary_fills_by_order.get(order.id, [])
    ]
    if any(
        not isinstance(fill.payload, dict)
        or fill.payload.get("fill_time_precision") != "exact"
        for fill in scoped_fills
    ):
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message=(
                "current-generation strategy inventory contains a fill "
                "without exact execution-time precision"
            ),
        )
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(scoped_fills)
    if any(
        fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=normalized_symbol,
            observed_at=fill.filled_at,
        )
        for fill in scoped_fills
    ):
        return failure(
            block_reason="strategy_lane_inventory_indeterminate",
            message="current-generation strategy fill chronology is ambiguous",
        )

    ledger = _replay_exact_strategy_family_ledger(
        orders=scoped_orders.matching_orders,
        fills_by_order=post_boundary_fills_by_order,
    )
    if ledger.error_kind is not None:
        return failure(
            block_reason=(
                "strategy_family_persisted_identity_mismatch"
                if ledger.error_kind == "persisted_identity"
                else "strategy_lane_inventory_indeterminate"
            ),
            message=ledger.error_message or "strategy lane inventory is indeterminate",
        )

    if ledger.total_position != authoritative_position_quantity:
        return failure(
            block_reason="strategy_lane_position_mismatch",
            message=(
                "current-generation strategy ledger does not reconcile to "
                "the authoritative current position"
            ),
            ledger_quantity=str(ledger.total_position),
            authoritative_position_quantity=str(authoritative_position_quantity),
        )

    candidate_lane = str(candidate_identity["decision_lane"])
    lane_remaining = sum(
        (
            quantity
            for (_family_key, lane), quantity in ledger.family_balances.items()
            if lane == candidate_lane
        ),
        Decimal("0"),
    )
    if lane_remaining <= 0:
        return None
    return failure(
        block_reason="same_lane_existing_position",
        message=(
            "the same decision lane already owns current inventory for this symbol"
        ),
        manual_review_required=False,
        decision_lane=candidate_lane,
        lane_position_quantity=int(lane_remaining),
        authoritative_position_quantity=int(authoritative_position_quantity),
    )


def _evaluate_automated_family_sell_boundary(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    execution_scope: dict[str, object] | None,
    account_snapshot: BrokerAccountSnapshot | None = None,
    account_snapshot_error: str | None = None,
    exclude_order_id: str | None = None,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
) -> PlaceBrokerOrderResult | None:
    if (
        command.side != "sell"
        or not _is_trusted_automated_family_sell(
            command=command,
            plan_item=plan_item,
            request_source=request_source,
            retry_lineage_proof=retry_lineage_proof,
        )
    ):
        return None
    candidate_identity = _strategy_entry_family_identity(plan_item)
    if candidate_identity is None:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_lineage_unavailable",
            message="automated monitoring exit lacks exact strategy family lineage",
        )

    route_scope = _execution_scope_route(execution_scope)
    if route_scope is None:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message=(
                "automated monitoring exit cannot prove its exact inventory route"
            ),
        )
    if account_snapshot_error is not None or account_snapshot is None:
        # Authentication/snapshot failures remain retryable at the existing
        # broker boundary below. They must not be converted into a permanent
        # family-attribution review solely because no snapshot was returned.
        return None

    normalized_symbol = command.symbol.strip().upper()
    broker_positions = [
        position
        for position in (account_snapshot.holdings or account_snapshot.positions)
        if position.symbol.strip().upper() == normalized_symbol
    ]
    if len(broker_positions) > 1:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_position_truth_unavailable",
            message="broker position truth contains duplicate symbol rows",
        )
    broker_position_quantity: Decimal | None = None
    if broker_positions:
        raw_broker_quantity = broker_positions[0].quantity
        if (
            isinstance(raw_broker_quantity, bool)
            or raw_broker_quantity < 0
            or Decimal(raw_broker_quantity)
            != Decimal(raw_broker_quantity).to_integral_value(rounding=ROUND_DOWN)
        ):
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_position_truth_unavailable",
                message="broker position truth has an invalid share quantity",
            )
        broker_position_quantity = Decimal(raw_broker_quantity)
    elif account_snapshot.holdings_complete:
        broker_position_quantity = Decimal("0")

    local_positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == route_scope.provider,
            ExecutionPosition.account_scope == route_scope.account_scope,
            ExecutionPosition.exchange.in_(
                _fifo_inventory_position_exchanges(route_scope)
            ),
            ExecutionPosition.symbol == command.symbol,
        )
    ).all()
    if len(local_positions) > 1 or any(
        position.quantity < 0
        or Decimal(position.quantity)
        != Decimal(position.quantity).to_integral_value(rounding=ROUND_DOWN)
        for position in local_positions
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_position_truth_unavailable",
            message="local position truth has an invalid inventory-scope quantity",
        )
    local_position_quantity = (
        Decimal(local_positions[0].quantity) if local_positions else None
    )
    if (
        local_position_quantity is not None
        and broker_positions
        and local_position_quantity != broker_position_quantity
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_position_mismatch",
            message="local and broker position truth disagree for this inventory scope",
        )
    if settings.runtime_mode == "live":
        if not account_snapshot.holdings_complete or broker_position_quantity is None:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_position_truth_unavailable",
                message=(
                    "live automated exit requires complete broker position truth"
                ),
            )
        if (
            local_position_quantity is not None
            and local_position_quantity != broker_position_quantity
        ):
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_position_mismatch",
                message="local and broker position truth disagree for this inventory scope",
            )
        authoritative_position_quantity = broker_position_quantity
    elif local_position_quantity is not None:
        authoritative_position_quantity = local_position_quantity
    elif broker_position_quantity is not None:
        authoritative_position_quantity = broker_position_quantity
    else:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_position_truth_unavailable",
            message=(
                "automated monitoring exit cannot reconcile family inventory "
                "to broker or exact local position truth"
            ),
        )

    ledger_through = datetime.now(UTC)
    isolation_cursor = load_fifo_isolation_cursor(
        db=db,
        runtime_mode=settings.runtime_mode,
        through=ledger_through,
    )
    if not isolation_cursor.apply_through(
        route_scope=route_scope,
        symbol=normalized_symbol,
        observed_at=ledger_through,
        lots=[],
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message=(
                "automated monitoring exit has unresolved legacy flat-boundary evidence"
            ),
        )
    latest_flat_boundary = isolation_cursor.latest_boundary_through(
        route_scope=route_scope,
        symbol=normalized_symbol,
        observed_at=ledger_through,
    )

    family_orders = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == adapter.provider,
            BrokerOrder.symbol == command.symbol,
        )
        .options(
            selectinload(BrokerOrder.plan_item),
            selectinload(BrokerOrder.fills),
        )
        .order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())
        .with_for_update()
    ).all()
    raw_fills_by_order = {
        order.id: list(order.fills)
        for order in family_orders
        if order.fills
    }
    filled_family_orders = [
        order
        for order in family_orders
        if order.id in raw_fills_by_order
    ]
    scoped_orders = _select_local_orders_for_fifo_inventory_scope(
        orders=filled_family_orders,
        execution_scope=execution_scope,
    )
    if scoped_orders.indeterminate_order is not None:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message=(
                "automated monitoring exit cannot prove account inventory-scoped "
                "strategy family inventory"
            ),
        )
    all_scoped_raw_fills = [
        fill
        for order in scoped_orders.matching_orders
        for fill in raw_fills_by_order[order.id]
    ]
    def is_current_generation_fill(fill: ExecutionFill) -> bool:
        return (
            latest_flat_boundary is None
            or _normalize_utc_datetime(fill.filled_at) > latest_flat_boundary
        )
    scoped_raw_fills = [
        fill for fill in all_scoped_raw_fills if is_current_generation_fill(fill)
    ]
    if any(
        _normalize_utc_datetime(fill.filled_at) > ledger_through
        for fill in scoped_raw_fills
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message="strategy family ledger contains a future-dated fill",
        )
    if execution_fills_have_relevant_conflicting_identity(
        all_scoped_raw_fills,
        is_relevant=is_current_generation_fill,
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message=(
                "current-generation strategy family inventory contains "
                "conflicting duplicate fill evidence"
            ),
        )
    post_boundary_fills_by_order = {
        order.id: canonical_fills
        for order in scoped_orders.matching_orders
        if (
            canonical_fills := dedupe_execution_fills(
                [
                    fill
                    for fill in raw_fills_by_order[order.id]
                    if is_current_generation_fill(fill)
                ]
            )
        )
    }
    scoped_post_boundary_fills = [
        fill
        for order in scoped_orders.matching_orders
        for fill in post_boundary_fills_by_order.get(order.id, [])
    ]
    if any(
        not isinstance(fill.payload, dict)
        or fill.payload.get("fill_time_precision") != "exact"
        for fill in scoped_post_boundary_fills
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message=(
                "post-boundary strategy family inventory contains a fill "
                "without exact execution-time precision"
            ),
        )
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(
        scoped_post_boundary_fills
    )
    if any(
        fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=normalized_symbol,
            observed_at=fill.filled_at,
        )
        for fill in scoped_post_boundary_fills
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_ledger_indeterminate",
            message="post-boundary strategy family fill chronology is ambiguous",
        )

    ledger = _replay_exact_strategy_family_ledger(
        orders=scoped_orders.matching_orders,
        fills_by_order=post_boundary_fills_by_order,
    )
    if ledger.error_kind is not None:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason=(
                "strategy_family_persisted_identity_mismatch"
                if ledger.error_kind == "persisted_identity"
                else "strategy_family_ledger_indeterminate"
            ),
            message=ledger.error_message
            or "automated monitoring exit has no complete family ledger",
        )
    if ledger.total_position != authoritative_position_quantity:
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_position_mismatch",
            message=(
                "post-boundary strategy family ledger does not reconcile to "
                "the authoritative current position"
            ),
        )
    candidate_family_key = (
        str(candidate_identity["entry_family_key"]),
        str(candidate_identity["decision_lane"]),
    )
    bought = ledger.family_bought.get(candidate_family_key, Decimal("0"))
    sold = ledger.family_sold.get(candidate_family_key, Decimal("0"))

    snapshot_payload = (
        account_snapshot.payload
        if isinstance(account_snapshot.payload, dict)
        else {}
    )
    degraded_open_order = snapshot_payload.get("open_orders_error")
    degraded_open_orders = snapshot_payload.get("open_orders_errors")
    if (
        not account_snapshot.open_orders_complete
        or (
            isinstance(degraded_open_order, dict)
            and degraded_open_order.get("degraded") is True
        )
        or (
            isinstance(degraded_open_orders, list)
            and any(
                isinstance(error, dict) and error.get("degraded") is True
                for error in degraded_open_orders
            )
        )
    ):
        return _manual_review_strategy_boundary_result(
            db=db,
            plan_item=plan_item,
            block_reason="strategy_family_reservation_indeterminate",
            message=(
                "automated monitoring exit requires complete broker open-sell truth"
            ),
        )

    broker_family_reserved = Decimal("0")
    matched_broker_local_order_ids: set[str] = set()
    for broker_order in account_snapshot.open_orders:
        if (
            str(broker_order.side or "").strip().lower() != "sell"
            or str(broker_order.symbol or "").strip().upper()
            != normalized_symbol
        ):
            continue
        matching_local_orders = [
            order
            for order in family_orders
            if _order_identity_claims_overlap(
                local=order,
                snapshot=broker_order,
            )
        ]
        reuses_local_order = any(
            order.id in matched_broker_local_order_ids
            for order in matching_local_orders
        )
        if len(matching_local_orders) != 1 or reuses_local_order:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_reservation_indeterminate",
                message=(
                    "a broker-open sell cannot be matched one-to-one to exact "
                    "local strategy ownership"
                ),
            )
        local_order = matching_local_orders[0]
        if (
            local_order.broker_order_id
            and broker_order.broker_order_id
            and local_order.broker_order_id != broker_order.broker_order_id
        ) or (
            local_order.idempotency_key
            and broker_order.idempotency_key
            and local_order.idempotency_key != broker_order.idempotency_key
        ):
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_reservation_indeterminate",
                message="broker-open sell identity claims conflict with its local row",
            )
        local_scope = _select_local_orders_for_fifo_inventory_scope(
            orders=[local_order],
            execution_scope=execution_scope,
        )
        remaining = _snapshot_open_order_remaining_quantity(broker_order)
        if (
            local_scope.indeterminate_order is not None
            or local_scope.matching_orders != (local_order,)
            or local_order.side != "sell"
            or local_order.symbol.strip().upper() != normalized_symbol
            or isinstance(broker_order.quantity, bool)
            or not isinstance(broker_order.quantity, int)
            or local_order.quantity is None
            or local_order.quantity <= 0
            or broker_order.quantity != local_order.quantity
            or remaining is None
            or remaining <= 0
            or remaining != remaining.to_integral_value(rounding=ROUND_DOWN)
            or remaining > Decimal(local_order.quantity)
        ):
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_reservation_indeterminate",
                message="broker-open sell has unverifiable exact family quantity",
            )
        local_identity, local_identity_error = (
            _validated_order_strategy_family_identity(local_order)
        )
        if local_identity_error is not None or local_identity is None:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_persisted_identity_mismatch",
                message=(
                    "broker-open sell conflicts with its immutable strategy "
                    "family command identity"
                ),
            )
        matched_broker_local_order_ids.add(local_order.id)
        if local_identity == candidate_identity:
            broker_family_reserved += remaining

    active_selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="sell",
        execution_scope=execution_scope,
        symbol=command.symbol,
        exclude_order_id=exclude_order_id,
        fifo_inventory_scope=True,
    )
    if active_selection.indeterminate_order is not None:
        return _reservation_scope_indeterminate_result(
            active_selection.indeterminate_order
        )
    retry_lineage_order_ids = _trusted_monitoring_retry_lineage_order_ids(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        request_source=request_source,
        source_order_pk=_trusted_retry_source_order_pk(
            command=command,
            request_source=request_source,
        ),
        execution_scope=execution_scope,
        expected_family_identity=candidate_identity,
        retry_lineage_proof=retry_lineage_proof,
        exclude_order_id=exclude_order_id,
    )
    reserved = broker_family_reserved
    for order in active_selection.matching_orders:
        if order.idempotency_key == command.idempotency_key:
            continue
        if order.id in matched_broker_local_order_ids:
            continue
        order_identity, persisted_identity_error = (
            _validated_order_strategy_family_identity(order)
        )
        if persisted_identity_error is not None or order_identity is None:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_persisted_identity_mismatch",
                message=(
                    "an active symbol exit conflicts with its immutable strategy "
                    "family command identity"
                ),
            )
        if order.id in retry_lineage_order_ids:
            continue
        if order_identity != candidate_identity:
            continue
        if order.quantity is None or order.quantity <= 0:
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_reservation_indeterminate",
                message="an active family exit has unknown reserved quantity",
            )
        filled = sum(
            (
                effective_fill_quantity(fill)
                for fill in dedupe_execution_fills(list(order.fills))
                if str(fill.side or "").strip().lower() == "sell"
            ),
            Decimal("0"),
        )
        outstanding = max(Decimal("0"), Decimal(order.quantity) - filled)
        if outstanding != outstanding.to_integral_value(rounding=ROUND_DOWN):
            return _manual_review_strategy_boundary_result(
                db=db,
                plan_item=plan_item,
                block_reason="strategy_family_reservation_indeterminate",
                message="an active family exit has non-integral reserved quantity",
            )
        reserved += outstanding

    candidate_quantity = command.quantity
    family_remaining = bought - sold
    available = family_remaining - reserved
    if candidate_quantity is not None and Decimal(candidate_quantity) <= available:
        return None
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": (
                "automated exit quantity exceeds its exact strategy family "
                "inventory after active reservations"
            ),
            "block_reason": "strategy_family_quantity_unavailable",
            "manual_review_required": False,
            "entry_family_key": candidate_identity["entry_family_key"],
            "decision_lane": candidate_identity["decision_lane"],
            "family_filled_quantity": int(bought),
            "family_exited_quantity": int(sold),
            "family_reserved_quantity": int(reserved),
            "family_available_quantity": max(0, int(available)),
            "candidate_quantity": candidate_quantity,
        },
    )


def _evaluate_sell_reservation_gate(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
    execution_scope: dict[str, object] | None,
    session_state: BrokerSession | None = None,
    session_error: str | None = None,
    account_snapshot: BrokerAccountSnapshot | None = None,
    account_snapshot_error: str | None = None,
    exclude_order_id: int | None = None,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
) -> PlaceBrokerOrderResult | None:
    if command.side != "sell":
        return None
    if adapter.provider == "alpaca":
        return _evaluate_alpaca_sell_reservation_gate(
            settings=settings,
            db=db,
            adapter=adapter,
            command=command,
            execution_scope=execution_scope,
            session_state=session_state,
            session_error=session_error,
            exclude_order_id=exclude_order_id,
        )
    if adapter.provider != "kis":
        return None
    local_selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="sell",
        execution_scope=execution_scope,
        symbol=command.symbol,
        exclude_order_id=exclude_order_id,
        fifo_inventory_scope=True,
    )
    if local_selection.indeterminate_order is not None:
        return _reservation_scope_indeterminate_result(
            local_selection.indeterminate_order
        )
    local_orders = list(local_selection.matching_orders)
    retry_source_order_pk = _trusted_retry_source_order_pk(
        command=command,
        request_source=request_source,
    )
    retry_lineage_order_ids = _trusted_monitoring_retry_lineage_order_ids(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        request_source=request_source,
        source_order_pk=retry_source_order_pk,
        execution_scope=execution_scope,
        retry_lineage_proof=retry_lineage_proof,
        exclude_order_id=exclude_order_id,
    )
    if (
        request_source == AUTOMATED_MONITORING_SOURCE
        or retry_lineage_proof is not None
        and retry_lineage_proof.automated_monitoring_family_identity is not None
    ):
        # Monitoring retries may release only the exact, durably linked chain
        # of terminal non-exposed submissions proven below. A caller-provided
        # source id alone is never enough to release a reservation.
        local_orders = [
            order for order in local_orders if order.id not in retry_lineage_order_ids
        ]
    elif retry_source_order_pk is not None:
        # A trusted internal retry intentionally replaces this exact row.
        # User-supplied metadata cannot bypass reservations because the caller
        # source is fixed by the execution-job or monitoring service.
        local_orders = [order for order in local_orders if order.id != retry_source_order_pk]
    if not local_orders and settings.runtime_mode != "live":
        return None

    if (
        account_snapshot_error is not None
        or session_error is not None
        or session_state is None
    ):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sellable position truth is unavailable; replacement sell is blocked",
                "block_reason": "sell_position_truth_unavailable",
                "error_type": "BrokerSessionUnavailable",
            },
        )
    snapshot = account_snapshot
    if snapshot is None:
        try:
            snapshot = adapter.get_account_snapshot(
                session_state.access_token or "",
                symbols=[command.symbol],
                exchange=command.exchange,
            )
        except Exception as exc:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "sellable position truth is unavailable; replacement sell is blocked",
                    "block_reason": "sell_position_truth_unavailable",
                    "error_type": exc.__class__.__name__,
                },
            )

    matching_positions = [
        position
        for position in (snapshot.holdings or snapshot.positions)
        if position.symbol == command.symbol
    ]
    total_position_quantity = sum(max(position.quantity, 0) for position in matching_positions)
    sellable_quantity_is_complete = bool(matching_positions) and all(
        getattr(position, "sellable_quantity", None) is not None
        for position in matching_positions
    )
    if len(matching_positions) > 1 or (
        not sellable_quantity_is_complete
        and (
            not snapshot.holdings_complete
            or not snapshot.open_orders_complete
        )
    ):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "sellable position or reservation truth is incomplete; "
                    "replacement sell is blocked"
                ),
                "block_reason": "sell_position_truth_unavailable",
                "matching_position_count": len(matching_positions),
                "holdings_complete": snapshot.holdings_complete,
                "open_orders_complete": snapshot.open_orders_complete,
                "sellable_quantity_is_complete": sellable_quantity_is_complete,
            },
        )
    position_quantity = sum(
        max(
            min(
                position.quantity,
                getattr(position, "sellable_quantity", None)
                if getattr(position, "sellable_quantity", None) is not None
                else position.quantity,
            ),
            0,
        )
        for position in matching_positions
    )

    broker_orders = [
        order
        for order in snapshot.open_orders
        if order.side.lower() == "sell" and order.symbol == command.symbol
    ]
    broker_open_sell_quantity = sum(
        max(order.remaining_quantity or order.quantity, 0)
        for order in broker_orders
    )
    # KIS ``ord_psbl_qty`` is already net of broker-live open sell orders. When
    # every matching holding supplies that truth, subtracting the same broker
    # orders again would understate capacity and can suppress a protective stop.
    # Local intents absent from the broker snapshot are still reserved below.
    reserved_quantity = 0 if sellable_quantity_is_complete else broker_open_sell_quantity
    for order in local_orders:
        if any(
            _same_order_identity(local=order, snapshot=snapshot_order)
            for snapshot_order in broker_orders
        ):
            continue
        if order.quantity is None or order.quantity <= 0:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "an existing sell reservation has unknown quantity",
                    "block_reason": "sell_reservation_unknown",
                    "prior_order_id": order.id,
                },
            )
        reserved_quantity += order.quantity

    candidate_quantity = command.quantity
    if candidate_quantity is None or position_quantity <= 0:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sellable position quantity could not be verified",
                "block_reason": "sell_position_truth_unavailable",
                "position_quantity": position_quantity,
                "total_position_quantity": total_position_quantity,
                "reserved_quantity": reserved_quantity,
                "broker_open_sell_quantity": broker_open_sell_quantity,
                "sellable_quantity_is_complete": sellable_quantity_is_complete,
            },
        )
    if reserved_quantity + candidate_quantity <= position_quantity:
        return None
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": "sell quantity exceeds the position remaining after active reservations",
            "block_reason": "insufficient_unreserved_position",
            "position_quantity": position_quantity,
            "total_position_quantity": total_position_quantity,
            "reserved_quantity": reserved_quantity,
            "broker_open_sell_quantity": broker_open_sell_quantity,
            "sellable_quantity_is_complete": sellable_quantity_is_complete,
            "candidate_quantity": candidate_quantity,
        },
    )


def _evaluate_alpaca_sell_reservation_gate(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
    session_state: BrokerSession | None,
    session_error: str | None,
    exclude_order_id: int | None = None,
) -> PlaceBrokerOrderResult | None:
    """Prove an Alpaca SELL only reduces the authenticated account's holding."""
    if execution_scope is None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sell account scope is unavailable; short opening is blocked",
                "block_reason": "sell_position_truth_unavailable",
            },
        )
    selection = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="sell",
        execution_scope=execution_scope,
        symbol=command.symbol,
        exclude_order_id=exclude_order_id,
    )
    if selection.indeterminate_order is not None:
        return _reservation_scope_indeterminate_result(selection.indeterminate_order)
    if session_error is not None or session_state is None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sellable position truth is unavailable; short opening is blocked",
                "block_reason": "sell_position_truth_unavailable",
            },
        )
    try:
        snapshot = adapter.get_account_snapshot(
            session_state.access_token or "",
            symbols=[command.symbol],
            exchange=command.exchange,
        )
    except Exception:
        snapshot = None
    if snapshot is None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sellable position truth is unavailable; short opening is blocked",
                "block_reason": "sell_position_truth_unavailable",
            },
        )
    if not snapshot.holdings_complete:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "broker holdings coverage is incomplete; short opening is blocked"
                ),
                "block_reason": "sell_position_truth_unavailable",
            },
        )
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    open_orders_error = snapshot_payload.get("open_orders_error")
    if (
        not snapshot.open_orders_complete
        or snapshot_payload.get("open_orders_coverage") == "incomplete"
        or (
            isinstance(open_orders_error, dict)
            and open_orders_error.get("degraded") is True
        )
    ):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": (
                    "open sell reservation coverage is incomplete; "
                    "short opening is blocked"
                ),
                "block_reason": "sell_position_truth_unavailable",
            },
        )

    position_quantity = sum(
        (
            _positive_decimal(holding.payload.get("base_quantity"))
            if isinstance(holding.payload, dict)
            else None
        )
        or Decimal(max(holding.quantity, 0))
        for holding in snapshot.holdings
        if holding.symbol == command.symbol
    )
    broker_sell_orders = [
        order
        for order in snapshot.open_orders
        if order.side.lower() == "sell" and order.symbol == command.symbol
    ]
    reserved_quantity = Decimal("0")
    for broker_sell_order in broker_sell_orders:
        payload = (
            broker_sell_order.payload
            if isinstance(broker_sell_order.payload, dict)
            else {}
        )
        if "remaining_base_quantity" in payload:
            try:
                remaining_quantity = Decimal(
                    str(payload["remaining_base_quantity"]).strip()
                )
            except (DecimalException, TypeError, ValueError):
                remaining_quantity = None
            if (
                remaining_quantity is None
                or not remaining_quantity.is_finite()
                or remaining_quantity < 0
            ):
                return PlaceBrokerOrderResult(
                    status_code=409,
                    payload={
                        "duplicate": False,
                        "message": (
                            "an existing broker sell reservation has unknown quantity"
                        ),
                        "block_reason": "sell_reservation_unknown",
                        "broker_order_id": broker_sell_order.broker_order_id,
                    },
                )
        else:
            integer_remaining = (
                broker_sell_order.remaining_quantity
                if broker_sell_order.remaining_quantity is not None
                else broker_sell_order.quantity
            )
            # Alpaca's integer compatibility fields are zero for fractional
            # orders. Without the exact decimal payload, zero is not proof that
            # an open broker order reserves nothing.
            if integer_remaining <= 0:
                return PlaceBrokerOrderResult(
                    status_code=409,
                    payload={
                        "duplicate": False,
                        "message": (
                            "an existing broker sell reservation has unknown quantity"
                        ),
                        "block_reason": "sell_reservation_unknown",
                        "broker_order_id": broker_sell_order.broker_order_id,
                    },
                )
            remaining_quantity = Decimal(integer_remaining)
        reserved_quantity += remaining_quantity
    for order in selection.matching_orders:
        if any(
            _same_order_identity(local=order, snapshot=snapshot_order)
            for snapshot_order in broker_sell_orders
        ):
            continue
        local_quantity = (
            Decimal(order.base_quantity)
            if order.base_quantity is not None and order.base_quantity > 0
            else Decimal(order.quantity)
            if order.quantity is not None and order.quantity > 0
            else None
        )
        if local_quantity is None:
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "an existing sell reservation has unknown quantity",
                    "block_reason": "sell_reservation_unknown",
                    "prior_order_id": order.id,
                },
            )
        reserved_quantity += local_quantity

    candidate_quantity = (
        Decimal(command.base_quantity)
        if command.base_quantity is not None
        else Decimal(command.quantity)
        if command.quantity is not None
        else None
    )
    if candidate_quantity is None or position_quantity <= 0:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": "sellable position quantity could not be verified",
                "block_reason": "sell_position_truth_unavailable",
                "position_quantity": str(position_quantity),
                "reserved_quantity": str(reserved_quantity),
            },
        )
    if reserved_quantity + candidate_quantity <= position_quantity:
        return None
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": "sell quantity exceeds the authenticated long position after active reservations",
            "block_reason": "insufficient_unreserved_position",
            "position_quantity": str(position_quantity),
            "reserved_quantity": str(reserved_quantity),
            "candidate_quantity": str(candidate_quantity),
        },
    )


def _monitoring_sell_quantity_cap_error(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
) -> tuple[str, str] | None:
    if not _is_trusted_automated_family_sell(
        command=command,
        plan_item=plan_item,
        request_source=request_source,
        retry_lineage_proof=retry_lineage_proof,
    ):
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    quantity_cap = metadata.get("deterministic_sell_quantity_cap")
    if (
        isinstance(quantity_cap, bool)
        or not isinstance(quantity_cap, int)
        or quantity_cap <= 0
    ):
        return (
            "monitoring_sell_quantity_cap_missing",
            "automated monitoring sell requires a positive deterministic quantity cap",
        )
    if command.quantity is None or command.quantity > quantity_cap:
        return (
            "monitoring_sell_quantity_cap_exceeded",
            "automated monitoring sell quantity exceeds its deterministic strategy intent",
        )
    return None


def _trusted_retry_source_order_pk(
    *,
    command: PlaceOrderRequest,
    request_source: str,
) -> str | None:
    if not (
        request_source.startswith("execution-job:")
        or request_source == "monitoring:intraday_target_stop_monitor"
    ):
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    retry = metadata.get("retry")
    if not isinstance(retry, dict) or retry.get("source") != "rejected_order_retry":
        return None
    source_order_pk = retry.get("source_broker_order_pk")
    if not isinstance(source_order_pk, str) or not source_order_pk.strip():
        return None
    return source_order_pk


def _persisted_retry_root_order_pk(order: BrokerOrder) -> str | None:
    """Return a prior retry's root only when both command copies and audit agree."""

    request_metadata = (
        order.request_metadata if isinstance(order.request_metadata, dict) else {}
    )
    routed_metadata = request_metadata.get("metadata")
    original_command = request_metadata.get("original_command")
    original_metadata = (
        original_command.get("metadata")
        if isinstance(original_command, dict)
        else None
    )
    routed_retry = (
        routed_metadata.get("retry") if isinstance(routed_metadata, dict) else None
    )
    original_retry = (
        original_metadata.get("retry")
        if isinstance(original_metadata, dict)
        else None
    )
    if not (
        isinstance(routed_retry, dict)
        and routed_retry.get("source") == "rejected_order_retry"
        and routed_retry == original_retry
    ):
        return None
    retry_job_id = routed_retry.get("retry_job_id")
    root_order_pk = routed_retry.get("root_broker_order_pk")
    if not (
        isinstance(retry_job_id, str)
        and retry_job_id
        and request_metadata.get("source") == f"execution-job:{retry_job_id}"
        and isinstance(root_order_pk, str)
        and root_order_pk
        and order.idempotency_key == f"retry:{root_order_pk}:{retry_job_id}"
    ):
        return None
    durable_edge = any(
        event.event_type in {"submit_requested", "submit_failed_before_broker_call"}
        and isinstance(event.request_payload, dict)
        and isinstance(event.request_payload.get("metadata"), dict)
        and event.request_payload["metadata"].get("retry") == routed_retry
        for event in order.events
    )
    return root_order_pk if durable_edge else None


def _rejected_order_retry_lineage_proof(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> RejectedOrderRetryLineageProof | None:
    """Prove an execution-job retry without trusting its source prefix alone."""

    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    retry = metadata.get("retry")
    if not (
        request_source.startswith("execution-job:")
        and isinstance(retry, dict)
        and retry.get("source") == "rejected_order_retry"
    ):
        return None

    job_id = request_source.split(":", 1)[1].strip()
    source_order_pk = retry.get("source_broker_order_pk")
    failure_id = retry.get("execution_failure_id")
    retry_job_id = retry.get("retry_job_id")
    source_scope = retry.get("source_execution_scope")
    source_route = _execution_scope_route(source_scope)
    job = db.get(ExecutionJob, job_id) if job_id else None
    source_order = (
        db.get(BrokerOrder, source_order_pk)
        if isinstance(source_order_pk, str) and source_order_pk
        else None
    )
    failure = (
        db.get(ExecutionFailure, failure_id)
        if isinstance(failure_id, str) and failure_id
        else None
    )
    source_metadata = (
        source_order.request_metadata
        if source_order is not None
        and isinstance(source_order.request_metadata, dict)
        else {}
    )
    source_root_order_pk = (
        _persisted_retry_root_order_pk(source_order)
        if source_order is not None
        and isinstance(source_metadata.get("metadata"), dict)
        and isinstance(source_metadata["metadata"].get("retry"), dict)
        else source_order.id if source_order is not None else None
    )
    root_order_pk = retry.get("root_broker_order_pk")
    root_order = (
        db.get(BrokerOrder, root_order_pk)
        if isinstance(root_order_pk, str) and root_order_pk
        else None
    )
    root_metadata = (
        root_order.request_metadata
        if root_order is not None and isinstance(root_order.request_metadata, dict)
        else {}
    )
    if not (
        source_route is not None
        and isinstance(source_scope, dict)
        and job is not None
        and job.id == retry_job_id == job_id
        and job.job_type == "broker_order_retry"
        and job.status == "running"
        and job.execution_plan_item_id == command.execution_plan_item_id
        and failure is not None
        and failure.mode == settings.runtime_mode
        and failure.retry_job_id == job.id
        and failure.broker_order_pk == source_order_pk
        and failure.execution_plan_item_id == command.execution_plan_item_id
        and failure.retry_order_idempotency_key == command.idempotency_key
        and retry.get("attempt") == failure.retry_attempt_count
        and retry.get("max_attempts") == failure.retry_max_attempts
        and source_order is not None
        and source_order.mode == settings.runtime_mode
        and source_order.broker_name == adapter.provider == source_route.provider
        and source_order.execution_plan_item_id == command.execution_plan_item_id
        and source_order.symbol == command.symbol
        and source_order.side == command.side
        and retry.get("source_broker_order_id") == source_order.broker_order_id
        and source_metadata.get("execution_scope") == source_scope
        and source_root_order_pk is not None
        and root_order_pk == source_root_order_pk
        and root_order is not None
        and root_order.mode == source_order.mode
        and root_order.broker_name == source_order.broker_name
        and root_order.execution_plan_item_id
        == source_order.execution_plan_item_id
        and root_order.symbol == source_order.symbol
        and root_order.side == source_order.side
        and root_metadata.get("execution_scope") == source_scope
    ):
        return None

    automated_family_identity: dict[str, object] | None = None
    automated_family_error: str | None = None
    expected_identity = _strategy_entry_family_identity(plan_item)
    supplied_identity = metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
    source_identity, source_identity_error = (
        _validated_order_strategy_family_identity(source_order)
    )
    root_identity, root_identity_error = _validated_order_strategy_family_identity(
        root_order
    )
    root_is_automated_monitoring_family_sell = bool(
        root_metadata.get("source") == AUTOMATED_MONITORING_SOURCE
        and command.side == "sell"
        and command.provider == "kis"
        and command.asset_class == "stock"
        and command.exchange in KRX_EXCHANGES
        and plan_item is not None
        and command.execution_plan_item_id == plan_item.id
        and plan_item.intent in AUTOMATED_FAMILY_EXIT_INTENTS
    )
    if (
        root_is_automated_monitoring_family_sell
        and expected_identity is not None
        and supplied_identity == expected_identity
        and source_identity_error is None
        and root_identity_error is None
        and source_identity == root_identity == expected_identity
    ):
        automated_family_identity = expected_identity
    elif root_is_automated_monitoring_family_sell:
        automated_family_error = "strategy_family_persisted_identity_mismatch"

    return RejectedOrderRetryLineageProof(
        job=job,
        failure=failure,
        source_order=source_order,
        root_order=root_order,
        source_scope=dict(source_scope),
        source_route=source_route,
        automated_monitoring_family_identity=automated_family_identity,
        automated_monitoring_family_error=automated_family_error,
    )


def _is_trusted_automated_family_sell(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None,
) -> bool:
    return bool(
        (
            command.side == "sell"
            and _is_automated_monitoring_order(
                command=command,
                plan_item=plan_item,
                request_source=request_source,
            )
        )
        or (
            retry_lineage_proof is not None
            and retry_lineage_proof.automated_monitoring_family_identity is not None
            and retry_lineage_proof.source_order.execution_plan_item_id
            == command.execution_plan_item_id
            and retry_lineage_proof.source_order.symbol == command.symbol
            and retry_lineage_proof.source_order.side == command.side == "sell"
        )
    )


def _rejected_order_retry_route_guard(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    routed_command: PlaceOrderRequest,
    request_source: str,
    observed_at: datetime,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
) -> PlaceBrokerOrderResult | None:
    """Keep an acknowledged rejection retry inside its proven route authority."""

    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    retry = metadata.get("retry")
    if not (
        request_source.startswith("execution-job:")
        and isinstance(retry, dict)
        and retry.get("source") == "rejected_order_retry"
    ):
        return None

    job_id = request_source.split(":", 1)[1].strip()
    source_order_pk = retry.get("source_broker_order_pk")
    failure_id = retry.get("execution_failure_id")
    retry_job_id = retry.get("retry_job_id")
    source_scope = retry.get("source_execution_scope")
    source_route = _execution_scope_route(source_scope)
    job = db.get(ExecutionJob, job_id) if job_id else None
    source_order = (
        db.get(BrokerOrder, source_order_pk)
        if isinstance(source_order_pk, str) and source_order_pk
        else None
    )
    failure = (
        db.get(ExecutionFailure, failure_id)
        if isinstance(failure_id, str) and failure_id
        else None
    )
    source_metadata = (
        source_order.request_metadata
        if source_order is not None
        and isinstance(source_order.request_metadata, dict)
        else {}
    )
    retry_lineage_proof = retry_lineage_proof or _rejected_order_retry_lineage_proof(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=source_order.plan_item if source_order is not None else None,
        request_source=request_source,
    )
    lineage_is_verified = retry_lineage_proof is not None
    canonical_route = (
        str(command.provider or "").strip().lower(),
        str(command.venue or adapter.provider).strip().upper(),
        str(command.asset_class or "").strip().lower(),
        str(command.exchange or "").strip().upper(),
        str(command.order_type or "").strip().lower(),
    )
    routed_route = (
        str(routed_command.provider or "").strip().lower(),
        str(routed_command.venue or adapter.provider).strip().upper(),
        str(routed_command.asset_class or "").strip().lower(),
        str(routed_command.exchange or "").strip().upper(),
        str(routed_command.order_type or "").strip().lower(),
    )
    proven_route = (
        (
            source_route.provider,
            source_route.venue,
            source_route.asset_class,
            source_route.exchange,
            str(command.order_type or "").strip().lower(),
        )
        if source_route is not None
        else None
    )
    reason = (
        "retry_lineage_authority_invalid"
        if retry_lineage_proof is not None
        and retry_lineage_proof.automated_monitoring_family_error is not None
        else
        "retry_route_authority_changed"
        if lineage_is_verified
        and canonical_route == proven_route
        and routed_route != proven_route
        else "retry_lineage_authority_invalid"
        if not lineage_is_verified or canonical_route != proven_route
        else None
    )
    if reason is None:
        return None

    if lineage_is_verified and source_order is not None and failure is not None:
        from app.services.rejected_order_retries import (
            _quarantine_retry_command_integrity,
        )

        _quarantine_retry_command_integrity(
            db=db,
            order=source_order,
            failure=failure,
            reason=reason,
            recorded_at=_normalize_utc_datetime(observed_at),
        )
        db.commit()

    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": (
                "rejected-order retry no longer matches its verified execution "
                "route; manual review is required"
            ),
            "block_reason": reason,
            "manual_reconciliation_required": True,
            "permanent_rejection": True,
            "preserve_plan_item_status": lineage_is_verified,
            "source_broker_order_pk": (
                source_order.id if source_order is not None else None
            ),
        },
    )


def _trusted_monitoring_retry_lineage_order_ids(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
    source_order_pk: str | None,
    execution_scope: dict[str, object] | None,
    expected_family_identity: dict[str, object] | None = None,
    retry_lineage_proof: RejectedOrderRetryLineageProof | None = None,
    exclude_order_id: str | None = None,
) -> set[str]:
    """Return only a proven chain of terminal, non-exposed monitoring retries.

    The current command names one source row. Older ancestors are released only
    when every child-to-parent edge is persisted in both order metadata and the
    immutable submission event. A same-plan scan is intentionally insufficient:
    unrelated, partially filled, active, and unknown-outcome rows keep reserving
    quantity even when a caller supplies their primary key.
    """

    if (
        request_source != "monitoring:intraday_target_stop_monitor"
        and (
            retry_lineage_proof is None
            or retry_lineage_proof.automated_monitoring_family_identity is None
        )
        or (
            retry_lineage_proof is not None
            and retry_lineage_proof.source_order.id != source_order_pk
        )
        or source_order_pk is None
        or command.execution_plan_item_id is None
    ):
        return set()
    source_order = _load_monitoring_retry_lineage_order(
        db=db,
        order_id=source_order_pk,
    )
    if source_order is None:
        return set()
    if not (
        source_order.mode == settings.runtime_mode
        and source_order.broker_name == adapter.provider
        and source_order.execution_plan_item_id == command.execution_plan_item_id
        and source_order.symbol == command.symbol
        and source_order.side == command.side
    ):
        return set()
    latest_order_id = db.scalar(
        select(BrokerOrder.id)
        .join(BrokerOrderEvent, BrokerOrderEvent.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == adapter.provider,
            BrokerOrder.execution_plan_item_id == command.execution_plan_item_id,
            BrokerOrder.symbol == command.symbol,
            BrokerOrder.side == command.side,
            *(
                (BrokerOrder.id != exclude_order_id,)
                if exclude_order_id is not None
                else ()
            ),
            BrokerOrderEvent.event_type.in_(
                ("submit_requested", "submit_failed_before_broker_call")
            ),
        )
        .order_by(
            BrokerOrderEvent.observed_at.desc(),
            BrokerOrderEvent.created_at.desc(),
            BrokerOrderEvent.id.desc(),
        )
        .limit(1)
    )
    if latest_order_id is None:
        latest_order_id = db.scalar(
            select(BrokerOrder.id)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.broker_name == adapter.provider,
                BrokerOrder.execution_plan_item_id == command.execution_plan_item_id,
                BrokerOrder.symbol == command.symbol,
                BrokerOrder.side == command.side,
                *(
                    (BrokerOrder.id != exclude_order_id,)
                    if exclude_order_id is not None
                    else ()
                ),
            )
            .order_by(
                BrokerOrder.updated_at.desc(),
                BrokerOrder.created_at.desc(),
                BrokerOrder.id.desc(),
            )
            .limit(1)
        )
    if latest_order_id != source_order.id:
        return set()

    family_identity = expected_family_identity or _strategy_entry_family_identity(
        source_order.plan_item
    )
    if family_identity is None:
        return set()
    trusted_order_ids: set[str] = set()
    current = source_order
    while current.id not in trusted_order_ids:
        if not _monitoring_retry_order_is_conclusively_non_exposed(
            settings=settings,
            adapter=adapter,
            order=current,
            command=command,
            execution_scope=execution_scope,
            expected_family_identity=family_identity,
        ):
            break
        trusted_order_ids.add(current.id)
        parent_order_id = _persisted_monitoring_retry_parent_order_id(current)
        if parent_order_id is None or parent_order_id in trusted_order_ids:
            break
        parent = _load_monitoring_retry_lineage_order(
            db=db,
            order_id=parent_order_id,
        )
        if parent is None:
            break
        current = parent
    return trusted_order_ids


def _load_monitoring_retry_lineage_order(
    *,
    db: Session,
    order_id: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .where(BrokerOrder.id == order_id)
        .options(
            selectinload(BrokerOrder.plan_item),
            selectinload(BrokerOrder.events),
            selectinload(BrokerOrder.fills),
        )
        .with_for_update()
    )


def _monitoring_retry_order_is_conclusively_non_exposed(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
    expected_family_identity: dict[str, object],
) -> bool:
    persisted_family_identity, persisted_family_error = (
        _persisted_order_strategy_family_identity(order)
    )
    current_family_identity = _strategy_entry_family_identity(order.plan_item)
    if not (
        order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and order.execution_plan_item_id == command.execution_plan_item_id
        and order.symbol == command.symbol
        and order.side == command.side
        and order.status
        in {
            NormalizedBrokerOrderStatus.RETRY_PENDING.value,
            NormalizedBrokerOrderStatus.REJECTED.value,
        }
        and order.last_error_retryable is True
        and order.broker_order_id is None
        and order.submitted_at is None
        and persisted_family_error is None
        and persisted_family_identity == expected_family_identity
        and current_family_identity == persisted_family_identity
        and not order.fills
    ):
        return False
    scoped = _select_local_orders_for_execution_scope(
        orders=[order],
        execution_scope=execution_scope,
    )
    if scoped.indeterminate_order is not None or scoped.matching_orders != (order,):
        return False
    if any(
        event.event_type == "submit_outcome_unknown"
        or (
            isinstance(event.payload, dict)
            and event.payload.get("manual_reconciliation_required") is True
        )
        for event in order.events
    ):
        return False
    error_code = str(order.last_error_code or "").strip().upper()
    for event in order.events:
        payload = event.payload if isinstance(event.payload, dict) else {}
        if (
            event.retryable is True
            and event.event_type == "submit_failed_before_broker_call"
            and payload.get("broker_call_started") is False
            and payload.get("manual_reconciliation_required") is False
        ):
            return True
        if (
            error_code in CONCLUSIVE_MONITORING_SESSION_REJECTION_CODES
            and event.retryable is True
            and event.event_type == "submit_failed"
            and str(event.broker_status or "").strip().upper() == error_code
            and event.normalized_status == order.status
            and payload.get("broker_rejection_acknowledged") is True
            and payload.get("manual_reconciliation_required") is False
        ):
            return True
    return False


def _persisted_monitoring_retry_parent_order_id(
    order: BrokerOrder,
) -> str | None:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    command_metadata = metadata.get("metadata")
    retry = command_metadata.get("retry") if isinstance(command_metadata, dict) else None
    original_command = metadata.get("original_command")
    original_metadata = (
        original_command.get("metadata")
        if isinstance(original_command, dict)
        else None
    )
    original_retry = (
        original_metadata.get("retry")
        if isinstance(original_metadata, dict)
        else None
    )
    if not (
        isinstance(retry, dict)
        and retry.get("source") == "rejected_order_retry"
        and retry == original_retry
    ):
        return None
    parent_order_id = retry.get("source_broker_order_pk")
    if not isinstance(parent_order_id, str) or not parent_order_id.strip():
        return None
    request_source = str(metadata.get("source") or "").strip()
    if request_source == AUTOMATED_MONITORING_SOURCE:
        expected_prefix = f"monitoring:stop_sell:{order.execution_plan_item_id}"
        if not (
            order.idempotency_key.startswith(expected_prefix)
            and order.idempotency_key.endswith(f":retry:{parent_order_id}")
        ):
            return None
    elif request_source.startswith("execution-job:"):
        retry_job_id = retry.get("retry_job_id")
        root_order_pk = _persisted_retry_root_order_pk(order)
        if not (
            isinstance(retry_job_id, str)
            and retry_job_id
            and request_source == f"execution-job:{retry_job_id}"
            and root_order_pk is not None
            and order.idempotency_key == f"retry:{root_order_pk}:{retry_job_id}"
        ):
            return None
    else:
        return None
    durable_edge = any(
        event.event_type in {"submit_requested", "submit_failed_before_broker_call"}
        and isinstance(event.request_payload, dict)
        and isinstance(event.request_payload.get("metadata"), dict)
        and event.request_payload["metadata"].get("retry") == retry
        for event in order.events
    )
    return parent_order_id if durable_edge else None


def _evaluate_new_buy_account_gate(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str = "account_drawdown_gate",
    now: datetime | None = None,
    freshness_reference_at: datetime | None = None,
    session_state: BrokerSession | None = None,
    session_error: str | None = None,
    execution_scope: dict[str, object] | None = None,
    require_fresh_snapshot: bool = False,
    exclude_order_id: int | None = None,
) -> AccountDrawdownDecision | None:
    if command.side != "buy":
        return None
    kis_execution = command.provider == "kis" and adapter.provider == "kis"
    snapshot_fetch_started = monotonic()

    def decision_observed_at() -> datetime:
        reference_at = freshness_reference_at or now
        if reference_at is None:
            return datetime.now(UTC)
        return _normalize_utc_datetime(reference_at) + timedelta(
            seconds=max(0.0, monotonic() - snapshot_fetch_started)
        )

    if session_error is not None:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason=f"broker account snapshot unavailable before new buy: {session_error}",
            now=decision_observed_at(),
        )
    try:
        session_state = session_state or ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="account_drawdown_gate",
            reason=f"load broker account snapshot before new buy {command.idempotency_key}",
        )
        if kis_execution and command.asset_class == "us_equity":
            snapshots = [
                (
                    exchange,
                    adapter.get_account_snapshot(
                        session_state.access_token or "",
                        symbols=[command.symbol],
                        exchange=exchange,
                    ),
                )
                for exchange in sorted(("NASD", "NYSE", "AMEX"))
            ]
            snapshot = _merge_us_account_gate_snapshots(snapshots)
        else:
            snapshot = adapter.get_account_snapshot(
                session_state.access_token or "",
                symbols=[command.symbol],
                exchange=command.exchange or "KRX",
            )
    except Exception as exc:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
            reason=(
                "broker account snapshot unavailable before new buy: "
                f"{safe_broker_error_message(exc)}"
            ),
            now=decision_observed_at(),
        )

    observed_at = decision_observed_at()
    if require_fresh_snapshot and not snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=observed_at,
        settings=settings,
    ):
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason="broker account snapshot is stale or future-dated during recovery",
            now=observed_at,
        )
    open_buy_reservation, indeterminate_order = _scoped_open_buy_reservation(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        snapshot=snapshot,
        request_source=request_source,
        execution_scope=execution_scope,
        exclude_order_id=exclude_order_id,
    )
    if indeterminate_order is not None:
        return _reservation_scope_indeterminate_decision(
            command=command,
            plan_item=plan_item,
            order=indeterminate_order,
            observed_at=observed_at,
        )
    assert open_buy_reservation is not None
    local_open_buy_notional = open_buy_reservation.local_unmatched_notional
    local_open_buy_unknown_count = open_buy_reservation.local_unknown_count
    capital_decision = _buy_capital_reservation_decision(
        settings=settings,
        snapshot=snapshot,
        command=command,
        plan_item=plan_item,
        local_open_buy_notional=local_open_buy_notional,
        local_open_buy_unknown_count=local_open_buy_unknown_count,
        observed_at=observed_at,
        open_buy_reservation=open_buy_reservation,
    )
    if capital_decision is not None:
        return capital_decision
    if not kis_execution:
        # Provider-specific strategies own their drawdown/sleeve policy, but
        # broker buying power and exact-scope outstanding intents remain a
        # mandatory execution invariant for every BUY.
        return None
    snapshot_completeness_required = require_fresh_snapshot or (
        settings.account_drawdown_gate_enabled
        and _is_new_buy_order(command=command, plan_item=plan_item)
    )
    if snapshot_completeness_required:
        incomplete_reason = _account_drawdown_recovery_snapshot_incomplete(
            settings=settings,
            snapshot=snapshot,
            command=command,
            require_reliable_open_orders=require_fresh_snapshot,
        )
        if incomplete_reason is not None:
            return account_drawdown_uncertain_decision(
                order_idempotency_key=command.idempotency_key,
                execution_plan_item_id=(
                    plan_item.id
                    if plan_item is not None
                    else command.execution_plan_item_id
                ),
                reason=incomplete_reason,
                now=observed_at,
            )
    # Buying-power and outstanding-intent reservations are execution invariants,
    # not optional drawdown policy. A risk-reduction/hedge label may bypass loss
    # thresholds, but it must never bypass cash reservation.
    if not settings.account_drawdown_gate_enabled:
        return None
    if not _is_new_buy_order(command=command, plan_item=plan_item):
        return None
    if command.asset_class == "us_equity":
        notional = _order_notional(command=command, plan_item=plan_item)
        if notional is None:
            return account_drawdown_uncertain_decision(
                order_idempotency_key=command.idempotency_key,
                execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
                reason="KIS US-equity account gate requires deterministic USD order notional",
            )
        return evaluate_us_equity_account_gate(
            settings=settings,
            db=db,
            snapshot=snapshot,
            order_notional=notional,
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
            additional_open_buy_notional=local_open_buy_notional,
            additional_open_buy_unknown_count=local_open_buy_unknown_count,
            now=observed_at,
        )
    return evaluate_account_drawdown_gate(
        settings=settings,
        snapshot=snapshot_with_raw_balance_holdings(
            snapshot,
            as_of=observed_at,
            max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
        ),
        order_idempotency_key=command.idempotency_key,
        execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
        now=observed_at,
    )


def _account_drawdown_recovery_snapshot_incomplete(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    command: PlaceOrderRequest,
    require_reliable_open_orders: bool = True,
) -> str | None:
    if not snapshot.holdings_complete:
        return "broker account snapshot lacks complete holdings truth required for recovery"
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    degraded_open_orders = snapshot_payload.get("open_orders_error")
    aggregated_open_order_errors = snapshot_payload.get("open_orders_errors")
    if require_reliable_open_orders and (
        not snapshot.open_orders_complete
        or (
            isinstance(degraded_open_orders, dict)
            and degraded_open_orders.get("degraded") is True
        )
        or (
            isinstance(aggregated_open_order_errors, list)
            and any(
                isinstance(error, dict) and error.get("degraded") is True
                for error in aggregated_open_order_errors
            )
        )
    ):
        return "broker account snapshot lacks reliable open-order truth required for recovery"
    if command.asset_class == "us_equity":
        if (
            settings.kis_us_unrealized_loss_limit_usd > 0
            or settings.kis_us_unrealized_loss_pct > 0
        ) and snapshot.unrealized_pnl is None:
            return "broker account snapshot lacks US unrealized P/L required for recovery"
        if settings.kis_us_severe_holding_loss_pct > 0 and any(
            holding.unrealized_pnl_pct is None for holding in snapshot.holdings
        ):
            return "broker account snapshot has US holdings without loss percent required for recovery"
        return None

    if (
        settings.account_daily_total_asset_drawdown_pct > 0
        and (
            snapshot.total_asset_value is None
            or snapshot.previous_total_asset_value is None
        )
    ):
        return "broker account snapshot lacks daily asset baseline required for recovery"
    if (
        settings.account_daily_total_asset_drawdown_krw > 0
        and snapshot.asset_change_amount is None
        and (
            snapshot.total_asset_value is None
            or snapshot.previous_total_asset_value is None
        )
    ):
        return "broker account snapshot lacks daily asset change required for recovery"
    if (
        settings.account_weekly_total_asset_baseline_krw > 0
        and settings.account_weekly_total_asset_drawdown_pct > 0
        and snapshot.total_asset_value is None
    ):
        return "broker account snapshot lacks weekly asset value required for recovery"
    if (
        settings.account_unrealized_loss_abs_krw > 0
        or settings.account_unrealized_loss_pct > 0
    ) and snapshot.unrealized_pnl is None:
        return "broker account snapshot lacks unrealized P/L required for recovery"
    if settings.account_severe_holding_loss_abs_krw > 0 and any(
        holding.unrealized_pnl is None for holding in snapshot.holdings
    ):
        return "broker account snapshot has holdings without unrealized P/L required for recovery"
    if settings.account_severe_holding_loss_pct > 0 and any(
        not _holding_has_loss_percent(holding) for holding in snapshot.holdings
    ):
        return "broker account snapshot has holdings without loss percent required for recovery"
    return None


def _holding_has_loss_percent(holding: BrokerAccountHolding) -> bool:
    if (
        holding.unrealized_pnl_pct is not None
        and holding.unrealized_pnl_pct.is_finite()
    ):
        return True
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    for key in ("evlu_pfls_rt", "evlu_erng_rt", "profit_loss_pct"):
        value = payload.get(key)
        if isinstance(value, bool) or value is None:
            continue
        try:
            parsed = Decimal(str(value).replace(",", "").strip())
        except Exception:
            continue
        if parsed.is_finite():
            return True
    return False


def _merge_us_account_gate_snapshots(
    routed_snapshots: list[tuple[str, BrokerAccountSnapshot]],
) -> BrokerAccountSnapshot:
    if len(routed_snapshots) != 3:
        raise ValueError("KIS US-equity account gate requires all three exchange snapshots")
    snapshots = [snapshot for _exchange, snapshot in routed_snapshots]
    observed_at = min(snapshot.observed_at for snapshot in snapshots)
    cash_values = [snapshot.available_cash for snapshot in snapshots if snapshot.available_cash is not None]
    holdings_by_key: dict[tuple[str, str], BrokerAccountHolding] = {}
    for requested_exchange, snapshot in routed_snapshots:
        for holding in snapshot.holdings:
            holding_exchange = _us_holding_exchange(holding.payload)
            if holding_exchange is None:
                raise ValueError(
                    "KIS US holding lacks exchange proof for "
                    f"{requested_exchange}:{holding.symbol}"
                )
            if holding_exchange not in {"NASD", "NYSE", "AMEX"}:
                raise ValueError(
                    "KIS US holding reports unsupported exchange for "
                    f"{requested_exchange}:{holding.symbol}"
                )
            key = (holding_exchange, holding.symbol)
            existing = holdings_by_key.get(key)
            if existing is None:
                holdings_by_key[key] = holding
                continue
            if (
                existing.quantity != holding.quantity
                or existing.average_price != holding.average_price
                or existing.sellable_quantity != holding.sellable_quantity
                or existing.unrealized_pnl != holding.unrealized_pnl
                or existing.unrealized_pnl_pct != holding.unrealized_pnl_pct
                or _us_holding_valuation_amount(existing.payload)
                != _us_holding_valuation_amount(holding.payload)
            ):
                raise ValueError(
                    "conflicting duplicate KIS US holding for "
                    f"{holding_exchange}:{holding.symbol}"
                )
    open_orders_by_key: dict[tuple[str, str], BrokerAccountOpenOrder] = {}
    for requested_exchange, snapshot in routed_snapshots:
        for order in snapshot.open_orders:
            order_exchange = _us_holding_exchange(order.payload)
            identity = order.broker_order_id
            key = (
                ("broker_order_id", identity)
                if identity
                else (
                    order_exchange or requested_exchange,
                    f"{order.symbol}:{order.side}:{order.quantity}:{order.limit_price}",
                )
            )
            existing = open_orders_by_key.get(key)
            if existing is None:
                open_orders_by_key[key] = order
                continue
            existing_exchange = _us_holding_exchange(existing.payload)
            if (
                existing.symbol != order.symbol
                or existing.side != order.side
                or existing.quantity != order.quantity
                or existing.remaining_quantity != order.remaining_quantity
                or existing.limit_price != order.limit_price
                or (
                    existing_exchange is not None
                    and order_exchange is not None
                    and existing_exchange != order_exchange
                )
            ):
                raise ValueError(
                    "conflicting duplicate KIS US open order for "
                    f"{order_exchange or requested_exchange}:{identity}"
                )
            if existing_exchange is None and order_exchange is not None:
                open_orders_by_key[key] = order
    holdings = list(holdings_by_key.values())
    unrealized_pnl = (
        None
        if any(holding.unrealized_pnl is None for holding in holdings)
        else sum(
            (holding.unrealized_pnl for holding in holdings),
            Decimal("0"),
        )
    )
    open_orders_errors = [
        {
            "exchange": requested_exchange,
            **error,
        }
        for requested_exchange, snapshot in routed_snapshots
        for error in [
            (
                snapshot.payload.get("open_orders_error")
                if isinstance(snapshot.payload, dict)
                else None
            )
        ]
        if isinstance(error, dict)
    ]
    return BrokerAccountSnapshot(
        available_cash=min(cash_values) if cash_values else None,
        observed_at=observed_at,
        source="kis:us_equity_account_gate_aggregate",
        unrealized_pnl=unrealized_pnl,
        holdings=holdings,
        open_orders=list(open_orders_by_key.values()),
        symbol_buying_power={
            symbol: buying_power
            for snapshot in snapshots
            for symbol, buying_power in snapshot.symbol_buying_power.items()
        },
        holdings_complete=all(snapshot.holdings_complete for snapshot in snapshots),
        open_orders_complete=all(
            snapshot.open_orders_complete for snapshot in snapshots
        ),
        diagnostics={
            "route_snapshots": {
                exchange: {
                    "holdings_complete": snapshot.holdings_complete,
                    "open_orders_complete": snapshot.open_orders_complete,
                    "diagnostics": dict(snapshot.diagnostics),
                }
                for exchange, snapshot in routed_snapshots
            }
        },
        payload={
            "currency": "USD",
            "account_gate_exchange_coverage": ["AMEX", "NASD", "NYSE"],
            "realized_pnl_coverage": "degraded_local_execution_fills_only",
            "snapshot_sources": [snapshot.source for snapshot in snapshots],
            "open_orders_errors": open_orders_errors,
        },
    )


def _us_holding_exchange(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ovrs_excg_cd", "OVRS_EXCG_CD", "exchange", "excg_cd"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            normalized = value.strip().upper()
            return {
                "NAS": "NASD",
                "NASDAQ": "NASD",
                "NYS": "NYSE",
                "AMS": "AMEX",
            }.get(normalized, normalized)
    return None


def _us_holding_valuation_amount(payload: object) -> Decimal | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ovrs_stck_evlu_amt", "market_value", "evaluation_amount"):
        raw = payload.get(key)
        if isinstance(raw, bool) or raw is None:
            continue
        try:
            parsed = Decimal(str(raw).replace(",", "").strip())
        except Exception:
            continue
        if parsed.is_finite() and parsed >= 0:
            return parsed
    return None


def _is_new_buy_order(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> bool:
    del plan_item
    # This runtime is long-only: every BUY increases exposure regardless of
    # caller metadata or durable intent labels. A future hedge/cover order must
    # use a separately proven position-direction authority, not this escape hatch.
    return command.side == "buy"


def _kis_us_otc_quarantine_block(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
    lock: bool = False,
) -> dict[str, object] | None:
    if (
        str(getattr(adapter, "provider", None) or command.provider or "")
        .strip()
        .lower()
        != "kis"
        or command.asset_class != "us_equity"
        or command.side != "buy"
    ):
        return None
    account_scope = (
        str(execution_scope.get("account_scope") or "").strip()
        if isinstance(execution_scope, dict)
        and str(execution_scope.get("provider") or "").strip().lower() == "kis"
        else ""
    )
    if not account_scope:
        return {
            "block_reason": "kis_us_otc_position_quarantine_state_unavailable",
            "message": (
                "new KIS US-equity buys require an exact account scope before "
                "OTCB/OOTC quarantine state can be checked"
            ),
            "quarantined_position_count": None,
        }
    query = (
        select(ExecutionPosition.id)
        .where(
            func.lower(func.trim(ExecutionPosition.mode))
            == settings.runtime_mode.strip().lower(),
            func.lower(func.trim(ExecutionPosition.provider)) == "kis",
            func.lower(func.trim(ExecutionPosition.account_scope))
            == account_scope.lower(),
            func.upper(func.trim(ExecutionPosition.exchange)).in_(
                tuple(sorted(KIS_US_POSITION_QUARANTINE_EXCHANGES))
            ),
            or_(
                ExecutionPosition.quantity != 0,
                ExecutionPosition.base_quantity != Decimal("0"),
            ),
        )
        .order_by(ExecutionPosition.id.asc())
    )
    if lock:
        query = query.with_for_update(nowait=True)
    try:
        with db.begin_nested():
            position_ids = db.scalars(query).all()
    except DBAPIError:
        return {
            "block_reason": "kis_us_otc_position_quarantine_state_unavailable",
            "message": (
                "new KIS US-equity buys are blocked because OTCB/OOTC "
                "quarantine state could not be locked"
            ),
            "quarantined_position_count": None,
        }
    if not position_ids:
        return None
    return {
        "block_reason": "kis_us_otc_position_quarantine_active",
        "message": (
            "new KIS US-equity buys are blocked while this account has a "
            "non-flat OTCB/OOTC quarantine position"
        ),
        "quarantined_position_count": len(position_ids),
    }


def _persist_account_gate_block(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    original_command: PlaceOrderRequest,
    request_source: str,
    plan_item: ExecutionPlanItem | None,
    route_decision: RouteDecision | None,
    decision: AccountDrawdownDecision,
    existing_order: BrokerOrder | None = None,
    observed_at: datetime | None = None,
) -> PlaceBrokerOrderResult:
    observed_at = _normalize_utc_datetime(observed_at or datetime.now(UTC))
    sanitized_decision_summary = _redacted_payload(decision.summary) or {}
    sanitized_decision_payload = _redacted_payload(decision.payload) or {}
    preserve_current_plan_status = False
    if plan_item is not None:
        current_plan_state = _current_buy_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=True,
        )
        preserve_current_plan_status = bool(
            current_plan_state is None
            or current_plan_state[0]
            in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
        )
        if current_plan_state is not None:
            # Keep the caller's identity-mapped object aligned with the row we
            # just locked. Otherwise a monitoring caller can commit its stale
            # pre-snapshot status after this function returns.
            db.refresh(plan_item, attribute_names=["status", "risk_context"])
    decision_lineage = decision_lineage_for_plan_item(plan_item)
    request_metadata = {
        "source": request_source,
        "requested_at": observed_at.isoformat(),
        "original_command": _order_command_payload(original_command),
        "account_drawdown_recovery_policy": {
            "policy_version": 1,
            "recovery_scope": _account_drawdown_recovery_scope(
                settings=settings,
                provider=adapter.provider,
                asset_class=original_command.asset_class,
                exchange=original_command.exchange,
            ),
            "enabled": settings.account_drawdown_recovery_enabled,
            "policy_fingerprint": recovery_policy_fingerprint(settings),
            "threshold_ratio": str(settings.account_drawdown_recovery_threshold_ratio),
            "required_healthy_observations": (
                settings.account_drawdown_recovery_required_healthy_observations
            ),
            "cooldown_seconds": settings.account_drawdown_recovery_cooldown_seconds,
        },
        "time_in_force": command.time_in_force,
        "provider": command.provider,
        "asset_class": command.asset_class,
        "venue": command.venue,
        "sizing": _sizing_payload(command),
        "account_drawdown_gate": sanitized_decision_summary,
        **(
            {"decision_lineage": decision_lineage}
            if decision_lineage is not None
            else {}
        ),
        **(
            {"metadata": redact_account_references(command.metadata)}
            if command.metadata is not None
            else {}
        ),
        **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
    }
    if existing_order is None:
        order = BrokerOrder(
            broker_name=adapter.provider,
            execution_plan_item_id=plan_item.id if plan_item else None,
            mode=settings.runtime_mode,
            idempotency_key=command.idempotency_key,
            symbol=command.symbol,
            side=command.side,
            quantity=command.quantity,
            base_quantity=command.base_quantity,
            quote_notional=command.quote_notional,
            quantity_precision=command.quantity_precision,
            price_precision=command.price_precision,
            venue=command.venue,
            asset_class=command.asset_class,
            limit_price=command.limit_price,
        )
    else:
        order = existing_order
        request_metadata = _preserve_decision_lineage(
            request_metadata=request_metadata,
            existing_order=existing_order,
        )
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
    )
    order.request_metadata = request_metadata
    order.response_metadata = {
        "account_drawdown_gate": sanitized_decision_summary,
    }
    order.last_error_classification = "validation"
    order.last_error_code = decision.gate_name
    order.last_error_message = decision.reason
    order.last_error_retryable = False
    order.raw_broker_payload = sanitized_decision_payload
    order.last_broker_sync_at = observed_at
    db.add(order)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        duplicate = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
        if duplicate is None:
            raise
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(duplicate, include_events=True),
            },
        )

    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submission_blocked",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED.value,
            broker_status=decision.gate_name,
            message=decision.reason,
            request_payload=_order_command_payload(command),
            response_payload=sanitized_decision_payload,
            payload={
                "provider": adapter.provider,
                "account_drawdown_gate": sanitized_decision_summary,
                "sells_risk_reduction_allowed": decision.sells_risk_reduction_allowed,
            },
            observed_at=observed_at,
        )
    )
    if plan_item is not None and not preserve_current_plan_status:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="watch_buy_triggered_blocked" if plan_item.status.startswith("watch_") else "new_buy_blocked",
            source="account_drawdown_gate",
            reason=decision.reason,
            payload={
                "broker_order_pk": order.id,
                "idempotency_key": order.idempotency_key,
                "gate": decision.summary,
            },
        )
    record_account_drawdown_block(
        db=db,
        execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
        decision=decision,
        occurred_at=observed_at,
    )
    db.commit()
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": "new buy order blocked by account drawdown gate",
            "blocked": True,
            "block_reason": (
                "execution_plan_item_state_changed"
                if preserve_current_plan_status
                else decision.gate_name
            ),
            "account_drawdown_block_reason": decision.gate_name,
            "account_drawdown_gate": decision.summary,
            "sells_risk_reduction_allowed": decision.sells_risk_reduction_allowed,
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _order_command_payload(command: PlaceOrderRequest) -> dict[str, object]:
    payload = redact_account_references(command.model_dump(mode="json"))
    return payload if isinstance(payload, dict) else {}


def _redacted_payload(value: object | None) -> dict[str, object] | None:
    if value is None:
        return None
    redacted = redact_account_references(value)
    return dict(redacted) if isinstance(redacted, dict) else {}


def _redacted_error_payload(value: object | None) -> dict[str, object] | None:
    if value is None:
        return None
    redacted = sanitize_broker_error_payload(value)
    return dict(redacted) if isinstance(redacted, dict) else {}


def _canonical_execution_scope(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    access_token: str,
    command: PlaceOrderRequest,
    broker_account_id: str | None = None,
    broker_account_id_resolved: bool = False,
) -> dict[str, object]:
    provider = str(adapter.provider or "").strip().lower()
    if provider == "alpaca" and not broker_account_id_resolved:
        account_id_loader = getattr(adapter, "get_broker_account_id", None)
        if callable(account_id_loader):
            try:
                loaded_account_id = account_id_loader(access_token)
            except Exception:
                loaded_account_id = None
            broker_account_id = (
                loaded_account_id.strip()
                if isinstance(loaded_account_id, str) and loaded_account_id.strip()
                else None
            )
    try:
        identity = (
            build_authenticated_broker_position_identity(
                settings=settings,
                provider=provider,
                exchange=command.exchange,
                broker_account_id=broker_account_id or "",
            )
            if provider == "alpaca"
            else build_position_identity(
                settings=settings,
                provider=provider,
                exchange=command.exchange,
                broker_account_id=broker_account_id,
            )
        )
    except PositionIdentityConflictError as exc:
        raise BrokerAdapterError(
            "broker order submission requires a stable opaque account scope",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            retryable=False,
            code="execution_scope_unavailable",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        ) from exc
    return execution_scope_payload(
        identity=identity,
        venue=command.venue or command.exchange,
        asset_class=command.asset_class,
    )


def _bind_canonical_execution_scope(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    access_token: str,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    broker_account_id: str | None = None,
    broker_account_id_resolved: bool = False,
) -> dict[str, object]:
    scope = _canonical_execution_scope(
        settings=settings,
        adapter=adapter,
        access_token=access_token,
        command=command,
        broker_account_id=broker_account_id,
        broker_account_id_resolved=broker_account_id_resolved,
    )
    if (
        scope["provider"] != adapter.provider
        or scope["venue"] != str(order.venue or "").strip().upper()
        or scope["asset_class"] != str(order.asset_class or "").strip().lower()
        or scope["exchange"] != str(command.exchange or "").strip().upper()
    ):
        raise BrokerAdapterError(
            "canonical execution scope does not match the persisted order route",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            retryable=False,
            code="execution_scope_conflict",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    request_metadata = (
        dict(order.request_metadata)
        if isinstance(order.request_metadata, dict)
        else {}
    )
    existing_exchange = request_metadata.get("exchange")
    if "exchange" in request_metadata and (
        not isinstance(existing_exchange, str)
        or existing_exchange.strip().upper() != scope["exchange"]
    ):
        raise BrokerAdapterError(
            "persisted order exchange conflicts with the canonical execution scope",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            retryable=False,
            code="execution_scope_conflict",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    existing_scope = request_metadata.get("execution_scope")
    if "execution_scope" in request_metadata and existing_scope != scope:
        raise BrokerAdapterError(
            "canonical execution scope conflicts with the persisted order scope",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            retryable=False,
            code="execution_scope_conflict",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    request_metadata["execution_scope"] = scope
    request_metadata["exchange"] = scope["exchange"]
    order.request_metadata = request_metadata
    db.add(order)
    return scope


def _preserve_canonical_execution_scope(
    *,
    request_metadata: dict[str, object],
    existing_order: BrokerOrder | None,
) -> dict[str, object]:
    metadata = dict(request_metadata)
    existing_metadata = (
        existing_order.request_metadata
        if existing_order is not None
        and isinstance(existing_order.request_metadata, dict)
        else {}
    )
    if "execution_scope" in existing_metadata:
        existing_scope = existing_metadata.get("execution_scope")
        metadata["execution_scope"] = (
            dict(existing_scope)
            if isinstance(existing_scope, dict)
            else existing_scope
        )
    return metadata


def _preserve_decision_lineage(
    *,
    request_metadata: dict[str, object],
    existing_order: BrokerOrder | None,
) -> dict[str, object]:
    """Keep the original decision snapshot stable across safe order retries."""

    metadata = dict(request_metadata)
    existing_metadata = (
        existing_order.request_metadata
        if existing_order is not None
        and isinstance(existing_order.request_metadata, dict)
        else None
    )
    if existing_order is not None:
        if existing_metadata is not None and "decision_lineage" in existing_metadata:
            metadata["decision_lineage"] = existing_metadata["decision_lineage"]
        else:
            # Exact absence is historical evidence too. A retry of a legacy
            # order must not capture today's mutable plan row and silently
            # upgrade attribution that was missing at initial submission.
            metadata.pop("decision_lineage", None)
    return metadata


def _merge_order_request_metadata(
    *,
    base: dict[str, object] | None,
    command: PlaceOrderRequest,
    request_source: str,
    route_decision: RouteDecision | None = None,
    canonical_execution_scope: dict[str, object] | None = None,
) -> dict[str, object]:
    sanitized_base = redact_account_references(base or {})
    metadata = dict(sanitized_base) if isinstance(sanitized_base, dict) else {}
    metadata["source"] = request_source
    metadata["trusted_order_origin"] = _trusted_order_origin(request_source)
    metadata["time_in_force"] = command.time_in_force
    metadata["provider"] = command.provider
    metadata["asset_class"] = command.asset_class
    metadata["venue"] = command.venue
    metadata["exchange"] = str(command.exchange or "").strip().upper()
    metadata["sizing"] = _sizing_payload(command)
    if command.metadata is not None:
        metadata["metadata"] = redact_account_references(command.metadata)
    if route_decision is not None:
        metadata["route"] = _route_payload(route_decision)
    if canonical_execution_scope is not None:
        metadata["execution_scope"] = dict(canonical_execution_scope)
    return metadata


def _trusted_order_origin(request_source: str) -> str:
    return request_source.split(":", 1)[0] if request_source else "unknown"


def _resolve_route_decision(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> RouteDecision | None:
    if command.provider != "kis":
        return None
    if command.asset_class == "us_equity":
        return None
    if not settings.domestic_order_session_routing_enabled:
        return None
    metadata = dict(command.metadata or {})
    if plan_item is not None and isinstance(plan_item.risk_context, dict):
        for key in ("nxt_eligible", "order_purpose", "purpose"):
            if key in plan_item.risk_context and key not in metadata:
                metadata[key] = plan_item.risk_context[key]
    purpose = _order_purpose(command=command, plan_item=plan_item, metadata=metadata)
    return resolve_domestic_order_route(
        settings=settings,
        now=_route_now(metadata),
        symbol=command.symbol,
        side=command.side,
        purpose=purpose,
        desired_order_type=command.order_type,
        limit_price=command.limit_price,
        requested_exchange=command.exchange,
        metadata=metadata,
    )


def _routed_order_command(
    *,
    command: PlaceOrderRequest,
    route_decision: RouteDecision | None,
) -> PlaceOrderRequest:
    if route_decision is None:
        return command
    metadata = dict(command.metadata or {})
    metadata["route"] = _route_payload(route_decision)
    return command.model_copy(
        update={
            "exchange": route_decision.exchange,
            "order_type": route_decision.effective_order_type,
            "limit_price": command.limit_price,
            "metadata": metadata,
        }
    )


def _route_payload(route_decision: RouteDecision | None) -> dict[str, object] | None:
    if route_decision is None:
        return None
    return {
        "action": route_decision.action,
        "exchange": route_decision.exchange,
        "session": route_decision.session,
        "ord_dvsn": route_decision.ord_dvsn,
        "effective_order_type": route_decision.effective_order_type,
        "price_source": route_decision.price_source,
        "next_retry_at": (
            route_decision.next_retry_at.isoformat()
            if route_decision.next_retry_at is not None
            else None
        ),
        "reason": route_decision.reason,
    }


def _sizing_payload(command: PlaceOrderRequest) -> dict[str, object]:
    return {
        "quantity": command.quantity,
        "base_quantity": str(command.base_quantity) if command.base_quantity is not None else None,
        "quote_notional": str(command.quote_notional) if command.quote_notional is not None else None,
        "limit_price": str(command.limit_price) if command.limit_price is not None else None,
        "quantity_precision": command.quantity_precision,
        "price_precision": command.price_precision,
    }


def _record_upbit_paper_simulated_fill(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    observed_at: datetime,
) -> None:
    if command.provider != "upbit" or settings.runtime_mode != "paper":
        return
    fill_price = _simulated_fill_price(command=command, plan_item=plan_item)
    if fill_price is None:
        return
    base_quantity = _simulated_base_quantity(command=command, fill_price=fill_price)
    quote_notional = _order_notional(command=command, plan_item=plan_item)
    if base_quantity is None or base_quantity <= 0:
        return

    apply_broker_order_status(order=order, status=NormalizedBrokerOrderStatus.FILLED.value)
    order.last_broker_sync_at = observed_at
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="filled",
            source="broker_order_service",
            reason="Upbit paper order filled by local simulator",
            payload={"broker_order_pk": order.id, "idempotency_key": order.idempotency_key},
            occurred_at=observed_at,
        )

    fill = ExecutionFill(
        idempotency_key=execution_fill_idempotency_key(
            broker_order_pk=order.id,
            broker_fill_id=f"paper-fill:{order.idempotency_key}",
            symbol=order.symbol,
            side=order.side,
            filled_at=observed_at,
            price=fill_price,
            quantity=0,
        ),
        broker_order_pk=order.id,
        broker_fill_id=f"paper-fill:{order.idempotency_key}",
        symbol=order.symbol,
        side=order.side,
        quantity=0,
        base_quantity=base_quantity,
        quote_notional=quote_notional,
        price=fill_price,
        settlement_currency=settlement_currency_for_order(order),
        transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
        transaction_cost_source="paper_simulator:no_transaction_cost_model",
        filled_at=observed_at,
        payload=payload_with_order_decision_lineage(
            {
                "provider": "upbit",
                "mode": "paper",
                "simulated": True,
                "asset_class": "crypto",
                "venue": "upbit",
                "base_quantity": str(base_quantity),
                "quote_notional": (
                    str(quote_notional) if quote_notional is not None else None
                ),
                "price": str(fill_price),
            },
            order=order,
        ),
    )
    db.add(fill)
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="paper_fill_simulated",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.FILLED.value,
            broker_status="paper_filled",
            message="Upbit paper fill simulated",
            request_payload=_order_command_payload(command),
            response_payload=fill.payload,
            payload={"provider": "upbit", "paper": True, "simulated": True},
            observed_at=observed_at,
        )
    )
    _apply_upbit_paper_position(
        settings=settings,
        db=db,
        symbol=order.symbol,
        side=order.side,
        base_quantity=base_quantity,
        average_price=fill_price,
        observed_at=observed_at,
        fill_payload=fill.payload,
    )


def _apply_upbit_paper_position(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    side: str,
    base_quantity: Decimal,
    average_price: Decimal,
    observed_at: datetime,
    fill_payload: dict[str, object],
) -> None:
    position_identity = build_position_identity(
        settings=settings,
        provider="upbit",
        exchange="UPBIT",
    )
    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == position_identity.provider,
            ExecutionPosition.account_scope == position_identity.account_scope,
            ExecutionPosition.exchange == position_identity.exchange,
            ExecutionPosition.symbol == symbol,
        )
    )
    if position is None:
        position = ExecutionPosition(
            id=new_id(),
            mode=settings.runtime_mode,
            provider=position_identity.provider,
            account_scope=position_identity.account_scope,
            exchange=position_identity.exchange,
            symbol=symbol,
            quantity=0,
            base_quantity=Decimal("0"),
            average_price=None,
            updated_from="upbit_paper_simulated_fill",
            last_reconciled_at=observed_at,
            raw_payload=payload_with_position_identity(
                {"provider": "upbit", "paper": True, "simulated": True},
                identity=position_identity,
            ),
        )
        db.add(position)
    else:
        column_identity = position_identity_from_columns(position)
        if column_identity != position_identity:
            raise PositionIdentityConflictError(
                "paper fill position columns conflict with the active route"
            )
        stored_identity = position_identity_from_payload(position.raw_payload)
        if stored_identity is not None and stored_identity != position_identity:
            raise PositionIdentityConflictError(
                "paper fill position route conflicts with existing position truth"
            )
        legacy_payload = (
            position.raw_payload if isinstance(position.raw_payload, dict) else {}
        )
        legacy_provider = str(legacy_payload.get("provider") or "").strip().lower()
        if stored_identity is None and legacy_provider not in {"", "upbit"}:
            raise PositionIdentityConflictError(
                "legacy paper position belongs to a different provider"
            )
    current = position.base_quantity or Decimal("0")
    current_average_price = position.average_price
    if side == "buy":
        next_quantity = current + base_quantity
        if current > 0 and current_average_price is not None:
            next_average_price = (
                (current * current_average_price) + (base_quantity * average_price)
            ) / next_quantity
        else:
            next_average_price = average_price
    else:
        next_quantity = max(current - base_quantity, Decimal("0"))
        next_average_price = current_average_price if next_quantity > 0 else None
    position.quantity = 0
    position.base_quantity = next_quantity
    position.average_price = next_average_price
    position.updated_from = "upbit_paper_simulated_fill"
    position.last_reconciled_at = observed_at
    position.raw_payload = payload_with_position_identity(
        {
            "provider": "upbit",
            "paper": True,
            "simulated": True,
            "last_fill": fill_payload,
        },
        identity=position_identity,
    )


def _simulated_fill_price(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> Decimal | None:
    if command.limit_price is not None:
        return command.limit_price
    risk_context = getattr(plan_item, "risk_context", None) if plan_item is not None else None
    if not isinstance(risk_context, dict):
        return None
    for key in ("simulated_fill_price", "paper_fill_price", "reference_price", "estimated_reference_price"):
        value = risk_context.get(key)
        parsed = _positive_decimal(value)
        if parsed is not None:
            return parsed
    provider_metadata = risk_context.get("provider_metadata")
    if isinstance(provider_metadata, dict):
        for key in ("simulated_fill_price", "paper_fill_price", "reference_price"):
            parsed = _positive_decimal(provider_metadata.get(key))
            if parsed is not None:
                return parsed
    return None


def _simulated_base_quantity(
    *,
    command: PlaceOrderRequest,
    fill_price: Decimal,
) -> Decimal | None:
    if command.base_quantity is not None:
        return command.base_quantity
    if command.quote_notional is None:
        return None
    return (command.quote_notional / fill_price).quantize(Decimal("0.000000000001"), rounding=ROUND_DOWN)


def _order_notional(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> Decimal | None:
    if command.quote_notional is not None:
        return command.quote_notional
    risk_context = getattr(plan_item, "risk_context", None) if plan_item is not None else None
    if (
        command.limit_price is None
        and isinstance(risk_context, dict)
        and "exec_agent_decision" in risk_context
    ):
        # A normalized exec-agent audit takes precedence over legacy context.
        # If it is present but inconsistent, remain fail-closed rather than
        # silently falling back to a less trusted reference price.
        return _verified_exec_agent_market_notional(
            command=command,
            plan_item=plan_item,
        )
    price = command.limit_price or _simulated_fill_price(command=command, plan_item=plan_item)
    if price is None:
        return None
    if command.quantity is not None:
        return Decimal(command.quantity) * price
    if command.base_quantity is None:
        return None
    return (command.base_quantity * price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _verified_exec_agent_market_notional(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> Decimal | None:
    """Return a live market-order estimate only from a self-consistent approval audit."""
    if plan_item is None or command.quantity is None or command.quantity <= 0:
        return None
    risk_context = plan_item.risk_context
    if not isinstance(risk_context, dict):
        return None
    decision = risk_context.get("exec_agent_decision")
    if not isinstance(decision, dict):
        return None
    if (
        decision.get("version") != "short-term-exec-agent-v1"
        or decision.get("action") != "approve"
        or decision.get("reason_code") != "approved"
        or decision.get("execution_plan_item_id") != plan_item.id
        or decision.get("idempotency_key")
        not in _verified_exec_agent_idempotency_keys(command)
    ):
        return None

    approved_quantity = decision.get("quantity")
    if (
        isinstance(approved_quantity, bool)
        or not isinstance(approved_quantity, int)
        or approved_quantity != command.quantity
    ):
        return None
    sizing_price = _positive_decimal(decision.get("sizing_price"))
    reference_price = _positive_decimal(decision.get("reference_price"))
    approved_notional = _positive_decimal(decision.get("approved_notional"))
    if sizing_price is None or reference_price is None or approved_notional is None:
        return None
    if sizing_price != reference_price:
        return None

    candidate_notional = (Decimal(command.quantity) * sizing_price).quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )
    if candidate_notional != approved_notional:
        return None
    return candidate_notional


def _verified_exec_agent_idempotency_keys(command: PlaceOrderRequest) -> set[str]:
    """Return command identities covered by the original agent approval.

    A recovery child may retain the logical source approval only when its key
    is the deterministic child key derived from complete recovery lineage.
    Arbitrary metadata or a mismatched child key remains fail-closed.
    """

    keys = {command.idempotency_key}
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    recovery = metadata.get("account_drawdown_recovery")
    if not isinstance(recovery, dict) or recovery.get("policy_version") != 1:
        return keys
    source_key = recovery.get("source_idempotency_key")
    source_pk = recovery.get("source_broker_order_pk")
    generation = recovery.get("recovery_generation")
    if (
        not isinstance(source_key, str)
        or not source_key
        or not isinstance(source_pk, str)
        or not source_pk
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
    ):
        return keys
    expected_child_key = recovery_idempotency_key(
        base_key=source_key,
        source_broker_order_pk=source_pk,
        generation=generation,
    )
    if command.idempotency_key == expected_child_key:
        keys.add(source_key)
    return keys


def _positive_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value).strip())
    except Exception:
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _order_purpose(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    metadata: dict[str, object],
) -> str:
    # A BUY's routing authority cannot come from caller-controlled metadata.
    # Preserve only the durable plan intent; unlinked BUYs are ordinary manual
    # exposure increases regardless of any claimed risk-reduction purpose.
    if command.side == "buy":
        return plan_item.intent if plan_item is not None else "manual_order"
    raw_purpose = metadata.get("order_purpose") or metadata.get("purpose")
    if isinstance(raw_purpose, str) and raw_purpose.strip():
        return raw_purpose.strip().lower()
    if plan_item is not None and plan_item.intent == "stop_sell":
        return "protective_stop"
    if plan_item is not None:
        return plan_item.intent
    return "manual_order"


def _route_now(metadata: dict[str, object]) -> datetime:
    raw_value = metadata.get("route_now") or metadata.get("observed_at")
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    return datetime.now(UTC)


def _normalize_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _plan_item_status_for_order_status(order_status: str) -> str:
    return plan_item_status_for_broker_order_status(order_status) or order_status


def _response_event_observed_at(
    *,
    observed_at: datetime,
    request_observed_at: datetime,
) -> datetime:
    minimum = request_observed_at + timedelta(microseconds=1)
    if observed_at >= minimum:
        return observed_at
    return minimum
