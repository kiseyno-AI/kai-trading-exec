from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import fcntl
from hashlib import sha256
from pathlib import Path
from tempfile import gettempdir
from threading import Lock, RLock
from time import monotonic
from typing import Iterator
from zoneinfo import ZoneInfo

from sqlalchemy import case, exists, or_, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload, undefer

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerEventSource,
    BrokerOrderPlacementRequest,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    BrokerSession,
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
    POLICY_DISABLED_RELEASE_EVENT_TYPE,
    RECOVERABLE_LOSS_GATE_NAMES,
    RECOVERY_RELEASED_EVENT_TYPE,
    record_healthy_observation,
    record_recovery_release,
    record_recovery_reset,
    recovery_account_fingerprint,
    recovery_cooldown_until,
    recovery_gate_settings,
    recovery_idempotency_key,
    recovery_policy_fingerprint,
    recovery_state,
    snapshot_is_fresh_for_recovery,
)
from app.services.broker_raw_holdings import snapshot_with_raw_balance_holdings
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.domestic_order_router import RouteDecision, resolve_domestic_order_route
from app.services.execution_failures import record_actionable_execution_failure
from app.services.execution_api import serialize_broker_order
from app.services.fill_idempotency import execution_fill_idempotency_key
from app.services.fill_economics import COST_COVERAGE_UNKNOWN, settlement_currency_for_order
from app.services.monitoring_price_source import is_us_regular_session, next_us_regular_open
from app.services.plan_judgment_actions import project_buy_plan_item_for_hourly_risk_off
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
        "next_open_buy_failed",
    }
)
ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES = (
    "monitoring:",
    "execution-job:",
    "short-term-watcher:",
)
_ORDER_SCOPE_LOCKS: dict[str, RLock] = {}
_ORDER_SCOPE_LOCKS_GUARD = Lock()


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
    with _ORDER_SCOPE_LOCKS_GUARD:
        process_lock = _ORDER_SCOPE_LOCKS.setdefault(scope, RLock())
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
            digest = sha256(scope.encode("utf-8")).hexdigest()[:24]
            lock_path = Path(gettempdir()) / f"kai-exec-order-intent-{digest}.lock"
            with lock_path.open("a+", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            return
        yield


def _order_intent_scope_key(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> str:
    provider = str(getattr(adapter, "provider", None) or command.provider or "unknown").lower()
    account = (
        settings.kis_account_number
        if provider == "kis"
        else settings.upbit_access_key
        if provider == "upbit"
        else settings.alpaca_api_key
        if provider == "alpaca"
        else None
    )
    exchange = str(command.exchange or command.venue or "").upper()
    market = (
        "USD"
        if command.asset_class == "us_equity" or exchange in {"AMEX", "NASD", "NYSE"}
        else command.symbol.split("-", 1)[0]
        if "-" in command.symbol
        else "KRW"
    )
    account_fingerprint = sha256(str(account or "default").encode("utf-8")).hexdigest()[:16]
    return ":".join(
        ("broker-order-intent", provider, settings.runtime_mode, account_fingerprint, market)
    )


def _acquire_cross_process_order_intent_lock(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> None:
    if not make_url(settings.database_url).drivername.startswith("postgresql"):
        return
    scope = _order_intent_scope_key(
        settings=settings,
        adapter=adapter,
        command=command,
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
    retryable_safety_fenced_order = (
        existing
        if existing is not None
        and _runtime_safety_fenced_order_matches_command(
            settings=settings,
            adapter=adapter,
            order=existing,
            command=command,
        )
        else None
    )
    if retryable_safety_fenced_order is not None:
        if not _runtime_safety_order_allows_resized_retrigger(
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
        and command.execution_plan_item_id is None
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
                "message": "automated order submission is blocked by runtime safety controls",
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

    validation_error = _validate_plan_item(plan_item=plan_item, command=command)
    if validation_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={"message": validation_error},
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
        and retryable_safety_fenced_order is None
    ):
        # A monitoring/job candidate can be cancelled while it is waiting on
        # an account-wide cooldown or scope check. Refresh and lock its durable
        # state before every early recovery response so the caller cannot
        # overwrite a terminal operator decision with a retryable status.
        current_recovery_candidate = _current_buy_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=True,
        )
        if (
            current_recovery_candidate is None
            or current_recovery_candidate[0]
            in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
        ):
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_candidate_ineligible",
                message=(
                    "execution plan item is no longer eligible for automatic "
                    "account drawdown recovery"
                ),
            )
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
            if _runtime_safety_fenced_order_matches_command(
                settings=settings,
                adapter=adapter,
                order=existing_recovery_order,
                command=command,
            ):
                retryable_safety_fenced_order = existing_recovery_order
                recovery_already_released = True
                if not _runtime_safety_order_allows_resized_retrigger(
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
            and plan_item.status in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
            and retryable_safety_fenced_order is None
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
    routed_command = _routed_order_command(
        command=command,
        route_decision=route_decision,
    )

    prepared_broker_session: BrokerSession | None = None
    prepared_broker_session_error: str | None = None
    if (
        routed_command.side == "buy"
        and (routed_command.provider == "kis" or adapter.provider == "kis")
    ):
        # Broker authentication can commit token bookkeeping. Do it before the
        # PostgreSQL transaction-scoped account lock so the commit cannot
        # silently release serialization between account truth/reservation
        # evaluation and the durable submission-intent commit.
        try:
            prepared_broker_session = ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
                source="account_drawdown_gate",
                reason=(
                    "prepare broker session before serialized account checks for "
                    f"{routed_command.idempotency_key}"
                ),
            )
        except Exception as exc:
            prepared_broker_session_error = (
                f"{exc.__class__.__name__}: {exc}"
            )

    _acquire_cross_process_order_intent_lock(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
    )

    if plan_item is not None and routed_command.side == "buy":
        pending_plan_status = plan_item.status
        pending_risk_context = _json_dict(plan_item.risk_context)
        current_plan_state = _current_buy_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=False,
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
        plan_item.status = pending_plan_status
        plan_item.risk_context = _merge_pending_plan_item_risk_context(
            persisted=current_risk_context,
            pending=pending_risk_context,
        )
        if _risk_context_has_active_hourly_risk_off(current_risk_context):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "buy submission is blocked by the active hourly risk-off fence",
                    "block_reason": "hourly_plan_risk_off",
                },
            )
        if (
            request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and current_status in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
            and retryable_safety_fenced_order is None
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
            drawdown_recovery_source_order is not None
            and current_status
            in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
            and retryable_safety_fenced_order is None
        ):
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_candidate_ineligible",
                message=(
                    "execution plan item became ineligible while waiting for "
                    "the account lock"
                ),
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
        if locked_recovery_state.released and not recovery_already_released:
            # Another process released this account-wide latch while this
            # candidate waited on the account intent lock. Do not let two
            # different candidates both become the release order.
            return _account_drawdown_recovery_blocked_result(
                source_order=drawdown_recovery_source_order,
                block_reason="account_drawdown_recovery_state_changed",
                message=(
                    "account drawdown recovery was released by another order; "
                    "reevaluate this candidate against fresh account state"
                ),
            )

    unresolved_prior = _load_unresolved_prior_submission(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
    )
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

    sell_reservation_block = _evaluate_sell_reservation_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        request_source=request_source,
    )
    if sell_reservation_block is not None:
        return sell_reservation_block

    if (
        drawdown_recovery_source_order is not None
        and settings.account_drawdown_gate_enabled
        and not recovery_candidate_can_advance
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
    account_gate_decision = _evaluate_new_buy_account_gate(
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
        require_fresh_snapshot=drawdown_recovery_source_order is not None,
    )
    if account_gate_decision is not None and not account_gate_decision.allowed:
        if drawdown_recovery_source_order is not None:
            if not recovery_already_released and recovery_candidate_can_advance:
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

    if plan_item is not None and routed_command.side == "buy":
        pending_plan_status = plan_item.status
        pending_risk_context = _json_dict(plan_item.risk_context)
        current_plan_state = _current_buy_plan_item_state(
            db=db,
            item_id=plan_item.id,
            lock=True,
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
        if _risk_context_has_active_hourly_risk_off(current_risk_context):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "buy submission is blocked by the active hourly risk-off fence",
                    "block_reason": "hourly_plan_risk_off",
                },
            )
        if (
            request_source.startswith(
                ACCOUNT_DRAWDOWN_RECOVERY_AUTOMATED_SOURCE_PREFIXES
            )
            and current_status in ACCOUNT_DRAWDOWN_RECOVERY_INELIGIBLE_PLAN_STATUSES
            and retryable_safety_fenced_order is None
        ):
            return PlaceBrokerOrderResult(
                status_code=409,
                payload={
                    "duplicate": False,
                    "message": "execution plan item changed while account safety checks were running",
                    "block_reason": "execution_plan_item_state_changed",
                },
            )
        plan_item.status = pending_plan_status
        plan_item.risk_context = _merge_pending_plan_item_risk_context(
            persisted=current_risk_context,
            pending=pending_risk_context,
        )

    if drawdown_recovery_source_order is not None:
        healthy_count = 0
        if settings.account_drawdown_gate_enabled:
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
        if not recovery_already_released:
            record_recovery_release(
                db=db,
                execution_plan_item_id=(
                    drawdown_recovery_source_order.execution_plan_item_id
                ),
                source_broker_order_pk=drawdown_recovery_source_order.id,
                source_idempotency_key=drawdown_recovery_source_order.idempotency_key,
                recovery_idempotency_key=routed_command.idempotency_key,
                recovery_generation=recovery_generation or 1,
                healthy_observation_count=healthy_count,
                settings=settings,
                occurred_at=observed_at,
                gate_disabled=not settings.account_drawdown_gate_enabled,
            )

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
        **({"metadata": routed_command.metadata} if routed_command.metadata is not None else {}),
        **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
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
    if retryable_safety_fenced_order is None:
        order = BrokerOrder(
            broker_name=adapter.provider,
            execution_plan_item_id=plan_item.id if plan_item else None,
            mode=settings.runtime_mode,
            idempotency_key=routed_command.idempotency_key,
            symbol=routed_command.symbol,
            side=routed_command.side,
            status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
            quantity=routed_command.quantity,
            base_quantity=routed_command.base_quantity,
            quote_notional=routed_command.quote_notional,
            quantity_precision=routed_command.quantity_precision,
            price_precision=routed_command.price_precision,
            venue=routed_command.venue,
            asset_class=routed_command.asset_class,
            limit_price=routed_command.limit_price,
            request_metadata=request_metadata,
        )
    else:
        order = retryable_safety_fenced_order
        apply_broker_order_status(
            order=order,
            status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
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
        order.last_error_classification = None
        order.last_error_code = None
        order.last_error_message = None
        order.last_error_retryable = None
        order.raw_broker_payload = None
    db.add(order)
    request_observed_at = datetime.now(UTC)
    try:
        db.flush()
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="submit_requested",
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
                request_payload=_order_command_payload(routed_command),
                payload={"provider": adapter.provider},
                observed_at=request_observed_at,
            )
        )
        if plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status="broker_order_submission_pending",
                source="broker_order_service",
                reason=f"broker order submission requested for {routed_command.idempotency_key}",
                payload={"broker_order_pk": order.id, "idempotency_key": order.idempotency_key},
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

    broker_call_started = False
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="broker_order_service",
            reason=f"ensure active broker session for {command.idempotency_key}",
        )
        # Authentication may have taken long enough for an operator to pause or
        # kill the runtime. Commit broker-session bookkeeping, then force a fresh
        # control-row read immediately before crossing the broker side-effect
        # boundary. ``expire_on_commit`` is disabled for request sessions, so an
        # explicit refresh is required here.
        db.commit()
        control_state = _lock_runtime_control_for_broker_boundary(
            db=db,
            control_state=control_state,
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
            error_code = exc.code
            event_type = "submit_failed"
            retryable = exc.retryable
            error_message = str(exc)
        apply_broker_order_status(order=order, status=status)
        order.last_broker_sync_at = observed_at
        order.request_metadata = exc.request_metadata or order.request_metadata
        order.response_metadata = exc.response_metadata or None
        order.last_error_classification = classification
        order.last_error_code = error_code
        order.last_error_message = error_message
        order.last_error_retryable = retryable
        order.raw_broker_payload = exc.response_payload or None
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type=event_type,
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=order.status,
                broker_status=exc.code,
                error_classification=classification,
                retryable=retryable,
                message=error_message,
                request_payload=exc.request_payload or _order_command_payload(routed_command),
                response_payload=exc.response_payload or None,
                payload={
                    "provider": adapter.provider,
                    "broker_call_started": broker_call_started,
                    "broker_rejection_acknowledged": rejection_acknowledged,
                    "reported_error_classification": exc.classification.value,
                    "reported_retryable": exc.retryable,
                    "manual_reconciliation_required": outcome_unknown,
                },
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
                    "broker_status": exc.code,
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
                broker_error_code=exc.code,
                broker_error_message=str(exc),
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
                request_payload=_order_command_payload(routed_command),
                payload={
                    "provider": adapter.provider,
                    "exception_type": exc.__class__.__name__,
                    "broker_call_started": broker_call_started,
                    "manual_reconciliation_required": outcome_unknown,
                },
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
    order_request_metadata = _merge_order_request_metadata(
        base=placement.request_metadata,
        command=routed_command,
        request_source=request_source,
        route_decision=route_decision,
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
    order.request_metadata = order_request_metadata
    response_metadata = dict(placement.response_metadata or {})
    if account_gate_decision is not None:
        response_metadata["account_drawdown_gate"] = account_gate_decision.summary
    order.response_metadata = response_metadata
    order.last_error_classification = None
    order.last_error_code = None
    order.last_error_message = None
    order.last_error_retryable = None
    order.raw_broker_payload = placement.response_payload
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
            request_payload=placement.request_payload,
            response_payload=placement.response_payload,
            payload={"provider": adapter.provider},
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
) -> PlaceBrokerOrderResult:
    observed_at = datetime.now(UTC)
    block_reason = (
        block_reason_override
        or automated_order_submission_block_reason(control_state)
        or "runtime_control"
    )
    message = (
        "broker submission was fenced before the broker call because runtime safety controls "
        f"changed to {block_reason}"
    )
    retry_after_at = (
        hourly_risk_off_retry_after_at(
            settings=settings,
            state=control_state,
            now=observed_at,
        )
        if risk_off_fence
        else None
    )
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
    )
    order.last_error_classification = RUNTIME_SAFETY_CONTROL_CLASSIFICATION
    order.last_error_code = block_reason
    order.last_error_message = message
    order.last_error_retryable = True
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
            retryable=True,
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
    if plan_item is not None:
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
            "retry_after_at": (
                retry_after_at.isoformat() if retry_after_at is not None else None
            ),
            "safety": runtime_safety_payload(control_state),
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
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
        and order.broker_order_id is None
        and order.submitted_at is None
        and order.mode == settings.runtime_mode
        and order.broker_name == adapter.provider
        and account_scope_matches
        and isinstance(original_command, dict)
        and _runtime_safety_command_identity(original_command)
        == _runtime_safety_command_identity(_order_command_payload(command))
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
    audit_released = exists(
        select(1).where(
            ExecutionPlanItemAudit.execution_plan_item_id
            == BrokerOrder.execution_plan_item_id,
            ExecutionPlanItemAudit.event_type.in_(
                (
                    RECOVERY_RELEASED_EVENT_TYPE,
                    POLICY_DISABLED_RELEASE_EVENT_TYPE,
                )
            ),
            ExecutionPlanItemAudit.summary[
                "source_broker_order_pk"
            ].as_string()
            == BrokerOrder.id,
        )
    )
    order_event_released = exists(
        select(1).where(
            BrokerOrderEvent.broker_order_pk == BrokerOrder.id,
            BrokerOrderEvent.event_type.in_(
                (
                    RECOVERY_RELEASED_EVENT_TYPE,
                    POLICY_DISABLED_RELEASE_EVENT_TYPE,
                )
            ),
        )
    )
    released = or_(audit_released, order_event_released)
    scope_expression = BrokerOrder.request_metadata[
        "account_drawdown_recovery_policy"
    ]["recovery_scope"].as_string()
    currency_scope = recovery_scope.rsplit(":", 1)[-1]
    scope_predicate = scope_expression == recovery_scope
    if not settings.account_drawdown_recovery_scope_secret:
        # Without a dedicated stable key, a broker-credential rotation changes
        # the opaque account fingerprint. Conservatively keep a same-currency
        # orphan latch closed. A configured stable key makes account identity
        # deterministic, so matching another account would be a false block.
        scope_predicate = or_(
            scope_predicate,
            scope_expression.like(f"{provider}:%:{currency_scope}"),
        )
    return db.scalar(
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
            ~released,
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
        .limit(1)
    )


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


def _current_buy_plan_item_state(
    *,
    db: Session,
    item_id: str,
    lock: bool,
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
        statement = select(
            ExecutionPlanItem.status,
            ExecutionPlanItem.risk_context,
        ).where(ExecutionPlanItem.id == item_id)
        if lock:
            statement = statement.with_for_update()
        row = db.execute(statement).one_or_none()
    if row is None:
        return None
    return str(row[0]), _json_dict(row[1])


def _merge_pending_plan_item_risk_context(
    *,
    persisted: dict[str, object],
    pending: dict[str, object],
) -> dict[str, object]:
    merged = dict(persisted)
    for key, value in pending.items():
        if key == "hourly_plan_action" and key in persisted:
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


def _validate_plan_item(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
) -> str | None:
    if plan_item is None:
        return None
    if plan_item.symbol != command.symbol:
        return "execution plan item symbol does not match order symbol"
    if plan_item.side != command.side:
        return "execution plan item side does not match order side"
    if plan_item.provider and plan_item.provider != command.provider:
        return "execution plan item provider does not match order provider"
    if plan_item.asset_class and plan_item.asset_class != command.asset_class:
        return "execution plan item asset_class does not match order asset_class"
    if plan_item.venue and command.venue and plan_item.venue != command.venue:
        return "execution plan item venue does not match order venue"
    return None


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


def _load_unresolved_prior_submission(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> BrokerOrder | None:
    """Keep a new key from bypassing an order whose broker outcome is unknown."""
    return db.scalar(
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
        .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
        .limit(1)
    )


def _local_capital_reserving_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    side: str,
    asset_class: str,
    symbol: str | None = None,
) -> list[BrokerOrder]:
    query = select(BrokerOrder).where(
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == adapter.provider,
        BrokerOrder.side == side,
        BrokerOrder.status.in_(tuple(CAPITAL_RESERVING_ORDER_STATUSES)),
    )
    if symbol is not None:
        query = query.where(BrokerOrder.symbol == symbol)
    orders = db.scalars(query.order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())).all()
    return [
        order
        for order in orders
        if order.asset_class is None or order.asset_class == asset_class
    ]


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
) -> tuple[Decimal, int]:
    total = Decimal("0")
    unknown_count = 0
    retry_source_order_pk = _trusted_retry_source_order_pk(
        command=command,
        request_source=request_source,
    )
    for order in _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="buy",
        asset_class=command.asset_class,
    ):
        if order.id == retry_source_order_pk:
            continue
        if any(
            _same_order_identity(local=order, snapshot=snapshot_order)
            for snapshot_order in snapshot.open_orders
        ):
            continue
        notional = _local_order_notional(order)
        if notional is None:
            unknown_count += 1
        else:
            total += notional
    return total, unknown_count


def _snapshot_open_buy_reservation(snapshot: BrokerAccountSnapshot) -> tuple[Decimal, int]:
    total = Decimal("0")
    unknown_count = 0
    for order in snapshot.open_orders:
        if order.side.lower() != "buy":
            continue
        remaining = order.remaining_quantity or order.quantity
        if remaining <= 0:
            continue
        if order.limit_price is None or order.limit_price <= 0:
            unknown_count += 1
        else:
            total += Decimal(remaining) * order.limit_price
    return total, unknown_count


def _buy_capital_reservation_decision(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    local_open_buy_notional: Decimal,
    local_open_buy_unknown_count: int,
    observed_at: datetime,
) -> AccountDrawdownDecision | None:
    candidate_notional = _order_notional(command=command, plan_item=plan_item)
    snapshot_notional, snapshot_unknown_count = _snapshot_open_buy_reservation(snapshot)
    unknown_count = snapshot_unknown_count + local_open_buy_unknown_count
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    open_orders_error = snapshot_payload.get("open_orders_error")
    if settings.runtime_mode == "live" and (
        candidate_notional is None
        or unknown_count > 0
        or (
            isinstance(open_orders_error, dict)
            and open_orders_error.get("degraded") is True
        )
    ):
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
        if settings.runtime_mode != "live":
            return None
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason="live buy capital reservation requires broker orderable cash",
        )

    orderable_cash = min(cash_candidates)
    reserved_notional = snapshot_notional + local_open_buy_notional
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
                "local_unmatched_open_buy_notional": str(local_open_buy_notional),
                "candidate_notional": str(candidate_notional),
                "projected_buy_notional": str(projected_notional),
                "unknown_open_buy_count": unknown_count,
            },
        },
    )


def _evaluate_sell_reservation_gate(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
) -> PlaceBrokerOrderResult | None:
    if command.side != "sell" or adapter.provider != "kis":
        return None
    local_orders = _local_capital_reserving_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        side="sell",
        asset_class=command.asset_class,
        symbol=command.symbol,
    )
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
    )
    if retry_source_order_pk is not None:
        # A trusted internal retry intentionally replaces this exact row.
        # User-supplied metadata cannot bypass reservations because the caller
        # source is fixed by the execution-job or monitoring service.
        local_orders = [
            order
            for order in local_orders
            if order.id != retry_source_order_pk and order.id not in retry_lineage_order_ids
        ]
    if not local_orders and settings.runtime_mode != "live":
        return None

    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="sell_quantity_reservation_gate",
            reason=f"verify unreserved position before sell {command.idempotency_key}",
        )
        try:
            snapshot = adapter.get_account_snapshot(
                session_state.access_token or "",
                symbols=[command.symbol],
                exchange=command.exchange,
            )
        except TypeError:
            snapshot = adapter.get_account_snapshot(session_state.access_token or "")
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


def _trusted_monitoring_retry_lineage_order_ids(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
    source_order_pk: str | None,
) -> set[str]:
    """Return superseded retry-pending rows for a verified stop retry lineage.

    Retry-pending rows represent acknowledged terminal submission failures, not
    live broker orders. Active, pending-outcome, and manual-review rows are never
    returned and therefore continue to reserve quantity.
    """

    if (
        request_source != "monitoring:intraday_target_stop_monitor"
        or source_order_pk is None
        or command.execution_plan_item_id is None
    ):
        return set()
    source_order = db.get(BrokerOrder, source_order_pk)
    if source_order is None:
        return set()
    if not (
        source_order.mode == settings.runtime_mode
        and source_order.broker_name == adapter.provider
        and source_order.execution_plan_item_id == command.execution_plan_item_id
        and source_order.symbol == command.symbol
        and source_order.side == command.side
        and source_order.status
        in {
            NormalizedBrokerOrderStatus.RETRY_PENDING.value,
            NormalizedBrokerOrderStatus.REJECTED.value,
        }
        and source_order.last_error_retryable is True
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
            BrokerOrderEvent.event_type == "submit_requested",
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
    return set(
        db.scalars(
            select(BrokerOrder.id).where(
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.broker_name == adapter.provider,
                BrokerOrder.execution_plan_item_id == command.execution_plan_item_id,
                BrokerOrder.symbol == command.symbol,
                BrokerOrder.side == command.side,
                BrokerOrder.status == NormalizedBrokerOrderStatus.RETRY_PENDING.value,
            )
        ).all()
    )


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
    require_fresh_snapshot: bool = False,
) -> AccountDrawdownDecision | None:
    if command.side != "buy":
        return None
    if command.provider != "kis" and adapter.provider != "kis":
        return None
    snapshot_fetch_started = monotonic()
    if session_error is not None:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=(
                plan_item.id if plan_item is not None else command.execution_plan_item_id
            ),
            reason=f"broker account snapshot unavailable before new buy: {session_error}",
        )
    try:
        session_state = session_state or ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="account_drawdown_gate",
            reason=f"load broker account snapshot before new buy {command.idempotency_key}",
        )
        if command.asset_class == "us_equity":
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
            try:
                snapshot = adapter.get_account_snapshot(
                    session_state.access_token or "",
                    symbols=[command.symbol],
                    exchange=command.exchange or "KRX",
                )
            except TypeError:
                snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    except Exception as exc:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
            reason=f"broker account snapshot unavailable before new buy: {exc.__class__.__name__}: {exc}",
        )

    observed_at = (
        _normalize_utc_datetime(freshness_reference_at or now)
        + timedelta(seconds=max(0.0, monotonic() - snapshot_fetch_started))
        if freshness_reference_at is not None or now is not None
        else datetime.now(UTC)
    )
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
    local_open_buy_notional, local_open_buy_unknown_count = _local_unmatched_buy_reservation(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        snapshot=snapshot,
        request_source=request_source,
    )
    capital_decision = _buy_capital_reservation_decision(
        settings=settings,
        snapshot=snapshot,
        command=command,
        plan_item=plan_item,
        local_open_buy_notional=local_open_buy_notional,
        local_open_buy_unknown_count=local_open_buy_unknown_count,
        observed_at=observed_at,
    )
    if capital_decision is not None:
        return capital_decision
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
    snapshot_payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    degraded_open_orders = snapshot_payload.get("open_orders_error")
    aggregated_open_order_errors = snapshot_payload.get("open_orders_errors")
    if require_reliable_open_orders and (
        (
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
    if holding.unrealized_pnl_pct is not None:
        return True
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    for key in ("evlu_pfls_rt", "evlu_erng_rt", "profit_loss_pct"):
        value = payload.get(key)
        if isinstance(value, bool) or value is None:
            continue
        try:
            Decimal(str(value).replace(",", "").strip())
        except Exception:
            continue
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
            holding_exchange = _us_holding_exchange(holding.payload) or requested_exchange
            key = (holding_exchange, holding.symbol)
            existing = holdings_by_key.get(key)
            if existing is None:
                holdings_by_key[key] = holding
                continue
            if (
                existing.quantity != holding.quantity
                or existing.average_price != holding.average_price
                or existing.sellable_quantity != holding.sellable_quantity
            ):
                raise ValueError(
                    "conflicting duplicate KIS US holding for "
                    f"{holding_exchange}:{holding.symbol}"
                )
    open_orders_by_key: dict[tuple[str, str], BrokerAccountOpenOrder] = {}
    for requested_exchange, snapshot in routed_snapshots:
        for order in snapshot.open_orders:
            order_exchange = _us_holding_exchange(order.payload) or requested_exchange
            identity = order.broker_order_id or (
                f"{order.symbol}:{order.side}:{order.quantity}:{order.limit_price}"
            )
            key = (order_exchange, identity)
            existing = open_orders_by_key.get(key)
            if existing is None:
                open_orders_by_key[key] = order
                continue
            if existing.remaining_quantity != order.remaining_quantity:
                raise ValueError(
                    "conflicting duplicate KIS US open order for "
                    f"{order_exchange}:{identity}"
                )
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


def _is_new_buy_order(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> bool:
    if command.side != "buy":
        return False
    purpose = _order_purpose(
        command=command,
        plan_item=plan_item,
        metadata=dict(command.metadata or {}),
    )
    if purpose in {"risk_reduction", "reduce_risk", "risk_reducing", "protective_hedge"}:
        return False
    if plan_item is not None and plan_item.intent in {
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
    }:
        return False
    return True


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
        "account_drawdown_gate": decision.summary,
        **({"metadata": command.metadata} if command.metadata is not None else {}),
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
    apply_broker_order_status(
        order=order,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
    )
    order.request_metadata = request_metadata
    order.response_metadata = {"account_drawdown_gate": decision.summary}
    order.last_error_classification = "validation"
    order.last_error_code = decision.gate_name
    order.last_error_message = decision.reason
    order.last_error_retryable = False
    order.raw_broker_payload = decision.payload
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
            response_payload=decision.payload,
            payload={
                "provider": adapter.provider,
                "account_drawdown_gate": decision.summary,
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
    return command.model_dump(mode="json")


def _merge_order_request_metadata(
    *,
    base: dict[str, object] | None,
    command: PlaceOrderRequest,
    request_source: str,
    route_decision: RouteDecision | None = None,
) -> dict[str, object]:
    metadata = dict(base or {})
    metadata["source"] = request_source
    metadata["trusted_order_origin"] = _trusted_order_origin(request_source)
    metadata["time_in_force"] = command.time_in_force
    metadata["provider"] = command.provider
    metadata["asset_class"] = command.asset_class
    metadata["venue"] = command.venue
    metadata["sizing"] = _sizing_payload(command)
    if command.metadata is not None:
        metadata["metadata"] = command.metadata
    if route_decision is not None:
        metadata["route"] = _route_payload(route_decision)
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
        payload={
            "provider": "upbit",
            "mode": "paper",
            "simulated": True,
            "asset_class": "crypto",
            "venue": "upbit",
            "base_quantity": str(base_quantity),
            "quote_notional": str(quote_notional) if quote_notional is not None else None,
            "price": str(fill_price),
        },
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
    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol == symbol,
        )
    )
    if position is None:
        position = ExecutionPosition(
            id=new_id(),
            mode=settings.runtime_mode,
            symbol=symbol,
            quantity=0,
            base_quantity=Decimal("0"),
            average_price=None,
            updated_from="upbit_paper_simulated_fill",
            last_reconciled_at=observed_at,
            raw_payload={"provider": "upbit", "paper": True, "simulated": True},
        )
        db.add(position)
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
    position.raw_payload = {
        "provider": "upbit",
        "paper": True,
        "simulated": True,
        "last_fill": fill_payload,
    }


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
