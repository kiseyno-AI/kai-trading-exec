from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import hmac
import json
from typing import TYPE_CHECKING

from sqlalchemy import case, select
from sqlalchemy.orm import Session, undefer

from app.core.config import Settings
from app.models.runtime import BrokerOrderEvent, ExecutionPlanItemAudit
from app.services.execution_account_route import sanitize_account_payload
from app.services.position_identity import stable_account_scope_hmac_key

if TYPE_CHECKING:
    from app.services.account_drawdown_gate import AccountDrawdownDecision


RECOVERY_OBSERVED_EVENT_TYPE = "account_drawdown_recovery_observed"
RECOVERY_RESET_EVENT_TYPE = "account_drawdown_recovery_reset"
RECOVERY_RELEASED_EVENT_TYPE = "account_drawdown_recovery_released"
POLICY_DISABLED_RELEASE_EVENT_TYPE = "account_drawdown_gate_disabled_release"
RECOVERY_EVENT_TYPES = (
    RECOVERY_OBSERVED_EVENT_TYPE,
    RECOVERY_RESET_EVENT_TYPE,
    RECOVERY_RELEASED_EVENT_TYPE,
    POLICY_DISABLED_RELEASE_EVENT_TYPE,
)

RECOVERABLE_LOSS_GATE_NAMES = frozenset(
    {
        "daily_total_asset_drawdown_pct",
        "daily_total_asset_drawdown_krw",
        "weekly_total_asset_drawdown_pct",
        "unrealized_portfolio_loss_krw",
        "unrealized_portfolio_loss_pct",
        "severe_holding_unrealized_loss_pct",
        "severe_holding_unrealized_loss_krw",
        "us_daily_realized_loss_usd",
        "us_weekly_realized_loss_usd",
        "us_unrealized_loss_usd",
        "us_unrealized_loss_pct",
        "us_severe_holding_loss_pct",
    }
)

_RECOVERY_THRESHOLD_FIELDS = (
    "account_daily_total_asset_drawdown_pct",
    "account_daily_total_asset_drawdown_krw",
    "account_weekly_total_asset_drawdown_pct",
    "account_unrealized_loss_abs_krw",
    "account_unrealized_loss_pct",
    "account_severe_holding_loss_pct",
    "account_severe_holding_loss_abs_krw",
    "kis_us_daily_realized_loss_limit_usd",
    "kis_us_weekly_realized_loss_limit_usd",
    "kis_us_unrealized_loss_limit_usd",
    "kis_us_unrealized_loss_pct",
    "kis_us_severe_holding_loss_pct",
)


@dataclass(frozen=True)
class AccountDrawdownRecoveryState:
    healthy_observation_count: int = 0
    last_snapshot_observed_at: datetime | None = None
    last_unhealthy_at: datetime | None = None
    policy_fingerprint: str | None = None
    recovery_idempotency_key: str | None = None
    recovery_generation: int | None = None
    source_broker_order_pk: str | None = None
    source_idempotency_key: str | None = None
    released: bool = False


def recovery_gate_settings(settings: Settings) -> Settings:
    ratio = settings.account_drawdown_recovery_threshold_ratio
    return settings.model_copy(
        update={
            field: Decimal(getattr(settings, field)) * ratio
            for field in _RECOVERY_THRESHOLD_FIELDS
        }
    )


def recovery_policy_fingerprint(settings: Settings) -> str:
    fields = (
        *_RECOVERY_THRESHOLD_FIELDS,
        "account_weekly_total_asset_baseline_krw",
        "account_weekly_total_asset_baseline_at",
        "account_drawdown_recovery_threshold_ratio",
        "account_drawdown_recovery_required_healthy_observations",
        "account_drawdown_recovery_cooldown_seconds",
        "account_drawdown_recovery_min_observation_interval_seconds",
        "account_drawdown_recovery_max_snapshot_future_skew_seconds",
        "exec_agent_max_account_snapshot_age_seconds",
        "kis_us_max_total_exposure_usd",
        "kis_us_allow_degraded_realized_pnl_coverage",
    )
    policy = {field: str(getattr(settings, field)) for field in fields}
    # Recovery observations must never survive an account switch.  Store only
    # a one-way identifier in policy/audit metadata; the KIS account number is
    # deliberately not persisted in plaintext here.
    policy["account_fingerprint"] = recovery_account_fingerprint(settings)
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def recovery_account_fingerprint(settings: Settings) -> str:
    configured_scope = str(settings.execution_position_account_scope or "").strip()
    account_number = str(settings.kis_account_number or "").strip()
    product_code = str(settings.kis_account_product_code or "").strip()
    # KIS account numbers have a small input space, so an unkeyed digest would
    # still be reversible by offline enumeration.  Key the opaque identifier
    # with the broker credential; the raw account number never enters durable
    # recovery metadata.
    key = stable_account_scope_hmac_key(settings)
    keyed_kis_account_available = bool(account_number and product_code and key)
    identity = (
        "|".join(("kis", account_number, product_code))
        if keyed_kis_account_available or not configured_scope
        else f"configured-account-scope|{configured_scope}"
    )
    if not account_number and not configured_scope:
        identity = "|".join(("kis", "unconfigured", product_code))
    if key is not None:
        return hmac.new(key, identity.encode("utf-8"), sha256).hexdigest()[:24]
    if configured_scope:
        return sha256(identity.encode("utf-8")).hexdigest()[:24]
    if settings.runtime_mode == "live":
        # Health and order intake reject this configuration. A constant opaque
        # sentinel deliberately keeps any pre-existing recovery latch closed
        # rather than keying durable metadata with a short or rotating secret.
        return sha256(b"live-kis-account-scope-unavailable").hexdigest()[:24]
    paper_key = (
        settings.account_drawdown_recovery_scope_secret
        or settings.kis_app_secret
        or settings.kis_app_key
        or "unconfigured"
    ).encode("utf-8")
    return hmac.new(paper_key, identity.encode("utf-8"), sha256).hexdigest()[:24]


def recovery_idempotency_key(
    *,
    base_key: str,
    source_broker_order_pk: str,
    generation: int,
) -> str:
    suffix = f":recovery:{generation}"
    if len(base_key) + len(suffix) <= 128:
        return f"{base_key}{suffix}"
    digest = sha256(
        f"{base_key}|{source_broker_order_pk}|{generation}".encode("utf-8")
    ).hexdigest()
    return f"account-drawdown-recovery:{generation}:{digest}"


def scope_recovery_release_idempotency_key(
    *,
    source_broker_order_pk: str,
    generation: int,
) -> str:
    """Return the canonical key for an observer-only account latch release."""

    digest = sha256(
        f"{source_broker_order_pk}|{generation}".encode("utf-8")
    ).hexdigest()
    return f"account-drawdown-scope-release:{digest}"


def recovery_state_is_current_release(
    *,
    settings: Settings,
    state: AccountDrawdownRecoveryState,
    source_broker_order_pk: str,
    source_idempotency_key: str,
    recovery_generation: int,
) -> bool:
    """Trust only an exact, current durable release transition."""

    expected_order_recovery_key = recovery_idempotency_key(
        base_key=source_idempotency_key,
        source_broker_order_pk=source_broker_order_pk,
        generation=recovery_generation,
    )
    expected_scope_release_key = scope_recovery_release_idempotency_key(
        source_broker_order_pk=source_broker_order_pk,
        generation=recovery_generation,
    )
    return bool(
        state.released
        and state.source_broker_order_pk == source_broker_order_pk
        and state.source_idempotency_key == source_idempotency_key
        and state.recovery_generation == recovery_generation
        and state.recovery_idempotency_key
        in {expected_order_recovery_key, expected_scope_release_key}
        and state.policy_fingerprint == recovery_policy_fingerprint(settings)
    )


def recovery_cooldown_until(
    *,
    blocked_at: datetime,
    settings: Settings,
    last_unhealthy_at: datetime | None = None,
) -> datetime:
    anchor = last_unhealthy_at or blocked_at
    return _normalize_utc(anchor) + timedelta(
        seconds=settings.account_drawdown_recovery_cooldown_seconds
    )


def snapshot_is_fresh_for_recovery(
    *,
    observed_at: datetime,
    now: datetime,
    settings: Settings,
) -> bool:
    return account_snapshot_freshness_failure(
        observed_at=observed_at,
        now=now,
        settings=settings,
    ) is None


def account_snapshot_freshness_failure(
    *,
    observed_at: datetime | None,
    now: datetime,
    settings: Settings,
) -> str | None:
    if (
        not isinstance(observed_at, datetime)
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
    ):
        return "account_snapshot_timestamp_invalid"
    observed = _normalize_utc(observed_at)
    current = _normalize_utc(now)
    if observed < current - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    ):
        return "account_snapshot_stale"
    if observed > current + timedelta(
        seconds=settings.account_drawdown_recovery_max_snapshot_future_skew_seconds
    ):
        return "account_snapshot_future_skew"
    return None


def account_snapshot_authority_deadline(
    *,
    observed_at: datetime | None,
    request_started_at: datetime,
    settings: Settings,
    agent_sla_seconds: int | float,
    enclosing_deadline: datetime | None = None,
) -> datetime:
    """Bound a decision to the original snapshot, request SLA, and caller deadline."""

    freshness_failure = account_snapshot_freshness_failure(
        observed_at=observed_at,
        now=request_started_at,
        settings=settings,
    )
    if freshness_failure is not None:
        raise ValueError(freshness_failure)
    assert observed_at is not None
    snapshot_deadline = _normalize_utc(observed_at) + timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    request_deadline = _normalize_utc(request_started_at) + timedelta(
        seconds=max(1.0, float(agent_sla_seconds))
    )
    deadlines = [snapshot_deadline, request_deadline]
    if enclosing_deadline is not None:
        if enclosing_deadline.tzinfo is None or enclosing_deadline.utcoffset() is None:
            raise ValueError("enclosing_deadline_timestamp_invalid")
        deadlines.append(_normalize_utc(enclosing_deadline))
    return min(deadlines)


def recovery_state(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    source_broker_order_pk: str,
) -> AccountDrawdownRecoveryState:
    # Recovery observation and release audits are intentionally written in one
    # transaction with the same occurred_at.  Database timestamps can have the
    # same precision too, while audit UUIDs carry no temporal meaning.  Make
    # the domain transition order explicit so a release always wins a tied
    # observation/reset instead of relying on random UUID ordering.
    event_priority = case(
        (
            ExecutionPlanItemAudit.event_type.in_(
                (RECOVERY_RELEASED_EVENT_TYPE, POLICY_DISABLED_RELEASE_EVENT_TYPE)
            ),
            3,
        ),
        (ExecutionPlanItemAudit.event_type == RECOVERY_RESET_EVENT_TYPE, 2),
        else_=1,
    )
    if execution_plan_item_id is None:
        broker_event_priority = case(
            (
                BrokerOrderEvent.event_type.in_(
                    (RECOVERY_RELEASED_EVENT_TYPE, POLICY_DISABLED_RELEASE_EVENT_TYPE)
                ),
                3,
            ),
            (BrokerOrderEvent.event_type == RECOVERY_RESET_EVENT_TYPE, 2),
            else_=1,
        )
        events = db.scalars(
            select(BrokerOrderEvent)
            .where(
                BrokerOrderEvent.broker_order_pk == source_broker_order_pk,
                BrokerOrderEvent.event_type.in_(RECOVERY_EVENT_TYPES),
            )
            .order_by(
                BrokerOrderEvent.observed_at.desc(),
                broker_event_priority.desc(),
                BrokerOrderEvent.created_at.desc(),
                BrokerOrderEvent.id.desc(),
            )
        ).all()
        return _state_from_broker_order_events(events)

    audits = db.scalars(
        select(ExecutionPlanItemAudit)
        .where(
            ExecutionPlanItemAudit.execution_plan_item_id == execution_plan_item_id,
            ExecutionPlanItemAudit.event_type.in_(RECOVERY_EVENT_TYPES),
        )
        .options(undefer(ExecutionPlanItemAudit.summary))
        .order_by(
            ExecutionPlanItemAudit.occurred_at.desc(),
            event_priority.desc(),
            ExecutionPlanItemAudit.created_at.desc(),
            ExecutionPlanItemAudit.id.desc(),
        )
    ).all()
    latest_matching: ExecutionPlanItemAudit | None = None
    latest_reset_at: datetime | None = None
    snapshot_watermark_at: datetime | None = None
    for audit in audits:
        summary = audit.summary if isinstance(audit.summary, dict) else {}
        if summary.get("source_broker_order_pk") != source_broker_order_pk:
            continue
        snapshot_observed_at = _parse_datetime(summary.get("snapshot_observed_at"))
        if snapshot_observed_at is not None and (
            snapshot_watermark_at is None
            or snapshot_observed_at > snapshot_watermark_at
        ):
            snapshot_watermark_at = snapshot_observed_at
        if audit.event_type == RECOVERY_RESET_EVENT_TYPE and latest_reset_at is None:
            latest_reset_at = _normalize_utc(audit.occurred_at)
        if latest_matching is None:
            latest_matching = audit
    if latest_matching is None:
        return AccountDrawdownRecoveryState(
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
        )
    summary = (
        latest_matching.summary
        if isinstance(latest_matching.summary, dict)
        else {}
    )
    if latest_matching.event_type in {
        RECOVERY_RELEASED_EVENT_TYPE,
        POLICY_DISABLED_RELEASE_EVENT_TYPE,
    }:
        return AccountDrawdownRecoveryState(
            healthy_observation_count=_non_negative_int(
                summary.get("healthy_observation_count")
            ),
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
            policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
            recovery_idempotency_key=_optional_text(
                summary.get("recovery_idempotency_key")
            ),
            recovery_generation=_optional_int(summary.get("recovery_generation")),
            source_broker_order_pk=_optional_text(
                summary.get("source_broker_order_pk")
            ),
            source_idempotency_key=_optional_text(
                summary.get("source_idempotency_key")
            ),
            released=True,
        )
    if latest_matching.event_type == RECOVERY_OBSERVED_EVENT_TYPE:
        return AccountDrawdownRecoveryState(
            healthy_observation_count=_non_negative_int(
                summary.get("healthy_observation_count")
            ),
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
            policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
        )
    return AccountDrawdownRecoveryState(
        last_snapshot_observed_at=snapshot_watermark_at,
        last_unhealthy_at=latest_reset_at,
        policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
    )


def record_healthy_observation(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    source_broker_order_pk: str,
    source_idempotency_key: str,
    recovery_generation: int,
    decision: AccountDrawdownDecision,
    settings: Settings,
    occurred_at: datetime,
) -> tuple[int, bool]:
    state = recovery_state(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
    )
    policy_fingerprint = recovery_policy_fingerprint(settings)
    snapshot_observed_at = _parse_datetime(decision.baseline_timestamp)
    if (
        state.healthy_observation_count > 0
        and state.policy_fingerprint != policy_fingerprint
    ):
        _add_recovery_event(
            db=db,
            execution_plan_item_id=execution_plan_item_id,
            source_broker_order_pk=source_broker_order_pk,
            event_type=RECOVERY_RESET_EVENT_TYPE,
            summary={
                "source_broker_order_pk": source_broker_order_pk,
                "source_idempotency_key": source_idempotency_key,
                "recovery_generation": recovery_generation,
                "previous_healthy_observation_count": state.healthy_observation_count,
                "reset_reason": "recovery_policy_changed",
                "policy_fingerprint": policy_fingerprint,
                "snapshot_observed_at": (
                    snapshot_observed_at.isoformat()
                    if snapshot_observed_at is not None
                    else None
                ),
            },
            evidence={"reason": "account drawdown recovery policy changed"},
            error_message="account drawdown recovery policy changed",
            occurred_at=occurred_at,
        )
        return 0, False
    if snapshot_observed_at is None:
        return state.healthy_observation_count, False
    if state.last_snapshot_observed_at is not None:
        elapsed = snapshot_observed_at - state.last_snapshot_observed_at
        if elapsed < timedelta(
            seconds=settings.account_drawdown_recovery_min_observation_interval_seconds
        ):
            return state.healthy_observation_count, False
    required = settings.account_drawdown_recovery_required_healthy_observations
    count = min(required, state.healthy_observation_count + 1)
    summary = {
        "source_broker_order_pk": source_broker_order_pk,
        "source_idempotency_key": source_idempotency_key,
        "recovery_generation": recovery_generation,
        "healthy_observation_count": count,
        "required_healthy_observations": required,
        "threshold_ratio": str(settings.account_drawdown_recovery_threshold_ratio),
        "policy_fingerprint": policy_fingerprint,
        "gate_allowed": decision.allowed,
        "snapshot_observed_at": snapshot_observed_at.isoformat(),
    }
    _add_recovery_event(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
        event_type=RECOVERY_OBSERVED_EVENT_TYPE,
        summary=summary,
        evidence={"decision": decision.summary, "evidence": decision.payload},
        error_message=None,
        occurred_at=occurred_at,
    )
    return count, True


def record_recovery_reset(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    source_broker_order_pk: str,
    source_idempotency_key: str,
    recovery_generation: int,
    decision: AccountDrawdownDecision,
    settings: Settings,
    occurred_at: datetime,
) -> bool:
    state = recovery_state(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
    )
    _add_recovery_event(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
        event_type=RECOVERY_RESET_EVENT_TYPE,
        summary={
            "source_broker_order_pk": source_broker_order_pk,
            "source_idempotency_key": source_idempotency_key,
            "recovery_generation": recovery_generation,
            "previous_healthy_observation_count": state.healthy_observation_count,
            "gate_name": decision.gate_name,
            "snapshot_observed_at": decision.baseline_timestamp,
            "policy_fingerprint": recovery_policy_fingerprint(settings),
        },
        evidence={"decision": decision.summary, "evidence": decision.payload},
        error_message=decision.reason,
        occurred_at=occurred_at,
    )
    return True


def record_recovery_release(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    source_broker_order_pk: str,
    source_idempotency_key: str,
    recovery_idempotency_key: str,
    recovery_generation: int,
    healthy_observation_count: int,
    settings: Settings,
    occurred_at: datetime,
    gate_disabled: bool = False,
) -> None:
    state = recovery_state(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
    )
    event_type = (
        POLICY_DISABLED_RELEASE_EVENT_TYPE
        if gate_disabled
        else RECOVERY_RELEASED_EVENT_TYPE
    )
    _add_recovery_event(
        db=db,
        execution_plan_item_id=execution_plan_item_id,
        source_broker_order_pk=source_broker_order_pk,
        event_type=event_type,
        summary={
            "source_broker_order_pk": source_broker_order_pk,
            "source_idempotency_key": source_idempotency_key,
            "recovery_idempotency_key": recovery_idempotency_key,
            "recovery_generation": recovery_generation,
            "healthy_observation_count": healthy_observation_count,
            "snapshot_observed_at": (
                state.last_snapshot_observed_at.isoformat()
                if state.last_snapshot_observed_at is not None
                else None
            ),
            "required_healthy_observations": (
                settings.account_drawdown_recovery_required_healthy_observations
            ),
            "threshold_ratio": str(settings.account_drawdown_recovery_threshold_ratio),
            "policy_fingerprint": recovery_policy_fingerprint(settings),
            "gate_disabled": gate_disabled,
        },
        evidence={
            "reason": (
                "account_drawdown_gate_disabled"
                if gate_disabled
                else "account drawdown metrics remained below recovery thresholds"
            )
        },
        error_message=None,
        occurred_at=occurred_at,
    )


def _add_recovery_event(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    source_broker_order_pk: str,
    event_type: str,
    summary: dict[str, object],
    evidence: dict[str, object],
    error_message: str | None,
    occurred_at: datetime,
) -> None:
    normalized_at = _normalize_utc(occurred_at)
    sanitized_summary = sanitize_account_payload(summary)
    sanitized_evidence = sanitize_account_payload(evidence)
    if execution_plan_item_id is not None:
        db.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=execution_plan_item_id,
                event_type=event_type,
                source="broker_order_service",
                summary=sanitized_summary,
                payload=sanitized_evidence,
                error_message=error_message,
                occurred_at=normalized_at,
            )
        )
        return
    db.add(
        BrokerOrderEvent(
            broker_order_pk=source_broker_order_pk,
            event_type=event_type,
            event_source="system",
            normalized_status="rejected",
            message=error_message,
            response_payload=sanitized_evidence,
            payload=sanitized_summary,
            observed_at=normalized_at,
        )
    )


def _state_from_broker_order_events(
    events: list[BrokerOrderEvent],
) -> AccountDrawdownRecoveryState:
    latest_reset_at: datetime | None = None
    snapshot_watermark_at: datetime | None = None
    latest_matching: BrokerOrderEvent | None = None
    for event in events:
        summary = event.payload if isinstance(event.payload, dict) else {}
        snapshot_observed_at = _parse_datetime(summary.get("snapshot_observed_at"))
        if snapshot_observed_at is not None and (
            snapshot_watermark_at is None
            or snapshot_observed_at > snapshot_watermark_at
        ):
            snapshot_watermark_at = snapshot_observed_at
        if event.event_type == RECOVERY_RESET_EVENT_TYPE and latest_reset_at is None:
            latest_reset_at = _normalize_utc(event.observed_at)
        if latest_matching is None:
            latest_matching = event
    if latest_matching is None:
        return AccountDrawdownRecoveryState(
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
        )
    summary = (
        latest_matching.payload if isinstance(latest_matching.payload, dict) else {}
    )
    if latest_matching.event_type in {
        RECOVERY_RELEASED_EVENT_TYPE,
        POLICY_DISABLED_RELEASE_EVENT_TYPE,
    }:
        return AccountDrawdownRecoveryState(
            healthy_observation_count=_non_negative_int(
                summary.get("healthy_observation_count")
            ),
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
            policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
            recovery_idempotency_key=_optional_text(
                summary.get("recovery_idempotency_key")
            ),
            recovery_generation=_optional_int(summary.get("recovery_generation")),
            source_broker_order_pk=_optional_text(
                summary.get("source_broker_order_pk")
            ),
            source_idempotency_key=_optional_text(
                summary.get("source_idempotency_key")
            ),
            released=True,
        )
    if latest_matching.event_type == RECOVERY_OBSERVED_EVENT_TYPE:
        return AccountDrawdownRecoveryState(
            healthy_observation_count=_non_negative_int(
                summary.get("healthy_observation_count")
            ),
            last_snapshot_observed_at=snapshot_watermark_at,
            last_unhealthy_at=latest_reset_at,
            policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
        )
    return AccountDrawdownRecoveryState(
        last_snapshot_observed_at=snapshot_watermark_at,
        last_unhealthy_at=latest_reset_at,
        policy_fingerprint=_optional_text(summary.get("policy_fingerprint")),
    )


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _normalize_utc(parsed)


def _non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value
