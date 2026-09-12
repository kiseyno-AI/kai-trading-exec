from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session, undefer

from app.api.schemas.buy_authority import (
    ACK_SCHEMA_VERSION,
    OBSERVATION_ID_PREFIX,
    CoreBuyAuthorityAckV1,
    CoreBuyAuthorityObservationV1,
    core_buy_authority_material_fingerprint,
)
from app.core.config import Settings
from app.models.runtime import (
    CoreBuyAuthorityObservation,
    CoreBuyAuthorityState,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    RuntimeControlState,
)
from app.services.execution_account_route import (
    classify_kis_account_route,
    configured_kis_account_reference,
    redact_account_references,
)
from app.services.plan_judgment_actions import (
    BUY_MUTABLE_STATUSES,
    BUY_WATCH_INTENTS,
    RISK_OFF_EXPIRABLE_STATUSES,
)
from app.services.position_identity import build_position_identity
from app.services.runtime_controls import RUNTIME_CONTROL_ID, ensure_runtime_control_state


SOURCE = "core_live_market_buy_authority"
ADJUSTMENT_KEY = "core_buy_authority"
CORE_V4_BINDING_KEY = "_core_v4_contract_binding"
CORE_V4_BINDING_VERSION = "core-v4-persisted-binding-v1"
SEOUL = ZoneInfo("Asia/Seoul")


class CoreBuyAuthorityConflictError(RuntimeError):
    pass


class CoreBuyAuthorityScopeError(RuntimeError):
    pass


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _scope_identity(
    *,
    settings: Settings,
    command: CoreBuyAuthorityObservationV1,
) -> tuple[str, str]:
    scope = command.scope
    if scope.runtime_mode != settings.runtime_mode:
        raise CoreBuyAuthorityScopeError(
            "authority runtime_mode does not match the execution runtime"
        )
    if (scope.provider, scope.market, scope.currency) != ("kis", "KRX", "KRW"):
        raise CoreBuyAuthorityScopeError("authority route must be exactly KIS/KRX/KRW")
    expected_reference = configured_kis_account_reference(settings)
    if expected_reference is None or scope.account_reference != expected_reference:
        raise CoreBuyAuthorityScopeError(
            "authority account_reference does not match the configured KIS route"
        )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    material = {
        "provider": "kis",
        "market": "KRX",
        "currency": "KRW",
        "runtime_mode": settings.runtime_mode,
        "account_scope": identity.account_scope,
    }
    encoded = json.dumps(
        material,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), identity.account_scope


def _lock_global_control(*, settings: Settings, db: Session) -> RuntimeControlState:
    ensure_runtime_control_state(settings=settings, db=db)
    if db.get_bind().dialect.name == "sqlite":
        db.execute(
            update(RuntimeControlState)
            .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
            .values(hourly_risk_off_epoch=RuntimeControlState.hourly_risk_off_epoch)
        )
    state = db.scalar(
        select(RuntimeControlState)
        .where(RuntimeControlState.id == RUNTIME_CONTROL_ID)
        .with_for_update()
    )
    if state is None:
        raise RuntimeError("runtime control state disappeared during authority transition")
    return state


def _load_state(
    *,
    db: Session,
    scope_fingerprint: str,
    lock: bool,
) -> CoreBuyAuthorityState | None:
    query = (
        select(CoreBuyAuthorityState)
        .where(CoreBuyAuthorityState.scope_fingerprint == scope_fingerprint)
        .options(undefer(CoreBuyAuthorityState.context))
    )
    if lock:
        query = query.with_for_update()
    return db.scalar(query)


def _temporal_freshness_failure(
    *,
    settings: Settings,
    command: CoreBuyAuthorityObservationV1,
    now: datetime,
) -> str | None:
    if command.action != "allow":
        return None
    observed_at = command.source_observed_at
    if observed_at is None:
        return "source_observed_at_missing"
    observed_utc = _utc(observed_at)
    now_utc = _utc(now)
    if any(
        evidence.observed_at is not None
        and _utc(evidence.observed_at)
        > now_utc
        + timedelta(
            seconds=settings.core_buy_authority_max_future_skew_seconds
        )
        for evidence in command.market_evidence
    ):
        return "market_evidence_observed_at_in_future"
    if observed_utc > now_utc + timedelta(
        seconds=settings.core_buy_authority_max_future_skew_seconds
    ):
        return "source_observed_at_in_future"
    if observed_utc < now_utc - timedelta(
        seconds=settings.core_buy_authority_max_observation_age_seconds
    ):
        return "source_observation_stale"
    if command.market_date != now_utc.astimezone(SEOUL).date():
        return "market_date_not_current"
    return None


def _trusted_block_source_observed_at(
    *,
    settings: Settings,
    command: CoreBuyAuthorityObservationV1,
    now: datetime,
) -> datetime | None:
    """Return the newest trustworthy market observation that caused a block."""
    if command.action != "block_new_buy":
        return None
    now_utc = _utc(now)
    oldest_allowed = now_utc - timedelta(
        seconds=settings.core_buy_authority_max_observation_age_seconds
    )
    newest_allowed = now_utc + timedelta(
        seconds=settings.core_buy_authority_max_future_skew_seconds
    )
    current_market_date = now_utc.astimezone(SEOUL).date()
    candidates = [
        _utc(evidence.observed_at)
        for evidence in command.market_evidence
        if evidence.action == "block_new_buy"
        and evidence.status == "available"
        and evidence.authoritative is True
        and evidence.observed_at is not None
        and oldest_allowed <= _utc(evidence.observed_at) <= newest_allowed
        and evidence.observed_at.astimezone(SEOUL).date() == current_market_date
    ]
    return max(candidates) if candidates else None


def _freshness_failure(
    *,
    settings: Settings,
    command: CoreBuyAuthorityObservationV1,
    state: CoreBuyAuthorityState | None,
    now: datetime,
) -> str | None:
    if command.action != "allow":
        return None
    temporal_failure = _temporal_freshness_failure(
        settings=settings,
        command=command,
        now=now,
    )
    if temporal_failure is not None:
        return temporal_failure
    observed_utc = _utc(command.source_observed_at)  # type: ignore[arg-type]
    if state is not None and state.fence_active:
        recovery_floors = [
            _utc(value)
            for value in (
                state.last_blocked_at,
                state.last_block_source_observed_at,
            )
            if value is not None
        ]
        if recovery_floors and observed_utc <= max(recovery_floors):
            return "source_observation_precedes_latest_block"
    watermark = state.source_observed_at if state is not None else None
    if watermark is not None and observed_utc < _utc(watermark):
        return "source_observation_regressed"
    return None


def _new_state(
    *,
    settings: Settings,
    command: CoreBuyAuthorityObservationV1,
    scope_fingerprint: str,
    account_scope: str,
    now: datetime,
) -> CoreBuyAuthorityState:
    return CoreBuyAuthorityState(
        scope_fingerprint=scope_fingerprint,
        provider="kis",
        market="KRX",
        currency="KRW",
        runtime_mode=settings.runtime_mode,
        account_scope=account_scope,
        fence_active=True,
        epoch=0,
        delivery_status="pending",
        projection_pending=True,
        healthy_observation_count=0,
        last_observation_id=command.observation_id,
        last_material_fingerprint=command.material_fingerprint,
        last_action=command.action,
        market_date=command.market_date,
        source_observed_at=None,
        last_block_source_observed_at=None,
        last_received_at=now,
        context={},
    )


def _ack(
    *,
    status: str,
    command: CoreBuyAuthorityObservationV1,
    state: CoreBuyAuthorityState,
    changed_item_count: int,
) -> CoreBuyAuthorityAckV1:
    return CoreBuyAuthorityAckV1.model_validate(
        {
            "schema_version": ACK_SCHEMA_VERSION,
            "status": status,
            "observation_id": command.observation_id,
            "material_fingerprint": command.material_fingerprint,
            "scope_fingerprint": state.scope_fingerprint,
            "fence_active": state.fence_active,
            "projection_pending": state.projection_pending,
            "healthy_observation_count": state.healthy_observation_count,
            "changed_item_count": changed_item_count,
            "accepted_source_observed_at": (
                _utc(state.source_observed_at)
                if state.source_observed_at is not None
                else None
            ),
        }
    )


def _validate_material(command: CoreBuyAuthorityObservationV1) -> None:
    payload = command.model_dump(mode="python")
    expected = core_buy_authority_material_fingerprint(payload)
    if (
        not hmac.compare_digest(expected, command.material_fingerprint)
        or command.observation_id != f"{OBSERVATION_ID_PREFIX}{expected}"
    ):
        raise CoreBuyAuthorityConflictError(
            "authority observation identity conflicts with its canonical material"
        )


def apply_core_buy_authority_observation(
    *,
    settings: Settings,
    db: Session,
    command: CoreBuyAuthorityObservationV1,
    now: datetime | None = None,
) -> CoreBuyAuthorityAckV1:
    received_at = _utc(now or datetime.now(UTC))
    scope_fingerprint, account_scope = _scope_identity(
        settings=settings,
        command=command,
    )
    _lock_global_control(settings=settings, db=db)
    state = _load_state(
        db=db,
        scope_fingerprint=scope_fingerprint,
        lock=True,
    )
    existing = db.get(CoreBuyAuthorityObservation, command.observation_id)
    if existing is not None:
        if (
            existing.scope_fingerprint == scope_fingerprint
            and hmac.compare_digest(
                existing.material_fingerprint,
                command.material_fingerprint,
            )
        ):
            if state is None:
                raise RuntimeError("authority observation exists without its scope state")
            temporal_failure = _temporal_freshness_failure(
                settings=settings,
                command=command,
                now=received_at,
            )
            if temporal_failure is not None:
                state.fence_active = True
                state.epoch += 1
                state.delivery_status = "rejected_stale"
                state.projection_pending = True
                state.healthy_observation_count = 0
                state.last_healthy_source_observed_at = None
                state.last_blocked_at = received_at
                state.last_received_at = received_at
                state.context = {
                    "status": "blocked",
                    "observation_id": command.observation_id,
                    "material_fingerprint": command.material_fingerprint,
                    "reason_codes": list(command.reason_codes),
                    "freshness_failure": temporal_failure,
                    "received_at": received_at.isoformat(),
                }
                db.commit()
                changed = _project_scope(
                    settings=settings,
                    db=db,
                    state_scope_fingerprint=scope_fingerprint,
                    mode="block",
                    command=command,
                    now=received_at,
                )
                state = _load_state(
                    db=db,
                    scope_fingerprint=scope_fingerprint,
                    lock=False,
                )
                if state is None:
                    raise RuntimeError(
                        "authority scope state disappeared after stale duplicate"
                    )
                return _ack(
                    status="rejected_stale",
                    command=command,
                    state=state,
                    changed_item_count=changed,
                )
            if state.projection_pending:
                context = state.context if isinstance(state.context, dict) else {}
                context_status = str(context.get("status") or "")
                projection_mode = (
                    "recover"
                    if context_status == "recovery_ready"
                    else "observe"
                    if context_status == "observing"
                    else "block"
                )
                changed = _project_scope(
                    settings=settings,
                    db=db,
                    state_scope_fingerprint=scope_fingerprint,
                    mode=projection_mode,
                    command=command,
                    now=received_at,
                )
                state = _load_state(
                    db=db,
                    scope_fingerprint=scope_fingerprint,
                    lock=False,
                )
                if state is None:
                    raise RuntimeError(
                        "authority scope state disappeared after projection retry"
                    )
                return _ack(
                    status=(
                        "rejected_stale"
                        if state.delivery_status == "rejected_stale"
                        else "duplicate"
                    ),
                    command=command,
                    state=state,
                    changed_item_count=changed,
                )
            return _ack(
                status="duplicate",
                command=command,
                state=state,
                changed_item_count=0,
            )
        if state is None:
            state = _new_state(
                settings=settings,
                command=command,
                scope_fingerprint=scope_fingerprint,
                account_scope=account_scope,
                now=received_at,
            )
            db.add(state)
        state.fence_active = True
        state.epoch += 1
        state.delivery_status = "conflict"
        state.projection_pending = True
        state.healthy_observation_count = 0
        state.last_received_at = received_at
        state.context = {
            "status": "conflict",
            "conflicting_observation_id": command.observation_id,
            "received_at": received_at.isoformat(),
        }
        db.commit()
        _project_scope(
            settings=settings,
            db=db,
            state_scope_fingerprint=scope_fingerprint,
            mode="block",
            command=command,
            now=received_at,
        )
        raise CoreBuyAuthorityConflictError(
            "observation_id was previously bound to different authority material"
        )

    try:
        _validate_material(command)
    except CoreBuyAuthorityConflictError:
        if state is None:
            state = _new_state(
                settings=settings,
                command=command,
                scope_fingerprint=scope_fingerprint,
                account_scope=account_scope,
                now=received_at,
            )
            db.add(state)
        state.fence_active = True
        state.epoch += 1
        state.delivery_status = "conflict"
        state.projection_pending = True
        state.healthy_observation_count = 0
        state.last_received_at = received_at
        state.context = {
            "status": "conflict",
            "conflicting_observation_id": command.observation_id,
            "received_at": received_at.isoformat(),
        }
        db.commit()
        _project_scope(
            settings=settings,
            db=db,
            state_scope_fingerprint=scope_fingerprint,
            mode="block",
            command=command,
            now=received_at,
        )
        raise

    state_was_new = state is None
    if state is None:
        state = _new_state(
            settings=settings,
            command=command,
            scope_fingerprint=scope_fingerprint,
            account_scope=account_scope,
            now=received_at,
        )
        db.add(state)
        db.flush()

    freshness_failure = _freshness_failure(
        settings=settings,
        command=command,
        state=state,
        now=received_at,
    )
    prior_active = bool(state.fence_active and not state_was_new)
    prior_blocked_at = state.last_blocked_at
    prior_healthy_count = int(state.healthy_observation_count or 0)
    trusted_block_source = _trusted_block_source_observed_at(
        settings=settings,
        command=command,
        now=received_at,
    )
    state.last_observation_id = command.observation_id
    state.last_material_fingerprint = command.material_fingerprint
    state.last_action = command.action
    state.market_date = command.market_date
    state.last_received_at = received_at
    state.delivery_status = "accepted" if freshness_failure is None else "rejected_stale"
    state.projection_pending = True

    if command.action != "allow" or freshness_failure is not None:
        state.fence_active = True
        state.epoch += 1
        state.healthy_observation_count = 0
        state.last_healthy_source_observed_at = None
        state.last_blocked_at = received_at
        if trusted_block_source is not None and (
            state.last_block_source_observed_at is None
            or trusted_block_source > _utc(state.last_block_source_observed_at)
        ):
            state.last_block_source_observed_at = trusted_block_source
        # Blocking is always fail-closed, including malformed/future source
        # time.  It must never advance the trusted healthy watermark used to
        # decide whether later recovery evidence regressed.
        projection_mode = "block"
    elif not prior_active:
        state.fence_active = False
        state.healthy_observation_count = 0
        state.source_observed_at = command.source_observed_at
        state.last_healthy_source_observed_at = command.source_observed_at
        state.projection_pending = False
        projection_mode = "none"
    else:
        source_observed_at = _utc(command.source_observed_at)  # type: ignore[arg-type]
        last_healthy = state.last_healthy_source_observed_at
        advances = bool(
            last_healthy is None
            or source_observed_at
            >= _utc(last_healthy)
            + timedelta(
                seconds=(
                    settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
                )
            )
        )
        healthy_count = prior_healthy_count + (1 if advances else 0)
        state.healthy_observation_count = min(
            settings.hourly_plan_judgment_recovery_required_healthy_observations,
            healthy_count,
        )
        state.source_observed_at = command.source_observed_at
        if advances:
            state.last_healthy_source_observed_at = command.source_observed_at
        cooldown_satisfied = received_at >= _utc(
            prior_blocked_at or received_at
        ) + timedelta(
            seconds=settings.hourly_plan_judgment_recovery_cooldown_seconds
        )
        recovery_ready = bool(
            state.healthy_observation_count
            >= settings.hourly_plan_judgment_recovery_required_healthy_observations
            and cooldown_satisfied
        )
        state.fence_active = True
        projection_mode = "recover" if recovery_ready else "observe"

    state.context = {
        "status": (
            "blocked"
            if projection_mode == "block"
            else "healthy"
            if projection_mode == "none"
            else "recovery_ready"
            if projection_mode == "recover"
            else "observing"
        ),
        "observation_id": command.observation_id,
        "material_fingerprint": command.material_fingerprint,
        "reason_codes": list(command.reason_codes),
        "freshness_failure": freshness_failure,
        "received_at": received_at.isoformat(),
        "required_healthy_observations": (
            settings.hourly_plan_judgment_recovery_required_healthy_observations
        ),
        "cooldown_seconds": settings.hourly_plan_judgment_recovery_cooldown_seconds,
        "min_observation_interval_seconds": (
            settings.hourly_plan_judgment_recovery_min_observation_interval_seconds
        ),
    }
    ledger = CoreBuyAuthorityObservation(
        observation_id=command.observation_id,
        scope_fingerprint=scope_fingerprint,
        material_fingerprint=command.material_fingerprint,
        action=command.action,
        status=state.delivery_status,
        source_observed_at=command.source_observed_at,
        received_at=received_at,
        payload=redact_account_references(command.model_dump(mode="json")),
    )
    db.add(ledger)
    db.commit()

    changed = 0
    if projection_mode != "none":
        changed = _project_scope(
            settings=settings,
            db=db,
            state_scope_fingerprint=scope_fingerprint,
            mode=projection_mode,
            command=command,
            now=received_at,
        )
    state = _load_state(
        db=db,
        scope_fingerprint=scope_fingerprint,
        lock=False,
    )
    if state is None:
        raise RuntimeError("authority scope state disappeared after projection")
    status = "rejected_stale" if freshness_failure is not None else "applied"
    response = _ack(
        status=status,
        command=command,
        state=state,
        changed_item_count=changed,
    )
    ledger = db.get(CoreBuyAuthorityObservation, command.observation_id)
    if ledger is not None:
        ledger.ack_payload = response.model_dump(mode="json")
        db.commit()
    return response


def _project_scope(
    *,
    settings: Settings,
    db: Session,
    state_scope_fingerprint: str,
    mode: str,
    command: CoreBuyAuthorityObservationV1,
    now: datetime,
) -> int:
    _lock_global_control(settings=settings, db=db)
    state = _load_state(
        db=db,
        scope_fingerprint=state_scope_fingerprint,
        lock=True,
    )
    if state is None:
        raise RuntimeError("authority scope state disappeared during projection")
    if (
        state.projection_pending is not True
        or state.last_observation_id != command.observation_id
        or not hmac.compare_digest(
            str(state.last_material_fingerprint or ""),
            command.material_fingerprint,
        )
        or state.last_action != command.action
    ):
        # Another observation won after this request committed phase one.  The
        # newer state owns projection; a delayed request must not mutate its
        # plan items or clear its pending bit.
        return 0
    if db.get_bind().dialect.name == "postgresql":
        db.execute(text("LOCK TABLE execution_plan_items IN EXCLUSIVE MODE"))
    items = list(
        db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlan, ExecutionPlan.id == ExecutionPlanItem.execution_plan_id)
            .options(undefer(ExecutionPlanItem.risk_context))
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.provider == "kis",
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.intent.in_(BUY_WATCH_INTENTS),
            )
            .order_by(ExecutionPlanItem.id)
            .with_for_update()
        ).all()
    )
    matching = [item for item in items if _item_matches_scope(settings=settings, item=item)]
    changed = 0
    if mode == "block":
        for item in matching:
            if _project_block(item=item, command=command, now=now):
                changed += 1
                _audit(
                    db=db,
                    item=item,
                    event_type="core_buy_authority_blocked",
                    command=command,
                    now=now,
                )
        state.fence_active = True
    elif mode == "observe":
        for item in matching:
            _record_recovery_progress(item=item, state=state, now=now)
    elif mode == "recover":
        for item in matching:
            restored = _restore_blocked_item(item=item, state=state, now=now)
            if restored:
                changed += 1
                _audit(
                    db=db,
                    item=item,
                    event_type="core_buy_authority_recovered",
                    command=command,
                    now=now,
                )
        state.fence_active = False
        state.context = {
            **(state.context if isinstance(state.context, dict) else {}),
            "status": "recovered",
            "recovered_at": now.isoformat(),
        }
    state.projection_pending = False
    db.commit()
    return changed


def _item_matches_scope(*, settings: Settings, item: ExecutionPlanItem) -> bool:
    risk = item.risk_context if isinstance(item.risk_context, dict) else {}
    binding = risk.get(CORE_V4_BINDING_KEY)
    exchange = str(risk.get("exchange") or item.venue or "").strip().upper()
    currency = str(risk.get("currency") or "").strip().upper()
    if (
        not isinstance(binding, dict)
        or binding.get("version") != CORE_V4_BINDING_VERSION
        or str(item.provider or "").strip().lower() != "kis"
        or str(item.asset_class or "").strip().lower() not in {"stock", "kr_equity"}
        or exchange != "KRX"
        or currency != "KRW"
    ):
        return False
    return (
        classify_kis_account_route(
            settings=settings,
            provider=item.provider,
            risk_context=risk,
            quantity_policy=item.quantity_policy,
        )
        == "active"
    )


def _project_block(
    *,
    item: ExecutionPlanItem,
    command: CoreBuyAuthorityObservationV1,
    now: datetime,
) -> bool:
    risk = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    existing = risk.get(ADJUSTMENT_KEY)
    if isinstance(existing, dict) and existing.get("source") == SOURCE:
        existing = dict(existing)
        marker_active = existing.get("action") == "block_new_buy"
        if not marker_active:
            existing["previous"] = {
                "status": item.status,
                "trigger_price": (
                    str(item.trigger_price)
                    if item.trigger_price is not None
                    else None
                ),
                "limit_price": (
                    str(item.limit_price) if item.limit_price is not None else None
                ),
            }
        existing["action"] = "block_new_buy"
        existing["observation_id"] = command.observation_id
        existing["material_fingerprint"] = command.material_fingerprint
        existing["reason_codes"] = list(command.reason_codes)
        existing["applied_at"] = now.isoformat()
        existing["recovery"] = {
            "status": "blocked",
            "healthy_observation_count": 0,
        }
        risk[ADJUSTMENT_KEY] = existing
        item.risk_context = risk
        if item.status != "watch_expired" and item.status in RISK_OFF_EXPIRABLE_STATUSES:
            item.status = "watch_expired"
            item.updated_at = now
            return True
        return False
    if item.status not in RISK_OFF_EXPIRABLE_STATUSES:
        return False
    risk[ADJUSTMENT_KEY] = {
        "schema_version": "core_buy_authority_projection_v1",
        "source": SOURCE,
        "action": "block_new_buy",
        "observation_id": command.observation_id,
        "material_fingerprint": command.material_fingerprint,
        "reason_codes": list(command.reason_codes),
        "applied_at": now.isoformat(),
        "previous": {
            "status": item.status,
            "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
            "limit_price": str(item.limit_price) if item.limit_price is not None else None,
        },
        "recovery": {
            "status": "blocked",
            "healthy_observation_count": 0,
        },
    }
    item.risk_context = risk
    item.status = "watch_expired"
    item.updated_at = now
    return True


def _record_recovery_progress(
    *,
    item: ExecutionPlanItem,
    state: CoreBuyAuthorityState,
    now: datetime,
) -> None:
    risk = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    adjustment = risk.get(ADJUSTMENT_KEY)
    if not isinstance(adjustment, dict) or adjustment.get("source") != SOURCE:
        return
    updated = dict(adjustment)
    updated["recovery"] = {
        "status": "observing",
        "healthy_observation_count": state.healthy_observation_count,
        "last_source_observed_at": (
            state.last_healthy_source_observed_at.isoformat()
            if state.last_healthy_source_observed_at is not None
            else None
        ),
        "updated_at": now.isoformat(),
    }
    risk[ADJUSTMENT_KEY] = updated
    item.risk_context = risk


def _restore_blocked_item(
    *,
    item: ExecutionPlanItem,
    state: CoreBuyAuthorityState,
    now: datetime,
) -> bool:
    risk = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    adjustment = risk.get(ADJUSTMENT_KEY)
    if (
        not isinstance(adjustment, dict)
        or adjustment.get("source") != SOURCE
        or adjustment.get("schema_version")
        != "core_buy_authority_projection_v1"
        or adjustment.get("action") != "block_new_buy"
    ):
        return False
    previous = adjustment.get("previous")
    previous_status = (
        str(previous.get("status") or "") if isinstance(previous, dict) else ""
    )
    if item.status != "watch_expired" or previous_status not in BUY_MUTABLE_STATUSES:
        updated = dict(adjustment)
        updated["recovery"] = {
            "status": "not_restored_terminal",
            "healthy_observation_count": state.healthy_observation_count,
            "updated_at": now.isoformat(),
        }
        risk[ADJUSTMENT_KEY] = updated
        item.risk_context = risk
        return False
    item.status = previous_status
    updated = dict(adjustment)
    updated["action"] = "recovered"
    updated["recovery"] = {
        "status": "resumed",
        "healthy_observation_count": state.healthy_observation_count,
        "resumed_at": now.isoformat(),
    }
    risk[ADJUSTMENT_KEY] = updated
    item.risk_context = risk
    item.updated_at = now
    return True


def _audit(
    *,
    db: Session,
    item: ExecutionPlanItem,
    event_type: str,
    command: CoreBuyAuthorityObservationV1,
    now: datetime,
) -> None:
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=event_type,
            source=SOURCE,
            summary={
                "symbol": item.symbol,
                "observation_id": command.observation_id,
                "action": command.action,
            },
            payload={"reason_codes": list(command.reason_codes)},
            occurred_at=now,
            created_at=now,
            updated_at=now,
        )
    )


def core_buy_authority_block_reason(
    *,
    settings: Settings,
    db: Session,
    plan_item: ExecutionPlanItem | None,
    now: datetime | None = None,
    lock: bool = False,
) -> str | None:
    if plan_item is None or not _item_matches_scope(settings=settings, item=plan_item):
        return None
    observed_at = _utc(now or datetime.now(UTC))
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    material = {
        "provider": "kis",
        "market": "KRX",
        "currency": "KRW",
        "runtime_mode": settings.runtime_mode,
        "account_scope": identity.account_scope,
    }
    scope_fingerprint = hashlib.sha256(
        json.dumps(
            material,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    state = _load_state(
        db=db,
        scope_fingerprint=scope_fingerprint,
        lock=lock,
    )
    if state is None:
        return "core_buy_authority_missing"
    if state.projection_pending or state.delivery_status in {"conflict", "pending"}:
        return "core_buy_authority_projection_pending"
    if state.fence_active or state.last_action != "allow":
        return "core_buy_authority_blocked"
    source_observed_at = state.source_observed_at
    if source_observed_at is None:
        return "core_buy_authority_unavailable"
    source_utc = _utc(source_observed_at)
    if (
        source_utc
        < observed_at
        - timedelta(seconds=settings.core_buy_authority_max_observation_age_seconds)
        or source_utc
        > observed_at
        + timedelta(seconds=settings.core_buy_authority_max_future_skew_seconds)
        or state.market_date != observed_at.astimezone(SEOUL).date()
    ):
        return "core_buy_authority_stale"
    return None


def project_plan_item_for_core_buy_authority(
    *,
    settings: Settings,
    db: Session,
    plan_item: ExecutionPlanItem,
    now: datetime | None = None,
) -> bool:
    if not _item_matches_scope(settings=settings, item=plan_item):
        return False
    observed_at = _utc(now or datetime.now(UTC))
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    scope_material = json.dumps(
        {
            "provider": "kis",
            "market": "KRX",
            "currency": "KRW",
            "runtime_mode": settings.runtime_mode,
            "account_scope": identity.account_scope,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    state = _load_state(
        db=db,
        scope_fingerprint=hashlib.sha256(scope_material).hexdigest(),
        lock=True,
    )
    if state is None or not state.fence_active:
        return False
    context = state.context if isinstance(state.context, dict) else {}
    command_payload = {
        "schema_version": "core_exec_buy_authority_observation_v1",
        "authority_schema_version": "current_live_market_buy_authority_v1",
        "source": "current_market_snapshot_cache",
        "observation_id": state.last_observation_id,
        "material_fingerprint": state.last_material_fingerprint,
        "scope": {
            "provider": "kis",
            "market": "KRX",
            "currency": "KRW",
            "runtime_mode": settings.runtime_mode,
            "account_reference": configured_kis_account_reference(settings),
        },
        "market_date": state.market_date,
        "evaluated_at": state.last_received_at,
        "source_observed_at": state.source_observed_at,
        "action": "block_new_buy",
        "authoritative": False,
        "source_status": "unavailable",
        "reason_codes": context.get("reason_codes") or ["active_core_buy_authority_fence"],
        "required_markets": ["KOSPI", "KOSDAQ"],
        "market_evidence": [
            {
                "market": market,
                "status": "unavailable",
                "authoritative": False,
                "action": "block_new_buy",
                "observed_at": None,
                "reason_code": "active_core_buy_authority_fence",
            }
            for market in ("KOSPI", "KOSDAQ")
        ],
    }
    # The synthetic object is used only to carry the already-validated durable
    # observation identity into the late-candidate projection. Its material is
    # intentionally not revalidated against the original payload.
    command = CoreBuyAuthorityObservationV1.model_construct(**command_payload)
    return _project_block(item=plan_item, command=command, now=observed_at)
