from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy import or_, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, undefer

from app.broker.base import NormalizedBrokerOrderStatus
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.account_drawdown_recovery import (
    account_snapshot_freshness_failure,
    recovery_account_fingerprint,
)
from app.services.execution_account_route import validate_kis_account_route
from app.services.kis_krx_flat_rebind_contract import (
    KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY,
    KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE,
    KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME,
    KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION,
    KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS,
    is_lower_hex_sha256,
    kis_krx_flat_absence_proof_digest,
    kis_krx_flat_configured_account_binding_digest,
    kis_krx_flat_evidence_fingerprint,
)
from app.services.order_intent_serialization import (
    order_intent_file_lock,
    order_intent_process_lock,
    order_intent_scope_key_for_account,
)
from app.services.position_identity import (
    PositionIdentity,
    PositionIdentityConflictError,
    build_position_identity,
    position_identity_from_columns,
    position_identity_from_payload,
)


_RETIRED_HISTORICAL_ORDER_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.CANCELED.value,
        NormalizedBrokerOrderStatus.REJECTED.value,
        NormalizedBrokerOrderStatus.FAILED.value,
        NormalizedBrokerOrderStatus.EXPIRED.value,
        NormalizedBrokerOrderStatus.RECONCILE_WINDOW_EXPIRED.value,
    }
)


class RevocationExposureSerializationBusy(RuntimeError):
    """The exact account/order scope could not be reserved without waiting."""


@contextmanager
def serialized_revocation_exposure(
    *,
    db: Session,
    settings: Settings,
    items: Sequence[ExecutionPlanItem],
) -> Iterator[None]:
    """Hold the shared order-intent scope through the revocation commit."""
    identities = {
        identity
        for item in items
        if (identity := _expected_identity(settings=settings, item=item)) is not None
    }
    if not identities:
        yield
        return

    local_scope = order_intent_scope_key_for_account(
        provider="kis",
        runtime_mode=settings.runtime_mode,
        account_scope=recovery_account_fingerprint(settings),
        market="KRW",
    )
    with ExitStack() as serialization:
        process_lock = order_intent_process_lock(local_scope)
        if not process_lock.acquire(blocking=False):
            raise RevocationExposureSerializationBusy(
                "revocation exposure scope is busy"
            )
        serialization.callback(process_lock.release)

        database = make_url(settings.database_url)
        if (
            database.drivername.startswith("sqlite")
            and database.database
            and database.database != ":memory:"
        ):
            try:
                serialization.enter_context(
                    order_intent_file_lock(local_scope, blocking=False)
                )
            except OSError as exc:
                raise RevocationExposureSerializationBusy(
                    "revocation exposure scope is busy"
                ) from exc

        bind = db.get_bind()
        dialect = (
            bind.dialect.name
            if bind is not None
            else database.get_backend_name()
        )
        try:
            if dialect == "sqlite":
                # SQLite ignores SELECT ... FOR UPDATE. Reserve its sole writer
                # before reading orders, fills, and positions so reconciliation
                # cannot change broker truth before the revocation commit.
                db.connection().exec_driver_sql("BEGIN IMMEDIATE")
            for identity in sorted(
                identities,
                key=lambda value: value.account_scope,
            ):
                _acquire_revocation_order_intent_serialization(
                    db=db,
                    settings=settings,
                    identity=identity,
                )
            if dialect == "postgresql":
                # Exact-route uniqueness prevents a second current position,
                # but alternate scoped positions remain possible phantoms.
                db.execute(
                    text(
                        "LOCK TABLE execution_positions "
                        "IN EXCLUSIVE MODE NOWAIT"
                    )
                )
        except DBAPIError as exc:
            raise RevocationExposureSerializationBusy(
                "revocation exposure scope could not be reserved"
            ) from exc
        yield


def item_has_revocation_possible_exposure(
    *,
    db: Session,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    settings: Settings | None,
    evaluated_at: datetime | None = None,
) -> bool:
    """Allow only proven-flat, pre-authority history during Core revocation."""
    expected_identity = _expected_identity(settings=settings, item=item)
    if db.scalar(
        select(BrokerOrder.id)
        .where(BrokerOrder.execution_plan_item_id == item.id)
        .with_for_update()
        .limit(1)
    ) is not None:
        return True

    positions = db.scalars(
        select(ExecutionPosition)
        .options(undefer(ExecutionPosition.raw_payload))
        .where(
            ExecutionPosition.mode == plan.mode,
            ExecutionPosition.symbol == item.symbol,
        )
        .with_for_update()
    ).all()
    if any(not _position_is_numerically_flat(position) for position in positions):
        return True

    provider = str(item.provider or "").strip().lower()
    order_rows = [
        (order, linked_plan_id)
        for order, linked_plan_id in db.execute(
            select(BrokerOrder, ExecutionPlanItem.execution_plan_id)
            .outerjoin(
                ExecutionPlanItem,
                ExecutionPlanItem.id == BrokerOrder.execution_plan_item_id,
            )
            .where(
                BrokerOrder.mode == plan.mode,
                BrokerOrder.symbol == item.symbol,
                or_(
                    BrokerOrder.execution_plan_item_id.is_(None),
                    BrokerOrder.execution_plan_item_id != item.id,
                ),
            )
            .with_for_update(of=BrokerOrder)
        ).all()
        if _unknown_or_matches(order.broker_name, provider)
    ]
    if not order_rows:
        return False

    history_cutoff = _plan_history_cutoff(plan)
    proof_cutoff = _quarantine_proof_cutoff(db=db, plan=plan, item=item)
    if history_cutoff is None or proof_cutoff is None:
        return True
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = str(risk_context.get("exchange") or item.venue or "").strip().upper()
    historical_venues = {"", provider.upper(), exchange}
    orders = [order for order, _linked_plan_id in order_rows]
    if any(
        order.execution_plan_item_id is None
        or linked_plan_id in {None, plan.id}
        or str(order.broker_name or "").strip().lower() != provider
        or str(order.venue or "").strip().upper() not in historical_venues
        or str(order.status or "").strip().lower()
        not in _RETIRED_HISTORICAL_ORDER_STATUSES
        or not _order_predates_authority(order, history_cutoff)
        for order, linked_plan_id in order_rows
    ):
        return True

    fills = db.scalars(
        select(ExecutionFill)
        .where(ExecutionFill.broker_order_pk.in_({order.id for order in orders}))
        .with_for_update()
    ).all()
    if any(
        not isinstance(fill.filled_at, datetime)
        or _utc(fill.filled_at) >= history_cutoff
        for fill in fills
    ):
        return True
    proof_cutoff = max(
        proof_cutoff,
        *(
            _utc(timestamp)
            for order in orders
            for timestamp in (
                order.created_at,
                order.submitted_at,
                order.updated_at,
                order.last_broker_sync_at,
            )
            if isinstance(timestamp, datetime)
        ),
        *(_utc(fill.filled_at) for fill in fills),
    )
    return not _current_flat_proof_matches(
        db=db,
        plan=plan,
        item=item,
        positions=positions,
        expected_identity=expected_identity,
        settings=settings,
        proof_cutoff=proof_cutoff,
        evaluated_at=evaluated_at or datetime.now(UTC),
    )


def _current_flat_proof_matches(
    *,
    db: Session,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    positions: list[ExecutionPosition],
    expected_identity: PositionIdentity | None,
    settings: Settings | None,
    proof_cutoff: datetime,
    evaluated_at: datetime,
) -> bool:
    if settings is None or expected_identity is None or len(positions) != 1:
        return False
    position = positions[0]
    proof = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    try:
        column_identity = position_identity_from_columns(position)
        payload_identity = position_identity_from_payload(proof)
        configured_account_binding_digest = (
            kis_krx_flat_configured_account_binding_digest(settings=settings)
        )
    except PositionIdentityConflictError:
        return False
    observed_at = _aware_datetime(proof.get("snapshot_observed_at"))
    absence_digest = (
        proof.get("absence_proof_digest")
        if is_lower_hex_sha256(proof.get("absence_proof_digest"))
        else None
    )
    if not (
        _position_is_canonical_bound_flat(position)
        and column_identity == expected_identity
        and payload_identity == expected_identity
        and set(proof) == KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS
        and proof.get("provider") == "kis"
        and proof.get("mode") == plan.mode
        and proof.get("asset_class") == "equity"
        and proof.get("exchange") == "KRX"
        and proof.get("snapshot_source") == "kis:account_snapshot"
        and observed_at is not None
        and observed_at > proof_cutoff
        and _same_instant(position.last_reconciled_at, observed_at)
        and account_snapshot_freshness_failure(
            observed_at=observed_at,
            now=evaluated_at,
            settings=settings,
        )
        is None
        and proof.get("holdings_coverage") == "complete"
        and proof.get("open_orders_coverage") == "complete"
        and proof.get("holding_absent") is True
        and proof.get("open_order_absent") is True
        and proof.get("historical_account_attributed") is False
        and proof.get("current_flat_truth_bound") is True
        and proof.get("configured_account_binding_digest")
        == configured_account_binding_digest
        and proof.get("safety_policy_version")
        == KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
        and absence_digest
        == kis_krx_flat_absence_proof_digest(
            runtime_mode=plan.mode,
            identity=expected_identity,
            symbol=item.symbol,
            snapshot_source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holding_absent=True,
            open_order_absent=True,
            configured_account_binding_digest=configured_account_binding_digest,
        )
    ):
        return False

    event = db.scalar(
        select(RuntimeRemediationEvent)
        .options(undefer(RuntimeRemediationEvent.evidence_summary))
        .where(
            RuntimeRemediationEvent.mode == plan.mode,
            RuntimeRemediationEvent.candidate_type
            == KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE,
            RuntimeRemediationEvent.candidate_key == position.id,
            RuntimeRemediationEvent.subject_id == position.id,
            RuntimeRemediationEvent.outcome == KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME,
        )
        .with_for_update()
    )
    summary = (
        event.evidence_summary
        if event is not None and isinstance(event.evidence_summary, dict)
        else {}
    )
    event_observed_at = _aware_datetime(summary.get("broker_snapshot_observed_at"))
    event_occurred_at = (
        _utc(event.occurred_at)
        if event is not None and isinstance(event.occurred_at, datetime)
        else None
    )
    prior_digest = (
        summary.get("prior_position_digest")
        if is_lower_hex_sha256(summary.get("prior_position_digest"))
        else None
    )
    expected_fingerprint = (
        kis_krx_flat_evidence_fingerprint(
            runtime_mode=plan.mode,
            position_id=position.id,
            symbol=item.symbol,
            prior_position_digest=prior_digest,
            identity=expected_identity,
            snapshot_source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holding_absent=True,
            open_order_absent=True,
            absence_proof_digest=absence_digest,
            configured_account_binding_digest=configured_account_binding_digest,
        )
        if prior_digest is not None and absence_digest is not None
        else None
    )
    return bool(
        event is not None
        and set(summary) == KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS
        and summary.get("symbol") == item.symbol
        and summary.get("provider") == expected_identity.provider
        and summary.get("account_scope") == expected_identity.account_scope
        and summary.get("exchange") == expected_identity.exchange
        and summary.get("asset_class") == "equity"
        and summary.get("broker_snapshot_source") == proof.get("snapshot_source")
        and event_observed_at == observed_at
        and event_occurred_at is not None
        and event_occurred_at >= observed_at
        and event_occurred_at > proof_cutoff
        and account_snapshot_freshness_failure(
            observed_at=event_occurred_at,
            now=evaluated_at,
            settings=settings,
        )
        is None
        and summary.get("holdings_complete") is True
        and summary.get("open_orders_complete") is True
        and summary.get("holding_absent") is True
        and summary.get("open_order_absent") is True
        and summary.get("absence_proof_digest") == absence_digest
        and summary.get("configured_account_binding_digest")
        == configured_account_binding_digest
        and prior_digest is not None
        and summary.get("evidence_fingerprint") == expected_fingerprint
        and summary.get("safety_policy_version")
        == KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
        and summary.get("historical_account_attributed") is False
        and summary.get("current_flat_truth_bound") is True
        and all(
            type(summary.get(field)) is int and summary[field] == 0
            for field in KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS
        )
    )


def _expected_identity(
    *, settings: Settings | None, item: ExecutionPlanItem
) -> PositionIdentity | None:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = (
        item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    )
    if (
        settings is None
        or str(item.provider or "").strip().lower() != "kis"
        or str(risk_context.get("exchange") or item.venue or "").strip().upper()
        != "KRX"
        or validate_kis_account_route(
            settings=settings,
            provider=item.provider,
            risk_context=risk_context,
            quantity_policy=quantity_policy,
            require_binding=True,
        )
        is not None
    ):
        return None
    try:
        return build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return None


def _acquire_revocation_order_intent_serialization(
    *,
    db: Session,
    settings: Settings,
    identity: PositionIdentity,
) -> None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return
    scope = order_intent_scope_key_for_account(
        provider=identity.provider,
        runtime_mode=settings.runtime_mode,
        account_scope=identity.account_scope,
        market="KRW",
    )
    try:
        acquired = db.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtext(:scope))"),
            {"scope": scope},
        )
    except DBAPIError as exc:
        raise RevocationExposureSerializationBusy(
            "revocation exposure scope could not be reserved"
        ) from exc
    if acquired is not True:
        raise RevocationExposureSerializationBusy(
            "revocation exposure scope is busy"
        )


def _plan_history_cutoff(plan: ExecutionPlan) -> datetime | None:
    values = [
        _utc(value)
        for value in (plan.requested_at, plan.accepted_at, plan.created_at)
        if isinstance(value, datetime)
    ]
    return min(values) if values else None


def _quarantine_proof_cutoff(
    *, db: Session, plan: ExecutionPlan, item: ExecutionPlanItem
) -> datetime | None:
    quarantine_at = db.scalar(
        select(ExecutionPlanItemAudit.occurred_at)
        .where(
            ExecutionPlanItemAudit.execution_plan_item_id == item.id,
            ExecutionPlanItemAudit.event_type == "legacy_core_authority_quarantined",
        )
        .order_by(ExecutionPlanItemAudit.occurred_at.desc())
        .with_for_update()
        .limit(1)
    )
    plan_times = [
        _utc(value)
        for value in (plan.requested_at, plan.accepted_at, plan.created_at)
        if isinstance(value, datetime)
    ]
    return max(*plan_times, _utc(quarantine_at)) if quarantine_at is not None else None


def _order_predates_authority(order: BrokerOrder, authority_at: datetime) -> bool:
    timestamps = [order.created_at]
    if order.submitted_at is not None:
        timestamps.append(order.submitted_at)
    return all(
        isinstance(timestamp, datetime) and _utc(timestamp) < authority_at
        for timestamp in timestamps
    )


def _position_is_numerically_flat(position: ExecutionPosition) -> bool:
    values = (position.quantity, position.base_quantity)
    try:
        parsed = [Decimal(str(value or 0)) for value in values]
    except (InvalidOperation, TypeError, ValueError):
        return False
    return all(value.is_finite() and value == 0 for value in parsed)


def _position_is_canonical_bound_flat(position: ExecutionPosition) -> bool:
    return bool(
        position.base_quantity == KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY
        and _position_is_numerically_flat(position)
        and position.average_price is None
        and position.updated_from == "reconciliation"
    )


def _aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return (
        _utc(parsed)
        if parsed.tzinfo is not None and parsed.utcoffset() is not None
        else None
    )


def _unknown_or_matches(actual: object, expected: str) -> bool:
    normalized = str(actual or "").strip().lower()
    return not normalized or not expected or normalized == expected


def _same_instant(left: datetime | None, right: datetime | None) -> bool:
    return bool(
        isinstance(left, datetime)
        and isinstance(right, datetime)
        and _utc(left) == _utc(right)
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
