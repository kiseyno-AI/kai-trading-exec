from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import hmac
from inspect import Parameter, signature
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, selectinload

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerEventSource,
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
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlanItem,
    ExecutionPosition,
    RuntimeRemediationEvent,
    new_id,
)
from app.services.account_drawdown_recovery import (
    snapshot_is_fresh_for_recovery,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import (
    CAPITAL_RESERVING_ORDER_STATUSES,
    NONTERMINAL_ORDER_STATUSES,
    broker_order_observed_at_expr,
)
from app.services.execution_account_route import redact_account_references
from app.services.execution_failures import (
    record_actionable_execution_failure,
    resolve_execution_failure_for_order,
)
from app.services.fill_economics import (
    COST_COVERAGE_COMPLETE,
    COST_COVERAGE_UNKNOWN,
    FillEconomics,
    economics_payload,
    execution_fill_economics,
    normalize_fill_economics,
)
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    execution_fill_dedupe_key,
    execution_fill_idempotency_key,
)
from app.services.fifo_inventory import fifo_route_scope_from_order
from app.services.kis_krx_flat_proof_validation import (
    aware_iso_datetime as kis_krx_aware_iso_datetime,
    canonical_flat_row_matches_latest_proof,
    event_datetime as kis_krx_event_datetime,
    generic_sync_snapshot_payload_proves_symbol_absent,
    kis_krx_flat_proof_timestamp_is_fresh,
    load_kis_krx_flat_proof_lineage,
)
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentity,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
    payload_with_position_identity,
    position_identity_from_columns,
    position_identity_from_payload,
)
from app.services.rejected_order_retries import handle_reconciled_rejected_order
from app.services.runtime_transitions import (
    apply_broker_order_status,
    plan_item_status_for_broker_order_status,
    set_plan_item_status,
)


class LegacyManualReviewApprovalConflictError(ValueError):
    """An approval no longer matches the exact legacy-order evidence it reviewed."""


LEGACY_MANUAL_REVIEW_MINIMUM_AGE_SECONDS = 30 * 24 * 60 * 60
LEGACY_MANUAL_REVIEW_ACTIVE_JOB_STATUSES = frozenset(
    {"queued", "scheduled", "running", "retrying"}
)
LEGACY_MANUAL_REVIEW_OUTCOME_STATUSES = frozenset(
    {
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
    }
)
LEGACY_MANUAL_REVIEW_OUTCOME_PLAN_ITEM_STATUSES = frozenset(
    {"filled", "partially_filled", "broker_order_partially_filled"}
)


def reconcile_broker_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    orders: list[BrokerOrder],
) -> dict[str, int]:
    reconciled = 0
    fills_added = 0
    scope_rejected = 0
    scope_quarantined = 0
    for order in orders:
        result = reconcile_broker_order(
            settings=settings,
            db=db,
            adapter=adapter,
            order=order,
        )
        if result["status_changed"] is True:
            reconciled += 1
        fills_added += int(result["fills_added"])
        scope_classification = result.get("scope_classification")
        if scope_classification is not None:
            scope_rejected += 1
        if scope_classification == "invalid_execution_scope":
            scope_quarantined += 1
    return {
        "candidate_order_count": len(orders),
        "reconciled_order_count": reconciled,
        "fills_added": fills_added,
        "scope_rejected_order_count": scope_rejected,
        "scope_quarantined_order_count": scope_quarantined,
    }


def reconcile_broker_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    actor: str | None = None,
    operator_reason: str | None = None,
) -> dict[str, object]:
    order_status_before = order.status
    event_time = _observation_time()
    fills_added = 0

    scope_rejection = _order_reconciliation_scope_rejection(
        settings=settings,
        adapter=adapter,
        order=order,
        allow_deferred_authenticated_scope=True,
    )
    if scope_rejection is not None:
        return _reject_order_reconciliation_scope(
            db=db,
            order=order,
            classification=scope_rejection[0],
            reason=scope_rejection[1],
            observed_at=event_time,
            actor=actor,
            operator_reason=operator_reason,
        )

    if not order.broker_order_id:
        return {
            "status_code": 200,
            "broker_order_id": None,
            "status": "skipped_no_broker_order_id",
            "status_before": order.status,
            "status_changed": False,
            "fills_added": 0,
            "filled_delta": 0,
            "message": "broker order has no broker_order_id",
        }

    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="reconciliation",
            reason="refresh broker order state before reconciliation",
        )
        authenticated_scope_rejection = _order_reconciliation_scope_rejection(
            settings=settings,
            adapter=adapter,
            order=order,
            access_token=session_state.access_token or "",
        )
        if authenticated_scope_rejection is not None:
            return _reject_order_reconciliation_scope(
                db=db,
                order=order,
                classification=authenticated_scope_rejection[0],
                reason=authenticated_scope_rejection[1],
                observed_at=event_time,
                actor=actor,
                operator_reason=operator_reason,
            )
        payload = _call_reconcile_order(
            adapter=adapter,
            access_token=session_state.access_token or "",
            order=order,
        )
        if not isinstance(payload, dict):
            raise ValueError("reconcile_order adapter returned non-dictionary payload")
        _validate_reconcile_payload_identity(
            payload=payload,
            adapter=adapter,
            order=order,
        )
    except (BrokerAdapterError, Exception) as exc:  # broad except to keep reconcile cycle durable
        return _reconcile_adapter_error(
            exc=exc,
            order=order,
            adapter=adapter,
            requested_broker_order_id=order.broker_order_id,
            event_time=event_time,
            db=db,
            order_status_before=order_status_before,
            actor=actor,
            operator_reason=operator_reason,
        )

    event_time = _parse_timestamp(payload.get("observed_at"), fallback=event_time)
    normalized_status = _normalize_status(payload.get("status"))
    fills, fill_delta_anomalies = _parse_fills_from_payload(
        payload=payload,
        order=order,
        observed_at=event_time,
    )

    if normalized_status is None:
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="reconcile_result_unrecognized_status",
                event_source=BrokerEventSource.RECONCILIATION.value,
                normalized_status=order.status,
                broker_status=_optional_str(payload.get("broker_status")),
                message="reconcile payload has unrecognized status",
                request_payload=_dict_or_none(payload.get("request_payload")),
                response_payload=_safe_payload(payload),
                payload={
                    **_build_reconcile_event_payload(payload=payload, adapter=adapter),
                    "raw_status_details": _raw_status_details(payload),
                    **(
                        {"fill_delta_anomalies": _json_safe_value(fill_delta_anomalies)}
                        if fill_delta_anomalies
                        else {}
                    ),
                },
                observed_at=event_time,
            )
        )
        order.last_error_classification = "unknown_status"
        order.last_error_code = _optional_str(payload.get("status"))
        order.last_error_message = "unrecognized broker status"
        order.last_error_retryable = False
        order.last_broker_sync_at = event_time
        _apply_order_metadata(
            order=order,
            payload=payload,
        )
        repaired_absent = _should_terminalize_absent_prior_session_order(
            settings=settings,
            adapter=adapter,
            order=order,
            payload=payload,
            observed_at=event_time,
        )
        if repaired_absent:
            status_after = NormalizedBrokerOrderStatus.STALE_TERMINAL_UNKNOWN.value
            reason = "prior_session_order_absent_after_bounded_threshold"
            apply_broker_order_status(order=order, status=status_after)
            if order.plan_item is not None:
                plan_status = _order_status_to_plan_item_status(status_after)
                if plan_status is not None:
                    set_plan_item_status(
                        db=db,
                        plan_item=order.plan_item,
                        status=plan_status,
                        source="reconciliation",
                        reason=reason,
                        payload={
                            "broker_order_pk": order.id,
                            "broker_order_id": order.broker_order_id,
                            "status_before": order_status_before,
                            "status": status_after,
                            "broker_result": _json_safe_value(payload),
                        },
                        occurred_at=event_time,
                    )
            _record_stale_order_repair_event(
                db=db,
                order=order,
                status_before=order_status_before,
                status_after=status_after,
                classification="stale_terminal_unknown",
                reason=reason,
                observed_broker_code=(
                    _optional_str(payload.get("broker_status"))
                    or _optional_str(payload.get("status"))
                    or "unknown"
                ),
                observed_at=event_time,
                result={
                    "status_code": 200,
                    "status": payload.get("status"),
                    "broker_status": payload.get("broker_status"),
                    "message": payload.get("message"),
                    "response_metadata": payload.get("response_metadata"),
                    "payload": payload.get("payload"),
                },
                actor=actor,
                operator_reason=operator_reason,
            )
        db.commit()
        db.refresh(order)
        return {
            "status_code": 200,
            "broker_order_id": order.broker_order_id,
            "status": order.status if repaired_absent else "unknown",
            "status_before": order_status_before,
            "status_changed": repaired_absent,
            "fills_added": 0,
            "filled_delta": 0,
            "message": (
                "prior-session order absent from successful broker reconciliation; "
                "moved to terminal attention state"
                if repaired_absent
                else "unrecognized broker status"
            ),
        }

    apply_broker_order_status(order=order, status=normalized_status)
    fills_added = _upsert_order_fills(
        db=db,
        order=order,
        fills=fills,
    )
    _apply_order_metadata(
        order=order,
        payload=payload,
    )
    order.last_broker_sync_at = event_time

    reconcile_event_payload = _build_reconcile_event_payload(payload=payload, adapter=adapter)
    if fill_delta_anomalies:
        reconcile_event_payload["fill_delta_anomalies"] = _json_safe_value(fill_delta_anomalies)
    if actor is not None or operator_reason is not None:
        reconcile_event_payload["actor"] = actor
        reconcile_event_payload["operator_reason"] = operator_reason

    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="reconcile_result",
            event_source=BrokerEventSource.RECONCILIATION.value,
            normalized_status=normalized_status,
            broker_status=_optional_str(payload.get("broker_status")),
            message=_optional_str(payload.get("message")),
            request_payload=_dict_or_none(payload.get("request_payload")),
            response_payload=_safe_payload(payload.get("payload")),
            payload=reconcile_event_payload,
            observed_at=event_time,
        )
    )

    retry_failure = None
    if normalized_status == NormalizedBrokerOrderStatus.REJECTED.value:
        redacted_retry_payload = redact_account_references(payload)
        if not isinstance(redacted_retry_payload, dict):
            redacted_retry_payload = {}
        retry_failure = handle_reconciled_rejected_order(
            settings=settings,
            db=db,
            order=order,
            payload=redacted_retry_payload,
            observed_at=event_time,
        )
    else:
        order.last_error_classification = None
        order.last_error_code = None
        order.last_error_message = None
        order.last_error_retryable = None

    if order.plan_item is not None:
        retry_already_submitted = (
            retry_failure is not None
            and retry_failure.retry_decision_status in {"submitted", "completed"}
        )
        if not retry_already_submitted:
            plan_status = _order_status_to_plan_item_status(order.status)
            if plan_status is not None:
                set_plan_item_status(
                    db=db,
                    plan_item=order.plan_item,
                    status=plan_status,
                    source="reconciliation",
                    reason="broker truth reconciliation updated plan item status",
                    payload={
                        "broker_order_pk": order.id,
                        "broker_order_id": order.broker_order_id,
                        "idempotency_key": order.idempotency_key,
                        "status_before": order_status_before,
                        "status": order.status,
                    },
                    occurred_at=event_time,
                )

    status_changed = order.status != order_status_before
    db.commit()
    db.refresh(order)
    return {
        "status_code": 200,
        "broker_order_id": order.broker_order_id,
        "status": order.status,
        "status_before": order_status_before,
        "status_changed": status_changed,
        "fills_added": fills_added,
        "filled_delta": fills_added,
    }


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


def _order_reconciliation_scope_rejection(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    access_token: str | None = None,
    allow_deferred_authenticated_scope: bool = False,
) -> tuple[str, str] | None:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    raw_scope = metadata.get("execution_scope")
    if not isinstance(raw_scope, dict):
        return "invalid_execution_scope", "canonical_execution_scope_missing"
    if set(raw_scope) != _CANONICAL_EXECUTION_SCOPE_KEYS:
        return "invalid_execution_scope", "canonical_execution_scope_malformed"
    if raw_scope.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION:
        return "invalid_execution_scope", "canonical_execution_scope_version_invalid"
    route_scope = fifo_route_scope_from_order(order)
    if route_scope is None:
        return "invalid_execution_scope", "canonical_execution_scope_conflicts_with_order"

    adapter_provider = str(adapter.provider or "").strip().lower()
    if (
        order.mode != settings.runtime_mode
        or route_scope.provider != adapter_provider
        or str(order.broker_name or "").strip().lower() != adapter_provider
    ):
        return "foreign_execution_scope", "broker_or_runtime_scope_mismatch"

    broker_account_id: str | None = None
    if adapter_provider == "alpaca":
        if access_token is None:
            if allow_deferred_authenticated_scope:
                return None
            return (
                "authenticated_execution_scope_unavailable",
                "dry_run_cannot_verify_authenticated_account_identity",
            )
        account_id_loader = getattr(adapter, "get_broker_account_id", None)
        if not callable(account_id_loader):
            return (
                "authenticated_execution_scope_unavailable",
                "authenticated_account_identity_unavailable",
            )
        try:
            loaded_account_id = account_id_loader(access_token)
        except Exception:
            return (
                "authenticated_execution_scope_unavailable",
                "authenticated_account_identity_unavailable",
            )
        broker_account_id = (
            loaded_account_id.strip()
            if isinstance(loaded_account_id, str) and loaded_account_id.strip()
            else None
        )
        if broker_account_id is None:
            return (
                "authenticated_execution_scope_unavailable",
                "authenticated_account_identity_unavailable",
            )
    try:
        current_identity = build_position_identity(
            settings=settings,
            provider=adapter_provider,
            exchange=route_scope.exchange,
            broker_account_id=broker_account_id,
        )
    except PositionIdentityConflictError:
        return (
            "authenticated_execution_scope_unavailable",
            "authenticated_account_identity_unavailable",
        )
    if (
        route_scope.provider != current_identity.provider
        or route_scope.account_scope != current_identity.account_scope
        or route_scope.exchange != current_identity.exchange
    ):
        return "foreign_execution_scope", "authenticated_account_scope_mismatch"
    return None


def _reject_order_reconciliation_scope(
    *,
    db: Session,
    order: BrokerOrder,
    classification: str,
    reason: str,
    observed_at: datetime,
    actor: str | None,
    operator_reason: str | None,
) -> dict[str, object]:
    status_before = order.status
    if classification == "invalid_execution_scope":
        apply_broker_order_status(
            order=order,
            status=NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
        )
        order.last_error_classification = classification
        order.last_error_code = reason
        order.last_error_message = (
            "broker reconciliation blocked because canonical execution scope "
            "is unavailable or invalid"
        )
        order.last_error_retryable = False
        if order.plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=order.plan_item,
                status="broker_order_failed",
                source="reconciliation",
                reason=reason,
                payload={
                    "broker_order_pk": order.id,
                    "status_before": status_before,
                    "status": order.status,
                    "classification": classification,
                },
                occurred_at=observed_at,
            )
    prior_event_id = db.scalar(
        select(BrokerOrderEvent.id)
        .where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_scope_rejected",
            BrokerOrderEvent.error_classification == classification,
            BrokerOrderEvent.message == reason,
        )
        .limit(1)
    )
    if prior_event_id is None:
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="reconcile_scope_rejected",
                event_source=BrokerEventSource.RECONCILIATION.value,
                normalized_status=order.status,
                error_classification=classification,
                retryable=False,
                message=reason,
                payload={
                    "classification": classification,
                    "reason": reason,
                    "actor": actor,
                    "operator_reason": operator_reason,
                },
                observed_at=observed_at,
            )
        )
    db.commit()
    db.refresh(order)
    return {
        "status_code": 409,
        "broker_order_id": order.broker_order_id,
        "status": order.status,
        "status_before": status_before,
        "status_changed": order.status != status_before,
        "fills_added": 0,
        "filled_delta": 0,
        "message": reason,
        "scope_classification": classification,
    }


def _call_reconcile_order(
    *,
    adapter: BrokerAdapter,
    access_token: str,
    order: BrokerOrder,
) -> dict[str, Any] | None:
    optional_kwargs: dict[str, object] = {
        "exchange": _order_reconciliation_exchange(order),
        "submitted_at": order.submitted_at,
        "expected_symbol": order.symbol,
    }
    try:
        parameters = signature(adapter.reconcile_order).parameters
    except (TypeError, ValueError):
        # The protocol's long-standing exchange argument is the only safe
        # fallback when a callable does not expose an inspectable signature.
        kwargs = {"exchange": optional_kwargs["exchange"]}
    else:
        accepts_extra_keywords = any(
            parameter.kind is Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        kwargs = {
            name: value
            for name, value in optional_kwargs.items()
            if accepts_extra_keywords or name in parameters
        }
    return adapter.reconcile_order(
        access_token,
        order.broker_order_id or "",
        **kwargs,
    )


def _validate_reconcile_payload_identity(
    *,
    payload: dict[str, Any],
    adapter: BrokerAdapter,
    order: BrokerOrder,
) -> None:
    mismatches: list[str] = []
    route_scope = fifo_route_scope_from_order(order)
    identity_provider = str(adapter.provider or "").strip().casefold()
    identity_asset_class = (
        route_scope.asset_class
        if route_scope is not None
        else str(order.asset_class or "").strip().lower()
    )
    identity_exchange = (
        route_scope.exchange
        if route_scope is not None
        else str(order.venue or "").strip().upper()
    )
    expected_identities = {
        "broker_order_id": str(order.broker_order_id or "").strip(),
        "provider": identity_provider,
        "symbol": _canonical_reconcile_symbol(
            order.symbol,
            provider=identity_provider,
            asset_class=identity_asset_class,
            exchange=identity_exchange,
        ),
    }
    for key, expected in expected_identities.items():
        if key not in payload:
            continue
        observed = payload.get(key)
        if not isinstance(observed, str) or not observed.strip():
            mismatches.append(key)
            continue
        normalized_observed = observed.strip()
        if key == "provider":
            normalized_observed = normalized_observed.casefold()
        elif key == "symbol":
            normalized_observed = _canonical_reconcile_symbol(
                normalized_observed,
                provider=identity_provider,
                asset_class=identity_asset_class,
                exchange=identity_exchange,
            )
        if normalized_observed != expected:
            mismatches.append(key)
    if not mismatches:
        return
    raise BrokerAdapterError(
        "broker reconciliation response identity did not match the persisted order",
        status_code=502,
        classification=BrokerErrorClassification.UNKNOWN,
        retryable=True,
        code="reconcile_identity_mismatch",
        response_payload={
            "mismatched_fields": mismatches,
            "payload": payload,
        },
        observed_at=_observation_time(),
    )


def _canonical_reconcile_symbol(
    value: object,
    *,
    provider: str,
    asset_class: str,
    exchange: str,
) -> str:
    normalized = str(value or "").strip().upper()
    if provider == "alpaca" and asset_class == "crypto" and exchange == "ALPACA":
        normalized = normalized.replace("-", "/")
        if "/" not in normalized and normalized.endswith("USD") and len(normalized) > 3:
            normalized = f"{normalized[:-3]}/USD"
    return normalized


def reconcile_pending_broker_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str] | None = None,
) -> dict[str, int]:
    orders = _load_pending_orders_for_reconciliation(
        db=db,
        settings=settings,
        broker_name=adapter.provider,
        symbols=_normalize_symbols(symbols),
    )
    return reconcile_broker_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        orders=orders,
    )


def reconcile_stale_nonterminal_broker_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    older_than_seconds: int | None = None,
    symbols: set[str] | None = None,
    dry_run: bool = False,
    actor: str | None = None,
    operator_reason: str | None = None,
    manual_review_approvals: dict[str, str] | None = None,
    include_legacy_manual_review: bool = False,
) -> dict[str, object]:
    threshold_seconds = (
        older_than_seconds
        if older_than_seconds is not None
        else settings.stale_broker_order_reconciliation_threshold_seconds
    )
    observed_at = _observation_time()
    cutoff = observed_at - timedelta(seconds=threshold_seconds)
    orders = _load_stale_nonterminal_orders_for_reconciliation(
        db=db,
        settings=settings,
        broker_name=adapter.provider,
        stale_before=cutoff,
        symbols=_normalize_symbols(symbols),
    )

    counts = {
        "live": 0,
        "closed": 0,
        "cancelled": 0,
        "unknown": 0,
        "failed_to_reconcile": 0,
        "manual_review_required": 0,
        "reconcile_window_expired": 0,
        "stale_terminal_unknown": 0,
    }
    repaired_count = 0
    items: list[dict[str, object]] = []
    position_sync_symbols: dict[str, set[str]] = {}
    for order in orders:
        classification_observed_at = _observation_time()
        status_before = order.status
        error_code_before = order.last_error_code
        error_classification_before = order.last_error_classification
        reason: str | None = None
        result: dict[str, object] | None = None
        scope_classification: str | None = None
        scope_quarantined = False
        scope_would_quarantine = False
        if not order.broker_order_id:
            classification = "failed_to_reconcile"
            reason = "missing_broker_order_id"
            if not dry_run:
                order.last_error_classification = "missing_broker_order_id"
                order.last_error_message = "stale nonterminal order has no broker_order_id"
                order.last_error_retryable = False
                order.last_broker_sync_at = classification_observed_at
        else:
            if dry_run:
                scope_rejection = _order_reconciliation_scope_rejection(
                    settings=settings,
                    adapter=adapter,
                    order=order,
                )
                if scope_rejection is None:
                    result = _stale_order_dry_run_result(order=order)
                else:
                    scope_classification, reason = scope_rejection
                    result = {
                        **_stale_order_dry_run_result(order=order),
                        "status_code": 409,
                        "message": reason,
                        "scope_classification": scope_classification,
                    }
            else:
                result = reconcile_broker_order(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    order=order,
                    actor=actor,
                    operator_reason=operator_reason,
                )
                raw_scope_classification = result.get("scope_classification")
                scope_classification = (
                    str(raw_scope_classification)
                    if raw_scope_classification is not None
                    else None
                )
            if scope_classification == "invalid_execution_scope":
                classification = "manual_review_required"
                reason = _optional_str(result.get("message")) or reason
                if dry_run:
                    scope_would_quarantine = True
                else:
                    scope_quarantined = True
            elif scope_classification is not None:
                classification = "failed_to_reconcile"
                reason = _optional_str(result.get("message")) or reason
            else:
                classification, reason = _stale_order_reconciliation_classification(
                    status=str(result.get("status") or order.status),
                    status_code=int(result.get("status_code") or 200),
                    message=result.get("message"),
                )
            if int(result.get("fills_added") or 0) > 0 or str(result.get("status") or "") == "filled":
                exchange = _order_reconciliation_exchange(order)
                position_sync_symbols.setdefault(exchange, set()).add(order.symbol)

        if scope_would_quarantine:
            repair = {
                "status_after": NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
                "classification": "manual_review_required",
                "reason": reason or "canonical_execution_scope_invalid",
                "observed_broker_code": "invalid_execution_scope",
            }
        elif scope_quarantined:
            repair = None
        else:
            repair = _stale_order_repair_decision(
                order=order,
                status_before=status_before,
                classification=classification,
                reason=reason,
                result=result,
                error_code_before=error_code_before,
                error_classification_before=error_classification_before,
            )
        if scope_quarantined:
            repaired_count += 1
        if repair is not None:
            repaired_count += 1
            classification, reason = repair["classification"], repair["reason"]
            if not dry_run:
                apply_broker_order_status(order=order, status=repair["status_after"])
                order.last_error_retryable = False
                if repair["status_after"] == NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value:
                    order.last_error_classification = "missing_broker_order_id"
                    order.last_error_message = "stale nonterminal order has no broker_order_id"
                if order.plan_item is not None:
                    sibling_outcome_status = (
                        _authoritative_plan_sibling_outcome_status(
                            db=db,
                            order=order,
                        )
                    )
                    plan_status = sibling_outcome_status or (
                        _order_status_to_plan_item_status(order.status)
                    )
                    if plan_status is not None:
                        set_plan_item_status(
                            db=db,
                            plan_item=order.plan_item,
                            status=plan_status,
                            source="reconciliation",
                            reason=(
                                "authoritative plan sibling outcome preserved"
                                if sibling_outcome_status is not None
                                else repair["reason"]
                            ),
                            payload={
                                "broker_order_pk": order.id,
                                "broker_order_id": order.broker_order_id,
                                "idempotency_key": order.idempotency_key,
                                "status_before": status_before,
                                "status": order.status,
                                "classification": repair["classification"],
                                "authoritative_sibling_outcome": (
                                    sibling_outcome_status
                                ),
                            },
                            occurred_at=classification_observed_at,
                        )
                _record_stale_order_repair_event(
                    db=db,
                    order=order,
                    status_before=status_before,
                    status_after=repair["status_after"],
                    classification=repair["classification"],
                    reason=repair["reason"],
                    observed_broker_code=repair["observed_broker_code"],
                    observed_at=classification_observed_at,
                    result=result,
                    actor=actor,
                    operator_reason=operator_reason,
                )
                if repair["status_after"] == NormalizedBrokerOrderStatus.RECONCILE_WINDOW_EXPIRED.value:
                    resolve_execution_failure_for_order(
                        db=db,
                        order=order,
                        observed_at=classification_observed_at,
                        source="stale_order_reconciliation",
                        reason=repair["reason"] or "broker_lookup_window_expired",
                    )
                elif order.last_error_classification == "broker_rejected":
                    record_actionable_execution_failure(
                        settings=settings,
                        db=db,
                        order=order,
                        plan_item=order.plan_item,
                        broker_error_classification=order.last_error_classification,
                        broker_error_code=order.last_error_code,
                        broker_error_message=order.last_error_message
                        or repair["reason"]
                        or "broker rejected stale order",
                        retryable=False,
                        observed_at=classification_observed_at,
                        request_source="stale_order_reconciliation",
                    )
        elif not dry_run and _is_nonactionable_stale_result(order.status):
            if order.status != status_before:
                repaired_count += 1
            _record_stale_order_repair_event(
                db=db,
                order=order,
                status_before=status_before,
                status_after=order.status,
                classification=classification,
                reason=reason or "terminal_broker_status",
                observed_broker_code=order.last_error_code,
                observed_at=classification_observed_at,
                result=result,
                actor=actor,
                operator_reason=operator_reason,
            )

        if not dry_run:
            _record_stale_order_classification_event(
                db=db,
                order=order,
                classification=classification,
                reason=reason,
                observed_at=_observation_time(),
                result=result,
                actor=actor,
                operator_reason=operator_reason,
            )
        counts[classification] += 1
        items.append(
            {
                "broker_order_pk": order.id,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "symbol": order.symbol,
                "side": order.side,
                "status": status_before if dry_run else order.status,
                "status_before": status_before,
                "status_after": repair["status_after"] if repair is not None else order.status,
                "classification": classification.replace("_", "-"),
                "reason": reason,
                "repair_action": (
                    "would_quarantine"
                    if scope_would_quarantine
                    else "quarantined"
                    if scope_quarantined
                    else "would_repair"
                    if dry_run and repair is not None
                    else "repaired"
                    if repair is not None
                    else "none"
                ),
                "observed_broker_code": (
                    repair["observed_broker_code"]
                    if repair is not None
                    else order.last_error_code
                ),
                "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
                "result": result,
            }
        )

    if include_legacy_manual_review:
        legacy_manual_review_resolution = _reconcile_legacy_manual_review_orders(
            settings=settings,
            db=db,
            adapter=adapter,
            stale_before=observed_at
            - timedelta(
                seconds=max(
                    threshold_seconds,
                    LEGACY_MANUAL_REVIEW_MINIMUM_AGE_SECONDS,
                )
            ),
            symbols=_normalize_symbols(symbols),
            dry_run=dry_run,
            actor=actor,
            operator_reason=operator_reason,
            approvals=manual_review_approvals or {},
        )
    else:
        if manual_review_approvals:
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review approvals require explicit discovery opt-in"
            )
        legacy_manual_review_resolution = (
            _empty_legacy_manual_review_resolution()
        )

    expired_failure_cleanup = _resolve_open_failures_for_expired_reconciliation_orders(
        settings=settings,
        db=db,
        broker_name=adapter.provider,
        symbols=_normalize_symbols(symbols),
        observed_at=observed_at,
        dry_run=dry_run,
    )

    position_sync_result = None
    if position_sync_symbols and not dry_run:
        results = [
            sync_execution_positions_from_broker_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=scoped_symbols,
                exchange=exchange,
            )
            for exchange, scoped_symbols in sorted(position_sync_symbols.items())
        ]
        position_sync_result = {
            key: sum(int(result.get(key, 0)) for result in results)
            for key in ("tracked_symbol_count", "created", "updated", "zeroed")
        }
    elif not dry_run:
        db.commit()
    return {
        "dry_run": dry_run,
        "candidate_order_count": len(orders),
        "threshold_seconds": threshold_seconds,
        "stale_before": cutoff.isoformat(),
        "repaired_order_count": 0 if dry_run else repaired_count,
        "would_repair_order_count": repaired_count if dry_run else 0,
        "counts": {
            "live": counts["live"],
            "closed": counts["closed"],
            "cancelled": counts["cancelled"],
            "unknown": counts["unknown"],
            "failed-to-reconcile": counts["failed_to_reconcile"],
            "manual-review-required": counts.get("manual_review_required", 0),
            "reconcile-window-expired": counts.get("reconcile_window_expired", 0),
            "stale-terminal-unknown": counts.get("stale_terminal_unknown", 0),
        },
        "position_sync": position_sync_result
        or {
            "tracked_symbol_count": 0,
            "created": 0,
            "updated": 0,
            "zeroed": 0,
        },
        "expired_failure_cleanup": expired_failure_cleanup,
        "legacy_manual_review_resolution": legacy_manual_review_resolution,
        "items": items,
    }


def _authoritative_plan_sibling_outcome_status(
    *,
    db: Session,
    order: BrokerOrder,
) -> str | None:
    if order.execution_plan_item_id is None:
        return None
    order_scope = fifo_route_scope_from_order(order)
    if order_scope is None:
        return None
    statuses = set(
        sibling.status
        for sibling in db.scalars(
            select(BrokerOrder).where(
                BrokerOrder.execution_plan_item_id
                == order.execution_plan_item_id,
                BrokerOrder.id != order.id,
                BrokerOrder.mode == order.mode,
                BrokerOrder.broker_name == order.broker_name,
                BrokerOrder.symbol == order.symbol,
                BrokerOrder.side == order.side,
                BrokerOrder.status.in_(
                    tuple(LEGACY_MANUAL_REVIEW_OUTCOME_STATUSES)
                ),
            )
        ).all()
        if fifo_route_scope_from_order(sibling) == order_scope
    )
    if NormalizedBrokerOrderStatus.FILLED.value in statuses:
        return "filled"
    if NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value in statuses:
        return "partially_filled"
    return None


def _empty_legacy_manual_review_resolution() -> dict[str, object]:
    return {
        "minimum_age_seconds": LEGACY_MANUAL_REVIEW_MINIMUM_AGE_SECONDS,
        "candidate_order_count": 0,
        "eligible_order_count": 0,
        "would_resolve_order_count": 0,
        "resolved_order_count": 0,
        "items": [],
    }


def _reconcile_legacy_manual_review_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    stale_before: datetime,
    symbols: set[str] | None,
    dry_run: bool,
    actor: str | None,
    operator_reason: str | None,
    approvals: dict[str, str],
) -> dict[str, object]:
    orders = _load_legacy_manual_review_orders(
        settings=settings,
        db=db,
        broker_name=adapter.provider,
        stale_before=stale_before,
        symbols=symbols,
        for_update=False,
    )
    if not orders:
        if approvals:
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review approval target is no longer a candidate"
            )
        return _empty_legacy_manual_review_resolution()

    snapshot: BrokerAccountSnapshot | None = None
    snapshot_failure: str | None = None
    access_token = ""
    plan_items_by_id: dict[str, ExecutionPlanItem] = {}
    plan_siblings_by_item_id: dict[str, list[BrokerOrder]] = {}
    sibling_fill_counts: dict[str, int] = {}
    sibling_fills_by_order_id: dict[str, list[ExecutionFill]] = {}
    sibling_events_by_order_id: dict[str, list[BrokerOrderEvent]] = {}
    active_jobs_by_item_id: dict[str, list[ExecutionJob]] = {}
    if str(adapter.provider or "").strip().lower() != "kis":
        snapshot_failure = "unsupported_broker_provider"
    else:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="stale_order_reconciliation",
            reason="verify legacy manual-review order absence",
        )
        access_token = session_state.access_token or ""
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=None,
            exchange="KRX",
        )
        snapshot_failure = _legacy_manual_review_snapshot_failure(
            settings=settings,
            snapshot=snapshot,
            evaluated_at=_observation_time(),
        )

    requested_approval_ids = set(approvals)
    if not dry_run and requested_approval_ids:
        initial_order_ids = {order.id for order in orders}
        if not requested_approval_ids.issubset(initial_order_ids):
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review approval target is no longer a candidate"
            )
        _reserve_sqlite_legacy_manual_review_approval_rows(
            settings=settings,
            db=db,
            broker_name=adapter.provider,
            stale_before=stale_before,
            symbols=symbols,
            approved_order_ids=requested_approval_ids,
        )

    if not dry_run:
        orders = _load_legacy_manual_review_orders(
            settings=settings,
            db=db,
            broker_name=adapter.provider,
            stale_before=stale_before,
            symbols=symbols,
            for_update=True,
        )
        if requested_approval_ids and not requested_approval_ids.issubset(
            {order.id for order in orders}
        ):
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review approval target drifted during writer reservation"
            )

    plan_item_ids = {
        order.execution_plan_item_id
        for order in orders
        if order.execution_plan_item_id is not None
    }
    if plan_item_ids:
        plan_item_statement = (
            select(ExecutionPlanItem)
            .where(ExecutionPlanItem.id.in_(sorted(plan_item_ids)))
            .order_by(ExecutionPlanItem.id.asc())
        )
        if not dry_run:
            plan_item_statement = plan_item_statement.with_for_update().execution_options(
                populate_existing=True
            )
        plan_items = db.scalars(plan_item_statement).all()
        plan_items_by_id = {plan_item.id: plan_item for plan_item in plan_items}
        (
            plan_siblings_by_item_id,
            sibling_fill_counts,
            sibling_fills_by_order_id,
            sibling_events_by_order_id,
        ) = _load_legacy_manual_review_plan_siblings(
            db=db,
            plan_item_ids=plan_item_ids,
            for_update=not dry_run,
        )
        active_jobs_by_item_id = _load_legacy_manual_review_active_jobs(
            db=db,
            plan_item_ids=plan_item_ids,
            for_update=not dry_run,
        )

    if not dry_run and snapshot is not None:
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=None,
            exchange="KRX",
        )
        snapshot_failure = _legacy_manual_review_snapshot_failure(
            settings=settings,
            snapshot=snapshot,
            evaluated_at=_observation_time(),
        )

    if not dry_run and requested_approval_ids:
        _acquire_legacy_manual_review_position_truth_fence(db=db)

    local_positions_statement = select(ExecutionPosition).where(
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.symbol.in_([order.symbol for order in orders]),
    )
    if not dry_run:
        local_positions_statement = local_positions_statement.with_for_update().execution_options(
            populate_existing=True
        )
    local_position_symbols = {
        position.symbol
        for position in db.scalars(local_positions_statement).all()
        if position.quantity > 0
        or (position.base_quantity is not None and position.base_quantity > 0)
    }
    items: list[dict[str, object]] = []
    for order in orders:
        plan_item_id = order.execution_plan_item_id
        plan_item = (
            plan_items_by_id.get(plan_item_id)
            if plan_item_id is not None
            else None
        )
        plan_item_status = plan_item.status if plan_item is not None else None
        plan_siblings = (
            plan_siblings_by_item_id.get(plan_item_id, [])
            if plan_item_id is not None
            else []
        )
        active_jobs = (
            active_jobs_by_item_id.get(plan_item_id, [])
            if plan_item_id is not None
            else []
        )
        order_fill_count = sibling_fill_counts.get(order.id, len(order.fills))
        outcome_bearing_siblings = [
            sibling
            for sibling in plan_siblings
            if sibling.id != order.id
            and (
                sibling.status in LEGACY_MANUAL_REVIEW_OUTCOME_STATUSES
                or sibling_fill_counts.get(sibling.id, 0) > 0
            )
        ]
        verified_retry_successor = (
            _verified_legacy_manual_review_retry_successor(
                settings=settings,
                order=order,
                plan_item=plan_item,
                outcome_bearing_siblings=outcome_bearing_siblings,
                fills_by_order_id=sibling_fills_by_order_id,
                events_by_order_id=sibling_events_by_order_id,
            )
            if plan_item is not None
            else None
        )
        plan_sibling_state = _legacy_manual_review_plan_sibling_state(
            siblings=plan_siblings,
            fills_by_order_id=sibling_fills_by_order_id,
        )
        broker_position_match = bool(
            snapshot is not None
            and any(
                holding.symbol == order.symbol and holding.quantity > 0
                for holding in snapshot.holdings
            )
        )
        broker_open_order_match = bool(
            snapshot is not None
            and any(
                open_order.symbol == order.symbol
                and (open_order.remaining_quantity or 0) > 0
                for open_order in snapshot.open_orders
            )
        )
        route_failure = _legacy_manual_review_order_route_failure(
            settings=settings,
            order=order,
            snapshot=snapshot,
        )
        (
            account_product_code_proof,
            _account_product_code_failure,
        ) = _legacy_manual_review_account_product_code_proof(
            settings=settings,
            order=order,
        )
        reason = (
            snapshot_failure
            or route_failure
            or ("broker_position_present" if broker_position_match else None)
            or ("broker_open_order_present" if broker_open_order_match else None)
            or (
                "local_position_present"
                if order.symbol in local_position_symbols
                else None
            )
            or ("execution_fills_present" if order_fill_count else None)
            or (
                "outcome_bearing_plan_sibling_present"
                if outcome_bearing_siblings
                and verified_retry_successor is None
                else None
            )
            or (
                "outcome_bearing_plan_item_status"
                if plan_item_status
                in LEGACY_MANUAL_REVIEW_OUTCOME_PLAN_ITEM_STATUSES
                and verified_retry_successor is None
                else None
            )
            or (
                "linked_execution_failure_present"
                if order.execution_failure is not None
                else None
            )
            or (
                "active_execution_job_present"
                if active_jobs
                else None
            )
        )
        eligible = reason is None
        fingerprint = (
            _legacy_manual_review_evidence_fingerprint(
                order=order,
                snapshot=snapshot,
                broker_position_match=broker_position_match,
                broker_open_order_match=broker_open_order_match,
                local_position_match=order.symbol in local_position_symbols,
                order_fill_count=order_fill_count,
                active_execution_jobs=active_jobs,
                plan_sibling_state=plan_sibling_state,
                plan_item_status=plan_item_status,
                verified_retry_successor=verified_retry_successor,
                account_product_code_proof=account_product_code_proof,
                snapshot_route_product_code=(
                    settings.kis_account_product_code
                ),
            )
            if eligible and snapshot is not None
            else None
        )
        items.append(
            {
                "broker_order_pk": order.id,
                "execution_plan_item_id": order.execution_plan_item_id,
                "symbol": order.symbol,
                "side": order.side,
                "status_before": order.status,
                "eligible": eligible,
                "reason": reason,
                "evidence_fingerprint": fingerprint,
                "broker_position_match": broker_position_match,
                "broker_open_order_match": broker_open_order_match,
                "local_position_match": order.symbol in local_position_symbols,
                "account_product_code_proof": account_product_code_proof,
                "snapshot_route_product_code": (
                    settings.kis_account_product_code
                ),
                "linked_execution_failure_count": (
                    1 if order.execution_failure is not None else 0
                ),
                "active_execution_job_count": len(active_jobs),
                "outcome_bearing_sibling_count": len(
                    outcome_bearing_siblings
                ),
                "plan_item_status": plan_item_status,
                "plan_sibling_state": plan_sibling_state,
                "resolution_context": (
                    "verified_filled_retry_successor"
                    if verified_retry_successor is not None
                    else "current_broker_absence"
                ),
                "verified_retry_successor": verified_retry_successor,
                "repair_action": "approval_required" if eligible else "blocked",
            }
        )

    eligible_count = sum(item["eligible"] is True for item in items)
    resolved_count = 0
    if not dry_run and approvals:
        normalized_actor = str(actor or "").strip()
        normalized_reason = str(operator_reason or "").strip()
        if not normalized_actor or not normalized_reason:
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review apply requires actor and reason"
            )
        item_by_order_id = {
            str(item["broker_order_pk"]): item
            for item in items
        }
        order_by_id = {order.id: order for order in orders}
        unknown_approval_ids = sorted(set(approvals) - set(item_by_order_id))
        if unknown_approval_ids:
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review approval target is no longer a candidate"
            )
        approved_order_ids: set[str] = set()
        for order_id, approved_fingerprint in approvals.items():
            item = item_by_order_id[order_id]
            current_fingerprint = item.get("evidence_fingerprint")
            if (
                item.get("eligible") is not True
                or not isinstance(current_fingerprint, str)
                or not hmac.compare_digest(
                    current_fingerprint,
                    str(approved_fingerprint),
                )
            ):
                raise LegacyManualReviewApprovalConflictError(
                    "legacy manual-review approval fingerprint is stale or ineligible"
                )
            approved_order_ids.add(order_id)

        approved_plan_item_ids = {
            order_by_id[order_id].execution_plan_item_id
            for order_id in approved_order_ids
            if order_by_id[order_id].execution_plan_item_id is not None
        }
        if approved_plan_item_ids:
            unresolved_siblings = [
                sibling
                for plan_item_id in approved_plan_item_ids
                for sibling in plan_siblings_by_item_id.get(plan_item_id, [])
                if sibling.status in CAPITAL_RESERVING_ORDER_STATUSES
            ]
            if any(
                sibling.id not in approved_order_ids
                for sibling in unresolved_siblings
            ):
                raise LegacyManualReviewApprovalConflictError(
                    "legacy manual-review sibling group requires atomic approval"
                )

        assert snapshot is not None
        default_repair_reason = (
            "legacy_manual_review_absent_from_complete_broker_truth"
        )
        for order_id in sorted(approved_order_ids):
            order = order_by_id[order_id]
            item = item_by_order_id[order_id]
            fingerprint = str(item["evidence_fingerprint"])
            resolution_context = str(item["resolution_context"])
            verified_retry_successor = item["verified_retry_successor"]
            repair_reason = (
                "legacy_manual_review_predecessor_has_verified_filled_retry_successor"
                if verified_retry_successor is not None
                else default_repair_reason
            )
            status_before = order.status
            apply_broker_order_status(
                order=order,
                status=NormalizedBrokerOrderStatus.STALE_TERMINAL_UNKNOWN.value,
            )
            _record_stale_order_repair_event(
                db=db,
                order=order,
                status_before=status_before,
                status_after=order.status,
                classification="stale_terminal_unknown",
                reason=repair_reason,
                observed_broker_code=order.last_error_code,
                observed_at=_observation_time(),
                result={
                    "evidence_fingerprint": fingerprint,
                    "safety_policy_version": (
                        "legacy-manual-review-terminal-unknown-v4"
                    ),
                    "resolution_context": resolution_context,
                    "verified_retry_successor": verified_retry_successor,
                    "snapshot_source": snapshot.source,
                    "snapshot_observed_at": snapshot.observed_at.isoformat(),
                    "holdings_complete": snapshot.holdings_complete,
                    "open_orders_complete": snapshot.open_orders_complete,
                    "broker_position_match": False,
                    "broker_open_order_match": False,
                    "local_position_match": False,
                    "broker_mutations": 0,
                    "fills_written": 0,
                    "pnl_inferences": 0,
                    "execution_scope_inferences": 0,
                    "historical_outcome": "unknown",
                    "current_holding_absent": True,
                    "current_open_order_absent": True,
                    "fill_inferred": False,
                    "account_scope_attributed": False,
                    "account_product_code_proof": item[
                        "account_product_code_proof"
                    ],
                    "snapshot_route_product_code": item[
                        "snapshot_route_product_code"
                    ],
                    "plan_sibling_state": item["plan_sibling_state"],
                    "plan_item_status": item["plan_item_status"],
                },
                actor=normalized_actor,
                operator_reason=normalized_reason,
            )
            db.add(
                RuntimeRemediationEvent(
                    mode=settings.runtime_mode,
                    candidate_type="legacy_manual_review_order",
                    candidate_key=fingerprint,
                    subject_id=order.id,
                    outcome="stale_terminal_unknown",
                    actor=normalized_actor,
                    reason=normalized_reason,
                    evidence_summary={
                        "evidence_fingerprint": fingerprint,
                        "safety_policy_version": (
                            "legacy-manual-review-terminal-unknown-v4"
                        ),
                        "transition_reason": repair_reason,
                        "resolution_context": resolution_context,
                        "verified_retry_successor": verified_retry_successor,
                        "status_before": status_before,
                        "status_after": order.status,
                        "symbol": order.symbol,
                        "side": order.side,
                        "snapshot_source": snapshot.source,
                        "snapshot_observed_at": snapshot.observed_at.isoformat(),
                        "holdings_complete": snapshot.holdings_complete,
                        "open_orders_complete": snapshot.open_orders_complete,
                        "broker_position_match": False,
                        "broker_open_order_match": False,
                        "local_position_match": False,
                        "broker_mutations": 0,
                        "fills_written": 0,
                        "pnl_inferences": 0,
                        "execution_scope_inferences": 0,
                        "historical_outcome": "unknown",
                        "current_holding_absent": True,
                        "current_open_order_absent": True,
                        "fill_inferred": False,
                        "account_scope_attributed": False,
                        "account_product_code_proof": item[
                            "account_product_code_proof"
                        ],
                        "snapshot_route_product_code": item[
                            "snapshot_route_product_code"
                        ],
                        "plan_sibling_state": item[
                            "plan_sibling_state"
                        ],
                        "plan_item_status": item["plan_item_status"],
                    },
                    occurred_at=_observation_time(),
                )
            )
            item["repair_action"] = "repaired"
            item["status_after"] = order.status
            resolved_count += 1

        db.flush()
        for plan_item_id in sorted(approved_plan_item_ids):
            still_reserving = any(
                sibling.status in CAPITAL_RESERVING_ORDER_STATUSES
                for sibling in plan_siblings_by_item_id.get(plan_item_id, [])
            )
            if still_reserving:
                continue
            plan_item = plan_items_by_id.get(plan_item_id)
            approved_items_for_plan = [
                item_by_order_id[order_id]
                for order_id in approved_order_ids
                if order_by_id[order_id].execution_plan_item_id == plan_item_id
            ]
            verified_successors = [
                item["verified_retry_successor"]
                for item in approved_items_for_plan
                if item["verified_retry_successor"] is not None
            ]
            if plan_item is not None and verified_successors:
                set_plan_item_status(
                    db=db,
                    plan_item=plan_item,
                    status="filled",
                    source="operator_remediation",
                    reason=(
                        "verified filled retry successor preserves plan outcome"
                    ),
                    payload={
                        "approved_broker_order_ids": sorted(approved_order_ids),
                        "verified_retry_successors": verified_successors,
                        "actor": normalized_actor,
                        "operator_reason": normalized_reason,
                        "broker_mutations": 0,
                        "fills_written": 0,
                    },
                    occurred_at=_observation_time(),
                )
            elif (
                plan_item is not None
                and plan_item.status == "manual_review_required"
            ):
                set_plan_item_status(
                    db=db,
                    plan_item=plan_item,
                    status="broker_order_failed",
                    source="operator_remediation",
                    reason=default_repair_reason,
                    payload={
                        "approved_broker_order_ids": sorted(approved_order_ids),
                        "actor": normalized_actor,
                        "operator_reason": normalized_reason,
                        "broker_mutations": 0,
                        "fills_written": 0,
                    },
                    occurred_at=_observation_time(),
                )

    return {
        "minimum_age_seconds": LEGACY_MANUAL_REVIEW_MINIMUM_AGE_SECONDS,
        "candidate_order_count": len(orders),
        "eligible_order_count": eligible_count,
        "would_resolve_order_count": eligible_count if dry_run else 0,
        "resolved_order_count": resolved_count,
        "items": items,
    }


def _load_legacy_manual_review_orders(
    *,
    settings: Settings,
    db: Session,
    broker_name: str,
    stale_before: datetime,
    symbols: set[str] | None,
    for_update: bool,
) -> list[BrokerOrder]:
    where = _legacy_manual_review_order_conditions(
        settings=settings,
        broker_name=broker_name,
        stale_before=stale_before,
        symbols=symbols,
    )
    statement = (
        select(BrokerOrder)
        .options(
            selectinload(BrokerOrder.fills),
            selectinload(BrokerOrder.execution_failure),
            selectinload(BrokerOrder.plan_item).selectinload(
                ExecutionPlanItem.jobs
            ),
        )
        .where(*where)
        .order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())
    )
    if for_update:
        statement = statement.with_for_update().execution_options(
            populate_existing=True
        )
    return db.scalars(statement).all()


def _legacy_manual_review_order_conditions(
    *,
    settings: Settings,
    broker_name: str,
    stale_before: datetime,
    symbols: set[str] | None,
) -> list[Any]:
    conditions: list[Any] = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == broker_name,
        BrokerOrder.status
        == NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
        BrokerOrder.broker_order_id.is_(None),
        BrokerOrder.submitted_at.is_(None),
        BrokerOrder.venue.is_(None),
        BrokerOrder.asset_class.is_(None),
        BrokerOrder.last_error_classification == "missing_broker_order_id",
        BrokerOrder.created_at < stale_before,
    ]
    if symbols is not None:
        conditions.append(BrokerOrder.symbol.in_(sorted(symbols)))
    return conditions


def _reserve_sqlite_legacy_manual_review_approval_rows(
    *,
    settings: Settings,
    db: Session,
    broker_name: str,
    stale_before: datetime,
    symbols: set[str] | None,
    approved_order_ids: set[str],
) -> None:
    if not approved_order_ids or db.get_bind().dialect.name != "sqlite":
        return
    conditions = _legacy_manual_review_order_conditions(
        settings=settings,
        broker_name=broker_name,
        stale_before=stale_before,
        symbols=symbols,
    )
    conditions.append(BrokerOrder.id.in_(sorted(approved_order_ids)))
    table = BrokerOrder.__table__
    statement = (
        update(table)
        .where(*conditions)
        .values(status=table.c.status, updated_at=table.c.updated_at)
        .execution_options(synchronize_session=False)
    )
    try:
        result = db.execute(statement)
    except OperationalError as exc:
        if "locked" in str(exc).lower():
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review SQLite writer reservation failed: database is locked"
            ) from exc
        raise
    if result.rowcount != len(approved_order_ids):
        raise LegacyManualReviewApprovalConflictError(
            "legacy manual-review approval target drifted during writer reservation"
        )


def _acquire_legacy_manual_review_position_truth_fence(*, db: Session) -> None:
    if db.get_bind().dialect.name != "postgresql":
        return
    try:
        db.execute(
            text("LOCK TABLE execution_positions IN SHARE MODE NOWAIT")
        )
    except OperationalError as exc:
        original = getattr(exc, "orig", None)
        sqlstate = getattr(original, "sqlstate", None) or getattr(
            original,
            "pgcode",
            None,
        )
        if sqlstate == "55P03":
            raise LegacyManualReviewApprovalConflictError(
                "legacy manual-review position-truth fence is busy"
            ) from exc
        raise


def _load_legacy_manual_review_plan_siblings(
    *,
    db: Session,
    plan_item_ids: set[str],
    for_update: bool,
) -> tuple[
    dict[str, list[BrokerOrder]],
    dict[str, int],
    dict[str, list[ExecutionFill]],
    dict[str, list[BrokerOrderEvent]],
]:
    if not plan_item_ids:
        return {}, {}, {}, {}
    sibling_statement = (
        select(BrokerOrder)
        .where(
            BrokerOrder.execution_plan_item_id.in_(sorted(plan_item_ids)),
        )
        .order_by(BrokerOrder.execution_plan_item_id.asc(), BrokerOrder.id.asc())
    )
    if for_update:
        sibling_statement = sibling_statement.with_for_update().execution_options(
            populate_existing=True
        )
    siblings = db.scalars(sibling_statement).all()
    sibling_ids = [sibling.id for sibling in siblings]
    fill_counts = {sibling.id: 0 for sibling in siblings}
    fills_by_order_id: dict[str, list[ExecutionFill]] = {
        sibling.id: [] for sibling in siblings
    }
    events_by_order_id: dict[str, list[BrokerOrderEvent]] = {
        sibling.id: [] for sibling in siblings
    }
    if sibling_ids:
        fill_statement = (
            select(ExecutionFill)
            .where(ExecutionFill.broker_order_pk.in_(sibling_ids))
            .order_by(ExecutionFill.broker_order_pk.asc(), ExecutionFill.id.asc())
        )
        if for_update:
            fill_statement = fill_statement.with_for_update().execution_options(
                populate_existing=True
            )
        for fill in db.scalars(fill_statement).all():
            fill_counts[fill.broker_order_pk] = (
                fill_counts.get(fill.broker_order_pk, 0) + 1
            )
            fills_by_order_id.setdefault(fill.broker_order_pk, []).append(fill)
        event_statement = (
            select(BrokerOrderEvent)
            .where(BrokerOrderEvent.broker_order_pk.in_(sibling_ids))
            .order_by(
                BrokerOrderEvent.broker_order_pk.asc(),
                BrokerOrderEvent.observed_at.asc(),
                BrokerOrderEvent.id.asc(),
            )
        )
        if for_update:
            event_statement = event_statement.with_for_update().execution_options(
                populate_existing=True
            )
        for order_event in db.scalars(event_statement).all():
            events_by_order_id.setdefault(
                order_event.broker_order_pk,
                [],
            ).append(order_event)

    siblings_by_item_id: dict[str, list[BrokerOrder]] = {}
    for sibling in siblings:
        if sibling.execution_plan_item_id is None:
            continue
        siblings_by_item_id.setdefault(
            sibling.execution_plan_item_id,
            [],
        ).append(sibling)
    return (
        siblings_by_item_id,
        fill_counts,
        fills_by_order_id,
        events_by_order_id,
    )


def _load_legacy_manual_review_active_jobs(
    *,
    db: Session,
    plan_item_ids: set[str],
    for_update: bool,
) -> dict[str, list[ExecutionJob]]:
    if not plan_item_ids:
        return {}
    statement = (
        select(ExecutionJob)
        .where(
            ExecutionJob.execution_plan_item_id.in_(sorted(plan_item_ids)),
        )
        .order_by(ExecutionJob.execution_plan_item_id.asc(), ExecutionJob.id.asc())
    )
    if for_update:
        statement = statement.with_for_update().execution_options(
            populate_existing=True
        )
    jobs_by_item_id: dict[str, list[ExecutionJob]] = {}
    for job in db.scalars(statement).all():
        if job.status not in LEGACY_MANUAL_REVIEW_ACTIVE_JOB_STATUSES:
            continue
        jobs_by_item_id.setdefault(job.execution_plan_item_id, []).append(job)
    return jobs_by_item_id


def _legacy_manual_review_plan_sibling_state(
    *,
    siblings: list[BrokerOrder],
    fills_by_order_id: dict[str, list[ExecutionFill]],
) -> list[dict[str, object]]:
    return [
        {
            "broker_order_pk": sibling.id,
            "status": sibling.status,
            "fill_count": len(fills_by_order_id.get(sibling.id, [])),
            "fills": _legacy_manual_review_fill_state(
                fills_by_order_id.get(sibling.id, [])
            ),
        }
        for sibling in sorted(siblings, key=lambda sibling: sibling.id)
    ]


def _legacy_manual_review_fill_state(
    fills: list[ExecutionFill],
) -> list[dict[str, object]]:
    return [
        {
            "fill_id": fill.id,
            "broker_fill_id": fill.broker_fill_id,
            "symbol": fill.symbol,
            "side": fill.side,
            "quantity": fill.quantity,
            "base_quantity": _normalized_decimal_evidence_value(
                fill.base_quantity
            ),
            "price": _normalized_decimal_evidence_value(fill.price),
            "filled_at": _normalized_timestamp_signature(fill.filled_at),
        }
        for fill in sorted(fills, key=lambda fill: (fill.filled_at, fill.id))
    ]


def _normalized_decimal_evidence_value(
    value: Decimal | None,
) -> str | None:
    if value is None:
        return None
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _verified_legacy_manual_review_retry_successor(
    *,
    settings: Settings,
    order: BrokerOrder,
    plan_item: ExecutionPlanItem,
    outcome_bearing_siblings: list[BrokerOrder],
    fills_by_order_id: dict[str, list[ExecutionFill]],
    events_by_order_id: dict[str, list[BrokerOrderEvent]],
) -> dict[str, object] | None:
    expected_candidate_key = f"monitoring:stop_sell:{plan_item.id}"
    if (
        plan_item.intent != "stop_sell"
        or order.side != "sell"
        or order.idempotency_key != expected_candidate_key
        or len(outcome_bearing_siblings) != 1
    ):
        return None
    successor = outcome_bearing_siblings[0]
    if (
        successor.status != NormalizedBrokerOrderStatus.FILLED.value
        or successor.mode != order.mode
        or successor.mode != settings.runtime_mode
        or successor.broker_name != order.broker_name
        or successor.broker_name != "kis"
        or successor.execution_plan_item_id != order.execution_plan_item_id
        or successor.symbol != order.symbol
        or successor.side != order.side
        or successor.quantity is None
        or order.quantity is None
        or successor.quantity != order.quantity
        or not successor.broker_order_id
        or successor.submitted_at is None
    ):
        return None

    candidate_metadata = (
        order.request_metadata
        if isinstance(order.request_metadata, dict)
        else {}
    )
    successor_request_metadata = (
        successor.request_metadata
        if isinstance(successor.request_metadata, dict)
        else {}
    )
    successor_response_metadata = (
        successor.response_metadata
        if isinstance(successor.response_metadata, dict)
        else {}
    )
    candidate_account_mask = str(
        candidate_metadata.get("account_number_masked") or ""
    ).strip()
    successor_account_mask = str(
        successor_response_metadata.get("account_number_masked") or ""
    ).strip()
    if (
        str(successor_request_metadata.get("provider") or "").strip().lower()
        != "kis"
        or not candidate_account_mask
        or successor_account_mask != candidate_account_mask
    ):
        return None

    successor_submitted_at = _required_evidence_timestamp(
        successor.submitted_at
    )
    if successor_submitted_at is None:
        return None
    try:
        market_timezone = ZoneInfo(settings.market_timezone)
    except ZoneInfoNotFoundError:
        market_timezone = UTC
    expected_successor_key = (
        f"{order.idempotency_key}:"
        f"{successor_submitted_at.astimezone(market_timezone).date().isoformat()}"
    )
    if successor.idempotency_key != expected_successor_key:
        return None

    retryable_failures = [
        event
        for event in events_by_order_id.get(order.id, [])
        if event.event_type == "submit_failed"
        and event.normalized_status
        == NormalizedBrokerOrderStatus.RETRY_PENDING.value
        and event.retryable is True
        and bool(order.last_error_code)
        and event.broker_status == order.last_error_code
    ]
    if not retryable_failures:
        return None
    retryable_failure = max(
        retryable_failures,
        key=lambda event: (event.observed_at, event.id),
    )
    failure_observed_at = _required_evidence_timestamp(
        retryable_failure.observed_at
    )
    if failure_observed_at is None:
        return None
    risk_context = (
        plan_item.risk_context
        if isinstance(plan_item.risk_context, dict)
        else {}
    )
    monitoring_events = risk_context.get("monitoring_events")
    if not isinstance(monitoring_events, list):
        return None
    matching_rearms: list[tuple[datetime, dict[str, object]]] = []
    for raw_event in monitoring_events:
        if not isinstance(raw_event, dict):
            continue
        if (
            raw_event.get("event") != "protective_stop_rearmed"
            or raw_event.get("reason") != "retryable_session_rejection"
            or raw_event.get("prior_status") != "broker_order_retry_pending"
        ):
            continue
        recorded_at = _required_evidence_timestamp(
            raw_event.get("recorded_at")
        )
        if recorded_at is None:
            continue
        if failure_observed_at <= recorded_at <= successor_submitted_at:
            matching_rearms.append((recorded_at, raw_event))
    if not matching_rearms:
        return None
    rearmed_at, rearm_event = max(matching_rearms, key=lambda item: item[0])

    successor_fills = fills_by_order_id.get(successor.id, [])
    if not successor_fills or any(
        fill.symbol != order.symbol
        or fill.side != order.side
        or fill.quantity is None
        or Decimal(fill.quantity) <= 0
        for fill in successor_fills
    ):
        return None
    filled_quantity = sum(
        (Decimal(fill.quantity) for fill in successor_fills),
        start=Decimal("0"),
    )
    if filled_quantity != Decimal(order.quantity):
        return None

    return {
        "broker_order_pk": successor.id,
        "broker_order_id": successor.broker_order_id,
        "idempotency_key": successor.idempotency_key,
        "mode": successor.mode,
        "broker_name": successor.broker_name,
        "symbol": successor.symbol,
        "side": successor.side,
        "status": successor.status,
        "quantity": successor.quantity,
        "filled_quantity": str(filled_quantity),
        "submitted_at": _normalized_timestamp_signature(
            successor.submitted_at
        ),
        "account_number_masked": successor_account_mask,
        "retryable_failure_event": {
            "event_id": retryable_failure.id,
            "observed_at": _normalized_timestamp_signature(
                retryable_failure.observed_at
            ),
            "normalized_status": retryable_failure.normalized_status,
            "error_classification": retryable_failure.error_classification,
            "broker_status": retryable_failure.broker_status,
            "retryable": retryable_failure.retryable,
        },
        "rearm_event": {
            **_json_safe_dict(rearm_event),
            "recorded_at": _normalized_timestamp_signature(rearmed_at),
        },
        "fills": _legacy_manual_review_fill_state(successor_fills),
        "plan_item_outcome_status": "filled",
    }


def _legacy_manual_review_snapshot_failure(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    evaluated_at: datetime,
) -> str | None:
    if snapshot.source != "kis:account_snapshot":
        return "broker_snapshot_source_invalid"
    if snapshot.holdings_complete is not True:
        return "broker_holdings_snapshot_incomplete"
    if snapshot.open_orders_complete is not True:
        return "broker_open_orders_snapshot_incomplete"
    if not snapshot_is_fresh_for_recovery(
        observed_at=snapshot.observed_at,
        now=evaluated_at,
        settings=settings,
    ):
        return "broker_snapshot_not_fresh"
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    if str(payload.get("exchange") or "").strip().upper() != "KRX":
        return "broker_snapshot_route_invalid"
    if not str(payload.get("account_number_masked") or "").strip():
        return "broker_snapshot_account_proof_missing"
    return None


def _legacy_manual_review_order_route_failure(
    *,
    settings: Settings,
    order: BrokerOrder,
    snapshot: BrokerAccountSnapshot | None,
) -> str | None:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    route = metadata.get("route")
    if not isinstance(route, dict):
        return "legacy_route_proof_missing"
    if "execution_scope" in metadata:
        return "legacy_order_has_canonical_scope"
    if str(metadata.get("provider") or "").strip().lower() != "kis":
        return "legacy_provider_proof_invalid"
    _, account_product_code_failure = (
        _legacy_manual_review_account_product_code_proof(
            settings=settings,
            order=order,
        )
    )
    if account_product_code_failure is not None:
        return account_product_code_failure
    if str(route.get("exchange") or "").strip().upper() != "KRX":
        return "legacy_route_proof_invalid"
    if metadata.get("endpoint") != "/uapi/domestic-stock/v1/trading/order-cash":
        return "legacy_endpoint_proof_invalid"
    if str(metadata.get("method") or "").strip().upper() != "POST":
        return "legacy_method_proof_invalid"
    live_prefix = "T" if settings.runtime_mode == "live" else "V"
    expected_tr_id = (
        f"{live_prefix}TTC0012U"
        if str(order.side or "").strip().lower() == "buy"
        else f"{live_prefix}TTC0011U"
        if str(order.side or "").strip().lower() == "sell"
        else None
    )
    if expected_tr_id is None or metadata.get("tr_id") != expected_tr_id:
        return "legacy_transaction_route_proof_invalid"
    stored_mask = str(metadata.get("account_number_masked") or "").strip()
    snapshot_payload = (
        snapshot.payload
        if snapshot is not None and isinstance(snapshot.payload, dict)
        else {}
    )
    snapshot_mask = str(snapshot_payload.get("account_number_masked") or "").strip()
    if not stored_mask or not snapshot_mask or stored_mask != snapshot_mask:
        return "legacy_account_route_proof_invalid"
    return None


def _legacy_manual_review_account_product_code_proof(
    *,
    settings: Settings,
    order: BrokerOrder,
) -> tuple[str | None, str | None]:
    metadata = (
        order.request_metadata
        if isinstance(order.request_metadata, dict)
        else {}
    )
    configured_product_code = settings.kis_account_product_code
    if not isinstance(configured_product_code, str) or not configured_product_code:
        return None, "configured_account_product_code_missing"
    if "account_product_code" not in metadata:
        return "legacy_missing", None
    stored_product_code = metadata.get("account_product_code")
    if (
        not isinstance(stored_product_code, str)
        or not stored_product_code
        or stored_product_code != configured_product_code
    ):
        return None, "legacy_account_product_code_proof_invalid"
    return "exact_match", None


def _legacy_manual_review_evidence_fingerprint(
    *,
    order: BrokerOrder,
    snapshot: BrokerAccountSnapshot | None,
    broker_position_match: bool,
    broker_open_order_match: bool,
    local_position_match: bool,
    order_fill_count: int,
    active_execution_jobs: list[ExecutionJob],
    plan_sibling_state: list[dict[str, object]],
    plan_item_status: str | None,
    verified_retry_successor: dict[str, object] | None,
    account_product_code_proof: str | None,
    snapshot_route_product_code: str,
) -> str:
    snapshot_payload = (
        snapshot.payload
        if snapshot is not None and isinstance(snapshot.payload, dict)
        else {}
    )
    material = {
        "policy_version": "legacy-manual-review-terminal-unknown-v4",
        "order": {
            "id": order.id,
            "execution_plan_item_id": order.execution_plan_item_id,
            "mode": order.mode,
            "broker_name": order.broker_name,
            "idempotency_key": order.idempotency_key,
            "broker_order_id": order.broker_order_id,
            "symbol": order.symbol,
            "side": order.side,
            "status": order.status,
            "quantity": order.quantity,
            "base_quantity": order.base_quantity,
            "quote_notional": order.quote_notional,
            "venue": order.venue,
            "asset_class": order.asset_class,
            "limit_price": order.limit_price,
            "created_at": (
                _normalized_timestamp_signature(order.created_at)
                if order.created_at is not None
                else None
            ),
            "submitted_at": (
                _normalized_timestamp_signature(order.submitted_at)
                if order.submitted_at is not None
                else None
            ),
            "last_broker_sync_at": (
                _normalized_timestamp_signature(order.last_broker_sync_at)
                if order.last_broker_sync_at is not None
                else None
            ),
            "last_error_classification": order.last_error_classification,
            "last_error_code": order.last_error_code,
            "last_error_message": order.last_error_message,
            "last_error_retryable": order.last_error_retryable,
            "request_metadata": order.request_metadata,
            "response_metadata": order.response_metadata,
            "raw_broker_payload": order.raw_broker_payload,
            "fill_count": order_fill_count,
            "linked_execution_failure_count": (
                1 if order.execution_failure is not None else 0
            ),
            "active_execution_jobs": sorted(
                (job.id, job.status)
                for job in active_execution_jobs
            ),
            "plan_sibling_state": plan_sibling_state,
            "plan_item_status": plan_item_status,
            "verified_retry_successor": verified_retry_successor,
        },
        "broker_evidence": {
            "source": snapshot.source if snapshot is not None else None,
            "exchange": snapshot_payload.get("exchange"),
            "account_number_masked": snapshot_payload.get(
                "account_number_masked"
            ),
            "account_product_code_proof": account_product_code_proof,
            "snapshot_route_product_code": snapshot_route_product_code,
            "holdings_complete": (
                snapshot.holdings_complete if snapshot is not None else False
            ),
            "open_orders_complete": (
                snapshot.open_orders_complete if snapshot is not None else False
            ),
            "broker_position_match": broker_position_match,
            "broker_open_order_match": broker_open_order_match,
            "local_position_match": local_position_match,
        },
    }
    encoded = json.dumps(
        _json_safe_value(material),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _resolve_open_failures_for_expired_reconciliation_orders(
    *,
    settings: Settings,
    db: Session,
    broker_name: str,
    symbols: set[str] | None,
    observed_at: datetime,
    dry_run: bool,
) -> dict[str, object]:
    failures = _load_open_failures_for_expired_reconciliation_orders(
        settings=settings,
        db=db,
        broker_name=broker_name,
        symbols=symbols,
    )
    items: list[dict[str, object]] = []
    resolved_count = 0
    for failure in failures:
        order = failure.broker_order
        if not _is_lookup_window_expired_evidence(
            code=order.last_error_code or failure.broker_error_code,
            message=order.last_error_message or failure.broker_error_message,
        ):
            items.append(
                _expired_failure_cleanup_item(
                    failure=failure,
                    order=order,
                    action="skipped",
                    reason="missing_lookup_window_expired_evidence",
                )
            )
            continue

        resolved_count += 1
        reason = "broker_lookup_window_expired"
        if not dry_run:
            status_before = failure.status
            resolve_execution_failure_for_order(
                db=db,
                order=order,
                observed_at=observed_at,
                source="stale_order_reconciliation_expired_order_cleanup",
                reason=reason,
            )
            _record_expired_order_failure_cleanup_event(
                db=db,
                order=order,
                failure=failure,
                status_before=status_before,
                observed_at=observed_at,
                reason=reason,
            )
        items.append(
            _expired_failure_cleanup_item(
                failure=failure,
                order=order,
                action="would_resolve" if dry_run else "resolved",
                reason=reason,
            )
        )

    return {
        "candidate_failure_count": len(failures),
        "resolved_failure_count": 0 if dry_run else resolved_count,
        "would_resolve_failure_count": resolved_count if dry_run else 0,
        "items": items,
    }


def _load_open_failures_for_expired_reconciliation_orders(
    *,
    settings: Settings,
    db: Session,
    broker_name: str,
    symbols: set[str] | None,
) -> list[ExecutionFailure]:
    where = [
        ExecutionFailure.mode == settings.runtime_mode,
        ExecutionFailure.broker_name == broker_name,
        ExecutionFailure.status == "open",
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == broker_name,
        BrokerOrder.status == NormalizedBrokerOrderStatus.RECONCILE_WINDOW_EXPIRED.value,
    ]
    if symbols is not None:
        where.append(BrokerOrder.symbol.in_(sorted(symbols)))
    return db.scalars(
        select(ExecutionFailure)
        .join(ExecutionFailure.broker_order)
        .options(selectinload(ExecutionFailure.broker_order))
        .where(*where)
        .order_by(ExecutionFailure.latest_failed_at.asc(), ExecutionFailure.created_at.asc())
    ).all()


def _record_expired_order_failure_cleanup_event(
    *,
    db: Session,
    order: BrokerOrder,
    failure: ExecutionFailure,
    status_before: str,
    observed_at: datetime,
    reason: str,
) -> None:
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="expired_reconciliation_failure_resolved",
            event_source=BrokerEventSource.RECONCILIATION.value,
            normalized_status=order.status,
            broker_status=order.last_error_code or failure.broker_error_code,
            error_classification="reconcile_window_expired",
            retryable=False,
            message=reason,
            payload={
                "execution_failure_id": failure.id,
                "failure_status_before": status_before,
                "failure_status_after": failure.status,
                "status_updated_source": failure.status_updated_source,
                "status_updated_reason": failure.status_updated_reason,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "broker_error_code": failure.broker_error_code,
                "broker_error_classification": failure.broker_error_classification,
            },
            observed_at=observed_at,
        )
    )


def _expired_failure_cleanup_item(
    *,
    failure: ExecutionFailure,
    order: BrokerOrder,
    action: str,
    reason: str,
) -> dict[str, object]:
    return {
        "execution_failure_id": failure.id,
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "symbol": order.symbol,
        "side": order.side,
        "failure_status": failure.status,
        "order_status": order.status,
        "action": action,
        "reason": reason,
        "broker_error_code": failure.broker_error_code,
    }


def _load_stale_nonterminal_orders_for_reconciliation(
    *,
    db: Session,
    settings: Settings,
    broker_name: str,
    stale_before: datetime,
    symbols: set[str] | None,
) -> list[BrokerOrder]:
    observed_at = broker_order_observed_at_expr()
    where = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == broker_name,
        BrokerOrder.status.in_(_reconcilable_order_statuses()),
        observed_at < stale_before,
    ]
    if symbols is not None:
        where.append(BrokerOrder.symbol.in_(sorted(symbols)))
    return db.scalars(
        select(BrokerOrder)
        .options(
            selectinload(BrokerOrder.fills),
            selectinload(BrokerOrder.plan_item),
        )
        .where(*where)
        .order_by(observed_at.asc(), BrokerOrder.created_at.asc())
    ).all()


def _stale_order_reconciliation_classification(
    *,
    status: str,
    status_code: int,
    message: object,
) -> tuple[str, str | None]:
    normalized = _normalize_status(status) or status
    if status_code >= 400:
        return "failed_to_reconcile", _optional_str(message) or "broker_truth_unreachable"
    if normalized in {
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
    }:
        return "live", None
    if normalized == NormalizedBrokerOrderStatus.CANCELED.value:
        return "cancelled", None
    if normalized in {
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.REJECTED.value,
        NormalizedBrokerOrderStatus.FAILED.value,
        NormalizedBrokerOrderStatus.EXPIRED.value,
    }:
        return "closed", None
    if normalized == NormalizedBrokerOrderStatus.RETRY_PENDING.value:
        return "failed_to_reconcile", "local_retry_pending_not_broker_live_state"
    if normalized == NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value:
        return "manual_review_required", "manual_review_required"
    if normalized == NormalizedBrokerOrderStatus.RECONCILE_WINDOW_EXPIRED.value:
        return "reconcile_window_expired", "broker_lookup_window_expired"
    if normalized == NormalizedBrokerOrderStatus.STALE_TERMINAL_UNKNOWN.value:
        return "stale_terminal_unknown", "repeated_unknown_status_after_stale_threshold"
    return "unknown", _optional_str(message)


def _stale_order_dry_run_result(*, order: BrokerOrder) -> dict[str, object]:
    return {
        "status_code": 200,
        "broker_order_id": order.broker_order_id,
        "status": order.status,
        "status_before": order.status,
        "status_changed": False,
        "fills_added": 0,
        "filled_delta": 0,
        "message": order.last_error_message,
    }


def _should_terminalize_absent_prior_session_order(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    order: BrokerOrder,
    payload: dict[str, Any],
    observed_at: datetime,
) -> bool:
    if adapter.provider != "kis" or order.status not in _reconcilable_order_statuses():
        return False
    status = str(payload.get("status") or "").strip().lower()
    message = str(payload.get("message") or "").strip().lower()
    if status != "unknown" or "not found in broker order query" not in message:
        return False
    age_anchor = order.submitted_at or order.created_at
    if age_anchor is None:
        return False
    age_anchor_utc = _parse_timestamp(age_anchor, fallback=observed_at)
    if observed_at - age_anchor_utc < timedelta(
        seconds=settings.stale_broker_order_reconciliation_threshold_seconds
    ):
        return False
    try:
        market_timezone = ZoneInfo(settings.market_timezone)
    except ZoneInfoNotFoundError:
        market_timezone = UTC
    return age_anchor_utc.astimezone(market_timezone).date() < observed_at.astimezone(
        market_timezone
    ).date()


def _stale_order_repair_decision(
    *,
    order: BrokerOrder,
    status_before: str,
    classification: str,
    reason: str | None,
    result: dict[str, object] | None,
    error_code_before: str | None,
    error_classification_before: str | None,
) -> dict[str, str] | None:
    if status_before not in _reconcilable_order_statuses():
        return None
    observed_broker_code = order.last_error_code or error_code_before
    observed_classification = order.last_error_classification or error_classification_before
    if not order.broker_order_id:
        return {
            "status_after": NormalizedBrokerOrderStatus.MANUAL_REVIEW_REQUIRED.value,
            "classification": "manual_review_required",
            "reason": "missing_broker_order_id",
            "observed_broker_code": observed_broker_code or "missing_broker_order_id",
        }
    if _is_lookup_window_expired_evidence(
        code=observed_broker_code,
        message=order.last_error_message or _optional_str((result or {}).get("message")),
    ):
        return {
            "status_after": NormalizedBrokerOrderStatus.RECONCILE_WINDOW_EXPIRED.value,
            "classification": "reconcile_window_expired",
            "reason": "broker_lookup_window_expired",
            "observed_broker_code": observed_broker_code or "lookup_window_expired",
        }
    if (
        observed_classification == "unknown_status"
        or str((result or {}).get("status") or "").strip().lower() == "unknown"
        or classification == "unknown"
    ):
        return {
            "status_after": NormalizedBrokerOrderStatus.STALE_TERMINAL_UNKNOWN.value,
            "classification": "stale_terminal_unknown",
            "reason": "repeated_unknown_status_after_stale_threshold",
            "observed_broker_code": observed_broker_code or "unknown",
        }
    return None


def _is_lookup_window_expired_evidence(*, code: str | None, message: str | None) -> bool:
    normalized_code = (code or "").strip().upper()
    if normalized_code == "KIER2570":
        return True
    normalized_message = (message or "").strip()
    return "조회일자를 확인하십시오" in normalized_message


def _is_nonactionable_stale_result(status: str) -> bool:
    return status in {
        NormalizedBrokerOrderStatus.CANCELED.value,
        NormalizedBrokerOrderStatus.FILLED.value,
        NormalizedBrokerOrderStatus.REJECTED.value,
        NormalizedBrokerOrderStatus.FAILED.value,
        NormalizedBrokerOrderStatus.EXPIRED.value,
    }


def _record_stale_order_repair_event(
    *,
    db: Session,
    order: BrokerOrder,
    status_before: str,
    status_after: str,
    classification: str,
    reason: str | None,
    observed_broker_code: str | None,
    observed_at: datetime,
    result: dict[str, object] | None,
    actor: str | None = None,
    operator_reason: str | None = None,
) -> None:
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="stale_order_repair",
            event_source=BrokerEventSource.RECONCILIATION.value,
            normalized_status=status_after,
            broker_status=observed_broker_code,
            error_classification=classification,
            retryable=False,
            message=reason,
            payload={
                "status_before": status_before,
                "status_after": status_after,
                "observed_broker_code": observed_broker_code,
                "classification": classification,
                "reason": reason,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "result": _json_safe_value(result) if result is not None else None,
                "actor": actor,
                "operator_reason": operator_reason,
            },
            observed_at=observed_at,
        )
    )


def _record_stale_order_classification_event(
    *,
    db: Session,
    order: BrokerOrder,
    classification: str,
    reason: str | None,
    observed_at: datetime,
    result: dict[str, object] | None,
    actor: str | None = None,
    operator_reason: str | None = None,
) -> None:
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="stale_order_reconcile_classification",
            event_source=BrokerEventSource.RECONCILIATION.value,
            normalized_status=order.status,
            broker_status=order.last_error_code,
            error_classification=classification,
            retryable=order.last_error_retryable,
            message=reason,
            payload={
                "classification": classification.replace("_", "-"),
                "reason": reason,
                "status": order.status,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
                "result": _json_safe_value(result) if result is not None else None,
                "actor": actor,
                "operator_reason": operator_reason,
            },
            observed_at=observed_at,
        )
    )


def _load_pending_orders_for_reconciliation(
    *,
    db: Session,
    settings: Settings,
    broker_name: str,
    symbols: set[str] | None,
) -> list[BrokerOrder]:
    where = [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.broker_name == broker_name,
        BrokerOrder.status.in_(_reconcilable_order_statuses()),
        BrokerOrder.broker_order_id.is_not(None),
    ]
    if symbols is not None:
        where.append(BrokerOrder.symbol.in_(sorted(symbols)))
    return db.scalars(
        select(BrokerOrder)
        .options(
            selectinload(BrokerOrder.fills),
            selectinload(BrokerOrder.plan_item),
        )
        .where(*where)
        .order_by(BrokerOrder.created_at.asc())
    ).all()


def sync_execution_positions_from_broker_snapshot(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str] | None = None,
    exchange: str = "KRX",
    discover_holdings_only: bool = False,
    commit: bool = True,
    authenticated_access_token: str | None = None,
) -> dict[str, object]:
    normalized_symbols = _normalize_symbols(symbols)
    if authenticated_access_token is None:
        if not commit:
            raise ValueError(
                "commit=False position sync requires independently persisted authentication"
            )
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="reconciliation",
            reason="refresh execution positions from broker snapshot",
        )
        access_token = session_state.access_token or ""
    else:
        access_token = authenticated_access_token
    snapshot = adapter.get_account_snapshot(
        access_token,
        symbols=sorted(normalized_symbols) if normalized_symbols is not None else None,
        exchange=exchange,
    )
    normalized_provider = str(adapter.provider or "").strip().lower()
    if not snapshot.holdings_complete:
        partial_exposure = _supersede_kis_krx_flat_proof_from_partial_exposure(
            settings=settings,
            db=db,
            snapshot=snapshot,
            normalized_provider=normalized_provider,
            requested_exchange=exchange,
            symbols=normalized_symbols,
        )
        if partial_exposure["superseded_count"]:
            if commit:
                db.commit()
            else:
                db.flush()
                return {
                    "tracked_symbol_count": partial_exposure[
                        "tracked_symbol_count"
                    ],
                    "created": 0,
                    "updated": partial_exposure["positive_holding_count"],
                    "zeroed": 0,
                    "monitoring_recovered_count": 0,
                    "partial_exposure_superseded_count": partial_exposure[
                        "superseded_count"
                    ],
                    "holdings_complete": False,
                    "execution_scope": partial_exposure["execution_scope"],
                    "execution_scope_source": partial_exposure[
                        "execution_scope_source"
                    ],
                }
        raise BrokerAdapterError(
            "broker account snapshot holdings coverage is incomplete; position sync blocked",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
            response_payload={"diagnostics": snapshot.diagnostics},
            observed_at=snapshot.observed_at,
        )
    identity_exchange = _position_sync_identity_exchange(
        provider=normalized_provider,
        requested_exchange=exchange,
    )
    position_identity = (
        build_authenticated_broker_position_identity(
            settings=settings,
            provider=normalized_provider,
            exchange=identity_exchange,
            broker_account_id=snapshot.broker_account_id or "",
        )
        if normalized_provider == "alpaca"
        else build_position_identity(
            settings=settings,
            provider=normalized_provider,
            exchange=identity_exchange,
            broker_account_id=snapshot.broker_account_id,
        )
    )
    routed_holdings = _position_sync_holdings_for_route(
        holdings=snapshot.holdings,
        provider=normalized_provider,
        exchange=identity_exchange,
        observed_at=snapshot.observed_at,
    )
    execution_scope = {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": position_identity.provider,
        "account_scope": position_identity.account_scope,
    }
    execution_scope_source = (
        "authenticated_broker_account"
        if snapshot.broker_account_id
        else "configured_account_route"
    )
    if discover_holdings_only:
        if normalized_symbols is not None:
            raise ValueError("holdings-only discovery requires symbols=None")
        tracked_symbols = {holding.symbol for holding in routed_holdings}
    elif normalized_symbols is not None:
        tracked_symbols = normalized_symbols
    else:
        local_symbols = {
            position.symbol
            for position in db.scalars(
                select(ExecutionPosition).where(
                    ExecutionPosition.mode == settings.runtime_mode,
                )
            ).all()
            if _position_belongs_to_sync_scope(
                position=position,
                identity=position_identity,
            )
        }
        tracked_symbols = local_symbols | {holding.symbol for holding in routed_holdings}
    if not tracked_symbols:
        return {
            "tracked_symbol_count": 0,
            "created": 0,
            "updated": 0,
            "zeroed": 0,
            "monitoring_recovered_count": 0,
            "execution_scope": execution_scope,
            "execution_scope_source": execution_scope_source,
        }

    holding_by_symbol = {
        holding.symbol: holding
        for holding in routed_holdings
        if holding.symbol in tracked_symbols
    }
    position_by_symbol, created_symbols = _lock_execution_position_rows(
        db=db,
        mode=settings.runtime_mode,
        symbols=tracked_symbols,
        identity=position_identity,
        holdings_by_symbol=holding_by_symbol,
    )
    observed_at = snapshot.observed_at

    legacy_positions_to_bind: list[ExecutionPosition] = []
    for symbol in sorted(tracked_symbols):
        position = position_by_symbol[symbol]
        stored_identity = position_identity_from_payload(position.raw_payload)
        if stored_identity is not None:
            if stored_identity != position_identity:
                raise PositionIdentityConflictError(
                    "position route conflicts with the broker snapshot route"
                )
            continue
        if (
            position.last_reconciled_at is not None
            and _utc_naive(observed_at) < _utc_naive(position.last_reconciled_at)
        ):
            continue
        if not _legacy_position_is_empty_and_safe_to_bind(position=position):
            raise PositionIdentityConflictError(
                "legacy non-flat position requires explicit account mapping"
            )
        legacy_positions_to_bind.append(position)

    for position in legacy_positions_to_bind:
        position.raw_payload = payload_with_position_identity(
            position.raw_payload,
            identity=position_identity,
        )

    created = len(created_symbols)
    updated = 0
    zeroed = 0
    monitoring_recovered_count = 0
    for symbol in sorted(tracked_symbols):
        holding = holding_by_symbol.get(symbol)
        position = position_by_symbol.get(symbol)
        assert position is not None
        if (
            position.last_reconciled_at is not None
            and _utc_naive(observed_at)
            < _utc_naive(position.last_reconciled_at)
        ):
            continue
        if holding is None:
            if _preserve_fresh_kis_krx_flat_proof_during_sync(
                settings=settings,
                db=db,
                adapter=adapter,
                snapshot=snapshot,
                position=position,
                identity=position_identity,
                symbol=symbol,
                exchange=identity_exchange,
                evaluated_at=_observation_time(),
            ):
                continue
            changed = False
            if position.quantity != 0:
                position.quantity = 0
                changed = True
            if (
                normalized_provider == "kis"
                and identity_exchange == "KRX"
                and position.base_quantity != Decimal("0")
            ):
                position.base_quantity = Decimal("0")
                changed = True
            elif position.base_quantity is not None and position.base_quantity != 0:
                position.base_quantity = Decimal("0")
                changed = True
            if (
                normalized_provider == "kis"
                and identity_exchange == "KRX"
                and position.average_price is not None
            ):
                position.average_price = None
                changed = True
            if changed:
                zeroed += 1
            position.updated_from = "reconciliation"
            position.last_reconciled_at = observed_at
            position.raw_payload = _position_snapshot_payload(
                snapshot_payload=snapshot.payload,
                holding=None,
                exchange=identity_exchange,
                identity=position_identity,
                prior_payload=position.raw_payload,
            )
            continue

        holding_base_quantity = _holding_base_quantity(holding.payload)
        changed = False
        expected_quantity = 0 if holding_base_quantity is not None else holding.quantity
        if position.quantity != expected_quantity:
            position.quantity = expected_quantity
            changed = True
        if holding_base_quantity is not None and position.base_quantity != holding_base_quantity:
            position.base_quantity = holding_base_quantity
            changed = True
        if holding_base_quantity is None and position.base_quantity is not None:
            position.base_quantity = None
            changed = True
        if position.average_price != holding.average_price:
            position.average_price = holding.average_price
            changed = True
        if position.updated_from != "reconciliation":
            position.updated_from = "reconciliation"
            changed = True
        position.last_reconciled_at = observed_at
        monitoring_recovery_evidence = _unpriced_kis_us_holding_recovery_evidence(
            settings=settings,
            snapshot=snapshot,
            holding=holding,
            exchange=identity_exchange,
            prior_payload=position.raw_payload,
            evaluated_at=_observation_time(),
        )
        position.raw_payload = _position_snapshot_payload(
            snapshot_payload=snapshot.payload,
            holding=holding,
            exchange=identity_exchange,
            identity=position_identity,
            prior_payload=position.raw_payload,
            monitoring_recovery_evidence=monitoring_recovery_evidence,
        )
        if monitoring_recovery_evidence is not None:
            monitoring_recovered_count += 1
            prior_recorded_at = monitoring_recovery_evidence[
                "manual_review_recorded_at"
            ]
            candidate_key = sha256(
                (
                    "unpriced-kis-us-holding-recovery:v1|"
                    f"{position.id}|{prior_recorded_at}|"
                    f"{snapshot.observed_at.isoformat()}"
                ).encode("utf-8")
            ).hexdigest()
            db.add(
                RuntimeRemediationEvent(
                    mode=settings.runtime_mode,
                    candidate_type="position_monitoring_manual_review",
                    candidate_key=candidate_key,
                    subject_id=position.id,
                    outcome="monitoring_reenabled",
                    actor="system",
                    reason="fresh_complete_kis_holding_is_priceable_and_sellable",
                    evidence_summary=monitoring_recovery_evidence
                    | {
                        "orders_submitted": 0,
                        "orders_amended": 0,
                        "orders_canceled": 0,
                    },
                    occurred_at=_observation_time(),
                )
            )
        if changed and symbol not in created_symbols:
            updated += 1

    if commit:
        db.commit()
    return {
        "tracked_symbol_count": len(tracked_symbols),
        "created": created,
        "updated": updated,
        "zeroed": zeroed,
        "monitoring_recovered_count": monitoring_recovered_count,
        "execution_scope": execution_scope,
        "execution_scope_source": execution_scope_source,
    }


def _supersede_kis_krx_flat_proof_from_partial_exposure(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
    normalized_provider: str,
    requested_exchange: str,
    symbols: set[str] | None,
) -> dict[str, object]:
    """Persist strong positive exposure without inferring absence from a partial scan."""
    identity_exchange = _position_sync_identity_exchange(
        provider=normalized_provider,
        requested_exchange=requested_exchange,
    )
    empty_result: dict[str, object] = {
        "tracked_symbol_count": 0,
        "superseded_count": 0,
        "positive_holding_count": 0,
        "execution_scope": {
            "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
            "provider": normalized_provider,
            "account_scope": None,
        },
        "execution_scope_source": "configured_account_route",
    }
    if normalized_provider != "kis" or identity_exchange != "KRX":
        return empty_result
    identity = build_position_identity(
        settings=settings,
        provider=normalized_provider,
        exchange=identity_exchange,
        broker_account_id=snapshot.broker_account_id,
    )
    execution_scope = {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
    }
    routed_holdings = _position_sync_holdings_for_route(
        holdings=snapshot.holdings,
        provider=normalized_provider,
        exchange=identity_exchange,
        observed_at=snapshot.observed_at,
    )
    positive_holdings_by_symbol: dict[str, BrokerAccountHolding] = {}
    for holding in routed_holdings:
        holding_base_quantity = _holding_base_quantity(holding.payload)
        if (
            holding_base_quantity is not None
            and holding_base_quantity > 0
        ) or (
            holding_base_quantity is None
            and holding.quantity > 0
        ):
            positive_holdings_by_symbol[holding.symbol] = holding
    positive_open_order_symbols = {
        str(order.symbol or "").strip().upper()
        for order in snapshot.open_orders
        if str(order.symbol or "").strip()
        and (
            order.remaining_quantity > 0
            if order.remaining_quantity is not None
            else order.quantity > 0
        )
    }
    exposure_symbols = set(positive_holdings_by_symbol) | positive_open_order_symbols
    if symbols is not None:
        exposure_symbols &= symbols
    if not exposure_symbols:
        return {
            **empty_result,
            "execution_scope": execution_scope,
            "execution_scope_source": (
                "authenticated_broker_account"
                if snapshot.broker_account_id
                else "configured_account_route"
            ),
        }
    holdings_by_symbol = {
        symbol: holding
        for symbol, holding in positive_holdings_by_symbol.items()
        if symbol in exposure_symbols
    }
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol.in_(sorted(exposure_symbols)),
        )
        .order_by(ExecutionPosition.symbol.asc(), ExecutionPosition.id.asc())
        .with_for_update()
        .execution_options(populate_existing=True)
    ).all()
    positions_by_symbol: dict[str, list[ExecutionPosition]] = {}
    for position in positions:
        positions_by_symbol.setdefault(position.symbol, []).append(position)

    superseded_count = 0
    positive_holding_count = 0
    for symbol in sorted(exposure_symbols):
        canonical_matches: list[ExecutionPosition] = []
        for position in positions_by_symbol.get(symbol, []):
            lineage = load_kis_krx_flat_proof_lineage(
                settings=settings,
                db=db,
                position_id=position.id,
                symbol=symbol,
                identity=identity,
                for_update=True,
            )
            if lineage is None:
                continue
            latest_event, latest_summary = lineage
            proof_observed_at = kis_krx_aware_iso_datetime(
                latest_summary.get("broker_snapshot_observed_at")
            )
            proof_occurred_at = kis_krx_event_datetime(latest_event.occurred_at)
            if (
                proof_observed_at is None
                or proof_occurred_at is None
                or _utc_naive(snapshot.observed_at)
                <= max(
                    _utc_naive(proof_observed_at),
                    _utc_naive(proof_occurred_at),
                )
            ):
                continue
            if canonical_flat_row_matches_latest_proof(
                settings=settings,
                position=position,
                symbol=symbol,
                identity=identity,
                latest_summary=latest_summary,
            ):
                canonical_matches.append(position)
        if len(canonical_matches) != 1:
            continue
        position = canonical_matches[0]
        holding = holdings_by_symbol.get(symbol)
        if holding is not None:
            holding_base_quantity = _holding_base_quantity(holding.payload)
            position.quantity = (
                0 if holding_base_quantity is not None else holding.quantity
            )
            position.base_quantity = holding_base_quantity
            position.average_price = holding.average_price
            position.updated_from = "reconciliation"
            position.last_reconciled_at = snapshot.observed_at
            positive_holding_count += 1
        position.raw_payload = _position_snapshot_payload(
            snapshot_payload=snapshot.payload,
            holding=holding,
            exchange=identity_exchange,
            identity=identity,
            prior_payload=position.raw_payload,
        )
        superseded_count += 1
    return {
        "tracked_symbol_count": len(exposure_symbols),
        "superseded_count": superseded_count,
        "positive_holding_count": positive_holding_count,
        "execution_scope": execution_scope,
        "execution_scope_source": (
            "authenticated_broker_account"
            if snapshot.broker_account_id
            else "configured_account_route"
        ),
    }


def _preserve_fresh_kis_krx_flat_proof_during_sync(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    snapshot: BrokerAccountSnapshot,
    position: ExecutionPosition,
    identity: PositionIdentity,
    symbol: str,
    exchange: str,
    evaluated_at: datetime,
) -> bool:
    """Keep a still-consumable proof through a redundant complete flat sync.

    Once the proof freshness window expires, or the new snapshot cannot prove
    exact target absence, normal reconciliation intentionally supersedes it.
    The consumer then fails closed until the audited refresh flow re-proves it.
    """
    if not (
        str(adapter.provider or "").strip().lower() == "kis"
        and exchange == "KRX"
        and snapshot.source == "kis:account_snapshot"
        and snapshot.holdings_complete is True
        and snapshot.open_orders_complete is True
        and generic_sync_snapshot_payload_proves_symbol_absent(
            raw=(snapshot.payload if isinstance(snapshot.payload, dict) else {}),
            symbol=symbol,
        )
        and all(
            str(getattr(row, "symbol", "") or "").strip().upper() != symbol
            for row in (*snapshot.holdings, *snapshot.open_orders)
        )
        and kis_krx_flat_proof_timestamp_is_fresh(
            observed_at=snapshot.observed_at,
            evaluated_at=evaluated_at,
            settings=settings,
        )
    ):
        return False
    lineage = load_kis_krx_flat_proof_lineage(
        settings=settings,
        db=db,
        position_id=position.id,
        symbol=symbol,
        identity=identity,
        for_update=True,
    )
    if lineage is None:
        return False
    latest_event, latest_summary = lineage
    proof_observed_at = kis_krx_aware_iso_datetime(
        latest_summary.get("broker_snapshot_observed_at")
    )
    proof_occurred_at = kis_krx_event_datetime(latest_event.occurred_at)
    return bool(
        canonical_flat_row_matches_latest_proof(
            settings=settings,
            position=position,
            symbol=symbol,
            identity=identity,
            latest_summary=latest_summary,
        )
        and kis_krx_flat_proof_timestamp_is_fresh(
            observed_at=proof_observed_at,
            evaluated_at=evaluated_at,
            settings=settings,
        )
        and kis_krx_flat_proof_timestamp_is_fresh(
            observed_at=proof_occurred_at,
            evaluated_at=evaluated_at,
            settings=settings,
        )
    )


def _position_sync_identity_exchange(
    *,
    provider: str,
    requested_exchange: str,
) -> str:
    normalized_exchange = str(requested_exchange or "").strip().upper()
    if provider == "kis" and normalized_exchange in {
        "KRX",
        "KOSPI",
        "KOSDAQ",
        "NXT",
        "SOR",
    }:
        return "KRX"
    return normalized_exchange


def _position_sync_holdings_for_route(
    *,
    holdings: list[BrokerAccountHolding],
    provider: str,
    exchange: str,
    observed_at: datetime,
) -> list[BrokerAccountHolding]:
    """Keep holdings on their proven route and reject ambiguous position truth."""

    routed = list(holdings)
    if provider == "kis" and exchange in {"NASD", "NYSE", "AMEX"}:
        routed = []
        for holding in holdings:
            reported_exchange = _holding_reported_exchange(holding.payload)
            if reported_exchange is None:
                raise BrokerAdapterError(
                    "KIS US holding lacks broker exchange proof; position sync blocked",
                    status_code=502,
                    classification=BrokerErrorClassification.UNKNOWN,
                    response_payload={
                        "requested_exchange": exchange,
                        "holding_symbol": holding.symbol,
                    },
                    observed_at=observed_at,
                )
            if reported_exchange not in {"NASD", "NYSE", "AMEX"}:
                raise BrokerAdapterError(
                    "KIS US holding reports an unsupported exchange; position sync blocked",
                    status_code=502,
                    classification=BrokerErrorClassification.UNKNOWN,
                    response_payload={
                        "requested_exchange": exchange,
                        "reported_exchange": reported_exchange,
                        "holding_symbol": holding.symbol,
                    },
                    observed_at=observed_at,
                )
            if reported_exchange == exchange:
                routed.append(holding)
    return _dedupe_position_sync_holdings(
        holdings=routed,
        provider=provider,
        exchange=exchange,
        observed_at=observed_at,
    )


def _dedupe_position_sync_holdings(
    *,
    holdings: list[BrokerAccountHolding],
    provider: str,
    exchange: str,
    observed_at: datetime,
) -> list[BrokerAccountHolding]:
    holdings_by_symbol: dict[str, BrokerAccountHolding] = {}
    for holding in holdings:
        normalized_symbol = str(holding.symbol or "").strip().upper()
        normalized_holding = replace(holding, symbol=normalized_symbol)
        existing = holdings_by_symbol.get(normalized_symbol)
        if existing is None:
            holdings_by_symbol[normalized_symbol] = normalized_holding
            continue
        existing_values = _position_sync_holding_values(existing)
        incoming_values = _position_sync_holding_values(normalized_holding)
        conflicting_fields = sorted(
            field
            for field, existing_value in existing_values.items()
            if existing_value != incoming_values[field]
        )
        if conflicting_fields:
            raise BrokerAdapterError(
                "conflicting duplicate broker holding; position sync blocked",
                status_code=502,
                classification=BrokerErrorClassification.UNKNOWN,
                response_payload={
                    "provider": provider,
                    "exchange": exchange,
                    "holding_symbol": normalized_symbol,
                    "conflicting_fields": conflicting_fields,
                },
                observed_at=observed_at,
            )
    return list(holdings_by_symbol.values())


def _position_sync_holding_values(
    holding: BrokerAccountHolding,
) -> dict[str, object]:
    return {
        "quantity": _normalized_position_holding_decimal(holding.quantity),
        "base_quantity": _position_holding_payload_decimal(
            holding.payload,
            ("base_quantity",),
        ),
        "average_price": _normalized_position_holding_decimal(
            holding.average_price
        ),
        "sellable_quantity": _normalized_position_holding_decimal(
            holding.sellable_quantity
        ),
        "unrealized_pnl": _normalized_position_holding_decimal(
            holding.unrealized_pnl
        ),
        "unrealized_pnl_pct": _normalized_position_holding_decimal(
            holding.unrealized_pnl_pct
        ),
        "valuation": _position_holding_payload_decimal(
            holding.payload,
            ("ovrs_stck_evlu_amt", "market_value", "evaluation_amount"),
        ),
    }


def _normalized_position_holding_decimal(value: object) -> object:
    if value is None:
        return None
    parsed = _parse_decimal_candidate(value)
    if parsed is None or not parsed.is_finite():
        # A malformed numeric value cannot prove duplicate equivalence.
        return object()
    return parsed.normalize()


def _position_holding_payload_decimal(
    payload: object,
    keys: tuple[str, ...],
) -> object:
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key not in payload or payload.get(key) is None:
            continue
        return _normalized_position_holding_decimal(payload.get(key))
    return None


def _lock_execution_position_rows(
    *,
    db: Session,
    mode: str,
    symbols: set[str],
    identity: PositionIdentity,
    holdings_by_symbol: dict[str, BrokerAccountHolding] | None = None,
) -> tuple[dict[str, ExecutionPosition], set[str]]:
    created_symbols: set[str] = set()
    dialect_name = db.get_bind().dialect.name
    for symbol in sorted(symbols):
        existing_rows = db.scalars(
            select(ExecutionPosition)
            .where(
                ExecutionPosition.mode == mode,
                ExecutionPosition.symbol == symbol,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).all()
        rows_by_identity: dict[PositionIdentity, ExecutionPosition] = {}
        unscoped_rows: list[ExecutionPosition] = []
        for position in existing_rows:
            column_identity = _position_column_identity(position)
            stored_identity = position_identity_from_payload(position.raw_payload)
            if column_identity is None and stored_identity is None:
                unscoped_rows.append(position)
                continue
            resolved_identity = column_identity or stored_identity
            assert resolved_identity is not None
            if column_identity is not None and stored_identity not in {
                None,
                column_identity,
            }:
                raise PositionIdentityConflictError(
                    "execution position columns conflict with stored identity"
                )
            if resolved_identity in rows_by_identity:
                raise PositionIdentityConflictError(
                    "execution position one-row-per-route-symbol invariant is violated"
                )
            rows_by_identity[resolved_identity] = position
            if column_identity is None:
                _bind_position_route(position=position, identity=resolved_identity)
            if stored_identity is None:
                position.raw_payload = payload_with_position_identity(
                    position.raw_payload,
                    identity=resolved_identity,
                )

        if len(unscoped_rows) > 1:
            raise PositionIdentityConflictError(
                "execution position legacy one-row-per-symbol invariant is violated"
            )
        if unscoped_rows and identity in rows_by_identity:
            raise PositionIdentityConflictError(
                "parallel legacy position requires explicit account cleanup"
            )
        if identity not in rows_by_identity and unscoped_rows:
            legacy_position = unscoped_rows[0]
            if not _legacy_position_is_empty_and_safe_to_bind(
                position=legacy_position,
            ):
                raise PositionIdentityConflictError(
                    "legacy non-flat position requires explicit account mapping"
                )
            _bind_position_route(position=legacy_position, identity=identity)
            legacy_position.raw_payload = payload_with_position_identity(
                legacy_position.raw_payload,
                identity=identity,
            )
            rows_by_identity[identity] = legacy_position

        if identity in rows_by_identity:
            continue

        values = {
            "id": new_id(),
            "mode": mode,
            "provider": identity.provider,
            "account_scope": identity.account_scope,
            "exchange": identity.exchange,
            "symbol": symbol,
            "quantity": 0,
            "base_quantity": Decimal("0"),
            "updated_from": "reconciliation_pending",
            "raw_payload": payload_with_position_identity(
                None,
                identity=identity,
            ),
        }
        if dialect_name == "sqlite":
            statement = sqlite_insert(ExecutionPosition).values(**values)
            result = db.execute(
                statement.on_conflict_do_nothing(
                    index_elements=[
                        ExecutionPosition.mode,
                        ExecutionPosition.provider,
                        ExecutionPosition.account_scope,
                        ExecutionPosition.exchange,
                        ExecutionPosition.symbol,
                    ]
                )
            )
        elif dialect_name == "postgresql":
            statement = postgresql_insert(ExecutionPosition).values(**values)
            result = db.execute(
                statement.on_conflict_do_nothing(
                    index_elements=[
                        ExecutionPosition.mode,
                        ExecutionPosition.provider,
                        ExecutionPosition.account_scope,
                        ExecutionPosition.exchange,
                        ExecutionPosition.symbol,
                    ]
                )
            )
        else:
            raise RuntimeError(
                "position reconciliation requires SQLite or PostgreSQL locking support"
            )
        if result.rowcount == 1:
            created_symbols.add(symbol)

    # Route-column backfills must be visible to the exact scoped query below.
    db.flush()
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == mode,
            ExecutionPosition.provider == identity.provider,
            ExecutionPosition.account_scope == identity.account_scope,
            ExecutionPosition.exchange == identity.exchange,
            ExecutionPosition.symbol.in_(sorted(symbols)),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    ).all()
    position_by_symbol: dict[str, ExecutionPosition] = {}
    for position in positions:
        if position.symbol not in symbols or position.symbol in position_by_symbol:
            raise PositionIdentityConflictError(
                "execution position one-row-per-route-symbol invariant is violated"
            )
        position_by_symbol[position.symbol] = position
    if set(position_by_symbol) != symbols:
        raise PositionIdentityConflictError(
            "execution position one-row-per-route-symbol invariant is violated"
        )
    return position_by_symbol, created_symbols


def _position_column_identity(
    position: ExecutionPosition,
) -> PositionIdentity | None:
    return position_identity_from_columns(position)


def _bind_position_route(
    *,
    position: ExecutionPosition,
    identity: PositionIdentity,
) -> None:
    position.provider = identity.provider
    position.account_scope = identity.account_scope
    position.exchange = identity.exchange


def _holding_base_quantity(payload: object) -> Decimal | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("base_quantity")
    parsed = _parse_decimal(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def _snapshot_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return None
    return _json_safe_dict(payload)


def _position_snapshot_payload(
    *,
    snapshot_payload: dict[str, Any] | None,
    holding: BrokerAccountHolding | None,
    exchange: str,
    identity: PositionIdentity,
    prior_payload: dict[str, Any] | None = None,
    monitoring_recovery_evidence: dict[str, object] | None = None,
) -> dict[str, Any] | None:
    payload = _snapshot_payload(snapshot_payload)
    if payload is None:
        payload = {}
    else:
        payload = dict(payload)
    normalized_exchange = str(exchange or "").strip().upper()
    payload = payload_with_position_identity(payload, identity=identity)
    prior_sync = (
        prior_payload.get("_position_sync")
        if isinstance(prior_payload, dict)
        else None
    )
    if holding is None:
        if (
            normalized_exchange in {"NASD", "NYSE", "AMEX"}
            and isinstance(prior_sync, dict)
            and prior_sync.get("monitoring_eligible") is False
            and prior_sync.get("manual_review_required") is True
        ):
            payload["_position_sync"] = dict(prior_sync)
        return payload
    if normalized_exchange not in {"NASD", "NYSE", "AMEX"}:
        return payload

    reported_exchange = _holding_reported_exchange(holding.payload) or normalized_exchange
    monitoring_eligible = (
        reported_exchange in {"NASD", "NYSE", "AMEX"}
        and reported_exchange == normalized_exchange
        and holding.average_price is not None
        and holding.average_price > 0
    )
    position_sync: dict[str, Any] = {
        "asset_class": "us_equity",
        "exchange": reported_exchange,
        "monitoring_eligible": monitoring_eligible,
    }
    if not monitoring_eligible:
        position_sync["manual_review_reason"] = (
            "unpriced_overseas_holding"
            if reported_exchange in {"NASD", "NYSE", "AMEX"}
            else "unsupported_overseas_exchange"
        )
    if (
        isinstance(prior_sync, dict)
        and prior_sync.get("monitoring_eligible") is False
        and prior_sync.get("manual_review_required") is True
        and monitoring_recovery_evidence is None
    ):
        position_sync["monitoring_eligible"] = False
        for key in (
            "trade_eligible",
            "manual_review_required",
            "manual_review_reason",
            "manual_review_recorded_at",
            "route_disposition",
        ):
            if key in prior_sync:
                position_sync[key] = prior_sync[key]
    payload["_position_sync"] = position_sync
    return payload


def _unpriced_kis_us_holding_recovery_evidence(
    *,
    settings: Settings,
    snapshot: object,
    holding: BrokerAccountHolding,
    exchange: str,
    prior_payload: dict[str, Any] | None,
    evaluated_at: datetime,
) -> dict[str, object] | None:
    prior_sync = (
        prior_payload.get("_position_sync")
        if isinstance(prior_payload, dict)
        else None
    )
    if not isinstance(prior_sync, dict) or not (
        prior_sync.get("asset_class") == "us_equity"
        and str(prior_sync.get("exchange") or "").strip().upper()
        == str(exchange or "").strip().upper()
        and prior_sync.get("monitoring_eligible") is False
        and prior_sync.get("manual_review_required") is True
        and prior_sync.get("manual_review_reason") == "unpriced_overseas_holding"
    ):
        return None
    if (
        prior_sync.get("trade_eligible") is False
        or prior_sync.get("route_disposition") is not None
    ):
        return None
    normalized_exchange = str(exchange or "").strip().upper()
    if normalized_exchange not in {"NASD", "NYSE", "AMEX"}:
        return None
    if (
        getattr(snapshot, "source", None) != "kis:us_equity_account_snapshot"
        or getattr(snapshot, "holdings_complete", False) is not True
        or getattr(snapshot, "open_orders_complete", False) is not True
    ):
        return None
    snapshot_exchange = str(
        (getattr(snapshot, "payload", None) or {}).get("exchange") or ""
    ).strip().upper()
    if snapshot_exchange != normalized_exchange:
        return None
    snapshot_observed_at = getattr(snapshot, "observed_at", None)
    if not isinstance(snapshot_observed_at, datetime) or not snapshot_is_fresh_for_recovery(
        observed_at=snapshot_observed_at,
        now=evaluated_at,
        settings=settings,
    ):
        return None
    missing_latch_time = datetime.min.replace(tzinfo=UTC)
    manual_review_recorded_at = _parse_timestamp(
        prior_sync.get("manual_review_recorded_at"),
        fallback=missing_latch_time,
    )
    if (
        manual_review_recorded_at == missing_latch_time
        or _utc_naive(snapshot_observed_at)
        <= _utc_naive(manual_review_recorded_at)
    ):
        return None
    reported_exchange = _holding_reported_exchange(holding.payload)
    average_price = _parse_decimal(holding.average_price)
    if (
        reported_exchange != normalized_exchange
        or holding.quantity <= 0
        or average_price is None
        or average_price <= 0
    ):
        return None
    if (
        holding.sellable_quantity is None
        or holding.sellable_quantity <= 0
        or holding.sellable_quantity > holding.quantity
    ):
        return None
    holding_payload = holding.payload if isinstance(holding.payload, dict) else {}
    current_price = _parse_decimal(holding_payload.get("now_pric2"))
    if current_price is None or current_price <= 0:
        return None
    for field in ("now_pric", "current_price", "prpr", "last_price"):
        if field not in holding_payload:
            continue
        corroborating_price = _parse_decimal(holding_payload[field])
        if corroborating_price is None or corroborating_price != current_price:
            return None
    return {
        "symbol": holding.symbol,
        "exchange": normalized_exchange,
        "snapshot_source": str(snapshot.source),
        "snapshot_observed_at": snapshot_observed_at.isoformat(),
        "holdings_complete": True,
        "open_orders_complete": True,
        "holding_quantity": holding.quantity,
        "holding_average_price": str(average_price),
        "current_price": str(current_price),
        "current_price_field": "now_pric2",
        "sellable_quantity": holding.sellable_quantity,
        "prior_manual_review_reason": prior_sync.get("manual_review_reason"),
        "manual_review_recorded_at": prior_sync.get("manual_review_recorded_at"),
    }


def _position_belongs_to_sync_scope(
    *,
    position: ExecutionPosition,
    identity: PositionIdentity,
) -> bool:
    column_identity = _position_column_identity(position)
    stored_identity = position_identity_from_payload(position.raw_payload)
    if (
        column_identity is not None
        and stored_identity is not None
        and column_identity != stored_identity
    ):
        raise PositionIdentityConflictError(
            "execution position columns conflict with stored identity"
        )
    resolved_identity = column_identity or stored_identity
    return resolved_identity is None or resolved_identity == identity


def _legacy_position_is_empty_and_safe_to_bind(
    *,
    position: ExecutionPosition,
) -> bool:
    """Bind only an unused placeholder; broker-value equality proves no account identity."""
    return (
        position.quantity == 0
        and (position.base_quantity is None or position.base_quantity == 0)
        and position.average_price is None
        and position.last_reconciled_at is None
        and not position.raw_payload
        and position.updated_from in {"bootstrap", "reconciliation_pending"}
    )


def _holding_reported_exchange(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ovrs_excg_cd", "OVRS_EXCG_CD", "exchange", "excg_cd"):
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        normalized = value.strip().upper()
        return {
            "NAS": "NASD",
            "NASDAQ": "NASD",
            "NYS": "NYSE",
            "AMS": "AMEX",
        }.get(normalized, normalized)
    return None


def _reconcilable_order_statuses() -> tuple[str, ...]:
    return NONTERMINAL_ORDER_STATUSES


def _normalize_symbols(symbols: set[str] | None) -> set[str] | None:
    if symbols is None:
        return None
    return {
        normalized
        for symbol in symbols
        if (normalized := str(symbol or "").strip().upper())
    }


def _reconcile_adapter_error(
    *,
    exc: BaseException,
    order: BrokerOrder,
    adapter: BrokerAdapter,
    requested_broker_order_id: str,
    event_time: datetime,
    db: Session,
    order_status_before: str,
    actor: str | None = None,
    operator_reason: str | None = None,
) -> dict[str, object]:
    if isinstance(exc, BrokerAdapterError):
        message = safe_broker_error_message(exc)
        status_code = exc.status_code or 503
        safe_error_code = safe_broker_error_code(
            exc.code,
            provider=adapter.provider,
        )
        safe_request_metadata = _safe_payload(exc.request_metadata)
        safe_response_metadata = _safe_broker_error_payload(exc.response_metadata)
        safe_request_payload = _safe_payload(exc.request_payload)
        safe_response_payload = _safe_broker_error_payload(exc.response_payload)
        order.last_error_classification = exc.classification.value
        order.last_error_code = safe_error_code
        order.last_error_message = message
        order.last_error_retryable = exc.retryable
        order.last_broker_sync_at = exc.observed_at or event_time
        _apply_order_metadata(
            order=order,
            payload={
                "provider": adapter.provider,
                "broker_order_id": requested_broker_order_id,
                "status": None,
                "broker_status": safe_error_code,
                "observed_at": _serialize_datetime(order.last_broker_sync_at),
                "request_metadata": safe_request_metadata,
                "response_metadata": safe_response_metadata,
                "request_payload": safe_request_payload,
                "payload": safe_response_payload,
                "message": message,
            },
        )
        if _should_record_reconcile_failure_event(
            db=db,
            order=order,
            message=message,
            broker_status=safe_error_code,
            error_classification=exc.classification.value,
            observed_at=order.last_broker_sync_at,
        ):
            event_payload = _build_reconcile_event_payload(
                payload={
                    "provider": adapter.provider,
                    "broker_order_id": requested_broker_order_id,
                    "status": None,
                    "broker_status": safe_error_code,
                    "observed_at": _serialize_datetime(order.last_broker_sync_at),
                    "message": message,
                    "request_metadata": safe_request_metadata,
                    "response_metadata": safe_response_metadata,
                    "request_payload": safe_request_payload,
                    "payload": safe_response_payload,
                },
                adapter=adapter,
            )
            if actor is not None or operator_reason is not None:
                event_payload["actor"] = actor
                event_payload["operator_reason"] = operator_reason
            db.add(
                BrokerOrderEvent(
                    broker_order_pk=order.id,
                    event_type="reconcile_failed",
                    event_source=BrokerEventSource.RECONCILIATION.value,
                    normalized_status=order.status,
                    broker_status=safe_error_code,
                    error_classification=exc.classification.value,
                    retryable=exc.retryable,
                    message=message,
                    request_payload=safe_request_payload,
                    response_payload=safe_response_payload,
                    payload=event_payload,
                    observed_at=order.last_broker_sync_at,
                )
            )
        db.commit()
        db.refresh(order)
        return {
            "status_code": status_code,
            "broker_order_id": order.broker_order_id,
            "status": order.status,
            "status_before": order_status_before,
            "status_changed": False,
            "fills_added": 0,
            "filled_delta": 0,
            "message": message,
        }

    observed_at = _observation_time()
    message = safe_broker_error_message(exc)
    order.last_error_classification = "unknown"
    order.last_error_code = None
    order.last_error_message = message
    order.last_error_retryable = False
    order.last_broker_sync_at = observed_at
    _apply_order_metadata(
        order=order,
        payload={
            "provider": adapter.provider,
            "broker_order_id": requested_broker_order_id,
            "status": None,
            "broker_status": None,
            "observed_at": _serialize_datetime(observed_at),
            "message": message,
        },
    )
    if _should_record_reconcile_failure_event(
        db=db,
        order=order,
        message=message,
        broker_status=None,
        error_classification="unknown",
        observed_at=observed_at,
    ):
        event_payload = _build_reconcile_event_payload(
            payload={
                "provider": adapter.provider,
                "broker_order_id": requested_broker_order_id,
                "status": None,
                "broker_status": None,
                "observed_at": _serialize_datetime(observed_at),
                "message": message,
            },
            adapter=adapter,
        )
        if actor is not None or operator_reason is not None:
            event_payload["actor"] = actor
            event_payload["operator_reason"] = operator_reason
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="reconcile_failed",
                event_source=BrokerEventSource.RECONCILIATION.value,
                normalized_status=order.status,
                error_classification="unknown",
                message=message,
                payload=event_payload,
                observed_at=observed_at,
            )
        )
    db.commit()
    db.refresh(order)
    return {
        "status_code": 503,
        "broker_order_id": order.broker_order_id,
        "status": order.status,
        "status_before": order_status_before,
        "status_changed": False,
        "fills_added": 0,
        "filled_delta": 0,
        "message": message,
    }


def _should_record_reconcile_failure_event(
    *,
    db: Session,
    order: BrokerOrder,
    message: str,
    broker_status: str | None,
    error_classification: str,
    observed_at: datetime,
) -> bool:
    latest_failure = db.scalar(
        select(BrokerOrderEvent)
        .where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "reconcile_failed",
        )
        .order_by(BrokerOrderEvent.observed_at.desc(), BrokerOrderEvent.created_at.desc())
        .limit(1)
    )
    if latest_failure is None:
        return True
    if (
        latest_failure.message != message
        or latest_failure.broker_status != broker_status
        or latest_failure.error_classification != error_classification
    ):
        return True
    return _utc_naive(observed_at) - _utc_naive(latest_failure.observed_at) >= timedelta(minutes=5)


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _parse_timestamp(value: object, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        for candidate in (value, value.replace(" ", "T"), value.replace("Z", "+00:00")):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y%m%d%H%M%S%z",
                "%Y%m%d%H%M%S",
            ):
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    if parsed.tzinfo is None:
                        return parsed.replace(tzinfo=UTC)
                    return parsed
                except (ValueError, TypeError):
                    pass
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed
        except ValueError:
            pass
    return fallback


def _required_evidence_timestamp(value: object) -> datetime | None:
    missing = datetime.min.replace(tzinfo=UTC)
    parsed = _parse_timestamp(value, fallback=missing)
    return None if parsed == missing else parsed


def _observation_time() -> datetime:
    return datetime.now(UTC)


def _normalize_status(raw_status: object) -> str | None:
    if not isinstance(raw_status, str):
        return None
    normalized = raw_status.strip().lower().replace("-", "_").replace(" ", "_")
    status_lookup: dict[str, str] = {
        "submitted": NormalizedBrokerOrderStatus.SUBMITTED.value,
        "open": NormalizedBrokerOrderStatus.OPEN.value,
        "partially_filled": NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        "partial": NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        "partially filled": NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        "filled": NormalizedBrokerOrderStatus.FILLED.value,
        "filled/closed": NormalizedBrokerOrderStatus.FILLED.value,
        "cancel_pending": NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
        "cancel_pending_pending": NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
        "canceled": NormalizedBrokerOrderStatus.CANCELED.value,
        "cancelled": NormalizedBrokerOrderStatus.CANCELED.value,
        "retry_pending": NormalizedBrokerOrderStatus.RETRY_PENDING.value,
        "rejected": NormalizedBrokerOrderStatus.REJECTED.value,
        "failed": NormalizedBrokerOrderStatus.FAILED.value,
        "expired": NormalizedBrokerOrderStatus.EXPIRED.value,
        "미체결": NormalizedBrokerOrderStatus.OPEN.value,
        "부분체결": NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        "체결": NormalizedBrokerOrderStatus.FILLED.value,
        "취소": NormalizedBrokerOrderStatus.CANCELED.value,
        "거부": NormalizedBrokerOrderStatus.REJECTED.value,
        "실패": NormalizedBrokerOrderStatus.FAILED.value,
        "만료": NormalizedBrokerOrderStatus.EXPIRED.value,
    }
    for key in status_lookup:
        if normalized == key:
            return status_lookup[key]
    return None


def _build_reconcile_event_payload(
    *,
    payload: dict[str, Any],
    adapter: BrokerAdapter,
) -> dict[str, object]:
    event_payload: dict[str, object] = {
        "provider": adapter.provider,
        "event_source": BrokerEventSource.RECONCILIATION.value,
        "observed_at": payload.get("observed_at"),
        "status": payload.get("status"),
        "broker_status": payload.get("broker_status"),
        "normalized_payload": _safe_payload(payload.get("payload")),
        "response_metadata": _dict_or_none(payload.get("response_metadata"))
        or _dict_or_none(payload.get("response_payload")),
        "request_metadata": _dict_or_none(payload.get("request_metadata")),
    }
    for key in (
        "requested_quantity",
        "filled_quantity",
        "remaining_quantity",
        "filled_base_quantity",
    ):
        if payload.get(key) is not None:
            event_payload[key] = _json_safe_value(payload[key])
    return event_payload


def _raw_status_details(payload: dict[str, Any]) -> dict[str, object]:
    return {
        "status": _optional_str(payload.get("status")),
        "broker_status": _optional_str(payload.get("broker_status")),
        "message": _optional_str(payload.get("message")),
        "observed_at": _optional_str(payload.get("observed_at")),
    }


def _safe_payload(payload: object) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        return _json_safe_dict(payload)
    return None


def _safe_broker_error_payload(payload: object) -> dict[str, Any] | None:
    sanitized = sanitize_broker_error_payload(payload)
    return dict(sanitized) if isinstance(sanitized, dict) else None


def _order_reconciliation_exchange(order: BrokerOrder) -> str:
    canonical_scope = fifo_route_scope_from_order(order)
    if canonical_scope is not None:
        return canonical_scope.exchange
    for payload in (order.request_metadata, order.response_metadata, order.raw_broker_payload):
        exchange = _exchange_from_payload(payload)
        if exchange is not None:
            return exchange
    if order.plan_item is not None:
        exchange = _exchange_from_payload(order.plan_item.risk_context)
        if exchange is not None:
            return exchange
    venue_exchange = _normalized_exchange(order.venue)
    if venue_exchange is not None:
        return venue_exchange
    if str(order.asset_class or "").strip().lower() == "us_equity":
        raise ValueError(
            "US-equity reconciliation requires an explicit NASD, NYSE, or AMEX exchange"
        )
    return "KRX"


def _exchange_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None

    route = payload.get("route")
    if isinstance(route, dict):
        exchange = _normalized_exchange(route.get("exchange"))
        if exchange is not None:
            return exchange

    params = payload.get("params")
    if isinstance(params, dict):
        exchange = _normalized_exchange(params.get("EXCG_ID_DVSN_CD"))
        if exchange is not None:
            return exchange

    exchange = _normalized_exchange(payload.get("exchange"))
    if exchange is not None:
        return exchange

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return _exchange_from_payload(metadata)
    return None


def _normalized_exchange(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if normalized == "NASDAQ":
        normalized = "NASD"
    if normalized in {
        "KRX",
        "NXT",
        "SOR",
        "UPBIT",
        "ALPACA",
        "US",
        "NASD",
        "NYSE",
        "AMEX",
    }:
        return normalized
    return None


def _dict_or_none(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return _json_safe_dict(value)
    return None


def _parse_fills_from_payload(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
    observed_at: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    fill_time_metadata = _fill_time_metadata(payload)
    fills_payload = payload.get("fills")
    if isinstance(fills_payload, list) and fills_payload:
        result: list[dict[str, object]] = []
        anomalies: list[dict[str, object]] = []
        inherited_cost_scope = _transaction_cost_scope(payload)
        for fill_index, fill_item in enumerate(fills_payload):
            if not isinstance(fill_item, dict):
                continue
            integrity_reason = _fill_payload_integrity_reason(
                fill=fill_item,
                order=order,
            )
            if integrity_reason is not None:
                anomalies.append(
                    {
                        "reason": integrity_reason,
                        "fill_index": fill_index,
                        "broker_order_id": order.broker_order_id,
                        "symbol": order.symbol,
                        "side": order.side,
                    }
                )
                continue
            parsed_fill = _normalize_fill_payload(
                fill_item=fill_item,
                observed_at=observed_at,
                order=order,
                inherited_transaction_cost_scope=inherited_cost_scope,
            )
            if parsed_fill is None:
                continue
            parsed_fill.update(fill_time_metadata)
            result.append(parsed_fill)
        return result, anomalies

    existing_fills = dedupe_execution_fills(order.fills)
    total_filled_base = _parse_decimal(payload.get("filled_base_quantity"))
    if total_filled_base is not None and total_filled_base > 0:
        existing_total_base = sum(
            Decimal(fill.base_quantity or 0)
            for fill in existing_fills
        )
        average_fill_price = _parse_decimal(payload.get("average_fill_price"))
        delta_result = _cumulative_fill_delta(
            order=order,
            total_filled=total_filled_base,
            existing_total=existing_total_base,
            average_fill_price=average_fill_price,
            quantity_basis="base_quantity",
            existing_fills=existing_fills,
        )
        if delta_result["status"] == "noop":
            return _cumulative_existing_fill_revision_rows(
                payload=payload,
                order=order,
            ), []
        if delta_result["status"] != "ok":
            anomalies = delta_result["anomalies"]
            return [], anomalies if isinstance(anomalies, list) else []
        delta = _coerce_decimal(delta_result["delta"])
        incremental_notional = _coerce_decimal(delta_result["incremental_notional"])
        incremental_price = _coerce_decimal(delta_result["incremental_price"])
        filled_at = _parse_timestamp(payload.get("filled_at"), fallback=observed_at)
        broker_fill_id = _optional_str(payload.get("broker_fill_id")) or (
            f"{order.broker_order_id}:cumulative:{format(total_filled_base.normalize(), 'f')}"
        )
        economics = economics_payload(
            _cumulative_fill_economics(payload=payload, order=order)
        )
        return [{
            "broker_fill_id": broker_fill_id,
            "quantity": 0,
            "base_quantity": delta,
            "quote_notional": incremental_notional,
            "price": incremental_price,
            "filled_at": filled_at.isoformat(),
            "symbol": order.symbol,
            "side": order.side,
            "price_derivation": "cumulative_notional_delta",
            "cumulative_base_quantity": total_filled_base,
            "cumulative_average_fill_price": average_fill_price,
            "cumulative_quote_notional": delta_result["current_cumulative_notional"],
            "previous_base_quantity": existing_total_base,
            "previous_quote_notional": delta_result["previous_cumulative_notional"],
            **economics,
            **fill_time_metadata,
        }], []

    total_filled = _parse_int(payload.get("filled_quantity"))
    if total_filled is None or total_filled <= 0:
        return [], []

    existing_total = _sum_fill_quantities(existing_fills)
    average_fill_price = _parse_decimal(payload.get("average_fill_price"))
    delta_result = _cumulative_fill_delta(
        order=order,
        total_filled=Decimal(total_filled),
        existing_total=Decimal(existing_total),
        average_fill_price=average_fill_price,
        quantity_basis="quantity",
        existing_fills=existing_fills,
    )
    if delta_result["status"] == "noop":
        return _cumulative_existing_fill_revision_rows(
            payload=payload,
            order=order,
        ), []
    if delta_result["status"] != "ok":
        anomalies = delta_result["anomalies"]
        return [], anomalies if isinstance(anomalies, list) else []

    filled_at = _parse_timestamp(
        payload.get("filled_at"),
        fallback=observed_at,
    )
    economics = economics_payload(
        _cumulative_fill_economics(payload=payload, order=order)
    )
    return [
        {
            "broker_fill_id": _optional_str(payload.get("broker_fill_id")),
            "quantity": total_filled - existing_total,
            "quote_notional": delta_result["incremental_notional"],
            "price": delta_result["incremental_price"],
            "filled_at": filled_at.isoformat(),
            "symbol": order.symbol,
            "side": order.side,
            "price_derivation": "cumulative_notional_delta",
            "cumulative_quantity": total_filled,
            "cumulative_average_fill_price": average_fill_price,
            "cumulative_quote_notional": delta_result["current_cumulative_notional"],
            "previous_quantity": existing_total,
            "previous_quote_notional": delta_result["previous_cumulative_notional"],
            **economics,
            **fill_time_metadata,
        }
    ], []


def _fill_time_metadata(payload: dict[str, object]) -> dict[str, str]:
    precision = _optional_str(payload.get("fill_time_precision"))
    if precision not in {"exact", "observed_upper_bound"}:
        return {}
    source = _optional_str(payload.get("fill_time_source"))
    result = {"fill_time_precision": precision}
    if source is not None:
        result["fill_time_source"] = source
    return result


def _cumulative_fill_economics(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> FillEconomics:
    economics = normalize_fill_economics(payload, order=order)
    if economics.transaction_cost is None:
        return economics

    scope = _transaction_cost_scope(payload)
    if scope in {"incremental", "per_fill"}:
        return economics
    if scope != "cumulative":
        return FillEconomics(
            settlement_currency=economics.settlement_currency,
            transaction_cost=None,
            transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
            transaction_cost_source="broker_payload:ambiguous_cumulative_cost_scope",
            settlement_currency_conflict=economics.settlement_currency_conflict,
        )

    existing_cost = Decimal("0")
    for fill in order.fills:
        persisted_economics = execution_fill_economics(fill)
        if (
            persisted_economics.transaction_cost is None
            or persisted_economics.transaction_cost_coverage != COST_COVERAGE_COMPLETE
        ):
            return FillEconomics(
                settlement_currency=economics.settlement_currency,
                transaction_cost=None,
                transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
                transaction_cost_source="broker_payload:cumulative_cost_delta_unavailable",
                settlement_currency_conflict=economics.settlement_currency_conflict,
            )
        existing_cost += persisted_economics.transaction_cost
    incremental_cost = economics.transaction_cost - existing_cost
    if incremental_cost < 0:
        return FillEconomics(
            settlement_currency=economics.settlement_currency,
            transaction_cost=None,
            transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
            transaction_cost_source="broker_payload:cumulative_cost_regressed",
            settlement_currency_conflict=economics.settlement_currency_conflict,
        )
    return FillEconomics(
        settlement_currency=economics.settlement_currency,
        transaction_cost=incremental_cost,
        transaction_cost_coverage=economics.transaction_cost_coverage,
        transaction_cost_source="broker_payload:cumulative_cost_delta",
        settlement_currency_conflict=economics.settlement_currency_conflict,
    )


def _transaction_cost_scope(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    economics = payload.get("economics")
    if isinstance(economics, dict) and "transaction_cost_scope" in economics:
        raw_scope = economics.get("transaction_cost_scope")
    else:
        raw_scope = payload.get("transaction_cost_scope")
    return str(raw_scope or "").strip().lower()


def _transaction_cost_is_authoritative(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    economics = payload.get("economics")
    if isinstance(economics, dict) and "transaction_cost_authoritative" in economics:
        return economics.get("transaction_cost_authoritative") is True
    return payload.get("transaction_cost_authoritative") is True


def _cumulative_cost_revision_rows(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> list[dict[str, object]]:
    """Return an existing fill identity with a safely revised cumulative cost.

    Quantity no-op snapshots can still finalize broker costs. With multiple
    persisted fills, a revision is allocatable only when all prior costs are
    complete; the cumulative delta is assigned to the latest fill. A lone fill
    can safely receive the broker's full cumulative total.
    """

    economics = normalize_fill_economics(payload, order=order)
    if (
        _transaction_cost_scope(payload) != "cumulative"
        or economics.transaction_cost is None
        or economics.transaction_cost_coverage != COST_COVERAGE_COMPLETE
        or not order.fills
    ):
        return []

    fills = sorted(
        order.fills,
        key=lambda fill: (
            _parse_timestamp(fill.filled_at, fallback=_observation_time()),
            str(fill.id or ""),
        ),
    )
    persisted_costs: list[Decimal] = []
    for fill in fills:
        persisted = execution_fill_economics(fill)
        if (
            persisted.transaction_cost is None
            or persisted.transaction_cost_coverage != COST_COVERAGE_COMPLETE
        ):
            return []
        persisted_costs.append(persisted.transaction_cost)

    existing_total = sum(persisted_costs, Decimal("0"))
    target_total = economics.transaction_cost
    revised_costs = list(persisted_costs)
    source = "broker_payload:cumulative_cost_revision"
    if target_total < existing_total:
        if (
            not _transaction_cost_is_authoritative(payload)
            or _normalize_status(payload.get("status"))
            != NormalizedBrokerOrderStatus.FILLED.value
        ):
            return []
        # Final settlement may correct a provisional fee downward. The broker only
        # gives a cumulative total, so reduce newest fills first while preserving
        # non-negative per-fill costs and the exact authoritative total.
        remaining_reduction = existing_total - target_total
        for index in range(len(revised_costs) - 1, -1, -1):
            reduction = min(revised_costs[index], remaining_reduction)
            revised_costs[index] -= reduction
            remaining_reduction -= reduction
            if remaining_reduction == 0:
                break
        if remaining_reduction != 0:
            return []
        source = "broker_payload:authoritative_cumulative_cost_revision"
    else:
        revised_costs[-1] += target_total - existing_total

    rows: list[dict[str, object]] = []
    for fill, previous_cost, revised_cost in zip(
        fills,
        persisted_costs,
        revised_costs,
        strict=True,
    ):
        if revised_cost == previous_cost:
            continue
        rows.append(
            {
                "broker_fill_id": fill.broker_fill_id,
                "quantity": fill.quantity,
                "base_quantity": fill.base_quantity,
                "quote_notional": fill.quote_notional,
                "price": fill.price,
                "filled_at": _parse_timestamp(
                    fill.filled_at,
                    fallback=_observation_time(),
                ).isoformat(),
                "symbol": fill.symbol,
                "side": fill.side,
                **economics_payload(
                    FillEconomics(
                        settlement_currency=economics.settlement_currency,
                        transaction_cost=revised_cost,
                        transaction_cost_coverage=COST_COVERAGE_COMPLETE,
                        transaction_cost_source=source,
                        settlement_currency_conflict=(
                            economics.settlement_currency_conflict
                        ),
                    )
                ),
            }
        )
    return rows


def _cumulative_existing_fill_revision_rows(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> list[dict[str, object]]:
    """Enrich a cumulative no-op without inventing another execution fill."""

    rows = _cumulative_cost_revision_rows(payload=payload, order=order)
    time_metadata = _fill_time_metadata(payload)
    if time_metadata.get("fill_time_precision") != "observed_upper_bound":
        return rows

    rows_by_signature = {
        signature: row
        for row in rows
        if (signature := _fill_signature_candidate(order=order, fill=row))
        is not None
    }
    for fill in dedupe_execution_fills(order.fills):
        persisted_payload = (
            fill.payload if isinstance(fill.payload, dict) else {}
        )
        if not _is_cumulative_reconciliation_fill_payload(persisted_payload):
            continue
        persisted_precision = _optional_str(
            persisted_payload.get("fill_time_precision")
        )
        persisted_source = _optional_str(persisted_payload.get("fill_time_source"))
        incoming_source = time_metadata.get("fill_time_source")
        if persisted_precision == "exact" or (
            persisted_precision == "observed_upper_bound"
            and (persisted_source is not None or incoming_source is None)
        ):
            continue
        revision = {
            "broker_fill_id": fill.broker_fill_id,
            "quantity": fill.quantity,
            "base_quantity": fill.base_quantity,
            "quote_notional": fill.quote_notional,
            "price": fill.price,
            "filled_at": _parse_timestamp(
                fill.filled_at,
                fallback=_observation_time(),
            ).isoformat(),
            "symbol": fill.symbol,
            "side": fill.side,
            **time_metadata,
        }
        signature = _fill_signature_candidate(order=order, fill=revision)
        if signature is not None and signature in rows_by_signature:
            rows_by_signature[signature].update(time_metadata)
        else:
            rows.append(revision)
            if signature is not None:
                rows_by_signature[signature] = revision
    return rows


def _is_cumulative_reconciliation_fill_payload(payload: dict[str, object]) -> bool:
    return (
        payload.get("price_derivation") == "cumulative_notional_delta"
        or any(
            key in payload
            for key in (
                "cumulative_quantity",
                "cumulative_base_quantity",
                "cumulative_quote_notional",
            )
        )
        or _is_legacy_order_time_repair_payload(payload)
    )


def _is_legacy_order_time_repair_payload(payload: dict[str, object]) -> bool:
    """Treat every pre-contract operator repair as chronology-unproven."""

    return (
        payload.get("source") == "operator_fill_evidence_repair"
        and payload.get("fill_time_precision") != "exact"
    )


def _sum_fill_quantities(fills: list[ExecutionFill]) -> int:
    return sum(fill.quantity for fill in fills)


def _cumulative_fill_delta(
    *,
    order: BrokerOrder,
    total_filled: Decimal,
    existing_total: Decimal,
    average_fill_price: Decimal | None,
    quantity_basis: str,
    existing_fills: list[ExecutionFill],
) -> dict[str, object]:
    if total_filled < existing_total:
        return _cumulative_fill_delta_anomaly(
            reason="cumulative_quantity_regressed",
            total_filled=total_filled,
            existing_total=existing_total,
            average_fill_price=average_fill_price,
            quantity_basis=quantity_basis,
            order=order,
        )
    if total_filled == existing_total:
        return {"status": "noop", "anomalies": []}
    if average_fill_price is None:
        return _cumulative_fill_delta_anomaly(
            reason="missing_average_fill_price",
            total_filled=total_filled,
            existing_total=existing_total,
            average_fill_price=average_fill_price,
            quantity_basis=quantity_basis,
            order=order,
        )
    if average_fill_price <= 0:
        return _cumulative_fill_delta_anomaly(
            reason="non_positive_average_fill_price",
            total_filled=total_filled,
            existing_total=existing_total,
            average_fill_price=average_fill_price,
            quantity_basis=quantity_basis,
            order=order,
        )

    delta = total_filled - existing_total
    previous_cumulative_notional = _sum_existing_fill_notional(
        fills=existing_fills,
        quantity_basis=quantity_basis,
    )
    current_cumulative_notional = total_filled * average_fill_price
    incremental_notional = current_cumulative_notional - previous_cumulative_notional
    if incremental_notional <= 0:
        return _cumulative_fill_delta_anomaly(
            reason="non_positive_incremental_notional",
            total_filled=total_filled,
            existing_total=existing_total,
            average_fill_price=average_fill_price,
            quantity_basis=quantity_basis,
            order=order,
            current_cumulative_notional=current_cumulative_notional,
            previous_cumulative_notional=previous_cumulative_notional,
            incremental_notional=incremental_notional,
        )

    return {
        "status": "ok",
        "anomalies": [],
        "delta": delta,
        "current_cumulative_notional": current_cumulative_notional,
        "previous_cumulative_notional": previous_cumulative_notional,
        "incremental_notional": incremental_notional,
        "incremental_price": incremental_notional / delta,
    }


def _cumulative_fill_delta_anomaly(
    *,
    reason: str,
    total_filled: Decimal,
    existing_total: Decimal,
    average_fill_price: Decimal | None,
    quantity_basis: str,
    order: BrokerOrder,
    current_cumulative_notional: Decimal | None = None,
    previous_cumulative_notional: Decimal | None = None,
    incremental_notional: Decimal | None = None,
) -> dict[str, object]:
    anomaly = {
        "reason": reason,
        "broker_order_id": order.broker_order_id,
        "symbol": order.symbol,
        "side": order.side,
        "quantity_basis": quantity_basis,
        "total_filled": total_filled,
        "existing_total": existing_total,
        "average_fill_price": average_fill_price,
        "current_cumulative_notional": current_cumulative_notional,
        "previous_cumulative_notional": previous_cumulative_notional,
        "incremental_notional": incremental_notional,
    }
    return {"status": "anomaly", "anomalies": [anomaly]}


def _sum_existing_fill_notional(
    *,
    fills: list[ExecutionFill],
    quantity_basis: str,
) -> Decimal:
    total = Decimal("0")
    for fill in fills:
        if fill.quote_notional is not None and fill.quote_notional >= 0:
            total += Decimal(fill.quote_notional)
            continue
        quantity = _fill_notional_quantity(fill=fill, quantity_basis=quantity_basis)
        if quantity <= 0:
            continue
        total += quantity * Decimal(fill.price)
    return total


def _fill_notional_quantity(*, fill: ExecutionFill, quantity_basis: str) -> Decimal:
    if quantity_basis == "base_quantity":
        return Decimal(fill.base_quantity or 0)
    return Decimal(fill.quantity)


def _normalize_fill_payload(
    *,
    fill_item: dict[str, object],
    observed_at: datetime,
    order: BrokerOrder,
    inherited_transaction_cost_scope: str = "",
) -> dict[str, object] | None:
    quantity = _parse_int(fill_item.get("quantity")) or 0
    base_quantity = _parse_decimal(fill_item.get("base_quantity"))
    if quantity <= 0 and (base_quantity is None or base_quantity <= 0):
        return None
    price = _parse_decimal(fill_item.get("price"))
    if price is None:
        return None
    filled_at = _parse_timestamp(
        fill_item.get("filled_at"),
        fallback=observed_at,
    )
    economics = normalize_fill_economics(fill_item, order=order)
    cost_scope = _transaction_cost_scope(fill_item) or inherited_transaction_cost_scope
    if economics.transaction_cost is not None and cost_scope == "cumulative":
        economics = FillEconomics(
            settlement_currency=economics.settlement_currency,
            transaction_cost=None,
            transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
            transaction_cost_source=(
                "broker_payload:ambiguous_explicit_fill_cumulative_cost_scope"
            ),
            settlement_currency_conflict=economics.settlement_currency_conflict,
        )
    return {
        "broker_fill_id": _optional_str(fill_item.get("broker_fill_id")),
        "quantity": quantity,
        "base_quantity": base_quantity,
        "quote_notional": _parse_decimal(fill_item.get("quote_notional")),
        "price": price,
        "filled_at": filled_at.isoformat(),
        "symbol": _normalized_fill_symbol(order.symbol),
        "side": _normalized_fill_side(order.side),
        **economics_payload(economics),
    }


def _fill_payload_integrity_reason(
    *,
    fill: dict[str, object],
    order: BrokerOrder,
) -> str | None:
    order_symbol = _normalized_fill_symbol(order.symbol)
    fill_symbol = _optional_str(fill.get("symbol"))
    if fill_symbol is not None and _normalized_fill_symbol(fill_symbol) != order_symbol:
        return "fill_symbol_mismatch"

    order_side = _normalized_fill_side(order.side)
    fill_side = _optional_str(fill.get("side"))
    if fill_side is not None and _normalized_fill_side(fill_side) != order_side:
        return "fill_side_mismatch"

    price = _parse_decimal_candidate(fill.get("price"))
    if price is None:
        return "invalid_fill_price"
    if not price.is_finite():
        return "non_finite_fill_price"
    if price <= 0:
        return "non_positive_fill_price"

    raw_quantity = fill.get("quantity")
    quantity = (
        Decimal("0")
        if raw_quantity is None
        else _parse_decimal_candidate(raw_quantity)
    )
    raw_base_quantity = fill.get("base_quantity")
    base_quantity = (
        None
        if raw_base_quantity is None
        else _parse_decimal_candidate(raw_base_quantity)
    )
    if quantity is None or base_quantity is None and raw_base_quantity is not None:
        return "invalid_fill_quantity"
    if not quantity.is_finite() or (
        base_quantity is not None and not base_quantity.is_finite()
    ):
        return "non_finite_fill_quantity"
    if quantity != quantity.to_integral_value():
        return "invalid_fill_quantity"
    if quantity < 0 or (base_quantity is not None and base_quantity < 0):
        return "non_positive_fill_quantity"
    if quantity <= 0 and (base_quantity is None or base_quantity <= 0):
        return "non_positive_fill_quantity"
    return None


def _normalized_fill_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalized_fill_side(value: object) -> str:
    return str(value or "").strip().lower()


def _upsert_order_fills(
    *,
    db: Session,
    order: BrokerOrder,
    fills: list[dict[str, object]],
) -> int:
    existing_fill_rows = db.scalars(
        select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id)
    ).all()
    existing_fills_by_key = {
        execution_fill_dedupe_key(fill): fill for fill in existing_fill_rows
    }

    added = 0
    for fill in fills:
        if _fill_payload_integrity_reason(fill=fill, order=order) is not None:
            continue
        fill_signature = _fill_signature_candidate(order=order, fill=fill)
        if fill_signature is None:
            continue
        existing_fill = existing_fills_by_key.get(fill_signature)
        if existing_fill is not None:
            _enrich_existing_fill_economics(existing=existing_fill, incoming=fill)
            continue

        new_fill = ExecutionFill(
            idempotency_key=fill_signature,
            broker_order_pk=order.id,
            broker_fill_id=_optional_str(fill.get("broker_fill_id")),
            symbol=_normalized_fill_symbol(order.symbol),
            side=_normalized_fill_side(order.side),
            quantity=int(fill["quantity"]),
            base_quantity=_parse_decimal(fill.get("base_quantity")),
            quote_notional=_parse_decimal(fill.get("quote_notional")),
            price=_coerce_decimal(fill["price"]),
            settlement_currency=_optional_str(fill.get("settlement_currency")),
            transaction_cost=_parse_decimal(fill.get("transaction_cost")),
            transaction_cost_coverage=_optional_str(
                fill.get("transaction_cost_coverage")
            ),
            transaction_cost_source=_optional_str(fill.get("transaction_cost_source")),
            filled_at=_parse_timestamp(
                fill["filled_at"],
                fallback=_observation_time(),
            ),
            payload=_copy_payload(fill),
        )
        db.add(new_fill)
        added += 1
        existing_fills_by_key[fill_signature] = new_fill
    return added


def _enrich_existing_fill_economics(
    *,
    existing: ExecutionFill,
    incoming: dict[str, object],
) -> None:
    """Monotonically enrich economics without duplicating an execution fill."""

    existing_payload = dict(existing.payload) if isinstance(existing.payload, dict) else {}
    existing_currency_conflict = (
        existing_payload.get("settlement_currency_conflict") is True
    )
    incoming_currency_conflict = incoming.get("settlement_currency_conflict") is True
    incoming_currency = _optional_str(incoming.get("settlement_currency"))
    existing_currency = _optional_str(existing.settlement_currency)
    if incoming_currency_conflict:
        existing.settlement_currency = None
        existing_payload["settlement_currency_conflict"] = True
    elif not existing_currency_conflict and incoming_currency is not None:
        if existing_currency is None:
            existing.settlement_currency = incoming_currency
        elif existing_currency.upper() != incoming_currency.upper():
            existing.settlement_currency = None
            existing_payload["settlement_currency_conflict"] = True

    incoming_cost = _parse_decimal(incoming.get("transaction_cost"))
    incoming_coverage = _optional_str(incoming.get("transaction_cost_coverage"))
    incoming_source = _optional_str(incoming.get("transaction_cost_source"))
    persisted = execution_fill_economics(existing)
    should_update_cost = False
    if incoming_cost is not None and incoming_cost >= 0 and incoming_source is not None:
        if incoming_coverage == COST_COVERAGE_COMPLETE:
            should_update_cost = (
                persisted.transaction_cost_coverage != COST_COVERAGE_COMPLETE
                or persisted.transaction_cost is None
                or incoming_cost >= persisted.transaction_cost
                or incoming_source
                == "broker_payload:authoritative_cumulative_cost_revision"
            )
        elif (
            incoming_coverage == "partial"
            and persisted.transaction_cost_coverage == COST_COVERAGE_UNKNOWN
        ):
            should_update_cost = True
    if should_update_cost:
        existing.transaction_cost = incoming_cost
        existing.transaction_cost_coverage = incoming_coverage
        existing.transaction_cost_source = incoming_source

    chronology_updated = False
    incoming_time_precision = _optional_str(incoming.get("fill_time_precision"))
    incoming_time_source = _optional_str(incoming.get("fill_time_source"))
    existing_time_precision = _optional_str(
        existing_payload.get("fill_time_precision")
    )
    if (
        incoming_time_precision == "observed_upper_bound"
        and existing_time_precision != "exact"
    ):
        if existing_time_precision != "observed_upper_bound":
            existing_payload["fill_time_precision"] = "observed_upper_bound"
            chronology_updated = True
        if (
            incoming_time_source is not None
            and _optional_str(existing_payload.get("fill_time_source")) is None
        ):
            existing_payload["fill_time_source"] = incoming_time_source
            chronology_updated = True

    if (
        incoming_currency_conflict
        or incoming_currency is not None
        or should_update_cost
        or chronology_updated
    ):
        existing_payload["economics_revision"] = _copy_payload(
            {
                "settlement_currency": incoming_currency,
                "settlement_currency_conflict": incoming_currency_conflict,
                "transaction_cost": incoming_cost,
                "transaction_cost_coverage": incoming_coverage,
                "transaction_cost_source": incoming_source,
            }
        )
        existing.payload = _copy_payload(existing_payload)


def _fill_signature_candidate(
    *,
    order: BrokerOrder,
    fill: dict[str, object],
) -> str | None:
    if _fill_payload_integrity_reason(fill=fill, order=order) is not None:
        return None
    broker_fill_id = _optional_str(fill.get("broker_fill_id"))
    quantity = _parse_int(fill.get("quantity")) or 0
    base_quantity = _parse_decimal(fill.get("base_quantity"))
    price = _parse_decimal(fill.get("price"))
    symbol = _normalized_fill_symbol(order.symbol)
    side = _normalized_fill_side(order.side)
    if (
        (quantity <= 0 and (base_quantity is None or base_quantity <= 0))
        or price is None
        or not symbol
        or not side
    ):
        return None
    filled_at = _parse_timestamp(fill.get("filled_at"), fallback=_observation_time())
    return execution_fill_idempotency_key(
        broker_order_pk=order.id,
        broker_fill_id=broker_fill_id,
        symbol=symbol,
        side=side,
        filled_at=filled_at,
        price=price,
        quantity=quantity,
        base_quantity=base_quantity,
        quote_notional=_parse_decimal(fill.get("quote_notional")),
    )


def _coerce_decimal(value: object) -> Decimal:
    parsed = _parse_decimal(value)
    if parsed is None:
        return Decimal("0")
    return parsed


def _apply_order_metadata(
    *,
    order: BrokerOrder,
    payload: dict[str, object],
) -> None:
    observed_at = _parse_timestamp(
        payload.get("observed_at"),
        fallback=_observation_time(),
    )
    order.last_broker_sync_at = observed_at
    request_metadata = _dict_or_none(payload.get("request_metadata"))
    response_metadata = _dict_or_none(payload.get("response_metadata"))
    raw_payload = _dict_or_none(payload.get("payload"))
    response_payload = _dict_or_none(payload.get("response_payload"))
    if raw_payload is None:
        raw_payload = response_payload
    if request_metadata is not None:
        order.request_metadata = _merge_reconcile_request_metadata(
            existing=order.request_metadata,
            incoming=request_metadata,
        )
    if response_metadata is not None:
        order.response_metadata = response_metadata
    if raw_payload is not None:
        order.raw_broker_payload = raw_payload
    elif response_payload is not None:
        order.raw_broker_payload = response_payload


def _merge_reconcile_request_metadata(
    *,
    existing: object,
    incoming: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(incoming)
    if not isinstance(existing, dict):
        return merged
    for key in ("source", "metadata", "route", "time_in_force"):
        if key in existing and key not in merged:
            merged[key] = _json_safe_value(existing[key])
    for key in ("exchange", "execution_scope"):
        if key in existing:
            merged[key] = _json_safe_value(existing[key])
    return merged


def _order_status_to_plan_item_status(order_status: str) -> str | None:
    if order_status == NormalizedBrokerOrderStatus.CANCEL_PENDING.value:
        return "cancel_pending"
    if order_status == NormalizedBrokerOrderStatus.CANCELED.value:
        return "canceled"
    if order_status == NormalizedBrokerOrderStatus.EXPIRED.value:
        return "expired"
    try:
        return plan_item_status_for_broker_order_status(order_status)
    except ValueError:
        return None


def _parse_decimal(value: object) -> Decimal | None:
    parsed = _parse_decimal_candidate(value)
    return parsed if parsed is not None and parsed.is_finite() else None


def _parse_decimal_candidate(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        try:
            return Decimal(value)
        except (InvalidOperation, ValueError):
            return None
    return None


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    parsed = _parse_decimal_candidate(value)
    if (
        parsed is None
        or not parsed.is_finite()
        or parsed != parsed.to_integral_value()
    ):
        return None
    try:
        return int(parsed)
    except (OverflowError, ValueError):
        return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _copy_payload(payload: dict[str, object]) -> dict[str, object]:
    return _json_safe_dict(payload)


def _json_safe_dict(payload: dict[str, Any]) -> dict[str, Any]:
    redacted = redact_account_references(payload)
    if not isinstance(redacted, dict):
        return {}
    return {
        str(key): _json_safe_value(value)
        for key, value in redacted.items()
    }


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return _json_safe_dict(value)
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    return value


def _normalized_timestamp_signature(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()
