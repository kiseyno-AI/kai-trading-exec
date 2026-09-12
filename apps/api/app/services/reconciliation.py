from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from inspect import Parameter, signature
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, selectinload

from app.broker.base import (
    BrokerAccountHolding,
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
    ExecutionPosition,
    new_id,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import (
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
                    plan_status = _order_status_to_plan_item_status(order.status)
                    if plan_status is not None:
                        set_plan_item_status(
                            db=db,
                            plan_item=order.plan_item,
                            status=plan_status,
                            source="reconciliation",
                            reason=repair["reason"],
                            payload={
                                "broker_order_pk": order.id,
                                "broker_order_id": order.broker_order_id,
                                "idempotency_key": order.idempotency_key,
                                "status_before": status_before,
                                "status": order.status,
                                "classification": repair["classification"],
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
        "items": items,
    }


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
    if not snapshot.holdings_complete:
        raise BrokerAdapterError(
            "broker account snapshot holdings coverage is incomplete; position sync blocked",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
            response_payload={"diagnostics": snapshot.diagnostics},
            observed_at=snapshot.observed_at,
        )
    normalized_provider = str(adapter.provider or "").strip().lower()
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
            changed = False
            if position.quantity != 0:
                position.quantity = 0
                changed = True
            if position.base_quantity is not None and position.base_quantity != 0:
                position.base_quantity = Decimal("0")
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
        position.raw_payload = _position_snapshot_payload(
            snapshot_payload=snapshot.payload,
            holding=holding,
            exchange=identity_exchange,
            identity=position_identity,
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
        "execution_scope": execution_scope,
        "execution_scope_source": execution_scope_source,
    }


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
) -> dict[str, Any] | None:
    payload = _snapshot_payload(snapshot_payload)
    if payload is None:
        payload = {}
    else:
        payload = dict(payload)
    normalized_exchange = str(exchange or "").strip().upper()
    payload = payload_with_position_identity(payload, identity=identity)
    if holding is None or normalized_exchange not in {"NASD", "NYSE", "AMEX"}:
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
    payload["_position_sync"] = position_sync
    return payload


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
