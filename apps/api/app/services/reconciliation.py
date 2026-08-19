from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from inspect import signature
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.broker.base import (
    BrokerAdapter,
    BrokerAdapterError,
    BrokerEventSource,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import NONTERMINAL_ORDER_STATUSES
from app.services.fill_idempotency import execution_fill_dedupe_key, execution_fill_idempotency_key
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
    return {
        "candidate_order_count": len(orders),
        "reconciled_order_count": reconciled,
        "fills_added": fills_added,
    }


def reconcile_broker_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    order: BrokerOrder,
) -> dict[str, object]:
    order_status_before = order.status
    event_time = _observation_time()
    fills_added = 0

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
        payload = _call_reconcile_order(
            adapter=adapter,
            access_token=session_state.access_token or "",
            order=order,
        )
        if not isinstance(payload, dict):
            raise ValueError("reconcile_order adapter returned non-dictionary payload")
    except (BrokerAdapterError, Exception) as exc:  # broad except to keep reconcile cycle durable
        return _reconcile_adapter_error(
            exc=exc,
            order=order,
            adapter=adapter,
            requested_broker_order_id=order.broker_order_id,
            event_time=event_time,
            db=db,
            order_status_before=order_status_before,
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
        db.commit()
        db.refresh(order)
        return {
            "status_code": 200,
            "broker_order_id": order.broker_order_id,
            "status": "unknown",
            "status_before": order_status_before,
            "status_changed": False,
            "fills_added": 0,
            "filled_delta": 0,
            "message": "unrecognized broker status",
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
        retry_failure = handle_reconciled_rejected_order(
            settings=settings,
            db=db,
            order=order,
            payload=payload,
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


def _call_reconcile_order(
    *,
    adapter: BrokerAdapter,
    access_token: str,
    order: BrokerOrder,
) -> dict[str, Any] | None:
    kwargs: dict[str, object] = {"exchange": _order_reconciliation_exchange(order)}
    try:
        parameters = signature(adapter.reconcile_order).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "submitted_at" in parameters:
        kwargs["submitted_at"] = order.submitted_at
    return adapter.reconcile_order(
        access_token,
        order.broker_order_id or "",
        **kwargs,
    )


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
    position_sync_symbols: set[str] = set()
    for order in orders:
        classification_observed_at = _observation_time()
        status_before = order.status
        error_code_before = order.last_error_code
        error_classification_before = order.last_error_classification
        reason: str | None = None
        result: dict[str, object] | None = None
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
                result = _stale_order_dry_run_result(order=order)
            else:
                result = reconcile_broker_order(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    order=order,
                )
            classification, reason = _stale_order_reconciliation_classification(
                status=str(result.get("status") or order.status),
                status_code=int(result.get("status_code") or 200),
                message=result.get("message"),
            )
            if int(result.get("fills_added") or 0) > 0 or str(result.get("status") or "") == "filled":
                position_sync_symbols.add(order.symbol)

        repair = _stale_order_repair_decision(
            order=order,
            status_before=status_before,
            classification=classification,
            reason=reason,
            result=result,
            error_code_before=error_code_before,
            error_classification_before=error_classification_before,
        )
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
            )

        if not dry_run:
            _record_stale_order_classification_event(
                db=db,
                order=order,
                classification=classification,
                reason=reason,
                observed_at=_observation_time(),
                result=result,
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
                "repair_action": "would_repair" if dry_run and repair is not None else "repaired" if repair is not None else "none",
                "observed_broker_code": (
                    repair["observed_broker_code"]
                    if repair is not None
                    else order.last_error_code
                ),
                "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
                "result": result,
            }
        )

    position_sync_result = None
    if position_sync_symbols and not dry_run:
        position_sync_result = sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=position_sync_symbols,
        )
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
        "items": items,
    }


def _load_stale_nonterminal_orders_for_reconciliation(
    *,
    db: Session,
    settings: Settings,
    broker_name: str,
    stale_before: datetime,
    symbols: set[str] | None,
) -> list[BrokerOrder]:
    observed_at = func.coalesce(
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.submitted_at,
        BrokerOrder.updated_at,
        BrokerOrder.created_at,
    )
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
) -> dict[str, int]:
    normalized_symbols = _normalize_symbols(symbols)
    session_state = ensure_authenticated_broker_session(
        settings=settings,
        db=db,
        adapter=adapter,
        source="reconciliation",
        reason="refresh execution positions from broker snapshot",
    )
    snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    if normalized_symbols is not None:
        tracked_symbols = normalized_symbols
    else:
        local_symbols = set(
            db.scalars(
                select(ExecutionPosition.symbol).where(
                    ExecutionPosition.mode == settings.runtime_mode,
                )
            ).all()
        )
        tracked_symbols = local_symbols | {holding.symbol for holding in snapshot.holdings}
    if not tracked_symbols:
        return {
            "tracked_symbol_count": 0,
            "created": 0,
            "updated": 0,
            "zeroed": 0,
        }

    position_by_symbol = {
        position.symbol: position
        for position in db.scalars(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.symbol.in_(sorted(tracked_symbols)),
            )
        ).all()
    }
    holding_by_symbol = {
        holding.symbol: holding
        for holding in snapshot.holdings
        if holding.symbol in tracked_symbols
    }
    observed_at = snapshot.observed_at

    created = 0
    updated = 0
    zeroed = 0
    for symbol in sorted(tracked_symbols):
        holding = holding_by_symbol.get(symbol)
        position = position_by_symbol.get(symbol)
        if holding is None:
            if position is None:
                db.add(
                    ExecutionPosition(
                        mode=settings.runtime_mode,
                        symbol=symbol,
                        quantity=0,
                        base_quantity=Decimal("0"),
                        average_price=None,
                        updated_from="reconciliation",
                        last_reconciled_at=observed_at,
                        raw_payload=_snapshot_payload(snapshot.payload),
                    )
                )
                created += 1
                continue

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
            position.raw_payload = _snapshot_payload(snapshot.payload)
            continue

        holding_base_quantity = _holding_base_quantity(holding.payload)
        if position is None:
            db.add(
                ExecutionPosition(
                    mode=settings.runtime_mode,
                    symbol=symbol,
                    quantity=0 if holding_base_quantity is not None else holding.quantity,
                    base_quantity=holding_base_quantity,
                    average_price=holding.average_price,
                    updated_from="reconciliation",
                    last_reconciled_at=observed_at,
                    raw_payload=_snapshot_payload(snapshot.payload),
                )
            )
            created += 1
            continue

        changed = False
        expected_quantity = 0 if holding_base_quantity is not None else holding.quantity
        if position.quantity != expected_quantity:
            position.quantity = expected_quantity
            changed = True
        if holding_base_quantity is not None and position.base_quantity != holding_base_quantity:
            position.base_quantity = holding_base_quantity
            changed = True
        if holding_base_quantity is None and position.base_quantity not in {None, Decimal("0")}:
            position.base_quantity = None
            changed = True
        if position.average_price != holding.average_price:
            position.average_price = holding.average_price
            changed = True
        if position.updated_from != "reconciliation":
            position.updated_from = "reconciliation"
            changed = True
        position.last_reconciled_at = observed_at
        position.raw_payload = _snapshot_payload(snapshot.payload)
        if changed:
            updated += 1

    db.commit()
    return {
        "tracked_symbol_count": len(tracked_symbols),
        "created": created,
        "updated": updated,
        "zeroed": zeroed,
    }


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
    return payload


def _reconcilable_order_statuses() -> tuple[str, ...]:
    return NONTERMINAL_ORDER_STATUSES


def _normalize_symbols(symbols: set[str] | None) -> set[str] | None:
    if symbols is None:
        return None
    return {symbol.upper() for symbol in symbols}


def _reconcile_adapter_error(
    *,
    exc: BaseException,
    order: BrokerOrder,
    adapter: BrokerAdapter,
    requested_broker_order_id: str,
    event_time: datetime,
    db: Session,
    order_status_before: str,
) -> dict[str, object]:
    if isinstance(exc, BrokerAdapterError):
        message = str(exc)
        status_code = exc.status_code or 503
        order.last_error_classification = exc.classification.value
        order.last_error_code = exc.code
        order.last_error_message = message
        order.last_error_retryable = exc.retryable
        order.last_broker_sync_at = exc.observed_at or event_time
        _apply_order_metadata(
            order=order,
            payload={
                "provider": adapter.provider,
                "broker_order_id": requested_broker_order_id,
                "status": None,
                "broker_status": exc.code,
                "observed_at": _serialize_datetime(order.last_broker_sync_at),
                "request_metadata": exc.request_metadata,
                "response_metadata": exc.response_metadata,
                "request_payload": exc.request_payload,
                "payload": exc.response_payload,
                "message": message,
            },
        )
        if _should_record_reconcile_failure_event(
            db=db,
            order=order,
            message=message,
            broker_status=exc.code,
            error_classification=exc.classification.value,
            observed_at=order.last_broker_sync_at,
        ):
            db.add(
                BrokerOrderEvent(
                    broker_order_pk=order.id,
                    event_type="reconcile_failed",
                    event_source=BrokerEventSource.RECONCILIATION.value,
                    normalized_status=order.status,
                    broker_status=exc.code,
                    error_classification=exc.classification.value,
                    retryable=exc.retryable,
                    message=message,
                    request_payload=exc.request_payload,
                    response_payload=exc.response_payload,
                    payload=_build_reconcile_event_payload(
                        payload={
                            "provider": adapter.provider,
                            "broker_order_id": requested_broker_order_id,
                            "status": None,
                            "broker_status": exc.code,
                            "observed_at": _serialize_datetime(order.last_broker_sync_at),
                            "message": message,
                            "request_metadata": exc.request_metadata,
                            "response_metadata": exc.response_metadata,
                            "request_payload": exc.request_payload,
                            "payload": exc.response_payload,
                        },
                        adapter=adapter,
                    ),
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
    message = f"{exc.__class__.__name__}: {exc}"
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
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="reconcile_failed",
                event_source=BrokerEventSource.RECONCILIATION.value,
                normalized_status=order.status,
                error_classification="unknown",
                message=message,
                payload=_build_reconcile_event_payload(
                    payload={
                        "provider": adapter.provider,
                        "broker_order_id": requested_broker_order_id,
                        "status": None,
                        "broker_status": None,
                        "observed_at": _serialize_datetime(observed_at),
                        "message": message,
                    },
                    adapter=adapter,
                ),
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
    return {
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


def _order_reconciliation_exchange(order: BrokerOrder) -> str:
    for payload in (order.request_metadata, order.response_metadata, order.raw_broker_payload):
        exchange = _exchange_from_payload(payload)
        if exchange is not None:
            return exchange
    if order.plan_item is not None:
        exchange = _exchange_from_payload(order.plan_item.risk_context)
        if exchange is not None:
            return exchange
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
    if normalized in {"KRX", "NXT", "SOR", "UPBIT", "ALPACA", "US"}:
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
    fills_payload = payload.get("fills")
    if isinstance(fills_payload, list) and fills_payload:
        result: list[dict[str, object]] = []
        for fill_item in fills_payload:
            if not isinstance(fill_item, dict):
                continue
            parsed_fill = _normalize_fill_payload(
                fill_item=fill_item,
                observed_at=observed_at,
                order=order,
            )
            if parsed_fill is None:
                continue
            result.append(parsed_fill)
        return result, []

    total_filled_base = _parse_decimal(payload.get("filled_base_quantity"))
    if total_filled_base is not None and total_filled_base > 0:
        existing_total_base = sum(
            Decimal(fill.base_quantity or 0)
            for fill in order.fills
        )
        average_fill_price = _parse_decimal(payload.get("average_fill_price"))
        delta_result = _cumulative_fill_delta(
            order=order,
            total_filled=total_filled_base,
            existing_total=existing_total_base,
            average_fill_price=average_fill_price,
            quantity_basis="base_quantity",
        )
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
        }], []

    total_filled = _parse_int(payload.get("filled_quantity"))
    if total_filled is None or total_filled <= 0:
        return [], []

    existing_total = _sum_fill_quantities(order.fills)
    average_fill_price = _parse_decimal(payload.get("average_fill_price"))
    delta_result = _cumulative_fill_delta(
        order=order,
        total_filled=Decimal(total_filled),
        existing_total=Decimal(existing_total),
        average_fill_price=average_fill_price,
        quantity_basis="quantity",
    )
    if delta_result["status"] != "ok":
        anomalies = delta_result["anomalies"]
        return [], anomalies if isinstance(anomalies, list) else []

    filled_at = _parse_timestamp(
        payload.get("filled_at"),
        fallback=observed_at,
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
        }
    ], []


def _sum_fill_quantities(fills: list[ExecutionFill]) -> int:
    return sum(fill.quantity for fill in fills)


def _cumulative_fill_delta(
    *,
    order: BrokerOrder,
    total_filled: Decimal,
    existing_total: Decimal,
    average_fill_price: Decimal | None,
    quantity_basis: str,
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
        fills=order.fills,
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
    return {
        "broker_fill_id": _optional_str(fill_item.get("broker_fill_id")),
        "quantity": quantity,
        "base_quantity": base_quantity,
        "quote_notional": _parse_decimal(fill_item.get("quote_notional")),
        "price": price,
        "filled_at": filled_at.isoformat(),
        "symbol": _optional_str(fill_item.get("symbol")) or order.symbol,
        "side": _optional_str(fill_item.get("side")) or order.side,
    }


def _upsert_order_fills(
    *,
    db: Session,
    order: BrokerOrder,
    fills: list[dict[str, object]],
) -> int:
    existing_fill_rows = db.scalars(
        select(ExecutionFill).where(ExecutionFill.broker_order_pk == order.id)
    ).all()
    existing_fill_keys = {
        execution_fill_dedupe_key(fill)
        for fill in existing_fill_rows
    }

    added = 0
    for fill in fills:
        fill_signature = _fill_signature_candidate(order=order, fill=fill)
        if fill_signature is None or fill_signature in existing_fill_keys:
            continue

        db.add(
            ExecutionFill(
                idempotency_key=fill_signature,
                broker_order_pk=order.id,
                broker_fill_id=_optional_str(fill.get("broker_fill_id")),
                symbol=str(fill["symbol"]),
                side=str(fill["side"]),
                quantity=int(fill["quantity"]),
                base_quantity=_parse_decimal(fill.get("base_quantity")),
                quote_notional=_parse_decimal(fill.get("quote_notional")),
                price=_coerce_decimal(fill["price"]),
                filled_at=_parse_timestamp(
                    fill["filled_at"],
                    fallback=_observation_time(),
                ),
                payload=_copy_payload(fill),
            )
        )
        added += 1
        existing_fill_keys.add(fill_signature)
    return added


def _fill_signature_candidate(
    *,
    order: BrokerOrder,
    fill: dict[str, object],
) -> str | None:
    broker_fill_id = _optional_str(fill.get("broker_fill_id"))
    quantity = _parse_int(fill.get("quantity")) or 0
    base_quantity = _parse_decimal(fill.get("base_quantity"))
    price = _parse_decimal(fill.get("price"))
    symbol = _optional_str(fill.get("symbol"))
    side = _optional_str(fill.get("side"))
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
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float):
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            return Decimal(value)
        except (InvalidOperation, ValueError):
            return None
    return None


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(Decimal(value))
        except (InvalidOperation, ValueError):
            return None
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _copy_payload(payload: dict[str, object]) -> dict[str, object]:
    return _json_safe_dict(payload)


def _json_safe_dict(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _json_safe_value(value)
        for key, value in payload.items()
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
