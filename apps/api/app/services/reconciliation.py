from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
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
from app.services.fill_idempotency import execution_fill_dedupe_key, execution_fill_idempotency_key
from app.services.rejected_order_retries import handle_reconciled_rejected_order
from app.services.runtime_transitions import apply_broker_order_status, set_plan_item_status


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
        payload = adapter.reconcile_order(
            session_state.access_token or "",
            order.broker_order_id,
            exchange=_order_reconciliation_exchange(order),
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
    fills = _parse_fills_from_payload(
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
            payload=_build_reconcile_event_payload(payload=payload, adapter=adapter),
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
    }
    items: list[dict[str, object]] = []
    position_sync_symbols: set[str] = set()
    for order in orders:
        classification_observed_at = _observation_time()
        reason: str | None = None
        result: dict[str, object] | None = None
        if not order.broker_order_id:
            classification = "failed_to_reconcile"
            reason = "missing_broker_order_id"
            order.last_error_classification = "missing_broker_order_id"
            order.last_error_message = "stale nonterminal order has no broker_order_id"
            order.last_error_retryable = False
            order.last_broker_sync_at = classification_observed_at
            _record_stale_order_classification_event(
                db=db,
                order=order,
                classification=classification,
                reason=reason,
                observed_at=classification_observed_at,
                result=None,
            )
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
            _record_stale_order_classification_event(
                db=db,
                order=order,
                classification=classification,
                reason=reason,
                observed_at=_observation_time(),
                result=result,
            )
            if int(result.get("fills_added") or 0) > 0 or str(result.get("status") or "") == "filled":
                position_sync_symbols.add(order.symbol)

        counts[classification] += 1
        items.append(
            {
                "broker_order_pk": order.id,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "symbol": order.symbol,
                "side": order.side,
                "status": order.status,
                "classification": classification.replace("_", "-"),
                "reason": reason,
                "last_broker_sync_at": _serialize_datetime(order.last_broker_sync_at),
                "result": result,
            }
        )

    position_sync_result = None
    if position_sync_symbols:
        position_sync_result = sync_execution_positions_from_broker_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=position_sync_symbols,
        )
    else:
        db.commit()
    return {
        "candidate_order_count": len(orders),
        "threshold_seconds": threshold_seconds,
        "stale_before": cutoff.isoformat(),
        "counts": {
            "live": counts["live"],
            "closed": counts["closed"],
            "cancelled": counts["cancelled"],
            "unknown": counts["unknown"],
            "failed-to-reconcile": counts["failed_to_reconcile"],
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
    return "unknown", _optional_str(message)


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
    symbols: set[str] | None,
) -> list[BrokerOrder]:
    where = [
        BrokerOrder.mode == settings.runtime_mode,
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
                        average_price=None,
                        updated_from="reconciliation",
                        last_reconciled_at=observed_at,
                        raw_payload=_snapshot_payload(snapshot.payload),
                    )
                )
                created += 1
                continue

            if position.quantity != 0:
                position.quantity = 0
                zeroed += 1
            position.updated_from = "reconciliation"
            position.last_reconciled_at = observed_at
            position.raw_payload = _snapshot_payload(snapshot.payload)
            if position.quantity == 0:
                continue
            updated += 1
            continue

        if position is None:
            db.add(
                ExecutionPosition(
                    mode=settings.runtime_mode,
                    symbol=symbol,
                    quantity=holding.quantity,
                    average_price=holding.average_price,
                    updated_from="reconciliation",
                    last_reconciled_at=observed_at,
                    raw_payload=_snapshot_payload(snapshot.payload),
                )
            )
            created += 1
            continue

        changed = False
        if position.quantity != holding.quantity:
            position.quantity = holding.quantity
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


def _snapshot_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _reconcilable_order_statuses() -> tuple[str, ...]:
    return (
        NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        NormalizedBrokerOrderStatus.SUBMITTED.value,
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
        NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
        NormalizedBrokerOrderStatus.RETRY_PENDING.value,
    )


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
    if normalized in {"KRX", "NXT", "SOR"}:
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
) -> list[dict[str, object]]:
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
        return result

    total_filled = _parse_int(payload.get("filled_quantity"))
    if total_filled is None or total_filled <= 0:
        return []

    existing_total = _sum_fill_quantities(order.fills)
    if total_filled <= existing_total:
        return []

    average_fill_price = _parse_decimal(payload.get("average_fill_price"))
    if average_fill_price is None and payload.get("fills") is None:
        return []

    filled_at = _parse_timestamp(
        payload.get("filled_at"),
        fallback=observed_at,
    )
    return [
        {
            "broker_fill_id": _optional_str(payload.get("broker_fill_id")),
            "quantity": total_filled - existing_total,
            "price": average_fill_price or Decimal("0"),
            "filled_at": filled_at.isoformat(),
            "symbol": order.symbol,
            "side": order.side,
        }
    ]


def _sum_fill_quantities(fills: list[ExecutionFill]) -> int:
    return sum(fill.quantity for fill in fills)


def _normalize_fill_payload(
    *,
    fill_item: dict[str, object],
    observed_at: datetime,
    order: BrokerOrder,
) -> dict[str, object] | None:
    quantity = _parse_int(fill_item.get("quantity"))
    if quantity is None or quantity <= 0:
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
    quantity = _parse_int(fill.get("quantity"))
    price = _parse_decimal(fill.get("price"))
    symbol = _optional_str(fill.get("symbol"))
    side = _optional_str(fill.get("side"))
    if (
        quantity is None
        or quantity <= 0
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
    if order_status == NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value:
        return "broker_order_submission_pending"
    if order_status == NormalizedBrokerOrderStatus.SUBMITTED.value:
        return "broker_order_submitted"
    if order_status == NormalizedBrokerOrderStatus.OPEN.value:
        return "open"
    if order_status == NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value:
        return "partially_filled"
    if order_status == NormalizedBrokerOrderStatus.FILLED.value:
        return "filled"
    if order_status == NormalizedBrokerOrderStatus.CANCEL_PENDING.value:
        return "cancel_pending"
    if order_status == NormalizedBrokerOrderStatus.CANCELED.value:
        return "canceled"
    if order_status == NormalizedBrokerOrderStatus.EXPIRED.value:
        return "expired"
    if order_status == NormalizedBrokerOrderStatus.RETRY_PENDING.value:
        return "broker_order_retry_pending"
    if order_status == NormalizedBrokerOrderStatus.REJECTED.value:
        return "broker_order_rejected"
    if order_status == NormalizedBrokerOrderStatus.FAILED.value:
        return "broker_order_failed"
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
