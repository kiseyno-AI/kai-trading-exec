from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import MonitoringEvaluationRequest, PlaceOrderRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition, MonitoringCheckpoint
from app.services.broker_orders import place_broker_order

MONITORED_INTENTS = frozenset({"target_sell", "stop_sell"})
BLOCKED_MONITORING_STATUSES = frozenset(
    {
        "target_sell_triggered_blocked",
        "stop_sell_triggered_blocked",
    }
)
NON_MONITORABLE_PLAN_ITEM_STATUSES = frozenset(
    {
        "broker_order_submission_pending",
        "broker_order_submitted",
        "broker_order_retry_pending",
        "broker_order_rejected",
        "broker_order_failed",
        "submitted",
        "open",
        "partially_filled",
        "filled",
        "cancel_pending",
        "canceled",
        "expired",
        "retry_pending",
        "rejected",
        "failed",
    }
)
MONITORING_WORKER_NAME = "intraday_target_stop_monitor"


@dataclass(frozen=True)
class EvaluateMonitoringResult:
    status_code: int
    payload: dict[str, object]


def materialize_monitoring_plan_items(plan: ExecutionPlan) -> None:
    for item in plan.items:
        if item.intent not in MONITORED_INTENTS:
            continue
        if _monitoring_item_is_configured(item):
            item.status = _monitoring_status(item.intent)
            continue
        item.status = _pending_configuration_status(item.intent)


def evaluate_monitoring(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: MonitoringEvaluationRequest,
    now: datetime | None = None,
) -> EvaluateMonitoringResult:
    triggered_at = _normalize_utc(now or command.observed_at or datetime.now(UTC))
    prices = {price.symbol: price for price in command.prices}
    remaining_quantities_by_symbol = _load_remaining_quantities_by_symbol(db)
    items = _load_monitored_items(settings=settings, db=db)
    results: list[dict[str, object]] = []
    triggered_item_count = 0
    submitted_order_count = 0
    blocked_item_count = 0

    for item in items:
        if item.status in NON_MONITORABLE_PLAN_ITEM_STATUSES:
            continue

        price = prices.get(item.symbol)
        if price is None:
            continue
        if not _monitoring_item_is_configured(item):
            item.status = _pending_configuration_status(item.intent)
            results.append(
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "intent": item.intent,
                    "trigger_price": _serialize_decimal(item.trigger_price),
                    "observed_price": _serialize_decimal(price.price),
                    "outcome": "invalid_configuration",
                    "message": "target/stop monitoring item is missing sell-side trigger configuration",
                }
            )
            continue

        if not _is_triggered(item=item, observed_price=price.price):
            if item.status in BLOCKED_MONITORING_STATUSES:
                item.status = _monitoring_status(item.intent)
            results.append(
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "intent": item.intent,
                    "trigger_price": _serialize_decimal(item.trigger_price),
                    "observed_price": _serialize_decimal(price.price),
                    "outcome": "waiting",
                    "message": "trigger condition not met",
                }
            )
            continue

        triggered_item_count += 1
        quantity = _resolve_sell_quantity(
            item=item,
            remaining_quantity=remaining_quantities_by_symbol.get(item.symbol),
        )
        if quantity is None:
            results.append(
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "intent": item.intent,
                    "trigger_price": _serialize_decimal(item.trigger_price),
                    "observed_price": _serialize_decimal(price.price),
                    "outcome": "no_position",
                    "message": "no sellable execution position is available for this trigger",
                }
            )
            continue

        order_request = PlaceOrderRequest(
            idempotency_key=f"monitoring:{item.intent}:{item.id}",
            execution_plan_item_id=item.id,
            symbol=item.symbol,
            side="sell",
            quantity=quantity,
            order_type="limit" if item.limit_price is not None else "market",
            limit_price=item.limit_price,
            time_in_force=item.time_in_force or "day",
            exchange=_plan_item_exchange(item),
        )
        placement = place_broker_order(
            settings=settings,
            db=db,
            adapter=adapter,
            command=order_request,
            request_source=f"monitoring:{MONITORING_WORKER_NAME}",
        )

        item_result = {
            "execution_plan_item_id": item.id,
            "symbol": item.symbol,
            "intent": item.intent,
            "trigger_price": _serialize_decimal(item.trigger_price),
            "observed_price": _serialize_decimal(price.price),
        }
        if placement.status_code == 201:
            _reserve_remaining_quantity(
                remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                symbol=item.symbol,
                quantity=quantity,
            )
            submitted_order_count += 1
            item_result["outcome"] = "submitted"
            item_result["order"] = placement.payload.get("order")
            results.append(item_result)
            continue

        if placement.status_code == 409 and "block_reason" in placement.payload:
            blocked_item_count += 1
            item.status = f"{item.intent}_triggered_blocked"
            db.commit()
            item_result["outcome"] = "blocked"
            item_result["block_reason"] = placement.payload.get("block_reason")
            item_result["message"] = placement.payload.get("message")
            results.append(item_result)
            continue

        if placement.payload.get("duplicate") is True:
            duplicate_quantity = _serialized_order_quantity(placement.payload.get("order"))
            if duplicate_quantity is not None:
                _reserve_remaining_quantity(
                    remaining_quantities_by_symbol=remaining_quantities_by_symbol,
                    symbol=item.symbol,
                    quantity=duplicate_quantity,
                )
            item_result["outcome"] = "duplicate"
            item_result["order"] = placement.payload.get("order")
            item_result["message"] = placement.payload.get("message")
            results.append(item_result)
            continue

        item_result["outcome"] = "error"
        item_result["message"] = placement.payload.get("message")
        results.append(item_result)

    checkpoint = _update_monitoring_checkpoint(
        db=db,
        worker_name=MONITORING_WORKER_NAME,
        triggered_at=triggered_at,
        details={
            "evaluated_item_count": len(results),
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": submitted_order_count,
            "blocked_item_count": blocked_item_count,
            "price_count": len(prices),
        },
    )
    return EvaluateMonitoringResult(
        status_code=200,
        payload={
            "triggered_at": triggered_at.isoformat(),
            "worker_name": checkpoint.worker_name,
            "evaluated_item_count": len(results),
            "triggered_item_count": triggered_item_count,
            "submitted_order_count": submitted_order_count,
            "blocked_item_count": blocked_item_count,
            "items": results,
        },
    )


def _load_monitored_items(
    *,
    settings: Settings,
    db: Session,
) -> list[ExecutionPlanItem]:
    return db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.intent.in_(tuple(MONITORED_INTENTS)),
        )
        .options(selectinload(ExecutionPlanItem.plan))
        .order_by(ExecutionPlanItem.created_at.asc(), ExecutionPlanItem.id.asc())
    ).all()


def _load_remaining_quantities_by_symbol(db: Session) -> dict[str, int]:
    positions = db.scalars(
        select(ExecutionPosition).order_by(ExecutionPosition.symbol.asc())
    ).all()
    return {
        position.symbol: position.quantity
        for position in positions
        if position.quantity > 0
    }


def _monitoring_item_is_configured(item: ExecutionPlanItem) -> bool:
    return (
        item.side == "sell"
        and item.trigger_price is not None
        and _quantity_policy_supported(item.quantity_policy)
    )


def _quantity_policy_supported(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = quantity_policy.get("quantity")
        return _parse_positive_int(quantity) is not None
    if kind == "percent":
        fraction = _parse_fraction(quantity_policy.get("fraction"))
        return fraction is not None
    return False


def _is_triggered(
    *,
    item: ExecutionPlanItem,
    observed_price: Decimal,
) -> bool:
    trigger_price = item.trigger_price
    if trigger_price is None:
        return False
    if item.intent == "target_sell":
        return observed_price >= trigger_price
    if item.intent == "stop_sell":
        return observed_price <= trigger_price
    return False


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

    kind = quantity_policy.get("kind")
    if kind == "shares":
        quantity = _parse_positive_int(quantity_policy.get("quantity"))
        if quantity is None or quantity > remaining_quantity:
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


def _monitoring_status(intent: str) -> str:
    return f"{intent}_monitoring"


def _pending_configuration_status(intent: str) -> str:
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


def _serialized_order_quantity(order_payload: object) -> int | None:
    if not isinstance(order_payload, dict):
        return None
    return _parse_positive_int(order_payload.get("quantity"))


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _parse_fraction(value: object) -> Decimal | None:
    try:
        fraction = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if fraction <= 0 or fraction > 1:
        return None
    return fraction


def _plan_item_exchange(item: ExecutionPlanItem) -> str:
    risk_context = item.risk_context
    if not isinstance(risk_context, dict):
        return "KRX"
    exchange = risk_context.get("exchange")
    if not isinstance(exchange, str):
        return "KRX"
    normalized = exchange.strip().upper()
    return normalized or "KRX"


def _update_monitoring_checkpoint(
    *,
    db: Session,
    worker_name: str,
    triggered_at: datetime,
    details: dict[str, object],
) -> MonitoringCheckpoint:
    checkpoint = db.scalar(
        select(MonitoringCheckpoint).where(MonitoringCheckpoint.worker_name == worker_name)
    )
    if checkpoint is None:
        checkpoint = MonitoringCheckpoint(worker_name=worker_name)
        db.add(checkpoint)
    checkpoint.status = "running"
    checkpoint.last_heartbeat_at = triggered_at
    checkpoint.last_success_at = triggered_at
    checkpoint.details = details
    db.commit()
    db.refresh(checkpoint)
    return checkpoint


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"
