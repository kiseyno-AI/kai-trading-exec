from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest, ShortTermWatcherDecisionRequest
from app.broker.base import BrokerAdapter, BrokerAdapterError, BrokerAccountSnapshot
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_orders import place_broker_order
from app.services.execution_api import serialize_broker_order
from app.services.live_market_context import load_live_market_regime_context, merge_live_market_regime_context
from app.services.market_regime_guardrails import evaluate_market_regime_guardrail, market_regime_context_from_item_payloads
from app.services.broker_order_state import (
    CAPITAL_RESERVING_ORDER_STATUSES,
    active_local_broker_order_filters,
)
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)

OPEN_ORDER_STATUSES = frozenset(CAPITAL_RESERVING_ORDER_STATUSES)
SUPPORTED_SHORT_TERM_BUY_INTENTS = frozenset({"next_open_buy", "intraday_watch_buy"})
SHORT_TERM_DECIDER_NAME = "short-term-decider"
MAX_APPROVED_NOTIONAL = Decimal("10000000")


@dataclass(frozen=True)
class ShortTermWatcherDecisionResult:
    status_code: int
    payload: dict[str, object]


def evaluate_short_term_watcher_decision(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: ShortTermWatcherDecisionRequest,
) -> ShortTermWatcherDecisionResult:
    now = datetime.now(UTC)
    item = _load_candidate(settings=settings, db=db, execution_plan_item_id=command.execution_plan_item_id)
    if item is None:
        return _rejected(404, "candidate_not_found", "execution_plan_item_id was not found")

    safety_state = ensure_runtime_control_state(settings=settings, db=db)
    safety = runtime_safety_payload(safety_state)
    account_context = command.account_context
    broker_account_snapshot = None
    if not command.dry_run and automated_order_submission_allowed(safety_state):
        try:
            session_state = ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
                source="short_term_watcher",
                reason=f"load live account context for {item.id}",
            )
            broker_account_snapshot = adapter.get_account_snapshot(session_state.access_token or "")
            account_context = command.account_context_from_snapshot(broker_account_snapshot)
        except AttributeError:
            broker_account_snapshot = None
        except BrokerAdapterError as exc:
            return ShortTermWatcherDecisionResult(
                status_code=exc.status_code or 503,
                payload={
                    "approved": False,
                    "dry_run": command.dry_run,
                    "reason_code": "broker_account_context_unavailable",
                    "message": str(exc),
                    "candidate": _candidate_payload(item),
                    "safety": safety,
                },
            )
    safety = runtime_safety_payload(safety_state)
    context = _candidate_context(
        settings=settings,
        db=db,
        item=item,
        command=command,
        account_context=account_context,
        broker_account_snapshot=broker_account_snapshot,
    )
    context["allowed_action_envelope"] = _allowed_action_envelope(settings=settings, context=context, command=command)

    rejection = _validate_candidate(item=item)
    if rejection is None:
        rejection = _validate_agent_output(command=command)
    if rejection is None:
        rejection = _validate_account_context(context=context)
    if rejection is None:
        rejection = _validate_risk_rails(command=command, context=context)
    if rejection is None and not command.dry_run and not automated_order_submission_allowed(safety_state):
        block_reason = automated_order_submission_block_reason(safety_state)
        rejection = ("runtime_safety_blocked", str(block_reason))

    audit = {
        "evaluated_at": now.isoformat(),
        "dry_run": command.dry_run,
        "strict_contract": "short_term_watcher_decision.v1",
        "agent_output": command.agent_output.model_dump(mode="json"),
        "account_context": account_context.model_dump(mode="json"),
        "candidate_context": context,
        "safety": safety,
    }

    if rejection is not None:
        code, message = rejection
        _persist_audit(item=item, audit={**audit, "outcome": "rejected", "reason_code": code, "message": message})
        db.commit()
        return ShortTermWatcherDecisionResult(
            status_code=200,
            payload={
                "approved": False,
                "dry_run": command.dry_run,
                "reason_code": code,
                "message": message,
                "candidate": _candidate_payload(item),
                "context": context,
                "safety": safety,
            },
        )

    quantity = _resolve_quantity(command=command, reference_price=Decimal(str(context["reference_price"])))
    envelope = context.get("allowed_action_envelope")
    if isinstance(envelope, dict):
        quantity = min(int(quantity or 0), int(envelope.get("max_quantity") or 0))
    assert quantity is not None
    order_preview = _order_preview(item=item, command=command, quantity=quantity)
    monitoring_preview = _monitoring_preview(item=item, command=command, quantity=quantity)

    if command.dry_run:
        _persist_audit(item=item, audit={**audit, "outcome": "approved_preview", "order": order_preview, "monitoring": monitoring_preview})
        db.commit()
        return ShortTermWatcherDecisionResult(
            status_code=200,
            payload={
                "approved": True,
                "dry_run": True,
                "reason_code": "approved_preview",
                "candidate": _candidate_payload(item),
                "context": context,
                "order_preview": order_preview,
                "monitoring_preview": monitoring_preview,
                "safety": safety,
            },
        )

    order_result = place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=PlaceOrderRequest(**order_preview),
        request_source="short-term-watcher:decision",
    )
    persisted_order = _load_order(settings=settings, db=db, idempotency_key=order_preview["idempotency_key"])
    refreshed_item = db.get(ExecutionPlanItem, item.id, options=[selectinload(ExecutionPlanItem.plan)]) or item
    submission_accepted = _order_submission_accepted(order_result=order_result, order=persisted_order)
    materialized_monitoring = []
    outcome = "submitted" if submission_accepted else "submission_not_accepted"
    if submission_accepted:
        materialized_monitoring = _materialize_monitoring_items(
            db=db,
            item=refreshed_item,
            command=command,
            quantity=quantity,
        )
    _persist_audit(
        item=refreshed_item,
        audit={
            **audit,
            "outcome": outcome,
            "order": order_preview,
            "monitoring_preview": monitoring_preview,
            "monitoring": materialized_monitoring,
            "order_result": order_result.payload,
        },
    )
    db.commit()
    return ShortTermWatcherDecisionResult(
        status_code=order_result.status_code,
        payload={
            "approved": submission_accepted,
            "dry_run": False,
            "reason_code": outcome,
            "candidate": _candidate_payload(refreshed_item),
            "order": serialize_broker_order(persisted_order, include_events=True) if persisted_order else order_result.payload.get("order"),
            "monitoring": materialized_monitoring,
            "monitoring_preview": monitoring_preview,
            "safety": safety,
        },
    )


def _load_candidate(*, settings: Settings, db: Session, execution_plan_item_id: str) -> ExecutionPlanItem | None:
    return db.scalar(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(ExecutionPlanItem.id == execution_plan_item_id, ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionPlanItem.plan))
    )


def _validate_candidate(*, item: ExecutionPlanItem) -> tuple[str, str] | None:
    if item.side != "buy" or item.intent not in SUPPORTED_SHORT_TERM_BUY_INTENTS:
        return (
            "unsupported_candidate",
            "short-term watcher can only approve supported short-term buy candidates",
        )
    if (item.decision_agent_name or "").strip().lower() != SHORT_TERM_DECIDER_NAME:
        return "unsupported_decider", "candidate is not owned by short-term-decider"
    return None


def _validate_agent_output(*, command: ShortTermWatcherDecisionRequest) -> tuple[str, str] | None:
    output = command.agent_output
    if output.decision == "reject":
        return (output.reason_code or "agent_rejected", "agent decision rejected candidate")
    if output.order_type != "market":
        return "unsupported_order_type", "short-term watcher live buys only support market orders"
    if output.quantity is None and output.max_notional is None:
        return "missing_sizing", "approved decision requires quantity or max_notional"
    if output.target_price is None or output.stop_price is None:
        return "missing_monitoring", "approved decision requires target_price and stop_price"
    if output.target_price <= output.stop_price:
        return "invalid_monitoring_prices", "target_price must be greater than stop_price"
    return None


def _validate_account_context(*, context: dict[str, Any]) -> tuple[str, str] | None:
    if context["runtime_position_quantity"] > 0 or context["account_holding_quantity"] > 0:
        return "existing_holding", "candidate already has a holding/runtime position"
    if context["open_buy_order_count"] > 0:
        return "duplicate_open_order", "candidate already has an open buy order"
    requested_notional = Decimal(str(context["requested_notional"]))
    available_cash = Decimal(str(context["available_cash"]))
    if requested_notional > available_cash:
        return "insufficient_cash", "available cash is below requested notional"
    return None


def _validate_risk_rails(*, command: ShortTermWatcherDecisionRequest, context: dict[str, Any]) -> tuple[str, str] | None:
    requested_notional = Decimal(str(context["requested_notional"]))
    if requested_notional <= 0:
        return "invalid_sizing", "requested notional must be positive"
    envelope = context.get("allowed_action_envelope")
    if isinstance(envelope, dict):
        hard_reject_reasons = envelope.get("hard_reject_reasons")
        if isinstance(hard_reject_reasons, list) and hard_reject_reasons:
            return "allowed_action_envelope_blocked", "allowed action envelope does not permit a buy"
        max_quantity = int(envelope.get("max_quantity") or 0)
        max_notional = Decimal(str(envelope.get("max_notional_krw") or "0"))
        if max_quantity <= 0 or max_notional <= 0:
            return "allowed_action_envelope_blocked", "allowed action envelope left no buyable quantity"
        market_limited = bool(envelope.get("market_regime_limited"))
        resolved_quantity = int(context.get("resolved_quantity") or 0)
        if not market_limited and resolved_quantity > max_quantity:
            return "allowed_action_envelope_exceeded", "requested quantity exceeds allowed action envelope"
        if not market_limited and requested_notional > max_notional:
            return "allowed_action_envelope_exceeded", "requested notional exceeds allowed action envelope"
    max_notional = command.agent_output.max_notional or MAX_APPROVED_NOTIONAL
    if max_notional > MAX_APPROVED_NOTIONAL:
        return "risk_notional_too_large", "max_notional exceeds hard short-term watcher rail"
    if requested_notional > max_notional:
        return "risk_notional_exceeded", "requested notional exceeds approved max_notional"
    return None


def _candidate_context(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    command: ShortTermWatcherDecisionRequest,
    account_context,
    broker_account_snapshot: BrokerAccountSnapshot | None = None,
) -> dict[str, Any]:
    reference_price = command.agent_output.reference_price or item.limit_price or item.trigger_price
    if reference_price is None:
        reference_price = account_context.last_price
    quantity = _resolve_quantity(command=command, reference_price=reference_price)
    requested_notional = (reference_price * Decimal(quantity or 0)).quantize(Decimal("0.0001"))
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.mode == settings.runtime_mode, ExecutionPosition.symbol == item.symbol))
    open_orders = db.scalars(
        select(BrokerOrder).where(
            *active_local_broker_order_filters(
                settings=settings,
                statuses=OPEN_ORDER_STATUSES,
            ),
            BrokerOrder.symbol == item.symbol,
            BrokerOrder.side == "buy",
        )
    ).all()
    return {
        "symbol": item.symbol,
        "reference_price": str(reference_price),
        "resolved_quantity": quantity,
        "requested_notional": str(requested_notional),
        "available_cash": str(account_context.available_cash),
        "runtime_position_quantity": position.quantity if position is not None else 0,
        "account_holding_quantity": account_context.holdings.get(item.symbol, 0),
        "open_buy_order_count": len(open_orders) + account_context.open_order_symbols.count(item.symbol),
        "broker_account_source": broker_account_snapshot.source if broker_account_snapshot is not None else "request",
        "open_order_ids": [order.id for order in open_orders],
        "market_regime_context": merge_live_market_regime_context(
            market_regime_context_from_item_payloads(item.risk_context, item.quantity_policy),
            load_live_market_regime_context(settings=settings),
        ),
        "market_date": item.plan.market_date.isoformat() if item.plan is not None and item.plan.market_date else None,
    }


def _allowed_action_envelope(*, settings: Settings, context: dict[str, Any], command: ShortTermWatcherDecisionRequest) -> dict[str, Any]:
    reference_price = Decimal(str(context["reference_price"]))
    available_cash = Decimal(str(context["available_cash"]))
    resolved_quantity = int(context["resolved_quantity"] or 0)
    requested_notional = Decimal(str(context["requested_notional"]))
    hard_reject_reasons: list[str] = []
    soft_warnings: list[str] = []
    if context["runtime_position_quantity"] > 0 or context["account_holding_quantity"] > 0:
        hard_reject_reasons.append("existing_holding")
    if context["open_buy_order_count"] > 0:
        hard_reject_reasons.append("duplicate_open_buy_order")
    if reference_price <= 0:
        hard_reject_reasons.append("missing_reference_price")
    cash_limited_quantity = int((available_cash / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN)) if reference_price > 0 else 0
    max_notional = min(available_cash, command.agent_output.max_notional or MAX_APPROVED_NOTIONAL, MAX_APPROVED_NOTIONAL)
    market_guardrail = evaluate_market_regime_guardrail(
        settings=settings,
        context=context.get("market_regime_context") if isinstance(context.get("market_regime_context"), dict) else None,
        market_date=context.get("market_date"),
        original_notional=max_notional,
        reference_price=reference_price,
        account_context=None,
    )
    if market_guardrail.rejected:
        hard_reject_reasons.append(market_guardrail.reason_code)
        max_notional = Decimal("0")
    elif market_guardrail.action == "downsize":
        adjusted = _decimal(market_guardrail.audit.get("adjusted_notional_krw") or "0")
        max_notional = min(max_notional, adjusted).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        soft_warnings.append(market_guardrail.reason_code)
    max_quantity = int((max_notional / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN)) if reference_price > 0 else 0
    if max_quantity <= 0:
        hard_reject_reasons.append("no_buyable_quantity")
    if requested_notional > max_notional or resolved_quantity > max_quantity:
        soft_warnings.append("agent_requested_above_envelope")
    allowed_actions = ["hold", "reject"]
    if not hard_reject_reasons and max_quantity > 0:
        allowed_actions.extend(["buy", "downsize"])
    return {
        "schema_version": "allowed_action_envelope_v1",
        "allowed_actions": allowed_actions,
        "max_quantity": max(0, max_quantity),
        "max_notional_krw": f"{max_notional.quantize(Decimal('0.0001'), rounding=ROUND_DOWN):.4f}",
        "cash_limited_quantity": cash_limited_quantity,
        "requested_quantity": resolved_quantity,
        "requested_notional_krw": str(requested_notional),
        "existing_position_quantity": context["runtime_position_quantity"] + context["account_holding_quantity"],
        "open_buy_order_count": context["open_buy_order_count"],
        "hard_reject_reasons": hard_reject_reasons,
        "soft_warnings": soft_warnings,
        "market_regime_guardrail": market_guardrail.audit,
        "market_regime_limited": market_guardrail.action == "downsize",
        "computed_at": datetime.now(UTC).isoformat(),
    }


def _resolve_quantity(*, command: ShortTermWatcherDecisionRequest, reference_price: Decimal) -> int | None:
    if command.agent_output.quantity is not None:
        return command.agent_output.quantity
    if command.agent_output.max_notional is None or reference_price <= 0:
        return None
    quantity = int((command.agent_output.max_notional / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN))
    return quantity if quantity > 0 else None


def _order_preview(*, item: ExecutionPlanItem, command: ShortTermWatcherDecisionRequest, quantity: int) -> dict[str, object]:
    exchange = "KRX"
    if isinstance(item.risk_context, dict) and isinstance(item.risk_context.get("exchange"), str):
        exchange = item.risk_context["exchange"].strip().upper() or "KRX"
    return {
        "idempotency_key": f"short-term-watcher:buy:{item.id}",
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "side": "buy",
        "quantity": quantity,
        "order_type": command.agent_output.order_type,
        "limit_price": None,
        "time_in_force": item.time_in_force or "day",
        "exchange": exchange,
    }


def _monitoring_preview(*, item: ExecutionPlanItem, command: ShortTermWatcherDecisionRequest, quantity: int) -> list[dict[str, object]]:
    output = command.agent_output
    return [
        {"symbol": item.symbol, "side": "sell", "intent": "target_sell", "trigger_price": str(output.target_price), "quantity_policy": {"kind": "shares", "quantity": quantity}},
        {"symbol": item.symbol, "side": "sell", "intent": "stop_sell", "trigger_price": str(output.stop_price), "quantity_policy": {"kind": "shares", "quantity": quantity}},
    ]


ACCEPTED_ORDER_STATUSES = frozenset(
    {
        "submitted",
        "open",
        "partially_filled",
        "filled",
        "cancel_pending",
        "canceled",
        "expired",
    }
)


def _order_submission_accepted(*, order_result: object, order: BrokerOrder | None) -> bool:
    status_code = getattr(order_result, "status_code", 500)
    if not isinstance(status_code, int) or status_code >= 300:
        return False
    if order is None:
        return True
    return order.status in ACCEPTED_ORDER_STATUSES


def _materialize_monitoring_items(*, db: Session, item: ExecutionPlanItem, command: ShortTermWatcherDecisionRequest, quantity: int) -> list[dict[str, object]]:
    existing = db.scalars(
        select(ExecutionPlanItem).where(
            ExecutionPlanItem.execution_plan_id == item.execution_plan_id,
            ExecutionPlanItem.symbol == item.symbol,
            ExecutionPlanItem.intent.in_(("target_sell", "stop_sell")),
            ExecutionPlanItem.source_plan_item_id == item.source_plan_item_id,
        )
    ).all()
    existing_intents = {existing_item.intent for existing_item in existing}
    next_index = max((plan_item.plan_item_index for plan_item in item.plan.items), default=0) + 1
    materialized: list[dict[str, object]] = []
    for preview in _monitoring_preview(item=item, command=command, quantity=quantity):
        if preview["intent"] in existing_intents:
            continue
        monitoring_item = ExecutionPlanItem(
            execution_plan_id=item.execution_plan_id,
            plan_item_index=next_index,
            symbol=item.symbol,
            side="sell",
            intent=str(preview["intent"]),
            status=f"{preview['intent']}_monitoring",
            quantity_policy=preview["quantity_policy"],
            trigger_price=_decimal(preview["trigger_price"]),
            time_in_force=item.time_in_force,
            risk_context={"materialized_from": "short_term_watcher", "entry_plan_item_id": item.id},
            decision_agent_name=item.decision_agent_name,
            decision_snapshot_id=item.decision_snapshot_id,
            decision_run_id=item.decision_run_id,
            source_plan_item_id=item.source_plan_item_id,
            source_market_date=item.source_market_date,
        )
        db.add(monitoring_item)
        db.flush()
        materialized.append(_monitoring_item_payload(monitoring_item))
        next_index += 1
    return materialized


def _monitoring_item_payload(item: ExecutionPlanItem) -> dict[str, object]:
    return {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "side": item.side,
        "intent": item.intent,
        "status": item.status,
        "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
        "quantity_policy": item.quantity_policy,
    }


def _persist_audit(*, item: ExecutionPlanItem, audit: dict[str, Any]) -> None:
    risk_context = dict(item.risk_context or {})
    risk_context["short_term_watcher_exec_audit"] = audit
    item.risk_context = risk_context


def _candidate_payload(item: ExecutionPlanItem) -> dict[str, object]:
    return {"execution_plan_item_id": item.id, "symbol": item.symbol, "side": item.side, "intent": item.intent, "status": item.status}


def _load_order(*, settings: Settings, db: Session, idempotency_key: str) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .where(BrokerOrder.mode == settings.runtime_mode, BrokerOrder.idempotency_key == idempotency_key)
        .options(selectinload(BrokerOrder.events), selectinload(BrokerOrder.plan_item).selectinload(ExecutionPlanItem.plan))
    )


def _rejected(status_code: int, code: str, message: str) -> ShortTermWatcherDecisionResult:
    return ShortTermWatcherDecisionResult(status_code=status_code, payload={"approved": False, "reason_code": code, "message": message})


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid decimal: {value}") from exc
