from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest, ShortTermWatcherDecisionRequest
from app.broker.base import BrokerAdapter, BrokerAdapterError, BrokerAccountSnapshot
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_orders import place_broker_order
from app.services.execution_api import serialize_broker_order
from app.services.execution_account_route import (
    KIS_ACCOUNT_ROUTE_BINDING_KEY,
    kis_account_route_binding_from_payloads,
    validate_kis_account_route,
)
from app.services.fifo_inventory import fifo_route_scope_from_order
from app.services.live_market_context import load_live_market_regime_context, merge_live_market_regime_context
from app.services.market_regime_guardrails import evaluate_market_regime_guardrail, market_regime_context_from_item_payloads
from app.services.broker_order_state import (
    CAPITAL_RESERVING_ORDER_STATUSES,
    active_local_broker_order_filters,
)
from app.services.runtime_controls import (
    automated_buy_submission_allowed,
    automated_buy_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_position_identity,
)
from app.services.fill_economics import effective_fill_quantity
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.plan_item_identity import (
    plan_item_decision_agent_name,
    plan_item_entry_family_key,
    plan_item_inventory_scope_key,
    plan_item_source_plan_item_id,
)

OPEN_ORDER_STATUSES = frozenset(CAPITAL_RESERVING_ORDER_STATUSES)
SUPPORTED_SHORT_TERM_BUY_INTENTS = frozenset({"next_open_buy", "intraday_watch_buy"})
SHORT_TERM_DECIDER_NAME = "short-term-decider"
MAX_APPROVED_NOTIONAL = Decimal("10000000")


@dataclass(frozen=True)
class ShortTermWatcherDecisionResult:
    status_code: int
    payload: dict[str, object]


class InvalidEntryRouteError(ValueError):
    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


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
    candidate_rejection = _validate_candidate(item=item)
    account_context = command.account_context
    broker_account_snapshot = None
    if (
        candidate_rejection is None
        and not command.dry_run
        and automated_buy_submission_allowed(safety_state)
    ):
        try:
            session_state = ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
                source="short_term_watcher",
                reason=f"load live account context for {item.id}",
            )
            broker_account_snapshot = adapter.get_account_snapshot(session_state.access_token or "")
            if (
                not broker_account_snapshot.holdings_complete
                or not broker_account_snapshot.open_orders_complete
            ):
                return ShortTermWatcherDecisionResult(
                    status_code=503,
                    payload={
                        "approved": False,
                        "dry_run": command.dry_run,
                        "reason_code": "broker_account_context_incomplete",
                        "message": (
                            "broker holdings and open-order truth must be complete "
                            "before a live watcher decision"
                        ),
                        "candidate": _candidate_payload(item),
                        "safety": safety,
                        "account_snapshot": {
                            "source": broker_account_snapshot.source,
                            "observed_at": broker_account_snapshot.observed_at.isoformat(),
                            "holdings_complete": broker_account_snapshot.holdings_complete,
                            "open_orders_complete": broker_account_snapshot.open_orders_complete,
                            "diagnostics": dict(broker_account_snapshot.diagnostics),
                        },
                    },
                )
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

    rejection = candidate_rejection
    if rejection is None:
        rejection = _validate_agent_output(command=command)
    if rejection is None:
        rejection = _validate_account_context(context=context)
    if rejection is None:
        rejection = _validate_risk_rails(command=command, context=context)
    if (
        rejection is None
        and not command.dry_run
        and not automated_buy_submission_allowed(safety_state)
    ):
        block_reason = automated_buy_submission_block_reason(safety_state)
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
    order_preview = _order_preview(
        item=item,
        command=command,
        quantity=quantity,
        reference_price=Decimal(str(context["reference_price"])),
    )
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

    try:
        inherited_exit_route = _validated_entry_route_context(
            settings=settings,
            item=item,
        )
    except InvalidEntryRouteError as exc:
        message = (
            "KIS entry route binding is unavailable; regenerate the candidate "
            "before submission"
        )
        _persist_audit(
            item=item,
            audit={
                **audit,
                "outcome": "rejected",
                "reason_code": exc.reason_code,
                "message": message,
            },
        )
        db.commit()
        return ShortTermWatcherDecisionResult(
            status_code=409,
            payload={
                "approved": False,
                "dry_run": False,
                "reason_code": exc.reason_code,
                "message": message,
                "candidate": _candidate_payload(item),
                "context": context,
                "safety": safety,
            },
        )

    # Persist the already-validated agent geometry before the broker service
    # establishes its durable order intent. The final live-buy boundary reloads
    # this row under lock and must never depend on ephemeral request state.
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    order_command = PlaceOrderRequest(**order_preview)
    item.risk_context = {
        **risk_context,
        "target_price": str(command.agent_output.target_price),
        "stop_price": str(command.agent_output.stop_price),
        "short_term_watcher_entry_geometry": {
            "source": "validated_short_term_watcher_agent_output",
            "target_price": str(command.agent_output.target_price),
            "stop_price": str(command.agent_output.stop_price),
            "validated_at": now.isoformat(),
        },
        "short_term_watcher_order_authority": {
            "policy_version": 1,
            "source": "short-term-watcher:decision",
            "command": order_command.model_dump(mode="json"),
        },
    }
    db.flush()
    order_result = place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,
        command=order_command,
        request_source="short-term-watcher:decision",
    )
    result_order = order_result.payload.get("order")
    result_order_key = (
        result_order.get("idempotency_key")
        if isinstance(result_order, dict)
        and isinstance(result_order.get("idempotency_key"), str)
        else None
    )
    persisted_order = _load_order(
        settings=settings,
        db=db,
        idempotency_key=result_order_key or order_preview["idempotency_key"],
    )
    refreshed_item = db.get(
        ExecutionPlanItem,
        item.id,
        options=[selectinload(ExecutionPlanItem.plan)],
        populate_existing=True,
    ) or item
    submission_accepted = _order_submission_accepted(order_result=order_result, order=persisted_order)
    materialized_monitoring = []
    outcome = "submitted" if submission_accepted else "submission_not_accepted"
    if submission_accepted:
        materialized_monitoring = _materialize_monitoring_items(
            db=db,
            item=refreshed_item,
            command=command,
            quantity=quantity,
            inherited_route=inherited_exit_route,
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
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = str(risk_context.get("exchange") or "KRX").strip().upper()
    if (
        str(item.provider or "kis").strip().lower() != "kis"
        or str(item.asset_class or "stock").strip().lower() != "stock"
        or exchange != "KRX"
    ):
        return (
            "unsupported_execution_route",
            "short-term watcher decisions support only KIS KRX equities",
        )
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
    if context["same_lane_position_quantity"] > 0:
        return "existing_holding", "candidate already has a holding/runtime position"
    if context["same_lane_open_buy_order_count"] > 0:
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
    position = _load_route_scoped_position(
        settings=settings,
        db=db,
        item=item,
        broker_account_id=(
            broker_account_snapshot.broker_account_id
            if broker_account_snapshot is not None
            else None
        ),
    )
    open_orders = _load_route_scoped_open_buy_orders(
        settings=settings,
        db=db,
        item=item,
        broker_account_id=(
            broker_account_snapshot.broker_account_id
            if broker_account_snapshot is not None
            else None
        ),
    )
    runtime_position_quantity = position.quantity if position is not None else 0
    account_holding_quantity = account_context.holdings.get(item.symbol, 0)
    aggregate_holding_quantity = max(
        runtime_position_quantity,
        account_holding_quantity,
    )
    same_lane_position_quantity = _same_lane_holding_quantity(
        settings=settings,
        db=db,
        item=item,
        aggregate_quantity=aggregate_holding_quantity,
        observed_at=(
            broker_account_snapshot.observed_at
            if broker_account_snapshot is not None
            else datetime.now(UTC)
        ),
    )
    (
        open_buy_order_count,
        same_lane_open_buy_order_count,
        open_buy_notional,
    ) = _open_buy_order_context(
        item=item,
        local_orders=open_orders,
        request_open_order_symbols=account_context.open_order_symbols,
        broker_account_snapshot=broker_account_snapshot,
        reference_price=reference_price,
    )
    existing_symbol_exposure = (
        Decimal(aggregate_holding_quantity) * reference_price + open_buy_notional
    ).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    return {
        "symbol": item.symbol,
        "reference_price": str(reference_price),
        "resolved_quantity": quantity,
        "requested_notional": str(requested_notional),
        "available_cash": str(account_context.available_cash),
        "runtime_position_quantity": runtime_position_quantity,
        "account_holding_quantity": account_holding_quantity,
        "same_lane_position_quantity": same_lane_position_quantity,
        "open_buy_order_count": open_buy_order_count,
        "same_lane_open_buy_order_count": same_lane_open_buy_order_count,
        "existing_symbol_exposure": str(existing_symbol_exposure),
        "broker_account_source": broker_account_snapshot.source if broker_account_snapshot is not None else "request",
        "open_order_ids": [order.id for order in open_orders],
        "market_regime_context": merge_live_market_regime_context(
            market_regime_context_from_item_payloads(item.risk_context, item.quantity_policy),
            load_live_market_regime_context(settings=settings),
        ),
        "market_date": item.plan.market_date.isoformat() if item.plan is not None and item.plan.market_date else None,
    }


def _load_route_scoped_position(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    broker_account_id: str | None,
) -> ExecutionPosition | None:
    identity = _entry_route_identity(
        settings=settings,
        item=item,
        broker_account_id=broker_account_id,
    )
    return db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == identity.provider,
            ExecutionPosition.account_scope == identity.account_scope,
            ExecutionPosition.exchange == identity.exchange,
            ExecutionPosition.symbol == item.symbol,
        )
    )


def _load_route_scoped_open_buy_orders(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    broker_account_id: str | None,
) -> list[BrokerOrder]:
    identity = _entry_route_identity(
        settings=settings,
        item=item,
        broker_account_id=broker_account_id,
    )
    candidates = db.scalars(
        select(BrokerOrder).where(
            *active_local_broker_order_filters(
                settings=settings,
                statuses=OPEN_ORDER_STATUSES,
            ),
            BrokerOrder.symbol == item.symbol,
            BrokerOrder.side == "buy",
        ).options(
            selectinload(BrokerOrder.plan_item),
            selectinload(BrokerOrder.fills),
        )
    ).all()
    scoped_orders: list[BrokerOrder] = []
    for order in candidates:
        route_scope = fifo_route_scope_from_order(order)
        if (
            route_scope is None
            or (
                route_scope.provider == identity.provider
                and route_scope.account_scope == identity.account_scope
                and route_scope.exchange == identity.exchange
            )
        ):
            scoped_orders.append(order)
    return scoped_orders


def _normalized_decision_lane(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().casefold()


def _same_lane_holding_quantity(
    *,
    settings: Settings,
    db: Session,
    item: ExecutionPlanItem,
    aggregate_quantity: int,
    observed_at: datetime,
) -> int:
    if aggregate_quantity <= 0:
        return 0
    from app.services.monitoring import _post_entry_family_inventories

    inventories, lineage_found = _post_entry_family_inventories(
        settings=settings,
        db=db,
        symbol=item.symbol,
        position_quantity=aggregate_quantity,
        through=observed_at,
    )
    if not lineage_found or not inventories:
        return aggregate_quantity
    candidate_lane = _normalized_decision_lane(
        plan_item_decision_agent_name(item)
    )
    if candidate_lane is None:
        return aggregate_quantity
    lane_quantity = 0
    attributed_quantity = 0
    for inventory in inventories:
        inventory_lane = _normalized_decision_lane(
            plan_item_decision_agent_name(inventory.entry_item)
        )
        if inventory_lane is None:
            return aggregate_quantity
        attributed_quantity += inventory.remaining_quantity
        if inventory_lane == candidate_lane:
            lane_quantity += inventory.remaining_quantity
    if attributed_quantity != aggregate_quantity:
        return aggregate_quantity
    return lane_quantity


def _open_buy_order_context(
    *,
    item: ExecutionPlanItem,
    local_orders: list[BrokerOrder],
    request_open_order_symbols: list[str],
    broker_account_snapshot: BrokerAccountSnapshot | None,
    reference_price: Decimal,
) -> tuple[int, int, Decimal]:
    candidate_lane = _normalized_decision_lane(
        plan_item_decision_agent_name(item)
    )
    total_count = 0
    same_lane_count = 0
    total_notional = Decimal("0")
    local_orders = [
        order
        for order in local_orders
        if _local_order_remaining_quantity(order) != 0
    ]
    local_by_broker_id = _unique_local_order_index(
        local_orders,
        key=lambda order: order.broker_order_id,
    )
    local_by_idempotency = _unique_local_order_index(
        local_orders,
        key=lambda order: order.idempotency_key,
    )
    local_by_source_lane_scope = _unique_local_order_composite_index(
        local_orders,
        key=_local_order_source_lane_scope_key,
    )
    for order in local_orders:
        exact_remaining_quantity = _local_order_remaining_quantity(order)
        if exact_remaining_quantity == 0:
            continue
        quantity = exact_remaining_quantity
        if quantity is None:
            quantity = _conservative_local_order_quantity(order)
        total_count += 1
        order_lane = _normalized_decision_lane(
            plan_item_decision_agent_name(order.plan_item)
            if order.plan_item is not None
            else None
        )
        if quantity is None:
            # An order with unknown capital usage blocks every lane.  Guessing
            # one share would understate aggregate symbol exposure.
            same_lane_count += 1
        elif candidate_lane is None or order_lane is None or order_lane == candidate_lane:
            same_lane_count += 1
        price = order.limit_price or reference_price
        if quantity is not None and quantity > 0 and price > 0:
            total_notional += Decimal(quantity) * Decimal(price)

    if broker_account_snapshot is not None:
        for broker_order in broker_account_snapshot.open_orders:
            if (
                broker_order.symbol != item.symbol
                or str(broker_order.side or "").strip().lower() != "buy"
            ):
                continue
            quantity = (
                broker_order.remaining_quantity
                if broker_order.remaining_quantity is not None
                else broker_order.quantity
            ) or 0
            if quantity <= 0:
                continue
            matched = None
            identity_contradiction = False
            if broker_order.broker_order_id:
                candidate = local_by_broker_id.get(broker_order.broker_order_id)
                if candidate is not None:
                    identity_contradiction = _broker_local_identity_contradicts(
                        broker_order,
                        candidate,
                    )
                    if not identity_contradiction:
                        matched = candidate
            if (
                matched is None
                and not identity_contradiction
                and broker_order.idempotency_key
            ):
                candidate = local_by_idempotency.get(broker_order.idempotency_key)
                if candidate is not None:
                    identity_contradiction = _broker_local_identity_contradicts(
                        broker_order,
                        candidate,
                    )
                    if not identity_contradiction:
                        matched = candidate
            if (
                matched is None
                and not identity_contradiction
                and broker_order.source_candidate_id
            ):
                source_lane_scope_key = _broker_order_source_lane_scope_key(
                    broker_order
                )
                if source_lane_scope_key is not None:
                    candidate = local_by_source_lane_scope.get(
                        source_lane_scope_key
                    )
                    if (
                        candidate is not None
                        and not _broker_local_identity_contradicts(
                            broker_order,
                            candidate,
                        )
                    ):
                        matched = candidate
            if matched is not None:
                continue
            total_count += 1
            # Broker-only orders have no trusted decision lineage and therefore
            # remain a same-lane duplicate blocker.
            same_lane_count += 1
            price = broker_order.limit_price or reference_price
            total_notional += Decimal(quantity) * Decimal(price)
    else:
        unknown_count = request_open_order_symbols.count(item.symbol)
        total_count += unknown_count
        same_lane_count += unknown_count
    return total_count, same_lane_count, total_notional.quantize(
        Decimal("0.0001"),
        rounding=ROUND_DOWN,
    )


def _unique_local_order_index(
    orders: list[BrokerOrder],
    *,
    key: Callable[[BrokerOrder], str | None],
) -> dict[str, BrokerOrder]:
    candidates: dict[str, list[BrokerOrder]] = {}
    for order in orders:
        value = key(order)
        if isinstance(value, str) and value:
            candidates.setdefault(value, []).append(order)
    return {
        value: matches[0]
        for value, matches in candidates.items()
        if len(matches) == 1
    }


def _unique_local_order_composite_index(
    orders: list[BrokerOrder],
    *,
    key: Callable[[BrokerOrder], tuple[str, ...] | None],
) -> dict[tuple[str, ...], BrokerOrder]:
    candidates: dict[tuple[str, ...], list[BrokerOrder]] = {}
    for order in orders:
        value = key(order)
        if value is not None:
            candidates.setdefault(value, []).append(order)
    return {
        value: matches[0]
        for value, matches in candidates.items()
        if len(matches) == 1
    }


def _complete_execution_scope_key(payload: object) -> tuple[str, ...] | None:
    if not isinstance(payload, dict):
        return None
    scope = payload.get("execution_scope")
    if (
        not isinstance(scope, dict)
        or scope.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION
    ):
        return None
    account_scope = str(scope.get("account_scope") or "").strip().lower()
    if len(account_scope) != 24 or any(
        character not in "0123456789abcdef" for character in account_scope
    ):
        return None
    normalized = (
        str(scope.get("provider") or "").strip().lower(),
        account_scope,
        str(scope.get("venue") or "").strip().upper(),
        str(scope.get("asset_class") or "").strip().lower(),
        str(scope.get("exchange") or "").strip().upper(),
    )
    if not all(normalized):
        return None
    return normalized


def _local_order_source_lane_scope_key(
    order: BrokerOrder,
) -> tuple[str, ...] | None:
    plan_item = order.plan_item
    source = (
        plan_item_source_plan_item_id(plan_item)
        if plan_item is not None
        else None
    )
    lane = _normalized_decision_lane(
        plan_item_decision_agent_name(plan_item)
        if plan_item is not None
        else None
    )
    route_scope = fifo_route_scope_from_order(order)
    if not source or lane is None or route_scope is None:
        return None
    scope = route_scope.as_payload()
    return (
        source,
        lane,
        *tuple(scope[key] for key in (
            "provider",
            "account_scope",
            "venue",
            "asset_class",
            "exchange",
        )),
        str(order.symbol or "").strip().upper(),
        str(order.side or "").strip().lower(),
    )


def _broker_order_source_lane_scope_key(
    order: object,
) -> tuple[str, ...] | None:
    source = str(getattr(order, "source_candidate_id", None) or "").strip()
    payload = getattr(order, "payload", None)
    payload = payload if isinstance(payload, dict) else {}
    lane = _normalized_decision_lane(payload.get("decision_agent_name"))
    scope = _complete_execution_scope_key(payload)
    if not source or lane is None or scope is None:
        return None
    return (
        source,
        lane,
        *scope,
        str(getattr(order, "symbol", "") or "").strip().upper(),
        str(getattr(order, "side", "") or "").strip().lower(),
    )


def _broker_local_identity_contradicts(
    broker_order: object,
    local_order: BrokerOrder,
) -> bool:
    for attribute in ("broker_order_id", "idempotency_key"):
        broker_value = str(getattr(broker_order, attribute, None) or "").strip()
        local_value = str(getattr(local_order, attribute, None) or "").strip()
        if broker_value and local_value and broker_value != local_value:
            return True
    return False


def _local_order_remaining_quantity(order: BrokerOrder) -> int | None:
    ordered_quantity = order.quantity
    if isinstance(ordered_quantity, bool) or not isinstance(ordered_quantity, int):
        return None
    if ordered_quantity <= 0:
        return None
    filled_quantity = Decimal("0")
    try:
        fills = dedupe_execution_fills(list(order.fills))
        for fill in fills:
            if str(fill.side or "").strip().lower() != "buy":
                return None
            quantity = effective_fill_quantity(fill)
            if not quantity.is_finite() or quantity <= 0:
                return None
            filled_quantity += quantity
    except (InvalidOperation, TypeError, ValueError):
        return None
    if filled_quantity > Decimal(ordered_quantity):
        return None
    remaining = Decimal(ordered_quantity) - filled_quantity
    integral = remaining.to_integral_value(rounding=ROUND_DOWN)
    if integral != remaining:
        return None
    return int(integral)


def _conservative_local_order_quantity(order: BrokerOrder) -> int | None:
    quantity = order.quantity
    if isinstance(quantity, int) and not isinstance(quantity, bool) and quantity > 0:
        return quantity
    return None


def _entry_route_identity(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    broker_account_id: str | None,
):
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    raw_exchange = risk_context.get("exchange")
    exchange = str(raw_exchange or "").strip().upper()
    if not exchange and item.symbol.isdigit() and len(item.symbol) == 6:
        exchange = "KRX"
    if not exchange:
        raise PositionIdentityConflictError(
            "short-term watcher position lookup requires an exact exchange"
        )
    return build_position_identity(
        settings=settings,
        provider=str(item.provider or "kis"),
        exchange=exchange,
        broker_account_id=broker_account_id,
    )


def _allowed_action_envelope(*, settings: Settings, context: dict[str, Any], command: ShortTermWatcherDecisionRequest) -> dict[str, Any]:
    reference_price = Decimal(str(context["reference_price"]))
    available_cash = Decimal(str(context["available_cash"]))
    resolved_quantity = int(context["resolved_quantity"] or 0)
    requested_notional = Decimal(str(context["requested_notional"]))
    hard_reject_reasons: list[str] = []
    soft_warnings: list[str] = []
    if context["same_lane_position_quantity"] > 0:
        hard_reject_reasons.append("existing_holding")
    if context["same_lane_open_buy_order_count"] > 0:
        hard_reject_reasons.append("duplicate_open_buy_order")
    if reference_price <= 0:
        hard_reject_reasons.append("missing_reference_price")
    cash_limited_quantity = int((available_cash / reference_price).quantize(Decimal("1"), rounding=ROUND_DOWN)) if reference_price > 0 else 0
    existing_symbol_exposure = Decimal(str(context["existing_symbol_exposure"]))
    raw_account_basis = context.get("account_basis")
    try:
        account_basis = Decimal(str(raw_account_basis))
    except (InvalidOperation, TypeError, ValueError):
        account_basis = Decimal(
            str(settings.exec_agent_watch_sizing_fallback_account_basis_krw)
        )
    if not account_basis.is_finite() or account_basis <= 0:
        account_basis = Decimal(
            str(settings.exec_agent_watch_sizing_fallback_account_basis_krw)
        )
    gross_symbol_cap = min(
        MAX_APPROVED_NOTIONAL,
        Decimal(str(settings.exec_agent_max_symbol_budget)),
        account_basis
        * Decimal(str(settings.exec_agent_max_position_allocation_pct)),
    ).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    remaining_symbol_capacity = max(
        Decimal("0"),
        gross_symbol_cap - existing_symbol_exposure,
    )
    max_notional = min(
        available_cash,
        command.agent_output.max_notional or MAX_APPROVED_NOTIONAL,
        remaining_symbol_capacity,
    )
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
        "gross_symbol_cap_krw": f"{gross_symbol_cap:.4f}",
        "remaining_symbol_capacity_krw": (
            f"{remaining_symbol_capacity.quantize(Decimal('0.0001'), rounding=ROUND_DOWN):.4f}"
        ),
        "existing_symbol_exposure_krw": (
            f"{existing_symbol_exposure.quantize(Decimal('0.0001'), rounding=ROUND_DOWN):.4f}"
        ),
        "cash_limited_quantity": cash_limited_quantity,
        "requested_quantity": resolved_quantity,
        "requested_notional_krw": str(requested_notional),
        "existing_position_quantity": context["runtime_position_quantity"] + context["account_holding_quantity"],
        "same_lane_position_quantity": context["same_lane_position_quantity"],
        "open_buy_order_count": context["open_buy_order_count"],
        "same_lane_open_buy_order_count": context[
            "same_lane_open_buy_order_count"
        ],
        "existing_symbol_exposure": str(existing_symbol_exposure),
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


def _order_preview(
    *,
    item: ExecutionPlanItem,
    command: ShortTermWatcherDecisionRequest,
    quantity: int,
    reference_price: Decimal,
) -> dict[str, object]:
    exchange = "KRX"
    if isinstance(item.risk_context, dict) and isinstance(item.risk_context.get("exchange"), str):
        exchange = item.risk_context["exchange"].strip().upper() or "KRX"
    return {
        "idempotency_key": f"short-term-watcher:buy:{item.id}",
        "execution_plan_item_id": item.id,
        "provider": item.provider or "kis",
        "asset_class": item.asset_class or "stock",
        "venue": item.venue,
        "symbol": item.symbol,
        "side": "buy",
        "quantity": quantity,
        # The agent may express immediate-execution intent as ``market``, but
        # the executable order is always capped.  In live mode the broker
        # boundary refreshes this cap to the current ask only when that ask is
        # no worse than this already risk-checked reference.
        "order_type": "limit",
        "limit_price": str(reference_price),
        "time_in_force": "ioc",
        "exchange": exchange,
        "metadata": {
            "agent_order_intent": command.agent_output.order_type,
            "entry_price_cap_source": "validated_decision_reference_price",
        },
    }


def _monitoring_preview(*, item: ExecutionPlanItem, command: ShortTermWatcherDecisionRequest, quantity: int) -> list[dict[str, object]]:
    output = command.agent_output
    quantity_policy = {
        "kind": "shares",
        "quantity": quantity,
    }
    if item.source_plan_item_id:
        quantity_policy["linked_entry_item_key"] = item.source_plan_item_id
    return [
        {"symbol": item.symbol, "side": "sell", "intent": "target_sell", "trigger_price": str(output.target_price), "quantity_policy": dict(quantity_policy)},
        {"symbol": item.symbol, "side": "sell", "intent": "stop_sell", "trigger_price": str(output.stop_price), "quantity_policy": dict(quantity_policy)},
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


def _materialize_monitoring_items(
    *,
    db: Session,
    item: ExecutionPlanItem,
    command: ShortTermWatcherDecisionRequest,
    quantity: int,
    inherited_route: dict[str, object],
) -> list[dict[str, object]]:
    entry_risk_context = (
        dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    )
    entry_risk_context.setdefault(
        "max_holding_close_policy",
        {"enabled": True, "trading_sessions": 3},
    )
    item.risk_context = entry_risk_context
    existing_candidates = db.scalars(
        select(ExecutionPlanItem).where(
            ExecutionPlanItem.execution_plan_id == item.execution_plan_id,
            ExecutionPlanItem.symbol == item.symbol,
            ExecutionPlanItem.intent.in_(("target_sell", "stop_sell")),
        )
    ).all()
    entry_lane = _normalized_decision_lane(plan_item_decision_agent_name(item))
    entry_family = plan_item_entry_family_key(item)
    entry_route = plan_item_inventory_scope_key(item)
    if entry_route is None:
        raise InvalidEntryRouteError("account_route_binding_missing")
    existing = [
        candidate
        for candidate in existing_candidates
        if entry_lane is not None
        and entry_family is not None
        and _normalized_decision_lane(
            plan_item_decision_agent_name(candidate)
        )
        == entry_lane
        and plan_item_entry_family_key(candidate) == entry_family
        and plan_item_inventory_scope_key(candidate) == entry_route
    ]
    existing_intents = {existing_item.intent for existing_item in existing}
    next_index = (
        max((plan_item.plan_item_index for plan_item in item.plan.items), default=0)
        + 1
    )
    for existing_item in existing:
        _apply_entry_execution_route(
            monitor=existing_item,
            entry=item,
            inherited_route=inherited_route,
        )
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
            risk_context={
                "materialized_from": "short_term_watcher",
                "entry_plan_item_id": item.id,
                "entry_context": {
                    "source_plan_item_id": item.source_plan_item_id,
                    "execution_plan_item_id": item.id,
                    "decision_agent_name": item.decision_agent_name,
                },
            },
            decision_agent_name=item.decision_agent_name,
            decision_snapshot_id=item.decision_snapshot_id,
            decision_run_id=item.decision_run_id,
            source_plan_item_id=item.source_plan_item_id,
            source_market_date=item.source_market_date,
        )
        _apply_entry_execution_route(
            monitor=monitoring_item,
            entry=item,
            inherited_route=inherited_route,
        )
        db.add(monitoring_item)
        db.flush()
        materialized.append(_monitoring_item_payload(monitoring_item))
        next_index += 1
    return materialized


def _validated_entry_route_context(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    inherited: dict[str, object] = {}
    exchange = risk_context.get("exchange")
    if isinstance(exchange, str) and exchange.strip():
        inherited["exchange"] = exchange.strip().upper()

    if str(item.provider or "kis").strip().lower() != "kis":
        return inherited
    mismatch = validate_kis_account_route(
        settings=settings,
        provider=item.provider,
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
        require_binding=True,
    )
    binding = kis_account_route_binding_from_payloads(
        item.risk_context,
        item.quantity_policy,
    )
    if mismatch is not None or binding is None:
        # Entry and derived exits must share one already persisted account
        # identity. Never fabricate a replacement binding from current settings.
        reason = (
            mismatch.reason_code
            if mismatch is not None
            else "account_route_binding_missing"
        )
        raise InvalidEntryRouteError(reason)
    inherited[KIS_ACCOUNT_ROUTE_BINDING_KEY] = binding
    return inherited


def _apply_entry_execution_route(
    *,
    monitor: ExecutionPlanItem,
    entry: ExecutionPlanItem,
    inherited_route: dict[str, object],
) -> None:
    monitor.provider = entry.provider
    monitor.venue = entry.venue
    monitor.asset_class = entry.asset_class
    risk_context = (
        dict(monitor.risk_context)
        if isinstance(monitor.risk_context, dict)
        else {}
    )
    risk_context.update(inherited_route)
    monitor.risk_context = risk_context


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
