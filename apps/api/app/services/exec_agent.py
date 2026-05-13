from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.broker.base import (
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.plan_item_identity import serialize_plan_item_source_identity

SHORT_TERM_DECIDER_NAME = "short-term-decider"
SUPPORTED_SHORT_TERM_BUY_INTENTS = frozenset({"next_open_buy"})
EXEC_AGENT_CONTRACT_KEYS = (
    "exec_agent_decision",
    "short_term_exec_agent",
    "short_term_exec_agent_decision",
)
EXEC_AGENT_DECISION_VERSION = "short-term-exec-agent-v1"
OPEN_BUY_ORDER_STATUSES = frozenset(
    {
        "submission_pending",
        "submitted",
        "open",
        "partially_filled",
        "retry_pending",
        "cancel_pending",
    }
)
REJECTED_STATUS = "exec_agent_rejected"


class ShortTermExecAgentContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["approve", "reject"]
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    max_budget: Decimal | None = Field(default=None, gt=0)
    max_symbol_budget: Decimal | None = Field(default=None, gt=0)
    max_total_budget: Decimal | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    allow_existing_holding: bool = False
    reason_code: str | None = Field(default=None, min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=512)
    feedback_ids: list[str] = Field(default_factory=list)
    context: dict[str, Any] | None = None

    @field_validator("reason_code", "reason")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("feedback_ids")
    @classmethod
    def normalize_feedback_ids(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str):
                raise ValueError("feedback_ids must contain strings")
            stripped = value.strip()
            if not stripped:
                raise ValueError("feedback_ids must not contain empty strings")
            normalized.append(stripped)
        return normalized

    @model_validator(mode="after")
    def validate_action_payload(self) -> ShortTermExecAgentContract:
        if self.action == "reject" and self.reason_code is None:
            raise ValueError("reject action requires reason_code")
        if self.action == "approve":
            missing = [
                field
                for field in ("max_budget", "target_price", "stop_price")
                if getattr(self, field) is None
            ]
            if missing:
                raise ValueError(
                    "approve action requires " + ", ".join(missing)
                )
        return self


@dataclass(frozen=True)
class ExecAgentApplicationResult:
    evaluated_count: int
    approved_count: int
    rejected_count: int
    account_snapshot: dict[str, object] | None
    decisions: list[dict[str, object]]

    def payload(self) -> dict[str, object]:
        return {
            "version": EXEC_AGENT_DECISION_VERSION,
            "evaluated_count": self.evaluated_count,
            "approved_count": self.approved_count,
            "rejected_count": self.rejected_count,
            "account_snapshot": self.account_snapshot,
            "decisions": self.decisions,
        }


@dataclass(frozen=True)
class _AccountContext:
    available_cash: Decimal | None
    observed_at: datetime
    snapshot_payload: dict[str, object] | None
    holdings_by_symbol: dict[str, int]
    open_orders: list[BrokerAccountOpenOrder]


class _InvalidAgentContract(ValueError):
    pass


class _MissingAgentContract(ValueError):
    pass


def apply_short_term_exec_agent_decisions(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None,
    plan: ExecutionPlan,
) -> ExecAgentApplicationResult:
    candidates = [
        item
        for item in plan.items
        if _is_short_term_buy_candidate(item)
    ]
    if not candidates:
        return ExecAgentApplicationResult(
            evaluated_count=0,
            approved_count=0,
            rejected_count=0,
            account_snapshot=None,
            decisions=[],
        )

    account_context = _load_account_context(
        settings=settings,
        db=db,
        adapter=adapter,
    )
    decisions: list[dict[str, object]] = []
    approved_count = 0
    rejected_count = 0
    reserved_total_budget = Decimal("0")
    next_plan_item_index = _next_plan_item_index(plan)

    for item in candidates:
        decision = _decide_candidate(
            settings=settings,
            plan=plan,
            item=item,
            account_context=account_context,
            reserved_total_budget=reserved_total_budget,
        )
        decisions.append(decision)
        if decision["action"] == "approve":
            approved_count += 1
            reserved_total_budget += Decimal(str(decision["approved_notional"]))
            _apply_approved_decision(item=item, decision=decision)
            next_plan_item_index = _append_monitoring_items(
                plan=plan,
                buy_item=item,
                decision=decision,
                next_plan_item_index=next_plan_item_index,
            )
            continue
        if decision["action"] == "pending":
            continue

        rejected_count += 1
        _apply_rejected_decision(item=item, decision=decision)

    return ExecAgentApplicationResult(
        evaluated_count=len(candidates),
        approved_count=approved_count,
        rejected_count=rejected_count,
        account_snapshot=account_context.snapshot_payload,
        decisions=decisions,
    )


def _is_short_term_buy_candidate(item: ExecutionPlanItem) -> bool:
    agent_name = (item.decision_agent_name or "").strip().lower()
    return (
        agent_name == SHORT_TERM_DECIDER_NAME
        and item.side == "buy"
        and item.intent in SUPPORTED_SHORT_TERM_BUY_INTENTS
        and item.status in {"pending", "short_term_watcher_pending"}
    )


def _load_account_context(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None,
) -> _AccountContext:
    observed_at = datetime.now(UTC)
    if adapter is None:
        return _unavailable_account_context(
            observed_at=observed_at,
            reason="broker adapter is unavailable",
        )

    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="exec_agent_sizing",
            reason="fetch account snapshot before short-term candidate sizing",
        )
        snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError) as exc:
        return _unavailable_account_context(
            observed_at=observed_at,
            reason=f"{exc.__class__.__name__}: {exc}",
        )

    holdings_by_symbol = {
        holding.symbol: holding.quantity
        for holding in snapshot.holdings
        if holding.quantity > 0
    }
    for position in _load_local_positions(settings=settings, db=db):
        if position.quantity <= 0:
            continue
        holdings_by_symbol[position.symbol] = max(
            holdings_by_symbol.get(position.symbol, 0),
            position.quantity,
        )

    return _AccountContext(
        available_cash=snapshot.available_cash,
        observed_at=snapshot.observed_at,
        snapshot_payload=_serialize_account_snapshot(snapshot),
        holdings_by_symbol=holdings_by_symbol,
        open_orders=[*snapshot.open_orders, *_load_local_open_orders(settings=settings, db=db)],
    )


def _unavailable_account_context(
    *,
    observed_at: datetime,
    reason: str,
) -> _AccountContext:
    return _AccountContext(
        available_cash=None,
        observed_at=observed_at,
        snapshot_payload={
            "available": False,
            "reason": reason,
            "observed_at": observed_at.isoformat(),
        },
        holdings_by_symbol={},
        open_orders=[],
    )


def _load_local_positions(
    *,
    settings: Settings,
    db: Session,
) -> list[ExecutionPosition]:
    return db.scalars(
        select(ExecutionPosition).where(ExecutionPosition.mode == settings.runtime_mode)
    ).all()


def _load_local_open_orders(
    *,
    settings: Settings,
    db: Session,
) -> list[BrokerAccountOpenOrder]:
    orders = db.scalars(
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.status.in_(OPEN_BUY_ORDER_STATUSES),
            BrokerOrder.side == "buy",
        )
    ).all()
    return [
        BrokerAccountOpenOrder(
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            remaining_quantity=order.quantity,
            status=order.status,
            broker_order_id=order.broker_order_id,
            idempotency_key=order.idempotency_key,
            limit_price=order.limit_price,
            payload={
                "source": "local_broker_orders",
                "broker_order_pk": order.id,
                "execution_plan_item_id": order.execution_plan_item_id,
            },
        )
        for order in orders
    ]


def _decide_candidate(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    account_context: _AccountContext,
    reserved_total_budget: Decimal,
) -> dict[str, object]:
    source_identity = serialize_plan_item_source_identity(item)
    idempotency_key = _candidate_idempotency_key(plan=plan, item=item)
    base = {
        "version": EXEC_AGENT_DECISION_VERSION,
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "source_identity": source_identity,
        "idempotency_key": idempotency_key,
        "account_snapshot": account_context.snapshot_payload,
        "decided_at": datetime.now(UTC).isoformat(),
    }

    try:
        agent_contract = _load_agent_contract(item)
    except _MissingAgentContract as exc:
        return {
            **base,
            "action": "pending",
            "reason_code": "agent_output_missing",
            "reason": str(exc),
        }
    except _InvalidAgentContract as exc:
        return _reject(base, "invalid_agent_output", str(exc))

    if agent_contract.action == "reject":
        return _reject(
            base,
            agent_contract.reason_code or "agent_rejected",
            agent_contract.reason or "short-term exec agent rejected candidate",
            agent_contract=agent_contract,
        )
    if item.intent not in SUPPORTED_SHORT_TERM_BUY_INTENTS:
        return _reject(
            base,
            "unsupported_buy_intent",
            "short-term exec agent only supports next_open_buy candidates",
            agent_contract=agent_contract,
        )
    if plan.market_date is None:
        return _reject(
            base,
            "missing_market_date",
            "next_open_buy candidate requires plan market_date",
            agent_contract=agent_contract,
        )
    if item.trigger_price is not None or _has_chase_or_trigger_fields(item):
        return _reject(
            base,
            "unsupported_entry_trigger_or_chase",
            "short-term exec agent does not support trigger/chase buy semantics",
            agent_contract=agent_contract,
        )
    available_cash = account_context.available_cash
    if available_cash is None:
        return _reject(
            base,
            "account_snapshot_unavailable",
            "account cash, holdings, and open orders were unavailable",
            agent_contract=agent_contract,
        )
    if available_cash <= 0:
        return _reject(
            base,
            "insufficient_cash",
            "available cash must be positive before approving a buy",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )
    existing_holding = account_context.holdings_by_symbol.get(item.symbol, 0)
    if existing_holding > 0 and not agent_contract.allow_existing_holding:
        return _reject(
            base,
            "existing_holding",
            "existing holding blocks short-term add-on buy",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
        )
    if _has_duplicate_open_buy_order(
        item=item,
        idempotency_key=idempotency_key,
        open_orders=account_context.open_orders,
    ):
        return _reject(
            base,
            "duplicate_open_buy_order",
            "open buy order already exists for this source candidate",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )

    sizing_price = _sizing_price(item=item, agent_contract=agent_contract)
    if sizing_price is None:
        return _reject(
            base,
            "missing_reference_price",
            "market buy sizing requires a strict positive reference_price",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )

    max_budget = agent_contract.max_budget
    assert max_budget is not None
    budget = min(
        max_budget,
        agent_contract.max_symbol_budget or _settings_decimal(
            settings,
            "exec_agent_max_symbol_budget",
            Decimal("1000000"),
        ),
        available_cash,
    )
    total_cap = min(
        agent_contract.max_total_budget or _settings_decimal(
            settings,
            "exec_agent_max_total_budget",
            Decimal("3000000"),
        ),
        _settings_decimal(
            settings,
            "exec_agent_max_total_budget",
            Decimal("3000000"),
        ),
    )
    remaining_total_budget = total_cap - reserved_total_budget
    if remaining_total_budget <= 0:
        return _reject(
            base,
            "total_budget_cap_reached",
            "exec-agent total budget cap is already reserved",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )
    budget = min(budget, remaining_total_budget)
    quantity = int((budget / sizing_price).quantize(Decimal("1"), rounding=ROUND_DOWN))
    if quantity <= 0:
        return _reject(
            base,
            "insufficient_cash",
            "available cash and budget caps cannot buy one share",
            agent_contract=agent_contract,
            available_cash=available_cash,
            sizing_price=sizing_price,
        )

    approved_notional = sizing_price * Decimal(quantity)
    return {
        **base,
        "action": "approve",
        "reason_code": "approved",
        "reason": "short-term exec agent approved bounded market-open buy",
        "confidence": _serialize_decimal(agent_contract.confidence),
        "quantity": quantity,
        "order_type": "limit" if item.limit_price is not None else "market",
        "sizing_price": _serialize_decimal(sizing_price),
        "approved_notional": _serialize_decimal(approved_notional),
        "available_cash": _serialize_decimal(available_cash),
        "budget": _serialize_decimal(budget),
        "target_price": _serialize_decimal(agent_contract.target_price),
        "stop_price": _serialize_decimal(agent_contract.stop_price),
        "allow_existing_holding": agent_contract.allow_existing_holding,
        "feedback_ids": list(agent_contract.feedback_ids),
        "agent_contract": agent_contract.model_dump(mode="json"),
        "account_snapshot_observed_at": account_context.observed_at.isoformat(),
    }


def _load_agent_contract(item: ExecutionPlanItem) -> ShortTermExecAgentContract:
    for payload in (item.risk_context, item.quantity_policy):
        if not isinstance(payload, dict):
            continue
        for key in EXEC_AGENT_CONTRACT_KEYS:
            raw_contract = payload.get(key)
            if raw_contract is None:
                continue
            if not isinstance(raw_contract, dict):
                raise _InvalidAgentContract(f"{key} must be an object")
            try:
                return ShortTermExecAgentContract.model_validate(raw_contract)
            except ValidationError as exc:
                raise _InvalidAgentContract(_validation_error_message(exc)) from exc
    raise _MissingAgentContract(
        "missing strict exec_agent_decision contract in risk_context or quantity_policy"
    )


def _validation_error_message(exc: ValidationError) -> str:
    first = exc.errors()[0] if exc.errors() else None
    if first is None:
        return "invalid exec-agent decision contract"
    location = ".".join(str(part) for part in first.get("loc", ())) or "payload"
    message = str(first.get("msg") or "invalid value")
    return f"{location}: {message}"


def _reject(
    base: dict[str, object],
    reason_code: str,
    reason: str,
    *,
    agent_contract: ShortTermExecAgentContract | None = None,
    available_cash: Decimal | None = None,
    existing_holding: int | None = None,
    sizing_price: Decimal | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        **base,
        "action": "reject",
        "reason_code": reason_code,
        "reason": reason,
    }
    if agent_contract is not None:
        payload["confidence"] = _serialize_decimal(agent_contract.confidence)
        payload["feedback_ids"] = list(agent_contract.feedback_ids)
        payload["agent_contract"] = agent_contract.model_dump(mode="json")
    if available_cash is not None:
        payload["available_cash"] = _serialize_decimal(available_cash)
    if existing_holding is not None:
        payload["existing_holding"] = existing_holding
    if sizing_price is not None:
        payload["sizing_price"] = _serialize_decimal(sizing_price)
    return payload


def _apply_approved_decision(
    *,
    item: ExecutionPlanItem,
    decision: dict[str, object],
) -> None:
    existing_risk_context = dict(item.risk_context or {})
    existing_risk_context["exec_agent_decision"] = decision
    item.risk_context = existing_risk_context
    item.quantity_policy = {
        "kind": "shares",
        "quantity": decision["quantity"],
        "source": "exec_agent",
        "idempotency_key": decision["idempotency_key"],
        "sizing_price": decision["sizing_price"],
        "approved_notional": decision["approved_notional"],
        "confidence": decision.get("confidence"),
    }
    item.status = "pending"


def _apply_rejected_decision(
    *,
    item: ExecutionPlanItem,
    decision: dict[str, object],
) -> None:
    existing_risk_context = dict(item.risk_context or {})
    existing_risk_context["exec_agent_decision"] = decision
    item.risk_context = existing_risk_context
    item.status = REJECTED_STATUS


def _append_monitoring_items(
    *,
    plan: ExecutionPlan,
    buy_item: ExecutionPlanItem,
    decision: dict[str, object],
    next_plan_item_index: int,
) -> int:
    quantity = int(decision["quantity"])
    for intent, trigger_key in (
        ("target_sell", "target_price"),
        ("stop_sell", "stop_price"),
    ):
        trigger_price = Decimal(str(decision[trigger_key]))
        plan.items.append(
            ExecutionPlanItem(
                plan_item_index=next_plan_item_index,
                symbol=buy_item.symbol,
                side="sell",
                intent=intent,
                status="pending",
                quantity_policy={
                    "kind": "shares",
                    "quantity": quantity,
                    "source": "exec_agent_monitoring",
                    "parent_idempotency_key": decision["idempotency_key"],
                },
                trigger_price=trigger_price,
                time_in_force=buy_item.time_in_force or "day",
                risk_context={
                    "generated_by": EXEC_AGENT_DECISION_VERSION,
                    "parent_source_identity": serialize_plan_item_source_identity(buy_item),
                    "parent_idempotency_key": decision["idempotency_key"],
                    "confidence": decision.get("confidence"),
                },
                decision_agent_name=buy_item.decision_agent_name,
                decision_snapshot_id=buy_item.decision_snapshot_id,
                decision_run_id=buy_item.decision_run_id,
                source_plan_item_id=_monitoring_source_plan_item_id(buy_item, intent),
                source_market_date=buy_item.source_market_date or plan.market_date,
            )
        )
        next_plan_item_index += 1
    return next_plan_item_index


def _monitoring_source_plan_item_id(item: ExecutionPlanItem, intent: str) -> str | None:
    if item.source_plan_item_id:
        return f"{item.source_plan_item_id}:{intent}"
    return None


def _has_duplicate_open_buy_order(
    *,
    item: ExecutionPlanItem,
    idempotency_key: str,
    open_orders: list[BrokerAccountOpenOrder],
) -> bool:
    source_identity = serialize_plan_item_source_identity(item)
    source_candidate_id = source_identity.get("source_plan_item_id")
    for order in open_orders:
        if order.side != "buy" or order.remaining_quantity <= 0:
            continue
        if order.idempotency_key == idempotency_key:
            return True
        if source_candidate_id and order.source_candidate_id == source_candidate_id:
            return True
        if order.symbol == item.symbol and order.idempotency_key is None and order.source_candidate_id is None:
            return True
    return False


def _sizing_price(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> Decimal | None:
    if item.limit_price is not None and item.limit_price > 0:
        return item.limit_price
    if agent_contract.reference_price is not None and agent_contract.reference_price > 0:
        return agent_contract.reference_price
    return None


def _candidate_idempotency_key(
    *,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
) -> str:
    source = item.source_plan_item_id or str(item.plan_item_index)
    return f"short-term-exec-agent:{plan.external_plan_id}:{source}"


def _next_plan_item_index(plan: ExecutionPlan) -> int:
    if not plan.items:
        return 0
    return max(item.plan_item_index for item in plan.items) + 1


def _settings_decimal(settings: Settings, field_name: str, default: Decimal) -> Decimal:
    value = getattr(settings, field_name, default)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default
    if parsed <= 0:
        return default
    return parsed


def _serialize_account_snapshot(snapshot: BrokerAccountSnapshot) -> dict[str, object]:
    return {
        "available": True,
        "source": snapshot.source,
        "observed_at": snapshot.observed_at.isoformat(),
        "available_cash": _serialize_decimal(snapshot.available_cash),
        "holding_count": len(snapshot.holdings),
        "open_order_count": len(snapshot.open_orders),
        "holdings": [
            {
                "symbol": holding.symbol,
                "quantity": holding.quantity,
                "sellable_quantity": holding.sellable_quantity,
                "average_price": _serialize_decimal(holding.average_price),
            }
            for holding in snapshot.holdings
        ],
        "open_orders": [
            {
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "remaining_quantity": order.remaining_quantity,
                "status": order.status,
                "broker_order_id": order.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "source_candidate_id": order.source_candidate_id,
                "limit_price": _serialize_decimal(order.limit_price),
            }
            for order in snapshot.open_orders
        ],
    }


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.4f}"


def _has_chase_or_trigger_fields(item: ExecutionPlanItem) -> bool:
    payloads = [item.risk_context, item.quantity_policy]
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in (
            "chase_limit_price",
            "chase_limit_offset",
            "chase_limit_percent",
            "chase_limit_ticks",
            "max_chase_limit_price",
            "entry_limit_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "trigger_price",
        ):
            if payload.get(key) is not None:
                return True
    return False
