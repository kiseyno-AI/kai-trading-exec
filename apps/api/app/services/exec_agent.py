from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Literal, Protocol

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
from app.models.runtime import (
    BrokerOrder,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    RuntimeControlState,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.live_market_context import load_live_market_regime_context, merge_live_market_regime_context
from app.services.plan_item_identity import serialize_plan_item_source_identity
from app.services.risk_sizing import (
    RiskSizingRejection,
    RiskSizingResult,
    effective_stop_price,
    resolve_risk_based_buy_quantity,
)
from app.services.market_regime_guardrails import market_regime_context_from_item_payloads
from app.services.runtime_controls import RUNTIME_CONTROL_ID, effective_automation_enabled

SHORT_TERM_DECIDER_NAME = "short-term-decider"
SUPPORTED_SHORT_TERM_BUY_INTENTS = frozenset({"next_open_buy"})
TargetProfile = Literal["tactical_2pct", "short_swing", "standard_breakout", "extended_momentum"]
EXEC_AGENT_CONTRACT_KEYS = (
    "exec_agent_decision",
    "short_term_exec_agent",
    "short_term_exec_agent_decision",
)
EXEC_AGENT_DECISION_VERSION = "short-term-exec-agent-v1"
LOGGER = logging.getLogger(__name__)
OPENCLAW_EXEC_AGENT_ID = "exec-agent"
OPENCLAW_EXEC_AGENT_MODEL = "openai-codex/gpt-5.5"
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
CONVICTION_MULTIPLIERS = {
    "very_high": Decimal("1.00"),
    "highest": Decimal("1.00"),
    "high": Decimal("1.00"),
    "strong": Decimal("1.00"),
    "medium": Decimal("0.75"),
    "moderate": Decimal("0.75"),
    "normal": Decimal("0.75"),
    "low": Decimal("0.50"),
    "weak": Decimal("0.50"),
}
CONFIRMATION_SIGNAL_KEYS = (
    "confirmation_signal",
    "confirmation",
    "confirmed_signal",
    "entry_confirmation",
    "breakout",
    "vwap_recovery",
    "vwap_reclaim",
    "vwap_recovered",
    "trigger_hit",
    "trigger_price_hit",
    "liquidity_expansion",
    "liquidity_confirmed",
    "volume_expansion",
    "volume_confirmed",
    "momentum_continuation",
    "short_term_momentum",
)
CONFIRMATION_SIGNAL_VALUES = frozenset(
    {
        "breakout",
        "vwap_recovery",
        "vwap_reclaim",
        "trigger_hit",
        "trigger_price_hit",
        "liquidity_expansion",
        "liquidity_confirmed",
        "volume_expansion",
        "volume_confirmed",
        "momentum_continuation",
        "short_term_momentum",
        "momentum_continuation_confirmed",
        "deep_liquidity",
    }
)
RELAXED_REWARD_RISK_FLAGS = frozenset(
    {
        "high_confidence",
        "liquidity_confirmed",
        "liquidity_expansion",
        "volume_expansion",
        "volume_confirmed",
        "deep_liquidity",
        "breakout",
        "vwap_recovery",
        "momentum_continuation",
    }
)
ENTRY_LIMIT_KEYS = (
    "chase_limit_price",
    "max_chase_limit_price",
    "entry_limit_price",
    "planned_entry_price",
    "planned_entry_limit",
)


class ShortTermExecAgentContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["approve", "reject"]
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    max_budget: Decimal | None = Field(default=None, gt=0)
    max_symbol_budget: Decimal | None = Field(default=None, gt=0)
    max_total_budget: Decimal | None = Field(default=None, gt=0)
    max_quantity: int | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    reward_risk: Decimal | None = Field(default=None, gt=0)
    exposure_after_trade: Decimal | None = Field(default=None, ge=0)
    allow_existing_holding: bool = False
    special_tactical_scalp: bool = False
    target_profile: TargetProfile | None = None
    reason_code: str | None = Field(default=None, min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=512)
    feedback_ids: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
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

    @field_validator("feedback_ids", "risk_flags")
    @classmethod
    def normalize_text_list(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str):
                raise ValueError("items must contain strings")
            stripped = value.strip()
            if not stripped:
                raise ValueError("items must not contain empty strings")
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


class ExecAgentSizingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = OPENCLAW_EXEC_AGENT_ID
    model: str = OPENCLAW_EXEC_AGENT_MODEL
    version: str = EXEC_AGENT_DECISION_VERSION
    plan_id: str
    execution_plan_item_id: str
    symbol: str
    side: str
    intent: str
    source_identity: dict[str, object]
    candidate_context: dict[str, Any]
    decider_input: dict[str, Any] | None = None
    decider_output: dict[str, Any] | None = None
    account: dict[str, Any]
    portfolio: dict[str, Any]
    market_data: dict[str, Any] | None = None
    risk_caps: dict[str, Any]


class ExecAgentAllocationCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_plan_item_id: str
    symbol: str
    side: str
    intent: str
    source_identity: dict[str, object]
    candidate_context: dict[str, Any]
    decider_input: dict[str, Any] | None = None
    decider_output: dict[str, Any] | None = None
    market_data: dict[str, Any] | None = None


class ExecAgentBatchSizingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = OPENCLAW_EXEC_AGENT_ID
    model: str = OPENCLAW_EXEC_AGENT_MODEL
    version: str = EXEC_AGENT_DECISION_VERSION
    plan_id: str
    market_date: str | None = None
    strategy_run_id: str | None = None
    allocation_scope: Literal["next_open_buy"] = "next_open_buy"
    candidates: list[ExecAgentAllocationCandidate] = Field(min_length=1)
    account: dict[str, Any]
    portfolio: dict[str, Any]
    risk_caps: dict[str, Any]


class ExecAgentSizingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["approve", "reject"]
    quantity: int | None = Field(default=None, gt=0)
    max_quantity: int | None = Field(default=None, gt=0)
    max_budget: Decimal | None = Field(default=None, gt=0)
    final_budget: Decimal | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    reward_risk: Decimal | None = Field(default=None, gt=0)
    exposure_after_trade: Decimal | None = Field(default=None, ge=0)
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    reason_code: str | None = Field(default=None, min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=512)
    reasons: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    allow_existing_holding: bool = False
    special_tactical_scalp: bool = False
    target_profile: TargetProfile | None = None
    explanation: dict[str, Any] | None = None

    @field_validator("reasons", "risk_flags")
    @classmethod
    def normalize_text_list(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str):
                raise ValueError("items must be strings")
            stripped = value.strip()
            if not stripped:
                raise ValueError("items must not be empty")
            normalized.append(stripped)
        return normalized

    @model_validator(mode="after")
    def validate_decision(self) -> ExecAgentSizingResponse:
        if self.final_budget is None and self.max_budget is not None:
            self.final_budget = self.max_budget
        if self.quantity is None and self.max_quantity is not None:
            self.quantity = self.max_quantity
        if self.action == "approve":
            missing = [
                field
                for field in ("quantity", "final_budget", "reference_price", "target_price", "stop_price")
                if getattr(self, field) is None
            ]
            if missing:
                raise ValueError("approve action requires " + ", ".join(missing))
        return self


class ExecAgentAllocationDecision(ExecAgentSizingResponse):
    execution_plan_item_id: str


class ExecAgentBatchSizingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decisions: list[ExecAgentAllocationDecision] = Field(min_length=1)


class ExecAgentClient(Protocol):
    def size_candidates(self, request: ExecAgentBatchSizingRequest) -> ExecAgentBatchSizingResponse:
        """Return one portfolio-wide allocation for all supplied candidates or raise."""

    def size_candidate(self, request: ExecAgentSizingRequest) -> ExecAgentSizingResponse:
        """Legacy per-candidate fallback for clients not yet migrated to batch sizing."""


class ExecAgentUnavailable(RuntimeError):
    pass


class UnavailableExecAgentClient:
    def size_candidates(self, request: ExecAgentBatchSizingRequest) -> ExecAgentBatchSizingResponse:
        del request
        raise ExecAgentUnavailable("OpenClaw exec-agent transport is not configured")

    def size_candidate(self, request: ExecAgentSizingRequest) -> ExecAgentSizingResponse:
        del request
        raise ExecAgentUnavailable("OpenClaw exec-agent transport is not configured")


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
    buying_power: Decimal | None
    observed_at: datetime
    snapshot_payload: dict[str, object] | None
    holdings_by_symbol: dict[str, int]
    open_orders: list[BrokerAccountOpenOrder]


class _InvalidAgentContract(ValueError):
    pass


class _MissingAgentContract(ValueError):
    def __init__(self, message: str, *, reason_code: str = "exec_agent_unavailable") -> None:
        super().__init__(message)
        self.reason_code = reason_code


_AgentContractResolution = ShortTermExecAgentContract | _MissingAgentContract | _InvalidAgentContract


def _short_term_auto_buy_evaluation_enabled(*, settings: Settings, db: Session) -> bool:
    if getattr(settings, "automation_enabled", False):
        return True

    state = db.get(RuntimeControlState, RUNTIME_CONTROL_ID)
    return bool(state is not None and effective_automation_enabled(state))


def apply_short_term_exec_agent_decisions(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None,
    plan: ExecutionPlan,
    exec_agent_client: ExecAgentClient | None = None,
) -> ExecAgentApplicationResult:
    automation_enabled = _short_term_auto_buy_evaluation_enabled(settings=settings, db=db)
    candidates = [
        item
        for item in plan.items
        if _is_short_term_buy_candidate(item)
        and (
            getattr(settings, "exec_agent_enabled", False)
            or (automation_enabled and _has_decider_output(item))
            or _has_agent_contract(item)
        )
    ]
    if not candidates:
        return ExecAgentApplicationResult(
            evaluated_count=0,
            approved_count=0,
            rejected_count=0,
            account_snapshot=None,
            decisions=[],
        )

    LOGGER.info(
        "short_term_decider_candidates_loaded",
        extra={
            "candidate_count": len(candidates),
            "execution_phase": "next_open",
            "trade_date": plan.market_date.isoformat() if plan.market_date else None,
            "plan_id": plan.external_plan_id,
            "automation_enabled": automation_enabled,
            "exec_agent_enabled": getattr(settings, "exec_agent_enabled", False),
        },
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
    reserved_cash_budget = Decimal("0")
    reserved_daily_buy_notional = _load_daily_new_buy_notional(
        settings=settings,
        db=db,
        market_date=plan.market_date,
    )
    next_plan_item_index = _next_plan_item_index(plan)
    agent_contracts = _resolve_agent_contracts(
        settings=settings,
        plan=plan,
        candidates=candidates,
        account_context=account_context,
        exec_agent_client=exec_agent_client,
    )

    for item in candidates:
        decision = _decide_candidate(
            settings=settings,
            plan=plan,
            item=item,
            account_context=account_context,
            reserved_total_budget=reserved_total_budget,
            reserved_cash_budget=reserved_cash_budget,
            reserved_daily_buy_notional=reserved_daily_buy_notional,
            agent_contract_result=agent_contracts.get(item.id),
        )
        decisions.append(decision)
        if decision["action"] == "approve":
            approved_notional = Decimal(str(decision["approved_notional"]))
            approved_count += 1
            reserved_total_budget += approved_notional
            reserved_cash_budget += approved_notional
            reserved_daily_buy_notional += approved_notional
            _apply_approved_decision(item=item, decision=decision)
            next_plan_item_index = _append_monitoring_items(
                settings=settings,
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

    LOGGER.info(
        "short_term_decider_auto_buy_evaluated",
        extra={
            "candidate_count": len(candidates),
            "approved_count": approved_count,
            "rejected_count": rejected_count,
            "plan_id": plan.external_plan_id,
            "trade_date": plan.market_date.isoformat() if plan.market_date else None,
        },
    )
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


def _has_agent_contract(item: ExecutionPlanItem) -> bool:
    for payload in (item.risk_context, item.quantity_policy):
        if not isinstance(payload, dict):
            continue
        if any(key in payload for key in EXEC_AGENT_CONTRACT_KEYS):
            return True
    return False


def _has_decider_output(item: ExecutionPlanItem) -> bool:
    return isinstance(item.risk_context, dict) and isinstance(item.risk_context.get("decider_output"), dict)


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
        buying_power=snapshot.buying_power,
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
        buying_power=None,
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


def _load_daily_new_buy_notional(
    *,
    settings: Settings,
    db: Session,
    market_date: date | None,
) -> Decimal:
    if market_date is None:
        return Decimal("0")
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            ExecutionPlanItem.side == "buy",
            ExecutionPlanItem.status.in_(
                (
                    "pending",
                    "next_open_buy_scheduled",
                    "next_open_buy_dispatching",
                    "next_open_buy_retry_pending",
                    "broker_order_submission_pending",
                    "broker_order_submitted",
                    "broker_order_retry_pending",
                    "submitted",
                    "open",
                    "partially_filled",
                    "filled",
                    "retry_pending",
                )
            ),
        )
    ).all()
    total = Decimal("0")
    for item in items:
        total += _approved_notional_from_item(item)
    return total


def _approved_notional_from_item(item: ExecutionPlanItem) -> Decimal:
    for source in (item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        decision = source.get("exec_agent_decision")
        if isinstance(decision, dict):
            value = _parse_positive_decimal(decision.get("approved_notional"))
            if value is not None:
                return value
        for key in ("approved_notional", "notional", "max_notional"):
            value = _parse_positive_decimal(source.get(key))
            if value is not None:
                return value

    quantity = None
    if isinstance(item.quantity_policy, dict):
        quantity = item.quantity_policy.get("quantity")
    parsed_quantity = _parse_positive_int(quantity)
    if parsed_quantity is None:
        return Decimal("0")
    price = item.limit_price or _first_decimal(item.risk_context, ("sizing_price", "reference_price", "entry_price"))
    if price is None:
        price = _first_decimal(item.quantity_policy, ("sizing_price", "reference_price", "entry_price"))
    if price is None:
        return Decimal("0")
    return price * Decimal(parsed_quantity)


def _parse_positive_decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if parsed <= 0 or not parsed.is_finite():
        return None
    return parsed


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _minimum_decimal(*values: Decimal | None) -> Decimal | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return min(concrete)


def _decide_candidate(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    account_context: _AccountContext,
    reserved_total_budget: Decimal,
    reserved_cash_budget: Decimal,
    reserved_daily_buy_notional: Decimal,
    agent_contract_result: _AgentContractResolution | None,
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

    if agent_contract_result is None:
        return _reject(
            base,
            "exec_agent_unavailable",
            "OpenClaw exec-agent did not return a decision for this candidate",
        )
    if isinstance(agent_contract_result, _MissingAgentContract):
        return _reject(base, agent_contract_result.reason_code, str(agent_contract_result))
    if isinstance(agent_contract_result, _InvalidAgentContract):
        return _reject(base, "invalid_agent_output", str(agent_contract_result))
    agent_contract = agent_contract_result

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
            "unsupported_entry_trigger",
            "short-term exec agent does not support trigger buy semantics",
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
    if _account_snapshot_is_stale(settings=settings, observed_at=account_context.observed_at):
        return _reject(
            base,
            "stale_account_snapshot",
            "account snapshot is too old for exec-agent sizing",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )
    remaining_cash = available_cash - reserved_cash_budget
    if remaining_cash <= 0:
        return _reject(
            base,
            "insufficient_cash",
            "available cash must be positive after earlier approved candidates are reserved",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )
    existing_holding = account_context.holdings_by_symbol.get(item.symbol, 0)
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

    entry_limit_price = _entry_limit_price(item=item, agent_contract=agent_contract)
    if _price_above_entry_limit(
        settings=settings,
        reference_price=sizing_price,
        entry_limit_price=entry_limit_price,
    ):
        return _reject(
            base,
            "price_above_entry_limit",
            "reference price is above the planned entry/chase limit",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details={
                "entry_limit_price": _serialize_decimal(entry_limit_price),
                "max_entry_premium_pct": _serialize_decimal(
                    _settings_non_negative_decimal(
                        settings,
                        "exec_agent_max_entry_premium_pct",
                        Decimal("0"),
                    )
                ),
            },
        )

    effective_stop = effective_stop_price(
        settings=settings,
        entry_price=sizing_price,
        upstream_stop_price=agent_contract.stop_price,
    )
    reward_risk = _reward_risk(
        reference_price=sizing_price,
        target_price=agent_contract.target_price,
        stop_price=effective_stop,
    )
    if reward_risk is None:
        return _reject(
            base,
            "invalid_reward_risk_prices",
            "target and stop must create positive reward and risk distances",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details=_decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=None,
                exposure_after_trade=None,
                extra={"effective_stop_price": _serialize_decimal(effective_stop)},
            ),
        )
    reward_risk_threshold = _reward_risk_threshold(
        settings=settings,
        agent_contract=agent_contract,
        reference_price=sizing_price,
        target_price=agent_contract.target_price,
    )
    if _reward_risk_below_floor(settings=settings, agent_contract=agent_contract, reward_risk=reward_risk, reference_price=sizing_price):
        return _reject(
            base,
            "reward_risk_below_minimum",
            "reward/risk is below the absolute exec-agent floor",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details=_decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=reward_risk,
                exposure_after_trade=None,
                extra={
                    "effective_stop_price": _serialize_decimal(effective_stop),
                    "required_reward_risk": _serialize_decimal(
                        _settings_decimal(settings, "exec_agent_absolute_min_reward_risk", Decimal("1.00"))
                    )
                },
            ),
        )
    if reward_risk < reward_risk_threshold:
        return _reject(
            base,
            "reward_risk_below_threshold",
            "reward/risk is below the configured approval threshold",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details=_decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=reward_risk,
                exposure_after_trade=None,
                extra={
                    "effective_stop_price": _serialize_decimal(effective_stop),
                    "required_reward_risk": _serialize_decimal(reward_risk_threshold),
                },
            ),
        )
    confirmation_signal = _confirmation_signal(item=item, agent_contract=agent_contract)
    if confirmation_signal is None:
        return _reject(
            base,
            "missing_confirmation_signal",
            "approval requires breakout, VWAP, trigger, liquidity, volume, or momentum confirmation",
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details=_decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=reward_risk,
                exposure_after_trade=None,
                extra={"effective_stop_price": _serialize_decimal(effective_stop)},
            ),
        )

    sizing_budget = _candidate_sizing_budget(
        settings=settings,
        item=item,
        agent_contract=agent_contract,
        reference_price=sizing_price,
        available_cash=remaining_cash,
        reserved_total_budget=reserved_total_budget,
    )
    if sizing_budget is None:
        return _reject(
            base,
            "confidence_below_minimum",
            "short-term candidate confidence is below the execution sizing minimum",
            agent_contract=agent_contract,
            available_cash=available_cash,
            sizing_price=sizing_price,
        )
    if sizing_budget.remaining_total_budget <= 0:
        return _reject(
            base,
            "total_budget_cap_reached",
            "exec-agent total budget cap is already reserved",
            agent_contract=agent_contract,
            available_cash=available_cash,
        )

    max_symbol_budget = agent_contract.max_symbol_budget or _settings_decimal(
        settings,
        "exec_agent_max_symbol_budget",
        Decimal("1000000"),
    )
    existing_symbol_exposure = sizing_price * Decimal(existing_holding)
    account_basis = _minimum_decimal(account_context.available_cash, account_context.buying_power) or available_cash
    market_regime_context = market_regime_context_from_item_payloads(
        agent_contract.context,
        item.risk_context,
        item.quantity_policy,
    )
    market_regime_context = merge_live_market_regime_context(
        market_regime_context,
        load_live_market_regime_context(settings=settings),
    )
    risk_sizing = resolve_risk_based_buy_quantity(
        settings=settings,
        account_basis=account_basis,
        available_cash=remaining_cash,
        entry_price=sizing_price,
        stop_price=effective_stop,
        confidence=agent_contract.confidence,
        candidate_max_budget=sizing_budget.final_budget,
        agent_max_quantity=agent_contract.max_quantity,
        existing_symbol_exposure=existing_symbol_exposure,
        daily_new_buy_notional_used=reserved_daily_buy_notional,
        vol_corr_context=_vol_corr_context(
            item=item,
            agent_contract=agent_contract,
            account_context=account_context,
        ),
        market_regime_context=market_regime_context,
        market_date=item.plan.market_date if item.plan is not None else None,
    )
    if isinstance(risk_sizing, RiskSizingRejection):
        return _reject(
            base,
            risk_sizing.reason_code,
            risk_sizing.message,
            agent_contract=agent_contract,
            available_cash=available_cash,
            existing_holding=existing_holding,
            sizing_price=sizing_price,
            details=_decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=reward_risk,
                exposure_after_trade=None,
                extra={
                    "existing_symbol_exposure": _serialize_decimal(existing_symbol_exposure),
                    "effective_stop_price": _serialize_decimal(effective_stop),
                    **risk_sizing.details,
                },
            ),
        )
    assert isinstance(risk_sizing, RiskSizingResult)

    quantity = risk_sizing.quantity
    approved_notional = risk_sizing.approved_notional
    budget = sizing_budget.final_budget
    max_symbol_exposure = risk_sizing.max_symbol_exposure
    exposure_after_trade = risk_sizing.exposure_after_trade
    position_risk = risk_sizing.stop_distance * Decimal(quantity)

    risk_flags = _decision_risk_flags(agent_contract=agent_contract, confirmation_signal=confirmation_signal)
    target_profile = _target_profile(
        settings=settings,
        agent_contract=agent_contract,
        reference_price=sizing_price,
        target_price=agent_contract.target_price,
    )
    return {
        **base,
        "action": "approve",
        "reason_code": "approved",
        "reason": "guarded exec-agent buy approved within exposure, reward/risk, and confirmation rails",
        "confidence": _serialize_decimal(agent_contract.confidence),
        "conviction": sizing_budget.conviction,
        "quantity": quantity,
        "max_quantity": quantity,
        "agent_approved_quantity_upper_bound": agent_contract.max_quantity,
        "order_type": "limit" if item.limit_price is not None else "market",
        "reference_price": _serialize_decimal(sizing_price),
        "sizing_price": _serialize_decimal(sizing_price),
        "entry_limit_price": _serialize_decimal(entry_limit_price),
        "approved_notional": _serialize_decimal(approved_notional),
        "available_cash": _serialize_decimal(available_cash),
        "remaining_available_cash": _serialize_decimal(remaining_cash),
        "budget": _serialize_decimal(budget),
        "max_budget": _serialize_decimal(budget),
        "base_budget": _serialize_decimal(sizing_budget.base_budget),
        "budget_multiplier": _serialize_decimal(sizing_budget.budget_multiplier),
        "risk_adjustments": sizing_budget.risk_adjustments,
        "final_budget": _serialize_decimal(sizing_budget.final_budget),
        "max_budget_upper_bound": _serialize_decimal(agent_contract.max_budget),
        "max_symbol_budget_upper_bound": _serialize_decimal(max_symbol_budget),
        "max_symbol_exposure": _serialize_decimal(max_symbol_exposure),
        "max_position_budget": _serialize_decimal(risk_sizing.max_position_budget),
        "existing_holding": existing_holding,
        "existing_symbol_exposure": _serialize_decimal(existing_symbol_exposure),
        "exposure_after_trade": _serialize_decimal(exposure_after_trade),
        "remaining_total_budget": _serialize_decimal(sizing_budget.remaining_total_budget),
        "target_price": _serialize_decimal(agent_contract.target_price),
        "target_profile": target_profile,
        "target_tag": f"target_profile:{target_profile}",
        "special_tactical_scalp": agent_contract.special_tactical_scalp,
        "stop_price": _serialize_decimal(effective_stop),
        "upstream_stop_price": _serialize_decimal(agent_contract.stop_price),
        "reward_risk": _serialize_decimal(reward_risk),
        "required_reward_risk": _serialize_decimal(reward_risk_threshold),
        "position_risk": _serialize_decimal(position_risk),
        "max_trade_risk": _serialize_decimal(risk_sizing.risk_budget),
        "risk_pct": _serialize_decimal(risk_sizing.risk_pct),
        "risk_budget": _serialize_decimal(risk_sizing.risk_budget),
        "stop_distance": _serialize_decimal(risk_sizing.stop_distance),
        "risk_based_quantity": risk_sizing.risk_quantity,
        "allocation_quantity": risk_sizing.allocation_quantity,
        "cash_quantity": risk_sizing.cash_quantity,
        "daily_budget_remaining": _serialize_decimal(risk_sizing.daily_budget_remaining),
        "sizing_caps": risk_sizing.cap_details,
        "confirmation_signal": confirmation_signal,
        "risk_flags": risk_flags,
        "allow_existing_holding": agent_contract.allow_existing_holding,
        "feedback_ids": list(agent_contract.feedback_ids),
        "agent_contract": agent_contract.model_dump(mode="json"),
        "account_snapshot_observed_at": account_context.observed_at.isoformat(),
    }


@dataclass(frozen=True)
class _SizingBudget:
    base_budget: Decimal
    budget_multiplier: Decimal
    risk_adjustments: dict[str, object]
    final_budget: Decimal
    remaining_total_budget: Decimal
    conviction: str | None


def _candidate_sizing_budget(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
    available_cash: Decimal,
    reserved_total_budget: Decimal,
) -> _SizingBudget | None:
    max_budget = agent_contract.max_budget
    assert max_budget is not None
    per_symbol_cap = min(
        max_budget,
        agent_contract.max_symbol_budget or _settings_decimal(
            settings,
            "exec_agent_max_symbol_budget",
            Decimal("1000000"),
        ),
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
        return _SizingBudget(
            base_budget=Decimal("0"),
            budget_multiplier=Decimal("0"),
            risk_adjustments={},
            final_budget=Decimal("0"),
            remaining_total_budget=remaining_total_budget,
            conviction=None,
        )

    base_budget = min(per_symbol_cap, available_cash, remaining_total_budget)
    confidence_multiplier = _confidence_budget_multiplier(agent_contract.confidence)
    if confidence_multiplier is None:
        return None
    conviction = _candidate_conviction(item=item, agent_contract=agent_contract)
    conviction_multiplier = CONVICTION_MULTIPLIERS.get(conviction or "")
    budget_multiplier = min(
        confidence_multiplier,
        conviction_multiplier if conviction_multiplier is not None else confidence_multiplier,
    )
    risk_multiplier, risk_adjustments = _risk_budget_adjustments(
        item=item,
        agent_contract=agent_contract,
        reference_price=reference_price,
    )
    final_budget = min(
        base_budget,
        (base_budget * budget_multiplier * risk_multiplier).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        ),
    )
    return _SizingBudget(
        base_budget=base_budget,
        budget_multiplier=budget_multiplier,
        risk_adjustments=risk_adjustments,
        final_budget=final_budget,
        remaining_total_budget=remaining_total_budget,
        conviction=conviction,
    )


def _confidence_budget_multiplier(confidence: Decimal) -> Decimal | None:
    if confidence < Decimal("0.50"):
        return None
    if confidence >= Decimal("0.80"):
        return Decimal("1.00")
    if confidence >= Decimal("0.65"):
        return Decimal("0.75")
    return Decimal("0.50")


def _candidate_conviction(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> str | None:
    for source in (
        agent_contract.context,
        item.risk_context,
        item.quantity_policy,
    ):
        if not isinstance(source, dict):
            continue
        value = source.get("conviction")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    return None


def _risk_budget_adjustments(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
) -> tuple[Decimal, dict[str, object]]:
    adjustments: dict[str, object] = {}
    multiplier = Decimal("1.00")

    target_stop_multiplier, target_stop = _target_stop_adjustment(
        agent_contract=agent_contract,
        reference_price=reference_price,
    )
    if target_stop:
        adjustments["target_stop"] = target_stop
        multiplier *= target_stop_multiplier

    liquidity_multiplier, liquidity = _metric_adjustment(
        item=item,
        agent_contract=agent_contract,
        keys=("liquidity_score", "liquidity", "volume_score", "marketability_score"),
        thresholds=(
            (Decimal("0.30"), Decimal("0.50")),
            (Decimal("0.60"), Decimal("0.75")),
            (Decimal("0.80"), Decimal("0.90")),
        ),
        default_multiplier=Decimal("1.00"),
    )
    if liquidity:
        adjustments["liquidity"] = liquidity
        multiplier *= liquidity_multiplier

    orderbook_multiplier, orderbook = _metric_adjustment(
        item=item,
        agent_contract=agent_contract,
        keys=("orderbook_quality", "order_book_quality", "book_quality"),
        thresholds=(
            (Decimal("0.30"), Decimal("0.60")),
            (Decimal("0.60"), Decimal("0.80")),
            (Decimal("0.80"), Decimal("0.90")),
        ),
        default_multiplier=Decimal("1.00"),
    )
    if orderbook:
        adjustments["orderbook_quality"] = orderbook
        multiplier *= orderbook_multiplier

    spread_multiplier, spread = _spread_adjustment(item=item, agent_contract=agent_contract)
    if spread:
        adjustments["spread"] = spread
        multiplier *= spread_multiplier

    adjustments["combined_multiplier"] = _serialize_decimal(multiplier)
    return multiplier, adjustments


def _target_stop_adjustment(
    *,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
) -> tuple[Decimal, dict[str, object]]:
    target_price = agent_contract.target_price
    stop_price = agent_contract.stop_price
    if target_price is None or stop_price is None:
        return Decimal("1.00"), {}
    reward_distance = (target_price - reference_price) / reference_price
    risk_distance = (reference_price - stop_price) / reference_price
    if reward_distance <= 0 or risk_distance <= 0:
        return Decimal("0.50"), {
            "multiplier": "0.5000",
            "reason": "invalid_target_or_stop_distance",
            "reward_distance": _serialize_decimal(reward_distance),
            "risk_distance": _serialize_decimal(risk_distance),
        }
    risk_reward = reward_distance / risk_distance
    multiplier = Decimal("1.00")
    reason = "balanced_target_stop"
    if risk_reward < Decimal("1.00"):
        multiplier = Decimal("0.50")
        reason = "risk_reward_below_one"
    elif risk_reward < Decimal("1.50"):
        multiplier = Decimal("0.75")
        reason = "thin_risk_reward"
    if risk_distance > Decimal("0.08"):
        multiplier = min(multiplier, Decimal("0.80"))
        reason = "wide_stop_distance"
    elif risk_distance < Decimal("0.01"):
        multiplier = min(multiplier, Decimal("0.85"))
        reason = "tight_stop_distance"
    return multiplier, {
        "multiplier": _serialize_decimal(multiplier),
        "reason": reason,
        "reward_distance": _serialize_decimal(reward_distance),
        "risk_distance": _serialize_decimal(risk_distance),
        "risk_reward": _serialize_decimal(risk_reward),
    }


def _metric_adjustment(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
    keys: tuple[str, ...],
    thresholds: tuple[tuple[Decimal, Decimal], ...],
    default_multiplier: Decimal,
) -> tuple[Decimal, dict[str, object]]:
    value = _first_decimal_from_sources(item=item, agent_contract=agent_contract, keys=keys)
    if value is None:
        return default_multiplier, {}
    normalized = value / Decimal("100") if value > 1 and value <= 100 else value
    if normalized < 0:
        normalized = Decimal("0")
    multiplier = default_multiplier
    for upper_bound, threshold_multiplier in thresholds:
        if normalized < upper_bound:
            multiplier = threshold_multiplier
            break
    return multiplier, {
        "value": _serialize_decimal(value),
        "normalized": _serialize_decimal(normalized),
        "multiplier": _serialize_decimal(multiplier),
    }


def _spread_adjustment(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> tuple[Decimal, dict[str, object]]:
    spread_bps = _first_decimal_from_sources(
        item=item,
        agent_contract=agent_contract,
        keys=("spread_bps", "bid_ask_spread_bps", "orderbook_spread_bps"),
    )
    if spread_bps is None:
        spread_pct = _first_decimal_from_sources(
            item=item,
            agent_contract=agent_contract,
            keys=("spread_pct", "spread_percent", "bid_ask_spread_pct"),
        )
        if spread_pct is None:
            return Decimal("1.00"), {}
        spread_bps = spread_pct * (Decimal("100") if spread_pct <= 1 else Decimal("1"))
    multiplier = Decimal("1.00")
    if spread_bps > Decimal("100"):
        multiplier = Decimal("0.50")
    elif spread_bps > Decimal("50"):
        multiplier = Decimal("0.75")
    elif spread_bps > Decimal("20"):
        multiplier = Decimal("0.90")
    return multiplier, {
        "spread_bps": _serialize_decimal(spread_bps),
        "multiplier": _serialize_decimal(multiplier),
    }


def _first_decimal_from_sources(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
    keys: tuple[str, ...],
) -> Decimal | None:
    for source in (agent_contract.context, item.risk_context, item.quantity_policy):
        value = _first_decimal(source, keys)
        if value is not None:
            return value
    return None


def _vol_corr_context(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
    account_context: _AccountContext,
) -> dict[str, object]:
    context: dict[str, object] = {}
    for source in (agent_contract.context, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        for key in (
            "realized_volatility_pct",
            "volatility_pct",
            "atr_pct",
            "recent_realized_volatility_pct",
            "daily_return_pcts",
            "return_pcts",
            "candidate_return_pcts",
            "daily_returns",
            "returns",
            "candidate_returns",
            "max_holding_correlation",
            "holding_correlation",
            "correlation",
            "correlation_symbol",
            "holding_return_pcts_by_symbol",
            "holding_returns_by_symbol",
        ):
            if key in source:
                context[key] = source[key]

    holding_symbols = sorted(account_context.holdings_by_symbol)
    if holding_symbols:
        context["holding_symbols"] = holding_symbols
    return context


def _first_decimal(source: object, keys: tuple[str, ...]) -> Decimal | None:
    if not isinstance(source, dict):
        return None
    for key in keys:
        raw = source.get(key)
        if raw is None:
            continue
        try:
            value = Decimal(str(raw))
        except (InvalidOperation, ValueError):
            continue
        if value.is_finite():
            return value
    return None


def _entry_limit_price(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> Decimal | None:
    if item.limit_price is not None and item.limit_price > 0:
        return item.limit_price
    return _first_decimal_from_sources(
        item=item,
        agent_contract=agent_contract,
        keys=ENTRY_LIMIT_KEYS,
    )


def _price_above_entry_limit(
    *,
    settings: Settings,
    reference_price: Decimal,
    entry_limit_price: Decimal | None,
) -> bool:
    if entry_limit_price is None or entry_limit_price <= 0:
        return False
    max_premium = _settings_non_negative_decimal(
        settings,
        "exec_agent_max_entry_premium_pct",
        Decimal("0"),
    )
    return reference_price > (entry_limit_price * (Decimal("1") + max_premium))


def _reward_risk(
    *,
    reference_price: Decimal,
    target_price: Decimal | None,
    stop_price: Decimal | None,
) -> Decimal | None:
    if target_price is None or stop_price is None:
        return None
    reward = target_price - reference_price
    risk = reference_price - stop_price
    if reward <= 0 or risk <= 0:
        return None
    return (reward / risk).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _reward_risk_threshold(
    *,
    settings: Settings,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
    target_price: Decimal | None,
) -> Decimal:
    normal_threshold = _settings_decimal(
        settings,
        "exec_agent_min_reward_risk",
        Decimal("1.30"),
    )
    relaxed_threshold = _settings_decimal(
        settings,
        "exec_agent_relaxed_min_reward_risk",
        Decimal("1.10"),
    )
    if _allows_special_tactical_scalp(
        settings=settings,
        agent_contract=agent_contract,
        reference_price=reference_price,
        target_price=target_price,
    ):
        return min(
            normal_threshold,
            relaxed_threshold,
            _settings_decimal(settings, "exec_agent_absolute_min_reward_risk", Decimal("1.00")),
        )
    if _uses_relaxed_reward_risk_threshold(agent_contract=agent_contract):
        return min(normal_threshold, relaxed_threshold)
    return normal_threshold


def _uses_relaxed_reward_risk_threshold(*, agent_contract: ShortTermExecAgentContract) -> bool:
    if agent_contract.confidence >= Decimal("0.80"):
        return True
    risk_flags = _all_risk_flags(agent_contract=agent_contract)
    return bool(risk_flags.intersection(RELAXED_REWARD_RISK_FLAGS))


def _reward_risk_below_floor(
    *,
    settings: Settings,
    agent_contract: ShortTermExecAgentContract,
    reward_risk: Decimal,
    reference_price: Decimal,
) -> bool:
    floor = _settings_decimal(settings, "exec_agent_absolute_min_reward_risk", Decimal("1.00"))
    if reward_risk >= floor:
        return False
    return not _allows_special_tactical_scalp(
        settings=settings,
        agent_contract=agent_contract,
        reference_price=reference_price,
        target_price=agent_contract.target_price,
    )


def _allows_special_tactical_scalp(
    *,
    settings: Settings,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
    target_price: Decimal | None,
) -> bool:
    if not agent_contract.special_tactical_scalp or agent_contract.stop_price is None or target_price is None:
        return False
    target_distance = (target_price - reference_price) / reference_price
    stop_distance = (reference_price - agent_contract.stop_price) / reference_price
    if target_distance <= 0 or stop_distance <= 0:
        return False
    min_target_distance = _settings_decimal(
        settings,
        "exec_agent_tactical_scalp_min_target_pct",
        Decimal("0.02"),
    )
    max_stop_distance = _settings_decimal(
        settings,
        "exec_agent_tactical_scalp_max_stop_pct",
        Decimal("0.015"),
    )
    return target_distance >= min_target_distance and stop_distance <= max_stop_distance


def _target_profile(
    *,
    settings: Settings,
    agent_contract: ShortTermExecAgentContract,
    reference_price: Decimal,
    target_price: Decimal | None,
) -> TargetProfile:
    if agent_contract.target_profile is not None:
        return agent_contract.target_profile
    if target_price is None:
        return "standard_breakout"
    target_distance = (target_price - reference_price) / reference_price
    if _allows_special_tactical_scalp(
        settings=settings,
        agent_contract=agent_contract,
        reference_price=reference_price,
        target_price=target_price,
    ) or target_distance <= Decimal("0.025"):
        return "tactical_2pct"
    if target_distance <= Decimal("0.05"):
        return "short_swing"
    if target_distance <= Decimal("0.08"):
        return "standard_breakout"
    return "extended_momentum"


def _confirmation_signal(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> str | None:
    for flag in _all_risk_flags(agent_contract=agent_contract):
        normalized = _normalize_signal_text(flag)
        if normalized in CONFIRMATION_SIGNAL_VALUES:
            return normalized

    for source in _confirmation_sources(item=item, agent_contract=agent_contract):
        for key in CONFIRMATION_SIGNAL_KEYS:
            signal = _signal_from_source(source, key)
            if signal is not None:
                return signal
    return None


def _confirmation_sources(
    *,
    item: ExecutionPlanItem,
    agent_contract: ShortTermExecAgentContract,
) -> list[object]:
    sources: list[object] = [agent_contract.context, item.risk_context, item.quantity_policy]
    for source in (agent_contract.context, item.risk_context, item.quantity_policy):
        if not isinstance(source, dict):
            continue
        sources.extend(
            source.get(key)
            for key in ("decider_output", "market_data", "quote", "orderbook")
            if isinstance(source.get(key), dict)
        )
    return sources


def _signal_from_source(source: object, key: str) -> str | None:
    if not isinstance(source, dict) or key not in source:
        return None
    value = source.get(key)
    if value is True:
        return key
    if isinstance(value, str):
        normalized = _normalize_signal_text(value)
        if normalized in CONFIRMATION_SIGNAL_VALUES:
            return normalized
        if key in CONFIRMATION_SIGNAL_KEYS and normalized not in {"", "false", "none", "no"}:
            return key
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, str):
                normalized = _normalize_signal_text(item)
                if normalized in CONFIRMATION_SIGNAL_VALUES:
                    return normalized
    return None


def _normalize_signal_text(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def _all_risk_flags(*, agent_contract: ShortTermExecAgentContract) -> set[str]:
    flags = {_normalize_signal_text(flag) for flag in agent_contract.risk_flags}
    context = agent_contract.context
    if isinstance(context, dict):
        raw_flags = context.get("risk_flags")
        if isinstance(raw_flags, list):
            flags.update(
                _normalize_signal_text(flag)
                for flag in raw_flags
                if isinstance(flag, str) and flag.strip()
            )
    return flags


def _decision_risk_flags(
    *,
    agent_contract: ShortTermExecAgentContract,
    confirmation_signal: str,
) -> list[str]:
    flags = _all_risk_flags(agent_contract=agent_contract)
    flags.add(f"confirmation:{confirmation_signal}")
    return sorted(flags)


def _decision_contract_fields(
    *,
    agent_contract: ShortTermExecAgentContract,
    reward_risk: Decimal | None,
    exposure_after_trade: Decimal | None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "max_quantity": agent_contract.max_quantity,
        "max_budget": _serialize_decimal(agent_contract.max_budget),
        "reference_price": _serialize_decimal(agent_contract.reference_price),
        "target_price": _serialize_decimal(agent_contract.target_price),
        "stop_price": _serialize_decimal(agent_contract.stop_price),
        "reward_risk": _serialize_decimal(reward_risk),
        "exposure_after_trade": _serialize_decimal(exposure_after_trade),
        "risk_flags": sorted(_all_risk_flags(agent_contract=agent_contract)),
    }
    if extra:
        payload.update(extra)
    return payload


def _resolve_agent_contracts(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    candidates: list[ExecutionPlanItem],
    account_context: _AccountContext,
    exec_agent_client: ExecAgentClient | None,
) -> dict[str, _AgentContractResolution]:
    results: dict[str, _AgentContractResolution] = {}
    precomputed_contracts: dict[str, ShortTermExecAgentContract] = {}
    missing_contract_items: list[ExecutionPlanItem] = []
    exec_agent_enabled = getattr(settings, "exec_agent_enabled", False)

    for item in candidates:
        try:
            precomputed_contracts[item.id] = _load_agent_contract(item)
        except _MissingAgentContract:
            missing_contract_items.append(item)
        except _InvalidAgentContract as exc:
            results[item.id] = exc

    results.update(precomputed_contracts)

    if not exec_agent_enabled:
        missing = _MissingAgentContract(
            "short-term auto-buy requires a precomputed exec_agent_decision when OpenClaw exec-agent is disabled",
            reason_code="missing_exec_agent_contract",
        )
        for item in missing_contract_items:
            results[item.id] = missing
        return results

    openclaw_candidates = [item for item in missing_contract_items if item.id not in results]
    if not openclaw_candidates:
        return results

    try:
        results.update(
            _request_batch_agent_contracts(
                settings=settings,
                plan=plan,
                candidates=openclaw_candidates,
                account_context=account_context,
                exec_agent_client=exec_agent_client,
            )
        )
    except (_MissingAgentContract, _InvalidAgentContract) as exc:
        for item in openclaw_candidates:
            results[item.id] = precomputed_contracts.get(item.id, exc)
    return results


def _request_batch_agent_contracts(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    candidates: list[ExecutionPlanItem],
    account_context: _AccountContext,
    exec_agent_client: ExecAgentClient | None,
) -> dict[str, ShortTermExecAgentContract]:
    client = exec_agent_client or UnavailableExecAgentClient()
    request = _build_batch_agent_request(
        settings=settings,
        plan=plan,
        candidates=candidates,
        account_context=account_context,
    )
    try:
        response = _call_batch_sizing_client(
            client=client,
            request=request,
            settings=settings,
            plan=plan,
            candidates=candidates,
            account_context=account_context,
        )
    except (_MissingAgentContract, _InvalidAgentContract):
        raise
    except Exception as exc:
        raise _MissingAgentContract(f"OpenClaw exec-agent batch request failed: {exc}") from exc

    if not isinstance(response, ExecAgentBatchSizingResponse):
        try:
            response = ExecAgentBatchSizingResponse.model_validate(response)
        except ValidationError as exc:
            raise _InvalidAgentContract(_validation_error_message(exc)) from exc
    return _contracts_from_batch_response(response=response, candidates=candidates)


def _call_batch_sizing_client(
    *,
    client: ExecAgentClient,
    request: ExecAgentBatchSizingRequest,
    settings: Settings,
    plan: ExecutionPlan,
    candidates: list[ExecutionPlanItem],
    account_context: _AccountContext,
) -> ExecAgentBatchSizingResponse:
    batch_sizer = getattr(client, "size_candidates", None)
    if callable(batch_sizer):
        return batch_sizer(request)

    legacy_sizer = getattr(client, "size_candidate", None)
    if not callable(legacy_sizer):
        raise ExecAgentUnavailable("OpenClaw exec-agent client does not expose batch sizing")

    decisions: list[ExecAgentAllocationDecision] = []
    for item in candidates:
        try:
            response = legacy_sizer(
                _build_agent_request(
                    settings=settings,
                    plan=plan,
                    item=item,
                    account_context=account_context,
                    source_identity=serialize_plan_item_source_identity(item),
                )
            )
        except Exception as exc:
            raise _MissingAgentContract(f"OpenClaw exec-agent request failed: {exc}") from exc
        if not isinstance(response, ExecAgentSizingResponse):
            try:
                response = ExecAgentSizingResponse.model_validate(response)
            except ValidationError as exc:
                raise _InvalidAgentContract(_validation_error_message(exc)) from exc
        decisions.append(
            ExecAgentAllocationDecision.model_validate(
                {
                    **response.model_dump(),
                    "execution_plan_item_id": item.id,
                }
            )
        )
    return ExecAgentBatchSizingResponse(decisions=decisions)


def _contracts_from_batch_response(
    *,
    response: ExecAgentBatchSizingResponse,
    candidates: list[ExecutionPlanItem],
) -> dict[str, ShortTermExecAgentContract]:
    expected_ids = {item.id for item in candidates}
    contracts: dict[str, ShortTermExecAgentContract] = {}
    for decision in response.decisions:
        item_id = decision.execution_plan_item_id
        if item_id not in expected_ids:
            raise _InvalidAgentContract(
                f"batch response included unknown execution_plan_item_id {item_id}"
            )
        if item_id in contracts:
            raise _InvalidAgentContract(
                f"batch response included duplicate execution_plan_item_id {item_id}"
            )
        response_payload = decision.model_dump(exclude={"execution_plan_item_id"})
        contracts[item_id] = _contract_from_agent_response(
            ExecAgentSizingResponse.model_validate(response_payload)
        )

    missing_ids = expected_ids.difference(contracts)
    if missing_ids:
        missing = ", ".join(sorted(missing_ids))
        raise _InvalidAgentContract(
            f"batch response missing decisions for execution_plan_item_id: {missing}"
        )
    return contracts


def _build_batch_agent_request(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    candidates: list[ExecutionPlanItem],
    account_context: _AccountContext,
) -> ExecAgentBatchSizingRequest:
    return ExecAgentBatchSizingRequest(
        agent_id=getattr(settings, "exec_agent_id", OPENCLAW_EXEC_AGENT_ID),
        model=getattr(settings, "exec_agent_model", OPENCLAW_EXEC_AGENT_MODEL),
        plan_id=plan.external_plan_id,
        market_date=plan.market_date.isoformat() if plan.market_date else None,
        strategy_run_id=plan.strategy_run_id,
        candidates=[
            _build_agent_candidate(
                plan=plan,
                item=item,
                source_identity=serialize_plan_item_source_identity(item),
            )
            for item in candidates
        ],
        account=_agent_account_payload(account_context),
        portfolio=_agent_portfolio_payload(account_context),
        risk_caps=_agent_risk_caps(settings),
    )


def _build_agent_request(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    account_context: _AccountContext,
    source_identity: dict[str, object],
) -> ExecAgentSizingRequest:
    candidate = _build_agent_candidate(
        plan=plan,
        item=item,
        source_identity=source_identity,
    )
    return ExecAgentSizingRequest(
        agent_id=getattr(settings, "exec_agent_id", OPENCLAW_EXEC_AGENT_ID),
        model=getattr(settings, "exec_agent_model", OPENCLAW_EXEC_AGENT_MODEL),
        plan_id=plan.external_plan_id,
        execution_plan_item_id=item.id,
        symbol=item.symbol,
        side=item.side,
        intent=item.intent,
        source_identity=source_identity,
        candidate_context=candidate.candidate_context,
        decider_input=candidate.decider_input,
        decider_output=candidate.decider_output,
        account=_agent_account_payload(account_context),
        portfolio=_agent_portfolio_payload(account_context),
        market_data=candidate.market_data,
        risk_caps=_agent_risk_caps(settings),
    )


def _build_agent_candidate(
    *,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    source_identity: dict[str, object],
) -> ExecAgentAllocationCandidate:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    candidate_context = {
        "risk_context": risk_context,
        "quantity_policy": quantity_policy,
        "limit_price": _serialize_decimal(item.limit_price),
        "trigger_price": _serialize_decimal(item.trigger_price),
        "time_in_force": item.time_in_force,
        "market_date": plan.market_date.isoformat() if plan.market_date else None,
        "strategy_run_id": plan.strategy_run_id,
    }
    return ExecAgentAllocationCandidate(
        execution_plan_item_id=item.id,
        symbol=item.symbol,
        side=item.side,
        intent=item.intent,
        source_identity=source_identity,
        candidate_context=candidate_context,
        decider_input=_dict_or_none(risk_context.get("decider_input")),
        decider_output=_dict_or_none(risk_context.get("decider_output")),
        market_data=_dict_or_none(risk_context.get("market_data"))
        or _dict_or_none(risk_context.get("quote"))
        or _dict_or_none(risk_context.get("orderbook")),
    )


def _agent_account_payload(account_context: _AccountContext) -> dict[str, object]:
    return dict(account_context.snapshot_payload or {})


def _agent_portfolio_payload(account_context: _AccountContext) -> dict[str, object]:
    return {
        "holdings_by_symbol": dict(account_context.holdings_by_symbol),
        "open_orders": [_serialize_open_order(order) for order in account_context.open_orders],
    }


def _agent_risk_caps(settings: Settings) -> dict[str, object]:
    max_total_budget = _settings_decimal(settings, "exec_agent_max_total_budget", Decimal("3000000"))
    return {
        "max_symbol_budget": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_symbol_budget", Decimal("1000000"))
        ),
        "max_total_budget": _serialize_decimal(max_total_budget),
        "max_intraday_buy_budget": _serialize_decimal(max_total_budget),
        "base_risk_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_base_risk_pct", Decimal("0.005"))
        ),
        "aggressive_risk_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_aggressive_risk_pct", Decimal("0.015"))
        ),
        "aggressive_confidence_threshold": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_aggressive_confidence_threshold", Decimal("0.90"))
        ),
        "max_position_allocation_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_position_allocation_pct", Decimal("0.10"))
        ),
        "max_symbol_exposure_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_symbol_exposure_pct", Decimal("0.20"))
        ),
        "max_symbol_total_exposure_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_symbol_total_exposure_pct", Decimal("0.20"))
        ),
        "max_daily_new_buy_notional": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_daily_new_buy_notional", Decimal("0"))
        ),
        "max_trade_risk_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_max_trade_risk_pct", Decimal("0.01"))
        ),
        "stop_loss_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_stop_loss_pct", Decimal("0.025"))
        ),
        "max_entry_premium_pct": _serialize_decimal(
            _settings_non_negative_decimal(settings, "exec_agent_max_entry_premium_pct", Decimal("0"))
        ),
        "min_reward_risk": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_min_reward_risk", Decimal("1.30"))
        ),
        "relaxed_min_reward_risk": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_relaxed_min_reward_risk", Decimal("1.10"))
        ),
        "absolute_min_reward_risk": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_absolute_min_reward_risk", Decimal("1.00"))
        ),
        "tactical_scalp_min_target_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_tactical_scalp_min_target_pct", Decimal("0.02"))
        ),
        "tactical_scalp_max_stop_pct": _serialize_decimal(
            _settings_decimal(settings, "exec_agent_tactical_scalp_max_stop_pct", Decimal("0.015"))
        ),
        "target_profiles": [
            "tactical_2pct",
            "short_swing",
            "standard_breakout",
            "extended_momentum",
        ],
        "add_on_requires_confirmation": True,
        "max_holding_trading_sessions": getattr(settings, "exec_agent_max_holding_trading_sessions", 2),
        "max_account_snapshot_age_seconds": getattr(settings, "exec_agent_max_account_snapshot_age_seconds", 300),
    }


def _contract_from_agent_response(response: ExecAgentSizingResponse) -> ShortTermExecAgentContract:
    if response.action == "reject":
        reason = response.reason or (response.reasons[0] if response.reasons else "OpenClaw exec-agent rejected candidate")
        return ShortTermExecAgentContract(
            action="reject",
            confidence=response.confidence,
            reason_code=response.reason_code or "agent_rejected",
            reason=reason,
            context={
                "openclaw_agent_id": OPENCLAW_EXEC_AGENT_ID,
                "reasons": response.reasons,
                "risk_flags": response.risk_flags,
                "explanation": response.explanation,
            },
        )
    assert response.final_budget is not None
    assert response.reference_price is not None
    assert response.target_price is not None
    assert response.stop_price is not None
    return ShortTermExecAgentContract(
        action="approve",
        confidence=response.confidence,
        max_budget=response.final_budget,
        max_quantity=response.quantity,
        reference_price=response.reference_price,
        target_price=response.target_price,
        stop_price=response.stop_price,
        reward_risk=response.reward_risk,
        exposure_after_trade=response.exposure_after_trade,
        allow_existing_holding=response.allow_existing_holding,
        special_tactical_scalp=response.special_tactical_scalp,
        target_profile=response.target_profile,
        reason="; ".join(response.reasons) if response.reasons else None,
        risk_flags=response.risk_flags,
        context={
            "openclaw_agent_id": OPENCLAW_EXEC_AGENT_ID,
            "requested_quantity": response.quantity,
            "final_budget": _serialize_decimal(response.final_budget),
            "reasons": response.reasons,
            "risk_flags": response.risk_flags,
            "explanation": response.explanation,
        },
    )


def _dict_or_none(value: object) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _account_snapshot_is_stale(*, settings: Settings, observed_at: datetime) -> bool:
    if not getattr(settings, "exec_agent_enabled", False):
        return False
    observed = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=UTC)
    max_age = timedelta(seconds=getattr(settings, "exec_agent_max_account_snapshot_age_seconds", 300))
    return datetime.now(UTC) - observed.astimezone(UTC) > max_age


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
    details: dict[str, object] | None = None,
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
        payload.update(
            _decision_contract_fields(
                agent_contract=agent_contract,
                reward_risk=agent_contract.reward_risk,
                exposure_after_trade=agent_contract.exposure_after_trade,
            )
        )
    if available_cash is not None:
        payload["available_cash"] = _serialize_decimal(available_cash)
    if existing_holding is not None:
        payload["existing_holding"] = existing_holding
    if sizing_price is not None:
        payload["sizing_price"] = _serialize_decimal(sizing_price)
        payload["reference_price"] = _serialize_decimal(sizing_price)
    if details:
        payload.update(details)
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
    settings: Settings,
    plan: ExecutionPlan,
    buy_item: ExecutionPlanItem,
    decision: dict[str, object],
    next_plan_item_index: int,
) -> int:
    quantity = int(decision["quantity"])
    for stage in _take_profit_stages(settings=settings, decision=decision, quantity=quantity):
        plan.items.append(
            _new_monitoring_item(
                plan=plan,
                buy_item=buy_item,
                decision=decision,
                intent="target_sell",
                quantity=stage["quantity"],
                next_plan_item_index=next_plan_item_index,
                trigger_price=stage["trigger_price"],
                max_sessions=getattr(settings, "exec_agent_max_holding_trading_sessions", 2),
                exit_rule=stage["exit_rule"],
            )
        )
        next_plan_item_index += 1

    stop_price = Decimal(str(decision["stop_price"]))
    plan.items.append(
        _new_monitoring_item(
            plan=plan,
            buy_item=buy_item,
            decision=decision,
            intent="stop_sell",
            quantity=quantity,
            next_plan_item_index=next_plan_item_index,
            trigger_price=stop_price,
            max_sessions=getattr(settings, "exec_agent_max_holding_trading_sessions", 2),
            exit_rule={
                "kind": "hard_stop_loss",
                "stop_price": _serialize_decimal(stop_price),
                "upstream_stop_price": decision.get("upstream_stop_price"),
            },
        )
    )
    next_plan_item_index += 1

    plan.items.append(
        _new_monitoring_item(
            plan=plan,
            buy_item=buy_item,
            decision=decision,
            intent="max_holding_close_sell",
            quantity=quantity,
            next_plan_item_index=next_plan_item_index,
            trigger_price=None,
            max_sessions=getattr(settings, "exec_agent_max_holding_trading_sessions", 2),
            exit_rule={"kind": "time_stop"},
        )
    )
    return next_plan_item_index + 1


def _take_profit_stages(
    *,
    settings: Settings,
    decision: dict[str, object],
    quantity: int,
) -> list[dict[str, object]]:
    entry_price = Decimal(str(decision["reference_price"]))
    upstream_target = Decimal(str(decision["target_price"]))
    target_profile = str(decision.get("target_profile") or "standard_breakout")
    first_pct = _settings_decimal(settings, "exec_agent_first_take_profit_pct", Decimal("0.04"))
    final_pct = _settings_decimal(settings, "exec_agent_final_take_profit_pct", Decimal("0.08"))
    partial_fraction = _settings_decimal(settings, "exec_agent_partial_take_profit_fraction", Decimal("0.50"))
    first_target = min(
        upstream_target,
        (entry_price * (Decimal("1") + first_pct)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN),
    )
    final_target = min(
        upstream_target,
        (entry_price * (Decimal("1") + final_pct)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN),
    )
    if final_target < first_target:
        final_target = first_target

    if quantity >= 2 and first_target < final_target:
        first_quantity = int((Decimal(quantity) * partial_fraction).quantize(Decimal("1"), rounding=ROUND_DOWN))
        first_quantity = max(1, min(first_quantity, quantity - 1))
        final_quantity = quantity - first_quantity
        return [
            {
                "trigger_price": first_target,
                "quantity": first_quantity,
                "exit_rule": {
                    "kind": "partial_take_profit",
                    "stage": "first",
                    "target_profile": target_profile,
                    "target_tag": f"{target_profile}:first",
                    "trigger_pct": _serialize_decimal(first_pct),
                    "actual_trigger_pct": _serialize_decimal((first_target - entry_price) / entry_price),
                    "fraction": _serialize_decimal(partial_fraction),
                    "entry_price": _serialize_decimal(entry_price),
                    "upstream_target_price": _serialize_decimal(upstream_target),
                },
            },
            {
                "trigger_price": final_target,
                "quantity": final_quantity,
                "exit_rule": {
                    "kind": "final_take_profit",
                    "stage": "final",
                    "target_profile": target_profile,
                    "target_tag": f"{target_profile}:final",
                    "trigger_pct": _serialize_decimal(final_pct),
                    "actual_trigger_pct": _serialize_decimal((final_target - entry_price) / entry_price),
                    "entry_price": _serialize_decimal(entry_price),
                    "upstream_target_price": _serialize_decimal(upstream_target),
                },
            },
        ]

    return [
        {
            "trigger_price": final_target,
            "quantity": quantity,
            "exit_rule": {
                "kind": "final_take_profit",
                "stage": "final",
                "target_profile": target_profile,
                "target_tag": f"{target_profile}:final",
                "trigger_pct": _serialize_decimal(final_pct),
                "actual_trigger_pct": _serialize_decimal((final_target - entry_price) / entry_price),
                "entry_price": _serialize_decimal(entry_price),
                "upstream_target_price": _serialize_decimal(upstream_target),
            },
        }
    ]


def _new_monitoring_item(
    *,
    plan: ExecutionPlan,
    buy_item: ExecutionPlanItem,
    decision: dict[str, object],
    intent: str,
    quantity: int,
    next_plan_item_index: int,
    trigger_price: Decimal | None,
    max_sessions: int,
    exit_rule: dict[str, object],
) -> ExecutionPlanItem:
    buy_market_date = buy_item.source_market_date or plan.market_date
    close_exit_date = _add_trading_sessions(buy_market_date, max_sessions + 1) if buy_market_date else None
    return ExecutionPlanItem(
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
            "exit_rule": exit_rule,
            "confidence": decision.get("confidence"),
            "max_holding_trading_sessions": max_sessions,
            "close_exit_date": close_exit_date.isoformat() if close_exit_date else None,
            "close_exit_policy": "market_on_or_after_close_exit_date",
        },
        decision_agent_name=buy_item.decision_agent_name,
        decision_snapshot_id=buy_item.decision_snapshot_id,
        decision_run_id=buy_item.decision_run_id,
        source_plan_item_id=_monitoring_source_plan_item_id(
            buy_item,
            f"{intent}:{exit_rule.get('stage') or exit_rule.get('kind') or 'default'}",
        ),
        source_market_date=buy_market_date,
    )


def _add_trading_sessions(start: date | None, sessions: int) -> date | None:
    if start is None:
        return None
    current = start
    added = 0
    while added < sessions:
        current = current + timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


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


def _settings_non_negative_decimal(settings: Settings, field_name: str, default: Decimal) -> Decimal:
    value = getattr(settings, field_name, default)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default
    if parsed < 0:
        return default
    return parsed


def _serialize_account_snapshot(snapshot: BrokerAccountSnapshot) -> dict[str, object]:
    return {
        "available": True,
        "source": snapshot.source,
        "observed_at": snapshot.observed_at.isoformat(),
        "available_cash": _serialize_decimal(snapshot.available_cash),
        "buying_power": _serialize_decimal(snapshot.buying_power),
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
        "open_orders": [_serialize_open_order(order) for order in snapshot.open_orders],
    }


def _serialize_open_order(order: BrokerAccountOpenOrder) -> dict[str, object]:
    return {
        "symbol": order.symbol,
        "side": order.side,
        "quantity": order.quantity,
        "remaining_quantity": order.remaining_quantity,
        "status": order.status,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "source_candidate_id": order.source_candidate_id,
        "limit_price": _serialize_decimal(order.limit_price),
        "payload": order.payload,
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
            "buy_trigger_price",
            "entry_trigger_price",
            "trigger_price",
        ):
            if payload.get(key) is not None:
                return True
    return False
