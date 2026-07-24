from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import func, select
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
from app.services.live_market_context import load_live_market_regime_context, merge_live_market_regime_context
from app.services.plan_item_identity import serialize_plan_item_source_identity
from app.services.broker_order_state import active_local_broker_order_filters
from app.services.trade_outcome_feedback import load_latest_trade_outcome_feedback_snippet

TRIGGER_TIME_EXEC_AGENT_VERSION = "trigger-time-exec-agent-v1"
TRIGGER_TIME_REASON_MAX_LENGTH = 1000
OPEN_ORDER_STATUSES = frozenset(
    {
        "submission_pending",
        "submitted",
        "open",
        "partially_filled",
        "retry_pending",
        "cancel_pending",
    }
)
AGENT_PENDING_STATUS = "triggered_pending_agent"
AGENT_DELAY_STATUS = "agent_delay"
AGENT_REJECTED_STATUS = "agent_rejected"
AGENT_APPROVED_PENDING_SUBMIT_STATUS = "agent_approved_pending_submit"
AGENT_TERMINAL_REJECTED_STATUSES = frozenset({AGENT_REJECTED_STATUS})
AGENT_ACTIVE_STATUSES = frozenset(
    {
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
    }
)


class TriggerTimeExecAgentDecision(BaseModel):
    model_config = ConfigDict(extra="allow")

    decision: Literal["execute_now", "keep_watching", "delay", "reject"]
    side: Literal["buy", "sell"]
    quantity: int | None = Field(default=None, gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    updated_target_price: Decimal | None = Field(default=None, gt=0)
    updated_stop_price: Decimal | None = Field(default=None, gt=0)
    valid_until: datetime | None = None
    reason: str = Field(min_length=1, max_length=1000)
    risk_notes: list[str] = Field(default_factory=list)
    confidence: Decimal | None = Field(default=None, ge=Decimal("0"), le=Decimal("1"))
    context_staleness_ok: bool = True

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("reason must not be empty")
        return normalized

    @field_validator("risk_notes")
    @classmethod
    def normalize_risk_notes(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            if not isinstance(value, str):
                raise ValueError("risk_notes must contain strings")
            stripped = value.strip()
            if stripped:
                normalized.append(stripped)
        return normalized

    @model_validator(mode="after")
    def validate_execute_now(self) -> TriggerTimeExecAgentDecision:
        if self.decision == "execute_now":
            if self.quantity is None:
                raise ValueError("execute_now requires quantity")
            if not self.context_staleness_ok:
                raise ValueError("execute_now requires context_staleness_ok=true")
        return self


@dataclass(frozen=True)
class TriggerTimeExecAgentResolution:
    decision: TriggerTimeExecAgentDecision
    request_payload: dict[str, object]
    request_payload_hash: str
    response_payload: dict[str, object]
    decided_at: datetime
    source: str
    error: str | None = None


def trigger_time_exec_agent_enabled(*, settings: Settings, item: ExecutionPlanItem) -> bool:
    if not getattr(settings, "exec_agent_enabled", False):
        return False
    if not getattr(settings, "exec_agent_trigger_time_enabled", True):
        return False
    if item.side == "sell" and not getattr(settings, "exec_agent_trigger_time_sell_enabled", True):
        return False
    if deterministic_emergency_stop_required(item):
        return False
    return item.side in {"buy", "sell"}


def deterministic_emergency_stop_required(item: ExecutionPlanItem) -> bool:
    if item.side != "sell" or item.intent != "stop_sell":
        return False
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    for key in (
        "deterministic_emergency_stop",
        "emergency_stop",
        "bypass_trigger_time_exec_agent",
    ):
        if risk_context.get(key) is True:
            return True
    execution_policy = risk_context.get("execution_policy")
    if isinstance(execution_policy, str) and execution_policy.strip().lower() in {
        "deterministic_emergency_stop",
        "emergency_stop",
    }:
        return True
    return False


def resolve_trigger_time_exec_agent_decision(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    observed_at: datetime | None,
    triggered_at: datetime,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    intended_quantity: int | None,
    live_market_context: dict[str, object] | None = None,
    remaining_quantity: int | None = None,
) -> TriggerTimeExecAgentResolution:
    side = "buy" if item.side == "buy" else "sell"
    market_context, stale_reason = _market_context(
        settings=settings,
        item=item,
        observed_price=observed_price,
        observed_at=observed_at,
        triggered_at=triggered_at,
        trigger_reason=trigger_reason,
        trigger_metadata=trigger_metadata,
        live_market_context=live_market_context,
    )
    if stale_reason is not None:
        request_payload = _request_payload(
            settings=settings,
            item=item,
            side=side,
            market_context=market_context,
            account_context={"available": False, "reason": stale_reason},
            peer_context=_peer_context(settings=settings, db=db, item=item),
            intended_quantity=intended_quantity,
            remaining_quantity=remaining_quantity,
        )
        return _local_resolution(
            request_payload=request_payload,
            side=side,
            decision="delay",
            reason=stale_reason,
            source="exec_runtime_context_gate",
            context_staleness_ok=False,
        )
    if side == "buy" and market_context.get("live_execution_context_available") is not True:
        missing_fields = market_context.get("missing_critical_fields")
        missing_text = ", ".join(missing_fields) if isinstance(missing_fields, list) else "unknown"
        request_payload = _request_payload(
            settings=settings,
            item=item,
            side=side,
            market_context=market_context,
            account_context={"available": False, "reason": "live execution quality gate blocked before account load"},
            peer_context=_peer_context(settings=settings, db=db, item=item),
            intended_quantity=intended_quantity,
            remaining_quantity=remaining_quantity,
        )
        return _local_resolution(
            request_payload=request_payload,
            side=side,
            decision="delay",
            reason=f"live execution quality context is incomplete for trigger-time buy: {missing_text}",
            source="exec_runtime_market_quality_gate",
            context_staleness_ok=False,
        )

    account_context = _load_account_context(
        settings=settings,
        db=db,
        adapter=adapter,
        item=item,
        observed_price=observed_price,
    )
    request_payload = _request_payload(
        settings=settings,
        item=item,
        side=side,
        market_context=market_context,
        account_context=account_context,
        peer_context=_peer_context(settings=settings, db=db, item=item),
        intended_quantity=intended_quantity,
        remaining_quantity=remaining_quantity,
    )
    if account_context.get("available") is not True:
        return _local_resolution(
            request_payload=request_payload,
            side=side,
            decision="delay",
            reason=str(account_context.get("reason") or "account context unavailable"),
            source="exec_runtime_context_gate",
        )

    try:
        response_payload = _request_exec_agent_decision(
            settings=settings,
            payload=request_payload,
        )
        decision = TriggerTimeExecAgentDecision.model_validate(response_payload)
    except (ValidationError, ValueError, TypeError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        safe_reason = _bounded_reason(
            "trigger-time exec-agent unavailable or invalid",
            detail=str(exc),
        )
        return _local_resolution(
            request_payload=request_payload,
            side=side,
            decision="delay",
            reason=safe_reason,
            source="exec_runtime_context_gate",
            error=str(exc),
        )

    if decision.side != side:
        return _local_resolution(
            request_payload=request_payload,
            side=side,
            decision="delay",
            reason=f"trigger-time exec-agent returned side={decision.side} for {side} item",
            source="exec_runtime_context_gate",
        )
    if decision.decision == "execute_now" and side == "sell" and remaining_quantity is not None:
        assert decision.quantity is not None
        if decision.quantity > remaining_quantity:
            return _local_resolution(
                request_payload=request_payload,
                side=side,
                decision="reject",
                reason=(
                    "trigger-time exec-agent quantity exceeds remaining sellable "
                    f"position: {decision.quantity}>{remaining_quantity}"
                ),
                source="exec_runtime_context_gate",
                response_payload={
                    "decision": "reject",
                    "side": side,
                    "quantity": None,
                    "reason": (
                        "trigger-time exec-agent quantity exceeds remaining sellable "
                        f"position: {decision.quantity}>{remaining_quantity}"
                    ),
                    "risk_notes": ["agent_response_quantity_exceeded_remaining_position"],
                    "context_staleness_ok": True,
                    "agent_response": response_payload,
                },
            )

    return TriggerTimeExecAgentResolution(
        decision=decision,
        request_payload=request_payload,
        request_payload_hash=_payload_hash(request_payload),
        response_payload=_json_safe(response_payload),
        decided_at=datetime.now(UTC),
        source="openclaw_exec_agent",
    )


def record_trigger_time_exec_agent_interaction(
    *,
    item: ExecutionPlanItem,
    resolution: TriggerTimeExecAgentResolution,
    previous_status: str,
    new_status: str,
    recorded_at: datetime,
    order: dict[str, object] | None = None,
) -> None:
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    interactions = risk_context.get("trigger_time_exec_agent_interactions")
    if not isinstance(interactions, list):
        interactions = []
    interaction = {
        "version": TRIGGER_TIME_EXEC_AGENT_VERSION,
        "source": resolution.source,
        "request_payload_hash": resolution.request_payload_hash,
        "request_payload": _json_safe(resolution.request_payload),
        "response_payload": _json_safe(resolution.response_payload),
        "decision": resolution.decision.model_dump(mode="json"),
        "model": resolution.request_payload.get("model"),
        "agent_id": resolution.request_payload.get("agent_id"),
        "decision_timestamp": resolution.decided_at.isoformat(),
        "context_timestamps": _context_timestamps(resolution.request_payload),
        "previous_monitoring_state": previous_status,
        "new_monitoring_state": new_status,
        "recorded_at": recorded_at.isoformat(),
    }
    if resolution.error is not None:
        interaction["error"] = resolution.error[-1000:]
    if order is not None:
        interaction["order"] = order
    risk_context["last_trigger_time_exec_agent_interaction"] = interaction
    risk_context["trigger_time_exec_agent_interactions"] = [*interactions[-19:], interaction]
    item.risk_context = risk_context


def attach_order_to_last_trigger_time_interaction(
    *,
    item: ExecutionPlanItem,
    order: dict[str, object] | None,
    new_status: str,
    recorded_at: datetime,
) -> None:
    if order is None:
        return
    risk_context = dict(item.risk_context) if isinstance(item.risk_context, dict) else {}
    last = risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(last, dict):
        return
    updated = {
        **last,
        "order": order,
        "new_monitoring_state": new_status,
        "order_recorded_at": recorded_at.isoformat(),
    }
    interactions = risk_context.get("trigger_time_exec_agent_interactions")
    if isinstance(interactions, list) and interactions:
        interactions = [*interactions[:-1], updated]
    else:
        interactions = [updated]
    risk_context["last_trigger_time_exec_agent_interaction"] = updated
    risk_context["trigger_time_exec_agent_interactions"] = interactions[-20:]
    item.risk_context = risk_context


def _request_payload(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    side: Literal["buy", "sell"],
    market_context: dict[str, object],
    account_context: dict[str, object],
    peer_context: dict[str, object],
    intended_quantity: int | None,
    remaining_quantity: int | None,
) -> dict[str, object]:
    plan = item.plan
    return {
        "version": TRIGGER_TIME_EXEC_AGENT_VERSION,
        "agent_id": getattr(settings, "exec_agent_id", "exec-agent"),
        "model": getattr(settings, "exec_agent_model", "openai-codex/gpt-5.5"),
        "task": (
            "Make the final trigger-time execution decision immediately before broker submission. "
            "Return JSON only with decision execute_now, keep_watching, delay, or reject. "
            "For buys, execute_now is allowed only when market_context.live_execution_context_available is true."
        ),
        "plan": {
            "plan_id": plan.external_plan_id if plan is not None else None,
            "market_date": plan.market_date.isoformat() if plan is not None and plan.market_date else None,
            "strategy_run_id": plan.strategy_run_id if plan is not None else None,
            "source": plan.source if plan is not None else None,
        },
        "signal_context": _signal_context(item=item, side=side),
        "trade_outcome_feedback": load_latest_trade_outcome_feedback_snippet(settings),
        "market_context": market_context,
        "account_context": account_context,
        "peer_context": peer_context,
        "execution_context": {
            "intended_quantity": intended_quantity,
            "remaining_sellable_quantity": remaining_quantity,
            "current_status": item.status,
            "time_in_force": item.time_in_force,
            "exchange": _plan_item_exchange(item),
        },
        "expected_response_schema": {
            "decision": "execute_now|keep_watching|delay|reject",
            "side": "buy|sell",
            "quantity": "positive integer required for execute_now",
            "limit_price": "optional positive decimal",
            "updated_target_price": "optional positive decimal",
            "updated_stop_price": "optional positive decimal",
            "valid_until": "optional ISO datetime for delay/keep_watching",
            "reason": "required string",
            "risk_notes": "array of strings",
            "confidence": "optional 0..1 decimal",
            "context_staleness_ok": "boolean; execute_now requires true",
        },
    }


def _signal_context(*, item: ExecutionPlanItem, side: Literal["buy", "sell"]) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    return {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "side": side,
        "intent": item.intent,
        "source_identity": serialize_plan_item_source_identity(item),
        "trigger_price": _decimal_text(item.trigger_price),
        "limit_price": _decimal_text(item.limit_price),
        "quantity_policy": quantity_policy,
        "original_target_price": _first_text(
            risk_context,
            quantity_policy,
            keys=("target_price", "take_profit_price", "profit_target_price", "target"),
        ),
        "original_stop_price": _first_text(
            risk_context,
            quantity_policy,
            keys=("stop_price", "stop_sell_price", "stop_loss_price", "risk_stop_price", "stop"),
        ),
        "confidence": _first_text(
            risk_context,
            quantity_policy,
            keys=("confidence", "confidence_score_10", "score", "probability"),
        ),
        "conviction": _first_text(risk_context, quantity_policy, keys=("conviction", "conviction_score_10")),
        "scoring_summary": _compact_dict(
            risk_context,
            keys=(
                "score",
                "expected_return_pct",
                "liquidity_tier",
                "target_profile",
                "target_tag",
                "supporting_evidence",
            ),
        ),
        "market_regime_context": _first_dict(
            risk_context,
            keys=("market_regime", "market_context", "market_regime_context"),
        ),
        "rationale": _first_text(
            risk_context,
            quantity_policy,
            keys=("rationale", "reason", "thesis_summary", "decision_rationale"),
        ),
        "risks": risk_context.get("decision_risks") or risk_context.get("risk_flags"),
        "decider_input": _dict_or_none(risk_context.get("decider_input")),
        "decider_output": _dict_or_none(risk_context.get("decider_output")),
        "decider_handoff_context": _dict_or_none(risk_context.get("decider_handoff_context")),
    }


def _market_context(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
    observed_at: datetime | None,
    triggered_at: datetime,
    trigger_reason: str | None,
    trigger_metadata: dict[str, object] | None,
    live_market_context: dict[str, object] | None,
) -> tuple[dict[str, object], str | None]:
    normalized_observed_at = _normalize_utc(observed_at) if observed_at is not None else None
    stale_reason = None
    quote_age_seconds = None
    if observed_price is None:
        stale_reason = "quote is missing for trigger-time exec-agent decision"
    elif normalized_observed_at is None:
        stale_reason = "quote timestamp is missing for trigger-time exec-agent decision"
    else:
        quote_age_seconds = (triggered_at - normalized_observed_at).total_seconds()
        if quote_age_seconds > settings.monitoring_max_quote_age_seconds:
            stale_reason = (
                "quote is stale for trigger-time exec-agent decision: "
                f"{quote_age_seconds:.3f}s>{settings.monitoring_max_quote_age_seconds}s"
            )
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    market_payload = _first_dict(risk_context, keys=("market_data", "quote", "orderbook", "order_book")) or {}
    live_payload = merge_live_market_regime_context(
        live_market_context or {},
        load_live_market_regime_context(settings=settings),
    )
    live_orderbook_payload = _first_dict(live_payload, keys=("orderbook", "order_book")) or {}
    session_type = (
        _first_payload_value(
            live_payload,
            keys=("session_type", "session", "market_state", "route_now"),
        )
    )
    bid = _first_payload_value(live_payload, keys=("bid", "bid_price", "best_bid"))
    ask = _first_payload_value(live_payload, keys=("ask", "ask_price", "best_ask"))
    spread = _first_payload_value(
        live_payload,
        keys=("spread", "spread_price"),
    )
    spread_bps = _first_payload_value(
        live_payload,
        keys=("spread_bps", "spread_bp"),
    )
    orderbook_depth = _first_payload_value(
        live_payload,
        live_orderbook_payload,
        keys=("orderbook_depth", "book_depth", "total_depth", "depth"),
    )
    orderbook_imbalance = _first_payload_value(
        live_payload,
        live_orderbook_payload,
        keys=("orderbook_imbalance", "book_imbalance", "imbalance"),
    )
    volume = _first_payload_value(
        live_payload,
        keys=("volume", "accumulated_volume"),
    )
    turnover = _first_payload_value(
        live_payload,
        keys=("turnover", "trading_value"),
    )
    normal_ordering_available = _first_payload_value(
        live_payload,
        keys=("normal_ordering_available", "ordering_available"),
    )
    missing_fields = _live_execution_quality_missing_fields(
        bid=bid,
        ask=ask,
        orderbook_depth=orderbook_depth,
        orderbook_imbalance=orderbook_imbalance,
        volume=volume,
        turnover=turnover,
        session_type=session_type,
        normal_ordering_available=normal_ordering_available,
    )
    context_available = stale_reason is None and not missing_fields
    field_freshness = {
        "latest_price": {"available": observed_price is not None, "observed_at": normalized_observed_at.isoformat() if normalized_observed_at is not None else None},
        "bid": _field_presence(bid, source="live_market_context"),
        "ask": _field_presence(ask, source="live_market_context"),
        "orderbook": _field_presence(orderbook_depth if orderbook_depth is not None else orderbook_imbalance, source="live_market_context"),
        "volume": _field_presence(volume, source="live_market_context"),
        "turnover": _field_presence(turnover, source="live_market_context"),
        "session_type": _field_presence(session_type, source="live_market_context"),
        "normal_ordering_available": _field_presence(normal_ordering_available, source="live_market_context"),
    }
    return (
        {
            "symbol": item.symbol,
            "latest_price": _decimal_text(observed_price),
            "observed_at": normalized_observed_at.isoformat() if normalized_observed_at is not None else None,
            "triggered_at": triggered_at.isoformat(),
            "quote_age_seconds": quote_age_seconds,
            "max_quote_age_seconds": settings.monitoring_max_quote_age_seconds,
            "context_staleness_ok": stale_reason is None,
            "stale_reason": stale_reason,
            "live_execution_context_available": context_available,
            "missing_critical_fields": missing_fields,
            "field_freshness": _json_safe(field_freshness),
            "live_execution_quality_gate": {
                "available": context_available,
                "missing_fields": missing_fields,
                "stale_reason": stale_reason,
            },
            "bid": _json_scalar(bid),
            "ask": _json_scalar(ask),
            "spread": _json_scalar(spread),
            "spread_bps": _json_scalar(spread_bps),
            "orderbook_depth": _json_scalar(orderbook_depth),
            "orderbook_imbalance": _json_scalar(orderbook_imbalance),
            "volume": _json_scalar(volume),
            "turnover": _json_scalar(turnover),
            "session_type": str(session_type) if session_type is not None else None,
            "normal_ordering_available": normal_ordering_available,
            "trigger_reason": trigger_reason or item.intent,
            "trigger_metadata": trigger_metadata,
            "raw_market_payload": _json_safe(market_payload),
            "live_market_context": _json_safe(live_payload),
        },
        stale_reason,
    )


def _load_account_context(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    item: ExecutionPlanItem,
    observed_price: Decimal | None,
) -> dict[str, object]:
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="trigger_time_exec_agent",
            reason=f"load account context for trigger-time {item.side} decision",
        )
        try:
            snapshot = adapter.get_account_snapshot(
                session_state.access_token or "",
                symbols=[item.symbol],
                reference_prices=(
                    {item.symbol: observed_price}
                    if observed_price is not None
                    else None
                ),
                exchange=_plan_item_exchange(item),
            )
        except TypeError:
            snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError, TypeError) as exc:
        return {
            "available": False,
            "reason": f"{exc.__class__.__name__}: {exc}",
            "observed_at": datetime.now(UTC).isoformat(),
        }
    return _account_context_payload(settings=settings, db=db, snapshot=snapshot)


def _account_context_payload(
    *,
    settings: Settings,
    db: Session,
    snapshot: BrokerAccountSnapshot,
) -> dict[str, object]:
    positions = snapshot.positions or snapshot.holdings
    holdings = [
        {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "sellable_quantity": getattr(position, "sellable_quantity", None),
            "average_price": _decimal_text(getattr(position, "average_price", None)),
        }
        for position in positions
        if position.quantity > 0
    ]
    local_positions = [
        {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "average_price": _decimal_text(position.average_price),
            "last_reconciled_at": position.last_reconciled_at.isoformat() if position.last_reconciled_at else None,
        }
        for position in db.scalars(
            select(ExecutionPosition).where(ExecutionPosition.mode == settings.runtime_mode)
        ).all()
        if position.quantity > 0
    ]
    open_orders = [
        _open_order_payload(order)
        for order in [*snapshot.open_orders, *_local_open_orders(settings=settings, db=db)]
    ]
    return {
        "available": True,
        "source": snapshot.source,
        "observed_at": _normalize_utc(snapshot.observed_at).isoformat(),
        "orderable_cash_krw": _decimal_text(snapshot.available_cash),
        "buying_power_krw": _decimal_text(snapshot.buying_power),
        "holding_count": len(holdings),
        "holdings": holdings,
        "local_positions": local_positions,
        "open_order_count": len(open_orders),
        "open_orders": open_orders,
        "symbol_buying_power": {
            symbol: {
                "available_cash": _decimal_text(power.available_cash),
                "buying_power": _decimal_text(power.buying_power),
                "buyable_quantity": power.buyable_quantity,
            }
            for symbol, power in snapshot.symbol_buying_power.items()
        },
    }


def _local_open_orders(*, settings: Settings, db: Session) -> list[BrokerAccountOpenOrder]:
    orders = db.scalars(
        select(BrokerOrder).where(
            *active_local_broker_order_filters(
                settings=settings,
                statuses=OPEN_ORDER_STATUSES,
            )
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


def _open_order_payload(order: BrokerAccountOpenOrder) -> dict[str, object]:
    return {
        "symbol": order.symbol,
        "side": order.side,
        "quantity": order.quantity,
        "remaining_quantity": order.remaining_quantity,
        "status": order.status,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "source_candidate_id": order.source_candidate_id,
        "limit_price": _decimal_text(order.limit_price),
    }


def _peer_context(*, settings: Settings, db: Session, item: ExecutionPlanItem) -> dict[str, object]:
    active_statuses = (
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "target_sell_monitoring",
        "stop_sell_monitoring",
        AGENT_PENDING_STATUS,
        AGENT_DELAY_STATUS,
        AGENT_APPROVED_PENDING_SUBMIT_STATUS,
        "watch_buy_triggered_pending_dispatch",
        "target_sell_triggered_pending_dispatch",
        "stop_sell_triggered_pending_dispatch",
    )
    candidates = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.id != item.id,
            ExecutionPlanItem.status.in_(active_statuses),
        )
        .order_by(ExecutionPlanItem.updated_at.desc(), ExecutionPlanItem.id.asc())
        .limit(25)
    ).all()
    pending_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.status == AGENT_PENDING_STATUS,
            )
        )
        or 0
    )
    delayed_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlanItem.status == AGENT_DELAY_STATUS,
            )
        )
        or 0
    )
    return {
        "active_candidate_count": len(candidates),
        "pending_agent_decision_count": pending_count,
        "agent_delayed_count": delayed_count,
        "active_candidates": [
            {
                "execution_plan_item_id": candidate.id,
                "symbol": candidate.symbol,
                "side": candidate.side,
                "intent": candidate.intent,
                "status": candidate.status,
                "trigger_price": _decimal_text(candidate.trigger_price),
            }
            for candidate in candidates
        ],
    }


def _request_exec_agent_decision(*, settings: Settings, payload: dict[str, object]) -> dict[str, object]:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2)
        request_path = Path(handle.name)
    prompt = f"""
Read {request_path} and make the final trigger-time execution decision.

Use only this compact current execution context. Return JSON only:
{{
  "decision": "execute_now|keep_watching|delay|reject",
  "side": "buy|sell",
  "quantity": 1,
  "limit_price": 1234,
  "updated_target_price": 1234,
  "updated_stop_price": 1234,
  "valid_until": "2026-06-05T01:23:45Z",
  "reason": "...",
  "risk_notes": ["..."],
  "confidence": 0.0,
  "context_staleness_ok": true
}}

Only execute_now when quote and account context are fresh enough for broker submission.
For buy decisions, only execute_now when market_context.live_execution_context_available is true.
If bid/ask/orderbook/volume/turnover/session quality is missing, return delay or keep_watching.
If trade_outcome_feedback is present, apply recent loss lessons as hard downsize/reject pressure.
""".strip()
    timeout_seconds = getattr(settings, "exec_agent_trigger_time_timeout_seconds", 90)
    command = [
        settings.openclaw_cli_path,
        "agent",
        "--agent",
        settings.exec_agent_id,
        "--session-key",
        f"trigger-exec-{uuid.uuid4()}",
        "--thinking",
        "low",
        "--message",
        prompt,
        "--json",
        "--timeout",
        str(timeout_seconds),
    ]
    completed = _run_exec_agent_command(command, timeout_seconds=timeout_seconds + 30)
    response = json.loads(completed.stdout)
    return _parse_json_text(_agent_response_text(response))


def _run_exec_agent_command(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_group(process)
        stdout, stderr = process.communicate()
        raise subprocess.TimeoutExpired(
            cmd=command,
            timeout=timeout_seconds,
            output=stdout,
            stderr=stderr,
        ) from exc
    completed = subprocess.CompletedProcess(
        args=command,
        returncode=process.returncode,
        stdout=stdout,
        stderr=stderr,
    )
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=stdout,
            stderr=stderr,
        )
    return completed


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def _agent_response_text(response: dict[str, object]) -> str:
    result = response.get("result")
    if isinstance(result, dict):
        payloads = result.get("payloads")
        if isinstance(payloads, list):
            texts = [payload.get("text") for payload in payloads if isinstance(payload, dict)]
            joined = "\n".join(text for text in texts if isinstance(text, str) and text.strip())
            if joined:
                return joined
    raise ValueError("OpenClaw exec-agent response did not include text payload")


def _parse_json_text(text: str) -> dict[str, object]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:].strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end < start:
            raise
        parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("exec-agent response JSON must be an object")
    return parsed


def _local_resolution(
    *,
    request_payload: dict[str, object],
    side: Literal["buy", "sell"],
    decision: Literal["keep_watching", "delay", "reject"],
    reason: str,
    source: str,
    context_staleness_ok: bool = True,
    response_payload: dict[str, object] | None = None,
    error: str | None = None,
) -> TriggerTimeExecAgentResolution:
    safe_reason = _bounded_reason(reason)
    payload = response_payload or {
        "decision": decision,
        "side": side,
        "quantity": None,
        "reason": safe_reason,
        "risk_notes": [safe_reason],
        "confidence": None,
        "context_staleness_ok": context_staleness_ok,
    }
    if response_payload is not None:
        payload = dict(response_payload)
        payload["reason"] = _bounded_reason(str(payload.get("reason") or safe_reason))
        risk_notes = payload.get("risk_notes")
        if isinstance(risk_notes, list):
            payload["risk_notes"] = [
                _bounded_reason(str(note))
                for note in risk_notes
                if str(note).strip()
            ]
    parsed = TriggerTimeExecAgentDecision.model_validate(payload)
    return TriggerTimeExecAgentResolution(
        decision=parsed,
        request_payload=request_payload,
        request_payload_hash=_payload_hash(request_payload),
        response_payload=_json_safe(payload),
        decided_at=datetime.now(UTC),
        source=source,
        error=error,
    )


def _bounded_reason(prefix: str, *, detail: str | None = None) -> str:
    value = prefix.strip()
    if detail is not None and detail.strip():
        value = f"{value}: {detail.strip()}"
    if not value:
        value = "trigger-time exec-agent fallback"
    if len(value) <= TRIGGER_TIME_REASON_MAX_LENGTH:
        return value
    suffix = " ... [truncated]"
    return value[: TRIGGER_TIME_REASON_MAX_LENGTH - len(suffix)].rstrip() + suffix


def _payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _context_timestamps(request_payload: dict[str, object]) -> dict[str, object]:
    market_context = request_payload.get("market_context")
    account_context = request_payload.get("account_context")
    return {
        "market_observed_at": (
            market_context.get("observed_at")
            if isinstance(market_context, dict)
            else None
        ),
        "triggered_at": (
            market_context.get("triggered_at")
            if isinstance(market_context, dict)
            else None
        ),
        "account_observed_at": (
            account_context.get("observed_at")
            if isinstance(account_context, dict)
            else None
        ),
    }


def _plan_item_exchange(item: ExecutionPlanItem) -> str:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    value = risk_context.get("exchange")
    if isinstance(value, str) and value.strip():
        return value.strip().upper()
    return "KRX"


def _first_text(*payloads: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for payload in payloads:
        for key in keys:
            value = payload.get(key)
            if value is not None and not isinstance(value, (dict, list)):
                text = str(value).strip()
                if text:
                    return text
    return None


def _live_execution_quality_missing_fields(
    *,
    bid: object | None,
    ask: object | None,
    orderbook_depth: object | None,
    orderbook_imbalance: object | None,
    volume: object | None,
    turnover: object | None,
    session_type: object | None,
    normal_ordering_available: object | None,
) -> list[str]:
    missing: list[str] = []
    if bid is None:
        missing.append("bid")
    if ask is None:
        missing.append("ask")
    if orderbook_depth is None and orderbook_imbalance is None:
        missing.append("orderbook")
    if volume is None:
        missing.append("volume")
    if turnover is None:
        missing.append("turnover")
    if session_type is None:
        missing.append("session_type")
    if normal_ordering_available is not True:
        missing.append("normal_ordering_available")
    return missing


def _field_presence(value: object | None, *, source: str) -> dict[str, object]:
    return {
        "available": value is not None,
        "source": source if value is not None else None,
    }


def _compact_dict(payload: dict[str, object], *, keys: tuple[str, ...]) -> dict[str, object]:
    return {
        key: _json_safe(payload[key])
        for key in keys
        if key in payload and payload[key] is not None
    }


def _first_dict(payload: dict[str, object], *, keys: tuple[str, ...]) -> dict[str, object] | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return None


def _dict_or_none(value: object) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _first_payload_value(*payloads: dict[str, object], keys: tuple[str, ...]) -> object | None:
    for payload in payloads:
        for key in keys:
            if key in payload and payload[key] is not None:
                return payload[key]
    return None


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else f"{value:.4f}"


def _json_scalar(value: object) -> object | None:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value
