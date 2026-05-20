from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem

LOGGER = logging.getLogger(__name__)

WATCH_BUY_INTENT = "intraday_watch_buy"
SIZED_WATCH_STATUSES = frozenset({"watch_monitoring_pending", "watch_monitoring_active", "unsupported_semantics"})
TERMINAL_WATCH_STATUSES = frozenset({
    "exec_agent_rejected",
    "watch_buy_rejected",
    "watch_buy_submitted",
    "broker_order_submitted",
    "filled",
    "canceled",
    "cancelled",
    "expired",
    "failed",
})


@dataclass(frozen=True)
class WatchExecAgentSizingResult:
    status_code: int
    payload: dict[str, object]


def run_due_watch_exec_agent_sizing(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> WatchExecAgentSizingResult:
    """Ask OpenClaw exec-agent for daily intraday watch-buy quantities once the 08:00 window opens."""
    if not settings.exec_agent_watch_sizing_enabled:
        return _result(idle=True, reason="disabled")
    if not settings.exec_agent_enabled:
        return _result(idle=True, reason="exec_agent_disabled")

    cycle_at = now or datetime.now(UTC)
    if not _inside_sizing_window(settings=settings, now=cycle_at):
        return _result(idle=True, reason="outside_sizing_window")

    local_today = cycle_at.astimezone(ZoneInfo(settings.market_timezone)).date()
    plans = _load_candidate_plans(settings=settings, db=db, market_date=local_today)
    if not plans:
        return _result(idle=True, reason="no_unsized_watch_candidates")

    plan_payloads: list[dict[str, object]] = []
    applied: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    for plan in plans:
        candidates = [_candidate_payload(item) for item in _unsized_watch_items(plan)]
        if not candidates:
            continue
        request_payload = {
            "version": "watch-exec-agent-sizing-request-v1",
            "plan_id": plan.external_plan_id,
            "market_date": plan.market_date.isoformat() if plan.market_date else None,
            "strategy_run_id": plan.strategy_run_id,
            "task": (
                "Return exec-agent sizing decisions for intraday watch buys. "
                "Approve only if risk/reward/liquidity/cash make sense. "
                "Quantity must be positive shares for approve, or reject with a reason. "
                "These are pending trigger-limit buys; use trigger_price/reference and chase limit."
            ),
            "candidates": candidates,
        }
        try:
            decision_payload = _request_exec_agent_decisions(settings=settings, payload=request_payload)
            summary = _apply_decisions(db=db, plan=plan, decision_payload=decision_payload)
            applied.append(summary)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            LOGGER.exception(
                "watch_exec_agent_sizing_failed",
                extra={"plan_id": plan.external_plan_id, "runtime_mode": settings.runtime_mode},
            )
            errors.append({"plan_id": plan.external_plan_id, "error": str(exc)})
        plan_payloads.append({"plan_id": plan.external_plan_id, "candidate_count": len(candidates)})

    db.commit()
    return _result(
        idle=not applied and not errors,
        plans=plan_payloads,
        applied=applied,
        errors=errors,
    )


def _inside_sizing_window(*, settings: Settings, now: datetime) -> bool:
    tz = ZoneInfo(settings.market_timezone)
    local_now = now.astimezone(tz).time()
    return _parse_hhmm(settings.exec_agent_watch_sizing_start_local_time) <= local_now <= _parse_hhmm(
        settings.exec_agent_watch_sizing_end_local_time
    )


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(hour=int(hour), minute=int(minute))


def _load_candidate_plans(*, settings: Settings, db: Session, market_date) -> list[ExecutionPlan]:
    return db.scalars(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            ExecutionPlan.items.any(
                (ExecutionPlanItem.intent == WATCH_BUY_INTENT)
                & (ExecutionPlanItem.status.in_(tuple(SIZED_WATCH_STATUSES)))
            ),
        )
        .options(selectinload(ExecutionPlan.items))
        .order_by(ExecutionPlan.accepted_at.asc(), ExecutionPlan.created_at.asc())
    ).all()


def _unsized_watch_items(plan: ExecutionPlan) -> list[ExecutionPlanItem]:
    items: list[ExecutionPlanItem] = []
    for item in plan.items:
        if item.intent != WATCH_BUY_INTENT or item.side != "buy":
            continue
        if item.status in TERMINAL_WATCH_STATUSES:
            continue
        if _has_exec_agent_decision(item):
            continue
        if _quantity_is_shares(item.quantity_policy):
            continue
        items.append(item)
    return sorted(items, key=lambda item: (item.symbol, item.plan_item_index, item.id))


def _has_exec_agent_decision(item: ExecutionPlanItem) -> bool:
    for payload in (item.risk_context, item.quantity_policy):
        if isinstance(payload, dict) and isinstance(payload.get("exec_agent_decision"), dict):
            return True
    return False


def _quantity_is_shares(quantity_policy: dict[str, object] | None) -> bool:
    if not isinstance(quantity_policy, dict):
        return False
    return quantity_policy.get("kind") == "shares" and _positive_int(quantity_policy.get("quantity")) is not None


def _candidate_payload(item: ExecutionPlanItem) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    execution_feasibility = risk_context.get("execution_feasibility_context")
    if not isinstance(execution_feasibility, dict):
        execution_feasibility = {}
    return {
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "intent": item.intent,
        "side": item.side,
        "status": item.status,
        "trigger_price": _json_decimal(item.trigger_price),
        "chase_limit_price": _json_decimal(item.limit_price),
        "budget_krw": quantity_policy.get("budget_krw") or risk_context.get("allocation_budget_krw"),
        "max_safe_notional_krw": quantity_policy.get("max_safe_notional_krw")
        or execution_feasibility.get("max_safe_notional_krw"),
        "sizing_hints": quantity_policy.get("sizing_hints") or execution_feasibility.get("sizing_hints"),
        "liquidity_tier": quantity_policy.get("liquidity_tier") or execution_feasibility.get("liquidity_tier"),
        "target_price": risk_context.get("target_price"),
        "stop_loss_price": risk_context.get("stop_loss_price"),
        "expected_return_pct": risk_context.get("expected_return_pct"),
        "confidence_score_10": risk_context.get("confidence_score_10"),
        "conviction_score_10": risk_context.get("conviction_score_10"),
        "thesis_summary": risk_context.get("thesis_summary"),
        "decision_risks": risk_context.get("decision_risks"),
        "data_quality_warnings": risk_context.get("data_quality_warnings"),
        "supporting_evidence": risk_context.get("supporting_evidence"),
    }


def _request_exec_agent_decisions(*, settings: Settings, payload: dict[str, object]) -> dict[str, object]:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2)
        request_path = Path(handle.name)
    prompt = f"""
긴급: {request_path} 파일의 intraday_watch_buy 후보를 지금 수량 판단하세요.

요구사항:
- 각 candidate별 approve 또는 reject 판단.
- approve 시 positive integer quantity, approved_notional_krw, sizing_price, confidence, reason_code, reason 포함.
- reject 시 quantity=0, approved_notional_krw=0, sizing_price, confidence, reason_code, reason 포함.
- max_safe_notional_krw / max_safe_quantity / liquidity_tier / target/stop / confidence/conviction / risks를 반영하세요.
- budget_krw가 0이어도 exec-agent sizing delegated 상태이므로 max_safe_notional과 위험도로 보수적 수량을 정하세요.
- 결과는 반드시 JSON만 출력하세요. 스키마:
{{"version":"manual-exec-agent-watch-sizing-v1","plan_id":"{payload['plan_id']}","decisions":[{{"execution_plan_item_id":"...","symbol":"...","action":"approve|reject","quantity":1,"approved_notional_krw":12345,"sizing_price":1234,"confidence":0.0,"reason_code":"...","reason":"..."}}]}}
""".strip()
    command = [
        settings.openclaw_cli_path,
        "agent",
        "--agent",
        settings.exec_agent_id,
        "--message",
        prompt,
        "--json",
        "--timeout",
        str(settings.exec_agent_watch_sizing_timeout_seconds),
    ]
    completed = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
        timeout=settings.exec_agent_watch_sizing_timeout_seconds + 30,
    )
    response = json.loads(completed.stdout)
    text = _agent_response_text(response)
    return _parse_json_text(text)


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


def _apply_decisions(*, db: Session, plan: ExecutionPlan, decision_payload: dict[str, object]) -> dict[str, object]:
    decisions_raw = decision_payload.get("decisions")
    if not isinstance(decisions_raw, list):
        raise ValueError("exec-agent response missing decisions list")
    decisions = {str(decision.get("execution_plan_item_id")): decision for decision in decisions_raw if isinstance(decision, dict)}
    items = _unsized_watch_items(plan)
    expected_ids = {item.id for item in items}
    if set(decisions) != expected_ids:
        raise ValueError(
            "exec-agent decision id mismatch: "
            f"missing={sorted(expected_ids.difference(decisions))} extra={sorted(set(decisions).difference(expected_ids))}"
        )
    approved = 0
    rejected = 0
    for item in items:
        decision = decisions[item.id]
        action = str(decision.get("action") or "").strip().lower()
        record = _decision_record(decision=decision, item=item, version=str(decision_payload.get("version") or ""))
        risk_context = dict(item.risk_context or {})
        risk_context["exec_agent_decision"] = record
        risk_context["short_term_exec_agent_decision"] = record
        if action == "approve":
            quantity = _positive_int(decision.get("quantity"))
            if quantity is None:
                raise ValueError(f"approved decision has invalid quantity for {item.symbol}")
            previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
            item.quantity_policy = {
                "kind": "shares",
                "quantity": quantity,
                "source": "exec_agent_watch_sizing",
                "sizing_price": decision.get("sizing_price"),
                "approved_notional": decision.get("approved_notional_krw"),
                "confidence": decision.get("confidence"),
                "reason_code": decision.get("reason_code"),
                "previous_policy": previous_policy,
            }
            item.status = "watch_monitoring_active"
            approved += 1
        elif action == "reject":
            item.status = "exec_agent_rejected"
            rejected += 1
        else:
            raise ValueError(f"unknown exec-agent decision action for {item.symbol}: {action}")
        item.risk_context = risk_context
    return {"plan_id": plan.external_plan_id, "evaluated": len(items), "approved": approved, "rejected": rejected}


def _decision_record(*, decision: dict[str, object], item: ExecutionPlanItem, version: str) -> dict[str, object]:
    action = str(decision.get("action") or "").strip().lower()
    quantity = int(decision.get("quantity") or 0)
    return {
        "version": version or "manual-exec-agent-watch-sizing-v1",
        "source": "openclaw_exec_agent_watch_sizing",
        "action": action,
        "decision": "approved" if action == "approve" else "rejected",
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "quantity": quantity,
        "approved_quantity": quantity,
        "approved_notional_krw": decision.get("approved_notional_krw"),
        "sizing_price": decision.get("sizing_price"),
        "reference_price": decision.get("sizing_price"),
        "confidence": decision.get("confidence"),
        "reason_code": decision.get("reason_code"),
        "reason": decision.get("reason"),
        "decided_at": datetime.now(UTC).isoformat(),
    }


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _json_decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


def _result(**payload: object) -> WatchExecAgentSizingResult:
    return WatchExecAgentSizingResult(status_code=200, payload={"worker_name": "watch_exec_agent_sizing", **payload})
