from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload, sessionmaker

from app.broker.base import BrokerAccountSnapshot, BrokerAdapter, BrokerAdapterError
from app.core.config import Settings
from app.db.sqlite_diagnostics import sqlite_write_lock
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.market_regime_guardrails import evaluate_market_regime_guardrail
from app.services.reconciliation import reconcile_stale_nonterminal_broker_orders
from app.services.risk_sizing import apply_vol_corr_quantity_cap
from app.services.trade_outcome_feedback import load_latest_trade_outcome_feedback_snippet

LOGGER = logging.getLogger(__name__)

WATCH_BUY_INTENTS = frozenset({"intraday_watch_buy", "next_open_buy"})
SIZED_WATCH_STATUSES = frozenset({
    "watch_monitoring_pending",
    "watch_monitoring_active",
    "watch_buy_triggered_blocked",
    "unsupported_semantics",
})
WATCH_EXEC_AGENT_BATCH_SIZE = 10
WATCH_EXEC_AGENT_RETRY_BATCH_SIZE = 1
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
    db: Session | None = None,
    session_factory: sessionmaker[Session] | None = None,
    adapter: BrokerAdapter | None = None,
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
    if session_factory is not None:
        return _run_due_watch_exec_agent_sizing_short_transactions(
            settings=settings,
            session_factory=session_factory,
            adapter=adapter,
            cycle_at=cycle_at,
            market_date=local_today,
        )
    if db is None:
        raise ValueError("db or session_factory is required")
    plans = _load_candidate_plans(settings=settings, db=db, market_date=local_today)
    if not plans:
        return _result(idle=True, reason="no_unsized_watch_candidates")

    account_context = _load_sizing_account_context(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=_candidate_symbols(plans),
    )
    plan_payloads: list[dict[str, object]] = []
    applied: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    batches_processed = 0
    max_batches_per_cycle = int(getattr(settings, "exec_agent_watch_sizing_max_batches_per_cycle", 3))
    for plan in plans:
        stale_block_summary = _reject_stale_state_blocked_items(
            db=db,
            plan=plan,
            account_context=account_context,
        )
        if stale_block_summary["rejected"]:
            applied.append(stale_block_summary)
        candidates = [_candidate_payload(item) for item in _unsized_watch_items(plan)]
        if not candidates:
            continue
        candidate_universe = _candidate_universe_payload(candidates)
        batch_size = int(getattr(settings, "exec_agent_watch_sizing_batch_size", WATCH_EXEC_AGENT_BATCH_SIZE))
        for batch in _chunks(candidates, batch_size):
            if batches_processed >= max_batches_per_cycle:
                break
            for request_batch in _sizing_request_batches(batch):
                if batches_processed >= max_batches_per_cycle:
                    break
                batches_processed += 1
                batch_ids = [str(candidate["execution_plan_item_id"]) for candidate in request_batch]
                request_payload = _sizing_request_payload(
                    settings=settings,
                    plan=plan,
                    candidates=request_batch,
                    account_context=account_context,
                    candidate_universe=candidate_universe,
                )
                try:
                    decision_payload = _request_exec_agent_decisions_with_retries(
                        settings=settings,
                        db=db,
                        plan=plan,
                        payload=request_payload,
                        expected_item_ids=set(batch_ids),
                    )
                    if decision_payload is None:
                        LOGGER.warning(
                            "watch_exec_agent_sizing_batch_exhausted",
                            extra={"plan_id": plan.external_plan_id, "batch_item_ids": batch_ids},
                        )
                        errors.append(
                            {
                                "plan_id": plan.external_plan_id,
                                "item_ids": batch_ids,
                                "error": "exec-agent did not return applicable decisions after max attempts",
                            }
                        )
                        continue
                    summary = _apply_decisions(
                        db=db,
                        settings=settings,
                        plan=plan,
                        decision_payload=decision_payload,
                        account_context=account_context,
                        expected_item_ids=set(batch_ids),
                    )
                    applied.append(summary)
                    missing_ids = set(_summary_missing_item_ids(summary))
                    if missing_ids:
                        LOGGER.warning(
                            "watch_exec_agent_sizing_retrying_missing_decisions",
                            extra={
                                "plan_id": plan.external_plan_id,
                                "missing_item_ids": sorted(missing_ids),
                            },
                        )
                        for retry_batch in _chunks(
                            [candidate for candidate in request_batch if str(candidate["execution_plan_item_id"]) in missing_ids],
                            WATCH_EXEC_AGENT_RETRY_BATCH_SIZE,
                        ):
                            retry_ids = [str(candidate["execution_plan_item_id"]) for candidate in retry_batch]
                            retry_payload = _sizing_request_payload(
                                settings=settings,
                                plan=plan,
                                candidates=retry_batch,
                                account_context=account_context,
                                candidate_universe=candidate_universe,
                                retry=True,
                            )
                            retry_decision_payload = _request_exec_agent_decisions_with_retries(
                                settings=settings,
                                db=db,
                                plan=plan,
                                payload=retry_payload,
                                expected_item_ids=set(retry_ids),
                            )
                            if retry_decision_payload is None:
                                errors.append(
                                    {
                                        "plan_id": plan.external_plan_id,
                                        "item_ids": retry_ids,
                                        "error": "exec-agent retry did not return applicable decisions after max attempts",
                                    }
                                )
                                continue
                            applied.append(
                                _apply_decisions(
                                    db=db,
                                    settings=settings,
                                    plan=plan,
                                    decision_payload=retry_decision_payload,
                                    account_context=account_context,
                                    expected_item_ids=set(retry_ids),
                                )
                            )
                except Exception as exc:  # pragma: no cover - defensive runtime guard
                    LOGGER.exception(
                        "watch_exec_agent_sizing_failed",
                        extra={
                            "plan_id": plan.external_plan_id,
                            "runtime_mode": settings.runtime_mode,
                            "batch_item_ids": batch_ids,
                        },
                    )
                    errors.append({"plan_id": plan.external_plan_id, "item_ids": batch_ids, "error": str(exc)})
        plan_payloads.append({"plan_id": plan.external_plan_id, "candidate_count": len(candidates)})
        if batches_processed >= max_batches_per_cycle:
            break

    with sqlite_write_lock(db):
        db.commit()
    return _result(
        idle=not applied and not errors,
        plans=plan_payloads,
        applied=applied,
        errors=errors,
        batches_processed=batches_processed,
        max_batches_per_cycle=max_batches_per_cycle,
    )


def _run_due_watch_exec_agent_sizing_short_transactions(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
    adapter: BrokerAdapter | None,
    cycle_at: datetime,
    market_date,
) -> WatchExecAgentSizingResult:
    with session_factory() as db:
        plans = _load_candidate_plans(settings=settings, db=db, market_date=market_date)
        db.expunge_all()
    if not plans:
        return _result(idle=True, reason="no_unsized_watch_candidates")

    account_context = _load_sizing_account_context_short_transaction(
        settings=settings,
        session_factory=session_factory,
        adapter=adapter,
        symbols=_candidate_symbols(plans),
    )
    stale_blocked_ids, stale_block_summaries = _reject_stale_state_blocked_items_short_transaction(
        session_factory=session_factory,
        plans=plans,
        account_context=account_context,
    )
    plan_payloads: list[dict[str, object]] = []
    applied: list[dict[str, object]] = [*stale_block_summaries]
    errors: list[dict[str, object]] = []
    batches_processed = 0
    max_batches_per_cycle = int(getattr(settings, "exec_agent_watch_sizing_max_batches_per_cycle", 3))
    plans_by_id = {plan.id: plan for plan in plans}
    candidate_work: list[tuple[str, dict[str, object]]] = []
    for plan in plans:
        candidates = []
        for item in _unsized_watch_items(plan):
            if item.id in stale_blocked_ids:
                continue
            candidate = _candidate_payload(item)
            candidate["source_plan_id"] = plan.external_plan_id
            candidates.append(candidate)
        if not candidates:
            continue
        candidate_work.extend((plan.id, candidate) for candidate in candidates)
        plan_payloads.append({"plan_id": plan.external_plan_id, "candidate_count": len(candidates)})

    candidate_universe = _candidate_universe_payload([candidate for _, candidate in candidate_work])
    batch_size = int(getattr(settings, "exec_agent_watch_sizing_batch_size", WATCH_EXEC_AGENT_BATCH_SIZE))
    for batch in _chunks(candidate_work, batch_size):
        if batches_processed >= max_batches_per_cycle:
            break
        for request_batch in _sizing_request_batches([candidate for _, candidate in batch]):
            if batches_processed >= max_batches_per_cycle:
                break
            batches_processed += 1
            request_plan = plans_by_id[batch[0][0]]
            batch_ids = [str(candidate["execution_plan_item_id"]) for candidate in request_batch]
            request_payload = _sizing_request_payload(
                settings=settings,
                plan=request_plan,
                candidates=request_batch,
                account_context=account_context,
                candidate_universe=candidate_universe,
            )
            plan_expected_ids: dict[str, set[str]] = {}
            for plan_id, candidate in batch:
                candidate_id = str(candidate["execution_plan_item_id"])
                if candidate_id in batch_ids:
                    plan_expected_ids.setdefault(plan_id, set()).add(candidate_id)
            try:
                decision_payload = _request_exec_agent_decisions_with_retries(
                    settings=settings,
                    db=None,
                    plan=request_plan,
                    payload=request_payload,
                    expected_item_ids=set(batch_ids),
                )
                if decision_payload is None:
                    max_attempts = int(getattr(settings, "exec_agent_watch_sizing_max_attempts", 3))
                    with session_factory() as write_db:
                        with sqlite_write_lock(write_db):
                            for plan_id, expected_ids in plan_expected_ids.items():
                                write_plan = _load_plan_for_update(db=write_db, plan_id=plan_id)
                                _record_sizing_attempt(
                                    plan=write_plan,
                                    expected_item_ids=expected_ids,
                                    attempt=max_attempts,
                                    max_attempts=max_attempts,
                                    error="exec-agent did not return applicable decisions after max attempts",
                                )
                            write_db.commit()
                    for plan_id, expected_ids in plan_expected_ids.items():
                        errors.append(
                            {
                                "plan_id": plans_by_id[plan_id].external_plan_id,
                                "item_ids": sorted(expected_ids),
                                "error": "exec-agent did not return applicable decisions after max attempts",
                            }
                        )
                    continue
                with session_factory() as write_db:
                    with sqlite_write_lock(write_db):
                        for plan_id, expected_ids in plan_expected_ids.items():
                            write_plan = _load_plan_for_update(db=write_db, plan_id=plan_id)
                            filtered_payload = _decision_payload_for_expected_ids(
                                decision_payload,
                                expected_item_ids=expected_ids,
                            )
                            if not filtered_payload["decisions"]:
                                applied.append(
                                    {
                                        "plan_id": write_plan.external_plan_id,
                                        "evaluated": 0,
                                        "approved": 0,
                                        "rejected": 0,
                                        "missing": len(expected_ids),
                                        "missing_item_ids": sorted(expected_ids),
                                        "ignored_extra": 0,
                                    }
                                )
                                continue
                            summary = _apply_decisions(
                                db=write_db,
                                settings=settings,
                                plan=write_plan,
                                decision_payload=filtered_payload,
                                account_context=account_context,
                                expected_item_ids=expected_ids,
                            )
                            applied.append(summary)
                        write_db.commit()
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                LOGGER.exception(
                    "watch_exec_agent_sizing_failed",
                    extra={
                        "plan_id": "multi-plan-watch-sizing",
                        "runtime_mode": settings.runtime_mode,
                        "batch_item_ids": batch_ids,
                    },
                )
                errors.append({"plan_id": "multi-plan-watch-sizing", "item_ids": batch_ids, "error": str(exc)})

    return _result(
        idle=not applied and not errors,
        plans=plan_payloads,
        applied=applied,
        errors=errors,
        batches_processed=batches_processed,
        max_batches_per_cycle=max_batches_per_cycle,
    )


def _load_plan_for_update(*, db: Session, plan_id: str) -> ExecutionPlan:
    plan = db.scalars(
        select(ExecutionPlan)
        .where(ExecutionPlan.id == plan_id)
        .options(selectinload(ExecutionPlan.items))
    ).one()
    return plan


def _decision_payload_for_expected_ids(
    decision_payload: dict[str, object],
    *,
    expected_item_ids: set[str],
) -> dict[str, object]:
    filtered = dict(decision_payload)
    decisions_raw = decision_payload.get("decisions")
    if not isinstance(decisions_raw, list):
        filtered["decisions"] = []
        return filtered
    filtered["decisions"] = [
        decision
        for decision in decisions_raw
        if isinstance(decision, dict) and str(decision.get("execution_plan_item_id")) in expected_item_ids
    ]
    return filtered


def _inside_sizing_window(*, settings: Settings, now: datetime) -> bool:
    tz = ZoneInfo(settings.market_timezone)
    local_now = now.astimezone(tz).time()
    return _parse_hhmm(settings.exec_agent_watch_sizing_start_local_time) <= local_now <= _parse_hhmm(
        settings.exec_agent_watch_sizing_end_local_time
    )


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(hour=int(hour), minute=int(minute))


def _chunks(items: list[dict[str, object]], size: int) -> list[list[dict[str, object]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _sizing_request_batches(candidates: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    return [candidates]


def _sizing_request_payload(
    *,
    settings: Settings,
    plan: ExecutionPlan,
    candidates: list[dict[str, object]],
    account_context: dict[str, object],
    candidate_universe: dict[str, object],
    retry: bool = False,
) -> dict[str, object]:
    task = (
        "Return exec-agent sizing decisions for intraday watch buys. "
        "Keep the normal high-upside approval logic, but also approve small tactical candidates "
        "when about 2% upside is realistic, downside is tightly stopped near breakeven, "
        "and upside probability is better than downside probability. "
        "Quantity must be positive shares for approve, or reject with a reason. "
        "Set target_profile to tactical_2pct, short_swing, standard_breakout, or extended_momentum "
        "so later monitoring shows which target line is being traded. "
        "These are pending trigger-limit buys; use trigger_price/reference and chase limit. "
        "Return exactly one decision for every candidate execution_plan_item_id."
    )
    if retry:
        task += " This is a retry for candidates missing from a previous response; do not omit any candidate."
    candidates = _candidates_with_allowed_action_envelopes(
        settings=settings,
        candidates=candidates,
        account_context=account_context,
    )
    return {
        "version": "watch-exec-agent-sizing-request-v1",
        "plan_id": plan.external_plan_id,
        "market_date": plan.market_date.isoformat() if plan.market_date else None,
        "strategy_run_id": plan.strategy_run_id,
        "task": task,
        "trade_outcome_feedback": load_latest_trade_outcome_feedback_snippet(settings),
        "account_context": account_context,
        "candidate_universe": candidate_universe,
        "candidates": candidates,
    }


def _summary_missing_item_ids(summary: dict[str, object]) -> list[str]:
    value = summary.get("missing_item_ids")
    if not isinstance(value, list):
        return []
    return [str(item_id) for item_id in value]


def _applicable_decision_ids(decision_payload: dict[str, object], *, expected_item_ids: set[str]) -> set[str]:
    decisions_raw = decision_payload.get("decisions")
    if not isinstance(decisions_raw, list):
        return set()
    return {
        str(decision.get("execution_plan_item_id"))
        for decision in decisions_raw
        if isinstance(decision, dict) and str(decision.get("execution_plan_item_id")) in expected_item_ids
    }


def _load_candidate_plans(*, settings: Settings, db: Session, market_date) -> list[ExecutionPlan]:
    return db.scalars(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlan.market_date == market_date,
            ExecutionPlan.items.any(
                (ExecutionPlanItem.intent.in_(tuple(WATCH_BUY_INTENTS)))
                & (ExecutionPlanItem.status.in_(tuple(SIZED_WATCH_STATUSES)))
            ),
        )
        .options(selectinload(ExecutionPlan.items))
        .order_by(ExecutionPlan.accepted_at.asc(), ExecutionPlan.created_at.asc())
    ).all()


def _unsized_watch_items(plan: ExecutionPlan) -> list[ExecutionPlanItem]:
    items: list[ExecutionPlanItem] = []
    for item in plan.items:
        if item.intent not in WATCH_BUY_INTENTS or item.side != "buy":
            continue
        if item.status in TERMINAL_WATCH_STATUSES:
            continue
        if _has_exec_agent_decision(item):
            continue
        if _quantity_is_shares(item.quantity_policy):
            continue
        items.append(item)
    return sorted(items, key=lambda item: (item.symbol, item.plan_item_index, item.id))


def _candidate_symbols(plans: list[ExecutionPlan]) -> set[str]:
    return {
        item.symbol.strip().upper()
        for plan in plans
        for item in _unsized_watch_items(plan)
        if item.symbol and item.symbol.strip()
    }


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
    payload: dict[str, object] = {
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
    payload.update(_market_regime_guardrail_context(risk_context))
    decider_flow_context = _decider_flow_short_sector_context(risk_context)
    if decider_flow_context is not None:
        payload["decider_flow_short_sector_context"] = decider_flow_context
    return payload


def _market_regime_guardrail_context(risk_context: dict[str, object]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key in (
        "market",
        "exchange",
        "market_breadth",
        "market_context",
        "sector_context",
        "sector_peer_context",
        "sector_peers_context",
        "market_relative_context",
    ):
        if key in risk_context:
            payload[key] = risk_context[key]
    pre_decider, _ = _pre_decider_enrichment_context(risk_context)
    if isinstance(pre_decider, dict):
        compact_pre_decider = {
            key: pre_decider[key]
            for key in ("market_breadth", "sector_peer_context", "sector_peers_context", "market_context")
            if key in pre_decider
        }
        if compact_pre_decider:
            payload["pre_decider_enrichment_context"] = compact_pre_decider
    handoff = risk_context.get("decider_handoff_context")
    if isinstance(handoff, dict):
        compact_handoff = {
            key: handoff[key]
            for key in ("market_breadth", "sector_peer_context", "sector_peers_context", "market_context")
            if key in handoff
        }
        if compact_handoff:
            payload["decider_handoff_context"] = compact_handoff
    return payload


def _decider_flow_short_sector_context(risk_context: dict[str, object]) -> dict[str, object] | None:
    direct_flow_context = _flow_short_sector_context_from(risk_context)
    if direct_flow_context is not None:
        return {
            "source": "risk_context",
            "flow_short_sector_context": direct_flow_context,
        }

    handoff = risk_context.get("decider_handoff_context")
    if isinstance(handoff, dict):
        handoff_flow_context = _flow_short_sector_context_from(handoff)
        if handoff_flow_context is not None:
            return {
                "source": "risk_context.decider_handoff_context",
                "flow_short_sector_context": handoff_flow_context,
            }

    pre_decider_context, source_path = _pre_decider_enrichment_context(risk_context)
    if pre_decider_context is None:
        return None
    flow_short_sector_context = _flow_short_sector_context_from(pre_decider_context)
    if flow_short_sector_context is None:
        return None
    payload: dict[str, object] = {
        "source": source_path,
        "schema_version": pre_decider_context.get("schema_version"),
        "status": pre_decider_context.get("status"),
        "market_date": pre_decider_context.get("market_date"),
        "flow_short_sector_context": flow_short_sector_context,
    }
    if "context_provenance" in pre_decider_context:
        payload["context_provenance"] = pre_decider_context.get("context_provenance")
    return payload


def _flow_short_sector_context_from(payload: dict[str, object]) -> dict[str, object] | None:
    flow_short_sector_context = payload.get("flow_short_sector_context")
    if isinstance(flow_short_sector_context, dict):
        return flow_short_sector_context

    compact_context: dict[str, object] = {}
    for source_key, output_key in (
        ("investor_flow_context", "investor_flow"),
        ("short_selling_context", "short_selling"),
        ("sector_peer_context", "sector_peers"),
        ("sector_peers_context", "sector_peers"),
    ):
        value = payload.get(source_key)
        if isinstance(value, dict):
            compact_context[output_key] = value
    if compact_context:
        return compact_context
    return None


def _pre_decider_enrichment_context(risk_context: dict[str, object]) -> tuple[dict[str, object] | None, str | None]:
    direct = risk_context.get("pre_decider_enrichment_context")
    if isinstance(direct, dict):
        return direct, "risk_context.pre_decider_enrichment_context"

    handoff = risk_context.get("decider_handoff_context")
    if not isinstance(handoff, dict):
        return None, None
    decider_input = handoff.get("decider_input_context")
    if not isinstance(decider_input, dict):
        return None, None

    handoff_pre_decider = decider_input.get("pre_decider_enrichment_context")
    if isinstance(handoff_pre_decider, dict):
        return (
            handoff_pre_decider,
            "risk_context.decider_handoff_context.decider_input_context.pre_decider_enrichment_context",
        )

    request_payload = decider_input.get("request_payload")
    if not isinstance(request_payload, dict):
        return None, None
    symbol_day_context = request_payload.get("symbol_day_context")
    if not isinstance(symbol_day_context, dict):
        return None, None
    request_pre_decider = symbol_day_context.get("pre_decider_enrichment_context")
    if isinstance(request_pre_decider, dict):
        return (
            request_pre_decider,
            (
                "risk_context.decider_handoff_context.decider_input_context.request_payload."
                "symbol_day_context.pre_decider_enrichment_context"
            ),
        )
    return None, None


def _load_sizing_account_context(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None,
    symbols: set[str] | None = None,
) -> dict[str, object]:
    if adapter is None:
        return {"available": False, "reason": "broker_adapter_unavailable"}
    stale_reconciliation: dict[str, object] | None = None
    try:
        stale_reconciliation = _reconcile_stale_sizing_orders(
            settings=settings,
            db=db,
            adapter=adapter,
            symbols=symbols,
        )
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="watch_exec_agent_sizing",
            reason="load account cash, holdings, and open orders for exec-agent watch sizing",
        )
        snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError) as exc:
        return _unavailable_account_context(
            reason=f"{exc.__class__.__name__}: {exc}",
            stale_reconciliation=stale_reconciliation,
        )
    return _account_context_payload(snapshot, stale_reconciliation=stale_reconciliation)


def _load_sizing_account_context_short_transaction(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
    adapter: BrokerAdapter | None,
    symbols: set[str] | None = None,
) -> dict[str, object]:
    if adapter is None:
        return {"available": False, "reason": "broker_adapter_unavailable"}
    stale_reconciliation: dict[str, object] | None = None
    try:
        with session_factory() as db:
            stale_reconciliation = _reconcile_stale_sizing_orders(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=symbols,
            )
            session_state = ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
                source="watch_exec_agent_sizing",
                reason="load account cash, holdings, and open orders for exec-agent watch sizing",
            )
            access_token = session_state.access_token or ""
            db.commit()
        snapshot = adapter.get_account_snapshot(access_token)
    except (BrokerAdapterError, AttributeError, NotImplementedError, RuntimeError, ValueError) as exc:
        return _unavailable_account_context(
            reason=f"{exc.__class__.__name__}: {exc}",
            stale_reconciliation=stale_reconciliation,
        )
    return _account_context_payload(snapshot, stale_reconciliation=stale_reconciliation)


def _reconcile_stale_sizing_orders(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    symbols: set[str] | None,
) -> dict[str, object]:
    return reconcile_stale_nonterminal_broker_orders(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols=symbols if symbols else None,
    )


def _account_context_payload(
    snapshot: BrokerAccountSnapshot,
    stale_reconciliation: dict[str, object] | None = None,
) -> dict[str, object]:
    positions = snapshot.positions or snapshot.holdings
    holdings = [
        {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "sellable_quantity": getattr(position, "sellable_quantity", None),
            "average_price": _json_decimal(getattr(position, "average_price", None)),
        }
        for position in positions
        if position.quantity > 0
    ]
    open_orders = [
        {
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "remaining_quantity": order.remaining_quantity,
            "status": order.status,
            "limit_price": _json_decimal(order.limit_price),
            "source_candidate_id": order.source_candidate_id,
        }
        for order in snapshot.open_orders
    ]
    payload: dict[str, object] = {
        "available": True,
        "source": snapshot.source,
        "observed_at": snapshot.observed_at.isoformat(),
        "orderable_cash_krw": _json_decimal(snapshot.available_cash),
        "buying_power_krw": _json_decimal(snapshot.buying_power),
        "holding_count": len(holdings),
        "open_order_count": len(open_orders),
        "holdings": holdings,
        "open_orders": open_orders,
    }
    if stale_reconciliation is not None:
        blockers = _stale_state_blockers_from_reconciliation(stale_reconciliation)
        payload["stale_order_reconciliation"] = _json_safe(stale_reconciliation)
        payload["stale_state_blocker_count"] = len(blockers)
        payload["stale_state_blocked_symbols"] = sorted({str(row["symbol"]) for row in blockers})
        payload["stale_state_blockers"] = blockers
    return payload


def _unavailable_account_context(
    *,
    reason: str,
    stale_reconciliation: dict[str, object] | None,
) -> dict[str, object]:
    payload: dict[str, object] = {"available": False, "reason": reason}
    if stale_reconciliation is not None:
        blockers = _stale_state_blockers_from_reconciliation(stale_reconciliation)
        payload["stale_order_reconciliation"] = _json_safe(stale_reconciliation)
        payload["stale_state_blocker_count"] = len(blockers)
        payload["stale_state_blocked_symbols"] = sorted({str(row["symbol"]) for row in blockers})
        payload["stale_state_blockers"] = blockers
    return payload


def _stale_state_blockers_from_reconciliation(reconciliation: dict[str, object]) -> list[dict[str, object]]:
    items = reconciliation.get("items")
    if not isinstance(items, list):
        return []
    blockers: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        classification = str(item.get("classification") or "").strip().lower()
        if classification not in {"unknown", "failed-to-reconcile"}:
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        result = item.get("result") if isinstance(item.get("result"), dict) else {}
        blockers.append(
            {
                "symbol": symbol,
                "reason": "stale_order_broker_truth_unconfirmed",
                "classification": classification,
                "broker_order_pk": item.get("broker_order_pk"),
                "broker_order_id": item.get("broker_order_id"),
                "idempotency_key": item.get("idempotency_key"),
                "side": item.get("side"),
                "status": item.get("status"),
                "message": item.get("reason") or result.get("message"),
                "last_broker_sync_at": item.get("last_broker_sync_at"),
            }
        )
    return blockers


def _stale_state_blockers_by_symbol(account_context: dict[str, object] | None) -> dict[str, list[dict[str, object]]]:
    if not isinstance(account_context, dict):
        return {}
    blockers = account_context.get("stale_state_blockers")
    if not isinstance(blockers, list):
        return {}
    by_symbol: dict[str, list[dict[str, object]]] = {}
    for blocker in blockers:
        if not isinstance(blocker, dict):
            continue
        symbol = str(blocker.get("symbol") or "").strip().upper()
        if symbol:
            by_symbol.setdefault(symbol, []).append(blocker)
    return by_symbol


def _reject_stale_state_blocked_items(
    *,
    db: Session | None,
    plan: ExecutionPlan,
    account_context: dict[str, object] | None,
) -> dict[str, object]:
    blockers_by_symbol = _stale_state_blockers_by_symbol(account_context)
    rejected_ids: list[str] = []
    for item in _unsized_watch_items(plan):
        blockers = blockers_by_symbol.get(item.symbol.strip().upper())
        if not blockers:
            continue
        _reject_stale_state_blocked_item(db=db, item=item, blockers=blockers)
        rejected_ids.append(item.id)
    return {
        "plan_id": plan.external_plan_id,
        "evaluated": len(rejected_ids),
        "approved": 0,
        "rejected": len(rejected_ids),
        "missing": 0,
        "missing_item_ids": [],
        "ignored_extra": 0,
        "stale_state_blocked_item_ids": rejected_ids,
    }


def _reject_stale_state_blocked_items_short_transaction(
    *,
    session_factory: sessionmaker[Session],
    plans: list[ExecutionPlan],
    account_context: dict[str, object] | None,
) -> tuple[set[str], list[dict[str, object]]]:
    if not _stale_state_blockers_by_symbol(account_context):
        return set(), []
    blocked_ids: set[str] = set()
    summaries: list[dict[str, object]] = []
    with session_factory() as db:
        with sqlite_write_lock(db):
            for plan in plans:
                write_plan = _load_plan_for_update(db=db, plan_id=plan.id)
                summary = _reject_stale_state_blocked_items(
                    db=db,
                    plan=write_plan,
                    account_context=account_context,
                )
                ids = {str(item_id) for item_id in summary["stale_state_blocked_item_ids"]}
                blocked_ids.update(ids)
                if summary["rejected"]:
                    summaries.append(summary)
            db.commit()
    return blocked_ids, summaries


def _reject_stale_state_blocked_item(
    *,
    db: Session | None,
    item: ExecutionPlanItem,
    blockers: list[dict[str, object]],
) -> None:
    summary = {
        "source": "watch_exec_agent_sizing",
        "action": "reject",
        "decision": "rejected",
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "quantity": 0,
        "approved_quantity": 0,
        "approved_notional_krw": "0.0000",
        "sizing_price": _json_decimal(item.limit_price or item.trigger_price),
        "confidence": None,
        "target_profile": None,
        "target_tag": None,
        "reason_code": "stale_order_broker_truth_unconfirmed",
        "reason": "stale local open order could not be confirmed against broker truth",
        "decided_at": datetime.now(UTC).isoformat(),
    }
    previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
    item.quantity_policy = {
        "kind": "exec_agent_rejected",
        "source": "watch_exec_agent_sizing",
        "exec_agent_decision": summary,
        "short_term_exec_agent_decision": summary,
        "exec_agent_decision_audit": {
            "event_type": "watch_exec_agent_sizing_stale_state_blocked",
            "storage": "execution_plan_item_audits",
        },
        "stale_state_blockers": _json_safe(blockers),
        "previous_policy": previous_policy,
    }
    item.status = "exec_agent_rejected"
    _add_plan_item_audit(
        db=db,
        item=item,
        event_type="watch_exec_agent_sizing_stale_state_blocked",
        summary=summary,
        payload={
            "summary": summary,
            "stale_state_blockers": _json_safe(blockers),
        },
    )


def _candidate_universe_payload(candidates: list[dict[str, object]]) -> dict[str, object]:
    compact_candidates = []
    for candidate in candidates:
        compact_candidates.append(
            {
                "execution_plan_item_id": candidate.get("execution_plan_item_id"),
                "symbol": candidate.get("symbol"),
                "trigger_price": candidate.get("trigger_price"),
                "chase_limit_price": candidate.get("chase_limit_price"),
                "max_safe_notional_krw": candidate.get("max_safe_notional_krw"),
                "confidence_score_10": candidate.get("confidence_score_10"),
                "conviction_score_10": candidate.get("conviction_score_10"),
                "expected_return_pct": candidate.get("expected_return_pct"),
                "liquidity_tier": candidate.get("liquidity_tier"),
            }
        )
    return {
        "candidate_count": len(candidates),
        "candidates": compact_candidates,
    }


def _request_exec_agent_decisions_with_retries(
    *,
    settings: Settings,
    db: Session | None,
    plan: ExecutionPlan,
    payload: dict[str, object],
    expected_item_ids: set[str],
) -> dict[str, object] | None:
    max_attempts = int(getattr(settings, "exec_agent_watch_sizing_max_attempts", 3))
    last_error: str | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            decision_payload = _request_exec_agent_decisions(settings=settings, payload=payload)
        except Exception as exc:
            last_error = str(exc)
            _record_sizing_attempt(
                plan=plan,
                expected_item_ids=expected_item_ids,
                attempt=attempt,
                max_attempts=max_attempts,
                error=last_error,
            )
            continue
        if _applicable_decision_ids(decision_payload, expected_item_ids=expected_item_ids):
            _record_sizing_attempt(
                plan=plan,
                expected_item_ids=expected_item_ids,
                attempt=attempt,
                max_attempts=max_attempts,
                success=True,
            )
            return decision_payload
        last_error = "exec-agent response contained no applicable decisions"
        _record_sizing_attempt(
            plan=plan,
            expected_item_ids=expected_item_ids,
            attempt=attempt,
            max_attempts=max_attempts,
            error=last_error,
        )
    if db is not None:
        db.flush()
    return None


def _record_sizing_attempt(
    *,
    plan: ExecutionPlan,
    expected_item_ids: set[str],
    attempt: int,
    max_attempts: int,
    success: bool = False,
    error: str | None = None,
) -> None:
    recorded_at = datetime.now(UTC).isoformat()
    for item in plan.items:
        if item.id not in expected_item_ids:
            continue
        quantity_policy = dict(item.quantity_policy or {})
        events = quantity_policy.get("exec_agent_watch_sizing_attempts")
        if not isinstance(events, list):
            events = []
        event: dict[str, object] = {
            "attempt": attempt,
            "max_attempts": max_attempts,
            "success": success,
            "recorded_at": recorded_at,
        }
        if error is not None:
            event["error"] = error[-500:]
        quantity_policy["exec_agent_watch_sizing_attempt_count"] = attempt
        quantity_policy["exec_agent_watch_sizing_max_attempts"] = max_attempts
        quantity_policy["exec_agent_watch_sizing_fallback_allowed"] = bool(not success and attempt >= max_attempts)
        quantity_policy["exec_agent_watch_sizing_attempts"] = [*events[-9:], event]
        item.quantity_policy = quantity_policy


def _request_exec_agent_decisions(*, settings: Settings, payload: dict[str, object]) -> dict[str, object]:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2)
        request_path = Path(handle.name)
    prompt = f"""
긴급: {request_path} 파일의 intraday_watch_buy 후보를 지금 수량 판단하세요.

요구사항:
- 각 candidate별 approve 또는 reject 판단.
- 파일에는 정제된 candidate, account_context, candidate_universe만 있습니다. 다른 세션/긴 컨텍스트를 보지 말고 이 JSON만 기준으로 판단하세요.
- account_context.orderable_cash_krw / buying_power_krw, 보유/미체결 주문, 같은 plan의 나머지 후보(candidate_universe)를 같이 보고 현금 배분을 보수적으로 정하세요.
- trade_outcome_feedback이 있으면 최근 손실 패턴은 축소/거절하고 positive expectancy 패턴만 증액 근거로 사용하세요.
- 기본 로직은 유지하되, 2% 정도만 현실적인 candidate도 stop이 짧고 상승 확률 우위가 있으면 소액 approve 하세요.
- approve 시 positive integer quantity, approved_notional_krw, sizing_price, confidence, target_profile, reason_code, reason 포함.
- reject 시 quantity=0, approved_notional_krw=0, sizing_price, confidence, reason_code, reason 포함.
- max_safe_notional_krw / max_safe_quantity / liquidity_tier / target/stop / confidence/conviction / risks를 반영하세요.
- budget_krw가 0이어도 exec-agent sizing delegated 상태이므로 max_safe_notional과 위험도로 보수적 수량을 정하세요.
- target_profile은 tactical_2pct, short_swing, standard_breakout, extended_momentum 중 하나입니다.
- 결과는 반드시 JSON만 출력하세요. 스키마:
{{"version":"manual-exec-agent-watch-sizing-v1","plan_id":"{payload['plan_id']}","decisions":[{{"execution_plan_item_id":"...","symbol":"...","action":"approve|reject","quantity":1,"approved_notional_krw":12345,"sizing_price":1234,"confidence":0.0,"target_profile":"tactical_2pct","reason_code":"...","reason":"..."}}]}}
""".strip()
    command = [
        settings.openclaw_cli_path,
        "agent",
        "--agent",
        settings.exec_agent_id,
        "--session-key",
        f"watch-sizing-{uuid.uuid4()}",
        "--thinking",
        "low",
        "--message",
        prompt,
        "--json",
        "--timeout",
        str(settings.exec_agent_watch_sizing_timeout_seconds),
    ]
    completed = _run_exec_agent_command(
        command,
        timeout_seconds=settings.exec_agent_watch_sizing_timeout_seconds + 30,
    )
    response = json.loads(completed.stdout)
    text = _agent_response_text(response)
    return _parse_json_text(text)


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


def _apply_decisions(
    *,
    db: Session,
    settings: Settings | None = None,
    plan: ExecutionPlan,
    decision_payload: dict[str, object],
    account_context: dict[str, object] | None = None,
    expected_item_ids: set[str] | None = None,
) -> dict[str, object]:
    decisions_raw = decision_payload.get("decisions")
    if not isinstance(decisions_raw, list):
        raise ValueError("exec-agent response missing decisions list")
    decisions = {str(decision.get("execution_plan_item_id")): decision for decision in decisions_raw if isinstance(decision, dict)}
    expected_ids = expected_item_ids or {item.id for item in _unsized_watch_items(plan)}
    extra_ids = set(decisions).difference(expected_ids)
    missing_ids = expected_ids.difference(decisions)
    if extra_ids:
        LOGGER.warning(
            "watch_exec_agent_sizing_ignored_extra_decisions",
            extra={"plan_id": plan.external_plan_id, "extra_item_ids": sorted(extra_ids)},
        )
    if missing_ids:
        LOGGER.warning(
            "watch_exec_agent_sizing_partial_response",
            extra={"plan_id": plan.external_plan_id, "missing_item_ids": sorted(missing_ids)},
        )
    decisions = {
        item_id: decision
        for item_id, decision in decisions.items()
        if item_id in expected_ids
    }
    if not decisions:
        raise ValueError(
            "exec-agent response contained no applicable decisions: "
            f"missing={sorted(missing_ids)} extra={sorted(extra_ids)}"
        )
    items = [item for item in _unsized_watch_items(plan) if item.id in decisions]
    approved = 0
    rejected = 0
    for item in items:
        decision = decisions[item.id]
        action = str(decision.get("action") or "").strip().lower()
        record = _decision_record(decision=decision, item=item, version=str(decision_payload.get("version") or ""))
        _add_plan_item_audit(
            db=db,
            item=item,
            event_type="watch_exec_agent_sizing_decision",
            summary=_decision_summary(record),
            payload=record,
        )
        if action == "approve":
            was_triggered_blocked = item.status == "watch_buy_triggered_blocked"
            quantity = _positive_int(decision.get("quantity"))
            if quantity is None:
                raise ValueError(f"approved decision has invalid quantity for {item.symbol}")
            envelope = _allowed_action_envelope_for_item(
                settings=settings,
                item=item,
                account_context=account_context,
                decision=decision,
            )
            sizing = _adjust_approved_watch_quantity(
                settings=settings,
                item=item,
                decision=decision,
                account_context=account_context,
                agent_quantity=quantity,
                allowed_action_envelope=envelope,
            )
            quantity = sizing["quantity"]
            if sizing["decision"] == "skip":
                decision["action"] = "reject"
                decision["reason_code"] = sizing["reason_code"]
                decision["reason"] = sizing["reason"]
                record = _decision_record(decision=decision, item=item, version=str(decision_payload.get("version") or ""))
                previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
                item.quantity_policy = {
                    "kind": "exec_agent_rejected",
                    "source": "exec_agent_watch_sizing",
                    "reason_code": sizing["reason_code"],
                    "reason": sizing["reason"],
                    "exec_agent_decision": _decision_summary(record),
                    "short_term_exec_agent_decision": _decision_summary(record),
                    "exec_agent_decision_audit": {
                        "event_type": "watch_exec_agent_sizing_decision",
                        "storage": "execution_plan_item_audits",
                    },
                    "sizing_adjustment": sizing["adjustment"],
                    "allowed_action_envelope": envelope,
                    "market_regime_guardrail": envelope.get("market_regime_guardrail"),
                    "previous_policy": previous_policy,
                }
                _add_plan_item_audit(
                    db=db,
                    item=item,
                    event_type="watch_exec_agent_sizing_cap_decision",
                    summary={
                        "decision": "skip",
                        "reason_code": sizing["reason_code"],
                        "agent_approved_quantity": sizing["adjustment"].get("agent_approved_quantity"),
                        "agent_approved_notional": sizing["adjustment"].get("agent_approved_notional"),
                        "minimum_required_notional": sizing["adjustment"].get("minimum_required_notional"),
                        "cap_notional": sizing["adjustment"].get("cap_notional"),
                        "final_quantity": sizing["adjustment"].get("final_quantity"),
                        "final_notional": sizing["adjustment"].get("final_notional"),
                    },
                    payload=sizing["adjustment"],
                )
                item.status = "exec_agent_rejected"
                rejected += 1
                continue
            if quantity <= 0:
                decision["action"] = "reject"
                decision["reason_code"] = "allowed_action_envelope_blocked"
                decision["reason"] = "allowed action envelope left no buyable quantity"
                record = _decision_record(decision=decision, item=item, version=str(decision_payload.get("version") or ""))
                previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
                item.quantity_policy = {
                    "kind": "exec_agent_rejected",
                    "source": "exec_agent_watch_sizing",
                    "exec_agent_decision": _decision_summary(record),
                    "short_term_exec_agent_decision": _decision_summary(record),
                    "exec_agent_decision_audit": {
                        "event_type": "watch_exec_agent_sizing_decision",
                        "storage": "execution_plan_item_audits",
                    },
                    "allowed_action_envelope": envelope,
                    "market_regime_guardrail": envelope.get("market_regime_guardrail"),
                    "previous_policy": previous_policy,
                }
                item.status = "exec_agent_rejected"
                rejected += 1
                continue
            previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
            item.quantity_policy = {
                "kind": "shares",
                "quantity": quantity,
                "source": "exec_agent_watch_sizing",
                "sizing_price": _decimal_text(sizing["sizing_price"]),
                "approved_notional": _decimal_text(sizing["approved_notional"]),
                "confidence": decision.get("confidence"),
                "target_profile": record["target_profile"],
                "target_tag": record["target_tag"],
                "reason_code": decision.get("reason_code"),
                "exec_agent_decision": _decision_summary(record),
                "short_term_exec_agent_decision": _decision_summary(record),
                "exec_agent_decision_audit": {
                    "event_type": "watch_exec_agent_sizing_decision",
                    "storage": "execution_plan_item_audits",
                },
                "sizing_adjustment": sizing["adjustment"],
                "allowed_action_envelope": envelope,
                "market_regime_guardrail": envelope.get("market_regime_guardrail"),
                "previous_policy": previous_policy,
            }
            item.status = "watch_buy_triggered_blocked" if was_triggered_blocked else "watch_monitoring_active"
            approved += 1
        elif action == "reject":
            previous_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else None
            item.quantity_policy = {
                "kind": "exec_agent_rejected",
                "source": "exec_agent_watch_sizing",
                "exec_agent_decision": _decision_summary(record),
                "short_term_exec_agent_decision": _decision_summary(record),
                "exec_agent_decision_audit": {
                    "event_type": "watch_exec_agent_sizing_decision",
                    "storage": "execution_plan_item_audits",
                },
                "previous_policy": previous_policy,
            }
            item.status = "exec_agent_rejected"
            rejected += 1
        else:
            raise ValueError(f"unknown exec-agent decision action for {item.symbol}: {action}")
    return {
        "plan_id": plan.external_plan_id,
        "evaluated": len(items),
        "approved": approved,
        "rejected": rejected,
        "missing": len(missing_ids),
        "missing_item_ids": sorted(missing_ids),
        "ignored_extra": len(extra_ids),
    }


def _decision_summary(record: dict[str, object]) -> dict[str, object]:
    return {
        "source": record.get("source"),
        "action": record.get("action"),
        "decision": record.get("decision"),
        "execution_plan_item_id": record.get("execution_plan_item_id"),
        "symbol": record.get("symbol"),
        "quantity": record.get("quantity"),
        "approved_quantity": record.get("approved_quantity"),
        "approved_notional_krw": record.get("approved_notional_krw"),
        "sizing_price": record.get("sizing_price"),
        "confidence": record.get("confidence"),
        "target_profile": record.get("target_profile"),
        "target_tag": record.get("target_tag"),
        "reason_code": record.get("reason_code"),
        "reason": record.get("reason"),
        "decided_at": record.get("decided_at"),
    }


def _add_plan_item_audit(
    *,
    db: Session | None,
    item: ExecutionPlanItem,
    event_type: str,
    summary: dict[str, object],
    payload: dict[str, object],
    error_message: str | None = None,
) -> None:
    if db is None or not hasattr(db, "add"):
        return
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=event_type,
            source="watch_exec_agent_sizing",
            summary=summary,
            payload=payload,
            error_message=error_message,
            occurred_at=datetime.now(UTC),
        )
    )


def _adjust_approved_watch_quantity(
    *,
    settings: Settings | None,
    item: ExecutionPlanItem,
    decision: dict[str, object],
    account_context: dict[str, object] | None,
    agent_quantity: int,
    allowed_action_envelope: dict[str, object] | None = None,
) -> dict[str, object]:
    sizing_price = (
        _decimal_value(decision.get("sizing_price"))
        or item.limit_price
        or item.trigger_price
    )
    if sizing_price is None or sizing_price <= 0:
        return {
            "quantity": agent_quantity,
            "sizing_price": Decimal("0"),
            "approved_notional": Decimal("0"),
            "decision": "submit",
            "reason_code": None,
            "reason": None,
            "adjustment": {"applied": False, "reason": "missing_sizing_price"},
        }
    if settings is None:
        approved_notional = sizing_price * Decimal(agent_quantity)
        return {
            "quantity": agent_quantity,
            "sizing_price": sizing_price,
            "approved_notional": approved_notional,
            "decision": "submit",
            "reason_code": None,
            "reason": None,
            "adjustment": {
                "applied": False,
                "reason": "settings_unavailable",
                "agent_quantity": agent_quantity,
                "agent_approved_quantity": agent_quantity,
                "agent_notional": _decimal_text(approved_notional),
                "agent_approved_notional": _decimal_text(approved_notional),
                "minimum_required_notional": None,
                "cap_notional": _decimal_text(approved_notional),
                "final_quantity": agent_quantity,
                "final_notional": _decimal_text(approved_notional),
                "decision": "submit",
            },
        }

    original_notional = sizing_price * Decimal(agent_quantity)
    agent_reported_notional = _decimal_value(decision.get("approved_notional_krw"))
    agent_cap_notional = original_notional
    if agent_reported_notional is not None and agent_reported_notional > 0:
        agent_cap_notional = min(agent_cap_notional, agent_reported_notional)
    target_notional = _target_min_notional(settings=settings, item=item, decision=decision)
    runtime_cap_notional = _watch_sizing_cap_notional(
        settings=settings,
        item=item,
        account_context=account_context,
        sizing_price=sizing_price,
    )
    cap_notional = min(runtime_cap_notional, agent_cap_notional).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    effective_target = min(target_notional, cap_notional)
    adjusted_quantity = agent_quantity
    max_quantity = _floor_quantity(cap_notional / sizing_price)
    envelope_max_quantity = _positive_int(
        allowed_action_envelope.get("max_quantity") if isinstance(allowed_action_envelope, dict) else None
    )
    if envelope_max_quantity is not None:
        max_quantity = min(max_quantity, envelope_max_quantity)
    min_notional_upsize_suppressed = target_notional > original_notional
    adjusted_quantity = min(adjusted_quantity, max_quantity)
    vol_corr_cap = apply_vol_corr_quantity_cap(
        settings=settings,
        baseline_quantity=adjusted_quantity,
        context=_vol_corr_context(item=item, account_context=account_context),
    )
    if vol_corr_cap is not None:
        adjusted_quantity = int(vol_corr_cap["capped_quantity"])
    approved_notional = sizing_price * Decimal(adjusted_quantity)
    decision_name = "submit"
    reason_code = None
    reason = None
    adjustment: dict[str, object] = {
        "applied": adjusted_quantity != agent_quantity,
        "agent_quantity": agent_quantity,
        "agent_approved_quantity": agent_quantity,
        "agent_notional": _decimal_text(original_notional),
        "agent_approved_notional": _decimal_text(agent_cap_notional),
        "target_min_notional": _decimal_text(target_notional),
        "minimum_required_notional": _decimal_text(target_notional),
        "cap_notional": _decimal_text(cap_notional),
        "runtime_cap_notional": _decimal_text(runtime_cap_notional),
        "agent_cap_notional": _decimal_text(agent_cap_notional),
        "allowed_action_envelope_max_quantity": envelope_max_quantity,
        "effective_target_notional": _decimal_text(effective_target),
        "min_notional_upsize_suppressed": min_notional_upsize_suppressed,
        "adjusted_quantity": adjusted_quantity,
        "adjusted_notional": _decimal_text(approved_notional),
        "final_quantity": adjusted_quantity,
        "final_notional": _decimal_text(approved_notional),
        "decision": decision_name,
        "reason_code": reason_code,
        "reason": reason,
    }
    if vol_corr_cap is not None:
        adjustment["vol_corr_cap"] = vol_corr_cap
    return {
        "quantity": adjusted_quantity,
        "sizing_price": sizing_price,
        "approved_notional": approved_notional,
        "decision": decision_name,
        "reason_code": reason_code,
        "reason": reason,
        "adjustment": adjustment,
    }


def _candidates_with_allowed_action_envelopes(
    *,
    settings: Settings,
    candidates: list[dict[str, object]],
    account_context: dict[str, object],
) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for candidate in candidates:
        payload = dict(candidate)
        payload["allowed_action_envelope"] = _allowed_action_envelope_for_candidate(
            settings=settings,
            candidate=payload,
            account_context=account_context,
        )
        enriched.append(payload)
    return enriched


def _allowed_action_envelope_for_item(
    *,
    settings: Settings | None,
    item: ExecutionPlanItem,
    account_context: dict[str, object] | None,
    decision: dict[str, object] | None = None,
) -> dict[str, object]:
    if settings is None:
        return {
            "schema_version": "allowed_action_envelope_v1",
            "allowed_actions": ["hold", "reject"],
            "max_quantity": 0,
            "max_notional_krw": "0.0000",
            "hard_reject_reasons": ["settings_unavailable"],
            "soft_warnings": [],
        }
    candidate = _candidate_payload(item)
    if decision is not None and decision.get("sizing_price") is not None:
        candidate["chase_limit_price"] = decision.get("sizing_price")
    return _allowed_action_envelope_for_candidate(
        settings=settings,
        candidate=candidate,
        account_context=account_context,
    )


def _allowed_action_envelope_for_candidate(
    *,
    settings: Settings,
    candidate: dict[str, object],
    account_context: dict[str, object] | None,
) -> dict[str, object]:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    sizing_price = (
        _decimal_value(candidate.get("chase_limit_price"))
        or _decimal_value(candidate.get("trigger_price"))
    )
    hard_reject_reasons: list[str] = []
    soft_warnings: list[str] = []
    if sizing_price is None or sizing_price <= 0:
        hard_reject_reasons.append("missing_reference_price")
        sizing_price = Decimal("0")

    available_cash = _account_available_cash(account_context)
    if available_cash is None:
        soft_warnings.append("account_cash_unavailable")
        available_cash = Decimal(str(settings.exec_agent_watch_sizing_fallback_account_basis_krw))

    holding_quantity = _account_holding_quantity(account_context=account_context, symbol=symbol)
    open_buy_order_count = _account_open_buy_order_count(account_context=account_context, symbol=symbol)
    if holding_quantity > 0:
        hard_reject_reasons.append("existing_holding")
    if open_buy_order_count > 0:
        hard_reject_reasons.append("duplicate_open_buy_order")

    account_basis = _account_basis(settings=settings, account_context=account_context)
    cap_candidates: list[Decimal] = [
        available_cash,
        Decimal(str(settings.exec_agent_max_symbol_budget)),
        account_basis * Decimal(str(settings.exec_agent_max_position_allocation_pct)),
    ]
    for key in ("budget_krw", "max_safe_notional_krw"):
        value = _decimal_value(candidate.get(key))
        if value is not None and value > 0:
            cap_candidates.append(value)

    stop_price = _decimal_value(candidate.get("stop_loss_price"))
    stop_distance_pct: str | None = None
    risk_cap: Decimal | None = None
    if stop_price is not None and sizing_price > 0 and stop_price < sizing_price:
        stop_distance = sizing_price - stop_price
        stop_distance_pct = _decimal_text((stop_distance / sizing_price) * Decimal("100"))
        risk_budget = account_basis * Decimal(str(settings.exec_agent_max_trade_risk_pct))
        risk_quantity = _floor_quantity(risk_budget / stop_distance)
        risk_cap = sizing_price * Decimal(max(0, risk_quantity))
        cap_candidates.append(risk_cap)
    elif stop_price is None:
        soft_warnings.append("missing_stop_price")
    else:
        hard_reject_reasons.append("invalid_stop_distance")

    max_notional = min(cap_candidates).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    market_guardrail = evaluate_market_regime_guardrail(
        settings=settings,
        context=candidate,
        market_date=candidate.get("market_date"),
        original_notional=max_notional,
        reference_price=sizing_price if sizing_price > 0 else None,
        original_quantity=_floor_quantity(max_notional / sizing_price) if sizing_price > 0 else None,
        account_context=account_context,
    )
    if market_guardrail.rejected:
        hard_reject_reasons.append(market_guardrail.reason_code)
        max_notional = Decimal("0")
    elif market_guardrail.action == "downsize":
        adjusted = _decimal_value(market_guardrail.audit.get("adjusted_notional_krw"))
        if adjusted is not None:
            max_notional = min(max_notional, adjusted).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        soft_warnings.append(market_guardrail.reason_code)
    max_quantity = _floor_quantity(max_notional / sizing_price) if sizing_price > 0 else 0
    if max_quantity <= 0:
        hard_reject_reasons.append("no_buyable_quantity")
    allowed_actions = ["hold", "reject"]
    if not hard_reject_reasons and max_quantity > 0:
        allowed_actions.extend(["buy", "downsize"])

    return {
        "schema_version": "allowed_action_envelope_v1",
        "allowed_actions": allowed_actions,
        "max_quantity": max(0, max_quantity),
        "max_notional_krw": _decimal_text(max_notional),
        "cash_limited_quantity": _floor_quantity(available_cash / sizing_price) if sizing_price > 0 else 0,
        "risk_limited_notional_krw": _decimal_text(risk_cap),
        "reference_price": _decimal_text(sizing_price),
        "stop_distance_pct": stop_distance_pct,
        "existing_position_quantity": holding_quantity,
        "open_buy_order_count": open_buy_order_count,
        "hard_reject_reasons": hard_reject_reasons,
        "soft_warnings": soft_warnings,
        "market_regime_guardrail": market_guardrail.audit,
        "computed_at": datetime.now(UTC).isoformat(),
    }


def _account_holding_quantity(*, account_context: dict[str, object] | None, symbol: str) -> int:
    if not isinstance(account_context, dict) or account_context.get("available") is not True:
        return 0
    holdings = account_context.get("holdings")
    if not isinstance(holdings, list):
        return 0
    total = 0
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        if str(holding.get("symbol") or "").strip().upper() != symbol:
            continue
        quantity = _positive_int(holding.get("quantity"))
        if quantity is not None:
            total += quantity
    return total


def _account_open_buy_order_count(*, account_context: dict[str, object] | None, symbol: str) -> int:
    if not isinstance(account_context, dict) or account_context.get("available") is not True:
        return 0
    open_orders = account_context.get("open_orders")
    if not isinstance(open_orders, list):
        return 0
    count = 0
    for order in open_orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").strip().upper() != symbol:
            continue
        if str(order.get("side") or "").strip().lower() == "buy":
            count += 1
    return count


def _target_min_notional(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    decision: dict[str, object],
) -> Decimal:
    base = Decimal(str(settings.exec_agent_watch_sizing_min_notional_krw))
    high_conviction = Decimal(str(settings.exec_agent_watch_sizing_high_conviction_min_notional_krw))
    confidence = _decimal_value(decision.get("confidence"))
    threshold = Decimal(str(settings.exec_agent_watch_sizing_high_conviction_threshold))
    liquidity_tier = _liquidity_tier(item)
    if confidence is not None and confidence >= threshold:
        return high_conviction
    if liquidity_tier == "high" and confidence is not None and confidence >= Decimal("0.60"):
        return high_conviction
    return base


def _watch_sizing_cap_notional(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    account_context: dict[str, object] | None,
    sizing_price: Decimal,
) -> Decimal:
    account_basis = _account_basis(settings=settings, account_context=account_context)
    cap_candidates = [
        Decimal(str(settings.exec_agent_max_symbol_budget)),
        account_basis * Decimal(str(settings.exec_agent_max_position_allocation_pct)),
    ]
    available_cash = _account_available_cash(account_context)
    if available_cash is not None and available_cash > 0:
        cap_candidates.append(available_cash)
    candidate_budget = _candidate_budget(item)
    if candidate_budget is not None and candidate_budget > 0:
        cap_candidates.append(candidate_budget)
    max_safe_notional = _candidate_max_safe_notional(item)
    if max_safe_notional is not None and max_safe_notional > 0:
        cap_candidates.append(max_safe_notional)
    risk_cap = _risk_cap_notional(settings=settings, item=item, account_basis=account_basis, sizing_price=sizing_price)
    if risk_cap is not None and risk_cap > 0:
        cap_candidates.append(risk_cap)
    return min(cap_candidates).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _risk_cap_notional(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
    account_basis: Decimal,
    sizing_price: Decimal,
) -> Decimal | None:
    stop_price = _stop_price(item)
    if stop_price is None or stop_price <= 0 or stop_price >= sizing_price:
        return None
    risk_budget = account_basis * Decimal(str(settings.exec_agent_max_trade_risk_pct))
    stop_distance = sizing_price - stop_price
    risk_quantity = _floor_quantity(risk_budget / stop_distance)
    if risk_quantity <= 0:
        return Decimal("0")
    return sizing_price * Decimal(risk_quantity)


def _vol_corr_context(
    *,
    item: ExecutionPlanItem,
    account_context: dict[str, object] | None,
) -> dict[str, object]:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    context: dict[str, object] = {}
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
        if key in risk_context:
            context[key] = risk_context[key]

    holding_symbols = _account_holding_symbols(account_context)
    if holding_symbols:
        context["holding_symbols"] = holding_symbols
    return context


def _account_holding_symbols(account_context: dict[str, object] | None) -> list[str]:
    if not isinstance(account_context, dict) or account_context.get("available") is not True:
        return []
    holdings = account_context.get("holdings")
    if not isinstance(holdings, list):
        return []
    symbols: list[str] = []
    for holding in holdings:
        if not isinstance(holding, dict):
            continue
        symbol = str(holding.get("symbol") or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _account_basis(*, settings: Settings, account_context: dict[str, object] | None) -> Decimal:
    if isinstance(account_context, dict) and account_context.get("available") is True:
        for key in ("buying_power_krw", "orderable_cash_krw"):
            value = _decimal_value(account_context.get(key))
            if value is not None and value > 0:
                return value
    return Decimal(str(settings.exec_agent_watch_sizing_fallback_account_basis_krw))


def _account_available_cash(account_context: dict[str, object] | None) -> Decimal | None:
    if not isinstance(account_context, dict) or account_context.get("available") is not True:
        return None
    return _decimal_value(account_context.get("orderable_cash_krw")) or _decimal_value(account_context.get("buying_power_krw"))


def _candidate_budget(item: ExecutionPlanItem) -> Decimal | None:
    for payload in (item.quantity_policy, item.risk_context):
        if isinstance(payload, dict):
            value = _decimal_value(payload.get("budget_krw") or payload.get("allocation_budget_krw"))
            if value is not None and value > 0:
                return value
    return None


def _candidate_max_safe_notional(item: ExecutionPlanItem) -> Decimal | None:
    for payload in (item.quantity_policy, item.risk_context):
        if isinstance(payload, dict):
            value = _decimal_value(payload.get("max_safe_notional_krw"))
            if value is not None and value > 0:
                return value
            feasibility = payload.get("execution_feasibility_context")
            if isinstance(feasibility, dict):
                value = _decimal_value(feasibility.get("max_safe_notional_krw"))
                if value is not None and value > 0:
                    return value
    return None


def _stop_price(item: ExecutionPlanItem) -> Decimal | None:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    for key in ("stop_loss_price", "stop_price", "effective_stop_price"):
        value = _decimal_value(risk_context.get(key))
        if value is not None and value > 0:
            return value
    return None


def _liquidity_tier(item: ExecutionPlanItem) -> str | None:
    for payload in (item.quantity_policy, item.risk_context):
        if isinstance(payload, dict):
            value = payload.get("liquidity_tier")
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
            feasibility = payload.get("execution_feasibility_context")
            if isinstance(feasibility, dict):
                value = feasibility.get("liquidity_tier")
                if isinstance(value, str) and value.strip():
                    return value.strip().lower()
    return None


def _decision_record(*, decision: dict[str, object], item: ExecutionPlanItem, version: str) -> dict[str, object]:
    action = str(decision.get("action") or "").strip().lower()
    quantity = int(decision.get("quantity") or 0)
    target_profile = _decision_target_profile(decision=decision, item=item)
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
        "target_profile": target_profile,
        "target_tag": f"target_profile:{target_profile}",
        "reason_code": decision.get("reason_code"),
        "reason": decision.get("reason"),
        "decided_at": datetime.now(UTC).isoformat(),
    }


def _decision_target_profile(*, decision: dict[str, object], item: ExecutionPlanItem) -> str:
    raw_profile = str(decision.get("target_profile") or "").strip().lower()
    if raw_profile in {"tactical_2pct", "short_swing", "standard_breakout", "extended_momentum"}:
        return raw_profile

    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    reference_price = _positive_decimal(decision.get("sizing_price")) or item.trigger_price
    target_price = _positive_decimal(risk_context.get("target_price"))
    if reference_price is None or target_price is None:
        return "standard_breakout"
    target_distance = (target_price - reference_price) / reference_price
    if target_distance <= Decimal("0.025"):
        return "tactical_2pct"
    if target_distance <= Decimal("0.05"):
        return "short_swing"
    if target_distance <= Decimal("0.08"):
        return "standard_breakout"
    return "extended_momentum"


def _positive_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _decimal_value(value: object) -> Decimal | None:
    return _positive_decimal(value)


def _floor_quantity(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_DOWN))


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.0001'), rounding=ROUND_DOWN):.4f}"


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
