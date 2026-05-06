from __future__ import annotations

from datetime import date
from typing import Any

from app.models.runtime import ExecutionPlanItem

_DECISION_AGENT_KEYS = (
    "decision_agent_name",
    "agent_name",
    "decider_name",
    "decision_actor",
    "agent",
    "decider",
)
_DECISION_RUN_ID_KEYS = ("decision_run_id", "run_id")
_SOURCE_PLAN_ITEM_ID_KEYS = ("source_plan_item_id", "core_plan_item_id", "plan_item_id")
_SOURCE_MARKET_DATE_KEYS = ("source_market_date", "strategy_market_date", "market_date")
_SOURCE_IDENTITY_SORT_KEYS = (
    "decision_agent_name",
    "decision_snapshot_id",
    "decision_run_id",
    "source_plan_item_id",
    "source_plan_id",
    "source_market_date",
)


def raw_plan_item_payload(plan_item: ExecutionPlanItem) -> dict[str, Any] | None:
    plan = plan_item.plan
    if not isinstance(plan.raw_payload, dict):
        return None
    raw_items = plan.raw_payload.get("items")
    if not isinstance(raw_items, list):
        return None
    if not 0 <= plan_item.plan_item_index < len(raw_items):
        return None
    raw_item = raw_items[plan_item.plan_item_index]
    if isinstance(raw_item, dict):
        return raw_item
    return None


def plan_item_decision_agent_name(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.decision_agent_name:
        return plan_item.decision_agent_name
    return _first_string_value(plan_item=plan_item, keys=_DECISION_AGENT_KEYS)


def plan_item_decision_run_id(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.decision_run_id:
        return plan_item.decision_run_id
    return _first_string_value(plan_item=plan_item, keys=_DECISION_RUN_ID_KEYS)


def plan_item_source_plan_item_id(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.source_plan_item_id:
        return plan_item.source_plan_item_id
    return _first_string_value(plan_item=plan_item, keys=_SOURCE_PLAN_ITEM_ID_KEYS)


def plan_item_source_market_date(plan_item: ExecutionPlanItem) -> date | None:
    if plan_item.source_market_date is not None:
        return plan_item.source_market_date
    parsed = _first_date_value(plan_item=plan_item, keys=_SOURCE_MARKET_DATE_KEYS)
    if parsed is not None:
        return parsed
    return plan_item.plan.market_date


def serialize_plan_item_source_identity(plan_item: ExecutionPlanItem) -> dict[str, object | None]:
    decision_agent_name = plan_item_decision_agent_name(plan_item)
    source_plan_item_id = plan_item_source_plan_item_id(plan_item)
    return {
        "decision_agent_name": decision_agent_name,
        "agent_name": decision_agent_name,
        "decision_snapshot_id": plan_item.decision_snapshot_id,
        "decision_run_id": plan_item_decision_run_id(plan_item),
        "source_plan_item_id": source_plan_item_id,
        "core_plan_item_id": source_plan_item_id,
        "source_plan_id": plan_item.plan.external_plan_id,
        "core_plan_id": plan_item.plan.external_plan_id,
        "plan_source": plan_item.plan.source,
        "strategy_run_id": plan_item.plan.strategy_run_id,
        "source_market_date": _serialize_date(plan_item_source_market_date(plan_item)),
    }


def serialize_symbol_source_identities(
    plan_items: list[ExecutionPlanItem],
) -> list[dict[str, object | None]]:
    identities_by_fingerprint: dict[tuple[object | None, ...], dict[str, object | None]] = {}
    for plan_item in plan_items:
        identity = serialize_plan_item_source_identity(plan_item)
        fingerprint = tuple(identity.get(key) for key in _SOURCE_IDENTITY_SORT_KEYS)
        identities_by_fingerprint[fingerprint] = identity
    return [
        identity
        for _, identity in sorted(
            identities_by_fingerprint.items(),
            key=lambda item: tuple("" if value is None else str(value) for value in item[0]),
        )
    ]


def _first_string_value(
    *,
    plan_item: ExecutionPlanItem,
    keys: tuple[str, ...],
) -> str | None:
    for candidate in _candidate_payloads(plan_item):
        for key in keys:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _first_date_value(
    *,
    plan_item: ExecutionPlanItem,
    keys: tuple[str, ...],
) -> date | None:
    for candidate in _candidate_payloads(plan_item):
        for key in keys:
            value = candidate.get(key)
            parsed = _parse_date(value)
            if parsed is not None:
                return parsed
    return None


def _candidate_payloads(plan_item: ExecutionPlanItem) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    raw_item = raw_plan_item_payload(plan_item)
    if raw_item is not None:
        payloads.append(raw_item)
    if isinstance(plan_item.risk_context, dict):
        payloads.append(plan_item.risk_context)
    plan = plan_item.plan
    if isinstance(plan.raw_payload, dict):
        payloads.append(plan.raw_payload)
    return payloads


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None
