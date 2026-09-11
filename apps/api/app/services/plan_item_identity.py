from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm.exc import DetachedInstanceError

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
    if "plan" in sa_inspect(plan_item).unloaded:
        return None
    plan = plan_item.plan
    if "raw_payload" in sa_inspect(plan).unloaded:
        return None
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
    if "risk_context" in sa_inspect(plan_item).unloaded:
        return None
    return _first_string_value(plan_item=plan_item, keys=_DECISION_AGENT_KEYS)


def plan_item_decision_run_id(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.decision_run_id:
        return plan_item.decision_run_id
    if "risk_context" in sa_inspect(plan_item).unloaded:
        return None
    return _first_string_value(plan_item=plan_item, keys=_DECISION_RUN_ID_KEYS)


def plan_item_source_plan_item_id(plan_item: ExecutionPlanItem) -> str | None:
    if plan_item.source_plan_item_id:
        return plan_item.source_plan_item_id
    if "risk_context" in sa_inspect(plan_item).unloaded:
        return None
    return _first_string_value(plan_item=plan_item, keys=_SOURCE_PLAN_ITEM_ID_KEYS)


def plan_item_source_market_date(plan_item: ExecutionPlanItem) -> date | None:
    if plan_item.source_market_date is not None:
        return plan_item.source_market_date
    if "risk_context" in sa_inspect(plan_item).unloaded:
        if "plan" in sa_inspect(plan_item).unloaded:
            return None
        return plan_item.plan.market_date
    parsed = _first_date_value(plan_item=plan_item, keys=_SOURCE_MARKET_DATE_KEYS)
    if parsed is not None:
        return parsed
    if "plan" in sa_inspect(plan_item).unloaded:
        return None
    return plan_item.plan.market_date


def plan_item_entry_family_key(plan_item: ExecutionPlanItem) -> str | None:
    """Return the exact entry family represented by an entry or managed exit."""
    if plan_item.side == "buy":
        source_key = plan_item_source_plan_item_id(plan_item)
        if source_key is not None:
            return f"source:{source_key}"
        item_id = str(plan_item.id or "").strip()
        return f"execution:{item_id}" if item_id else None

    for payload in (plan_item.quantity_policy, plan_item.risk_context):
        family_key = _entry_family_key_from_payload(payload)
        if family_key is not None:
            return family_key
    return None


def select_plan_item_family_members(
    *,
    anchor: ExecutionPlanItem,
    candidates: Sequence[ExecutionPlanItem],
) -> list[ExecutionPlanItem]:
    """Select only candidates proven to belong to ``anchor``'s entry family.

    A single unlinked candidate is retained for a genuine one-entry legacy
    plan. Linked or multi-entry plans never fall back to symbol-only matching.
    """
    candidate_list = list(candidates)
    anchor_key = plan_item_entry_family_key(anchor)
    if anchor_key is not None:
        exact = [
            candidate
            for candidate in candidate_list
            if plan_item_entry_family_key(candidate) == anchor_key
        ]
        if exact:
            return exact
    if any(plan_item_entry_family_key(candidate) is not None for candidate in candidate_list):
        return []
    if len(candidate_list) != 1 or _same_symbol_entry_count(anchor) > 1:
        return []
    return candidate_list


def _entry_family_key_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    linked_key = payload.get("linked_entry_item_key")
    if isinstance(linked_key, str) and linked_key.strip():
        return f"source:{linked_key.strip()}"
    entry_context = payload.get("entry_context")
    if not isinstance(entry_context, dict):
        return None
    source_key = entry_context.get("source_plan_item_id")
    if isinstance(source_key, str) and source_key.strip():
        return f"source:{source_key.strip()}"
    execution_item_id = entry_context.get("execution_plan_item_id")
    if isinstance(execution_item_id, str) and execution_item_id.strip():
        return f"execution:{execution_item_id.strip()}"
    return None


def _same_symbol_entry_count(anchor: ExecutionPlanItem) -> int:
    try:
        plan = anchor.plan
    except DetachedInstanceError:
        # An unloaded detached plan cannot prove that symbol-only fallback is
        # unambiguous.
        return 2
    if plan is None:
        return 0
    return sum(
        1
        for item in plan.items
        if item.symbol == anchor.symbol and item.side == "buy"
    )


def serialize_plan_item_source_identity(plan_item: ExecutionPlanItem) -> dict[str, object | None]:
    decision_agent_name = plan_item_decision_agent_name(plan_item)
    source_plan_item_id = plan_item_source_plan_item_id(plan_item)
    plan = None if "plan" in sa_inspect(plan_item).unloaded else plan_item.plan
    return {
        "decision_agent_name": decision_agent_name,
        "agent_name": decision_agent_name,
        "decision_snapshot_id": plan_item.decision_snapshot_id,
        "decision_run_id": plan_item_decision_run_id(plan_item),
        "source_plan_item_id": source_plan_item_id,
        "core_plan_item_id": source_plan_item_id,
        "source_plan_id": plan.external_plan_id if plan is not None else None,
        "core_plan_id": plan.external_plan_id if plan is not None else None,
        "plan_source": plan.source if plan is not None else None,
        "strategy_run_id": plan.strategy_run_id if plan is not None else None,
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
    if "risk_context" not in sa_inspect(plan_item).unloaded and isinstance(plan_item.risk_context, dict):
        payloads.append(plan_item.risk_context)
    if "plan" in sa_inspect(plan_item).unloaded:
        return payloads
    plan = plan_item.plan
    if "raw_payload" in sa_inspect(plan).unloaded:
        return payloads
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
