from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, object_session
from sqlalchemy.orm.exc import DetachedInstanceError

from app.models.runtime import ExecutionPlanItem
from app.services.execution_account_route import (
    execution_account_scope_binding_from_payloads,
    kis_account_route_binding_from_payloads,
)
from app.services.execution_semantics import KRX_ASSET_CLASS_ALIASES, KRX_EXCHANGES

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
    family_key, _error = resolve_plan_item_entry_family_key(plan_item)
    return family_key


def resolve_plan_item_entry_family_key(
    plan_item: ExecutionPlanItem,
    *,
    session: Session | None = None,
) -> tuple[str | None, str | None]:
    """Resolve one family only when every comparable lineage claim agrees."""
    if plan_item.side == "buy":
        source_key = plan_item_source_plan_item_id(plan_item)
        if source_key is not None:
            return f"source:{source_key}", None
        item_id = str(plan_item.id or "").strip()
        if item_id:
            return f"execution:{item_id}", None
        return None, "entry_family_unavailable"

    source_claims: set[str] = set()
    execution_claims: set[str] = set()
    for payload in (plan_item.quantity_policy, plan_item.risk_context):
        payload_sources, payload_executions = _entry_family_claims_from_payload(
            payload
        )
        source_claims.update(payload_sources)
        execution_claims.update(payload_executions)

    if len(source_claims) > 1:
        return None, "entry_family_ambiguous"
    if len(execution_claims) > 1:
        return None, "entry_family_ambiguous"
    if execution_claims:
        execution_item_id = next(iter(execution_claims))
        referenced_family = _referenced_entry_family_key(
            plan_item=plan_item,
            execution_item_id=execution_item_id,
            session=session,
        )
        if referenced_family is None:
            return None, "entry_family_ambiguous"
        if referenced_family.startswith("source:"):
            referenced_source = referenced_family.removeprefix("source:")
            if source_claims and referenced_source not in source_claims:
                return None, "entry_family_ambiguous"
            source_claims.add(referenced_source)
        elif source_claims or referenced_family != f"execution:{execution_item_id}":
            return None, "entry_family_ambiguous"
        else:
            return referenced_family, None
    if source_claims:
        return f"source:{next(iter(source_claims))}", None
    return None, "entry_family_unavailable"


def select_plan_item_family_members(
    *,
    anchor: ExecutionPlanItem,
    candidates: Sequence[ExecutionPlanItem],
) -> list[ExecutionPlanItem]:
    """Select only candidates proven to belong to ``anchor``'s entry family.

    Exact membership requires both the raw entry-family key and normalized
    decision lane to match. A single candidate with neither identity field is
    retained only for a genuine one-entry legacy plan. Partial identities and
    linked or multi-entry plans never fall back to symbol-only matching.
    """
    candidate_list = list(candidates)
    anchor_key = plan_item_entry_family_key(anchor)
    anchor_lane = _normalized_decision_lane(
        plan_item_decision_agent_name(anchor)
    )
    if (anchor_key is None) != (anchor_lane is None):
        return []
    if anchor_key is not None and anchor_lane is not None:
        exact = [
            candidate
            for candidate in candidate_list
            if plan_item_entry_family_key(candidate) == anchor_key
            and _normalized_decision_lane(
                plan_item_decision_agent_name(candidate)
            )
            == anchor_lane
        ]
        if exact:
            return exact
    candidate_identities = [
        (
            plan_item_entry_family_key(candidate),
            _normalized_decision_lane(plan_item_decision_agent_name(candidate)),
        )
        for candidate in candidate_list
    ]
    if any(family_key is not None or lane is not None for family_key, lane in candidate_identities):
        return []
    if len(candidate_list) != 1 or _same_symbol_entry_count(anchor) > 1:
        return []
    return candidate_list


def plan_item_execution_route_key(
    plan_item: ExecutionPlanItem,
) -> tuple[str, str, str, str] | None:
    """Return a credential-free broker route only from exact persisted binding."""
    provider = str(plan_item.provider or "kis").strip().lower()
    if provider in {"korea-investment", "korea_investment"}:
        provider = "kis"
    try:
        raw_risk_context = plan_item.risk_context
    except DetachedInstanceError:
        return None
    risk_context = (
        raw_risk_context if isinstance(raw_risk_context, dict) else {}
    )
    exchange = _plan_item_exchange(
        plan_item=plan_item,
        risk_context=risk_context,
    )
    asset_class = str(plan_item.asset_class or "stock").strip().lower()
    if provider == "kis":
        binding = kis_account_route_binding_from_payloads(
            risk_context,
            plan_item.quantity_policy,
        )
        if binding is None:
            return None
        return (
            provider,
            exchange,
            asset_class,
            binding["configured_route_fingerprint"],
        )
    binding = execution_account_scope_binding_from_payloads(
        risk_context,
        plan_item.quantity_policy,
    )
    if binding is None or binding.get("provider") != provider:
        return None
    return provider, exchange, asset_class, str(binding["account_scope"])


def plan_item_inventory_scope_key(
    plan_item: ExecutionPlanItem,
) -> tuple[str, str, str, str] | None:
    """Return the account inventory scope while preserving exact order routes.

    KIS reconciles domestic holdings as one account-level KRX inventory even
    when an individual order is routed through KRX, NXT, or SOR.  The opaque
    account binding remains part of the key; only the interchangeable domestic
    execution-route fields are folded.
    """

    route_key = plan_item_execution_route_key(plan_item)
    if route_key is None:
        return None
    provider, exchange, asset_class, account_binding = route_key
    if (
        provider == "kis"
        and exchange in KRX_EXCHANGES
        and asset_class in KRX_ASSET_CLASS_ALIASES
    ):
        return provider, "KRX", "stock", account_binding
    return route_key


def _plan_item_exchange(
    *,
    plan_item: ExecutionPlanItem,
    risk_context: dict[str, Any],
) -> str:
    exchange = risk_context.get("exchange")
    if isinstance(exchange, str) and exchange.strip():
        return exchange.strip().upper()
    venue = str(plan_item.venue or "").strip().upper()
    if venue in KRX_EXCHANGES | {"AMEX", "NASD", "NYSE", "US", "UPBIT"}:
        return venue
    return "KRX"


def _normalized_decision_lane(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    lane = value.strip().casefold()
    return lane or None


def _entry_family_claims_from_payload(
    payload: object,
) -> tuple[set[str], set[str]]:
    if not isinstance(payload, dict):
        return set(), set()
    source_claims: set[str] = set()
    execution_claims: set[str] = set()
    linked_key = payload.get("linked_entry_item_key")
    if isinstance(linked_key, str) and linked_key.strip():
        source_claims.add(linked_key.strip())
    entry_context = payload.get("entry_context")
    if not isinstance(entry_context, dict):
        return source_claims, execution_claims
    source_key = entry_context.get("source_plan_item_id")
    if isinstance(source_key, str) and source_key.strip():
        source_claims.add(source_key.strip())
    execution_item_id = entry_context.get("execution_plan_item_id")
    if isinstance(execution_item_id, str) and execution_item_id.strip():
        execution_claims.add(execution_item_id.strip())
    return source_claims, execution_claims


def _referenced_entry_family_key(
    *,
    plan_item: ExecutionPlanItem,
    execution_item_id: str,
    session: Session | None = None,
) -> str | None:
    candidates: list[ExecutionPlanItem] = []
    try:
        plan = plan_item.plan
        plan_items = plan.items if plan is not None else ()
    except DetachedInstanceError:
        plan_items = ()
    candidates.extend(
        candidate
        for candidate in plan_items
        if str(candidate.id or "").strip() == execution_item_id
    )
    lookup_session = session if session is not None else object_session(plan_item)
    if lookup_session is not None:
        candidates.extend(
            candidate
            for candidate in lookup_session.identity_map.values()
            if isinstance(candidate, ExecutionPlanItem)
            and str(candidate.id or "").strip() == execution_item_id
        )
        if not candidates:
            referenced = lookup_session.get(ExecutionPlanItem, execution_item_id)
            if referenced is not None:
                candidates.append(referenced)
    unique_matches = {id(candidate): candidate for candidate in candidates}
    if len(unique_matches) != 1:
        return None
    entry = next(iter(unique_matches.values()))
    if (
        str(entry.side or "").strip().lower() != "buy"
        or str(entry.symbol or "").strip().upper()
        != str(plan_item.symbol or "").strip().upper()
    ):
        return None
    family_key, error = resolve_plan_item_entry_family_key(
        entry,
        session=lookup_session,
    )
    return family_key if error is None else None


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
