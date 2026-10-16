from __future__ import annotations

from datetime import date
from typing import Any

from app.models.runtime import BrokerOrder, ExecutionPlanItem


EXECUTION_DECISION_LINEAGE_VERSION = "execution-decision-lineage-v1"
_LINEAGE_STATUSES = frozenset({"complete", "partial"})


def decision_lineage_for_plan_item(
    plan_item: ExecutionPlanItem | None,
) -> dict[str, object] | None:
    """Capture the decision identity in force when an order is created.

    Plan items used for long-lived sell monitoring may be updated in place by a
    later Core handoff.  Persisting this bounded snapshot on the order keeps a
    historical fill attributed to the decision that actually authorized it.
    Missing fields remain explicit; this helper never manufactures lineage.
    """

    if plan_item is None:
        return None
    plan = plan_item.plan
    strategy_run_id = plan.strategy_run_id if plan is not None else None
    required_identity = (
        plan_item.id,
        plan_item.execution_plan_id,
        plan_item.source_plan_item_id,
        plan_item.decision_agent_name,
        plan_item.decision_snapshot_id,
        plan_item.decision_run_id,
    )
    core_run_matches = bool(
        plan is None
        or plan.source != "kai-trading-core"
        or (
            strategy_run_id is not None
            and plan_item.decision_run_id == strategy_run_id
        )
    )
    status = (
        "complete"
        if all(value is not None and str(value).strip() for value in required_identity)
        and core_run_matches
        else "partial"
    )
    return {
        "schema_version": EXECUTION_DECISION_LINEAGE_VERSION,
        "status": status,
        "execution_plan_id": plan_item.execution_plan_id,
        "execution_plan_item_id": plan_item.id,
        "external_plan_id": plan.external_plan_id if plan is not None else None,
        "plan_source": plan.source if plan is not None else None,
        "strategy_run_id": strategy_run_id,
        "target_market_date": _date_text(plan.market_date) if plan is not None else None,
        "source_plan_item_id": plan_item.source_plan_item_id,
        "decision_agent_name": plan_item.decision_agent_name,
        "decision_snapshot_id": plan_item.decision_snapshot_id,
        "decision_run_id": plan_item.decision_run_id,
        "source_market_date": _date_text(plan_item.source_market_date),
    }


def decision_lineage_for_order(order: BrokerOrder | None) -> dict[str, object] | None:
    """Return only a structurally valid lineage snapshot persisted on an order."""

    if order is None:
        return None
    return decision_lineage_from_metadata(
        order.request_metadata,
        execution_plan_item_id=order.execution_plan_item_id,
    )


def decision_lineage_from_metadata(
    request_metadata: object,
    *,
    execution_plan_item_id: object,
) -> dict[str, object] | None:
    """Validate a persisted lineage snapshot without consulting mutable plan rows."""

    if not isinstance(request_metadata, dict):
        return None
    lineage = request_metadata.get("decision_lineage")
    if not isinstance(lineage, dict):
        return None
    if lineage.get("schema_version") != EXECUTION_DECISION_LINEAGE_VERSION:
        return None
    if lineage.get("status") not in _LINEAGE_STATUSES:
        return None
    persisted_item_id = _optional_text(lineage.get("execution_plan_item_id"))
    order_item_id = _optional_text(execution_plan_item_id)
    if persisted_item_id is None or persisted_item_id != order_item_id:
        return None
    normalized = {
        "schema_version": EXECUTION_DECISION_LINEAGE_VERSION,
        "status": str(lineage["status"]),
        "execution_plan_id": _optional_text(lineage.get("execution_plan_id")),
        "execution_plan_item_id": persisted_item_id,
        "external_plan_id": _optional_text(lineage.get("external_plan_id")),
        "plan_source": _optional_text(lineage.get("plan_source")),
        "strategy_run_id": _optional_text(lineage.get("strategy_run_id")),
        "target_market_date": _date_text(lineage.get("target_market_date")),
        "source_plan_item_id": _optional_text(lineage.get("source_plan_item_id")),
        "decision_agent_name": _optional_text(lineage.get("decision_agent_name")),
        "decision_snapshot_id": _optional_text(lineage.get("decision_snapshot_id")),
        "decision_run_id": _optional_text(lineage.get("decision_run_id")),
        "source_market_date": _date_text(lineage.get("source_market_date")),
    }
    required = (
        normalized["execution_plan_id"],
        normalized["execution_plan_item_id"],
        normalized["source_plan_item_id"],
        normalized["decision_agent_name"],
        normalized["decision_snapshot_id"],
        normalized["decision_run_id"],
    )
    core_run_matches = bool(
        normalized["plan_source"] != "kai-trading-core"
        or (
            normalized["strategy_run_id"] is not None
            and normalized["decision_run_id"] == normalized["strategy_run_id"]
        )
    )
    if normalized["status"] == "complete" and (
        not all(required) or not core_run_matches
    ):
        normalized["status"] = "partial"
    return normalized


def payload_with_order_decision_lineage(
    payload: dict[str, Any] | None,
    *,
    order: BrokerOrder,
) -> dict[str, Any]:
    result = dict(payload or {})
    lineage = decision_lineage_for_order(order)
    existing = result.get("decision_lineage")
    if lineage is not None and existing is None:
        result["decision_lineage"] = lineage
    elif lineage is not None and existing != lineage:
        # Never silently rewrite already-persisted historical attribution.
        result["decision_lineage_conflict"] = True
    return result


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _date_text(value: object) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    try:
        return date.fromisoformat(normalized).isoformat()
    except ValueError:
        return None
