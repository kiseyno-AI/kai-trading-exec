from __future__ import annotations

from datetime import date
from typing import Any

from app.models.runtime import BrokerOrder, ExecutionPlanItem


EXECUTION_DECISION_LINEAGE_VERSION = "execution-decision-lineage-v1"
SELL_MONITOR_AUTHORITY_KEY = "sell_monitor_authority_v1"
SELL_MONITOR_AUTHORITY_VERSION = "sell-monitor-authority-v2"
_LINEAGE_STATUSES = frozenset({"complete", "partial"})
_COMPLETE_LINEAGE_FIELDS = (
    "execution_plan_id",
    "execution_plan_item_id",
    "external_plan_id",
    "plan_source",
    "strategy_run_id",
    "target_market_date",
    "source_plan_item_id",
    "decision_agent_name",
    "decision_snapshot_id",
    "decision_run_id",
    "source_market_date",
)


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
    authority = _sell_monitor_authority(plan_item.risk_context)
    sell_authority_present = (
        isinstance(plan_item.risk_context, dict)
        and SELL_MONITOR_AUTHORITY_KEY in plan_item.risk_context
    )
    legacy_authority = (
        plan_item.risk_context.get(SELL_MONITOR_AUTHORITY_KEY)
        if sell_authority_present and isinstance(plan_item.risk_context, dict)
        else None
    )
    execution_plan_id = (
        authority.get("execution_plan_id")
        if authority is not None
        else (None if sell_authority_present else plan_item.execution_plan_id)
    )
    external_plan_id = (
        authority.get("external_plan_id")
        if authority is not None
        else (
            None
            if sell_authority_present
            else (plan.external_plan_id if plan is not None else None)
        )
    )
    plan_source = (
        authority.get("plan_source")
        if authority is not None
        else (
            None if sell_authority_present else (plan.source if plan is not None else None)
        )
    )
    strategy_run_id = (
        authority.get("strategy_run_id")
        if authority is not None
        else (
            None
            if sell_authority_present
            else (plan.strategy_run_id if plan is not None else None)
        )
    )
    target_market_date = (
        authority.get("target_market_date")
        if authority is not None
        else (
            (
                legacy_authority.get("market_date")
                if isinstance(legacy_authority, dict)
                else None
            )
            if sell_authority_present
            else (plan.market_date if plan is not None else None)
        )
    )
    lineage: dict[str, object] = {
        "schema_version": EXECUTION_DECISION_LINEAGE_VERSION,
        "status": "partial",
        "execution_plan_id": execution_plan_id,
        # In-place sell replans retain the operational row/FK while a newer
        # Core plan becomes its decision authority. Keep both identities
        # explicit instead of presenting the cross-generation pair as one DB
        # parent-child relationship.
        "operational_execution_plan_id": plan_item.execution_plan_id,
        "execution_plan_item_id": plan_item.id,
        "external_plan_id": external_plan_id,
        "plan_source": plan_source,
        "strategy_run_id": strategy_run_id,
        "target_market_date": _date_text(target_market_date),
        "source_plan_item_id": plan_item.source_plan_item_id,
        "decision_agent_name": plan_item.decision_agent_name,
        "decision_snapshot_id": plan_item.decision_snapshot_id,
        "decision_run_id": plan_item.decision_run_id,
        "source_market_date": _date_text(plan_item.source_market_date),
    }
    if _lineage_identity_is_complete(lineage):
        lineage["status"] = "complete"
    return lineage


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

    assessment = _metadata_lineage_assessment(
        request_metadata,
        execution_plan_item_id=execution_plan_item_id,
    )
    lineage = assessment.get("lineage")
    return dict(lineage) if isinstance(lineage, dict) else None


def resolve_decision_lineage_evidence(
    *,
    order: BrokerOrder | None,
    fill_payload: object = None,
) -> dict[str, object]:
    """Resolve immutable order/fill snapshots without failing open to mutable rows."""

    return resolve_decision_lineage_payloads(
        order_metadata=order.request_metadata if order is not None else None,
        fill_payload=fill_payload,
        execution_plan_item_id=(
            order.execution_plan_item_id if order is not None else None
        ),
    )


def resolve_decision_lineage_payloads(
    *,
    order_metadata: object,
    fill_payload: object,
    execution_plan_item_id: object,
) -> dict[str, object]:
    """Resolve persisted payloads when a reporting query does not load ORM rows."""

    order_assessment = _metadata_lineage_assessment(
        order_metadata,
        execution_plan_item_id=execution_plan_item_id,
    )
    fill_assessment = _metadata_lineage_assessment(
        fill_payload,
        execution_plan_item_id=execution_plan_item_id,
    )
    if isinstance(fill_payload, dict) and fill_payload.get(
        "decision_lineage_conflict"
    ) is True:
        return {
            "status": "conflict",
            "source": "fill_conflict_marker",
            "lineage": None,
            "order_snapshot_status": order_assessment["status"],
            "fill_snapshot_status": fill_assessment["status"],
        }

    order_present = order_assessment["status"] != "absent"
    fill_present = fill_assessment["status"] != "absent"
    if order_present and fill_present:
        if (
            order_assessment["status"] == fill_assessment["status"]
            and order_assessment.get("lineage") == fill_assessment.get("lineage")
            and order_assessment["status"] in _LINEAGE_STATUSES
        ):
            return {
                "status": order_assessment["status"],
                "source": "order_and_fill_snapshot",
                "lineage": order_assessment["lineage"],
                "order_snapshot_status": order_assessment["status"],
                "fill_snapshot_status": fill_assessment["status"],
            }
        return {
            "status": "conflict",
            "source": "order_fill_snapshot_conflict",
            "lineage": None,
            "order_snapshot_status": order_assessment["status"],
            "fill_snapshot_status": fill_assessment["status"],
        }
    if order_present:
        return {
            "status": order_assessment["status"],
            "source": "order_snapshot",
            "lineage": order_assessment.get("lineage"),
            "order_snapshot_status": order_assessment["status"],
            "fill_snapshot_status": "absent",
        }
    if fill_present:
        return {
            "status": fill_assessment["status"],
            "source": "fill_snapshot",
            "lineage": fill_assessment.get("lineage"),
            "order_snapshot_status": "absent",
            "fill_snapshot_status": fill_assessment["status"],
        }
    return {
        "status": "legacy_degraded",
        "source": "mutable_plan_item_fallback",
        "lineage": None,
        "order_snapshot_status": "absent",
        "fill_snapshot_status": "absent",
    }


def _metadata_lineage_assessment(
    metadata: object,
    *,
    execution_plan_item_id: object,
) -> dict[str, object]:
    if not isinstance(metadata, dict) or "decision_lineage" not in metadata:
        return {"status": "absent", "lineage": None}
    raw = metadata.get("decision_lineage")
    if not isinstance(raw, dict):
        return {"status": "invalid", "lineage": None}
    raw_status = raw.get("status")
    if (
        raw.get("schema_version") != EXECUTION_DECISION_LINEAGE_VERSION
        or raw_status not in _LINEAGE_STATUSES
    ):
        return {"status": "invalid", "lineage": None}
    persisted_item_id = _optional_text(raw.get("execution_plan_item_id"))
    order_item_id = _optional_text(execution_plan_item_id)
    if persisted_item_id is None or persisted_item_id != order_item_id:
        return {"status": "invalid", "lineage": None}
    normalized: dict[str, object] = {
        "schema_version": EXECUTION_DECISION_LINEAGE_VERSION,
        "status": str(raw_status),
        "execution_plan_id": _optional_text(raw.get("execution_plan_id")),
        "operational_execution_plan_id": _optional_text(
            raw.get("operational_execution_plan_id")
        ),
        "execution_plan_item_id": persisted_item_id,
        "external_plan_id": _optional_text(raw.get("external_plan_id")),
        "plan_source": _optional_text(raw.get("plan_source")),
        "strategy_run_id": _optional_text(raw.get("strategy_run_id")),
        "target_market_date": _date_text(raw.get("target_market_date")),
        "source_plan_item_id": _optional_text(raw.get("source_plan_item_id")),
        "decision_agent_name": _optional_text(raw.get("decision_agent_name")),
        "decision_snapshot_id": _optional_text(raw.get("decision_snapshot_id")),
        "decision_run_id": _optional_text(raw.get("decision_run_id")),
        "source_market_date": _date_text(raw.get("source_market_date")),
    }
    if raw_status == "complete" and not _lineage_identity_is_complete(normalized):
        normalized["status"] = "partial"
    return {"status": normalized["status"], "lineage": normalized}


def _lineage_identity_is_complete(lineage: dict[str, object]) -> bool:
    return bool(
        all(_optional_text(lineage.get(field)) for field in _COMPLETE_LINEAGE_FIELDS)
        and lineage.get("decision_run_id") == lineage.get("strategy_run_id")
    )


def _sell_monitor_authority(risk_context: object) -> dict[str, object] | None:
    if not isinstance(risk_context, dict):
        return None
    authority = risk_context.get(SELL_MONITOR_AUTHORITY_KEY)
    if not isinstance(authority, dict):
        return None
    if authority.get("schema_version") != SELL_MONITOR_AUTHORITY_VERSION:
        return None
    return authority


def payload_with_order_decision_lineage(
    payload: dict[str, Any] | None,
    *,
    order: BrokerOrder,
) -> dict[str, Any]:
    result = dict(payload or {})
    lineage = decision_lineage_for_order(order)
    lineage_key_present = "decision_lineage" in result
    existing = result.get("decision_lineage")
    if lineage is not None and not lineage_key_present:
        result["decision_lineage"] = lineage
    elif lineage is not None:
        existing_assessment = _metadata_lineage_assessment(
            {"decision_lineage": existing},
            execution_plan_item_id=order.execution_plan_item_id,
        )
        if existing_assessment.get("lineage") != lineage:
            # Never silently rewrite already-persisted historical attribution.
            # Compare normalized snapshots so additive optional fields do not
            # make a semantically identical older v1 payload look conflicting.
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
