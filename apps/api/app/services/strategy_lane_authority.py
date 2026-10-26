from __future__ import annotations

from copy import deepcopy


STRATEGY_FEEDBACK_SCOPE_VERSION = "strategy-feedback-scope-v1"
STRATEGY_LANE_LEARNING_VERSION = "strategy-lane-learning-v1"
STRATEGY_LANE_OPERATIONAL_LEARNING_VERSION = (
    "strategy-lane-operational-learning-v1"
)


def normalized_decision_lane(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold()
    return normalized or None


def sizing_authority_risk_context(
    risk_context: dict[str, object],
    *,
    decision_lane: str | None,
) -> dict[str, object]:
    """Project persisted audit evidence to one lane's sizing authority.

    Opposite-lane alignment and cross-check material remains persisted for
    audit, but is omitted from the agent request so it cannot change approval
    or quantity.
    """

    projected = _without_cross_lane_sizing_evidence(
        risk_context,
        decision_lane=decision_lane,
    )
    return projected if isinstance(projected, dict) else {}


def sizing_authority_account_context(
    account_context: dict[str, object],
    *,
    decision_lane: str | None,
) -> dict[str, object]:
    """Expose shared account risk plus only the candidate lane's ownership.

    Aggregate holdings and open-order economics remain visible because they
    constrain shared cash and symbol exposure.  Strategy ownership, durable
    identifiers, and family detail from another (or unknown) lane are not
    sizing authority and therefore must not reach the model.
    """

    lane = normalized_decision_lane(decision_lane)
    projected = deepcopy(account_context)

    raw_quantities = projected.get("holding_quantities_by_decision_lane")
    if isinstance(raw_quantities, dict):
        scoped_quantities: dict[str, dict[str, object]] = {}
        for raw_symbol, raw_by_lane in raw_quantities.items():
            if not isinstance(raw_by_lane, dict):
                continue
            matches = [
                quantity
                for raw_lane, quantity in raw_by_lane.items()
                if lane is not None
                and normalized_decision_lane(raw_lane) == lane
            ]
            scoped_quantities[str(raw_symbol)] = (
                {lane: deepcopy(matches[0])}
                if lane is not None and len(matches) == 1
                else {}
            )
        projected["holding_quantities_by_decision_lane"] = scoped_quantities

    raw_orders = projected.get("open_orders")
    if isinstance(raw_orders, list):
        projected["open_orders"] = [
            _sizing_authority_open_order(order, decision_lane=lane)
            for order in raw_orders
            if isinstance(order, dict)
        ]

    return projected


def _sizing_authority_open_order(
    order: dict[str, object],
    *,
    decision_lane: str | None,
) -> dict[str, object]:
    order_lane = normalized_decision_lane(order.get("decision_agent_name"))
    if decision_lane is not None and order_lane == decision_lane:
        return deepcopy(order)

    aggregate_fields = {
        "symbol",
        "side",
        "quantity",
        "remaining_quantity",
        "remaining_quantity_indeterminate",
        "status",
        "limit_price",
        "quote_notional",
        "reserved_notional",
        "reservation_notional",
    }
    return {
        key: deepcopy(value)
        for key, value in order.items()
        if key in aggregate_fields
    }


def strategy_feedback_for_decision_lane(
    feedback: dict[str, object],
    *,
    decision_lane: str | None,
) -> dict[str, object]:
    """Return only learning evidence authorized for one exact strategy lane."""

    lane = normalized_decision_lane(decision_lane)
    if lane is None:
        return _unavailable_lane_feedback(
            decision_lane=None,
            reason="candidate_decision_lane_unavailable",
        )

    learning_by_lane = feedback.get("strategy_learning_by_lane")
    operational_by_lane = feedback.get("operational_learning_by_lane")
    learning = _exact_lane_context(
        learning_by_lane,
        decision_lane=lane,
        schema_version=STRATEGY_LANE_LEARNING_VERSION,
    )
    operational = _exact_lane_context(
        operational_by_lane,
        decision_lane=lane,
        schema_version=STRATEGY_LANE_OPERATIONAL_LEARNING_VERSION,
    )
    if learning is None:
        return _unavailable_lane_feedback(
            decision_lane=lane,
            reason="strategy_lane_learning_unavailable",
            operational=operational,
        )

    scoped = deepcopy(learning)
    scoped["decision_lane"] = lane
    scoped["execution_authority_scope"] = "exact_decision_lane_only"
    if not isinstance(scoped.get("snippet"), dict):
        quality_gate = scoped.get("quality_gate")
        quality_gate = quality_gate if isinstance(quality_gate, dict) else {}
        consumer_eligible = (
            scoped.get("status") == "ready"
            and scoped.get("consumer_eligible") is True
            and scoped.get("execution_authority") is True
        )
        scoped["snippet"] = {
            "summary": deepcopy(scoped.get("summary")),
            "lessons": deepcopy(scoped.get("lessons")),
            "candidate_rule_replay": deepcopy(
                scoped.get("candidate_rule_replay")
            ),
            "quality_gate": {
                **deepcopy(quality_gate),
                "consumer_eligible": consumer_eligible,
                "instruction": (
                    "apply_only_quality_gated_lessons"
                    if consumer_eligible
                    else "do_not_apply_learning_lessons"
                ),
            },
        }
    scoped["operational_learning_context"] = (
        operational
        if operational is not None
        else _unavailable_lane_operational_feedback(
            decision_lane=lane,
            reason="strategy_lane_operational_learning_unavailable",
        )
    )
    return scoped


def _exact_lane_context(
    value: object,
    *,
    decision_lane: str,
    schema_version: str,
) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    matches = [
        context
        for raw_lane, context in value.items()
        if normalized_decision_lane(raw_lane) == decision_lane
        and isinstance(context, dict)
        and context.get("schema_version") == schema_version
        and normalized_decision_lane(context.get("decision_lane"))
        == decision_lane
    ]
    if len(matches) != 1:
        return None
    return deepcopy(matches[0])


def _unavailable_lane_feedback(
    *,
    decision_lane: str | None,
    reason: str,
    operational: dict[str, object] | None = None,
) -> dict[str, object]:
    quality_gate = {
        "status": "unavailable",
        "consumer_eligible": False,
        "blocked_reasons": [reason],
        "instruction": "do_not_apply_learning_lessons",
    }
    return {
        "schema_version": STRATEGY_LANE_LEARNING_VERSION,
        "status": "unavailable",
        "consumer_eligible": False,
        "decision_lane": decision_lane,
        "execution_authority": False,
        "execution_authority_scope": "exact_decision_lane_only",
        "reason": reason,
        "quality_gate": quality_gate,
        "snippet": {"quality_gate": quality_gate},
        "operational_learning_context": (
            operational
            if operational is not None
            else _unavailable_lane_operational_feedback(
                decision_lane=decision_lane,
                reason=reason,
            )
        ),
    }


def _unavailable_lane_operational_feedback(
    *,
    decision_lane: str | None,
    reason: str,
) -> dict[str, object]:
    return {
        "schema_version": STRATEGY_LANE_OPERATIONAL_LEARNING_VERSION,
        "status": "unavailable",
        "consumer_eligible": False,
        "decision_lane": decision_lane,
        "execution_authority": False,
        "execution_authority_scope": "exact_decision_lane_only",
        "blocked_reasons": [reason],
        "failure_count": 0,
        "allowed_effects": ["delay", "downsize", "reject"],
        "prohibited_effects": ["approve", "increase_size"],
        "instruction": "do_not_apply_operational_lessons",
    }


def _without_cross_lane_sizing_evidence(
    value: object,
    *,
    decision_lane: str | None,
) -> object:
    if isinstance(value, dict):
        alignment_derived = _contains_cross_lane_sizing_marker(
            value,
            decision_lane=decision_lane,
        )
        projected: dict[str, object] = {}
        for key, child in value.items():
            if _cross_lane_sizing_key(key, decision_lane=decision_lane):
                continue
            if _evidence_token(key) in {
                "final_enforcement_decision",
                "requested_quantity",
                "final_quantity",
            }:
                # These are persisted server enforcement results, not fresh
                # sizing inputs, even when an older producer omitted its
                # provenance marker.
                continue
            if (
                alignment_derived
                and _evidence_token(key)
                in {
                    "max_quantity",
                    "enforcement_source",
                }
            ):
                continue
            if isinstance(child, list):
                projected[key] = [
                    _without_cross_lane_sizing_evidence(
                        entry,
                        decision_lane=decision_lane,
                    )
                    for entry in child
                    if not _contains_cross_lane_sizing_marker(
                        entry,
                        decision_lane=decision_lane,
                    )
                ]
                continue
            projected[key] = _without_cross_lane_sizing_evidence(
                child,
                decision_lane=decision_lane,
            )
        return projected
    if isinstance(value, list):
        return [
            _without_cross_lane_sizing_evidence(
                entry,
                decision_lane=decision_lane,
            )
            for entry in value
            if not _contains_cross_lane_sizing_marker(
                entry,
                decision_lane=decision_lane,
            )
        ]
    return deepcopy(value)


def _cross_lane_sizing_key(key: object, *, decision_lane: str | None) -> bool:
    normalized = _evidence_token(key)
    if (
        normalized == "alignment_gate"
        or normalized == "signal_decider_alignment"
        or normalized.startswith("alignment_")
        or normalized.endswith("_alignment")
    ):
        return True
    return any(
        marker in normalized
        for marker in _opposite_lane_sizing_markers(decision_lane)
    )


def _contains_cross_lane_sizing_marker(
    value: object,
    *,
    decision_lane: str | None,
) -> bool:
    if isinstance(value, str):
        normalized = _evidence_token(value)
        if "alignment" in normalized:
            return True
        return any(
            marker in normalized
            for marker in _opposite_lane_sizing_markers(decision_lane)
        )
    if isinstance(value, dict):
        return any(
            _cross_lane_sizing_key(key, decision_lane=decision_lane)
            or _contains_cross_lane_sizing_marker(
                child,
                decision_lane=decision_lane,
            )
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(
            _contains_cross_lane_sizing_marker(
                child,
                decision_lane=decision_lane,
            )
            for child in value
        )
    return False


def _opposite_lane_sizing_markers(decision_lane: str | None) -> tuple[str, ...]:
    normalized_lane = _evidence_token(decision_lane)
    if "short_term" in normalized_lane:
        return ("signal_decider",)
    if "signal" in normalized_lane:
        return ("short_term_decider",)
    # Unknown lanes cannot safely treat either strategy agent as sizing
    # authority. Deterministic route/account/risk fields are still retained.
    return ("signal_decider", "short_term_decider")


def _evidence_token(value: object) -> str:
    return "_".join(
        str(value or "").strip().casefold().replace("-", "_").split()
    )
