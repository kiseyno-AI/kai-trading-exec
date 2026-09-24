from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import hmac
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import (
    CORE_V4_UNFULFILLED_BUY_EXECUTABLE_FIELDS,
    CORE_EXECUTION_ACTIVATION_BINDING_KEY,
    CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION,
    CORE_EXECUTION_ACTIVATION_CAPABILITY,
    CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION,
    CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
    CORE_EXECUTION_PLAN_ACK_SCHEMA_VERSION,
    CORE_EXECUTION_PLAN_CAPABILITY,
    CORE_EXECUTION_REVOCATION_CAPABILITY,
    CORE_EXECUTION_REVOCATION_SCHEMA_VERSION,
    CoreExecutionActivationRequestV4,
    CoreExecutionPlanRequestV4,
    CoreExecutionRevocationRequestV4,
    ExecutionPlanRequest,
    _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY,
)
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    new_id,
)
from app.services.execution_account_route import redact_account_references
from app.services.execution_api import CreateExecutionPlanResult, create_execution_plan
from app.services.json_compaction import json_payload_hash


ENTRY_INTENTS = frozenset({"next_open_buy", "intraday_watch_buy"})
SAFE_LOCAL_PRE_SUBMISSION_ORDER_STATUSES = frozenset({"rejected", "failed"})
CORE_V4_PERSISTED_BINDING_KEY = "_core_v4_contract_binding"
CORE_V4_PERSISTED_BINDING_VERSION = "core-v4-persisted-binding-v1"
CORE_V4_ACTIVATION_STATE_KEY = "_core_activation_state"
CORE_V4_ACTIVATION_STATE_VERSION = "core-exec-activation-state-v1"
CORE_V4_ACTION_FIELDS = (
    "handoff_intent",
    "raw_action",
    "tactical_action",
    "generic_action",
)
CORE_V4_SIZED_POLICY_SOURCES = frozenset(
    {
        "exec_agent",
        "exec_agent_watch_sizing",
        "short_term_exec_agent_v1",
    }
)
CORE_V4_EXEC_AGENT_POLICY_KEYS = frozenset(
    {
        "kind",
        "quantity",
        "source",
        "idempotency_key",
        "sizing_price",
        "approved_notional",
        "confidence",
    }
)
CORE_V4_SHORT_TERM_ENTRY_POLICY_KEYS = frozenset(
    {"kind", "quantity", "source"}
)
CORE_V4_SHORT_TERM_EXIT_POLICY_KEYS = frozenset(
    {"kind", "quantity", "source", "linked_entry_item_key"}
)
CORE_V4_WATCH_POLICY_KEYS = frozenset(
    {
        "kind",
        "quantity",
        "source",
        "approval_provenance",
        "sizing_price",
        "approved_notional",
        "confidence",
        "target_profile",
        "target_tag",
        "reason_code",
        "exec_agent_decision",
        "short_term_exec_agent_decision",
        "exec_agent_decision_audit",
        "sizing_adjustment",
        "sizing_explanation",
        "allowed_action_envelope",
        "market_regime_guardrail",
        "previous_policy",
    }
)
CORE_V4_WATCH_ATTEMPT_POLICY_KEYS = frozenset(
    {
        "exec_agent_watch_sizing_attempt_count",
        "exec_agent_watch_sizing_max_attempts",
        "exec_agent_watch_sizing_fallback_allowed",
        "exec_agent_watch_sizing_attempts",
    }
)


def create_core_execution_plan_v4(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: CoreExecutionPlanRequestV4,
) -> CreateExecutionPlanResult:
    if command.mode != settings.runtime_mode:
        return CreateExecutionPlanResult(
            status_code=409,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "mode",
                        "message": "request mode does not match the execution runtime",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {"mode": settings.runtime_mode},
            },
        )

    if any(item.asset_class == "us_equity" for item in command.items):
        # Exec's current delegated sizing worker is KRW/KRX-specific. Accepting
        # a USD plan here would produce a durable capability acknowledgement,
        # then quarantine it later or, worse, mix domestic account limits into
        # US sizing. Reject before the plan or any jobs are persisted.
        return CreateExecutionPlanResult(
            status_code=422,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "items",
                        "message": (
                            "US equity delegated sizing is not supported by this Exec runtime"
                        ),
                        "reason_code": "unsupported_us_equity_exec_agent_sizing",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {"mode": settings.runtime_mode},
            },
        )

    legacy_command = ExecutionPlanRequest.model_validate(command.model_dump(mode="json"))
    result = create_execution_plan(
        settings=settings,
        db=db,
        adapter=adapter,
        command=legacy_command,
        commit=False,
    )
    if result.status_code != 201:
        return result

    plan = db.scalar(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.external_plan_id == command.plan_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
        .options(selectinload(ExecutionPlan.items))
    )
    if plan is None:
        db.rollback()
        return CreateExecutionPlanResult(
            status_code=500,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "plan_id",
                        "message": "persisted v4 execution plan could not be read back",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {"mode": settings.runtime_mode},
            },
        )
    protected_family_failure = _protected_entry_family_failure(
        command=command,
        plan=plan,
    )
    if protected_family_failure is not None:
        db.rollback()
        return CreateExecutionPlanResult(
            status_code=422,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [protected_family_failure],
                "persisted_execution_item_ids": [],
                "exec_agent": result.payload.get("exec_agent"),
                "runtime": {"mode": settings.runtime_mode},
            },
        )
    strict_raw_payload = _strict_server_normalized_core_payload(
        command=command,
        normalized_payload=plan.raw_payload,
    )
    if not isinstance(strict_raw_payload, dict):
        db.rollback()
        return CreateExecutionPlanResult(
            status_code=500,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "metadata",
                        "message": "persisted v4 execution contract payload is invalid",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {"mode": settings.runtime_mode},
            },
        )
    plan.raw_payload = strict_raw_payload
    _bind_persisted_core_contract(
        plan=plan,
        command=command,
        payload_sha256=json_payload_hash(strict_raw_payload),
    )
    contract_payload = persisted_core_plan_contract_ack(plan)
    if contract_payload is None:
        # The v4 plan has only been flushed in this transaction. Roll it back
        # before returning so no worker can ever observe a claimable plan whose
        # durable contract proof failed.
        db.rollback()
        return CreateExecutionPlanResult(
            status_code=500,
            payload={
                "plan_id": command.plan_id,
                "accepted_item_count": 0,
                "rejected_item_count": len(command.items),
                "validation_failures": [
                    {
                        "field": "metadata",
                        "message": "persisted v4 execution contract proof is invalid",
                    }
                ],
                "persisted_execution_item_ids": [],
                "runtime": {"mode": settings.runtime_mode},
            },
        )
    _hold_core_confirmed_buy_activation(db=db, plan=plan, command=command)
    db.commit()
    return CreateExecutionPlanResult(
        status_code=201,
        payload={
            "plan_id": plan.external_plan_id,
            "execution_plan_id": plan.id,
            "accepted_item_count": len(plan.items),
            "rejected_item_count": 0,
            **contract_payload,
        },
    )


def _core_activation_authority_generation(
    command: CoreExecutionPlanRequestV4,
) -> str | None:
    authority_generation: str | None = None
    for item in command.items:
        risk_context = item.risk_context
        binding = risk_context.get(CORE_EXECUTION_ACTIVATION_BINDING_KEY)
        if binding is None:
            return None
        if (
            not isinstance(binding, dict)
            or set(binding)
            != {
                "schema_version",
                "mode",
                "authority_generation_fingerprint",
            }
            or binding.get("schema_version")
            != CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION
            or binding.get("mode") != "core_confirmed"
        ):
            return None
        item_generation = binding.get("authority_generation_fingerprint")
        if (
            not isinstance(item_generation, str)
            or len(item_generation) != 64
            or any(character not in "0123456789abcdef" for character in item_generation)
        ):
            return None
        if authority_generation is None:
            authority_generation = item_generation
        elif authority_generation != item_generation:
            return None
    return authority_generation


def _hold_core_confirmed_buy_activation(
    *,
    db: Session,
    plan: ExecutionPlan,
    command: CoreExecutionPlanRequestV4,
) -> None:
    authority_generation = _core_activation_authority_generation(command)
    if authority_generation is None:
        return
    for item in plan.items:
        if item.side != "buy" or item.intent not in ENTRY_INTENTS:
            continue
        prior_status = item.status
        risk_context = (
            dict(item.risk_context)
            if isinstance(item.risk_context, dict)
            else {}
        )
        risk_context[CORE_V4_ACTIVATION_STATE_KEY] = {
            "version": CORE_V4_ACTIVATION_STATE_VERSION,
            "status": "pending",
            "prior_status": prior_status,
            "dispatch_material_fingerprint": (
                command.metadata.dispatch_material_fingerprint
            ),
            "authority_generation_fingerprint": authority_generation,
        }
        item.risk_context = risk_context
        item.status = "core_activation_pending"
        plan_item_id = _nonempty_text(item.id)
        if plan_item_id is not None:
            # The plan and items have already been flushed, so this audit is
            # committed atomically with the non-claimable status.
            db.add(
                ExecutionPlanItemAudit(
                    id=new_id(),
                    execution_plan_item_id=plan_item_id,
                    event_type="core_activation_pending",
                    source="kai-trading-exec",
                    summary={"prior_status": prior_status},
                    payload={
                        "schema_version": (
                            CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION
                        ),
                        "authority_generation_fingerprint": authority_generation,
                    },
                )
            )
    plan.status = "activation_pending"


def activate_core_execution_plan_v4(
    *,
    settings: Settings,
    db: Session,
    command: CoreExecutionActivationRequestV4,
) -> CreateExecutionPlanResult:
    plan = db.scalar(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.id == command.execution_plan_id,
            ExecutionPlan.external_plan_id == command.plan_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
        .with_for_update()
        .options(selectinload(ExecutionPlan.items))
    )
    if plan is None:
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=404,
            reason_code="execution_plan_not_found",
        )

    locked_items = db.scalars(
        select(ExecutionPlanItem)
        .where(ExecutionPlanItem.execution_plan_id == plan.id)
        .order_by(ExecutionPlanItem.plan_item_index.asc())
        .with_for_update()
    ).all()
    item_ids = {item.id for item in locked_items}
    db.scalars(
        select(ExecutionJob)
        .where(ExecutionJob.execution_plan_item_id.in_(item_ids))
        .order_by(ExecutionJob.id.asc())
        .with_for_update()
    ).all()

    contract_ack = persisted_core_plan_contract_ack(plan)
    if contract_ack is None:
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=409,
            reason_code="persisted_v4_contract_not_proven",
            execution_plan_id=plan.id,
        )
    persisted_contract = contract_ack.get("contract_ack")
    stored_dispatch_fingerprint = (
        persisted_contract.get("dispatch_material_fingerprint")
        if isinstance(persisted_contract, dict)
        else None
    )
    if (
        not isinstance(stored_dispatch_fingerprint, str)
        or not hmac.compare_digest(
            stored_dispatch_fingerprint,
            command.dispatch_material_fingerprint,
        )
    ):
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=409,
            reason_code="activation_dispatch_material_mismatch",
            execution_plan_id=plan.id,
        )

    stored_command = CoreExecutionPlanRequestV4.model_validate(
        plan.raw_payload,
        context={
            _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY: True,
        },
    )
    stored_authority_generation = _core_activation_authority_generation(
        stored_command
    )
    if (
        stored_authority_generation is None
        or not hmac.compare_digest(
            stored_authority_generation,
            command.authority_generation_fingerprint,
        )
    ):
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=409,
            reason_code="activation_authority_generation_mismatch",
            execution_plan_id=plan.id,
        )

    requested_by_key = {item.core_item_key: item for item in command.items}
    locked_by_key = {
        str(item.source_plan_item_id): item
        for item in locked_items
        if item.source_plan_item_id is not None
    }
    exact_bindings = bool(
        len(locked_items) == len(command.items)
        and set(locked_by_key) == set(requested_by_key)
        and all(
            locked_by_key[key].id == request.execution_plan_item_id
            and locked_by_key[key].decision_snapshot_id
            == request.decision_snapshot_id
            for key, request in requested_by_key.items()
        )
    )
    if not exact_bindings:
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=409,
            reason_code="activation_item_binding_mismatch",
            execution_plan_id=plan.id,
        )

    buy_items = [
        item
        for item in locked_items
        if item.side == "buy" and item.intent in ENTRY_INTENTS
    ]
    states = {
        item.id: _core_activation_state(item)
        for item in buy_items
    }
    replay = bool(
        plan.status == "accepted"
        and states
        and all(
            state is not None and state.get("status") == "activated"
            for state in states.values()
        )
    )
    if replay:
        db.commit()
        return _activation_success(
            command=command,
            execution_plan_id=plan.id,
            idempotent_replay=True,
        )

    pending_state_is_exact = bool(
        plan.status == "activation_pending"
        and buy_items
        and all(item.status == "core_activation_pending" for item in buy_items)
        and all(
            _pending_activation_state_matches(
                state=states[item.id],
                dispatch_material_fingerprint=(
                    command.dispatch_material_fingerprint
                ),
                authority_generation_fingerprint=(
                    command.authority_generation_fingerprint
                ),
            )
            for item in buy_items
        )
    )
    if not pending_state_is_exact:
        db.rollback()
        return _activation_failure(
            command=command,
            status_code=409,
            reason_code="activation_state_not_pending",
            execution_plan_id=plan.id,
        )

    activated_at = datetime.now(UTC)
    for item in buy_items:
        state = states[item.id]
        assert state is not None
        prior_status = str(state["prior_status"])
        risk_context = dict(item.risk_context or {})
        risk_context[CORE_V4_ACTIVATION_STATE_KEY] = {
            **state,
            "status": "activated",
            "activated_at": activated_at.isoformat(),
            "activation_requested_at": command.requested_at.isoformat(),
        }
        item.risk_context = risk_context
        item.status = prior_status
        db.add(
            ExecutionPlanItemAudit(
                id=new_id(),
                execution_plan_item_id=item.id,
                event_type="core_activation_confirmed",
                source="kai-trading-core",
                summary={
                    "prior_status": "core_activation_pending",
                    "restored_status": prior_status,
                },
                payload={
                    "schema_version": command.schema_version,
                    "contract_version": command.contract_version,
                    "required_capability": command.required_capability,
                    "authority_generation_fingerprint": (
                        command.authority_generation_fingerprint
                    ),
                    "dispatch_material_fingerprint": (
                        command.dispatch_material_fingerprint
                    ),
                    "requested_at": command.requested_at.isoformat(),
                },
            )
        )
    plan.status = "accepted"
    db.commit()
    return _activation_success(
        command=command,
        execution_plan_id=plan.id,
        idempotent_replay=False,
    )


def _core_activation_state(
    item: ExecutionPlanItem,
) -> dict[str, object] | None:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    state = risk_context.get(CORE_V4_ACTIVATION_STATE_KEY)
    return dict(state) if isinstance(state, dict) else None


def _pending_activation_state_matches(
    *,
    state: dict[str, object] | None,
    dispatch_material_fingerprint: str,
    authority_generation_fingerprint: str,
) -> bool:
    return bool(
        isinstance(state, dict)
        and set(state)
        == {
            "version",
            "status",
            "prior_status",
            "dispatch_material_fingerprint",
            "authority_generation_fingerprint",
        }
        and state.get("version") == CORE_V4_ACTIVATION_STATE_VERSION
        and state.get("status") == "pending"
        and _nonempty_text(state.get("prior_status")) is not None
        and state.get("prior_status") != "core_activation_pending"
        and state.get("dispatch_material_fingerprint")
        == dispatch_material_fingerprint
        and state.get("authority_generation_fingerprint")
        == authority_generation_fingerprint
    )


def _activation_success(
    *,
    command: CoreExecutionActivationRequestV4,
    execution_plan_id: str,
    idempotent_replay: bool,
) -> CreateExecutionPlanResult:
    return CreateExecutionPlanResult(
        status_code=200,
        payload={
            "plan_id": command.plan_id,
            "execution_plan_id": execution_plan_id,
            "contract_ack": {
                "schema_version": CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION,
                "contract_version": CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
                "capability": CORE_EXECUTION_ACTIVATION_CAPABILITY,
                "plan_id": command.plan_id,
                "status": "activated",
                "dispatch_material_fingerprint": (
                    command.dispatch_material_fingerprint
                ),
                "authority_generation_fingerprint": (
                    command.authority_generation_fingerprint
                ),
            },
            "item_acks": [
                {
                    "core_item_key": item.core_item_key,
                    "execution_plan_item_id": item.execution_plan_item_id,
                    "decision_snapshot_id": item.decision_snapshot_id,
                    "status": "activated",
                    "execution_plan_id": execution_plan_id,
                }
                for item in command.items
            ],
            "activated_item_count": len(command.items),
            "idempotent_replay": idempotent_replay,
        },
    )


def _activation_failure(
    *,
    command: CoreExecutionActivationRequestV4,
    status_code: int,
    reason_code: str,
    execution_plan_id: str | None = None,
) -> CreateExecutionPlanResult:
    payload: dict[str, object] = {
        "plan_id": command.plan_id,
        "reason_code": reason_code,
        "activated_item_count": 0,
        "item_acks": [],
    }
    if execution_plan_id is not None:
        payload["execution_plan_id"] = execution_plan_id
    return CreateExecutionPlanResult(status_code=status_code, payload=payload)


def _protected_entry_family_failure(
    *,
    command: CoreExecutionPlanRequestV4,
    plan: ExecutionPlan,
) -> dict[str, str] | None:
    persisted_by_source_key = {
        item.source_plan_item_id: item
        for item in plan.items
        if _nonempty_text(item.source_plan_item_id) is not None
    }
    for entry in command.items:
        if (
            entry.side != "buy"
            or entry.intent != "next_open_buy"
            or entry.decision_agent_name != "short-term-decider"
        ):
            continue
        persisted_entry = persisted_by_source_key.get(entry.core_item_key)
        required_jobs = (
            [
                job
                for job in persisted_entry.jobs
                if job.job_type == "next_open_buy"
                and job.status in {"queued", "scheduled", "retrying"}
            ]
            if persisted_entry is not None
            else []
        )
        if (
            persisted_entry is None
            or persisted_entry.status != "next_open_buy_scheduled"
            or len(required_jobs) != 1
        ):
            return {
                "field": "items",
                "message": (
                    "short-term protected entry family did not produce one "
                    "claimable entry job"
                ),
                "reason_code": "protected_entry_family_not_claimable",
                "core_item_key": entry.core_item_key,
                "entry_status": (
                    persisted_entry.status
                    if persisted_entry is not None
                    else "missing"
                ),
            }
    return None


def _strict_server_normalized_core_payload(
    *,
    command: CoreExecutionPlanRequestV4,
    normalized_payload: object,
) -> dict[str, object] | None:
    strict_payload = redact_account_references(command.model_dump(mode="json"))
    normalized_payload = redact_account_references(normalized_payload)
    if not isinstance(strict_payload, dict) or not isinstance(normalized_payload, dict):
        return None
    strict_items = strict_payload.get("items")
    normalized_items = normalized_payload.get("items")
    if (
        not isinstance(strict_items, list)
        or not isinstance(normalized_items, list)
        or len(strict_items) != len(normalized_items)
    ):
        return None

    for strict_item, normalized_item in zip(
        strict_items,
        normalized_items,
        strict=True,
    ):
        if not isinstance(strict_item, dict) or not isinstance(normalized_item, dict):
            return None
        normalized_risk = normalized_item.get("risk_context")
        normalized_policy = normalized_item.get("quantity_policy")
        if not isinstance(normalized_risk, dict) or not isinstance(
            normalized_policy,
            dict,
        ):
            return None
        canonical_risk = dict(normalized_risk)
        canonical_risk.pop(CORE_V4_PERSISTED_BINDING_KEY, None)
        for field_name in CORE_V4_ACTION_FIELDS:
            action = strict_item.get(field_name)
            if action is None:
                canonical_risk.pop(field_name, None)
            else:
                canonical_risk[field_name] = action
        strict_item["risk_context"] = canonical_risk
        strict_item["quantity_policy"] = dict(normalized_policy)

    try:
        validated = CoreExecutionPlanRequestV4.model_validate(
            strict_payload,
            context={
                _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY: True,
            },
        )
    except ValidationError:
        return None
    normalized_strict_payload = validated.model_dump(mode="json")
    return (
        normalized_strict_payload
        if isinstance(normalized_strict_payload, dict)
        else None
    )


def _bind_persisted_core_contract(
    *,
    plan: ExecutionPlan,
    command: CoreExecutionPlanRequestV4,
    payload_sha256: str,
) -> None:
    raw_payload = plan.raw_payload if isinstance(plan.raw_payload, dict) else {}
    raw_items = raw_payload.get("items")
    if not isinstance(raw_items, list) or len(raw_items) != len(plan.items):
        return
    for index, (raw_item, item) in enumerate(
        zip(raw_items, plan.items, strict=True)
    ):
        if not isinstance(raw_item, dict):
            continue
        raw_risk = raw_item.get("risk_context")
        if not isinstance(raw_risk, dict):
            continue
        risk_context = (
            dict(item.risk_context)
            if isinstance(item.risk_context, dict)
            else {}
        )
        for field_name in CORE_V4_ACTION_FIELDS:
            if field_name not in raw_risk:
                risk_context.pop(field_name, None)
        risk_context.update(raw_risk)
        risk_context[CORE_V4_PERSISTED_BINDING_KEY] = {
            "version": CORE_V4_PERSISTED_BINDING_VERSION,
            "plan_payload_sha256": payload_sha256,
            "core_item_key": raw_item.get("core_item_key"),
            "plan_item_index": index,
        }
        item.risk_context = risk_context
        item.decision_run_id = command.strategy_run_id
        item.source_market_date = command.metadata.source_market_date


def _quarantine_unproven_core_plan(*, db: Session, plan: ExecutionPlan) -> None:
    item_ids = {item.id for item in plan.items}
    jobs = db.scalars(
        select(ExecutionJob)
        .where(ExecutionJob.execution_plan_item_id.in_(item_ids))
        .with_for_update()
    ).all()
    for item in plan.items:
        prior_status = item.status
        item.status = "contract_quarantined"
        db.add(
            ExecutionPlanItemAudit(
                id=new_id(),
                execution_plan_item_id=item.id,
                event_type="core_contract_quarantined",
                source="kai-trading-exec",
                summary={"prior_status": prior_status},
                payload={"reason_code": "persisted_v4_contract_not_proven"},
            )
        )
    for job in jobs:
        job.status = "revoked"
        job.locked_at = None
        job.last_error = "persisted v4 contract proof failed"
    plan.status = "contract_quarantined"
    db.commit()


def fence_unproven_legacy_core_buy_authority(
    *,
    db: Session,
    execution_plan_item_id: str,
) -> bool:
    """Atomically make pre-v4 Core BUY authority nonclaimable.

    Protective exits remain untouched because an unverified historical entry may
    already have exposure that has not reached local fill truth yet.
    """
    plan_id = db.scalar(
        select(ExecutionPlanItem.execution_plan_id).where(
            ExecutionPlanItem.id == execution_plan_item_id
        )
    )
    if plan_id is None:
        return False
    plan = db.scalar(
        select(ExecutionPlan)
        .where(ExecutionPlan.id == plan_id)
        .with_for_update()
        .options(selectinload(ExecutionPlan.items))
    )
    raw_payload = plan.raw_payload if plan is not None and isinstance(plan.raw_payload, dict) else {}
    if (
        plan is None
        or (
            plan.source != "kai-trading-core"
            and raw_payload.get("source") != "kai-trading-core"
        )
        or persisted_core_plan_contract_ack(plan) is not None
    ):
        return False

    locked_items = db.scalars(
        select(ExecutionPlanItem)
        .where(ExecutionPlanItem.execution_plan_id == plan.id)
        .order_by(ExecutionPlanItem.plan_item_index.asc())
        .with_for_update()
    ).all()
    locked_item_ids = {item.id for item in locked_items}
    all_jobs = db.scalars(
        select(ExecutionJob)
        .where(ExecutionJob.execution_plan_item_id.in_(locked_item_ids))
        .with_for_update()
    ).all()
    entry_job_item_ids = {
        job.execution_plan_item_id
        for job in all_jobs
        if job.job_type in ENTRY_INTENTS
    }
    raw_entry_keys = _raw_core_entry_keys(raw_payload)
    buy_items = [
        item
        for item in locked_items
        if item.side == "buy"
        or item.intent in ENTRY_INTENTS
        or item.source_plan_item_id in raw_entry_keys
        or item.id in entry_job_item_ids
    ]
    buy_item_ids = {item.id for item in buy_items}
    if not buy_item_ids:
        return False
    jobs = [job for job in all_jobs if job.execution_plan_item_id in buy_item_ids]
    broker_orders = db.scalars(
        select(BrokerOrder)
        .where(BrokerOrder.execution_plan_item_id.in_(buy_item_ids))
        .with_for_update()
    ).all()
    broker_order_ids = {order.id for order in broker_orders}
    fills = (
        db.scalars(
            select(ExecutionFill)
            .where(ExecutionFill.broker_order_pk.in_(broker_order_ids))
            .with_for_update()
        ).all()
        if broker_order_ids
        else []
    )
    possible_exposure = bool(
        fills
        or any(
            not (
                str(order.status or "").strip().lower()
                in SAFE_LOCAL_PRE_SUBMISSION_ORDER_STATUSES
                and order.broker_order_id is None
                and order.submitted_at is None
            )
            for order in broker_orders
        )
    )
    fenced_status = (
        "manual_review_required" if possible_exposure else "contract_quarantined"
    )
    for item in buy_items:
        prior_status = item.status
        item.status = fenced_status
        if prior_status != fenced_status:
            db.add(
                ExecutionPlanItemAudit(
                    id=new_id(),
                    execution_plan_item_id=item.id,
                    event_type="legacy_core_authority_quarantined",
                    source="kai-trading-exec",
                    summary={
                        "prior_status": prior_status,
                        "protective_exits_preserved": True,
                    },
                    payload={
                        "reason_code": "persisted_v4_contract_not_proven",
                        "manual_review_required": possible_exposure,
                    },
                )
            )
    for job in jobs:
        job.status = "revoked"
        job.locked_at = None
        job.last_error = "legacy Core BUY lacks an exact persisted v4 contract proof"
    plan.status = fenced_status
    db.commit()
    return execution_plan_item_id in buy_item_ids


def _raw_core_entry_keys(raw_payload: object) -> set[str]:
    if not isinstance(raw_payload, dict):
        return set()
    raw_items = raw_payload.get("items")
    if not isinstance(raw_items, list):
        return set()
    keys: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        side = str(raw_item.get("side") or "").strip().lower()
        intent = str(raw_item.get("intent") or "").strip().lower()
        if side != "buy" and intent not in ENTRY_INTENTS:
            continue
        key = _nonempty_text(
            raw_item.get("core_item_key") or raw_item.get("source_plan_item_id")
        )
        if key is not None:
            keys.add(key)
    return keys


def reconcile_unproven_legacy_core_buy_authority(
    *,
    db: Session,
    runtime_mode: str,
) -> int:
    """Quarantine persisted pre-v4 Core entries before the due-job query."""
    item_ids = list(
        db.scalars(
            select(ExecutionPlanItem.id)
            .join(ExecutionPlanItem.plan)
            .where(
                ExecutionPlan.mode == runtime_mode,
                ExecutionPlan.source == "kai-trading-core",
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.status.not_in(
                    {
                        "contract_quarantined",
                        "manual_review_required",
                        "revoked",
                    }
                ),
            )
            .order_by(ExecutionPlan.id.asc(), ExecutionPlanItem.plan_item_index.asc())
        ).all()
    )
    fenced = 0
    for item_id in item_ids:
        if fence_unproven_legacy_core_buy_authority(
            db=db,
            execution_plan_item_id=item_id,
        ):
            fenced += 1
    return fenced


def list_core_execution_plans_v4(
    *,
    settings: Settings,
    db: Session,
    external_plan_id: str | None = None,
) -> list[dict[str, object]]:
    query = (
        select(ExecutionPlan)
        .where(ExecutionPlan.mode == settings.runtime_mode)
        .options(selectinload(ExecutionPlan.items))
        .order_by(ExecutionPlan.accepted_at.desc(), ExecutionPlan.created_at.desc())
    )
    if external_plan_id:
        query = query.where(ExecutionPlan.external_plan_id == external_plan_id)
    payloads: list[dict[str, object]] = []
    for plan in db.scalars(query).all():
        contract_payload = persisted_core_plan_contract_ack(plan)
        if contract_payload is None:
            continue
        payloads.append(
            {
                "execution_plan_id": plan.id,
                "external_plan_id": plan.external_plan_id,
                "plan_id": plan.external_plan_id,
                "status": plan.status,
                **contract_payload,
            }
        )
    return payloads


def persisted_core_plan_contract_ack(
    plan: ExecutionPlan,
) -> dict[str, object] | None:
    if not isinstance(plan.raw_payload, dict):
        return None
    try:
        command = CoreExecutionPlanRequestV4.model_validate(
            plan.raw_payload,
            context={
                _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY: True,
            },
        )
    except ValidationError:
        return None
    if not _persisted_plan_identity_matches(plan=plan, command=command):
        return None

    payload_sha256 = json_payload_hash(plan.raw_payload)
    raw_keys: list[str] = []
    persisted_items = list(plan.items)
    if len(persisted_items) != len(command.items):
        return None
    persisted_by_source_key: dict[str, ExecutionPlanItem] = {}
    for item in persisted_items:
        source_key = _nonempty_text(item.source_plan_item_id)
        if source_key is None or source_key in persisted_by_source_key:
            return None
        persisted_by_source_key[source_key] = item
    persisted_by_key: dict[str, ExecutionPlanItem] = {}
    persisted_ids: set[str] = set()
    for index, (raw_item, item) in enumerate(
        zip(command.items, persisted_items, strict=True)
    ):
        raw_linked_entry_key = raw_item.quantity_policy.get(
            "linked_entry_item_key"
        )
        linked_entry = (
            persisted_by_source_key.get(raw_linked_entry_key)
            if isinstance(raw_linked_entry_key, str)
            else None
        )
        if not _persisted_item_identity_matches(
            plan=plan,
            item=item,
            raw_item=raw_item,
            expected_index=index,
            expected_source_market_date=command.metadata.source_market_date,
            payload_sha256=payload_sha256,
            linked_entry=linked_entry,
        ):
            return None
        item_id = _nonempty_text(item.id)
        key = raw_item.core_item_key
        if item_id is None or item_id in persisted_ids or key in persisted_by_key:
            return None
        persisted_ids.add(item_id)
        raw_keys.append(key)
        persisted_by_key[key] = item

    return {
        "plan_id": plan.external_plan_id,
        "execution_plan_id": plan.id,
        "accepted_item_count": len(raw_keys),
        "rejected_item_count": 0,
        "contract_ack": {
            "schema_version": CORE_EXECUTION_PLAN_ACK_SCHEMA_VERSION,
            "contract_version": CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
            "capability": CORE_EXECUTION_PLAN_CAPABILITY,
            "plan_id": plan.external_plan_id,
            "status": "accepted",
            "dispatch_material_fingerprint": (
                command.metadata.dispatch_material_fingerprint
            ),
        },
        "item_acks": [
            {
                "core_item_key": key,
                "status": "accepted",
                "execution_plan_id": plan.id,
                "execution_plan_item_id": persisted_by_key[key].id,
            }
            for key in raw_keys
        ],
    }


def _persisted_plan_identity_matches(
    *,
    plan: ExecutionPlan,
    command: CoreExecutionPlanRequestV4,
) -> bool:
    return bool(
        _nonempty_text(plan.id) is not None
        and plan.external_plan_id == command.plan_id
        and plan.source == command.source
        and plan.mode == command.mode
        and plan.market_date == command.market_date
        and plan.strategy_run_id == command.strategy_run_id
        and _same_instant(plan.requested_at, command.requested_at)
    )


def _persisted_item_identity_matches(
    *,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    raw_item: Any,
    expected_index: int,
    expected_source_market_date: Any,
    payload_sha256: str,
    linked_entry: ExecutionPlanItem | None,
) -> bool:
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    session_policy = raw_item.session_policy.model_dump(mode="json")
    quantity_policy = (
        item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    )
    raw_policy = raw_item.quantity_policy
    raw_risk = raw_item.risk_context
    return bool(
        item.execution_plan_id == plan.id
        and item.plan_item_index == expected_index
        and item.source_plan_item_id == raw_item.core_item_key
        and item.symbol == raw_item.symbol
        and item.side == raw_item.side
        and item.intent == raw_item.intent
        and item.provider == raw_item.provider
        and item.venue == raw_item.venue
        and item.asset_class == raw_item.asset_class
        and item.decision_agent_name == raw_item.decision_agent_name
        and item.decision_snapshot_id == raw_item.decision_snapshot_id
        and item.decision_run_id == plan.strategy_run_id
        and item.source_market_date == expected_source_market_date
        and item.time_in_force == raw_item.time_in_force
        and _limit_price_matches(
            raw_limit_price=raw_item.limit_price,
            current_limit_price=item.limit_price,
            quantity_policy=quantity_policy,
        )
        and item.trigger_price == raw_item.trigger_price
        and item.base_quantity is None
        and item.quote_notional is None
        and item.quantity_precision is None
        and item.price_precision is None
        and risk_context.get("market") == raw_item.market
        and risk_context.get("exchange") == raw_item.exchange
        and risk_context.get("currency") == raw_item.currency
        and risk_context.get("broker") == raw_item.broker
        and risk_context.get("session_policy") == session_policy
        and _persisted_contract_binding_matches(
            risk_context=risk_context,
            core_item_key=raw_item.core_item_key,
            expected_index=expected_index,
            payload_sha256=payload_sha256,
        )
        and _action_fields_match(raw_item=raw_item, risk_context=risk_context)
        and _payload_is_subset(expected=raw_risk, actual=risk_context)
        and _quantity_policy_matches(
            raw_policy=raw_policy,
            quantity_policy=quantity_policy,
            risk_context=risk_context,
            side=raw_item.side,
            item=item,
            linked_entry=linked_entry,
        )
    )


def _persisted_contract_binding_matches(
    *,
    risk_context: dict[str, object],
    core_item_key: str,
    expected_index: int,
    payload_sha256: str,
) -> bool:
    binding = risk_context.get(CORE_V4_PERSISTED_BINDING_KEY)
    return bool(
        isinstance(binding, dict)
        and set(binding)
        == {
            "version",
            "plan_payload_sha256",
            "core_item_key",
            "plan_item_index",
        }
        and binding.get("version") == CORE_V4_PERSISTED_BINDING_VERSION
        and binding.get("plan_payload_sha256") == payload_sha256
        and binding.get("core_item_key") == core_item_key
        and binding.get("plan_item_index") == expected_index
    )


def _action_fields_match(*, raw_item: Any, risk_context: dict[str, object]) -> bool:
    for field_name in CORE_V4_ACTION_FIELDS:
        expected = getattr(raw_item, field_name)
        if expected is None:
            if field_name in risk_context and risk_context.get(field_name) is not None:
                return False
        elif risk_context.get(field_name) != expected:
            return False
    return True


def _payload_is_subset(*, expected: object, actual: object) -> bool:
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return False
    return all(key in actual and actual[key] == value for key, value in expected.items())


def _limit_price_matches(
    *,
    raw_limit_price: Decimal | None,
    current_limit_price: Decimal | None,
    quantity_policy: dict[str, object],
) -> bool:
    if current_limit_price == raw_limit_price:
        return True
    if raw_limit_price is not None:
        return False
    return bool(
        quantity_policy.get("source") == "exec_agent_watch_sizing"
        and _positive_decimal(quantity_policy.get("sizing_price"))
        == current_limit_price
    )


def _quantity_policy_matches(
    *,
    raw_policy: object,
    quantity_policy: object,
    risk_context: dict[str, object],
    side: str,
    item: ExecutionPlanItem,
    linked_entry: ExecutionPlanItem | None,
) -> bool:
    if not isinstance(raw_policy, dict) or not isinstance(quantity_policy, dict):
        return False
    raw_type = raw_policy.get("type")
    if side == "buy" and (
        raw_type != "exec_agent_sized"
        or CORE_V4_UNFULFILLED_BUY_EXECUTABLE_FIELDS.intersection(raw_policy)
    ):
        return False
    if quantity_policy == raw_policy:
        return True
    if _watch_attempt_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=quantity_policy,
    ):
        return True

    quantity = quantity_policy.get("quantity")
    source = quantity_policy.get("source")
    if (
        quantity_policy.get("kind") != "shares"
        or isinstance(quantity, bool)
        or not isinstance(quantity, int)
        or quantity <= 0
        or source not in CORE_V4_SIZED_POLICY_SOURCES
    ):
        return False
    if side == "buy":
        if raw_type != "exec_agent_sized":
            return False
        if source == "exec_agent":
            return _exec_agent_policy_matches(
                raw_policy=raw_policy,
                quantity_policy=quantity_policy,
                risk_context=risk_context,
                decision=risk_context.get("exec_agent_decision"),
            )
        if source == "short_term_exec_agent_v1":
            return _short_term_entry_policy_matches(
                quantity_policy=quantity_policy,
                decision=risk_context.get("exec_agent_decision"),
            )
        return _watch_sizing_policy_matches(
            raw_policy=raw_policy,
            quantity_policy=quantity_policy,
            item=item,
        )
    if raw_type != "position_exit_from_entry":
        return False
    return _short_term_exit_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=quantity_policy,
        risk_context=risk_context,
        item=item,
        linked_entry=linked_entry,
    )


def _watch_attempt_policy_matches(
    *,
    raw_policy: dict[str, object],
    quantity_policy: dict[str, object],
) -> bool:
    if set(quantity_policy) != set(raw_policy) | CORE_V4_WATCH_ATTEMPT_POLICY_KEYS:
        return False
    if any(quantity_policy.get(key) != value for key, value in raw_policy.items()):
        return False
    attempt_count = quantity_policy.get("exec_agent_watch_sizing_attempt_count")
    max_attempts = quantity_policy.get("exec_agent_watch_sizing_max_attempts")
    attempts = quantity_policy.get("exec_agent_watch_sizing_attempts")
    if (
        isinstance(attempt_count, bool)
        or not isinstance(attempt_count, int)
        or attempt_count <= 0
        or isinstance(max_attempts, bool)
        or not isinstance(max_attempts, int)
        or max_attempts <= 0
        or attempt_count > max_attempts
        or quantity_policy.get("exec_agent_watch_sizing_fallback_allowed")
        is not False
        or not isinstance(attempts, list)
        or not 1 <= len(attempts) <= 3
    ):
        return False
    for event in attempts:
        if not _watch_attempt_event_matches(event=event, max_attempts=max_attempts):
            return False
    latest = attempts[-1]
    return bool(
        isinstance(latest, dict) and latest.get("attempt") == attempt_count
    )


def _watch_attempt_event_matches(*, event: object, max_attempts: int) -> bool:
    if not isinstance(event, dict) or not set(event).issubset(
        {"attempt", "max_attempts", "success", "recorded_at", "error"}
    ):
        return False
    attempt = event.get("attempt")
    recorded_at = event.get("recorded_at")
    error = event.get("error")
    if (
        isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or not 1 <= attempt <= max_attempts
        or event.get("max_attempts") != max_attempts
        or not isinstance(event.get("success"), bool)
        or not isinstance(recorded_at, str)
        or (error is not None and (not isinstance(error, str) or len(error) > 500))
    ):
        return False
    try:
        parsed_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed_at.tzinfo is not None and parsed_at.utcoffset() is not None


def _exec_agent_policy_matches(
    *,
    raw_policy: dict[str, object],
    quantity_policy: dict[str, object],
    risk_context: dict[str, object],
    decision: object,
) -> bool:
    if (
        set(quantity_policy) != CORE_V4_EXEC_AGENT_POLICY_KEYS
        or not isinstance(decision, dict)
        or decision.get("action") != "approve"
        or _nonempty_text(decision.get("idempotency_key")) is None
    ):
        return False
    for field_name in (
        "quantity",
        "idempotency_key",
        "sizing_price",
        "approved_notional",
        "confidence",
    ):
        if quantity_policy.get(field_name) != decision.get(field_name):
            return False
    target_price = _positive_decimal(raw_policy.get("target_price"))
    stop_price = _positive_decimal(
        raw_policy.get("stop_price") or raw_policy.get("stop_loss_price")
    )
    return bool(
        _positive_decimal(quantity_policy.get("sizing_price")) is not None
        and _positive_decimal(quantity_policy.get("approved_notional")) is not None
        and target_price is not None
        and stop_price is not None
        and _positive_decimal(decision.get("target_price")) == target_price
        and _positive_decimal(decision.get("stop_price")) == stop_price
        and _positive_decimal(risk_context.get("target_price")) == target_price
        and _positive_decimal(risk_context.get("stop_price")) == stop_price
    )


def _short_term_entry_policy_matches(
    *,
    quantity_policy: dict[str, object],
    decision: object,
) -> bool:
    quantity = quantity_policy.get("quantity")
    if (
        set(quantity_policy) != CORE_V4_SHORT_TERM_ENTRY_POLICY_KEYS
        or isinstance(quantity, bool)
        or not isinstance(quantity, int)
        or quantity <= 0
        or not isinstance(decision, dict)
        or decision.get("agent") != "short_term_exec_agent_v1"
        or decision.get("decision") != "approved"
        or decision.get("approved_quantity") != quantity
    ):
        return False
    reference_price = _positive_decimal(decision.get("reference_price"))
    approved_notional = _positive_decimal(decision.get("approved_notional"))
    return bool(
        reference_price is not None
        and approved_notional is not None
        and approved_notional == reference_price * Decimal(quantity)
    )


def _short_term_exit_policy_matches(
    *,
    raw_policy: dict[str, object],
    quantity_policy: dict[str, object],
    risk_context: dict[str, object],
    item: ExecutionPlanItem,
    linked_entry: ExecutionPlanItem | None,
) -> bool:
    linked_key = raw_policy.get("linked_entry_item_key")
    quantity = quantity_policy.get("quantity")
    entry_context = risk_context.get("entry_context")
    if (
        quantity_policy.get("source") != "short_term_exec_agent_v1"
        or set(quantity_policy) != CORE_V4_SHORT_TERM_EXIT_POLICY_KEYS
        or quantity_policy.get("linked_entry_item_key") != linked_key
        or not isinstance(entry_context, dict)
        or set(entry_context)
        != {
            "source_plan_item_id",
            "decision_run_id",
            "decision_snapshot_id",
            "entry_quantity",
            "linked_by",
        }
        or entry_context.get("source_plan_item_id") != linked_key
        or entry_context.get("decision_run_id") != item.decision_run_id
        or entry_context.get("decision_snapshot_id")
        != item.decision_snapshot_id
        or entry_context.get("entry_quantity") != quantity
        or entry_context.get("linked_by") != "short_term_exec_agent_v1"
        or linked_entry is None
    ):
        return False
    linked_policy = (
        linked_entry.quantity_policy
        if isinstance(linked_entry.quantity_policy, dict)
        else {}
    )
    linked_risk = (
        linked_entry.risk_context
        if isinstance(linked_entry.risk_context, dict)
        else {}
    )
    linked_decision = linked_risk.get("exec_agent_decision")
    return bool(
        linked_entry.side == "buy"
        and linked_entry.symbol == item.symbol
        and linked_entry.decision_run_id == item.decision_run_id
        and linked_entry.decision_snapshot_id == item.decision_snapshot_id
        and linked_policy.get("source") == "short_term_exec_agent_v1"
        and linked_policy.get("quantity") == quantity
        and _short_term_entry_policy_matches(
            quantity_policy=linked_policy,
            decision=linked_decision,
        )
    )


def _watch_sizing_policy_matches(
    *,
    raw_policy: dict[str, object],
    quantity_policy: dict[str, object],
    item: ExecutionPlanItem,
) -> bool:
    if set(quantity_policy) != CORE_V4_WATCH_POLICY_KEYS:
        return False
    quantity = quantity_policy.get("quantity")
    decision = quantity_policy.get("exec_agent_decision")
    mirrored_decision = quantity_policy.get("short_term_exec_agent_decision")
    if (
        not isinstance(quantity, int)
        or isinstance(quantity, bool)
        or quantity <= 0
        or not isinstance(decision, dict)
        or decision != mirrored_decision
        or quantity_policy.get("previous_policy") != raw_policy
        or decision.get("source") != "openclaw_exec_agent_watch_sizing"
        or decision.get("action") != "approve"
        or decision.get("decision") != "approved"
        or decision.get("execution_plan_item_id") != item.id
        or decision.get("symbol") != item.symbol
        or decision.get("quantity") != quantity
        or decision.get("approved_quantity") != quantity
    ):
        return False
    for policy_field, decision_field in (
        ("sizing_price", "sizing_price"),
        ("approved_notional", "approved_notional_krw"),
        ("confidence", "confidence"),
        ("target_profile", "target_profile"),
        ("target_tag", "target_tag"),
        ("reason_code", "reason_code"),
    ):
        if quantity_policy.get(policy_field) != decision.get(decision_field):
            return False
    sizing_price = _positive_decimal(quantity_policy.get("sizing_price"))
    approved_notional = _positive_decimal(
        quantity_policy.get("approved_notional")
    )
    if (
        sizing_price is None
        or approved_notional is None
        or approved_notional != sizing_price * Decimal(quantity)
        or item.limit_price != sizing_price
    ):
        return False

    provenance = quantity_policy.get("approval_provenance")
    runtime_provenance = decision.get("runtime_provenance")
    if (
        not isinstance(provenance, dict)
        or provenance.get("kind")
        not in {
            "verified_exec_agent_watch_sizing",
            "reported_unverified_exec_agent_watch_sizing",
        }
        or provenance.get("runtime") != runtime_provenance
    ):
        return False
    audit = quantity_policy.get("exec_agent_decision_audit")
    if audit != {
        "event_type": "watch_exec_agent_sizing_decision",
        "storage": "execution_plan_item_audits",
    }:
        return False
    adjustment = quantity_policy.get("sizing_adjustment")
    explanation = quantity_policy.get("sizing_explanation")
    if (
        not isinstance(adjustment, dict)
        or adjustment.get("final_quantity") != quantity
        or adjustment.get("final_notional")
        != quantity_policy.get("approved_notional")
        or adjustment.get("decision") != "submit"
        or not isinstance(explanation, dict)
        or explanation.get("source") != "exec_agent_watch_sizing"
        or explanation.get("final_quantity") != quantity
        or explanation.get("final_notional")
        != quantity_policy.get("approved_notional")
        or explanation.get("sizing_price")
        != quantity_policy.get("sizing_price")
        or explanation.get("confidence") != quantity_policy.get("confidence")
        or explanation.get("target_profile")
        != quantity_policy.get("target_profile")
        or explanation.get("target_tag") != quantity_policy.get("target_tag")
    ):
        return False
    envelope = quantity_policy.get("allowed_action_envelope")
    return bool(
        isinstance(envelope, dict)
        and quantity_policy.get("market_regime_guardrail")
        == envelope.get("market_regime_guardrail")
    )


def _positive_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _same_instant(left: datetime | None, right: datetime | None) -> bool:
    if not isinstance(left, datetime) or not isinstance(right, datetime):
        return False

    def normalize(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    return normalize(left) == normalize(right)


def revoke_core_execution_plan_v4(
    *,
    settings: Settings,
    db: Session,
    command: CoreExecutionRevocationRequestV4,
) -> CreateExecutionPlanResult:
    plan = db.scalar(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.external_plan_id == command.plan_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
        .with_for_update()
        .options(selectinload(ExecutionPlan.items))
    )
    if plan is None:
        return _revocation_failure(
            command=command,
            status_code=404,
            status="not_found",
            reason_code="execution_plan_not_found",
        )
    if persisted_core_plan_contract_ack(plan) is None:
        return _revocation_failure(
            command=command,
            status_code=409,
            status="manual_review_required",
            reason_code="persisted_v4_contract_not_proven",
        )

    requested_by_key = {item.core_item_key: item for item in command.items}
    requested_ids = {item.execution_plan_item_id for item in command.items}
    locked_items = db.scalars(
        select(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.execution_plan_id == plan.id,
            ExecutionPlanItem.id.in_(requested_ids),
        )
        .with_for_update()
        .order_by(ExecutionPlanItem.plan_item_index.asc())
    ).all()
    locked_by_key = {
        str(item.source_plan_item_id): item
        for item in locked_items
        if item.source_plan_item_id is not None
    }
    exact_bindings = bool(
        len(locked_items) == len(command.items)
        and set(locked_by_key) == set(requested_by_key)
        and all(
            locked_by_key[key].id == request.execution_plan_item_id
            and locked_by_key[key].decision_snapshot_id == request.decision_snapshot_id
            for key, request in requested_by_key.items()
        )
    )
    if not exact_bindings or not _request_covers_linked_family(
        plan_items=plan.items,
        requested_keys=set(requested_by_key),
    ):
        return _revocation_failure(
            command=command,
            status_code=409,
            status="manual_review_required",
            reason_code="revocation_item_binding_mismatch",
        )

    item_ids = {item.id for item in locked_items}
    jobs = db.scalars(
        select(ExecutionJob)
        .where(ExecutionJob.execution_plan_item_id.in_(item_ids))
        .with_for_update()
    ).all()
    broker_orders = db.scalars(
        select(BrokerOrder)
        .where(BrokerOrder.execution_plan_item_id.in_(item_ids))
        .with_for_update()
    ).all()
    broker_order_ids = {order.id for order in broker_orders}
    fills = (
        db.scalars(
            select(ExecutionFill)
            .where(ExecutionFill.broker_order_pk.in_(broker_order_ids))
            .with_for_update()
        ).all()
        if broker_order_ids
        else []
    )
    possible_exposure_orders = [
        order
        for order in broker_orders
        if not (
            str(order.status or "").strip().lower()
            in SAFE_LOCAL_PRE_SUBMISSION_ORDER_STATUSES
            and order.broker_order_id is None
            and order.submitted_at is None
        )
    ]
    if fills or possible_exposure_orders:
        buy_item_ids = {
            item.id
            for item in locked_items
            if item.side == "buy" and item.intent in ENTRY_INTENTS
        }
        for item in locked_items:
            if item.id in buy_item_ids:
                item.status = "manual_review_required"
        for job in jobs:
            if job.execution_plan_item_id in buy_item_ids:
                job.status = "revoked"
                job.locked_at = None
                job.last_error = "Core decision changed after broker exposure became possible"
        plan.status = "manual_review_required"
        _audit_revocation_manual_review(
            db=db,
            items=locked_items,
            command=command,
            reason_code="broker_commit_or_exposure_possible",
        )
        db.commit()
        item_acks = [
            {
                "core_item_key": request.core_item_key,
                "status": (
                    "manual_review_required"
                    if locked_by_key[request.core_item_key].id in buy_item_ids
                    else "preserved_protective_exit"
                ),
                "execution_plan_id": plan.id,
                "execution_plan_item_id": locked_by_key[request.core_item_key].id,
            }
            for request in command.items
        ]
        return CreateExecutionPlanResult(
            status_code=409,
            payload={
                "plan_id": plan.external_plan_id,
                "execution_plan_id": plan.id,
                "contract_ack": _revocation_contract_ack(
                    plan_id=command.plan_id,
                    status="manual_review_required",
                ),
                "item_acks": item_acks,
                "revoked_item_count": 0,
                "manual_review_item_count": len(buy_item_ids),
                "preserved_item_count": len(item_acks) - len(buy_item_ids),
                "reason_code": "broker_commit_or_exposure_possible",
            },
        )

    already_revoked = all(item.status == "revoked" for item in locked_items)
    if not already_revoked:
        for item in locked_items:
            prior_status = item.status
            item.status = "revoked"
            db.add(
                ExecutionPlanItemAudit(
                    id=new_id(),
                    execution_plan_item_id=item.id,
                    event_type="core_decision_revoked",
                    source="kai-trading-core",
                    summary={
                        "reason_code": command.reason_code,
                        "prior_status": prior_status,
                        "replacement_decision_snapshot_id": command.replacement_decision_snapshot_id,
                        "replacement_action": command.replacement_action,
                    },
                    payload={
                        "contract_version": command.contract_version,
                        "schema_version": command.schema_version,
                        "required_capability": command.required_capability,
                        "core_item_key": item.source_plan_item_id,
                    },
                )
            )
        for job in jobs:
            job.status = "revoked"
            job.locked_at = None
            job.last_error = "core decision revoked before broker submission"
        if all(item.status == "revoked" for item in plan.items):
            plan.status = "revoked"
        db.commit()

    return CreateExecutionPlanResult(
        status_code=200,
        payload={
            "plan_id": plan.external_plan_id,
            "execution_plan_id": plan.id,
            "contract_ack": _revocation_contract_ack(
                plan_id=command.plan_id,
                status="revoked",
            ),
            "item_acks": _revocation_item_acks(
                command=command,
                execution_plan_id=plan.id,
                persisted_items=locked_by_key,
                status="revoked",
            ),
            "reason_code": command.reason_code,
            "revoked_item_count": len(command.items),
            "manual_review_item_count": 0,
            "preserved_item_count": 0,
            "idempotent_replay": already_revoked,
        },
    )


def _request_covers_linked_family(
    *,
    plan_items: Sequence[ExecutionPlanItem],
    requested_keys: set[str],
) -> bool:
    requested_entries = {
        str(item.source_plan_item_id)
        for item in plan_items
        if item.source_plan_item_id in requested_keys
        and item.side == "buy"
        and item.intent in ENTRY_INTENTS
    }
    if not requested_entries:
        return False
    linked_keys = set(requested_entries)
    for item in plan_items:
        quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
        risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
        linked_entry_key = _nonempty_text(
            quantity_policy.get("linked_entry_item_key")
            or risk_context.get("linked_entry_item_key")
        )
        if linked_entry_key in requested_entries and item.source_plan_item_id is not None:
            linked_keys.add(str(item.source_plan_item_id))
    return linked_keys == requested_keys


def _audit_revocation_manual_review(
    *,
    db: Session,
    items: Sequence[ExecutionPlanItem],
    command: CoreExecutionRevocationRequestV4,
    reason_code: str,
) -> None:
    for item in items:
        db.add(
            ExecutionPlanItemAudit(
                id=new_id(),
                execution_plan_item_id=item.id,
                event_type="core_revocation_manual_review",
                source="kai-trading-core",
                summary={
                    "reason_code": reason_code,
                    "current_status": item.status,
                    "replacement_decision_snapshot_id": command.replacement_decision_snapshot_id,
                    "replacement_action": command.replacement_action,
                },
                payload={
                    "contract_version": command.contract_version,
                    "schema_version": command.schema_version,
                    "core_item_key": item.source_plan_item_id,
                },
            )
        )


def _revocation_failure(
    *,
    command: CoreExecutionRevocationRequestV4,
    status_code: int,
    status: str,
    reason_code: str,
    execution_plan_id: str | None = None,
    persisted_items: dict[str, ExecutionPlanItem] | None = None,
) -> CreateExecutionPlanResult:
    return CreateExecutionPlanResult(
        status_code=status_code,
        payload={
            "plan_id": command.plan_id,
            "execution_plan_id": execution_plan_id,
            "contract_ack": _revocation_contract_ack(
                plan_id=command.plan_id,
                status=status,
            ),
            "item_acks": _revocation_item_acks(
                command=command,
                execution_plan_id=execution_plan_id,
                persisted_items=persisted_items or {},
                status=status,
            ),
            "reason_code": reason_code,
            "revoked_item_count": 0,
            "manual_review_item_count": (
                len(command.items) if status == "manual_review_required" else 0
            ),
            "preserved_item_count": 0,
        },
    )


def _revocation_contract_ack(*, plan_id: str, status: str) -> dict[str, object]:
    return {
        "schema_version": CORE_EXECUTION_REVOCATION_SCHEMA_VERSION,
        "contract_version": CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
        "capability": CORE_EXECUTION_REVOCATION_CAPABILITY,
        "plan_id": plan_id,
        "status": status,
    }


def _revocation_item_acks(
    *,
    command: CoreExecutionRevocationRequestV4,
    execution_plan_id: str | None,
    persisted_items: dict[str, ExecutionPlanItem],
    status: str,
) -> list[dict[str, object]]:
    return [
        {
            "core_item_key": request.core_item_key,
            "status": status,
            "execution_plan_id": execution_plan_id,
            "execution_plan_item_id": (
                persisted_items[request.core_item_key].id
                if request.core_item_key in persisted_items
                else request.execution_plan_item_id
            ),
        }
        for request in command.items
    ]


def _nonempty_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
