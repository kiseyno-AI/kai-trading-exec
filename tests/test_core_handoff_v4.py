from collections.abc import Iterator
from copy import deepcopy
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import threading
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import (
    CoreExecutionPlanRequestV4,
    _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY,
    core_execution_plan_material_fingerprint,
)
from app.broker.base import BrokerAccountSnapshot
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services import core_handoff_v4
from app.services import core_revocation_exposure
from app.services import execution_exposure
from app.services import position_truth_remediation
from app.services import watch_exec_agent_sizing as watch_sizing
from app.services.kis_krx_flat_rebind_contract import (
    KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION,
    kis_krx_flat_absence_proof_digest,
    kis_krx_flat_configured_account_binding_digest,
    kis_krx_flat_evidence_fingerprint,
)
from app.services.position_identity import (
    build_position_identity,
    payload_with_position_identity,
)


def test_watch_rejection_policy_is_valid_terminal_core_v4_transition() -> None:
    raw_policy = {"type": "exec_agent_sized", "budget_krw": "1000000"}
    item = ExecutionPlanItem(id="item-1", symbol="005930")
    decision = {
        "source": "openclaw_exec_agent_watch_sizing",
        "action": "reject",
        "decision": "rejected",
        "execution_plan_item_id": item.id,
        "quantity": 0,
        "approved_quantity": 0,
    }
    policy = {
        "kind": "exec_agent_rejected",
        "source": "exec_agent_watch_sizing",
        "reason": "hard gate",
        "reason_code": "hard_gate",
        "exec_agent_decision": decision,
        "short_term_exec_agent_decision": decision,
        "exec_agent_decision_audit": {"event_type": "watch_exec_agent_sizing_decision"},
        "previous_policy": raw_policy,
    }

    assert core_handoff_v4._watch_rejection_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=policy,
        item=item,
    )


def test_watch_rejection_policy_accepts_the_durable_attempt_wrapper() -> None:
    raw_policy = {"type": "exec_agent_sized", "budget_krw": "1000000"}
    item = ExecutionPlanItem(id="item-attempted-reject", symbol="005930")
    decision = {
        "source": "openclaw_exec_agent_watch_sizing",
        "action": "reject",
        "decision": "rejected",
        "execution_plan_item_id": item.id,
        "quantity": 0,
        "approved_quantity": 0,
        "reason_code": "unverified_exec_agent_runtime",
        "reason_family": "runtime_contract",
        "rejection_disposition": "terminal",
        "reason": "approval lacks exact exec-agent runtime provenance",
    }
    attempted_policy = {
        **raw_policy,
        "exec_agent_watch_sizing_attempt_count": 1,
        "exec_agent_watch_sizing_max_attempts": 3,
        "exec_agent_watch_sizing_fallback_allowed": False,
        "exec_agent_watch_sizing_attempts": [
            {
                "attempt": 1,
                "max_attempts": 3,
                "success": True,
                "recorded_at": "2026-10-08T00:00:00+00:00",
            }
        ],
    }
    policy = {
        "kind": "exec_agent_rejected",
        "source": "exec_agent_watch_sizing",
        "reason": decision["reason"],
        "reason_code": decision["reason_code"],
        "reason_family": decision["reason_family"],
        "rejection_disposition": decision["rejection_disposition"],
        "exec_agent_decision": decision,
        "short_term_exec_agent_decision": decision,
        "exec_agent_decision_audit": {
            "event_type": "watch_exec_agent_sizing_decision",
            "storage": "execution_plan_item_audits",
        },
        "previous_policy": attempted_policy,
    }

    assert core_handoff_v4._watch_rejection_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=policy,
        item=item,
    )


@pytest.mark.parametrize(
    "decision_source",
    ["openclaw_exec_agent_watch_sizing", "watch_exec_agent_sizing"],
)
def test_watch_rejection_policy_accepts_only_canonical_zero_authority(
    decision_source: str,
) -> None:
    raw_policy = {"type": "exec_agent_sized", "budget_krw": "1000000"}
    item = ExecutionPlanItem(id="item-canonical-reject", symbol="005930")
    decision = {
        "source": decision_source,
        "action": "reject",
        "decision": "rejected",
        "execution_plan_item_id": item.id,
        "quantity": 0,
        "approved_quantity": 0,
        "approved_notional_krw": None,
        "sizing_price": None,
        "reason_code": "deterministic_block",
        "reason_family": "action_envelope",
        "rejection_disposition": "terminal",
        "reason": "deterministic rejection",
    }
    policy = {
        "kind": "exec_agent_rejected",
        "source": "exec_agent_watch_sizing",
        "reason": decision["reason"],
        "reason_code": decision["reason_code"],
        "reason_family": decision["reason_family"],
        "rejection_disposition": decision["rejection_disposition"],
        "exec_agent_decision": decision,
        "short_term_exec_agent_decision": decision,
        "exec_agent_decision_audit": {
            "event_type": "watch_exec_agent_sizing_decision",
            "storage": "execution_plan_item_audits",
        },
        "previous_policy": raw_policy,
    }

    assert core_handoff_v4._watch_rejection_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=policy,
        item=item,
    )

    forged = deepcopy(policy)
    forged_decision = dict(decision)
    forged_decision["approved_notional_krw"] = "71500.0000"
    forged["exec_agent_decision"] = forged_decision
    forged["short_term_exec_agent_decision"] = forged_decision
    assert not core_handoff_v4._watch_rejection_policy_matches(
        raw_policy=raw_policy,
        quantity_policy=forged,
        item=item,
    )


@pytest.mark.parametrize(
    "status",
    [
        "agent_rejected",
        "crypto_paper_failed",
        "crypto_paper_rejected",
        "manual_review_resolved_no_exposure",
        "not_applicable",
    ],
)
def test_v4_item_state_marks_no_execution_terminal_statuses_terminal(
    status: str,
) -> None:
    state = core_handoff_v4._core_v4_item_state(
        ExecutionPlanItem(
            id=f"item-{status}",
            source_plan_item_id=f"core:{status}",
            status=status,
        )
    )

    assert state["status"] == status
    assert state["terminal"] is True


@pytest.mark.parametrize(
    ("status", "risk_context", "expected"),
    [
        (
            "agent_rejected",
            {
                "last_trigger_time_exec_agent_interaction": {
                    "decision": {
                        "decision": "reject",
                        "reason": "liquidity disappeared",
                    }
                }
            },
            {
                "action": "reject",
                "decision": "rejected",
                "reason_family": "trigger_time_exec_agent",
                "rejection_disposition": "terminal",
                "reason": "liquidity disappeared",
            },
        ),
        (
            "exec_agent_rejected",
            {
                "exec_agent_decision": {
                    "agent": "short_term_exec_agent_v1",
                    "decision": "rejected",
                    "reason": "existing_position",
                }
            },
            {
                "action": "reject",
                "decision": "rejected",
                "reason_code": "existing_position",
                "reason_family": "short_term_exec_agent",
                "rejection_disposition": "terminal",
                "reason": "existing_position",
            },
        ),
    ],
)
def test_v4_item_state_compacts_terminal_reasons_from_durable_risk_context(
    status: str,
    risk_context: dict[str, object],
    expected: dict[str, object],
) -> None:
    state = core_handoff_v4._core_v4_item_state(
        ExecutionPlanItem(
            id=f"item-{status}",
            source_plan_item_id=f"core:{status}",
            status=status,
            quantity_policy={"type": "exec_agent_sized"},
            risk_context=risk_context,
        )
    )

    assert state["terminal"] is True
    assert state["decision"] == expected


@pytest.mark.parametrize(
    "drift",
    ["wrong_previous_policy", "mismatched_mirror", "top_level_quantity"],
)
def test_legacy_terminal_watch_migration_rejects_ambiguous_shapes(
    drift: str,
) -> None:
    raw_policy = {"type": "exec_agent_sized", "budget_krw": "1000000"}
    item = ExecutionPlanItem(
        id="legacy-terminal-item",
        symbol="005930",
        status="exec_agent_rejected",
    )
    decision = {
        "source": "openclaw_exec_agent_watch_sizing",
        "action": "reject",
        "decision": "rejected",
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "quantity": 3,
        "approved_quantity": 3,
        "reason_code": "allowed_action_envelope_blocked",
        "reason": "legacy terminal decision",
    }
    policy = {
        "kind": "exec_agent_rejected",
        "source": "exec_agent_watch_sizing",
        "reason_code": decision["reason_code"],
        "reason": decision["reason"],
        "exec_agent_decision": decision,
        "short_term_exec_agent_decision": decision,
        "previous_policy": raw_policy,
    }
    if drift == "wrong_previous_policy":
        policy["previous_policy"] = {"type": "shares", "quantity": 3}
    elif drift == "mismatched_mirror":
        policy["short_term_exec_agent_decision"] = {
            **decision,
            "execution_plan_item_id": "different-item",
        }
    else:
        policy["quantity"] = 3

    assert (
        core_handoff_v4._legacy_watch_authority_normalization(
            item=item,
            raw_policy=raw_policy,
            quantity_policy=policy,
        )
        is None
    )


@pytest.mark.parametrize(
    "status",
    [
        "broker_order_failed",
        "broker_order_rejected",
        "next_open_buy_failed",
    ],
)
def test_v4_terminal_failure_never_exposes_stale_sizing_approval(
    status: str,
) -> None:
    state = core_handoff_v4._core_v4_item_state(
        ExecutionPlanItem(
            id=f"item-{status}",
            source_plan_item_id=f"core:{status}",
            status=status,
            quantity_policy={
                "kind": "shares",
                "exec_agent_decision": {
                    "action": "approve",
                    "decision": "approved",
                    "reason_code": "approved",
                    "reason": "sizing was approved before broker submission",
                },
            },
        )
    )

    assert state["terminal"] is True
    assert state["decision"] == {
        "action": "reject",
        "decision": "rejected",
        "reason_code": status,
        "reason_family": "execution_status",
        "rejection_disposition": "terminal",
        "reason": f"current execution status is {status}",
    }


def test_v4_terminal_failure_never_exposes_stale_defer() -> None:
    state = core_handoff_v4._core_v4_item_state(
        ExecutionPlanItem(
            id="item-terminal-after-defer",
            source_plan_item_id="core:terminal-after-defer",
            status="exec_agent_rejected",
            quantity_policy={"type": "exec_agent_sized"},
            risk_context={
                "watch_exec_agent_deferred": {
                    "disposition": "defer",
                    "reason_code": "reward_risk_hard_block",
                    "reason_family": "reward_risk",
                    "reason": "retry later",
                    "retry_after": "2026-10-07T15:00:00+00:00",
                }
            },
        )
    )

    assert state["terminal"] is True
    assert state["decision"] == {
        "action": "reject",
        "decision": "rejected",
        "reason_code": "exec_agent_rejected",
        "reason_family": "execution_status",
        "rejection_disposition": "terminal",
        "reason": "current execution status is exec_agent_rejected",
    }
from app.services.execution_jobs import _claim_job, _lock_job_for_broker_submission
from app.services.watch_exec_agent_sizing import _apply_decisions
from openclaw_contract_test_support import verified_runtime_provenance


CONTRACT_VERSION = "execution_handoff_contract_v4"
PLAN_SCHEMA_VERSION = "signal_decider_exec_plan_v4"
PLAN_CAPABILITY = "delegated_final_full_cost_gate_recheck_v1"
PLAN_ACK_SCHEMA_VERSION = "execution_handoff_ack_v4"
REVOCATION_SCHEMA_VERSION = "execution_handoff_revocation_v4"
REVOCATION_CAPABILITY = "atomic_pre_submission_revocation_v1"
ACTIVATION_SCHEMA_VERSION = "execution_handoff_activation_v1"
ACTIVATION_CAPABILITY = "core_confirmed_activation_v1"
ACTIVATION_BINDING_SCHEMA_VERSION = "core_execution_activation_binding_v1"
AUTHORITY_GENERATION_FINGERPRINT = "a" * 64


class _KisFlatSnapshotAdapter:
    provider = "kis"

    def __init__(self, snapshot: BrokerAccountSnapshot) -> None:
        self.snapshot = snapshot
        self.snapshot_requests: list[tuple[str, list[str] | None, str]] = []

    def authenticate(self):
        raise AssertionError("flat rebind must use the shared KIS token path")

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        self.snapshot_requests.append((access_token, symbols, exchange))
        return self.snapshot


def _complete_flat_kis_snapshot(*, observed_at: datetime) -> BrokerAccountSnapshot:
    empty_parse = {
        "raw_row_count": 0,
        "parsed_row_count": 0,
        "omitted_row_count": 0,
        "invalid_row_count": 0,
        "duplicate_identity_count": 0,
    }
    return BrokerAccountSnapshot(
        observed_at=observed_at,
        source="kis:account_snapshot",
        holdings=[],
        open_orders=[],
        holdings_complete=True,
        open_orders_complete=True,
        diagnostics={
            "requested_exchange": "KRX",
            "holdings_pagination": {"complete": True},
            "open_orders_pagination": {"complete": True},
            "holdings_parse": dict(empty_parse),
            "open_orders_parse": dict(empty_parse),
        },
        payload={
            "exchange": "KRX",
            "account_number_masked": "****5678",
            "holdings_coverage": "complete",
            "open_orders_coverage": "complete",
            "open_orders_error": None,
        },
    )


def _refresh_material_fingerprint(payload: dict[str, object]) -> None:
    payload["metadata"]["dispatch_material_fingerprint"] = (
        core_execution_plan_material_fingerprint(payload)
    )


def _build_test_app(
    *,
    database_url: str = "sqlite:///:memory:",
    settings: Settings | None = None,
):
    settings = settings or Settings(database_url=database_url, runtime_mode="mock")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
    )
    return app, engine


def _item(
    *,
    core_item_key: str,
    side: str,
    intent: str,
    linked_entry_item_key: str | None = None,
) -> dict[str, object]:
    quantity_policy: dict[str, object] = {"type": "exec_agent_sized"}
    if linked_entry_item_key is not None:
        quantity_policy = {
            "type": "position_exit_from_entry",
            "linked_entry_item_key": linked_entry_item_key,
        }
    return {
        "core_item_key": core_item_key,
        "source_plan_item_id": core_item_key,
        "agent_name": "signal-decider",
        "decision_agent_name": "signal-decider",
        "symbol": "005930",
        "asset_class": "stock",
        "market": "KRX",
        "exchange": "KRX",
        "currency": "KRW",
        "broker": "kis",
        "provider": "kis",
        "venue": "kis",
        "session_policy": {
            "session": "regular",
            "timezone": "Asia/Seoul",
            "allow_extended_hours": False,
        },
        "side": side,
        "intent": intent,
        "quantity_policy": quantity_policy,
        "limit_price": None,
        "trigger_price": 72000 if side == "sell" else None,
        "time_in_force": "day",
        "risk_context": {
            "account_reference": "KIS:00000000-00",
            "delegated_gate_codes": [],
            "delegated_gate_confirmation_required": False,
            "delegated_gates": [],
        },
        "decision_snapshot_id": "101",
    }


def _plan_payload(*, plan_id: str = "core-v4-plan") -> dict[str, object]:
    entry_key = "core-plan-1:next_open_buy"
    payload = {
        "plan_id": plan_id,
        "source": "kai-trading-core",
        "mode": "mock",
        "market_date": "2026-09-10",
        "strategy_run_id": "signal-decider:2026-09-10:test",
        "requested_at": "2026-09-09T12:00:00Z",
        "items": [
            _item(core_item_key=entry_key, side="buy", intent="next_open_buy"),
            _item(
                core_item_key="core-plan-1:target_sell",
                side="sell",
                intent="target_sell",
                linked_entry_item_key=entry_key,
            ),
            _item(
                core_item_key="core-plan-1:stop_sell",
                side="sell",
                intent="stop_sell",
                linked_entry_item_key=entry_key,
            ),
        ],
        "metadata": {
            "schema_version": PLAN_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "required_capability": PLAN_CAPABILITY,
            "agent_name": "signal-decider",
            "decision_agent_name": "signal-decider",
            "trigger": "test",
            "requested_at": "2026-09-09T12:00:00Z",
            "account_reference": "KIS:00000000-00",
            "account_label": "TEST",
            "source_market_date": "2026-09-09",
            "target_trade_date": "2026-09-10",
            "dispatch_batch_type": "initial",
            "dispatch_batch_key": None,
        },
    }
    _refresh_material_fingerprint(payload)
    return payload


def _multi_family_plan_payload(
    *,
    plan_id: str,
    symbols: tuple[str, ...] = ("005930", "000660", "035420"),
) -> dict[str, object]:
    payload = _plan_payload(plan_id=plan_id)
    template_items = deepcopy(payload["items"])
    payload["items"] = []
    for family_index, symbol in enumerate(symbols, start=1):
        family_prefix = f"core-plan-{family_index}"
        for template in template_items:
            item = deepcopy(template)
            item["core_item_key"] = str(item["core_item_key"]).replace(
                "core-plan-1", family_prefix
            )
            item["source_plan_item_id"] = item["core_item_key"]
            item["symbol"] = symbol
            item["decision_snapshot_id"] = str(100 + family_index)
            linked_key = item["quantity_policy"].get("linked_entry_item_key")
            if isinstance(linked_key, str):
                item["quantity_policy"]["linked_entry_item_key"] = (
                    linked_key.replace("core-plan-1", family_prefix)
                )
            payload["items"].append(item)
    _refresh_material_fingerprint(payload)
    return payload


def _activation_bound_plan_payload(
    *,
    plan_id: str = "core-v4-activation-bound-plan",
    authority_generation_fingerprint: str = AUTHORITY_GENERATION_FINGERPRINT,
) -> dict[str, object]:
    payload = _plan_payload(plan_id=plan_id)
    for item in payload["items"]:
        item["risk_context"]["_core_activation"] = {
            "schema_version": ACTIVATION_BINDING_SCHEMA_VERSION,
            "mode": "core_confirmed",
            "authority_generation_fingerprint": (
                authority_generation_fingerprint
            ),
        }
    _refresh_material_fingerprint(payload)
    return payload


def _revocation_payload(
    create_payload: dict[str, object],
    item_acks: list[dict[str, object]],
    *,
    reason_code: str = "latest_snapshot_not_actionable",
) -> dict[str, object]:
    return {
        "schema_version": REVOCATION_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "required_capability": REVOCATION_CAPABILITY,
        "plan_id": create_payload["plan_id"],
        "requested_at": "2026-09-09T12:05:00Z",
        "reason_code": reason_code,
        "replacement_decision_snapshot_id": "102",
        "replacement_action": "wait",
        "items": [
            {
                "core_item_key": ack["core_item_key"],
                "execution_plan_item_id": ack["execution_plan_item_id"],
                "decision_snapshot_id": "101",
            }
            for ack in item_acks
        ],
    }


def _activation_payload(
    create_payload: dict[str, object],
    create_response: dict[str, object],
    *,
    authority_generation_fingerprint: str = AUTHORITY_GENERATION_FINGERPRINT,
) -> dict[str, object]:
    item_acks = create_response["item_acks"]
    decision_snapshot_by_key = {
        item["core_item_key"]: item["decision_snapshot_id"]
        for item in create_payload["items"]
    }
    return {
        "schema_version": ACTIVATION_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "required_capability": ACTIVATION_CAPABILITY,
        "plan_id": create_payload["plan_id"],
        "execution_plan_id": create_response["execution_plan_id"],
        "dispatch_material_fingerprint": create_payload["metadata"][
            "dispatch_material_fingerprint"
        ],
        "authority_generation_fingerprint": (
            authority_generation_fingerprint
        ),
        "requested_at": "2026-09-09T12:05:00Z",
        "items": [
            {
                "core_item_key": ack["core_item_key"],
                "execution_plan_item_id": ack["execution_plan_item_id"],
                "decision_snapshot_id": decision_snapshot_by_key[
                    ack["core_item_key"]
                ],
            }
            for ack in item_acks
        ],
    }


def _exec_agent_approval(
    *,
    quantity: int = 2,
    idempotency_key: str = "exec-agent-approval-key",
) -> tuple[dict[str, object], dict[str, object]]:
    decision = {
        "action": "approve",
        "quantity": quantity,
        "idempotency_key": idempotency_key,
        "sizing_price": "71000",
        "approved_notional": "142000",
        "confidence": "0.82",
        "target_price": "72000",
        "stop_price": "72000",
    }
    policy = {
        "kind": "shares",
        "quantity": quantity,
        "source": "exec_agent",
        "idempotency_key": idempotency_key,
        "sizing_price": "71000",
        "approved_notional": "142000",
        "confidence": "0.82",
    }
    return decision, policy


def _short_term_exec_agent_approval(
    *,
    quantity: int = 2,
) -> tuple[dict[str, object], dict[str, object]]:
    decision = {
        "agent": "short_term_exec_agent_v1",
        "decision": "approved",
        "reason": "approved_by_deterministic_rails",
        "approved_quantity": quantity,
        "reference_price": "71000",
        "approved_notional": "142000",
        "candidate_max_budget": "200000",
        "budget_multiplier": "1.00",
        "confidence": "0.82",
        "conviction": "high",
        "source_key": "candidate-1",
    }
    policy = {
        "kind": "shares",
        "quantity": quantity,
        "source": "short_term_exec_agent_v1",
    }
    return decision, policy


def test_v4_intake_requires_exact_contract_and_returns_one_to_one_durable_ack() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload()

    old_endpoint = client.post("/execution-plans", json=payload)
    assert old_endpoint.status_code in {422, 426}
    assert client.get("/execution-plans").json() == []

    v4_payload = _plan_payload(plan_id="core-v4-strict-plan")
    response = client.post("/v4/execution-plans", json=v4_payload)

    assert response.status_code == 201
    body = response.json()
    assert body["plan_id"] == "core-v4-strict-plan"
    assert body["execution_plan_id"]
    assert body["accepted_item_count"] == 3
    assert body["rejected_item_count"] == 0
    assert body["contract_ack"] == {
        "schema_version": PLAN_ACK_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "capability": PLAN_CAPABILITY,
        "plan_id": "core-v4-strict-plan",
        "status": "accepted",
        "dispatch_material_fingerprint": v4_payload["metadata"][
            "dispatch_material_fingerprint"
        ],
    }
    assert [item["core_item_key"] for item in body["item_acks"]] == [
        item["core_item_key"] for item in v4_payload["items"]
    ]
    assert all(item["status"] == "accepted" for item in body["item_acks"])
    assert all(item["execution_plan_id"] for item in body["item_acks"])
    assert all(item["execution_plan_item_id"] for item in body["item_acks"])
    assert len({item["execution_plan_item_id"] for item in body["item_acks"]}) == 3

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "accepted"
        assert all(
            item.status != "core_activation_pending" for item in plan.items
        )

    readback = client.get(
        "/v4/execution-plans",
        params={"external_plan_id": "core-v4-strict-plan"},
    )
    assert readback.status_code == 200
    assert readback.json()[0]["contract_ack"] == body["contract_ack"]
    assert readback.json()[0]["item_acks"] == body["item_acks"]
    assert readback.json()[0]["item_status_counts"] == {
        "stop_sell_pending_config": 1,
        "target_sell_pending_config": 1,
        "watch_sizing_pending": 1,
    }
    assert readback.json()[0]["item_states"] == [
        {
            "core_item_key": item["core_item_key"],
            "execution_plan_item_id": ack["execution_plan_item_id"],
            "status": status,
            "updated_at": readback.json()[0]["item_states"][index]["updated_at"],
            "terminal": False,
            "decision": None,
        }
        for index, (item, ack, status) in enumerate(
            zip(
                v4_payload["items"],
                body["item_acks"],
                [
                    "watch_sizing_pending",
                    "target_sell_pending_config",
                    "stop_sell_pending_config",
                ],
                strict=True,
            )
        )
    ]
    assert all(
        state["updated_at"] is not None
        for state in readback.json()[0]["item_states"]
    )

    duplicate = client.post("/v4/execution-plans", json=v4_payload)
    assert duplicate.status_code == 409
    assert "contract_ack" not in duplicate.json()
    engine.dispose()


def test_v4_rejects_duplicate_delta_batch_under_a_different_plan_id() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    first = _plan_payload(plan_id="core-v4-delta-first")
    first["metadata"].update(
        {
            "dispatch_batch_type": "delta",
            "dispatch_batch_key": "signal-decider:2026-09-10:delta:1",
        }
    )
    _refresh_material_fingerprint(first)
    second = deepcopy(first)
    second["plan_id"] = "core-v4-delta-duplicate"
    second["metadata"]["dispatch_batch_key"] = (
        f"  {first['metadata']['dispatch_batch_key']}  "
    )
    _refresh_material_fingerprint(second)

    first_response = client.post("/v4/execution-plans", json=first)
    duplicate_response = client.post("/v4/execution-plans", json=second)

    assert first_response.status_code == 201, first_response.json()
    assert duplicate_response.status_code == 409
    assert duplicate_response.json()["reason_code"] == "duplicate_delta_batch"
    with app.state.db_session_factory() as session:
        plans = session.scalars(select(ExecutionPlan)).all()
        assert len(plans) == 1
        assert plans[0].dispatch_batch_key == first["metadata"]["dispatch_batch_key"]
    engine.dispose()


def test_v4_duplicate_delta_batch_is_atomic_under_concurrent_requests(
    tmp_path,
    monkeypatch,
) -> None:
    app, engine = _build_test_app(
        database_url=f"sqlite:///{tmp_path / 'delta-batch-race.db'}"
    )
    first = _plan_payload(plan_id="core-v4-delta-race-first")
    first["metadata"].update(
        {
            "dispatch_batch_type": "delta",
            "dispatch_batch_key": "signal-decider:2026-09-10:delta:race",
        }
    )
    _refresh_material_fingerprint(first)
    second = deepcopy(first)
    second["plan_id"] = "core-v4-delta-race-second"
    _refresh_material_fingerprint(second)

    original_lookup = core_handoff_v4._duplicate_delta_batch_plan_id
    first_checks = 0
    checks_lock = threading.Lock()
    both_checked = threading.Barrier(2)

    def synchronized_lookup(*, db, command):
        nonlocal first_checks
        result = original_lookup(db=db, command=command)
        with checks_lock:
            first_checks += 1
            check_number = first_checks
        if check_number <= 2:
            both_checked.wait(timeout=5)
        return result

    monkeypatch.setattr(
        core_handoff_v4,
        "_duplicate_delta_batch_plan_id",
        synchronized_lookup,
    )
    responses: list[tuple[int, dict[str, object]]] = []

    def submit(payload: dict[str, object]) -> None:
        response = TestClient(app).post("/v4/execution-plans", json=payload)
        responses.append((response.status_code, response.json()))

    threads = [
        threading.Thread(target=submit, args=(payload,))
        for payload in (first, second)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(status for status, _ in responses) == [201, 409]
    conflict = next(body for status, body in responses if status == 409)
    assert conflict["reason_code"] == "duplicate_delta_batch"
    with app.state.db_session_factory() as session:
        plans = session.scalars(select(ExecutionPlan)).all()
        assert len(plans) == 1
        assert plans[0].dispatch_batch_key == first["metadata"]["dispatch_batch_key"]
    engine.dispose()


def test_v4_core_confirmed_plan_is_nonclaimable_until_activation() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _activation_bound_plan_payload()

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 201, response.json()
    assert response.json()["contract_ack"]["status"] == "accepted"
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "activation_pending"
        buy = next(item for item in plan.items if item.side == "buy")
        assert buy.status == "core_activation_pending"
        assert buy.risk_context["_core_activation_state"]["prior_status"] == (
            "watch_sizing_pending"
        )
        job = ExecutionJob(
            id="core-activation-pending-job",
            execution_plan_item_id=buy.id,
            job_type="next_open_buy",
            status="scheduled",
            scheduled_for=datetime(2026, 9, 10, 0, 0, tzinfo=UTC),
        )
        session.add(job)
        session.commit()

        assert not _claim_job(
            db=session,
            job=job,
            triggered_at=datetime(2026, 9, 10, 0, 1, tzinfo=UTC),
        )
        session.refresh(job)
        assert job.status == "scheduled"
        assert job.locked_at is None
    engine.dispose()


@pytest.mark.parametrize(
    "mutate_binding",
    [
        lambda payload: payload["items"][0]["risk_context"].pop(
            "_core_activation"
        ),
        lambda payload: payload["items"][0]["risk_context"][
            "_core_activation"
        ].update({"unexpected": True}),
        lambda payload: payload["items"][0]["risk_context"][
            "_core_activation"
        ].update({"authority_generation_fingerprint": "b" * 64}),
    ],
)
def test_v4_activation_binding_must_be_exact_and_plan_wide(
    mutate_binding,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _activation_bound_plan_payload(
        plan_id="core-v4-invalid-activation-binding"
    )
    mutate_binding(payload)
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 422
    with app.state.db_session_factory() as session:
        assert session.scalar(select(ExecutionPlan)) is None
    engine.dispose()


def test_v4_exact_activation_restores_buy_and_replay_is_idempotent() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _activation_bound_plan_payload(
        plan_id="core-v4-exact-activation"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201, created.json()
    activation_payload = _activation_payload(create_payload, created.json())

    activated = client.post(
        "/v4/execution-plans/activations",
        json=activation_payload,
    )

    assert activated.status_code == 200, activated.json()
    assert activated.json() == {
        "plan_id": create_payload["plan_id"],
        "execution_plan_id": created.json()["execution_plan_id"],
        "contract_ack": {
            "schema_version": ACTIVATION_SCHEMA_VERSION,
            "contract_version": CONTRACT_VERSION,
            "capability": ACTIVATION_CAPABILITY,
            "plan_id": create_payload["plan_id"],
            "status": "activated",
            "dispatch_material_fingerprint": create_payload["metadata"][
                "dispatch_material_fingerprint"
            ],
            "authority_generation_fingerprint": (
                AUTHORITY_GENERATION_FINGERPRINT
            ),
        },
        "item_acks": [
            {
                **item,
                "status": "activated",
                "execution_plan_id": created.json()["execution_plan_id"],
            }
            for item in activation_payload["items"]
        ],
        "activated_item_count": 3,
        "idempotent_replay": False,
    }
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "accepted"
        buy = next(item for item in plan.items if item.side == "buy")
        assert buy.status == "watch_sizing_pending"
        assert buy.risk_context["_core_activation_state"]["status"] == (
            "activated"
        )
        activation_audits_before_replay = session.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type
                == "core_activation_confirmed"
            )
        ).all()
        assert len(activation_audits_before_replay) == 1

    replay = client.post(
        "/v4/execution-plans/activations",
        json=activation_payload,
    )

    assert replay.status_code == 200, replay.json()
    assert replay.json()["idempotent_replay"] is True
    with app.state.db_session_factory() as session:
        activation_audits_after_replay = session.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type
                == "core_activation_confirmed"
            )
        ).all()
        assert len(activation_audits_after_replay) == 1
    engine.dispose()


@pytest.mark.parametrize(
    ("mismatch", "expected_status"),
    [
        ("plan_id", 404),
        ("execution_plan_id", 404),
        ("dispatch_material_fingerprint", 409),
        ("authority_generation_fingerprint", 409),
        ("core_item_key", 409),
        ("execution_plan_item_id", 409),
        ("decision_snapshot_id", 409),
        ("incomplete_items", 409),
    ],
)
def test_v4_activation_mismatch_leaves_the_entire_plan_pending(
    mismatch: str,
    expected_status: int,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _activation_bound_plan_payload(
        plan_id=f"core-v4-activation-mismatch-{mismatch}"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201, created.json()
    activation_payload = _activation_payload(create_payload, created.json())
    if mismatch == "plan_id":
        activation_payload["plan_id"] = "other-plan"
    elif mismatch == "execution_plan_id":
        activation_payload["execution_plan_id"] = "other-execution-plan"
    elif mismatch == "dispatch_material_fingerprint":
        activation_payload["dispatch_material_fingerprint"] = "b" * 64
    elif mismatch == "authority_generation_fingerprint":
        activation_payload["authority_generation_fingerprint"] = "b" * 64
    elif mismatch == "incomplete_items":
        activation_payload["items"].pop()
    else:
        activation_payload["items"][0][mismatch] = f"other-{mismatch}"

    response = client.post(
        "/v4/execution-plans/activations",
        json=activation_payload,
    )

    assert response.status_code == expected_status, response.json()
    assert response.json()["activated_item_count"] == 0
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "activation_pending"
        assert all(
            item.status == "core_activation_pending"
            for item in plan.items
            if item.side == "buy"
        )
        confirmed_audits = session.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type
                == "core_activation_confirmed"
            )
        ).all()
        assert confirmed_audits == []
    engine.dispose()


def test_v4_preactivation_revocation_cannot_be_reactivated() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _activation_bound_plan_payload(
        plan_id="core-v4-revoked-before-activation"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201, created.json()
    item_acks = created.json()["item_acks"]
    activation_payload = _activation_payload(create_payload, created.json())
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionJob(
                id="core-preactivation-revocation-job",
                execution_plan_item_id=item_acks[0][
                    "execution_plan_item_id"
                ],
                job_type="next_open_buy",
                status="scheduled",
                scheduled_for=datetime(2026, 9, 10, 0, 0, tzinfo=UTC),
            )
        )
        session.commit()

    revoked = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(create_payload, item_acks),
    )
    reactivation = client.post(
        "/v4/execution-plans/activations",
        json=activation_payload,
    )

    assert revoked.status_code == 200, revoked.json()
    assert reactivation.status_code == 409, reactivation.json()
    assert reactivation.json()["reason_code"] == "activation_state_not_pending"
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "revoked"
        assert all(item.status == "revoked" for item in plan.items)
        job = session.get(ExecutionJob, "core-preactivation-revocation-job")
        assert job is not None
        assert job.status == "revoked"
        assert session.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type
                == "core_activation_confirmed"
            )
        ).all() == []
    engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("kind", "shares"),
        ("quantity", 1),
        ("planned_quantity", 1),
        ("computed_quantity", 1),
    ],
)
def test_v4_intake_rejects_executable_fields_in_delegated_buy_policy(
    field_name: str,
    field_value: object,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id=f"core-v4-contradictory-policy-{field_name}")
    payload["items"][0]["quantity_policy"][field_name] = field_value
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 422
    with app.state.db_session_factory() as session:
        assert session.scalar(select(ExecutionPlan)) is None
    engine.dispose()


def test_v4_intake_rejects_timezone_naive_request_timestamp() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-naive-request-time")
    payload["requested_at"] = "2026-09-09T12:00:00"
    payload["metadata"]["requested_at"] = "2026-09-09T12:00:00"
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 422
    with app.state.db_session_factory() as session:
        assert session.scalar(select(ExecutionPlan)) is None
    engine.dispose()


def test_v4_persists_a_strictly_revalidatable_contract_payload() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)

    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-revalidatable-payload"),
    )

    assert response.status_code == 201
    with app.state.db_session_factory() as session:
        plan = session.scalar(select(ExecutionPlan))
        assert plan is not None
        validated = CoreExecutionPlanRequestV4.model_validate(
            plan.raw_payload,
            context={
                _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY: True,
            },
        )
        assert validated.plan_id == plan.external_plan_id
    engine.dispose()


def test_v4_canonicalizes_inherited_entry_actions_out_of_protective_exits() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-inherited-exit-actions")
    inherited_entry_actions = {
        "handoff_intent": "intraday_watch_buy",
        "raw_action": "watch_for_trigger",
        "tactical_action": "watch_for_trigger",
        "generic_action": "monitor_candidate",
    }
    for exit_item in payload["items"][1:]:
        exit_item["risk_context"].update(inherited_entry_actions)
        assert all(field not in exit_item for field in inherited_entry_actions)
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 201, response.json()
    assert response.json()["accepted_item_count"] == 3
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        exits = [item for item in plan.items if item.side == "sell"]
        assert len(exits) == 2
        assert all(
            all(field not in item.risk_context for field in inherited_entry_actions)
            for item in exits
        )
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
    engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "corrupt_value"),
    [
        ("plan_item_index", 9),
        ("symbol", "000660"),
        ("side", "sell"),
        ("intent", "stop_sell"),
        ("provider", "alpaca"),
        ("venue", "upbit"),
        ("asset_class", "crypto"),
        ("decision_agent_name", "short-term-decider"),
        ("decision_snapshot_id", "corrupt-snapshot"),
        ("decision_run_id", "corrupt-run"),
        ("source_market_date", datetime(2026, 9, 8, tzinfo=UTC).date()),
        ("time_in_force", "ioc"),
        ("base_quantity", Decimal("1")),
        ("quote_notional", Decimal("100000")),
        ("quantity_precision", 4),
        ("price_precision", 2),
    ],
)
def test_v4_contract_proof_rejects_persisted_item_identity_drift(
    field_name: str,
    corrupt_value: object,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id=f"core-v4-item-drift-{field_name}"),
    )
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        setattr(plan.items[0], field_name, corrupt_value)
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "corrupt_value"),
    [
        ("source", "operator-import"),
        ("mode", "paper"),
        ("strategy_run_id", "corrupt-run"),
        ("market_date", datetime(2026, 9, 11, tzinfo=UTC).date()),
        ("requested_at", datetime(2026, 9, 9, 12, 1, tzinfo=UTC)),
    ],
)
def test_v4_contract_proof_rejects_persisted_plan_identity_drift(
    field_name: str,
    corrupt_value: object,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id=f"core-v4-plan-drift-{field_name}"),
    )
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        setattr(plan, field_name, corrupt_value)
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_contract_proof_rejects_raw_payload_outside_the_strict_schema() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-raw-schema-drift"),
    )
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        raw_payload = deepcopy(plan.raw_payload)
        raw_payload["unexpected_authority"] = True
        plan.raw_payload = raw_payload
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


@pytest.mark.parametrize(
    "drift",
    [
        "entry_limit_price",
        "target_trigger_price",
        "quantity_budget",
        "tactical_action",
        "raw_action",
        "generic_action",
        "handoff_intent",
    ],
)
def test_v4_contract_proof_rejects_price_policy_and_action_drift(
    drift: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id=f"core-v4-semantic-drift-{drift}")
    entry = payload["items"][0]
    entry["limit_price"] = "71000"
    entry["handoff_intent"] = "next_open_buy"
    entry["raw_action"] = "buy"
    entry["tactical_action"] = "watch_for_trigger"
    entry["generic_action"] = "buy"
    entry["quantity_policy"]["max_budget_krw"] = "100000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = sorted(plan.items, key=lambda item: item.plan_item_index)
        if drift == "entry_limit_price":
            items[0].limit_price = Decimal("70000")
        elif drift == "target_trigger_price":
            items[1].trigger_price = Decimal("73000")
        elif drift == "quantity_budget":
            items[0].quantity_policy = {
                **items[0].quantity_policy,
                "max_budget_krw": "999999",
            }
        elif drift == "tactical_action":
            items[0].risk_context = {
                **items[0].risk_context,
                "tactical_action": "wait",
            }
        else:
            raw_payload = deepcopy(plan.raw_payload)
            raw_entry = raw_payload["items"][0]
            raw_entry[drift] = (
                "tactical_buy" if drift == "handoff_intent" else "wait"
            )
            plan.raw_payload = raw_payload
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_contract_proof_rejects_executable_fields_appended_to_unsized_policy() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-forged-additive-sizing"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
        }
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_contract_proof_allows_bounded_watch_attempt_metadata() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-watch-attempt-metadata"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        entry.quantity_policy = {
            **entry.quantity_policy,
            "exec_agent_watch_sizing_attempt_count": 1,
            "exec_agent_watch_sizing_max_attempts": 3,
            "exec_agent_watch_sizing_fallback_allowed": False,
            "exec_agent_watch_sizing_attempts": [
                {
                    "attempt": 1,
                    "max_attempts": 3,
                    "success": False,
                    "recorded_at": "2026-09-09T12:01:00+00:00",
                    "error": "temporary agent timeout",
                }
            ],
        }
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
    engine.dispose()


def test_v4_contract_proof_preserves_and_pins_server_account_route_binding() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="mock",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret=(
            "core-v4-account-scope-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-account-binding-proof"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = sorted(plan.items, key=lambda item: item.plan_item_index)
        raw_payload = deepcopy(plan.raw_payload)
        raw_risk = raw_payload["items"][0]["risk_context"]
        persisted_risk = items[0].risk_context
        assert raw_risk["execution_account_route"] == persisted_risk[
            "execution_account_route"
        ]

        corrupted_binding = {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "a" * 32,
        }
        raw_risk["execution_account_route"] = corrupted_binding
        raw_payload["items"][0]["quantity_policy"][
            "execution_account_route"
        ] = corrupted_binding
        plan.raw_payload = raw_payload
        items[0].risk_context = {
            **persisted_risk,
            "execution_account_route": corrupted_binding,
        }
        items[0].quantity_policy = {
            **items[0].quantity_policy,
            "execution_account_route": corrupted_binding,
        }
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_contract_proof_rejects_unproven_legacy_exec_agent_at_job_claim() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-exact-agent-sizing"),
    )
    assert response.status_code == 201, response.json()
    lease = datetime(2026, 9, 9, 12, 10, tzinfo=UTC)

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        decision, policy = _exec_agent_approval()
        decision.update(
            {
                "version": "short-term-exec-agent-v1",
                "execution_plan_item_id": entry.id,
                "account_snapshot": {
                    "holdings_complete": True,
                    "open_orders_complete": True,
                    "observed_at": "2026-09-09T12:05:00+00:00",
                },
                "agent_contract": {
                    "decision": "buy",
                    "confidence": "0.82",
                },
            }
        )
        entry.risk_context = {
            **entry.risk_context,
            "exec_agent_decision": decision,
            "target_price": decision["target_price"],
            "stop_price": decision["stop_price"],
        }
        entry.quantity_policy = policy
        entry.status = "next_open_buy_dispatching"
        job = ExecutionJob(
            id="core-v4-exact-agent-sizing-job",
            execution_plan_item_id=entry.id,
            job_type="next_open_buy",
            status="running",
            locked_at=lease,
        )
        session.add(job)
        session.commit()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
        assert _lock_job_for_broker_submission(
            db=session,
            job_id="core-v4-exact-agent-sizing-job",
            expected_locked_at=lease,
        ) is None

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        assert entry.status == "contract_quarantined"
        assert plan.status == "contract_quarantined"
        assert session.get(
            ExecutionJob,
            "core-v4-exact-agent-sizing-job",
        ).status == "revoked"
    engine.dispose()


def test_v4_watch_sizing_preserves_contract_proof_through_job_claim(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-watch-route-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-watch-sizing-job-claim")
    payload["mode"] = "paper"
    payload["items"][0]["quantity_policy"].update(
        {
            "budget_krw": "200000",
            "estimated_reference_price": "71000",
        }
    )
    payload["items"][1]["trigger_price"] = "76000"
    payload["items"][2]["trigger_price"] = "69000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()
    lease = datetime(2026, 9, 9, 23, 40, tzinfo=UTC)

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        request_payload = {"test_scope": "core-v4-watch-sizing-job-claim"}
        agent_response = {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": plan.external_plan_id,
            "_runtime_provenance": verified_runtime_provenance(
                settings=settings,
                request_payload=request_payload,
                session_id="core-v4-watch-sizing-session",
            ),
            "decisions": [
                {
                    "execution_plan_item_id": entry.id,
                    "symbol": entry.symbol,
                    "action": "approve",
                    "quantity": 2,
                    "approved_notional_krw": "142000",
                    "sizing_price": "71000",
                    "confidence": 0.82,
                    "target_profile": "short_swing",
                    "reason_code": "approved",
                    "reason": "bounded next-open entry",
                }
            ],
        }
        monkeypatch.setattr(
            watch_sizing,
            "_request_exec_agent_decisions",
            lambda **_: agent_response,
        )
        decision_payload = watch_sizing._request_exec_agent_decisions_with_retries(
            settings=settings,
            db=session,
            plan=plan,
            payload=request_payload,
            expected_item_ids={entry.id},
        )
        assert decision_payload == agent_response
        assert entry.quantity_policy["exec_agent_watch_sizing_attempt_count"] == 1
        assert entry.quantity_policy["exec_agent_watch_sizing_attempts"][-1][
            "success"
        ] is True
        session.commit()
        session.expire_all()
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload=decision_payload,
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={entry.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["approved"] == 1
        assert len(entry.jobs) == 1
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert (
                core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                    db=session,
                    runtime_mode="paper",
                    settings=settings,
                )
            == 0
        )
        entry.status = "next_open_buy_dispatching"
        entry.jobs[0].status = "running"
        entry.jobs[0].locked_at = lease
        job_id = entry.jobs[0].id
        session.commit()

    with app.state.db_session_factory() as session:
        assert _lock_job_for_broker_submission(
            db=session,
            job_id=job_id,
            expected_locked_at=lease,
        ) is not None
    engine.dispose()


def test_v4_terminal_watch_rejection_remains_proven_and_is_not_refenced() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        exec_agent_watch_sizing_max_attempts=1,
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-watch-reject-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-terminal-watch-rejection-proof")
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        request_payload = {"test_scope": "terminal-watch-rejection-proof"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": verified_runtime_provenance(
                    settings=settings,
                    request_payload=request_payload,
                    session_id="core-v4-watch-reject-session",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": entry.id,
                        "symbol": entry.symbol,
                        "action": "reject",
                        "quantity": 0,
                        "reason_code": "creative_agent_phrase",
                        "reason": "freeform language must not own authority",
                    }
                ],
            },
            account_context={
                "available": False,
                "unavailable_reasons": ["broker_account_snapshot_unavailable"],
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={entry.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["rejected"] == 1
        assert summary["no_trade"]["completed"] is True
        assert plan.status == "completed_no_trade"
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        session.commit()

    v4_readback = client.get(
        "/v4/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    )
    assert v4_readback.status_code == 200
    assert v4_readback.json()[0]["status"] == "completed_no_trade"
    v4_states = v4_readback.json()[0]["item_states"]
    assert [state["status"] for state in v4_states] == [
        "exec_agent_rejected",
        "not_applicable",
        "not_applicable",
    ]
    assert all(state["terminal"] is True for state in v4_states)
    execution_readback = client.get(
        "/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    )
    assert execution_readback.status_code == 200
    readback_items = execution_readback.json()[0]["items"]
    assert next(item for item in readback_items if item["side"] == "buy")[
        "status"
    ] == "exec_agent_rejected"
    assert {
        item["status"] for item in readback_items if item["side"] == "sell"
    } == {"not_applicable"}

    with app.state.db_session_factory() as session:
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
            )
            == 0
        )
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
            )
            == 0
        )
        audits = session.scalars(select(ExecutionPlanItemAudit)).all()
        assert all(
            audit.event_type != "legacy_core_authority_quarantined"
            for audit in audits
        )
    engine.dispose()


@pytest.mark.parametrize(
    "terminal_path",
    [
        "missed_background",
        "stale_broker_truth",
        "missed_response",
        "allowed_action_envelope",
        "sizing_skip",
        "zero_quantity",
        "agent_terminal",
    ],
)
def test_every_terminal_watch_writer_preserves_core_v4_contract_proof(
    terminal_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-canonical-reject-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _plan_payload(plan_id=f"core-v4-{terminal_path}")
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        decision_at = datetime(2026, 9, 9, 23, 30, tzinfo=UTC)

        if terminal_path == "missed_background":
            result = watch_sizing._expire_missed_next_open_items(
                db=session,
                settings=settings,
                plan=plan,
                now=datetime(2026, 9, 10, 0, 0, tzinfo=UTC),
            )
            assert result["rejected"] == 1
            assert result["no_trade"]["completed"] is True
            assert plan.status == "completed_no_trade"
        elif terminal_path == "stale_broker_truth":
            watch_sizing._reject_stale_state_blocked_item(
                db=session,
                item=entry,
                blockers=[
                    {
                        "classification": "stale-terminal-unknown",
                        "broker_order_id": "unknown-order",
                    }
                ],
            )
        else:
            if terminal_path in {
                "allowed_action_envelope",
                "sizing_skip",
                "zero_quantity",
                "agent_terminal",
            }:
                monkeypatch.setattr(
                    watch_sizing,
                    "_allowed_action_envelope_for_item",
                    lambda **_: {
                        "allowed_actions": ["buy", "downsize", "reject"],
                        "max_quantity": 3,
                        "max_notional_krw": "213000.0000",
                        "hard_reject_reasons": (
                            ["existing_holding"]
                            if terminal_path == "allowed_action_envelope"
                            else []
                        ),
                        "soft_warnings": [],
                    },
                )
            if terminal_path in {"sizing_skip", "zero_quantity"}:
                monkeypatch.setattr(
                    watch_sizing,
                    "_adjust_approved_watch_quantity",
                    lambda **_: {
                        "decision": (
                            "skip" if terminal_path == "sizing_skip" else "submit"
                        ),
                        "reason_code": "minimum_notional_unreachable",
                        "reason": "deterministic sizing left no executable quantity",
                        "quantity": 0,
                        "approved_notional": Decimal("0"),
                        "sizing_price": Decimal("71000"),
                        "adjustment": {
                            "agent_approved_quantity": 3,
                            "agent_approved_notional": "213000.0000",
                            "final_quantity": 0,
                            "final_notional": "0.0000",
                        },
                    },
                )
            request_payload = {"test_scope": f"core-v4-{terminal_path}"}
            action = "reject" if terminal_path == "agent_terminal" else "approve"
            if terminal_path == "missed_response":
                decision_at = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)
            result = _apply_decisions(
                db=session,
                settings=settings,
                plan=plan,
                request_payload=request_payload,
                decision_payload={
                    "version": "manual-exec-agent-watch-sizing-v1",
                    "plan_id": plan.external_plan_id,
                    "_runtime_provenance": verified_runtime_provenance(
                        settings=settings,
                        request_payload=request_payload,
                        session_id=f"core-v4-{terminal_path}-session",
                    ),
                    "decisions": [
                        {
                            "execution_plan_item_id": entry.id,
                            "symbol": entry.symbol,
                            "action": action,
                            "quantity": 3,
                            "approved_notional_krw": "213000.0000",
                            "sizing_price": "71000",
                            "confidence": 0.9,
                            "target_profile": "short_swing",
                            "reason_code": "minimum_notional_unreachable",
                            "reason": "proposal must not retain executable authority",
                        }
                    ],
                },
                account_context={
                    "available": True,
                    "orderable_cash_krw": "1000000",
                    "buying_power_krw": "1000000",
                    "holdings": [],
                    "open_orders": [],
                },
                expected_item_ids={entry.id},
                now=decision_at,
            )
            assert result["rejected"] == 1

        decision = entry.quantity_policy["exec_agent_decision"]
        assert entry.quantity_policy["rejection_disposition"] == "terminal"
        assert decision["quantity"] == 0
        assert decision["approved_quantity"] == 0
        assert decision["approved_notional_krw"] is None
        assert decision["sizing_price"] is None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        session.commit()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
            )
            == 0
        )
    engine.dispose()


@pytest.mark.parametrize("mismatch_kind", ["provider", "account_route"])
def test_route_mismatch_rejection_does_not_quarantine_deferred_siblings(
    mismatch_kind: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-route-mismatch-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id=f"core-v4-{mismatch_kind}-sibling-isolation"
    )
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buys = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        assert len(buys) == 3
        for item in buys[1:]:
            item.risk_context = {
                **item.risk_context,
                "watch_exec_agent_deferred": {
                    "disposition": "defer",
                    "reason_code": "reward_risk_below_floor",
                    "reason_family": "reward_risk",
                    "reason": "await a fresh quote",
                    "retry_after": "2099-01-01T00:00:00+00:00",
                },
            }

        now = datetime(2026, 9, 9, 23, 30, tzinfo=UTC)
        if mismatch_kind == "provider":
            summaries = watch_sizing._quarantine_provider_mismatched_items(
                settings=settings,
                db=session,
                plans=[plan],
                adapter=SimpleNamespace(provider="alpaca"),
                now=now,
            )
            expected_reason = "watch_exec_agent_sizing_route_unsupported"
            mismatch_key = "provider_mismatch_item_ids"
        else:
            rotated = settings.model_copy(
                update={"kis_account_number": "87654321"}
            )
            summaries = watch_sizing._quarantine_account_route_mismatched_items(
                settings=rotated,
                db=session,
                plans=[plan],
                now=now,
            )
            expected_reason = "account_route_mismatch"
            mismatch_key = "account_route_mismatch_item_ids"

        assert summaries[0][mismatch_key] == [buys[0].id]
        rejected_decision = buys[0].quantity_policy["exec_agent_decision"]
        assert buys[0].status == "exec_agent_rejected"
        assert buys[0].quantity_policy["reason_code"] == expected_reason
        assert rejected_decision["quantity"] == 0
        assert rejected_decision["approved_quantity"] == 0
        assert rejected_decision["approved_notional_krw"] is None
        assert [item.status for item in buys[1:]] == [
            "watch_sizing_pending",
            "watch_sizing_pending",
        ]
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
            )
            == 0
        )
        assert [item.status for item in buys] == [
            "exec_agent_rejected",
            "watch_sizing_pending",
            "watch_sizing_pending",
        ]
    engine.dispose()


def test_provider_mismatch_finalizes_an_all_terminal_family_without_exposure() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-provider-finalizer-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-provider-finalizer")
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        summaries = watch_sizing._quarantine_provider_mismatched_items(
            settings=settings,
            db=session,
            plans=[plan],
            adapter=SimpleNamespace(provider="alpaca"),
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )

        assert summaries[0]["no_trade"]["completed"] is True
        assert plan.status == "completed_no_trade"
        assert [item.status for item in plan.items] == [
            "exec_agent_rejected",
            "not_applicable",
            "not_applicable",
        ]
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
    engine.dispose()


def test_v4_mixed_terminal_rejects_do_not_quarantine_retryable_deferred_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-mixed-outcome-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-mixed-watch-outcomes"
    )
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buy_items = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        assert len(buy_items) == 3
        for index, item in enumerate(buy_items, start=1):
            raw_policy = dict(item.quantity_policy)
            attempted_policy = {
                **raw_policy,
                "exec_agent_watch_sizing_attempt_count": 1,
                "exec_agent_watch_sizing_max_attempts": 3,
                "exec_agent_watch_sizing_fallback_allowed": False,
                "exec_agent_watch_sizing_attempts": [
                    {
                        "attempt": 1,
                        "max_attempts": 3,
                        "success": True,
                        "recorded_at": f"2026-10-08T00:0{index}:00+00:00",
                    }
                ],
            }
            item.quantity_policy = attempted_policy
        plan.status = "running"
        monkeypatch.setattr(
            watch_sizing,
            "_allowed_action_envelope_for_item",
            lambda **kwargs: {
                "allowed_actions": ["buy", "downsize", "reject"],
                "max_quantity": 3,
                "max_notional_krw": "213000.0000",
                "hard_reject_reasons": (
                    ["reward_risk_below_minimum"]
                    if kwargs["item"].symbol == "035420"
                    else []
                ),
                "soft_warnings": [],
            },
        )
        monkeypatch.setattr(
            watch_sizing,
            "_reward_risk_rejection_price_cap",
            lambda **kwargs: (
                Decimal("70000") if kwargs["item"].symbol == "035420" else None
            ),
        )
        request_payload = {"test_scope": "actual-mixed-watch-outcomes"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": {
                    "actual_agent_id": "wrong-agent",
                    "actual_model_ref": settings.exec_agent_model,
                    "session_id": "unverified-mixed-outcomes",
                },
                "decisions": [
                    {
                        "execution_plan_item_id": item.id,
                        "symbol": item.symbol,
                        "action": "approve" if index <= 2 else "reject",
                        "quantity": 2 if index <= 2 else 0,
                        "approved_notional_krw": (
                            "142000.0000" if index <= 2 else None
                        ),
                        "sizing_price": "71000",
                        "reason_code": (
                            "agent_approval"
                            if index <= 2
                            else "reward_risk_below_floor"
                        ),
                        "reason": (
                            "proposal has invalid runtime provenance"
                            if index <= 2
                            else "reward/risk must be recalculated on a fresh quote"
                        ),
                    }
                    for index, item in enumerate(buy_items, start=1)
                ],
            },
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={item.id for item in buy_items},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["rejected"] == 2
        assert summary["deferred"] == 1
        session.commit()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
            )
            == 0
        )
        buy_statuses = [
            item.status
            for item in sorted(plan.items, key=lambda item: item.plan_item_index)
            if item.side == "buy"
        ]
        assert buy_statuses == [
            "exec_agent_rejected",
            "exec_agent_rejected",
            "watch_sizing_pending",
        ]
        assert plan.status == "running"

    readback = client.get(
        "/v4/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    )
    assert readback.status_code == 200
    buy_states = [
        state
        for state in readback.json()[0]["item_states"]
        if state["core_item_key"].endswith("next_open_buy")
    ]
    assert [state["status"] for state in buy_states] == [
        "exec_agent_rejected",
        "exec_agent_rejected",
        "watch_sizing_pending",
    ]
    assert [state["terminal"] for state in buy_states] == [True, True, False]
    assert buy_states[2]["decision"] == {
        "action": "defer",
        "decision": "deferred",
        "reason_code": "reward_risk_below_floor",
        "reason_family": "reward_risk",
        "rejection_disposition": "defer",
        "reason": "reward/risk must be recalculated on a fresh quote",
        "retry_after": "2026-09-09T23:35:00+00:00",
    }
    engine.dispose()


@pytest.mark.parametrize(
    "legacy_shape",
    [
        "missed_window",
        "stale_broker_truth",
        "allowed_action_envelope",
        "sizing_skip",
        "zero_quantity",
    ],
)
def test_reconcile_migrates_pre_fix_terminal_reject_without_fencing_siblings(
    legacy_shape: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-pre-fix-terminal-reject",
    )
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buys = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        terminal, deferred, untouched = buys
        raw_policy = deepcopy(terminal.quantity_policy)
        decision_source = (
            "watch_exec_agent_sizing"
            if legacy_shape in {"missed_window", "stale_broker_truth"}
            else "openclaw_exec_agent_watch_sizing"
        )
        proposed_quantity = 0 if legacy_shape in {
            "missed_window",
            "stale_broker_truth",
        } else 3
        reason_code = {
            "missed_window": "missed_next_open_window",
            "stale_broker_truth": "stale_order_broker_truth_unconfirmed",
            "allowed_action_envelope": "allowed_action_envelope_blocked",
            "sizing_skip": "approved_notional_below_minimum",
            "zero_quantity": "allowed_action_envelope_blocked",
        }[legacy_shape]
        reason = {
            "missed_window": (
                "next-open sizing was not completed before the market-open deadline"
            ),
            "stale_broker_truth": (
                "stale local open order could not be confirmed against broker truth"
            ),
            "allowed_action_envelope": (
                "legacy deterministic envelope rejected this proposal"
            ),
            "sizing_skip": "approved notional was below the executable minimum",
            "zero_quantity": "allowed action envelope left no buyable quantity",
        }[legacy_shape]
        legacy_decision = {
            "source": decision_source,
            "action": "reject",
            "decision": "rejected",
            "execution_plan_item_id": terminal.id,
            "symbol": terminal.symbol,
            "quantity": proposed_quantity,
            "approved_quantity": proposed_quantity,
            "approved_notional_krw": (
                "0.0000" if proposed_quantity == 0 else "213000.0000"
            ),
            "reason_code": reason_code,
            "reason": reason,
            "decided_at": "2026-09-09T12:00:00+00:00",
        }
        if proposed_quantity:
            legacy_decision["sizing_price"] = "71000.0000"
        legacy_policy = {
            "kind": "exec_agent_rejected",
            "source": "exec_agent_watch_sizing",
            "exec_agent_decision": legacy_decision,
            "short_term_exec_agent_decision": legacy_decision,
            "previous_policy": raw_policy,
        }
        if legacy_shape == "missed_window":
            legacy_policy.update({"reason_code": reason_code, "reason": reason})
            terminal.status = "next_open_buy_expired"
        elif legacy_shape == "stale_broker_truth":
            legacy_policy.update(
                {
                    "exec_agent_decision_audit": {
                        "event_type": (
                            "watch_exec_agent_sizing_stale_state_blocked"
                        ),
                        "storage": "execution_plan_item_audits",
                    },
                    "stale_state_blockers": [
                        {
                            "classification": "stale-terminal-unknown",
                            "broker_order_id": "legacy-unknown-order",
                        }
                    ],
                }
            )
            terminal.status = "exec_agent_rejected"
        else:
            legacy_policy.update(
                {
                    "exec_agent_decision_audit": {
                        "event_type": "watch_exec_agent_sizing_decision",
                        "storage": "execution_plan_item_audits",
                    },
                    "allowed_action_envelope": {
                        "allowed_actions": ["reject"],
                        "hard_reject_reasons": [reason_code],
                    },
                    "market_regime_guardrail": {"regime": "risk_off"},
                }
            )
            if legacy_shape != "zero_quantity":
                legacy_policy.update({"reason_code": reason_code, "reason": reason})
            if legacy_shape == "sizing_skip":
                legacy_policy.update(
                    {
                        "sizing_adjustment": {
                            "agent_approved_quantity": 3,
                            "agent_approved_notional": "213000.0000",
                            "final_quantity": 0,
                            "final_notional": "0.0000",
                        },
                        "sizing_explanation": {
                            "reason_code": reason_code,
                            "agent_proposed_quantity": 3,
                        },
                    }
                )
            terminal.status = "exec_agent_rejected"
        terminal.quantity_policy = legacy_policy
        deferred.risk_context = {
            **deferred.risk_context,
            "watch_exec_agent_deferred": {
                "disposition": "defer",
                "reason_family": "reward_risk",
                "reason_code": "reward_risk_hard_block",
                "reason": "wait for a safer entry",
                "retry_after": "2026-09-09T12:10:00+00:00",
            },
        }
        sibling_statuses = {
            deferred.id: deferred.status,
            untouched.id: untouched.status,
        }
        plan.status = "running"
        session.commit()

    with app.state.db_session_factory() as session:
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="mock",
                settings=app.state.settings,
            )
            == 1
        )

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buys = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        terminal, deferred, untouched = buys
        decision = terminal.quantity_policy["exec_agent_decision"]
        assert terminal.status == (
            "next_open_buy_expired"
            if legacy_shape == "missed_window"
            else "exec_agent_rejected"
        )
        assert decision["reason_code"] == reason_code
        assert decision["quantity"] == 0
        assert decision["approved_quantity"] == 0
        assert decision["approved_notional_krw"] is None
        assert terminal.quantity_policy["rejection_disposition"] == "terminal"
        assert deferred.status == sibling_statuses[deferred.id]
        assert untouched.status == sibling_statuses[untouched.id]
        assert plan.status == "running"
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == terminal.id,
                ExecutionPlanItemAudit.event_type
                == "legacy_watch_authority_canonicalized",
            )
        )
        assert audit is not None
        assert audit.payload["prior_quantity_policy"]["exec_agent_decision"][
            "quantity"
        ] == proposed_quantity
    engine.dispose()


def test_reconcile_zeroes_reported_unverified_watch_approval_only() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-unverified-migration-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-reported-unverified-watch-approval",
    )
    payload["mode"] = "paper"
    for raw_item in payload["items"]:
        if raw_item["side"] == "buy":
            raw_item["intent"] = "intraday_watch_buy"
            raw_item["trigger_price"] = "70000"
            raw_item["limit_price"] = "71000"
        elif raw_item["intent"] == "target_sell":
            raw_item["trigger_price"] = "76000"
        elif raw_item["intent"] == "stop_sell":
            raw_item["trigger_price"] = "69000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buys = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        unverified, deferred, untouched = buys
        request_payload = {"test_scope": "reported-unverified-migration"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": verified_runtime_provenance(
                    settings=settings,
                    request_payload=request_payload,
                    session_id="reported-unverified-migration-session",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": unverified.id,
                        "symbol": unverified.symbol,
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": "142000",
                        "sizing_price": "71000",
                        "confidence": 0.82,
                        "target_profile": "short_swing",
                        "reason_code": "approved",
                        "reason": "pre-fix runtime accepted reported metadata",
                    }
                ],
            },
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={unverified.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["approved"] == 1
        assert unverified.status == "watch_monitoring_active"
        legacy_runtime = dict(
            unverified.quantity_policy["approval_provenance"]["runtime"]
        )
        legacy_runtime.pop("runtime_contract")
        legacy_runtime.pop("verified_agent_id", None)
        legacy_runtime.pop("agent_identity_source", None)
        legacy_runtime["actual_agent_id"] = None
        legacy_runtime["metadata_status"] = "partial"
        legacy_decision = {
            **unverified.quantity_policy["exec_agent_decision"],
            "runtime_provenance": legacy_runtime,
        }
        unverified.quantity_policy = {
            **unverified.quantity_policy,
            "approval_provenance": {
                "kind": "reported_unverified_exec_agent_watch_sizing",
                "runtime": legacy_runtime,
            },
            "exec_agent_decision": legacy_decision,
            "short_term_exec_agent_decision": legacy_decision,
        }
        deferred.risk_context = {
            **deferred.risk_context,
            "watch_exec_agent_deferred": {
                "disposition": "defer",
                "reason_family": "reward_risk",
                "reason_code": "reward_risk_hard_block",
                "reason": "wait for a safer entry",
                "retry_after": "2026-09-09T12:10:00+00:00",
            },
        }
        sibling_statuses = {
            deferred.id: deferred.status,
            untouched.id: untouched.status,
        }
        session.commit()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
                settings=settings,
            )
            == 1
        )

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        buys = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        unverified, deferred, untouched = buys
        decision = unverified.quantity_policy["exec_agent_decision"]
        assert unverified.status == "exec_agent_rejected"
        assert decision["reason_code"] == "unverified_exec_agent_runtime"
        assert decision["quantity"] == 0
        assert decision["approved_quantity"] == 0
        assert decision["approved_notional_krw"] is None
        assert "approval_provenance" not in unverified.quantity_policy
        assert deferred.status == sibling_statuses[deferred.id]
        assert untouched.status == sibling_statuses[untouched.id]
        assert plan.status != "contract_quarantined"
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == unverified.id,
                ExecutionPlanItemAudit.event_type
                == "legacy_watch_authority_canonicalized",
            )
        )
        assert audit is not None
        assert audit.payload["prior_quantity_policy"]["quantity"] == 2
    engine.dispose()


def test_direct_fence_canonicalizes_unverified_watch_approval_before_submission() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_app_key="mock-key",
        kis_app_secret="mock-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-direct-fence-migration-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-direct-fence-unverified-approval")
    payload["mode"] = "paper"
    payload["items"][0]["intent"] = "intraday_watch_buy"
    payload["items"][0]["trigger_price"] = "70000"
    payload["items"][0]["limit_price"] = "71000"
    payload["items"][1]["trigger_price"] = "76000"
    payload["items"][2]["trigger_price"] = "69000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        request_payload = {"test_scope": "direct-fence-unverified-approval"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": verified_runtime_provenance(
                    settings=settings,
                    request_payload=request_payload,
                    session_id="direct-fence-unverified-approval-session",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": entry.id,
                        "symbol": entry.symbol,
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": "142000",
                        "sizing_price": "71000",
                        "confidence": 0.82,
                        "target_profile": "short_swing",
                        "reason_code": "approved",
                        "reason": "historical runtime trusted reported metadata",
                    }
                ],
            },
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={entry.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["approved"] == 1
        entry.quantity_policy = {
            **entry.quantity_policy,
            "approval_provenance": {
                **entry.quantity_policy["approval_provenance"],
                "kind": "reported_unverified_exec_agent_watch_sizing",
            },
        }
        session.commit()
        entry_id = entry.id

    with app.state.db_session_factory() as session:
        assert session.scalars(select(BrokerOrder)).all() == []
        assert core_handoff_v4.fence_unproven_legacy_core_buy_authority(
            db=session,
            execution_plan_item_id=entry_id,
            settings=settings,
        ) is True
        assert session.scalars(select(BrokerOrder)).all() == []

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        exits = [item for item in plan.items if item.side == "sell"]
        decision = entry.quantity_policy["exec_agent_decision"]
        assert entry.status == "exec_agent_rejected"
        assert decision["quantity"] == 0
        assert decision["approved_quantity"] == 0
        assert decision["approved_notional_krw"] is None
        assert plan.status == "completed_no_trade"
        assert {item.status for item in exits} == {"not_applicable"}
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert session.scalars(select(BrokerOrder)).all() == []

    readback = client.get(
        "/v4/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    )
    assert readback.status_code == 200
    assert readback.json()[0]["status"] == "completed_no_trade"
    assert [state["status"] for state in readback.json()[0]["item_states"]] == [
        "exec_agent_rejected",
        "not_applicable",
        "not_applicable",
    ]
    assert all(state["terminal"] for state in readback.json()[0]["item_states"])
    engine.dispose()


@pytest.mark.parametrize(
    "exposure_kind",
    [
        "linked_broker_order",
        "scoped_position",
        "legacy_unscoped_position",
        "unlinked_active_order",
        "unlinked_filled_order",
        "unlinked_fill_evidence",
        "unlinked_unknown_order",
        "unlinked_blank_venue",
    ],
)
def test_reconcile_does_not_zero_unverified_approval_with_possible_exposure(
    exposure_kind: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-exposure-migration-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-unverified-with-exposure"
    )
    payload["mode"] = "paper"
    for raw_item in payload["items"]:
        if raw_item["side"] == "buy":
            raw_item["intent"] = "intraday_watch_buy"
            raw_item["trigger_price"] = "70000"
            raw_item["limit_price"] = "71000"
        elif raw_item["intent"] == "target_sell":
            raw_item["trigger_price"] = "76000"
        elif raw_item["intent"] == "stop_sell":
            raw_item["trigger_price"] = "69000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entries = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        entry, deferred, untouched = entries
        request_payload = {"test_scope": "unverified-with-exposure"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": verified_runtime_provenance(
                    settings=settings,
                    request_payload=request_payload,
                    session_id="unverified-with-exposure-session",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": entry.id,
                        "symbol": entry.symbol,
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": "142000",
                        "sizing_price": "71000",
                        "confidence": 0.82,
                        "target_profile": "short_swing",
                        "reason_code": "approved",
                        "reason": "pre-fix approval with possible exposure",
                    }
                ],
            },
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={entry.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["approved"] == 1
        entry.quantity_policy = {
            **entry.quantity_policy,
            "approval_provenance": {
                **entry.quantity_policy["approval_provenance"],
                "kind": "reported_unverified_exec_agent_watch_sizing",
            },
        }
        deferred.status = "watch_sizing_pending"
        deferred.risk_context = {
            **deferred.risk_context,
            "watch_exec_agent_deferred": {
                "disposition": "defer",
                "reason_family": "reward_risk",
                "reason_code": "reward_risk_hard_block",
                "reason": "wait for a safer entry",
                "retry_after": "2026-09-09T12:10:00+00:00",
            },
        }
        for candidate in entries:
            session.add(
                ExecutionJob(
                    id=f"job-{candidate.id}",
                    execution_plan_item_id=candidate.id,
                    job_type="intraday_watch_buy",
                    status="queued",
                )
            )
        sibling_statuses = {
            item.id: item.status for item in plan.items if item.id != entry.id
        }
        if exposure_kind in {"scoped_position", "legacy_unscoped_position"}:
            identity = build_position_identity(
                settings=settings,
                provider="kis",
                exchange="KRX",
            )
            session.add(
                ExecutionPosition(
                    mode="paper",
                    provider=(
                        identity.provider if exposure_kind == "scoped_position" else None
                    ),
                    account_scope=(
                        identity.account_scope
                        if exposure_kind == "scoped_position"
                        else None
                    ),
                    exchange=(
                        identity.exchange if exposure_kind == "scoped_position" else None
                    ),
                    symbol=entry.symbol,
                    quantity=2,
                    updated_from="broker_reconciliation",
                )
            )
        else:
            order = BrokerOrder(
                execution_plan_item_id=(
                    entry.id if exposure_kind == "linked_broker_order" else None
                ),
                broker_name="kis",
                mode="paper",
                idempotency_key=f"unverified-with-exposure:{exposure_kind}",
                broker_order_id=f"broker-order-{exposure_kind}",
                symbol=entry.symbol,
                side="buy",
                status=(
                    "filled"
                    if exposure_kind == "unlinked_filled_order"
                    else "mystery"
                    if exposure_kind == "unlinked_unknown_order"
                    else "failed"
                    if exposure_kind == "unlinked_fill_evidence"
                    else "open"
                ),
                quantity=2,
                venue=("" if exposure_kind == "unlinked_blank_venue" else "KRX"),
            )
            session.add(order)
            if exposure_kind == "unlinked_fill_evidence":
                session.flush()
                session.add(
                    ExecutionFill(
                        broker_order_pk=order.id,
                        broker_fill_id="unlinked-fill-evidence",
                        symbol=entry.symbol,
                        side="buy",
                        quantity=2,
                        price=Decimal("71000"),
                        filled_at=datetime(2026, 9, 9, 23, 35, tzinfo=UTC),
                    )
                )
        session.commit()

    with app.state.db_session_factory() as session:
        assert (
            core_handoff_v4.reconcile_unproven_legacy_core_buy_authority(
                db=session,
                runtime_mode="paper",
                settings=settings,
            )
            == 1
        )

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entries = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        entry, deferred, untouched = entries
        protective_exits = [item for item in plan.items if item.side == "sell"]
        assert entry.status == "manual_review_required"
        assert entry.quantity_policy["quantity"] == 2
        assert plan.status == "manual_review_required"
        assert deferred.status == sibling_statuses[deferred.id]
        assert untouched.status == sibling_statuses[untouched.id]
        assert all(
            item.status == sibling_statuses[item.id] for item in protective_exits
        )
        jobs = {
            job.execution_plan_item_id: job
            for job in session.scalars(select(ExecutionJob)).all()
        }
        assert jobs[entry.id].status == "revoked"
        assert jobs[deferred.id].status == "queued"
        assert jobs[untouched.id].status == "queued"
        assert session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == entry.id,
                ExecutionPlanItemAudit.event_type
                == "legacy_watch_authority_canonicalized",
            )
        ) is None
    engine.dispose()


def test_invalid_linked_exit_fences_only_its_entry_family() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-linked-exit-fence-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-invalid-linked-exit-family"
    )
    payload["mode"] = "paper"
    for raw_item in payload["items"]:
        if raw_item["side"] == "buy":
            raw_item["quantity_policy"].update(
                {
                    "budget_krw": "200000",
                    "estimated_reference_price": "71000",
                }
            )
            raw_item["limit_price"] = "71000"
        elif raw_item["intent"] == "target_sell":
            raw_item["trigger_price"] = "76000"
        elif raw_item["intent"] == "stop_sell":
            raw_item["trigger_price"] = "69000"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entries = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        affected, deferred, untouched = entries
        request_payload = {"test_scope": "valid-sibling-after-family-fence"}
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            request_payload=request_payload,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "_runtime_provenance": verified_runtime_provenance(
                    settings=settings,
                    request_payload=request_payload,
                    session_id="valid-sibling-after-family-fence-session",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": untouched.id,
                        "symbol": untouched.symbol,
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": "142000",
                        "sizing_price": "71000",
                        "confidence": 0.82,
                        "target_profile": "short_swing",
                        "reason_code": "approved",
                        "reason": "valid sibling remains executable",
                    }
                ],
            },
            account_context={
                "available": True,
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={untouched.id},
            now=datetime(2026, 9, 9, 23, 30, tzinfo=UTC),
        )
        assert summary["approved"] == 1
        assert len(untouched.jobs) == 1
        linked_exit = next(
            item
            for item in plan.items
            if item.side == "sell"
            and item.quantity_policy.get("linked_entry_item_key")
            == affected.source_plan_item_id
        )
        linked_exit.quantity_policy = {
            **linked_exit.quantity_policy,
            "linked_entry_item_key": "tampered-entry-key",
        }
        deferred.status = "watch_sizing_pending"
        for entry in (affected, deferred):
            session.add(
                ExecutionJob(
                    id=f"linked-exit-job-{entry.id}",
                    execution_plan_item_id=entry.id,
                    job_type="next_open_buy",
                    status="queued",
                )
            )
        lease = datetime(2026, 9, 9, 23, 40, tzinfo=UTC)
        untouched.status = "next_open_buy_dispatching"
        untouched.jobs[0].status = "running"
        untouched.jobs[0].locked_at = lease
        untouched_job_id = untouched.jobs[0].id
        sibling_statuses = {
            item.id: item.status for item in plan.items if item.id != affected.id
        }
        session.commit()
        affected_id = affected.id
        deferred_id = deferred.id
        untouched_id = untouched.id

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
        assert core_handoff_v4.fence_unproven_legacy_core_buy_authority(
            db=session,
            execution_plan_item_id=affected_id,
            settings=settings,
        ) is True
        assert core_handoff_v4.fence_unproven_legacy_core_buy_authority(
            db=session,
            execution_plan_item_id=deferred_id,
            settings=settings,
        ) is False
        assert _lock_job_for_broker_submission(
            db=session,
            job_id=untouched_job_id,
            expected_locked_at=lease,
            settings=settings,
        ) is not None

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = {item.id: item for item in plan.items}
        assert items[affected_id].status == "contract_quarantined"
        assert items[deferred_id].status == sibling_statuses[deferred_id]
        assert items[untouched_id].status == sibling_statuses[untouched_id]
        for item_id, prior_status in sibling_statuses.items():
            assert items[item_id].status == prior_status
        jobs = {
            job.execution_plan_item_id: job
            for job in session.scalars(select(ExecutionJob)).all()
        }
        assert jobs[affected_id].status == "revoked"
        assert jobs[deferred_id].status == "queued"
        assert jobs[untouched_id].status == "running"
        assert plan.status == "manual_review_required"
    engine.dispose()


def test_ambiguous_persisted_item_mapping_fences_all_entry_families() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "core-v4-ambiguous-mapping-secret-at-least-32-bytes"
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    payload = _multi_family_plan_payload(
        plan_id="core-v4-ambiguous-persisted-item-mapping"
    )
    payload["mode"] = "paper"
    _refresh_material_fingerprint(payload)
    response = client.post("/v4/execution-plans", json=payload)
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entries = sorted(
            (item for item in plan.items if item.side == "buy"),
            key=lambda item: item.plan_item_index,
        )
        entries[1].source_plan_item_id = entries[0].source_plan_item_id
        for entry in entries:
            session.add(
                ExecutionJob(
                    id=f"ambiguous-mapping-job-{entry.id}",
                    execution_plan_item_id=entry.id,
                    job_type="next_open_buy",
                    status="queued",
                )
            )
        exit_statuses = {
            item.id: item.status for item in plan.items if item.side == "sell"
        }
        session.commit()
        target_entry_id = entries[0].id
        entry_ids = {item.id for item in entries}

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4._unproven_core_buy_item_ids(plan) is None
        assert core_handoff_v4.fence_unproven_legacy_core_buy_authority(
            db=session,
            execution_plan_item_id=target_entry_id,
            settings=settings,
        ) is True

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = {item.id: item for item in plan.items}
        assert {items[item_id].status for item_id in entry_ids} == {
            "contract_quarantined"
        }
        assert all(
            items[item_id].status == prior_status
            for item_id, prior_status in exit_statuses.items()
        )
        jobs = session.scalars(select(ExecutionJob)).all()
        assert {job.status for job in jobs} == {"revoked"}
        assert plan.status == "contract_quarantined"
    engine.dispose()


def test_v4_readback_keeps_immutable_acks_visible_for_quarantined_current_state() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-quarantined-readback")
    create_response = client.post("/v4/execution-plans", json=payload)
    assert create_response.status_code == 201, create_response.json()
    immutable_item_acks = create_response.json()["item_acks"]

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
            "exec_agent_decision": {
                "action": "approve",
                "decision": "approved",
                "reason_code": "unproven_approval",
                "reason": "must never override parent quarantine",
            },
        }
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        session.commit()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None

    readback = client.get(
        "/v4/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    )
    assert readback.status_code == 200
    assert len(readback.json()) == 1
    assert readback.json()[0]["item_acks"] == immutable_item_acks
    entry_state = next(
        state
        for state in readback.json()[0]["item_states"]
        if state["core_item_key"].endswith("next_open_buy")
    )
    assert entry_state["status"] == "contract_quarantined"
    assert entry_state["terminal"] is True
    assert entry_state["decision"] == {
        "action": "reject",
        "decision": "rejected",
        "reason_code": "persisted_v4_contract_not_proven",
        "reason_family": "runtime_contract",
        "rejection_disposition": "terminal",
        "reason": "current item authority is contract-quarantined",
    }
    engine.dispose()


def test_v4_readback_paginates_with_bounded_query_parameters() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    plan_ids = {f"core-v4-page-{index}" for index in range(3)}
    for plan_id in sorted(plan_ids):
        response = client.post(
            "/v4/execution-plans",
            json=_plan_payload(plan_id=plan_id),
        )
        assert response.status_code == 201, response.json()

    first_page = client.get(
        "/v4/execution-plans",
        params={"limit": 2, "offset": 0},
    )
    second_page = client.get(
        "/v4/execution-plans",
        params={"limit": 2, "offset": 2},
    )

    assert first_page.status_code == 200
    assert second_page.status_code == 200
    assert len(first_page.json()) == 2
    assert len(second_page.json()) == 1
    assert {
        plan["plan_id"] for plan in first_page.json() + second_page.json()
    } == plan_ids
    assert client.get("/v4/execution-plans", params={"limit": 0}).status_code == 422
    assert client.get("/v4/execution-plans", params={"limit": 501}).status_code == 422
    assert client.get("/v4/execution-plans", params={"offset": -1}).status_code == 422
    assert (
        client.get("/v4/execution-plans", params={"offset": 10001}).status_code
        == 422
    )
    engine.dispose()


@pytest.mark.parametrize(
    "drift",
    [
        "quantity",
        "source",
        "idempotency_key",
        "sizing_price",
        "approved_notional",
        "confidence",
        "decision_action",
        "decision_idempotency_key",
        "decision_target_price",
        "decision_stop_price",
        "risk_target_price",
        "risk_stop_price",
    ],
)
def test_v4_contract_proof_rejects_exec_agent_policy_decision_drift(
    drift: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id=f"core-v4-agent-policy-drift-{drift}"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        decision, policy = _exec_agent_approval()
        if drift == "quantity":
            policy["quantity"] = 3
        elif drift == "source":
            policy["source"] = "short_term_exec_agent_v1"
        elif drift == "idempotency_key":
            policy["idempotency_key"] = "other-key"
        elif drift == "sizing_price":
            policy["sizing_price"] = "70000"
        elif drift == "approved_notional":
            policy["approved_notional"] = "140000"
        elif drift == "confidence":
            policy["confidence"] = "0.10"
        elif drift == "decision_action":
            decision["action"] = "reject"
        elif drift == "decision_idempotency_key":
            decision["idempotency_key"] = "other-key"
        elif drift == "decision_target_price":
            decision["target_price"] = "73000"
        elif drift == "decision_stop_price":
            decision["stop_price"] = "70000"
        entry.risk_context = {
            **entry.risk_context,
            "exec_agent_decision": decision,
            "target_price": (
                "73000"
                if drift == "risk_target_price"
                else decision["target_price"]
            ),
            "stop_price": (
                "70000"
                if drift == "risk_stop_price"
                else decision["stop_price"]
            ),
        }
        entry.quantity_policy = policy
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_contract_proof_rejects_unproven_short_term_linked_exit_transform() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-short-term-linked-exit"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = sorted(plan.items, key=lambda item: item.plan_item_index)
        entry = items[0]
        protective_exits = items[1:]
        decision, entry_policy = _short_term_exec_agent_approval()
        entry.risk_context = {
            **entry.risk_context,
            "exec_agent_decision": decision,
        }
        entry.quantity_policy = entry_policy
        for protective_exit in protective_exits:
            protective_exit.quantity_policy = {
                "kind": "shares",
                "quantity": 2,
                "source": "short_term_exec_agent_v1",
                "linked_entry_item_key": entry.source_plan_item_id,
            }
            protective_exit.risk_context = {
                **protective_exit.risk_context,
                "entry_context": {
                    "source_plan_item_id": entry.source_plan_item_id,
                    "decision_run_id": entry.decision_run_id,
                    "decision_snapshot_id": entry.decision_snapshot_id,
                    "entry_quantity": 2,
                    "linked_by": "short_term_exec_agent_v1",
                },
            }
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
        assert core_handoff_v4.fence_unproven_legacy_core_buy_authority(
            db=session,
            execution_plan_item_id=entry.id,
            settings=app.state.settings,
        ) is True
        assert entry.status == "contract_quarantined"
        assert all(
            item.status != "contract_quarantined" for item in protective_exits
        )
    engine.dispose()


def test_v4_contract_proof_rejects_linked_exit_quantity_outside_entry_decision() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-linked-exit-quantity-drift"),
    )
    assert response.status_code == 201, response.json()

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        items = sorted(plan.items, key=lambda item: item.plan_item_index)
        entry, target = items[:2]
        decision, entry_policy = _short_term_exec_agent_approval(quantity=2)
        entry.risk_context = {
            **entry.risk_context,
            "exec_agent_decision": decision,
        }
        entry.quantity_policy = entry_policy
        target.quantity_policy = {
            "kind": "shares",
            "quantity": 3,
            "source": "short_term_exec_agent_v1",
            "linked_entry_item_key": entry.source_plan_item_id,
        }
        target.risk_context = {
            **target.risk_context,
            # The old proof only required any decision-shaped dict here and did
            # not bind the exit quantity back to the entry approval.
            "exec_agent_decision": {},
            "entry_context": {
                "source_plan_item_id": entry.source_plan_item_id,
                "decision_run_id": entry.decision_run_id,
                "decision_snapshot_id": entry.decision_snapshot_id,
                "entry_quantity": 3,
                "linked_by": "short_term_exec_agent_v1",
            },
        }
        session.flush()

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is None
    engine.dispose()


def test_v4_side_drift_cannot_escape_buy_authority_quarantine() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="core-v4-side-drift-quarantine"),
    )
    assert response.status_code == 201
    lease = datetime(2026, 9, 9, 12, 10, tzinfo=UTC)

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = plan.items[0]
        job = session.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == entry.id
            )
        )
        if job is None:
            job = ExecutionJob(
                id="side-drift-buy-job",
                execution_plan_item_id=entry.id,
                job_type="next_open_buy",
            )
            session.add(job)
        entry.side = "sell"
        entry.status = "next_open_buy_dispatching"
        job.status = "running"
        job.locked_at = lease
        entry_id = entry.id
        job_id = job.id
        session.commit()

    with app.state.db_session_factory() as session:
        assert _lock_job_for_broker_submission(
            db=session,
            job_id=job_id,
            expected_locked_at=lease,
        ) is None

    with app.state.db_session_factory() as session:
        entry = session.get(ExecutionPlanItem, entry_id)
        job = session.get(ExecutionJob, job_id)
        assert entry is not None
        assert entry.status == "contract_quarantined"
        assert job is not None
        assert job.status == "revoked"
        assert job.locked_at is None
    engine.dispose()


def test_v4_intake_rejects_us_equity_before_persisting_unsupported_sizing() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-us-equity-plan")
    entry_key = payload["items"][0]["core_item_key"]

    for item in payload["items"]:
        item.update(
            {
                "symbol": "AAPL",
                "asset_class": "us_equity",
                "market": "US",
                "exchange": "NASD",
                "currency": "USD",
                "session_policy": {
                    "session": "regular",
                    "timezone": "America/New_York",
                    "allow_extended_hours": False,
                    "whole_shares_only": True,
                },
            }
        )
    payload["items"][0].update(
        {
            "quantity_policy": {
                "type": "exec_agent_sized",
                "max_quantity": 1,
                "budget_usd": "100",
                "estimated_reference_price": "50.25",
            },
            "limit_price": "50.25",
        }
    )
    payload["items"][1]["quantity_policy"]["linked_entry_item_key"] = entry_key
    payload["items"][1]["trigger_price"] = "60"
    payload["items"][2]["quantity_policy"]["linked_entry_item_key"] = entry_key
    payload["items"][2]["trigger_price"] = "45"
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 422, response.json()
    body = response.json()
    assert body["accepted_item_count"] == 0
    assert body["rejected_item_count"] == 3
    assert body["validation_failures"] == [
        {
            "field": "items",
            "message": "US equity delegated sizing is not supported by this Exec runtime",
            "reason_code": "unsupported_us_equity_exec_agent_sizing",
        }
    ]
    assert client.get(
        "/v4/execution-plans",
        params={"external_plan_id": payload["plan_id"]},
    ).json() == []
    with Session(engine) as db:
        persisted = db.scalar(
            select(ExecutionPlan)
            .where(ExecutionPlan.external_plan_id == payload["plan_id"])
            .options(selectinload(ExecutionPlan.items))
        )
        assert persisted is None
    engine.dispose()


def test_v4_watch_buy_normalization_uses_its_exact_linked_exit_family() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id="core-v4-two-families")

    first_entry_key = payload["items"][0]["core_item_key"]
    payload["items"][1]["trigger_price"] = 120
    payload["items"][2]["trigger_price"] = 90

    second_entry_key = "core-plan-2:next_open_buy"
    payload["items"].extend(
        [
            _item(
                core_item_key=second_entry_key,
                side="buy",
                intent="next_open_buy",
            ),
            _item(
                core_item_key="core-plan-2:target_sell",
                side="sell",
                intent="target_sell",
                linked_entry_item_key=second_entry_key,
            ),
            _item(
                core_item_key="core-plan-2:stop_sell",
                side="sell",
                intent="stop_sell",
                linked_entry_item_key=second_entry_key,
            ),
        ]
    )
    payload["items"][4]["trigger_price"] = 102
    payload["items"][5]["trigger_price"] = 99
    _refresh_material_fingerprint(payload)

    response = client.post("/v4/execution-plans", json=payload)

    assert response.status_code == 201
    with app.state.db_session_factory() as session:
        entries = {
            item.source_plan_item_id: item
            for item in session.scalars(
                select(ExecutionPlanItem).where(ExecutionPlanItem.side == "buy")
            ).all()
        }
        assert Decimal(entries[first_entry_key].quantity_policy["target_price"]) == Decimal("120")
        assert Decimal(entries[first_entry_key].quantity_policy["stop_price"]) == Decimal("90")
        assert Decimal(entries[second_entry_key].quantity_policy["target_price"]) == Decimal("102")
        assert Decimal(entries[second_entry_key].quantity_policy["stop_price"]) == Decimal("99")
    engine.dispose()


def test_v4_intake_rejects_old_schema_missing_capability_and_unknown_fields() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)

    old_schema = _plan_payload(plan_id="old-schema")
    old_schema["metadata"]["schema_version"] = "signal_decider_exec_plan_v3"
    assert client.post("/v4/execution-plans", json=old_schema).status_code == 422

    no_capability = _plan_payload(plan_id="no-capability")
    del no_capability["metadata"]["required_capability"]
    assert client.post("/v4/execution-plans", json=no_capability).status_code == 422

    unknown = _plan_payload(plan_id="unknown-field")
    unknown["metadata"]["legacy_compatibility"] = True
    assert client.post("/v4/execution-plans", json=unknown).status_code == 422
    assert client.get("/v4/execution-plans").json() == []
    engine.dispose()


@pytest.mark.parametrize(
    "payload_patch",
    (
        {"source": "kai-trading-core"},
        {
            "source": "operator-import",
            "metadata": {"schema_version": "signal_decider_exec_plan_v3"},
        },
        {
            "source": "operator-import",
            "metadata": {"contract_version": "execution_handoff_contract_v3"},
        },
        {
            "source": "operator-import",
            "metadata": {"required_capability": "delegated_final_full_cost_gate_recheck_typo"},
        },
    ),
)
def test_legacy_create_and_preview_reject_all_core_provenance_markers(
    payload_patch: dict[str, object],
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload: dict[str, object] = {
        "plan_id": "legacy-core-bypass",
        "source": "operator-import",
        "items": [
            {
                "symbol": "005930",
                "side": "buy",
                "intent": "next_open_buy",
                "quantity_policy": {"kind": "shares", "quantity": 1},
                "time_in_force": "day",
            }
        ],
        **payload_patch,
    }

    assert client.post("/execution-plans/preview", json=payload).status_code == 426
    assert client.post("/execution-plans", json=payload).status_code == 426
    with app.state.db_session_factory() as session:
        assert session.scalars(select(ExecutionPlan)).all() == []
        assert session.scalars(select(ExecutionPlanItem)).all() == []
        assert session.scalars(select(ExecutionJob)).all() == []
    engine.dispose()


def test_legacy_endpoint_still_accepts_genuine_operator_import() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "operator-import-plan",
            "source": "operator-import",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "time_in_force": "day",
                }
            ],
        },
    )

    assert response.status_code == 201
    assert response.json()["accepted_item_count"] == 1
    engine.dispose()


def test_v4_intake_requires_one_exact_target_and_stop_family_per_buy() -> None:
    base = _plan_payload()
    malformed_payloads: list[dict[str, object]] = []

    missing_target = deepcopy(base)
    missing_target["items"] = [
        item for item in missing_target["items"] if item["intent"] != "target_sell"
    ]
    malformed_payloads.append(missing_target)

    orphan_stop = deepcopy(base)
    orphan_stop["items"][2]["quantity_policy"]["linked_entry_item_key"] = "missing-entry"
    malformed_payloads.append(orphan_stop)

    mismatched_target = deepcopy(base)
    mismatched_target["items"][1]["symbol"] = "000660"
    malformed_payloads.append(mismatched_target)

    missing_stop_trigger = deepcopy(base)
    missing_stop_trigger["items"][2]["trigger_price"] = None
    malformed_payloads.append(missing_stop_trigger)

    for index, payload in enumerate(malformed_payloads):
        app, engine = _build_test_app()
        payload["plan_id"] = f"malformed-family-{index}"
        response = TestClient(app).post("/v4/execution-plans", json=payload)
        assert response.status_code == 422
        engine.dispose()


def test_v4_revocation_atomically_revokes_linked_family_and_invalidates_running_lease() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload()
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).order_by(ExecutionJob.id.asc()))
        if job is None:
            job = ExecutionJob(
                id="running-revocation-job",
                execution_plan_item_id=item_acks[0]["execution_plan_item_id"],
                job_type="next_open_buy",
            )
            session.add(job)
        job.status = "running"
        job.locked_at = datetime(2026, 9, 9, 12, 4, tzinfo=UTC)
        session.commit()

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract_ack"] == {
        "schema_version": REVOCATION_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "capability": REVOCATION_CAPABILITY,
        "plan_id": "core-v4-plan",
        "status": "revoked",
    }
    assert {item["core_item_key"] for item in body["item_acks"]} == {
        item["core_item_key"] for item in create_payload["items"]
    }
    assert all(item["status"] == "revoked" for item in body["item_acks"])

    with app.state.db_session_factory() as session:
        items = session.scalars(
            select(ExecutionPlanItem).order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        jobs = session.scalars(select(ExecutionJob)).all()
        audits = session.scalars(select(ExecutionPlanItemAudit)).all()
        assert [item.status for item in items] == ["revoked", "revoked", "revoked"]
        assert jobs
        assert all(job.status == "revoked" and job.locked_at is None for job in jobs)
        assert len(audits) == 3
        assert all(audit.event_type == "core_decision_revoked" for audit in audits)
    engine.dispose()


@pytest.mark.parametrize(
    "reason_code",
    [
        "current_live_market_risk_off",
        "current_live_market_evidence_unavailable",
    ],
)
def test_v4_revocation_accepts_current_live_market_buy_recall_authority(
    reason_code: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload(plan_id="live-risk-off-plan")
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            created.json()["item_acks"],
            reason_code=reason_code,
        ),
    )

    assert response.status_code == 200
    assert response.json()["contract_ack"]["status"] == "revoked"
    with app.state.db_session_factory() as session:
        items = session.scalars(
            select(ExecutionPlanItem).order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        assert [item.status for item in items] == ["revoked", "revoked", "revoked"]
    engine.dispose()


def test_v4_revocation_retires_quarantined_family_with_intact_intake_proof_and_no_exposure() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _activation_bound_plan_payload(
        plan_id="quarantined-no-exposure-plan"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]
    activation_payload = _activation_payload(create_payload, created.json())

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
        }
        target = next(item for item in plan.items if item.intent == "target_sell")
        target.quantity_policy = {
            **target.quantity_policy,
            "linked_entry_item_key": "runtime-drifted-link",
        }
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        session.commit()

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract_ack"]["status"] == "revoked"
    assert body["reason_code"] == "current_live_market_evidence_unavailable"
    assert body["revoked_item_count"] == 3
    assert body["manual_review_item_count"] == 0
    assert all(item["status"] == "revoked" for item in body["item_acks"])
    replay = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )
    assert replay.status_code == 200
    assert replay.json()["contract_ack"]["status"] == "revoked"
    assert replay.json()["idempotent_replay"] is True
    reactivation = client.post(
        "/v4/execution-plans/activations",
        json=activation_payload,
    )
    assert reactivation.status_code == 409
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "revoked"
        assert {item.status for item in plan.items} == {"revoked"}
        assert session.scalars(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type
                == "core_activation_confirmed"
            )
        ).all() == []
    engine.dispose()


def test_v4_revocation_consumes_actual_kis_flat_rebind_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
        kis_account_number="12345678",
        kis_account_product_code="01",
        execution_position_account_scope="primary-kis-live",
        account_drawdown_recovery_scope_secret="s" * 32,
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    account_reference = "KIS:12345678-01"
    create_payload = _plan_payload(plan_id="quarantined-029460-real-flat-proof")
    create_payload["mode"] = "live"
    create_payload["metadata"]["account_reference"] = account_reference
    for item in create_payload["items"]:
        item["symbol"] = "029460"
        item["risk_context"]["account_reference"] = account_reference
    _refresh_material_fingerprint(create_payload)
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]

    reference_at = datetime.now(UTC)
    snapshot_at = reference_at - timedelta(seconds=2)
    event_at = reference_at - timedelta(seconds=1)
    authority_at = reference_at - timedelta(seconds=30)
    historical_orders = (
        (
            "jul-buy",
            "buy",
            "filled",
            6,
            None,
            datetime(2026, 7, 1, 0, 50, tzinfo=UTC),
        ),
        (
            "jul-sell",
            "sell",
            "filled",
            6,
            None,
            datetime(2026, 7, 1, 1, 6, tzinfo=UTC),
        ),
        (
            "aug-buy",
            "buy",
            "filled",
            8,
            "kis",
            datetime(2026, 8, 26, 11, 38, tzinfo=UTC),
        ),
        (
            "aug-expired",
            "buy",
            "reconcile_window_expired",
            8,
            "kis",
            datetime(2026, 8, 27, 8, 50, tzinfo=UTC),
        ),
        (
            "aug-sell",
            "sell",
            "filled",
            8,
            "kis",
            datetime(2026, 8, 27, 9, 1, tzinfo=UTC),
        ),
    )
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        plan.accepted_at = authority_at
        plan.created_at = authority_at
        entry = next(item for item in plan.items if item.side == "buy")
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
        }
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        session.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=entry.id,
                event_type="legacy_core_authority_quarantined",
                source="kai-trading-exec",
                occurred_at=authority_at,
            )
        )
        old_plan = ExecutionPlan(
            id="integration-historical-029460-plan",
            external_plan_id="integration-historical-029460-plan",
            source="kai-trading-exec",
            mode="live",
            status="completed",
            requested_at=datetime(2026, 7, 1, tzinfo=UTC),
            accepted_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
        session.add(old_plan)
        for index, (suffix, side, status, quantity, venue, created_at) in enumerate(
            historical_orders
        ):
            historical_item = ExecutionPlanItem(
                id=f"integration-historical-029460-{suffix}-item",
                execution_plan_id=old_plan.id,
                plan_item_index=index,
                symbol="029460",
                side=side,
                intent="historical_execution",
                status="filled" if status == "filled" else status,
                provider="kis",
                venue=venue,
                asset_class="stock",
            )
            order = BrokerOrder(
                id=f"integration-historical-029460-{suffix}-order",
                execution_plan_item_id=historical_item.id,
                broker_name="kis",
                mode="live",
                idempotency_key=f"integration-historical-029460-{suffix}",
                broker_order_id=f"broker-{suffix}",
                symbol="029460",
                side=side,
                status=status,
                quantity=quantity,
                venue=venue,
                submitted_at=created_at,
                last_broker_sync_at=created_at + timedelta(minutes=1),
                created_at=created_at,
                updated_at=created_at + timedelta(minutes=1),
            )
            session.add_all([historical_item, order])
            if status == "filled":
                session.add(
                    ExecutionFill(
                        broker_order_pk=order.id,
                        broker_fill_id=f"integration-{suffix}-fill",
                        symbol="029460",
                        side=side,
                        quantity=quantity,
                        price=Decimal("35000"),
                        filled_at=created_at,
                    )
                )
        legacy_position = ExecutionPosition(
            id="integration-legacy-flat-029460-position",
            mode="live",
            symbol="029460",
            quantity=0,
            base_quantity=None,
            average_price=Decimal("35800"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 9, 12, tzinfo=UTC),
            raw_payload={"legacy_scope": "unknown"},
        )
        session.add(legacy_position)
        session.commit()

        monkeypatch.setattr(
            position_truth_remediation,
            "ensure_authenticated_broker_session",
            lambda **_kwargs: SimpleNamespace(access_token="shared-kis-token"),
        )
        monkeypatch.setattr(
            position_truth_remediation,
            "_real_utc_now",
            lambda: event_at,
        )
        adapter = _KisFlatSnapshotAdapter(
            _complete_flat_kis_snapshot(observed_at=snapshot_at)
        )
        dry_run = (
            position_truth_remediation.analyze_kis_krx_legacy_flat_position_rebind(
                settings=settings,
                db=session,
                auth_session_factory=app.state.db_session_factory,
                adapter=adapter,
                symbol="029460",
                now=reference_at,
            )
        )
        evidence = dry_run["items"][0]
        applied = (
            position_truth_remediation.apply_kis_krx_legacy_flat_position_rebind(
                settings=settings,
                db=session,
                auth_session_factory=app.state.db_session_factory,
                adapter=adapter,
                symbol="029460",
                approved_evidence={
                    legacy_position.id: evidence["evidence_fingerprint"]
                },
                actor="codex-integration-test",
                reason="prove current flat truth before Core revocation",
                now=reference_at,
            )
        )
        assert applied["summary"]["bound_count"] == 1
        assert applied["summary"]["broker_api_mutation_count"] == 0
        assert len(adapter.snapshot_requests) == 3
        assert session.scalar(select(func.count()).select_from(BrokerOrder)) == 5
        assert session.scalar(select(func.count()).select_from(ExecutionFill)) == 4

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["contract_ack"]["status"] == "revoked"
    assert body["revoked_item_count"] == 3
    assert body["manual_review_item_count"] == 0
    with app.state.db_session_factory() as session:
        rebound = session.get(ExecutionPosition, legacy_position.id)
        assert rebound is not None
        assert rebound.provider == "kis"
        assert rebound.exchange == "KRX"
        assert rebound.quantity == 0
        assert rebound.base_quantity == 0
        assert rebound.average_price is None
        event = session.scalar(
            select(RuntimeRemediationEvent).where(
                RuntimeRemediationEvent.subject_id == rebound.id
            )
        )
        assert event is not None
        assert event.outcome == "current_flat_truth_bound"
    engine.dispose()


@pytest.mark.parametrize(
    "proof_failure",
    [
        None,
        "missing_position",
        "legacy_only",
        "stale_truth",
        "holdings_incomplete",
        "open_orders_incomplete",
        "missing_event",
        "event_digest_mismatch",
        "event_before_snapshot",
        "event_future",
        "event_mutation_fact",
        "event_mutation_bool",
        "configured_account_binding_mismatch",
        "serialization_busy",
        "partial_rebind",
        "canonical_base_missing",
        "post_authority_order",
        "post_authority_fill",
        "current_plan_sibling_order",
        "unlinked_history",
        "unknown_terminal_status",
        "conflicting_venue",
        "provider_venue_post_proof_sync",
        "null_venue_post_proof_sync",
        "positive_alternate_position",
    ],
)
def test_v4_revocation_requires_current_flat_proof_for_terminal_history(
    proof_failure: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="mock",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="s" * 32,
        exec_agent_max_account_snapshot_age_seconds=(
            1 if proof_failure == "stale_truth" else 300
        ),
    )
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    create_payload = _plan_payload(plan_id="quarantined-029460-history")
    for item in create_payload["items"]:
        item["symbol"] = "029460"
    _refresh_material_fingerprint(create_payload)
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]

    historical_orders = (
        (
            "jul-buy",
            "buy",
            "filled",
            6,
            None,
            datetime(2026, 7, 1, 0, 50, tzinfo=UTC),
        ),
        (
            "jul-sell",
            "sell",
            "filled",
            6,
            None,
            datetime(2026, 7, 1, 1, 6, tzinfo=UTC),
        ),
        (
            "aug-buy",
            "buy",
            "filled",
            8,
            "kis",
            datetime(2026, 8, 26, 11, 38, tzinfo=UTC),
        ),
        (
            "aug-expired",
            "buy",
            "reconcile_window_expired",
            8,
            "kis",
            datetime(2026, 8, 27, 8, 50, tzinfo=UTC),
        ),
        (
            "aug-sell",
            "sell",
            "filled",
            8,
            "kis",
            datetime(2026, 8, 27, 9, 1, tzinfo=UTC),
        ),
    )
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        authority_at = datetime.now(UTC) - timedelta(seconds=30)
        plan.accepted_at = authority_at
        plan.created_at = authority_at
        entry = next(item for item in plan.items if item.side == "buy")
        current_sibling = next(item for item in plan.items if item.id != entry.id)
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
        }
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        session.add(
            ExecutionPlanItemAudit(
                execution_plan_item_id=entry.id,
                event_type="legacy_core_authority_quarantined",
                source="kai-trading-exec",
                occurred_at=authority_at,
            )
        )

        old_plan = ExecutionPlan(
            id="historical-029460-plan",
            external_plan_id="historical-029460-plan",
            source="kai-trading-exec",
            mode="mock",
            status="completed",
            requested_at=datetime(2026, 7, 1, tzinfo=UTC),
            accepted_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
        session.add(old_plan)
        for index, (suffix, side, status, quantity, venue, created_at) in enumerate(
            historical_orders
        ):
            effective_status = (
                "stale_terminal_unknown"
                if proof_failure == "unknown_terminal_status"
                and suffix == "aug-expired"
                else status
            )
            effective_created_at = (
                authority_at + timedelta(seconds=1)
                if proof_failure == "post_authority_order" and suffix == "aug-expired"
                else created_at
            )
            effective_venue = (
                "NYSE"
                if proof_failure == "conflicting_venue" and suffix == "aug-sell"
                else venue
            )
            last_broker_sync_at = (
                datetime.now(UTC) + timedelta(seconds=1)
                if (
                    proof_failure == "provider_venue_post_proof_sync"
                    and suffix == "aug-sell"
                )
                or (
                    proof_failure == "null_venue_post_proof_sync"
                    and suffix == "jul-sell"
                )
                else effective_created_at + timedelta(minutes=1)
            )
            historical_item = ExecutionPlanItem(
                id=f"historical-029460-{suffix}-item",
                execution_plan_id=old_plan.id,
                plan_item_index=index,
                symbol="029460",
                side=side,
                intent="historical_execution",
                status=(
                    "filled" if effective_status == "filled" else effective_status
                ),
                provider="kis",
                venue=effective_venue,
                asset_class="stock",
            )
            order = BrokerOrder(
                id=f"historical-029460-{suffix}-order",
                execution_plan_item_id=(
                    None
                    if proof_failure == "unlinked_history"
                    and suffix == "aug-expired"
                    else (
                        current_sibling.id
                        if proof_failure == "current_plan_sibling_order"
                        and suffix == "aug-expired"
                        else historical_item.id
                    )
                ),
                broker_name="kis",
                mode="mock",
                idempotency_key=f"historical-029460-{suffix}",
                broker_order_id=f"broker-{suffix}",
                symbol="029460",
                side=side,
                status=effective_status,
                quantity=quantity,
                venue=effective_venue,
                submitted_at=effective_created_at,
                last_broker_sync_at=last_broker_sync_at,
                created_at=effective_created_at,
                updated_at=effective_created_at + timedelta(minutes=1),
            )
            session.add_all([historical_item, order])
            if status == "filled":
                session.add(
                    ExecutionFill(
                        broker_order_pk=order.id,
                        broker_fill_id=f"historical-029460-{suffix}-fill",
                        symbol="029460",
                        side=side,
                        quantity=quantity,
                        price=Decimal("35000"),
                        filled_at=(
                            authority_at + timedelta(seconds=1)
                            if proof_failure == "post_authority_fill"
                            and suffix == "aug-buy"
                            else created_at
                        ),
                    )
                )

        proof_at = (
            datetime.now(UTC) - timedelta(seconds=5)
            if proof_failure == "stale_truth"
            else datetime.now(UTC) + timedelta(microseconds=1)
        )
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
        configured_account_binding_digest = (
            kis_krx_flat_configured_account_binding_digest(settings=settings)
        )
        persisted_account_binding_digest = (
            "a" * 64
            if proof_failure == "configured_account_binding_mismatch"
            else configured_account_binding_digest
        )
        absence_digest = kis_krx_flat_absence_proof_digest(
            runtime_mode="mock",
            identity=identity,
            symbol="029460",
            snapshot_source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holding_absent=True,
            open_order_absent=True,
            configured_account_binding_digest=persisted_account_binding_digest,
        )
        prior_position_digest = "b" * 64
        evidence_fingerprint = kis_krx_flat_evidence_fingerprint(
            runtime_mode="mock",
            position_id="current-flat-029460-position",
            symbol="029460",
            prior_position_digest=prior_position_digest,
            identity=identity,
            snapshot_source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holding_absent=True,
            open_order_absent=True,
            absence_proof_digest=absence_digest,
            configured_account_binding_digest=persisted_account_binding_digest,
        )
        holdings_complete = proof_failure != "holdings_incomplete"
        open_orders_complete = proof_failure != "open_orders_incomplete"
        if proof_failure in {"legacy_only", "missing_position"}:
            if proof_failure == "legacy_only":
                session.add(
                    ExecutionPosition(
                        id="legacy-flat-029460-position",
                        mode="mock",
                        symbol="029460",
                        quantity=0,
                        updated_from="reconciliation",
                        last_reconciled_at=datetime(2026, 9, 12, tzinfo=UTC),
                        raw_payload={"legacy_scope": "unknown"},
                    )
                )
            session.commit()
            assert execution_exposure.item_has_external_possible_exposure(
                db=session,
                plan=plan,
                item=entry,
                settings=settings,
            ) is True
            assert core_revocation_exposure.item_has_revocation_possible_exposure(
                db=session,
                plan=plan,
                item=entry,
                settings=settings,
            ) is True
        else:
            position = ExecutionPosition(
                id="current-flat-029460-position",
                mode="mock",
                symbol="029460",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                quantity=0,
                base_quantity=(
                    None
                    if proof_failure == "canonical_base_missing"
                    else Decimal("0")
                ),
                average_price=(
                    Decimal("35000") if proof_failure == "partial_rebind" else None
                ),
                updated_from="reconciliation",
                last_reconciled_at=proof_at,
                raw_payload=payload_with_position_identity(
                    {
                        "provider": "kis",
                        "mode": "mock",
                        "asset_class": "equity",
                        "exchange": "KRX",
                        "snapshot_source": "kis:account_snapshot",
                        "snapshot_observed_at": proof_at.isoformat(),
                        "holdings_coverage": (
                            "complete" if holdings_complete else "incomplete"
                        ),
                        "open_orders_coverage": (
                            "complete" if open_orders_complete else "incomplete"
                        ),
                        "holding_absent": holdings_complete,
                        "open_order_absent": open_orders_complete,
                        "historical_account_attributed": False,
                        "current_flat_truth_bound": True,
                        "absence_proof_digest": absence_digest,
                        "configured_account_binding_digest": (
                            persisted_account_binding_digest
                        ),
                        "safety_policy_version": (
                            KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
                        ),
                    },
                    identity=identity,
                ),
            )
            session.add(position)
            if proof_failure == "positive_alternate_position":
                session.add(
                    ExecutionPosition(
                        id="alternate-positive-029460-position",
                        mode="mock",
                        symbol="029460",
                        provider="kis",
                        account_scope="f" * 24,
                        exchange="KRX",
                        quantity=1,
                        base_quantity=Decimal("0"),
                        updated_from="reconciliation",
                    )
                )
            if proof_failure != "missing_event":
                session.add(
                    RuntimeRemediationEvent(
                        mode="mock",
                        candidate_type="kis_krx_legacy_flat_position_rebind",
                        candidate_key=position.id,
                        subject_id=position.id,
                        outcome="current_flat_truth_bound",
                        actor="test",
                        reason="bind only current broker-flat truth",
                        occurred_at=(
                            proof_at - timedelta(seconds=1)
                            if proof_failure == "event_before_snapshot"
                            else (
                                proof_at + timedelta(hours=1)
                                if proof_failure == "event_future"
                                else proof_at
                            )
                        ),
                        evidence_summary={
                            "symbol": "029460",
                            "provider": identity.provider,
                            "account_scope": identity.account_scope,
                            "exchange": identity.exchange,
                            "asset_class": "equity",
                            "broker_snapshot_source": "kis:account_snapshot",
                            "broker_snapshot_observed_at": proof_at.isoformat(),
                            "holdings_complete": holdings_complete,
                            "open_orders_complete": open_orders_complete,
                            "holding_absent": holdings_complete,
                            "open_order_absent": open_orders_complete,
                            "historical_account_attributed": False,
                            "current_flat_truth_bound": True,
                            "absence_proof_digest": (
                                "d" * 64
                                if proof_failure == "event_digest_mismatch"
                                else absence_digest
                            ),
                            "configured_account_binding_digest": (
                                persisted_account_binding_digest
                            ),
                            "prior_position_digest": prior_position_digest,
                            "evidence_fingerprint": evidence_fingerprint,
                            "safety_policy_version": (
                                KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
                            ),
                            "broker_api_mutations": (
                                False
                                if proof_failure == "event_mutation_bool"
                                else 0
                            ),
                            "orders_submitted": (
                                1 if proof_failure == "event_mutation_fact" else 0
                            ),
                            "orders_amended": 0,
                            "orders_canceled": 0,
                            "fills_created": 0,
                            "fills_deleted": 0,
                            "pnl_records_mutated": 0,
                            "strategy_records_mutated": 0,
                        },
                    )
                )
            session.commit()
            assert execution_exposure.item_has_external_possible_exposure(
                db=session,
                plan=plan,
                item=entry,
                settings=settings,
            ) is True
            if proof_failure == "serialization_busy":
                @contextmanager
                def raise_serialization_busy(
                    **_kwargs: object,
                ) -> Iterator[None]:
                    raise core_revocation_exposure.RevocationExposureSerializationBusy
                    yield

                monkeypatch.setattr(
                    core_handoff_v4,
                    "serialized_revocation_exposure",
                    raise_serialization_busy,
                )
            else:
                assert core_revocation_exposure.item_has_revocation_possible_exposure(
                    db=session,
                    plan=plan,
                    item=entry,
                    settings=settings,
                    evaluated_at=datetime.now(UTC),
                ) is (proof_failure is not None)

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    if proof_failure == "serialization_busy":
        assert response.status_code == 409
        body = response.json()
        assert body["contract_ack"]["status"] == "blocked"
        assert body["reason_code"] == "revocation_exposure_scope_busy"
        with app.state.db_session_factory() as session:
            persisted = session.scalar(
                select(ExecutionPlan).where(
                    ExecutionPlan.external_plan_id == create_payload["plan_id"]
                )
            )
            assert persisted is not None
            assert persisted.status == "contract_quarantined"
        engine.dispose()
        return

    if proof_failure is not None:
        assert response.status_code == 409
        body = response.json()
        assert body["contract_ack"]["status"] == "manual_review_required"
        assert body["reason_code"] == "broker_commit_or_exposure_possible"
        assert body["revoked_item_count"] == 0
        assert body["manual_review_item_count"] == 1
        engine.dispose()
        return

    assert response.status_code == 200
    body = response.json()
    assert body["contract_ack"]["status"] == "revoked"
    assert body["manual_review_item_count"] == 0
    assert body["revoked_item_count"] == 3
    replay = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )
    assert replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    engine.dispose()


@pytest.mark.parametrize("proof_failure", ["identity_drift", "binding_missing"])
def test_v4_revocation_refuses_quarantined_family_without_intact_intake_proof(
    proof_failure: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload(
        plan_id=f"quarantined-intake-proof-{proof_failure}"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        if proof_failure == "identity_drift":
            entry.symbol = "000660"
        else:
            risk_context = dict(entry.risk_context or {})
            risk_context.pop(core_handoff_v4.CORE_V4_PERSISTED_BINDING_KEY)
            entry.risk_context = risk_context
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        session.commit()

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["contract_ack"]["status"] == "manual_review_required"
    assert body["reason_code"] == "persisted_v4_contract_not_proven"
    assert body["revoked_item_count"] == 0
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "contract_quarantined"
        assert any(item.status == "contract_quarantined" for item in plan.items)
        assert all(item.status != "revoked" for item in plan.items)
    engine.dispose()


@pytest.mark.parametrize(
    "exposure_kind",
    [
        "linked_unsubmitted_order",
        "protective_unsubmitted_order",
        "linked_fill",
        "unlinked_active_order",
        "scoped_position",
        "alternate_scoped_position",
        "manual_position",
        "scope_unproven",
    ],
)
def test_v4_revocation_keeps_quarantined_family_in_manual_review_when_exposure_is_possible(
    exposure_kind: str,
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="mock")
    app, engine = _build_test_app(settings=settings)
    client = TestClient(app)
    create_payload = _plan_payload(
        plan_id=f"quarantined-exposure-{exposure_kind}"
    )
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    item_acks = created.json()["item_acks"]
    entry_ack = item_acks[0]

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        entry = next(item for item in plan.items if item.side == "buy")
        entry.quantity_policy = {
            **entry.quantity_policy,
            "kind": "shares",
            "quantity": 999,
            "source": "unproven-sizing",
        }
        entry.status = "contract_quarantined"
        plan.status = "contract_quarantined"
        if exposure_kind == "scope_unproven":
            monkeypatch.setattr(
                core_handoff_v4,
                "_item_exposure_scope_is_proven",
                lambda **_kwargs: False,
            )
        elif exposure_kind in {
            "manual_position",
            "scoped_position",
            "alternate_scoped_position",
        }:
            identity = build_position_identity(
                settings=settings,
                provider="kis",
                exchange="KRX",
            )
            session.add(
                ExecutionPosition(
                    mode="mock",
                        provider=(
                            identity.provider
                            if exposure_kind
                            in {"scoped_position", "alternate_scoped_position"}
                            else None
                    ),
                    account_scope=(
                        "f" * 24
                        if exposure_kind == "alternate_scoped_position"
                        else (
                            identity.account_scope
                            if exposure_kind == "scoped_position"
                            else None
                        )
                    ),
                        exchange=(
                            identity.exchange
                            if exposure_kind
                            in {"scoped_position", "alternate_scoped_position"}
                            else None
                    ),
                    symbol=entry.symbol,
                    quantity=1,
                    updated_from="operator_manual",
                )
            )
        else:
            order_item_id = entry.id
            if exposure_kind == "protective_unsubmitted_order":
                order_item_id = next(
                    item.id for item in plan.items if item.intent == "stop_sell"
                )
            elif exposure_kind == "unlinked_active_order":
                order_item_id = None
            order = BrokerOrder(
                id=f"quarantined-{exposure_kind}-order",
                execution_plan_item_id=order_item_id,
                broker_name="kis",
                mode="mock",
                idempotency_key=f"quarantined-{exposure_kind}",
                symbol=entry.symbol,
                side="buy",
                status=(
                    "open" if exposure_kind == "unlinked_active_order" else "failed"
                ),
                quantity=1,
                venue="KRX",
            )
            session.add(order)
            if exposure_kind == "linked_fill":
                session.flush()
                session.add(
                    ExecutionFill(
                        broker_order_pk=order.id,
                        broker_fill_id="quarantined-linked-fill",
                        symbol=entry.symbol,
                        side="buy",
                        quantity=1,
                        price=Decimal("71000"),
                        filled_at=datetime(2026, 9, 9, 12, 3, tzinfo=UTC),
                    )
                )
        session.commit()

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["contract_ack"]["status"] == "manual_review_required"
    assert body["reason_code"] == "broker_commit_or_exposure_possible"
    assert body["revoked_item_count"] == 0
    assert body["manual_review_item_count"] == 1
    assert next(
        item
        for item in body["item_acks"]
        if item["execution_plan_item_id"] == entry_ack["execution_plan_item_id"]
    )["status"] == "manual_review_required"
    assert {
        item["status"]
        for item in body["item_acks"]
        if item["execution_plan_item_id"] != entry_ack["execution_plan_item_id"]
    } == {"preserved_protective_exit"}
    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).options(selectinload(ExecutionPlan.items))
        )
        assert plan is not None
        assert plan.status == "manual_review_required"
        entry = next(item for item in plan.items if item.side == "buy")
        assert entry.status == "manual_review_required"
        assert all(item.status != "revoked" for item in plan.items)
    engine.dispose()


def test_v4_revocation_contract_proof_failure_keeps_exact_plan_identity(
    monkeypatch,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload(plan_id="revocation-contract-proof-plan")
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    created_body = created.json()

    monkeypatch.setattr(
        core_handoff_v4,
        "persisted_core_plan_contract_ack",
        lambda _plan: None,
    )
    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            created_body["item_acks"],
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["reason_code"] == "persisted_v4_contract_not_proven"
    assert body["execution_plan_id"] == created_body["execution_plan_id"]
    assert all(
        item["execution_plan_id"] == created_body["execution_plan_id"]
        for item in body["item_acks"]
    )
    assert {
        item["core_item_key"]: item["execution_plan_item_id"]
        for item in body["item_acks"]
    } == {
        item["core_item_key"]: item["execution_plan_item_id"]
        for item in created_body["item_acks"]
    }
    engine.dispose()


def test_v4_revocation_binding_failure_keeps_exact_plan_identity() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload(plan_id="revocation-binding-failure-plan")
    created = client.post("/v4/execution-plans", json=create_payload)
    assert created.status_code == 201
    created_body = created.json()
    revocation_payload = _revocation_payload(
        create_payload,
        created_body["item_acks"],
        reason_code="current_live_market_evidence_unavailable",
    )
    revocation_payload["items"][0]["execution_plan_item_id"] = "wrong-item-id"

    response = client.post(
        "/v4/execution-plans/revocations",
        json=revocation_payload,
    )

    assert response.status_code == 409
    body = response.json()
    assert body["reason_code"] == "revocation_item_binding_mismatch"
    assert body["execution_plan_id"] == created_body["execution_plan_id"]
    assert all(
        item["execution_plan_id"] == created_body["execution_plan_id"]
        for item in body["item_acks"]
    )
    assert body["item_acks"][0]["execution_plan_item_id"] == "wrong-item-id"
    engine.dispose()


@pytest.mark.parametrize("broker_status", ["submitted", "canceled"])
def test_v4_revocation_refuses_and_flags_manual_review_after_broker_order_exists(
    broker_status: str,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    create_payload = _plan_payload(plan_id="too-late-plan")
    created = client.post("/v4/execution-plans", json=create_payload)
    item_acks = created.json()["item_acks"]
    entry_ack = item_acks[0]

    with app.state.db_session_factory() as session:
        for index, ack in enumerate(item_acks):
            session.add(
                ExecutionJob(
                    id=f"too-late-job-{index}",
                    execution_plan_item_id=ack["execution_plan_item_id"],
                    job_type=f"test-{index}",
                    status="running" if index == 0 else "queued",
                    locked_at=(
                        datetime(2026, 9, 9, 12, 4, tzinfo=UTC)
                        if index == 0
                        else None
                    ),
                )
            )
        session.add(
            BrokerOrder(
                id="committed-order",
                execution_plan_item_id=entry_ack["execution_plan_item_id"],
                broker_name="kis",
                mode="mock",
                idempotency_key="committed-order",
                symbol="005930",
                side="buy",
                status=broker_status,
                quantity=1,
            )
        )
        session.commit()

    response = client.post(
        "/v4/execution-plans/revocations",
        json=_revocation_payload(
            create_payload,
            item_acks,
            reason_code="current_live_market_evidence_unavailable",
        ),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["contract_ack"]["status"] == "manual_review_required"
    assert body["item_acks"][0]["status"] == "manual_review_required"
    assert {item["status"] for item in body["item_acks"][1:]} == {
        "preserved_protective_exit"
    }
    assert body["reason_code"] == "broker_commit_or_exposure_possible"
    with app.state.db_session_factory() as session:
        items = session.scalars(
            select(ExecutionPlanItem).order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        jobs = session.scalars(
            select(ExecutionJob).order_by(ExecutionJob.id.asc())
        ).all()
        assert items[0].status == "manual_review_required"
        assert all(item.status != "revoked" for item in items[1:])
        buy_job = next(
            job
            for job in jobs
            if job.execution_plan_item_id == items[0].id
        )
        protective_jobs = [
            job
            for job in jobs
            if job.execution_plan_item_id in {items[1].id, items[2].id}
        ]
        assert buy_job.status == "revoked"
        assert buy_job.locked_at is None
        assert all(job.status == "queued" for job in protective_jobs)
    engine.dispose()


def test_v4_contract_proof_failure_rolls_back_every_plan_item_and_job(
    monkeypatch,
) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    monkeypatch.setattr(
        core_handoff_v4,
        "persisted_core_plan_contract_ack",
        lambda _plan: None,
    )

    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="proof-failure-plan"),
    )

    assert response.status_code == 500
    with app.state.db_session_factory() as session:
        assert session.scalars(select(ExecutionPlan)).all() == []
        assert session.scalars(select(ExecutionPlanItem)).all() == []
        assert session.scalars(select(ExecutionJob)).all() == []
    engine.dispose()


def test_v4_plan_is_invisible_to_concurrent_claimers_until_contract_proof(
    monkeypatch,
    tmp_path,
) -> None:
    app, engine = _build_test_app(
        database_url=f"sqlite:///{tmp_path / 'v4-staging-visibility.db'}",
    )
    client = TestClient(app)
    observed_counts: list[tuple[int, int, int]] = []
    original_proof = core_handoff_v4.persisted_core_plan_contract_ack

    def prove_while_observing(plan):
        with app.state.db_session_factory() as observer:
            observed_counts.append(
                (
                    len(observer.scalars(select(ExecutionPlan)).all()),
                    len(observer.scalars(select(ExecutionPlanItem)).all()),
                    len(observer.scalars(select(ExecutionJob)).all()),
                )
            )
        return original_proof(plan)

    monkeypatch.setattr(
        core_handoff_v4,
        "persisted_core_plan_contract_ack",
        prove_while_observing,
    )

    response = client.post(
        "/v4/execution-plans",
        json=_plan_payload(plan_id="atomic-staging-plan"),
    )

    assert response.status_code == 201
    assert observed_counts == [(0, 0, 0)]
    with app.state.db_session_factory() as session:
        assert len(session.scalars(select(ExecutionPlan)).all()) == 1
        assert len(session.scalars(select(ExecutionPlanItem)).all()) == 3
    engine.dispose()


def test_legacy_plan_creation_still_invalidates_runtime_snapshot(monkeypatch) -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    invalidations: list[str] = []
    monkeypatch.setattr(
        app.state.autonomous_runtime,
        "invalidate_monitoring_runtime_snapshot",
        lambda *, reason: invalidations.append(reason),
    )

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "legacy-non-core-plan",
            "source": "operator-import",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                }
            ],
        },
    )

    assert response.status_code == 201
    assert invalidations == ["execution_plan_created"]
    engine.dispose()


def test_legacy_core_buy_job_is_quarantined_while_protective_exit_is_preserved() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    lease = datetime(2026, 9, 9, 12, 10, tzinfo=UTC)
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            id="legacy-core-plan-row",
            external_plan_id="legacy-core-plan",
            source="kai-trading-core",
            mode="mock",
            status="accepted",
            raw_payload={
                "plan_id": "legacy-core-plan",
                "source": "kai-trading-core",
                "metadata": {
                    "schema_version": "signal_decider_exec_plan_v3",
                    "contract_version": "execution_handoff_contract_v3",
                },
            },
        )
        buy = ExecutionPlanItem(
            id="legacy-core-buy",
            execution_plan_id=plan.id,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_dispatching",
            quantity_policy={"kind": "shares", "quantity": 1},
            source_plan_item_id="legacy:buy",
        )
        protective = ExecutionPlanItem(
            id="legacy-core-stop",
            execution_plan_id=plan.id,
            plan_item_index=1,
            symbol="005930",
            side="sell",
            intent="stop_sell",
            status="monitoring",
            quantity_policy={"kind": "position"},
            source_plan_item_id="legacy:stop",
        )
        session.add_all([plan, buy, protective])
        session.flush()
        session.add_all(
            [
                ExecutionJob(
                    id="legacy-core-buy-job",
                    execution_plan_item_id=buy.id,
                    job_type="next_open_buy",
                    status="running",
                    locked_at=lease,
                ),
                ExecutionJob(
                    id="legacy-core-stop-job",
                    execution_plan_item_id=protective.id,
                    job_type="stop_sell",
                    status="queued",
                ),
            ]
        )
        session.commit()

    dispatched = client.post("/execution-jobs/dispatch")
    assert dispatched.status_code == 200
    assert dispatched.json()["due_job_count"] == 0

    with app.state.db_session_factory() as session:
        assert _lock_job_for_broker_submission(
            db=session,
            job_id="legacy-core-buy-job",
            expected_locked_at=lease,
        ) is None

    with app.state.db_session_factory() as session:
        buy = session.get(ExecutionPlanItem, "legacy-core-buy")
        protective = session.get(ExecutionPlanItem, "legacy-core-stop")
        buy_job = session.get(ExecutionJob, "legacy-core-buy-job")
        protective_job = session.get(ExecutionJob, "legacy-core-stop-job")
        plan = session.get(ExecutionPlan, "legacy-core-plan-row")
        assert buy.status == "contract_quarantined"
        assert buy_job.status == "revoked"
        assert buy_job.locked_at is None
        assert protective.status == "monitoring"
        assert protective_job.status == "queued"
        assert plan.status == "contract_quarantined"
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == buy.id,
                ExecutionPlanItemAudit.event_type
                == "legacy_core_authority_quarantined",
            )
        )
        assert audit is not None
    engine.dispose()
