from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import (
    CoreExecutionPlanRequestV4,
    _SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY,
    core_execution_plan_material_fingerprint,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
)
from app.services import core_handoff_v4


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

    duplicate = client.post("/v4/execution-plans", json=v4_payload)
    assert duplicate.status_code == 409
    assert "contract_ack" not in duplicate.json()
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
            "watch_monitoring_pending"
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
        assert buy.status == "watch_monitoring_pending"
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


def test_v4_contract_proof_allows_exact_exec_agent_sizing_through_job_claim() -> None:
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
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
        assert _lock_job_for_broker_submission(
            db=session,
            job_id="core-v4-exact-agent-sizing-job",
            expected_locked_at=lease,
        ) is not None
    engine.dispose()


def test_v4_watch_sizing_preserves_contract_proof_through_job_claim() -> None:
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
        assert len(entry.jobs) == 1
        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
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


def test_v4_contract_proof_allows_exact_short_term_linked_exit_transform() -> None:
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

        assert core_handoff_v4.persisted_core_plan_contract_ack(plan) is not None
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
