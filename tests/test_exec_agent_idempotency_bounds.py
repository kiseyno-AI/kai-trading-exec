"""Accepted plan identities must produce durable, replayable order keys."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.api.schemas.execution import ExecutionPlanRequest
from app.db.session import create_session_factory
from app.models.runtime import BrokerOrder, ExecutionJob, ExecutionPlan, ExecutionPlanItem
from app.services import broker_orders, exec_agent, execution_jobs
from test_reconciliation_concurrency import reconciliation_database


@pytest.mark.parametrize(("plan_length", "source_length"), [
    (8, 8), (8, 128 - len("short-term-exec-agent:") - 8 - 1),
    (128, 8), (8, 128), (128, 128),
])
def test_generated_order_key_survives_dispatch_storage_and_replay(
    reconciliation_database, plan_length, source_length,
):
    settings, engine = reconciliation_database
    request = ExecutionPlanRequest(
        plan_id="p" * plan_length, source="test", market_date="2026-11-09",
        items=[{
            "symbol": "005930", "side": "buy", "intent": "next_open_buy",
            "source_plan_item_id": "s" * source_length,
        }],
    )
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id=request.plan_id, source=request.source,
            market_date=request.market_date, mode=settings.runtime_mode,
        )
        item = ExecutionPlanItem(
            plan=plan, plan_item_index=0, symbol="005930", side="buy",
            intent="next_open_buy", source_plan_item_id=request.items[0].source_plan_item_id,
            provider="kis", venue="KRX", asset_class="stock",
        )
        db.add(plan)
        db.flush()
        key = exec_agent._candidate_idempotency_key(plan=plan, item=item)
        original = f"short-term-exec-agent:{plan.external_plan_id}:{item.source_plan_item_id}"
        if len(original) <= 128:
            assert key == original
        exec_agent._apply_approved_decision(settings=settings, item=item, decision={
            "version": exec_agent.EXEC_AGENT_DECISION_VERSION, "action": "approve",
            "reason_code": "approved", "execution_plan_item_id": item.id,
            "idempotency_key": key, "quantity": 2, "sizing_price": "50",
            "reference_price": "50", "approved_notional": "100",
        })
        job = ExecutionJob(plan_item=item, job_type="next_open_buy", status="scheduled")
        db.add(job)
        db.commit()
        command = execution_jobs._build_place_order_request(
            settings=settings, db=db, adapter=SimpleNamespace(provider="kis"), job=job,
        )
        assert broker_orders._verified_exec_agent_market_notional(
            command=command, plan_item=item,
        ) == Decimal("100")
        db.add(BrokerOrder(
            plan_item=item, mode=settings.runtime_mode, broker_name="kis",
            idempotency_key=command.idempotency_key, symbol="005930", side="buy",
            status="submitted", quantity=command.quantity,
        ))
        db.commit()
        job_id = job.id

    with session_factory() as db:
        job = db.get(ExecutionJob, job_id)
        regenerated = exec_agent._candidate_idempotency_key(plan=job.plan_item.plan, item=job.plan_item)
        assert regenerated == key
        assert execution_jobs._place_order_idempotency_key(job) == key
        assert execution_jobs._load_order_status(settings=settings, db=db, idempotency_key=key) == "submitted"
        different = ExecutionPlanItem(source_plan_item_id="s" * (source_length - 1) + "t")
        assert exec_agent._candidate_idempotency_key(plan=job.plan_item.plan, item=different) != key
