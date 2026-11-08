"""Generated monitor identities must fit storage without losing entry lineage."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.api.schemas.execution import ExecutionPlanItemRequest
from app.db.session import create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services import exec_agent, monitoring
from app.services.execution_account_route import bind_kis_account_route
from app.services.plan_item_identity import (
    plan_item_entry_family_key,
    select_plan_item_family_members,
)
from test_reconciliation_concurrency import reconciliation_database


EXIT_STAGES = (
    ("target_sell", "first", Decimal("52000")),
    ("target_sell", "final", Decimal("54000")),
    ("stop_sell", "hard_stop_loss", Decimal("47000")),
    ("max_holding_close_sell", "time_stop", None),
)


@pytest.mark.parametrize("intent,stage,_price", EXIT_STAGES)
def test_existing_generated_monitor_ids_remain_exact(intent, stage, _price):
    suffix = f"{intent}:{stage}"
    for length in range(1, 128 - len(suffix)):
        item = ExecutionPlanItem(source_plan_item_id="x" * length)
        assert exec_agent._monitoring_source_plan_item_id(item, suffix) == f"{item.source_plan_item_id}:{suffix}"
    assert exec_agent._monitoring_source_plan_item_id(ExecutionPlanItem(), suffix) is None


@pytest.mark.parametrize("source_length", [8, 128])
def test_generated_monitors_persist_and_retain_family_on_replay(
    reconciliation_database, source_length,
):
    settings, engine = reconciliation_database
    settings = settings.model_copy(update={
        "kis_account_number": "12345678",
        "account_drawdown_recovery_scope_secret": "monitor-identity-test-secret",
    })
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="monitor-identities", source="test", mode="paper",
            market_date=date(2026, 11, 9), status="accepted",
        )
        entries = []
        for index, final_character in enumerate(("x", "y")):
            source = "x" * (source_length - 1) + final_character
            incoming = ExecutionPlanItemRequest(
                symbol="005930", side="buy", intent="next_open_buy",
                source_plan_item_id=source,
            )
            risk, policy = bind_kis_account_route(
                settings=settings, provider="kis", risk_context={"exchange": "KRX"},
                quantity_policy={"kind": "shares", "quantity": 2},
            )
            entries.append(ExecutionPlanItem(
                plan=plan, plan_item_index=index, symbol=incoming.symbol,
                side="buy", intent=incoming.intent, status="filled",
                source_plan_item_id=source, decision_agent_name="short-term-decider",
                provider="kis", venue="KRX", asset_class="stock",
                risk_context=risk, quantity_policy=policy,
            ))
        db.add(plan)
        db.flush()
        monitors = []
        for entry in entries:
            for intent, stage, price in EXIT_STAGES:
                monitor = exec_agent._new_monitoring_item(
                    plan=plan, buy_item=entry, decision={"idempotency_key": "local-approval"},
                    intent=intent, quantity=2, next_plan_item_index=2 + len(monitors),
                    trigger_price=price, max_sessions=2, exit_rule={"stage": stage},
                )
                plan.items.append(monitor)
                monitors.append(monitor)
        db.commit()
        plan_id = plan.id
        original_keys = {
            monitor.id: monitoring._monitoring_order_idempotency_key(
                item=monitor, is_watch_buy=False, retry_source_order_pk=None,
            ) for monitor in monitors
        }
        assert len({monitor.source_plan_item_id for monitor in monitors}) == 8
        assert all(len(monitor.source_plan_item_id) <= 128 for monitor in monitors)

    with session_factory() as db:
        plan = db.get(ExecutionPlan, plan_id)
        exits = [item for item in plan.items if item.side == "sell"]
        for entry in (item for item in plan.items if item.side == "buy"):
            family = select_plan_item_family_members(anchor=entry, candidates=exits)
            assert len(family) == 4
            for monitor in family:
                assert plan_item_entry_family_key(monitor) == f"source:{entry.source_plan_item_id}"
                assert monitor.quantity_policy["linked_entry_item_key"] == entry.source_plan_item_id
                assert monitor.risk_context["entry_context"]["source_plan_item_id"] == entry.source_plan_item_id
                assert monitor.source_plan_item_id == exec_agent._monitoring_source_plan_item_id(
                    entry, f"{monitor.intent}:{monitor.risk_context['exit_rule']['stage']}",
                )
                assert monitoring._monitoring_order_idempotency_key(
                    item=monitor, is_watch_buy=False, retry_source_order_pk=None,
                ) == original_keys[monitor.id]
            repaired, _changed = monitoring._ensure_post_entry_stop_monitor(
                db=db, entry_item=entry, quantity=2, stop_price=Decimal("47000"),
                entry_price=Decimal("50000"), entry_market_date=plan.market_date,
                entry_market_date_source="fill", entry_market_date_status="verified",
                entry_market_date_precision="exact", triggered_at=datetime(2026, 11, 9, tzinfo=UTC),
            )
            assert repaired.id == next(item.id for item in family if item.intent == "stop_sell")
        db.commit()
        assert db.scalar(select(func.count()).select_from(ExecutionPlanItem)) == 10
