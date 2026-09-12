from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from app.api.schemas.execution import PlaceOrderRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.models.runtime import ExecutionPosition
from app.services import broker_orders
from app.services import monitoring
from app.services import short_term_exec_agent
from app.services import trigger_time_exec_agent
from app.services.plan_item_identity import (
    plan_item_entry_family_key,
    select_plan_item_family_members,
)
from app.services.trigger_time_exec_agent import TriggerTimeExecAgentDecision


def _entry(*, item_id: str, source_key: str) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        source_plan_item_id=source_key,
    )


def _exit(
    *,
    item_id: str,
    source_key: str,
    linked_entry_key: str | None,
    intent: str = "stop_sell",
    trigger_price: str = "90",
) -> ExecutionPlanItem:
    policy: dict[str, object] = {"kind": "shares", "quantity": 1}
    if linked_entry_key is not None:
        policy["linked_entry_item_key"] = linked_entry_key
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=1,
        symbol="005930",
        side="sell",
        intent=intent,
        status=f"{intent}_monitoring",
        source_plan_item_id=source_key,
        quantity_policy=policy,
        trigger_price=Decimal(trigger_price),
    )


def test_exact_family_selection_never_returns_another_same_symbol_exit() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
    )
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
    )
    ExecutionPlan(
        external_plan_id="two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_stop, second, second_stop],
    )

    assert plan_item_entry_family_key(second_stop) == "source:family-2:entry"
    assert select_plan_item_family_members(
        anchor=second,
        candidates=[first_stop, second_stop],
    ) == [second_stop]


def test_ambiguous_unlinked_same_symbol_exits_fail_closed() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    legacy_stop = _exit(
        item_id="legacy-stop",
        source_key="legacy-stop",
        linked_entry_key=None,
    )
    ExecutionPlan(
        external_plan_id="ambiguous-legacy",
        source="operator-import",
        mode="paper",
        status="accepted",
        items=[first, second, legacy_stop],
    )

    assert select_plan_item_family_members(
        anchor=second,
        candidates=[legacy_stop],
    ) == []


def test_persisted_ambiguous_legacy_family_does_not_bypass_lazy_plan_loading() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    legacy_stop = _exit(
        item_id="legacy-stop",
        source_key="legacy-stop",
        linked_entry_key=None,
    )
    db.add(
        ExecutionPlan(
            external_plan_id="persisted-ambiguous-legacy",
            source="operator-import",
            mode="paper",
            status="accepted",
            items=[first, second, legacy_stop],
        )
    )
    db.commit()
    db.expunge_all()
    loaded_second = db.get(ExecutionPlanItem, second.id)
    loaded_stop = db.get(ExecutionPlanItem, legacy_stop.id)
    assert loaded_second is not None
    assert loaded_stop is not None

    assert select_plan_item_family_members(
        anchor=loaded_second,
        candidates=[loaded_stop],
    ) == []
    db.close()
    engine.dispose()


def test_post_entry_context_proves_derived_exit_family() -> None:
    entry = _entry(item_id="entry-1", source_key="family-1:entry")
    derived_stop = _exit(
        item_id="derived-stop",
        source_key="derived-stop",
        linked_entry_key=None,
    )
    derived_stop.risk_context = {
        "entry_context": {
            "source_plan_item_id": entry.source_plan_item_id,
            "execution_plan_item_id": entry.id,
        }
    }
    ExecutionPlan(
        external_plan_id="derived-family",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, derived_stop],
    )

    assert plan_item_entry_family_key(derived_stop) == "source:family-1:entry"
    assert select_plan_item_family_members(
        anchor=entry,
        candidates=[derived_stop],
    ) == [derived_stop]


def test_trigger_time_adjustment_mutates_only_the_exact_exit_family() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_target = _exit(
        item_id="target-1",
        source_key="family-1:target",
        linked_entry_key=first.source_plan_item_id,
        intent="target_sell",
        trigger_price="120",
    )
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
        trigger_price="90",
    )
    second_target = _exit(
        item_id="target-2",
        source_key="family-2:target",
        linked_entry_key=second.source_plan_item_id,
        intent="target_sell",
        trigger_price="102",
    )
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="99",
    )
    ExecutionPlan(
        external_plan_id="trigger-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_target, first_stop, second, second_target, second_stop],
    )
    decision = TriggerTimeExecAgentDecision(
        version="trigger-time-exec-agent-v1",
        plan_id="trigger-two-families",
        execution_plan_item_id=second.id,
        symbol=second.symbol,
        decision="execute_now",
        side="buy",
        quantity=1,
        updated_target_price=Decimal("110"),
        updated_stop_price=Decimal("98"),
        context_staleness_ok=True,
        reason="exact family adjustment",
    )

    monitoring._apply_trigger_time_agent_adjustments(item=second, decision=decision)

    assert first_target.trigger_price == Decimal("120")
    assert first_stop.trigger_price == Decimal("90")
    assert second_target.trigger_price == Decimal("110")
    assert second_stop.trigger_price == Decimal("98")
    assert monitoring._active_sibling_monitor(
        entry_item=second,
        intent="stop_sell",
    ) is second_stop


def test_partial_take_profit_rearms_only_its_exact_stop_family() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
        trigger_price="90",
    )
    second_target = _exit(
        item_id="target-2",
        source_key="family-2:target",
        linked_entry_key=second.source_plan_item_id,
        intent="target_sell",
        trigger_price="110",
    )
    second_target.risk_context = {"exit_rule": {"kind": "partial_take_profit"}}
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="95",
    )
    ExecutionPlan(
        external_plan_id="partial-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_stop, second, second_target, second_stop],
    )

    monitoring._rearm_stop_after_partial_take_profit(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            breakeven_stop_buffer_pct=Decimal("1"),
        ),
        item=second_target,
        position=ExecutionPosition(
            mode="paper",
            symbol="005930",
            quantity=2,
            average_price=Decimal("100"),
        ),
        remaining_quantity=1,
        submitted_quantity=1,
        recorded_at=datetime(2026, 9, 11, tzinfo=UTC),
    )

    assert first_stop.trigger_price == Decimal("90")
    assert first_stop.quantity_policy["quantity"] == 1
    assert second_stop.trigger_price == Decimal("101")
    assert second_stop.quantity_policy["quantity"] == 1
    assert second_stop.quantity_policy["linked_entry_item_key"] == second.source_plan_item_id


def test_short_term_sizing_context_and_linking_stay_in_the_exact_family() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_target = _exit(
        item_id="target-1",
        source_key="family-1:target",
        linked_entry_key=first.source_plan_item_id,
        intent="target_sell",
        trigger_price="120",
    )
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
        trigger_price="90",
    )
    second_target = _exit(
        item_id="target-2",
        source_key="family-2:target",
        linked_entry_key=second.source_plan_item_id,
        intent="target_sell",
        trigger_price="102",
    )
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="99",
    )
    plan = ExecutionPlan(
        external_plan_id="sizing-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_target, first_stop, second, second_target, second_stop],
    )

    context = short_term_exec_agent._target_stop_context(plan=plan, item=second)

    assert context.target_items == [second_target]
    assert context.stop_items == [second_stop]
    short_term_exec_agent._link_monitoring_item(
        monitor_item=second_stop,
        entry_item=second,
        quantity=2,
        trigger_price=Decimal("98"),
    )
    assert second_stop.quantity_policy["linked_entry_item_key"] == second.source_plan_item_id
    assert first_stop.quantity_policy == {
        "kind": "shares",
        "quantity": 1,
        "linked_entry_item_key": first.source_plan_item_id,
    }


def test_trigger_time_protective_stop_reads_only_the_exact_family() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
        trigger_price="105",
    )
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="99",
    )
    ExecutionPlan(
        external_plan_id="protective-stop-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_stop, second, second_stop],
    )

    assert trigger_time_exec_agent.effective_protective_stop_price(second) == Decimal("99")


def test_final_target_sell_integrity_compares_only_its_exact_stop_family() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    first_stop = _exit(
        item_id="stop-1",
        source_key="family-1:stop",
        linked_entry_key=first.source_plan_item_id,
        trigger_price="100",
    )
    second_target = _exit(
        item_id="target-2",
        source_key="family-2:target",
        linked_entry_key=second.source_plan_item_id,
        intent="target_sell",
        trigger_price="110",
    )
    second_target.risk_context = {
        "last_trigger_time_exec_agent_interaction": {
            "decision": {
                "decision": "execute_now",
                "side": "sell",
                "updated_stop_price": "98",
            }
        }
    }
    second_stop = _exit(
        item_id="stop-2",
        source_key="family-2:stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="95",
    )
    db.add(
        ExecutionPlan(
            external_plan_id="final-stop-two-families",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
            items=[first, first_stop, second, second_target, second_stop],
        )
    )
    db.commit()

    assert monitoring._locked_effective_protective_stop_price(
        db=db,
        item=second_target,
    ) == Decimal("95")

    failure = broker_orders._final_target_sell_stop_integrity_failure(
        db=db,
        command=PlaceOrderRequest(
            idempotency_key="exact-family-target-sell",
            symbol="005930",
            side="sell",
            quantity=1,
            limit_price=Decimal("110"),
        ),
        plan_item=second_target,
    )

    assert failure is None
    db.close()
    engine.dispose()
