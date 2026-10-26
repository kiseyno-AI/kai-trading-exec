from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.api.schemas.execution import PlaceOrderRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services import broker_orders
from app.services import execution_api
from app.services import monitoring
from app.services import short_term_exec_agent
from app.services import trigger_time_exec_agent
from app.services.plan_item_identity import (
    plan_item_entry_family_key,
    plan_item_execution_route_key,
    plan_item_inventory_scope_key,
    select_plan_item_family_members,
)
from app.services.trigger_time_exec_agent import TriggerTimeExecAgentDecision


_TEST_KIS_ROUTE_BINDING = {
    "version": "kis-account-route-v1",
    "configured_route_fingerprint": "a" * 32,
}


def _entry(
    *,
    item_id: str,
    source_key: str | None,
    decision_agent_name: str | None = "signal-decider",
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        quantity_policy={"execution_account_route": _TEST_KIS_ROUTE_BINDING},
        source_plan_item_id=source_key,
        decision_agent_name=decision_agent_name,
    )


def _exit(
    *,
    item_id: str,
    source_key: str,
    linked_entry_key: str | None,
    intent: str = "stop_sell",
    trigger_price: str = "90",
    decision_agent_name: str | None = "signal-decider",
) -> ExecutionPlanItem:
    policy: dict[str, object] = {
        "kind": "shares",
        "quantity": 1,
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }
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
        decision_agent_name=decision_agent_name,
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


def test_exact_family_selection_also_requires_the_same_normalized_decision_lane() -> None:
    entry = _entry(
        item_id="signal-entry",
        source_key="shared-family:entry",
        decision_agent_name=" Signal-Decider ",
    )
    signal_stop = _exit(
        item_id="signal-stop",
        source_key="signal-stop",
        linked_entry_key=entry.source_plan_item_id,
        decision_agent_name="SIGNAL-DECIDER",
    )
    short_stop = _exit(
        item_id="short-stop",
        source_key="short-stop",
        linked_entry_key=entry.source_plan_item_id,
        decision_agent_name="short-term-decider",
    )
    ExecutionPlan(
        external_plan_id="same-family-two-lanes",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, signal_stop, short_stop],
    )

    assert select_plan_item_family_members(
        anchor=entry,
        candidates=[signal_stop, short_stop],
    ) == [signal_stop]


@pytest.mark.parametrize("missing_identity_part", ["family", "lane"])
def test_partial_candidate_family_identity_fails_closed(
    missing_identity_part: str,
) -> None:
    entry = _entry(item_id="entry", source_key="family:entry")
    candidate = _exit(
        item_id=f"{missing_identity_part}-missing-stop",
        source_key="legacy-stop",
        linked_entry_key=(
            None if missing_identity_part == "family" else entry.source_plan_item_id
        ),
        decision_agent_name=(
            None if missing_identity_part == "lane" else "signal-decider"
        ),
    )
    ExecutionPlan(
        external_plan_id=f"partial-{missing_identity_part}-identity",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, candidate],
    )

    assert select_plan_item_family_members(
        anchor=entry,
        candidates=[candidate],
    ) == []


def test_partial_anchor_family_identity_fails_closed() -> None:
    entry = _entry(
        item_id="family-only-entry",
        source_key="family:entry",
        decision_agent_name=None,
    )
    legacy_stop = _exit(
        item_id="legacy-stop-for-partial-anchor",
        source_key="legacy-stop",
        linked_entry_key=None,
        decision_agent_name=None,
    )
    ExecutionPlan(
        external_plan_id="partial-anchor-identity",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, legacy_stop],
    )

    assert select_plan_item_family_members(
        anchor=entry,
        candidates=[legacy_stop],
    ) == []


def test_single_entry_candidate_without_family_or_lane_keeps_legacy_fallback() -> None:
    entry = _entry(item_id="entry", source_key="family:entry")
    legacy_stop = _exit(
        item_id="genuine-legacy-stop",
        source_key="legacy-stop",
        linked_entry_key=None,
        decision_agent_name=None,
    )
    ExecutionPlan(
        external_plan_id="single-entry-legacy",
        source="operator-import",
        mode="paper",
        status="accepted",
        items=[entry, legacy_stop],
    )

    assert select_plan_item_family_members(
        anchor=entry,
        candidates=[legacy_stop],
    ) == [legacy_stop]


def test_plan_item_execution_route_key_normalizes_kis_route_without_credentials() -> None:
    item = _entry(item_id="kis-route-entry", source_key="family:entry")
    item.provider = " Korea_Investment "
    item.asset_class = " STOCK "
    item.risk_context = {
        "exchange": " nasd ",
        "account_reference": "KIS:12345678-01",
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }

    route_key = plan_item_execution_route_key(item)

    assert route_key == ("kis", "NASD", "stock", "a" * 32)
    assert "12345678" not in repr(route_key)


def test_plan_item_inventory_scope_folds_kis_domestic_execution_routes() -> None:
    krx = _entry(item_id="kis-krx-entry", source_key="family:entry")
    nxt = _entry(item_id="kis-nxt-entry", source_key="family:entry")
    krx.provider = nxt.provider = "kis"
    krx.asset_class = "stock"
    nxt.asset_class = "kr_equity"
    krx.risk_context = {
        "exchange": "KRX",
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }
    nxt.risk_context = {
        "exchange": "NXT",
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }

    assert plan_item_execution_route_key(krx) != plan_item_execution_route_key(nxt)
    assert plan_item_inventory_scope_key(krx) == plan_item_inventory_scope_key(nxt)
    assert plan_item_inventory_scope_key(krx) == (
        "kis",
        "KRX",
        "stock",
        "a" * 32,
    )


def test_plan_item_inventory_scope_preserves_account_and_overseas_boundaries() -> None:
    active = _entry(item_id="active-domestic", source_key="family:entry")
    foreign = _entry(item_id="foreign-domestic", source_key="family:entry")
    overseas = _entry(item_id="active-overseas", source_key="family:entry")
    active.risk_context = {
        "exchange": "KRX",
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }
    foreign_binding = {
        "version": "kis-account-route-v1",
        "configured_route_fingerprint": "f" * 32,
    }
    foreign.risk_context = {
        "exchange": "NXT",
        "execution_account_route": foreign_binding,
    }
    foreign.quantity_policy = {"execution_account_route": foreign_binding}
    overseas.asset_class = "us_equity"
    overseas.risk_context = {
        "exchange": "NASD",
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }

    assert plan_item_inventory_scope_key(active) != plan_item_inventory_scope_key(
        foreign
    )
    assert plan_item_inventory_scope_key(active) != plan_item_inventory_scope_key(
        overseas
    )
    assert plan_item_inventory_scope_key(overseas) == (
        "kis",
        "NASD",
        "us_equity",
        "a" * 32,
    )


def test_plan_item_execution_route_key_uses_non_kis_opaque_account_scope() -> None:
    item = _entry(item_id="alpaca-route-entry", source_key="family:entry")
    item.provider = " ALPACA "
    item.asset_class = " US_EQUITY "
    item.venue = "NYSE"
    item.quantity_policy = {"kind": "shares", "quantity": 1}
    item.risk_context = {
        "execution_account_scope": {
            "policy_version": 1,
            "provider": "alpaca",
            "account_scope": "b" * 24,
        }
    }

    assert plan_item_execution_route_key(item) == (
        "alpaca",
        "NYSE",
        "us_equity",
        "b" * 24,
    )


@pytest.mark.parametrize("provider", ["kis", "alpaca"])
def test_plan_item_execution_route_key_requires_an_exact_account_binding(
    provider: str,
) -> None:
    item = _entry(item_id=f"{provider}-unbound-entry", source_key="family:entry")
    item.provider = provider
    item.quantity_policy = {"kind": "shares", "quantity": 1}
    item.risk_context = {"exchange": "KRX" if provider == "kis" else "NYSE"}

    assert plan_item_execution_route_key(item) is None


def test_plan_item_execution_route_key_rejects_conflicting_kis_bindings() -> None:
    item = _entry(item_id="conflicting-route-entry", source_key="family:entry")
    item.risk_context = {"execution_account_route": _TEST_KIS_ROUTE_BINDING}
    item.quantity_policy = {
        "execution_account_route": {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "f" * 32,
        }
    }

    assert plan_item_execution_route_key(item) is None


def test_plan_item_execution_route_key_rejects_foreign_non_kis_binding() -> None:
    item = _entry(item_id="foreign-scope-entry", source_key="family:entry")
    item.provider = "alpaca"
    item.quantity_policy = {"kind": "shares", "quantity": 1}
    item.risk_context = {
        "exchange": "NYSE",
        "execution_account_scope": {
            "policy_version": 1,
            "provider": "upbit",
            "account_scope": "b" * 24,
        },
    }

    assert plan_item_execution_route_key(item) is None


def test_ambiguous_unlinked_same_symbol_exits_fail_closed() -> None:
    first = _entry(item_id="entry-1", source_key="family-1:entry")
    second = _entry(item_id="entry-2", source_key="family-2:entry")
    legacy_stop = _exit(
        item_id="legacy-stop",
        source_key="legacy-stop",
        linked_entry_key=None,
        decision_agent_name=None,
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
        decision_agent_name=None,
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


def test_same_payload_conflicting_source_family_claims_fail_closed() -> None:
    stop = _exit(
        item_id="conflicting-family-stop",
        source_key="conflicting-family-stop",
        linked_entry_key="family-a:entry",
    )
    stop.quantity_policy = {
        **(stop.quantity_policy or {}),
        "entry_context": {
            "source_plan_item_id": "family-b:entry",
            "execution_plan_item_id": "entry-b",
        },
    }

    assert plan_item_entry_family_key(stop) is None


def test_equivalent_source_claims_keep_source_family_over_execution_alias() -> None:
    entry = _entry(item_id="entry-a", source_key="family-a:entry")
    stop = _exit(
        item_id="equivalent-family-stop",
        source_key="equivalent-family-stop",
        linked_entry_key="family-a:entry",
    )
    stop.quantity_policy = {
        **(stop.quantity_policy or {}),
        "entry_context": {
            "source_plan_item_id": "family-a:entry",
            "execution_plan_item_id": "entry-a",
        },
    }
    stop.risk_context = {
        "entry_context": {
            "source_plan_item_id": "family-a:entry",
            "execution_plan_item_id": "entry-a",
        }
    }
    ExecutionPlan(
        external_plan_id="equivalent-family-claims",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, stop],
    )

    assert plan_item_entry_family_key(stop) == "source:family-a:entry"


@pytest.mark.parametrize(
    "invalid_reference",
    ["missing", "different-family", "other-symbol", "sell-side"],
)
def test_execution_entry_claim_must_resolve_to_the_claimed_same_symbol_buy(
    invalid_reference: str,
) -> None:
    stop = _exit(
        item_id=f"invalid-execution-reference-{invalid_reference}",
        source_key=f"invalid-execution-reference-{invalid_reference}:stop",
        linked_entry_key="family-a:entry",
    )
    stop.quantity_policy = {
        **(stop.quantity_policy or {}),
        "entry_context": {
            "source_plan_item_id": "family-a:entry",
            "execution_plan_item_id": "entry-a",
        },
    }
    items = [stop]
    if invalid_reference != "missing":
        entry = _entry(
            item_id="entry-a",
            source_key=(
                "family-b:entry"
                if invalid_reference == "different-family"
                else "family-a:entry"
            ),
        )
        if invalid_reference == "other-symbol":
            entry.symbol = "000660"
        elif invalid_reference == "sell-side":
            entry.side = "sell"
        items.insert(0, entry)
    ExecutionPlan(
        external_plan_id=f"invalid-execution-reference-{invalid_reference}",
        source="test",
        mode="paper",
        status="accepted",
        items=items,
    )

    assert plan_item_entry_family_key(stop) is None


def test_different_execution_entry_claims_do_not_hide_behind_one_source_family() -> None:
    first = _entry(item_id="entry-a", source_key="family-a:entry")
    second = _entry(item_id="entry-b", source_key="family-a:entry")
    stop = _exit(
        item_id="conflicting-execution-claims",
        source_key="conflicting-execution-claims:stop",
        linked_entry_key="family-a:entry",
    )
    stop.quantity_policy = {
        **(stop.quantity_policy or {}),
        "entry_context": {
            "source_plan_item_id": "family-a:entry",
            "execution_plan_item_id": first.id,
        },
    }
    stop.risk_context = {
        "entry_context": {
            "source_plan_item_id": "family-a:entry",
            "execution_plan_item_id": second.id,
        }
    }
    ExecutionPlan(
        external_plan_id="conflicting-execution-family-claims",
        source="test",
        mode="paper",
        status="accepted",
        items=[first, second, stop],
    )

    assert plan_item_entry_family_key(stop) is None


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
        family_average_entry_price=Decimal("100"),
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
    foreign_route_target = _exit(
        item_id="target-2-foreign-route",
        source_key="family-2:foreign-target",
        linked_entry_key=second.source_plan_item_id,
        intent="target_sell",
        trigger_price="105",
    )
    foreign_route_stop = _exit(
        item_id="stop-2-foreign-route",
        source_key="family-2:foreign-stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="101",
    )
    for foreign_item in (foreign_route_target, foreign_route_stop):
        foreign_item.quantity_policy = {
            **(foreign_item.quantity_policy or {}),
            "execution_account_route": {
                "version": "kis-account-route-v1",
                "configured_route_fingerprint": "b" * 32,
            },
        }
    plan = ExecutionPlan(
        external_plan_id="sizing-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[
            first,
            first_target,
            first_stop,
            second,
            second_target,
            second_stop,
            foreign_route_target,
            foreign_route_stop,
        ],
    )

    context = short_term_exec_agent._target_stop_context(plan=plan, item=second)

    assert context.target_items == [second_target]
    assert context.stop_items == [second_stop]
    foreign_target_policy = dict(foreign_route_target.quantity_policy or {})
    foreign_stop_policy = dict(foreign_route_stop.quantity_policy or {})
    short_term_exec_agent._link_monitoring_item(
        monitor_item=second_stop,
        entry_item=second,
        quantity=2,
        trigger_price=Decimal("98"),
    )
    assert second_stop.quantity_policy["linked_entry_item_key"] == second.source_plan_item_id
    assert foreign_route_target.quantity_policy == foreign_target_policy
    assert foreign_route_stop.quantity_policy == foreign_stop_policy
    assert first_stop.quantity_policy == {
        "kind": "shares",
        "quantity": 1,
        "linked_entry_item_key": first.source_plan_item_id,
        "execution_account_route": _TEST_KIS_ROUTE_BINDING,
    }


def test_execution_api_policy_injection_requires_exact_lane_and_route() -> None:
    entry = _entry(item_id="entry", source_key="family:entry")
    foreign_route_target = _exit(
        item_id="foreign-route-target",
        source_key="foreign-route-target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="105",
    )
    foreign_route_target.quantity_policy = {
        **(foreign_route_target.quantity_policy or {}),
        "execution_account_route": {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "b" * 32,
        },
    }
    missing_route_target = _exit(
        item_id="missing-route-target",
        source_key="missing-route-target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="104",
    )
    missing_route_target.quantity_policy = {
        key: value
        for key, value in (missing_route_target.quantity_policy or {}).items()
        if key != "execution_account_route"
    }
    foreign_lane_target = _exit(
        item_id="foreign-lane-target",
        source_key="foreign-lane-target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="103",
        decision_agent_name="short-term-decider",
    )
    exact_target = _exit(
        item_id="exact-target",
        source_key="exact-target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="102",
    )
    plan = ExecutionPlan(
        external_plan_id="policy-injection-exact-lane-route",
        source="test",
        mode="paper",
        status="accepted",
        items=[
            entry,
            foreign_route_target,
            missing_route_target,
            foreign_lane_target,
            exact_target,
        ],
    )

    execution_api._normalize_watch_buy_handoff_items(plan)

    assert entry.quantity_policy["target_price"] == "102.0000"


def test_execution_api_policy_injection_fails_closed_without_anchor_route() -> None:
    entry = _entry(item_id="unbound-entry", source_key="unbound-family:entry")
    entry.quantity_policy = {"kind": "shares", "quantity": 1}
    target = _exit(
        item_id="bound-target",
        source_key="bound-target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="102",
    )
    plan = ExecutionPlan(
        external_plan_id="policy-injection-unbound-anchor",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, target],
    )

    execution_api._normalize_watch_buy_handoff_items(plan)

    assert "target_price" not in entry.quantity_policy


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
    foreign_route_stop = _exit(
        item_id="stop-2-foreign-route",
        source_key="family-2:foreign-stop",
        linked_entry_key=second.source_plan_item_id,
        trigger_price="105",
    )
    foreign_route_stop.quantity_policy = {
        **(foreign_route_stop.quantity_policy or {}),
        "execution_account_route": {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "b" * 32,
        },
    }
    ExecutionPlan(
        external_plan_id="protective-stop-two-families",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[first, first_stop, second, second_stop, foreign_route_stop],
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


def test_final_target_sell_integrity_uses_only_exact_route_and_blocks_own_stop() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    entry = _entry(item_id="entry", source_key="family:entry")
    target = _exit(
        item_id="target",
        source_key="family:target",
        linked_entry_key=entry.source_plan_item_id,
        intent="target_sell",
        trigger_price="120",
    )
    target.risk_context = {
        "last_trigger_time_exec_agent_interaction": {
            "decision": {
                "decision": "execute_now",
                "side": "sell",
                "updated_stop_price": "102",
            }
        }
    }
    foreign_route_stop = _exit(
        item_id="foreign-route-stop",
        source_key="foreign-route-stop",
        linked_entry_key=entry.source_plan_item_id,
        trigger_price="105",
    )
    foreign_route_stop.quantity_policy = {
        **(foreign_route_stop.quantity_policy or {}),
        "execution_account_route": {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "b" * 32,
        },
    }
    missing_route_stop = _exit(
        item_id="missing-route-stop",
        source_key="missing-route-stop",
        linked_entry_key=entry.source_plan_item_id,
        trigger_price="110",
    )
    missing_route_stop.quantity_policy = {
        key: value
        for key, value in (missing_route_stop.quantity_policy or {}).items()
        if key != "execution_account_route"
    }
    own_route_stop = _exit(
        item_id="own-route-stop",
        source_key="own-route-stop",
        linked_entry_key=entry.source_plan_item_id,
        trigger_price="100",
    )
    db.add(
        ExecutionPlan(
            external_plan_id="final-stop-exact-route",
            source="test",
            mode="paper",
            status="accepted",
            items=[
                entry,
                target,
                foreign_route_stop,
                missing_route_stop,
                own_route_stop,
            ],
        )
    )
    db.commit()
    command = PlaceOrderRequest(
        idempotency_key="exact-route-target-sell",
        symbol="005930",
        side="sell",
        quantity=1,
        limit_price=Decimal("120"),
    )

    assert broker_orders._final_target_sell_stop_integrity_failure(
        db=db,
        command=command,
        plan_item=target,
    ) is None

    target.risk_context = {
        "last_trigger_time_exec_agent_interaction": {
            "decision": {
                "decision": "execute_now",
                "side": "sell",
                "updated_stop_price": "99",
            }
        }
    }
    assert broker_orders._final_target_sell_stop_integrity_failure(
        db=db,
        command=command,
        plan_item=target,
    ) == (
        "live_sell_protective_stop_relaxation",
        "trigger-time target sell approval would loosen the current protective stop",
    )

    target.quantity_policy = {
        key: value
        for key, value in (target.quantity_policy or {}).items()
        if key != "execution_account_route"
    }
    assert broker_orders._final_target_sell_stop_integrity_failure(
        db=db,
        command=command,
        plan_item=target,
    ) == (
        "live_sell_protective_stop_route_unavailable",
        "trigger-time target sell approval has no exact execution route",
    )
    db.close()
    engine.dispose()
