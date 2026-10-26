from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.api.schemas.execution import ShortTermWatcherDecisionRequest
from app.broker.base import BrokerAccountOpenOrder, BrokerAccountSnapshot
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem
from app.services import exec_agent
from app.services import short_term_watcher
from app.services import watch_exec_agent_sizing
from app.services.execution_account_route import bind_kis_account_route
from app.services.plan_item_identity import (
    plan_item_entry_family_key,
    plan_item_execution_route_key,
)


def _scope(*, account: str = "a" * 24) -> dict[str, object]:
    return {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": account,
        "venue": "KRX",
        "asset_class": "stock",
        "exchange": "KRX",
    }


def _short_term_command(*, max_notional: str = "10000000") -> ShortTermWatcherDecisionRequest:
    return ShortTermWatcherDecisionRequest.model_validate(
        {
            "execution_plan_item_id": "short-candidate",
            "dry_run": True,
            "agent_output": {
                "decision": "approve",
                "order_type": "market",
                "max_notional": max_notional,
                "reference_price": "100000",
                "target_price": "120000",
                "stop_price": "90000",
            },
            "account_context": {
                "available_cash": "10000000",
                "last_price": "100000",
                "holdings": {},
                "open_order_symbols": [],
            },
        }
    )


def _entry_item(
    *,
    item_id: str,
    lane: str,
    source: str = "shared-source",
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="broker_order_submitted",
        decision_agent_name=lane,
        source_plan_item_id=source,
        provider="kis",
        venue="KRX",
        asset_class="stock",
    )


def _local_order(
    *,
    item: ExecutionPlanItem,
    order_id: str,
    quantity: int = 10,
    fills: list[ExecutionFill] | None = None,
) -> BrokerOrder:
    order = BrokerOrder(
        id=order_id,
        broker_name="kis",
        mode="paper",
        execution_plan_item_id=item.id,
        plan_item=item,
        idempotency_key=f"local:{order_id}",
        symbol=item.symbol,
        side="buy",
        status="partially_filled" if fills else "submitted",
        quantity=quantity,
        limit_price=Decimal("100"),
        venue="KRX",
        asset_class="stock",
    )
    order.fills = fills or []
    return order


def _fractional_fill(*, order_id: str, quantity: str = "4.5") -> ExecutionFill:
    return ExecutionFill(
        broker_order_pk=order_id,
        broker_fill_id=f"fill:{order_id}",
        symbol="005930",
        side="buy",
        quantity=4,
        base_quantity=Decimal(quantity),
        price=Decimal("100"),
        filled_at=datetime(2026, 10, 26, tzinfo=UTC),
        payload={"fill_time_precision": "exact"},
    )


def test_exec_merge_does_not_dedupe_shared_source_across_lanes() -> None:
    local = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=10,
        source_candidate_id="shared-source",
        limit_price=Decimal("100"),
        payload={
            "decision_agent_name": "signal-decider",
            "execution_scope": _scope(),
        },
    )
    broker = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=2,
        remaining_quantity=2,
        source_candidate_id="shared-source",
        limit_price=Decimal("100"),
        payload={
            "decision_agent_name": "short-term-decider",
            "execution_scope": _scope(),
        },
    )

    merged = exec_agent._merge_account_open_orders(
        broker_orders=[broker],
        local_orders=[local],
    )

    assert len(merged) == 2
    assert sum(order.remaining_quantity or 0 for order in merged) == 12


def test_exec_merge_does_not_source_dedupe_contradicting_strong_ids() -> None:
    local = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=6,
        broker_order_id="local-broker-id",
        idempotency_key="local-idempotency",
        source_candidate_id="same-source",
        payload={
            "decision_agent_name": "signal-decider",
            "execution_scope": _scope(),
        },
    )
    broker = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=5,
        broker_order_id="different-broker-id",
        idempotency_key="different-idempotency",
        source_candidate_id="same-source",
        payload={
            "decision_agent_name": "signal-decider",
            "execution_scope": _scope(),
        },
    )

    merged = exec_agent._merge_account_open_orders(
        broker_orders=[broker],
        local_orders=[local],
    )

    assert len(merged) == 2
    assert {order.broker_order_id for order in merged} == {
        "local-broker-id",
        "different-broker-id",
    }


def test_exec_merge_allows_source_dedupe_only_with_same_lane_and_complete_scope() -> None:
    local = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=6,
        source_candidate_id="same-source",
        limit_price=Decimal("100"),
        payload={
            "decision_agent_name": "signal-decider",
            "execution_scope": _scope(),
        },
    )
    exact_copy = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=5,
        source_candidate_id="same-source",
        limit_price=Decimal("100"),
        payload={
            "decision_agent_name": "signal-decider",
            "execution_scope": _scope(),
        },
    )
    unknown_lineage = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        remaining_quantity=1,
        source_candidate_id="same-source",
        limit_price=Decimal("100"),
    )

    exact = exec_agent._merge_account_open_orders(
        broker_orders=[exact_copy],
        local_orders=[local],
    )
    unknown = exec_agent._merge_account_open_orders(
        broker_orders=[unknown_lineage],
        local_orders=[local],
    )

    assert len(exact) == 1
    assert exact[0].remaining_quantity == 5
    assert len(unknown) == 2


def test_exec_unknown_source_lineage_remains_a_duplicate_blocker() -> None:
    candidate = _entry_item(
        item_id="short-candidate",
        lane="short-term-decider",
        source="same-source",
    )
    known_other_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=2,
        source_candidate_id="same-source",
        payload={"decision_agent_name": "signal-decider"},
    )
    unknown_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        source_candidate_id="same-source",
    )

    assert exec_agent._has_duplicate_open_buy_order(
        item=candidate,
        idempotency_key="new-short-order",
        open_orders=[known_other_lane, unknown_lane],
    ) is True


@pytest.mark.parametrize(
    "service",
    [exec_agent, watch_exec_agent_sizing, short_term_watcher],
)
def test_fractional_or_overfilled_local_fill_is_indeterminate_not_flat(service) -> None:
    item = _entry_item(item_id=f"entry:{service.__name__}", lane="signal-decider")
    fractional = _local_order(
        item=item,
        order_id=f"fractional:{service.__name__}",
    )
    fractional.fills = [_fractional_fill(order_id=fractional.id)]
    overfilled = _local_order(
        item=item,
        order_id=f"overfilled:{service.__name__}",
    )
    overfilled.fills = [_fractional_fill(order_id=overfilled.id, quantity="11")]
    malformed = _local_order(
        item=item,
        order_id=f"malformed:{service.__name__}",
    )
    malformed.fills = [
        _fractional_fill(order_id=malformed.id, quantity="NaN")
    ]
    exactly_filled = _local_order(
        item=item,
        order_id=f"filled:{service.__name__}",
    )
    exactly_filled.fills = [_fractional_fill(order_id=exactly_filled.id, quantity="10")]

    assert service._local_order_remaining_quantity(fractional) is None
    assert service._local_order_remaining_quantity(overfilled) is None
    assert service._local_order_remaining_quantity(malformed) is None
    assert service._local_order_remaining_quantity(exactly_filled) == 0


@pytest.mark.parametrize(
    "service",
    [exec_agent, watch_exec_agent_sizing, short_term_watcher],
)
def test_unknown_local_order_quantity_is_never_invented_as_one_share(service) -> None:
    item = _entry_item(item_id=f"unknown:{service.__name__}", lane="signal-decider")
    order = _local_order(item=item, order_id=f"unknown-order:{service.__name__}")
    order.quantity = None

    assert service._local_order_remaining_quantity(order) is None
    assert service._conservative_local_order_quantity(order) is None


def test_exec_unknown_quantity_blocks_cross_lane_buy() -> None:
    candidate = _entry_item(
        item_id="short-candidate-unknown-quantity",
        lane="short-term-decider",
        source="short-source",
    )
    unknown_other_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=0,
        remaining_quantity=0,
        source_candidate_id="signal-source",
        payload={
            "decision_agent_name": "signal-decider",
            "capital_truth_uncertain": True,
            "remaining_quantity_indeterminate": True,
        },
    )

    assert exec_agent._has_duplicate_open_buy_order(
        item=candidate,
        idempotency_key="new-short-order",
        open_orders=[unknown_other_lane],
    ) is True


def test_short_watcher_keeps_cross_lane_source_orders_distinct() -> None:
    candidate = _entry_item(item_id="short-candidate", lane="short-term-decider")
    local_item = _entry_item(item_id="signal-entry", lane="signal-decider")
    local = _local_order(item=local_item, order_id="signal-local")
    snapshot = BrokerAccountSnapshot(
        observed_at=datetime(2026, 10, 26, tzinfo=UTC),
        source="test",
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=2,
                remaining_quantity=2,
                source_candidate_id="shared-source",
                limit_price=Decimal("100"),
                payload={"decision_agent_name": "short-term-decider"},
            )
        ],
    )

    total, same_lane, notional = short_term_watcher._open_buy_order_context(
        item=candidate,
        local_orders=[local],
        request_open_order_symbols=[],
        broker_account_snapshot=snapshot,
        reference_price=Decimal("100"),
    )

    assert total == 2
    assert same_lane == 1
    assert notional == Decimal("1200.0000")


def test_short_watcher_does_not_source_dedupe_contradicting_strong_ids() -> None:
    candidate = _entry_item(item_id="short-candidate", lane="short-term-decider")
    local_item = _entry_item(item_id="signal-entry", lane="signal-decider")
    local = _local_order(item=local_item, order_id="signal-local-strong-id")
    local.request_metadata = {"execution_scope": _scope()}
    snapshot = BrokerAccountSnapshot(
        observed_at=datetime(2026, 10, 26, tzinfo=UTC),
        source="test",
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=5,
                remaining_quantity=5,
                idempotency_key="different-idempotency",
                source_candidate_id="shared-source",
                limit_price=Decimal("100"),
                payload={
                    "decision_agent_name": "signal-decider",
                    "execution_scope": _scope(),
                },
            )
        ],
    )

    total, same_lane, notional = short_term_watcher._open_buy_order_context(
        item=candidate,
        local_orders=[local],
        request_open_order_symbols=[],
        broker_account_snapshot=snapshot,
        reference_price=Decimal("100"),
    )

    assert total == 2
    assert same_lane == 1
    assert notional == Decimal("1500.0000")


def test_short_watcher_reserves_full_order_when_fill_remaining_is_indeterminate() -> None:
    candidate = _entry_item(item_id="short-candidate", lane="short-term-decider")
    local_item = _entry_item(item_id="signal-entry", lane="signal-decider")
    local = _local_order(item=local_item, order_id="fractional-local")
    local.fills = [_fractional_fill(order_id=local.id)]

    total, same_lane, notional = short_term_watcher._open_buy_order_context(
        item=candidate,
        local_orders=[local],
        request_open_order_symbols=[],
        broker_account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime(2026, 10, 26, tzinfo=UTC),
            source="test",
        ),
        reference_price=Decimal("100"),
    )

    assert total == 1
    assert same_lane == 0
    assert notional == Decimal("1000.0000")


def test_short_watcher_unknown_cross_lane_quantity_blocks_without_one_share_guess() -> None:
    candidate = _entry_item(item_id="short-candidate", lane="short-term-decider")
    local_item = _entry_item(item_id="signal-entry", lane="signal-decider")
    local = _local_order(item=local_item, order_id="unknown-local")
    local.quantity = None

    total, same_lane, notional = short_term_watcher._open_buy_order_context(
        item=candidate,
        local_orders=[local],
        request_open_order_symbols=[],
        broker_account_snapshot=BrokerAccountSnapshot(
            observed_at=datetime(2026, 10, 26, tzinfo=UTC),
            source="test",
        ),
        reference_price=Decimal("100"),
    )

    assert total == 1
    assert same_lane == 1
    assert notional == Decimal("0.0000")


def test_direct_short_watcher_uses_configured_symbol_and_allocation_cap() -> None:
    envelope = short_term_watcher._allowed_action_envelope(
        settings=Settings(
            runtime_mode="paper",
            exec_agent_max_symbol_budget=Decimal("3000000"),
            exec_agent_max_position_allocation_pct=Decimal("0.30"),
            exec_agent_watch_sizing_fallback_account_basis_krw=Decimal("10000000"),
        ),
        context={
            "reference_price": "100000",
            "available_cash": "10000000",
            "resolved_quantity": 100,
            "requested_notional": "10000000",
            "same_lane_position_quantity": 0,
            "same_lane_open_buy_order_count": 0,
            "runtime_position_quantity": 0,
            "account_holding_quantity": 0,
            "open_buy_order_count": 0,
            "existing_symbol_exposure": "500000",
            "market_regime_context": {},
            "market_date": "2026-10-26",
        },
        command=_short_term_command(),
    )

    assert envelope["gross_symbol_cap_krw"] == "3000000.0000"
    assert envelope["existing_symbol_exposure_krw"] == "500000.0000"
    assert envelope["max_notional_krw"] == "2500000.0000"
    assert envelope["max_quantity"] == 25


def test_direct_short_watcher_subtracts_exposure_before_applying_cash_cap() -> None:
    envelope = short_term_watcher._allowed_action_envelope(
        settings=Settings(
            runtime_mode="paper",
            exec_agent_max_symbol_budget=Decimal("3000000"),
            exec_agent_max_position_allocation_pct=Decimal("0.30"),
            exec_agent_watch_sizing_fallback_account_basis_krw=Decimal("10000000"),
        ),
        context={
            "reference_price": "100000",
            "available_cash": "1000000",
            "resolved_quantity": 100,
            "requested_notional": "10000000",
            "same_lane_position_quantity": 0,
            "same_lane_open_buy_order_count": 0,
            "runtime_position_quantity": 0,
            "account_holding_quantity": 0,
            "open_buy_order_count": 1,
            "existing_symbol_exposure": "500000",
            "market_regime_context": {},
            "market_date": "2026-10-26",
        },
        command=_short_term_command(),
    )

    assert envelope["gross_symbol_cap_krw"] == "3000000.0000"
    assert envelope["remaining_symbol_capacity_krw"] == "2500000.0000"
    assert envelope["max_notional_krw"] == "1000000.0000"
    assert envelope["max_quantity"] == 10


def test_watch_sizing_unknown_cross_lane_quantity_blocks_new_buy() -> None:
    envelope = watch_exec_agent_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(
            runtime_mode="paper",
            exec_agent_max_symbol_budget=Decimal("3000000"),
            exec_agent_max_position_allocation_pct=Decimal("0.30"),
            exec_agent_watch_sizing_fallback_account_basis_krw=Decimal("10000000"),
        ),
        candidate={
            "symbol": "005930",
            "provider": "kis",
            "asset_class": "stock",
            "exchange": "KRX",
            "intent": "intraday_watch_buy",
            "decision_agent_name": "short-term-decider",
            "chase_limit_price": "100",
            "target_price": "120",
            "stop_loss_price": "90",
        },
        account_context={
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "holding_quantities_by_decision_lane": {},
            "holding_lane_attribution_complete_by_symbol": {},
            "open_orders": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "quantity": None,
                    "remaining_quantity": None,
                    "decision_agent_name": "signal-decider",
                    "remaining_quantity_indeterminate": True,
                }
            ],
        },
    )

    assert "open_order_quantity_indeterminate" in envelope["hard_reject_reasons"]
    assert envelope["allowed_actions"] == ["hold", "reject"]


def test_exec_generated_exit_carries_exact_entry_family_and_lane() -> None:
    plan = ExecutionPlan(
        id="exec-plan",
        external_plan_id="exec-plan",
        source="test",
        mode="paper",
        market_date=date(2026, 10, 26),
    )
    entry = _entry_item(
        item_id="exec-entry",
        lane="short-term-decider",
        source="core-entry-1",
    )
    entry.plan = plan

    monitor = exec_agent._new_monitoring_item(
        plan=plan,
        buy_item=entry,
        decision={"idempotency_key": "exec-entry-order", "confidence": "0.9"},
        intent="stop_sell",
        quantity=2,
        next_plan_item_index=1,
        trigger_price=Decimal("90000"),
        max_sessions=3,
        exit_rule={"kind": "protective_stop"},
    )
    plan.items.append(monitor)

    assert monitor.quantity_policy["linked_entry_item_key"] == "core-entry-1"
    assert monitor.risk_context["entry_context"] == {
        "source_plan_item_id": "core-entry-1",
        "execution_plan_item_id": "exec-entry",
        "decision_agent_name": "short-term-decider",
    }
    assert plan_item_entry_family_key(monitor) == plan_item_entry_family_key(entry)


def test_short_watcher_does_not_reuse_cross_lane_family_monitors() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="short-watcher-lane-route",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        entry = _entry_item(
            item_id="short-entry",
            lane="short-term-decider",
            source="shared-family",
        )
        entry.status = "short_term_watcher_pending"
        entry.risk_context, entry.quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 2},
        )
        plan = ExecutionPlan(
            id="short-plan",
            external_plan_id="short-plan",
            source="test",
            mode="paper",
            market_date=date(2026, 10, 26),
            items=[entry],
        )
        for index, intent in enumerate(("target_sell", "stop_sell"), start=1):
            sibling = ExecutionPlanItem(
                    id=f"signal-{intent}",
                    plan_item_index=index,
                    symbol="005930",
                    side="sell",
                    intent=intent,
                    status=f"{intent}_monitoring",
                    decision_agent_name="signal-decider",
                    source_plan_item_id="shared-family",
                    quantity_policy={
                        "kind": "shares",
                        "quantity": 2,
                        "linked_entry_item_key": "shared-family",
                    },
                    risk_context={
                        "entry_context": {
                            "source_plan_item_id": "shared-family",
                            "execution_plan_item_id": "signal-entry",
                            "decision_agent_name": "signal-decider",
                        }
                    },
                    provider="kis",
                    venue="KRX",
                    asset_class="stock",
                )
            sibling.risk_context, sibling.quantity_policy = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=sibling.risk_context,
                quantity_policy=sibling.quantity_policy,
            )
            plan.items.append(sibling)
        db.add(plan)
        db.flush()

        created = short_term_watcher._materialize_monitoring_items(
            db=db,
            item=entry,
            command=_short_term_command(),
            quantity=2,
            inherited_route=short_term_watcher._validated_entry_route_context(
                settings=settings,
                item=entry,
            ),
        )
        monitors = db.scalars(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.execution_plan_id == plan.id,
                ExecutionPlanItem.side == "sell",
            )
        ).all()

        assert len(created) == 2
        assert len(monitors) == 4
        assert sum(
            monitor.decision_agent_name == "short-term-decider"
            for monitor in monitors
        ) == 2
    engine.dispose()


def test_short_watcher_preserves_foreign_route_monitors_and_creates_own_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="short-watcher-route-a",
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "short-watcher-route-b"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        entry = _entry_item(
            item_id="short-route-entry",
            lane="short-term-decider",
            source="shared-family",
        )
        entry.status = "short_term_watcher_pending"
        entry.risk_context, entry.quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 2},
        )
        plan = ExecutionPlan(
            id="short-route-plan",
            external_plan_id="short-route-plan",
            source="test",
            mode="paper",
            market_date=date(2026, 10, 26),
            items=[entry],
        )
        foreign_monitors: list[ExecutionPlanItem] = []
        for index, intent in enumerate(("target_sell", "stop_sell"), start=1):
            risk_context, quantity_policy = bind_kis_account_route(
                settings=foreign_settings,
                provider="kis",
                risk_context={
                    "exchange": "KRX",
                    "foreign_marker": intent,
                    "entry_context": {
                        "source_plan_item_id": "shared-family",
                        "execution_plan_item_id": entry.id,
                        "decision_agent_name": "short-term-decider",
                    },
                },
                quantity_policy={
                    "kind": "shares",
                    "quantity": 2,
                    "linked_entry_item_key": "shared-family",
                },
            )
            monitor = ExecutionPlanItem(
                id=f"foreign-{intent}",
                plan_item_index=index,
                symbol=entry.symbol,
                side="sell",
                intent=intent,
                status=f"{intent}_monitoring",
                decision_agent_name="short-term-decider",
                source_plan_item_id="shared-family",
                quantity_policy=quantity_policy,
                risk_context=risk_context,
                trigger_price=Decimal("120000" if intent == "target_sell" else "90000"),
                provider="kis",
                venue="NXT",
                asset_class="stock",
            )
            plan.items.append(monitor)
            foreign_monitors.append(monitor)
        db.add(plan)
        db.flush()
        before = {
            monitor.id: (
                monitor.provider,
                monitor.venue,
                monitor.asset_class,
                dict(monitor.risk_context or {}),
                dict(monitor.quantity_policy or {}),
                monitor.trigger_price,
            )
            for monitor in foreign_monitors
        }

        created = short_term_watcher._materialize_monitoring_items(
            db=db,
            item=entry,
            command=_short_term_command(),
            quantity=2,
            inherited_route=short_term_watcher._validated_entry_route_context(
                settings=settings,
                item=entry,
            ),
        )
        monitors = db.scalars(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.execution_plan_id == plan.id,
                ExecutionPlanItem.side == "sell",
            )
        ).all()

        assert len(created) == 2
        assert len(monitors) == 4
        assert all(
            before[monitor.id]
            == (
                monitor.provider,
                monitor.venue,
                monitor.asset_class,
                dict(monitor.risk_context or {}),
                dict(monitor.quantity_policy or {}),
                monitor.trigger_price,
            )
            for monitor in foreign_monitors
        )
        entry_route = plan_item_execution_route_key(entry)
        assert entry_route is not None
        own_monitors = [
            monitor for monitor in monitors if monitor.id not in before
        ]
        assert len(own_monitors) == 2
        assert all(
            plan_item_execution_route_key(monitor) == entry_route
            for monitor in own_monitors
        )
    engine.dispose()


def test_watch_sizing_keeps_unknown_source_copy_and_local_order_separate() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        item = _entry_item(item_id="watch-signal-entry", lane="signal-decider")
        plan = ExecutionPlan(
            id="watch-plan",
            external_plan_id="watch-plan",
            source="test",
            mode="paper",
            market_date=date(2026, 10, 26),
            items=[item],
        )
        db.add(plan)
        db.add(_local_order(item=item, order_id="watch-local"))
        db.commit()
        context: dict[str, object] = {
            "available": True,
            "holdings": [],
            "open_orders": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "quantity": 2,
                    "remaining_quantity": 2,
                    "status": "open",
                    "limit_price": "100",
                    "source_candidate_id": "shared-source",
                    "decision_agent_name": "short-term-decider",
                }
            ],
        }

        watch_exec_agent_sizing._enrich_account_context_with_decision_lanes(
            settings=settings,
            db=db,
            account_context=context,
            observed_at=datetime.now(UTC),
        )

        assert len(context["open_orders"]) == 2
        assert {
            order.get("decision_agent_name")
            for order in context["open_orders"]
        } == {"signal-decider", "short-term-decider"}
    engine.dispose()


def test_watch_sizing_does_not_source_dedupe_contradicting_strong_ids() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        item = _entry_item(item_id="watch-strong-id-entry", lane="signal-decider")
        plan = ExecutionPlan(
            id="watch-strong-id-plan",
            external_plan_id="watch-strong-id-plan",
            source="test",
            mode="paper",
            market_date=date(2026, 10, 26),
            items=[item],
        )
        local = _local_order(item=item, order_id="watch-local-strong-id")
        local.request_metadata = {"execution_scope": _scope()}
        db.add(plan)
        db.add(local)
        db.commit()
        context: dict[str, object] = {
            "available": True,
            "holdings": [],
            "open_orders": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "quantity": 5,
                    "remaining_quantity": 5,
                    "status": "open",
                    "limit_price": "100",
                    "idempotency_key": "different-idempotency",
                    "source_candidate_id": "shared-source",
                    "decision_agent_name": "signal-decider",
                    "execution_scope": _scope(),
                }
            ],
        }

        watch_exec_agent_sizing._enrich_account_context_with_decision_lanes(
            settings=settings,
            db=db,
            account_context=context,
            observed_at=datetime.now(UTC),
        )

        assert len(context["open_orders"]) == 2
        assert {
            order.get("idempotency_key")
            for order in context["open_orders"]
        } == {"local:watch-local-strong-id", "different-idempotency"}
    engine.dispose()
