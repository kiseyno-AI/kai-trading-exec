from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem
from app.services.execution_lineage import decision_lineage_for_plan_item
from app.services.strategy_family_attribution import (
    _loaded_plan_item_entry_family_key,
)
from app.services.plan_item_identity import (
    plan_item_decision_agent_name,
    plan_item_entry_family_key,
)
from app.services.trade_outcome_feedback import (
    _feedback_learning_quality_gate,
    _fifo_closed_trades,
    _strategy_family_closed_trades,
    _with_strategy_scope,
    generate_trade_outcome_feedback,
    load_latest_trade_outcome_feedback_snippet,
)
from app.services.weekly_trade_review import (
    WEEKLY_TRADE_REVIEW_SCHEMA_VERSION,
    _weekly_closed_trades_with_strategy,
    generate_weekly_trade_review,
)


ACCOUNT_SCOPE = "c" * 24
STRATEGY_FAMILY_METADATA_KEY = "strategy_entry_family_v1"


def _item(
    *,
    item_id: str,
    source_key: str,
    agent: str,
    side: str,
    linked_entry_key: str | None = None,
) -> ExecutionPlanItem:
    plan = ExecutionPlan(
        id=f"{item_id}-plan",
        external_plan_id=f"{item_id}-external",
        source="kai-trading-core",
        market_date=date(2026, 10, 26),
        strategy_run_id=f"{item_id}-run",
        mode="paper",
        status="accepted",
    )
    quantity_policy: dict[str, object] = {"kind": "shares", "quantity": 1}
    if linked_entry_key is not None:
        quantity_policy.update(
            {
                "type": "position_exit_from_entry",
                "linked_entry_item_key": linked_entry_key,
            }
        )
    return ExecutionPlanItem(
        id=item_id,
        execution_plan_id=plan.id,
        plan_item_index=0,
        symbol="005930",
        side=side,
        intent="entry" if side == "buy" else "target_sell",
        status="filled",
        quantity_policy=quantity_policy,
        decision_agent_name=agent,
        decision_snapshot_id=f"{item_id}-snapshot",
        decision_run_id=plan.strategy_run_id,
        source_plan_item_id=source_key,
        source_market_date=date(2026, 10, 25),
        provider="kis",
        venue="KRX",
        asset_class="stock",
        plan=plan,
    )


def _fill(
    *,
    item: ExecutionPlanItem,
    fill_id: str,
    side: str,
    price: str,
    quantity: int = 1,
    filled_at: datetime,
) -> ExecutionFill:
    lineage = decision_lineage_for_plan_item(item)
    assert lineage is not None
    family_key = plan_item_entry_family_key(item)
    decision_agent_name = plan_item_decision_agent_name(item)
    family_identity = (
        {
            "policy_version": 1,
            "entry_family_key": family_key,
            "decision_lane": decision_agent_name,
        }
        if family_key is not None and decision_agent_name is not None
        else None
    )
    command_metadata = (
        {STRATEGY_FAMILY_METADATA_KEY: dict(family_identity)}
        if family_identity is not None
        else {}
    )
    order = BrokerOrder(
        id=f"{fill_id}-order",
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="paper",
        idempotency_key=f"{fill_id}-order",
        broker_order_id=f"{fill_id}-broker-order",
        symbol=item.symbol,
        side=side,
        status="filled",
        quantity=quantity,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": ACCOUNT_SCOPE,
            },
            "decision_lineage": lineage,
            "metadata": command_metadata,
            "original_command": {"metadata": dict(command_metadata)},
        },
        plan_item=item,
    )
    return ExecutionFill(
        id=fill_id,
        broker_order=order,
        broker_fill_id=fill_id,
        symbol=item.symbol,
        side=side,
        quantity=quantity,
        price=Decimal(price),
        settlement_currency="KRW",
        transaction_cost=Decimal("0"),
        transaction_cost_coverage="complete",
        transaction_cost_source="test-exact-zero",
        filled_at=filled_at,
        payload={"decision_lineage": lineage},
    )


def _two_lane_fills(*, sell_quantity: int = 1, linked: bool = True) -> list[ExecutionFill]:
    started = datetime(2026, 10, 26, tzinfo=UTC)
    signal = _item(
        item_id="signal-entry-item",
        source_key="signal-entry",
        agent="signal-decider",
        side="buy",
    )
    short = _item(
        item_id="short-entry-item",
        source_key="short-entry",
        agent="short-term-decider",
        side="buy",
    )
    short_exit = _item(
        item_id="short-exit-item",
        source_key="short-exit",
        agent="short-term-decider",
        side="sell",
        linked_entry_key="short-entry" if linked else None,
    )
    return [
        _fill(
            item=signal,
            fill_id="signal-buy-fill",
            side="buy",
            price="100",
            filled_at=started,
        ),
        _fill(
            item=short,
            fill_id="short-buy-fill",
            side="buy",
            price="200",
            filled_at=started + timedelta(minutes=1),
        ),
        _fill(
            item=short_exit,
            fill_id="short-sell-fill",
            side="sell",
            price="210",
            quantity=sell_quantity,
            filled_at=started + timedelta(minutes=2),
        ),
    ]


def _same_plan_linked_family_fills(
    *,
    family_index: int,
    conflicting_risk_source: bool = False,
    decision_lane: str = "short-term-decider",
    sell_price: str = "110",
) -> list[ExecutionFill]:
    symbol = f"{family_index + 1:06d}"
    family_source = f"weekly-family-{family_index}:entry"
    started = datetime(2026, 10, 26, tzinfo=UTC) + timedelta(
        minutes=family_index * 10
    )
    plan = ExecutionPlan(
        id=f"weekly-family-plan-{family_index}",
        external_plan_id=f"weekly-family-plan-{family_index}",
        source="kai-trading-core",
        market_date=date(2026, 10, 26),
        strategy_run_id=f"weekly-family-run-{family_index}",
        mode="paper",
        status="accepted",
    )
    entry = ExecutionPlanItem(
        id=f"weekly-family-entry-{family_index}",
        plan=plan,
        execution_plan_id=plan.id,
        plan_item_index=0,
        symbol=symbol,
        side="buy",
        intent="entry",
        status="filled",
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={"exchange": "KRX"},
        decision_agent_name=decision_lane,
        decision_snapshot_id=f"weekly-family-entry-snapshot-{family_index}",
        decision_run_id=plan.strategy_run_id,
        source_plan_item_id=family_source,
        source_market_date=date(2026, 10, 25),
        provider="kis",
        venue="KRX",
        asset_class="stock",
    )
    exit_item = ExecutionPlanItem(
        id=f"weekly-family-exit-{family_index}",
        plan=plan,
        execution_plan_id=plan.id,
        plan_item_index=1,
        symbol=symbol,
        side="sell",
        intent="target_sell",
        status="filled",
        quantity_policy={
            "kind": "shares",
            "quantity": 1,
            "linked_entry_item_key": family_source,
        },
        risk_context={
            "entry_context": {
                "source_plan_item_id": family_source,
                "execution_plan_item_id": entry.id,
            }
        },
        decision_agent_name=decision_lane,
        decision_snapshot_id=f"weekly-family-exit-snapshot-{family_index}",
        decision_run_id=plan.strategy_run_id,
        source_plan_item_id=f"weekly-family-{family_index}:exit",
        source_market_date=date(2026, 10, 25),
        provider="kis",
        venue="KRX",
        asset_class="stock",
    )
    buy_fill = _fill(
        item=entry,
        fill_id=f"weekly-family-buy-fill-{family_index}",
        side="buy",
        price="100",
        filled_at=started,
    )
    sell_fill = _fill(
        item=exit_item,
        fill_id=f"weekly-family-sell-fill-{family_index}",
        side="sell",
        price=sell_price,
        filled_at=started + timedelta(minutes=1),
    )
    if conflicting_risk_source:
        exit_item.risk_context = {
            "entry_context": {
                "source_plan_item_id": f"weekly-family-{family_index}:conflict",
                "execution_plan_item_id": entry.id,
            }
        }
    return [buy_fill, sell_fill]


@pytest.mark.parametrize(
    "decision_lane",
    ["signal-decider", "short-term-decider"],
)
def test_feedback_partitions_exact_strategy_learning_by_decision_lane(
    tmp_path,
    decision_lane: str,
) -> None:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    fills = _same_plan_linked_family_fills(
        family_index=41 if decision_lane == "signal-decider" else 42,
        decision_lane=decision_lane,
        sell_price="90",
    )
    _attach_exact_execution_scope(fills)
    try:
        with Session(engine) as db:
            _persist_fills(db, fills)
            db.commit()
            report = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
                provider="kis",
                account_scope=ACCOUNT_SCOPE,
                exchange="KRX",
            )
            loaded = load_latest_trade_outcome_feedback_snippet(
                settings,
                now=datetime(2026, 10, 27, tzinfo=UTC),
                provider="kis",
                account_scope=ACCOUNT_SCOPE,
                exchange="KRX",
            )

        strategy_trade = report["strategy_closed_trades"][0]
        assert strategy_trade["strategy_scope"] == {
            "schema_version": "strategy-feedback-scope-v1",
            "status": "exact",
            "consumer_eligible": True,
            "decision_lane": decision_lane,
            "entry_family_key": f"source:weekly-family-{41 if decision_lane == 'signal-decider' else 42}:entry",
            "evidence_status": "exact",
        }
        assert set(report["strategy_learning_by_lane"]) == {decision_lane}
        lane_learning = report["strategy_learning_by_lane"][decision_lane]
        assert lane_learning["schema_version"] == "strategy-lane-learning-v1"
        assert lane_learning["status"] == "ready"
        assert lane_learning["consumer_eligible"] is True
        assert lane_learning["execution_authority"] is True
        assert lane_learning["decision_lane"] == decision_lane
        assert lane_learning["execution_account_scope"]["provider"] == "kis"
        assert lane_learning["execution_account_scope"]["account_scope"] == ACCOUNT_SCOPE
        assert lane_learning["closed_trade_count"] == 1
        assert lane_learning["lessons"]["worst_losses"][0]["strategy_scope"] == (
            strategy_trade["strategy_scope"]
        )
        assert report["lessons"]["execution_authority"] is False
        assert report["summary"]["execution_authority"] is False
        assert report["strategy_summary"]["execution_authority"] is False
        assert report["candidate_rule_replay"]["execution_authority"] is False
        assert report["quality_gate"]["execution_authority"] is False
        assert report["prompt_snippet"]["execution_authority"] is False
        assert loaded["status"] == "ready"
        assert loaded["strategy_learning_by_lane"][decision_lane][
            "consumer_eligible"
        ] is True
        assert loaded["snippet"]["strategy_learning_by_lane"][decision_lane][
            "consumer_eligible"
        ] is True
        for rule in lane_learning["candidate_rule_replay"]["rules"]:
            assert rule["strategy_scope"]["decision_lane"] == decision_lane
            assert rule["execution_authority"] is True
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("exact_lane", "ambiguous_lane"),
    [
        ("signal-decider", "short-term-decider"),
        ("short-term-decider", "signal-decider"),
    ],
)
def test_lane_known_ambiguous_strategy_row_blocks_only_its_own_lane(
    tmp_path,
    exact_lane: str,
    ambiguous_lane: str,
) -> None:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    exact_fills = _same_plan_linked_family_fills(
        family_index=43,
        decision_lane=exact_lane,
        sell_price="90",
    )
    ambiguous_fills = _same_plan_linked_family_fills(
        family_index=44,
        decision_lane=ambiguous_lane,
        sell_price="90",
    )
    ambiguous_sell_order = ambiguous_fills[-1].broker_order
    assert ambiguous_sell_order is not None
    ambiguous_sell_order.request_metadata["original_command"]["metadata"][
        STRATEGY_FAMILY_METADATA_KEY
    ] = {
        "policy_version": 1,
        "entry_family_key": "source:not-the-persisted-family",
        "decision_lane": ambiguous_lane,
    }
    try:
        with Session(engine) as db:
            _persist_fills(db, exact_fills + ambiguous_fills)
            db.commit()
            report = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
            )

        ambiguous_trade = next(
            trade
            for trade in report["strategy_closed_trades"]
            if trade["strategy_family_attribution_status"]
            == "strategy_family_persisted_identity_mismatch"
        )
        assert ambiguous_trade["strategy_scope"]["status"] == "unavailable"
        assert ambiguous_trade["strategy_scope"]["consumer_eligible"] is False
        assert ambiguous_trade["strategy_scope"]["decision_lane"] == (
            ambiguous_lane
        )
        assert ambiguous_trade["strategy_scope"]["entry_family_key"] is None
        assert set(report["strategy_learning_by_lane"]) == {
            "signal-decider",
            "short-term-decider",
        }
        exact_learning = report["strategy_learning_by_lane"][exact_lane]
        assert exact_learning["status"] == "ready"
        assert exact_learning["consumer_eligible"] is True
        assert exact_learning["execution_authority"] is True
        assert exact_learning["excluded_unattributed_trade_count"] == 0
        assert "incomplete_strategy_lane_attribution" not in exact_learning[
            "blocked_reasons"
        ]
        ambiguous_learning = report["strategy_learning_by_lane"][ambiguous_lane]
        assert ambiguous_learning["status"] == "blocked"
        assert ambiguous_learning["consumer_eligible"] is False
        assert ambiguous_learning["execution_authority"] is False
        assert ambiguous_learning["excluded_unattributed_trade_count"] == 1
        assert "incomplete_strategy_lane_attribution" in ambiguous_learning[
            "blocked_reasons"
        ]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "exact_lane",
    ["signal-decider", "short-term-decider"],
)
def test_unknown_strategy_lane_blocks_every_present_lane_bucket(
    tmp_path,
    exact_lane: str,
) -> None:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    exact_fills = _same_plan_linked_family_fills(
        family_index=45,
        decision_lane=exact_lane,
    )
    unknown_lane_fills = _same_plan_linked_family_fills(
        family_index=46,
        decision_lane="unknown-decider",
    )
    try:
        with Session(engine) as db:
            _persist_fills(db, exact_fills + unknown_lane_fills)
            db.commit()
            report = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
            )

        unknown_trade = next(
            trade
            for trade in report["strategy_closed_trades"]
            if trade["strategy_scope"]["decision_lane"] == "unknown-decider"
        )
        assert unknown_trade["strategy_scope"]["consumer_eligible"] is False
        assert unknown_trade["strategy_scope"]["evidence_status"] == (
            "unknown_decision_lane"
        )
        assert set(report["strategy_learning_by_lane"]) == {exact_lane}
        exact_learning = report["strategy_learning_by_lane"][exact_lane]
        assert exact_learning["status"] == "blocked"
        assert exact_learning["consumer_eligible"] is False
        assert exact_learning["unknown_or_ambiguous_lane_trade_count"] == 1
        assert "incomplete_strategy_lane_attribution" in exact_learning[
            "blocked_reasons"
        ]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("conflict_lane", "clean_lane"),
    [
        ("signal-decider", "short-term-decider"),
        ("short-term-decider", "signal-decider"),
    ],
)
def test_exact_duplicate_fill_conflict_blocks_only_its_attributed_lane(
    tmp_path,
    conflict_lane: str,
    clean_lane: str,
) -> None:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    conflict_fills = _same_plan_linked_family_fills(
        family_index=47,
        decision_lane=conflict_lane,
    )
    clean_fills = _same_plan_linked_family_fills(
        family_index=48,
        decision_lane=clean_lane,
    )
    all_fills = conflict_fills + clean_fills
    _attach_exact_execution_scope(all_fills)
    duplicate = _conflicting_duplicate(
        conflict_fills[-1],
        suffix="economic-conflict",
    )
    try:
        with Session(engine) as db:
            _persist_fills(db, all_fills)
            db.add(duplicate)
            db.commit()
            report = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
                provider="kis",
                account_scope=ACCOUNT_SCOPE,
                exchange="KRX",
            )

        evidence = report["duplicate_fill_conflict_evidence"]
        assert evidence["status"] == "conflict"
        assert evidence["decision_lanes"] == [conflict_lane]
        assert evidence["ambiguous_conflict_group_count"] == 0
        assert set(report["strategy_learning_by_lane"]) == {
            conflict_lane,
            clean_lane,
        }
        conflict_learning = report["strategy_learning_by_lane"][conflict_lane]
        clean_learning = report["strategy_learning_by_lane"][clean_lane]
        assert conflict_learning["consumer_eligible"] is False
        assert "duplicate_fill_economic_conflict" in conflict_learning[
            "blocked_reasons"
        ]
        assert clean_learning["status"] == "ready"
        assert clean_learning["consumer_eligible"] is True
        assert "duplicate_fill_economic_conflict" not in clean_learning[
            "blocked_reasons"
        ]
    finally:
        engine.dispose()


def test_ambiguous_duplicate_fill_conflict_blocks_both_lane_buckets(
    tmp_path,
) -> None:
    settings = Settings(
        database_url="sqlite+pysqlite:///:memory:",
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    signal_fills = _same_plan_linked_family_fills(
        family_index=49,
        decision_lane="signal-decider",
    )
    short_fills = _same_plan_linked_family_fills(
        family_index=50,
        decision_lane="short-term-decider",
    )
    unknown_fills = _same_plan_linked_family_fills(
        family_index=51,
        decision_lane="unknown-decider",
    )
    all_fills = signal_fills + short_fills + unknown_fills
    _attach_exact_execution_scope(all_fills)
    duplicate = _conflicting_duplicate(
        unknown_fills[-1],
        suffix="ambiguous-economic-conflict",
    )
    try:
        with Session(engine) as db:
            _persist_fills(db, all_fills)
            db.add(duplicate)
            db.commit()
            report = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
                provider="kis",
                account_scope=ACCOUNT_SCOPE,
                exchange="KRX",
            )

        evidence = report["duplicate_fill_conflict_evidence"]
        assert evidence["ambiguous_conflict_group_count"] == 1
        assert evidence["decision_lanes"] == []
        assert set(report["strategy_learning_by_lane"]) == {
            "signal-decider",
            "short-term-decider",
        }
        for lane in ("signal-decider", "short-term-decider"):
            lane_learning = report["strategy_learning_by_lane"][lane]
            assert lane_learning["status"] == "blocked"
            assert lane_learning["consumer_eligible"] is False
            assert lane_learning[
                "ambiguous_duplicate_fill_conflict_group_count"
            ] == 1
            assert "duplicate_fill_economic_conflict" in lane_learning[
                "blocked_reasons"
            ]
    finally:
        engine.dispose()


def _persist_fills(db: Session, fills: list[ExecutionFill]) -> None:
    for fill in fills:
        assert fill.broker_order is not None
        assert fill.broker_order.plan_item is not None
        assert fill.broker_order.plan_item.plan is not None
        db.add(fill.broker_order.plan_item.plan)
        db.add(fill.broker_order.plan_item)
        db.add(fill.broker_order)
        db.add(fill)


def _attach_exact_execution_scope(fills: list[ExecutionFill]) -> None:
    for fill in fills:
        assert fill.broker_order is not None
        fill.broker_order.request_metadata["execution_scope"] = {
            "policy_version": 1,
            "provider": "kis",
            "account_scope": ACCOUNT_SCOPE,
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
        }


def _conflicting_duplicate(
    fill: ExecutionFill,
    *,
    suffix: str,
) -> ExecutionFill:
    assert fill.broker_order is not None
    return ExecutionFill(
        id=f"{fill.id}-{suffix}",
        broker_order=fill.broker_order,
        broker_fill_id=fill.broker_fill_id,
        symbol=fill.symbol,
        side=fill.side,
        quantity=fill.quantity,
        price=Decimal(fill.price) + Decimal("1"),
        settlement_currency=fill.settlement_currency,
        transaction_cost=fill.transaction_cost,
        transaction_cost_coverage=fill.transaction_cost_coverage,
        transaction_cost_source=fill.transaction_cost_source,
        filled_at=fill.filled_at,
        payload=dict(fill.payload or {}),
    )


def test_strategy_learning_closes_linked_short_family_not_older_signal_fifo_lot() -> None:
    fills = _two_lane_fills()

    account_fifo = _fifo_closed_trades(fills)
    strategy = _strategy_family_closed_trades(fills)

    assert account_fifo[0]["buy"]["broker_fill_id"] == "signal-buy-fill"
    assert account_fifo[0]["gross_realized_pnl_krw"] == "110.0000"
    assert account_fifo[0]["pnl_attribution_basis"] == "broker_account_route_symbol_fifo"

    assert len(strategy) == 1
    assert strategy[0]["buy"]["broker_fill_id"] == "short-buy-fill"
    assert strategy[0]["gross_realized_pnl_krw"] == "10.0000"
    assert strategy[0]["decision_agent_name"] == "short-term-decider"
    assert strategy[0]["strategy_entry_family_key"] == "source:short-entry"
    assert strategy[0]["strategy_family_attribution_status"] == "exact"
    assert strategy[0]["pnl_attribution_basis"] == "strategy_virtual_entry_family"
    assert strategy[0]["profitability_eligible"] is True
    quality_gate = _feedback_learning_quality_gate(
        closed_trades=strategy,
        candidate_rule_replay={"status": "ready"},
    )
    assert quality_gate["pnl_attribution_basis"] == "strategy_virtual_entry_family"
    assert quality_gate["strategy_family_eligible_trade_count"] == 1
    assert "incomplete_strategy_family_attribution" not in quality_gate["blocked_reasons"]


def test_strategy_learning_folds_kis_domestic_buy_and_sell_routes() -> None:
    fills = _two_lane_fills()
    short_buy = fills[1]
    short_buy_item = short_buy.broker_order.plan_item
    assert short_buy_item is not None
    short_buy_item.venue = "NXT"
    short_buy_item.risk_context = {"exchange": "NXT"}
    short_buy.broker_order.venue = "NXT"
    short_buy.broker_order.request_metadata = {
        **short_buy.broker_order.request_metadata,
        "exchange": "NXT",
    }

    strategy = _strategy_family_closed_trades(fills)

    assert len(strategy) == 1
    assert strategy[0]["buy"]["broker_fill_id"] == "short-buy-fill"
    assert strategy[0]["sell"]["broker_fill_id"] == "short-sell-fill"
    assert strategy[0]["strategy_family_attribution_status"] == "exact"
    assert strategy[0]["strategy_family_learning_eligible"] is True


@pytest.mark.parametrize(
    ("broker_name", "exchange", "asset_class", "account_scope"),
    [
        ("kis", "KRX", "stock", "different-account"),
        ("kis", "NASD", "us_equity", ACCOUNT_SCOPE),
        ("alpaca", "KRX", "stock", ACCOUNT_SCOPE),
    ],
)
def test_strategy_learning_never_folds_foreign_inventory_scopes(
    broker_name: str,
    exchange: str,
    asset_class: str,
    account_scope: str,
) -> None:
    fills = _two_lane_fills()
    sell = fills[-1]
    sell.broker_order.broker_name = broker_name
    sell.broker_order.venue = exchange
    sell.broker_order.asset_class = asset_class
    sell.broker_order.request_metadata = {
        **sell.broker_order.request_metadata,
        "exchange": exchange,
        "broker_account_scope": {
            "policy_version": 1,
            "scope": account_scope,
        },
    }

    strategy = _strategy_family_closed_trades(fills)

    assert len(strategy) == 1
    assert strategy[0]["strategy_family_attribution_status"] == "family_not_found"
    assert strategy[0]["strategy_family_learning_eligible"] is False


def test_unlinked_two_lane_sell_is_excluded_from_strategy_learning() -> None:
    strategy = _strategy_family_closed_trades(_two_lane_fills(linked=False))

    assert len(strategy) == 1
    assert strategy[0]["strategy_family_attribution_status"] == (
        "modern_family_unlinked"
    )
    assert strategy[0]["profitability_eligible"] is False
    assert strategy[0]["realized_pnl_krw"] is None
    assert "strategy_family_modern_family_unlinked" in strategy[0][
        "outcome_quality_flags"
    ]
    quality_gate = _feedback_learning_quality_gate(
        closed_trades=strategy,
        candidate_rule_replay={"status": "ready"},
    )
    assert quality_gate["consumer_eligible"] is False
    assert "incomplete_strategy_family_attribution" in quality_gate["blocked_reasons"]


def test_modern_unlinked_sell_fails_closed_with_only_one_open_family() -> None:
    fills = _two_lane_fills(linked=False)

    strategy = _strategy_family_closed_trades([fills[1], fills[2]])

    assert len(strategy) == 1
    assert strategy[0]["strategy_family_attribution_status"] == (
        "modern_family_unlinked"
    )
    assert strategy[0]["strategy_family_learning_eligible"] is False
    assert strategy[0]["profitability_eligible"] is False


def test_linked_sell_without_decision_agent_fails_closed() -> None:
    fills = _two_lane_fills()
    sell_item = fills[-1].broker_order.plan_item
    assert sell_item is not None
    sell_item.decision_agent_name = None

    strategy = _strategy_family_closed_trades(fills)

    assert strategy[0]["strategy_family_attribution_status"] == (
        "family_agent_missing"
    )
    assert strategy[0]["strategy_family_learning_eligible"] is False
    assert strategy[0]["profitability_eligible"] is False


def test_linked_sell_agent_must_match_immutable_order_and_fill_lineage() -> None:
    fills = _two_lane_fills()
    sell_item = fills[-1].broker_order.plan_item
    assert sell_item is not None
    sell_item.decision_agent_name = "signal-decider"

    strategy = _strategy_family_closed_trades(fills)

    assert strategy[0]["strategy_family_attribution_status"] == (
        "family_agent_lineage_mismatch"
    )
    assert strategy[0]["strategy_family_learning_eligible"] is False
    assert strategy[0]["profitability_eligible"] is False


def test_modern_sell_requires_two_matching_immutable_family_copies() -> None:
    missing_copy_fills = _two_lane_fills()
    missing_copy_order = missing_copy_fills[-1].broker_order
    assert missing_copy_order is not None
    missing_copy_order.request_metadata["original_command"] = {"metadata": {}}

    missing_copy_strategy = _strategy_family_closed_trades(missing_copy_fills)

    assert missing_copy_strategy[0]["strategy_family_attribution_status"] == (
        "strategy_family_persisted_identity_unavailable"
    )
    assert missing_copy_strategy[0]["strategy_family_learning_eligible"] is False
    assert missing_copy_strategy[0]["realized_pnl_krw"] is None

    conflicting_copy_fills = _two_lane_fills()
    conflicting_copy_order = conflicting_copy_fills[-1].broker_order
    assert conflicting_copy_order is not None
    conflicting_copy_order.request_metadata["original_command"]["metadata"][
        STRATEGY_FAMILY_METADATA_KEY
    ] = {
        "policy_version": 1,
        "entry_family_key": "source:signal-entry",
        "decision_lane": "short-term-decider",
    }

    conflicting_copy_strategy = _strategy_family_closed_trades(
        conflicting_copy_fills
    )

    assert conflicting_copy_strategy[0]["strategy_family_attribution_status"] == (
        "strategy_family_persisted_identity_mismatch"
    )
    assert conflicting_copy_strategy[0]["strategy_family_learning_eligible"] is False
    assert conflicting_copy_strategy[0]["realized_pnl_krw"] is None


def test_current_family_mutation_cannot_reassign_historical_strategy_pnl() -> None:
    fills = _two_lane_fills()
    sell_item = fills[-1].broker_order.plan_item
    assert sell_item is not None
    sell_item.quantity_policy = {
        "type": "position_exit_from_entry",
        "linked_entry_item_key": "signal-entry",
        "quantity": 1,
    }

    strategy = _strategy_family_closed_trades(fills)

    assert strategy[0]["strategy_family_attribution_status"] == (
        "strategy_family_current_identity_mismatch"
    )
    assert strategy[0]["strategy_family_learning_eligible"] is False
    assert strategy[0]["realized_pnl_krw"] is None


@pytest.mark.parametrize(
    "reference_failure",
    ["missing", "different-family", "other-symbol", "sell-side"],
)
def test_unloaded_learning_identity_validates_execution_entry_claim_without_sql(
    reference_failure: str,
) -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    plan = ExecutionPlan(
        id=f"lazy-family-{reference_failure}-plan",
        external_plan_id=f"lazy-family-{reference_failure}-plan",
        source="test",
        market_date=date(2026, 10, 26),
        mode="paper",
        status="accepted",
    )
    exit_item = ExecutionPlanItem(
        id=f"lazy-family-{reference_failure}-exit",
        plan=plan,
        plan_item_index=1,
        symbol="005930",
        side="sell",
        intent="target_sell",
        status="filled",
        source_plan_item_id=f"lazy-family-{reference_failure}-exit-source",
        quantity_policy={
            "kind": "shares",
            "quantity": 1,
            "linked_entry_item_key": "family-a:entry",
            "entry_context": {
                "source_plan_item_id": "family-a:entry",
                "execution_plan_item_id": "entry-a",
            },
        },
        risk_context={"deferred_blob": "must-not-load"},
    )
    items = [exit_item]
    if reference_failure != "missing":
        entry = ExecutionPlanItem(
            id="entry-a",
            plan=plan,
            plan_item_index=0,
            symbol=("000660" if reference_failure == "other-symbol" else "005930"),
            side=("sell" if reference_failure == "sell-side" else "buy"),
            intent="entry",
            status="filled",
            source_plan_item_id=(
                "family-b:entry"
                if reference_failure == "different-family"
                else "family-a:entry"
            ),
        )
        items.insert(0, entry)

    statements: list[str] = []

    def capture_sql(
        _conn, _cursor, statement, _parameters, _context, _executemany
    ) -> None:  # type: ignore[no-untyped-def]
        statements.append(statement)

    try:
        with Session(engine) as db:
            db.add_all(items)
            db.commit()
            for item in items:
                db.refresh(item)
            db.expire(exit_item, ["risk_context"])
            event.listen(engine, "before_cursor_execute", capture_sql)
            try:
                family_key = _loaded_plan_item_entry_family_key(exit_item)
            finally:
                event.remove(engine, "before_cursor_execute", capture_sql)

        assert family_key is None
        assert statements == []
    finally:
        engine.dispose()


def test_unloaded_learning_identity_accepts_exact_loaded_execution_entry_alias() -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    plan = ExecutionPlan(
        id="lazy-exact-family-plan",
        external_plan_id="lazy-exact-family-plan",
        source="test",
        market_date=date(2026, 10, 26),
        mode="paper",
        status="accepted",
    )
    entry = ExecutionPlanItem(
        id="lazy-exact-entry",
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="entry",
        status="filled",
        source_plan_item_id="lazy-exact-family:entry",
    )
    exit_item = ExecutionPlanItem(
        id="lazy-exact-exit",
        plan=plan,
        plan_item_index=1,
        symbol="005930",
        side="sell",
        intent="target_sell",
        status="filled",
        quantity_policy={
            "kind": "shares",
            "quantity": 1,
            "linked_entry_item_key": "lazy-exact-family:entry",
            "entry_context": {
                "source_plan_item_id": "lazy-exact-family:entry",
                "execution_plan_item_id": entry.id,
            },
        },
        risk_context={"deferred_blob": "must-not-load"},
    )
    try:
        with Session(engine) as db:
            db.add_all([entry, exit_item])
            db.commit()
            db.refresh(entry)
            db.refresh(exit_item)
            db.expire(exit_item, ["risk_context"])

            assert _loaded_plan_item_entry_family_key(exit_item) == (
                "source:lazy-exact-family:entry"
            )
    finally:
        engine.dispose()


def test_legacy_planless_single_lane_sell_retains_bounded_fallback() -> None:
    fills = _two_lane_fills()
    buy_fill = fills[1]
    sell_fill = fills[2]
    sell_order = sell_fill.broker_order
    assert sell_order is not None
    sell_order.plan_item = None
    sell_order.execution_plan_item_id = None
    sell_order.request_metadata = {
        key: value
        for key, value in sell_order.request_metadata.items()
        if key != "decision_lineage"
    }
    sell_fill.payload = {}

    strategy = _strategy_family_closed_trades([buy_fill, sell_fill])

    assert len(strategy) == 1
    assert strategy[0]["strategy_family_attribution_status"] == (
        "legacy_single_lane_fifo"
    )
    assert strategy[0]["strategy_entry_family_key"] == "source:short-entry"
    assert strategy[0]["strategy_decision_agent_name"] == "short-term-decider"
    assert strategy[0]["strategy_family_learning_eligible"] is True
    scoped_trade = _with_strategy_scope(strategy[0])
    assert scoped_trade["strategy_scope"] == {
        "schema_version": "strategy-feedback-scope-v1",
        "status": "unavailable",
        "consumer_eligible": False,
        "decision_lane": "short-term-decider",
        "entry_family_key": "source:short-entry",
        "evidence_status": "legacy_single_lane_fifo",
    }


def test_missing_fee_never_generates_net_strategy_pnl_or_learning() -> None:
    fills = _two_lane_fills()
    fills[-1].transaction_cost = None
    fills[-1].transaction_cost_coverage = "unknown"
    fills[-1].transaction_cost_source = None

    strategy = _strategy_family_closed_trades(fills)
    quality_gate = _feedback_learning_quality_gate(
        closed_trades=strategy,
        candidate_rule_replay={"status": "ready"},
    )

    assert strategy[0]["net_realized_pnl"] is None
    assert strategy[0]["net_realized_pnl_krw"] is None
    assert strategy[0]["profitability_eligible"] is False
    assert quality_gate["consumer_eligible"] is False
    assert "incomplete_net_profitability_coverage" in quality_gate["blocked_reasons"]
    assert "incomplete_transaction_cost_coverage" in quality_gate["blocked_reasons"]


def test_overdrawn_family_sell_does_not_partially_consume_another_lane() -> None:
    strategy = _strategy_family_closed_trades(_two_lane_fills(sell_quantity=2))

    assert len(strategy) == 1
    assert strategy[0]["quantity"] == 2
    assert strategy[0]["strategy_family_attribution_status"] == "family_overdrawn"
    assert strategy[0]["strategy_family_unmatched_quantity"] == "1.0000"
    assert strategy[0]["profitability_eligible"] is False


def test_same_family_key_with_other_decider_is_not_cross_consumed() -> None:
    fills = _two_lane_fills()
    sell_item = fills[-1].broker_order.plan_item
    assert sell_item is not None
    sell_item.quantity_policy = {
        "type": "position_exit_from_entry",
        "linked_entry_item_key": "signal-entry",
        "quantity": 1,
    }

    strategy = _strategy_family_closed_trades(fills)

    assert strategy[0]["strategy_family_attribution_status"] == (
        "strategy_family_current_identity_mismatch"
    )
    assert strategy[0]["profitability_eligible"] is False


def test_weekly_review_emits_account_fifo_and_strategy_family_evidence_separately(
    tmp_path,
) -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    fills = _two_lane_fills()
    try:
        with Session(engine) as db:
            for fill in fills:
                assert fill.broker_order is not None
                assert fill.broker_order.plan_item is not None
                db.add(fill.broker_order.plan_item.plan)
                db.add(fill.broker_order.plan_item)
                db.add(fill.broker_order)
                db.add(fill)
            db.commit()

            account_rows, strategy_rows, truncated = _weekly_closed_trades_with_strategy(
                db=db,
                settings=settings,
                mode="paper",
                start_at=datetime(2026, 10, 26, tzinfo=UTC),
                end_at=datetime(2026, 10, 27, tzinfo=UTC),
                max_rows=100,
            )

            report = generate_weekly_trade_review(
                settings=settings,
                db=db,
                start_at=datetime(2026, 10, 26, tzinfo=UTC),
                end_at=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path,
                max_fills=100,
            )
            feedback = generate_trade_outcome_feedback(
                settings=settings,
                db=db,
                as_of=datetime(2026, 10, 27, tzinfo=UTC),
                output_dir=tmp_path / "feedback",
            )

        assert truncated is False
        assert account_rows[0]["cost_basis_price"] == Decimal("100")
        assert account_rows[0]["pnl_attribution_basis"] == "broker_account_route_symbol_fifo"
        assert strategy_rows[0]["cost_basis_price"] == Decimal("200")
        assert strategy_rows[0]["decision_agent_name"] == "short-term-decider"
        assert strategy_rows[0]["strategy_entry_family_key"] == "source:short-entry"
        assert strategy_rows[0]["pnl_attribution_basis"] == "strategy_virtual_entry_family"
        assert report["schema_version"] == WEEKLY_TRADE_REVIEW_SCHEMA_VERSION
        assert report["account_fifo_pnl_basis"] == "broker_account_route_symbol_fifo"
        assert report["strategy_pnl_basis"] == "strategy_virtual_entry_family"
        assert report["strategy_closed_trades"][0]["decision_agent_name"] == (
            "short-term-decider"
        )
        account_performance = report["learning_evidence"]["performance_feedback"]
        strategy_performance = report["learning_evidence"][
            "strategy_performance_feedback"
        ]
        assert account_performance["closed_trade_count"] == len(
            report["closed_trades"]
        )
        assert "pnl_attribution_basis" not in account_performance
        assert account_performance["consumer_eligible"] is True
        assert strategy_performance["pnl_attribution_basis"] == (
            "strategy_virtual_entry_family"
        )
        assert strategy_performance["closed_trade_count"] == len(
            report["strategy_closed_trades"]
        )
        assert strategy_performance["strategy_family_attribution_ineligible_count"] == 0
        assert strategy_performance["consumer_eligible"] is True
        assert feedback["closed_trades"][0]["buy"]["broker_fill_id"] == (
            "signal-buy-fill"
        )
        assert feedback["summary"]["realized_pnl_krw"] == "110.0000"
        assert feedback["account_fifo_pnl_basis"] == (
            "broker_account_route_symbol_fifo"
        )
        assert feedback["strategy_closed_trades"][0]["buy"][
            "broker_fill_id"
        ] == "short-buy-fill"
        assert feedback["strategy_summary"]["realized_pnl_krw"] == "10.0000"
        assert feedback["strategy_pnl_basis"] == "strategy_virtual_entry_family"
    finally:
        engine.dispose()


def test_weekly_load_only_path_rejects_conflicting_risk_context_family_claim() -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as db:
            _persist_fills(
                db,
                _same_plan_linked_family_fills(
                    family_index=0,
                    conflicting_risk_source=True,
                ),
            )
            db.commit()

        with Session(engine) as db:
            _account_rows, strategy_rows, truncated = (
                _weekly_closed_trades_with_strategy(
                    db=db,
                    settings=settings,
                    mode="paper",
                    start_at=datetime(2026, 10, 26, tzinfo=UTC),
                    end_at=datetime(2026, 10, 27, tzinfo=UTC),
                    max_rows=100,
                )
            )

        assert truncated is False
        assert len(strategy_rows) == 1
        assert strategy_rows[0]["strategy_family_attribution_status"] == (
            "modern_family_unlinked"
        )
        assert strategy_rows[0]["strategy_family_learning_eligible"] is False
    finally:
        engine.dispose()


def test_weekly_family_reference_loading_has_fixed_statement_count() -> None:
    statement_counts: list[int] = []
    for family_count in (1, 5):
        settings = Settings(
            database_url="sqlite+pysqlite:///:memory:",
            runtime_mode="paper",
        )
        engine = create_db_engine(settings)
        Base.metadata.create_all(engine)
        statements: list[str] = []

        def capture_sql(
            _conn, _cursor, statement, _parameters, _context, _executemany
        ) -> None:  # type: ignore[no-untyped-def]
            statements.append(statement)

        try:
            with Session(engine) as db:
                for family_index in range(family_count):
                    _persist_fills(
                        db,
                        _same_plan_linked_family_fills(
                            family_index=family_index,
                        ),
                    )
                db.commit()

            event.listen(engine, "before_cursor_execute", capture_sql)
            try:
                with Session(engine) as db:
                    _account_rows, strategy_rows, truncated = (
                        _weekly_closed_trades_with_strategy(
                            db=db,
                            settings=settings,
                            mode="paper",
                            start_at=datetime(2026, 10, 26, tzinfo=UTC),
                            end_at=datetime(2026, 10, 27, tzinfo=UTC),
                            max_rows=100,
                        )
                    )
            finally:
                event.remove(engine, "before_cursor_execute", capture_sql)

            assert truncated is False
            assert len(strategy_rows) == family_count
            assert all(
                row["strategy_family_learning_eligible"] is True
                for row in strategy_rows
            )
            plan_item_selects = [
                statement
                for statement in statements
                if "FROM execution_plan_items" in statement
            ]
            assert len(plan_item_selects) == 2
            statement_counts.append(len(statements))
        finally:
            engine.dispose()

    assert statement_counts[0] == statement_counts[1]
