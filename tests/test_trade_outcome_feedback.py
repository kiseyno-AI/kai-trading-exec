from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem
from app.services.trade_outcome_feedback import (
    generate_trade_outcome_feedback,
    load_latest_trade_outcome_feedback_snippet,
)
from app.services.trigger_time_exec_agent import _request_payload
from app.services.watch_exec_agent_sizing import _candidate_universe_payload, _sizing_request_payload


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, future=True, expire_on_commit=False)()


def test_generate_trade_outcome_feedback_writes_fifo_report_and_snippet(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=10,
    )
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-feedback-1",
        source="kai-trading-core",
        strategy_run_id="run-1",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-buy-1",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="filled",
        quantity_policy={
            "kind": "shares",
            "quantity": 10,
            "sizing_adjustment": {"reason": "min_notional_floor"},
        },
        risk_context={
            "exec_agent_decision": {
                "action": "approve",
                "quantity": 10,
                "confidence": 0.71,
                "reason_code": "standard_breakout",
                "target_profile": "standard_breakout",
            },
            "stop_price": "69500",
            "liquidity_tier": "high",
            "trading_value": "90000000000",
        },
        decision_agent_name="short-term-decider",
        decision_run_id="decision-run-1",
        source_plan_item_id="core-item-1",
    )
    buy_order = BrokerOrder(
        id="order-buy-1",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="buy-1",
        broker_order_id="kis-buy-1",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=10,
    )
    sell_order = BrokerOrder(
        id="order-sell-1",
        broker_name="kis",
        mode="paper",
        idempotency_key="sell-1",
        broker_order_id="kis-sell-1",
        symbol="005930",
        side="sell",
        status="filled",
        quantity=10,
    )
    session.add_all(
        [
            plan,
            buy_order,
            sell_order,
            ExecutionFill(
                broker_order=buy_order,
                broker_fill_id="fill-buy-1",
                symbol="005930",
                side="buy",
                quantity=10,
                price=Decimal("71000"),
                filled_at=datetime(2026, 6, 10, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="fill-sell-1",
                symbol="005930",
                side="sell",
                quantity=10,
                price=Decimal("70000"),
                filled_at=datetime(2026, 6, 11, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 12, tzinfo=UTC),
    )

    assert report["closed_trade_count"] == 1
    trade = report["closed_trades"][0]
    assert trade["realized_pnl_krw"] == "-10000.0000"
    assert trade["return_pct"] == "-1.4085"
    assert trade["decision_run_id"] == "decision-run-1"
    assert report["prompt_snippet"]["patterns_to_downsize_or_reject"]
    assert (tmp_path / "latest.json").exists()
    assert (tmp_path / "latest.md").exists()
    assert load_latest_trade_outcome_feedback_snippet(settings)["closed_trade_count"] == 1

    session.close()
    engine.dispose()


def test_exec_agent_payloads_include_latest_trade_outcome_feedback(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        """
{
  "version": "trade-outcome-feedback-v1",
  "generated_at": "2026-06-12T00:00:00+00:00",
  "lookback_days": 20,
  "closed_trade_count": 1,
  "summary": {"realized_pnl_krw": "-10000.0000"},
  "prompt_snippet": {
    "summary": "Recent closed trades: pnl=-10000.0000 KRW",
    "patterns_to_downsize_or_reject": ["Downsize standard_breakout"],
    "positive_expectancy_patterns": [],
    "required_hard_gates": ["Reject repeat worst-loss setups"]
  }
}
""".strip(),
        encoding="utf-8",
    )
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-feedback-payload",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-1",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_pending",
        trigger_price=Decimal("71000"),
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={},
    )
    session.add(plan)
    session.commit()

    sizing_payload = _sizing_request_payload(
        settings=settings,
        plan=plan,
        candidates=[{"execution_plan_item_id": item.id, "symbol": item.symbol}],
        account_context={"available": False},
        candidate_universe=_candidate_universe_payload([]),
    )
    trigger_payload = _request_payload(
        settings=settings,
        item=item,
        side="buy",
        market_context={},
        account_context={},
        peer_context={},
        intended_quantity=None,
        remaining_quantity=None,
    )

    assert sizing_payload["trade_outcome_feedback"]["snippet"]["required_hard_gates"] == [
        "Reject repeat worst-loss setups"
    ]
    assert trigger_payload["trade_outcome_feedback"]["snippet"]["patterns_to_downsize_or_reject"] == [
        "Downsize standard_breakout"
    ]

    session.close()
    engine.dispose()
