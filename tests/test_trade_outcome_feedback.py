from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
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


def _add_closed_trade(
    session,
    *,
    plan: ExecutionPlan,
    suffix: str,
    symbol: str,
    buy_price: str,
    sell_price: str,
    quantity: int = 10,
    trigger_price: str | None = None,
    risk_context: dict[str, object] | None = None,
    filled_day: int = 10,
) -> None:
    item = ExecutionPlanItem(
        id=f"item-buy-{suffix}",
        plan=plan,
        symbol=symbol,
        side="buy",
        intent="next_open_buy",
        status="filled",
        trigger_price=Decimal(trigger_price) if trigger_price is not None else None,
        quantity_policy={"kind": "shares", "quantity": quantity},
        risk_context=risk_context or {},
        decision_agent_name="short-term-decider",
        decision_run_id=f"decision-{suffix}",
        source_plan_item_id=f"core-{suffix}",
    )
    buy_order = BrokerOrder(
        id=f"order-buy-{suffix}",
        plan_item=item,
        broker_name="kis",
        mode=plan.mode,
        idempotency_key=f"buy-{suffix}",
        broker_order_id=f"kis-buy-{suffix}",
        symbol=symbol,
        side="buy",
        status="filled",
        quantity=quantity,
    )
    sell_order = BrokerOrder(
        id=f"order-sell-{suffix}",
        broker_name="kis",
        mode=plan.mode,
        idempotency_key=f"sell-{suffix}",
        broker_order_id=f"kis-sell-{suffix}",
        symbol=symbol,
        side="sell",
        status="filled",
        quantity=quantity,
    )
    session.add_all(
        [
            buy_order,
            sell_order,
            ExecutionFill(
                broker_order=buy_order,
                broker_fill_id=f"fill-buy-{suffix}",
                symbol=symbol,
                side="buy",
                quantity=quantity,
                price=Decimal(buy_price),
                filled_at=datetime(2026, 6, filled_day, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id=f"fill-sell-{suffix}",
                symbol=symbol,
                side="sell",
                quantity=quantity,
                price=Decimal(sell_price),
                filled_at=datetime(2026, 6, filled_day + 1, 1, 0, tzinfo=UTC),
            ),
        ]
    )


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
    assert report["portfolio_risk"]["available"] is False
    assert report["portfolio_risk"]["source_failed"] == (
        "execution_positions returned no positive local positions for mode=paper; "
        "broker account snapshots are not persisted by trade_outcome_feedback"
    )
    assert report["prompt_snippet"]["patterns_to_downsize_or_reject"]
    assert report["prompt_snippet"]["top_portfolio_risks"][0].startswith("Portfolio risk unavailable:")
    assert (tmp_path / "latest.json").exists()
    assert (tmp_path / "latest.md").exists()
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    for heading in ("이번주 분석 결과", "포트폴리오 리스크", "개선 사항", "검증", "월요일 전 우선순위"):
        assert heading in markdown
    assert load_latest_trade_outcome_feedback_snippet(settings)["closed_trade_count"] == 1

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_portfolio_risk_and_sell_reason_contributions(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    plan = ExecutionPlan(
        id="plan-risk-1",
        external_plan_id="plan-risk-1",
        source="kai-trading-core",
        strategy_run_id="run-risk-1",
        mode="paper",
    )
    buy_a = ExecutionPlanItem(
        id="item-buy-a",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="filled",
        risk_context={"sector": "Semiconductors"},
    )
    stop_sell = ExecutionPlanItem(
        id="item-stop-sell",
        plan=plan,
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="filled",
        risk_context={"trigger_reason": "stop_sell"},
    )
    buy_b = ExecutionPlanItem(
        id="item-buy-b",
        plan=plan,
        symbol="000660",
        side="buy",
        intent="next_open_buy",
        status="filled",
        risk_context={"sector": "Semiconductors"},
    )
    target_sell = ExecutionPlanItem(
        id="item-target-sell",
        plan=plan,
        symbol="000660",
        side="sell",
        intent="target_sell",
        status="filled",
    )
    buy_c = ExecutionPlanItem(
        id="item-buy-c",
        plan=plan,
        symbol="035420",
        side="buy",
        intent="next_open_buy",
        status="filled",
        risk_context={"sector": "Internet"},
    )
    max_holding_sell = ExecutionPlanItem(
        id="item-max-holding-sell",
        plan=plan,
        symbol="035420",
        side="sell",
        intent="max_holding_close_sell",
        status="filled",
    )
    open_item = ExecutionPlanItem(
        id="item-open-order",
        plan=plan,
        symbol="068270",
        side="sell",
        intent="target_sell",
        status="broker_order_submitted",
    )
    orders = [
        BrokerOrder(
            id="order-buy-a",
            plan_item=buy_a,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-buy-a",
            broker_order_id="risk-buy-a",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=10,
        ),
        BrokerOrder(
            id="order-stop-sell",
            plan_item=stop_sell,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-stop-sell",
            broker_order_id="risk-stop-sell",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=10,
        ),
        BrokerOrder(
            id="order-buy-b",
            plan_item=buy_b,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-buy-b",
            broker_order_id="risk-buy-b",
            symbol="000660",
            side="buy",
            status="filled",
            quantity=5,
        ),
        BrokerOrder(
            id="order-target-sell",
            plan_item=target_sell,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-target-sell",
            broker_order_id="risk-target-sell",
            symbol="000660",
            side="sell",
            status="filled",
            quantity=5,
        ),
        BrokerOrder(
            id="order-buy-c",
            plan_item=buy_c,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-buy-c",
            broker_order_id="risk-buy-c",
            symbol="035420",
            side="buy",
            status="filled",
            quantity=2,
        ),
        BrokerOrder(
            id="order-max-holding-sell",
            plan_item=max_holding_sell,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-max-holding-sell",
            broker_order_id="risk-max-holding-sell",
            symbol="035420",
            side="sell",
            status="filled",
            quantity=2,
        ),
        BrokerOrder(
            id="order-open-stale",
            plan_item=open_item,
            broker_name="kis",
            mode="paper",
            idempotency_key="risk-open-stale",
            broker_order_id="risk-open-stale",
            symbol="068270",
            side="sell",
            status="broker_order_submitted",
            quantity=3,
            limit_price=Decimal("220000"),
            submitted_at=datetime(2026, 6, 19, 0, 0, tzinfo=UTC),
            last_broker_sync_at=datetime(2026, 6, 19, 0, 1, tzinfo=UTC),
        ),
    ]
    session.add_all(
        [
            plan,
            *orders,
            ExecutionPosition(
                id="pos-005930",
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100000"),
                updated_from="broker_reconciliation",
                last_reconciled_at=datetime(2026, 6, 19, 0, 0, tzinfo=UTC),
            ),
            ExecutionPosition(
                id="pos-000660",
                mode="paper",
                symbol="000660",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="broker_reconciliation",
                last_reconciled_at=datetime(2026, 6, 20, 0, 9, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[0],
                broker_fill_id="risk-fill-buy-a",
                symbol="005930",
                side="buy",
                quantity=10,
                price=Decimal("100000"),
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[1],
                broker_fill_id="risk-fill-stop-sell",
                symbol="005930",
                side="sell",
                quantity=10,
                price=Decimal("99000"),
                filled_at=datetime(2026, 6, 16, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[2],
                broker_fill_id="risk-fill-buy-b",
                symbol="000660",
                side="buy",
                quantity=5,
                price=Decimal("100000"),
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[3],
                broker_fill_id="risk-fill-target-sell",
                symbol="000660",
                side="sell",
                quantity=5,
                price=Decimal("130000"),
                filled_at=datetime(2026, 6, 17, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[4],
                broker_fill_id="risk-fill-buy-c",
                symbol="035420",
                side="buy",
                quantity=2,
                price=Decimal("100000"),
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[5],
                broker_fill_id="risk-fill-max-holding-sell",
                symbol="035420",
                side="sell",
                quantity=2,
                price=Decimal("95000"),
                filled_at=datetime(2026, 6, 18, 0, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, 0, 10, tzinfo=UTC),
    )

    risk = report["portfolio_risk"]
    assert risk["available"] is True
    assert risk["open_holding_count"] == 2
    assert risk["total_market_value_krw"] == "2000000.0000"
    assert [row["symbol"] for row in risk["top_position_concentration"]] == ["000660", "005930"]
    assert risk["top_position_concentration"][0]["portfolio_weight_pct"] == "50.0000"
    assert risk["sector_concentration"][0]["sector"] == "Semiconductors"
    assert risk["stale_open_order_warnings"][0]["symbol"] == "068270"
    assert risk["stale_state_blockers"][0]["symbol"] == "068270"
    assert risk["stale_state_blockers"][0]["type"] == "stale_open_order_broker_truth"
    assert risk["normal_open_orders"] == []
    assert {row["type"] for row in risk["monday_pre_market_checks"]} >= {"open_order", "stale_local_position", "open_position"}
    by_reason = {row["name"]: row for row in risk["sell_reason_contributions"]}
    assert by_reason["stop_sell"]["realized_pnl_krw"] == "-10000.0000"
    assert by_reason["target_sell"]["realized_pnl_krw"] == "150000.0000"
    assert by_reason["max_holding_close_sell"]["realized_pnl_krw"] == "-10000.0000"
    assert risk["stop_sell_loss_contribution"]["realized_pnl_krw"] == "-10000.0000"
    assert risk["target_sell_profit_contribution"]["realized_pnl_krw"] == "150000.0000"
    assert risk["max_holding_close_contribution"]["realized_pnl_krw"] == "-10000.0000"
    assert any("Top concentration" in line for line in report["prompt_snippet"]["top_portfolio_risks"])
    assert any("Stale-state blockers" in line for line in report["prompt_snippet"]["top_portfolio_risks"])

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_portfolio_risk_uses_upbit_base_quantity(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    session.add_all(
        [
            ExecutionPosition(
                id="pos-paper-btc",
                mode="paper",
                symbol="KRW-BTC",
                quantity=0,
                base_quantity=Decimal("0.010000000000"),
                average_price=Decimal("150000000"),
                updated_from="upbit_paper_simulated_fill",
                last_reconciled_at=datetime(2026, 6, 20, 0, 9, tzinfo=UTC),
                raw_payload={"provider": "upbit", "paper": True, "simulated": True},
            ),
            ExecutionPosition(
                id="pos-live-btc",
                mode="live",
                symbol="KRW-BTC",
                quantity=0,
                base_quantity=Decimal("1.000000000000"),
                average_price=Decimal("150000000"),
                updated_from="upbit_paper_simulated_fill",
                last_reconciled_at=datetime(2026, 6, 20, 0, 9, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, 0, 10, tzinfo=UTC),
    )

    risk = report["portfolio_risk"]
    assert risk["available"] is True
    assert risk["open_holding_count"] == 1
    assert risk["total_market_value_krw"] == "1500000.0000"
    assert risk["positions_missing_market_value_count"] == 0
    concentration = risk["top_position_concentration"][0]
    assert concentration["symbol"] == "KRW-BTC"
    assert concentration["quantity"] == 0
    assert concentration["base_quantity"] == "0.010000000000"
    assert concentration["effective_quantity"] == "0.010000000000"
    assert concentration["market_value_krw"] == "1500000.0000"
    assert concentration["portfolio_weight_pct"] == "100.0000"
    assert risk["monday_pre_market_checks"][0]["base_quantity"] == "0.010000000000"
    assert any("KRW-BTC" in line and "1500000.0000" in line for line in report["prompt_snippet"]["top_portfolio_risks"])

    session.close()
    engine.dispose()


def test_candidate_rule_replay_improves_pnl_by_avoiding_stale_trigger_loser(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-improve", external_plan_id="plan-replay-improve", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"stale-loss-{index}",
            symbol=f"STL{index}",
            trigger_price="100",
            buy_price="104",
            sell_price="94",
            filled_day=10 + index,
            risk_context={"exec_agent_decision": {"confidence": "0.70"}},
        )
    _add_closed_trade(
        session,
        plan=plan,
        suffix="clean-win",
        symbol="CLN",
        trigger_price="100",
        buy_price="101",
        sell_price="106",
        filled_day=13,
        risk_context={"exec_agent_decision": {"confidence": "0.70"}},
    )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    stale = rules["stale_trigger_drift_veto"]
    assert stale["recommendation"] == "promote_to_issue"
    assert stale["avoided_losers"] == 3
    assert stale["skipped_winners"] == 0
    assert stale["gross_pnl_delta_krw"] == "300.0000"
    assert len(stale["top_avoided_losers"]) == 3
    assert any("stale_trigger_drift_veto" in line for line in report["prompt_snippet"]["candidate_rule_replay"])

    session.close()
    engine.dispose()


def test_candidate_rule_replay_rejects_rule_that_skips_winners(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-hurts", external_plan_id="plan-replay-hurts", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"weak-win-{index}",
            symbol=f"WIN{index}",
            buy_price="100",
            sell_price="108",
            filled_day=10 + index,
            risk_context={"confidence": "0.42"},
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    weak = rules["weak_confidence_reject"]
    assert weak["recommendation"] == "reject_candidate"
    assert weak["skipped_entries"] == 3
    assert weak["skipped_winners"] == 3
    assert weak["gross_pnl_delta_krw"] == "-240.0000"
    assert len(weak["top_skipped_winners"]) == 3

    session.close()
    engine.dispose()


def test_candidate_rule_replay_flags_low_sample_size(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-low-sample", external_plan_id="plan-replay-low-sample", source="kai-trading-core", mode="paper")
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="volatile-loss",
        symbol="VOL",
        buy_price="100",
        sell_price="90",
        risk_context={"atr_pct": "12.5"},
    )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    high_volatility = rules["high_volatility_downsize"]
    assert high_volatility["matched_trade_count"] == 1
    assert high_volatility["recommendation"] == "needs_more_data"
    assert high_volatility["gross_pnl_delta_krw"] == "50.0000"

    session.close()
    engine.dispose()


def test_high_volatility_replay_matches_watch_buy_atr_aliases(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-atr-aliases", external_plan_id="plan-replay-atr-aliases", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index, alias in enumerate(("atr_pct_14", "atr14_pct")):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"volatile-alias-{index}",
            symbol=f"ATR{index}",
            buy_price="100",
            sell_price="90",
            filled_day=10 + index,
            risk_context={alias: "12.5"},
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    closed_by_symbol = {row["symbol"]: row for row in report["closed_trades"]}
    assert closed_by_symbol["ATR0"]["candidate_rule_context"]["atr_pct_14"] == "12.5"
    assert closed_by_symbol["ATR1"]["candidate_rule_context"]["atr14_pct"] == "12.5"
    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    high_volatility = rules["high_volatility_downsize"]
    assert high_volatility["matched_trade_count"] == 2
    assert high_volatility["gross_pnl_delta_krw"] == "100.0000"
    evidence_fields = {row["evidence"]["field"] for row in high_volatility["top_avoided_losers"]}
    assert evidence_fields == {"atr_pct_14", "atr14_pct"}

    session.close()
    engine.dispose()


def test_candidate_rule_replay_metrics_include_win_rate_and_drawdown_proxy(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-metrics", external_plan_id="plan-replay-metrics", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index, sell_price in enumerate(("90", "112", "88")):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"corr-{index}",
            symbol=f"COR{index}",
            buy_price="100",
            sell_price=sell_price,
            filled_day=10 + index,
            risk_context={"portfolio_correlation": "0.82", "fee_slippage_bps": "15"},
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    replay = report["candidate_rule_replay"]
    rules = {row["rule_id"]: row for row in replay["rules"]}
    high_correlation = rules["high_correlation_downsize"]
    assert replay["fee_slippage_assumption_bps"] == "15.0000"
    assert "win_rate_delta_pct" in high_correlation
    assert "max_drawdown_proxy_krw" in high_correlation
    assert high_correlation["average_return_delta_pct"] == "1.6667"
    assert high_correlation["max_drawdown_proxy_krw"] == "-60.0000"
    assert high_correlation["estimated_net_pnl_delta_krw"] is not None
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert "### Candidate Rule Replay" in markdown
    assert "recommendation=" in markdown

    session.close()
    engine.dispose()


def test_exec_agent_payloads_include_latest_trade_outcome_feedback(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        """
{
  "version": "trade-outcome-feedback-v1",
  "mode": "paper",
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


def test_latest_trade_outcome_feedback_ignores_other_runtime_modes(tmp_path) -> None:
    settings = Settings(runtime_mode="live", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        """
{
  "version": "trade-outcome-feedback-v1",
  "mode": "paper",
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

    assert load_latest_trade_outcome_feedback_snippet(settings) is None
