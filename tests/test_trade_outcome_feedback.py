from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
)
from app.services.trade_outcome_feedback import (
    REPORT_VERSION,
    ensure_fresh_trade_outcome_feedback,
    generate_trade_outcome_feedback,
    load_latest_trade_outcome_feedback_snippet,
)
from app.services.account_drawdown_gate import (
    _legacy_drawdown_block_is_current,
    latest_account_drawdown_gate_state,
)


def test_legacy_daily_drawdown_block_expires_at_next_market_day() -> None:
    settings = Settings(market_timezone="Asia/Seoul")
    decision = {"summary": {"gate_name": "daily_total_asset_drawdown_krw"}}

    assert not _legacy_drawdown_block_is_current(
        settings=settings,
        occurred_at=datetime(2026, 9, 8, 6, 11, tzinfo=UTC),
        decision=decision,
        now=datetime(2026, 10, 1, 3, 40, tzinfo=UTC),
    )
from app.services.account_drawdown_recovery import (
    recovery_account_fingerprint,
    recovery_idempotency_key,
    recovery_policy_fingerprint,
)
from app.services import trade_outcome_feedback as feedback_service
from app.services.fifo_inventory import FifoIsolationCursor, FifoRouteScope
from app.services.position_identity import PositionIdentity, payload_with_position_identity
from app.services.trigger_time_exec_agent import _request_payload
from app.services.watch_exec_agent_sizing import _candidate_universe_payload, _sizing_request_payload


COMPLETE_KRW_FILL_ECONOMICS: dict[str, object] = {
    "settlement_currency": "KRW",
    "transaction_cost": Decimal("0"),
    "transaction_cost_coverage": "complete",
    "transaction_cost_source": "test_fixture",
}
FEEDBACK_TEST_ACCOUNT_SCOPE = "a" * 24


class _CapturingPositionSession:
    def __init__(self, positions: list[ExecutionPosition]) -> None:
        self.positions = positions
        self.statement = None

    def scalars(self, statement):
        self.statement = statement
        return self.positions


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, future=True, expire_on_commit=False)()


def _current_recovery_release_summary(
    *,
    settings: Settings,
    source_order: BrokerOrder,
    gate_disabled: bool = False,
) -> dict[str, object]:
    return {
        "source_broker_order_pk": source_order.id,
        "source_idempotency_key": source_order.idempotency_key,
        "recovery_idempotency_key": recovery_idempotency_key(
            base_key=source_order.idempotency_key,
            source_broker_order_pk=source_order.id,
            generation=1,
        ),
        "recovery_generation": 1,
        "healthy_observation_count": 2,
        "policy_fingerprint": recovery_policy_fingerprint(settings),
        "gate_disabled": gate_disabled,
    }


@pytest.mark.parametrize("enabled", [True, False])
def test_account_drawdown_gate_state_has_stable_empty_contract(enabled: bool) -> None:
    engine, session = _session()

    state = latest_account_drawdown_gate_state(
        settings=Settings(
            runtime_mode="paper",
            account_drawdown_gate_enabled=enabled,
        ),
        db=session,
    )

    assert state == {
        "available": True,
        "enabled": enabled,
        "gated": False,
        "provider": "kis",
        "latest_decision": None,
        "active_recovery_scopes": {},
        "reason": None,
    }

    session.close()
    engine.dispose()


def test_legacy_cost_basis_loader_fails_closed_on_duplicate_symbol_positions() -> None:
    identity = PositionIdentity(
        provider="kis",
        account_scope="a" * 24,
        exchange="KRX",
    )
    positions = [
        ExecutionPosition(
            id=f"position-{suffix}",
            mode="paper",
            symbol="005930",
            quantity=0,
            average_price=price,
            updated_from="reconciliation",
            raw_payload=payload_with_position_identity(None, identity=identity),
        )
        for suffix, price in (("one", Decimal("100")), ("two", Decimal("200")))
    ]
    db = _CapturingPositionSession(positions)
    route = FifoRouteScope("kis", "a" * 24, "KRX", "stock", "KRX")

    costs = feedback_service._load_legacy_sell_cost_basis(
        settings=Settings(runtime_mode="paper"),
        db=db,  # type: ignore[arg-type]
        route_symbols={(route, "005930")},
    )

    assert costs == {}
    assert db.statement is not None
    assert " LIMIT " in f" {db.statement} ".upper()


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
    buy_transaction_cost: str = "0",
    sell_transaction_cost: str = "0",
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
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "kis:test-account:krw",
            },
        },
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
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "kis:test-account:krw",
            },
        },
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
                settlement_currency="KRW",
                transaction_cost=Decimal(buy_transaction_cost),
                transaction_cost_coverage="complete",
                transaction_cost_source="test_fixture",
                filled_at=datetime(2026, 6, filled_day, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id=f"fill-sell-{suffix}",
                symbol=symbol,
                side="sell",
                quantity=quantity,
                price=Decimal(sell_price),
                settlement_currency="KRW",
                transaction_cost=Decimal(sell_transaction_cost),
                transaction_cost_coverage="complete",
                transaction_cost_source="test_fixture",
                filled_at=datetime(2026, 6, filled_day + 1, 1, 0, tzinfo=UTC),
            ),
        ]
    )


def _bind_closed_trade_to_execution_scope(
    session,
    *,
    suffix: str,
    account_scope: str,
    exchange: str = "KRX",
    provider: str = "kis",
) -> None:
    for side in ("buy", "sell"):
        order = session.get(BrokerOrder, f"order-{side}-{suffix}")
        assert order is not None
        order.broker_name = provider
        order.venue = exchange
        order.request_metadata = {
            "execution_scope": {
                "policy_version": 1,
                "provider": provider,
                "account_scope": account_scope,
                "venue": exchange,
                "asset_class": "stock",
                "exchange": exchange,
            }
        }


def _fifo_test_order(*, row_id: str, side: str) -> BrokerOrder:
    return BrokerOrder(
        id=row_id,
        broker_name="kis",
        mode="paper",
        idempotency_key=row_id,
        symbol="005930",
        side=side,
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "execution_scope": {
                "policy_version": 1,
                "provider": "kis",
                "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
                "venue": "KRX",
                "asset_class": "stock",
                "exchange": "KRX",
            }
        },
    )


def _fifo_test_fill(
    *,
    row_id: str,
    order: BrokerOrder,
    side: str,
    price: str,
    filled_at: datetime,
) -> ExecutionFill:
    return ExecutionFill(
        id=row_id,
        broker_order=order,
        broker_fill_id=row_id,
        symbol="005930",
        side=side,
        quantity=1,
        price=Decimal(price),
        **COMPLETE_KRW_FILL_ECONOMICS,
        filled_at=filled_at,
    )


def test_fifo_outcome_fails_closed_for_materially_different_same_time_buys() -> None:
    filled_at = datetime(2026, 6, 10, 0, 30, tzinfo=UTC)
    first_buy = _fifo_test_order(row_id="ambiguous-buy-one", side="buy")
    second_buy = _fifo_test_order(row_id="ambiguous-buy-two", side="buy")
    sell = _fifo_test_order(row_id="ambiguous-later-sell", side="sell")

    rows = feedback_service._fifo_closed_trades(
        [
            _fifo_test_fill(
                row_id="ambiguous-buy-fill-one",
                order=first_buy,
                side="buy",
                price="100",
                filled_at=filled_at,
            ),
            _fifo_test_fill(
                row_id="ambiguous-buy-fill-two",
                order=second_buy,
                side="buy",
                price="200",
                filled_at=filled_at,
            ),
            _fifo_test_fill(
                row_id="ambiguous-sell-fill",
                order=sell,
                side="sell",
                price="150",
                filled_at=filled_at + timedelta(minutes=1),
            ),
        ]
    )

    assert len(rows) == 1
    assert rows[0]["profitability_eligible"] is False
    assert "fill_chronology_ambiguous" in rows[0]["outcome_quality_flags"]


def test_fifo_outcome_accepts_clean_trade_after_exact_route_flat_boundary() -> None:
    old_at = datetime(2026, 6, 10, 0, 30, tzinfo=UTC)
    boundary_at = old_at + timedelta(seconds=1)
    route = FifoRouteScope(
        "kis", FEEDBACK_TEST_ACCOUNT_SCOPE, "KRX", "stock", "KRX"
    )
    cursor = FifoIsolationCursor(
        isolated_at_by_route_symbol={(route, "005930"): [boundary_at]}
    )
    orders = {
        key: _fifo_test_order(row_id=f"generation-{key}", side=side)
        for key, side in (
            ("old-buy-one", "buy"),
            ("old-buy-two", "buy"),
            ("new-buy", "buy"),
            ("new-sell", "sell"),
        )
    }

    rows = feedback_service._fifo_closed_trades(
        [
            _fifo_test_fill(
                row_id="generation-old-buy-fill-one",
                order=orders["old-buy-one"],
                side="buy",
                price="100",
                filled_at=old_at,
            ),
            _fifo_test_fill(
                row_id="generation-old-buy-fill-two",
                order=orders["old-buy-two"],
                side="buy",
                price="200",
                filled_at=old_at,
            ),
            _fifo_test_fill(
                row_id="generation-new-buy-fill",
                order=orders["new-buy"],
                side="buy",
                price="300",
                filled_at=boundary_at + timedelta(seconds=1),
            ),
            _fifo_test_fill(
                row_id="generation-new-sell-fill",
                order=orders["new-sell"],
                side="sell",
                price="350",
                filled_at=boundary_at + timedelta(minutes=1),
            ),
        ],
        isolation_cursor=cursor,
    )

    assert len(rows) == 1
    assert rows[0]["profitability_eligible"] is True
    assert rows[0]["gross_realized_pnl"] == "50.0000"
    assert "fill_chronology_ambiguous" not in rows[0]["outcome_quality_flags"]


@pytest.mark.parametrize(
    (
        "buy_provider",
        "buy_account_scope",
        "buy_venue",
        "buy_exchange",
        "sell_provider",
        "sell_account_scope",
        "sell_venue",
        "sell_exchange",
    ),
    [
        (
            "kis",
            "kis-account-a",
            "KRX",
            "KRX",
            "alpaca",
            "alpaca-account-b",
            "ALPACA",
            "NASD",
        ),
        (
            "alpaca",
            "alpaca-account-a",
            "ALPACA",
            "NASD",
            "alpaca",
            "alpaca-account-a",
            "ALPACA",
            "NYSE",
        ),
    ],
)
def test_trade_outcome_feedback_never_matches_same_symbol_across_route_dimensions(
    tmp_path,
    buy_provider: str,
    buy_account_scope: str,
    buy_venue: str,
    buy_exchange: str,
    sell_provider: str,
    sell_account_scope: str,
    sell_venue: str,
    sell_exchange: str,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=7,
    )

    def order(
        *,
        row_id: str,
        provider: str,
        side: str,
        account_scope: str,
        venue: str,
        exchange: str,
    ) -> BrokerOrder:
        return BrokerOrder(
            id=row_id,
            broker_name=provider,
            mode="paper",
            idempotency_key=row_id,
            broker_order_id=row_id,
            symbol="AAPL",
            side=side,
            status="filled",
            quantity=1,
            venue=venue,
            asset_class="stock",
            request_metadata={
                "provider": provider,
                "venue": venue,
                "asset_class": "stock",
                "exchange": exchange,
                "broker_account_scope": {
                    "policy_version": 1,
                    "scope": account_scope,
                },
            },
        )

    scoped_buy = order(
        row_id="kis-aapl-buy",
        provider=buy_provider,
        side="buy",
        account_scope=buy_account_scope,
        venue=buy_venue,
        exchange=buy_exchange,
    )
    scoped_sell = order(
        row_id="alpaca-aapl-sell",
        provider=sell_provider,
        side="sell",
        account_scope=sell_account_scope,
        venue=sell_venue,
        exchange=sell_exchange,
    )
    session.add_all([scoped_buy, scoped_sell])
    session.flush()
    session.add_all(
        [
            ExecutionFill(
                broker_order=scoped_buy,
                broker_fill_id="kis-aapl-buy-fill",
                symbol="AAPL",
                side="buy",
                quantity=1,
                price=Decimal("100"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 10, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=scoped_sell,
                broker_fill_id="alpaca-aapl-sell-fill",
                symbol="AAPL",
                side="sell",
                quantity=1,
                price=Decimal("200"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 11, 0, 30, tzinfo=UTC),
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
    assert trade["gross_realized_pnl"] is None
    assert trade["profitability_eligible"] is False
    assert trade["execution_scope"]["provider"] == sell_provider
    assert trade["execution_scope"]["exchange"] == sell_exchange
    assert report["lessons"]["eligible_trade_count"] == 0
    assert report["summary"]["realized_pnl_krw"] is None
    session.close()
    engine.dispose()


def test_trade_outcome_feedback_marks_legacy_unscoped_fifo_quality_unavailable(
    tmp_path,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=7,
    )
    buy_order = BrokerOrder(
        id="legacy-scope-buy",
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-scope-buy",
        symbol="005930",
        side="buy",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
    )
    sell_order = BrokerOrder(
        id="legacy-scope-sell",
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-scope-sell",
        symbol="005930",
        side="sell",
        status="filled",
        quantity=1,
        venue="KRX",
        asset_class="stock",
    )
    session.add_all([buy_order, sell_order])
    session.flush()
    session.add_all(
        [
            ExecutionFill(
                broker_order=buy_order,
                broker_fill_id="legacy-scope-buy-fill",
                symbol="005930",
                side="buy",
                quantity=1,
                price=Decimal("100"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 10, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="legacy-scope-sell-fill",
                symbol="005930",
                side="sell",
                quantity=1,
                price=Decimal("200"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 11, 0, 30, tzinfo=UTC),
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
    assert trade["gross_realized_pnl"] is None
    assert trade["profitability_eligible"] is False
    assert trade["outcome_source"] == "unavailable"
    assert "execution_scope_unavailable" in trade["outcome_quality_flags"]
    assert report["lessons"]["eligible_trade_count"] == 0
    session.close()
    engine.dispose()


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
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
            },
        },
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
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
            },
        },
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
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 10, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="fill-sell-1",
                symbol="005930",
                side="sell",
                quantity=10,
                price=Decimal("70000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
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
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        now=datetime(2026, 6, 12, 0, 1, tzinfo=UTC),
        provider="kis",
        account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
        exchange="KRX",
    )["closed_trade_count"] == 1

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_does_not_use_unscoped_position_cost_basis(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=7,
    )
    plan = ExecutionPlan(
        id="plan-legacy-sell-estimate",
        external_plan_id="plan-legacy-sell-estimate",
        source="kai-trading-core",
        strategy_run_id="run-legacy-sell-estimate",
        mode="paper",
    )
    sell_item = ExecutionPlanItem(
        id="item-legacy-sell-estimate",
        plan=plan,
        symbol="002380",
        side="sell",
        intent="max_holding_close_sell",
        status="filled",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    sell_order = BrokerOrder(
        id="order-legacy-sell-estimate",
        plan_item=sell_item,
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-sell-estimate",
        broker_order_id="legacy-sell-estimate",
        symbol="002380",
        side="sell",
        status="filled",
        quantity=1,
    )
    position = ExecutionPosition(
        id="pos-legacy-sell-estimate",
        mode="paper",
        symbol="002380",
        quantity=0,
        average_price=Decimal("511000"),
        updated_from="reconciliation",
        last_reconciled_at=datetime(2026, 7, 14, 12, 20, tzinfo=UTC),
    )
    session.add_all(
        [
            plan,
            sell_order,
            position,
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="fill-legacy-sell-estimate",
                symbol="002380",
                side="sell",
                quantity=1,
                price=Decimal("413500"),
                filled_at=datetime(2026, 7, 14, 12, 14, 3, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 7, 18, tzinfo=UTC),
    )

    trade = report["closed_trades"][0]
    assert trade["symbol"] == "002380"
    assert trade["realized_pnl_krw"] is None
    assert trade["gross_realized_pnl_krw"] is None
    assert trade["return_pct"] is None
    assert trade["gross_return_pct"] is None
    assert trade["profitability_eligible"] is False
    assert trade["outcome_bucket"] == "unavailable"
    assert trade["transaction_cost_coverage"] == "unknown"
    assert trade["outcome_source"] == "unavailable"
    assert trade["buy"]["price"] is None
    assert "missing_fifo_buy_lot" in trade["outcome_quality_flags"]
    assert "execution_scope_unavailable" in trade["outcome_quality_flags"]
    assert report["summary"]["realized_pnl_krw"] is None
    assert report["summary"]["gross_realized_pnl_by_currency"] == {}
    assert report["summary"]["estimated_trade_count"] == 0
    assert report["summary"]["unavailable_trade_count"] == 1
    assert report["candidate_rule_replay"]["status"] == "blocked"
    assert report["candidate_rule_replay"]["promotion_eligible"] is False
    assert report["candidate_rule_replay"]["blocked_reason"] == (
        "transaction_costs_not_ingested"
    )
    assert "gross results are diagnostic only" in report["prompt_snippet"][
        "candidate_rule_replay"
    ][0]
    session.refresh(position)
    assert position.quantity == 0
    assert position.average_price == Decimal("511000.0000")

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_marks_legacy_sell_unavailable_without_cost_basis(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=7,
    )
    sell_order = BrokerOrder(
        id="order-legacy-sell-unavailable",
        broker_name="kis",
        mode="paper",
        idempotency_key="legacy-sell-unavailable",
        broker_order_id="legacy-sell-unavailable",
        symbol="031330",
        side="sell",
        status="filled",
        quantity=35,
    )
    session.add_all(
        [
            sell_order,
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="fill-legacy-sell-unavailable",
                symbol="031330",
                side="sell",
                quantity=35,
                price=Decimal("9970"),
                filled_at=datetime(2026, 7, 14, 12, 14, 32, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 7, 18, tzinfo=UTC),
    )

    trade = report["closed_trades"][0]
    assert trade["outcome_source"] == "unavailable"
    assert trade["realized_pnl_krw"] is None
    assert "missing_average_cost_basis" in trade["outcome_quality_flags"]
    assert report["summary"]["realized_pnl_krw"] is None
    assert report["summary"]["estimated_trade_count"] == 0
    assert report["summary"]["unavailable_trade_count"] == 1

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_headlines_broker_total_asset_change_with_unrealized_drawdown(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=1,
        exec_agent_max_account_snapshot_age_seconds=20000,
    )
    plan = ExecutionPlan(
        id="plan-asset-drawdown",
        external_plan_id="plan-asset-drawdown",
        source="kai-trading-core",
        strategy_run_id="run-asset-drawdown",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="small-fifo-loss",
        symbol="005930",
        buy_price="100000",
        sell_price="95632.5",
        quantity=10,
        filled_day=25,
    )
    session.add(
        ExecutionPosition(
            id="pos-asset-drawdown",
            mode="paper",
            provider="kis",
            account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="000660",
            quantity=10,
            average_price=Decimal("200000"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 26, 6, 0, tzinfo=UTC),
            raw_payload={
                "balance": {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "000660",
                            "prdt_name": "SK hynix",
                            "hldg_qty": "10",
                            "ord_psbl_qty": "10",
                            "pchs_avg_pric": "200000",
                            "pchs_amt": "2000000",
                            "evlu_amt": "1637474",
                            "evlu_pfls_amt": "-362526",
                            "evlu_pfls_rt": "-18.1263",
                        }
                    ],
                    "output2": [
                        {
                            "tot_evlu_amt": "9593799",
                            "nass_amt": "9593799",
                            "bfdy_tot_asst_evlu_amt": "10000000",
                            "asst_icdc_amt": "-406201",
                            "evlu_pfls_smtl_amt": "-362526",
                            "pchs_amt_smtl_amt": "2000000",
                            "evlu_amt_smtl_amt": "1637474",
                            "dnca_tot_amt": "7956325",
                        }
                    ],
                }
            },
        )
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 26, 9, 0, tzinfo=UTC),
    )

    assert report["summary"]["primary_performance_metric"] == "broker_total_asset_change"
    assert report["summary"]["broker_total_asset_change_krw"] == "-406201.0000"
    assert report["summary"]["realized_pnl_krw"] == "-43675.0000"
    broker_asset = report["broker_asset_performance"]
    assert broker_asset["baseline_total_asset_krw"] == "10000000.0000"
    assert broker_asset["latest_total_asset_krw"] == "9593799.0000"
    assert broker_asset["latest_net_asset_krw"] == "9593799.0000"
    assert broker_asset["asset_change_pct"] == "-4.0620"
    assert broker_asset["broker_unrealized_pnl_krw"] == "-362526.0000"
    assert broker_asset["holdings_source"] == "execution_positions.raw_payload.balance.output1"
    assert broker_asset["holdings"][0]["symbol"] == "000660"
    assert broker_asset["holdings"][0]["market_value_krw"] == "1637474.0000"
    assert broker_asset["holdings"][0]["unrealized_pnl_krw"] == "-362526.0000"
    assert broker_asset["divergence_warning"]["difference_krw"] == "-362526.0000"
    assert report["prompt_snippet"]["summary"].startswith("Broker total asset change: -406201.0000 KRW")
    assert report["prompt_snippet"]["broker_asset_warnings"][0].startswith("Divergence warning:")
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert markdown.index("Broker total asset change: -406201.0000 KRW") < markdown.index("FIFO realized P/L: -43675.0000 KRW")

    session.close()
    engine.dispose()


def test_broker_asset_performance_fails_closed_across_account_scopes(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=1,
        exec_agent_max_account_snapshot_age_seconds=20000,
    )

    def position(*, symbol: str, scope: str, observed_at: datetime) -> ExecutionPosition:
        return ExecutionPosition(
            mode="paper",
            provider="kis",
            account_scope=scope,
            exchange="KRX",
            symbol=symbol,
            quantity=1,
            average_price=Decimal("100"),
            updated_from="reconciliation",
            last_reconciled_at=observed_at,
            raw_payload={
                "balance": {
                    "output2": [
                        {
                            "tot_evlu_amt": "1000",
                            "nass_amt": "1000",
                            "bfdy_tot_asst_evlu_amt": "900",
                            "asst_icdc_amt": "100",
                        }
                    ]
                }
            },
        )

    session.add_all(
        [
            position(
                symbol="005930",
                scope="a" * 24,
                observed_at=datetime(2026, 6, 26, 5, 0, tzinfo=UTC),
            ),
            position(
                symbol="000660",
                scope="b" * 24,
                observed_at=datetime(2026, 6, 26, 6, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 26, 9, 0, tzinfo=UTC),
    )

    broker_asset = report["broker_asset_performance"]
    assert broker_asset["available"] is False
    assert broker_asset["source_failed"] == (
        "multiple broker account scopes are present in balance snapshots; "
        "account-level asset performance is unavailable"
    )
    assert report["summary"]["primary_performance_metric"] != (
        "broker_total_asset_change"
    )
    session.close()
    engine.dispose()


def test_trade_outcome_feedback_surfaces_broker_raw_normalized_drift(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=1,
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    plan = ExecutionPlan(
        id="plan-raw-drift",
        external_plan_id="plan-raw-drift",
        source="kai-trading-core",
        strategy_run_id="run-raw-drift",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="raw-drift",
        symbol="005930",
        buy_price="100000",
        sell_price="101000",
        quantity=1,
        filled_day=26,
    )
    session.add(
        ExecutionPosition(
            id="pos-raw-drift-carrier",
            mode="paper",
            provider="kis",
            account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="005930",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 27, 4, 0, tzinfo=UTC),
            raw_payload={
                "balance": {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "002380",
                            "prdt_name": "KCC",
                            "hldg_qty": "3",
                            "ord_psbl_qty": "3",
                            "pchs_avg_pric": "350000",
                            "prpr": "331000",
                            "evlu_amt": "993000",
                            "evlu_pfls_amt": "-57000",
                            "evlu_pfls_rt": "-5.4286",
                        },
                        {
                            "pdno": "031330",
                            "prdt_name": "EG",
                            "hldg_qty": "7",
                            "ord_psbl_qty": "7",
                            "pchs_avg_pric": "9500",
                            "prpr": "8900",
                            "evlu_amt": "62300",
                            "evlu_pfls_amt": "-4200",
                            "evlu_pfls_rt": "-6.3158",
                        },
                    ],
                    "output2": [
                        {
                            "tot_evlu_amt": "9000000",
                            "nass_amt": "9000000",
                            "bfdy_tot_asst_evlu_amt": "9010000",
                            "asst_icdc_amt": "-10000",
                            "evlu_pfls_smtl_amt": "-61200",
                        }
                    ],
                }
            },
        )
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 27, 4, 2, tzinfo=UTC),
    )

    broker_asset = report["broker_asset_performance"]
    assert broker_asset["raw_balance_valid"] is True
    assert broker_asset["raw_balance_fresh"] is True
    assert broker_asset["raw_normalized_position_drift_count"] == 2
    drift_by_symbol = {
        row["symbol"]: row
        for row in broker_asset["raw_normalized_position_drift"]
    }
    assert drift_by_symbol["002380"]["reason"] == "broker_raw_open_missing_from_normalized"
    assert drift_by_symbol["002380"]["raw_quantity"] == "3.0000"
    assert drift_by_symbol["002380"]["normalized_quantity"] == "0.0000"
    assert drift_by_symbol["002380"]["raw_current_price"] == "331000.0000"
    assert drift_by_symbol["002380"]["raw_unrealized_pnl_krw"] == "-57000.0000"
    assert drift_by_symbol["031330"]["raw_quantity"] == "7.0000"
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert "Broker raw vs normalized position drift" in markdown
    assert "002380: raw_qty=3.0000, normalized_qty=0.0000" in markdown

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_bounds_execution_position_raw_payload_reads(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=1,
        exec_agent_max_account_snapshot_age_seconds=20000,
    )
    plan = ExecutionPlan(
        id="plan-bounded-raw-payload",
        external_plan_id="plan-bounded-raw-payload",
        source="kai-trading-core",
        strategy_run_id="run-bounded-raw-payload",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="bounded-raw-payload",
        symbol="005930",
        buy_price="100000",
        sell_price="101000",
        quantity=1,
        filled_day=25,
    )
    for index in range(40):
        session.add(
            ExecutionPosition(
                id=f"pos-large-raw-{index}",
                mode="paper",
                provider="kis",
                account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
                exchange="KRX",
                symbol=f"9{index:05d}",
                quantity=0,
                average_price=None,
                updated_from="historical-reconciliation",
                last_reconciled_at=datetime(2026, 5, 1, 6, index % 60, tzinfo=UTC),
                raw_payload={"balance": {"output2": [{"tot_evlu_amt": str(1000000 + index)}]}, "blob": "x" * 1000},
            )
        )
    session.add(
        ExecutionPosition(
            id="pos-latest-balance",
            mode="paper",
            provider="kis",
            account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="005930",
            quantity=1,
            average_price=Decimal("100000"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 26, 6, 0, tzinfo=UTC),
            raw_payload={
                "balance": {
                    "output1": [{"pdno": "005930", "hldg_qty": "1", "evlu_amt": "101000"}],
                    "output2": [
                        {
                            "tot_evlu_amt": "1001000",
                            "nass_amt": "1001000",
                            "bfdy_tot_asst_evlu_amt": "1000000",
                            "asst_icdc_amt": "1000",
                        }
                    ],
                }
            },
        )
    )
    session.commit()
    statements: list[str] = []

    def capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany) -> None:
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_sql)
    try:
        report = generate_trade_outcome_feedback(
            settings=settings,
            db=session,
            as_of=datetime(2026, 6, 26, 9, 0, tzinfo=UTC),
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture_sql)

    raw_payload_reads = [
        statement
        for statement in statements
        if "execution_positions.raw_payload" in statement
        and "FROM execution_positions" in statement
    ]
    assert report["broker_asset_performance"]["available"] is True
    assert raw_payload_reads
    assert all("LIMIT" in statement.upper() for statement in raw_payload_reads)

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_reports_account_drawdown_gate_state(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        account_drawdown_gate_enabled=True,
    )
    plan = ExecutionPlan(
        id="plan-drawdown-gate-report",
        external_plan_id="plan-drawdown-gate-report",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-drawdown-gate-report",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    summary = {
        "gate_name": "daily_total_asset_drawdown_pct",
        "current_metric": "2.0000",
        "threshold": "1.0000",
        "baseline_value": "10000000.0000",
        "order_idempotency_key": "drawdown-report-order",
        "execution_plan_item_id": item.id,
        "sells_risk_reduction_allowed": True,
    }
    session.add_all(
        [
            plan,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=summary,
                payload={"decision": summary},
                error_message="daily total asset drawdown percent threshold reached",
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    assert report["account_drawdown_gate"]["available"] is True
    assert report["account_drawdown_gate"]["enabled"] is True
    assert report["account_drawdown_gate"]["provider"] == "kis"
    assert report["account_drawdown_gate"]["reason"] is None
    assert report["account_drawdown_gate"]["gated"] is True
    assert (
        report["account_drawdown_gate"]["latest_decision"]["summary"]["gate_name"]
        == "daily_total_asset_drawdown_pct"
    )
    assert "Account drawdown gate active: daily_total_asset_drawdown_pct" in report["prompt_snippet"]["account_drawdown_gate"]
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert "### Account Drawdown Gate" in markdown
    assert "gate=daily_total_asset_drawdown_pct" in markdown
    assert "sells/risk-reduction allowed=True" in markdown

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_latest_allowed_order_clears_account_drawdown_gate(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        account_drawdown_gate_enabled=True,
    )
    plan = ExecutionPlan(
        id="plan-drawdown-gate-cleared",
        external_plan_id="plan-drawdown-gate-cleared",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-drawdown-gate-cleared",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    blocked_summary = {
        "gate_name": "daily_total_asset_drawdown_pct",
        "current_metric": "2.0000",
        "threshold": "1.0000",
        "baseline_value": "10000000.0000",
        "order_idempotency_key": "older-blocked-order",
        "execution_plan_item_id": item.id,
        "sells_risk_reduction_allowed": True,
    }
    allowed_summary = {
        "blocked": False,
        "order_idempotency_key": "newer-allowed-order",
        "execution_plan_item_id": item.id,
    }
    session.add_all(
        [
            plan,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=blocked_summary,
                payload={"blocked": True, "decision": blocked_summary},
                error_message="daily total asset drawdown percent threshold reached",
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
            BrokerOrder(
                id="order-newer-allowed-drawdown-gate",
                plan_item=item,
                broker_name="kis",
                mode="paper",
                idempotency_key="newer-allowed-order",
                broker_order_id="kis-newer-allowed-order",
                symbol="005930",
                side="buy",
                status="submitted",
                quantity=1,
                response_metadata={"account_drawdown_gate": allowed_summary},
                raw_broker_payload={"blocked": False},
                last_broker_sync_at=datetime(2026, 6, 20, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    assert report["account_drawdown_gate"]["gated"] is False
    assert report["account_drawdown_gate"]["latest_decision"]["source"] == "broker_order"
    assert (
        report["account_drawdown_gate"]["latest_decision"]["summary"]["order_idempotency_key"]
        == "newer-allowed-order"
    )
    assert (
        report["prompt_snippet"]["account_drawdown_gate"]
        == "Account drawdown gate is not active in the latest audited decision."
    )
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert "Account drawdown gate: not currently gated by the latest audited decision." in markdown

    session.close()
    engine.dispose()


def test_account_drawdown_gate_state_keeps_recovery_scopes_independent() -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        account_drawdown_gate_enabled=True,
    )
    account_fingerprint = recovery_account_fingerprint(settings)
    krw_scope = f"kis:{account_fingerprint}:krw"
    usd_scope = f"kis:{account_fingerprint}:usd"
    plan = ExecutionPlan(
        id="plan-drawdown-scope-state",
        external_plan_id="plan-drawdown-scope-state",
        source="kai-trading-core",
        mode="paper",
    )
    krw_item = ExecutionPlanItem(
        id="item-drawdown-scope-krw",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    usd_item = ExecutionPlanItem(
        id="item-drawdown-scope-usd",
        plan=plan,
        symbol="AAPL",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )

    def source_order(*, item: ExecutionPlanItem, order_id: str, scope: str) -> BrokerOrder:
        summary = {
            "gate_name": "daily_total_asset_drawdown_pct",
            "order_idempotency_key": order_id,
            "execution_plan_item_id": item.id,
            "sells_risk_reduction_allowed": True,
        }
        return BrokerOrder(
            id=order_id,
            plan_item=item,
            broker_name="kis",
            mode="paper",
            idempotency_key=order_id,
            symbol=item.symbol,
            side="buy",
            status="rejected",
            quantity=1,
            request_metadata={
                "account_drawdown_recovery_policy": {
                    "policy_version": 1,
                    "recovery_scope": scope,
                }
            },
            response_metadata={"account_drawdown_gate": summary},
            raw_broker_payload={"blocked": True, "decision": summary},
            last_error_classification="validation",
            last_error_code="daily_total_asset_drawdown_pct",
            last_broker_sync_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
        )

    krw_order = source_order(
        item=krw_item,
        order_id="order-drawdown-scope-krw",
        scope=krw_scope,
    )
    usd_order = source_order(
        item=usd_item,
        order_id="order-drawdown-scope-usd",
        scope=usd_scope,
    )
    session.add_all(
        [
            plan,
            krw_order,
            usd_order,
            ExecutionPlanItemAudit(
                execution_plan_item_id=krw_item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=(krw_order.response_metadata or {})["account_drawdown_gate"],
                payload=krw_order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=usd_item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=(usd_order.response_metadata or {})["account_drawdown_gate"],
                payload=usd_order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=krw_item.id,
                event_type="account_drawdown_recovery_released",
                source="broker_order_service",
                summary=_current_recovery_release_summary(
                    settings=settings,
                    source_order=krw_order,
                ),
                occurred_at=datetime(2026, 6, 20, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    partially_recovered = latest_account_drawdown_gate_state(
        settings=settings,
        db=session,
    )

    assert partially_recovered["gated"] is True
    assert set(partially_recovered["active_recovery_scopes"]) == {usd_scope}
    assert partially_recovered["latest_decision"]["order_id"] == usd_order.id

    session.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=usd_item.id,
            event_type="account_drawdown_recovery_released",
            source="broker_order_service",
            summary=_current_recovery_release_summary(
                settings=settings,
                source_order=usd_order,
            ),
            occurred_at=datetime(2026, 6, 20, 1, 5, tzinfo=UTC),
        )
    )
    session.commit()

    recovered = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert recovered["gated"] is False
    assert recovered["active_recovery_scopes"] == {}
    assert recovered["latest_decision"]["source"] == "account_drawdown_recovery_audit"
    assert recovered["latest_decision"]["event_type"] == "account_drawdown_recovery_released"

    session.close()
    engine.dispose()


def test_account_drawdown_gate_state_preserves_nonrecoverable_policy_v1_block() -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", account_drawdown_gate_enabled=True)
    plan = ExecutionPlan(
        id="plan-drawdown-nonrecoverable-state",
        external_plan_id="plan-drawdown-nonrecoverable-state",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-drawdown-nonrecoverable-state",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    summary = {
        "gate_name": "account_drawdown_snapshot_unavailable",
        "order_idempotency_key": "order-drawdown-nonrecoverable-state",
        "execution_plan_item_id": item.id,
        "sells_risk_reduction_allowed": True,
    }
    order = BrokerOrder(
        id="order-drawdown-nonrecoverable-state",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="order-drawdown-nonrecoverable-state",
        symbol="005930",
        side="buy",
        status="rejected",
        quantity=1,
        request_metadata={
            "account_drawdown_recovery_policy": {
                "policy_version": 1,
                "recovery_scope": "kis:opaque-account:krw",
            }
        },
        response_metadata={"account_drawdown_gate": summary},
        raw_broker_payload={"blocked": True, "decision": summary},
        last_error_classification="validation",
        last_error_code="account_drawdown_snapshot_unavailable",
        last_broker_sync_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
    )
    session.add_all(
        [
            plan,
            order,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=summary,
                payload=order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    state = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert state["gated"] is True
    assert state["active_recovery_scopes"] == {}
    assert (
        state["latest_decision"]["summary"]["gate_name"]
        == "account_drawdown_snapshot_unavailable"
    )

    session.close()
    engine.dispose()


@pytest.mark.parametrize(
    "scope_settings",
    [
        {
            "kis_account_number": "11112222",
            "account_drawdown_recovery_scope_secret": "stable-scope-secret",
        },
        {
            "kis_account_number": "11112222",
            "execution_position_account_scope": "primary-kis-account",
        },
    ],
    ids=("stable-secret", "configured-route-label"),
)
def test_account_drawdown_gate_state_hides_foreign_account_policy_block_with_scope_authority(
    scope_settings: dict[str, str],
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        account_drawdown_gate_enabled=True,
        **scope_settings,
    )
    plan = ExecutionPlan(
        id="plan-drawdown-foreign-account-state",
        external_plan_id="plan-drawdown-foreign-account-state",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-drawdown-foreign-account-state",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    summary = {
        "gate_name": "account_drawdown_snapshot_unavailable",
        "order_idempotency_key": "order-drawdown-foreign-account-state",
        "execution_plan_item_id": item.id,
        "sells_risk_reduction_allowed": True,
    }
    order = BrokerOrder(
        id="order-drawdown-foreign-account-state",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="order-drawdown-foreign-account-state",
        symbol="005930",
        side="buy",
        status="rejected",
        quantity=1,
        request_metadata={
            "account_drawdown_recovery_policy": {
                "policy_version": 1,
                "recovery_scope": "kis:foreign-account-fingerprint:krw",
            }
        },
        response_metadata={"account_drawdown_gate": summary},
        raw_broker_payload={"blocked": True, "decision": summary},
        last_error_classification="validation",
        last_error_code="account_drawdown_snapshot_unavailable",
        last_broker_sync_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
    )
    session.add_all(
        [
            plan,
            order,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=summary,
                payload=order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    state = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert state["gated"] is False
    assert state["active_recovery_scopes"] == {}
    assert state["latest_decision"] is None

    session.close()
    engine.dispose()


def test_account_drawdown_gate_state_ignores_empty_order_metadata() -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", account_drawdown_gate_enabled=True)
    session.add(
        BrokerOrder(
            id="order-with-empty-response-metadata",
            broker_name="kis",
            mode="paper",
            idempotency_key="order-with-empty-response-metadata",
            symbol="005930",
            side="sell",
            status="submitted",
            quantity=1,
            response_metadata={},
            last_broker_sync_at=datetime(2026, 6, 20, 2, 0, tzinfo=UTC),
        )
    )
    session.commit()

    state = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert state["gated"] is False
    assert state["latest_decision"] is None

    session.close()
    engine.dispose()


def test_foreign_account_allowed_order_cannot_clear_current_scoped_block() -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        account_drawdown_gate_enabled=True,
        kis_account_number="11112222",
        account_drawdown_recovery_scope_secret="stable-scope-secret",
    )
    current_scope = (
        f"kis:{recovery_account_fingerprint(settings)}:krw"
    )
    plan = ExecutionPlan(
        id="plan-current-scoped-nonrecoverable-block",
        external_plan_id="plan-current-scoped-nonrecoverable-block",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-current-scoped-nonrecoverable-block",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    blocked_summary = {
        "gate_name": "account_drawdown_snapshot_unavailable",
        "order_idempotency_key": "current-scoped-block",
        "execution_plan_item_id": item.id,
    }
    blocked_order = BrokerOrder(
        id="current-scoped-block",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="current-scoped-block",
        symbol="005930",
        side="buy",
        status="rejected",
        quantity=1,
        request_metadata={
            "account_drawdown_recovery_policy": {
                "policy_version": 1,
                "recovery_scope": current_scope,
            }
        },
        response_metadata={"account_drawdown_gate": blocked_summary},
        raw_broker_payload={"blocked": True, "decision": blocked_summary},
        last_error_classification="validation",
        last_error_code="account_drawdown_snapshot_unavailable",
        last_broker_sync_at=datetime(2026, 6, 20, 1, 0, tzinfo=UTC),
    )
    allowed_summary = {
        "blocked": False,
        "order_idempotency_key": "foreign-account-allowed",
        "execution_plan_item_id": None,
    }
    foreign_allowed = BrokerOrder(
        id="foreign-account-allowed",
        broker_name="kis",
        mode="paper",
        idempotency_key="foreign-account-allowed",
        symbol="000660",
        side="buy",
        status="submitted",
        quantity=1,
        request_metadata={
            "account_drawdown_recovery_policy": {
                "policy_version": 1,
                "recovery_scope": "kis:foreign-account-fingerprint:krw",
            }
        },
        response_metadata={"account_drawdown_gate": allowed_summary},
        raw_broker_payload={"accepted": True},
        submitted_at=datetime(2026, 6, 20, 1, 5, tzinfo=UTC),
        last_broker_sync_at=datetime(2026, 6, 20, 1, 5, tzinfo=UTC),
    )
    session.add_all(
        [
            plan,
            blocked_order,
            foreign_allowed,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=blocked_summary,
                payload=blocked_order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    state = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert state["gated"] is True
    assert state["latest_decision"]["summary"]["gate_name"] == (
        "account_drawdown_snapshot_unavailable"
    )

    session.close()
    engine.dispose()


def test_account_drawdown_gate_state_honors_legacy_source_release() -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", account_drawdown_gate_enabled=True)
    plan = ExecutionPlan(
        id="plan-drawdown-legacy-release-state",
        external_plan_id="plan-drawdown-legacy-release-state",
        source="kai-trading-core",
        mode="paper",
    )
    item = ExecutionPlanItem(
        id="item-drawdown-legacy-release-state",
        plan=plan,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="new_buy_blocked",
    )
    summary = {
        "gate_name": "daily_total_asset_drawdown_pct",
        "order_idempotency_key": "order-drawdown-legacy-release-state",
        "execution_plan_item_id": item.id,
        "sells_risk_reduction_allowed": True,
    }
    order = BrokerOrder(
        id="order-drawdown-legacy-release-state",
        plan_item=item,
        broker_name="kis",
        mode="paper",
        idempotency_key="order-drawdown-legacy-release-state",
        symbol="005930",
        side="buy",
        status="rejected",
        quantity=1,
        response_metadata={"account_drawdown_gate": summary},
        raw_broker_payload={"blocked": True, "decision": summary},
        last_error_classification="validation",
        last_error_code="daily_total_asset_drawdown_pct",
        last_broker_sync_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
    )
    session.add_all(
        [
            plan,
            order,
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_new_buy_blocked",
                source="broker_order_service",
                summary=summary,
                payload=order.raw_broker_payload,
                occurred_at=datetime(2026, 6, 20, 0, 30, tzinfo=UTC),
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type="account_drawdown_gate_disabled_release",
                source="broker_order_service",
                summary=_current_recovery_release_summary(
                    settings=settings,
                    source_order=order,
                    gate_disabled=True,
                ),
                occurred_at=datetime(2026, 6, 20, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    state = latest_account_drawdown_gate_state(settings=settings, db=session)

    assert state["gated"] is False
    assert state["active_recovery_scopes"] == {}
    assert state["latest_decision"]["event_type"] == (
        "account_drawdown_gate_disabled_release"
    )

    session.close()
    engine.dispose()


def test_trade_outcome_feedback_multi_day_does_not_headline_previous_day_broker_asset_change(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=5,
    )
    plan = ExecutionPlan(
        id="plan-multiday-asset",
        external_plan_id="plan-multiday-asset",
        source="kai-trading-core",
        strategy_run_id="run-multiday-asset",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="multiday-fifo-win",
        symbol="005930",
        buy_price="100000",
        sell_price="101000",
        quantity=10,
        filled_day=22,
    )
    session.add(
        ExecutionPosition(
            id="pos-multiday-latest-only",
            mode="paper",
            provider="kis",
            account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="000660",
            quantity=10,
            average_price=Decimal("200000"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 26, 6, 0, tzinfo=UTC),
            raw_payload={
                "balance": {
                    "output2": [
                        {
                            "tot_evlu_amt": "9593799",
                            "nass_amt": "9593799",
                            "bfdy_tot_asst_evlu_amt": "10000000",
                            "asst_icdc_amt": "-406201",
                        }
                    ],
                }
            },
        )
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 26, 9, 0, tzinfo=UTC),
    )

    assert report["summary"]["primary_performance_metric"] == "fifo_realized_pnl"
    assert report["summary"]["realized_pnl_krw"] == "10000.0000"
    assert "broker_total_asset_change_krw" not in report["summary"]
    broker_asset = report["broker_asset_performance"]
    assert broker_asset["available"] is False
    assert "previous-day fields bfdy_tot_asst_evlu_amt/asst_icdc_amt do not cover the report lookback" in broker_asset["source_failed"]
    assert report["prompt_snippet"]["summary"].startswith("Recent closed trades: pnl=10000.0000 KRW")
    assert report["prompt_snippet"]["broker_asset_warnings"][0].startswith("Broker asset performance unavailable:")

    session.close()
    engine.dispose()


@pytest.mark.parametrize(
    "output2",
    [
        {"ord_psbl_cash": "123456"},
        {
            "tot_evlu_amt": "Infinity",
            "nass_amt": "NaN",
            "bfdy_tot_asst_evlu_amt": "-Infinity",
            "asst_icdc_amt": "Infinity",
        },
    ],
    ids=["missing", "nonfinite"],
)
def test_trade_outcome_feedback_output2_without_finite_asset_fields_is_unavailable(
    tmp_path,
    output2: dict[str, str],
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=1,
    )
    plan = ExecutionPlan(
        id="plan-missing-asset-fields",
        external_plan_id="plan-missing-asset-fields",
        source="kai-trading-core",
        strategy_run_id="run-missing-asset-fields",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="missing-asset-fields",
        symbol="005930",
        buy_price="100000",
        sell_price="101000",
        quantity=10,
        filled_day=25,
    )
    session.add(
        ExecutionPosition(
            id="pos-missing-asset-fields",
            mode="paper",
            provider="kis",
            account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
            exchange="KRX",
            symbol="000660",
            quantity=10,
            average_price=Decimal("200000"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 26, 6, 0, tzinfo=UTC),
            raw_payload={"balance": {"output2": [output2]}},
        )
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 26, 9, 0, tzinfo=UTC),
    )

    assert report["summary"]["primary_performance_metric"] == "fifo_realized_pnl"
    assert report["summary"]["realized_pnl_krw"] == "10000.0000"
    assert "broker_total_asset_change_krw" not in report["summary"]
    broker_asset = report["broker_asset_performance"]
    assert broker_asset["available"] is False
    assert broker_asset["source_failed"] == (
        "broker balance output2 missing parseable total/baseline/change asset fields; "
        "cannot headline broker total asset change"
    )
    assert report["prompt_snippet"]["summary"].startswith("Recent closed trades: pnl=10000.0000 KRW")

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
    for order in orders:
        order.venue = "KRX"
        order.asset_class = "stock"
        order.request_metadata = {
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "kis:test-account:krw",
            },
        }
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
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[1],
                broker_fill_id="risk-fill-stop-sell",
                symbol="005930",
                side="sell",
                quantity=10,
                price=Decimal("99000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 16, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[2],
                broker_fill_id="risk-fill-buy-b",
                symbol="000660",
                side="buy",
                quantity=5,
                price=Decimal("100000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[3],
                broker_fill_id="risk-fill-target-sell",
                symbol="000660",
                side="sell",
                quantity=5,
                price=Decimal("130000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 17, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[4],
                broker_fill_id="risk-fill-buy-c",
                symbol="035420",
                side="buy",
                quantity=2,
                price=Decimal("100000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 15, 0, 0, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=orders[5],
                broker_fill_id="risk-fill-max-holding-sell",
                symbol="035420",
                side="sell",
                quantity=2,
                price=Decimal("95000"),
                **COMPLETE_KRW_FILL_ECONOMICS,
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


def test_trade_outcome_feedback_sector_lookup_caps_each_symbol(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    plan = ExecutionPlan(
        id="plan-sector-cap",
        external_plan_id="plan-sector-cap",
        source="kai-trading-core",
        strategy_run_id="run-sector-cap",
        mode="paper",
    )
    session.add(plan)
    for index in range(45):
        session.add(
            ExecutionPlanItem(
                id=f"item-sector-cap-early-{index}",
                plan=plan,
                plan_item_index=index,
                symbol="000001",
                side="buy",
                intent="next_open_buy",
                status="filled",
                risk_context={"sector": "Crowded Early Sector"},
            )
        )
    session.add(
        ExecutionPlanItem(
            id="item-sector-cap-late",
            plan=plan,
            plan_item_index=99,
            symbol="999999",
            side="buy",
            intent="next_open_buy",
            status="filled",
            risk_context={"sector": "Late Active Sector"},
        )
    )
    session.add_all(
        [
            ExecutionPosition(
                id="pos-sector-cap-early",
                mode="paper",
                symbol="000001",
                quantity=1,
                average_price=Decimal("100000"),
                updated_from="broker_reconciliation",
                last_reconciled_at=datetime(2026, 6, 20, 0, 9, tzinfo=UTC),
            ),
            ExecutionPosition(
                id="pos-sector-cap-late",
                mode="paper",
                symbol="999999",
                quantity=1,
                average_price=Decimal("100000"),
                updated_from="broker_reconciliation",
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

    sectors = {row["sector"] for row in report["portfolio_risk"]["sector_concentration"]}
    assert "Crowded Early Sector" in sectors
    assert "Late Active Sector" in sectors

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


def test_trade_outcome_feedback_degrades_mixed_currency_portfolio_aggregation(
    tmp_path,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    reconciled_at = datetime(2026, 6, 20, 0, 9, tzinfo=UTC)
    krw_identity = PositionIdentity(
        provider="kis",
        account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
        exchange="KRX",
    )
    usd_identity = PositionIdentity(
        provider="kis",
        account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
        exchange="NASD",
    )
    session.add_all(
        [
            ExecutionPosition(
                id="pos-mixed-krw",
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100000"),
                provider=krw_identity.provider,
                account_scope=krw_identity.account_scope,
                exchange=krw_identity.exchange,
                updated_from="broker_reconciliation",
                last_reconciled_at=reconciled_at,
                raw_payload=payload_with_position_identity(
                    None,
                    identity=krw_identity,
                ),
            ),
            ExecutionPosition(
                id="pos-mixed-usd",
                mode="paper",
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("200"),
                provider=usd_identity.provider,
                account_scope=usd_identity.account_scope,
                exchange=usd_identity.exchange,
                updated_from="broker_reconciliation",
                last_reconciled_at=reconciled_at,
                raw_payload=payload_with_position_identity(
                    {
                        "_position_sync": {
                            "asset_class": "us_equity",
                            "exchange": "NASD",
                        }
                    },
                    identity=usd_identity,
                ),
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
    assert risk["aggregation_status"] == "degraded"
    assert (
        risk["aggregation_reason"]
        == "mixed_settlement_currencies_without_fx_conversion"
    )
    assert risk["aggregate_currency"] is None
    assert risk["total_market_value"] is None
    assert risk["total_market_value_krw"] is None
    assert risk["by_currency"] == {
        "KRW": {
            "position_count": 1,
            "known_market_value_count": 1,
            "known_market_value_subtotal": "1000000.0000",
            "total_market_value": "1000000.0000",
        },
        "USD": {
            "position_count": 1,
            "known_market_value_count": 1,
            "known_market_value_subtotal": "400.0000",
            "total_market_value": "400.0000",
        },
    }
    concentrations = {row["symbol"]: row for row in risk["top_position_concentration"]}
    assert concentrations["005930"]["market_value_krw"] == "1000000.0000"
    assert concentrations["AAPL"]["market_value_krw"] is None
    assert concentrations["AAPL"]["market_value"] == "400.0000"
    assert concentrations["005930"]["portfolio_weight_pct"] is None
    assert concentrations["AAPL"]["portfolio_weight_pct"] is None
    assert concentrations["005930"]["currency_weight_pct"] == "100.0000"
    assert concentrations["AAPL"]["currency_weight_pct"] == "100.0000"
    assert any(
        "Currency aggregation degraded" in line
        for line in report["prompt_snippet"]["top_portfolio_risks"]
    )

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


def test_downsize_replay_keeps_full_observed_cost_as_conservative_bound(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(
        id="plan-replay-fixed-cost",
        external_plan_id="plan-replay-fixed-cost",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="fixed-cost",
        symbol="COST",
        buy_price="100",
        sell_price="110",
        quantity=1,
        risk_context={"atr_pct": "12.5"},
        buy_transaction_cost="4",
        sell_transaction_cost="4",
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    downsize = rules["high_volatility_downsize"]
    example = downsize["top_matched_examples"][0]
    assert example["realized_pnl_krw"] == "2.0000"
    assert example["candidate_pnl_krw"] == "-3.0000"
    assert example["evidence"]["transaction_cost_replay_model"] == (
        "scaled_gross_minus_full_observed_cost"
    )
    assert downsize["net_pnl_delta_krw"] == "-5.0000"

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


def test_candidate_rule_replay_covers_high_volatility_chase_over_several_weeks(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path), trade_outcome_feedback_lookback_days=30)
    plan = ExecutionPlan(id="plan-replay-high-vol-chase", external_plan_id="plan-replay-high-vol-chase", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index, filled_day in enumerate((2, 9, 16), start=1):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"high-vol-chase-loss-{index}",
            symbol=f"HVC{index}",
            buy_price="100",
            sell_price="87",
            filled_day=filled_day,
            risk_context={
                "short_term_entry_quality": {"atr_pct_14": "17.2160", "return_5d": "44.5993"},
                "last_monitoring_event": {"reason": "intraday_watch_buy_day_high_cross"},
            },
        )
    for index, filled_day in enumerate((23, 24), start=1):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"non-chase-win-{index}",
            symbol=f"NCW{index}",
            buy_price="100",
            sell_price="108",
            filled_day=filled_day,
            risk_context={
                "short_term_entry_quality": {"atr_pct_14": "17.2160", "return_5d": "44.5993"},
                "last_monitoring_event": {"reason": "intraday_watch_buy_day_low_cross"},
            },
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 30, tzinfo=UTC))

    replay = report["candidate_rule_replay"]
    rules = {row["rule_id"]: row for row in replay["rules"]}
    chase = rules["high_volatility_chase_veto"]
    assert replay["baseline_closed_trade_count"] == 5
    assert chase["matched_trade_count"] == 3
    assert chase["skipped_entries"] == 3
    assert chase["avoided_losers"] == 3
    assert chase["skipped_winners"] == 0
    assert chase["recommendation"] == "promote_to_issue"
    assert chase["gross_pnl_delta_krw"] == "390.0000"
    assert len(chase["top_avoided_losers"]) == 3
    assert "high_volatility_chase_veto" in (tmp_path / "latest.md").read_text(encoding="utf-8")

    session.close()
    engine.dispose()


def test_high_volatility_chase_replay_uses_configured_return_threshold(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=30,
        watch_buy_high_volatility_chase_atr_pct_threshold=Decimal("15.0"),
        watch_buy_high_volatility_chase_ret5_threshold_pct=Decimal("40.0"),
    )
    plan = ExecutionPlan(
        id="plan-replay-high-vol-chase-configured-ret5",
        external_plan_id="plan-replay-high-vol-chase-configured-ret5",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="configured-ret5-below-threshold",
        symbol="HVCRET5",
        buy_price="100",
        sell_price="87",
        filled_day=12,
        risk_context={
            "short_term_entry_quality": {"atr_pct_14": "17.2160", "return_5d": "35.0000"},
            "last_monitoring_event": {"reason": "intraday_watch_buy_day_high_cross"},
        },
    )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 30, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    chase = rules["high_volatility_chase_veto"]
    assert chase["matched_trade_count"] == 0
    assert chase["skipped_entries"] == 0
    assert chase["gross_pnl_delta_krw"] == "0.0000"

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
    assert high_correlation["average_return_delta_pct"] == "0.0000"
    assert high_correlation["max_drawdown_proxy_krw"] == "-60.0000"
    assert high_correlation["estimated_net_pnl_delta_krw"] is not None
    markdown = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert "### Candidate Rule Replay" in markdown
    assert "recommendation=" in markdown

    session.close()
    engine.dispose()


def test_candidate_rule_replay_does_not_promote_post_entry_tighten_without_exit_context(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-no-progress", external_plan_id="plan-replay-no-progress", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"no-progress-loss-{index}",
            symbol=f"NPG{index}",
            buy_price="100",
            sell_price="92",
            filled_day=10 + index,
            risk_context={
                "post_entry_managed": True,
                "last_monitoring_event": {
                    "event": "post_entry_review_required",
                    "reasons": [
                        "gap_down_breached_risk_threshold",
                        "next_day_no_progress",
                        "max_holding_without_progress",
                    ],
                    "holding_session": 3,
                },
            },
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    assert tightened["recommendation"] == "needs_more_data"
    assert tightened["matched_trade_count"] == 3
    assert tightened["skipped_entries"] == 0
    assert tightened["avoided_losers"] == 0
    assert tightened["skipped_winners"] == 0
    assert tightened["size_multiplier"] == "1.0000"
    assert tightened["gross_pnl_delta_krw"] == "0.0000"
    assert tightened["top_avoided_losers"] == []
    evidence = tightened["top_matched_examples"][0]["evidence"]
    assert evidence["matched_reasons"] == [
        "gap_down_breached_risk_threshold",
        "max_holding_without_progress",
        "next_day_no_progress",
    ]
    assert evidence["replay_status"] == "missing_tightened_exit_price"

    session.close()
    engine.dispose()


def test_candidate_rule_replay_replays_post_entry_tightened_exit_price(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-tightened-exit", external_plan_id="plan-replay-tightened-exit", source="kai-trading-core", mode="paper")
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"tightened-exit-loss-{index}",
            symbol=f"TEX{index}",
            buy_price="100",
            sell_price="92",
            filled_day=10 + index,
            risk_context={
                "post_entry_managed": True,
                "post_entry_review": {
                    "requires_review": True,
                    "reasons": [
                        "gap_down_breached_risk_threshold",
                        "next_day_no_progress",
                        "max_holding_without_progress",
                    ],
                    "observed_price": "99.50",
                    "tightened_stop_price": "99.00",
                    "recorded_at": f"2026-06-{10 + index:02d}T04:00:00+00:00",
                    "exit_execution_evidence": {
                        "bid_price": "99.00",
                        "observed_at": f"2026-06-{10 + index:02d}T05:00:00+00:00",
                    },
                },
                "last_monitoring_event": {
                    "event": "post_entry_review_required",
                    "reasons": [
                        "gap_down_breached_risk_threshold",
                        "next_day_no_progress",
                        "max_holding_without_progress",
                    ],
                },
            },
        )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    assert tightened["recommendation"] == "promote_to_issue"
    assert tightened["promotion_eligible"] is True
    assert tightened["matched_trade_count"] == 3
    assert tightened["replay_eligible_count"] == 3
    assert tightened["replay_ineligible_count"] == 0
    assert tightened["skipped_entries"] == 0
    assert tightened["avoided_losers"] == 0
    assert tightened["estimated_net_pnl_delta_krw"] == "210.0000"
    assert tightened["gross_pnl_delta_krw"] == "210.0000"
    assert tightened["top_avoided_losers"] == []
    evidence = tightened["top_matched_examples"][0]["evidence"]
    assert evidence["replay_status"] == "replayed_tightened_exit_price"
    assert evidence["baseline_sell_price"] == "92.0000"
    assert evidence["replay_exit_price"] == "99.0000"
    assert evidence["replay_pnl_krw"] == "-10.0000"

    session.close()
    engine.dispose()


def test_candidate_rule_replay_excludes_tighten_recorded_after_actual_sell(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(
        id="plan-replay-post-sell-tighten",
        external_plan_id="plan-replay-post-sell-tighten",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"post-sell-tighten-{index}",
            symbol=f"PST{index}",
            buy_price="100",
            sell_price="92",
            filled_day=10 + index,
            risk_context={
                "post_entry_review": {
                    "reasons": [
                        "gap_down_breached_risk_threshold",
                        "next_day_no_progress",
                        "max_holding_without_progress",
                    ],
                    "tightened_stop_price": "99",
                    "recorded_at": f"2026-06-{11 + index:02d}T02:00:00+00:00",
                    "exit_execution_evidence": {
                        "bid_price": "99",
                        "observed_at": f"2026-06-{11 + index:02d}T02:01:00+00:00",
                    },
                }
            },
        )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    assert tightened["recommendation"] == "needs_more_data"
    assert tightened["promotion_eligible"] is False
    assert tightened["promotion_eligible_matched_trade_count"] == 0
    assert tightened["promotion_excluded_matched_trade_count"] == 3
    assert tightened["net_pnl_delta_krw"] == "0.0000"
    assert {
        row["evidence"]["replay_status"]
        for row in tightened["top_matched_examples"]
    } == {"tighten_recorded_at_or_after_actual_sell"}

    session.close()
    engine.dispose()


def test_candidate_rule_replay_uses_stored_tradable_price_below_tightened_stop(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(
        id="plan-replay-gap-below-stop",
        external_plan_id="plan-replay-gap-below-stop",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    _add_closed_trade(
        session,
        plan=plan,
        suffix="gap-below-stop",
        symbol="GAPSTOP",
        buy_price="100",
        sell_price="97",
        filled_day=10,
        risk_context={
            "post_entry_review": {
                "reasons": [
                    "gap_down_breached_risk_threshold",
                    "next_day_no_progress",
                    "max_holding_without_progress",
                ],
                "tightened_stop_price": "99",
                "recorded_at": "2026-06-10T04:00:00+00:00",
                "exit_execution_evidence": {
                    "bid_price": "95",
                    "observed_at": "2026-06-10T05:00:00+00:00",
                },
            }
        },
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    evidence = tightened["top_matched_examples"][0]["evidence"]
    assert evidence["replay_eligible"] is True
    assert evidence["execution_evidence_source"] == "stored_quote.bid_price"
    assert evidence["replay_exit_price"] == "95.0000"
    assert evidence["baseline_sell_price"] == "97.0000"
    assert tightened["net_pnl_delta_krw"] == "-20.0000"
    assert tightened["recommendation"] == "needs_more_data"

    session.close()
    engine.dispose()


def test_candidate_rule_replay_does_not_promote_three_tightens_without_price_path(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(
        id="plan-replay-no-price-path",
        external_plan_id="plan-replay-no-price-path",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    for index in range(3):
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"no-price-path-{index}",
            symbol=f"NPP{index}",
            buy_price="100",
            sell_price="92",
            filled_day=10 + index,
            risk_context={
                "post_entry_review": {
                    "reasons": [
                        "gap_down_breached_risk_threshold",
                        "next_day_no_progress",
                        "max_holding_without_progress",
                    ],
                    "observed_price": "99.50",
                    "tightened_stop_price": "99",
                    "recorded_at": f"2026-06-{10 + index:02d}T04:00:00+00:00",
                    "session_snapshot": {
                        "day_low": "90",
                        "observed_at": f"2026-06-{10 + index:02d}T05:00:00+00:00",
                    },
                }
            },
        )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    assert tightened["matched_trade_count"] == 3
    assert tightened["promotion_eligible_matched_trade_count"] == 0
    assert tightened["promotion_excluded_matched_trade_count"] == 3
    assert tightened["promotion_eligible"] is False
    assert tightened["recommendation"] == "needs_more_data"
    assert tightened["net_pnl_delta_krw"] == "0.0000"
    assert {
        row["evidence"]["replay_status"]
        for row in tightened["top_matched_examples"]
    } == {"missing_post_tighten_execution_evidence"}

    session.close()
    engine.dispose()


def test_candidate_rule_replay_needs_three_replay_eligible_tightens_for_promotion(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(
        id="plan-replay-one-evidenced-tighten",
        external_plan_id="plan-replay-one-evidenced-tighten",
        source="kai-trading-core",
        mode="paper",
    )
    session.add(plan)
    for index in range(3):
        post_entry_review: dict[str, object] = {
            "reasons": [
                "gap_down_breached_risk_threshold",
                "next_day_no_progress",
                "max_holding_without_progress",
            ],
            "tightened_stop_price": "99",
            "recorded_at": f"2026-06-{10 + index:02d}T04:00:00+00:00",
        }
        if index == 0:
            post_entry_review["exit_execution_evidence"] = {
                "bid_price": "99",
                "observed_at": "2026-06-10T05:00:00+00:00",
            }
        _add_closed_trade(
            session,
            plan=plan,
            suffix=f"one-evidenced-tighten-{index}",
            symbol=f"OET{index}",
            buy_price="100",
            sell_price="92",
            filled_day=10 + index,
            risk_context={"post_entry_review": post_entry_review},
        )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 20, tzinfo=UTC),
    )

    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    tightened = rules["post_entry_no_progress_exit_tighten"]
    assert tightened["matched_trade_count"] == 3
    assert tightened["replay_eligible_count"] == 1
    assert tightened["replay_ineligible_count"] == 2
    assert tightened["net_pnl_delta_krw"] == "70.0000"
    assert tightened["promotion_eligible"] is False
    assert tightened["recommendation"] == "needs_more_data"

    session.close()
    engine.dispose()


def test_candidate_rule_replay_uses_sell_post_entry_context(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    plan = ExecutionPlan(id="plan-replay-sell-context", external_plan_id="plan-replay-sell-context", source="kai-trading-core", mode="paper")
    buy_item = ExecutionPlanItem(
        id="item-buy-sell-context",
        plan=plan,
        symbol="SCTX",
        side="buy",
        intent="intraday_watch_buy",
        status="filled",
        quantity_policy={"kind": "shares", "quantity": 10},
        risk_context={},
    )
    sell_item = ExecutionPlanItem(
        id="item-sell-sell-context",
        plan=plan,
        symbol="SCTX",
        side="sell",
        intent="stop_sell",
        status="filled",
        quantity_policy={"kind": "shares", "quantity": 10},
        risk_context={
            "last_monitoring_event": {
                "event": "post_entry_review_required",
                "reasons": ["gap_down_breached_risk_threshold", "next_day_no_progress"],
            }
        },
    )
    buy_order = BrokerOrder(
        id="order-buy-sell-context",
        plan_item=buy_item,
        broker_name="kis",
        mode=plan.mode,
        idempotency_key="buy-sell-context",
        broker_order_id="kis-buy-sell-context",
        symbol="SCTX",
        side="buy",
        status="filled",
        quantity=10,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "kis:test-account:krw",
            },
        },
    )
    sell_order = BrokerOrder(
        id="order-sell-sell-context",
        plan_item=sell_item,
        broker_name="kis",
        mode=plan.mode,
        idempotency_key="sell-sell-context",
        broker_order_id="kis-sell-sell-context",
        symbol="SCTX",
        side="sell",
        status="filled",
        quantity=10,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "provider": "kis",
            "venue": "KRX",
            "asset_class": "stock",
            "exchange": "KRX",
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "kis:test-account:krw",
            },
        },
    )
    session.add_all(
        [
            buy_order,
            sell_order,
            ExecutionFill(
                broker_order=buy_order,
                broker_fill_id="fill-buy-sell-context",
                symbol="SCTX",
                side="buy",
                quantity=10,
                price=Decimal("100"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 10, 0, 30, tzinfo=UTC),
            ),
            ExecutionFill(
                broker_order=sell_order,
                broker_fill_id="fill-sell-sell-context",
                symbol="SCTX",
                side="sell",
                quantity=10,
                price=Decimal("92"),
                **COMPLETE_KRW_FILL_ECONOMICS,
                filled_at=datetime(2026, 6, 11, 1, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(settings=settings, db=session, as_of=datetime(2026, 6, 20, tzinfo=UTC))

    trade = report["closed_trades"][0]
    assert trade["candidate_rule_context"]["sell_candidate_rule_context"]["last_monitoring_event"]["reasons"] == [
        "gap_down_breached_risk_threshold",
        "next_day_no_progress",
    ]
    rules = {row["rule_id"]: row for row in report["candidate_rule_replay"]["rules"]}
    assert rules["post_entry_no_progress_exit_tighten"]["matched_trade_count"] == 1

    session.close()
    engine.dispose()


def test_exec_agent_payloads_include_latest_trade_outcome_feedback(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        json.dumps(
            {
                "version": "trade-outcome-feedback-v2",
                "mode": "paper",
                "execution_account_scope": {
                    "status": "single",
                    "provider": "kis",
                    "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
                },
                "generated_at": datetime.now(UTC).isoformat(),
                "lookback_days": 20,
                "closed_trade_count": 1,
                "summary": {"realized_pnl_krw": "-10000.0000"},
                "prompt_snippet": {
                    "summary": "Recent closed trades: pnl=-10000.0000 KRW",
                    "patterns_to_downsize_or_reject": ["Downsize standard_breakout"],
                    "positive_expectancy_patterns": [],
                    "required_hard_gates": ["Reject repeat worst-loss setups"],
                },
            }
        ),
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
        account_context={
            "available": False,
            "execution_account_scope": {
                "provider": "kis",
                "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
            },
        },
        candidate_universe=_candidate_universe_payload([]),
    )
    trigger_payload = _request_payload(
        settings=settings,
        item=item,
        side="buy",
        market_context={},
        account_context={
            "execution_account_scope": {
                "provider": "kis",
                "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
            }
        },
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


def test_latest_trade_outcome_feedback_requires_exact_account_scope(tmp_path) -> None:
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        json.dumps(
            {
                "version": "trade-outcome-feedback-v2",
                "mode": "paper",
                "execution_account_scope": {
                    "status": "single",
                    "provider": "kis",
                    "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
                },
                "generated_at": datetime.now(UTC).isoformat(),
                "lookback_days": 20,
                "closed_trade_count": 1,
                "summary": {"realized_pnl_krw": "-10000.0000"},
                "prompt_snippet": {"patterns_to_downsize_or_reject": ["loss-pattern"]},
            }
        ),
        encoding="utf-8",
    )

    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        provider="kis",
        account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
    ) is not None
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        provider="kis",
        account_scope="b" * 24,
    ) is None
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        provider="alpaca",
        account_scope=FEEDBACK_TEST_ACCOUNT_SCOPE,
    ) is None
    assert load_latest_trade_outcome_feedback_snippet(settings) is None


def test_mixed_account_feedback_is_never_reused_for_agent_decisions(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))
    for suffix, account_scope in (("a", "a" * 24), ("b", "b" * 24)):
        session.add(
            BrokerOrder(
                id=f"open-{suffix}",
                broker_name="kis",
                mode="paper",
                idempotency_key=f"open-{suffix}",
                broker_order_id=f"open-{suffix}",
                symbol="005930",
                side="buy",
                status="submitted",
                quantity=1,
                venue="KRX",
                asset_class="stock",
                request_metadata={
                    "execution_scope": {
                        "policy_version": 1,
                        "provider": "kis",
                        "account_scope": account_scope,
                        "venue": "KRX",
                        "asset_class": "stock",
                        "exchange": "KRX",
                    }
                },
            )
        )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime.now(UTC),
    )

    assert report["execution_account_scope"]["status"] == "mixed"
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        provider="kis",
        account_scope="a" * 24,
    ) is None

    session.close()
    engine.dispose()


def test_scoped_feedback_generation_excludes_foreign_and_legacy_route_rows(
    tmp_path,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=10,
    )
    account_a = "a" * 24
    account_b = "b" * 24
    plan_a = ExecutionPlan(
        id="plan-scoped-feedback-a",
        external_plan_id="plan-scoped-feedback-a",
        source="kai-trading-core",
        mode="paper",
    )
    plan_b = ExecutionPlan(
        id="plan-scoped-feedback-b",
        external_plan_id="plan-scoped-feedback-b",
        source="kai-trading-core",
        mode="paper",
    )
    session.add_all([plan_a, plan_b])
    _add_closed_trade(
        session,
        plan=plan_a,
        suffix="scoped-a",
        symbol="ACCOUNT-A-WINNER",
        buy_price="100",
        sell_price="110",
    )
    _add_closed_trade(
        session,
        plan=plan_b,
        suffix="scoped-b",
        symbol="ACCOUNT-B-LOSER",
        buy_price="100",
        sell_price="80",
    )
    _bind_closed_trade_to_execution_scope(
        session,
        suffix="scoped-a",
        account_scope=account_a,
    )
    _bind_closed_trade_to_execution_scope(
        session,
        suffix="scoped-b",
        account_scope=account_b,
    )
    session.add(
        BrokerOrder(
            id="legacy-open-feedback-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="legacy-open-feedback-order",
            broker_order_id="legacy-open-feedback-order",
            symbol="LEGACY-FOREIGN",
            side="buy",
            status="submitted",
            quantity=1,
            venue="KRX",
            asset_class="stock",
            request_metadata={
                "broker_account_scope": {
                    "policy_version": 1,
                    "scope": account_a,
                },
                "exchange": "KRX",
            },
        )
    )
    session.add(
        BrokerOrder(
            id="invalid-open-feedback-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="invalid-open-feedback-order",
            broker_order_id="invalid-open-feedback-order",
            symbol="INVALID-FOREIGN",
            side="buy",
            status="submitted",
            quantity=1,
            venue="KRX",
            asset_class="stock",
            request_metadata={
                "execution_scope": {
                    "policy_version": 1,
                    "provider": "kis",
                    "account_scope": "raw-account-reference",
                    "venue": "KRX",
                    "asset_class": "stock",
                    "exchange": "KRX",
                }
            },
        )
    )
    identity_a = PositionIdentity(
        provider="kis",
        account_scope=account_a,
        exchange="KRX",
    )
    identity_b = PositionIdentity(
        provider="kis",
        account_scope=account_b,
        exchange="KRX",
    )
    session.add_all(
        [
            ExecutionPosition(
                id="scoped-feedback-position-a",
                mode="paper",
                provider="kis",
                account_scope=account_a,
                exchange="KRX",
                symbol="POSITION-A",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="reconciliation",
                raw_payload=payload_with_position_identity(None, identity=identity_a),
                last_reconciled_at=datetime(2026, 6, 12, tzinfo=UTC),
            ),
            ExecutionPosition(
                id="scoped-feedback-position-b",
                mode="paper",
                provider="kis",
                account_scope=account_b,
                exchange="KRX",
                symbol="POSITION-B",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="reconciliation",
                raw_payload=payload_with_position_identity(None, identity=identity_b),
                last_reconciled_at=datetime(2026, 6, 12, tzinfo=UTC),
            ),
            ExecutionPosition(
                id="scoped-feedback-position-legacy",
                mode="paper",
                provider="kis",
                account_scope=account_a,
                exchange="KRX",
                symbol="POSITION-LEGACY",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="legacy",
                raw_payload={},
                last_reconciled_at=datetime(2026, 6, 12, tzinfo=UTC),
            ),
        ]
    )
    session.commit()

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=session,
        as_of=datetime(2026, 6, 12, tzinfo=UTC),
        provider="kis",
        account_scope=account_a,
        exchange="KRX",
    )

    assert report["execution_account_scope"] == {
        "status": "single",
        "provider": "kis",
        "account_scope": account_a,
        "exchange": "KRX",
        "evidence_count": 3,
    }
    assert report["closed_trade_count"] == 1
    assert report["closed_trades"][0]["symbol"] == "ACCOUNT-A-WINNER"
    assert report["summary"]["realized_pnl_krw"] == "100.0000"
    serialized_report = json.dumps(report)
    assert "ACCOUNT-B-LOSER" not in serialized_report
    assert "LEGACY-FOREIGN" not in serialized_report
    assert "INVALID-FOREIGN" not in serialized_report
    assert "POSITION-A" in serialized_report
    assert "POSITION-B" not in serialized_report
    assert "POSITION-LEGACY" not in serialized_report
    assert "raw-account-reference" not in serialized_report

    session.close()
    engine.dispose()


def test_feedback_freshness_and_artifacts_are_exact_account_and_exchange_scoped(
    tmp_path,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_lookback_days=10,
        trade_outcome_feedback_max_age_seconds=3600,
    )
    account_a = "a" * 24
    account_b = "b" * 24
    plan_a = ExecutionPlan(
        id="plan-feedback-freshness-a",
        external_plan_id="plan-feedback-freshness-a",
        source="kai-trading-core",
        mode="paper",
    )
    plan_b = ExecutionPlan(
        id="plan-feedback-freshness-b",
        external_plan_id="plan-feedback-freshness-b",
        source="kai-trading-core",
        mode="paper",
    )
    session.add_all([plan_a, plan_b])
    _add_closed_trade(
        session,
        plan=plan_a,
        suffix="freshness-a",
        symbol="FRESH-A",
        buy_price="100",
        sell_price="110",
    )
    _add_closed_trade(
        session,
        plan=plan_b,
        suffix="freshness-b",
        symbol="FRESH-B",
        buy_price="100",
        sell_price="90",
    )
    _bind_closed_trade_to_execution_scope(
        session,
        suffix="freshness-a",
        account_scope=account_a,
        exchange="KRX",
    )
    _bind_closed_trade_to_execution_scope(
        session,
        suffix="freshness-b",
        account_scope=account_b,
        exchange="KRX",
    )
    session.commit()
    as_of = datetime(2026, 6, 12, 0, 1, tzinfo=UTC)

    generated_a = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=as_of,
        provider="kis",
        account_scope=account_a,
        exchange="KRX",
    )
    generated_b = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=as_of,
        provider="kis",
        account_scope=account_b,
        exchange="KRX",
    )
    reused_a = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=as_of,
        provider="kis",
        account_scope=account_a,
        exchange="KRX",
    )

    assert generated_a["status"] == "generated"
    assert generated_b["status"] == "generated"
    assert reused_a["status"] == "fresh"
    assert not (tmp_path / "latest.json").exists()
    scoped_latest_paths = sorted(tmp_path.glob("latest.*.json"))
    assert len(scoped_latest_paths) == 2
    assert all(
        account_a not in path.name and account_b not in path.name
        for path in scoped_latest_paths
    )
    feedback_a = load_latest_trade_outcome_feedback_snippet(
        settings,
        now=as_of,
        provider="kis",
        account_scope=account_a,
        exchange="KRX",
    )
    feedback_b = load_latest_trade_outcome_feedback_snippet(
        settings,
        now=as_of,
        provider="kis",
        account_scope=account_b,
        exchange="KRX",
    )
    assert feedback_a is not None
    assert feedback_b is not None
    assert feedback_a["summary"]["realized_pnl_krw"] == "100.0000"
    assert feedback_b["summary"]["realized_pnl_krw"] == "-100.0000"
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        now=as_of,
        provider="kis",
        account_scope=account_a,
        exchange="NASD",
    ) is None

    session.close()
    engine.dispose()


def test_feedback_generation_rejects_invalid_requested_account_scope(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(runtime_mode="paper", trade_outcome_feedback_dir=str(tmp_path))

    result = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=datetime(2026, 6, 12, tzinfo=UTC),
        provider="kis",
        account_scope="raw-account-reference",
        exchange="KRX",
    )

    assert result == {
        "status": "unavailable",
        "version": REPORT_VERSION,
        "reason": "invalid_requested_execution_account_scope",
    }
    assert not (tmp_path / "latest.json").exists()

    session.close()
    engine.dispose()


def test_latest_trade_outcome_feedback_ignores_other_runtime_modes(tmp_path) -> None:
    settings = Settings(runtime_mode="live", trade_outcome_feedback_dir=str(tmp_path))
    (tmp_path / "latest.json").write_text(
        """
{
  "version": "trade-outcome-feedback-v2",
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


def test_latest_trade_outcome_feedback_ignores_stale_or_unversioned_artifacts(tmp_path) -> None:
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_max_age_seconds=3600,
    )
    (tmp_path / "latest.json").write_text(
        """
{
  "version": "trade-outcome-feedback-v2",
  "mode": "paper",
  "generated_at": "2026-06-12T00:00:00+00:00",
  "lookback_days": 20,
  "closed_trade_count": 1,
  "summary": {},
  "prompt_snippet": {"patterns_to_downsize_or_reject": ["stale lesson"]}
}
""".strip(),
        encoding="utf-8",
    )

    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        now=datetime(2026, 6, 12, 2, 0, tzinfo=UTC),
    ) is None

    (tmp_path / "latest.json").write_text(
        '{"version":"trade-outcome-feedback-v2","mode":"paper",'
        '"prompt_snippet":{"patterns_to_downsize_or_reject":[]}}',
        encoding="utf-8",
    )
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        now=datetime(2026, 6, 12, 0, 1, tzinfo=UTC),
    ) is None

    (tmp_path / "latest.json").write_text(
        '{"version":"trade-outcome-feedback-v999","mode":"paper",'
        '"generated_at":"2026-06-12T00:00:00+00:00",'
        '"prompt_snippet":{"patterns_to_downsize_or_reject":[]}}',
        encoding="utf-8",
    )
    assert load_latest_trade_outcome_feedback_snippet(
        settings,
        now=datetime(2026, 6, 12, 0, 1, tzinfo=UTC),
    ) is None


def test_ensure_fresh_trade_outcome_feedback_generates_missing_artifact_once(tmp_path) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_max_age_seconds=3600,
    )
    as_of = datetime(2026, 6, 12, 0, 1, tzinfo=UTC)

    generated = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=as_of,
    )
    latest_path = tmp_path / "latest.json"
    first_contents = latest_path.read_text(encoding="utf-8")
    reused = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=as_of,
    )

    assert generated["status"] == "generated"
    assert generated["version"] == REPORT_VERSION
    assert reused["status"] == "fresh"
    assert latest_path.read_text(encoding="utf-8") == first_contents

    session.close()
    engine.dispose()


def test_ensure_fresh_trade_outcome_feedback_replaces_non_object_artifact(
    tmp_path,
) -> None:
    engine, session = _session()
    settings = Settings(
        runtime_mode="paper",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_max_age_seconds=3600,
    )
    (tmp_path / "latest.json").write_text("[]", encoding="utf-8")

    result = ensure_fresh_trade_outcome_feedback(
        settings=settings,
        db=session,
        now=datetime(2026, 6, 12, 0, 1, tzinfo=UTC),
    )

    assert result["status"] == "generated"
    refreshed = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    assert isinstance(refreshed, dict)
    assert refreshed["version"] == REPORT_VERSION

    session.close()
    engine.dispose()


def test_feedback_atomic_write_preserves_last_good_artifact_when_replace_fails(
    tmp_path,
    monkeypatch,
) -> None:
    target = tmp_path / "latest.json"
    target.write_text("last-good", encoding="utf-8")

    def fail_replace(*_):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(feedback_service.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        feedback_service._atomic_write_text(target, "new-report")

    assert target.read_text(encoding="utf-8") == "last-good"
    assert list(tmp_path.iterdir()) == [target]


def test_feedback_artifacts_recursively_redact_raw_account_identifiers(tmp_path) -> None:
    report_date = datetime(2026, 6, 12, tzinfo=UTC).date()
    feedback_service._write_artifacts(
        report={
            "version": REPORT_VERSION,
            "generated_at": datetime(2026, 6, 12, tzinfo=UTC),
            "mode": "paper",
            "closed_trade_count": 0,
            "summary": {},
            "prompt_snippet": {},
            "broker_response": {
                "CANO": "12345678",
                "nested": [
                    {
                        "AcCoUnT_Id": "raw-broker-account",
                        "ACNT_PRDT_CD": "01",
                        "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
                        "operational_marker": "preserved",
                    }
                ],
            },
        },
        report_date=report_date,
        output_dir=tmp_path,
    )

    persisted = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    nested = persisted["broker_response"]["nested"][0]
    assert "CANO" not in persisted["broker_response"]
    assert "AcCoUnT_Id" not in nested
    assert "ACNT_PRDT_CD" not in nested
    assert nested == {
        "account_scope": FEEDBACK_TEST_ACCOUNT_SCOPE,
        "operational_marker": "preserved",
    }
