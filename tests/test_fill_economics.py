from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from app.broker.base import BrokerAccountSnapshot
from app.core.config import Settings
from app.db.session import create_db_engine, create_session_factory
from app.db.base import Base
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPosition
from app.services.account_drawdown_gate import evaluate_us_equity_account_gate
from app.services.fill_economics import (
    closed_trade_economics,
    economics_payload,
    execution_fill_economics,
    normalize_fill_economics,
)
from app.services.reconciliation import _parse_fills_from_payload, _upsert_order_fills
from app.services.runtime import (
    _default_daily_pnl_payload,
    _default_monthly_pnl_payload,
    _default_weekly_pnl_payload,
    _pnl_economics_payload,
    monthly_pnl_payload,
)
from app.services.trade_outcome_feedback import (
    _candidate_rule_replay,
    _fifo_closed_trades,
    _lessons,
    _summary,
)
from app.services.weekly_trade_review import _weekly_realized_pnl_summary


def _fill(
    *,
    order: BrokerOrder,
    fill_id: str,
    side: str,
    quantity: int,
    price: str,
    currency: str | None,
    cost: str | None,
    coverage: str | None,
    filled_at: datetime | None = None,
) -> ExecutionFill:
    return ExecutionFill(
        id=fill_id,
        broker_order=order,
        broker_fill_id=fill_id,
        symbol=order.symbol,
        side=side,
        quantity=quantity,
        price=Decimal(price),
        settlement_currency=currency,
        transaction_cost=Decimal(cost) if cost is not None else None,
        transaction_cost_coverage=coverage,
        transaction_cost_source="test_fixture" if cost is not None else None,
        filled_at=filled_at or datetime(2026, 9, 1, tzinfo=UTC),
    )


def _order(
    *,
    order_id: str,
    symbol: str,
    side: str,
    provider: str = "kis",
    asset_class: str = "stock",
) -> BrokerOrder:
    return BrokerOrder(
        id=order_id,
        broker_name=provider,
        mode="paper",
        idempotency_key=order_id,
        symbol=symbol,
        side=side,
        status="filled",
        quantity=10,
        asset_class=asset_class,
    )


def _session(settings: Settings) -> tuple[Session, object]:
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return create_session_factory(engine)(), engine


def test_complete_partial_allocations_turn_gross_winner_into_net_loser() -> None:
    buy = _fill(
        order=_order(order_id="buy", symbol="005930", side="buy"),
        fill_id="buy-fill",
        side="buy",
        quantity=10,
        price="100",
        currency="KRW",
        cost="8",
        coverage="complete",
    )
    sell = _fill(
        order=_order(order_id="sell", symbol="005930", side="sell"),
        fill_id="sell-fill",
        side="sell",
        quantity=10,
        price="101",
        currency="KRW",
        cost="4",
        coverage="complete",
    )

    first = closed_trade_economics(
        buy_fill=buy,
        sell_fill=sell,
        matched_quantity=Decimal("4"),
        gross_realized_pnl=Decimal("4"),
    )
    second = closed_trade_economics(
        buy_fill=buy,
        sell_fill=sell,
        matched_quantity=Decimal("6"),
        gross_realized_pnl=Decimal("6"),
    )

    assert first.known_transaction_cost + second.known_transaction_cost == Decimal("12.0000")
    assert first.net_realized_pnl + second.net_realized_pnl == Decimal("-2.0000")
    assert first.transaction_cost_coverage == second.transaction_cost_coverage == "complete"

    trade = _fifo_closed_trades([buy, sell])[0]
    assert trade["gross_realized_pnl_krw"] == "10.0000"
    assert trade["net_realized_pnl_krw"] == "-2.0000"
    assert trade["outcome_bucket"] == "loss"
    assert _lessons([trade], max_lessons=5)["positive_expectancy_patterns"] == []
    replay = _candidate_rule_replay(settings=Settings(), closed_trades=[trade])
    assert replay["baseline_gross_pnl_krw"] == "10.0000"
    assert replay["baseline_net_pnl_krw"] == "-2.0000"


def test_unknown_or_mismatched_currency_never_produces_net() -> None:
    unknown_buy = _fill(
        order=_order(
            order_id="unknown-buy",
            symbol="XYZ",
            side="buy",
            provider="custom",
            asset_class="custom",
        ),
        fill_id="unknown-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency=None,
        cost="1",
        coverage="complete",
    )
    krw_sell = _fill(
        order=_order(order_id="krw-sell", symbol="XYZ", side="sell"),
        fill_id="krw-sell-fill",
        side="sell",
        quantity=1,
        price="110",
        currency="KRW",
        cost="2",
        coverage="complete",
    )
    result = closed_trade_economics(
        buy_fill=unknown_buy,
        sell_fill=krw_sell,
        matched_quantity=Decimal("1"),
        gross_realized_pnl=Decimal("10"),
    )
    assert result.settlement_currency is None
    assert result.buy_transaction_cost == Decimal("1.0000")
    assert result.sell_transaction_cost == Decimal("2.0000")
    assert result.known_transaction_cost is None
    assert result.net_realized_pnl is None

    unknown_buy.settlement_currency = "USD"
    mismatch = closed_trade_economics(
        buy_fill=unknown_buy,
        sell_fill=krw_sell,
        matched_quantity=Decimal("1"),
        gross_realized_pnl=Decimal("10"),
    )
    assert mismatch.settlement_currency is None
    assert mismatch.known_transaction_cost is None
    assert mismatch.net_realized_pnl is None


def test_complete_cost_coverage_without_cost_value_is_downgraded() -> None:
    economics = normalize_fill_economics(
        {
            "settlement_currency": "KRW",
            "transaction_cost_coverage": "complete",
            "transaction_cost_source": "invalid-test-payload",
        },
        order=_order(order_id="invalid-coverage", symbol="005930", side="buy"),
    )
    assert economics.transaction_cost is None
    assert economics.transaction_cost_coverage == "unknown"

    fill_without_provenance = _fill(
        order=_order(order_id="missing-provenance", symbol="005930", side="buy"),
        fill_id="missing-provenance-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="0",
        coverage="complete",
    )
    fill_without_provenance.transaction_cost_source = None
    assert (
        execution_fill_economics(fill_without_provenance).transaction_cost_coverage
        == "unknown"
    )


def test_order_currency_conflict_is_persistently_degraded() -> None:
    order = _order(
        order_id="currency-conflict",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
    )
    economics = normalize_fill_economics(
        {
            "settlement_currency": "KRW",
            "total_transaction_cost": "0",
        },
        order=order,
    )

    assert economics.settlement_currency is None
    assert economics.settlement_currency_conflict is True

    fill = _fill(
        order=order,
        fill_id="currency-conflict-fill",
        side="buy",
        quantity=1,
        price="100",
        currency=None,
        cost="0",
        coverage="complete",
    )
    fill.payload = economics_payload(economics)
    persisted = execution_fill_economics(fill)
    assert persisted.settlement_currency is None
    assert persisted.settlement_currency_conflict is True

    fill.payload = None
    fill.settlement_currency = "KRW"
    persisted_conflicting_column = execution_fill_economics(fill)
    assert persisted_conflicting_column.settlement_currency is None
    assert persisted_conflicting_column.settlement_currency_conflict is True


def test_transaction_cost_allocation_conserves_eight_decimal_precision() -> None:
    buy = _fill(
        order=_order(order_id="precision-buy", symbol="KRW-BTC", side="buy", provider="upbit"),
        fill_id="precision-buy-fill",
        side="buy",
        quantity=2,
        price="100",
        currency="KRW",
        cost="0.0003",
        coverage="complete",
    )
    sell = _fill(
        order=_order(order_id="precision-sell", symbol="KRW-BTC", side="sell", provider="upbit"),
        fill_id="precision-sell-fill",
        side="sell",
        quantity=2,
        price="101",
        currency="KRW",
        cost="0",
        coverage="complete",
    )

    first = closed_trade_economics(
        buy_fill=buy,
        sell_fill=sell,
        matched_quantity=Decimal("1"),
        gross_realized_pnl=Decimal("1"),
    )
    second = closed_trade_economics(
        buy_fill=buy,
        sell_fill=sell,
        matched_quantity=Decimal("1"),
        gross_realized_pnl=Decimal("1"),
    )

    assert first.buy_transaction_cost == Decimal("0.00015000")
    assert first.buy_transaction_cost + second.buy_transaction_cost == Decimal("0.00030000")


def test_cumulative_reconciliation_cost_is_delta_not_repeated() -> None:
    order = _order(order_id="cumulative", symbol="005930", side="buy")
    order.broker_order_id = "broker-cumulative"
    first_rows, first_anomalies = _parse_fills_from_payload(
        payload={
            "filled_quantity": 1,
            "average_fill_price": "100",
            "filled_at": "2026-09-01T00:00:00+00:00",
            "settlement_currency": "KRW",
            "total_transaction_cost": "1",
            "transaction_cost_scope": "cumulative",
        },
        order=order,
        observed_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert first_anomalies == []
    assert first_rows[0]["transaction_cost"] == Decimal("1")
    _fill(
        order=order,
        fill_id="cumulative-first",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="1",
        coverage="complete",
    )

    second_rows, second_anomalies = _parse_fills_from_payload(
        payload={
            "filled_quantity": 2,
            "average_fill_price": "105",
            "filled_at": "2026-09-01T00:01:00+00:00",
            "settlement_currency": "KRW",
            "total_transaction_cost": "3",
            "transaction_cost_scope": "cumulative",
        },
        order=order,
        observed_at=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
    )
    assert second_anomalies == []
    assert second_rows[0]["quantity"] == 1
    assert second_rows[0]["transaction_cost"] == Decimal("2")


def test_same_quantity_cumulative_snapshot_revises_existing_cost_once() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    observed_at = datetime(2026, 9, 1, tzinfo=UTC)
    try:
        order = _order(order_id="cost-revision", symbol="005930", side="buy")
        order.broker_order_id = "broker-cost-revision"
        db.add(order)
        db.flush()
        first_rows, first_anomalies = _parse_fills_from_payload(
            payload={
                "broker_fill_id": "cost-revision-fill",
                "filled_quantity": 1,
                "average_fill_price": "100",
                "filled_at": observed_at.isoformat(),
                "total_transaction_cost": "1",
                "transaction_cost_scope": "cumulative",
            },
            order=order,
            observed_at=observed_at,
        )
        assert first_anomalies == []
        assert _upsert_order_fills(db=db, order=order, fills=first_rows) == 1
        db.commit()
        db.refresh(order)

        second_rows, second_anomalies = _parse_fills_from_payload(
            payload={
                "broker_fill_id": "cost-revision-fill",
                "filled_quantity": 1,
                "average_fill_price": "100",
                "filled_at": observed_at.isoformat(),
                "total_transaction_cost": "2",
                "transaction_cost_scope": "cumulative",
            },
            order=order,
            observed_at=observed_at + timedelta(minutes=1),
        )
        assert second_anomalies == []
        assert len(second_rows) == 1
        assert _upsert_order_fills(db=db, order=order, fills=second_rows) == 0
        db.commit()
        db.refresh(order)

        assert len(order.fills) == 1
        assert order.fills[0].transaction_cost == Decimal("2.00000000")
        assert order.fills[0].transaction_cost_coverage == "complete"
        assert order.fills[0].transaction_cost_source == (
            "broker_payload:cumulative_cost_revision"
        )
    finally:
        db.close()
        engine.dispose()


def test_final_authoritative_cumulative_cost_can_correct_provisional_cost_downward() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    observed_at = datetime(2026, 9, 1, tzinfo=UTC)
    try:
        order = _order(order_id="cost-final-correction", symbol="005930", side="buy")
        order.broker_order_id = "broker-cost-final-correction"
        db.add(order)
        db.flush()
        db.add(
            _fill(
                order=order,
                fill_id="cost-final-correction-fill",
                side="buy",
                quantity=1,
                price="100",
                currency="KRW",
                cost="2",
                coverage="complete",
            )
        )
        db.commit()
        db.refresh(order)

        provisional_rows, provisional_anomalies = _parse_fills_from_payload(
            payload={
                "status": "filled",
                "filled_quantity": 1,
                "average_fill_price": "100",
                "total_transaction_cost": "1.5",
                "transaction_cost_scope": "cumulative",
            },
            order=order,
            observed_at=observed_at,
        )
        assert provisional_anomalies == []
        assert provisional_rows == []

        final_rows, final_anomalies = _parse_fills_from_payload(
            payload={
                "status": "filled",
                "filled_quantity": 1,
                "average_fill_price": "100",
                "total_transaction_cost": "1.5",
                "transaction_cost_scope": "cumulative",
                "transaction_cost_authoritative": True,
            },
            order=order,
            observed_at=observed_at + timedelta(minutes=1),
        )
        assert final_anomalies == []
        assert len(final_rows) == 1
        assert _upsert_order_fills(db=db, order=order, fills=final_rows) == 0
        db.commit()
        db.refresh(order)

        assert order.fills[0].transaction_cost == Decimal("1.50000000")
        assert order.fills[0].transaction_cost_source == (
            "broker_payload:authoritative_cumulative_cost_revision"
        )
    finally:
        db.close()
        engine.dispose()


def test_explicit_fill_cumulative_cost_scope_is_not_repeated_per_fill() -> None:
    order = _order(order_id="explicit-cumulative", symbol="005930", side="buy")
    rows, anomalies = _parse_fills_from_payload(
        payload={
            "fills": [
                {
                    "broker_fill_id": "explicit-1",
                    "quantity": 1,
                    "price": "100",
                    "filled_at": "2026-09-01T00:00:00+00:00",
                    "total_transaction_cost": "1",
                    "transaction_cost_scope": "cumulative",
                },
                {
                    "broker_fill_id": "explicit-2",
                    "quantity": 1,
                    "price": "101",
                    "filled_at": "2026-09-01T00:01:00+00:00",
                    "total_transaction_cost": "2",
                    "transaction_cost_scope": "cumulative",
                },
            ]
        },
        order=order,
        observed_at=datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert anomalies == []
    assert [row["transaction_cost"] for row in rows] == [None, None]
    assert [row["transaction_cost_coverage"] for row in rows] == [
        "unknown",
        "unknown",
    ]


def test_mixed_currency_runtime_keeps_separate_pnl_and_notional_subtotals() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monthly_target_krw=Decimal("1000"),
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, tzinfo=UTC)
    try:
        krw_buy_order = _order(order_id="krw-buy", symbol="005930", side="buy")
        krw_sell_order = _order(order_id="krw-sell", symbol="005930", side="sell")
        usd_buy_order = _order(
            order_id="usd-buy",
            symbol="AAPL",
            side="buy",
            asset_class="us_equity",
        )
        usd_sell_order = _order(
            order_id="usd-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
        )
        usd_unmatched_sell_order = _order(
            order_id="usd-unmatched-sell",
            symbol="MSFT",
            side="sell",
            asset_class="us_equity",
        )
        db.add_all(
            [
                krw_buy_order,
                krw_sell_order,
                usd_buy_order,
                usd_sell_order,
                usd_unmatched_sell_order,
                _fill(
                    order=krw_buy_order,
                    fill_id="krw-buy-fill",
                    side="buy",
                    quantity=1,
                    price="1000",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=3),
                ),
                _fill(
                    order=krw_sell_order,
                    fill_id="krw-sell-fill",
                    side="sell",
                    quantity=1,
                    price="1100",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=2),
                ),
                _fill(
                    order=usd_buy_order,
                    fill_id="usd-buy-fill",
                    side="buy",
                    quantity=1,
                    price="200",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=3),
                ),
                _fill(
                    order=usd_sell_order,
                    fill_id="usd-sell-fill",
                    side="sell",
                    quantity=1,
                    price="210",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=2),
                ),
                _fill(
                    order=usd_unmatched_sell_order,
                    fill_id="usd-unmatched-sell-fill",
                    side="sell",
                    quantity=1,
                    price="50",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        db.commit()

        payload = monthly_pnl_payload(settings=settings, db=db, now=now)
        assert payload["monthly_realized_pnl"] == "100.0000"
        assert payload["monthly_net_realized_pnl_krw"] == "100.0000"
        assert payload["target_progress_pct"] == "10.0000"
        assert payload["realized_pnl_by_currency"]["KRW"]["gross_realized_pnl"] == "100.0000"
        assert payload["realized_pnl_by_currency"]["USD"]["gross_realized_pnl"] is None
        assert payload["realized_pnl_by_currency"]["USD"]["covered_gross_realized_pnl"] == (
            "10.0000"
        )
        assert payload["unmatched_sell_quantity_by_currency"] == {"USD": 1}
        assert payload["monthly_buy_notional"] == "1000.0000"
        assert payload["buy_notional_by_currency"] == {
            "KRW": "1000.0000",
            "USD": "200.0000",
        }
        assert payload["profitability_status"] == "degraded_mixed_currency"
        assert payload["notional_status"] == "mixed_currency_subtotals"
    finally:
        db.close()
        engine.dispose()


def test_unmatched_krw_sell_does_not_publish_verified_zero_or_target_progress() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monthly_target_krw=Decimal("1000"),
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, tzinfo=UTC)
    try:
        sell_order = _order(
            order_id="unmatched-krw-sell",
            symbol="005930",
            side="sell",
        )
        db.add_all(
            [
                sell_order,
                _fill(
                    order=sell_order,
                    fill_id="unmatched-krw-sell-fill",
                    side="sell",
                    quantity=1,
                    price="1000",
                    currency="KRW",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        db.commit()

        payload = monthly_pnl_payload(settings=settings, db=db, now=now)
        assert payload["monthly_realized_pnl"] is None
        assert payload["monthly_gross_realized_pnl_krw"] is None
        assert payload["monthly_net_realized_pnl_krw"] is None
        assert payload["target_progress_pct"] is None
        assert payload["mode_recommendation"] == "insufficient_net_pnl_data"
        assert payload["profitability_status"] == "degraded_unknown_cost_basis"
        assert payload["pnl_cost_basis_coverage"] == "incomplete"
        assert payload["unmatched_sell_quantity_by_currency"] == {"KRW": 1}
    finally:
        db.close()
        engine.dispose()


def test_unknown_currency_sell_invalidates_aggregate_krw_net() -> None:
    buy = _fill(
        order=_order(order_id="known-krw-buy", symbol="005930", side="buy"),
        fill_id="known-krw-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="0",
        coverage="complete",
    )
    sell = _fill(
        order=_order(order_id="known-krw-sell", symbol="005930", side="sell"),
        fill_id="known-krw-sell-fill",
        side="sell",
        quantity=1,
        price="110",
        currency="KRW",
        cost="0",
        coverage="complete",
    )
    closed = closed_trade_economics(
        buy_fill=buy,
        sell_fill=sell,
        matched_quantity=Decimal("1"),
        gross_realized_pnl=Decimal("10"),
    )

    payload = _pnl_economics_payload(
        [closed],
        unknown_currency_unmatched_sell_quantity=Decimal("1"),
    )
    assert payload["gross_realized_pnl_krw"] is None
    assert payload["covered_gross_realized_pnl_krw"] == Decimal("10.0000")
    assert payload["net_realized_pnl_krw"] is None
    assert payload["profitability_status"] == "degraded_unknown_currency"


def test_skipped_pnl_sections_are_not_reported_as_verified_zero() -> None:
    settings = Settings(monthly_target_krw=Decimal("1000"))
    monthly = _default_monthly_pnl_payload(settings=settings)
    weekly = _default_weekly_pnl_payload()
    daily = _default_daily_pnl_payload()

    assert monthly["monthly_realized_pnl"] is None
    assert monthly["monthly_net_realized_pnl_krw"] is None
    assert monthly["target_progress_pct"] is None
    assert monthly["profitability_status"] == "degraded_not_computed"
    assert monthly["calculation_status"] == "not_computed"
    assert weekly["weekly_realized_pnl"] is None
    assert weekly["calculation_status"] == "not_computed"
    assert daily["daily_realized_pnl"] is None
    assert daily["calculation_status"] == "not_computed"


def test_broker_reconciled_price_basis_does_not_reuse_fifo_buy_fee() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, tzinfo=UTC)
    try:
        buy_order = _order(order_id="old-basis-buy", symbol="005930", side="buy")
        sell_order = _order(order_id="reconciled-basis-sell", symbol="005930", side="sell")
        db.add_all(
            [
                buy_order,
                sell_order,
                _fill(
                    order=buy_order,
                    fill_id="old-basis-buy-fill",
                    side="buy",
                    quantity=1,
                    price="90",
                    currency="KRW",
                    cost="1",
                    coverage="complete",
                    filled_at=datetime(2026, 8, 1, tzinfo=UTC),
                ),
                _fill(
                    order=sell_order,
                    fill_id="reconciled-basis-sell-fill",
                    side="sell",
                    quantity=1,
                    price="110",
                    currency="KRW",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(days=1),
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=0,
                    average_price=Decimal("100"),
                    updated_from="broker_reconciliation",
                    last_reconciled_at=now,
                ),
            ]
        )
        db.commit()

        payload = monthly_pnl_payload(settings=settings, db=db, now=now)
        assert payload["monthly_gross_realized_pnl_krw"] == "10.0000"
        assert payload["monthly_net_realized_pnl_krw"] is None
        assert payload["krw_transaction_cost_coverage"] == "partial"
        assert payload["cost_basis_sources"]["broker_reconciliation"] == 1
    finally:
        db.close()
        engine.dispose()


def test_weekly_review_does_not_rank_absolute_pnl_across_currencies() -> None:
    rows = [
        {
            "sell_fill_id": "krw-sell",
            "sell_broker_order_pk": "krw-order",
            "symbol": "005930",
            "sell_filled_at": datetime(2026, 9, 1, tzinfo=UTC),
            "quantity": 1,
            "cost_basis_price": Decimal("1000"),
            "realized_pnl": Decimal("100"),
            "settlement_currency": "KRW",
            "known_transaction_cost": Decimal("0"),
            "net_realized_pnl": Decimal("100"),
            "transaction_cost_coverage": "complete",
        },
        {
            "sell_fill_id": "usd-sell",
            "sell_broker_order_pk": "usd-order",
            "symbol": "AAPL",
            "sell_filled_at": datetime(2026, 9, 1, tzinfo=UTC),
            "quantity": 1,
            "cost_basis_price": Decimal("100"),
            "realized_pnl": Decimal("10"),
            "settlement_currency": "USD",
            "known_transaction_cost": Decimal("0"),
            "net_realized_pnl": Decimal("10"),
            "transaction_cost_coverage": "complete",
        },
    ]

    summary = _weekly_realized_pnl_summary(rows, truncated=False)
    assert summary["best_trade"] is None
    assert summary["worst_trade"] is None
    assert set(summary["best_trade_by_currency"]) == {"KRW", "USD"}
    assert summary["gross_realized_pnl_by_currency"] == {
        "KRW": Decimal("100.0000"),
        "USD": Decimal("10.0000"),
    }


def test_feedback_does_not_label_usd_as_krw_or_learn_from_unknown_costs() -> None:
    buy_order = _order(
        order_id="usd-feedback-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
    )
    sell_order = _order(
        order_id="usd-feedback-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
    )
    buy = _fill(
        order=buy_order,
        fill_id="usd-feedback-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="USD",
        cost=None,
        coverage="unknown",
    )
    sell = _fill(
        order=sell_order,
        fill_id="usd-feedback-sell-fill",
        side="sell",
        quantity=1,
        price="101",
        currency="USD",
        cost=None,
        coverage="unknown",
        filled_at=datetime(2026, 9, 2, tzinfo=UTC),
    )

    trades = _fifo_closed_trades([buy, sell])
    assert trades[0]["settlement_currency"] == "USD"
    assert trades[0]["gross_realized_pnl"] == "1.0000"
    assert trades[0]["realized_pnl_krw"] is None
    assert trades[0]["profitability_eligible"] is False
    assert trades[0]["outcome_bucket"] == "unavailable"
    assert _summary(trades)["realized_pnl_krw"] is None
    lessons = _lessons(trades, max_lessons=5)
    assert lessons["positive_expectancy_patterns"] == []
    assert lessons["eligible_trade_count"] == 0
    assert lessons["excluded_degraded_trade_count"] == 1


def test_feedback_keeps_complete_krw_subtotal_when_usd_trade_is_present() -> None:
    krw_buy = _fill(
        order=_order(order_id="mixed-feedback-krw-buy", symbol="005930", side="buy"),
        fill_id="mixed-feedback-krw-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="0",
        coverage="complete",
    )
    krw_sell = _fill(
        order=_order(order_id="mixed-feedback-krw-sell", symbol="005930", side="sell"),
        fill_id="mixed-feedback-krw-sell-fill",
        side="sell",
        quantity=1,
        price="110",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=datetime(2026, 9, 2, tzinfo=UTC),
    )
    usd_buy = _fill(
        order=_order(
            order_id="mixed-feedback-usd-buy",
            symbol="AAPL",
            side="buy",
            asset_class="us_equity",
        ),
        fill_id="mixed-feedback-usd-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
    )
    usd_sell = _fill(
        order=_order(
            order_id="mixed-feedback-usd-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
        ),
        fill_id="mixed-feedback-usd-sell-fill",
        side="sell",
        quantity=1,
        price="105",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=datetime(2026, 9, 2, tzinfo=UTC),
    )

    summary = _summary(_fifo_closed_trades([krw_buy, usd_buy, krw_sell, usd_sell]))
    assert summary["realized_pnl_krw"] == "10.0000"
    assert summary["krw_profitability_coverage"] == "complete"
    assert summary["net_realized_pnl_by_currency"] == {
        "KRW": "10.0000",
        "USD": "5.0000",
    }
    assert summary["profitability_status"] == "degraded_mixed_currency"


def test_krw_gross_winner_with_unknown_cost_cannot_promote_strategy() -> None:
    buy = _fill(
        order=_order(order_id="unknown-cost-buy", symbol="005930", side="buy"),
        fill_id="unknown-cost-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost=None,
        coverage="unknown",
    )
    sell = _fill(
        order=_order(order_id="unknown-cost-sell", symbol="005930", side="sell"),
        fill_id="unknown-cost-sell-fill",
        side="sell",
        quantity=1,
        price="101",
        currency="KRW",
        cost=None,
        coverage="unknown",
        filled_at=datetime(2026, 9, 2, tzinfo=UTC),
    )

    trade = _fifo_closed_trades([buy, sell])[0]
    assert trade["gross_realized_pnl_krw"] == "1.0000"
    assert trade["net_realized_pnl_krw"] is None
    assert trade["profitability_eligible"] is False
    assert _lessons([trade], max_lessons=5)["positive_expectancy_patterns"] == []

    replay = _candidate_rule_replay(settings=Settings(), closed_trades=[trade])
    assert replay["baseline_closed_trade_count"] == 0
    assert replay["excluded_degraded_trade_count"] == 1
    assert all(
        rule["recommendation"] != "promote_to_issue"
        for rule in replay["rules"]
    )


def test_us_drawdown_gate_uses_complete_net_pnl_not_gross() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_daily_realized_loss_limit_usd=Decimal("1"),
        kis_us_allow_degraded_realized_pnl_coverage=False,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    try:
        buy_order = _order(
            order_id="gate-buy",
            symbol="AAPL",
            side="buy",
            asset_class="us_equity",
        )
        sell_order = _order(
            order_id="gate-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
        )
        db.add_all(
            [
                buy_order,
                sell_order,
                _fill(
                    order=buy_order,
                    fill_id="gate-buy-fill",
                    side="buy",
                    quantity=10,
                    price="100",
                    currency="USD",
                    cost="8",
                    coverage="complete",
                    filled_at=now - timedelta(hours=2),
                ),
                _fill(
                    order=sell_order,
                    fill_id="gate-sell-fill",
                    side="sell",
                    quantity=10,
                    price="101",
                    currency="USD",
                    cost="4",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        for fill in (buy_order.fills[0], sell_order.fills[0]):
            fill.base_quantity = Decimal("0")
        db.commit()
        decision = evaluate_us_equity_account_gate(
            settings=settings,
            db=db,
            snapshot=BrokerAccountSnapshot(
                observed_at=now,
                source="test",
                payload={"realized_pnl_coverage": "broker_confirmed"},
            ),
            order_notional=Decimal("0"),
            order_idempotency_key="gate-next-buy",
            execution_plan_item_id=None,
            now=now,
        )
        assert decision.allowed is False
        assert decision.gate_name == "us_daily_realized_loss_usd"
        assert decision.payload["metrics"]["daily_gross_realized_pnl_usd"] == "10.0000"
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] == "-2.0000"
        assert decision.payload["metrics"]["daily_transaction_cost_coverage"] == "complete"
        assert decision.payload["metrics"]["daily_realized_loss_gate_basis"] == (
            "net_after_transaction_costs"
        )
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_when_sell_cost_basis_is_unmatched() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=False,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    try:
        sell_order = _order(
            order_id="unmatched-gate-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
        )
        db.add_all(
            [
                sell_order,
                _fill(
                    order=sell_order,
                    fill_id="unmatched-gate-sell-fill",
                    side="sell",
                    quantity=1,
                    price="101",
                    currency="USD",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        db.commit()

        decision = evaluate_us_equity_account_gate(
            settings=settings,
            db=db,
            snapshot=BrokerAccountSnapshot(
                observed_at=now,
                source="test",
                payload={"realized_pnl_coverage": "broker_confirmed"},
            ),
            order_notional=Decimal("0"),
            order_idempotency_key="unmatched-gate-next-buy",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_transaction_cost_coverage_unconfirmed"
        metrics = decision.payload["metrics"]
        assert metrics["daily_realized_pnl_usd"] is None
        assert metrics["daily_gross_realized_pnl_usd"] is None
        assert metrics["daily_unmatched_sell_quantity"] == "1.0000"
        assert metrics["daily_realized_pnl_coverage"] == "incomplete"
        assert metrics["daily_transaction_cost_coverage"] == "unknown"
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_on_covered_loss_floor_with_unmatched_sell() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_daily_realized_loss_limit_usd=Decimal("50"),
        kis_us_weekly_realized_loss_limit_usd=Decimal("50"),
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    try:
        buy_order = _order(
            order_id="covered-loss-buy",
            symbol="AAPL",
            side="buy",
            asset_class="us_equity",
        )
        matched_sell_order = _order(
            order_id="covered-loss-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
        )
        unmatched_sell_order = _order(
            order_id="unrelated-unmatched-sell",
            symbol="MSFT",
            side="sell",
            asset_class="us_equity",
        )
        db.add_all(
            [
                buy_order,
                matched_sell_order,
                unmatched_sell_order,
                _fill(
                    order=buy_order,
                    fill_id="covered-loss-buy-fill",
                    side="buy",
                    quantity=1,
                    price="200",
                    currency="USD",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(hours=3),
                ),
                _fill(
                    order=matched_sell_order,
                    fill_id="covered-loss-sell-fill",
                    side="sell",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(hours=2),
                ),
                _fill(
                    order=unmatched_sell_order,
                    fill_id="unrelated-unmatched-sell-fill",
                    side="sell",
                    quantity=1,
                    price="300",
                    currency="USD",
                    cost="1",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        db.commit()

        decision = evaluate_us_equity_account_gate(
            settings=settings,
            db=db,
            snapshot=BrokerAccountSnapshot(
                observed_at=now,
                source="test",
                payload={"realized_pnl_coverage": "degraded_local_execution_fills_only"},
            ),
            order_notional=Decimal("0"),
            order_idempotency_key="covered-loss-next-buy",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_daily_realized_loss_usd"
        assert decision.current_metric == Decimal("102.00000000")
        metrics = decision.payload["metrics"]
        assert metrics["daily_realized_pnl_usd"] is None
        assert metrics["daily_gross_realized_pnl_usd"] is None
        assert metrics["daily_covered_gross_realized_pnl_usd"] == "-100.0000"
        assert metrics["daily_known_transaction_cost_usd"] == "2.0000"
        assert metrics["daily_covered_realized_pnl_loss_floor_usd"] == (
            "-102.0000"
        )
        assert metrics["daily_realized_loss_gate_basis"] == (
            "covered_loss_floor_gross_minus_known_transaction_cost"
        )
        assert metrics["daily_unmatched_sell_quantity"] == "1.0000"
    finally:
        db.close()
        engine.dispose()
