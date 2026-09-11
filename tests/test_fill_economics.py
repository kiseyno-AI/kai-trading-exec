from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.broker.base import BrokerAccountSnapshot
from app.core.config import Settings
from app.db.session import create_db_engine, create_session_factory
from app.db.base import Base
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPosition,
    FifoInventoryIsolation,
)
from app.services.account_drawdown_recovery import recovery_account_fingerprint
from app.services.account_drawdown_gate import evaluate_us_equity_account_gate
from app.services.fill_economics import (
    closed_trade_economics,
    economics_payload,
    execution_fill_economics,
    normalize_fill_economics,
)
from app.services.reconciliation import _parse_fills_from_payload, _upsert_order_fills
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
)
from app.services.runtime import (
    _default_daily_pnl_payload,
    _default_monthly_pnl_payload,
    _default_weekly_pnl_payload,
    _pnl_economics_payload,
    daily_pnl_payload,
    monthly_pnl_payload,
    weekly_pnl_payload,
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
    payload: dict[str, object] | None = None,
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
        payload=payload,
    )


def _order(
    *,
    order_id: str,
    symbol: str,
    side: str,
    provider: str = "kis",
    asset_class: str = "stock",
    account_scope: str | None = None,
    quantity: int = 1,
) -> BrokerOrder:
    venue = "KRX" if provider == "kis" else provider.upper()
    exchange = "NASD" if asset_class == "us_equity" else venue
    return BrokerOrder(
        id=order_id,
        broker_name=provider,
        mode="paper",
        idempotency_key=order_id,
        symbol=symbol,
        side=side,
        status="filled",
        quantity=quantity,
        asset_class=asset_class,
        venue=venue,
        request_metadata={
            "provider": provider,
            "venue": venue,
            "asset_class": asset_class,
            "exchange": exchange,
            "broker_account_scope": {
                "policy_version": 1,
                "scope": account_scope or f"{provider}:test-account",
            },
        },
    )


def _bind_order_to_runtime_scope(
    order: BrokerOrder,
    *,
    settings: Settings,
) -> BrokerOrder:
    metadata = order.request_metadata if isinstance(order.request_metadata, dict) else {}
    exchange = str(metadata.get("exchange") or order.venue or "").strip().upper()
    identity = build_position_identity(
        settings=settings,
        provider=order.broker_name,
        exchange=exchange,
    )
    order.request_metadata = {
        "exchange": exchange,
        "execution_scope": execution_scope_payload(
            identity=identity,
            venue=str(order.venue or exchange),
            asset_class=str(order.asset_class or "stock"),
        ),
    }
    return order


def _session(settings: Settings) -> tuple[Session, object]:
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return create_session_factory(engine)(), engine


def _kis_us_account_scope(settings: Settings) -> str:
    return f"kis:{recovery_account_fingerprint(settings)}:usd"


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
        filled_at=datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
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


def test_fifo_profitability_fails_closed_for_opposing_same_time_fills() -> None:
    observed_at = datetime(2026, 9, 1, tzinfo=UTC)
    buy = _fill(
        order=_order(order_id="same-time-buy", symbol="005930", side="buy"),
        fill_id="z-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=observed_at,
    )
    sell = _fill(
        order=_order(order_id="same-time-sell", symbol="005930", side="sell"),
        fill_id="a-sell-fill",
        side="sell",
        quantity=1,
        price="110",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=observed_at,
    )

    trades = _fifo_closed_trades([buy, sell])

    assert len(trades) == 1
    assert trades[0]["profitability_eligible"] is False
    assert trades[0]["outcome_bucket"] == "unavailable"
    assert "fill_chronology_ambiguous" in trades[0]["outcome_quality_flags"]


def test_daily_pnl_fails_closed_for_opposing_same_time_fills() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    observed_at = datetime(2026, 9, 1, 1, tzinfo=UTC)
    buy_order = _bind_order_to_runtime_scope(
        _order(order_id="same-time-daily-buy", symbol="005930", side="buy"),
        settings=settings,
    )
    sell_order = _bind_order_to_runtime_scope(
        _order(order_id="same-time-daily-sell", symbol="005930", side="sell"),
        settings=settings,
    )
    try:
        db.add_all(
            [
                buy_order,
                sell_order,
                _fill(
                    order=buy_order,
                    fill_id="z-daily-buy-fill",
                    side="buy",
                    quantity=1,
                    price="100",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=observed_at,
                ),
                _fill(
                    order=sell_order,
                    fill_id="a-daily-sell-fill",
                    side="sell",
                    quantity=1,
                    price="110",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=observed_at,
                ),
            ]
        )
        db.commit()

        payload = daily_pnl_payload(settings=settings, db=db, now=observed_at)

        assert payload["daily_realized_pnl"] is None
        assert payload["calculation_status"] == "not_computed"
        assert payload["calculation_reason"] == "indeterminate_fill_chronology"
    finally:
        db.close()
        engine.dispose()


def test_bounded_fill_observation_invalidates_earlier_fifo_sale_for_all_consumers() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    exact_buy_at = datetime(2026, 9, 1, 1, tzinfo=UTC)
    exact_sell_at = exact_buy_at + timedelta(minutes=30)
    bounded_buy_at = exact_buy_at + timedelta(hours=1)
    now = bounded_buy_at + timedelta(hours=1)
    exact_buy_order = _bind_order_to_runtime_scope(
        _order(order_id="pre-bound-exact-buy", symbol="005930", side="buy"),
        settings=settings,
    )
    exact_sell_order = _bind_order_to_runtime_scope(
        _order(order_id="pre-bound-exact-sell", symbol="005930", side="sell"),
        settings=settings,
    )
    bounded_buy_order = _bind_order_to_runtime_scope(
        _order(order_id="later-bounded-buy", symbol="005930", side="buy"),
        settings=settings,
    )
    exact_buy = _fill(
        order=exact_buy_order,
        fill_id="pre-bound-exact-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=exact_buy_at,
        payload={"fill_time_precision": "exact"},
    )
    exact_sell = _fill(
        order=exact_sell_order,
        fill_id="pre-bound-exact-sell-fill",
        side="sell",
        quantity=1,
        price="200",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=exact_sell_at,
        payload={"fill_time_precision": "exact"},
    )
    bounded_buy = _fill(
        order=bounded_buy_order,
        fill_id="later-bounded-buy-fill",
        side="buy",
        quantity=1,
        price="300",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=bounded_buy_at,
        payload={
            "fill_time_precision": "observed_upper_bound",
            "fill_time_source": "reconciliation_observed_at",
        },
    )
    try:
        db.add_all(
            [
                exact_buy_order,
                exact_sell_order,
                bounded_buy_order,
                exact_buy,
                exact_sell,
                bounded_buy,
            ]
        )
        db.commit()

        trades = _fifo_closed_trades([bounded_buy, exact_sell, exact_buy])
        daily = daily_pnl_payload(settings=settings, db=db, now=now)
        weekly = weekly_pnl_payload(settings=settings, db=db, now=now)

        assert len(trades) == 1
        assert trades[0]["profitability_eligible"] is False
        assert trades[0]["net_realized_pnl"] is None
        assert "fill_chronology_observation_bounded_generation" in trades[0][
            "outcome_quality_flags"
        ]
        assert "fill_chronology_ambiguous" not in trades[0][
            "outcome_quality_flags"
        ]
        assert daily["daily_realized_pnl"] is None
        assert daily["calculation_status"] == "not_computed"
        assert daily["calculation_reason"] == "indeterminate_fill_chronology"
        assert weekly["weekly_realized_pnl"] is None
        assert weekly["calculation_status"] == "not_computed"
        assert weekly["calculation_reason"] == "indeterminate_fill_chronology"
    finally:
        db.close()
        engine.dispose()


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


@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"symbol": "MSFT"}, "fill_symbol_mismatch"),
        ({"side": "sell"}, "fill_side_mismatch"),
        ({"price": "0"}, "non_positive_fill_price"),
        ({"price": "NaN"}, "non_finite_fill_price"),
        ({"quantity": "-1"}, "non_positive_fill_quantity"),
        ({"quantity": "Infinity"}, "non_finite_fill_quantity"),
    ],
)
def test_reconciliation_rejects_explicit_fill_integrity_violations(
    overrides: dict[str, object],
    expected_reason: str,
) -> None:
    order = _order(order_id="invalid-explicit-fill", symbol="AAPL", side="buy")
    fill_payload: dict[str, object] = {
        "broker_fill_id": "invalid-explicit-fill",
        "symbol": order.symbol,
        "side": order.side,
        "quantity": 1,
        "price": "100",
        "filled_at": "2026-09-01T00:00:00+00:00",
    }
    fill_payload.update(overrides)

    rows, anomalies = _parse_fills_from_payload(
        payload={"fills": [fill_payload]},
        order=order,
        observed_at=datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert rows == []
    assert len(anomalies) == 1
    assert anomalies[0]["reason"] == expected_reason
    assert anomalies[0]["fill_index"] == 0


def test_reconciliation_canonicalizes_normalized_fill_symbol_and_side() -> None:
    order = _order(order_id="normalized-explicit-fill", symbol="AAPL", side="buy")

    rows, anomalies = _parse_fills_from_payload(
        payload={
            "fills": [
                {
                    "broker_fill_id": "normalized-explicit-fill",
                    "symbol": " aapl ",
                    "side": " BUY ",
                    "quantity": 1,
                    "price": "100",
                    "filled_at": "2026-09-01T00:00:00+00:00",
                }
            ]
        },
        order=order,
        observed_at=datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert anomalies == []
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["side"] == "buy"


@pytest.mark.parametrize(
    "invalid_fill",
    [
        {"symbol": "MSFT", "side": "buy", "quantity": 1, "price": "100"},
        {"symbol": "AAPL", "side": "sell", "quantity": 1, "price": "100"},
        {"symbol": "AAPL", "side": "buy", "quantity": 1, "price": "-1"},
        {
            "symbol": "AAPL",
            "side": "buy",
            "quantity": -1,
            "base_quantity": "1",
            "price": "100",
        },
    ],
)
def test_fill_upsert_rejects_integrity_violations_from_internal_callers(
    invalid_fill: dict[str, object],
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    order = _order(order_id="invalid-upsert-fill", symbol="AAPL", side="buy")
    invalid_fill = {
        "broker_fill_id": "invalid-upsert-fill",
        "filled_at": "2026-09-01T00:00:00+00:00",
        **invalid_fill,
    }
    try:
        db.add(order)
        db.flush()

        assert _upsert_order_fills(db=db, order=order, fills=[invalid_fill]) == 0
        db.flush()
        assert db.query(ExecutionFill).count() == 0
    finally:
        db.close()
        engine.dispose()


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
        for order in (
            krw_buy_order,
            krw_sell_order,
            usd_buy_order,
            usd_sell_order,
            usd_unmatched_sell_order,
        ):
            _bind_order_to_runtime_scope(order, settings=settings)
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
        _bind_order_to_runtime_scope(sell_order, settings=settings)
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


def test_monthly_pnl_excludes_other_provider_or_account_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monthly_target_krw=Decimal("1000"),
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, tzinfo=UTC)
    try:
        buy_order = _order(
            order_id="scoped-buy",
            symbol="SHARED",
            side="buy",
            provider="kis",
            account_scope="account-a",
        )
        other_account_sell = _order(
            order_id="other-account-sell",
            symbol="SHARED",
            side="sell",
            provider="kis",
            account_scope="account-b",
        )
        other_provider_sell = _order(
            order_id="other-provider-sell",
            symbol="SHARED",
            side="sell",
            provider="alpaca",
            account_scope="account-a",
        )
        _bind_order_to_runtime_scope(buy_order, settings=settings)
        foreign_settings = settings.model_copy(
            update={"execution_position_account_scope": "foreign-account"}
        )
        _bind_order_to_runtime_scope(
            other_account_sell,
            settings=foreign_settings,
        )
        _bind_order_to_runtime_scope(other_provider_sell, settings=settings)
        db.add_all(
            [
                buy_order,
                other_account_sell,
                other_provider_sell,
                _fill(
                    order=buy_order,
                    fill_id="scoped-buy-fill",
                    side="buy",
                    quantity=2,
                    price="100",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=3),
                ),
                _fill(
                    order=other_account_sell,
                    fill_id="other-account-sell-fill",
                    side="sell",
                    quantity=1,
                    price="200",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=2),
                ),
                _fill(
                    order=other_provider_sell,
                    fill_id="other-provider-sell-fill",
                    side="sell",
                    quantity=1,
                    price="300",
                    currency="KRW",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
            ]
        )
        db.commit()

        payload = monthly_pnl_payload(settings=settings, db=db, now=now)

        assert payload["matched_sell_quantity"] == 0
        assert payload["unmatched_sell_quantity"] == 0
        assert payload["monthly_gross_realized_pnl_krw"] == "0.0000"
        assert payload["monthly_buy_notional"] == "200.0000"
        assert payload["monthly_sell_notional"] == "0.0000"
        assert payload["target_progress_pct"] == "0.0000"
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
        buy_order = _order(
            order_id="old-basis-buy",
            symbol="005930",
            side="buy",
            account_scope="a" * 24,
        )
        sell_order = _order(
            order_id="reconciled-basis-sell",
            symbol="005930",
            side="sell",
            account_scope="a" * 24,
        )
        _bind_order_to_runtime_scope(buy_order, settings=settings)
        _bind_order_to_runtime_scope(sell_order, settings=settings)
        active_identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
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
                        provider=active_identity.provider,
                        account_scope=active_identity.account_scope,
                        exchange=active_identity.exchange,
                        symbol="005930",
                        quantity=0,
                        average_price=Decimal("100"),
                        updated_from="broker_reconciliation",
                        last_reconciled_at=now,
                        raw_payload=payload_with_position_identity(
                            None,
                            identity=active_identity,
                        ),
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
            account_scope=_kis_us_account_scope(settings),
            quantity=10,
        )
        sell_order = _order(
            order_id="gate-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
            account_scope=_kis_us_account_scope(settings),
            quantity=10,
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


def test_cumulative_reconcile_preserves_observation_bounded_fill_time_precision() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    order = _order(
        order_id="bounded-cumulative-fill",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
    )
    observed_at = datetime(2026, 9, 3, 16, tzinfo=UTC)

    try:
        db.add(order)
        db.flush()
        rows, anomalies = _parse_fills_from_payload(
            payload={
                "filled_quantity": 1,
                "average_fill_price": "99",
                "filled_at": observed_at.isoformat(),
                "fill_time_precision": "observed_upper_bound",
                "fill_time_source": "reconciliation_observed_at",
            },
            order=order,
            observed_at=observed_at,
        )

        assert anomalies == []
        assert rows[0]["filled_at"] == observed_at.isoformat()
        assert rows[0]["fill_time_precision"] == "observed_upper_bound"
        assert rows[0]["fill_time_source"] == "reconciliation_observed_at"
        assert _upsert_order_fills(db=db, order=order, fills=rows) == 1
        db.commit()
        persisted = db.query(ExecutionFill).one()
        assert persisted.filled_at == observed_at.replace(tzinfo=None)
        assert persisted.payload["fill_time_precision"] == "observed_upper_bound"
        assert persisted.payload["fill_time_source"] == "reconciliation_observed_at"
    finally:
        db.close()
        engine.dispose()


def test_cumulative_noop_reconcile_backfills_observation_bounded_chronology() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    observed_at = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id="legacy-unmarked-cumulative-fill",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
    )
    order.broker_order_id = "legacy-unmarked-cumulative-fill"
    legacy_fill = _fill(
        order=order,
        fill_id="legacy-cumulative-fill-row",
        side="sell",
        quantity=1,
        price="99",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=observed_at - timedelta(days=1),
        payload={
            "price_derivation": "cumulative_notional_delta",
            "cumulative_quantity": 1,
        },
    )
    try:
        db.add_all([order, legacy_fill])
        db.commit()
        db.refresh(order)

        rows, anomalies = _parse_fills_from_payload(
            payload={
                "filled_quantity": 1,
                "average_fill_price": "99",
                "filled_at": observed_at.isoformat(),
                "fill_time_precision": "observed_upper_bound",
                "fill_time_source": "reconciliation_observed_at",
            },
            order=order,
            observed_at=observed_at,
        )

        assert anomalies == []
        assert len(rows) == 1
        assert rows[0]["fill_time_precision"] == "observed_upper_bound"
        assert _upsert_order_fills(db=db, order=order, fills=rows) == 0
        db.commit()
        db.refresh(legacy_fill)
        assert legacy_fill.payload["fill_time_precision"] == "observed_upper_bound"
        assert legacy_fill.payload["fill_time_source"] == (
            "reconciliation_observed_at"
        )
    finally:
        db.close()
        engine.dispose()


def test_cumulative_noop_reconcile_marks_legacy_order_time_repair_as_bounded() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    db, engine = _session(settings)
    observed_at = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id="legacy-kis-order-time-repair",
        symbol="005930",
        side="sell",
    )
    order.broker_order_id = "legacy-kis-order-time-repair"
    legacy_fill = _fill(
        order=order,
        fill_id="legacy-kis-order-time-repair-row",
        side="sell",
        quantity=1,
        price="71000",
        currency="KRW",
        cost="0",
        coverage="complete",
        filled_at=observed_at - timedelta(days=1),
        payload={
            "source": "operator_fill_evidence_repair",
            "repair_evidence": {
                "source": "broker_orders.raw_broker_payload",
                "fields": ["average_fill_price", "filled_quantity"],
            },
            "proposed_fill": {
                "filled_at": (observed_at - timedelta(days=1)).isoformat(),
                "quantity": 1,
                "price": "71000",
            },
        },
    )
    try:
        db.add_all([order, legacy_fill])
        db.commit()
        db.refresh(order)

        rows, anomalies = _parse_fills_from_payload(
            payload={
                "filled_quantity": 1,
                "average_fill_price": "71000",
                "filled_at": observed_at.isoformat(),
                "fill_time_precision": "observed_upper_bound",
                "fill_time_source": "reconciliation_observed_at",
            },
            order=order,
            observed_at=observed_at,
        )

        assert anomalies == []
        assert len(rows) == 1
        assert rows[0]["fill_time_precision"] == "observed_upper_bound"
        assert _upsert_order_fills(db=db, order=order, fills=rows) == 0
        db.commit()
        db.refresh(legacy_fill)
        assert legacy_fill.payload["fill_time_precision"] == "observed_upper_bound"
        assert legacy_fill.payload["fill_time_source"] == (
            "reconciliation_observed_at"
        )
    finally:
        db.close()
        engine.dispose()


@pytest.mark.parametrize(
    "chronology_payload",
    [
        {
            "fill_time_precision": "observed_upper_bound",
            "fill_time_source": "reconciliation_observed_at",
        },
        {
            "price_derivation": "cumulative_notional_delta",
            "cumulative_quantity": 1,
        },
    ],
    ids=["marked-observation-bound", "legacy-unmarked-cumulative"],
)
def test_us_drawdown_gate_blocks_observation_bounded_fill_chronology(
    chronology_payload: dict[str, object],
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    try:
        buy_order = _order(
            order_id="bounded-time-buy",
            symbol="AAPL",
            side="buy",
            asset_class="us_equity",
            account_scope=account_scope,
        )
        sell_order = _order(
            order_id="bounded-time-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
            account_scope=account_scope,
        )
        db.add_all(
            [
                buy_order,
                sell_order,
                _fill(
                    order=buy_order,
                    fill_id="bounded-time-buy-fill",
                    side="buy",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(days=1),
                ),
                _fill(
                    order=sell_order,
                    fill_id="bounded-time-sell-fill",
                    side="sell",
                    quantity=1,
                    price="90",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now,
                    payload=chronology_payload,
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
            order_idempotency_key="bounded-time-next-buy",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_chronology_unconfirmed"
        assert (
            decision.payload["metrics"]["daily_fifo_chronology_coverage"]
            == "incomplete"
        )
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_conflicting_duplicate_fill_identity() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    buy_order = _order(
        order_id="conflicting-fill-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    sell_order = _order(
        order_id="conflicting-fill-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    buy = _fill(
        order=buy_order,
        fill_id="conflicting-fill-buy-row",
        side="buy",
        quantity=1,
        price="200",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now - timedelta(hours=1),
    )
    sell_low = _fill(
        order=sell_order,
        fill_id="a-conflicting-fill-sell-row",
        side="sell",
        quantity=1,
        price="90",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    sell_high = _fill(
        order=sell_order,
        fill_id="b-conflicting-fill-sell-row",
        side="sell",
        quantity=1,
        price="190",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    sell_low.broker_fill_id = "conflicting-broker-fill"
    sell_high.broker_fill_id = "conflicting-broker-fill"
    try:
        db.add_all([buy_order, sell_order, buy, sell_low, sell_high])
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
            order_idempotency_key="conflicting-fill-next-buy",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_fill_conflict"
        assert decision.payload["metrics"]["daily_conflicting_fill_group_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_duplicate_identity_with_conflicting_fill_time() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    order = _order(
        order_id="conflicting-time-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    old_observation = _fill(
        order=order,
        fill_id="old-conflicting-time-row",
        side="sell",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now - timedelta(days=10),
    )
    current_observation = _fill(
        order=order,
        fill_id="current-conflicting-time-row",
        side="sell",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    old_observation.broker_fill_id = "same-broker-fill-id"
    current_observation.broker_fill_id = "same-broker-fill-id"
    try:
        db.add_all([order, old_observation, current_observation])
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
            order_idempotency_key="buy-after-conflicting-fill-time",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_fill_conflict"
        assert decision.payload["metrics"]["daily_conflicting_fill_group_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "corrupt_value"),
    [
        ("symbol", "MSFT"),
        ("side", "sell"),
        ("price", Decimal("0")),
        ("price", Decimal("Infinity")),
        ("quantity", 0),
    ],
)
def test_us_drawdown_gate_blocks_single_invalid_legacy_fill(
    field_name: str,
    corrupt_value: object,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id=f"invalid-legacy-{field_name}",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=_kis_us_account_scope(settings),
    )
    fill = _fill(
        order=order,
        fill_id=f"invalid-legacy-{field_name}-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    setattr(fill, field_name, corrupt_value)
    try:
        db.add_all([order, fill])
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
            order_idempotency_key=f"buy-after-invalid-{field_name}",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_invalid_fill"
        assert decision.payload["metrics"]["invalid_fill_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_replay_uses_normalized_fill_symbol_and_side() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    buy_order = _order(
        order_id="normalized-legacy-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    sell_order = _order(
        order_id="normalized-legacy-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    buy = _fill(
        order=buy_order,
        fill_id="normalized-legacy-buy-fill",
        side="buy",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now - timedelta(hours=1),
    )
    buy.symbol = " aapl "
    buy.side = " BUY "
    sell = _fill(
        order=sell_order,
        fill_id="normalized-legacy-sell-fill",
        side="sell",
        quantity=1,
        price="110",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    try:
        db.add_all([buy_order, sell_order, buy, sell])
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
            order_idempotency_key="buy-after-normalized-legacy-fill",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is True
        assert decision.payload["metrics"]["invalid_fill_count"] == 0
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] == "10.0000"
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_unmatched_sale_from_older_conflicting_cost_basis() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    buy_order = _order(
        order_id="old-conflicting-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    sell_order = _order(
        order_id="current-unmatched-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    old_buy_low = _fill(
        order=buy_order,
        fill_id="old-conflicting-buy-low",
        side="buy",
        quantity=1,
        price="200",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now - timedelta(days=10),
    )
    old_buy_high = _fill(
        order=buy_order,
        fill_id="old-conflicting-buy-high",
        side="buy",
        quantity=1,
        price="300",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now - timedelta(days=10),
    )
    old_buy_low.broker_fill_id = "same-old-broker-fill"
    old_buy_high.broker_fill_id = "same-old-broker-fill"
    buy_order.submitted_at = now - timedelta(days=10)
    buy_order.last_broker_sync_at = now - timedelta(days=10)
    sell_order.submitted_at = now
    sell_order.last_broker_sync_at = now
    sale = _fill(
        order=sell_order,
        fill_id="current-sale-after-conflict",
        side="sell",
        quantity=1,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    try:
        db.add_all([buy_order, sell_order, old_buy_low, old_buy_high, sale])
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
            order_idempotency_key="buy-after-old-conflict",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_fill_conflict"
        assert decision.payload["metrics"]["daily_conflicting_fill_group_count"] == 1
        assert (
            decision.payload["metrics"]["daily_unmatched_sell_quantity"]
            == "1.0000"
        )
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_old_conflicting_sale_before_descendant_fifo_pnl() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-old-conflict-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    buy_order = _order(
        order_id="old-conflict-lineage-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
        quantity=2,
    )
    conflicting_sell_order = _order(
        order_id="old-conflicting-lineage-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    current_sell_order = _order(
        order_id="current-sale-after-old-conflicting-sale",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    old_at = now - timedelta(days=10)
    for order in (buy_order, conflicting_sell_order):
        order.submitted_at = old_at
        order.last_broker_sync_at = old_at
    current_sell_order.submitted_at = now
    current_sell_order.last_broker_sync_at = now
    old_buy = _fill(
        order=buy_order,
        fill_id="old-conflict-lineage-buy-fill",
        side="buy",
        quantity=2,
        price="100",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=old_at,
    )
    old_sell_low = _fill(
        order=conflicting_sell_order,
        fill_id="old-conflicting-sale-low",
        side="sell",
        quantity=1,
        price="150",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=old_at,
    )
    old_sell_high = _fill(
        order=conflicting_sell_order,
        fill_id="old-conflicting-sale-high",
        side="sell",
        quantity=1,
        price="160",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=old_at,
    )
    old_sell_low.broker_fill_id = "same-old-conflicting-sale"
    old_sell_high.broker_fill_id = "same-old-conflicting-sale"
    current_sale = _fill(
        order=current_sell_order,
        fill_id="current-sale-after-old-conflict-fill",
        side="sell",
        quantity=1,
        price="200",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=now,
    )
    try:
        db.add_all(
            [
                buy_order,
                conflicting_sell_order,
                current_sell_order,
                old_buy,
                old_sell_low,
                old_sell_high,
                current_sale,
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
            order_idempotency_key="buy-after-old-conflicting-sale",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_fill_conflict"
        assert decision.payload["metrics"]["daily_conflicting_fill_group_count"] == 1
        assert decision.payload["metrics"]["daily_covered_gross_realized_pnl_usd"] == "100.0000"
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_verified_flat_boundary_isolates_old_conflicting_fill_group() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-conflict-isolation-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    old_at = now - timedelta(days=10)
    boundary_at = now - timedelta(days=5)
    account_scope = _kis_us_account_scope(settings)
    order = _order(
        order_id="isolated-old-conflicting-sale",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    order.submitted_at = old_at
    order.last_broker_sync_at = old_at
    low = _fill(
        order=order,
        fill_id="isolated-old-conflicting-sale-low",
        side="sell",
        quantity=1,
        price="90",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=old_at,
    )
    high = _fill(
        order=order,
        fill_id="isolated-old-conflicting-sale-high",
        side="sell",
        quantity=1,
        price="190",
        currency="USD",
        cost="0",
        coverage="complete",
        filled_at=old_at,
    )
    low.broker_fill_id = "same-isolated-old-sale"
    high.broker_fill_id = "same-isolated-old-sale"
    try:
        db.add_all(
            [
                order,
                low,
                high,
                FifoInventoryIsolation(
                    mode=settings.runtime_mode,
                    provider="kis",
                    account_scope=account_scope,
                    venue="KRX",
                    asset_class="us_equity",
                    exchange="NASD",
                    symbol="AAPL",
                    residual_quantity=Decimal("0"),
                    last_local_fill_at=old_at,
                    broker_position_id="flat-after-old-conflicting-sale",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=boundary_at,
                    isolated_through_at=boundary_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="exact route was subsequently verified flat",
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
            order_idempotency_key="buy-after-isolated-old-conflict",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is True
        assert decision.payload["metrics"]["daily_conflicting_fill_group_count"] == 0
        assert decision.payload["metrics"]["weekly_conflicting_fill_group_count"] == 0
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_filled_order_missing_execution_fill() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id="filled-without-fill",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=_kis_us_account_scope(settings),
    )
    order.broker_order_id = "broker-filled-without-fill"
    order.submitted_at = now - timedelta(minutes=1)
    order.last_broker_sync_at = now
    try:
        db.add(order)
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
            order_idempotency_key="buy-with-missing-fill",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_missing_fill"
        assert decision.payload["metrics"]["daily_missing_fill_order_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_terminal_order_with_partial_fill_evidence() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id="filled-with-partial-evidence",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=_kis_us_account_scope(settings),
        quantity=10,
    )
    order.broker_order_id = "broker-filled-with-partial-evidence"
    order.submitted_at = now - timedelta(minutes=1)
    order.last_broker_sync_at = now
    try:
        db.add_all(
            [
                order,
                _fill(
                    order=order,
                    fill_id="only-one-of-ten",
                    side="sell",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now,
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
            order_idempotency_key="buy-with-partial-fill-evidence",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_incomplete_fill"
        assert (
            decision.payload["metrics"]["daily_incomplete_fill_order_count"]
            == 1
        )
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


@pytest.mark.parametrize(
    ("status", "evidence_field", "evidence"),
    [
        ("canceled", "response_metadata", {"filled_quantity": "1"}),
        ("rejected", "response_metadata", {"filled_quantity": "1"}),
        ("failed", "response_metadata", {"filled_quantity": "1"}),
        ("expired", "response_metadata", {"filled_quantity": "1"}),
        (
            "canceled",
            "raw_broker_payload",
            {
                "output": [
                    {
                        "ft_ord_qty": "10",
                        "ft_ccld_qty": "1",
                        "nccs_qty": "0",
                    }
                ]
            },
        ),
    ],
    ids=[
        "canceled-canonical",
        "rejected-canonical",
        "failed-canonical",
        "expired-canonical",
        "canceled-kis-raw",
    ],
)
def test_us_drawdown_gate_blocks_nonfilled_terminal_with_broker_fill_evidence(
    status: str,
    evidence_field: str,
    evidence: dict[str, object],
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id=f"{status}-{evidence_field}-with-missing-fill",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=_kis_us_account_scope(settings),
        quantity=10,
    )
    order.status = status
    order.broker_order_id = f"broker-{status}-{evidence_field}"
    order.submitted_at = now - timedelta(minutes=1)
    order.last_broker_sync_at = now
    setattr(order, evidence_field, evidence)
    try:
        db.add(order)
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
            order_idempotency_key=f"buy-after-{status}-broker-fill",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_missing_fill"
        assert decision.payload["metrics"]["daily_missing_fill_order_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_nonfilled_terminal_with_partial_broker_fill_evidence() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    order = _order(
        order_id="canceled-with-partial-broker-fill-evidence",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=_kis_us_account_scope(settings),
        quantity=10,
    )
    order.status = "canceled"
    order.broker_order_id = "broker-canceled-with-partial-fill"
    order.submitted_at = now - timedelta(minutes=1)
    order.last_broker_sync_at = now
    order.response_metadata = {"filled_quantity": "2"}
    try:
        db.add_all(
            [
                order,
                _fill(
                    order=order,
                    fill_id="one-of-two-canceled-fill",
                    side="sell",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now,
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
            order_idempotency_key="buy-after-partial-canceled-fill",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_incomplete_fill"
        assert decision.payload["metrics"]["daily_incomplete_fill_order_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


@pytest.mark.parametrize(
    ("persist_partial_fill", "expected_gate", "expected_metric"),
    [
        (False, "us_realized_pnl_missing_fill", "daily_missing_fill_order_count"),
        (
            True,
            "us_realized_pnl_incomplete_fill",
            "daily_incomplete_fill_order_count",
        ),
    ],
)
def test_us_drawdown_gate_blocks_old_unresolved_terminal_fill_lineage(
    persist_partial_fill: bool,
    expected_gate: str,
    expected_metric: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-lineage-test-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    old_buy_order = _order(
        order_id="old-lineage-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
        quantity=2,
    )
    unresolved_sell_order = _order(
        order_id="old-unresolved-lineage-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
        quantity=2,
    )
    current_sell_order = _order(
        order_id="current-lineage-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    old_buy_order.submitted_at = now - timedelta(days=11)
    old_buy_order.last_broker_sync_at = now - timedelta(days=11)
    unresolved_sell_order.submitted_at = now - timedelta(days=10)
    unresolved_sell_order.last_broker_sync_at = now - timedelta(days=10)
    current_sell_order.submitted_at = now
    current_sell_order.last_broker_sync_at = now
    rows: list[object] = [
        old_buy_order,
        unresolved_sell_order,
        current_sell_order,
        _fill(
            order=old_buy_order,
            fill_id="old-lineage-buy-fill",
            side="buy",
            quantity=2,
            price="100",
            currency="USD",
            cost="0",
            coverage="complete",
            filled_at=now - timedelta(days=11),
        ),
        _fill(
            order=current_sell_order,
            fill_id="current-lineage-sell-fill",
            side="sell",
            quantity=1,
            price="200",
            currency="USD",
            cost="0",
            coverage="complete",
            filled_at=now,
        ),
    ]
    if persist_partial_fill:
        rows.append(
            _fill(
                order=unresolved_sell_order,
                fill_id="old-partial-lineage-sell-fill",
                side="sell",
                quantity=1,
                price="150",
                currency="USD",
                cost="0",
                coverage="complete",
                filled_at=now - timedelta(days=10),
            )
        )
    try:
        db.add_all(rows)
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
            order_idempotency_key="buy-after-unresolved-old-lineage",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == expected_gate
        assert decision.payload["metrics"][expected_metric] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_verified_flat_boundary_isolates_old_unresolved_terminal_fill_lineage() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-isolation-test-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    order = _order(
        order_id="isolated-old-unresolved-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    order.submitted_at = now - timedelta(days=10)
    order.last_broker_sync_at = now - timedelta(days=10)
    boundary_at = now - timedelta(days=5)
    try:
        db.add_all(
            [
                order,
                FifoInventoryIsolation(
                    mode=settings.runtime_mode,
                    provider="kis",
                    account_scope=account_scope,
                    venue="KRX",
                    asset_class="us_equity",
                    exchange="NASD",
                    symbol="AAPL",
                    residual_quantity=Decimal("0"),
                    last_local_fill_at=order.last_broker_sync_at,
                    broker_position_id="flat-after-unresolved-terminal-fill",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=boundary_at,
                    isolated_through_at=boundary_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="exact route was subsequently verified flat",
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
            order_idempotency_key="buy-after-isolated-old-lineage",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is True
        assert decision.payload["metrics"]["daily_missing_fill_order_count"] == 0
        assert decision.payload["metrics"]["weekly_missing_fill_order_count"] == 0
    finally:
        db.close()
        engine.dispose()


def test_verified_flat_boundary_isolates_old_nonfilled_terminal_broker_fill_evidence() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-canceled-isolation-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    order = _order(
        order_id="isolated-old-canceled-with-broker-fill",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
        quantity=10,
    )
    order.status = "canceled"
    order.raw_broker_payload = {
        "output": [
            {
                "ft_ord_qty": "10",
                "ft_ccld_qty": "1",
                "nccs_qty": "0",
            }
        ]
    }
    order.submitted_at = now - timedelta(days=10)
    order.last_broker_sync_at = now - timedelta(days=10)
    boundary_at = now - timedelta(days=5)
    try:
        db.add_all(
            [
                order,
                FifoInventoryIsolation(
                    mode=settings.runtime_mode,
                    provider="kis",
                    account_scope=account_scope,
                    venue="KRX",
                    asset_class="us_equity",
                    exchange="NASD",
                    symbol="AAPL",
                    residual_quantity=Decimal("0"),
                    last_local_fill_at=order.last_broker_sync_at,
                    broker_position_id="flat-after-canceled-broker-fill",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=boundary_at,
                    isolated_through_at=boundary_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="exact route was subsequently verified flat",
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
            order_idempotency_key="buy-after-isolated-canceled-lineage",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is True
        assert decision.payload["metrics"]["daily_missing_fill_order_count"] == 0
        assert decision.payload["metrics"]["weekly_missing_fill_order_count"] == 0
    finally:
        db.close()
        engine.dispose()


def test_flat_boundary_before_known_partial_fill_does_not_isolate_missing_lineage() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-partial-boundary-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    order = _order(
        order_id="partial-after-flat-boundary",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
        quantity=2,
    )
    order.submitted_at = now - timedelta(days=10)
    order.last_broker_sync_at = now - timedelta(days=10)
    boundary_at = now - timedelta(days=5)
    partial_fill_at = now - timedelta(days=4)
    try:
        db.add_all(
            [
                order,
                _fill(
                    order=order,
                    fill_id="partial-after-flat-boundary-fill",
                    side="sell",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=partial_fill_at,
                ),
                FifoInventoryIsolation(
                    mode=settings.runtime_mode,
                    provider="kis",
                    account_scope=account_scope,
                    venue="KRX",
                    asset_class="us_equity",
                    exchange="NASD",
                    symbol="AAPL",
                    residual_quantity=Decimal("0"),
                    last_local_fill_at=order.last_broker_sync_at,
                    broker_position_id="flat-before-known-partial-fill",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=boundary_at,
                    isolated_through_at=boundary_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="boundary predates known partial fill",
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
            order_idempotency_key="buy-after-too-early-flat-boundary",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_incomplete_fill"
        assert decision.payload["metrics"]["daily_incomplete_fill_order_count"] == 1
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_future_dated_execution_fill() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    account_scope = _kis_us_account_scope(settings)
    buy_order = _order(
        order_id="future-fill-buy",
        symbol="AAPL",
        side="buy",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    sell_order = _order(
        order_id="future-fill-sell",
        symbol="AAPL",
        side="sell",
        asset_class="us_equity",
        account_scope=account_scope,
    )
    try:
        db.add_all(
            [
                buy_order,
                sell_order,
                _fill(
                    order=buy_order,
                    fill_id="past-buy-fill",
                    side="buy",
                    quantity=1,
                    price="200",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now - timedelta(hours=1),
                ),
                _fill(
                    order=sell_order,
                    fill_id="future-sell-fill",
                    side="sell",
                    quantity=1,
                    price="100",
                    currency="USD",
                    cost="0",
                    coverage="complete",
                    filled_at=now + timedelta(hours=1),
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
            order_idempotency_key="buy-before-future-sale",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_future_fill"
        assert decision.payload["metrics"]["future_fill_count"] == 1
        assert decision.payload["metrics"]["daily_realized_pnl_usd"] is None
    finally:
        db.close()
        engine.dispose()


def test_us_drawdown_gate_blocks_ambiguous_sale_despite_later_verified_flat() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_us_allow_degraded_realized_pnl_coverage=True,
        account_drawdown_recovery_scope_secret="stable-account-scope-secret",
    )
    db, engine = _session(settings)
    now = datetime(2026, 9, 3, 16, tzinfo=UTC)
    ambiguous_at = now - timedelta(hours=3)
    sold_at = ambiguous_at + timedelta(seconds=1)
    boundary_at = sold_at + timedelta(seconds=1)
    account_scope = _kis_us_account_scope(settings)
    try:
        orders = [
            _order(
                order_id=order_id,
                symbol="AAPL",
                side=side,
                asset_class="us_equity",
                account_scope=account_scope,
            )
            for order_id, side in (
                ("ambiguous-cheap-buy", "buy"),
                ("ambiguous-expensive-buy", "buy"),
                ("ambiguous-sale", "sell"),
            )
        ]
        cheap_buy = _fill(
            order=orders[0],
            fill_id="aaa-ambiguous-cheap-buy-fill",
            side="buy",
            quantity=1,
            price="100",
            currency="USD",
            cost="0",
            coverage="complete",
            filled_at=ambiguous_at,
        )
        expensive_buy = _fill(
            order=orders[1],
            fill_id="zzz-ambiguous-expensive-buy-fill",
            side="buy",
            quantity=1,
            price="200",
            currency="USD",
            cost="0",
            coverage="complete",
            filled_at=ambiguous_at,
        )
        sell = _fill(
            order=orders[2],
            fill_id="ambiguous-sale-fill",
            side="sell",
            quantity=1,
            price="150",
            currency="USD",
            cost="0",
            coverage="complete",
            filled_at=sold_at,
        )
        db.add_all(
            [
                *orders,
                cheap_buy,
                expensive_buy,
                sell,
                FifoInventoryIsolation(
                    mode=settings.runtime_mode,
                    provider="kis",
                    account_scope=account_scope,
                    venue="KRX",
                    asset_class="us_equity",
                    exchange="NASD",
                    symbol="AAPL",
                    residual_quantity=Decimal("1"),
                    last_local_fill_at=sold_at,
                    broker_position_id="future-flat-cannot-prove-old-us-fifo",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=boundary_at,
                    isolated_through_at=boundary_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="flat evidence only starts the next generation",
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
            order_idempotency_key="ambiguous-next-buy",
            execution_plan_item_id=None,
            now=now,
        )

        assert decision.allowed is False
        assert decision.gate_name == "us_realized_pnl_chronology_unconfirmed"
        metrics = decision.payload["metrics"]
        assert metrics["daily_fifo_chronology_coverage"] == "incomplete"
        assert metrics["weekly_fifo_chronology_coverage"] == "incomplete"
        assert metrics["daily_realized_pnl_usd"] is None
        assert metrics["weekly_realized_pnl_usd"] is None
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
            account_scope=_kis_us_account_scope(settings),
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
            account_scope=_kis_us_account_scope(settings),
        )
        matched_sell_order = _order(
            order_id="covered-loss-sell",
            symbol="AAPL",
            side="sell",
            asset_class="us_equity",
            account_scope=_kis_us_account_scope(settings),
        )
        unmatched_sell_order = _order(
            order_id="unrelated-unmatched-sell",
            symbol="MSFT",
            side="sell",
            asset_class="us_equity",
            account_scope=_kis_us_account_scope(settings),
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
