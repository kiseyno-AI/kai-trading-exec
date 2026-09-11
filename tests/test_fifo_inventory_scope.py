from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models.runtime import FifoInventoryIsolation
from app.services.fifo_inventory import (
    FifoIsolationCursor,
    FifoRouteScope,
    ambiguous_fifo_chronology_buckets,
    ambiguous_fifo_chronology_fill_count,
    fifo_chronology_ambiguity_reason_at,
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
    replay_open_long_fill_inventory,
)
from app.services.position_identity import PositionIdentity, execution_scope_payload


OBSERVED_AT = datetime(2026, 9, 9, 14, 0, tzinfo=UTC)


def _scoped_order(*, symbol: str = "AAPL") -> SimpleNamespace:
    return SimpleNamespace(
        broker_name="alpaca",
        venue="ALPACA",
        asset_class="us_equity",
        symbol=symbol,
        request_metadata={
            "exchange": "ALPACA",
            "execution_scope": {
                "policy_version": 1,
                "provider": "alpaca",
                "account_scope": "a" * 24,
                "venue": "ALPACA",
                "asset_class": "us_equity",
                "exchange": "ALPACA",
            },
        },
    )


def _fill_event(
    *,
    order: SimpleNamespace,
    side: str,
    quantity: str,
    observed_at: datetime,
    fill_id: str,
    price: str = "100",
    transaction_cost: str | None = "0",
    payload: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=fill_id,
        broker_order=order,
        symbol=order.symbol,
        side=side,
        quantity=int(Decimal(quantity)),
        base_quantity=Decimal(quantity),
        quote_notional=None,
        price=Decimal(price),
        settlement_currency="USD",
        transaction_cost=(
            Decimal(transaction_cost) if transaction_cost is not None else None
        ),
        transaction_cost_coverage=(
            "known" if transaction_cost is not None else "unknown"
        ),
        transaction_cost_source=(
            "broker" if transaction_cost is not None else None
        ),
        payload=payload,
        filled_at=observed_at,
    )


def test_fill_inventory_replay_is_quantity_and_chronology_aware() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="sell",
                quantity="7",
                observed_at=OBSERVED_AT - timedelta(minutes=1),
                fill_id="sell-before-local-buy",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="10",
                observed_at=OBSERVED_AT,
                fill_id="buy-ten",
            ),
            _fill_event(
                order=order,
                side="sell",
                quantity="1",
                observed_at=OBSERVED_AT + timedelta(minutes=1),
                fill_id="partial-sell",
            ),
        ]
    )

    assert replay.open_quantities[(route, "AAPL")] == Decimal("9")
    assert replay.indeterminate_fill_count == 1


def test_fill_inventory_replay_reports_exact_flat_only_after_full_sell() -> None:
    order = _scoped_order()
    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="buy",
                quantity="3",
                observed_at=OBSERVED_AT,
                fill_id="buy-three",
            ),
            _fill_event(
                order=order,
                side="sell",
                quantity="3",
                observed_at=OBSERVED_AT + timedelta(minutes=1),
                fill_id="sell-three",
            ),
        ]
    )

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 0


@pytest.mark.parametrize(
    ("buy_fill_id", "sell_fill_id"),
    [("z-buy", "a-sell"), ("a-buy", "z-sell")],
)
def test_fill_inventory_replay_fails_closed_for_opposing_same_time_fills(
    buy_fill_id: str,
    sell_fill_id: str,
) -> None:
    order = _scoped_order()

    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                observed_at=OBSERVED_AT,
                fill_id=buy_fill_id,
            ),
            _fill_event(
                order=order,
                side="sell",
                quantity="1",
                observed_at=OBSERVED_AT,
                fill_id=sell_fill_id,
            ),
        ]
    )

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 2


@pytest.mark.parametrize(
    ("first_fill_id", "second_fill_id"),
    [("z-first", "a-second"), ("a-first", "z-second")],
)
def test_fill_inventory_replay_fails_closed_for_materially_different_same_time_lots(
    first_fill_id: str,
    second_fill_id: str,
) -> None:
    order = _scoped_order()

    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                price="100",
                transaction_cost="1",
                observed_at=OBSERVED_AT,
                fill_id=first_fill_id,
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="2",
                price="200",
                transaction_cost="3",
                observed_at=OBSERVED_AT,
                fill_id=second_fill_id,
            ),
            _fill_event(
                order=order,
                side="sell",
                quantity="1",
                price="150",
                observed_at=OBSERVED_AT + timedelta(minutes=1),
                fill_id="later-partial-sell",
            ),
        ]
    )

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 3


def test_fill_inventory_replay_allows_merge_safe_same_time_lots() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None

    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                observed_at=OBSERVED_AT,
                fill_id=fill_id,
            )
            for fill_id in ("first-identical", "second-identical")
        ]
    )

    assert replay.open_quantities[(route, "AAPL")] == Decimal("2")
    assert replay.indeterminate_fill_count == 0


def test_observation_bounded_fill_time_marks_single_fill_generation_ambiguous() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    fill = _fill_event(
        order=order,
        side="buy",
        quantity="1",
        observed_at=OBSERVED_AT,
        fill_id="cumulative-reconcile-observation",
        payload={
            "fill_time_precision": "observed_upper_bound",
            "fill_time_source": "reconciliation_observed_at",
        },
    )

    assert ambiguous_fifo_chronology_buckets([fill]) == {
        (route, "AAPL", OBSERVED_AT)
    }

    replay = replay_open_long_fill_inventory(fills=[fill])

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 1


def test_bounded_chronology_is_bidirectional_and_distinct_until_verified_flat() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    bounded_at = OBSERVED_AT
    earlier_at = bounded_at - timedelta(minutes=30)
    boundary_at = bounded_at + timedelta(seconds=1)
    bounded_fill = _fill_event(
        order=order,
        side="buy",
        quantity="1",
        observed_at=bounded_at,
        fill_id="bounded-generation-marker",
        payload={"fill_time_precision": "observed_upper_bound"},
    )
    bounded_buckets = ambiguous_fifo_chronology_buckets([bounded_fill])

    assert fifo_chronology_ambiguity_reason_at(
        ambiguous_buckets=bounded_buckets,
        isolation_cursor=None,
        route_scope=route,
        symbol="AAPL",
        observed_at=earlier_at,
    ) == "observation_bounded_generation"

    cursor = FifoIsolationCursor(
        isolated_at_by_route_symbol={(route, "AAPL"): [boundary_at]}
    )
    assert fifo_chronology_ambiguity_reason_at(
        ambiguous_buckets=bounded_buckets,
        isolation_cursor=cursor,
        route_scope=route,
        symbol="AAPL",
        observed_at=boundary_at + timedelta(seconds=1),
    ) is None

    exact_same_time = [
        _fill_event(
            order=order,
            side="buy",
            quantity="1",
            price=price,
            observed_at=earlier_at,
            fill_id=fill_id,
            payload={"fill_time_precision": "exact"},
        )
        for price, fill_id in (("100", "exact-low"), ("200", "exact-high"))
    ]
    assert fifo_chronology_ambiguity_reason_at(
        ambiguous_buckets=ambiguous_fifo_chronology_buckets(exact_same_time),
        isolation_cursor=None,
        route_scope=route,
        symbol="AAPL",
        observed_at=earlier_at,
    ) == "same_timestamp_bucket"


def test_unmarked_legacy_operator_repair_is_never_treated_as_exact_chronology() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    fill = _fill_event(
        order=order,
        side="buy",
        quantity="1",
        observed_at=OBSERVED_AT,
        fill_id="legacy-unmarked-operator-repair",
        payload={
            "source": "operator_fill_evidence_repair",
            "repair_evidence": {
                "fields": ["average_fill_price", "filled_quantity"],
            },
        },
    )

    assert ambiguous_fifo_chronology_buckets([fill]) == {
        (route, "AAPL", OBSERVED_AT)
    }


def test_explicitly_exact_operator_repair_retains_exact_chronology() -> None:
    order = _scoped_order()
    fill = _fill_event(
        order=order,
        side="buy",
        quantity="1",
        observed_at=OBSERVED_AT,
        fill_id="exact-operator-repair",
        payload={
            "source": "operator_fill_evidence_repair",
            "fill_time_precision": "exact",
            "fill_time_source": "broker_payload",
        },
    )

    assert ambiguous_fifo_chronology_buckets([fill]) == set()


def test_unmarked_cumulative_fill_is_bounded_without_waiting_for_reconciliation() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    fill = _fill_event(
        order=order,
        side="buy",
        quantity="1",
        observed_at=OBSERVED_AT,
        fill_id="legacy-cumulative-fill",
        payload={
            "price_derivation": "cumulative_notional_delta",
            "cumulative_quantity": 1,
        },
    )

    assert ambiguous_fifo_chronology_buckets([fill]) == {
        (route, "AAPL", OBSERVED_AT)
    }


def test_verified_flat_boundary_starts_a_clean_inventory_generation() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    boundary_at = OBSERVED_AT + timedelta(seconds=1)
    cursor = FifoIsolationCursor(
        isolated_at_by_route_symbol={(route, "AAPL"): [boundary_at]},
        legacy_unscoped_at_by_symbol={
            "AAPL": [boundary_at - timedelta(milliseconds=500)]
        },
    )

    fills = [
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                price="100",
                observed_at=OBSERVED_AT,
                fill_id="ambiguous-old-one",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="2",
                price="200",
                observed_at=OBSERVED_AT,
                fill_id="ambiguous-old-two",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="3",
                price="300",
                observed_at=boundary_at + timedelta(seconds=1),
                fill_id="new-generation-buy",
            ),
        ]
    replay = replay_open_long_fill_inventory(
        fills=fills,
        isolation_cursor=cursor,
    )

    assert replay.open_quantities[(route, "AAPL")] == Decimal("3")
    assert replay.indeterminate_fill_count == 0
    assert ambiguous_fifo_chronology_fill_count(
        fills,
        isolation_cursor=cursor,
        through=boundary_at + timedelta(minutes=1),
    ) == 0


def test_new_ambiguity_after_verified_flat_boundary_fails_closed_again() -> None:
    order = _scoped_order()
    route = fifo_route_scope_from_order(order)
    assert route is not None
    boundary_at = OBSERVED_AT + timedelta(seconds=1)
    cursor = FifoIsolationCursor(
        isolated_at_by_route_symbol={(route, "AAPL"): [boundary_at]}
    )

    fills = [
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                price="100",
                observed_at=OBSERVED_AT,
                fill_id="old-ambiguous-one",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="2",
                price="200",
                observed_at=OBSERVED_AT,
                fill_id="old-ambiguous-two",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="3",
                price="300",
                observed_at=boundary_at + timedelta(seconds=1),
                fill_id="new-ambiguous-one",
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="4",
                price="400",
                observed_at=boundary_at + timedelta(seconds=1),
                fill_id="new-ambiguous-two",
            ),
        ]
    replay = replay_open_long_fill_inventory(
        fills=fills,
        isolation_cursor=cursor,
    )

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 2
    assert ambiguous_fifo_chronology_fill_count(
        fills,
        isolation_cursor=cursor,
        through=boundary_at + timedelta(minutes=1),
    ) > 0


def test_authoritative_same_time_fill_sequence_allows_fifo_replay() -> None:
    order = _scoped_order()

    replay = replay_open_long_fill_inventory(
        fills=[
            _fill_event(
                order=order,
                side="sell",
                quantity="1",
                observed_at=OBSERVED_AT,
                fill_id="sequence-two-sell",
                payload={"broker_fill_sequence": 2},
            ),
            _fill_event(
                order=order,
                side="buy",
                quantity="1",
                observed_at=OBSERVED_AT,
                fill_id="sequence-one-buy",
                payload={"broker_fill_sequence": 1},
            ),
        ]
    )

    assert replay.open_quantities == {}
    assert replay.indeterminate_fill_count == 0


def _isolation(
    *,
    row_id: str,
    provider: str | None,
    account_scope: str | None,
    venue: str | None,
    asset_class: str | None,
    exchange: str | None,
) -> FifoInventoryIsolation:
    return FifoInventoryIsolation(
        id=row_id,
        mode="live",
        provider=provider,
        account_scope=account_scope,
        venue=venue,
        asset_class=asset_class,
        exchange=exchange,
        symbol="AAPL",
        residual_quantity=Decimal("1"),
        last_local_fill_at=OBSERVED_AT - timedelta(minutes=1),
        broker_position_id=f"position-{row_id}",
        broker_position_quantity=Decimal("0"),
        broker_reconciled_at=OBSERVED_AT,
        isolated_through_at=OBSERVED_AT,
        evidence_source="broker_flat_reconciliation",
        actor="operator",
        reason="verified flat",
    )


def test_fifo_cursor_applies_boundary_only_to_exact_composite_route() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine, future=True)()
    db.add(
        _isolation(
            row_id="kis-a",
            provider="kis",
            account_scope="account-a",
            venue="NASD",
            asset_class="us_equity",
            exchange="NASD",
        )
    )
    db.commit()
    cursor = load_fifo_isolation_cursor(db=db, runtime_mode="live")
    exact_lots = [Decimal("1")]
    other_account_lots = [Decimal("1")]
    other_provider_lots = [Decimal("1")]

    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-a", "NASD", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=exact_lots,
    ) is True
    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-b", "NASD", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=other_account_lots,
    ) is True
    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "alpaca", "account-a", "NASD", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=other_provider_lots,
    ) is True

    assert exact_lots == []
    assert other_account_lots == [Decimal("1")]
    assert other_provider_lots == [Decimal("1")]
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "mutation",
    ["nonflat_position", "unverified_source", "timestamp_drift"],
)
def test_fifo_cursor_rejects_unproven_exact_route_boundaries(
    mutation: str,
) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine, future=True)()
    isolation = _isolation(
        row_id=f"unproven-{mutation}",
        provider="kis",
        account_scope="account-a",
        venue="NASD",
        asset_class="us_equity",
        exchange="NASD",
    )
    if mutation == "nonflat_position":
        isolation.broker_position_quantity = Decimal("1")
    elif mutation == "unverified_source":
        isolation.evidence_source = "manual_override"
    else:
        isolation.broker_reconciled_at = OBSERVED_AT - timedelta(seconds=1)
    db.add(isolation)
    db.commit()
    cursor = load_fifo_isolation_cursor(db=db, runtime_mode="live")
    lots = [Decimal("1")]

    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-a", "NASD", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=lots,
    ) is False
    assert lots == [Decimal("1")]
    db.close()
    engine.dispose()


def test_fifo_cursor_does_not_apply_legacy_unscoped_boundary_and_reports_ambiguity() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine, future=True)()
    db.add(
        _isolation(
            row_id="legacy",
            provider=None,
            account_scope=None,
            venue=None,
            asset_class=None,
            exchange=None,
        )
    )
    db.commit()
    cursor = load_fifo_isolation_cursor(db=db, runtime_mode="live")
    lots = [Decimal("1")]

    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-a", "NASD", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=lots,
    ) is False
    assert lots == [Decimal("1")]
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("method_name", "event_at"),
    [
        ("apply_before", OBSERVED_AT + timedelta(seconds=2)),
        ("apply_through", OBSERVED_AT + timedelta(seconds=1)),
    ],
)
def test_exact_route_boundary_supersedes_older_legacy_boundary(
    method_name: str,
    event_at: datetime,
) -> None:
    route = FifoRouteScope(
        "kis",
        "account-a",
        "NASD",
        "us_equity",
        "NASD",
    )
    exact_boundary_at = OBSERVED_AT + timedelta(seconds=1)
    cursor = FifoIsolationCursor(
        isolated_at_by_route_symbol={(route, "AAPL"): [exact_boundary_at]},
        legacy_unscoped_at_by_symbol={
            "AAPL": [exact_boundary_at - timedelta(milliseconds=500)]
        },
    )
    lots = [Decimal("1")]

    applied = getattr(cursor, method_name)(
        route_scope=route,
        symbol="AAPL",
        observed_at=event_at,
        lots=lots,
    )

    assert applied is True
    assert lots == []


def test_fifo_model_prevents_duplicate_legacy_unscoped_boundaries() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine, future=True)()
    db.add_all(
        [
            _isolation(
                row_id=row_id,
                provider=None,
                account_scope=None,
                venue=None,
                asset_class=None,
                exchange=None,
            )
            for row_id in ("legacy-one", "legacy-two")
        ]
    )

    with pytest.raises(IntegrityError):
        db.commit()

    db.rollback()
    db.close()
    engine.dispose()


def test_fifo_route_scope_prefers_canonical_execution_scope_metadata() -> None:
    order = SimpleNamespace(
        broker_name="alpaca",
        venue="NASD",
        asset_class="us_equity",
        request_metadata={
            "execution_scope": {
                "policy_version": 1,
                "provider": "alpaca",
                "account_scope": "a" * 24,
                "venue": "NASD",
                "asset_class": "us_equity",
                "exchange": "NASD",
            },
            "broker_account_scope": {
                "policy_version": 1,
                "scope": "legacy-kis-recovery-scope",
            },
        },
    )

    assert fifo_route_scope_from_order(order) == FifoRouteScope(
        "alpaca",
        "a" * 24,
        "NASD",
        "us_equity",
        "NASD",
    )


def test_fifo_route_scope_rejects_canonical_metadata_that_conflicts_with_order_route() -> None:
    order = SimpleNamespace(
        broker_name="alpaca",
        venue="NASD",
        asset_class="us_equity",
        request_metadata={
            "execution_scope": {
                "policy_version": 1,
                "provider": "kis",
                "account_scope": "a" * 24,
                "venue": "NASD",
                "asset_class": "us_equity",
                "exchange": "NASD",
            }
        },
    )

    assert fifo_route_scope_from_order(order) is None


def test_fifo_route_scope_rejects_canonical_exchange_conflict() -> None:
    order = SimpleNamespace(
        broker_name="alpaca",
        venue="ALPACA",
        asset_class="us_equity",
        request_metadata={
            "exchange": "NYSE",
            "execution_scope": {
                "policy_version": 1,
                "provider": "alpaca",
                "account_scope": "a" * 24,
                "venue": "ALPACA",
                "asset_class": "us_equity",
                "exchange": "NASD",
            },
        },
    )

    assert fifo_route_scope_from_order(order) is None


def test_execution_scope_payload_reuses_opaque_position_identity() -> None:
    payload = execution_scope_payload(
        identity=PositionIdentity(
            provider="Alpaca",
            account_scope="b" * 24,
            exchange="nasd",
        ),
        venue="nasd",
        asset_class="US_Equity",
    )

    assert payload == {
        "policy_version": 1,
        "provider": "alpaca",
        "account_scope": "b" * 24,
        "venue": "NASD",
        "asset_class": "us_equity",
        "exchange": "NASD",
    }


def test_fifo_cursor_keeps_same_symbol_lots_isolated_across_market_exchanges() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine, future=True)()
    db.add(
        _isolation(
            row_id="kis-us-nasd",
            provider="kis",
            account_scope="account-a",
            venue="KIS-US",
            asset_class="us_equity",
            exchange="NASD",
        )
    )
    db.commit()
    cursor = load_fifo_isolation_cursor(db=db, runtime_mode="live")
    nasd_lots = [Decimal("1")]
    nyse_lots = [Decimal("1")]

    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-a", "KIS-US", "us_equity", "NASD"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=nasd_lots,
    ) is True
    assert cursor.apply_before(
        route_scope=FifoRouteScope(
            "kis", "account-a", "KIS-US", "us_equity", "NYSE"
        ),
        symbol="AAPL",
        observed_at=OBSERVED_AT + timedelta(seconds=1),
        lots=nyse_lots,
    ) is True

    assert nasd_lots == []
    assert nyse_lots == [Decimal("1")]
    db.close()
    engine.dispose()
