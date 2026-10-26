from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from threading import Thread
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerLiveMarketContext,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis import KIS_LIVE_API_BASE_URL, KIS_PAPER_API_BASE_URL
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    FifoInventoryIsolation,
)
from app.services import broker_orders as broker_orders_service
from app.services import monitoring as monitoring_service
from app.services.fifo_inventory import (
    FifoRouteScope,
    canonical_fifo_inventory_route_scope,
)
from app.services.rejected_order_retries import build_rejected_order_retry_request
from app.services.position_identity import (
    build_position_identity,
    payload_with_position_identity,
)
from test_broker_auth import (
    StubBrokerAdapter,
    _bound_kis_item_payloads,
    _seed_exact_strategy_family_in_session,
    _test_kis_execution_scope,
    build_order_placement,
    build_test_app,
    build_token_grant,
)


MONITORING_SOURCE = "monitoring:intraday_target_stop_monitor"


def test_fifo_inventory_scope_folds_only_same_account_kis_domestic_stock() -> None:
    domestic_scopes = [
        canonical_fifo_inventory_route_scope(
            FifoRouteScope(
                provider="kis",
                account_scope="account-a",
                venue="kis",
                asset_class="stock",
                exchange=exchange,
            )
        )
        for exchange in ("KRX", "NXT", "SOR", "KOSPI", "KOSDAQ")
    ]
    assert len(set(domestic_scopes)) == 1

    different_account = canonical_fifo_inventory_route_scope(
        FifoRouteScope(
            provider="kis",
            account_scope="account-b",
            venue="kis",
            asset_class="stock",
            exchange="NXT",
        )
    )
    kis_us = FifoRouteScope(
        provider="kis",
        account_scope="account-a",
        venue="kis",
        asset_class="us_equity",
        exchange="NASD",
    )
    alpaca = FifoRouteScope(
        provider="alpaca",
        account_scope="account-a",
        venue="alpaca",
        asset_class="us_equity",
        exchange="NASDAQ",
    )
    assert different_account != domestic_scopes[0]
    assert canonical_fifo_inventory_route_scope(kis_us) == kis_us
    assert canonical_fifo_inventory_route_scope(alpaca) == alpaca


def _settings(tmp_path, name: str) -> Settings:
    return Settings(
        database_url=f"sqlite:///{tmp_path / name}",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
        account_drawdown_gate_enabled=False,
        account_drawdown_recovery_scope_secret=(
            "two-track-boundary-scope-secret-at-least-32-bytes"
        ),
    )


def _adapter(
    *,
    holdings: list[BrokerAccountHolding] | None = None,
    open_orders: list[BrokerAccountOpenOrder] | None = None,
    total_asset_value: Decimal | None = Decimal("100000000"),
    available_cash: Decimal | None = Decimal("100000000"),
    buying_power: Decimal | None = Decimal("100000000"),
    open_orders_complete: bool = True,
) -> StubBrokerAdapter:
    return StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(
            broker_order_id="two-track-boundary-order"
        ),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=available_cash,
            buying_power=buying_power,
            observed_at=datetime.now(UTC),
            source="stub:two-track-boundary",
            holdings=holdings if holdings is not None else [],
            open_orders=open_orders if open_orders is not None else [],
            holdings_complete=True,
            open_orders_complete=open_orders_complete,
            total_asset_value=total_asset_value,
            previous_total_asset_value=Decimal("100000000"),
            asset_change_amount=Decimal("0"),
            unrealized_pnl=Decimal("0"),
        ),
    )


def _resume(app) -> None:  # noqa: ANN001
    response = TestClient(app).post(
        "/controls/resume",
        json={"actor": "test", "source": "test", "reason": "boundary regression"},
    )
    assert response.status_code == 200, response.text


def _watch_item(
    *,
    settings: Settings,
    item_id: str,
    lane: str,
    source_key: str,
    price: Decimal = Decimal("100"),
    quantity: int = 1,
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    risk_context, quantity_policy = _bound_kis_item_payloads(
        settings=settings,
        risk_context={
            "exchange": exchange,
            "stop_price": str(price * Decimal("0.90")),
            "target_price": str(price * Decimal("1.20")),
        },
        quantity_policy={"kind": "shares", "quantity": quantity},
    )
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="watch_buy",
        status="watch_buy_triggered_pending_dispatch",
        provider="kis",
        venue="kis",
        asset_class="stock",
        trigger_price=price,
        limit_price=price,
        time_in_force="day",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
        decision_agent_name=lane,
        source_plan_item_id=source_key,
        source_market_date=date(2026, 10, 26),
    )


def _family_sell_item(
    *,
    settings: Settings,
    item_id: str,
    lane: str,
    family: str,
    intent: str,
    status: str,
    quantity: int,
    trigger_price: Decimal = Decimal("90"),
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    risk_context, quantity_policy = _bound_kis_item_payloads(
        settings=settings,
        risk_context={
            "exchange": exchange,
            "entry_context": {"source_plan_item_id": family},
        },
        quantity_policy={
            "kind": "shares",
            "quantity": quantity,
            "linked_entry_item_key": family,
        },
    )
    return ExecutionPlanItem(
        id=item_id,
        symbol="005930",
        side="sell",
        intent=intent,
        status=status,
        provider="kis",
        venue=exchange.lower(),
        asset_class="stock",
        trigger_price=trigger_price,
        time_in_force="day",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
        decision_agent_name=lane,
        source_plan_item_id=family,
        source_market_date=date(2026, 10, 26),
    )


def _held_position(*, settings: Settings, quantity: int) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    return ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=quantity,
        average_price=Decimal("100"),
        updated_from="test",
        last_reconciled_at=datetime.now(UTC),
        raw_payload=payload_with_position_identity(
            {"source": "test"},
            identity=identity,
        ),
    )


def _watch_command(item: ExecutionPlanItem) -> PlaceOrderRequest:
    quantity_policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    quantity = quantity_policy.get("quantity")
    assert isinstance(quantity, int) and not isinstance(quantity, bool) and quantity > 0
    assert item.limit_price is not None
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = str(risk_context.get("exchange") or "KRX").strip().upper()
    return PlaceOrderRequest(
        idempotency_key=f"monitoring:watch_buy:{item.id}",
        execution_plan_item_id=item.id,
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol=item.symbol,
        side="buy",
        quantity=quantity,
        order_type="limit",
        limit_price=item.limit_price,
        time_in_force="day",
        exchange=exchange,
        metadata={
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: (
                broker_orders_service._strategy_entry_family_identity(item)
            )
        },
    )


def _persist_watch_plan(*, session, items: list[ExecutionPlanItem], suffix: str) -> None:  # noqa: ANN001
    for index, item in enumerate(items):
        item.plan_item_index = index
    session.add(
        ExecutionPlan(
            external_plan_id=f"two-track-watch-{suffix}",
            source="test",
            market_date=date(2026, 10, 26),
            mode="paper",
            status="accepted",
            items=items,
        )
    )
    session.commit()


def _persist_local_market_buy_reservation(
    *,
    session,  # noqa: ANN001
    settings: Settings,
    item: ExecutionPlanItem,
    order_id: str,
    broker_order_id: str,
    idempotency_key: str,
    quantity: int,
    unit_price: Decimal,
    reserved_notional: Decimal | None = None,
) -> BrokerOrder:
    family_identity = broker_orders_service._strategy_entry_family_identity(item)
    assert family_identity is not None
    command_metadata = {
        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
    }
    order = BrokerOrder(
        id=order_id,
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=idempotency_key,
        broker_order_id=broker_order_id,
        symbol=item.symbol,
        side="buy",
        status="open",
        quantity=quantity,
        venue="kis",
        asset_class="stock",
        limit_price=None,
        request_metadata={
            "exchange": "KRX",
            "execution_scope": _test_kis_execution_scope(
                settings,
                venue="kis",
            ),
            "metadata": command_metadata,
            "original_command": {"metadata": command_metadata},
            broker_orders_service.SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY: {
                "policy_version": 1,
                "provider": "kis",
                "exchange": "KRX",
                "symbol": item.symbol,
                "side": "buy",
                "execution_plan_item_id": item.id,
                "idempotency_key": idempotency_key,
                "quantity": quantity,
                "unit_price": str(unit_price),
                "reserved_notional": str(
                    reserved_notional
                    if reserved_notional is not None
                    else unit_price * Decimal(quantity)
                ),
            },
        },
    )
    session.add(order)
    session.flush()
    return order


def _seed_single_family_inventory(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    candidate: ExecutionPlanItem,
    lane: str = "signal-decider",
    family: str = "held-family",
    quantity: int = 5,
    entry_exchange: str = "KRX",
) -> None:
    entry = _watch_item(
        settings=settings,
        item_id=f"{family}-entry",
        lane=lane,
        source_key=family,
        quantity=quantity,
        exchange=entry_exchange,
    )
    entry.status = "filled"
    _persist_watch_plan(
        session=session,
        items=[entry, candidate],
        suffix=f"{family}-inventory",
    )
    family_identity = broker_orders_service._strategy_entry_family_identity(entry)
    assert family_identity is not None
    command_metadata = {
        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
    }
    order = BrokerOrder(
        execution_plan_item_id=entry.id,
        broker_name="kis",
        mode="paper",
        idempotency_key=f"{family}-filled-buy",
        broker_order_id=f"broker-{family}-filled-buy",
        symbol=entry.symbol,
        side="buy",
        status="filled",
        quantity=quantity,
        venue="kis",
        asset_class="stock",
        limit_price=Decimal("100"),
        request_metadata={
            "exchange": entry_exchange,
            "execution_scope": _test_kis_execution_scope(
                settings,
                exchange=entry_exchange,
                venue="kis",
            ),
            "metadata": command_metadata,
            "original_command": {"metadata": command_metadata},
        },
    )
    session.add(order)
    session.flush()
    session.add(
        ExecutionFill(
            broker_order_pk=order.id,
            broker_fill_id=f"fill-{family}-filled-buy",
            symbol=entry.symbol,
            side="buy",
            quantity=quantity,
            price=Decimal("100"),
            filled_at=datetime.now(UTC),
            payload={"fill_time_precision": "exact"},
        )
    )
    session.commit()


def test_conflicting_duplicate_buy_fills_never_create_family_monitor_authority(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "conflicting-family-monitor-fill.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    entry = _watch_item(
        settings=settings,
        item_id="conflicting-family-monitor-entry",
        lane="signal-decider",
        source_key="conflicting-family-monitor-family",
        quantity=5,
    )
    entry.status = "filled"
    triggered_at = datetime.now(UTC)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[entry],
            suffix="conflicting-family-monitor",
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(entry)
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        order = BrokerOrder(
            execution_plan_item_id=entry.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="conflicting-family-monitor-buy",
            broker_order_id="broker-conflicting-family-monitor-buy",
            symbol=entry.symbol,
            side="buy",
            status="filled",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("100"),
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _test_kis_execution_scope(
                    settings,
                    venue="kis",
                ),
                "metadata": command_metadata,
                "original_command": {"metadata": command_metadata},
            },
        )
        position = _held_position(settings=settings, quantity=5)
        session.add_all([order, position])
        session.flush()
        for quantity in (5, 4):
            session.add(
                ExecutionFill(
                    broker_order_pk=order.id,
                    broker_fill_id="conflicting-family-monitor-fill",
                    symbol=entry.symbol,
                    side="buy",
                    quantity=quantity,
                    price=Decimal("100"),
                    filled_at=triggered_at - timedelta(minutes=1),
                    payload={"fill_time_precision": "exact"},
                )
            )
        session.commit()

        maintenance_count, review_count = (
            monitoring_service._maintain_post_entry_exit_monitors(
                settings=settings,
                db=session,
                positions_by_symbol={entry.symbol: position},
                prices={},
                triggered_at=triggered_at,
            )
        )
        session.flush()
        generated_exits = session.scalars(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.symbol == entry.symbol,
                ExecutionPlanItem.side == "sell",
            )
        ).all()

        assert maintenance_count == 0
        assert review_count == 0
        assert generated_exits == []

    engine.dispose()


def test_flat_boundary_isolates_old_duplicate_conflict_for_clean_family_monitors(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "isolated-family-monitor-conflict.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    entry = _watch_item(
        settings=settings,
        item_id="isolated-family-monitor-entry",
        lane="signal-decider",
        source_key="isolated-family-monitor-family",
        quantity=5,
    )
    entry.status = "filled"
    triggered_at = datetime.now(UTC)
    old_at = triggered_at - timedelta(hours=3)
    flat_at = triggered_at - timedelta(hours=2)
    current_at = triggered_at - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[entry],
            suffix="isolated-family-monitor",
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(entry)
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }

        def filled_order(*, row_id: str, exchange: str) -> BrokerOrder:
            return BrokerOrder(
                id=row_id,
                execution_plan_item_id=entry.id,
                broker_name="kis",
                mode="paper",
                idempotency_key=row_id,
                broker_order_id=f"broker-{row_id}",
                symbol=entry.symbol,
                side="buy",
                status="filled",
                quantity=5,
                venue="kis",
                asset_class="stock",
                limit_price=Decimal("100"),
                request_metadata={
                    "exchange": exchange,
                    "execution_scope": _test_kis_execution_scope(
                        settings,
                        exchange=exchange,
                        venue="kis",
                    ),
                    "metadata": command_metadata,
                    "original_command": {"metadata": command_metadata},
                },
            )

        old_order = filled_order(
            row_id="isolated-family-monitor-old-buy",
            exchange="NXT",
        )
        current_order = filled_order(
            row_id="isolated-family-monitor-current-buy",
            exchange="KRX",
        )
        position = _held_position(settings=settings, quantity=5)
        scope = _test_kis_execution_scope(settings, exchange="KRX", venue="kis")
        session.add_all([old_order, current_order, position])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    id="isolated-family-monitor-old-fill-a",
                    broker_order=old_order,
                    broker_fill_id="isolated-family-monitor-old-fill",
                    symbol=entry.symbol,
                    side="buy",
                    quantity=5,
                    price=Decimal("90"),
                    filled_at=old_at,
                    payload={"fill_time_precision": "exact"},
                ),
                ExecutionFill(
                    id="isolated-family-monitor-old-fill-b",
                    broker_order=old_order,
                    broker_fill_id="isolated-family-monitor-old-fill",
                    symbol=entry.symbol,
                    side="buy",
                    quantity=4,
                    price=Decimal("90"),
                    filled_at=old_at,
                    payload={"fill_time_precision": "exact"},
                ),
                ExecutionFill(
                    id="isolated-family-monitor-current-fill",
                    broker_order=current_order,
                    broker_fill_id="isolated-family-monitor-current-fill",
                    symbol=entry.symbol,
                    side="buy",
                    quantity=5,
                    price=Decimal("100"),
                    filled_at=current_at,
                    payload={"fill_time_precision": "exact"},
                ),
                FifoInventoryIsolation(
                    mode="paper",
                    provider=scope["provider"],
                    account_scope=scope["account_scope"],
                    venue=scope["venue"],
                    asset_class=scope["asset_class"],
                    exchange=scope["exchange"],
                    symbol=entry.symbol,
                    residual_quantity=Decimal("5"),
                    last_local_fill_at=old_at,
                    broker_position_id="isolated-family-monitor-flat-position",
                    broker_position_quantity=Decimal("0"),
                    broker_reconciled_at=flat_at,
                    isolated_through_at=flat_at,
                    evidence_source="broker_flat_reconciliation",
                    actor="test",
                    reason="verified domestic flat boundary",
                ),
            ]
        )
        session.commit()

        maintenance_count, review_count = (
            monitoring_service._maintain_post_entry_exit_monitors(
                settings=settings,
                db=session,
                positions_by_symbol={entry.symbol: position},
                prices={},
                triggered_at=triggered_at,
            )
        )
        session.flush()
        generated_exits = session.scalars(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.symbol == entry.symbol,
                ExecutionPlanItem.side == "sell",
            )
        ).all()

        assert maintenance_count >= 2
        assert review_count == 0
        assert {item.intent for item in generated_exits} >= {
            "stop_sell",
            "target_sell",
        }

    engine.dispose()


def test_boundary_spanning_duplicate_conflict_never_creates_family_monitors(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "spanning-family-monitor-conflict.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    candidate = _watch_item(
        settings=settings,
        item_id="spanning-family-monitor-candidate",
        lane="short-term-decider",
        source_key="spanning-family-monitor-candidate-family",
    )
    triggered_at = datetime.now(UTC)
    old_at = triggered_at - timedelta(hours=3)
    flat_at = triggered_at - timedelta(hours=2)
    current_at = triggered_at - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            quantity=5,
        )
        order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == "held-family-filled-buy"
            )
        )
        assert order is not None
        original_fill = session.scalar(
            select(ExecutionFill).where(
                ExecutionFill.broker_order_pk == order.id,
                ExecutionFill.broker_fill_id == "fill-held-family-filled-buy",
            )
        )
        assert original_fill is not None
        original_fill.filled_at = old_at
        position = _held_position(settings=settings, quantity=5)
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=order.id,
                    broker_fill_id=original_fill.broker_fill_id,
                    symbol=order.symbol,
                    side="buy",
                    quantity=4,
                    price=Decimal("100"),
                    filled_at=current_at,
                    payload={"fill_time_precision": "exact"},
                ),
                ExecutionFill(
                    broker_order_pk=order.id,
                    broker_fill_id="spanning-family-monitor-clean-fill",
                    symbol=order.symbol,
                    side="buy",
                    quantity=1,
                    price=Decimal("100"),
                    filled_at=current_at + timedelta(microseconds=1),
                    payload={"fill_time_precision": "exact"},
                ),
                position,
            ]
        )
        _persist_domestic_flat_boundary(
            settings=settings,
            session=session,
            symbol=order.symbol,
            broker_reconciled_at=flat_at,
            last_local_fill_at=old_at,
            suffix="spanning-family-monitor",
        )
        session.commit()

        maintenance_count, review_count = (
            monitoring_service._maintain_post_entry_exit_monitors(
                settings=settings,
                db=session,
                positions_by_symbol={order.symbol: position},
                prices={},
                triggered_at=triggered_at,
            )
        )
        generated_exits = session.scalars(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.symbol == order.symbol,
                ExecutionPlanItem.side == "sell",
            )
        ).all()

        assert maintenance_count == 0
        assert review_count == 0
        assert generated_exits == []

    engine.dispose()


@pytest.mark.parametrize(
    "request_source",
    (
        MONITORING_SOURCE,
        "short-term-watcher:decision",
        "execution-job:two-track-boundary",
    ),
)
def test_same_lane_concurrent_watch_buys_claim_one_durable_boundary(
    tmp_path,
    request_source: str,
) -> None:
    source_slug = request_source.split(":", 1)[0]
    settings = _settings(tmp_path, f"same-lane-buy-{source_slug}.db")
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    first_item = _watch_item(
        settings=settings,
        item_id="same-lane-watch-a",
        lane="signal-decider",
        source_key="same-lane-source-a",
    )
    second_item = _watch_item(
        settings=settings,
        item_id="same-lane-watch-b",
        lane="signal-decider",
        source_key="same-lane-source-b",
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[first_item, second_item],
            suffix="same-lane",
        )

    results: list[broker_orders_service.PlaceBrokerOrderResult] = []

    def submit(item_id: str) -> None:
        with app.state.db_session_factory() as session:
            item = session.get(ExecutionPlanItem, item_id)
            assert item is not None
            results.append(
                broker_orders_service.place_broker_order(
                    settings=settings,
                    db=session,
                    adapter=adapter,
                    command=_watch_command(item),
                    request_source=request_source,
                )
            )

    threads = [
        Thread(target=submit, args=(first_item.id,)),
        Thread(target=submit, args=(second_item.id,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status_code for result in results) == [201, 409], [
        result.payload for result in results
    ]
    blocked = next(result for result in results if result.status_code == 409)
    assert blocked.payload["block_reason"] == "same_lane_active_buy"
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_short_term_watch_buy_without_exact_lane_fails_before_broker_call(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "short-term-missing-lane.db")
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _watch_item(
        settings=settings,
        item_id="short-term-missing-lane",
        lane="",
        source_key="short-term-missing-lane-source",
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[item],
            suffix="short-term-missing-lane",
        )
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted).model_copy(
                update={"metadata": None}
            ),
            request_source="short-term-watcher:decision",
        )

    assert result.status_code == 409
    assert result.payload["block_reason"] == "strategy_family_lineage_unavailable"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("candidate_lane", "expected_status", "expected_reason"),
    (
        ("signal-decider", 409, "same_lane_existing_position"),
        ("short-term-decider", 201, None),
    ),
)
def test_existing_position_blocks_only_its_exact_decision_lane(
    tmp_path,
    candidate_lane: str,
    expected_status: int,
    expected_reason: str | None,
) -> None:
    settings = _settings(
        tmp_path,
        f"held-lane-{candidate_lane}.db",
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id=(
            "held-lane-candidate-signal"
            if candidate_lane == "signal-decider"
            else "held-lane-candidate-short"
        ),
        lane=candidate_lane,
        source_key=f"new-{candidate_lane}-family",
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
        )
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source="execution-job:held-lane-boundary",
        )

    assert result.status_code == expected_status, result.payload
    assert result.payload.get("block_reason") == expected_reason
    assert len(adapter.order_calls) == (1 if expected_status == 201 else 0)
    engine.dispose()


@pytest.mark.parametrize(
    ("entry_exchange", "candidate_exchange"),
    (("NXT", "KRX"), ("KRX", "NXT")),
)
@pytest.mark.parametrize(
    ("candidate_lane", "expected_status", "expected_reason"),
    (
        ("signal-decider", 409, "same_lane_existing_position"),
        ("short-term-decider", 201, None),
    ),
)
def test_domestic_route_variants_share_lane_inventory_but_not_lane_ownership(
    tmp_path,
    entry_exchange: str,
    candidate_exchange: str,
    candidate_lane: str,
    expected_status: int,
    expected_reason: str | None,
) -> None:
    settings = _settings(
        tmp_path,
        (
            f"domestic-lane-{entry_exchange}-{candidate_exchange}-"
            f"{candidate_lane}.db"
        ),
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id=(
            f"dom-lane-{entry_exchange.lower()}-{candidate_exchange.lower()}-"
            f"{'signal' if candidate_lane == 'signal-decider' else 'short'}"
        ),
        lane=candidate_lane,
        source_key=f"new-{candidate_lane}-domestic-family",
        exchange=candidate_exchange,
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            entry_exchange=entry_exchange,
        )
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source="execution-job:domestic-route-lane-boundary",
        )

    assert result.status_code == expected_status, result.payload
    assert result.payload.get("block_reason") == expected_reason
    assert len(adapter.order_calls) == (1 if expected_status == 201 else 0)
    engine.dispose()


@pytest.mark.parametrize(
    "request_source",
    (
        MONITORING_SOURCE,
        "short-term-watcher:decision",
        "execution-job:two-track-boundary",
    ),
)
def test_cross_lane_pending_watch_buy_does_not_conflict_with_other_lane(
    tmp_path,
    request_source: str,
) -> None:
    source_slug = request_source.split(":", 1)[0]
    settings = _settings(tmp_path, f"cross-lane-pending-{source_slug}.db")
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    signal_item = _watch_item(
        settings=settings,
        item_id="cross-lane-signal",
        lane="signal-decider",
        source_key="shared-upstream-source",
    )
    short_item = _watch_item(
        settings=settings,
        item_id="cross-lane-short",
        lane="short-term-decider",
        source_key="shared-upstream-source",
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[signal_item, short_item],
            suffix="cross-lane",
        )
        session.add(
            BrokerOrder(
                execution_plan_item_id=signal_item.id,
                broker_name="kis",
                mode="paper",
                idempotency_key="cross-lane-signal-pending",
                symbol="005930",
                side="buy",
                status="submission_pending",
                quantity=1,
                venue="kis",
                asset_class="stock",
                limit_price=Decimal("100"),
                request_metadata={
                    "exchange": "KRX",
                    "execution_scope": _test_kis_execution_scope(
                        settings,
                        venue="kis",
                    ),
                    "metadata": {
                        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: (
                            broker_orders_service._strategy_entry_family_identity(
                                signal_item
                            )
                        ),
                    },
                    "original_command": {
                        "metadata": {
                            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: (
                                broker_orders_service._strategy_entry_family_identity(
                                    signal_item
                                )
                            ),
                        },
                    },
                },
            )
        )
        session.commit()
        short_item = session.get(ExecutionPlanItem, short_item.id)
        assert short_item is not None
        selection = broker_orders_service._load_unresolved_prior_submission(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(short_item),
            execution_scope=_test_kis_execution_scope(settings, venue="kis"),
            plan_item=short_item,
            request_source=request_source,
        )

    assert selection.indeterminate_order is None
    assert selection.matching_orders == ()
    engine.dispose()


@pytest.mark.parametrize(
    "request_source",
    (
        MONITORING_SOURCE,
        "short-term-watcher:decision",
        "execution-job:two-track-boundary",
    ),
)
def test_cross_lane_buys_share_symbol_budget_and_submit_concurrently(
    tmp_path,
    request_source: str,
) -> None:
    source_slug = request_source.split(":", 1)[0]
    settings = _settings(
        tmp_path,
        f"cross-lane-symbol-budget-fit-{source_slug}.db",
    )
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    items = [
        _watch_item(
            settings=settings,
            item_id="symbol-budget-fit-signal",
            lane="signal-decider",
            source_key="symbol-budget-fit-signal-source",
            price=Decimal("100000"),
            quantity=14,
        ),
        _watch_item(
            settings=settings,
            item_id="symbol-budget-fit-short",
            lane="short-term-decider",
            source_key="symbol-budget-fit-short-source",
            price=Decimal("100000"),
            quantity=14,
        ),
    ]
    with app.state.db_session_factory() as session:
        _persist_watch_plan(session=session, items=items, suffix="symbol-budget-fit")

    results: list[broker_orders_service.PlaceBrokerOrderResult] = []
    def submit(item_id: str) -> None:
        with app.state.db_session_factory() as session:
            persisted = session.get(ExecutionPlanItem, item_id)
            assert persisted is not None
            results.append(
                broker_orders_service.place_broker_order(
                    settings=settings,
                    db=session,
                    adapter=adapter,
                    command=_watch_command(persisted),
                    request_source=request_source,
                )
            )

    threads = [Thread(target=submit, args=(item.id,)) for item in items]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status_code for result in results) == [201, 201], [
        result.payload for result in results
    ]
    assert len(adapter.order_calls) == 2
    engine.dispose()


def test_concurrent_cross_lane_buys_cannot_overdraw_shared_symbol_budget(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "cross-lane-symbol-budget-over.db")
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    items = [
        _watch_item(
            settings=settings,
            item_id="symbol-budget-over-signal",
            lane="signal-decider",
            source_key="symbol-budget-over-signal-source",
            price=Decimal("100000"),
            quantity=20,
        ),
        _watch_item(
            settings=settings,
            item_id="symbol-budget-over-short",
            lane="short-term-decider",
            source_key="symbol-budget-over-short-source",
            price=Decimal("100000"),
            quantity=20,
        ),
    ]
    with app.state.db_session_factory() as session:
        _persist_watch_plan(session=session, items=items, suffix="symbol-budget-over")

    results: list[broker_orders_service.PlaceBrokerOrderResult] = []

    def submit(item_id: str) -> None:
        with app.state.db_session_factory() as session:
            persisted = session.get(ExecutionPlanItem, item_id)
            assert persisted is not None
            results.append(
                broker_orders_service.place_broker_order(
                    settings=settings,
                    db=session,
                    adapter=adapter,
                    command=_watch_command(persisted),
                    request_source=MONITORING_SOURCE,
                )
            )

    threads = [Thread(target=submit, args=(item.id,)) for item in items]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status_code for result in results) == [201, 409], [
        result.payload for result in results
    ]
    blocked = next(result for result in results if result.status_code == 409)
    assert blocked.payload["block_reason"] == "symbol_exposure_cap_exceeded"
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_broker_open_buy_remaining_notional_counts_toward_symbol_budget(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "broker-open-symbol-budget.db")
    adapter = _adapter(
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=25,
                remaining_quantity=20,
                limit_price=Decimal("100000"),
                broker_order_id="external-open-buy",
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _watch_item(
        settings=settings,
        item_id="broker-open-symbol-budget-item",
        lane="short-term-decider",
        source_key="broker-open-symbol-budget-source",
        price=Decimal("100000"),
        quantity=20,
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[item],
            suffix="broker-open-symbol-budget",
        )
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "symbol_exposure_cap_exceeded"
    exposure = result.payload["symbol_exposure"]
    assert exposure["broker_open_buy_remaining_notional"] == "2000000"
    assert exposure["broker_open_orders_reapplied_to_buying_power"] is False
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_unknown_broker_open_buy_price_blocks_automated_buy_before_broker(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "broker-open-unknown-price.db")
    adapter = _adapter(
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=5,
                remaining_quantity=5,
                limit_price=None,
                broker_order_id="external-open-buy-unknown-price",
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _watch_item(
        settings=settings,
        item_id="broker-open-unknown-price-item",
        lane="signal-decider",
        source_key="broker-open-unknown-price-source",
        price=Decimal("100000"),
        quantity=1,
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[item],
            suffix="broker-open-unknown-price",
        )
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "symbol_exposure_indeterminate"
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_matched_market_buy_uses_one_durable_valuation_for_symbol_and_cash(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "matched-market-buy-reservation.db")
    prior = _watch_item(
        settings=settings,
        item_id="matched-market-prior",
        lane="signal-decider",
        source_key="matched-market-prior-source",
        price=Decimal("100000"),
        quantity=8,
    )
    candidate = _watch_item(
        settings=settings,
        item_id="matched-market-candidate",
        lane="short-term-decider",
        source_key="matched-market-candidate-source",
        price=Decimal("100000"),
        quantity=2,
    )
    adapter = _adapter(
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=8,
                remaining_quantity=8,
                limit_price=None,
                broker_order_id="matched-market-broker-order",
                idempotency_key="matched-market-local-order",
            )
        ],
        available_cash=Decimal("1000000"),
        buying_power=Decimal("1000000"),
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[prior, candidate],
            suffix="matched-market-buy-reservation",
        )
        _persist_local_market_buy_reservation(
            session=session,
            settings=settings,
            item=prior,
            order_id="matched-market-local-pk",
            broker_order_id="matched-market-broker-order",
            idempotency_key="matched-market-local-order",
            quantity=8,
            unit_price=Decimal("100000"),
        )
        session.commit()
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_final_ioc_price_rechecks_aggregate_cash_under_account_lock(
    tmp_path,
) -> None:
    observed_at = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)

    class FinalAskAdapter(StubBrokerAdapter):
        def get_live_market_context(
            self,
            access_token: str,
            **kwargs: object,
        ) -> BrokerLiveMarketContext:
            assert access_token
            return BrokerLiveMarketContext(
                symbol=str(kwargs["symbol"]),
                exchange=str(kwargs["exchange"]),
                observed_at=observed_at,
                bid=Decimal("900"),
                ask=Decimal("901"),
                normal_ordering_available=True,
                freshness_source="kis_exchange_orderbook_time",
            )

    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'final-ioc-cash-boundary.db'}",
        runtime_mode="live",
        automation_enabled=False,
        domestic_order_session_routing_enabled=True,
        kis_app_key="live-key",
        kis_app_secret="live-secret",
        kis_account_number="12345678",
        account_drawdown_gate_enabled=False,
        account_drawdown_recovery_scope_secret=(
            "two-track-final-cash-boundary-secret-at-least-32-bytes"
        ),
        exec_agent_max_symbol_budget=Decimal("10000"),
        exec_agent_max_position_allocation_pct=Decimal("1"),
        exec_agent_max_symbol_exposure_pct=Decimal("1"),
        exec_agent_max_symbol_total_exposure_pct=Decimal("1"),
    )
    prior_idempotency_key = "final-ioc-prior-market-order"
    prior_broker_order_id = "final-ioc-prior-broker-order"
    adapter = FinalAskAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="live",
            configured=True,
            api_base_url=KIS_LIVE_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(
            broker_order_id="final-ioc-candidate-broker-order"
        ),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            buying_power=Decimal("1000"),
            observed_at=observed_at,
            source="stub:final-ioc-cash-boundary",
            holdings=[],
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol="000660",
                    side="buy",
                    quantity=1,
                    remaining_quantity=1,
                    limit_price=None,
                    broker_order_id=prior_broker_order_id,
                    idempotency_key=prior_idempotency_key,
                )
            ],
            holdings_complete=True,
            open_orders_complete=True,
            total_asset_value=Decimal("10000"),
            previous_total_asset_value=Decimal("10000"),
            asset_change_amount=Decimal("0"),
            unrealized_pnl=Decimal("0"),
        ),
        krx_market_open=True,
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    market_date = observed_at.astimezone(ZoneInfo("Asia/Seoul")).date()
    prior = _watch_item(
        settings=settings,
        item_id="final-ioc-prior-item",
        lane="short-term-decider",
        source_key="final-ioc-prior-family",
        price=Decimal("100"),
        quantity=1,
    )
    prior.symbol = "000660"
    prior.source_market_date = market_date
    candidate = _watch_item(
        settings=settings,
        item_id="final-ioc-candidate-item",
        lane="signal-decider",
        source_key="final-ioc-candidate-family",
        price=Decimal("900"),
        quantity=1,
    )
    candidate.source_market_date = market_date
    candidate.risk_context = {
        **candidate.risk_context,
        "reference_price": "900",
        "trigger_direction": "at_or_above",
        "target_price": "1200",
        "stop_price": "800",
    }
    candidate.quantity_policy = {
        **candidate.quantity_policy,
        "exec_trigger_time_liquidity_evidence_v1": {
            "version": 1,
            "provider": "kis",
            "asset_class": "stock",
            "exchange": "KRX",
            "symbol": candidate.symbol,
            "observed_at": observed_at.isoformat(),
            "freshness_source": "kis_exchange_intraday_minute_time",
            "endpoint": (
                "/uapi/domestic-stock/v1/quotations/"
                "inquire-time-itemchartprice"
            ),
            "tr_id": "FHKST03010200",
            "turnover": "3500000000",
        },
    }

    with app.state.db_session_factory() as session:
        prior.plan_item_index = 0
        candidate.plan_item_index = 1
        session.add(
            ExecutionPlan(
                external_plan_id="final-ioc-cash-boundary-plan",
                source="test",
                market_date=market_date,
                mode="live",
                status="accepted",
                items=[prior, candidate],
            )
        )
        session.flush()
        _persist_local_market_buy_reservation(
            session=session,
            settings=settings,
            item=prior,
            order_id="final-ioc-prior-local-order",
            broker_order_id=prior_broker_order_id,
            idempotency_key=prior_idempotency_key,
            quantity=1,
            unit_price=Decimal("100"),
        )
        session.commit()
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        base_command = _watch_command(persisted)
        command = base_command.model_copy(
            update={
                "order_type": "market",
                "limit_price": None,
                "metadata": {
                    **(base_command.metadata or {}),
                    "route_now": observed_at.astimezone(
                        ZoneInfo("Asia/Seoul")
                    ).isoformat(),
                },
            }
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
            now=observed_at,
            freshness_reference_at=observed_at,
        )
        candidate_order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == command.idempotency_key
            )
        )
        assert candidate_order is not None, result.payload
        request_metadata = candidate_order.request_metadata

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "buy_capital_reservation"
    reservation = request_metadata[
        broker_orders_service.SYMBOL_EXPOSURE_RESERVATION_METADATA_KEY
    ]
    assert reservation["unit_price"] == "901"
    assert reservation["reserved_notional"] == "901"
    assert request_metadata["final_broker_command"]["limit_price"] == "901"
    assert len(adapter.order_calls) == 0
    engine.dispose()


@pytest.mark.parametrize(
    ("snapshot_quantity", "snapshot_remaining", "reserved_notional"),
    [
        (8, 8, Decimal("700000")),
        (8, 9, Decimal("800000")),
    ],
    ids=("tampered-notional", "remaining-exceeds-reservation"),
)
def test_matched_market_buy_invalid_durable_valuation_fails_closed(
    tmp_path,
    snapshot_quantity: int,
    snapshot_remaining: int,
    reserved_notional: Decimal,
) -> None:
    settings = _settings(tmp_path, f"invalid-market-buy-{snapshot_remaining}.db")
    prior = _watch_item(
        settings=settings,
        item_id=f"invalid-market-prior-{snapshot_remaining}",
        lane="signal-decider",
        source_key=f"invalid-market-prior-source-{snapshot_remaining}",
        price=Decimal("100000"),
        quantity=8,
    )
    candidate = _watch_item(
        settings=settings,
        item_id=f"invalid-market-candidate-{snapshot_remaining}",
        lane="short-term-decider",
        source_key=f"invalid-market-candidate-source-{snapshot_remaining}",
        price=Decimal("100000"),
        quantity=1,
    )
    adapter = _adapter(
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=snapshot_quantity,
                remaining_quantity=snapshot_remaining,
                limit_price=None,
                broker_order_id=f"invalid-market-broker-{snapshot_remaining}",
                idempotency_key=f"invalid-market-local-{snapshot_remaining}",
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[prior, candidate],
            suffix=f"invalid-market-buy-{snapshot_remaining}",
        )
        _persist_local_market_buy_reservation(
            session=session,
            settings=settings,
            item=prior,
            order_id=f"invalid-market-local-pk-{snapshot_remaining}",
            broker_order_id=f"invalid-market-broker-{snapshot_remaining}",
            idempotency_key=f"invalid-market-local-{snapshot_remaining}",
            quantity=8,
            unit_price=Decimal("100000"),
            reserved_notional=reserved_notional,
        )
        session.commit()
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_matched_market_buy_multiple_local_matches_fail_closed(tmp_path) -> None:
    settings = _settings(tmp_path, "ambiguous-market-buy.db")
    first = _watch_item(
        settings=settings,
        item_id="ambiguous-market-prior-a",
        lane="signal-decider",
        source_key="ambiguous-market-prior-source-a",
        quantity=4,
    )
    second = _watch_item(
        settings=settings,
        item_id="ambiguous-market-prior-b",
        lane="short-term-decider",
        source_key="ambiguous-market-prior-source-b",
        quantity=4,
    )
    candidate = _watch_item(
        settings=settings,
        item_id="ambiguous-market-candidate",
        lane="signal-decider-secondary",
        source_key="ambiguous-market-candidate-source",
    )
    adapter = _adapter(
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=4,
                remaining_quantity=4,
                limit_price=None,
                broker_order_id="ambiguous-market-broker-order",
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[first, second, candidate],
            suffix="ambiguous-market-buy",
        )
        for suffix, item in (("a", first), ("b", second)):
            _persist_local_market_buy_reservation(
                session=session,
                settings=settings,
                item=item,
                order_id=f"ambiguous-market-local-pk-{suffix}",
                broker_order_id="ambiguous-market-broker-order",
                idempotency_key=f"ambiguous-market-local-{suffix}",
                quantity=4,
                unit_price=Decimal("100"),
            )
        session.commit()
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_local_partial_buy_reserves_only_remaining_symbol_notional(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        "local-partial-symbol-budget.db",
    ).model_copy(update={"exec_agent_max_symbol_budget": Decimal("4000000")})
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("100000"),
                payload={"current_price": "100000"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    prior = _watch_item(
        settings=settings,
        item_id="local-partial-prior",
        lane="signal-decider",
        source_key="local-partial-prior-source",
        price=Decimal("100000"),
        quantity=20,
    )
    candidate = _watch_item(
        settings=settings,
        item_id="local-partial-candidate",
        lane="short-term-decider",
        source_key="local-partial-candidate-source",
        price=Decimal("100000"),
        quantity=20,
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[prior, candidate],
            suffix="local-partial-symbol-budget",
        )
        prior_identity = broker_orders_service._strategy_entry_family_identity(prior)
        assert prior_identity is not None
        prior_command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: prior_identity,
        }
        prior_order = BrokerOrder(
            execution_plan_item_id=prior.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="local-partial-prior-order",
            broker_order_id="local-partial-prior-broker-order",
            symbol="005930",
            side="buy",
            status="partially_filled",
            quantity=20,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("100000"),
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _test_kis_execution_scope(settings, venue="kis"),
                "metadata": prior_command_metadata,
                "original_command": {"metadata": prior_command_metadata},
            },
        )
        session.add(prior_order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=prior_order.id,
                broker_fill_id="local-partial-prior-fill",
                symbol="005930",
                side="buy",
                quantity=10,
                price=Decimal("100000"),
                filled_at=datetime.now(UTC),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_unknown_holding_current_value_blocks_automated_buy_before_broker(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "unknown-holding-symbol-budget.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=1,
                average_price=Decimal("100000"),
                payload={},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _watch_item(
        settings=settings,
        item_id="unknown-holding-symbol-budget-item",
        lane="signal-decider",
        source_key="unknown-holding-symbol-budget-source",
        price=Decimal("100000"),
        quantity=1,
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=item,
            lane="short-term-decider",
            family="unknown-holding-short-family",
            quantity=1,
        )
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "symbol_exposure_indeterminate"
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_unknown_account_basis_blocks_percentage_symbol_caps_before_broker(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "unknown-account-basis-symbol-budget.db")
    adapter = _adapter(
        total_asset_value=None,
        available_cash=None,
        buying_power=None,
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _watch_item(
        settings=settings,
        item_id="unknown-account-basis-item",
        lane="signal-decider",
        source_key="unknown-account-basis-symbol-budget-source",
        price=Decimal("100000"),
        quantity=1,
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[item],
            suffix="unknown-account-basis-symbol-budget",
        )
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(persisted),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "symbol_exposure_indeterminate"
    assert len(adapter.order_calls) == 0
    engine.dispose()


def _family_item(
    *,
    settings: Settings,
    item_id: str,
    index: int,
    side: str,
    intent: str,
    lane: str,
    family: str,
    trigger_price: Decimal | None = None,
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    risk_context, quantity_policy = _bound_kis_item_payloads(
        settings=settings,
        risk_context={"exchange": exchange},
        quantity_policy={
            "kind": "shares",
            "quantity": 5,
            **(
                {"linked_entry_item_key": family}
                if side == "sell"
                else {}
            ),
        },
    )
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=index,
        symbol="005930",
        side=side,
        intent=intent,
        status=(
            f"{intent}_triggered_pending_dispatch"
            if side == "sell"
            else "filled"
        ),
        provider="kis",
        venue="kis",
        asset_class="stock",
        trigger_price=trigger_price,
        limit_price=trigger_price,
        time_in_force="day",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
        decision_agent_name=lane,
        source_plan_item_id=family if side == "buy" else f"{family}-{intent}",
        source_market_date=date(2026, 10, 26),
    )


def _sell_command(item: ExecutionPlanItem) -> PlaceOrderRequest:
    identity = broker_orders_service._strategy_entry_family_identity(item)
    assert identity is not None
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = str(risk_context.get("exchange") or "KRX").strip().upper()
    return PlaceOrderRequest(
        idempotency_key=f"monitoring:{item.intent}:{item.id}",
        execution_plan_item_id=item.id,
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol=item.symbol,
        side="sell",
        quantity=5,
        order_type="limit",
        limit_price=item.limit_price,
        time_in_force="day",
        exchange=exchange,
        metadata={
            "deterministic_sell_quantity_cap": 5,
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: identity,
        },
    )


def _bind_monitoring_authority(
    item: ExecutionPlanItem,
    command: PlaceOrderRequest,
) -> None:
    risk_context = dict(item.risk_context or {})
    risk_context["monitoring_order_authority"] = {
        "policy_version": 1,
        "source": MONITORING_SOURCE,
        "command": command.model_dump(mode="json"),
    }
    item.risk_context = risk_context


def _seed_two_family_inventory(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    exit_items: list[ExecutionPlanItem],
    signal_fill_at: datetime | None = None,
    short_fill_at: datetime | None = None,
) -> None:
    signal_buy = _family_item(
        settings=settings,
        item_id="family-a-entry",
        index=0,
        side="buy",
        intent="watch_buy",
        lane="signal-decider",
        family="family-a",
    )
    short_buy = _family_item(
        settings=settings,
        item_id="family-b-entry",
        index=1,
        side="buy",
        intent="watch_buy",
        lane="short-term-decider",
        family="family-b",
    )
    plan = ExecutionPlan(
        external_plan_id="two-track-family-inventory",
        source="kai-trading-core",
        market_date=date(2026, 10, 26),
        mode="paper",
        status="accepted",
        items=[signal_buy, short_buy, *exit_items],
    )
    session.add(plan)
    session.flush()
    default_signal_fill_at = datetime.now(UTC)
    fill_times = {
        "family-a-buy": signal_fill_at or default_signal_fill_at,
        "family-b-buy": short_fill_at
        or (default_signal_fill_at + timedelta(microseconds=1)),
    }
    for item, key, price in (
        (signal_buy, "family-a-buy", Decimal("100")),
        (short_buy, "family-b-buy", Decimal("110")),
    ):
        family_identity = broker_orders_service._strategy_entry_family_identity(item)
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        order = BrokerOrder(
            execution_plan_item_id=item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key=key,
            broker_order_id=f"broker-{key}",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=price,
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _test_kis_execution_scope(settings, venue="kis"),
                "metadata": command_metadata,
                "original_command": {"metadata": command_metadata},
            },
        )
        session.add(order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=order.id,
                broker_fill_id=f"fill-{key}",
                symbol="005930",
                side="buy",
                quantity=5,
                price=price,
                filled_at=fill_times[key],
                payload={"fill_time_precision": "exact"},
            )
        )
    session.commit()


def _persist_exact_family_buy_fill(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    item: ExecutionPlanItem,
    exchange: str,
    quantity: int,
    price: Decimal,
    filled_at: datetime,
    suffix: str,
) -> BrokerOrder:
    family_identity = broker_orders_service._strategy_entry_family_identity(item)
    assert family_identity is not None
    command_metadata = {
        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
    }
    order = BrokerOrder(
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=f"domestic-family-buy-{suffix}",
        broker_order_id=f"broker-domestic-family-buy-{suffix}",
        symbol=item.symbol,
        side="buy",
        status="filled",
        quantity=quantity,
        venue="kis",
        asset_class="stock",
        limit_price=price,
        request_metadata={
            "exchange": exchange,
            "execution_scope": _test_kis_execution_scope(
                settings,
                exchange=exchange,
                venue="kis",
            ),
            "metadata": command_metadata,
            "original_command": {"metadata": command_metadata},
        },
    )
    session.add(order)
    session.flush()
    session.add(
        ExecutionFill(
            broker_order_pk=order.id,
            broker_fill_id=f"fill-domestic-family-buy-{suffix}",
            symbol=item.symbol,
            side="buy",
            quantity=quantity,
            price=price,
            filled_at=filled_at,
            payload={"fill_time_precision": "exact"},
        )
    )
    return order


def _persist_exact_family_sell_fill(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    item: ExecutionPlanItem,
    exchange: str,
    quantity: int,
    price: Decimal,
    filled_at: datetime,
    suffix: str,
) -> BrokerOrder:
    family_identity = broker_orders_service._strategy_entry_family_identity(item)
    assert family_identity is not None
    command_metadata = {
        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
    }
    order = BrokerOrder(
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=f"domestic-family-sell-{suffix}",
        broker_order_id=f"broker-domestic-family-sell-{suffix}",
        symbol=item.symbol,
        side="sell",
        status="filled",
        quantity=quantity,
        venue="kis",
        asset_class="stock",
        limit_price=price,
        request_metadata={
            "exchange": exchange,
            "execution_scope": _test_kis_execution_scope(
                settings,
                exchange=exchange,
                venue="kis",
            ),
            "metadata": command_metadata,
            "original_command": {"metadata": command_metadata},
        },
    )
    session.add(order)
    session.flush()
    session.add(
        ExecutionFill(
            broker_order_pk=order.id,
            broker_fill_id=f"fill-domestic-family-sell-{suffix}",
            symbol=item.symbol,
            side="sell",
            quantity=quantity,
            price=price,
            filled_at=filled_at,
            payload={"fill_time_precision": "exact"},
        )
    )
    item.status = "filled"
    return order


@pytest.mark.parametrize("candidate_side", ("buy", "sell"))
def test_final_boundary_blocks_crossing_broker_order_on_opposite_lane(
    tmp_path,
    candidate_side: str,
) -> None:
    settings = _settings(
        tmp_path,
        f"opposite-lane-crossing-{candidate_side}.db",
    )
    opposite_side = "sell" if candidate_side == "buy" else "buy"
    adapter = _adapter(
        holdings=(
            [
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=5,
                    sellable_quantity=5,
                    average_price=Decimal("100"),
                    payload={"current_price": "100"},
                )
            ]
            if candidate_side == "sell"
            else []
        ),
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side=opposite_side,
                quantity=1,
                remaining_quantity=1,
                limit_price=Decimal("100"),
                broker_order_id=f"broker-opposite-{opposite_side}",
                idempotency_key=f"opposite-{opposite_side}",
            )
        ],
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)

    if candidate_side == "buy":
        candidate = _watch_item(
            settings=settings,
            item_id="opposite-lane-crossing-buy",
            lane="short-term-decider",
            source_key="opposite-lane-buy-family",
            price=Decimal("100"),
        )
        command = _watch_command(candidate)
        with app.state.db_session_factory() as session:
            _persist_watch_plan(
                session=session,
                items=[candidate],
                suffix="opposite-lane-crossing-buy",
            )
            result = broker_orders_service.place_broker_order(
                settings=settings,
                db=session,
                adapter=adapter,
                command=command,
                request_source=MONITORING_SOURCE,
            )
    else:
        candidate = _family_item(
            settings=settings,
            item_id="opposite-lane-crossing-sell",
            index=1,
            side="sell",
            intent="stop_sell",
            lane="signal-decider",
            family="opposite-lane-sell-family",
            trigger_price=Decimal("100"),
        )
        command = _sell_command(candidate)
        _bind_monitoring_authority(candidate, command)
        with app.state.db_session_factory() as session:
            _seed_single_family_inventory(
                settings=settings,
                session=session,
                candidate=candidate,
                lane="signal-decider",
                family="opposite-lane-sell-family",
            )
            result = broker_orders_service.place_broker_order(
                settings=settings,
                db=session,
                adapter=adapter,
                command=command,
                request_source=MONITORING_SOURCE,
            )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "opposite_order_self_cross_risk"
    assert result.payload["opposite_order_boundary"]["crossing_order_count"] == 1
    assert adapter.order_calls == []
    engine.dispose()


def _persist_active_opposite_lane_order(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    item: ExecutionPlanItem,
    side: str,
    price: Decimal,
    exchange: str,
    suffix: str,
) -> BrokerOrder:
    identity = broker_orders_service._strategy_entry_family_identity(item)
    assert identity is not None
    order = BrokerOrder(
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=f"opposite-lane-local-{suffix}",
        broker_order_id=f"opposite-lane-broker-{suffix}",
        symbol=item.symbol,
        side=side,
        status="open",
        quantity=1,
        venue="kis",
        asset_class="stock",
        limit_price=price,
        request_metadata={
            "exchange": exchange,
            "execution_scope": _test_kis_execution_scope(
                settings,
                exchange=exchange,
                venue="kis",
            ),
            "metadata": {
                broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: identity,
            },
            "original_command": {
                "order_type": "limit",
                "limit_price": str(price),
                "metadata": {
                    broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: (
                        identity
                    ),
                },
            },
        },
    )
    session.add(order)
    session.commit()
    return order


@pytest.mark.parametrize("candidate_side", ("buy", "sell"))
def test_final_boundary_allows_non_crossing_buy_and_sell_across_lanes_and_routes(
    tmp_path,
    candidate_side: str,
) -> None:
    settings = _settings(
        tmp_path,
        f"opposite-lane-noncrossing-{candidate_side}.db",
    )
    opposite_side = "sell" if candidate_side == "buy" else "buy"
    candidate_price = Decimal("90") if candidate_side == "buy" else Decimal("110")
    opposite_price = Decimal("110") if candidate_side == "buy" else Decimal("90")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side=opposite_side,
                quantity=1,
                remaining_quantity=1,
                limit_price=opposite_price,
                broker_order_id=(
                    f"opposite-lane-broker-noncrossing-{candidate_side}"
                ),
                idempotency_key=(
                    f"opposite-lane-local-noncrossing-{candidate_side}"
                ),
            )
        ],
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)

    if candidate_side == "buy":
        opposite_item = _family_item(
            settings=settings,
            item_id="noncrossing-signal-sell",
            index=1,
            side="sell",
            intent="target_sell",
            lane="signal-decider",
            family="noncrossing-signal-family",
            trigger_price=opposite_price,
            exchange="NXT",
        )
        candidate = _watch_item(
            settings=settings,
            item_id="noncrossing-short-buy",
            lane="short-term-decider",
            source_key="noncrossing-short-family",
            price=candidate_price,
            exchange="KRX",
        )
        command = _watch_command(candidate)
        with app.state.db_session_factory() as session:
            _seed_single_family_inventory(
                settings=settings,
                session=session,
                candidate=opposite_item,
                lane="signal-decider",
                family="noncrossing-signal-family",
                entry_exchange="NXT",
            )
            _persist_watch_plan(
                session=session,
                items=[candidate],
                suffix="noncrossing-short-buy",
            )
            _persist_active_opposite_lane_order(
                settings=settings,
                session=session,
                item=opposite_item,
                side="sell",
                price=opposite_price,
                exchange="NXT",
                suffix="noncrossing-buy",
            )
            result = broker_orders_service.place_broker_order(
                settings=settings,
                db=session,
                adapter=adapter,
                command=command,
                request_source=MONITORING_SOURCE,
            )
    else:
        candidate = _family_item(
            settings=settings,
            item_id="noncrossing-signal-sell-candidate",
            index=1,
            side="sell",
            intent="target_sell",
            lane="signal-decider",
            family="noncrossing-signal-sell-family",
            trigger_price=candidate_price,
            exchange="KRX",
        )
        command = _sell_command(candidate)
        _bind_monitoring_authority(candidate, command)
        opposite_item = _watch_item(
            settings=settings,
            item_id="noncrossing-short-buy-open",
            lane="short-term-decider",
            source_key="noncrossing-short-open-family",
            price=opposite_price,
            exchange="NXT",
        )
        opposite_item.status = "broker_order_open"
        with app.state.db_session_factory() as session:
            _seed_single_family_inventory(
                settings=settings,
                session=session,
                candidate=candidate,
                lane="signal-decider",
                family="noncrossing-signal-sell-family",
            )
            _persist_watch_plan(
                session=session,
                items=[opposite_item],
                suffix="noncrossing-short-buy-open",
            )
            _persist_active_opposite_lane_order(
                settings=settings,
                session=session,
                item=opposite_item,
                side="buy",
                price=opposite_price,
                exchange="NXT",
                suffix="noncrossing-sell",
            )
            result = broker_orders_service.place_broker_order(
                settings=settings,
                db=session,
                adapter=adapter,
                command=command,
                request_source=MONITORING_SOURCE,
            )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


@pytest.mark.parametrize(
    ("broker_order", "expected_reason"),
    (
        (
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=1,
                remaining_quantity=1,
                limit_price=None,
                broker_order_id="market-opposite-sell",
            ),
            "opposite_order_self_cross_risk",
        ),
        (
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=1,
                remaining_quantity=1,
                limit_price=Decimal("110"),
            ),
            "opposite_order_truth_indeterminate",
        ),
        (
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=1,
                remaining_quantity=-1,
                limit_price=Decimal("110"),
                broker_order_id="unknown-remaining-opposite-sell",
            ),
            "opposite_order_truth_indeterminate",
        ),
    ),
    ids=("market-price", "missing-identity", "invalid-remaining"),
)
def test_final_boundary_fails_closed_on_unsafe_opposite_broker_truth(
    tmp_path,
    broker_order: BrokerAccountOpenOrder,
    expected_reason: str,
) -> None:
    case_slug = (broker_order.broker_order_id or "identity")[:16]
    settings = _settings(
        tmp_path,
        f"unsafe-opposite-{expected_reason}-{case_slug}.db",
    )
    adapter = _adapter(open_orders=[broker_order])
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id=f"unsafe-opposite-{case_slug}",
        lane="short-term-decider",
        source_key="unsafe-opposite-candidate-family",
        price=Decimal("100"),
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[candidate],
            suffix=f"unsafe-opposite-{case_slug}",
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == expected_reason
    assert adapter.order_calls == []
    engine.dispose()


def test_final_boundary_requires_complete_opposite_broker_order_truth(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "incomplete-opposite-order-truth.db")
    adapter = _adapter(open_orders_complete=False)
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id="incomplete-opposite-truth-buy",
        lane="short-term-decider",
        source_key="incomplete-opposite-truth-family",
        price=Decimal("100"),
    )
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[candidate],
            suffix="incomplete-opposite-truth",
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "opposite_order_truth_indeterminate"
    assert result.payload["opposite_order_boundary"] == {"truth_complete": False}
    assert adapter.order_calls == []
    engine.dispose()


def test_final_boundary_blocks_crossing_local_order_missing_from_broker_snapshot(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "local-only-opposite-crossing.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    opposite_item = _family_item(
        settings=settings,
        item_id="local-only-signal-sell",
        index=1,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="local-only-signal-family",
        trigger_price=Decimal("100"),
        exchange="NXT",
    )
    candidate = _watch_item(
        settings=settings,
        item_id="local-only-short-buy",
        lane="short-term-decider",
        source_key="local-only-short-family",
        price=Decimal("100"),
        exchange="KRX",
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=opposite_item,
            lane="signal-decider",
            family="local-only-signal-family",
            entry_exchange="NXT",
        )
        _persist_watch_plan(
            session=session,
            items=[candidate],
            suffix="local-only-short-buy",
        )
        _persist_active_opposite_lane_order(
            settings=settings,
            session=session,
            item=opposite_item,
            side="sell",
            price=Decimal("100"),
            exchange="NXT",
            suffix="local-only-crossing",
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "opposite_order_self_cross_risk"
    assert result.payload["opposite_order_boundary"] == {
        "truth_complete": True,
        "candidate_side": "buy",
        "opposite_side": "sell",
        "broker_active_order_count": 0,
        "local_unmatched_active_order_count": 1,
        "crossing_order_count": 1,
        "market_or_unknown_price_count": 0,
    }
    assert adapter.order_calls == []
    engine.dispose()


def _persist_domestic_flat_boundary(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    symbol: str,
    broker_reconciled_at: datetime,
    last_local_fill_at: datetime,
    suffix: str,
) -> None:
    scope = _test_kis_execution_scope(settings, exchange="KRX", venue="kis")
    session.add(
        FifoInventoryIsolation(
            mode=settings.runtime_mode,
            provider=scope["provider"],
            account_scope=scope["account_scope"],
            venue=scope["venue"],
            asset_class=scope["asset_class"],
            exchange=scope["exchange"],
            symbol=symbol,
            residual_quantity=Decimal("5"),
            last_local_fill_at=last_local_fill_at,
            broker_position_id=f"flat-position-{suffix}",
            broker_position_quantity=Decimal("0"),
            broker_reconciled_at=broker_reconciled_at,
            isolated_through_at=broker_reconciled_at,
            evidence_source="broker_flat_reconciliation",
            actor="test",
            reason="verified domestic flat boundary",
        )
    )


def test_boundary_spanning_duplicate_fill_conflict_blocks_cross_lane_watch_buy(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "current-conflict-watch-buy.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id="current-conflict-short-buy",
        lane="short-term-decider",
        source_key="current-conflict-short-family",
    )
    now = datetime.now(UTC)
    old_at = now - timedelta(hours=3)
    flat_at = now - timedelta(hours=2)
    current_at = now - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            quantity=5,
        )
        held_order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == "held-family-filled-buy"
            )
        )
        assert held_order is not None
        original_fill = session.scalar(
            select(ExecutionFill).where(
                ExecutionFill.broker_order_pk == held_order.id,
                ExecutionFill.broker_fill_id == "fill-held-family-filled-buy",
            )
        )
        assert original_fill is not None
        original_fill.filled_at = old_at
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=held_order.id,
                    broker_fill_id=original_fill.broker_fill_id,
                    symbol=held_order.symbol,
                    side="buy",
                    quantity=4,
                    price=Decimal("100"),
                    filled_at=current_at,
                    payload={"fill_time_precision": "exact"},
                ),
                ExecutionFill(
                    broker_order_pk=held_order.id,
                    broker_fill_id="current-conflict-clean-held-fill",
                    symbol=held_order.symbol,
                    side="buy",
                    quantity=1,
                    price=Decimal("100"),
                    filled_at=current_at + timedelta(microseconds=1),
                    payload={"fill_time_precision": "exact"},
                ),
            ]
        )
        _persist_domestic_flat_boundary(
            settings=settings,
            session=session,
            symbol=held_order.symbol,
            broker_reconciled_at=flat_at,
            last_local_fill_at=old_at,
            suffix="spanning-conflict-watch-buy",
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_lane_inventory_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_boundary_spanning_duplicate_fill_conflict_blocks_exact_family_sell(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "current-conflict-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="current-conflict-family-a-sell",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate)
    _bind_monitoring_authority(candidate, command)
    now = datetime.now(UTC)
    old_at = now - timedelta(hours=3)
    flat_at = now - timedelta(hours=2)
    current_at = now - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[candidate],
            signal_fill_at=current_at,
            short_fill_at=old_at,
        )
        family_b_order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.idempotency_key == "family-b-buy")
        )
        assert family_b_order is not None
        original_fill = session.scalar(
            select(ExecutionFill).where(
                ExecutionFill.broker_order_pk == family_b_order.id,
                ExecutionFill.broker_fill_id == "fill-family-b-buy",
            )
        )
        assert original_fill is not None
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=family_b_order.id,
                    broker_fill_id=original_fill.broker_fill_id,
                    symbol=family_b_order.symbol,
                    side="buy",
                    quantity=4,
                    price=Decimal("110"),
                    filled_at=current_at + timedelta(microseconds=1),
                    payload={"fill_time_precision": "exact"},
                ),
                ExecutionFill(
                    broker_order_pk=family_b_order.id,
                    broker_fill_id="current-conflict-clean-family-b-fill",
                    symbol=family_b_order.symbol,
                    side="buy",
                    quantity=1,
                    price=Decimal("110"),
                    filled_at=current_at + timedelta(microseconds=2),
                    payload={"fill_time_precision": "exact"},
                ),
            ]
        )
        _persist_domestic_flat_boundary(
            settings=settings,
            session=session,
            symbol=family_b_order.symbol,
            broker_reconciled_at=flat_at,
            last_local_fill_at=old_at,
            suffix="spanning-conflict-family-sell",
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_ledger_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_verified_flat_isolates_old_conflict_for_cross_lane_watch_buy(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "old-conflict-flat-watch-buy.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id="old-conflict-flat-short-buy",
        lane="short-term-decider",
        source_key="old-conflict-flat-short-family",
    )
    now = datetime.now(UTC)
    old_at = now - timedelta(hours=3)
    flat_at = now - timedelta(hours=2)
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            quantity=5,
        )
        entry = session.get(ExecutionPlanItem, "held-family-entry")
        assert entry is not None
        old_order = _persist_exact_family_buy_fill(
            settings=settings,
            session=session,
            item=entry,
            exchange="NXT",
            quantity=5,
            price=Decimal("90"),
            filled_at=old_at,
            suffix="old-conflict-flat-watch-buy",
        )
        old_order.request_metadata = {
            **old_order.request_metadata,
            "metadata": {},
            "original_command": {"metadata": {}},
        }
        session.add(
            ExecutionFill(
                broker_order_pk=old_order.id,
                broker_fill_id="fill-domestic-family-buy-old-conflict-flat-watch-buy",
                symbol=entry.symbol,
                side="buy",
                quantity=4,
                price=Decimal("90"),
                filled_at=old_at,
                payload={"fill_time_precision": "exact"},
            )
        )
        _persist_domestic_flat_boundary(
            settings=settings,
            session=session,
            symbol=entry.symbol,
            broker_reconciled_at=flat_at,
            last_local_fill_at=old_at,
            suffix="old-conflict-flat-watch-buy",
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_verified_flat_isolates_old_conflict_for_exact_family_sell(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "old-conflict-flat-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="old-conflict-flat-family-a-sell",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate)
    _bind_monitoring_authority(candidate, command)
    now = datetime.now(UTC)
    old_at = now - timedelta(hours=3)
    flat_at = now - timedelta(hours=2)
    current_at = now - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[candidate],
            signal_fill_at=current_at,
            short_fill_at=current_at + timedelta(microseconds=1),
        )
        entry = session.get(ExecutionPlanItem, "family-a-entry")
        assert entry is not None
        old_order = _persist_exact_family_buy_fill(
            settings=settings,
            session=session,
            item=entry,
            exchange="NXT",
            quantity=5,
            price=Decimal("90"),
            filled_at=old_at,
            suffix="old-conflict-flat-family-sell",
        )
        old_order.request_metadata = {
            **old_order.request_metadata,
            "metadata": {},
            "original_command": {"metadata": {}},
        }
        session.add(
            ExecutionFill(
                broker_order_pk=old_order.id,
                broker_fill_id="fill-domestic-family-buy-old-conflict-flat-family-sell",
                symbol=entry.symbol,
                side="buy",
                quantity=4,
                price=Decimal("90"),
                filled_at=old_at,
                payload={"fill_time_precision": "exact"},
            )
        )
        _persist_domestic_flat_boundary(
            settings=settings,
            session=session,
            symbol=entry.symbol,
            broker_reconciled_at=flat_at,
            last_local_fill_at=old_at,
            suffix="old-conflict-flat-family-sell",
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_current_generation_legacy_identity_blocks_cross_lane_watch_buy(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "legacy-identity-current-watch-buy.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id="legacy-identity-current-short-buy",
        lane="short-term-decider",
        source_key="legacy-identity-current-short-family",
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            quantity=5,
        )
        current_order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == "held-family-filled-buy"
            )
        )
        assert current_order is not None
        current_order.request_metadata = {
            **current_order.request_metadata,
            "metadata": {},
            "original_command": {"metadata": {}},
        }
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == (
        "strategy_family_persisted_identity_mismatch"
    )
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_current_generation_legacy_identity_blocks_exact_family_sell(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "legacy-identity-current-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="legacy-current-family-a-sell",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate)
    _bind_monitoring_authority(candidate, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[candidate],
        )
        current_order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == "family-a-buy"
            )
        )
        assert current_order is not None
        current_order.request_metadata = {
            **current_order.request_metadata,
            "metadata": {},
            "original_command": {"metadata": {}},
        }
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == (
        "strategy_family_persisted_identity_mismatch"
    )
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def _monitoring_retry_command(
    item: ExecutionPlanItem,
    *,
    source_order_id: str,
    quantity: int = 5,
) -> PlaceOrderRequest:
    command = _sell_command(item)
    metadata = dict(command.metadata or {})
    metadata["retry"] = {
        "source": "rejected_order_retry",
        "source_broker_order_pk": source_order_id,
    }
    return command.model_copy(
        update={
            "idempotency_key": (
                f"monitoring:{item.intent}:{item.id}:retry:{source_order_id}"
            ),
            "quantity": quantity,
            "metadata": metadata,
        }
    )


def _seed_monitoring_retry_source(
    *,
    settings: Settings,
    session,  # noqa: ANN001
    item: ExecutionPlanItem,
    order_id: str,
    status: str = "retry_pending",
    parent_order_id: str | None = None,
    event_at: datetime | None = None,
    error_code: str = "KIOK0320",
) -> BrokerOrder:
    family_identity = broker_orders_service._strategy_entry_family_identity(item)
    assert family_identity is not None
    retry = (
        {
            "source": "rejected_order_retry",
            "source_broker_order_pk": parent_order_id,
        }
        if parent_order_id is not None
        else None
    )
    command_metadata: dict[str, object] = {
        broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
    }
    if retry is not None:
        command_metadata["retry"] = retry
    request_metadata: dict[str, object] = {
        "source": MONITORING_SOURCE,
        "exchange": "KRX",
        "execution_scope": _test_kis_execution_scope(settings, venue="kis"),
        "metadata": command_metadata,
        "original_command": {"metadata": command_metadata},
    }
    order = BrokerOrder(
        id=order_id,
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="paper",
        idempotency_key=(
            f"monitoring:{item.intent}:{item.id}:retry:{parent_order_id}"
            if parent_order_id is not None
            else f"monitoring:{item.intent}:{item.id}:initial:{order_id}"
        ),
        symbol=item.symbol,
        side="sell",
        status=status,
        quantity=5,
        venue="kis",
        asset_class="stock",
        limit_price=item.limit_price,
        request_metadata=request_metadata,
        last_error_classification="broker_rejected",
        last_error_code=error_code,
        last_error_message="market session does not accept orders",
        last_error_retryable=True,
    )
    session.add(order)
    session.flush()
    if event_at is not None:
        session.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="submit_requested",
                event_source="submission",
                normalized_status="submission_pending",
                request_payload={"metadata": {"retry": retry}} if retry else {},
                observed_at=event_at,
            )
        )
    return order


def _add_verified_session_rejection_event(
    *,
    session,  # noqa: ANN001
    order: BrokerOrder,
    observed_at: datetime,
) -> None:
    session.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submit_failed",
            event_source="submission",
            normalized_status=order.status,
            broker_status=order.last_error_code,
            error_classification=order.last_error_classification,
            retryable=True,
            payload={
                "broker_call_started": True,
                "broker_rejection_acknowledged": True,
                "manual_reconciliation_required": False,
            },
            observed_at=observed_at,
        )
    )


@pytest.mark.parametrize(
    ("failure_event", "expected_status", "expected_broker_calls"),
    [
        (None, 409, 0),
        ("mismatched-error", 409, 0),
        ("manual-reconciliation", 409, 0),
        ("verified", 201, 1),
    ],
)
def test_session_retry_release_requires_matching_immutable_failure_event(
    tmp_path,
    failure_event: str | None,
    expected_status: int,
    expected_broker_calls: int,
) -> None:
    settings = _settings(tmp_path, f"family-session-retry-{failure_event}.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-a-session-retry",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-session-retry-source",
        )
        if failure_event is not None:
            session.add(
                BrokerOrderEvent(
                    broker_order_pk=source.id,
                    event_type="submit_failed",
                    event_source="submission",
                    normalized_status=source.status,
                    broker_status=(
                        "APBK0918"
                        if failure_event == "mismatched-error"
                        else source.last_error_code
                    ),
                    error_classification=source.last_error_classification,
                    retryable=True,
                    payload={
                        "broker_call_started": True,
                        "broker_rejection_acknowledged": True,
                        "manual_reconciliation_required": (
                            failure_event == "manual-reconciliation"
                        ),
                    },
                    observed_at=datetime.now(UTC),
                )
            )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == expected_status, result.payload
    assert len(adapter.order_calls) == expected_broker_calls
    engine.dispose()


def test_retry_blocks_manual_review_when_plan_family_mutates_after_order_snapshot(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-retry-plan-tamper.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-retry-plan-tamper",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-retry-plan-tamper-source",
        )
        mutated_policy = dict(stop.quantity_policy or {})
        mutated_policy["linked_entry_item_key"] = "family-b"
        stop.quantity_policy = mutated_policy
        stop.decision_agent_name = "short-term-decider"
        session.flush()
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == (
        "strategy_family_persisted_identity_mismatch"
    )
    assert result.payload["manual_review_required"] is True
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_same_payload_family_conflict_blocks_before_broker(tmp_path) -> None:
    settings = _settings(tmp_path, "same-payload-family-conflict.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="same-payload-family-conflict",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(stop)
    _bind_monitoring_authority(stop, command)
    mutated_policy = dict(stop.quantity_policy or {})
    mutated_policy["entry_context"] = {
        "execution_plan_item_id": "family-b-entry",
        "source_plan_item_id": "family-b",
    }
    stop.quantity_policy = mutated_policy

    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_lineage_unavailable"
    assert result.payload["manual_review_required"] is True
    assert len(adapter.order_calls) == 0
    engine.dispose()


def test_conclusive_pre_submit_retry_releases_exact_family_reservation(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-pre-submit-retry.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-a-pre-submit-retry",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-pre-submit-source",
            error_code="BROKER_SESSION_UNAVAILABLE",
        )
        session.add(
            BrokerOrderEvent(
                broker_order_pk=source.id,
                event_type="submit_failed_before_broker_call",
                event_source="submission",
                normalized_status="retry_pending",
                retryable=True,
                request_payload={},
                payload={
                    "broker_call_started": False,
                    "manual_reconciliation_required": False,
                },
                observed_at=datetime(2026, 10, 26, 1, 0, tzinfo=UTC),
            )
        )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_conflicting_duplicate_fill_evidence_never_releases_retry_reservation(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-conflicting-fill-retry.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-a-conflicting-fill-retry",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-conflicting-fill-retry-source",
            error_code="BROKER_SESSION_UNAVAILABLE",
        )
        source.events.append(
            BrokerOrderEvent(
                event_type="submit_failed_before_broker_call",
                event_source="submission",
                normalized_status="retry_pending",
                retryable=True,
                request_payload={},
                payload={
                    "broker_call_started": False,
                    "manual_reconciliation_required": False,
                },
                observed_at=datetime(2026, 10, 26, 1, 0, tzinfo=UTC),
            )
        )
        for quantity in (1, 2):
            source.fills.append(
                ExecutionFill(
                    broker_fill_id="conflicting-retry-fill",
                    symbol=source.symbol,
                    side="sell",
                    quantity=quantity,
                    price=Decimal("90"),
                    filled_at=datetime(2026, 10, 26, 0, 59, tzinfo=UTC),
                    payload={"fill_time_precision": "exact"},
                )
            )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_ledger_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_conclusive_retry_releases_only_durably_linked_ancestry(tmp_path) -> None:
    settings = _settings(tmp_path, "family-session-retry-chain.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-a-session-retry-chain",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    first_event_at = datetime(2026, 10, 26, 1, 0, tzinfo=UTC)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        first = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-session-retry-first",
            event_at=first_event_at,
        )
        second = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-session-retry-second",
            parent_order_id=first.id,
            event_at=first_event_at + timedelta(minutes=1),
        )
        _add_verified_session_rejection_event(
            session=session,
            order=first,
            observed_at=first_event_at + timedelta(seconds=1),
        )
        _add_verified_session_rejection_event(
            session=session,
            order=second,
            observed_at=first_event_at + timedelta(minutes=1, seconds=1),
        )
        command = _monitoring_retry_command(
            stop,
            source_order_id=second.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_retry_ancestry_stops_before_partially_filled_parent(tmp_path) -> None:
    settings = _settings(tmp_path, "family-partial-retry-ancestor.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="family-a-partial-ancestor",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    first_event_at = datetime(2026, 10, 26, 1, 0, tzinfo=UTC)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        partial = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-partial-ancestor-source",
            status="partially_filled",
            event_at=first_event_at,
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="family-a-partial-ancestor-current",
            parent_order_id=partial.id,
            event_at=first_event_at + timedelta(minutes=1),
        )
        _add_verified_session_rejection_event(
            session=session,
            order=source,
            observed_at=first_event_at + timedelta(minutes=1, seconds=1),
        )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["family_reserved_quantity"] == 5
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    "unsafe_status",
    ("submitted", "open", "partially_filled", "manual_review_required"),
)
def test_retry_metadata_never_releases_nonterminal_or_unknown_family_reservation(
    tmp_path,
    unsafe_status: str,
) -> None:
    settings = _settings(tmp_path, f"unsafe-family-retry-{unsafe_status}.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id=f"unsafe-retry-{unsafe_status[:12]}",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id=f"unsafe-family-retry-source-{unsafe_status}",
            status=unsafe_status,
        )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] in {
        "strategy_family_quantity_unavailable",
        "unresolved_prior_submission",
    }
    assert adapter.order_calls == []
    engine.dispose()


def test_retry_does_not_release_unrelated_same_family_reservation(tmp_path) -> None:
    settings = _settings(tmp_path, "unrelated-family-retry.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stop = _family_item(
        settings=settings,
        item_id="unrelated-family-retry",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    event_at = datetime(2026, 10, 26, 1, 0, tzinfo=UTC)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stop],
        )
        _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="unrelated-family-retry-reservation",
            event_at=event_at,
        )
        source = _seed_monitoring_retry_source(
            settings=settings,
            session=session,
            item=stop,
            order_id="unrelated-family-retry-source",
            event_at=event_at + timedelta(minutes=1),
        )
        _add_verified_session_rejection_event(
            session=session,
            order=source,
            observed_at=event_at + timedelta(minutes=1, seconds=1),
        )
        command = _monitoring_retry_command(
            stop,
            source_order_id=source.id,
        )
        _bind_monitoring_authority(stop, command)
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["family_reserved_quantity"] == 5
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("mutate_family_after_snapshot", "expected_reason", "manual_review"),
    (
        (False, "strategy_family_quantity_unavailable", False),
        (True, "retry_lineage_authority_invalid", True),
    ),
)
def test_execution_job_retry_cannot_borrow_another_family_inventory(
    tmp_path,
    mutate_family_after_snapshot: bool,
    expected_reason: str,
    manual_review: bool,
) -> None:
    settings = _settings(
        tmp_path,
        f"execution-job-family-retry-{mutate_family_after_snapshot}.db",
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("110"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="family-a-execution-job-retry",
        index=1,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            lane="short-term-decider",
            family="family-b",
            quantity=5,
        )
        candidate.status = "broker_order_retry_pending"
        source_command = _sell_command(candidate)
        _bind_monitoring_authority(candidate, source_command)
        family_identity = broker_orders_service._strategy_entry_family_identity(
            candidate
        )
        assert family_identity is not None
        command_metadata = dict(source_command.metadata or {})
        execution_scope = _test_kis_execution_scope(settings, venue="kis")
        source_order = BrokerOrder(
            id="family-a-execution-job-retry-source",
            execution_plan_item_id=candidate.id,
            broker_name="kis",
            mode="paper",
            idempotency_key=source_command.idempotency_key,
            broker_order_id="broker-family-a-execution-job-retry-source",
            symbol=candidate.symbol,
            side="sell",
            status="retry_pending",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("90"),
            submitted_at=datetime.now(UTC) - timedelta(minutes=2),
            request_metadata={
                "source": MONITORING_SOURCE,
                "provider": "kis",
                "venue": "kis",
                "asset_class": "stock",
                "exchange": "KRX",
                "time_in_force": "day",
                "sizing": {
                    "quantity": 5,
                    "base_quantity": None,
                    "quote_notional": None,
                    "limit_price": "90",
                    "quantity_precision": None,
                    "price_precision": None,
                },
                "execution_scope": execution_scope,
                "metadata": command_metadata,
                "original_command": source_command.model_dump(mode="json"),
            },
            last_error_classification="broker_rejected",
            last_error_code="KIOK0320",
            last_error_message="market session does not accept orders",
            last_error_retryable=True,
        )
        source_order.events.extend(
            [
                BrokerOrderEvent(
                    event_type="submit_requested",
                    event_source="submission",
                    normalized_status="submission_pending",
                    request_payload=source_command.model_dump(mode="json"),
                    observed_at=datetime.now(UTC) - timedelta(minutes=2),
                ),
                BrokerOrderEvent(
                    event_type="submit_failed",
                    event_source="submission",
                    normalized_status="retry_pending",
                    broker_status="KIOK0320",
                    error_classification="broker_rejected",
                    retryable=True,
                    payload={
                        "broker_call_started": True,
                        "broker_rejection_acknowledged": True,
                        "manual_reconciliation_required": False,
                    },
                    observed_at=datetime.now(UTC) - timedelta(minutes=1),
                ),
            ]
        )
        retry_job = ExecutionJob(
            id="family-a-execution-job-retry-job",
            execution_plan_item_id=candidate.id,
            job_type="broker_order_retry",
            status="running",
            scheduled_for=datetime.now(UTC) - timedelta(seconds=1),
            locked_at=datetime.now(UTC),
        )
        session.add_all([source_order, retry_job])
        session.flush()
        failure = ExecutionFailure(
            mode="paper",
            broker_order_pk=source_order.id,
            execution_plan_item_id=candidate.id,
            broker_name="kis",
            idempotency_key=source_order.idempotency_key,
            symbol=candidate.symbol,
            side="sell",
            intent=candidate.intent,
            requested_quantity=5,
            requested_limit_price=Decimal("90"),
            classification="temporary_market_state",
            broker_error_code="KIOK0320",
            broker_error_message="market session does not accept orders",
            retryable=True,
            status="open",
            first_failed_at=datetime.now(UTC) - timedelta(minutes=1),
            latest_failed_at=datetime.now(UTC) - timedelta(minutes=1),
            retry_decision_status="scheduled",
            retry_decision_reason="temporary_orderability_window",
            retry_next_at=retry_job.scheduled_for,
            retry_deadline_at=datetime.now(UTC) + timedelta(hours=1),
            retry_max_attempts=2,
            retry_attempt_count=0,
            retry_job_id=retry_job.id,
        )
        session.add(failure)
        if mutate_family_after_snapshot:
            mutated_policy = dict(candidate.quantity_policy or {})
            mutated_policy["linked_entry_item_key"] = "family-b-mutated"
            candidate.quantity_policy = mutated_policy
            candidate.decision_agent_name = "short-term-decider"
        session.commit()

        retry_command = build_rejected_order_retry_request(
            settings=settings,
            db=session,
            adapter=adapter,
            job=retry_job,
            triggered_at=datetime.now(UTC),
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=retry_command,
            request_source=f"execution-job:{retry_job.id}",
        )
        if mutate_family_after_snapshot:
            session.refresh(source_order)
            session.refresh(failure)
            session.refresh(candidate)
            assert source_order.status == "manual_review_required"
            assert failure.retry_decision_status == "manual_review_required"
            assert candidate.status == "manual_review_required"

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == expected_reason
    assert bool(
        result.payload.get("manual_review_required")
        or result.payload.get("manual_reconciliation_required")
    ) is manual_review
    assert adapter.order_calls == []
    engine.dispose()


def test_second_execution_job_retry_releases_every_proven_non_exposed_ancestor(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "second-execution-job-family-retry.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="second-execution-job-family-retry",
        index=1,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    root_observed_at = datetime(2026, 10, 26, 1, 0, tzinfo=UTC)
    with app.state.db_session_factory() as session:
        _seed_single_family_inventory(
            settings=settings,
            session=session,
            candidate=candidate,
            lane="signal-decider",
            family="family-a",
            quantity=5,
        )
        candidate.status = "broker_order_retry_pending"
        root_command = _sell_command(candidate)
        _bind_monitoring_authority(candidate, root_command)
        root_order = BrokerOrder(
            id="second-execution-job-root-order",
            execution_plan_item_id=candidate.id,
            broker_name="kis",
            mode="paper",
            idempotency_key=root_command.idempotency_key,
            symbol=candidate.symbol,
            side="sell",
            status="retry_pending",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("90"),
            request_metadata={
                "source": MONITORING_SOURCE,
                "provider": "kis",
                "venue": "kis",
                "asset_class": "stock",
                "exchange": "KRX",
                "time_in_force": "day",
                "sizing": {
                    "quantity": 5,
                    "base_quantity": None,
                    "quote_notional": None,
                    "limit_price": "90",
                    "quantity_precision": None,
                    "price_precision": None,
                },
                "execution_scope": _test_kis_execution_scope(
                    settings,
                    venue="kis",
                ),
                "metadata": dict(root_command.metadata or {}),
                "original_command": root_command.model_dump(mode="json"),
            },
            last_error_classification="broker_rejected",
            last_error_code="KIOK0320",
            last_error_message="market session does not accept orders",
            last_error_retryable=True,
        )
        root_order.events.extend(
            [
                BrokerOrderEvent(
                    event_type="submit_requested",
                    event_source="submission",
                    normalized_status="submission_pending",
                    request_payload=root_command.model_dump(mode="json"),
                    observed_at=root_observed_at,
                ),
                BrokerOrderEvent(
                    event_type="submit_failed",
                    event_source="submission",
                    normalized_status="retry_pending",
                    broker_status="KIOK0320",
                    error_classification="broker_rejected",
                    retryable=True,
                    payload={
                        "broker_call_started": True,
                        "broker_rejection_acknowledged": True,
                        "manual_reconciliation_required": False,
                    },
                    observed_at=root_observed_at + timedelta(seconds=1),
                ),
            ]
        )
        first_job = ExecutionJob(
            id="second-execution-job-retry-one",
            execution_plan_item_id=candidate.id,
            job_type="broker_order_retry",
            status="running",
            scheduled_for=root_observed_at + timedelta(minutes=1),
            locked_at=root_observed_at + timedelta(minutes=1),
        )
        session.add_all([root_order, first_job])
        session.flush()
        first_failure = ExecutionFailure(
            mode="paper",
            broker_order_pk=root_order.id,
            execution_plan_item_id=candidate.id,
            broker_name="kis",
            idempotency_key=root_order.idempotency_key,
            symbol=candidate.symbol,
            side="sell",
            intent=candidate.intent,
            requested_quantity=5,
            requested_limit_price=Decimal("90"),
            classification="temporary_market_state",
            broker_error_code="KIOK0320",
            broker_error_message="market session does not accept orders",
            retryable=True,
            status="open",
            first_failed_at=root_observed_at,
            latest_failed_at=root_observed_at,
            retry_decision_status="scheduled",
            retry_decision_reason="temporary_orderability_window",
            retry_next_at=first_job.scheduled_for,
            retry_deadline_at=root_observed_at + timedelta(hours=1),
            retry_max_attempts=3,
            retry_attempt_count=0,
            retry_job_id=first_job.id,
        )
        session.add(first_failure)
        session.commit()

        first_retry_command = build_rejected_order_retry_request(
            settings=settings,
            db=session,
            adapter=adapter,
            job=first_job,
            triggered_at=root_observed_at + timedelta(minutes=1),
        )
        adapter.order_error = BrokerAdapterError(
            "market session does not accept orders",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            retryable=True,
            code="KIOK0320",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={
                "rt_cd": "1",
                "msg_cd": "KIOK0320",
                "msg1": "market session does not accept orders",
            },
            observed_at=root_observed_at + timedelta(minutes=1, seconds=1),
        )
        first_result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=first_retry_command,
            request_source=f"execution-job:{first_job.id}",
            now=root_observed_at + timedelta(minutes=1),
        )
        assert first_result.status_code == 422, first_result.payload
        first_retry_order = session.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key
                == first_retry_command.idempotency_key
            )
        )
        assert first_retry_order is not None
        assert first_retry_order.status == "rejected"

        # ExecutionJob has one durable row per plan item/type.  A later retry
        # generation can be recreated only after the prior terminal lease is
        # retired; the already-persisted retry order must still retain that
        # immutable ancestry after the job row is gone.
        first_failure.retry_job_id = None
        session.delete(first_job)
        session.commit()

        second_job = ExecutionJob(
            id="second-execution-job-retry-two",
            execution_plan_item_id=candidate.id,
            job_type="broker_order_retry",
            status="running",
            scheduled_for=root_observed_at + timedelta(minutes=2),
            locked_at=root_observed_at + timedelta(minutes=2),
        )
        session.add(second_job)
        session.flush()
        second_failure = session.scalar(
            select(ExecutionFailure).where(
                ExecutionFailure.broker_order_pk == first_retry_order.id
            )
        )
        if second_failure is None:
            second_failure = ExecutionFailure(
                mode="paper",
                broker_order_pk=first_retry_order.id,
                execution_plan_item_id=candidate.id,
                broker_name="kis",
                idempotency_key=first_retry_order.idempotency_key,
                symbol=candidate.symbol,
                side="sell",
                intent=candidate.intent,
                requested_quantity=5,
                requested_limit_price=Decimal("90"),
                classification="temporary_market_state",
                broker_error_code="KIOK0320",
                broker_error_message="market session does not accept orders",
                retryable=True,
                status="open",
                first_failed_at=root_observed_at + timedelta(minutes=1),
                latest_failed_at=root_observed_at + timedelta(minutes=1),
            )
            session.add(second_failure)
            session.flush()
        second_failure.retry_decision_status = "scheduled"
        second_failure.retry_decision_reason = "temporary_orderability_window"
        second_failure.retry_next_at = second_job.scheduled_for
        second_failure.retry_deadline_at = root_observed_at + timedelta(hours=1)
        second_failure.retry_max_attempts = 3
        second_failure.retry_attempt_count = 1
        second_failure.retry_job_id = second_job.id
        candidate.status = "broker_order_retry_pending"
        session.commit()

        second_retry_command = build_rejected_order_retry_request(
            settings=settings,
            db=session,
            adapter=adapter,
            job=second_job,
            triggered_at=root_observed_at + timedelta(minutes=2),
        )
        adapter.order_error = None
        second_result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=second_retry_command,
            request_source=f"execution-job:{second_job.id}",
            now=root_observed_at + timedelta(minutes=2),
        )

    assert second_result.status_code == 201, second_result.payload
    assert len(adapter.order_calls) == 2
    engine.dispose()


@pytest.mark.parametrize(
    ("entry_exchange", "exit_exchange"),
    (("NXT", "KRX"), ("KRX", "NXT")),
)
def test_domestic_cross_route_buy_can_fund_its_exact_family_stop(
    tmp_path,
    entry_exchange: str,
    exit_exchange: str,
) -> None:
    settings = _settings(
        tmp_path,
        f"domestic-family-{entry_exchange}-{exit_exchange}.db",
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    entry = _family_item(
        settings=settings,
        item_id=f"domestic-family-entry-{entry_exchange.lower()}",
        index=0,
        side="buy",
        intent="watch_buy",
        lane="signal-decider",
        family="domestic-cross-route-family",
        exchange=entry_exchange,
    )
    stop = _family_item(
        settings=settings,
        item_id=f"domestic-family-stop-{exit_exchange.lower()}",
        index=1,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="domestic-cross-route-family",
        trigger_price=Decimal("90"),
        exchange=exit_exchange,
    )
    command = _sell_command(stop)
    _bind_monitoring_authority(stop, command)
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id=(
                    f"domestic-family-{entry_exchange}-{exit_exchange}"
                ),
                source="kai-trading-core",
                market_date=date(2026, 10, 26),
                mode="paper",
                status="accepted",
                items=[entry, stop],
            )
        )
        session.flush()
        _persist_exact_family_buy_fill(
            settings=settings,
            session=session,
            item=entry,
            exchange=entry_exchange,
            quantity=5,
            price=Decimal("100"),
            filled_at=datetime.now(UTC) - timedelta(minutes=1),
            suffix=f"{entry_exchange.lower()}-{exit_exchange.lower()}",
        )
        session.commit()
        persisted = session.get(ExecutionPlanItem, stop.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_split_domestic_route_fills_reconcile_to_one_family_inventory(tmp_path) -> None:
    settings = _settings(tmp_path, "split-domestic-family-fills.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("106"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    krx_entry = _family_item(
        settings=settings,
        item_id="split-domestic-krx-entry",
        index=0,
        side="buy",
        intent="watch_buy",
        lane="signal-decider",
        family="split-domestic-family",
        exchange="KRX",
    )
    nxt_entry = _family_item(
        settings=settings,
        item_id="split-domestic-nxt-entry",
        index=1,
        side="buy",
        intent="watch_buy",
        lane="signal-decider",
        family="split-domestic-family",
        exchange="NXT",
    )
    stop = _family_item(
        settings=settings,
        item_id="split-domestic-sor-stop",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="split-domestic-family",
        trigger_price=Decimal("90"),
        exchange="SOR",
    )
    command = _sell_command(stop)
    _bind_monitoring_authority(stop, command)
    fill_base = datetime.now(UTC) - timedelta(minutes=2)
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="split-domestic-family-fills",
                source="kai-trading-core",
                market_date=date(2026, 10, 26),
                mode="paper",
                status="accepted",
                items=[krx_entry, nxt_entry, stop],
            )
        )
        session.flush()
        _persist_exact_family_buy_fill(
            settings=settings,
            session=session,
            item=krx_entry,
            exchange="KRX",
            quantity=2,
            price=Decimal("100"),
            filled_at=fill_base,
            suffix="split-krx",
        )
        _persist_exact_family_buy_fill(
            settings=settings,
            session=session,
            item=nxt_entry,
            exchange="NXT",
            quantity=3,
            price=Decimal("110"),
            filled_at=fill_base + timedelta(microseconds=1),
            suffix="split-nxt",
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_same_family_parallel_exits_cannot_consume_other_family_inventory(tmp_path) -> None:
    settings = _settings(tmp_path, "same-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    target = _family_item(
        settings=settings,
        item_id="family-a-target",
        index=2,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
    )
    stop = _family_item(
        settings=settings,
        item_id="family-a-stop",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    target_command = _sell_command(target)
    stop_command = _sell_command(stop)
    _bind_monitoring_authority(target, target_command)
    _bind_monitoring_authority(stop, stop_command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[target, stop],
        )

    results: list[broker_orders_service.PlaceBrokerOrderResult] = []

    def submit(item_id: str, command: PlaceOrderRequest) -> None:
        with app.state.db_session_factory() as session:
            results.append(
                broker_orders_service.place_broker_order(
                    settings=settings,
                    db=session,
                    adapter=adapter,
                    command=command,
                    request_source=MONITORING_SOURCE,
                )
            )

    threads = [
        Thread(target=submit, args=(target.id, target_command)),
        Thread(target=submit, args=(stop.id, stop_command)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status_code for result in results) == [201, 409]
    blocked = next(result for result in results if result.status_code == 409)
    assert blocked.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert blocked.payload["manual_review_required"] is False
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_final_family_sell_boundary_rechecks_inventory_after_intent_commit(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "final-family-sell-recheck.db")
    initial_snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("100000000"),
        buying_power=Decimal("100000000"),
        observed_at=datetime.now(UTC),
        source="stub:final-family-sell-initial",
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ],
        open_orders=[],
        holdings_complete=True,
        open_orders_complete=True,
        total_asset_value=Decimal("100000000"),
        previous_total_asset_value=Decimal("100000000"),
        asset_change_amount=Decimal("0"),
        unrealized_pnl=Decimal("0"),
    )
    final_snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("100000000"),
        buying_power=Decimal("100000000"),
        observed_at=datetime.now(UTC),
        source="stub:final-family-sell-current",
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("110"),
            )
        ],
        open_orders=[],
        holdings_complete=True,
        open_orders_complete=True,
        total_asset_value=Decimal("100000000"),
        previous_total_asset_value=Decimal("100000000"),
        asset_change_amount=Decimal("0"),
        unrealized_pnl=Decimal("0"),
    )

    class InventoryRaceAdapter(StubBrokerAdapter):
        before_final_snapshot = None

        def get_account_snapshot(
            self,
            access_token: str,
            **_kwargs: object,
        ) -> BrokerAccountSnapshot:
            self.account_snapshot_calls += 1
            assert access_token
            if self.account_snapshot_calls == 2:
                callback = self.before_final_snapshot
                assert callable(callback)
                callback()
                return final_snapshot
            return initial_snapshot

    adapter = InventoryRaceAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(
            broker_order_id="must-not-submit-stale-family-sell"
        ),
        account_snapshot=initial_snapshot,
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="family-a-final-recheck-candidate",
        index=2,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
    )
    intervening_exit = _family_item(
        settings=settings,
        item_id="family-a-final-recheck-intervening",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate)
    _bind_monitoring_authority(candidate, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[candidate, intervening_exit],
        )

    def consume_family_a_during_snapshot_window() -> None:
        with app.state.db_session_factory() as concurrent:
            persisted_exit = concurrent.get(
                ExecutionPlanItem,
                intervening_exit.id,
            )
            assert persisted_exit is not None
            _persist_exact_family_sell_fill(
                settings=settings,
                session=concurrent,
                item=persisted_exit,
                exchange="KRX",
                quantity=5,
                price=Decimal("90"),
                filled_at=datetime.now(UTC),
                suffix="final-recheck-race",
            )
            concurrent.commit()

    adapter.before_final_snapshot = consume_family_a_during_snapshot_window
    with app.state.db_session_factory() as session:
        persisted = session.get(ExecutionPlanItem, candidate.id)
        assert persisted is not None
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert adapter.account_snapshot_calls == 2
    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["manual_review_required"] is False
    assert result.payload["strategy_family_inventory"][
        "family_available_quantity"
    ] == 0
    assert adapter.order_calls == []
    engine.dispose()


def test_cross_family_parallel_exits_keep_independent_inventory(tmp_path) -> None:
    settings = _settings(tmp_path, "cross-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    signal_exit = _family_item(
        settings=settings,
        item_id="family-a-parallel-exit",
        index=2,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
    )
    short_exit = _family_item(
        settings=settings,
        item_id="family-b-parallel-exit",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="short-term-decider",
        family="family-b",
        trigger_price=Decimal("90"),
    )
    commands = {
        item.id: _sell_command(item)
        for item in (signal_exit, short_exit)
    }
    for item in (signal_exit, short_exit):
        _bind_monitoring_authority(item, commands[item.id])
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[signal_exit, short_exit],
        )

    results: list[broker_orders_service.PlaceBrokerOrderResult] = []

    def submit(item_id: str) -> None:
        with app.state.db_session_factory() as session:
            results.append(
                broker_orders_service.place_broker_order(
                    settings=settings,
                    db=session,
                    adapter=adapter,
                    command=commands[item_id],
                    request_source=MONITORING_SOURCE,
                )
            )

    threads = [
        Thread(target=submit, args=(item.id,))
        for item in (signal_exit, short_exit)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status_code for result in results) == [201, 201], [
        result.payload for result in results
    ]
    assert len(adapter.order_calls) == 2
    engine.dispose()


def test_historical_cross_family_overdraw_blocks_every_later_family_exit(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "historical-family-overdraw.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _family_item(
        settings=settings,
        item_id="family-a-after-cross-family-overdraw",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    historical_cross_sell = _family_item(
        settings=settings,
        item_id="family-b-cross-family-sell",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="short-term-decider",
        family="family-b",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate)
    _bind_monitoring_authority(candidate, command)
    fill_base = datetime.now(UTC) - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[historical_cross_sell, candidate],
            signal_fill_at=fill_base,
            short_fill_at=fill_base + timedelta(minutes=2),
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(
            historical_cross_sell
        )
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        historical_order = BrokerOrder(
            execution_plan_item_id=historical_cross_sell.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="family-b-historical-cross-sell",
            broker_order_id="broker-family-b-historical-cross-sell",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("90"),
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _test_kis_execution_scope(
                    settings,
                    venue="kis",
                ),
                "metadata": command_metadata,
                "original_command": {"metadata": command_metadata},
            },
        )
        session.add(historical_order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=historical_order.id,
                broker_fill_id="fill-family-b-historical-cross-sell",
                symbol="005930",
                side="sell",
                quantity=5,
                price=Decimal("90"),
                filled_at=fill_base + timedelta(minutes=1),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_ledger_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_historical_cross_family_overdraw_blocks_later_watch_buy(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "historical-family-overdraw-buy.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
                payload={"current_price": "100"},
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate = _watch_item(
        settings=settings,
        item_id="family-c-after-cross-family-overdraw",
        lane="short-term-decider",
        source_key="family-c",
    )
    candidate.plan_item_index = 3
    historical_cross_sell = _family_item(
        settings=settings,
        item_id="family-b-cross-family-sell-before-buy",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="short-term-decider",
        family="family-b",
        trigger_price=Decimal("90"),
    )
    fill_base = datetime.now(UTC) - timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[historical_cross_sell, candidate],
            signal_fill_at=fill_base,
            short_fill_at=fill_base + timedelta(minutes=2),
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(
            historical_cross_sell
        )
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        historical_order = BrokerOrder(
            execution_plan_item_id=historical_cross_sell.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="family-b-historical-cross-sell-before-buy",
            broker_order_id="broker-family-b-historical-cross-sell-before-buy",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=5,
            venue="kis",
            asset_class="stock",
            limit_price=Decimal("90"),
            request_metadata={
                "exchange": "KRX",
                "execution_scope": _test_kis_execution_scope(
                    settings,
                    venue="kis",
                ),
                "metadata": command_metadata,
                "original_command": {"metadata": command_metadata},
            },
        )
        session.add(historical_order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=historical_order.id,
                broker_fill_id="fill-family-b-historical-cross-sell-before-buy",
                symbol="005930",
                side="sell",
                quantity=5,
                price=Decimal("90"),
                filled_at=fill_base + timedelta(minutes=1),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()
        failure = broker_orders_service._automated_buy_lane_inventory_failure(
            settings=settings,
            db=session,
            adapter=adapter,
            command=_watch_command(candidate),
            plan_item=candidate,
            request_source="execution-job:historical-family-overdraw-buy",
            execution_scope=_test_kis_execution_scope(settings, venue="kis"),
            account_snapshot=adapter.get_account_snapshot(
                "paper-token",
                symbols=[candidate.symbol],
                exchange="KRX",
            ),
            account_snapshot_error=None,
        )

    assert failure is not None
    assert failure.block_reason == "strategy_lane_inventory_indeterminate"
    assert failure.manual_review_required is True
    assert adapter.order_calls == []
    engine.dispose()


def test_broker_open_sell_reserves_its_exact_family_before_new_exit(tmp_path) -> None:
    settings = _settings(tmp_path, "broker-open-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=5,
                average_price=Decimal("105"),
            )
        ],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=5,
                remaining_quantity=5,
                limit_price=Decimal("120"),
                broker_order_id="broker-family-a-open-sell",
                idempotency_key="family-a-open-sell",
            )
        ],
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    existing_exit = _family_item(
        settings=settings,
        item_id="family-a-existing-open-exit",
        index=2,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
    )
    candidate_exit = _family_item(
        settings=settings,
        item_id="family-a-candidate-exit",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate_exit)
    _bind_monitoring_authority(candidate_exit, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[existing_exit, candidate_exit],
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(
            existing_exit
        )
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        session.add(
            BrokerOrder(
                execution_plan_item_id=existing_exit.id,
                broker_name="kis",
                mode="paper",
                idempotency_key="family-a-open-sell",
                broker_order_id="broker-family-a-open-sell",
                symbol="005930",
                side="sell",
                status="open",
                quantity=5,
                venue="kis",
                asset_class="stock",
                limit_price=Decimal("120"),
                request_metadata={
                    "exchange": "KRX",
                    "execution_scope": _test_kis_execution_scope(
                        settings,
                        venue="kis",
                    ),
                    "metadata": command_metadata,
                    "original_command": {"metadata": command_metadata},
                },
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["family_reserved_quantity"] == 5
    assert result.payload["family_available_quantity"] == 0
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("tamper_persisted_route", "expected_reason"),
    (
        (False, "strategy_family_quantity_unavailable"),
        (True, "strategy_family_reservation_indeterminate"),
    ),
)
def test_domestic_cross_route_broker_open_sell_reservation_requires_valid_exact_route(
    tmp_path,
    tamper_persisted_route: bool,
    expected_reason: str,
) -> None:
    settings = _settings(
        tmp_path,
        f"domestic-open-sell-{tamper_persisted_route}.db",
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=5,
                average_price=Decimal("105"),
            )
        ],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=5,
                remaining_quantity=5,
                limit_price=Decimal("120"),
                broker_order_id="broker-domestic-nxt-open-sell",
                idempotency_key="domestic-nxt-open-sell",
            )
        ],
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    existing_exit = _family_item(
        settings=settings,
        item_id=f"domestic-nxt-open-exit-{tamper_persisted_route}",
        index=2,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
        exchange="NXT",
    )
    candidate_exit = _family_item(
        settings=settings,
        item_id=f"domestic-krx-candidate-exit-{tamper_persisted_route}",
        index=3,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
        exchange="KRX",
    )
    command = _sell_command(candidate_exit)
    _bind_monitoring_authority(candidate_exit, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[existing_exit, candidate_exit],
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(
            existing_exit
        )
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity,
        }
        session.add(
            BrokerOrder(
                execution_plan_item_id=existing_exit.id,
                broker_name="kis",
                mode="paper",
                idempotency_key="domestic-nxt-open-sell",
                broker_order_id="broker-domestic-nxt-open-sell",
                symbol="005930",
                side="sell",
                status="open",
                quantity=5,
                venue="kis",
                asset_class="stock",
                limit_price=Decimal("120"),
                request_metadata={
                    "exchange": "KRX" if tamper_persisted_route else "NXT",
                    "execution_scope": _test_kis_execution_scope(
                        settings,
                        exchange="NXT",
                        venue="kis",
                    ),
                    "metadata": command_metadata,
                    "original_command": {"metadata": command_metadata},
                },
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == expected_reason
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("tamper_persisted_route", "expected_reason"),
    (
        (False, "insufficient_unreserved_position"),
        (True, "order_reservation_scope_indeterminate"),
    ),
)
def test_domestic_local_sell_reservation_spans_routes_only_with_exact_route_proof(
    tmp_path,
    tamper_persisted_route: bool,
    expected_reason: str,
) -> None:
    settings = _settings(
        tmp_path,
        f"domestic-local-sell-{tamper_persisted_route}.db",
    )
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    with app.state.db_session_factory() as session:
        session.add(
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key=f"domestic-local-nxt-{tamper_persisted_route}",
                broker_order_id=f"broker-local-nxt-{tamper_persisted_route}",
                symbol="005930",
                side="sell",
                status="open",
                quantity=5,
                venue="kis",
                asset_class="stock",
                limit_price=Decimal("110"),
                request_metadata={
                    "exchange": "KRX" if tamper_persisted_route else "NXT",
                    "execution_scope": _test_kis_execution_scope(
                        settings,
                        exchange="NXT",
                        venue="kis",
                    ),
                },
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=PlaceOrderRequest(
                idempotency_key=f"domestic-local-krx-{tamper_persisted_route}",
                provider="kis",
                venue="kis",
                asset_class="stock",
                symbol="005930",
                side="sell",
                quantity=1,
                order_type="limit",
                limit_price=Decimal("90"),
                time_in_force="day",
                exchange="KRX",
            ),
            request_source="post:/orders",
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == expected_reason
    assert adapter.order_calls == []
    engine.dispose()


def test_unmatched_broker_open_sell_requires_manual_family_review(tmp_path) -> None:
    settings = _settings(tmp_path, "unmatched-broker-open-family-sell.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=5,
                average_price=Decimal("105"),
            )
        ],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="sell",
                quantity=5,
                remaining_quantity=5,
                limit_price=Decimal("120"),
                broker_order_id="unmatched-broker-family-sell",
            )
        ],
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    candidate_exit = _family_item(
        settings=settings,
        item_id="unmatched-broker-open-exit",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(candidate_exit)
    _bind_monitoring_authority(candidate_exit, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[candidate_exit],
        )
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_reservation_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_flat_isolation_prevents_stale_family_from_borrowing_current_inventory(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "flat-isolated-family-ledger.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("110"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stale_exit = _family_item(
        settings=settings,
        item_id="flat-isolated-family-a-exit",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(stale_exit)
    _bind_monitoring_authority(stale_exit, command)
    stale_fill_at = datetime.now(UTC) - timedelta(hours=3)
    flat_at = stale_fill_at + timedelta(hours=1)
    current_fill_at = flat_at + timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stale_exit],
            signal_fill_at=stale_fill_at,
            short_fill_at=current_fill_at,
        )
        scope = _test_kis_execution_scope(settings, venue="kis")
        session.add(
            FifoInventoryIsolation(
                mode="paper",
                provider=scope["provider"],
                account_scope=scope["account_scope"],
                venue=scope["venue"],
                asset_class=scope["asset_class"],
                exchange=scope["exchange"],
                symbol="005930",
                residual_quantity=Decimal("0"),
                last_local_fill_at=stale_fill_at,
                broker_position_id="flat-isolated-family-position",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=flat_at,
                isolated_through_at=flat_at,
                evidence_source="broker_flat_reconciliation",
                actor="test",
                reason="verified exact-scope flat boundary",
                audit_payload={"test": True},
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["family_filled_quantity"] == 0
    assert result.payload["family_available_quantity"] == 0
    assert adapter.order_calls == []
    engine.dispose()


def test_domestic_flat_proof_isolates_stale_inventory_across_order_routes(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "domestic-flat-cross-route.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("110"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    stale_exit = _family_item(
        settings=settings,
        item_id="domestic-flat-stale-sor-exit",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
        exchange="SOR",
    )
    command = _sell_command(stale_exit)
    _bind_monitoring_authority(stale_exit, command)
    stale_fill_at = datetime.now(UTC) - timedelta(hours=3)
    flat_at = stale_fill_at + timedelta(hours=1)
    current_fill_at = flat_at + timedelta(hours=1)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[stale_exit],
            signal_fill_at=stale_fill_at,
            short_fill_at=current_fill_at,
        )
        stale_order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.idempotency_key == "family-a-buy")
        )
        assert stale_order is not None
        stale_metadata = dict(stale_order.request_metadata or {})
        stale_metadata["exchange"] = "NXT"
        stale_metadata["execution_scope"] = _test_kis_execution_scope(
            settings,
            exchange="NXT",
            venue="kis",
        )
        stale_order.request_metadata = stale_metadata
        scope = _test_kis_execution_scope(
            settings,
            exchange="KRX",
            venue="kis",
        )
        session.add(
            FifoInventoryIsolation(
                mode="paper",
                provider=scope["provider"],
                account_scope=scope["account_scope"],
                venue=scope["venue"],
                asset_class=scope["asset_class"],
                exchange=scope["exchange"],
                symbol="005930",
                residual_quantity=Decimal("0"),
                last_local_fill_at=stale_fill_at,
                broker_position_id="domestic-flat-cross-route-position",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=flat_at,
                isolated_through_at=flat_at,
                evidence_source="broker_flat_reconciliation",
                actor="test",
                reason="verified domestic account flat boundary",
                audit_payload={"test": True},
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_quantity_unavailable"
    assert result.payload["family_filled_quantity"] == 0
    assert result.payload["family_available_quantity"] == 0
    assert adapter.order_calls == []
    engine.dispose()


def test_filled_inventory_cannot_move_family_after_plan_row_mutation(tmp_path) -> None:
    settings = _settings(tmp_path, "filled-family-plan-mutation.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    exit_item = _family_item(
        settings=settings,
        item_id="filled-family-plan-mutation-exit",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(exit_item)
    _bind_monitoring_authority(exit_item, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[exit_item],
        )
        mutated_entry = session.get(ExecutionPlanItem, "family-a-entry")
        assert mutated_entry is not None
        mutated_entry.source_plan_item_id = "family-b-mutated"
        mutated_entry.decision_agent_name = "short-term-decider"
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == (
        "strategy_family_persisted_identity_mismatch"
    )
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


def test_post_boundary_observation_bounded_fill_requires_manual_review(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "imprecise-family-fill.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=10,
                sellable_quantity=10,
                average_price=Decimal("105"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    exit_item = _family_item(
        settings=settings,
        item_id="imprecise-family-fill-exit",
        index=2,
        side="sell",
        intent="stop_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("90"),
    )
    command = _sell_command(exit_item)
    _bind_monitoring_authority(exit_item, command)
    with app.state.db_session_factory() as session:
        _seed_two_family_inventory(
            settings=settings,
            session=session,
            exit_items=[exit_item],
        )
        imprecise_fill = session.scalar(
            select(ExecutionFill).where(
                ExecutionFill.broker_fill_id == "fill-family-b-buy"
            )
        )
        assert imprecise_fill is not None
        imprecise_fill.payload = {
            "fill_time_precision": "observed_upper_bound",
            "cumulative_quantity": "5",
        }
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409, result.payload
    assert result.payload["block_reason"] == "strategy_family_ledger_indeterminate"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize(
    ("intent", "status", "limit_price"),
    (
        ("target_sell", "target_sell_triggered_pending_dispatch", Decimal("120")),
        ("stop_sell", "stop_sell_triggered_pending_dispatch", Decimal("90")),
        (
            "max_holding_close_sell",
            "max_holding_close_sell_triggered_pending_dispatch",
            Decimal("100"),
        ),
    ),
)
def test_unlinked_automated_exit_is_manual_review_without_broker_call(
    tmp_path,
    intent: str,
    status: str,
    limit_price: Decimal,
) -> None:
    settings = _settings(tmp_path, "unlinked-exit.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    risk_context, quantity_policy = _bound_kis_item_payloads(
        settings=settings,
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 5},
    )
    item_id = f"unlinked-{intent}"
    item = ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent=intent,
        status=status,
        provider="kis",
        venue="kis",
        asset_class="stock",
        trigger_price=limit_price,
        limit_price=limit_price,
        time_in_force="day",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
        decision_agent_name="signal-decider",
    )
    command = PlaceOrderRequest(
        idempotency_key=f"monitoring:{intent}:{item_id}",
        execution_plan_item_id=item.id,
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol="005930",
        side="sell",
        quantity=5,
        order_type="limit",
        limit_price=limit_price,
        time_in_force="day",
        exchange="KRX",
        metadata={"deterministic_sell_quantity_cap": 5},
    )
    _bind_monitoring_authority(item, command)
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="unlinked-automated-exit-plan",
                source="kai-trading-core",
                market_date=date(2026, 10, 26),
                mode="paper",
                status="accepted",
                items=[item],
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409
    assert result.payload["block_reason"] == "strategy_family_lineage_unavailable"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    with app.state.db_session_factory() as session:
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        assert persisted.status == "manual_review_required"
        assert session.scalars(select(BrokerOrder)).all() == []
    engine.dispose()


def test_automated_exit_rejects_family_identity_tampering_before_broker_call(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-identity-mismatch.db")
    adapter = _adapter()
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    item = _family_item(
        settings=settings,
        item_id="family-identity-mismatch",
        index=0,
        side="sell",
        intent="target_sell",
        lane="signal-decider",
        family="family-a",
        trigger_price=Decimal("120"),
    )
    command = _sell_command(item).model_copy(
        update={
            "metadata": {
                "deterministic_sell_quantity_cap": 5,
                broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: {
                    "policy_version": 1,
                    "entry_family_key": "source:family-b",
                    "decision_lane": "short-term-decider",
                },
            }
        }
    )
    _bind_monitoring_authority(item, command)
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="family-identity-mismatch-plan",
                source="kai-trading-core",
                market_date=date(2026, 10, 26),
                mode="paper",
                status="accepted",
                items=[item],
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source=MONITORING_SOURCE,
        )

    assert result.status_code == 409
    assert result.payload["block_reason"] == "strategy_family_identity_mismatch"
    assert result.payload["manual_review_required"] is True
    assert adapter.order_calls == []
    with app.state.db_session_factory() as session:
        persisted = session.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        assert persisted.status == "manual_review_required"
        assert session.scalars(select(BrokerOrder)).all() == []
    engine.dispose()


def test_explicit_operator_exit_keeps_legacy_unlinked_order_contract(tmp_path) -> None:
    settings = _settings(tmp_path, "operator-unlinked-exit.db")
    adapter = _adapter(
        holdings=[
            BrokerAccountHolding(
                symbol="005930",
                quantity=5,
                sellable_quantity=5,
                average_price=Decimal("100"),
            )
        ]
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    _resume(app)
    risk_context, quantity_policy = _bound_kis_item_payloads(
        settings=settings,
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 5},
    )
    item = ExecutionPlanItem(
        id="operator-unlinked-target",
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="target_sell",
        status="target_sell_monitoring",
        provider="kis",
        venue="kis",
        asset_class="stock",
        trigger_price=Decimal("120"),
        limit_price=Decimal("120"),
        time_in_force="day",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
    )
    command = PlaceOrderRequest(
        idempotency_key="operator:target-sell:unlinked",
        execution_plan_item_id=item.id,
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol="005930",
        side="sell",
        quantity=5,
        order_type="limit",
        limit_price=Decimal("120"),
        time_in_force="day",
        exchange="KRX",
    )
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="operator-unlinked-target-plan",
                source="operator",
                market_date=date(2026, 10, 26),
                mode="paper",
                status="accepted",
                items=[item],
            )
        )
        session.commit()
        result = broker_orders_service.place_broker_order(
            settings=settings,
            db=session,
            adapter=adapter,
            command=command,
            request_source="post:/orders",
        )

    assert result.status_code == 201, result.payload
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_short_term_max_holding_cannot_be_disabled_or_extended() -> None:
    settings = Settings(exec_agent_max_holding_trading_sessions=9)
    for policy in (
        False,
        {"enabled": False},
        {"enabled": True, "trading_sessions": 99},
    ):
        item = ExecutionPlanItem(
            id=f"short-policy-{str(policy)}",
            execution_plan_id="short-policy-plan",
            symbol="005930",
            side="buy",
            intent="watch_buy",
            status="filled",
            decision_agent_name="short-term-decider",
            risk_context={"max_holding_close_policy": policy},
        )
        assert monitoring_service._post_entry_max_holding_sessions(
            settings=settings,
            entry_item=item,
        ) == 3

    signal_item = ExecutionPlanItem(
        id="signal-policy-disabled",
        execution_plan_id="signal-policy-plan",
        symbol="005930",
        side="buy",
        intent="watch_buy",
        status="filled",
        decision_agent_name="signal-decider",
        risk_context={"max_holding_close_policy": False},
    )
    assert monitoring_service._post_entry_max_holding_sessions(
        settings=settings,
        entry_item=signal_item,
    ) is None

    short_disabled_with_lower_global_default = ExecutionPlanItem(
        id="short-policy-disabled-lower-default",
        execution_plan_id="short-policy-plan",
        symbol="005930",
        side="buy",
        intent="watch_buy",
        status="filled",
        decision_agent_name="short-term-decider",
        risk_context={"max_holding_close_policy": {"enabled": False}},
    )
    assert monitoring_service._post_entry_max_holding_sessions(
        settings=Settings(exec_agent_max_holding_trading_sessions=2),
        entry_item=short_disabled_with_lower_global_default,
    ) == 3


def test_source_identity_dedupe_requires_same_decision_lane() -> None:
    signal = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        source_candidate_id="shared-source",
        payload={"decision_agent_name": "signal-decider"},
    )
    short = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        source_candidate_id="shared-source",
        payload={"decision_agent_name": "short-term-decider"},
    )
    same_signal = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        source_candidate_id="shared-source",
        payload={"decision_agent_name": "signal-decider"},
    )

    assert not monitoring_service._broker_open_orders_share_identity(signal, short)
    assert monitoring_service._broker_open_orders_share_identity(signal, same_signal)


def test_stop_and_target_coverage_are_isolated_by_exact_family_lane(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-lane-coverage.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    stop_a = _family_sell_item(
        settings=settings,
        item_id="coverage-stop-a",
        lane="signal-decider",
        family="coverage-family-a",
        intent="stop_sell",
        status="broker_order_submitted",
        quantity=10,
    )
    stop_b = _family_sell_item(
        settings=settings,
        item_id="coverage-stop-b",
        lane="short-term-decider",
        family="coverage-family-b",
        intent="stop_sell",
        status="stop_sell_pending_config",
        quantity=5,
    )
    target_a = _family_sell_item(
        settings=settings,
        item_id="coverage-target-a",
        lane="signal-decider",
        family="coverage-family-a",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=10,
        trigger_price=Decimal("120"),
    )
    for item in (stop_a, stop_b, target_a):
        item.quantity_policy = {
            key: value
            for key, value in dict(item.quantity_policy or {}).items()
            if key != "linked_entry_item_key"
        }
        item.risk_context = {
            key: value
            for key, value in dict(item.risk_context or {}).items()
            if key != "entry_context"
        }
        item.source_plan_item_id = None
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="family-lane-coverage",
            source="test",
            market_date=date(2026, 10, 26),
            mode="paper",
            status="accepted",
            items=[stop_a, stop_b, target_a],
        )
        session.add_all([plan, _held_position(settings=settings, quantity=10)])
        session.flush()
        _seed_exact_strategy_family_in_session(
            settings=settings,
            session=session,
            sell_items=[stop_a, target_a],
            quantity=5,
            entry_price=Decimal("100"),
            decision_agent_name="signal-decider",
        )
        _seed_exact_strategy_family_in_session(
            settings=settings,
            session=session,
            sell_items=[stop_b],
            quantity=5,
            entry_price=Decimal("100"),
            decision_agent_name="short-term-decider",
        )
        session.commit()

        position = session.scalar(
            select(ExecutionPosition).where(
                ExecutionPosition.symbol == "005930"
            )
        )
        assert position is not None
        issues, repaired_count = (
            monitoring_service._repair_unprotected_stop_coverage(
                settings=settings,
                db=session,
                positions_by_symbol={"005930": position},
                triggered_at=datetime.now(UTC),
            )
        )
        session.flush()
        target_issues = monitoring_service._profit_target_coverage_issues(
            settings=settings,
            db=session,
            positions_by_symbol={"005930": position},
            evaluated_at=datetime.now(UTC),
        )

        assert repaired_count == 1, issues
        assert issues == []
        assert stop_a.quantity_policy["quantity"] == 10
        assert stop_b.status == "stop_sell_monitoring"
        assert stop_b.quantity_policy["quantity"] == 5
        assert len(target_issues) == 1
        assert target_issues[0].held_quantity == 5
        assert target_issues[0].missing_quantity == 5
        assert target_issues[0].decision_lane == "short-term-decider"
    engine.dispose()


def test_due_max_holding_keeps_same_raw_family_in_both_lanes(tmp_path) -> None:
    settings = _settings(tmp_path, "max-holding-lane-key.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    signal = _family_sell_item(
        settings=settings,
        item_id="max-holding-signal",
        lane="signal-decider",
        family="shared-raw-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
    )
    short = _family_sell_item(
        settings=settings,
        item_id="max-holding-short",
        lane="short-term-decider",
        family="shared-raw-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
    )
    for item in (signal, short):
        item.risk_context = {
            **dict(item.risk_context or {}),
            "close_exit_date": "2026-10-25",
        }
    position = _held_position(settings=settings, quantity=10)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[signal, short],
            suffix="max-holding-lanes",
        )
        session.add(position)
        session.commit()
        selected = monitoring_service._prepare_due_max_holding_items(
            settings=settings,
            db=session,
            items=[signal, short],
            positions_by_symbol={"005930": position},
            triggered_at=datetime(2026, 10, 26, 1, tzinfo=UTC),
        )

        assert {item.id for item in selected} == {signal.id, short.id}
        assert signal.status == "max_holding_close_sell_monitoring"
        assert short.status == "max_holding_close_sell_monitoring"
    engine.dispose()


def test_due_max_holding_canonicalizes_same_family_lane_across_domestic_routes(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "max-holding-route-key.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    krx = _family_sell_item(
        settings=settings,
        item_id="max-holding-route-krx",
        lane="signal-decider",
        family="shared-route-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    nxt = _family_sell_item(
        settings=settings,
        item_id="max-holding-route-nxt",
        lane="signal-decider",
        family="shared-route-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
        exchange="NXT",
    )
    for item in (krx, nxt):
        item.risk_context = {
            **dict(item.risk_context or {}),
            "close_exit_date": "2026-10-25",
        }
    position = _held_position(settings=settings, quantity=10)
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[krx, nxt],
            suffix="max-holding-routes",
        )
        session.add(position)
        session.commit()

        selected = monitoring_service._prepare_due_max_holding_items(
            settings=settings,
            db=session,
            items=[krx, nxt],
            positions_by_symbol={"005930": position},
            triggered_at=datetime(2026, 10, 26, 1, tzinfo=UTC),
        )

        assert {item.id for item in selected} == {nxt.id}
        assert krx.status == "superseded"
        assert nxt.status == "max_holding_close_sell_monitoring"
    engine.dispose()


def test_family_coverage_shares_inventory_across_kis_domestic_routes(tmp_path) -> None:
    settings = _settings(tmp_path, "family-route-coverage.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    krx_stop = _family_sell_item(
        settings=settings,
        item_id="coverage-route-krx-stop",
        lane="signal-decider",
        family="seed-will-replace",
        intent="stop_sell",
        status="stop_sell_pending_config",
        quantity=5,
        exchange="KRX",
    )
    krx_target = _family_sell_item(
        settings=settings,
        item_id="coverage-route-krx-target",
        lane="signal-decider",
        family="seed-will-replace",
        intent="target_sell",
        status="target_sell_pending_config",
        quantity=5,
        trigger_price=Decimal("120"),
        exchange="KRX",
    )
    nxt_stop = _family_sell_item(
        settings=settings,
        item_id="coverage-route-nxt-stop",
        lane="signal-decider",
        family="seed-will-replace",
        intent="stop_sell",
        status="broker_order_submitted",
        quantity=5,
        exchange="NXT",
    )
    nxt_target = _family_sell_item(
        settings=settings,
        item_id="coverage-route-nxt-target",
        lane="signal-decider",
        family="seed-will-replace",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("120"),
        exchange="NXT",
    )
    for item in (krx_stop, krx_target):
        item.quantity_policy = {
            key: value
            for key, value in dict(item.quantity_policy or {}).items()
            if key != "linked_entry_item_key"
        }
        item.risk_context = {
            key: value
            for key, value in dict(item.risk_context or {}).items()
            if key != "entry_context"
        }
        item.source_plan_item_id = None

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="family-route-coverage",
            source="test",
            market_date=date(2026, 10, 26),
            mode="paper",
            status="accepted",
            items=[krx_stop, krx_target, nxt_stop, nxt_target],
        )
        position = _held_position(settings=settings, quantity=5)
        session.add_all([plan, position])
        session.flush()
        entry_id = _seed_exact_strategy_family_in_session(
            settings=settings,
            session=session,
            sell_items=[krx_stop, krx_target],
            quantity=5,
            entry_price=Decimal("100"),
            decision_agent_name="signal-decider",
        )
        entry = session.get(ExecutionPlanItem, entry_id)
        assert entry is not None
        family_source = entry.source_plan_item_id
        assert family_source is not None
        for item in (nxt_stop, nxt_target):
            item.quantity_policy = {
                **dict(item.quantity_policy or {}),
                "linked_entry_item_key": family_source,
            }
            item.risk_context = {
                **dict(item.risk_context or {}),
                "entry_context": {"source_plan_item_id": family_source},
            }
        session.commit()

        issues, repaired_count = (
            monitoring_service._repair_unprotected_stop_coverage(
                settings=settings,
                db=session,
                positions_by_symbol={"005930": position},
                triggered_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
            )
        )
        target_issues = monitoring_service._profit_target_coverage_issues(
            settings=settings,
            db=session,
            positions_by_symbol={"005930": position},
            evaluated_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
        )

        assert repaired_count == 0, issues
        assert issues == []
        assert krx_stop.status == "stop_sell_pending_config"
        assert nxt_stop.status == "broker_order_submitted"
        assert target_issues == []
    engine.dispose()


def test_same_cycle_stop_repair_preference_folds_domestic_routes(tmp_path) -> None:
    settings = _settings(tmp_path, "same-cycle-stop-route.db")
    recorded_at = datetime(2026, 10, 26, 2, tzinfo=UTC)
    old_krx = _family_sell_item(
        settings=settings,
        item_id="same-cycle-old-krx",
        lane="signal-decider",
        family="same-cycle-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    repaired_krx = _family_sell_item(
        settings=settings,
        item_id="same-cycle-repair-krx",
        lane="signal-decider",
        family="same-cycle-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    nxt = _family_sell_item(
        settings=settings,
        item_id="same-cycle-nxt",
        lane="signal-decider",
        family="same-cycle-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="NXT",
    )
    repaired_krx.risk_context = {
        **dict(repaired_krx.risk_context or {}),
        "last_monitoring_event": {
            "event": "unprotected_position_stop_rearmed",
            "cloned_repair_item": True,
            "active_stop_quantity": 0,
            "recorded_at": recorded_at.isoformat(),
        },
    }

    selected = monitoring_service._prefer_same_cycle_cloned_stop_repairs(
        items=[old_krx, repaired_krx, nxt],
        triggered_at=recorded_at,
    )

    assert {item.id for item in selected} == {repaired_krx.id}


def test_sibling_trigger_updates_same_family_lane_across_domestic_routes(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "sibling-trigger-scope.db")
    entry = _watch_item(
        settings=settings,
        item_id="sibling-entry",
        lane="signal-decider",
        source_key="shared-sibling-family",
        quantity=5,
    )
    signal_krx = _family_sell_item(
        settings=settings,
        item_id="sibling-signal-krx",
        lane="signal-decider",
        family="shared-sibling-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    short_krx = _family_sell_item(
        settings=settings,
        item_id="sibling-short-krx",
        lane="short-term-decider",
        family="shared-sibling-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    signal_nxt = _family_sell_item(
        settings=settings,
        item_id="sibling-signal-nxt",
        lane="signal-decider",
        family="shared-sibling-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        exchange="NXT",
    )
    ExecutionPlan(
        external_plan_id="sibling-family-lane-route",
        source="test",
        market_date=date(2026, 10, 26),
        mode="paper",
        status="accepted",
        items=[entry, signal_krx, short_krx, signal_nxt],
    )

    monitoring_service._update_sibling_trigger_price(
        item=entry,
        intent="stop_sell",
        trigger_price=Decimal("88"),
    )

    assert signal_krx.trigger_price == Decimal("88")
    assert short_krx.trigger_price == Decimal("90")
    assert signal_nxt.trigger_price == Decimal("88")
    assert monitoring_service._active_sibling_monitor(
        entry_item=entry,
        intent="stop_sell",
    ) is signal_krx


def test_stop_read_and_partial_rearm_share_domestic_family_inventory_scope(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "stop-read-rearm-scope.db").model_copy(
        update={"breakeven_stop_buffer_pct": Decimal("1")}
    )
    target = _family_sell_item(
        settings=settings,
        item_id="partial-target-signal-krx",
        lane="signal-decider",
        family="shared-stop-family",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=1,
        trigger_price=Decimal("110"),
        exchange="KRX",
    )
    target.risk_context = {
        **dict(target.risk_context or {}),
        "exit_rule": {"kind": "partial_take_profit"},
    }
    signal_krx = _family_sell_item(
        settings=settings,
        item_id="partial-stop-signal-krx",
        lane="signal-decider",
        family="shared-stop-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("90"),
        exchange="KRX",
    )
    short_krx = _family_sell_item(
        settings=settings,
        item_id="partial-stop-short-krx",
        lane="short-term-decider",
        family="shared-stop-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("99"),
        exchange="KRX",
    )
    signal_nxt = _family_sell_item(
        settings=settings,
        item_id="partial-stop-signal-nxt",
        lane="signal-decider",
        family="shared-stop-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("98"),
        exchange="NXT",
    )
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[target, signal_krx, short_krx, signal_nxt],
            suffix="stop-read-rearm",
        )

        assert monitoring_service._locked_effective_protective_stop_price(
            db=session,
            item=target,
        ) == Decimal("98")

        monitoring_service._rearm_stop_after_partial_take_profit(
            settings=settings,
            item=target,
            position=_held_position(settings=settings, quantity=5),
            remaining_quantity=4,
            submitted_quantity=1,
            recorded_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
        )

        assert signal_krx.trigger_price == Decimal("101.0000")
        assert signal_krx.quantity_policy["quantity"] == 4
        assert short_krx.trigger_price == Decimal("99")
        assert short_krx.quantity_policy["quantity"] == 5
        assert signal_nxt.trigger_price == Decimal("101.0000")
        assert signal_nxt.quantity_policy["quantity"] == 4
    engine.dispose()


def test_disabled_max_holding_policy_mutates_all_domestic_inventory_routes(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "disabled-max-holding-route.db")
    entry = _watch_item(
        settings=settings,
        item_id="disabled-max-entry",
        lane="signal-decider",
        source_key="disabled-max-family",
        quantity=5,
    )
    krx = _family_sell_item(
        settings=settings,
        item_id="disabled-max-krx",
        lane="signal-decider",
        family="disabled-max-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    nxt = _family_sell_item(
        settings=settings,
        item_id="disabled-max-nxt",
        lane="signal-decider",
        family="disabled-max-family",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        quantity=5,
        exchange="NXT",
    )
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    with app.state.db_session_factory() as session:
        _persist_watch_plan(
            session=session,
            items=[entry, krx, nxt],
            suffix="disabled-max-route",
        )

        changed = (
            monitoring_service._reconcile_disabled_post_entry_max_holding_monitors(
                db=session,
                entry_item=entry,
                triggered_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
            )
        )

        assert changed == 2
        assert krx.status == "superseded"
        assert nxt.status == "superseded"
    engine.dispose()


def test_family_remaining_quantity_and_reservation_share_domestic_scope(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "family-remaining-route.db")
    krx = _family_sell_item(
        settings=settings,
        item_id="remaining-family-krx",
        lane="signal-decider",
        family="remaining-shared-family",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=5,
        exchange="KRX",
    )
    nxt = _family_sell_item(
        settings=settings,
        item_id="remaining-family-nxt",
        lane="signal-decider",
        family="remaining-shared-family",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=5,
        exchange="NXT",
    )
    krx_scope = monitoring_service._plan_item_family_route_inventory_key(krx)
    nxt_scope = monitoring_service._plan_item_family_route_inventory_key(nxt)
    assert krx_scope is not None
    assert nxt_scope is not None
    assert krx_scope == nxt_scope
    remaining = {
        (krx.symbol, krx_scope[0], krx_scope[1]): 2,
    }

    assert monitoring_service._sell_remaining_quantity(
        item=krx,
        remaining_quantities_by_symbol={krx.symbol: 7},
        remaining_quantities_by_family=remaining,
    ) == 2
    assert monitoring_service._sell_remaining_quantity(
        item=nxt,
        remaining_quantities_by_symbol={nxt.symbol: 7},
        remaining_quantities_by_family=remaining,
    ) == 2

    monitoring_service._reserve_family_quantity(
        remaining_quantities_by_family=remaining,
        item=krx,
        quantity=2,
    )

    assert monitoring_service._sell_remaining_quantity(
        item=nxt,
        remaining_quantities_by_symbol={nxt.symbol: 7},
        remaining_quantities_by_family=remaining,
    ) is None

    assert (krx.symbol, krx_scope[0], krx_scope[1]) not in remaining


def test_nonretryable_target_suppression_is_exact_family_lane_and_route(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "target-suppression-exact-scope.db")
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "87654321"}
    )
    entry = _watch_item(
        settings=settings,
        item_id="target-suppression-entry",
        lane="signal-decider",
        source_key="target-suppression-family",
        quantity=5,
    )
    entry.status = "broker_order_submitted"
    expected_source = monitoring_service._derived_source_plan_item_id(
        entry,
        "post-entry-target",
    )

    def rejected_target(
        *, item_id: str, lane: str, route_settings: Settings
    ) -> ExecutionPlanItem:
        item = _family_sell_item(
            settings=route_settings,
            item_id=item_id,
            lane=lane,
            family="target-suppression-family",
            intent="target_sell",
            status="broker_order_rejected",
            quantity=5,
            trigger_price=Decimal("120"),
        )
        item.source_plan_item_id = expected_source
        item.broker_orders.append(
            BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key=f"rejected:{item_id}",
                symbol=item.symbol,
                side="sell",
                status="rejected",
                quantity=5,
                last_error_retryable=False,
            )
        )
        return item

    other_lane = rejected_target(
        item_id="target-rejected-other-lane",
        lane="short-term-decider",
        route_settings=settings,
    )
    other_route = rejected_target(
        item_id="target-rejected-other-route",
        lane="signal-decider",
        route_settings=foreign_settings,
    )
    plan = ExecutionPlan(
        external_plan_id="target-suppression-exact-scope",
        source="test",
        mode="paper",
        status="accepted",
        items=[entry, other_lane, other_route],
    )

    assert not monitoring_service._nonretryable_sibling_monitor_suppresses_recreation(
        entry_item=entry,
        intent="target_sell",
        trigger_price=Decimal("120"),
    )

    own_scope = rejected_target(
        item_id="target-rejected-own-scope",
        lane="signal-decider",
        route_settings=settings,
    )
    plan.items.append(own_scope)

    assert monitoring_service._nonretryable_sibling_monitor_suppresses_recreation(
        entry_item=entry,
        intent="target_sell",
        trigger_price=Decimal("120"),
    )


def test_active_sell_replacement_keeps_same_raw_family_in_both_lanes(
    tmp_path,
) -> None:
    settings = _settings(tmp_path, "active-sell-lane-key.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    signal = _family_sell_item(
        settings=settings,
        item_id="active-target-signal",
        lane="signal-decider",
        family="shared-raw-family",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("120"),
    )
    short = _family_sell_item(
        settings=settings,
        item_id="active-target-short",
        lane="short-term-decider",
        family="shared-raw-family",
        intent="target_sell",
        status="target_sell_monitoring",
        quantity=5,
        trigger_price=Decimal("115"),
    )
    position = _held_position(settings=settings, quantity=10)
    with app.state.db_session_factory() as session:
        session.add_all(
            [
                ExecutionPlan(
                    external_plan_id="active-target-old-short",
                    source="test",
                    market_date=date(2026, 10, 25),
                    accepted_at=datetime(2026, 10, 25, 1, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[short],
                ),
                ExecutionPlan(
                    external_plan_id="active-target-new-signal",
                    source="test",
                    market_date=date(2026, 10, 26),
                    accepted_at=datetime(2026, 10, 26, 1, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[signal],
                ),
                position,
            ]
        )
        session.commit()
        selected = (
            monitoring_service._append_held_active_sell_monitor_reconciliations(
                settings=settings,
                db=session,
                selected_items=[signal],
                symbols={"005930"},
                current_market_date=date(2026, 10, 26),
                effective_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
            )
        )

        assert {item.id for item in selected} == {signal.id, short.id}
        assert short.status == "target_sell_monitoring"
    engine.dispose()


def test_stop_fallback_keeps_same_raw_family_in_both_lanes(tmp_path) -> None:
    settings = _settings(tmp_path, "stop-fallback-lane-key.db")
    app, engine = build_test_app(settings=settings, broker_adapter=_adapter())
    signal = _family_sell_item(
        settings=settings,
        item_id="fallback-stop-signal",
        lane="signal-decider",
        family="shared-raw-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
    )
    short = _family_sell_item(
        settings=settings,
        item_id="fallback-stop-short",
        lane="short-term-decider",
        family="shared-raw-family",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity=5,
    )
    position = _held_position(settings=settings, quantity=10)
    with app.state.db_session_factory() as session:
        session.add_all(
            [
                ExecutionPlan(
                    external_plan_id="fallback-stops",
                    source="test",
                    market_date=date(2026, 10, 26),
                    mode="paper",
                    status="accepted",
                    items=[signal, short],
                ),
                position,
            ]
        )
        session.commit()
        selected = monitoring_service._append_held_stop_sell_fallbacks(
            settings=settings,
            db=session,
            monitored_item_filter=(
                ExecutionPlanItem.status == "stop_sell_monitoring"
            ),
            selected_items=[signal],
            symbols={"005930"},
            current_market_date=date(2026, 10, 26),
            effective_at=datetime(2026, 10, 26, 2, tzinfo=UTC),
        )

        assert {item.id for item in selected} == {signal.id, short.id}
        assert short.status == "stop_sell_monitoring"
    engine.dispose()


@pytest.mark.parametrize("signal_action", ("wait", "reject"))
def test_signal_crosscheck_is_audit_only_for_short_term_watch_buy(
    tmp_path,
    signal_action: str,
) -> None:
    settings = _settings(tmp_path, f"crosscheck-audit-{signal_action}.db")
    item = _watch_item(
        settings=settings,
        item_id=f"short-watch-with-signal-{signal_action}",
        lane="short-term-decider",
        source_key=f"short-family-with-signal-{signal_action}",
    )
    risk_context = dict(item.risk_context or {})
    risk_context.update(
        {
            "signal_decider_crosscheck": {
                "action": signal_action,
                "warning": "cross-lane audit evidence",
            },
            "tactical_action": "tactical_buy",
        }
    )
    item.risk_context = risk_context
    original_status = item.status
    original_quantity_policy = dict(item.quantity_policy or {})

    rejection = monitoring_service._watch_buy_high_risk_gate_rejection(
        settings=settings,
        item=item,
        observed_price=Decimal("100"),
    )

    assert rejection is None
    assert item.status == original_status
    assert item.quantity_policy == original_quantity_policy
    assert item.risk_context["signal_decider_crosscheck"] == {
        "action": signal_action,
        "warning": "cross-lane audit evidence",
    }


def test_short_term_crosscheck_is_audit_only_for_signal_watch_buy(tmp_path) -> None:
    settings = _settings(tmp_path, "reverse-crosscheck-audit.db")
    item = _watch_item(
        settings=settings,
        item_id="signal-watch-with-short-reject",
        lane="signal-decider",
        source_key="signal-family-with-short-reject",
    )
    risk_context = dict(item.risk_context or {})
    risk_context["short_term_decider_crosscheck"] = {
        "action": "reject",
        "warning": "cross-lane audit evidence",
    }
    item.risk_context = risk_context
    original_status = item.status
    original_quantity_policy = dict(item.quantity_policy or {})

    rejection = monitoring_service._watch_buy_high_risk_gate_rejection(
        settings=settings,
        item=item,
        observed_price=Decimal("100"),
    )

    assert rejection is None
    assert item.status == original_status
    assert item.quantity_policy == original_quantity_policy
    assert item.risk_context["short_term_decider_crosscheck"]["action"] == "reject"
