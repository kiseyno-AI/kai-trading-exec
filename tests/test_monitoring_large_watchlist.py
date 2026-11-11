"""Large watchlists must reach the normal broker/order persistence boundaries."""
from dataclasses import replace
from math import ceil
from decimal import Decimal

import pytest

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.broker.base import BrokerAccountSnapshot, BrokerConfig
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlanItem, ExecutionPosition
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StaticMonitoringPriceSource,
    StubBrokerAdapter,
    _create_watch_buy_candidate,
    _freeze_autonomous_runtime_at_krx_open,
    _seed_exact_strategy_family_for_sell_items,
    _use_historical_broker_boundary,
    build_order_placement,
    build_test_app,
    build_token_grant,
)


def test_hundred_buy_and_sell_watches_submit_without_duplicates_across_bounded_cycles(monkeypatch):
    started_at = _freeze_autonomous_runtime_at_krx_open(monkeypatch)
    _use_historical_broker_boundary(monkeypatch)
    symbols = [f'{index:06d}' for index in range(1, 101)]
    buys, sells = symbols[:50], symbols[50:]
    targets, stops = sells[:25], sells[25:]

    class Adapter(StubBrokerAdapter):
        def place_order(self, access_token, request):
            placement = super().place_order(access_token, request)
            return replace(placement, broker_order_id=f'large-watch-{len(self.order_calls)}')

    adapter = Adapter(
        config=BrokerConfig(provider='kis', mode='paper', configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id='large-watch'),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal('100000000'), buying_power=Decimal('100000000'),
            total_asset_value=Decimal('100000000'), previous_total_asset_value=Decimal('100000000'),
            observed_at=started_at, source='stub:account_snapshot',
        ),
    )
    prices = StaticMonitoringPriceSource({
        symbol: Decimal('70950') if symbol in stops else Decimal('71050') for symbol in symbols
    })
    app, engine = build_test_app(
        settings=Settings(
            database_url='sqlite:///:memory:', runtime_mode='paper', kis_app_key='paper-key',
            kis_app_secret='paper-secret', kis_account_number='12345678',
            monitoring_max_symbols_per_cycle=6, monitoring_max_broker_actions_per_cycle=1,
            monitoring_expire_prior_market_watch_buys_enabled=False, account_drawdown_gate_enabled=False,
        ),
        broker_adapter=adapter, monitoring_price_source=prices, autonomous_runtime_enabled=True,
    )
    client = TestClient(app)
    try:
        assert client.post('/controls/resume', json={
            'actor': 'test', 'source': 'test', 'reason': 'large watchlist regression',
        }).status_code == 200
        items = [{
            'symbol': symbol, 'side': 'buy', 'intent': 'next_open_buy',
            'quantity_policy': {'kind': 'shares', 'quantity': 1},
            'buy_recommendation_price': '71000', 'chase_limit_price': '71200',
            'time_in_force': 'day', 'decision_agent_name': 'short-term-decider',
            'source_plan_item_id': f'large-buy-{symbol}',
            'risk_context': {'exchange': 'KRX', 'stop_price': '70000', 'trigger_direction': 'at_or_above'},
        } for symbol in buys] + [{
            'symbol': symbol, 'side': 'sell', 'intent': 'target_sell' if symbol in targets else 'stop_sell',
            'quantity_policy': {'kind': 'shares', 'quantity': 1}, 'trigger_price': '71000',
            'time_in_force': 'day', 'risk_context': {'exchange': 'KRX'},
        } for symbol in sells]
        response = client.post('/execution-plans', json={
            'plan_id': 'hundred-watches', 'source': 'operator-import',
            'market_date': started_at.date().isoformat(), 'items': items,
        })
        assert response.status_code == 201, response.text
        item_ids = response.json()['persisted_execution_item_ids']
        with app.state.db_session_factory() as db:
            db.add_all([ExecutionPosition(
                mode='paper', symbol=symbol, quantity=1,
                average_price=Decimal('72000') if symbol in stops else Decimal('70000'),
                updated_from='reconciliation', raw_payload={'source': 'broker'},
            ) for symbol in sells])
            db.commit()
        for symbol, item_id in zip(sells, item_ids[50:]):
            _seed_exact_strategy_family_for_sell_items(
                app, sell_item_ids=[item_id], quantity=1,
                entry_price=Decimal('72000') if symbol in stops else Decimal('70000'),
            )

        coordinator = app.state.autonomous_runtime
        # A single action slot requires multiple complete quote sweeps of both sides.
        max_cycles = 2 * ceil(len(symbols) / 6) * 6
        for _ in range(max_cycles):
            coordinator._monitoring_cycle_sync(started_at)
            if len(adapter.order_calls) == 100:
                break
        assert {call.symbol for call in adapter.order_calls} == set(symbols), sorted(set(symbols) - {call.symbol for call in adapter.order_calls})
        assert len(adapter.order_calls) == 100
        assert {call.symbol for call in adapter.order_calls if call.side == 'buy'} == set(buys)
        assert {call.symbol for call in adapter.order_calls if call.side == 'sell'} == set(sells)
        assert set().union(*(set(call) for call in prices.calls)) == set(symbols)
        assert all(len(call) <= 6 for call in prices.calls)
        # Repeating the same trigger must reuse durable submitted order state.
        for _ in range(20):
            coordinator._monitoring_cycle_sync(started_at)
        assert len(adapter.order_calls) == 100
        with app.state.db_session_factory() as db:
            orders = db.scalars(select(BrokerOrder).where(BrokerOrder.execution_plan_item_id.in_(item_ids))).all()
        assert len(orders) == 100
        assert len({order.idempotency_key for order in orders}) == 100
    finally:
        engine.dispose()


@pytest.mark.parametrize('retry_price,submitted_count', [('70950', 2), ('71300', 1)])
def test_deferred_buy_touch_survives_coordinator_restart_and_rechecks_chase(monkeypatch, retry_price, submitted_count):
    started_at = _freeze_autonomous_runtime_at_krx_open(monkeypatch)
    _use_historical_broker_boundary(monkeypatch)

    class Adapter(StubBrokerAdapter):
        def place_order(self, access_token, request):
            placement = super().place_order(access_token, request)
            return replace(placement, broker_order_id=f'durable-touch-{len(self.order_calls)}')

    adapter = Adapter(
        config=BrokerConfig(provider='kis', mode='paper', configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id='durable-touch'),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal('100000000'), buying_power=Decimal('100000000'),
            total_asset_value=Decimal('100000000'), previous_total_asset_value=Decimal('100000000'),
            observed_at=started_at, source='stub:account_snapshot',
        ),
    )
    prices = StaticMonitoringPriceSource({'000001': Decimal('71050'), '000002': Decimal('71050')})
    app, engine = build_test_app(
        settings=Settings(
            database_url='sqlite:///:memory:', runtime_mode='paper', kis_app_key='paper-key',
            kis_app_secret='paper-secret', kis_account_number='12345678',
            monitoring_max_symbols_per_cycle=6, monitoring_max_broker_actions_per_cycle=1,
            monitoring_expire_prior_market_watch_buys_enabled=False, account_drawdown_gate_enabled=False,
        ),
        broker_adapter=adapter, monitoring_price_source=prices, autonomous_runtime_enabled=True,
    )
    client = TestClient(app)
    try:
        assert client.post('/controls/resume', json={
            'actor': 'test', 'source': 'test', 'reason': 'durable touch regression',
        }).status_code == 200
        item_ids = [_create_watch_buy_candidate(
            client, plan_id=f'durable-touch-{symbol}', symbol=symbol,
            market_date=started_at.date().isoformat(),
            risk_context={'exchange': 'KRX', 'stop_price': '70000', 'trigger_direction': 'at_or_above'},
        ) for symbol in prices.prices_by_symbol]
        first = app.state.autonomous_runtime._monitoring_cycle_sync(started_at)
        assert len(adapter.order_calls) == 1, first.get("items")
        deferred_id = next(item_id for item_id in item_ids if item_id != adapter.order_calls[0].execution_plan_item_id)
        with app.state.db_session_factory() as db:
            deferred = db.get(ExecutionPlanItem, deferred_id)
            assert deferred.status == 'watch_buy_triggered_pending_dispatch'
            touch = deferred.risk_context['watch_buy_trigger_cross']
            assert touch
            deferred_symbol = deferred.symbol

        # Fresh coordinator memory must restore the observed touch from the persisted plan.
        prices.prices_by_symbol[deferred_symbol] = Decimal(retry_price)
        restarted = AutonomousRuntimeCoordinator(
            settings=app.state.settings, session_factory=app.state.db_session_factory,
            broker_adapter_factory=lambda _: adapter, monitoring_price_source=prices,
        )
        result = restarted._monitoring_cycle_sync(started_at)
        assert len(adapter.order_calls) == submitted_count
        with app.state.db_session_factory() as db:
            deferred = db.get(ExecutionPlanItem, deferred_id)
            if submitted_count == 2:
                assert deferred.risk_context['watch_buy_trigger_cross'] == touch
            assert deferred.status == ('watch_buy_submitted' if submitted_count == 2 else 'watch_monitoring_active')
        if submitted_count == 1:
            assert any(item.get('block_reason') == 'price_above_chase_limit' for item in result['items'])
        restarted._monitoring_cycle_sync(started_at)
        assert len(adapter.order_calls) == submitted_count
    finally:
        engine.dispose()
