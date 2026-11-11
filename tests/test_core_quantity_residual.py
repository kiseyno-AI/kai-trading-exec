from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.api.schemas.execution import PlaceOrderRequest
from app.core.config import Settings
from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot, BrokerConfig, BrokerLiveMarketContext
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionJob, ExecutionPlanItem
from app.services import broker_orders, core_quantity, execution_jobs, monitoring
from test_broker_auth import StubBrokerAdapter, build_order_placement, build_token_grant, _test_kis_execution_scope, _use_historical_broker_boundary
from test_core_handoff_v4 import _build_test_app, _refresh_material_fingerprint
from test_core_quantity import core_plan, revision_request


@pytest.mark.parametrize('watch', [False, True])
@pytest.mark.parametrize('unknown_outcome', [False, True])
def test_same_revision_terminal_partial_buy_executes_residual_once_across_restart(monkeypatch, watch, unknown_outcome):
    now = datetime(2026, 9, 10, 2, tzinfo=UTC)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: now)
    monkeypatch.setattr(broker_orders, 'core_buy_authority_block_reason', lambda **kwargs: None)
    _use_historical_broker_boundary(monkeypatch)
    results = []

    def place_and_record(**kwargs):
        result = broker_orders.place_broker_order(**kwargs)
        results.append(result.payload)
        return result

    monkeypatch.setattr(execution_jobs, 'place_broker_order', place_and_record)
    adapter = StubBrokerAdapter(
        config=BrokerConfig(provider='kis', mode='mock', configured=True, api_base_url='https://openapivts.koreainvestment.com:29443'),
        token_grant=build_token_grant(), order_placement=build_order_placement(broker_order_id='residual-two'),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal('10000000'), buying_power=Decimal('10000000'), observed_at=now,
            total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'),
            holdings=[BrokerAccountHolding(symbol='005930', quantity=4, average_price=Decimal('70000'), payload={'current_price': '70000'})],
        ),
    )
    adapter.get_live_market_context = lambda *args, **kwargs: BrokerLiveMarketContext(symbol='005930', exchange='KRX', observed_at=now, bid=Decimal('69990'), ask=Decimal('70000'), freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True)
    if unknown_outcome:
        adapter.order_error = TimeoutError('broker response lost after submission')
    app, engine = _build_test_app(settings=Settings(database_url='sqlite:///:memory:', runtime_mode='mock', account_drawdown_gate_enabled=False))
    with TestClient(app) as client:
        plan = core_plan()
        plan['items'][0]['limit_price'] = '70000'
        if watch:
            plan['items'][0]['intent'] = 'intraday_watch_buy'
            plan['items'][0]['trigger_price'] = '70000'
            plan['items'][0]['limit_price'] = '70000'
            plan['items'][0]['risk_context']['trigger_direction'] = 'at_or_below'
        plan['items'][1]['trigger_price'], plan['items'][2]['trigger_price'] = '76000', '69000'
        _refresh_material_fingerprint(plan)
        accepted = client.post('/v4/execution-plans', json=plan).json()
        update = revision_request(accepted, plan, target=6)
        assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['updated_count'] == 1
        assert client.post('/controls/resume').status_code == 200
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            base = monitoring._monitoring_order_idempotency_key(item=entry, is_watch_buy=True, retry_source_order_pk=None) if watch else execution_jobs._place_order_idempotency_key(job)
            metadata = {broker_orders.STRATEGY_ENTRY_FAMILY_METADATA_KEY: broker_orders._strategy_entry_family_identity(entry)}
            order = BrokerOrder(
                id='partially-filled-canceled', execution_plan_item_id=entry.id, broker_name='kis', mode='mock',
                idempotency_key=base, broker_order_id='original-six', symbol=entry.symbol, side='buy', status='canceled', quantity=6,
                venue='kis', asset_class='stock', request_metadata={'execution_scope': _test_kis_execution_scope(app.state.settings, venue='kis'), 'metadata': metadata, 'original_command': {'metadata': metadata}},
                response_metadata={'filled_quantity': 4},
            )
            order.fills.append(ExecutionFill(symbol=entry.symbol, side='buy', quantity=4, price=Decimal('70000'), filled_at=now, payload={'fill_time_precision': 'exact'}))
            db.add(order)
            entry_id = entry.id
            entry.status = 'broker_order_canceled'
            if job is not None:
                job.status = 'completed'
            db.commit()
        assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['unchanged_count'] == 1
        with app.state.db_session_factory() as db:
            entry = db.get(ExecutionPlanItem, entry_id)
            if watch:
                entry.status = 'watch_buy_triggered_pending_dispatch'
                result = monitoring._place_monitoring_order(settings=app.state.settings, db=db, adapter=adapter, item=entry, quantity=2, observed_price=Decimal('70000'), trigger_reason='watch_buy', trigger_metadata=None, triggered_at=now)
                assert result.status_code == (503 if unknown_outcome else 201), result.payload
            else:
                job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
                job.status, job.locked_at, entry.status = 'running', now, 'next_open_buy_dispatching'
                db.commit()
                outcome = execution_jobs._dispatch_job(settings=app.state.settings, db=db, adapter=adapter, job_id=job.id, claim_locked_at=now)
                assert outcome == ('failed' if unknown_outcome else 'completed'), (outcome, job.last_error, [(value.get('message'), value.get('block_reason')) for value in results])
        assert len(adapter.order_calls) == 1 and adapter.order_calls[0].quantity == 2
        with app.state.db_session_factory() as db:
            entry = db.get(ExecutionPlanItem, entry_id)
            key = monitoring._monitoring_order_idempotency_key(item=entry, is_watch_buy=True, retry_source_order_pk=None) if watch else execution_jobs._place_order_idempotency_key(db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy')))
            new_order = db.scalar(select(BrokerOrder).where(BrokerOrder.idempotency_key == key))
            assert new_order is not None and new_order.id != 'partially-filled-canceled'
            assert new_order.status == ('manual_review_required' if unknown_outcome else 'submitted')
            result = broker_orders.place_broker_order(settings=app.state.settings, db=db, adapter=adapter, command=PlaceOrderRequest(idempotency_key=key, execution_plan_item_id=entry.id, provider='kis', asset_class='stock', symbol=entry.symbol, side='buy', quantity=2, order_type='limit' if watch else 'market', limit_price=Decimal('70000') if watch else None, exchange='KRX', time_in_force='day', metadata={'core_quantity_revision': 2}), now=now)
            assert result.payload['duplicate'] is True
        assert len(adapter.order_calls) == 1
    engine.dispose()
