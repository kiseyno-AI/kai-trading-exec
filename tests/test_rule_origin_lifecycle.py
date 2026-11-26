from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.core.config import Settings
from app.api.routes import execution as execution_routes
from app.broker.base import BrokerAccountHolding, BrokerAccountOpenOrder, BrokerAccountSnapshot, BrokerConfig, BrokerLiveMarketContext
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionJob, ExecutionPlanItem, ExecutionPosition
from app.services import broker_orders, core_quantity, core_quantity_reductions, execution_jobs, monitoring, reconciliation, rule_execution
from app.services.execution_lineage import decision_lineage_for_plan_item
from app.services.position_identity import build_position_identity
from test_core_handoff_v4 import _build_test_app, _refresh_material_fingerprint
from test_core_quantity import core_plan, revision_request
from test_rule_origin_v4 import activation_request, rule_plan
from test_broker_auth import StubBrokerAdapter, _test_kis_execution_scope, _use_historical_broker_boundary, build_order_placement, build_token_grant


@pytest.mark.parametrize('exposure_status', [None, 'open', 'filled', 'manual_review_required'])
def test_current_rule_ignores_only_unsubmitted_future_baseline(exposure_status, monkeypatch):
    now = datetime(2026, 9, 9, 23, 30, tzinfo=UTC)
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: now)
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='mock', krx_us_rebound_enabled=True,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        future = core_plan()
        future.update(plan_id='future-baseline', market_date='2026-09-11')
        future['metadata']['target_trade_date'] = future['market_date']
        for key in ('calculated_at', 'valid_until'):
            future['items'][0]['quantity_policy'][key] = future['items'][0]['quantity_policy'][key].replace('2026-09-10', '2026-09-11')
        _refresh_material_fingerprint(future)
        response = client.post('/v4/execution-plans', json=future)
        assert response.status_code == 201, response.text
        if exposure_status:
            with app.state.db_session_factory() as db:
                entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
                entry.status = exposure_status
                db.add(BrokerOrder(execution_plan_item_id=entry.id, broker_name='kis', mode='mock',
                    idempotency_key='future-exposure', broker_order_id='future-broker-order', symbol=entry.symbol,
                    side='buy', status=exposure_status, quantity=1, venue='kis', asset_class='stock',
                    request_metadata={'execution_scope': _test_kis_execution_scope(settings, venue='kis')}))
                db.commit()
        current = rule_plan()
        accepted = client.post('/v4/execution-plans', json=current).json()
        response = client.post('/v4/execution-plans/activations', json=activation_request(current, accepted))
        if exposure_status:
            assert response.status_code == 409, response.text
        else:
            assert response.status_code == 200, response.text
        with app.state.db_session_factory() as db:
            assert db.scalars(select(BrokerOrder).where(BrokerOrder.idempotency_key != 'future-exposure')).all() == []
    engine.dispose()


@pytest.fixture
def partial_rule(monkeypatch, tmp_path, request):
    clock = [datetime(2026, 9, 9, 23, 30, tzinfo=UTC)]
    class SimulatedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz is not None else clock[0].replace(tzinfo=None)

    monkeypatch.setattr(broker_orders, 'datetime', SimulatedDateTime)
    for module in (core_quantity, core_quantity_reductions, rule_execution):
        monkeypatch.setattr(module, 'utc_now', lambda: clock[0])
    monkeypatch.setattr(reconciliation, '_observation_time', lambda: clock[0])
    monkeypatch.setattr(execution_routes, 'dispatch_due_execution_jobs',
                        lambda **kwargs: execution_jobs.dispatch_due_execution_jobs(**kwargs, now=clock[0]))
    monkeypatch.setattr(execution_routes, 'evaluate_monitoring',
                        lambda **kwargs: monitoring.evaluate_monitoring(**kwargs, now=clock[0]))
    _use_historical_broker_boundary(monkeypatch)
    settings = Settings(database_url=f'sqlite:///{tmp_path}/rule-lifecycle.db', runtime_mode='mock',
                        krx_us_rebound_enabled=True, account_drawdown_gate_enabled=False,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    broker = SimpleNamespace(filled=4, canceled=False, unknown=False, cancel_calls=[], price=Decimal('70000'))
    adapter = StubBrokerAdapter(
        config=BrokerConfig(provider='kis', mode='mock', configured=True, api_base_url='https://mock.invalid'),
        token_grant=build_token_grant(), order_placement=build_order_placement(broker_order_id='protective-sell'),
    )
    adapter.get_live_market_context = lambda *args, **kwargs: BrokerLiveMarketContext(
        symbol='005930', exchange='KRX', observed_at=clock[0], bid=broker.price, ask=broker.price + 10,
        freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True)
    adapter.is_krx_trading_day = lambda *args, **kwargs: True
    adapter.get_account_snapshot = lambda *args, **kwargs: BrokerAccountSnapshot(
        available_cash=Decimal('9000000'), buying_power=Decimal('9000000'), observed_at=clock[0],
        total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'),
        holdings=[BrokerAccountHolding(symbol='005930', quantity=broker.filled, average_price=Decimal('70000'))], holdings_complete=True,
        open_orders=[] if broker.canceled else [BrokerAccountOpenOrder(symbol='005930', side='buy', quantity=10,
            remaining_quantity=10 - broker.filled, broker_order_id='partial-rule-buy', limit_price=Decimal('70000'))])
    adapter.reconcile_order = lambda *args, **kwargs: {
        'provider': 'kis', 'broker_order_id': 'partial-rule-buy', 'symbol': '005930',
        'status': 'canceled' if broker.canceled else 'partially_filled', 'filled_quantity': broker.filled,
        'average_fill_price': '70000', 'observed_at': clock[0].isoformat(),
    }
    def cancel(*args, **kwargs):
        broker.cancel_calls.append(kwargs)
        if broker.unknown:
            raise TimeoutError('cancel response lost')
        broker.canceled = True
        return {'status': 'accepted'}
    adapter.cancel_order = cancel
    app.state.broker_adapter_factory = lambda _settings: adapter
    with TestClient(app) as client:
        original = rule_plan()
        original['items'][0]['quantity_policy']['valid_until'] = getattr(request, 'param', '2026-09-10T01:00:00Z')
        _refresh_material_fingerprint(original)
        accepted = client.post('/v4/execution-plans', json=original).json()
        response = client.post('/v4/execution-plans/activations', json=activation_request(original, accepted))
        assert response.status_code == 200, response.text
        assert client.post('/controls/resume').status_code == 200
        clock[0] = datetime(2026, 9, 10, 0, 1, tzinfo=UTC)
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            entry.status = 'partially_filled'
            family = {broker_orders.STRATEGY_ENTRY_FAMILY_METADATA_KEY: broker_orders._strategy_entry_family_identity(entry)}
            order = BrokerOrder(id='partial-rule-buy', plan_item=entry, broker_name='kis', mode='mock',
                idempotency_key='partial-rule-buy', broker_order_id='partial-rule-buy', symbol=entry.symbol,
                side='buy', status='partially_filled', quantity=10, venue='kis', asset_class='stock',
                limit_price=Decimal('70000'), request_metadata={
                    'execution_scope': _test_kis_execution_scope(settings, venue='kis'), 'metadata': family,
                    'original_command': {'metadata': family}, 'decision_lineage': decision_lineage_for_plan_item(entry),
                }, response_metadata={'filled_quantity': 4})
            order.fills.append(ExecutionFill(idempotency_key='partial-fill', symbol=entry.symbol, side='buy', quantity=4,
                price=Decimal('70000'), filled_at=clock[0], payload={'fill_time_precision': 'exact'}))
            db.add(order)
            identity = build_position_identity(settings=settings, provider='kis', exchange='KRX')
            db.add(ExecutionPosition(mode='mock', provider='kis', account_scope=identity.account_scope, exchange='KRX',
                symbol=entry.symbol, quantity=4, average_price=Decimal('70000'), updated_from='test', last_reconciled_at=clock[0]))
            job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            job.status = 'completed'
            db.commit()
            entry_id = entry.id
        yield SimpleNamespace(app=app, engine=engine, client=client, settings=settings, adapter=adapter, broker=broker,
                              clock=clock, original=original, accepted=accepted, entry_id=entry_id)
    engine.dispose()


@pytest.mark.parametrize('control', [None, 'pause', 'kill-switch', 'disabled_flag'])
def test_expired_partial_rule_cancels_without_a_quantity_revision(partial_rule, control):
    env = partial_rule
    if control == 'disabled_flag':
        env.app.state.settings = env.settings.model_copy(update={'krx_us_rebound_enabled': False})
    elif control:
        assert env.client.post(f'/controls/{control}').status_code == 200
    env.clock[0] = datetime(2026, 9, 10, 1, 0, 1, tzinfo=UTC)
    response = env.client.post('/execution-jobs/dispatch')
    assert response.status_code == 200, response.text
    assert len(env.broker.cancel_calls) == 1, response.text
    with env.app.state.db_session_factory() as db:
        order = db.get(BrokerOrder, 'partial-rule-buy')
        assert order.status == 'canceled'
        entry = db.get(ExecutionPlanItem, env.entry_id)
        state = core_quantity.core_quantity_state(db=db, item=entry)
        assert state['entry_closed'] is True and state['remaining_quantity'] == 0
        assert state['filled_quantity'] == 4 and entry.quantity_policy['target_quantity'] == 10
        assert entry.plan.raw_payload['items'][0]['quantity_policy'] == env.original['items'][0]['quantity_policy']
        assert all(item.status != 'revoked' for item in entry.plan.items if item.side == 'sell')
    assert env.adapter.order_calls == []


def test_rule_expiry_wakes_dispatch_at_the_authority_deadline(partial_rule):
    env = partial_rule
    response = env.client.post('/execution-jobs/dispatch')
    assert response.status_code == 200, response.text
    assert datetime.fromisoformat(response.json()['next_dispatch_at']) == datetime(2026, 9, 10, 1, tzinfo=UTC)
    assert env.broker.cancel_calls == []


def test_expired_rule_cannot_reopen_with_an_immediate_late_quantity_revision(partial_rule):
    env = partial_rule
    env.clock[0] = datetime(2026, 9, 10, 1, 0, 1, tzinfo=UTC)
    revision = revision_request(env.accepted, env.original, target=10)
    revision.update(decision_snapshot_id=None, rule_origin=deepcopy(env.original['rule_origin']),
                    calculated_at=env.clock[0].isoformat(), valid_until='2026-09-10T03:00:00Z')
    response = env.client.post('/v4/execution-plans/quantities', json={'items': [revision]})
    assert response.status_code == 200, response.text
    assert response.json()['items'][0]['entry_closed'] is True, response.text
    env.client.post('/execution-jobs/dispatch')
    with env.app.state.db_session_factory() as db:
        entry = db.get(ExecutionPlanItem, env.entry_id)
        assert entry.risk_context[core_quantity.RULE_ENTRY_CLOSED_KEY]['reason_code'] == 'quantity_authority_expired'
        assert core_quantity.executable_core_quantity(db=db, item=entry) == 0
        assert core_quantity.core_quantity_state(db=db, item=entry)['remaining_quantity'] == 0
    assert len(env.broker.cancel_calls) == 1 and env.adapter.order_calls == []


def test_closed_rule_lost_cancel_ack_keeps_backoff_and_reconciles_late_fill_after_restart(partial_rule):
    env = partial_rule
    env.broker.unknown = True
    env.clock[0] = datetime(2026, 9, 10, 1, 0, 1, tzinfo=UTC)
    env.client.post('/execution-jobs/dispatch')
    assert len(env.broker.cancel_calls) == 1

    with env.app.state.db_session_factory() as db:
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction'))
        assert job.status == 'retrying' and job.attempts == 1
        retry_at = job.scheduled_for
        assert db.get(BrokerOrder, 'partial-rule-buy').status == 'cancel_pending'
    env.clock[0] += timedelta(seconds=1)
    env.client.post('/execution-jobs/dispatch')
    with env.app.state.db_session_factory() as db:
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction'))
        assert job.scheduled_for == retry_at and job.attempts == 1
    # A fresh app and DB session must reconcile, never repeat an uncertain cancel.
    restarted, engine = _build_test_app(settings=env.settings)
    restarted.state.broker_adapter_factory = lambda _settings: env.adapter
    with TestClient(restarted) as client:
        env.clock[0] += timedelta(seconds=31)
        client.post('/execution-jobs/dispatch')
        assert len(env.broker.cancel_calls) == 1
        env.broker.filled = 6
        env.broker.canceled = True
        env.clock[0] += timedelta(seconds=31)
        response = client.post('/execution-jobs/dispatch')
        assert response.status_code == 200, response.text
        with restarted.state.db_session_factory() as db:
            entry = db.get(ExecutionPlanItem, env.entry_id)
            state = core_quantity.core_quantity_state(db=db, item=entry)
            assert state['entry_closed'] and state['filled_quantity'] == 6 and state['open_quantity'] == 0
            assert state['remaining_quantity'] == 0
            assert db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction')).status == 'completed'
            assert entry.quantity_policy['target_quantity'] == 10
        response = client.post('/monitoring/evaluate', json={
            'prices': [{'symbol': '005930', 'price': '70000', 'observed_at': env.clock[0].isoformat()}],
            'observed_at': env.clock[0].isoformat(),
        })
        assert response.status_code == 200, response.text
        assert response.json()['protective_stop_coverage']['unprotected_position_count'] == 0, response.text
        with restarted.state.db_session_factory() as db:
            from app.services.plan_item_identity import plan_item_entry_family_key
            entry = db.get(ExecutionPlanItem, env.entry_id)
            family = plan_item_entry_family_key(entry)
            assert all(plan_item_entry_family_key(item) == family for item in entry.plan.items)
            assert all(item.decision_agent_name is None for item in entry.plan.items)
        assert client.get('/v4/execution-plans').json()[0]['contract_ack']['rule_origin'] == env.original['rule_origin']
    engine.dispose()
    assert len(env.broker.cancel_calls) == 1 and env.adapter.order_calls == []


def test_rule_cancel_recovery_reconciles_a_prior_session_even_when_today_is_closed(partial_rule):
    env = partial_rule
    env.broker.unknown = True
    env.clock[0] = datetime(2026, 9, 10, 1, 0, 1, tzinfo=UTC)
    env.client.post('/execution-jobs/dispatch')
    assert len(env.broker.cancel_calls) == 1
    env.broker.canceled = True
    env.clock[0] = datetime(2026, 9, 11, 22, tzinfo=UTC)
    env.adapter.is_krx_trading_day = lambda *args, **kwargs: False
    response = env.client.post('/execution-jobs/dispatch')
    assert response.status_code == 200, response.text
    with env.app.state.db_session_factory() as db:
        assert db.get(BrokerOrder, 'partial-rule-buy').status == 'canceled'
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction'))
        assert job.status == 'completed'
    assert len(env.broker.cancel_calls) == 1


@pytest.mark.parametrize(('intent', 'price'), [('target_sell', '76000'), ('stop_sell', '68000')])
def test_first_rule_protective_exit_closes_entry_before_sell_and_queues_cancel(partial_rule, intent, price):
    env = partial_rule
    wakes = []
    env.app.state.autonomous_runtime = SimpleNamespace(
        invalidate_monitoring_runtime_snapshot=lambda **kwargs: None,
        invalidate_dispatch_schedule=lambda **kwargs: wakes.append(kwargs),
    )
    # Refresh exact-family monitors after the partial fill.
    payload = lambda value: {'prices': [{'symbol': '005930', 'price': value, 'observed_at': env.clock[0].isoformat()}],
                             'observed_at': env.clock[0].isoformat()}
    assert env.client.post('/monitoring/evaluate', json=payload('70000')).status_code == 200
    assert wakes == []
    env.clock[0] = datetime(2026, 9, 10, 0, 30, tzinfo=UTC)
    env.broker.price = Decimal(price)
    original_place = env.adapter.place_order

    def observe_closure(*args, **kwargs):
        with env.app.state.db_session_factory() as db:
            entry = db.get(ExecutionPlanItem, env.entry_id)
            assert entry.risk_context[core_quantity.RULE_ENTRY_CLOSED_KEY]['reason_code'] == 'protective_exit_started'
            assert db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction')) is not None
        return original_place(*args, **kwargs)

    env.adapter.place_order = observe_closure
    response = env.client.post('/monitoring/evaluate', json=payload(price))
    assert response.status_code == 200, response.text
    assert response.json()['rule_entry_closure_count'] == 1, response.text
    assert wakes == [{'reason': 'manual_monitoring_evaluation'}]
    # The existing self-cross fence defers the SELL until its residual BUY is gone.
    assert env.adapter.order_calls == []
    assert env.broker.cancel_calls == []  # Protective worker never waits for cancellation.
    response = env.client.post('/execution-jobs/dispatch')
    assert len(env.broker.cancel_calls) == 1, response.text
    env.clock[0] += timedelta(seconds=env.settings.monitoring_watch_buy_retry_cooldown_seconds + 1)
    response = env.client.post('/monitoring/evaluate', json=payload(price))
    assert len(env.adapter.order_calls) == 1, response.text
    assert env.adapter.order_calls[0].side == 'sell' and env.adapter.order_calls[0].quantity == 4
    with env.app.state.db_session_factory() as db:
        entry = db.get(ExecutionPlanItem, env.entry_id)
        assert core_quantity.core_quantity_state(db=db, item=entry)['entry_closed'] is True
        exit_item = next(item for item in entry.plan.items if item.intent == intent)
        assert exit_item.status == 'broker_order_submitted'
        assert rule_execution.rule_entry_block_reason(settings=env.settings, db=db, plan_item=entry, now=env.clock[0]) == 'rule_entry_closed'
    assert len(env.adapter.order_calls) == 1


def test_manual_agent_monitoring_retains_existing_dispatch_cache_behavior():
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='mock')
    app, engine = _build_test_app(settings=settings)
    wakes = []
    with TestClient(app) as client:
        assert client.post('/v4/execution-plans', json=core_plan()).status_code == 201
        app.state.autonomous_runtime = SimpleNamespace(
            invalidate_monitoring_runtime_snapshot=lambda **kwargs: None,
            invalidate_dispatch_schedule=lambda **kwargs: wakes.append(kwargs),
        )
        # A pending agent plan has no filled inventory and creates no rule debt.
        response = client.post('/monitoring/evaluate', json={
            'prices': [{'symbol': '005930', 'price': '70000', 'observed_at': datetime.now(UTC).isoformat()}],
        })
        assert response.status_code == 200, response.text
        assert 'rule_entry_closure_count' not in response.json()
        assert wakes == []
    engine.dispose()


@pytest.mark.parametrize('partial_rule', ['2026-09-10T06:20:00Z'], indirect=True)
def test_close_deadline_cancels_partial_buy_then_timed_exit_sells_only_fills(partial_rule):
    env = partial_rule
    payload = lambda: {'prices': [{'symbol': '005930', 'price': '70000', 'observed_at': env.clock[0].isoformat()}],
                        'observed_at': env.clock[0].isoformat()}
    env.client.post('/monitoring/evaluate', json=payload())
    env.clock[0] = datetime(2026, 9, 10, 6, 20, tzinfo=UTC)
    response = env.client.post('/execution-jobs/dispatch')
    assert len(env.broker.cancel_calls) == 1, response.text
    with env.app.state.db_session_factory() as db:
        entry = db.get(ExecutionPlanItem, env.entry_id)
        assert entry.risk_context[core_quantity.RULE_ENTRY_CLOSED_KEY]['reason_code'] == 'close_exit_at'
    response = env.client.post('/monitoring/evaluate', json=payload())
    assert len(env.adapter.order_calls) == 1, response.text
    assert env.adapter.order_calls[0].side == 'sell' and env.adapter.order_calls[0].quantity == 4


def test_closed_rule_with_zero_open_unknown_buy_keeps_reconciliation_debt(partial_rule, monkeypatch):
    env = partial_rule
    with env.app.state.db_session_factory() as db:
        order = db.get(BrokerOrder, 'partial-rule-buy')
        order.status, order.response_metadata = 'manual_review_required', {'filled_quantity': 10}
        db.commit()
    # Inquiry has not yet provided terminal broker truth for this uncertain intent.
    monkeypatch.setattr(reconciliation, 'reconcile_broker_order', lambda **kwargs: {'status_code': 200})
    env.clock[0] = datetime(2026, 9, 10, 1, 0, 1, tzinfo=UTC)
    response = env.client.post('/execution-jobs/dispatch')
    assert response.status_code == 200, response.text
    with env.app.state.db_session_factory() as db:
        entry = db.get(ExecutionPlanItem, env.entry_id)
        state = core_quantity.core_quantity_state(db=db, item=entry)
        assert state['entry_closed'] and state['remaining_quantity'] == state['open_quantity'] == 0
        assert state['submission_uncertain'] is True
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction'))
        assert job.status == 'retrying' and job.last_error == 'quantity_followup_submission_uncertain'
    assert env.broker.cancel_calls == []


def test_rule_closure_survives_saturated_risk_compaction_and_invalidates_cached_dispatch(monkeypatch):
    from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
    from app.services.json_compaction import compact_json_object

    closure = {'schema_version': 'rule_entry_closed_v1', 'reason_code': 'protective_exit_started'}
    risk = {**{f'noise_{index}': index for index in range(100)}, core_quantity.RULE_ENTRY_CLOSED_KEY: closure}
    assert compact_json_object(risk, max_dict_items=10)[core_quantity.RULE_ENTRY_CLOSED_KEY] == closure
    coordinator = AutonomousRuntimeCoordinator(settings=Settings(database_url='sqlite:///:memory:'),
        session_factory=None, broker_adapter_factory=lambda _settings: None)
    now = datetime(2026, 9, 10, 0, 30, tzinfo=UTC)
    coordinator._dispatch_next_db_check_at = now + timedelta(seconds=60)
    generation = coordinator._dispatch_schedule_generation
    monkeypatch.setattr(coordinator, '_monitoring_cycle_sync', lambda _started_at: {'rule_entry_closure_count': 1})
    coordinator._monitoring_cycle_sync_guarded(now)
    assert coordinator._dispatch_next_db_check_at is None
    assert coordinator._dispatch_schedule_generation == generation + 1


@pytest.mark.parametrize('holiday', [False, True])
@pytest.mark.parametrize('broker_terminal', [False, True])
def test_closed_rule_reconciliation_is_not_hidden_by_older_ineligible_agent_job(partial_rule, holiday, broker_terminal):
    env = partial_rule
    original = core_plan()
    original['plan_id'] = 'older-agent-plan'
    original['items'][0]['core_item_key'] = 'older-agent-entry'
    for item in original['items'][1:]:
        item['core_item_key'] = f"older-agent-{item['intent']}"
        item['quantity_policy']['linked_entry_item_key'] = 'older-agent-entry'
    for item in original['items']:
        item['source_plan_item_id'] = item['core_item_key']
    _refresh_material_fingerprint(original)
    response = env.client.post('/v4/execution-plans', json=original)
    assert response.status_code == 201, response.text
    with env.app.state.db_session_factory() as db:
        entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.source_plan_item_id == 'older-agent-entry'))
        entry.status = 'core_activation_pending'
        for job in entry.jobs:
            job.status = 'completed'
        job = ExecutionJob(plan_item=entry, job_type='core_quantity_reduction', status='queued', scheduled_for=env.clock[0])
        db.add(job)
        db.commit()
        db.refresh(job)
        ordinary_id = job.id
        ordinary_state = (job.status, job.attempts, job.scheduled_for, job.last_error)
    env.clock[0] = datetime(2026, 9, 10, 2, tzinfo=UTC) if holiday else datetime(2026, 9, 10, 6, 31, tzinfo=UTC)
    env.adapter.is_krx_trading_day = lambda *args, **kwargs: not holiday
    env.broker.canceled = broker_terminal
    response = env.client.post('/execution-jobs/dispatch')
    assert response.status_code == 200, response.text
    assert response.json()['quantity_reduction_processed_count'] == 1, response.text
    env.clock[0] += timedelta(seconds=31)
    assert env.client.post('/execution-jobs/dispatch').status_code == 200
    with env.app.state.db_session_factory() as db:
        ordinary = db.get(ExecutionJob, ordinary_id)
        assert (ordinary.status, ordinary.attempts, ordinary.scheduled_for, ordinary.last_error) == ordinary_state
        entry = db.get(ExecutionPlanItem, env.entry_id)
        state = core_quantity.core_quantity_state(db=db, item=entry)
        job = db.scalar(select(ExecutionJob).where(
            ExecutionJob.execution_plan_item_id == env.entry_id, ExecutionJob.job_type == 'core_quantity_reduction'))
        assert state['entry_closed'] and state['remaining_quantity'] == 0
        assert state['filled_quantity'] == 4
        if broker_terminal:
            assert db.get(BrokerOrder, 'partial-rule-buy').status == 'canceled'
            assert state['open_quantity'] == 0 and job.status == 'completed'
        else:
            assert state['open_quantity'] == 6 and state['submission_uncertain'] is True
            assert job.status == 'retrying' and job.last_error == 'rule_cancel_market_session_closed'
    assert env.broker.cancel_calls == [] and env.adapter.order_calls == []
