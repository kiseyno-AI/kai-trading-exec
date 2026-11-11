from copy import deepcopy
from datetime import UTC, datetime

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlanItem
from app.core.config import Settings
from app.services import core_quantity
from app.services import exec_agent, trigger_time_exec_agent
from test_core_handoff_v4 import _build_test_app, _plan_payload, _refresh_material_fingerprint


def core_plan():
    payload = _plan_payload(plan_id='core-quantity-plan')
    payload['items'][0]['quantity_policy'] = {
        'type': 'core_sized', 'target_quantity': 10, 'revision': 1,
        'calculated_at': '2026-09-10T00:00:00Z', 'valid_until': '2026-09-10T06:30:00Z',
        'sizing_inputs': {'price_contract_digest': 'a' * 64, 'sizing_price': '70000'},
    }
    _refresh_material_fingerprint(payload)
    return payload


def test_core_owned_quantity_intake_is_executable_without_agent_sizing():
    app, engine = _build_test_app()
    with TestClient(app) as client:
        response = client.post('/v4/execution-plans', json=core_plan())
        assert response.status_code == 201, response.json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert entry.quantity_policy['type'] == 'core_sized'
            assert entry.status == 'next_open_buy_scheduled'
        readback = client.get('/v4/execution-plans', params={'external_plan_id': 'core-quantity-plan'})
        assert readback.json()[0]['contract_ack'] == response.json()['contract_ack']
    engine.dispose()


def revision_request(accepted, original, *, target, revision=2):
    policy = deepcopy(original['items'][0]['quantity_policy'])
    policy.update(revision=revision, target_quantity=target, calculated_at='2026-09-10T02:00:00Z')
    return {
        **policy, 'external_plan_id': original['plan_id'],
        'execution_plan_id': accepted['execution_plan_id'],
        'core_item_key': original['items'][0]['core_item_key'],
        'decision_snapshot_id': '101', 'expected_revision': revision - 1,
    }


def test_quantity_targets_count_fills_and_open_orders_without_selling_filled_shares(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            order = BrokerOrder(
                id='partial-buy', execution_plan_item_id=entry.id, broker_name='kis', mode='mock',
                idempotency_key='first-buy', broker_order_id='broker-1', symbol=entry.symbol,
                side='buy', status='partially_filled', quantity=7,
            )
            order.fills.append(ExecutionFill(
                id='first-fill', symbol=entry.symbol, side='buy', quantity=4, price=70000,
                filled_at=datetime(2026, 9, 10, 1, tzinfo=UTC),
            ))
            db.add(order)
            db.commit()
        state = client.get('/v4/execution-plans').json()[0]['item_states'][0]['core_quantity_state']
        assert (state['filled_quantity'], state['open_quantity'], state['remaining_quantity']) == (4, 3, 3)
        update = revision_request(accepted, original, target=2)
        result = client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()
        assert result['updated_count'] == 1
        assert result['items'][0]['remaining_quantity'] == 0
        assert result['items'][0]['reason_code'] == 'reduction_deferred_open_orders'
        update = revision_request(accepted, original, target=12, revision=3)
        result = client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()
        assert result['items'][0]['remaining_quantity'] == 5
        with app.state.db_session_factory() as db:
            order = db.get(BrokerOrder, 'partial-buy')
            order.status = 'manual_review_required'
            db.commit()
        update = revision_request(accepted, original, target=20, revision=4)
        result = client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()
        assert result['items'][0]['reason_code'] == 'quantity_submission_uncertain'
        update['target_quantity'] = 1
        result = client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()
        assert result['updated_count'] == 1
        with app.state.db_session_factory() as db:
            orders = db.scalars(select(BrokerOrder)).all()
            assert len(orders) == 1 and orders[0].side == 'buy' and orders[0].quantity == 7
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert core_quantity.executable_core_quantity(db=db, item=entry) == 0
    engine.dispose()


def test_revision_rejects_price_fields_and_isolates_binding_failure(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        update = revision_request(accepted, original, target=6)
        invalid = {**update, 'decision_snapshot_id': 'wrong-snapshot'}
        result = client.post('/v4/execution-plans/quantities', json={'items': [invalid, update]}).json()
        assert (result['updated_count'], result['failed_count']) == (1, 1)
        invalid = {**update, 'trigger_price': 1}
        assert client.post('/v4/execution-plans/quantities', json={'items': [invalid]}).status_code == 422
        invalid = revision_request(accepted, original, target=7, revision=3)
        invalid['sizing_inputs']['price_contract_digest'] = 'b' * 64
        assert client.post('/v4/execution-plans/quantities', json={'items': [invalid]}).json()['failed_count'] == 1
    engine.dispose()


def test_core_size_final_order_boundary_rejects_excess_or_stale_revision(monkeypatch):
    from app.services import broker_orders
    monkeypatch.setattr(broker_orders, 'core_buy_authority_block_reason', lambda **kwargs: None)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        accepted = client.post('/v4/execution-plans', json=core_plan()).json()
        assert client.post('/controls/resume').status_code == 200
        entry_id = accepted['item_acks'][0]['execution_plan_item_id']
        command = {
            'idempotency_key': 'core-quantity-test-order', 'execution_plan_item_id': entry_id,
            'provider': 'kis', 'asset_class': 'stock', 'venue': 'kis', 'symbol': '005930',
            'side': 'buy', 'quantity': 11, 'order_type': 'market', 'exchange': 'KRX',
            'time_in_force': 'day', 'metadata': {'core_quantity_revision': 1},
        }
        response = client.post('/orders', json=command)
        assert response.status_code == 422, response.json()
        assert 'Core cumulative buy quantity authority' in response.json()['message']
        command['quantity'] = 1
        command['metadata']['core_quantity_revision'] = 0
        response = client.post('/orders', json=command)
        assert response.status_code == 422, response.json()
        with app.state.db_session_factory() as db:
            assert db.scalars(select(BrokerOrder)).all() == []
    engine.dispose()


def test_core_owned_short_term_intake_and_protective_exits_never_call_agents(monkeypatch):
    def forbidden(**kwargs):
        raise AssertionError('Core-owned size must not invoke Exec sizing')
    monkeypatch.setattr(exec_agent, '_load_account_context', forbidden)
    app, engine = _build_test_app(settings=Settings(database_url='sqlite:///:memory:', runtime_mode='mock', exec_agent_enabled=True))
    original = core_plan()
    original['metadata']['agent_name'] = 'short-term-decider'
    original['metadata']['decision_agent_name'] = 'short-term-decider'
    for item in original['items']:
        item['agent_name'] = 'short-term-decider'
        item['decision_agent_name'] = 'short-term-decider'
    _refresh_material_fingerprint(original)
    with TestClient(app) as client:
        response = client.post('/v4/execution-plans', json=original)
        assert response.status_code == 201, response.json()
        with app.state.db_session_factory() as db:
            entries = db.scalars(select(ExecutionPlanItem)).all()
            assert all(not trigger_time_exec_agent.trigger_time_exec_agent_enabled(settings=app.state.settings, item=item) for item in entries)
    engine.dispose()


@pytest.mark.parametrize('status', ['canceled', 'expired', 'rejected'])
def test_broker_reported_partial_fill_consumes_lifetime_target_without_fill_rows(status, monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        accepted = client.post('/v4/execution-plans', json=core_plan()).json()
        entry_id = accepted['item_acks'][0]['execution_plan_item_id']
        with app.state.db_session_factory() as db:
            db.add(BrokerOrder(
                execution_plan_item_id=entry_id, broker_name='kis', mode='mock', idempotency_key='partial-no-detail',
                broker_order_id='broker-canceled', symbol='005930', side='buy', status=status, quantity=10,
                response_metadata={'filled_quantity': 4},
            ))
            db.commit()
        state = client.get('/v4/execution-plans').json()[0]['item_states'][0]['core_quantity_state']
        assert (state['filled_quantity'], state['remaining_quantity']) == (4, 6)
        with app.state.db_session_factory() as db:
            order = db.scalar(select(BrokerOrder))
            order.response_metadata = {'filled_quantity': 11}
            db.commit()
        state = client.get('/v4/execution-plans').json()[0]['item_states'][0]['core_quantity_state']
        assert state['submission_uncertain'] is True and state['remaining_quantity'] == 0
    engine.dispose()


def test_quantity_revision_is_bound_idempotent_and_does_not_change_prices(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        policy = deepcopy(original['items'][0]['quantity_policy'])
        policy.update(revision=2, target_quantity=6, calculated_at='2026-09-10T02:00:00Z')
        update = {
            **policy, 'external_plan_id': original['plan_id'],
            'execution_plan_id': accepted['execution_plan_id'],
            'core_item_key': original['items'][0]['core_item_key'],
            'decision_snapshot_id': '101', 'expected_revision': 1,
        }
        response = client.post('/v4/execution-plans/quantities', json={'items': [update]})
        assert response.status_code == 200, response.json()
        assert response.json()['updated_count'] == 1
        assert response.json()['items'][0]['remaining_quantity'] == 6
        replay = client.post('/v4/execution-plans/quantities', json={'items': [update]})
        assert replay.json()['unchanged_count'] == 1
        conflict = deepcopy(update)
        conflict['target_quantity'] = 7
        assert client.post('/v4/execution-plans/quantities', json={'items': [conflict]}).json()['failed_count'] == 1
        readback = client.get('/v4/execution-plans', params={'external_plan_id': original['plan_id']}).json()[0]
        assert readback['contract_ack'] == accepted['contract_ack']
        assert readback['item_states'][0]['core_quantity_state']['target_quantity'] == 6
        with app.state.db_session_factory() as db:
            entries = db.scalars(select(ExecutionPlanItem).order_by(ExecutionPlanItem.plan_item_index)).all()
            assert entries[1].trigger_price == 72000
            assert entries[2].trigger_price == 72000
    engine.dispose()


def test_quantity_revision_replay_survives_expiry_but_cannot_change_entry_ceiling(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        update = revision_request(accepted, original, target=6)
        assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['updated_count'] == 1
        changed = revision_request(accepted, original, target=7, revision=3)
        changed['sizing_inputs']['sizing_price'] = '80000'
        assert client.post('/v4/execution-plans/quantities', json={'items': [changed]}).json()['items'][0]['reason_code'] == 'quantity_entry_price_changed'
        monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 11, 2, tzinfo=UTC))
        assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['unchanged_count'] == 1
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert core_quantity.executable_core_quantity(db=db, item=entry) == 0
    engine.dispose()


@pytest.mark.parametrize(('ask', 'bid', 'target', 'reason'), [
    ('130', '129', '140', 'live_buy_core_entry_ceiling_exceeded'),
    ('100', '90', '125', 'live_buy_net_reward_risk_below_minimum'),
])
def test_core_next_open_rechecks_original_entry_ceiling_and_real_bid_ask_costs(ask, bid, target, reason):
    from decimal import Decimal
    from app.api.schemas.execution import PlaceOrderRequest
    from app.broker.base import BrokerLiveMarketContext
    from app.models.runtime import ExecutionPlan
    from app.services import broker_orders, monitoring

    now = datetime(2026, 9, 10, 2, tzinfo=UTC)
    raw = core_plan()
    raw['items'][0]['quantity_policy']['sizing_inputs']['sizing_price'] = '100'
    raw['items'][1]['trigger_price'] = target
    raw['items'][2]['trigger_price'] = '95'
    item = ExecutionPlanItem(
        id='core-price-check', source_plan_item_id=raw['items'][0]['core_item_key'],
        plan=ExecutionPlan(external_plan_id='price-check', source='kai-trading-core', mode='live', market_date=now.date(), raw_payload=raw),
        symbol='005930', side='buy', intent='next_open_buy', provider='kis', asset_class='stock',
        quantity_policy=raw['items'][0]['quantity_policy'],
    )
    command = PlaceOrderRequest(
        idempotency_key='core-price-check', execution_plan_item_id=item.id,
        provider='kis', asset_class='stock', symbol='005930', side='buy',
        quantity=1, order_type='market', time_in_force='day', exchange='KRX',
    )
    evidence = broker_orders.AutomatedKisBuyMarketEvidence(is_domestic=True, context=BrokerLiveMarketContext(
        symbol='005930', exchange='KRX', observed_at=now, bid=Decimal(bid), ask=Decimal(ask),
        freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True,
    ))
    settings = Settings(runtime_mode='live', kis_krx_round_trip_non_spread_cost_bps=Decimal('50'))
    failure = broker_orders._automated_kis_buy_market_evidence_failure(
        settings=settings, command=command, plan_item=item, evidence=evidence, reference_at=now,
    )
    assert failure[0] == reason
    assert monitoring._post_entry_initial_stop_price(settings=settings, entry_item=item, entry_price=Decimal('100')) == Decimal('95')


def test_runtime_sizing_account_counts_prior_date_local_intents_once_and_excludes_foreign_scope():
    from decimal import Decimal
    from test_broker_auth import _test_kis_execution_scope

    settings = Settings(
        database_url='sqlite:///:memory:', runtime_mode='mock', kis_account_number='12345678',
        kis_account_product_code='01', account_drawdown_recovery_scope_secret='quantity-account-test-secret-at-least-32-bytes',
        exec_agent_max_symbol_exposure_pct='0.10',
    )
    app, engine = _build_test_app(settings=settings)
    with app.state.db_session_factory() as db:
        empty = core_quantity.runtime_core_quantity_account(db=db, settings=settings, scope_available=True)
        assert empty['account_submission_uncertain'] is False
        assert empty['limits']['max_total_budget_krw'] == str(settings.exec_agent_max_total_budget)
        assert Decimal(empty['limits']['max_symbol_fraction']) == Decimal('0.10')
        for number, account in enumerate(['12345678', '99999999']):
            scope = _test_kis_execution_scope(settings.model_copy(update={'kis_account_number': account}), venue='kis')
            db.add(BrokerOrder(
                broker_name='kis', mode='mock', idempotency_key=f'old-local-{number}', symbol='005930',
                venue='kis', asset_class='stock', side='buy', status='submission_pending',
                quantity=4, limit_price=Decimal('100'), created_at=datetime(2026, 9, 9, tzinfo=UTC),
                request_metadata={'execution_scope': scope},
            ))
        db.commit()
        account = core_quantity.runtime_core_quantity_account(db=db, settings=settings, scope_available=True)
        assert account['account_submission_uncertain'] is True
        assert Decimal(account['account_local_unacknowledged_buy_notional_krw']) == 400
    engine.dispose()


def test_exit_started_closes_only_its_entry_family_buy_capacity(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            stop = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'stop_sell'))
            db.add(BrokerOrder(execution_plan_item_id=entry.id, broker_name='kis', mode='mock', idempotency_key='initial-four', broker_order_id='filled-four', symbol=entry.symbol, side='buy', quantity=4, status='filled'))
            stop.status = 'stop_sell_triggered_pending_dispatch'
            db.commit()
        result = client.post('/v4/execution-plans/quantities', json={'items': [revision_request(accepted, original, target=12)]}).json()
        assert result['updated_count'] == 1
        state = result['items'][0]
        assert state['filled_quantity'] == 4 and state['remaining_quantity'] == 0 and state['entry_closed'] is True
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert core_quantity.executable_core_quantity(db=db, item=entry) == 0
    engine.dispose()


def test_final_quantity_boundary_excludes_only_its_fresh_local_intent(monkeypatch):
    from app.api.schemas.execution import PlaceOrderRequest

    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime(2026, 9, 10, 2, tzinfo=UTC))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            current = BrokerOrder(id='current-local-intent', execution_plan_item_id=entry.id, broker_name='kis', mode='mock', idempotency_key='current-local', symbol=entry.symbol, side='buy', quantity=10, status='submission_pending')
            db.add(current)
            db.commit()
            command = PlaceOrderRequest(idempotency_key='current-local', execution_plan_item_id=entry.id, provider='kis', asset_class='stock', symbol=entry.symbol, side='buy', quantity=10, order_type='market', time_in_force='day', exchange='KRX', metadata={'core_quantity_revision': 1})
            assert core_quantity.core_quantity_order_failure(db=db, item=entry, command=command, exclude_order_id=current.id) is None
            current.last_error_classification = 'submit_outcome_unknown'
            db.commit()
            assert core_quantity.core_quantity_order_failure(db=db, item=entry, command=command, exclude_order_id=current.id) is not None
            current.last_error_classification = None
            db.commit()
        result = client.post('/v4/execution-plans/quantities', json={'items': [revision_request(accepted, original, target=5)]}).json()
        assert result['updated_count'] == 1
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert core_quantity.core_quantity_order_failure(db=db, item=entry, command=command, exclude_order_id='current-local-intent') is not None
    engine.dispose()


@pytest.mark.parametrize('future_calculated_at', [False, True])
def test_running_next_open_job_keeps_latest_revision_claimable_when_old_command_is_rejected(monkeypatch, future_calculated_at):
    from datetime import timedelta
    from types import SimpleNamespace
    from app.api.schemas.execution import CoreQuantityRevisionRequest
    from app.models.runtime import ExecutionJob
    from app.services import execution_jobs
    from app.services.broker_orders import PlaceBrokerOrderResult

    claimed_at = datetime(2026, 9, 10, 2, tzinfo=UTC)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: claimed_at + timedelta(seconds=3))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        request = revision_request(accepted, original, target=12)
        request['calculated_at'] = (claimed_at + timedelta(seconds=20 if future_calculated_at else 1)).isoformat()

        def revise_before_final_lock(*, db, command, **kwargs):
            result = core_quantity.revise_core_quantities(
                settings=app.state.settings, db=db,
                command=CoreQuantityRevisionRequest.model_validate({'items': [request]}),
            )
            assert result['updated_count'] == 1, result

        def reject_old_command(*, command, **kwargs):
            assert command.metadata['core_quantity_revision'] == 1
            return PlaceBrokerOrderResult(status_code=422, payload={'message': 'revision changed before broker intent'})

        monkeypatch.setattr(execution_jobs, 'place_broker_order', reject_old_command)
        monkeypatch.setattr(execution_jobs, '_verified_agent_next_open_quote_guard', revise_before_final_lock)
        with app.state.db_session_factory() as db:
            job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            job.status, job.locked_at = 'running', claimed_at
            job.plan_item.status = 'next_open_buy_dispatching'
            db.commit()
            status = execution_jobs._dispatch_job(
                settings=app.state.settings, db=db, adapter=SimpleNamespace(provider='kis'),
                job_id=job.id, claim_locked_at=claimed_at,
            )
            assert status == 'retrying'
            db.refresh(job)
            assert job.plan_item.status == 'next_open_buy_retry_pending'
            assert job.plan_item.quantity_policy['revision'] == 2, job.last_error
            if future_calculated_at:
                assert job.scheduled_for.replace(tzinfo=UTC) == claimed_at + timedelta(seconds=20)
            assert db.scalars(select(BrokerOrder)).all() == []
    engine.dispose()


@pytest.mark.parametrize('initial_policy', [False, True])
def test_future_dated_core_quantity_retries_at_validity_start_then_submits_once(monkeypatch, initial_policy):
    from datetime import timedelta
    from types import SimpleNamespace
    from app.models.runtime import ExecutionJob
    from app.services import execution_jobs
    from app.services.broker_orders import PlaceBrokerOrderResult

    clock = [datetime(2026, 9, 10, 2, tzinfo=UTC)]
    begins_at = clock[0] + timedelta(seconds=20)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: clock[0])
    submitted = []

    def submit(*, db, command, **kwargs):
        submitted.append(command)
        order = BrokerOrder(execution_plan_item_id=command.execution_plan_item_id, broker_name='kis', mode='mock', idempotency_key=command.idempotency_key, broker_order_id='future-fill', symbol=command.symbol, side='buy', quantity=command.quantity, status='filled')
        db.add(order)
        db.commit()
        return PlaceBrokerOrderResult(status_code=200, payload={'order': {'broker_order_pk': order.id, 'status': 'filled'}})

    monkeypatch.setattr(execution_jobs, 'place_broker_order', submit)
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = core_plan()
        if initial_policy:
            original['items'][0]['quantity_policy']['calculated_at'] = begins_at.isoformat()
            _refresh_material_fingerprint(original)
        accepted = client.post('/v4/execution-plans', json=original).json()
        if not initial_policy:
            update = revision_request(accepted, original, target=12)
            update['calculated_at'] = begins_at.isoformat()
            assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['updated_count'] == 1
        with app.state.db_session_factory() as db:
            job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            job.status, job.locked_at, job.plan_item.status = 'running', clock[0], 'next_open_buy_dispatching'
            db.commit()
            assert execution_jobs._dispatch_job(settings=app.state.settings, db=db, adapter=SimpleNamespace(provider='kis'), job_id=job.id, claim_locked_at=clock[0]) == 'retrying'
            db.refresh(job)
            assert job.scheduled_for.replace(tzinfo=UTC) == begins_at
            assert core_quantity.executable_core_quantity(db=db, item=job.plan_item) == 0 and submitted == []
            clock[0] += timedelta(seconds=21)
            job.status, job.locked_at, job.plan_item.status = 'running', clock[0], 'next_open_buy_dispatching'
            db.commit()
            assert execution_jobs._dispatch_job(settings=app.state.settings, db=db, adapter=SimpleNamespace(provider='kis'), job_id=job.id, claim_locked_at=clock[0]) == 'completed'
            assert len(submitted) == 1 and submitted[0].quantity == (10 if initial_policy else 12)
            assert submitted[0].metadata['core_quantity_revision'] == (1 if initial_policy else 2)
            assert core_quantity.executable_core_quantity(db=db, item=job.plan_item) == 0
    engine.dispose()


def test_future_dated_watch_keeps_retryable_monitoring_without_early_quantity(monkeypatch):
    from datetime import timedelta
    from decimal import Decimal
    from types import SimpleNamespace
    from app.services import monitoring

    clock = [datetime(2026, 9, 10, 2, tzinfo=UTC)]
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: clock[0])
    original = core_plan()
    original['items'][0]['intent'] = 'intraday_watch_buy'
    original['items'][0]['trigger_price'] = '70000'
    original['items'][0]['quantity_policy']['calculated_at'] = (clock[0] + timedelta(seconds=20)).isoformat()
    _refresh_material_fingerprint(original)
    app, engine = _build_test_app()
    with TestClient(app) as client:
        response = client.post('/v4/execution-plans', json=original)
        assert response.status_code == 201, response.json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            before = entry.status
            approval = monitoring._resolve_watch_buy_order(settings=app.state.settings, db=db, adapter=SimpleNamespace(provider='kis'), item=entry, observed_price=Decimal('70000'))
            assert approval.quantity is None and approval.reason in monitoring.RETRYABLE_WATCH_BUY_APPROVAL_REASONS
            assert entry.status == before and before == 'watch_monitoring_active'
            clock[0] += timedelta(seconds=21)
            assert core_quantity.executable_core_quantity(db=db, item=entry) == 10
            assert entry.status == before
    engine.dispose()
