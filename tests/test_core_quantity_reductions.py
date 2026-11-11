from datetime import UTC, datetime
from types import SimpleNamespace
from datetime import timedelta
from contextlib import contextmanager
from copy import deepcopy

import pytest

from fastapi.testclient import TestClient
from sqlalchemy import select, update as sql_update

from app.broker.base import BrokerAdapterError
from app.broker.kis import KisBrokerAdapter
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionJob, ExecutionPlan, ExecutionPlanItem
from app.services import broker_auth, core_quantity, core_quantity_reductions, reconciliation, core_revocation_exposure
from test_broker_auth import RecordingHttpClient, _test_kis_execution_scope
from test_core_handoff_v4 import _build_test_app
from test_core_quantity import core_plan, revision_request


def test_kis_cancel_checks_current_cancelable_order_and_cancels_only_remaining_buy():
    http = RecordingHttpClient(
        {'rt_cd': '0', 'output': {'ODNO': 'cancel-ticket'}},
        get_response={'rt_cd': '0', 'output': [{
            'odno': 'original-1', 'pdno': '005930', 'psbl_qty': '3',
            'krx_fwdg_ord_orgno': 'branch-1', 'ord_dvsn_cd': '00',
            'sll_buy_dvsn_cd': '02',
        }]},
    )
    adapter = KisBrokerAdapter(settings=Settings(
        runtime_mode='live', kis_app_key='fake-key', kis_app_secret='fake-secret',
        kis_account_number='12345678', kis_use_hashkey=False,
    ), http_client=http)
    result = adapter.cancel_order('fake-token', broker_order_id='original-1', symbol='005930')
    assert result['status'] == 'accepted'
    assert len(http.get_calls) == len(http.calls) == 1
    assert http.get_calls[0]['headers']['tr_id'] == 'TTTC0084R'
    assert http.calls[0]['headers']['tr_id'] == 'TTTC0013U'
    assert http.calls[0]['payload']['ORGN_ODNO'] == 'original-1'
    assert http.calls[0]['payload']['RVSE_CNCL_DVSN_CD'] == '02'
    assert http.calls[0]['payload']['QTY_ALL_ORD_YN'] == 'Y'
    assert http.calls[0]['payload']['ORD_QTY'] == '3'
    assert adapter.cancel_order('fake-token', broker_order_id='another-order', symbol='005930')['status'] == 'not_cancelable'
    assert len(http.calls) == 1


@pytest.mark.parametrize('broker_side', ['01', None])
def test_kis_cancel_rejects_sell_or_missing_broker_side_without_cancel_post(broker_side):
    row = {
        'odno': 'original-1', 'pdno': '005930', 'psbl_qty': '3',
        'krx_fwdg_ord_orgno': 'branch-1', 'ord_dvsn_cd': '00',
    }
    if broker_side is not None:
        row['sll_buy_dvsn_cd'] = broker_side
    http = RecordingHttpClient(
        {'rt_cd': '0', 'output': {'ODNO': 'cancel-ticket'}},
        get_response={'rt_cd': '0', 'output': [row]},
    )
    adapter = KisBrokerAdapter(settings=Settings(
        runtime_mode='live', kis_app_key='fake-key', kis_app_secret='fake-secret',
        kis_account_number='12345678', kis_use_hashkey=False,
    ), http_client=http)

    with pytest.raises(BrokerAdapterError, match='positively identified as a BUY'):
        adapter.cancel_order('fake-token', broker_order_id='original-1', symbol='005930')

    assert len(http.get_calls) == 1
    assert http.calls == []


@pytest.mark.parametrize('outcome', ['success', 'unknown', 'foreign_account', 'inquiry_failure', 'temporarily_not_cancelable', 'lost_claim', 'raced_intent', 'stale_job'])
def test_reduction_is_queued_then_cancelled_and_reconciled_without_selling_fills(monkeypatch, outcome):
    now = datetime(2026, 9, 10, 2, tzinfo=UTC)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: now)
    monkeypatch.setattr(core_quantity_reductions, 'utc_now', lambda: now)
    monkeypatch.setattr(broker_auth, 'ensure_authenticated_broker_session', lambda **kwargs: SimpleNamespace(access_token='fake'))
    adapter = SimpleNamespace(provider='kis', cancelled=False, calls=0)

    def reconcile(*, order, db, **kwargs):
        if outcome == 'inquiry_failure':
            return {'status_code': 503}
        if adapter.cancelled:
            order.status = 'canceled'
            order.response_metadata = {'filled_quantity': 4}
            db.commit()
        return {'status_code': 200}

    def cancel(*args, **kwargs):
        adapter.calls += 1
        if outcome == 'unknown':
            raise TimeoutError('lost response')
        if outcome == 'temporarily_not_cancelable' and adapter.calls == 1:
            return {'status': 'not_cancelable'}
        adapter.cancelled = True
        assert kwargs['broker_order_id'] == 'source-buy'
        return {'status': 'accepted'}

    adapter.cancel_order = cancel
    monkeypatch.setattr(reconciliation, 'reconcile_broker_order', reconcile)
    app, engine = _build_test_app()
    with TestClient(app) as client:
        plan = core_plan()
        accepted = client.post('/v4/execution-plans', json=plan).json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            db.add(BrokerOrder(
                execution_plan_item_id=entry.id, broker_name='kis', mode='mock', idempotency_key='old-buy',
                broker_order_id='source-buy', symbol='005930', side='buy', status='partially_filled', quantity=10,
                venue='kis', asset_class='stock',
                request_metadata={'execution_scope': _test_kis_execution_scope(app.state.settings, venue='kis')},
                response_metadata={'filled_quantity': 4},
            ))
            db.commit()
        result = client.post('/v4/execution-plans/quantities', json={'items': [revision_request(accepted, plan, target=6)]}).json()
        assert result['updated_count'] == 1
        assert adapter.calls == 0
        if outcome == 'foreign_account':
            app.state.settings = app.state.settings.model_copy(update={'kis_account_number': '99999999'})
        if outcome in {'lost_claim', 'raced_intent'}:
            original_lock = core_revocation_exposure.serialized_revocation_exposure

            @contextmanager
            def racing_lock(*, db, **kwargs):
                with original_lock(db=db, **kwargs) as acquired:
                    if outcome == 'lost_claim':
                        job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE))
                        job.locked_at = now + timedelta(seconds=1)
                    else:
                        order = db.scalar(select(BrokerOrder))
                        db.add(BrokerOrderEvent(
                            broker_order_pk=order.id, event_type=core_quantity_reductions.CANCEL_INTENT,
                            event_source='another-worker', normalized_status='cancel_pending', observed_at=now,
                        ))
                    db.commit()
                    yield acquired

            monkeypatch.setattr(core_revocation_exposure, 'serialized_revocation_exposure', racing_lock)
        with app.state.db_session_factory() as db:
            if outcome == 'stale_job':
                stale = ExecutionPlanItem(
                    id='stale-entry', symbol='005930', side='buy', intent='next_open_buy',
                    plan=ExecutionPlan(external_plan_id='yesterday', source='test', mode='mock', market_date=now.date() - timedelta(days=1)),
                    quantity_policy=plan['items'][0]['quantity_policy'],
                )
                db.add(ExecutionJob(id='stale-job', plan_item=stale, job_type=core_quantity_reductions.JOB_TYPE, status='queued', scheduled_for=now - timedelta(days=1)))
                db.commit()
            core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=now, krx_trading_day=lambda day: True)
            job = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE, ExecutionJob.id != 'stale-job'))
            if outcome == 'lost_claim':
                assert job.status == 'running' and adapter.calls == 0
                return
            if outcome not in {'success', 'stale_job'}:
                assert job.status == 'retrying'
                core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=now + timedelta(seconds=31), krx_trading_day=lambda day: True)
                db.refresh(job)
                if outcome != 'temporarily_not_cancelable':
                    assert job.status == 'retrying'
                    assert adapter.calls == (1 if outcome == 'unknown' else 0)
                    return
            assert job.status == 'completed'
            if outcome == 'stale_job':
                assert db.get(ExecutionJob, 'stale-job').status == 'queued'
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert core_quantity.core_quantity_state(db=db, item=entry)['remaining_quantity'] == 2
            next_open = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            assert next_open.status == 'scheduled'
            assert core_quantity.executable_core_quantity(db=db, item=entry) == 2
            orders = db.scalars(select(BrokerOrder)).all()
            assert len(orders) == 1 and orders[0].side == 'buy' and orders[0].status == 'canceled'
        assert adapter.calls == (2 if outcome == 'temporarily_not_cancelable' else 1)
    engine.dispose()


def test_blocking_quantity_cancellation_runs_independently_from_protective_monitoring(monkeypatch):
    from threading import Event, Thread
    from app.services import execution_jobs, monitoring

    started, release = Event(), Event()
    now = datetime(2026, 9, 10, 2, tzinfo=UTC)

    def blocked_cancel(**kwargs):
        started.set()
        assert release.wait(timeout=3)
        return 0

    monkeypatch.setattr(core_quantity_reductions, 'process_core_quantity_reductions', blocked_cancel)
    app, engine = _build_test_app()
    adapter = SimpleNamespace(provider='kis')
    errors = []
    with TestClient(app) as client:
        assert client.post('/controls/resume').status_code == 200

        def dispatch():
            try:
                with app.state.db_session_factory() as db:
                    execution_jobs.dispatch_due_execution_jobs(settings=app.state.settings, db=db, adapter=adapter, now=now)
            except BaseException as exc:
                errors.append(exc)

        worker = Thread(target=dispatch)
        worker.start()
        try:
            assert started.wait(timeout=2)
            with app.state.db_session_factory() as db:
                result = monitoring._evaluate_monitoring_cycle(
                    settings=app.state.settings, db=db, adapter=adapter,
                    prices={}, triggered_at=now, allow_persisted_triggered_without_price=False,
                    update_checkpoint_if_idle=False,
                )
                assert result.status_code == 200
            assert worker.is_alive()
        finally:
            release.set()
            worker.join(timeout=3)
        assert errors == [] and not worker.is_alive()
    engine.dispose()


@pytest.mark.parametrize(('terminal_status', 'filled_count'), [('filled', 10), ('canceled', 4)])
@pytest.mark.parametrize('lifecycle', ['active', 'closed', 'expired', 'running', 'newer_revision', 'repair_replay', 'revoked_during_reconcile'])
def test_upward_revision_waits_durably_for_open_buy_then_rearms_latest_remainder_once(monkeypatch, terminal_status, filled_count, lifecycle):
    from app.services import execution_jobs

    clock = [datetime(2026, 9, 10, 2, tzinfo=UTC)]
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: clock[0])
    monkeypatch.setattr(core_quantity_reductions, 'utc_now', lambda: clock[0])
    monkeypatch.setattr(reconciliation, '_observation_time', lambda: clock[0])
    authenticated = lambda **kwargs: SimpleNamespace(access_token='fake')
    monkeypatch.setattr(broker_auth, 'ensure_authenticated_broker_session', authenticated)
    monkeypatch.setattr(reconciliation, 'ensure_authenticated_broker_session', authenticated)
    broker_status = ['open']

    def reconcile_order(access_token, broker_order_id, **kwargs):
        return {
            'provider': 'kis', 'broker_order_id': broker_order_id, 'symbol': '005930',
            'status': broker_status[0], 'filled_quantity': filled_count if broker_status[0] != 'open' else 0,
            'average_fill_price': '70000', 'observed_at': clock[0].isoformat(),
        }

    def forbidden_cancel(*args, **kwargs):
        raise AssertionError('increased target must not cancel an already authorized BUY')

    adapter = SimpleNamespace(provider='kis', reconcile_order=reconcile_order, cancel_order=forbidden_cancel)
    if lifecycle == 'revoked_during_reconcile':
        original_reconcile = core_quantity_reductions._reconcile_before_cancel

        def reconcile_then_revoke(*, db, item, **kwargs):
            original_reconcile(db=db, item=item, **kwargs)
            if broker_status[0] != 'open':
                db.execute(sql_update(ExecutionPlanItem).where(ExecutionPlanItem.id == item.id).values(status='revoked'), execution_options={'synchronize_session': False})
                db.execute(sql_update(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item.id, ExecutionJob.job_type == 'next_open_buy').values(status='superseded'), execution_options={'synchronize_session': False})
                db.commit()

        monkeypatch.setattr(core_quantity_reductions, '_reconcile_before_cancel', reconcile_then_revoke)
    app, engine = _build_test_app()
    with TestClient(app) as client:
        plan = core_plan()
        accepted = client.post('/v4/execution-plans', json=plan).json()
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            next_open = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy'))
            next_open.status, entry.status = 'completed', 'broker_order_open'
            original_job_id = next_open.id
            original_raw_payload = deepcopy(entry.plan.raw_payload)
            db.add(BrokerOrder(
                execution_plan_item_id=entry.id, broker_name='kis', mode='mock', idempotency_key='prior-open-buy',
                broker_order_id='source-buy', symbol='005930', side='buy', status='open', quantity=10,
                venue='kis', asset_class='stock',
                request_metadata={'execution_scope': _test_kis_execution_scope(app.state.settings, venue='kis')},
            ))
            db.commit()
        update = revision_request(accepted, plan, target=12)
        if lifecycle == 'expired':
            update['valid_until'] = (clock[0] + timedelta(seconds=20)).isoformat()
        result = client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()
        assert result['updated_count'] == 1
        with app.state.db_session_factory() as db:
            followup = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE))
            assert followup is not None and followup.status == 'queued'
            core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=clock[0], krx_trading_day=lambda day: True)
            db.refresh(followup)
            assert followup.status == 'retrying'
            assert db.get(ExecutionJob, original_job_id).status == 'completed'

        if lifecycle == 'repair_replay':
            broker_status[0] = terminal_status
            with app.state.db_session_factory() as db:
                db.delete(db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE)))
                db.commit()
                order = db.scalar(select(BrokerOrder))
                assert reconciliation.reconcile_broker_order(settings=app.state.settings, db=db, adapter=adapter, order=order)['status_code'] == 200
        assert client.post('/v4/execution-plans/quantities', json={'items': [update]}).json()['unchanged_count'] == 1
        if lifecycle == 'repair_replay':
            with app.state.db_session_factory() as db:
                job = db.get(ExecutionJob, original_job_id)
                assert job.status == 'scheduled'
                command = execution_jobs._build_place_order_request(settings=app.state.settings, db=db, adapter=adapter, job=job)
                assert command.quantity == 12 - filled_count and command.metadata['core_quantity_revision'] == 2
            return
        expected_revision, expected_remaining = 2, 12 - filled_count
        if lifecycle == 'newer_revision':
            latest = revision_request(accepted, plan, target=11, revision=3)
            assert client.post('/v4/execution-plans/quantities', json={'items': [latest]}).json()['updated_count'] == 1
            expected_revision, expected_remaining = 3, 11 - filled_count
        if lifecycle in {'closed', 'running'}:
            with app.state.db_session_factory() as db:
                if lifecycle == 'closed':
                    stop = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'stop_sell'))
                    stop.status = 'stop_sell_triggered_pending_dispatch'
                else:
                    job = db.get(ExecutionJob, original_job_id)
                    job.status, job.locked_at = 'running', clock[0]
                    job.plan_item.status = 'next_open_buy_dispatching'
                db.commit()
        clock[0] += timedelta(seconds=31)
        broker_status[0] = terminal_status
        with app.state.db_session_factory() as db:
            core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=clock[0], krx_trading_day=lambda day: True)
            jobs = db.scalars(select(ExecutionJob).where(ExecutionJob.job_type == 'next_open_buy')).all()
            followup = db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE))
            if lifecycle == 'revoked_during_reconcile':
                assert jobs[0].status == 'superseded' and followup.status == 'completed'
                assert jobs[0].plan_item.status == 'revoked'
                return
            if lifecycle in {'closed', 'expired'}:
                assert jobs[0].status == 'completed' and followup.status == 'completed'
                assert core_quantity.executable_core_quantity(db=db, item=jobs[0].plan_item) == 0
                return
            if lifecycle == 'running':
                assert jobs[0].status == 'running' and followup.status == 'retrying'
                assert followup.last_error == 'quantity_followup_dispatch_in_flight'
                jobs[0].status, jobs[0].locked_at = 'completed', None
                db.commit()
                clock[0] += timedelta(seconds=31)
                core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=clock[0], krx_trading_day=lambda day: True)
                db.refresh(jobs[0])
            assert len(jobs) == 1 and jobs[0].id == original_job_id and jobs[0].status == 'scheduled', followup.last_error
            command = execution_jobs._build_place_order_request(settings=app.state.settings, db=db, adapter=adapter, job=jobs[0])
            assert command.quantity == expected_remaining and command.metadata['core_quantity_revision'] == expected_revision
            assert command.idempotency_key.endswith(f':core:{expected_revision}')
            assert jobs[0].plan_item.plan.raw_payload == original_raw_payload
            assert db.scalar(select(ExecutionJob).where(ExecutionJob.job_type == core_quantity_reductions.JOB_TYPE)).status == 'completed'
            core_quantity_reductions.process_core_quantity_reductions(settings=app.state.settings, db=db, adapter=adapter, now=clock[0], krx_trading_day=lambda day: True)
            assert db.get(ExecutionJob, original_job_id).status == 'scheduled'
    engine.dispose()
