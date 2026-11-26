"""PostgreSQL row locks must never precede rule maintenance's process lock."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
import os
from threading import Event
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine, event, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import Settings
from app.api.schemas.execution import PlaceOrderRequest
from app.api.routes import execution as execution_routes
from app.broker.base import BrokerAccountHolding, BrokerAccountOpenOrder, BrokerAccountSnapshot, BrokerConfig, BrokerLiveMarketContext
from app.db.base import Base
from app.db.session import create_db_engine
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionJob, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services import broker_orders, core_handoff_v4, core_quantity, monitoring, rule_execution
from app.services.core_revocation_exposure import serialized_revocation_exposure
from app.services.execution_lineage import decision_lineage_for_plan_item
from app.services.position_identity import build_position_identity
from app.services.account_drawdown_recovery import recovery_account_fingerprint
from app.services.order_intent_serialization import order_intent_process_lock, order_intent_scope_key_for_account
from app.services.runtime_shutdown import CommitGuardedSessionFactory, RuntimeCycleFence, RuntimeCycleStopped
from test_core_handoff_v4 import _build_test_app
from test_rule_origin_v4 import activation_request, rule_plan
from test_broker_auth import StubBrokerAdapter, _test_kis_execution_scope, _use_historical_broker_boundary, build_order_placement, build_token_grant


@pytest.fixture
def rule_lock_database():
    url = os.getenv('KAI_EXEC_TEST_POSTGRES_URL')
    if not url:
        pytest.skip('KAI_EXEC_TEST_POSTGRES_URL is required for PostgreSQL rule lock tests')
    parsed = make_url(url)
    if parsed.get_backend_name() != 'postgresql' or not (parsed.database or '').endswith('_test'):
        pytest.fail('isolated PostgreSQL database must end with _test')
    admin = create_db_engine(Settings(database_url=url, runtime_mode='mock'))
    schema = f'rule_lock_{uuid4().hex[:12]}'
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    scoped_url = parsed.update_query_dict({'options': f'-csearch_path={schema}'}).render_as_string(hide_password=False)
    settings = Settings(database_url=scoped_url, runtime_mode='mock', krx_us_rebound_enabled=True,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    try:
        with TestClient(app) as client:
            response = client.post('/v4/execution-plans', json=rule_plan())
            assert response.status_code == 201, response.text
        with app.state.db_session_factory() as db:
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            db.add(BrokerOrder(plan_item=entry, broker_name='kis', mode='mock', idempotency_key='rule-lock-buy',
                               broker_order_id='rule-lock-buy', symbol=entry.symbol, side='buy', status='open', quantity=10,
                               venue='kis', asset_class='stock', limit_price=Decimal('70000')))
            db.commit()
            entry_id = entry.id
        yield settings, engine, app.state.db_session_factory, entry_id
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        admin.dispose()


@pytest.mark.parametrize('already_closed', [False, True])
@pytest.mark.parametrize('connection_bound', [False, True])
def test_noop_rule_maintenance_leaves_buy_and_job_rows_unlocked(rule_lock_database, already_closed, connection_bound):
    settings, engine, factory, entry_id = rule_lock_database
    now = datetime(2026, 9, 10, 7 if already_closed else 2, tzinfo=UTC)
    if already_closed:
        with factory() as setup:
            assert core_quantity.maintain_rule_entry_closures(settings=settings, db=setup, now=now) == 1
            setup.commit()
    with engine.connect() as connection, Session(bind=connection if connection_bound else engine) as owner:
        owner.execute(text('SELECT 1'))
        checked_out = engine.pool.checkedout()
        assert core_quantity.maintain_rule_entry_closures(settings=settings, db=owner, now=now) == 0
        assert engine.pool.checkedout() == checked_out
        # Keep owner open: another actual PostgreSQL connection must lock both
        # rows immediately, including the no-op existing queued-job case.
        with factory() as probe:
            assert probe.scalar(select(ExecutionPlanItem.id).where(
                ExecutionPlanItem.id == entry_id).with_for_update(nowait=True)) == entry_id
            jobs = probe.scalars(select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == entry_id,
                ExecutionJob.job_type == 'core_quantity_reduction').with_for_update(nowait=True)).all()
            assert bool(jobs) is already_closed


def test_busy_process_scope_skips_rule_maintenance_without_touching_caller_writes(rule_lock_database):
    settings, _engine, factory, entry_id = rule_lock_database
    held, probe_buy = Event(), Event()

    def hold_scope_then_lock_buy():
        with factory() as other:
            item = other.get(ExecutionPlanItem, entry_id)
            with serialized_revocation_exposure(settings=settings, db=other, items=[item]):
                held.set()
                assert probe_buy.wait(timeout=5)
                assert other.scalar(select(ExecutionPlanItem.id).where(
                    ExecutionPlanItem.id == entry_id).with_for_update(nowait=True)) == entry_id
                other.rollback()

    with ThreadPoolExecutor(max_workers=1) as workers:
        future = workers.submit(hold_scope_then_lock_buy)
        assert held.wait(timeout=5)
        try:
            with factory() as caller:
                sell = caller.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
                original_status = sell.status
                sell.status = 'target_sell_triggered_pending_dispatch'
                sell.risk_context = {**sell.risk_context, 'unrelated_caller_evidence': 'preserve'}
                assert core_quantity.maintain_rule_entry_closures(
                    settings=settings, db=caller, now=datetime(2026, 9, 10, 7, tzinfo=UTC)) == 0
                assert sell.status == 'target_sell_triggered_pending_dispatch'
                assert sell.risk_context['unrelated_caller_evidence'] == 'preserve'
                with factory() as observe:
                    assert observe.get(ExecutionPlanItem, sell.id).status == original_status
                probe_buy.set()
                future.result(timeout=5)
                caller.commit()
            with factory() as verify:
                assert verify.get(ExecutionPlanItem, sell.id).risk_context['unrelated_caller_evidence'] == 'preserve'
        finally:
            probe_buy.set()
            future.result(timeout=5)


def test_busy_buy_row_is_skipped_without_wait_or_caller_rollback(rule_lock_database):
    settings, _engine, factory, entry_id = rule_lock_database
    with factory() as locker, factory() as caller:
        locker.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.id == entry_id).with_for_update())
        # The old discovery FOR UPDATE waits and fails this bounded real-PG
        # timeout. Maintenance must instead use its own NOWAIT transaction.
        caller.execute(text("SET LOCAL lock_timeout = '250ms'"))
        sell = caller.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
        sell.risk_context = {**sell.risk_context, 'unrelated_caller_evidence': 'preserve'}
        assert core_quantity.maintain_rule_entry_closures(
            settings=settings, db=caller, now=datetime(2026, 9, 10, 7, tzinfo=UTC)) == 0
        assert sell.risk_context['unrelated_caller_evidence'] == 'preserve'
        locker.rollback()
        caller.commit()


def test_busy_job_row_releases_maintenance_buy_and_process_locks(rule_lock_database):
    settings, engine, factory, entry_id = rule_lock_database
    now = datetime(2026, 9, 10, 7, tzinfo=UTC)
    with factory() as setup:
        assert core_quantity.maintain_rule_entry_closures(settings=settings, db=setup, now=now) == 1
        job = setup.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction'))
        job.status = 'completed'
        setup.commit()
        job_id = job.id
    with factory() as locker, factory() as caller:
        locker.scalar(select(ExecutionJob).where(ExecutionJob.id == job_id).with_for_update())
        caller.execute(text('SELECT 1'))
        checked_out = engine.pool.checkedout()
        assert core_quantity.maintain_rule_entry_closures(settings=settings, db=caller, now=now) == 0
        assert engine.pool.checkedout() == checked_out

        def probe_after_skip():
            with factory() as probe:
                item = probe.get(ExecutionPlanItem, entry_id)
                with serialized_revocation_exposure(settings=settings, db=probe, items=[item]):
                    probe.scalar(select(ExecutionPlanItem.id).where(
                        ExecutionPlanItem.id == entry_id).with_for_update(nowait=True))
                    probe.rollback()

        with ThreadPoolExecutor(max_workers=1) as workers:
            workers.submit(probe_after_skip).result(timeout=5)
        locker.rollback()


def test_maintenance_database_error_closes_owned_transaction_and_preserves_caller(rule_lock_database):
    settings, engine, factory, entry_id = rule_lock_database
    with engine.begin() as setup:
        setup.execute(text("""CREATE FUNCTION reject_rule_closure_audit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected rule audit failure'; END; $$"""))
        setup.execute(text("""CREATE TRIGGER reject_rule_closure_audit BEFORE INSERT ON execution_plan_item_audits
            FOR EACH ROW EXECUTE FUNCTION reject_rule_closure_audit()"""))
    with factory() as caller:
        sell = caller.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
        sell.risk_context = {**sell.risk_context, 'unrelated_caller_evidence': 'preserve'}
        checked_out = engine.pool.checkedout()
        with pytest.raises(DBAPIError, match='injected rule audit failure'):
            core_quantity.maintain_rule_entry_closures(
                settings=settings, db=caller, now=datetime(2026, 9, 10, 7, tzinfo=UTC))
        assert engine.pool.checkedout() == checked_out
        assert sell.risk_context['unrelated_caller_evidence'] == 'preserve'

        def probe_after_failure():
            with factory() as probe:
                item = probe.get(ExecutionPlanItem, entry_id)
                with serialized_revocation_exposure(settings=settings, db=probe, items=[item]):
                    probe.scalar(select(ExecutionPlanItem.id).where(
                        ExecutionPlanItem.id == entry_id).with_for_update(nowait=True))
                    assert core_quantity.RULE_ENTRY_CLOSED_KEY not in item.risk_context
                    probe.rollback()

        with ThreadPoolExecutor(max_workers=1) as workers:
            workers.submit(probe_after_failure).result(timeout=5)


def test_no_rule_maintenance_does_not_touch_a_shared_sqlite_caller_transaction():
    engine = create_engine('sqlite://', poolclass=StaticPool)
    Base.metadata.create_all(engine)
    settings = Settings(database_url='sqlite://', runtime_mode='mock')
    with Session(engine) as caller:
        item = ExecutionPlanItem(plan=ExecutionPlan(external_plan_id='ordinary-agent', source='manual', mode='mock'),
                                 symbol='005930', side='sell', intent='target_sell', status='before', quantity_policy={})
        caller.add(item)
        caller.commit()
        caller.execute(update(ExecutionPlanItem).where(ExecutionPlanItem.id == item.id).values(status='caller-staged'))
        assert core_quantity.maintain_rule_entry_closures(
            settings=settings, db=caller, now=datetime(2026, 9, 10, 7, tzinfo=UTC), commit_rule_caller_state=True) == 0
        assert caller.scalar(select(ExecutionPlanItem.status).where(ExecutionPlanItem.id == item.id)) == 'caller-staged'
        caller.rollback()
        assert caller.get(ExecutionPlanItem, item.id).status == 'before'
    engine.dispose()


@pytest.mark.parametrize('busy_first_exit', [False, True])
def test_public_rule_sell_boundary_has_no_maintenance_locks_and_preserves_closure(rule_lock_database, monkeypatch, busy_first_exit):
    settings, _engine, factory, entry_id = rule_lock_database
    clock = [datetime(2026, 9, 9, 23, 30, tzinfo=UTC)]
    class SimulatedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz is not None else clock[0].replace(tzinfo=None)

    monkeypatch.setattr(broker_orders, 'datetime', SimulatedDateTime)
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: clock[0])
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: clock[0])
    monkeypatch.setattr(execution_routes, 'evaluate_monitoring',
                        lambda **kwargs: monitoring.evaluate_monitoring(**kwargs, now=clock[0]))
    _use_historical_broker_boundary(monkeypatch)
    app, engine = _build_test_app(settings=settings)
    adapter = StubBrokerAdapter(config=BrokerConfig(provider='kis', mode='mock', configured=True,
        api_base_url='https://mock.invalid'), token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id='must-stay-deferred'))
    adapter.get_account_snapshot = lambda *args, **kwargs: BrokerAccountSnapshot(
        available_cash=Decimal('9000000'), buying_power=Decimal('9000000'), observed_at=clock[0],
        total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'), holdings_complete=True,
        holdings=[BrokerAccountHolding(symbol='005930', quantity=4, average_price=Decimal('70000'))],
        open_orders=[BrokerAccountOpenOrder(symbol='005930', side='buy', quantity=10, remaining_quantity=6,
            broker_order_id='rule-lock-buy', limit_price=Decimal('70000'))])
    adapter.get_live_market_context = lambda *args, **kwargs: BrokerLiveMarketContext(
        symbol='005930', exchange='KRX', observed_at=clock[0], bid=Decimal('76000'), ask=Decimal('76010'),
        freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True)
    adapter.is_krx_trading_day = lambda *args, **kwargs: True
    app.state.broker_adapter_factory = lambda _settings: adapter
    original_place = monitoring.place_broker_order
    boundary_probes = []
    scope_held, release_scope = Event(), Event()
    holder_pool = ThreadPoolExecutor(max_workers=1)
    holder = None

    def hold_only_process_scope():
        scope = order_intent_scope_key_for_account(provider='kis', runtime_mode=settings.runtime_mode,
            account_scope=recovery_account_fingerprint(settings), market='KRW')
        with order_intent_process_lock(scope):
            scope_held.set()
            assert release_scope.wait(timeout=5)

    def probe_then_place(**kwargs):
        if holder is not None and not boundary_probes:
            release_scope.set()
            holder.result(timeout=5)
        def another_scope_owner():
            with factory() as probe:
                entry = probe.get(ExecutionPlanItem, entry_id)
                with serialized_revocation_exposure(settings=settings, db=probe, items=[entry]):
                    probe.scalar(select(ExecutionPlanItem.id).where(
                        ExecutionPlanItem.id == entry_id).with_for_update(nowait=True))
                    probe.scalars(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == entry_id,
                        ExecutionJob.job_type == 'core_quantity_reduction').with_for_update(nowait=True)).all()
                    if not busy_first_exit or boundary_probes:
                        assert core_quantity.RULE_ENTRY_CLOSED_KEY in entry.risk_context
                    probe.rollback()
        with ThreadPoolExecutor(max_workers=1) as workers:
            workers.submit(another_scope_owner).result(timeout=5)
        boundary_probes.append(kwargs['command'].side)
        return original_place(**kwargs)

    monkeypatch.setattr(monitoring, 'place_broker_order', probe_then_place)
    try:
        with TestClient(app) as client:
            accepted = client.post('/v4/execution-plans', json=rule_plan()).json()
            response = client.post('/v4/execution-plans/activations', json=activation_request(rule_plan(), accepted))
            assert response.status_code == 200, response.text
            client.post('/controls/resume')
            clock[0] = datetime(2026, 9, 10, 0, 1, tzinfo=UTC)
            with factory() as setup:
                entry = setup.get(ExecutionPlanItem, entry_id)
                entry.status = 'partially_filled'
                order = setup.scalar(select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == entry_id))
                order.status = 'partially_filled'
                family = {broker_orders.STRATEGY_ENTRY_FAMILY_METADATA_KEY: broker_orders._strategy_entry_family_identity(entry)}
                order.request_metadata = {'execution_scope': _test_kis_execution_scope(settings, venue='kis'),
                    'metadata': family, 'original_command': {'metadata': family},
                    'decision_lineage': decision_lineage_for_plan_item(entry)}
                order.response_metadata = {'filled_quantity': 4}
                order.fills.append(ExecutionFill(idempotency_key='rule-lock-fill', symbol=entry.symbol, side='buy',
                    quantity=4, price=Decimal('70000'), filled_at=clock[0], payload={'fill_time_precision': 'exact'}))
                identity = build_position_identity(settings=settings, provider='kis', exchange='KRX')
                setup.add(ExecutionPosition(mode='mock', provider='kis', account_scope=identity.account_scope,
                    exchange='KRX', symbol=entry.symbol, quantity=4, average_price=Decimal('70000'),
                    updated_from='test', last_reconciled_at=clock[0]))
                setup.commit()
            if busy_first_exit:
                holder = holder_pool.submit(hold_only_process_scope)
                assert scope_held.wait(timeout=5)
            for seconds in (0, settings.monitoring_watch_buy_retry_cooldown_seconds + 1):
                clock[0] = datetime(2026, 9, 10, 0, 30, seconds, tzinfo=UTC)
                response = client.post('/monitoring/evaluate', json={
                    'prices': [{'symbol': '005930', 'price': '76000', 'observed_at': clock[0].isoformat()}],
                    'observed_at': clock[0].isoformat(),
                })
                assert response.status_code == 200, response.text
                with factory() as verify:
                    entry = verify.get(ExecutionPlanItem, entry_id)
                    assert core_quantity.core_quantity_state(db=verify, item=entry)['entry_closed'] is True, response.text
                    if not busy_first_exit or seconds:
                        assert entry.risk_context[core_quantity.RULE_ENTRY_CLOSED_KEY]['reason_code'] == 'protective_exit_started'
                    assert entry.quantity_policy['target_quantity'] == 10
            assert boundary_probes == ['sell', 'sell']
            assert adapter.order_calls == []
    finally:
        release_scope.set()
        holder_pool.shutdown(wait=True)
        engine.dispose()


@pytest.mark.parametrize('authority_variant', ['valid', 'absent', 'source', 'policy', 'item', 'buy', 'origin'])
def test_only_exact_rule_sell_dispatch_authority_closes_a_blocked_unsubmitted_exit(rule_lock_database, authority_variant):
    settings, _engine, factory, entry_id = rule_lock_database
    with factory() as db:
        entry = db.get(ExecutionPlanItem, entry_id)
        sell = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
        sell.status = 'target_sell_triggered_blocked'
        command = PlaceOrderRequest(idempotency_key='blocked-sell', execution_plan_item_id=sell.id,
            provider='kis', asset_class='stock', venue='kis', symbol=sell.symbol, side='sell', quantity=4,
            order_type='market', exchange='KRX')
        authority = {'policy_version': 1, 'source': 'monitoring:intraday_target_stop_monitor',
                     'command': command.model_dump(mode='json')}
        if authority_variant == 'source':
            authority['source'] = 'manual'
        elif authority_variant == 'policy':
            authority['policy_version'] = 2
        elif authority_variant == 'item':
            authority['command']['execution_plan_item_id'] = 'another-sell'
        elif authority_variant == 'buy':
            authority['command']['side'] = 'buy'
        risk = dict(sell.risk_context)
        if authority_variant != 'absent':
            risk['monitoring_order_authority'] = authority
        if authority_variant == 'origin':
            risk['rule_origin'] = {**risk['rule_origin'], 'evaluation_id': 'another-evaluation'}
        sell.risk_context = risk
        db.add(BrokerOrder(plan_item=sell, broker_name='kis', mode='mock', idempotency_key='unsubmitted-sell',
                           symbol=sell.symbol, side='sell', quantity=4, status='rejected'))
        db.commit()
        reason = core_quantity.rule_entry_closure_reason(db=db, item=entry, now=datetime(2026, 9, 10, 2, tzinfo=UTC))
        assert reason == ('protective_exit_started' if authority_variant == 'valid' else None)


def test_owned_rule_maintenance_keeps_the_callers_runtime_commit_fence(rule_lock_database):
    settings, engine, factory, entry_id = rule_lock_database
    fence = RuntimeCycleFence()
    guarded = CommitGuardedSessionFactory(factory, fence)
    lease = fence.capture_cycle_lease()
    audit_prepared = Event()

    def expire_before_owned_commit(session):
        from app.models.runtime import ExecutionPlanItemAudit
        if any(isinstance(row, ExecutionPlanItemAudit) and row.event_type == 'rule_entry_closed' for row in session.new):
            audit_prepared.set()
            fence.expire_cycle_lease(lease)

    def stale_cycle():
        with guarded() as caller:
            sell = caller.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
            sell.risk_context = {**sell.risk_context, 'published_caller_evidence': 'preserve'}
            core_quantity.maintain_rule_entry_closures(settings=settings, db=caller,
                now=datetime(2026, 9, 10, 7, tzinfo=UTC), commit_rule_caller_state=True)

    # Class listeners run before the session-instance guard admits commit.
    # Expiry during an already-admitted flush intentionally remains allowed.
    event.listen(Session, 'before_commit', expire_before_owned_commit)
    try:
        with pytest.raises(RuntimeCycleStopped):
            fence.run(lease.generation, stale_cycle, lease=lease)
    finally:
        event.remove(Session, 'before_commit', expire_before_owned_commit)
    assert audit_prepared.is_set()
    assert engine.pool.checkedout() == 0
    with factory() as verify:
        from app.models.runtime import ExecutionPlanItemAudit
        entry = verify.get(ExecutionPlanItem, entry_id)
        assert core_quantity.RULE_ENTRY_CLOSED_KEY not in entry.risk_context
        assert verify.scalar(select(ExecutionJob).where(ExecutionJob.job_type == 'core_quantity_reduction')) is None
        assert verify.scalar(select(ExecutionPlanItemAudit).where(ExecutionPlanItemAudit.event_type == 'rule_entry_closed')) is None
        sell = verify.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'target_sell'))
        assert sell.risk_context['published_caller_evidence'] == 'preserve'

    def probe_after_stopped_cycle():
        with factory() as probe:
            item = probe.get(ExecutionPlanItem, entry_id)
            with serialized_revocation_exposure(settings=settings, db=probe, items=[item]):
                probe.scalar(select(ExecutionPlanItem.id).where(
                    ExecutionPlanItem.id == entry_id).with_for_update(nowait=True))
                probe.rollback()
    with ThreadPoolExecutor(max_workers=1) as workers:
        workers.submit(probe_after_stopped_cycle).result(timeout=5)


def test_rule_legacy_normalizer_never_locks_buy_or_job_rows(rule_lock_database):
    settings, _engine, factory, entry_id = rule_lock_database
    with factory() as setup:
        item = setup.get(ExecutionPlanItem, entry_id)
        setup.add(ExecutionJob(plan_item=item, job_type='core_quantity_reduction', status='queued',
                              scheduled_for=datetime(2026, 9, 10, 7, tzinfo=UTC)))
        setup.commit()
    with factory() as owner:
        entry = owner.get(ExecutionPlanItem, entry_id)
        assert core_handoff_v4._normalize_safe_legacy_watch_authority(db=owner, plan=entry.plan, settings=settings) == set()
        with factory() as probe:
            probe.scalar(select(ExecutionPlanItem.id).where(
                ExecutionPlanItem.id == entry_id).with_for_update(nowait=True))
            assert probe.scalar(select(ExecutionJob.id).where(ExecutionJob.execution_plan_item_id == entry_id,
                ExecutionJob.job_type == 'core_quantity_reduction').with_for_update(nowait=True)) is not None


@pytest.mark.parametrize('failure_stage', ['before', 'after'])
def test_rollback_failure_still_releases_rule_account_lock(monkeypatch, tmp_path, failure_stage):
    settings = Settings(database_url=f'sqlite:///{tmp_path}/rollback-failure.db', runtime_mode='mock',
        kis_account_number='12345678', kis_account_product_code='01',
        account_drawdown_recovery_scope_secret='rollback-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)

    class RollbackFailureSession(Session):
        def rollback(self):
            if not self.info.get('rollback_failure_injected'):
                self.info['rollback_failure_injected'] = True
                if failure_stage == 'after':
                    super().rollback()
                raise RuntimeError('injected maintenance rollback failure')
            return super().rollback()

    scope = order_intent_scope_key_for_account(provider='kis', runtime_mode=settings.runtime_mode,
        account_scope=recovery_account_fingerprint(settings), market='KRW')
    process_lock = order_intent_process_lock(scope)

    def another_thread_can_acquire():
        acquired = process_lock.acquire(blocking=False)
        if acquired:
            process_lock.release()
        return acquired

    try:
        with TestClient(app) as client:
            assert client.post('/v4/execution-plans', json=rule_plan()).status_code == 201
            monkeypatch.setattr(core_quantity, 'Session', RollbackFailureSession)
            with app.state.db_session_factory() as caller:
                with pytest.raises(RuntimeError, match='injected maintenance rollback failure'):
                    core_quantity.maintain_rule_entry_closures(
                        settings=settings, db=caller, now=datetime(2026, 9, 10, 7, tzinfo=UTC))
            assert engine.pool.checkedout() == 0
            with ThreadPoolExecutor(max_workers=1) as workers:
                assert workers.submit(another_thread_can_acquire).result(timeout=5)
    finally:
        # Keep a reproduced pre-fix leaked lease from contaminating other tests.
        try:
            process_lock.release()
        except RuntimeError:
            pass
        engine.dispose()
