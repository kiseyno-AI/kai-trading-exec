from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import json
from types import SimpleNamespace
import pytest

from fastapi.testclient import TestClient

from app.api.schemas.execution import CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION, CoreExecutionPlanRequestV4, rule_price_contract_digest
from app.api.schemas.execution import (
    CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION, CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
    CORE_EXECUTION_ACTIVATION_CAPABILITY, CORE_EXECUTION_REVOCATION_SCHEMA_VERSION,
    CORE_EXECUTION_REVOCATION_CAPABILITY,
)
from app.core.config import Settings
from app.services import core_quantity
from app.services import broker_orders, execution_jobs, monitoring, rule_execution
from app.services.execution_lineage import decision_lineage_for_plan_item, decision_lineage_for_order
from app.services.plan_item_identity import plan_item_entry_family_key, plan_item_decision_agent_name, plan_item_strategy_lane
from app.broker.base import BrokerAccountSnapshot, BrokerAccountHolding, BrokerConfig, BrokerLiveMarketContext, DomesticCashCapacity
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.api.schemas.execution import MonitoringEvaluationRequest
from app.services.position_identity import build_position_identity
from app.services.max_holding_actionability import max_holding_actionability_summary
from app.services.autonomous_runtime import CachedMonitoringItem, _cached_monitoring_item_triggered
from sqlalchemy import select
from test_broker_auth import StubBrokerAdapter, build_order_placement, build_token_grant, _use_historical_broker_boundary
from test_core_quantity import revision_request
from test_core_quantity import core_plan
from test_core_handoff_v4 import _build_test_app, _refresh_material_fingerprint
from test_core_handoff_v4 import _plan_payload


def rule_plan():
    payload = core_plan()
    payload.update(origin_type='rule', strategy_run_id=None, rule_origin={
        'schema_version': 'rule_origin.v1', 'strategy_id': 'krx_us_rebound',
        'strategy_lane': 'krx_us_rebound', 'rule_version': 'v1', 'run_id': 'rule-run-1',
        'evaluation_id': 'evaluation-1', 'input_manifest_id': 'manifest-1',
        'config_hash': 'b' * 64, 'input_hash': 'c' * 64,
        'target_trade_date': payload['market_date'], 'setup_id': 'us-rebound-open',
    })
    payload['metadata'].update(agent_name=None, decision_agent_name=None)
    for item in payload['items']:
        item.update(agent_name=None, decision_agent_name=None, decision_snapshot_id=None,
                    rule_origin=deepcopy(payload['rule_origin']), close_exit_at='2026-09-10T06:20:00Z')
        item['risk_context'].update(rule_session={
            'calendar_status': 'authoritative', 'calendar_source': 'test-calendar',
            'session_close_at': '2026-09-10T06:30:00Z', 'close_buffer_seconds': 600,
        }, _core_activation={
            'schema_version': CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION,
            'mode': 'core_confirmed', 'authority_generation_fingerprint': 'd' * 64,
        })
    payload['items'][0]['limit_price'] = '70000'
    payload['items'][1]['trigger_price'] = 76000
    payload['items'][2]['trigger_price'] = 69000
    payload['items'][0]['quantity_policy']['valid_until'] = '2026-09-10T06:20:00Z'
    payload['items'][0]['quantity_policy']['sizing_inputs']['price_contract_digest'] = rule_price_contract_digest(
        entry_limit_price=Decimal(70000), entry_trigger_price=None, target_price=Decimal(76000), stop_price=Decimal(69000),
        close_exit_at=datetime.fromisoformat('2026-09-10T06:20:00+00:00'),
    )
    _refresh_material_fingerprint(payload)
    return payload


def test_rule_origin_registers_without_agent_provenance_and_stays_pending_by_default():
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = rule_plan()
        original['requested_at'] = '2026-09-09T21:00:00+09:00'
        original['metadata']['requested_at'] = original['requested_at']
        _refresh_material_fingerprint(original)
        response = client.post('/v4/execution-plans', json=original)
        assert response.status_code == 201, response.json()
        accepted = response.json()
        assert accepted['contract_ack']['rule_origin'] == original['rule_origin']
        readback = client.get('/v4/execution-plans').json()[0]
        assert readback['rule_origin'] == original['rule_origin']
        assert readback['item_states'][0]['status'] == 'core_activation_pending'
        assert all(row['decision_snapshot_id'] is None and row['decision_agent_name'] is None
                   and row['rule_origin'] == original['rule_origin'] for row in readback['item_states'])
        replay = client.post('/v4/execution-plans', json=original)
        assert replay.status_code == 201, replay.json()
        assert replay.json()['item_acks'] == accepted['item_acks']
        capabilities = client.get('/v4/execution-plans/capabilities').json()
        assert 'rule_origin_krx_us_rebound_v1' in capabilities['capabilities']
        assert capabilities['rule_origin']['enabled'] is False
    engine.dispose()


@pytest.mark.parametrize('runtime_mode', ['mock', 'paper'])
def test_rule_uses_existing_mock_broker_and_restores_exact_timed_family_after_restart(monkeypatch, tmp_path, runtime_mode):
    now = datetime.fromisoformat('2026-09-09T23:30:00+00:00')
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: now)
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: now)
    monkeypatch.setattr(broker_orders, 'core_buy_authority_block_reason', lambda **kwargs: None)
    _use_historical_broker_boundary(monkeypatch)
    settings = Settings(database_url=f'sqlite:///{tmp_path}/rule-runtime.db', runtime_mode=runtime_mode,
                        krx_us_rebound_enabled=True, account_drawdown_gate_enabled=False,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    adapter = StubBrokerAdapter(
        config=BrokerConfig(provider='kis', mode=runtime_mode, configured=True, api_base_url='https://mock.invalid'),
        token_grant=build_token_grant(), order_placement=build_order_placement(broker_order_id='rule-buy'),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal('10000000'), buying_power=Decimal('10000000'), observed_at=now,
            total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'), holdings_complete=True,
        ),
    )
    adapter.get_domestic_cash_capacity = lambda **kwargs: DomesticCashCapacity(
        provider='kis', runtime_mode=runtime_mode, account_reference='KIS:12345678-01',
        symbol=kwargs['symbol'], exchange='KRX', currency='KRW', requested_price=kwargs['limit_price'],
        quantity_calculation_price=kwargs['limit_price'], orderable_cash_krw=Decimal('9000000'),
        no_misu_buying_amount_krw=Decimal('9000000'), no_misu_buying_quantity=100,
        received_at=now, capacity_id='a' * 64,
    )
    adapter.get_live_market_context = lambda *args, **kwargs: BrokerLiveMarketContext(
        symbol='005930', exchange='KRX', observed_at=now, bid=Decimal('69990'), ask=Decimal('70000'),
        freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True,
    )
    with TestClient(app) as client:
        original = rule_plan()
        original['mode'] = runtime_mode
        original['items'][0]['quantity_policy']['sizing_inputs']['cash_per_share'] = '70350'
        _refresh_material_fingerprint(original)
        accepted = client.post('/v4/execution-plans', json=original).json()
        response = client.post('/v4/execution-plans/activations', json=activation_request(original, accepted))
        assert response.status_code == 200, response.text
        assert client.post('/controls/resume').status_code == 200
        now = datetime.fromisoformat('2026-09-10T00:01:00+00:00')
        adapter.account_snapshot = replace(adapter.account_snapshot, observed_at=now)
        with app.state.db_session_factory() as db:
            result = execution_jobs.dispatch_due_execution_jobs(settings=settings, db=db, adapter=adapter, now=now)
            assert result.payload.get('completed_job_count') == 1, result.payload
            order = db.scalar(select(BrokerOrder).where(BrokerOrder.side == 'buy'))
            assert order is not None and order.quantity == 10
            lineage = decision_lineage_for_order(order)
            assert lineage['status'] == 'complete' and lineage['rule_origin'] == original['rule_origin']
            assert lineage['decision_snapshot_id'] is None and lineage['decision_run_id'] is None
            order.status = 'filled'
            order.plan_item.status = 'filled'
            order.fills.append(ExecutionFill(symbol='005930', side='buy', quantity=10, price=Decimal('70000'),
                                            filled_at=now, payload={'fill_time_precision': 'exact'}))
            identity = build_position_identity(settings=settings, provider='kis', exchange='KRX')
            db.add(ExecutionPosition(mode=runtime_mode, provider='kis', account_scope=identity.account_scope, exchange='KRX',
                                     symbol='005930', quantity=10, average_price=Decimal('70000'), updated_from='test',
                                     last_reconciled_at=now))
            db.commit()
            monitoring.evaluate_monitoring(settings=settings, db=db, adapter=adapter,
                command=MonitoringEvaluationRequest(prices=[{'symbol': '005930', 'price': '70000', 'observed_at': now}], observed_at=now), now=now)
            timer = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'max_holding_close_sell'))
            assert timer is not None and timer.risk_context['close_exit_at'] == original['items'][0]['close_exit_at']
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            family = plan_item_entry_family_key(entry)
            assert family and all(plan_item_entry_family_key(item) == family for item in entry.plan.items)
            assert plan_item_decision_agent_name(timer) is None and plan_item_strategy_lane(timer) == 'krx_us_rebound'
            assert decision_lineage_for_plan_item(timer)['status'] == 'complete'
        assert len(adapter.order_calls) == 1
    engine.dispose()
    # A fresh process reads the same durable ownership and explicit deadline.
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        readback = client.get('/v4/execution-plans').json()
        assert len(readback) == 1 and readback[0]['contract_ack']['rule_origin'] == original['rule_origin']
        state = next(row for row in readback[0]['item_states'] if row['core_item_key'] == original['items'][0]['core_item_key'])
        assert state['core_activation_state']['status'] == 'activated'
        timer_state = next(row for row in readback[0]['item_states'] if row['intent'] == 'max_holding_close_sell')
        assert timer_state['side'] == 'sell'
        with app.state.db_session_factory() as db:
            timer = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.intent == 'max_holding_close_sell'))
            assert timer is not None and plan_item_entry_family_key(timer) == family
            now = datetime.fromisoformat('2026-09-10T06:19:59+00:00')
            summary = max_holding_actionability_summary(settings=settings, db=db, mode=runtime_mode, reference_at=now)
            assert summary['due_item_count'] == 0, summary
            adapter.account_snapshot = BrokerAccountSnapshot(
                available_cash=Decimal('9300000'), buying_power=Decimal('9300000'), observed_at=now,
                total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'),
                holdings=[BrokerAccountHolding(symbol='005930', quantity=10, average_price=Decimal('70000'))],
                holdings_complete=True,
            )
            monitoring.evaluate_monitoring(settings=settings, db=db, adapter=adapter,
                command=MonitoringEvaluationRequest(prices=[{'symbol': '005930', 'price': '70000', 'observed_at': now}], observed_at=now), now=now)
            assert len(adapter.order_calls) == 1
            now = datetime.fromisoformat('2026-09-10T06:20:00+00:00')
            adapter.order_placement = build_order_placement(broker_order_id='rule-close')
            result = monitoring.evaluate_monitoring(settings=settings, db=db, adapter=adapter,
                command=MonitoringEvaluationRequest(prices=[{'symbol': '005930', 'price': '70000', 'observed_at': now}], observed_at=now), now=now)
            assert len(adapter.order_calls) == 2, result.payload
            assert adapter.order_calls[-1].side == 'sell' and adapter.order_calls[-1].quantity == 10
            sell = db.scalar(select(BrokerOrder).where(BrokerOrder.side == 'sell'))
            assert decision_lineage_for_order(sell)['rule_origin'] == original['rule_origin']
            monitoring.evaluate_monitoring(settings=settings, db=db, adapter=adapter,
                command=MonitoringEvaluationRequest(prices=[{'symbol': '005930', 'price': '70000', 'observed_at': now}], observed_at=now), now=now)
        assert len(adapter.order_calls) == 2
    engine.dispose()


def activation_request(original, accepted):
    return {
        'origin_type': 'rule', 'schema_version': CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION,
        'contract_version': CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
        'required_capability': CORE_EXECUTION_ACTIVATION_CAPABILITY,
        'plan_id': original['plan_id'], 'execution_plan_id': accepted['execution_plan_id'],
        'dispatch_material_fingerprint': original['metadata']['dispatch_material_fingerprint'],
        'authority_generation_fingerprint': 'd' * 64, 'requested_at': original['requested_at'],
        'items': [{
            'core_item_key': ack['core_item_key'], 'execution_plan_item_id': ack['execution_plan_item_id'],
            'decision_snapshot_id': None, 'rule_origin': deepcopy(original['rule_origin']),
        } for ack in accepted['item_acks']],
    }


def test_rule_controls_bind_real_origin_and_disabled_flag_without_snapshot_aliases(monkeypatch):
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: datetime.fromisoformat('2026-09-10T02:00:00+00:00'))
    app, engine = _build_test_app()
    with TestClient(app) as client:
        original = rule_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        activation = activation_request(original, accepted)
        response = client.post('/v4/execution-plans/activations', json=activation)
        assert response.status_code == 409 and response.json()['reason_code'] == 'rule_strategy_disabled'
        revision = revision_request(accepted, original, target=6)
        revision.update(decision_snapshot_id=None, rule_origin=deepcopy(original['rule_origin']))
        forged = deepcopy(revision)
        forged['rule_origin']['evaluation_id'] = 'different-evaluation'
        result = client.post('/v4/execution-plans/quantities', json={'items': [forged, revision]}).json()
        assert (result['failed_count'], result['updated_count']) == (1, 1), result
        revocation = {
            'origin_type': 'rule', 'schema_version': CORE_EXECUTION_REVOCATION_SCHEMA_VERSION,
            'contract_version': CORE_EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': CORE_EXECUTION_REVOCATION_CAPABILITY,
            'plan_id': original['plan_id'], 'requested_at': original['requested_at'],
            'reason_code': 'execution_control_disabled', 'replacement_decision_snapshot_id': None,
            'replacement_rule_origin': {**original['rule_origin'], 'evaluation_id': 'evaluation-disabled'},
            'replacement_action': 'wait', 'items': activation['items'],
        }
        forged = deepcopy(revocation)
        forged['items'][0]['rule_origin']['run_id'] = 'different-run'
        assert client.post('/v4/execution-plans/revocations', json=forged).status_code == 409
        response = client.post('/v4/execution-plans/revocations', json=revocation)
        assert response.status_code == 200, response.text
        assert all(item['status'] == 'revoked' for item in response.json()['item_acks'])
    engine.dispose()


@pytest.mark.parametrize('mutation', ['fake_agent', 'wrong_origin', 'agent_sizing', 'naive_deadline', 'wrong_deadline', 'changed_price'])
def test_rule_intake_rejects_unproven_provenance_quantity_and_deadline(mutation):
    app, engine = _build_test_app()
    payload = rule_plan()
    if mutation == 'fake_agent':
        payload['items'][0]['decision_agent_name'] = 'signal-decider'
    elif mutation == 'wrong_origin':
        payload['items'][1]['rule_origin']['input_manifest_id'] = 'different-inputs'
    elif mutation == 'agent_sizing':
        payload['items'][0]['quantity_policy'] = {'type': 'exec_agent_sized'}
    elif mutation == 'naive_deadline':
        payload['items'][0]['close_exit_at'] = '2026-09-10T06:20:00'
    elif mutation == 'wrong_deadline':
        payload['items'][0]['close_exit_at'] = '2026-09-10T06:30:00Z'
    elif mutation == 'changed_price':
        payload['items'][1]['trigger_price'] = 77000
    _refresh_material_fingerprint(payload)
    with TestClient(app) as client:
        assert client.post('/v4/execution-plans', json=payload).status_code == 422
        assert client.get('/v4/execution-plans').json() == []
    engine.dispose()


def test_agent_serialization_and_canonical_rule_price_digest_are_stable():
    old = CoreExecutionPlanRequestV4.model_validate(_plan_payload()).model_dump(mode='json')
    assert 'origin_type' not in old and 'rule_origin' not in old
    assert all('rule_origin' not in item and 'close_exit_at' not in item for item in old['items'])
    assert rule_price_contract_digest(
        entry_limit_price=Decimal('10000.0'), entry_trigger_price=Decimal('10000.00'),
        target_price=Decimal('10500'), stop_price=Decimal('9800'),
        close_exit_at=datetime.fromisoformat('2026-11-27T15:20:00+09:00'),
    ) == '295d5aa22d7dcc2102e8224094d8e273e953e04e43b73180f2f289b08f21a94f'


def test_rule_activation_cutoff_preserves_original_lanes_and_exact_replay(monkeypatch):
    now = datetime.fromisoformat('2026-09-09T23:40:00+00:00')
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: now)
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='mock', krx_us_rebound_enabled=True,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        original = rule_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        activation = activation_request(original, accepted)
        response = client.post('/v4/execution-plans/activations', json=activation)
        assert response.status_code == 409 and response.json()['reason_code'] == 'rule_activation_cutoff_passed'
        now = datetime.fromisoformat('2026-09-09T23:39:59+00:00')
        assert client.post('/v4/execution-plans/activations', json=activation).status_code == 200
        now = datetime.fromisoformat('2026-09-10T07:00:00+00:00')
        response = client.post('/v4/execution-plans/activations', json=activation)
        assert response.status_code == 200 and response.json()['idempotent_replay'] is True
        caps = client.get('/v4/execution-plans/capabilities', headers={'X-Core-Account-Reference': 'KIS:12345678-01'}).json()
        assert caps['runtime_binding']['account_reference_matches'] is True
        assert caps['runtime_binding']['mode'] == 'mock' and caps['runtime_binding']['provider'] == 'kis'
        assert '12345678' not in str(caps)
        assert client.get('/v4/execution-plans/capabilities', headers={'X-Core-Account-Reference': 'KIS:other-01'}).json()['runtime_binding']['account_reference_matches'] is False
    engine.dispose()


def test_rule_activation_yields_to_potential_original_entry(monkeypatch):
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: datetime.fromisoformat('2026-09-09T23:30:00+00:00'))
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='mock', krx_us_rebound_enabled=True,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        baseline = core_plan()
        baseline['plan_id'] = 'original-lane-plan'
        _refresh_material_fingerprint(baseline)
        assert client.post('/v4/execution-plans', json=baseline).status_code == 201
        original = rule_plan()
        accepted = client.post('/v4/execution-plans', json=original).json()
        response = client.post('/v4/execution-plans/activations', json=activation_request(original, accepted))
        assert response.status_code == 409 and response.json()['reason_code'] == 'rule_baseline_entry_priority', response.text
        readback = client.get('/v4/execution-plans', params={'external_plan_id': baseline['plan_id']}).json()[0]
        assert readback['item_states'][0]['status'] == 'next_open_buy_scheduled'
    engine.dispose()


def test_rule_version_cannot_reset_session_ownership(monkeypatch):
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: datetime.fromisoformat('2026-09-09T23:30:00+00:00'))
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='mock', krx_us_rebound_enabled=True,
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        first = rule_plan()
        accepted = client.post('/v4/execution-plans', json=first).json()
        assert client.post('/v4/execution-plans/activations', json=activation_request(first, accepted)).status_code == 200
        second = rule_plan()
        second['plan_id'] = 'same-setup-new-version'
        second['rule_origin'].update(rule_version='v2', run_id='new-run', evaluation_id='new-evaluation')
        for item in second['items']:
            item['rule_origin'] = deepcopy(second['rule_origin'])
        _refresh_material_fingerprint(second)
        response = client.post('/v4/execution-plans', json=second)
        assert response.status_code == 409 and response.json()['reason_code'] == 'rule_session_reentry_forbidden', response.text
        with app.state.db_session_factory() as db:
            entries = db.scalars(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy')).all()
            assert len(entries) == 1
            stable = plan_item_entry_family_key(entries[0])
            changed = {**entries[0].risk_context, 'rule_origin': second['rule_origin']}
            entries[0].risk_context = changed
            assert stable is not None and plan_item_entry_family_key(entries[0]) == stable
    engine.dispose()


def test_actual_core_builder_fixture_is_accepted_and_readable_in_paper_runtime():
    payload = json.loads((Path(__file__).parent / 'fixtures' / 'us_rebound_rule_v4.json').read_text())
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode='paper',
                        kis_account_number='12345678', kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-cross-repo-test-scope-32-bytes')
    app, engine = _build_test_app(settings=settings)
    with TestClient(app) as client:
        response = client.post('/v4/execution-plans', json=payload)
        assert response.status_code == 201, response.text
        accepted = response.json()
        assert len(accepted['item_acks']) == 3
        assert accepted['contract_ack']['dispatch_material_fingerprint'] == '3f1c25f505d2a83b939f146e7ff4c63726674127d53096a5ae7d59e26d0ba44a'
        row = client.get('/v4/execution-plans', params={'external_plan_id': payload['plan_id']}).json()[0]
        assert row['rule_origin'] == payload['rule_origin']
        assert row['item_states'][0]['status'] == 'core_activation_pending'
        assert row['rule_contract']['items'][0]['quantity_policy'] == payload['items'][0]['quantity_policy']
    engine.dispose()


@pytest.mark.parametrize('rule_origin', [False, True])
def test_v4_readback_identifies_buy_and_protective_sell_states(rule_origin):
    payload = rule_plan() if rule_origin else core_plan()
    app, engine = _build_test_app()
    with TestClient(app) as client:
        response = client.post('/v4/execution-plans', json=payload)
        assert response.status_code == 201, response.text
        accepted = response.json()
        row = client.get('/v4/execution-plans', params={'external_plan_id': payload['plan_id']}).json()[0]
        states = {state['execution_plan_item_id']: state for state in row['item_states']}
        for item, ack in zip(payload['items'], accepted['item_acks'], strict=True):
            state = states[ack['execution_plan_item_id']]
            assert state['side'] == item['side']
            assert state['intent'] == item['intent']
        assert row['contract_ack']['dispatch_material_fingerprint'] == payload['metadata']['dispatch_material_fingerprint']
        if not rule_origin:
            assert 'rule_origin' not in row and 'origin_type' not in row
    engine.dispose()


def test_rule_cash_only_capacity_caps_cost_quantity_and_unacknowledged_reservations(monkeypatch):
    now = datetime.fromisoformat('2026-11-27T00:01:00+00:00')
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: now)
    payload = json.loads((Path(__file__).parent / 'fixtures' / 'us_rebound_rule_v4.json').read_text())
    source = payload['items'][0]
    item = ExecutionPlanItem(plan=ExecutionPlan(raw_payload=payload),
                             source_plan_item_id=source['core_item_key'], quantity_policy=source['quantity_policy'])
    settings = Settings(runtime_mode='paper', kis_account_number='12345678', kis_account_product_code='01')
    capacity = DomesticCashCapacity(provider='kis', runtime_mode='paper', account_reference='KIS:12345678-01',
        symbol=source['symbol'], exchange='KRX', currency='KRW', requested_price=Decimal('10050'),
        quantity_calculation_price=Decimal('10050'), orderable_cash_krw=Decimal('30000'),
        no_misu_buying_amount_krw=Decimal('25000'), no_misu_buying_quantity=100, received_at=now, capacity_id='a' * 64)
    calls = []
    def read_capacity(**kwargs):
        calls.append(kwargs)
        return capacity
    adapter = SimpleNamespace(get_domestic_cash_capacity=read_capacity)
    command = SimpleNamespace(symbol=source['symbol'], exchange='KRX', quantity=2)
    def check(local='0', unknown=0):
        return rule_execution.rule_cash_capacity_failure(settings=settings, adapter=adapter, access_token='test-token',
            command=command, item=item, local_unacknowledged_notional=Decimal(local), local_unknown_count=unknown)
    assert check() is None  # floor(25000 / 10100.25) = 2, irrespective of broker quantity 100.
    assert calls[-1]['limit_price'] == Decimal('10050')
    capacity = replace(capacity, quantity_calculation_price=Decimal('13000'))
    assert check() is None  # The cash ceiling still uses our worst entry, not the provider's quantity basis.
    capacity = replace(capacity, quantity_calculation_price=Decimal('0'))
    assert check() == 'rule_cash_capacity_invalid'
    capacity = replace(capacity, quantity_calculation_price=Decimal('10050'))
    command.quantity = 3
    assert check() == 'rule_cash_only_capacity_exceeded'
    command.quantity = 2
    assert check(local='4800') == 'rule_cash_only_capacity_exceeded'
    assert check(unknown=1) == 'rule_cash_capacity_scope_or_reservations_unproven'
    capacity = replace(capacity, no_misu_buying_quantity=1)
    assert check() == 'rule_cash_only_capacity_exceeded'
    capacity = replace(capacity, no_misu_buying_quantity=100, runtime_mode='live')
    assert check() == 'rule_cash_capacity_scope_or_freshness_mismatch'
    capacity = replace(capacity, runtime_mode='paper', no_misu_buying_amount_krw=Decimal('NaN'))
    assert check() == 'rule_cash_capacity_invalid'
    adapter = SimpleNamespace()
    assert check() == 'rule_cash_capacity_unavailable'


def test_cached_rule_deadline_never_falls_back_to_session_date():
    deadline = datetime.fromisoformat('2026-09-10T15:20:00+09:00')
    item = CachedMonitoringItem(id='rule-timer', symbol='005930', intent='max_holding_close_sell',
        status='max_holding_close_sell_monitoring', trigger_price=None, exchange='KRX',
        close_exit_date=deadline.date(), close_exit_at=deadline, rule_origin_present=True)
    assert not _cached_monitoring_item_triggered(item=item, observed_price=None,
        triggered_at=datetime.fromisoformat('2026-09-10T06:19:59+00:00'))
    assert _cached_monitoring_item_triggered(item=item, observed_price=None,
        triggered_at=datetime.fromisoformat('2026-09-10T06:20:00+00:00'))
    assert not _cached_monitoring_item_triggered(item=replace(item, close_exit_at=None), observed_price=None,
        triggered_at=datetime.fromisoformat('2026-09-10T06:20:00+00:00'))


@pytest.mark.parametrize('runtime_mode', ['mock', 'paper'])
def test_legacy_import_cannot_grant_rule_buy_authority(monkeypatch, runtime_mode):
    from app.api.routes import execution as execution_routes

    now = datetime.fromisoformat('2026-09-10T00:01:00+00:00')
    monkeypatch.setattr(core_quantity, 'utc_now', lambda: now)
    monkeypatch.setattr(rule_execution, 'utc_now', lambda: now)
    monkeypatch.setattr(execution_routes, 'dispatch_due_execution_jobs',
                        lambda **kwargs: execution_jobs.dispatch_due_execution_jobs(**kwargs, now=now))
    _use_historical_broker_boundary(monkeypatch)
    settings = Settings(database_url='sqlite:///:memory:', runtime_mode=runtime_mode, krx_us_rebound_enabled=True,
                        account_drawdown_gate_enabled=False, kis_account_number='12345678',
                        kis_account_product_code='01',
                        account_drawdown_recovery_scope_secret='rule-test-account-scope-secret-32-bytes')
    app, engine = _build_test_app(settings=settings)
    original = rule_plan()
    legacy = {key: deepcopy(original[key]) for key in ('plan_id', 'market_date', 'requested_at', 'items')}
    legacy.update(plan_id='legacy-rule-import', source='manual-feed')
    legacy['items'][0]['quantity_policy']['sizing_inputs']['cash_per_share'] = '70350'
    for item in legacy['items']:
        item['risk_context']['rule_origin'] = item.pop('rule_origin')
    adapter = StubBrokerAdapter(
        config=BrokerConfig(provider='kis', mode=runtime_mode, configured=True, api_base_url='https://mock.invalid'),
        token_grant=build_token_grant(), order_placement=build_order_placement(broker_order_id='legacy-rule-buy'),
        account_snapshot=BrokerAccountSnapshot(available_cash=Decimal('10000000'), buying_power=Decimal('10000000'),
            total_asset_value=Decimal('10000000'), previous_total_asset_value=Decimal('10000000'),
            observed_at=now, holdings_complete=True),
    )
    adapter.get_live_market_context = lambda *args, **kwargs: BrokerLiveMarketContext(
        symbol='005930', exchange='KRX', observed_at=now, bid=Decimal('69990'), ask=Decimal('70000'),
        freshness_source='kis_exchange_orderbook_time', normal_ordering_available=True,
    )
    adapter.get_domestic_cash_capacity = lambda **kwargs: DomesticCashCapacity(
        provider='kis', runtime_mode=runtime_mode, account_reference='KIS:12345678-01',
        symbol=kwargs['symbol'], exchange='KRX', currency='KRW', requested_price=kwargs['limit_price'],
        quantity_calculation_price=kwargs['limit_price'], orderable_cash_krw=Decimal('9000000'),
        no_misu_buying_amount_krw=Decimal('9000000'), no_misu_buying_quantity=100,
        received_at=now, capacity_id='a' * 64,
    )
    app.state.broker_adapter_factory = lambda _settings: adapter
    with TestClient(app) as client:
        response = client.post('/execution-plans', json=legacy)
        assert response.status_code == 201, response.text
        assert client.post('/controls/resume').status_code == 200
        response = client.post('/execution-jobs/dispatch')
        assert response.status_code == 200, response.text
        assert not adapter.order_calls and response.json()['completed_job_count'] == 0, response.text
        with app.state.db_session_factory() as db:
            assert db.scalars(select(BrokerOrder)).all() == []
            entry = db.scalar(select(ExecutionPlanItem).where(ExecutionPlanItem.side == 'buy'))
            assert rule_execution.rule_entry_block_reason(settings=settings, db=db, plan_item=entry, now=now) == 'rule_v4_contract_unproven'
    engine.dispose()
