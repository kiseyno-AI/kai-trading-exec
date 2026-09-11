from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import hashlib
import json

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.core.config import Settings
from app.api.schemas.execution import core_execution_plan_material_fingerprint
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import ExecutionJob, ExecutionPlan, ExecutionPlanItem


CONTRACT_VERSION = 'execution_handoff_contract_v4'
PLAN_SCHEMA_VERSION = 'signal_decider_exec_plan_v4'
PLAN_CAPABILITY = 'delegated_final_full_cost_gate_recheck_v1'
CANONICAL_FINGERPRINT_VECTOR = (
    '375c4f82a02c723364675d9999ae9d0bb9e41a46d7ae54654d070e5f45bb54b6'
)


def _canonical_number(value: int | float | Decimal) -> str:
    number = Decimal(str(value)) if isinstance(value, float) else Decimal(value)
    assert number.is_finite()
    return '0' if number == 0 else str(number.normalize()).lower()


def _canonical(value: object) -> object:
    if value is None:
        return {'$null': True}
    if isinstance(value, bool):
        return {'$bool': value}
    if isinstance(value, (int, float, Decimal)):
        return {'$number': _canonical_number(value)}
    if isinstance(value, str):
        return {'$string': value}
    if isinstance(value, list):
        return {'$array': [_canonical(item) for item in value]}
    assert isinstance(value, dict)
    return {
        '$object': [
            [key, _canonical(value[key])]
            for key in sorted(value)
        ]
    }


def _material_fingerprint(payload: dict[str, object]) -> str:
    material = deepcopy(payload)
    material.pop('requested_at', None)
    metadata = material['metadata']
    assert isinstance(metadata, dict)
    metadata.pop('requested_at', None)
    metadata.pop('trigger', None)
    metadata.pop('dispatch_material_fingerprint', None)
    encoded = json.dumps(
        _canonical(material),
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _item(
    *,
    key: str,
    side: str,
    intent: str,
    entry_key: str,
) -> dict[str, object]:
    return {
        'core_item_key': key,
        'source_plan_item_id': key,
        'agent_name': 'signal-decider',
        'decision_agent_name': 'signal-decider',
        'symbol': '005930',
        'asset_class': 'stock',
        'market': 'KRX',
        'exchange': 'KRX',
        'currency': 'KRW',
        'broker': 'kis',
        'provider': 'kis',
        'venue': 'kis',
        'session_policy': {
            'session': 'regular',
            'timezone': 'Asia/Seoul',
            'allow_extended_hours': False,
        },
        'side': side,
        'intent': intent,
        'quantity_policy': (
            {'type': 'exec_agent_sized', 'budget_krw': 100_000}
            if side == 'buy'
            else {
                'type': 'position_exit_from_entry',
                'linked_entry_item_key': entry_key,
            }
        ),
        'limit_price': None,
        'trigger_price': 72_000 if side == 'sell' else None,
        'time_in_force': 'day',
        'risk_context': {},
        'decision_snapshot_id': '101',
    }


def _plan_payload(*, plan_id: str) -> dict[str, object]:
    entry_key = 'core-plan-1:next_open_buy'
    payload: dict[str, object] = {
        'plan_id': plan_id,
        'source': 'kai-trading-core',
        'mode': 'mock',
        'market_date': '2026-09-10',
        'strategy_run_id': 'signal-decider:2026-09-10:test',
        'requested_at': '2026-09-09T12:00:00Z',
        'items': [
            _item(
                key=entry_key,
                side='buy',
                intent='next_open_buy',
                entry_key=entry_key,
            ),
            _item(
                key='core-plan-1:target_sell',
                side='sell',
                intent='target_sell',
                entry_key=entry_key,
            ),
            _item(
                key='core-plan-1:stop_sell',
                side='sell',
                intent='stop_sell',
                entry_key=entry_key,
            ),
        ],
        'metadata': {
            'schema_version': PLAN_SCHEMA_VERSION,
            'contract_version': CONTRACT_VERSION,
            'required_capability': PLAN_CAPABILITY,
            'agent_name': 'signal-decider',
            'decision_agent_name': 'signal-decider',
            'trigger': 'test',
            'requested_at': '2026-09-09T12:00:00Z',
            'account_reference': 'KIS:00000000-00',
            'account_label': 'TEST',
            'source_market_date': '2026-09-09',
            'target_trade_date': '2026-09-10',
            'dispatch_batch_type': 'initial',
            'dispatch_batch_key': None,
        },
    }
    payload['metadata']['dispatch_material_fingerprint'] = _material_fingerprint(payload)
    return payload


def _build_test_app(*, automation_enabled: bool = False):
    settings = Settings(
        database_url='sqlite:///:memory:',
        runtime_mode='mock',
        automation_enabled=automation_enabled,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
    )
    return app, engine


def test_material_fingerprint_matches_core_canonical_vector() -> None:
    payload = {
        'plan_id': 'canonical-vector',
        'requested_at': 'ignored-top',
        'metadata': {
            'trigger': 'ignored-trigger',
            'requested_at': 'ignored-metadata',
            'dispatch_material_fingerprint': 'f' * 64,
            'numeric': 1,
        },
        'nested': [
            True,
            1,
            1.0,
            Decimal('1.000'),
            '1',
            None,
            {'b': 2, 'a': '한글'},
        ],
    }

    assert (
        core_execution_plan_material_fingerprint(payload)
        == CANONICAL_FINGERPRINT_VECTOR
    )


def test_v4_recomputes_material_fingerprint_and_echoes_it_from_create_and_list() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id='material-bound-plan')
    fingerprint = payload['metadata']['dispatch_material_fingerprint']

    created = client.post('/v4/execution-plans', json=payload)

    assert created.status_code == 201, created.json()
    assert created.json()['contract_ack']['dispatch_material_fingerprint'] == fingerprint
    listed = client.get(
        '/v4/execution-plans',
        params={'external_plan_id': payload['plan_id']},
    )
    assert listed.status_code == 200
    assert listed.json()[0]['contract_ack']['dispatch_material_fingerprint'] == fingerprint

    tampered = deepcopy(payload)
    tampered['items'][0]['quantity_policy']['budget_krw'] = 200_000
    rejected = client.post('/v4/execution-plans', json=tampered)
    assert rejected.status_code == 422
    assert 'dispatch material fingerprint mismatch' in str(rejected.json())
    engine.dispose()


def test_v4_requires_a_lowercase_sha256_material_fingerprint() -> None:
    app, engine = _build_test_app()
    client = TestClient(app)
    payload = _plan_payload(plan_id='material-fingerprint-format')

    missing = deepcopy(payload)
    del missing['metadata']['dispatch_material_fingerprint']
    assert client.post('/v4/execution-plans', json=missing).status_code == 422

    uppercase = deepcopy(payload)
    uppercase['metadata']['dispatch_material_fingerprint'] = str(
        uppercase['metadata']['dispatch_material_fingerprint']
    ).upper()
    assert client.post('/v4/execution-plans', json=uppercase).status_code == 422
    engine.dispose()


def test_v4_never_accepts_a_short_term_family_rejected_by_the_exec_gate() -> None:
    app, engine = _build_test_app(automation_enabled=True)
    client = TestClient(app)
    payload = _plan_payload(plan_id='short-term-gate-rejected-plan')
    payload['metadata']['agent_name'] = 'short-term-decider'
    payload['metadata']['decision_agent_name'] = 'short-term-decider'
    for item in payload['items']:
        item['agent_name'] = 'short-term-decider'
        item['decision_agent_name'] = 'short-term-decider'
    payload['items'][0]['risk_context']['decider_output'] = {
        'decision_action': 'buy',
    }
    payload['metadata']['dispatch_material_fingerprint'] = _material_fingerprint(payload)

    response = client.post('/v4/execution-plans', json=payload)

    assert response.status_code == 422, response.json()
    body = response.json()
    assert body['accepted_item_count'] == 0
    assert body['rejected_item_count'] == 3
    assert 'contract_ack' not in body
    assert body['validation_failures'][0]['reason_code'] == (
        'protected_entry_family_not_claimable'
    )
    with app.state.db_session_factory() as session:
        assert session.scalars(select(ExecutionPlan)).all() == []
        assert session.scalars(select(ExecutionPlanItem)).all() == []
        assert session.scalars(select(ExecutionJob)).all() == []
    engine.dispose()
