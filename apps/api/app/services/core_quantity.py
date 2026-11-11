"""Core-owned cumulative BUY quantity authority, separate from immutable price decisions."""

from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.orm import Session, object_session, selectinload

from app.api.schemas.execution import CoreSizedQuantityPolicy, CoreQuantityRevisionRequest
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit, ExecutionJob, new_id
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.fill_economics import effective_fill_quantity
from app.services.fill_idempotency import broker_reported_filled_quantity, dedupe_execution_fills
from app.services.json_compaction import json_payload_hash

REVISION_EVENT = "core_quantity_revision"
REVISION_BINDING_KEY = "_core_quantity_revision"
KST = ZoneInfo("Asia/Seoul")
LIQUIDITY_EVIDENCE_KEY = 'exec_trigger_time_liquidity_evidence_v1'


def utc_now() -> datetime:
    return datetime.now(UTC)


def is_core_sized(policy: object) -> bool:
    return isinstance(policy, dict) and policy.get("type") == "core_sized"


def core_owned_entry_family(item: ExecutionPlanItem) -> bool:
    if is_core_sized(item.quantity_policy):
        return True
    policy = item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    linked_key = policy.get('linked_entry_item_key')
    context = (item.risk_context or {}).get('entry_context', {})
    linked_id = context.get('execution_plan_item_id') if isinstance(context, dict) else None
    plan = getattr(item, 'plan', None)
    return bool((linked_key or linked_id) and plan is not None and any(
        sibling.side == 'buy' and (sibling.source_plan_item_id == linked_key or sibling.id == linked_id)
        and is_core_sized(sibling.quantity_policy) for sibling in plan.items
    ))


def core_price_contract_level(item: ExecutionPlanItem, intent: str) -> Decimal | None:
    plan = getattr(item, 'plan', None)
    if not is_core_sized(item.quantity_policy) or plan is None:
        return None
    raw_items = (plan.raw_payload or {}).get('items', [])
    prices = [row.get('trigger_price') for row in raw_items if row.get('intent') == intent
              and (row.get('quantity_policy') or {}).get('linked_entry_item_key') == item.source_plan_item_id]
    if len(prices) != 1:
        return None
    try:
        value = Decimal(str(prices[0]))
    except (ValueError, ArithmeticError):
        return None
    return value if value.is_finite() and value > 0 else None


def core_entry_price_ceiling(item: ExecutionPlanItem) -> Decimal | None:
    if not is_core_sized(item.quantity_policy) or item.plan is None:
        return None
    raw = next((row for row in (item.plan.raw_payload or {}).get('items', [])
                if row.get('core_item_key') == item.source_plan_item_id), None)
    if not isinstance(raw, dict):
        return None
    values = [((raw.get('quantity_policy') or {}).get('sizing_inputs') or {}).get('sizing_price')]
    risk = raw.get('risk_context') or {}
    values.append(risk.get('chase_limit_price'))
    positive = []
    for value in values:
        try:
            parsed = Decimal(str(value))
            if parsed.is_finite() and parsed > 0:
                positive.append(parsed)
        except (ValueError, ArithmeticError):
            pass
    return min(positive) if positive else None


def _quantity_policy(policy: object) -> CoreSizedQuantityPolicy:
    base = dict(policy) if isinstance(policy, dict) else {}
    base.pop(LIQUIDITY_EVIDENCE_KEY, None)
    return CoreSizedQuantityPolicy.model_validate(base)


def core_quantity_policy_matches(*, raw_policy: dict, item: ExecutionPlanItem) -> bool:
    try:
        original = CoreSizedQuantityPolicy.model_validate(raw_policy)
        current_policy = dict(item.quantity_policy or {})
        evidence = current_policy.pop(LIQUIDITY_EVIDENCE_KEY, None)
        if evidence is not None:
            from app.services.core_handoff_v4 import _server_trigger_time_liquidity_evidence_matches

            if not _server_trigger_time_liquidity_evidence_matches(item=item, evidence=evidence):
                return False
        current = CoreSizedQuantityPolicy.model_validate(current_policy)
    except ValidationError:
        return False
    if current_policy == raw_policy:
        return True
    if (
        current.revision <= original.revision
        or current.sizing_inputs['price_contract_digest'] != original.sizing_inputs['price_contract_digest']
        or current.sizing_inputs.get('sizing_price') != original.sizing_inputs.get('sizing_price')
    ):
        return False
    binding = (item.risk_context or {}).get(REVISION_BINDING_KEY)
    db = object_session(item)
    if not isinstance(binding, dict) or db is None:
        return False
    audit = db.get(ExecutionPlanItemAudit, binding.get('audit_id'))
    return bool(
        audit is not None and audit.execution_plan_item_id == item.id
        and audit.event_type == REVISION_EVENT
        and audit.payload == {
            'original_policy_sha256': json_payload_hash(raw_policy),
            'quantity_policy': current_policy,
        }
        and binding.get('policy_sha256') == json_payload_hash(current_policy)
    )


def _entry_family_exit_started(*, db: Session, item: ExecutionPlanItem) -> bool:
    from app.services.execution_exposure import broker_order_is_provably_unsubmitted

    exits = db.scalars(select(ExecutionPlanItem).where(
        ExecutionPlanItem.execution_plan_id == item.execution_plan_id,
        ExecutionPlanItem.side == 'sell',
    ).execution_options(populate_existing=True)).all()
    exit_ids = []
    for sibling in exits:
        context = (sibling.risk_context or {}).get('entry_context', {})
        if not (
            (sibling.quantity_policy or {}).get('linked_entry_item_key') == item.source_plan_item_id
            or (isinstance(context, dict) and context.get('execution_plan_item_id') == item.id)
        ):
            continue
        exit_ids.append(sibling.id)
        if sibling.status.endswith('_triggered_pending_dispatch') or sibling.status.startswith('broker_order_') or sibling.status in {'cancel_pending', 'manual_review_required'}:
            return True
    if not exit_ids:
        return False
    orders = db.scalars(select(BrokerOrder).where(
        BrokerOrder.execution_plan_item_id.in_(exit_ids), BrokerOrder.mode == item.plan.mode,
        BrokerOrder.side == 'sell',
    ).options(selectinload(BrokerOrder.events), selectinload(BrokerOrder.fills))).all()
    return any(not broker_order_is_provably_unsubmitted(order) for order in orders)


def core_quantity_state(*, db: Session, item: ExecutionPlanItem, exclude_order_id: str | None = None) -> dict:
    policy = _quantity_policy(item.quantity_policy)
    orders = db.scalars(select(BrokerOrder).where(
        BrokerOrder.execution_plan_item_id == item.id,
        BrokerOrder.side == 'buy', BrokerOrder.mode == item.plan.mode,
    ).options(selectinload(BrokerOrder.fills), selectinload(BrokerOrder.events)).execution_options(populate_existing=True)).all()
    filled = Decimal(0)
    open_quantity = Decimal(0)
    uncertain = False
    for order in orders:
        executed = sum((
            effective_fill_quantity(fill) for fill in dedupe_execution_fills(list(order.fills))
            if fill.side == 'buy'
        ), Decimal(0))
        ordered = Decimal(order.quantity or 0)
        reported = broker_reported_filled_quantity(order) or Decimal(0)
        executed = max(executed, reported)
        # The final broker boundary owns one newly committed, not-yet-submitted
        # intent. It must count every other reservation, without charging itself twice.
        if (order.id == exclude_order_id and order.status == 'submission_pending'
            and not order.broker_order_id and executed == 0
            and order.last_error_classification != 'submit_outcome_unknown'
            and order.last_error_code != 'submit_outcome_unknown'):
            continue
        if ordered <= 0 or executed > ordered:
            uncertain = True
        # A broker-confirmed fill without its detailed fill rows still consumes the target.
        if order.status == 'filled':
            executed = max(executed, ordered)
        filled += executed
        if order.status in CAPITAL_RESERVING_ORDER_STATUSES:
            open_quantity += max(Decimal(0), ordered - executed)
        if (
            order.status == 'manual_review_required'
            or order.last_error_classification == 'submit_outcome_unknown'
            or order.last_error_code == 'submit_outcome_unknown'
            or (order.status in CAPITAL_RESERVING_ORDER_STATUSES and not order.broker_order_id)
        ):
            uncertain = True
    if filled != filled.to_integral_value() or open_quantity != open_quantity.to_integral_value():
        uncertain = True
    reduction = db.scalar(select(ExecutionJob).where(
        ExecutionJob.execution_plan_item_id == item.id, ExecutionJob.job_type == 'core_quantity_reduction',
        ExecutionJob.status.in_(['running', 'retrying']),
    ))
    if reduction is not None:
        uncertain = True
    filled_count, open_count = int(filled), int(open_quantity)
    entry_closed = _entry_family_exit_started(db=db, item=item)
    return {
        'target_quantity': policy.target_quantity, 'revision': policy.revision,
        'filled_quantity': filled_count, 'open_quantity': open_count,
        'remaining_quantity': 0 if entry_closed else max(0, policy.target_quantity - filled_count - open_count),
        'submission_uncertain': uncertain,
        'entry_closed': entry_closed,
    }


def local_unacknowledged_buy_notional(*, db: Session, plan: ExecutionPlan) -> str:
    total = Decimal(0)
    for item in plan.items:
        if item.side != 'buy' or not is_core_sized(item.quantity_policy):
            continue
        price = _reservation_price(item=item)
        orders = db.scalars(select(BrokerOrder).where(
            BrokerOrder.execution_plan_item_id == item.id,
            BrokerOrder.mode == plan.mode, BrokerOrder.side == 'buy',
            BrokerOrder.status.in_(CAPITAL_RESERVING_ORDER_STATUSES),
            BrokerOrder.broker_order_id.is_(None),
        ).options(selectinload(BrokerOrder.fills))).all()
        for order in orders:
            filled = sum((effective_fill_quantity(fill) for fill in dedupe_execution_fills(list(order.fills)) if fill.side == 'buy'), Decimal(0))
            total += max(Decimal(0), Decimal(order.quantity or 0) - filled) * max(price, order.limit_price or Decimal(0))
    return str(total)


def _reservation_price(*, item: ExecutionPlanItem | None, order: BrokerOrder | None = None) -> Decimal:
    values = [order.limit_price if order is not None else None]
    if item is not None:
        values.extend([item.limit_price, item.trigger_price])
        inputs = (item.quantity_policy or {}).get('sizing_inputs', {})
        values.extend(inputs.get(key) for key in ('reference_price', 'sizing_price', 'chase_limit_price'))
        values.append((item.risk_context or {}).get('chase_limit_price'))
    positive = [Decimal(0)]
    for value in values:
        try:
            parsed = Decimal(str(value))
            if parsed.is_finite() and parsed > 0:
                positive.append(parsed)
        except (ValueError, ArithmeticError):
            pass
    return max(positive)


def account_quantity_reservations(*, db: Session, settings) -> dict:
    from app.services.monitoring import _kis_order_scope_classification
    from app.services.fifo_inventory import fifo_route_scope_from_order
    from app.services.execution_semantics import is_canonical_krx_short_code

    uncertain = False
    local_notional = Decimal(0)
    orders = db.scalars(select(BrokerOrder).where(
        BrokerOrder.mode == settings.runtime_mode, BrokerOrder.broker_name == 'kis',
        BrokerOrder.side == 'buy', BrokerOrder.status.in_(CAPITAL_RESERVING_ORDER_STATUSES),
    ).options(selectinload(BrokerOrder.plan_item), selectinload(BrokerOrder.fills))).all()
    for order in orders:
        route = fifo_route_scope_from_order(order)
        if order.asset_class not in {None, 'stock'} or (route is not None and route.exchange not in {'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'}):
            continue
        if route is None and not is_canonical_krx_short_code(order.symbol):
            continue
        scope, exchange = _kis_order_scope_classification(settings=settings, order=order)
        if scope == 'foreign' or (exchange and exchange not in {'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'}):
            continue
        if scope != 'active' or order.status in {'manual_review_required', 'cancel_pending'}:
            uncertain = True
        if order.broker_order_id:
            continue
        uncertain = True
        reference = _reservation_price(item=order.plan_item, order=order)
        reported = broker_reported_filled_quantity(order) or Decimal(0)
        detailed = sum((effective_fill_quantity(fill) for fill in dedupe_execution_fills(list(order.fills)) if fill.side == 'buy'), Decimal(0))
        local_notional += max(Decimal(0), Decimal(order.quantity or 0) - max(reported, detailed)) * max(Decimal(0), reference or Decimal(0))
    return {
        'account_submission_uncertain': uncertain,
        'account_local_unacknowledged_buy_notional_krw': str(local_notional),
    }


def runtime_core_quantity_account(*, db: Session, settings, scope_available: bool) -> dict:
    from sqlalchemy.exc import SQLAlchemyError

    result = {
        'as_of': utc_now().isoformat(),
        'account_submission_uncertain': True,
        'account_local_unacknowledged_buy_notional_krw': None,
        'limits': {
            'max_symbol_budget_krw': str(settings.exec_agent_max_symbol_budget),
            'max_total_budget_krw': str(settings.exec_agent_max_total_budget),
            'risk_fraction': str(settings.exec_agent_max_trade_risk_pct),
            'max_symbol_fraction': str(min(
                settings.exec_agent_max_symbol_total_exposure_pct,
                settings.exec_agent_max_symbol_exposure_pct,
                settings.exec_agent_max_position_allocation_pct,
            )),
        },
    }
    if scope_available:
        try:
            with db.begin_nested():
                result.update(account_quantity_reservations(db=db, settings=settings))
        except SQLAlchemyError:
            pass
    return result


def executable_core_quantity(*, db: Session, item: ExecutionPlanItem, now: datetime | None = None, exclude_order_id: str | None = None) -> int:
    now = now or utc_now()
    policy = _quantity_policy(item.quantity_policy)
    if not policy.calculated_at <= now < policy.valid_until:
        return 0
    local = now.astimezone(KST)
    if local.date() != item.plan.market_date or not time(9) <= local.time() < time(15, 30):
        return 0
    state = core_quantity_state(db=db, item=item, exclude_order_id=exclude_order_id)
    return 0 if state['submission_uncertain'] else state['remaining_quantity']


def core_quantity_wait_until(*, db: Session, item: ExecutionPlanItem, now: datetime | None = None) -> datetime | None:
    now = now or utc_now()
    policy = _quantity_policy(item.quantity_policy)
    if now < policy.calculated_at < policy.valid_until:
        state = core_quantity_state(db=db, item=item)
        if state['remaining_quantity'] and not state['submission_uncertain']:
            return policy.calculated_at
    return None


def core_quantity_order_failure(*, db, item, command, exclude_order_id=None):
    if item is None or command.side != 'buy' or not is_core_sized(item.quantity_policy):
        return None
    remaining = executable_core_quantity(db=db, item=item, exclude_order_id=exclude_order_id)
    if (command.metadata.get('core_quantity_revision') != item.quantity_policy.get('revision')
        or type(command.quantity) is not int or not 0 < command.quantity <= remaining):
        return 'Core cumulative buy quantity authority changed, exhausted, or expired'
    return None


def core_quantity_idempotency_key(*, item: ExecutionPlanItem, base_key: str) -> str:
    db = object_session(item)
    if db is None:
        return base_key
    orders = db.scalars(select(BrokerOrder).where(
        BrokerOrder.execution_plan_item_id == item.id, BrokerOrder.mode == item.plan.mode,
        BrokerOrder.side == 'buy',
        (BrokerOrder.idempotency_key == base_key) | BrokerOrder.idempotency_key.startswith(f'{base_key}:residual:', autoescape=True),
    ).execution_options(populate_existing=True)).all()
    by_key = {order.idempotency_key: order for order in orders}
    state = core_quantity_state(db=db, item=item)
    key = base_key
    visited = set()
    while key in by_key and key not in visited:
        visited.add(key)
        order = by_key[key]
        next_key = f'{base_key}:residual:{order.id}'
        # An existing pending/unknown successor keeps its identity across restarts.
        if next_key in by_key:
            key = next_key
            continue
        if (
            order.broker_order_id and order.status in {'filled', 'canceled', 'expired'}
            and order.last_error_classification != 'submit_outcome_unknown'
            and order.last_error_code != 'submit_outcome_unknown'
            and state['remaining_quantity'] and not state['submission_uncertain']
        ):
            return next_key
        break
    return key


def revise_core_quantities(*, settings, db: Session, command: CoreQuantityRevisionRequest) -> dict:
    from app.services.core_handoff_v4 import persisted_core_plan_contract_ack
    from app.services.core_revocation_exposure import (
        RevocationExposureSerializationBusy, serialized_revocation_exposure,
    )

    results = []
    for request in command.items:
        result = {'core_item_key': request.core_item_key, 'execution_plan_id': request.execution_plan_id}
        try:
            item = db.scalar(select(ExecutionPlanItem).join(ExecutionPlanItem.plan).where(
                ExecutionPlan.id == request.execution_plan_id,
                ExecutionPlan.external_plan_id == request.external_plan_id,
                ExecutionPlan.mode == settings.runtime_mode,
                ExecutionPlan.source == 'kai-trading-core',
                ExecutionPlanItem.source_plan_item_id == request.core_item_key,
                ExecutionPlanItem.decision_snapshot_id == request.decision_snapshot_id,
                ExecutionPlanItem.side == 'buy',
            ).options(selectinload(ExecutionPlanItem.plan)))
            if item is None:
                raise ValueError('quantity_item_binding_mismatch')
            with serialized_revocation_exposure(db=db, settings=settings, items=[item]):
                db.refresh(item, with_for_update=True)
                if not is_core_sized(item.quantity_policy) or persisted_core_plan_contract_ack(item.plan) is None:
                    raise ValueError('quantity_contract_unproven')
                old = _quantity_policy(item.quantity_policy)
                new = CoreSizedQuantityPolicy.model_validate(request.model_dump(include=set(CoreSizedQuantityPolicy.model_fields)))
                result['execution_item_id'] = item.id
                state = core_quantity_state(db=db, item=item)
                result.update(state)
                if new.model_dump() == old.model_dump():
                    if state['remaining_quantity'] and old.valid_until > utc_now() and item.status not in {
                        'core_activation_pending', 'revoked', 'superseded', 'manual_review_required', 'contract_quarantined',
                    }:
                        if state['open_quantity'] or state['submission_uncertain'] or not _rearm_quantity_item(db=db, item=item, now=utc_now()):
                            from app.services.core_quantity_reductions import queue_core_quantity_reduction

                            queue_core_quantity_reduction(db=db, item=item, now=utc_now())
                        db.commit()
                    else:
                        db.rollback()
                    results.append({**result, 'status': 'unchanged'})
                    continue
                if old.revision != request.expected_revision or new.revision != old.revision + 1:
                    raise ValueError('quantity_revision_conflict')
                if new.sizing_inputs['price_contract_digest'] != old.sizing_inputs['price_contract_digest']:
                    raise ValueError('quantity_price_contract_changed')
                if new.sizing_inputs.get('sizing_price') != old.sizing_inputs.get('sizing_price'):
                    raise ValueError('quantity_entry_price_changed')
                now = utc_now()
                local_expiry = new.valid_until.astimezone(KST)
                if (
                    new.calculated_at < old.calculated_at or new.calculated_at > now + timedelta(seconds=30)
                    or new.valid_until <= now or local_expiry.date() != item.plan.market_date
                    or local_expiry.time() > time(15, 30)
                ):
                    raise ValueError('quantity_revision_expired_or_wrong_session')
                if item.status in {'revoked', 'superseded', 'manual_review_required', 'contract_quarantined'}:
                    raise ValueError('quantity_item_not_active')
                if state['submission_uncertain'] and new.target_quantity > old.target_quantity:
                    raise ValueError('quantity_submission_uncertain')
                raw_items = (item.plan.raw_payload or {}).get('items', [])
                original = next(row['quantity_policy'] for row in raw_items if row['core_item_key'] == item.source_plan_item_id)
                policy = new.model_dump(mode='json')
                audit_id = new_id()
                db.add(ExecutionPlanItemAudit(
                    id=audit_id, execution_plan_item_id=item.id, event_type=REVISION_EVENT,
                    source='core_quantity', occurred_at=now,
                    summary={'revision': new.revision, 'target_quantity': new.target_quantity},
                    payload={'original_policy_sha256': json_payload_hash(original), 'quantity_policy': policy},
                ))
                revised_risk = {
                    **(item.risk_context or {}),
                    REVISION_BINDING_KEY: {'audit_id': audit_id, 'policy_sha256': json_payload_hash(policy)},
                }
                changed = db.execute(update(ExecutionPlanItem).where(
                    ExecutionPlanItem.id == item.id,
                    ExecutionPlanItem.quantity_policy['revision'].as_integer() == request.expected_revision,
                ).values(quantity_policy=policy, risk_context=revised_risk), execution_options={'synchronize_session': False})
                if changed.rowcount != 1:
                    raise ValueError('quantity_revision_conflict')
                db.refresh(item)
                db.flush()
                state = core_quantity_state(db=db, item=item)
                if (
                    (new.target_quantity < state['filled_quantity'] + state['open_quantity'] and state['open_quantity'])
                    or (state['remaining_quantity'] and (state['open_quantity'] or state['submission_uncertain']))
                ):
                    from app.services.core_quantity_reductions import queue_core_quantity_reduction

                    queue_core_quantity_reduction(db=db, item=item, now=now)
                if state['remaining_quantity'] and not state['open_quantity'] and not state['submission_uncertain']:
                    if not _rearm_quantity_item(db=db, item=item, now=now):
                        from app.services.core_quantity_reductions import queue_core_quantity_reduction

                        queue_core_quantity_reduction(db=db, item=item, now=now)
                db.commit()
                result.update(state)
                if new.target_quantity < state['filled_quantity'] + state['open_quantity']:
                    result['reason_code'] = 'reduction_deferred_open_orders' if state['open_quantity'] else 'filled_quantity_preserved'
                results.append({**result, 'status': 'updated'})
        except (ValueError, RevocationExposureSerializationBusy) as exc:
            db.rollback()
            reason = str(exc) if isinstance(exc, ValueError) else 'quantity_scope_busy'
            results.append({**result, 'status': 'conflict' if reason == 'quantity_revision_conflict' else 'blocked', 'reason_code': reason})
    return {
        'updated_count': sum(row['status'] == 'updated' for row in results),
        'unchanged_count': sum(row['status'] == 'unchanged' for row in results),
        'failed_count': sum(row['status'] not in {'updated', 'unchanged'} for row in results),
        'items': results,
    }


def _rearm_quantity_item(*, db: Session, item: ExecutionPlanItem, now: datetime) -> bool:
    from app.services.execution_jobs import materialize_execution_jobs

    # Quantity changes never grant missing activation or revive revoked authority.
    if item.status in {'core_activation_pending', 'revoked', 'superseded', 'manual_review_required', 'contract_quarantined'}:
        return False
    if now.astimezone(KST).date() != item.plan.market_date or now.astimezone(KST).time() >= time(15, 30):
        return False
    if item.intent == 'intraday_watch_buy':
        item.status = 'watch_monitoring_pending'
    elif item.intent == 'next_open_buy':
        job = db.scalar(select(ExecutionJob).where(
            ExecutionJob.execution_plan_item_id == item.id, ExecutionJob.job_type == 'next_open_buy',
        ).with_for_update())
        if job is not None and job.status == 'running':
            return False
        if job is not None:
            job.status = 'scheduled'
            job.locked_at = None
            job.last_error = None
        item.status = 'pending'
        materialize_execution_jobs(item.plan, only_item_ids={item.id})
    return True
