"""Durably cancel excess unfilled Core BUY capacity in the separate dispatch worker."""

from datetime import UTC, timedelta

from sqlalchemy import and_, case, or_, select, update
from sqlalchemy.orm import selectinload

from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionJob, ExecutionPlan, ExecutionPlanItem
from app.services.broker_order_state import CAPITAL_RESERVING_ORDER_STATUSES
from app.services.core_quantity import KST, core_quantity_state, utc_now
from app.services.plan_item_identity import rule_origin_for_plan_item

JOB_TYPE = 'core_quantity_reduction'
CANCEL_INTENT = 'core_quantity_cancel_requested'


def _needs_cancellation(*, item, state):
    return state['open_quantity'] > 0 and (
        state['entry_closed'] and rule_origin_for_plan_item(item) is not None
        or state['filled_quantity'] + state['open_quantity'] > state['target_quantity']
    )


def _assert_cancel_scope(*, settings, adapter, item, order):
    from app.services.execution_account_route import validate_kis_account_route
    from app.services.reconciliation import _order_reconciliation_scope_rejection

    if (
        item.provider != 'kis' or item.venue != 'kis' or item.asset_class != 'stock'
        or order.broker_name != 'kis' or order.asset_class != 'stock'
        or order.symbol != item.symbol or order.side != 'buy'
        or validate_kis_account_route(
            settings=settings, provider=item.provider, risk_context=item.risk_context,
            quantity_policy=item.quantity_policy, require_binding=True,
        ) is not None
        or _order_reconciliation_scope_rejection(
            settings=settings, adapter=adapter, order=order,
        ) is not None
    ):
        raise ValueError('quantity_cancel_account_route_mismatch')
    if rule_origin_for_plan_item(item) is not None:
        from app.services.broker_orders import _strategy_entry_family_identity, _validated_order_strategy_family_identity

        family, error = _validated_order_strategy_family_identity(order)
        if error is not None or order.execution_plan_item_id != item.id or family != _strategy_entry_family_identity(item):
            raise ValueError('quantity_cancel_rule_family_mismatch')


def _reconcile_before_cancel(*, settings, db, adapter, item, order):
    from app.services.reconciliation import reconcile_broker_order

    _assert_cancel_scope(settings=settings, adapter=adapter, item=item, order=order)
    result = reconcile_broker_order(settings=settings, db=db, adapter=adapter, order=order)
    if not isinstance(result, dict) or result.get('status_code') != 200:
        raise ValueError('quantity_cancel_reconciliation_unavailable')
    db.refresh(order)
    db.refresh(item)


def _owns_claim(job, claimed_at):
    locked = job.locked_at
    return job.status == 'running' and locked is not None and (
        locked.replace(tzinfo=UTC) if locked.tzinfo is None else locked
    ) == claimed_at


def _has_unresolved_cancel(*, db, order):
    events = db.scalars(select(BrokerOrderEvent).where(
        BrokerOrderEvent.broker_order_pk == order.id,
        BrokerOrderEvent.event_type.in_([CANCEL_INTENT, 'core_quantity_cancel_ack']),
    )).all()
    never_submitted = {
        event.payload.get('intent_event_id') for event in events
        if event.event_type == 'core_quantity_cancel_ack'
        and isinstance(event.payload, dict) and event.payload.get('status') == 'not_cancelable'
    }
    return any(event.event_type == CANCEL_INTENT and event.id not in never_submitted for event in events)


def queue_core_quantity_reduction(*, db, item, now, nowait=False):
    job = db.scalar(select(ExecutionJob).where(
        ExecutionJob.execution_plan_item_id == item.id, ExecutionJob.job_type == JOB_TYPE,
    ).with_for_update(nowait=nowait))
    if job is None:
        db.add(ExecutionJob(plan_item=item, job_type=JOB_TYPE, status='queued', scheduled_for=now))
    elif job.status != 'running':
        job.status, job.scheduled_for, job.last_error = 'queued', now, None


def process_core_quantity_reductions(*, settings, db, adapter, now=None, krx_trading_day=None, rule_only=False):
    from app.services.broker_auth import ensure_authenticated_broker_session
    from app.services.core_handoff_v4 import persisted_core_plan_contract_ack
    from app.services.core_quantity import _rearm_quantity_item, core_quantity_wait_until, executable_core_quantity, maintain_rule_entry_closures
    from app.services.core_revocation_exposure import serialized_revocation_exposure

    now = now or utc_now()
    maintain_rule_entry_closures(settings=settings, db=db, now=now, commit_rule_caller_state=True)
    processed_jobs = 0
    closed_rule = and_(
        ExecutionPlan.source == 'kai-trading-core',
        ExecutionPlan.raw_payload['origin_type'].as_string() == 'rule',
        ExecutionPlanItem.risk_context['_rule_entry_closed']['schema_version'].as_string() == 'rule_entry_closed_v1',
    )
    query = select(ExecutionJob).join(ExecutionJob.plan_item).join(ExecutionPlan).where(
        ExecutionPlan.mode == settings.runtime_mode,
        closed_rule if rule_only else or_(ExecutionPlan.market_date == now.astimezone(KST).date(), closed_rule),
        ExecutionJob.job_type == JOB_TYPE,
        ExecutionJob.status.in_(['queued', 'retrying', 'running']),
        ExecutionJob.scheduled_for <= now,
    ).options(selectinload(ExecutionJob.plan_item)).order_by(
        # Closed-rule reconciliation remains eligible when an older agent job
        # is held by its trading-session guard; keep the single-job bound.
        case((closed_rule, 0), else_=1), ExecutionJob.scheduled_for, ExecutionJob.id,
    ).limit(1)
    jobs = db.scalars(query).all()
    for job in jobs:
        item = job.plan_item
        local = now.astimezone(KST)
        is_closed_rule = rule_origin_for_plan_item(item) is not None and '_rule_entry_closed' in (item.risk_context or {})
        in_session = 9 <= local.hour < 16 and not (local.hour == 15 and local.minute >= 30)
        if not is_closed_rule:
            if local.date() != item.plan.market_date or not in_session:
                continue
            try:
                if krx_trading_day is None or krx_trading_day(item.plan.market_date) is not True:
                    continue
            except Exception:
                continue
        if job.status == 'running' and job.locked_at is not None:
            locked = job.locked_at.replace(tzinfo=now.tzinfo) if job.locked_at.tzinfo is None else job.locked_at
            if now - locked < timedelta(seconds=60):
                continue
        previous_status = job.status
        previous_lock = job.locked_at
        claimed = db.execute(update(ExecutionJob).where(
            ExecutionJob.id == job.id, ExecutionJob.status == previous_status,
            ExecutionJob.locked_at == previous_lock,
        ).values(status='running', locked_at=now, attempts=ExecutionJob.attempts + 1), execution_options={'synchronize_session': False})
        if claimed.rowcount != 1:
            db.rollback()
            continue
        db.commit()
        processed_jobs += 1
        try:
            if persisted_core_plan_contract_ack(item.plan) is None:
                raise ValueError('quantity_cancel_contract_unproven')
            orders = db.scalars(select(BrokerOrder).where(
                BrokerOrder.execution_plan_item_id == item.id, BrokerOrder.side == 'buy',
                BrokerOrder.mode == settings.runtime_mode,
                BrokerOrder.status.in_(CAPITAL_RESERVING_ORDER_STATUSES),
            ).order_by(BrokerOrder.created_at).options(selectinload(BrokerOrder.events))).all()
            for order in orders:
                _reconcile_before_cancel(settings=settings, db=db, adapter=adapter, item=item, order=order)
                state = core_quantity_state(db=db, item=item)
                if not _needs_cancellation(item=item, state=state):
                    break
                if order.status not in CAPITAL_RESERVING_ORDER_STATUSES:
                    continue
                if not order.broker_order_id or order.status == 'manual_review_required':
                    raise ValueError('quantity_cancel_submission_uncertain')
                # Never repeat a possibly accepted cancellation after restart or lost ACK.
                if _has_unresolved_cancel(db=db, order=order):
                    raise ValueError('cancel_outcome_awaiting_reconciliation')
                if is_closed_rule:
                    # Prior-session/closed-market recovery may read broker truth;
                    # a new cancellation still requires today's proven session.
                    if not in_session or krx_trading_day is None or krx_trading_day(local.date()) is not True:
                        raise ValueError('rule_cancel_market_session_closed')
                cancel = getattr(adapter, 'cancel_order', None)
                if not callable(cancel):
                    raise ValueError('quantity_cancel_provider_unsupported')
                session = ensure_authenticated_broker_session(
                    settings=settings, db=db, adapter=adapter, source=JOB_TYPE,
                    reason='reduce Core-authorized unfilled buy capacity',
                )
                with serialized_revocation_exposure(db=db, settings=settings, items=[item]):
                    db.refresh(item)
                    db.refresh(order)
                    db.refresh(job, with_for_update=True)
                    if not _owns_claim(job, now):
                        raise ValueError('quantity_cancel_claim_lost')
                    if _has_unresolved_cancel(db=db, order=order):
                        raise ValueError('cancel_outcome_awaiting_reconciliation')
                    _assert_cancel_scope(settings=settings, adapter=adapter, item=item, order=order)
                    state = core_quantity_state(db=db, item=item)
                    if not _needs_cancellation(item=item, state=state):
                        break
                    order.status = 'cancel_pending'
                    intent = BrokerOrderEvent(
                        broker_order_pk=order.id, event_type=CANCEL_INTENT, event_source=JOB_TYPE,
                        normalized_status='cancel_pending', observed_at=now,
                        payload={
                            'revision': item.quantity_policy['revision'], 'target_quantity': state['target_quantity'],
                            **({'entry_closure': item.risk_context['_rule_entry_closed'],
                                'effective_cancellation_threshold': state['filled_quantity']} if is_closed_rule else {}),
                        },
                    )
                    db.add(intent)
                    db.commit()
                db.refresh(job)
                if not _owns_claim(job, now):
                    raise ValueError('quantity_cancel_claim_lost_after_intent')
                acknowledgement = cancel(session.access_token or '', broker_order_id=order.broker_order_id, symbol=order.symbol, exchange='KRX')
                db.add(BrokerOrderEvent(
                    broker_order_pk=order.id, event_type='core_quantity_cancel_ack', event_source=JOB_TYPE,
                    normalized_status='cancel_pending', observed_at=utc_now(),
                    payload={**acknowledgement, 'intent_event_id': intent.id},
                ))
                db.commit()
                # An ACK accepts cancellation, not its final filled/canceled outcome.
                _reconcile_before_cancel(settings=settings, db=db, adapter=adapter, item=item, order=order)
                break
            with serialized_revocation_exposure(db=db, settings=settings, items=[item]):
                db.refresh(item, with_for_update=True)
                db.refresh(job, with_for_update=True)
                if not _owns_claim(job, now):
                    continue
                state = core_quantity_state(db=db, item=item)
                if state['open_quantity']:
                    job.status, job.last_error = 'retrying', (
                        'cancel_outcome_awaiting_reconciliation'
                        if _needs_cancellation(item=item, state=state)
                        else 'quantity_followup_awaiting_open_buy'
                    )
                    job.scheduled_for = utc_now() + timedelta(seconds=15)
                else:
                    job.status, job.last_error = 'completed', None
                    db.flush()
                    state = core_quantity_state(db=db, item=item)
                    if (state['remaining_quantity'] or state['entry_closed'] and rule_origin_for_plan_item(item) is not None) and state['submission_uncertain']:
                        job.status, job.last_error = 'retrying', 'quantity_followup_submission_uncertain'
                        job.scheduled_for = utc_now() + timedelta(seconds=15)
                    elif item.status not in {
                        'core_activation_pending', 'revoked', 'superseded', 'manual_review_required', 'contract_quarantined',
                    }:
                        ready_at = core_quantity_wait_until(db=db, item=item)
                        if ready_at is not None:
                            job.status, job.last_error = 'retrying', 'quantity_followup_not_yet_valid'
                            job.scheduled_for = ready_at
                        elif executable_core_quantity(db=db, item=item) and not _rearm_quantity_item(db=db, item=item, now=utc_now()):
                            job.status, job.last_error = 'retrying', 'quantity_followup_dispatch_in_flight'
                            job.scheduled_for = utc_now() + timedelta(seconds=15)
                db.commit()
        except Exception as exc:
            db.rollback()
            db.refresh(job)
            if not _owns_claim(job, now):
                continue
            job.status, job.last_error = 'retrying', str(exc) if isinstance(exc, ValueError) else 'cancel_outcome_unknown'
            job.scheduled_for = utc_now() + timedelta(seconds=30)
            db.commit()
    return processed_jobs
