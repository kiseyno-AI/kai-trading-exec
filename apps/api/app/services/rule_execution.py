"""Rule-specific entry fences on the existing serialized execution path."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hmac
from inspect import iscoroutinefunction
import re
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import CoreRuleOriginV1, CoreRuleSessionV1
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services.execution_exposure import broker_order_is_provably_unsubmitted
from app.services.execution_account_route import classify_kis_account_route, configured_kis_account_reference
from app.services.plan_item_identity import (
    plan_item_inventory_scope_key, plan_item_strategy_lane, rule_origin_for_plan_item,
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def rule_registration_conflict_reason(*, settings: Settings, db: Session, command) -> str | None:
    """A later evaluation cannot acquire the first evaluation's filled family."""
    symbol = command.items[0].symbol
    items = db.scalars(select(ExecutionPlanItem).join(ExecutionPlanItem.plan).where(
        ExecutionPlan.mode == settings.runtime_mode, ExecutionPlan.market_date == command.market_date,
        ExecutionPlanItem.symbol == symbol, ExecutionPlanItem.side == 'buy',
    ).options(selectinload(ExecutionPlanItem.broker_orders))).all()
    retired = {'revoked', 'superseded', 'rejected', 'expired', 'canceled', 'cancelled',
               'contract_quarantined', 'manual_review_resolved_no_exposure'}
    for item in items:
        risk = item.risk_context if isinstance(item.risk_context, dict) else {}
        if 'rule_origin' not in risk:
            continue
        scope = classify_kis_account_route(settings=settings, provider=item.provider,
                                           risk_context=risk, quantity_policy=item.quantity_policy)
        if scope == 'foreign':
            continue
        if scope != 'active' or rule_origin_for_plan_item(item) is None:
            return 'rule_session_ownership_unproven'
        if item.status not in retired or any(not broker_order_is_provably_unsubmitted(order) for order in item.broker_orders):
            return 'rule_session_reentry_forbidden'
    return None


def rule_close_exit_at(item: ExecutionPlanItem) -> datetime | None:
    if rule_origin_for_plan_item(item) is None:
        return None
    return rule_close_exit_at_from_context(item.risk_context)


def rule_close_exit_at_from_context(risk_context: object) -> datetime | None:
    risk = risk_context if isinstance(risk_context, dict) else {}
    if 'rule_origin' not in risk:
        return None
    try:
        origin = CoreRuleOriginV1.model_validate(risk.get('rule_origin'))
        session = CoreRuleSessionV1.model_validate(risk.get('rule_session'))
        deadline = datetime.fromisoformat(risk['close_exit_at'].replace('Z', '+00:00'))
    except (ValidationError, ValueError, TypeError, KeyError, AttributeError):
        return None
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        return None
    expected = session.session_close_at - timedelta(seconds=session.close_buffer_seconds)
    local_close = session.session_close_at.astimezone(ZoneInfo('Asia/Seoul'))
    local_exit = deadline.astimezone(ZoneInfo('Asia/Seoul'))
    return deadline if deadline == expected and local_close.date() == local_exit.date() == origin.target_trade_date else None


def rule_cash_capacity_failure(
    *, settings: Settings, adapter, access_token: str, command, item: ExecutionPlanItem,
    local_unacknowledged_notional: Decimal, local_unknown_count: int,
) -> str | None:
    """Conservative cash-only ceiling, never a settled-cash/order guarantee."""
    if settings.runtime_mode == 'mock':
        return None
    from app.services.core_quantity import core_entry_price_ceiling

    read_capacity = getattr(adapter, 'get_domestic_cash_capacity', None)
    ceiling = core_entry_price_ceiling(item)
    account = configured_kis_account_reference(settings)
    if not callable(read_capacity) or iscoroutinefunction(read_capacity) or ceiling is None or account is None:
        return 'rule_cash_capacity_unavailable'
    if (command.exchange != 'KRX' or local_unknown_count
        or not local_unacknowledged_notional.is_finite() or local_unacknowledged_notional < 0):
        return 'rule_cash_capacity_scope_or_reservations_unproven'
    try:
        capacity = read_capacity(access_token=access_token, symbol=command.symbol, limit_price=ceiling, exchange='KRX')
        now = utc_now()
        received_at = capacity.received_at
        if (
            capacity.provider != 'kis' or capacity.runtime_mode != settings.runtime_mode
            or not hmac.compare_digest(capacity.account_reference.encode('utf-8'), account.encode('utf-8'))
            or capacity.symbol != command.symbol or capacity.exchange != 'KRX' or capacity.currency != 'KRW'
            or capacity.requested_price != ceiling or capacity.source_order_type != '00'
            or capacity.cma_included is not False or capacity.overseas_included is not False
            or re.fullmatch(r'[0-9a-f]{64}', capacity.capacity_id) is None
            or received_at.tzinfo is None or received_at.utcoffset() is None
            or not -timedelta(seconds=5) <= now - received_at <= timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
        ):
            return 'rule_cash_capacity_scope_or_freshness_mismatch'
        cash = Decimal(str(capacity.orderable_cash_krw))
        no_misu = Decimal(str(capacity.no_misu_buying_amount_krw))
        calculation_price = Decimal(str(capacity.quantity_calculation_price))
        core_cost_price = Decimal(str(item.quantity_policy['sizing_inputs']['cash_per_share']))
        if (
            not all(value.is_finite() for value in (cash, no_misu, calculation_price, core_cost_price))
            or min(cash, no_misu) < 0 or calculation_price <= 0
            or core_cost_price < ceiling
            or type(capacity.no_misu_buying_quantity) is not int or capacity.no_misu_buying_quantity < 0
            or type(command.quantity) is not int or command.quantity <= 0
        ):
            return 'rule_cash_capacity_invalid'
        unit_cost = max(core_cost_price, ceiling * (Decimal(1) + settings.kis_krx_round_trip_non_spread_cost_bps / Decimal(10000)))
        spendable = max(Decimal(0), min(cash, no_misu) - local_unacknowledged_notional)
        cash_quantity = int(spendable // unit_cost)
        if command.quantity > min(cash_quantity, capacity.no_misu_buying_quantity):
            return 'rule_cash_only_capacity_exceeded'
    except (AttributeError, KeyError, TypeError, ValueError, InvalidOperation, ArithmeticError):
        return 'rule_cash_capacity_invalid'
    except Exception:
        return 'rule_cash_capacity_unavailable'
    return None


def rule_entry_block_reason(
    *, settings: Settings, db: Session, plan_item: ExecutionPlanItem | None,
    now: datetime, lock: bool = False,
) -> str | None:
    if plan_item is None or plan_item.side != 'buy':
        return None
    risk = plan_item.risk_context if isinstance(plan_item.risk_context, dict) else {}
    raw = plan_item.plan.raw_payload if plan_item.plan is not None else None
    if 'rule_origin' not in risk and not (isinstance(raw, dict) and raw.get('origin_type') == 'rule'):
        return None
    if not settings.krx_us_rebound_enabled:
        return 'rule_strategy_disabled'
    origin = rule_origin_for_plan_item(plan_item)
    deadline = rule_close_exit_at(plan_item)
    if origin is None or deadline is None:
        return 'rule_origin_or_deadline_unproven'
    from app.services.core_handoff_v4 import persisted_core_plan_contract_ack

    plan = plan_item.plan
    proof = (
        persisted_core_plan_contract_ack(plan)
        if plan is not None and plan.source == 'kai-trading-core'
        and isinstance(raw, dict) and raw.get('origin_type') == 'rule'
        else None
    )
    if (
        proof is None or proof.get('origin_type') != 'rule' or proof.get('rule_origin') != origin
        or not any(ack['execution_plan_item_id'] == plan_item.id
                   and ack['core_item_key'] == plan_item.source_plan_item_id
                   for ack in proof['item_acks'])
    ):
        return 'rule_v4_contract_unproven'
    if now.tzinfo is None or now.utcoffset() is None or now >= deadline:
        return 'rule_entry_deadline_passed'
    scope = plan_item_inventory_scope_key(plan_item)
    if scope is None:
        return 'rule_account_scope_unproven'
    query = select(ExecutionPlanItem).join(ExecutionPlanItem.plan).where(
        ExecutionPlan.mode == settings.runtime_mode, ExecutionPlanItem.symbol == plan_item.symbol,
        ExecutionPlanItem.side == 'buy', ExecutionPlanItem.id != plan_item.id,
    ).options(selectinload(ExecutionPlanItem.plan), selectinload(ExecutionPlanItem.broker_orders))
    if lock:
        query = query.with_for_update()
    terminal = {
        'revoked', 'superseded', 'rejected', 'canceled', 'cancelled', 'expired',
        'not_applicable', 'next_open_buy_expired', 'watch_expired', 'watch_cancelled',
        'exec_agent_rejected', 'agent_rejected', 'contract_quarantined',
        'manual_review_resolved_no_exposure', 'core_quantity_satisfied', 'filled',
    }
    for other in db.scalars(query).all():
        other_scope = plan_item_inventory_scope_key(other)
        if other_scope is not None and other_scope != scope:
            continue
        lane = plan_item_strategy_lane(other)
        other_origin = rule_origin_for_plan_item(other)
        has_exposure = any(not broker_order_is_provably_unsubmitted(order) for order in other.broker_orders)
        if lane != 'krx_us_rebound':
            if other.status not in terminal or other.status == 'manual_review_required':
                return 'rule_baseline_entry_priority'
            # Broker/ledger gates below prove whether retired baseline orders
            # still own current inventory; historical flat families may coexist.
            if other_scope is None and has_exposure:
                return 'rule_baseline_inventory_ambiguous'
        elif other_origin is None:
            return 'rule_origin_or_deadline_unproven'
        elif other_origin['target_trade_date'] == origin['target_trade_date'] and (
            has_exposure or other.status not in terminal
        ):
            return 'rule_session_reentry_forbidden'
    return None
