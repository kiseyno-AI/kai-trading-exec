from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
)
from app.services.execution_account_route import (
    execution_account_scope_binding_from_payloads,
    validate_kis_account_route,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
)


SAFE_LOCAL_PRE_SUBMISSION_ORDER_STATUSES = frozenset({"rejected", "failed"})


def broker_order_is_provably_unsubmitted(order: BrokerOrder) -> bool:
    return bool(
        str(order.status or "").strip().lower()
        in SAFE_LOCAL_PRE_SUBMISSION_ORDER_STATUSES
        and order.broker_order_id is None
        and order.submitted_at is None
    )


def item_exposure_scope_is_proven(
    *,
    item: ExecutionPlanItem,
    settings: Settings | None,
) -> bool:
    provider = str(item.provider or "").strip().lower()
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = (
        item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    )
    exchange = str(risk_context.get("exchange") or item.venue or "").strip().upper()
    return bool(
        _expected_item_position_account_scope(
            provider=provider,
            exchange=exchange,
            risk_context=risk_context,
            quantity_policy=quantity_policy,
            settings=settings,
        )
    )


def item_has_any_broker_exposure(
    *,
    db: Session,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    settings: Settings | None,
) -> bool:
    if (
        db.scalar(
            select(BrokerOrder.id)
            .where(BrokerOrder.execution_plan_item_id == item.id)
            .limit(1)
        )
        is not None
    ):
        return True
    return item_has_external_possible_exposure(
        db=db,
        plan=plan,
        item=item,
        settings=settings,
    )


def item_has_external_possible_exposure(
    *,
    db: Session,
    plan: ExecutionPlan,
    item: ExecutionPlanItem,
    settings: Settings | None,
) -> bool:
    """Detect broker truth that may not yet be linked to this plan item."""
    provider = str(item.provider or "").strip().lower()
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    quantity_policy = (
        item.quantity_policy if isinstance(item.quantity_policy, dict) else {}
    )
    exchange = str(risk_context.get("exchange") or item.venue or "").strip().upper()

    unlinked_order_filters = [
        BrokerOrder.mode == plan.mode,
        BrokerOrder.symbol == item.symbol,
        or_(
            BrokerOrder.execution_plan_item_id.is_(None),
            BrokerOrder.execution_plan_item_id != item.id,
        ),
    ]
    candidate_orders = db.scalars(
        select(BrokerOrder)
        .where(*unlinked_order_filters)
        .with_for_update()
    ).all()
    expected_venues = {
        value
        for value in (
            exchange,
            str(item.venue or "").strip().upper(),
        )
        if value
    }
    unlinked_orders = [
        order
        for order in candidate_orders
        if _unknown_or_matches(
            actual=order.broker_name,
            expected={provider} if provider else set(),
        )
        and _unknown_or_matches(
            actual=order.venue,
            expected=expected_venues,
        )
    ]
    if unlinked_orders:
        if any(
            not broker_order_is_provably_unsubmitted(order)
            for order in unlinked_orders
        ):
            return True
        if db.scalar(
            select(ExecutionFill.id)
            .where(
                ExecutionFill.broker_order_pk.in_(
                    {order.id for order in unlinked_orders}
                )
            )
            .with_for_update()
            .limit(1)
        ) is not None:
            return True

    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == plan.mode,
            ExecutionPosition.symbol == item.symbol,
            or_(
                ExecutionPosition.quantity != 0,
                ExecutionPosition.base_quantity != 0,
            ),
        )
    ).all()
    if not positions:
        return False
    expected_account_scope = _expected_item_position_account_scope(
        provider=provider,
        exchange=exchange,
        risk_context=risk_context,
        quantity_policy=quantity_policy,
        settings=settings,
    )
    for position in positions:
        position_provider = str(position.provider or "").strip().lower()
        position_exchange = str(position.exchange or "").strip().upper()
        position_account_scope = str(position.account_scope or "").strip()
        if provider and position_provider and position_provider != provider:
            continue
        if exchange and position_exchange and position_exchange != exchange:
            continue
        if (
            not position_provider
            or not position_exchange
            or not position_account_scope
            or expected_account_scope is None
        ):
            return True
        if position_account_scope == expected_account_scope:
            return True
    return False


def _unknown_or_matches(*, actual: object, expected: set[str]) -> bool:
    normalized = str(actual or "").strip().lower()
    return not normalized or not expected or normalized in {
        value.strip().lower() for value in expected
    }


def _expected_item_position_account_scope(
    *,
    provider: str,
    exchange: str,
    risk_context: dict[str, object],
    quantity_policy: dict[str, object],
    settings: Settings | None,
) -> str | None:
    if not provider or not exchange:
        return None
    if provider == "kis":
        if settings is None or validate_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context=risk_context,
            quantity_policy=quantity_policy,
            require_binding=True,
        ) is not None:
            return None
        try:
            return build_position_identity(
                settings=settings,
                provider=provider,
                exchange=exchange,
            ).account_scope
        except PositionIdentityConflictError:
            return None
    binding = execution_account_scope_binding_from_payloads(
        risk_context,
        quantity_policy,
    )
    if (
        not isinstance(binding, dict)
        or binding.get("provider") != provider
        or not isinstance(binding.get("account_scope"), str)
    ):
        return None
    return str(binding["account_scope"])
