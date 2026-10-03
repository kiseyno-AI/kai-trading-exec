from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.execution_account_route import (
    classify_kis_account_route,
    execution_account_scope_binding_from_payloads,
)
from app.services.position_identity import (
    PositionIdentity,
    PositionIdentityConflictError,
    build_position_identity,
    position_identity_from_columns,
)
from app.services.runtime_transitions import (
    ACTIVE_SELL_MONITORING_STATUSES,
    SELL_AGENT_ACTIVE_STATUSES,
)


ACTIVE_MAX_HOLDING_STATUSES = tuple(
    sorted(
        {"max_holding_close_sell_pending_config"}
        | SELL_AGENT_ACTIVE_STATUSES
        | {
            status
            for status in ACTIVE_SELL_MONITORING_STATUSES
            if status.startswith("max_holding_close_sell_")
        }
    )
)


def max_holding_actionability_summary(
    *,
    settings: Settings,
    db: Session,
    mode: str,
    reference_at: datetime | None = None,
    provider: str | None = None,
    account_scope: str | None = None,
    exchanges: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Return a read-only summary of due max-holding exits and their blockers."""
    reference_at = (reference_at or datetime.now(UTC)).astimezone(UTC)
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItem.intent == "max_holding_close_sell",
            ExecutionPlanItem.status.in_(ACTIVE_MAX_HOLDING_STATUSES),
        )
    ).all()
    items = [
        item
        for item in items
        if str(
            item.provider
            or ("kis" if item.symbol.isdigit() and len(item.symbol) == 6 else "")
        ).strip().lower()
        != "kis"
        or _expected_position_identity(settings=settings, item=item) is not None
    ]
    if provider is not None:
        allowed_exchanges = set(exchanges or ())
        items = [
            item
            for item in items
            if (
                (identity := _expected_position_identity(settings=settings, item=item))
                is not None
                and identity.provider == provider
                and account_scope is not None
                and identity.account_scope == account_scope
                and identity.exchange in allowed_exchanges
            )
        ]
    due: list[tuple[ExecutionPlanItem, str]] = []
    for item in items:
        context = item.risk_context if isinstance(item.risk_context, dict) else {}
        close_date = context.get("close_exit_date")
        if not isinstance(close_date, str):
            continue
        try:
            is_due = datetime.strptime(close_date, "%Y-%m-%d").date() <= reference_at.date()
        except ValueError:
            continue
        if is_due:
            due.append((item, close_date))

    identities_by_item_id = {
        item.id: _expected_position_identity(settings=settings, item=item)
        for item, _ in due
    }
    expected_identities = {
        identity
        for identity in identities_by_item_id.values()
        if identity is not None
    }
    scoped_positions = (
        list(
            db.scalars(
                select(ExecutionPosition).where(
                    ExecutionPosition.mode == mode,
                    ExecutionPosition.quantity > 0,
                    or_(
                        *(
                            and_(
                                ExecutionPosition.provider == identity.provider,
                                ExecutionPosition.account_scope
                                == identity.account_scope,
                                ExecutionPosition.exchange == identity.exchange,
                            )
                            for identity in expected_identities
                        )
                    ),
                )
            ).all()
        )
        if expected_identities
        else []
    )

    blocked = [
        item
        for item, _ in due
        if not _has_one_exact_route_position(
            item=item,
            expected_identity=identities_by_item_id[item.id],
            positions=scoped_positions,
        )
    ]
    pending = [item for item, _ in due if item.status.endswith("_pending_config")]
    symbols = sorted({item.symbol for item, _ in due})
    return {
        "reference_at": reference_at.isoformat(),
        "due_item_count": len(due),
        "due_symbol_count": len(symbols),
        "pending_config_count": len(pending),
        "missing_position_count": len(blocked),
        "oldest_due_date": min((close_date for _, close_date in due), default=None),
        "symbols": symbols,
        "broker_mutations": False,
    }


def _has_one_exact_route_position(
    *,
    item: ExecutionPlanItem,
    expected_identity: PositionIdentity | None,
    positions: list[ExecutionPosition],
) -> bool:
    if expected_identity is None:
        return False
    matches: list[ExecutionPosition] = []
    for position in positions:
        if position.symbol != item.symbol:
            continue
        try:
            actual_identity = position_identity_from_columns(position)
        except PositionIdentityConflictError:
            continue
        if actual_identity == expected_identity:
            matches.append(position)
    return len(matches) == 1


def _expected_position_identity(
    *,
    settings: Settings,
    item: ExecutionPlanItem,
) -> PositionIdentity | None:
    provider = str(
        item.provider
        or ("kis" if item.symbol.isdigit() and len(item.symbol) == 6 else "")
    ).strip().lower()
    risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
    exchange = str(risk_context.get("exchange") or "").strip().upper()
    if not exchange and provider == "kis" and item.symbol.isdigit() and len(item.symbol) == 6:
        exchange = "KRX"
    if not provider or not exchange:
        return None
    if provider != "kis":
        binding = execution_account_scope_binding_from_payloads(
            item.risk_context,
            item.quantity_policy,
        )
        if binding is None or binding.get("provider") != provider:
            return None
        account_scope = binding.get("account_scope")
        if not isinstance(account_scope, str):
            return None
        return PositionIdentity(
            provider=provider,
            account_scope=account_scope,
            exchange=exchange,
        )
    if classify_kis_account_route(
        settings=settings,
        provider=provider,
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
    ) != "active":
        return None
    try:
        return build_position_identity(
            settings=settings,
            provider=provider,
            exchange=exchange,
        )
    except PositionIdentityConflictError:
        return None
