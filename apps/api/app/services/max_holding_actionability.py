from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition


ACTIVE_MAX_HOLDING_STATUSES = (
    "max_holding_close_sell_pending_config",
    "max_holding_close_sell_monitoring",
)


def max_holding_actionability_summary(
    *,
    db: Session,
    mode: str,
    reference_at: datetime | None = None,
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
    positions = {
        position.symbol: position
        for position in db.scalars(
            select(ExecutionPosition).where(ExecutionPosition.mode == mode)
        ).all()
        if position.quantity > 0
    }
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

    blocked = [item for item, _ in due if item.symbol not in positions]
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
