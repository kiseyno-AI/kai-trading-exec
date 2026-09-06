from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit


LATE_ENTRY_GATE_EVENT_TYPE = "watch_buy_late_entry_gate"


def late_entry_actionability_summary(
    *,
    db: Session,
    mode: str,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, object]:
    """Summarize persisted late-entry decisions without broker mutations."""
    rows = db.execute(
        select(
            ExecutionPlanItem.symbol,
            ExecutionPlanItemAudit.summary,
            ExecutionPlanItemAudit.occurred_at,
        )
        .select_from(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItemAudit.event_type == LATE_ENTRY_GATE_EVENT_TYPE,
            ExecutionPlanItemAudit.occurred_at >= start_at,
            ExecutionPlanItemAudit.occurred_at < end_at,
        )
        .order_by(ExecutionPlanItemAudit.occurred_at.asc())
    ).all()
    counts = {"shadow": 0, "block": 0}
    symbols: set[str] = set()
    for symbol, summary, _ in rows:
        gate_mode = summary.get("mode") if isinstance(summary, dict) else None
        if gate_mode in counts:
            counts[gate_mode] += 1
        symbols.add(str(symbol))
    return {
        "event_count": len(rows),
        "shadow_count": counts["shadow"],
        "block_count": counts["block"],
        "symbols": sorted(symbols),
        "first_observed_at": rows[0][2].isoformat() if rows else None,
        "last_observed_at": rows[-1][2].isoformat() if rows else None,
        "broker_mutations": False,
    }
