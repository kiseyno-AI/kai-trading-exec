from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import FifoInventoryIsolation


@dataclass
class FifoIsolationCursor:
    """Apply durable broker-flat boundaries while replaying local fills."""

    isolated_at_by_symbol: dict[str, list[datetime]]
    _applied_count_by_symbol: dict[str, int] = field(default_factory=dict)

    def apply_before(
        self,
        *,
        symbol: str,
        observed_at: datetime,
        lots: list[object],
    ) -> None:
        boundaries = self.isolated_at_by_symbol.get(symbol, [])
        index = self._applied_count_by_symbol.get(symbol, 0)
        while index < len(boundaries) and boundaries[index] <= _as_utc(observed_at):
            lots.clear()
            index += 1
        self._applied_count_by_symbol[symbol] = index

    def apply_through(
        self,
        *,
        symbol: str,
        observed_at: datetime,
        lots: list[object],
    ) -> None:
        self.apply_before(symbol=symbol, observed_at=observed_at, lots=lots)


def load_fifo_isolation_cursor(
    *,
    db: Session,
    runtime_mode: str,
    through: datetime | None = None,
) -> FifoIsolationCursor:
    where = [FifoInventoryIsolation.mode == runtime_mode]
    if through is not None:
        where.append(FifoInventoryIsolation.isolated_through_at <= through)
    rows = db.execute(
        select(
            FifoInventoryIsolation.symbol,
            FifoInventoryIsolation.isolated_through_at,
        )
        .where(*where)
        .order_by(
            FifoInventoryIsolation.symbol.asc(),
            FifoInventoryIsolation.isolated_through_at.asc(),
            FifoInventoryIsolation.id.asc(),
        )
    ).all()
    boundaries: dict[str, list[datetime]] = {}
    for symbol, isolated_through_at in rows:
        boundaries.setdefault(symbol, []).append(_as_utc(isolated_through_at))
    return FifoIsolationCursor(isolated_at_by_symbol=boundaries)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
