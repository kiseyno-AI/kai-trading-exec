from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class TradingSessionDeadline:
    close_date: date
    status: str
    source: str
    unknown_dates: tuple[date, ...] = ()


@dataclass(frozen=True)
class TradingSessionAge:
    sessions: int
    status: str
    source: str
    unknown_dates: tuple[date, ...] = ()
    capped_at: int | None = None


def add_weekday_sessions(start: date | None, sessions: int) -> date | None:
    """Advance by weekdays, excluding the entry date.

    This is intentionally only a provisional schedule for exchanges whose
    holiday calendar has not yet been resolved.
    """

    if start is None:
        return None
    current = start
    added = 0
    while added < max(0, sessions):
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def count_weekday_holding_sessions(entry_date: date, market_date: date) -> int:
    sessions = 1
    current = entry_date
    while current < market_date:
        current += timedelta(days=1)
        if current.weekday() < 5:
            sessions += 1
    return sessions


def count_krx_holding_sessions(
    *,
    entry_date: date,
    market_date: date,
    is_trading_day: Callable[[date], bool | None],
    max_sessions: int | None = None,
) -> TradingSessionAge:
    """Count the filled entry session plus later authoritative KRX sessions."""

    provisional = count_weekday_holding_sessions(entry_date, market_date)
    sessions = 1
    current = entry_date
    while current < market_date:
        if max_sessions is not None and sessions >= max(1, max_sessions):
            return TradingSessionAge(
                sessions=sessions,
                status="authoritative",
                source="broker_adapter_is_krx_trading_day",
                capped_at=max(1, max_sessions),
            )
        current += timedelta(days=1)
        if current.weekday() >= 5:
            continue
        try:
            is_open = is_trading_day(current)
        except Exception:
            is_open = None
        if not isinstance(is_open, bool):
            return TradingSessionAge(
                sessions=provisional,
                status="weekday_provisional",
                source="weekday_fallback_calendar_unknown",
                unknown_dates=(current,),
            )
        if is_open:
            sessions += 1
    return TradingSessionAge(
        sessions=sessions,
        status="authoritative",
        source="broker_adapter_is_krx_trading_day",
        capped_at=(
            max(1, max_sessions)
            if max_sessions is not None and sessions >= max(1, max_sessions)
            else None
        ),
    )


def resolve_krx_session_deadline(
    *,
    entry_date: date,
    sessions: int,
    is_trading_day: Callable[[date], bool | None],
) -> TradingSessionDeadline:
    """Resolve an exclusive KRX-session deadline or fail safe to weekdays.

    An unknown weekday makes the exact exchange-session deadline unknowable.
    The weekday deadline is intentionally earlier than or equal to a deadline
    that excludes additional holidays, so risk is reduced while a later cycle
    retries the authoritative calendar.
    """

    provisional = add_weekday_sessions(entry_date, sessions)
    assert provisional is not None
    current = entry_date
    added = 0
    scan_days = 0
    while added < max(0, sessions):
        current += timedelta(days=1)
        scan_days += 1
        if current.weekday() >= 5:
            continue
        try:
            is_open = is_trading_day(current)
        except Exception:
            is_open = None
        if not isinstance(is_open, bool):
            return TradingSessionDeadline(
                close_date=provisional,
                status="weekday_provisional",
                source="weekday_fallback_calendar_unknown",
                unknown_dates=(current,),
            )
        if is_open:
            added += 1
        if scan_days >= 366 and added < sessions:
            return TradingSessionDeadline(
                close_date=provisional,
                status="weekday_provisional",
                source="weekday_fallback_calendar_scan_limit",
            )
    return TradingSessionDeadline(
        close_date=current,
        status="authoritative",
        source="broker_adapter_is_krx_trading_day",
    )


def configured_krx_market_holidays(raw_dates: str) -> set[date]:
    holidays: set[date] = set()
    for raw_date in str(raw_dates or "").replace(",", " ").split():
        try:
            holidays.add(date.fromisoformat(raw_date))
        except ValueError:
            continue
    return holidays
