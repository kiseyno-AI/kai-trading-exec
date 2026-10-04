from datetime import date

import pytest

from app.services.trading_sessions import (
    add_weekday_sessions,
    count_krx_holding_sessions,
    resolve_krx_session_deadline,
)


def test_weekday_session_advance_excludes_entry_and_skips_weekend() -> None:
    assert add_weekday_sessions(date(2026, 6, 12), 3) == date(2026, 6, 17)


def test_krx_session_deadline_skips_authoritative_holiday() -> None:
    calendar = {
        date(2026, 10, 5): False,
        date(2026, 10, 6): True,
        date(2026, 10, 7): True,
    }

    result = resolve_krx_session_deadline(
        entry_date=date(2026, 10, 2),
        sessions=2,
        is_trading_day=calendar.get,
    )

    assert result.close_date == date(2026, 10, 7)
    assert result.status == "authoritative"
    assert result.unknown_dates == ()


@pytest.mark.parametrize("failure", [None, RuntimeError("calendar unavailable")])
def test_krx_session_deadline_falls_back_early_when_calendar_is_unknown(
    failure: Exception | None,
) -> None:
    def unknown_calendar(_market_date: date) -> bool | None:
        if failure is not None:
            raise failure
        return None

    result = resolve_krx_session_deadline(
        entry_date=date(2026, 10, 2),
        sessions=2,
        is_trading_day=unknown_calendar,
    )

    assert result.close_date == date(2026, 10, 6)
    assert result.status == "weekday_provisional"
    assert result.unknown_dates == (date(2026, 10, 5),)


def test_krx_holding_age_counts_open_sessions_not_calendar_days() -> None:
    calendar = {
        date(2026, 10, 5): False,
        date(2026, 10, 6): True,
    }

    holiday_age = count_krx_holding_sessions(
        entry_date=date(2026, 10, 2),
        market_date=date(2026, 10, 5),
        is_trading_day=calendar.get,
    )
    next_session_age = count_krx_holding_sessions(
        entry_date=date(2026, 10, 2),
        market_date=date(2026, 10, 6),
        is_trading_day=calendar.get,
    )

    assert holiday_age.sessions == 1
    assert holiday_age.status == "authoritative"
    assert next_session_age.sessions == 2
    assert next_session_age.status == "authoritative"


def test_krx_holding_age_stops_calendar_calls_at_relevant_threshold() -> None:
    calls: list[date] = []

    def open_day(market_date: date) -> bool:
        calls.append(market_date)
        return True

    age = count_krx_holding_sessions(
        entry_date=date(2020, 1, 2),
        market_date=date(2026, 10, 2),
        is_trading_day=open_day,
        max_sessions=2,
    )

    assert age.sessions == 2
    assert age.capped_at == 2
    assert calls == [date(2020, 1, 3)]
