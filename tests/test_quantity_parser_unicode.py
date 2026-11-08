"""Quantity parsers accept representable decimal numerals and reject invalid input."""

import pytest

from app.services import exec_agent, execution_api, execution_jobs, monitoring, short_term_exec_agent
from app.services.watch_exec_agent_sizing import _open_order_remaining_quantity


def _remaining_quantity(value):
    return _open_order_remaining_quantity({"remaining_quantity": value})


@pytest.mark.parametrize("parse", [
    pytest.param(execution_api._parse_positive_int, id="plan-intake"),
    pytest.param(execution_jobs._parse_positive_int, id="job-dispatch"),
    pytest.param(exec_agent._parse_positive_int, id="exec-agent"),
    pytest.param(short_term_exec_agent._parse_positive_int, id="short-term-agent"),
    pytest.param(monitoring._parse_positive_int, id="monitoring"),
    pytest.param(_remaining_quantity, id="open-order-sizing"),
])
@pytest.mark.parametrize(("value", "expected"), [
    ("²", None), ("①", None), ("2", 2), ("٢", 2), ("２", 2),
    pytest.param("9" * 5000, None, id="integer-conversion-limit"),
])
def test_quantity_parser_handles_decimal_numerals_and_invalid_input(parse, value, expected):
    result = parse(value)
    if expected is None:
        assert result is None
    else:
        assert result == expected


@pytest.mark.parametrize("value", ["2.0", "2e0", "+2", " 2 "])
def test_monitoring_quantity_parser_preserves_existing_integral_formats(value):
    assert monitoring._parse_positive_int(value) == 2
