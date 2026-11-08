"""Quantity parsers accept decimal numerals, not every Unicode digit symbol."""

import pytest

from app.services import exec_agent, execution_api, execution_jobs, short_term_exec_agent
from app.services.watch_exec_agent_sizing import _open_order_remaining_quantity


def _remaining_quantity(value):
    return _open_order_remaining_quantity({"remaining_quantity": value})


@pytest.mark.parametrize("parse", [
    pytest.param(execution_api._parse_positive_int, id="plan-intake"),
    pytest.param(execution_jobs._parse_positive_int, id="job-dispatch"),
    pytest.param(exec_agent._parse_positive_int, id="exec-agent"),
    pytest.param(short_term_exec_agent._parse_positive_int, id="short-term-agent"),
    pytest.param(_remaining_quantity, id="open-order-sizing"),
])
@pytest.mark.parametrize(("value", "expected"), [
    ("²", None), ("①", None), ("2", 2), ("٢", 2), ("２", 2),
])
def test_quantity_parser_distinguishes_unicode_decimal_numerals(parse, value, expected):
    assert parse(value) == expected
