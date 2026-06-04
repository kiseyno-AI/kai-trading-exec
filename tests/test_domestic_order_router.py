from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.services.domestic_order_router import resolve_domestic_order_route

KST = ZoneInfo("Asia/Seoul")


def test_nxt_eligible_symbol_routes_to_nxt_pre_when_enabled() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="033530",
            nxt_order_route_preference="nxt",
        ),
        now=datetime(2026, 5, 28, 8, 0, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("7300"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "NXT"
    assert decision.session == "nxt_pre"
    assert decision.ord_dvsn == "00"


def test_official_nxt_eligible_symbol_routes_when_env_list_is_empty(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.services.domestic_order_router.is_nxt_eligible_symbol",
        lambda *, settings, symbol: symbol == "005930",
    )

    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="",
            nxt_order_route_preference="nxt",
        ),
        now=datetime(2026, 5, 28, 8, 0, tzinfo=KST),
        symbol="005930",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("70000"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "NXT"
    assert decision.session == "nxt_pre"


def test_nxt_pre_market_order_without_limit_defers_protective_sell() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="033530",
        ),
        now=datetime(2026, 5, 28, 8, 0, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="market",
        limit_price=None,
        requested_exchange="KRX",
    )

    assert decision.action == "defer"
    assert decision.reason == "nxt_pre_session:limit_required"
    assert decision.ord_dvsn is None
    assert decision.next_retry_at is not None


def test_nxt_pre_market_order_without_limit_blocks_manual_buy() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="033530",
        ),
        now=datetime(2026, 5, 28, 8, 0, tzinfo=KST),
        symbol="033530",
        side="buy",
        purpose="manual_order",
        desired_order_type="market",
        limit_price=None,
        requested_exchange="KRX",
    )

    assert decision.action == "block"
    assert decision.reason == "nxt_pre_session:limit_required"
    assert decision.ord_dvsn is None
    assert decision.next_retry_at is not None


def test_non_nxt_symbol_defers_at_0800_without_regular_krx_submit() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="005930",
        ),
        now=datetime(2026, 5, 28, 8, 0, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("7300"),
        requested_exchange="KRX",
    )

    assert decision.action == "defer"
    assert decision.exchange == "KRX"
    assert decision.ord_dvsn is None
    assert decision.next_retry_at is not None


def test_krx_preopen_close_uses_order_division_05_when_enabled() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_krx_pre_open_offhours_orders=True,
        ),
        now=datetime(2026, 5, 28, 8, 35, tzinfo=KST),
        symbol="005930",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("70000"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "KRX"
    assert decision.session == "krx_preopen_close"
    assert decision.ord_dvsn == "05"


def test_krx_regular_keeps_regular_limit_order_division() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(domestic_order_session_routing_enabled=True),
        now=datetime(2026, 5, 28, 9, 0, tzinfo=KST),
        symbol="005930",
        side="buy",
        purpose="approved_buy",
        desired_order_type="limit",
        limit_price=Decimal("70000"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "KRX"
    assert decision.session == "krx_regular"
    assert decision.ord_dvsn == "00"


def test_nxt_main_can_be_preferred_during_overlap() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_main_orders=True,
            nxt_eligible_symbols="033530",
            nxt_order_route_preference="nxt",
        ),
        now=datetime(2026, 5, 28, 9, 1, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("7300"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "NXT"
    assert decision.session == "nxt_main"


def test_nxt_after_limit_only_acceptance_at_1530() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_after_orders=True,
            nxt_eligible_symbols="033530",
        ),
        now=datetime(2026, 5, 28, 15, 30, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("7300"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "NXT"
    assert decision.session == "nxt_after"
    assert decision.effective_order_type == "limit"


def test_krx_after_close_uses_order_division_06() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_krx_after_hours_closing_orders=True,
        ),
        now=datetime(2026, 5, 28, 15, 40, tzinfo=KST),
        symbol="005930",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("70000"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.session == "krx_after_close"
    assert decision.ord_dvsn == "06"


def test_krx_after_single_price_uses_order_division_07_for_non_nxt_symbol() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_krx_after_hours_single_price_orders=True,
            nxt_eligible_symbols="033530",
        ),
        now=datetime(2026, 5, 28, 16, 0, tzinfo=KST),
        symbol="005930",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("70000"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.session == "krx_after_single_price"
    assert decision.ord_dvsn == "07"


def test_nxt_after_routes_until_2000_for_eligible_symbol() -> None:
    decision = resolve_domestic_order_route(
        settings=Settings(
            domestic_order_session_routing_enabled=True,
            allow_nxt_after_orders=True,
            nxt_eligible_symbols="033530",
        ),
        now=datetime(2026, 5, 28, 19, 59, tzinfo=KST),
        symbol="033530",
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=Decimal("7300"),
        requested_exchange="KRX",
    )

    assert decision.action == "submit"
    assert decision.exchange == "NXT"
    assert decision.session == "nxt_after"
