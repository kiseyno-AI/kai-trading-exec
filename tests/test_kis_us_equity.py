from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.api.schemas.execution import ExecutionPlanRequest, PlaceOrderRequest
from app.broker.base import BrokerOrderPlacementRequest
from app.broker.kis import (
    KIS_OVERSEAS_BALANCE_PATH,
    KIS_OVERSEAS_BUYING_POWER_PATH,
    KIS_OVERSEAS_CCLD_PATH,
    KIS_OVERSEAS_CURRENT_PRICE_PATH,
    KIS_OVERSEAS_OPEN_ORDERS_PATH,
    KIS_OVERSEAS_ORDER_PATH,
    JsonHttpResponse,
    KisBrokerAdapter,
)
from app.core.config import Settings
from app.services.broker_orders import _validate_kis_us_equity_guardrails
from app.services.execution_api import _build_execution_plan
from app.services.execution_jobs import _build_place_order_request, materialize_execution_jobs
from app.services.monitoring import materialize_monitoring_plan_items


class QueueHttpClient:
    def __init__(
        self,
        *,
        get_payloads: list[dict[str, object]] | None = None,
        post_payload: dict[str, object] | None = None,
    ) -> None:
        self.get_payloads = list(get_payloads or [])
        self.post_payload = post_payload or {}
        self.get_calls: list[dict[str, object]] = []
        self.post_calls: list[dict[str, object]] = []

    def get_json(self, **kwargs: object) -> JsonHttpResponse:
        self.get_calls.append(dict(kwargs))
        return JsonHttpResponse(status_code=200, headers={}, payload=self.get_payloads.pop(0))

    def post_json(self, **kwargs: object) -> JsonHttpResponse:
        self.post_calls.append(dict(kwargs))
        return JsonHttpResponse(status_code=200, headers={}, payload=self.post_payload)


def _settings(**overrides: object) -> Settings:
    return Settings(
        runtime_mode="paper",
        kis_app_key="app-key",
        kis_app_secret="app-secret",
        kis_account_number="12345678",
        kis_use_hashkey=False,
        **overrides,
    )


def _placement_request(*, side: str = "buy") -> BrokerOrderPlacementRequest:
    return BrokerOrderPlacementRequest(
        idempotency_key=f"us-{side}-1",
        symbol="AAPL",
        side=side,
        quantity=2,
        order_type="limit",
        time_in_force="day",
        exchange="NASD",
        limit_price=Decimal("201.25"),
        provider="kis",
        venue="kis",
        asset_class="us_equity",
    )


@pytest.mark.parametrize(
    ("side", "tr_id"),
    [("buy", "VTTT1002U"), ("sell", "VTTT1001U")],
)
def test_kis_us_equity_order_uses_official_paper_contract(side: str, tr_id: str) -> None:
    http = QueueHttpClient(
        get_payloads=(
            [{"rt_cd": "0", "output": {"max_ord_psbl_qty": "10", "ord_psbl_frcr_amt": "5000"}}]
            if side == "buy"
            else []
        ),
        post_payload={
            "rt_cd": "0",
            "msg_cd": "APBK0013",
            "msg1": "주문 전송 완료",
            "output": {"ODNO": "1234567890", "ORD_TMD": "093001"},
        }
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    placement = adapter.place_order("token", _placement_request(side=side))

    call = http.post_calls[0]
    assert call["url"].endswith(KIS_OVERSEAS_ORDER_PATH)
    assert call["headers"]["tr_id"] == tr_id
    assert call["payload"] == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "OVRS_EXCG_CD": "NASD",
        "PDNO": "AAPL",
        "ORD_QTY": "2",
        "OVRS_ORD_UNPR": "201.25",
        "CTAC_TLNO": "",
        "MGCO_APTM_ODNO": "",
        "SLL_TYPE": "00" if side == "sell" else "",
        "ORD_SVR_DVSN_CD": "0",
        "ORD_DVSN": "00",
    }
    assert placement.broker_order_id == "1234567890"


def test_kis_us_equity_quote_maps_order_exchange_to_quote_exchange() -> None:
    http = QueueHttpClient(get_payloads=[{"rt_cd": "0", "output": {"last": "201.35"}}])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    quote = adapter.get_current_price("token", symbol="AAPL", exchange="NASD")

    assert quote is not None and quote.price == Decimal("201.35")
    assert http.get_calls[0]["url"].endswith(KIS_OVERSEAS_CURRENT_PRICE_PATH)
    assert http.get_calls[0]["params"] == {"AUTH": "", "EXCD": "NAS", "SYMB": "AAPL"}


def test_kis_us_equity_account_snapshot_combines_balance_orders_and_buying_power() -> None:
    http = QueueHttpClient(
        get_payloads=[
            {
                "rt_cd": "0",
                "output1": [
                    {
                        "ovrs_pdno": "AAPL",
                        "ovrs_cblc_qty": "2",
                        "ord_psbl_qty": "2",
                        "pchs_avg_pric": "190.00",
                        "frcr_evlu_pfls_amt": "22.70",
                        "evlu_pfls_rt": "5.97",
                        "ovrs_stck_evlu_amt": "402.70",
                    }
                ],
                "output2": {},
            },
            {
                "rt_cd": "0",
                "output": [
                    {
                        "pdno": "MSFT",
                        "odno": "77",
                        "sll_buy_dvsn_cd": "02",
                        "ft_ord_qty": "3",
                        "ft_ccld_qty": "1",
                        "nccs_qty": "2",
                        "ft_ord_unpr3": "410.00",
                    }
                ],
            },
            {
                "rt_cd": "0",
                "output": {
                    "ord_psbl_frcr_amt": "5000.50",
                    "ovrs_ord_psbl_amt": "5000.50",
                    "max_ord_psbl_qty": "24",
                },
            },
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot(
        "token",
        symbols=["AAPL"],
        reference_prices={"AAPL": Decimal("201.25")},
        exchange="NASD",
    )

    assert [call["url"].rsplit("/", 1)[-1] for call in http.get_calls] == [
        KIS_OVERSEAS_BALANCE_PATH.rsplit("/", 1)[-1],
        KIS_OVERSEAS_OPEN_ORDERS_PATH.rsplit("/", 1)[-1],
        KIS_OVERSEAS_BUYING_POWER_PATH.rsplit("/", 1)[-1],
    ]
    assert snapshot.available_cash == Decimal("5000.50")
    assert snapshot.total_asset_value == Decimal("5403.20")
    assert snapshot.holdings[0].symbol == "AAPL"
    assert snapshot.open_orders[0].remaining_quantity == 2
    assert snapshot.symbol_buying_power["AAPL"].buyable_quantity == 24


def test_kis_us_equity_reconciliation_normalizes_cumulative_fill() -> None:
    http = QueueHttpClient(
        get_payloads=[
            {
                "rt_cd": "0",
                "output": [
                    {
                        "odno": "1234567890",
                        "pdno": "AAPL",
                        "ft_ord_qty": "3",
                        "ft_ccld_qty": "1",
                        "nccs_qty": "2",
                        "ft_ccld_unpr3": "200.50",
                        "prcs_stat_name": "부분체결",
                        "ord_dt": "20260819",
                        "ord_tmd": "101500",
                    }
                ],
            }
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "1234567890",
        exchange="NASD",
        submitted_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
    )

    assert result is not None
    assert http.get_calls[0]["url"].endswith(KIS_OVERSEAS_CCLD_PATH)
    assert http.get_calls[0]["headers"]["tr_id"] == "VTTS3035R"
    assert result["status"] == "partially_filled"
    assert result["filled_quantity"] == 1
    assert result["remaining_quantity"] == 2
    assert result["average_fill_price"] == Decimal("200.50")


def test_kis_us_equity_schema_requires_whole_share_limit_order() -> None:
    accepted = PlaceOrderRequest(
        idempotency_key="us-plan-1",
        execution_plan_item_id="plan-item-1",
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        symbol="AAPL",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price=Decimal("100"),
        exchange="NASD",
    )
    assert accepted.quantity == 1
    with pytest.raises(ValidationError, match="limit orders only"):
        PlaceOrderRequest(
            idempotency_key="us-plan-2",
            provider="kis",
            asset_class="us_equity",
            venue="kis",
            symbol="AAPL",
            side="buy",
            quantity=1,
            order_type="market",
            exchange="NASD",
        )


def test_kis_us_equity_guard_is_disabled_by_default_and_caps_usd_notional() -> None:
    command = PlaceOrderRequest(
        idempotency_key="us-plan-1",
        execution_plan_item_id="plan-item-1",
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        symbol="AAPL",
        side="buy",
        quantity=2,
        order_type="limit",
        limit_price=Decimal("60"),
        exchange="NASD",
    )
    adapter = SimpleNamespace(provider="kis")
    plan_item = SimpleNamespace(provider="kis", asset_class="us_equity", venue="kis")
    assert "disabled" in (_validate_kis_us_equity_guardrails(
        settings=_settings(), adapter=adapter, command=command, plan_item=plan_item
    ) or "")
    assert "maximum USD notional" in (_validate_kis_us_equity_guardrails(
        settings=_settings(kis_us_trading_enabled=True, kis_us_max_order_notional_usd=Decimal("100")),
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    ) or "")


def test_us_execution_handoff_preserves_route_and_schedules_new_york_open() -> None:
    plan = _build_execution_plan(
        settings=_settings(kis_us_trading_enabled=True),
        command=ExecutionPlanRequest(
            plan_id="core-us-20260819",
            source="kai-trading-core",
            market_date=date(2026, 8, 19),
            items=[
                {
                    "symbol": "AAPL",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "provider": "kis",
                    "venue": "kis",
                    "asset_class": "us_equity",
                    "market": "US",
                    "exchange": "NASD",
                    "currency": "USD",
                    "session_policy": {
                        "session": "regular",
                        "timezone": "America/New_York",
                        "allow_extended_hours": False,
                    },
                    "limit_price": "201.25",
                    "time_in_force": "day",
                }
            ],
        ),
    )
    materialize_execution_jobs(plan)

    item = plan.items[0]
    assert item.risk_context["exchange"] == "NASD"
    assert item.risk_context["currency"] == "USD"
    assert item.jobs[0].scheduled_for == datetime(2026, 8, 19, 13, 30, tzinfo=UTC)
    request = _build_place_order_request(
        settings=_settings(kis_us_trading_enabled=True),
        db=None,  # not used for a new-order job
        adapter=SimpleNamespace(provider="kis"),
        job=item.jobs[0],
    )
    assert request.provider == "kis"
    assert request.asset_class == "us_equity"
    assert request.venue == "kis"
    assert request.exchange == "NASD"


def test_us_execution_plan_materializes_target_and_stop_monitoring() -> None:
    entry_key = "us:2026-08-19:AAPL:next_open_buy"
    common = {
        "symbol": "AAPL",
        "provider": "kis",
        "venue": "kis",
        "asset_class": "us_equity",
        "market": "US",
        "exchange": "NASD",
        "currency": "USD",
        "broker": "kis",
        "session_policy": {"session": "regular", "timezone": "America/New_York"},
        "time_in_force": "day",
    }
    plan = _build_execution_plan(
        settings=_settings(kis_us_trading_enabled=True),
        command=ExecutionPlanRequest(
            plan_id="core-us-monitors-20260819",
            source="kai-trading-core",
            market_date=date(2026, 8, 20),
            items=[
                {
                    **common,
                    "core_item_key": entry_key,
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "limit_price": "50.25",
                },
                {
                    **common,
                    "core_item_key": "us:2026-08-19:AAPL:target_sell",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": entry_key,
                    },
                    "trigger_price": "60.00",
                },
                {
                    **common,
                    "core_item_key": "us:2026-08-19:AAPL:stop_sell",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": entry_key,
                    },
                    "trigger_price": "45.00",
                },
            ],
        ),
    )

    materialize_execution_jobs(plan)
    materialize_monitoring_plan_items(plan)

    items = {item.intent: item for item in plan.items}
    assert items["next_open_buy"].status == "next_open_buy_scheduled"
    assert items["target_sell"].status == "target_sell_pending_config"
    assert items["stop_sell"].status == "stop_sell_pending_config"
    assert items["target_sell"].risk_context["exchange"] == "NASD"
