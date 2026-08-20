from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.api.schemas.execution import ExecutionPlanRequest, PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerErrorClassification,
    BrokerOrderPlacementRequest,
)
from app.broker.kis import (
    KIS_OVERSEAS_BALANCE_PATH,
    KIS_OVERSEAS_BUYING_POWER_PATH,
    KIS_OVERSEAS_CCLD_PATH,
    KIS_OVERSEAS_CURRENT_PRICE_PATH,
    KIS_OVERSEAS_OPEN_ORDERS_PATH,
    KIS_OVERSEAS_ORDER_PATH,
    JsonHttpResponse,
    KisBrokerAdapter,
    _classify_error,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder
from app.services import broker_orders as broker_orders_module
from app.services.account_drawdown_gate import evaluate_us_equity_account_gate
from app.services.broker_orders import (
    _evaluate_new_buy_account_gate,
    _merge_us_account_gate_snapshots,
    _order_notional,
    _validate_kis_us_equity_guardrails,
    place_broker_order,
)
from app.services.execution_api import _build_execution_plan
from app.services.execution_jobs import (
    _build_place_order_request,
    _defer_us_next_open_job_outside_session,
    materialize_execution_jobs,
)
from app.services.monitoring import materialize_monitoring_plan_items
from app.services.runtime_controls import ControlCommand, resume_runtime


class QueueHttpClient:
    def __init__(
        self,
        *,
        get_payloads: list[dict[str, object] | JsonHttpResponse] | None = None,
        post_payload: dict[str, object] | None = None,
    ) -> None:
        self.get_payloads = list(get_payloads or [])
        self.post_payload = post_payload or {}
        self.get_calls: list[dict[str, object]] = []
        self.post_calls: list[dict[str, object]] = []

    def get_json(self, **kwargs: object) -> JsonHttpResponse:
        self.get_calls.append(dict(kwargs))
        queued = self.get_payloads.pop(0)
        if isinstance(queued, JsonHttpResponse):
            return queued
        return JsonHttpResponse(status_code=200, headers={}, payload=queued)

    def post_json(self, **kwargs: object) -> JsonHttpResponse:
        self.post_calls.append(dict(kwargs))
        return JsonHttpResponse(status_code=200, headers={}, payload=self.post_payload)


def test_kis_paper_key_mismatch_is_not_retryable_transient_error() -> None:
    assert (
        _classify_error(
            status_code=500,
            code="EGW02007",
            message="해당 앱키는 모의투자용 앱키가 아닙니다.",
        )
        == BrokerErrorClassification.VALIDATION
    )


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "runtime_mode": "paper",
        "kis_app_key": "app-key",
        "kis_app_secret": "app-secret",
        "kis_account_number": "12345678",
        "kis_use_hashkey": False,
        "kis_us_allow_degraded_realized_pnl_coverage": True,
    }
    values.update(overrides)
    return Settings(
        **values,
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
    assert http.get_calls[1]["headers"]["tr_id"] == "VTTS3018R"
    assert snapshot.total_asset_value == Decimal("5403.20")
    assert snapshot.holdings[0].symbol == "AAPL"
    assert snapshot.open_orders[0].remaining_quantity == 2
    assert snapshot.symbol_buying_power["AAPL"].buyable_quantity == 24


def test_kis_us_equity_live_open_orders_uses_live_transaction_id() -> None:
    http = QueueHttpClient(
        get_payloads=[
            {"rt_cd": "0", "output1": [], "output2": {}},
            {"rt_cd": "0", "output": []},
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(runtime_mode="live"), http_client=http)

    adapter.get_account_snapshot("token", exchange="NASD")

    assert http.get_calls[1]["headers"]["tr_id"] == "TTTS3018R"


def test_kis_us_equity_balance_follows_official_continuation_cursor() -> None:
    http = QueueHttpClient(
        get_payloads=[
            JsonHttpResponse(
                status_code=200,
                headers={"tr_cont": "F"},
                payload={
                    "rt_cd": "0",
                    "output1": [{"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "1"}],
                    "output2": {
                        "frcr_dncl_amt_2": "500",
                        "ctx_area_fk200": "cursor-fk",
                        "ctx_area_nk200": "cursor-nk",
                    },
                },
            ),
            JsonHttpResponse(
                status_code=200,
                headers={"tr_cont": ""},
                payload={
                    "rt_cd": "0",
                    "output1": [{"ovrs_pdno": "MSFT", "ovrs_cblc_qty": "2"}],
                    "output2": {},
                },
            ),
            {"rt_cd": "0", "output": []},
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="NASD")

    assert [holding.symbol for holding in snapshot.holdings] == ["AAPL", "MSFT"]
    assert http.get_calls[1]["params"]["CTX_AREA_FK200"] == "cursor-fk"
    assert http.get_calls[1]["params"]["CTX_AREA_NK200"] == "cursor-nk"
    assert http.get_calls[1]["headers"]["tr_cont"] == "N"


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
    assert _order_notional(command=command, plan_item=plan_item) == Decimal("120.0000")
    assert _validate_kis_us_equity_guardrails(
        settings=_settings(kis_us_trading_enabled=True, kis_us_max_order_notional_usd=Decimal("120")),
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    ) is None
    assert "maximum USD notional" in (_validate_kis_us_equity_guardrails(
        settings=_settings(kis_us_trading_enabled=True, kis_us_max_order_notional_usd=Decimal("100")),
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    ) or "")

    sell = command.model_copy(update={"side": "sell", "quantity": 10})
    assert _validate_kis_us_equity_guardrails(
        settings=_settings(kis_us_trading_enabled=True, kis_us_max_order_notional_usd=Decimal("100")),
        adapter=adapter,
        command=sell,
        plan_item=plan_item,
    ) is None


def test_kis_us_equity_account_gate_blocks_projected_usd_exposure() -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_max_total_exposure_usd=Decimal("500"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
        source="test",
        holdings=[
            BrokerAccountHolding(
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("200"),
                payload={"ovrs_stck_evlu_amt": "450"},
            )
        ],
    )

    decision = evaluate_us_equity_account_gate(
        settings=settings,
        db=db,
        snapshot=snapshot,
        order_notional=Decimal("100"),
        order_idempotency_key="us-exposure-test",
        execution_plan_item_id="item-1",
        now=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
    )

    assert decision.allowed is False
    assert decision.gate_name == "us_total_exposure_usd"
    assert decision.summary["currency"] == "USD"
    db.close()
    engine.dispose()


def test_kis_us_equity_account_gate_reserves_open_buy_notional() -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_max_total_exposure_usd=Decimal("1000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("2000"),
        observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
        source="test",
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="AAPL",
                side="buy",
                quantity=3,
                remaining_quantity=3,
                limit_price=Decimal("200"),
            )
        ],
    )

    decision = evaluate_us_equity_account_gate(
        settings=settings,
        db=db,
        snapshot=snapshot,
        order_notional=Decimal("600"),
        order_idempotency_key="us-open-buy-exposure-test",
        execution_plan_item_id="item-2",
        now=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
    )

    assert decision.allowed is False
    assert decision.gate_name == "us_total_exposure_usd"
    assert decision.payload["metrics"]["open_buy_notional_usd"] == "600.0000"
    assert decision.payload["metrics"]["projected_exposure_usd"] == "1200.0000"
    db.close()
    engine.dispose()


def test_kis_us_equity_account_gate_uses_whole_share_notional(monkeypatch) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_max_total_exposure_usd=Decimal("150"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
        source="test",
    )
    adapter = SimpleNamespace(
        provider="kis",
        get_account_snapshot=lambda *_args, **_kwargs: snapshot,
    )
    monkeypatch.setattr(
        broker_orders_module,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    command = PlaceOrderRequest(
        idempotency_key="us-account-gate-quantity",
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

    decision = _evaluate_new_buy_account_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=None,
    )

    assert decision is not None and decision.allowed is True
    assert decision.payload["metrics"]["projected_exposure_usd"] == "120.0000"
    db.close()
    engine.dispose()


def test_kis_us_equity_new_buy_gate_aggregates_all_supported_exchange_snapshots(
    monkeypatch,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_max_total_exposure_usd=Decimal("500"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    snapshots = {
        "NASD": BrokerAccountSnapshot(
            observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
            source="test:NASD",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=1,
                    average_price=Decimal("300"),
                    payload={"ovrs_stck_evlu_amt": "300"},
                )
            ],
        ),
        "NYSE": BrokerAccountSnapshot(
            observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
            source="test:NYSE",
            holdings=[
                BrokerAccountHolding(
                    symbol="IBM",
                    quantity=1,
                    average_price=Decimal("250"),
                    payload={"ovrs_stck_evlu_amt": "250"},
                )
            ],
        ),
        "AMEX": BrokerAccountSnapshot(
            observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
            source="test:AMEX",
        ),
    }
    calls: list[tuple[list[str] | None, str]] = []

    def get_account_snapshot(_token, *, symbols=None, exchange="KRX"):
        calls.append((symbols, exchange))
        return snapshots[exchange]

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    monkeypatch.setattr(
        broker_orders_module,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    command = PlaceOrderRequest(
        idempotency_key="us-account-gate-all-exchanges",
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        symbol="MSFT",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price=Decimal("100"),
        exchange="NASD",
    )

    decision = _evaluate_new_buy_account_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=None,
    )

    assert calls == [
        (["MSFT"], "AMEX"),
        (["MSFT"], "NASD"),
        (["MSFT"], "NYSE"),
    ]
    assert decision is not None and decision.allowed is False
    assert decision.gate_name == "us_total_exposure_usd"
    assert decision.payload["metrics"]["current_exposure_usd"] == "550.0000"
    assert decision.payload["metrics"]["projected_exposure_usd"] == "650.0000"
    db.close()
    engine.dispose()


def test_kis_us_equity_new_buy_gate_blocks_unconfirmed_broker_realized_pnl_coverage(
    monkeypatch,
) -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_allow_degraded_realized_pnl_coverage=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    snapshot = BrokerAccountSnapshot(
        observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
        source="test",
    )
    adapter = SimpleNamespace(
        provider="kis",
        get_account_snapshot=lambda *_args, **_kwargs: snapshot,
    )
    monkeypatch.setattr(
        broker_orders_module,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    command = PlaceOrderRequest(
        idempotency_key="us-account-gate-realized-coverage",
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

    decision = _evaluate_new_buy_account_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=None,
    )

    assert decision is not None and decision.allowed is False
    assert decision.gate_name == "us_realized_pnl_coverage_unconfirmed"
    assert decision.payload["metrics"]["realized_pnl_coverage"] == (
        "degraded_local_execution_fills_only"
    )
    db.close()
    engine.dispose()


def test_kis_us_equity_new_buy_gate_fails_closed_on_conflicting_duplicate_holding(
    monkeypatch,
) -> None:
    settings = _settings(database_url="sqlite:///:memory:")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()

    def get_account_snapshot(_token, *, symbols=None, exchange="KRX"):
        del symbols
        quantity = 1 if exchange == "NASD" else 2
        holdings = [] if exchange == "AMEX" else [
            BrokerAccountHolding(
                symbol="AAPL",
                quantity=quantity,
                average_price=Decimal("100"),
                payload={"ovrs_excg_cd": "NASD", "ovrs_stck_evlu_amt": str(100 * quantity)},
            )
        ]
        return BrokerAccountSnapshot(
            observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
            source=f"test:{exchange}",
            holdings=holdings,
            payload={"exchange": exchange},
        )

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    monkeypatch.setattr(
        broker_orders_module,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    command = PlaceOrderRequest(
        idempotency_key="us-account-gate-conflicting-holding",
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        symbol="MSFT",
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price=Decimal("100"),
        exchange="NASD",
    )

    decision = _evaluate_new_buy_account_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=command,
        plan_item=None,
    )

    assert decision is not None and decision.allowed is False
    assert decision.gate_name == "account_drawdown_snapshot_unavailable"
    assert "conflicting duplicate KIS US holding" in (decision.reason or "")
    db.close()
    engine.dispose()


def test_us_account_gate_deduplicates_identical_holdings_without_double_counting_pnl() -> None:
    holding = BrokerAccountHolding(
        symbol="AAPL",
        quantity=2,
        average_price=Decimal("100"),
        unrealized_pnl=Decimal("15"),
        payload={"ovrs_excg_cd": "NASD", "ovrs_stck_evlu_amt": "215"},
    )
    snapshots = [
        (
            exchange,
            BrokerAccountSnapshot(
                observed_at=datetime(2026, 8, 19, 14, 0, tzinfo=UTC),
                source=f"test:{exchange}",
                holdings=[holding] if exchange in {"NASD", "NYSE"} else [],
            ),
        )
        for exchange in ("AMEX", "NASD", "NYSE")
    ]

    merged = _merge_us_account_gate_snapshots(snapshots)

    assert len(merged.holdings) == 1
    assert merged.unrealized_pnl == Decimal("15")


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
                    "core_item_key": "core-us-20260819:AAPL:entry",
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
    assert item.source_plan_item_id == "core-us-20260819:AAPL:entry"
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


def test_us_execution_job_is_deferred_when_dispatch_occurs_after_new_york_close() -> None:
    settings = _settings(database_url="sqlite:///:memory:", kis_us_trading_enabled=True)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    plan = _build_execution_plan(
        settings=settings,
        command=ExecutionPlanRequest(
            plan_id="core-us-session-recheck-20260819",
            source="kai-trading-core",
            market_date=date(2026, 8, 19),
            items=[
                {
                    "symbol": "AAPL",
                    "source_plan_item_id": "core-us-session-recheck-20260819:AAPL:entry",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "provider": "kis",
                    "venue": "kis",
                    "asset_class": "us_equity",
                    "market": "US",
                    "exchange": "NASD",
                    "currency": "USD",
                    "limit_price": "201.25",
                    "time_in_force": "day",
                }
            ],
        ),
    )
    materialize_execution_jobs(plan)
    db.add(plan)
    db.commit()
    job = plan.items[0].jobs[0]

    deferred = _defer_us_next_open_job_outside_session(
        db=db,
        job=job,
        triggered_at=datetime(2026, 8, 19, 20, 30, tzinfo=UTC),
    )

    assert deferred is True
    assert job.status == "scheduled"
    assert job.attempts == 0
    assert job.scheduled_for == datetime(2026, 8, 20, 13, 30, tzinfo=UTC)
    assert "outside New York regular session" in (job.last_error or "")
    db.close()
    engine.dispose()


def test_us_target_sell_order_is_deferred_outside_new_york_regular_session() -> None:
    settings = _settings(
        database_url="sqlite:///:memory:",
        kis_us_trading_enabled=True,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    plan = _build_execution_plan(
        settings=settings,
        command=ExecutionPlanRequest(
            plan_id="core-us-target-session-gate-20260819",
            source="kai-trading-core",
            market_date=date(2026, 8, 19),
            items=[
                {
                    "symbol": "AAPL",
                    "source_plan_item_id": "core-us-target-session-gate:AAPL:target",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "provider": "kis",
                    "venue": "kis",
                    "asset_class": "us_equity",
                    "market": "US",
                    "exchange": "NASD",
                    "currency": "USD",
                    "trigger_price": "210.00",
                    "limit_price": "210.00",
                    "time_in_force": "day",
                }
            ],
        ),
    )
    item = plan.items[0]
    db.add(plan)
    db.commit()
    resume_runtime(
        settings=settings,
        db=db,
        command=ControlCommand(actor="test", source="test", reason="session gate test"),
    )
    command = PlaceOrderRequest(
        idempotency_key="monitoring:us-target-session-gate",
        execution_plan_item_id=item.id,
        provider="kis",
        asset_class="us_equity",
        venue="kis",
        symbol="AAPL",
        side="sell",
        quantity=1,
        order_type="limit",
        limit_price=Decimal("210"),
        exchange="NASD",
        metadata={
            "order_purpose": "target_sell",
            "route_now": "2026-08-19T20:30:00+00:00",
        },
    )

    result = place_broker_order(
        settings=settings,
        db=db,
        adapter=SimpleNamespace(provider="kis"),
        command=command,
        request_source="monitoring:autonomous_monitoring",
    )

    assert result.status_code == 202
    assert result.payload["deferred"] is True
    assert result.payload["route"]["session"] == "us_closed"
    assert result.payload["route"]["next_retry_at"] == "2026-08-20T13:30:00+00:00"
    assert db.scalars(select(BrokerOrder)).all() == []
    db.close()
    engine.dispose()


def test_us_execution_handoff_backfills_provider_venue_and_identity_from_core_aliases() -> None:
    command = ExecutionPlanRequest(
        plan_id="core-us-aliases-20260819",
        source="kai-trading-core",
        items=[
            {
                "symbol": "AAPL",
                "core_item_key": "core-us-aliases:AAPL:entry",
                "side": "buy",
                "intent": "next_open_buy",
                "quantity_policy": {"kind": "shares", "quantity": 1},
                "broker": "kis",
                "asset_class": "us_equity",
                "market": "US",
                "exchange": "NASD",
                "currency": "USD",
            }
        ],
    )

    item = command.items[0]
    assert item.provider == "kis"
    assert item.venue == "kis"
    assert item.source_plan_item_id == "core-us-aliases:AAPL:entry"


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


def test_real_core_v3_fixture_materializes_kis_us_job_and_protective_monitors() -> None:
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "core_us_execution_plan_v3.json").read_text()
    )
    plan = _build_execution_plan(
        settings=_settings(kis_us_trading_enabled=True),
        command=ExecutionPlanRequest.model_validate(payload),
    )

    materialize_execution_jobs(plan)
    materialize_monitoring_plan_items(plan)

    items = {item.intent: item for item in plan.items}
    assert items["next_open_buy"].provider == "kis"
    assert items["next_open_buy"].venue == "kis"
    assert items["next_open_buy"].source_plan_item_id == "us:2026-08-19:AAPL:next_open_buy"
    assert items["next_open_buy"].jobs[0].scheduled_for == datetime(2026, 8, 20, 13, 30, tzinfo=UTC)
    assert items["target_sell"].status == "target_sell_pending_config"
    assert items["stop_sell"].status == "stop_sell_pending_config"
    assert items["target_sell"].risk_context["exchange"] == "NASD"
    assert items["stop_sell"].risk_context["exchange"] == "NASD"
