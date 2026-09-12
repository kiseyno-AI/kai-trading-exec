from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import time as system_time
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.broker.base import BrokerAccountSnapshot, BrokerAdapterError
from app.broker.kis import (
    KIS_BALANCE_PATH,
    KIS_DAILY_CCLD_PATH,
    KIS_OPEN_ORDERS_PATH,
    JsonHttpResponse,
    KisBrokerAdapter,
    _daily_ccld_period_division,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlanItem, ExecutionPosition
from app.services import exec_agent as exec_agent_service
from app.services import trigger_time_exec_agent as trigger_time_exec_agent_service
from app.services.position_identity import build_position_identity
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot


class QueueHttpClient:
    def __init__(self, responses: list[JsonHttpResponse]) -> None:
        self.responses = list(responses)
        self.get_calls: list[dict[str, object]] = []

    def get_json(self, **kwargs: object) -> JsonHttpResponse:
        self.get_calls.append(dict(kwargs))
        return self.responses.pop(0)

    def post_json(self, **_kwargs: object) -> JsonHttpResponse:
        raise AssertionError("snapshot tests must not make POST requests")


def _settings() -> Settings:
    return Settings(
        runtime_mode="paper",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_account_number="12345678",
        kis_use_hashkey=False,
    )


def _response(
    payload: dict[str, object],
    *,
    tr_cont: str = "D",
) -> JsonHttpResponse:
    return JsonHttpResponse(
        status_code=200,
        headers={"tr_cont": tr_cont},
        payload={"rt_cd": "0", **payload},
    )


def test_domestic_snapshot_paginates_balance_and_open_orders_without_changing_route() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [{"pdno": "005930", "hldg_qty": "2"}],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                    "ctx_area_fk100": "balance-fk",
                    "ctx_area_nk100": "balance-nk",
                },
                tr_cont="F",
            ),
            _response(
                {
                    "output1": [{"pdno": "000660", "hldg_qty": "3"}],
                    "output2": [],
                }
            ),
            _response(
                {
                    "output1": [
                        {
                            "pdno": "005930",
                            "odno": "11",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "2",
                            "ord_uncc_qty": "1",
                            "ord_unpr": "70000",
                        }
                    ],
                    "ctx_area_fk100": "orders-fk",
                    "ctx_area_nk100": "orders-nk",
                },
                tr_cont="M",
            ),
            _response(
                {
                    "output1": [
                        {
                            "pdno": "000660",
                            "odno": "12",
                            "sll_buy_dvsn_cd": "01",
                            "ord_qty": "3",
                            "ord_uncc_qty": "2",
                            "ord_unpr": "120000",
                        }
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="KRX")

    assert snapshot.available_cash == Decimal("1000000")
    assert [holding.symbol for holding in snapshot.holdings] == ["005930", "000660"]
    assert [order.broker_order_id for order in snapshot.open_orders] == ["11", "12"]
    assert snapshot.holdings_complete is True
    assert snapshot.open_orders_complete is True
    assert snapshot.diagnostics == {
        "requested_exchange": "KRX",
        "holdings_pagination": {
            "complete": True,
            "page_count": 2,
            "cursor_width": 100,
        },
        "open_orders_pagination": {
            "complete": True,
            "page_count": 2,
            "cursor_width": 100,
        },
        "holdings_parse": {
            "raw_row_count": 2,
            "parsed_row_count": 2,
            "omitted_row_count": 0,
            "invalid_row_count": 0,
            "duplicate_identity_count": 0,
        },
        "open_orders_parse": {
            "raw_row_count": 2,
            "parsed_row_count": 2,
            "omitted_row_count": 0,
            "invalid_row_count": 0,
            "duplicate_identity_count": 0,
        },
    }
    assert snapshot.payload["holdings_coverage"] == "complete"
    assert snapshot.payload["open_orders_coverage"] == "complete"
    assert [call["url"] for call in http.get_calls] == [
        f"https://openapivts.koreainvestment.com:29443{KIS_BALANCE_PATH}",
        f"https://openapivts.koreainvestment.com:29443{KIS_BALANCE_PATH}",
        f"https://openapivts.koreainvestment.com:29443{KIS_OPEN_ORDERS_PATH}",
        f"https://openapivts.koreainvestment.com:29443{KIS_OPEN_ORDERS_PATH}",
    ]
    balance_next = http.get_calls[1]
    assert balance_next["params"]["CANO"] == "12345678"  # type: ignore[index]
    assert balance_next["params"]["ACNT_PRDT_CD"] == "01"  # type: ignore[index]
    assert balance_next["params"]["CTX_AREA_FK100"] == "balance-fk"  # type: ignore[index]
    assert balance_next["params"]["CTX_AREA_NK100"] == "balance-nk"  # type: ignore[index]
    assert balance_next["headers"]["tr_cont"] == "N"  # type: ignore[index]
    orders_next = http.get_calls[3]
    assert orders_next["params"]["CANO"] == "12345678"  # type: ignore[index]
    assert orders_next["params"]["ACNT_PRDT_CD"] == "01"  # type: ignore[index]
    assert orders_next["params"]["CTX_AREA_FK100"] == "orders-fk"  # type: ignore[index]
    assert orders_next["params"]["CTX_AREA_NK100"] == "orders-nk"  # type: ignore[index]
    assert orders_next["params"]["INQR_STRT_DT"] == orders_next["params"]["INQR_END_DT"]  # type: ignore[index]
    assert orders_next["params"]["CCLD_DVSN"] == "02"  # type: ignore[index]
    assert orders_next["headers"]["tr_cont"] == "N"  # type: ignore[index]


def test_domestic_snapshot_rejects_a_repeated_continuation_cursor() -> None:
    repeated_page = {
        "output1": [{"pdno": "005930", "hldg_qty": "2"}],
        "output2": [{"ord_psbl_cash": "1000000"}],
        "ctx_area_fk100": "same-fk",
        "ctx_area_nk100": "same-nk",
    }
    http = QueueHttpClient(
        [
            _response(repeated_page, tr_cont="F"),
            _response(repeated_page, tr_cont="M"),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError, match="repeated continuation cursor"):
        adapter.get_account_snapshot("token", exchange="KRX")

    assert len(http.get_calls) == 2


@pytest.mark.parametrize("tr_cont", ["D", "E"])
def test_domestic_snapshot_accepts_only_documented_terminal_continuation_states(
    tr_cont: str,
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                },
                tr_cont=tr_cont,
            ),
            _response({"output1": []}, tr_cont=tr_cont),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="KRX")

    assert snapshot.holdings_complete is True
    assert snapshot.open_orders_complete is True
    assert len(http.get_calls) == 2


@pytest.mark.parametrize(
    "headers, expected_state",
    [
        ({}, "missing"),
        ({"tr_cont": ""}, "missing"),
        ({"tr_cont": "X"}, "X"),
    ],
)
def test_domestic_snapshot_rejects_missing_or_unknown_continuation_state(
    headers: dict[str, str],
    expected_state: str,
) -> None:
    http = QueueHttpClient(
        [
            JsonHttpResponse(
                status_code=200,
                headers=headers,
                payload={
                    "rt_cd": "0",
                    "output1": [],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                },
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(
        BrokerAdapterError,
        match=rf"invalid continuation state: {expected_state}",
    ):
        adapter.get_account_snapshot("token", exchange="KRX")

    assert len(http.get_calls) == 1


def test_domestic_order_reconciliation_finds_a_match_on_a_later_page() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {
                            "odno": "other-order",
                            "pdno": "005930",
                            "ord_qty": "1",
                            "tot_ccld_qty": "0",
                            "rmn_qty": "1",
                        }
                    ],
                    "ctx_area_fk100": "daily-fk",
                    "ctx_area_nk100": "daily-nk",
                },
                tr_cont="F",
            ),
            _response(
                {
                    "output1": [
                        {
                            "odno": "target-order",
                            "pdno": "000660",
                            "excg_id_dvsn_Cd": "nxt",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "2",
                            "tot_ccld_qty": "2",
                            "rmn_qty": "0",
                            "avg_pric": "120000",
                            "ord_dt": "20260910",
                            "ord_tmd": "101500",
                            "ord_st_dvsn_cd_name": "체결",
                        }
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "target-order",
        exchange="NXT",
        submitted_at=datetime(2026, 9, 10, 1, tzinfo=UTC),
        expected_symbol="000660",
    )

    assert result is not None and result["status"] == "filled"
    assert len(http.get_calls) == 2
    first_params = http.get_calls[0]["params"]
    next_params = http.get_calls[1]["params"]
    assert next_params["CANO"] == first_params["CANO"] == "12345678"  # type: ignore[index]
    assert next_params["ACNT_PRDT_CD"] == first_params["ACNT_PRDT_CD"] == "01"  # type: ignore[index]
    assert next_params["ODNO"] == first_params["ODNO"] == "target-order"  # type: ignore[index]
    assert next_params["PDNO"] == first_params["PDNO"] == "000660"  # type: ignore[index]
    assert next_params["EXCG_ID_DVSN_CD"] == first_params["EXCG_ID_DVSN_CD"] == "NXT"  # type: ignore[index]
    assert next_params["INQR_STRT_DT"] == first_params["INQR_STRT_DT"] == "20260910"  # type: ignore[index]
    assert next_params["INQR_END_DT"] == first_params["INQR_END_DT"] == "20260910"  # type: ignore[index]
    assert next_params["CTX_AREA_FK100"] == "daily-fk"  # type: ignore[index]
    assert next_params["CTX_AREA_NK100"] == "daily-nk"  # type: ignore[index]
    assert http.get_calls[1]["headers"]["tr_cont"] == "N"  # type: ignore[index]


def test_domestic_order_reconciliation_skips_every_inexact_identity_before_exact_row() -> None:
    submitted_at = datetime(2026, 9, 9, 16, 5, tzinfo=UTC)
    common = {
        "odno": "reused-order-id",
        "ord_qty": "2",
        "tot_ccld_qty": "2",
        "rmn_qty": "0",
        "ord_tmd": "010500",
        "ord_st_dvsn_cd_name": "체결",
    }
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {
                            **common,
                            "pdno": "005930",
                            "ord_dt": "20260910",
                            "excg_id_dvsn_Cd": "NXT",
                            "avg_pric": "1",
                        },
                        {
                            **common,
                            "pdno": "000660",
                            "ord_dt": "20260909",
                            "excg_id_dvsn_Cd": "NXT",
                            "avg_pric": "2",
                        },
                        {
                            **common,
                            "pdno": "000660",
                            "ord_dt": "20260910",
                            "excg_id_dvsn_Cd": "KRX",
                            "avg_pric": "3",
                        },
                        {
                            **common,
                            "pdno": "000660",
                            "ord_dt": "20260910",
                            "avg_pric": "4",
                        },
                        {
                            **common,
                            "pdno": "000660",
                            "ord_dt": "20260910",
                            "excg_id_dvsn_Cd": "nXt",
                            "avg_pric": "120000",
                        },
                    ]
                }
            )
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "reused-order-id",
        exchange="NXT",
        submitted_at=submitted_at,
        expected_symbol="000660",
    )

    assert result is not None
    assert result["status"] == "filled"
    assert result["symbol"] == "000660"
    assert result["average_fill_price"] == Decimal("120000")
    assert http.get_calls[0]["params"]["PDNO"] == "000660"  # type: ignore[index]
    assert http.get_calls[0]["params"]["INQR_STRT_DT"] == "20260910"  # type: ignore[index]
    assert http.get_calls[0]["params"]["INQR_END_DT"] == "20260910"  # type: ignore[index]


@pytest.mark.parametrize(
    "row_patch",
    [
        {"pdno": "005930"},
        {"ord_dt": "20260909"},
        {"excg_id_dvsn_Cd": "KRX"},
        {"excg_id_dvsn_Cd": None},
    ],
)
def test_domestic_order_reconciliation_returns_unknown_for_inexact_identity(
    row_patch: dict[str, object],
) -> None:
    exact_row: dict[str, object] = {
        "odno": "identity-order",
        "pdno": "000660",
        "ord_dt": "20260910",
        "excg_id_dvsn_Cd": "NXT",
        "ord_qty": "1",
        "tot_ccld_qty": "1",
        "rmn_qty": "0",
        "ord_tmd": "010500",
        "ord_st_dvsn_cd_name": "체결",
    }
    exact_row.update(row_patch)
    http = QueueHttpClient([_response({"output1": [exact_row]})])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "identity-order",
        exchange="NXT",
        submitted_at=datetime(2026, 9, 9, 16, 5, tzinfo=UTC),
        expected_symbol="000660",
    )

    assert result is not None
    assert result["status"] == "unknown"


@pytest.mark.parametrize(
    "row_patch",
    [
        {"ord_qty": "2.5"},
        {"ord_qty": "not-an-integer"},
        {"tot_ccld_qty": "1.5"},
        {"tot_ccld_qty": "not-an-integer"},
        {"rmn_qty": "0.5"},
        {"rmn_qty": "not-an-integer"},
        {"tot_ccld_qty": "-1", "rmn_qty": "0"},
        {"tot_ccld_qty": "2", "rmn_qty": "-1"},
        {"tot_ccld_qty": "3", "rmn_qty": "0"},
        {"tot_ccld_qty": "1", "rmn_qty": "2"},
        {"tot_ccld_qty": "1", "rmn_qty": "0"},
        {
            "tot_ccld_qty": "1",
            "rmn_qty": "1",
            "ord_st_dvsn_cd_name": "미체결",
        },
        {
            "tot_ccld_qty": "2",
            "rmn_qty": "0",
            "ord_st_dvsn_cd_name": "부분체결",
        },
    ],
)
def test_domestic_reconciliation_rejects_inexact_or_inconsistent_quantity_evidence(
    row_patch: dict[str, object],
) -> None:
    row: dict[str, object] = {
        "odno": "domestic-quantity-order",
        "pdno": "005930",
        "excg_id_dvsn_Cd": "KRX",
        "ord_qty": "2",
        "tot_ccld_qty": "2",
        "rmn_qty": "0",
        "ord_dt": "20260910",
        "ord_tmd": "101500",
        "ord_st_dvsn_cd_name": "체결",
    }
    row.update(row_patch)
    http = QueueHttpClient([_response({"output1": [row]})])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "domestic-quantity-order",
        exchange="KRX",
        submitted_at=datetime(2026, 9, 10, tzinfo=UTC),
        expected_symbol="005930",
    )

    assert result is not None
    assert result["status"] == "unknown"
    assert "filled_quantity" not in result


def test_domestic_reconciliation_never_skips_corrupt_exact_identity_for_a_later_row() -> None:
    common: dict[str, object] = {
        "odno": "duplicate-quantity-order",
        "pdno": "005930",
        "excg_id_dvsn_Cd": "KRX",
        "ord_qty": "2",
        "rmn_qty": "0",
        "ord_dt": "20260910",
        "ord_tmd": "101500",
        "ord_st_dvsn_cd_name": "체결",
    }
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {**common, "tot_ccld_qty": "2.5"},
                        {**common, "tot_ccld_qty": "2"},
                    ]
                }
            )
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "duplicate-quantity-order",
        exchange="KRX",
        submitted_at=datetime(2026, 9, 10, tzinfo=UTC),
        expected_symbol="005930",
    )

    assert result is not None
    assert result["status"] == "unknown"
    assert "filled_quantity" not in result


@pytest.mark.parametrize(
    "row_patch",
    [
        {"ft_ord_qty": "2.5", "ord_qty": "2"},
        {"ft_ord_qty": "not-an-integer", "ord_qty": "2"},
        {"ft_ccld_qty": "1.5"},
        {"ft_ccld_qty": "not-an-integer"},
        {"nccs_qty": "0.5"},
        {"nccs_qty": "not-an-integer"},
        {"ft_ccld_qty": "-1", "nccs_qty": "0"},
        {"ft_ccld_qty": "2", "nccs_qty": "-1"},
        {"ft_ccld_qty": "3", "nccs_qty": "0"},
        {"ft_ccld_qty": "1", "nccs_qty": "2"},
        {"ft_ccld_qty": "1", "nccs_qty": "0"},
        {
            "ft_ccld_qty": "1",
            "nccs_qty": "1",
            "prcs_stat_name": "미체결",
        },
        {
            "ft_ccld_qty": "2",
            "nccs_qty": "0",
            "prcs_stat_name": "부분체결",
        },
    ],
)
def test_overseas_reconciliation_rejects_inexact_or_inconsistent_quantity_evidence(
    row_patch: dict[str, object],
) -> None:
    row: dict[str, object] = {
        "odno": "overseas-quantity-order",
        "pdno": "AAPL",
        "ft_ord_qty": "2",
        "ft_ccld_qty": "2",
        "nccs_qty": "0",
        "ord_dt": "20260910",
        "ord_tmd": "101500",
        "prcs_stat_name": "체결",
    }
    row.update(row_patch)
    http = QueueHttpClient([_response({"output": [row]})])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    result = adapter.reconcile_order(
        "token",
        "overseas-quantity-order",
        exchange="NASD",
        submitted_at=datetime(2026, 9, 10, tzinfo=UTC),
        expected_symbol="AAPL",
    )

    assert result is not None
    assert result["status"] == "unknown"
    assert "filled_quantity" not in result


@pytest.mark.parametrize("rt_cd", [None, "", 0, True, {"nested": "0"}])
def test_domestic_account_snapshot_rejects_missing_or_non_string_business_code(
    rt_cd: object,
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "rt_cd": rt_cd,
                    "output1": [],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            )
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError, match="rt_cd"):
        adapter.get_account_snapshot("token", exchange="KRX")

    assert len(http.get_calls) == 1


@pytest.mark.parametrize("rt_cd", [None, "", 0, True, ["0"]])
def test_domestic_reconciliation_rejects_missing_or_non_string_business_code(
    rt_cd: object,
) -> None:
    http = QueueHttpClient([_response({"rt_cd": rt_cd, "output1": []})])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError, match="rt_cd"):
        adapter.reconcile_order(
            "token",
            "identity-order",
            exchange="KRX",
            submitted_at=datetime(2026, 9, 10, tzinfo=UTC),
            expected_symbol="005930",
        )


def test_daily_ccld_period_uses_three_calendar_month_boundary() -> None:
    observed_at = datetime(2026, 5, 31, 3, tzinfo=UTC)

    assert _daily_ccld_period_division(
        submitted_at=datetime(2026, 2, 27, 14, 59, tzinfo=UTC),
        observed_at=observed_at,
    ) == "before"
    assert _daily_ccld_period_division(
        submitted_at=datetime(2026, 2, 27, 15, 0, tzinfo=UTC),
        observed_at=observed_at,
    ) == "inner"


def test_domestic_reconciliation_interprets_naive_database_timestamp_as_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {
                            "odno": "naive-domestic-order",
                            "pdno": "005930",
                            "excg_id_dvsn_Cd": "KRX",
                            "ord_qty": "2",
                            "tot_ccld_qty": "2",
                            "rmn_qty": "0",
                            "ord_dt": "20260910",
                            "ord_tmd": "210000",
                            "ord_st_dvsn_cd_name": "체결",
                        }
                    ]
                }
            )
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    try:
        with monkeypatch.context() as local_timezone:
            local_timezone.setenv("TZ", "America/New_York")
            system_time.tzset()
            result = adapter.reconcile_order(
                "token",
                "naive-domestic-order",
                exchange="KRX",
                submitted_at=datetime(2026, 9, 10, 12),
                expected_symbol="005930",
            )
            period = _daily_ccld_period_division(
                submitted_at=datetime(2026, 2, 27, 12),
                observed_at=datetime(2026, 5, 31, 3, tzinfo=UTC),
            )
    finally:
        system_time.tzset()

    assert result is not None and result["status"] == "filled"
    assert http.get_calls[0]["params"]["INQR_STRT_DT"] == "20260910"  # type: ignore[index]
    assert http.get_calls[0]["params"]["INQR_END_DT"] == "20260910"  # type: ignore[index]
    assert period == "before"


def test_overseas_reconciliation_interprets_naive_database_timestamp_as_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http = QueueHttpClient([_response({"output": []})])
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    try:
        with monkeypatch.context() as local_timezone:
            local_timezone.setenv("TZ", "America/New_York")
            system_time.tzset()
            adapter.reconcile_order(
                "token",
                "naive-overseas-order",
                exchange="NASD",
                submitted_at=datetime(2026, 9, 10, 3),
                expected_symbol="AAPL",
            )
    finally:
        system_time.tzset()

    assert http.get_calls[0]["params"]["ORD_STRT_DT"] == "20260909"  # type: ignore[index]


def test_position_sync_never_zeroes_a_position_from_incomplete_holdings() -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        source="test:incomplete",
        holdings=[],
        holdings_complete=False,
        diagnostics={
            "holdings_pagination": {
                "complete": False,
                "reason": "page_limit_exceeded",
            }
        },
        payload={"holdings_coverage": "incomplete", "exchange": "NASD"},
    )
    calls: list[tuple[list[str] | None, str]] = []

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del reference_prices
        calls.append((symbols, exchange))
        return snapshot

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    try:
        with session_factory() as db:
            db.add(
                ExecutionPosition(
                    mode="paper",
                    provider=identity.provider,
                    account_scope=identity.account_scope,
                    exchange=identity.exchange,
                    symbol="AAPL",
                    quantity=5,
                )
            )
            db.commit()

            with pytest.raises(BrokerAdapterError, match="holdings coverage is incomplete"):
                sync_execution_positions_from_broker_snapshot(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    symbols={"AAPL"},
                    exchange="NASD",
                    authenticated_access_token="token",
                )

            db.expire_all()
            position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "AAPL"))
            assert position is not None and position.quantity == 5
            assert position.last_reconciled_at is None
            assert calls == [(["AAPL"], "NASD")]
    finally:
        engine.dispose()


def test_position_sync_does_not_retry_unscoped_after_internal_type_error() -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    calls: list[tuple[list[str] | None, str]] = []

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del reference_prices
        calls.append((symbols, exchange))
        if exchange == "NASD":
            raise TypeError("internal NASD snapshot parser failure")
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            source="dangerous-default-krx-snapshot",
        )

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    try:
        with session_factory() as db:
            with pytest.raises(TypeError, match="internal NASD snapshot parser failure"):
                sync_execution_positions_from_broker_snapshot(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    symbols={"AAPL"},
                    exchange="NASD",
                    authenticated_access_token="token",
                )

            assert calls == [(["AAPL"], "NASD")]
            assert db.scalar(select(ExecutionPosition)) is None
    finally:
        engine.dispose()


def test_exec_agent_does_not_retry_a_failed_us_snapshot_as_default_krx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    calls: list[tuple[list[str] | None, str]] = []

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del reference_prices
        calls.append((symbols, exchange))
        if exchange == "NASD":
            raise TypeError("internal NASD snapshot failure")
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            source="dangerous-default-krx-snapshot",
        )

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    candidate = ExecutionPlanItem(
        id="us-candidate",
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        status="pending",
        provider="kis",
        venue="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
    )
    monkeypatch.setattr(
        exec_agent_service,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    try:
        with session_factory() as db:
            with pytest.raises(TypeError, match="internal NASD snapshot failure"):
                exec_agent_service._load_account_context(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    candidates=[candidate],
                )

        assert calls == [(None, "NASD")]
    finally:
        engine.dispose()


def test_trigger_time_agent_fails_closed_without_unscoped_snapshot_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    calls: list[tuple[list[str] | None, dict[str, Decimal] | None, str]] = []

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        calls.append((symbols, reference_prices, exchange))
        if exchange == "NASD":
            raise TypeError("internal trigger-time NASD snapshot failure")
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            source="dangerous-default-krx-snapshot",
        )

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    item = ExecutionPlanItem(
        id="trigger-us-candidate",
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        status="pending",
        provider="kis",
        venue="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
    )
    monkeypatch.setattr(
        trigger_time_exec_agent_service,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    try:
        with session_factory() as db:
            context = trigger_time_exec_agent_service._load_account_context(
                settings=settings,
                db=db,
                adapter=adapter,
                item=item,
                observed_price=Decimal("201.25"),
                observed_at=datetime(2026, 9, 11, 14, 30, tzinfo=UTC),
            )

        assert context["available"] is False
        assert context["reason"] == (
            "TypeError: internal trigger-time NASD snapshot failure"
        )
        assert calls == [(["AAPL"], {"AAPL": Decimal("201.25")}, "NASD")]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("side", "expected_available"),
    [("buy", False), ("sell", True)],
)
def test_trigger_time_buy_fails_closed_on_incomplete_reservation_truth_without_blocking_sell(
    monkeypatch: pytest.MonkeyPatch,
    side: str,
    expected_available: bool,
) -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=datetime.now(UTC),
        source="test:incomplete-open-orders",
        open_orders_complete=False,
        diagnostics={"open_orders_parse": {"invalid_row_count": 1}},
        payload={"open_orders_coverage": "incomplete"},
    )

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        assert symbols == ["005930"]
        assert reference_prices == {"005930": Decimal("70000")}
        assert exchange == "KRX"
        return snapshot

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    item = ExecutionPlanItem(
        id=f"trigger-incomplete-{side}",
        plan_item_index=0,
        symbol="005930",
        side=side,
        intent="target_sell" if side == "sell" else "intraday_watch_buy",
        status="pending",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    monkeypatch.setattr(
        trigger_time_exec_agent_service,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    try:
        with session_factory() as db:
            context = trigger_time_exec_agent_service._load_account_context(
                settings=settings,
                db=db,
                adapter=adapter,
                item=item,
                observed_price=Decimal("70000"),
                observed_at=datetime.now(UTC),
            )

        assert context["available"] is expected_available
        if side == "buy":
            assert context["reason"] == "broker open-order coverage is incomplete"
        else:
            assert context["holdings_complete"] is True
            assert context["open_orders_complete"] is False
            assert context["diagnostics"] == snapshot.diagnostics
    finally:
        engine.dispose()


def test_exec_agent_rejects_incomplete_broker_order_reservation_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings().model_copy(
        update={"database_url": "sqlite:///:memory:"},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        source="test:incomplete-open-orders",
        open_orders_complete=False,
        diagnostics={
            "open_orders_pagination": {
                "complete": False,
                "reason": "broker_rejected_daily_order_query",
            }
        },
        payload={"open_orders_coverage": "incomplete"},
    )

    def get_account_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del symbols, reference_prices
        assert exchange == "NASD"
        return snapshot

    adapter = SimpleNamespace(provider="kis", get_account_snapshot=get_account_snapshot)
    candidate = ExecutionPlanItem(
        id="reservation-candidate",
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        status="pending",
        provider="kis",
        venue="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
    )
    monkeypatch.setattr(
        exec_agent_service,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="token"),
    )
    try:
        with session_factory() as db:
            context = exec_agent_service._load_account_context(
                settings=settings,
                db=db,
                adapter=adapter,
                candidates=[candidate],
            )

        assert context.available_cash is None
        assert context.buying_power is None
        assert context.snapshot_payload == {
            "available": False,
            "reason": (
                "broker account snapshot is incomplete for exec-agent sizing: "
                "open_orders"
            ),
            "observed_at": snapshot.observed_at.isoformat(),
        }
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("runtime_mode", "exchange", "orders_market"),
    [
        ("paper", "NXT", "KRX"),
        ("paper", "SOR", "KRX"),
        ("live", "NXT", "ALL"),
        ("live", "SOR", "ALL"),
    ],
)
def test_domestic_snapshot_uses_account_wide_inventory_and_reservation_truth(
    runtime_mode: str,
    exchange: str,
    orders_market: str,
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [{"pdno": "005930", "hldg_qty": "2"}],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response({"output1": []}),
        ]
    )
    adapter = KisBrokerAdapter(
        settings=_settings().model_copy(update={"runtime_mode": runtime_mode}),
        http_client=http,
    )

    snapshot = adapter.get_account_snapshot("token", exchange=exchange)

    assert snapshot.payload["exchange"] == exchange
    assert http.get_calls[0]["params"]["AFHR_FLPR_YN"] == "N"  # type: ignore[index]
    assert http.get_calls[1]["params"]["EXCG_ID_DVSN_CD"] == orders_market  # type: ignore[index]


def test_snapshot_marks_malformed_holdings_and_open_orders_incomplete() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [{"pdno": "005930", "hldg_qty": "not-a-number"}],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response(
                {
                    "output1": [
                        {
                            "pdno": "000660",
                            "odno": "malformed-order",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "not-a-number",
                            "ord_uncc_qty": "1",
                        }
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="KRX")

    assert snapshot.holdings == []
    assert snapshot.open_orders == []
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.payload["holdings_coverage"] == "incomplete"
    assert snapshot.payload["open_orders_coverage"] == "incomplete"
    assert snapshot.diagnostics["holdings_parse"] == {
        "raw_row_count": 1,
        "parsed_row_count": 0,
        "omitted_row_count": 0,
        "invalid_row_count": 1,
        "duplicate_identity_count": 0,
    }
    assert snapshot.diagnostics["open_orders_parse"] == {
        "raw_row_count": 1,
        "parsed_row_count": 0,
        "omitted_row_count": 0,
        "invalid_row_count": 1,
        "duplicate_identity_count": 0,
    }


@pytest.mark.parametrize(
    ("exchange", "holding_row", "order_key", "order_row"),
    [
        (
            "KRX",
            {"pdno": "005930", "hldg_qty": "1.9"},
            "output1",
            {
                "pdno": "005930",
                "odno": "fractional-domestic-order",
                "sll_buy_dvsn_cd": "02",
                "ord_qty": "2",
                "ord_uncc_qty": "1.1",
            },
        ),
        (
            "NASD",
            {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "1.9"},
            "output",
            {
                "pdno": "AAPL",
                "odno": "fractional-overseas-order",
                "sll_buy_dvsn_cd": "02",
                "ft_ord_qty": "2.5",
                "nccs_qty": "1",
            },
        ),
    ],
)
def test_snapshot_rejects_fractional_share_quantities(
    exchange: str,
    holding_row: dict[str, object],
    order_key: str,
    order_row: dict[str, object],
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [holding_row],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response({order_key: [order_row]}),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange=exchange)

    assert snapshot.holdings == []
    assert snapshot.open_orders == []
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.diagnostics["holdings_parse"]["invalid_row_count"] == 1
    assert snapshot.diagnostics["open_orders_parse"]["invalid_row_count"] == 1


@pytest.mark.parametrize(
    ("exchange", "holding_row"),
    [
        ("KRX", {"pdno": "005930", "hldg_qty": "2", "ord_psbl_qty": "1.5"}),
        (
            "KRX",
            {"pdno": "005930", "hldg_qty": "2", "ord_psbl_qty": "invalid"},
        ),
        (
            "NASD",
            {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "2", "ord_psbl_qty": "-1"},
        ),
        (
            "NASD",
            {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "2", "ord_psbl_qty": "3"},
        ),
    ],
)
def test_snapshot_rejects_invalid_present_sellable_holding_quantity(
    exchange: str,
    holding_row: dict[str, object],
) -> None:
    open_orders_key = "output1" if exchange == "KRX" else "output"
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [holding_row],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response({open_orders_key: []}),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange=exchange)

    assert snapshot.holdings == []
    assert snapshot.holdings_complete is False
    assert snapshot.diagnostics["holdings_parse"]["invalid_row_count"] == 1


def test_domestic_snapshot_rejects_raw_non_objects_duplicates_and_missing_order_identity() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {"pdno": "005930", "hldg_qty": "2"},
                        {"pdno": "005930", "hldg_qty": "3"},
                        "corrupt-holding-row",
                        {"pdno": "000660", "hldg_qty": "0"},
                    ],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response(
                {
                    "output1": [
                        {
                            "pdno": "005930",
                            "odno": "open-1",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "2",
                            "ord_uncc_qty": "1",
                        },
                        {
                            "pdno": "005930",
                            "odno": "open-1",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "2",
                            "ord_uncc_qty": "1",
                        },
                        "corrupt-order-row",
                        {
                            "pdno": "000660",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "1",
                            "ord_uncc_qty": "1",
                        },
                        {
                            "pdno": "035420",
                            "odno": "unknown-side",
                            "ord_qty": "1",
                            "ord_uncc_qty": "1",
                        },
                        {
                            "pdno": "035720",
                            "odno": "missing-remaining",
                            "sll_buy_dvsn_cd": "01",
                            "ord_qty": "1",
                        },
                        {
                            "pdno": "068270",
                            "odno": "filled-1",
                            "sll_buy_dvsn_cd": "01",
                            "ord_qty": "2",
                            "ord_uncc_qty": "0",
                        },
                        {
                            "pdno": "207940",
                            "odno": "zero-1",
                            "sll_buy_dvsn_cd": "02",
                            "ord_qty": "0",
                            "ord_uncc_qty": "0",
                        },
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="KRX")

    assert len(snapshot.holdings) == 2
    assert len(snapshot.open_orders) == 2
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.diagnostics["holdings_parse"] == {
        "raw_row_count": 4,
        "parsed_row_count": 2,
        "omitted_row_count": 1,
        "invalid_row_count": 1,
        "duplicate_identity_count": 1,
    }
    assert snapshot.diagnostics["open_orders_parse"] == {
        "raw_row_count": 8,
        "parsed_row_count": 2,
        "omitted_row_count": 2,
        "invalid_row_count": 4,
        "duplicate_identity_count": 1,
    }


def test_domestic_snapshot_keeps_known_zero_and_fully_filled_rows_complete() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [{"pdno": "005930", "hldg_qty": "0"}],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response(
                {
                    "output1": [
                        {
                            "pdno": "005930",
                            "odno": "filled-1",
                            "sll_buy_dvsn_cd": "01",
                            "ord_qty": "2",
                            "ord_uncc_qty": "0",
                        }
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="KRX")

    assert snapshot.holdings == []
    assert snapshot.open_orders == []
    assert snapshot.holdings_complete is True
    assert snapshot.open_orders_complete is True
    assert snapshot.diagnostics["holdings_parse"]["omitted_row_count"] == 1
    assert snapshot.diagnostics["open_orders_parse"]["omitted_row_count"] == 1


def test_overseas_snapshot_marks_malformed_and_duplicate_rows_incomplete() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [
                        {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "2"},
                        {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "1"},
                        "corrupt-holding-row",
                        {"ovrs_pdno": "MSFT", "ovrs_cblc_qty": "0"},
                    ],
                    "output2": [{"frcr_dncl_amt_2": "5000"}],
                }
            ),
            _response(
                {
                    "output": [
                        {
                            "pdno": "NVDA",
                            "odno": "us-open-1",
                            "sll_buy_dvsn_cd": "02",
                            "ft_ord_qty": "2",
                            "nccs_qty": "1",
                        },
                        {
                            "pdno": "NVDA",
                            "odno": "us-open-1",
                            "sll_buy_dvsn_cd": "02",
                            "ft_ord_qty": "2",
                            "nccs_qty": "1",
                        },
                        "corrupt-order-row",
                        {
                            "pdno": "TSLA",
                            "sll_buy_dvsn_cd": "01",
                            "ft_ord_qty": "1",
                            "nccs_qty": "1",
                        },
                        {
                            "pdno": "META",
                            "odno": "us-filled-1",
                            "sll_buy_dvsn_cd": "01",
                            "ft_ord_qty": "2",
                            "nccs_qty": "0",
                        },
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="NASD")

    assert len(snapshot.holdings) == 2
    assert len(snapshot.open_orders) == 2
    assert snapshot.holdings_complete is False
    assert snapshot.open_orders_complete is False
    assert snapshot.diagnostics["holdings_parse"] == {
        "raw_row_count": 4,
        "parsed_row_count": 2,
        "omitted_row_count": 1,
        "invalid_row_count": 1,
        "duplicate_identity_count": 1,
    }
    assert snapshot.diagnostics["open_orders_parse"] == {
        "raw_row_count": 5,
        "parsed_row_count": 2,
        "omitted_row_count": 1,
        "invalid_row_count": 2,
        "duplicate_identity_count": 1,
    }


def test_overseas_snapshot_keeps_known_zero_and_fully_filled_rows_complete() -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [{"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "0"}],
                    "output2": [{"frcr_dncl_amt_2": "5000"}],
                }
            ),
            _response(
                {
                    "output": [
                        {
                            "pdno": "AAPL",
                            "odno": "us-filled-1",
                            "sll_buy_dvsn_cd": "02",
                            "ft_ord_qty": "2",
                            "nccs_qty": "0",
                        }
                    ]
                }
            ),
        ]
    )
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    snapshot = adapter.get_account_snapshot("token", exchange="NASD")

    assert snapshot.holdings == []
    assert snapshot.open_orders == []
    assert snapshot.holdings_complete is True
    assert snapshot.open_orders_complete is True
    assert snapshot.diagnostics["holdings_parse"]["omitted_row_count"] == 1
    assert snapshot.diagnostics["open_orders_parse"]["omitted_row_count"] == 1


@pytest.mark.parametrize(
    ("runtime_mode", "expected_transaction_id"),
    [("paper", "VTTC0081R"), ("live", "TTTC0081R")],
)
def test_domestic_snapshot_uses_current_daily_order_transaction_id(
    runtime_mode: str,
    expected_transaction_id: str,
) -> None:
    http = QueueHttpClient(
        [
            _response(
                {
                    "output1": [],
                    "output2": [{"ord_psbl_cash": "1000000"}],
                }
            ),
            _response({"output1": []}),
        ]
    )
    adapter = KisBrokerAdapter(
        settings=_settings().model_copy(update={"runtime_mode": runtime_mode}),
        http_client=http,
    )

    adapter.get_account_snapshot("token", exchange="KRX")

    assert http.get_calls[1]["url"].endswith(KIS_OPEN_ORDERS_PATH)
    assert http.get_calls[1]["headers"]["tr_id"] == expected_transaction_id  # type: ignore[index]


@pytest.mark.parametrize(
    ("runtime_mode", "expected_transaction_id"),
    [("paper", "VTSC9215R"), ("live", "CTSC9215R")],
)
def test_domestic_reconcile_uses_before_period_transaction_for_old_orders(
    runtime_mode: str,
    expected_transaction_id: str,
) -> None:
    http = QueueHttpClient([_response({"output1": []})])
    adapter = KisBrokerAdapter(
        settings=_settings().model_copy(update={"runtime_mode": runtime_mode}),
        http_client=http,
    )

    adapter.reconcile_order(
        "token",
        "old-order",
        exchange="NXT",
        submitted_at=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert http.get_calls[0]["url"].endswith(KIS_DAILY_CCLD_PATH)
    assert http.get_calls[0]["headers"]["tr_id"] == expected_transaction_id  # type: ignore[index]
    assert http.get_calls[0]["params"]["EXCG_ID_DVSN_CD"] == "NXT"  # type: ignore[index]
