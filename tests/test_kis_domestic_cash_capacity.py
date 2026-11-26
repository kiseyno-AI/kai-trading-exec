from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from app.broker.base import BrokerAdapterError, BrokerErrorClassification
from app.broker import kis as kis_module
from app.broker.kis import JsonHttpResponse, KisBrokerAdapter
from app.core.config import Settings


class MockJsonHttpClient:
    def __init__(
        self, payload: dict[str, object], *, status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.requests: list[httpx.Request] = []

    def get_json(self, *, url, headers, params, timeout_seconds) -> JsonHttpResponse:
        def respond(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return httpx.Response(self.status_code, json=self.payload, headers=self.headers)

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            response = client.get(url, headers=headers, params=params, timeout=timeout_seconds)
        return JsonHttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            payload=response.json(),
        )

    def post_json(self, **_kwargs: object) -> JsonHttpResponse:
        raise AssertionError("cash-capacity inquiry must never POST")


def _settings(**overrides: object) -> Settings:
    return Settings(
        **{
            "runtime_mode": "paper",
            "runtime_broker_provider": "kis",
            "kis_app_key": "fixture-app-key",
            "kis_app_secret": "fixture-app-secret",
            "kis_account_number": "12345678",
            "kis_account_product_code": "01",
            "kis_use_hashkey": False,
            "database_url": "sqlite:///:memory:",
            **overrides,
        }
    )


def _payload() -> dict[str, object]:
    return {
        "rt_cd": "0",
        "output": {
            "ord_psbl_cash": "980000",
            "nrcvb_buy_amt": "970000",
            "nrcvb_buy_qty": "13",
            "psbl_qty_calc_unpr": "73400",
            "max_buy_amt": "999999999",
            "max_buy_qty": "999999",
        },
    }


@pytest.mark.parametrize(
    ("runtime_mode", "base_url", "transaction_id"),
    [
        ("paper", "https://openapivts.koreainvestment.com:29443", "VTTC8908R"),
        ("live", "https://openapi.koreainvestment.com:9443", "TTTC8908R"),
    ],
)
@pytest.mark.parametrize("symbol", ["069500", "0017J0"])
def test_cash_capacity_preserves_cash_only_fields_and_request_scope(
    runtime_mode: str, base_url: str, transaction_id: str, symbol: str,
) -> None:
    http = MockJsonHttpClient(_payload())
    adapter = KisBrokerAdapter(settings=_settings(runtime_mode=runtime_mode), http_client=http)
    before = datetime.now(UTC)

    capacity = adapter.get_domestic_cash_capacity(
        access_token="fixture-token",
        symbol=symbol,
        limit_price=Decimal("73400"),
    )

    request, = http.requests
    assert request.method == "GET"
    assert str(request.url).split("?", 1)[0] == (
        f"{base_url}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    )
    assert request.headers["tr_id"] == transaction_id
    assert request.headers["authorization"] == "Bearer fixture-token"
    assert dict(request.url.params) == {
        "CANO": "12345678",
        "ACNT_PRDT_CD": "01",
        "PDNO": symbol,
        "ORD_UNPR": "73400",
        "ORD_DVSN": "00",
        "CMA_EVLU_AMT_ICLD_YN": "N",
        "OVRS_ICLD_YN": "N",
    }
    assert capacity.provider == "kis"
    assert capacity.runtime_mode == runtime_mode
    assert capacity.account_reference == "KIS:12345678-01"
    assert (capacity.symbol, capacity.exchange, capacity.currency) == (symbol, "KRX", "KRW")
    assert capacity.requested_price == Decimal("73400")
    assert capacity.quantity_calculation_price == Decimal("73400")
    assert capacity.orderable_cash_krw == Decimal("980000")
    assert capacity.no_misu_buying_amount_krw == Decimal("970000")
    assert capacity.no_misu_buying_quantity == 13
    assert before <= capacity.received_at <= datetime.now(UTC)
    assert capacity.received_at.tzinfo == UTC
    assert capacity.source_order_type == "00"
    assert capacity.cma_included is False
    assert capacity.overseas_included is False
    with pytest.raises(FrozenInstanceError):
        capacity.orderable_cash_krw = Decimal("999999999")


@pytest.mark.parametrize(
    "request_overrides",
    [
        {"symbol": "69500"},
        {"symbol": "069500 "},
        {"symbol": "１２３４５６"},
        {"symbol": "0017j0"},
        {"symbol": " 0017J0"},
        {"symbol": "0017J0 "},
        {"symbol": "AAPLXX"},
        {"symbol": "0017J-"},
        {"symbol": None},
        {"exchange": "NXT"},
        {"exchange": "NYSE"},
        {"exchange": "krx"},
        {"limit_price": Decimal("0")},
        {"limit_price": Decimal("-5")},
        {"limit_price": Decimal("NaN")},
        {"limit_price": Decimal("sNaN")},
        {"limit_price": Decimal("Infinity")},
        {"limit_price": 73400},
        {"limit_price": "73400"},
        {"access_token": ""},
        {"access_token": "fixture\ntoken"},
    ],
)
def test_invalid_request_scope_fails_before_http(request_overrides: dict[str, object]) -> None:
    http = MockJsonHttpClient(_payload())
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError) as caught:
        adapter.get_domestic_cash_capacity(**{
            "access_token": "fixture-token",
            "symbol": "069500",
            "limit_price": Decimal("73400"),
            **request_overrides,
        })

    assert caught.value.classification == BrokerErrorClassification.VALIDATION
    assert http.requests == []


@pytest.mark.parametrize(
    "settings_overrides",
    [
        {"runtime_mode": "mock"},
        {"runtime_mode": "invalid-mode"},
        {"runtime_broker_provider": "alpaca"},
        {"kis_account_number": None},
        {"kis_account_number": "1234567"},
        {"kis_account_product_code": "1"},
        {"kis_app_key": None},
        {"kis_app_secret": None},
    ],
)
def test_unavailable_configured_scope_fails_before_http(
    settings_overrides: dict[str, object],
) -> None:
    http = MockJsonHttpClient(_payload())
    settings = _settings().model_copy(update=settings_overrides)
    adapter = KisBrokerAdapter(settings=settings, http_client=http)

    with pytest.raises(BrokerAdapterError):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )

    assert http.requests == []


@pytest.mark.parametrize(
    "field", ["ord_psbl_cash", "nrcvb_buy_amt", "nrcvb_buy_qty", "psbl_qty_calc_unpr"],
)
@pytest.mark.parametrize("value", [None, "", "bad", "NaN", "Infinity", "-1", True, "1,000"])
def test_invalid_response_amounts_fail_closed(field: str, value: object) -> None:
    payload = _payload()
    payload["output"][field] = value
    http = MockJsonHttpClient(payload)
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError, match="cash capacity"):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )


@pytest.mark.parametrize(
    "field", ["ord_psbl_cash", "nrcvb_buy_amt", "nrcvb_buy_qty", "psbl_qty_calc_unpr"],
)
def test_missing_response_field_never_uses_maximum_or_asset_fallback(field: str) -> None:
    payload = _payload()
    del payload["output"][field]
    payload["output"]["tot_evlu_amt"] = "999999999"
    payload["output"]["ord_psbl_sbst"] = "999999999"
    http = MockJsonHttpClient(payload)
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )


@pytest.mark.parametrize("output", [None, [], {}, "bad"])
def test_non_object_or_empty_output_fails_closed(output: object) -> None:
    http = MockJsonHttpClient({"rt_cd": "0", "output": output})
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )


@pytest.mark.parametrize(
    "overrides", [{"nrcvb_buy_qty": "1.5"}, {"psbl_qty_calc_unpr": "0"}],
)
def test_positive_capacity_requires_whole_quantity_and_positive_calculation_price(
    overrides: dict[str, str],
) -> None:
    payload = _payload()
    payload["output"].update(overrides)
    http = MockJsonHttpClient(payload)
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )


def test_zero_capacity_is_valid_without_using_large_maximum_values() -> None:
    payload = _payload()
    payload["output"].update({
        "ord_psbl_cash": "0", "nrcvb_buy_amt": "0", "nrcvb_buy_qty": "0",
        "psbl_qty_calc_unpr": "0",
    })
    http = MockJsonHttpClient(payload)
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    capacity = adapter.get_domestic_cash_capacity(
        access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
    )

    assert capacity.orderable_cash_krw == capacity.no_misu_buying_amount_krw == Decimal("0")
    assert capacity.no_misu_buying_quantity == 0
    assert capacity.quantity_calculation_price == Decimal("0")


@pytest.mark.parametrize("status_code, rt_cd", [(200, "1"), (503, "0"), (200, None), (200, 0)])
def test_provider_failure_or_unproven_business_success_fails_closed(
    status_code: int, rt_cd: object,
) -> None:
    payload = {**_payload(), "msg_cd": "APBK1234", "msg1": "fixture rejection"}
    if rt_cd is None:
        del payload["rt_cd"]
    else:
        payload["rt_cd"] = rt_cd
    http = MockJsonHttpClient(payload, status_code=status_code)
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )


def test_capacity_id_binds_scope_raw_fields_and_utc_receipt_without_credentials(monkeypatch) -> None:
    class Clock(datetime):
        current = datetime(2026, 11, 27, 0, 1, 2, 345678, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz) if tz is not None else cls.current.replace(tzinfo=None)

    monkeypatch.setattr(kis_module, "datetime", Clock)
    http = MockJsonHttpClient(_payload())

    def read(*, settings=None, **request):
        adapter = KisBrokerAdapter(settings=settings or _settings(), http_client=http)
        return adapter.get_domestic_cash_capacity(**{
            "access_token": "fixture-token", "symbol": "069500",
            "limit_price": Decimal("73400"), **request,
        })

    baseline = read()
    assert baseline.received_at == Clock.current
    assert len(baseline.capacity_id) == 64
    assert set(baseline.capacity_id) <= set("0123456789abcdef")
    assert read().capacity_id == baseline.capacity_id
    assert read(access_token="rotated-fixture-token").capacity_id == baseline.capacity_id
    http.payload["output"]["max_buy_qty"] = "1"
    assert read().capacity_id == baseline.capacity_id
    assert "fixture-token" not in repr(baseline)
    assert "fixture-app-key" not in repr(baseline)
    assert "fixture-app-secret" not in repr(baseline)

    assert read(symbol="091160").capacity_id != baseline.capacity_id
    assert read(limit_price=Decimal("73405")).capacity_id != baseline.capacity_id
    for overrides in (
        {"runtime_mode": "live"},
        {"kis_account_number": "87654321"},
        {"kis_account_product_code": "02"},
    ):
        assert read(settings=_settings(**overrides)).capacity_id != baseline.capacity_id
    for field, value in (
        ("ord_psbl_cash", "980001"),
        ("nrcvb_buy_amt", "970001"),
        ("nrcvb_buy_qty", "14"),
        ("psbl_qty_calc_unpr", "73405"),
    ):
        original = http.payload["output"][field]
        http.payload["output"][field] = value
        assert read().capacity_id != baseline.capacity_id
        http.payload["output"][field] = original

    Clock.current += timedelta(microseconds=1)
    assert read().capacity_id != baseline.capacity_id


@pytest.mark.parametrize("tr_cont", ["F", "M"])
def test_unexpected_continuation_cannot_authorize_capacity(tr_cont: str) -> None:
    http = MockJsonHttpClient(_payload(), headers={"tr_cont": tr_cont})
    adapter = KisBrokerAdapter(settings=_settings(), http_client=http)

    with pytest.raises(BrokerAdapterError, match="continuation"):
        adapter.get_domestic_cash_capacity(
            access_token="fixture-token", symbol="069500", limit_price=Decimal("73400"),
        )
