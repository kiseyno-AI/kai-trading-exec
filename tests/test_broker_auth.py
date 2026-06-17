from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import time

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis import (
    KIS_LIVE_API_BASE_URL,
    KIS_PAPER_API_BASE_URL,
    KIS_WEBSOCKET_APPROVAL_PATH,
    JsonHttpResponse,
    KisBrokerAdapter,
)
from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    BrokerSession,
    BrokerSessionEvent,
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.monitoring import list_autonomous_monitoring_symbols, run_autonomous_monitoring_cycle
from app.services.watch_exec_agent_sizing import _apply_decisions


class RecordingHttpClient:
    def __init__(
        self,
        response: dict[str, object],
        *,
        get_response: dict[str, object] | None = None,
    ) -> None:
        self.response = response
        self.get_response = response if get_response is None else get_response
        self.calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []

    def get_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        params: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        self.get_calls.append(
            {
                "url": url,
                "headers": headers,
                "params": params,
                "timeout_seconds": timeout_seconds,
            }
        )
        return JsonHttpResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            payload=self.get_response,
        )

    def post_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        payload: dict[str, object],
        timeout_seconds: float,
    ) -> JsonHttpResponse:
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        return JsonHttpResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            payload=self.response,
        )


class StubBrokerAdapter:
    provider = "kis"

    def __init__(
        self,
        *,
        config: BrokerConfig,
        token_grant: BrokerTokenGrant | None = None,
        approval: BrokerRealtimeApproval | None = None,
        order_placement: BrokerOrderPlacement | None = None,
        account_snapshot: BrokerAccountSnapshot | None = None,
        current_prices: dict[str, Decimal] | None = None,
        auth_error: Exception | None = None,
        approval_error: Exception | None = None,
        order_error: Exception | None = None,
        account_snapshot_error: Exception | None = None,
        current_price_error: Exception | None = None,
        revoke_error: Exception | None = None,
    ) -> None:
        self.config = config
        self.token_grant = token_grant
        self.approval = approval or BrokerRealtimeApproval(
            approval_key="approval-key",
            issued_at=datetime.now(UTC),
        )
        self.order_placement = order_placement
        self.account_snapshot = account_snapshot
        self.current_prices = current_prices or {}
        self.auth_error = auth_error
        self.approval_error = approval_error
        self.order_error = order_error
        self.account_snapshot_error = account_snapshot_error
        self.current_price_error = current_price_error
        self.revoke_error = revoke_error
        self.authenticate_calls = 0
        self.approval_calls = 0
        self.revoke_calls: list[str] = []
        self.order_calls: list[BrokerOrderPlacementRequest] = []
        self.current_price_calls: list[tuple[str, str]] = []

    def describe(self) -> BrokerConfig:
        return self.config

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        if self.auth_error is not None:
            raise self.auth_error
        assert self.token_grant is not None
        return self.token_grant

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        self.approval_calls += 1
        if self.approval_error is not None:
            raise self.approval_error
        assert access_token
        return self.approval

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        self.revoke_calls.append(access_token)
        if self.revoke_error is not None:
            raise self.revoke_error
        return BrokerTokenRevocation(message="token revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        self.order_calls.append(request)
        if self.order_error is not None:
            raise self.order_error
        assert access_token
        assert self.order_placement is not None
        return self.order_placement

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        self.current_price_calls.append((symbol, exchange))
        if self.current_price_error is not None:
            raise self.current_price_error
        assert access_token
        price = self.current_prices.get(symbol)
        if price is None:
            return None
        return BrokerCurrentPrice(
            symbol=symbol,
            exchange=exchange,
            price=price,
            observed_at=datetime.now(UTC),
            payload={"output": {"stck_prpr": str(price)}},
        )

    def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
        assert access_token
        if self.account_snapshot_error is not None:
            raise self.account_snapshot_error
        if self.account_snapshot is not None:
            return self.account_snapshot
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime.now(UTC),
            source="stub:account_snapshot",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(
            supported=True,
            strategy="shared_realtime_subscription",
        )

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
    ) -> dict[str, object]:
        return {
            "provider": self.provider,
            "access_token_present": bool(access_token),
            "broker_order_id": broker_order_id,
            "status": "not_implemented",
        }

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        headers = {"authorization": f"Bearer {access_token}"}
        if transaction_id is not None:
            headers["tr_id"] = transaction_id
        if hashkey is not None:
            headers["hashkey"] = hashkey
        return headers


class StaticMonitoringPriceSource:
    def __init__(self, prices_by_symbol: dict[str, Decimal]) -> None:
        self.prices_by_symbol = prices_by_symbol
        self.calls: list[list[str]] = []

    def get_prices(
        self,
        *,
        symbols: list[str],
        observed_at: datetime,
        settings: Settings,
    ) -> list[dict[str, str]]:
        del observed_at, settings
        self.calls.append(list(symbols))
        return [
            {"symbol": symbol, "price": str(self.prices_by_symbol[symbol])}
            for symbol in symbols
            if symbol in self.prices_by_symbol
        ]


def build_test_app(
    *,
    settings: Settings,
    broker_adapter: StubBrokerAdapter | None = None,
    monitoring_price_source: object | None = None,
    autonomous_runtime_enabled: bool = False,
):
    effective_settings = settings.model_copy(
        update={"autonomous_runtime_enabled": autonomous_runtime_enabled}
    )
    engine = create_db_engine(effective_settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=effective_settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=(
            (lambda _settings: broker_adapter)
            if broker_adapter is not None
            else None
        ),
        monitoring_price_source=monitoring_price_source,
    )
    return app, engine


def wait_for(predicate, *, timeout_seconds: float = 1.5, interval_seconds: float = 0.02) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval_seconds)
    assert predicate()


def build_token_grant() -> BrokerTokenGrant:
    return BrokerTokenGrant(
        access_token="top-secret-token",
        token_type="Bearer",
        access_token_expires_at=_future_token_expiry(),
    )


def _future_token_expiry() -> datetime:
    return datetime.now(UTC) + timedelta(days=7)


def build_order_placement(
    *,
    broker_order_id: str = "kis-order-100",
) -> BrokerOrderPlacement:
    observed_at = datetime(2026, 4, 30, 9, 0, tzinfo=UTC)
    return BrokerOrderPlacement(
        broker_order_id=broker_order_id,
        normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
        broker_status="0",
        submitted_at=observed_at,
        observed_at=observed_at,
        request_metadata={
            "provider": "kis",
            "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
            "tr_id": "VTTC0012U",
        },
        response_metadata={
            "status_code": 200,
            "msg_cd": "0",
            "msg1": "accepted",
        },
        request_payload={
            "CANO": "12345678",
            "ACNT_PRDT_CD": "01",
            "PDNO": "005930",
            "ORD_DVSN": "00",
            "ORD_QTY": "5",
            "ORD_UNPR": "71000.00",
            "EXCG_ID_DVSN_CD": "KRX",
            "SLL_TYPE": "",
            "CNDT_PRIC": "",
        },
        response_payload={
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {
                "ODNO": broker_order_id,
                "KRX_FWDG_ORD_ORGNO": "00000",
                "ORD_TMD": "090000",
            },
        },
        message="accepted",
    )


def test_settings_load_kis_environment_fields() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_RUNTIME_MODE": "paper",
            "KAI_EXEC_KIS_APP_KEY": "paper-app-key",
            "KAI_EXEC_KIS_APP_SECRET": "paper-app-secret",
            "KAI_EXEC_KIS_ACCOUNT_NUMBER": "12345678",
            "KAI_EXEC_KIS_ACCOUNT_PRODUCT_CODE": "01",
            "KAI_EXEC_KIS_HTS_ID": "ops-user",
        },
    )

    assert settings.runtime_mode == "paper"
    assert settings.kis_app_key == "paper-app-key"
    assert settings.kis_app_secret == "paper-app-secret"
    assert settings.kis_account_number == "12345678"
    assert settings.kis_account_product_code == "01"
    assert settings.kis_hts_id == "ops-user"


def test_settings_load_profit_protection_environment_fields() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_PROFIT_PROTECTION_MIN_PEAK_PCT": "1.5",
            "KAI_EXEC_PROFIT_PROTECTION_GIVEBACK_PCT": "0.7",
            "KAI_EXEC_PROFIT_PROTECTION_AGED_SESSION_MIN": "3",
            "KAI_EXEC_PROFIT_PROTECTION_AGED_MIN_CURRENT_PROFIT_PCT": "1.1",
            "KAI_EXEC_BREAKEVEN_STOP_BUFFER_PCT": "0.3",
            "KAI_EXEC_WATCH_BUY_HIGH_RISK_GATE_ENABLED": "true",
            "KAI_EXEC_WATCH_BUY_HIGH_ATR_PCT_THRESHOLD": "8.5",
            "KAI_EXEC_WATCH_BUY_WEAK_RET20_THRESHOLD_PCT": "-1.0",
            "KAI_EXEC_WATCH_BUY_DEEP_NEGATIVE_RET20_THRESHOLD_PCT": "-12.0",
            "KAI_EXEC_WATCH_BUY_EXTENDED_RET5_THRESHOLD_PCT": "14.0",
            "KAI_EXEC_WATCH_BUY_MAX_INTRADAY_RANGE_POSITION_PCT": "70.0",
            "KAI_EXEC_EXEC_AGENT_WATCH_SIZING_MIN_NOTIONAL_KRW": "1200000",
            "KAI_EXEC_EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_MIN_NOTIONAL_KRW": "1800000",
            "KAI_EXEC_EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_THRESHOLD": "0.7",
            "KAI_EXEC_EXEC_AGENT_WATCH_SIZING_FALLBACK_ACCOUNT_BASIS_KRW": "11000000",
            "KAI_EXEC_TRADE_OUTCOME_FEEDBACK_ENABLED": "true",
            "KAI_EXEC_TRADE_OUTCOME_FEEDBACK_DIR": "var/test-feedback",
            "KAI_EXEC_TRADE_OUTCOME_FEEDBACK_LOOKBACK_DAYS": "30",
            "KAI_EXEC_TRADE_OUTCOME_FEEDBACK_MAX_LESSONS": "4",
        },
    )

    assert settings.profit_protection_min_peak_pct == Decimal("1.5")
    assert settings.profit_protection_giveback_pct == Decimal("0.7")
    assert settings.profit_protection_aged_session_min == 3
    assert settings.profit_protection_aged_min_current_profit_pct == Decimal("1.1")
    assert settings.breakeven_stop_buffer_pct == Decimal("0.3")
    assert settings.watch_buy_high_risk_gate_enabled is True
    assert settings.watch_buy_high_atr_pct_threshold == Decimal("8.5")
    assert settings.watch_buy_weak_ret20_threshold_pct == Decimal("-1.0")
    assert settings.watch_buy_deep_negative_ret20_threshold_pct == Decimal("-12.0")
    assert settings.watch_buy_extended_ret5_threshold_pct == Decimal("14.0")
    assert settings.watch_buy_max_intraday_range_position_pct == Decimal("70.0")
    assert settings.exec_agent_watch_sizing_min_notional_krw == Decimal("1200000")
    assert settings.exec_agent_watch_sizing_high_conviction_min_notional_krw == Decimal("1800000")
    assert settings.exec_agent_watch_sizing_high_conviction_threshold == Decimal("0.7")
    assert settings.exec_agent_watch_sizing_fallback_account_basis_krw == Decimal("11000000")
    assert settings.trade_outcome_feedback_enabled is True
    assert settings.trade_outcome_feedback_dir == "var/test-feedback"
    assert settings.trade_outcome_feedback_lookback_days == 30
    assert settings.trade_outcome_feedback_max_lessons == 4


def test_kis_adapter_uses_client_credentials_token_flow() -> None:
    http_client = RecordingHttpClient(
        {
            "access_token": "access-token-1",
            "token_type": "Bearer",
            "expires_in": 7776000,
            "access_token_token_expired": "2026-05-30 08:10:10",
        }
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        http_client=http_client,
    )

    grant = adapter.authenticate()

    assert http_client.calls[0]["url"] == f"{KIS_PAPER_API_BASE_URL}/oauth2/tokenP"
    assert http_client.calls[0]["payload"] == {
        "grant_type": "client_credentials",
        "appkey": "paper-key",
        "appsecret": "paper-secret",
    }
    assert grant.access_token == "access-token-1"
    assert grant.token_type == "Bearer"
    assert grant.access_token_expires_at.isoformat() == "2026-05-30T08:10:10+09:00"


def test_kis_adapter_fetches_current_price_quote() -> None:
    http_client = RecordingHttpClient(
        {},
        get_response={
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "success",
            "output": {
                "stck_prpr": "125100",
            },
        },
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        http_client=http_client,
    )

    quote = adapter.get_current_price("access-token", symbol="005930")

    assert http_client.get_calls[0]["url"] == (
        f"{KIS_PAPER_API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    )
    assert http_client.get_calls[0]["headers"]["authorization"] == "Bearer access-token"
    assert http_client.get_calls[0]["headers"]["tr_id"] == "FHKST01010100"
    assert http_client.get_calls[0]["params"] == {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": "005930",
    }
    assert quote is not None
    assert quote.symbol == "005930"
    assert quote.exchange == "KRX"
    assert quote.price == Decimal("125100")


def test_kis_adapter_fetches_live_market_context_from_orderbook_quote() -> None:
    http_client = RecordingHttpClient(
        {},
        get_response={
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "success",
            "output": {
                "bidp1": "1582",
                "askp1": "1584",
                "bidp_rsqn1": "1000",
                "bidp_rsqn2": "500",
                "askp_rsqn1": "700",
                "askp_rsqn2": "300",
            },
        },
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        http_client=http_client,
    )

    context = adapter.get_live_market_context(
        "access-token",
        symbol="073560",
        reference_price=Decimal("1583"),
    )

    assert http_client.get_calls[0]["url"] == (
        f"{KIS_PAPER_API_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
    )
    assert http_client.get_calls[0]["headers"]["authorization"] == "Bearer access-token"
    assert http_client.get_calls[0]["headers"]["tr_id"] == "FHKST01010200"
    assert http_client.get_calls[0]["params"] == {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": "073560",
    }
    assert context is not None
    assert context.symbol == "073560"
    assert context.bid == Decimal("1582")
    assert context.ask == Decimal("1584")
    assert context.spread == Decimal("2")
    assert context.orderbook_depth == Decimal("2500")
    assert context.orderbook_imbalance == Decimal("0.2")
    assert context.payload["endpoint"] == "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"


def test_kis_adapter_sends_nxt_exchange_and_routed_order_division() -> None:
    http_client = RecordingHttpClient(
        {
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "accepted",
            "output": {
                "ODNO": "nxt-order-1",
                "ORD_TMD": "080001",
            },
        }
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            kis_use_hashkey=False,
        ),
        http_client=http_client,
    )

    placement = adapter.place_order(
        "access-token",
        BrokerOrderPlacementRequest(
            idempotency_key="nxt-pre-sell",
            symbol="033530",
            side="sell",
            quantity=1,
            order_type="limit",
            limit_price=Decimal("7300"),
            time_in_force="day",
            exchange="NXT",
            order_division="00",
            route_session="nxt_pre",
            route_reason="nxt_pre_session",
        ),
    )

    order_call = http_client.calls[-1]
    assert order_call["payload"]["EXCG_ID_DVSN_CD"] == "NXT"
    assert order_call["payload"]["ORD_DVSN"] == "00"
    assert placement.request_metadata["route"]["exchange"] == "NXT"
    assert placement.request_metadata["route"]["session"] == "nxt_pre"


def test_kis_session_time_rejection_is_retryable() -> None:
    http_client = RecordingHttpClient(
        {
            "rt_cd": "1",
            "msg_cd": "KIOK0320",
            "msg1": "장운영시간이 아닙니다",
        }
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            kis_use_hashkey=False,
        ),
        http_client=http_client,
    )

    with pytest.raises(BrokerAdapterError) as exc_info:
        adapter.place_order(
            "access-token",
            BrokerOrderPlacementRequest(
                idempotency_key="session-retry",
                symbol="033530",
                side="sell",
                quantity=1,
                order_type="market",
                time_in_force="day",
                exchange="KRX",
            ),
        )

    assert exc_info.value.code == "KIOK0320"
    assert exc_info.value.retryable is True
    assert exc_info.value.normalized_status == NormalizedBrokerOrderStatus.RETRY_PENDING


def test_kis_adapter_fetches_account_snapshot() -> None:
    http_client = RecordingHttpClient(
        {},
        get_response={
            "rt_cd": "0",
            "msg_cd": "0",
            "msg1": "success",
            "output1": [
                {
                    "pdno": "005930",
                    "hldg_qty": "3",
                    "pchs_avg_pric": "70000",
                    "ord_psbl_qty": "3",
                }
            ],
            "output2": [{"ord_psbl_cash": "1000000"}],
        },
    )
    adapter = KisBrokerAdapter(
        settings=Settings(
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        http_client=http_client,
    )

    snapshot = adapter.get_account_snapshot("access-token")

    assert [call["url"] for call in http_client.get_calls] == [
        f"{KIS_PAPER_API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance",
        f"{KIS_PAPER_API_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
    ]
    assert http_client.get_calls[0]["headers"]["tr_id"] == "VTTC8434R"
    assert http_client.get_calls[1]["headers"]["tr_id"] == "VTTC8001R"
    assert snapshot.available_cash == Decimal("1000000")
    assert snapshot.holdings[0].symbol == "005930"
    assert snapshot.holdings[0].quantity == 3


def test_runtime_status_reports_disabled_broker_summary_in_mock_mode() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="mock",
        )
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["broker"]["provider"] == "kis"
    assert payload["broker"]["configured"] is False
    assert payload["broker"]["status"] == "disabled"
    assert payload["broker"]["access_token_present"] is False
    assert payload["broker"]["shared_realtime_subscriptions_supported"] is True
    assert payload["broker"]["reconnect_supported"] is True
    assert payload["broker"]["websocket_approval_path"] == KIS_WEBSOCKET_APPROVAL_PATH

    engine.dispose()


def test_runtime_status_reports_uninitialized_kis_session_when_credentials_exist() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        )
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["broker"]["configured"] is True
    assert payload["broker"]["status"] == "uninitialized"
    assert payload["broker"]["api_base_url"] == KIS_PAPER_API_BASE_URL
    assert payload["broker"]["access_token_present"] is False
    assert payload["broker"]["approval_key_present"] is False
    assert payload["broker"]["bounded_rest_reconciliation_supported"] is True

    engine.dispose()


def test_broker_authenticate_endpoint_persists_session_and_audit() -> None:
    token_grant = BrokerTokenGrant(
        access_token="top-secret-token",
        token_type="Bearer",
        access_token_expires_at=_future_token_expiry(),
    )
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=token_grant,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    response = client.post(
        "/broker/session/authenticate",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "start paper execution session",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "authenticate"
    assert payload["outcome"] == "applied"
    assert payload["changed"] is True
    assert payload["session"]["status"] == "active"
    assert payload["session"]["access_token_present"] is True
    assert "access_token" not in payload["session"]

    with app.state.db_session_factory() as session:
        broker_session = session.get(BrokerSession, "kis:paper")
        assert broker_session is not None
        assert broker_session.access_token == "top-secret-token"

        events = session.query(BrokerSessionEvent).all()
        assert len(events) == 1
        assert events[0].action == "authenticate"
        assert events[0].outcome == "applied"
        assert events[0].current_state["access_token_present"] is True
        assert "access_token" not in events[0].current_state

    engine.dispose()


def test_broker_authenticate_endpoint_reuses_active_session_without_force() -> None:
    token_grant = BrokerTokenGrant(
        access_token="stable-token",
        token_type="Bearer",
        access_token_expires_at=_future_token_expiry(),
    )
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=token_grant,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    first = client.post("/broker/session/authenticate", json={"actor": "ops"})
    second = client.post("/broker/session/authenticate", json={"actor": "ops"})

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["outcome"] == "noop"
    assert broker_adapter.authenticate_calls == 1

    engine.dispose()


def test_broker_revoke_endpoint_clears_session_and_records_event() -> None:
    token_grant = BrokerTokenGrant(
        access_token="revocable-token",
        token_type="Bearer",
        access_token_expires_at=_future_token_expiry(),
    )
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=token_grant,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post("/broker/session/authenticate", json={"actor": "ops"})
    response = client.post("/broker/session/revoke", json={"actor": "ops"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "revoke"
    assert payload["outcome"] == "applied"
    assert payload["session"]["status"] == "revoked"
    assert broker_adapter.revoke_calls == ["revocable-token"]

    with app.state.db_session_factory() as session:
        broker_session = session.get(BrokerSession, "kis:paper")
        assert broker_session is not None
        assert broker_session.access_token is None

        events = session.query(BrokerSessionEvent).order_by(BrokerSessionEvent.occurred_at).all()
        assert [event.action for event in events] == ["authenticate", "revoke"]

    engine.dispose()


def test_broker_authenticate_endpoint_surfaces_adapter_failure() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        auth_error=BrokerAdapterError("broker rejected credentials"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    response = client.post("/broker/session/authenticate", json={"actor": "ops"})

    assert response.status_code == 502
    payload = response.json()
    assert payload["outcome"] == "rejected"
    assert payload["session"]["status"] == "error"
    assert payload["session"]["last_error"] == "broker rejected credentials"

    with app.state.db_session_factory() as session:
        events = session.query(BrokerSessionEvent).all()
        assert len(events) == 1
        assert events[0].outcome == "rejected"
        assert "access_token" not in events[0].current_state

    engine.dispose()


def test_restart_reuses_persisted_broker_session_for_due_job_dispatch(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'restart-dispatch-session.db'}"
    settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    initial_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=settings,
        broker_adapter=initial_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    authenticate = client.post(
        "/broker/session/authenticate",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "persist reusable session before restart",
        },
    )
    assert authenticate.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-restart-dispatch-session",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    client.close()
    engine.dispose()

    restarted_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-order-101"),
    )
    restarted_app, restarted_engine = build_test_app(
        settings=settings,
        broker_adapter=restarted_adapter,
    )
    restarted_client = TestClient(restarted_app)

    runtime_status = restarted_client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_payload = runtime_status.json()
    assert runtime_payload["safety"]["paused"] is False
    assert runtime_payload["counts"]["active_execution_jobs"] == 1
    assert runtime_payload["broker"]["status"] == "active"
    assert runtime_payload["broker"]["access_token_present"] is True

    dispatch_response = restarted_client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["due_job_count"] == 1
    assert dispatch_payload["completed_job_count"] == 1
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 0
    assert dispatch_payload["jobs"][0]["execution_plan_item_id"] == plan_item_id
    assert dispatch_payload["jobs"][0]["status"] == "completed"

    assert restarted_adapter.authenticate_calls == 0
    assert len(restarted_adapter.order_calls) == 1
    assert restarted_adapter.order_calls[0].execution_plan_item_id == plan_item_id

    with restarted_app.state.db_session_factory() as session:
        broker_session = session.get(BrokerSession, "kis:paper")
        assert broker_session is not None
        assert broker_session.access_token == "top-secret-token"

        events = session.query(BrokerSessionEvent).order_by(BrokerSessionEvent.occurred_at).all()
        assert [(event.action, event.outcome) for event in events] == [
            ("authenticate", "applied"),
        ]

        job = session.scalar(
            select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == plan_item_id)
        )
        assert job is not None
        assert job.status == "completed"
        assert job.attempts == 1

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "submitted"

    restarted_client.close()
    restarted_engine.dispose()


def test_restart_recovers_stale_running_dispatch_job_after_interrupted_outage(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'restart-recover-running-job.db'}"
    settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    app, engine = build_test_app(
        settings=settings,
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url=KIS_PAPER_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(),
        ),
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-restart-recover-running-job",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        job.status = "running"
        job.locked_at = datetime.now(UTC) - timedelta(minutes=10)
        job.attempts = 1
        job.last_error = None

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        plan_item.status = "next_open_buy_dispatching"
        session.commit()

    client.close()
    engine.dispose()

    restarted_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-order-102"),
    )
    restarted_app, restarted_engine = build_test_app(
        settings=settings,
        broker_adapter=restarted_adapter,
    )
    restarted_client = TestClient(restarted_app)

    dispatch_response = restarted_client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["due_job_count"] == 1
    assert dispatch_payload["completed_job_count"] == 1
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 0
    assert dispatch_payload["jobs"][0]["execution_plan_item_id"] == plan_item_id
    assert dispatch_payload["jobs"][0]["status"] == "completed"

    assert restarted_adapter.authenticate_calls == 1
    assert len(restarted_adapter.order_calls) == 1

    with restarted_app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        assert job.status == "completed"
        assert job.attempts == 2
        assert job.locked_at is None
        assert job.last_error is None

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "submitted"

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

    restarted_client.close()
    restarted_engine.dispose()


def test_restart_preserves_reconciliation_positions_for_monitoring_evaluation(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'restart-monitoring-reconciliation.db'}"
    settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    app, engine = build_test_app(
        settings=settings,
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url=KIS_PAPER_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(broker_order_id="kis-sell-201"),
        ),
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-restart-monitoring-reconciliation",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "percent", "fraction": 0.5},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 4, 29, 9, 4, tzinfo=UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    client.close()
    engine.dispose()

    restarted_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-sell-202"),
    )
    restarted_app, restarted_engine = build_test_app(
        settings=settings,
        broker_adapter=restarted_adapter,
    )
    restarted_client = TestClient(restarted_app)

    positions_response = restarted_client.get("/positions")
    assert positions_response.status_code == 200
    positions_payload = positions_response.json()
    assert len(positions_payload) == 1
    assert positions_payload[0]["symbol"] == "005930"
    assert positions_payload[0]["updated_from"] == "reconciliation"
    assert positions_payload[0]["last_reconciled_at"] == "2026-04-29T09:04:00"

    runtime_before = restarted_client.get("/runtime/status")
    assert runtime_before.status_code == 200
    assert runtime_before.json()["liveness"]["monitoring"]["status"] == "not_started"

    evaluation_response = restarted_client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "resume target/stop monitoring after restart",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                }
            ],
        },
    )

    assert evaluation_response.status_code == 200
    evaluation_payload = evaluation_response.json()
    assert evaluation_payload["triggered_item_count"] == 1
    assert evaluation_payload["submitted_order_count"] == 1
    assert evaluation_payload["blocked_item_count"] == 0
    assert evaluation_payload["items"][0]["execution_plan_item_id"] == plan_item_id
    assert evaluation_payload["items"][0]["outcome"] == "submitted"

    runtime_after = restarted_client.get("/runtime/status")
    assert runtime_after.status_code == 200
    assert runtime_after.json()["liveness"]["monitoring"]["status"] == "running"

    assert restarted_adapter.authenticate_calls == 1
    assert len(restarted_adapter.order_calls) == 1

    with restarted_app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "submitted"

        checkpoint = session.scalar(
            select(MonitoringCheckpoint).where(
                MonitoringCheckpoint.worker_name == "intraday_target_stop_monitor"
            )
        )
        assert checkpoint is not None
        assert checkpoint.status == "running"
        assert checkpoint.last_success_at is not None

    restarted_client.close()
    restarted_engine.dispose()


def test_place_order_endpoint_persists_normalized_order_and_events() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-broker-order-1",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "order-1",
            "execution_plan_item_id": plan_item_id,
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["duplicate"] is False
    assert payload["order"]["broker_name"] == "kis"
    assert payload["order"]["status"] == "submitted"
    assert payload["order"]["broker_order_id"] == "kis-order-100"
    assert payload["order"]["last_error_classification"] is None
    assert payload["order"]["request_metadata"]["endpoint"] == "/uapi/domestic-stock/v1/trading/order-cash"
    assert payload["order"]["response_metadata"]["msg_cd"] == "0"
    assert [event["event_type"] for event in payload["order"]["events"]] == [
        "submit_requested",
        "submit_acknowledged",
    ]

    with app.state.db_session_factory() as session:
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.idempotency_key == "order-1")
        )
        assert order is not None
        assert order.status == "submitted"
        assert order.broker_order_id == "kis-order-100"
        assert order.request_metadata["tr_id"] == "VTTC0012U"

        events = session.scalars(
            select(BrokerOrderEvent)
            .where(BrokerOrderEvent.broker_order_pk == order.id)
            .order_by(BrokerOrderEvent.observed_at.asc())
        ).all()
        assert [event.event_type for event in events] == [
            "submit_requested",
            "submit_acknowledged",
        ]

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

    engine.dispose()


def test_place_order_endpoint_routes_nxt_pre_when_enabled() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-nxt-pre-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_order_route_preference="nxt",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "nxt-pre-order-1",
            "symbol": "033530",
            "side": "sell",
            "quantity": 1,
            "order_type": "limit",
            "limit_price": "7300.00",
            "time_in_force": "day",
            "exchange": "KRX",
            "metadata": {
                "order_purpose": "protective_stop",
                "nxt_eligible": True,
                "route_now": "2026-05-28T08:00:00+09:00",
            },
        },
    )

    assert response.status_code == 201
    assert broker_adapter.order_calls[0].exchange == "NXT"
    assert broker_adapter.order_calls[0].order_division == "00"
    assert broker_adapter.order_calls[0].route_session == "nxt_pre"
    assert response.json()["order"]["request_metadata"]["route"]["exchange"] == "NXT"

    engine.dispose()


def test_place_order_endpoint_defers_nxt_pre_market_order_without_limit() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-should-not-submit"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_order_route_preference="nxt",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "nxt-pre-market-no-limit-1",
            "symbol": "033530",
            "side": "sell",
            "quantity": 1,
            "order_type": "market",
            "time_in_force": "day",
            "exchange": "KRX",
            "metadata": {
                "order_purpose": "protective_stop",
                "nxt_eligible": True,
                "route_now": "2026-05-28T08:00:00+09:00",
            },
        },
    )

    assert response.status_code == 202
    assert response.json()["deferred"] is True
    assert response.json()["message"] == "nxt_pre_session:limit_required"
    assert response.json()["route"]["ord_dvsn"] is None
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        assert session.scalars(select(BrokerOrder)).all() == []

    engine.dispose()


def test_place_order_endpoint_defers_non_nxt_protective_sell_at_0800() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-should-not-submit"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            domestic_order_session_routing_enabled=True,
            allow_nxt_pre_orders=True,
            nxt_eligible_symbols="005930",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "defer-non-nxt-1",
            "symbol": "033530",
            "side": "sell",
            "quantity": 1,
            "order_type": "limit",
            "limit_price": "7300.00",
            "time_in_force": "day",
            "exchange": "KRX",
            "metadata": {
                "order_purpose": "protective_stop",
                "route_now": "2026-05-28T08:00:00+09:00",
            },
        },
    )

    assert response.status_code == 202
    assert response.json()["deferred"] is True
    assert response.json()["route"]["next_retry_at"] is not None
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        assert session.scalars(select(BrokerOrder)).all() == []

    engine.dispose()


def test_dispatch_due_next_open_buy_job_places_broker_order() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-next-open-dispatch-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    dispatch_response = client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["blocked"] is False
    assert dispatch_payload["due_job_count"] == 1
    assert dispatch_payload["completed_job_count"] == 1
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 0
    assert len(dispatch_payload["jobs"]) == 1
    assert dispatch_payload["jobs"][0]["execution_plan_item_id"] == plan_item_id
    assert dispatch_payload["jobs"][0]["status"] == "completed"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == plan_item_id
    assert broker_adapter.order_calls[0].quantity == 5
    assert broker_adapter.order_calls[0].order_type == "limit"
    assert broker_adapter.order_calls[0].time_in_force == "day"

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        assert job.status == "completed"
        assert job.attempts == 1

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "submitted"
        assert order.idempotency_key == f"execution-job:{job.id}"

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

    engine.dispose()


def test_dispatch_due_next_open_buy_job_keeps_retrying_duplicate_retryable_order() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "rate limited by broker",
            status_code=503,
            classification=BrokerErrorClassification.RATE_LIMIT,
            retryable=True,
            code="RATE_LIMIT",
            normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            response_payload={"msg_cd": "RATE_LIMIT", "msg1": "rate limited by broker"},
            observed_at=datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-next-open-dispatch-duplicate-retry",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    first_dispatch = client.post("/execution-jobs/dispatch")
    assert first_dispatch.status_code == 200
    first_payload = first_dispatch.json()
    assert first_payload["due_job_count"] == 1
    assert first_payload["completed_job_count"] == 0
    assert first_payload["retrying_job_count"] == 1
    assert first_payload["failed_job_count"] == 0
    assert first_payload["jobs"][0]["status"] == "retrying"

    second_dispatch = client.post("/execution-jobs/dispatch")
    assert second_dispatch.status_code == 200
    second_payload = second_dispatch.json()
    assert second_payload["due_job_count"] == 1
    assert second_payload["completed_job_count"] == 0
    assert second_payload["retrying_job_count"] == 1
    assert second_payload["failed_job_count"] == 0
    assert second_payload["jobs"][0]["status"] == "retrying"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        assert job.status == "retrying"
        assert job.attempts == 2
        assert job.last_error == "existing broker order is retry_pending"

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "retry_pending"

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "next_open_buy_retry_pending"

    engine.dispose()


def test_dispatch_due_next_open_buy_job_recovers_stale_running_job() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-next-open-dispatch-recover-running",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        job.status = "running"
        job.locked_at = datetime.now(UTC) - timedelta(minutes=10)
        job.attempts = 1
        job.last_error = None

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        plan_item.status = "next_open_buy_dispatching"
        session.commit()

    dispatch_response = client.post("/execution-jobs/dispatch")

    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["due_job_count"] == 1
    assert dispatch_payload["completed_job_count"] == 1
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 0
    assert dispatch_payload["jobs"][0]["status"] == "completed"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob).where(ExecutionJob.job_type == "next_open_buy"))
        assert job is not None
        assert job.status == "completed"
        assert job.attempts == 2
        assert job.locked_at is None
        assert job.last_error is None

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.status == "submitted"

        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

    engine.dispose()


def test_execution_plan_items_preserve_cross_decider_identity_without_symbol_date_collision() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        ),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-cross-decider-identity-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                    "agent_name": "signal-decider",
                    "decision_snapshot_id": "signal-snapshot-1",
                    "decision_run_id": "signal-decision-run-1",
                    "source_plan_item_id": "core-signal-item-1",
                    "source_market_date": "2024-01-03",
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "76000.00",
                    "time_in_force": "day",
                    "decision_agent_name": "short-term-decider",
                    "decision_snapshot_id": "short-term-snapshot-1",
                    "decision_run_id": "short-term-decision-run-1",
                    "source_plan_item_id": "core-short-term-item-1",
                    "source_market_date": "2024-01-03",
                },
            ],
        },
    )
    assert response.status_code == 201

    plans_payload = client.get("/execution-plans").json()
    assert len(plans_payload) == 1
    items_by_source = {
        item["source_plan_item_id"]: item
        for item in plans_payload[0]["items"]
    }
    assert set(items_by_source) == {"core-signal-item-1", "core-short-term-item-1"}

    signal_item = items_by_source["core-signal-item-1"]
    assert signal_item["symbol"] == "005930"
    assert signal_item["status"] == "next_open_buy_scheduled"
    assert signal_item["decision_agent_name"] == "signal-decider"
    assert signal_item["agent_name"] == "signal-decider"
    assert signal_item["decision_snapshot_id"] == "signal-snapshot-1"
    assert signal_item["decision_run_id"] == "signal-decision-run-1"
    assert signal_item["source_market_date"] == "2024-01-03"

    short_term_item = items_by_source["core-short-term-item-1"]
    assert short_term_item["symbol"] == "005930"
    assert short_term_item["status"] == "target_sell_monitoring"
    assert short_term_item["decision_agent_name"] == "short-term-decider"
    assert short_term_item["agent_name"] == "short-term-decider"
    assert short_term_item["decision_snapshot_id"] == "short-term-snapshot-1"
    assert short_term_item["decision_run_id"] == "short-term-decision-run-1"
    assert short_term_item["source_market_date"] == "2024-01-03"

    symbol_detail_payload = client.get("/symbols/005930").json()
    source_identity_by_item = {
        item["source_identity"]["source_plan_item_id"]: item["source_identity"]["decision_agent_name"]
        for item in symbol_detail_payload["plan_items"]
    }
    assert source_identity_by_item == {
        "core-signal-item-1": "signal-decider",
        "core-short-term-item-1": "short-term-decider",
    }

    engine.dispose()


def test_short_term_watch_buy_items_are_monitored_without_open_scheduling() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-short-term-entry-safety-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "tactical_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "trigger_price": "71000.00",
                    "chase_limit_price": "71200.00",
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-tactical-entry-1",
                },
                {
                    "symbol": "000660",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 2},
                    "limit_price": "120000.00",
                    "trigger_price": "119500.00",
                    "chase_limit_price": "120500.00",
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-trigger-chase-entry-1",
                },
                {
                    "symbol": "035420",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "limit_price": "210000.00",
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-plain-next-open-entry-1",
                },
                {
                    "symbol": "068270",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 4},
                    "limit_price": "150000.00",
                    "buy_recommendation_price": "149500.00",
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-recommendation-only-entry-1",
                },
            ],
        },
    )
    assert response.status_code == 201

    plans_payload = client.get("/execution-plans").json()
    items_by_source = {
        item["source_plan_item_id"]: item
        for item in plans_payload[0]["items"]
    }
    assert items_by_source["core-tactical-entry-1"]["status"] == "watch_monitoring_active"
    assert items_by_source["core-trigger-chase-entry-1"]["status"] == "watch_monitoring_active"
    assert items_by_source["core-plain-next-open-entry-1"]["status"] == "short_term_watcher_pending"
    recommendation_only_item = items_by_source["core-recommendation-only-entry-1"]
    assert recommendation_only_item["status"] == "watch_monitoring_active"
    assert recommendation_only_item["trigger_price"] == "149500.0000"

    jobs_payload = client.get("/execution-jobs").json()
    assert jobs_payload == []

    dispatch_payload = client.post("/execution-jobs/dispatch").json()
    assert dispatch_payload["due_job_count"] == 0
    assert dispatch_payload["completed_job_count"] == 0
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 0
    assert broker_adapter.order_calls == []
    assert client.get("/orders").json() == []

    engine.dispose()


def test_core_active_short_term_handoff_becomes_runtime_buy_watch_with_segments() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-short-term-core-handoff-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 2},
                    "trigger_price": "71000.00",
                    "chase_limit_price": "71200.00",
                    "target_price": "72500.00",
                    "stop_loss_price": "70000.00",
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-watch-for-trigger-1",
                    "risk_context": {
                        "tactical_action": "watch_for_trigger",
                        "exec_monitoring_active": True,
                    },
                },
                {
                    "symbol": "000660",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-avoid-chase-1",
                    "risk_context": {
                        "tactical_action": "avoid_chase",
                        "exec_monitoring_active": False,
                    },
                },
            ],
        },
    )

    assert response.status_code == 201
    items_by_source = {
        item["source_plan_item_id"]: item
        for item in client.get("/execution-plans").json()[0]["items"]
    }
    watch_item = items_by_source["core-watch-for-trigger-1"]
    assert watch_item["status"] == "watch_monitoring_active"
    assert watch_item["tactical_action"] == "watch_for_trigger"
    assert watch_item["risk_context"]["watch_buy_monitoring"]["exec_monitoring_active"] is True
    assert watch_item["risk_context"]["watch_buy_monitoring"]["tactical_action"] == "watch_for_trigger"

    blocked_item = items_by_source["core-avoid-chase-1"]
    assert blocked_item["status"] == "short_term_watch_blocked"
    assert blocked_item["tactical_action"] == "avoid_chase"
    assert blocked_item["blocker_reason"] == "tactical_action_avoid_chase_is_not_active_buy_monitoring"

    waiting = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "below trigger",
            "prices": [{"symbol": "005930", "price": "70900.00"}],
        },
    )
    assert waiting.status_code == 200
    waiting_item = waiting.json()["items"][0]
    assert waiting_item["outcome"] == "waiting"
    assert waiting_item["tactical_action"] == "watch_for_trigger"
    assert waiting_item["target_price"] == "72500.0000"
    assert waiting_item["stop_loss_price"] == "70000.0000"
    assert waiting_item["current_price"] == "70900.0000"

    runtime_status = client.get("/runtime/status").json()
    segments = runtime_status["liveness"]["monitoring"]["segments"]
    assert segments["short_term_buy_watch_monitoring"]["item_count"] == 1
    assert segments["short_term_buy_watch_monitoring"]["symbols"] == ["005930"]
    assert segments["blocked_short_term_candidates"]["item_count"] == 1
    assert segments["blocked_short_term_candidates"]["symbols"] == ["000660"]
    assert segments["blocked_short_term_candidates"]["items"][0]["tactical_action"] == "avoid_chase"

    assert client.get("/execution-jobs").json() == []
    assert broker_adapter.order_calls == []
    engine.dispose()


def _build_watch_buy_test_app(
    *,
    order_id: str = "kis-watch-buy-100",
    resume: bool = True,
    max_quote_age_seconds: int = 30,
    retry_cooldown_seconds: int = 5,
):
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id=order_id),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_quote_age_seconds=max_quote_age_seconds,
            monitoring_watch_buy_retry_cooldown_seconds=retry_cooldown_seconds,
            exec_agent_max_position_allocation_pct=Decimal("0.20"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)
    if resume:
        response = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable watch buys"},
        )
        assert response.status_code == 200
    return app, engine, client, broker_adapter


def _create_watch_buy_candidate(
    client: TestClient,
    *,
    plan_id: str,
    trigger_price: str = "71000.00",
    chase_limit_price: str | None = "71200.00",
    quantity_policy: dict[str, object] | None = None,
    risk_context: dict[str, object] | None = None,
    market_date: str | None = "2024-01-03",
    source_market_date: str | None = None,
) -> str:
    item = {
        "symbol": "005930",
        "side": "buy",
        "intent": "next_open_buy",
        "quantity_policy": quantity_policy or {"kind": "shares", "quantity": 2},
        "buy_recommendation_price": trigger_price,
        "time_in_force": "day",
        "risk_context": risk_context or {"exchange": "KRX", "stop_price": "70000.00"},
        "decision_agent_name": "short-term-decider",
        "source_plan_item_id": f"{plan_id}-watch",
    }
    if source_market_date is not None:
        item["source_market_date"] = source_market_date
    if chase_limit_price is not None:
        item["chase_limit_price"] = chase_limit_price
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": plan_id,
            "source": "kai-trading-core",
            "market_date": market_date,
            "items": [item],
        },
    )
    assert response.status_code == 201
    item_id = response.json()["persisted_execution_item_ids"][0]
    persisted_item = client.get("/execution-plans").json()[0]["items"][0]
    assert persisted_item["status"] == "watch_monitoring_active"
    assert persisted_item["trigger_price"] == f"{Decimal(trigger_price):.4f}"
    assert client.get("/execution-jobs").json() == []
    assert client.get("/orders").json() == []
    return item_id


def test_watch_buy_candidate_waits_until_trigger_then_submits_limit_buy() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-trigger-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-trigger-1",
    )

    waiting = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "below trigger should keep monitoring",
            "prices": [{"symbol": "005930", "price": "70900.00"}],
        },
    )
    assert waiting.status_code == 200
    assert waiting.json()["items"][0]["outcome"] == "waiting"
    assert broker_adapter.order_calls == []

    triggered = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger watch buy",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert triggered.status_code == 200
    payload = triggered.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["status"] == "watch_buy_submitted"

    assert len(broker_adapter.order_calls) == 1
    order_call = broker_adapter.order_calls[0]
    assert order_call.execution_plan_item_id == item_id
    assert order_call.side == "buy"
    assert order_call.quantity == 2
    assert order_call.order_type == "limit"
    assert order_call.limit_price == Decimal("71050.00")

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_submitted"
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == item_id)
        )
        assert order is not None
        assert order.idempotency_key == f"monitoring:watch_buy:{item_id}"

    engine.dispose()


def test_watch_buy_day_high_cross_waits_when_only_stale_aggregate_crossed() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-day-high-cross-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-day-high-cross-1",
        trigger_price="71000.00",
        chase_limit_price="71200.00",
    )

    triggered = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high crossed trigger but current pulled back",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "70900.00",
                    "day_low": "70500.00",
                    "day_high": "71100.00",
                }
            ],
        },
    )

    assert triggered.status_code == 200
    payload = triggered.json()
    assert payload["triggered_item_count"] == 0
    assert payload["submitted_order_count"] == 0
    assert payload["items"][0]["outcome"] == "waiting"

    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert "watch_buy_trigger_cross" not in item.risk_context

    engine.dispose()


def test_watch_buy_persisted_trigger_cross_retries_when_price_returns_inside_chase() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-persisted-cross-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-persisted-cross-1",
        trigger_price="71000.00",
        chase_limit_price="71200.00",
    )

    blocked = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger crossed but price is above chase",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "71300.00",
                    "day_low": "70000.00",
                    "day_high": "71400.00",
                }
            ],
        },
    )

    assert blocked.status_code == 200
    assert blocked.json()["submitted_order_count"] == 0
    assert blocked.json()["items"][0]["block_reason"] == "price_above_chase_limit"
    assert broker_adapter.order_calls == []

    retry = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "price returned inside chase after persisted trigger cross",
            "prices": [{"symbol": "005930", "price": "70900.00"}],
        },
    )

    assert retry.status_code == 200
    retry_payload = retry.json()
    assert retry_payload["triggered_item_count"] == 1
    assert retry_payload["submitted_order_count"] == 1
    assert retry_payload["items"][0]["outcome"] == "submitted"
    assert retry_payload["items"][0]["trigger_reason"] == "intraday_watch_buy_day_high_cross"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].limit_price == Decimal("70900.00")

    engine.dispose()


def test_watch_buy_pullback_below_trigger_bypasses_intraday_range_chase_block() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-pullback-below-trigger-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-pullback-below-trigger-1",
        trigger_price="71000.00",
        chase_limit_price="71200.00",
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "short_term_entry_quality": {
                "day_low": "70000.00",
                "day_high": "71400.00",
            },
        },
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger crossed live but price is above chase",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "71300.00",
                    "day_low": "70000.00",
                    "day_high": "71400.00",
                }
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0
    assert crossed.json()["items"][0]["block_reason"] == "price_above_chase_limit"

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "trigger crossed but current has pulled below trigger",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "70900.00",
                    "day_low": "70000.00",
                    "day_high": "71400.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["trigger_reason"] == "intraday_watch_buy_day_high_cross"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].limit_price == Decimal("70900.00")

    engine.dispose()


def test_watch_buy_dip_rebound_keeps_intraday_range_gate_after_cross() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-dip-rebound-range-gate-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-dip-rebound-range-gate-1",
        trigger_price="71000.00",
        chase_limit_price="72000.00",
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "trigger_direction": "at_or_below",
            "short_term_entry_quality": {
                "day_low": "70000.00",
                "day_high": "71400.00",
            },
        },
    )
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.risk_context = {
            **item.risk_context,
            "watch_buy_trigger_cross": {
                "reason": "intraday_watch_buy_day_low_cross",
                "direction": "at_or_below",
                "trigger_price": "71000.0000",
                "observed_price": "70900.0000",
                "observed_at": "2024-01-03T01:00:00+00:00",
                "recorded_at": "2024-01-03T01:00:00+00:00",
                "cross_price": "70900.0000",
                "day_high": "71400.0000",
                "day_low": "70900.0000",
            },
        }
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "dip trigger crossed earlier but rebound is high in range",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "71080.00",
                    "day_low": "70000.00",
                    "day_high": "71400.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "watch_buy_intraday_range_chase_block"
    assert broker_adapter.order_calls == []

    engine.dispose()


def test_watch_buy_intraday_range_gate_prefers_live_day_range_over_stale_context() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-live-range-1"
    )
    _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-live-range-1",
        trigger_price="71000.00",
        chase_limit_price="71200.00",
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "short_term_entry_quality": {
                "day_low": "70000.00",
                "day_high": "71400.00",
            },
        },
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live intraday range should override stale risk context range",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "71080.00",
                    "day_low": "70500.00",
                    "day_high": "71400.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert len(broker_adapter.order_calls) == 1

    engine.dispose()


def test_autonomous_monitoring_expires_prior_day_unfilled_watch_buys() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    prior_active_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-expire-prior-active",
        market_date="2024-01-03",
    )
    prior_blocked_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-expire-prior-blocked",
        market_date="2024-01-03",
    )
    current_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-retain-current",
        market_date="2024-01-04",
    )

    with app.state.db_session_factory() as session:
        blocked_item = session.get(ExecutionPlanItem, prior_blocked_id)
        assert blocked_item is not None
        blocked_item.status = "watch_buy_triggered_blocked"
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=date(2024, 1, 4),
        )

        assert symbols == ["005930"]
        prior_active = session.get(ExecutionPlanItem, prior_active_id)
        prior_blocked = session.get(ExecutionPlanItem, prior_blocked_id)
        current = session.get(ExecutionPlanItem, current_id)
        assert prior_active is not None
        assert prior_blocked is not None
        assert current is not None
        assert prior_active.status == "watch_expired"
        assert prior_blocked.status == "watch_expired"
        assert current.status == "watch_monitoring_active"
        assert (
            prior_active.risk_context["last_monitoring_event"]["event"]
            == "market_date_expired_unfilled"
        )
        assert (
            prior_blocked.risk_context["last_monitoring_event"]["prior_status"]
            == "watch_buy_triggered_blocked"
        )

    assert broker_adapter.order_calls == []
    engine.dispose()


def test_autonomous_monitoring_expires_prior_source_when_plan_date_missing() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    prior_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-expire-prior-source",
        market_date=None,
        source_market_date="2024-01-03",
    )
    current_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-retain-current-with-plan-date",
        market_date="2024-01-04",
    )

    with app.state.db_session_factory() as session:
        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=date(2024, 1, 4),
        )

        assert symbols == ["005930"]
        prior = session.get(ExecutionPlanItem, prior_id)
        current = session.get(ExecutionPlanItem, current_id)
        assert prior is not None
        assert current is not None
        assert prior.status == "watch_expired"
        assert current.status == "watch_monitoring_active"
        assert (
            prior.risk_context["last_monitoring_event"]["event"]
            == "market_date_expired_unfilled"
        )
        assert (
            prior.risk_context["last_monitoring_event"]["source_market_date"]
            == "2024-01-03"
        )

    assert broker_adapter.order_calls == []
    engine.dispose()


def test_autonomous_monitoring_retains_watch_buy_when_source_is_prior_but_plan_is_current() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    current_plan_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-holiday-rollover",
        market_date="2024-01-04",
        source_market_date="2024-01-03",
    )

    with app.state.db_session_factory() as session:
        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=date(2024, 1, 4),
        )

        assert symbols == ["005930"]
        item = session.get(ExecutionPlanItem, current_plan_id)
        assert item is not None
        assert item.status == "watch_monitoring_active"

    assert broker_adapter.order_calls == []
    engine.dispose()


def test_autonomous_monitoring_retains_current_day_and_terminal_watch_buys() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    current_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-current-retained",
        market_date="2024-01-04",
    )
    submitted_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-submitted-retained",
        market_date="2024-01-03",
    )
    rejected_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-rejected-retained",
        market_date="2024-01-03",
    )

    with app.state.db_session_factory() as session:
        submitted = session.get(ExecutionPlanItem, submitted_id)
        rejected = session.get(ExecutionPlanItem, rejected_id)
        assert submitted is not None
        assert rejected is not None
        submitted.status = "watch_buy_submitted"
        rejected.status = "watch_buy_rejected"
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=date(2024, 1, 4),
        )

        assert symbols == ["005930"]
        current = session.get(ExecutionPlanItem, current_id)
        submitted = session.get(ExecutionPlanItem, submitted_id)
        rejected = session.get(ExecutionPlanItem, rejected_id)
        assert current is not None
        assert submitted is not None
        assert rejected is not None
        assert current.status == "watch_monitoring_active"
        assert submitted.status == "watch_buy_submitted"
        assert rejected.status == "watch_buy_rejected"

    assert broker_adapter.order_calls == []
    engine.dispose()


def test_watch_buy_waits_for_pullback_when_gap_above_chase_limit() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-chase-limit-1",
        trigger_price="71000.00",
        chase_limit_price="71100.00",
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "gap above chase limit",
            "prices": [{"symbol": "005930", "price": "71200.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "price_above_chase_limit"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_monitoring_active"
        assert item.risk_context["last_monitoring_event"]["reason"] == "price_above_chase_limit"
        assert item.risk_context["last_monitoring_event"]["event"] == "watch_buy_pullback_wait"

    retry = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "pullback into chase limit should submit",
            "prices": [{"symbol": "005930", "price": "71080.00"}],
        },
    )

    assert retry.status_code == 200
    retry_payload = retry.json()
    assert retry_payload["submitted_order_count"] == 1
    assert retry_payload["items"][0]["outcome"] == "submitted"
    assert retry_payload["items"][0]["status"] == "watch_buy_submitted"
    assert len(broker_adapter.order_calls) == 1

    engine.dispose()


def test_watch_buy_intraday_range_chase_block_keeps_monitoring_active() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-intraday-range-chase-1",
        trigger_price="71000.00",
        chase_limit_price="71200.00",
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "short_term_entry_quality": {
                "day_low": "70000.00",
                "day_high": "71400.00",
            },
        },
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "intraday range position is too extended",
            "prices": [{"symbol": "005930", "price": "71080.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "watch_buy_intraday_range_chase_block"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_monitoring_active"
        last_event = item.risk_context["last_monitoring_event"]
        assert last_event["event"] == "watch_buy_pullback_wait"
        assert last_event["reason"] == "watch_buy_intraday_range_chase_block"

    engine.dispose()


def test_watch_buy_rejects_high_atr_weak_trend_before_submit() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-high-risk-gate-1",
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "short_term_entry_quality": {
                "atr_pct_14": "9.4",
                "ret20": "-2.1",
                "ret5": "4.0",
                "trend_state": "range/mixed",
            },
        },
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "high atr weak trend should block",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "rejected"
    assert payload["items"][0]["block_reason"] == "short_term_high_atr_weak_trend_block"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_rejected"
        last_event = item.risk_context["last_monitoring_event"]
        assert last_event["reason"] == "short_term_high_atr_weak_trend_block"
        assert last_event["details"]["atr_pct_14"] == "9.4000"

    engine.dispose()


def test_watch_buy_rejects_missing_stop_without_submitting() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-missing-stop-1",
        risk_context={"exchange": "KRX"},
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "missing stop should block risk sizing",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "rejected"
    assert payload["items"][0]["message"] == "watch buy requires entry and stop prices for risk-based sizing"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_rejected"
        assert item.risk_context["last_monitoring_event"]["reason"] == "missing_stop_or_entry_price"

    engine.dispose()


def test_exec_agent_sized_watch_buy_waits_for_agent_quantity_without_submitting() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-agent-sized-pending-1",
        quantity_policy={
            "type": "exec_agent_sized",
            "budget_krw": "0",
            "max_safe_notional_krw": "150000",
        },
        risk_context={"exchange": "KRX", "stop_price": "70000.00"},
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "exec-agent sized watch buy should wait for agent quantity",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "exec_agent_sizing_pending"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_triggered_blocked"
        assert item.risk_context["last_monitoring_event"]["reason"] == "exec_agent_sizing_pending"

    engine.dispose()


def test_exec_agent_sized_watch_buy_uses_fallback_only_after_agent_attempts_exhausted() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-agent-fallback-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-agent-sized-fallback-1",
        quantity_policy={
            "type": "exec_agent_sized",
            "budget_krw": "0",
            "max_safe_notional_krw": "150000",
        },
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "target_price": "72420.00",
        },
    )

    first_response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "exec-agent attempts not exhausted",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )
    assert first_response.status_code == 200
    assert first_response.json()["submitted_order_count"] == 0
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = "watch_monitoring_active"
        risk_context = dict(item.risk_context or {})
        risk_context["exec_agent_watch_sizing_attempt_count"] = 3
        risk_context["exec_agent_watch_sizing_max_attempts"] = 3
        risk_context["exec_agent_watch_sizing_fallback_allowed"] = True
        item.risk_context = risk_context
        session.commit()

    fallback_response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "exec-agent attempts exhausted",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert fallback_response.status_code == 200
    payload = fallback_response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].quantity > 0

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        event = item.risk_context["monitoring_events"][-2]
        assert event["sizing_source"] == "risk_sizing_after_exec_agent_attempts_exhausted"

    engine.dispose()


def test_exec_agent_watch_sizing_response_materializes_stub_order_path() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-agent-sized-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-agent-sized-approved-1",
        quantity_policy={
            "type": "exec_agent_sized",
            "budget_krw": "0",
            "max_safe_notional_krw": "150000",
        },
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "target_price": "72420.00",
        },
    )

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).where(
                ExecutionPlan.external_plan_id == "plan-watch-buy-agent-sized-approved-1"
            )
        )
        assert plan is not None
        summary = _apply_decisions(
            db=session,
            plan=plan,
            expected_item_ids={item_id},
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "decisions": [
                    {
                        "execution_plan_item_id": item_id,
                        "symbol": "005930",
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": 142100,
                        "sizing_price": "71050.00",
                        "confidence": 0.68,
                        "target_profile": "tactical_2pct",
                        "reason_code": "agent_sized_tactical_buy",
                        "reason": "agent approved a small tactical watch quantity",
                    }
                ],
            },
        )
        assert summary["approved"] == 1
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "agent-sized response should reach order path",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].quantity == 2

    engine.dispose()


def test_watch_buy_rejects_stale_quote_without_submitting() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        max_quote_age_seconds=10
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-stale-quote-1",
    )
    stale_observed_at = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "stale quote should block watch buy",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "71050.00",
                    "observed_at": stale_observed_at,
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "stale_quote"
    assert "quote is stale" in payload["items"][0]["message"]
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_monitoring_active"
        assert item.risk_context["last_monitoring_event"]["reason"] == "stale_quote"

    engine.dispose()


def test_watch_buy_safety_pause_keeps_trigger_retryable() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(resume=False)
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-paused-1",
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "paused runtime should block watch buy",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "paused"
    assert payload["items"][0]["status"] == "watch_buy_triggered_blocked"
    assert broker_adapter.authenticate_calls == 0
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_triggered_blocked"
        assert item.risk_context["last_monitoring_event"]["reason"] == "paused"
        assert (
            item.risk_context["last_monitoring_event"]["event"]
            == "watch_buy_triggered_blocked"
        )

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "resume automation"},
    )
    assert resume.status_code == 200

    retry = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "resumed runtime should retry watch buy",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert retry.status_code == 200
    retry_payload = retry.json()
    assert retry_payload["triggered_item_count"] == 1
    assert retry_payload["submitted_order_count"] == 1
    assert retry_payload["items"][0]["outcome"] == "submitted"
    assert retry_payload["items"][0]["status"] == "watch_buy_submitted"
    assert len(broker_adapter.order_calls) == 1

    engine.dispose()


def test_watch_buy_account_snapshot_failure_keeps_trigger_retryable() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        retry_cooldown_seconds=5
    )
    broker_adapter.account_snapshot_error = BrokerAdapterError("account lookup timeout")
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-account-snapshot-retry-1",
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "account snapshot failure should not terminally reject watch buy",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "account_snapshot_unavailable"
    assert payload["items"][0]["status"] == "watch_buy_triggered_blocked"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_triggered_blocked"
        assert (
            item.risk_context["last_monitoring_event"]["event"]
            == "watch_buy_triggered_blocked"
        )
        assert (
            item.risk_context["last_monitoring_event"]["reason"]
            == "account_snapshot_unavailable"
        )

    broker_adapter.account_snapshot_error = None
    immediate_retry = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "immediate retry should respect cooldown",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert immediate_retry.status_code == 200
    immediate_payload = immediate_retry.json()
    assert immediate_payload["submitted_order_count"] == 0
    assert immediate_payload["blocked_item_count"] == 1
    assert immediate_payload["items"][0]["outcome"] == "blocked"
    assert immediate_payload["items"][0]["block_reason"] == "retry_cooldown"
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        event = item.risk_context["last_monitoring_event"]
        item.risk_context = {
            **item.risk_context,
            "last_monitoring_event": {
                **event,
                "recorded_at": (datetime.now(UTC) - timedelta(seconds=10)).isoformat(),
            },
        }
        flag_modified(item, "risk_context")
        session.commit()

    cooldown_elapsed_retry = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "account snapshot recovered should submit retryable watch buy",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert cooldown_elapsed_retry.status_code == 200
    retry_payload = cooldown_elapsed_retry.json()
    assert retry_payload["triggered_item_count"] == 1
    assert retry_payload["submitted_order_count"] == 1
    assert retry_payload["items"][0]["outcome"] == "submitted"
    assert retry_payload["items"][0]["status"] == "watch_buy_submitted"
    assert len(broker_adapter.order_calls) == 1

    engine.dispose()


@pytest.mark.parametrize(
    ("control_action", "expected_block_reason"),
    [
        (None, "paused"),
        ("/controls/kill-switch", "kill_switch"),
    ],
)
def test_budget_only_watch_buy_safety_block_keeps_trigger_retryable(
    control_action: str | None,
    expected_block_reason: str,
) -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(resume=False)
    if control_action is not None:
        control = client.post(
            control_action,
            json={"actor": "ops", "source": "console", "reason": "block automation"},
        )
        assert control.status_code == 200
    item_id = _create_watch_buy_candidate(
        client,
        plan_id=f"plan-budget-watch-buy-{expected_block_reason}",
        quantity_policy={"kind": "budget", "max_budget_krw": "150000.00"},
        risk_context={
            "exchange": "KRX",
            "stop_price": "70000.00",
            "max_budget_krw": "150000.00",
        },
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "blocked runtime should keep budget-only watch buy retryable",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == expected_block_reason
    assert payload["items"][0]["status"] == "watch_buy_triggered_blocked"
    assert broker_adapter.authenticate_calls == 0
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_triggered_blocked"
        assert (
            item.risk_context["last_monitoring_event"]["event"]
            == "watch_buy_triggered_blocked"
        )
        assert item.risk_context["last_monitoring_event"]["reason"] == expected_block_reason

    if expected_block_reason == "paused":
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "resume automation"},
        )
        assert resume.status_code == 200

        retry = client.post(
            "/monitoring/evaluate",
            json={
                "actor": "ops",
                "source": "test",
                "reason": "resumed runtime should retry budget-only watch buy",
                "prices": [{"symbol": "005930", "price": "71050.00"}],
            },
        )

        assert retry.status_code == 200
        retry_payload = retry.json()
        assert retry_payload["triggered_item_count"] == 1
        assert retry_payload["submitted_order_count"] == 1
        assert retry_payload["items"][0]["outcome"] == "submitted"
        assert retry_payload["items"][0]["status"] == "watch_buy_submitted"
        assert len(broker_adapter.order_calls) == 1

    engine.dispose()


def test_watch_buy_duplicate_monitor_cycles_do_not_submit_duplicate_order() -> None:
    app, engine, client, broker_adapter = _build_watch_buy_test_app(
        order_id="kis-watch-buy-dedupe-1"
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-watch-buy-dedupe-1",
    )

    first = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "first trigger",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )
    second = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "duplicate trigger",
            "prices": [{"symbol": "005930", "price": "71050.00"}],
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["submitted_order_count"] == 1
    assert second.json()["submitted_order_count"] == 0
    assert len(broker_adapter.order_calls) == 1

    with app.state.db_session_factory() as session:
        orders = session.scalars(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == item_id)
        ).all()
        assert len(orders) == 1

    engine.dispose()


def _create_short_term_watcher_candidate(client: TestClient) -> str:
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": f"plan-short-term-watcher-{time.monotonic_ns()}",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "notional", "max_notional": "350000"},
                    "decision_agent_name": "short-term-decider",
                    "decision_snapshot_id": "short-term-snapshot-live-1",
                    "decision_run_id": "short-term-run-live-1",
                    "source_plan_item_id": "core-short-term-live-1",
                }
            ],
        },
    )
    assert response.status_code == 201
    item_id = response.json()["persisted_execution_item_ids"][0]
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "short_term_watcher_pending"
    assert item["execution_plan_item_id"] == item_id
    return item_id


def _short_term_decision_payload(
    item_id: str,
    *,
    dry_run: bool = True,
    available_cash: str = "1000000.00",
    holdings: dict[str, int] | None = None,
    open_order_symbols: list[str] | None = None,
) -> dict[str, object]:
    return {
        "execution_plan_item_id": item_id,
        "dry_run": dry_run,
        "agent_output": {
            "decision": "approve",
            "order_type": "market",
            "max_notional": "350000.00",
            "reference_price": "70000.00",
            "target_price": "77000.00",
            "stop_price": "66500.00",
            "reason_code": "strict_json_approved",
        },
        "account_context": {
            "available_cash": available_cash,
            "last_price": "70000.00",
            "holdings": holdings or {},
            "open_order_symbols": open_order_symbols or [],
        },
    }


def test_short_term_watcher_preview_rejects_insufficient_cash_existing_holding_and_duplicate_order() -> None:
    app, engine = build_test_app(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(),
        ),
    )
    client = TestClient(app)
    item_id = _create_short_term_watcher_candidate(client)

    insufficient = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, available_cash="1000.00"),
    )
    assert insufficient.status_code == 200
    assert insufficient.json()["reason_code"] == "insufficient_cash"

    holding = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, holdings={"005930": 1}),
    )
    assert holding.status_code == 200
    assert holding.json()["reason_code"] == "existing_holding"

    duplicate = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, open_order_symbols=["005930"]),
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["reason_code"] == "duplicate_open_order"
    engine.dispose()


def test_short_term_watcher_rejects_invalid_free_form_agent_output() -> None:
    app, engine = build_test_app(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(),
        ),
    )
    client = TestClient(app)
    item_id = _create_short_term_watcher_candidate(client)
    payload = _short_term_decision_payload(item_id)
    payload["agent_output"] = {"text": "buy some shares please"}

    response = client.post("/short-term-watcher/decisions", json=payload)

    assert response.status_code == 422
    engine.dispose()


def test_short_term_watcher_approved_market_buy_submits_order_and_materializes_monitoring() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-short-term-buy-1"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="***",
            kis_app_secret="***",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)
    resume = client.post("/controls/resume", json={"actor": "ops", "source": "console", "reason": "enable short-term watcher"})
    assert resume.status_code == 200
    item_id = _create_short_term_watcher_candidate(client)

    response = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, dry_run=False),
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["approved"] is True
    assert payload["order"]["idempotency_key"] == f"short-term-watcher:buy:{item_id}"
    assert broker_adapter.order_calls[0].order_type == "market"
    assert broker_adapter.order_calls[0].quantity == 5

    plan_items = client.get("/execution-plans").json()[0]["items"]
    items_by_intent = {item["intent"]: item for item in plan_items}
    assert items_by_intent["target_sell"]["status"] == "target_sell_monitoring"
    assert items_by_intent["target_sell"]["trigger_price"] == "77000.0000"
    assert items_by_intent["stop_sell"]["status"] == "stop_sell_monitoring"
    assert items_by_intent["stop_sell"]["trigger_price"] == "66500.0000"
    engine.dispose()


def test_short_term_watcher_broker_rejection_does_not_materialize_monitoring() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "broker rejected order",
            status_code=422,
            classification=BrokerErrorClassification.VALIDATION,
            code="KIS_REJECTED",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={"rt_cd": "1", "msg_cd": "KIS_REJECTED"},
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="***",
            kis_app_secret="***",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)
    resume = client.post("/controls/resume", json={"actor": "ops", "source": "console", "reason": "enable short-term watcher"})
    assert resume.status_code == 200
    item_id = _create_short_term_watcher_candidate(client)

    response = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, dry_run=False),
    )

    assert response.status_code == 422
    payload = response.json()
    assert payload["approved"] is False
    assert payload["reason_code"] == "submission_not_accepted"
    assert payload["monitoring"] == []
    assert payload["monitoring_preview"] != []
    assert len(broker_adapter.order_calls) == 1

    plan_items = client.get("/execution-plans").json()[0]["items"]
    assert {item["intent"] for item in plan_items} == {"next_open_buy"}
    assert plan_items[0]["status"] == "broker_order_rejected"
    engine.dispose()


def test_short_term_watcher_live_submission_uses_broker_account_snapshot() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime.now(UTC),
            source="stub:live-account",
            holdings=[BrokerAccountHolding(symbol="005930", quantity=3)],
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="***",
            kis_app_secret="***",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)
    resume = client.post("/controls/resume", json={"actor": "ops", "source": "console", "reason": "enable short-term watcher"})
    assert resume.status_code == 200
    item_id = _create_short_term_watcher_candidate(client)

    response = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, dry_run=False, holdings={}),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["reason_code"] == "existing_holding"
    assert payload["context"]["broker_account_source"] == "stub:live-account"
    assert broker_adapter.order_calls == []
    engine.dispose()


def test_short_term_watcher_live_submission_respects_runtime_safety_guards() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url=KIS_PAPER_API_BASE_URL),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)
    item_id = _create_short_term_watcher_candidate(client)

    blocked = client.post(
        "/short-term-watcher/decisions",
        json=_short_term_decision_payload(item_id, dry_run=False),
    )

    assert blocked.status_code == 200
    assert blocked.json()["reason_code"] == "runtime_safety_blocked"
    assert broker_adapter.order_calls == []
    assert client.get("/orders").json() == []
    engine.dispose()



def test_short_term_exec_agent_sized_next_open_buy_materializes_when_budget_funds_shares() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        ),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-short-term-exec-sized-entry-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {
                        "type": "exec_agent_sized",
                        "budget_krw": "150000",
                        "estimated_reference_price": 71000,
                    },
                    "decision_agent_name": "short-term-decider",
                    "source_plan_item_id": "core-short-term-exec-sized-entry-1",
                },
            ],
        },
    )
    assert response.status_code == 201

    plans_payload = client.get("/execution-plans").json()
    item = plans_payload[0]["items"][0]
    assert item["status"] == "next_open_buy_scheduled"

    jobs_payload = client.get("/execution-jobs").json()
    assert len(jobs_payload) == 1
    assert jobs_payload[0]["job_type"] == "next_open_buy"
    assert jobs_payload[0]["status"] == "scheduled"

    engine.dispose()


def test_orders_monitoring_fills_and_positions_retain_decider_identity() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-identity-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-identity-retention-1",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                    "decision_agent_name": "signal-decider",
                    "decision_snapshot_id": "signal-order-snapshot-1",
                    "decision_run_id": "signal-order-run-1",
                    "source_plan_item_id": "core-signal-order-1",
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "76000.00",
                    "time_in_force": "day",
                    "decision_agent_name": "short-term-decider",
                    "decision_snapshot_id": "short-monitor-snapshot-1",
                    "decision_run_id": "short-monitor-run-1",
                    "source_plan_item_id": "core-short-monitor-1",
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    buy_item_id, sell_item_id = plan_response.json()["persisted_execution_item_ids"]

    dispatch_response = client.post("/execution-jobs/dispatch")
    assert dispatch_response.status_code == 200
    assert dispatch_response.json()["completed_job_count"] == 1

    with app.state.db_session_factory() as session:
        buy_order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == buy_item_id)
        )
        assert buy_order is not None
        session.add(
            ExecutionFill(
                broker_order_pk=buy_order.id,
                broker_fill_id="fill-identity-1",
                symbol="005930",
                side="buy",
                quantity=5,
                price=Decimal("71000.0000"),
                filled_at=datetime(2026, 5, 1, 9, 5, tzinfo=UTC),
                payload={"source": "broker"},
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("71000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    monitoring_response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "identity retention check",
            "prices": [{"symbol": "005930", "price": "76010.00"}],
        },
    )
    assert monitoring_response.status_code == 200
    monitoring_item = monitoring_response.json()["items"][0]
    assert monitoring_item["execution_plan_item_id"] == sell_item_id
    assert monitoring_item["source_identity"]["decision_agent_name"] == "short-term-decider"
    assert monitoring_item["source_identity"]["source_plan_item_id"] == "core-short-monitor-1"
    assert monitoring_item["order"]["source_identity"]["decision_agent_name"] == "short-term-decider"
    assert monitoring_item["order"]["source_identity"]["decision_snapshot_id"] == "short-monitor-snapshot-1"

    orders_payload = client.get("/orders").json()
    orders_by_item_id = {
        order["execution_plan_item_id"]: order
        for order in orders_payload
    }
    assert orders_by_item_id[buy_item_id]["source_identity"]["decision_agent_name"] == "signal-decider"
    assert orders_by_item_id[buy_item_id]["source_identity"]["source_plan_item_id"] == "core-signal-order-1"
    assert orders_by_item_id[sell_item_id]["source_identity"]["decision_agent_name"] == "short-term-decider"
    assert orders_by_item_id[sell_item_id]["source_identity"]["source_plan_item_id"] == "core-short-monitor-1"

    fills_payload = client.get("/executions").json()
    assert len(fills_payload) == 1
    assert fills_payload[0]["source_identity"]["decision_agent_name"] == "signal-decider"
    assert fills_payload[0]["source_identity"]["source_plan_item_id"] == "core-signal-order-1"

    positions_payload = client.get("/positions").json()
    assert len(positions_payload) == 1
    position_identity_by_source = {
        identity["source_plan_item_id"]: identity["decision_agent_name"]
        for identity in positions_payload[0]["source_identities"]
    }
    assert position_identity_by_source == {
        "core-short-monitor-1": "short-term-decider",
        "core-signal-order-1": "signal-decider",
    }

    symbol_detail_payload = client.get("/symbols/005930").json()
    symbol_position_identity_by_source = {
        identity["source_plan_item_id"]: identity["decision_agent_name"]
        for identity in symbol_detail_payload["position"]["source_identities"]
    }
    assert symbol_position_identity_by_source == position_identity_by_source

    engine.dispose()


def test_monitoring_evaluate_triggers_target_sell_order_from_position_and_price_snapshot() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-sell-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-target-sell-monitor-1",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "percent", "fraction": 0.5},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "intraday trigger check",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 1
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["blocked_item_count"] == 0
    assert payload["items"][0]["execution_plan_item_id"] == plan_item_id
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["symbol"] == "005930"
    assert payload["items"][0]["intent"] == "target_sell"
    assert payload["items"][0]["trigger_price"] == "125000.0000"
    assert payload["items"][0]["observed_price"] == "125100.0000"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == plan_item_id
    assert broker_adapter.order_calls[0].side == "sell"
    assert broker_adapter.order_calls[0].quantity == 5
    assert broker_adapter.order_calls[0].order_type == "market"
    assert broker_adapter.order_calls[0].exchange == "KRX"

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    assert runtime_status.json()["liveness"]["monitoring"]["status"] == "running"

    with app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "broker_order_submitted"

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.idempotency_key == f"monitoring:target_sell:{plan_item_id}"
        assert order.status == "submitted"

        checkpoint = session.scalar(
            select(MonitoringCheckpoint).where(
                MonitoringCheckpoint.worker_name == "intraday_target_stop_monitor"
            )
        )
        assert checkpoint is not None
        assert checkpoint.status == "running"
        assert checkpoint.last_heartbeat_at is not None
        assert checkpoint.last_success_at is not None

    engine.dispose()


def test_monitoring_rearms_stop_to_breakeven_after_partial_take_profit() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-partial-profit-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            breakeven_stop_buffer_pct=Decimal("0.2"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-partial-profit-rearm-stop",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 4},
                    "trigger_price": "102500.00",
                    "risk_context": {
                        "exchange": "KRX",
                        "exit_rule": {
                            "kind": "partial_take_profit",
                            "stage": "first",
                        },
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 10},
                    "trigger_price": "97000.00",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "first take profit should protect remainder",
            "prices": [{"symbol": "005930", "price": "102600.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["execution_plan_item_id"] == target_item_id
    assert payload["items"][0]["outcome"] == "submitted"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == target_item_id
    assert broker_adapter.order_calls[0].quantity == 4

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.trigger_price == Decimal("100200.0000")
        assert stop_item.quantity_policy == {
            "kind": "shares",
            "quantity": 6,
            "source": "partial_take_profit_breakeven_rearm",
            "parent_target_item_id": target_item_id,
        }
        assert stop_item.risk_context["last_monitoring_event"]["event"] == (
            "stop_sell_rearmed_after_partial_take_profit"
        )
        assert stop_item.risk_context["last_monitoring_event"]["remaining_quantity"] == 6

    engine.dispose()


def test_monitoring_does_not_commit_partial_take_profit_without_stop_rearm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-partial-profit-rollback"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            breakeven_stop_buffer_pct=Decimal("0.2"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200
    auth_response = client.post(
        "/broker/session/authenticate",
        json={"actor": "ops", "source": "console", "reason": "prime broker session"},
    )
    assert auth_response.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-partial-profit-rearm-stop-failure",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 4},
                    "trigger_price": "102500.00",
                    "risk_context": {
                        "exchange": "KRX",
                        "exit_rule": {
                            "kind": "partial_take_profit",
                            "stage": "first",
                        },
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 10},
                    "trigger_price": "97000.00",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    def fail_stop_rearm(**_: object) -> None:
        raise RuntimeError("simulated stop rearm persistence failure")

    monkeypatch.setattr(
        "app.services.monitoring._rearm_stop_after_partial_take_profit",
        fail_stop_rearm,
    )

    with pytest.raises(RuntimeError, match="simulated stop rearm persistence failure"):
        client.post(
            "/monitoring/evaluate",
            json={
                "actor": "ops",
                "source": "test",
                "reason": "first take profit should protect remainder",
                "prices": [{"symbol": "005930", "price": "102600.00"}],
            },
        )

    assert len(broker_adapter.order_calls) == 1
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "target_sell_monitoring"
        assert stop_item.trigger_price == Decimal("97000.0000")
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 10}
        assert stop_item.risk_context == {"exchange": "KRX"}
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == target_item_id)
        )
        assert order is None

    engine.dispose()


def test_monitoring_evaluate_triggers_profit_protection_after_peak_giveback() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-profit-protect-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            profit_protection_min_peak_pct=Decimal("2.0"),
            profit_protection_giveback_pct=Decimal("1.2"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-profit-protect-monitor-1",
            "source": "kai-trading-core",
            "market_date": "2026-05-27",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "110000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=7,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    first = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "establish profitable peak",
            "observed_at": "2026-05-27T01:00:00Z",
            "prices": [{"symbol": "005930", "price": "103000.00"}],
        },
    )
    assert first.status_code == 200
    assert first.json()["items"][0]["outcome"] == "waiting"
    assert broker_adapter.order_calls == []

    second = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "protect profit after giveback",
            "observed_at": "2026-05-27T01:05:00Z",
            "prices": [{"symbol": "005930", "price": "101700.00"}],
        },
    )

    assert second.status_code == 200
    payload = second.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    item_payload = payload["items"][0]
    assert item_payload["outcome"] == "submitted"
    assert item_payload["trigger_reason"] == "profit_protection_trailing_stop"
    assert item_payload["trigger"]["peak_profit_pct"] == "3.0000"
    assert item_payload["trigger"]["current_profit_pct"] == "1.7000"
    assert item_payload["trigger"]["giveback_pct"] == "1.3000"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == plan_item_id
    assert broker_adapter.order_calls[0].side == "sell"
    assert broker_adapter.order_calls[0].quantity == 7

    with app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.risk_context["profit_protection"]["peak_price"] == "103000.0000"
        assert plan_item.risk_context["last_monitoring_event"]["reason"] == "profit_protection_trailing_stop"

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is not None
        assert order.idempotency_key == f"monitoring:target_sell:{plan_item_id}"
        assert order.request_metadata["metadata"]["trigger_reason"] == "profit_protection_trailing_stop"
        assert order.request_metadata["metadata"]["trigger"]["peak_profit_pct"] == "3.0000"

    engine.dispose()


def test_autonomous_retry_preserves_profit_protection_trigger_metadata() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-profit-protect-retry"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_broker_actions_per_cycle=1,
            profit_protection_min_peak_pct=Decimal("2.0"),
            profit_protection_giveback_pct=Decimal("1.2"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-profit-protect-retry",
            "source": "kai-trading-core",
            "market_date": "2026-05-27",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "151000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "110000.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "profit_protection": {
                            "peak_price": "103000.0000",
                            "peak_profit_pct": "3.0000",
                        },
                    },
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    first_item_id, profit_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add_all(
            [
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=1,
                    average_price=Decimal("150000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=7,
                    average_price=Decimal("100000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
            ]
        )
        session.commit()

    evaluation = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "defer profit protection behind broker cap",
            "observed_at": "2026-05-27T01:05:00Z",
            "prices": [
                {"symbol": "000660", "price": "151500.00"},
                {"symbol": "005930", "price": "101700.00"},
            ],
        },
    )
    assert evaluation.status_code == 200
    assert evaluation.json()["submitted_order_count"] == 1
    assert evaluation.json()["deferred_item_count"] == 1

    with app.state.db_session_factory() as session:
        profit_item = session.get(ExecutionPlanItem, profit_item_id)
        assert profit_item is not None
        assert profit_item.status == "target_sell_triggered_pending_dispatch"
        assert (
            profit_item.risk_context["last_monitoring_event"]["reason"]
            == "profit_protection_trailing_stop"
        )

        retry = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[],
            now=datetime(2026, 5, 27, 1, 6, tzinfo=UTC),
        )

        assert retry.payload["submitted_order_count"] == 1
        assert retry.payload["triggered_item_count"] == 1

        orders = session.scalars(select(BrokerOrder).order_by(BrokerOrder.created_at.asc())).all()
        assert [order.execution_plan_item_id for order in orders] == [
            first_item_id,
            profit_item_id,
        ]
        retry_order = orders[1]
        assert (
            retry_order.request_metadata["metadata"]["trigger_reason"]
            == "profit_protection_trailing_stop"
        )
        assert retry_order.request_metadata["metadata"]["trigger"]["peak_profit_pct"] == "3.0000"
        assert retry_order.request_metadata["metadata"]["trigger"]["current_profit_pct"] == "1.7000"
        assert retry_order.request_metadata["metadata"]["trigger"]["giveback_pct"] == "1.3000"

    engine.dispose()


def test_monitoring_evaluate_preserves_stop_precedence_over_profit_protection() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-stop-precedence-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            profit_protection_min_peak_pct=Decimal("2.0"),
            profit_protection_giveback_pct=Decimal("1.2"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-profit-protect-stop-precedence",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "110000.00",
                    "risk_context": {
                        "exchange": "KRX",
                        "profit_protection": {
                            "peak_price": "103000.0000",
                            "peak_profit_pct": "3.0000",
                        },
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "101800.00",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=7,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "stop should win over protection",
            "prices": [{"symbol": "005930", "price": "101700.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    results_by_item_id = {
        item["execution_plan_item_id"]: item
        for item in payload["items"]
    }
    assert results_by_item_id[target_item_id]["outcome"] == "waiting"
    assert results_by_item_id[stop_item_id]["outcome"] == "submitted"
    assert results_by_item_id[stop_item_id]["trigger_reason"] == "stop_sell"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == stop_item_id

    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "target_sell_pending_config"
        assert stop_item.status == "broker_order_submitted"

    engine.dispose()


def test_monitoring_evaluate_triggers_aged_position_profit_protection() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-aged-profit-protect-100"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            profit_protection_min_peak_pct=Decimal("2.0"),
            profit_protection_giveback_pct=Decimal("1.2"),
            profit_protection_aged_session_min=2,
            profit_protection_aged_min_current_profit_pct=Decimal("0.5"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-aged-profit-protect-monitor",
            "source": "kai-trading-core",
            "market_date": "2026-05-26",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "110000.00",
                    "risk_context": {
                        "exchange": "KRX",
                        "profit_protection": {
                            "peak_price": "101800.0000",
                            "peak_profit_pct": "1.8000",
                        },
                    },
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=4,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "protect aged positive position",
            "observed_at": "2026-05-27T01:00:00Z",
            "prices": [{"symbol": "005930", "price": "100500.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1
    item_payload = payload["items"][0]
    assert item_payload["trigger_reason"] == "aged_position_profit_protection"
    assert item_payload["trigger"]["holding_session"] == 2
    assert item_payload["trigger"]["current_profit_pct"] == "0.5000"
    assert item_payload["trigger"]["giveback_pct"] == "1.3000"

    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == plan_item_id
    assert broker_adapter.order_calls[0].quantity == 4

    engine.dispose()


def test_monitoring_evaluate_blocks_aged_profit_protection_for_small_current_profit() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-aged-profit-protect-small"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            profit_protection_min_peak_pct=Decimal("3.0"),
            profit_protection_giveback_pct=Decimal("1.2"),
            profit_protection_aged_session_min=2,
            profit_protection_aged_min_current_profit_pct=Decimal("1.0"),
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-aged-profit-protect-small-profit",
            "source": "kai-trading-core",
            "market_date": "2026-05-26",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "trigger_price": "110000.00",
                    "risk_context": {
                        "exchange": "KRX",
                        "profit_protection": {
                            "peak_price": "101800.0000",
                            "peak_profit_pct": "1.8000",
                        },
                    },
                }
            ],
        },
    )
    assert plan_response.status_code == 201

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=4,
                average_price=Decimal("100000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "small aged profit should keep watching",
            "observed_at": "2026-05-27T01:00:00Z",
            "prices": [{"symbol": "005930", "price": "100500.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 0
    assert payload["submitted_order_count"] == 0
    assert payload["items"][0]["outcome"] == "waiting"
    assert broker_adapter.order_calls == []

    engine.dispose()


def test_monitoring_evaluate_reports_blocked_stop_sell_trigger_when_runtime_is_paused() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-sell-200"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-stop-sell-monitor-1",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "trigger_price": "70000.00",
                    "limit_price": "69950.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("70950.5000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "paused trigger check",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "69900.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 1
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["execution_plan_item_id"] == plan_item_id
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "paused"

    assert broker_adapter.authenticate_calls == 0
    assert broker_adapter.order_calls == []

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    assert runtime_status.json()["liveness"]["monitoring"]["status"] == "running"

    with app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "stop_sell_triggered_blocked"

        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == plan_item_id)
        )
        assert order is None

        checkpoint = session.scalar(
            select(MonitoringCheckpoint).where(
                MonitoringCheckpoint.worker_name == "intraday_target_stop_monitor"
            )
        )
        assert checkpoint is not None
        assert checkpoint.status == "running"
        assert checkpoint.last_heartbeat_at is not None

    engine.dispose()


def test_monitoring_evaluate_tracks_remaining_shares_across_triggered_items() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-sell-oversell-guard"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-target-sell-monitor-remaining-shares",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 6},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 6},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    first_item_id, second_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "multiple triggered sells should not oversell",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 2
    assert payload["triggered_item_count"] == 2
    assert payload["submitted_order_count"] == 1
    assert payload["blocked_item_count"] == 0
    assert len(payload["items"]) == 2

    items_by_outcome = {item["outcome"]: item for item in payload["items"]}
    submitted_item = items_by_outcome["submitted"]
    no_position_item = items_by_outcome["no_position"]

    assert submitted_item["execution_plan_item_id"] in {first_item_id, second_item_id}
    assert submitted_item["symbol"] == "005930"
    assert submitted_item["intent"] == "target_sell"
    assert submitted_item["trigger_price"] == "125000.0000"
    assert submitted_item["observed_price"] == "125100.0000"
    assert submitted_item["order"]["quantity"] == 6

    assert no_position_item["execution_plan_item_id"] == (
        second_item_id if submitted_item["execution_plan_item_id"] == first_item_id else first_item_id
    )
    assert no_position_item["symbol"] == "005930"
    assert no_position_item["intent"] == "target_sell"
    assert no_position_item["trigger_price"] == "125000.0000"
    assert no_position_item["observed_price"] == "125100.0000"
    assert no_position_item["outcome"] == "no_position"
    assert no_position_item["message"] == "no sellable execution position is available for this trigger"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == first_item_id
    assert broker_adapter.order_calls[0].side == "sell"
    assert broker_adapter.order_calls[0].quantity == 6
    assert broker_adapter.order_calls[0].order_type == "market"
    assert broker_adapter.order_calls[0].exchange == "KRX"

    with app.state.db_session_factory() as session:
        first_item = session.get(ExecutionPlanItem, first_item_id)
        second_item = session.get(ExecutionPlanItem, second_item_id)
        assert first_item is not None
        assert second_item is not None
        assert first_item.status == "broker_order_submitted"
        assert second_item.status == "target_sell_monitoring"

        orders = session.scalars(
            select(BrokerOrder)
            .where(BrokerOrder.symbol == "005930")
            .order_by(BrokerOrder.created_at.asc(), BrokerOrder.id.asc())
        ).all()
        assert len(orders) == 1
        assert orders[0].execution_plan_item_id == first_item_id
        assert orders[0].quantity == 6

    engine.dispose()


def test_monitoring_evaluate_ignores_triggered_sell_for_non_held_symbol() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-sell-non-held"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-non-held-sell-monitor-risk-noise",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "non-held sell timing should not count as monitoring risk",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 0
    assert payload["triggered_item_count"] == 0
    assert payload["submitted_order_count"] == 0
    assert payload["items"] == []
    assert broker_adapter.order_calls == []

    engine.dispose()


def test_monitoring_evaluate_round_robins_symbols_when_dispatch_capacity_is_bounded() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-shared-monitor-runtime"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_symbols_per_cycle=128,
            monitoring_max_broker_actions_per_cycle=2,
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-shared-monitor-runtime-round-robin",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "90000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    first_item_id, second_item_id, third_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add_all(
            [
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("120000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=1,
                    average_price=Decimal("88000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
            ]
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "shared symbol scheduler should round robin before draining one symbol",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                },
                {
                    "symbol": "000660",
                    "price": "90100.00",
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 3
    assert payload["triggered_item_count"] == 3
    assert payload["submitted_order_count"] == 2
    assert payload["blocked_item_count"] == 0
    assert payload["deferred_item_count"] == 1
    assert payload["items"][0]["execution_plan_item_id"] == first_item_id
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][1]["execution_plan_item_id"] == second_item_id
    assert payload["items"][1]["outcome"] == "deferred"
    assert payload["items"][1]["message"] == "broker action capacity reached for this monitoring cycle"
    assert payload["items"][2]["execution_plan_item_id"] == third_item_id
    assert payload["items"][2]["outcome"] == "submitted"

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 2
    assert [call.execution_plan_item_id for call in broker_adapter.order_calls] == [
        first_item_id,
        third_item_id,
    ]

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    monitoring = runtime_status.json()["liveness"]["monitoring"]
    assert monitoring["status"] == "degraded"
    assert monitoring["topology"] == "shared_symbol_scheduler"
    assert monitoring["scheduler"]["selection_strategy"] == "round_robin_by_symbol"
    assert monitoring["dispatcher"]["max_broker_actions_per_cycle"] == 2
    assert monitoring["dispatcher"]["last_cycle_action_count"] == 2
    assert monitoring["dispatcher"]["backlog_item_count"] == 1
    assert monitoring["dispatcher"]["backlog_symbols"] == ["005930"]
    assert monitoring["last_cycle"]["deferred_item_count"] == 1

    with app.state.db_session_factory() as session:
        first_item = session.get(ExecutionPlanItem, first_item_id)
        second_item = session.get(ExecutionPlanItem, second_item_id)
        third_item = session.get(ExecutionPlanItem, third_item_id)
        assert first_item is not None
        assert second_item is not None
        assert third_item is not None
        assert first_item.status == "broker_order_submitted"
        assert second_item.status == "target_sell_triggered_pending_dispatch"
        assert third_item.status == "broker_order_submitted"

    engine.dispose()


def test_monitoring_evaluate_limits_loaded_symbols_per_cycle() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-monitor-cap"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_symbols_per_cycle=2,
            monitoring_max_broker_actions_per_cycle=8,
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-shared-monitor-runtime-symbol-cap",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "90000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "035420",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "180000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    first_item_id, second_item_id, third_item_id = plan_response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add_all(
            [
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("120000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=1,
                    average_price=Decimal("88000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="035420",
                    quantity=1,
                    average_price=Decimal("175000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                ),
            ]
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "shared symbol scheduler should cap distinct symbols per cycle",
            "prices": [
                {
                    "symbol": "005930",
                    "price": "125100.00",
                },
                {
                    "symbol": "000660",
                    "price": "90100.00",
                },
                {
                    "symbol": "035420",
                    "price": "180100.00",
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["evaluated_item_count"] == 2
    assert payload["triggered_item_count"] == 2
    assert payload["submitted_order_count"] == 2
    assert payload["blocked_item_count"] == 0
    assert payload["deferred_item_count"] == 0
    assert [item["execution_plan_item_id"] for item in payload["items"]] == [
        first_item_id,
        second_item_id,
    ]
    assert [item["outcome"] for item in payload["items"]] == ["submitted", "submitted"]

    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 2
    assert [call.execution_plan_item_id for call in broker_adapter.order_calls] == [
        first_item_id,
        second_item_id,
    ]

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    monitoring = runtime_status.json()["liveness"]["monitoring"]
    assert monitoring["scheduler"]["max_symbols_per_cycle"] == 2
    assert monitoring["last_cycle"]["price_count"] == 3
    assert monitoring["last_cycle"]["evaluated_symbol_count"] == 2
    assert monitoring["last_cycle"]["triggered_symbol_count"] == 2

    with app.state.db_session_factory() as session:
        first_item = session.get(ExecutionPlanItem, first_item_id)
        second_item = session.get(ExecutionPlanItem, second_item_id)
        third_item = session.get(ExecutionPlanItem, third_item_id)
        assert first_item is not None
        assert second_item is not None
        assert third_item is not None
        assert first_item.status == "broker_order_submitted"
        assert second_item.status == "broker_order_submitted"
        assert third_item.status == "target_sell_monitoring"

        third_item_order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == third_item_id)
        )
        assert third_item_order is None

    engine.dispose()


def test_place_order_endpoint_is_idempotent() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    payload = {
        "idempotency_key": "repeat-order-1",
        "symbol": "005930",
        "side": "buy",
        "quantity": 5,
        "order_type": "limit",
        "limit_price": "71000.00",
        "time_in_force": "day",
        "exchange": "KRX",
    }

    first = client.post("/orders", json=payload)
    second = client.post("/orders", json=payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1

    with app.state.db_session_factory() as session:
        orders = session.scalars(select(BrokerOrder)).all()
        assert len(orders) == 1

    engine.dispose()


def test_place_order_idempotency_keys_are_scoped_to_runtime_mode(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'cross-mode-idempotency.db'}"
    paper_app, paper_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url=KIS_PAPER_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(broker_order_id="paper-order-100"),
        ),
    )
    live_app, live_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="live",
            kis_app_key="live-key",
            kis_app_secret="live-secret",
            kis_account_number="87654321",
        ),
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="live",
                configured=True,
                api_base_url=KIS_LIVE_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(broker_order_id="live-order-100"),
        ),
    )
    paper_client = TestClient(paper_app)
    live_client = TestClient(live_app)

    for client in (paper_client, live_client):
        client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable automation"},
        )

    payload = {
        "idempotency_key": "cross-mode-repeat-order-1",
        "symbol": "005930",
        "side": "buy",
        "quantity": 5,
        "order_type": "limit",
        "limit_price": "71000.00",
        "time_in_force": "day",
        "exchange": "KRX",
    }

    paper_response = paper_client.post("/orders", json=payload)
    live_response = live_client.post("/orders", json=payload)

    assert paper_response.status_code == 201
    assert live_response.status_code == 201
    assert paper_response.json()["duplicate"] is False
    assert live_response.json()["duplicate"] is False
    assert paper_response.json()["order"]["broker_order_id"] == "paper-order-100"
    assert live_response.json()["order"]["broker_order_id"] == "live-order-100"

    assert [order["broker_order_id"] for order in paper_client.get("/orders").json()] == [
        "paper-order-100"
    ]
    assert [order["broker_order_id"] for order in live_client.get("/orders").json()] == [
        "live-order-100"
    ]

    with paper_app.state.db_session_factory() as session:
        orders = session.scalars(
            select(BrokerOrder)
            .where(BrokerOrder.idempotency_key == payload["idempotency_key"])
            .order_by(BrokerOrder.mode.asc())
        ).all()
        assert [order.mode for order in orders] == ["live", "paper"]

    paper_engine.dispose()
    live_engine.dispose()


def test_place_order_execution_plan_item_ids_are_scoped_to_runtime_mode(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'cross-mode-plan-item.db'}"
    paper_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="paper-order-200"),
    )
    live_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="live",
            configured=True,
            api_base_url=KIS_LIVE_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="live-order-200"),
    )
    paper_app, paper_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=paper_adapter,
    )
    live_app, live_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="live",
            kis_app_key="live-key",
            kis_app_secret="live-secret",
            kis_account_number="87654321",
        ),
        broker_adapter=live_adapter,
    )
    paper_client = TestClient(paper_app)
    live_client = TestClient(live_app)

    paper_client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable paper automation"},
    )

    live_plan = live_client.post(
        "/execution-plans",
        json={
            "plan_id": "live-plan-foreign-item-1",
            "source": "signal-decider",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "manual_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                }
            ],
        },
    )
    assert live_plan.status_code == 201
    live_plan_item_id = live_plan.json()["persisted_execution_item_ids"][0]

    response = paper_client.post(
        "/orders",
        json={
            "idempotency_key": "paper-order-with-live-item-1",
            "execution_plan_item_id": live_plan_item_id,
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert response.status_code == 404
    assert response.json() == {
        "message": "execution_plan_item_id was not found",
        "execution_plan_item_id": live_plan_item_id,
    }
    assert paper_adapter.authenticate_calls == 0
    assert len(paper_adapter.order_calls) == 0

    with paper_app.state.db_session_factory() as session:
        orders = session.scalars(select(BrokerOrder)).all()
        assert orders == []

    paper_engine.dispose()
    live_engine.dispose()


@pytest.mark.parametrize(
    ("control_path", "reason"),
    [
        ("/controls/pause", "pause after submission"),
        ("/controls/kill-switch", "kill switch after submission"),
    ],
)
def test_place_order_endpoint_returns_existing_order_when_runtime_is_blocked(
    control_path: str,
    reason: str,
) -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    payload = {
        "idempotency_key": "repeat-order-after-pause-1",
        "symbol": "005930",
        "side": "buy",
        "quantity": 5,
        "order_type": "limit",
        "limit_price": "71000.00",
        "time_in_force": "day",
        "exchange": "KRX",
    }

    first = client.post("/orders", json=payload)
    pause = client.post(
        control_path,
        json={"actor": "ops", "source": "console", "reason": reason},
    )
    second = client.post("/orders", json=payload)

    assert first.status_code == 201
    assert pause.status_code == 200
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["order"]["idempotency_key"] == payload["idempotency_key"]
    assert broker_adapter.authenticate_calls == 1
    assert len(broker_adapter.order_calls) == 1

    with app.state.db_session_factory() as session:
        orders = session.scalars(select(BrokerOrder)).all()
        assert len(orders) == 1

    engine.dispose()


def test_place_order_endpoint_persists_retryable_failure() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "rate limited by broker",
            status_code=503,
            classification=BrokerErrorClassification.RATE_LIMIT,
            retryable=True,
            code="RATE_LIMIT",
            normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            response_payload={"msg_cd": "RATE_LIMIT", "msg1": "rate limited by broker"},
            observed_at=datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "retry-order-1",
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert response.status_code == 503
    payload = response.json()
    assert payload["order"]["status"] == "retry_pending"
    assert payload["order"]["last_error_classification"] == "rate_limit"
    assert payload["order"]["last_error_retryable"] is True
    assert {event["event_type"] for event in payload["order"]["events"]} == {
        "submit_requested",
        "submit_failed",
    }

    with app.state.db_session_factory() as session:
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.idempotency_key == "retry-order-1")
        )
        assert order is not None
        assert order.status == "retry_pending"
        assert order.last_error_code == "RATE_LIMIT"

    engine.dispose()


def test_dispatch_due_next_open_buy_job_persists_insufficient_funds_execution_failure() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "insufficient cash available for order",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            retryable=False,
            code="INSUFFICIENT_CASH",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={
                "msg_cd": "INSUFFICIENT_CASH",
                "msg1": "insufficient cash available for order",
            },
            observed_at=datetime(2026, 5, 1, 9, 5, tzinfo=UTC),
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-insufficient-funds-1",
            "source": "kai-trading-core",
            "strategy_run_id": "run-insufficient-funds-1",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                    "risk_context": {"agent_name": "signal-decider"},
                    "decision_snapshot_id": "decision-1",
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    dispatch_response = client.post("/execution-jobs/dispatch")
    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["completed_job_count"] == 0
    assert dispatch_payload["retrying_job_count"] == 0
    assert dispatch_payload["failed_job_count"] == 1

    failures_response = client.get(
        "/execution-failures",
        params={"classification": "insufficient_funds", "status": "open"},
    )
    assert failures_response.status_code == 200
    failures_payload = failures_response.json()
    assert len(failures_payload) == 1
    failure = failures_payload[0]
    assert failure["classification"] == "insufficient_funds"
    assert failure["status"] == "open"
    assert failure["execution_plan_item_id"] == plan_item_id
    assert failure["plan_id"] == "plan-insufficient-funds-1"
    assert failure["strategy_run_id"] == "run-insufficient-funds-1"
    assert failure["decision_actor"] == "signal-decider"
    assert failure["symbol"] == "005930"
    assert failure["side"] == "buy"
    assert failure["intent"] == "next_open_buy"
    assert failure["account"] == "****5678"
    assert failure["requested_quantity"] == 5
    assert failure["requested_notional"] == "355000.0000"
    assert failure["broker_error_code"] == "INSUFFICIENT_CASH"
    assert failure["broker_error_message"] == "insufficient cash available for order"
    assert failure["retryable"] is False
    assert failure["first_failed_at"] == "2026-05-01T09:05:00"
    assert failure["latest_failed_at"] == "2026-05-01T09:05:00"

    with app.state.db_session_factory() as session:
        failures = session.scalars(select(ExecutionFailure)).all()
        assert len(failures) == 1
        assert failures[0].classification == "insufficient_funds"
        assert failures[0].status == "open"

    engine.dispose()


def test_monitoring_evaluate_persists_insufficient_sellable_quantity_execution_failure() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "insufficient sellable quantity",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            retryable=False,
            code="INSUFFICIENT_SELLABLE_QTY",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={
                "msg_cd": "INSUFFICIENT_SELLABLE_QTY",
                "msg1": "insufficient sellable quantity",
            },
            observed_at=datetime(2026, 5, 1, 9, 7, tzinfo=UTC),
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-insufficient-sellable-qty-1",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "trigger_price": "70000.00",
                    "limit_price": "69950.00",
                    "time_in_force": "day",
                    "risk_context": {"agent_name": "stop-guard"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=10,
                average_price=Decimal("70950.5000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "sellable quantity rejection",
            "prices": [{"symbol": "005930", "price": "69900.00"}],
        },
    )
    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 0

    failures_response = client.get(
        "/execution-failures",
        params={"classification": "insufficient_sellable_quantity", "status": "open"},
    )
    assert failures_response.status_code == 200
    failures_payload = failures_response.json()
    assert len(failures_payload) == 1
    failure = failures_payload[0]
    assert failure["classification"] == "insufficient_sellable_quantity"
    assert failure["execution_plan_item_id"] == plan_item_id
    assert failure["decision_actor"] == "stop-guard"
    assert failure["symbol"] == "005930"
    assert failure["side"] == "sell"
    assert failure["intent"] == "stop_sell"
    assert failure["requested_quantity"] == 3
    assert failure["requested_notional"] == "209850.0000"
    assert failure["broker_error_code"] == "INSUFFICIENT_SELLABLE_QTY"

    engine.dispose()


def test_execution_failures_endpoint_filters_resolved_status() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "insufficient buying power for order",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            retryable=False,
            code="INSUFFICIENT_BUYING_POWER",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={
                "msg_cd": "INSUFFICIENT_BUYING_POWER",
                "msg1": "insufficient buying power for order",
            },
            observed_at=datetime(2026, 5, 1, 9, 10, tzinfo=UTC),
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "insufficient-buying-power-1",
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )
    assert response.status_code == 422

    failures_response = client.get("/execution-failures", params={"status": "open"})
    assert failures_response.status_code == 200
    failures_payload = failures_response.json()
    assert len(failures_payload) == 1
    failure_id = failures_payload[0]["execution_failure_id"]

    patch_response = client.patch(
        f"/execution-failures/{failure_id}",
        json={
            "status": "resolved",
            "actor": "ops",
            "source": "console",
            "reason": "cash transfer completed",
        },
    )
    assert patch_response.status_code == 200
    patched_payload = patch_response.json()
    assert patched_payload["status"] == "resolved"
    assert patched_payload["status_updated_by"] == "ops"
    assert patched_payload["status_updated_source"] == "console"
    assert patched_payload["status_updated_reason"] == "cash transfer completed"

    open_failures = client.get("/execution-failures", params={"status": "open"})
    resolved_failures = client.get("/execution-failures", params={"status": "resolved"})
    assert open_failures.status_code == 200
    assert resolved_failures.status_code == 200
    assert open_failures.json() == []
    assert [item["execution_failure_id"] for item in resolved_failures.json()] == [failure_id]

    engine.dispose()


def test_execution_failures_ignore_retryable_and_duplicate_replays() -> None:
    rate_limit_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "rate limited by broker",
            status_code=503,
            classification=BrokerErrorClassification.RATE_LIMIT,
            retryable=True,
            code="RATE_LIMIT",
            normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
            response_payload={"msg_cd": "RATE_LIMIT", "msg1": "rate limited by broker"},
            observed_at=datetime(2026, 5, 1, 9, 12, tzinfo=UTC),
        ),
    )
    rate_limit_app, rate_limit_engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=rate_limit_adapter,
    )
    rate_limit_client = TestClient(rate_limit_app)

    rate_limit_client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    rate_limit_response = rate_limit_client.post(
        "/orders",
        json={
            "idempotency_key": "retry-order-2",
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )
    assert rate_limit_response.status_code == 503
    assert rate_limit_client.get("/execution-failures").json() == []

    rate_limit_client.close()
    rate_limit_engine.dispose()

    insufficient_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_error=BrokerAdapterError(
            "insufficient cash available for order",
            status_code=422,
            classification=BrokerErrorClassification.BROKER_REJECTED,
            retryable=False,
            code="INSUFFICIENT_CASH",
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
            response_payload={
                "msg_cd": "INSUFFICIENT_CASH",
                "msg1": "insufficient cash available for order",
            },
            observed_at=datetime(2026, 5, 1, 9, 13, tzinfo=UTC),
        ),
    )
    insufficient_app, insufficient_engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=insufficient_adapter,
    )
    insufficient_client = TestClient(insufficient_app)

    insufficient_client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    payload = {
        "idempotency_key": "insufficient-duplicate-1",
        "symbol": "005930",
        "side": "buy",
        "quantity": 5,
        "order_type": "limit",
        "limit_price": "71000.00",
        "time_in_force": "day",
        "exchange": "KRX",
    }
    first = insufficient_client.post("/orders", json=payload)
    second = insufficient_client.post("/orders", json=payload)

    assert first.status_code == 422
    assert second.status_code == 200

    failures_payload = insufficient_client.get("/execution-failures").json()
    assert len(failures_payload) == 1
    assert failures_payload[0]["classification"] == "insufficient_funds"

    with insufficient_app.state.db_session_factory() as session:
        assert session.query(ExecutionFailure).count() == 1

    insufficient_client.close()
    insufficient_engine.dispose()


def test_execution_failures_persist_after_restart(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'execution-failures-restart.db'}"
    settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
    )
    app, engine = build_test_app(
        settings=settings,
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url=KIS_PAPER_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_error=BrokerAdapterError(
                "insufficient cash available for order",
                status_code=422,
                classification=BrokerErrorClassification.BROKER_REJECTED,
                retryable=False,
                code="INSUFFICIENT_CASH",
                normalized_status=NormalizedBrokerOrderStatus.REJECTED,
                response_payload={
                    "msg_cd": "INSUFFICIENT_CASH",
                    "msg1": "insufficient cash available for order",
                },
                observed_at=datetime(2026, 5, 1, 9, 15, tzinfo=UTC),
            ),
        ),
    )
    client = TestClient(app)

    client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable automation"},
    )
    response = client.post(
        "/orders",
        json={
            "idempotency_key": "restart-insufficient-funds-1",
            "symbol": "005930",
            "side": "buy",
            "quantity": 5,
            "order_type": "limit",
            "limit_price": "71000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )
    assert response.status_code == 422

    client.close()
    engine.dispose()

    restarted_app, restarted_engine = build_test_app(
        settings=settings,
        broker_adapter=StubBrokerAdapter(
            config=BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url=KIS_PAPER_API_BASE_URL,
            ),
            token_grant=build_token_grant(),
            order_placement=build_order_placement(),
        ),
    )
    restarted_client = TestClient(restarted_app)

    failures_response = restarted_client.get(
        "/execution-failures",
        params={"classification": "insufficient_funds", "status": "open"},
    )
    assert failures_response.status_code == 200
    failures_payload = failures_response.json()
    assert len(failures_payload) == 1
    assert failures_payload[0]["idempotency_key"] == "restart-insufficient-funds-1"

    restarted_client.close()
    restarted_engine.dispose()


def test_live_mode_cycle_exposes_symbol_readback_and_daily_board_views() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="live",
            configured=True,
            api_base_url=KIS_LIVE_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-live-cycle-1"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
            kis_app_key="live-key",
            kis_app_secret="live-secret",
            kis_account_number="12345678",
        ),
        broker_adapter=broker_adapter,
    )
    client = TestClient(app)

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable guarded live validation"},
    )
    assert resume.status_code == 200

    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-readback-cycle",
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "trigger_price": "76000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    buy_item_id, monitor_item_id = plan_response.json()["persisted_execution_item_ids"]

    dispatch_response = client.post("/execution-jobs/dispatch")
    assert dispatch_response.status_code == 200
    dispatch_payload = dispatch_response.json()
    assert dispatch_payload["completed_job_count"] == 1
    assert dispatch_payload["jobs"][0]["execution_plan_item_id"] == buy_item_id
    assert dispatch_payload["jobs"][0]["status"] == "completed"

    with app.state.db_session_factory() as session:
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.execution_plan_item_id == buy_item_id)
        )
        assert order is not None
        session.add(
            ExecutionFill(
                broker_order_pk=order.id,
                broker_fill_id="fill-live-cycle-1",
                symbol="005930",
                side="buy",
                quantity=3,
                price=Decimal("71000.0000"),
                filled_at=datetime(2026, 4, 30, 9, 5, tzinfo=UTC),
                payload={"source": "broker"},
            )
        )
        session.add(
            ExecutionPosition(
                mode="live",
                symbol="005930",
                quantity=3,
                average_price=Decimal("71000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    plans_payload = client.get("/execution-plans").json()
    orders_payload = client.get("/orders").json()
    executions_payload = client.get("/executions").json()
    positions_payload = client.get("/positions").json()
    symbols_payload = client.get("/symbols").json()
    symbol_detail_payload = client.get("/symbols/005930").json()
    runtime_status_payload = client.get("/runtime/status").json()
    daily_board_payload = client.get("/runtime/daily-board").json()

    assert len(plans_payload) == 1
    assert plans_payload[0]["mode"] == "live"
    assert plans_payload[0]["item_count"] == 2
    assert len(orders_payload) == 1
    assert orders_payload[0]["broker_order_id"] == "kis-live-cycle-1"
    assert len(executions_payload) == 1
    assert executions_payload[0]["broker_fill_id"] == "fill-live-cycle-1"
    assert len(positions_payload) == 1
    assert positions_payload[0]["symbol"] == "005930"
    assert positions_payload[0]["quantity"] == 3

    assert len(symbols_payload) == 1
    assert symbols_payload[0]["symbol"] == "005930"
    assert symbols_payload[0]["monitored"] is True
    assert symbols_payload[0]["plan_item_count"] == 2
    assert symbols_payload[0]["order_count"] == 1
    assert symbols_payload[0]["fill_count"] == 1
    assert symbols_payload[0]["position_quantity"] == 3
    assert symbols_payload[0]["monitoring_statuses"] == ["target_sell_monitoring"]

    assert symbol_detail_payload["symbol"] == "005930"
    assert symbol_detail_payload["mode"] == "live"
    assert symbol_detail_payload["summary"]["monitored"] is True
    assert symbol_detail_payload["summary"]["fill_count"] == 1
    assert symbol_detail_payload["position"]["quantity"] == 3
    assert len(symbol_detail_payload["plan_items"]) == 2
    assert len(symbol_detail_payload["orders"]) == 1
    assert len(symbol_detail_payload["fills"]) == 1
    assert {item["execution_plan_item_id"] for item in symbol_detail_payload["plan_items"]} == {
        buy_item_id,
        monitor_item_id,
    }

    assert runtime_status_payload["mode"] == "live"
    assert runtime_status_payload["broker"]["status"] == "active"
    assert runtime_status_payload["symbols"]["monitored"] == ["005930"]
    assert runtime_status_payload["counts"]["open_orders"] == 1
    assert runtime_status_payload["counts"]["active_execution_jobs"] == 0

    assert daily_board_payload["mode"] == "live"
    assert daily_board_payload["runtime"]["mode"] == "live"
    assert daily_board_payload["totals"] == {
        "symbol_count": 1,
        "monitored_symbol_count": 1,
        "open_order_symbol_count": 1,
        "filled_symbol_count": 1,
        "position_symbol_count": 1,
    }
    assert len(daily_board_payload["symbols"]) == 1
    assert daily_board_payload["symbols"][0]["symbol"] == "005930"

    missing_symbol_response = client.get("/symbols/000660")
    assert missing_symbol_response.status_code == 404

    engine.dispose()


def test_autonomous_dispatch_loop_progresses_due_plan_without_manual_dispatch() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-auto-dispatch-1"),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            dispatch_loop_interval_seconds=0.05,
            monitoring_loop_interval_seconds=60,
        ),
        broker_adapter=broker_adapter,
        autonomous_runtime_enabled=True,
    )
    with TestClient(app) as client:
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable autonomous runtime"},
        )
        assert resume.status_code == 200

        plan_response = client.post(
            "/execution-plans",
            json={
                "plan_id": "plan-autonomous-dispatch",
                "source": "kai-trading-core",
                "market_date": "2024-01-03",
                "items": [
                    {
                        "symbol": "005930",
                        "side": "buy",
                        "intent": "next_open_buy",
                        "quantity_policy": {"kind": "shares", "quantity": 5},
                        "limit_price": "71000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    }
                ],
            },
        )
        assert plan_response.status_code == 201
        plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

        wait_for(lambda: len(broker_adapter.order_calls) == 1)

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        autonomous = runtime_status.json()["liveness"]["autonomous"]
        assert autonomous["enabled"] is True
        assert autonomous["dispatch"]["status"] == "running"
        assert autonomous["dispatch"]["last_success_at"] is not None
        assert autonomous["dispatch"]["last_cycle"] is not None

        with app.state.db_session_factory() as session:
            job = session.scalar(select(ExecutionJob))
            plan_item = session.get(ExecutionPlanItem, plan_item_id)
            order = session.scalar(select(BrokerOrder))
            assert job is not None
            assert plan_item is not None
            assert order is not None
            assert job.status == "completed"
            assert plan_item.status == "broker_order_submitted"
            assert order.broker_order_id == "kis-auto-dispatch-1"

    engine.dispose()


def test_autonomous_monitoring_loop_triggers_target_sell_from_price_source() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-auto-monitor-1"),
    )
    price_source = StaticMonitoringPriceSource({"005930": Decimal("125100.00")})
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            dispatch_loop_interval_seconds=60,
            monitoring_loop_interval_seconds=0.05,
        ),
        broker_adapter=broker_adapter,
        monitoring_price_source=price_source,
        autonomous_runtime_enabled=True,
    )
    with TestClient(app) as client:
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable autonomous runtime"},
        )
        assert resume.status_code == 200

        plan_response = client.post(
            "/execution-plans",
            json={
                "plan_id": "plan-autonomous-monitoring",
                "source": "kai-trading-core",
                "items": [
                    {
                        "symbol": "005930",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "125000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    }
                ],
            },
        )
        assert plan_response.status_code == 201
        plan_item_id = plan_response.json()["persisted_execution_item_ids"][0]

        with app.state.db_session_factory() as session:
            session.add(
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("120000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                )
            )
            session.commit()

        wait_for(lambda: len(broker_adapter.order_calls) == 1)

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        payload = runtime_status.json()
        assert payload["liveness"]["monitoring"]["status"] == "running"
        autonomous = payload["liveness"]["autonomous"]["monitoring"]
        assert autonomous["status"] == "running"
        assert autonomous["price_source_configured"] is True
        assert autonomous["last_success_at"] is not None
        assert price_source.calls

        with app.state.db_session_factory() as session:
            plan_item = session.get(ExecutionPlanItem, plan_item_id)
            order = session.scalar(select(BrokerOrder))
            checkpoint = session.scalar(select(MonitoringCheckpoint))
            assert plan_item is not None
            assert order is not None
            assert checkpoint is not None
            assert plan_item.status == "broker_order_submitted"
            assert order.broker_order_id == "kis-auto-monitor-1"
            assert checkpoint.status == "running"

    engine.dispose()


def test_autonomous_monitoring_loop_reports_degraded_status_when_live_source_fails() -> None:
    broker_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        current_price_error=BrokerAdapterError(
            "quote feed unavailable",
            status_code=503,
            classification=BrokerErrorClassification.NETWORK,
            retryable=True,
        ),
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            dispatch_loop_interval_seconds=60,
            monitoring_loop_interval_seconds=0.05,
        ),
        broker_adapter=broker_adapter,
        autonomous_runtime_enabled=True,
    )
    with TestClient(app) as client:
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable autonomous runtime"},
        )
        assert resume.status_code == 200

        plan_response = client.post(
            "/execution-plans",
            json={
                "plan_id": "plan-autonomous-monitoring-failure",
                "source": "kai-trading-core",
                "items": [
                    {
                        "symbol": "005930",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "125000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    }
                ],
            },
        )
        assert plan_response.status_code == 201

        with app.state.db_session_factory() as session:
            session.add(
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("120000.0000"),
                    updated_from="reconciliation",
                    raw_payload={"source": "broker"},
                )
            )
            session.commit()

        wait_for(
            lambda: client.get("/runtime/status").json()["liveness"]["autonomous"]["monitoring"]["price_source"]["status"]
            == "degraded"
        )

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        payload = runtime_status.json()
        assert payload["ready"] is False
        assert (
            "monitoring price source is degraded: monitoring price source request failed"
            in payload["readiness"]["reasons"]
        )
        assert payload["liveness"]["monitoring"]["status"] == "degraded"
        autonomous = payload["liveness"]["autonomous"]["monitoring"]
        assert autonomous["status"] == "running"
        assert autonomous["price_source_configured"] is True
        assert autonomous["price_source"]["status"] == "degraded"
        assert autonomous["price_source"]["last_error"] == "quote feed unavailable"

        with app.state.db_session_factory() as session:
            order = session.scalar(select(BrokerOrder))
            checkpoint = session.scalar(select(MonitoringCheckpoint))
            assert order is None
            assert checkpoint is not None
            assert checkpoint.details["price_count"] == 0

    engine.dispose()


def test_autonomous_monitoring_loop_retries_pending_dispatch_after_restart_with_no_quotes(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'autonomous-monitoring-restart.db'}"
    first_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-monitor-initial-1"),
    )
    first_app, first_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_broker_actions_per_cycle=1,
        ),
        broker_adapter=first_adapter,
    )
    first_client = TestClient(first_app)

    resume = first_client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "console", "reason": "enable monitoring"},
    )
    assert resume.status_code == 200

    plan_response = first_client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-monitoring-backlog-restart",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    first_item_id, second_item_id = plan_response.json()["persisted_execution_item_ids"]

    with first_app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=2,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    evaluation = first_client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "seed triggered monitoring backlog before restart",
            "prices": [{"symbol": "005930", "price": "125100.00"}],
        },
    )
    assert evaluation.status_code == 200
    assert evaluation.json()["submitted_order_count"] == 1
    assert evaluation.json()["deferred_item_count"] == 1

    with first_app.state.db_session_factory() as session:
        second_item = session.get(ExecutionPlanItem, second_item_id)
        assert second_item is not None
        assert second_item.status == "target_sell_triggered_pending_dispatch"

    first_client.close()
    first_engine.dispose()

    restarted_adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-monitor-restarted-2"),
    )
    restarted_app, restarted_engine = build_test_app(
        settings=Settings(
            database_url=database_url,
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_broker_actions_per_cycle=1,
            dispatch_loop_interval_seconds=60,
            monitoring_loop_interval_seconds=0.05,
        ),
        broker_adapter=restarted_adapter,
        autonomous_runtime_enabled=True,
    )
    with TestClient(restarted_app) as restarted_client:
        wait_for(lambda: len(restarted_adapter.order_calls) == 1)

        runtime_status = restarted_client.get("/runtime/status")
        assert runtime_status.status_code == 200
        payload = runtime_status.json()
        monitoring = payload["liveness"]["monitoring"]
        autonomous = payload["liveness"]["autonomous"]["monitoring"]
        assert payload["ready"] is False
        assert (
            "monitoring price source is degraded: price source returned no prices for monitored symbols"
            in payload["readiness"]["reasons"]
        )
        assert monitoring["status"] == "degraded"
        assert autonomous["status"] == "running"
        assert autonomous["last_cycle"] is not None
        assert autonomous["last_cycle"]["price_count"] == 0
        assert autonomous["price_source_configured"] is True
        assert autonomous["price_source"]["status"] == "degraded"
        assert (
            autonomous["price_source"]["reason"]
            == "price source returned no prices for monitored symbols"
        )

        with restarted_app.state.db_session_factory() as session:
            first_item = session.get(ExecutionPlanItem, first_item_id)
            second_item = session.get(ExecutionPlanItem, second_item_id)
            orders = session.scalars(select(BrokerOrder).order_by(BrokerOrder.created_at.asc())).all()
            checkpoint = session.scalar(select(MonitoringCheckpoint))
            assert first_item is not None
            assert second_item is not None
            assert checkpoint is not None
            assert first_item.status == "broker_order_submitted"
            assert second_item.status == "broker_order_submitted"
            assert [order.broker_order_id for order in orders] == [
                "kis-monitor-initial-1",
                "kis-monitor-restarted-2",
            ]
            assert checkpoint.details["retried_triggered_item_count"] >= 1

    restarted_engine.dispose()
