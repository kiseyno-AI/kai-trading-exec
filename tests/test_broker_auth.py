from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.broker.base import (
    BrokerAdapterError,
    BrokerConfig,
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
    ExecutionPlanItem,
)


class RecordingHttpClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

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
        auth_error: Exception | None = None,
        approval_error: Exception | None = None,
        order_error: Exception | None = None,
        revoke_error: Exception | None = None,
    ) -> None:
        self.config = config
        self.token_grant = token_grant
        self.approval = approval or BrokerRealtimeApproval(
            approval_key="approval-key",
            issued_at=datetime.now(UTC),
        )
        self.order_placement = order_placement
        self.auth_error = auth_error
        self.approval_error = approval_error
        self.order_error = order_error
        self.revoke_error = revoke_error
        self.authenticate_calls = 0
        self.approval_calls = 0
        self.revoke_calls: list[str] = []
        self.order_calls: list[BrokerOrderPlacementRequest] = []

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

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(
            supported=True,
            strategy="shared_realtime_subscription",
        )

    def reconcile_order(self, access_token: str, broker_order_id: str) -> dict[str, object]:
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


def build_test_app(
    *,
    settings: Settings,
    broker_adapter: StubBrokerAdapter | None = None,
):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=(
            (lambda _settings: broker_adapter)
            if broker_adapter is not None
            else None
        ),
    )
    return app, engine


def build_token_grant() -> BrokerTokenGrant:
    return BrokerTokenGrant(
        access_token="top-secret-token",
        token_type="Bearer",
        access_token_expires_at=datetime.fromisoformat("2026-05-30T08:10:10+09:00"),
    )


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
        access_token_expires_at=datetime.fromisoformat("2026-05-30T08:10:10+09:00"),
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
        access_token_expires_at=datetime.fromisoformat("2026-05-30T08:10:10+09:00"),
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
        access_token_expires_at=datetime.fromisoformat("2026-05-30T08:10:10+09:00"),
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
    assert [event["event_type"] for event in payload["order"]["events"]] == [
        "submit_requested",
        "submit_failed",
    ]

    with app.state.db_session_factory() as session:
        order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.idempotency_key == "retry-order-1")
        )
        assert order is not None
        assert order.status == "retry_pending"
        assert order.last_error_code == "RATE_LIMIT"

    engine.dispose()
