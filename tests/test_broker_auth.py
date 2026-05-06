from datetime import UTC, datetime, timedelta
from decimal import Decimal
import time

from fastapi.testclient import TestClient
import pytest
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
    ExecutionFailure,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
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


def test_short_term_trigger_and_chase_buy_items_are_safely_blocked() -> None:
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
            ],
        },
    )
    assert response.status_code == 201

    plans_payload = client.get("/execution-plans").json()
    items_by_source = {
        item["source_plan_item_id"]: item
        for item in plans_payload[0]["items"]
    }
    assert items_by_source["core-tactical-entry-1"]["status"] == "unsupported_semantics"
    assert items_by_source["core-trigger-chase-entry-1"]["status"] == "pending_configuration"

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


def test_autonomous_monitoring_loop_retries_pending_dispatch_after_restart_without_price_source(
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
        monitoring = runtime_status.json()["liveness"]["monitoring"]
        autonomous = runtime_status.json()["liveness"]["autonomous"]["monitoring"]
        assert monitoring["status"] == "running"
        assert autonomous["status"] == "running"
        assert autonomous["last_cycle"] is not None
        assert autonomous["last_cycle"]["price_count"] == 0

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
