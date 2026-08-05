import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import inspect, select, text

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models import EXEC_OWNED_TABLES
from app.models.runtime import (
    BrokerOrder,
    ExecutionJob,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    RuntimeControlEvent,
    RuntimeControlState,
)
from app.services import runtime as runtime_service, runtime_controls
from app.services.autonomous_runtime import MonitoringRuntimeSnapshot, _is_regular_market_session
from app.services.monitoring import (
    _maintain_post_entry_exit_monitors,
    _load_monitored_items,
    _monitoring_item_is_configured,
    _pending_broker_order_symbols,
    _resolve_sell_quantity,
    list_autonomous_monitoring_symbols,
    run_autonomous_monitoring_cycle,
)


def build_test_app(
    settings: Settings | None = None,
    *,
    autonomous_runtime_enabled: bool = False,
    monitoring_price_source: object | None = None,
    broker_adapter_factory: object | None = None,
):
    app_settings = settings or Settings(database_url="sqlite:///:memory:")
    app_settings = app_settings.model_copy(
        update={"autonomous_runtime_enabled": autonomous_runtime_enabled}
    )
    engine = create_db_engine(app_settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=app_settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=broker_adapter_factory,
        monitoring_price_source=monitoring_price_source,
    )
    return app, engine


def test_monitoring_symbol_cycle_limit_auto_uses_timeout_budget() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_MONITORING_MAX_SYMBOLS_PER_CYCLE": "auto",
            "KAI_EXEC_KIS_REQUEST_TIMEOUT_SECONDS": "3",
            "KAI_EXEC_MONITORING_AUTO_SYMBOL_BATCH_TARGET_SECONDS": "12",
        }
    )

    assert settings.monitoring_max_symbols_per_cycle == 12


def test_watch_buy_decimal_string_share_quantity_is_configured() -> None:
    item = ExecutionPlanItem(
        id="watch-decimal-string-quantity",
        execution_plan_id="plan-1",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="watch_monitoring_pending",
        trigger_price=Decimal("71000"),
        limit_price=Decimal("71500"),
        quantity_policy={"kind": "shares", "quantity": "24.00000000"},
    )

    assert _monitoring_item_is_configured(item)


class StubMonitoringBrokerAdapter:
    provider = "kis"

    def __init__(self, *, account_snapshot: BrokerAccountSnapshot | None = None) -> None:
        self.order_calls: list[BrokerOrderPlacementRequest] = []
        self.account_snapshot = account_snapshot
        self.account_snapshot_calls = 0

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=None,
        )

    def authenticate(self) -> BrokerTokenGrant:
        return BrokerTokenGrant(
            access_token="test-token",
            token_type="Bearer",
            access_token_expires_at=datetime(2026, 5, 29, tzinfo=UTC),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        assert access_token
        return BrokerRealtimeApproval(
            approval_key="test-approval",
            issued_at=datetime(2026, 5, 28, tzinfo=UTC),
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        assert access_token
        return BrokerTokenRevocation(message="revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        assert access_token
        self.order_calls.append(request)
        return BrokerOrderPlacement(
            broker_order_id=f"kis-{len(self.order_calls)}",
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="submitted",
            submitted_at=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
            observed_at=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
            request_metadata={"source": "stub"},
            response_metadata={"source": "stub"},
            request_payload={"symbol": request.symbol},
            response_payload={"broker_order_id": f"kis-{len(self.order_calls)}"},
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        assert access_token
        self.account_snapshot_calls += 1
        if self.account_snapshot is not None:
            return self.account_snapshot
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime(2026, 5, 28, tzinfo=UTC),
            source="stub",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
    ) -> dict[str, object] | None:
        assert access_token
        assert broker_order_id
        return None

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        assert access_token
        return {}


def build_unmigrated_runtime_safety_app(
    database_url: str,
    *,
    autonomous_runtime_enabled: bool = False,
):
    settings = Settings(
        database_url=database_url,
        autonomous_runtime_enabled=autonomous_runtime_enabled,
    )
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE runtime_control_state (
                    id VARCHAR(32) PRIMARY KEY NOT NULL,
                    mode VARCHAR(16) NOT NULL,
                    paused BOOLEAN NOT NULL,
                    kill_switch_enabled BOOLEAN NOT NULL,
                    automation_enabled BOOLEAN NOT NULL,
                    readiness_reason TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO runtime_control_state (
                    id,
                    mode,
                    paused,
                    kill_switch_enabled,
                    automation_enabled,
                    readiness_reason
                )
                VALUES (
                    'global',
                    'mock',
                    1,
                    0,
                    0,
                    'initialized fail-closed until operator controls are enabled'
                )
                """
            )
        )
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
    )
    return app, engine


def test_settings_load_from_environment() -> None:
    settings = load_settings(
        {
            "KAI_EXEC_ENVIRONMENT": "test",
            "KAI_EXEC_RUNTIME_MODE": "paper",
            "KAI_EXEC_DATABASE_URL": "sqlite:////tmp/kai-exec-test.db",
            "KAI_EXEC_API_PORT": "8022",
            "KAI_EXEC_AUTOMATION_ENABLED": "true",
            "KAI_EXEC_LOG_LEVEL": "debug",
        },
    )

    assert settings.environment == "test"
    assert settings.runtime_mode == "paper"
    assert settings.database_url == "sqlite:////tmp/kai-exec-test.db"
    assert settings.api_port == 8022
    assert settings.automation_enabled is True
    assert settings.log_level == "DEBUG"


def test_runtime_status_exposes_sqlite_contention_diagnostics() -> None:
    app, _engine = build_test_app()
    client = TestClient(app)

    payload = client.get("/runtime/status").json()

    assert payload["diagnostics"]["sqlite"]["lock_error_count"] >= 0
    assert payload["diagnostics"]["sqlite"]["write_retry_count"] >= 0
    assert "last_lock_error" in payload["diagnostics"]["sqlite"]


def test_autonomous_runtime_stays_opt_in_by_default() -> None:
    default_settings = load_settings({})
    assert default_settings.autonomous_runtime_enabled is False

    opted_in_settings = load_settings(
        {
            "KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED": "true",
        }
    )
    assert opted_in_settings.autonomous_runtime_enabled is True


def test_health_returns_dependency_aware_payload() -> None:
    app, engine = build_test_app()
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "kai-trading-exec"
    assert payload["dependencies"]["database"]["status"] == "ok"
    assert payload["dependencies"]["runtime_schema"]["status"] == "ok"
    assert set(payload["dependencies"]["runtime_schema"]["owned_tables"]) == set(EXEC_OWNED_TABLES)
    assert payload["liveness"]["process_id"] > 0

    engine.dispose()


def test_runtime_metadata_reads_process_id_at_liveness_time(monkeypatch) -> None:
    runtime = runtime_service.RuntimeMetadata(started_at=datetime.now(UTC))

    monkeypatch.setattr(runtime_service.os, "getpid", lambda: 111)
    assert runtime_service.liveness_payload(runtime)["process_id"] == 111

    monkeypatch.setattr(runtime_service.os, "getpid", lambda: 222)
    assert runtime_service.liveness_payload(runtime)["process_id"] == 222


def test_runtime_status_reports_readiness_and_default_safety() -> None:
    app, engine = build_test_app()
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "mock"
    assert payload["ready"] is False
    assert "runtime is paused" in payload["readiness"]["reasons"]
    assert payload["safety"]["paused"] is True
    assert payload["safety"]["kill_switch_enabled"] is False
    assert payload["safety"]["automated_order_submission_allowed"] is False
    assert payload["safety"]["automated_order_submission_block_reason"] == "paused"
    assert payload["safety"]["last_change"]["action"] == "bootstrap"
    assert payload["counts"] == {"open_orders": 0, "active_execution_jobs": 0}
    assert payload["symbols"] == {
        "monitored": [],
        "scheduled_today": [],
        "filled_today": [],
        "filled_today_by_side": {"buy": [], "sell": []},
        "filled_today_counts": {
            "all": {"symbols": 0, "fills": 0, "quantity": 0},
            "buy": {"symbols": 0, "fills": 0, "quantity": 0},
            "sell": {"symbols": 0, "fills": 0, "quantity": 0},
        },
        "failed_today": [],
        "positions": [],
    }
    monitoring = payload["liveness"]["monitoring"]
    autonomous = payload["liveness"]["autonomous"]
    assert monitoring["status"] == "not_started"
    assert monitoring["topology"] == "shared_symbol_scheduler"
    assert monitoring["scheduler"]["selection_strategy"] == "round_robin_by_symbol"
    assert monitoring["scheduler"]["max_symbols_per_cycle"] == 128
    assert monitoring["dispatcher"]["max_broker_actions_per_cycle"] == 32
    assert monitoring["dispatcher"]["backlog_item_count"] == 0
    assert monitoring["last_cycle"] is None
    assert autonomous["enabled"] is False
    assert autonomous["dispatch"]["status"] == "disabled"
    assert autonomous["monitoring"]["status"] == "disabled"

    with app.state.db_session_factory() as session:
        state = session.get(RuntimeControlState, "global")
        assert state is not None
        assert state.paused is True
        assert state.last_transition_action == "bootstrap"

    engine.dispose()


def test_runtime_status_falls_back_to_autonomous_snapshot_when_database_pool_is_unavailable(
    monkeypatch,
) -> None:
    app, engine = build_test_app()
    app.state.autonomous_runtime._monitoring_state.status = "running"
    app.state.autonomous_runtime._monitoring_state.last_heartbeat_at = datetime(2026, 5, 29, 2, 20, tzinfo=UTC)
    app.state.autonomous_runtime._monitoring_state.last_success_at = datetime(2026, 5, 29, 2, 20, tzinfo=UTC)
    app.state.autonomous_runtime._monitoring_state.last_cycle = {
        "dispatch_action_count": 0,
        "backlog_item_count": 0,
        "backlog_symbols": [],
        "evaluated_symbol_count": 13,
    }
    monkeypatch.setattr(
        runtime_service,
        "ping_database",
        lambda _db: (_ for _ in ()).throw(runtime_service.SQLAlchemyError("pool exhausted")),
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["dependencies"]["database"] == "error"
    monitoring = payload["liveness"]["monitoring"]
    assert monitoring["source"] == "autonomous_runtime_snapshot"
    assert monitoring["status"] == "running"
    assert monitoring["last_cycle"]["evaluated_symbol_count"] == 13

    engine.dispose()


def test_create_app_injects_default_kis_monitoring_price_source_for_paper_runtime() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
        )
    )

    snapshot = app.state.autonomous_runtime.snapshot()

    assert snapshot["monitoring"]["price_source_configured"] is True
    assert snapshot["monitoring"]["price_source"]["source_name"] == "kis_current_quote_rest"
    assert snapshot["monitoring"]["price_source"]["provider"] == "kis"
    assert snapshot["monitoring"]["price_source"]["status"] == "configured"

    engine.dispose()


def test_runtime_status_reports_monthly_realized_pnl_progress_and_mode() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monthly_target_krw=Decimal("1000"),
    )
    app, engine = build_test_app(settings=settings)
    client = TestClient(app)
    now = datetime.now(UTC)

    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="monthly-buy-1",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=10,
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="monthly-sell-1",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=5,
        )
        session.add_all([buy_order, sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="monthly-buy-fill-1",
                    symbol="005930",
                    side="buy",
                    quantity=10,
                    price=Decimal("10000"),
                    filled_at=now - timedelta(days=1),
                ),
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="monthly-sell-fill-1",
                    symbol="005930",
                    side="sell",
                    quantity=5,
                    price=Decimal("11000"),
                    filled_at=now,
                ),
            ]
        )
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    monthly = response.json()["monthly_pnl"]
    assert monthly["monthly_realized_pnl"] == "5000.0000"
    assert monthly["monthly_target_krw"] == "1000.0000"
    assert monthly["target_progress_pct"] == "500.0000"
    assert monthly["mode_recommendation"] == "capital_preservation"
    assert monthly["matched_sell_quantity"] == 5
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] == "5000.0000"
    assert weekly["weekly_buy_notional"] == "100000.0000"
    assert weekly["weekly_sell_notional"] == "55000.0000"
    assert weekly["matched_sell_quantity"] == 5
    assert weekly["unmatched_sell_quantity"] == 0
    assert weekly["closed_trade_count"] == 1
    assert weekly["closed_symbols"] == ["005930"]
    assert weekly["closed_symbol_count"] == 1
    assert weekly["win_count"] == 1
    assert weekly["loss_count"] == 0
    assert weekly["pnl_method"] == "fifo_internal_fills"
    daily = response.json()["daily_pnl"]
    assert daily["daily_realized_pnl"] == "5000.0000"
    assert daily["daily_buy_notional"] == "0.0000"
    assert daily["daily_sell_notional"] == "55000.0000"
    assert daily["matched_sell_quantity"] == 5
    assert daily["unmatched_sell_quantity"] == 0
    assert daily["sold_symbols"] == ["005930"]
    assert daily["sold_symbol_count"] == 1
    assert daily["closed_symbols"] == ["005930"]
    assert daily["closed_symbol_count"] == 1
    assert daily["unmatched_sell_symbols"] == []
    assert daily["unmatched_sell_symbol_count"] == 0
    assert daily["pnl_method"] == "fifo_internal_fills"

    engine.dispose()


def test_runtime_status_daily_pnl_uses_crypto_base_quantity_and_quote_notional() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
        )
    )
    client = TestClient(app)
    now = datetime.now(UTC)
    buy_filled_at = now - timedelta(seconds=1)

    with create_session_factory(engine)() as session:
        buy_order = BrokerOrder(
            broker_name="upbit",
            mode="paper",
            idempotency_key="daily-crypto-buy-1",
            symbol="KRW-BTC",
            side="buy",
            status="filled",
            quantity=0,
            base_quantity=Decimal("0.001000000000"),
            quote_notional=Decimal("100000.0000"),
            asset_class="crypto",
            venue="upbit",
        )
        sell_order = BrokerOrder(
            broker_name="upbit",
            mode="paper",
            idempotency_key="daily-crypto-sell-1",
            symbol="KRW-BTC",
            side="sell",
            status="filled",
            quantity=0,
            base_quantity=Decimal("0.000400000000"),
            asset_class="crypto",
            venue="upbit",
        )
        session.add_all([buy_order, sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="daily-crypto-buy-fill-1",
                    symbol="KRW-BTC",
                    side="buy",
                    quantity=0,
                    base_quantity=Decimal("0.001000000000"),
                    quote_notional=Decimal("100000.0000"),
                    price=Decimal("100000000.0000"),
                    filled_at=buy_filled_at,
                ),
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="daily-crypto-sell-fill-1",
                    symbol="KRW-BTC",
                    side="sell",
                    quantity=0,
                    base_quantity=Decimal("0.000400000000"),
                    quote_notional=Decimal("44000.0000"),
                    price=Decimal("110000000.0000"),
                    filled_at=now,
                ),
            ]
        )
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    monthly = response.json()["monthly_pnl"]
    assert monthly["monthly_realized_pnl"] == "4000.0000"
    assert monthly["monthly_buy_notional"] == "100000.0000"
    assert monthly["monthly_sell_notional"] == "44000.0000"
    assert monthly["matched_sell_quantity"] == "0.0004"
    assert monthly["unmatched_sell_quantity"] == 0
    assert monthly["pnl_method"] == "fifo_internal_fills"
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] == "4000.0000"
    assert weekly["weekly_buy_notional"] == "100000.0000"
    assert weekly["weekly_sell_notional"] == "44000.0000"
    assert weekly["matched_sell_quantity"] == "0.0004"
    assert weekly["unmatched_sell_quantity"] == 0
    assert weekly["closed_symbols"] == ["KRW-BTC"]
    assert weekly["closed_symbol_count"] == 1
    assert weekly["pnl_method"] == "fifo_internal_fills"
    daily = response.json()["daily_pnl"]
    assert daily["daily_realized_pnl"] == "4000.0000"
    assert daily["daily_buy_notional"] == "100000.0000"
    assert daily["daily_sell_notional"] == "44000.0000"
    assert daily["matched_sell_quantity"] == "0.0004"
    assert daily["unmatched_sell_quantity"] == 0
    assert daily["sold_symbols"] == ["KRW-BTC"]
    assert daily["sold_symbol_count"] == 1
    assert daily["closed_symbols"] == ["KRW-BTC"]
    assert daily["closed_symbol_count"] == 1
    assert daily["unmatched_sell_symbols"] == []
    assert daily["unmatched_sell_symbol_count"] == 0
    assert daily["pnl_method"] == "fifo_internal_fills"

    engine.dispose()


def test_runtime_status_daily_pnl_surfaces_unmatched_sell_coverage() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
        )
    )
    client = TestClient(app)
    now = datetime.now(UTC)

    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="daily-coverage-buy-1",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=2,
        )
        matched_sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="daily-coverage-sell-matched-1",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=1,
        )
        unmatched_sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="daily-coverage-sell-unmatched-1",
            symbol="003280",
            side="sell",
            status="filled",
            quantity=150,
        )
        session.add_all([buy_order, matched_sell_order, unmatched_sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="daily-coverage-buy-fill-1",
                    symbol="005930",
                    side="buy",
                    quantity=2,
                    price=Decimal("10000"),
                    filled_at=now - timedelta(days=1),
                ),
                ExecutionFill(
                    broker_order_pk=matched_sell_order.id,
                    broker_fill_id="daily-coverage-sell-fill-1",
                    symbol="005930",
                    side="sell",
                    quantity=1,
                    price=Decimal("11000"),
                    filled_at=now,
                ),
                ExecutionFill(
                    broker_order_pk=unmatched_sell_order.id,
                    broker_fill_id="daily-coverage-unmatched-sell-fill-1",
                    symbol="003280",
                    side="sell",
                    quantity=150,
                    price=Decimal("2020"),
                    filled_at=now,
                ),
            ]
        )
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    daily = response.json()["daily_pnl"]
    assert daily["daily_realized_pnl"] == "1000.0000"
    assert daily["daily_sell_notional"] == "314000.0000"
    assert daily["matched_sell_quantity"] == 1
    assert daily["unmatched_sell_quantity"] == 150
    assert daily["sold_symbols"] == ["003280", "005930"]
    assert daily["sold_symbol_count"] == 2
    assert daily["closed_symbols"] == ["005930"]
    assert daily["closed_symbol_count"] == 1
    assert daily["unmatched_sell_symbols"] == ["003280"]
    assert daily["unmatched_sell_symbol_count"] == 1

    engine.dispose()


def test_runtime_status_requires_complete_exec_owned_schema() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            automation_enabled=True,
        )
    )
    ExecutionFill.__table__.drop(engine)
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is False
    assert payload["dependencies"]["database"] == "ok"
    assert payload["dependencies"]["runtime_schema"] == "missing"
    assert "database or runtime schema dependency is not ready" in payload["readiness"]["reasons"]
    assert "execution_fills" in payload["safety"]["readiness_reason"]

    engine.dispose()


def test_metadata_matches_exec_owned_table_boundary() -> None:
    app, engine = build_test_app()
    inspector = inspect(engine)

    assert set(EXEC_OWNED_TABLES).issubset(set(inspector.get_table_names()))

    app.state.db_engine.dispose()


def test_execution_fills_has_filled_today_lookup_index() -> None:
    app, engine = build_test_app()
    inspector = inspect(engine)

    execution_fill_indexes = {
        index["name"]: index["column_names"]
        for index in inspector.get_indexes("execution_fills")
    }

    assert execution_fill_indexes["ix_execution_fills_filled_at_broker_order_pk"] == [
        "filled_at",
        "broker_order_pk",
    ]

    app.state.db_engine.dispose()


def test_runtime_safety_endpoint_reports_bootstrap_metadata() -> None:
    app, engine = build_test_app()
    client = TestClient(app)

    response = client.get("/runtime/safety")

    assert response.status_code == 200
    payload = response.json()
    assert payload["paused"] is True
    assert payload["kill_switch_enabled"] is False
    assert payload["automation_enabled"] is False
    assert payload["automated_order_submission_allowed"] is False
    assert payload["automated_order_submission_block_reason"] == "paused"
    assert payload["last_change"]["action"] == "bootstrap"
    assert payload["last_change"]["source"] == "system"
    assert payload["last_change"]["event_id"] is None

    engine.dispose()


def test_runtime_safety_fails_closed_when_runtime_safety_migration_is_missing(
    tmp_path,
) -> None:
    app, engine = build_unmigrated_runtime_safety_app(
        f"sqlite:///{tmp_path / 'runtime-safety-baseline.db'}"
    )
    client = TestClient(app)

    response = client.get("/runtime/safety")

    assert response.status_code == 200
    payload = response.json()
    assert payload["paused"] is True
    assert payload["kill_switch_enabled"] is False
    assert payload["automation_enabled"] is False
    assert payload["automated_order_submission_allowed"] is False
    assert payload["automated_order_submission_block_reason"] == "paused"
    assert payload["last_change"]["action"] == "bootstrap"
    assert "run migrations" in payload["readiness_reason"]

    engine.dispose()


def test_runtime_control_endpoints_record_transitions_and_audit() -> None:
    app, engine = build_test_app()
    client = TestClient(app)

    resume_response = client.post(
        "/controls/resume",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "ready for automated session",
        },
    )
    assert resume_response.status_code == 200
    resume_payload = resume_response.json()
    assert resume_payload["action"] == "resume"
    assert resume_payload["outcome"] == "applied"
    assert resume_payload["changed"] is True
    assert resume_payload["safety"]["paused"] is False
    assert resume_payload["safety"]["automated_order_submission_allowed"] is True
    assert resume_payload["safety"]["last_change"]["action"] == "resume"

    pause_response = client.post(
        "/controls/pause",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "manual pause for review",
        },
    )
    assert pause_response.status_code == 200
    pause_payload = pause_response.json()
    assert pause_payload["outcome"] == "applied"
    assert pause_payload["changed"] is True
    assert pause_payload["safety"]["paused"] is True
    assert pause_payload["safety"]["automated_order_submission_block_reason"] == "paused"

    pause_again_response = client.post(
        "/controls/pause",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "confirm paused state",
        },
    )
    assert pause_again_response.status_code == 200
    pause_again_payload = pause_again_response.json()
    assert pause_again_payload["outcome"] == "noop"
    assert pause_again_payload["changed"] is False

    kill_switch_response = client.post(
        "/controls/kill-switch",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "operator kill switch",
        },
    )
    assert kill_switch_response.status_code == 200
    kill_switch_payload = kill_switch_response.json()
    assert kill_switch_payload["outcome"] == "applied"
    assert kill_switch_payload["changed"] is True
    assert kill_switch_payload["safety"]["kill_switch_enabled"] is True
    assert kill_switch_payload["safety"]["paused"] is True
    assert kill_switch_payload["safety"]["automation_enabled"] is False
    assert kill_switch_payload["safety"]["automated_order_submission_allowed"] is False
    assert (
        kill_switch_payload["safety"]["automated_order_submission_block_reason"]
        == "kill_switch"
    )
    assert kill_switch_payload["safety"]["last_change"]["action"] == "kill_switch"

    kill_switch_again_response = client.post(
        "/controls/kill-switch",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "confirm kill switch",
        },
    )
    assert kill_switch_again_response.status_code == 200
    kill_switch_again_payload = kill_switch_again_response.json()
    assert kill_switch_again_payload["outcome"] == "noop"
    assert kill_switch_again_payload["changed"] is False
    assert kill_switch_again_payload["safety"]["kill_switch_enabled"] is True
    assert kill_switch_again_payload["safety"]["last_change"]["action"] == "kill_switch"

    blocked_resume_response = client.post(
        "/controls/resume",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "attempt resume while killed",
        },
    )
    assert blocked_resume_response.status_code == 409
    blocked_resume_payload = blocked_resume_response.json()
    assert blocked_resume_payload["outcome"] == "rejected"
    assert blocked_resume_payload["changed"] is False
    assert blocked_resume_payload["safety"]["kill_switch_enabled"] is True
    assert blocked_resume_payload["safety"]["last_change"]["action"] == "kill_switch"

    with app.state.db_session_factory() as session:
        state = session.get(RuntimeControlState, "global")
        assert state is not None
        assert state.kill_switch_enabled is True
        assert state.last_transition_action == "kill_switch"
        assert state.last_transition_event_id == kill_switch_payload["event_id"]

        events = session.scalars(
            select(RuntimeControlEvent).order_by(RuntimeControlEvent.occurred_at)
        ).all()
        assert [(event.action, event.outcome) for event in events] == [
            ("resume", "applied"),
            ("pause", "applied"),
            ("pause", "noop"),
            ("kill_switch", "applied"),
            ("kill_switch", "noop"),
            ("resume", "rejected"),
        ]

    engine.dispose()


def test_kill_switch_transition_is_serialized_under_concurrent_requests(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'runtime-safety-race.db'}")
    app, engine = build_test_app(settings)

    with app.state.db_session_factory() as session:
        runtime_controls.ensure_runtime_control_state(settings=settings, db=session)

    barrier = threading.Barrier(2)
    original_state_snapshot = runtime_controls._state_snapshot

    def synchronized_state_snapshot(state: RuntimeControlState) -> dict[str, object]:
        snapshot = original_state_snapshot(state)
        if not state.kill_switch_enabled:
            barrier.wait(timeout=5)
        return snapshot

    monkeypatch.setattr(runtime_controls, "_state_snapshot", synchronized_state_snapshot)

    results = []

    def worker(actor: str) -> None:
        with app.state.db_session_factory() as session:
            results.append(
                runtime_controls.enable_kill_switch(
                    settings=settings,
                    db=session,
                    command=runtime_controls.ControlCommand(
                        actor=actor,
                        source="test",
                        reason="concurrent kill switch request",
                    ),
                )
            )

    threads = [
        threading.Thread(target=worker, args=("alpha",)),
        threading.Thread(target=worker, args=("beta",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(result.outcome for result in results) == ["applied", "noop"]
    applied_result = next(result for result in results if result.outcome == "applied")
    noop_result = next(result for result in results if result.outcome == "noop")
    assert noop_result.safety["kill_switch_enabled"] is True
    assert noop_result.safety["last_change"]["event_id"] == applied_result.event_id

    with app.state.db_session_factory() as session:
        state = session.get(RuntimeControlState, "global")
        assert state is not None
        assert state.kill_switch_enabled is True
        assert state.last_transition_event_id == applied_result.event_id

        events = session.scalars(
            select(RuntimeControlEvent)
            .where(RuntimeControlEvent.action == "kill_switch")
            .order_by(RuntimeControlEvent.occurred_at)
        ).all()
        assert [event.outcome for event in events] == ["applied", "noop"]

    engine.dispose()


def test_runtime_safety_state_survives_restart(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'runtime-safety.db'}"
    first_settings = Settings(database_url=database_url)
    app, engine = build_test_app(first_settings)
    client = TestClient(app)

    resume_response = client.post(
        "/controls/resume",
        json={"actor": "alpha", "source": "console", "reason": "open runtime"},
    )
    assert resume_response.status_code == 200
    kill_switch_response = client.post(
        "/controls/kill-switch",
        json={"actor": "alpha", "source": "console", "reason": "halt runtime"},
    )
    assert kill_switch_response.status_code == 200

    engine.dispose()

    restarted_settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
    )
    restarted_app, restarted_engine = build_test_app(restarted_settings)
    restarted_client = TestClient(restarted_app)

    response = restarted_client.get("/runtime/safety")

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "paper"
    assert payload["paused"] is True
    assert payload["kill_switch_enabled"] is True
    assert payload["automation_enabled"] is False
    assert payload["automated_order_submission_allowed"] is False
    assert payload["automated_order_submission_block_reason"] == "kill_switch"
    assert payload["last_change"]["action"] == "kill_switch"
    assert payload["last_change"]["actor"] == "alpha"
    assert payload["last_change"]["reason"] == "halt runtime"

    with restarted_app.state.db_session_factory() as session:
        events = session.scalars(
            select(RuntimeControlEvent).order_by(RuntimeControlEvent.occurred_at)
        ).all()
        assert [(event.action, event.outcome) for event in events] == [
            ("resume", "applied"),
            ("kill_switch", "applied"),
        ]

    restarted_engine.dispose()


def test_execution_plan_intake_and_read_endpoints() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-20260429-open",
            "source": "kai-trading-core",
            "market_date": "2026-04-29",
            "strategy_run_id": "run-123",
            "requested_at": "2026-04-29T09:00:00Z",
            "items": [
                {
                    "symbol": "005930",
                    "side": "BUY",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 10},
                    "limit_price": "71000.00",
                    "time_in_force": "DAY",
                    "decision_snapshot_id": "decision-1",
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "percent", "fraction": 0.5},
                    "trigger_price": "125000.00",
                    "risk_context": {"stop": "tighten"},
                },
            ],
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["plan_id"] == "plan-20260429-open"
    assert payload["accepted_item_count"] == 2
    assert payload["rejected_item_count"] == 0
    assert payload["validation_failures"] == []
    assert len(payload["persisted_execution_item_ids"]) == 2
    assert payload["runtime"]["mode"] == "paper"

    plans_response = client.get("/execution-plans")
    assert plans_response.status_code == 200
    plans_payload = plans_response.json()
    assert len(plans_payload) == 1
    plan = plans_payload[0]
    assert plan["plan_id"] == "plan-20260429-open"
    assert plan["source"] == "kai-trading-core"
    assert plan["market_date"] == "2026-04-29"
    assert plan["strategy_run_id"] == "run-123"
    assert plan["mode"] == "paper"
    assert plan["status"] == "accepted"
    assert plan["item_count"] == 2
    items_by_symbol = {item["symbol"]: item for item in plan["items"]}
    assert set(items_by_symbol) == {"005930", "000660"}
    assert items_by_symbol["005930"]["side"] == "buy"
    assert items_by_symbol["005930"]["limit_price"] == "71000.0000"
    assert items_by_symbol["005930"]["time_in_force"] == "day"
    assert items_by_symbol["000660"]["status"] == "target_sell_monitoring"
    assert items_by_symbol["000660"]["trigger_price"] == "125000.0000"

    assert client.get("/orders").json() == []
    assert client.get("/executions").json() == []
    assert client.get("/positions").json() == []

    with app.state.db_session_factory() as session:
        persisted_plan = session.scalar(
            select(ExecutionPlan).where(
                ExecutionPlan.external_plan_id == "plan-20260429-open"
            )
        )
        assert persisted_plan is not None
        assert persisted_plan.market_date is not None
        assert persisted_plan.market_date.isoformat() == "2026-04-29"
        assert persisted_plan.strategy_run_id == "run-123"

        persisted_items = session.scalars(
            select(ExecutionPlanItem)
            .where(ExecutionPlanItem.execution_plan_id == persisted_plan.id)
            .order_by(ExecutionPlanItem.created_at)
        ).all()
        assert len(persisted_items) == 2
        assert persisted_items[0].limit_price == Decimal("71000.0000")
        assert persisted_items[0].time_in_force == "day"
        assert persisted_items[1].trigger_price == Decimal("125000.0000")

    engine.dispose()


def test_execution_plan_intake_schedules_next_open_buy_jobs() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-20260430-open",
            "source": "kai-trading-core",
            "market_date": "2026-04-30",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 10},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "percent", "fraction": 0.5},
                    "trigger_price": "125000.00",
                },
            ],
        },
    )

    assert response.status_code == 201

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    assert runtime_status.json()["counts"]["active_execution_jobs"] == 1

    with app.state.db_session_factory() as session:
        plan = session.scalar(
            select(ExecutionPlan).where(
                ExecutionPlan.external_plan_id == "plan-20260430-open"
            )
        )
        assert plan is not None

        items = session.scalars(
            select(ExecutionPlanItem)
            .where(ExecutionPlanItem.execution_plan_id == plan.id)
            .order_by(ExecutionPlanItem.symbol.asc())
        ).all()
        assert [item.symbol for item in items] == ["000660", "005930"]
        items_by_symbol = {item.symbol: item for item in items}
        assert items_by_symbol["005930"].status == "next_open_buy_scheduled"
        assert items_by_symbol["000660"].status == "target_sell_monitoring"

        jobs = session.scalars(
            select(ExecutionJob)
            .where(ExecutionJob.execution_plan_item_id == items_by_symbol["005930"].id)
        ).all()
        assert len(jobs) == 1
        assert jobs[0].job_type == "next_open_buy"
        assert jobs[0].status == "scheduled"
        assert jobs[0].scheduled_for is not None
        assert jobs[0].attempts == 0

    engine.dispose()


def test_regular_market_session_detection_controls_startup_monitoring_bootstrap() -> None:
    assert _is_regular_market_session(
        now=datetime(2026, 5, 22, 1, 30, tzinfo=UTC),
        market_timezone="Asia/Seoul",
    ) is True
    assert _is_regular_market_session(
        now=datetime(2026, 5, 22, 23, 30, tzinfo=UTC),
        market_timezone="Asia/Seoul",
    ) is False
    assert _is_regular_market_session(
        now=datetime(2026, 5, 23, 1, 0, tzinfo=UTC),
        market_timezone="Asia/Seoul",
    ) is False


def test_monitoring_items_use_short_pending_config_status_and_stay_evaluable() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-target-sell-missing-config",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "percent", "fraction": 0.5},
                }
            ],
        },
    )

    assert response.status_code == 201
    plan_item_id = response.json()["persisted_execution_item_ids"][0]

    plans_response = client.get("/execution-plans")
    assert plans_response.status_code == 200
    item = plans_response.json()[0]["items"][0]
    assert item["execution_plan_item_id"] == plan_item_id
    assert item["status"] == "target_sell_pending_config"
    assert len(item["status"]) <= 32

    evaluation = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "invalid config should not fail persistence",
            "prices": [
                {
                    "symbol": "000660",
                    "price": "125100.00",
                }
            ],
        },
    )

    assert evaluation.status_code == 200
    payload = evaluation.json()
    assert payload["evaluated_item_count"] == 1
    assert payload["triggered_item_count"] == 0
    assert payload["submitted_order_count"] == 0
    assert payload["items"][0]["execution_plan_item_id"] == plan_item_id
    assert payload["items"][0]["outcome"] == "invalid_configuration"

    with app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "target_sell_pending_config"
        assert len(plan_item.status) <= 32

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        assert [item.id for item in autonomous_items] == []

    engine.dispose()


def test_position_exit_pending_sell_is_autonomous_monitored_when_position_exists() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-position-exit-target",
            "source": "kai-trading-core",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "100.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                }
            ],
        },
    )
    assert response.status_code == 201
    plan_item_id = response.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        plan_item = session.get(ExecutionPlanItem, plan_item_id)
        assert plan_item is not None
        assert plan_item.status == "target_sell_pending_config"

        stale_state_autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        assert [item.id for item in stale_state_autonomous_items] == []

        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=7,
                average_price=Decimal("95.00"),
                updated_from="test",
            )
        )
        session.commit()

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        assert [item.id for item in autonomous_items] == [plan_item_id]

        assert _resolve_sell_quantity(
            item=autonomous_items[0],
            remaining_quantity=7,
        ) == 7

    engine.dispose()


def test_weekend_handoff_watch_buy_is_monitored_on_target_market_date() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    friday_accepted_at = datetime(2026, 7, 31, 23, 30, tzinfo=UTC)
    monday_market_date = datetime(2026, 8, 3).date()

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            id="weekend-handoff-plan",
            external_plan_id="weekend-handoff-plan",
            source="kai-trading-core",
            market_date=monday_market_date,
            mode="paper",
            status="accepted",
            accepted_at=friday_accepted_at,
        )
        item = ExecutionPlanItem(
            id="weekend-handoff-buy",
            execution_plan_id=plan.id,
            plan_item_index=0,
            symbol="011300",
            side="buy",
            intent="next_open_buy",
            status="watch_monitoring_pending",
            trigger_price=Decimal("1000"),
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        session.add(plan)
        session.add(item)
        session.commit()

        assert list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=monday_market_date,
        ) == ["011300"]
        assert runtime_service.runtime_monitored_symbols(
            settings=app.state.settings,
            db=session,
            today_start_utc=datetime(2026, 8, 2, 15, tzinfo=UTC),
        ) == ["011300"]

    engine.dispose()


def test_autonomous_monitoring_uses_latest_plan_per_symbol_only() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-old-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-21",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "100.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                }
            ],
        },
    )
    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-new-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "90.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                },
            ],
        },
    )
    assert older.status_code == 201
    assert newer.status_code == 201
    older_item_id = older.json()["persisted_execution_item_ids"][0]
    newer_item_ids = set(newer.json()["persisted_execution_item_ids"])

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=7,
                average_price=Decimal("95.00"),
                updated_from="test",
            )
        )
        session.commit()

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        autonomous_item_ids = {item.id for item in autonomous_items}
        assert older_item_id not in autonomous_item_ids
        assert autonomous_item_ids == newer_item_ids
        older_item = session.get(ExecutionPlanItem, older_item_id)
        assert older_item is not None
        assert older_item.status == "superseded"
        assert older_item.risk_context["last_monitoring_event"]["event"] == "active_sell_monitor_superseded"

    engine.dispose()


def test_replanned_sell_monitor_updates_existing_source_row_in_place() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-old-replanned-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-21",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "source_plan_item_id": "core-plan-row-1",
                    "trigger_price": "100.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "decision_snapshot_id": "snapshot-old",
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "stop_sell",
                    "source_plan_item_id": "core-plan-row-1",
                    "trigger_price": "90.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "decision_snapshot_id": "snapshot-old",
                },
            ],
        },
    )
    assert older.status_code == 201
    target_item_id, stop_item_id = older.json()["persisted_execution_item_ids"]

    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-new-replanned-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-21",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "source_plan_item_id": "core-plan-row-1",
                    "trigger_price": "112.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "decision_snapshot_id": "snapshot-new",
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "stop_sell",
                    "source_plan_item_id": "core-plan-row-1",
                    "trigger_price": "94.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "decision_snapshot_id": "snapshot-new",
                },
            ],
        },
    )
    assert newer.status_code == 201
    new_item_ids = newer.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="000660",
                quantity=3,
                average_price=Decimal("95.00"),
                updated_from="test",
            )
        )
        session.commit()

        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "target_sell_monitoring"
        assert stop_item.status == "stop_sell_monitoring"
        assert target_item.trigger_price == Decimal("112.0000")
        assert stop_item.trigger_price == Decimal("94.0000")
        assert target_item.quantity_policy == {"kind": "shares", "quantity": 3}
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 3}
        assert target_item.decision_snapshot_id == "snapshot-new"
        assert stop_item.decision_snapshot_id == "snapshot-new"
        assert target_item.risk_context["last_monitoring_event"]["event"] == "sell_monitor_replanned_in_place"
        assert stop_item.risk_context["last_monitoring_event"]["event"] == "sell_monitor_replanned_in_place"

        new_items = [session.get(ExecutionPlanItem, item_id) for item_id in new_item_ids]
        assert [item.status for item in new_items if item is not None] == ["superseded", "superseded"]

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        autonomous_item_ids = {item.id for item in autonomous_items}
        assert target_item_id in autonomous_item_ids
        assert stop_item_id in autonomous_item_ids
        assert not (autonomous_item_ids & set(new_item_ids))

    engine.dispose()


def test_autonomous_monitoring_includes_older_active_sell_monitor_without_same_intent_replacement() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-old-target-still-active",
            "source": "kai-trading-core",
            "market_date": "2026-06-11",
            "items": [
                {
                    "symbol": "073560",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "1583.00",
                    "quantity_policy": {"kind": "shares", "quantity": 10},
                }
            ],
        },
    )
    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-new-stop-only",
            "source": "kai-trading-core",
            "market_date": "2026-06-12",
            "items": [
                {
                    "symbol": "073560",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "1450.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                }
            ],
        },
    )
    assert older.status_code == 201
    assert newer.status_code == 201
    older_target_id = older.json()["persisted_execution_item_ids"][0]
    newer_stop_id = newer.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="073560",
                quantity=98,
                average_price=Decimal("1675.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 12, 1, 30, tzinfo=UTC),
            )
        )
        session.commit()

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )

        assert {item.id for item in autonomous_items} == {older_target_id, newer_stop_id}
        older_target = session.get(ExecutionPlanItem, older_target_id)
        assert older_target is not None
        assert older_target.status == "target_sell_monitoring"

    engine.dispose()


def test_autonomous_monitoring_falls_back_to_older_stop_when_latest_stop_rejected() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        )
    )
    client = TestClient(app)
    broker_adapter = StubMonitoringBrokerAdapter()

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable monitoring sell"},
    )
    assert resume.status_code == 200

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-old-stop",
            "source": "kai-trading-core",
            "market_date": "2026-05-26",
            "items": [
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "8060.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-new-target-rejected-stop",
            "source": "kai-trading-core",
            "market_date": "2026-05-27",
            "items": [
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "9250.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "8700.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert older.status_code == 201
    assert newer.status_code == 201
    older_stop_id = older.json()["persisted_execution_item_ids"][0]
    newer_target_id, newer_stop_id = newer.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        newer_stop = session.get(ExecutionPlanItem, newer_stop_id)
        assert newer_stop is not None
        newer_stop.status = "broker_order_rejected"
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="033530",
                quantity=58,
                average_price=Decimal("8500.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
            )
        )
        session.commit()

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        assert {item.id for item in autonomous_items} == {older_stop_id, newer_target_id}

        result = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[
                MonitoringPriceRequest(symbol="033530", price=Decimal("7300.00"))
            ],
            now=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
        )

        assert result.status_code == 200
        assert result.payload["submitted_order_count"] == 1
        assert result.payload["triggered_item_count"] == 1
        assert broker_adapter.order_calls[0].symbol == "033530"
        assert broker_adapter.order_calls[0].side == "sell"
        assert broker_adapter.order_calls[0].quantity == 58
        assert broker_adapter.order_calls[0].execution_plan_item_id == older_stop_id

        orders = session.scalars(select(BrokerOrder)).all()
        assert [order.execution_plan_item_id for order in orders] == [older_stop_id]

    engine.dispose()


def test_autonomous_monitoring_rearms_retry_pending_stop_after_session_rejection() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
        )
    )
    client = TestClient(app)
    broker_adapter = StubMonitoringBrokerAdapter()

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable monitoring sell"},
    )
    assert resume.status_code == 200

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-retry-pending-stop",
            "source": "kai-trading-core",
            "market_date": "2026-05-26",
            "items": [
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "8060.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-latest-target-dead-stop",
            "source": "kai-trading-core",
            "market_date": "2026-05-27",
            "items": [
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "9250.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "8700.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert older.status_code == 201
    assert newer.status_code == 201
    older_stop_id = older.json()["persisted_execution_item_ids"][0]
    newer_stop_id = newer.json()["persisted_execution_item_ids"][1]

    with app.state.db_session_factory() as session:
        older_stop = session.get(ExecutionPlanItem, older_stop_id)
        newer_stop = session.get(ExecutionPlanItem, newer_stop_id)
        assert older_stop is not None
        assert newer_stop is not None
        older_stop.status = "broker_order_retry_pending"
        newer_stop.status = "broker_order_rejected"
        session.add(
            BrokerOrder(
                execution_plan_item_id=older_stop_id,
                mode="paper",
                idempotency_key="session-retry-stop-033530",
                symbol="033530",
                side="sell",
                status="retry_pending",
                quantity=58,
                last_error_code="KIOK0320",
                last_error_message="장운영시간이 아닙니다",
                last_error_retryable=True,
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="033530",
                quantity=58,
                average_price=Decimal("8500.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
            )
        )
        session.commit()

        result = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[MonitoringPriceRequest(symbol="033530", price=Decimal("7550.00"))],
            now=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
        )

        assert result.status_code == 200
        assert result.payload["submitted_order_count"] == 1
        assert broker_adapter.order_calls[0].symbol == "033530"
        assert broker_adapter.order_calls[0].side == "sell"
        assert broker_adapter.order_calls[0].quantity == 58
        assert broker_adapter.order_calls[0].execution_plan_item_id == older_stop_id
        rearmed_stop = session.get(ExecutionPlanItem, older_stop_id)
        assert rearmed_stop is not None
        assert rearmed_stop.status == "broker_order_submitted"
        events = rearmed_stop.risk_context["monitoring_events"]
        assert any(event["event"] == "protective_stop_rearmed" for event in events)

    engine.dispose()


def test_autonomous_monitoring_expires_known_flat_pending_sell_monitors() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    stale_sell = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-flat-position-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                }
            ],
        },
    )
    held_sell = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-position-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "80000.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-2",
                    },
                }
            ],
        },
    )
    assert stale_sell.status_code == 201
    assert held_sell.status_code == 201
    stale_item_id = stale_sell.json()["persisted_execution_item_ids"][0]
    held_item_id = held_sell.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        live_watch_plan = ExecutionPlan(
            external_plan_id="live-cross-runtime-watch-buy",
            source="kai-trading-core",
            market_date=datetime(2026, 5, 22, tzinfo=UTC).date(),
            mode="live",
        )
        live_watch_item = ExecutionPlanItem(
            plan=live_watch_plan,
            plan_item_index=0,
            symbol="000660",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=Decimal("90.00"),
            quantity_policy={"kind": "shares", "quantity": 1},
            risk_context={
                "watch_buy_monitoring": {
                    "direction": "at_or_below",
                    "trigger_price": "90.00",
                },
            },
        )
        session.add_all(
            [
                live_watch_plan,
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=0,
                    average_price=Decimal("0.00"),
                    updated_from="reconciliation",
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=3,
                    average_price=Decimal("70000.00"),
                    updated_from="reconciliation",
                ),
            ]
        )
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 5, 22, tzinfo=UTC).date(),
        )

        stale_item = session.get(ExecutionPlanItem, stale_item_id)
        held_item = session.get(ExecutionPlanItem, held_item_id)
        assert stale_item is not None
        assert held_item is not None
        assert stale_item.status == "target_sell_pending_config"
        assert held_item.status == "target_sell_pending_config"
        assert live_watch_item.status == "watch_monitoring_active"
        assert symbols == ["005930"]

    engine.dispose()


def test_autonomous_monitoring_expires_known_flat_active_sell_monitors() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    stale_sell = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-flat-active-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                }
            ],
        },
    )
    held_sell = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-active-sell",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "70000.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-2",
                    },
                }
            ],
        },
    )
    assert stale_sell.status_code == 201
    assert held_sell.status_code == 201
    stale_item_id = stale_sell.json()["persisted_execution_item_ids"][0]
    held_item_id = held_sell.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        stale_item = session.get(ExecutionPlanItem, stale_item_id)
        held_item = session.get(ExecutionPlanItem, held_item_id)
        assert stale_item is not None
        assert held_item is not None
        stale_item.status = "target_sell_monitoring"
        held_item.status = "stop_sell_monitoring"
        session.add_all(
            [
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=0,
                    average_price=Decimal("0.00"),
                    updated_from="reconciliation",
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=3,
                    average_price=Decimal("71000.00"),
                    updated_from="reconciliation",
                ),
            ]
        )
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 5, 22, tzinfo=UTC).date(),
        )

        assert stale_item.status == "target_sell_monitoring"
        assert held_item.status == "stop_sell_monitoring"
        assert symbols == ["005930"]

    engine.dispose()


def test_monitoring_refreshes_stale_flat_position_before_sell_symbol_selection() -> None:
    broker_adapter = StubMonitoringBrokerAdapter(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime(2026, 5, 22, 1, 0, tzinfo=UTC),
            source="stub:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=3,
                    average_price=Decimal("71000.00"),
                )
            ],
        )
    )
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
            exec_agent_max_account_snapshot_age_seconds=60,
        )
    )
    client = TestClient(app)
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-stale-position-sell-monitor",
            "source": "kai-trading-core",
            "market_date": "2026-05-22",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "70000.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                }
            ],
        },
    )
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        item_id = response.json()["persisted_execution_item_ids"][0]
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = "stop_sell_monitoring"
        session.add(
            ExecutionPosition(
                mode="live",
                symbol="005930",
                quantity=0,
                average_price=Decimal("0.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 5, 22, 0, 0, tzinfo=UTC),
            )
        )
        session.commit()

        run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[],
            now=datetime(2026, 5, 22, 1, 2, tzinfo=UTC),
        )

        position = session.scalar(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == "live",
                ExecutionPosition.symbol == "005930",
            )
        )
        assert broker_adapter.account_snapshot_calls == 1
        assert position is not None
        assert position.quantity == 3
        assert list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 5, 22, tzinfo=UTC).date(),
        ) == ["005930"]

    engine.dispose()


def test_autonomous_monitoring_retains_pending_sell_after_unexited_buy_order() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-buy-filled-local-position-stale",
            "source": "kai-trading-core",
            "market_date": "2026-06-04",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "565000.00",
                    "limit_price": "575000.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "548000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, stop_item_id = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert buy_item is not None
        assert stop_item is not None
        buy_item.status = "broker_order_submitted"
        stop_item.status = "stop_sell_pending_config"
        session.add(
            BrokerOrder(
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=1,
                limit_price=Decimal("567000.00"),
                submitted_at=datetime(2026, 6, 4, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="002380",
                quantity=0,
                average_price=None,
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 4, 0, 53, tzinfo=UTC),
            )
        )
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 6, 4, tzinfo=UTC).date(),
        )

        retained_stop = session.get(ExecutionPlanItem, stop_item_id)
        assert retained_stop is not None
        assert retained_stop.status == "stop_sell_pending_config"
        assert symbols == ["002380"]

    engine.dispose()


def test_live_autonomous_monitoring_preserves_pending_sell_before_fresh_position_truth() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-buy-filled-position-stale-flat",
            "source": "kai-trading-core",
            "market_date": "2026-06-04",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "565000.00",
                    "limit_price": "575000.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "548000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, stop_item_id = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert buy_item is not None
        assert stop_item is not None
        buy_item.status = "broker_order_submitted"
        stop_item.status = "stop_sell_pending_config"
        session.add(
            BrokerOrder(
                mode="live",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=1,
                limit_price=Decimal("567000.00"),
                submitted_at=datetime(2026, 6, 4, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            ExecutionPosition(
                mode="live",
                symbol="002380",
                quantity=0,
                average_price=None,
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 4, 0, 53, tzinfo=UTC),
            )
        )
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 6, 4, tzinfo=UTC).date(),
        )

        retained_stop = session.get(ExecutionPlanItem, stop_item_id)
        assert retained_stop is not None
        assert retained_stop.status == "stop_sell_pending_config"
        assert symbols == ["002380"]

    engine.dispose()


def test_live_autonomous_monitoring_drops_pending_sell_after_fresh_flat_position_truth() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="live",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-buy-filled-position-flat",
            "source": "kai-trading-core",
            "market_date": "2026-06-04",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "565000.00",
                    "limit_price": "575000.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "548000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, stop_item_id = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert buy_item is not None
        assert stop_item is not None
        buy_item.status = "broker_order_submitted"
        stop_item.status = "stop_sell_pending_config"
        session.add(
            BrokerOrder(
                mode="live",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=1,
                limit_price=Decimal("567000.00"),
                submitted_at=datetime(2026, 6, 4, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            ExecutionPosition(
                mode="live",
                symbol="002380",
                quantity=0,
                average_price=None,
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
            )
        )
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 6, 4, tzinfo=UTC).date(),
        )

        dropped_stop = session.get(ExecutionPlanItem, stop_item_id)
        assert dropped_stop is not None
        assert dropped_stop.status == "stop_sell_pending_config"
        assert symbols == []

    engine.dispose()


def test_autonomous_monitoring_includes_held_stop_sell_outside_symbol_cap() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            monitoring_max_symbols_per_cycle=2,
        )
    )
    client = TestClient(app)

    first = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-cap-first-target",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "80000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                }
            ],
        },
    )
    second = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-cap-second-target",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "200000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                }
            ],
        },
    )
    held_stop = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-cap-held-kcc-stop",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "548000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                }
            ],
        },
    )
    assert first.status_code == 201
    assert second.status_code == 201
    assert held_stop.status_code == 201
    first_item_id = first.json()["persisted_execution_item_ids"][0]
    second_item_id = second.json()["persisted_execution_item_ids"][0]
    held_stop_item_id = held_stop.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        first_item = session.get(ExecutionPlanItem, first_item_id)
        second_item = session.get(ExecutionPlanItem, second_item_id)
        held_stop_item = session.get(ExecutionPlanItem, held_stop_item_id)
        assert first_item is not None
        assert second_item is not None
        assert held_stop_item is not None
        assert first_item.plan is not None
        assert second_item.plan is not None
        assert held_stop_item.plan is not None
        first_item.plan.accepted_at = datetime(2026, 6, 8, 0, 0, tzinfo=UTC)
        second_item.plan.accepted_at = datetime(2026, 6, 8, 0, 1, tzinfo=UTC)
        held_stop_item.plan.accepted_at = datetime(2026, 6, 8, 0, 2, tzinfo=UTC)
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="002380",
                quantity=1,
                average_price=Decimal("567000.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 8, 0, 32, tzinfo=UTC),
            )
        )
        session.commit()

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )

        assert [item.id for item in autonomous_items] == [held_stop_item_id]

    engine.dispose()


def test_post_entry_maintenance_creates_missing_target_sell_monitor() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-entry-target-repair",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "100.00",
                    "limit_price": "100.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "110.00",
                        "stop_loss_price": "95.00",
                    },
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "95.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, _ = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        assert buy_item is not None
        buy_item.status = "broker_order_submitted"
        session.add(
            BrokerOrder(
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                limit_price=Decimal("100.00"),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        position = ExecutionPosition(
            mode="paper",
            symbol="002380",
            quantity=3,
            average_price=Decimal("100.00"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
        )
        session.add(position)
        session.commit()

        maintenance_count, review_count = _maintain_post_entry_exit_monitors(
            settings=app.state.settings,
            db=session,
            positions_by_symbol={"002380": position},
            prices={"002380": MonitoringPriceRequest(symbol="002380", price=Decimal("105.00"))},
            triggered_at=datetime(2026, 6, 8, 1, 5, tzinfo=UTC),
        )
        session.commit()

        target = session.scalar(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.symbol == "002380",
                ExecutionPlanItem.intent == "target_sell",
                ExecutionPlanItem.status == "target_sell_monitoring",
            )
        )
        assert target is not None
        assert target.trigger_price == Decimal("110.0000")
        assert target.quantity_policy == {"kind": "shares", "quantity": 3}
        assert target.risk_context["post_entry_managed"] is True
        assert target.risk_context["entry_context"]["entry_price"] == "100.0000"
        assert maintenance_count >= 1
        assert review_count == 0

    engine.dispose()


def test_post_entry_maintenance_promotes_pending_target_sell_monitor() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-entry-target-pending-repair",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "100.00",
                    "limit_price": "100.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "110.00",
                        "stop_loss_price": "95.00",
                    },
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, target_item_id = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert buy_item is not None
        assert target_item is not None
        assert target_item.status == "target_sell_pending_config"
        buy_item.status = "broker_order_submitted"
        session.add(
            BrokerOrder(
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                limit_price=Decimal("100.00"),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        position = ExecutionPosition(
            mode="paper",
            symbol="002380",
            quantity=3,
            average_price=Decimal("100.00"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
        )
        session.add(position)
        session.commit()

        maintenance_count, review_count = _maintain_post_entry_exit_monitors(
            settings=app.state.settings,
            db=session,
            positions_by_symbol={"002380": position},
            prices={"002380": MonitoringPriceRequest(symbol="002380", price=Decimal("105.00"))},
            triggered_at=datetime(2026, 6, 8, 1, 5, tzinfo=UTC),
        )
        session.commit()

        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert target_item is not None
        assert target_item.status == "target_sell_monitoring"
        assert target_item.risk_context["last_monitoring_event"]["event"] == (
            "post_entry_target_monitor_activated"
        )
        assert target_item.risk_context["last_monitoring_event"]["prior_status"] == (
            "target_sell_pending_config"
        )
        assert maintenance_count >= 1
        assert review_count == 0

        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )
        assert target_item_id in {item.id for item in autonomous_items}

    engine.dispose()


def test_cached_autonomous_monitoring_repairs_targets_before_idle_exit() -> None:
    class StaticMonitoringPriceSource:
        provider = "test"

        def __init__(self, prices: dict[str, Decimal]) -> None:
            self.prices = prices
            self.calls: list[list[str]] = []

        def __call__(
            self,
            *,
            symbols: list[str],
            observed_at: datetime,
            settings: Settings,
        ) -> list[MonitoringPriceRequest]:
            assert settings
            self.calls.append(list(symbols))
            return [
                MonitoringPriceRequest(
                    symbol=symbol,
                    price=self.prices[symbol],
                    observed_at=observed_at,
                )
                for symbol in symbols
                if symbol in self.prices
            ]

    price_source = StaticMonitoringPriceSource({"002380": Decimal("105.00")})
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
        ),
        autonomous_runtime_enabled=True,
        monitoring_price_source=price_source,
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-cached-target-repair-before-idle",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "100.00",
                    "limit_price": "100.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "110.00",
                        "stop_loss_price": "95.00",
                    },
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {
                        "type": "position_exit_from_entry",
                        "linked_entry_item_key": "entry-1",
                    },
                },
            ],
        },
    )
    assert response.status_code == 201
    buy_item_id, target_item_id = response.json()["persisted_execution_item_ids"]

    with app.state.db_session_factory() as session:
        buy_item = session.get(ExecutionPlanItem, buy_item_id)
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert buy_item is not None
        assert target_item is not None
        assert target_item.status == "target_sell_pending_config"
        buy_item.status = "broker_order_submitted"
        session.add(
            BrokerOrder(
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                limit_price=Decimal("100.00"),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="002380",
                quantity=3,
                average_price=Decimal("100.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
            )
        )
        session.commit()

    payload = app.state.autonomous_runtime._monitoring_cycle_sync(
        datetime(2026, 6, 8, 1, 5, tzinfo=UTC)
    )

    assert price_source.calls == [["002380"]]
    assert payload["cached_evaluation"] is True
    assert payload["triggered_item_count"] == 0
    assert payload["post_entry_maintenance_count"] >= 1
    assert target_item_id in {item["execution_plan_item_id"] for item in payload["items"]}

    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert target_item is not None
        assert target_item.status == "target_sell_monitoring"

    engine.dispose()


def test_live_desired_symbol_reconcile_syncs_positions_and_filled_today(tmp_path, monkeypatch) -> None:
    class StaticMonitoringPriceSource:
        provider = "test"

        def __init__(self, prices: dict[str, Decimal]) -> None:
            self.prices = prices
            self.calls: list[list[str]] = []

        def __call__(
            self,
            *,
            symbols: list[str],
            observed_at: datetime,
            settings: Settings,
        ) -> list[MonitoringPriceRequest]:
            assert settings
            self.calls.append(list(symbols))
            return [
                MonitoringPriceRequest(
                    symbol=symbol,
                    price=self.prices[symbol],
                    observed_at=observed_at,
                )
                for symbol in symbols
                if symbol in self.prices
            ]

    class LiveFastPathBroker:
        provider = "kis"

        def __init__(self, observed_at: datetime) -> None:
            self.observed_at = observed_at
            self.reconcile_calls: list[tuple[str, str, datetime | None]] = []
            self.account_snapshot_calls = 0

        def describe(self) -> BrokerConfig:
            return BrokerConfig(
                provider="kis",
                mode="live",
                configured=True,
                api_base_url="https://stub.kis.local",
            )

        def authenticate(self) -> BrokerTokenGrant:
            return BrokerTokenGrant(
                access_token="live-token",
                token_type="Bearer",
                access_token_expires_at=self.observed_at + timedelta(hours=1),
            )

        def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
            assert access_token
            return BrokerRealtimeApproval(approval_key="approval", issued_at=self.observed_at)

        def revoke(self, access_token: str) -> BrokerTokenRevocation:
            assert access_token
            return BrokerTokenRevocation(message="revoked")

        def reconnect_handle(self) -> BrokerReconnectHandle:
            return BrokerReconnectHandle(supported=True, strategy="stub")

        def reconcile_order(
            self,
            access_token: str,
            broker_order_id: str,
            *,
            exchange: str = "KRX",
            submitted_at: datetime | None = None,
        ) -> dict[str, object]:
            assert access_token
            self.reconcile_calls.append((broker_order_id, exchange, submitted_at))
            return {
                "broker_order_id": broker_order_id,
                "status": "filled",
                "filled_quantity": 3,
                "remaining_quantity": 0,
                "average_fill_price": "100.00",
                "filled_at": self.observed_at.isoformat(),
                "observed_at": self.observed_at.isoformat(),
            }

        def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
            assert access_token
            self.account_snapshot_calls += 1
            return BrokerAccountSnapshot(
                available_cash=Decimal("1000000"),
                observed_at=self.observed_at,
                source="stub:account_snapshot",
                holdings=[
                    BrokerAccountHolding(
                        symbol="002380",
                        quantity=3,
                        average_price=Decimal("100.00"),
                        sellable_quantity=3,
                    )
                ],
            )

    started_at = datetime(2026, 6, 8, 1, 5, tzinfo=UTC)
    monkeypatch.setenv("KAI_EXEC_MONITORING_SYMBOL_PRIORITY", "002380")
    price_source = StaticMonitoringPriceSource({"002380": Decimal("105.00")})
    broker = LiveFastPathBroker(started_at)
    app, engine = build_test_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'live-fast-path-reconcile.db'}",
            runtime_mode="live",
            autonomous_runtime_enabled=True,
            kis_app_key="live-key",
            kis_app_secret="live-secret",
            kis_account_number="12345678",
        ),
        autonomous_runtime_enabled=True,
        monitoring_price_source=price_source,
        broker_adapter_factory=lambda _settings: broker,
    )

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="live-fast-path-reconcile",
            source="test",
            mode="live",
            market_date=started_at.date(),
        )
        buy_item = ExecutionPlanItem(
            execution_plan_id=plan.id,
            plan_item_index=0,
            symbol="002380",
            side="buy",
            intent="intraday_watch_buy",
            status="broker_order_submitted",
            trigger_price=Decimal("100.00"),
            limit_price=Decimal("100.00"),
            quantity_policy={"kind": "shares", "quantity": 3},
        )
        target_item = ExecutionPlanItem(
            execution_plan_id=plan.id,
            plan_item_index=1,
            symbol="002380",
            side="sell",
            intent="target_sell",
            status="target_sell_monitoring",
            trigger_price=Decimal("110.00"),
            limit_price=Decimal("110.00"),
            quantity_policy={"kind": "shares", "quantity": 3},
        )
        plan.items.extend([buy_item, target_item])
        session.add(plan)
        session.flush()
        session.add(
            BrokerOrder(
                broker_name="kis",
                mode="live",
                execution_plan_item_id=buy_item.id,
                idempotency_key=f"monitoring:watch_buy:{buy_item.id}",
                broker_order_id="fast-path-buy-1",
                symbol="002380",
                side="buy",
                status="submitted",
                quantity=3,
                limit_price=Decimal("100.00"),
                submitted_at=started_at - timedelta(minutes=1),
            )
        )
        session.commit()

    payload = app.state.autonomous_runtime._monitoring_cycle_sync(started_at)

    assert broker.reconcile_calls == [
        ("fast-path-buy-1", "KRX", (started_at - timedelta(minutes=1)).replace(tzinfo=None))
    ]
    assert broker.account_snapshot_calls == 1
    assert price_source.calls == [["002380"]]
    assert payload["cached_evaluation"] is True
    assert payload["evaluated_item_count"] == 1
    assert target_item.id in {item["execution_plan_item_id"] for item in payload["items"]}
    assert payload["runtime_snapshot"]["position_count"] == 1
    assert payload["runtime_snapshot"]["filled_today_symbols"] == ["002380"]

    with app.state.db_session_factory() as session:
        position = session.scalar(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == "live",
                ExecutionPosition.symbol == "002380",
            )
        )
        fills = session.scalars(select(ExecutionFill).where(ExecutionFill.symbol == "002380")).all()
        assert position is not None
        assert position.quantity == 3
        assert position.updated_from == "reconciliation"
        assert len(fills) == 1

    engine.dispose()


def test_monitoring_reports_missing_profit_target_coverage() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-target-coverage-gap",
            "source": "kai-trading-core",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "110.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "002380",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "95.00",
                    "quantity_policy": {"kind": "shares", "quantity": 3},
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="002380",
                quantity=3,
                average_price=Decimal("100.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
            )
        )
        session.commit()

    evaluation = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "target coverage audit",
            "prices": [{"symbol": "002380", "price": "105.00"}],
        },
    )

    assert evaluation.status_code == 200
    target_coverage = evaluation.json()["profit_target_coverage"]
    assert target_coverage["status"] == "degraded"
    assert target_coverage["missing_target_coverage_count"] == 1
    assert target_coverage["missing_quantities"] == {"002380": 2}
    assert target_coverage["issues"][0]["latest_target_trigger_price"] == "110.0000"

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_coverage = runtime_status.json()["liveness"]["monitoring"]["profit_target_coverage"]
    assert runtime_coverage["status"] == "degraded"
    assert runtime_coverage["missing_quantities"] == {"002380": 2}
    runtime_alerts = runtime_status.json()["actionable_alerts"]
    assert runtime_alerts["status"] == "needs_attention"
    assert runtime_alerts["warning_count"] >= 1
    assert {
        alert["kind"]
        for alert in runtime_alerts["alerts"]
    } >= {"profit_target_coverage_degraded"}

    engine.dispose()


def test_pending_broker_order_symbols_include_flat_submitted_sell() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        session.add(
            BrokerOrder(
                mode="paper",
                idempotency_key="monitoring:target_sell:flat-submitted",
                broker_order_id="flat-submitted-sell-1",
                symbol="003280",
                side="sell",
                status="submitted",
                quantity=150,
            )
        )
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="003280",
                quantity=0,
                average_price=Decimal("1988.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
            )
        )
        session.commit()

        assert _pending_broker_order_symbols(
            settings=app.state.settings,
            db=session,
            symbols=None,
        ) == {"003280"}

    engine.dispose()


def test_autonomous_monitoring_prioritizes_watch_buys_over_nonheld_stale_sell_pending() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            monitoring_max_symbols_per_cycle=2,
        )
    )
    client = TestClient(app)

    stale_sell = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-stale-nonheld-sell-pending",
            "source": "kai-trading-core",
            "market_date": "2026-06-01",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "trigger_price": "80000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
                {
                    "symbol": "000660",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "180000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                },
            ],
        },
    )
    watch_buy = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-current-watch-buy",
            "source": "kai-trading-core",
            "market_date": "2026-06-12",
            "items": [
                {
                    "symbol": "035420",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "180000.00",
                    "limit_price": "181000.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                }
            ],
        },
    )
    assert stale_sell.status_code == 201
    assert watch_buy.status_code == 201
    watch_buy_item_id = watch_buy.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
        )

        assert [item.id for item in autonomous_items] == [watch_buy_item_id]

    engine.dispose()


def test_autonomous_monitoring_includes_watch_buys_when_positions_are_held() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            monitoring_max_symbols_per_cycle=10,
        )
    )
    client = TestClient(app)

    held_stop = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-held-stop-and-watch-buy",
            "source": "kai-trading-core",
            "market_date": "2026-06-12",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "70000.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                }
            ],
        },
    )
    watch_buy = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-watch-buy-with-held-position",
            "source": "kai-trading-core",
            "market_date": "2026-06-12",
            "items": [
                {
                    "symbol": "035420",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "trigger_price": "180000.00",
                    "limit_price": "181000.00",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                }
            ],
        },
    )
    assert held_stop.status_code == 201
    assert watch_buy.status_code == 201
    held_stop_item_id = held_stop.json()["persisted_execution_item_ids"][0]
    watch_buy_item_id = watch_buy.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="paper",
                symbol="005930",
                quantity=1,
                average_price=Decimal("72000.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 12, 0, 0, tzinfo=UTC),
            )
        )
        held_stop_item = session.get(ExecutionPlanItem, held_stop_item_id)
        assert held_stop_item is not None
        held_stop_item.status = "stop_sell_monitoring"
        session.commit()

        symbols = list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=datetime(2026, 6, 12, tzinfo=UTC).date(),
        )
        autonomous_items = _load_monitored_items(
            settings=app.state.settings,
            db=session,
            exclude_pending_config=True,
            symbols=symbols,
        )

        assert symbols == ["005930", "035420"]
        assert [item.id for item in autonomous_items] == [held_stop_item_id, watch_buy_item_id]

    engine.dispose()


def test_duplicate_execution_plan_is_rejected() -> None:
    app, engine = build_test_app()
    client = TestClient(app)
    payload = {
        "plan_id": "duplicate-plan",
        "source": "kai-trading-core",
        "items": [
            {
                "symbol": "005930",
                "side": "buy",
                "intent": "next_open_buy",
            }
        ],
    }

    first_response = client.post("/execution-plans", json=payload)
    second_response = client.post("/execution-plans", json=payload)

    assert first_response.status_code == 201
    assert second_response.status_code == 409
    second_payload = second_response.json()
    assert second_payload["plan_id"] == "duplicate-plan"
    assert second_payload["accepted_item_count"] == 0
    assert second_payload["rejected_item_count"] == 1
    assert second_payload["persisted_execution_item_ids"] == []
    assert second_payload["validation_failures"] == [
        {
            "field": "plan_id",
            "message": "execution plan with this plan_id already exists",
        }
    ]

    engine.dispose()


def test_execution_plan_intake_allows_same_plan_id_in_different_runtime_modes(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'shared-execution-plans.db'}"
    payload = {
        "plan_id": "shared-plan",
        "source": "kai-trading-core",
        "items": [
            {
                "symbol": "005930",
                "side": "buy",
                "intent": "next_open_buy",
            }
        ],
    }

    paper_app, paper_engine = build_test_app(
        Settings(
            database_url=database_url,
            runtime_mode="paper",
        )
    )
    live_app, live_engine = build_test_app(
        Settings(
            database_url=database_url,
            runtime_mode="live",
        )
    )

    paper_client = TestClient(paper_app)
    live_client = TestClient(live_app)

    paper_response = paper_client.post("/execution-plans", json=payload)
    live_response = live_client.post("/execution-plans", json=payload)

    assert paper_response.status_code == 201
    assert live_response.status_code == 201
    assert paper_response.json()["runtime"]["mode"] == "paper"
    assert live_response.json()["runtime"]["mode"] == "live"

    with paper_app.state.db_session_factory() as session:
        persisted_plans = session.scalars(
            select(ExecutionPlan)
            .where(ExecutionPlan.external_plan_id == "shared-plan")
            .order_by(ExecutionPlan.mode.asc())
        ).all()
        assert [plan.mode for plan in persisted_plans] == ["live", "paper"]

    paper_engine.dispose()
    live_engine.dispose()


def test_execution_plan_reads_are_filtered_to_active_runtime_mode() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        paper_plan = ExecutionPlan(
            external_plan_id="paper-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        paper_plan.items.append(
            ExecutionPlanItem(
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
            )
        )
        live_plan = ExecutionPlan(
            external_plan_id="live-plan",
            source="kai-trading-core",
            mode="live",
            status="accepted",
        )
        live_plan.items.append(
            ExecutionPlanItem(
                symbol="000660",
                side="sell",
                intent="target_sell",
                status="pending",
            )
        )
        session.add_all([paper_plan, live_plan])
        session.commit()

    client = TestClient(app)

    response = client.get("/execution-plans")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["plan_id"] == "paper-plan"
    assert payload[0]["mode"] == "paper"
    assert payload[0]["item_count"] == 1
    assert payload[0]["items"][0]["symbol"] == "005930"

    engine.dispose()


def test_order_execution_and_position_read_endpoints_return_persisted_state() -> None:
    app, engine = build_test_app()

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-with-broker-state",
            source="kai-trading-core",
            mode="mock",
            status="accepted",
        )
        item = ExecutionPlanItem(
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="pending",
        )
        plan.items.append(item)
        session.add(plan)
        session.flush()

        order = BrokerOrder(
            execution_plan_item_id=item.id,
            mode="mock",
            idempotency_key="broker-order-1",
            broker_order_id="kis-100",
            symbol="005930",
            side="buy",
            status="open",
            quantity=10,
            limit_price=Decimal("71000.0000"),
            submitted_at=datetime(2026, 4, 29, 9, 1, tzinfo=UTC),
            last_broker_sync_at=datetime(2026, 4, 29, 9, 2, tzinfo=UTC),
            raw_broker_payload={"result": "accepted"},
        )
        session.add(order)
        session.flush()

        session.add(
            ExecutionFill(
                broker_order_pk=order.id,
                broker_fill_id="fill-1",
                symbol="005930",
                side="buy",
                quantity=10,
                price=Decimal("70950.5000"),
                filled_at=datetime(2026, 4, 29, 9, 3, tzinfo=UTC),
                payload={"fill": "full"},
            )
        )
        session.add(
            ExecutionPosition(
                mode="mock",
                symbol="005930",
                quantity=10,
                average_price=Decimal("70950.5000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 4, 29, 9, 4, tzinfo=UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    client = TestClient(app)

    orders_response = client.get("/orders")
    executions_response = client.get("/executions")
    positions_response = client.get("/positions")

    assert orders_response.status_code == 200
    assert executions_response.status_code == 200
    assert positions_response.status_code == 200

    orders_payload = orders_response.json()
    executions_payload = executions_response.json()
    positions_payload = positions_response.json()

    assert len(orders_payload) == 1
    assert orders_payload[0]["broker_order_id"] == "kis-100"
    assert orders_payload[0]["status"] == "open"
    assert orders_payload[0]["limit_price"] == "71000.0000"

    assert len(executions_payload) == 1
    assert executions_payload[0]["broker_fill_id"] == "fill-1"
    assert executions_payload[0]["price"] == "70950.5000"

    assert len(positions_payload) == 1
    assert positions_payload[0]["symbol"] == "005930"
    assert positions_payload[0]["average_price"] == "70950.5000"

    engine.dispose()


def test_order_execution_and_position_reads_are_filtered_to_active_runtime_mode() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        paper_plan = ExecutionPlan(
            external_plan_id="paper-read-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        paper_item = ExecutionPlanItem(
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="pending",
        )
        paper_plan.items.append(paper_item)

        live_plan = ExecutionPlan(
            external_plan_id="live-read-plan",
            source="kai-trading-core",
            mode="live",
            status="accepted",
        )
        live_item = ExecutionPlanItem(
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="pending",
        )
        live_plan.items.append(live_item)
        session.add_all([paper_plan, live_plan])
        session.flush()

        paper_order = BrokerOrder(
            execution_plan_item_id=paper_item.id,
            mode="paper",
            idempotency_key="paper-order-read-1",
            broker_order_id="paper-order-100",
            symbol="005930",
            side="buy",
            status="open",
            quantity=2,
            limit_price=Decimal("71000.0000"),
            submitted_at=datetime(2026, 4, 29, 9, 1, tzinfo=UTC),
        )
        live_order = BrokerOrder(
            execution_plan_item_id=live_item.id,
            mode="live",
            idempotency_key="live-order-read-1",
            broker_order_id="live-order-100",
            symbol="005930",
            side="buy",
            status="open",
            quantity=3,
            limit_price=Decimal("72000.0000"),
            submitted_at=datetime(2026, 4, 29, 9, 2, tzinfo=UTC),
        )
        session.add_all([paper_order, live_order])
        session.flush()

        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=paper_order.id,
                    broker_fill_id="paper-fill-1",
                    symbol="005930",
                    side="buy",
                    quantity=2,
                    price=Decimal("70950.0000"),
                    filled_at=datetime(2026, 4, 29, 9, 3, tzinfo=UTC),
                ),
                ExecutionFill(
                    broker_order_pk=live_order.id,
                    broker_fill_id="live-fill-1",
                    symbol="005930",
                    side="buy",
                    quantity=3,
                    price=Decimal("71950.0000"),
                    filled_at=datetime(2026, 4, 29, 9, 4, tzinfo=UTC),
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("70950.0000"),
                    updated_from="reconciliation",
                ),
                ExecutionPosition(
                    mode="live",
                    symbol="005930",
                    quantity=3,
                    average_price=Decimal("71950.0000"),
                    updated_from="reconciliation",
                ),
            ]
        )
        session.commit()

    client = TestClient(app)

    orders_payload = client.get("/orders").json()
    executions_payload = client.get("/executions").json()
    positions_payload = client.get("/positions").json()

    assert [order["broker_order_id"] for order in orders_payload] == ["paper-order-100"]
    assert [fill["broker_fill_id"] for fill in executions_payload] == ["paper-fill-1"]
    assert len(positions_payload) == 1
    assert positions_payload[0]["symbol"] == "005930"
    assert positions_payload[0]["quantity"] == 2
    assert positions_payload[0]["average_price"] == "70950.0000"

    engine.dispose()


def test_execution_read_endpoints_apply_plan_and_symbol_filters() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        first_plan = ExecutionPlan(
            external_plan_id="first-filter-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        first_item = ExecutionPlanItem(
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="pending",
        )
        first_plan.items.append(first_item)
        second_plan = ExecutionPlan(
            external_plan_id="second-filter-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        second_item = ExecutionPlanItem(
            symbol="000660",
            side="buy",
            intent="next_open_buy",
            status="pending",
        )
        second_plan.items.append(second_item)
        session.add_all([first_plan, second_plan])
        session.flush()

        first_order = BrokerOrder(
            execution_plan_item_id=first_item.id,
            mode="paper",
            idempotency_key="first-filter-order",
            broker_order_id="first-filter-order",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=1,
        )
        second_order = BrokerOrder(
            execution_plan_item_id=second_item.id,
            mode="paper",
            idempotency_key="second-filter-order",
            broker_order_id="second-filter-order",
            symbol="000660",
            side="buy",
            status="filled",
            quantity=1,
        )
        session.add_all([first_order, second_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=first_order.id,
                    broker_fill_id="first-filter-fill",
                    symbol="005930",
                    side="buy",
                    quantity=1,
                    price=Decimal("70000.0000"),
                    filled_at=datetime(2026, 4, 29, 9, 3, tzinfo=UTC),
                ),
                ExecutionFill(
                    broker_order_pk=second_order.id,
                    broker_fill_id="second-filter-fill",
                    symbol="000660",
                    side="buy",
                    quantity=1,
                    price=Decimal("180000.0000"),
                    filled_at=datetime(2026, 4, 29, 9, 4, tzinfo=UTC),
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("70000.0000"),
                    updated_from="reconciliation",
                ),
                ExecutionPosition(
                    mode="paper",
                    symbol="000660",
                    quantity=1,
                    average_price=Decimal("180000.0000"),
                    updated_from="reconciliation",
                ),
            ]
        )
        session.commit()

    client = TestClient(app)

    assert [plan["plan_id"] for plan in client.get("/execution-plans?external_plan_id=first-filter-plan").json()] == [
        "first-filter-plan"
    ]
    assert [
        fill["broker_fill_id"]
        for fill in client.get("/executions?external_plan_id=first-filter-plan&symbols=005930,000660").json()
    ] == ["first-filter-fill"]
    assert [
        order["broker_order_id"]
        for order in client.get("/orders?external_plan_id=first-filter-plan&symbols=005930,000660").json()
    ] == ["first-filter-order"]
    assert [position["symbol"] for position in client.get("/positions?symbols=000660").json()] == ["000660"]

    engine.dispose()


def test_runtime_status_exposes_core_facing_symbol_summary() -> None:
    app, engine = build_test_app(settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"))
    today_start_utc, _ = runtime_service._current_krx_day_utc_bounds()
    today_edge_utc = today_start_utc + timedelta(minutes=15)
    today_later_utc = today_start_utc + timedelta(hours=1)
    yesterday_edge_utc = today_start_utc - timedelta(minutes=15)
    # SQLite commonly drops tzinfo for DateTime(timezone=True); keep the fixture naive
    # to verify the runtime summary still treats persisted values as UTC.
    today_edge_naive = today_edge_utc.replace(tzinfo=None)
    today_later_naive = today_later_utc.replace(tzinfo=None)
    yesterday_edge_naive = yesterday_edge_utc.replace(tzinfo=None)

    with app.state.db_session_factory() as session:
        paper_plan = ExecutionPlan(
            external_plan_id="paper-summary-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        monitored_item = ExecutionPlanItem(
            symbol="005930",
            side="sell",
            intent="target_sell",
            status="target_sell_monitoring",
        )
        duplicate_monitored_item = ExecutionPlanItem(
            symbol="005930",
            side="sell",
            intent="stop_sell",
            status="stop_sell_monitoring",
        )
        scheduled_item = ExecutionPlanItem(
            symbol="000660",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_scheduled",
        )
        historical_scheduled_item = ExecutionPlanItem(
            symbol="207940",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_retry_pending",
        )
        failed_item = ExecutionPlanItem(
            symbol="035420",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_failed",
        )
        historical_failed_item = ExecutionPlanItem(
            symbol="051910",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_failed",
        )
        historical_fill_item = ExecutionPlanItem(
            symbol="066570",
            side="buy",
            intent="next_open_buy",
            status="filled",
        )
        paper_plan.items.extend(
            [
                monitored_item,
                duplicate_monitored_item,
                scheduled_item,
                historical_scheduled_item,
                failed_item,
                historical_failed_item,
                historical_fill_item,
            ]
        )
        session.add(paper_plan)
        session.flush()

        session.add_all(
            [
                ExecutionJob(
                    execution_plan_item_id=scheduled_item.id,
                    job_type="next_open_buy",
                    status="scheduled",
                    scheduled_for=today_edge_naive,
                ),
                ExecutionJob(
                    execution_plan_item_id=historical_scheduled_item.id,
                    job_type="next_open_buy",
                    status="retrying",
                    scheduled_for=yesterday_edge_naive,
                ),
            ]
        )
        open_order = BrokerOrder(
            execution_plan_item_id=scheduled_item.id,
            mode="paper",
            idempotency_key="paper-open-order",
            broker_order_id="kis-open-1",
            symbol="000660",
            side="buy",
            status="open",
            quantity=3,
            submitted_at=today_edge_naive,
            last_broker_sync_at=today_edge_naive,
        )
        failed_order = BrokerOrder(
            execution_plan_item_id=failed_item.id,
            mode="paper",
            idempotency_key="paper-failed-order",
            broker_order_id="kis-failed-1",
            symbol="035420",
            side="buy",
            status="failed",
            quantity=2,
            submitted_at=today_edge_naive,
            last_broker_sync_at=None,
            last_error_message="broker rejected order",
        )
        historical_failed_order = BrokerOrder(
            execution_plan_item_id=historical_failed_item.id,
            mode="paper",
            idempotency_key="paper-failed-order-yesterday",
            broker_order_id="kis-failed-2",
            symbol="051910",
            side="buy",
            status="rejected",
            quantity=1,
            submitted_at=yesterday_edge_naive,
            last_broker_sync_at=None,
            last_error_message="historical rejection",
        )
        historical_filled_order = BrokerOrder(
            execution_plan_item_id=historical_fill_item.id,
            mode="paper",
            idempotency_key="paper-filled-order-yesterday",
            broker_order_id="kis-filled-2",
            symbol="066570",
            side="buy",
            status="filled",
            quantity=4,
            submitted_at=yesterday_edge_naive,
            last_broker_sync_at=yesterday_edge_naive,
        )
        session.add_all(
            [
                open_order,
                failed_order,
                historical_failed_order,
                historical_filled_order,
            ]
        )
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=open_order.id,
                    broker_fill_id="fill-paper-1",
                    symbol="000660",
                    side="buy",
                    quantity=2,
                    price=Decimal("125000.0000"),
                    filled_at=today_edge_naive,
                    payload={"source": "broker"},
                ),
                ExecutionFill(
                    broker_order_pk=open_order.id,
                    broker_fill_id="fill-paper-2",
                    symbol="000660",
                    side="buy",
                    quantity=1,
                    price=Decimal("125050.0000"),
                    filled_at=today_later_naive,
                    payload={"source": "broker"},
                ),
                ExecutionFill(
                    broker_order_pk=historical_filled_order.id,
                    broker_fill_id="fill-paper-old",
                    symbol="066570",
                    side="buy",
                    quantity=4,
                    price=Decimal("88000.0000"),
                    filled_at=yesterday_edge_naive,
                    payload={"source": "broker"},
                ),
            ]
        )

        live_plan = ExecutionPlan(
            external_plan_id="live-summary-plan",
            source="kai-trading-core",
            mode="live",
            status="accepted",
        )
        live_plan.items.append(
            ExecutionPlanItem(
                symbol="251340",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
            )
        )
        session.add(live_plan)
        session.commit()

    client = TestClient(app)
    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"] == {"open_orders": 1, "active_execution_jobs": 2}
    assert payload["symbols"] == {
        "monitored": ["005930"],
        "scheduled_today": ["000660"],
        "filled_today": ["000660"],
        "filled_today_by_side": {"buy": ["000660"], "sell": []},
        "filled_today_counts": {
            "all": {"symbols": 1, "fills": 2, "quantity": 3},
            "buy": {"symbols": 1, "fills": 2, "quantity": 3},
            "sell": {"symbols": 0, "fills": 0, "quantity": 0},
        },
        "failed_today": ["035420"],
        "positions": [],
    }

    engine.dispose()


def test_runtime_status_deduplicates_legacy_fills_before_daily_counts() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    now = datetime.now(UTC)
    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="legacy-duplicate-fill-buy",
            symbol="282330",
            side="buy",
            status="filled",
            quantity=6,
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="legacy-duplicate-fill-sell",
            symbol="282330",
            side="sell",
            status="filled",
            quantity=2,
        )
        session.add_all([buy_order, sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    symbol="282330",
                    side="buy",
                    quantity=3,
                    price=Decimal("125000.0000"),
                    filled_at=now,
                ),
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    symbol="282330",
                    side="buy",
                    quantity=3,
                    price=Decimal("125000.0000"),
                    filled_at=now,
                ),
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="legacy-dedup-distinct-sell",
                    symbol="282330",
                    side="sell",
                    quantity=2,
                    price=Decimal("126000.0000"),
                    filled_at=now,
                ),
            ]
        )
        session.commit()

    client = TestClient(app)
    payload = client.get("/runtime/status").json()

    assert payload["symbols"]["filled_today_counts"] == {
        "all": {"symbols": 1, "fills": 2, "quantity": 5},
        "buy": {"symbols": 1, "fills": 1, "quantity": 3},
        "sell": {"symbols": 1, "fills": 1, "quantity": 2},
    }

    engine.dispose()


def test_fast_runtime_status_uses_autonomous_snapshot_for_monitored_symbols() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    app.state.autonomous_runtime._monitoring_state.status = "running"
    app.state.autonomous_runtime._monitoring_state.last_cycle = {
        "monitored_symbols": ["005930", "000660", "005930"],
    }
    app.state.autonomous_runtime._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=datetime.now(UTC),
        symbols=["005930", "000660", "005930"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=["282330"],
    )
    now = datetime.now(UTC)
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="fast-runtime-monitored-truth",
            source="test",
            mode="paper",
            market_date=now.date(),
        )
        session.add(plan)
        session.flush()
        session.add(
            ExecutionPlanItem(
                execution_plan_id=plan.id,
                symbol="035420",
                side="buy",
                intent="watch_buy",
                status="watch_monitoring_active",
                plan_item_index=0,
            )
        )
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="fast-runtime-buy",
            symbol="282330",
            side="buy",
            status="filled",
            quantity=3,
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="fast-runtime-sell",
            symbol="282330",
            side="sell",
            status="filled",
            quantity=3,
        )
        session.add_all([buy_order, sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="fast-runtime-buy-fill",
                    symbol="282330",
                    side="buy",
                    quantity=3,
                    price=Decimal("125000.0000"),
                    filled_at=now - timedelta(hours=1),
                ),
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="fast-runtime-sell-fill",
                    symbol="282330",
                    side="sell",
                    quantity=3,
                    price=Decimal("121100.0000"),
                    filled_at=now,
                ),
            ]
        )
        session.commit()
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["freshness"]["summary_source"] == "fast_autonomous_runtime_snapshot"
    assert payload["symbols"]["monitored"] == ["000660", "005930"]
    assert payload["symbols"]["monitoring_cycle_batch"] == ["000660", "005930"]
    assert payload["symbols"]["filled_today"] == ["282330"]
    assert payload["symbols"]["filled_today_by_side"] == {"buy": [], "sell": []}
    assert payload["symbols"]["filled_today_counts"] == {
        "all": {"symbols": 0, "fills": 0, "quantity": 0},
        "buy": {"symbols": 0, "fills": 0, "quantity": 0},
        "sell": {"symbols": 0, "fills": 0, "quantity": 0},
    }
    assert payload["monthly_pnl"]["pnl_method"] == "fifo_internal_fills"
    assert payload["monthly_pnl"]["monthly_realized_pnl"] == "0.0000"
    assert payload["monthly_pnl"]["matched_sell_quantity"] == 0
    assert payload["weekly_pnl"]["pnl_method"] == "fifo_internal_fills"
    assert payload["weekly_pnl"]["weekly_realized_pnl"] == "0.0000"
    assert payload["weekly_pnl"]["weekly_buy_notional"] == "0.0000"
    assert payload["weekly_pnl"]["weekly_sell_notional"] == "0.0000"
    assert payload["weekly_pnl"]["matched_sell_quantity"] == 0
    assert payload["weekly_pnl"]["closed_symbols"] == []
    assert payload["daily_pnl"]["pnl_method"] == "fifo_internal_fills"
    assert payload["daily_pnl"]["daily_realized_pnl"] == "0.0000"
    assert payload["daily_pnl"]["daily_buy_notional"] == "0.0000"
    assert payload["daily_pnl"]["daily_sell_notional"] == "0.0000"
    assert payload["daily_pnl"]["matched_sell_quantity"] == 0
    assert payload["daily_pnl"]["closed_symbols"] == []

    engine.dispose()


def test_fast_runtime_status_honors_durable_paused_state() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            autonomous_runtime_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["freshness"]["summary_source"] == "fast_autonomous_runtime_snapshot"
    assert payload["ready"] is False
    assert "runtime is paused" in payload["readiness"]["reasons"]
    assert payload["safety"]["paused"] is True
    assert payload["safety"]["kill_switch_enabled"] is False
    assert payload["safety"]["automated_order_submission_allowed"] is False
    assert payload["safety"]["automated_order_submission_block_reason"] == "paused"

    engine.dispose()


def test_fast_runtime_status_honors_durable_kill_switch_state() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            autonomous_runtime_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    client = TestClient(app)

    kill_switch_response = client.post(
        "/controls/kill-switch",
        json={
            "actor": "ops",
            "source": "console",
            "reason": "operator kill switch",
        },
    )
    assert kill_switch_response.status_code == 200

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["freshness"]["summary_source"] == "fast_autonomous_runtime_snapshot"
    assert payload["ready"] is False
    assert "kill switch is enabled" in payload["readiness"]["reasons"]
    assert payload["safety"]["paused"] is True
    assert payload["safety"]["kill_switch_enabled"] is True
    assert payload["safety"]["automated_order_submission_allowed"] is False
    assert payload["safety"]["automated_order_submission_block_reason"] == "kill_switch"

    engine.dispose()


def test_fast_runtime_status_uses_cached_filled_symbols() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    client = TestClient(app)

    payload = client.get("/runtime/status").json()
    assert payload["symbols"]["filled_today"] == []
    assert payload["symbols"]["filled_today_by_side"] == {"buy": [], "sell": []}
    assert payload["symbols"]["filled_today_counts"] == {
        "all": {"symbols": 0, "fills": 0, "quantity": 0},
        "buy": {"symbols": 0, "fills": 0, "quantity": 0},
        "sell": {"symbols": 0, "fills": 0, "quantity": 0},
    }

    app.state.autonomous_runtime._monitoring_runtime_snapshot = (
        app.state.autonomous_runtime._load_monitoring_runtime_snapshot(datetime.now(UTC))
    )
    app.state.autonomous_runtime._monitoring_runtime_snapshot = (
        app.state.autonomous_runtime._monitoring_runtime_snapshot.__class__(
            loaded_at=datetime.now(UTC),
            symbols=[],
            positions_by_symbol={},
            items=[],
            filled_today_symbols=["003280", "282330", "003280"],
        )
    )

    payload = client.get("/runtime/status").json()
    assert payload["freshness"]["summary_source"] == "fast_autonomous_runtime_snapshot"
    assert payload["symbols"]["filled_today"] == ["003280", "282330"]
    assert payload["symbols"]["filled_today_by_side"] == {"buy": [], "sell": []}
    assert payload["symbols"]["filled_today_counts"] == {
        "all": {"symbols": 0, "fills": 0, "quantity": 0},
        "buy": {"symbols": 0, "fills": 0, "quantity": 0},
        "sell": {"symbols": 0, "fills": 0, "quantity": 0},
    }

    engine.dispose()


def test_runtime_status_counts_are_filtered_to_active_runtime_mode() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        paper_plan = ExecutionPlan(
            external_plan_id="paper-count-plan",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
        )
        paper_item = ExecutionPlanItem(
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_scheduled",
        )
        paper_plan.items.append(paper_item)

        live_plan = ExecutionPlan(
            external_plan_id="live-count-plan",
            source="kai-trading-core",
            mode="live",
            status="accepted",
        )
        live_item = ExecutionPlanItem(
            symbol="000660",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_scheduled",
        )
        live_plan.items.append(live_item)
        session.add_all([paper_plan, live_plan])
        session.flush()

        session.add_all(
            [
                ExecutionJob(
                    execution_plan_item_id=paper_item.id,
                    job_type="next_open_buy",
                    status="scheduled",
                ),
                ExecutionJob(
                    execution_plan_item_id=live_item.id,
                    job_type="next_open_buy",
                    status="running",
                ),
                BrokerOrder(
                    execution_plan_item_id=paper_item.id,
                    mode="paper",
                    idempotency_key="paper-count-order",
                    broker_order_id="paper-count-order-1",
                    symbol="005930",
                    side="buy",
                    status="open",
                    quantity=1,
                ),
                BrokerOrder(
                    execution_plan_item_id=live_item.id,
                    mode="live",
                    idempotency_key="live-count-order",
                    broker_order_id="live-count-order-1",
                    symbol="000660",
                    side="buy",
                    status="open",
                    quantity=1,
                ),
            ]
        )
        session.commit()

    client = TestClient(app)
    payload = client.get("/runtime/status").json()

    assert payload["counts"] == {"open_orders": 1, "active_execution_jobs": 1}

    engine.dispose()
