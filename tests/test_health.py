import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.testclient import TestClient
import httpx
import pytest
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import load_only

from app.api.schemas.execution import MonitoringPriceRequest
from app.api.routes import runtime as runtime_routes
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
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
from app.services import schema as schema_service
from app.services.execution_account_route import bind_kis_account_route
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
    payload_with_position_identity,
)
from app.services.autonomous_runtime import MonitoringRuntimeSnapshot, _is_regular_market_session, _merge_symbol_priority
from app.services.monitoring import (
    _maintain_post_entry_exit_monitors,
    _load_monitored_items,
    _monitoring_item_is_configured,
    _pending_broker_order_symbols,
    _plan_item_exchange,
    _resolve_sell_quantity,
    expire_known_flat_pending_sell_monitors,
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


def _freeze_runtime_service_now(monkeypatch, now: datetime) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            if tz is None:
                return now.replace(tzinfo=None)
            return now.astimezone(tz)

    monkeypatch.setattr(runtime_service, "datetime", FixedDatetime)


def _execution_scope_metadata(
    settings: Settings,
    *,
    provider: str = "kis",
    exchange: str = "KRX",
    venue: str | None = None,
    asset_class: str = "kr_equity",
) -> dict[str, object]:
    identity = build_position_identity(
        settings=settings,
        provider=provider,
        exchange=exchange,
    )
    return {
        "exchange": identity.exchange,
        "execution_scope": execution_scope_payload(
            identity=identity,
            venue=venue or exchange,
            asset_class=asset_class,
        ),
    }


def _scoped_execution_position(
    settings: Settings,
    *,
    symbol: str,
    quantity: int,
    mode: str | None = None,
    provider: str = "kis",
    exchange: str = "KRX",
    raw_payload: object = None,
    **values: object,
) -> ExecutionPosition:
    target_mode = mode or settings.runtime_mode
    identity_settings = settings
    if target_mode != settings.runtime_mode:
        identity_settings = settings.model_copy(update={"runtime_mode": target_mode})
        if target_mode == "live":
            identity_settings = identity_settings.model_copy(
                update={"execution_position_account_scope": "inactive-test-route"}
            )
    identity = build_position_identity(
        settings=identity_settings,
        provider=provider,
        exchange=exchange,
    )
    return ExecutionPosition(
        mode=target_mode,
        symbol=symbol,
        quantity=quantity,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        raw_payload=payload_with_position_identity(
            raw_payload,
            identity=identity,
        ),
        **values,
    )


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


@pytest.mark.parametrize(
    ("inspection_method", "table_name", "expected_invariant"),
    [
        pytest.param(
            "get_unique_constraints",
            "execution_jobs",
            "uq_execution_jobs_plan_item_job_type",
            id="unique-constraint",
        ),
        pytest.param(
            "get_check_constraints",
            "execution_positions",
            "ck_execution_positions_scope_complete",
            id="check-constraint",
        ),
        pytest.param(
            "get_indexes",
            "execution_positions",
            "uq_execution_positions_legacy_symbol_mode",
            id="unique-index",
        ),
        pytest.param(
            "get_indexes",
            "crypto_strategy_states",
            "uq_crypto_strategy_states_legacy_mode_provider_version_symbol",
            id="crypto-state-legacy-unique-index",
        ),
        pytest.param(
            "get_indexes",
            "crypto_strategy_risk_states",
            "uq_crypto_strategy_risk_states_legacy_mode_provider_version",
            id="crypto-risk-legacy-unique-index",
        ),
        pytest.param(
            "get_indexes",
            "crypto_strategy_decisions",
            "uq_crypto_strategy_decisions_legacy_scope_bar",
            id="crypto-decision-legacy-unique-index",
        ),
    ],
)
def test_health_degrades_when_a_safety_invariant_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    inspection_method: str,
    table_name: str,
    expected_invariant: str,
) -> None:
    app, engine = build_test_app()
    actual_inspector = inspect(engine)

    class MissingInvariantInspector:
        def __getattr__(self, name: str):
            if name == inspection_method:
                return lambda inspected_table: (
                    []
                    if inspected_table == table_name
                    else getattr(actual_inspector, name)(inspected_table)
                )
            return getattr(actual_inspector, name)

    monkeypatch.setattr(
        schema_service,
        "inspect",
        lambda _bind: MissingInvariantInspector(),
    )

    payload = TestClient(app).get("/health").json()

    assert payload["status"] == "degraded"
    runtime_schema = payload["dependencies"]["runtime_schema"]
    assert runtime_schema["status"] == "missing"
    assert expected_invariant in runtime_schema["missing_safety_invariants"][
        table_name
    ]
    engine.dispose()


@pytest.mark.parametrize(
    ("table_name", "index_name"),
    [
        (
            "fifo_inventory_isolations",
            "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled",
        ),
        (
            "execution_positions",
            "uq_execution_positions_legacy_symbol_mode",
        ),
        (
            "monitoring_checkpoints",
            "uq_monitoring_checkpoints_legacy_worker_mode",
        ),
        (
            "crypto_strategy_states",
            "uq_crypto_strategy_states_legacy_mode_provider_version_symbol",
        ),
        (
            "crypto_strategy_risk_states",
            "uq_crypto_strategy_risk_states_legacy_mode_provider_version",
        ),
        (
            "crypto_strategy_decisions",
            "uq_crypto_strategy_decisions_legacy_scope_bar",
        ),
    ],
)
def test_health_degrades_when_legacy_index_predicate_is_wrong(
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
    index_name: str,
) -> None:
    app, engine = build_test_app()
    actual_inspector = inspect(engine)

    class WrongPredicateInspector:
        def __getattr__(self, name: str):
            if name != "get_indexes":
                return getattr(actual_inspector, name)

            def get_indexes(inspected_table: str):
                indexes = actual_inspector.get_indexes(inspected_table)
                if inspected_table != table_name:
                    return indexes
                return [
                    {
                        **index,
                        "dialect_options": {
                            "sqlite_where": text("1 = 1")
                        },
                    }
                    if index["name"] == index_name
                    else index
                    for index in indexes
                ]

            return get_indexes

    monkeypatch.setattr(
        schema_service,
        "inspect",
        lambda _bind: WrongPredicateInspector(),
    )

    payload = TestClient(app).get("/health").json()

    assert payload["status"] == "degraded"
    runtime_schema = payload["dependencies"]["runtime_schema"]
    assert runtime_schema["status"] == "missing"
    assert index_name in runtime_schema["missing_safety_invariants"][table_name]
    engine.dispose()


def test_schema_check_validation_accepts_postgres_truncated_constraint_name() -> None:
    expression = schema_service.REQUIRED_CHECK_CONSTRAINTS[
        "fifo_inventory_isolations"
    ]["ck_fifo_inventory_isolations_scope_complete"]

    assert schema_service._has_check_constraint(
        [
            {
                "name": "ck_fifo_inventory_isolations_ck_fifo_inventory_isola_9f31",
                "sqltext": expression,
            }
        ],
        expression=expression,
    )


def test_schema_check_validation_accepts_postgres_text_casts() -> None:
    expression = schema_service.REQUIRED_CHECK_CONSTRAINTS[
        "core_buy_authority_states"
    ]["ck_core_buy_authority_states_krx_scope"]

    assert schema_service._has_check_constraint(
        [
            {
                "name": "ck_core_buy_authority_states_krx_scope",
                "sqltext": (
                    "(provider)::text = 'kis'::text "
                    "AND (market)::text = 'KRX'::text "
                    "AND (currency)::text = 'KRW'::text"
                ),
            }
        ],
        expression=expression,
    )


def test_schema_check_validation_preserves_cast_like_text_inside_literal() -> None:
    assert not schema_service._has_check_constraint(
        [
            {
                "name": "different-provider-check",
                "sqltext": "provider::text = 'ki::texts'::text",
            }
        ],
        expression="provider = 'kis'",
    )


@pytest.mark.parametrize(
    "sqltext",
    [
        (
            "provider::text = 'KIS'::text "
            "AND market::text = 'KRX'::text "
            "AND currency::text = 'KRW'::text"
        ),
        "provider::text = 'kis'::text AND market::text = 'KRX'::text",
        (
            "provider::text = 'kis'::text "
            "AND market::text = 'KRX'::text "
            "AND currency::text = 'USD'::text"
        ),
    ],
)
def test_schema_check_validation_rejects_semantically_different_casted_checks(
    sqltext: str,
) -> None:
    expression = schema_service.REQUIRED_CHECK_CONSTRAINTS[
        "core_buy_authority_states"
    ]["ck_core_buy_authority_states_krx_scope"]

    assert not schema_service._has_check_constraint(
        [{"name": "different-scope-check", "sqltext": sqltext}],
        expression=expression,
    )


def test_runtime_schema_required_head_matches_alembic_script_head() -> None:
    config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    script = ScriptDirectory.from_config(config)

    assert sorted(schema_service.REQUIRED_ALEMBIC_HEADS) == sorted(
        script.get_heads()
    )


def test_health_degrades_when_alembic_revision_is_behind_head() -> None:
    app, engine = build_test_app()
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE alembic_version SET version_num = '202609110001'"
        )

    payload = TestClient(app).get("/health").json()

    assert payload["status"] == "degraded"
    runtime_schema = payload["dependencies"]["runtime_schema"]
    assert runtime_schema["status"] == "missing"
    assert runtime_schema["alembic"] == {
        "status": "behind",
        "expected_heads": ["202609110003"],
        "current_heads": ["202609110001"],
    }
    engine.dispose()


def test_health_degrades_when_alembic_version_table_is_missing() -> None:
    app, engine = build_test_app()
    with engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE alembic_version")

    payload = TestClient(app).get("/health").json()

    assert payload["status"] == "degraded"
    runtime_schema = payload["dependencies"]["runtime_schema"]
    assert runtime_schema["status"] == "missing"
    assert runtime_schema["alembic"] == {
        "status": "missing",
        "expected_heads": ["202609110003"],
        "current_heads": [],
    }
    engine.dispose()


def test_lifespan_keeps_automated_workers_stopped_when_schema_is_behind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=True,
            alpaca_us_equity_monitoring_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE alembic_version SET version_num = '202609110001'"
        )
    starts: list[str] = []

    def tracked_start(name: str):
        async def start() -> None:
            starts.append(name)

        return start

    monkeypatch.setattr(
        app.state.autonomous_runtime,
        "start",
        tracked_start("autonomous"),
    )
    monkeypatch.setattr(
        app.state.alpaca_crypto_runtime,
        "start",
        tracked_start("alpaca_crypto"),
    )
    monkeypatch.setattr(
        app.state.alpaca_us_equity_runtime,
        "start",
        tracked_start("alpaca_us_equity"),
    )

    with TestClient(app) as client:
        payload = client.get("/health").json()
        runtime_status = client.get("/runtime/status").json()

        assert starts == []
        assert payload["status"] == "degraded"
        assert payload["dependencies"]["automated_runtime_startup"]["status"] == (
            "blocked"
        )
        assert runtime_status["ready"] is False
        assert runtime_status["dependencies"] == {
            "database": "ok",
            "runtime_schema": "missing",
            "automated_runtime_startup": "blocked",
        }
        assert app.state.runtime_startup_gate == {
            "status": "blocked",
            "reason": "runtime_schema_not_ready",
            "runtime_schema_status": "missing",
            "alembic_status": "behind",
        }
        autonomous = app.state.autonomous_runtime.snapshot()
        assert autonomous["dispatch"]["status"] == "blocked"
        assert autonomous["monitoring"]["status"] == "blocked"
        assert app.state.alpaca_crypto_runtime.snapshot()["status"] == "blocked"
        assert app.state.alpaca_us_equity_runtime.snapshot()["status"] == "blocked"

        with engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE alembic_version SET version_num = '202609110003'"
            )

        repaired_health = client.get("/health").json()
        repaired_runtime = client.get("/runtime/status").json()
        repaired_board_runtime = client.get("/runtime/daily-board").json()["runtime"]
        assert repaired_health["dependencies"]["runtime_schema"]["status"] == "ok"
        assert repaired_health["status"] == "degraded"
        assert repaired_runtime["dependencies"]["runtime_schema"] == "ok"
        assert repaired_runtime["dependencies"]["automated_runtime_startup"] == (
            "blocked"
        )
        assert repaired_runtime["ready"] is False
        assert "restart" in " ".join(repaired_runtime["readiness"]["reasons"])
        assert repaired_board_runtime["dependencies"][
            "automated_runtime_startup"
        ] == "blocked"
        assert repaired_board_runtime["ready"] is False

    engine.dispose()


def test_lifespan_blocks_workers_for_unattributed_non_flat_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        ),
        autonomous_runtime_enabled=True,
    )
    with app.state.db_session_factory() as db:
        db.add(
            ExecutionPosition(
                id="legacy-open-position",
                mode="paper",
                provider=None,
                account_scope=None,
                exchange=None,
                symbol="005930",
                quantity=3,
                average_price=Decimal("70000"),
                updated_from="legacy_position_import",
            )
        )
        db.commit()
    starts: list[str] = []

    async def tracked_start() -> None:
        starts.append("autonomous")

    monkeypatch.setattr(app.state.autonomous_runtime, "start", tracked_start)

    with TestClient(app) as client:
        health = client.get("/health").json()
        runtime_status = client.get("/runtime/status").json()

        assert starts == []
        runtime_schema = health["dependencies"]["runtime_schema"]
        assert runtime_schema["status"] == "blocked"
        assert runtime_schema["blocking_data"]["execution_positions"] == {
            "status": "blocked",
            "reason": "non_flat_execution_position_scope_attribution_required",
            "blocking_row_count": 1,
            "remediation": (
                "kis_legacy_position_attribution_dry_run_apply_then_restart"
            ),
        }
        startup_dependency = health["dependencies"]["automated_runtime_startup"]
        assert startup_dependency == {
            "status": "blocked",
            "startup_status": "blocked",
            "reason": "non_flat_execution_position_scope_attribution_required",
            "runtime_schema_status": "blocked",
            "alembic_status": "ok",
            "remediation": (
                "kis_legacy_position_attribution_dry_run_apply_then_restart"
            ),
            "restart_required": True,
        }
        assert app.state.runtime_startup_gate == {
            "status": "blocked",
            "reason": "non_flat_execution_position_scope_attribution_required",
            "runtime_schema_status": "blocked",
            "alembic_status": "ok",
            "remediation": (
                "kis_legacy_position_attribution_dry_run_apply_then_restart"
            ),
            "restart_required": True,
        }
        assert runtime_status["runtime_startup_gate"] == {
            "status": "blocked",
            "reason": "non_flat_execution_position_scope_attribution_required",
            "runtime_schema_status": "blocked",
            "alembic_status": "ok",
            "remediation": (
                "kis_legacy_position_attribution_dry_run_apply_then_restart"
            ),
            "restart_required": True,
        }
        assert runtime_status["safety"]["readiness_reason"] == (
            "non_flat_execution_position_scope_attribution_required; "
            "remediation=kis_legacy_position_attribution_dry_run_apply_then_restart; "
            "restart_required=true"
        )
        readiness_reasons = " ".join(runtime_status["readiness"]["reasons"])
        assert "kis_legacy_position_attribution_dry_run_apply_then_restart" in (
            readiness_reasons
        )
        assert "restart_required=true" in readiness_reasons
        assert "migrat" not in readiness_reasons.casefold()

    engine.dispose()


def test_lifespan_requires_current_schema_before_position_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        ),
        autonomous_runtime_enabled=True,
    )
    with app.state.db_session_factory() as db:
        db.add(
            ExecutionPosition(
                id="legacy-open-position-behind-schema",
                mode="paper",
                provider=None,
                account_scope=None,
                exchange=None,
                symbol="005930",
                quantity=3,
                updated_from="legacy_position_import",
            )
        )
        db.commit()
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE alembic_version SET version_num = '202609110001'"
        )
    starts: list[str] = []

    async def tracked_start() -> None:
        starts.append("autonomous")

    monkeypatch.setattr(app.state.autonomous_runtime, "start", tracked_start)

    with TestClient(app):
        assert starts == []
        assert app.state.runtime_startup_gate == {
            "status": "blocked",
            "reason": "runtime_schema_not_ready",
            "runtime_schema_status": "missing",
            "alembic_status": "behind",
        }

    engine.dispose()


def test_runtime_schema_blocks_partial_non_flat_position_scope() -> None:
    app, engine = build_test_app()
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            text(
                """
                INSERT INTO execution_positions (
                    id,
                    mode,
                    provider,
                    account_scope,
                    exchange,
                    symbol,
                    quantity,
                    updated_from
                ) VALUES (
                    'partial-open-position',
                    'paper',
                    'kis',
                    NULL,
                    'KRX',
                    '000660',
                    2,
                    'corrupt_partial_import'
                )
                """
            )
        )

    with app.state.db_session_factory() as db:
        runtime_schema = schema_service.check_runtime_schema(db)

    assert runtime_schema["status"] == "blocked"
    assert runtime_schema["blocking_data"]["execution_positions"][
        "blocking_row_count"
    ] == 1
    engine.dispose()


def test_lifespan_allows_flat_legacy_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        ),
        autonomous_runtime_enabled=True,
    )
    with app.state.db_session_factory() as db:
        db.add(
            ExecutionPosition(
                id="legacy-flat-position",
                mode="paper",
                provider=None,
                account_scope=None,
                exchange=None,
                symbol="005930",
                quantity=0,
                base_quantity=Decimal("0"),
                average_price=Decimal("70000"),
                updated_from="legacy_position_import",
            )
        )
        db.commit()
    starts: list[str] = []

    async def tracked_start() -> None:
        starts.append("autonomous")

    monkeypatch.setattr(app.state.autonomous_runtime, "start", tracked_start)

    with TestClient(app) as client:
        runtime_schema = client.get("/health").json()["dependencies"][
            "runtime_schema"
        ]

        assert starts == ["autonomous"]
        assert runtime_schema["status"] == "ok"
        assert runtime_schema["blocking_data"] == {}

    engine.dispose()


def test_lifespan_starts_requested_workers_when_schema_is_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=True,
            alpaca_us_equity_monitoring_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    starts: list[str] = []

    def tracked_start(name: str):
        async def start() -> None:
            starts.append(name)

        return start

    monkeypatch.setattr(
        app.state.autonomous_runtime,
        "start",
        tracked_start("autonomous"),
    )
    monkeypatch.setattr(
        app.state.alpaca_crypto_runtime,
        "start",
        tracked_start("alpaca_crypto"),
    )
    monkeypatch.setattr(
        app.state.alpaca_us_equity_runtime,
        "start",
        tracked_start("alpaca_us_equity"),
    )

    with TestClient(app):
        assert starts == ["autonomous", "alpaca_crypto", "alpaca_us_equity"]
        assert app.state.runtime_startup_gate == {
            "status": "ready",
            "reason": None,
            "runtime_schema_status": "ok",
            "alembic_status": "ok",
        }

    engine.dispose()


def test_live_health_reports_missing_stable_execution_account_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="configured-key",
        kis_app_secret="configured-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    scope_health = payload["dependencies"]["execution_account_scope"]
    assert payload["status"] == "degraded"
    assert scope_health["status"] == "error"
    assert scope_health["providers"] == ["kis"]
    assert scope_health["required_settings"] == [
        "KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET",
        "KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE",
    ]
    assert "12345678" not in str(payload)
    assert "configured-secret" not in str(payload)
    engine.dispose()


def test_live_health_accepts_stable_hmac_execution_account_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="configured-key",
        kis_app_secret="configured-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "stable-position-scope-secret-32-bytes"
        ),
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    assert payload["status"] == "ok"
    assert payload["dependencies"]["execution_account_scope"] == {
        "status": "ok",
        "providers": ["kis"],
        "source": "stable_hmac_secret",
    }
    assert "stable-position-scope-secret-32-bytes" not in str(payload)
    engine.dispose()


def test_live_alpaca_health_does_not_accept_route_label_without_hmac_secret() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        alpaca_api_key="configured-key",
        alpaca_api_secret="configured-secret",
        alpaca_live_trading_enabled=True,
        execution_position_account_scope="opaque-live-account-route",
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    scope_health = payload["dependencies"]["execution_account_scope"]
    assert payload["status"] == "degraded"
    assert scope_health["status"] == "error"
    assert scope_health["providers"] == ["alpaca"]
    assert scope_health["required_settings"] == [
        "KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET"
    ]
    engine.dispose()


def test_live_alpaca_health_reports_authenticated_identity_hmac_source() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        alpaca_api_key="configured-key",
        alpaca_api_secret="configured-secret",
        alpaca_live_trading_enabled=True,
        execution_position_account_scope="ignored-when-account-id-is-authenticated",
        account_drawdown_recovery_scope_secret=(
            "stable-position-scope-secret-32-bytes"
        ),
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    assert payload["status"] == "ok"
    assert payload["dependencies"]["execution_account_scope"] == {
        "status": "ok",
        "providers": ["alpaca"],
        "source": "stable_hmac_secret",
    }
    engine.dispose()


def test_live_health_configured_route_label_can_bind_a_kis_plan() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="configured-key",
        kis_app_secret="configured-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        execution_position_account_scope="opaque-live-account-route",
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    health = client.get("/health").json()
    plan = client.post(
        "/execution-plans",
        json={
            "plan_id": "configured-route-label-plan",
            "source": "operator-import",
            "items": [
                {
                    "provider": "kis",
                    "venue": "krx",
                    "asset_class": "stock",
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "70000",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )

    assert health["status"] == "ok"
    assert health["dependencies"]["execution_account_scope"] == {
        "status": "ok",
        "providers": ["kis"],
        "source": "configured_route_label",
    }
    assert plan.status_code == 201
    persisted_item = client.get("/execution-plans").json()[0]["items"][0]
    binding = persisted_item["risk_context"]["execution_account_route"]
    assert binding["version"] == "kis-account-route-v1"
    assert len(binding["configured_route_fingerprint"]) == 32
    engine.dispose()


def test_live_health_ignores_credentials_for_disabled_secondary_providers() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="configured-key",
        kis_app_secret="configured-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        alpaca_api_key="dormant-key",
        alpaca_api_secret="dormant-secret",
        alpaca_live_trading_enabled=False,
        upbit_access_key="dormant-access-key",
        upbit_secret_key="dormant-secret-key",
        upbit_live_trading_enabled=False,
        execution_position_account_scope="opaque-live-account-route",
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    assert payload["status"] == "ok"
    assert payload["dependencies"]["execution_account_scope"] == {
        "status": "ok",
        "providers": ["kis"],
        "source": "configured_route_label",
    }
    assert "dormant-key" not in str(payload)
    assert "dormant-secret" not in str(payload)
    engine.dispose()


def test_live_health_rejects_short_execution_account_scope_hmac_secret() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_app_key="configured-key",
        kis_app_secret="configured-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="short-secret",
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    payload = client.get("/health").json()

    scope_health = payload["dependencies"]["execution_account_scope"]
    assert payload["status"] == "degraded"
    assert scope_health["status"] == "error"
    assert scope_health["invalid_settings"] == [
        "KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET"
    ]
    assert "short-secret" not in str(payload)
    engine.dispose()


def test_settings_repr_hides_account_scope_hmac_secret() -> None:
    secret = "stable-position-scope-secret-32-bytes"

    rendered = repr(Settings(account_drawdown_recovery_scope_secret=secret))

    assert secret not in rendered
    assert "account_drawdown_recovery_scope_secret" not in rendered


def test_live_upbit_health_requires_explicit_route_label_even_with_hmac_key() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        upbit_access_key="configured-key",
        upbit_secret_key="configured-secret",
        upbit_live_trading_enabled=True,
        account_drawdown_recovery_scope_secret=(
            "stable-position-scope-secret-32-bytes"
        ),
    )
    app, engine = build_test_app(settings)
    client = TestClient(app)

    scope_health = client.get("/health").json()["dependencies"][
        "execution_account_scope"
    ]

    assert scope_health["status"] == "error"
    assert scope_health["providers"] == ["upbit"]
    assert scope_health["required_settings"] == [
        "KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE"
    ]
    assert "configured-key" not in str(scope_health)
    assert "configured-secret" not in str(scope_health)
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
    assert payload["safety"]["automated_buy_submission_allowed"] is False
    assert payload["safety"]["automated_buy_submission_block_reason"] == "paused"
    assert payload["exec_agent"] == {
        "status": "disabled",
        "enabled": False,
        "agent_id": "exec-agent",
        "expected_model": "openai/gpt-5.6-sol",
        "provenance_policy": "exact_agent_and_model",
        "trigger_time": {"configured": True, "effective": False},
        "watch_sizing": {"configured": False, "effective": False},
        "reason_codes": ["exec_agent_disabled"],
    }
    assert payload["safety"]["hourly_risk_off"]["available"] is True
    assert payload["safety"]["hourly_risk_off"]["active"] is False
    assert payload["safety"]["last_change"]["action"] == "bootstrap"
    assert payload["account_drawdown_gate"] == {
        "available": True,
        "enabled": True,
        "gated": False,
        "provider": "kis",
        "latest_decision": None,
        "active_recovery_scopes": {},
        "reason": None,
    }
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
    assert payload["account_drawdown_gate"] == {
        "available": False,
        "enabled": True,
        "gated": None,
        "provider": "kis",
        "latest_decision": None,
        "active_recovery_scopes": {},
        "reason": "account_drawdown_gate_state_unavailable",
    }
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


def test_runtime_status_reports_monthly_realized_pnl_progress_and_mode(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monthly_target_krw=Decimal("1000"),
    )
    app, engine = build_test_app(settings=settings)
    client = TestClient(app)
    now = datetime(2026, 6, 17, 3, 0, tzinfo=UTC)
    _freeze_runtime_service_now(monkeypatch, now)

    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="monthly-buy-1",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=10,
            request_metadata=_execution_scope_metadata(settings),
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="monthly-sell-1",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=5,
            request_metadata=_execution_scope_metadata(settings),
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
    assert monthly["monthly_gross_realized_pnl_krw"] == "5000.0000"
    assert monthly["monthly_net_realized_pnl_krw"] is None
    assert monthly["transaction_cost_coverage"] == "unknown"
    assert monthly["monthly_target_krw"] == "1000.0000"
    assert monthly["target_progress_pct"] is None
    assert monthly["mode_recommendation"] == "insufficient_net_pnl_data"
    assert monthly["matched_sell_quantity"] == 5
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] == "5000.0000"
    assert weekly["weekly_net_realized_pnl_krw"] is None
    assert weekly["weekly_buy_notional"] == "100000.0000"
    assert weekly["weekly_sell_notional"] == "55000.0000"
    assert weekly["matched_sell_quantity"] == 5
    assert weekly["unmatched_sell_quantity"] == 0
    assert weekly["closed_trade_count"] == 1
    assert weekly["closed_symbols"] == ["005930"]
    assert weekly["closed_symbol_count"] == 1
    assert weekly["win_count"] == 1
    assert weekly["loss_count"] == 0
    assert weekly["cost_basis_sources"] == {"broker_reconciliation": 0, "fill_fifo": 1, "unknown": 0}
    assert weekly["pnl_method"] == "fifo_internal_fills_with_broker_reconciliation_fallback"
    daily = response.json()["daily_pnl"]
    assert daily["daily_realized_pnl"] == "5000.0000"
    assert daily["daily_net_realized_pnl_krw"] is None
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


def test_runtime_status_weekly_pnl_uses_broker_reconciliation_cost_for_legacy_flat_position() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    app, engine = build_test_app(settings=settings)
    client = TestClient(app)
    now = datetime.now(UTC)
    legacy_buy_at = now - timedelta(days=14)

    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="weekly-legacy-buy",
            symbol="049430",
            side="buy",
            status="filled",
            quantity=45,
            request_metadata=_execution_scope_metadata(settings),
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="weekly-legacy-sell",
            symbol="049430",
            side="sell",
            status="filled",
            quantity=6,
            request_metadata=_execution_scope_metadata(settings),
        )
        session.add_all([buy_order, sell_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="weekly-legacy-buy-fill",
                    symbol="049430",
                    side="buy",
                    quantity=45,
                    price=Decimal("2000"),
                    filled_at=legacy_buy_at,
                ),
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="weekly-legacy-sell-fill",
                    symbol="049430",
                    side="sell",
                    quantity=6,
                    price=Decimal("1500"),
                    filled_at=now,
                ),
                _scoped_execution_position(
                    settings,
                    symbol="049430",
                    quantity=0,
                    average_price=Decimal("1800"),
                    updated_from="reconciliation",
                    last_reconciled_at=now,
                ),
            ]
        )
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] == "-1800.0000"
    assert weekly["matched_sell_quantity"] == 6
    assert weekly["cost_basis_sources"] == {"broker_reconciliation": 1, "fill_fifo": 0, "unknown": 0}
    assert weekly["stale_fifo_inventory_alerts"][0]["symbol"] == "049430"
    assert weekly["stale_fifo_inventory_alerts"][0]["fifo_open_quantity"] == 39

    engine.dispose()


def test_runtime_status_weekly_pnl_keeps_later_repurchase_basis_unknown(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    app, engine = build_test_app(settings=settings)
    client = TestClient(app)
    now = datetime(2026, 6, 17, 3, 0, tzinfo=UTC)
    _freeze_runtime_service_now(monkeypatch, now)
    sell_at = now - timedelta(days=2)
    buy_at = now - timedelta(days=1)

    with app.state.db_session_factory() as session:
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="weekly-unmatched-sell",
            symbol="LATER",
            side="sell",
            status="filled",
            quantity=5,
            request_metadata=_execution_scope_metadata(settings),
        )
        buy_order = BrokerOrder(
            mode="paper",
            idempotency_key="weekly-later-buy",
            symbol="LATER",
            side="buy",
            status="filled",
            quantity=3,
            request_metadata=_execution_scope_metadata(settings),
        )
        session.add_all([sell_order, buy_order])
        session.flush()
        session.add_all(
            [
                ExecutionFill(
                    broker_order_pk=sell_order.id,
                    broker_fill_id="weekly-unmatched-sell-fill",
                    symbol="LATER",
                    side="sell",
                    quantity=5,
                    price=Decimal("1000"),
                    filled_at=sell_at,
                ),
                ExecutionFill(
                    broker_order_pk=buy_order.id,
                    broker_fill_id="weekly-later-buy-fill",
                    symbol="LATER",
                    side="buy",
                    quantity=3,
                    price=Decimal("700"),
                    filled_at=buy_at,
                ),
                _scoped_execution_position(
                    settings,
                    symbol="LATER",
                    quantity=3,
                    average_price=Decimal("700"),
                    updated_from="reconciliation",
                    last_reconciled_at=buy_at + timedelta(minutes=5),
                ),
            ]
        )
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] is None
    assert weekly["weekly_net_realized_pnl_krw"] is None
    assert weekly["profitability_status"] == "degraded_unknown_currency"
    assert weekly["pnl_cost_basis_coverage"] == "incomplete"
    assert weekly["matched_sell_quantity"] == 0
    assert weekly["unmatched_sell_quantity"] == 5
    assert weekly["unknown_cost_basis_sell_quantity"] == 5
    assert weekly["closed_trade_count"] == 0
    assert weekly["closed_symbols"] == []
    assert weekly["unmatched_sell_symbols"] == ["LATER"]
    assert weekly["cost_basis_sources"] == {"broker_reconciliation": 0, "fill_fifo": 0, "unknown": 1}

    engine.dispose()


def test_runtime_status_daily_pnl_uses_crypto_base_quantity_and_quote_notional() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            runtime_broker_provider="upbit",
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
            request_metadata=_execution_scope_metadata(
                app.state.settings,
                provider="upbit",
                exchange="UPBIT",
                venue="UPBIT",
                asset_class="crypto",
            ),
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
            request_metadata=_execution_scope_metadata(
                app.state.settings,
                provider="upbit",
                exchange="UPBIT",
                venue="UPBIT",
                asset_class="crypto",
            ),
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
    assert monthly["pnl_method"] == "fifo_internal_fills_with_broker_reconciliation_fallback"
    weekly = response.json()["weekly_pnl"]
    assert weekly["weekly_realized_pnl"] == "4000.0000"
    assert weekly["weekly_buy_notional"] == "100000.0000"
    assert weekly["weekly_sell_notional"] == "44000.0000"
    assert weekly["matched_sell_quantity"] == "0.0004"
    assert weekly["unmatched_sell_quantity"] == 0
    assert weekly["closed_symbols"] == ["KRW-BTC"]
    assert weekly["closed_symbol_count"] == 1
    assert weekly["cost_basis_sources"] == {"broker_reconciliation": 0, "fill_fifo": 1, "unknown": 0}
    assert weekly["pnl_method"] == "fifo_internal_fills_with_broker_reconciliation_fallback"
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
            request_metadata=_execution_scope_metadata(app.state.settings),
        )
        matched_sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="daily-coverage-sell-matched-1",
            symbol="005930",
            side="sell",
            status="filled",
            quantity=1,
            request_metadata=_execution_scope_metadata(app.state.settings),
        )
        unmatched_sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="daily-coverage-sell-unmatched-1",
            symbol="003280",
            side="sell",
            status="filled",
            quantity=150,
            request_metadata=_execution_scope_metadata(app.state.settings),
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
    assert daily["daily_realized_pnl"] is None
    assert daily["covered_gross_realized_pnl_krw"] == "1000.0000"
    assert daily["daily_net_realized_pnl_krw"] is None
    assert daily["profitability_status"] == "degraded_unknown_cost_basis"
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


def test_runtime_status_requires_hourly_risk_off_schema_columns() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            automation_enabled=True,
        )
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE runtime_control_state DROP COLUMN hourly_risk_off_context"
        )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is False
    assert payload["dependencies"]["runtime_schema"] == "missing"
    assert "runtime_control_state.hourly_risk_off_context" in (
        payload["safety"]["readiness_reason"]
    )
    assert payload["safety"]["automated_buy_submission_allowed"] is False
    assert payload["safety"]["automated_buy_submission_block_reason"] == "paused"
    assert payload["safety"]["hourly_risk_off"] == {
        "available": False,
        "active": None,
        "epoch": None,
        "context": None,
    }

    engine.dispose()


@pytest.mark.parametrize(
    ("table_name", "scope_columns"),
    [
        ("broker_sessions", ("generation", "refresh_after")),
        (
            "fifo_inventory_isolations",
            ("provider", "account_scope", "venue", "asset_class", "exchange"),
        ),
        ("execution_positions", ("provider", "account_scope", "exchange")),
        ("monitoring_checkpoints", ("provider", "account_scope")),
        ("crypto_strategy_states", ("account_scope", "strategy_family")),
        ("crypto_strategy_risk_states", ("account_scope", "strategy_family")),
        ("crypto_strategy_decisions", ("account_scope", "strategy_family")),
    ],
)
def test_health_rejects_pre_scope_migration_runtime_tables(
    table_name: str,
    scope_columns: tuple[str, ...],
) -> None:
    app, engine = build_test_app()
    legacy_table_name = f"{table_name}_pre_scope_migration"
    retained_columns = [
        str(column["name"])
        for column in inspect(engine).get_columns(table_name)
        if column["name"] not in scope_columns
    ]
    quoted_columns = ", ".join(f'"{column}"' for column in retained_columns)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE "{table_name}" RENAME TO "{legacy_table_name}"'
        )
        connection.exec_driver_sql(
            f'CREATE TABLE "{table_name}" AS '
            f'SELECT {quoted_columns} FROM "{legacy_table_name}" WHERE 0'
        )
    client = TestClient(app)

    payload = client.get("/health").json()

    assert payload["status"] == "degraded"
    runtime_schema = payload["dependencies"]["runtime_schema"]
    assert runtime_schema["status"] == "missing"
    assert runtime_schema["missing_columns"][table_name] == list(scope_columns)
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


def test_plan_judgment_action_does_not_block_runtime_safety_heartbeat(
    monkeypatch,
) -> None:
    app, engine = build_test_app()
    release_projection = threading.Event()

    def blocking_projection(**_kwargs: object) -> dict[str, object]:
        release_projection.wait(timeout=2)
        return {"status": "applied", "action": "risk_off_expire"}

    monkeypatch.setattr(
        runtime_routes,
        "apply_hourly_plan_judgment_action",
        blocking_projection,
    )

    async def scenario() -> tuple[httpx.Response, float]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://127.0.0.1",
        ) as client:
            timer = threading.Timer(0.75, release_projection.set)
            timer.start()
            try:
                projection = asyncio.create_task(
                    client.post(
                        "/runtime/plan-judgment/action",
                        json={"risk_flags": 3, "risk_reasons": ["test"]},
                    )
                )
                heartbeat_started = time.monotonic()
                heartbeat = await client.get("/runtime/safety")
                heartbeat_elapsed = time.monotonic() - heartbeat_started
                await projection
                return heartbeat, heartbeat_elapsed
            finally:
                release_projection.set()
                timer.cancel()

    heartbeat, heartbeat_elapsed = asyncio.run(scenario())

    assert heartbeat.status_code == 200
    assert heartbeat_elapsed < 0.5
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
            "source": "test-fixture",
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
    assert plan["source"] == "test-fixture"
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
            execution_position_account_scope="runtime-health-paper-account",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-20260430-open",
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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


def test_autonomous_runtime_prefers_local_latest_watch_symbols_over_stale_core_desired(
    tmp_path,
    monkeypatch,
) -> None:
    app, engine = build_test_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'monitoring-symbol-union.db'}",
            runtime_mode="live",
            execution_position_account_scope="monitoring-symbol-union-account",
            monitoring_max_symbols_per_cycle=4,
            market_context_enabled=True,
            market_context_base_url="http://core.test",
        )
    )
    older_date = datetime(2026, 8, 7).date()
    current_date = datetime(2026, 8, 10).date()

    with app.state.db_session_factory() as session:
        older_plan = ExecutionPlan(
            id="older-core-plan",
            external_plan_id="older-core-plan",
            source="kai-trading-core",
            market_date=older_date,
            mode="live",
            status="accepted",
            accepted_at=datetime(2026, 8, 7, 1, tzinfo=UTC),
        )
        current_plan = ExecutionPlan(
            id="current-core-plan",
            external_plan_id="current-core-plan",
            source="kai-trading-core",
            market_date=current_date,
            mode="live",
            status="accepted",
            accepted_at=datetime(2026, 8, 10, 5, tzinfo=UTC),
        )
        older_item = ExecutionPlanItem(
            id="older-watch",
            plan_item_index=0,
            symbol="OLD001",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=Decimal("100"),
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        older_item.risk_context, older_item.quantity_policy = bind_kis_account_route(
            settings=app.state.settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy=older_item.quantity_policy,
        )
        older_item.provider = "kis"
        older_plan.items.append(older_item)
        for index, symbol in enumerate(["004370", "027040", "028670", "049430"]):
            current_item = ExecutionPlanItem(
                id=f"current-watch-{symbol}",
                plan_item_index=index,
                symbol=symbol,
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=Decimal("100"),
                quantity_policy={"kind": "shares", "quantity": 1},
            )
            current_item.risk_context, current_item.quantity_policy = bind_kis_account_route(
                settings=app.state.settings,
                provider="kis",
                risk_context={"exchange": "KRX"},
                quantity_policy=current_item.quantity_policy,
            )
            current_item.provider = "kis"
            current_plan.items.append(current_item)
        session.add_all([older_plan, current_plan])
        session.commit()

    local_symbols = app.state.autonomous_runtime._load_local_desired_monitoring_symbols_from_sqlite()
    assert local_symbols[:4] == ["004370", "027040", "028670", "049430"]
    stale_core_symbols = ["OLD001", "OLD002", "OLD003", "OLD004"]
    assert _merge_symbol_priority(local_symbols, stale_core_symbols)[:4] == [
        "004370",
        "027040",
        "028670",
        "049430",
    ]

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
        item.risk_context, item.quantity_policy = bind_kis_account_route(
            settings=app.state.settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy=item.quantity_policy,
        )
        item.provider = "kis"
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
            "source": "test-fixture",
            "market_date": "2026-05-21",
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
    newer = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-new-sell",
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
        assert older_item.status == "target_sell_pending_config"
        assert older_item.risk_context == {
            "market": "KRX",
            "exchange": "KRX",
            "currency": "KRW",
        }

    engine.dispose()


def test_replanned_sell_monitor_updates_existing_source_row_in_place() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_account_number="12345678",
            kis_account_product_code="01",
            account_drawdown_recovery_scope_secret=(
                "replanned-sell-scope-secret-at-least-32-bytes"
            ),
        )
    )
    client = TestClient(app)

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-old-replanned-sell",
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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


def test_autonomous_monitoring_repairs_rejected_latest_stop_before_dispatch() -> None:
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
        repaired_stop_id = broker_adapter.order_calls[0].execution_plan_item_id
        assert repaired_stop_id not in {older_stop_id, newer_target_id, newer_stop_id}
        repaired_stop = session.get(ExecutionPlanItem, repaired_stop_id)
        assert repaired_stop is not None
        assert repaired_stop.intent == "stop_sell"
        assert repaired_stop.decision_agent_name == "post-entry-monitor-repair"
        assert repaired_stop.trigger_price == Decimal("8287.5000")

        orders = session.scalars(select(BrokerOrder)).all()
        assert [order.execution_plan_item_id for order in orders] == [repaired_stop_id]

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
    broker_adapter = StubMonitoringBrokerAdapter(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
            source="stub:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="033530",
                    quantity=58,
                    average_price=Decimal("8500.00"),
                )
            ],
        )
    )

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable monitoring sell"},
    )
    assert resume.status_code == 200

    older = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-retry-pending-stop",
            "source": "test-fixture",
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
            "source": "test-fixture",
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
        rejected_order = BrokerOrder(
            execution_plan_item_id=older_stop_id,
            broker_name="kis",
            mode="paper",
            idempotency_key="session-retry-stop-033530",
            symbol="033530",
            side="sell",
            status="retry_pending",
            quantity=58,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
            last_error_code="KIOK0320",
            last_error_message="장운영시간이 아닙니다",
            last_error_retryable=True,
        )
        session.add(rejected_order)
        session.add(
            _scoped_execution_position(
                app.state.settings,
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
        replacement_order = session.scalar(
            select(BrokerOrder).where(BrokerOrder.id != rejected_order.id)
        )
        assert replacement_order is not None
        assert replacement_order.request_metadata["metadata"]["retry"] == {
            "source": "rejected_order_retry",
            "source_broker_order_pk": rejected_order.id,
        }
        rearmed_stop = session.get(ExecutionPlanItem, older_stop_id)
        assert rearmed_stop is not None
        assert rearmed_stop.status == "broker_order_submitted"
        events = rearmed_stop.risk_context["monitoring_events"]
        assert any(event["event"] == "protective_stop_rearmed" for event in events)

    engine.dispose()


@pytest.mark.parametrize(
    ("recover_on_call", "expected_final_status"),
    [(2, "broker_order_retry_pending"), (None, "broker_order_failed")],
    ids=("session-recovers", "retry-budget-exhausted"),
)
def test_autonomous_monitoring_retries_each_terminal_session_rejection_once(
    recover_on_call: int | None,
    expected_final_status: str,
) -> None:
    class RejectOnceThenSubmitAdapter(StubMonitoringBrokerAdapter):
        def place_order(
            self,
            access_token: str,
            request: BrokerOrderPlacementRequest,
        ) -> BrokerOrderPlacement:
            assert access_token
            self.order_calls.append(request)
            if recover_on_call is None or len(self.order_calls) < recover_on_call:
                raise BrokerAdapterError(
                    "장운영시간이 아닙니다",
                    status_code=409,
                    classification=BrokerErrorClassification.BROKER_REJECTED,
                    retryable=True,
                    code="KIOK0320",
                    normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
                    response_payload={
                        "rt_cd": "1",
                        "msg_cd": "KIOK0320",
                        "msg1": "장운영시간이 아닙니다",
                    },
                )
            return BrokerOrderPlacement(
                broker_order_id="kis-recovered-stop",
                normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
                broker_status="submitted",
                submitted_at=datetime(2026, 5, 28, 1, 6, tzinfo=UTC),
                observed_at=datetime(2026, 5, 28, 1, 6, tzinfo=UTC),
                request_metadata={"source": "stub"},
                response_metadata={"source": "stub"},
                request_payload={"symbol": request.symbol},
                response_payload={"broker_order_id": "kis-recovered-stop"},
            )

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
    broker_adapter = RejectOnceThenSubmitAdapter(
        account_snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
            source="stub:retry-chain-position",
            holdings=[
                BrokerAccountHolding(
                    symbol="033530",
                    quantity=58,
                    sellable_quantity=58,
                    average_price=Decimal("8500.00"),
                )
            ],
        )
    )
    assert client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable monitoring retry"},
    ).status_code == 200
    plan = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-033530-multi-session-retry",
            "source": "test-fixture",
            "market_date": "2026-05-27",
            "items": [
                {
                    "symbol": "033530",
                    "side": "sell",
                    "intent": "stop_sell",
                    "trigger_price": "8700.00",
                    "quantity_policy": {"kind": "position_exit_from_entry"},
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    assert plan.status_code == 201
    stop_id = plan.json()["persisted_execution_item_ids"][0]

    with app.state.db_session_factory() as session:
        stop = session.get(ExecutionPlanItem, stop_id)
        assert stop is not None
        stop.status = "broker_order_retry_pending"
        first_rejection = BrokerOrder(
            execution_plan_item_id=stop_id,
            broker_name="kis",
            mode="paper",
            idempotency_key="initial-session-rejected-stop-033530",
            symbol="033530",
            side="sell",
            status="retry_pending",
            quantity=58,
            venue="KRX",
            asset_class="stock",
            request_metadata=_execution_scope_metadata(
                app.state.settings,
                asset_class="stock",
            ),
            last_error_classification="broker_rejected",
            last_error_code="KIOK0320",
            last_error_message="장운영시간이 아닙니다",
            last_error_retryable=True,
        )
        session.add(first_rejection)
        session.add(
            _scoped_execution_position(
                app.state.settings,
                mode="paper",
                symbol="033530",
                quantity=58,
                average_price=Decimal("8500.00"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
            )
        )
        session.commit()

        first_cycle = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[MonitoringPriceRequest(symbol="033530", price=Decimal("7550.00"))],
            now=datetime(2026, 5, 28, 1, 5, tzinfo=UTC),
        )
        assert first_cycle.status_code == 200
        assert len(broker_adapter.order_calls) == 1
        assert broker_adapter.order_calls[0].idempotency_key == (
            f"monitoring:stop_sell:{stop_id}:retry:{first_rejection.id}"
        )
        second_rejection = session.scalar(
            select(BrokerOrder)
            .where(BrokerOrder.id != first_rejection.id)
            .order_by(BrokerOrder.created_at.desc(), BrokerOrder.id.desc())
        )
        assert second_rejection is not None
        assert second_rejection.status == "retry_pending"

        second_cycle = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[MonitoringPriceRequest(symbol="033530", price=Decimal("7550.00"))],
            now=datetime(2026, 5, 28, 1, 6, tzinfo=UTC),
        )
        assert second_cycle.status_code == 200
        assert second_cycle.payload["submitted_order_count"] == (
            1 if recover_on_call is not None else 0
        ), repr(
            second_cycle.payload.get("items")
        )
        assert len(broker_adapter.order_calls) == 2
        assert broker_adapter.order_calls[1].idempotency_key == (
            f"monitoring:stop_sell:{stop_id}:retry:{second_rejection.id}"
        )

        active_order_count = len(
            session.scalars(
                select(BrokerOrder).where(
                    BrokerOrder.execution_plan_item_id == stop_id,
                    BrokerOrder.status == "submitted",
                )
            ).all()
        )
        assert active_order_count == (1 if recover_on_call is not None else 0)
        if recover_on_call is not None:
            session.refresh(stop)
            assert stop.status == "broker_order_submitted"
            # Simulate a stale plan-item projection. The durable active order
            # must still prevent an older rejection from spawning a new retry.
            stop.status = "broker_order_retry_pending"
            session.commit()
        third_cycle = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[MonitoringPriceRequest(symbol="033530", price=Decimal("7550.00"))],
            now=datetime(2026, 5, 28, 1, 7, tzinfo=UTC),
        )
        assert third_cycle.status_code == 200
        assert len(broker_adapter.order_calls) == 2
        session.refresh(stop)
        assert stop.status == expected_final_status
        if recover_on_call is None:
            assert stop.risk_context["last_monitoring_event"]["event"] == (
                "protective_stop_retry_exhausted"
            )
            assert stop.risk_context["last_monitoring_event"]["retry_attempt_count"] == 2

    engine.dispose()


def test_autonomous_monitoring_submits_same_symbol_sell_before_watch_buy_when_capacity_is_limited() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            monitoring_max_broker_actions_per_cycle=1,
            account_drawdown_gate_enabled=False,
        )
    )
    client = TestClient(app)
    broker_adapter = StubMonitoringBrokerAdapter()

    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable monitoring sell priority"},
    )
    assert resume.status_code == 200

    buy_risk_context, buy_quantity_policy = bind_kis_account_route(
        settings=app.state.settings,
        provider="kis",
        risk_context={},
        quantity_policy={
            "kind": "shares",
            "quantity": 1,
            "stop_price": "7200.0000",
            "max_budget": "10000.0000",
        },
    )
    sell_risk_context, sell_quantity_policy = bind_kis_account_route(
        settings=app.state.settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "position_exit_from_entry"},
    )

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            id="plan-same-symbol-buy-before-sell",
            external_plan_id="plan-same-symbol-buy-before-sell",
            source="kai-trading-core",
            market_date=datetime(2026, 8, 10, tzinfo=UTC).date(),
            mode="paper",
            status="accepted",
            accepted_at=datetime(2026, 8, 10, 1, 0, tzinfo=UTC),
            raw_payload={"source": "test"},
        )
        session.add(plan)
        session.add(
            _scoped_execution_position(
                app.state.settings,
                mode="paper",
                symbol="033530",
                quantity=1,
                average_price=Decimal("8500.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 8, 10, 1, 0, tzinfo=UTC),
            )
        )
        session.add_all(
            [
                ExecutionPlanItem(
                    id="same-symbol-watch-buy-first",
                    plan=plan,
                    plan_item_index=0,
                    symbol="033530",
                    side="buy",
                    intent="intraday_watch_buy",
                    status="watch_monitoring_active",
                        quantity_policy=buy_quantity_policy,
                        risk_context=buy_risk_context,
                    trigger_price=Decimal("7300.0000"),
                    limit_price=Decimal("7300.0000"),
                    time_in_force="day",
                ),
                ExecutionPlanItem(
                    id="same-symbol-protective-stop-second",
                    plan=plan,
                    plan_item_index=1,
                    symbol="033530",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_monitoring",
                        quantity_policy=sell_quantity_policy,
                    trigger_price=Decimal("7600.0000"),
                    time_in_force="day",
                        risk_context=sell_risk_context,
                ),
            ]
        )
        session.commit()

        result = run_autonomous_monitoring_cycle(
            settings=app.state.settings,
            db=session,
            adapter=broker_adapter,
            prices=[MonitoringPriceRequest(symbol="033530", price=Decimal("7300.0000"))],
            symbols=["033530"],
            now=datetime(2026, 8, 10, 1, 5, tzinfo=UTC),
        )

        assert result.status_code == 200
        assert result.payload["submitted_order_count"] == 1
        assert result.payload["deferred_item_count"] == 1
        assert [request.side for request in broker_adapter.order_calls] == ["sell"]
        assert broker_adapter.order_calls[0].execution_plan_item_id == "same-symbol-protective-stop-second"

        items_by_id = {
            item["execution_plan_item_id"]: item
            for item in result.payload["items"]
        }
        assert items_by_id["same-symbol-protective-stop-second"]["outcome"] == "submitted"
        assert items_by_id["same-symbol-watch-buy-first"]["outcome"] == "deferred"

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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
                _scoped_execution_position(
                    app.state.settings,
                    mode="paper",
                    symbol="000660",
                    quantity=0,
                    average_price=Decimal("0.00"),
                    updated_from="reconciliation",
                ),
                _scoped_execution_position(
                    app.state.settings,
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
                _scoped_execution_position(
                    app.state.settings,
                    mode="paper",
                    symbol="000660",
                    quantity=0,
                    average_price=Decimal("0.00"),
                    updated_from="reconciliation",
                ),
                _scoped_execution_position(
                    app.state.settings,
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


def test_stale_sell_monitor_expiry_preserves_only_active_protection_for_held_position() -> None:
    app, engine = build_test_app(
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            id="plan-held-protection-expiry-guard",
            external_plan_id="plan-held-protection-expiry-guard",
            source="test",
            mode="paper",
            status="accepted",
            accepted_at=datetime(2026, 8, 7, 1, 0, tzinfo=UTC),
        )
        session.add(plan)
        session.add(
            _scoped_execution_position(
                app.state.settings,
                mode="paper",
                symbol="459550",
                quantity=200,
                average_price=Decimal("1920.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 8, 7, 1, 0, tzinfo=UTC),
                raw_payload={"source": "test"},
            )
        )
        session.add_all(
            [
                ExecutionPlanItem(
                    id="held-original-stop-expired",
                    plan=plan,
                    plan_item_index=0,
                    symbol="459550",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_expired",
                    quantity_policy={"kind": "shares", "quantity": 200},
                    trigger_price=Decimal("1843.2000"),
                    time_in_force="day",
                ),
                ExecutionPlanItem(
                    id="held-recovery-stop-active",
                    plan=plan,
                    plan_item_index=1,
                    symbol="459550",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_monitoring",
                    quantity_policy={"kind": "shares", "quantity": 200},
                    trigger_price=Decimal("1872.0000"),
                    time_in_force="day",
                ),
                ExecutionPlanItem(
                    id="held-recovery-target-active",
                    plan=plan,
                    plan_item_index=2,
                    symbol="459550",
                    side="sell",
                    intent="target_sell",
                    status="target_sell_monitoring",
                    quantity_policy={"kind": "shares", "quantity": 200},
                    trigger_price=Decimal("2100.0000"),
                    time_in_force="day",
                ),
            ]
        )
        session.commit()

        expired_count = expire_known_flat_pending_sell_monitors(
            settings=app.state.settings,
            db=session,
            expired_at=datetime(2026, 8, 7, 2, 0, tzinfo=UTC),
        )

        assert expired_count == 0
        statuses = {
            item.id: item.status
            for item in session.scalars(
                select(ExecutionPlanItem).where(
                    ExecutionPlanItem.symbol == "459550",
                    ExecutionPlanItem.side == "sell",
                )
            )
        }
        assert statuses == {
            "held-original-stop-expired": "stop_sell_expired",
            "held-recovery-stop-active": "stop_sell_monitoring",
            "held-recovery-target-active": "target_sell_monitoring",
        }

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
            execution_position_account_scope="health-test-live-account",
            exec_agent_max_account_snapshot_age_seconds=60,
        )
    )
    client = TestClient(app)
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-stale-position-sell-monitor",
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
            "source": "test-fixture",
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
                broker_name="kis",
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=1,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("567000.00"),
                request_metadata=_execution_scope_metadata(app.state.settings),
                submitted_at=datetime(2026, 6, 4, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            _scoped_execution_position(
                app.state.settings,
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
            execution_position_account_scope="health-test-live-account",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-buy-filled-position-stale-flat",
            "source": "test-fixture",
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
                broker_name="kis",
                mode="live",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=1,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("567000.00"),
                request_metadata=_execution_scope_metadata(app.state.settings),
                submitted_at=datetime(2026, 6, 4, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            _scoped_execution_position(
                app.state.settings,
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
            execution_position_account_scope="health-test-live-account",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-live-buy-filled-position-flat",
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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


def test_plan_item_exchange_loads_deferred_risk_context_from_file_db(tmp_path) -> None:
    app, engine = build_test_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'deferred-risk-context.db'}",
            runtime_mode="paper",
        )
    )

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-deferred-risk-context-exchange",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
            accepted_at=datetime(2026, 6, 8, 0, 0, tzinfo=UTC),
        )
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="ETH",
            side="sell",
            intent="target_sell",
            status="target_sell_monitoring",
            quantity_policy={"kind": "shares", "quantity": 1},
            risk_context={"exchange": "NXT"},
        )
        session.add(item)
        session.commit()
        item_id = item.id

    with app.state.db_session_factory() as session:
        item = session.scalars(
            select(ExecutionPlanItem)
            .options(load_only(ExecutionPlanItem.id))
            .where(ExecutionPlanItem.id == item_id)
        ).one()
        assert "risk_context" in inspect(item).unloaded

        assert _plan_item_exchange(item) == "NXT"

    engine.dispose()


def test_monitoring_cycle_preserves_all_held_symbols_past_fast_path_cap(tmp_path) -> None:
    class StaticMonitoringPriceSource:
        provider = "test"

        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def __call__(
            self,
            *,
            symbols: list[str],
            observed_at: datetime,
            settings: Settings,
        ) -> list[MonitoringPriceRequest]:
            assert observed_at
            assert settings
            self.calls.append(list(symbols))
            return [
                MonitoringPriceRequest(
                    symbol=symbol,
                    price=Decimal("100.00"),
                    observed_at=observed_at,
                )
                for symbol in symbols
            ]

    price_source = StaticMonitoringPriceSource()
    app, engine = build_test_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'held-fast-path-cap.db'}",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
            monitoring_max_symbols_per_cycle=2,
            monitoring_max_broker_actions_per_cycle=0,
        ),
        autonomous_runtime_enabled=True,
        monitoring_price_source=price_source,
    )

    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-held-fast-path-cap",
            source="kai-trading-core",
            mode="paper",
            status="accepted",
            accepted_at=datetime(2026, 6, 8, 0, 0, tzinfo=UTC),
        )
        session.add(plan)
        for symbol in ("000001", "000002", "000003", "999999"):
            session.add(
                ExecutionPlanItem(
                    plan=plan,
                    plan_item_index=0,
                    symbol=symbol,
                    side="sell",
                    intent="target_sell",
                    status="target_sell_monitoring",
                    quantity_policy={"kind": "position_exit_from_entry"},
                    trigger_price=Decimal("90.00"),
                )
            )
        for symbol in ("000001", "000002", "000003"):
            session.add(
                _scoped_execution_position(
                    app.state.settings,
                    mode="paper",
                    symbol=symbol,
                    quantity=1,
                    average_price=Decimal("100.00"),
                    updated_from="reconciliation",
                    last_reconciled_at=datetime(2026, 6, 8, 1, 0, tzinfo=UTC),
                )
            )
        session.commit()

    payload = app.state.autonomous_runtime._monitoring_cycle_sync(
        datetime(2026, 6, 8, 1, 5, tzinfo=UTC)
    )

    assert payload["monitored_symbols"] == ["000001", "000002", "000003"]
    assert price_source.calls == [["000001", "000002", "000003"]]

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
            "source": "test-fixture",
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
                broker_name="kis",
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("100.00"),
                request_metadata=_execution_scope_metadata(app.state.settings),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        position = _scoped_execution_position(
            app.state.settings,
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
            "source": "test-fixture",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "source_plan_item_id": "entry-1",
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
                broker_name="kis",
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("100.00"),
                request_metadata=_execution_scope_metadata(app.state.settings),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        position = _scoped_execution_position(
            app.state.settings,
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
            "source": "test-fixture",
            "market_date": "2026-06-08",
            "items": [
                {
                    "symbol": "002380",
                    "side": "buy",
                    "intent": "intraday_watch_buy",
                    "source_plan_item_id": "entry-1",
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
                broker_name="kis",
                mode="paper",
                execution_plan_item_id=buy_item_id,
                idempotency_key=f"monitoring:watch_buy:{buy_item_id}",
                symbol="002380",
                side="buy",
                status="filled",
                quantity=3,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("100.00"),
                request_metadata=_execution_scope_metadata(app.state.settings),
                submitted_at=datetime(2026, 6, 8, 0, 55, tzinfo=UTC),
            )
        )
        session.add(
            _scoped_execution_position(
                app.state.settings,
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
            self.account_snapshot_calls: list[
                tuple[list[str] | None, dict[str, Decimal] | None, str]
            ] = []

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

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            self.account_snapshot_calls.append((symbols, reference_prices, exchange))
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
    price_source = StaticMonitoringPriceSource({"002380": Decimal("101.00")})
    broker = LiveFastPathBroker(started_at)
    app, engine = build_test_app(
        Settings(
            database_url=f"sqlite:///{tmp_path / 'live-fast-path-reconcile.db'}",
            runtime_mode="live",
            autonomous_runtime_enabled=True,
            kis_app_key="live-key",
            kis_app_secret="live-secret",
            kis_account_number="12345678",
            execution_position_account_scope="health-test-live-account",
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
        for item in (buy_item, target_item):
            item.risk_context, item.quantity_policy = bind_kis_account_route(
                settings=app.state.settings,
                provider="kis",
                risk_context={"exchange": "KRX"},
                quantity_policy=item.quantity_policy,
            )
            item.provider = "kis"
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
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("100.00"),
                submitted_at=started_at - timedelta(minutes=1),
                request_metadata=_execution_scope_metadata(app.state.settings),
            )
        )
        session.commit()

    payload = app.state.autonomous_runtime._monitoring_cycle_sync(started_at)

    assert broker.reconcile_calls == [
        ("fast-path-buy-1", "KRX", (started_at - timedelta(minutes=1)).replace(tzinfo=None))
    ]
    # KRX holdings discovery runs before the symbol-scoped order reconciliation
    # refresh, without serially querying unrelated overseas exchanges.
    assert broker.account_snapshot_calls == [
        (None, None, "KRX"),
        (["002380"], None, "KRX"),
    ]
    assert price_source.calls == [["002380"]]
    assert payload["cached_evaluation"] is True
    assert payload["evaluated_item_count"] == 3
    assert payload["post_entry_maintenance_count"] == 3
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
                broker_name="kis",
                mode="paper",
                idempotency_key="monitoring:target_sell:flat-submitted",
                broker_order_id="flat-submitted-sell-1",
                symbol="003280",
                side="sell",
                status="submitted",
                quantity=150,
                venue="KRX",
                asset_class="kr_equity",
                request_metadata=_execution_scope_metadata(app.state.settings),
            )
        )
        session.add(
            _scoped_execution_position(
                app.state.settings,
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
        "source": "test-fixture",
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
        "source": "test-fixture",
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
            _scoped_execution_position(
                app.state.settings,
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
            kis_account_number="12345678",
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
                _scoped_execution_position(
                    app.state.settings,
                    mode="paper",
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("70950.0000"),
                    updated_from="reconciliation",
                ),
                _scoped_execution_position(
                    app.state.settings,
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
                _scoped_execution_position(
                    app.state.settings,
                    mode="paper",
                    symbol="005930",
                    quantity=1,
                    average_price=Decimal("70000.0000"),
                    updated_from="reconciliation",
                ),
                _scoped_execution_position(
                    app.state.settings,
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
        for item in paper_plan.items:
            item.risk_context, item.quantity_policy = bind_kis_account_route(
                settings=app.state.settings,
                provider="kis",
                risk_context={"exchange": "KRX"},
                quantity_policy=item.quantity_policy,
            )
            item.provider = "kis"
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
            broker_name="kis",
            mode="paper",
            idempotency_key="paper-open-order",
            broker_order_id="kis-open-1",
            symbol="000660",
            side="buy",
            status="open",
            quantity=3,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
            submitted_at=today_edge_naive,
            last_broker_sync_at=today_edge_naive,
        )
        failed_order = BrokerOrder(
            execution_plan_item_id=failed_item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="paper-failed-order",
            broker_order_id="kis-failed-1",
            symbol="035420",
            side="buy",
            status="failed",
            quantity=2,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
            submitted_at=today_edge_naive,
            last_broker_sync_at=None,
            last_error_message="broker rejected order",
        )
        historical_failed_order = BrokerOrder(
            execution_plan_item_id=historical_failed_item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="paper-failed-order-yesterday",
            broker_order_id="kis-failed-2",
            symbol="051910",
            side="buy",
            status="rejected",
            quantity=1,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
            submitted_at=yesterday_edge_naive,
            last_broker_sync_at=None,
            last_error_message="historical rejection",
        )
        historical_filled_order = BrokerOrder(
            execution_plan_item_id=historical_fill_item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="paper-filled-order-yesterday",
            broker_order_id="kis-filled-2",
            symbol="066570",
            side="buy",
            status="filled",
            quantity=4,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
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


def test_runtime_status_deduplicates_repeated_fills_before_daily_counts() -> None:
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
        )
    )

    now = datetime.now(UTC)
    with app.state.db_session_factory() as session:
        buy_order = BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="legacy-duplicate-fill-buy",
            symbol="282330",
            side="buy",
            status="filled",
            quantity=6,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
        )
        sell_order = BrokerOrder(
            broker_name="kis",
            mode="paper",
            idempotency_key="legacy-duplicate-fill-sell",
            symbol="282330",
            side="sell",
            status="filled",
            quantity=2,
            venue="KRX",
            asset_class="kr_equity",
            request_metadata=_execution_scope_metadata(app.state.settings),
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
    identity = build_position_identity(
        settings=app.state.settings,
        provider="kis",
        exchange="KRX",
    )
    exact_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": identity.account_scope,
    }
    app.state.autonomous_runtime._store_cycle_payload(
        worker_key="monitoring",
        payload={"monitored_symbols": ["005930", "000660", "005930"]},
    )
    app.state.autonomous_runtime._monitoring_runtime_snapshot = MonitoringRuntimeSnapshot(
        loaded_at=datetime.now(UTC),
        symbols=["005930", "000660", "005930"],
        positions_by_symbol={},
        items=[],
        filled_today_symbols=["282330"],
        execution_scope=exact_scope,
    )
    krx_today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    now = datetime.combine(
        krx_today,
        datetime.min.time(),
        tzinfo=ZoneInfo("Asia/Seoul"),
    ).astimezone(UTC) + timedelta(hours=12)
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
            request_metadata=_execution_scope_metadata(app.state.settings),
        )
        sell_order = BrokerOrder(
            mode="paper",
            idempotency_key="fast-runtime-sell",
            symbol="282330",
            side="sell",
            status="filled",
            quantity=3,
            request_metadata=_execution_scope_metadata(app.state.settings),
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
    assert payload["symbols"]["filled_today_by_side"] == {"buy": ["282330"], "sell": ["282330"]}
    assert payload["symbols"]["filled_today_counts"] == {
        "all": {"symbols": 1, "fills": 2, "quantity": 6},
        "buy": {"symbols": 1, "fills": 1, "quantity": 3},
        "sell": {"symbols": 1, "fills": 1, "quantity": 3},
    }
    assert payload["monthly_pnl"]["pnl_method"] == "fifo_internal_fills_with_broker_reconciliation_fallback"
    assert payload["monthly_pnl"]["monthly_realized_pnl"] is None
    assert payload["monthly_pnl"]["matched_sell_quantity"] is None
    assert payload["monthly_pnl"]["calculation_status"] == "not_computed"
    assert payload["weekly_pnl"]["pnl_method"] == "fifo_internal_fills_with_broker_reconciliation_fallback"
    assert payload["weekly_pnl"]["weekly_realized_pnl"] is None
    assert payload["weekly_pnl"]["weekly_buy_notional"] is None
    assert payload["weekly_pnl"]["weekly_sell_notional"] is None
    assert payload["weekly_pnl"]["matched_sell_quantity"] is None
    assert payload["weekly_pnl"]["calculation_status"] == "not_computed"
    assert payload["weekly_pnl"]["closed_symbols"] == []
    assert payload["daily_pnl"]["pnl_method"] == "fifo_internal_fills"
    assert payload["daily_pnl"]["daily_realized_pnl"] == "-11700.0000"
    assert payload["daily_pnl"]["daily_buy_notional"] == "375000.0000"
    assert payload["daily_pnl"]["daily_sell_notional"] == "363300.0000"
    assert payload["daily_pnl"]["matched_sell_quantity"] == 3
    assert payload["daily_pnl"]["closed_symbols"] == ["282330"]

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


def test_fast_runtime_status_reports_durable_account_drawdown_gate(monkeypatch) -> None:
    expected_gate = {
        "available": True,
        "enabled": True,
        "gated": True,
        "provider": "kis",
        "latest_decision": {"summary": {"gate_name": "daily_total_asset_drawdown_pct"}},
        "active_recovery_scopes": {"kis:opaque:krw": {"order_id": "blocked-buy"}},
        "reason": None,
    }
    monkeypatch.setattr(
        runtime_service,
        "latest_account_drawdown_gate_state",
        lambda **_: expected_gate,
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            automation_enabled=True,
            autonomous_runtime_enabled=True,
            account_drawdown_gate_enabled=True,
        ),
        autonomous_runtime_enabled=True,
    )
    client = TestClient(app)

    response = client.get("/runtime/status")

    assert response.status_code == 200
    assert response.json()["account_drawdown_gate"] == expected_gate
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


def test_fast_runtime_status_prefers_database_fill_truth_over_cached_symbols() -> None:
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
    assert payload["symbols"]["filled_today"] == []
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
            execution_position_account_scope="runtime-counts-paper-account",
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
            provider="kis",
            venue="KRX",
            asset_class="kr_equity",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        paper_item.risk_context, paper_item.quantity_policy = bind_kis_account_route(
            settings=app.state.settings,
            provider="kis",
            risk_context=paper_item.risk_context,
            quantity_policy=paper_item.quantity_policy,
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
            provider="kis",
            venue="KRX",
            asset_class="kr_equity",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 1},
        )
        live_item.risk_context, live_item.quantity_policy = bind_kis_account_route(
            settings=app.state.settings,
            provider="kis",
            risk_context=live_item.risk_context,
            quantity_policy=live_item.quantity_policy,
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
                    broker_name="kis",
                    venue="KRX",
                    asset_class="kr_equity",
                    request_metadata=_execution_scope_metadata(app.state.settings),
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
                    broker_name="kis",
                    venue="KRX",
                    asset_class="kr_equity",
                    request_metadata=_execution_scope_metadata(app.state.settings),
                ),
            ]
        )
        session.commit()

    client = TestClient(app)
    payload = client.get("/runtime/status").json()

    assert payload["counts"] == {"open_orders": 1, "active_execution_jobs": 1}

    engine.dispose()
