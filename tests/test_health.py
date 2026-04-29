import threading
from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import inspect, select, text

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


def build_test_app(settings: Settings | None = None):
    app_settings = settings or Settings(database_url="sqlite:///:memory:")
    engine = create_db_engine(app_settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=app_settings,
        engine=engine,
        session_factory=session_factory,
    )
    return app, engine


def build_unmigrated_runtime_safety_app(database_url: str):
    settings = Settings(database_url=database_url)
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
    assert "automation is disabled" in payload["readiness"]["reasons"]
    assert payload["safety"]["paused"] is True
    assert payload["safety"]["kill_switch_enabled"] is False
    assert payload["safety"]["automated_order_submission_allowed"] is False
    assert payload["safety"]["automated_order_submission_block_reason"] == "paused"
    assert payload["safety"]["last_change"]["action"] == "bootstrap"
    assert payload["counts"] == {"open_orders": 0, "active_execution_jobs": 0}
    assert payload["liveness"]["monitoring"]["status"] == "not_started"

    with app.state.db_session_factory() as session:
        state = session.get(RuntimeControlState, "global")
        assert state is not None
        assert state.paused is True
        assert state.last_transition_action == "bootstrap"

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
