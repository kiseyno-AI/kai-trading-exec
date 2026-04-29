from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import inspect, select

from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models import EXEC_OWNED_TABLES
from app.models.runtime import ExecutionFill, RuntimeControlEvent, RuntimeControlState
from app.services import runtime as runtime_service


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
