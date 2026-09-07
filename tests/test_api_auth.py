from fastapi.testclient import TestClient
import pytest

from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app


def _app(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return (
        create_app(
            settings=settings,
            engine=engine,
            session_factory=create_session_factory(engine),
        ),
        engine,
    )


def test_api_bearer_token_loads_from_environment() -> None:
    settings = load_settings({"KAI_EXEC_API_BEARER_TOKEN": "internal-secret"})

    assert settings.api_bearer_token == "internal-secret"


def test_configured_bearer_token_protects_runtime_and_control_routes() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_bearer_token="internal-secret",
        )
    )
    client = TestClient(app)

    assert client.get("/health").status_code == 200
    missing = client.get("/runtime/status")
    wrong = client.post(
        "/controls/pause",
        headers={"Authorization": "Bearer wrong-secret"},
    )
    valid = client.get(
        "/runtime/status",
        headers={"Authorization": "Bearer internal-secret"},
    )

    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == "Bearer"
    assert wrong.status_code == 401
    assert valid.status_code == 200
    engine.dispose()


def test_non_loopback_api_requires_bearer_token() -> None:
    with pytest.raises(ValueError, match="API_BEARER_TOKEN"):
        create_app(
            settings=Settings(
                database_url="sqlite:///:memory:",
                api_host="0.0.0.0",
            )
        )


def test_non_loopback_api_allows_configured_bearer_token() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_host="0.0.0.0",
            api_bearer_token="internal-secret",
        )
    )

    response = TestClient(app).get(
        "/runtime/status",
        headers={"Authorization": "Bearer internal-secret"},
    )

    assert response.status_code == 200
    engine.dispose()


def test_remote_request_is_rejected_when_runtime_bind_bypasses_configured_host() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_host="127.0.0.1",
        )
    )
    client = TestClient(
        app,
        base_url="http://localhost",
        client=("203.0.113.10", 50000),
    )

    assert client.get("/health").status_code == 200
    response = client.get("/runtime/status")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    engine.dispose()
