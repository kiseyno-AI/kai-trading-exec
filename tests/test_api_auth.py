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


def test_loopback_mode_rejects_cross_origin_browser_writes() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_host="127.0.0.1",
        )
    )
    client = TestClient(app, base_url="http://127.0.0.1:8010")

    # Body-less POSTs are CORS "simple" requests: a hostile web page can send
    # them without a preflight, so the server must refuse them itself.
    for path in ("/controls/pause", "/controls/resume", "/execution-jobs/dispatch"):
        cross_origin = client.post(path, headers={"Origin": "https://evil.example"})
        cross_site = client.post(path, headers={"Sec-Fetch-Site": "cross-site"})
        opaque_origin = client.post(path, headers={"Origin": "null"})
        other_local_port = client.post(path, headers={"Origin": "http://127.0.0.1:3000"})

        assert cross_origin.status_code == 403, path
        assert cross_site.status_code == 403, path
        assert opaque_origin.status_code == 403, path
        assert other_local_port.status_code == 403, path
        assert cross_origin.json() == {"detail": "Cross-origin write requests are not allowed"}

    # A fresh runtime starts paused. The rejected cross-origin /controls/resume
    # calls above must not have lifted that operator-visible safety state.
    safety = client.get("/runtime/safety", headers={"Origin": "https://evil.example"})
    assert safety.status_code == 200
    assert safety.json()["paused"] is True
    engine.dispose()


def test_loopback_mode_still_allows_same_origin_and_non_browser_writes() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_host="127.0.0.1",
        )
    )
    client = TestClient(app, base_url="http://127.0.0.1:8010")

    # Core's HTTP client, curl and operator scripts send no Origin header.
    plain = client.post("/controls/pause")
    same_origin = client.post(
        "/controls/resume",
        headers={"Origin": "http://127.0.0.1:8010", "Sec-Fetch-Site": "same-origin"},
    )

    assert plain.status_code != 403
    assert same_origin.status_code != 403
    engine.dispose()


def test_valid_bearer_token_is_not_subject_to_the_cross_origin_write_guard() -> None:
    app, engine = _app(
        Settings(
            database_url="sqlite:///:memory:",
            api_host="127.0.0.1",
            api_bearer_token="internal-secret",
        )
    )
    client = TestClient(app, base_url="http://127.0.0.1:8010")

    response = client.post(
        "/controls/pause",
        headers={
            "Authorization": "Bearer internal-secret",
            "Origin": "https://ops-dashboard.example",
        },
    )
    unauthenticated = client.post(
        "/controls/pause",
        headers={"Origin": "https://ops-dashboard.example"},
    )

    assert response.status_code != 403
    assert response.status_code != 401
    assert unauthenticated.status_code == 401
    engine.dispose()
