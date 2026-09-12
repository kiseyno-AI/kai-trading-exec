from __future__ import annotations

import pytest

from app.services.database_cli_security import reject_secret_database_url_argument


@pytest.mark.parametrize(
    "database_url",
    [
        (
            "mssql+pyodbc:///?odbc_connect="
            "DRIVER%3DODBC%2BDriver%3BUID%3Doperator%3B"
            "PWD%3Dmust-not-leak-odbc-secret"
        ),
        "postgresql://db.example/exec?auth=must-not-leak-auth-secret",
        (
            "postgresql://db.example/exec?connection_string="
            "host%3Ddb.example%3Bpassword%3Dmust-not-leak-opaque-secret"
        ),
        (
            "postgresql://db.example/exec?options="
            "user%3Doperator%3Bpassword%3Dmust-not-leak-embedded-secret"
        ),
    ],
)
def test_database_url_rejects_query_embedded_credentials_without_echoing_them(
    database_url: str,
) -> None:
    with pytest.raises(SystemExit) as caught:
        reject_secret_database_url_argument(database_url)

    message = str(caught.value)
    assert "KAI_EXEC_DATABASE_URL environment variable" in message
    assert "must-not-leak" not in message
    assert database_url not in message


@pytest.mark.parametrize(
    "database_url",
    [
        "sqlite:///var/kai.db?mode=ro&uri=true",
        (
            "postgresql://db.example/exec?sslmode=require&"
            "application_name=kai-exec&connect_timeout=5"
        ),
    ],
)
def test_database_url_allows_known_non_secret_query_options(
    database_url: str,
) -> None:
    reject_secret_database_url_argument(database_url)
