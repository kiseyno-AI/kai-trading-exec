from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config
import pytest
import sqlalchemy

from app.core import config as runtime_config
from app.core.config import Settings


@pytest.mark.parametrize("offline", [True, False])
@pytest.mark.parametrize("password", ["p%40ss", "p%25ss", "plain-test-password"])
def test_alembic_environment_preserves_encoded_database_urls(
    monkeypatch, password: str, offline: bool
) -> None:
    # Synthetic credentials only; online execution uses an in-memory engine.
    database_url = (
        f"postgresql+psycopg://audit:{password}@127.0.0.1:55434/"
        "kai_trading_exec_migration_test"
    )
    settings = Settings(database_url=database_url)
    monkeypatch.setattr(runtime_config, "get_settings", lambda: settings)
    monkeypatch.setattr("logging.config.fileConfig", lambda *args, **kwargs: None)
    config = Config(
        str(Path(__file__).resolve().parents[1] / "alembic.ini"),
        output_buffer=StringIO(),
    )
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    received_urls: list[str] = []

    def isolated_engine_from_config(options, *, prefix, poolclass):
        received_urls.append(options[f"{prefix}url"])
        return engine

    monkeypatch.setattr(sqlalchemy, "engine_from_config", isolated_engine_from_config)
    try:
        command.upgrade(config, "base", sql=offline)

        assert config.get_main_option("sqlalchemy.url") == database_url
        assert received_urls == ([] if offline else [database_url])
    finally:
        engine.dispose()
