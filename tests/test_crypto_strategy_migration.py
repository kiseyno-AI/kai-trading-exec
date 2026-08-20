import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

from app.core.config import get_settings
from app.models.runtime import CryptoDailyBar


def _upgrade(database_url: str, revision: str, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("KAI_EXEC_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    command.upgrade(config, revision)
    get_settings.cache_clear()


def test_crypto_strategy_migration_repairs_partially_created_schema(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'partial-crypto.db'}"
    application_logger = logging.getLogger("app.services.nxt_eligibility")
    application_logger.disabled = False
    _upgrade(database_url, "202608010001", monkeypatch)
    assert application_logger.disabled is False

    engine = create_engine(database_url)
    CryptoDailyBar.__table__.create(engine)
    engine.dispose()

    _upgrade(database_url, "head", monkeypatch)
    assert application_logger.disabled is False

    engine = create_engine(database_url)
    inspector = inspect(engine)
    expected_tables = {
        "crypto_daily_bars",
        "crypto_strategy_states",
        "crypto_strategy_risk_states",
        "crypto_strategy_decisions",
    }
    assert expected_tables.issubset(inspector.get_table_names())
    assert {
        "ix_crypto_daily_bars_provider_symbol_close",
    }.issubset({index["name"] for index in inspector.get_indexes("crypto_daily_bars")})
    assert {
        "strategy_owned_base_quantity",
    }.issubset({column["name"] for column in inspector.get_columns("crypto_strategy_states")})
    engine.dispose()
