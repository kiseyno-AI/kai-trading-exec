from pathlib import Path

import pytest
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, Table, event, inspect
from sqlalchemy.engine import Connection

from app.db.base import Base
from app import models  # noqa: F401
from app.services import nxt_eligibility


ALEMBIC_CONFIG = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
ALEMBIC_SCRIPT = ScriptDirectory.from_config(ALEMBIC_CONFIG)


@pytest.fixture(autouse=True)
def isolate_nxt_catalogue(monkeypatch):
    """Routing tests must not use live market membership or another test's cache."""
    monkeypatch.setattr(nxt_eligibility, "_CACHE", nxt_eligibility.NxtEligibleSymbolCache())

    def unavailable_catalogue(*, settings):
        raise RuntimeError("NXT catalogue is unavailable unless the test supplies a fixture")

    monkeypatch.setattr(nxt_eligibility, "_fetch_nxt_eligible_payload", unavailable_catalogue)


@event.listens_for(Base.metadata, "after_create")
def stamp_metadata_created_test_schema(
    metadata: MetaData,
    connection: Connection,
    *,
    tables: list[Table],
    **_kwargs: object,
) -> None:
    """Keep model-created test schemas aligned with the current migration head."""
    if set(tables) != set(metadata.sorted_tables):
        return
    if inspect(connection).has_table("alembic_version"):
        return

    MigrationContext.configure(connection).stamp(ALEMBIC_SCRIPT, "heads")
