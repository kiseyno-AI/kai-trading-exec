from pathlib import Path

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, Table, event, inspect
from sqlalchemy.engine import Connection

from app.db.base import Base
from app import models  # noqa: F401


ALEMBIC_CONFIG = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
ALEMBIC_SCRIPT = ScriptDirectory.from_config(ALEMBIC_CONFIG)


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
