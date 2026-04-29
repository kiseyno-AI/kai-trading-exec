from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings


def ensure_database_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite"):
        return
    if not url.database or url.database == ":memory:":
        return
    Path(url.database).expanduser().parent.mkdir(parents=True, exist_ok=True)


def create_db_engine(settings: Settings) -> Engine:
    ensure_database_parent_dir(settings.database_url)
    url = make_url(settings.database_url)
    kwargs: dict[str, object] = {"future": True, "pool_pre_ping": True}
    if url.drivername.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
        if url.database == ":memory:":
            kwargs["poolclass"] = StaticPool
    return create_engine(settings.database_url, **kwargs)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        future=True,
    )


def ping_database(db: Session) -> None:
    db.execute(text("SELECT 1"))

