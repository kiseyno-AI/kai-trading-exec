from collections.abc import Generator

from fastapi import Request
from sqlalchemy.orm import Session

from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.runtime import RuntimeMetadata


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_runtime_metadata(request: Request) -> RuntimeMetadata:
    return request.app.state.runtime_metadata


def get_broker_adapter(request: Request) -> BrokerAdapter:
    adapter_factory = request.app.state.broker_adapter_factory
    return adapter_factory(request.app.state.settings)


def get_db_session(request: Request) -> Generator[Session]:
    session_factory = request.app.state.db_session_factory
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
