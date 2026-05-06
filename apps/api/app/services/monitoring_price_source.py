from __future__ import annotations

from typing import Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_auth import ensure_authenticated_broker_session


class KisCurrentQuoteMonitoringPriceSource:
    source_name = "kis_current_quote_rest"

    def __init__(
        self,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session],
        broker_adapter_factory: Callable[[Settings], BrokerAdapter],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.broker_adapter_factory = broker_adapter_factory

    def describe(self) -> dict[str, object]:
        adapter = self.broker_adapter_factory(self.settings)
        config = adapter.describe()
        return {
            "source_name": self.source_name,
            "provider": config.provider,
            "transport": "rest",
            "configured": config.configured,
            "mode": config.mode,
        }

    def get_prices(
        self,
        *,
        symbols: list[str],
        observed_at,
        settings: Settings,
    ) -> list[MonitoringPriceRequest]:
        del observed_at
        unique_symbols = list(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))
        if not unique_symbols:
            return []

        broker_adapter = self.broker_adapter_factory(settings)
        with self.session_factory() as db:
            session_state = _ensure_authenticated_broker_session_with_retry(
                settings=settings,
                db=db,
                adapter=broker_adapter,
            )
            access_token = session_state.access_token or ""
            prices: list[MonitoringPriceRequest] = []
            for symbol in unique_symbols:
                quote = broker_adapter.get_current_price(
                    access_token,
                    symbol=symbol,
                    exchange="KRX",
                )
                if quote is None:
                    continue
                prices.append(
                    MonitoringPriceRequest(
                        symbol=quote.symbol,
                        price=quote.price,
                    )
                )
            return prices


def _ensure_authenticated_broker_session_with_retry(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
):
    try:
        return ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="monitoring_price_source",
            reason="autonomous monitoring requires current broker prices",
        )
    except SQLAlchemyError:
        db.rollback()
        return ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="monitoring_price_source",
            reason="autonomous monitoring requires current broker prices",
        )


def build_default_monitoring_price_source(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
    broker_adapter_factory: Callable[[Settings], BrokerAdapter],
) -> object | None:
    if settings.runtime_mode == "mock":
        return None

    adapter = broker_adapter_factory(settings)
    if adapter.describe().provider != "kis":
        return None

    return KisCurrentQuoteMonitoringPriceSource(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=broker_adapter_factory,
    )
