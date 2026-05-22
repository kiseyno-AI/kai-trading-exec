from __future__ import annotations

from typing import Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_auth import ensure_authenticated_broker_session

MAX_REPORTED_QUOTE_FAILURES = 10


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
        self._last_diagnostics: dict[str, object] = {}

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
        self._last_diagnostics = {
            "requested_symbol_count": len(unique_symbols),
            "requested_symbols": unique_symbols,
            "priced_symbol_count": 0,
            "priced_symbols": [],
            "missing_symbol_count": len(unique_symbols),
            "missing_symbols": unique_symbols,
            "quote_failures": [],
            "quote_failure_count": 0,
            "quote_failures_truncated": False,
        }
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
            priced_symbols: list[str] = []
            failures: list[dict[str, str]] = []
            failure_count = 0
            for symbol in unique_symbols:
                try:
                    quote = broker_adapter.get_current_price(
                        access_token,
                        symbol=symbol,
                        exchange="KRX",
                    )
                except Exception as exc:
                    failure_count += 1
                    if len(failures) < MAX_REPORTED_QUOTE_FAILURES:
                        failures.append({"symbol": symbol, "error": str(exc)})
                    continue
                if quote is None:
                    continue
                priced_symbols.append(quote.symbol)
                prices.append(
                    MonitoringPriceRequest(
                        symbol=quote.symbol,
                        price=quote.price,
                        observed_at=quote.observed_at,
                    )
                )
            priced_set = set(priced_symbols)
            missing_symbols = [symbol for symbol in unique_symbols if symbol not in priced_set]
            self._last_diagnostics = {
                "requested_symbol_count": len(unique_symbols),
                "requested_symbols": unique_symbols,
                "priced_symbol_count": len(prices),
                "priced_symbols": priced_symbols,
                "missing_symbol_count": len(missing_symbols),
                "missing_symbols": missing_symbols,
                "quote_failures": failures,
                "quote_failure_count": failure_count,
                "quote_failures_truncated": failure_count > len(failures),
            }
            return prices

    def get_last_diagnostics(self) -> dict[str, object]:
        return dict(self._last_diagnostics)


def _ensure_authenticated_broker_session_with_retry(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
):
    last_error: SQLAlchemyError | None = None
    for _attempt in range(3):
        try:
            return ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
                source="monitoring_price_source",
                reason="autonomous monitoring requires current broker prices",
            )
        except SQLAlchemyError as exc:
            last_error = exc
            db.rollback()
    if last_error is not None:
        db.rollback()
        raise last_error
    raise RuntimeError("broker session authentication failed without an error")


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
