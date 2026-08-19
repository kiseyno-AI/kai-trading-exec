from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from decimal import Decimal
import threading
import time
from typing import Callable

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAdapter, BrokerCurrentPrice, BrokerLiveMarketContext
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.domestic_order_router import resolve_domestic_order_route

MAX_REPORTED_QUOTE_FAILURES = 10
MAX_REPORTED_CONTEXT_FAILURES = 10
MAX_QUOTE_WORKERS = 3
QUOTE_RETRY_ATTEMPTS = 0
QUOTE_RETRY_DELAY_SECONDS = 0.2
QUOTE_BATCH_TIMEOUT_BUFFER_SECONDS = 2.0


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
        self._quote_batch_lock = threading.Lock()
        self._quote_batch_state_lock = threading.Lock()
        self._quote_batch_futures: set[
            Future[tuple[str, BrokerCurrentPrice | None, Exception | None]]
        ] = set()
        self._quote_executor = ThreadPoolExecutor(
            max_workers=MAX_QUOTE_WORKERS,
            thread_name_prefix="kis-monitoring-quote",
        )

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
            "context_failures": [],
            "context_failure_count": 0,
            "context_failures_truncated": False,
        }
        if not unique_symbols:
            return []
        if not self._quote_batch_lock.acquire(blocking=False):
            self._last_diagnostics = {
                "requested_symbol_count": len(unique_symbols),
                "requested_symbols": unique_symbols,
                "priced_symbol_count": 0,
                "priced_symbols": [],
                "missing_symbol_count": len(unique_symbols),
                "missing_symbols": unique_symbols,
                "quote_failures": [
                    {
                        "symbol": symbol,
                        "error": "previous KIS current quote batch is still running",
                    }
                    for symbol in unique_symbols[:MAX_REPORTED_QUOTE_FAILURES]
                ],
                "quote_failure_count": len(unique_symbols),
                "quote_failures_truncated": len(unique_symbols) > MAX_REPORTED_QUOTE_FAILURES,
                "context_failures": [],
                "context_failure_count": 0,
                "context_failures_truncated": False,
                "quote_batch_in_flight": True,
            }
            return []

        broker_adapter = self.broker_adapter_factory(settings)
        release_quote_batch_lock = True
        try:
            with self.session_factory() as db:
                session_state = _ensure_authenticated_broker_session_with_retry(
                    settings=settings,
                    db=db,
                    adapter=broker_adapter,
                )
                access_token = session_state.access_token or ""
                exchange_by_symbol = _load_monitoring_exchange_by_symbol(
                    db=db,
                    mode=settings.runtime_mode,
                    symbols=unique_symbols,
                )
                prices: list[MonitoringPriceRequest] = []
                priced_symbols: list[str] = []
                failures: list[dict[str, str]] = []
                context_failures: list[dict[str, object]] = []
                failure_count = 0
                context_failure_count = 0

                def fetch_quote(symbol: str) -> tuple[str, BrokerCurrentPrice | None, Exception | None]:
                    worker_adapter = self.broker_adapter_factory(settings)
                    try:
                        return (
                            symbol,
                            worker_adapter.get_current_price(
                                access_token,
                                symbol=symbol,
                                exchange=exchange_by_symbol.get(symbol, "KRX"),
                            ),
                            None,
                        )
                    except Exception as exc:
                        return symbol, None, exc

                quote_results: dict[str, tuple[BrokerCurrentPrice | None, Exception | None]] = {}
                worker_count = min(MAX_QUOTE_WORKERS, len(unique_symbols))
                request_timeout = max(float(settings.kis_request_timeout_seconds), 1.0)
                wave_count = (len(unique_symbols) + worker_count - 1) // worker_count
                batch_timeout = (request_timeout * wave_count) + QUOTE_BATCH_TIMEOUT_BUFFER_SECONDS
                futures = {
                    self._quote_executor.submit(fetch_quote, symbol): symbol
                    for symbol in unique_symbols
                }
                with self._quote_batch_state_lock:
                    self._quote_batch_futures.update(futures)
                batch_timed_out = False
                try:
                    try:
                        completed_futures = as_completed(futures, timeout=batch_timeout)
                        for future in completed_futures:
                            symbol, quote, error = future.result()
                            quote_results[symbol] = (quote, error)
                    except FuturesTimeoutError:
                        batch_timed_out = True
                        for future, symbol in futures.items():
                            if symbol not in quote_results:
                                future.cancel()
                                quote_results[symbol] = (
                                    None,
                                    TimeoutError(
                                        f"KIS current quote batch timed out after {batch_timeout:g}s"
                                    ),
                                )
                finally:
                    if batch_timed_out:
                        release_quote_batch_lock = False
                        self._release_quote_batch_when_complete(set(futures))
                    else:
                        with self._quote_batch_state_lock:
                            self._quote_batch_futures.difference_update(futures)

                retry_symbols = [
                    symbol
                    for symbol, (quote, error) in quote_results.items()
                    if quote is None and error is not None
                ]
                for _attempt in range(QUOTE_RETRY_ATTEMPTS):
                    if not retry_symbols:
                        break
                    next_retry_symbols: list[str] = []
                    for symbol in retry_symbols:
                        if QUOTE_RETRY_DELAY_SECONDS > 0:
                            time.sleep(QUOTE_RETRY_DELAY_SECONDS)
                        _retry_symbol, quote, error = fetch_quote(symbol)
                        quote_results[symbol] = (quote, error)
                        if quote is None and error is not None:
                            next_retry_symbols.append(symbol)
                    retry_symbols = next_retry_symbols

                for symbol in unique_symbols:
                    quote, error = quote_results.get(symbol, (None, None))
                    if error is not None:
                        failure_count += 1
                        if len(failures) < MAX_REPORTED_QUOTE_FAILURES:
                            failures.append({"symbol": symbol, "error": str(error)})
                        continue
                    if quote is None:
                        continue
                    context = None
                    context_failure = {
                        "symbol": quote.symbol,
                        "endpoint": "broker_live_market_context",
                        "reason": "skipped_noncritical_enrichment",
                    }
                    context_failure_count += 1
                    if len(context_failures) < MAX_REPORTED_CONTEXT_FAILURES:
                        context_failures.append(context_failure)
                    priced_symbols.append(quote.symbol)
                    market_quality = _market_quality_from_quote_payload(quote.payload)
                    market_quality = _merge_live_market_context(
                        market_quality,
                        context=context,
                        context_failure=context_failure,
                        quote=quote,
                        settings=settings,
                    )
                    market_quality = _sanitize_monitoring_market_quality(market_quality)
                    prices.append(
                        MonitoringPriceRequest(
                            symbol=quote.symbol,
                            price=quote.price,
                            observed_at=quote.observed_at,
                            **market_quality,
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
                    "context_failures": context_failures,
                    "context_failure_count": context_failure_count,
                    "context_failures_truncated": context_failure_count > len(context_failures),
                    "quote_batch_timed_out": batch_timed_out,
                    "exchange_by_symbol": exchange_by_symbol,
                }
                return prices
        finally:
            if release_quote_batch_lock:
                self._quote_batch_lock.release()

    def close(self) -> None:
        self._quote_executor.shutdown(wait=False, cancel_futures=True)

    def _release_quote_batch_when_complete(
        self,
        futures: set[Future[tuple[str, BrokerCurrentPrice | None, Exception | None]]],
    ) -> None:
        def release_if_complete(
            _future: Future[
                tuple[str, BrokerCurrentPrice | None, Exception | None]
            ] | None = None,
        ) -> None:
            del _future
            if any(not future.done() for future in futures):
                return
            with self._quote_batch_state_lock:
                self._quote_batch_futures.difference_update(futures)
            try:
                self._quote_batch_lock.release()
            except RuntimeError:
                return

        for future in futures:
            future.add_done_callback(release_if_complete)
        release_if_complete()

    def get_last_diagnostics(self) -> dict[str, object]:
        return dict(self._last_diagnostics)


def _market_quality_from_quote_payload(payload: dict[str, object]) -> dict[str, object]:
    output = payload.get("output")
    if not isinstance(output, dict):
        output = payload
    return {
        "bid": _first_value(output, "bid", "bid_price", "best_bid", "bidp", "bidp1"),
        "ask": _first_value(output, "ask", "ask_price", "best_ask", "askp", "askp1"),
        "spread": _first_value(output, "spread", "spread_price"),
        "spread_bps": _first_value(output, "spread_bps", "spread_bp"),
        "orderbook_depth": _first_value(output, "orderbook_depth", "book_depth", "total_depth"),
        "orderbook_imbalance": _first_value(output, "orderbook_imbalance", "book_imbalance"),
        "volume": _first_value(output, "volume", "accumulated_volume", "acml_vol", "tvol"),
        "turnover": _first_value(output, "turnover", "trading_value", "acml_tr_pbmn"),
        "day_high": _first_value(output, "day_high", "intraday_high", "high_price", "stck_hgpr", "high"),
        "day_low": _first_value(output, "day_low", "intraday_low", "low_price", "stck_lwpr", "low"),
        "session_type": _first_value(output, "session_type", "session", "market_state"),
        "normal_ordering_available": _normal_ordering_available(output),
        "provenance": {"source": "broker_current_price_payload"},
    }


def _load_monitoring_exchange_by_symbol(
    *,
    db: Session,
    mode: str,
    symbols: list[str],
) -> dict[str, str]:
    if not symbols:
        return {}
    rows = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == mode,
            ExecutionPlanItem.symbol.in_(symbols),
        )
        .order_by(ExecutionPlanItem.updated_at.desc())
    ).all()
    exchanges: dict[str, str] = {}
    for item in rows:
        if item.symbol in exchanges:
            continue
        risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
        exchange = str(risk_context.get("exchange") or "").strip().upper()
        if item.asset_class == "us_equity" and exchange in {"NASD", "NYSE", "AMEX"}:
            exchanges[item.symbol] = exchange
        elif item.asset_class != "us_equity":
            exchanges[item.symbol] = "KRX"
    return exchanges


def _fetch_live_market_context(
    *,
    broker_adapter: BrokerAdapter,
    access_token: str,
    quote: BrokerCurrentPrice,
) -> tuple[BrokerLiveMarketContext | None, dict[str, object] | None]:
    context_getter = getattr(broker_adapter, "get_live_market_context", None)
    if not callable(context_getter):
        return None, {
            "symbol": quote.symbol,
            "endpoint": "broker_live_market_context",
            "reason": "broker adapter does not expose live market context enrichment",
        }
    try:
        return (
            context_getter(
                access_token,
                symbol=quote.symbol,
                exchange=quote.exchange,
                reference_price=quote.price,
            ),
            None,
        )
    except Exception as exc:
        return None, {
            "symbol": quote.symbol,
            "endpoint": "broker_live_market_context",
            "reason": str(exc),
        }


def _merge_live_market_context(
    base: dict[str, object],
    *,
    context: BrokerLiveMarketContext | None,
    context_failure: dict[str, object] | None,
    quote: BrokerCurrentPrice,
    settings: Settings,
) -> dict[str, object]:
    merged = dict(base)
    provenance = dict(merged.get("provenance") or {})
    provenance["current_price"] = {
        "source": "broker_current_price_payload",
        "endpoint": _endpoint_from_payload(quote.payload),
    }
    if context is not None:
        for key in (
            "bid",
            "ask",
            "spread",
            "spread_bps",
            "orderbook_depth",
            "orderbook_imbalance",
            "session_type",
            "normal_ordering_available",
        ):
            value = getattr(context, key)
            if value is not None:
                merged[key] = value
        provenance["orderbook"] = {
            "source": "broker_live_market_context",
            "endpoint": _endpoint_from_payload(context.payload),
            "observed_at": context.observed_at.isoformat(),
        }
    if context_failure is not None:
        provenance["orderbook"] = {
            "source": "broker_live_market_context",
            "status": "failed",
            **context_failure,
        }

    route_context = _derive_orderability_context(
        settings=settings,
        symbol=quote.symbol,
        price=quote.price,
        observed_at=quote.observed_at,
    )
    for key, value in route_context.items():
        if key in {"session_type", "normal_ordering_available"} and merged.get(key) is None:
            merged[key] = value
    provenance["session"] = {
        "source": "domestic_order_router",
        "reason": route_context["orderability_reason"],
        "action": route_context["orderability_action"],
        "exchange": route_context["orderability_exchange"],
        "observed_at": quote.observed_at.isoformat(),
    }

    missing_fields = _missing_market_quality_fields(merged)
    if missing_fields:
        provenance["missing_fields"] = missing_fields
    merged["provenance"] = provenance
    return merged


def _sanitize_monitoring_market_quality(payload: dict[str, object]) -> dict[str, object]:
    sanitized = dict(payload)
    for key in ("bid", "ask", "day_high", "day_low"):
        value = sanitized.get(key)
        if value is None:
            continue
        try:
            decimal_value = Decimal(str(value))
        except Exception:
            continue
        if decimal_value <= 0:
            sanitized[key] = None
    return sanitized


def _derive_orderability_context(
    *,
    settings: Settings,
    symbol: str,
    price: Decimal,
    observed_at,
) -> dict[str, object]:
    route_decision = resolve_domestic_order_route(
        settings=settings,
        now=observed_at,
        symbol=symbol,
        side="sell",
        purpose="protective_stop",
        desired_order_type="limit",
        limit_price=price,
        requested_exchange="KRX",
    )
    return {
        "session_type": route_decision.session,
        "normal_ordering_available": route_decision.action == "submit",
        "orderability_action": route_decision.action,
        "orderability_exchange": route_decision.exchange,
        "orderability_reason": route_decision.reason,
    }


def _endpoint_from_payload(payload: dict[str, object]) -> object | None:
    endpoint = payload.get("endpoint")
    if endpoint is not None:
        return endpoint
    output = payload.get("provider")
    if output == "kis":
        return payload.get("tr_id")
    return None


def _missing_market_quality_fields(payload: dict[str, object]) -> list[str]:
    missing: list[str] = []
    if payload.get("bid") is None:
        missing.append("bid")
    if payload.get("ask") is None:
        missing.append("ask")
    if payload.get("orderbook_depth") is None and payload.get("orderbook_imbalance") is None:
        missing.append("orderbook")
    if payload.get("volume") is None:
        missing.append("volume")
    if payload.get("turnover") is None:
        missing.append("turnover")
    if payload.get("session_type") is None:
        missing.append("session_type")
    if payload.get("normal_ordering_available") is not True:
        missing.append("normal_ordering_available")
    return missing


def _first_value(payload: dict[str, object], *keys: str) -> object | None:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


def _normal_ordering_available(payload: dict[str, object]) -> bool | None:
    value = _first_value(payload, "normal_ordering_available", "ordering_available")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "available", "open"}:
            return True
        if normalized in {"false", "0", "no", "n", "unavailable", "closed"}:
            return False
    return None


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
