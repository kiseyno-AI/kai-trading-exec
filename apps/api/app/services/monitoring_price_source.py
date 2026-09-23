from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from decimal import Decimal
import threading
import time
from typing import Callable
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import (
    BrokerAdapter,
    BrokerAdapterError,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerLiveMarketContext,
)
from app.broker.kis import KIS_OVERSEAS_QUOTE_EMPTY_CODE
from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.domestic_order_router import resolve_domestic_order_route
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
)

MAX_REPORTED_QUOTE_FAILURES = 10
MAX_REPORTED_CONTEXT_FAILURES = 10
MAX_QUOTE_WORKERS = 3
QUOTE_RETRY_ATTEMPTS = 0
QUOTE_RETRY_DELAY_SECONDS = 0.2
QUOTE_BATCH_TIMEOUT_BUFFER_SECONDS = 2.0
DURABLE_EMPTY_QUOTE_OBSERVATION_COUNT = 3
TOKEN_REUSE_MARGIN = timedelta(minutes=5)
RUNTIME_CONTEXT_RETRY_INTERVAL = timedelta(seconds=60)
_ROUTE_AUTHORITY_INTENTS = frozenset(
    {
        "target_sell",
        "stop_sell",
        "max_holding_close_sell",
        "intraday_watch_buy",
        "swing_watch_buy",
        "next_open_buy",
    }
)
_ROUTE_AUTHORITY_STATUSES = frozenset(
    {
        "target_sell_monitoring",
        "stop_sell_monitoring",
        "max_holding_close_sell_monitoring",
        "target_sell_triggered_blocked",
        "stop_sell_triggered_blocked",
        "target_sell_triggered_pending_dispatch",
        "stop_sell_triggered_pending_dispatch",
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "watch_buy_triggered",
        "watch_buy_triggered_blocked",
        "watch_buy_triggered_pending_dispatch",
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
    }
)


@dataclass(frozen=True)
class _KisMonitoringRuntimeContext:
    cached_at: datetime
    refresh_before: datetime
    access_token: str
    covered_symbols: frozenset[str]
    execution_context_by_symbol: dict[str, dict[str, object]]


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
        self._empty_quote_counts: dict[str, int] = {}
        self._runtime_context_lock = threading.Lock()
        self._runtime_context: _KisMonitoringRuntimeContext | None = None
        self._runtime_context_retry_at: datetime | None = None
        self._runtime_context_generation = 0
        self._quote_batch_lock = threading.Lock()
        self._quote_batch_state_lock = threading.Lock()
        self._quote_batch_futures: set[
            Future[tuple[str, BrokerCurrentPrice | None, Exception | None]]
        ] = set()
        self._quote_executor = ThreadPoolExecutor(
            max_workers=MAX_QUOTE_WORKERS,
            thread_name_prefix="kis-monitoring-quote",
        )
        self._context_executor = ThreadPoolExecutor(
            max_workers=MAX_QUOTE_WORKERS,
            thread_name_prefix="kis-monitoring-context",
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

    def prime_runtime_context(
        self,
        *,
        db: Session,
        symbols: list[str],
        loaded_at: datetime,
    ) -> None:
        with self._runtime_context_lock:
            refresh_generation = self._runtime_context_generation
        try:
            runtime_context = self._build_runtime_context(
                db=db,
                symbols=symbols,
                loaded_at=loaded_at,
            )
        except Exception:
            with self._runtime_context_lock:
                current_context = self._runtime_context
                if (
                    self._runtime_context_generation == refresh_generation
                    and current_context is None
                ):
                    self._runtime_context = None
                    self._runtime_context_retry_at = (
                        datetime.now(UTC) + RUNTIME_CONTEXT_RETRY_INTERVAL
                    )
            raise
        with self._runtime_context_lock:
            if self._runtime_context_generation != refresh_generation:
                raise RuntimeError(
                    "monitoring runtime context was invalidated during refresh"
                )
            current_context = self._runtime_context
            if (
                current_context is not None
                and current_context.cached_at > runtime_context.cached_at
            ):
                return
            self._runtime_context = runtime_context
            self._runtime_context_retry_at = None

    def _build_runtime_context(
        self,
        *,
        db: Session,
        symbols: list[str],
        loaded_at: datetime,
    ) -> _KisMonitoringRuntimeContext:
        unique_symbols = list(
            dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip())
        )
        broker_adapter = self.broker_adapter_factory(self.settings)
        session_state = _ensure_authenticated_broker_session_with_retry(
            settings=self.settings,
            db=db,
            adapter=broker_adapter,
        )
        access_token = str(session_state.access_token or "")
        if not access_token:
            raise BrokerAdapterError(
                "monitoring price source requires an active broker token",
                status_code=503,
            )
        cached_at = datetime.now(UTC)
        token_expires_at = _as_utc(session_state.access_token_expires_at)
        refresh_after = _as_utc(session_state.refresh_after)
        if refresh_after is None and token_expires_at is not None:
            refresh_after = token_expires_at - TOKEN_REUSE_MARGIN
        snapshot_refresh_at = cached_at + timedelta(
            seconds=self.settings.exec_agent_max_account_snapshot_age_seconds
        )
        refresh_candidates = [
            value
            for value in (refresh_after, token_expires_at, snapshot_refresh_at)
            if value is not None
        ]
        refresh_before = min(refresh_candidates, default=cached_at)
        execution_context_by_symbol = _load_monitoring_execution_context_by_symbol(
            settings=self.settings,
            db=db,
            symbols=unique_symbols,
        )
        return _KisMonitoringRuntimeContext(
            cached_at=cached_at,
            refresh_before=refresh_before,
            access_token=access_token,
            covered_symbols=frozenset(unique_symbols),
            execution_context_by_symbol={
                symbol: _copy_execution_context(context)
                for symbol, context in execution_context_by_symbol.items()
            },
        )

    def invalidate_runtime_context(self, *, reason: str) -> None:
        del reason
        with self._runtime_context_lock:
            self._runtime_context_generation += 1
            self._runtime_context = None
            self._runtime_context_retry_at = None

    @contextmanager
    def _runtime_context_scope(
        self,
        *,
        symbols: list[str],
    ):
        runtime_context = self._cached_runtime_context(symbols=symbols)
        if runtime_context is None:
            with self._runtime_context_lock:
                retry_at = self._runtime_context_retry_at
            if retry_at is not None and datetime.now(UTC) < retry_at:
                raise RuntimeError(
                    "monitoring runtime context refresh is in retry backoff"
                )
            with self.session_factory() as db:
                self.prime_runtime_context(
                    db=db,
                    symbols=symbols,
                    loaded_at=datetime.now(UTC),
                )
            runtime_context = self._cached_runtime_context(symbols=symbols)
        if runtime_context is None:
            raise RuntimeError("monitoring runtime context cache was not populated")
        yield runtime_context

    def _cached_runtime_context(
        self,
        *,
        symbols: list[str],
    ) -> _KisMonitoringRuntimeContext | None:
        requested_symbols = frozenset(symbols)
        now = datetime.now(UTC)
        with self._runtime_context_lock:
            runtime_context = self._runtime_context
            if (
                runtime_context is None
                or now >= runtime_context.refresh_before
                or not requested_symbols.issubset(runtime_context.covered_symbols)
            ):
                return None
            return runtime_context

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
            "durably_unquoteable_symbols": [],
            "empty_quote_consecutive_counts": {},
            "context_failures": [],
            "context_failure_count": 0,
            "context_failures_truncated": False,
        }
        if not unique_symbols:
            self._empty_quote_counts.clear()
            return []
        if not self._quote_batch_lock.acquire(blocking=False):
            for symbol in unique_symbols:
                self._empty_quote_counts.pop(symbol, None)
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
                "durably_unquoteable_symbols": [],
                "empty_quote_consecutive_counts": {},
                "context_failures": [],
                "context_failure_count": 0,
                "context_failures_truncated": False,
                "quote_batch_in_flight": True,
            }
            return []

        broker_adapter = self.broker_adapter_factory(settings)
        release_quote_batch_lock = True
        try:
            requested_symbol_set = set(unique_symbols)
            for symbol in list(self._empty_quote_counts):
                if symbol not in requested_symbol_set:
                    self._empty_quote_counts.pop(symbol, None)
            with self._runtime_context_scope(symbols=unique_symbols) as runtime_context:
                access_token = runtime_context.access_token
                execution_context_by_symbol = runtime_context.execution_context_by_symbol
                exchange_by_symbol = {
                    symbol: str(context["exchange"])
                    for symbol, context in execution_context_by_symbol.items()
                }
                prices: list[MonitoringPriceRequest] = []
                priced_symbols: list[str] = []
                failures: list[dict[str, object]] = []
                context_failures: list[dict[str, object]] = []
                failure_count = 0
                context_failure_count = 0
                durably_unquoteable_symbols: list[str] = []

                def fetch_quote(symbol: str) -> tuple[str, BrokerCurrentPrice | None, Exception | None]:
                    try:
                        expected_context = execution_context_by_symbol.get(symbol)
                        if expected_context is None:
                            return symbol, None, None
                        expected_provider = str(expected_context.get("provider") or "")
                        if expected_provider != broker_adapter.provider:
                            return (
                                symbol,
                                None,
                                ValueError(
                                    "monitoring quote provider does not match active route"
                                ),
                            )
                        expected_exchange = exchange_by_symbol[symbol]
                        quote = broker_adapter.get_current_price(
                            access_token,
                            symbol=symbol,
                            exchange=expected_exchange,
                        )
                        if quote is not None and (
                            quote.symbol.strip().upper() != symbol
                            or quote.exchange.strip().upper() != expected_exchange
                        ):
                            return (
                                symbol,
                                None,
                                ValueError(
                                    "monitoring quote identity does not match active route"
                                ),
                            )
                        return (
                            symbol,
                            quote,
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

                if any(
                    _is_broker_auth_rejection(error)
                    for _quote, error in quote_results.values()
                ):
                    self.invalidate_runtime_context(reason="broker_quote_auth_rejected")

                context_futures = {
                    symbol: self._context_executor.submit(
                        _fetch_live_market_context,
                        broker_adapter=broker_adapter,
                        access_token=access_token,
                        quote=quote,
                    )
                    for symbol in unique_symbols
                    for quote, error in [quote_results.get(symbol, (None, None))]
                    if quote is not None and error is None
                }

                for symbol in unique_symbols:
                    quote, error = quote_results.get(symbol, (None, None))
                    if error is not None:
                        if (
                            isinstance(error, BrokerAdapterError)
                            and error.code == KIS_OVERSEAS_QUOTE_EMPTY_CODE
                        ):
                            empty_count = self._empty_quote_counts.get(symbol, 0) + 1
                            self._empty_quote_counts[symbol] = empty_count
                            if empty_count >= DURABLE_EMPTY_QUOTE_OBSERVATION_COUNT:
                                durably_unquoteable_symbols.append(symbol)
                        else:
                            self._empty_quote_counts.pop(symbol, None)
                        failure_count += 1
                        if len(failures) < MAX_REPORTED_QUOTE_FAILURES:
                            failure: dict[str, object] = {
                                "symbol": symbol,
                                "error": str(error),
                            }
                            if isinstance(error, BrokerAdapterError) and error.code:
                                failure["code"] = error.code
                            failures.append(failure)
                        continue
                    if quote is None:
                        self._empty_quote_counts.pop(symbol, None)
                        continue
                    self._empty_quote_counts.pop(symbol, None)
                    context, context_failure = context_futures[symbol].result()
                    if context_failure is not None:
                        context_failure_count += 1
                        if len(context_failures) < MAX_REPORTED_CONTEXT_FAILURES:
                            context_failures.append(context_failure)
                    priced_symbols.append(quote.symbol)
                    prices.append(
                        monitoring_price_from_broker_quote(
                            settings=settings,
                            quote=quote,
                            context=context,
                            context_failure=context_failure,
                            side=str(
                                execution_context_by_symbol.get(symbol, {}).get(
                                    "side",
                                    "sell",
                                )
                            ),
                            purpose=str(
                                execution_context_by_symbol.get(symbol, {}).get(
                                    "purpose",
                                    "protective_stop",
                                )
                            ),
                            order_contexts=_order_contexts_from_execution_context(
                                execution_context_by_symbol.get(symbol)
                            ),
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
                    "durably_unquoteable_symbols": durably_unquoteable_symbols,
                    "empty_quote_consecutive_counts": {
                        symbol: self._empty_quote_counts[symbol]
                        for symbol in unique_symbols
                        if symbol in self._empty_quote_counts
                    },
                    "context_failures": context_failures,
                    "context_failure_count": context_failure_count,
                    "context_failures_truncated": context_failure_count > len(context_failures),
                    "quote_batch_timed_out": batch_timed_out,
                    "exchange_by_symbol": exchange_by_symbol,
                }
                return prices
        except Exception:
            for symbol in unique_symbols:
                self._empty_quote_counts.pop(symbol, None)
            raise
        finally:
            if release_quote_batch_lock:
                self._quote_batch_lock.release()

    def close(self) -> None:
        self.invalidate_runtime_context(reason="price_source_closed")
        self._quote_executor.shutdown(wait=False, cancel_futures=True)
        self._context_executor.shutdown(wait=False, cancel_futures=True)

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


def _market_quality_from_quote_payload(
    payload: dict[str, object],
    *,
    quote: BrokerCurrentPrice,
) -> dict[str, object]:
    output = payload.get("output")
    if not isinstance(output, dict):
        output = payload
    values = {
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
    }
    endpoint = _endpoint_from_payload(quote.payload)
    values["provenance"] = {
        "source": "broker_current_price_payload",
        "field_sources": {
            key: {
                "source": "broker_current_price_payload",
                "endpoint": endpoint,
                "observed_at": quote.observed_at.isoformat(),
                "freshness_source": quote.freshness_source,
            }
            for key, value in values.items()
            if value is not None
        },
    }
    return values


def monitoring_price_from_broker_quote(
    *,
    settings: Settings,
    quote: BrokerCurrentPrice,
    context: BrokerLiveMarketContext | None,
    context_failure: dict[str, object] | None,
    side: str,
    purpose: str,
    order_contexts: list[dict[str, str]] | None = None,
) -> MonitoringPriceRequest:
    market_quality = _market_quality_from_quote_payload(quote.payload, quote=quote)
    market_quality = _merge_live_market_context(
        market_quality,
        context=context,
        context_failure=context_failure,
        quote=quote,
        settings=settings,
        side=side,
        purpose=purpose,
        order_contexts=order_contexts,
    )
    market_quality = _sanitize_monitoring_market_quality(market_quality)
    observed_at = quote.observed_at
    freshness_source = quote.freshness_source
    if (
        settings.runtime_mode == "live"
        and context is not None
        and _is_trusted_exchange_freshness_source(context.freshness_source)
    ):
        observed_at = context.observed_at
        freshness_source = context.freshness_source
    return MonitoringPriceRequest(
        symbol=quote.symbol,
        # Keep the transport's reference/last price neutral. Monitoring picks
        # ask for buys and bid for sells per item, so one symbol shared by
        # opposite-side monitors never inherits the DB ordering of a context.
        price=quote.price,
        observed_at=observed_at,
        freshness_source=freshness_source,
        **market_quality,
    )


def _is_trusted_exchange_freshness_source(source: object) -> bool:
    return isinstance(source, str) and source in {
        "kis_exchange_orderbook_time",
        "kis_exchange_intraday_minute_time",
        "kis_overseas_exchange_bar_time",
        "kis_overseas_exchange_orderbook_time",
        "alpaca_exchange_trade_time",
        "alpaca_exchange_quote_time",
        "broker_realtime_exchange_time",
    }


def _load_monitoring_exchange_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> dict[str, str]:
    return {
        symbol: str(context["exchange"])
        for symbol, context in _load_monitoring_execution_context_by_symbol(
            settings=settings,
            db=db,
            symbols=symbols,
        ).items()
    }


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_broker_auth_rejection(error: Exception | None) -> bool:
    return bool(
        isinstance(error, BrokerAdapterError)
        and (
            error.classification == BrokerErrorClassification.AUTH
            or error.status_code in {401, 403}
            or error.response_metadata.get("status_code") in {401, 403}
        )
    )


def _copy_execution_context(context: dict[str, object]) -> dict[str, object]:
    copied = dict(context)
    order_contexts = context.get("order_contexts")
    if isinstance(order_contexts, list):
        copied["order_contexts"] = [
            dict(item) for item in order_contexts if isinstance(item, dict)
        ]
    return copied


def _load_monitoring_execution_context_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: list[str],
) -> dict[str, dict[str, object]]:
    if not symbols:
        return {}
    rows = db.scalars(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItem.symbol.in_(symbols),
            ExecutionPlanItem.intent.in_(tuple(_ROUTE_AUTHORITY_INTENTS)),
            ExecutionPlanItem.status.in_(tuple(_ROUTE_AUTHORITY_STATUSES)),
        )
        .order_by(ExecutionPlanItem.updated_at.desc())
    ).all()
    candidates: dict[str, list[tuple[str, str, str, str]]] = {}
    for item in rows:
        risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
        exchange = str(risk_context.get("exchange") or "").strip().upper()
        resolved_exchange: str | None = None
        if (
            item.provider == "kis"
            and item.asset_class == "us_equity"
            and exchange in {"NASD", "NYSE", "AMEX"}
        ):
            resolved_exchange = exchange
        elif (
            item.provider in {None, "kis"}
            and item.asset_class != "us_equity"
            and item.symbol.isdigit()
            and len(item.symbol) == 6
        ):
            resolved_exchange = "KRX"
        if resolved_exchange is None:
            continue
        purpose = _monitoring_order_purpose(item)
        candidates.setdefault(item.symbol, []).append(
            ("kis", resolved_exchange, item.side, purpose)
        )
    contexts: dict[str, dict[str, object]] = {}
    for symbol, symbol_candidates in candidates.items():
        routes = {
            (provider, exchange)
            for provider, exchange, _side, _purpose in symbol_candidates
        }
        if len(routes) != 1:
            # Two active rows that disagree about venue are not safe quote
            # authority. A held-position route may still resolve the symbol
            # below; otherwise this symbol remains intentionally unpriced.
            continue
        provider, resolved_exchange = next(iter(routes))
        _provider, _exchange, side, purpose = symbol_candidates[0]
        context: dict[str, object] = {
            "provider": provider,
            "exchange": resolved_exchange,
            "side": side,
            "purpose": purpose,
            "order_contexts": [],
        }
        contexts[symbol] = context
        order_contexts = context["order_contexts"]
        assert isinstance(order_contexts, list)
        for (
            _candidate_provider,
            _candidate_exchange,
            candidate_side,
            candidate_purpose,
        ) in symbol_candidates:
            order_context = {"side": candidate_side, "purpose": candidate_purpose}
            if order_context not in order_contexts:
                order_contexts.append(order_context)
    missing_symbols = [symbol for symbol in symbols if symbol not in contexts]
    if missing_symbols:
        try:
            active_identity = build_position_identity(
                settings=settings,
                provider="kis",
                exchange="KRX",
            )
        except PositionIdentityConflictError:
            positions = []
        else:
            positions = db.scalars(
                select(ExecutionPosition).where(
                    ExecutionPosition.mode == settings.runtime_mode,
                    ExecutionPosition.provider == active_identity.provider,
                    ExecutionPosition.account_scope == active_identity.account_scope,
                    ExecutionPosition.exchange.in_(("NASD", "NYSE", "AMEX")),
                    ExecutionPosition.symbol.in_(missing_symbols),
                    ExecutionPosition.quantity > 0,
                )
            ).all()
        positions_by_symbol: dict[str, list[ExecutionPosition]] = {}
        for position in positions:
            positions_by_symbol.setdefault(position.symbol, []).append(position)
        for symbol, scoped_positions in positions_by_symbol.items():
            if len(scoped_positions) != 1:
                continue
            position = scoped_positions[0]
            raw_payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
            sync_context = raw_payload.get("_position_sync")
            if not isinstance(sync_context, dict):
                continue
            exchange = str(sync_context.get("exchange") or "").strip().upper()
            if (
                sync_context.get("provider", "kis") == "kis"
                and sync_context.get("asset_class") == "us_equity"
                and sync_context.get("monitoring_eligible") is True
                and exchange in {"NASD", "NYSE", "AMEX"}
                and exchange == position.exchange
            ):
                contexts[symbol] = {
                    "provider": "kis",
                    "exchange": exchange,
                    "side": "sell",
                    "purpose": "protective_stop",
                    "order_contexts": [
                        {"side": "sell", "purpose": "protective_stop"}
                    ],
                }
    # A six-digit domestic security identifier is itself sufficient to bind a
    # KIS quote to KRX. Keep that deterministic fallback for direct diagnostic
    # quote requests; never infer a US venue from a bare symbol.
    for symbol in symbols:
        if symbol not in contexts and symbol.isdigit() and len(symbol) == 6:
            contexts[symbol] = {
                "provider": "kis",
                "exchange": "KRX",
                "side": "sell",
                "purpose": "protective_stop",
                "order_contexts": [
                    {"side": "sell", "purpose": "protective_stop"}
                ],
            }
    return contexts


def _order_contexts_from_execution_context(
    context: dict[str, object] | None,
) -> list[dict[str, str]]:
    if not isinstance(context, dict):
        return []
    value = context.get("order_contexts")
    if not isinstance(value, list):
        return []
    return [
        {"side": str(item["side"]), "purpose": str(item["purpose"])}
        for item in value
        if isinstance(item, dict)
        and isinstance(item.get("side"), str)
        and isinstance(item.get("purpose"), str)
    ]


def _monitoring_order_purpose(item: ExecutionPlanItem) -> str:
    return "protective_stop" if item.intent == "stop_sell" else item.intent


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
        context = context_getter(
            access_token,
            symbol=quote.symbol,
            exchange=quote.exchange,
            reference_price=quote.price,
        )
        if context is not None:
            payload = context.payload if isinstance(context.payload, dict) else {}
            if (
                context.symbol.strip().upper() != quote.symbol.strip().upper()
                or context.exchange.strip().upper() != quote.exchange.strip().upper()
                or (
                    payload.get("provider") is not None
                    and payload.get("provider") != broker_adapter.provider
                )
            ):
                return None, {
                    "symbol": quote.symbol,
                    "endpoint": "broker_live_market_context",
                    "reason": "live market context identity does not match active quote route",
                }
        return context, None
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
    side: str = "sell",
    purpose: str = "protective_stop",
    order_contexts: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    merged = dict(base)
    provenance = dict(merged.get("provenance") or {})
    field_sources = dict(provenance.get("field_sources") or {})
    provenance["current_price"] = {
        "source": "broker_current_price_payload",
        "endpoint": _endpoint_from_payload(quote.payload),
        "freshness_source": quote.freshness_source,
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
                field_sources[key] = {
                    "source": "broker_live_market_context",
                    "endpoint": _endpoint_from_payload(context.payload),
                    "observed_at": context.observed_at.isoformat(),
                    "freshness_source": context.freshness_source,
                }
            else:
                # A live context is the authoritative orderbook snapshot. Do
                # not retain a side or derived metric from an older current-
                # price payload and then label it with the context timestamp.
                merged.pop(key, None)
                field_sources.pop(key, None)
        provenance["orderbook"] = {
            "source": "broker_live_market_context",
            "endpoint": _endpoint_from_payload(context.payload),
            "observed_at": context.observed_at.isoformat(),
            "freshness_source": context.freshness_source,
        }
    if context_failure is not None:
        provenance["orderbook"] = {
            "source": "broker_live_market_context",
            "status": "failed",
            **context_failure,
        }

    broker_orderability = {
        "session_type": merged.get("session_type"),
        "normal_ordering_available": merged.get("normal_ordering_available"),
    }
    route_context = _derive_orderability_context(
        settings=settings,
        symbol=quote.symbol,
        price=quote.price,
        observed_at=quote.observed_at,
        exchange=quote.exchange,
        side=side,
        purpose=purpose,
    )
    for key, value in route_context.items():
        if key in {"session_type", "normal_ordering_available"} and merged.get(key) is None:
            merged[key] = value
    provenance["session"] = {
        "source": route_context["orderability_source"],
        "reason": route_context["orderability_reason"],
        "action": route_context["orderability_action"],
        "exchange": route_context["orderability_exchange"],
        "observed_at": quote.observed_at.isoformat(),
    }
    provenance["broker_orderability"] = broker_orderability
    provenance["field_sources"] = field_sources
    contexts = [
        {"side": side, "purpose": purpose},
        *(order_contexts or []),
    ]
    orderability_by_item: dict[str, dict[str, object]] = {}
    for order_context in contexts:
        context_side = str(order_context.get("side") or "").strip().lower()
        context_purpose = str(order_context.get("purpose") or "").strip().lower()
        if context_side not in {"buy", "sell"} or not context_purpose:
            continue
        key = monitoring_orderability_key(
            side=context_side,
            purpose=context_purpose,
        )
        if key in orderability_by_item:
            continue
        orderability_by_item[key] = (
            route_context
            if context_side == side and context_purpose == purpose
            else _derive_orderability_context(
                settings=settings,
                symbol=quote.symbol,
                price=quote.price,
                observed_at=quote.observed_at,
                exchange=quote.exchange,
                side=context_side,
                purpose=context_purpose,
            )
        )
    provenance["orderability_by_item"] = orderability_by_item

    missing_fields = _missing_market_quality_fields(merged)
    if missing_fields:
        provenance["missing_fields"] = missing_fields
    merged["provenance"] = provenance
    return merged


def monitoring_orderability_key(*, side: str, purpose: str) -> str:
    return f"{side.strip().lower()}:{purpose.strip().lower()}"


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
    exchange: str = "KRX",
    side: str = "sell",
    purpose: str = "protective_stop",
) -> dict[str, object]:
    normalized_exchange = str(exchange or "KRX").strip().upper()
    if normalized_exchange in {"NASD", "NYSE", "AMEX"}:
        observed = observed_at
        if not isinstance(observed, datetime):
            raise ValueError("US orderability requires a datetime observation")
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        local = observed.astimezone(ZoneInfo("America/New_York"))
        is_regular = is_us_regular_session(local)
        return {
            "session_type": "us_regular" if is_regular else "us_closed",
            "normal_ordering_available": is_regular,
            "orderability_action": "submit" if is_regular else "hold",
            "orderability_exchange": normalized_exchange,
            "orderability_reason": "us_regular_session" if is_regular else "outside_us_regular_session",
            "orderability_source": "us_regular_session_clock",
        }

    route_decision = resolve_domestic_order_route(
        settings=settings,
        now=observed_at,
        symbol=symbol,
        side=side,
        purpose=purpose,
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
        "orderability_source": "domestic_order_router",
    }


def is_us_regular_session(local: datetime) -> bool:
    market_date = local.date()
    if market_date.weekday() >= 5 or market_date in _us_market_holidays(market_date.year):
        return False
    close_time = datetime_time(13, 0) if _is_us_early_close(market_date) else datetime_time(16, 0)
    return datetime_time(9, 30) <= local.timetz().replace(tzinfo=None) < close_time


def _is_us_early_close(market_date: date) -> bool:
    if market_date.weekday() >= 5 or market_date in _us_market_holidays(market_date.year):
        return False
    thanksgiving = _nth_weekday(market_date.year, 11, 3, 4)
    if market_date == thanksgiving + timedelta(days=1):
        return True
    if market_date.month == 12 and market_date.day == 24:
        return True
    return market_date.month == 7 and market_date.day == 3


def next_us_regular_open(observed_at: datetime) -> datetime:
    observed = observed_at if observed_at.tzinfo is not None else observed_at.replace(tzinfo=UTC)
    local = observed.astimezone(ZoneInfo("America/New_York"))
    candidate_date = local.date()
    local_open = datetime.combine(candidate_date, datetime_time(9, 30), tzinfo=local.tzinfo)
    if is_us_regular_session(local_open) and local < local_open:
        return local_open.astimezone(UTC)
    candidate_date += timedelta(days=1)
    while True:
        local_open = datetime.combine(candidate_date, datetime_time(9, 30), tzinfo=local.tzinfo)
        if is_us_regular_session(local_open):
            return local_open.astimezone(UTC)
        candidate_date += timedelta(days=1)


def _us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed_fixed_holiday(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(date(year, 6, 19)))
    next_new_year = _observed_fixed_holiday(date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays.add(next_new_year)
    return holidays


def _observed_fixed_holiday(value: date) -> date:
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    candidate = next_month - timedelta(days=1)
    return candidate - timedelta(days=(candidate.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


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
