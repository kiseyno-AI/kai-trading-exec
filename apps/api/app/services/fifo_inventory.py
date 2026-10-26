from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import FifoInventoryIsolation
from app.services.execution_semantics import KRX_ASSET_CLASS_ALIASES, KRX_EXCHANGES
from app.services.position_identity import EXECUTION_SCOPE_POLICY_VERSION


FIFO_VERIFIED_FLAT_EVIDENCE_SOURCE = "broker_flat_reconciliation"
KIS_BROKER_NAMES = frozenset({"kis", "korea-investment", "korea_investment"})


@dataclass(frozen=True)
class FifoRouteScope:
    provider: str
    account_scope: str
    venue: str
    asset_class: str
    exchange: str

    def __post_init__(self) -> None:
        normalized = {
            "provider": str(self.provider or "").strip().lower(),
            "account_scope": str(self.account_scope or "").strip(),
            "venue": str(self.venue or "").strip().upper(),
            "asset_class": str(self.asset_class or "").strip().lower(),
            "exchange": str(self.exchange or "").strip().upper(),
        }
        if not all(normalized.values()):
            raise ValueError(
                "FIFO route scope requires provider, account, venue, asset, and exchange"
            )
        for field_name, value in normalized.items():
            object.__setattr__(self, field_name, value)

    def as_payload(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "account_scope": self.account_scope,
            "venue": self.venue,
            "asset_class": self.asset_class,
            "exchange": self.exchange,
        }


def canonical_fifo_inventory_route_scope(
    route_scope: FifoRouteScope,
) -> FifoRouteScope:
    """Return the physical inventory scope for an already-validated route.

    KIS domestic stock orders can execute through KRX, NXT, or SOR while
    settling into the same authenticated brokerage-account inventory.  Keep
    the persisted execution route exact, but collapse those route variants at
    FIFO/inventory boundaries so a later protective order can consume the lot
    bought through another domestic route.

    Every non-KIS, non-domestic, or non-stock route remains exact.  Callers
    must first construct/validate ``FifoRouteScope``; this helper deliberately
    does not try to repair incomplete or inconsistent persisted scopes.
    """

    if (
        route_scope.provider in KIS_BROKER_NAMES
        and route_scope.asset_class in KRX_ASSET_CLASS_ALIASES
        and route_scope.exchange in KRX_EXCHANGES
    ):
        return FifoRouteScope(
            provider="kis",
            account_scope=route_scope.account_scope,
            venue="KIS",
            asset_class="stock",
            exchange="KRX",
        )
    return route_scope


FifoChronologyBucket = tuple[FifoRouteScope, str, datetime]


class FifoChronologyAmbiguityBuckets(set[FifoChronologyBucket]):
    """Ambiguous buckets with bounded observations kept distinct.

    Set behavior preserves the existing public contract while the additional
    classification lets the shared predicate apply bidirectional generation
    contamination only to observation upper bounds.
    """

    def __init__(
        self,
        buckets: Iterable[FifoChronologyBucket] = (),
        *,
        observation_bounded_buckets: Iterable[FifoChronologyBucket] = (),
    ) -> None:
        super().__init__(buckets)
        self.observation_bounded_buckets = set(observation_bounded_buckets)


@dataclass
class FifoIsolationCursor:
    """Apply durable broker-flat boundaries while replaying local fills."""

    isolated_at_by_route_symbol: dict[tuple[FifoRouteScope, str], list[datetime]]
    legacy_unscoped_at_by_symbol: dict[str, list[datetime]] = field(
        default_factory=dict
    )
    _applied_count_by_route_symbol: dict[tuple[FifoRouteScope, str], int] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        canonical_boundaries: dict[tuple[FifoRouteScope, str], list[datetime]] = {}
        for (route_scope, symbol), boundaries in (
            self.isolated_at_by_route_symbol.items()
        ):
            route_symbol = (
                canonical_fifo_inventory_route_scope(route_scope),
                str(symbol or "").strip().upper(),
            )
            canonical_boundaries.setdefault(route_symbol, []).extend(boundaries)
        for boundaries in canonical_boundaries.values():
            boundaries.sort()
        self.isolated_at_by_route_symbol = canonical_boundaries

    def apply_before(
        self,
        *,
        route_scope: FifoRouteScope | None = None,
        symbol: str,
        observed_at: datetime,
        lots: list[object],
    ) -> bool:
        normalized_symbol = str(symbol or "").strip().upper()
        observed_at_utc = _as_utc(observed_at)
        if route_scope is None:
            return False
        route_key = (
            canonical_fifo_inventory_route_scope(route_scope),
            normalized_symbol,
        )
        boundaries = self.isolated_at_by_route_symbol.get(route_key, [])
        index = self._applied_count_by_route_symbol.get(route_key, 0)
        while index < len(boundaries) and boundaries[index] < observed_at_utc:
            lots.clear()
            index += 1
        self._applied_count_by_route_symbol[route_key] = index
        latest_exact_boundary = self.latest_boundary_before(
            route_scope=route_scope,
            symbol=normalized_symbol,
            observed_at=observed_at_utc,
        )
        return not self._has_unresolved_legacy_boundary(
            symbol=normalized_symbol,
            observed_at=observed_at_utc,
            latest_exact_boundary=latest_exact_boundary,
        )

    def apply_through(
        self,
        *,
        route_scope: FifoRouteScope | None = None,
        symbol: str,
        observed_at: datetime,
        lots: list[object],
    ) -> bool:
        normalized_symbol = str(symbol or "").strip().upper()
        observed_at_utc = _as_utc(observed_at)
        if route_scope is None:
            return False
        route_key = (
            canonical_fifo_inventory_route_scope(route_scope),
            normalized_symbol,
        )
        boundaries = self.isolated_at_by_route_symbol.get(route_key, [])
        index = self._applied_count_by_route_symbol.get(route_key, 0)
        while index < len(boundaries) and boundaries[index] <= observed_at_utc:
            lots.clear()
            index += 1
        self._applied_count_by_route_symbol[route_key] = index
        latest_exact_boundary = self.latest_boundary_through(
            route_scope=route_scope,
            symbol=normalized_symbol,
            observed_at=observed_at_utc,
        )
        return not self._has_unresolved_legacy_boundary(
            symbol=normalized_symbol,
            observed_at=observed_at_utc,
            latest_exact_boundary=latest_exact_boundary,
        )

    def _has_unresolved_legacy_boundary(
        self,
        *,
        symbol: str,
        observed_at: datetime,
        latest_exact_boundary: datetime | None,
    ) -> bool:
        """Whether an applicable unscoped boundary lacks a newer exact proof."""

        return any(
            (legacy_boundary := _as_utc(boundary)) <= observed_at
            and (
                latest_exact_boundary is None
                or legacy_boundary >= latest_exact_boundary
            )
            for boundary in self.legacy_unscoped_at_by_symbol.get(symbol, [])
        )

    def latest_boundary_before(
        self,
        *,
        route_scope: FifoRouteScope,
        symbol: str,
        observed_at: datetime,
    ) -> datetime | None:
        """Return the latest exact-scope flat proof strictly before an event.

        A boundary sharing the fill's timestamp cannot establish which happened
        first, so it must not erase chronology uncertainty in that same bucket.
        """

        observed_at_utc = _as_utc(observed_at)
        boundaries = self.isolated_at_by_route_symbol.get(
            (
                canonical_fifo_inventory_route_scope(route_scope),
                str(symbol or "").strip().upper(),
            ),
            [],
        )
        return next(
            (
                boundary
                for boundary in reversed(boundaries)
                if boundary < observed_at_utc
            ),
            None,
        )
    def latest_boundary_through(
        self,
        *,
        route_scope: FifoRouteScope,
        symbol: str,
        observed_at: datetime,
    ) -> datetime | None:
        observed_at_utc = _as_utc(observed_at)
        boundaries = self.isolated_at_by_route_symbol.get(
            (
                canonical_fifo_inventory_route_scope(route_scope),
                str(symbol or "").strip().upper(),
            ),
            [],
        )
        return next(
            (
                boundary
                for boundary in reversed(boundaries)
                if boundary <= observed_at_utc
            ),
            None,
        )


def legacy_kis_krx_event_matches(
    *,
    order: object,
    event_symbol: str,
    exact_route_scope: FifoRouteScope | None,
) -> bool:
    if (
        exact_route_scope is None
        or canonical_fifo_inventory_route_scope(exact_route_scope).provider != "kis"
        or canonical_fifo_inventory_route_scope(exact_route_scope).exchange != "KRX"
        or fifo_route_scope_from_order(order) is not None
    ):
        return False
    metadata = getattr(order, "request_metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if "execution_scope" in metadata or "broker_account_scope" in metadata:
        return False
    order_symbol = str(getattr(order, "symbol", None) or "").strip().upper()
    if not order_symbol or order_symbol != str(event_symbol or "").strip().upper():
        return False
    route_metadata = metadata.get("route")
    route_metadata = route_metadata if isinstance(route_metadata, dict) else {}
    explicit_exchange = str(
        metadata.get("exchange") or route_metadata.get("exchange") or ""
    ).strip().upper()
    if explicit_exchange and explicit_exchange not in KRX_EXCHANGES:
        return False
    broker_name = str(getattr(order, "broker_name", None) or "").strip().lower()
    venue = str(getattr(order, "venue", None) or "").strip().upper()
    asset_class = str(getattr(order, "asset_class", None) or "").strip().lower()
    if (
        broker_name not in KIS_BROKER_NAMES
        or venue not in KRX_EXCHANGES
        or asset_class not in KRX_ASSET_CLASS_ALIASES
    ):
        return False
    return True


def legacy_kis_krx_event_is_isolated(
    *,
    order: object,
    event_symbol: str,
    event_at: datetime,
    through: datetime,
    isolation_cursor: FifoIsolationCursor,
    exact_route_scope: FifoRouteScope | None,
) -> bool:
    """Return whether exact current-account flat proof closes legacy KRX history.

    This deliberately does not attribute the legacy order or fill to the route.
    It only proves that a KIS domestic event strictly before the exact scoped
    flat boundary cannot affect the current FIFO generation.
    """

    if not legacy_kis_krx_event_matches(
        order=order,
        event_symbol=event_symbol,
        exact_route_scope=exact_route_scope,
    ):
        return False
    assert exact_route_scope is not None
    order_symbol = str(getattr(order, "symbol", None) or "").strip().upper()
    boundary = isolation_cursor.latest_boundary_through(
        route_scope=exact_route_scope,
        symbol=order_symbol,
        observed_at=through,
    )
    return boundary is not None and _as_utc(event_at) < boundary


@dataclass(frozen=True)
class OpenLongFillInventoryReplay:
    """Point-in-time long inventory reconstructed only from canonical fills."""

    open_quantities: dict[tuple[FifoRouteScope, str], Decimal]
    indeterminate_fill_count: int
    open_epoch_start_fills: dict[tuple[FifoRouteScope, str], object] = field(
        default_factory=dict
    )


def ambiguous_fifo_chronology_route_symbols(
    fills: Sequence[object],
) -> set[tuple[FifoRouteScope, str]]:
    """Return routes containing at least one ambiguous FIFO bucket."""

    return {
        (route_scope, symbol)
        for route_scope, symbol, _observed_at in ambiguous_fifo_chronology_buckets(
            fills
        )
    }


def ambiguous_fifo_chronology_buckets(
    fills: Sequence[object],
) -> FifoChronologyAmbiguityBuckets:
    """Return exact timestamp buckets that cannot be replayed authoritatively.

    Broker fill timestamps may have only second precision.  A random database
    identifier must never decide either opposing fills or materially different
    same-side lots in that same second. The latter changes future partial FIFO
    realization even though current aggregate quantity happens to be stable.

    Cumulative reconciliation can only prove that a fill existed no later than
    the observation time.  That upper bound is not an execution timestamp, so a
    generation containing even one such fill is ambiguous until a later
    verified-flat isolation boundary starts a clean generation.
    """

    fills_by_bucket: dict[
        tuple[FifoRouteScope, str, datetime],
        list[object],
    ] = {}
    for fill in fills:
        observed_at = getattr(fill, "filled_at", None)
        if not isinstance(observed_at, datetime):
            continue
        order = getattr(fill, "broker_order", None)
        exact_route_scope = fifo_route_scope_from_order(order)
        route_scope = (
            canonical_fifo_inventory_route_scope(exact_route_scope)
            if exact_route_scope is not None
            else None
        )
        symbol = str(getattr(fill, "symbol", "") or "").strip().upper()
        order_symbol = str(getattr(order, "symbol", "") or "").strip().upper()
        side = str(getattr(fill, "side", "") or "").strip().lower()
        if (
            route_scope is None
            or not symbol
            or (order_symbol and order_symbol != symbol)
            or side not in {"buy", "sell"}
        ):
            continue
        fills_by_bucket.setdefault(
            (route_scope, symbol, _as_utc(observed_at)),
            [],
        ).append(fill)
    observation_bounded_buckets = {
        bucket
        for bucket, bucket_fills in fills_by_bucket.items()
        if any(_fill_time_is_observation_bounded(fill) for fill in bucket_fills)
    }
    same_timestamp_buckets = {
        bucket
        for bucket, bucket_fills in fills_by_bucket.items()
        if _fifo_bucket_has_same_timestamp_ambiguity(bucket_fills)
    }
    return FifoChronologyAmbiguityBuckets(
        observation_bounded_buckets | same_timestamp_buckets,
        observation_bounded_buckets=observation_bounded_buckets,
    )


def fifo_chronology_is_ambiguous_at(
    *,
    ambiguous_buckets: set[tuple[FifoRouteScope, str, datetime]],
    isolation_cursor: FifoIsolationCursor | None,
    route_scope: FifoRouteScope,
    symbol: str,
    observed_at: datetime,
) -> bool:
    """Whether the event belongs to a chronology-contaminated generation."""

    return fifo_chronology_ambiguity_reason_at(
        ambiguous_buckets=ambiguous_buckets,
        isolation_cursor=isolation_cursor,
        route_scope=route_scope,
        symbol=symbol,
        observed_at=observed_at,
    ) is not None


def fifo_chronology_ambiguity_reason_at(
    *,
    ambiguous_buckets: set[tuple[FifoRouteScope, str, datetime]],
    isolation_cursor: FifoIsolationCursor | None,
    route_scope: FifoRouteScope,
    symbol: str,
    observed_at: datetime,
) -> str | None:
    """Classify chronology uncertainty at one event.

    Same-timestamp ambiguity propagates forward because it can alter later FIFO
    matching. An observation upper bound is different: the execution may have
    occurred before *or* after earlier events, so it contaminates its entire
    exact-route generation in both directions. Only a later verified-flat
    boundary starts a generation that is independent of that bounded fill.
    """

    route_scope = canonical_fifo_inventory_route_scope(route_scope)
    normalized_symbol = str(symbol or "").strip().upper()
    event_at = _as_utc(observed_at)
    event_boundary = (
        isolation_cursor.latest_boundary_before(
            route_scope=route_scope,
            symbol=normalized_symbol,
            observed_at=event_at,
        )
        if isolation_cursor is not None
        else None
    )
    bounded_buckets = getattr(
        ambiguous_buckets,
        "observation_bounded_buckets",
        set(),
    )
    for candidate_scope, candidate_symbol, candidate_at in bounded_buckets:
        if candidate_scope != route_scope or candidate_symbol != normalized_symbol:
            continue
        candidate_boundary = (
            isolation_cursor.latest_boundary_before(
                route_scope=route_scope,
                symbol=normalized_symbol,
                observed_at=candidate_at,
            )
            if isolation_cursor is not None
            else None
        )
        if candidate_boundary == event_boundary:
            return "observation_bounded_generation"
    if any(
        candidate_scope == route_scope
        and candidate_symbol == normalized_symbol
        and candidate_at <= event_at
        and (event_boundary is None or candidate_at >= event_boundary)
        for candidate_scope, candidate_symbol, candidate_at in ambiguous_buckets
        if (
            candidate_scope,
            candidate_symbol,
            candidate_at,
        ) not in bounded_buckets
    ):
        return "same_timestamp_bucket"
    return None


def fifo_fill_sort_key(fill: object) -> tuple[datetime, int, int, str]:
    observed_at = getattr(fill, "filled_at", None)
    normalized_at = (
        _as_utc(observed_at)
        if isinstance(observed_at, datetime)
        else datetime.max.replace(tzinfo=UTC)
    )
    sequence = _authoritative_fill_sequence(fill)
    return (
        normalized_at,
        0 if sequence is not None else 1,
        sequence or 0,
        str(getattr(fill, "id", "") or ""),
    )


def _fifo_bucket_has_same_timestamp_ambiguity(fills: Sequence[object]) -> bool:
    if len(fills) < 2:
        return False
    sequences = [_authoritative_fill_sequence(fill) for fill in fills]
    if all(sequence is not None for sequence in sequences) and len(set(sequences)) == len(
        sequences
    ):
        return False
    return len({_fifo_fill_material_signature(fill) for fill in fills}) > 1


def _fill_time_is_observation_bounded(fill: object) -> bool:
    payload = getattr(fill, "payload", None)
    if not isinstance(payload, dict):
        return False
    precision = payload.get("fill_time_precision")
    if precision == "observed_upper_bound":
        return True
    if precision == "exact":
        return False
    if payload.get("source") == "operator_fill_evidence_repair":
        return True
    return payload.get("price_derivation") == "cumulative_notional_delta" or any(
        key in payload
        for key in (
            "cumulative_quantity",
            "cumulative_base_quantity",
            "cumulative_quote_notional",
        )
    )


def _fill_time_is_explicitly_exact(fill: object) -> bool:
    payload = getattr(fill, "payload", None)
    return isinstance(payload, dict) and payload.get("fill_time_precision") == "exact"


def _authoritative_fill_sequence(fill: object) -> int | None:
    payload = getattr(fill, "payload", None)
    if not isinstance(payload, dict):
        return None
    for key in ("broker_fill_sequence", "fill_sequence"):
        raw = payload.get(key)
        if isinstance(raw, bool):
            continue
        try:
            parsed = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if parsed >= 0 and str(parsed) == str(raw).strip():
            return parsed
    return None


def _fifo_fill_material_signature(fill: object) -> tuple[object, ...]:
    order = getattr(fill, "broker_order", None)
    return (
        str(getattr(fill, "side", "") or "").strip().lower(),
        _material_decimal(getattr(fill, "base_quantity", None)),
        _material_decimal(getattr(fill, "quantity", None)),
        _material_decimal(getattr(fill, "quote_notional", None)),
        _material_decimal(getattr(fill, "price", None)),
        _material_decimal(getattr(fill, "transaction_cost", None)),
        str(getattr(fill, "transaction_cost_coverage", "") or "")
        .strip()
        .lower(),
        str(getattr(fill, "transaction_cost_source", "") or "")
        .strip()
        .lower(),
        str(getattr(fill, "settlement_currency", "") or "").strip().upper(),
        str(getattr(order, "execution_plan_item_id", "") or ""),
    )


def _material_decimal(value: object) -> Decimal | None | str:
    if value is None:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return "invalid"
    return parsed if parsed.is_finite() else "invalid"


def ambiguous_fifo_chronology_fill_count(
    fills: Sequence[object],
    *,
    isolation_cursor: FifoIsolationCursor | None = None,
    through: datetime | None = None,
) -> int:
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    if not ambiguous_buckets:
        return 0
    reference_at = _as_utc(through or datetime.max.replace(tzinfo=UTC))
    ambiguous_routes = {
        (route_scope, symbol)
        for route_scope, symbol, ambiguous_at in ambiguous_buckets
        if ambiguous_at <= reference_at
        and fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=symbol,
            observed_at=reference_at,
        )
    }
    count = 0
    for fill in fills:
        order = getattr(fill, "broker_order", None)
        exact_route_scope = fifo_route_scope_from_order(order)
        route_scope = (
            canonical_fifo_inventory_route_scope(exact_route_scope)
            if exact_route_scope is not None
            else None
        )
        symbol = str(getattr(fill, "symbol", "") or "").strip().upper()
        if route_scope is not None and (route_scope, symbol) in ambiguous_routes:
            count += 1
    return count


def ambiguous_fifo_realized_pnl_sell_count(
    fills: Sequence[object],
    *,
    isolation_cursor: FifoIsolationCursor | None,
    start_at: datetime,
    end_at: datetime,
) -> int:
    """Count period sells whose FIFO cost basis is not authoritative.

    A broker-verified flat starts a clean generation for later events, but it
    cannot retroactively establish which same-timestamp lot funded an earlier
    sell. Evaluate ambiguity at each sell rather than at the report end time.
    """

    period_start = _as_utc(start_at)
    period_end = _as_utc(end_at)
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    if not ambiguous_buckets:
        return 0
    count = 0
    for fill in fills:
        observed_at = getattr(fill, "filled_at", None)
        if not isinstance(observed_at, datetime):
            continue
        observed_at = _as_utc(observed_at)
        if not period_start <= observed_at < period_end:
            continue
        if str(getattr(fill, "side", "") or "").strip().lower() != "sell":
            continue
        exact_route_scope = fifo_route_scope_from_order(
            getattr(fill, "broker_order", None)
        )
        route_scope = (
            canonical_fifo_inventory_route_scope(exact_route_scope)
            if exact_route_scope is not None
            else None
        )
        symbol = str(getattr(fill, "symbol", "") or "").strip().upper()
        if route_scope is None or not symbol:
            continue
        if fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=symbol,
            observed_at=observed_at,
        ):
            count += 1
    return count


def replay_open_long_fill_inventory(
    *,
    fills: Sequence[object],
    isolation_cursor: FifoIsolationCursor | None = None,
    through: datetime | None = None,
) -> OpenLongFillInventoryReplay:
    """Replay canonical fills without letting old sells erase later buys.

    Unmatched sells prove that local history is incomplete, but they do not
    consume a later buy.  Callers can therefore retain positive evidence while
    also surfacing the incomplete-history count instead of declaring a false
    flat position.
    """

    events: list[tuple[datetime, str, object]] = []
    indeterminate_fill_count = 0
    for fill in fills:
        observed_at = getattr(fill, "filled_at", None)
        if not isinstance(observed_at, datetime):
            indeterminate_fill_count += 1
            continue
        events.append(
            (
                _as_utc(observed_at),
                str(getattr(fill, "id", "") or ""),
                fill,
            )
        )

    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    lots_by_route_symbol: dict[
        tuple[FifoRouteScope, str], list[Decimal]
    ] = {}
    epoch_start_fill_by_route_symbol: dict[
        tuple[FifoRouteScope, str], object
    ] = {}
    indeterminate_by_route_symbol: dict[tuple[FifoRouteScope, str], int] = {}
    generation_by_route_symbol: dict[
        tuple[FifoRouteScope, str], datetime | None
    ] = {}
    for observed_at, _fill_id, fill in sorted(
        events,
        key=lambda event: fifo_fill_sort_key(event[2]),
    ):
        order = getattr(fill, "broker_order", None)
        exact_route_scope = fifo_route_scope_from_order(order)
        route_scope = (
            canonical_fifo_inventory_route_scope(exact_route_scope)
            if exact_route_scope is not None
            else None
        )
        symbol = str(getattr(fill, "symbol", "") or "").strip().upper()
        order_symbol = str(getattr(order, "symbol", "") or "").strip().upper()
        side = str(getattr(fill, "side", "") or "").strip().lower()
        try:
            raw_base_quantity = getattr(fill, "base_quantity", None)
            quantity = (
                Decimal(raw_base_quantity)
                if raw_base_quantity is not None
                and Decimal(raw_base_quantity) > 0
                else Decimal(getattr(fill, "quantity", 0))
            )
        except (InvalidOperation, TypeError, ValueError):
            quantity = Decimal("0")
        if (
            route_scope is None
            or not symbol
            or (order_symbol and order_symbol != symbol)
            or side not in {"buy", "sell"}
            or not quantity.is_finite()
            or quantity <= 0
        ):
            indeterminate_fill_count += 1
            continue

        route_symbol = (route_scope, symbol)
        lots = lots_by_route_symbol.setdefault(route_symbol, [])
        generation_boundary = (
            isolation_cursor.latest_boundary_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=observed_at,
            )
            if isolation_cursor is not None
            else None
        )
        if (
            route_symbol in generation_by_route_symbol
            and generation_by_route_symbol[route_symbol] != generation_boundary
        ):
            lots.clear()
            epoch_start_fill_by_route_symbol.pop(route_symbol, None)
            indeterminate_by_route_symbol[route_symbol] = 0
        generation_by_route_symbol[route_symbol] = generation_boundary
        scope_quality_available = True
        if isolation_cursor is not None:
            had_open_lots = bool(lots)
            scope_quality_available = isolation_cursor.apply_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=observed_at,
                lots=lots,
            )
            if had_open_lots and not lots:
                epoch_start_fill_by_route_symbol.pop(route_symbol, None)
        if fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=symbol,
            observed_at=observed_at,
        ):
            # Do not expose a potentially false flat/open balance.  Every fill
            # in this inventory generation is excluded until exact-route,
            # broker-authenticated flat evidence starts a later generation.
            lots.clear()
            epoch_start_fill_by_route_symbol.pop(route_symbol, None)
            indeterminate_by_route_symbol[route_symbol] = (
                indeterminate_by_route_symbol.get(route_symbol, 0) + 1
            )
            continue
        if not scope_quality_available:
            epoch_start_fill_by_route_symbol.pop(route_symbol, None)
            indeterminate_by_route_symbol[route_symbol] = (
                indeterminate_by_route_symbol.get(route_symbol, 0) + 1
            )
        if side == "buy":
            starts_open_epoch = not lots
            lots.append(quantity)
            if starts_open_epoch:
                if scope_quality_available and _fill_time_is_explicitly_exact(fill):
                    epoch_start_fill_by_route_symbol[route_symbol] = fill
                else:
                    epoch_start_fill_by_route_symbol.pop(route_symbol, None)
            continue

        remaining = quantity
        while remaining > 0 and lots:
            matched = min(remaining, lots[0])
            lots[0] -= matched
            remaining -= matched
            if lots[0] <= 0:
                lots.pop(0)
        if remaining > 0:
            indeterminate_by_route_symbol[route_symbol] = (
                indeterminate_by_route_symbol.get(route_symbol, 0) + 1
            )
        if not lots:
            epoch_start_fill_by_route_symbol.pop(route_symbol, None)

    if through is not None and isolation_cursor is not None:
        for (route_scope, symbol), lots in lots_by_route_symbol.items():
            route_symbol = (route_scope, symbol)
            final_boundary = isolation_cursor.latest_boundary_through(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=through,
            )
            if (
                final_boundary is not None
                and final_boundary != generation_by_route_symbol.get(route_symbol)
            ):
                indeterminate_by_route_symbol[route_symbol] = 0
            if not isolation_cursor.apply_through(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=through,
                lots=lots,
            ):
                epoch_start_fill_by_route_symbol.pop(route_symbol, None)
                indeterminate_by_route_symbol[route_symbol] = (
                    indeterminate_by_route_symbol.get(route_symbol, 0) + 1
                )
            if not lots:
                epoch_start_fill_by_route_symbol.pop(route_symbol, None)

    open_quantities = {
        route_symbol: sum(lots, Decimal("0"))
        for route_symbol, lots in lots_by_route_symbol.items()
        if sum(lots, Decimal("0")) > 0
    }

    return OpenLongFillInventoryReplay(
        open_quantities=open_quantities,
        open_epoch_start_fills={
            route_symbol: fill
            for route_symbol, fill in epoch_start_fill_by_route_symbol.items()
            if route_symbol in open_quantities
            and indeterminate_by_route_symbol.get(route_symbol, 0) == 0
        },
        indeterminate_fill_count=(
            indeterminate_fill_count + sum(indeterminate_by_route_symbol.values())
        ),
    )


def fifo_route_scope_from_order(order: object) -> FifoRouteScope | None:
    metadata = getattr(order, "request_metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    if "execution_scope" in metadata:
        return _canonical_execution_scope_from_order(
            order=order,
            payload=metadata.get("execution_scope"),
        )
    policy = metadata.get("broker_account_scope")
    if not isinstance(policy, dict) or policy.get("policy_version") not in {1, 2}:
        return None
    account_scope = policy.get("scope")
    provider = getattr(order, "broker_name", None) or metadata.get("provider")
    venue = getattr(order, "venue", None) or metadata.get("venue")
    asset_class = getattr(order, "asset_class", None) or metadata.get("asset_class")
    route_metadata = metadata.get("route")
    route_metadata = route_metadata if isinstance(route_metadata, dict) else {}
    exchange = metadata.get("exchange") or route_metadata.get("exchange")
    try:
        return FifoRouteScope(
            provider=str(provider or ""),
            account_scope=str(account_scope or ""),
            venue=str(venue or ""),
            asset_class=str(asset_class or ""),
            exchange=str(exchange or ""),
        )
    except ValueError:
        return None


def _canonical_execution_scope_from_order(
    *,
    order: object,
    payload: object,
) -> FifoRouteScope | None:
    if (
        not isinstance(payload, dict)
        or payload.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION
    ):
        return None
    account_scope = payload.get("account_scope")
    if (
        not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
    ):
        return None
    try:
        route_scope = FifoRouteScope(
            provider=str(payload.get("provider") or ""),
            account_scope=account_scope,
            venue=str(payload.get("venue") or ""),
            asset_class=str(payload.get("asset_class") or ""),
            exchange=str(payload.get("exchange") or ""),
        )
    except ValueError:
        return None

    metadata = getattr(order, "request_metadata", None)
    metadata = metadata if isinstance(metadata, dict) else {}
    route_metadata = metadata.get("route")
    route_metadata = route_metadata if isinstance(route_metadata, dict) else {}
    order_route = {
        "provider": str(getattr(order, "broker_name", None) or "").strip().lower(),
        "venue": str(getattr(order, "venue", None) or "").strip().upper(),
        "asset_class": str(getattr(order, "asset_class", None) or "")
        .strip()
        .lower(),
        "exchange": str(
            metadata.get("exchange") or route_metadata.get("exchange") or ""
        )
        .strip()
        .upper(),
    }
    if any(
        order_route[field_name]
        and order_route[field_name] != getattr(route_scope, field_name)
        for field_name in order_route
    ):
        return None
    return route_scope


def load_fifo_isolation_cursor(
    *,
    db: Session,
    runtime_mode: str,
    through: datetime | None = None,
) -> FifoIsolationCursor:
    where = [FifoInventoryIsolation.mode == runtime_mode]
    if through is not None:
        where.append(FifoInventoryIsolation.isolated_through_at <= through)
    rows = db.execute(
        select(
            FifoInventoryIsolation.provider,
            FifoInventoryIsolation.account_scope,
            FifoInventoryIsolation.venue,
            FifoInventoryIsolation.asset_class,
            FifoInventoryIsolation.exchange,
            FifoInventoryIsolation.symbol,
            FifoInventoryIsolation.isolated_through_at,
            FifoInventoryIsolation.broker_position_quantity,
            FifoInventoryIsolation.broker_reconciled_at,
            FifoInventoryIsolation.evidence_source,
        )
        .where(*where)
        .order_by(
            FifoInventoryIsolation.provider.asc(),
            FifoInventoryIsolation.account_scope.asc(),
            FifoInventoryIsolation.venue.asc(),
            FifoInventoryIsolation.asset_class.asc(),
            FifoInventoryIsolation.exchange.asc(),
            FifoInventoryIsolation.symbol.asc(),
            FifoInventoryIsolation.isolated_through_at.asc(),
            FifoInventoryIsolation.id.asc(),
        )
    ).all()
    boundaries: dict[tuple[FifoRouteScope, str], list[datetime]] = {}
    legacy_boundaries: dict[str, list[datetime]] = {}
    for (
        provider,
        account_scope,
        venue,
        asset_class,
        exchange,
        symbol,
        isolated_through_at,
        broker_position_quantity,
        broker_reconciled_at,
        evidence_source,
    ) in rows:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_boundary = _as_utc(isolated_through_at)
        try:
            route_scope = FifoRouteScope(
                provider=str(provider or ""),
                account_scope=str(account_scope or ""),
                venue=str(venue or ""),
                asset_class=str(asset_class or ""),
                exchange=str(exchange or ""),
            )
        except ValueError:
            legacy_boundaries.setdefault(normalized_symbol, []).append(
                normalized_boundary
            )
            continue
        verified_flat = (
            _material_decimal(broker_position_quantity) == Decimal("0")
            and str(evidence_source or "").strip()
            == FIFO_VERIFIED_FLAT_EVIDENCE_SOURCE
            and _as_utc(broker_reconciled_at) == normalized_boundary
        )
        if not verified_flat:
            # A scoped-looking row is not authority unless it preserves the
            # exact broker-flat proof emitted by the reconciliation workflow.
            # Keep it as an unresolved boundary so it can never erase lots or
            # chronology uncertainty.
            legacy_boundaries.setdefault(normalized_symbol, []).append(
                normalized_boundary
            )
            continue
        inventory_scope = canonical_fifo_inventory_route_scope(route_scope)
        boundaries.setdefault((inventory_scope, normalized_symbol), []).append(
            normalized_boundary
        )
    for route_boundaries in boundaries.values():
        route_boundaries.sort()
    return FifoIsolationCursor(
        isolated_at_by_route_symbol=boundaries,
        legacy_unscoped_at_by_symbol=legacy_boundaries,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
