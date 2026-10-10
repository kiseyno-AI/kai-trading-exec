from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload, undefer

from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionPosition,
    FifoInventoryIsolation,
    RuntimeRemediationEvent,
)
from app.services.fifo_inventory import (
    FifoRouteScope,
    ambiguous_fifo_chronology_buckets,
    fifo_chronology_is_ambiguous_at,
    fifo_fill_sort_key,
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
)
from app.services.fill_idempotency import (
    TERMINAL_FILL_EVIDENCE_STATUSES,
    broker_reported_filled_quantity,
    dedupe_execution_fills,
    duplicate_fill_conflicting_fields,
    duplicate_fill_economic_conflict,
    duplicate_fill_group_candidate_key,
    execution_fills_have_conflicting_identity,
    execution_fill_dedupe_key,
    execution_fill_idempotency_key,
)
from app.services.fill_economics import (
    COST_COVERAGE_UNKNOWN,
    FillEconomics,
    normalize_fill_economics,
)
from app.services.position_identity import (
    PositionIdentity,
    PositionIdentityConflictError,
    position_identity_from_columns,
    position_identity_from_payload,
)


REPAIR_EVENT_SOURCE = "operator_repair"
MISSING_FILL_REPAIR_EVENT = "execution_fill_missing_repair"
FIFO_INVENTORY_ISOLATION_SOURCE = "broker_flat_reconciliation"
MISSING_FILL_CANDIDATE_TYPE = "missing_execution_fill"
DUPLICATE_FILL_CANDIDATE_TYPE = "duplicate_fill_group"
FIFO_RESIDUAL_CANDIDATE_TYPE = "fifo_inventory_residual"


def analyze_fifo_inventory_repairs(
    *,
    db: Session,
    runtime_mode: str,
) -> dict[str, object]:
    items = _fifo_inventory_repair_items(db=db, runtime_mode=runtime_mode)
    return _fifo_inventory_repair_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        items=items,
        applied_count=0,
    )


def apply_fifo_inventory_repairs(
    *,
    db: Session,
    runtime_mode: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    isolated_at = now or datetime.now(UTC)
    items = _fifo_inventory_repair_items(db=db, runtime_mode=runtime_mode)
    applied_items: list[dict[str, object]] = []
    applied_blocker_count = 0
    pending_isolations: dict[
        tuple[str, str, str, str, str, str, str, datetime],
        FifoInventoryIsolation,
    ] = {}
    pending_applied_items: dict[
        tuple[str, str, str, str, str, str, str, datetime],
        dict[str, object],
    ] = {}
    for item in items:
        if item["eligible"] is not True:
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=FIFO_RESIDUAL_CANDIDATE_TYPE,
                candidate_key=_fifo_candidate_key(item),
                subject_id=str(item["broker_position_id"]),
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=isolated_at,
                evidence_summary={
                    "symbol": item["symbol"],
                    "residual_quantity": item["residual_quantity"],
                    "last_local_fill": item["last_local_fill"],
                    "broker_reconciliation_timestamp": item[
                        "broker_reconciliation_timestamp"
                    ],
                    "blocker": item["reason"],
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_blocker_count += 1
            continue
        broker_reconciled_at = _parse_timestamp(
            item["broker_reconciliation_timestamp"],
            fallback=isolated_at,
        )
        isolation_key = (
            runtime_mode,
            str(item["execution_scope"]["provider"]),
            str(item["execution_scope"]["account_scope"]),
            str(item["execution_scope"]["venue"]),
            str(item["execution_scope"]["asset_class"]),
            str(item["execution_scope"]["exchange"]),
            str(item["symbol"]),
            broker_reconciled_at,
        )
        pending = pending_isolations.get(isolation_key)
        if pending is not None:
            pending.residual_quantity += (
                _parse_decimal(item["residual_quantity"]) or Decimal("0")
            )
            pending.last_local_fill_at = max(
                pending.last_local_fill_at,
                _parse_timestamp(item["last_local_fill"], fallback=isolated_at),
            )
            audit_payload = dict(pending.audit_payload or {})
            evidence = list(
                audit_payload.get("coalesced_evidence")
                or [audit_payload["evidence"]]
            )
            evidence.append(item)
            audit_payload["coalesced_evidence"] = evidence
            audit_payload["coalesced_candidate_count"] = len(evidence)
            pending.audit_payload = audit_payload
            pending_applied_items[isolation_key]["coalesced_candidate_count"] = len(
                evidence
            )
            continue
        existing = db.scalar(
            select(FifoInventoryIsolation).where(
                FifoInventoryIsolation.mode == runtime_mode,
                FifoInventoryIsolation.provider
                == item["execution_scope"]["provider"],
                FifoInventoryIsolation.account_scope
                == item["execution_scope"]["account_scope"],
                FifoInventoryIsolation.venue
                == item["execution_scope"]["venue"],
                FifoInventoryIsolation.asset_class
                == item["execution_scope"]["asset_class"],
                FifoInventoryIsolation.exchange
                == item["execution_scope"]["exchange"],
                FifoInventoryIsolation.symbol == item["symbol"],
                FifoInventoryIsolation.broker_reconciled_at == broker_reconciled_at,
            )
        )
        if existing is not None:
            continue
        isolation = FifoInventoryIsolation(
            mode=runtime_mode,
            provider=str(item["execution_scope"]["provider"]),
            account_scope=str(item["execution_scope"]["account_scope"]),
            venue=str(item["execution_scope"]["venue"]),
            asset_class=str(item["execution_scope"]["asset_class"]),
            exchange=str(item["execution_scope"]["exchange"]),
            symbol=str(item["symbol"]),
            residual_quantity=_parse_decimal(item["residual_quantity"]) or Decimal("0"),
            last_local_fill_at=_parse_timestamp(item["last_local_fill"], fallback=isolated_at),
            broker_position_id=str(item["broker_position_id"]),
            broker_position_quantity=_parse_decimal(item["broker_position_quantity"])
            or Decimal("0"),
            broker_reconciled_at=broker_reconciled_at,
            isolated_through_at=broker_reconciled_at,
            evidence_source=FIFO_INVENTORY_ISOLATION_SOURCE,
            actor=actor,
            reason=reason,
            audit_payload={
                "action": "isolate_fifo_residual",
                "actor": actor,
                "reason": reason,
                "isolated_at": isolated_at.isoformat(),
                "runtime_mode": runtime_mode,
                "execution_scope": item["execution_scope"],
                "evidence": item,
                "fills_deleted": 0,
                "fills_fabricated": 0,
            },
        )
        db.add(isolation)
        pending_isolations[isolation_key] = isolation
        applied_item = dict(item)
        applied_item["coalesced_candidate_count"] = 1
        applied_items.append(applied_item)
        pending_applied_items[isolation_key] = applied_item
    db.commit()
    items = _annotate_fifo_reviews(
        db=db,
        runtime_mode=runtime_mode,
        items=items,
    )
    return _fifo_inventory_repair_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        items=items,
        applied_count=len(applied_items),
        applied_items=applied_items,
        applied_blocker_count=applied_blocker_count,
    )


def _fifo_inventory_repair_payload(
    *,
    mode: str,
    runtime_mode: str,
    items: list[dict[str, object]],
    applied_count: int,
    applied_items: list[dict[str, object]] | None = None,
    applied_blocker_count: int = 0,
) -> dict[str, object]:
    eligible_count = sum(1 for item in items if item["eligible"] is True)
    manual_review_count = sum(
        1 for item in items if item.get("review_outcome") == "manual_review_required"
    )
    return {
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "broker_flat_residual_count": len(items),
            "eligible_isolation_count": eligible_count,
            "recency_blocked_count": len(items) - eligible_count,
            "applied_isolation_count": applied_count,
            "manual_review_count": manual_review_count,
            "unreviewed_recency_blocked_count": (
                len(items) - eligible_count - manual_review_count
            ),
            "applied_blocker_count": applied_blocker_count,
        },
        "broker_flat_fifo_residuals": items,
        "applied_isolations": applied_items or [],
    }


def _fifo_inventory_repair_items(
    *,
    db: Session,
    runtime_mode: str,
) -> list[dict[str, object]]:
    fills = dedupe_execution_fills(list(
        db.scalars(
            select(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(BrokerOrder.mode == runtime_mode)
            .options(
                undefer(ExecutionFill.payload),
                selectinload(ExecutionFill.broker_order).load_only(
                    BrokerOrder.id,
                    BrokerOrder.broker_name,
                    BrokerOrder.request_metadata,
                    BrokerOrder.venue,
                    BrokerOrder.asset_class,
                )
            )
            .order_by(ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        ).all()
    ))
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    isolation_cursor = load_fifo_isolation_cursor(db=db, runtime_mode=runtime_mode)
    lots_by_route_symbol: dict[
        tuple[FifoRouteScope | None, str],
        list[Decimal],
    ] = {}
    last_fill_at_by_route_symbol: dict[
        tuple[FifoRouteScope | None, str],
        datetime,
    ] = {}
    ambiguous_isolation_scopes: set[tuple[FifoRouteScope, str]] = set()
    generation_by_route_symbol: dict[
        tuple[FifoRouteScope, str], datetime | None
    ] = {}
    for fill in sorted(fills, key=fifo_fill_sort_key):
        symbol = fill.symbol
        filled_at = _parse_timestamp(fill.filled_at, fallback=datetime.now(UTC))
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        route_symbol = (route_scope, symbol)
        lots = lots_by_route_symbol.setdefault(route_symbol, [])
        if route_scope is not None:
            scoped_route_symbol = (route_scope, symbol)
            generation_boundary = isolation_cursor.latest_boundary_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=filled_at,
            )
            if (
                scoped_route_symbol in generation_by_route_symbol
                and generation_by_route_symbol[scoped_route_symbol]
                != generation_boundary
            ):
                lots.clear()
                ambiguous_isolation_scopes.discard(scoped_route_symbol)
            generation_by_route_symbol[scoped_route_symbol] = generation_boundary
            scope_quality_available = isolation_cursor.apply_before(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=filled_at,
                lots=lots,
            )
        else:
            scope_quality_available = False
        if route_scope is not None and fifo_chronology_is_ambiguous_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=symbol,
            observed_at=filled_at,
        ):
            lots.clear()
            ambiguous_isolation_scopes.add((route_scope, symbol))
            last_fill_at_by_route_symbol[route_symbol] = filled_at
            continue
        if route_scope is not None and not scope_quality_available:
            ambiguous_isolation_scopes.add((route_scope, symbol))
        quantity = (
            Decimal(fill.base_quantity)
            if fill.base_quantity is not None and fill.base_quantity > 0
            else Decimal(fill.quantity)
        )
        if str(fill.side or "").lower() == "buy":
            lots.append(quantity)
        elif str(fill.side or "").lower() == "sell":
            remaining = quantity
            while remaining > 0 and lots:
                matched = min(remaining, lots[0])
                lots[0] -= matched
                remaining -= matched
                if lots[0] <= 0:
                    lots.pop(0)
        last_fill_at_by_route_symbol[route_symbol] = filled_at

    for (route_scope, symbol), lots in lots_by_route_symbol.items():
        if route_scope is not None:
            scoped_route_symbol = (route_scope, symbol)
            final_boundary = isolation_cursor.latest_boundary_through(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=datetime.max.replace(tzinfo=UTC),
            )
            if (
                final_boundary is not None
                and final_boundary
                != generation_by_route_symbol.get(scoped_route_symbol)
            ):
                ambiguous_isolation_scopes.discard(scoped_route_symbol)
            if not isolation_cursor.apply_through(
                route_scope=route_scope,
                symbol=symbol,
                observed_at=datetime.max.replace(tzinfo=UTC),
                lots=lots,
            ):
                ambiguous_isolation_scopes.add(scoped_route_symbol)

    positions = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == runtime_mode,
            ExecutionPosition.last_reconciled_at.is_not(None),
        )
    ).all()
    scoped_position_candidates: dict[
        tuple[str, str, str, str],
        list[ExecutionPosition],
    ] = defaultdict(list)
    scoped_positions_by_symbol: dict[str, list[ExecutionPosition]] = defaultdict(list)
    legacy_position_candidates: dict[str, list[ExecutionPosition]] = defaultdict(list)
    for position in positions:
        identity = _validated_position_identity(position)
        if identity is None:
            if _is_legacy_unscoped_position(position):
                legacy_position_candidates[position.symbol].append(position)
            continue
        scoped_position_candidates[
            (
                identity.provider,
                identity.account_scope,
                identity.exchange,
                position.symbol,
            )
        ].append(position)
        scoped_positions_by_symbol[position.symbol].append(position)
    position_by_route_symbol = {
        key: candidates[0]
        for key, candidates in scoped_position_candidates.items()
        if len(candidates) == 1
    }
    legacy_position_by_symbol = {
        symbol: candidates[0]
        for symbol, candidates in legacy_position_candidates.items()
        if len(candidates) == 1
    }
    items: list[dict[str, object]] = []
    for (route_scope, symbol), lots in sorted(
        lots_by_route_symbol.items(),
        key=lambda item: (
            item[0][1],
            item[0][0].provider if item[0][0] is not None else "",
            item[0][0].account_scope if item[0][0] is not None else "",
            item[0][0].venue if item[0][0] is not None else "",
            item[0][0].asset_class if item[0][0] is not None else "",
            item[0][0].exchange if item[0][0] is not None else "",
        ),
    ):
        residual_quantity = sum(lots, Decimal("0"))
        ambiguous_generation = bool(
            route_scope is not None
            and (route_scope, symbol) in ambiguous_isolation_scopes
        )
        if residual_quantity <= 0 and not ambiguous_generation:
            continue
        last_fill_at = last_fill_at_by_route_symbol.get((route_scope, symbol))
        if last_fill_at is None:
            continue
        route_position_key = (
            (
                route_scope.provider,
                route_scope.account_scope,
                route_scope.exchange,
                symbol,
            )
            if route_scope is not None
            else None
        )
        legacy_positions = legacy_position_candidates.get(symbol, [])
        exact_symbol_positions = scoped_positions_by_symbol.get(symbol, [])
        ambiguous_positions = (
            scoped_position_candidates.get(route_position_key, [])
            if route_position_key is not None
            else legacy_positions or exact_symbol_positions
        )
        if len(ambiguous_positions) > 1:
            items.append(
                {
                    "symbol": symbol,
                    "residual_quantity": _decimal_text(residual_quantity),
                    "last_local_fill": last_fill_at.isoformat(),
                    "broker_reconciliation_timestamp": None,
                    "broker_position_id": f"ambiguous-position-route:{symbol}",
                    "broker_position_ids": sorted(
                        position.id for position in ambiguous_positions
                    ),
                    "broker_position_quantity": None,
                    "execution_scope": (
                        route_scope.as_payload() if route_scope is not None else None
                    ),
                    "eligible": False,
                    "repair_action": "skip",
                    "reason": (
                        "broker_position_route_ambiguous"
                        if route_scope is not None
                        else "legacy_position_route_ambiguous"
                    ),
                }
            )
            continue
        position = (
            position_by_route_symbol.get(route_position_key)
            if route_position_key is not None
            else (
                legacy_position_by_symbol.get(symbol)
                or (
                    exact_symbol_positions[0]
                    if len(exact_symbol_positions) == 1
                    else None
                )
            )
        )
        if (
            position is None
            or "reconciliation" not in str(position.updated_from or "")
            or _position_quantity(position) > 0
            or position.last_reconciled_at is None
        ):
            continue
        broker_reconciled_at = _parse_timestamp(
            position.last_reconciled_at,
            fallback=datetime.min.replace(tzinfo=UTC),
        )
        effective_route_scope = route_scope or _exact_krx_legacy_reset_scope(position)
        existing_boundary = (
            isolation_cursor.latest_boundary_through(
                route_scope=effective_route_scope,
                symbol=symbol,
                observed_at=broker_reconciled_at,
            )
            if effective_route_scope is not None
            else None
        )
        if existing_boundary is not None and existing_boundary >= last_fill_at:
            continue
        scope_matches_position = _fifo_position_matches_route(
            position=position,
            route_scope=effective_route_scope,
        )
        scope_available = bool(
            effective_route_scope is not None and scope_matches_position
        )
        eligible = scope_available and broker_reconciled_at > last_fill_at
        if route_scope is None and effective_route_scope is not None and eligible:
            reason = "exact_krx_flat_resets_legacy_generation"
        elif route_scope is None:
            reason = "execution_scope_unavailable"
        elif not scope_matches_position:
            reason = "broker_position_execution_scope_mismatch"
        elif eligible and ambiguous_generation:
            reason = "broker_flat_evidence_resets_ambiguous_generation"
        elif eligible:
            reason = "broker_flat_evidence_newer_than_last_local_fill"
        else:
            reason = "broker_flat_evidence_not_newer_than_last_local_fill"
        items.append(
            {
                "symbol": symbol,
                "residual_quantity": _decimal_text(residual_quantity),
                "last_local_fill": last_fill_at.isoformat(),
                "broker_reconciliation_timestamp": broker_reconciled_at.isoformat(),
                "broker_position_id": position.id,
                "broker_position_quantity": _decimal_text(_position_quantity(position)),
                "execution_scope": (
                    effective_route_scope.as_payload()
                    if effective_route_scope is not None
                    else None
                ),
                "eligible": eligible,
                "repair_action": "isolate" if eligible else "skip",
                "reason": reason,
                **(
                    {"fifo_history_quality": "ambiguous_before_flat_boundary"}
                    if ambiguous_generation
                    else {}
                ),
            }
        )
    return _annotate_fifo_reviews(db=db, runtime_mode=runtime_mode, items=items)


def _fifo_candidate_key(item: dict[str, object]) -> str:
    scope = item.get("execution_scope")
    scope = scope if isinstance(scope, dict) else {}
    return "|".join(
        (
            str(scope.get("provider") or "unscoped"),
            str(scope.get("account_scope") or "unscoped"),
            str(scope.get("venue") or "unscoped"),
            str(scope.get("asset_class") or "unscoped"),
            str(scope.get("exchange") or "unscoped"),
            str(item["symbol"]),
            str(item["last_local_fill"]),
            str(item["broker_reconciliation_timestamp"]),
        )
    )


def _fifo_position_matches_route(
    *,
    position: ExecutionPosition,
    route_scope: FifoRouteScope | None,
) -> bool:
    if route_scope is None:
        return False
    identity = _validated_position_identity(position)
    return bool(
        identity is not None
        and identity.provider == route_scope.provider
        and identity.account_scope == route_scope.account_scope
        and identity.exchange == route_scope.exchange
    )


def _exact_krx_legacy_reset_scope(
    position: ExecutionPosition,
) -> FifoRouteScope | None:
    """Start a clean scoped generation without attributing legacy fills.

    A unique, current KIS/KRX position row can prove only that this exact
    account is flat now. That is sufficient to isolate older unscoped history
    from future FIFO replay, but not to rewrite or value the historical fills.
    Other routes remain fail-closed because their venue and asset mapping is not
    established by this remediation path.
    """

    identity = _validated_position_identity(position)
    if (
        identity is None
        or identity.provider != "kis"
        or identity.exchange != "KRX"
    ):
        return None
    return FifoRouteScope(
        provider=identity.provider,
        account_scope=identity.account_scope,
        venue="KRX",
        asset_class="stock",
        exchange=identity.exchange,
    )


def _validated_position_identity(
    position: ExecutionPosition,
) -> PositionIdentity | None:
    try:
        column_identity = position_identity_from_columns(position)
        payload_identity = position_identity_from_payload(position.raw_payload)
    except PositionIdentityConflictError:
        return None
    if column_identity is None:
        return None
    if payload_identity is not None and payload_identity != column_identity:
        return None
    return column_identity


def _is_legacy_unscoped_position(position: ExecutionPosition) -> bool:
    if any(
        value is not None
        for value in (position.provider, position.account_scope, position.exchange)
    ):
        return False
    try:
        return position_identity_from_payload(position.raw_payload) is None
    except PositionIdentityConflictError:
        return False


def _annotate_fifo_reviews(
    *,
    db: Session,
    runtime_mode: str,
    items: list[dict[str, object]],
) -> list[dict[str, object]]:
    candidate_keys = {_fifo_candidate_key(item) for item in items}
    if not candidate_keys:
        return items
    reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == FIFO_RESIDUAL_CANDIDATE_TYPE,
            RuntimeRemediationEvent.candidate_key.in_(sorted(candidate_keys)),
        )
    ).all()
    review_by_key = {review.candidate_key: review for review in reviews}
    for item in items:
        review = review_by_key.get(_fifo_candidate_key(item))
        if review is not None:
            item["review_outcome"] = review.outcome
            item["review_actor"] = review.actor
            item["review_reason"] = review.reason
            item["reviewed_at"] = _serialize_datetime(review.occurred_at)
    return items


def _position_quantity(position: ExecutionPosition) -> Decimal:
    if position.base_quantity is not None:
        return Decimal(position.base_quantity)
    return Decimal(position.quantity)


def analyze_execution_fill_repairs(
    *,
    db: Session,
    runtime_mode: str,
) -> dict[str, object]:
    missing = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=MISSING_FILL_CANDIDATE_TYPE,
        items=_missing_fill_items(db=db, runtime_mode=runtime_mode),
    )
    duplicates = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
        items=_duplicate_fill_groups(db=db, runtime_mode=runtime_mode),
    )
    return _repair_payload(
        mode="dry-run",
        runtime_mode=runtime_mode,
        missing=missing,
        duplicates=duplicates,
        inserted_fill_count=0,
        deleted_fill_count=0,
        audit_event_count=0,
        applied_manual_review_count=0,
        applied_duplicate_isolation_count=0,
    )


def apply_execution_fill_repairs(
    *,
    db: Session,
    runtime_mode: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    repaired_at = now or datetime.now(UTC)
    inserted_fill_count = 0
    audit_event_count = 0
    applied_manual_review_count = 0
    applied_duplicate_isolation_count = 0

    missing = _missing_fill_items(db=db, runtime_mode=runtime_mode)
    existing_keys = {
        execution_fill_dedupe_key(fill)
        for fill in db.scalars(select(ExecutionFill)).all()
    }
    for item in missing:
        order = db.get(BrokerOrder, item["broker_order_pk"])
        if order is None:
            continue
        current_issue = _terminal_fill_evidence_issue(order)
        if current_issue is None:
            continue
        proposed_fills = item.get("proposed_fills")
        if not isinstance(proposed_fills, list) or not proposed_fills:
            blocker = str(
                current_issue.get("candidate_reason")
                or "authoritative_broker_fill_evidence_unavailable"
            )
            if blocker == "missing_execution_fill":
                blocker = "authoritative_broker_fill_evidence_unavailable"
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=MISSING_FILL_CANDIDATE_TYPE,
                candidate_key=str(order.id),
                subject_id=order.id,
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=repaired_at,
                evidence_summary={
                    "symbol": order.symbol,
                    "side": order.side,
                    "broker_order_id": order.broker_order_id,
                    "idempotency_key": order.idempotency_key,
                    "blocker": blocker,
                    "quantity_basis": current_issue.get("quantity_basis"),
                    "expected_quantity": current_issue.get("expected_quantity"),
                    "persisted_quantity": current_issue.get("persisted_quantity"),
                    "persisted_fill_count": current_issue.get(
                        "persisted_fill_count"
                    ),
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_manual_review_count += 1
                audit_event_count += 1
            continue
        validated_fills, validation_blocker = _validated_repair_fill_proposals(
            order=order,
            proposed_fills=proposed_fills,
            existing_keys=existing_keys,
        )
        if validation_blocker is not None:
            if _record_remediation_event(
                db=db,
                runtime_mode=runtime_mode,
                candidate_type=MISSING_FILL_CANDIDATE_TYPE,
                candidate_key=str(order.id),
                subject_id=order.id,
                outcome="manual_review_required",
                actor=actor,
                reason=reason,
                occurred_at=repaired_at,
                evidence_summary={
                    "symbol": order.symbol,
                    "side": order.side,
                    "broker_order_id": order.broker_order_id,
                    "idempotency_key": order.idempotency_key,
                    "blocker": validation_blocker["code"],
                    "validation": validation_blocker,
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                },
            ):
                applied_manual_review_count += 1
                audit_event_count += 1
            continue
        inserted_for_order: list[dict[str, object]] = []
        for proposed, signature in validated_fills:
            economics = _proposed_fill_economics(proposed, order=order)
            filled_at = _parse_authoritative_fill_timestamp(proposed.get("filled_at"))
            if filled_at is None:
                raise ValueError(
                    "validated repair fill is missing broker-authoritative chronology"
                )
            fill = ExecutionFill(
                idempotency_key=signature,
                broker_order_pk=order.id,
                broker_fill_id=_optional_str(proposed.get("broker_fill_id")),
                symbol=str(proposed["symbol"]),
                side=str(proposed["side"]),
                quantity=_parse_int(proposed.get("quantity")) or 0,
                base_quantity=_parse_decimal(proposed.get("base_quantity")),
                quote_notional=_parse_decimal(proposed.get("quote_notional")),
                price=_parse_decimal(proposed["price"]) or Decimal("0"),
                settlement_currency=economics.settlement_currency,
                transaction_cost=economics.transaction_cost,
                transaction_cost_coverage=economics.transaction_cost_coverage,
                transaction_cost_source=economics.transaction_cost_source,
                filled_at=filled_at,
                payload={
                    "source": "operator_fill_evidence_repair",
                    "actor": actor,
                    "reason": reason,
                    "fill_time_precision": "exact",
                    "fill_time_source": "broker_payload",
                    "settlement_currency_conflict": (
                        economics.settlement_currency_conflict
                    ),
                    "repair_evidence": item.get("available_repair_evidence"),
                    "proposed_fill": _json_safe_value(proposed),
                },
            )
            db.add(fill)
            existing_keys.add(signature)
            inserted_fill_count += 1
            inserted_for_order.append(_fill_row_payload(fill, id_override=None))
        if inserted_for_order:
            db.add(
                BrokerOrderEvent(
                    broker_order_pk=order.id,
                    event_type=MISSING_FILL_REPAIR_EVENT,
                    event_source=REPAIR_EVENT_SOURCE,
                    normalized_status=order.status,
                    broker_status=None,
                    message="operator repaired missing execution fill evidence",
                    payload={
                        "actor": actor,
                        "reason": reason,
                        "repaired_at": repaired_at.isoformat(),
                        "runtime_mode": runtime_mode,
                        "broker_order_id": order.broker_order_id,
                        "idempotency_key": order.idempotency_key,
                        "inserted_fills": inserted_for_order,
                        "dry_run_item": item,
                    },
                    observed_at=repaired_at,
                )
            )
            audit_event_count += 1

    db.flush()
    duplicates = _duplicate_fill_groups(db=db, runtime_mode=runtime_mode)
    for group in duplicates:
        retained = group.get("retained_fill")
        duplicate_fills = group.get("duplicate_fills")
        if (
            not isinstance(retained, dict)
            or not isinstance(duplicate_fills, list)
            or not duplicate_fills
        ):
            continue
        conflict = group.get("economic_conflict") is True
        outcome = "manual_review_required" if conflict else "non_destructive_isolation"
        evidence_summary = {
            "symbol": group.get("symbol"),
            "side": group.get("side"),
            "broker_order_id": group.get("broker_order_id"),
            "retained_fill_id": retained.get("id"),
            "duplicate_fill_ids": [
                row.get("id") for row in duplicate_fills if isinstance(row, dict)
            ],
            "duplicate_group_key": group.get("duplicate_group_key"),
            "group_membership_version": _duplicate_group_membership_version(group),
            "fills_deleted": 0,
            "fills_fabricated": 0,
            "orders_submitted": 0,
        }
        if conflict:
            evidence_summary["blocker"] = "duplicate_fill_economic_conflict"
            evidence_summary["conflicting_fields"] = group.get("conflicting_fields")
            evidence_summary["dedupe_behavior"] = (
                "application_readers_preserve_conflicting_duplicate_identity_rows"
            )
        else:
            evidence_summary["dedupe_behavior"] = (
                "application_readers_ignore_duplicate_identity"
            )
        if _record_remediation_event(
            db=db,
            runtime_mode=runtime_mode,
            candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
            candidate_key=_duplicate_group_candidate_key(group),
            subject_id=str(group["broker_order_pk"]),
            outcome=outcome,
            actor=actor,
            reason=reason,
            occurred_at=repaired_at,
            evidence_summary=evidence_summary,
        ):
            if conflict:
                applied_manual_review_count += 1
            else:
                applied_duplicate_isolation_count += 1
            audit_event_count += 1

    db.commit()
    remaining_missing = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=MISSING_FILL_CANDIDATE_TYPE,
        items=_missing_fill_items(db=db, runtime_mode=runtime_mode),
    )
    remaining_duplicates = _annotate_remediation_reviews(
        db=db,
        runtime_mode=runtime_mode,
        candidate_type=DUPLICATE_FILL_CANDIDATE_TYPE,
        items=_duplicate_fill_groups(db=db, runtime_mode=runtime_mode),
    )
    return _repair_payload(
        mode="apply",
        runtime_mode=runtime_mode,
        missing=remaining_missing,
        duplicates=remaining_duplicates,
        inserted_fill_count=inserted_fill_count,
        deleted_fill_count=0,
        audit_event_count=audit_event_count,
        applied_manual_review_count=applied_manual_review_count,
        applied_duplicate_isolation_count=applied_duplicate_isolation_count,
    )


def _repair_payload(
    *,
    mode: str,
    runtime_mode: str,
    missing: list[dict[str, object]],
    duplicates: list[dict[str, object]],
    inserted_fill_count: int,
    deleted_fill_count: int,
    audit_event_count: int,
    applied_manual_review_count: int,
    applied_duplicate_isolation_count: int,
) -> dict[str, object]:
    proposed_missing = sum(
        len(item.get("proposed_fills") or [])
        for item in missing
        if isinstance(item.get("proposed_fills"), list)
    )
    manual_review_count = sum(
        1 for item in missing if item.get("review_outcome") == "manual_review_required"
    )
    isolated_duplicate_count = sum(
        1
        for group in duplicates
        if group.get("review_outcome") == "non_destructive_isolation"
    )
    manual_review_duplicate_count = sum(
        1
        for group in duplicates
        if group.get("review_outcome") == "manual_review_required"
    )
    return {
        "mode": mode,
        "runtime_mode": runtime_mode,
        "summary": {
            "missing_filled_order_count": len(missing),
            "proposed_missing_fill_insert_count": proposed_missing,
            "duplicate_fill_group_count": len(duplicates),
            "proposed_manual_review_count": sum(
                1
                for item in missing
                if not item.get("proposed_fills") and not item.get("review_outcome")
            ),
            "proposed_duplicate_fill_isolation_count": sum(
                1
                for group in duplicates
                if group.get("repair_action") == "isolate_non_destructively"
                and not group.get("review_outcome")
            ),
            "proposed_duplicate_fill_manual_review_count": sum(
                1
                for group in duplicates
                if group.get("repair_action") == "record_manual_review"
                and not group.get("review_outcome")
            ),
            "manual_review_missing_fill_count": manual_review_count,
            "unreviewed_missing_fill_count": len(missing) - manual_review_count,
            "isolated_duplicate_fill_group_count": isolated_duplicate_count,
            "manual_review_duplicate_fill_group_count": manual_review_duplicate_count,
            "unreviewed_duplicate_fill_group_count": (
                len(duplicates) - isolated_duplicate_count - manual_review_duplicate_count
            ),
            "inserted_fill_count": inserted_fill_count,
            "deleted_fill_count": deleted_fill_count,
            "audit_event_count": audit_event_count,
            "applied_manual_review_count": applied_manual_review_count,
            "applied_duplicate_isolation_count": applied_duplicate_isolation_count,
        },
        "missing_filled_orders": missing,
        "duplicate_fill_groups": duplicates,
    }


def _missing_fill_items(*, db: Session, runtime_mode: str) -> list[dict[str, object]]:
    rows = db.scalars(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == runtime_mode,
            BrokerOrder.status.in_(TERMINAL_FILL_EVIDENCE_STATUSES),
        )
        .options(
            undefer(BrokerOrder.response_metadata),
            undefer(BrokerOrder.raw_broker_payload),
            selectinload(BrokerOrder.fills).undefer(ExecutionFill.payload),
            selectinload(BrokerOrder.events).undefer(
                BrokerOrderEvent.response_payload
            ),
        )
        .order_by(BrokerOrder.submitted_at.asc(), BrokerOrder.created_at.asc())
    ).all()
    return [
        _missing_fill_item(db=db, order=order, issue=issue)
        for order in rows
        if (issue := _terminal_fill_evidence_issue(order)) is not None
    ]


def _missing_fill_item(
    *,
    db: Session,
    order: BrokerOrder,
    issue: dict[str, object],
) -> dict[str, object]:
    evidence = _repair_evidence_for_order(db=db, order=order)
    proposed_fills = evidence.get("proposed_fills")
    automatic_repair_safe = issue["candidate_reason"] == "missing_execution_fill"
    return {
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "symbol": order.symbol,
        "side": order.side,
        "quantity": order.quantity,
        "submitted_at": _serialize_datetime(order.submitted_at),
        **issue,
        "available_repair_evidence": evidence.get("available_repair_evidence"),
        "proposed_fills": (
            proposed_fills
            if automatic_repair_safe and isinstance(proposed_fills, list)
            else []
        ),
    }


def _terminal_fill_evidence_issue(order: BrokerOrder) -> dict[str, object] | None:
    quantity_contract = _terminal_order_fill_quantity_contract(order)
    if order.status != "filled" and quantity_contract is None:
        return None
    persisted_fills = dedupe_execution_fills(list(order.fills))
    if not order.fills:
        quantity_basis = quantity_contract[0] if quantity_contract else None
        expected_quantity = quantity_contract[1] if quantity_contract else None
        return {
            "candidate_reason": "missing_execution_fill",
            "quantity_basis": quantity_basis,
            "expected_quantity": (
                _quantity_evidence_value(
                    expected_quantity,
                    quantity_basis=quantity_basis,
                )
                if expected_quantity is not None and quantity_basis is not None
                else None
            ),
            "persisted_quantity": (
                _quantity_evidence_value(
                    Decimal("0"),
                    quantity_basis=quantity_basis,
                )
                if quantity_basis is not None
                else None
            ),
            "persisted_fill_count": 0,
        }
    if execution_fills_have_conflicting_identity(list(order.fills)):
        # The duplicate-fill candidate retains all conflicting evidence and
        # records one manual-review decision for this defect.
        return None
    if quantity_contract is None:
        return {
            "candidate_reason": "terminal_fill_quantity_mismatch",
            "quantity_basis": None,
            "expected_quantity": None,
            "persisted_quantity": None,
            "persisted_fill_count": len(order.fills),
        }

    quantity_basis, expected_quantity = quantity_contract
    persisted_quantities = [
        _fill_quantity_for_basis(
            _fill_row_payload(fill),
            quantity_basis=quantity_basis,
        )
        for fill in persisted_fills
    ]
    complete_quantity_evidence = bool(persisted_fills) and all(
        quantity is not None and quantity > 0
        for quantity in persisted_quantities
    )
    persisted_quantity = sum(
        (
            quantity
            for quantity in persisted_quantities
            if quantity is not None and quantity > 0
        ),
        Decimal("0"),
    )
    if complete_quantity_evidence and persisted_quantity == expected_quantity:
        return None
    return {
        "candidate_reason": "terminal_fill_quantity_mismatch",
        "quantity_basis": quantity_basis,
        "expected_quantity": _quantity_evidence_value(
            expected_quantity,
            quantity_basis=quantity_basis,
        ),
        "persisted_quantity": _quantity_evidence_value(
            persisted_quantity,
            quantity_basis=quantity_basis,
        ),
        "persisted_fill_count": len(order.fills),
    }


def _repair_evidence_for_order(*, db: Session, order: BrokerOrder) -> dict[str, object]:
    candidates: list[tuple[str, dict[str, object], datetime | None, str | None]] = []
    if isinstance(order.raw_broker_payload, dict):
        candidates.append(("broker_orders.raw_broker_payload", order.raw_broker_payload, order.last_broker_sync_at, None))
    events = db.scalars(
        select(BrokerOrderEvent)
        .where(BrokerOrderEvent.broker_order_pk == order.id)
        .order_by(BrokerOrderEvent.observed_at.desc(), BrokerOrderEvent.created_at.desc())
    ).all()
    for event in events:
        if str(event.event_source or "").lower() not in {"reconciliation", "realtime"}:
            continue
        if isinstance(event.payload, dict):
            candidates.append(("broker_order_events.payload", event.payload, event.observed_at, event.id))
            normalized_payload = event.payload.get("normalized_payload")
            if isinstance(normalized_payload, dict):
                candidates.append(("broker_order_events.payload.normalized_payload", normalized_payload, event.observed_at, event.id))
        if isinstance(event.response_payload, dict):
            candidates.append(("broker_order_events.response_payload", event.response_payload, event.observed_at, event.id))

    for source, payload, observed_at, event_id in candidates:
        fills = _proposed_fills_from_payload(
            payload=payload,
            order=order,
        )
        if fills and _fills_match_filled_order(order=order, fills=fills):
            return {
                "available_repair_evidence": {
                    "source": source,
                    "evidence_kind": "persisted_broker_payload",
                    "broker_order_event_id": event_id,
                    "observed_at": _serialize_datetime(observed_at),
                    "fields": sorted(_evidence_fields(payload)),
                    "validation": "exact_quantity_symbol_and_side_match",
                },
                "proposed_fills": fills,
            }
    return {
        "available_repair_evidence": None,
        "proposed_fills": [],
    }


def _fills_match_filled_order(
    *,
    order: BrokerOrder,
    fills: list[dict[str, object]],
) -> bool:
    quantity_contract = _terminal_order_fill_quantity_contract(order)
    if quantity_contract is None:
        return False
    quantity_basis, expected_quantity = quantity_contract
    total_quantity = Decimal("0")
    for fill in fills:
        if str(fill.get("symbol") or "").strip().upper() != order.symbol.strip().upper():
            return False
        if str(fill.get("side") or "").strip().lower() != order.side.strip().lower():
            return False
        quantity = _fill_quantity_for_basis(fill, quantity_basis=quantity_basis)
        price = _parse_decimal(fill.get("price"))
        if quantity is None or quantity <= 0 or price is None or price <= 0:
            return False
        total_quantity += quantity
    return total_quantity == expected_quantity


def _order_fill_quantity_contract(order: BrokerOrder) -> tuple[str, Decimal] | None:
    base_quantity = _parse_decimal(order.base_quantity)
    if base_quantity is not None and base_quantity > 0:
        return "base_quantity", base_quantity
    quantity = _parse_int(order.quantity)
    if quantity is not None and quantity > 0:
        return "quantity", Decimal(quantity)
    return None


def _terminal_order_fill_quantity_contract(
    order: BrokerOrder,
) -> tuple[str, Decimal] | None:
    if order.status == "filled":
        return _order_fill_quantity_contract(order)
    reported_quantity = broker_reported_filled_quantity(order)
    if reported_quantity is None:
        return None
    return "quantity", reported_quantity


def _fill_quantity_for_basis(
    fill: dict[str, object],
    *,
    quantity_basis: str,
) -> Decimal | None:
    if quantity_basis == "base_quantity":
        return _parse_decimal(fill.get("base_quantity"))
    quantity = _parse_int(fill.get("quantity"))
    return Decimal(quantity) if quantity is not None else None


def _quantity_evidence_value(value: Decimal, *, quantity_basis: str) -> int | str:
    if quantity_basis == "quantity":
        return int(value)
    return _decimal_evidence_text(value) or "0"


def _proposed_fills_from_payload(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> list[dict[str, object]]:
    fills = payload.get("fills")
    if isinstance(fills, list):
        inherited_cost_scope = _transaction_cost_scope(payload)
        parsed = [
            parsed_fill
            for item in fills
            if isinstance(item, dict)
            for parsed_fill in [
                _normalize_explicit_fill(
                    item,
                    order=order,
                    inherited_transaction_cost_scope=inherited_cost_scope,
                )
            ]
            if parsed_fill is not None
        ]
        if parsed:
            return parsed

    nested_order = payload.get("order")
    if isinstance(nested_order, dict):
        nested = _proposed_fills_from_payload(payload=nested_order, order=order)
        if nested:
            return nested

    rows = payload.get("output1")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            if _optional_str(row.get("odno")) not in {None, order.broker_order_id}:
                continue
            parsed = _cumulative_fill_from_payload(payload=row, order=order)
            if parsed:
                return parsed

    return _cumulative_fill_from_payload(payload=payload, order=order)


def _normalize_explicit_fill(
    fill: dict[str, object],
    *,
    order: BrokerOrder,
    inherited_transaction_cost_scope: str = "",
) -> dict[str, object] | None:
    quantity = _parse_int(fill.get("quantity")) or 0
    base_quantity = _first_positive_decimal(
        fill,
        "base_quantity",
        "filled_base_quantity",
    )
    quote_notional = _optional_positive_decimal(fill.get("quote_notional"))
    price = _parse_decimal(fill.get("price"))
    if (
        (quantity <= 0 and base_quantity is None)
        or price is None
        or price <= 0
    ):
        return None
    filled_at = _parse_authoritative_fill_timestamp(fill.get("filled_at"))
    if filled_at is None:
        return None
    economics = normalize_fill_economics(fill, order=order)
    cost_scope = _transaction_cost_scope(fill) or inherited_transaction_cost_scope
    if economics.transaction_cost is not None and cost_scope == "cumulative":
        economics = _unknown_transaction_cost(
            economics,
            source="broker_payload:ambiguous_explicit_fill_cumulative_cost_scope",
        )
    normalized: dict[str, object] = {
        "broker_fill_id": _optional_str(fill.get("broker_fill_id")),
        "symbol": _optional_str(fill.get("symbol")) or order.symbol,
        "side": _optional_str(fill.get("side")) or order.side,
        "quantity": quantity,
        "price": _decimal_text(price),
        "filled_at": filled_at.isoformat(),
    }
    if base_quantity is not None:
        normalized["base_quantity"] = _decimal_evidence_text(base_quantity)
    if quote_notional is not None:
        normalized["quote_notional"] = _decimal_evidence_text(quote_notional)
    if _has_explicit_economics_evidence(fill):
        normalized.update(_fill_economics_payload(economics))
    if cost_scope:
        normalized["transaction_cost_scope"] = cost_scope
    return normalized


def _cumulative_fill_from_payload(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> list[dict[str, object]]:
    quantity = _parse_int(payload.get("filled_quantity"))
    if quantity is None:
        quantity = _parse_int(payload.get("tot_ccld_qty"))
    quantity = quantity or 0
    base_quantity = _first_positive_decimal(
        payload,
        "filled_base_quantity",
        "base_quantity",
    )
    quote_notional = _optional_positive_decimal(payload.get("quote_notional"))
    if quantity <= 0 and base_quantity is None:
        return []
    price = (
        _parse_decimal(payload.get("average_fill_price"))
        or _parse_decimal(payload.get("avg_upr"))
        or _parse_decimal(payload.get("trd_prc"))
        or _parse_decimal(payload.get("avg_pric"))
        or _parse_decimal(payload.get("avg_prvs"))
        or _parse_decimal(payload.get("ord_unpr"))
    )
    if price is None or price <= 0:
        return []
    if _optional_str(payload.get("fill_time_precision")) == "observed_upper_bound":
        return []
    filled_at = _parse_authoritative_fill_timestamp(payload.get("filled_at"))
    if filled_at is None:
        return []
    economics = _cumulative_repair_economics(payload=payload, order=order)
    normalized: dict[str, object] = {
        "broker_fill_id": _optional_str(payload.get("broker_fill_id")),
        "symbol": (
            _optional_str(payload.get("symbol"))
            or _optional_str(payload.get("pdno"))
            or order.symbol
        ),
        "side": _optional_str(payload.get("side")) or order.side,
        "quantity": quantity,
        "price": _decimal_text(price),
        "filled_at": filled_at.isoformat(),
    }
    if base_quantity is not None:
        normalized["base_quantity"] = _decimal_evidence_text(base_quantity)
    if quote_notional is not None:
        normalized["quote_notional"] = _decimal_evidence_text(quote_notional)
    if _has_explicit_economics_evidence(payload):
        normalized.update(_fill_economics_payload(economics))
    cost_scope = _transaction_cost_scope(payload)
    if cost_scope:
        normalized["transaction_cost_scope"] = cost_scope
    return [normalized]


def _cumulative_repair_economics(
    *,
    payload: dict[str, object],
    order: BrokerOrder,
) -> FillEconomics:
    economics = normalize_fill_economics(payload, order=order)
    if economics.transaction_cost is None:
        return economics
    if _transaction_cost_scope(payload) == "cumulative":
        # A missing-fill repair creates one synthetic row for the full
        # cumulative quantity, so an explicitly cumulative cost belongs wholly
        # on that row. Preserve the broker's declared coverage as-is.
        return economics
    return _unknown_transaction_cost(
        economics,
        source="broker_payload:ambiguous_reconstructed_cumulative_cost_scope",
    )


def _proposed_fill_economics(
    fill: dict[str, object],
    *,
    order: BrokerOrder,
) -> FillEconomics:
    economics = normalize_fill_economics(fill, order=order)
    if fill.get("settlement_currency_conflict") is not True:
        return economics
    return FillEconomics(
        settlement_currency=None,
        transaction_cost=economics.transaction_cost,
        transaction_cost_coverage=economics.transaction_cost_coverage,
        transaction_cost_source=economics.transaction_cost_source,
        settlement_currency_conflict=True,
    )


def _unknown_transaction_cost(
    economics: FillEconomics,
    *,
    source: str,
) -> FillEconomics:
    return FillEconomics(
        settlement_currency=economics.settlement_currency,
        transaction_cost=None,
        transaction_cost_coverage=COST_COVERAGE_UNKNOWN,
        transaction_cost_source=source,
        settlement_currency_conflict=economics.settlement_currency_conflict,
    )


def _fill_economics_payload(economics: FillEconomics) -> dict[str, object]:
    return {
        "settlement_currency": economics.settlement_currency,
        "transaction_cost": _decimal_evidence_text(economics.transaction_cost),
        "transaction_cost_coverage": economics.transaction_cost_coverage,
        "transaction_cost_source": economics.transaction_cost_source,
        "settlement_currency_conflict": economics.settlement_currency_conflict,
    }


def _transaction_cost_scope(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    economics = payload.get("economics")
    if isinstance(economics, dict) and "transaction_cost_scope" in economics:
        raw_scope = economics.get("transaction_cost_scope")
    else:
        raw_scope = payload.get("transaction_cost_scope")
    return str(raw_scope or "").strip().lower()


def _has_explicit_economics_evidence(payload: dict[str, object]) -> bool:
    economics = payload.get("economics")
    candidates = {str(key) for key in payload}
    if isinstance(economics, dict):
        candidates.update(str(key) for key in economics)
    return bool(
        candidates.intersection(
            {
                "settlement_currency",
                "quote_currency",
                "currency",
                "total_transaction_cost",
                "transaction_cost",
                "commission",
                "commission_amount",
                "fee",
                "fee_amount",
                "tax",
                "tax_amount",
                "transaction_cost_coverage",
                "cost_coverage",
                "transaction_cost_source",
                "cost_source",
            }
        )
    )


def _duplicate_fill_groups(*, db: Session, runtime_mode: str) -> list[dict[str, object]]:
    fills = db.scalars(
        select(ExecutionFill)
        .join(ExecutionFill.broker_order)
        .where(BrokerOrder.mode == runtime_mode)
        .order_by(ExecutionFill.broker_order_pk.asc(), ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
    ).all()
    groups: dict[str, list[ExecutionFill]] = defaultdict(list)
    for fill in fills:
        groups[execution_fill_dedupe_key(fill)].append(fill)
    duplicate_groups: list[dict[str, object]] = []
    for key, group in groups.items():
        if len(group) <= 1:
            continue
        retained = group[0]
        duplicate_fills = group[1:]
        economic_conflict = duplicate_fill_economic_conflict(group)
        conflicting_fields = duplicate_fill_conflicting_fields(group)
        duplicate_groups.append(
            {
                "duplicate_group_key": key,
                "broker_order_pk": retained.broker_order_pk,
                "broker_order_id": retained.broker_order.broker_order_id if retained.broker_order is not None else None,
                "symbol": retained.symbol,
                "side": retained.side,
                "quantity": retained.quantity,
                "price": _decimal_text(Decimal(retained.price)),
                "filled_at": _serialize_datetime(retained.filled_at),
                "base_quantity": (
                    _decimal_text(Decimal(retained.base_quantity))
                    if retained.base_quantity is not None
                    else None
                ),
                "economic_conflict": economic_conflict,
                "conflicting_fields": conflicting_fields,
                "retained_fill": _fill_row_payload(retained),
                "duplicate_fills": [_fill_row_payload(fill) for fill in duplicate_fills],
            }
        )
    return duplicate_groups


def _fill_signature(*, order: BrokerOrder, fill: dict[str, object]) -> str | None:
    quantity = _parse_int(fill.get("quantity")) or 0
    base_quantity = _optional_positive_decimal(fill.get("base_quantity"))
    quote_notional = _optional_positive_decimal(fill.get("quote_notional"))
    price = _parse_decimal(fill.get("price"))
    symbol = _optional_str(fill.get("symbol"))
    side = _optional_str(fill.get("side"))
    filled_at = _parse_authoritative_fill_timestamp(fill.get("filled_at"))
    if (
        (quantity <= 0 and base_quantity is None)
        or price is None
        or price <= 0
        or not symbol
        or not side
        or filled_at is None
    ):
        return None
    return execution_fill_idempotency_key(
        broker_order_pk=order.id,
        broker_fill_id=_optional_str(fill.get("broker_fill_id")),
        symbol=symbol,
        side=side,
        filled_at=filled_at,
        price=price,
        quantity=quantity,
        base_quantity=base_quantity,
        quote_notional=quote_notional,
    )


def _validated_repair_fill_proposals(
    *,
    order: BrokerOrder,
    proposed_fills: list[object],
    existing_keys: set[str],
) -> tuple[list[tuple[dict[str, object], str]], dict[str, object] | None]:
    """Preflight every fill so one order is repaired completely or not at all."""
    quantity_contract = _order_fill_quantity_contract(order)
    if quantity_contract is None:
        return [], {"code": "missing_order_quantity_contract"}
    quantity_basis, expected_quantity = quantity_contract
    validated: list[tuple[dict[str, object], str]] = []
    signatures: list[str] = []
    proposed_quantity = Decimal("0")
    for index, proposed in enumerate(proposed_fills):
        if not isinstance(proposed, dict):
            return [], {
                "code": "invalid_proposed_fill",
                "invalid_fill_index": index,
            }
        signature = _fill_signature(order=order, fill=proposed)
        quantity = _fill_quantity_for_basis(
            proposed,
            quantity_basis=quantity_basis,
        )
        if signature is None or quantity is None or quantity <= 0:
            return [], {
                "code": "invalid_proposed_fill",
                "invalid_fill_index": index,
            }
        validated.append((proposed, signature))
        signatures.append(signature)
        proposed_quantity += quantity

    signature_counts: dict[str, int] = defaultdict(int)
    for signature in signatures:
        signature_counts[signature] += 1
    duplicate_signatures = sorted(
        signature for signature, count in signature_counts.items() if count > 1
    )
    existing_signature_collisions = sorted(set(signatures) & existing_keys)
    retained_quantity = Decimal("0")
    retained_signatures: set[str] = set()
    for proposed, signature in validated:
        if signature in existing_signature_collisions or signature in retained_signatures:
            continue
        retained_signatures.add(signature)
        retained_quantity += _fill_quantity_for_basis(
            proposed,
            quantity_basis=quantity_basis,
        ) or Decimal("0")
    if (
        duplicate_signatures
        or existing_signature_collisions
        or proposed_quantity != expected_quantity
        or retained_quantity != expected_quantity
    ):
        return [], {
            "code": "proposed_fill_set_not_insertable_atomically",
            "quantity_basis": quantity_basis,
            "expected_quantity": _quantity_evidence_value(
                expected_quantity,
                quantity_basis=quantity_basis,
            ),
            "proposed_quantity": _quantity_evidence_value(
                proposed_quantity,
                quantity_basis=quantity_basis,
            ),
            "retained_quantity": _quantity_evidence_value(
                retained_quantity,
                quantity_basis=quantity_basis,
            ),
            "duplicate_signatures": duplicate_signatures,
            "existing_signature_collisions": existing_signature_collisions,
        }
    return validated, None


def _annotate_remediation_reviews(
    *,
    db: Session,
    runtime_mode: str,
    candidate_type: str,
    items: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not items:
        return items
    candidate_keys = {
        str(item["broker_order_pk"])
        if candidate_type == MISSING_FILL_CANDIDATE_TYPE
        else _duplicate_group_candidate_key(item)
        for item in items
    }
    reviews = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == candidate_type,
            RuntimeRemediationEvent.candidate_key.in_(sorted(candidate_keys)),
        )
    ).all()
    review_by_key = {review.candidate_key: review for review in reviews}
    for item in items:
        candidate_key = (
            str(item["broker_order_pk"])
            if candidate_type == MISSING_FILL_CANDIDATE_TYPE
            else _duplicate_group_candidate_key(item)
        )
        review = review_by_key.get(candidate_key)
        item["candidate_key"] = candidate_key
        item["repair_action"] = (
            "insert_authoritative_fill"
            if candidate_type == MISSING_FILL_CANDIDATE_TYPE and item.get("proposed_fills")
            else "record_manual_review"
            if (
                candidate_type == MISSING_FILL_CANDIDATE_TYPE
                or item.get("economic_conflict") is True
            )
            else "isolate_non_destructively"
        )
        if review is not None:
            item["review_outcome"] = review.outcome
            item["review_actor"] = review.actor
            item["review_reason"] = review.reason
            item["reviewed_at"] = _serialize_datetime(review.occurred_at)
    return items


def _duplicate_group_candidate_key(item: dict[str, object]) -> str:
    return duplicate_fill_group_candidate_key(
        group_key=str(item["duplicate_group_key"]),
        fill_ids=_duplicate_group_member_ids(item),
    )


def _duplicate_group_member_ids(item: dict[str, object]) -> list[str]:
    member_ids: list[str] = []
    retained = item.get("retained_fill")
    if isinstance(retained, dict) and retained.get("id") is not None:
        member_ids.append(str(retained["id"]))
    duplicate_fills = item.get("duplicate_fills")
    if isinstance(duplicate_fills, list):
        member_ids.extend(
            str(fill["id"])
            for fill in duplicate_fills
            if isinstance(fill, dict) and fill.get("id") is not None
        )
    return member_ids


def _duplicate_group_membership_version(item: dict[str, object]) -> str:
    candidate_key = duplicate_fill_group_candidate_key(
        group_key=str(item["duplicate_group_key"]),
        fill_ids=_duplicate_group_member_ids(item),
    )
    return candidate_key.rsplit(":", 1)[-1]


def _record_remediation_event(
    *,
    db: Session,
    runtime_mode: str,
    candidate_type: str,
    candidate_key: str,
    subject_id: str | None,
    outcome: str,
    actor: str,
    reason: str,
    occurred_at: datetime,
    evidence_summary: dict[str, object],
) -> bool:
    existing = db.scalar(
        select(RuntimeRemediationEvent.id).where(
            RuntimeRemediationEvent.mode == runtime_mode,
            RuntimeRemediationEvent.candidate_type == candidate_type,
            RuntimeRemediationEvent.candidate_key == candidate_key,
            RuntimeRemediationEvent.outcome == outcome,
        )
    )
    if existing is not None:
        return False
    db.add(
        RuntimeRemediationEvent(
            mode=runtime_mode,
            candidate_type=candidate_type,
            candidate_key=candidate_key,
            subject_id=subject_id,
            outcome=outcome,
            actor=actor,
            reason=reason,
            evidence_summary=evidence_summary,
            occurred_at=occurred_at,
        )
    )
    return True


def _fill_row_payload(fill: ExecutionFill, *, id_override: str | None = "__fill_id__") -> dict[str, object]:
    payload = {
        "id": fill.id if id_override == "__fill_id__" else id_override,
        "idempotency_key": fill.idempotency_key,
        "broker_order_pk": fill.broker_order_pk,
        "broker_fill_id": fill.broker_fill_id,
        "symbol": fill.symbol,
        "side": fill.side,
        "quantity": fill.quantity,
        "base_quantity": _decimal_evidence_text(fill.base_quantity),
        "quote_notional": _decimal_evidence_text(fill.quote_notional),
        "price": _decimal_text(Decimal(fill.price)),
        "settlement_currency": fill.settlement_currency,
        "transaction_cost": _decimal_evidence_text(fill.transaction_cost),
        "transaction_cost_coverage": fill.transaction_cost_coverage,
        "transaction_cost_source": fill.transaction_cost_source,
        "filled_at": _serialize_datetime(fill.filled_at),
        "created_at": _serialize_datetime(fill.created_at),
        "updated_at": _serialize_datetime(fill.updated_at),
    }
    if payload["id"] is None:
        payload.pop("id")
    return payload


def _evidence_fields(payload: dict[str, object]) -> set[str]:
    fields = set(payload.keys())
    nested = payload.get("order")
    if isinstance(nested, dict):
        fields.update(f"order.{key}" for key in nested.keys())
    return fields


def _parse_authoritative_fill_timestamp(value: object) -> datetime | None:
    """Parse only broker-origin timestamps with an explicit timezone.

    A reconciliation/event observation time describes when Exec learned about
    a fill, not when the broker executed it, so it is intentionally not a
    fallback for canonical FIFO chronology.
    """

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    candidates = (text.replace("Z", "+00:00"), text.replace(" ", "T"))
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is not None and parsed.tzinfo is not None and parsed.utcoffset() is not None:
            return parsed.astimezone(UTC)
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y%m%d%H%M%S%z",
        ):
            try:
                return datetime.strptime(candidate, fmt).astimezone(UTC)
            except ValueError:
                continue
    return None


def _parse_timestamp(value: object, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        text = value.strip()
        for candidate in (text, text.replace(" ", "T"), text.replace("Z", "+00:00")):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y%m%d%H%M%S%z",
                "%Y%m%d%H%M%S",
            ):
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    return (
                        parsed.astimezone(UTC)
                        if parsed.tzinfo is not None
                        else parsed.replace(tzinfo=UTC)
                    )
                except (TypeError, ValueError):
                    pass
        try:
            parsed = datetime.fromisoformat(text)
            return (
                parsed.astimezone(UTC)
                if parsed.tzinfo is not None
                else parsed.replace(tzinfo=UTC)
            )
        except ValueError:
            pass
    return fallback


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int | float):
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    if isinstance(value, str):
        try:
            parsed = Decimal(value.strip())
            return parsed if parsed.is_finite() else None
        except (InvalidOperation, ValueError):
            return None
    return None


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    parsed = _parse_decimal(value)
    if parsed is None or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _optional_positive_decimal(value: object) -> Decimal | None:
    parsed = _parse_decimal(value)
    return parsed if parsed is not None and parsed > 0 else None


def _first_positive_decimal(
    payload: dict[str, object],
    *keys: str,
) -> Decimal | None:
    for key in keys:
        parsed = _optional_positive_decimal(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _decimal_text(value: Decimal) -> str:
    return f"{value:.4f}"


def _decimal_evidence_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(value), "f")


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value
