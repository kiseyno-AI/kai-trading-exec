from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerErrorClassification,
)
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import ACTIONABLE_OPEN_ORDER_STATUSES
from app.services.fifo_inventory import fifo_route_scope_from_order
from app.services.position_identity import (
    POSITION_IDENTITY_KEY,
    PositionIdentity,
    PositionIdentityConflictError,
    build_position_identity,
    payload_with_position_identity,
    position_identity_from_columns,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot
from app.services.runtime import position_truth_freshness_payload


POSITION_CANDIDATE_TYPE = "stale_position_truth"
LEGACY_POSITION_ATTRIBUTION_CANDIDATE_TYPE = "legacy_position_attribution"
SUPPORTED_POSITION_EXCHANGES = {"KRX", "NASD", "NYSE", "AMEX"}
POSITION_EVIDENCE_ORDER_STATUSES = (*ACTIONABLE_OPEN_ORDER_STATUSES, "filled")


def analyze_legacy_position_attribution(
    *,
    settings: Settings,
    db: Session,
    auth_session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
    exchange: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Prove, without mutating, which legacy positions match current account truth."""
    evaluated_at = now or datetime.now(UTC)
    normalized_exchange = _supported_attribution_exchange(exchange)
    provider = str(adapter.provider or "").strip().lower()
    if not provider or provider == "unknown":
        raise PositionIdentityConflictError(
            "legacy position attribution broker provider is unavailable"
        )
    if provider != "kis":
        raise PositionIdentityConflictError(
            "legacy position attribution supports only KIS route semantics"
        )
    legacy_positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider.is_(None),
            ExecutionPosition.account_scope.is_(None),
            ExecutionPosition.exchange.is_(None),
        )
        .order_by(ExecutionPosition.symbol.asc(), ExecutionPosition.id.asc())
    ).all()
    if not legacy_positions:
        return _legacy_attribution_payload(
            mode="dry-run",
            settings=settings,
            exchange=normalized_exchange,
            evaluated_at=evaluated_at,
            snapshot_observed_at=None,
            items=[],
        )

    normalized_legacy_symbols = [
        _normalized_position_symbol(position.symbol)
        for position in legacy_positions
    ]
    if len(set(normalized_legacy_symbols)) != len(normalized_legacy_symbols):
        raise PositionIdentityConflictError(
            "legacy position attribution found a duplicate legacy position symbol"
        )

    access_token = _durably_authenticate_for_position_refresh(
        settings=settings,
        session_factory=auth_session_factory,
        adapter=adapter,
    )
    requested_symbols = sorted(normalized_legacy_symbols)
    snapshot = adapter.get_account_snapshot(
        access_token,
        symbols=requested_symbols,
        exchange=normalized_exchange,
    )
    _require_current_attribution_snapshot(
        settings=settings,
        evaluated_at=evaluated_at,
        snapshot_observed_at=snapshot.observed_at,
    )
    if not snapshot.holdings_complete:
        raise BrokerAdapterError(
            "broker account snapshot holdings coverage is incomplete; "
            "legacy position attribution blocked",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
            response_payload={"diagnostics": snapshot.diagnostics},
            observed_at=snapshot.observed_at,
        )
    snapshot_exchange = _reported_attribution_exchange(snapshot.payload)
    if snapshot_exchange is None:
        raise PositionIdentityConflictError(
            "legacy position attribution snapshot exchange proof is unavailable"
        )
    if (
        snapshot_exchange is not None
        and snapshot_exchange != normalized_exchange
    ):
        raise PositionIdentityConflictError(
            "legacy position attribution snapshot exchange does not match "
            "the requested exchange"
        )
    identity = build_position_identity(
        settings=settings,
        provider=provider,
        exchange=normalized_exchange,
        broker_account_id=snapshot.broker_account_id,
    )
    holdings_by_symbol: dict[str, BrokerAccountHolding] = {}
    for holding in snapshot.holdings:
        holding_symbol = _normalized_position_symbol(holding.symbol)
        if holding_symbol in holdings_by_symbol:
            raise PositionIdentityConflictError(
                "legacy position attribution snapshot contains duplicate broker "
                f"holding for {holding_symbol}"
            )
        holdings_by_symbol[holding_symbol] = holding
    scoped_symbols = set(
        db.scalars(
            select(ExecutionPosition.symbol).where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.provider == identity.provider,
                ExecutionPosition.account_scope == identity.account_scope,
                ExecutionPosition.exchange == identity.exchange,
                ExecutionPosition.symbol.in_(
                    [position.symbol for position in legacy_positions]
                ),
            )
        ).all()
    )
    items = [
        _legacy_attribution_item(
            settings=settings,
            position=position,
            holding=holdings_by_symbol.get(
                _normalized_position_symbol(position.symbol)
            ),
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            scoped_route_exists=position.symbol in scoped_symbols,
        )
        for position in legacy_positions
    ]
    return _legacy_attribution_payload(
        mode="dry-run",
        settings=settings,
        exchange=normalized_exchange,
        evaluated_at=evaluated_at,
        snapshot_observed_at=snapshot.observed_at,
        items=items,
    )


def apply_legacy_position_attribution(
    *,
    settings: Settings,
    db: Session,
    auth_session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
    exchange: str,
    approved_evidence: dict[str, str],
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Bind only rows whose operator-approved evidence still matches broker truth."""
    normalized_actor = str(actor or "").strip()
    normalized_reason = str(reason or "").strip()
    normalized_approvals = _validated_legacy_attribution_approvals(approved_evidence)
    if not normalized_actor or not normalized_reason:
        raise ValueError("legacy position attribution requires actor and reason")

    evaluated_at = now or datetime.now(UTC)
    try:
        current = analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=auth_session_factory,
            adapter=adapter,
            exchange=exchange,
            now=evaluated_at,
        )
        current_items = {
            str(item["position_id"]): item
            for item in current["items"]
            if isinstance(item, dict)
        }
        approved_items: list[dict[str, object]] = []
        for position_id, approved_fingerprint in normalized_approvals.items():
            item = current_items.get(position_id)
            current_fingerprint = (
                item.get("evidence_fingerprint") if item is not None else None
            )
            if (
                item is None
                or item.get("eligible") is not True
                or not isinstance(current_fingerprint, str)
                or not hmac.compare_digest(
                    approved_fingerprint,
                    current_fingerprint,
                )
            ):
                raise PositionIdentityConflictError(
                    "approved legacy position evidence no longer matches current "
                    "broker truth"
                )
            approved_items.append(item)

        approved_ids = sorted(normalized_approvals)
        locked_positions = db.scalars(
            select(ExecutionPosition)
            .where(
                ExecutionPosition.id.in_(approved_ids),
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.provider.is_(None),
                ExecutionPosition.account_scope.is_(None),
                ExecutionPosition.exchange.is_(None),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).all()
        locked_by_id = {position.id: position for position in locked_positions}
        if set(locked_by_id) != set(approved_ids):
            raise PositionIdentityConflictError(
                "approved legacy position is missing or already attributed"
            )

        for item in approved_items:
            position_id = str(item["position_id"])
            position = locked_by_id[position_id]
            if (
                _normalized_position_symbol(position.symbol) != item["symbol"]
                or position.updated_from != item["updated_from"]
                or _serialize_datetime(position.last_reconciled_at)
                != item["last_reconciled_at"]
                or _legacy_payload_digest(position.raw_payload)
                != item["legacy_payload_digest"]
                or _canonical_attribution_decimal(position.quantity)
                != item["legacy_integer_quantity"]
                or _canonical_attribution_decimal(position.base_quantity)
                != item["legacy_base_quantity"]
                or _canonical_attribution_decimal(_position_quantity(position))
                != item["legacy_quantity"]
                or _canonical_attribution_decimal(position.average_price)
                != item["legacy_average_price"]
            ):
                raise PositionIdentityConflictError(
                    "approved legacy position changed while attribution was pending"
                )
            identity = PositionIdentity(
                provider=str(item["provider"]),
                account_scope=str(item["account_scope"]),
                exchange=str(item["exchange"]),
            )
            position.provider = identity.provider
            position.account_scope = identity.account_scope
            position.exchange = identity.exchange
            position.raw_payload = payload_with_position_identity(
                position.raw_payload,
                identity=identity,
            )
            db.add(
                RuntimeRemediationEvent(
                    mode=settings.runtime_mode,
                    candidate_type=LEGACY_POSITION_ATTRIBUTION_CANDIDATE_TYPE,
                    candidate_key=position.id,
                    subject_id=position.id,
                    outcome="position_route_attributed",
                    actor=normalized_actor,
                    reason=normalized_reason,
                    evidence_summary={
                        "symbol": position.symbol,
                        "provider": identity.provider,
                        "account_scope": identity.account_scope,
                        "exchange": identity.exchange,
                        "updated_from": item["updated_from"],
                        "last_reconciled_at": item["last_reconciled_at"],
                        "legacy_payload_digest": item["legacy_payload_digest"],
                        "legacy_integer_quantity": item[
                            "legacy_integer_quantity"
                        ],
                        "legacy_base_quantity": item["legacy_base_quantity"],
                        "legacy_quantity": item["legacy_quantity"],
                        "legacy_average_price": item["legacy_average_price"],
                        "broker_quantity": item["broker_quantity"],
                        "broker_average_price": item["broker_average_price"],
                        "broker_snapshot_observed_at": current[
                            "broker_snapshot_observed_at"
                        ],
                        "evidence_fingerprint": item["evidence_fingerprint"],
                        "orders_submitted": 0,
                        "orders_amended": 0,
                        "orders_canceled": 0,
                    },
                    occurred_at=evaluated_at,
                )
            )
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "mode": "apply",
        "runtime_mode": settings.runtime_mode,
        "provider": approved_items[0]["provider"],
        "exchange": approved_items[0]["exchange"],
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": current["broker_snapshot_observed_at"],
        "summary": {
            "approved_count": len(approved_items),
            "attributed_count": len(approved_items),
            "manual_review_count": 0,
        },
        "items": [
            {**item, "outcome": "position_route_attributed"}
            for item in approved_items
        ],
    }


def _validated_legacy_attribution_approvals(
    approved_evidence: dict[str, str],
) -> dict[str, str]:
    if not isinstance(approved_evidence, dict) or not approved_evidence:
        raise ValueError(
            "legacy position attribution requires explicit approved evidence"
        )
    normalized: dict[str, str] = {}
    for raw_position_id, raw_fingerprint in approved_evidence.items():
        position_id = str(raw_position_id or "").strip()
        fingerprint = str(raw_fingerprint or "").strip().lower()
        if (
            not position_id
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            raise ValueError("legacy position attribution approval is invalid")
        normalized[position_id] = fingerprint
    return normalized


def _supported_attribution_exchange(exchange: str) -> str:
    normalized = str(exchange or "").strip().upper()
    if normalized not in SUPPORTED_POSITION_EXCHANGES:
        raise ValueError(
            "legacy position attribution requires a supported exact exchange"
        )
    return normalized


def _require_current_attribution_snapshot(
    *,
    settings: Settings,
    evaluated_at: datetime,
    snapshot_observed_at: datetime,
) -> None:
    if (
        evaluated_at.tzinfo is None
        or evaluated_at.utcoffset() is None
        or snapshot_observed_at.tzinfo is None
        or snapshot_observed_at.utcoffset() is None
    ):
        raise BrokerAdapterError(
            "broker account snapshot is not current; aware timestamps are required",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
        )
    age = evaluated_at.astimezone(UTC) - snapshot_observed_at.astimezone(UTC)
    max_age = timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    max_future_skew = timedelta(
        seconds=settings.account_drawdown_recovery_max_snapshot_future_skew_seconds
    )
    if age > max_age or age < -max_future_skew:
        raise BrokerAdapterError(
            "broker account snapshot is not current; legacy position attribution blocked",
            status_code=502,
            classification=BrokerErrorClassification.UNKNOWN,
            observed_at=snapshot_observed_at,
        )


def _reported_attribution_exchange(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("exchange")
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip().upper()
    return {
        "NAS": "NASD",
        "NASDAQ": "NASD",
        "NYS": "NYSE",
        "AMS": "AMEX",
        "KOSPI": "KRX",
        "KOSDAQ": "KRX",
        "NXT": "KRX",
        "SOR": "KRX",
    }.get(normalized, normalized)


def _normalized_position_symbol(symbol: object) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise PositionIdentityConflictError(
            "legacy position attribution encountered an empty symbol"
        )
    return normalized


def _legacy_attribution_item(
    *,
    settings: Settings,
    position: ExecutionPosition,
    holding: BrokerAccountHolding | None,
    provider: str,
    account_scope: str,
    exchange: str,
    scoped_route_exists: bool,
) -> dict[str, object]:
    legacy_quantity = _position_quantity(position)
    broker_quantity = _attribution_holding_quantity(holding)
    legacy_average_price = (
        Decimal(position.average_price)
        if position.average_price is not None
        else None
    )
    broker_average_price = (
        Decimal(holding.average_price)
        if holding is not None and holding.average_price is not None
        else None
    )
    holding_exchange = _reported_holding_exchange(holding)
    stored_route_identity_present = (
        isinstance(position.raw_payload, dict)
        and POSITION_IDENTITY_KEY in position.raw_payload
    )
    if str(position.symbol or "").strip() != _normalized_position_symbol(
        position.symbol
    ):
        reason = "legacy_symbol_not_canonical"
    elif not _symbol_matches_attribution_exchange(
        symbol=position.symbol,
        exchange=exchange,
    ):
        reason = "symbol_exchange_incompatible"
    elif stored_route_identity_present:
        reason = "stored_route_identity_present"
    elif scoped_route_exists:
        reason = "scoped_route_symbol_collision"
    elif position.base_quantity is not None:
        reason = "unsupported_quantity_representation"
    elif legacy_quantity <= 0:
        reason = "legacy_position_not_positive"
    elif holding is None:
        reason = "broker_holding_missing"
    elif exchange != "KRX" and holding_exchange is None:
        reason = "broker_holding_exchange_missing"
    elif holding_exchange is not None and holding_exchange != exchange:
        reason = "broker_holding_exchange_mismatch"
    elif broker_quantity != legacy_quantity:
        reason = "quantity_mismatch"
    elif (
        legacy_average_price is None
        or legacy_average_price <= 0
        or broker_average_price is None
        or broker_average_price <= 0
    ):
        reason = "average_price_missing"
    elif broker_average_price != legacy_average_price:
        reason = "average_price_mismatch"
    else:
        reason = "exact_broker_holding_match"

    eligible = reason == "exact_broker_holding_match"
    evidence = {
        "mode": settings.runtime_mode,
        "position_id": position.id,
        "symbol": _normalized_position_symbol(position.symbol),
        "updated_from": position.updated_from,
        "last_reconciled_at": _serialize_datetime(position.last_reconciled_at),
        "legacy_payload_digest": _legacy_payload_digest(position.raw_payload),
        "legacy_integer_quantity": _canonical_attribution_decimal(position.quantity),
        "legacy_base_quantity": _canonical_attribution_decimal(position.base_quantity),
        "legacy_quantity": _canonical_attribution_decimal(legacy_quantity),
        "legacy_average_price": _canonical_attribution_decimal(legacy_average_price),
        "broker_quantity": _canonical_attribution_decimal(broker_quantity),
        "broker_average_price": _canonical_attribution_decimal(broker_average_price),
        "provider": provider,
        "account_scope": account_scope,
        "exchange": exchange,
    }
    evidence_fingerprint = (
        hashlib.sha256(
            json.dumps(
                evidence,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if eligible
        else None
    )
    return {
        "position_id": position.id,
        "symbol": position.symbol,
        "updated_from": evidence["updated_from"],
        "last_reconciled_at": evidence["last_reconciled_at"],
        "legacy_payload_digest": evidence["legacy_payload_digest"],
        "legacy_integer_quantity": evidence["legacy_integer_quantity"],
        "legacy_base_quantity": evidence["legacy_base_quantity"],
        "legacy_quantity": evidence["legacy_quantity"],
        "legacy_average_price": evidence["legacy_average_price"],
        "broker_quantity": evidence["broker_quantity"],
        "broker_average_price": evidence["broker_average_price"],
        "provider": provider,
        "account_scope": account_scope,
        "exchange": exchange,
        "eligible": eligible,
        "reason": reason,
        "evidence_fingerprint": evidence_fingerprint,
    }


def _symbol_matches_attribution_exchange(*, symbol: str, exchange: str) -> bool:
    normalized_symbol = _normalized_position_symbol(symbol)
    is_domestic_symbol = len(normalized_symbol) == 6 and normalized_symbol.isdigit()
    return is_domestic_symbol if exchange == "KRX" else not is_domestic_symbol


def _reported_holding_exchange(
    holding: BrokerAccountHolding | None,
) -> str | None:
    if holding is None or not isinstance(holding.payload, dict):
        return None
    for key in (
        "ovrs_excg_cd",
        "OVRS_EXCG_CD",
        "exchange",
        "excg_cd",
        "EXCG_CD",
    ):
        value = holding.payload.get(key)
        if isinstance(value, str) and value.strip():
            return _reported_attribution_exchange({"exchange": value})
    return None


def _legacy_payload_digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _attribution_holding_quantity(
    holding: BrokerAccountHolding | None,
) -> Decimal | None:
    if holding is None:
        return None
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    base_quantity = payload.get("base_quantity")
    if base_quantity is not None:
        try:
            parsed = Decimal(str(base_quantity))
        except Exception:
            return None
        return parsed if parsed >= 0 else None
    return Decimal(holding.quantity)


def _canonical_attribution_decimal(value: object) -> str | None:
    if value is None:
        return None
    parsed = Decimal(value)
    if not parsed.is_finite():
        return None
    normalized = parsed.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _legacy_attribution_payload(
    *,
    mode: str,
    settings: Settings,
    exchange: str,
    evaluated_at: datetime,
    snapshot_observed_at: datetime | None,
    items: list[dict[str, object]],
) -> dict[str, object]:
    eligible_count = sum(item["eligible"] is True for item in items)
    return {
        "mode": mode,
        "runtime_mode": settings.runtime_mode,
        "provider": items[0]["provider"] if items else None,
        "exchange": exchange,
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": _serialize_datetime(snapshot_observed_at),
        "summary": {
            "candidate_count": len(items),
            "eligible_count": eligible_count,
            "manual_review_count": len(items) - eligible_count,
        },
        "items": items,
    }


def analyze_position_truth_remediation(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    freshness = position_truth_freshness_payload(settings=settings, db=db, now=observed_at)
    stale_symbols = sorted(
        {
            *(str(symbol) for symbol in freshness.get("stale_symbols") or []),
            *_stale_active_kis_position_symbols(
                settings=settings,
                db=db,
                now=observed_at,
            ),
        }
    )
    positions = _active_kis_positions_by_symbol(
        settings=settings,
        db=db,
        symbols=set(stale_symbols),
    )
    order_exchanges = _missing_position_order_exchanges(
        settings=settings,
        db=db,
        symbols=set(stale_symbols),
    )
    items: list[dict[str, object]] = []
    for symbol in sorted(stale_symbols):
        position = positions.get(symbol)
        exchange = _position_exchange(
            symbol=symbol,
            position=position,
            order_exchange=order_exchanges.get(symbol),
        )
        eligible = exchange in SUPPORTED_POSITION_EXCHANGES
        last_reconciled_at = position.last_reconciled_at if position is not None else None
        items.append(
            {
                "symbol": symbol,
                "position_id": position.id if position is not None else None,
                "last_reconciled_at": _serialize_datetime(last_reconciled_at),
                "exchange": exchange,
                "eligible": eligible,
                "repair_action": "refresh_from_broker_account_inquiry" if eligible else "manual_review",
                "reason": (
                    "supported_read_only_account_inquiry_route"
                    if eligible
                    else "unsupported_or_unknown_position_inquiry_route"
                ),
            }
        )
    return {
        "mode": "dry-run",
        "runtime_mode": settings.runtime_mode,
        "generated_at": observed_at.isoformat(),
        "summary": {
            "candidate_count": len(items),
            "eligible_refresh_count": sum(1 for item in items if item["eligible"] is True),
            "manual_review_count": sum(1 for item in items if item["eligible"] is not True),
        },
        "items": items,
    }


def apply_position_truth_remediation(
    *,
    settings: Settings,
    db: Session,
    auth_session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    started_at = now or datetime.now(UTC)
    analysis = analyze_position_truth_remediation(settings=settings, db=db, now=started_at)
    items = list(analysis["items"])
    symbols_by_exchange: dict[str, set[str]] = {}
    for item in items:
        if item["eligible"] is True:
            symbols_by_exchange.setdefault(str(item["exchange"]), set()).add(str(item["symbol"]))

    eligible_symbols = {
        str(item["symbol"])
        for item in items
        if item["eligible"] is True
    }
    positions_before = {
        symbol: _position_quantity(position)
        for symbol, position in _active_kis_positions_by_symbol(
            settings=settings,
            db=db,
            symbols=eligible_symbols,
        ).items()
    }

    sync_results: dict[str, dict[str, int]] = {}
    try:
        access_token = (
            _durably_authenticate_for_position_refresh(
                settings=settings,
                session_factory=auth_session_factory,
                adapter=adapter,
            )
            if symbols_by_exchange
            else None
        )
        for exchange, symbols in sorted(symbols_by_exchange.items()):
            sync_results[exchange] = sync_execution_positions_from_broker_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=symbols,
                exchange=exchange,
                commit=False,
                authenticated_access_token=access_token,
            )
    except Exception:
        db.rollback()
        raise

    db.flush()
    positions_after = _active_kis_positions_by_symbol(
        settings=settings,
        db=db,
        symbols=eligible_symbols,
    )
    post_refresh_evidence = {
        symbol: _post_refresh_position_evidence(
            position=positions_after[symbol],
            existed_before=symbol in positions_before,
            previous_quantity=positions_before.get(symbol),
        )
        for symbol in sorted(eligible_symbols)
    }

    applied_event_count = 0
    for item in items:
        eligible = item["eligible"] is True
        outcome = "position_truth_refreshed" if eligible else "manual_review_required"
        candidate_key = _candidate_key(item)
        existing = db.scalar(
            select(RuntimeRemediationEvent.id).where(
                RuntimeRemediationEvent.mode == settings.runtime_mode,
                RuntimeRemediationEvent.candidate_type == POSITION_CANDIDATE_TYPE,
                RuntimeRemediationEvent.candidate_key == candidate_key,
                RuntimeRemediationEvent.outcome == outcome,
            )
        )
        if existing is not None:
            continue
        db.add(
            RuntimeRemediationEvent(
                mode=settings.runtime_mode,
                candidate_type=POSITION_CANDIDATE_TYPE,
                candidate_key=candidate_key,
                subject_id=str(item["position_id"] or item["symbol"]),
                outcome=outcome,
                actor=actor,
                reason=reason,
                evidence_summary={
                    "symbol": item["symbol"],
                    "exchange": item["exchange"],
                    "previous_last_reconciled_at": item["last_reconciled_at"],
                    "inquiry_type": "broker_account_snapshot" if eligible else None,
                    **(post_refresh_evidence[str(item["symbol"])] if eligible else {}),
                    "blocker": None if eligible else item["reason"],
                    "fills_deleted": 0,
                    "fills_fabricated": 0,
                    "orders_submitted": 0,
                    "orders_amended": 0,
                    "orders_canceled": 0,
                },
                occurred_at=started_at,
            )
        )
        applied_event_count += 1
    db.commit()

    refreshed_count = sum(1 for item in items if item["eligible"] is True)
    manual_review_count = len(items) - refreshed_count
    return {
        "mode": "apply",
        "runtime_mode": settings.runtime_mode,
        "generated_at": started_at.isoformat(),
        "summary": {
            "candidate_count": len(items),
            "refreshed_count": refreshed_count,
            "manual_review_count": manual_review_count,
            "audit_event_count": applied_event_count,
            "orders_submitted": 0,
            "orders_amended": 0,
            "orders_canceled": 0,
        },
        "sync_results": sync_results,
        "items": [
            {
                **item,
                "outcome": (
                    "position_truth_refreshed"
                    if item["eligible"] is True
                    else "manual_review_required"
                ),
            }
            for item in items
        ],
    }


def _durably_authenticate_for_position_refresh(
    *,
    settings: Settings,
    session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
) -> str:
    """Commit auth state and its audit event outside the position transaction."""
    with session_factory() as auth_db:
        state = ensure_authenticated_broker_session(
            settings=settings,
            db=auth_db,
            adapter=adapter,
            source="position_truth_remediation",
            reason="refresh execution positions from broker snapshot",
        )
        return state.access_token or ""


def _candidate_key(item: dict[str, object]) -> str:
    return f"{item['symbol']}:{item.get('last_reconciled_at') or 'missing'}"


def _post_refresh_position_evidence(
    *,
    position: ExecutionPosition,
    existed_before: bool,
    previous_quantity: Decimal | None,
) -> dict[str, object]:
    resulting_quantity = _position_quantity(position)
    if not existed_before:
        refresh_outcome = "created"
    elif previous_quantity is not None and previous_quantity != 0 and resulting_quantity == 0:
        refresh_outcome = "zeroed"
    else:
        refresh_outcome = "updated"
    return {
        "broker_snapshot_observed_at": _serialize_datetime(position.last_reconciled_at),
        "position_refresh_outcome": refresh_outcome,
        "resulting_position_id": position.id,
        "resulting_quantity": position.quantity,
        "resulting_base_quantity": _serialize_decimal(position.base_quantity),
        "resulting_average_price": _serialize_decimal(position.average_price),
    }


def _position_quantity(position: ExecutionPosition) -> Decimal:
    if position.base_quantity is not None:
        return Decimal(position.base_quantity)
    return Decimal(position.quantity)


def _active_kis_positions_by_symbol(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
) -> dict[str, ExecutionPosition]:
    if not symbols:
        return {}
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return {}
    rows = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == identity.provider,
            ExecutionPosition.account_scope == identity.account_scope,
            ExecutionPosition.exchange.is_not(None),
            ExecutionPosition.symbol.in_(sorted(symbols)),
        )
    ).all()
    positions: dict[str, ExecutionPosition] = {}
    for position in rows:
        if position.symbol in positions:
            raise PositionIdentityConflictError(
                "position remediation route is ambiguous for symbol "
                f"{position.symbol}"
            )
        positions[position.symbol] = position
    return positions


def _stale_active_kis_position_symbols(
    *,
    settings: Settings,
    db: Session,
    now: datetime,
) -> set[str]:
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return set()
    stale_before = now - timedelta(
        seconds=settings.exec_agent_max_account_snapshot_age_seconds
    )
    return {
        str(symbol)
        for symbol in db.scalars(
            select(ExecutionPosition.symbol).where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.provider == identity.provider,
                ExecutionPosition.account_scope == identity.account_scope,
                ExecutionPosition.exchange.is_not(None),
                or_(
                    ExecutionPosition.last_reconciled_at.is_(None),
                    ExecutionPosition.last_reconciled_at < stale_before,
                ),
            )
        ).all()
    }


def _serialize_decimal(value: Any) -> str | None:
    if value is None:
        return None
    return format(Decimal(value), "f")


def _missing_position_order_exchanges(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
) -> dict[str, str]:
    if not symbols:
        return {}
    try:
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
    except PositionIdentityConflictError:
        return {}
    rows = db.execute(
        select(BrokerOrder, BrokerOrderEvent.request_payload)
        .join(BrokerOrderEvent, BrokerOrderEvent.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == "kis",
            BrokerOrder.asset_class == "us_equity",
            BrokerOrder.venue == "kis",
            or_(
                and_(
                    BrokerOrder.side == "buy",
                    BrokerOrder.status.in_(POSITION_EVIDENCE_ORDER_STATUSES),
                ),
                and_(
                    BrokerOrder.side == "sell",
                    BrokerOrder.status.in_(POSITION_EVIDENCE_ORDER_STATUSES),
                ),
            ),
            BrokerOrder.symbol.in_(sorted(symbols)),
            BrokerOrderEvent.event_type == "submit_requested",
        )
    ).all()
    exchanges_by_symbol: dict[str, set[str]] = {}
    for order, request_payload in rows:
        route_scope = fifo_route_scope_from_order(order)
        if (
            route_scope is None
            or route_scope.provider != identity.provider
            or route_scope.account_scope != identity.account_scope
            or route_scope.exchange not in SUPPORTED_POSITION_EXCHANGES - {"KRX"}
        ):
            continue
        if not isinstance(request_payload, dict):
            continue
        exchange = str(request_payload.get("exchange") or "").strip().upper()
        if exchange != route_scope.exchange:
            continue
        exchanges_by_symbol.setdefault(str(order.symbol), set()).add(exchange)
    return {
        symbol: next(iter(exchanges))
        for symbol, exchanges in exchanges_by_symbol.items()
        if len(exchanges) == 1
    }


def _position_exchange(
    *,
    symbol: str,
    position: ExecutionPosition | None,
    order_exchange: str | None,
) -> str | None:
    if position is not None:
        try:
            identity = position_identity_from_columns(position)
        except PositionIdentityConflictError:
            return None
        if identity is None:
            return None
        return identity.exchange
    if order_exchange is not None:
        return order_exchange
    if symbol.isdigit() and len(symbol) == 6:
        return "KRX"
    return None


def _serialize_datetime(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
