from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import hmac
import json
from typing import Any

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerErrorClassification,
)
from app.broker.kis import KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.account_drawdown_recovery import recovery_account_fingerprint
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.broker_order_state import ACTIONABLE_OPEN_ORDER_STATUSES
from app.services.fifo_inventory import fifo_route_scope_from_order
from app.services.order_intent_serialization import (
    alpaca_pre_auth_order_intent_account_scope,
    order_intent_file_lock,
    order_intent_process_lock,
    order_intent_scope_key_for_account,
)
from app.services.position_identity import (
    KIS_US_POSITION_QUARANTINE_EXCHANGES,
    POSITION_IDENTITY_KEY,
    PositionIdentity,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
    payload_with_position_identity,
    position_identity_from_columns,
    stable_account_scope_hmac_key,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot
from app.services.runtime import position_truth_freshness_payload


POSITION_CANDIDATE_TYPE = "stale_position_truth"
LEGACY_POSITION_ATTRIBUTION_CANDIDATE_TYPE = "legacy_position_attribution"
ALPACA_CRYPTO_FLAT_REBIND_CANDIDATE_TYPE = (
    "alpaca_crypto_flat_position_rebind"
)
SUPPORTED_POSITION_EXCHANGES = {"KRX", "NASD", "NYSE", "AMEX"}
ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES = frozenset({"US"})
ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE = "ALPACA"
LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES = KIS_US_POSITION_QUARANTINE_EXCHANGES
LEGACY_QUARANTINE_ROUTE_DISPOSITION = "account_attributed_quarantine"
LEGACY_ATTRIBUTION_SAFETY_POLICY_VERSION = (
    "legacy-position-attribution-safety-v5"
)
EXECUTION_POSITION_AVERAGE_PRICE_SCALE = int(
    ExecutionPosition.__table__.c.average_price.type.scale
)
EXECUTION_POSITION_AVERAGE_PRICE_QUANTUM = Decimal(1).scaleb(
    -EXECUTION_POSITION_AVERAGE_PRICE_SCALE
)
ALPACA_AVERAGE_PRICE_MATCH_POLICY = (
    "numeric_18_4_round_half_up_strict_storage_bucket_v1"
)
ALPACA_CRYPTO_FLAT_REBIND_SAFETY_POLICY_VERSION = (
    "alpaca-crypto-flat-position-rebind-v1"
)
POSITION_EVIDENCE_ORDER_STATUSES = (*ACTIONABLE_OPEN_ORDER_STATUSES, "filled")


def _real_utc_now() -> datetime:
    return datetime.now(UTC)


def analyze_alpaca_crypto_flat_position_rebind(
    *,
    settings: Settings,
    db: Session,
    auth_session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
    now: datetime | None = None,
) -> dict[str, object]:
    """Prove which legacy flat crypto slots belong to the current Alpaca account.

    A historical flat row does not prove which account produced its old payload.
    This workflow therefore binds only *current* authenticated flat truth.  It
    never attributes historical holdings, fills, P&L, or strategy activity.
    """
    evaluated_at = now or datetime.now(UTC)
    _require_alpaca_crypto_flat_rebind_route(
        settings=settings,
        adapter=adapter,
    )
    configured_symbols = _configured_alpaca_crypto_symbols(settings)
    legacy_positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider.is_(None),
            ExecutionPosition.account_scope.is_(None),
            ExecutionPosition.exchange.is_(None),
            func.upper(func.trim(ExecutionPosition.symbol)).in_(
                sorted(configured_symbols)
            ),
        )
        .order_by(ExecutionPosition.symbol.asc(), ExecutionPosition.id.asc())
    ).all()
    if not legacy_positions:
        return _alpaca_crypto_flat_rebind_payload(
            mode="dry-run",
            settings=settings,
            evaluated_at=evaluated_at,
            snapshot_observed_at=None,
            snapshot_source=None,
            items=[],
        )

    normalized_symbols = [
        _normalized_position_symbol(position.symbol)
        for position in legacy_positions
    ]
    if len(set(normalized_symbols)) != len(normalized_symbols):
        raise PositionIdentityConflictError(
            "Alpaca crypto flat rebind found a duplicate legacy position symbol"
        )

    access_token = _durably_authenticate_for_position_refresh(
        settings=settings,
        session_factory=auth_session_factory,
        adapter=adapter,
    )
    snapshot = adapter.get_account_snapshot(
        access_token,
        symbols=sorted(normalized_symbols),
        exchange=ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
    )
    _require_current_attribution_snapshot(
        settings=settings,
        evaluated_at=evaluated_at,
        snapshot_observed_at=snapshot.observed_at,
    )
    _require_alpaca_crypto_flat_snapshot(
        settings=settings,
        snapshot=snapshot,
    )
    _, items = _alpaca_crypto_flat_items_for_snapshot(
        settings=settings,
        db=db,
        positions=legacy_positions,
        snapshot=snapshot,
        configured_symbols=configured_symbols,
    )
    return _alpaca_crypto_flat_rebind_payload(
        mode="dry-run",
        settings=settings,
        evaluated_at=evaluated_at,
        snapshot_observed_at=snapshot.observed_at,
        snapshot_source=snapshot.source,
        items=items,
    )


def apply_alpaca_crypto_flat_position_rebind(
    *,
    settings: Settings,
    db: Session,
    auth_session_factory: sessionmaker[Session],
    adapter: BrokerAdapter,
    approved_evidence: dict[str, str],
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Bind an operator-approved set of legacy flat slots to current truth."""
    normalized_actor = str(actor or "").strip()
    normalized_reason = str(reason or "").strip()
    normalized_approvals = _validated_alpaca_crypto_flat_approvals(
        approved_evidence
    )
    if not normalized_actor or not normalized_reason:
        raise ValueError("Alpaca crypto flat rebind requires actor and reason")

    evaluated_at = now or datetime.now(UTC)
    serialization = None
    approved_items: list[dict[str, object]] = []
    try:
        current = analyze_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=auth_session_factory,
            adapter=adapter,
            now=evaluated_at,
        )
        current_items = {
            str(item["position_id"]): item
            for item in current["items"]
            if isinstance(item, dict)
        }
        if not current_items:
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind found no current candidates"
            )
        if any(item.get("eligible") is not True for item in current_items.values()):
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind candidate set is not fully eligible"
            )
        if set(normalized_approvals) != set(current_items):
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind requires the complete current candidate set"
            )
        for position_id, approved_fingerprint in normalized_approvals.items():
            item = current_items[position_id]
            current_fingerprint = item.get("evidence_fingerprint")
            if (
                not isinstance(current_fingerprint, str)
                or not hmac.compare_digest(
                    approved_fingerprint,
                    current_fingerprint,
                )
            ):
                raise PositionIdentityConflictError(
                    "approved Alpaca crypto flat evidence no longer matches "
                    "current broker truth"
                )
            approved_items.append(item)

        approved_ids = sorted(normalized_approvals)
        db.rollback()
        serialization = _acquire_legacy_attribution_apply_serialization(
            settings=settings,
            db=db,
            provider="alpaca",
            account_scope=str(approved_items[0]["account_scope"]),
            exchange=ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
        )
        try:
            locked_positions = db.scalars(
                select(ExecutionPosition)
                .where(
                    ExecutionPosition.mode == settings.runtime_mode,
                    ExecutionPosition.provider.is_(None),
                    ExecutionPosition.account_scope.is_(None),
                    ExecutionPosition.exchange.is_(None),
                    func.upper(func.trim(ExecutionPosition.symbol)).in_(
                        sorted(_configured_alpaca_crypto_symbols(settings))
                    ),
                )
                .order_by(
                    ExecutionPosition.symbol.asc(),
                    ExecutionPosition.id.asc(),
                )
                .with_for_update(nowait=True)
                .execution_options(populate_existing=True)
            ).all()
        except DBAPIError as exc:
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind rows are busy; retry from a new dry-run"
            ) from exc
        locked_by_id = {position.id: position for position in locked_positions}
        if set(locked_by_id) != set(approved_ids):
            raise PositionIdentityConflictError(
                "approved Alpaca crypto flat position is missing or already bound"
            )

        # The operator-approved dry-run and the first apply read are only
        # preliminary evidence.  Once the local scope and target rows are
        # serialized, authenticate again and obtain a second account snapshot.
        # Only this lock-after-refetch proof is allowed to mutate the rows.
        locked_grant = adapter.authenticate()
        locked_snapshot = adapter.get_account_snapshot(
            locked_grant.access_token,
            symbols=sorted(
                _normalized_position_symbol(position.symbol)
                for position in locked_positions
            ),
            exchange=ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
        )
        locked_evaluated_at = _real_utc_now()
        _require_current_attribution_snapshot(
            settings=settings,
            evaluated_at=locked_evaluated_at,
            snapshot_observed_at=locked_snapshot.observed_at,
        )
        _require_alpaca_crypto_flat_snapshot(
            settings=settings,
            snapshot=locked_snapshot,
        )
        _, locked_items = _alpaca_crypto_flat_items_for_snapshot(
            settings=settings,
            db=db,
            positions=locked_positions,
            snapshot=locked_snapshot,
            configured_symbols=_configured_alpaca_crypto_symbols(settings),
        )
        locked_items_by_id = {
            str(item["position_id"]): item for item in locked_items
        }
        if (
            set(locked_items_by_id) != set(normalized_approvals)
            or any(
                item.get("eligible") is not True
                for item in locked_items_by_id.values()
            )
        ):
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind evidence changed after row locking"
            )
        for position_id, approved_fingerprint in normalized_approvals.items():
            locked_fingerprint = locked_items_by_id[position_id].get(
                "evidence_fingerprint"
            )
            if (
                not isinstance(locked_fingerprint, str)
                or not hmac.compare_digest(
                    approved_fingerprint,
                    locked_fingerprint,
                )
            ):
                raise PositionIdentityConflictError(
                    "approved Alpaca crypto flat evidence changed after row locking"
                )
        approved_items = [
            locked_items_by_id[position_id]
            for position_id in normalized_approvals
        ]
        _reject_locked_scoped_symbol_collisions(
            settings=settings,
            db=db,
            approved_items=approved_items,
            approved_ids=approved_ids,
        )
        current = _alpaca_crypto_flat_rebind_payload(
            mode="dry-run",
            settings=settings,
            evaluated_at=locked_evaluated_at,
            snapshot_observed_at=locked_snapshot.observed_at,
            snapshot_source=locked_snapshot.source,
            items=approved_items,
        )
        snapshot_observed_at = locked_snapshot.observed_at
        _require_current_attribution_snapshot(
            settings=settings,
            evaluated_at=locked_evaluated_at,
            snapshot_observed_at=snapshot_observed_at,
        )
        snapshot_source = str(current.get("broker_snapshot_source") or "")

        for item in approved_items:
            position = locked_by_id[str(item["position_id"])]
            if not _locked_alpaca_crypto_flat_position_matches(
                position=position,
                item=item,
            ):
                raise PositionIdentityConflictError(
                    "approved Alpaca crypto flat position changed while rebind "
                    "was pending"
                )
            identity = PositionIdentity(
                provider="alpaca",
                account_scope=str(item["account_scope"]),
                exchange=ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
            )
            position.provider = identity.provider
            position.account_scope = identity.account_scope
            position.exchange = identity.exchange
            position.quantity = 0
            position.base_quantity = Decimal("0")
            position.average_price = None
            position.updated_from = "reconciliation"
            position.last_reconciled_at = snapshot_observed_at
            position.raw_payload = _alpaca_crypto_current_flat_truth_payload(
                settings=settings,
                identity=identity,
                snapshot_source=snapshot_source,
                snapshot_observed_at=snapshot_observed_at,
                absence_proof_digest=str(item["absence_proof_digest"]),
            )
            db.add(
                RuntimeRemediationEvent(
                    mode=settings.runtime_mode,
                    candidate_type=ALPACA_CRYPTO_FLAT_REBIND_CANDIDATE_TYPE,
                    candidate_key=position.id,
                    subject_id=position.id,
                    outcome="current_flat_truth_bound",
                    actor=normalized_actor,
                    reason=normalized_reason,
                    evidence_summary={
                        "symbol": position.symbol,
                        "provider": identity.provider,
                        "account_scope": identity.account_scope,
                        "exchange": identity.exchange,
                        "prior_updated_from": item["updated_from"],
                        "prior_last_reconciled_at": item[
                            "last_reconciled_at"
                        ],
                        "legacy_payload_digest": item[
                            "legacy_payload_digest"
                        ],
                        "legacy_integer_quantity": item[
                            "legacy_integer_quantity"
                        ],
                        "legacy_base_quantity": item[
                            "legacy_base_quantity"
                        ],
                        "legacy_average_price": item[
                            "legacy_average_price"
                        ],
                        "broker_snapshot_source": snapshot_source,
                        "broker_snapshot_observed_at": current[
                            "broker_snapshot_observed_at"
                        ],
                        "absence_proof_digest": item[
                            "absence_proof_digest"
                        ],
                        "evidence_fingerprint": item[
                            "evidence_fingerprint"
                        ],
                        "safety_policy_version": (
                            ALPACA_CRYPTO_FLAT_REBIND_SAFETY_POLICY_VERSION
                        ),
                        "historical_account_attributed": False,
                        "current_flat_truth_bound": True,
                        "holding_absent": True,
                        "open_order_absent": True,
                        "holdings_complete": True,
                        "open_orders_complete": True,
                        "broker_api_mutations": 0,
                        "orders_submitted": 0,
                        "orders_amended": 0,
                        "orders_canceled": 0,
                        "fills_created": 0,
                        "fills_deleted": 0,
                        "pnl_records_mutated": 0,
                        "strategy_records_mutated": 0,
                    },
                    occurred_at=evaluated_at,
                )
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if serialization is not None:
            serialization.close()

    return {
        "mode": "apply",
        "runtime_mode": settings.runtime_mode,
        "provider": "alpaca",
        "exchange": ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": current[
            "broker_snapshot_observed_at"
        ],
        "summary": {
            "approved_count": len(approved_items),
            "bound_count": len(approved_items),
            "historical_account_attributed_count": 0,
            "broker_api_mutation_count": 0,
        },
        "items": [
            {**item, "outcome": "current_flat_truth_bound"}
            for item in approved_items
        ],
    }


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
    quarantine_attribution = (
        normalized_exchange in LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
    )
    if quarantine_attribution and settings.runtime_mode != "live":
        raise ValueError("legacy quarantine attribution requires live mode")
    if (
        quarantine_attribution
        and legacy_attribution_account_scope_source(settings=settings) is None
    ):
        raise PositionIdentityConflictError(
            "legacy quarantine attribution requires a stable canonical account "
            "scope from ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET of at least 32 "
            "bytes or EXECUTION_POSITION_ACCOUNT_SCOPE"
        )
    provider = str(adapter.provider or "").strip().lower()
    if not provider or provider == "unknown":
        raise PositionIdentityConflictError(
            "legacy position attribution broker provider is unavailable"
        )
    if provider == "alpaca":
        if settings.runtime_mode != "paper":
            raise PositionIdentityConflictError(
                "Alpaca legacy position attribution supports only paper mode"
            )
        if settings.runtime_broker_provider != "alpaca":
            raise PositionIdentityConflictError(
                "Alpaca legacy position attribution requires runtime broker "
                "provider alpaca"
            )
        if normalized_exchange not in ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES:
            raise PositionIdentityConflictError(
                "Alpaca paper legacy position attribution requires exchange US"
            )
    elif provider != "kis":
        raise PositionIdentityConflictError(
            "legacy position attribution supports only KIS or Alpaca paper "
            "route semantics"
        )
    elif normalized_exchange in ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES:
        raise PositionIdentityConflictError(
            "KIS legacy position attribution does not support exchange US"
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
    if quarantine_attribution:
        snapshot_loader = getattr(adapter, "get_legacy_us_holdings_snapshot", None)
        if not callable(snapshot_loader):
            raise PositionIdentityConflictError(
                "legacy quarantine attribution requires a KIS holdings-only adapter"
            )
        snapshot = snapshot_loader(access_token)
        query_scope = _require_legacy_quarantine_snapshot(snapshot)
    else:
        snapshot = adapter.get_account_snapshot(
            access_token,
            symbols=requested_symbols,
            exchange=normalized_exchange,
        )
        query_scope = (
            f"alpaca_exact_us_equity_account_snapshot:{normalized_exchange}"
            if provider == "alpaca"
            else f"kis_exact_exchange_account_snapshot:{normalized_exchange}"
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
    if provider == "alpaca":
        _require_alpaca_paper_us_equity_snapshot(
            settings=settings,
            snapshot=snapshot,
        )
    elif not quarantine_attribution:
        snapshot_exchange = _reported_attribution_exchange(snapshot.payload)
        if snapshot_exchange is None:
            raise PositionIdentityConflictError(
                "legacy position attribution snapshot exchange proof is unavailable"
            )
        if snapshot_exchange != normalized_exchange:
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
            select(func.upper(func.trim(ExecutionPosition.symbol))).where(
                func.lower(func.trim(ExecutionPosition.mode))
                == settings.runtime_mode.strip().lower(),
                func.lower(func.trim(ExecutionPosition.provider))
                == identity.provider.strip().lower(),
                func.lower(func.trim(ExecutionPosition.account_scope))
                == identity.account_scope.strip().lower(),
                func.upper(func.trim(ExecutionPosition.symbol)).in_(
                    normalized_legacy_symbols
                ),
            )
        ).all()
    )
    snapshot_proof_digest = _legacy_snapshot_proof_digest(
        snapshot=snapshot,
        exchange=normalized_exchange,
        query_scope=query_scope,
    )
    safety_invariants = _legacy_attribution_safety_invariants(
        settings=settings,
        provider=provider,
        quarantine_attribution=quarantine_attribution,
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
            scoped_route_exists=(
                _normalized_position_symbol(position.symbol) in scoped_symbols
            ),
            query_scope=query_scope,
            quarantine_attribution=quarantine_attribution,
            snapshot_proof_digest=snapshot_proof_digest,
            safety_invariants=safety_invariants,
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
    serialization = None
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
        # Analysis is intentionally read-only. End its implicit transaction so
        # SQLite can take a write reservation before target/collision rechecks,
        # and PostgreSQL can start the apply locks from a fresh transaction.
        db.rollback()
        serialization = _acquire_legacy_attribution_apply_serialization(
            settings=settings,
            db=db,
            provider=str(approved_items[0]["provider"]),
            account_scope=str(approved_items[0]["account_scope"]),
            exchange=str(approved_items[0]["exchange"]),
        )
        try:
            locked_positions = db.scalars(
                select(ExecutionPosition)
                .where(
                    ExecutionPosition.id.in_(approved_ids),
                    ExecutionPosition.mode == settings.runtime_mode,
                    ExecutionPosition.provider.is_(None),
                    ExecutionPosition.account_scope.is_(None),
                    ExecutionPosition.exchange.is_(None),
                )
                .with_for_update(nowait=True)
                .execution_options(populate_existing=True)
            ).all()
        except DBAPIError as exc:
            raise PositionIdentityConflictError(
                "legacy position attribution rows are busy; retry from a new dry-run"
            ) from exc
        locked_by_id = {position.id: position for position in locked_positions}
        if set(locked_by_id) != set(approved_ids):
            raise PositionIdentityConflictError(
                "approved legacy position is missing or already attributed"
            )
        _reject_locked_scoped_symbol_collisions(
            settings=settings,
            db=db,
            approved_items=approved_items,
            approved_ids=approved_ids,
        )
        snapshot_observed_at = _required_attribution_snapshot_observed_at(current)
        _require_current_attribution_snapshot(
            settings=settings,
            evaluated_at=_real_utc_now(),
            snapshot_observed_at=snapshot_observed_at,
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
            quarantine_attribution = (
                item.get("disposition") == LEGACY_QUARANTINE_ROUTE_DISPOSITION
            )
            attributed_payload = _attributed_position_payload(
                payload=position.raw_payload,
                identity=identity,
                quarantine_attribution=quarantine_attribution,
            )
            position.raw_payload = attributed_payload
            outcome = (
                "position_route_attributed_quarantine"
                if quarantine_attribution
                else "position_route_attributed"
            )
            evidence_summary = {
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
                "safety_policy_version": item["safety_policy_version"],
                "safety_invariants": item["safety_invariants"],
                "snapshot_proof_digest": item["snapshot_proof_digest"],
                "orders_submitted": 0,
                "orders_amended": 0,
                "orders_canceled": 0,
            }
            if quarantine_attribution:
                evidence_summary.update(
                    {
                        "query_scope": item["query_scope"],
                        "route_disposition": item["disposition"],
                        "trading_eligible": item["trading_eligible"],
                        "monitoring_eligible": item["monitoring_eligible"],
                    }
                )
            db.add(
                RuntimeRemediationEvent(
                    mode=settings.runtime_mode,
                    candidate_type=LEGACY_POSITION_ATTRIBUTION_CANDIDATE_TYPE,
                    candidate_key=position.id,
                    subject_id=position.id,
                    outcome=outcome,
                    actor=normalized_actor,
                    reason=normalized_reason,
                    evidence_summary=evidence_summary,
                    occurred_at=evaluated_at,
                )
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if serialization is not None:
            serialization.close()

    quarantined_count = sum(
        item.get("disposition") == LEGACY_QUARANTINE_ROUTE_DISPOSITION
        for item in approved_items
    )
    summary = {
        "approved_count": len(approved_items),
        "attributed_count": len(approved_items),
        "manual_review_count": quarantined_count,
    }
    if quarantined_count:
        summary["quarantined_count"] = quarantined_count
    return {
        "mode": "apply",
        "runtime_mode": settings.runtime_mode,
        "provider": approved_items[0]["provider"],
        "exchange": approved_items[0]["exchange"],
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": current["broker_snapshot_observed_at"],
        "summary": summary,
        "items": [
            {
                **item,
                "outcome": (
                    "position_route_attributed_quarantine"
                    if item.get("disposition")
                    == LEGACY_QUARANTINE_ROUTE_DISPOSITION
                    else "position_route_attributed"
                ),
            }
            for item in approved_items
        ],
    }


def _acquire_legacy_attribution_apply_serialization(
    *,
    settings: Settings,
    db: Session,
    provider: str,
    account_scope: str,
    exchange: str,
) -> ExitStack:
    normalized_provider = str(provider).strip().lower()
    market = (
        "ACCOUNT"
        if normalized_provider == "alpaca"
        else "KRW"
        if exchange == "KRX"
        else "USD"
    )
    if normalized_provider == "kis":
        pre_auth_account_scope = recovery_account_fingerprint(settings)
    elif normalized_provider == "alpaca":
        pre_auth_account_scope = alpaca_pre_auth_order_intent_account_scope(
            runtime_mode=settings.runtime_mode,
        )
    else:
        pre_auth_account_scope = str(account_scope).strip().lower()
    local_scope = order_intent_scope_key_for_account(
        provider=normalized_provider,
        runtime_mode=settings.runtime_mode,
        account_scope=pre_auth_account_scope,
        market=market,
    )
    exact_scope = order_intent_scope_key_for_account(
        provider=provider,
        runtime_mode=settings.runtime_mode,
        account_scope=account_scope,
        market=market,
    )
    serialization = ExitStack()
    process_lock = order_intent_process_lock(local_scope)
    if not process_lock.acquire(blocking=False):
        raise PositionIdentityConflictError(
            "legacy position attribution scope is busy; retry from a new dry-run"
        )
    serialization.callback(process_lock.release)
    try:
        database = make_url(settings.database_url)
        if (
            database.drivername.startswith("sqlite")
            and database.database
            and database.database != ":memory:"
        ):
            try:
                serialization.enter_context(
                    order_intent_file_lock(local_scope, blocking=False)
                )
            except OSError as exc:
                raise PositionIdentityConflictError(
                    "legacy position attribution lock is busy; "
                    "retry from a new dry-run"
                ) from exc
        if database.drivername.startswith("sqlite"):
            # SQLite ignores SELECT ... FOR UPDATE. Reserve the one writer slot
            # before target and collision reads so an uncoordinated position
            # writer cannot create a cross-exchange duplicate before commit.
            db.connection().exec_driver_sql("BEGIN IMMEDIATE")
        bind = db.get_bind()
        if bind is not None and bind.dialect.name == "postgresql":
            acquired = db.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtext(:scope))"),
                {"scope": exact_scope},
            )
            if acquired is not True:
                raise PositionIdentityConflictError(
                    "legacy position attribution scope is busy; retry from a new dry-run"
                )
            db.execute(
                text(
                    "LOCK TABLE execution_positions "
                    "IN SHARE ROW EXCLUSIVE MODE NOWAIT"
                )
            )
    except DBAPIError as exc:
        serialization.close()
        raise PositionIdentityConflictError(
            "legacy position attribution lock is busy; retry from a new dry-run"
        ) from exc
    except Exception:
        serialization.close()
        raise
    return serialization


def _reject_locked_scoped_symbol_collisions(
    *,
    settings: Settings,
    db: Session,
    approved_items: list[dict[str, object]],
    approved_ids: list[str],
) -> None:
    provider = str(approved_items[0]["provider"])
    account_scope = str(approved_items[0]["account_scope"])
    symbols = sorted(
        {_normalized_position_symbol(item["symbol"]) for item in approved_items}
    )
    try:
        collisions = db.scalars(
            select(ExecutionPosition.id)
            .where(
                ExecutionPosition.id.not_in(approved_ids),
                func.lower(func.trim(ExecutionPosition.mode))
                == settings.runtime_mode.strip().lower(),
                func.lower(func.trim(ExecutionPosition.provider))
                == provider.strip().lower(),
                func.lower(func.trim(ExecutionPosition.account_scope))
                == account_scope.strip().lower(),
                func.upper(func.trim(ExecutionPosition.symbol)).in_(symbols),
            )
            .order_by(ExecutionPosition.id.asc())
            .with_for_update(nowait=True)
        ).all()
    except DBAPIError as exc:
        raise PositionIdentityConflictError(
            "legacy position attribution collision check is busy; "
            "retry from a new dry-run"
        ) from exc
    if collisions:
        raise PositionIdentityConflictError(
            "legacy position attribution found a scoped route symbol collision"
        )


def _required_attribution_snapshot_observed_at(
    payload: dict[str, object],
) -> datetime:
    raw_value = payload.get("broker_snapshot_observed_at")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise PositionIdentityConflictError(
            "legacy position attribution snapshot timestamp is unavailable"
        )
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PositionIdentityConflictError(
            "legacy position attribution snapshot timestamp is invalid"
        ) from exc
    return parsed


def _attributed_position_payload(
    *,
    payload: object,
    identity: PositionIdentity,
    quarantine_attribution: bool,
) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise PositionIdentityConflictError(
            "legacy position attribution payload is not a mapping"
        )
    attributed_payload = payload_with_position_identity(
        payload,
        identity=identity,
    )
    if not quarantine_attribution:
        return attributed_payload

    if "_position_sync" in attributed_payload:
        prior_sync = attributed_payload.pop("_position_sync")
        if prior_sync is not None and not isinstance(prior_sync, dict):
            raise PositionIdentityConflictError(
                "legacy position sync evidence is not a mapping"
            )
        if (
            "legacy_position_sync" in attributed_payload
            and attributed_payload["legacy_position_sync"] != prior_sync
        ):
            raise PositionIdentityConflictError(
                "legacy position attribution cannot preserve conflicting sync evidence"
            )
        attributed_payload["legacy_position_sync"] = prior_sync
    attributed_payload["_position_sync"] = {
        "asset_class": "us_equity",
        "exchange": identity.exchange,
        "monitoring_eligible": False,
        "trade_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unsupported_overseas_exchange",
        "route_disposition": LEGACY_QUARANTINE_ROUTE_DISPOSITION,
    }
    return attributed_payload


def _require_alpaca_crypto_flat_rebind_route(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
) -> None:
    if settings.runtime_mode != "paper":
        raise PositionIdentityConflictError(
            "Alpaca crypto flat rebind supports only paper mode"
        )
    if settings.runtime_broker_provider != "alpaca":
        raise PositionIdentityConflictError(
            "Alpaca crypto flat rebind requires runtime broker provider alpaca"
        )
    if str(adapter.provider or "").strip().lower() != "alpaca":
        raise PositionIdentityConflictError(
            "Alpaca crypto flat rebind requires an Alpaca adapter"
        )


def _configured_alpaca_crypto_symbols(settings: Settings) -> set[str]:
    configured: set[str] = set()
    for raw_symbol in settings.alpaca_crypto_allowed_symbols.split(","):
        stripped = raw_symbol.strip().upper().replace("-", "/")
        if not stripped:
            continue
        if "/" not in stripped and stripped.endswith("USD") and len(stripped) > 3:
            stripped = f"{stripped[:-3]}/USD"
        parts = stripped.split("/")
        if (
            len(parts) != 2
            or not all(parts)
            or any(not part.isalnum() for part in parts)
        ):
            raise PositionIdentityConflictError(
                "Alpaca crypto flat rebind requires canonical configured symbols"
            )
        configured.add(stripped)
    if not configured:
        raise PositionIdentityConflictError(
            "Alpaca crypto flat rebind requires configured crypto symbols"
        )
    return configured


def _require_alpaca_crypto_flat_snapshot(
    *,
    settings: Settings,
    snapshot: object,
) -> None:
    payload = getattr(snapshot, "payload", None)
    if (
        not isinstance(payload, dict)
        or payload.get("provider") != "alpaca"
        or payload.get("mode") != "paper"
        or payload.get("asset_class") != "crypto"
        or str(getattr(snapshot, "source", "") or "") != "alpaca:paper"
        or not str(getattr(snapshot, "broker_account_id", "") or "").strip()
        or getattr(snapshot, "holdings_complete", None) is not True
        or getattr(snapshot, "open_orders_complete", None) is not True
        or payload.get("holdings_coverage") != "complete"
        or payload.get("open_orders_coverage") != "complete"
        or settings.runtime_mode != "paper"
    ):
        raise PositionIdentityConflictError(
            "Alpaca paper crypto snapshot proof is unavailable or incomplete"
        )


def _unique_snapshot_symbols(
    *,
    rows: list[object],
    evidence_type: str,
    allow_repeated_symbols: bool = False,
) -> set[str]:
    symbols: list[str] = []
    for row in rows:
        raw_symbol = str(getattr(row, "symbol", "") or "").strip()
        normalized_symbol = _normalized_position_symbol(raw_symbol)
        if raw_symbol != normalized_symbol:
            raise PositionIdentityConflictError(
                f"Alpaca crypto snapshot {evidence_type} symbol is not canonical"
            )
        symbols.append(normalized_symbol)
    if not allow_repeated_symbols and len(set(symbols)) != len(symbols):
        raise PositionIdentityConflictError(
            f"Alpaca crypto snapshot contains duplicate {evidence_type} symbols"
        )
    return set(symbols)


def _alpaca_crypto_flat_items_for_snapshot(
    *,
    settings: Settings,
    db: Session,
    positions: list[ExecutionPosition],
    snapshot: object,
    configured_symbols: set[str],
) -> tuple[PositionIdentity, list[dict[str, object]]]:
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange=ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
        broker_account_id=str(getattr(snapshot, "broker_account_id", "") or ""),
    )
    normalized_symbols = [
        _normalized_position_symbol(position.symbol) for position in positions
    ]
    holding_symbols = _unique_snapshot_symbols(
        rows=list(getattr(snapshot, "holdings", []) or []),
        evidence_type="holding",
    )
    open_order_symbols = _unique_snapshot_symbols(
        rows=list(getattr(snapshot, "open_orders", []) or []),
        evidence_type="open order",
        allow_repeated_symbols=True,
    )
    scoped_symbols = set(
        db.scalars(
            select(func.upper(func.trim(ExecutionPosition.symbol))).where(
                func.lower(func.trim(ExecutionPosition.mode))
                == settings.runtime_mode.strip().lower(),
                func.lower(func.trim(ExecutionPosition.provider))
                == identity.provider,
                func.lower(func.trim(ExecutionPosition.account_scope))
                == identity.account_scope,
                func.upper(func.trim(ExecutionPosition.symbol)).in_(
                    sorted(normalized_symbols)
                ),
            )
        ).all()
    )
    absence_proof_digest = _alpaca_crypto_absence_proof_digest(
        settings=settings,
        snapshot=snapshot,
        identity=identity,
        requested_symbols=normalized_symbols,
        holding_symbols=holding_symbols,
        open_order_symbols=open_order_symbols,
    )
    items = [
        _alpaca_crypto_flat_rebind_item(
            settings=settings,
            position=position,
            identity=identity,
            configured_symbols=configured_symbols,
            holding_present=(
                _normalized_position_symbol(position.symbol) in holding_symbols
            ),
            open_order_present=(
                _normalized_position_symbol(position.symbol)
                in open_order_symbols
            ),
            scoped_route_exists=(
                _normalized_position_symbol(position.symbol) in scoped_symbols
            ),
            absence_proof_digest=absence_proof_digest,
        )
        for position in positions
    ]
    return identity, items


def _alpaca_crypto_absence_proof_digest(
    *,
    settings: Settings,
    snapshot: object,
    identity: PositionIdentity,
    requested_symbols: list[str],
    holding_symbols: set[str],
    open_order_symbols: set[str],
) -> str:
    material = {
        "proof_version": 1,
        "safety_policy_version": (
            ALPACA_CRYPTO_FLAT_REBIND_SAFETY_POLICY_VERSION
        ),
        "runtime_mode": settings.runtime_mode,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
        "exchange": identity.exchange,
        "asset_class": "crypto",
        "snapshot_source": str(getattr(snapshot, "source", "") or ""),
        "holdings_complete": getattr(snapshot, "holdings_complete", None) is True,
        "open_orders_complete": (
            getattr(snapshot, "open_orders_complete", None) is True
        ),
        "requested_symbols": sorted(requested_symbols),
        "holding_symbols": sorted(holding_symbols),
        "open_order_symbols": sorted(open_order_symbols),
    }
    return hashlib.sha256(
        json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _alpaca_crypto_flat_rebind_item(
    *,
    settings: Settings,
    position: ExecutionPosition,
    identity: PositionIdentity,
    configured_symbols: set[str],
    holding_present: bool,
    open_order_present: bool,
    scoped_route_exists: bool,
    absence_proof_digest: str,
) -> dict[str, object]:
    symbol = _normalized_position_symbol(position.symbol)
    raw_payload_is_mapping = isinstance(position.raw_payload, dict)
    stored_route_identity_present = (
        raw_payload_is_mapping and POSITION_IDENTITY_KEY in position.raw_payload
    )
    integer_quantity = _canonical_attribution_decimal(position.quantity)
    base_quantity = _canonical_attribution_decimal(position.base_quantity)
    average_price = _canonical_attribution_decimal(position.average_price)
    base_quantity_is_flat = (
        position.base_quantity is None or base_quantity == "0"
    )
    flat_quantity = integer_quantity == "0" and base_quantity_is_flat
    average_price_is_valid = (
        position.average_price is None or average_price is not None
    )

    if not raw_payload_is_mapping:
        reason = "legacy_payload_invalid"
    elif str(position.symbol or "").strip() != symbol:
        reason = "legacy_symbol_not_canonical"
    elif symbol not in configured_symbols or "/" not in symbol:
        reason = "unsupported_alpaca_crypto_symbol"
    elif stored_route_identity_present:
        reason = "stored_route_identity_present"
    elif not flat_quantity:
        reason = "legacy_position_not_flat"
    elif not average_price_is_valid:
        reason = "legacy_average_price_invalid"
    elif scoped_route_exists:
        reason = "scoped_route_symbol_collision"
    elif holding_present:
        reason = "broker_holding_present"
    elif open_order_present:
        reason = "broker_open_order_present"
    else:
        reason = "current_broker_flat_truth_proven"

    eligible = reason == "current_broker_flat_truth_proven"
    evidence = {
        "safety_policy_version": (
            ALPACA_CRYPTO_FLAT_REBIND_SAFETY_POLICY_VERSION
        ),
        "mode": settings.runtime_mode,
        "position_id": position.id,
        "symbol": symbol,
        "updated_from": position.updated_from,
        "last_reconciled_at": _serialize_datetime(position.last_reconciled_at),
        "legacy_payload_digest": _legacy_payload_digest(position.raw_payload),
        "legacy_integer_quantity": integer_quantity,
        "legacy_base_quantity": base_quantity,
        "legacy_average_price": average_price,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
        "exchange": identity.exchange,
        "asset_class": "crypto",
        "holding_absent": not holding_present,
        "open_order_absent": not open_order_present,
        "absence_proof_digest": absence_proof_digest,
        "historical_account_attributed": False,
        "current_flat_truth_bound": True,
    }
    fingerprint = (
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
        "symbol": symbol,
        "updated_from": evidence["updated_from"],
        "last_reconciled_at": evidence["last_reconciled_at"],
        "legacy_payload_digest": evidence["legacy_payload_digest"],
        "legacy_integer_quantity": integer_quantity,
        "legacy_base_quantity": base_quantity,
        "legacy_average_price": average_price,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
        "exchange": identity.exchange,
        "asset_class": "crypto",
        "holding_absent": not holding_present,
        "open_order_absent": not open_order_present,
        "absence_proof_digest": absence_proof_digest,
        "historical_account_attributed": False,
        "current_flat_truth_bound": True,
        "eligible": eligible,
        "reason": reason,
        "evidence_fingerprint": fingerprint,
        "safety_policy_version": (
            ALPACA_CRYPTO_FLAT_REBIND_SAFETY_POLICY_VERSION
        ),
    }


def _alpaca_crypto_flat_rebind_payload(
    *,
    mode: str,
    settings: Settings,
    evaluated_at: datetime,
    snapshot_observed_at: datetime | None,
    snapshot_source: str | None,
    items: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "mode": mode,
        "runtime_mode": settings.runtime_mode,
        "provider": "alpaca",
        "exchange": ALPACA_CRYPTO_FLAT_REBIND_EXCHANGE,
        "asset_class": "crypto",
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": _serialize_datetime(
            snapshot_observed_at
        ),
        "broker_snapshot_source": snapshot_source,
        "summary": {
            "candidate_count": len(items),
            "eligible_count": sum(item["eligible"] is True for item in items),
            "manual_review_count": sum(
                item["eligible"] is not True for item in items
            ),
        },
        "items": items,
    }


def _locked_alpaca_crypto_flat_position_matches(
    *,
    position: ExecutionPosition,
    item: dict[str, object],
) -> bool:
    return bool(
        _normalized_position_symbol(position.symbol) == item["symbol"]
        and position.updated_from == item["updated_from"]
        and _serialize_datetime(position.last_reconciled_at)
        == item["last_reconciled_at"]
        and _legacy_payload_digest(position.raw_payload)
        == item["legacy_payload_digest"]
        and _canonical_attribution_decimal(position.quantity)
        == item["legacy_integer_quantity"]
        and _canonical_attribution_decimal(position.base_quantity)
        == item["legacy_base_quantity"]
        and _canonical_attribution_decimal(position.average_price)
        == item["legacy_average_price"]
    )


def _alpaca_crypto_current_flat_truth_payload(
    *,
    settings: Settings,
    identity: PositionIdentity,
    snapshot_source: str,
    snapshot_observed_at: datetime,
    absence_proof_digest: str,
) -> dict[str, object]:
    return payload_with_position_identity(
        {
            "provider": identity.provider,
            "mode": settings.runtime_mode,
            "asset_class": "crypto",
            "exchange": identity.exchange,
            "snapshot_source": snapshot_source,
            "snapshot_observed_at": _serialize_datetime(
                snapshot_observed_at
            ),
            "holdings_coverage": "complete",
            "open_orders_coverage": "complete",
            "holding_absent": True,
            "open_order_absent": True,
            "absence_proof_digest": absence_proof_digest,
            "historical_account_attributed": False,
            "current_flat_truth_bound": True,
        },
        identity=identity,
    )


def _validated_alpaca_crypto_flat_approvals(
    approved_evidence: dict[str, str],
) -> dict[str, str]:
    if not isinstance(approved_evidence, dict) or not approved_evidence:
        raise ValueError(
            "Alpaca crypto flat rebind requires explicit approved evidence"
        )
    normalized: dict[str, str] = {}
    for raw_position_id, raw_fingerprint in approved_evidence.items():
        position_id = str(raw_position_id or "").strip()
        fingerprint = str(raw_fingerprint or "").strip().lower()
        if (
            not position_id
            or position_id in normalized
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            raise ValueError("Alpaca crypto flat rebind approval is invalid")
        normalized[position_id] = fingerprint
    return normalized


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
    if normalized not in (
        SUPPORTED_POSITION_EXCHANGES
        | ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES
        | LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
    ):
        raise ValueError(
            "legacy position attribution requires a supported exact exchange"
        )
    return normalized


def _require_alpaca_paper_us_equity_snapshot(
    *,
    settings: Settings,
    snapshot: object,
) -> None:
    payload = getattr(snapshot, "payload", None)
    if (
        not isinstance(payload, dict)
        or payload.get("provider") != "alpaca"
        or payload.get("mode") != settings.runtime_mode
        or payload.get("asset_class") != "us_equity"
        or not str(getattr(snapshot, "broker_account_id", "") or "").strip()
    ):
        raise PositionIdentityConflictError(
            "Alpaca paper US-equity snapshot proof is unavailable"
        )


def _require_legacy_quarantine_snapshot(snapshot: object) -> str:
    payload = getattr(snapshot, "payload", None)
    if (
        not isinstance(payload, dict)
        or payload.get("query_scope") != KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE
        or payload.get("evidence_type") != "holdings_only"
        or payload.get("exchange") != "US_ALL"
        or getattr(snapshot, "open_orders", None) != []
        or getattr(snapshot, "open_orders_complete", None) is not False
    ):
        raise PositionIdentityConflictError(
            "legacy quarantine attribution holdings-only query proof is unavailable"
        )
    return KIS_LEGACY_US_HOLDINGS_QUERY_SCOPE


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
    query_scope: str,
    quarantine_attribution: bool,
    snapshot_proof_digest: str,
    safety_invariants: dict[str, object],
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
    if not isinstance(position.raw_payload, dict):
        reason = "legacy_payload_invalid"
    elif (
        provider == "alpaca"
        and not _alpaca_paper_legacy_position_provenance_is_valid(position)
    ):
        reason = "legacy_route_proof_missing"
    elif str(position.symbol or "").strip() != _normalized_position_symbol(
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
    elif not _legacy_quantity_representation_is_supported(
        position=position,
        provider=provider,
        exchange=exchange,
    ):
        reason = "unsupported_quantity_representation"
    elif legacy_quantity <= 0:
        reason = "legacy_position_not_positive"
    elif holding is None:
        reason = "broker_holding_missing"
    elif (
        provider == "alpaca"
        and not _alpaca_us_equity_holding_provenance_is_valid(holding)
    ):
        reason = "broker_holding_route_proof_missing"
    elif provider == "kis" and exchange != "KRX" and holding_exchange is None:
        reason = "broker_holding_exchange_missing"
    elif (
        provider == "kis"
        and holding_exchange is not None
        and holding_exchange != exchange
    ):
        reason = "broker_holding_exchange_mismatch"
    elif broker_quantity != legacy_quantity:
        reason = "quantity_mismatch"
    elif not (
        _positive_finite_attribution_decimal(legacy_average_price)
        and _positive_finite_attribution_decimal(broker_average_price)
    ):
        reason = "average_price_missing"
    elif not _legacy_average_price_matches(
        provider=provider,
        legacy_average_price=legacy_average_price,
        broker_average_price=broker_average_price,
    ):
        reason = "average_price_mismatch"
    else:
        reason = "exact_broker_holding_match"

    eligible = reason == "exact_broker_holding_match"
    evidence = {
        "safety_policy_version": LEGACY_ATTRIBUTION_SAFETY_POLICY_VERSION,
        "safety_invariants": safety_invariants,
        "snapshot_proof_digest": snapshot_proof_digest,
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
    if quarantine_attribution:
        evidence.update(
            {
                "query_scope": query_scope,
                "disposition": LEGACY_QUARANTINE_ROUTE_DISPOSITION,
            }
        )
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
    item = {
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
        "safety_policy_version": LEGACY_ATTRIBUTION_SAFETY_POLICY_VERSION,
        "safety_invariants": dict(safety_invariants),
        "snapshot_proof_digest": snapshot_proof_digest,
    }
    if quarantine_attribution:
        item.update(
            {
                "attribution_eligible": eligible,
                "trading_eligible": False,
                "monitoring_eligible": False,
                "manual_review_required": True,
                "disposition": LEGACY_QUARANTINE_ROUTE_DISPOSITION,
                "query_scope": query_scope,
            }
        )
    return item


def _legacy_attribution_safety_invariants(
    *,
    settings: Settings,
    provider: str,
    quarantine_attribution: bool,
) -> dict[str, object]:
    return {
        "account_scope_source": (
            legacy_attribution_account_scope_source(settings=settings)
            if quarantine_attribution
            else "position_identity_policy"
        ),
        "broker_actions_allowed": False,
        "collision_scope": "mode_provider_account_symbol_any_exchange",
        "provider": provider,
        "fractional_quantity_allowed": provider == "alpaca",
        "average_price_match_policy": (
            ALPACA_AVERAGE_PRICE_MATCH_POLICY
            if provider == "alpaca"
            else "exact_decimal_v1"
        ),
        "manual_review_required": quarantine_attribution,
        "monitoring_allowed": not quarantine_attribution,
        "route_disposition": (
            LEGACY_QUARANTINE_ROUTE_DISPOSITION
            if quarantine_attribution
            else "account_attributed_supported_route"
        ),
    }


def _legacy_quantity_representation_is_supported(
    *,
    position: ExecutionPosition,
    provider: str,
    exchange: str,
) -> bool:
    if provider != "alpaca":
        return position.base_quantity is None
    if exchange != "US" or position.quantity != 0 or position.base_quantity is None:
        return False
    base_quantity = Decimal(position.base_quantity)
    return base_quantity.is_finite() and base_quantity > 0


def _positive_finite_attribution_decimal(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _legacy_average_price_matches(
    *,
    provider: str,
    legacy_average_price: Decimal,
    broker_average_price: Decimal,
) -> bool:
    if provider != "alpaca":
        return broker_average_price == legacy_average_price
    try:
        stored_legacy = legacy_average_price.quantize(
            EXECUTION_POSITION_AVERAGE_PRICE_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
        stored_broker = broker_average_price.quantize(
            EXECUTION_POSITION_AVERAGE_PRICE_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
    except InvalidOperation:
        return False
    half_quantum = EXECUTION_POSITION_AVERAGE_PRICE_QUANTUM / 2
    return (
        legacy_average_price == stored_legacy
        and stored_broker == stored_legacy
        and abs(broker_average_price - legacy_average_price) < half_quantum
    )


def _alpaca_us_equity_holding_provenance_is_valid(
    holding: BrokerAccountHolding,
) -> bool:
    payload = holding.payload if isinstance(holding.payload, dict) else {}
    return (
        payload.get("provider") == "alpaca"
        and payload.get("asset_class") == "us_equity"
        and holding.quantity == 0
        and payload.get("base_quantity") is not None
    )


def _alpaca_paper_legacy_position_provenance_is_valid(
    position: ExecutionPosition,
) -> bool:
    payload = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    return (
        payload.get("provider") == "alpaca"
        and payload.get("mode") == "paper"
        and payload.get("asset_class") == "us_equity"
    )


def legacy_attribution_account_scope_source(
    *,
    settings: Settings,
) -> str | None:
    if stable_account_scope_hmac_key(settings) is not None:
        return "stable_hmac"
    if str(settings.execution_position_account_scope or "").strip():
        return "configured_route_label"
    return None


def _legacy_snapshot_proof_digest(
    *,
    snapshot: object,
    exchange: str,
    query_scope: str,
) -> str:
    holdings = getattr(snapshot, "holdings", None)
    if not isinstance(holdings, list):
        holdings = []
    canonical_holdings = sorted(
        (
            {
                "symbol": _normalized_position_symbol(holding.symbol),
                "quantity": _canonical_attribution_decimal(
                    _attribution_holding_quantity(holding)
                ),
                "average_price": _canonical_attribution_decimal(
                    holding.average_price
                ),
                "exchange": _reported_holding_exchange(holding),
            }
            for holding in holdings
        ),
        key=lambda item: (
            str(item["symbol"]),
            str(item["exchange"] or ""),
        ),
    )
    material = {
        "proof_version": 1,
        "source": str(getattr(snapshot, "source", "") or ""),
        "query_scope": query_scope,
        "requested_exchange": exchange,
        "provider": (
            getattr(snapshot, "payload", {}).get("provider")
            if isinstance(getattr(snapshot, "payload", None), dict)
            else None
        ),
        "runtime_mode": (
            getattr(snapshot, "payload", {}).get("mode")
            if isinstance(getattr(snapshot, "payload", None), dict)
            else None
        ),
        "asset_class": (
            getattr(snapshot, "payload", {}).get("asset_class")
            if isinstance(getattr(snapshot, "payload", None), dict)
            else None
        ),
        "reported_exchange": _reported_attribution_exchange(
            getattr(snapshot, "payload", None)
        ),
        "holdings_complete": getattr(snapshot, "holdings_complete", None) is True,
        "holdings": canonical_holdings,
    }
    return hashlib.sha256(
        json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


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
    quarantine_candidate = exchange in LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
    quarantined_on_apply_count = sum(
        item["eligible"] is True
        and item.get("disposition") == LEGACY_QUARANTINE_ROUTE_DISPOSITION
        for item in items
    )
    manual_review_count = sum(
        item["eligible"] is not True
        or item.get("manual_review_required") is True
        for item in items
    )
    summary = {
        "candidate_count": len(items),
        "eligible_count": eligible_count,
        "manual_review_count": manual_review_count,
    }
    if quarantine_candidate:
        summary["quarantined_on_apply_count"] = quarantined_on_apply_count
    return {
        "mode": mode,
        "runtime_mode": settings.runtime_mode,
        "provider": items[0]["provider"] if items else None,
        "exchange": exchange,
        "generated_at": _serialize_datetime(evaluated_at),
        "broker_snapshot_observed_at": _serialize_datetime(snapshot_observed_at),
        "summary": summary,
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
