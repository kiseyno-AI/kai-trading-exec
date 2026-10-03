from __future__ import annotations

import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from typing import Any, Protocol

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from app.broker.base import BrokerAccountSnapshot
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.fill_idempotency import (
    dedupe_execution_fills,
    execution_fill_dedupe_key,
    execution_fills_have_conflicting_identity,
)
from app.services.fifo_inventory import FifoRouteScope, fifo_route_scope_from_order
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
)
from app.services.position_truth_remediation import (
    _acquire_legacy_attribution_apply_serialization,
)


PROVIDER = "alpaca"
EXCHANGE = "US"
ASSET_CLASS = "us_equity"
STRATEGY_FAMILY = "us_equity:US"
POLICY_VERSION = "alpaca-us-legacy-strategy-exposure-recovery-v1"
CANDIDATE_TYPE = "alpaca_us_legacy_strategy_exposure"
OUTCOME = "opening_inventory_seeded"
MAX_EVIDENCE_AGE_SECONDS = 300
MAX_FUTURE_SKEW_SECONDS = 5
ACTIVE_ORDER_STATUSES = frozenset(
    {
        "accepted",
        "new",
        "open",
        "partially_filled",
        "pending_new",
        "submission_pending",
        "submitted",
    }
)
ORDER_PROOF_TERMINAL_STATUSES = frozenset(
    {"filled", "canceled", "rejected", "failed", "expired"}
)
STRATEGY_DECISION_ORDER_STATUSES = frozenset({"submitted", "duplicate"})
AVERAGE_PRICE_QUANTUM = Decimal("0.0001")
QUANTITY_QUANTUM = Decimal("0.000000001")


class AlpacaLegacyRecoveryReadAdapter(Protocol):
    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        exchange: str = EXCHANGE,
    ) -> BrokerAccountSnapshot: ...

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = EXCHANGE,
        submitted_at: datetime | None = None,
        expected_symbol: str | None = None,
    ) -> dict[str, Any] | None: ...


def analyze_alpaca_us_legacy_strategy_exposure(
    *,
    settings: Settings,
    db: Session,
    adapter: AlpacaLegacyRecoveryReadAdapter,
    access_token: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Build read-only proof for the quarantined Alpaca paper US sleeve."""

    evaluated_at = _as_utc(now or datetime.now(UTC))
    symbols = _configured_symbols(settings)
    _require_supported_route(settings=settings, symbols=symbols)
    snapshot = adapter.get_account_snapshot(
        access_token,
        symbols=list(symbols),
        exchange=EXCHANGE,
    )
    account_scope = _validated_snapshot_account_scope(
        settings=settings,
        snapshot=snapshot,
        evaluated_at=evaluated_at,
        symbols=symbols,
    )

    positions = _load_exact_positions(
        settings=settings,
        db=db,
        account_scope=account_scope,
        symbols=symbols,
        snapshot=snapshot,
        evaluated_at=evaluated_at,
    )
    legacy_states, legacy_risk_states, legacy_decisions = _load_legacy_graph(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    _require_unseeded_scope(
        settings=settings,
        db=db,
        account_scope=account_scope,
        symbols=symbols,
    )
    all_orders = _load_local_orders(
        settings=settings,
        db=db,
        symbols=symbols,
    )
    orders = _local_exposure_orders(all_orders)
    route_scope = FifoRouteScope(
        provider=PROVIDER,
        account_scope=account_scope,
        venue=PROVIDER,
        asset_class=ASSET_CLASS,
        exchange=EXCHANGE,
    )
    authority_orders = legacy_strategy_authority_orders(
        orders=all_orders,
        order_origin="alpaca_us_equity_strategy",
        strategy_family=STRATEGY_FAMILY,
        route_scope=route_scope,
    )
    strategy_order_ids = _strategy_order_ids(
        decisions=legacy_decisions,
        orders=orders,
    )
    broker_proofs = _authenticated_order_proofs(
        adapter=adapter,
        access_token=access_token,
        orders=orders,
    )
    opening_inventory = _opening_inventory(
        snapshot=snapshot,
        symbols=symbols,
        orders=orders,
        strategy_order_ids=strategy_order_ids,
    )

    authority_fingerprint = legacy_strategy_authority_fingerprint(
        states=legacy_states,
        risk_states=legacy_risk_states,
        decisions=legacy_decisions,
        orders=authority_orders,
    )
    stable_evidence = {
        "policy_version": POLICY_VERSION,
        "mode": settings.runtime_mode,
        "provider": PROVIDER,
        "account_scope": account_scope,
        "asset_class": ASSET_CLASS,
        "exchange": EXCHANGE,
        "strategy_family": STRATEGY_FAMILY,
        "strategy_version": settings.alpaca_us_equity_strategy_version,
        "symbols": list(symbols),
        "authority_fingerprint": authority_fingerprint,
        "legacy_state_ids": sorted(row.id for row in legacy_states),
        "legacy_risk_state_ids": sorted(row.id for row in legacy_risk_states),
        "legacy_decision_ids": sorted(row.id for row in legacy_decisions),
        "authority_order_ids": sorted(row.id for row in authority_orders),
        "positions": [_position_material(row) for row in positions],
        "snapshot": _snapshot_material(snapshot=snapshot, symbols=symbols),
        "orders": [_order_material(row) for row in orders],
        "broker_order_proofs": broker_proofs,
        "strategy_order_ids": sorted(strategy_order_ids),
        "baseline_fill_identity_keys": sorted(
            {
                execution_fill_dedupe_key(fill)
                for order in orders
                for fill in dedupe_execution_fills(order.fills)
            }
        ),
        "opening_inventory": opening_inventory,
    }
    approval_fingerprint = _fingerprint(stable_evidence)
    return {
        "mode": "dry-run",
        "eligible": True,
        "policy_version": POLICY_VERSION,
        "provider": PROVIDER,
        "account_scope": account_scope,
        "asset_class": ASSET_CLASS,
        "exchange": EXCHANGE,
        "strategy_family": STRATEGY_FAMILY,
        "strategy_version": settings.alpaca_us_equity_strategy_version,
        "symbols": list(symbols),
        "evaluated_at": evaluated_at.isoformat(),
        "snapshot_observed_at": _as_utc(snapshot.observed_at).isoformat(),
        "authority_fingerprint": authority_fingerprint,
        "approval_fingerprint": approval_fingerprint,
        "opening_inventory": opening_inventory,
        "legacy_state_ids": stable_evidence["legacy_state_ids"],
        "legacy_risk_state_ids": stable_evidence["legacy_risk_state_ids"],
        "legacy_decision_ids": stable_evidence["legacy_decision_ids"],
        "authority_order_ids": stable_evidence["authority_order_ids"],
        "legacy_order_ids": [row.id for row in orders],
        "baseline_fill_identity_keys": stable_evidence[
            "baseline_fill_identity_keys"
        ],
        "evidence": stable_evidence,
    }


def apply_alpaca_us_legacy_strategy_exposure(
    *,
    settings: Settings,
    db: Session,
    adapter: AlpacaLegacyRecoveryReadAdapter,
    access_token: str,
    approved_fingerprint: str,
    actor: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, object]:
    """Seed scoped state only when current proof matches the dry-run exactly."""

    normalized_actor = str(actor or "").strip()
    normalized_reason = str(reason or "").strip()
    normalized_fingerprint = str(approved_fingerprint or "").strip().lower()
    if not normalized_actor or not normalized_reason:
        raise ValueError("Alpaca legacy recovery apply requires actor and reason")
    if (
        len(normalized_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in normalized_fingerprint)
    ):
        raise ValueError("Alpaca legacy recovery approval fingerprint is invalid")

    evaluated_at = _as_utc(now or datetime.now(UTC))
    serialization = None
    try:
        preliminary = analyze_alpaca_us_legacy_strategy_exposure(
            settings=settings,
            db=db,
            adapter=adapter,
            access_token=access_token,
            now=evaluated_at,
        )
        preliminary_fingerprint = str(preliminary["approval_fingerprint"])
        if not hmac.compare_digest(
            normalized_fingerprint,
            preliminary_fingerprint,
        ):
            raise PositionIdentityConflictError(
                "approved Alpaca legacy recovery evidence no longer matches current truth"
            )

        account_scope = str(preliminary["account_scope"])
        db.rollback()
        serialization = _acquire_legacy_attribution_apply_serialization(
            settings=settings,
            db=db,
            provider=PROVIDER,
            account_scope=account_scope,
            exchange=EXCHANGE,
        )
        _lock_recovery_rows(
            settings=settings,
            db=db,
            account_scope=account_scope,
            symbols=_configured_symbols(settings),
        )
        current = analyze_alpaca_us_legacy_strategy_exposure(
            settings=settings,
            db=db,
            adapter=adapter,
            access_token=access_token,
            now=evaluated_at,
        )
        current_fingerprint = str(current["approval_fingerprint"])
        if not hmac.compare_digest(
            normalized_fingerprint,
            current_fingerprint,
        ):
            raise PositionIdentityConflictError(
                "approved Alpaca legacy recovery evidence changed after locking"
            )
        authority_fingerprint = str(current["authority_fingerprint"])
        candidate_key = _recovery_candidate_key(
            settings=settings,
            account_scope=account_scope,
            authority_fingerprint=authority_fingerprint,
        )
        opening_inventory = current["opening_inventory"]
        assert isinstance(opening_inventory, dict)
        event_evidence = {
            **dict(current["evidence"]),
            "approval_fingerprint": current_fingerprint,
            "candidate_key": candidate_key,
            "applied_at": evaluated_at.isoformat(),
            "broker_mutations": 0,
            "orders_submitted": 0,
            "orders_amended": 0,
            "orders_canceled": 0,
            "legacy_rows_mutated": 0,
        }
        event = RuntimeRemediationEvent(
            mode=settings.runtime_mode,
            candidate_type=CANDIDATE_TYPE,
            candidate_key=candidate_key,
            subject_id=account_scope,
            outcome=OUTCOME,
            actor=normalized_actor,
            reason=normalized_reason,
            evidence_summary=event_evidence,
            occurred_at=evaluated_at,
        )
        db.add(event)
        for symbol in _configured_symbols(settings):
            inventory = opening_inventory[symbol]
            assert isinstance(inventory, dict)
            strategy_quantity = _required_decimal(
                inventory["strategy_base_quantity"]
            )
            db.add(
                CryptoStrategyState(
                    mode=settings.runtime_mode,
                    provider=PROVIDER,
                    account_scope=account_scope,
                    strategy_family=STRATEGY_FAMILY,
                    strategy_version=settings.alpaca_us_equity_strategy_version,
                    symbol=symbol,
                    eligible=False,
                    target_fraction=Decimal("0"),
                    strategy_owned_base_quantity=strategy_quantity,
                    last_action=OUTCOME,
                    last_action_at=evaluated_at,
                    metrics={
                        "legacy_strategy_recovery": {
                            "policy_version": POLICY_VERSION,
                            "candidate_key": candidate_key,
                            "approval_fingerprint": current_fingerprint,
                            "authority_fingerprint": authority_fingerprint,
                            "opening_strategy_base_quantity": inventory[
                                "strategy_base_quantity"
                            ],
                            "opening_manual_base_quantity": inventory[
                                "manual_base_quantity"
                            ],
                            "opening_broker_base_quantity": inventory[
                                "broker_base_quantity"
                            ],
                        }
                    },
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
        "status": "applied",
        "policy_version": POLICY_VERSION,
        "provider": PROVIDER,
        "account_scope": account_scope,
        "asset_class": ASSET_CLASS,
        "exchange": EXCHANGE,
        "strategy_family": STRATEGY_FAMILY,
        "strategy_version": settings.alpaca_us_equity_strategy_version,
        "candidate_key": candidate_key,
        "approval_fingerprint": current_fingerprint,
        "opening_inventory": opening_inventory,
        "broker_mutations": 0,
    }


def _lock_recovery_rows(
    *,
    settings: Settings,
    db: Session,
    account_scope: str,
    symbols: tuple[str, ...],
) -> None:
    """Refresh and lock every local row that participates in the proof."""

    statements = (
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == PROVIDER,
            ExecutionPosition.account_scope == account_scope,
            ExecutionPosition.exchange == EXCHANGE,
            ExecutionPosition.symbol.in_(symbols),
        ),
        select(CryptoStrategyState).where(
            CryptoStrategyState.mode == settings.runtime_mode,
            CryptoStrategyState.provider == PROVIDER,
            CryptoStrategyState.account_scope.is_(None),
            CryptoStrategyState.strategy_family.is_(None),
            CryptoStrategyState.symbol.in_(symbols),
        ),
        select(CryptoStrategyRiskState).where(
            CryptoStrategyRiskState.mode == settings.runtime_mode,
            CryptoStrategyRiskState.provider == PROVIDER,
            CryptoStrategyRiskState.account_scope.is_(None),
            CryptoStrategyRiskState.strategy_family.is_(None),
        ),
        select(CryptoStrategyDecision).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            CryptoStrategyDecision.account_scope.is_(None),
            CryptoStrategyDecision.strategy_family.is_(None),
            CryptoStrategyDecision.symbol.in_(symbols),
        ),
        select(BrokerOrder).where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == PROVIDER,
            BrokerOrder.asset_class == ASSET_CLASS,
            BrokerOrder.symbol.in_(symbols),
        ),
    )
    for statement in statements:
        db.scalars(
            statement.with_for_update(nowait=True).execution_options(
                populate_existing=True
            )
        ).all()


def legacy_strategy_authority_fingerprint(
    *,
    states: list[CryptoStrategyState],
    risk_states: list[CryptoStrategyRiskState],
    decisions: list[CryptoStrategyDecision],
    orders: list[BrokerOrder],
) -> str:
    material = {
        "states": [
            {
                "id": row.id,
                "strategy_version": row.strategy_version,
                "symbol": row.symbol,
                "eligible": row.eligible,
                "target_fraction": str(row.target_fraction),
                "strategy_owned_base_quantity": str(
                    row.strategy_owned_base_quantity
                ),
                "high_water_price": _optional_decimal_text(
                    row.high_water_price
                ),
                "protective_stop_price": _optional_decimal_text(
                    row.protective_stop_price
                ),
                "last_action": row.last_action,
                "last_action_at": _optional_datetime_text(row.last_action_at),
            }
            for row in sorted(states, key=lambda item: item.id)
        ],
        "risk_states": [
            {
                "id": row.id,
                "strategy_version": row.strategy_version,
                "risk_status": row.risk_status,
                "daily_baseline_equity": _optional_decimal_text(
                    row.daily_baseline_equity
                ),
                "weekly_baseline_equity": _optional_decimal_text(
                    row.weekly_baseline_equity
                ),
                "peak_equity": _optional_decimal_text(row.peak_equity),
                "current_equity": _optional_decimal_text(row.current_equity),
            }
            for row in sorted(risk_states, key=lambda item: item.id)
        ],
        "decisions": [
            {
                "id": row.id,
                "strategy_version": row.strategy_version,
                "symbol": row.symbol,
                "decision_bar_at": row.decision_bar_at.isoformat(),
                "action": row.action,
                "decision_status": row.decision_status,
                "requested_base_quantity": _optional_decimal_text(
                    row.requested_base_quantity
                ),
                "requested_quote_notional": _optional_decimal_text(
                    row.requested_quote_notional
                ),
                "idempotency_key": row.idempotency_key,
            }
            for row in sorted(decisions, key=lambda item: item.id)
        ],
        "orders": [
            _legacy_authority_order_material(row)
            for row in sorted(orders, key=lambda item: item.id)
        ],
    }
    return _fingerprint(material)


def legacy_strategy_authority_orders(
    *,
    orders: list[BrokerOrder],
    order_origin: str,
    strategy_family: str,
    route_scope: FifoRouteScope,
) -> list[BrokerOrder]:
    """Select the exact order rows treated as unscoped runtime authority."""

    expected_scope = {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        **route_scope.as_payload(),
    }
    legacy_orders: list[BrokerOrder] = []
    for order in orders:
        request_metadata = (
            order.request_metadata
            if isinstance(order.request_metadata, dict)
            else {}
        )
        source = str(request_metadata.get("source") or "")
        if not (
            source == order_origin
            or source.startswith(f"{order_origin}:")
        ):
            continue
        stored_route = fifo_route_scope_from_order(order)
        if stored_route is None:
            legacy_orders.append(order)
            continue
        if stored_route != route_scope:
            continue
        command_metadata = request_metadata.get("metadata")
        command_metadata = (
            command_metadata if isinstance(command_metadata, dict) else {}
        )
        if (
            command_metadata.get("strategy_family") != strategy_family
            or command_metadata.get("expected_execution_scope")
            != expected_scope
        ):
            legacy_orders.append(order)
    return legacy_orders


def _legacy_authority_order_material(
    order: BrokerOrder,
) -> dict[str, object]:
    return {
        "id": order.id,
        "broker_order_id": order.broker_order_id,
        "symbol": order.symbol,
        "side": order.side,
        "status": order.status,
        "quantity": order.quantity,
        "base_quantity": _optional_decimal_text(order.base_quantity),
        "quote_notional": _optional_decimal_text(order.quote_notional),
        "request_metadata": order.request_metadata,
        "fills": [
            {
                "id": fill.id,
                "symbol": fill.symbol,
                "side": fill.side,
                "quantity": fill.quantity,
                "base_quantity": _optional_decimal_text(
                    fill.base_quantity
                ),
                "quote_notional": _optional_decimal_text(
                    fill.quote_notional
                ),
                "price": str(fill.price),
                "filled_at": fill.filled_at.isoformat(),
            }
            for fill in sorted(order.fills, key=lambda item: item.id)
        ],
    }


def _require_supported_route(
    *,
    settings: Settings,
    symbols: tuple[str, ...],
) -> None:
    if (
        settings.runtime_mode != "paper"
        or str(settings.runtime_broker_provider or "").strip().lower()
        != PROVIDER
    ):
        raise PositionIdentityConflictError(
            "Alpaca legacy strategy exposure recovery requires the paper Alpaca runtime"
        )
    if not symbols:
        raise PositionIdentityConflictError(
            "Alpaca legacy strategy exposure recovery requires configured symbols"
        )


def _configured_symbols(settings: Settings) -> tuple[str, ...]:
    raw = str(settings.alpaca_us_equity_allowed_symbols or "")
    symbols = tuple(
        sorted({item.strip().upper() for item in raw.split(",") if item.strip()})
    )
    if any("/" in symbol for symbol in symbols):
        raise PositionIdentityConflictError(
            "Alpaca US-equity recovery received a non-equity symbol"
        )
    return symbols


def _validated_snapshot_account_scope(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    evaluated_at: datetime,
    symbols: tuple[str, ...],
) -> str:
    broker_account_id = str(snapshot.broker_account_id or "").strip()
    if not broker_account_id:
        raise PositionIdentityConflictError(
            "Alpaca recovery requires an authenticated broker account identity"
        )
    age_seconds = (evaluated_at - _as_utc(snapshot.observed_at)).total_seconds()
    if not (
        -MAX_FUTURE_SKEW_SECONDS
        <= age_seconds
        <= MAX_EVIDENCE_AGE_SECONDS
    ):
        raise PositionIdentityConflictError(
            "Alpaca recovery requires a fresh account snapshot"
        )
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    if (
        not snapshot.holdings_complete
        or not snapshot.open_orders_complete
        or payload.get("holdings_coverage") != "complete"
        or payload.get("open_orders_coverage") != "complete"
        or payload.get("provider") != PROVIDER
        or payload.get("mode") != "paper"
        or payload.get("asset_class") != ASSET_CLASS
    ):
        raise PositionIdentityConflictError(
            "Alpaca recovery requires complete paper US-equity broker truth"
        )
    expected_symbols = set(symbols)
    holding_symbols = [str(row.symbol or "").strip().upper() for row in snapshot.holdings]
    if len(holding_symbols) != len(set(holding_symbols)):
        raise PositionIdentityConflictError(
            "Alpaca recovery snapshot contains duplicate holdings"
        )
    if set(holding_symbols) != expected_symbols:
        raise PositionIdentityConflictError(
            "Alpaca recovery snapshot must contain the exact configured holdings"
        )
    if any(
        str(row.symbol or "").strip().upper() in expected_symbols
        for row in snapshot.open_orders
    ):
        raise PositionIdentityConflictError(
            "Alpaca recovery requires no open orders for the recovered symbols"
        )
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider=PROVIDER,
        exchange=EXCHANGE,
        broker_account_id=broker_account_id,
    )
    return identity.account_scope


def _load_exact_positions(
    *,
    settings: Settings,
    db: Session,
    account_scope: str,
    symbols: tuple[str, ...],
    snapshot: BrokerAccountSnapshot,
    evaluated_at: datetime,
) -> list[ExecutionPosition]:
    positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.provider == PROVIDER,
            ExecutionPosition.account_scope == account_scope,
            ExecutionPosition.exchange == EXCHANGE,
            ExecutionPosition.symbol.in_(symbols),
        )
        .order_by(ExecutionPosition.symbol.asc(), ExecutionPosition.id.asc())
    ).all()
    if [row.symbol for row in positions] != list(symbols):
        raise PositionIdentityConflictError(
            "Alpaca recovery requires one exact scoped position per symbol"
        )
    holdings = {
        str(row.symbol).strip().upper(): row for row in snapshot.holdings
    }
    for position in positions:
        if position.last_reconciled_at is None:
            raise PositionIdentityConflictError(
                "Alpaca recovery requires fresh scoped positions"
            )
        age_seconds = (
            evaluated_at - _as_utc(position.last_reconciled_at)
        ).total_seconds()
        if not (
            -MAX_FUTURE_SKEW_SECONDS
            <= age_seconds
            <= MAX_EVIDENCE_AGE_SECONDS
        ):
            raise PositionIdentityConflictError(
                "Alpaca recovery requires fresh scoped positions"
            )
        holding = holdings[position.symbol]
        broker_quantity = _holding_quantity(holding.payload)
        if Decimal(position.base_quantity or 0) != broker_quantity:
            raise PositionIdentityConflictError(
                "Alpaca scoped position quantity does not match broker truth"
            )
        stored_average = _optional_decimal(position.average_price)
        broker_average = _optional_decimal(holding.average_price)
        if (
            stored_average is None
            or broker_average is None
            or stored_average.quantize(
                AVERAGE_PRICE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
            != broker_average.quantize(
                AVERAGE_PRICE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
        ):
            raise PositionIdentityConflictError(
                "Alpaca scoped position average price does not match broker truth"
            )
    return positions


def _load_legacy_graph(
    *,
    settings: Settings,
    db: Session,
    symbols: tuple[str, ...],
) -> tuple[
    list[CryptoStrategyState],
    list[CryptoStrategyRiskState],
    list[CryptoStrategyDecision],
]:
    states = db.scalars(
        select(CryptoStrategyState).where(
            CryptoStrategyState.mode == settings.runtime_mode,
            CryptoStrategyState.provider == PROVIDER,
            CryptoStrategyState.account_scope.is_(None),
            CryptoStrategyState.strategy_family.is_(None),
            CryptoStrategyState.symbol.in_(symbols),
        )
    ).all()
    decisions = db.scalars(
        select(CryptoStrategyDecision).where(
            CryptoStrategyDecision.mode == settings.runtime_mode,
            CryptoStrategyDecision.provider == PROVIDER,
            CryptoStrategyDecision.account_scope.is_(None),
            CryptoStrategyDecision.strategy_family.is_(None),
            CryptoStrategyDecision.symbol.in_(symbols),
        )
    ).all()
    if {row.symbol for row in states} != set(symbols) or not decisions:
        raise PositionIdentityConflictError(
            "Alpaca recovery requires the complete legacy strategy graph"
        )
    relevant_versions = {
        *(row.strategy_version for row in states),
        *(row.strategy_version for row in decisions),
    }
    risks = db.scalars(
        select(CryptoStrategyRiskState).where(
            CryptoStrategyRiskState.mode == settings.runtime_mode,
            CryptoStrategyRiskState.provider == PROVIDER,
            CryptoStrategyRiskState.account_scope.is_(None),
            CryptoStrategyRiskState.strategy_family.is_(None),
            CryptoStrategyRiskState.strategy_version.in_(relevant_versions),
        )
    ).all()
    return states, risks, decisions


def _require_unseeded_scope(
    *,
    settings: Settings,
    db: Session,
    account_scope: str,
    symbols: tuple[str, ...],
) -> None:
    current = db.scalar(
        select(CryptoStrategyState.id).where(
            CryptoStrategyState.mode == settings.runtime_mode,
            CryptoStrategyState.provider == PROVIDER,
            CryptoStrategyState.account_scope == account_scope,
            CryptoStrategyState.strategy_family == STRATEGY_FAMILY,
            CryptoStrategyState.symbol.in_(symbols),
        )
    )
    if current is not None:
        raise PositionIdentityConflictError(
            "Alpaca recovery refuses to overwrite scoped strategy state"
        )


def _load_local_orders(
    *,
    settings: Settings,
    db: Session,
    symbols: tuple[str, ...],
) -> list[BrokerOrder]:
    candidates = db.scalars(
        select(BrokerOrder)
        .options(selectinload(BrokerOrder.fills))
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == PROVIDER,
            BrokerOrder.asset_class == ASSET_CLASS,
            BrokerOrder.symbol.in_(symbols),
        )
        .order_by(BrokerOrder.id.asc())
    ).all()
    for order in candidates:
        if execution_fills_have_conflicting_identity(order.fills):
            raise PositionIdentityConflictError(
                "Alpaca recovery found conflicting local fill identity"
            )
        if (
            not any(
                Decimal(fill.base_quantity or 0) > 0
                for fill in dedupe_execution_fills(order.fills)
            )
            and str(order.status or "").strip().lower()
            in ACTIVE_ORDER_STATUSES
        ):
            raise PositionIdentityConflictError(
                "Alpaca recovery found a locally active order"
            )
    return candidates


def _local_exposure_orders(
    candidates: list[BrokerOrder],
) -> list[BrokerOrder]:
    orders = [
        order
        for order in candidates
        if any(
            Decimal(fill.base_quantity or 0) > 0
            for fill in dedupe_execution_fills(order.fills)
        )
    ]
    if not orders:
        raise PositionIdentityConflictError(
            "Alpaca recovery found no local filled order evidence"
        )
    return orders


def _strategy_order_ids(
    *,
    decisions: list[CryptoStrategyDecision],
    orders: list[BrokerOrder],
) -> set[str]:
    by_key: dict[str, list[BrokerOrder]] = {}
    for order in orders:
        by_key.setdefault(str(order.idempotency_key), []).append(order)
    strategy_order_ids: set[str] = set()
    submitted = [
        row
        for row in decisions
        if str(row.decision_status or "").strip().lower()
        in STRATEGY_DECISION_ORDER_STATUSES
    ]
    if not submitted:
        raise PositionIdentityConflictError(
            "Alpaca recovery found no submitted legacy strategy decisions"
        )
    for decision in submitted:
        key = str(decision.idempotency_key or "").strip()
        matched = [
            order
            for order in by_key.get(key, [])
            if order.symbol == decision.symbol
        ]
        if not key or len(matched) != 1:
            raise PositionIdentityConflictError(
                "Legacy strategy decision does not identify one local order"
            )
        decision_side = str(decision.action or "").strip().lower()
        order_side = str(matched[0].side or "").strip().lower()
        if decision_side not in {"buy", "sell"} or decision_side != order_side:
            raise PositionIdentityConflictError(
                "Legacy strategy decision side does not match its local order"
            )
        strategy_order_ids.add(matched[0].id)
    return strategy_order_ids


def _authenticated_order_proofs(
    *,
    adapter: AlpacaLegacyRecoveryReadAdapter,
    access_token: str,
    orders: list[BrokerOrder],
) -> list[dict[str, object]]:
    proofs: list[dict[str, object]] = []
    for order in orders:
        broker_order_id = str(order.broker_order_id or "").strip()
        if not broker_order_id:
            raise PositionIdentityConflictError(
                "Alpaca recovery requires every filled order broker ID"
            )
        proof = adapter.reconcile_order(
            access_token,
            broker_order_id,
            exchange=EXCHANGE,
            submitted_at=order.submitted_at,
            expected_symbol=order.symbol,
        )
        if not isinstance(proof, dict):
            raise PositionIdentityConflictError(
                "Alpaca recovery could not authenticate a local broker order"
            )
        payload = proof.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        local_filled = sum(
            (
                Decimal(fill.base_quantity or 0)
                for fill in dedupe_execution_fills(order.fills)
            ),
            Decimal("0"),
        )
        proof_status = str(proof.get("status") or "").strip().lower()
        proof_quantity = _required_decimal(proof.get("filled_base_quantity"))
        if not (
            proof.get("provider") == PROVIDER
            and proof.get("broker_order_id") == broker_order_id
            and str(proof.get("symbol") or "").strip().upper() == order.symbol
            and proof_status in ORDER_PROOF_TERMINAL_STATUSES
            and proof_quantity == local_filled
            and payload.get("id") == broker_order_id
            and str(payload.get("symbol") or "").strip().upper() == order.symbol
            and str(payload.get("side") or "").strip().lower()
            == str(order.side or "").strip().lower()
            and payload.get("client_order_id") == order.idempotency_key
            and _required_decimal(payload.get("filled_qty")) == local_filled
        ):
            raise PositionIdentityConflictError(
                "Authenticated Alpaca order proof does not match local evidence"
            )
        proofs.append(
            {
                "local_order_id": order.id,
                "broker_order_id": broker_order_id,
                "idempotency_key": order.idempotency_key,
                "symbol": order.symbol,
                "side": order.side,
                "status": proof_status,
                "filled_base_quantity": _quantity_text(proof_quantity),
            }
        )
    return sorted(proofs, key=lambda item: str(item["local_order_id"]))


def _opening_inventory(
    *,
    snapshot: BrokerAccountSnapshot,
    symbols: tuple[str, ...],
    orders: list[BrokerOrder],
    strategy_order_ids: set[str],
) -> dict[str, dict[str, str]]:
    holdings = {
        str(row.symbol).strip().upper(): row for row in snapshot.holdings
    }
    totals = {symbol: Decimal("0") for symbol in symbols}
    strategy = {symbol: Decimal("0") for symbol in symbols}
    for order in orders:
        side = str(order.side or "").strip().lower()
        if side == "buy":
            direction = Decimal("1")
        elif side == "sell":
            direction = Decimal("-1")
        else:
            raise PositionIdentityConflictError(
                "Alpaca recovery found an unsupported local order side"
            )
        quantity = sum(
            (
                Decimal(fill.base_quantity or 0)
                for fill in dedupe_execution_fills(order.fills)
            ),
            Decimal("0"),
        )
        totals[order.symbol] += direction * quantity
        if order.id in strategy_order_ids:
            strategy[order.symbol] += direction * quantity

    result: dict[str, dict[str, str]] = {}
    for symbol in symbols:
        broker_quantity = _holding_quantity(holdings[symbol].payload)
        if totals[symbol] != broker_quantity:
            raise PositionIdentityConflictError(
                "Local fills do not conserve the current Alpaca holding"
            )
        if strategy[symbol] < 0 or strategy[symbol] > broker_quantity:
            raise PositionIdentityConflictError(
                "Legacy strategy inventory is outside the broker holding"
            )
        result[symbol] = {
            "broker_base_quantity": _quantity_text(broker_quantity),
            "strategy_base_quantity": _quantity_text(strategy[symbol]),
            "manual_base_quantity": _quantity_text(
                broker_quantity - strategy[symbol]
            ),
        }
    return result


def _position_material(position: ExecutionPosition) -> dict[str, object]:
    return {
        "id": position.id,
        "provider": position.provider,
        "account_scope": position.account_scope,
        "exchange": position.exchange,
        "symbol": position.symbol,
        "base_quantity": _quantity_text(Decimal(position.base_quantity or 0)),
        "average_price": _optional_decimal_text(position.average_price),
        "updated_from": position.updated_from,
        "last_reconciled_at": _optional_datetime_text(
            position.last_reconciled_at
        ),
    }


def _snapshot_material(
    *,
    snapshot: BrokerAccountSnapshot,
    symbols: tuple[str, ...],
) -> dict[str, object]:
    holdings = {
        str(row.symbol).strip().upper(): row for row in snapshot.holdings
    }
    return {
        "source": snapshot.source,
        "holdings_complete": snapshot.holdings_complete,
        "open_orders_complete": snapshot.open_orders_complete,
        "holdings": [
            {
                "symbol": symbol,
                "base_quantity": _quantity_text(
                    _holding_quantity(holdings[symbol].payload)
                ),
                "average_price": _optional_decimal_text(
                    holdings[symbol].average_price
                ),
            }
            for symbol in symbols
        ],
        "open_order_count": len(snapshot.open_orders),
    }


def _order_material(order: BrokerOrder) -> dict[str, object]:
    return {
        "id": order.id,
        "broker_order_id": order.broker_order_id,
        "idempotency_key": order.idempotency_key,
        "symbol": order.symbol,
        "side": order.side,
        "status": order.status,
        "quantity": order.quantity,
        "base_quantity": _optional_decimal_text(order.base_quantity),
        "quote_notional": _optional_decimal_text(order.quote_notional),
        "request_metadata": order.request_metadata,
        "fills": [
            {
                "id": fill.id,
                "identity_key": execution_fill_dedupe_key(fill),
                "symbol": fill.symbol,
                "side": fill.side,
                "quantity": fill.quantity,
                "base_quantity": _optional_decimal_text(fill.base_quantity),
                "quote_notional": _optional_decimal_text(fill.quote_notional),
                "price": str(fill.price),
                "filled_at": fill.filled_at.isoformat(),
            }
            for fill in sorted(
                dedupe_execution_fills(order.fills),
                key=lambda item: item.id,
            )
        ],
    }


def _holding_quantity(payload: object) -> Decimal:
    if not isinstance(payload, dict):
        raise PositionIdentityConflictError(
            "Alpaca holding lacks fractional quantity evidence"
        )
    quantity = _required_decimal(payload.get("base_quantity"))
    if quantity < 0:
        raise PositionIdentityConflictError(
            "Alpaca holding quantity cannot be negative"
        )
    return quantity


def _required_decimal(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PositionIdentityConflictError(
            "Alpaca recovery evidence contains an invalid decimal"
        ) from exc
    if not parsed.is_finite():
        raise PositionIdentityConflictError(
            "Alpaca recovery evidence contains a non-finite decimal"
        )
    return parsed


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return _required_decimal(value)


def _quantity_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    quantized = value.quantize(QUANTITY_QUANTUM)
    if value != quantized:
        raise PositionIdentityConflictError(
            "Alpaca recovery quantity precision exceeds the supported quantum"
        )
    return format(quantized, "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _optional_datetime_text(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def fingerprints_match(left: str, right: str) -> bool:
    return hmac.compare_digest(str(left), str(right))


def has_active_alpaca_us_legacy_strategy_recovery(
    *,
    settings: Settings,
    db: Session,
    account_scope: str,
    strategy_family: str,
    symbols: tuple[str, ...],
    authority_fingerprint: str,
) -> bool:
    """Verify that the exact legacy graph has one applied opening seed."""

    if not (
        settings.runtime_mode == "paper"
        and strategy_family == STRATEGY_FAMILY
        and set(symbols) == set(_configured_symbols(settings))
    ):
        return False
    event = _active_recovery_event(
        settings=settings,
        db=db,
        account_scope=account_scope,
    )
    if event is None:
        return False
    evidence = (
        event.evidence_summary
        if isinstance(event.evidence_summary, dict)
        else {}
    )
    if not _stored_recovery_evidence_is_intact(evidence):
        return False
    expected_candidate_key = _recovery_candidate_key(
        settings=settings,
        account_scope=account_scope,
        authority_fingerprint=authority_fingerprint,
    )
    if not (
        event.candidate_key == expected_candidate_key
        and evidence.get("policy_version") == POLICY_VERSION
        and evidence.get("provider") == PROVIDER
        and evidence.get("account_scope") == account_scope
        and evidence.get("asset_class") == ASSET_CLASS
        and evidence.get("exchange") == EXCHANGE
        and evidence.get("strategy_family") == STRATEGY_FAMILY
        and evidence.get("strategy_version")
        == settings.alpaca_us_equity_strategy_version
        and set(evidence.get("symbols") or []) == set(symbols)
        and evidence.get("authority_fingerprint")
        == authority_fingerprint
        and evidence.get("approval_fingerprint")
    ):
        return False
    states = db.scalars(
        select(CryptoStrategyState).where(
            CryptoStrategyState.mode == settings.runtime_mode,
            CryptoStrategyState.provider == PROVIDER,
            CryptoStrategyState.account_scope == account_scope,
            CryptoStrategyState.strategy_family == STRATEGY_FAMILY,
            CryptoStrategyState.strategy_version
            == settings.alpaca_us_equity_strategy_version,
            CryptoStrategyState.symbol.in_(symbols),
        )
    ).all()
    if {row.symbol for row in states} != set(symbols) or len(states) != len(
        symbols
    ):
        return False
    for state in states:
        metrics = state.metrics if isinstance(state.metrics, dict) else {}
        marker = metrics.get("legacy_strategy_recovery")
        marker = marker if isinstance(marker, dict) else {}
        if not (
            marker.get("policy_version") == POLICY_VERSION
            and marker.get("candidate_key") == event.candidate_key
            and marker.get("approval_fingerprint")
            == evidence.get("approval_fingerprint")
            and marker.get("authority_fingerprint")
            == authority_fingerprint
        ):
            return False
    return True


def recovered_alpaca_us_strategy_inventory(
    *,
    settings: Settings,
    db: Session,
    states: dict[str, CryptoStrategyState],
    snapshot: BrokerAccountSnapshot,
    orders: list[BrokerOrder],
    route_scope: FifoRouteScope,
    order_origin: str,
    strategy_family: str,
    authority_released: bool,
    observed_at: datetime,
) -> tuple[dict[str, Decimal] | None, str | None]:
    """Replay only post-seed fills and prove total inventory conservation."""

    recovery_states = [
        state
        for state in states.values()
        if isinstance(state.metrics, dict)
        and isinstance(
            state.metrics.get("legacy_strategy_recovery"),
            dict,
        )
    ]
    if not recovery_states:
        return None, None
    drift_reason = "legacy_strategy_opening_inventory_conservation_drift"
    if not (
        authority_released
        and settings.runtime_mode == "paper"
        and route_scope.provider == PROVIDER
        and route_scope.asset_class == ASSET_CLASS
        and route_scope.exchange == EXCHANGE
        and strategy_family == STRATEGY_FAMILY
        and len(recovery_states) == len(states)
    ):
        return None, drift_reason
    snapshot_age_seconds = (
        _as_utc(observed_at) - _as_utc(snapshot.observed_at)
    ).total_seconds()
    snapshot_payload = (
        snapshot.payload if isinstance(snapshot.payload, dict) else {}
    )
    if not (
        -MAX_FUTURE_SKEW_SECONDS
        <= snapshot_age_seconds
        <= MAX_EVIDENCE_AGE_SECONDS
        and snapshot.holdings_complete
        and snapshot.open_orders_complete
        and snapshot_payload.get("holdings_coverage") == "complete"
        and snapshot_payload.get("open_orders_coverage") == "complete"
    ):
        return None, drift_reason
    candidate_keys = {
        str(
            state.metrics["legacy_strategy_recovery"].get("candidate_key")
            or ""
        )
        for state in recovery_states
    }
    if len(candidate_keys) != 1 or not next(iter(candidate_keys)):
        return None, drift_reason
    event = _active_recovery_event(
        settings=settings,
        db=db,
        account_scope=route_scope.account_scope,
    )
    if event is None or event.candidate_key not in candidate_keys:
        return None, drift_reason
    evidence = (
        event.evidence_summary
        if isinstance(event.evidence_summary, dict)
        else {}
    )
    if not _stored_recovery_evidence_is_intact(evidence):
        return None, drift_reason
    opening = evidence.get("opening_inventory")
    if not isinstance(opening, dict) or set(opening) != set(states):
        return None, drift_reason
    baseline_keys_value = evidence.get("baseline_fill_identity_keys")
    if not isinstance(baseline_keys_value, list) or not all(
        isinstance(value, str) and value for value in baseline_keys_value
    ):
        return None, drift_reason
    baseline_keys = set(baseline_keys_value)

    evidence_orders = evidence.get("orders")
    if not isinstance(evidence_orders, list):
        return None, drift_reason
    evidence_order_ids = {
        str(item.get("id"))
        for item in evidence_orders
        if isinstance(item, dict) and item.get("id")
    }
    current_evidence_orders = [
        order for order in orders if order.id in evidence_order_ids
    ]
    current_evidence_material = [
        _order_material(order)
        for order in sorted(
            current_evidence_orders,
            key=lambda item: item.id,
        )
    ]
    if (
        len(current_evidence_orders) != len(evidence_orders)
        or current_evidence_material
        != sorted(evidence_orders, key=lambda item: str(item.get("id")))
    ):
        return None, drift_reason

    current_keys = {
        execution_fill_dedupe_key(fill)
        for order in current_evidence_orders
        for fill in dedupe_execution_fills(order.fills)
    }
    if current_keys != baseline_keys:
        return None, drift_reason

    total_quantities: dict[str, Decimal] = {}
    strategy_quantities: dict[str, Decimal] = {}
    for symbol in states:
        inventory = opening.get(symbol)
        if not isinstance(inventory, dict):
            return None, drift_reason
        try:
            total_quantities[symbol] = _required_decimal(
                inventory.get("broker_base_quantity")
            )
            strategy_quantities[symbol] = _required_decimal(
                inventory.get("strategy_base_quantity")
            )
        except PositionIdentityConflictError:
            return None, drift_reason

    expected_scope = {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        **route_scope.as_payload(),
    }
    for order in orders:
        post_seed_fills = [
            fill
            for fill in dedupe_execution_fills(order.fills)
            if execution_fill_dedupe_key(fill) not in baseline_keys
        ]
        if not post_seed_fills:
            continue
        if fifo_route_scope_from_order(order) != route_scope:
            return None, drift_reason
        side = str(order.side or "").strip().lower()
        if side == "buy":
            direction = Decimal("1")
        elif side == "sell":
            direction = Decimal("-1")
        else:
            return None, drift_reason
        delta = sum(
            (Decimal(fill.base_quantity or 0) for fill in post_seed_fills),
            Decimal("0"),
        )
        if delta <= 0 or order.symbol not in total_quantities:
            return None, drift_reason
        total_quantities[order.symbol] += direction * delta
        request_metadata = (
            order.request_metadata
            if isinstance(order.request_metadata, dict)
            else {}
        )
        command_metadata = request_metadata.get("metadata")
        command_metadata = (
            command_metadata if isinstance(command_metadata, dict) else {}
        )
        source = str(request_metadata.get("source") or "")
        if (
            source == order_origin
            or source.startswith(f"{order_origin}:")
        ) and (
            command_metadata.get("strategy_family") == strategy_family
            and command_metadata.get("expected_execution_scope")
            == expected_scope
        ):
            strategy_quantities[order.symbol] += direction * delta

    holdings = {
        str(holding.symbol or "").strip().upper(): holding
        for holding in snapshot.holdings
    }
    if not set(holdings).issubset(states):
        return None, drift_reason
    for symbol in states:
        try:
            broker_quantity = (
                _holding_quantity(holdings[symbol].payload)
                if symbol in holdings
                else Decimal("0")
            )
        except PositionIdentityConflictError:
            return None, drift_reason
        if not (
            broker_quantity == total_quantities[symbol]
            and Decimal("0")
            <= strategy_quantities[symbol]
            <= total_quantities[symbol]
        ):
            return None, drift_reason
    return strategy_quantities, None


def _active_recovery_event(
    *,
    settings: Settings,
    db: Session,
    account_scope: str,
) -> RuntimeRemediationEvent | None:
    events = db.scalars(
        select(RuntimeRemediationEvent).where(
            RuntimeRemediationEvent.mode == settings.runtime_mode,
            RuntimeRemediationEvent.candidate_type == CANDIDATE_TYPE,
            RuntimeRemediationEvent.outcome == OUTCOME,
            RuntimeRemediationEvent.subject_id == account_scope,
        )
    ).all()
    return events[0] if len(events) == 1 else None


def _recovery_candidate_key(
    *,
    settings: Settings,
    account_scope: str,
    authority_fingerprint: str,
) -> str:
    return _fingerprint(
        {
            "policy_version": POLICY_VERSION,
            "mode": settings.runtime_mode,
            "provider": PROVIDER,
            "account_scope": account_scope,
            "strategy_family": STRATEGY_FAMILY,
            "authority_fingerprint": authority_fingerprint,
        }
    )


def _stored_recovery_evidence_is_intact(
    evidence: dict[str, Any],
) -> bool:
    approval_fingerprint = evidence.get("approval_fingerprint")
    if not isinstance(approval_fingerprint, str):
        return False
    applied_only_fields = {
        "approval_fingerprint",
        "candidate_key",
        "applied_at",
        "broker_mutations",
        "orders_submitted",
        "orders_amended",
        "orders_canceled",
        "legacy_rows_mutated",
    }
    stable_evidence = {
        key: value
        for key, value in evidence.items()
        if key not in applied_only_fields
    }
    return hmac.compare_digest(
        approval_fingerprint,
        _fingerprint(stable_evidence),
    )
