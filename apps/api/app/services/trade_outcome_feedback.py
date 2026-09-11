from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, load_only, selectinload

from app.core.config import Settings, load_settings
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlanItem, ExecutionPosition
from app.services.fill_idempotency import dedupe_execution_fills
from app.services.fill_economics import (
    closed_trade_economics,
    effective_fill_quantity,
)
from app.services.fifo_inventory import (
    FifoIsolationCursor,
    FifoRouteScope,
    ambiguous_fifo_chronology_buckets,
    fifo_chronology_ambiguity_reason_at,
    fifo_fill_sort_key,
    fifo_route_scope_from_order,
    load_fifo_isolation_cursor,
)
from app.services.position_identity import (
    PositionIdentity,
    PositionIdentityConflictError,
    position_identity_from_columns,
    position_identity_from_payload,
)
from app.services.account_drawdown_gate import (
    account_drawdown_gate_prompt_summary,
    account_drawdown_gate_report_lines,
    latest_account_drawdown_gate_state,
)
from app.services.execution_account_route import redact_account_references
from app.services.broker_raw_holdings import (
    BrokerRawHolding,
    broker_raw_balance_is_fresh,
    broker_raw_balance_is_valid,
    broker_raw_holding_payload,
    broker_raw_holdings_from_payload,
    raw_vs_normalized_position_drift,
)
from app.services.portfolio_risk import (
    UNKNOWN_CURRENCY_KEY,
    aggregation_state_with_value_coverage,
    currency_aggregation_state,
    infer_order_settlement_currency,
    infer_settlement_currency,
    load_symbol_settlement_currency_hints,
)

REPORT_VERSION = "trade-outcome-feedback-v2"
MIN_CANDIDATE_RULE_SAMPLE_SIZE = 3
BROKER_BALANCE_OUTPUT2_FIELDS = (
    "tot_evlu_amt",
    "nass_amt",
    "bfdy_tot_asst_evlu_amt",
    "asst_icdc_amt",
    "evlu_pfls_smtl_amt",
    "pchs_amt_smtl_amt",
    "evlu_amt_smtl_amt",
    "dnca_tot_amt",
)
BROKER_BALANCE_SNAPSHOT_CANDIDATE_LIMIT = 16
SECTOR_LOOKBACK_PER_SYMBOL = 20


@dataclass(frozen=True)
class CandidateRule:
    rule_id: str
    name: str
    action: str
    size_multiplier: Decimal
    threshold: Decimal
    evaluator: Callable[[dict[str, object], "CandidateRule"], tuple[bool, dict[str, object]]]
    return_5d_threshold: Decimal | None = None


@dataclass(frozen=True)
class CandidateTradeReplay:
    pnl: Decimal
    return_pct: Decimal
    skipped_entry: bool
    skipped_winner: bool
    avoided_loser: bool
    evidence: dict[str, object]


@dataclass
class OpenLot:
    quantity: Decimal
    price: Decimal
    filled_at: datetime
    fill: ExecutionFill


@dataclass(frozen=True)
class LegacySellCostBasis:
    symbol: str
    average_price: Decimal
    source: str
    observed_at: datetime | None


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    position: ExecutionPosition
    position_identity: PositionIdentity
    balance: dict[str, object]
    output2: dict[str, object]
    observed_at: datetime | None


@dataclass(frozen=True)
class FeedbackExecutionScope:
    provider: str
    account_scope: str
    exchange: str | None = None

    def as_payload(self) -> dict[str, str]:
        payload = {
            "provider": self.provider,
            "account_scope": self.account_scope,
        }
        if self.exchange is not None:
            payload["exchange"] = self.exchange
        return payload


def generate_trade_outcome_feedback(
    *,
    settings: Settings,
    db: Session,
    lookback_days: int | None = None,
    as_of: datetime | None = None,
    output_dir: Path | None = None,
    provider: str | None = None,
    account_scope: str | None = None,
    exchange: str | None = None,
) -> dict[str, object]:
    requested_scope, scope_error = _requested_feedback_scope(
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    )
    if scope_error is not None:
        raise ValueError(scope_error)
    now = _normalize_utc(as_of or datetime.now(UTC))
    days = max(1, int(lookback_days or settings.trade_outcome_feedback_lookback_days))
    started_at = now - timedelta(days=days)
    fills = _load_fills(
        settings=settings,
        db=db,
        requested_scope=requested_scope,
    )
    execution_account_scope = _report_execution_account_scope(
        settings=settings,
        db=db,
        fills=fills,
        requested_scope=requested_scope,
    )
    scoped_sell_routes = {
        (route_scope, fill.symbol)
        for fill in fills
        if (route_scope := fifo_route_scope_from_order(fill.broker_order)) is not None
        if str(fill.side or "").lower() == "sell"
    }
    legacy_sell_cost_basis = _load_legacy_sell_cost_basis(
        settings=settings,
        db=db,
        route_symbols=scoped_sell_routes,
    )
    closed_trades = [
        trade
        for trade in _fifo_closed_trades(
            fills,
            legacy_sell_cost_basis=legacy_sell_cost_basis,
            isolation_cursor=load_fifo_isolation_cursor(
                db=db,
                runtime_mode=settings.runtime_mode,
                through=now,
            ),
        )
        if _parse_datetime(str((trade.get("sell") or {}).get("filled_at"))) >= started_at
    ]
    broker_asset_performance = _broker_asset_performance(
        settings=settings,
        db=db,
        closed_trades=closed_trades,
        lookback_days=days,
        started_at=started_at,
        as_of=now,
        requested_scope=requested_scope,
    )
    summary = _summary(closed_trades, broker_asset_performance=broker_asset_performance)
    lessons = _lessons(closed_trades, max_lessons=settings.trade_outcome_feedback_max_lessons)
    portfolio_risk = _portfolio_risk(
        settings=settings,
        db=db,
        closed_trades=closed_trades,
        as_of=now,
        requested_scope=requested_scope,
    )
    account_drawdown_gate = latest_account_drawdown_gate_state(settings=settings, db=db)
    candidate_rule_replay = _candidate_rule_replay(settings=settings, closed_trades=closed_trades)
    snippet = _snippet(
        summary=summary,
        lessons=lessons,
        portfolio_risk=portfolio_risk,
        candidate_rule_replay=candidate_rule_replay,
        broker_asset_performance=broker_asset_performance,
        account_drawdown_gate=account_drawdown_gate,
    )
    report_date = now.astimezone(UTC).date()
    report: dict[str, object] = {
        "version": REPORT_VERSION,
        "mode": settings.runtime_mode,
        "execution_account_scope": execution_account_scope,
        "generated_at": now.isoformat(),
        "lookback_days": days,
        "started_at": started_at.isoformat(),
        "closed_trade_count": len(closed_trades),
        "summary": summary,
        "broker_asset_performance": broker_asset_performance,
        "portfolio_risk": portfolio_risk,
        "account_drawdown_gate": account_drawdown_gate,
        "closed_trades": closed_trades,
        "lessons": lessons,
        "candidate_rule_replay": candidate_rule_replay,
        "prompt_snippet": snippet,
    }
    target_dir = output_dir or Path(settings.trade_outcome_feedback_dir)
    _write_artifacts(
        report=report,
        report_date=report_date,
        output_dir=target_dir,
        artifact_scope=requested_scope,
    )
    return report


def load_latest_trade_outcome_feedback_snippet(
    settings: Settings,
    *,
    now: datetime | None = None,
    provider: str | None = None,
    account_scope: str | None = None,
    exchange: str | None = None,
) -> dict[str, object] | None:
    if not settings.trade_outcome_feedback_enabled:
        return None
    loaded = _load_latest_trade_outcome_feedback(
        settings=settings,
        now=now,
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    )
    if loaded is None:
        return None
    payload, generated_at = loaded
    if not _feedback_scope_matches(
        payload.get("execution_account_scope"),
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    ):
        return None
    snippet = payload.get("prompt_snippet")
    if not isinstance(snippet, dict):
        return None
    return {
        "version": payload.get("version"),
        "mode": payload.get("mode"),
        "execution_account_scope": payload.get("execution_account_scope"),
        "generated_at": generated_at.isoformat(),
        "lookback_days": payload.get("lookback_days"),
        "closed_trade_count": payload.get("closed_trade_count"),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        "snippet": snippet,
    }


def ensure_fresh_trade_outcome_feedback(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
    provider: str | None = None,
    account_scope: str | None = None,
    exchange: str | None = None,
) -> dict[str, object]:
    """Generate the bounded feedback artifact when the configured copy is missing or stale."""
    if not settings.trade_outcome_feedback_enabled:
        return {"status": "disabled", "version": REPORT_VERSION}

    requested_scope, scope_error = _requested_feedback_scope(
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    )
    if scope_error is not None:
        return {
            "status": "unavailable",
            "version": REPORT_VERSION,
            "reason": scope_error,
        }

    current_time = _normalize_utc(now or datetime.now(UTC))
    existing = _load_latest_trade_outcome_feedback(
        settings=settings,
        now=current_time,
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    )
    if existing is not None:
        existing_payload, generated_at = existing
        return {
            "status": "fresh",
            "version": existing_payload["version"],
            "generated_at": generated_at.isoformat(),
            "closed_trade_count": existing_payload.get("closed_trade_count"),
        }

    report = generate_trade_outcome_feedback(
        settings=settings,
        db=db,
        as_of=current_time,
        provider=requested_scope.provider if requested_scope is not None else None,
        account_scope=(
            requested_scope.account_scope if requested_scope is not None else None
        ),
        exchange=requested_scope.exchange if requested_scope is not None else None,
    )
    return {
        "status": "generated",
        "version": report["version"],
        "generated_at": report["generated_at"],
        "closed_trade_count": report["closed_trade_count"],
    }


def _load_latest_trade_outcome_feedback(
    *,
    settings: Settings,
    now: datetime | None,
    provider: str | None = None,
    account_scope: str | None = None,
    exchange: str | None = None,
) -> tuple[dict[str, object], datetime] | None:
    requested_scope, scope_error = _requested_feedback_scope(
        provider=provider,
        account_scope=account_scope,
        exchange=exchange,
    )
    if scope_error is not None:
        return None
    output_dir = Path(settings.trade_outcome_feedback_dir)
    candidate_paths = (
        [
            output_dir
            / _scoped_artifact_name(
                prefix="latest",
                suffix="json",
                scope=requested_scope,
                runtime_mode=settings.runtime_mode,
            ),
            output_dir / "latest.json",
        ]
        if requested_scope is not None
        else [output_dir / "latest.json"]
    )
    for latest_path in candidate_paths:
        try:
            payload = json.loads(latest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("version") != REPORT_VERSION:
            continue
        if payload.get("mode") != settings.runtime_mode:
            continue
        if requested_scope is not None and not _feedback_scope_matches(
            payload.get("execution_account_scope"),
            provider=requested_scope.provider,
            account_scope=requested_scope.account_scope,
            exchange=requested_scope.exchange,
        ):
            continue
        generated_at = _parse_feedback_generated_at(payload.get("generated_at"))
        if generated_at is None:
            continue
        current_time = _normalize_utc(now or datetime.now(UTC))
        age_seconds = (current_time - generated_at).total_seconds()
        if (
            age_seconds < -300
            or age_seconds > settings.trade_outcome_feedback_max_age_seconds
        ):
            continue
        return payload, generated_at
    return None


def _feedback_scope_matches(
    value: object,
    *,
    provider: str | None,
    account_scope: str | None,
    exchange: str | None = None,
) -> bool:
    if not isinstance(value, dict) or set(value) < {
        "status",
        "provider",
        "account_scope",
    }:
        return False
    expected_provider = str(provider or "").strip().lower()
    expected_account_scope = str(account_scope or "").strip()
    stored_provider = str(value.get("provider") or "").strip().lower()
    stored_account_scope = str(value.get("account_scope") or "").strip()
    expected_exchange = str(exchange or "").strip().upper()
    stored_exchange = str(value.get("exchange") or "").strip().upper()
    return (
        value.get("status") == "single"
        and bool(expected_provider)
        and _valid_account_scope(expected_account_scope)
        and stored_provider == expected_provider
        and stored_account_scope == expected_account_scope
        and stored_exchange == expected_exchange
    )


def _parse_feedback_generated_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _load_fills(
    *,
    settings: Settings,
    db: Session,
    requested_scope: FeedbackExecutionScope | None = None,
) -> list[ExecutionFill]:
    fills = dedupe_execution_fills(list(
        db.scalars(
            select(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
            )
            .options(
                load_only(
                    ExecutionFill.id,
                    ExecutionFill.broker_order_pk,
                    ExecutionFill.broker_fill_id,
                    ExecutionFill.idempotency_key,
                    ExecutionFill.symbol,
                    ExecutionFill.side,
                    ExecutionFill.quantity,
                    ExecutionFill.base_quantity,
                    ExecutionFill.quote_notional,
                    ExecutionFill.price,
                    ExecutionFill.settlement_currency,
                    ExecutionFill.transaction_cost,
                    ExecutionFill.transaction_cost_coverage,
                    ExecutionFill.transaction_cost_source,
                    ExecutionFill.payload,
                    ExecutionFill.filled_at,
                ),
                selectinload(ExecutionFill.broker_order).load_only(
                    BrokerOrder.id,
                    BrokerOrder.execution_plan_item_id,
                    BrokerOrder.broker_order_id,
                    BrokerOrder.symbol,
                    BrokerOrder.side,
                    BrokerOrder.mode,
                    BrokerOrder.broker_name,
                    BrokerOrder.venue,
                    BrokerOrder.asset_class,
                    BrokerOrder.request_metadata,
                ),
                selectinload(ExecutionFill.broker_order)
                .selectinload(BrokerOrder.plan_item)
                .load_only(
                    ExecutionPlanItem.id,
                    ExecutionPlanItem.intent,
                    ExecutionPlanItem.source_plan_item_id,
                    ExecutionPlanItem.decision_run_id,
                    ExecutionPlanItem.decision_agent_name,
                    ExecutionPlanItem.risk_context,
                    ExecutionPlanItem.quantity_policy,
                ),
            )
            .order_by(ExecutionFill.symbol.asc(), ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        )
    ))
    if requested_scope is None:
        return fills
    return [
        fill
        for fill in fills
        if _order_matches_feedback_scope(
            fill.broker_order,
            requested_scope,
        )
    ]


def _report_execution_account_scope(
    *,
    settings: Settings,
    db: Session,
    fills: list[ExecutionFill],
    requested_scope: FeedbackExecutionScope | None = None,
) -> dict[str, object]:
    scopes: set[tuple[str, str, str]] = set()
    invalid_evidence_count = 0
    evidence_count = 0

    def record_scope(
        provider: object,
        account_scope: object,
        exchange: object,
    ) -> None:
        nonlocal evidence_count, invalid_evidence_count
        evidence_count += 1
        normalized_provider = str(provider or "").strip().lower()
        normalized_account_scope = str(account_scope or "").strip()
        normalized_exchange = str(exchange or "").strip().upper()
        if (
            not normalized_provider
            or not _valid_account_scope(normalized_account_scope)
            or not normalized_exchange
        ):
            invalid_evidence_count += 1
            return
        scopes.add(
            (normalized_provider, normalized_account_scope, normalized_exchange)
        )

    for fill in fills:
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        if route_scope is None:
            record_scope(None, None, None)
        else:
            record_scope(
                route_scope.provider,
                route_scope.account_scope,
                route_scope.exchange,
            )

    position_scope_fields = [
        ExecutionPosition.provider,
        ExecutionPosition.account_scope,
        ExecutionPosition.exchange,
    ]
    if requested_scope is not None:
        position_scope_fields.append(ExecutionPosition.raw_payload)
    relevant_positions = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.mode == settings.runtime_mode,
        )
        .options(
            load_only(*position_scope_fields)
        )
    ).all()
    for position in relevant_positions:
        identity = (
            _canonical_position_identity(position)
            if requested_scope is not None
            else _position_column_identity(position)
        )
        if identity is None:
            if requested_scope is None:
                record_scope(None, None, None)
            continue
        if requested_scope is not None and not _position_identity_matches_feedback_scope(
            identity,
            requested_scope,
        ):
            continue
        record_scope(identity.provider, identity.account_scope, identity.exchange)

    for order in _load_open_orders(
        settings=settings,
        db=db,
        requested_scope=requested_scope,
    ):
        route_scope = fifo_route_scope_from_order(order)
        if route_scope is None:
            if requested_scope is None:
                record_scope(None, None, None)
        else:
            record_scope(
                route_scope.provider,
                route_scope.account_scope,
                route_scope.exchange,
            )

    if requested_scope is not None:
        payload: dict[str, object] = {
            "status": "single",
            **requested_scope.as_payload(),
            "evidence_count": evidence_count,
        }
        return payload

    if invalid_evidence_count:
        return {
            "status": "unavailable",
            "reason": "unscoped_or_invalid_route_evidence",
            "evidence_count": evidence_count,
            "invalid_evidence_count": invalid_evidence_count,
        }
    if len(scopes) > 1:
        return {
            "status": "mixed",
            "reason": "multiple_execution_account_scopes",
            "evidence_count": evidence_count,
            "scope_count": len(scopes),
        }
    if not scopes:
        return {
            "status": "unavailable",
            "reason": "no_execution_account_scope_evidence",
            "evidence_count": 0,
        }
    provider, account_scope, exchange = next(iter(scopes))
    return {
        "status": "single",
        "provider": provider,
        "account_scope": account_scope,
        "exchange": exchange,
        "evidence_count": evidence_count,
    }


def _valid_account_scope(value: object) -> bool:
    normalized = str(value or "").strip()
    return len(normalized) == 24 and all(
        character in "0123456789abcdef" for character in normalized
    )


def _requested_feedback_scope(
    *,
    provider: str | None,
    account_scope: str | None,
    exchange: str | None,
) -> tuple[FeedbackExecutionScope | None, str | None]:
    if provider is None and account_scope is None and exchange is None:
        return None, None
    normalized_provider = str(provider or "").strip().lower()
    normalized_account_scope = str(account_scope or "").strip()
    normalized_exchange = str(exchange or "").strip().upper() or None
    provider_is_safe = bool(normalized_provider) and all(
        character.isalnum() or character in {"-", "_"}
        for character in normalized_provider
    )
    exchange_is_safe = normalized_exchange is None or all(
        character.isalnum() or character in {"-", "_", "."}
        for character in normalized_exchange
    )
    if (
        not provider_is_safe
        or not _valid_account_scope(normalized_account_scope)
        or not exchange_is_safe
    ):
        return None, "invalid_requested_execution_account_scope"
    return (
        FeedbackExecutionScope(
            provider=normalized_provider,
            account_scope=normalized_account_scope,
            exchange=normalized_exchange,
        ),
        None,
    )


def _fifo_route_matches_feedback_scope(
    route_scope: FifoRouteScope | None,
    requested_scope: FeedbackExecutionScope,
) -> bool:
    return (
        route_scope is not None
        and route_scope.provider == requested_scope.provider
        and route_scope.account_scope == requested_scope.account_scope
        and (
            requested_scope.exchange is None
            or route_scope.exchange == requested_scope.exchange
        )
    )


def _order_matches_feedback_scope(
    order: BrokerOrder,
    requested_scope: FeedbackExecutionScope,
) -> bool:
    metadata = order.request_metadata
    if not isinstance(metadata, dict) or "execution_scope" not in metadata:
        return False
    return _fifo_route_matches_feedback_scope(
        fifo_route_scope_from_order(order),
        requested_scope,
    )


def _position_identity_matches_feedback_scope(
    identity: PositionIdentity,
    requested_scope: FeedbackExecutionScope,
) -> bool:
    return (
        identity.provider == requested_scope.provider
        and identity.account_scope == requested_scope.account_scope
        and (
            requested_scope.exchange is None
            or identity.exchange == requested_scope.exchange
        )
    )


def _position_column_identity(position: ExecutionPosition) -> PositionIdentity | None:
    try:
        return position_identity_from_columns(position)
    except PositionIdentityConflictError:
        return None


def _canonical_position_identity(
    position: ExecutionPosition,
) -> PositionIdentity | None:
    column_identity = _position_column_identity(position)
    if column_identity is None:
        return None
    try:
        payload_identity = position_identity_from_payload(position.raw_payload)
    except PositionIdentityConflictError:
        return None
    return column_identity if payload_identity == column_identity else None


def _scoped_artifact_name(
    *,
    prefix: str,
    suffix: str,
    scope: FeedbackExecutionScope,
    runtime_mode: str,
) -> str:
    scope_key = "\0".join(
        (
            scope.provider,
            scope.account_scope,
            scope.exchange or "*",
            str(runtime_mode or "").strip().lower(),
        )
    )
    digest = hashlib.sha256(scope_key.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}.{digest}.{suffix}"


def _load_legacy_sell_cost_basis(
    *,
    settings: Settings,
    db: Session,
    route_symbols: set[tuple[FifoRouteScope, str]],
) -> dict[tuple[FifoRouteScope, str], LegacySellCostBasis]:
    normalized_route_symbols = {
        (route_scope, symbol.strip().upper())
        for route_scope, symbol in route_symbols
        if symbol.strip()
    }
    if not normalized_route_symbols:
        return {}
    costs: dict[tuple[FifoRouteScope, str], LegacySellCostBasis] = {}
    for route_scope, symbol in sorted(
        normalized_route_symbols,
        key=lambda item: (*item[0].as_payload().values(), item[1]),
    ):
        positions = list(
            db.scalars(
                select(ExecutionPosition)
                .where(
                    ExecutionPosition.mode == settings.runtime_mode,
                    ExecutionPosition.provider == route_scope.provider,
                    ExecutionPosition.account_scope == route_scope.account_scope,
                    ExecutionPosition.exchange == route_scope.exchange,
                    ExecutionPosition.symbol == symbol,
                )
                .options(
                    load_only(
                        ExecutionPosition.provider,
                        ExecutionPosition.account_scope,
                        ExecutionPosition.exchange,
                        ExecutionPosition.symbol,
                        ExecutionPosition.average_price,
                        ExecutionPosition.last_reconciled_at,
                        ExecutionPosition.updated_at,
                        ExecutionPosition.raw_payload,
                    )
                )
                # A second exact row means the scoped uniqueness invariant is
                # damaged; never pick one by ordering.
                .limit(2)
            )
        )
        if len(positions) != 1:
            continue
        position = positions[0]
        try:
            column_identity = position_identity_from_columns(position)
            payload_identity = position_identity_from_payload(position.raw_payload)
        except PositionIdentityConflictError:
            continue
        if column_identity is None or (
            payload_identity is not None and payload_identity != column_identity
        ):
            continue
        if (
            column_identity.provider != route_scope.provider
            or column_identity.account_scope != route_scope.account_scope
            or column_identity.exchange != route_scope.exchange
        ):
            continue
        position_average = _decimal_value(position.average_price)
        if position_average is not None and position_average > 0:
            costs[(route_scope, symbol)] = LegacySellCostBasis(
                symbol=symbol,
                average_price=position_average,
                source="reconciled_average_cost_estimate",
                observed_at=_normalize_utc(position.last_reconciled_at)
                if position.last_reconciled_at is not None
                else None,
            )
            continue
        raw_holding = _raw_holding_for_symbol(payload=position.raw_payload, symbol=symbol)
        if raw_holding is not None and raw_holding.average_price is not None and raw_holding.average_price > 0:
            costs[(route_scope, symbol)] = LegacySellCostBasis(
                symbol=symbol,
                average_price=raw_holding.average_price,
                source="broker_raw_holding_average_cost_estimate",
                observed_at=_normalize_utc(position.last_reconciled_at)
                if position.last_reconciled_at is not None
                else None,
            )
    return costs


def _raw_holding_for_symbol(*, payload: dict[str, Any] | None, symbol: str) -> BrokerRawHolding | None:
    normalized = symbol.strip().upper()
    for holding in broker_raw_holdings_from_payload(payload):
        if holding.symbol == normalized:
            return holding
    return None


def _fifo_closed_trades(
    fills: list[ExecutionFill],
    *,
    legacy_sell_cost_basis: (
        dict[tuple[FifoRouteScope, str], LegacySellCostBasis] | None
    ) = None,
    isolation_cursor: FifoIsolationCursor | None = None,
) -> list[dict[str, object]]:
    lots_by_route_symbol: dict[tuple[FifoRouteScope, str], list[OpenLot]] = {}
    closed: list[dict[str, object]] = []
    cost_basis = legacy_sell_cost_basis or {}
    ambiguous_buckets = ambiguous_fifo_chronology_buckets(fills)
    for fill in sorted(
        fills,
        key=lambda row: (
            row.symbol,
            *fifo_fill_sort_key(row),
        ),
    ):
        side = str(fill.side or "").lower()
        route_scope = fifo_route_scope_from_order(fill.broker_order)
        if route_scope is None:
            if side == "sell":
                closed.append(
                    _legacy_sell_estimate_trade(
                        sell_fill=fill,
                        quantity=effective_fill_quantity(fill),
                        cost_basis=None,
                        route_scope=None,
                        scope_quality_available=False,
                    )
                )
            continue
        route_symbol = (route_scope, fill.symbol)
        lots = lots_by_route_symbol.setdefault(route_symbol, [])
        scope_quality_available = True
        if isolation_cursor is not None:
            scope_quality_available = isolation_cursor.apply_before(
                route_scope=route_scope,
                symbol=fill.symbol,
                observed_at=_normalize_utc(fill.filled_at),
                lots=lots,
            )
        chronology_reason = fifo_chronology_ambiguity_reason_at(
            ambiguous_buckets=ambiguous_buckets,
            isolation_cursor=isolation_cursor,
            route_scope=route_scope,
            symbol=fill.symbol,
            observed_at=_normalize_utc(fill.filled_at),
        )
        if chronology_reason is not None:
            if side == "sell":
                closed.append(
                    _legacy_sell_estimate_trade(
                        sell_fill=fill,
                        quantity=effective_fill_quantity(fill),
                        cost_basis=None,
                        route_scope=route_scope,
                        scope_quality_available=True,
                        quality_flag=(
                            "fill_chronology_observation_bounded_generation"
                            if chronology_reason
                            == "observation_bounded_generation"
                            else "fill_chronology_ambiguous"
                        ),
                    )
                )
            continue
        if not scope_quality_available:
            if side == "sell":
                closed.append(
                    _legacy_sell_estimate_trade(
                        sell_fill=fill,
                        quantity=effective_fill_quantity(fill),
                        cost_basis=None,
                        route_scope=route_scope,
                        scope_quality_available=False,
                    )
                )
            continue
        if side == "buy":
            lots.append(
                OpenLot(
                    quantity=effective_fill_quantity(fill),
                    price=Decimal(fill.price),
                    filled_at=_normalize_utc(fill.filled_at),
                    fill=fill,
                )
            )
            continue
        if side != "sell":
            continue
        remaining = effective_fill_quantity(fill)
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, lot.quantity)
            closed.append(
                _closed_trade(
                    lot=lot,
                    sell_fill=fill,
                    quantity=matched,
                    route_scope=route_scope,
                )
            )
            lot.quantity -= matched
            remaining -= matched
            if lot.quantity <= 0:
                lots.pop(0)
        if remaining > 0:
            closed.append(
                _legacy_sell_estimate_trade(
                    sell_fill=fill,
                    quantity=remaining,
                    cost_basis=cost_basis.get(route_symbol),
                    route_scope=route_scope,
                    scope_quality_available=True,
                )
            )
    return closed


def _closed_trade(
    *,
    lot: OpenLot,
    sell_fill: ExecutionFill,
    quantity: Decimal,
    route_scope: FifoRouteScope,
) -> dict[str, object]:
    buy_order = lot.fill.broker_order
    sell_order = sell_fill.broker_order
    buy_item = buy_order.plan_item if buy_order is not None else None
    sell_item = sell_order.plan_item if sell_order is not None else None
    sell_price = Decimal(sell_fill.price)
    buy_notional = lot.price * quantity
    sell_notional = sell_price * quantity
    gross_pnl = sell_notional - buy_notional
    economics = closed_trade_economics(
        buy_fill=lot.fill,
        sell_fill=sell_fill,
        matched_quantity=quantity,
        gross_realized_pnl=gross_pnl,
    )
    analysis_pnl = economics.net_realized_pnl
    gross_return_pct = (
        gross_pnl / buy_notional * Decimal("100")
        if buy_notional > 0
        else Decimal("0")
    )
    return_pct = (
        analysis_pnl / buy_notional * Decimal("100")
        if analysis_pnl is not None and buy_notional > 0
        else None
    )
    hold_minutes = int((_normalize_utc(sell_fill.filled_at) - lot.filled_at).total_seconds() // 60)
    source_item = buy_item or sell_item
    return {
        "symbol": sell_fill.symbol,
        "execution_scope": route_scope.as_payload(),
        "quantity": _quantity_value(quantity),
        "buy": _fill_payload(fill=lot.fill, order=buy_order, item=buy_item, quantity=quantity),
        "sell": _fill_payload(fill=sell_fill, order=sell_order, item=sell_item, quantity=quantity),
        "realized_pnl_krw": _decimal_text(analysis_pnl)
        if economics.settlement_currency == "KRW"
        else None,
        "realized_pnl_basis": "net_after_transaction_costs"
        if analysis_pnl is not None
        else "unavailable_incomplete_transaction_costs",
        "profitability_eligible": analysis_pnl is not None
        and economics.settlement_currency == "KRW",
        "settlement_currency": economics.settlement_currency,
        "buy_settlement_currency": economics.buy_settlement_currency,
        "sell_settlement_currency": economics.sell_settlement_currency,
        "gross_realized_pnl": _decimal_text(gross_pnl),
        "gross_realized_pnl_krw": _decimal_text(gross_pnl)
        if economics.settlement_currency == "KRW"
        else None,
        "buy_transaction_cost": _decimal_text(economics.buy_transaction_cost),
        "sell_transaction_cost": _decimal_text(economics.sell_transaction_cost),
        "known_transaction_cost": _decimal_text(economics.known_transaction_cost),
        "net_realized_pnl": _decimal_text(economics.net_realized_pnl),
        "net_realized_pnl_krw": _decimal_text(economics.net_realized_pnl)
        if economics.settlement_currency == "KRW"
        else None,
        "transaction_cost_coverage": economics.transaction_cost_coverage,
        "return_pct": _decimal_text(return_pct),
        "gross_return_pct": _decimal_text(gross_return_pct),
        "hold_minutes": hold_minutes,
        "hold_hours": _decimal_text(Decimal(hold_minutes) / Decimal("60")),
        "source_execution_plan_item_id": source_item.id if source_item is not None else None,
        "decision_run_id": source_item.decision_run_id if source_item is not None else None,
        "decision_agent_name": source_item.decision_agent_name if source_item is not None else None,
        "exec_agent_decision": _exec_agent_decision(source_item),
        "risk_sizing": _risk_sizing_context(source_item),
        "trigger_time": _trigger_time_context(source_item),
        "market_enrichment": _market_enrichment_context(source_item),
        "candidate_rule_context": _merged_candidate_rule_context(buy_item=buy_item, sell_item=sell_item, buy_fill=lot.fill),
        "outcome_source": "fifo_exact",
        "outcome_quality_flags": (
            []
            if economics.transaction_cost_coverage == "complete"
            else [f"transaction_cost_coverage_{economics.transaction_cost_coverage}"]
        )
        + ([] if economics.settlement_currency is not None else ["settlement_currency_unknown"]),
        "outcome_bucket": (
            _outcome_bucket(analysis_pnl) if analysis_pnl is not None else "unavailable"
        ),
    }


def _legacy_sell_estimate_trade(
    *,
    sell_fill: ExecutionFill,
    quantity: Decimal,
    cost_basis: LegacySellCostBasis | None,
    route_scope: FifoRouteScope | None,
    scope_quality_available: bool,
    quality_flag: str | None = None,
) -> dict[str, object]:
    sell_order = sell_fill.broker_order
    sell_item = sell_order.plan_item if sell_order is not None else None
    sell_price = Decimal(sell_fill.price)
    source_item = sell_item
    if cost_basis is None or not scope_quality_available:
        pnl = None
        return_pct = None
        buy_price = None
        outcome_source = "unavailable"
        quality_flags = ["missing_fifo_buy_lot", "missing_average_cost_basis"]
        if not scope_quality_available:
            quality_flags.append("execution_scope_unavailable")
    else:
        buy_price = cost_basis.average_price
        buy_notional = buy_price * quantity
        sell_notional = sell_price * quantity
        pnl = sell_notional - buy_notional
        return_pct = (pnl / buy_notional * Decimal("100")) if buy_notional > 0 else Decimal("0")
        outcome_source = cost_basis.source
        quality_flags = ["missing_fifo_buy_lot", f"estimated_from_{cost_basis.source}"]
    economics = (
        closed_trade_economics(
            buy_fill=None,
            sell_fill=sell_fill,
            matched_quantity=quantity,
            gross_realized_pnl=pnl,
        )
        if pnl is not None
        else None
    )
    currency = economics.settlement_currency if economics is not None else None
    if economics is not None:
        quality_flags.append(
            f"transaction_cost_coverage_{economics.transaction_cost_coverage}"
        )
    if currency is None:
        quality_flags.append("settlement_currency_unknown")
    if quality_flag is not None:
        quality_flags.append(quality_flag)
    return {
        "symbol": sell_fill.symbol,
        "execution_scope": (
            route_scope.as_payload() if route_scope is not None else None
        ),
        "quantity": quantity,
        "buy": {
            "filled_at": cost_basis.observed_at.isoformat()
            if cost_basis is not None and cost_basis.observed_at is not None
            else None,
            "quantity": _quantity_value(quantity),
            "price": _decimal_text(buy_price),
            "broker_fill_id": None,
            "broker_order_id": None,
            "execution_plan_item_id": None,
            "intent": None,
            "source_plan_item_id": None,
            "cost_basis_source": outcome_source,
        },
        "sell": _fill_payload(fill=sell_fill, order=sell_order, item=sell_item, quantity=quantity),
        "realized_pnl_krw": None,
        "realized_pnl_basis": "unavailable_incomplete_transaction_costs",
        "profitability_eligible": False,
        "settlement_currency": currency,
        "buy_settlement_currency": (
            economics.buy_settlement_currency if economics is not None else None
        ),
        "sell_settlement_currency": (
            economics.sell_settlement_currency if economics is not None else None
        ),
        "gross_realized_pnl": _decimal_text(pnl),
        "gross_realized_pnl_krw": _decimal_text(pnl) if currency == "KRW" else None,
        "buy_transaction_cost": _decimal_text(
            economics.buy_transaction_cost if economics is not None else None
        ),
        "sell_transaction_cost": _decimal_text(
            economics.sell_transaction_cost if economics is not None else None
        ),
        "known_transaction_cost": _decimal_text(
            economics.known_transaction_cost if economics is not None else None
        ),
        "net_realized_pnl": None,
        "net_realized_pnl_krw": None,
        "transaction_cost_coverage": (
            economics.transaction_cost_coverage if economics is not None else "unknown"
        ),
        "return_pct": None,
        "gross_return_pct": _decimal_text(return_pct),
        "hold_minutes": None,
        "hold_hours": None,
        "source_execution_plan_item_id": source_item.id if source_item is not None else None,
        "decision_run_id": source_item.decision_run_id if source_item is not None else None,
        "decision_agent_name": source_item.decision_agent_name if source_item is not None else None,
        "exec_agent_decision": _exec_agent_decision(source_item),
        "risk_sizing": _risk_sizing_context(source_item),
        "trigger_time": _trigger_time_context(source_item),
        "market_enrichment": _market_enrichment_context(source_item),
        "candidate_rule_context": _candidate_rule_context(item=source_item, buy_fill=sell_fill),
        "outcome_source": outcome_source,
        "outcome_quality_flags": quality_flags,
        "outcome_bucket": "unavailable",
    }


def _fill_payload(
    *,
    fill: ExecutionFill,
    order: BrokerOrder | None,
    item: ExecutionPlanItem | None,
    quantity: Decimal,
) -> dict[str, object]:
    return {
        "filled_at": _normalize_utc(fill.filled_at).isoformat(),
        "quantity": _quantity_value(quantity),
        "price": _decimal_text(Decimal(fill.price)),
        "broker_fill_id": fill.broker_fill_id,
        "broker_order_id": order.broker_order_id if order is not None else None,
        "execution_plan_item_id": item.id if item is not None else None,
        "intent": item.intent if item is not None else None,
        "source_plan_item_id": item.source_plan_item_id if item is not None else None,
    }


def _summary(
    closed_trades: list[dict[str, object]],
    *,
    broker_asset_performance: dict[str, object] | None = None,
) -> dict[str, object]:
    pnl_values = [
        value
        for row in closed_trades
        for value in [_decimal_value(row.get("realized_pnl_krw"))]
        if value is not None
    ]
    winners = [value for value in pnl_values if value > 0]
    losers = [value for value in pnl_values if value < 0]
    total = sum(pnl_values, Decimal("0"))
    priced_trades = [row for row in closed_trades if row.get("gross_realized_pnl") is not None]
    currencies = {
        str(row["settlement_currency"])
        for row in priced_trades
        if row.get("settlement_currency")
    }
    unknown_currency_count = sum(
        1 for row in closed_trades if not row.get("settlement_currency")
    )
    gross_by_currency: dict[str, str | None] = {}
    net_by_currency: dict[str, str | None] = {}
    for currency in sorted(currencies):
        currency_rows = [
            row for row in priced_trades if row.get("settlement_currency") == currency
        ]
        gross_by_currency[currency] = _decimal_text(
            sum(
                (
                    _decimal_value(row.get("gross_realized_pnl")) or Decimal("0")
                    for row in currency_rows
                ),
                Decimal("0"),
            )
        )
        net_values = [_decimal_value(row.get("net_realized_pnl")) for row in currency_rows]
        net_by_currency[currency] = (
            _decimal_text(sum((value for value in net_values if value is not None), Decimal("0")))
            if net_values and all(value is not None for value in net_values)
            else None
        )
    cost_coverages = {
        str(row.get("transaction_cost_coverage") or "unknown")
        for row in closed_trades
    }
    transaction_cost_coverage = (
        "complete"
        if not closed_trades or cost_coverages == {"complete"}
        else (
            "partial"
            if "complete" in cost_coverages or "partial" in cost_coverages
            else "unknown"
        )
    )
    eligible_trade_count = sum(
        1 for row in closed_trades if row.get("profitability_eligible") is True
    )
    all_closed_trades_net_eligible = (
        not closed_trades or eligible_trade_count == len(closed_trades)
    )
    known_krw_trades = [
        row for row in closed_trades if row.get("settlement_currency") == "KRW"
    ]
    krw_net_subtotal_available = (
        not closed_trades
        or (
            bool(known_krw_trades)
            and unknown_currency_count == 0
            and all(
                row.get("profitability_eligible") is True
                for row in known_krw_trades
            )
        )
    )
    gross_values = [
        value
        for row in priced_trades
        for value in [_decimal_value(row.get("gross_realized_pnl"))]
        if value is not None
    ]
    estimated_count = sum(
        1
        for row in closed_trades
        if str(row.get("outcome_source") or "") not in {"", "fifo_exact", "unavailable"}
    )
    unavailable_count = sum(1 for row in closed_trades if row.get("outcome_source") == "unavailable")
    quality_flags = sorted(
        {
            str(flag)
            for row in closed_trades
            for flag in (row.get("outcome_quality_flags") or [])
            if str(flag)
        }
    )
    summary: dict[str, object] = {
        "primary_performance_metric": "broker_total_asset_change"
        if broker_asset_performance and broker_asset_performance.get("available")
        else (
            # Keep the legacy metric identifier for consumers while making its
            # economic basis explicit below. It is emitted only for complete,
            # KRW-denominated net outcomes.
            "fifo_realized_pnl"
            if all_closed_trades_net_eligible
            else "fifo_gross_realized_pnl_degraded"
        ),
        "primary_performance_basis": (
            "broker_total_asset_change"
            if broker_asset_performance and broker_asset_performance.get("available")
            else (
                "net_after_transaction_costs"
                if all_closed_trades_net_eligible
                else "gross_diagnostic_only_incomplete_economics"
            )
        ),
        "realized_pnl_krw": _decimal_text(total)
        if krw_net_subtotal_available
        else None,
        "realized_pnl_basis": "net_after_transaction_costs"
        if krw_net_subtotal_available
        else "unavailable_incomplete_transaction_costs",
        "krw_profitability_coverage": (
            "complete" if krw_net_subtotal_available else "incomplete"
        ),
        "gross_realized_pnl_by_currency": gross_by_currency,
        "net_realized_pnl_by_currency": net_by_currency,
        "transaction_cost_coverage": transaction_cost_coverage,
        "currency_coverage": "complete" if unknown_currency_count == 0 else "incomplete",
        "unknown_currency_trade_count": unknown_currency_count,
        "profitability_status": (
            "complete"
            if all_closed_trades_net_eligible
            else (
                "degraded_unknown_currency"
                if unknown_currency_count > 0
                else (
                    "degraded_mixed_currency"
                    if len(currencies) > 1
                    else (
                        "degraded_transaction_cost_coverage"
                        if transaction_cost_coverage != "complete"
                        else "complete_non_krw"
                    )
                )
            )
        ),
        "profitability_eligible_trade_count": eligible_trade_count,
        "excluded_degraded_trade_count": len(closed_trades) - eligible_trade_count,
        "win_count": len(winners),
        "loss_count": len(losers),
        "gross_win_count": sum(1 for value in gross_values if value > 0),
        "gross_loss_count": sum(1 for value in gross_values if value < 0),
        "win_rate_pct": _decimal_text((Decimal(len(winners)) / Decimal(len(pnl_values)) * Decimal("100")) if pnl_values else Decimal("0")),
        "average_win_krw": _decimal_text(sum(winners, Decimal("0")) / Decimal(len(winners)) if winners else Decimal("0")),
        "average_loss_krw": _decimal_text(sum(losers, Decimal("0")) / Decimal(len(losers)) if losers else Decimal("0")),
        "worst_loss_krw": _decimal_text(min(pnl_values) if pnl_values else Decimal("0")),
        "estimated_trade_count": estimated_count,
        "unavailable_trade_count": unavailable_count,
        "realized_pnl_quality_flags": quality_flags,
    }
    if broker_asset_performance and broker_asset_performance.get("available"):
        summary.update(
            {
                "broker_total_asset_baseline_krw": broker_asset_performance.get("baseline_total_asset_krw"),
                "broker_latest_total_asset_krw": broker_asset_performance.get("latest_total_asset_krw"),
                "broker_latest_net_asset_krw": broker_asset_performance.get("latest_net_asset_krw"),
                "broker_total_asset_change_krw": broker_asset_performance.get("asset_change_krw"),
                "broker_total_asset_change_pct": broker_asset_performance.get("asset_change_pct"),
                "broker_unrealized_pnl_krw": broker_asset_performance.get("broker_unrealized_pnl_krw"),
            }
        )
    return summary


def _broker_asset_performance(
    *,
    settings: Settings,
    db: Session,
    closed_trades: list[dict[str, object]],
    lookback_days: int,
    started_at: datetime,
    as_of: datetime,
    requested_scope: FeedbackExecutionScope | None = None,
) -> dict[str, object]:
    snapshot_positions = _load_broker_balance_snapshot_positions(
        settings=settings,
        db=db,
        started_at=started_at,
        lookback_days=lookback_days,
        requested_scope=requested_scope,
    )
    snapshots = _broker_balance_snapshots(
        snapshot_positions,
        require_canonical_identity=requested_scope is not None,
    )
    if not snapshots:
        return _broker_asset_unavailable(
            f"execution_positions raw broker balance payload unavailable for mode={settings.runtime_mode}; "
            "cannot headline broker total asset change"
        )

    account_scopes = {
        (
            snapshot.position_identity.provider,
            snapshot.position_identity.account_scope,
        )
        for snapshot in snapshots
    }
    if len(account_scopes) != 1:
        return _broker_asset_unavailable(
            "multiple broker account scopes are present in balance snapshots; "
            "account-level asset performance is unavailable"
        )
    account_provider, account_scope = next(iter(account_scopes))

    if lookback_days <= 1:
        snapshot = snapshots[0]
        latest_total_asset, latest_net_asset, baseline_total_asset, asset_change = _previous_day_asset_metrics(snapshot.output2)
        baseline_observed_at = None
        source_window = "previous_day"
    else:
        snapshot, baseline_snapshot, reason = _multi_day_broker_asset_snapshots(
            snapshots=snapshots,
            started_at=started_at,
        )
        if snapshot is None or baseline_snapshot is None:
            return _broker_asset_unavailable(reason)
        latest_total_asset = _decimal_value(snapshot.output2.get("tot_evlu_amt"))
        latest_net_asset = _decimal_value(snapshot.output2.get("nass_amt"))
        baseline_total_asset = _decimal_value(baseline_snapshot.output2.get("tot_evlu_amt"))
        asset_change = (
            latest_total_asset - baseline_total_asset
            if latest_total_asset is not None and baseline_total_asset is not None
            else None
        )
        baseline_observed_at = baseline_snapshot.observed_at
        source_window = "report_lookback_snapshots"

    if asset_change is None or latest_total_asset is None or baseline_total_asset is None:
        return _broker_asset_unavailable(
            "broker balance output2 missing parseable total/baseline/change asset fields; "
            "cannot headline broker total asset change"
        )

    position = snapshot.position
    positions = _load_positions(
        settings=settings,
        db=db,
        provider=account_provider,
        account_scope=account_scope,
        exchange=(
            requested_scope.exchange if requested_scope is not None else None
        ),
        require_canonical_identity=requested_scope is not None,
    )
    balance = snapshot.balance
    output2 = snapshot.output2
    raw_balance_valid = broker_raw_balance_is_valid(position.raw_payload)
    raw_balance_fresh = broker_raw_balance_is_fresh(
        observed_at=snapshot.observed_at,
        as_of=as_of,
        max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
    )
    raw_holdings = (
        broker_raw_holdings_from_payload(position.raw_payload)
        if raw_balance_valid and raw_balance_fresh
        else []
    )
    raw_drift = raw_vs_normalized_position_drift(raw_holdings=raw_holdings, positions=positions)
    raw_fields = {key: output2.get(key) for key in BROKER_BALANCE_OUTPUT2_FIELDS if key in output2}
    asset_change_pct = (
        asset_change / baseline_total_asset * Decimal("100")
        if asset_change is not None and baseline_total_asset is not None and baseline_total_asset != 0
        else None
    )
    pnl_eligible = not closed_trades or all(
        row.get("profitability_eligible") is True for row in closed_trades
    )
    realized_pnl = (
        sum(
            (
                _decimal_value(row.get("realized_pnl_krw")) or Decimal("0")
                for row in closed_trades
            ),
            Decimal("0"),
        )
        if pnl_eligible
        else None
    )
    divergence = (
        _asset_fifo_divergence(asset_change=asset_change, realized_pnl=realized_pnl)
        if realized_pnl is not None
        else None
    )
    holdings = [broker_raw_holding_payload(holding) for holding in raw_holdings]
    if not holdings:
        holdings = [_position_holding_fallback(position=row) for row in positions if _position_effective_quantity(row) > 0]
    return {
        "available": True,
        "source": "execution_positions.raw_payload.balance.output2",
        "provider": account_provider,
        "account_scope": account_scope,
        "source_window": source_window,
        "holdings_source": "execution_positions.raw_payload.balance.output1"
        if raw_holdings
        else "execution_positions",
        "raw_balance_valid": raw_balance_valid,
        "raw_balance_fresh": raw_balance_fresh,
        "raw_balance_freshness_threshold_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
        "observed_at": _normalize_utc(position.last_reconciled_at).isoformat()
        if position.last_reconciled_at is not None
        else None,
        "baseline_observed_at": baseline_observed_at.isoformat() if baseline_observed_at is not None else None,
        "source_position_symbol": position.symbol,
        "baseline_total_asset_krw": _decimal_text(baseline_total_asset),
        "latest_total_asset_krw": _decimal_text(latest_total_asset),
        "latest_net_asset_krw": _decimal_text(latest_net_asset),
        "asset_change_krw": _decimal_text(asset_change),
        "asset_change_pct": _decimal_text(asset_change_pct),
        "broker_unrealized_pnl_krw": _decimal_text(_decimal_value(output2.get("evlu_pfls_smtl_amt"))),
        "broker_purchase_amount_krw": _decimal_text(_decimal_value(output2.get("pchs_amt_smtl_amt"))),
        "broker_securities_value_krw": _decimal_text(_decimal_value(output2.get("evlu_amt_smtl_amt"))),
        "broker_cash_krw": _decimal_text(_decimal_value(output2.get("dnca_tot_amt"))),
        "fifo_realized_pnl_krw": _decimal_text(realized_pnl),
        "fifo_realized_pnl_basis": "net_after_transaction_costs"
        if pnl_eligible
        else "unavailable_incomplete_transaction_costs",
        "divergence_warning": divergence,
        "holdings": _json_safe(holdings),
        "raw_normalized_position_drift": _json_safe(raw_drift),
        "raw_normalized_position_drift_count": len(raw_drift),
        "raw_output2_fields": _json_safe(raw_fields),
    }


def _load_broker_balance_snapshot_positions(
    *,
    settings: Settings,
    db: Session,
    started_at: datetime,
    lookback_days: int,
    requested_scope: FeedbackExecutionScope | None = None,
) -> list[ExecutionPosition]:
    where = [
        ExecutionPosition.mode == settings.runtime_mode,
        ExecutionPosition.raw_payload.is_not(None),
    ]
    if requested_scope is not None:
        where.extend(
            [
                ExecutionPosition.provider == requested_scope.provider,
                ExecutionPosition.account_scope == requested_scope.account_scope,
            ]
        )
        if requested_scope.exchange is not None:
            where.append(ExecutionPosition.exchange == requested_scope.exchange)
    base_query = (
        select(ExecutionPosition)
        .where(*where)
        .options(
            load_only(
                ExecutionPosition.id,
                ExecutionPosition.provider,
                ExecutionPosition.account_scope,
                ExecutionPosition.exchange,
                ExecutionPosition.symbol,
                ExecutionPosition.quantity,
                ExecutionPosition.base_quantity,
                ExecutionPosition.average_price,
                ExecutionPosition.last_reconciled_at,
                ExecutionPosition.updated_at,
                ExecutionPosition.raw_payload,
            )
        )
    )
    latest_query = (
        base_query.order_by(
            ExecutionPosition.last_reconciled_at.desc().nulls_last(),
            ExecutionPosition.updated_at.desc(),
            ExecutionPosition.id.desc(),
        )
        .limit(BROKER_BALANCE_SNAPSHOT_CANDIDATE_LIMIT)
    )
    if lookback_days <= 1:
        return list(db.scalars(latest_query))

    started_at = _normalize_utc(started_at)
    baseline_query = (
        base_query.where(ExecutionPosition.last_reconciled_at <= started_at)
        .order_by(
            ExecutionPosition.last_reconciled_at.desc().nulls_last(),
            ExecutionPosition.updated_at.desc(),
            ExecutionPosition.id.desc(),
        )
        .limit(BROKER_BALANCE_SNAPSHOT_CANDIDATE_LIMIT)
    )
    positions_by_id = {position.id: position for position in db.scalars(latest_query)}
    for position in db.scalars(baseline_query):
        positions_by_id.setdefault(position.id, position)
    return list(positions_by_id.values())


def _broker_asset_unavailable(reason: str | None) -> dict[str, object]:
    return {
        "available": False,
        "source": "execution_positions.raw_payload.balance.output2",
        "source_failed": reason or "broker asset performance unavailable",
    }


def _broker_balance_snapshots(
    positions: list[ExecutionPosition],
    *,
    require_canonical_identity: bool = False,
) -> list[BrokerBalanceSnapshot]:
    snapshots = []
    for position in positions:
        try:
            column_identity = position_identity_from_columns(position)
            payload_identity = position_identity_from_payload(position.raw_payload)
        except PositionIdentityConflictError:
            continue
        if column_identity is None or (
            payload_identity is not None and payload_identity != column_identity
        ):
            continue
        if require_canonical_identity and payload_identity is None:
            continue
        payload = position.raw_payload if isinstance(position.raw_payload, dict) else None
        if payload is None:
            continue
        balance = payload.get("balance")
        if not isinstance(balance, dict):
            continue
        output2 = _first_payload_row(balance.get("output2"))
        if output2 is None:
            continue
        observed_at = _normalize_utc(position.last_reconciled_at) if position.last_reconciled_at is not None else None
        snapshots.append(
            BrokerBalanceSnapshot(
                position=position,
                position_identity=column_identity,
                balance=balance,
                output2=output2,
                observed_at=observed_at,
            )
        )
    return sorted(
        snapshots,
        key=lambda row: (
            row.observed_at or datetime.min.replace(tzinfo=UTC),
            _normalize_utc(row.position.updated_at) if row.position.updated_at is not None else datetime.min.replace(tzinfo=UTC),
        ),
        reverse=True,
    )


def _previous_day_asset_metrics(output2: dict[str, object]) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    latest_total_asset = _decimal_value(output2.get("tot_evlu_amt"))
    latest_net_asset = _decimal_value(output2.get("nass_amt"))
    baseline_total_asset = _decimal_value(output2.get("bfdy_tot_asst_evlu_amt"))
    asset_change = _decimal_value(output2.get("asst_icdc_amt"))
    if asset_change is None and latest_total_asset is not None and baseline_total_asset is not None:
        asset_change = latest_total_asset - baseline_total_asset
    if baseline_total_asset is None and latest_total_asset is not None and asset_change is not None:
        baseline_total_asset = latest_total_asset - asset_change
    return latest_total_asset, latest_net_asset, baseline_total_asset, asset_change


def _multi_day_broker_asset_snapshots(
    *,
    snapshots: list[BrokerBalanceSnapshot],
    started_at: datetime,
) -> tuple[BrokerBalanceSnapshot | None, BrokerBalanceSnapshot | None, str | None]:
    started_at = _normalize_utc(started_at)
    latest_snapshot = next(
        (
            snapshot
            for snapshot in snapshots
            if snapshot.observed_at is not None
            and snapshot.observed_at >= started_at
            and _decimal_value(snapshot.output2.get("tot_evlu_amt")) is not None
        ),
        None,
    )
    baseline_snapshot = next(
        (
            snapshot
            for snapshot in snapshots
            if snapshot.observed_at is not None
            and snapshot.observed_at <= started_at
            and _decimal_value(snapshot.output2.get("tot_evlu_amt")) is not None
        ),
        None,
    )
    if (
        latest_snapshot is None
        or baseline_snapshot is None
        or latest_snapshot.observed_at is None
        or baseline_snapshot.observed_at is None
        or latest_snapshot.observed_at <= baseline_snapshot.observed_at
    ):
        return (
            None,
            None,
            "multi-day broker asset performance unavailable: need execution_positions raw balance snapshots "
            f"spanning started_at={started_at.isoformat()}; latest broker output2 previous-day fields "
            "bfdy_tot_asst_evlu_amt/asst_icdc_amt do not cover the report lookback",
        )
    return latest_snapshot, baseline_snapshot, None


def _first_payload_row(value: object) -> dict[str, object] | None:
    rows = _payload_rows(value)
    return rows[0] if rows else None


def _payload_rows(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _broker_holdings_from_output1(value: object) -> list[dict[str, object]]:
    holdings = []
    for row in _payload_rows(value):
        symbol = str(row.get("pdno") or row.get("symbol") or "").strip().upper()
        quantity = _decimal_value(row.get("hldg_qty"))
        if not symbol or quantity is None or quantity <= 0:
            continue
        holdings.append(
            {
                "symbol": symbol,
                "name": row.get("prdt_name") or row.get("prdt_name120"),
                "quantity": _decimal_text(quantity),
                "average_price": _decimal_text(_decimal_value(row.get("pchs_avg_pric"))),
                "purchase_amount_krw": _decimal_text(_decimal_value(row.get("pchs_amt"))),
                "market_value_krw": _decimal_text(_decimal_value(row.get("evlu_amt"))),
                "unrealized_pnl_krw": _decimal_text(_decimal_value(row.get("evlu_pfls_amt"))),
                "unrealized_pnl_pct": _decimal_text(_decimal_value(row.get("evlu_pfls_rt"))),
                "sellable_quantity": _decimal_text(_decimal_value(row.get("ord_psbl_qty"))),
                "raw_payload": _json_safe(row),
            }
        )
    return sorted(holdings, key=lambda row: _decimal_value(row.get("market_value_krw")) or Decimal("0"), reverse=True)


def _position_holding_fallback(*, position: ExecutionPosition) -> dict[str, object]:
    effective_quantity = _position_effective_quantity(position)
    market_value = Decimal(position.average_price) * effective_quantity if position.average_price is not None else None
    return {
        "symbol": position.symbol,
        "name": None,
        "quantity": _quantity_text(effective_quantity),
        "average_price": _decimal_text(Decimal(position.average_price)) if position.average_price is not None else None,
        "purchase_amount_krw": None,
        "market_value_krw": _decimal_text(market_value),
        "unrealized_pnl_krw": None,
        "unrealized_pnl_pct": None,
        "sellable_quantity": None,
        "raw_payload": None,
    }


def _asset_fifo_divergence(*, asset_change: Decimal | None, realized_pnl: Decimal) -> dict[str, object] | None:
    if asset_change is None:
        return None
    difference = asset_change - realized_pnl
    threshold = max(Decimal("50000"), abs(asset_change) * Decimal("0.10"))
    if abs(difference) < threshold:
        return None
    return {
        "type": "broker_asset_change_fifo_realized_pnl_divergence",
        "asset_change_krw": _decimal_text(asset_change),
        "fifo_realized_pnl_krw": _decimal_text(realized_pnl),
        "difference_krw": _decimal_text(difference),
        "threshold_krw": _decimal_text(threshold),
        "reason": "broker total asset movement includes open-position unrealized PnL, cash, fees, and broker reconciliation effects; FIFO realized PnL is closed-trade detail only",
    }


def _portfolio_risk(
    *,
    settings: Settings,
    db: Session,
    closed_trades: list[dict[str, object]],
    as_of: datetime,
    requested_scope: FeedbackExecutionScope | None = None,
) -> dict[str, object]:
    positions = _load_positions(
        settings=settings,
        db=db,
        provider=requested_scope.provider if requested_scope is not None else None,
        account_scope=(
            requested_scope.account_scope if requested_scope is not None else None
        ),
        exchange=requested_scope.exchange if requested_scope is not None else None,
        require_canonical_identity=requested_scope is not None,
    )
    open_orders = _load_open_orders(
        settings=settings,
        db=db,
        requested_scope=requested_scope,
    )
    stale_before = as_of - timedelta(seconds=settings.exec_agent_max_account_snapshot_age_seconds)
    sectors = (
        {}
        if requested_scope is not None
        else _sectors_for_symbols(
            settings=settings,
            db=db,
            symbols={position.symbol for position in positions},
        )
    )
    position_currency_hints = (
        {
            position.symbol: currency_hint
            for position in positions
            if (
                currency_hint := infer_settlement_currency(
                    symbol=position.symbol,
                    provider=position.provider,
                    venue=position.exchange,
                )
            )[0]
            is not None
        }
        if requested_scope is not None
        else load_symbol_settlement_currency_hints(
            settings=settings,
            db=db,
            symbols=(position.symbol for position in positions),
        )
    )
    position_rows = [
        _position_risk_row(
            position=position,
            sector=sectors.get(position.symbol),
            currency_hint=position_currency_hints.get(position.symbol),
        )
        for position in positions
    ]
    rows_with_value = [
        row
        for row in position_rows
        if _decimal_value(row.get("market_value")) is not None
    ]
    currency_aggregation = aggregation_state_with_value_coverage(
        currency_aggregation_state(
            (
                str(row["settlement_currency"])
                if row.get("settlement_currency") is not None
                else None
            )
            for row in position_rows
        ),
        total_count=len(position_rows),
        known_value_count=len(rows_with_value),
        incomplete_reason="incomplete_market_value_coverage",
    )
    unconverted_market_value = sum(
        (_decimal_value(row.get("market_value")) or Decimal("0"))
        for row in rows_with_value
    )
    total_market_value = (
        unconverted_market_value
        if currency_aggregation["status"] == "available"
        and len(rows_with_value) == len(position_rows)
        else None
    )
    totals_by_currency = _position_market_value_totals(position_rows)
    top_concentration = (
        sorted(
            rows_with_value,
            key=lambda row: _decimal_value(row.get("market_value"))
            or Decimal("0"),
            reverse=True,
        )
        if currency_aggregation["status"] == "available"
        else sorted(
            rows_with_value,
            key=lambda row: _portfolio_currency_sort_key(row),
        )
    )[:5]
    for row in top_concentration:
        value = _decimal_value(row.get("market_value")) or Decimal("0")
        currency = (
            str(row["settlement_currency"])
            if row.get("settlement_currency") is not None
            else None
        )
        row["portfolio_weight_pct"] = _decimal_text(
            (value / total_market_value * Decimal("100"))
            if total_market_value is not None and total_market_value > 0
            else None
        )
        currency_total = totals_by_currency.get(currency) if currency is not None else None
        row["currency_weight_pct"] = _decimal_text(
            (value / currency_total * Decimal("100"))
            if currency_total is not None and currency_total > 0
            else None
        )
    stale_positions = [
        {
            "symbol": position.symbol,
            "quantity": position.quantity,
            "base_quantity": _position_base_quantity_text(position),
            "effective_quantity": _position_effective_quantity_text(position),
            "last_reconciled_at": _normalize_utc(position.last_reconciled_at).isoformat()
            if position.last_reconciled_at is not None
            else None,
            "reason": "missing_last_reconciled_at"
            if position.last_reconciled_at is None
            else "last_reconciled_at_older_than_max_age",
        }
        for position in positions
        if position.last_reconciled_at is None or _normalize_utc(position.last_reconciled_at) < stale_before
    ]
    open_order_rows = [
        _open_order_risk_row(
            order=order,
            as_of=as_of,
            stale_before=stale_before,
        )
        for order in open_orders
    ]
    stale_orders = [row for row in open_order_rows if row.get("stale") is True]
    normal_open_orders = [row for row in open_order_rows if row.get("stale") is not True]
    stale_state_blockers = [_stale_state_blocker_row(row) for row in stale_orders]
    account_source_failed = None
    if not position_rows:
        account_source_failed = (
            f"execution_positions returned no positive local positions for mode={settings.runtime_mode}; "
            "broker account snapshots are not persisted by trade_outcome_feedback"
        )
    contributions = _realized_contributions(closed_trades)
    sector_concentration = (
        _sector_concentration(
            position_rows,
            aggregation_available=currency_aggregation["status"] == "available",
            totals_by_currency=totals_by_currency,
        )
        if any(row.get("sector") for row in position_rows)
        else []
    )
    monday_checks = _monday_pre_market_checks(
        positions=position_rows,
        stale_positions=stale_positions,
        open_orders=open_order_rows,
    )
    return {
        "available": bool(position_rows),
        "source": "execution_positions",
        "source_failed": account_source_failed,
        "as_of": as_of.isoformat(),
        "stale_threshold_seconds": settings.exec_agent_max_account_snapshot_age_seconds,
        "aggregation_status": currency_aggregation["status"],
        "aggregation_reason": currency_aggregation["reason"],
        "aggregate_currency": currency_aggregation["aggregate_currency"],
        "currency_aggregation": currency_aggregation,
        "open_holding_count": len(position_rows) if position_rows else None,
        "total_market_value": _decimal_text(total_market_value),
        "total_market_value_krw": (
            _decimal_text(total_market_value)
            if currency_aggregation["aggregate_currency"] == "KRW"
            else None
        ),
        "by_currency": _position_subtotals_by_currency(position_rows),
        "positions_missing_market_value_count": len(position_rows) - len(rows_with_value),
        "top_position_concentration": _json_safe(top_concentration),
        "sector_concentration": _json_safe(sector_concentration),
        "realized_pnl_by_symbol": contributions["by_symbol"],
        "sell_reason_contributions": contributions["by_sell_reason"],
        "stop_sell_loss_contribution": contributions["stop_sell_loss"],
        "target_sell_profit_contribution": contributions["target_sell_profit"],
        "max_holding_close_contribution": contributions["max_holding_close"],
        "open_orders": _json_safe(open_order_rows),
        "open_orders_by_currency": _open_order_subtotals_by_currency(open_order_rows),
        "normal_open_orders": _json_safe(normal_open_orders),
        "stale_state_blockers": _json_safe(stale_state_blockers),
        "stale_open_order_warnings": _json_safe(stale_orders),
        "stale_local_position_warnings": _json_safe(stale_positions),
        "monday_pre_market_checks": _json_safe(monday_checks),
    }


def _load_positions(
    *,
    settings: Settings,
    db: Session,
    provider: str | None = None,
    account_scope: str | None = None,
    exchange: str | None = None,
    require_canonical_identity: bool = False,
) -> list[ExecutionPosition]:
    where = [
        ExecutionPosition.mode == settings.runtime_mode,
        or_(
            ExecutionPosition.quantity > 0,
            ExecutionPosition.base_quantity > 0,
        ),
    ]
    if provider is not None or account_scope is not None or exchange is not None:
        if provider is None or account_scope is None:
            return []
        where.extend(
            [
                ExecutionPosition.provider == provider,
                ExecutionPosition.account_scope == account_scope,
            ]
        )
        if exchange is not None:
            where.append(ExecutionPosition.exchange == exchange)
    position_fields = [
        ExecutionPosition.id,
        ExecutionPosition.provider,
        ExecutionPosition.account_scope,
        ExecutionPosition.exchange,
        ExecutionPosition.symbol,
        ExecutionPosition.quantity,
        ExecutionPosition.base_quantity,
        ExecutionPosition.average_price,
        ExecutionPosition.last_reconciled_at,
        ExecutionPosition.updated_from,
    ]
    if require_canonical_identity:
        position_fields.append(ExecutionPosition.raw_payload)
    positions = list(
        db.scalars(
            select(ExecutionPosition)
            .where(*where)
            .options(
                load_only(*position_fields)
            )
            .order_by(ExecutionPosition.symbol.asc())
        )
    )
    if not require_canonical_identity:
        return positions
    return [
        position
        for position in positions
        if _canonical_position_identity(position) is not None
    ]


def _load_open_orders(
    *,
    settings: Settings,
    db: Session,
    requested_scope: FeedbackExecutionScope | None = None,
) -> list[BrokerOrder]:
    terminal_statuses = {
        "filled",
        "canceled",
        "cancelled",
        "rejected",
        "expired",
        "failed",
        "closed",
    }
    orders = list(
        db.scalars(
            select(BrokerOrder)
            .where(BrokerOrder.mode == settings.runtime_mode)
            .where(BrokerOrder.status.not_in(terminal_statuses))
            .options(selectinload(BrokerOrder.plan_item), selectinload(BrokerOrder.fills))
            .order_by(BrokerOrder.symbol.asc(), BrokerOrder.submitted_at.asc().nulls_last(), BrokerOrder.id.asc())
        )
    )
    if requested_scope is None:
        return orders
    return [
        order
        for order in orders
        if _order_matches_feedback_scope(
            order,
            requested_scope,
        )
    ]


def _position_risk_row(
    *,
    position: ExecutionPosition,
    sector: str | None,
    currency_hint: tuple[str, str] | None,
) -> dict[str, object]:
    effective_quantity = _position_effective_quantity(position)
    market_value = None
    if position.average_price is not None and effective_quantity > 0:
        market_value = Decimal(position.average_price) * effective_quantity
    if currency_hint is not None:
        settlement_currency, currency_source = currency_hint
    else:
        settlement_currency, currency_source = infer_settlement_currency(
            symbol=position.symbol,
        )
    return {
        "provider": position.provider,
        "account_scope": position.account_scope,
        "exchange": position.exchange,
        "symbol": position.symbol,
        "quantity": position.quantity,
        "base_quantity": _position_base_quantity_text(position),
        "effective_quantity": _position_effective_quantity_text(position),
        "average_price": _decimal_text(Decimal(position.average_price)) if position.average_price is not None else None,
        "market_value": _decimal_text(market_value),
        "market_value_krw": (
            _decimal_text(market_value)
            if settlement_currency == "KRW"
            else None
        ),
        "settlement_currency": settlement_currency,
        "currency_source": currency_source,
        "last_reconciled_at": _normalize_utc(position.last_reconciled_at).isoformat()
        if position.last_reconciled_at is not None
        else None,
        "updated_from": position.updated_from,
        "sector": sector,
    }


def _position_effective_quantity(position: ExecutionPosition) -> Decimal:
    if position.quantity > 0:
        return Decimal(position.quantity)
    base_quantity = Decimal(position.base_quantity) if position.base_quantity is not None else Decimal("0")
    return base_quantity if base_quantity > 0 else Decimal("0")


def _position_base_quantity_text(position: ExecutionPosition) -> str | None:
    if position.base_quantity is None:
        return None
    return _quantity_text(Decimal(position.base_quantity))


def _position_effective_quantity_text(position: ExecutionPosition) -> str:
    if position.quantity > 0:
        return _decimal_text(Decimal(position.quantity)) or "0.0000"
    return _quantity_text(_position_effective_quantity(position))


def _open_order_risk_row(*, order: BrokerOrder, as_of: datetime, stale_before: datetime) -> dict[str, object]:
    reference_at = order.last_broker_sync_at or order.submitted_at or order.updated_at or order.created_at
    normalized_reference = _normalize_utc(reference_at) if reference_at is not None else None
    remaining_quantity = _remaining_order_quantity(order)
    is_stale = normalized_reference is None or normalized_reference < stale_before
    settlement_currency, currency_source = infer_order_settlement_currency(order)
    effective_remaining_quantity = (
        Decimal(order.base_quantity)
        if order.base_quantity is not None and order.base_quantity > 0
        else Decimal(remaining_quantity)
        if remaining_quantity is not None
        else None
    )
    notional = (
        Decimal(order.quote_notional)
        if order.quote_notional is not None
        else effective_remaining_quantity * Decimal(order.limit_price)
        if effective_remaining_quantity is not None and order.limit_price is not None
        else None
    )
    return {
        "broker_order_pk": order.id,
        "broker_order_id": order.broker_order_id,
        "execution_plan_item_id": order.execution_plan_item_id,
        "symbol": order.symbol,
        "side": order.side,
        "intent": order.plan_item.intent if order.plan_item is not None else None,
        "status": order.status,
        "quantity": order.quantity,
        "base_quantity": (
            _quantity_text(Decimal(order.base_quantity))
            if order.base_quantity is not None
            else None
        ),
        "remaining_quantity": remaining_quantity,
        "limit_price": _decimal_text(Decimal(order.limit_price)) if order.limit_price is not None else None,
        "notional": _decimal_text(notional),
        "settlement_currency": settlement_currency,
        "currency_source": currency_source,
        "submitted_at": _normalize_utc(order.submitted_at).isoformat() if order.submitted_at is not None else None,
        "last_broker_sync_at": _normalize_utc(order.last_broker_sync_at).isoformat()
        if order.last_broker_sync_at is not None
        else None,
        "age_seconds": int((as_of - normalized_reference).total_seconds()) if normalized_reference is not None else None,
        "stale": is_stale,
        "stale_reason": "missing_order_timestamp" if normalized_reference is None else ("older_than_max_age" if is_stale else None),
        "last_error_classification": order.last_error_classification,
        "last_error_code": order.last_error_code,
        "last_error_message": order.last_error_message,
    }


def _stale_state_blocker_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "type": "stale_open_order_broker_truth",
        "symbol": row.get("symbol"),
        "side": row.get("side"),
        "status": row.get("status"),
        "broker_order_id": row.get("broker_order_id"),
        "broker_order_pk": row.get("broker_order_pk"),
        "stale_reason": row.get("stale_reason"),
        "last_broker_sync_at": row.get("last_broker_sync_at"),
        "age_seconds": row.get("age_seconds"),
        "last_error_classification": row.get("last_error_classification"),
        "last_error_code": row.get("last_error_code"),
        "last_error_message": row.get("last_error_message"),
        "reason": "refresh or resolve broker truth before new automation for this symbol/account",
    }


def _remaining_order_quantity(order: BrokerOrder) -> int | None:
    if order.quantity is None:
        return None
    filled_quantity = sum(int(fill.quantity) for fill in order.fills)
    return max(0, int(order.quantity) - filled_quantity)


def _realized_contributions(closed_trades: list[dict[str, object]]) -> dict[str, object]:
    by_symbol: dict[str, list[Decimal]] = {}
    by_reason: dict[str, list[Decimal]] = {}
    eligible_trades = [
        trade for trade in closed_trades if trade.get("profitability_eligible") is True
    ]
    for trade in eligible_trades:
        symbol = str(trade.get("symbol") or "unknown")
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        reason = _sell_reason(trade)
        by_symbol.setdefault(symbol, []).append(pnl)
        by_reason.setdefault(reason, []).append(pnl)
    return {
        "by_symbol": _contribution_rows(by_symbol),
        "by_sell_reason": _contribution_rows(by_reason),
        "stop_sell_loss": _reason_filtered_contribution(eligible_trades, reason="stop_sell", pnl_filter="loss"),
        "target_sell_profit": _reason_filtered_contribution(eligible_trades, reason="target_sell", pnl_filter="profit"),
        "max_holding_close": _reason_filtered_contribution(eligible_trades, reason="max_holding_close_sell", pnl_filter=None),
        "excluded_degraded_trade_count": len(closed_trades) - len(eligible_trades),
    }


def _contribution_rows(stats: dict[str, list[Decimal]]) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        rows.append(
            {
                "name": name,
                "trade_count": len(values),
                "realized_pnl_krw": _decimal_text(total),
            }
        )
    return sorted(rows, key=lambda row: Decimal(str(row["realized_pnl_krw"])))


def _reason_filtered_contribution(
    closed_trades: list[dict[str, object]],
    *,
    reason: str,
    pnl_filter: str | None,
) -> dict[str, object]:
    values = []
    symbols: dict[str, list[Decimal]] = {}
    for trade in closed_trades:
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        if _sell_reason(trade) != reason:
            continue
        if pnl_filter == "loss" and pnl >= 0:
            continue
        if pnl_filter == "profit" and pnl <= 0:
            continue
        values.append(pnl)
        symbols.setdefault(str(trade.get("symbol") or "unknown"), []).append(pnl)
    return {
        "trade_count": len(values),
        "realized_pnl_krw": _decimal_text(sum(values, Decimal("0"))),
        "by_symbol": _contribution_rows(symbols),
    }


def _sell_reason(trade: dict[str, object]) -> str:
    sell = trade.get("sell") if isinstance(trade.get("sell"), dict) else {}
    intent = str(sell.get("intent") or "").strip()
    if intent:
        return intent
    decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
    reason = str(decision.get("reason_code") or "").strip()
    return reason or "unknown"


def _sectors_for_symbols(*, settings: Settings, db: Session, symbols: set[str]) -> dict[str, str]:
    if not symbols:
        return {}
    normalized_symbols = sorted(symbol.strip().upper() for symbol in symbols if symbol.strip())
    if not normalized_symbols:
        return {}
    ranked_item_ids = (
        select(ExecutionPlanItem)
        .with_only_columns(
            ExecutionPlanItem.id.label("item_id"),
            func.row_number()
            .over(
                partition_by=ExecutionPlanItem.symbol,
                order_by=(
                    ExecutionPlanItem.updated_at.desc().nulls_last(),
                    ExecutionPlanItem.created_at.desc(),
                    ExecutionPlanItem.id.desc(),
                ),
            )
            .label("symbol_rank"),
        )
        .where(
            ExecutionPlanItem.symbol.in_(normalized_symbols),
            ExecutionPlanItem.plan.has(mode=settings.runtime_mode),
        )
        .subquery()
    )
    items = db.scalars(
        select(ExecutionPlanItem)
        .join(ranked_item_ids, ExecutionPlanItem.id == ranked_item_ids.c.item_id)
        .where(ranked_item_ids.c.symbol_rank <= SECTOR_LOOKBACK_PER_SYMBOL)
        .options(
            load_only(
                ExecutionPlanItem.symbol,
                ExecutionPlanItem.risk_context,
                ExecutionPlanItem.updated_at,
                ExecutionPlanItem.created_at,
            )
        )
        .order_by(
            ExecutionPlanItem.symbol.asc(),
            ExecutionPlanItem.updated_at.desc().nulls_last(),
            ExecutionPlanItem.created_at.desc(),
            ExecutionPlanItem.id.desc(),
        )
    )
    sectors: dict[str, str] = {}
    for item in items:
        if item.symbol in sectors:
            continue
        if not isinstance(item.risk_context, dict):
            continue
        for key in ("sector", "sector_name", "industry", "theme"):
            value = item.risk_context.get(key)
            if isinstance(value, str) and value.strip():
                sectors[item.symbol] = value.strip()
                break
    return sectors


def _position_market_value_totals(
    position_rows: list[dict[str, object]],
) -> dict[str, Decimal]:
    values: dict[str, list[Decimal]] = {}
    counts: dict[str, int] = {}
    for row in position_rows:
        currency = str(row.get("settlement_currency") or "").strip()
        if not currency:
            continue
        counts[currency] = counts.get(currency, 0) + 1
        value = _decimal_value(row.get("market_value"))
        if value is not None:
            values.setdefault(currency, []).append(value)
    return {
        currency: sum(currency_values, Decimal("0"))
        for currency, currency_values in values.items()
        if len(currency_values) == counts[currency]
    }


def _position_subtotals_by_currency(
    position_rows: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for row in position_rows:
        currency = str(row.get("settlement_currency") or "").strip() or None
        key = currency or UNKNOWN_CURRENCY_KEY
        entry = stats.setdefault(
            key,
            {"position_count": 0, "known_market_value_count": 0, "values": []},
        )
        entry["position_count"] = int(entry["position_count"]) + 1
        value = _decimal_value(row.get("market_value"))
        if value is not None:
            entry["known_market_value_count"] = (
                int(entry["known_market_value_count"]) + 1
            )
            entry["values"].append(value)
    return {
        key: {
            "position_count": entry["position_count"],
            "known_market_value_count": entry["known_market_value_count"],
            "known_market_value_subtotal": (
                _decimal_text(sum(entry["values"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY and entry["values"]
                else None
            ),
            "total_market_value": (
                _decimal_text(sum(entry["values"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY
                and entry["values"]
                and entry["known_market_value_count"]
                == entry["position_count"]
                else None
            ),
        }
        for key, entry in sorted(stats.items())
    }


def _open_order_subtotals_by_currency(
    open_order_rows: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    for row in open_order_rows:
        currency = str(row.get("settlement_currency") or "").strip() or None
        key = currency or UNKNOWN_CURRENCY_KEY
        entry = stats.setdefault(
            key,
            {"order_count": 0, "known_notional_count": 0, "notionals": []},
        )
        entry["order_count"] = int(entry["order_count"]) + 1
        notional = _decimal_value(row.get("notional"))
        if notional is not None:
            entry["known_notional_count"] = int(entry["known_notional_count"]) + 1
            entry["notionals"].append(notional)
    return {
        key: {
            "order_count": entry["order_count"],
            "known_notional_count": entry["known_notional_count"],
            "known_notional_subtotal": (
                _decimal_text(sum(entry["notionals"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY and entry["notionals"]
                else None
            ),
            "total_notional": (
                _decimal_text(sum(entry["notionals"], Decimal("0")))
                if key != UNKNOWN_CURRENCY_KEY
                and entry["notionals"]
                and entry["known_notional_count"] == entry["order_count"]
                else None
            ),
        }
        for key, entry in sorted(stats.items())
    }


def _portfolio_currency_sort_key(
    row: dict[str, object],
) -> tuple[bool, str, Decimal]:
    currency = str(row.get("settlement_currency") or "").strip() or None
    return (
        currency is None,
        currency or UNKNOWN_CURRENCY_KEY,
        -(_decimal_value(row.get("market_value")) or Decimal("0")),
    )


def _sector_concentration(
    position_rows: list[dict[str, object]],
    *,
    aggregation_available: bool,
    totals_by_currency: dict[str, Decimal],
) -> list[dict[str, object]]:
    stats: dict[tuple[str, str | None], list[Decimal]] = {}
    for row in position_rows:
        sector = str(row.get("sector") or "").strip()
        currency = str(row.get("settlement_currency") or "").strip() or None
        value = _decimal_value(row.get("market_value"))
        if sector and value is not None:
            stats.setdefault((sector, currency), []).append(value)
    total = sum(
        (sum(values, Decimal("0")) for values in stats.values()),
        Decimal("0"),
    )
    rows = []
    for (sector, currency), values in stats.items():
        value = sum(values, Decimal("0"))
        rows.append(
            {
                "sector": sector,
                "position_count": len(values),
                "settlement_currency": currency,
                "market_value": _decimal_text(value),
                "market_value_krw": (
                    _decimal_text(value) if currency == "KRW" else None
                ),
                "portfolio_weight_pct": _decimal_text(
                    (value / total * Decimal("100"))
                    if aggregation_available and total > 0
                    else None
                ),
                "currency_weight_pct": _decimal_text(
                    (value / totals_by_currency[currency] * Decimal("100"))
                    if currency is not None and totals_by_currency[currency] > 0
                    else None
                ),
            }
        )
    if aggregation_available:
        return sorted(
            rows,
            key=lambda row: _decimal_value(row["market_value"]) or Decimal("0"),
            reverse=True,
        )
    return sorted(rows, key=_portfolio_currency_sort_key)


def _monday_pre_market_checks(
    *,
    positions: list[dict[str, object]],
    stale_positions: list[dict[str, object]],
    open_orders: list[dict[str, object]],
) -> list[dict[str, object]]:
    checks = []
    for order in open_orders:
        checks.append(
            {
                "type": "open_order",
                "symbol": order.get("symbol"),
                "side": order.get("side"),
                "status": order.get("status"),
                "reason": "confirm broker open-order truth before Monday pre-market",
            }
        )
    stale_symbols = {str(row.get("symbol")) for row in stale_positions}
    for position in positions:
        if str(position.get("symbol")) in stale_symbols:
            checks.append(
                {
                    "type": "stale_local_position",
                    "symbol": position.get("symbol"),
                    "quantity": position.get("quantity"),
                    "base_quantity": position.get("base_quantity"),
                    "effective_quantity": position.get("effective_quantity"),
                    "reason": "refresh local position against broker truth before Monday pre-market",
                }
            )
        else:
            checks.append(
                {
                    "type": "open_position",
                    "symbol": position.get("symbol"),
                    "quantity": position.get("quantity"),
                    "base_quantity": position.get("base_quantity"),
                    "effective_quantity": position.get("effective_quantity"),
                    "market_value": position.get("market_value"),
                    "market_value_krw": position.get("market_value_krw"),
                    "settlement_currency": position.get("settlement_currency"),
                    "reason": "review concentration and exit monitors before Monday pre-market",
                }
            )
    return checks


def _lessons(closed_trades: list[dict[str, object]], *, max_lessons: int) -> dict[str, object]:
    eligible_trades = [
        trade for trade in closed_trades if trade.get("profitability_eligible") is True
    ]
    worst_losses = sorted(
        eligible_trades,
        key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"),
    )[:max_lessons]
    profile_stats: dict[str, list[Decimal]] = {}
    reason_stats: dict[str, list[Decimal]] = {}
    for trade in eligible_trades:
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
        profile = str(decision.get("target_profile") or "unknown")
        reason = str(decision.get("reason_code") or "unknown")
        profile_stats.setdefault(profile, []).append(pnl)
        reason_stats.setdefault(reason, []).append(pnl)
    return {
        "worst_losses": [_lesson_trade(row) for row in worst_losses],
        "patterns_to_downsize_or_reject": _negative_patterns(profile_stats, max_lessons=max_lessons),
        "positive_expectancy_patterns": _positive_patterns(profile_stats, max_lessons=max_lessons),
        "negative_reason_codes": _negative_patterns(reason_stats, max_lessons=max_lessons),
        "eligible_trade_count": len(eligible_trades),
        "excluded_degraded_trade_count": len(closed_trades) - len(eligible_trades),
    }


def _candidate_rules(settings: Settings) -> list[CandidateRule]:
    return [
        CandidateRule(
            rule_id="stale_trigger_drift_veto",
            name="Stale trigger drift veto",
            action="veto",
            size_multiplier=Decimal("0"),
            threshold=Decimal(settings.watch_buy_stale_trigger_reclaim_threshold_pct),
            evaluator=_rule_stale_trigger_drift,
        ),
        CandidateRule(
            rule_id="high_volatility_downsize",
            name="High volatility downsize",
            action="downsize",
            size_multiplier=Decimal("0.50"),
            threshold=Decimal(settings.watch_buy_high_atr_pct_threshold),
            evaluator=_rule_high_volatility,
        ),
        CandidateRule(
            rule_id="high_volatility_chase_veto",
            name="High-volatility chase veto",
            action="veto",
            size_multiplier=Decimal("0"),
            threshold=Decimal(settings.watch_buy_high_volatility_chase_atr_pct_threshold),
            evaluator=_rule_high_volatility_chase,
            return_5d_threshold=Decimal(settings.watch_buy_high_volatility_chase_ret5_threshold_pct),
        ),
        CandidateRule(
            rule_id="high_correlation_downsize",
            name="High correlation downsize",
            action="downsize",
            size_multiplier=Decimal("0.50"),
            threshold=Decimal("0.75"),
            evaluator=_rule_high_correlation,
        ),
        CandidateRule(
            rule_id="weak_confidence_reject",
            name="Weak confidence reject",
            action="reject",
            size_multiplier=Decimal("0"),
            threshold=Decimal("0.50"),
            evaluator=_rule_weak_confidence,
        ),
        CandidateRule(
            rule_id="post_entry_no_progress_exit_tighten",
            name="Post-entry no-progress exit tighten",
            action="exit_tighten",
            size_multiplier=Decimal("1"),
            threshold=Decimal("2"),
            evaluator=_rule_post_entry_no_progress_exit_tighten,
        ),
    ]


def _candidate_rule_replay(*, settings: Settings, closed_trades: list[dict[str, object]]) -> dict[str, object]:
    eligible_trades = [
        trade for trade in closed_trades if trade.get("profitability_eligible") is True
    ]
    incomplete_cost_trade_count = sum(
        1
        for trade in closed_trades
        if trade.get("transaction_cost_coverage") != "complete"
    )
    if eligible_trades:
        replay_status = "ready" if len(eligible_trades) == len(closed_trades) else "degraded"
        blocked_reason = None
    elif closed_trades and incomplete_cost_trade_count:
        replay_status = "blocked"
        blocked_reason = "transaction_costs_not_ingested"
    elif closed_trades:
        replay_status = "blocked"
        blocked_reason = "no_krw_net_eligible_trades"
    else:
        replay_status = "no_data"
        blocked_reason = "no_closed_trades"
    baseline_pnls = [_decimal_value(row.get("realized_pnl_krw")) or Decimal("0") for row in eligible_trades]
    baseline_gross_pnls = [
        _decimal_value(row.get("gross_realized_pnl_krw")) or Decimal("0")
        for row in eligible_trades
    ]
    baseline_returns = [_decimal_value(row.get("return_pct")) or Decimal("0") for row in eligible_trades]
    baseline_net = sum(baseline_pnls, Decimal("0"))
    baseline_gross = sum(baseline_gross_pnls, Decimal("0"))
    baseline_win_rate = _win_rate_pct(baseline_pnls)
    baseline_avg_return = _average_decimal(baseline_returns)
    fee_slippage_bps = _fee_slippage_bps(eligible_trades)
    rule_results = [
        _candidate_rule_result(
            rule=rule,
            closed_trades=eligible_trades,
            baseline_pnls=baseline_pnls,
            baseline_gross_pnls=baseline_gross_pnls,
            baseline_returns=baseline_returns,
            baseline_net=baseline_net,
            baseline_gross=baseline_gross,
            baseline_win_rate=baseline_win_rate,
            baseline_avg_return=baseline_avg_return,
        )
        for rule in _candidate_rules(settings)
    ]
    if replay_status != "ready":
        for result in rule_results:
            if result.get("recommendation") == "promote_to_issue":
                result["recommendation"] = "needs_more_data"
            result["promotion_eligible"] = False
            result["promotion_blocked_reason"] = blocked_reason or (
                "incomplete_profitability_cohort"
            )
    return {
        "source": "execution_fills+broker_orders+execution_plan_items.risk_context",
        "status": replay_status,
        "promotion_eligible": replay_status == "ready",
        "blocked_reason": blocked_reason,
        "baseline_closed_trade_count": len(eligible_trades),
        "excluded_degraded_trade_count": len(closed_trades) - len(eligible_trades),
        "incomplete_transaction_cost_trade_count": incomplete_cost_trade_count,
        "gross_diagnostics_available": any(
            row.get("gross_realized_pnl") is not None for row in closed_trades
        ),
        "baseline_pnl_basis": "net_after_transaction_costs",
        "baseline_net_pnl_krw": _decimal_text(baseline_net),
        "baseline_gross_pnl_krw": _decimal_text(baseline_gross),
        "baseline_win_rate_pct": _decimal_text(baseline_win_rate),
        "baseline_average_return_pct": _decimal_text(baseline_avg_return),
        "fee_slippage_assumption_bps": _decimal_text(fee_slippage_bps),
        "rules": rule_results,
    }


def _candidate_rule_result(
    *,
    rule: CandidateRule,
    closed_trades: list[dict[str, object]],
    baseline_pnls: list[Decimal],
    baseline_gross_pnls: list[Decimal],
    baseline_returns: list[Decimal],
    baseline_net: Decimal,
    baseline_gross: Decimal,
    baseline_win_rate: Decimal,
    baseline_avg_return: Decimal,
) -> dict[str, object]:
    examples: list[dict[str, object]] = []
    candidate_pnls: list[Decimal] = []
    candidate_gross_pnls: list[Decimal] = []
    candidate_returns: list[Decimal] = []
    skipped_entries = 0
    skipped_winners = 0
    avoided_losers = 0
    affected_count = 0
    promotion_eligible_match_count = 0
    promotion_excluded_match_count = 0
    for trade, baseline_pnl, baseline_gross_pnl, baseline_return in zip(
        closed_trades,
        baseline_pnls,
        baseline_gross_pnls,
        baseline_returns,
        strict=False,
    ):
        matched, evidence = rule.evaluator(trade, rule)
        if matched:
            affected_count += 1
            replay = _candidate_trade_replay(
                trade=trade,
                rule=rule,
                baseline_pnl=baseline_pnl,
                baseline_return=baseline_return,
                evidence=evidence,
            )
            if replay.skipped_entry:
                skipped_entries += 1
            if replay.skipped_winner:
                skipped_winners += 1
            if replay.avoided_loser:
                avoided_losers += 1
            if rule.action == "exit_tighten":
                if replay.evidence.get("replay_eligible") is True:
                    promotion_eligible_match_count += 1
                else:
                    promotion_excluded_match_count += 1
            else:
                promotion_eligible_match_count += 1
            examples.append(
                _candidate_rule_example(
                    trade=trade,
                    baseline_pnl=baseline_pnl,
                    candidate_pnl=replay.pnl,
                    evidence=replay.evidence,
                )
            )
            candidate_pnls.append(replay.pnl)
            candidate_gross_pnls.append(
                _candidate_gross_replay_pnl(
                    rule=rule,
                    baseline_gross_pnl=baseline_gross_pnl,
                    replay=replay,
                )
            )
            candidate_returns.append(replay.return_pct)
        else:
            candidate_pnls.append(baseline_pnl)
            candidate_gross_pnls.append(baseline_gross_pnl)
            candidate_returns.append(baseline_return)
    candidate_net = sum(candidate_pnls, Decimal("0"))
    candidate_gross = sum(candidate_gross_pnls, Decimal("0"))
    net_delta = candidate_net - baseline_net
    gross_delta = candidate_gross - baseline_gross
    win_rate_delta = _win_rate_pct(candidate_pnls) - baseline_win_rate
    avg_return_delta = _average_decimal(candidate_returns) - baseline_avg_return
    max_drawdown_proxy = _max_drawdown_proxy(candidate_pnls)
    recommendation = _candidate_rule_recommendation(
        matched_trade_count=promotion_eligible_match_count,
        net_delta=net_delta,
        skipped_winners=skipped_winners,
    )
    return {
        "rule_id": rule.rule_id,
        "rule_name": rule.name,
        "action": rule.action,
        "threshold": _decimal_text(rule.threshold),
        "size_multiplier": _decimal_text(rule.size_multiplier),
        "baseline_closed_trade_count": len(closed_trades),
        "candidate_closed_trade_count": len(candidate_pnls) - skipped_entries,
        "matched_trade_count": affected_count,
        "replay_eligible_count": promotion_eligible_match_count,
        "replay_ineligible_count": promotion_excluded_match_count,
        "promotion_eligible_matched_trade_count": promotion_eligible_match_count,
        "promotion_excluded_matched_trade_count": promotion_excluded_match_count,
        "skipped_entries": skipped_entries,
        "skipped_winners": skipped_winners,
        "avoided_losers": avoided_losers,
        "gross_pnl_delta_krw": _decimal_text(gross_delta),
        "net_pnl_delta_krw": _decimal_text(net_delta),
        "estimated_net_pnl_delta_krw": _decimal_text(net_delta),
        "recommendation_pnl_basis": "net_after_transaction_costs",
        "win_rate_delta_pct": _decimal_text(win_rate_delta),
        "average_return_delta_pct": _decimal_text(avg_return_delta),
        "max_drawdown_proxy_krw": _decimal_text(max_drawdown_proxy),
        "promotion_eligible": recommendation == "promote_to_issue",
        "recommendation": recommendation,
        "top_matched_examples": _top_candidate_examples(examples, outcome="matched")[:5],
        "top_skipped_winners": _top_candidate_examples(examples, outcome="skipped_win")[:5],
        "top_avoided_losers": _top_candidate_examples(examples, outcome="avoided_loss")[:5],
    }


def _candidate_trade_replay(
    *,
    trade: dict[str, object],
    rule: CandidateRule,
    baseline_pnl: Decimal,
    baseline_return: Decimal,
    evidence: dict[str, object],
) -> CandidateTradeReplay:
    if rule.action == "exit_tighten":
        return _exit_tighten_trade_replay(
            trade=trade,
            baseline_pnl=baseline_pnl,
            baseline_return=baseline_return,
            evidence=evidence,
        )
    multiplier = rule.size_multiplier
    skipped_entry = multiplier <= 0
    replay_pnl = baseline_pnl * multiplier
    replay_return = Decimal("0") if skipped_entry else baseline_return
    replay_cost_model = "baseline_net_scaled"
    if skipped_entry:
        replay_pnl = Decimal("0")
        replay_cost_model = "no_entry_no_transaction_cost"
    elif multiplier < 1:
        baseline_gross_pnl = _decimal_value(trade.get("gross_realized_pnl_krw"))
        observed_transaction_cost = _decimal_value(trade.get("known_transaction_cost"))
        buy_price = _nested_decimal(trade, ("buy", "price"))
        quantity = (
            _decimal_value(trade.get("quantity"))
            or _nested_decimal(trade, ("buy", "quantity"))
        )
        if baseline_gross_pnl is not None and observed_transaction_cost is not None:
            # A reduced order may still pay the same minimum/fixed fee. Keeping the
            # entire observed cost is a conservative lower bound; scaling net P&L
            # would incorrectly assume every cost component scales with quantity.
            replay_pnl = baseline_gross_pnl * multiplier - observed_transaction_cost
            replay_cost_model = "scaled_gross_minus_full_observed_cost"
            scaled_buy_notional = (
                buy_price * quantity * multiplier
                if buy_price is not None and quantity is not None
                else None
            )
            if scaled_buy_notional is not None and scaled_buy_notional > 0:
                replay_return = replay_pnl / scaled_buy_notional * Decimal("100")
    return CandidateTradeReplay(
        pnl=replay_pnl,
        return_pct=replay_return,
        skipped_entry=skipped_entry,
        skipped_winner=skipped_entry and baseline_pnl > 0,
        avoided_loser=baseline_pnl < 0,
        evidence={
            **evidence,
            "replay_model": "entry_size_multiplier",
            "transaction_cost_replay_model": replay_cost_model,
            "replay_net_pnl_krw": _decimal_text(replay_pnl),
            "counts_as_skipped_winner": skipped_entry and baseline_pnl > 0,
            "counts_as_avoided_loser": baseline_pnl < 0,
        },
    )


def _candidate_gross_replay_pnl(
    *,
    rule: CandidateRule,
    baseline_gross_pnl: Decimal,
    replay: CandidateTradeReplay,
) -> Decimal:
    if rule.action == "exit_tighten":
        replay_gross = _decimal_value(replay.evidence.get("replay_gross_pnl_krw"))
        return replay_gross if replay_gross is not None else baseline_gross_pnl
    return baseline_gross_pnl * rule.size_multiplier


def _exit_tighten_trade_replay(
    *,
    trade: dict[str, object],
    baseline_pnl: Decimal,
    baseline_return: Decimal,
    evidence: dict[str, object],
) -> CandidateTradeReplay:
    buy_price = _nested_decimal(trade, ("buy", "price"))
    sell_price = _nested_decimal(trade, ("sell", "price"))
    quantity = (
        _decimal_value(trade.get("quantity"))
        or _nested_decimal(trade, ("sell", "quantity"))
        or _nested_decimal(trade, ("buy", "quantity"))
    )
    tighten_context = _post_entry_tighten_replay_context(trade)
    replay_evidence = {**evidence, **tighten_context, "replay_model": "post_entry_tightened_exit"}
    if buy_price is None or sell_price is None or quantity is None or buy_price <= 0 or quantity <= 0:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "missing_trade_price_or_quantity",
            },
        )
    tightened_stop = _decimal_value(tighten_context.get("tightened_exit_price"))
    if tightened_stop is None or tightened_stop <= 0:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "missing_tightened_exit_price",
            },
        )
    tighten_recorded_at = _parse_optional_aware_datetime(
        tighten_context.get("tighten_recorded_at")
    )
    sell_filled_at = _parse_optional_aware_datetime(
        (trade.get("sell") or {}).get("filled_at")
        if isinstance(trade.get("sell"), dict)
        else None
    )
    if tighten_recorded_at is None or sell_filled_at is None:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                "baseline_sell_price": _decimal_text(sell_price),
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "missing_tighten_or_sell_timestamp",
            },
        )
    if tighten_recorded_at >= sell_filled_at:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                "baseline_sell_price": _decimal_text(sell_price),
                "sell_filled_at": sell_filled_at.isoformat(),
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "tighten_recorded_at_or_after_actual_sell",
            },
        )
    execution_evidence = _stored_tightened_exit_execution_evidence(
        trade=trade,
        tightened_stop=tightened_stop,
        tighten_recorded_at=tighten_recorded_at,
        sell_filled_at=sell_filled_at,
    )
    replay_exit_price = _decimal_value(execution_evidence.get("execution_price"))
    if replay_exit_price is None or replay_exit_price <= 0:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                "baseline_sell_price": _decimal_text(sell_price),
                "sell_filled_at": sell_filled_at.isoformat(),
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "missing_post_tighten_execution_evidence",
            },
        )
    replay_gross_pnl = (replay_exit_price - buy_price) * quantity
    known_transaction_cost = _decimal_value(trade.get("known_transaction_cost"))
    if known_transaction_cost is None:
        return CandidateTradeReplay(
            pnl=baseline_pnl,
            return_pct=baseline_return,
            skipped_entry=False,
            skipped_winner=False,
            avoided_loser=False,
            evidence={
                **replay_evidence,
                **execution_evidence,
                "counts_as_skipped_winner": False,
                "counts_as_avoided_loser": False,
                "replay_eligible": False,
                "replay_status": "transaction_cost_unavailable",
            },
        )
    replay_pnl = replay_gross_pnl - known_transaction_cost
    buy_notional = buy_price * quantity
    replay_return = replay_pnl / buy_notional * Decimal("100")
    return CandidateTradeReplay(
        pnl=replay_pnl,
        return_pct=replay_return,
        skipped_entry=False,
        skipped_winner=False,
        avoided_loser=baseline_pnl < 0 and replay_pnl >= 0,
        evidence={
            **replay_evidence,
            **execution_evidence,
            "baseline_sell_price": _decimal_text(sell_price),
            "replay_exit_price": _decimal_text(replay_exit_price),
            "replay_gross_pnl_krw": _decimal_text(replay_gross_pnl),
            "replay_pnl_krw": _decimal_text(replay_pnl),
            "replay_pnl_basis": "net_with_actual_transaction_cost_held_constant",
            "actual_transaction_cost_krw": _decimal_text(known_transaction_cost),
            "counts_as_skipped_winner": False,
            "counts_as_avoided_loser": baseline_pnl < 0 and replay_pnl >= 0,
            "replay_eligible": True,
            "replay_status": "replayed_tightened_exit_price",
        },
    )


def _stored_tightened_exit_execution_evidence(
    *,
    trade: dict[str, object],
    tightened_stop: Decimal,
    tighten_recorded_at: datetime,
    sell_filled_at: datetime,
) -> dict[str, object]:
    """Return the earliest stored, conservative sell-stop execution evidence.

    A configured stop is not a fill. Only quote or OHLC observations recorded
    after the tighten decision and no later than the actual sell may support the
    counterfactual. For quote evidence we prefer the executable bid. For OHLC
    evidence the stored low is deliberately used as the conservative price;
    assuming a fill at the stop would overstate a gap-down exit.
    """

    context = trade.get("candidate_rule_context")
    candidates: list[tuple[datetime, Decimal, str]] = []
    for node in _iter_dict_nodes(context):
        observed_at = _first_node_datetime(
            node,
            keys=(
                "observed_at",
                "quoted_at",
                "timestamp",
                "as_of",
                "recorded_at",
                "triggered_at",
            ),
        )
        low_price = _first_node_decimal(node, keys=("low_price", "day_low", "low"))
        interval_started_at = _first_node_datetime(
            node,
            keys=(
                "interval_started_at",
                "interval_start",
                "bar_started_at",
                "bar_start",
                "opened_at",
            ),
        )
        if (
            low_price is not None
            and Decimal("0") < low_price <= tightened_stop
            and interval_started_at is not None
            and interval_started_at > tighten_recorded_at
            and interval_started_at <= sell_filled_at
        ):
            candidates.append((interval_started_at, low_price, "stored_ohlc_low"))
            continue

        if (
            observed_at is None
            or observed_at <= tighten_recorded_at
            or observed_at > sell_filled_at
        ):
            continue

        quote_price, quote_field = _first_node_decimal_with_key(
            node,
            keys=(
                "bid_price",
                "bid",
                "trade_price",
            ),
        )
        if quote_price is not None and Decimal("0") < quote_price <= tightened_stop:
            candidates.append(
                (observed_at, quote_price, f"stored_quote.{quote_field}")
            )

    if not candidates:
        return {}
    observed_at, execution_price, source = min(candidates, key=lambda row: row[0])
    return {
        "execution_price": _decimal_text(execution_price),
        "execution_evidence_observed_at": observed_at.isoformat(),
        "execution_evidence_source": source,
        "execution_price_model": "stored_conservative_tradable_price",
    }


def _first_node_datetime(
    node: dict[str, object],
    *,
    keys: tuple[str, ...],
) -> datetime | None:
    for key in keys:
        parsed = _parse_optional_aware_datetime(node.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_optional_aware_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _first_node_decimal(
    node: dict[str, object],
    *,
    keys: tuple[str, ...],
) -> Decimal | None:
    return _first_node_decimal_with_key(node, keys=keys)[0]


def _first_node_decimal_with_key(
    node: dict[str, object],
    *,
    keys: tuple[str, ...],
) -> tuple[Decimal | None, str | None]:
    for key in keys:
        value = _decimal_value(node.get(key))
        if value is not None:
            return value, key
    return None, None


def _candidate_rule_recommendation(
    *,
    matched_trade_count: int,
    net_delta: Decimal,
    skipped_winners: int,
) -> str:
    if matched_trade_count < MIN_CANDIDATE_RULE_SAMPLE_SIZE:
        return "needs_more_data"
    if net_delta > 0 and skipped_winners == 0:
        return "promote_to_issue"
    if net_delta < 0:
        return "reject_candidate"
    return "needs_more_data"


def _candidate_rule_example(
    *,
    trade: dict[str, object],
    baseline_pnl: Decimal,
    candidate_pnl: Decimal,
    evidence: dict[str, object],
) -> dict[str, object]:
    buy = trade.get("buy") if isinstance(trade.get("buy"), dict) else {}
    return {
        "symbol": trade.get("symbol"),
        "realized_pnl_krw": _decimal_text(baseline_pnl),
        "candidate_pnl_krw": _decimal_text(candidate_pnl),
        "return_pct": trade.get("return_pct"),
        "buy_filled_at": buy.get("filled_at"),
        "execution_plan_item_id": buy.get("execution_plan_item_id") or trade.get("source_execution_plan_item_id"),
        "decision_run_id": trade.get("decision_run_id"),
        "evidence": _json_safe(evidence),
    }


def _top_candidate_examples(examples: list[dict[str, object]], *, outcome: str) -> list[dict[str, object]]:
    if outcome == "matched":
        return sorted(
            examples,
            key=lambda row: abs(
                (_decimal_value(row.get("candidate_pnl_krw")) or Decimal("0"))
                - (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0"))
            ),
            reverse=True,
        )
    if outcome == "skipped_win":
        winners = [
            row
            for row in examples
            if (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0")) > 0
            and isinstance(row.get("evidence"), dict)
            and row["evidence"].get("counts_as_skipped_winner") is True
        ]
        return sorted(winners, key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"), reverse=True)
    losers = [
        row
        for row in examples
        if (_decimal_value(row.get("realized_pnl_krw")) or Decimal("0")) < 0
        and isinstance(row.get("evidence"), dict)
        and row["evidence"].get("counts_as_avoided_loser") is True
    ]
    return sorted(losers, key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"))


def _rule_stale_trigger_drift(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    trigger_price = _candidate_context_decimal(trade, keys=("trigger_price", "watch_trigger_price", "buy_trigger_price", "entry_trigger_price"))
    buy_price = _nested_decimal(trade, ("buy", "price"))
    if trigger_price is None or trigger_price <= 0 or buy_price is None:
        return False, {"reason": "missing_trigger_or_buy_price"}
    drift_pct = (buy_price - trigger_price) / trigger_price * Decimal("100")
    return drift_pct >= rule.threshold, {"trigger_price": _decimal_text(trigger_price), "buy_price": _decimal_text(buy_price), "drift_pct": _decimal_text(drift_pct)}


def _rule_high_volatility(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "atr_pct",
            "atr_pct_14",
            "atr14_pct",
            "atr_percent",
            "volatility_pct",
            "realized_volatility_pct",
            "intraday_range_pct",
            "daily_range_pct",
        ),
    )
    if value is None:
        return False, {"reason": "missing_volatility_context"}
    return value >= rule.threshold, {"field": key, "value_pct": _decimal_text(value)}


def _rule_high_volatility_chase(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    if not _candidate_trade_is_chase_entry(trade):
        return False, {"reason": "not_chase_entry"}
    atr, atr_key = _candidate_context_first_decimal(
        trade,
        keys=("atr_pct_14", "atr_pct", "atr14_pct", "atr_percent"),
    )
    ret5, ret5_key = _candidate_context_first_decimal(
        trade,
        keys=("return_5d", "ret5", "ret_5d", "pct_change_5d"),
    )
    if atr is None or ret5 is None:
        return False, {"reason": "missing_atr_or_return_5d_context", "atr_field": atr_key, "return_5d_field": ret5_key}
    ret5_threshold = rule.return_5d_threshold
    if ret5_threshold is None:
        return False, {"reason": "missing_return_5d_threshold"}
    matched = atr > rule.threshold and ret5 > ret5_threshold
    return matched, {
        "atr_field": atr_key,
        "atr_pct_14": _decimal_text(atr),
        "atr_threshold_pct": _decimal_text(rule.threshold),
        "return_5d_field": ret5_key,
        "return_5d": _decimal_text(ret5),
        "return_5d_threshold_pct": _decimal_text(ret5_threshold),
        "trigger_reason": _candidate_trade_trigger_reason(trade),
    }


def _candidate_trade_is_chase_entry(trade: dict[str, object]) -> bool:
    trigger_reason = _candidate_trade_trigger_reason(trade)
    if trigger_reason in {
        "intraday_watch_buy_day_high_cross",
        "intraday_watch_buy_breakout_cross",
        "intraday_watch_buy_momentum_cross",
    }:
        return True
    context = trade.get("candidate_rule_context")
    if not isinstance(context, dict):
        return False
    intent = str(context.get("intent") or (trade.get("buy") or {}).get("intent") or "").strip().lower()
    direction = str(context.get("trigger_direction") or context.get("direction") or "").strip().lower()
    return intent in {"intraday_watch_buy", "chase_buy", "tactical_buy"} and direction in {
        "at_or_above",
        "above",
        "breakout",
        "momentum",
        "up",
    }


def _candidate_trade_trigger_reason(trade: dict[str, object]) -> str | None:
    for node in _iter_dict_nodes(trade.get("candidate_rule_context")):
        for key in ("trigger_reason", "reason"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
    return None


def _rule_high_correlation(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "portfolio_correlation",
            "max_correlation",
            "correlation",
            "sector_correlation",
            "market_correlation",
        ),
    )
    if value is None:
        return False, {"reason": "missing_correlation_context"}
    return value >= rule.threshold, {"field": key, "value": _decimal_text(value)}


def _rule_weak_confidence(trade: dict[str, object], rule: CandidateRule) -> tuple[bool, dict[str, object]]:
    value, key = _candidate_context_first_decimal(
        trade,
        keys=(
            "confidence",
            "confidence_score",
            "confidence_score_10",
            "probability",
            "score",
        ),
    )
    if value is None:
        return False, {"reason": "missing_confidence_context"}
    normalized = value / Decimal("10") if key == "confidence_score_10" or value > 1 else value
    return normalized < rule.threshold, {"field": key, "confidence": _decimal_text(normalized)}


def _rule_post_entry_no_progress_exit_tighten(
    trade: dict[str, object],
    rule: CandidateRule,
) -> tuple[bool, dict[str, object]]:
    context = trade.get("candidate_rule_context")
    if not isinstance(context, dict):
        return False, {"reason": "missing_candidate_rule_context"}
    reasons = _post_entry_review_reasons(context)
    matched_reasons = [
        reason
        for reason in reasons
        if reason
        in {
            "gap_down_breached_risk_threshold",
            "next_day_no_progress",
            "max_holding_without_progress",
        }
    ]
    matched = len(set(matched_reasons)) >= int(rule.threshold)
    return matched, {"matched_reasons": sorted(set(matched_reasons)), "threshold": int(rule.threshold)}


def _post_entry_tighten_replay_context(trade: dict[str, object]) -> dict[str, object]:
    context = trade.get("candidate_rule_context")
    for node in _iter_dict_nodes(context):
        tightened_stop = _decimal_value(node.get("tightened_stop_price"))
        if tightened_stop is not None:
            return _json_safe(
                {
                    "tightened_exit_price": _decimal_text(tightened_stop),
                    "observed_price": _decimal_text(_decimal_value(node.get("observed_price"))),
                    "tighten_recorded_at": node.get("recorded_at") or node.get("observed_at") or node.get("triggered_at"),
                    "tightened_exit_source": "post_entry_review.tightened_stop_price",
                }
            )
    for node in _iter_dict_nodes(context):
        if node.get("event") != "post_entry_stop_monitor_tightened":
            continue
        trigger_price = _decimal_value(node.get("trigger_price"))
        if trigger_price is not None:
            return _json_safe(
                {
                    "tightened_exit_price": _decimal_text(trigger_price),
                    "observed_price": _decimal_text(_decimal_value(node.get("observed_price"))),
                    "tighten_recorded_at": node.get("recorded_at") or node.get("observed_at") or node.get("triggered_at"),
                    "tightened_exit_source": "post_entry_stop_monitor_tightened.trigger_price",
                }
            )
    return {}


def _iter_dict_nodes(value: object) -> Iterator[dict[str, object]]:
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _iter_dict_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_dict_nodes(item)


def _candidate_context_decimal(trade: dict[str, object], *, keys: tuple[str, ...]) -> Decimal | None:
    return _candidate_context_first_decimal(trade, keys=keys)[0]


def _candidate_context_first_decimal(trade: dict[str, object], *, keys: tuple[str, ...]) -> tuple[Decimal | None, str | None]:
    contexts = [
        trade.get("candidate_rule_context"),
        trade.get("exec_agent_decision"),
        trade.get("risk_sizing"),
        trade.get("trigger_time"),
        trade.get("market_enrichment"),
    ]
    buy = trade.get("buy") if isinstance(trade.get("buy"), dict) else {}
    contexts.append(buy)
    for context in contexts:
        value, key = _first_decimal_deep(context, keys=keys)
        if value is not None:
            return value, key
    return None, None


def _first_decimal_deep(value: object, *, keys: tuple[str, ...]) -> tuple[Decimal | None, str | None]:
    if isinstance(value, dict):
        for key in keys:
            parsed = _decimal_value(value.get(key))
            if parsed is not None:
                return parsed, key
        for nested in value.values():
            parsed, key = _first_decimal_deep(nested, keys=keys)
            if parsed is not None:
                return parsed, key
    if isinstance(value, list):
        for nested in value:
            parsed, key = _first_decimal_deep(nested, keys=keys)
            if parsed is not None:
                return parsed, key
    return None, None


def _nested_decimal(value: dict[str, object], path: tuple[str, ...]) -> Decimal | None:
    current: object = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _decimal_value(current)


def _fee_slippage_bps(closed_trades: list[dict[str, object]]) -> Decimal | None:
    values: list[Decimal] = []
    for trade in closed_trades:
        value, _key = _candidate_context_first_decimal(
            trade,
            keys=(
                "fee_slippage_bps",
                "round_trip_cost_bps",
                "estimated_round_trip_cost_bps",
                "fee_bps",
                "slippage_bps",
            ),
        )
        if value is not None and value >= 0:
            values.append(value)
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _win_rate_pct(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return Decimal(len([value for value in values if value > 0])) / Decimal(len(values)) * Decimal("100")


def _average_decimal(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _max_drawdown_proxy(values: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    max_drawdown = Decimal("0")
    for value in values:
        equity += value
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _candidate_rule_replay_snippet_lines(candidate_rule_replay: dict[str, object]) -> list[str]:
    status = str(candidate_rule_replay.get("status") or "")
    if status in {"blocked", "no_data"}:
        return [
            "candidate-rule replay is "
            f"{status}: {candidate_rule_replay.get('blocked_reason')}; "
            "gross results are diagnostic only and no rule may be promoted."
        ]
    rows = candidate_rule_replay.get("rules")
    if not isinstance(rows, list):
        return []
    lines = []
    for row in rows[:6]:
        if isinstance(row, dict):
            lines.append(
                f"{row.get('rule_id')}: recommendation={row.get('recommendation')}, "
                f"net_delta={row.get('net_pnl_delta_krw')} KRW, "
                f"gross_delta={row.get('gross_pnl_delta_krw')} KRW, "
                f"skipped_winners={row.get('skipped_winners')}, avoided_losers={row.get('avoided_losers')}."
            )
    return lines


def _snippet(
    *,
    summary: dict[str, object],
    lessons: dict[str, object],
    portfolio_risk: dict[str, object],
    candidate_rule_replay: dict[str, object],
    broker_asset_performance: dict[str, object],
    account_drawdown_gate: dict[str, object],
) -> dict[str, object]:
    fifo_summary = _feedback_pnl_summary_text(summary)
    hard_gates = []
    for row in lessons.get("patterns_to_downsize_or_reject", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Downsize/reject target_profile={row.get('name')} until evidence improves; avg_pnl={row.get('avg_pnl_krw')} over {row.get('count')} closed trades."
            )
    for row in lessons.get("worst_losses", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Recent worst loss {row.get('symbol')} {row.get('return_pct')}% "
                f"({row.get('realized_pnl_krw')} KRW; {row.get('realized_pnl_basis')}), "
                f"profile={row.get('target_profile')}, reason={row.get('reason_code')}."
            )
    if broker_asset_performance.get("available"):
        snippet_summary = (
            f"Broker total asset change: {broker_asset_performance.get('asset_change_krw')} KRW "
            f"({broker_asset_performance.get('asset_change_pct')}%), "
            f"baseline={broker_asset_performance.get('baseline_total_asset_krw')} KRW, "
            f"latest_total={broker_asset_performance.get('latest_total_asset_krw')} KRW, "
            f"net_asset={broker_asset_performance.get('latest_net_asset_krw')} KRW. "
            f"FIFO closed-trade PnL: {fifo_summary}."
        )
    else:
        snippet_summary = (
            f"Recent closed trades: pnl={fifo_summary}, "
            f"win_rate={summary.get('win_rate_pct')}%, worst_loss={summary.get('worst_loss_krw')} KRW."
        )
    return {
        "summary": snippet_summary,
        "account_drawdown_gate": account_drawdown_gate_prompt_summary(account_drawdown_gate),
        "patterns_to_downsize_or_reject": hard_gates[:6],
        "positive_expectancy_patterns": lessons.get("positive_expectancy_patterns", []),
        "broker_asset_warnings": _broker_asset_warning_lines(broker_asset_performance),
        "top_portfolio_risks": _portfolio_risk_snippet_lines(portfolio_risk),
        "monday_pre_market_checks": _monday_check_snippet_lines(portfolio_risk),
        "candidate_rule_replay": _candidate_rule_replay_snippet_lines(candidate_rule_replay),
        "required_hard_gates": [
            "Do not increase size for profiles/reason_codes listed as negative without fresh superior live quality.",
            "Prefer delay/reject over execute_now when recent worst-loss conditions resemble the current candidate.",
        ],
    }


def _feedback_pnl_summary_text(summary: dict[str, object]) -> str:
    krw_net = summary.get("realized_pnl_krw")
    prefix = (
        f"{krw_net} KRW (net after transaction costs)"
        if krw_net is not None
        else "unavailable (no complete KRW net aggregate)"
    )
    return (
        f"{prefix}; gross_by_currency={summary.get('gross_realized_pnl_by_currency')}, "
        f"net_by_currency={summary.get('net_realized_pnl_by_currency')}, "
        f"cost_coverage={summary.get('transaction_cost_coverage')}, "
        f"currency_coverage={summary.get('currency_coverage')}"
    )


def _broker_asset_warning_lines(broker_asset_performance: dict[str, object]) -> list[str]:
    if not broker_asset_performance.get("available"):
        return [f"Broker asset performance unavailable: {broker_asset_performance.get('source_failed')}"]
    lines = []
    divergence = broker_asset_performance.get("divergence_warning")
    if isinstance(divergence, dict):
        lines.append(
            f"Divergence warning: broker asset change={divergence.get('asset_change_krw')} KRW, "
            f"FIFO realized={divergence.get('fifo_realized_pnl_krw')} KRW, "
            f"difference={divergence.get('difference_krw')} KRW."
        )
    unrealized = broker_asset_performance.get("broker_unrealized_pnl_krw")
    if unrealized is not None:
        lines.append(f"Broker unrealized PnL summary={unrealized} KRW.")
    holdings = broker_asset_performance.get("holdings")
    if isinstance(holdings, list):
        for row in holdings[:3]:
            if isinstance(row, dict):
                lines.append(
                    f"Holding {row.get('symbol')}: unrealized={row.get('unrealized_pnl_krw')} KRW, "
                    f"market_value={row.get('market_value_krw')} KRW."
                )
    return lines[:6]


def _portfolio_risk_snippet_lines(portfolio_risk: dict[str, object]) -> list[str]:
    if not portfolio_risk.get("available"):
        return [f"Portfolio risk unavailable: {portfolio_risk.get('source_failed')}"]
    aggregation_degraded = portfolio_risk.get("aggregation_status") == "degraded"
    if aggregation_degraded:
        lines = [
            "Currency aggregation degraded: "
            f"{portfolio_risk.get('aggregation_reason')}; "
            f"by_currency={portfolio_risk.get('by_currency')}."
        ]
    else:
        lines = [
            (
                f"Open holdings={portfolio_risk.get('open_holding_count')}, "
                f"market_value={portfolio_risk.get('total_market_value')} "
                f"{portfolio_risk.get('aggregate_currency')}."
            )
        ]
    top_positions = portfolio_risk.get("top_position_concentration")
    if isinstance(top_positions, list):
        for row in top_positions[:3]:
            if isinstance(row, dict):
                weight = (
                    row.get("currency_weight_pct")
                    if aggregation_degraded
                    else row.get("portfolio_weight_pct")
                )
                weight_label = (
                    "currency_weight" if aggregation_degraded else "portfolio_weight"
                )
                lines.append(
                    f"Top concentration {row.get('symbol')}: "
                    f"{row.get('market_value')} {row.get('settlement_currency') or 'UNKNOWN'} "
                    f"({weight_label}={weight}%)."
                )
    stale_orders = portfolio_risk.get("stale_open_order_warnings")
    stale_blockers = portfolio_risk.get("stale_state_blockers")
    stale_positions = portfolio_risk.get("stale_local_position_warnings")
    if isinstance(stale_blockers, list) and stale_blockers:
        lines.append(f"Stale-state blockers need broker-truth refresh: {len(stale_blockers)}.")
    if isinstance(stale_orders, list) and stale_orders:
        lines.append(f"Stale open orders need review: {len(stale_orders)}.")
    if isinstance(stale_positions, list) and stale_positions:
        lines.append(f"Stale local positions need broker-truth refresh: {len(stale_positions)}.")
    return lines[:6]


def _monday_check_snippet_lines(portfolio_risk: dict[str, object]) -> list[str]:
    checks = portfolio_risk.get("monday_pre_market_checks")
    if not isinstance(checks, list) or not checks:
        if portfolio_risk.get("source_failed"):
            return [f"Confirm missing account/position source before Monday: {portfolio_risk.get('source_failed')}"]
        return []
    lines = []
    for row in checks[:8]:
        if isinstance(row, dict):
            lines.append(f"{row.get('type')} {row.get('symbol')}: {row.get('reason')}")
    return lines


def _write_artifacts(
    *,
    report: dict[str, object],
    report_date: date,
    output_dir: Path,
    artifact_scope: FeedbackExecutionScope | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_date.isoformat()}.json"
    md_path = output_dir / f"{report_date.isoformat()}.md"
    payload = _json_safe(redact_account_references(report))
    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    markdown_text = _markdown_report(payload)
    if artifact_scope is not None:
        _atomic_write_text(
            output_dir
            / _scoped_artifact_name(
                prefix=report_date.isoformat(),
                suffix="json",
                scope=artifact_scope,
                runtime_mode=str(report.get("mode") or ""),
            ),
            json_text + "\n",
        )
        _atomic_write_text(
            output_dir
            / _scoped_artifact_name(
                prefix="latest",
                suffix="json",
                scope=artifact_scope,
                runtime_mode=str(report.get("mode") or ""),
            ),
            json_text + "\n",
        )
        _atomic_write_text(
            output_dir
            / _scoped_artifact_name(
                prefix=report_date.isoformat(),
                suffix="md",
                scope=artifact_scope,
                runtime_mode=str(report.get("mode") or ""),
            ),
            markdown_text,
        )
        _atomic_write_text(
            output_dir
            / _scoped_artifact_name(
                prefix="latest",
                suffix="md",
                scope=artifact_scope,
                runtime_mode=str(report.get("mode") or ""),
            ),
            markdown_text,
        )
        return
    _atomic_write_text(json_path, json_text + "\n")
    _atomic_write_text(output_dir / "latest.json", json_text + "\n")
    _atomic_write_text(md_path, markdown_text)
    _atomic_write_text(output_dir / "latest.md", markdown_text)


def _atomic_write_text(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _markdown_report(report: dict[str, object]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    snippet = report.get("prompt_snippet") if isinstance(report.get("prompt_snippet"), dict) else {}
    broker_asset = report.get("broker_asset_performance") if isinstance(report.get("broker_asset_performance"), dict) else {}
    portfolio_risk = report.get("portfolio_risk") if isinstance(report.get("portfolio_risk"), dict) else {}
    account_drawdown_gate = report.get("account_drawdown_gate") if isinstance(report.get("account_drawdown_gate"), dict) else {}
    candidate_rule_replay = report.get("candidate_rule_replay") if isinstance(report.get("candidate_rule_replay"), dict) else {}
    fifo_summary = _feedback_pnl_summary_text(summary)
    verified_fifo_krw = summary.get("realized_pnl_krw")
    fifo_report_line = (
        f"- FIFO realized P/L: {verified_fifo_krw} KRW; economics: {fifo_summary}"
        if verified_fifo_krw is not None
        else f"- FIFO realized P/L: unavailable; economics: {fifo_summary}"
    )
    fifo_report_line_ko = (
        f"- FIFO 실현손익(보조 지표): {verified_fifo_krw} KRW; 경제성: {fifo_summary}"
        if verified_fifo_krw is not None
        else f"- FIFO 실현손익(보조 지표): 사용 불가; 경제성: {fifo_summary}"
    )
    lines = [
        f"# Trade Outcome Feedback {str(report.get('generated_at'))[:10]}",
        "",
        f"- Mode: {report.get('mode')}",
        f"- Closed trades: {report.get('closed_trade_count')}",
        f"- Primary performance metric: {summary.get('primary_performance_metric')}",
        f"- Broker total asset change: {summary.get('broker_total_asset_change_krw')} KRW ({summary.get('broker_total_asset_change_pct')}%)"
        if broker_asset.get("available")
        else f"- Broker total asset change: unavailable ({broker_asset.get('source_failed')})",
        fifo_report_line,
        f"- Win rate: {summary.get('win_rate_pct')}%",
        f"- Worst loss: {summary.get('worst_loss_krw')} KRW",
        "",
        "## 이번주 분석 결과",
        "",
        f"- 브로커 총자산 변화: {summary.get('broker_total_asset_change_krw')} KRW ({summary.get('broker_total_asset_change_pct')}%)"
        if broker_asset.get("available")
        else f"- 브로커 총자산 변화: 사용 불가 ({broker_asset.get('source_failed')})",
        fifo_report_line_ko,
        f"- 승/패: {summary.get('win_count')}승 / {summary.get('loss_count')}패",
        "- 근거: execution_positions.raw_payload.balance.output2, execution_positions.raw_payload.balance.output1, execution_fills, broker_orders",
        "",
        "### Account Drawdown Gate",
        "",
        *[f"- {line}" for line in account_drawdown_gate_report_lines(account_drawdown_gate)],
        "",
        "### Broker Total Asset Performance",
        "",
    ]
    if broker_asset.get("available"):
        lines.extend(
            [
                f"- Baseline total asset: {broker_asset.get('baseline_total_asset_krw')} KRW",
                f"- Latest total asset: {broker_asset.get('latest_total_asset_krw')} KRW",
                f"- Latest net asset: {broker_asset.get('latest_net_asset_krw')} KRW",
                f"- Asset change: {broker_asset.get('asset_change_krw')} KRW ({broker_asset.get('asset_change_pct')}%)",
                f"- Broker unrealized P/L: {broker_asset.get('broker_unrealized_pnl_krw')} KRW",
                f"- Securities value: {broker_asset.get('broker_securities_value_krw')} KRW",
                f"- Cash: {broker_asset.get('broker_cash_krw')} KRW",
                f"- FIFO realized P/L detail: {broker_asset.get('fifo_realized_pnl_krw')} KRW",
            ]
        )
        divergence = broker_asset.get("divergence_warning")
        if isinstance(divergence, dict):
            lines.append(
                f"- Divergence warning: asset change {divergence.get('asset_change_krw')} KRW vs "
                f"FIFO realized {divergence.get('fifo_realized_pnl_krw')} KRW "
                f"(difference {divergence.get('difference_krw')} KRW)"
            )
        holdings = broker_asset.get("holdings")
        if isinstance(holdings, list) and holdings:
            lines.append("- Current holdings from broker output1:")
            for row in holdings[:10]:
                if isinstance(row, dict):
                    lines.append(
                        f"  - {row.get('symbol')}: qty={row.get('quantity')}, "
                        f"value={row.get('market_value_krw')} KRW, unrealized={row.get('unrealized_pnl_krw')} KRW"
                    )
        drift = broker_asset.get("raw_normalized_position_drift")
        if isinstance(drift, list) and drift:
            lines.append("- Broker raw vs normalized position drift:")
            for row in drift[:10]:
                if isinstance(row, dict):
                    lines.append(
                        f"  - {row.get('symbol')}: raw_qty={row.get('raw_quantity')}, "
                        f"normalized_qty={row.get('normalized_quantity')}, "
                        f"raw_avg={row.get('raw_average_price')}, normalized_avg={row.get('normalized_average_price')}, "
                        f"raw_price={row.get('raw_current_price')}, raw_unrealized={row.get('raw_unrealized_pnl_krw')} KRW"
                    )
    else:
        lines.append(f"- 사용 불가: {broker_asset.get('source_failed')}")
    lines.extend(
        [
            "",
            "### 포트폴리오 리스크",
            "",
        ]
    )
    if portfolio_risk.get("available"):
        lines.append(f"- 보유 종목 수: {portfolio_risk.get('open_holding_count')}")
        if portfolio_risk.get("aggregation_status") == "degraded":
            lines.append(
                "- 통화 합산 사용 불가: "
                f"{portfolio_risk.get('aggregation_reason')} "
                f"(통화별={portfolio_risk.get('by_currency')})"
            )
        else:
            lines.append(
                f"- 총 평가 금액: {portfolio_risk.get('total_market_value')} "
                f"{portfolio_risk.get('aggregate_currency')}"
            )
        lines.append(
            f"- 평가 금액 누락 포지션: {portfolio_risk.get('positions_missing_market_value_count')}"
        )
        top_positions = portfolio_risk.get("top_position_concentration")
        if isinstance(top_positions, list) and top_positions:
            lines.append("- 상위 집중:")
            for row in top_positions[:5]:
                if isinstance(row, dict):
                    lines.append(
                        f"  - {row.get('symbol')}: {row.get('market_value')} "
                        f"{row.get('settlement_currency') or 'UNKNOWN'} "
                        f"(portfolio={row.get('portfolio_weight_pct')}%, "
                        f"currency={row.get('currency_weight_pct')}%)"
                    )
    else:
        lines.append(f"- 사용 불가: {portfolio_risk.get('source_failed')}")
    for label, key in (
        ("종목별 실현손익", "realized_pnl_by_symbol"),
        ("매도 사유별 실현손익", "sell_reason_contributions"),
    ):
        rows = portfolio_risk.get(key)
        if isinstance(rows, list) and rows:
            lines.append(f"- {label}:")
            for row in rows:
                if isinstance(row, dict):
                    lines.append(f"  - {row.get('name')}: {row.get('realized_pnl_krw')} KRW ({row.get('trade_count')}건)")
    stale_orders = portfolio_risk.get("stale_open_order_warnings")
    stale_blockers = portfolio_risk.get("stale_state_blockers")
    stale_positions = portfolio_risk.get("stale_local_position_warnings")
    if isinstance(stale_blockers, list) and stale_blockers:
        lines.append(f"- 오래된 주문 상태 차단: {len(stale_blockers)}건")
    if isinstance(stale_orders, list) and stale_orders:
        lines.append(f"- 오래된 미체결 주문 경고: {len(stale_orders)}건")
    if isinstance(stale_positions, list) and stale_positions:
        lines.append(f"- 오래된 로컬 포지션 경고: {len(stale_positions)}건")
    lines.extend(
        [
            "",
            "### Candidate Rule Replay",
            "",
            f"- Baseline closed trades: {candidate_rule_replay.get('baseline_closed_trade_count')}",
            f"- Baseline net P/L: {candidate_rule_replay.get('baseline_net_pnl_krw')} KRW",
            f"- Baseline gross P/L: {candidate_rule_replay.get('baseline_gross_pnl_krw')} KRW",
        ]
    )
    replay_rules = candidate_rule_replay.get("rules")
    if isinstance(replay_rules, list) and replay_rules:
        for row in replay_rules:
            if not isinstance(row, dict):
                continue
            lines.append(
                f"- {row.get('rule_id')} ({row.get('rule_name')}): "
                f"recommendation={row.get('recommendation')}, "
                f"net_delta={row.get('net_pnl_delta_krw')} KRW, "
                f"gross_delta={row.get('gross_pnl_delta_krw')} KRW, "
                f"estimated_net_delta={row.get('estimated_net_pnl_delta_krw')} KRW, "
                f"win_rate_delta={row.get('win_rate_delta_pct')}%, "
                f"avg_return_delta={row.get('average_return_delta_pct')}%, "
                f"drawdown_proxy={row.get('max_drawdown_proxy_krw')} KRW, "
                f"skipped={row.get('skipped_entries')}, skipped_winners={row.get('skipped_winners')}, avoided_losers={row.get('avoided_losers')}"
            )
            avoided = row.get("top_avoided_losers")
            if isinstance(avoided, list) and avoided:
                lines.append("  - Top avoided losers:")
                for example in avoided[:5]:
                    if isinstance(example, dict):
                        lines.append(
                            f"    - {example.get('symbol')}: {example.get('realized_pnl_krw')} KRW ({example.get('return_pct')}%)"
                        )
            skipped = row.get("top_skipped_winners")
            if isinstance(skipped, list) and skipped:
                lines.append("  - Top skipped winners:")
                for example in skipped[:5]:
                    if isinstance(example, dict):
                        lines.append(
                            f"    - {example.get('symbol')}: {example.get('realized_pnl_krw')} KRW ({example.get('return_pct')}%)"
                        )
    else:
        lines.append("- No configured candidate rules produced replay output.")
    lines.extend(
        [
            "",
            "## 개선 사항",
            "",
            "- 최근 손실 패턴은 아래 Gates에 반영됩니다.",
            "",
            "## 검증",
            "",
            "- 생성 JSON에는 portfolio_risk가 별도 machine-readable 객체로 포함됩니다.",
            "",
            "## 월요일 전 우선순위",
            "",
        ]
    )
    monday_checks = portfolio_risk.get("monday_pre_market_checks")
    if isinstance(monday_checks, list) and monday_checks:
        for row in monday_checks[:8]:
            if isinstance(row, dict):
                lines.append(f"- {row.get('symbol')}: {row.get('reason')}")
    elif portfolio_risk.get("source_failed"):
        lines.append(f"- 계좌/포지션 소스 확인: {portfolio_risk.get('source_failed')}")
    else:
        lines.append("- 특이 미체결/포지션 점검 항목 없음")
    lines.extend(
        [
            "",
            "## Prompt Snippet",
            "",
            str(snippet.get("summary") or ""),
            "",
            "## Portfolio Risk Summary",
        ]
    )
    for item in snippet.get("broker_asset_warnings", []):
        lines.append(f"- {item}")
    for item in snippet.get("top_portfolio_risks", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Monday Pre-Market Checks",
        ]
    )
    for item in snippet.get("monday_pre_market_checks", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Candidate Rule Replay",
        ]
    )
    for item in snippet.get("candidate_rule_replay", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Gates",
        ]
    )
    for item in snippet.get("patterns_to_downsize_or_reject", []):
        lines.append(f"- {item}")
    for item in snippet.get("required_hard_gates", []):
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def _negative_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg < 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])))[:max_lessons]


def _positive_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg > 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])), reverse=True)[:max_lessons]


def _lesson_trade(trade: dict[str, object]) -> dict[str, object]:
    decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
    return {
        "symbol": trade.get("symbol"),
        "realized_pnl_krw": trade.get("realized_pnl_krw"),
        "return_pct": trade.get("return_pct"),
        "hold_hours": trade.get("hold_hours"),
        "target_profile": decision.get("target_profile"),
        "reason_code": decision.get("reason_code"),
        "decision_run_id": trade.get("decision_run_id"),
    }


def _exec_agent_decision(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    for source in (item.risk_context, item.quantity_policy):
        if isinstance(source, dict):
            for key in ("exec_agent_decision", "short_term_exec_agent_decision", "last_trigger_time_exec_agent_interaction"):
                value = source.get(key)
                if isinstance(value, dict):
                    return _json_safe(value)
    return {}


def _risk_sizing_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    payload: dict[str, object] = {}
    for source in (item.quantity_policy, item.risk_context):
        if not isinstance(source, dict):
            continue
        for key in (
            "requested_quantity",
            "approved_quantity",
            "stop_price",
            "effective_stop_price",
            "sizing_adjustment",
            "max_safe_notional_krw",
            "budget_krw",
        ):
            if key in source and key not in payload:
                payload[key] = source[key]
    return _json_safe(payload)


def _trigger_time_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    interaction = item.risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return {}
    decision = interaction.get("decision") if isinstance(interaction.get("decision"), dict) else {}
    request = interaction.get("request_payload") if isinstance(interaction.get("request_payload"), dict) else {}
    market = request.get("market_context") if isinstance(request.get("market_context"), dict) else {}
    return _json_safe(
        {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "risk_notes": decision.get("risk_notes"),
            "context_staleness_ok": decision.get("context_staleness_ok"),
            "market_quality_gate": market.get("live_execution_quality_gate"),
        }
    )


def _market_enrichment_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    payload: dict[str, object] = {}
    for key in (
        "liquidity_tier",
        "trading_value",
        "short_ratio",
        "investor_flow",
        "market_breadth",
        "data_quality_flags",
        "data_quality_warnings",
    ):
        if key in item.risk_context:
            payload[key] = item.risk_context[key]
    return _json_safe(payload)


def _candidate_rule_context(*, item: ExecutionPlanItem | None, buy_fill: ExecutionFill) -> dict[str, object]:
    if item is None:
        return {}
    payload: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "trigger_price": _decimal_text(Decimal(item.trigger_price)) if item.trigger_price is not None else None,
        "limit_price": _decimal_text(Decimal(item.limit_price)) if item.limit_price is not None else None,
        "buy_fill_price": _decimal_text(Decimal(buy_fill.price)),
        "intent": item.intent,
    }
    for source in (item.quantity_policy, item.risk_context):
        if not isinstance(source, dict):
            continue
        for key in (
            "trigger_price",
            "watch_trigger_price",
            "buy_trigger_price",
            "entry_trigger_price",
            "confidence",
            "confidence_score",
            "confidence_score_10",
            "probability",
            "score",
            "atr_pct",
            "atr_pct_14",
            "atr14_pct",
            "atr_percent",
            "ret5",
            "ret_5d",
            "return_5d",
            "pct_change_5d",
            "volatility_pct",
            "realized_volatility_pct",
            "intraday_range_pct",
            "daily_range_pct",
            "trigger_direction",
            "direction",
            "trigger_reason",
            "portfolio_correlation",
            "max_correlation",
            "correlation",
            "sector_correlation",
            "market_correlation",
            "fee_slippage_bps",
            "round_trip_cost_bps",
            "estimated_round_trip_cost_bps",
            "fee_bps",
            "slippage_bps",
        ):
            if key in source and key not in payload:
                payload[key] = source[key]
        for nested_key in (
            "exec_agent_decision",
            "short_term_exec_agent_decision",
            "market_data",
            "quote",
            "decider_output",
            "short_term_entry_quality",
        ):
            nested = source.get(nested_key)
            if isinstance(nested, dict):
                for key in (
                    "confidence",
                    "confidence_score",
                    "confidence_score_10",
                    "atr_pct",
                    "atr_pct_14",
                    "atr14_pct",
                    "atr_percent",
                    "ret5",
                    "ret_5d",
                    "return_5d",
                    "pct_change_5d",
                    "volatility_pct",
                    "portfolio_correlation",
                    "max_correlation",
                    "correlation",
                    "fee_slippage_bps",
                    "round_trip_cost_bps",
                    "estimated_round_trip_cost_bps",
                ):
                    if key in nested and key not in payload:
                        payload[key] = nested[key]
        for nested_key in ("entry_context", "last_monitoring_event", "post_entry_review", "last_high_volatility_chase_decision"):
            nested = source.get(nested_key)
            if isinstance(nested, dict) and nested_key not in payload:
                payload[nested_key] = nested
        for key in ("post_entry_managed", "deterministic_emergency_stop"):
            if key in source and key not in payload:
                payload[key] = source[key]
    return _json_safe({key: value for key, value in payload.items() if value is not None})


def _merged_candidate_rule_context(
    *,
    buy_item: ExecutionPlanItem | None,
    sell_item: ExecutionPlanItem | None,
    buy_fill: ExecutionFill,
) -> dict[str, object]:
    payload = _candidate_rule_context(item=buy_item or sell_item, buy_fill=buy_fill)
    if sell_item is not None and sell_item is not buy_item:
        sell_payload = _candidate_rule_context(item=sell_item, buy_fill=buy_fill)
        for key, value in sell_payload.items():
            if key not in payload or payload[key] in (None, "", [], {}):
                payload[key] = value
        if sell_payload:
            payload["sell_candidate_rule_context"] = sell_payload
    return _json_safe(payload)


def _post_entry_review_reasons(value: object) -> list[str]:
    reasons: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "reasons" and isinstance(item, list):
                reasons.extend(str(reason) for reason in item if str(reason))
            else:
                reasons.extend(_post_entry_review_reasons(item))
    elif isinstance(value, list):
        for item in value:
            reasons.extend(_post_entry_review_reasons(item))
    return reasons


def _outcome_bucket(pnl: Decimal) -> str:
    if pnl < 0:
        return "loss"
    if pnl > 0:
        return "win"
    return "flat"


def _decimal_value(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _quantity_text(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP))


def _quantity_value(value: Decimal) -> int | str:
    if value == value.to_integral_value():
        return int(value)
    return _quantity_text(value)


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime:
    return _normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _json_safe(value: object) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _normalize_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate exec-agent trade outcome feedback artifacts.")
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    settings = load_settings()
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        report = generate_trade_outcome_feedback(
            settings=settings,
            db=db,
            lookback_days=args.lookback_days,
            output_dir=args.output_dir,
        )
    print(json.dumps(_json_safe({"status": "ok", "closed_trade_count": report["closed_trade_count"], "output_dir": str(args.output_dir or settings.trade_outcome_feedback_dir)})))


if __name__ == "__main__":
    main()
