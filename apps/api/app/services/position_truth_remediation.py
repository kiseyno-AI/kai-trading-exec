from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot
from app.services.runtime import position_truth_freshness_payload


POSITION_CANDIDATE_TYPE = "stale_position_truth"
SUPPORTED_POSITION_EXCHANGES = {"KRX", "NASD", "NYSE", "AMEX"}


def analyze_position_truth_remediation(
    *,
    settings: Settings,
    db: Session,
    now: datetime | None = None,
) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    freshness = position_truth_freshness_payload(settings=settings, db=db, now=observed_at)
    stale_symbols = [str(symbol) for symbol in freshness.get("stale_symbols") or []]
    positions = {
        position.symbol: position
        for position in db.scalars(
            select(ExecutionPosition).where(
                ExecutionPosition.mode == settings.runtime_mode,
                ExecutionPosition.symbol.in_(stale_symbols or [""]),
            )
        ).all()
    }
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

    sync_results: dict[str, dict[str, int]] = {}
    try:
        for exchange, symbols in sorted(symbols_by_exchange.items()):
            sync_results[exchange] = sync_execution_positions_from_broker_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                symbols=symbols,
                exchange=exchange,
                commit=False,
            )
    except Exception:
        db.rollback()
        raise

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


def _candidate_key(item: dict[str, object]) -> str:
    return f"{item['symbol']}:{item.get('last_reconciled_at') or 'missing'}"


def _missing_position_order_exchanges(
    *,
    settings: Settings,
    db: Session,
    symbols: set[str],
) -> dict[str, str]:
    if not symbols:
        return {}
    rows = db.execute(
        select(BrokerOrder.symbol, BrokerOrderEvent.request_payload)
        .join(BrokerOrderEvent, BrokerOrderEvent.broker_order_pk == BrokerOrder.id)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.broker_name == "kis",
            BrokerOrder.asset_class == "us_equity",
            BrokerOrder.venue == "kis",
            BrokerOrder.side == "buy",
            BrokerOrder.status == "filled",
            BrokerOrder.symbol.in_(sorted(symbols)),
            BrokerOrderEvent.event_type == "submit_requested",
        )
    ).all()
    exchanges_by_symbol: dict[str, set[str]] = {}
    for symbol, request_payload in rows:
        if not isinstance(request_payload, dict):
            continue
        exchange = str(request_payload.get("exchange") or "").strip().upper()
        if exchange not in SUPPORTED_POSITION_EXCHANGES - {"KRX"}:
            continue
        exchanges_by_symbol.setdefault(str(symbol), set()).add(exchange)
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
    if position is not None and isinstance(position.raw_payload, dict):
        sync_context = position.raw_payload.get("_position_sync")
        if isinstance(sync_context, dict):
            exchange = str(sync_context.get("exchange") or "").strip().upper()
            if exchange:
                return exchange
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
