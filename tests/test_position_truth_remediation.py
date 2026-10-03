from __future__ import annotations

import fcntl
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Event, Thread
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerReconnectHandle,
    BrokerTokenGrant,
)
from app.broker.error_safety import safe_broker_error_message
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    BrokerSession,
    BrokerSessionEvent,
    ExecutionFill,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services import broker_orders as broker_orders_service
from app.services import order_intent_serialization as order_intent_serialization_service
from app.services import position_truth_remediation as remediation_service
from app.services.account_drawdown_recovery import recovery_account_fingerprint
from app.services.position_truth_remediation import (
    analyze_position_truth_remediation,
    apply_position_truth_remediation,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
    execution_scope_payload,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot


NOW = datetime(2026, 9, 6, 18, 0, tzinfo=UTC)


class PositionInquiryAdapter:
    provider = "kis"

    def __init__(
        self,
        *,
        fail_exchange: str | None = None,
        reject_authentication: bool = False,
    ) -> None:
        self.exchanges: list[str] = []
        self.fail_exchange = fail_exchange
        self.reject_authentication = reject_authentication
        self.authenticate_calls = 0

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url="https://broker.test",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="test")

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        if self.reject_authentication:
            raise BrokerAdapterError("authentication rejected", status_code=401)
        return BrokerTokenGrant(
            access_token="renewed-token",
            token_type="Bearer",
            access_token_expires_at=NOW + timedelta(days=1),
        )

    def get_account_snapshot(self, access_token: str, *, symbols=None, exchange="KRX"):
        self.exchanges.append(exchange)
        if exchange == self.fail_exchange:
            raise RuntimeError(f"{exchange} inquiry failed")
        holdings = []
        if exchange == "KRX":
            holdings = [
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                    payload={},
                )
            ]
        elif symbols:
            holdings = [
                BrokerAccountHolding(
                    symbol=symbol,
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": exchange},
                )
                for symbol in symbols
                if {"AAPL": "NASD", "IBM": "NYSE", "SPY": "AMEX"}.get(symbol) == exchange
            ]
        return BrokerAccountSnapshot(
            observed_at=NOW,
            source="stub:account_snapshot",
            available_cash=Decimal("1"),
            buying_power=Decimal("1"),
            holdings=holdings,
            positions=holdings,
            open_orders=[],
            payload={"rt_cd": "0"},
        )


class LegacyAttributionAdapter(PositionInquiryAdapter):
    def __init__(self, snapshot: BrokerAccountSnapshot) -> None:
        super().__init__()
        self.snapshot = snapshot
        self.snapshot_requests: list[tuple[list[str] | None, str]] = []

    def get_account_snapshot(self, access_token: str, *, symbols=None, exchange="KRX"):
        self.snapshot_requests.append((symbols, exchange))
        return self.snapshot


class LegacyQuarantineAttributionAdapter(LegacyAttributionAdapter):
    def get_account_snapshot(self, access_token: str, *, symbols=None, exchange="KRX"):
        raise AssertionError("quarantine attribution must not open account trading reads")

    def get_legacy_us_holdings_snapshot(self, access_token: str):
        self.snapshot_requests.append((None, "US_ALL_HOLDINGS_ONLY"))
        return self.snapshot


def _session():
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return settings, engine, session_factory, session_factory()


def _scoped_position(
    settings: Settings,
    *,
    exchange: str,
    **values: object,
) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange=exchange,
    )
    return ExecutionPosition(
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        **values,
    )


def _legacy_position(*, symbol: str, quantity: int, average_price: str) -> ExecutionPosition:
    return ExecutionPosition(
        mode="paper",
        symbol=symbol,
        quantity=quantity,
        average_price=Decimal(average_price),
        updated_from="reconciliation",
        last_reconciled_at=NOW - timedelta(minutes=10),
        raw_payload={"legacy_snapshot": True},
    )


def _legacy_fractional_position(
    *,
    symbol: str,
    base_quantity: str,
    average_price: str,
) -> ExecutionPosition:
    return ExecutionPosition(
        mode="paper",
        symbol=symbol,
        quantity=0,
        base_quantity=Decimal(base_quantity),
        average_price=Decimal(average_price),
        updated_from="reconciliation",
        last_reconciled_at=NOW - timedelta(minutes=10),
        raw_payload={
            "provider": "alpaca",
            "mode": "paper",
            "asset_class": "us_equity",
            "legacy_snapshot": True,
        },
    )


def _alpaca_paper_us_snapshot(
    *,
    account_id: str | None = "alpaca-paper-account-id",
    base_quantity: str = "0.125",
    holding_payload: dict[str, object] | None = None,
) -> BrokerAccountSnapshot:
    return BrokerAccountSnapshot(
        broker_account_id=account_id,
        observed_at=NOW,
        source="alpaca:paper:us_equity",
        holdings=[
            BrokerAccountHolding(
                symbol="QQQ",
                quantity=0,
                average_price=Decimal("481.25"),
                payload=(
                    holding_payload
                    if holding_payload is not None
                    else {
                        "provider": "alpaca",
                        "asset_class": "us_equity",
                        "base_quantity": base_quantity,
                    }
                ),
            )
        ],
        payload={
            "provider": "alpaca",
            "mode": "paper",
            "asset_class": "us_equity",
            "holdings_coverage": "complete",
        },
    )


def _add_filled_us_buy_order_event(
    db,
    *,
    settings: Settings,
    symbol: str,
    exchange: str,
    suffix: str,
    side: str = "buy",
    status: str = "filled",
    account_scope: str | None = None,
) -> BrokerOrder:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange=exchange,
    )
    execution_scope = execution_scope_payload(
        identity=identity,
        venue="KIS",
        asset_class="us_equity",
    )
    if account_scope is not None:
        execution_scope["account_scope"] = account_scope
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"{suffix}-{symbol.lower()}-position",
        broker_order_id=f"{suffix}-{symbol.lower()}-{side}",
        symbol=symbol,
        side=side,
        status=status,
        quantity=2,
        venue="kis",
        asset_class="us_equity",
        request_metadata={
            "exchange": exchange,
            "execution_scope": execution_scope,
        },
        submitted_at=NOW - timedelta(minutes=10),
    )
    db.add(order)
    db.flush()
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submit_requested",
            event_source="submission",
            request_payload={
                "provider": "kis",
                "venue": "kis",
                "asset_class": "us_equity",
                "symbol": symbol,
                "side": side,
                "exchange": exchange,
            },
            observed_at=NOW - timedelta(minutes=10),
        )
    )
    return order


def test_position_truth_remediation_routes_supported_and_blocks_unknown(monkeypatch) -> None:
    settings, engine, session_factory, db = _session()
    old = NOW - timedelta(days=1)
    db.add_all(
        [
            _scoped_position(
                settings,
                exchange="KRX",
                mode="paper",
                symbol="003550",
                quantity=10,
                average_price=Decimal("250000"),
                updated_from="reconciliation",
                last_reconciled_at=old,
            ),
            _scoped_position(
                settings,
                exchange="NASD",
                mode="paper",
                symbol="APTX",
                quantity=3,
                average_price=Decimal("1"),
                updated_from="reconciliation",
                last_reconciled_at=old,
                raw_payload={"_position_sync": {"exchange": "NASD"}},
            ),
            _scoped_position(
                settings,
                exchange="OTCB",
                mode="paper",
                symbol="AFIB",
                quantity=4,
                average_price=Decimal("1"),
                updated_from="reconciliation",
                last_reconciled_at=old,
                raw_payload={"_position_sync": {"exchange": "OTCB"}},
            ),
        ]
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="issue #265: refresh position truth through account inquiry only",
        now=NOW,
    )

    assert dry_run["summary"] == {
        "candidate_count": 3,
        "eligible_refresh_count": 2,
        "manual_review_count": 1,
    }
    assert adapter.exchanges == ["KRX", "NASD"]
    assert applied["summary"]["refreshed_count"] == 2
    assert applied["summary"]["manual_review_count"] == 1
    afib = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "AFIB"))
    assert afib is not None
    assert afib.last_reconciled_at == old.replace(tzinfo=None)
    events = db.scalars(select(RuntimeRemediationEvent)).all()
    assert sorted(event.outcome for event in events) == [
        "manual_review_required",
        "position_truth_refreshed",
        "position_truth_refreshed",
    ]
    assert all(event.actor == "Peter" for event in events)
    assert all(event.evidence_summary["orders_submitted"] == 0 for event in events)
    events_by_symbol = {event.evidence_summary["symbol"]: event for event in events}
    updated_evidence = events_by_symbol["003550"].evidence_summary
    assert updated_evidence["broker_snapshot_observed_at"] == NOW.isoformat()
    assert updated_evidence["position_refresh_outcome"] == "updated"
    assert updated_evidence["resulting_quantity"] == 10
    assert updated_evidence["resulting_base_quantity"] is None
    assert updated_evidence["resulting_average_price"] == "250000.0000"
    zeroed_evidence = events_by_symbol["APTX"].evidence_summary
    assert zeroed_evidence["broker_snapshot_observed_at"] == NOW.isoformat()
    assert zeroed_evidence["position_refresh_outcome"] == "zeroed"
    assert zeroed_evidence["resulting_quantity"] == 0
    assert zeroed_evidence["resulting_base_quantity"] is None
    assert zeroed_evidence["resulting_average_price"] == "1.0000"
    engine.dispose()


@pytest.mark.parametrize(
    ("symbol", "exchange"),
    [("AAPL", "NASD"), ("IBM", "NYSE"), ("SPY", "AMEX")],
)
def test_missing_us_position_recovers_exchange_from_filled_order_event(
    monkeypatch,
    symbol: str,
    exchange: str,
) -> None:
    settings, engine, session_factory, db = _session()
    order = _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol=symbol,
        exchange=exchange,
        suffix="missing",
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="refresh missing U.S. position through account inquiry only",
        now=NOW,
    )

    assert dry_run["items"] == [
        {
            "symbol": symbol,
            "position_id": None,
            "last_reconciled_at": None,
            "exchange": exchange,
            "eligible": True,
            "repair_action": "refresh_from_broker_account_inquiry",
            "reason": "supported_read_only_account_inquiry_route",
        }
    ]
    assert adapter.exchanges == [exchange]
    assert applied["summary"]["refreshed_count"] == 1
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == symbol))
    assert position is not None
    assert position.raw_payload["_position_sync"]["exchange"] == exchange
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    assert event.evidence_summary["broker_snapshot_observed_at"] == NOW.isoformat()
    assert event.evidence_summary["position_refresh_outcome"] == "created"
    assert event.evidence_summary["resulting_position_id"] == position.id
    assert event.evidence_summary["resulting_quantity"] == 2
    assert event.evidence_summary["resulting_base_quantity"] is None
    assert event.evidence_summary["resulting_average_price"] == "200.0000"
    assert db.scalars(select(ExecutionFill)).all() == []
    assert db.scalars(select(BrokerOrder)).all() == [order]
    engine.dispose()


def test_missing_position_ignores_foreign_account_order_route() -> None:
    settings, engine, _session_factory, db = _session()
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NASD",
        suffix="foreign-account",
        account_scope="f" * 24,
    )
    db.commit()

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)

    assert dry_run["summary"] == {
        "candidate_count": 0,
        "eligible_refresh_count": 0,
        "manual_review_count": 0,
    }
    assert dry_run["items"] == []
    engine.dispose()


@pytest.mark.parametrize(
    ("symbol", "exchange", "status"),
    [
        ("AAPL", "NASD", "submitted"),
        ("IBM", "NYSE", "partially_filled"),
        ("SPY", "AMEX", "open"),
    ],
)
def test_missing_us_position_recovers_exchange_from_active_order_event(
    monkeypatch,
    symbol: str,
    exchange: str,
    status: str,
) -> None:
    settings, engine, session_factory, db = _session()
    order = _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol=symbol,
        exchange=exchange,
        suffix=f"active-{status}",
        status=status,
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="recover active U.S. order exchange through account inquiry only",
        now=NOW,
    )

    assert dry_run["items"][0]["symbol"] == symbol
    assert dry_run["items"][0]["exchange"] == exchange
    assert dry_run["items"][0]["eligible"] is True
    assert adapter.exchanges == [exchange]
    assert applied["summary"]["refreshed_count"] == 1
    assert db.scalars(select(BrokerOrder)).all() == [order]
    assert db.scalars(select(ExecutionFill)).all() == []
    engine.dispose()


@pytest.mark.parametrize(
    ("symbol", "exchange", "status"),
    [
        ("AAPL", "NASD", "submitted"),
        ("IBM", "NYSE", "partially_filled"),
        ("SPY", "AMEX", "open"),
    ],
)
def test_missing_us_position_recovers_exchange_from_sell_order_event(
    monkeypatch,
    symbol: str,
    exchange: str,
    status: str,
) -> None:
    settings, engine, session_factory, db = _session()
    order = _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol=symbol,
        exchange=exchange,
        suffix=f"active-sell-{status}",
        side="sell",
        status=status,
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="recover U.S. sell order exchange through account inquiry only",
        now=NOW,
    )

    assert dry_run["items"][0]["symbol"] == symbol
    assert dry_run["items"][0]["exchange"] == exchange
    assert dry_run["items"][0]["eligible"] is True
    assert adapter.exchanges == [exchange]
    assert applied["summary"]["refreshed_count"] == 1
    assert db.scalars(select(BrokerOrder)).all() == [order]
    assert db.scalars(select(ExecutionFill)).all() == []
    engine.dispose()


def test_stale_us_position_recovers_exchange_from_filled_sell_order_event(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    old = NOW - timedelta(days=1)
    db.add(
        _scoped_position(
            settings,
            exchange="NASD",
            mode="paper",
            symbol="AAPL",
            quantity=1,
            average_price=Decimal("200"),
            updated_from="reconciliation",
            last_reconciled_at=old,
            raw_payload={"rt_cd": "0"},
        )
    )
    order = _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NASD",
        suffix="filled-sell",
        side="sell",
        status="filled",
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="recover stale U.S. position exchange from filled sell evidence",
        now=NOW,
    )

    assert dry_run["items"][0]["symbol"] == "AAPL"
    assert dry_run["items"][0]["exchange"] == "NASD"
    assert dry_run["items"][0]["eligible"] is True
    assert adapter.exchanges == ["NASD"]
    assert applied["summary"]["refreshed_count"] == 1
    assert db.scalars(select(BrokerOrder)).all() == [order]
    assert db.scalars(select(ExecutionFill)).all() == []
    engine.dispose()


def test_missing_us_position_with_ambiguous_buy_and_sell_exchanges_fails_closed() -> None:
    settings, engine, session_factory, db = _session()
    old = NOW - timedelta(days=1)
    db.add(
        _scoped_position(
            settings,
            exchange="UNKNOWN",
            mode="paper",
            symbol="AAPL",
            quantity=1,
            average_price=Decimal("200"),
            updated_from="reconciliation",
            last_reconciled_at=old,
            raw_payload={"rt_cd": "0"},
        )
    )
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NASD",
        suffix="ambiguous-buy",
        side="buy",
        status="filled",
    )
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NYSE",
        suffix="ambiguous-sell",
        side="sell",
        status="filled",
    )
    db.commit()
    adapter = PositionInquiryAdapter()

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="ambiguous buy and sell exchange evidence requires manual review",
        now=NOW,
    )

    assert dry_run["items"][0]["exchange"] == "UNKNOWN"
    assert dry_run["items"][0]["eligible"] is False
    assert adapter.exchanges == []
    assert applied["summary"]["manual_review_count"] == 1
    review = db.scalar(select(RuntimeRemediationEvent))
    assert review is not None
    assert review.outcome == "manual_review_required"
    engine.dispose()


def test_existing_us_position_without_exchange_recovers_unique_filled_order_exchange(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add(
        _scoped_position(
            settings,
            exchange="NASD",
            mode="paper",
            symbol="AAPL",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=NOW - timedelta(days=1),
            raw_payload={"rt_cd": "0"},
        )
    )
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NASD",
        suffix="existing",
    )
    db.commit()
    adapter = PositionInquiryAdapter()
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="recover existing U.S. position route from unique order evidence",
        now=NOW,
    )

    assert dry_run["items"][0]["exchange"] == "NASD"
    assert dry_run["items"][0]["eligible"] is True
    assert adapter.exchanges == ["NASD"]
    assert applied["summary"]["refreshed_count"] == 1
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "AAPL"))
    assert position is not None
    assert position.quantity == 2
    assert position.raw_payload["_position_sync"]["exchange"] == "NASD"
    engine.dispose()


def test_existing_us_position_with_ambiguous_order_exchanges_fails_closed() -> None:
    settings, engine, session_factory, db = _session()
    old = NOW - timedelta(days=1)
    db.add(
        _scoped_position(
            settings,
            exchange="UNKNOWN",
            mode="paper",
            symbol="AAPL",
            quantity=0,
            average_price=None,
            updated_from="reconciliation",
            last_reconciled_at=old,
            raw_payload={"rt_cd": "0"},
        )
    )
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NASD",
        suffix="ambiguous-nasd",
    )
    _add_filled_us_buy_order_event(
        db,
        settings=settings,
        symbol="AAPL",
        exchange="NYSE",
        suffix="ambiguous-nyse",
    )
    db.commit()
    adapter = PositionInquiryAdapter()

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        actor="Peter",
        reason="ambiguous exchange evidence requires manual review",
        now=NOW,
    )

    assert dry_run["items"][0]["exchange"] == "UNKNOWN"
    assert dry_run["items"][0]["eligible"] is False
    assert adapter.exchanges == []
    assert applied["summary"]["manual_review_count"] == 1
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "AAPL"))
    assert position is not None
    assert position.last_reconciled_at == old.replace(tzinfo=None)
    review = db.scalar(select(RuntimeRemediationEvent))
    assert review is not None
    assert review.outcome == "manual_review_required"
    engine.dispose()


def test_position_truth_remediation_rolls_back_prior_exchange_when_later_inquiry_fails(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    old = NOW - timedelta(days=1)
    db.add_all(
        [
            _scoped_position(
                settings,
                exchange="KRX",
                mode="paper",
                symbol="003550",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="test",
                last_reconciled_at=old,
            ),
            _scoped_position(
                settings,
                exchange="NASD",
                mode="paper",
                symbol="APTX",
                quantity=3,
                average_price=Decimal("1"),
                updated_from="test",
                last_reconciled_at=old,
                raw_payload={"_position_sync": {"exchange": "NASD"}},
            ),
        ]
    )
    db.commit()
    adapter = PositionInquiryAdapter(fail_exchange="NASD")
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(RuntimeError, match="NASD inquiry failed"):
        apply_position_truth_remediation(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            actor="Peter",
            reason="issue #265: refresh position truth through account inquiry only",
            now=NOW,
        )

    db.expire_all()
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "003550"))
    events = db.scalars(select(RuntimeRemediationEvent)).all()
    assert adapter.exchanges == ["KRX", "NASD"]
    assert position is not None
    assert position.quantity == 5
    assert position.average_price == Decimal("200000.0000")
    assert position.last_reconciled_at == old.replace(tzinfo=None)
    assert events == []
    engine.dispose()


def test_position_truth_remediation_preserves_issued_auth_when_later_inquiry_fails(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'issued-auth.db'}",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    db = session_factory()
    old = NOW - timedelta(days=1)
    db.add_all(
        [
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                api_base_url="https://broker.test",
                access_token="original-token",
                token_type="Bearer",
                access_token_expires_at=NOW - timedelta(hours=1),
            ),
            _scoped_position(
                settings,
                exchange="KRX",
                mode="paper",
                symbol="003550",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="test",
                last_reconciled_at=old,
            ),
            _scoped_position(
                settings,
                exchange="NASD",
                mode="paper",
                symbol="APTX",
                quantity=3,
                average_price=Decimal("1"),
                updated_from="test",
                last_reconciled_at=old,
                raw_payload={"_position_sync": {"exchange": "NASD"}},
            ),
        ]
    )
    db.commit()
    adapter = PositionInquiryAdapter(fail_exchange="NASD")

    with pytest.raises(RuntimeError, match="NASD inquiry failed"):
        apply_position_truth_remediation(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            actor="Peter",
            reason="position changes are atomic but authentication is durable",
            now=NOW,
        )

    db.expire_all()
    position = db.scalar(select(ExecutionPosition).where(ExecutionPosition.symbol == "003550"))
    assert adapter.exchanges == ["KRX", "NASD"]
    assert adapter.authenticate_calls == 1
    assert position is not None
    assert position.quantity == 5
    assert position.average_price == Decimal("200000.0000")
    assert position.last_reconciled_at == old.replace(tzinfo=None)
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    with session_factory() as verification_db:
        broker_session = verification_db.get(BrokerSession, "kis:paper")
        auth_events = verification_db.scalars(select(BrokerSessionEvent)).all()
        assert broker_session is not None
        assert broker_session.access_token == "renewed-token"
        assert len(auth_events) == 1
        assert auth_events[0].action == "authenticate"
        assert auth_events[0].outcome == "applied"
        assert auth_events[0].source == "position_truth_remediation"
    db.close()
    engine.dispose()


def test_position_truth_remediation_preserves_rejected_auth_audit(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'rejected-auth.db'}",
        runtime_mode="paper",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    old = NOW - timedelta(days=1)
    with session_factory() as db:
        db.add(
            _scoped_position(
                settings,
                exchange="KRX",
                mode="paper",
                symbol="003550",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="test",
                last_reconciled_at=old,
            )
        )
        db.commit()
        adapter = PositionInquiryAdapter(reject_authentication=True)

        with pytest.raises(BrokerAdapterError, match="authentication rejected"):
            apply_position_truth_remediation(
                settings=settings,
                db=db,
                auth_session_factory=session_factory,
                adapter=adapter,
                actor="Peter",
                reason="record rejected authentication independently",
                now=NOW,
            )

        db.expire_all()
        position = db.scalar(
            select(ExecutionPosition).where(ExecutionPosition.symbol == "003550")
        )
        assert position is not None
        assert position.quantity == 5
        assert position.last_reconciled_at == old.replace(tzinfo=None)
        assert db.scalars(select(RuntimeRemediationEvent)).all() == []
        assert adapter.exchanges == []
        assert adapter.authenticate_calls == 1

    with session_factory() as verification_db:
        broker_session = verification_db.get(BrokerSession, "kis:paper")
        auth_events = verification_db.scalars(select(BrokerSessionEvent)).all()
        assert broker_session is not None
        assert broker_session.access_token is None
        assert broker_session.last_error == safe_broker_error_message(
            "authentication rejected"
        )
        assert "authentication rejected" not in broker_session.last_error
        assert len(auth_events) == 1
        assert auth_events[0].action == "authenticate"
        assert auth_events[0].outcome == "rejected"
        assert auth_events[0].source == "position_truth_remediation"
    engine.dispose()


def test_live_position_remediation_requires_explicit_database_url_source(tmp_path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/refresh_execution_position_truth.py",
            "--runtime-mode",
            "live",
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "KAI_EXEC_DATABASE_URL": "",
        },
    )

    assert result.returncode != 0
    assert "requires KAI_EXEC_DATABASE_URL or --database-url" in result.stderr
    assert not (tmp_path / "must-not-open.db").exists()


@pytest.mark.parametrize(
    ("actor", "reason"),
    [("   ", "valid reason"), ("Peter", "\t  ")],
)
def test_position_remediation_rejects_blank_audit_metadata_before_opening_database(
    tmp_path,
    actor: str,
    reason: str,
) -> None:
    database_path = tmp_path / "must-not-open.db"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/refresh_execution_position_truth.py",
            "--database-url",
            f"sqlite:///{database_path}",
            "--runtime-mode",
            "paper",
            "--apply",
            "--actor",
            actor,
            "--reason",
            reason,
        ],
        capture_output=True,
        text=True,
        env=os.environ,
    )

    assert result.returncode != 0
    assert "--apply requires --actor and --reason" in result.stderr
    assert not database_path.exists()


def test_position_remediation_trims_applied_audit_metadata(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'position-remediation.db'}"
    settings = Settings(database_url=database_url, runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        db.add(
            _scoped_position(
                settings,
                exchange="OTCB",
                mode="paper",
                symbol="AFIB",
                quantity=4,
                average_price=Decimal("1"),
                updated_from="test",
                last_reconciled_at=NOW - timedelta(days=1),
                raw_payload={"_position_sync": {"exchange": "OTCB"}},
            )
        )
        db.commit()
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/refresh_execution_position_truth.py",
            "--database-url",
            database_url,
            "--runtime-mode",
            "paper",
            "--apply",
            "--actor",
            "  Peter  ",
            "--reason",
            "  inspect unsupported route  ",
        ],
        capture_output=True,
        text=True,
        env=os.environ,
    )

    assert result.returncode == 0, result.stderr
    engine = create_db_engine(settings)
    with create_session_factory(engine)() as db:
        event = db.scalar(select(RuntimeRemediationEvent))
        assert event is not None
        assert event.actor == "Peter"
        assert event.reason == "inspect unsupported route"
    engine.dispose()


def test_position_remediation_cli_routes_us_legacy_dry_run_to_alpaca_paper(
    tmp_path,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'alpaca-paper-attribution.db'}"
    settings = Settings(database_url=database_url, runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    engine.dispose()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/refresh_execution_position_truth.py",
            "--database-url",
            database_url,
            "--runtime-mode",
            "paper",
            "--legacy-attribution-exchange",
            "US",
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "KAI_EXEC_RUNTIME_BROKER_PROVIDER": "alpaca",
            "KAI_EXEC_ALPACA_API_KEY": "paper-key",
            "KAI_EXEC_ALPACA_API_SECRET": "paper-secret",
        },
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["runtime_mode"] == "paper"
    assert payload["exchange"] == "US"
    assert payload["summary"] == {
        "candidate_count": 0,
        "eligible_count": 0,
        "manual_review_count": 0,
    }


def test_legacy_position_attribution_dry_run_proves_exact_matches_without_mutation(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={"account_drawdown_recovery_scope_secret": "s" * 32}
    )
    exact = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    another_exact = _legacy_position(
        symbol="005930",
        quantity=2,
        average_price="80000",
    )
    mismatch = _legacy_position(
        symbol="000660",
        quantity=5,
        average_price="120000",
    )
    db.add_all([exact, another_exact, mismatch])
    db.commit()
    snapshot = BrokerAccountSnapshot(
        observed_at=NOW,
        source="kis:account_snapshot",
        holdings=[
            BrokerAccountHolding(
                symbol="003550",
                quantity=10,
                average_price=Decimal("250000"),
            ),
            BrokerAccountHolding(
                symbol="005930",
                quantity=2,
                average_price=Decimal("80000"),
            ),
            BrokerAccountHolding(
                symbol="000660",
                quantity=6,
                average_price=Decimal("120000"),
            ),
        ],
        payload={"exchange": "KRX"},
    )
    adapter = LegacyAttributionAdapter(snapshot)
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )

    assert result["mode"] == "dry-run"
    assert result["summary"] == {
        "candidate_count": 3,
        "eligible_count": 2,
        "manual_review_count": 1,
    }
    by_symbol = {item["symbol"]: item for item in result["items"]}
    assert by_symbol["003550"]["reason"] == "exact_broker_holding_match"
    exact_item = by_symbol["003550"]
    existing_fingerprint_material = {
        "safety_policy_version": exact_item["safety_policy_version"],
        "safety_invariants": exact_item["safety_invariants"],
        "snapshot_proof_digest": exact_item["snapshot_proof_digest"],
        "mode": settings.runtime_mode,
        "position_id": exact.id,
        "symbol": "003550",
        "updated_from": exact_item["updated_from"],
        "last_reconciled_at": exact_item["last_reconciled_at"],
        "legacy_payload_digest": exact_item["legacy_payload_digest"],
        "legacy_integer_quantity": "10",
        "legacy_base_quantity": None,
        "legacy_quantity": "10",
        "legacy_average_price": "250000",
        "broker_quantity": "10",
        "broker_average_price": "250000",
        "provider": "kis",
        "account_scope": exact_item["account_scope"],
        "exchange": "KRX",
    }
    assert exact_item["evidence_fingerprint"] == hashlib.sha256(
        json.dumps(
            existing_fingerprint_material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert by_symbol["005930"]["reason"] == "exact_broker_holding_match"
    assert by_symbol["000660"]["reason"] == "quantity_mismatch"
    assert by_symbol["000660"]["evidence_fingerprint"] is None
    assert adapter.snapshot_requests == [
        (["000660", "003550", "005930"], "KRX")
    ]
    db.expire_all()
    assert all(
        position.provider is None
        and position.account_scope is None
        and position.exchange is None
        for position in db.scalars(select(ExecutionPosition)).all()
    )
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_alpaca_paper_fractional_legacy_attribution_requires_exact_approved_truth(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_broker_provider": "alpaca",
            "alpaca_api_secret": "paper-scope-secret",
        }
    )
    qqq = _legacy_fractional_position(
        symbol="QQQ",
        base_quantity="0.125",
        average_price="481.25",
    )
    spy = _legacy_fractional_position(
        symbol="SPY",
        base_quantity="0.375",
        average_price="598.50",
    )
    db.add_all([qqq, spy])
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            broker_account_id="alpaca-paper-account-id",
            observed_at=NOW,
            source="alpaca:paper:us_equity",
            holdings=[
                BrokerAccountHolding(
                    symbol="QQQ",
                    quantity=0,
                    average_price=Decimal("481.25"),
                    payload={
                        "provider": "alpaca",
                        "asset_class": "us_equity",
                        "base_quantity": "0.125",
                    },
                ),
                BrokerAccountHolding(
                    symbol="SPY",
                    quantity=0,
                    average_price=Decimal("598.50"),
                    payload={
                        "provider": "alpaca",
                        "asset_class": "us_equity",
                        "base_quantity": "0.375",
                    },
                ),
            ],
            payload={
                "provider": "alpaca",
                "mode": "paper",
                "asset_class": "us_equity",
                "holdings_coverage": "complete",
            },
        )
    )
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)

    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="US",
        now=NOW,
    )

    assert dry_run["provider"] == "alpaca"
    assert dry_run["exchange"] == "US"
    assert dry_run["summary"] == {
        "candidate_count": 2,
        "eligible_count": 2,
        "manual_review_count": 0,
    }
    assert {item["legacy_quantity"] for item in dry_run["items"]} == {
        "0.125",
        "0.375",
    }
    assert all(
        item["reason"] == "exact_broker_holding_match"
        and isinstance(item["evidence_fingerprint"], str)
        for item in dry_run["items"]
    )
    assert all(position.provider is None for position in (qqq, spy))

    result = remediation_service.apply_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="US",
        approved_evidence={
            str(item["position_id"]): str(item["evidence_fingerprint"])
            for item in dry_run["items"]
        },
        actor="paper-operator",
        reason="broker paper holdings reviewed",
        now=NOW,
    )

    assert result["summary"] == {
        "approved_count": 2,
        "attributed_count": 2,
        "manual_review_count": 0,
    }
    db.expire_all()
    persisted = db.scalars(
        select(ExecutionPosition).order_by(ExecutionPosition.symbol.asc())
    ).all()
    assert [(row.symbol, row.quantity, row.base_quantity) for row in persisted] == [
        ("QQQ", 0, Decimal("0.125000000000")),
        ("SPY", 0, Decimal("0.375000000000")),
    ]
    assert all(
        row.provider == "alpaca"
        and row.exchange == "US"
        and len(str(row.account_scope)) == 24
        and row.raw_payload["_position_identity"]["provider"] == "alpaca"
        and row.raw_payload["_position_identity"]["exchange"] == "US"
        for row in persisted
    )
    events = db.scalars(select(RuntimeRemediationEvent)).all()
    assert len(events) == 2
    assert all(event.outcome == "position_route_attributed" for event in events)
    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols={"QQQ", "SPY"},
        exchange="US",
        authenticated_access_token="test-token",
    )
    assert db.query(ExecutionPosition).count() == 2
    assert adapter.snapshot_requests == [
        (["QQQ", "SPY"], "US"),
        (["QQQ", "SPY"], "US"),
        (["QQQ", "SPY"], "US"),
    ]
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("settings_update", "snapshot_payload", "expected_error"),
    [
        (
            {"runtime_broker_provider": "kis"},
            {"provider": "alpaca", "mode": "paper", "asset_class": "us_equity"},
            "requires runtime broker provider alpaca",
        ),
        (
            {"runtime_broker_provider": "alpaca", "runtime_mode": "live"},
            {"provider": "alpaca", "mode": "live", "asset_class": "us_equity"},
            "supports only paper mode",
        ),
        (
            {"runtime_broker_provider": "alpaca"},
            {"provider": "alpaca", "mode": "paper", "asset_class": "crypto"},
            "US-equity snapshot proof is unavailable",
        ),
    ],
)
def test_alpaca_legacy_attribution_fails_closed_without_exact_route_proof(
    monkeypatch,
    settings_update: dict[str, str],
    snapshot_payload: dict[str, str],
    expected_error: str,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "alpaca_api_secret": "paper-scope-secret",
            **settings_update,
        }
    )
    db.add(
        _legacy_fractional_position(
            symbol="QQQ",
            base_quantity="0.125",
            average_price="481.25",
        )
    )
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            broker_account_id="alpaca-paper-account-id",
            observed_at=NOW,
            source=f"alpaca:{settings.runtime_mode}:us_equity",
            holdings=[
                BrokerAccountHolding(
                    symbol="QQQ",
                    quantity=0,
                    average_price=Decimal("481.25"),
                    payload={
                        "provider": "alpaca",
                        "asset_class": "us_equity",
                        "base_quantity": "0.125",
                    },
                )
            ],
            payload=snapshot_payload,
        )
    )
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(PositionIdentityConflictError, match=expected_error):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="US",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("legacy_payload", "holding_payload", "expected_reason"),
    [
        (
            {"legacy_snapshot": True},
            {
                "provider": "alpaca",
                "asset_class": "us_equity",
                "base_quantity": "0.125",
            },
            "legacy_route_proof_missing",
        ),
        (
            {
                "provider": "alpaca",
                "mode": "paper",
                "asset_class": "us_equity",
            },
            {"provider": "alpaca", "asset_class": "us_equity"},
            "broker_holding_route_proof_missing",
        ),
    ],
)
def test_alpaca_legacy_attribution_rejects_missing_row_or_fractional_provenance(
    monkeypatch,
    legacy_payload: dict[str, object],
    holding_payload: dict[str, object],
    expected_reason: str,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_broker_provider": "alpaca",
            "alpaca_api_secret": "paper-scope-secret",
        }
    )
    position = _legacy_fractional_position(
        symbol="QQQ",
        base_quantity="0.125",
        average_price="481.25",
    )
    position.raw_payload = legacy_payload
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        _alpaca_paper_us_snapshot(holding_payload=holding_payload)
    )
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="US",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == expected_reason
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


def test_alpaca_legacy_attribution_rejects_missing_authenticated_account_id(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_broker_provider": "alpaca",
            "alpaca_api_secret": "paper-scope-secret",
        }
    )
    db.add(
        _legacy_fractional_position(
            symbol="QQQ",
            base_quantity="0.125",
            average_price="481.25",
        )
    )
    db.commit()
    adapter = LegacyAttributionAdapter(
        _alpaca_paper_us_snapshot(account_id=None)
    )
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="US-equity snapshot proof is unavailable",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="US",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "drifted_snapshot",
    [
        _alpaca_paper_us_snapshot(
            account_id="rotated-alpaca-paper-account-id"
        ),
        _alpaca_paper_us_snapshot(base_quantity="0.126"),
    ],
)
def test_alpaca_legacy_attribution_apply_rejects_account_or_quantity_drift(
    monkeypatch,
    drifted_snapshot: BrokerAccountSnapshot,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_broker_provider": "alpaca",
            "alpaca_api_secret": "paper-scope-secret",
        }
    )
    position = _legacy_fractional_position(
        symbol="QQQ",
        base_quantity="0.125",
        average_price="481.25",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(_alpaca_paper_us_snapshot())
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="US",
        now=NOW,
    )
    approved_fingerprint = str(dry_run["items"][0]["evidence_fingerprint"])
    adapter.snapshot = drifted_snapshot

    with pytest.raises(
        PositionIdentityConflictError,
        match="approved legacy position evidence no longer matches",
    ):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="US",
            approved_evidence={position.id: approved_fingerprint},
            actor="paper-operator",
            reason="broker paper holdings reviewed",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_legacy_otc_attribution_dry_run_is_holdings_only_and_quarantined(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )
    position = _legacy_position(symbol="AFIB", quantity=4, average_price="1.25")
    position.mode = "live"
    unmatched = _legacy_position(
        symbol="MISSINGQ",
        quantity=1,
        average_price="2.50",
    )
    unmatched.mode = "live"
    db.add_all([position, unmatched])
    db.commit()
    before = {
        "positions": db.query(ExecutionPosition).count(),
        "orders": db.query(BrokerOrder).count(),
        "fills": db.query(ExecutionFill).count(),
        "remediation_events": db.query(RuntimeRemediationEvent).count(),
        "raw_payload": dict(position.raw_payload),
    }
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:legacy_us_holdings_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AFIB",
                    quantity=4,
                    average_price=Decimal("1.25"),
                    payload={"ovrs_excg_cd": "OTCB"},
                )
            ],
            open_orders=[],
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OTCB",
        now=NOW,
    )

    assert result["summary"] == {
        "candidate_count": 2,
        "eligible_count": 1,
        "quarantined_on_apply_count": 1,
        "manual_review_count": 2,
    }
    item = result["items"][0]
    assert item["eligible"] is True
    assert item["attribution_eligible"] is True
    assert item["trading_eligible"] is False
    assert item["monitoring_eligible"] is False
    assert item["manual_review_required"] is True
    assert item["disposition"] == "account_attributed_quarantine"
    assert item["query_scope"] == "kis_us_all_holdings_only_v1"
    assert item["safety_policy_version"] == (
        "legacy-position-attribution-safety-v4"
    )
    assert item["safety_invariants"] == {
        "account_scope_source": "stable_hmac",
        "broker_actions_allowed": False,
        "collision_scope": "mode_provider_account_symbol_any_exchange",
        "fractional_quantity_allowed": False,
        "manual_review_required": True,
        "monitoring_allowed": False,
        "provider": "kis",
        "route_disposition": "account_attributed_quarantine",
    }
    assert len(item["snapshot_proof_digest"]) == 64
    fingerprint_material = {
        "safety_policy_version": item["safety_policy_version"],
        "safety_invariants": item["safety_invariants"],
        "snapshot_proof_digest": item["snapshot_proof_digest"],
        "mode": settings.runtime_mode,
        "position_id": position.id,
        "symbol": "AFIB",
        "updated_from": item["updated_from"],
        "last_reconciled_at": item["last_reconciled_at"],
        "legacy_payload_digest": item["legacy_payload_digest"],
        "legacy_integer_quantity": "4",
        "legacy_base_quantity": None,
        "legacy_quantity": "4",
        "legacy_average_price": "1.25",
        "broker_quantity": "4",
        "broker_average_price": "1.25",
        "provider": "kis",
        "account_scope": item["account_scope"],
        "exchange": "OTCB",
        "query_scope": "kis_us_all_holdings_only_v1",
        "disposition": "account_attributed_quarantine",
    }
    assert item["evidence_fingerprint"] == hashlib.sha256(
        json.dumps(
            fingerprint_material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert adapter.snapshot_requests == [(None, "US_ALL_HOLDINGS_ONLY")]
    db.expire_all()
    assert {
        "positions": db.query(ExecutionPosition).count(),
        "orders": db.query(BrokerOrder).count(),
        "fills": db.query(ExecutionFill).count(),
        "remediation_events": db.query(RuntimeRemediationEvent).count(),
        "raw_payload": dict(db.get(ExecutionPosition, position.id).raw_payload),
    } == before
    db.close()
    engine.dispose()


def test_legacy_otc_attribution_apply_persists_non_trading_quarantine(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": None,
            "execution_position_account_scope": "test-live-account",
        }
    )
    position = _legacy_position(symbol="OTRQQ", quantity=2, average_price="3.50")
    position.mode = "live"
    position.raw_payload["_position_sync"] = {
        "source": "legacy-import",
        "exchange": "OOTC",
    }
    db.add(position)
    db.commit()
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:legacy_us_holdings_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="OTRQQ",
                    quantity=2,
                    average_price=Decimal("3.50"),
                    payload={"ovrs_excg_cd": "OOTC"},
                )
            ],
            open_orders=[],
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OOTC",
        now=NOW,
    )
    expected_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OOTC",
    )
    active_us_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    assert dry_run["items"][0]["account_scope"] == expected_identity.account_scope
    assert expected_identity.account_scope == active_us_identity.account_scope
    assert dry_run["items"][0]["safety_invariants"]["account_scope_source"] == (
        "configured_route_label"
    )
    fingerprint = str(dry_run["items"][0]["evidence_fingerprint"])

    result = remediation_service.apply_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OOTC",
        approved_evidence={position.id: fingerprint},
        actor="operator",
        reason="exact live KIS holdings evidence reviewed",
        now=NOW,
    )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider == "kis"
    assert persisted.account_scope == dry_run["items"][0]["account_scope"]
    assert persisted.exchange == "OOTC"
    assert persisted.raw_payload["_position_sync"] == {
        "asset_class": "us_equity",
        "exchange": "OOTC",
        "monitoring_eligible": False,
        "trade_eligible": False,
        "manual_review_required": True,
        "manual_review_reason": "unsupported_overseas_exchange",
        "route_disposition": "account_attributed_quarantine",
    }
    assert persisted.raw_payload["legacy_position_sync"] == {
        "source": "legacy-import",
        "exchange": "OOTC",
    }
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    assert event.outcome == "position_route_attributed_quarantine"
    assert event.evidence_summary["query_scope"] == "kis_us_all_holdings_only_v1"
    assert event.evidence_summary["route_disposition"] == (
        "account_attributed_quarantine"
    )
    assert event.evidence_summary["trading_eligible"] is False
    assert event.evidence_summary["monitoring_eligible"] is False
    assert result["items"][0]["outcome"] == "position_route_attributed_quarantine"
    assert result["summary"] == {
        "approved_count": 1,
        "attributed_count": 1,
        "quarantined_count": 1,
        "manual_review_count": 1,
    }
    assert db.query(BrokerOrder).count() == 0
    assert db.query(ExecutionFill).count() == 0
    db.close()
    engine.dispose()


def test_legacy_otc_attribution_requires_exact_holdings_only_query_proof(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )
    position = _legacy_position(symbol="AFIB", quantity=4, average_price="1.25")
    position.mode = "live"
    db.add(position)
    db.commit()
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AFIB",
                    quantity=4,
                    average_price=Decimal("1.25"),
                    payload={"ovrs_excg_cd": "OTCB"},
                )
            ],
            payload={"exchange": "OTCB"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="holdings-only query proof is unavailable",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="OTCB",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_legacy_otc_attribution_rejects_non_live_mode_before_authentication(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add(_legacy_position(symbol="AFIB", quantity=4, average_price="1.25"))
    db.commit()
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:legacy_us_holdings_snapshot",
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: pytest.fail("non-live OTC attribution attempted authentication"),
    )

    with pytest.raises(ValueError, match="requires live mode"):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="OTCB",
            now=NOW,
        )

    assert adapter.snapshot_requests == []
    db.close()
    engine.dispose()


def test_legacy_otc_attribution_requires_canonical_account_scope_before_db_read(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "execution_position_account_scope": None,
            "account_drawdown_recovery_scope_secret": "too-short",
        }
    )
    monkeypatch.setattr(
        db,
        "scalars",
        lambda *_args, **_kwargs: pytest.fail("unsafe scope opened position rows"),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="stable canonical account scope",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=LegacyQuarantineAttributionAdapter(
                BrokerAccountSnapshot(observed_at=NOW, source="test")
            ),
            exchange="OTCB",
            now=NOW,
        )

    db.close()
    engine.dispose()


def test_legacy_otc_empty_dry_run_keeps_quarantine_summary_shape() -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=LegacyQuarantineAttributionAdapter(
            BrokerAccountSnapshot(observed_at=NOW, source="unused")
        ),
        exchange="OTCB",
        now=NOW,
    )

    assert result["summary"] == {
        "candidate_count": 0,
        "eligible_count": 0,
        "manual_review_count": 0,
        "quarantined_on_apply_count": 0,
    }
    assert result["items"] == []
    db.close()
    engine.dispose()


def test_legacy_attribution_rejects_normalized_same_account_symbol_collision_across_exchange(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NYSE",
    )
    legacy = _legacy_position(symbol="AFIB", quantity=4, average_price="1.25")
    legacy.mode = "live"
    db.add_all(
        [
            legacy,
            ExecutionPosition(
                mode=" LIVE ",
                provider=" KIS ",
                account_scope=f" {identity.account_scope.upper()} ",
                exchange="NYSE",
                symbol=" afib ",
                quantity=4,
                average_price=Decimal("1.25"),
                updated_from="reconciliation",
            ),
        ]
    )
    db.commit()
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:legacy_us_holdings_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AFIB",
                    quantity=4,
                    average_price=Decimal("1.25"),
                    payload={"ovrs_excg_cd": "OTCB"},
                )
            ],
            open_orders=[],
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OTCB",
        now=NOW,
    )

    assert result["items"][0]["eligible"] is False
    assert result["items"][0]["reason"] == "scoped_route_symbol_collision"
    db.close()
    engine.dispose()


def test_legacy_quarantine_attribution_rejects_non_mapping_payload(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )
    position = _legacy_position(symbol="AFIB", quantity=4, average_price="1.25")
    position.mode = "live"
    position.raw_payload = ["legacy", "payload"]
    db.add(position)
    db.commit()
    adapter = LegacyQuarantineAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:legacy_us_holdings_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AFIB",
                    quantity=4,
                    average_price=Decimal("1.25"),
                    payload={"ovrs_excg_cd": "OTCB"},
                )
            ],
            open_orders=[],
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OTCB",
        now=NOW,
    )

    assert result["items"][0]["eligible"] is False
    assert result["items"][0]["reason"] == "legacy_payload_invalid"
    db.close()
    engine.dispose()


def test_legacy_quarantine_support_is_separate_from_position_refresh_routes() -> None:
    assert remediation_service.SUPPORTED_POSITION_EXCHANGES == {
        "KRX",
        "NASD",
        "NYSE",
        "AMEX",
    }
    assert remediation_service.LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES == {
        "OTCB",
        "OOTC",
    }


def test_legacy_attribution_serializes_with_kis_order_pre_auth_scope() -> None:
    settings, engine, _session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "kis_account_number": "12345678",
            "kis_account_product_code": "01",
            "account_drawdown_recovery_scope_secret": "s" * 32,
        }
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )
    order_scope = broker_orders_service.order_intent_scope_key_for_account(
        provider="kis",
        runtime_mode=settings.runtime_mode,
        account_scope=recovery_account_fingerprint(settings),
        market="USD",
    )
    order_lock = broker_orders_service.order_intent_process_lock(order_scope)
    lock_acquired = Event()
    release_lock = Event()

    def hold_order_scope() -> None:
        with order_lock:
            lock_acquired.set()
            assert release_lock.wait(timeout=2)

    holder = Thread(target=hold_order_scope)
    holder.start()
    assert lock_acquired.wait(timeout=2)
    serialization = None
    try:
        with pytest.raises(
            PositionIdentityConflictError,
            match="scope is busy",
        ):
            serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
                settings=settings,
                db=db,
                provider="kis",
                account_scope=identity.account_scope,
                exchange="OTCB",
            )
    finally:
        if serialization is not None:
            serialization.release()
        release_lock.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    db.close()
    engine.dispose()


def test_alpaca_legacy_attribution_serializes_with_order_pre_auth_scope() -> None:
    settings, engine, _session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_broker_provider": "alpaca",
            "alpaca_api_key": "paper-key",
            "alpaca_api_secret": "paper-secret",
        }
    )
    identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="US",
        broker_account_id="alpaca-paper-account-id",
    )
    command = PlaceOrderRequest(
        idempotency_key="alpaca-attribution-pre-auth-lock",
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        symbol="QQQ",
        side="sell",
        base_quantity=Decimal("0.125"),
        order_type="market",
        time_in_force="day",
        exchange="US",
    )
    order_scope = broker_orders_service._order_intent_scope_key(
        settings=settings,
        adapter=SimpleNamespace(provider="alpaca"),
        command=command,
    )
    order_lock = broker_orders_service.order_intent_process_lock(order_scope)
    lock_acquired = Event()
    release_lock = Event()

    def hold_order_scope() -> None:
        with order_lock:
            lock_acquired.set()
            assert release_lock.wait(timeout=2)

    holder = Thread(target=hold_order_scope)
    holder.start()
    assert lock_acquired.wait(timeout=2)
    serialization = None
    try:
        with pytest.raises(
            PositionIdentityConflictError,
            match="scope is busy",
        ):
            serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
                settings=settings,
                db=db,
                provider="alpaca",
                account_scope=identity.account_scope,
                exchange="US",
            )
    finally:
        if serialization is not None:
            serialization.close()
        release_lock.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    assert order_scope.endswith(":ACCOUNT")
    db.close()
    engine.dispose()


def test_legacy_attribution_fails_closed_when_sqlite_order_file_scope_is_busy(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'attribution-lock.db'}",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="s" * 32,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )
    order_scope = broker_orders_service.order_intent_scope_key_for_account(
        provider="kis",
        runtime_mode=settings.runtime_mode,
        account_scope=recovery_account_fingerprint(settings),
        market="USD",
    )
    digest = hashlib.sha256(order_scope.encode("utf-8")).hexdigest()[:24]
    lock_path = tmp_path / f"kai-exec-order-intent-{digest}.lock"
    monkeypatch.setattr(
        order_intent_serialization_service,
        "gettempdir",
        lambda: str(tmp_path),
    )

    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    serialization = None
    try:
        with os.fdopen(descriptor, "a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            with pytest.raises(
                PositionIdentityConflictError,
                match="lock is busy",
            ):
                serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
                    settings=settings,
                    db=db,
                    provider="kis",
                    account_scope=identity.account_scope,
                    exchange="OTCB",
                )
    finally:
        if serialization is not None:
            serialization.release()
        db.close()
        engine.dispose()


def test_legacy_attribution_reserves_sqlite_writer_before_collision_recheck(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'attribution-write-reservation.db'}",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="s" * 32,
        sqlite_busy_timeout_seconds=1,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    db = session_factory()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )
    monkeypatch.setattr(
        order_intent_serialization_service,
        "gettempdir",
        lambda: str(tmp_path),
    )
    serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
        settings=settings,
        db=db,
        provider="kis",
        account_scope=identity.account_scope,
        exchange="OTCB",
    )

    try:
        with session_factory() as concurrent_db:
            concurrent_db.connection().exec_driver_sql("PRAGMA busy_timeout=0")
            concurrent_db.add(
                ExecutionPosition(
                    mode="live",
                    provider="kis",
                    account_scope=identity.account_scope,
                    exchange="NYSE",
                    symbol="AFIB",
                    quantity=1,
                    average_price=Decimal("2"),
                    updated_from="concurrent_reconciliation",
                )
            )
            with pytest.raises(DBAPIError, match="database is locked"):
                concurrent_db.commit()
            concurrent_db.rollback()
    finally:
        db.rollback()
        serialization.close()
        db.close()
        engine.dispose()


def test_legacy_attribution_uses_non_waiting_exact_postgres_apply_locks() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://localhost/kai_exec_test",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="s" * 32,
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )
    scalar_calls: list[tuple[str, dict[str, str]]] = []
    execute_calls: list[str] = []

    class PostgresSessionStub:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def scalar(self, statement, params):
            scalar_calls.append((str(statement), params))
            return True

        def execute(self, statement):
            execute_calls.append(str(statement))

    serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
        settings=settings,
        db=PostgresSessionStub(),
        provider="kis",
        account_scope=identity.account_scope,
        exchange="OTCB",
    )
    serialization.close()

    assert scalar_calls == [
        (
            "SELECT pg_try_advisory_xact_lock(hashtext(:scope))",
            {
                "scope": broker_orders_service.order_intent_scope_key_for_account(
                    provider="kis",
                    runtime_mode="live",
                    account_scope=identity.account_scope,
                    market="USD",
                )
            },
        )
    ]
    assert len(execute_calls) == 1
    assert "LOCK TABLE execution_positions" in execute_calls[0]
    assert "NOWAIT" in execute_calls[0]


def test_alpaca_legacy_attribution_uses_exact_order_account_lock_scope() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://localhost/kai_exec_test",
        runtime_mode="paper",
        runtime_broker_provider="alpaca",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
    )
    identity = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="US",
        broker_account_id="alpaca-paper-account-id",
    )
    command = PlaceOrderRequest(
        idempotency_key="alpaca-attribution-lock-scope",
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        symbol="QQQ",
        side="sell",
        base_quantity=Decimal("0.125"),
        order_type="market",
        time_in_force="day",
        exchange="US",
    )
    expected_scope = broker_orders_service._order_intent_scope_key(
        settings=settings,
        adapter=SimpleNamespace(provider="alpaca"),
        command=command,
        execution_scope=execution_scope_payload(
            identity=identity,
            venue="ALPACA",
            asset_class="us_equity",
        ),
    )
    scalar_calls: list[dict[str, str]] = []

    class PostgresSessionStub:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def scalar(self, _statement, params):
            scalar_calls.append(params)
            return True

        def execute(self, _statement):
            return None

    serialization = remediation_service._acquire_legacy_attribution_apply_serialization(
        settings=settings,
        db=PostgresSessionStub(),
        provider="alpaca",
        account_scope=identity.account_scope,
        exchange="US",
    )
    serialization.close()

    assert scalar_calls == [{"scope": expected_scope}]
    assert expected_scope == (
        "broker-order-intent:alpaca:paper:"
        f"{identity.account_scope}:ACCOUNT"
    )


def test_quarantine_attribution_preserves_explicit_null_position_sync() -> None:
    settings = Settings(runtime_mode="paper")
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )

    payload = remediation_service._attributed_position_payload(
        payload={"_position_sync": None},
        identity=identity,
        quarantine_attribution=True,
    )

    assert "legacy_position_sync" in payload
    assert payload["legacy_position_sync"] is None
    assert payload["_position_sync"]["route_disposition"] == (
        "account_attributed_quarantine"
    )


def test_quarantine_attribution_rejects_non_mapping_position_sync() -> None:
    settings = Settings(runtime_mode="paper")
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="OTCB",
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="position sync evidence is not a mapping",
    ):
        remediation_service._attributed_position_payload(
            payload={"_position_sync": "unstructured-legacy-value"},
            identity=identity,
            quarantine_attribution=True,
        )


def test_legacy_snapshot_proof_digest_binds_holdings_not_observation_time() -> None:
    def snapshot(*, observed_at: datetime, quantity: int) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            observed_at=observed_at,
            source="kis:legacy_us_holdings_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AFIB",
                    quantity=quantity,
                    average_price=Decimal("1.25"),
                    payload={"ovrs_excg_cd": "OTCB"},
                )
            ],
            open_orders=[],
            open_orders_complete=False,
            payload={
                "exchange": "US_ALL",
                "query_scope": "kis_us_all_holdings_only_v1",
                "evidence_type": "holdings_only",
            },
        )

    first = remediation_service._legacy_snapshot_proof_digest(
        snapshot=snapshot(observed_at=NOW, quantity=4),
        exchange="OTCB",
        query_scope="kis_us_all_holdings_only_v1",
    )
    later = remediation_service._legacy_snapshot_proof_digest(
        snapshot=snapshot(observed_at=NOW + timedelta(seconds=30), quantity=4),
        exchange="OTCB",
        query_scope="kis_us_all_holdings_only_v1",
    )
    changed = remediation_service._legacy_snapshot_proof_digest(
        snapshot=snapshot(observed_at=NOW + timedelta(seconds=30), quantity=5),
        exchange="OTCB",
        query_scope="kis_us_all_holdings_only_v1",
    )

    assert first == later
    assert first != changed


@pytest.mark.parametrize(
    ("broker_quantity", "broker_average_price", "expected_reason"),
    [
        (9, Decimal("250000"), "quantity_mismatch"),
        (10, Decimal("249999"), "average_price_mismatch"),
        (10, None, "average_price_missing"),
    ],
)
def test_legacy_position_attribution_requires_exact_quantity_and_average_price(
    monkeypatch,
    broker_quantity: int,
    broker_average_price: Decimal | None,
    expected_reason: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=broker_quantity,
                    average_price=broker_average_price,
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == expected_reason
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_incomplete_holdings_snapshot(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            holdings_complete=False,
            diagnostics={"holdings_pagination": {"complete": False}},
            payload={"exchange": "KRX", "holdings_coverage": "incomplete"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(BrokerAdapterError, match="holdings coverage is incomplete"):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_duplicate_broker_holdings(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="AAPL",
        quantity=2,
        average_price="200",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": "NASD"},
                ),
                BrokerAccountHolding(
                    symbol="aapl",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": "NASD"},
                ),
            ],
            payload={"exchange": "NASD"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="duplicate broker holding",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="NASD",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "snapshot_observed_at",
    [
        NOW - timedelta(seconds=301),
        NOW + timedelta(seconds=31),
        NOW.replace(tzinfo=None),
    ],
)
def test_legacy_position_attribution_requires_current_aware_snapshot(
    monkeypatch,
    snapshot_observed_at: datetime,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=snapshot_observed_at,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(BrokerAdapterError, match="snapshot is not current"):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_missing_broker_provider(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add(_legacy_position(symbol="003550", quantity=10, average_price="250000"))
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="unknown:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    adapter.provider = ""
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="broker provider is unavailable",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


def test_alpaca_legacy_position_attribution_rejects_noncanonical_exchange(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "runtime_broker_provider": "alpaca",
        }
    )
    db.add(_legacy_position(symbol="AAPL", quantity=2, average_price="200"))
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            broker_account_id="alpaca-account-id",
            observed_at=NOW,
            source="alpaca:paper:us_equity",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                )
            ],
            payload={"asset_class": "us_equity"},
        )
    )
    adapter.provider = "alpaca"
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="requires exchange US",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="NASD",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_snapshot_route_mismatch(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add(_legacy_position(symbol="AAPL", quantity=2, average_price="200"))
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": "NYSE"},
                )
            ],
            payload={"exchange": "NYSE"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="snapshot exchange does not match",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="NASD",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


def test_legacy_kis_position_attribution_requires_snapshot_route_proof(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add(_legacy_position(symbol="003550", quantity=10, average_price="250000"))
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"rt_cd": "0"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="snapshot exchange proof is unavailable",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            now=NOW,
        )

    assert db.scalar(select(ExecutionPosition)).provider is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_blocks_holding_route_mismatch(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(symbol="AAPL", quantity=2, average_price="200")
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"ovrs_excg_cd": "NYSE"},
                )
            ],
            payload={"exchange": "NASD"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="NASD",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "broker_holding_exchange_mismatch"
    assert result["items"][0]["evidence_fingerprint"] is None
    db.expire_all()
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


def test_legacy_us_position_attribution_requires_holding_exchange_proof(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(symbol="AAPL", quantity=2, average_price="200")
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={},
                )
            ],
            payload={"exchange": "NASD"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="NASD",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "broker_holding_exchange_missing"
    assert result["items"][0]["evidence_fingerprint"] is None
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_blocks_existing_target_route_collision(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    legacy = _legacy_position(symbol="003550", quantity=10, average_price="250000")
    scoped = _scoped_position(
        settings,
        exchange="KRX",
        mode="paper",
        symbol="003550",
        quantity=10,
        average_price=Decimal("250000"),
        updated_from="reconciliation",
        last_reconciled_at=NOW,
    )
    db.add_all([legacy, scoped])
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "scoped_route_symbol_collision"
    assert result["items"][0]["evidence_fingerprint"] is None
    db.expire_all()
    assert db.get(ExecutionPosition, legacy.id).provider is None
    assert db.get(ExecutionPosition, scoped.id).provider == "kis"
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_normalized_legacy_symbol_duplicates(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    upper = _legacy_position(symbol="AAPL", quantity=2, average_price="200")
    lower = _legacy_position(symbol="aapl", quantity=2, average_price="200")
    db.add_all([upper, lower])
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": "NASD"},
                )
            ],
            payload={"exchange": "NASD"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="duplicate legacy position symbol",
    ):
        remediation_service.analyze_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="NASD",
            now=NOW,
        )

    assert adapter.snapshot_requests == []
    db.expire_all()
    assert all(
        position.provider is None
        for position in db.scalars(select(ExecutionPosition)).all()
    )
    db.close()
    engine.dispose()


def test_legacy_position_attribution_blocks_noncanonical_legacy_symbol(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(symbol="aapl", quantity=2, average_price="200")
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:us_equity_account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": "NASD"},
                )
            ],
            payload={"exchange": "NASD"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="NASD",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "legacy_symbol_not_canonical"
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("symbol", "exchange"),
    [("AAPL", "KRX"), ("003550", "NASD")],
)
def test_legacy_position_attribution_blocks_symbol_market_shape_mismatch(
    monkeypatch,
    symbol: str,
    exchange: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(symbol=symbol, quantity=2, average_price="200")
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol=symbol,
                    quantity=2,
                    average_price=Decimal("200"),
                    payload={"exchange": exchange},
                )
            ],
            payload={"exchange": exchange},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange=exchange,
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "symbol_exchange_incompatible"
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_apply_binds_only_explicitly_approved_match(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={"account_drawdown_recovery_scope_secret": "s" * 32}
    )
    approved = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    unapproved = _legacy_position(
        symbol="005930",
        quantity=2,
        average_price="80000",
    )
    mismatch = _legacy_position(
        symbol="000660",
        quantity=5,
        average_price="120000",
    )
    db.add_all([approved, unapproved, mismatch])
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                ),
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("80000"),
                ),
                BrokerAccountHolding(
                    symbol="000660",
                    quantity=6,
                    average_price=Decimal("120000"),
                ),
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )
    approved_item = next(
        item for item in dry_run["items"] if item["position_id"] == approved.id
    )

    result = remediation_service.apply_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        approved_evidence={
            approved.id: str(approved_item["evidence_fingerprint"]),
        },
        actor="  Peter  ",
        reason="  bind the broker-proven legacy position  ",
        now=NOW,
    )

    assert result["mode"] == "apply"
    assert result["summary"] == {
        "approved_count": 1,
        "attributed_count": 1,
        "manual_review_count": 0,
    }
    db.expire_all()
    attributed = db.get(ExecutionPosition, approved.id)
    preserved_unapproved = db.get(ExecutionPosition, unapproved.id)
    preserved_mismatch = db.get(ExecutionPosition, mismatch.id)
    expected_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    assert attributed is not None
    assert (
        attributed.provider,
        attributed.account_scope,
        attributed.exchange,
    ) == (
        expected_identity.provider,
        expected_identity.account_scope,
        expected_identity.exchange,
    )
    assert attributed.quantity == 10
    assert attributed.average_price == Decimal("250000.0000")
    assert attributed.raw_payload["legacy_snapshot"] is True
    assert attributed.raw_payload["_position_identity"] == expected_identity.as_payload()
    assert preserved_unapproved is not None
    assert preserved_unapproved.provider is None
    assert preserved_mismatch is not None
    assert preserved_mismatch.provider is None
    events = db.scalars(select(RuntimeRemediationEvent)).all()
    assert len(events) == 1
    assert events[0].candidate_type == "legacy_position_attribution"
    assert events[0].candidate_key == approved.id
    assert events[0].outcome == "position_route_attributed"
    assert events[0].actor == "Peter"
    assert events[0].reason == "bind the broker-proven legacy position"
    assert events[0].evidence_summary["evidence_fingerprint"] == approved_item[
        "evidence_fingerprint"
    ]
    db.close()
    engine.dispose()


def test_legacy_attribution_apply_rechecks_cross_exchange_collision_after_analysis(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": "s" * 32,
            "execution_position_account_scope": "test-live-account",
        }
    )
    position = _legacy_position(symbol="AFIB", quantity=4, average_price="1.25")
    position.mode = "live"
    db.add(position)
    db.commit()
    snapshot = BrokerAccountSnapshot(
        observed_at=NOW,
        source="kis:legacy_us_holdings_snapshot",
        holdings=[
            BrokerAccountHolding(
                symbol="AFIB",
                quantity=4,
                average_price=Decimal("1.25"),
                payload={"ovrs_excg_cd": "OTCB"},
            )
        ],
        open_orders=[],
        open_orders_complete=False,
        payload={
            "exchange": "US_ALL",
            "query_scope": "kis_us_all_holdings_only_v1",
            "evidence_type": "holdings_only",
        },
    )
    adapter = LegacyQuarantineAttributionAdapter(snapshot)
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="OTCB",
        now=NOW,
    )
    fingerprint = str(dry_run["items"][0]["evidence_fingerprint"])
    original_analyze = remediation_service.analyze_legacy_position_attribution

    def analyze_then_insert_collision(**kwargs):
        result = original_analyze(**kwargs)
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="NYSE",
        )
        db.add(
            ExecutionPosition(
                mode=" LIVE ",
                provider=" KIS ",
                account_scope=f" {identity.account_scope.upper()} ",
                exchange="NYSE",
                symbol=" afib ",
                quantity=1,
                average_price=Decimal("2"),
                updated_from="concurrent_reconciliation",
            )
        )
        db.commit()
        return result

    monkeypatch.setattr(
        remediation_service,
        "analyze_legacy_position_attribution",
        analyze_then_insert_collision,
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="scoped route symbol collision",
    ):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="OTCB",
            approved_evidence={position.id: fingerprint},
            actor="operator",
            reason="race must fail closed",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None and persisted.provider is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_legacy_attribution_apply_rechecks_snapshot_freshness_after_lock_with_real_time(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={"account_drawdown_recovery_scope_secret": "s" * 32}
    )
    position = _legacy_position(symbol="003550", quantity=10, average_price="250000")
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )
    monkeypatch.setattr(
        remediation_service,
        "_real_utc_now",
        lambda: NOW + timedelta(minutes=10),
        raising=False,
    )

    with pytest.raises(BrokerAdapterError, match="snapshot is not current"):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            approved_evidence={
                position.id: str(dry_run["items"][0]["evidence_fingerprint"])
            },
            actor="operator",
            reason="stale after serialization",
            now=NOW,
        )

    db.expire_all()
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


def test_legacy_position_attribution_does_not_overwrite_stored_route_identity(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    foreign_identity = build_position_identity(
        settings=settings.model_copy(
            update={"execution_position_account_scope": "foreign-account"}
        ),
        provider="kis",
        exchange="KRX",
    )
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    position.raw_payload = {
        "_position_identity": foreign_identity.as_payload(),
    }
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    result = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == "stored_route_identity_present"
    assert result["items"][0]["evidence_fingerprint"] is None
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted.provider is None
    assert persisted.raw_payload["_position_identity"] == foreign_identity.as_payload()
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_provenance_drift_after_dry_run(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )
    fingerprint = str(dry_run["items"][0]["evidence_fingerprint"])
    position.updated_from = "manual_edit_after_review"
    db.commit()

    with pytest.raises(
        PositionIdentityConflictError,
        match="evidence no longer matches",
    ):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            approved_evidence={position.id: fingerprint},
            actor="Peter",
            reason="must re-review changed provenance",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.account_scope is None
    assert persisted.exchange is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_quantity_semantics_drift(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_position(
        symbol="003550",
        quantity=10,
        average_price="250000",
    )
    db.add(position)
    db.commit()
    adapter = LegacyAttributionAdapter(
        BrokerAccountSnapshot(
            observed_at=NOW,
            source="kis:account_snapshot",
            holdings=[
                BrokerAccountHolding(
                    symbol="003550",
                    quantity=10,
                    average_price=Decimal("250000"),
                )
            ],
            payload={"exchange": "KRX"},
        )
    )
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )
    fingerprint = str(dry_run["items"][0]["evidence_fingerprint"])
    position.quantity = 0
    position.base_quantity = Decimal("10")
    db.commit()

    with pytest.raises(
        PositionIdentityConflictError,
        match="evidence no longer matches",
    ):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            approved_evidence={position.id: fingerprint},
            actor="Peter",
            reason="quantity representation changed after review",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.quantity == 0
    assert persisted.base_quantity == Decimal("10.000000000000")
    db.close()
    engine.dispose()


def test_legacy_position_attribution_rejects_broker_drift_atomically(
    monkeypatch,
) -> None:
    settings, engine, session_factory, db = _session()
    first = _legacy_position(symbol="003550", quantity=10, average_price="250000")
    second = _legacy_position(symbol="005930", quantity=2, average_price="80000")
    db.add_all([first, second])
    db.commit()
    snapshot = BrokerAccountSnapshot(
        observed_at=NOW,
        source="kis:account_snapshot",
        holdings=[
            BrokerAccountHolding(
                symbol="003550",
                quantity=10,
                average_price=Decimal("250000"),
            ),
            BrokerAccountHolding(
                symbol="005930",
                quantity=2,
                average_price=Decimal("80000"),
            ),
        ],
        payload={"exchange": "KRX"},
    )
    adapter = LegacyAttributionAdapter(snapshot)
    monkeypatch.setattr(
        "app.services.position_truth_remediation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )
    dry_run = remediation_service.analyze_legacy_position_attribution(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        exchange="KRX",
        now=NOW,
    )
    approvals = {
        str(item["position_id"]): str(item["evidence_fingerprint"])
        for item in dry_run["items"]
    }
    adapter.snapshot = BrokerAccountSnapshot(
        observed_at=NOW,
        source="kis:account_snapshot",
        holdings=[
            BrokerAccountHolding(
                symbol="003550",
                quantity=10,
                average_price=Decimal("250000"),
            ),
            BrokerAccountHolding(
                symbol="005930",
                quantity=3,
                average_price=Decimal("80000"),
            ),
        ],
        payload={"exchange": "KRX"},
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="evidence no longer matches",
    ):
        remediation_service.apply_legacy_position_attribution(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            exchange="KRX",
            approved_evidence=approvals,
            actor="Peter",
            reason="both rows were reviewed together",
            now=NOW,
        )

    db.expire_all()
    persisted = db.scalars(select(ExecutionPosition)).all()
    assert len(persisted) == 2
    assert all(position.provider is None for position in persisted)
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()
