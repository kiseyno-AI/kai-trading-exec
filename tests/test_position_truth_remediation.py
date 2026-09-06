from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerReconnectHandle,
    BrokerTokenGrant,
)
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
from app.services.position_truth_remediation import (
    analyze_position_truth_remediation,
    apply_position_truth_remediation,
)


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


def _add_filled_us_buy_order_event(
    db,
    *,
    symbol: str,
    exchange: str,
    suffix: str,
    side: str = "buy",
    status: str = "filled",
) -> BrokerOrder:
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
            ExecutionPosition(
                mode="paper",
                symbol="003550",
                quantity=10,
                average_price=Decimal("250000"),
                updated_from="reconciliation",
                last_reconciled_at=old,
            ),
            ExecutionPosition(
                mode="paper",
                symbol="APTX",
                quantity=3,
                average_price=Decimal("1"),
                updated_from="reconciliation",
                last_reconciled_at=old,
                raw_payload={"_position_sync": {"exchange": "NASD"}},
            ),
            ExecutionPosition(
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
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"missing-{symbol.lower()}-position",
        broker_order_id=f"broker-{symbol.lower()}-buy",
        symbol=symbol,
        side="buy",
        status="filled",
        quantity=2,
        venue="kis",
        asset_class="us_equity",
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
                "side": "buy",
                "exchange": exchange,
            },
            observed_at=NOW - timedelta(minutes=10),
        )
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
    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"active-{status}-{symbol.lower()}-position",
        broker_order_id=f"active-{status}-{symbol.lower()}-buy",
        symbol=symbol,
        side="buy",
        status=status,
        quantity=2,
        venue="kis",
        asset_class="us_equity",
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
                "side": "buy",
                "exchange": exchange,
            },
            observed_at=NOW - timedelta(minutes=10),
        )
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
def test_missing_us_position_recovers_exchange_from_active_sell_order_event(
    monkeypatch,
    symbol: str,
    exchange: str,
    status: str,
) -> None:
    settings, engine, session_factory, db = _session()
    order = _add_filled_us_buy_order_event(
        db,
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
        reason="recover active U.S. sell order exchange through account inquiry only",
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


def test_missing_us_position_with_ambiguous_buy_and_sell_exchanges_fails_closed() -> None:
    settings, engine, session_factory, db = _session()
    _add_filled_us_buy_order_event(
        db,
        symbol="AAPL",
        exchange="NASD",
        suffix="ambiguous-buy",
        side="buy",
        status="filled",
    )
    _add_filled_us_buy_order_event(
        db,
        symbol="AAPL",
        exchange="NYSE",
        suffix="ambiguous-sell",
        side="sell",
        status="open",
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

    assert dry_run["items"][0]["exchange"] is None
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
        ExecutionPosition(
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
        ExecutionPosition(
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
        symbol="AAPL",
        exchange="NASD",
        suffix="ambiguous-nasd",
    )
    _add_filled_us_buy_order_event(
        db,
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

    assert dry_run["items"][0]["exchange"] is None
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
            ExecutionPosition(
                mode="paper",
                symbol="003550",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="test",
                last_reconciled_at=old,
            ),
            ExecutionPosition(
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
            ExecutionPosition(
                mode="paper",
                symbol="003550",
                quantity=5,
                average_price=Decimal("200000"),
                updated_from="test",
                last_reconciled_at=old,
            ),
            ExecutionPosition(
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
            ExecutionPosition(
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
        assert broker_session.last_error == "authentication rejected"
        assert len(auth_events) == 1
        assert auth_events[0].action == "authenticate"
        assert auth_events[0].outcome == "rejected"
        assert auth_events[0].source == "position_truth_remediation"
    engine.dispose()


def test_live_position_remediation_requires_database_url_argument(tmp_path) -> None:
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
            "KAI_EXEC_DATABASE_URL": f"sqlite:///{tmp_path / 'must-not-open.db'}",
        },
    )

    assert result.returncode != 0
    assert "requires --database-url explicitly" in result.stderr
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
            ExecutionPosition(
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
