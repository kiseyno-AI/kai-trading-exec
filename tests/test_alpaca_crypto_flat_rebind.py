from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerTokenGrant,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services import position_truth_remediation as remediation_service
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
)
from app.services.reconciliation import (
    sync_execution_positions_from_broker_snapshot,
)


NOW = datetime(2026, 10, 3, 18, 0, tzinfo=UTC)


class AlpacaCryptoSnapshotAdapter:
    provider = "alpaca"

    def __init__(self, snapshots: list[BrokerAccountSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.snapshot_requests: list[tuple[list[str] | None, str]] = []
        self.authenticate_calls = 0

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        return BrokerTokenGrant(
            access_token="locked-refetch-token",
            token_type="ApiKey",
            access_token_expires_at=NOW + timedelta(hours=1),
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        exchange: str = "ALPACA",
    ) -> BrokerAccountSnapshot:
        del access_token
        self.snapshot_requests.append((symbols, exchange))
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]


def _session(*, runtime_mode: str = "paper", provider: str = "alpaca"):
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=runtime_mode,
        runtime_broker_provider=provider,
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-scope-secret",
        alpaca_crypto_allowed_symbols="BTC/USD,ETH/USD",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return settings, engine, session_factory, session_factory()


def _snapshot(
    *,
    account_id: str | None = "alpaca-paper-account",
    observed_at: datetime = NOW,
    holdings: list[BrokerAccountHolding] | None = None,
    open_orders: list[BrokerAccountOpenOrder] | None = None,
    holdings_complete: bool = True,
    open_orders_complete: bool = True,
    source: str = "alpaca:paper",
    payload_updates: dict[str, object] | None = None,
) -> BrokerAccountSnapshot:
    payload: dict[str, object] = {
        "provider": "alpaca",
        "mode": "paper",
        "asset_class": "crypto",
        "holdings_coverage": "complete" if holdings_complete else "incomplete",
        "open_orders_coverage": (
            "complete" if open_orders_complete else "incomplete"
        ),
    }
    payload.update(payload_updates or {})
    return BrokerAccountSnapshot(
        broker_account_id=account_id,
        observed_at=observed_at,
        source=source,
        holdings=holdings or [],
        open_orders=open_orders or [],
        holdings_complete=holdings_complete,
        open_orders_complete=open_orders_complete,
        payload=payload,
    )


def _legacy_flat_position(
    *,
    symbol: str,
    average_price: str | None,
) -> ExecutionPosition:
    return ExecutionPosition(
        mode="paper",
        symbol=symbol,
        quantity=0,
        base_quantity=Decimal("0"),
        average_price=(
            Decimal(average_price) if average_price is not None else None
        ),
        updated_from="reconciliation",
        last_reconciled_at=NOW - timedelta(days=21),
        raw_payload={
            "provider": "alpaca",
            "mode": "paper",
            "legacy_account_snapshot": True,
        },
    )


def _patch_auth_and_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        remediation_service,
        "ensure_authenticated_broker_session",
        lambda **kwargs: type(
            "SessionState",
            (),
            {"access_token": "initial-read-token"},
        )(),
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)


def _dry_run(
    *,
    settings: Settings,
    db,
    session_factory,
    adapter: AlpacaCryptoSnapshotAdapter,
) -> dict[str, object]:
    return remediation_service.analyze_alpaca_crypto_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        now=NOW,
    )


def _approvals(payload: dict[str, object]) -> dict[str, str]:
    return {
        str(item["position_id"]): str(item["evidence_fingerprint"])
        for item in payload["items"]
    }


def test_alpaca_crypto_flat_rebind_binds_btc_and_eth_without_creating_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    btc = _legacy_flat_position(symbol="BTC/USD", average_price="63063.8420")
    eth = _legacy_flat_position(symbol="ETH/USD", average_price=None)
    db.add_all([btc, eth])
    db.commit()
    row_count_before = db.scalar(select(func.count()).select_from(ExecutionPosition))
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])
    _patch_auth_and_clock(monkeypatch)

    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    result = remediation_service.apply_alpaca_crypto_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        approved_evidence=_approvals(dry_run),
        actor="codex",
        reason="bind only current authenticated flat crypto truth",
        now=NOW,
    )
    rebound_positions = db.scalars(
        select(ExecutionPosition).order_by(ExecutionPosition.symbol)
    ).all()
    assert all(
        position.raw_payload["current_flat_truth_bound"] is True
        for position in rebound_positions
    )
    assert all(
        position.raw_payload["historical_account_attributed"] is False
        for position in rebound_positions
    )
    sync_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols={"BTC/USD", "ETH/USD"},
        exchange="ALPACA",
        authenticated_access_token="post-rebind-sync-token",
    )

    assert dry_run["summary"] == {
        "candidate_count": 2,
        "eligible_count": 2,
        "manual_review_count": 0,
    }
    assert result["summary"] == {
        "approved_count": 2,
        "bound_count": 2,
        "historical_account_attributed_count": 0,
        "broker_api_mutation_count": 0,
    }
    assert adapter.authenticate_calls == 1
    assert adapter.snapshot_requests == [
        (["BTC/USD", "ETH/USD"], "ALPACA"),
        (["BTC/USD", "ETH/USD"], "ALPACA"),
        (["BTC/USD", "ETH/USD"], "ALPACA"),
        (["BTC/USD", "ETH/USD"], "ALPACA"),
    ]
    assert sync_result["tracked_symbol_count"] == 2
    assert sync_result["created"] == 0
    assert db.scalar(select(func.count()).select_from(ExecutionPosition)) == row_count_before
    positions = db.scalars(
        select(ExecutionPosition).order_by(ExecutionPosition.symbol)
    ).all()
    assert [(position.symbol, position.quantity) for position in positions] == [
        ("BTC/USD", 0),
        ("ETH/USD", 0),
    ]
    assert all(position.provider == "alpaca" for position in positions)
    assert all(position.exchange == "ALPACA" for position in positions)
    assert len({position.account_scope for position in positions}) == 1
    assert all(position.base_quantity == 0 for position in positions)
    assert all(position.average_price is None for position in positions)
    assert all(
        remediation_service._serialize_datetime(position.last_reconciled_at)
        == NOW.isoformat()
        for position in positions
    )
    events = db.scalars(
        select(RuntimeRemediationEvent).order_by(RuntimeRemediationEvent.subject_id)
    ).all()
    assert len(events) == 2
    assert all(event.outcome == "current_flat_truth_bound" for event in events)
    assert all(
        event.evidence_summary["broker_api_mutations"] == 0
        for event in events
    )
    assert all(
        event.evidence_summary["historical_account_attributed"] is False
        for event in events
    )
    assert db.scalar(select(func.count()).select_from(BrokerOrder)) == 0
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0
    db.close()
    engine.dispose()


def test_alpaca_crypto_flat_rebind_fingerprint_covers_stale_row_and_absence_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position(
        symbol="BTC/USD",
        average_price="63063.8420",
    )
    db.add(position)
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])
    _patch_auth_and_clock(monkeypatch)

    item = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )["items"][0]

    assert item["legacy_payload_digest"]
    assert item["legacy_average_price"] == "63063.842"
    assert item["last_reconciled_at"] == (
        NOW - timedelta(days=21)
    ).isoformat()
    assert len(item["account_scope"]) == 24
    assert len(item["absence_proof_digest"]) == 64
    assert len(item["evidence_fingerprint"]) == 64
    assert item["holding_absent"] is True
    assert item["open_order_absent"] is True
    db.close()
    engine.dispose()


def test_alpaca_crypto_flat_rebind_requires_explicit_complete_approvals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    db.add_all(
        [
            _legacy_flat_position(symbol="BTC/USD", average_price="1"),
            _legacy_flat_position(symbol="ETH/USD", average_price=None),
        ]
    )
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])
    _patch_auth_and_clock(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    approvals = _approvals(dry_run)
    first_id = next(iter(approvals))

    with pytest.raises(ValueError, match="explicit approved evidence"):
        remediation_service.apply_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            approved_evidence={},
            actor="codex",
            reason="test",
            now=NOW,
        )
    with pytest.raises(PositionIdentityConflictError, match="complete current candidate set"):
        remediation_service.apply_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            approved_evidence={first_id: approvals[first_id]},
            actor="codex",
            reason="test",
            now=NOW,
        )
    with pytest.raises(PositionIdentityConflictError, match="complete current candidate set"):
        remediation_service.apply_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            approved_evidence={**approvals, "unknown-position": "a" * 64},
            actor="codex",
            reason="test",
            now=NOW,
        )

    assert all(
        position.provider is None
        for position in db.scalars(select(ExecutionPosition)).all()
    )
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 0
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("base_quantity", "holding", "open_order", "expected_reason"),
    [
        (Decimal("0.01"), None, None, "legacy_position_not_flat"),
        (
            Decimal("0"),
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                average_price=Decimal("63000"),
                payload={"base_quantity": "0.01"},
            ),
            None,
            "broker_holding_present",
        ),
        (
            Decimal("0"),
            None,
            BrokerAccountOpenOrder(
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                remaining_quantity=0,
                broker_order_id="crypto-order-1",
                payload={"base_quantity": "0.01"},
            ),
            "broker_open_order_present",
        ),
    ],
)
def test_alpaca_crypto_flat_rebind_rejects_nonflat_or_broker_activity(
    monkeypatch: pytest.MonkeyPatch,
    base_quantity: Decimal,
    holding: BrokerAccountHolding | None,
    open_order: BrokerAccountOpenOrder | None,
    expected_reason: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    position.base_quantity = base_quantity
    db.add(position)
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter(
        [
            _snapshot(
                holdings=[holding] if holding is not None else [],
                open_orders=[open_order] if open_order is not None else [],
            )
        ]
    )
    _patch_auth_and_clock(monkeypatch)

    result = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    assert result["summary"]["eligible_count"] == 0
    assert result["items"][0]["reason"] == expected_reason
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, position.id).provider is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("base_quantity", Decimal("NaN"), "legacy_position_not_flat"),
        ("base_quantity", Decimal("Infinity"), "legacy_position_not_flat"),
        ("quantity", Decimal("NaN"), "legacy_position_not_flat"),
        ("quantity", Decimal("Infinity"), "legacy_position_not_flat"),
        ("average_price", Decimal("NaN"), "legacy_average_price_invalid"),
        (
            "average_price",
            Decimal("Infinity"),
            "legacy_average_price_invalid",
        ),
    ],
)
def test_alpaca_crypto_flat_rebind_rejects_nonfinite_local_values(
    field: str,
    value: Decimal,
    expected_reason: str,
) -> None:
    settings = Settings(
        runtime_mode="paper",
        runtime_broker_provider="alpaca",
        alpaca_api_secret="paper-scope-secret",
        alpaca_crypto_allowed_symbols="BTC/USD,ETH/USD",
    )
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="alpaca-paper-account",
    )
    position = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    position.id = "legacy-nonfinite-position"
    setattr(position, field, value)

    item = remediation_service._alpaca_crypto_flat_rebind_item(
        settings=settings,
        position=position,
        identity=identity,
        configured_symbols={"BTC/USD", "ETH/USD"},
        holding_present=False,
        open_order_present=False,
        scoped_route_exists=False,
        absence_proof_digest="a" * 64,
    )

    assert item["eligible"] is False
    assert item["reason"] == expected_reason
    assert item["evidence_fingerprint"] is None


def test_alpaca_crypto_flat_rebind_rejects_target_scope_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    legacy = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="alpaca-paper-account",
    )
    scoped = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="BTC/USD",
        quantity=0,
        base_quantity=Decimal("0"),
        updated_from="reconciliation",
    )
    db.add_all([legacy, scoped])
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])
    _patch_auth_and_clock(monkeypatch)

    result = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    assert result["items"][0]["reason"] == "scoped_route_symbol_collision"
    assert result["items"][0]["evidence_fingerprint"] is None
    assert db.get(ExecutionPosition, legacy.id).provider is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize("drift", ["account", "holding", "open_order"])
def test_alpaca_crypto_flat_rebind_revalidates_broker_truth_after_row_lock(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    db.add(position)
    db.commit()
    initial = _snapshot()
    if drift == "account":
        locked = _snapshot(account_id="different-alpaca-account")
    elif drift == "holding":
        locked = _snapshot(
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    average_price=Decimal("63000"),
                    payload={"base_quantity": "0.01"},
                )
            ]
        )
    else:
        locked = _snapshot(
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol="BTC/USD",
                    side="buy",
                    quantity=0,
                    remaining_quantity=0,
                    broker_order_id="appeared-after-lock",
                )
            ]
        )
    adapter = AlpacaCryptoSnapshotAdapter([initial, initial, locked])
    _patch_auth_and_clock(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    with pytest.raises(PositionIdentityConflictError, match="after row locking"):
        remediation_service.apply_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            approved_evidence=_approvals(dry_run),
            actor="codex",
            reason="test lock-after-refetch drift rejection",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.average_price == Decimal("1.0000")
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 0
    assert adapter.authenticate_calls == 1
    db.close()
    engine.dispose()


def test_alpaca_crypto_flat_rebind_rejects_local_fingerprint_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    db.add(position)
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])
    _patch_auth_and_clock(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    position.raw_payload = {**position.raw_payload, "drift": True}
    db.commit()

    with pytest.raises(PositionIdentityConflictError, match="no longer matches"):
        remediation_service.apply_alpaca_crypto_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            approved_evidence=_approvals(dry_run),
            actor="codex",
            reason="test",
            now=NOW,
        )

    assert db.get(ExecutionPosition, position.id).provider is None
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 0
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("runtime_mode", "provider", "expected_error"),
    [
        ("live", "alpaca", "supports only paper mode"),
        ("paper", "kis", "requires runtime broker provider alpaca"),
    ],
)
def test_alpaca_crypto_flat_rebind_rejects_unsupported_runtime_routes(
    runtime_mode: str,
    provider: str,
    expected_error: str,
) -> None:
    settings, engine, session_factory, db = _session(
        runtime_mode=runtime_mode,
        provider=provider,
    )
    adapter = AlpacaCryptoSnapshotAdapter([_snapshot()])

    with pytest.raises(PositionIdentityConflictError, match=expected_error):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    assert adapter.snapshot_requests == []
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "snapshot",
    [
        _snapshot(observed_at=NOW - timedelta(seconds=301)),
        _snapshot(observed_at=NOW + timedelta(seconds=31)),
        _snapshot(observed_at=NOW.replace(tzinfo=None)),
        _snapshot(holdings_complete=False),
        _snapshot(open_orders_complete=False),
        _snapshot(account_id=None),
        _snapshot(payload_updates={"asset_class": "us_equity"}),
    ],
)
def test_alpaca_crypto_flat_rebind_requires_fresh_complete_exact_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    snapshot: BrokerAccountSnapshot,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position(symbol="BTC/USD", average_price="1")
    db.add(position)
    db.commit()
    adapter = AlpacaCryptoSnapshotAdapter([snapshot])
    _patch_auth_and_clock(monkeypatch)

    with pytest.raises((BrokerAdapterError, PositionIdentityConflictError)):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    assert db.get(ExecutionPosition, position.id).provider is None
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 0
    db.close()
    engine.dispose()
