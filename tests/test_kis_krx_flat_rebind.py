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
from app.services.kis_krx_flat_rebind_contract import (
    KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE,
    KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME,
    KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION,
    kis_krx_flat_absence_proof_digest,
    kis_krx_flat_configured_account_binding_digest,
    kis_krx_flat_evidence_fingerprint,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
    payload_with_position_identity,
    position_identity_from_payload,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot


NOW = datetime(2026, 10, 8, 5, 30, tzinfo=UTC)
TARGET_SYMBOL = "029460"


class KisKrxSnapshotAdapter:
    provider = "kis"

    def __init__(self, snapshots: list[BrokerAccountSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.snapshot_requests: list[tuple[str, list[str] | None, str]] = []
        self.authenticate_calls = 0

    def authenticate(self):
        self.authenticate_calls += 1
        raise AssertionError("flat rebind must use the shared KIS token path")

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        self.snapshot_requests.append((access_token, symbols, exchange))
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]


def _session():
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
        kis_account_number="12345678",
        kis_account_product_code="01",
        execution_position_account_scope="primary-kis-live",
        account_drawdown_recovery_scope_secret="s" * 32,
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return settings, engine, session_factory, session_factory()


def _snapshot(
    *,
    observed_at: datetime = NOW,
    source: str = "kis:account_snapshot",
    holdings: list[BrokerAccountHolding] | None = None,
    open_orders: list[BrokerAccountOpenOrder] | None = None,
    holdings_complete: bool = True,
    open_orders_complete: bool = True,
    exchange: str = "KRX",
    account_number_masked: str = "****5678",
    open_orders_error: object = None,
) -> BrokerAccountSnapshot:
    return BrokerAccountSnapshot(
        observed_at=observed_at,
        source=source,
        holdings=holdings or [],
        open_orders=open_orders or [],
        holdings_complete=holdings_complete,
        open_orders_complete=open_orders_complete,
        diagnostics={
            "requested_exchange": exchange,
            "holdings_pagination": {"complete": holdings_complete},
            "open_orders_pagination": {"complete": open_orders_complete},
            "holdings_parse": {
                "raw_row_count": len(holdings or []),
                "parsed_row_count": len(holdings or []),
                "omitted_row_count": 0,
                "invalid_row_count": 0,
                "duplicate_identity_count": 0,
            },
            "open_orders_parse": {
                "raw_row_count": len(open_orders or []),
                "parsed_row_count": len(open_orders or []),
                "omitted_row_count": 0,
                "invalid_row_count": 0,
                "duplicate_identity_count": 0,
            },
        },
        payload={
            "exchange": exchange,
            "account_number_masked": account_number_masked,
            "holdings_coverage": (
                "complete" if holdings_complete else "incomplete"
            ),
            "open_orders_coverage": (
                "complete" if open_orders_complete else "incomplete"
            ),
            "open_orders_error": open_orders_error,
        },
    )


def _legacy_flat_position() -> ExecutionPosition:
    return ExecutionPosition(
        mode="live",
        symbol=TARGET_SYMBOL,
        quantity=0,
        base_quantity=None,
        average_price=Decimal("35800"),
        updated_from="reconciliation",
        last_reconciled_at=NOW - timedelta(days=26),
        raw_payload={
            "balance": {"legacy": True},
            "open_orders": {"legacy": True},
            "open_orders_error": {"code": "KIER2570"},
        },
    )


def _patch_shared_auth(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def ensure_authenticated_broker_session(**kwargs):
        calls.append(str(kwargs["reason"]))
        return type("SessionState", (), {"access_token": "shared-kis-token"})()

    monkeypatch.setattr(
        remediation_service,
        "ensure_authenticated_broker_session",
        ensure_authenticated_broker_session,
    )
    monkeypatch.setattr(remediation_service, "_real_utc_now", lambda: NOW)
    return calls


def _dry_run(*, settings, db, session_factory, adapter):
    return remediation_service.analyze_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        now=NOW,
    )


def test_kis_krx_flat_rebind_dry_run_proves_one_exact_absent_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    auth_calls = _patch_shared_auth(monkeypatch)

    result = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    assert result["summary"] == {
        "candidate_count": 1,
        "eligible_count": 1,
        "manual_review_count": 0,
    }
    item = result["items"][0]
    assert item["position_id"] == position.id
    assert item["symbol"] == TARGET_SYMBOL
    assert item["provider"] == "kis"
    assert item["exchange"] == "KRX"
    assert item["holdings_complete"] is True
    assert item["open_orders_complete"] is True
    assert item["holding_absent"] is True
    assert item["open_order_absent"] is True
    assert item["historical_account_attributed"] is False
    assert item["current_flat_truth_bound"] is True
    assert len(item["account_scope"]) == 24
    assert len(item["prior_position_digest"]) == 64
    assert len(item["absence_proof_digest"]) == 64
    assert len(item["configured_account_binding_digest"]) == 64
    assert len(item["evidence_fingerprint"]) == 64
    assert (
        item["safety_policy_version"]
        == KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    assert item["absence_proof_digest"] == kis_krx_flat_absence_proof_digest(
        runtime_mode="live",
        identity=identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        configured_account_binding_digest=item[
            "configured_account_binding_digest"
        ],
    )
    assert item["evidence_fingerprint"] == kis_krx_flat_evidence_fingerprint(
        runtime_mode="live",
        position_id=position.id,
        prior_position_digest=item["prior_position_digest"],
        identity=identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        absence_proof_digest=item["absence_proof_digest"],
        configured_account_binding_digest=item[
            "configured_account_binding_digest"
        ],
    )
    assert result["broker_snapshot_source"] == "kis:account_snapshot"
    assert result["broker_snapshot_observed_at"] == NOW.isoformat()
    assert auth_calls == ["refresh execution positions from broker snapshot"]
    assert adapter.authenticate_calls == 0
    assert adapter.snapshot_requests == [
        ("shared-kis-token", [TARGET_SYMBOL], "KRX")
    ]
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.average_price == Decimal("35800.0000")
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 0
    assert db.scalar(select(func.count()).select_from(BrokerOrder)) == 0
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_fingerprint_is_bound_to_exact_keyed_kis_account() -> None:
    settings, engine, _session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "account_drawdown_recovery_scope_secret": None,
            "execution_position_account_scope": "primary-kis-live",
            "kis_app_secret": "k" * 64,
        }
    )
    switched_settings = settings.model_copy(
        update={
            "kis_account_number": "87654321",
            "kis_account_product_code": "02",
        }
    )
    first_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    switched_identity = build_position_identity(
        settings=switched_settings,
        provider="kis",
        exchange="KRX",
    )
    first_account_binding = kis_krx_flat_configured_account_binding_digest(
        settings=settings
    )
    switched_account_binding = kis_krx_flat_configured_account_binding_digest(
        settings=switched_settings
    )
    first_digest = kis_krx_flat_absence_proof_digest(
        runtime_mode="live",
        identity=first_identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        configured_account_binding_digest=first_account_binding,
    )
    switched_digest = kis_krx_flat_absence_proof_digest(
        runtime_mode="live",
        identity=switched_identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        configured_account_binding_digest=switched_account_binding,
    )

    assert first_identity.account_scope == switched_identity.account_scope
    assert first_account_binding != switched_account_binding
    assert first_digest != switched_digest
    assert kis_krx_flat_evidence_fingerprint(
        runtime_mode="live",
        position_id="position-1",
        prior_position_digest="a" * 64,
        identity=first_identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        absence_proof_digest=first_digest,
        configured_account_binding_digest=first_account_binding,
    ) != kis_krx_flat_evidence_fingerprint(
        runtime_mode="live",
        position_id="position-1",
        prior_position_digest="a" * 64,
        identity=switched_identity,
        symbol=TARGET_SYMBOL,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        absence_proof_digest=switched_digest,
        configured_account_binding_digest=switched_account_binding,
    )
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_accepts_live_label_route_with_app_secret_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(
        update={
            "account_drawdown_recovery_scope_secret": None,
            "execution_position_account_scope": "primary-kis-live",
            "kis_app_secret": "k" * 64,
        }
    )
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    _patch_shared_auth(monkeypatch)

    result = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    expected_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    assert result["items"][0]["account_scope"] == expected_identity.account_scope
    assert result["items"][0]["eligible"] is True
    assert result["items"][0]["configured_account_binding_digest"] == (
        kis_krx_flat_configured_account_binding_digest(settings=settings)
    )
    approval = {
        position.id: result["items"][0]["evidence_fingerprint"],
    }
    applied = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence=approval,
        actor="codex",
        reason="live-shaped compatibility proof",
        now=NOW,
    )
    rebound = db.get(ExecutionPosition, position.id)
    assert applied["summary"]["bound_count"] == 1
    assert rebound is not None
    assert rebound.account_scope == expected_identity.account_scope
    replay = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence=approval,
        actor="codex",
        reason="live-shaped idempotent proof",
        now=NOW + timedelta(seconds=1),
    )
    assert replay["summary"]["idempotent_count"] == 1
    snapshot_request_count = len(adapter.snapshot_requests)
    switched_settings = settings.model_copy(
        update={
            "kis_account_number": "87654321",
            "kis_account_product_code": "02",
        }
    )
    with pytest.raises(
        PositionIdentityConflictError,
        match="no longer matches canonical flat truth",
    ):
        remediation_service.apply_kis_krx_legacy_flat_position_rebind(
            settings=switched_settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            symbol=TARGET_SYMBOL,
            approved_evidence=approval,
            actor="codex",
            reason="account switch must invalidate approval",
            now=NOW + timedelta(seconds=2),
        )
    assert len(adapter.snapshot_requests) == snapshot_request_count
    db.close()
    engine.dispose()


@pytest.mark.parametrize("symbol", ["٠٢٩٤٦٠", "０２９４６０"])
def test_kis_krx_flat_rebind_rejects_non_ascii_digit_symbol(symbol: str) -> None:
    settings, engine, session_factory, db = _session()
    adapter = KisKrxSnapshotAdapter([_snapshot()])

    with pytest.raises(ValueError, match="canonical six-digit symbol"):
        remediation_service.analyze_kis_krx_legacy_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            symbol=symbol,
            now=NOW,
        )

    assert adapter.snapshot_requests == []
    db.close()
    engine.dispose()


@pytest.mark.parametrize("legacy_mode", ["LIVE", " LIVE "])
def test_kis_krx_flat_rebind_rejects_noncanonical_legacy_mode_before_broker(
    monkeypatch: pytest.MonkeyPatch,
    legacy_mode: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    position.mode = legacy_mode
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    auth_calls = _patch_shared_auth(monkeypatch)

    with pytest.raises(
        PositionIdentityConflictError,
        match="requires one exact unscoped position",
    ):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    assert auth_calls == []
    assert adapter.snapshot_requests == []
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.mode == legacy_mode
    assert persisted.provider is None
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_apply_updates_same_row_and_normal_sync_reuses_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    position_id = position.id
    row_count_before = db.scalar(select(func.count()).select_from(ExecutionPosition))
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    auth_calls = _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    item = dry_run["items"][0]

    result = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence={position_id: item["evidence_fingerprint"]},
        actor="codex",
        reason="bind current complete KIS KRX flat truth",
        now=NOW,
    )

    assert result["summary"] == {
        "approved_count": 1,
        "bound_count": 1,
        "idempotent_count": 0,
        "historical_account_attributed_count": 0,
        "broker_api_mutation_count": 0,
    }
    assert db.scalar(select(func.count()).select_from(ExecutionPosition)) == row_count_before
    rebound = db.get(ExecutionPosition, position_id)
    assert rebound is not None
    assert rebound.id == position_id
    assert rebound.provider == "kis"
    assert rebound.exchange == "KRX"
    assert len(rebound.account_scope or "") == 24
    assert rebound.quantity == 0
    assert rebound.base_quantity == 0
    assert rebound.average_price is None
    assert rebound.updated_from == "reconciliation"
    assert remediation_service._serialize_datetime(rebound.last_reconciled_at) == NOW.isoformat()
    identity = position_identity_from_payload(rebound.raw_payload)
    assert identity is not None
    assert identity.provider == "kis"
    assert identity.account_scope == rebound.account_scope
    assert identity.exchange == "KRX"
    assert rebound.raw_payload == {
        "provider": "kis",
        "mode": "live",
        "asset_class": "equity",
        "exchange": "KRX",
        "snapshot_source": "kis:account_snapshot",
        "snapshot_observed_at": NOW.isoformat(),
        "holdings_coverage": "complete",
        "open_orders_coverage": "complete",
        "holding_absent": True,
        "open_order_absent": True,
        "absence_proof_digest": item["absence_proof_digest"],
        "configured_account_binding_digest": item[
            "configured_account_binding_digest"
        ],
        "historical_account_attributed": False,
        "current_flat_truth_bound": True,
        "safety_policy_version": item["safety_policy_version"],
        "_position_identity": {
            "version": "execution-position-route-v2",
            "provider": "kis",
            "account_scope": rebound.account_scope,
            "exchange": "KRX",
        },
    }
    assert set(rebound.raw_payload) == KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    assert event.candidate_type == KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE
    assert event.candidate_key == position_id
    assert event.subject_id == position_id
    assert event.outcome == KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME
    assert event.actor == "codex"
    assert event.reason == "bind current complete KIS KRX flat truth"
    assert event.evidence_summary == {
        "symbol": TARGET_SYMBOL,
        "provider": "kis",
        "account_scope": rebound.account_scope,
        "exchange": "KRX",
        "asset_class": "equity",
        "broker_snapshot_source": "kis:account_snapshot",
        "broker_snapshot_observed_at": NOW.isoformat(),
        "holdings_complete": True,
        "open_orders_complete": True,
        "holding_absent": True,
        "open_order_absent": True,
        "absence_proof_digest": item["absence_proof_digest"],
        "configured_account_binding_digest": item[
            "configured_account_binding_digest"
        ],
        "prior_position_digest": item["prior_position_digest"],
        "evidence_fingerprint": item["evidence_fingerprint"],
        "safety_policy_version": item["safety_policy_version"],
        "historical_account_attributed": False,
        "current_flat_truth_bound": True,
        "broker_api_mutations": 0,
        "orders_submitted": 0,
        "orders_amended": 0,
        "orders_canceled": 0,
        "fills_created": 0,
        "fills_deleted": 0,
        "pnl_records_mutated": 0,
        "strategy_records_mutated": 0,
    }
    assert (
        set(event.evidence_summary)
        == KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS
    )
    assert db.scalar(select(func.count()).select_from(BrokerOrder)) == 0
    assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 0

    sync_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=adapter,
        symbols={TARGET_SYMBOL},
        exchange="KRX",
        authenticated_access_token="post-rebind-shared-token",
    )

    assert sync_result["tracked_symbol_count"] == 1
    assert sync_result["created"] == 0
    assert db.scalar(select(func.count()).select_from(ExecutionPosition)) == row_count_before
    assert db.get(ExecutionPosition, position_id) is not None
    assert auth_calls == [
        "refresh execution positions from broker snapshot",
        "refresh execution positions from broker snapshot",
        "refresh execution positions from broker snapshot",
    ]
    assert adapter.authenticate_calls == 0
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_rejects_unproven_snapshot_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    snapshot = _snapshot()
    snapshot.diagnostics.clear()
    adapter = KisKrxSnapshotAdapter([snapshot])
    _patch_shared_auth(monkeypatch)

    with pytest.raises(
        remediation_service.PositionIdentityConflictError,
        match="unavailable or incomplete",
    ):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_fingerprint_ignores_unrelated_account_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    unrelated_holding = BrokerAccountHolding(
        symbol="005930",
        quantity=3,
        average_price=Decimal("70000"),
    )
    adapter = KisKrxSnapshotAdapter(
        [
            _snapshot(),
            _snapshot(holdings=[unrelated_holding]),
            _snapshot(holdings=[unrelated_holding]),
        ]
    )
    _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    item = dry_run["items"][0]

    result = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence={position.id: item["evidence_fingerprint"]},
        actor="codex",
        reason="target symbol remains absent from complete account truth",
        now=NOW,
    )

    assert result["summary"]["bound_count"] == 1
    assert db.get(ExecutionPosition, position.id).provider == "kis"
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_exact_apply_rerun_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    auth_calls = _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    approval = {
        position.id: dry_run["items"][0]["evidence_fingerprint"]
    }
    first = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence=approval,
        actor="codex",
        reason="bind once",
        now=NOW,
    )
    request_count = len(adapter.snapshot_requests)
    auth_count = len(auth_calls)

    second = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence=approval,
        actor="codex",
        reason="safe replay",
        now=NOW + timedelta(seconds=1),
    )

    assert first["summary"]["bound_count"] == 1
    assert second["summary"] == {
        "approved_count": 1,
        "bound_count": 0,
        "idempotent_count": 1,
        "historical_account_attributed_count": 0,
        "broker_api_mutation_count": 0,
    }
    assert second["items"][0]["outcome"] == "already_current_flat_truth_bound"
    assert len(adapter.snapshot_requests) == request_count
    assert len(auth_calls) == auth_count
    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 1
    assert db.scalar(select(func.count()).select_from(ExecutionPosition)) == 1
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_audit_time_is_not_before_locked_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter(
        [
            _snapshot(observed_at=NOW),
            _snapshot(observed_at=NOW + timedelta(seconds=1)),
            _snapshot(observed_at=NOW + timedelta(seconds=2)),
        ]
    )
    _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    monkeypatch.setattr(
        remediation_service,
        "_real_utc_now",
        lambda: NOW + timedelta(seconds=3),
    )

    result = remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence={
            position.id: dry_run["items"][0]["evidence_fingerprint"]
        },
        actor="codex",
        reason="final locked proof time",
        now=NOW,
    )

    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    assert remediation_service._serialize_datetime(event.occurred_at) == (
        NOW + timedelta(seconds=3)
    ).isoformat()
    assert result["generated_at"] == (
        NOW + timedelta(seconds=3)
    ).isoformat()
    assert result["broker_snapshot_observed_at"] == (
        NOW + timedelta(seconds=2)
    ).isoformat()
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "drift",
    [
        "raw_digest",
        "raw_extra_field",
        "event_bool_zero",
        "event_bad_digest",
        "event_valid_prior_digest",
        "event_account_binding_digest",
        "event_extra_field",
        "event_time_before_snapshot",
        "row_last_reconciled_at",
        "row_base_quantity_none",
    ],
)
def test_kis_krx_flat_rebind_idempotent_replay_rejects_proof_drift(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    approval = {position.id: dry_run["items"][0]["evidence_fingerprint"]}
    remediation_service.apply_kis_krx_legacy_flat_position_rebind(
        settings=settings,
        db=db,
        auth_session_factory=session_factory,
        adapter=adapter,
        symbol=TARGET_SYMBOL,
        approved_evidence=approval,
        actor="codex",
        reason="bind once",
        now=NOW,
    )
    rebound = db.get(ExecutionPosition, position.id)
    event = db.scalar(select(RuntimeRemediationEvent))
    assert rebound is not None and event is not None
    if drift == "raw_digest":
        rebound.raw_payload = {
            **rebound.raw_payload,
            "absence_proof_digest": "b" * 64,
        }
    elif drift == "raw_extra_field":
        rebound.raw_payload = {**rebound.raw_payload, "unexpected": True}
    elif drift == "event_bool_zero":
        event.evidence_summary = {
            **event.evidence_summary,
            "orders_submitted": False,
        }
    elif drift == "event_bad_digest":
        event.evidence_summary = {
            **event.evidence_summary,
            "absence_proof_digest": "G" * 64,
        }
    elif drift == "event_valid_prior_digest":
        event.evidence_summary = {
            **event.evidence_summary,
            "prior_position_digest": "b" * 64,
        }
    elif drift == "event_account_binding_digest":
        event.evidence_summary = {
            **event.evidence_summary,
            "configured_account_binding_digest": "b" * 64,
        }
    elif drift == "event_extra_field":
        event.evidence_summary = {**event.evidence_summary, "unexpected": True}
    elif drift == "event_time_before_snapshot":
        event.occurred_at = NOW - timedelta(seconds=1)
    elif drift == "row_last_reconciled_at":
        rebound.last_reconciled_at = NOW - timedelta(seconds=1)
    else:
        rebound.base_quantity = None
    db.commit()

    with pytest.raises(
        remediation_service.PositionIdentityConflictError,
        match="no longer matches canonical flat truth",
    ):
        remediation_service.apply_kis_krx_legacy_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            symbol=TARGET_SYMBOL,
            approved_evidence=approval,
            actor="codex",
            reason="unsafe replay",
            now=NOW + timedelta(seconds=1),
        )

    assert db.scalar(select(func.count()).select_from(RuntimeRemediationEvent)) == 1
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "case",
    [
        "stale",
        "future",
        "naive",
        "wrong_source",
        "wrong_exchange",
        "wrong_account",
        "holdings_incomplete",
        "open_orders_incomplete",
        "kier2570",
        "pagination_incomplete",
        "parse_incomplete",
    ],
)
def test_kis_krx_flat_rebind_rejects_incomplete_or_wrong_snapshot_proof(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    snapshot = _snapshot()
    if case == "stale":
        snapshot = _snapshot(observed_at=NOW - timedelta(seconds=301))
    elif case == "future":
        snapshot = _snapshot(observed_at=NOW + timedelta(seconds=31))
    elif case == "naive":
        snapshot = _snapshot(observed_at=NOW.replace(tzinfo=None))
    elif case == "wrong_source":
        snapshot = _snapshot(source="compat:account_snapshot")
    elif case == "wrong_exchange":
        snapshot = _snapshot(exchange="NASD")
    elif case == "wrong_account":
        snapshot = _snapshot(account_number_masked="****9999")
    elif case == "holdings_incomplete":
        snapshot = _snapshot(holdings_complete=False)
    elif case == "open_orders_incomplete":
        snapshot = _snapshot(open_orders_complete=False)
    elif case == "kier2570":
        snapshot = _snapshot(
            open_orders_error={
                "code": "KIER2570",
                "message": "daily inquiry rejected",
                "degraded": True,
            }
        )
    elif case == "pagination_incomplete":
        snapshot.diagnostics["open_orders_pagination"] = {"complete": False}
    elif case == "parse_incomplete":
        snapshot.diagnostics["holdings_parse"] = {
            **snapshot.diagnostics["holdings_parse"],
            "invalid_row_count": 1,
        }
    adapter = KisKrxSnapshotAdapter([snapshot])
    _patch_shared_auth(monkeypatch)

    with pytest.raises((BrokerAdapterError, PositionIdentityConflictError)):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("settings_update", "adapter_provider", "expected_error"),
    [
        ({"runtime_mode": "paper"}, "kis", "requires live mode"),
        (
            {"runtime_broker_provider": "alpaca"},
            "kis",
            "requires runtime broker provider kis",
        ),
        ({}, "alpaca", "requires a KIS adapter"),
        ({"kis_account_number": None}, "kis", "configured KIS account identity"),
        (
            {"kis_account_product_code": ""},
            "kis",
            "configured KIS account identity",
        ),
        (
            {
                "execution_position_account_scope": None,
                "account_drawdown_recovery_scope_secret": "short",
            },
            "kis",
            "stable canonical account scope",
        ),
        (
            {
                "account_drawdown_recovery_scope_secret": None,
                "execution_position_account_scope": "label-only",
            },
            "kis",
            "exact keyed configured account proof",
        ),
    ],
)
def test_kis_krx_flat_rebind_rejects_unsupported_or_ambiguous_route(
    settings_update: dict[str, object],
    adapter_provider: str,
    expected_error: str,
) -> None:
    settings, engine, session_factory, db = _session()
    settings = settings.model_copy(update=settings_update)
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    adapter.provider = adapter_provider

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
    ("local_change", "snapshot", "expected_reason"),
    [
        ("quantity", _snapshot(), "legacy_position_not_flat"),
        ("base_quantity", _snapshot(), "legacy_position_not_flat"),
        ("payload", _snapshot(), "legacy_payload_invalid"),
        ("identity", _snapshot(), "stored_route_identity_present"),
        ("symbol", _snapshot(), "legacy_symbol_not_canonical"),
        (
            "none",
            _snapshot(
                holdings=[
                    BrokerAccountHolding(
                        symbol=TARGET_SYMBOL,
                        quantity=1,
                        average_price=Decimal("35000"),
                    )
                ]
            ),
            "broker_holding_present",
        ),
        (
            "none",
            _snapshot(
                open_orders=[
                    BrokerAccountOpenOrder(
                        symbol=TARGET_SYMBOL,
                        side="buy",
                        quantity=1,
                        broker_order_id="open-target-order",
                    )
                ]
            ),
            "broker_open_order_present",
        ),
    ],
)
def test_kis_krx_flat_rebind_dry_run_marks_unsafe_candidate_ineligible(
    monkeypatch: pytest.MonkeyPatch,
    local_change: str,
    snapshot: BrokerAccountSnapshot,
    expected_reason: str,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    if local_change == "quantity":
        position.quantity = 1
    elif local_change == "base_quantity":
        position.base_quantity = Decimal("0.1")
    elif local_change == "payload":
        position.raw_payload = ["legacy", "invalid"]
    elif local_change == "identity":
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
        position.raw_payload = payload_with_position_identity(
            position.raw_payload,
            identity=identity,
        )
    elif local_change == "symbol":
        position.symbol = f" {TARGET_SYMBOL} "
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([snapshot])
    _patch_shared_auth(monkeypatch)

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


def test_kis_krx_flat_rebind_rejects_same_account_symbol_collision_across_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    legacy = _legacy_flat_position()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    collision = ExecutionPosition(
        mode="live",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange="NXT",
        symbol=TARGET_SYMBOL,
        quantity=0,
        base_quantity=Decimal("0"),
        average_price=None,
        updated_from="reconciliation",
        raw_payload=payload_with_position_identity({}, identity=identity),
    )
    db.add_all([legacy, collision])
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    _patch_shared_auth(monkeypatch)

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


@pytest.mark.parametrize(
    "locked_snapshot",
    [
        _snapshot(
            holdings=[
                BrokerAccountHolding(
                    symbol=TARGET_SYMBOL,
                    quantity=1,
                    average_price=Decimal("35000"),
                )
            ]
        ),
        _snapshot(
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol=TARGET_SYMBOL,
                    side="sell",
                    quantity=1,
                    broker_order_id="appeared-after-lock",
                )
            ]
        ),
        _snapshot(open_orders_complete=False),
        _snapshot(account_number_masked="****9999"),
    ],
)
def test_kis_krx_flat_rebind_rolls_back_when_locked_broker_truth_drifts(
    monkeypatch: pytest.MonkeyPatch,
    locked_snapshot: BrokerAccountSnapshot,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot(), _snapshot(), locked_snapshot])
    _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )

    with pytest.raises((BrokerAdapterError, PositionIdentityConflictError)):
        remediation_service.apply_kis_krx_legacy_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            symbol=TARGET_SYMBOL,
            approved_evidence={
                position.id: dry_run["items"][0]["evidence_fingerprint"]
            },
            actor="codex",
            reason="locked refetch must remain exact",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.provider is None
    assert persisted.average_price == Decimal("35800.0000")
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


def test_kis_krx_flat_rebind_rejects_local_fingerprint_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    position = _legacy_flat_position()
    db.add(position)
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    _patch_shared_auth(monkeypatch)
    dry_run = _dry_run(
        settings=settings,
        db=db,
        session_factory=session_factory,
        adapter=adapter,
    )
    position.raw_payload = {**position.raw_payload, "drift": True}
    db.commit()

    with pytest.raises(PositionIdentityConflictError, match="no longer matches"):
        remediation_service.apply_kis_krx_legacy_flat_position_rebind(
            settings=settings,
            db=db,
            auth_session_factory=session_factory,
            adapter=adapter,
            symbol=TARGET_SYMBOL,
            approved_evidence={
                position.id: dry_run["items"][0]["evidence_fingerprint"]
            },
            actor="codex",
            reason="local drift must block",
            now=NOW,
        )

    assert db.get(ExecutionPosition, position.id).provider is None
    assert db.scalars(select(RuntimeRemediationEvent)).all() == []
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "value", "expected_reason"),
    [
        ("quantity", Decimal("NaN"), "legacy_position_not_flat"),
        ("quantity", Decimal("Infinity"), "legacy_position_not_flat"),
        ("base_quantity", Decimal("NaN"), "legacy_position_not_flat"),
        ("base_quantity", Decimal("Infinity"), "legacy_position_not_flat"),
        ("average_price", Decimal("NaN"), "legacy_average_price_invalid"),
        ("average_price", Decimal("Infinity"), "legacy_average_price_invalid"),
    ],
)
def test_kis_krx_flat_rebind_rejects_nonfinite_local_values(
    field: str,
    value: Decimal,
    expected_reason: str,
) -> None:
    settings, engine, _session_factory, db = _session()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = _legacy_flat_position()
    position.id = "nonfinite-position"
    setattr(position, field, value)

    item = remediation_service._kis_krx_flat_rebind_item(
        settings=settings,
        position=position,
        identity=identity,
        snapshot=_snapshot(),
        holding_present=False,
        open_order_present=False,
        scoped_route_exists=False,
        absence_proof_digest="a" * 64,
    )

    assert item["eligible"] is False
    assert item["reason"] == expected_reason
    assert item["evidence_fingerprint"] is None
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    "approved_evidence",
    [
        {},
        {"position": "a" * 63},
        {"position": "g" * 64},
        {"position-1": "a" * 64, "position-2": "b" * 64},
    ],
)
def test_kis_krx_flat_rebind_requires_one_well_formed_approval(
    approved_evidence: dict[str, str],
) -> None:
    with pytest.raises(ValueError):
        remediation_service._validated_kis_krx_flat_approvals(
            approved_evidence
        )


def test_kis_krx_flat_rebind_rejects_duplicate_unscoped_target_before_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings, engine, session_factory, db = _session()
    normalized_duplicate = _legacy_flat_position()
    normalized_duplicate.symbol = f" {TARGET_SYMBOL} "
    db.add_all([_legacy_flat_position(), normalized_duplicate])
    db.commit()
    adapter = KisKrxSnapshotAdapter([_snapshot()])
    monkeypatch.setattr(
        remediation_service,
        "ensure_authenticated_broker_session",
        lambda **kwargs: pytest.fail("duplicate target attempted broker auth"),
    )

    with pytest.raises(
        PositionIdentityConflictError,
        match="one exact unscoped position",
    ):
        _dry_run(
            settings=settings,
            db=db,
            session_factory=session_factory,
            adapter=adapter,
        )

    assert adapter.snapshot_requests == []
    db.close()
    engine.dispose()
