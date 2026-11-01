from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
import json

import pytest
from sqlalchemy import select

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    CoreBuyAuthorityState,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    RuntimeControlState,
    RuntimeRemediationEvent,
)
from app.services import monitoring as monitoring_service
from app.services import position_truth_remediation as remediation_service
from app.services import runtime as runtime_service
from app.services.corporate_action_quarantine_contract import (
    REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE,
    REVIEWED_CORPORATE_ACTION_INVALID_REASON,
    REVIEWED_CORPORATE_ACTION_OUTCOME,
    REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION,
    ReviewedCorporateActionQuarantineState,
    load_reviewed_corporate_action_quarantine_state,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_position_identity,
    payload_with_position_identity,
)
from app.services.reconciliation import sync_execution_positions_from_broker_snapshot


NOW = datetime(2026, 11, 1, 3, 0, tzinfo=UTC)
SEC_REFS = [
    "https://www.sec.gov/Archives/edgar/data/1358403/000110465923068734/tm2317968d1_8k.htm",
    "https://www.sec.gov/Archives/edgar/data/1358403/000110465924028332/tm247084d1_8k.htm",
]


class ReconciliationOnlyBroker:
    provider = "kis"

    def __init__(self, snapshot: BrokerAccountSnapshot) -> None:
        self.snapshot = snapshot
        self.snapshot_calls = 0

    def get_account_snapshot(self, access_token: str, *, symbols=None, exchange="KRX"):
        self.snapshot_calls += 1
        assert access_token == "read-only-token"
        assert symbols == ["WWBLCM"]
        assert exchange == "NASD"
        return self.snapshot


def _session():
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        execution_position_account_scope="reviewed-corporate-action-test",
        kis_account_number="12345678",
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    factory = create_session_factory(engine)
    return settings, engine, factory, factory()


def _review_evidence() -> dict[str, object]:
    return {
        "source": "operator_reviewed_sec_filings",
        "issuer_name": "Bellicum Pharmaceuticals, Inc.",
        "corporate_action": "dissolution",
        "effective_date": "2024-02-23",
        "security_status": "no_longer_publicly_traded_or_outstanding",
        "evidence_refs": list(reversed(SEC_REFS)),
        "reviewed_at": (NOW - timedelta(hours=1)).isoformat(),
    }


def _snapshot_payload(*, quantity: str = "75") -> dict[str, object]:
    return {
        "currency": "USD",
        "exchange": "NASD",
        "account_number_masked": "****5678",
        "holdings_coverage": "complete",
        "open_orders_coverage": "complete",
        "balance": {
            "rt_cd": "0",
            "output1": [
                {
                    "ovrs_pdno": "WWBLCM",
                    "ovrs_excg_cd": "NASD",
                    "ovrs_cblc_qty": quantity,
                    "ord_psbl_qty": quantity,
                    "pchs_avg_pric": "0.9164",
                    "now_pric2": "0",
                    "ovrs_stck_evlu_amt": "0",
                    "prdt_name": "Bellicum Pharmaceuticals",
                }
            ],
        },
        "open_orders": {"rt_cd": "0", "output": []},
        "_position_sync": {
            "asset_class": "us_equity",
            "exchange": "NASD",
            "monitoring_eligible": False,
            "manual_review_required": True,
            "manual_review_reason": "unpriced_overseas_holding",
            "manual_review_recorded_at": (
                NOW - timedelta(minutes=2)
            ).isoformat(),
        },
    }


def _add_position(db, settings: Settings) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="WWBLCM",
        quantity=75,
        average_price=Decimal("0.9164"),
        updated_from="reconciliation",
        last_reconciled_at=NOW - timedelta(seconds=5),
        raw_payload=payload_with_position_identity(
            _snapshot_payload(),
            identity=identity,
        ),
    )
    db.add(position)
    db.commit()
    return position


def _analyze(settings: Settings, db, position: ExecutionPosition):
    return remediation_service.analyze_reviewed_corporate_action_quarantine(
        settings=settings,
        db=db,
        position_id=position.id,
        review_evidence=_review_evidence(),
        now=NOW,
    )


def _apply_quarantine(
    settings: Settings,
    db,
    position: ExecutionPosition,
) -> dict[str, object]:
    dry_run = _analyze(settings, db, position)
    return remediation_service.apply_reviewed_corporate_action_quarantine(
        settings=settings,
        db=db,
        position_id=position.id,
        review_evidence=_review_evidence(),
        approved_fingerprint=str(dry_run["item"]["evidence_fingerprint"]),
        actor="operator",
        reason="SEC dissolution evidence reviewed against exact KIS holding",
        now=NOW,
    )


def _quarantine_state(
    *,
    db,
    position: ExecutionPosition,
) -> ReviewedCorporateActionQuarantineState:
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    return load_reviewed_corporate_action_quarantine_state(
        db=db,
        position=persisted,
    )


def _runtime_alerts(
    *,
    settings: Settings,
    db,
    now: datetime = NOW,
) -> dict[str, object]:
    truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )
    return runtime_service.runtime_actionable_alerts_payload(
        monitoring={},
        freshness={"position_truth": truth},
    )


def _reconciliation_broker(*, observed_at: datetime) -> ReconciliationOnlyBroker:
    return ReconciliationOnlyBroker(
        BrokerAccountSnapshot(
            observed_at=observed_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holdings=[
                BrokerAccountHolding(
                    symbol="WWBLCM",
                    quantity=75,
                    average_price=Decimal("0.9164"),
                    sellable_quantity=75,
                    payload={
                        "ovrs_excg_cd": "NASD",
                        "now_pric2": "0",
                        "ovrs_stck_evlu_amt": "0",
                    },
                )
            ],
            open_orders=[],
            payload={
                **_snapshot_payload(),
                "_position_sync": None,
            },
        )
    )


def test_reviewed_corporate_action_analysis_binds_exact_review_and_account_truth() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    before = json.loads(json.dumps(position.raw_payload))

    first = _analyze(settings, db, position)
    second = _analyze(settings, db, position)

    item = first["item"]
    assert item["eligible"] is True
    assert item["reason"] == "exact_reviewed_corporate_action_match"
    assert item["review_evidence"]["evidence_refs"] == sorted(SEC_REFS)
    assert item["snapshot_evidence"]["broker_instrument_name"] == (
        "Bellicum Pharmaceuticals"
    )
    assert item["review_evidence_digest"] == hashlib.sha256(
        json.dumps(
            item["review_evidence"],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    material_keys = {
        "safety_policy_version",
        "safety_invariants",
        "review_evidence",
        "review_evidence_digest",
        "snapshot_evidence_digest",
        "mode",
        "position_id",
        "provider",
        "account_scope",
        "exchange",
        "symbol",
        "quantity",
        "base_quantity",
        "average_price",
        "updated_from",
        "last_reconciled_at",
        "raw_payload_digest",
    }
    expected_fingerprint = hashlib.sha256(
        json.dumps(
            {key: item[key] for key in material_keys},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert item["evidence_fingerprint"] == expected_fingerprint
    assert second["item"]["evidence_fingerprint"] == expected_fingerprint
    assert item["safety_invariants"] == {
        "broker_actions_allowed": False,
        "collision_scope": "mode_provider_account_symbol_any_exchange",
        "provider": "kis",
        "account_identity_preserved": True,
        "position_quantity_preserved": True,
        "position_average_price_preserved": True,
        "broker_reconciliation_continues": True,
        "quote_strategy_monitoring_allowed": False,
        "route_disposition": REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION,
    }
    db.expire_all()
    assert db.get(ExecutionPosition, position.id).raw_payload == before
    assert db.query(RuntimeRemediationEvent).count() == 0
    db.close()
    engine.dispose()


def test_apply_preserves_truth_excludes_workset_reconciles_and_emits_distinct_alert() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    original_identity = (
        position.provider,
        position.account_scope,
        position.exchange,
        position.symbol,
    )
    dry_run = _analyze(settings, db, position)

    applied = remediation_service.apply_reviewed_corporate_action_quarantine(
        settings=settings,
        db=db,
        position_id=position.id,
        review_evidence=_review_evidence(),
        approved_fingerprint=str(dry_run["item"]["evidence_fingerprint"]),
        actor="operator",
        reason="SEC dissolution evidence reviewed against exact KIS holding",
        now=NOW,
    )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert (
        persisted.provider,
        persisted.account_scope,
        persisted.exchange,
        persisted.symbol,
    ) == original_identity
    assert persisted.quantity == 75
    assert persisted.base_quantity is None
    assert persisted.average_price == Decimal("0.9164")
    assert persisted.raw_payload["pre_corporate_action_position_sync"][
        "manual_review_reason"
    ] == "unpriced_overseas_holding"
    assert persisted.raw_payload["_position_sync"]["route_disposition"] == (
        REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
    )
    assert persisted.raw_payload["_position_sync"]["manual_review_required"] is False
    assert applied["summary"]["broker_api_mutation_count"] == 0
    assert applied["summary"]["quarantined_count"] == 1
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    assert event.candidate_type == REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE
    assert event.outcome == REVIEWED_CORPORATE_ACTION_OUTCOME
    assert event.evidence_summary["review_evidence"]["evidence_refs"] == sorted(
        SEC_REFS
    )
    assert event.evidence_summary["orders_submitted"] == 0
    assert event.evidence_summary["orders_amended"] == 0
    assert event.evidence_summary["orders_canceled"] == 0
    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.VERIFIED
    assert db.query(BrokerOrder).count() == 0
    assert db.query(ExecutionFill).count() == 0
    plan = ExecutionPlan(
        external_plan_id="reviewed-corporate-action-plan",
        source="test",
        market_date=NOW.date(),
        mode=settings.runtime_mode,
        status="accepted",
    )
    plan.items.append(
        ExecutionPlanItem(
            plan_item_index=0,
            symbol="WWBLCM",
            side="buy",
            intent="next_open_buy",
            status="watch_monitoring_active",
            provider="kis",
            venue="kis",
            asset_class="us_equity",
            risk_context={"exchange": "NASD"},
        )
    )
    db.add(plan)
    db.commit()
    assert "WWBLCM" not in monitoring_service.list_autonomous_monitoring_symbols(
        settings=settings,
        db=db,
        current_market_date=NOW.date(),
    )
    db.refresh(persisted)
    assert monitoring_service._position_for_monitoring_item(
        settings=settings,
        item=plan.items[0],
        positions_by_symbol={"WWBLCM": persisted},
    ) is None

    reconciled_at = NOW + timedelta(minutes=1)
    broker = ReconciliationOnlyBroker(
        BrokerAccountSnapshot(
            observed_at=reconciled_at,
            source="kis:us_equity_account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holdings=[
                BrokerAccountHolding(
                    symbol="WWBLCM",
                    quantity=75,
                    average_price=Decimal("0.9164"),
                    sellable_quantity=75,
                    payload={
                        "ovrs_excg_cd": "NASD",
                        "now_pric2": "0",
                        "ovrs_stck_evlu_amt": "0",
                    },
                )
            ],
            open_orders=[],
            payload={
                **_snapshot_payload(),
                "_position_sync": None,
            },
        )
    )
    sync_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"WWBLCM"},
        exchange="NASD",
        authenticated_access_token="read-only-token",
    )
    db.refresh(persisted)
    assert broker.snapshot_calls == 1
    assert sync_result["updated"] == 0
    assert sync_result["monitoring_recovered_count"] == 0
    assert persisted.quantity == 75
    assert persisted.account_scope == original_identity[1]
    assert persisted.last_reconciled_at.replace(tzinfo=UTC) == reconciled_at
    assert persisted.raw_payload["_position_sync"]["route_disposition"] == (
        REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
    )
    assert "WWBLCM" not in monitoring_service.list_autonomous_monitoring_symbols(
        settings=settings,
        db=db,
        current_market_date=NOW.date(),
    )
    truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=reconciled_at,
    )
    assert truth["reviewed_corporate_action_count"] == 1
    assert truth["reviewed_corporate_action_symbols"] == ["WWBLCM"]
    assert truth["manual_review_count"] == 0
    alerts = runtime_service.runtime_actionable_alerts_payload(
        monitoring={},
        freshness={"position_truth": truth},
    )
    distinct = next(
        alert
        for alert in alerts["alerts"]
        if alert["kind"] == "reviewed_corporate_action_holding_quarantined"
    )
    assert distinct["severity"] == "warning"
    assert distinct["symbols"] == ["WWBLCM"]
    assert distinct["runtime_impact"]["broker_reconciliation_continues"] is True
    assert all(
        alert["kind"] != "position_monitoring_manual_review_required"
        for alert in alerts["alerts"]
    )
    assert db.query(BrokerOrder).count() == 0
    assert db.query(ExecutionFill).count() == 0
    db.close()
    engine.dispose()


def test_apply_rejects_fingerprint_drift_without_mutation() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    dry_run = _analyze(settings, db, position)
    fingerprint = str(dry_run["item"]["evidence_fingerprint"])
    mismatched = f"{'0' if fingerprint[0] != '0' else '1'}{fingerprint[1:]}"

    with pytest.raises(PositionIdentityConflictError):
        remediation_service.apply_reviewed_corporate_action_quarantine(
            settings=settings,
            db=db,
            position_id=position.id,
            review_evidence=_review_evidence(),
            approved_fingerprint=mismatched,
            actor="operator",
            reason="stale approval",
            now=NOW,
        )

    drifted_review = _review_evidence()
    drifted_review["issuer_name"] = "Different Issuer"
    with pytest.raises(PositionIdentityConflictError):
        remediation_service.apply_reviewed_corporate_action_quarantine(
            settings=settings,
            db=db,
            position_id=position.id,
            review_evidence=drifted_review,
            approved_fingerprint=fingerprint,
            actor="operator",
            reason="drifted review",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted.raw_payload["_position_sync"]["manual_review_required"] is True
    assert db.query(RuntimeRemediationEvent).count() == 0
    db.close()
    engine.dispose()


def test_apply_rejects_position_drift_after_review() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    dry_run = _analyze(settings, db, position)
    fingerprint = str(dry_run["item"]["evidence_fingerprint"])
    payload = dict(position.raw_payload)
    payload["balance"] = dict(payload["balance"])
    rows = [dict(row) for row in payload["balance"]["output1"]]
    rows[0]["ord_psbl_qty"] = "74"
    payload["balance"]["output1"] = rows
    position.raw_payload = payload
    db.commit()

    with pytest.raises(PositionIdentityConflictError):
        remediation_service.apply_reviewed_corporate_action_quarantine(
            settings=settings,
            db=db,
            position_id=position.id,
            review_evidence=_review_evidence(),
            approved_fingerprint=fingerprint,
            actor="operator",
            reason="drifted approval",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted.raw_payload["balance"]["output1"][0]["ord_psbl_qty"] == "74"
    assert persisted.raw_payload["_position_sync"]["manual_review_required"] is True
    assert db.query(RuntimeRemediationEvent).count() == 0
    db.close()
    engine.dispose()


def test_apply_rejects_cross_exchange_symbol_collision_after_review() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    dry_run = _analyze(settings, db, position)
    fingerprint = str(dry_run["item"]["evidence_fingerprint"])
    collision_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NYSE",
    )
    db.add(
        ExecutionPosition(
            mode=settings.runtime_mode,
            provider=collision_identity.provider,
            account_scope=collision_identity.account_scope,
            exchange=collision_identity.exchange,
            symbol="WWBLCM",
            quantity=0,
            updated_from="reconciliation",
            last_reconciled_at=NOW,
            raw_payload=payload_with_position_identity(
                {},
                identity=collision_identity,
            ),
        )
    )
    db.commit()

    with pytest.raises(
        PositionIdentityConflictError,
        match="symbol collision",
    ):
        remediation_service.apply_reviewed_corporate_action_quarantine(
            settings=settings,
            db=db,
            position_id=position.id,
            review_evidence=_review_evidence(),
            approved_fingerprint=fingerprint,
            actor="operator",
            reason="collision approval",
            now=NOW,
        )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted.raw_payload["_position_sync"]["manual_review_required"] is True
    assert db.query(RuntimeRemediationEvent).count() == 0
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        ("safety_policy_version", "reviewed-corporate-action-quarantine-v2"),
        ("review_evidence_digest", "0" * 64),
        (
            "corporate_action_reviewed_at",
            (NOW - timedelta(hours=2)).isoformat(),
        ),
        ("asset_class", "stock"),
        ("exchange", "NYSE"),
    ],
)
def test_quarantine_contract_rejects_mutable_sync_drift(
    field: str,
    drifted_value: object,
) -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    payload = deepcopy(persisted.raw_payload)
    payload["_position_sync"][field] = drifted_value
    persisted.raw_payload = payload
    db.commit()

    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    assert monitoring_service._position_monitoring_is_disabled(persisted) is True
    alerts = _runtime_alerts(settings=settings, db=db)
    critical = next(
        alert
        for alert in alerts["alerts"]
        if alert["kind"] == "position_monitoring_manual_review_required"
    )
    assert critical["symbols"] == ["WWBLCM"]
    assert critical["reasons"] == [
        {
            "code": REVIEWED_CORPORATE_ACTION_INVALID_REASON,
            "count": 1,
            "symbols": ["WWBLCM"],
        }
    ]
    assert all(
        alert["kind"] != "reviewed_corporate_action_holding_quarantined"
        for alert in alerts["alerts"]
    )
    db.close()
    engine.dispose()


def test_quarantine_contract_requires_exact_audit_event() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    db.delete(event)
    db.commit()

    assert _quarantine_state(
        db=db,
        position=position,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        ("position_id", "other-position"),
        ("provider", "alpaca"),
        ("account_scope", "other-account"),
        ("exchange", "NYSE"),
        ("symbol", "BLCM"),
        ("asset_class", "stock"),
        (
            "corporate_action_reviewed_at",
            (NOW - timedelta(hours=2)).isoformat(),
        ),
        ("safety_policy_version", "reviewed-corporate-action-quarantine-v2"),
        ("review_evidence_digest", "0" * 64),
        ("quantity", "74"),
        ("average_price", "0.9000"),
    ],
)
def test_quarantine_contract_binds_event_to_current_position_truth(
    field: str,
    drifted_value: object,
) -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    summary = deepcopy(event.evidence_summary)
    summary[field] = drifted_value
    event.evidence_summary = summary
    db.commit()

    assert _quarantine_state(
        db=db,
        position=position,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    db.close()
    engine.dispose()


def test_quarantine_contract_rejects_event_identity_and_evidence_drift() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    event = db.scalar(select(RuntimeRemediationEvent))
    assert event is not None
    event.candidate_key = "other-position"
    summary = deepcopy(event.evidence_summary)
    summary["review_evidence"]["issuer_name"] = "Different Issuer"
    event.evidence_summary = summary
    db.commit()

    assert _quarantine_state(
        db=db,
        position=position,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    db.close()
    engine.dispose()


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        ("quantity", 74),
        ("average_price", Decimal("0.9000")),
    ],
)
def test_quarantine_contract_rejects_current_position_drift(
    field: str,
    drifted_value: object,
) -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    setattr(persisted, field, drifted_value)
    db.commit()

    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    db.close()
    engine.dispose()


def test_invalid_marked_quarantine_never_reactivates_during_reconciliation() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    persisted.quantity = 74
    db.commit()
    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED

    reconciled_at = NOW + timedelta(minutes=1)
    broker = _reconciliation_broker(observed_at=reconciled_at)
    sync_result = sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=broker,
        symbols={"WWBLCM"},
        exchange="NASD",
        authenticated_access_token="read-only-token",
    )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    assert persisted.quantity == 75
    assert persisted.average_price == Decimal("0.9164")
    assert persisted.last_reconciled_at.replace(tzinfo=UTC) == reconciled_at
    sync_context = persisted.raw_payload["_position_sync"]
    assert sync_context["monitoring_eligible"] is False
    assert sync_context["trade_eligible"] is False
    assert sync_context["manual_review_required"] is True
    assert sync_context["manual_review_reason"] == (
        REVIEWED_CORPORATE_ACTION_INVALID_REASON
    )
    assert sync_context["quarantine_validation_state"] == "invalid_marked"
    assert sync_result["monitoring_recovered_count"] == 0
    assert monitoring_service._position_monitoring_is_disabled(persisted) is True
    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED
    alerts = _runtime_alerts(
        settings=settings,
        db=db,
        now=reconciled_at,
    )
    assert any(
        alert["kind"] == "position_monitoring_manual_review_required"
        and alert["symbols"] == ["WWBLCM"]
        for alert in alerts["alerts"]
    )
    assert all(
        alert["kind"] != "reviewed_corporate_action_holding_quarantined"
        for alert in alerts["alerts"]
    )
    assert db.query(BrokerOrder).count() == 0
    assert db.query(ExecutionFill).count() == 0
    db.close()
    engine.dispose()


def test_missing_sync_marker_is_restored_only_as_invalid_during_reconciliation() -> None:
    settings, engine, _, db = _session()
    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)
    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    payload = deepcopy(persisted.raw_payload)
    payload.pop("_position_sync")
    persisted.raw_payload = payload
    db.commit()
    assert _quarantine_state(
        db=db,
        position=persisted,
    ) is ReviewedCorporateActionQuarantineState.INVALID_MARKED

    reconciled_at = NOW + timedelta(minutes=1)
    sync_execution_positions_from_broker_snapshot(
        settings=settings,
        db=db,
        adapter=_reconciliation_broker(observed_at=reconciled_at),
        symbols={"WWBLCM"},
        exchange="NASD",
        authenticated_access_token="read-only-token",
    )

    db.expire_all()
    persisted = db.get(ExecutionPosition, position.id)
    assert persisted is not None
    sync_context = persisted.raw_payload["_position_sync"]
    assert sync_context["monitoring_eligible"] is False
    assert sync_context["trade_eligible"] is False
    assert sync_context["manual_review_required"] is True
    assert sync_context["manual_review_reason"] == (
        REVIEWED_CORPORATE_ACTION_INVALID_REASON
    )
    assert monitoring_service._position_monitoring_is_disabled(persisted) is True
    db.close()
    engine.dispose()


def test_reason_only_runtime_payload_is_critical_not_verified_warning() -> None:
    alerts = runtime_service.runtime_actionable_alerts_payload(
        monitoring={},
        freshness={
            "position_truth": {
                "manual_review_count": 1,
                "manual_review_symbols": ["WWBLCM"],
                "manual_review_reasons": {
                    "WWBLCM": "corporate_action_nontradeable",
                },
            }
        },
    )

    critical = next(
        alert
        for alert in alerts["alerts"]
        if alert["kind"] == "position_monitoring_manual_review_required"
    )
    assert critical["reasons"] == [
        {
            "code": REVIEWED_CORPORATE_ACTION_INVALID_REASON,
            "count": 1,
            "symbols": ["WWBLCM"],
        }
    ]
    assert all(
        alert["kind"] != "reviewed_corporate_action_holding_quarantined"
        for alert in alerts["alerts"]
    )


def test_unrelated_krx_and_runtime_safety_controls_remain_unchanged() -> None:
    settings, engine, _, db = _session()
    control = RuntimeControlState(
        id="global",
        mode=settings.runtime_mode,
        paused=True,
        kill_switch_enabled=False,
        hourly_risk_off_active=True,
        hourly_risk_off_epoch=7,
        hourly_risk_off_context={"reason": "existing-risk-off"},
        readiness_reason="existing-readiness",
        last_transition_action="pause",
        last_transition_actor="operator",
        last_transition_source="operator",
        last_transition_reason="existing pause",
        last_transition_at=NOW,
    )
    fence = CoreBuyAuthorityState(
        scope_fingerprint="f" * 64,
        provider="kis",
        market="KRX",
        currency="KRW",
        runtime_mode=settings.runtime_mode,
        account_scope="krx-control-test",
        fence_active=True,
        epoch=9,
        delivery_status="applied",
        projection_pending=False,
        healthy_observation_count=0,
        last_observation_id="existing-observation",
        last_material_fingerprint="a" * 64,
        last_action="block_new_buy",
        market_date=NOW.date(),
        last_received_at=NOW,
        context={"reason": "existing-fence"},
    )
    db.add_all([control, fence])
    krx_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    krx_position = ExecutionPosition(
        mode=settings.runtime_mode,
        provider=krx_identity.provider,
        account_scope=krx_identity.account_scope,
        exchange=krx_identity.exchange,
        symbol="005930",
        quantity=1,
        average_price=Decimal("70000"),
        updated_from="reconciliation",
        last_reconciled_at=NOW,
        raw_payload=payload_with_position_identity(
            {},
            identity=krx_identity,
        ),
    )
    db.add(krx_position)
    db.commit()
    safety_before = (
        control.paused,
        control.hourly_risk_off_active,
        control.hourly_risk_off_epoch,
        deepcopy(control.hourly_risk_off_context),
        fence.fence_active,
        fence.epoch,
        fence.last_action,
        deepcopy(fence.context),
    )

    position = _add_position(db, settings)
    _apply_quarantine(settings, db, position)

    db.expire_all()
    persisted_krx = db.get(ExecutionPosition, krx_position.id)
    assert persisted_krx is not None
    assert load_reviewed_corporate_action_quarantine_state(
        db=db,
        position=persisted_krx,
    ) is ReviewedCorporateActionQuarantineState.UNRELATED
    persisted_control = db.get(RuntimeControlState, "global")
    persisted_fence = db.get(CoreBuyAuthorityState, "f" * 64)
    assert persisted_control is not None
    assert persisted_fence is not None
    assert (
        persisted_control.paused,
        persisted_control.hourly_risk_off_active,
        persisted_control.hourly_risk_off_epoch,
        persisted_control.hourly_risk_off_context,
        persisted_fence.fence_active,
        persisted_fence.epoch,
        persisted_fence.last_action,
        persisted_fence.context,
    ) == safety_before
    db.close()
    engine.dispose()
