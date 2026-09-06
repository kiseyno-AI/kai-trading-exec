from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPosition, RuntimeRemediationEvent
from app.services.position_truth_remediation import (
    analyze_position_truth_remediation,
    apply_position_truth_remediation,
)


NOW = datetime(2026, 9, 6, 18, 0, tzinfo=UTC)


class PositionInquiryAdapter:
    provider = "kis"

    def __init__(self, *, fail_exchange: str | None = None) -> None:
        self.exchanges: list[str] = []
        self.fail_exchange = fail_exchange

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
    return settings, engine, create_session_factory(engine)()


def test_position_truth_remediation_routes_supported_and_blocks_unknown(monkeypatch) -> None:
    settings, engine, db = _session()
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
        "app.services.reconciliation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    dry_run = analyze_position_truth_remediation(settings=settings, db=db, now=NOW)
    applied = apply_position_truth_remediation(
        settings=settings,
        db=db,
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
    engine.dispose()


def test_position_truth_remediation_rolls_back_prior_exchange_when_later_inquiry_fails(
    monkeypatch,
) -> None:
    settings, engine, db = _session()
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
        "app.services.reconciliation.ensure_authenticated_broker_session",
        lambda **kwargs: type("SessionState", (), {"access_token": "test-token"})(),
    )

    with pytest.raises(RuntimeError, match="NASD inquiry failed"):
        apply_position_truth_remediation(
            settings=settings,
            db=db,
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
