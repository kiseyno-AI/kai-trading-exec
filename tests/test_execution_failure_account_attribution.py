"""Failure diagnostics must not borrow another provider's configured account."""

from datetime import UTC, datetime

import pytest

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder
from app.services.execution_failures import record_actionable_execution_failure
from app.services.rejected_order_retries import handle_reconciled_rejected_order


@pytest.mark.parametrize("provider", ["kis", "upbit", "alpaca"])
@pytest.mark.parametrize("source", ["submission", "reconciliation"])
@pytest.mark.parametrize("persisted_mask", [None, "****4321"])
def test_failure_account_uses_only_order_evidence_or_matching_provider_settings(
    provider: str, source: str, persisted_mask: str | None,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:", runtime_mode="paper",
        kis_account_number="12345678",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    try:
        with create_session_factory(engine)() as db:
            order = BrokerOrder(
                broker_name=provider, mode="paper", idempotency_key="account-attribution",
                symbol={"kis": "005930", "upbit": "KRW-BTC", "alpaca": "AAPL"}[provider],
                side="buy", status="rejected", quantity=1,
                request_metadata={"account_number_masked": persisted_mask},
            )
            db.add(order)
            db.flush()
            observed_at = datetime(2026, 11, 8, tzinfo=UTC)
            if source == "submission":
                failure = record_actionable_execution_failure(
                    settings=settings, db=db, order=order, plan_item=None,
                    broker_error_classification="broker_rejected",
                    broker_error_code="INSUFFICIENT_CASH",
                    broker_error_message="insufficient cash", retryable=False,
                    observed_at=observed_at, request_source="test",
                )
            else:
                failure = handle_reconciled_rejected_order(
                    settings=settings, db=db, order=order, observed_at=observed_at,
                    payload={
                        "reject_reason_code": "INSUFFICIENT_CASH",
                        "reject_reason_message": "insufficient cash",
                    },
                )
            db.commit()
            db.refresh(failure)

            expected = persisted_mask or ("****5678" if provider == "kis" else None)
            assert failure.account == expected
    finally:
        engine.dispose()
