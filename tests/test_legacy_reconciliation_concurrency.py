"""Legacy remediation and Core fencing must not acquire inverse row locks."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from threading import Event

import pytest
from sqlalchemy import event

from app.broker.base import BrokerAccountSnapshot
from app.db.session import create_session_factory
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem
from app.services import core_handoff_v4, reconciliation
from test_reconciliation import ReconciliationStubBroker
from test_reconciliation_concurrency import reconciliation_database


def test_legacy_manual_review_and_core_fencing_share_lock_order(reconciliation_database):
    settings, engine = reconciliation_database
    if engine.dialect.name != "postgresql":
        pytest.skip("requires PostgreSQL row locks")
    session_factory = create_session_factory(engine)
    stale_at = datetime.now(UTC) - timedelta(days=45)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="legacy-review-locks", source="kai-trading-core",
            market_date=date(2026, 9, 1), mode="paper", status="accepted",
        )
        item = ExecutionPlanItem(
            plan=plan, symbol="005930", side="buy", intent="next_open_buy",
            status="manual_review_required", provider="kis", venue="KRX",
            asset_class="stock", risk_context={"exchange": "KRX"},
        )
        order = BrokerOrder(
            plan_item=item, broker_name="kis", mode="paper", symbol="005930", side="buy",
            idempotency_key="legacy-review-locks", status="manual_review_required", quantity=1,
            created_at=stale_at, last_error_classification="missing_broker_order_id",
        )
        db.add(order)
        db.commit()
        item_id, order_id = item.id, order.id

    review_snapshot_started = Event()
    fence_locked_item = Event()
    review_requests_authority_lock = Event()

    class CoordinatedBroker(ReconciliationStubBroker):
        def get_account_snapshot(self, *args, **kwargs):
            review_snapshot_started.set()
            assert fence_locked_item.wait(10)
            return super().get_account_snapshot(*args, **kwargs)

    def before_query(connection, _cursor, statement, _parameters, _context, _many):
        if (
            connection.info.get("legacy_lock_test_role") == "review"
            and "FOR UPDATE" in statement
            and ("FROM execution_plan_items" in statement or "FROM execution_plans" in statement)
        ):
            review_requests_authority_lock.set()

    def after_query(connection, _cursor, statement, _parameters, _context, _many):
        if (
            connection.info.get("legacy_lock_test_role") == "fence"
            and "FROM execution_plan_items" in statement and "FOR UPDATE" in statement
        ):
            fence_locked_item.set()
            assert review_requests_authority_lock.wait(10)

    event.listen(engine, "before_cursor_execute", before_query)
    event.listen(engine, "after_cursor_execute", after_query)

    def review():
        with session_factory() as db:
            db.connection().info["legacy_lock_test_role"] = "review"
            broker = CoordinatedBroker(account_snapshot=BrokerAccountSnapshot(
                observed_at=datetime.now(UTC), source="kis:account_snapshot",
                holdings_complete=True, open_orders_complete=True,
                payload={"exchange": "KRX", "account_number_masked": "****5678"},
            ))
            result = reconciliation._reconcile_legacy_manual_review_orders(
                settings=settings, db=db, adapter=broker,
                stale_before=datetime.now(UTC) - timedelta(days=30), symbols=None,
                dry_run=False, actor=None, operator_reason=None, approvals={},
            )
            db.commit()
            assert broker.order_calls == []
            return result

    def fence():
        assert review_snapshot_started.wait(10)
        with session_factory() as db:
            db.connection().info["legacy_lock_test_role"] = "fence"
            return core_handoff_v4.fence_unproven_legacy_core_buy_authority(
                db=db, execution_plan_item_id=item_id, settings=settings,
            )

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            reviewed = workers.submit(review)
            fenced = workers.submit(fence)
            assert reviewed.result(timeout=20)["resolved_order_count"] == 0
            assert fenced.result(timeout=20) is True
    finally:
        event.remove(engine, "before_cursor_execute", before_query)
        event.remove(engine, "after_cursor_execute", after_query)

    with session_factory() as db:
        assert db.get(ExecutionPlanItem, item_id).status == "manual_review_required"
        assert db.get(BrokerOrder, order_id).status == "manual_review_required"
