"""Reconciliation must derive cumulative deltas from current durable fills."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateSchema, DropSchema

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill
from app.services import reconciliation
from test_reconciliation import OBSERVED_AT, ReconciliationStubBroker, _order, _plan_backed_order, _utc


@pytest.fixture(params=["sqlite", "postgresql"])
def reconciliation_database(request, tmp_path, monkeypatch):
    schema = None
    admin_engine = None
    if request.param == "postgresql":
        url = os.getenv("KAI_EXEC_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("KAI_EXEC_TEST_POSTGRES_URL is required for PostgreSQL concurrency")
        parsed = make_url(url)
        if not (parsed.database or "").endswith("_test"):
            pytest.fail("isolated PostgreSQL database must end with _test")
        admin_engine = create_db_engine(Settings(database_url=url))
        schema = f"reconciliation_{uuid4().hex[:12]}"
        with admin_engine.begin() as connection:
            connection.execute(CreateSchema(schema))
        url = parsed.update_query_dict({"options": f"-csearch_path={schema}"}).render_as_string(
            hide_password=False
        )
    else:
        url = f"sqlite:///{tmp_path / 'reconciliation.db'}"
    settings = Settings(database_url=url, runtime_mode="paper")
    engine = create_db_engine(settings)
    monkeypatch.setattr(
        reconciliation,
        "ensure_authenticated_broker_session",
        lambda **_kwargs: SimpleNamespace(access_token="local-test-token"),
    )
    try:
        Base.metadata.create_all(engine)
        yield settings, engine
    finally:
        engine.dispose()
        if admin_engine is not None:
            with admin_engine.begin() as connection:
                connection.execute(DropSchema(schema, cascade=True))
            admin_engine.dispose()


def _cumulative_payload(order, seconds: int) -> dict[str, object]:
    return {
        "broker_order_id": order.broker_order_id,
        "status": "filled",
        "filled_quantity": 2,
        "remaining_quantity": 0,
        "average_fill_price": "71000",
        "observed_at": (OBSERVED_AT + timedelta(seconds=seconds)).isoformat(),
    }


def test_reconciliation_refreshes_another_sessions_cached_fills(reconciliation_database):
    settings, engine = reconciliation_database
    session_factory = create_session_factory(engine)
    with session_factory() as setup:
        order = _order(settings=settings, broker_order_id="cached-fills")
        setup.add(order)
        setup.commit()
        order_id = order.id

    with session_factory() as first, session_factory() as second:
        first_order = first.get(BrokerOrder, order_id)
        second_order = second.get(BrokerOrder, order_id)
        assert first_order.fills == second_order.fills == []
        for offset, db, current in ((0, first, first_order), (1, second, second_order)):
            broker = ReconciliationStubBroker(reconcile_payloads=[_cumulative_payload(current, offset)])
            reconciliation.reconcile_broker_order(settings=settings, db=db, adapter=broker, order=current)

    with session_factory() as verify:
        assert verify.scalar(select(func.sum(ExecutionFill.quantity))) == 2
        assert verify.scalar(select(func.count()).select_from(ExecutionFill)) == 1


def test_overlapping_broker_reads_serialize_cumulative_fill_writes(reconciliation_database):
    settings, engine = reconciliation_database
    session_factory = create_session_factory(engine)
    with session_factory() as setup:
        order = _order(settings=settings, broker_order_id="overlapping-fills")
        setup.add(order)
        setup.commit()
        order_id = order.id
    both_inquiries_started = Barrier(2, timeout=10)

    def reconcile_once(offset: int):
        class OverlappingBroker(ReconciliationStubBroker):
            def reconcile_order(self, *args, **kwargs):
                both_inquiries_started.wait()
                return super().reconcile_order(*args, **kwargs)

        with session_factory() as db:
            current = db.get(BrokerOrder, order_id)
            assert current.fills == []
            broker = OverlappingBroker(reconcile_payloads=[_cumulative_payload(current, offset)])
            return reconciliation.reconcile_broker_order(settings=settings, db=db, adapter=broker, order=current)

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(reconcile_once, offset) for offset in (0, 1)]
        results = [future.result(timeout=20) for future in futures]

    assert sum(result["fills_added"] for result in results) == 1
    with session_factory() as verify:
        assert verify.scalar(select(func.sum(ExecutionFill.quantity))) == 2
        assert verify.scalar(select(func.count()).select_from(ExecutionFill)) == 1


@pytest.mark.parametrize("stale_status", ["partially_filled", "unknown"])
def test_delayed_reconciliation_cannot_regress_newer_broker_truth(
    reconciliation_database, stale_status: str
):
    settings, engine = reconciliation_database
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan, item, order = _plan_backed_order(
            settings=settings, plan_id="stale-reply", side="buy", intent="next_open_buy",
            broker_order_id="stale-reply",
        )
        db.add(plan)
        db.commit()
        newer = {**_cumulative_payload(order, 2), "response_metadata": {"marker": "newer"}}
        older = {
            **_cumulative_payload(order, 1), "status": stale_status,
            "filled_quantity": 1, "response_metadata": {"marker": "older"},
        }
        broker = ReconciliationStubBroker(reconcile_payloads=[newer, older])
        reconciliation.reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)
        result = reconciliation.reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

        db.refresh(order)
        db.refresh(item)
        assert result["status"] == "filled"
        assert result["status_changed"] is False
        assert result["fills_added"] == 0
        assert order.status == item.status == "filled"
        assert order.response_metadata == {"marker": "newer"}
        assert _utc(order.last_broker_sync_at) == OBSERVED_AT + timedelta(seconds=2)
        assert db.scalar(select(func.sum(ExecutionFill.quantity))) == 2
        assert db.scalar(select(func.count()).select_from(BrokerOrderEvent).where(
            BrokerOrderEvent.event_type == "reconcile_result_stale",
        )) == 1


@pytest.mark.parametrize(
    "prior_event", ["submit_acknowledged", "reconcile_failed", "reconcile_result_unrecognized_status"]
)
def test_ack_and_error_timestamps_do_not_block_successful_reconciliation(
    reconciliation_database, prior_event: str
):
    settings, engine = reconciliation_database
    with create_session_factory(engine)() as db:
        order = _order(settings=settings, broker_order_id="non-truth-timestamp")
        order.last_broker_sync_at = OBSERVED_AT + timedelta(seconds=3)
        db.add(order)
        db.flush()
        db.add(BrokerOrderEvent(
            broker_order_pk=order.id, event_type=prior_event,
            event_source="reconciliation", normalized_status="submitted",
            observed_at=order.last_broker_sync_at,
        ))
        db.commit()
        broker = ReconciliationStubBroker(reconcile_payloads=[_cumulative_payload(order, 2)])

        result = reconciliation.reconcile_broker_order(settings=settings, db=db, adapter=broker, order=order)

        assert result["status"] == "filled"
        assert result["fills_added"] == 1
        assert db.scalar(select(func.sum(ExecutionFill.quantity))) == 2
