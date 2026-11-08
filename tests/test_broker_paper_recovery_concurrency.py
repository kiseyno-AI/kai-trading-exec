"""Paper recovery must coexist with legacy Core authority fencing."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from threading import Barrier, Event

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.schemas.execution import PlaceOrderRequest
from app.db.session import create_session_factory
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionFill, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services import broker_orders, core_handoff_v4
from app.services.position_identity import build_position_identity
from test_broker_ack_persistence_recovery import _enable_paper_fills, _order_payload
from test_reconciliation_concurrency import reconciliation_database  # noqa: F401


def test_paper_recovery_and_legacy_authority_fencing_share_lock_order(
    reconciliation_database, monkeypatch
):
    settings, engine = reconciliation_database
    if engine.dialect.name != "postgresql":
        pytest.skip("requires PostgreSQL row locks")
    _enable_paper_fills(monkeypatch)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="legacy-paper-recovery", source="kai-trading-core",
            market_date=date(2026, 11, 9), mode="paper", status="accepted",
        )
        item = ExecutionPlanItem(
            plan=plan, symbol="KRW-BTC", side="buy", intent="next_open_buy",
            status="broker_order_submitted", provider="upbit", venue="upbit",
            asset_class="crypto", risk_context={"exchange": "UPBIT"},
        )
        db.add(item)
        db.flush()
        item_id = item.id
        command = PlaceOrderRequest.model_validate({
            **_order_payload("legacy-paper-recovery"), "execution_plan_item_id": item_id,
        })
        order = BrokerOrder(
            broker_name="upbit", mode="paper", idempotency_key=command.idempotency_key,
            symbol=command.symbol, side=command.side, status="submitted",
            execution_plan_item_id=item_id, broker_order_id="paper-ack",
            asset_class="crypto", venue="upbit", quote_notional=command.quote_notional,
            request_metadata={"original_command": command.model_dump(mode="json")},
        )
        db.add(order)
        db.flush()
        order_id = order.id
        db.add(BrokerOrderEvent(
            broker_order_pk=order_id, event_type=broker_orders.DEGRADED_ACK_EVENT_TYPE,
            event_source="submission", normalized_status="submitted",
        ))
        db.commit()

    replay_admitted = Event()
    fence_locked_plan = Event()
    replay_requests_plan = Event()
    original_retry = broker_orders._retry_paper_simulated_fill
    original_normalize = core_handoff_v4._normalize_safe_legacy_watch_authority
    original_get = Session.get

    def retry_after_fence_locks_plan(**kwargs):
        replay_admitted.set()
        assert fence_locked_plan.wait(10)
        return original_retry(**kwargs)

    def pause_fence_with_plan_locked(**kwargs):
        result = original_normalize(**kwargs)
        fence_locked_plan.set()
        assert replay_requests_plan.wait(10)
        return result

    def note_replay_plan_lock(self, entity, ident, **kwargs):
        if entity is ExecutionPlanItem and kwargs.get("with_for_update"):
            replay_requests_plan.set()
        return original_get(self, entity, ident, **kwargs)

    monkeypatch.setattr(broker_orders, "_retry_paper_simulated_fill", retry_after_fence_locks_plan)
    monkeypatch.setattr(core_handoff_v4, "_normalize_safe_legacy_watch_authority", pause_fence_with_plan_locked)
    monkeypatch.setattr(Session, "get", note_replay_plan_lock)

    def recover():
        with session_factory() as db:
            broker_orders._recover_degraded_replay(
                settings=settings, db=db, order=db.get(BrokerOrder, order_id),
            )

    def fence():
        assert replay_admitted.wait(10)
        with session_factory() as db:
            return core_handoff_v4.fence_unproven_legacy_core_buy_authority(
                db=db, execution_plan_item_id=item_id, settings=settings,
            )

    with ThreadPoolExecutor(max_workers=2) as workers:
        recovery = workers.submit(recover)
        fencing = workers.submit(fence)
        recovery.result(timeout=20)
        assert fencing.result(timeout=20) is True

    with session_factory() as db:
        assert db.get(ExecutionPlanItem, item_id).status == "manual_review_required"
        assert db.get(BrokerOrder, order_id).status == "filled"
        assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 1


def test_concurrent_paper_recoveries_preserve_both_position_increments(
    reconciliation_database,
):
    settings, engine = reconciliation_database
    if engine.dialect.name != "postgresql":
        pytest.skip("requires PostgreSQL row locks")
    session_factory = create_session_factory(engine)
    identity = build_position_identity(settings=settings, provider="upbit", exchange="UPBIT")
    with session_factory() as db:
        position = ExecutionPosition(
            mode="paper", provider=identity.provider, account_scope=identity.account_scope,
            exchange=identity.exchange, symbol="KRW-BTC", base_quantity=Decimal("1"),
            average_price=Decimal("100"),
        )
        db.add(position)
        order_ids = []
        for index, quantity, price in ((0, "1", "200"), (1, "2", "300")):
            command = PlaceOrderRequest.model_validate({
                **_order_payload(f"parallel-position-{index}"), "order_type": "limit",
                "quote_notional": None, "base_quantity": quantity, "limit_price": price,
            })
            order = BrokerOrder(
                broker_name="upbit", mode="paper", idempotency_key=command.idempotency_key,
                symbol=command.symbol, side=command.side, status="submitted",
                broker_order_id=f"paper-ack-{index}", asset_class="crypto", venue="upbit",
                base_quantity=command.base_quantity,
                request_metadata={"original_command": command.model_dump(mode="json")},
            )
            db.add(order)
            db.flush()
            order_ids.append(order.id)
            db.add(BrokerOrderEvent(
                broker_order_pk=order.id, event_type=broker_orders.DEGRADED_ACK_EVENT_TYPE,
                event_source="submission", normalized_status="submitted",
            ))
        db.commit()
        position_id = position.id

    both_positions_loaded = Barrier(2, timeout=10)

    def recover(order_id):
        with session_factory() as db:
            # Independent PostgreSQL workers can each cache the pre-fill position.
            cached_position = db.get(ExecutionPosition, position_id)
            assert cached_position.base_quantity == Decimal("1")
            both_positions_loaded.wait()
            broker_orders._recover_degraded_replay(
                settings=settings, db=db, order=db.get(BrokerOrder, order_id),
            )

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(recover, order_id) for order_id in order_ids]
        for future in futures:
            future.result(timeout=20)

    with session_factory() as db:
        assert db.scalar(select(func.count()).select_from(ExecutionFill)) == 2
        assert db.scalar(select(func.sum(ExecutionFill.base_quantity))) == Decimal("3")
        position = db.get(ExecutionPosition, position_id)
        assert position.base_quantity == Decimal("4")  # prior 1 + fills 1 + 2
        assert position.average_price == Decimal("225")  # (100 + 200 + 2 * 300) / 4
