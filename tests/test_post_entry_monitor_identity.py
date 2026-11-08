"""Post-entry exits retain family identity when accepted source IDs are long."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.api.schemas.execution import ExecutionPlanItemRequest
from app.db.session import create_session_factory
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem
from app.services import monitoring
from app.services.execution_account_route import bind_kis_account_route
from app.services.plan_item_identity import plan_item_entry_family_key
from test_reconciliation_concurrency import reconciliation_database


EXIT_SUFFIXES = {
    "stop_sell": "post-entry-stop",
    "target_sell": "post-entry-target",
    "max_holding_close_sell": "post-entry-max-holding",
}


def _ensure_monitor(*, settings, db, entry, intent):
    common = {
        "db": db, "entry_item": entry, "quantity": 2,
        "entry_market_date": entry.plan.market_date,
        "entry_market_date_source": "fill", "entry_market_date_status": "authoritative",
        "entry_market_date_precision": "exact",
        "triggered_at": datetime(2026, 11, 9, tzinfo=UTC),
    }
    if intent == "stop_sell":
        return monitoring._ensure_post_entry_stop_monitor(
            **common, stop_price=Decimal("47"), entry_price=Decimal("50"),
        )
    if intent == "target_sell":
        return monitoring._ensure_post_entry_target_monitor(
            **common, settings=settings, entry_price=Decimal("50"),
        )
    return monitoring._ensure_post_entry_max_holding_monitor(
        **common, max_holding_sessions=2, krx_trading_day=lambda value: value.weekday() < 5,
    )


@pytest.mark.parametrize("suffix", EXIT_SUFFIXES.values())
def test_existing_post_entry_source_ids_remain_exact(suffix):
    for length in range(1, 128 - len(suffix)):
        item = ExecutionPlanItem(source_plan_item_id="x" * length)
        assert monitoring._derived_source_plan_item_id(item, suffix) == f"{item.source_plan_item_id}:{suffix}"
    assert monitoring._derived_source_plan_item_id(ExecutionPlanItem(), suffix) is None


@pytest.mark.parametrize("intent", EXIT_SUFFIXES)
@pytest.mark.parametrize("source_length", [8, 128])
def test_post_entry_monitor_persists_and_reuses_its_family(
    reconciliation_database, intent, source_length,
):
    settings, engine = reconciliation_database
    settings = settings.model_copy(update={
        "kis_account_number": "12345678",
        "account_drawdown_recovery_scope_secret": "post-entry-identity-test-secret",
    })
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="post-entry-identities", source="test", mode="paper",
            market_date=date(2026, 11, 9), status="accepted",
        )
        entries = []
        for index, final_character in enumerate(("x", "y")):
            source = "x" * (source_length - 1) + final_character
            incoming = ExecutionPlanItemRequest(
                symbol="005930", side="buy", intent="next_open_buy", source_plan_item_id=source,
            )
            risk, policy = bind_kis_account_route(
                settings=settings, provider="kis", risk_context={"exchange": "KRX"},
                quantity_policy={"kind": "shares", "quantity": 2},
            )
            entries.append(ExecutionPlanItem(
                plan=plan, plan_item_index=index, symbol=incoming.symbol,
                side="buy", intent=incoming.intent, status="filled",
                source_plan_item_id=source, decision_agent_name="signal-decider",
                provider="kis", venue="KRX", asset_class="stock",
                risk_context=risk, quantity_policy=policy,
            ))
        db.add(plan)
        db.flush()
        monitors = []
        for entry in entries:
            monitor, created = _ensure_monitor(settings=settings, db=db, entry=entry, intent=intent)
            assert created == 1
            monitors.append(monitor)
        db.commit()
        plan_id = plan.id
        assert len({item.source_plan_item_id for item in monitors}) == 2
        assert all(len(item.source_plan_item_id) <= 128 for item in monitors)
        original_keys = {
            monitor.id: monitoring._monitoring_order_idempotency_key(
                item=monitor, is_watch_buy=False, retry_source_order_pk=None,
            ) for monitor in monitors
        }

    with session_factory() as db:
        plan = db.get(ExecutionPlan, plan_id)
        for entry in [item for item in plan.items if item.side == "buy"]:
            monitor, _changed = _ensure_monitor(settings=settings, db=db, entry=entry, intent=intent)
            assert monitor.id in original_keys
            assert plan_item_entry_family_key(monitor) == f"source:{entry.source_plan_item_id}"
            assert monitor.risk_context["entry_context"]["source_plan_item_id"] == entry.source_plan_item_id
            assert monitor.risk_context["entry_context"]["execution_plan_item_id"] == entry.id
            assert monitor.source_plan_item_id == monitoring._derived_source_plan_item_id(entry, EXIT_SUFFIXES[intent])
            assert monitoring._monitoring_order_idempotency_key(
                item=monitor, is_watch_buy=False, retry_source_order_pk=None,
            ) == original_keys[monitor.id]
            if intent == "target_sell":
                monitor.status = "broker_order_rejected"
                monitor.broker_orders.append(BrokerOrder(
                    broker_name="kis", mode="paper", idempotency_key=original_keys[monitor.id],
                    symbol=monitor.symbol, side="sell", status="rejected", quantity=2,
                    last_error_retryable=False,
                ))
                db.commit()
                assert _ensure_monitor(settings=settings, db=db, entry=entry, intent=intent) == (None, 0)
        db.commit()
        assert db.scalar(select(func.count()).select_from(ExecutionPlanItem)) == 4
