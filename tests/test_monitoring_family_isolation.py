from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
)
from app.services.execution_account_route import (
    bind_execution_account_scope,
    bind_kis_account_route,
    build_execution_account_scope_binding,
)
from app.services import monitoring as monitoring_service, trigger_time_exec_agent
from app.services.monitoring import (
    _append_held_active_sell_monitor_reconciliations,
    _append_held_stop_sell_fallbacks,
    _canonicalize_loaded_active_watch_buys,
    _evaluate_monitoring_cycle,
    _has_unexited_buy_order_for_symbol,
    _monitored_item_filter,
    _monitoring_checkpoint_execution_scope,
    _prepare_due_max_holding_items,
    _reactivate_retryable_protective_stops,
    _repair_unprotected_stop_coverage,
    _sell_monitor_route_key,
    materialize_monitoring_plan_items,
)
from app.services.position_identity import build_position_identity, execution_scope_payload
from app.services.plan_item_identity import plan_item_entry_family_key
from app.services.runtime_transitions import ACTIVE_SELL_MONITORING_STATUSES


def _settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-family-secret-at-least-32-bytes",
    )


def _session(settings: Settings):  # type: ignore[no-untyped-def]
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    factory = create_session_factory(engine)
    return engine, factory, factory()


def _plan(*, key: str, accepted_at: datetime) -> ExecutionPlan:
    return ExecutionPlan(
        external_plan_id=f"plan-{key}",
        source="test",
        mode="paper",
        status="accepted",
        market_date=accepted_at.date(),
        accepted_at=accepted_at,
    )


def _bound_item_payloads(
    settings: Settings,
    *,
    quantity: int,
    family_key: str | None,
    extra_risk: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    quantity_policy: dict[str, object] = {"kind": "shares", "quantity": quantity}
    if family_key is not None:
        quantity_policy["linked_entry_item_key"] = family_key
    risk_context = {"exchange": "KRX", **(extra_risk or {})}
    bound_risk, bound_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    assert bound_risk is not None
    assert bound_policy is not None
    return bound_risk, bound_policy


def _sell_item(
    settings: Settings,
    *,
    item_id: str,
    family_key: str | None,
    accepted_at: datetime,
    intent: str = "stop_sell",
    status: str = "stop_sell_monitoring",
    trigger_price: str = "90",
) -> ExecutionPlanItem:
    extra_risk = (
        {"close_exit_date": "2026-09-01"}
        if intent == "max_holding_close_sell"
        else {}
    )
    risk_context, quantity_policy = _bound_item_payloads(
        settings,
        quantity=10,
        family_key=family_key,
        extra_risk=extra_risk,
    )
    return ExecutionPlanItem(
        id=item_id,
        plan=_plan(key=item_id, accepted_at=accepted_at),
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent=intent,
        status=status,
        provider="kis",
        venue="KRX",
        asset_class="stock",
        trigger_price=Decimal(trigger_price),
        decision_agent_name="signal-decider",
        source_plan_item_id=f"{item_id}:exit",
        quantity_policy=quantity_policy,
        risk_context=risk_context,
    )


def _position(settings: Settings) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    return ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=20,
        average_price=Decimal("100"),
        updated_from="test",
        last_reconciled_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
    )


def _scoped_retry_order(
    settings: Settings,
    *,
    item: ExecutionPlanItem,
    quantity: int,
) -> BrokerOrder:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    return BrokerOrder(
        execution_plan_item_id=item.id,
        broker_name="kis",
        mode="paper",
        idempotency_key=f"retry-{item.id}",
        symbol=item.symbol,
        side="sell",
        status="retry_pending",
        quantity=quantity,
        venue="KRX",
        asset_class="stock",
        request_metadata={
            "exchange": "KRX",
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue="KRX",
                asset_class="stock",
            ),
        },
        last_error_code="KIOK0320",
        last_error_message="market is not open",
        last_error_retryable=True,
    )


def _scoped_filled_order(
    settings: Settings,
    *,
    order_id: str,
    symbol: str,
    side: str,
    created_at: datetime,
) -> BrokerOrder:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    return BrokerOrder(
        id=order_id,
        broker_name="kis",
        mode="paper",
        idempotency_key=order_id,
        symbol=symbol,
        side=side,
        status="filled",
        quantity=10,
        venue="KRX",
        asset_class="stock",
        created_at=created_at,
        request_metadata={
            "exchange": "KRX",
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue="KRX",
                asset_class="stock",
            ),
        },
    )


def _fill(
    *,
    order: BrokerOrder,
    fill_id: str,
    side: str,
    quantity: int,
    filled_at: datetime,
) -> ExecutionFill:
    return ExecutionFill(
        id=fill_id,
        idempotency_key=fill_id,
        broker_order=order,
        symbol=order.symbol,
        side=side,
        quantity=quantity,
        price=Decimal("100"),
        filled_at=filled_at,
    )


def test_newer_same_symbol_stop_never_supersedes_another_entry_family() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    older = _sell_item(
        settings,
        item_id="family-a-stop",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        trigger_price="90",
    )
    newer = _sell_item(
        settings,
        item_id="family-b-stop",
        family_key="family-b-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        trigger_price="80",
    )
    db.add_all([older.plan, newer.plan, _position(settings)])
    db.commit()

    reconciled = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=[newer],
    )

    assert {item.id for item in reconciled} == {older.id, newer.id}
    assert older.status == "stop_sell_monitoring"
    assert newer.status == "stop_sell_monitoring"
    db.close()
    engine.dispose()


@pytest.mark.parametrize("status", sorted(ACTIVE_SELL_MONITORING_STATUSES))
def test_held_position_appends_older_plan_for_every_live_sell_status(
    status: str,
) -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    if status.startswith("target_sell_"):
        intent = "target_sell"
    elif status.startswith("max_holding_close_sell_"):
        intent = "max_holding_close_sell"
    else:
        intent = "stop_sell"
    older = _sell_item(
        settings,
        item_id=f"older-{status}",
        family_key=f"older-family-{status}",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        intent=intent,
        status=status,
    )
    selected_risk, selected_policy = _bound_item_payloads(
        settings,
        quantity=1,
        family_key=f"selected-watch-{status}",
        extra_risk={"target_price": "110", "stop_price": "95"},
    )
    selected_watch = ExecutionPlanItem(
        id=f"selected-watch-{status}",
        plan=_plan(
            key=f"selected-watch-{status}",
            accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        ),
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        trigger_price=Decimal("100"),
        source_plan_item_id=f"selected-watch-family-{status}",
        quantity_policy=selected_policy,
        risk_context=selected_risk,
    )
    db.add_all([older.plan, selected_watch.plan, _position(settings)])
    db.commit()

    reconciled = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=[selected_watch],
    )

    assert {item.id for item in reconciled} == {selected_watch.id, older.id}
    assert older.status == status
    db.close()
    engine.dispose()


def test_active_sell_replacement_is_limited_to_the_same_explicit_family() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    older = _sell_item(
        settings,
        item_id="older-family-a-stop",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
    )
    newer = _sell_item(
        settings,
        item_id="newer-family-a-stop",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
    )
    db.add_all([older.plan, newer.plan, _position(settings)])
    db.commit()

    reconciled = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=[newer],
    )

    assert [item.id for item in reconciled] == [newer.id]
    assert older.status == "superseded"
    assert newer.status == "stop_sell_monitoring"
    db.close()
    engine.dispose()


def test_ambiguous_sell_claims_cannot_replace_exact_family_authority() -> None:
    settings = _settings()
    incumbent = _sell_item(
        settings,
        item_id="exact-family-incumbent",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
    )
    ambiguous = _sell_item(
        settings,
        item_id="ambiguous-family-newer",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        trigger_price="80",
    )
    incumbent.source_plan_item_id = "shared-direct-exit-row"
    ambiguous.source_plan_item_id = "shared-direct-exit-row"
    ambiguous.risk_context = {
        **dict(ambiguous.risk_context or {}),
        "entry_context": {"source_plan_item_id": "family-b-entry"},
    }

    canonical = _canonicalize_loaded_active_watch_buys(
        [incumbent, ambiguous],
        settings=settings,
    )

    assert plan_item_entry_family_key(incumbent) == "source:family-a-entry"
    assert plan_item_entry_family_key(ambiguous) is None
    assert {item.id for item in canonical} == {incumbent.id, ambiguous.id}
    assert incumbent.status == "stop_sell_monitoring"
    assert incumbent.trigger_price == Decimal("90")
    assert ambiguous.status == "stop_sell_monitoring"


def test_non_kis_sell_replan_never_crosses_authenticated_account_scope() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)

    def item(*, item_id: str, account_id: str, trigger_price: str) -> ExecutionPlanItem:
        binding = build_execution_account_scope_binding(
            settings=settings,
            provider="alpaca",
            exchange="US",
            broker_account_id=account_id,
        )
        risk_context, quantity_policy = bind_execution_account_scope(
            provider="alpaca",
            risk_context={"exchange": "US"},
            quantity_policy={"kind": "shares", "quantity": 1},
            binding=binding,
        )
        return ExecutionPlanItem(
            id=item_id,
            plan=_plan(
                key=item_id,
                accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
            ),
            plan_item_index=0,
            symbol="AAPL",
            side="sell",
            intent="stop_sell",
            status="pending",
            provider="alpaca",
            venue="ALPACA",
            asset_class="us_equity",
            trigger_price=Decimal(trigger_price),
            source_plan_item_id="same-core-source-row",
            quantity_policy=quantity_policy,
            risk_context=risk_context,
        )

    existing = item(
        item_id="alpaca-account-a-stop",
        account_id="alpaca-account-a",
        trigger_price="90",
    )
    existing.status = "stop_sell_monitoring"
    replanned = item(
        item_id="alpaca-account-b-stop",
        account_id="alpaca-account-b",
        trigger_price="80",
    )
    db.add(existing.plan)
    db.commit()
    db.add(replanned.plan)
    db.flush()

    materialize_monitoring_plan_items(replanned.plan, db=db)

    assert existing.status == "stop_sell_monitoring"
    assert existing.trigger_price == Decimal("90")
    assert replanned.status == "stop_sell_monitoring"
    assert _sell_monitor_route_key(existing) != _sell_monitor_route_key(replanned)
    db.close()
    engine.dispose()


def test_unbound_non_kis_sell_monitor_has_no_dedupe_route_key() -> None:
    item = ExecutionPlanItem(
        provider="alpaca",
        venue="ALPACA",
        asset_class="us_equity",
        symbol="AAPL",
        side="sell",
        intent="stop_sell",
        risk_context={"exchange": "US"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    assert _sell_monitor_route_key(item) is None


def test_watch_buy_canonicalization_keeps_one_authority_across_entry_families() -> None:
    settings = _settings()
    older_at = datetime(2026, 9, 9, 12, tzinfo=UTC)
    newer_at = datetime(2026, 9, 10, 12, tzinfo=UTC)
    risk_a, policy_a = _bound_item_payloads(
        settings,
        quantity=10,
        family_key=None,
    )
    risk_b, policy_b = _bound_item_payloads(
        settings,
        quantity=10,
        family_key=None,
    )
    family_a = ExecutionPlanItem(
        id="family-a-buy",
        plan=_plan(key="family-a-buy", accepted_at=older_at),
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        source_plan_item_id="family-a-entry",
        risk_context=risk_a,
        quantity_policy=policy_a,
    )
    family_b = ExecutionPlanItem(
        id="family-b-buy",
        plan=_plan(key="family-b-buy", accepted_at=newer_at),
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        source_plan_item_id="family-b-entry",
        risk_context=risk_b,
        quantity_policy=policy_b,
    )

    canonical = _canonicalize_loaded_active_watch_buys(
        [family_a, family_b],
        settings=settings,
    )

    assert [item.id for item in canonical] == [family_b.id]


def test_ambiguous_unlinked_active_sells_remain_protective_and_request_review() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    older = _sell_item(
        settings,
        item_id="legacy-stop-a",
        family_key=None,
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
    )
    newer = _sell_item(
        settings,
        item_id="legacy-stop-b",
        family_key=None,
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
    )
    db.add_all([older.plan, newer.plan, _position(settings)])
    db.commit()

    reconciled = _append_held_active_sell_monitor_reconciliations(
        settings=settings,
        db=db,
        selected_items=[newer],
    )

    assert {item.id for item in reconciled} == {older.id, newer.id}
    assert older.status == "stop_sell_monitoring"
    assert newer.status == "stop_sell_monitoring"
    for item in (older, newer):
        event = item.risk_context["last_monitoring_event"]
        assert event["reason"] == "ambiguous_entry_family"
        assert event["action"] == "manual_review"
    db.close()
    engine.dispose()


def test_due_max_holding_promotion_is_independent_per_entry_family() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    family_a = _sell_item(
        settings,
        item_id="family-a-max",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        intent="max_holding_close_sell",
        status="max_holding_close_sell_pending_config",
    )
    family_b = _sell_item(
        settings,
        item_id="family-b-max",
        family_key="family-b-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        intent="max_holding_close_sell",
        status="max_holding_close_sell_pending_config",
    )
    position = _position(settings)
    db.add_all([family_a.plan, family_b.plan, position])
    db.commit()

    prepared = _prepare_due_max_holding_items(
        settings=settings,
        db=db,
        items=[family_a, family_b],
        positions_by_symbol={position.symbol: position},
        triggered_at=datetime(2026, 9, 10, 13, tzinfo=UTC),
    )

    assert {item.id for item in prepared} == {family_a.id, family_b.id}
    assert family_a.status == "max_holding_close_sell_monitoring"
    assert family_b.status == "max_holding_close_sell_monitoring"
    db.close()
    engine.dispose()


def test_ambiguous_unlinked_max_holding_items_are_not_mutated_or_promoted() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    older = _sell_item(
        settings,
        item_id="legacy-max-a",
        family_key=None,
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        intent="max_holding_close_sell",
        status="max_holding_close_sell_pending_config",
    )
    newer = _sell_item(
        settings,
        item_id="legacy-max-b",
        family_key=None,
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        intent="max_holding_close_sell",
        status="max_holding_close_sell_pending_config",
    )
    position = _position(settings)
    db.add_all([older.plan, newer.plan, position])
    db.commit()

    prepared = _prepare_due_max_holding_items(
        settings=settings,
        db=db,
        items=[older, newer],
        positions_by_symbol={position.symbol: position},
        triggered_at=datetime(2026, 9, 10, 13, tzinfo=UTC),
    )

    assert prepared == []
    assert older.status == "max_holding_close_sell_pending_config"
    assert newer.status == "max_holding_close_sell_pending_config"
    assert older.risk_context["last_monitoring_event"]["reason"] == (
        "ambiguous_entry_family"
    )
    assert newer.risk_context["last_monitoring_event"]["reason"] == (
        "ambiguous_entry_family"
    )
    db.close()
    engine.dispose()


def test_stop_fallback_selection_is_independent_per_explicit_entry_family() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    active_family_a = _sell_item(
        settings,
        item_id="active-family-a-stop",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
    )
    retry_family_b = _sell_item(
        settings,
        item_id="retry-family-b-stop",
        family_key="family-b-entry",
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        status="broker_order_retry_pending",
    )
    db.add_all([active_family_a.plan, retry_family_b.plan, _position(settings)])
    db.commit()

    selected = _append_held_stop_sell_fallbacks(
        settings=settings,
        db=db,
        monitored_item_filter=_monitored_item_filter(),
        selected_items=[active_family_a],
    )

    assert {item.id for item in selected} == {
        active_family_a.id,
        retry_family_b.id,
    }
    db.close()
    engine.dispose()


def test_ambiguous_retry_stop_does_not_compete_with_explicit_families() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    active_family_a = _sell_item(
        settings,
        item_id="active-family-a-stop-with-legacy",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
    )
    ambiguous_retry = _sell_item(
        settings,
        item_id="ambiguous-retry-stop",
        family_key=None,
        accepted_at=datetime(2026, 9, 9, 12, tzinfo=UTC),
        status="broker_order_retry_pending",
    )
    db.add_all([active_family_a.plan, ambiguous_retry.plan, _position(settings)])
    db.commit()

    selected = _append_held_stop_sell_fallbacks(
        settings=settings,
        db=db,
        monitored_item_filter=_monitored_item_filter(),
        selected_items=[active_family_a],
    )

    assert [item.id for item in selected] == [active_family_a.id]
    assert ambiguous_retry.status == "broker_order_retry_pending"
    event = ambiguous_retry.risk_context["last_monitoring_event"]
    assert event["event"] == "protective_stop_fallback_manual_review"
    assert event["reason"] == "ambiguous_entry_family"
    assert event["action"] == "manual_review"
    db.close()
    engine.dispose()


def test_retry_reactivation_preserves_family_and_policy_route_authority() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    item = _sell_item(
        settings,
        item_id="family-bound-retry-stop",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        status="broker_order_retry_pending",
    )
    route_binding = dict(item.risk_context["execution_account_route"])
    item.quantity_policy = {
        "kind": "position_exit_from_entry",
        "linked_entry_item_key": "family-a-entry",
        "execution_account_route": route_binding,
    }
    db.add_all([item.plan, _scoped_retry_order(settings, item=item, quantity=7)])
    db.commit()

    rearmed_ids = _reactivate_retryable_protective_stops(
        settings=settings,
        items=[item],
        db=db,
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
    )

    assert rearmed_ids == {item.id}
    assert item.status == "stop_sell_monitoring"
    assert item.quantity_policy == {
        "kind": "shares",
        "quantity": 7,
        "linked_entry_item_key": "family-a-entry",
        "execution_account_route": route_binding,
    }
    db.close()
    engine.dispose()


def test_protective_stop_reauthorization_records_current_position_evidence() -> None:
    settings = _settings()
    item = _sell_item(
        settings,
        item_id="aged-repair-stop",
        family_key=None,
        accepted_at=datetime(2026, 9, 1, 12, tzinfo=UTC),
    )
    item.decision_agent_name = "post-entry-monitor-repair"
    item.decision_snapshot_id = None
    item.decision_run_id = None
    position = _position(settings)
    position.last_reconciled_at = datetime(2026, 9, 10, 12, tzinfo=UTC)
    evaluated_at = datetime(2026, 9, 10, 12, 1, tzinfo=UTC)

    approved, reason = monitoring_service._authorize_protective_stop_submission(
        settings=settings,
        item=item,
        position=position,
        requested_quantity=7,
        observed_price=Decimal("89"),
        price_observed_at=datetime(2026, 9, 10, 12, 0, 58, tzinfo=UTC),
        evaluated_at=evaluated_at,
    )

    assert approved is True
    assert reason is None
    assert item.status == "stop_sell_monitoring"
    authorization = item.risk_context["protective_stop_reauthorization"]
    assert authorization["status"] == "approved"
    assert authorization["authorization_id"]
    assert authorization["authorization_kind"] == "deterministic_emergency_stop"
    assert authorization["strategy_decision_lineage_status"] == "unavailable"
    assert authorization["position"] == {
        "position_id": position.id,
        "quantity": 20,
        "last_reconciled_at": "2026-09-10T12:00:00+00:00",
        "updated_from": "test",
    }
    assert authorization["request"] == {
        "quantity": 7,
        "observed_price": "89.0000",
        "price_observed_at": "2026-09-10T12:00:58+00:00",
        "fresh_broker_quote_revalidation_required": True,
    }
    order_metadata = monitoring_service._monitoring_order_metadata(
        item=item,
        observed_price=Decimal("89"),
        trigger_reason="stop_sell",
        trigger_metadata=None,
        triggered_at=evaluated_at,
        deterministic_sell_quantity_cap=7,
    )
    assert order_metadata["protective_stop_reauthorization"] == authorization


def test_protective_stop_reauthorization_quarantines_stale_position_truth() -> None:
    settings = _settings().model_copy(update={"runtime_mode": "live"})
    item = _sell_item(
        settings,
        item_id="stale-position-repair-stop",
        family_key=None,
        accepted_at=datetime(2026, 9, 1, 12, tzinfo=UTC),
    )
    position = _position(settings)
    position.last_reconciled_at = datetime(2026, 9, 10, 11, tzinfo=UTC)

    approved, reason = monitoring_service._authorize_protective_stop_submission(
        settings=settings,
        item=item,
        position=position,
        requested_quantity=7,
        observed_price=Decimal("89"),
        price_observed_at=datetime(2026, 9, 10, 12, 0, 58, tzinfo=UTC),
        evaluated_at=datetime(2026, 9, 10, 12, 1, tzinfo=UTC),
    )

    assert approved is False
    assert reason == "protective_stop_position_observation_stale"
    assert item.status == "manual_review_required"
    assert item.risk_context["protective_stop_reauthorization"]["status"] == (
        "manual_review_required"
    )
    assert item.risk_context["last_monitoring_event"] == {
        "event": "protective_stop_reauthorization_manual_review",
        "authorization_id": item.risk_context[
            "protective_stop_reauthorization"
        ]["authorization_id"],
        "reason": "protective_stop_position_observation_stale",
        "position_quantity": 20,
        "requested_quantity": 7,
        "strategy_decision_lineage_status": "unavailable",
        "recorded_at": "2026-09-10T12:01:00+00:00",
    }


def test_local_retry_is_not_live_coverage_and_cloned_repair_preserves_identity() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    retry_stop = _sell_item(
        settings,
        item_id="retry-stop-needing-coverage",
        family_key="family-a-entry",
        accepted_at=datetime(2026, 9, 10, 12, tzinfo=UTC),
        status="broker_order_retry_pending",
    )
    retry_stop.quantity_policy["quantity"] = 20
    retry_stop.quantity_precision = 0
    retry_stop.price_precision = 4
    position = _position(settings)
    db.add_all([retry_stop.plan, position])
    db.commit()

    issues, repaired_count = _repair_unprotected_stop_coverage(
        settings=settings,
        db=db,
        positions_by_symbol={position.symbol: position},
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
    )

    assert repaired_count == 1
    assert all(issue.outcome != "unprotected_position" for issue in issues)
    db.flush()
    repaired_items = db.query(ExecutionPlanItem).filter(
        ExecutionPlanItem.execution_plan_id == retry_stop.execution_plan_id,
        ExecutionPlanItem.id != retry_stop.id,
    ).all()
    assert len(repaired_items) == 1
    repair = repaired_items[0]
    assert repair.quantity_policy == {
        "kind": "shares",
        "quantity": position.quantity,
        "linked_entry_item_key": "family-a-entry",
    }
    assert repair.risk_context["execution_account_route"] == (
        retry_stop.risk_context["execution_account_route"]
    )
    assert repair.provider == retry_stop.provider
    assert repair.venue == retry_stop.venue
    assert repair.asset_class == retry_stop.asset_class
    assert repair.quantity_precision == retry_stop.quantity_precision
    assert repair.price_precision == retry_stop.price_precision
    db.close()
    engine.dispose()


def test_live_regular_buy_authority_veto_precedes_configurable_guardrail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluated_at = datetime(2026, 9, 11, 1, tzinfo=UTC)
    settings = _settings().model_copy(
        update={
            "runtime_mode": "live",
            "exec_agent_enabled": False,
            "market_regime_guard_enabled": False,
        }
    )
    plan = ExecutionPlan(
        external_plan_id="live-entry-line-authority-plan",
        source="test",
        mode="live",
        status="accepted",
        market_date=date(2026, 9, 11),
    )
    item = ExecutionPlanItem(
        id="live-entry-line-authority-item",
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={
            "exchange": "KRX",
            "trigger_direction": "at_or_below",
            "session_policy": {"session": "regular"},
        },
    )
    authority_audit = {
        "schema_version": "trigger_time_live_market_buy_authority_v1",
        "status": "blocked",
        "reason_code": "live_market_breadth_unavailable",
        "evaluated_at": evaluated_at.isoformat(),
    }
    captured: dict[str, object] = {}

    def fake_authority_gate(**kwargs: object) -> tuple[dict[str, object], str]:
        captured.update(kwargs)
        return authority_audit, "fresh live market breadth is unavailable for trigger-time buy"

    monkeypatch.setattr(
        monitoring_service,
        "load_live_market_regime_context",
        lambda **_kwargs: {"market_context_status": "unavailable"},
    )
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "evaluate_live_market_buy_authority_gate",
        fake_authority_gate,
    )
    monkeypatch.setattr(
        monitoring_service,
        "evaluate_market_regime_guardrail",
        lambda **_kwargs: pytest.fail(
            "mandatory live authority must veto before the configurable guardrail"
        ),
    )

    approval = monitoring_service._live_watch_buy_submission_veto(
        settings=settings,
        item=item,
        observed_price=Decimal("71000"),
        price=MonitoringPriceRequest(
            symbol="005930",
            price=Decimal("71000"),
            session_type="regular",
        ),
        quantity=1,
        recorded_at=evaluated_at,
    )

    assert approval is not None
    assert approval.reason == "live_market_breadth_unavailable"
    assert approval.message == (
        "fresh live market breadth is unavailable for trigger-time buy"
    )
    assert approval.reason in monitoring_service.RETRYABLE_WATCH_BUY_APPROVAL_REASONS
    assert approval.details["live_market_buy_authority_gate"] == authority_audit
    assert captured["settings"] is settings
    assert captured["item"] is item
    assert captured["session_type"] == "regular"
    assert captured["evaluated_at"] == evaluated_at


def test_monitoring_checkpoint_scope_uses_canonical_kis_account_identity() -> None:
    settings = _settings()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )

    assert _monitoring_checkpoint_execution_scope(settings=settings) == {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": identity.account_scope,
    }


def test_monitoring_checkpoint_scope_fails_closed_without_stable_live_identity() -> None:
    settings = _settings().model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": None,
        }
    )

    assert _monitoring_checkpoint_execution_scope(settings=settings) is None


def test_monitoring_checkpoint_is_not_written_when_identity_is_missing() -> None:
    settings = _settings().model_copy(
        update={
            "runtime_mode": "live",
            "account_drawdown_recovery_scope_secret": None,
        }
    )
    engine, _factory, db = _session(settings)

    result = _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=object(),  # type: ignore[arg-type]
        prices={},
        symbols={"005930"},
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=True,
    )

    assert result.status_code == 200
    assert db.query(MonitoringCheckpoint).count() == 0
    db.close()
    engine.dispose()


def test_unscoped_monitoring_writer_never_overwrites_active_account_checkpoint() -> None:
    active_settings = _settings().model_copy(update={"runtime_mode": "live"})
    unscoped_settings = active_settings.model_copy(
        update={"account_drawdown_recovery_scope_secret": None}
    )
    identity = build_position_identity(
        settings=active_settings,
        provider="kis",
        exchange="KRX",
    )
    engine, _factory, db = _session(unscoped_settings)
    original_heartbeat = datetime(2026, 9, 10, 1, tzinfo=UTC)
    db.add(
        MonitoringCheckpoint(
            worker_name="intraday_target_stop_monitor",
            mode="live",
            provider="kis",
            account_scope=identity.account_scope,
            status="running",
            last_heartbeat_at=original_heartbeat,
            last_success_at=original_heartbeat,
            details={
                "execution_scope": {
                    "policy_version": 1,
                    "provider": "kis",
                    "account_scope": identity.account_scope,
                }
            },
        )
    )
    db.commit()

    result = _evaluate_monitoring_cycle(
        settings=unscoped_settings,
        db=db,
        adapter=object(),  # type: ignore[arg-type]
        prices={},
        symbols={"005930"},
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=True,
    )

    checkpoint = db.query(MonitoringCheckpoint).one()
    assert result.status_code == 200
    assert checkpoint.provider == "kis"
    assert checkpoint.account_scope == identity.account_scope
    assert checkpoint.last_heartbeat_at is not None
    assert checkpoint.last_heartbeat_at.replace(tzinfo=UTC) == original_heartbeat
    assert checkpoint.details["execution_scope"]["account_scope"] == identity.account_scope
    db.close()
    engine.dispose()


def test_monitoring_checkpoint_writer_rejects_non_integer_scope_version() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    expected_scope = _monitoring_checkpoint_execution_scope(settings=settings)
    assert expected_scope is not None
    malformed_scope = dict(expected_scope)
    malformed_scope["policy_version"] = True

    checkpoint = monitoring_service._update_monitoring_checkpoint(
        settings=settings,
        db=db,
        worker_name="intraday_target_stop_monitor",
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
        status="running",
        details={"execution_scope": malformed_scope},
    )

    assert checkpoint is None
    assert db.query(MonitoringCheckpoint).count() == 0
    db.close()
    engine.dispose()


@pytest.mark.parametrize("max_broker_actions", [0, 1])
def test_every_monitoring_checkpoint_path_persists_canonical_scope(
    max_broker_actions: int,
) -> None:
    settings = _settings().model_copy(
        update={"monitoring_max_broker_actions_per_cycle": max_broker_actions}
    )
    engine, _factory, db = _session(settings)
    expected_scope = _monitoring_checkpoint_execution_scope(settings=settings)

    result = _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=object(),  # type: ignore[arg-type]
        prices={},
        symbols={"005930"},
        triggered_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
        allow_persisted_triggered_without_price=True,
        update_checkpoint_if_idle=True,
    )

    checkpoint = db.query(MonitoringCheckpoint).one()
    assert result.status_code == 200
    assert checkpoint.provider == expected_scope["provider"]
    assert checkpoint.account_scope == expected_scope["account_scope"]
    assert checkpoint.details["execution_scope"] == expected_scope
    assert checkpoint.details["execution_scope_status"] == "available"
    assert checkpoint.details["execution_scope_reason"] is None
    db.close()
    engine.dispose()


def test_monitoring_checkpoint_agent_counts_use_only_selected_account_items() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    current_risk, current_policy = _bound_item_payloads(
        settings,
        quantity=1,
        family_key="current-family",
    )
    foreign_risk, foreign_policy = _bound_item_payloads(
        foreign_settings,
        quantity=1,
        family_key="foreign-family",
    )
    engine, _factory, db = _session(settings)

    def watch_item(
        *,
        item_id: str,
        provider: str,
        risk_context: dict[str, object] | None,
        quantity_policy: dict[str, object] | None,
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan=_plan(
                key=item_id,
                accepted_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
            ),
            plan_item_index=0,
            symbol=(
                "005930"
                if item_id == "current-account-watch"
                else "000660"
                if provider == "kis"
                else "AAPL"
            ),
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            provider=provider,
            venue="KRX" if provider == "kis" else "ALPACA",
            asset_class="stock" if provider == "kis" else "us_equity",
            trigger_price=Decimal("100"),
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    db.add_all(
        [
            watch_item(
                item_id="current-account-watch",
                provider="kis",
                risk_context=current_risk,
                quantity_policy=current_policy,
            ),
            watch_item(
                item_id="foreign-account-watch",
                provider="kis",
                risk_context=foreign_risk,
                quantity_policy=foreign_policy,
            ),
            watch_item(
                item_id="other-provider-watch",
                provider="alpaca",
                risk_context={},
                quantity_policy={"kind": "shares", "quantity": 1},
            ),
        ]
    )
    db.commit()

    result = _evaluate_monitoring_cycle(
        settings=settings,
        db=db,
        adapter=object(),  # type: ignore[arg-type]
        prices={},
        triggered_at=datetime(2026, 9, 11, 2, tzinfo=UTC),
        allow_persisted_triggered_without_price=False,
        update_checkpoint_if_idle=True,
    )

    checkpoint = db.query(MonitoringCheckpoint).one()
    assert result.status_code == 200
    assert checkpoint.details["agent_count_scope"] == "selected_cycle_items"
    assert checkpoint.details["agent_count_item_count"] == 1
    assert checkpoint.details["agent_watching_count"] == 1
    assert checkpoint.details["agent_pending_decision_count"] == 0
    assert checkpoint.details["agent_delayed_count"] == 0
    assert checkpoint.details["agent_rejected_count"] == 0
    assert checkpoint.details["agent_approved_submitted_count"] == 0
    db.close()
    engine.dispose()


def test_partial_sell_fill_preserves_unexited_buy_inventory() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    buy_order = _scoped_filled_order(
        settings,
        order_id="partial-buy-order",
        symbol="005930",
        side="buy",
        created_at=datetime(2026, 9, 10, 1, tzinfo=UTC),
    )
    sell_order = _scoped_filled_order(
        settings,
        order_id="partial-sell-order",
        symbol="005930",
        side="sell",
        created_at=datetime(2026, 9, 10, 2, tzinfo=UTC),
    )
    db.add_all(
        [
            buy_order,
            sell_order,
            _fill(
                order=buy_order,
                fill_id="partial-buy-fill",
                side="buy",
                quantity=10,
                filled_at=datetime(2026, 9, 10, 1, 5, tzinfo=UTC),
            ),
            _fill(
                order=sell_order,
                fill_id="partial-sell-fill",
                side="sell",
                quantity=4,
                filled_at=datetime(2026, 9, 10, 2, 5, tzinfo=UTC),
            ),
        ]
    )
    db.commit()

    assert _has_unexited_buy_order_for_symbol(
        settings=settings,
        db=db,
        symbol="005930",
    )
    db.close()
    engine.dispose()


def test_fill_chronology_overrides_order_creation_chronology() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    sell_order = _scoped_filled_order(
        settings,
        order_id="early-created-sell-order",
        symbol="005930",
        side="sell",
        created_at=datetime(2026, 9, 10, 1, tzinfo=UTC),
    )
    buy_order = _scoped_filled_order(
        settings,
        order_id="late-created-buy-order",
        symbol="005930",
        side="buy",
        created_at=datetime(2026, 9, 10, 2, tzinfo=UTC),
    )
    db.add_all(
        [
            sell_order,
            buy_order,
            _fill(
                order=buy_order,
                fill_id="chronology-buy-fill",
                side="buy",
                quantity=10,
                filled_at=datetime(2026, 9, 10, 1, 5, tzinfo=UTC),
            ),
            _fill(
                order=sell_order,
                fill_id="chronology-sell-fill",
                side="sell",
                quantity=10,
                filled_at=datetime(2026, 9, 10, 2, 5, tzinfo=UTC),
            ),
        ]
    )
    db.commit()

    assert not _has_unexited_buy_order_for_symbol(
        settings=settings,
        db=db,
        symbol="005930",
    )
    db.close()
    engine.dispose()
