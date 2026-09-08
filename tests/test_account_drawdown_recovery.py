from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.api.schemas.execution import PlaceOrderRequest
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionPlanItemAudit
from app.services import broker_orders
from app.services.account_drawdown_recovery import (
    RECOVERY_OBSERVED_EVENT_TYPE,
    RECOVERY_RELEASED_EVENT_TYPE,
    RECOVERY_RESET_EVENT_TYPE,
    record_healthy_observation,
    recovery_account_fingerprint,
    recovery_idempotency_key,
    recovery_state,
)
from app.services.account_drawdown_gate import AccountDrawdownDecision
from app.services.broker_orders import _verified_exec_agent_market_notional
from test_broker_auth import (
    _build_watch_buy_test_app,
    _create_watch_buy_candidate,
    _daily_drawdown_snapshot,
)


def test_long_recovery_idempotency_key_is_deterministic_and_bounded() -> None:
    source_pk = "source-order-id"
    first = recovery_idempotency_key(
        base_key="x" * 128,
        source_broker_order_pk=source_pk,
        generation=1,
    )
    second = recovery_idempotency_key(
        base_key="x" * 128,
        source_broker_order_pk=source_pk,
        generation=1,
    )

    assert first == second
    assert len(first) <= 128
    assert first.startswith("account-drawdown-recovery:1:")


def test_account_fingerprint_uses_stable_scope_secret_across_broker_key_rotation() -> None:
    before_rotation = Settings(
        kis_app_secret="old-broker-secret",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="stable-scope-secret",
    )
    after_rotation = Settings(
        kis_app_secret="new-broker-secret",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="stable-scope-secret",
    )
    other_account = Settings(
        kis_app_secret="new-broker-secret",
        kis_account_number="87654321",
        account_drawdown_recovery_scope_secret="stable-scope-secret",
    )

    assert recovery_account_fingerprint(before_rotation) == recovery_account_fingerprint(
        after_rotation
    )
    assert recovery_account_fingerprint(after_rotation) != recovery_account_fingerprint(
        other_account
    )


def test_snapshot_max_age_policy_change_resets_healthy_recovery_streak() -> None:
    app, engine, client, _adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-recovery-snapshot-age-policy-change",
    )
    source_order_pk = "source-order-snapshot-age-policy-change"
    first_observed_at = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)
    permissive_settings = app.state.settings.model_copy(
        update={"exec_agent_max_account_snapshot_age_seconds": 600}
    )
    strict_settings = permissive_settings.model_copy(
        update={"exec_agent_max_account_snapshot_age_seconds": 60}
    )

    def healthy_decision(observed_at: datetime) -> AccountDrawdownDecision:
        return AccountDrawdownDecision(
            allowed=True,
            gate_name=None,
            reason=None,
            current_metric=None,
            threshold=None,
            baseline_value=None,
            baseline_timestamp=observed_at.isoformat(),
            sells_risk_reduction_allowed=True,
            summary={"allowed": True},
            payload={"evaluated_at": observed_at.isoformat()},
        )

    with app.state.db_session_factory() as db:
        first_count, first_recorded = record_healthy_observation(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=source_order_pk,
            source_idempotency_key="monitoring:watch_buy:policy-change",
            recovery_generation=1,
            decision=healthy_decision(first_observed_at),
            settings=permissive_settings,
            occurred_at=first_observed_at,
        )
        db.commit()
        second_count, second_recorded = record_healthy_observation(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=source_order_pk,
            source_idempotency_key="monitoring:watch_buy:policy-change",
            recovery_generation=1,
            decision=healthy_decision(first_observed_at + timedelta(seconds=61)),
            settings=strict_settings,
            occurred_at=first_observed_at + timedelta(seconds=61),
        )
        db.commit()
        state = recovery_state(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=source_order_pk,
        )

    assert (first_count, first_recorded) == (1, True)
    assert (second_count, second_recorded) == (0, False)
    assert state.healthy_observation_count == 0
    assert state.last_unhealthy_at == first_observed_at + timedelta(seconds=61)
    engine.dispose()


def test_recovery_child_keeps_verified_exec_agent_market_notional_lineage() -> None:
    logical_key = "execution-job:approved-market-order"
    plan_item = SimpleNamespace(
        id="plan-item-1",
        risk_context={
            "exec_agent_decision": {
                "version": "short-term-exec-agent-v1",
                "action": "approve",
                "reason_code": "approved",
                "execution_plan_item_id": "plan-item-1",
                "idempotency_key": logical_key,
                "quantity": 2,
                "sizing_price": "100.0000",
                "reference_price": "100.0000",
                "approved_notional": "200.0000",
            }
        },
    )
    command = PlaceOrderRequest(
        idempotency_key=f"{logical_key}:recovery:1",
        execution_plan_item_id="plan-item-1",
        provider="kis",
        asset_class="stock",
        symbol="005930",
        side="buy",
        quantity=2,
        order_type="market",
        time_in_force="day",
        exchange="KRX",
        metadata={
            "account_drawdown_recovery": {
                "policy_version": 1,
                "source_idempotency_key": logical_key,
                "source_broker_order_pk": "source-order-id",
                "recovery_generation": 1,
            }
        },
    )

    assert _verified_exec_agent_market_notional(
        command=command,
        plan_item=plan_item,
    ) == Decimal("200.0000")

    forged_command = command.model_copy(
        update={"idempotency_key": "unrelated-key:recovery:1"}
    )
    assert _verified_exec_agent_market_notional(
        command=forged_command,
        plan_item=plan_item,
    ) is None


def test_recovery_state_release_wins_when_audits_share_all_timestamps() -> None:
    app, engine, client, _adapter = _build_watch_buy_test_app()
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-recovery-audit-timestamp-tie",
    )
    source_order_pk = "source-order-timestamp-tie"
    occurred_at = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)

    with app.state.db_session_factory() as db:
        db.add_all(
            [
                ExecutionPlanItemAudit(
                    id="f" * 36,
                    execution_plan_item_id=item_id,
                    event_type=RECOVERY_OBSERVED_EVENT_TYPE,
                    source="broker_order_service",
                    summary={
                        "source_broker_order_pk": source_order_pk,
                        "healthy_observation_count": 2,
                        "snapshot_observed_at": occurred_at.isoformat(),
                    },
                    occurred_at=occurred_at,
                    created_at=occurred_at,
                    updated_at=occurred_at,
                ),
                ExecutionPlanItemAudit(
                    id="0" * 36,
                    execution_plan_item_id=item_id,
                    event_type=RECOVERY_RELEASED_EVENT_TYPE,
                    source="broker_order_service",
                    summary={
                        "source_broker_order_pk": source_order_pk,
                        "healthy_observation_count": 2,
                        "snapshot_observed_at": occurred_at.isoformat(),
                        "recovery_idempotency_key": "watch:recovery:1",
                    },
                    occurred_at=occurred_at,
                    created_at=occurred_at,
                    updated_at=occurred_at,
                ),
            ]
        )
        db.commit()

        state = recovery_state(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=source_order_pk,
        )

    assert state.released is True
    assert state.recovery_idempotency_key == "watch:recovery:1"
    engine.dispose()


def test_recovery_reset_snapshot_is_a_monotonic_watermark_for_linked_and_unlinked() -> None:
    app, engine, client, _adapter = _build_watch_buy_test_app(
        account_drawdown_recovery_min_observation_interval_seconds=60,
    )
    item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-recovery-reset-snapshot-watermark",
    )
    latest_seen_snapshot_at = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)
    cached_reset_snapshot_at = latest_seen_snapshot_at - timedelta(seconds=60)
    reset_occurred_at = latest_seen_snapshot_at + timedelta(seconds=60)
    linked_source_pk = "linked-reset-watermark-source"

    def healthy_decision() -> AccountDrawdownDecision:
        return AccountDrawdownDecision(
            allowed=True,
            gate_name=None,
            reason=None,
            current_metric=None,
            threshold=None,
            baseline_value=None,
            baseline_timestamp=latest_seen_snapshot_at.isoformat(),
            sells_risk_reduction_allowed=True,
            summary={"allowed": True},
            payload={"evaluated_at": latest_seen_snapshot_at.isoformat()},
        )

    with app.state.db_session_factory() as db:
        unlinked_source = BrokerOrder(
            broker_name="kis",
            execution_plan_item_id=None,
            mode=app.state.settings.runtime_mode,
            idempotency_key="unlinked-reset-watermark-source",
            symbol="005930",
            side="buy",
            status="rejected",
            quantity=1,
            asset_class="stock",
        )
        db.add(unlinked_source)
        db.flush()
        db.add_all(
            [
                ExecutionPlanItemAudit(
                    execution_plan_item_id=item_id,
                    event_type=RECOVERY_OBSERVED_EVENT_TYPE,
                    source="broker_order_service",
                    summary={
                        "source_broker_order_pk": linked_source_pk,
                        "healthy_observation_count": 1,
                        "snapshot_observed_at": latest_seen_snapshot_at.isoformat(),
                    },
                    occurred_at=latest_seen_snapshot_at,
                    created_at=latest_seen_snapshot_at,
                    updated_at=latest_seen_snapshot_at,
                ),
                ExecutionPlanItemAudit(
                    execution_plan_item_id=item_id,
                    event_type=RECOVERY_RESET_EVENT_TYPE,
                    source="broker_order_service",
                    summary={
                        "source_broker_order_pk": linked_source_pk,
                        "previous_healthy_observation_count": 1,
                        "snapshot_observed_at": cached_reset_snapshot_at.isoformat(),
                    },
                    occurred_at=reset_occurred_at,
                    created_at=reset_occurred_at,
                    updated_at=reset_occurred_at,
                ),
                BrokerOrderEvent(
                    broker_order_pk=unlinked_source.id,
                    event_type=RECOVERY_OBSERVED_EVENT_TYPE,
                    event_source="system",
                    normalized_status="rejected",
                    payload={
                        "source_broker_order_pk": unlinked_source.id,
                        "healthy_observation_count": 1,
                        "snapshot_observed_at": latest_seen_snapshot_at.isoformat(),
                    },
                    observed_at=latest_seen_snapshot_at,
                    created_at=latest_seen_snapshot_at,
                    updated_at=latest_seen_snapshot_at,
                ),
                BrokerOrderEvent(
                    broker_order_pk=unlinked_source.id,
                    event_type=RECOVERY_RESET_EVENT_TYPE,
                    event_source="system",
                    normalized_status="rejected",
                    payload={
                        "source_broker_order_pk": unlinked_source.id,
                        "previous_healthy_observation_count": 1,
                        "snapshot_observed_at": cached_reset_snapshot_at.isoformat(),
                    },
                    observed_at=reset_occurred_at,
                    created_at=reset_occurred_at,
                    updated_at=reset_occurred_at,
                ),
            ]
        )
        db.commit()

        linked_state = recovery_state(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=linked_source_pk,
        )
        unlinked_state = recovery_state(
            db=db,
            execution_plan_item_id=None,
            source_broker_order_pk=unlinked_source.id,
        )
        linked_count, linked_recorded = record_healthy_observation(
            db=db,
            execution_plan_item_id=item_id,
            source_broker_order_pk=linked_source_pk,
            source_idempotency_key="monitoring:watch_buy:reset-watermark",
            recovery_generation=1,
            decision=healthy_decision(),
            settings=app.state.settings,
            occurred_at=reset_occurred_at + timedelta(seconds=1),
        )
        unlinked_count, unlinked_recorded = record_healthy_observation(
            db=db,
            execution_plan_item_id=None,
            source_broker_order_pk=unlinked_source.id,
            source_idempotency_key=unlinked_source.idempotency_key,
            recovery_generation=1,
            decision=healthy_decision(),
            settings=app.state.settings,
            occurred_at=reset_occurred_at + timedelta(seconds=1),
        )

    assert linked_state.last_snapshot_observed_at == latest_seen_snapshot_at
    assert unlinked_state.last_snapshot_observed_at == latest_seen_snapshot_at
    assert (linked_count, linked_recorded) == (0, False)
    assert (unlinked_count, unlinked_recorded) == (0, False)
    engine.dispose()


def test_unlinked_recovery_preflight_honors_event_interval_and_reset_cooldown() -> None:
    app, engine, _client, adapter = _build_watch_buy_test_app(
        account_drawdown_gate_enabled=True,
        account_drawdown_recovery_enabled=True,
        account_drawdown_recovery_cooldown_seconds=300,
        account_drawdown_recovery_min_observation_interval_seconds=60,
    )
    observed_at = datetime.now(UTC)
    recovery_scope = broker_orders._account_drawdown_recovery_scope(
        settings=app.state.settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
    )

    with app.state.db_session_factory() as db:
        source = BrokerOrder(
            broker_name="kis",
            execution_plan_item_id=None,
            mode=app.state.settings.runtime_mode,
            idempotency_key="unlinked-preflight-event-state",
            symbol="005930",
            side="buy",
            status="rejected",
            quantity=1,
            asset_class="stock",
            limit_price=Decimal("71000.00"),
            request_metadata={
                "account_drawdown_recovery_policy": {
                    "policy_version": 1,
                    "recovery_scope": recovery_scope,
                }
            },
            last_error_classification="validation",
            last_error_code="daily_total_asset_drawdown_pct",
            last_error_retryable=False,
            last_broker_sync_at=observed_at - timedelta(hours=1),
        )
        db.add(source)
        db.flush()
        db.add(
            BrokerOrderEvent(
                broker_order_pk=source.id,
                event_type=RECOVERY_OBSERVED_EVENT_TYPE,
                event_source="system",
                normalized_status="rejected",
                payload={
                    "source_broker_order_pk": source.id,
                    "healthy_observation_count": 1,
                    "snapshot_observed_at": observed_at.isoformat(),
                },
                observed_at=observed_at,
            )
        )
        db.commit()

        interval = broker_orders.inspect_account_drawdown_recovery(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            idempotency_key="monitoring:watch_buy:unrelated-plan-item",
            execution_plan_item_id="unrelated-plan-item",
            symbol="000660",
            now=observed_at + timedelta(seconds=30),
        )
        assert interval.block_reason == "account_drawdown_recovery_observation_interval"
        assert interval.retry_after_at == observed_at + timedelta(seconds=60)

        reset_at = observed_at + timedelta(seconds=31)
        db.add(
            BrokerOrderEvent(
                broker_order_pk=source.id,
                event_type=RECOVERY_RESET_EVENT_TYPE,
                event_source="system",
                normalized_status="rejected",
                payload={
                    "source_broker_order_pk": source.id,
                    "previous_healthy_observation_count": 1,
                    "snapshot_observed_at": reset_at.isoformat(),
                },
                observed_at=reset_at,
            )
        )
        db.commit()

        cooldown = broker_orders.inspect_account_drawdown_recovery(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            idempotency_key="monitoring:watch_buy:unrelated-plan-item",
            execution_plan_item_id="unrelated-plan-item",
            symbol="000660",
            now=reset_at + timedelta(seconds=1),
        )
        assert cooldown.block_reason == "account_drawdown_recovery_cooldown"
        assert cooldown.retry_after_at == reset_at + timedelta(seconds=300)

    engine.dispose()


def test_buy_rechecks_loss_latch_created_while_waiting_for_advisory_lock(
    monkeypatch,
) -> None:
    app, engine, client, adapter = _build_watch_buy_test_app(
        account_drawdown_gate_enabled=True,
        account_drawdown_recovery_enabled=True,
        account_drawdown_recovery_cooldown_seconds=300,
    )
    source_item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-advisory-lock-loss-latch-race",
    )
    blocked_at = datetime.now(UTC)
    lock_calls = 0
    call_order: list[str] = []
    original_authenticate = broker_orders.ensure_authenticated_broker_session

    def track_authentication(**kwargs):
        call_order.append("authenticate")
        return original_authenticate(**kwargs)

    def create_competing_loss_latch_after_lock_wait(
        *,
        settings,
        db,
        adapter,
        command,
    ) -> None:
        call_order.append("account_lock")
        recovery_scope = broker_orders._account_drawdown_recovery_scope(
            settings=settings,
            provider=adapter.provider,
            asset_class="stock",
            exchange="KRX",
        )
        del command
        nonlocal lock_calls
        lock_calls += 1
        db.add(
            BrokerOrder(
                execution_plan_item_id=source_item_id,
                broker_name="kis",
                mode="paper",
                idempotency_key=f"monitoring:watch_buy:{source_item_id}",
                symbol="005930",
                side="buy",
                status="rejected",
                quantity=2,
                asset_class="stock",
                limit_price=Decimal("71000.00"),
                request_metadata={
                    "account_drawdown_recovery_policy": {
                        "policy_version": 1,
                        "recovery_scope": recovery_scope,
                    }
                },
                response_metadata={
                    "account_drawdown_gate": {
                        "gate_name": "daily_total_asset_drawdown_pct",
                    }
                },
                last_error_classification="validation",
                last_error_code="daily_total_asset_drawdown_pct",
                last_error_message="daily drawdown threshold reached",
                last_error_retryable=False,
                last_broker_sync_at=blocked_at,
            )
        )
        db.commit()

    monkeypatch.setattr(
        broker_orders,
        "ensure_authenticated_broker_session",
        track_authentication,
    )
    monkeypatch.setattr(
        broker_orders,
        "_acquire_cross_process_order_intent_lock",
        create_competing_loss_latch_after_lock_wait,
    )

    response = client.post(
        "/orders",
        json={
            "idempotency_key": "buy-waiting-on-account-advisory-lock",
            "provider": "kis",
            "asset_class": "stock",
            "symbol": "000660",
            "side": "buy",
            "quantity": 1,
            "order_type": "limit",
            "limit_price": "100000.00",
            "time_in_force": "day",
            "exchange": "KRX",
        },
    )

    assert lock_calls == 1
    assert call_order == ["authenticate", "account_lock"]
    assert response.status_code == 409
    assert response.json()["block_reason"].startswith("account_drawdown_recovery_")
    assert adapter.order_calls == []
    engine.dispose()


@pytest.mark.parametrize("account_drawdown_gate_enabled", [True, False])
def test_broker_secret_rotation_without_scope_secret_keeps_existing_latch_closed(
    account_drawdown_gate_enabled: bool,
) -> None:
    app, engine, _client, adapter = _build_watch_buy_test_app(
        account_drawdown_gate_enabled=account_drawdown_gate_enabled,
        account_drawdown_recovery_enabled=True,
        account_drawdown_recovery_cooldown_seconds=0,
    )
    source_item_id = _create_watch_buy_candidate(
        _client,
        plan_id="plan-loss-latch-broker-secret-rotation",
    )
    original_settings = app.state.settings
    original_scope = broker_orders._account_drawdown_recovery_scope(
        settings=original_settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
    )
    with app.state.db_session_factory() as db:
        db.add(
            BrokerOrder(
                execution_plan_item_id=source_item_id,
                broker_name="kis",
                mode="paper",
                idempotency_key=f"monitoring:watch_buy:{source_item_id}",
                symbol="005930",
                side="buy",
                status="rejected",
                quantity=2,
                asset_class="stock",
                request_metadata={
                    "account_drawdown_recovery_policy": {
                        "policy_version": 1,
                        "recovery_scope": original_scope,
                    }
                },
                response_metadata={
                    "account_drawdown_gate": {
                        "gate_name": "daily_total_asset_drawdown_pct",
                    }
                },
                last_error_classification="validation",
                last_error_code="daily_total_asset_drawdown_pct",
                last_error_retryable=False,
                last_broker_sync_at=datetime.now(UTC),
            )
        )
        db.commit()

    rotated_settings = original_settings.model_copy(
        update={"kis_app_secret": "rotated-paper-secret"}
    )
    with app.state.db_session_factory() as db:
        result = broker_orders.place_broker_order(
            settings=rotated_settings,
            db=db,
            adapter=adapter,
            command=PlaceOrderRequest(
                idempotency_key="distinct-buy-after-broker-secret-rotation",
                provider="kis",
                asset_class="stock",
                symbol="000660",
                side="buy",
                quantity=1,
                order_type="limit",
                limit_price=Decimal("100000.00"),
                time_in_force="day",
                exchange="KRX",
            ),
        )

    assert result.status_code == 409
    assert result.payload["block_reason"] == "account_drawdown_recovery_scope_mismatch"
    assert adapter.order_calls == []
    engine.dispose()


def test_stable_scope_secret_does_not_leak_latch_across_accounts() -> None:
    app, engine, client, _adapter = _build_watch_buy_test_app(
        account_drawdown_gate_enabled=True,
        account_drawdown_recovery_enabled=True,
    )
    source_item_id = _create_watch_buy_candidate(
        client,
        plan_id="plan-stable-secret-account-isolation",
    )
    account_a = app.state.settings.model_copy(
        update={
            "kis_account_number": "11112222",
            "account_drawdown_recovery_scope_secret": "stable-scope-secret",
        }
    )
    account_b = account_a.model_copy(update={"kis_account_number": "99990000"})
    account_a_scope = broker_orders._account_drawdown_recovery_scope(
        settings=account_a,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
    )
    account_b_scope = broker_orders._account_drawdown_recovery_scope(
        settings=account_b,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
    )
    assert account_a_scope != account_b_scope

    with app.state.db_session_factory() as db:
        db.add(
            BrokerOrder(
                execution_plan_item_id=source_item_id,
                broker_name="kis",
                mode="paper",
                idempotency_key=f"monitoring:watch_buy:{source_item_id}",
                symbol="005930",
                side="buy",
                status="rejected",
                quantity=2,
                asset_class="stock",
                request_metadata={
                    "account_drawdown_recovery_policy": {
                        "policy_version": 1,
                        "recovery_scope": account_a_scope,
                    }
                },
                response_metadata={
                    "account_drawdown_gate": {
                        "gate_name": "daily_total_asset_drawdown_pct",
                    }
                },
                last_error_classification="validation",
                last_error_code="daily_total_asset_drawdown_pct",
                last_error_retryable=False,
                last_broker_sync_at=datetime.now(UTC),
            )
        )
        db.commit()

        source = broker_orders._load_active_account_drawdown_recovery_source_for_scope(
            settings=account_b,
            db=db,
            provider="kis",
            recovery_scope=account_b_scope,
        )

    assert source is None
    engine.dispose()


def test_unlinked_direct_loss_block_latches_lane_and_exact_retry_recovers() -> None:
    app, engine, client, adapter = _build_watch_buy_test_app(
        account_drawdown_gate_enabled=True,
        account_drawdown_recovery_enabled=True,
        account_drawdown_recovery_cooldown_seconds=0,
        account_drawdown_recovery_min_observation_interval_seconds=1,
    )
    blocked_at = datetime.now(UTC)
    adapter.account_snapshot = _daily_drawdown_snapshot(
        drawdown_krw=Decimal("200000"),
        observed_at=blocked_at,
    )
    original_request = {
        # Direct callers are allowed to choose an idempotency key that happens
        # to share an internal monitoring prefix. Recovery matching must still
        # compare every economic field for an unlinked order.
        "idempotency_key": "monitoring:watch_buy:direct-unlinked-loss-latch",
        "provider": "kis",
        "asset_class": "stock",
        "symbol": "005930",
        "side": "buy",
        "quantity": 1,
        "order_type": "limit",
        "limit_price": "71000.00",
        "time_in_force": "day",
        "exchange": "KRX",
    }

    blocked = client.post("/orders", json=original_request)

    assert blocked.status_code == 409
    assert blocked.json()["block_reason"] in {
        "daily_total_asset_drawdown_pct",
        "daily_total_asset_drawdown_krw",
    }
    with app.state.db_session_factory() as db:
        source_before = db.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == original_request["idempotency_key"]
            )
        )
        assert source_before is not None
        immutable_source_state = (
            source_before.status,
            source_before.last_broker_sync_at,
            source_before.created_at,
            source_before.updated_at,
            source_before.last_error_code,
        )

    changed_payload = client.post(
        "/orders",
        json={**original_request, "quantity": 2},
    )
    assert changed_payload.status_code == 200
    assert changed_payload.json()["duplicate"] is True
    assert adapter.order_calls == []
    adapter.account_snapshot = _daily_drawdown_snapshot(
        drawdown_krw=Decimal("90000"),
        observed_at=blocked_at + timedelta(seconds=1),
    )
    distinct = client.post(
        "/orders",
        json={**original_request, "idempotency_key": "direct-distinct-buy-must-wait"},
    )
    assert distinct.status_code == 409
    assert distinct.json()["block_reason"] == "account_drawdown_recovery_pending"
    assert adapter.order_calls == []
    assert client.get("/runtime/status").json()["account_drawdown_gate"]["gated"] is True

    first_healthy_at = blocked_at + timedelta(seconds=2)
    adapter.account_snapshot = _daily_drawdown_snapshot(
        drawdown_krw=Decimal("70000"),
        observed_at=first_healthy_at,
    )
    first_retry = client.post("/orders", json=original_request)
    assert first_retry.status_code == 409
    assert first_retry.json()["block_reason"] == "account_drawdown_recovery_pending"
    same_snapshot_retry = client.post("/orders", json=original_request)
    assert same_snapshot_retry.status_code == 409
    assert same_snapshot_retry.json()["block_reason"] == (
        "account_drawdown_recovery_pending"
    )

    adapter.account_snapshot = _daily_drawdown_snapshot(
        drawdown_krw=Decimal("60000"),
        observed_at=first_healthy_at + timedelta(seconds=2),
    )
    second_retry = client.post("/orders", json=original_request)

    assert second_retry.status_code == 201
    assert adapter.order_calls and len(adapter.order_calls) == 1
    released_gate = client.get("/runtime/status").json()["account_drawdown_gate"]
    assert released_gate["gated"] is False
    assert released_gate["latest_decision"]["source"] == (
        "broker_order_recovery_event"
    )
    assert released_gate["latest_decision"]["event_type"] == (
        "account_drawdown_recovery_released"
    )
    with app.state.db_session_factory() as db:
        source = db.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key == original_request["idempotency_key"]
            )
        )
        child = db.scalar(
            select(BrokerOrder).where(
                BrokerOrder.idempotency_key
                == "monitoring:watch_buy:direct-unlinked-loss-latch:recovery:1"
            )
        )
        assert source is not None
        assert (
            source.status,
            source.last_broker_sync_at,
            source.created_at,
            source.updated_at,
            source.last_error_code,
        ) == immutable_source_state
        assert source.execution_plan_item_id is None
        assert child is not None
        assert child.status == "submitted"
        recovery_events = [
            event.event_type
            for event in source.events
            if event.event_type.startswith("account_drawdown_recovery_")
        ]
        assert recovery_events.count("account_drawdown_recovery_observed") == 2
        assert "account_drawdown_recovery_released" in recovery_events

    engine.dispose()
