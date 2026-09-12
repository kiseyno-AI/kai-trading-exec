from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.broker.base import BrokerConfig
from app.core.config import Settings
from app.models.runtime import (
    BrokerOrder,
    ExecutionFailure,
    ExecutionJob,
    ExecutionPlanItem,
    RuntimeControlState,
)
from app.services import exec_agent, execution_api, execution_jobs, plan_judgment_actions
from app.services.broker_orders import PlaceBrokerOrderResult
from app.services.execution_jobs import dispatch_due_execution_jobs
from app.services.plan_judgment_actions import apply_hourly_plan_judgment_action
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StubBrokerAdapter,
    build_order_placement,
    build_test_app,
    build_token_grant,
)


class _FrozenDateTime(datetime):
    current = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


def _next_open_app(tmp_path, *, plan_id: str):
    adapter = StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id=f"broker-{plan_id}"),
    )
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'exec.db'}",
        runtime_mode="paper",
        kis_app_key="paper-key",
        kis_app_secret="paper-secret",
        kis_account_number="12345678",
        account_drawdown_gate_enabled=False,
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    app, engine = build_test_app(settings=settings, broker_adapter=adapter)
    client = TestClient(app)
    assert client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable dispatch"},
    ).status_code == 200
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": plan_id,
                "source": "operator-import",
            "market_date": "2026-07-28",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "quantity_policy": {"kind": "shares", "quantity": 2},
                    "limit_price": "71000.00",
                    "time_in_force": "day",
                }
            ],
        },
    )
    assert response.status_code == 201
    return app, engine, adapter, response.json()["persisted_execution_item_ids"][0]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def test_execution_job_numeric_parser_rejects_nonfinite_legacy_values() -> None:
    for value in ("NaN", "Infinity", "-Infinity"):
        assert execution_jobs._parse_positive_decimal(value) is None


def test_plan_ingress_and_sync_agent_numeric_parsers_reject_nonfinite_values() -> None:
    for value in ("NaN", "Infinity", "-Infinity"):
        assert execution_api._parse_positive_decimal(value) is None
        assert exec_agent._parse_positive_decimal(value) is None


def test_risk_off_blocks_scheduled_next_open_dispatch_until_confirmed_recovery(
    monkeypatch,
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-next-open-risk-off-dispatch",
    )

    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    risk_off = apply_hourly_plan_judgment_action(
        settings=app.state.settings,
        risk_flags=3,
        risk_reasons=["broad market risk-off"],
    )
    assert risk_off["changed_count"] == 1

    with app.state.db_session_factory() as db:
        blocked_dispatch = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=started_at,
        )
    assert blocked_dispatch.status_code == 200
    assert blocked_dispatch.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item_id))
        assert item is not None
        assert item.status == "watch_expired"
        assert job is not None
        assert job.status == "scheduled"
        assert job.attempts == 0

    _FrozenDateTime.current = started_at + timedelta(seconds=1)
    apply_hourly_plan_judgment_action(settings=app.state.settings, risk_flags=0, risk_reasons=[])
    _FrozenDateTime.current = started_at + timedelta(seconds=2)
    apply_hourly_plan_judgment_action(settings=app.state.settings, risk_flags=0, risk_reasons=[])
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "next_open_buy_scheduled"

    with app.state.db_session_factory() as db:
        resumed_dispatch = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=started_at + timedelta(seconds=2),
        )
    assert resumed_dispatch.status_code == 200
    assert resumed_dispatch.payload["due_job_count"] == 1
    assert resumed_dispatch.payload["completed_job_count"] == 1
    assert len(adapter.order_calls) == 1
    engine.dispose()


def test_dispatch_revalidates_plan_item_status_immediately_before_claim(monkeypatch, tmp_path) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-next-open-claim-race",
    )

    def expire_between_query_and_claim(*, db, job, triggered_at):
        del triggered_at
        job.plan_item.status = "watch_expired"
        db.commit()
        return False

    monkeypatch.setattr(
        execution_jobs,
        "_defer_us_next_open_job_outside_session",
        expire_between_query_and_claim,
    )

    with app.state.db_session_factory() as db:
        response = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=datetime(2026, 7, 28, 0, 0, tzinfo=UTC),
        )

    assert response.status_code == 200
    assert response.payload["completed_job_count"] == 0
    assert response.payload["failed_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item_id))
        assert item is not None
        assert item.status == "watch_expired"
        assert job is not None
        assert job.status == "scheduled"
        assert job.attempts == 0
    engine.dispose()


def test_stale_running_job_does_not_revive_operator_cancelled_plan_item(
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-stale-running-operator-cancelled",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.status = "watch_cancelled"
        job.status = "running"
        job.locked_at = attempted_at - timedelta(minutes=10)
        job.attempts = 1
        db.commit()

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )

    assert result.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        assert item.status == "watch_cancelled"
        assert job.status == "failed"
        assert job.attempts == 1
        assert job.last_error == (
            "stale running job cannot resume because plan item is no longer "
            "in its dispatching state"
        )
    engine.dispose()


def test_stale_running_job_recovers_committed_broker_submission_as_completed(
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-stale-running-submitted-order",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.status = "broker_order_submitted"
        job.status = "running"
        job.locked_at = attempted_at - timedelta(minutes=10)
        job.attempts = 1
        db.add(
            BrokerOrder(
                execution_plan_item_id=item.id,
                broker_name="kis",
                mode="paper",
                idempotency_key=f"execution-job:{job.id}",
                broker_order_id="submitted-before-worker-crash",
                symbol=item.symbol,
                side=item.side,
                status="submitted",
                quantity=2,
                submitted_at=attempted_at - timedelta(minutes=9),
            )
        )
        db.commit()

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )

    assert result.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        assert item.status == "broker_order_submitted"
        assert job.status == "completed"
        assert job.last_error is None
    engine.dispose()


def test_retrying_job_converges_after_reconciliation_confirms_submission(
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-retrying-job-reconciled-submission",
    )
    observed_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.status = "broker_order_submission_pending"
        job.status = "retrying"
        job.locked_at = observed_at - timedelta(minutes=10)
        job.attempts = 1
        order = BrokerOrder(
            execution_plan_item_id=item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key=f"execution-job:{job.id}",
            broker_order_id="reconciled-after-worker-crash",
            symbol=item.symbol,
            side=item.side,
            status="submission_pending",
            quantity=2,
        )
        db.add(order)
        db.commit()

        # Simulate broker reconciliation completing after stale recovery already
        # projected the execution job to retrying.
        item.status = "broker_order_submitted"
        order.status = "submitted"
        db.commit()

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=observed_at,
        )

    assert result.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert job is not None
        assert job.status == "completed"
        assert job.last_error is None
    engine.dispose()


def test_stale_retry_job_recovers_using_materialized_retry_order_key(tmp_path) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-stale-retry-submitted-order",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        source_order = BrokerOrder(
            execution_plan_item_id=item.id,
            broker_name="kis",
            mode="paper",
            idempotency_key="source-order-intent",
            broker_order_id="rejected-source-order",
            symbol=item.symbol,
            side=item.side,
            status="rejected",
            quantity=2,
            submitted_at=attempted_at - timedelta(minutes=20),
        )
        db.add(source_order)
        db.flush()
        retry_key = f"retry:{source_order.id}:{job.id}"
        db.add(
            ExecutionFailure(
                mode="paper",
                broker_order_pk=source_order.id,
                execution_plan_item_id=item.id,
                broker_name="kis",
                idempotency_key=source_order.idempotency_key,
                symbol=item.symbol,
                side=item.side,
                requested_quantity=2,
                classification="broker_rejected",
                broker_error_message="retryable rejection",
                retryable=True,
                status="open",
                first_failed_at=attempted_at - timedelta(minutes=20),
                latest_failed_at=attempted_at - timedelta(minutes=20),
                retry_job_id=job.id,
                retry_order_idempotency_key=retry_key,
            )
        )
        db.add(
            BrokerOrder(
                execution_plan_item_id=item.id,
                broker_name="kis",
                mode="paper",
                idempotency_key=retry_key,
                broker_order_id="submitted-retry-before-worker-crash",
                symbol=item.symbol,
                side=item.side,
                status="submitted",
                quantity=2,
                submitted_at=attempted_at - timedelta(minutes=9),
            )
        )
        item.quantity_policy = {
            "kind": "shares",
            "quantity": 2,
            "idempotency_key": source_order.idempotency_key,
        }
        item.status = "broker_order_submitted"
        job.job_type = "broker_order_retry"
        job.status = "running"
        job.locked_at = attempted_at - timedelta(minutes=10)
        job.attempts = 1
        db.commit()

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )

    assert result.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id,
                ExecutionJob.job_type == "broker_order_retry",
            )
        )
        assert job is not None
        assert job.status == "completed"
        assert job.last_error is None
    engine.dispose()


def test_running_job_lease_covers_bounded_multi_call_dispatch() -> None:
    settings = Settings(kis_request_timeout_seconds=10)
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)

    assert execution_jobs._is_stale_running_job_lock(
        settings=settings,
        locked_at=now - timedelta(seconds=31),
        triggered_at=now,
    ) is False
    assert execution_jobs._is_stale_running_job_lock(
        settings=settings,
        locked_at=now - timedelta(minutes=5),
        triggered_at=now,
    ) is True

    slow_token_settings = Settings(
        kis_request_timeout_seconds=1,
        alpaca_request_timeout_seconds=1,
        upbit_request_timeout_seconds=1,
        kis_token_request_timeout_seconds=120,
        kis_token_issuance_lease_seconds=180,
    )
    assert execution_jobs._is_stale_running_job_lock(
        settings=slow_token_settings,
        locked_at=now - timedelta(minutes=2),
        triggered_at=now,
    ) is False


def test_kis_us_buy_job_lease_covers_full_paginated_account_gate() -> None:
    settings = Settings(
        kis_request_timeout_seconds=10,
        alpaca_request_timeout_seconds=1,
        upbit_request_timeout_seconds=1,
    )
    now = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    item = ExecutionPlanItem(
        symbol="AAPL",
        side="buy",
        intent="next_open_buy",
        provider="kis",
        asset_class="us_equity",
        venue="NASD",
    )
    job = ExecutionJob(
        job_type="next_open_buy",
        status="running",
        locked_at=now - timedelta(minutes=5),
        plan_item=item,
    )

    assert execution_jobs._is_stale_running_job(
        settings=settings,
        job=job,
        triggered_at=now,
    ) is False
    timeout_seconds = execution_jobs._execution_job_stale_after_seconds(
        settings=settings,
        job=job,
    )
    job.locked_at = now - timedelta(seconds=timeout_seconds + 1)
    assert execution_jobs._is_stale_running_job(
        settings=settings,
        job=job,
        triggered_at=now,
    ) is True


def test_superseded_dispatch_cannot_finish_a_newer_job_lease(tmp_path) -> None:
    app, engine, _adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-fenced-job-finish",
    )
    stale_lease = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    current_lease = stale_lease + timedelta(minutes=1)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.status = "next_open_buy_dispatching"
        job.status = "running"
        job.locked_at = current_lease
        job.attempts = 2
        db.commit()

        result = execution_jobs._finish_job(
            db=db,
            job_id=job.id,
            expected_locked_at=stale_lease,
            status="failed",
            last_error="stale worker failed after lease recovery",
            plan_item_status="next_open_buy_failed",
        )

        db.refresh(item)
        db.refresh(job)
        assert result == "superseded"
        assert item.status == "next_open_buy_dispatching"
        assert job.status == "running"
        assert _utc(job.locked_at) == current_lease
        assert job.last_error is None
    engine.dispose()


@pytest.mark.parametrize(
    "plan_item_status",
    ["revoked", "manual_review_required", "contract_quarantined"],
)
def test_broker_submission_fence_rejects_non_dispatching_plan_item_even_with_current_running_lease(
    tmp_path,
    plan_item_status: str,
) -> None:
    app, engine, _adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-revoked-at-submit-boundary",
    )
    lease = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.status = plan_item_status
        job.status = "running"
        job.locked_at = lease
        db.commit()

    with app.state.db_session_factory() as db:
        assert execution_jobs._lock_job_for_broker_submission(
            db=db,
            job_id=job.id,
            expected_locked_at=lease,
        ) is None

    engine.dispose()


def test_us_session_defer_does_not_revive_item_expired_after_due_query(monkeypatch, tmp_path) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-us-next-open-defer-risk-off-race",
    )
    attempted_at = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item_id))
        assert item is not None
        assert job is not None
        item.symbol = "AAPL"
        item.provider = "kis"
        item.asset_class = "us_equity"
        item.venue = "kis"
        item.risk_context = {"exchange": "NASD"}
        job.scheduled_for = attempted_at - timedelta(minutes=1)
        db.commit()

        def apply_risk_off_after_due_query(_local_time):
            item.status = "watch_expired"
            db.flush()
            return False

        monkeypatch.setattr(
            execution_jobs,
            "is_us_regular_session",
            apply_risk_off_after_due_query,
        )
        # This regression targets the defer-stage status revalidation. The
        # initial submission-window guard is covered independently.
        monkeypatch.setattr(
            execution_jobs,
            "_guard_initial_next_open_submission",
            lambda **_: None,
        )

        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )
        db.refresh(item)
        db.refresh(job)
        assert result.payload["due_job_count"] == 1
        assert result.payload["completed_job_count"] == 0
        assert item.status == "watch_expired"
        assert job.status == "scheduled"
        assert job.attempts == 0
        assert _utc(job.scheduled_for) > attempted_at
        assert adapter.order_calls == []
    engine.dispose()


def test_stale_us_next_open_job_expires_instead_of_rolling_to_a_later_session(
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-stale-us-next-open",
    )
    original_open = datetime(2026, 7, 28, 13, 30, tzinfo=UTC)
    next_session = datetime(2026, 7, 29, 13, 30, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        item.symbol = "AAPL"
        item.provider = "kis"
        item.asset_class = "us_equity"
        item.risk_context = {"exchange": "NASD"}
        job.scheduled_for = original_open
        db.commit()

        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=next_session,
        )
        db.refresh(item)
        db.refresh(job)
        persisted_status = item.status
        persisted_job_status = job.status
        persisted_last_error = job.last_error

    assert result.payload["due_job_count"] == 1
    assert result.payload["failed_job_count"] == 1
    assert persisted_status == "next_open_buy_expired"
    assert persisted_job_status == "expired"
    assert persisted_last_error == "missed_next_open_submission_window"
    assert adapter.order_calls == []
    engine.dispose()


def test_account_drawdown_retry_after_defers_scheduled_job(monkeypatch, tmp_path) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-next-open-drawdown-retry-after",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    retry_after_at = attempted_at + timedelta(minutes=15)
    placement_calls = 0

    def block_for_drawdown_recovery(*, db, command, **_kwargs):
        nonlocal placement_calls
        placement_calls += 1
        item = db.get(ExecutionPlanItem, command.execution_plan_item_id)
        assert item is not None
        item.status = "new_buy_blocked"
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "blocked": True,
                "block_reason": "account_drawdown_recovery_cooldown",
                "message": "account drawdown recovery cooldown has not elapsed",
                "retry_after_at": retry_after_at.isoformat(),
            },
        )

    monkeypatch.setattr(execution_jobs, "place_broker_order", block_for_drawdown_recovery)

    with app.state.db_session_factory() as db:
        first = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )
    assert first.payload["due_job_count"] == 1
    assert first.payload["completed_job_count"] == 0
    assert first.payload["failed_job_count"] == 0
    assert first.payload["jobs"][0]["status"] == "scheduled"
    assert first.payload["jobs"][0]["scheduled_for"] == retry_after_at.isoformat()
    assert placement_calls == 1

    with app.state.db_session_factory() as db:
        job = db.scalar(select(ExecutionJob).where(ExecutionJob.execution_plan_item_id == item_id))
        assert job is not None
        assert _utc(job.scheduled_for) == retry_after_at
        assert job.attempts == 1
        second = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=retry_after_at - timedelta(seconds=1),
        )
        db.refresh(job)
        assert second.payload["due_job_count"] == 0
        assert job.attempts == 1
    assert placement_calls == 1
    engine.dispose()


def test_account_drawdown_state_change_reschedules_job_in_safe_retry_state(
    monkeypatch,
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-next-open-drawdown-state-change",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)

    def block_for_changed_recovery_state(**_kwargs):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "blocked": True,
                "block_reason": "account_drawdown_recovery_state_changed",
                "message": (
                    "account drawdown recovery state changed while waiting for "
                    "the account lock; reevaluate this candidate"
                ),
            },
        )

    monkeypatch.setattr(
        execution_jobs,
        "place_broker_order",
        block_for_changed_recovery_state,
    )

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )

    assert result.payload["due_job_count"] == 1
    assert result.payload["failed_job_count"] == 0
    assert result.payload["retrying_job_count"] == 0
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        assert item.status == "new_buy_blocked"
        assert job.status == "scheduled"
        assert job.attempts == 1
    assert adapter.order_calls == []
    engine.dispose()


def test_stale_running_job_is_reprojected_under_active_hourly_risk_off(
    tmp_path,
) -> None:
    app, engine, adapter, item_id = _next_open_app(
        tmp_path,
        plan_id="plan-stale-running-active-risk-off",
    )
    attempted_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        control = db.get(RuntimeControlState, "global")
        assert item is not None
        assert job is not None
        assert control is not None
        item.status = "next_open_buy_dispatching"
        job.status = "running"
        job.locked_at = attempted_at - timedelta(minutes=10)
        job.attempts = 1
        control.hourly_risk_off_active = True
        control.hourly_risk_off_epoch = 1
        control.hourly_risk_off_context = {
            "status": "active",
            "epoch": 1,
            "activated_at": attempted_at.isoformat(),
            "last_risk_at": attempted_at.isoformat(),
            "activation_risk_flags": 3,
            "activation_risk_reasons": ["broad market risk-off"],
            "consecutive_healthy_observations": 0,
            "cooldown_until": attempted_at.isoformat(),
            "cooldown_satisfied": False,
        }
        db.commit()

    with app.state.db_session_factory() as db:
        result = dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=db,
            adapter=adapter,
            now=attempted_at,
        )

    assert result.payload["due_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        job = db.scalar(
            select(ExecutionJob).where(
                ExecutionJob.execution_plan_item_id == item_id
            )
        )
        assert item is not None
        assert job is not None
        assert item.status == "watch_expired"
        hourly_action = item.risk_context["hourly_plan_action"]
        assert hourly_action["source"] == "hourly_runtime_check"
        assert hourly_action["action"] == "risk_off_expire"
        assert hourly_action["resume"]["status"] == "next_open_buy_retry_pending"
        assert job.status == "retrying"
        assert job.attempts == 1
    engine.dispose()
