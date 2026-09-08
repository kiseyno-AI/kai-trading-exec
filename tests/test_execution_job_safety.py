from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.broker.base import BrokerConfig
from app.core.config import Settings
from app.models.runtime import ExecutionJob, ExecutionPlanItem, RuntimeControlState
from app.services import execution_jobs, plan_judgment_actions
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
            "source": "kai-trading-core",
            "market_date": "2024-01-03",
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

    blocked_dispatch = TestClient(app).post("/execution-jobs/dispatch")
    assert blocked_dispatch.status_code == 200
    assert blocked_dispatch.json()["due_job_count"] == 0
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

    resumed_dispatch = TestClient(app).post("/execution-jobs/dispatch")
    assert resumed_dispatch.status_code == 200
    assert resumed_dispatch.json()["due_job_count"] == 1
    assert resumed_dispatch.json()["completed_job_count"] == 1
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

    response = TestClient(app).post("/execution-jobs/dispatch")

    assert response.status_code == 200
    assert response.json()["completed_job_count"] == 0
    assert response.json()["failed_job_count"] == 0
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
