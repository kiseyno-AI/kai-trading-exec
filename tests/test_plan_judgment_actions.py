import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select, update
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import (
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    RuntimeControlState,
)
from app.services import plan_judgment_actions
from app.services.plan_judgment_actions import ADJUSTMENT_KEY, MAX_MUTATIONS_PER_RUN, apply_hourly_plan_judgment_action


class _FrozenDateTime(datetime):
    current = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.current.replace(tzinfo=None)
        return cls.current.astimezone(tz)


def test_postgres_projection_takes_table_lock_before_final_reconciliation() -> None:
    statements: list[str] = []

    class _Dialect:
        name = "postgresql"

    class _Bind:
        dialect = _Dialect()

    class _RecordingSession:
        def get_bind(self):
            return _Bind()

        def execute(self, statement):
            statements.append(str(statement))

    plan_judgment_actions._lock_execution_plan_items_for_hourly_projection(
        _RecordingSession()
    )

    assert statements == [
        "LOCK TABLE execution_plan_items IN EXCLUSIVE MODE"
    ]


def _create_plan_judgment_db(path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        create table execution_plans (id text primary key, mode text, accepted_at text);
        create table execution_plan_items (
          id text primary key,
          execution_plan_id text,
          symbol text,
          side text,
          intent text,
          status text,
          trigger_price numeric,
          limit_price numeric,
          risk_context text,
          created_at text,
          updated_at text
        );
        create table execution_plan_item_audits (
          id text primary key,
          execution_plan_item_id text,
          event_type text,
          source text,
          summary text,
          payload text,
          occurred_at text,
          created_at text,
          updated_at text
        );
        insert into execution_plans(id, mode, accepted_at) values ('plan-live', 'live', '2026-07-28T00:00:00Z');
        insert into execution_plan_items(
          id, execution_plan_id, symbol, side, intent, status,
          trigger_price, limit_price, risk_context, created_at, updated_at
        ) values
          ('buy-rejected', 'plan-live', '000001', 'buy', 'intraday_watch_buy', 'exec_agent_rejected', 100, 101, '{}', '2026-07-28', '2026-07-28'),
          ('buy-expired', 'plan-live', '000002', 'buy', 'next_open_buy', 'watch_expired', 200, 201, '{}', '2026-07-28', '2026-07-28'),
          ('sell-expired', 'plan-live', '000003', 'sell', 'stop_sell', 'watch_expired', 300, 301, '{}', '2026-07-28', '2026-07-28');
        """
    )
    conn.commit()
    conn.close()


def test_sqlite_risk_off_projection_pending_survives_failure_and_blocks_recovery(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "exec.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url=f"sqlite:///{db_path}",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    with Session(engine) as db:
        plan = ExecutionPlan(
            external_plan_id="plan-sqlite-durable-projection",
            source="test",
            mode="live",
        )
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all(
            [
                RuntimeControlState(
                    id="global",
                    mode="live",
                    paused=False,
                ),
                plan,
                item,
            ]
        )
        db.commit()
        item_id = item.id

    original_expire = plan_judgment_actions._expire_buy_watches
    projection_attempts = 0

    def fail_first_projection(conn, runtime_settings, risk_flags, risk_reasons):
        nonlocal projection_attempts
        projection_attempts += 1
        if projection_attempts == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_expire(conn, runtime_settings, risk_flags, risk_reasons)

    monkeypatch.setattr(plan_judgment_actions, "_expire_buy_watches", fail_first_projection)

    activation = apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=3,
        risk_reasons=["broad market risk-off"],
    )

    assert activation["projection_status"] == "pending"
    with Session(engine) as db:
        state = db.get(RuntimeControlState, "global")
        item = db.get(ExecutionPlanItem, item_id)
        assert state is not None
        assert state.hourly_risk_off_active is True
        assert state.hourly_risk_off_context["projection_pending"] is True
        assert item is not None
        assert item.status == "watch_monitoring_active"

    _FrozenDateTime.current = started_at + timedelta(seconds=1)
    first_healthy = apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=0,
        risk_reasons=[],
    )
    with Session(engine) as db:
        state = db.get(RuntimeControlState, "global")
        item = db.get(ExecutionPlanItem, item_id)
        assert state is not None
        assert state.hourly_risk_off_active is True
        assert state.hourly_risk_off_context["projection_pending"] is False
        assert item is not None
        assert item.status == "watch_expired"
        assert first_healthy["hourly_risk_off"]["active"] is True

    _FrozenDateTime.current = started_at + timedelta(seconds=2)
    second_healthy = apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=0,
        risk_reasons=[],
    )
    with Session(engine) as db:
        state = db.get(RuntimeControlState, "global")
        item = db.get(ExecutionPlanItem, item_id)
        assert state is not None
        assert state.hourly_risk_off_active is False
        assert state.hourly_risk_off_context["projection_pending"] is False
        assert item is not None
        assert item.status == "watch_monitoring_active"
        assert second_healthy["hourly_risk_off"]["active"] is False
    engine.dispose()


def test_active_risk_off_reconciles_late_mutable_buy_before_global_recovery(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    with Session(engine) as db:
        plan = ExecutionPlan(
            external_plan_id="plan-pg-late-risk-off-candidate",
            source="test",
            mode="live",
        )
        initial = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, initial])
        db.commit()

        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )
        late = ExecutionPlanItem(
            plan=plan,
            plan_item_index=1,
            symbol="000660",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=200,
            limit_price=201,
            risk_context={},
        )
        db.add(late)
        db.commit()

        _FrozenDateTime.current = started_at + timedelta(seconds=1)
        first_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(late)
        state = db.get(RuntimeControlState, "global")
        assert state is not None
        assert state.hourly_risk_off_active is True
        assert state.hourly_risk_off_context["projection_pending"] is False
        assert first_healthy["hourly_risk_off"]["active"] is True
        assert late.status == "watch_expired"
        assert late.risk_context[ADJUSTMENT_KEY]["action"] == "risk_off_expire"

        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        second_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(initial)
        db.refresh(late)
        db.refresh(state)
        assert second_healthy["hourly_risk_off"]["active"] is False
        assert state.hourly_risk_off_active is False
        assert initial.status == "watch_monitoring_active"
        assert late.status == "watch_monitoring_active"
    engine.dispose()


def test_global_risk_off_waits_for_every_recovery_beyond_single_batch(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    candidate_count = MAX_MUTATIONS_PER_RUN + 5
    with Session(engine) as db:
        plan = ExecutionPlan(
            external_plan_id="plan-pg-risk-off-recovery-over-batch",
            source="test",
            mode="live",
        )
        items = [
            ExecutionPlanItem(
                plan=plan,
                plan_item_index=index,
                symbol=f"{index:06d}",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=100 + index,
                limit_price=101 + index,
                risk_context={},
            )
            for index in range(candidate_count)
        ]
        db.add_all([plan, *items])
        db.commit()

        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )

        _FrozenDateTime.current = started_at + timedelta(seconds=1)
        first_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        assert first_healthy["hourly_risk_off"]["active"] is True
        assert db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .where(ExecutionPlanItem.status == "watch_expired")
        ) == candidate_count

        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        second_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        state = db.get(RuntimeControlState, "global")
        assert state is not None
        assert second_healthy["recovery_resumed_count"] == candidate_count
        assert second_healthy["hourly_risk_off"]["active"] is False
        assert state.hourly_risk_off_active is False
        assert db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .where(ExecutionPlanItem.status == "watch_monitoring_active")
        ) == candidate_count
    engine.dispose()


def test_normal_plan_judgment_does_not_resume_rejected_buy_candidates(tmp_path) -> None:
    db_path = tmp_path / "exec.db"
    _create_plan_judgment_db(db_path)
    settings = Settings(runtime_mode="live", database_url=f"sqlite:///{db_path}")

    result = apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])

    assert result["action"] == "restore_conservative_price_adjust"
    assert result["changed_count"] == 0
    conn = sqlite3.connect(db_path)
    statuses = dict(conn.execute("select id, status from execution_plan_items").fetchall())
    assert statuses["buy-rejected"] == "exec_agent_rejected"
    assert statuses["buy-expired"] == "watch_expired"
    assert statuses["sell-expired"] == "watch_expired"
    audit_count = conn.execute(
        "select count(*) from execution_plan_item_audits where event_type = 'hourly_plan_judgment_buy_watch_resumed'"
    ).fetchone()[0]
    assert audit_count == 0


def test_risk_off_expired_buy_resumes_only_after_healthy_streak_and_cooldown(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "exec.db"
    _create_plan_judgment_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        insert into execution_plan_items(
          id, execution_plan_id, symbol, side, intent, status,
          trigger_price, limit_price, risk_context, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "risk-off-buy",
            "plan-live",
            "005930",
            "buy",
            "intraday_watch_buy",
            "watch_monitoring_active",
            100,
            101,
            "{}",
            "2026-07-28T00:00:00+00:00",
            "2026-07-28T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()
    settings = Settings(
        runtime_mode="live",
        database_url=f"sqlite:///{db_path}",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=900,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)

    apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=3,
        risk_reasons=["broad market risk-off"],
    )

    _FrozenDateTime.current = started_at + timedelta(seconds=100)
    apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])
    conn = sqlite3.connect(db_path)
    status, raw_context = conn.execute(
        "select status, risk_context from execution_plan_items where id = 'risk-off-buy'"
    ).fetchone()
    recovery = json.loads(raw_context)[ADJUSTMENT_KEY]["recovery"]
    assert status == "watch_expired"
    assert recovery["status"] == "observing"
    assert recovery["consecutive_healthy_observations"] == 1
    assert recovery["required_healthy_observations"] == 2
    assert recovery["cooldown_seconds"] == 900
    assert conn.execute(
        """
        select count(*) from execution_plan_item_audits
        where execution_plan_item_id = 'risk-off-buy'
          and event_type = 'hourly_plan_judgment_buy_watch_recovery_observed'
        """
    ).fetchone()[0] == 1
    conn.close()

    _FrozenDateTime.current = started_at + timedelta(seconds=200)
    apply_hourly_plan_judgment_action(
        settings=settings,
        risk_flags=1,
        risk_reasons=["caution returned"],
    )
    conn = sqlite3.connect(db_path)
    status, raw_context = conn.execute(
        "select status, risk_context from execution_plan_items where id = 'risk-off-buy'"
    ).fetchone()
    recovery = json.loads(raw_context)[ADJUSTMENT_KEY]["recovery"]
    assert status == "watch_expired"
    assert recovery["status"] == "reset"
    assert recovery["consecutive_healthy_observations"] == 0
    assert recovery["last_risk_flags"] == 1
    assert conn.execute(
        """
        select count(*) from execution_plan_item_audits
        where execution_plan_item_id = 'risk-off-buy'
          and event_type = 'hourly_plan_judgment_buy_watch_recovery_reset'
        """
    ).fetchone()[0] == 1
    conn.close()

    _FrozenDateTime.current = started_at + timedelta(seconds=300)
    apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])
    _FrozenDateTime.current = started_at + timedelta(seconds=899)
    apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])
    conn = sqlite3.connect(db_path)
    status, raw_context = conn.execute(
        "select status, risk_context from execution_plan_items where id = 'risk-off-buy'"
    ).fetchone()
    recovery = json.loads(raw_context)[ADJUSTMENT_KEY]["recovery"]
    assert status == "watch_expired"
    assert recovery["consecutive_healthy_observations"] == 2
    assert recovery["cooldown_satisfied"] is False
    conn.close()

    _FrozenDateTime.current = started_at + timedelta(seconds=1099)
    apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])
    conn = sqlite3.connect(db_path)
    status = conn.execute(
        "select status from execution_plan_items where id = 'risk-off-buy'"
    ).fetchone()[0]
    assert status == "watch_expired"
    conn.close()

    _FrozenDateTime.current = started_at + timedelta(seconds=1100)
    apply_hourly_plan_judgment_action(settings=settings, risk_flags=0, risk_reasons=[])
    conn = sqlite3.connect(db_path)
    status, raw_context = conn.execute(
        "select status, risk_context from execution_plan_items where id = 'risk-off-buy'"
    ).fetchone()
    resumed_action = json.loads(raw_context)[ADJUSTMENT_KEY]
    assert status == "watch_monitoring_active"
    assert resumed_action["action"] == "risk_off_resume"
    assert resumed_action["recovery"]["status"] == "resumed"
    assert resumed_action["recovery"]["cooldown_satisfied"] is True
    assert resumed_action["recovery"]["resumed_at"] == _FrozenDateTime.current.isoformat()
    assert conn.execute(
        """
        select count(*) from execution_plan_item_audits
        where execution_plan_item_id = 'risk-off-buy'
          and event_type = 'hourly_plan_judgment_buy_watch_resumed'
        """
    ).fetchone()[0] == 1
    conn.close()


def test_risk_off_recovery_session_path_matches_sqlite_and_ignores_manual_expiry(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at + timedelta(seconds=1)
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    applied_at = started_at.isoformat()
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-recovery", source="test", mode="live")
        risk_off_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_expired",
            trigger_price=100,
            limit_price=101,
            risk_context={
                ADJUSTMENT_KEY: {
                    "action": "risk_off_expire",
                    "source": "hourly_runtime_check",
                    "risk_flags": 3,
                    "risk_reasons": ["broad market risk-off"],
                    "previous": {
                        "status": "watch_monitoring_pending",
                        "trigger_price": "100.0000",
                        "limit_price": "101.0000",
                    },
                    "applied_at": applied_at,
                }
            },
        )
        manual_expiry = ExecutionPlanItem(
            plan=plan,
            plan_item_index=1,
            symbol="000660",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_expired",
            trigger_price=200,
            limit_price=201,
            risk_context={"manual_reason": "operator expired"},
        )
        rejected = ExecutionPlanItem(
            plan=plan,
            plan_item_index=2,
            symbol="035420",
            side="buy",
            intent="next_open_buy",
            status="exec_agent_rejected",
            trigger_price=300,
            limit_price=301,
            risk_context={},
        )
        db.add_all([plan, risk_off_item, manual_expiry, rejected])
        db.commit()
        risk_off_item_id = risk_off_item.id
        manual_expiry_id = manual_expiry.id
        rejected_id = rejected.id

        apply_hourly_plan_judgment_action(settings=settings, db=db, risk_flags=0, risk_reasons=[])
        db.refresh(risk_off_item)
        db.refresh(manual_expiry)
        db.refresh(rejected)
        assert risk_off_item.status == "watch_expired"
        assert risk_off_item.risk_context[ADJUSTMENT_KEY]["recovery"]["consecutive_healthy_observations"] == 1
        assert manual_expiry.status == "watch_expired"
        assert manual_expiry.risk_context == {"manual_reason": "operator expired"}
        assert rejected.status == "exec_agent_rejected"
        assert rejected.risk_context == {}

        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        apply_hourly_plan_judgment_action(settings=settings, db=db, risk_flags=0, risk_reasons=[])
        db.refresh(risk_off_item)
        db.refresh(manual_expiry)
        db.refresh(rejected)
        assert risk_off_item.id == risk_off_item_id
        assert risk_off_item.status == "watch_monitoring_pending"
        assert risk_off_item.risk_context[ADJUSTMENT_KEY]["action"] == "risk_off_resume"
        assert risk_off_item.risk_context[ADJUSTMENT_KEY]["recovery"]["status"] == "resumed"
        assert manual_expiry.id == manual_expiry_id
        assert manual_expiry.status == "watch_expired"
        assert manual_expiry.risk_context == {"manual_reason": "operator expired"}
        assert rejected.id == rejected_id
        assert rejected.status == "exec_agent_rejected"
        assert rejected.risk_context == {}
        resumed_audits = db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItemAudit)
            .where(
                ExecutionPlanItemAudit.execution_plan_item_id == risk_off_item_id,
                ExecutionPlanItemAudit.event_type == "hourly_plan_judgment_buy_watch_resumed",
            )
        )
        assert resumed_audits == 1
    engine.dispose()


def test_risk_off_expires_scheduled_next_open_and_restores_it_without_touching_open_buy(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-next-open-risk-off", source="test", mode="live")
        scheduled_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="next_open_buy_scheduled",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        open_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=1,
            symbol="000660",
            side="buy",
            intent="next_open_buy",
            status="open",
            trigger_price=200,
            limit_price=201,
            risk_context={},
        )
        db.add_all([plan, scheduled_item, open_item])
        db.commit()

        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )
        db.refresh(scheduled_item)
        db.refresh(open_item)
        assert scheduled_item.status == "watch_expired"
        assert scheduled_item.risk_context[ADJUSTMENT_KEY]["previous"]["status"] == "next_open_buy_scheduled"
        assert open_item.status == "open"
        assert open_item.risk_context == {}

        _FrozenDateTime.current = started_at + timedelta(seconds=1)
        apply_hourly_plan_judgment_action(settings=settings, db=db, risk_flags=0, risk_reasons=[])
        db.refresh(scheduled_item)
        assert scheduled_item.status == "watch_expired"

        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        apply_hourly_plan_judgment_action(settings=settings, db=db, risk_flags=0, risk_reasons=[])
        db.refresh(scheduled_item)
        db.refresh(open_item)
        assert scheduled_item.status == "next_open_buy_scheduled"
        assert scheduled_item.risk_context[ADJUSTMENT_KEY]["action"] == "risk_off_resume"
        assert open_item.status == "open"
        assert open_item.risk_context == {}
    engine.dispose()


def test_risk_off_expires_recovery_wait_states_and_restores_each_exact_status(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    expected_statuses = {
        "watch-trigger-blocked": "watch_buy_triggered_blocked",
        "watch-sizing-pending": "watch_sizing_pending",
        "new-buy-blocked": "new_buy_blocked",
        "next-open-retry": "next_open_buy_retry_pending",
    }
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-risk-off-wait-states", source="test", mode="live")
        items = [
            ExecutionPlanItem(
                id=item_id,
                plan=plan,
                plan_item_index=index,
                symbol=f"{index + 1:06d}",
                side="buy",
                intent=(
                    "intraday_watch_buy"
                    if status.startswith("watch_")
                    else "next_open_buy"
                ),
                status=status,
                trigger_price=100 + index,
                limit_price=101 + index,
                risk_context={},
            )
            for index, (item_id, status) in enumerate(expected_statuses.items())
        ]
        db.add_all([plan, *items])
        db.commit()

        risk_off = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )
        assert risk_off["changed_count"] == len(items)
        for item in items:
            db.refresh(item)
            assert item.status == "watch_expired"
            assert item.risk_context[ADJUSTMENT_KEY]["previous"]["status"] == expected_statuses[item.id]

        _FrozenDateTime.current = started_at + timedelta(seconds=1)
        first_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        assert first_healthy["recovery_resumed_count"] == 0
        for item in items:
            db.refresh(item)
            assert item.status == "watch_expired"

        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        second_healthy = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        assert second_healthy["recovery_resumed_count"] == len(items)
        for item in items:
            db.refresh(item)
            assert item.status == expected_statuses[item.id]
            assert item.risk_context[ADJUSTMENT_KEY]["action"] == "risk_off_resume"
    engine.dispose()


def test_risk_off_recovery_requires_minimum_interval_between_healthy_observations(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=300,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-recovery-min-interval", source="test", mode="live")
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, item])
        db.commit()

        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["risk-off"],
        )

        first_healthy_at = started_at + timedelta(seconds=300)
        _FrozenDateTime.current = first_healthy_at
        first = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(item)
        assert first["recovery_observed_count"] == 1
        assert item.status == "watch_expired"
        assert item.risk_context[ADJUSTMENT_KEY]["recovery"]["consecutive_healthy_observations"] == 1
        assert item.risk_context[ADJUSTMENT_KEY]["recovery"]["last_observed_at"] == first_healthy_at.isoformat()

        _FrozenDateTime.current = first_healthy_at + timedelta(seconds=1)
        too_soon = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(item)
        assert too_soon["recovery_observed_count"] == 0
        assert too_soon["recovery_resumed_count"] == 0
        assert item.status == "watch_expired"
        assert item.risk_context[ADJUSTMENT_KEY]["recovery"]["consecutive_healthy_observations"] == 1
        assert item.risk_context[ADJUSTMENT_KEY]["recovery"]["last_observed_at"] == first_healthy_at.isoformat()

        _FrozenDateTime.current = first_healthy_at + timedelta(seconds=300)
        spaced = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(item)
        assert spaced["recovery_observed_count"] == 0
        assert spaced["recovery_resumed_count"] == 1
        assert item.status == "watch_monitoring_active"
    engine.dispose()


def test_global_risk_off_ignores_healthy_timestamp_not_after_latest_risk() -> None:
    settings = Settings(
        runtime_mode="live",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    latest_risk_at = datetime(2026, 7, 28, 0, 0, 1, tzinfo=UTC)
    stale_healthy_at = latest_risk_at - timedelta(seconds=1)
    active, epoch, context, event_type = (
        plan_judgment_actions._hourly_risk_off_fence_values(
            active=True,
            epoch=7,
            raw_context={
                "status": "active",
                "epoch": 7,
                "activated_at": stale_healthy_at.isoformat(),
                "last_risk_at": latest_risk_at.isoformat(),
                "consecutive_healthy_observations": 0,
                "last_healthy_observed_at": None,
            },
            settings=settings,
            risk_flags=0,
            risk_reasons=[],
            now=stale_healthy_at,
        )
    )

    assert active is True
    assert epoch == 7
    assert context is not None
    assert context["consecutive_healthy_observations"] == 0
    assert context["last_healthy_observed_at"] is None
    assert context["recovery_ready"] is False
    assert event_type is None

    _, _, fresh_context, fresh_event_type = (
        plan_judgment_actions._hourly_risk_off_fence_values(
            active=True,
            epoch=epoch,
            raw_context=context,
            settings=settings,
            risk_flags=0,
            risk_reasons=[],
            now=latest_risk_at + timedelta(seconds=1),
        )
    )
    assert fresh_context is not None
    assert fresh_context["consecutive_healthy_observations"] == 1
    assert fresh_event_type == "hourly_risk_off_observed"


@pytest.mark.parametrize("risk_flags", [1, 3])
def test_global_risk_off_stale_risk_timestamp_cannot_shorten_cooldown(
    risk_flags: int,
) -> None:
    settings = Settings(
        runtime_mode="live",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=300,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    latest_risk_at = datetime(2026, 7, 28, 0, 5, tzinfo=UTC)
    stale_risk_at = latest_risk_at - timedelta(minutes=5)
    _, _, context, _ = plan_judgment_actions._hourly_risk_off_fence_values(
        active=True,
        epoch=7,
        raw_context={
            "status": "observing",
            "epoch": 7,
            "activated_at": stale_risk_at.isoformat(),
            "last_risk_at": latest_risk_at.isoformat(),
            "consecutive_healthy_observations": 1,
            "last_healthy_observed_at": (
                latest_risk_at + timedelta(seconds=1)
            ).isoformat(),
        },
        settings=settings,
        risk_flags=risk_flags,
        risk_reasons=["stale concurrent observation"],
        now=stale_risk_at,
    )

    assert context is not None
    assert context["last_risk_at"] == latest_risk_at.isoformat()
    assert context["cooldown_until"] == (
        latest_risk_at + timedelta(seconds=300)
    ).isoformat()
    assert context["consecutive_healthy_observations"] == 0
    assert context["last_healthy_observed_at"] is None


def test_item_risk_off_recovery_ignores_stale_transition_timestamps() -> None:
    settings = Settings(
        runtime_mode="live",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=300,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    applied_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    latest_risk_at = applied_at + timedelta(minutes=5)
    stale_at = applied_at + timedelta(minutes=1)
    adjustment = {
        "action": "risk_off_expire",
        "applied_at": applied_at.isoformat(),
        "previous": {"status": "watch_monitoring_active"},
        "resume": {"status": "watch_monitoring_active"},
        "recovery": {
            "status": "observing",
            "last_risk_at": latest_risk_at.isoformat(),
            "consecutive_healthy_observations": 1,
            "last_observed_at": (latest_risk_at + timedelta(seconds=1)).isoformat(),
        },
    }

    stale_reset, _, _, _ = plan_judgment_actions._risk_off_recovery_transition(
        adjustment=adjustment,
        settings=settings,
        risk_flags=1,
        risk_reasons=["stale concurrent observation"],
        now=stale_at,
    )
    reset_state = stale_reset["recovery"]
    assert reset_state["last_risk_at"] == latest_risk_at.isoformat()
    assert reset_state["cooldown_until"] == (
        latest_risk_at + timedelta(seconds=300)
    ).isoformat()

    stale_healthy, status, event_type, _ = (
        plan_judgment_actions._risk_off_recovery_transition(
            adjustment={
                **adjustment,
                "recovery": {
                    **adjustment["recovery"],
                    "consecutive_healthy_observations": 0,
                    "last_observed_at": None,
                },
            },
            settings=settings,
            risk_flags=0,
            risk_reasons=[],
            now=stale_at,
        )
    )
    assert stale_healthy["recovery"]["consecutive_healthy_observations"] == 0
    assert stale_healthy["recovery"]["last_observed_at"] is None
    assert status == "watch_expired"
    assert event_type is None


@pytest.mark.parametrize("corrupted_count", ["not-an-integer", -7])
def test_risk_off_recovery_corrupted_healthy_count_fails_closed(corrupted_count) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    applied_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC).isoformat()
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id=f"plan-pg-corrupt-{corrupted_count}", source="test", mode="live")
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_expired",
            trigger_price=100,
            limit_price=101,
            risk_context={
                ADJUSTMENT_KEY: {
                    "action": "risk_off_expire",
                    "source": "hourly_runtime_check",
                    "previous": {
                        "status": "watch_monitoring_active",
                        "trigger_price": "100.0000",
                        "limit_price": "101.0000",
                    },
                    "applied_at": applied_at,
                    "recovery": {
                        "status": "observing",
                        "consecutive_healthy_observations": corrupted_count,
                        "last_risk_at": applied_at,
                        "last_observed_at": None,
                    },
                }
            },
        )
        db.add_all([plan, item])
        db.commit()

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )

        db.refresh(item)
        assert result["status"] == "applied"
        assert result["recovery_resumed_count"] == 0
        assert item.status == "watch_expired"
        assert item.risk_context[ADJUSTMENT_KEY]["recovery"]["consecutive_healthy_observations"] == 1
    engine.dispose()


def test_severe_risk_off_expires_every_buy_candidate_beyond_single_batch_limit() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
    )
    candidate_count = MAX_MUTATIONS_PER_RUN + 5
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-risk-off-over-batch", source="test", mode="live")
        items = [
            ExecutionPlanItem(
                plan=plan,
                plan_item_index=index,
                symbol=f"{index:06d}",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                trigger_price=100 + index,
                limit_price=101 + index,
                risk_context={},
            )
            for index in range(candidate_count)
        ]
        db.add_all([plan, *items])
        db.commit()

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )

        expired_count = db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.execution_plan_id == plan.id,
                ExecutionPlanItem.status == "watch_expired",
            )
        )
        audit_count = db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItemAudit)
            .where(
                ExecutionPlanItemAudit.execution_plan_item_id.in_([item.id for item in items]),
                ExecutionPlanItemAudit.event_type == "hourly_plan_judgment_buy_watch_expired",
            )
        )
        assert result["changed_count"] == candidate_count
        assert expired_count == candidate_count
        assert audit_count == candidate_count
    engine.dispose()


@pytest.mark.parametrize(
    "active_status",
    [
        "watch_buy_triggered",
        "watch_buy_triggered_pending_dispatch",
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
        "next_open_buy_dispatching",
    ],
)
def test_severe_risk_off_expires_every_pre_broker_dispatch_buy_state(active_status) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
    )
    with Session(engine) as db:
        plan = ExecutionPlan(
            external_plan_id=f"plan-pg-risk-off-{active_status}",
            source="test",
            mode="live",
        )
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent=(
                "next_open_buy"
                if active_status == "next_open_buy_dispatching"
                else "intraday_watch_buy"
            ),
            status=active_status,
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, item])
        db.commit()

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["broad market risk-off"],
        )

        db.refresh(item)
        assert result["changed_count"] == 1
        assert item.status == "watch_expired"
        assert item.risk_context[ADJUSTMENT_KEY]["previous"]["status"] == active_status
    engine.dispose()


def test_risk_off_recovery_restores_prices_from_prior_caution_adjustment(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(
        runtime_mode="live",
        database_url="postgresql+psycopg://unused/test",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
    )
    started_at = datetime(2026, 7, 28, 0, 0, tzinfo=UTC)
    _FrozenDateTime.current = started_at
    monkeypatch.setattr(plan_judgment_actions, "datetime", _FrozenDateTime)
    with Session(engine) as db:
        plan = ExecutionPlan(
            external_plan_id="plan-pg-caution-risk-off-recovery",
            source="test",
            mode="live",
        )
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, item])
        db.commit()

        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=1,
            risk_reasons=["caution"],
        )
        db.refresh(item)
        assert str(item.trigger_price) == "99.0000"
        assert str(item.limit_price) == "99.9900"

        _FrozenDateTime.current = started_at + timedelta(seconds=1)
        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["risk-off"],
        )
        _FrozenDateTime.current = started_at + timedelta(seconds=2)
        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        _FrozenDateTime.current = started_at + timedelta(seconds=3)
        apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=0,
            risk_reasons=[],
        )
        db.refresh(item)

        assert item.status == "watch_monitoring_active"
        assert str(item.trigger_price) == "100.0000"
        assert str(item.limit_price) == "101.0000"
    engine.dispose()


def test_plan_judgment_supports_non_sqlite_session() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(runtime_mode="live", database_url="postgresql+psycopg://unused/test")
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg", source="test", mode="live")
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, item])
        db.commit()

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=1,
            risk_reasons=["test caution"],
        )

        db.refresh(item)
        assert result["status"] == "applied"
        assert result["changed_count"] == 1
        assert str(item.trigger_price) == "99.0000"
        assert str(item.limit_price) == "99.9900"
        assert db.scalar(select(ExecutionPlanItemAudit).where(ExecutionPlanItemAudit.execution_plan_item_id == item.id))
    engine.dispose()


def test_postgres_expiry_preserves_status_compare_and_set_before_audit(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(runtime_mode="live", database_url="postgresql+psycopg://unused/test")
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-expiry-race", source="test", mode="live")
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add_all([plan, item])
        db.commit()
        item_id = item.id

    original_loader = plan_judgment_actions._load_mutable_buy_items_session

    def load_then_change_status(db: Session, settings: Settings, *, after=None, statuses=None):
        items = original_loader(
            db,
            settings,
            after=after,
            **({"statuses": statuses} if statuses is not None else {}),
        )
        db.execute(
            update(ExecutionPlanItem)
            .where(ExecutionPlanItem.id == item_id)
            .values(status="watch_buy_submitted")
            .execution_options(synchronize_session=False)
        )
        return items

    monkeypatch.setattr(plan_judgment_actions, "_load_mutable_buy_items_session", load_then_change_status)

    with Session(engine) as db:
        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=3,
            risk_reasons=["race-test"],
        )

    with Session(engine) as db:
        item = db.get(ExecutionPlanItem, item_id)
        audits = db.scalars(select(ExecutionPlanItemAudit)).all()
        assert result["changed_count"] == 0
        assert item is not None
        assert item.status == "watch_buy_submitted"
        assert item.risk_context == {}
        assert audits == []
    engine.dispose()


def test_postgres_caution_processes_unadjusted_watch_after_limit_of_adjusted_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(runtime_mode="live", database_url="postgresql+psycopg://unused/test")
    already_adjusted_context = {
        ADJUSTMENT_KEY: {
            "action": "conservative_price_adjust",
            "original": {"trigger_price": "100.0000", "limit_price": "101.0000"},
        }
    }
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-caution-over-limit", source="test", mode="live")
        for index in range(MAX_MUTATIONS_PER_RUN):
            db.add(
                ExecutionPlanItem(
                    plan=plan,
                    plan_item_index=index,
                    symbol=f"{index:06d}",
                    side="buy",
                    intent="intraday_watch_buy",
                    status="watch_monitoring_active",
                    trigger_price=99,
                    limit_price=99.99,
                    risk_context=already_adjusted_context,
                )
            )
        late_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=MAX_MUTATIONS_PER_RUN,
            symbol="999999",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=100,
            limit_price=101,
            risk_context={},
        )
        db.add(late_item)
        db.commit()
        late_item_id = late_item.id

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=1,
            risk_reasons=["over-limit"],
        )

        db.refresh(late_item)
        adjusted_audits = db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItemAudit)
            .where(ExecutionPlanItemAudit.event_type == "hourly_plan_judgment_buy_watch_price_adjusted")
        )
        assert result["changed_count"] == 1
        assert late_item.id == late_item_id
        assert str(late_item.trigger_price) == "99.0000"
        assert str(late_item.limit_price) == "99.9900"
        assert adjusted_audits == 1
    engine.dispose()


def test_postgres_caution_does_not_mutate_core_v4_bound_watch() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(runtime_mode="live", database_url="postgresql+psycopg://unused/test")
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-core-v4-caution", source="core", mode="live")
        item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=0,
            symbol="123456",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_pending",
            trigger_price=100,
            limit_price=101,
            risk_context={"_core_v4_contract_binding": {"version": "v1"}},
        )
        db.add(item)
        db.commit()

        result = apply_hourly_plan_judgment_action(
            settings=settings,
            db=db,
            risk_flags=1,
            risk_reasons=["caution"],
        )

        db.refresh(item)
        assert result["changed_count"] == 0
        assert str(item.trigger_price) == "100.0000"
        assert str(item.limit_price) == "101.0000"
        assert ADJUSTMENT_KEY not in item.risk_context
    engine.dispose()


def test_postgres_restore_processes_adjusted_watch_after_limit_of_unadjusted_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    settings = Settings(runtime_mode="live", database_url="postgresql+psycopg://unused/test")
    with Session(engine) as db:
        plan = ExecutionPlan(external_plan_id="plan-pg-restore-over-limit", source="test", mode="live")
        for index in range(MAX_MUTATIONS_PER_RUN):
            db.add(
                ExecutionPlanItem(
                    plan=plan,
                    plan_item_index=index,
                    symbol=f"{index:06d}",
                    side="buy",
                    intent="intraday_watch_buy",
                    status="watch_monitoring_active",
                    trigger_price=100,
                    limit_price=101,
                    risk_context={},
                )
            )
        late_item = ExecutionPlanItem(
            plan=plan,
            plan_item_index=MAX_MUTATIONS_PER_RUN,
            symbol="999999",
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            trigger_price=99,
            limit_price=99.99,
            risk_context={
                ADJUSTMENT_KEY: {
                    "action": "conservative_price_adjust",
                    "original": {"trigger_price": "100.0000", "limit_price": "101.0000"},
                }
            },
        )
        db.add(late_item)
        db.commit()

        result = apply_hourly_plan_judgment_action(settings=settings, db=db, risk_flags=0, risk_reasons=[])

        db.refresh(late_item)
        restored_audits = db.scalar(
            select(func.count())
            .select_from(ExecutionPlanItemAudit)
            .where(ExecutionPlanItemAudit.event_type == "hourly_plan_judgment_buy_watch_price_restored")
        )
        assert result["changed_count"] == 1
        assert str(late_item.trigger_price) == "100.0000"
        assert str(late_item.limit_price) == "101.0000"
        assert late_item.risk_context == {}
        assert restored_audits == 1
    engine.dispose()
