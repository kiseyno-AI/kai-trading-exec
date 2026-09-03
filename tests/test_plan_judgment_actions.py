import sqlite3

from sqlalchemy import create_engine, func, select, update
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit
from app.services import plan_judgment_actions
from app.services.plan_judgment_actions import ADJUSTMENT_KEY, MAX_MUTATIONS_PER_RUN, apply_hourly_plan_judgment_action


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

    def load_then_change_status(db: Session, settings: Settings, *, after=None):
        items = original_loader(db, settings, after=after)
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
