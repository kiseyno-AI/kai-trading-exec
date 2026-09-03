import sqlite3

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit
from app.services.plan_judgment_actions import apply_hourly_plan_judgment_action


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
