import sqlite3

from app.core.config import Settings
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
