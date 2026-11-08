from datetime import UTC, datetime
import time

import pytest

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit
from app.services.account_drawdown_gate import BLOCK_EVENT_TYPE, latest_account_drawdown_gate_state


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="requires host timezone control")
@pytest.mark.parametrize("gate_name", ["daily_total_asset_drawdown_pct", "weekly_total_asset_drawdown_pct"])
@pytest.mark.parametrize("observed_hour, expected_gated", [(22, True), (12, False)])
def test_persisted_utc_drawdown_period_does_not_depend_on_host_timezone(
    monkeypatch: pytest.MonkeyPatch,
    gate_name: str,
    observed_hour: int,
    expected_gated: bool,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    factory = create_session_factory(engine)
    try:
        with factory() as db:
            plan = ExecutionPlan(external_plan_id="legacy-period", source="test", mode="paper")
            item = ExecutionPlanItem(plan=plan, symbol="005930", side="buy", intent="next_open_buy")
            db.add(item)
            db.flush()
            audit = ExecutionPlanItemAudit(
                execution_plan_item_id=item.id,
                event_type=BLOCK_EVENT_TYPE,
                source="broker_order_service",
                occurred_at=datetime(2026, 11, 8, observed_hour, tzinfo=UTC),
                summary={"gate_name": gate_name, "order_idempotency_key": "legacy-period-order"},
            )
            db.add(audit)
            db.commit()
            db.refresh(audit)
            assert audit.occurred_at.tzinfo is None
            try:
                with monkeypatch.context() as environment:
                    environment.setenv("TZ", "Asia/Seoul")
                    time.tzset()
                    result = latest_account_drawdown_gate_state(
                        settings=settings,
                        db=db,
                        as_of=datetime(2026, 11, 9, 1, tzinfo=UTC),
                    )
            finally:
                time.tzset()
            assert result["gated"] is expected_gated
    finally:
        engine.dispose()
