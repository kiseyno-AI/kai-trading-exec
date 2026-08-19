from __future__ import annotations

import pytest

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services import monitoring


def _session():
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return settings, engine, create_session_factory(engine)()


def _add_item(db, *, symbol: str, asset_class: str, risk_context: dict[str, object]) -> None:
    plan = ExecutionPlan(
        external_plan_id=f"route-{symbol}",
        source="test",
        mode="paper",
        status="accepted",
    )
    plan.items.append(
        ExecutionPlanItem(
            plan_item_index=0,
            symbol=symbol,
            side="buy",
            intent="next_open_buy",
            status="pending",
            provider="kis",
            venue="kis",
            asset_class=asset_class,
            risk_context=risk_context,
        )
    )
    db.add(plan)
    db.commit()


def test_kis_monitoring_position_sync_splits_krx_and_us_exchange(monkeypatch) -> None:
    settings, engine, db = _session()
    _add_item(db, symbol="AAPL", asset_class="us_equity", risk_context={"exchange": "NASD"})
    _add_item(db, symbol="005930", asset_class="stock", risk_context={"exchange": "KRX"})
    calls: list[tuple[str, set[str]]] = []

    def fake_sync(**kwargs):
        calls.append((kwargs["exchange"], kwargs["symbols"]))

    monkeypatch.setattr(monitoring, "sync_execution_positions_from_broker_snapshot", fake_sync)

    monitoring._sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=type("Adapter", (), {"provider": "kis"})(),
        symbols={"AAPL", "005930"},
    )

    assert calls == [("KRX", {"005930"}), ("NASD", {"AAPL"})]
    db.close()
    engine.dispose()


def test_kis_monitoring_position_sync_fails_closed_without_us_exchange(monkeypatch) -> None:
    settings, engine, db = _session()
    _add_item(db, symbol="AAPL", asset_class="us_equity", risk_context={})
    calls: list[object] = []
    monkeypatch.setattr(
        monitoring,
        "sync_execution_positions_from_broker_snapshot",
        lambda **kwargs: calls.append(kwargs),
    )

    with pytest.raises(ValueError, match="explicit overseas exchange"):
        monitoring._sync_positions_for_monitoring_routes(
            settings=settings,
            db=db,
            adapter=type("Adapter", (), {"provider": "kis"})(),
            symbols={"AAPL"},
        )

    assert calls == []
    db.close()
    engine.dispose()
