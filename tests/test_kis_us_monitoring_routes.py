from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
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


def test_kis_monitoring_position_discovery_queries_krx_and_supported_us_exchanges(
    monkeypatch,
) -> None:
    settings, engine, db = _session()
    calls: list[tuple[str, set[str] | None, bool]] = []

    def fake_sync(**kwargs):
        calls.append(
            (
                kwargs["exchange"],
                kwargs["symbols"],
                kwargs.get("discover_holdings_only", False),
            )
        )

    monkeypatch.setattr(monitoring, "sync_execution_positions_from_broker_snapshot", fake_sync)

    monitoring._sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=type("Adapter", (), {"provider": "kis"})(),
        symbols=None,
        discover_us_holdings=True,
    )

    assert calls == [
        ("AMEX", None, True),
        ("KRX", None, True),
        ("NASD", None, True),
        ("NYSE", None, True),
    ]
    db.close()
    engine.dispose()


def test_kis_monitoring_routes_existing_us_position_from_broker_truth(monkeypatch) -> None:
    settings, engine, db = _session()
    db.add(
        ExecutionPosition(
            mode="paper",
            symbol="AAPL",
            quantity=2,
            average_price=Decimal("200"),
            updated_from="reconciliation",
            raw_payload={
                "currency": "USD",
                "exchange": "NASD",
                "_position_sync": {
                    "asset_class": "us_equity",
                    "exchange": "NASD",
                    "monitoring_eligible": True,
                },
            },
        )
    )
    db.commit()
    calls: list[tuple[str, set[str]]] = []

    def fake_sync(**kwargs):
        calls.append((kwargs["exchange"], kwargs["symbols"]))

    monkeypatch.setattr(monitoring, "sync_execution_positions_from_broker_snapshot", fake_sync)

    monitoring._sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=type("Adapter", (), {"provider": "kis"})(),
        symbols={"AAPL"},
    )

    assert calls == [("NASD", {"AAPL"})]
    db.close()
    engine.dispose()


def test_supported_existing_us_holding_gets_us_target_and_stop_monitoring() -> None:
    settings, engine, db = _session()
    _add_item(db, symbol="AAPL", asset_class="us_equity", risk_context={"exchange": "NASD"})
    position = ExecutionPosition(
        mode="paper",
        symbol="AAPL",
        quantity=2,
        average_price=Decimal("200"),
        updated_from="reconciliation",
        raw_payload={
            "currency": "USD",
            "exchange": "NASD",
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "NASD",
                "monitoring_eligible": True,
            },
        },
    )
    db.add(position)
    db.commit()

    maintenance_count, review_count = monitoring._maintain_post_entry_exit_monitors(
        settings=settings,
        db=db,
        positions_by_symbol={"AAPL": position},
        prices={"AAPL": MonitoringPriceRequest(symbol="AAPL", price=Decimal("205"))},
        triggered_at=datetime(2026, 8, 19, 15, 0, tzinfo=UTC),
    )
    db.commit()

    monitors = db.scalars(
        select(ExecutionPlanItem)
        .where(ExecutionPlanItem.symbol == "AAPL", ExecutionPlanItem.side == "sell")
        .order_by(ExecutionPlanItem.intent)
    ).all()
    assert maintenance_count == 2
    assert review_count == 0
    assert {item.intent for item in monitors} == {"stop_sell", "target_sell"}
    assert all(item.asset_class == "us_equity" for item in monitors)
    assert all(item.risk_context["exchange"] == "NASD" for item in monitors)
    assert all(item.venue == "kis" for item in monitors)
    db.close()
    engine.dispose()


def test_unsupported_unpriced_legacy_us_holding_is_visible_but_requires_manual_review() -> None:
    settings, engine, db = _session()
    _add_item(db, symbol="AAPL", asset_class="us_equity", risk_context={"exchange": "NASD"})
    position = ExecutionPosition(
        mode="paper",
        symbol="OLDCO",
        quantity=7,
        average_price=Decimal("12.50"),
        updated_from="reconciliation",
        raw_payload={
            "currency": "USD",
            "exchange": "OTC",
            "_position_sync": {
                "asset_class": "us_equity",
                "exchange": "OTC",
                "monitoring_eligible": False,
                "manual_review_reason": "unsupported_overseas_exchange",
            },
        },
    )
    db.add(position)
    db.commit()

    maintenance_count, review_count = monitoring._maintain_post_entry_exit_monitors(
        settings=settings,
        db=db,
        positions_by_symbol={"OLDCO": position},
        prices={},
        triggered_at=datetime(2026, 8, 19, 15, 0, tzinfo=UTC),
    )
    db.commit()

    assert maintenance_count == 0
    assert review_count == 1
    assert db.scalars(
        select(ExecutionPlanItem).where(
            ExecutionPlanItem.symbol == "OLDCO",
            ExecutionPlanItem.side == "sell",
        )
    ).all() == []
    assert position.quantity == 7
    assert position.raw_payload["_position_sync"]["manual_review_required"] is True
    db.close()
    engine.dispose()
