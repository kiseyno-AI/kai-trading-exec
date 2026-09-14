from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerReconnectHandle,
    BrokerTokenGrant,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services import monitoring
from app.services.monitoring_price_source import _load_monitoring_exchange_by_symbol
from app.services.position_identity import build_position_identity


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


def test_kis_monitoring_position_discovery_queries_supported_us_exchanges(
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
        ("NASD", None, True),
        ("NYSE", None, True),
    ]
    db.close()
    engine.dispose()


def test_kis_monitoring_position_discovery_can_scope_to_krx(monkeypatch) -> None:
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
        discover_krx_holdings=True,
    )

    assert calls == [("KRX", None, True)]
    db.close()
    engine.dispose()


def test_kis_monitoring_loader_ignores_foreign_provider_stop_without_mutating_it() -> None:
    settings, engine, db = _session()
    kis_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    foreign_stop = ExecutionPlanItem(
        id="alpaca-aapl-stop-owned-by-foreign-coordinator",
        plan_item_index=0,
        symbol="AAPL",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        trigger_price=Decimal("190"),
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={"exchange": "US"},
    )
    db.add_all(
        [
            ExecutionPlan(
                external_plan_id="foreign-alpaca-monitoring-plan",
                source="alpaca_us_equity_strategy",
                mode="paper",
                status="accepted",
                items=[foreign_stop],
            ),
            ExecutionPosition(
                mode="paper",
                provider=kis_identity.provider,
                account_scope=kis_identity.account_scope,
                exchange=kis_identity.exchange,
                symbol="AAPL",
                quantity=1,
                average_price=Decimal("200"),
                updated_from="reconciliation",
            ),
        ]
    )
    db.commit()

    loaded = monitoring._load_monitored_items(
        settings=settings,
        db=db,
        symbols=["AAPL"],
    )

    assert loaded == []
    db.refresh(foreign_stop)
    assert foreign_stop.status == "stop_sell_monitoring"
    assert "last_monitoring_event" not in (foreign_stop.risk_context or {})
    db.close()
    engine.dispose()


def test_kis_krx_discovery_zeroes_preexisting_broker_absent_holding() -> None:
    settings, engine, db = _session()
    identity = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="005930",
            quantity=7,
            average_price=Decimal("70000"),
            updated_from="reconciliation",
        )
    )
    db.commit()

    class FlatSnapshotBroker:
        provider = "kis"

        def __init__(self) -> None:
            self.snapshot_calls: list[tuple[list[str] | None, str]] = []

        def describe(self) -> BrokerConfig:
            return BrokerConfig(
                provider="kis",
                mode="paper",
                configured=True,
                api_base_url="https://stub.kis.local",
            )

        def authenticate(self) -> BrokerTokenGrant:
            return BrokerTokenGrant(
                access_token="test-token",
                token_type="Bearer",
                access_token_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
            )

        def reconnect_handle(self) -> BrokerReconnectHandle:
            return BrokerReconnectHandle(supported=True, strategy="stub")

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            assert access_token
            self.snapshot_calls.append((symbols, exchange))
            return BrokerAccountSnapshot(
                available_cash=Decimal("1000000"),
                observed_at=datetime(2026, 8, 19, 1, 0, tzinfo=UTC),
                source="stub:account_snapshot",
                holdings=(
                    [
                        BrokerAccountHolding(
                            symbol="000660",
                            quantity=3,
                            average_price=Decimal("120000"),
                        )
                    ]
                    if symbols is None
                    else []
                ),
            )

    broker = FlatSnapshotBroker()

    monitoring._sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=broker,
        symbols=None,
        discover_krx_holdings=True,
    )

    assert broker.snapshot_calls == [
        (None, "KRX"),
        (["005930"], "KRX"),
    ]
    existing = db.scalar(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "005930")
    )
    assert existing is not None
    assert existing.quantity == 0
    discovered = db.scalar(
        select(ExecutionPosition).where(ExecutionPosition.symbol == "000660")
    )
    assert discovered is not None
    assert discovered.quantity == 3
    db.close()
    engine.dispose()


def test_live_position_truth_refresh_propagates_snapshot_sync_failure(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    db.add(
        ExecutionPosition(
            mode="live",
            symbol="005930",
            quantity=7,
            average_price=Decimal("70000"),
            updated_from="reconciliation",
            last_reconciled_at=datetime(2026, 8, 19, 0, 0, tzinfo=UTC),
        )
    )
    db.commit()

    def fail_position_sync(**kwargs) -> None:
        del kwargs
        raise RuntimeError("broker snapshot unavailable")

    monkeypatch.setattr(
        monitoring,
        "_sync_positions_for_monitoring_routes",
        fail_position_sync,
    )

    with pytest.raises(RuntimeError, match="broker snapshot unavailable"):
        monitoring._refresh_position_truth_if_stale(
            settings=settings,
            db=db,
            adapter=type("Adapter", (), {"provider": "kis"})(),
            symbols={"005930"},
        )

    db.close()
    engine.dispose()


def test_kis_monitoring_routes_existing_us_position_from_broker_truth(monkeypatch) -> None:
    settings, engine, db = _session()
    identity = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
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


def test_kis_monitoring_position_sync_prefers_exact_position_exchange_over_newer_plan(
    monkeypatch,
) -> None:
    settings, engine, db = _session()
    identity = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="ROUTE",
            quantity=2,
            average_price=Decimal("200"),
            updated_from="reconciliation",
        )
    )
    _add_item(
        db,
        symbol="ROUTE",
        asset_class="us_equity",
        risk_context={"exchange": "NYSE"},
    )
    calls: list[tuple[str, set[str]]] = []

    monkeypatch.setattr(
        monitoring,
        "sync_execution_positions_from_broker_snapshot",
        lambda **kwargs: calls.append((kwargs["exchange"], kwargs["symbols"])),
    )

    monitoring._sync_positions_for_monitoring_routes(
        settings=settings,
        db=db,
        adapter=type("Adapter", (), {"provider": "kis"})(),
        symbols={"ROUTE"},
    )

    assert calls == [("NASD", {"ROUTE"})]
    db.close()
    engine.dispose()


def test_kis_quote_route_uses_verified_position_exchange_without_plan() -> None:
    settings, engine, db = _session()
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    db.add(
        ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="AAPL",
            quantity=2,
            average_price=Decimal("200"),
            updated_from="reconciliation",
            raw_payload={
                "_position_sync": {
                    "asset_class": "us_equity",
                    "exchange": "NASD",
                    "monitoring_eligible": True,
                }
            },
        )
    )
    db.commit()

    assert _load_monitoring_exchange_by_symbol(
        settings=settings,
        db=db,
        symbols=["AAPL"],
    ) == {"AAPL": "NASD"}

    db.close()
    engine.dispose()


def test_supported_existing_us_holding_gets_us_target_and_stop_monitoring() -> None:
    settings, engine, db = _session()
    _add_item(db, symbol="AAPL", asset_class="us_equity", risk_context={"exchange": "NASD"})
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
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


def test_quarantine_attribution_does_not_expand_monitoring_exchanges() -> None:
    assert set(monitoring.MONITORED_KIS_POSITION_EXCHANGES).isdisjoint(
        {"OTCB", "OOTC"}
    )
