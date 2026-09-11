from __future__ import annotations

from collections import deque
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from app.api.schemas.execution import MonitoringPriceRequest
from app.broker.base import BrokerAccountSnapshot, BrokerCurrentPrice
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.autonomous_runtime import (
    AutonomousRuntimeCoordinator,
    CachedMonitoringItem,
    CachedMonitoringPosition,
)
from app.services.execution_account_route import bind_kis_account_route
from app.services.exec_agent_approval import delegated_buy_gate_rejections
from app.services.json_compaction import compact_json_object, json_payload_hash
from app.services import monitoring as monitoring_service
from app.services import monitoring_price_source
from app.services import exec_agent as exec_agent_service
from app.services import short_term_exec_agent as short_term_exec_agent_service
from app.services import short_term_watcher as short_term_watcher_service
from app.services import trigger_time_exec_agent as trigger_time_exec_agent_service
from app.services import max_holding_actionability as max_holding_actionability_service
from app.services import portfolio_risk as portfolio_risk_service
from app.services import position_truth_remediation as position_truth_remediation_service
from app.services import broker_orders as broker_orders_service
from app.services import broker_raw_holdings as broker_raw_holdings_service
from app.services import runtime as runtime_service
from app.services.fifo_inventory import FifoRouteScope
from app.services.monitoring import _load_monitored_items, list_autonomous_monitoring_symbols
from app.services.domestic_order_router import RouteDecision
from app.services.position_identity import (
    PositionIdentity,
    build_position_identity,
    execution_scope_payload,
)


def _monitoring_order(
    settings: Settings,
    *,
    symbol: str,
    side: str,
    status: str,
    exchange: str = "KRX",
    account_scope: str | None = None,
    plan_item: ExecutionPlanItem | None = None,
    created_at: datetime | None = None,
) -> BrokerOrder:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange=exchange,
    )
    scope = execution_scope_payload(
        identity=identity,
        venue=exchange,
        asset_class="kr_equity" if exchange == "KRX" else "us_equity",
    )
    if account_scope is not None:
        scope["account_scope"] = account_scope
    return BrokerOrder(
        broker_name="kis",
        mode=settings.runtime_mode,
        idempotency_key=(
            f"monitoring-scope-{symbol}-{side}-{status}-{account_scope or exchange}"
        ),
        symbol=symbol,
        side=side,
        status=status,
        quantity=1,
        venue=exchange,
        asset_class="kr_equity" if exchange == "KRX" else "us_equity",
        execution_plan_item_id=plan_item.id if plan_item is not None else None,
        request_metadata={"exchange": exchange, "execution_scope": scope},
        created_at=created_at,
    )


def test_live_cached_monitoring_never_uses_last_price_without_executable_side() -> None:
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("100"),
        observed_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
    )
    buy = CachedMonitoringItem(
        id="buy",
        symbol="005930",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        trigger_price=Decimal("100"),
        trigger_direction="at_or_above",
    )
    sell = CachedMonitoringItem(
        id="sell",
        symbol="005930",
        intent="stop_sell",
        status="stop_sell_monitoring",
        trigger_price=Decimal("100"),
        trigger_direction=None,
    )

    assert monitoring_service._observed_price_for_side(
        price=price,
        side="buy",
        runtime_mode="live",
    ) is None
    assert monitoring_service._observed_price_for_side(
        price=price,
        side="sell",
        runtime_mode="live",
    ) is None
    from app.services import autonomous_runtime

    assert autonomous_runtime._cached_executable_price(
        price=price,
        item=buy,
        runtime_mode="live",
    ) is None
    assert autonomous_runtime._cached_executable_price(
        price=price,
        item=sell,
        runtime_mode="live",
    ) is None


def test_paper_monitoring_may_explicitly_fall_back_to_last_price() -> None:
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("100"),
        observed_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
    )
    item = CachedMonitoringItem(
        id="paper-buy",
        symbol="005930",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        trigger_price=Decimal("100"),
        trigger_direction="at_or_above",
    )

    assert monitoring_service._observed_price_for_side(
        price=price,
        side="buy",
        runtime_mode="paper",
    ) == Decimal("100")
    from app.services import autonomous_runtime

    assert autonomous_runtime._cached_executable_price(
        price=price,
        item=item,
        runtime_mode="paper",
    ) == Decimal("100")


def test_cached_sell_monitor_requires_exact_position_exchange() -> None:
    from app.services import autonomous_runtime

    item = CachedMonitoringItem(
        id="nyse-stop",
        symbol="AAPL",
        intent="stop_sell",
        status="stop_sell_monitoring",
        trigger_price=Decimal("180"),
        provider="kis",
        exchange="NYSE",
    )
    position = CachedMonitoringPosition(
        symbol="AAPL",
        quantity=1,
        provider="kis",
        exchange="NASD",
    )

    assert autonomous_runtime._cached_item_matches_position_route(
        item=item,
        position=position,
    ) is False


def test_sell_monitor_position_match_requires_current_bound_account_route() -> None:
    settings = Settings(
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = Settings(
        runtime_mode="paper",
        kis_account_number="99999999",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=3,
    )
    matching_risk, matching_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 3},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 3},
    )

    def stop_item(
        *,
        item_id: str,
        risk_context: dict[str, object] | None,
        quantity_policy: dict[str, object] | None,
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan_item_index=0,
            symbol="005930",
            side="sell",
            intent="stop_sell",
            status="stop_sell_monitoring",
            provider="kis",
            asset_class="stock",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    positions = {"005930": position}
    assert monitoring_service._position_for_monitoring_item(
        settings=settings,
        item=stop_item(
            item_id="matching-stop",
            risk_context=matching_risk,
            quantity_policy=matching_policy,
        ),
        positions_by_symbol=positions,
    ) is position
    for domestic_route in ("NXT", "SOR", "KOSPI", "KOSDAQ"):
        route_risk, route_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": domestic_route},
            quantity_policy={"kind": "shares", "quantity": 3},
        )
        assert monitoring_service._position_for_monitoring_item(
            settings=settings,
            item=stop_item(
                item_id=f"{domestic_route.lower()}-stop",
                risk_context=route_risk,
                quantity_policy=route_policy,
            ),
            positions_by_symbol=positions,
        ) is position
    assert monitoring_service._position_for_monitoring_item(
        settings=settings,
        item=stop_item(
            item_id="foreign-stop",
            risk_context=foreign_risk,
            quantity_policy=foreign_policy,
        ),
        positions_by_symbol=positions,
    ) is None
    assert monitoring_service._position_for_monitoring_item(
        settings=settings,
        item=stop_item(
            item_id="unbound-stop",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 3},
        ),
        positions_by_symbol=positions,
    ) is None
    wrong_exchange_risk = dict(matching_risk or {})
    wrong_exchange_risk["exchange"] = "NYSE"
    assert monitoring_service._position_for_monitoring_item(
        settings=settings,
        item=stop_item(
            item_id="wrong-exchange-stop",
            risk_context=wrong_exchange_risk,
            quantity_policy=matching_policy,
        ),
        positions_by_symbol=positions,
    ) is None


@pytest.mark.parametrize("route_exchange", ["NXT", "SOR", "KOSPI", "KOSDAQ"])
def test_domestic_route_order_matches_canonical_krx_position(
    route_exchange: str,
) -> None:
    settings = Settings(
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=3,
    )
    order = _monitoring_order(
        settings,
        symbol="005930",
        side="sell",
        status="open",
        exchange=route_exchange,
    )

    assert monitoring_service._active_kis_order_matches_position(
        settings=settings,
        order=order,
        position=position,
    ) is True


def test_flat_position_expiry_preserves_foreign_account_sell_monitors() -> None:
    current_settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
    )
    foreign_settings = current_settings.model_copy(
        update={"kis_account_number": "87654321"}
    )
    engine = create_db_engine(current_settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    current_identity = build_position_identity(
        settings=current_settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=current_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    current_item = ExecutionPlanItem(
        id="current-flat-stop",
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="stop_sell_pending_config",
        provider="kis",
        asset_class="stock",
        risk_context=current_risk,
        quantity_policy=current_policy,
    )
    foreign_item = ExecutionPlanItem(
        id="foreign-flat-stop",
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="stop_sell_pending_config",
        provider="kis",
        asset_class="stock",
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=current_identity.provider,
                        account_scope=current_identity.account_scope,
                        exchange=current_identity.exchange,
                        symbol="005930",
                        quantity=0,
                    ),
                    ExecutionPlan(
                        id="current-flat-plan",
                        external_plan_id="current-flat-plan",
                        source="test",
                        mode="paper",
                        items=[current_item],
                    ),
                    ExecutionPlan(
                        id="foreign-flat-plan",
                        external_plan_id="foreign-flat-plan",
                        source="test",
                        mode="paper",
                        items=[foreign_item],
                    ),
                ]
            )
            db.commit()

            expired = monitoring_service.expire_known_flat_pending_sell_monitors(
                settings=current_settings,
                db=db,
                expired_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
            )

            assert expired == 1
            assert current_item.status == "expired"
            assert foreign_item.status == "stop_sell_pending_config"
    finally:
        engine.dispose()


def test_foreign_stop_cannot_suppress_current_account_stop_coverage_repair() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 3},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 3},
    )

    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="current-account-repairable-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_stop = ExecutionPlanItem(
                id="foreign-account-active-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            position = ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="005930",
                quantity=3,
                average_price=Decimal("100"),
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="current-account-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-account-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[foreign_stop],
                    ),
                    position,
                ]
            )
            db.commit()

            issues, repaired_count = (
                monitoring_service._repair_unprotected_stop_coverage(
                    settings=settings,
                    db=db,
                    positions_by_symbol={"005930": position},
                    triggered_at=datetime(2026, 9, 8, 8, tzinfo=UTC),
                )
            )

            assert repaired_count == 1
            assert current_stop.status == "stop_sell_monitoring"
            assert current_stop.quantity_policy == {"kind": "shares", "quantity": 3}
            assert foreign_stop.status == "stop_sell_monitoring"
            assert "last_monitoring_event" not in foreign_stop.risk_context
            assert all(issue.outcome != "unprotected_position" for issue in issues)
    finally:
        engine.dispose()


def test_foreign_stop_cannot_suppress_current_account_default_exit_creation() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 3},
    )

    try:
        with session_factory() as db:
            foreign_stop = ExecutionPlanItem(
                id="foreign-default-exit-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                venue="krx",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            plan = ExecutionPlan(
                id="default-exit-repair-plan",
                external_plan_id="default-exit-repair-plan",
                source="test",
                market_date=date(2026, 9, 8),
                accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[foreign_stop],
            )
            position = ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="005930",
                quantity=3,
                average_price=Decimal("100"),
            )
            db.add_all([plan, position])
            db.commit()

            created_count, manual_review = (
                monitoring_service._ensure_existing_position_default_exit_monitors(
                    settings=settings,
                    db=db,
                    position=position,
                    observed_price=None,
                    triggered_at=datetime(2026, 9, 8, 8, tzinfo=UTC),
                )
            )
            db.flush()

            created = db.scalars(
                select(ExecutionPlanItem).where(
                    ExecutionPlanItem.execution_plan_id == plan.id,
                    ExecutionPlanItem.id != foreign_stop.id,
                )
            ).all()
            assert manual_review is False
            assert created_count == 2
            assert {item.intent for item in created} == {"stop_sell", "target_sell"}
            assert all(
                monitoring_service.classify_kis_account_route(
                    settings=settings,
                    provider=item.provider,
                    risk_context=item.risk_context,
                    quantity_policy=item.quantity_policy,
                )
                == "active"
                for item in created
            )
            assert foreign_stop.status == "stop_sell_monitoring"
            assert "last_monitoring_event" not in foreign_stop.risk_context
    finally:
        engine.dispose()


def test_foreign_newer_stop_cannot_supersede_matching_account_protection() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="matching-older-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_stop = ExecutionPlanItem(
                id="foreign-newer-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("91"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="matching-older-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-newer-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[foreign_stop],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol="005930",
                        quantity=2,
                        average_price=Decimal("100"),
                    ),
                ]
            )
            db.commit()

            items = _load_monitored_items(
                settings=settings,
                db=db,
                symbols=["005930"],
            )

            assert [item.id for item in items] == ["matching-older-stop"]
            assert current_stop.status == "stop_sell_monitoring"
            assert foreign_stop.status == "stop_sell_monitoring"
    finally:
        engine.dispose()


def test_foreign_replanned_stop_cannot_replace_matching_account_monitor() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="current-replanned-source-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="core-entry-1",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="current-replanned-source-plan",
                    source="test",
                    market_date=date(2026, 9, 7),
                    accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[current_stop],
                )
            )
            db.commit()

            foreign_stop = ExecutionPlanItem(
                id="foreign-replanned-source-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="core-entry-1",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("80"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            foreign_plan = ExecutionPlan(
                external_plan_id="foreign-replanned-source-plan",
                source="test",
                market_date=date(2026, 9, 8),
                accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[foreign_stop],
            )

            monitoring_service.materialize_monitoring_plan_items(
                foreign_plan,
                db=db,
            )

            assert current_stop.status == "stop_sell_monitoring"
            assert current_stop.trigger_price == Decimal("90")
            assert foreign_stop.status == "stop_sell_monitoring"
    finally:
        engine.dispose()


@pytest.mark.parametrize("replanned_market_date", [date(2026, 9, 10), date(2099, 1, 1)])
def test_temporally_invalid_replan_cannot_mutate_current_protective_monitor(
    replanned_market_date: date,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="current-temporal-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="core-temporal-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="current-temporal-plan",
                    source="test",
                    market_date=date(2026, 9, 11),
                    accepted_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[current_stop],
                )
            )
            db.add(
                ExecutionPosition(
                    mode="paper",
                    provider=identity.provider,
                    account_scope=identity.account_scope,
                    exchange=identity.exchange,
                    symbol="005930",
                    quantity=2,
                    average_price=Decimal("100"),
                )
            )
            db.commit()

            replanned_stop = ExecutionPlanItem(
                id=f"replanned-temporal-stop-{replanned_market_date.isoformat()}",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="core-temporal-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("10"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            replanned_plan = ExecutionPlan(
                external_plan_id=f"replanned-temporal-{replanned_market_date.isoformat()}",
                source="test",
                market_date=replanned_market_date,
                accepted_at=datetime(2026, 9, 11, 2, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[replanned_stop],
            )

            monitoring_service.materialize_monitoring_plan_items(
                replanned_plan,
                db=db,
                effective_at=datetime(2026, 9, 11, 3, tzinfo=UTC),
            )

            assert current_stop.status == "stop_sell_monitoring"
            assert current_stop.trigger_price == Decimal("90")
            assert current_stop.decision_snapshot_id is None
            db.add(replanned_plan)
            db.commit()

            selected = monitoring_service._load_monitored_items(
                settings=settings,
                db=db,
                symbols={"005930"},
                current_market_date=date(2026, 9, 11),
            )

            assert current_stop.status == "stop_sell_monitoring"
            assert current_stop in selected
            assert replanned_stop not in selected
    finally:
        engine.dispose()


def test_delayed_same_day_replan_cannot_downgrade_newer_core_protection() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="newer-core-request-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="core-sequenced-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                decision_snapshot_id="newer",
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="newer-core-request-plan",
                        source="test",
                        market_date=date(2026, 9, 11),
                        requested_at=datetime(2026, 9, 11, 12, 5, tzinfo=UTC),
                        accepted_at=datetime(2026, 9, 11, 12, 5, 30, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol="005930",
                        quantity=2,
                        average_price=Decimal("100"),
                    ),
                ]
            )
            db.commit()

            delayed_stop = ExecutionPlanItem(
                id="delayed-older-core-request-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="core-sequenced-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("10"),
                decision_snapshot_id="older",
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            delayed_plan = ExecutionPlan(
                external_plan_id="delayed-older-core-request-plan",
                source="test",
                market_date=date(2026, 9, 11),
                requested_at=datetime(2026, 9, 11, 12, tzinfo=UTC),
                accepted_at=datetime(2026, 9, 11, 12, 7, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[delayed_stop],
            )

            monitoring_service.materialize_monitoring_plan_items(
                delayed_plan,
                db=db,
                effective_at=datetime(2026, 9, 11, 12, 8, tzinfo=UTC),
            )

            assert current_stop.trigger_price == Decimal("90")
            assert current_stop.decision_snapshot_id == "newer"
            db.add(delayed_plan)
            db.commit()

            selected = monitoring_service._load_monitored_items(
                settings=settings,
                db=db,
                symbols={"005930"},
                current_market_date=date(2026, 9, 11),
            )

            assert current_stop in selected
            assert delayed_stop not in selected
            assert current_stop.status == "stop_sell_monitoring"

            latest_stop = ExecutionPlanItem(
                id="latest-core-request-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="core-sequenced-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("80"),
                decision_snapshot_id="latest",
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            latest_plan = ExecutionPlan(
                external_plan_id="latest-core-request-plan",
                source="test",
                market_date=date(2026, 9, 11),
                requested_at=datetime(2026, 9, 11, 12, 10, tzinfo=UTC),
                accepted_at=datetime(2026, 9, 11, 12, 11, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[latest_stop],
            )
            monitoring_service.materialize_monitoring_plan_items(
                latest_plan,
                db=db,
                effective_at=datetime(2026, 9, 11, 12, 11, tzinfo=UTC),
            )
            db.add(latest_plan)
            db.commit()

            assert current_stop.trigger_price == Decimal("80")
            assert current_stop.decision_snapshot_id == "latest"
            assert latest_stop.status == "superseded"

            delayed_after_latest_stop = ExecutionPlanItem(
                id="delayed-after-latest-core-request-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="core-sequenced-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("20"),
                decision_snapshot_id="delayed-after-latest",
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            delayed_after_latest_plan = ExecutionPlan(
                external_plan_id="delayed-after-latest-core-request-plan",
                source="test",
                market_date=date(2026, 9, 11),
                requested_at=datetime(2026, 9, 11, 12, 8, tzinfo=UTC),
                accepted_at=datetime(2026, 9, 11, 12, 12, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[delayed_after_latest_stop],
            )
            monitoring_service.materialize_monitoring_plan_items(
                delayed_after_latest_plan,
                db=db,
                effective_at=datetime(2026, 9, 11, 12, 12, tzinfo=UTC),
            )

            assert current_stop.trigger_price == Decimal("80")
            assert current_stop.decision_snapshot_id == "latest"
            assert delayed_after_latest_stop.status == "superseded"
    finally:
        engine.dispose()


def test_equal_core_request_time_with_divergent_sell_material_is_quarantined() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    try:
        with session_factory() as db:
            incumbent = ExecutionPlanItem(
                id="equal-time-incumbent",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="equal-time-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="equal-time-incumbent-plan",
                    source="test",
                    market_date=date(2026, 9, 11),
                    requested_at=datetime(2026, 9, 11, 12, tzinfo=UTC),
                    accepted_at=datetime(2026, 9, 11, 12, 1, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[incumbent],
                )
            )
            db.commit()

            conflict = ExecutionPlanItem(
                id="equal-time-conflict",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="pending",
                source_plan_item_id="equal-time-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("10"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            conflict_plan = ExecutionPlan(
                external_plan_id="equal-time-conflict-plan",
                source="test",
                market_date=date(2026, 9, 11),
                requested_at=datetime(2026, 9, 11, 12, tzinfo=UTC),
                accepted_at=datetime(2026, 9, 11, 12, 2, tzinfo=UTC),
                mode="paper",
                status="accepted",
                items=[conflict],
            )

            monitoring_service.materialize_monitoring_plan_items(
                conflict_plan,
                db=db,
                effective_at=datetime(2026, 9, 11, 12, 3, tzinfo=UTC),
            )

            assert incumbent.status == "stop_sell_monitoring"
            assert incumbent.trigger_price == Decimal("90")
            assert conflict.status == "manual_review_required"
            assert conflict.risk_context["last_monitoring_event"]["reason"] == (
                "equal_source_authority_with_divergent_material"
            )

            # A pre-fix/conflicting row already persisted as active must also
            # be resolved without allowing its identifier to choose authority.
            conflict.status = "stop_sell_monitoring"
            db.add(conflict_plan)
            db.commit()
            selected = monitoring_service._load_monitored_items(
                settings=settings,
                db=db,
                symbols={"005930"},
                current_market_date=date(2026, 9, 11),
            )

            assert incumbent in selected
            assert conflict not in selected
            assert conflict.status == "manual_review_required"
    finally:
        engine.dispose()


def test_future_stop_template_cannot_rearm_against_current_position() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    _, future_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={
            "kind": "position_exit_from_entry",
            "linked_entry_item_key": "future-entry",
        },
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="current-covered-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="current-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=risk_context,
            )
            future_stop = ExecutionPlanItem(
                id="future-entry-stop-template",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                source_plan_item_id="future-entry",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("10"),
                quantity_policy=future_policy,
                risk_context=risk_context,
            )
            position = ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="005930",
                quantity=2,
                average_price=Decimal("100"),
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="current-covered-plan",
                        source="test",
                        market_date=date(2026, 9, 11),
                        requested_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
                        accepted_at=datetime(2026, 9, 11, 1, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="future-entry-plan",
                        source="test",
                        market_date=date(2099, 1, 1),
                        requested_at=datetime(2026, 9, 11, 2, tzinfo=UTC),
                        accepted_at=datetime(2026, 9, 11, 2, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[future_stop],
                    ),
                    position,
                ]
            )
            db.commit()

            issues, repaired_count = monitoring_service._repair_unprotected_stop_coverage(
                settings=settings,
                db=db,
                positions_by_symbol={"005930": position},
                triggered_at=datetime(2026, 9, 11, 3, tzinfo=UTC),
            )

            assert repaired_count == 0
            assert all(issue.outcome != "unprotected_position" for issue in issues)
            assert current_stop.status == "stop_sell_monitoring"
            assert current_stop.trigger_price == Decimal("90")
            assert future_stop.status == "stop_sell_pending_config"
            assert future_stop.quantity_policy == future_policy
    finally:
        engine.dispose()


def test_kis_us_future_watch_uses_new_york_market_date() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "NASD"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    try:
        with session_factory() as db:
            watch = ExecutionPlanItem(
                id="future-us-watch",
                plan_item_index=0,
                symbol="AAPL",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                provider="kis",
                venue="kis",
                asset_class=None,
                trigger_price=Decimal("100"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            stop = ExecutionPlanItem(
                id="future-us-stop",
                plan_item_index=1,
                symbol="AAPL",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                source_plan_item_id="future-us-watch",
                provider="kis",
                venue="kis",
                asset_class=None,
                trigger_price=Decimal("90"),
                quantity_policy=quantity_policy,
                risk_context=risk_context,
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="future-us-watch-plan",
                    source="test",
                    market_date=date(2026, 9, 12),
                    requested_at=datetime(2026, 9, 11, 15, tzinfo=UTC),
                    accepted_at=datetime(2026, 9, 11, 15, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[watch, stop],
                )
            )
            db.commit()

            before_new_york_date = datetime(2026, 9, 11, 15, 30, tzinfo=UTC)
            assert before_new_york_date.astimezone(ZoneInfo("Asia/Seoul")).date() == date(
                2026, 9, 12
            )
            assert before_new_york_date.astimezone(
                ZoneInfo("America/New_York")
            ).date() == date(2026, 9, 11)

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                effective_at=before_new_york_date,
            ) == []
            assert monitoring_service._load_monitored_items(
                settings=settings,
                db=db,
                symbols={"AAPL"},
                effective_at=before_new_york_date,
            ) == []

            on_new_york_date = datetime(2026, 9, 12, 14, tzinfo=UTC)
            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                effective_at=on_new_york_date,
            ) == ["AAPL"]
            loaded = monitoring_service._load_monitored_items(
                settings=settings,
                db=db,
                symbols={"AAPL"},
                effective_at=on_new_york_date,
            )
            assert {item.id for item in loaded} == {watch.id, stop.id}
    finally:
        engine.dispose()


def test_monitoring_market_date_inference_is_conservative_and_venue_aware() -> None:
    evaluated_at = datetime(2026, 9, 11, 15, 30, tzinfo=UTC)

    krx_item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        risk_context={},
    )
    krx_item.plan = ExecutionPlan(market_date=date(2026, 9, 12))
    venue_only_us_item = ExecutionPlanItem(
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        venue="NASD",
        risk_context={},
    )
    venue_only_us_item.plan = ExecutionPlan(market_date=date(2026, 9, 12))
    unknown_item = ExecutionPlanItem(
        symbol="ABCDEF",
        side="buy",
        intent="intraday_watch_buy",
        venue="kis",
        risk_context={},
    )
    unknown_item.plan = ExecutionPlan(market_date=date(2026, 9, 12))

    assert monitoring_service._monitoring_item_is_effective_at(
        item=krx_item,
        effective_at=evaluated_at,
    ) is True
    assert monitoring_service._monitoring_item_is_effective_at(
        item=venue_only_us_item,
        effective_at=evaluated_at,
    ) is False
    assert monitoring_service._monitoring_item_is_effective_at(
        item=unknown_item,
        effective_at=evaluated_at,
    ) is False


def test_future_us_watch_is_not_quarantined_before_its_market_date() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    future_watch = ExecutionPlanItem(
        id="future-unbound-us-watch",
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    try:
        with session_factory() as db:
            db.add(
                ExecutionPlan(
                    external_plan_id="future-unbound-us-watch-plan",
                    source="test",
                    market_date=date(2026, 9, 12),
                    mode="paper",
                    status="accepted",
                    items=[future_watch],
                )
            )
            db.commit()

            expired = monitoring_service.expire_prior_market_date_unfilled_watch_buys(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 12),
                effective_at=datetime(2026, 9, 11, 15, 30, tzinfo=UTC),
            )

            assert expired == 0
            assert future_watch.status == "watch_monitoring_active"
            assert future_watch.risk_context == {"exchange": "NASD"}
    finally:
        engine.dispose()


def test_future_us_stop_is_not_quarantined_before_its_market_date() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="monitor-route-secret-for-tests",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
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
        quantity=1,
        average_price=Decimal("100"),
    )
    future_stop = ExecutionPlanItem(
        id="future-unbound-us-stop",
        plan_item_index=0,
        symbol="AAPL",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        provider="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
        quantity_policy={"kind": "shares", "quantity": 1},
        trigger_price=Decimal("90"),
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="future-unbound-us-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 12),
                        mode="paper",
                        status="accepted",
                        items=[future_stop],
                    ),
                    position,
                ]
            )
            db.commit()

            assert monitoring_service._has_active_sell_monitor(
                settings=settings,
                db=db,
                position=position,
                intent="stop_sell",
                evaluated_at=datetime(2026, 9, 11, 15, 30, tzinfo=UTC),
            ) is False
            assert monitoring_service._stop_items_for_symbol(
                settings=settings,
                db=db,
                symbol="AAPL",
                evaluated_at=datetime(2026, 9, 11, 15, 30, tzinfo=UTC),
            ) == []
            assert monitoring_service._position_truth_evidence_by_symbol(
                settings=settings,
                db=db,
                symbols={"AAPL"},
                evaluated_at=datetime(2026, 9, 11, 15, 30, tzinfo=UTC),
            ) == {}
            position.quantity = 0
            db.flush()
            assert monitoring_service.expire_known_flat_pending_sell_monitors(
                settings=settings,
                db=db,
                expired_at=datetime(2026, 9, 11, 15, 30, tzinfo=UTC),
            ) == 0
            assert future_stop.status == "stop_sell_monitoring"
            assert future_stop.risk_context == {"exchange": "NASD"}
    finally:
        engine.dispose()


def test_foreign_selected_stop_cannot_hide_matching_account_fallback() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="matching-fallback-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_stop = ExecutionPlanItem(
                id="foreign-selected-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("91"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="matching-fallback-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-selected-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[foreign_stop],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol="005930",
                        quantity=2,
                        average_price=Decimal("100"),
                    ),
                ]
            )
            db.commit()

            items = monitoring_service._append_held_stop_sell_fallbacks(
                settings=settings,
                db=db,
                monitored_item_filter=monitoring_service._monitored_item_filter(),
                selected_items=[foreign_stop],
            )

            assert [item.id for item in items] == ["matching-fallback-stop"]
            assert foreign_stop.status == "stop_sell_monitoring"
            assert current_stop.status == "stop_sell_monitoring"
    finally:
        engine.dispose()


def test_foreign_selected_stop_cannot_supersede_matching_active_stop_directly() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    try:
        with session_factory() as db:
            current_stop = ExecutionPlanItem(
                id="matching-active-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_stop = ExecutionPlanItem(
                id="foreign-active-replacement-stop",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("91"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="matching-active-stop-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-active-replacement-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[foreign_stop],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol="005930",
                        quantity=2,
                        average_price=Decimal("100"),
                    ),
                ]
            )
            db.commit()

            items = monitoring_service._append_held_active_sell_monitor_reconciliations(
                settings=settings,
                db=db,
                selected_items=[foreign_stop],
            )

            assert [item.id for item in items] == ["matching-active-stop"]
            assert current_stop.status == "stop_sell_monitoring"
            assert foreign_stop.status == "stop_sell_monitoring"
    finally:
        engine.dispose()


def test_foreign_target_cannot_count_toward_matching_account_coverage() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="monitor-route-secret",
        account_drawdown_recovery_scope_secret="monitor-route-secret",
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "99999999"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            current_target = ExecutionPlanItem(
                id="matching-partial-target",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("110"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_target = ExecutionPlanItem(
                id="foreign-target",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("111"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            current_stop = ExecutionPlanItem(
                id="matching-stop-selection",
                plan_item_index=1,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("90"),
                quantity_policy=current_policy,
                risk_context=current_risk,
            )
            foreign_stop = ExecutionPlanItem(
                id="foreign-stop-selection",
                plan_item_index=1,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                trigger_price=Decimal("89"),
                quantity_policy=foreign_policy,
                risk_context=foreign_risk,
            )
            position = ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="005930",
                quantity=2,
                average_price=Decimal("100"),
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="matching-partial-target-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[current_target, current_stop],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-target-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="paper",
                        status="accepted",
                        items=[foreign_target, foreign_stop],
                    ),
                    position,
                ]
            )
            db.commit()

            issues = monitoring_service._profit_target_coverage_issues(
                settings=settings,
                db=db,
                positions_by_symbol={"005930": position},
            )

            assert len(issues) == 1
            assert issues[0].active_target_quantity == 1
            assert issues[0].missing_quantity == 1
            assert current_target.status == "target_sell_monitoring"
            assert foreign_target.status == "target_sell_monitoring"
            stop_items = monitoring_service._stop_items_for_symbol(
                settings=settings,
                db=db,
                symbol="005930",
            )
            assert [item.id for item in stop_items] == ["matching-stop-selection"]
            assert foreign_stop.status == "stop_sell_monitoring"
    finally:
        engine.dispose()


def test_monitoring_position_reader_excludes_legacy_and_other_routes() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    other_provider = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="KRX",
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=3,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other_provider.provider,
                        account_scope=other_provider.account_scope,
                        exchange=other_provider.exchange,
                        symbol="AAPL",
                        quantity=7,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="000660",
                        quantity=5,
                    ),
                ]
            )
            db.commit()

            positions = monitoring_service._load_positions_by_symbol(
                settings=settings,
                db=db,
            )

        assert set(positions) == {"005930"}
        assert positions["005930"].quantity == 3
    finally:
        engine.dispose()


def test_monitoring_position_reader_fails_closed_on_same_symbol_route_ambiguity() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    krx = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    nasd = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=krx.provider,
                        account_scope=krx.account_scope,
                        exchange=krx.exchange,
                        symbol="DUAL",
                        quantity=1,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=nasd.provider,
                        account_scope=nasd.account_scope,
                        exchange=nasd.exchange,
                        symbol="DUAL",
                        quantity=2,
                    ),
                ]
            )
            db.commit()

            with pytest.raises(
                monitoring_service.PositionIdentityConflictError,
                match="ambiguous",
            ):
                monitoring_service._load_positions_by_symbol(
                    settings=settings,
                    db=db,
                )
    finally:
        engine.dispose()


def test_monitoring_order_evidence_ignores_foreign_and_legacy_routes() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            db.add_all(
                [
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="buy",
                        status="open",
                        account_scope="f" * 24,
                        created_at=datetime(2026, 9, 8, 2, 0, tzinfo=UTC),
                    ),
                    BrokerOrder(
                        broker_name="kis",
                        mode="paper",
                        idempotency_key="legacy-unscoped-monitoring-evidence",
                        symbol="005930",
                        side="buy",
                        status="filled",
                        quantity=1,
                    ),
                ]
            )
            db.commit()

            assert monitoring_service._position_truth_evidence_by_symbol(
                settings=settings,
                db=db,
                symbols={"005930"},
            ) == {}
            db.add(
                _monitoring_order(
                    settings,
                    symbol="005930",
                    side="sell",
                    status="open",
                    created_at=datetime(2026, 9, 8, 1, 0, tzinfo=UTC),
                )
            )
            db.commit()
            assert monitoring_service._has_unexited_buy_order_for_symbol(
                settings=settings,
                db=db,
                symbol="005930",
            ) is False
            assert monitoring_service._symbols_with_unexited_buy_orders(
                settings=settings,
                db=db,
                symbols=["005930"],
            ) == set()
    finally:
        engine.dispose()


def test_position_truth_evidence_ignores_foreign_sell_monitors() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            foreign_sell = ExecutionPlanItem(
                id="foreign-position-truth-sell",
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                risk_context=foreign_risk,
                quantity_policy=foreign_policy,
            )
            current_sell = ExecutionPlanItem(
                id="current-position-truth-sell",
                plan_item_index=0,
                symbol="000660",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                provider="kis",
                asset_class="stock",
                risk_context=current_risk,
                quantity_policy=current_policy,
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="foreign-position-truth-plan",
                        source="test",
                        mode="paper",
                        items=[foreign_sell],
                    ),
                    ExecutionPlan(
                        external_plan_id="current-position-truth-plan",
                        source="test",
                        mode="paper",
                        items=[current_sell],
                    ),
                ]
            )
            db.commit()

            evidence = monitoring_service._position_truth_evidence_by_symbol(
                settings=settings,
                db=db,
                symbols={"005930", "000660"},
            )

            assert evidence == {"000660": ["sell_monitor:stop_sell_monitoring"]}
            assert foreign_sell.status == "stop_sell_monitoring"
            assert "last_monitoring_event" not in foreign_sell.risk_context
    finally:
        engine.dispose()


def test_latest_filled_entry_order_requires_position_account_and_exchange() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=1,
    )
    try:
        with session_factory() as db:
            active_plan = ExecutionPlan(
                external_plan_id="active-entry-route",
                source="test",
                mode="paper",
                status="accepted",
            )
            active_item = ExecutionPlanItem(
                id="active-entry-item",
                plan=active_plan,
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="entry_buy",
                status="filled",
            )
            foreign_plan = ExecutionPlan(
                external_plan_id="foreign-entry-route",
                source="test",
                mode="paper",
                status="accepted",
            )
            foreign_item = ExecutionPlanItem(
                id="foreign-entry-item",
                plan=foreign_plan,
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="entry_buy",
                status="filled",
            )
            other_exchange_plan = ExecutionPlan(
                external_plan_id="other-exchange-entry-route",
                source="test",
                mode="paper",
                status="accepted",
            )
            other_exchange_item = ExecutionPlanItem(
                id="other-exchange-entry-item",
                plan=other_exchange_plan,
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="entry_buy",
                status="filled",
            )
            active_order = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="filled",
                plan_item=active_item,
                created_at=datetime(2026, 9, 8, 1, 0, tzinfo=UTC),
            )
            foreign_order = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="filled",
                account_scope="f" * 24,
                plan_item=foreign_item,
                created_at=datetime(2026, 9, 8, 2, 0, tzinfo=UTC),
            )
            other_exchange_order = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="filled",
                exchange="NASD",
                plan_item=other_exchange_item,
                created_at=datetime(2026, 9, 8, 3, 0, tzinfo=UTC),
            )
            db.add_all(
                [
                    active_plan,
                    active_item,
                    foreign_plan,
                    foreign_item,
                    other_exchange_plan,
                    other_exchange_item,
                    active_order,
                    foreign_order,
                    other_exchange_order,
                ]
            )
            db.commit()

            selected = monitoring_service._latest_filled_buy_order(
                settings=settings,
                db=db,
                symbol="005930",
                position=position,
            )

            assert selected is not None
            assert selected.id == active_order.id
    finally:
        engine.dispose()


@pytest.mark.parametrize("entry_route", ["NXT", "SOR", "KOSPI", "KOSDAQ"])
def test_latest_filled_entry_order_searches_all_canonical_domestic_routes(
    entry_route: str,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    position = ExecutionPosition(
        mode="paper",
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol="005930",
        quantity=1,
    )
    try:
        with session_factory() as db:
            plan = ExecutionPlan(
                external_plan_id=f"{entry_route.lower()}-entry-route",
                source="test",
                mode="paper",
                status="accepted",
            )
            item = ExecutionPlanItem(
                id=f"{entry_route.lower()}-entry-item",
                plan=plan,
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="entry_buy",
                status="filled",
            )
            order = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="filled",
                exchange=entry_route,
                plan_item=item,
                created_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
            )
            db.add_all([plan, item, order])
            db.commit()

            selected = monitoring_service._latest_filled_buy_order(
                settings=settings,
                db=db,
                symbol="005930",
                position=position,
            )

            assert selected is not None
            assert selected.id == order.id
    finally:
        engine.dispose()


def test_existing_us_position_route_ignores_foreign_plan_provenance() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "NYSE"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            position = ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="ROUTE",
                quantity=1,
                average_price=Decimal("100"),
            )
            db.add_all(
                [
                    position,
                    ExecutionPlan(
                        external_plan_id="foreign-us-route-plan",
                        source="test",
                        mode="paper",
                        accepted_at=datetime(2026, 9, 8, 8, tzinfo=UTC),
                        items=[
                            ExecutionPlanItem(
                                id="foreign-us-route-item",
                                plan_item_index=0,
                                symbol="ROUTE",
                                side="sell",
                                intent="stop_sell",
                                status="stop_sell_monitoring",
                                provider="kis",
                                venue="kis",
                                asset_class="us_equity",
                                risk_context=foreign_risk,
                                quantity_policy=foreign_policy,
                            )
                        ],
                    ),
                ]
            )
            db.commit()

            assert monitoring_service._existing_position_monitoring_route(
                settings=settings,
                db=db,
                position=position,
            ) == {
                "exchange": "NASD",
                "venue": "kis",
                "asset_class": "us_equity",
                "manual_review_reason": None,
            }
    finally:
        engine.dispose()


def test_pending_sell_fallback_requires_exact_order_account_scope() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            plan = ExecutionPlan(
                external_plan_id="scoped-sell-fallbacks",
                source="test",
                mode="paper",
                status="accepted",
            )
            foreign_item = ExecutionPlanItem(
                id="foreign-order-fallback-item",
                plan=plan,
                plan_item_index=0,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={"kind": "position_exit_from_entry"},
                provider="kis",
                risk_context={"exchange": "KRX"},
            )
            active_item = ExecutionPlanItem(
                id="active-order-fallback-item",
                plan=plan,
                plan_item_index=1,
                symbol="000660",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={"kind": "position_exit_from_entry"},
                provider="kis",
                risk_context={"exchange": "KRX"},
            )
            db.add_all(
                [
                    plan,
                    foreign_item,
                    active_item,
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="buy",
                        status="filled",
                        account_scope="f" * 24,
                    ),
                    _monitoring_order(
                        settings,
                        symbol="000660",
                        side="buy",
                        status="filled",
                    ),
                ]
            )
            db.commit()

            items = _load_monitored_items(
                settings=settings,
                db=db,
                exclude_pending_config=True,
            )

            assert [item.id for item in items] == ["active-order-fallback-item"]
    finally:
        engine.dispose()


def test_local_buy_reservation_excludes_foreign_and_surfaces_legacy_scope() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            active = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="open",
            )
            active.limit_price = Decimal("100")
            foreign = _monitoring_order(
                settings,
                symbol="005930",
                side="buy",
                status="open",
                account_scope="f" * 24,
            )
            foreign.limit_price = Decimal("900")
            legacy = BrokerOrder(
                broker_name="kis",
                mode="paper",
                idempotency_key="legacy-buy-reservation",
                symbol="005930",
                side="buy",
                status="open",
                quantity=1,
                limit_price=Decimal("700"),
            )
            db.add_all([active, foreign, legacy])
            db.commit()

            total, complete = monitoring_service._local_open_buy_order_notional(
                settings=settings,
                db=db,
                snapshot_open_orders=[],
            )

            assert total == Decimal("100")
            assert complete is False
    finally:
        engine.dispose()


def test_live_watch_buy_reservation_blocks_incomplete_open_order_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("100000"),
        buying_power=Decimal("100000"),
        open_orders=[],
        open_orders_complete=False,
        payload={"snapshot_diagnostics": ["malformed open-order row"]},
    )
    monkeypatch.setattr(
        monitoring_service,
        "_load_watch_buy_account_snapshot",
        lambda **_kwargs: snapshot,
    )
    try:
        with session_factory() as db:
            result = monitoring_service._load_watch_buy_cash_reservation(
                settings=settings,
                db=db,
                adapter=object(),
                triggered_at=datetime(2026, 9, 11, 12, tzinfo=UTC),
            )

            assert isinstance(result, str)
            assert "open-order truth is incomplete" in result
    finally:
        engine.dispose()


def test_exit_reservation_ignores_foreign_route_and_blocks_legacy_route() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    positions = {
        symbol: ExecutionPosition(
            mode="paper",
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol=symbol,
            quantity=10,
        )
        for symbol in ("005930", "000660")
    }
    try:
        with session_factory() as db:
            db.add_all(
                [
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="sell",
                        status="open",
                        account_scope="f" * 24,
                    ),
                    BrokerOrder(
                        broker_name="kis",
                        mode="paper",
                        idempotency_key="legacy-active-exit-reservation",
                        symbol="000660",
                        side="sell",
                        status="open",
                        quantity=10,
                    ),
                ]
            )
            db.commit()
            remaining = {"005930": 10, "000660": 10}

            monitoring_service._reserve_existing_exit_orders(
                settings=settings,
                db=db,
                remaining_quantities_by_symbol=remaining,
                positions_by_symbol=positions,
                rearmed_plan_item_ids=set(),
            )

            assert remaining == {"005930": 10}
    finally:
        engine.dispose()


def test_sell_monitor_does_not_consume_same_symbol_position_on_other_exchange() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    nasd = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    position = ExecutionPosition(
        mode="paper",
        provider=nasd.provider,
        account_scope=nasd.account_scope,
        exchange=nasd.exchange,
        symbol="AAPL",
        quantity=3,
    )
    item = ExecutionPlanItem(
        execution_plan_id="plan",
        plan_item_index=0,
        symbol="AAPL",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        quantity_policy={"kind": "all"},
        trigger_price=Decimal("180"),
        provider="kis",
        venue="kis",
        asset_class="us_equity",
        risk_context={"exchange": "NYSE"},
    )

    assert monitoring_service._filter_sell_items_to_held_symbols(
        settings=settings,
        items=[item],
        positions_by_symbol={"AAPL": position},
    ) == []


def test_runtime_position_truth_excludes_legacy_and_other_accounts() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = PositionIdentity(
        provider="kis",
        account_scope="b" * 24,
        exchange="KRX",
    )
    observed_at = datetime(2026, 9, 9, 12, tzinfo=UTC)
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=1,
                        last_reconciled_at=observed_at,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="AAPL",
                        quantity=7,
                        last_reconciled_at=observed_at,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="LEGACY",
                        quantity=9,
                        last_reconciled_at=observed_at,
                    ),
                ]
            )
            db.commit()

            payload = runtime_service.position_truth_freshness_payload(
                settings=settings,
                db=db,
                now=observed_at,
            )

        assert payload["nonzero_position_count"] == 1
        assert payload["tracked_symbol_count"] == 1
        assert payload["stale_symbols"] == []
    finally:
        engine.dispose()


def test_runtime_pnl_positions_are_keyed_by_exact_route_and_symbol() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    kis = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    alpaca = build_position_identity(settings=settings, provider="alpaca", exchange="ALPACA")
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=kis.provider,
                        account_scope=kis.account_scope,
                        exchange=kis.exchange,
                        symbol="AAPL",
                        quantity=0,
                        average_price=Decimal("150"),
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=alpaca.provider,
                        account_scope=alpaca.account_scope,
                        exchange=alpaca.exchange,
                        symbol="AAPL",
                        quantity=0,
                        average_price=Decimal("250"),
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="AAPL",
                        quantity=0,
                        average_price=Decimal("999"),
                    ),
                ]
            )
            db.commit()

            positions = runtime_service._pnl_positions_by_route_symbol(
                settings=settings,
                db=db,
                symbols={"AAPL"},
            )

        kis_scope = FifoRouteScope(
            provider="kis",
            account_scope=kis.account_scope,
            venue="KIS",
            asset_class="us_equity",
            exchange="NASD",
        )
        alpaca_scope = FifoRouteScope(
            provider="alpaca",
            account_scope=alpaca.account_scope,
            venue="ALPACA",
            asset_class="us_equity",
            exchange="ALPACA",
        )
        assert runtime_service._pnl_position_for_route(
            positions_by_route_symbol=positions,
            route_scope=kis_scope,
            symbol="AAPL",
        ).average_price == Decimal("150")
        assert runtime_service._pnl_position_for_route(
            positions_by_route_symbol=positions,
            route_scope=alpaca_scope,
            symbol="AAPL",
        ).average_price == Decimal("250")
    finally:
        engine.dispose()


def test_monitoring_symbol_discovery_uses_only_active_kis_account_positions() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=1,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="000660",
                        quantity=1,
                    ),
                    ExecutionPosition(mode="paper", symbol="035420", quantity=1),
                ]
            )
            db.commit()

            symbols = list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            )

        assert symbols == ["005930"]
    finally:
        engine.dispose()


def test_recent_flat_truth_from_other_route_does_not_suppress_active_kis_sell() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    try:
        with session_factory() as db:
            db.add(
                ExecutionPosition(
                    mode="paper",
                    provider=other.provider,
                    account_scope=other.account_scope,
                    exchange=other.exchange,
                    symbol="005930",
                    quantity=0,
                    last_reconciled_at=datetime.now(UTC),
                )
            )
            db.commit()

            assert monitoring_service._symbols_with_recent_flat_position_truth(
                settings=settings,
                db=db,
                symbols=["005930"],
            ) == set()
    finally:
        engine.dispose()


def test_quote_route_fallback_uses_only_active_scoped_kis_position() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="NASD")
    sync_payload = {
        "_position_sync": {
            "provider": "kis",
            "asset_class": "us_equity",
            "exchange": "NASD",
            "monitoring_eligible": True,
        }
    }
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="AAPL",
                        quantity=2,
                        raw_payload=sync_payload,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="AAPL",
                        quantity=99,
                        raw_payload=sync_payload,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="AAPL",
                        quantity=88,
                        raw_payload=sync_payload,
                    ),
                ]
            )
            db.commit()

            contexts = monitoring_price_source._load_monitoring_execution_context_by_symbol(
                settings=settings,
                db=db,
                symbols=["AAPL"],
            )

        assert contexts["AAPL"]["provider"] == "kis"
        assert contexts["AAPL"]["exchange"] == "NASD"
    finally:
        engine.dispose()


def test_autonomous_sqlite_position_fast_path_is_active_route_scoped(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'route-scoped-positions.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: None,  # type: ignore[arg-type,return-value]
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=1,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="000660",
                        quantity=1,
                    ),
                    ExecutionPosition(mode="paper", symbol="035420", quantity=1),
                ]
            )
            db.commit()

        assert coordinator._load_local_position_symbols_from_sqlite() == ["005930"]
        assert coordinator._load_local_position_symbols() == ["005930"]
    finally:
        engine.dispose()


def test_autonomous_sqlite_position_fast_path_omits_ambiguous_symbol(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'ambiguous-route-positions.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    krx = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    nasd = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: None,  # type: ignore[arg-type,return-value]
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=krx.provider,
                        account_scope=krx.account_scope,
                        exchange=krx.exchange,
                        symbol="DUAL",
                        quantity=1,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=nasd.provider,
                        account_scope=nasd.account_scope,
                        exchange=nasd.exchange,
                        symbol="DUAL",
                        quantity=2,
                    ),
                ]
            )
            db.commit()

        assert coordinator._load_local_position_symbols_from_sqlite() == []
    finally:
        engine.dispose()


def test_exec_agent_local_holdings_reader_is_exact_route_scoped() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=3,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="000660",
                        quantity=9,
                    ),
                    ExecutionPosition(mode="paper", symbol="035420", quantity=8),
                ]
            )
            db.commit()

            positions = exec_agent_service._load_local_positions(
                settings=settings,
                db=db,
                provider="kis",
                exchange="KRX",
                broker_account_id=None,
            )

        assert [(position.symbol, position.quantity) for position in positions] == [
            ("005930", 3)
        ]
    finally:
        engine.dispose()


def test_legacy_short_term_agent_local_state_is_exact_route_scoped() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="current-account",
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-account"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    current_source_item = ExecutionPlanItem(
        id="current-active-source",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="next_open_buy_scheduled",
        provider="kis",
        decision_agent_name="short-term-decider",
        decision_run_id="current-run",
        source_plan_item_id="current-source",
        source_market_date=date(2026, 9, 8),
        risk_context=current_risk,
        quantity_policy=current_policy,
    )
    foreign_source_item = ExecutionPlanItem(
        id="foreign-active-source",
        plan_item_index=0,
        symbol="000660",
        side="buy",
        intent="next_open_buy",
        status="next_open_buy_scheduled",
        provider="kis",
        decision_agent_name="short-term-decider",
        decision_run_id="foreign-run",
        source_plan_item_id="foreign-source",
        source_market_date=date(2026, 9, 8),
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=3,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="000660",
                        quantity=9,
                    ),
                    ExecutionPosition(mode="paper", symbol="035420", quantity=8),
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="buy",
                        status="open",
                    ),
                    _monitoring_order(
                        settings,
                        symbol="000660",
                        side="buy",
                        status="open",
                        account_scope="f" * 24,
                    ),
                    ExecutionPlan(
                        id="current-active-source-plan",
                        external_plan_id="current-active-source-plan",
                        source="test",
                        mode="paper",
                        items=[current_source_item],
                    ),
                    ExecutionPlan(
                        id="foreign-active-source-plan",
                        external_plan_id="foreign-active-source-plan",
                        source="test",
                        mode="paper",
                        items=[foreign_source_item],
                    ),
                ]
            )
            db.commit()

            state = short_term_exec_agent_service._load_local_account_state(
                settings=settings,
                db=db,
            )

        assert state.positions_by_symbol == {"005930": 3}
        assert state.open_buy_order_symbols == {"005930"}
        assert state.active_source_keys == {
            short_term_exec_agent_service._source_candidate_key(
                current_source_item
            )
        }
    finally:
        engine.dispose()


def test_short_term_watcher_position_lookup_is_bound_to_entry_route() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    item = ExecutionPlanItem(
        id="entry",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        provider="kis",
        venue="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        id="plan",
                        external_plan_id="plan",
                        source="test",
                        mode="paper",
                        status="accepted",
                        items=[item],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=2,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="005930",
                        quantity=99,
                    ),
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="buy",
                        status="open",
                    ),
                    _monitoring_order(
                        settings,
                        symbol="005930",
                        side="buy",
                        status="open",
                        account_scope="f" * 24,
                    ),
                ]
            )
            db.commit()

            position = short_term_watcher_service._load_route_scoped_position(
                settings=settings,
                db=db,
                item=item,
                broker_account_id=None,
            )
            open_orders = (
                short_term_watcher_service._load_route_scoped_open_buy_orders(
                    settings=settings,
                    db=db,
                    item=item,
                    broker_account_id=None,
                )
            )

        assert position is not None
        assert position.quantity == 2
        assert len(open_orders) == 1
        assert open_orders[0].request_metadata["execution_scope"][
            "account_scope"
        ] == active.account_scope
    finally:
        engine.dispose()


def test_trigger_time_account_context_exposes_only_exact_route_positions() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    snapshot = BrokerAccountSnapshot(
        observed_at=datetime.now(UTC),
        source="test",
        available_cash=Decimal("1000"),
        buying_power=Decimal("1000"),
        holdings=[],
        positions=[],
        open_orders=[],
        payload={},
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=2,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="005930",
                        quantity=99,
                    ),
                    ExecutionPosition(mode="paper", symbol="000660", quantity=88),
                ]
            )
            db.commit()

            payload = trigger_time_exec_agent_service._account_context_payload(
                settings=settings,
                db=db,
                snapshot=snapshot,
                provider="kis",
                exchange="KRX",
                observed_at=snapshot.observed_at,
            )

        assert payload["local_positions"] == [
            {
                "symbol": "005930",
                "quantity": 2,
                "average_price": None,
                "last_reconciled_at": None,
            }
        ]
    finally:
        engine.dispose()


def test_max_holding_summary_does_not_match_other_or_legacy_position_route() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    other = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")
    due_item = ExecutionPlanItem(
        id="due-kis-exit",
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX", "close_exit_date": "2026-09-08"},
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="due-plan",
                        source="test",
                        mode="paper",
                        status="accepted",
                        items=[due_item],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="005930",
                        quantity=9,
                    ),
                ]
            )
            db.commit()

            summary = max_holding_actionability_service.max_holding_actionability_summary(
                settings=settings,
                db=db,
                mode="paper",
                reference_at=datetime(2026, 9, 9, tzinfo=UTC),
            )

        assert summary["due_item_count"] == 1
        assert summary["missing_position_count"] == 1
    finally:
        engine.dispose()


def test_portfolio_readers_exclude_legacy_and_preserve_route_conflict() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    kis = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    alpaca = build_position_identity(settings=settings, provider="alpaca", exchange="US")
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=kis.provider,
                        account_scope=kis.account_scope,
                        exchange=kis.exchange,
                        symbol="AAPL",
                        quantity=1,
                        raw_payload={"market_value": "100", "exchange": "NASD"},
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=alpaca.provider,
                        account_scope=alpaca.account_scope,
                        exchange=alpaca.exchange,
                        symbol="AAPL",
                        quantity=2,
                        raw_payload={"market_value": "200", "exchange": "US"},
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="LEGACY",
                        quantity=9,
                        raw_payload={"market_value": "900"},
                    ),
                ]
            )
            db.commit()

            positions = portfolio_risk_service._load_positions(
                settings=settings,
                db=db,
                position_scope=portfolio_risk_service.PortfolioPositionScope(
                    identity=kis,
                    exchanges=("NASD",),
                ),
            )
            hints = portfolio_risk_service.load_symbol_risk_metadata_hints(
                settings=settings,
                db=db,
                symbols=["AAPL", "LEGACY"],
            )

        assert [(row.provider, row.symbol) for row in positions] == [("kis", "AAPL")]
        assert hints["AAPL"].position_market_value is None
        assert hints["LEGACY"].position_market_value is None
    finally:
        engine.dispose()


def test_portfolio_snapshot_never_merges_another_account_same_symbol() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    adapter = SimpleNamespace(
        provider="kis",
        describe=lambda: SimpleNamespace(
            provider="kis",
            mode="paper",
            configured=False,
            reason="offline test",
        ),
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="005930",
                        quantity=2,
                        average_price=Decimal("70000"),
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider="kis",
                        account_scope="f" * 24,
                        exchange="KRX",
                        symbol="005930",
                        quantity=99,
                        average_price=Decimal("1"),
                    ),
                ]
            )
            db.commit()

            payload = portfolio_risk_service.build_portfolio_risk_snapshot(
                settings=settings,
                db=db,
                adapter=adapter,
                now=datetime(2026, 9, 9, tzinfo=UTC),
                market_scope="krx",
            )

        assert payload["holdings"]["open_count"] == 1
        assert payload["holdings"]["top_by_market_value"][0]["quantity"] == 2
        assert payload["account"]["position_scope_status"] == "ok"
    finally:
        engine.dispose()


def test_raw_position_drift_rejects_non_exact_route_input() -> None:
    settings = Settings(runtime_mode="paper")
    kis = build_position_identity(settings=settings, provider="kis", exchange="KRX")
    alpaca = build_position_identity(settings=settings, provider="alpaca", exchange="KRX")

    with pytest.raises(ValueError, match="one exact route"):
        broker_raw_holdings_service.raw_vs_normalized_position_drift(
            raw_holdings=[],
            positions=[
                ExecutionPosition(
                    mode="paper",
                    provider=kis.provider,
                    account_scope=kis.account_scope,
                    exchange=kis.exchange,
                    symbol="AAPL",
                    quantity=1,
                ),
                ExecutionPosition(
                    mode="paper",
                    provider=alpaca.provider,
                    account_scope=alpaca.account_scope,
                    exchange=alpaca.exchange,
                    symbol="AAPL",
                    quantity=2,
                ),
            ],
        )


def test_position_truth_remediation_selects_only_active_kis_route(monkeypatch) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="NASD")
    other = build_position_identity(settings=settings, provider="alpaca", exchange="NASD")
    sync_payload = {"_position_sync": {"provider": "kis", "exchange": "NASD"}}
    monkeypatch.setattr(
        position_truth_remediation_service,
        "position_truth_freshness_payload",
        lambda **_kwargs: {"stale_symbols": ["AAPL"]},
    )
    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        id="other-alpaca-position",
                        mode="paper",
                        provider=other.provider,
                        account_scope=other.account_scope,
                        exchange=other.exchange,
                        symbol="AAPL",
                        quantity=99,
                        raw_payload=sync_payload,
                    ),
                ]
            )
            db.commit()

            analysis = position_truth_remediation_service.analyze_position_truth_remediation(
                settings=settings,
                db=db,
                now=datetime.now(UTC),
            )

        assert analysis["items"][0]["position_id"] is None
        assert analysis["items"][0]["exchange"] is None
    finally:
        engine.dispose()


def test_upbit_paper_fill_updates_only_its_exact_position_route() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    other = build_position_identity(settings=settings, provider="kis", exchange="UPBIT")
    observed_at = datetime.now(UTC)
    try:
        with session_factory() as db:
            unrelated = ExecutionPosition(
                id="unrelated-kis-position",
                mode="paper",
                provider=other.provider,
                account_scope=other.account_scope,
                exchange=other.exchange,
                symbol="KRW-BTC",
                quantity=7,
                base_quantity=Decimal("7"),
            )
            db.add(unrelated)
            db.commit()

            broker_orders_service._apply_upbit_paper_position(
                settings=settings,
                db=db,
                symbol="KRW-BTC",
                side="buy",
                base_quantity=Decimal("0.1"),
                average_price=Decimal("100000000"),
                observed_at=observed_at,
                fill_payload={"safe": True},
            )
            db.flush()
            rows = db.scalars(
                select(ExecutionPosition).where(ExecutionPosition.symbol == "KRW-BTC")
            ).all()

        assert len(rows) == 2
        assert unrelated.base_quantity == Decimal("7")
        upbit = next(row for row in rows if row.provider == "upbit")
        assert upbit.base_quantity == Decimal("0.1")
        assert upbit.exchange == "UPBIT"
    finally:
        engine.dispose()


def test_compaction_prioritizes_runtime_state_after_large_upstream_context() -> None:
    upstream_context = {f"upstream_field_{index:03d}": index for index in range(80)}
    runtime_state = {
        "watch_buy_trigger_cross": {"observed_price": "71000"},
        "latest_live_market_context": {"quote_observed_at": "2026-09-08T13:00:00+00:00"},
        "monitoring_events": [{"event": "watch_buy_trigger_cross_seen"}],
        "last_monitoring_event": {"event": "trigger_time_exec_agent_delay"},
        "last_trigger_time_exec_agent_interaction": {
            "decision": "delay",
            "resolved_at": "2026-09-08T13:00:01+00:00",
        },
        "trigger_time_exec_agent_interactions": [{"decision": "delay"}],
        "last_stale_trigger_reclaim_check": {"outcome": "blocked"},
        "exec_agent_watch_sizing_attempt_count": 2,
        "exec_agent_watch_sizing_attempts": [{"attempt": 2, "outcome": "timeout"}],
        "exec_agent_watch_sizing_max_attempts": 3,
        "exec_agent_watch_sizing_fallback_allowed": False,
        "trigger_time_limit_price": "71100",
        "trigger_time_limit_price_revalidation": {"reason": "fresh_ask_limit_cap"},
        "updated_target_price": "73000",
        "updated_stop_price": "69800",
    }

    compacted = compact_json_object({**upstream_context, **runtime_state})

    assert runtime_state.keys() <= compacted.keys()
    assert compacted["watch_buy_trigger_cross"] == runtime_state["watch_buy_trigger_cross"]
    assert compacted["last_trigger_time_exec_agent_interaction"]["decision"] == "delay"
    assert len([key for key in compacted if not key.startswith("_compacted_")]) <= 60
    assert compacted["_compacted_skipped_keys"] > 0


def test_compaction_never_drops_submission_gates_or_account_route() -> None:
    evaluated_at = datetime.now(UTC)
    seoul = ZoneInfo("Asia/Seoul")
    expiry_date = evaluated_at.astimezone(seoul).date() + timedelta(days=1)
    expires_at = datetime(
        expiry_date.year,
        expiry_date.month,
        expiry_date.day,
        15,
        30,
        tzinfo=seoul,
    )
    codes = [
        "short_term_liquidity_floor_block_delegated_to_exec_watch",
        "short_term_reward_risk_ratio_block_delegated_to_exec_watch",
        "transaction_cost_evidence_pending_live_delegated_to_exec_watch",
    ]
    common = {
        "policy_version": "core_delegated_gate_policy_v1",
        "observed_at": (evaluated_at - timedelta(minutes=1)).isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    delegated_context = {
        "delegated_gate_codes": codes,
        "delegated_gate_confirmation_required": True,
        "delegated_gates": [
            {
                "code": codes[0],
                "observed_value": "2500000000",
                "field_path": "risk_context.validation.short_term_entry_quality.raw_trading_value_krw",
                "comparator": "gte",
                "threshold": "3000000000",
                "recheck_source": "exec_trigger_time_market_liquidity",
                **common,
            },
            {
                "code": codes[1],
                "observed_value": "1.50",
                "field_path": "risk_context.validation.short_term_entry_quality.reward_risk_ratio",
                "comparator": "gte",
                "threshold": "1.80",
                "recheck_source": "exec_trigger_time_price_geometry_and_cost_model",
                **common,
            },
            {
                "code": codes[2],
                "observed_value": None,
                "field_path": "risk_context.execution_feasibility_context.expected_slippage_bps",
                "comparator": "finite_non_negative_with_basis",
                "threshold": "0",
                "recheck_source": "exec_trigger_time_quote_and_cost_model",
                **common,
            },
        ],
        "execution_account_route": {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "opaque-route-binding",
        },
    }
    compacted = compact_json_object(
        {
            **{f"upstream_noise_{index:03d}": index for index in range(80)},
            **delegated_context,
            "last_monitoring_event": {"event": "trigger_time_exec_agent_pending"},
        }
    )

    assert delegated_context.keys() <= compacted.keys()
    assert delegated_buy_gate_rejections(
        compacted,
        evaluated_at=evaluated_at,
        require_live_recheck=True,
    ) == [f"delegated_gate_failed:{code}" for code in codes]


@pytest.mark.parametrize(
    "authority_key",
    ("monitoring_order_authority", "short_term_watcher_order_authority"),
)
def test_compaction_never_drops_server_owned_order_authority(
    authority_key: str,
) -> None:
    authority = {
        "policy_version": 1,
        "source": "trusted-server-worker",
        "command": {
            "execution_plan_item_id": "protected-item",
            "side": "sell",
            "symbol": "005930",
            "quantity": None,
        },
    }

    compacted = compact_json_object(
        {
            **{f"caller_noise_{index:03d}": index for index in range(80)},
            authority_key: authority,
        }
    )

    assert compacted[authority_key] == authority


def test_monitoring_action_order_is_global_stop_then_other_sell_then_buy() -> None:
    def action(*, item_id: str, symbol: str, side: str, intent: str):
        return monitoring_service.PendingMonitoringAction(
            item=SimpleNamespace(
                id=item_id,
                symbol=symbol,
                side=side,
                intent=intent,
                status="watch_monitoring_active",
                plan_item_index=0,
            ),
            observed_price=Decimal("100"),
        )

    actions = {
        "TARGET": deque(
            [action(item_id="target", symbol="TARGET", side="sell", intent="target_sell")]
        ),
        "BUY": deque(
            [action(item_id="buy", symbol="BUY", side="buy", intent="intraday_watch_buy")]
        ),
        "STOP": deque(
            [action(item_id="stop", symbol="STOP", side="sell", intent="stop_sell")]
        ),
    }

    monitoring_service._prioritize_sell_actions_by_symbol(actions)
    ordered = monitoring_service._prioritized_monitoring_symbol_order(
        symbol_order=["TARGET", "BUY", "STOP"],
        actions_by_symbol=actions,
    )

    assert ordered == ["STOP", "TARGET", "BUY"]


def test_trigger_agent_timeout_is_clamped_to_monitoring_cycle_budget(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_service.time, "monotonic", lambda: 103.2)

    bounded = monitoring_service._cycle_bounded_trigger_agent_settings(
        settings=Settings(
            monitoring_cycle_timeout_seconds=20,
            exec_agent_trigger_time_timeout_seconds=900,
        ),
        cycle_started=100.0,
    )

    assert bounded is not None
    assert bounded.exec_agent_trigger_time_timeout_seconds == 14


def test_trigger_agent_claim_identity_binds_nontransient_risk_context() -> None:
    item = SimpleNamespace(
        execution_plan_id="plan-1",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        limit_price=Decimal("71000"),
        trigger_price=Decimal("70500"),
        time_in_force="day",
        base_quantity=None,
        quote_notional=None,
        quantity_precision=0,
        price_precision=0,
        quantity_policy={"kind": "shares", "value": 1},
        risk_context={"target_price": "73000", "stop_price": "69500"},
    )

    original = monitoring_service._trigger_time_agent_claim_identity(
        item,
        risk_context=item.risk_context,
    )
    item.risk_context = {"target_price": "73000", "stop_price": "70000"}

    assert monitoring_service._trigger_time_agent_claim_identity(
        item,
        risk_context=item.risk_context,
    ) != original


def test_trigger_agent_claim_identity_ignores_only_runtime_event_churn() -> None:
    item = SimpleNamespace(
        execution_plan_id="plan-1",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        limit_price=Decimal("71000"),
        trigger_price=Decimal("70500"),
        time_in_force="day",
        base_quantity=None,
        quote_notional=None,
        quantity_precision=0,
        price_precision=0,
        quantity_policy={"kind": "shares", "value": 1},
        risk_context={"target_price": "73000", "stop_price": "69500"},
    )
    original = monitoring_service._trigger_time_agent_claim_identity(
        item,
        risk_context=item.risk_context,
    )
    item.risk_context = {
        **item.risk_context,
        "monitoring_events": [{"event": "heartbeat"}],
        "last_monitoring_event": {"event": "heartbeat"},
    }

    assert monitoring_service._trigger_time_agent_claim_identity(
        item,
        risk_context=item.risk_context,
    ) == original


def test_compaction_accepts_caller_priority_keys_without_growing_the_bound() -> None:
    payload = {f"field_{index:03d}": index for index in range(10)}
    payload["domain_checkpoint"] = {"status": "retrying"}

    compacted = compact_json_object(
        payload,
        max_dict_items=3,
        priority_keys=("domain_checkpoint",),
    )

    assert compacted["domain_checkpoint"] == {"status": "retrying"}
    assert len([key for key in compacted if not key.startswith("_compacted_")]) == 3


def test_compaction_honors_caller_priority_order_when_the_bound_is_tighter() -> None:
    compacted = compact_json_object(
        {
            "second_checkpoint": {"status": "waiting"},
            "first_checkpoint": {"status": "retrying"},
        },
        max_dict_items=1,
        priority_keys=("first_checkpoint", "second_checkpoint"),
    )

    assert compacted["first_checkpoint"] == {"status": "retrying"}
    assert "second_checkpoint" not in compacted


def test_compaction_and_hash_never_emit_nonstandard_nonfinite_json_numbers() -> None:
    payload = {
        "float_nan": float("nan"),
        "float_infinity": float("inf"),
        "decimal_nan": Decimal("NaN"),
        "decimal_infinity": Decimal("Infinity"),
        "nested": [Decimal("-Infinity")],
    }

    compacted = compact_json_object(payload)

    assert compacted == {
        "float_nan": "<non-finite-number>",
        "float_infinity": "<non-finite-number>",
        "decimal_nan": "<non-finite-number>",
        "decimal_infinity": "<non-finite-number>",
        "nested": ["<non-finite-number>"],
    }
    json.dumps(compacted, allow_nan=False)
    assert json_payload_hash(payload) == json_payload_hash(compacted)


def test_watch_candidates_are_ranked_before_round_robin_without_pre_limit(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_max_symbols_per_cycle=1,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    symbols = [f"{index:06d}" for index in range(1, 11)] + ["999999"]
    monkeypatch.setattr(
        "app.services.monitoring._load_core_desired_monitoring_symbols",
        lambda settings: ["999999"],
    )

    try:
        with session_factory() as db:
            db.add(
                ExecutionPlan(
                    external_plan_id="unbounded-before-round-robin",
                    source="test",
                    market_date=date(2026, 9, 8),
                    mode="live",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            plan_item_index=index,
                            symbol=symbol,
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                        )
                        for index, symbol in enumerate(symbols)
                    ],
                )
            )
            db.commit()

            candidates = list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            )

        assert candidates[0] == "999999"
        assert set(candidates) == set(symbols)
    finally:
        engine.dispose()


def test_watch_candidates_are_not_hidden_when_held_symbols_exceed_cycle_cap() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=1,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    active = build_position_identity(settings=settings, provider="kis", exchange="KRX")

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="000001",
                        quantity=1,
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=active.provider,
                        account_scope=active.account_scope,
                        exchange=active.exchange,
                        symbol="000002",
                        quantity=1,
                    ),
                    ExecutionPlan(
                        external_plan_id="watch-beyond-held-cap",
                        source="test",
                        market_date=date(2026, 9, 8),
                        mode="paper",
                        status="accepted",
                        items=[
                            ExecutionPlanItem(
                                plan_item_index=0,
                                symbol="999999",
                                side="buy",
                                intent="intraday_watch_buy",
                                status="watch_monitoring_active",
                            )
                        ],
                    ),
                ]
            )
            db.commit()

            candidates = list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            )

        assert candidates == ["000001", "000002", "999999"]
    finally:
        engine.dispose()


def test_watch_candidate_listing_supersedes_older_duplicate_active_item() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    older_item = ExecutionPlanItem(
        id="older-watch-item",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        source_plan_item_id="same-watch-family",
    )
    newer_item = ExecutionPlanItem(
        id="newer-watch-item",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        source_plan_item_id="same-watch-family",
    )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        id="older-watch-plan",
                        external_plan_id="older-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[older_item],
                    ),
                    ExecutionPlan(
                        id="newer-watch-plan",
                        external_plan_id="newer-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[newer_item],
                    ),
                ]
            )
            db.commit()

            candidates = list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            )

            assert candidates == ["005930"]
            assert newer_item.status == "watch_monitoring_active"
            assert older_item.status == "superseded"
            event = older_item.risk_context["last_monitoring_event"]
            assert event["event"] == "duplicate_watch_buy_superseded"
            assert event["reason"] == "newer_active_watch_buy_exists"
            assert event["prior_status"] == "watch_monitoring_active"
            assert event["superseded_by_execution_plan_item_id"] == "newer-watch-item"
            assert datetime.fromisoformat(event["recorded_at"]).tzinfo is not None
    finally:
        engine.dispose()


def test_kis_duplicate_housekeeping_ignores_non_kis_and_foreign_kis_rows() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    def item(
        *,
        item_id: str,
        provider: str,
        symbol: str,
        risk_context: dict[str, object] | None = None,
        quantity_policy: dict[str, object] | None = None,
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan_item_index=0,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            provider=provider,
            asset_class="stock" if provider == "kis" else "us_equity",
            source_plan_item_id=f"{provider}-same-family",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    rows = [
        item(item_id="alpaca-old", provider="alpaca", symbol="AAPL"),
        item(item_id="alpaca-new", provider="alpaca", symbol="AAPL"),
        item(
            item_id="foreign-kis-old",
            provider="kis",
            symbol="005930",
            risk_context=foreign_risk,
            quantity_policy=foreign_policy,
        ),
        item(
            item_id="foreign-kis-new",
            provider="kis",
            symbol="005930",
            risk_context=foreign_risk,
            quantity_policy=foreign_policy,
        ),
    ]
    try:
        with session_factory() as db:
            for index, row in enumerate(rows):
                db.add(
                    ExecutionPlan(
                        external_plan_id=f"plan-{row.id}",
                        source="test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7 + index, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[row],
                    )
                )
            db.commit()

            changed = monitoring_service.supersede_duplicate_active_watch_buys(
                settings=settings,
                db=db,
                superseded_at=datetime(2026, 9, 8, 12, tzinfo=UTC),
            )

            assert changed == 0
            assert {row.id: row.status for row in rows} == {
                "alpaca-old": "watch_monitoring_active",
                "alpaca-new": "watch_monitoring_active",
                "foreign-kis-old": "watch_monitoring_active",
                "foreign-kis-new": "watch_monitoring_active",
            }
            assert all(not row.risk_context for row in rows[:2])
            assert all(
                "last_monitoring_event" not in (row.risk_context or {})
                for row in rows[2:]
            )
    finally:
        engine.dispose()


def test_newer_legacy_mismatched_account_watch_cannot_supersede_valid_watch() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        kis_app_secret="route-fingerprint-secret-at-least-32-bytes",
        account_drawdown_recovery_scope_secret=(
            "route-fingerprint-secret-at-least-32-bytes"
        ),
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    valid_risk_context, _ = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"account_reference": "KIS:12345678-01"},
        quantity_policy={},
    )
    valid = ExecutionPlanItem(
        id="valid-account-watch",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        risk_context=valid_risk_context,
    )
    mismatched = ExecutionPlanItem(
        id="mismatched-account-watch",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        risk_context={"account_reference": "KIS:99999999-01"},
    )
    unbound_matching = ExecutionPlanItem(
        id="unbound-matching-account-watch",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        asset_class="stock",
        risk_context={"account_reference": "KIS:12345678-01"},
    )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="valid-account-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[valid],
                    ),
                    ExecutionPlan(
                        external_plan_id="mismatched-account-plan",
                        source="legacy-test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[mismatched],
                    ),
                    ExecutionPlan(
                        external_plan_id="unbound-matching-account-plan",
                        source="legacy-test",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 8, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[unbound_matching],
                    ),
                ]
            )
            db.commit()

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            ) == ["005930"]
            assert valid.status == "watch_monitoring_active"
            assert mismatched.status == "watch_buy_rejected"
            assert unbound_matching.status == "watch_buy_rejected"
            event = mismatched.risk_context["last_monitoring_event"]
            assert event["reason"] == "account_route_mismatch"
            assert "99999999" not in str(event)
            assert unbound_matching.risk_context["last_monitoring_event"]["reason"] == (
                "account_route_binding_missing"
            )
    finally:
        engine.dispose()


def test_watch_expiry_only_mutates_current_account_route() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-monitor-route-secret-for-tests",
        monitoring_watch_buy_max_age_trading_sessions=1,
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    current_risk, current_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    def watch_item(
        *,
        item_id: str,
        symbol: str,
        risk_context: dict[str, object] | None,
        quantity_policy: dict[str, object] | None,
        provider: str = "kis",
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan_item_index=0,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status="watch_monitoring_active",
            provider=provider,
            asset_class="stock" if provider == "kis" else "us_equity",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    current = watch_item(
        item_id="current-expirable-watch",
        symbol="005930",
        risk_context=current_risk,
        quantity_policy=current_policy,
    )
    foreign_same_symbol = watch_item(
        item_id="foreign-same-symbol-expirable-watch",
        symbol="005930",
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )
    foreign_other_symbol = watch_item(
        item_id="foreign-other-symbol-expirable-watch",
        symbol="000660",
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )
    non_kis = watch_item(
        item_id="alpaca-expirable-watch",
        symbol="AAPL",
        provider="alpaca",
        risk_context={},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    indeterminate_kis = watch_item(
        item_id="indeterminate-kis-expirable-watch",
        symbol="035420",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="current-expirable-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[current],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-same-symbol-expirable-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 8, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[foreign_same_symbol],
                    ),
                    ExecutionPlan(
                        external_plan_id="foreign-other-symbol-expirable-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 9, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[foreign_other_symbol],
                    ),
                    ExecutionPlan(
                        external_plan_id="alpaca-expirable-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 10, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[non_kis],
                    ),
                    ExecutionPlan(
                        external_plan_id="indeterminate-kis-expirable-watch-plan",
                        source="legacy-test",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 11, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[indeterminate_kis],
                    ),
                    ExecutionPlan(
                        external_plan_id="current-expiry-session-marker",
                        source="test-session-marker",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                    ),
                ]
            )
            db.commit()

            expired = monitoring_service.expire_prior_market_date_unfilled_watch_buys(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
                expired_at=datetime(2026, 9, 8, 8, tzinfo=UTC),
                max_age_trading_sessions=1,
            )

            assert expired == 1
            assert current.status == "watch_expired"
            assert foreign_same_symbol.status == "watch_monitoring_active"
            assert foreign_other_symbol.status == "watch_monitoring_active"
            assert non_kis.status == "watch_monitoring_active"
            assert indeterminate_kis.status == "watch_buy_rejected"
            assert "last_monitoring_event" not in foreign_same_symbol.risk_context
            assert "last_monitoring_event" not in foreign_other_symbol.risk_context
            assert "last_monitoring_event" not in non_kis.risk_context
            assert (
                indeterminate_kis.risk_context["last_monitoring_event"]["reason"]
                == "account_route_binding_missing"
            )
    finally:
        engine.dispose()


def test_watch_candidate_ttl_spans_two_trading_sessions_then_expires() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_expire_prior_market_watch_buys_enabled=False,
        monitoring_watch_buy_max_age_trading_sessions=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as db:
            item = ExecutionPlanItem(
                id="friday-watch-item",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
            )
            db.add(
                ExecutionPlan(
                    id="friday-watch-plan",
                    external_plan_id="friday-watch-plan",
                    source="test",
                    market_date=date(2026, 9, 4),
                    accepted_at=datetime(2026, 9, 4, 7, tzinfo=UTC),
                    mode="live",
                    status="accepted",
                    items=[item],
                )
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="observed-monday-session",
                        source="test-session-marker",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                    ),
                    ExecutionPlan(
                        external_plan_id="observed-tuesday-session",
                        source="test-session-marker",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                    ),
                ]
            )
            db.commit()

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 7),
            ) == ["005930"]
            assert item.status == "watch_monitoring_active"

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            ) == []
            assert item.status == "watch_expired"
            event = item.risk_context["last_monitoring_event"]
            assert event["event"] == "watch_buy_trading_session_ttl_elapsed"
            assert event["age_trading_sessions"] == 2
            assert event["max_age_trading_sessions"] == 2
    finally:
        engine.dispose()


def test_future_market_date_watch_is_not_in_current_monitoring_universe() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="live-key",
        kis_app_secret="live-secret",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret=(
            "future-watch-route-secret-at-least-32-bytes"
        ),
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            item = ExecutionPlanItem(
                id="future-watch-item",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                provider="kis",
                asset_class="stock",
                risk_context=risk_context,
                quantity_policy=quantity_policy,
            )
            db.add(
                ExecutionPlan(
                    id="future-watch-plan",
                    external_plan_id="future-watch-plan",
                    source="kai-trading-core",
                    market_date=date(2099, 9, 10),
                    accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                    mode="paper",
                    status="accepted",
                    items=[item],
                )
            )
            db.commit()

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            ) == []
            result = monitoring_service._evaluate_monitoring_cycle(
                settings=settings,
                db=db,
                adapter=SimpleNamespace(provider="kis"),  # type: ignore[arg-type]
                prices={
                    "005930": MonitoringPriceRequest(
                        symbol="005930",
                        price=Decimal("71000"),
                    )
                },
                symbols=["005930"],
                triggered_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
                allow_persisted_triggered_without_price=True,
                update_checkpoint_if_idle=False,
            )
            assert result.payload["evaluated_item_count"] == 0
            assert item.status == "watch_monitoring_active"
    finally:
        engine.dispose()


def test_sqlite_fast_path_returns_every_watch_symbol_for_downstream_round_robin(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-candidates.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=1,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    symbols = [f"{index:06d}" for index in range(1, 12)]
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=session_factory,
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )

    try:
        with session_factory() as db:
            db.add(
                ExecutionPlan(
                    external_plan_id="sqlite-full-watch-universe",
                    source="test",
                    market_date=date(2026, 9, 8),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            plan_item_index=index,
                            symbol=symbol,
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                        )
                        for index, symbol in enumerate(symbols)
                    ],
                )
            )
            db.commit()

        assert coordinator._load_local_desired_monitoring_symbols_from_sqlite() == symbols
    finally:
        engine.dispose()


def test_monitoring_cycle_applies_core_priority_before_round_robin(monkeypatch) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=1,
        monitoring_max_broker_actions_per_cycle=0,
    )
    coordinator = AutonomousRuntimeCoordinator(
        settings=settings,
        session_factory=None,  # type: ignore[arg-type]
        broker_adapter_factory=lambda settings: None,  # type: ignore[arg-type,return-value]
    )
    requested_symbols: list[str] = []
    monkeypatch.setattr(
        "app.services.autonomous_runtime._load_core_desired_monitoring_symbols",
        lambda settings: ["999999"],
    )
    monkeypatch.setattr(
        coordinator,
        "_load_local_desired_monitoring_symbols",
        lambda: ["000001", "999999", "000002"],
    )
    monkeypatch.setattr(coordinator, "_load_local_position_symbols", lambda: [])
    monkeypatch.setattr(
        coordinator,
        "_load_monitoring_prices",
        lambda **kwargs: requested_symbols.extend(kwargs["symbols"]) or [],
    )
    monkeypatch.setattr(coordinator, "_refresh_monitoring_snapshot_filled_today", lambda now: None)

    payload = coordinator._monitoring_cycle_sync(datetime(2026, 9, 8, 1, tzinfo=UTC))

    assert requested_symbols == ["999999"]
    assert payload["monitored_symbols"] == ["999999"]


def test_live_watch_uses_calendar_safety_ttl_when_session_evidence_is_missing() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_expire_prior_market_watch_buys_enabled=False,
        monitoring_watch_buy_max_age_trading_sessions=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as db:
            item = ExecutionPlanItem(
                id="undated-old-watch-item",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="undated-old-watch-plan",
                    source="test",
                    market_date=None,
                    accepted_at=datetime(2026, 8, 31, 0, tzinfo=UTC),
                    mode="live",
                    status="accepted",
                    items=[item],
                )
            )
            db.commit()

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            ) == []
            assert item.status == "watch_expired"
            event = item.risk_context["last_monitoring_event"]
            assert event["event"] == "watch_buy_calendar_safety_ttl_elapsed"
            assert event["reason"] == "calendar_safety_ttl_elapsed"
            assert event["source_market_date"] == "2026-08-31"
    finally:
        engine.dispose()


def test_watch_ttl_counts_observed_market_sessions_not_weekends_or_closures() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        monitoring_watch_buy_max_age_trading_sessions=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as db:
            item = ExecutionPlanItem(
                id="pre-closure-watch-item",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
            )
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="pre-closure-watch-plan",
                        source="test",
                        market_date=date(2026, 9, 4),
                        accepted_at=datetime(2026, 9, 4, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                        items=[item],
                    ),
                    ExecutionPlan(
                        external_plan_id="post-closure-first-session",
                        source="test-session-marker",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                    ),
                    ExecutionPlan(
                        external_plan_id="post-closure-second-session",
                        source="test-session-marker",
                        market_date=date(2026, 9, 9),
                        accepted_at=datetime(2026, 9, 9, 7, tzinfo=UTC),
                        mode="live",
                        status="accepted",
                    ),
                ]
            )
            db.commit()

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 7),
            ) == ["005930"]
            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 8),
            ) == ["005930"]
            assert item.status == "watch_monitoring_active"

            assert list_autonomous_monitoring_symbols(
                settings=settings,
                db=db,
                current_market_date=date(2026, 9, 9),
            ) == []
            assert item.status == "watch_expired"
    finally:
        engine.dispose()


def test_loaded_watch_work_uses_only_latest_item_in_runtime_and_keeps_all_sells() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    def plan(
        *,
        plan_id: str,
        mode: str,
        market_date: date,
        accepted_at: datetime,
        items: list[ExecutionPlanItem],
    ) -> ExecutionPlan:
        return ExecutionPlan(
            id=plan_id,
            external_plan_id=plan_id,
            source="test",
            market_date=market_date,
            accepted_at=accepted_at,
            mode=mode,
            status="accepted",
            items=items,
        )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    plan(
                        plan_id="live-old-plan",
                        mode="live",
                        market_date=date(2026, 9, 7),
                        accepted_at=datetime(2026, 9, 7, 7, tzinfo=UTC),
                        items=[
                                ExecutionPlanItem(
                                    id="live-old-buy",
                                plan_item_index=0,
                                symbol="005930",
                                side="buy",
                                    intent="intraday_watch_buy",
                                    status="watch_monitoring_active",
                                    source_plan_item_id="same-live-watch-family",
                            )
                        ],
                    ),
                    plan(
                        plan_id="live-new-plan",
                        mode="live",
                        market_date=date(2026, 9, 8),
                        accepted_at=datetime(2026, 9, 8, 7, tzinfo=UTC),
                        items=[
                                ExecutionPlanItem(
                                    id="live-new-buy",
                                plan_item_index=0,
                                symbol="005930",
                                side="buy",
                                    intent="intraday_watch_buy",
                                    status="watch_monitoring_active",
                                    source_plan_item_id="same-live-watch-family",
                            ),
                            ExecutionPlanItem(
                                id="live-target-sell",
                                plan_item_index=1,
                                symbol="005930",
                                side="sell",
                                intent="target_sell",
                                status="target_sell_monitoring",
                            ),
                            ExecutionPlanItem(
                                id="live-stop-sell",
                                plan_item_index=2,
                                symbol="005930",
                                side="sell",
                                intent="stop_sell",
                                status="stop_sell_monitoring",
                            ),
                        ],
                    ),
                    plan(
                        plan_id="paper-newest-plan",
                        mode="paper",
                        market_date=date(2026, 9, 9),
                        accepted_at=datetime(2026, 9, 9, 7, tzinfo=UTC),
                        items=[
                            ExecutionPlanItem(
                                id="paper-newest-buy",
                                plan_item_index=0,
                                symbol="005930",
                                side="buy",
                                intent="intraday_watch_buy",
                                status="watch_monitoring_active",
                            )
                        ],
                    ),
                ]
            )
            db.commit()

            items = _load_monitored_items(
                settings=settings,
                db=db,
                symbols=["005930"],
            )

        assert {item.id for item in items} == {
            "live-new-buy",
            "live-target-sell",
            "live-stop-sell",
        }
    finally:
        engine.dispose()


def test_same_symbol_buy_and_protective_sell_keep_side_specific_orderability(
    monkeypatch,
) -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    observed_at = datetime(2026, 9, 8, 12, tzinfo=UTC)

    def fake_route(**kwargs) -> RouteDecision:
        is_protective_sell = (
            kwargs["side"] == "sell" and kwargs["purpose"] == "protective_stop"
        )
        return RouteDecision(
            action="submit" if is_protective_sell else "block",
            exchange="KRX",
            session="test_sell_open" if is_protective_sell else "test_buy_closed",
            ord_dvsn="00" if is_protective_sell else None,
            effective_order_type="limit",
            price_source="explicit_limit" if is_protective_sell else "none",
            next_retry_at=None,
            reason=(
                "protective_sell_orderable"
                if is_protective_sell
                else "new_buy_not_orderable"
            ),
        )

    monkeypatch.setattr(
        monitoring_price_source,
        "resolve_domestic_order_route",
        fake_route,
    )

    try:
        with session_factory() as db:
            buy = ExecutionPlanItem(
                id="same-symbol-buy",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="intraday_watch_buy",
                status="watch_monitoring_active",
                risk_context={"exchange": "KRX"},
            )
            protective_sell = ExecutionPlanItem(
                id="same-symbol-stop",
                plan_item_index=1,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                risk_context={"exchange": "KRX"},
            )
            db.add(
                ExecutionPlan(
                    external_plan_id="same-symbol-opposite-sides",
                    source="test",
                    market_date=date(2026, 9, 8),
                    mode="live",
                    status="accepted",
                    items=[buy, protective_sell],
                )
            )
            db.commit()

            execution_context = (
                monitoring_price_source._load_monitoring_execution_context_by_symbol(
                    settings=settings,
                    db=db,
                    symbols=["005930"],
                )["005930"]
            )

            assert {
                (context["side"], context["purpose"])
                for context in execution_context["order_contexts"]
            } == {
                ("buy", "intraday_watch_buy"),
                ("sell", "protective_stop"),
            }

            price = monitoring_price_source.monitoring_price_from_broker_quote(
                settings=settings,
                quote=BrokerCurrentPrice(
                    symbol="005930",
                    exchange="KRX",
                    price=Decimal("71000"),
                    observed_at=observed_at,
                    payload={},
                ),
                context=None,
                context_failure=None,
                side="buy",
                purpose="intraday_watch_buy",
                order_contexts=execution_context["order_contexts"],
            )

            buy_quality = monitoring_service._market_quality_context_from_price(
                price,
                item=buy,
            )
            sell_quality = monitoring_service._market_quality_context_from_price(
                price,
                item=protective_sell,
            )

            assert buy_quality is not None
            assert sell_quality is not None
            assert buy_quality["normal_ordering_available"] is False
            assert buy_quality["session_type"] == "test_buy_closed"
            assert sell_quality["normal_ordering_available"] is True
            assert sell_quality["session_type"] == "test_sell_open"
    finally:
        engine.dispose()


def test_same_symbol_buy_and_stop_use_ask_and_bid_for_their_own_triggers() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        monitoring_max_broker_actions_per_cycle=0,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    observed_at = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)
    position_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )

    try:
        with session_factory() as db:
            db.add_all(
                [
                    ExecutionPlan(
                        external_plan_id="same-symbol-side-specific-price",
                        source="test",
                        market_date=date(2026, 9, 8),
                        mode="paper",
                        status="accepted",
                        items=[
                            ExecutionPlanItem(
                                id="same-symbol-ask-buy",
                                plan_item_index=0,
                                symbol="005930",
                                side="buy",
                                intent="intraday_watch_buy",
                                status="watch_monitoring_active",
                                trigger_price=Decimal("101"),
                                quantity_policy={"kind": "shares", "quantity": 1},
                                risk_context={
                                    "exchange": "KRX",
                                    "trigger_direction": "at_or_above",
                                    "chase_limit_price": "102",
                                    "target_price": "110",
                                    "stop_price": "90",
                                },
                            ),
                            ExecutionPlanItem(
                                id="same-symbol-bid-stop",
                                plan_item_index=1,
                                symbol="005930",
                                side="sell",
                                intent="stop_sell",
                                status="stop_sell_monitoring",
                                trigger_price=Decimal("99"),
                                quantity_policy={"kind": "shares", "quantity": 1},
                                risk_context={"exchange": "KRX"},
                            ),
                        ],
                    ),
                    ExecutionPosition(
                        mode="paper",
                        provider=position_identity.provider,
                        account_scope=position_identity.account_scope,
                        exchange=position_identity.exchange,
                        symbol="005930",
                        quantity=1,
                        average_price=Decimal("100"),
                    ),
                ]
            )
            db.commit()

            result = monitoring_service._evaluate_monitoring_cycle(
                settings=settings,
                db=db,
                adapter=SimpleNamespace(provider="kis"),  # type: ignore[arg-type]
                prices={
                    "005930": MonitoringPriceRequest(
                        symbol="005930",
                        price=Decimal("100"),
                        bid=Decimal("99"),
                        ask=Decimal("101"),
                        observed_at=observed_at,
                    )
                },
                symbols=["005930"],
                triggered_at=observed_at,
                allow_persisted_triggered_without_price=True,
                update_checkpoint_if_idle=False,
            )

        results = {
            item["execution_plan_item_id"]: item for item in result.payload["items"]
        }
        assert results["same-symbol-ask-buy"]["observed_price"] == "101.0000"
        assert results["same-symbol-bid-stop"]["observed_price"] == "99.0000"
        assert results["same-symbol-ask-buy"]["outcome"] == "deferred"
        assert results["same-symbol-bid-stop"]["outcome"] == "deferred"
    finally:
        engine.dispose()
