from __future__ import annotations

from datetime import UTC, datetime

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.execution_account_route import (
    bind_execution_account_scope,
    bind_kis_account_route,
    build_execution_account_scope_binding,
)
from app.services.max_holding_actionability import (
    _expected_position_identity,
    max_holding_actionability_summary,
)
from app.services.position_identity import (
    build_authenticated_broker_position_identity,
    build_position_identity,
)


def _settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="operator-display-label",
        account_drawdown_recovery_scope_secret=(
            "max-holding-account-scope-secret-at-least-32-bytes"
        ),
    )


def _max_holding_item(
    *,
    settings: Settings,
    index: int,
    symbol: str,
    broker_account_id: str,
) -> ExecutionPlanItem:
    risk_context, quantity_policy = bind_execution_account_scope(
        provider="alpaca",
        risk_context={
            "exchange": "ALPACA",
            "close_exit_date": "2026-09-08",
        },
        quantity_policy={"kind": "base_quantity", "quantity": "1"},
        binding=build_execution_account_scope_binding(
            settings=settings,
            provider="alpaca",
            exchange="ALPACA",
            broker_account_id=broker_account_id,
        ),
    )
    return ExecutionPlanItem(
        plan_item_index=index,
        symbol=symbol,
        side="sell",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        provider="alpaca",
        venue="ALPACA",
        asset_class="crypto",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )


def test_max_holding_identity_uses_persisted_authenticated_account_binding() -> None:
    settings = _settings()
    item = _max_holding_item(
        settings=settings,
        index=0,
        symbol="BTC/USD",
        broker_account_id="authenticated-account-a",
    )

    expected = _expected_position_identity(settings=settings, item=item)
    authenticated = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="authenticated-account-a",
    )

    assert expected == authenticated


def test_max_holding_summary_isolates_foreign_same_provider_account() -> None:
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    active = _max_holding_item(
        settings=settings,
        index=0,
        symbol="BTC/USD",
        broker_account_id="authenticated-account-a",
    )
    foreign = _max_holding_item(
        settings=settings,
        index=1,
        symbol="ETH/USD",
        broker_account_id="authenticated-account-b",
    )
    plan = ExecutionPlan(
        external_plan_id="max-holding-same-provider-scope",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
        items=[active, foreign],
    )
    active_identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="authenticated-account-a",
    )
    db.add_all(
        [
            plan,
            ExecutionPosition(
                mode=settings.runtime_mode,
                provider=active_identity.provider,
                account_scope=active_identity.account_scope,
                exchange=active_identity.exchange,
                symbol="BTC/USD",
                quantity=1,
            ),
        ]
    )
    db.commit()

    summary = max_holding_actionability_summary(
        settings=settings,
        db=db,
        mode=settings.runtime_mode,
        reference_at=datetime(2026, 9, 9, tzinfo=UTC),
    )

    assert summary["due_item_count"] == 2
    assert summary["missing_position_count"] == 1
    db.close()
    engine.dispose()


def test_max_holding_identity_rejects_foreign_kis_route_binding() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "max-holding-kis-scope-secret-at-least-32-bytes"
        ),
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={
            "exchange": "KRX",
            "close_exit_date": "2026-09-08",
        },
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    foreign_item = ExecutionPlanItem(
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="max_holding_close_sell",
        status="max_holding_close_sell_monitoring",
        provider="kis",
        venue="KRX",
        asset_class="kr_equity",
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )

    assert _expected_position_identity(settings=settings, item=foreign_item) is None

    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    db.add(
        ExecutionPlan(
            external_plan_id="foreign-kis-max-holding-route",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            items=[foreign_item],
        )
    )
    db.commit()

    summary = max_holding_actionability_summary(
        settings=settings,
        db=db,
        mode=settings.runtime_mode,
        reference_at=datetime(2026, 9, 9, tzinfo=UTC),
        provider="kis",
        account_scope=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ).account_scope,
        exchanges=("KRX",),
    )

    assert summary["due_item_count"] == 0
    assert summary["symbols"] == []

    unscoped_summary = max_holding_actionability_summary(
        settings=settings,
        db=db,
        mode=settings.runtime_mode,
        reference_at=datetime(2026, 9, 9, tzinfo=UTC),
    )
    assert unscoped_summary["due_item_count"] == 0
    assert unscoped_summary["symbols"] == []
    db.close()
    engine.dispose()
