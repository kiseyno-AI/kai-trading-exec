from __future__ import annotations

from decimal import Decimal

import pytest

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services.execution_account_route import (
    bind_execution_account_scope,
    build_execution_account_scope_binding,
    bind_kis_account_route,
)
from app.services.execution_api import (
    ExecutionPositionScopeError,
    get_runtime_symbol_detail,
    list_execution_positions,
    list_runtime_symbols,
)
from app.services.position_identity import build_position_identity


def test_position_read_surfaces_preserve_same_symbol_routes_without_overwrite() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                provider="kis",
                account_scope="a" * 24,
                exchange="KRX",
                symbol="SHARED",
                quantity=2,
                average_price=Decimal("100.0000"),
                updated_from="reconciliation",
            ),
            ExecutionPosition(
                mode="paper",
                provider="alpaca",
                account_scope="b" * 24,
                exchange="NASDAQ",
                symbol="SHARED",
                quantity=3,
                average_price=Decimal("200.0000"),
                updated_from="reconciliation",
            ),
        ]
    )
    db.commit()

    positions = list_execution_positions(settings=settings, db=db)
    summaries = list_runtime_symbols(settings=settings, db=db)
    detail = get_runtime_symbol_detail(
        settings=settings,
        db=db,
        symbol="SHARED",
    )

    assert [position["provider"] for position in positions] == ["alpaca", "kis"]
    assert len(summaries) == 1
    assert summaries[0]["position_quantity"] == 5
    assert summaries[0]["position_average_price"] is None
    assert summaries[0]["position_route_count"] == 2
    assert summaries[0]["position_scope_complete"] is True
    assert detail is not None
    assert detail["position"] is None
    assert [position["provider"] for position in detail["positions"]] == [
        "alpaca",
        "kis",
    ]
    assert detail["position_ambiguity"] == {
        "ambiguous": True,
        "reason": "multiple_execution_position_routes",
        "route_count": 2,
    }
    db.close()
    engine.dispose()


def test_plan_scoped_position_read_excludes_same_symbol_foreign_account_route() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "position-plan-scope-secret-at-least-32-bytes"
        ),
    )
    foreign_settings = settings.model_copy(
        update={"kis_account_number": "87654321"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    plan = ExecutionPlan(
        external_plan_id="position-account-scoped-plan",
        source="kai-trading-core",
        mode="paper",
        status="accepted",
        items=[
            ExecutionPlanItem(
                plan_item_index=0,
                symbol="SHARED",
                side="buy",
                intent="next_open_buy",
                status="accepted",
                provider="kis",
                venue="KRX",
                asset_class="stock",
                risk_context=risk_context,
                quantity_policy=quantity_policy,
            )
        ],
    )
    active_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    foreign_identity = build_position_identity(
        settings=foreign_settings,
        provider="kis",
        exchange="KRX",
    )
    db.add(plan)
    db.flush()
    db.add_all(
        [
            ExecutionPosition(
                mode="paper",
                provider=active_identity.provider,
                account_scope=active_identity.account_scope,
                exchange=active_identity.exchange,
                symbol="SHARED",
                quantity=5,
                average_price=Decimal("100"),
                updated_from="reconciliation",
            ),
            ExecutionPosition(
                mode="paper",
                provider=foreign_identity.provider,
                account_scope=foreign_identity.account_scope,
                exchange=foreign_identity.exchange,
                symbol="SHARED",
                quantity=100,
                average_price=Decimal("999"),
                updated_from="reconciliation",
            ),
        ]
    )
    db.commit()

    positions = list_execution_positions(
        settings=settings,
        db=db,
        external_plan_id=plan.external_plan_id,
        execution_plan_id=plan.id,
        symbols={"SHARED"},
    )

    assert [position["quantity"] for position in positions] == [5]
    assert positions[0]["position_scope_proof"] == {
        "schema_version": "execution_position_plan_scope_v1",
        "plan_id": plan.external_plan_id,
        "execution_plan_id": plan.id,
        "provider": active_identity.provider,
        "account_scope": active_identity.account_scope,
        "exchange": active_identity.exchange,
    }
    with pytest.raises(ExecutionPositionScopeError, match="execution_plan_id_mismatch"):
        list_execution_positions(
            settings=settings,
            db=db,
            external_plan_id=plan.external_plan_id,
            execution_plan_id="wrong-plan-id",
            symbols={"SHARED"},
        )

    db.close()
    engine.dispose()


def test_runtime_symbols_exclude_foreign_same_provider_plan_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="active-upbit-account",
        account_drawdown_recovery_scope_secret=(
            "runtime-symbol-account-scope-secret-at-least-32-bytes"
        ),
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-upbit-account"}
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    db = create_session_factory(engine)()
    plan = ExecutionPlan(
        external_plan_id="same-provider-route-isolation",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
    )
    for index, (item_settings, symbol) in enumerate(
        ((settings, "KRW-BTC"), (foreign_settings, "KRW-ETH"))
    ):
        risk_context, quantity_policy = bind_execution_account_scope(
            provider="upbit",
            risk_context={"exchange": "UPBIT"},
            quantity_policy={"kind": "quote_notional", "amount": "10000"},
            binding=build_execution_account_scope_binding(
                settings=item_settings,
                provider="upbit",
                exchange="UPBIT",
            ),
        )
        plan.items.append(
            ExecutionPlanItem(
                plan_item_index=index,
                symbol=symbol,
                side="buy",
                intent="watch_buy",
                status="watch_monitoring_active",
                provider="upbit",
                venue="UPBIT",
                asset_class="crypto",
                risk_context=risk_context,
                quantity_policy=quantity_policy,
            )
        )
    db.add(plan)
    db.commit()
    active_binding = build_execution_account_scope_binding(
        settings=settings,
        provider="upbit",
        exchange="UPBIT",
    )

    rows = list_runtime_symbols(
        settings=settings,
        db=db,
        provider="upbit",
        account_scope=str(active_binding["account_scope"]),
        exchanges=("UPBIT",),
    )

    assert [row["symbol"] for row in rows] == ["KRW-BTC"]
    db.close()
    engine.dispose()
