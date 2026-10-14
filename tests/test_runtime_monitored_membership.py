from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
)
from app.services import runtime as runtime_service
from app.services.execution_account_route import bind_kis_account_route
from app.services.position_identity import build_position_identity, execution_scope_payload


def _settings(**updates: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "sqlite:///:memory:",
        "runtime_mode": "live",
        "runtime_broker_provider": "kis",
        "kis_account_number": "12345678",
        "kis_account_product_code": "01",
        "account_drawdown_recovery_scope_secret": (
            "runtime-membership-scope-secret-32-bytes"
        ),
    }
    values.update(updates)
    return Settings(**values)


def _app(settings: Settings):  # type: ignore[no-untyped-def]
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    return (
        create_app(
            settings=settings,
            engine=engine,
            session_factory=session_factory,
        ),
        engine,
        session_factory,
    )


def _current_krx_market_date() -> date:
    return datetime.now(UTC).astimezone(runtime_service.KRX_TIMEZONE).date()


def _monitored_item(
    settings: Settings,
    *,
    symbol: str,
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": exchange},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    return ExecutionPlanItem(
        symbol=symbol,
        side="buy",
        intent="watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue=exchange,
        asset_class="kr_equity",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )


def _monitored_sell_item(
    settings: Settings,
    *,
    symbol: str,
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    item = _monitored_item(settings, symbol=symbol, exchange=exchange)
    item.side = "sell"
    item.intent = "stop_sell"
    item.status = "stop_sell_monitoring"
    return item


def _held_position(
    settings: Settings,
    *,
    symbol: str,
    quantity: int = 1,
    base_quantity: Decimal | None = None,
) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    return ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol=symbol,
        quantity=quantity,
        base_quantity=base_quantity,
        average_price=Decimal("100"),
        updated_from="test",
    )


def test_membership_returns_full_db_set_beyond_monitoring_cycle_batch() -> None:
    settings = _settings(monitoring_max_symbols_per_cycle=1)
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="full-runtime-membership",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            market_date=_current_krx_market_date(),
        )
        plan.items.extend(
            [
                _monitored_item(settings, symbol="000660"),
                _monitored_item(settings, symbol="005930"),
                _monitored_item(settings, symbol="035420"),
            ]
        )
        db.add(plan)
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": "runtime_monitored_symbol_membership_v1",
        "market_date": response.json()["market_date"],
        "generated_at": response.json()["generated_at"],
        "membership_complete": True,
        "symbols": ["000660", "005930", "035420"],
        "membership_scope": {
            "market": "KRX",
            "exchanges": ["KOSDAQ", "KOSPI", "KRX", "NXT", "SOR"],
        },
        "coverage": {
            "status": "complete",
            "reason": None,
            "provider": "kis",
            "runtime_mode": "live",
            "execution_scope": {
                "provider": "kis",
                "status": "available",
                "source": "configured_broker_account",
                "reason": None,
                "exchanges": [
                    "AMEX",
                    "KOSDAQ",
                    "KOSPI",
                    "KRX",
                    "NASD",
                    "NXT",
                    "NYSE",
                    "SOR",
                ],
            },
        },
    }
    generated_at = datetime.fromisoformat(response.json()["generated_at"])
    assert generated_at.tzinfo is not None
    assert generated_at.utcoffset() == UTC.utcoffset(generated_at)

    engine.dispose()


def test_membership_includes_only_quote_monitorable_sizing_deferrals() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        price_watch_risk, price_watch_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={
                "exchange": "KRX",
                "watch_exec_agent_deferred": {
                    "reason_family": "reward_risk",
                    "disposition": "defer",
                    "price_watch_only": True,
                },
            },
            quantity_policy={"type": "exec_agent_sized", "budget_krw": "150000"},
        )
        ordinary_risk, ordinary_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={
                "exchange": "KRX",
                "watch_exec_agent_deferred": {
                    "reason_family": "account_snapshot",
                    "disposition": "retry",
                },
            },
            quantity_policy={"type": "exec_agent_sized", "budget_krw": "150000"},
        )
        db.add(
            ExecutionPlan(
                external_plan_id="price-watch-runtime-membership",
                source="test",
                mode=settings.runtime_mode,
                status="accepted",
                market_date=_current_krx_market_date(),
                items=[
                    ExecutionPlanItem(
                        symbol="028670",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_sizing_pending",
                        provider="kis",
                        venue="KRX",
                        asset_class="kr_equity",
                        trigger_price=Decimal("6630"),
                        risk_context=price_watch_risk,
                        quantity_policy=price_watch_policy,
                    ),
                    ExecutionPlanItem(
                        symbol="041960",
                        side="buy",
                        intent="intraday_watch_buy",
                        status="watch_sizing_pending",
                        provider="kis",
                        venue="KRX",
                        asset_class="kr_equity",
                        trigger_price=Decimal("11900"),
                        risk_context=ordinary_risk,
                        quantity_policy=ordinary_policy,
                    ),
                ],
            )
        )
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["028670"]

    engine.dispose()


def test_membership_allows_authoritative_empty_set() -> None:
    settings = _settings()
    app, engine, _session_factory = _app(settings)

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == []
    assert response.json()["coverage"]["status"] == "complete"
    assert response.json()["coverage"]["reason"] is None

    engine.dispose()


def test_membership_includes_scoped_held_position_without_sell_plan() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        db.add(_held_position(settings, symbol="030000"))
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["030000"]

    engine.dispose()


def test_membership_includes_scoped_positive_base_quantity_position() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        db.add(
            _held_position(
                settings,
                symbol="035420",
                quantity=0,
                base_quantity=Decimal("0.5"),
            )
        )
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["035420"]

    engine.dispose()


@pytest.mark.parametrize("symbol", ["AAPL", "００５９３０"])
def test_membership_fails_closed_for_invalid_krx_position_symbol(
    symbol: str,
) -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        db.add(_held_position(settings, symbol=symbol))
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is False
    assert response.json()["symbols"] == []
    assert response.json()["coverage"]["status"] == "unavailable"
    assert (
        response.json()["coverage"]["reason"]
        == "runtime_membership_invalid_krx_symbol"
    )

    engine.dispose()


def test_membership_excludes_foreign_account_and_us_held_positions() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(
        update={'kis_account_number': '87654321'}
    )
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        foreign_position = _held_position(
            foreign_settings,
            symbol="066570",
        )
        us_identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="NYSE",
        )
        db.add_all(
            [
                foreign_position,
                ExecutionPosition(
                    mode=settings.runtime_mode,
                    provider=us_identity.provider,
                    account_scope=us_identity.account_scope,
                    exchange=us_identity.exchange,
                    symbol="AAPL",
                    quantity=1,
                    average_price=Decimal("100"),
                    updated_from="test",
                ),
            ]
        )
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == []

    engine.dispose()


def test_membership_matches_nxt_sell_plan_to_canonical_krx_position() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="nxt-sell-canonical-position",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
        )
        plan.items.append(
            _monitored_sell_item(settings, symbol="027710", exchange="NXT")
        )
        db.add_all([plan, _held_position(settings, symbol="027710")])
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["027710"]

    engine.dispose()


def test_membership_excludes_future_active_watch(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    fixed_now = datetime(2026, 9, 14, 0, 0, tzinfo=UTC)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(runtime_service, "datetime", FixedDatetime)
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="future-active-watch",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            market_date=fixed_now.astimezone(runtime_service.KRX_TIMEZONE).date().replace(
                day=15
            ),
        )
        plan.items.append(_monitored_item(settings, symbol="005930"))
        db.add(plan)
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["market_date"] == "2026-09-14"
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == []

    engine.dispose()


def test_membership_includes_pending_sell_for_unexited_buy() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        risk_context, quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"type": "position_exit_from_entry"},
        )
        plan = ExecutionPlan(
            external_plan_id="pending-sell-unexited-buy",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            market_date=_current_krx_market_date() + timedelta(days=1),
        )
        pending_sell = ExecutionPlanItem(
            symbol="002380",
            side="sell",
            intent="stop_sell",
            status="stop_sell_pending_config",
            provider="kis",
            venue="KRX",
            asset_class="kr_equity",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )
        plan.items.append(pending_sell)
        db.add(plan)
        db.flush()
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
        db.add(
            BrokerOrder(
                broker_name="kis",
                mode=settings.runtime_mode,
                execution_plan_item_id=pending_sell.id,
                idempotency_key="membership-unexited-buy",
                symbol=pending_sell.symbol,
                side="buy",
                status="filled",
                quantity=1,
                venue="KRX",
                asset_class="kr_equity",
                limit_price=Decimal("567000"),
                request_metadata={
                    "exchange": "KRX",
                    "execution_scope": execution_scope_payload(
                        identity=identity,
                        venue="KRX",
                        asset_class="kr_equity",
                    ),
                },
                submitted_at=datetime(2026, 9, 14, 0, 0, tzinfo=UTC),
            )
        )
        db.commit()

    statement_count = 0

    def count_statement(*_args: object, **_kwargs: object) -> None:
        nonlocal statement_count
        statement_count += 1

    event.listen(engine, "before_cursor_execute", count_statement)
    try:
        response = TestClient(app).get("/runtime/monitored-symbol-membership")
    finally:
        event.remove(engine, "before_cursor_execute", count_statement)

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["002380"]
    assert statement_count <= 8

    engine.dispose()


def test_membership_excludes_pending_sell_without_unexited_buy_in_paper() -> None:
    settings = _settings(runtime_mode="paper")
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        risk_context, quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"type": "position_exit_from_entry"},
        )
        plan = ExecutionPlan(
            external_plan_id="paper-pending-sell-without-entry",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
        )
        plan.items.append(
            ExecutionPlanItem(
                symbol="009830",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                provider="kis",
                venue="KRX",
                asset_class="kr_equity",
                risk_context=risk_context,
                quantity_policy=quantity_policy,
            )
        )
        db.add(plan)
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == []

    engine.dispose()


def test_membership_fails_closed_when_execution_scope_is_unavailable() -> None:
    settings = _settings(
        kis_account_number="",
        kis_account_product_code="",
        execution_position_account_scope="",
    )
    app, engine, _session_factory = _app(settings)

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is False
    assert response.json()["symbols"] == []
    assert response.json()["coverage"] == {
        "status": "unavailable",
        "reason": "runtime_execution_scope_unavailable",
        "provider": "kis",
        "runtime_mode": "live",
        "execution_scope": {
            "provider": "kis",
            "status": "unavailable",
            "source": None,
            "reason": "stable_account_scope_unavailable",
            "exchanges": [
                "AMEX",
                "KOSDAQ",
                "KOSPI",
                "KRX",
                "NASD",
                "NXT",
                "NYSE",
                "SOR",
            ],
        },
    }

    engine.dispose()


def test_membership_fails_closed_when_membership_query_is_unavailable(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    settings = _settings()
    app, engine, _session_factory = _app(settings)
    monkeypatch.setattr(
        runtime_service,
        "runtime_monitored_symbols",
        lambda **_kwargs: (_ for _ in ()).throw(SQLAlchemyError("missing")),
    )

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is False
    assert response.json()["symbols"] == []
    assert response.json()["coverage"]["status"] == "unavailable"
    assert (
        response.json()["coverage"]["reason"]
        == "runtime_membership_query_unavailable"
    )

    engine.dispose()


def test_membership_fails_closed_when_scope_check_query_is_unavailable(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    settings = _settings()
    app, engine, _session_factory = _app(settings)
    monkeypatch.setattr(
        runtime_service,
        "_runtime_membership_indeterminate_position_count",
        lambda **_kwargs: (_ for _ in ()).throw(SQLAlchemyError("missing")),
    )

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is False
    assert response.json()["symbols"] == []
    assert response.json()["coverage"]["status"] == "unavailable"
    assert (
        response.json()["coverage"]["reason"]
        == "runtime_membership_query_unavailable"
    )

    engine.dispose()


def test_membership_reports_known_symbols_but_not_complete_when_data_is_blocked(
) -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="blocked-runtime-membership",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            market_date=_current_krx_market_date(),
        )
        plan.items.append(_monitored_item(settings, symbol="027710"))
        db.add(plan)
        db.add(
            ExecutionPosition(
                mode="live",
                symbol="LEGACY-UNSCOPED",
                quantity=1,
                average_price=Decimal("100"),
                updated_from="test",
            )
        )
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is False
    assert response.json()["symbols"] == ["027710"]
    assert response.json()["coverage"]["status"] == "unavailable"
    assert response.json()["coverage"]["reason"] == "runtime_membership_scope_blocked"

    engine.dispose()


def test_membership_ignores_unrelated_mode_and_provider_blockers() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        db.execute(text("PRAGMA ignore_check_constraints = ON"))
        db.execute(
            text(
                """
                INSERT INTO execution_positions
                    (id, mode, provider, account_scope, exchange, symbol,
                     quantity, average_price, updated_from)
                VALUES
                    ('paper-legacy', 'paper', NULL, NULL, NULL,
                     'PAPER-LEGACY', 1, 100, 'test'),
                    ('upbit-legacy', 'live', 'upbit', NULL, NULL,
                     'UPBIT-LEGACY', 1, 100, 'test')
                """
            )
        )
        db.commit()
        db.execute(text("PRAGMA ignore_check_constraints = OFF"))

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == []
    assert response.json()["coverage"]["status"] == "complete"

    engine.dispose()


def test_membership_excludes_kis_us_symbols_from_krx_scope() -> None:
    settings = _settings()
    app, engine, session_factory = _app(settings)
    with session_factory() as db:
        plan = ExecutionPlan(
            external_plan_id="mixed-kis-membership",
            source="test",
            mode=settings.runtime_mode,
            status="accepted",
            market_date=_current_krx_market_date(),
        )
        plan.items.extend(
            [
                _monitored_item(settings, symbol="005930"),
                _monitored_item(settings, symbol="AAPL", exchange="NYSE"),
            ]
        )
        db.add(plan)
        db.commit()

    response = TestClient(app).get("/runtime/monitored-symbol-membership")

    assert response.status_code == 200
    assert response.json()["membership_complete"] is True
    assert response.json()["symbols"] == ["005930"]
    assert response.json()["membership_scope"]["market"] == "KRX"

    engine.dispose()


def test_membership_endpoint_keeps_query_budget_bounded() -> None:
    settings = _settings()
    app, engine, _session_factory = _app(settings)
    statement_count = 0

    def count_statement(*_args: object, **_kwargs: object) -> None:
        nonlocal statement_count
        statement_count += 1

    event.listen(engine, "before_cursor_execute", count_statement)
    try:
        response = TestClient(app).get("/runtime/monitored-symbol-membership")
    finally:
        event.remove(engine, "before_cursor_execute", count_statement)

    assert response.status_code == 200
    assert statement_count <= 3

    engine.dispose()
