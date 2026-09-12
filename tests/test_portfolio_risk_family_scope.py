from datetime import UTC, datetime
from decimal import Decimal

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services.execution_account_route import (
    bind_execution_account_scope,
    bind_kis_account_route,
    build_execution_account_scope_binding,
)
from app.services.portfolio_risk import (
    ACTIVE_MONITORING_STATUSES,
    HoldingRiskRow,
    PortfolioPositionScope,
    _estimated_stop_risk_payload,
    _load_stop_metadata,
    _plan_item_position_scope_classification,
)
from app.services.position_identity import build_position_identity


def _settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="portfolio-family-secret-at-least-32-bytes",
    )


def _session(settings: Settings):  # type: ignore[no-untyped-def]
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    factory = create_session_factory(engine)
    return engine, factory()


def _stop_item(
    settings: Settings,
    *,
    item_id: str,
    family_key: str | None,
    stop_price: str,
    quantity: int,
    accepted_at: datetime,
) -> ExecutionPlanItem:
    quantity_policy: dict[str, object] = {"kind": "shares", "quantity": quantity}
    if family_key is not None:
        quantity_policy["linked_entry_item_key"] = family_key
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX", "stop_price": stop_price},
        quantity_policy=quantity_policy,
    )
    return ExecutionPlanItem(
        id=item_id,
        plan=ExecutionPlan(
            external_plan_id=f"plan-{item_id}",
            source="test",
            mode="paper",
            status="accepted",
            accepted_at=accepted_at,
        ),
        plan_item_index=0,
        symbol="005930",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        trigger_price=Decimal(stop_price),
        source_plan_item_id=f"{item_id}:stop",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )


def _holding() -> HoldingRiskRow:
    return HoldingRiskRow(
        symbol="005930",
        quantity=20,
        base_quantity=None,
        average_price=Decimal("100"),
        market_value=Decimal("2000"),
        settlement_currency="KRW",
        currency_source="test",
        source="test",
    )


def _scope(settings: Settings) -> PortfolioPositionScope:
    return PortfolioPositionScope(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
        exchanges=("KRX",),
    )


def test_stop_risk_is_split_by_exact_same_symbol_entry_family() -> None:
    settings = _settings()
    engine, db = _session(settings)
    family_a = _stop_item(
        settings,
        item_id="family-a-stop",
        family_key="family-a-entry",
        stop_price="90",
        quantity=10,
        accepted_at=datetime(2026, 9, 9, tzinfo=UTC),
    )
    family_b = _stop_item(
        settings,
        item_id="family-b-stop",
        family_key="family-b-entry",
        stop_price="99",
        quantity=10,
        accepted_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    db.add_all([family_a.plan, family_b.plan])
    db.commit()

    metadata, scope_summary = _load_stop_metadata(
        settings=settings,
        db=db,
        position_scope=_scope(settings),
    )
    rows = _estimated_stop_risk_payload(
        holdings=[_holding()],
        stop_metadata=metadata,
        ambiguous_symbols=set(scope_summary["ambiguous_family_symbols"]),
    )

    by_family = {row["entry_family_key"]: row for row in rows}
    assert by_family["source:family-a-entry"]["protected_quantity"] == "10.0000"
    assert by_family["source:family-a-entry"]["stop_price"] == "90.0000"
    assert by_family["source:family-a-entry"]["estimated_risk"] == "100.0000"
    assert by_family["source:family-b-entry"]["protected_quantity"] == "10.0000"
    assert by_family["source:family-b-entry"]["stop_price"] == "99.0000"
    assert by_family["source:family-b-entry"]["estimated_risk"] == "10.0000"
    assert sum(Decimal(str(row["estimated_risk"])) for row in rows) == Decimal("110")
    assert all(row["metadata_status"] == "present" for row in rows)
    assert scope_summary["status"] == "ok"
    db.close()
    engine.dispose()


def test_ambiguous_unlinked_same_symbol_stops_fail_closed() -> None:
    settings = _settings()
    engine, db = _session(settings)
    older = _stop_item(
        settings,
        item_id="legacy-stop-a",
        family_key=None,
        stop_price="90",
        quantity=10,
        accepted_at=datetime(2026, 9, 9, tzinfo=UTC),
    )
    newer = _stop_item(
        settings,
        item_id="legacy-stop-b",
        family_key=None,
        stop_price="99",
        quantity=10,
        accepted_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    db.add_all([older.plan, newer.plan])
    db.commit()

    metadata, scope_summary = _load_stop_metadata(
        settings=settings,
        db=db,
        position_scope=_scope(settings),
    )
    rows = _estimated_stop_risk_payload(
        holdings=[_holding()],
        stop_metadata=metadata,
        ambiguous_symbols=set(scope_summary["ambiguous_family_symbols"]),
    )

    assert metadata == {}
    assert scope_summary["status"] == "incomplete"
    assert scope_summary["excluded_missing_or_invalid_scope_count"] == 2
    assert scope_summary["ambiguous_family_symbols"] == ["005930"]
    assert rows == [
        {
            "symbol": "005930",
            "entry_family_key": None,
            "protected_quantity": "20.0000",
            "stop_price": None,
            "estimated_risk": None,
            "settlement_currency": "KRW",
            "currency_source": "test",
            "metadata_status": "unavailable",
            "metadata_reason": "ambiguous_entry_family",
            "plan_item_id": None,
        }
    ]
    assert older.status == "stop_sell_monitoring"
    assert newer.status == "stop_sell_monitoring"
    db.close()
    engine.dispose()


def test_stop_metadata_excludes_foreign_same_provider_account_scope() -> None:
    settings = Settings(
        runtime_mode="paper",
        execution_position_account_scope="active-upbit-account",
        account_drawdown_recovery_scope_secret=(
            "portfolio-account-scope-secret-at-least-32-bytes"
        ),
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-upbit-account"}
    )
    active_identity = build_position_identity(
        settings=settings,
        provider="upbit",
        exchange="UPBIT",
    )
    position_scope = PortfolioPositionScope(
        identity=active_identity,
        exchanges=("UPBIT",),
    )

    def _item(item_settings: Settings, symbol: str) -> ExecutionPlanItem:
        risk_context, quantity_policy = bind_execution_account_scope(
            provider="upbit",
            risk_context={"exchange": "UPBIT", "stop_price": "90"},
            quantity_policy={"kind": "base_quantity", "quantity": "1"},
            binding=build_execution_account_scope_binding(
                settings=item_settings,
                provider="upbit",
                exchange="UPBIT",
            ),
        )
        return ExecutionPlanItem(
            symbol=symbol,
            side="sell",
            intent="stop_sell",
            status="stop_sell_monitoring",
            provider="upbit",
            venue="UPBIT",
            asset_class="crypto",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    active = _item(settings, "KRW-BTC")
    foreign = _item(foreign_settings, "KRW-ETH")
    legacy = ExecutionPlanItem(
        symbol="KRW-XRP",
        side="sell",
        intent="stop_sell",
        status="stop_sell_monitoring",
        provider="upbit",
        venue="UPBIT",
        asset_class="crypto",
        risk_context={"exchange": "UPBIT", "stop_price": "90"},
        quantity_policy={"kind": "base_quantity", "quantity": "1"},
    )

    assert _plan_item_position_scope_classification(
        settings=settings,
        item=active,
        position_scope=position_scope,
    ) == "active"
    assert _plan_item_position_scope_classification(
        settings=settings,
        item=foreign,
        position_scope=position_scope,
    ) == "foreign"
    assert _plan_item_position_scope_classification(
        settings=settings,
        item=legacy,
        position_scope=position_scope,
    ) == "invalid"


def test_target_only_family_does_not_claim_stop_loss_protection() -> None:
    settings = _settings()
    engine, db = _session(settings)
    target_only = _stop_item(
        settings,
        item_id="target-only-family",
        family_key="target-only-entry",
        stop_price="90",
        quantity=10,
        accepted_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    target_only.intent = "target_sell"
    target_only.status = "target_sell_monitoring"
    db.add(target_only.plan)
    db.commit()

    metadata, scope_summary = _load_stop_metadata(
        settings=settings,
        db=db,
        position_scope=_scope(settings),
    )

    assert metadata == {}
    assert scope_summary["status"] == "incomplete"
    assert scope_summary["ambiguous_family_symbols"] == ["005930"]
    db.close()
    engine.dispose()


def test_stop_risk_accepts_only_canonical_triggered_pending_dispatch_status() -> None:
    settings = _settings()
    engine, db = _session(settings)
    canonical = _stop_item(
        settings,
        item_id="canonical-triggered-stop",
        family_key="canonical-triggered-entry",
        stop_price="90",
        quantity=10,
        accepted_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    canonical.status = "stop_sell_triggered_pending_dispatch"
    db.add(canonical.plan)
    db.commit()

    metadata, scope_summary = _load_stop_metadata(
        settings=settings,
        db=db,
        position_scope=_scope(settings),
    )

    assert "stop_sell_triggered_pending_dispatch" in ACTIVE_MONITORING_STATUSES
    assert "stop_sell_pending_dispatch" not in ACTIVE_MONITORING_STATUSES
    assert len(metadata["005930"]) == 1
    assert scope_summary["status"] == "ok"
    db.close()
    engine.dispose()
