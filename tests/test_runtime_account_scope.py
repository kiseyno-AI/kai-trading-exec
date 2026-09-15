from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from app.broker.alpaca import AlpacaBrokerAdapter, AlpacaHttpResponse
from app.broker.upbit import UpbitBrokerAdapter
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
    FifoInventoryIsolation,
    MonitoringCheckpoint,
)
from app.services import runtime as runtime_service
from app.services.autonomous_runtime import _load_filled_today_symbols
from app.services.execution_account_route import (
    bind_execution_account_scope,
    bind_kis_account_route,
    build_execution_account_scope_binding,
)
from app.services.position_identity import (
    build_authenticated_broker_position_identity,
    build_position_identity,
    execution_scope_payload,
)
from app.services.execution_semantics import KRX_EXCHANGES


def _settings() -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="runtime-account-scope-secret-32-bytes",
    )


def _session(settings: Settings):  # type: ignore[no-untyped-def]
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    factory = create_session_factory(engine)
    return engine, factory, factory()


def _bound_plan_item(
    settings: Settings,
    *,
    symbol: str,
    side: str = "buy",
    intent: str = "next_open_buy",
    status: str = "pending",
    exchange: str = "KRX",
    provider: str = "kis",
    broker_account_id: str | None = None,
) -> ExecutionPlanItem:
    if provider == "kis":
        risk_context, quantity_policy = bind_kis_account_route(
            settings=settings,
            provider=provider,
            risk_context={"exchange": exchange},
            quantity_policy={"kind": "shares", "quantity": 1},
        )
    else:
        risk_context, quantity_policy = bind_execution_account_scope(
            provider=provider,
            risk_context={"exchange": exchange},
            quantity_policy={"kind": "shares", "quantity": 1},
            binding=build_execution_account_scope_binding(
                settings=settings,
                provider=provider,
                exchange=exchange,
                broker_account_id=broker_account_id,
            ),
        )
    return ExecutionPlanItem(
        symbol=symbol,
        side=side,
        intent=intent,
        status=status,
        provider=provider,
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )


def _legacy_plan_item(
    *,
    symbol: str,
    side: str = "buy",
    intent: str = "next_open_buy",
    status: str = "pending",
    exchange: str = "KRX",
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        symbol=symbol,
        side=side,
        intent=intent,
        status=status,
        provider="kis",
        risk_context={"exchange": exchange},
        quantity_policy={"kind": "shares", "quantity": 1},
    )


def _canonical_order(
    settings: Settings | None,
    *,
    key: str,
    symbol: str,
    side: str,
    status: str,
    exchange: str = "KRX",
    mode: str = "live",
    observed_at: datetime | None = None,
    provider: str = "kis",
    broker_account_id: str | None = None,
) -> BrokerOrder:
    asset_class = (
        "us_equity"
        if provider == "alpaca" and exchange == "US"
        else "crypto"
        if provider in {"alpaca", "upbit"}
        else "kr_equity"
        if provider == "kis" and exchange in KRX_EXCHANGES
        else "us_equity"
    )
    metadata: dict[str, object] | None = None
    if settings is not None:
        identity = build_position_identity(
            settings=settings,
            provider=provider,
            exchange=exchange,
            broker_account_id=broker_account_id,
        )
        metadata = {
            "exchange": exchange,
            "execution_scope": execution_scope_payload(
                identity=identity,
                venue=exchange,
                asset_class=asset_class,
            ),
        }
    return BrokerOrder(
        broker_name=provider,
        mode=mode,
        idempotency_key=key,
        broker_order_id=f"broker-{key}",
        symbol=symbol,
        side=side,
        status=status,
        quantity=1,
        venue=exchange,
        asset_class=asset_class,
        last_broker_sync_at=observed_at,
        submitted_at=observed_at,
        request_metadata=metadata,
    )


def _fill(
    order: BrokerOrder,
    *,
    key: str,
    quantity: int,
    filled_at: datetime,
    price: str = "100.0000",
    settlement_currency: str | None = None,
    transaction_cost: str | None = None,
) -> ExecutionFill:
    return ExecutionFill(
        broker_order_pk=order.id,
        broker_fill_id=key,
        symbol=order.symbol,
        side=order.side,
        quantity=quantity,
        price=Decimal(price),
        settlement_currency=settlement_currency,
        transaction_cost=(
            Decimal(transaction_cost) if transaction_cost is not None else None
        ),
        transaction_cost_coverage=(
            "complete" if transaction_cost is not None else None
        ),
        transaction_cost_source=(
            "test_fixture" if transaction_cost is not None else None
        ),
        filled_at=filled_at,
    )


def _position(
    settings: Settings,
    *,
    symbol: str,
    exchange: str,
    provider: str = "kis",
    broker_account_id: str | None = None,
) -> ExecutionPosition:
    identity = build_position_identity(
        settings=settings,
        provider=provider,
        exchange=exchange,
        broker_account_id=broker_account_id,
    )
    return ExecutionPosition(
        mode=settings.runtime_mode,
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        symbol=symbol,
        quantity=1,
        average_price=Decimal("100.0000"),
        updated_from="test",
    )


class _StubAlpacaHttpClient:
    def __init__(self, responses: list[AlpacaHttpResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def request_json(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_runtime_alpaca_scope_uses_authenticated_account_despite_operator_label() -> None:
    settings = Settings(
        runtime_mode="paper",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
        execution_position_account_scope="operator-display-label",
        account_drawdown_recovery_scope_secret=(
            "runtime-account-scope-secret-at-least-32-bytes"
        ),
    )
    account_id = "authenticated-alpaca-account-a"
    http_client = _StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload={"id": account_id},
            )
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=http_client)

    scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="alpaca",
        broker_adapter=adapter,
    )
    authenticated = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id=account_id,
    )
    label_only = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
    )

    assert scope.available is True
    assert scope.source == "authenticated_broker_account"
    assert scope.account_scope == authenticated.account_scope
    assert scope.account_scope != label_only.account_scope
    assert len(http_client.calls) == 1


def test_runtime_freshness_and_autonomous_fill_cache_use_exact_active_scope() -> None:
    settings = _settings().model_copy(
        update={"stale_broker_order_reconciliation_threshold_seconds": 60}
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    stale_at = now - timedelta(seconds=61)

    active_filled_buy = _canonical_order(
        settings,
        key="freshness-active-filled-buy",
        symbol="ACTIVE-EVIDENCE",
        side="buy",
        status="filled",
        observed_at=stale_at,
    )
    foreign_filled_sell = _canonical_order(
        foreign_settings,
        key="freshness-foreign-filled-sell",
        symbol="ACTIVE-EVIDENCE",
        side="sell",
        status="filled",
        observed_at=stale_at,
    )
    foreign_filled_buy = _canonical_order(
        foreign_settings,
        key="freshness-foreign-filled-buy",
        symbol="FOREIGN-EVIDENCE",
        side="buy",
        status="filled",
        observed_at=stale_at,
    )
    legacy_filled_buy = _canonical_order(
        None,
        key="freshness-legacy-filled-buy",
        symbol="LEGACY-EVIDENCE",
        side="buy",
        status="filled",
        observed_at=stale_at,
    )
    active_stale = _canonical_order(
        settings,
        key="freshness-active-stale",
        symbol="ACTIVE-STALE",
        side="buy",
        status="open",
        observed_at=stale_at,
    )
    foreign_stale = _canonical_order(
        foreign_settings,
        key="freshness-foreign-stale",
        symbol="FOREIGN-STALE",
        side="buy",
        status="open",
        observed_at=stale_at,
    )
    legacy_stale = _canonical_order(
        None,
        key="freshness-legacy-stale",
        symbol="LEGACY-STALE",
        side="buy",
        status="open",
        observed_at=stale_at,
    )
    db.add_all(
        [
            active_filled_buy,
            foreign_filled_sell,
            foreign_filled_buy,
            legacy_filled_buy,
            active_stale,
            foreign_stale,
            legacy_stale,
        ]
    )
    db.flush()
    db.add_all(
        [
            _fill(
                active_filled_buy,
                key="freshness-active-fill",
                quantity=1,
                filled_at=now,
            ),
            _fill(
                foreign_filled_sell,
                key="freshness-foreign-sell-fill",
                quantity=1,
                filled_at=now,
            ),
            _fill(
                foreign_filled_buy,
                key="freshness-foreign-buy-fill",
                quantity=1,
                filled_at=now,
            ),
            _fill(
                legacy_filled_buy,
                key="freshness-legacy-fill",
                quantity=1,
                filled_at=now,
            ),
        ]
    )
    db.commit()

    position_truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )
    stale_orders = runtime_service.stale_nonterminal_order_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )

    assert position_truth["stale_symbols"] == ["ACTIVE-EVIDENCE"]
    assert position_truth["scope_status"] == "unavailable"
    assert position_truth["scope_reason"] == "indeterminate_execution_scope"
    assert position_truth["excluded_indeterminate_order_count"] == 1
    assert position_truth["broker_truth_reachable"] is False
    assert stale_orders["count"] == 1
    assert stale_orders["symbols"] == ["ACTIVE-STALE"]
    assert stale_orders["scope_status"] == "unavailable"
    assert stale_orders["scope_reason"] == "indeterminate_execution_scope"
    assert stale_orders["excluded_indeterminate_order_count"] == 1
    assert stale_orders["excluded_other_route_count"] == 1
    assert _load_filled_today_symbols(
        settings=settings,
        db=db,
        now=now,
    ) == ["ACTIVE-EVIDENCE"]

    alerts = runtime_service.runtime_actionable_alerts_payload(
        monitoring={},
        freshness={"broker_orders": {"stale_nonterminal": stale_orders}},
    )
    assert next(
        alert for alert in alerts["alerts"]
        if alert["kind"] == "stale_broker_orders_need_reconciliation"
    )["symbols"] == ["ACTIVE-STALE"]
    assert any(
        alert["kind"] == "broker_order_execution_scope_unavailable"
        for alert in alerts["alerts"]
    )
    assert "FOREIGN-STALE" not in str(alerts)
    assert "LEGACY-STALE" not in str(alerts)
    db.close()
    engine.dispose()


def test_position_truth_replays_fill_quantity_and_chronology_for_reentry() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 4, 0, tzinfo=UTC)
    first_buy = _canonical_order(
        settings,
        key="reentry-first-buy",
        symbol="REENTRY",
        side="buy",
        status="filled",
    )
    exit_sell = _canonical_order(
        settings,
        key="reentry-exit-sell",
        symbol="REENTRY",
        side="sell",
        status="filled",
    )
    second_buy = _canonical_order(
        settings,
        key="reentry-second-buy",
        symbol="REENTRY",
        side="buy",
        status="filled",
    )
    db.add_all([first_buy, exit_sell, second_buy])
    db.flush()
    db.add_all(
        [
            _fill(first_buy, key="reentry-fill-1", quantity=1, filled_at=now - timedelta(minutes=3)),
            _fill(exit_sell, key="reentry-fill-2", quantity=1, filled_at=now - timedelta(minutes=2)),
            _fill(second_buy, key="reentry-fill-3", quantity=1, filled_at=now - timedelta(minutes=1)),
        ]
    )
    db.commit()

    position_truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )

    assert position_truth["stale_symbols"] == ["REENTRY"]
    assert position_truth["evidence_symbol_count"] == 1
    assert position_truth["excluded_indeterminate_fill_count"] == 0
    db.close()
    engine.dispose()


def test_position_truth_surfaces_manual_review_position_monitoring_gap() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 15, 4, 0, tzinfo=UTC)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    db.add(
        ExecutionPosition(
            mode=settings.runtime_mode,
            provider=identity.provider,
            account_scope=identity.account_scope,
            exchange=identity.exchange,
            symbol="OLDCO",
            quantity=7,
            average_price=Decimal("12.50"),
            updated_from="reconciliation",
            last_reconciled_at=now,
            raw_payload={
                "_position_sync": {
                    "asset_class": "us_equity",
                    "exchange": "NASD",
                    "monitoring_eligible": False,
                    "manual_review_required": True,
                    "manual_review_reason": "unpriced_overseas_holding",
                }
            },
        )
    )
    db.commit()

    position_truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )
    alerts = runtime_service.runtime_actionable_alerts_payload(
        monitoring={},
        freshness={"position_truth": position_truth},
    )

    assert position_truth["manual_review_count"] == 1
    assert position_truth["manual_review_symbols"] == ["OLDCO"]
    assert position_truth["manual_review_reasons"] == {
        "OLDCO": "unpriced_overseas_holding"
    }
    assert alerts["status"] == "needs_attention"
    assert next(
        alert
        for alert in alerts["alerts"]
        if alert["kind"] == "position_monitoring_manual_review_required"
    ) == {
        "kind": "position_monitoring_manual_review_required",
        "severity": "critical",
        "count": 1,
        "symbols": ["OLDCO"],
        "message": (
            "held positions cannot be protected automatically until their "
            "quote and execution route are reviewed"
        ),
    }
    db.close()
    engine.dispose()


def test_position_truth_keeps_incomplete_filled_order_detail_as_exposure_evidence() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 4, 0, tzinfo=UTC)
    incomplete_buy = _canonical_order(
        settings,
        key="incomplete-buy",
        symbol="INCOMPLETE",
        side="buy",
        status="filled",
    )
    incomplete_buy.quantity = 2
    recorded_sell = _canonical_order(
        settings,
        key="incomplete-sell",
        symbol="INCOMPLETE",
        side="sell",
        status="filled",
    )
    db.add_all([incomplete_buy, recorded_sell])
    db.flush()
    db.add_all(
        [
            _fill(incomplete_buy, key="incomplete-buy-fill", quantity=1, filled_at=now - timedelta(minutes=2)),
            _fill(recorded_sell, key="incomplete-sell-fill", quantity=1, filled_at=now - timedelta(minutes=1)),
        ]
    )
    db.commit()

    position_truth = runtime_service.position_truth_freshness_payload(
        settings=settings,
        db=db,
        now=now,
    )

    assert position_truth["stale_symbols"] == ["INCOMPLETE"]
    assert position_truth["scope_status"] == "unavailable"
    assert position_truth["excluded_indeterminate_fill_count"] == 1
    db.close()
    engine.dispose()


def test_runtime_operational_sections_exclude_foreign_account_scope() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    now = datetime.now(UTC)
    active_item = _bound_plan_item(
        settings,
        symbol="ACTIVE-SCOPE",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
    )
    active_item.decision_agent_name = "short-term-decider"
    foreign_item = _bound_plan_item(
        foreign_settings,
        symbol="FOREIGN-SCOPE",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
    )
    foreign_item.decision_agent_name = "short-term-decider"
    active_plan = ExecutionPlan(
        external_plan_id="active-runtime-scope-plan",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
        items=[active_item],
    )
    foreign_plan = ExecutionPlan(
        external_plan_id="foreign-runtime-scope-plan",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
        items=[foreign_item],
    )
    db.add_all([active_plan, foreign_plan])
    db.flush()

    active_open = _canonical_order(
        settings,
        key="active-runtime-open",
        symbol="ACTIVE-SCOPE",
        side="buy",
        status="open",
        observed_at=now - timedelta(seconds=10),
    )
    foreign_open = _canonical_order(
        foreign_settings,
        key="foreign-runtime-open",
        symbol="FOREIGN-SCOPE",
        side="buy",
        status="open",
        observed_at=now,
    )
    active_missing_fill = _canonical_order(
        settings,
        key="active-runtime-missing-fill",
        symbol="ACTIVE-MISSING",
        side="buy",
        status="filled",
        observed_at=now - timedelta(seconds=20),
    )
    foreign_missing_fill = _canonical_order(
        foreign_settings,
        key="foreign-runtime-missing-fill",
        symbol="FOREIGN-MISSING",
        side="buy",
        status="filled",
        observed_at=now + timedelta(seconds=20),
    )
    db.add_all([active_open, foreign_open, active_missing_fill, foreign_missing_fill])
    db.flush()
    db.add_all(
        [
            ExecutionJob(
                execution_plan_item_id=active_item.id,
                job_type="next_open_buy",
                status="queued",
            ),
            ExecutionJob(
                execution_plan_item_id=foreign_item.id,
                job_type="next_open_buy",
                status="queued",
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=active_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=now,
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=foreign_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=now,
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=active_item.id,
                event_type="watch_buy_late_entry_gate",
                source="test",
                occurred_at=now - timedelta(seconds=2),
                summary={"mode": "shadow"},
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=foreign_item.id,
                event_type="watch_buy_late_entry_gate",
                source="test",
                occurred_at=now - timedelta(seconds=1),
                summary={"mode": "block"},
            ),
            BrokerOrderEvent(
                broker_order_pk=active_open.id,
                event_type="sync",
                event_source="reconciliation",
                observed_at=now - timedelta(seconds=9),
            ),
            BrokerOrderEvent(
                broker_order_pk=foreign_open.id,
                event_type="sync",
                event_source="reconciliation",
                observed_at=now + timedelta(seconds=9),
            ),
        ]
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
    db.add_all(
        [
            MonitoringCheckpoint(
                worker_name="active-monitor",
                mode=settings.runtime_mode,
                provider="kis",
                account_scope=active_identity.account_scope,
                status="running",
                last_heartbeat_at=now - timedelta(seconds=5),
                details={
                    "execution_scope": {
                        "policy_version": 1,
                        "provider": "kis",
                        "account_scope": active_identity.account_scope,
                    }
                },
            ),
            MonitoringCheckpoint(
                worker_name="foreign-monitor",
                mode=settings.runtime_mode,
                provider="kis",
                account_scope=foreign_identity.account_scope,
                status="degraded",
                last_heartbeat_at=now + timedelta(seconds=5),
                details={
                    "execution_scope": {
                        "policy_version": 1,
                        "provider": "kis",
                        "account_scope": foreign_identity.account_scope,
                    }
                },
            ),
            MonitoringCheckpoint(
                worker_name="legacy-monitor",
                mode=settings.runtime_mode,
                status="degraded",
                last_heartbeat_at=now + timedelta(seconds=10),
                details={},
            ),
        ]
    )
    db.commit()

    runtime_scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="kis",
    )
    counts = runtime_service.runtime_counts(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )
    vetoes = runtime_service.runtime_watch_buy_veto_summary(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )
    late_entry = runtime_service.runtime_late_entry_summary(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )
    monitoring = runtime_service.monitoring_payload(
        settings=settings,
        db=db,
        provider="kis",
        runtime_scope=runtime_scope,
    )
    freshness = runtime_service.runtime_freshness_payload(
        settings=settings,
        db=db,
        monitoring=monitoring,
        now=now,
        provider="kis",
        runtime_scope=runtime_scope,
    )

    assert counts == {"open_orders": 1, "active_execution_jobs": 1}
    assert vetoes == {"today_count": 1, "today_symbols": ["ACTIVE-SCOPE"]}
    assert late_entry["event_count"] == 1
    assert late_entry["shadow_count"] == 1
    assert late_entry["block_count"] == 0
    assert late_entry["symbols"] == ["ACTIVE-SCOPE"]
    assert monitoring["segments"]["short_term_buy_watch_monitoring"]["symbols"] == [
        "ACTIVE-SCOPE"
    ]
    assert [worker["name"] for worker in monitoring["workers"]] == ["active-monitor"]
    assert monitoring["excluded_checkpoint_count"] == 2
    assert monitoring["last_cycle"]["execution_scope"] == {
        "policy_version": 1,
        "provider": "kis",
    }
    assert freshness["broker_orders"]["latest_broker_sync_at"] == (
        now - timedelta(seconds=10)
    ).isoformat()
    assert freshness["broker_orders"]["latest_reconciliation_event_at"] == (
        now - timedelta(seconds=9)
    ).replace(tzinfo=None).isoformat()
    assert freshness["broker_orders"]["fill_quality"][
        "missing_filled_order_fill_unreviewed_symbols"
    ] == ["ACTIVE-MISSING"]
    assert "FOREIGN" not in str({"counts": counts, "vetoes": vetoes, "monitoring": monitoring, "freshness": freshness})
    db.close()
    engine.dispose()


def test_fast_autonomous_status_uses_scoped_database_counts_and_veto_truth() -> None:
    settings = _settings().model_copy(update={"autonomous_runtime_enabled": True})
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    now = datetime.now(UTC)
    active_item = _bound_plan_item(
        settings,
        symbol="ACTIVE-FAST",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
    )
    foreign_item = _bound_plan_item(
        foreign_settings,
        symbol="FOREIGN-FAST",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
    )
    db.add_all(
        [
            ExecutionPlan(
                external_plan_id="active-fast-runtime",
                source="test",
                mode=settings.runtime_mode,
                status="accepted",
                items=[active_item],
            ),
            ExecutionPlan(
                external_plan_id="foreign-fast-runtime",
                source="test",
                mode=settings.runtime_mode,
                status="accepted",
                items=[foreign_item],
            ),
        ]
    )
    db.flush()
    db.add_all(
        [
            ExecutionJob(
                execution_plan_item_id=active_item.id,
                job_type="next_open_buy",
                status="queued",
            ),
            ExecutionJob(
                execution_plan_item_id=foreign_item.id,
                job_type="next_open_buy",
                status="queued",
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=active_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=now,
            ),
            ExecutionPlanItemAudit(
                execution_plan_item_id=foreign_item.id,
                event_type=runtime_service.LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                source="test",
                occurred_at=now,
            ),
            _canonical_order(
                settings,
                key="active-fast-open",
                symbol="ACTIVE-FAST",
                side="buy",
                status="open",
                observed_at=now,
            ),
            _canonical_order(
                foreign_settings,
                key="foreign-fast-open",
                symbol="FOREIGN-FAST",
                side="buy",
                status="open",
                observed_at=now,
            ),
        ]
    )
    db.commit()
    runtime_scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="kis",
    )
    exact_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": runtime_scope.account_scope,
    }
    autonomous = {
        "enabled": True,
        "execution_scope": exact_scope,
        "monitoring": {
            "status": "running",
            "last_cycle": {
                "execution_scope": exact_scope,
                "monitored_symbols": ["ACTIVE-FAST"],
            },
            "runtime_snapshot": {
                "execution_scope": exact_scope,
                "symbols": ["ACTIVE-FAST"],
                "filled_today_symbols": [],
            },
        },
    }

    payload = runtime_service.build_runtime_status(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=now),
        db=db,
        autonomous_runtime=autonomous,
        include_expensive_sections=False,
        runtime_scope=runtime_scope,
    )

    assert payload["counts"] == {"open_orders": 1, "active_execution_jobs": 1}
    assert payload["watch_buy_vetoes"] == {
        "today_count": 1,
        "today_symbols": ["ACTIVE-FAST"],
    }
    assert "account_scope" not in payload["liveness"]["autonomous"]["execution_scope"]
    assert "account_scope" not in payload["liveness"]["monitoring"]["last_cycle"][
        "execution_scope"
    ]
    assert runtime_scope.account_scope not in repr(payload)
    db.close()
    engine.dispose()


def test_fast_autonomous_status_rejects_unscoped_or_foreign_cached_truth() -> None:
    settings = _settings().model_copy(update={"autonomous_runtime_enabled": True})
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    now = datetime.now(UTC)
    runtime_scope = runtime_service._resolve_runtime_execution_scope(
        settings=settings,
        provider="kis",
    )
    foreign_scope = runtime_service._resolve_runtime_execution_scope(
        settings=foreign_settings,
        provider="kis",
    )

    exact_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": runtime_scope.account_scope,
    }
    foreign_exact_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": foreign_scope.account_scope,
    }
    for root_scope, cached_scope in (
        (None, None),
        (foreign_exact_scope, foreign_exact_scope),
        (exact_scope, foreign_exact_scope),
    ):
        last_cycle: dict[str, object] = {
            "monitored_symbols": ["FOREIGN-CACHED"],
        }
        runtime_snapshot: dict[str, object] = {
            "symbols": ["FOREIGN-CACHED"],
            "filled_today_symbols": ["FOREIGN-CACHED"],
        }
        autonomous: dict[str, object] = {
            "enabled": True,
            "monitoring": {
                "status": "running",
                "last_cycle": last_cycle,
                "runtime_snapshot": runtime_snapshot,
            },
        }
        if root_scope is not None:
            autonomous["execution_scope"] = root_scope
        if cached_scope is not None:
            last_cycle["execution_scope"] = cached_scope
            runtime_snapshot["execution_scope"] = cached_scope

        payload = runtime_service.build_runtime_status(
            settings=settings,
            runtime=runtime_service.RuntimeMetadata(started_at=now),
            db=db,
            autonomous_runtime=autonomous,
            include_expensive_sections=False,
            runtime_scope=runtime_scope,
        )

        assert payload["ready"] is False
        assert payload["liveness"]["monitoring"]["status"] == "degraded"
        assert payload["symbols"]["monitored"] == []
        assert payload["symbols"]["monitoring_cycle_batch"] == []
        assert any(
            "autonomous" in reason and "execution scope" in reason
            for reason in payload["readiness"]["reasons"]
        )
        assert foreign_scope.account_scope not in repr(payload)

    db.close()
    engine.dispose()


def test_runtime_daily_symbols_and_counts_use_only_active_canonical_route() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    today_start, tomorrow_start = runtime_service._current_krx_day_utc_bounds()
    observed_at = today_start + timedelta(hours=1)

    plan = ExecutionPlan(
        external_plan_id="runtime-account-scope-summary",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
    )
    active_scheduled = _bound_plan_item(settings, symbol="ACTIVE-SCHEDULE")
    active_us_scheduled = _bound_plan_item(
        settings,
        symbol="ACTIVE-US-SCHEDULE",
        exchange="NASD",
    )
    foreign_same_scheduled = _bound_plan_item(
        foreign_settings,
        symbol="ACTIVE-SCHEDULE",
    )
    foreign_scheduled = _bound_plan_item(
        foreign_settings,
        symbol="FOREIGN-SCHEDULE",
    )
    legacy_scheduled = _legacy_plan_item(symbol="LEGACY-SCHEDULE")
    unsupported_scheduled = _bound_plan_item(
        settings,
        symbol="UNSUPPORTED-SCHEDULE",
        exchange="TSE",
    )
    plan.items.extend(
        [
            active_scheduled,
            active_us_scheduled,
            foreign_same_scheduled,
            foreign_scheduled,
            legacy_scheduled,
            unsupported_scheduled,
        ]
    )
    for item in plan.items:
        item.jobs.append(
            ExecutionJob(
                job_type="submit_order",
                status="scheduled",
                scheduled_for=observed_at,
            )
        )
    db.add(plan)

    active_buy = _canonical_order(
        settings,
        key="active-buy",
        symbol="SHARED-FILL",
        side="buy",
        status="filled",
    )
    foreign_same_buy = _canonical_order(
        foreign_settings,
        key="foreign-same-buy",
        symbol="SHARED-FILL",
        side="buy",
        status="filled",
    )
    foreign_sell = _canonical_order(
        foreign_settings,
        key="foreign-sell",
        symbol="FOREIGN-FILL",
        side="sell",
        status="filled",
    )
    legacy_buy = _canonical_order(
        None,
        key="legacy-buy",
        symbol="LEGACY-FILL",
        side="buy",
        status="filled",
    )
    unsupported_buy = _canonical_order(
        settings,
        key="unsupported-buy",
        symbol="UNSUPPORTED-FILL",
        side="buy",
        status="filled",
        exchange="TSE",
    )
    active_us_sell = _canonical_order(
        settings,
        key="active-us-sell",
        symbol="ACTIVE-US-FILL",
        side="sell",
        status="filled",
        exchange="NYSE",
    )
    active_failed = _canonical_order(
        settings,
        key="active-failed",
        symbol="ACTIVE-FAIL",
        side="buy",
        status="rejected",
        observed_at=observed_at,
    )
    foreign_same_failed = _canonical_order(
        foreign_settings,
        key="foreign-same-failed",
        symbol="ACTIVE-FAIL",
        side="buy",
        status="rejected",
        observed_at=observed_at,
    )
    foreign_failed = _canonical_order(
        foreign_settings,
        key="foreign-failed",
        symbol="FOREIGN-FAIL",
        side="buy",
        status="rejected",
        observed_at=observed_at,
    )
    legacy_failed = _canonical_order(
        None,
        key="legacy-failed",
        symbol="LEGACY-FAIL",
        side="buy",
        status="rejected",
        observed_at=observed_at,
    )
    db.add_all(
        [
            active_buy,
            foreign_same_buy,
            foreign_sell,
            legacy_buy,
            unsupported_buy,
            active_us_sell,
            active_failed,
            foreign_same_failed,
            foreign_failed,
            legacy_failed,
        ]
    )
    db.flush()
    db.add_all(
        [
            _fill(active_buy, key="active-buy-fill", quantity=2, filled_at=observed_at),
            _fill(
                foreign_same_buy,
                key="foreign-same-buy-fill",
                quantity=7,
                filled_at=observed_at,
            ),
            _fill(
                foreign_sell,
                key="foreign-sell-fill",
                quantity=11,
                filled_at=observed_at,
            ),
            _fill(legacy_buy, key="legacy-buy-fill", quantity=13, filled_at=observed_at),
            _fill(
                unsupported_buy,
                key="unsupported-buy-fill",
                quantity=17,
                filled_at=observed_at,
            ),
            _fill(
                active_us_sell,
                key="active-us-sell-fill",
                quantity=3,
                filled_at=observed_at,
            ),
        ]
    )
    db.commit()

    summary = runtime_service.runtime_symbol_summary(settings=settings, db=db)
    filled_summary = runtime_service.runtime_filled_today_summary(
        settings=settings,
        db=db,
        today_start_utc=today_start,
        tomorrow_start_utc=tomorrow_start,
    )

    assert summary["scheduled_today"] == ["ACTIVE-SCHEDULE", "ACTIVE-US-SCHEDULE"]
    assert summary["filled_today"] == ["ACTIVE-US-FILL", "SHARED-FILL"]
    assert summary["filled_today_by_side"] == {
        "buy": ["SHARED-FILL"],
        "sell": ["ACTIVE-US-FILL"],
    }
    assert summary["filled_today_counts"] == {
        "all": {"symbols": 2, "fills": 2, "quantity": 5},
        "buy": {"symbols": 1, "fills": 1, "quantity": 2},
        "sell": {"symbols": 1, "fills": 1, "quantity": 3},
    }
    assert summary["failed_today"] == ["ACTIVE-FAIL"]
    assert filled_summary == {
        "by_side": {
            "buy": ["SHARED-FILL"],
            "sell": ["ACTIVE-US-FILL"],
        },
        "counts": summary["filled_today_counts"],
    }
    assert runtime_service.runtime_filled_today_symbols(
        settings=settings,
        db=db,
    ) == ["ACTIVE-US-FILL", "SHARED-FILL"]

    db.close()
    engine.dispose()


def test_runtime_monitored_symbols_require_active_account_and_exact_exchange() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    plan = ExecutionPlan(
        external_plan_id="runtime-account-scope-monitoring",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
    )
    plan.items.extend(
        [
            _bound_plan_item(
                settings,
                symbol="BUY-A",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
            _bound_plan_item(
                settings,
                symbol="BUY-US",
                intent="watch_buy",
                status="watch_monitoring_active",
                exchange="NASD",
            ),
            _bound_plan_item(
                foreign_settings,
                symbol="BUY-A",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
            _bound_plan_item(
                foreign_settings,
                symbol="BUY-FOREIGN",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
            _legacy_plan_item(
                symbol="BUY-LEGACY",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
            _bound_plan_item(
                settings,
                symbol="BUY-UNSUPPORTED",
                intent="watch_buy",
                status="watch_monitoring_active",
                exchange="TSE",
            ),
            _bound_plan_item(
                settings,
                symbol="SELL-A",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
            ),
            _bound_plan_item(
                foreign_settings,
                symbol="SELL-A",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
            ),
            _bound_plan_item(
                foreign_settings,
                symbol="SELL-FOREIGN",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
            ),
            _legacy_plan_item(
                symbol="SELL-LEGACY",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
            ),
            _bound_plan_item(
                settings,
                symbol="SELL-EXCHANGE-COLLISION",
                side="sell",
                intent="target_sell",
                status="target_sell_monitoring",
                exchange="NASD",
            ),
            _bound_plan_item(
                settings,
                symbol="SELL-US",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
                exchange="NYSE",
            ),
        ]
    )
    db.add(plan)
    db.add_all(
        [
            _position(settings, symbol="SELL-A", exchange="KRX"),
            _position(settings, symbol="SELL-FOREIGN", exchange="KRX"),
            _position(settings, symbol="SELL-LEGACY", exchange="KRX"),
            _position(settings, symbol="SELL-EXCHANGE-COLLISION", exchange="KRX"),
            _position(settings, symbol="SELL-US", exchange="NYSE"),
        ]
    )
    db.commit()

    assert runtime_service.runtime_monitored_symbols(
        settings=settings,
        db=db,
        today_start_utc=datetime(2026, 9, 10, tzinfo=UTC),
    ) == ["BUY-A", "BUY-US", "SELL-A", "SELL-US"]

    db.close()
    engine.dispose()


def test_non_kis_runtime_plan_filter_requires_current_account_scope() -> None:
    settings = Settings(
        runtime_mode="paper",
        execution_position_account_scope="active-upbit-account",
        account_drawdown_recovery_scope_secret="runtime-account-scope-secret-32-bytes",
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-upbit-account"}
    )
    active = _bound_plan_item(
        settings,
        symbol="KRW-BTC",
        provider="upbit",
        exchange="UPBIT",
        intent="watch_buy",
        status="watch_monitoring_active",
    )
    foreign = _bound_plan_item(
        foreign_settings,
        symbol="KRW-ETH",
        provider="upbit",
        exchange="UPBIT",
        intent="watch_buy",
        status="watch_monitoring_active",
    )
    missing = ExecutionPlanItem(
        symbol="KRW-XRP",
        side="buy",
        intent="watch_buy",
        status="watch_monitoring_active",
        provider="upbit",
        venue="upbit",
        risk_context={"exchange": "UPBIT"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    assert runtime_service._active_runtime_plan_item_exchange(
        settings=settings,
        item=active,
        provider="upbit",
    ) == "UPBIT"
    assert runtime_service._active_runtime_plan_item_exchange(
        settings=settings,
        item=foreign,
        provider="upbit",
    ) is None
    assert runtime_service._active_runtime_plan_item_exchange(
        settings=settings,
        item=missing,
        provider="upbit",
    ) is None


def test_runtime_pnl_and_monthly_recommendation_exclude_foreign_account_routes() -> None:
    settings = _settings().model_copy(
        update={"monthly_target_krw": Decimal("100")}
    )
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)

    active_buy = _canonical_order(
        settings,
        key="pnl-active-buy",
        symbol="ACTIVE-PNL",
        side="buy",
        status="filled",
    )
    active_sell = _canonical_order(
        settings,
        key="pnl-active-sell",
        symbol="ACTIVE-PNL",
        side="sell",
        status="filled",
    )
    foreign_buy = _canonical_order(
        foreign_settings,
        key="pnl-foreign-buy",
        symbol="FOREIGN-PNL",
        side="buy",
        status="filled",
    )
    foreign_sell = _canonical_order(
        foreign_settings,
        key="pnl-foreign-sell",
        symbol="FOREIGN-PNL",
        side="sell",
        status="filled",
    )
    foreign_same_symbol_buy = _canonical_order(
        foreign_settings,
        key="pnl-foreign-same-buy",
        symbol="ACTIVE-PNL",
        side="buy",
        status="filled",
    )
    foreign_same_symbol_sell = _canonical_order(
        foreign_settings,
        key="pnl-foreign-same-sell",
        symbol="ACTIVE-PNL",
        side="sell",
        status="filled",
    )
    db.add_all(
        [
            active_buy,
            active_sell,
            foreign_buy,
            foreign_sell,
            foreign_same_symbol_buy,
            foreign_same_symbol_sell,
        ]
    )
    db.flush()
    db.add_all(
        [
            _fill(
                active_buy,
                key="pnl-active-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=7),
            ),
            _fill(
                active_sell,
                key="pnl-active-sell-fill",
                quantity=1,
                price="120",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=6),
            ),
            _fill(
                foreign_buy,
                key="pnl-foreign-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=5),
            ),
            _fill(
                foreign_sell,
                key="pnl-foreign-sell-fill",
                quantity=1,
                price="1000",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=4),
            ),
            _fill(
                foreign_same_symbol_buy,
                key="pnl-foreign-same-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=3),
            ),
            _fill(
                foreign_same_symbol_sell,
                key="pnl-foreign-same-sell-fill",
                quantity=1,
                price="1000",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=now - timedelta(hours=2),
            ),
        ]
    )
    db.commit()

    daily = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)
    weekly = runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now)
    monthly = runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now)

    assert daily["daily_gross_realized_pnl_krw"] == "20.0000"
    assert daily["daily_net_realized_pnl_krw"] == "20.0000"
    assert daily["daily_buy_notional"] == "100.0000"
    assert daily["daily_sell_notional"] == "120.0000"
    assert daily["matched_sell_quantity"] == 1
    assert daily["closed_symbols"] == ["ACTIVE-PNL"]
    assert weekly["weekly_gross_realized_pnl_krw"] == "20.0000"
    assert weekly["weekly_buy_notional"] == "100.0000"
    assert weekly["weekly_sell_notional"] == "120.0000"
    assert weekly["closed_symbols"] == ["ACTIVE-PNL"]
    assert monthly["monthly_gross_realized_pnl_krw"] == "20.0000"
    assert monthly["monthly_net_realized_pnl_krw"] == "20.0000"
    assert monthly["monthly_buy_notional"] == "100.0000"
    assert monthly["monthly_sell_notional"] == "120.0000"
    assert monthly["target_progress_pct"] == "20.0000"
    assert monthly["mode_recommendation"] == "aggressive"

    db.close()
    engine.dispose()


def test_runtime_pnl_includes_supported_kis_nxt_and_sor_routes() -> None:
    settings = _settings().model_copy(update={"monthly_target_krw": Decimal("100")})
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)

    fills: list[ExecutionFill] = []
    for offset, exchange in enumerate(("NXT", "SOR")):
        buy = _canonical_order(
            settings,
            key=f"pnl-{exchange.lower()}-buy",
            symbol=f"{exchange}-PNL",
            side="buy",
            status="filled",
            exchange=exchange,
        )
        sell = _canonical_order(
            settings,
            key=f"pnl-{exchange.lower()}-sell",
            symbol=f"{exchange}-PNL",
            side="sell",
            status="filled",
            exchange=exchange,
        )
        db.add_all((buy, sell))
        db.flush()
        fills.extend(
            (
                _fill(
                    buy,
                    key=f"pnl-{exchange.lower()}-buy-fill",
                    quantity=1,
                    price="100",
                    settlement_currency="KRW",
                    transaction_cost="0",
                    filled_at=now - timedelta(hours=8 - offset * 2),
                ),
                _fill(
                    sell,
                    key=f"pnl-{exchange.lower()}-sell-fill",
                    quantity=1,
                    price="120",
                    settlement_currency="KRW",
                    transaction_cost="0",
                    filled_at=now - timedelta(hours=7 - offset * 2),
                ),
            )
        )
    db.add_all(fills)
    db.commit()

    daily = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)
    weekly = runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now)
    monthly = runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now)

    assert daily["daily_gross_realized_pnl_krw"] == "40.0000"
    assert daily["daily_buy_notional"] == "200.0000"
    assert daily["daily_sell_notional"] == "240.0000"
    assert daily["closed_symbols"] == ["NXT-PNL", "SOR-PNL"]
    assert weekly["weekly_gross_realized_pnl_krw"] == "40.0000"
    assert weekly["closed_symbols"] == ["NXT-PNL", "SOR-PNL"]
    assert monthly["monthly_gross_realized_pnl_krw"] == "40.0000"
    assert monthly["monthly_net_realized_pnl_krw"] == "40.0000"
    assert monthly["target_progress_pct"] == "40.0000"

    db.close()
    engine.dispose()


def test_runtime_pnl_does_not_report_zero_when_fill_scope_is_ambiguous() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    legacy_sell = _canonical_order(
        None,
        key="pnl-ambiguous-sell",
        symbol="AMBIGUOUS-PNL",
        side="sell",
        status="filled",
    )
    db.add(legacy_sell)
    db.flush()
    db.add(
        _fill(
            legacy_sell,
            key="pnl-ambiguous-sell-fill",
            quantity=1,
            price="100",
            settlement_currency="KRW",
            transaction_cost="0",
            filled_at=now - timedelta(hours=1),
        )
    )
    db.commit()

    daily = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)
    weekly = runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now)
    monthly = runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now)

    for payload in (daily, weekly, monthly):
        assert payload["calculation_status"] == "not_computed"
        assert payload["calculation_reason"] == "indeterminate_execution_fill_scope"
        assert payload["gross_realized_pnl_krw"] is None
        assert payload["net_realized_pnl_krw"] is None
    assert monthly["target_progress_pct"] is None
    assert monthly["mode_recommendation"] == "insufficient_net_pnl_data"

    db.close()
    engine.dispose()


def test_runtime_pnl_uses_clean_generation_after_exact_verified_flat() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    old_at = now - timedelta(hours=5)
    legacy_boundary_at = old_at + timedelta(seconds=1)
    exact_boundary_at = old_at + timedelta(seconds=2)
    orders = [
        _canonical_order(
            settings,
            key=key,
            symbol="GENERATION-PNL",
            side=side,
            status="filled",
        )
        for key, side in (
            ("generation-old-buy-one", "buy"),
            ("generation-old-buy-two", "buy"),
            ("generation-new-buy", "buy"),
            ("generation-new-sell", "sell"),
        )
    ]
    db.add_all(orders)
    db.flush()
    execution_scope = orders[0].request_metadata["execution_scope"]
    db.add_all(
        [
            _fill(
                orders[0],
                key="generation-old-buy-fill-one",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=old_at,
            ),
            _fill(
                orders[1],
                key="generation-old-buy-fill-two",
                quantity=2,
                price="200",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=old_at,
            ),
            _fill(
                orders[2],
                key="generation-new-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=exact_boundary_at + timedelta(seconds=1),
            ),
            _fill(
                orders[3],
                key="generation-new-sell-fill",
                quantity=1,
                price="120",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=exact_boundary_at + timedelta(seconds=2),
            ),
            FifoInventoryIsolation(
                mode=settings.runtime_mode,
                provider=None,
                account_scope=None,
                venue=None,
                asset_class=None,
                exchange=None,
                symbol="GENERATION-PNL",
                residual_quantity=Decimal("3"),
                last_local_fill_at=old_at,
                broker_position_id="legacy-generation-flat",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=legacy_boundary_at,
                isolated_through_at=legacy_boundary_at,
                evidence_source="legacy_broker_flat_reconciliation",
                actor="test",
                reason="legacy unscoped flat proof",
            ),
            FifoInventoryIsolation(
                mode=settings.runtime_mode,
                provider=execution_scope["provider"],
                account_scope=execution_scope["account_scope"],
                venue=execution_scope["venue"],
                asset_class=execution_scope["asset_class"],
                exchange=execution_scope["exchange"],
                symbol="GENERATION-PNL",
                residual_quantity=Decimal("3"),
                last_local_fill_at=old_at,
                broker_position_id="exact-generation-flat",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=exact_boundary_at,
                isolated_through_at=exact_boundary_at,
                evidence_source="broker_flat_reconciliation",
                actor="test",
                reason="exact-route broker-authenticated flat proof",
            ),
        ]
    )
    db.commit()

    daily = runtime_service.daily_pnl_payload(settings=settings, db=db, now=now)
    weekly = runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now)
    monthly = runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now)

    for payload in (daily, weekly, monthly):
        assert payload["calculation_status"] == "computed"
        assert Decimal(str(payload["gross_realized_pnl_krw"])) == Decimal("20.0000")
        assert payload["matched_sell_quantity"] == 1
    assert daily["closed_symbols"] == ["GENERATION-PNL"]
    assert weekly["closed_symbols"] == ["GENERATION-PNL"]

    db.close()
    engine.dispose()


def test_runtime_pnl_future_flat_does_not_retroactively_resolve_ambiguous_sale() -> None:
    settings = _settings()
    engine, _factory, db = _session(settings)
    now = datetime(2026, 9, 10, 3, 0, tzinfo=UTC)
    ambiguous_at = now - timedelta(hours=5)
    sold_at = ambiguous_at + timedelta(seconds=1)
    boundary_at = sold_at + timedelta(seconds=1)
    orders = [
        _canonical_order(
            settings,
            key=key,
            symbol="HISTORICAL-AMBIGUITY",
            side=side,
            status="filled",
        )
        for key, side in (
            ("historical-ambiguous-cheap-buy", "buy"),
            ("historical-ambiguous-expensive-buy", "buy"),
            ("historical-ambiguous-sell", "sell"),
        )
    ]
    db.add_all(orders)
    db.flush()
    cheap_buy = _fill(
        orders[0],
        key="historical-ambiguous-cheap-buy-fill",
        quantity=1,
        price="100",
        settlement_currency="KRW",
        transaction_cost="0",
        filled_at=ambiguous_at,
    )
    expensive_buy = _fill(
        orders[1],
        key="historical-ambiguous-expensive-buy-fill",
        quantity=1,
        price="200",
        settlement_currency="KRW",
        transaction_cost="0",
        filled_at=ambiguous_at,
    )
    sell = _fill(
        orders[2],
        key="historical-ambiguous-sell-fill",
        quantity=1,
        price="150",
        settlement_currency="KRW",
        transaction_cost="0",
        filled_at=sold_at,
    )
    cheap_buy.id = "aaa-historical-ambiguous-cheap-buy"
    expensive_buy.id = "zzz-historical-ambiguous-expensive-buy"
    sell.id = "historical-ambiguous-sell"
    execution_scope = orders[0].request_metadata["execution_scope"]
    db.add_all(
        [
            cheap_buy,
            expensive_buy,
            sell,
            FifoInventoryIsolation(
                mode=settings.runtime_mode,
                provider=execution_scope["provider"],
                account_scope=execution_scope["account_scope"],
                venue=execution_scope["venue"],
                asset_class=execution_scope["asset_class"],
                exchange=execution_scope["exchange"],
                symbol="HISTORICAL-AMBIGUITY",
                residual_quantity=Decimal("1"),
                last_local_fill_at=sold_at,
                broker_position_id="future-flat-cannot-prove-old-fifo",
                broker_position_quantity=Decimal("0"),
                broker_reconciled_at=boundary_at,
                isolated_through_at=boundary_at,
                evidence_source="broker_flat_reconciliation",
                actor="test",
                reason="flat evidence only starts the next generation",
            ),
        ]
    )
    db.commit()

    payloads = (
        runtime_service.daily_pnl_payload(settings=settings, db=db, now=now),
        runtime_service.weekly_pnl_payload(settings=settings, db=db, now=now),
        runtime_service.monthly_pnl_payload(settings=settings, db=db, now=now),
    )

    for payload in payloads:
        assert payload["calculation_status"] == "not_computed"
        assert payload["calculation_reason"] == "indeterminate_fill_chronology"
        assert payload["gross_realized_pnl_krw"] is None
        assert payload["net_realized_pnl_krw"] is None

    db.close()
    engine.dispose()


def test_runtime_status_preserves_active_non_kis_canonical_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="active-upbit-account",
        account_drawdown_recovery_scope_secret="runtime-account-scope-secret-32-bytes",
        monthly_target_krw=Decimal("100"),
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-upbit-account"}
    )
    engine, _factory, db = _session(settings)
    today_start, _tomorrow_start = runtime_service._current_krx_day_utc_bounds()
    observed_at = today_start + timedelta(hours=1)

    plan = ExecutionPlan(
        external_plan_id="runtime-upbit-scope",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
    )
    scheduled = _bound_plan_item(
        settings,
        symbol="UPBIT-SCHEDULE",
        provider="upbit",
        exchange="UPBIT",
    )
    scheduled.risk_context = {
        key: value
        for key, value in (scheduled.risk_context or {}).items()
        if key != "exchange"
    }
    scheduled.venue = "upbit"
    scheduled.asset_class = "crypto"
    scheduled.jobs.append(
        ExecutionJob(
            job_type="submit_order",
            status="scheduled",
            scheduled_for=observed_at,
        )
    )
    plan.items.extend(
        [
            scheduled,
            _bound_plan_item(
                settings,
                symbol="UPBIT-BUY-WATCH",
                provider="upbit",
                exchange="UPBIT",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
            _bound_plan_item(
                settings,
                symbol="UPBIT-SELL-WATCH",
                provider="upbit",
                exchange="UPBIT",
                side="sell",
                intent="stop_sell",
                status="stop_sell_monitoring",
            ),
            _bound_plan_item(
                settings,
                symbol="KIS-WATCH",
                provider="kis",
                exchange="KRX",
                intent="watch_buy",
                status="watch_monitoring_active",
            ),
        ]
    )
    db.add(plan)
    db.add(
        _position(
            settings,
            symbol="UPBIT-SELL-WATCH",
            provider="upbit",
            exchange="UPBIT",
        )
    )

    active_buy = _canonical_order(
        settings,
        key="upbit-active-buy",
        symbol="KRW-BTC",
        side="buy",
        status="filled",
        provider="upbit",
        exchange="UPBIT",
        mode=settings.runtime_mode,
    )
    active_sell = _canonical_order(
        settings,
        key="upbit-active-sell",
        symbol="KRW-BTC",
        side="sell",
        status="filled",
        provider="upbit",
        exchange="UPBIT",
        mode=settings.runtime_mode,
    )
    foreign_buy = _canonical_order(
        foreign_settings,
        key="upbit-foreign-buy",
        symbol="KRW-BTC",
        side="buy",
        status="filled",
        provider="upbit",
        exchange="UPBIT",
        mode=settings.runtime_mode,
    )
    foreign_sell = _canonical_order(
        foreign_settings,
        key="upbit-foreign-sell",
        symbol="KRW-BTC",
        side="sell",
        status="filled",
        provider="upbit",
        exchange="UPBIT",
        mode=settings.runtime_mode,
    )
    kis_order = _canonical_order(
        settings,
        key="upbit-runtime-kis-order",
        symbol="KIS-FILL",
        side="buy",
        status="filled",
        provider="kis",
        exchange="KRX",
        mode=settings.runtime_mode,
    )
    failed = _canonical_order(
        settings,
        key="upbit-active-failed",
        symbol="UPBIT-FAIL",
        side="buy",
        status="rejected",
        provider="upbit",
        exchange="UPBIT",
        mode=settings.runtime_mode,
        observed_at=observed_at,
    )
    db.add_all(
        [active_buy, active_sell, foreign_buy, foreign_sell, kis_order, failed]
    )
    db.flush()
    db.add_all(
        [
            _fill(
                active_buy,
                key="upbit-active-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                active_sell,
                key="upbit-active-sell-fill",
                quantity=1,
                price="110",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
            _fill(
                foreign_buy,
                key="upbit-foreign-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                foreign_sell,
                key="upbit-foreign-sell-fill",
                quantity=1,
                price="1000",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
            _fill(
                kis_order,
                key="upbit-runtime-kis-fill",
                quantity=1,
                price="500",
                settlement_currency="KRW",
                transaction_cost="0",
                filled_at=observed_at,
            ),
        ]
    )
    db.commit()

    payload = runtime_service.build_runtime_status(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=observed_at),
        db=db,
        broker_adapter=UpbitBrokerAdapter(settings=settings),
    )
    board = runtime_service.build_runtime_daily_board(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=observed_at),
        db=db,
        broker_adapter=UpbitBrokerAdapter(settings=settings),
    )

    assert payload["symbols"]["scheduled_today"] == ["UPBIT-SCHEDULE"]
    assert payload["symbols"]["monitored"] == [
        "UPBIT-BUY-WATCH",
        "UPBIT-SELL-WATCH",
    ]
    assert payload["symbols"]["filled_today"] == ["KRW-BTC"]
    assert payload["symbols"]["failed_today"] == ["UPBIT-FAIL"]
    assert payload["symbols"]["positions"] == ["UPBIT-SELL-WATCH"]
    assert payload["daily_pnl"]["daily_gross_realized_pnl_krw"] == "10.0000"
    assert payload["daily_pnl"]["timezone"] == "Asia/Seoul"
    assert payload["monthly_pnl"]["monthly_gross_realized_pnl_krw"] == "10.0000"
    assert payload["monthly_pnl"]["timezone"] == "Asia/Seoul"
    assert payload["monthly_pnl"]["monthly_buy_notional"] == "100.0000"
    assert payload["monthly_pnl"]["monthly_sell_notional"] == "110.0000"
    assert board["runtime"]["execution_scope"] == {
        "provider": "upbit",
        "status": "available",
        "source": "configured_account_scope",
        "reason": None,
        "exchanges": ["UPBIT"],
    }
    board_symbols = {
        row["symbol"]: row
        for row in board["symbols"]
    }
    assert board_symbols["KRW-BTC"]["order_count"] == 2
    assert board_symbols["KRW-BTC"]["fill_count"] == 2
    assert board_symbols["KRW-BTC"]["filled_quantity"] == 2

    db.close()
    engine.dispose()


def test_runtime_status_preserves_active_alpaca_canonical_scope() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
        execution_position_account_scope="active-alpaca-account",
        account_drawdown_recovery_scope_secret="runtime-account-scope-secret-32-bytes",
    )
    foreign_settings = settings.model_copy(
        update={"execution_position_account_scope": "foreign-alpaca-account"}
    )
    engine, _factory, db = _session(settings)
    today_start, _tomorrow_start = runtime_service._current_pnl_day_utc_bounds(
        timezone=runtime_service.US_MARKET_TIMEZONE,
    )
    observed_at = today_start + timedelta(hours=1)
    active_account_id = "authenticated-active-alpaca-account"
    foreign_account_id = "authenticated-foreign-alpaca-account"
    plan = ExecutionPlan(
        external_plan_id="runtime-alpaca-scope",
        source="test",
        mode=settings.runtime_mode,
        status="accepted",
    )
    plan.items.extend(
        [
            _bound_plan_item(
                settings,
                symbol="ETH/USD",
                provider="alpaca",
                exchange="ALPACA",
                intent="watch_buy",
                status="watch_monitoring_active",
                broker_account_id=active_account_id,
            ),
            _bound_plan_item(
                settings,
                symbol="SPY",
                provider="alpaca",
                exchange="US",
                intent="watch_buy",
                status="watch_monitoring_active",
                broker_account_id=active_account_id,
            ),
        ]
    )
    db.add(plan)

    active_buy = _canonical_order(
        settings,
        key="alpaca-active-buy",
        symbol="BTC/USD",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=active_account_id,
    )
    active_sell = _canonical_order(
        settings,
        key="alpaca-active-sell",
        symbol="BTC/USD",
        side="sell",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=active_account_id,
    )
    foreign_buy = _canonical_order(
        foreign_settings,
        key="alpaca-foreign-buy",
        symbol="SOL/USD",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=foreign_account_id,
    )
    foreign_sell = _canonical_order(
        foreign_settings,
        key="alpaca-foreign-sell",
        symbol="SOL/USD",
        side="sell",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=foreign_account_id,
    )
    active_us_buy = _canonical_order(
        settings,
        key="alpaca-active-us-buy",
        symbol="SPY",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="US",
        mode=settings.runtime_mode,
        broker_account_id=active_account_id,
    )
    db.add_all(
        [active_buy, active_sell, foreign_buy, foreign_sell, active_us_buy]
    )
    db.flush()
    db.add_all(
        [
            _fill(
                active_buy,
                key="alpaca-active-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                active_sell,
                key="alpaca-active-sell-fill",
                quantity=1,
                price="110",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
            _fill(
                foreign_buy,
                key="alpaca-foreign-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                foreign_sell,
                key="alpaca-foreign-sell-fill",
                quantity=1,
                price="1000",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
            _fill(
                active_us_buy,
                key="alpaca-active-us-buy-fill",
                quantity=1,
                price="50",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=2),
            ),
        ]
    )
    db.commit()

    adapter = AlpacaBrokerAdapter(
        settings=settings,
        http_client=_StubAlpacaHttpClient(
            [
                AlpacaHttpResponse(
                    status_code=200,
                    headers={},
                    payload={"id": active_account_id},
                )
            ]
        ),
    )
    payload = runtime_service.build_runtime_status(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=observed_at),
        db=db,
        broker_adapter=adapter,
    )

    assert payload["symbols"]["monitored"] == ["ETH/USD", "SPY"]
    assert payload["symbols"]["filled_today"] == ["BTC/USD", "SPY"]
    assert payload["daily_pnl"]["realized_pnl_by_currency"]["USD"][
        "gross_realized_pnl"
    ] == "10.0000"
    assert payload["daily_pnl"]["timezone"] == "America/New_York"
    assert payload["weekly_pnl"]["timezone"] == "America/New_York"
    assert payload["monthly_pnl"]["timezone"] == "America/New_York"
    assert payload["daily_pnl"]["buy_notional_by_currency"] == {
        "USD": "150.0000"
    }
    assert payload["daily_pnl"]["sell_notional_by_currency"] == {
        "USD": "110.0000"
    }

    db.close()
    engine.dispose()


def test_runtime_status_resolves_alpaca_hmac_scope_from_authenticated_account() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
        account_drawdown_recovery_scope_secret=(
            "runtime-account-scope-secret-32-bytes"
        ),
    )
    engine, _factory, db = _session(settings)
    today_start, _tomorrow_start = runtime_service._current_pnl_day_utc_bounds(
        timezone=runtime_service.US_MARKET_TIMEZONE,
    )
    observed_at = today_start + timedelta(hours=1)
    active_account_id = "active-alpaca-account-id"
    foreign_account_id = "foreign-alpaca-account-id"

    active_buy = _canonical_order(
        settings,
        key="alpaca-hmac-active-buy",
        symbol="BTC/USD",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=active_account_id,
    )
    active_sell = _canonical_order(
        settings,
        key="alpaca-hmac-active-sell",
        symbol="BTC/USD",
        side="sell",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=active_account_id,
    )
    foreign_buy = _canonical_order(
        settings,
        key="alpaca-hmac-foreign-buy",
        symbol="BTC/USD",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=foreign_account_id,
    )
    foreign_sell = _canonical_order(
        settings,
        key="alpaca-hmac-foreign-sell",
        symbol="BTC/USD",
        side="sell",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id=foreign_account_id,
    )
    db.add_all([active_buy, active_sell, foreign_buy, foreign_sell])
    db.add(
        _position(
            settings,
            symbol="BTC/USD",
            provider="alpaca",
            exchange="ALPACA",
            broker_account_id=active_account_id,
        )
    )
    db.flush()
    db.add_all(
        [
            _fill(
                active_buy,
                key="alpaca-hmac-active-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                active_sell,
                key="alpaca-hmac-active-sell-fill",
                quantity=1,
                price="110",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
            _fill(
                foreign_buy,
                key="alpaca-hmac-foreign-buy-fill",
                quantity=1,
                price="100",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at,
            ),
            _fill(
                foreign_sell,
                key="alpaca-hmac-foreign-sell-fill",
                quantity=1,
                price="1000",
                settlement_currency="USD",
                transaction_cost="0",
                filled_at=observed_at + timedelta(minutes=1),
            ),
        ]
    )
    db.commit()
    http_client = _StubAlpacaHttpClient(
        [
            AlpacaHttpResponse(
                status_code=200,
                headers={},
                payload={"id": active_account_id},
            )
        ]
    )
    adapter = AlpacaBrokerAdapter(settings=settings, http_client=http_client)

    payload = runtime_service.build_runtime_status(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=observed_at),
        db=db,
        broker_adapter=adapter,
    )

    assert payload["execution_scope"] == {
        "provider": "alpaca",
        "status": "available",
        "source": "authenticated_broker_account",
        "reason": None,
        "exchanges": ["ALPACA", "US"],
    }
    assert payload["symbols"]["filled_today"] == ["BTC/USD"]
    assert payload["symbols"]["filled_today_counts"]["all"] == {
        "symbols": 1,
        "fills": 2,
        "quantity": 2,
    }
    assert payload["symbols"]["positions"] == ["BTC/USD"]
    assert payload["daily_pnl"]["realized_pnl_by_currency"]["USD"][
        "gross_realized_pnl"
    ] == "10.0000"
    assert payload["daily_pnl"]["timezone"] == "America/New_York"
    assert len(http_client.calls) == 1
    assert str(http_client.calls[0]["url"]).endswith("/v2/account")
    assert active_account_id not in repr(payload)
    assert foreign_account_id not in repr(payload)

    db.close()
    engine.dispose()


def test_runtime_status_reports_alpaca_scope_unavailable_without_identity_proof() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        alpaca_api_key="paper-key",
        alpaca_api_secret="paper-secret",
        account_drawdown_recovery_scope_secret=(
            "runtime-account-scope-secret-32-bytes"
        ),
    )
    engine, _factory, db = _session(settings)
    today_start, _tomorrow_start = runtime_service._current_krx_day_utc_bounds()
    observed_at = today_start + timedelta(hours=1)
    order = _canonical_order(
        settings,
        key="alpaca-unproved-scope",
        symbol="BTC/USD",
        side="buy",
        status="filled",
        provider="alpaca",
        exchange="ALPACA",
        mode=settings.runtime_mode,
        broker_account_id="unproved-alpaca-account-id",
    )
    db.add(order)
    db.flush()
    db.add(
        _fill(
            order,
            key="alpaca-unproved-scope-fill",
            quantity=1,
            price="100",
            settlement_currency="USD",
            transaction_cost="0",
            filled_at=observed_at,
        )
    )
    db.commit()
    adapter = AlpacaBrokerAdapter(
        settings=settings,
        http_client=_StubAlpacaHttpClient(
            [AlpacaHttpResponse(status_code=200, headers={}, payload={"id": " "})]
        ),
    )

    payload = runtime_service.build_runtime_status(
        settings=settings,
        runtime=runtime_service.RuntimeMetadata(started_at=observed_at),
        db=db,
        broker_adapter=adapter,
    )

    assert payload["execution_scope"] == {
        "provider": "alpaca",
        "status": "unavailable",
        "source": None,
        "reason": "authenticated_account_identity_unavailable",
        "exchanges": ["ALPACA", "US"],
    }
    assert payload["symbols"]["filled_today"] == []
    for period in ("daily_pnl", "weekly_pnl", "monthly_pnl"):
        assert payload[period]["calculation_status"] == "not_computed"
        assert (
            payload[period]["calculation_reason"]
            == "authenticated_account_identity_unavailable"
        )
        assert payload[period]["gross_realized_pnl_krw"] is None
    assert any(
        "authenticated_account_identity_unavailable" in reason
        for reason in payload["readiness"]["reasons"]
    )

    db.close()
    engine.dispose()


def test_runtime_daily_board_route_uses_injected_broker_provider() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="daily-board-upbit-account",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda current_settings: UpbitBrokerAdapter(
            settings=current_settings
        ),
    )

    with TestClient(app) as client:
        response = client.get("/runtime/daily-board")

    assert response.status_code == 200
    assert response.json()["runtime"]["execution_scope"]["provider"] == "upbit"
    engine.dispose()
