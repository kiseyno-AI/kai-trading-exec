from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerLiveMarketContext,
    BrokerOrderPlacement,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerReconnectHandle,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
)
from app.services import broker_orders as broker_orders_service
from app.services import exec_agent as strict_exec_agent
from app.services import execution_api as execution_api_service
from app.services import execution_jobs as execution_jobs_service
from app.services import monitoring as monitoring_service
from app.services import short_term_exec_agent as legacy_exec_agent
from app.services import trade_outcome_feedback as trade_outcome_feedback_service
from app.services.artifact_integrity import attach_artifact_integrity
from app.services.execution_account_route import bind_kis_account_route
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
)


class StubBrokerAdapter:
    provider = "kis"

    def __init__(
        self,
        *,
        mode: str = "live",
        available_cash: Decimal = Decimal("500000"),
        holdings: list[BrokerAccountHolding] | None = None,
        open_orders: list[BrokerAccountOpenOrder] | None = None,
    ) -> None:
        self.mode = mode
        self.snapshot = BrokerAccountSnapshot(
            available_cash=available_cash,
            observed_at=datetime.now(UTC),
            source="stub:account_snapshot",
            total_asset_value=available_cash,
            previous_total_asset_value=available_cash,
            asset_change_amount=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            holdings=holdings or [],
            open_orders=open_orders or [],
        )
        self.authenticate_calls = 0
        self.snapshot_calls = 0
        self.order_calls: list[BrokerOrderPlacementRequest] = []

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider=self.provider,
            mode=self.mode,  # type: ignore[arg-type]
            configured=True,
            api_base_url="https://stub.kis.local",
        )

    def authenticate(self) -> BrokerTokenGrant:
        self.authenticate_calls += 1
        return BrokerTokenGrant(
            access_token="stub-token",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        assert access_token
        return BrokerRealtimeApproval(
            approval_key="stub-approval",
            issued_at=datetime.now(UTC),
        )

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        assert access_token
        return BrokerTokenRevocation(message="revoked")

    def place_order(
        self,
        access_token: str,
        request: BrokerOrderPlacementRequest,
    ) -> BrokerOrderPlacement:
        assert access_token
        self.order_calls.append(request)
        observed_at = datetime(2026, 5, 13, 9, 1, tzinfo=UTC)
        return BrokerOrderPlacement(
            broker_order_id="kis-short-term-1",
            normalized_status=NormalizedBrokerOrderStatus.SUBMITTED,
            broker_status="0",
            submitted_at=observed_at,
            observed_at=observed_at,
            request_metadata={
                "provider": self.provider,
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "tr_id": "TTTC0012U",
            },
            response_metadata={"status_code": 200, "msg_cd": "0"},
            request_payload={
                "PDNO": request.symbol,
                "ORD_QTY": str(request.quantity),
                "ORD_UNPR": "0",
            },
            response_payload={
                "rt_cd": "0",
                "msg_cd": "0",
                "output": {"ODNO": "kis-short-term-1"},
            },
            message="accepted",
        )

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        assert access_token
        return BrokerCurrentPrice(
            symbol=symbol,
            exchange=exchange,
            price=Decimal("50000"),
            observed_at=datetime.now(UTC),
            payload={},
        )

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        assert access_token
        del symbols, reference_prices, exchange
        self.snapshot_calls += 1
        return self.snapshot

    def is_krx_trading_day(self, access_token: str, *, market_date: date) -> bool:
        assert access_token
        return market_date.weekday() < 5

    def get_live_market_context(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
        reference_price: Decimal | None = None,
    ) -> BrokerLiveMarketContext:
        assert access_token
        del reference_price
        return BrokerLiveMarketContext(
            symbol=symbol,
            exchange=exchange,
            observed_at=datetime(2026, 5, 13, 0, 0, 1, tzinfo=UTC),
            bid=Decimal("49990"),
            ask=Decimal("50000"),
            freshness_source="kis_exchange_orderbook_time",
            normal_ordering_available=True,
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(
        self,
        access_token: str,
        broker_order_id: str,
        *,
        exchange: str = "KRX",
    ) -> dict[str, object]:
        return {"access_token_present": bool(access_token), "broker_order_id": broker_order_id}

    def build_headers(
        self,
        access_token: str,
        *,
        transaction_id: str | None = None,
        hashkey: str | None = None,
    ) -> dict[str, str]:
        del transaction_id, hashkey
        return {"authorization": f"Bearer {access_token}"}


def build_test_app(
    adapter: StubBrokerAdapter,
    *,
    runtime_mode: str = "paper",
    exec_agent_max_symbol_budget: Decimal = Decimal("300000"),
    exec_agent_max_total_budget: Decimal = Decimal("600000"),
    exec_agent_max_daily_new_buy_notional: Decimal = Decimal("2000000"),
    exec_agent_max_trade_risk_pct: Decimal = Decimal("0.015"),
    exec_agent_stop_loss_pct: Decimal = Decimal("0.10"),
    exec_agent_tactical_scalp_max_stop_pct: Decimal = Decimal("0.015"),
    exec_agent_max_holding_trading_sessions: int = 2,
    vol_corr_sizing_enabled: bool = False,
):
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=runtime_mode,  # type: ignore[arg-type]
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="route-binding-secret",
        exec_agent_max_symbol_budget=exec_agent_max_symbol_budget,
        exec_agent_max_total_budget=exec_agent_max_total_budget,
        exec_agent_base_risk_pct=Decimal("0.01"),
        exec_agent_max_trade_risk_pct=exec_agent_max_trade_risk_pct,
        exec_agent_aggressive_confidence_threshold=Decimal("0.80"),
        exec_agent_max_position_allocation_pct=Decimal("0.50"),
        exec_agent_max_symbol_exposure_pct=Decimal("0.20"),
        exec_agent_max_symbol_total_exposure_pct=Decimal("0.20"),
        exec_agent_max_daily_new_buy_notional=exec_agent_max_daily_new_buy_notional,
        exec_agent_stop_loss_pct=exec_agent_stop_loss_pct,
        exec_agent_tactical_scalp_max_stop_pct=exec_agent_tactical_scalp_max_stop_pct,
        exec_agent_max_holding_trading_sessions=(
            exec_agent_max_holding_trading_sessions
        ),
        vol_corr_sizing_enabled=vol_corr_sizing_enabled,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        broker_adapter_factory=lambda _settings: adapter,
        monitoring_price_source=None,
    )
    return app, engine


def dispatch_short_term_jobs_at_the_planned_open(
    app,
    adapter: StubBrokerAdapter,
):
    planned_open = datetime(2026, 5, 13, 0, 0, 1, tzinfo=UTC)
    adapter.snapshot = replace(adapter.snapshot, observed_at=planned_open)
    with app.state.db_session_factory() as session:
        return execution_jobs_service.dispatch_due_execution_jobs(
            settings=app.state.settings,
            db=session,
            adapter=adapter,
            now=planned_open,
        )


def short_term_plan_payload(
    *,
    plan_id: str = "plan-short-term-agent-1",
    source_plan_item_id: str = "watcher-candidate-1",
    exec_agent_decision: dict[str, object] | None = None,
    source_market_date: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "plan_id": plan_id,
        "source": "test-fixture",
        "market_date": "2026-05-13",
        "items": [
            {
                "symbol": "005930",
                "side": "buy",
                "intent": "next_open_buy",
                "time_in_force": "day",
                "decision_agent_name": "short-term-decider",
                "decision_snapshot_id": "snapshot-1",
                "decision_run_id": "run-1",
                "source_plan_item_id": source_plan_item_id,
                "risk_context": {
                    "confirmation_signal": "momentum_continuation",
                    "exec_agent_decision": exec_agent_decision
                    or {
                        "action": "approve",
                        "confidence": "0.82",
                        "max_budget": "260000",
                        "reference_price": "50000",
                        "target_price": "56000",
                        "stop_price": "47000",
                        "risk_flags": ["momentum_continuation"],
                        "feedback_ids": ["fb-1"],
                        "context": {
                            "watcher_signal_id": "sig-1",
                            "confirmation_signal": "momentum_continuation",
                        },
                    }
                },
            }
        ],
    }
    if source_market_date is not None:
        items = payload["items"]
        assert isinstance(items, list)
        item = items[0]
        assert isinstance(item, dict)
        item["source_market_date"] = source_market_date
    _bind_precomputed_exec_agent_decisions(payload)
    return payload


def multi_short_term_plan_payload(
    *,
    plan_id: str = "plan-short-term-agent-multi",
    candidates: list[tuple[str, str, dict[str, object]]],
) -> dict[str, object]:
    payload: dict[str, object] = {
        "plan_id": plan_id,
        "source": "test-fixture",
        "market_date": "2026-05-13",
        "items": [
            {
                "symbol": symbol,
                "side": "buy",
                "intent": "next_open_buy",
                "time_in_force": "day",
                "decision_agent_name": "short-term-decider",
                "decision_snapshot_id": f"snapshot-{index}",
                "decision_run_id": "run-1",
                "source_plan_item_id": source_plan_item_id,
                "risk_context": {
                    "confirmation_signal": "momentum_continuation",
                    "exec_agent_decision": exec_agent_decision,
                },
            }
            for index, (symbol, source_plan_item_id, exec_agent_decision) in enumerate(candidates, start=1)
        ],
    }
    _bind_precomputed_exec_agent_decisions(payload)
    return payload


def _bind_precomputed_exec_agent_decisions(payload: dict[str, object]) -> None:
    items = payload.get("items")
    assert isinstance(items, list)
    for raw_item in items:
        assert isinstance(raw_item, dict)
        risk_context = raw_item.get("risk_context")
        if not isinstance(risk_context, dict):
            continue
        decision = risk_context.get("exec_agent_decision")
        if not isinstance(decision, dict):
            continue
        bound_risk, bound_policy = bind_kis_account_route(
            settings=Settings(
                runtime_mode="paper",
                kis_account_number="12345678",
                kis_account_product_code="01",
                account_drawdown_recovery_scope_secret="route-binding-secret",
            ),
            provider=str(raw_item.get("provider") or "kis"),
            risk_context=risk_context,
            quantity_policy=raw_item.get("quantity_policy"),
        )
        raw_item["risk_context"] = bound_risk or {}
        if bound_policy is not None:
            raw_item["quantity_policy"] = bound_policy
        risk_context = raw_item["risk_context"]
        assert isinstance(risk_context, dict)
        decision = risk_context.get("exec_agent_decision")
        assert isinstance(decision, dict)
        decision["binding"] = strict_exec_agent.build_precomputed_exec_agent_binding(
            plan_id=str(payload["plan_id"]),
            market_date=(
                str(payload["market_date"])
                if payload.get("market_date") is not None
                else None
            ),
            strategy_run_id=(
                str(payload["strategy_run_id"])
                if payload.get("strategy_run_id") is not None
                else None
            ),
            item_payload=raw_item,
            valid_until=datetime(2030, 1, 1, tzinfo=UTC),
        )


def strict_exec_decision(
    confidence: str,
    *,
    max_budget: str = "400000",
    context: dict[str, object] | None = None,
    target_price: str = "56000",
    stop_price: str = "47000",
    reward_risk: str | None = None,
    max_total_budget: str | None = None,
    special_tactical_scalp: bool = False,
    target_profile: str | None = None,
    allow_existing_holding: bool = False,
) -> dict[str, object]:
    decision_context = {"confirmation_signal": "momentum_continuation"}
    if context is not None:
        decision_context.update(context)
    decision: dict[str, object] = {
        "action": "approve",
        "confidence": confidence,
        "max_budget": max_budget,
        "reference_price": "50000",
        "target_price": target_price,
        "stop_price": stop_price,
        "risk_flags": ["momentum_continuation"],
        "special_tactical_scalp": special_tactical_scalp,
        "allow_existing_holding": allow_existing_holding,
        "context": decision_context,
    }
    if target_profile is not None:
        decision["target_profile"] = target_profile
    if reward_risk is not None:
        decision["reward_risk"] = reward_risk
    if max_total_budget is not None:
        decision["max_total_budget"] = max_total_budget
    return decision


def test_strict_short_term_exec_agent_sizes_candidates_by_confidence_with_equal_caps() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=multi_short_term_plan_payload(
            candidates=[
                ("005930", "candidate-high", strict_exec_decision("0.82")),
                ("000660", "candidate-medium", strict_exec_decision("0.70")),
                ("035420", "candidate-low", strict_exec_decision("0.55")),
            ]
        ),
    )

    assert response.status_code == 200
    items = {
        item["symbol"]: item
        for item in response.json()["items"]
        if item["intent"] == "next_open_buy"
    }
    audits = {
        symbol: item["risk_context"]["exec_agent_decision"]
        for symbol, item in items.items()
    }
    assert audits["005930"]["final_budget"] == "400000.0000"
    assert audits["000660"]["final_budget"] == "300000.0000"
    assert audits["035420"]["final_budget"] == "200000.0000"
    assert items["005930"]["quantity_policy"]["quantity"] == 8
    assert items["000660"]["quantity_policy"]["quantity"] == 6
    assert items["035420"]["quantity_policy"]["quantity"] == 4
    assert audits["005930"]["base_budget"] == "400000.0000"
    assert audits["005930"]["budget_multiplier"] == "1.0000"
    assert audits["005930"]["reference_price"] == "50000.0000"
    assert audits["005930"]["quantity"] == 8

    engine.dispose()


def test_strict_short_term_exec_agent_rejects_weak_confidence() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(exec_agent_decision=strict_exec_decision("0.49")),
    )

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "confidence_below_minimum"

    engine.dispose()


def test_strict_short_term_exec_agent_reserves_total_budget_across_candidates() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("600000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=multi_short_term_plan_payload(
            candidates=[
                ("005930", "candidate-high", strict_exec_decision("0.82")),
                ("000660", "candidate-medium", strict_exec_decision("0.70")),
                ("035420", "candidate-low", strict_exec_decision("0.55")),
            ]
        ),
    )

    assert response.status_code == 200
    items = [item for item in response.json()["items"] if item["intent"] == "next_open_buy"]
    by_symbol = {item["symbol"]: item for item in items}
    assert by_symbol["005930"]["risk_context"]["exec_agent_decision"]["final_budget"] == "400000.0000"
    assert by_symbol["000660"]["risk_context"]["exec_agent_decision"]["base_budget"] == "200000.0000"
    assert by_symbol["000660"]["risk_context"]["exec_agent_decision"]["final_budget"] == "150000.0000"
    assert by_symbol["000660"]["quantity_policy"]["quantity"] == 3
    assert by_symbol["035420"]["status"] == "exec_agent_rejected"
    assert by_symbol["035420"]["risk_context"]["exec_agent_decision"]["reason_code"] == "insufficient_cash"

    engine.dispose()


def test_strict_short_term_exec_agent_reserves_available_cash_across_candidates_when_total_cap_is_higher() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=multi_short_term_plan_payload(
            candidates=[
                ("005930", "candidate-high-1", strict_exec_decision("0.82")),
                ("000660", "candidate-high-2", strict_exec_decision("0.82")),
                ("035420", "candidate-high-3", strict_exec_decision("0.82")),
            ]
        ),
    )

    assert response.status_code == 200
    items = [item for item in response.json()["items"] if item["intent"] == "next_open_buy"]
    by_symbol = {item["symbol"]: item for item in items}
    first_audit = by_symbol["005930"]["risk_context"]["exec_agent_decision"]
    second_audit = by_symbol["000660"]["risk_context"]["exec_agent_decision"]
    third_audit = by_symbol["035420"]["risk_context"]["exec_agent_decision"]

    assert first_audit["base_budget"] == "400000.0000"
    assert first_audit["final_budget"] == "400000.0000"
    assert by_symbol["005930"]["quantity_policy"]["quantity"] == 2
    assert second_audit["base_budget"] == "400000.0000"
    assert second_audit["final_budget"] == "400000.0000"
    assert second_audit["remaining_available_cash"] == "400000.0000"
    assert by_symbol["000660"]["quantity_policy"]["quantity"] == 2
    assert by_symbol["035420"]["status"] == "next_open_buy_scheduled"
    assert third_audit["risk_based_quantity"] == 2
    assert by_symbol["035420"]["quantity_policy"]["quantity"] == 2

    engine.dispose()


def test_strict_short_term_exec_agent_applies_target_liquidity_and_spread_adjustments() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    client = TestClient(app)

    payload = multi_short_term_plan_payload(
        candidates=[
            ("005930", "candidate-clean", strict_exec_decision("0.82")),
            (
                "000660",
                "candidate-risky",
                strict_exec_decision(
                    "0.82",
                    target_price="57000",
                    stop_price="45000",
                ),
            ),
        ]
    )
    risky_context = payload["items"][1]["risk_context"]
    assert isinstance(risky_context, dict)
    risky_context.update(
        {
            "liquidity_score": "0.45",
            "spread_bps": "80",
            "orderbook_quality": "0.55",
        }
    )
    _bind_precomputed_exec_agent_decisions(payload)

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    items = {
        item["symbol"]: item
        for item in response.json()["items"]
        if item["intent"] == "next_open_buy"
    }
    clean_audit = items["005930"]["risk_context"]["exec_agent_decision"]
    risky_audit = items["000660"]["risk_context"]["exec_agent_decision"]
    assert clean_audit["final_budget"] == "400000.0000"
    assert Decimal(risky_audit["final_budget"]) < Decimal(clean_audit["final_budget"])
    assert risky_audit["risk_adjustments"]["target_stop"]["reason"] == "wide_stop_distance"
    assert risky_audit["risk_adjustments"]["liquidity"]["multiplier"] == "0.7500"
    assert risky_audit["risk_adjustments"]["spread"]["multiplier"] == "0.7500"
    assert risky_audit["risk_adjustments"]["orderbook_quality"]["multiplier"] == "0.8000"
    assert items["000660"]["quantity_policy"]["quantity"] < items["005930"]["quantity_policy"]["quantity"]

    engine.dispose()


def test_short_term_exec_agent_quantity_is_capped_by_stop_distance_risk() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.82",
                max_budget="500000",
                target_price="65000",
                stop_price="40000",
            )
        ),
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["quantity_policy"]["quantity"] == 6
    assert audit["risk_based_quantity"] == 6
    assert audit["allocation_quantity"] == 8
    assert audit["stop_distance"] == "5000.0000"

    engine.dispose()


def test_short_term_exec_agent_applies_vol_corr_cap_when_enabled() -> None:
    adapter = StubBrokerAdapter(
        available_cash=Decimal("2000000"),
        holdings=[BrokerAccountHolding(symbol="000660", quantity=3)],
    )
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
        vol_corr_sizing_enabled=True,
    )
    client = TestClient(app)

    payload = short_term_plan_payload(
        exec_agent_decision=strict_exec_decision("0.82", max_budget="500000")
    )
    item = payload["items"][0]
    assert isinstance(item, dict)
    risk_context = item["risk_context"]
    assert isinstance(risk_context, dict)
    risk_context.update(
        {
            "realized_volatility_pct": "5.0",
            "max_holding_correlation": "0.82",
            "correlation_symbol": "000660",
        }
    )
    _bind_precomputed_exec_agent_decisions(payload)

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    buy_item = response.json()["items"][0]
    audit = buy_item["risk_context"]["exec_agent_decision"]
    assert buy_item["quantity_policy"]["quantity"] == 4
    assert audit["quantity"] == 4
    assert audit["risk_based_quantity"] == 10
    assert audit["allocation_quantity"] == 8
    sizing_caps = audit["sizing_caps"]
    assert sizing_caps["baseline_quantity"] == 8
    assert sizing_caps["capped_quantity"] == 4
    assert sizing_caps["volatility_multiplier"] == "0.7500"
    assert sizing_caps["correlation_multiplier"] == "0.8000"
    assert sizing_caps["vol_corr_multiplier"] == "0.6000"
    assert sizing_caps["vol_corr_cap_reason"] == "high_volatility+high_correlation"

    engine.dispose()


def test_short_term_exec_agent_enforces_daily_new_buy_notional_cap() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
        exec_agent_max_daily_new_buy_notional=Decimal("120000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=multi_short_term_plan_payload(
            candidates=[
                ("005930", "candidate-first", strict_exec_decision("0.82")),
                ("000660", "candidate-second", strict_exec_decision("0.82")),
            ]
        ),
    )

    assert response.status_code == 200
    items = {
        item["symbol"]: item
        for item in response.json()["items"]
        if item["intent"] == "next_open_buy"
    }
    assert items["005930"]["quantity_policy"]["quantity"] == 2
    assert items["000660"]["status"] == "exec_agent_rejected"
    assert items["000660"]["risk_context"]["exec_agent_decision"]["reason_code"] == "daily_new_buy_cap_reached"

    engine.dispose()


def test_daily_new_buy_notional_loaders_count_filled_buy_items() -> None:
    adapter = StubBrokerAdapter()
    app, engine = build_test_app(adapter)
    market_date = date(2026, 5, 13)

    with Session(engine) as db:
        foreign_settings = app.state.settings.model_copy(
            update={"kis_account_number": "87654321"}
        )
        foreign_identity = build_position_identity(
            settings=foreign_settings,
            provider="kis",
            exchange="KRX",
        )
        plan = ExecutionPlan(
            external_plan_id="plan-filled-daily-notional",
            source="kai-trading-core",
            market_date=market_date,
            mode=app.state.settings.runtime_mode,
            raw_payload={},
        )
        other_date_plan = ExecutionPlan(
            external_plan_id="plan-filled-daily-notional-other-date",
            source="kai-trading-core",
            market_date=date(2026, 5, 12),
            mode=app.state.settings.runtime_mode,
            raw_payload={},
        )
        foreign_filled_risk, foreign_filled_policy = bind_kis_account_route(
            settings=foreign_settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 20},
        )
        foreign_unresolved_risk, foreign_unresolved_policy = (
            bind_kis_account_route(
                settings=foreign_settings,
                provider="kis",
                risk_context={"exchange": "KRX"},
                quantity_policy={"kind": "shares", "quantity": 30},
            )
        )
        foreign_unresolved_item = ExecutionPlanItem(
            plan_item_index=1,
            symbol="051910",
            side="buy",
            intent="next_open_buy",
            status="broker_order_failed",
            risk_context=foreign_unresolved_risk,
            quantity_policy=foreign_unresolved_policy,
            limit_price=Decimal("10000"),
        )
        foreign_plan = ExecutionPlan(
            external_plan_id="plan-filled-daily-notional-foreign-account",
            source="kai-trading-core",
            market_date=market_date,
            mode=app.state.settings.runtime_mode,
            raw_payload={},
            items=[
                ExecutionPlanItem(
                    plan_item_index=0,
                    symbol="373220",
                    side="buy",
                    intent="next_open_buy",
                    status="filled",
                    risk_context=foreign_filled_risk,
                    quantity_policy=foreign_filled_policy,
                    limit_price=Decimal("10000"),
                ),
                foreign_unresolved_item,
            ],
        )
        unresolved_item = ExecutionPlanItem(
            plan_item_index=3,
            symbol="068270",
            side="buy",
            intent="next_open_buy",
            status="broker_order_failed",
            quantity_policy={"kind": "shares", "quantity": 8},
            limit_price=Decimal("10000"),
        )
        plan.items = [
            ExecutionPlanItem(
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="filled",
                quantity_policy={"kind": "shares", "quantity": 2},
                limit_price=Decimal("50000"),
            ),
            ExecutionPlanItem(
                plan_item_index=1,
                symbol="000660",
                side="buy",
                intent="next_open_buy",
                status="partially_filled",
                quantity_policy={"kind": "shares", "quantity": 1},
                limit_price=Decimal("70000"),
            ),
            ExecutionPlanItem(
                plan_item_index=2,
                symbol="035420",
                side="sell",
                intent="target_sell",
                status="filled",
                quantity_policy={"kind": "shares", "quantity": 10},
                limit_price=Decimal("100000"),
            ),
            unresolved_item,
        ]
        other_date_plan.items = [
            ExecutionPlanItem(
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="filled",
                quantity_policy={"kind": "shares", "quantity": 99},
                limit_price=Decimal("50000"),
            )
        ]
        db.add_all(
            [
                plan,
                other_date_plan,
                foreign_plan,
                BrokerOrder(
                    plan_item=unresolved_item,
                    broker_name="kis",
                    mode=app.state.settings.runtime_mode,
                    idempotency_key="daily-cap-unknown-buy",
                    symbol="068270",
                    side="buy",
                    status="manual_review_required",
                    quantity=8,
                    limit_price=Decimal("10000"),
                ),
                BrokerOrder(
                    plan_item=foreign_unresolved_item,
                    broker_name="kis",
                    mode=app.state.settings.runtime_mode,
                    idempotency_key="daily-cap-foreign-unknown-buy",
                    symbol="051910",
                    side="buy",
                    status="manual_review_required",
                    quantity=30,
                    limit_price=Decimal("10000"),
                    request_metadata={
                        "exchange": "KRX",
                        "execution_scope": execution_scope_payload(
                            identity=foreign_identity,
                            venue="KRX",
                            asset_class="kr_equity",
                        ),
                    },
                ),
            ]
        )
        db.commit()

        assert strict_exec_agent._load_daily_new_buy_notional(
            settings=app.state.settings,
            db=db,
            market_date=market_date,
        ) == Decimal("250000.0000")
        assert monitoring_service._load_daily_new_buy_notional(
            settings=app.state.settings,
            db=db,
            market_date=market_date,
        ) == Decimal("250000.0000")

    engine.dispose()


def test_short_term_exec_agent_uses_configured_hard_stop_when_upstream_stop_is_looser() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
        exec_agent_stop_loss_pct=Decimal("0.04"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.82",
                max_budget="500000",
                target_price="56000",
                stop_price="45000",
            )
        ),
    )

    assert response.status_code == 200
    items = response.json()["items"]
    buy_item = items[0]
    stop_item = [item for item in items if item["intent"] == "stop_sell"][0]
    audit = buy_item["risk_context"]["exec_agent_decision"]
    assert audit["upstream_stop_price"] == "45000.0000"
    assert audit["stop_price"] == "48000.0000"
    assert stop_item["trigger_price"] == "48000.0000"

    engine.dispose()


def test_strict_and_legacy_short_term_exec_agent_confidence_multipliers_stay_aligned() -> None:
    cases = [
        (Decimal("0.82"), "high"),
        (Decimal("0.70"), "medium"),
        (Decimal("0.55"), "low"),
        (Decimal("0.49"), "low"),
    ]

    for confidence, conviction in cases:
        assert strict_exec_agent._confidence_budget_multiplier(confidence) == legacy_exec_agent._budget_multiplier(
            confidence=confidence,
            conviction=conviction,
        )


def test_short_term_exec_agent_approves_market_open_buy_and_materializes_monitoring() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(source_market_date="2026-05-08"),
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["exec_agent"]["approved_count"] == 1
    assert payload["exec_agent"]["rejected_count"] == 0
    assert len(payload["persisted_execution_item_ids"]) == 5

    plans = client.get("/execution-plans").json()
    items_by_intent = {item["intent"]: item for item in plans[0]["items"] if item["intent"] != "target_sell"}
    target_items = [item for item in plans[0]["items"] if item["intent"] == "target_sell"]
    assert items_by_intent["next_open_buy"]["status"] == "next_open_buy_scheduled"
    assert items_by_intent["next_open_buy"]["quantity_policy"]["quantity"] == 2
    assert [item["quantity_policy"]["quantity"] for item in target_items] == [1, 1]
    assert [item["trigger_price"] for item in target_items] == ["51250.0000", "52750.0000"]
    assert target_items[0]["risk_context"]["exit_rule"]["kind"] == "partial_take_profit"
    assert target_items[1]["risk_context"]["exit_rule"]["kind"] == "final_take_profit"
    assert items_by_intent["stop_sell"]["status"] == "stop_sell_monitoring"
    assert items_by_intent["stop_sell"]["trigger_price"] == "47000.0000"
    assert items_by_intent["max_holding_close_sell"]["status"] == "max_holding_close_sell_monitoring"
    max_holding_context = items_by_intent["max_holding_close_sell"]["risk_context"]
    assert max_holding_context["max_holding_trading_sessions"] == 2
    assert max_holding_context["close_exit_date"] == "2026-05-14"
    assert max_holding_context["close_exit_date_status"] == "weekday_provisional"
    assert max_holding_context["close_exit_date_source"] == "plan_market_date"
    assert max_holding_context["entry_market_date"] == "2026-05-13"
    assert max_holding_context["entry_market_date_source"] == "plan_market_date"
    assert items_by_intent["max_holding_close_sell"]["source_market_date"] == "2026-05-08"

    dispatch = dispatch_short_term_jobs_at_the_planned_open(app, adapter)

    assert dispatch.status_code == 200
    assert dispatch.payload["completed_job_count"] == 1, str(dispatch.payload["jobs"])
    assert len(adapter.order_calls) == 1
    order_call = adapter.order_calls[0]
    assert order_call.quantity == 2
    assert order_call.order_type == "market"
    assert order_call.idempotency_key == (
        "short-term-exec-agent:plan-short-term-agent-1:watcher-candidate-1"
    )

    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob))
        assert job is not None
        assert job.status == "completed"
        order = session.scalar(select(BrokerOrder))
        assert order is not None
        assert order.idempotency_key == order_call.idempotency_key
        assert order.broker_order_id == "kis-short-term-1"
        buy_item = session.scalar(
            select(ExecutionPlanItem).where(ExecutionPlanItem.intent == "next_open_buy")
        )
        assert buy_item is not None
        audit = buy_item.risk_context["exec_agent_decision"]
        assert audit["feedback_ids"] == ["fb-1"]
        assert audit["agent_contract"]["context"] == {
            "watcher_signal_id": "sig-1",
            "confirmation_signal": "momentum_continuation",
        }
        assert audit["max_quantity"] == 2
        assert audit["max_budget"] == "260000.0000"
        assert audit["reward_risk"] == "2.0000"
        assert audit["exposure_after_trade"] == "100000.0000"
        assert audit["risk_based_quantity"] == 2
        assert audit["reason_code"] == "approved"
        assert audit["risk_flags"] == [
            "confirmation:momentum_continuation",
            "momentum_continuation",
        ]
        assert audit["account_snapshot"]["available_cash"] == "500000.0000"

    engine.dispose()


@pytest.mark.parametrize(
    ("configured_sessions", "policy", "expected_sessions", "expected_close_date"),
    [
        (1, None, 2, "2026-05-14"),
        (9, None, 3, "2026-05-15"),
        (2, False, 3, "2026-05-15"),
        (2, {"enabled": False}, 3, "2026-05-15"),
    ],
)
def test_short_term_prefill_max_holding_is_an_inclusive_two_to_three_session_cap(
    configured_sessions: int,
    policy: dict[str, object] | bool | None,
    expected_sessions: int,
    expected_close_date: str,
) -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_holding_trading_sessions=configured_sessions,
    )
    client = TestClient(app)
    payload = short_term_plan_payload(
        plan_id=f"prefill-max-holding-{configured_sessions}-{policy}",
    )
    if policy is not None:
        items = payload["items"]
        assert isinstance(items, list)
        item = items[0]
        assert isinstance(item, dict)
        risk_context = item["risk_context"]
        assert isinstance(risk_context, dict)
        risk_context["max_holding_close_policy"] = policy
        _bind_precomputed_exec_agent_decisions(payload)

    response = client.post("/execution-plans", json=payload)

    assert response.status_code == 201, response.text
    persisted_items = client.get("/execution-plans").json()[0]["items"]
    max_holding = next(
        item
        for item in persisted_items
        if item["intent"] == "max_holding_close_sell"
    )
    context = max_holding["risk_context"]
    assert context["max_holding_trading_sessions"] == expected_sessions
    assert context["close_exit_date"] == expected_close_date
    assert context["close_exit_policy"] == "market_on_or_after_close_exit_date"

    engine.dispose()


@pytest.mark.parametrize(
    ("configured_sessions", "policy", "expected_sessions"),
    [
        (1, None, 2),
        (9, None, 3),
        (2, {"enabled": True, "trading_sessions": 1}, 2),
        (2, {"enabled": True, "trading_sessions": 9}, 3),
        (2, False, 3),
        (2, {"enabled": False}, 3),
    ],
)
def test_short_term_postfill_max_holding_uses_the_same_two_to_three_session_cap(
    configured_sessions: int,
    policy: dict[str, object] | bool | None,
    expected_sessions: int,
) -> None:
    risk_context: dict[str, object] = {"exchange": "KRX"}
    if policy is not None:
        risk_context["max_holding_close_policy"] = policy
    entry_item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        decision_agent_name="short-term-decider",
        risk_context=risk_context,
    )

    assert monitoring_service._post_entry_max_holding_sessions(
        settings=Settings(
            exec_agent_max_holding_trading_sessions=configured_sessions,
        ),
        entry_item=entry_item,
    ) == expected_sessions


@pytest.mark.parametrize(
    ("policy", "expected_sessions"),
    [
        (None, None),
        (False, None),
        ({"enabled": True, "trading_sessions": 1}, 1),
    ],
)
def test_signal_postfill_max_holding_contract_is_unchanged(
    policy: dict[str, object] | bool | None,
    expected_sessions: int | None,
) -> None:
    risk_context: dict[str, object] = {"exchange": "KRX"}
    if policy is not None:
        risk_context["max_holding_close_policy"] = policy
    entry_item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        decision_agent_name="signal-decider",
        risk_context=risk_context,
    )

    assert monitoring_service._post_entry_max_holding_sessions(
        settings=Settings(exec_agent_max_holding_trading_sessions=1),
        entry_item=entry_item,
    ) == expected_sessions


def test_prefill_monitor_keeps_missing_source_market_date_as_lineage_only() -> None:
    plan = ExecutionPlan(
        external_plan_id="prefill-lineage",
        source="test",
        mode="paper",
        market_date=date(2026, 10, 2),
        status="accepted",
    )
    buy_item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="next_open_buy_scheduled",
        provider="kis",
        venue="krx",
        asset_class="stock",
        source_market_date=None,
        risk_context={"exchange": "KRX"},
    )

    monitor = strict_exec_agent._new_monitoring_item(
        plan=plan,
        buy_item=buy_item,
        decision={"idempotency_key": "prefill-lineage-key", "confidence": "high"},
        intent="max_holding_close_sell",
        quantity=1,
        next_plan_item_index=1,
        trigger_price=None,
        max_sessions=2,
        exit_rule={"kind": "max_holding"},
    )

    assert monitor.source_market_date is None
    assert monitor.risk_context["entry_market_date"] == "2026-10-02"
    assert monitor.risk_context["entry_market_date_status"] == "provisional"


def test_market_buy_capital_gate_prefers_verified_exec_agent_audit_over_legacy_context() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    response = client.post("/execution-plans", json=short_term_plan_payload())
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        buy_item = session.scalar(
            select(ExecutionPlanItem).where(ExecutionPlanItem.intent == "next_open_buy")
        )
        assert buy_item is not None
        risk_context = dict(buy_item.risk_context or {})
        risk_context["reference_price"] = "999999.0000"
        buy_item.risk_context = risk_context
        session.commit()

    dispatch = dispatch_short_term_jobs_at_the_planned_open(app, adapter)

    assert dispatch.status_code == 200
    assert dispatch.payload["completed_job_count"] == 1
    assert len(adapter.order_calls) == 1
    assert adapter.order_calls[0].order_type == "market"
    assert adapter.order_calls[0].quantity == 2

    engine.dispose()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sizing_price", "NaN"),
        ("sizing_price", "Infinity"),
        ("reference_price", "51000.0000"),
        ("approved_notional", "99999.0000"),
    ],
)
def test_market_buy_capital_gate_rejects_invalid_or_conflicting_exec_agent_audit(
    field: str,
    value: str,
) -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    response = client.post("/execution-plans", json=short_term_plan_payload())
    assert response.status_code == 201

    with app.state.db_session_factory() as session:
        buy_item = session.scalar(
            select(ExecutionPlanItem).where(ExecutionPlanItem.intent == "next_open_buy")
        )
        assert buy_item is not None
        risk_context = dict(buy_item.risk_context or {})
        risk_context["reference_price"] = "50000.0000"
        decision = dict(risk_context["exec_agent_decision"])
        decision[field] = value
        risk_context["exec_agent_decision"] = decision
        buy_item.risk_context = risk_context
        session.commit()

    dispatch = dispatch_short_term_jobs_at_the_planned_open(app, adapter)

    assert dispatch.status_code == 200
    assert dispatch.payload["completed_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as session:
        blocked_order = session.scalar(select(BrokerOrder))
        assert blocked_order is None
        buy_item = session.scalar(
            select(ExecutionPlanItem).where(ExecutionPlanItem.intent == "next_open_buy")
        )
        assert buy_item is not None
        assert buy_item.status == "next_open_buy_failed"

    engine.dispose()


def test_short_term_exec_agent_rejects_insufficient_cash() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("40000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "risk_budget_too_small"
    assert client.get("/execution-jobs").json() == []
    assert adapter.order_calls == []

    engine.dispose()


@pytest.mark.parametrize(
    "snapshot_completeness",
    [
        {"holdings_complete": False},
        {"open_orders_complete": False},
    ],
)
def test_legacy_short_term_exec_agent_rejects_incomplete_broker_truth(
    snapshot_completeness: dict[str, bool],
) -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    adapter.snapshot = replace(adapter.snapshot, **snapshot_completeness)
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == (
        "account_snapshot_unavailable"
    )
    assert client.get("/execution-jobs").json() == []
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_existing_holding_without_exact_lane_lineage() -> None:
    adapter = StubBrokerAdapter(
        available_cash=Decimal("2000000"),
        holdings=[BrokerAccountHolding(symbol="005930", quantity=3)],
    )
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("1000000"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.82",
                max_budget="100000",
                allow_existing_holding=True,
            )
        ),
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "holding_lane_attribution_incomplete"
    assert audit["existing_holding"] == 3
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_checks_lineage_before_existing_holding_exposure() -> None:
    adapter = StubBrokerAdapter(
        available_cash=Decimal("500000"),
        holdings=[BrokerAccountHolding(symbol="005930", quantity=3)],
    )
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision("0.82", max_budget="50000")
        ),
    )

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "holding_lane_attribution_incomplete"
    assert audit["existing_holding"] == 3
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_poor_reward_risk() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.82",
                target_price="52000",
                stop_price="47000",
            )
        ),
    )

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "reward_risk_below_minimum"
    assert audit["reward_risk"] == "0.6666"
    assert audit["required_reward_risk"] == "1.0000"
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_invalid_exit_prices_despite_supplied_reward_risk() -> None:
    invalid_exit_price_cases = [
        {"target_price": "56000", "stop_price": "50000"},
        {"target_price": "56000", "stop_price": "51000"},
        {"target_price": "50000", "stop_price": "47000"},
        {"target_price": "49000", "stop_price": "47000"},
    ]

    for index, case in enumerate(invalid_exit_price_cases, start=1):
        adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
        app, engine = build_test_app(adapter)
        client = TestClient(app)

        response = client.post(
            "/execution-plans",
            json=short_term_plan_payload(
                plan_id=f"plan-invalid-exit-price-{index}",
                source_plan_item_id=f"watcher-invalid-exit-price-{index}",
                exec_agent_decision=strict_exec_decision(
                    "0.82",
                    reward_risk="9.99",
                    **case,
                ),
            ),
        )

        assert response.status_code == 201
        item = client.get("/execution-plans").json()[0]["items"][0]
        audit = item["risk_context"]["exec_agent_decision"]
        assert item["status"] == "exec_agent_rejected"
        assert audit["reason_code"] == "invalid_reward_risk_prices"
        assert audit["reward_risk"] is None
        assert audit["agent_contract"]["reward_risk"] == "9.99"
        assert adapter.order_calls == []

        engine.dispose()


def test_short_term_exec_agent_rejects_duplicate_open_buy_order() -> None:
    duplicate_key = "short-term-exec-agent:plan-short-term-agent-1:watcher-candidate-1"
    adapter = StubBrokerAdapter(
        available_cash=Decimal("500000"),
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=5,
                remaining_quantity=5,
                status="open",
                idempotency_key=duplicate_key,
            )
        ],
    )
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == (
        "duplicate_open_buy_order"
    )
    assert adapter.order_calls == []

    engine.dispose()


def test_open_buy_duplicate_guard_allows_other_decision_lane_only() -> None:
    item = ExecutionPlanItem(
        id="short-candidate",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id="short-family-entry",
    )
    other_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=2,
        remaining_quantity=2,
        status="open",
        broker_order_id="signal-open-order",
        payload={"decision_agent_name": "signal-decider"},
    )
    same_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        remaining_quantity=1,
        status="open",
        broker_order_id="short-open-order",
        payload={"decision_agent_name": " SHORT-TERM-DECIDER "},
    )

    assert strict_exec_agent._has_duplicate_open_buy_order(
        item=item,
        idempotency_key="candidate-idempotency",
        open_orders=[other_lane],
    ) is False
    assert strict_exec_agent._has_duplicate_open_buy_order(
        item=item,
        idempotency_key="candidate-idempotency",
        open_orders=[other_lane, same_lane],
    ) is True


def test_open_buy_duplicate_guard_scopes_shared_source_key_to_decision_lane() -> None:
    item = ExecutionPlanItem(
        id="short-shared-source-candidate",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id="shared-source-key",
    )
    other_lane = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=2,
        remaining_quantity=2,
        status="open",
        source_candidate_id="shared-source-key",
        payload={"decision_agent_name": "signal-decider"},
    )
    same_lane = replace(
        other_lane,
        payload={"decision_agent_name": "short-term-decider"},
    )

    assert strict_exec_agent._has_duplicate_open_buy_order(
        item=item,
        idempotency_key="candidate-idempotency",
        open_orders=[other_lane],
    ) is False
    assert strict_exec_agent._has_duplicate_open_buy_order(
        item=item,
        idempotency_key="candidate-idempotency",
        open_orders=[same_lane],
    ) is True


def _legacy_lane_candidate() -> tuple[ExecutionPlan, ExecutionPlanItem]:
    item = ExecutionPlanItem(
        id="legacy-lane-candidate",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id="legacy-lane-source",
        provider="kis",
        venue="KRX",
        asset_class="stock",
        risk_context={"confirmation_signal": "momentum_continuation"},
    )
    plan = ExecutionPlan(
        external_plan_id="legacy-lane-plan",
        market_date=date(2026, 5, 13),
        mode="paper",
        items=[item],
    )
    return plan, item


def _legacy_lane_account(
    *,
    lane_quantities: dict[str, int],
    attribution_complete: bool = True,
    open_orders: list[BrokerAccountOpenOrder] | None = None,
) -> strict_exec_agent._AccountContext:
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("10000"),
        buying_power=Decimal("10000"),
        observed_at=datetime.now(UTC),
        snapshot_payload={"available": True},
        holdings_by_symbol={"005930": 2},
        open_orders=open_orders or [],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
        holding_quantities_by_decision_lane={"005930": lane_quantities},
        holding_lane_attribution_complete_by_symbol={
            "005930": attribution_complete,
        },
    )
    return account


def _legacy_lane_contract(
    *,
    allow_existing_holding: bool,
) -> strict_exec_agent.ShortTermExecAgentContract:
    return strict_exec_agent.ShortTermExecAgentContract(
        action="approve",
        confidence=Decimal("0.90"),
        max_budget=Decimal("1000"),
        reference_price=Decimal("100"),
        target_price=Decimal("120"),
        stop_price=Decimal("90"),
        allow_existing_holding=allow_existing_holding,
    )


def test_legacy_next_open_sizing_keeps_other_lane_holding_independent() -> None:
    settings = Settings(
        runtime_mode="paper",
        exec_agent_max_symbol_budget=Decimal("1000"),
        exec_agent_max_total_budget=Decimal("5000"),
        exec_agent_max_position_allocation_pct=Decimal("1"),
        exec_agent_max_symbol_exposure_pct=Decimal("1"),
        exec_agent_max_symbol_total_exposure_pct=Decimal("1"),
        exec_agent_max_trade_risk_pct=Decimal("0.015"),
        exec_agent_stop_loss_pct=Decimal("0.10"),
    )
    plan, item = _legacy_lane_candidate()
    other_lane_account = _legacy_lane_account(
        lane_quantities={"signal-decider": 2}
    )

    allowed = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=other_lane_account,
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=_legacy_lane_contract(
            allow_existing_holding=True,
        ),
    )
    explicit_false = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=other_lane_account,
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=_legacy_lane_contract(
            allow_existing_holding=False,
        ),
    )
    omitted = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=other_lane_account,
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=strict_exec_agent.ShortTermExecAgentContract(
            action="approve",
            confidence=Decimal("0.90"),
            max_budget=Decimal("1000"),
            reference_price=Decimal("100"),
            target_price=Decimal("120"),
            stop_price=Decimal("90"),
        ),
    )
    same_lane = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=_legacy_lane_account(
            lane_quantities={"short-term-decider": 2}
        ),
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=_legacy_lane_contract(
            allow_existing_holding=True,
        ),
    )
    incomplete = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=_legacy_lane_account(
            lane_quantities={},
            attribution_complete=False,
        ),
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=_legacy_lane_contract(
            allow_existing_holding=True,
        ),
    )

    assert allowed["action"] == "approve"
    assert explicit_false["action"] == "approve"
    assert omitted["action"] == "approve"
    assert allowed["existing_symbol_exposure"] == "200.0000"
    assert explicit_false["existing_symbol_exposure"] == "200.0000"
    assert omitted["existing_symbol_exposure"] == "200.0000"
    assert same_lane["reason_code"] == "existing_holding"
    assert incomplete["reason_code"] == "holding_lane_attribution_incomplete"


def test_legacy_next_open_sizing_counts_other_lane_open_order_remaining_exposure() -> None:
    settings = Settings(
        runtime_mode="paper",
        exec_agent_max_symbol_budget=Decimal("1000"),
        exec_agent_max_total_budget=Decimal("5000"),
        exec_agent_max_position_allocation_pct=Decimal("1"),
        exec_agent_max_symbol_exposure_pct=Decimal("1"),
        exec_agent_max_symbol_total_exposure_pct=Decimal("1"),
        exec_agent_max_trade_risk_pct=Decimal("0.015"),
        exec_agent_stop_loss_pct=Decimal("0.10"),
    )
    plan, item = _legacy_lane_candidate()
    account = _legacy_lane_account(
        lane_quantities={"signal-decider": 2},
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=10,
                remaining_quantity=3,
                status="partially_filled",
                limit_price=Decimal("100"),
                payload={"decision_agent_name": "signal-decider"},
            )
        ],
    )

    decision = strict_exec_agent._decide_candidate(
        settings=settings,
        plan=plan,
        item=item,
        account_context=account,
        reserved_total_budget=Decimal("0"),
        reserved_cash_budget=Decimal("0"),
        reserved_daily_buy_notional=Decimal("0"),
        reserved_symbol_notional=Decimal("0"),
        reserved_entry_lanes=set(),
        agent_contract_result=_legacy_lane_contract(
            allow_existing_holding=True,
        ),
    )

    assert decision["action"] == "approve"
    assert decision["existing_symbol_exposure"] == "500.0000"
    assert decision["approved_notional"] == "500.0000"


def test_local_partial_buy_order_exposes_only_unfilled_remaining_quantity() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="route-binding-secret",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        risk_context, quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 10},
        )
        item = ExecutionPlanItem(
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="next_open_buy",
            status="broker_order_submitted",
            decision_agent_name="signal-decider",
            source_plan_item_id="partial-local-family",
            provider="kis",
            venue="KRX",
            asset_class="stock",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )
        plan = ExecutionPlan(
            external_plan_id="partial-local-plan",
            source="test",
            market_date=date(2026, 5, 13),
            mode="paper",
            items=[item],
        )
        session.add(plan)
        session.flush()
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
        order = BrokerOrder(
            broker_name="kis",
            mode="paper",
            execution_plan_item_id=item.id,
            idempotency_key="partial-local-order",
            broker_order_id="partial-local-order",
            symbol="005930",
            side="buy",
            status="partially_filled",
            quantity=10,
            limit_price=Decimal("100"),
            venue="KRX",
            asset_class="stock",
            request_metadata={
                "exchange": "KRX",
                "execution_scope": execution_scope_payload(
                    identity=identity,
                    venue="KRX",
                    asset_class="stock",
                ),
            },
        )
        session.add(order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=order.id,
                broker_fill_id="partial-local-fill",
                symbol="005930",
                side="buy",
                quantity=4,
                price=Decimal("100"),
                filled_at=datetime(2026, 5, 13, 0, 1, tzinfo=UTC),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()

        loaded = strict_exec_agent._load_local_open_orders(
            settings=settings,
            db=session,
            provider="kis",
            exchange="KRX",
            broker_account_id=None,
        )

    assert len(loaded) == 1
    assert loaded[0].quantity == 10
    assert loaded[0].remaining_quantity == 6
    engine.dispose()


def test_account_open_order_merge_counts_local_and_broker_copy_once() -> None:
    local = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=6,
        status="partially_filled",
        broker_order_id="shared-broker-order",
        idempotency_key="shared-idempotency",
        source_candidate_id="shared-source",
        limit_price=Decimal("100"),
        payload={"decision_agent_name": "signal-decider"},
    )
    broker = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=10,
        remaining_quantity=5,
        status="partially_filled",
        broker_order_id="shared-broker-order",
        limit_price=Decimal("101"),
    )

    merged = strict_exec_agent._merge_account_open_orders(
        broker_orders=[broker],
        local_orders=[local],
    )

    assert len(merged) == 1
    assert merged[0].remaining_quantity == 5
    assert merged[0].limit_price == Decimal("101")
    assert merged[0].payload["decision_agent_name"] == "signal-decider"


def test_legacy_holding_lane_attribution_uses_exact_family_fills() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="route-binding-secret",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        risk_context, quantity_policy = bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context={"exchange": "KRX"},
            quantity_policy={"kind": "shares", "quantity": 2},
        )
        entry = ExecutionPlanItem(
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="filled",
            decision_agent_name="signal-decider",
            source_plan_item_id="exact-signal-family",
            provider="kis",
            venue="KRX",
            asset_class="stock",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )
        plan = ExecutionPlan(
            external_plan_id="exact-lane-attribution-plan",
            source="test",
            market_date=date(2026, 10, 26),
            mode="paper",
            items=[entry],
        )
        session.add(plan)
        session.flush()
        identity = build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        )
        order = BrokerOrder(
            broker_name="kis",
            mode="paper",
            execution_plan_item_id=entry.id,
            idempotency_key="exact-signal-family-order",
            broker_order_id="exact-signal-family-order",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=2,
            limit_price=Decimal("100"),
            venue="KRX",
            asset_class="stock",
            request_metadata={
                "exchange": "KRX",
                "execution_scope": execution_scope_payload(
                    identity=identity,
                    venue="KRX",
                    asset_class="stock",
                ),
            },
        )
        session.add(order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=order.id,
                broker_fill_id="exact-signal-family-fill",
                symbol="005930",
                side="buy",
                quantity=2,
                price=Decimal("100"),
                filled_at=datetime(2026, 10, 26, 0, 1, tzinfo=UTC),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()

        quantities, complete = strict_exec_agent._holding_lane_attribution(
            settings=settings,
            db=session,
            holdings_by_symbol={"005930": 2},
            observed_at=datetime(2026, 10, 26, 0, 2, tzinfo=UTC),
        )

    assert quantities == {"005930": {"signal-decider": 2}}
    assert complete == {"005930": True}
    engine.dispose()


def test_short_term_exec_agent_reserves_old_manual_review_buy_outcome() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    old_observation = datetime.now(UTC) - timedelta(days=7)
    with app.state.db_session_factory() as db:
        db.add(
            BrokerOrder(
                broker_name="kis",
                mode=app.state.settings.runtime_mode,
                idempotency_key="unknown-buy-reserves-capital",
                broker_order_id=None,
                symbol="005930",
                side="buy",
                status="manual_review_required",
                quantity=5,
                limit_price=Decimal("50000"),
                last_error_classification="submit_outcome_unknown",
                last_error_code="SUBMIT_OUTCOME_UNKNOWN",
                last_error_retryable=False,
                created_at=old_observation,
                updated_at=old_observation,
            )
        )
        db.commit()

    response = client.post("/execution-plans", json=short_term_plan_payload())

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == (
        "duplicate_open_buy_order"
    )
    assert adapter.order_calls == []
    engine.dispose()


def test_short_term_exec_agent_rejects_invalid_agent_output() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans",
        json=short_term_plan_payload(
            exec_agent_decision={
                "action": "approve",
                "confidence": "0.9",
                "max_budget": "260000",
            }
        ),
    )

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "invalid_agent_output"
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_preview_does_not_persist_or_submit() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post("/execution-plans/preview", json=short_term_plan_payload())

    assert response.status_code == 200
    payload = response.json()
    assert payload["dry_run"] is True
    assert payload["exec_agent"]["approved_count"] == 1
    assert payload["execution_jobs"][0]["job_type"] == "next_open_buy"
    assert payload["items"][0]["quantity_policy"]["quantity"] == 2
    assert {item["intent"] for item in payload["items"]} == {
        "next_open_buy",
        "target_sell",
        "stop_sell",
        "max_holding_close_sell",
    }
    assert [item["intent"] for item in payload["items"]].count("target_sell") == 2
    assert adapter.order_calls == []

    with app.state.db_session_factory() as session:
        assert session.scalars(select(ExecutionPlan)).all() == []
        assert session.scalars(select(BrokerOrder)).all() == []

    engine.dispose()


def test_runtime_safety_controls_still_block_live_short_term_order_submission() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    plan_response = client.post("/execution-plans", json=short_term_plan_payload())
    assert plan_response.status_code == 201
    authenticate_calls_before_dispatch = adapter.authenticate_calls
    snapshot_calls_before_dispatch = adapter.snapshot_calls

    dispatch = dispatch_short_term_jobs_at_the_planned_open(app, adapter)

    assert dispatch.status_code == 200
    assert dispatch.payload["blocked"] is True
    assert dispatch.payload["block_reason"] == "paused"
    assert dispatch.payload["safety"]["automated_order_submission_allowed"] is True
    assert dispatch.payload["safety"]["automated_order_submission_block_reason"] is None
    assert dispatch.payload["safety"]["automated_buy_submission_allowed"] is False
    assert dispatch.payload["safety"]["automated_buy_submission_block_reason"] == (
        "paused"
    )
    assert dispatch.payload["due_job_count"] == 1
    assert dispatch.payload["blocked_buy_job_count"] == 1
    assert dispatch.payload["jobs"][0]["status"] == "scheduled"
    assert dispatch.payload["jobs"][0]["last_error"] is None
    assert adapter.authenticate_calls == authenticate_calls_before_dispatch
    assert adapter.snapshot_calls == snapshot_calls_before_dispatch
    assert adapter.order_calls == []
    assert client.get("/orders").json() == []
    with app.state.db_session_factory() as session:
        job = session.scalar(select(ExecutionJob))
        assert job is not None
        assert job.status == "scheduled"
        assert job.last_error is None
        item = session.get(ExecutionPlanItem, job.execution_plan_item_id)
        assert item is not None
        assert item.status == "next_open_buy_scheduled"

    engine.dispose()


class FakeExecAgentClient:
    def __init__(
        self,
        *,
        quantity: int = 7,
        final_budget: Decimal = Decimal("350000"),
        reference_price: Decimal = Decimal("50000"),
    ) -> None:
        self.requests = []
        self.quantity = quantity
        self.final_budget = final_budget
        self.reference_price = reference_price

    def size_candidate(self, request):
        self.requests.append(request)
        return strict_exec_agent.ExecAgentSizingResponse(
            binding=request.binding,
            action="approve",
            confidence=Decimal("0.91"),
            quantity=self.quantity,
            final_budget=self.final_budget,
            reference_price=self.reference_price,
            target_price=Decimal("57000"),
            stop_price=Decimal("47000"),
            reasons=["high conviction with sufficient cash"],
            risk_flags=["normal_spread", "momentum_continuation"],
            explanation={"score": "9.1"},
        )


class FakeBatchExecAgentClient:
    def __init__(self, decisions_by_item_id: dict[str, dict[str, object]]) -> None:
        self.requests = []
        self.legacy_requests = []
        self.decisions_by_item_id = decisions_by_item_id

    def size_candidates(self, request):
        self.requests.append(request)
        return strict_exec_agent.ExecAgentBatchSizingResponse(
            decisions=[
                strict_exec_agent.ExecAgentAllocationDecision(
                    execution_plan_item_id=candidate.execution_plan_item_id,
                    binding=candidate.binding,
                    **self.decisions_by_item_id[candidate.execution_plan_item_id],
                )
                for candidate in request.candidates
            ]
        )

    def size_candidate(self, request):
        self.legacy_requests.append(request)
        raise AssertionError("batch allocation must not fall back to per-candidate sizing")


def test_openclaw_exec_agent_client_handoff_builds_context_and_sizes_without_prefilled_contract() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("1000000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    client = FakeExecAgentClient()
    plan = ExecutionPlan(
        id="plan-pk-openclaw",
        external_plan_id="plan-openclaw-handoff",
        source="kai-trading-core",
        market_date=datetime(2026, 5, 13, tzinfo=UTC).date(),
        mode="live",
        status="accepted",
        items=[
            ExecutionPlanItem(
                id="item-openclaw",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-openclaw",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "decider_input": {"score": 6.7},
                    "decider_output": {"action": "watch"},
                    "market_data": {"spread_bps": "12"},
                },
            )
        ],
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 1
    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.agent_id == "exec-agent"
    assert request.model == "openai/gpt-5.6-sol"
    assert request.decider_input == {"score": 6.7}
    assert request.account["available_cash"] == "800000.0000"
    audit = plan.items[0].risk_context["exec_agent_decision"]
    assert audit["agent_contract"]["context"]["risk_flags"] == [
        "normal_spread",
        "momentum_continuation",
    ]
    assert audit["final_budget"] == "350000.0000"
    assert plan.items[0].quantity_policy["quantity"] == 4
    assert audit["risk_based_quantity"] == 9
    assert audit["allocation_quantity"] == 4

    engine.dispose()


@pytest.mark.parametrize("signal_action", ("wait", "reject"))
def test_openclaw_exec_agent_candidate_projection_excludes_cross_lane_alignment_authority(
    signal_action: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-cross-lane-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("1000000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    client = FakeExecAgentClient()
    monkeypatch.setattr(
        strict_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        lambda *_, **__: {
            "summary": {"aggregate_signal_loss": True},
            "execution_authority": False,
            "strategy_learning_by_lane": {
                "signal-decider": {
                    "schema_version": "strategy-lane-learning-v1",
                    "status": "ready",
                    "consumer_eligible": True,
                    "execution_authority": True,
                    "decision_lane": "signal-decider",
                    "summary": {"lane": "signal-decider"},
                    "lessons": [{"lesson": "signal-only-loss"}],
                    "quality_gate": {
                        "status": "ready",
                        "consumer_eligible": True,
                    },
                },
                "short-term-decider": {
                    "schema_version": "strategy-lane-learning-v1",
                    "status": "ready",
                    "consumer_eligible": True,
                    "execution_authority": True,
                    "decision_lane": "short-term-decider",
                    "summary": {"lane": "short-term-decider"},
                    "lessons": [{"lesson": "short-term-own-loss"}],
                    "quality_gate": {
                        "status": "ready",
                        "consumer_eligible": True,
                    },
                },
            },
            "operational_learning_by_lane": {
                "signal-decider": {
                    "schema_version": "strategy-lane-operational-learning-v1",
                    "status": "ready",
                    "consumer_eligible": True,
                    "decision_lane": "signal-decider",
                    "failure_count": 9,
                },
                "short-term-decider": {
                    "schema_version": "strategy-lane-operational-learning-v1",
                    "status": "ready",
                    "consumer_eligible": True,
                    "decision_lane": "short-term-decider",
                    "failure_count": 1,
                },
            },
        },
    )
    risk_context: dict[str, object] = {
        "confirmation_signal": "momentum_continuation",
        "decider_input": {
            "score": 6.7,
            "alignment_gate": {"signal_decider_action": signal_action},
        },
        "decider_output": {
            "action": "watch",
            "lane": "short-term-decider",
            "signal_decider_crosscheck": {"action": signal_action},
        },
        "market_data": {
            "spread_bps": "12",
            "signal_decider_context": {"action": signal_action},
        },
        "signal_decider_alignment": f"{signal_action}_conflict_block",
        "alignment_gate": {
            "signal_decider_action": signal_action,
            "signal_decider_context": {
                "data_quality_warnings": ["signal alignment warning"]
            },
        },
        "final_enforcement_decision": signal_action,
        "requested_quantity": 7,
        "final_quantity": 0,
        "enforcement_source": "core_wait_conflict_alignment_gate",
    }
    quantity_policy: dict[str, object] = {
        "sizing_hints": {"liquidity": "normal"},
        "max_quantity": 9,
        "requested_quantity": 7,
        "final_quantity": 7,
        "final_enforcement_decision": "report_only_block_candidate",
    }
    original_risk = deepcopy(risk_context)
    original_policy = deepcopy(quantity_policy)
    item = ExecutionPlanItem(
        id=f"item-cross-lane-{signal_action}",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id=f"candidate-cross-lane-{signal_action}",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    plan = ExecutionPlan(
        id=f"plan-pk-cross-lane-{signal_action}",
        external_plan_id=f"plan-cross-lane-{signal_action}",
        source="kai-trading-core",
        market_date=date(2026, 5, 13),
        mode="live",
        status="accepted",
        items=[item],
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 1
    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.decider_output == {
        "action": "watch",
        "lane": "short-term-decider",
    }
    assert request.decider_input == {"score": 6.7}
    assert request.market_data == {"spread_bps": "12"}
    assert request.trade_outcome_feedback["summary"] == {
        "lane": "short-term-decider"
    }
    assert request.trade_outcome_feedback["execution_authority"] is True
    assert request.trade_outcome_feedback["execution_authority_scope"] == (
        "exact_decision_lane_only"
    )
    assert request.trade_outcome_feedback["lessons"] == [
        {"lesson": "short-term-own-loss"}
    ]
    assert request.trade_outcome_feedback["operational_learning_context"][
        "failure_count"
    ] == 1
    assert "signal-only-loss" not in str(request.trade_outcome_feedback)
    assert "aggregate_signal_loss" not in str(request.trade_outcome_feedback)
    projected_risk = request.candidate_context["risk_context"]
    projected_policy = request.candidate_context["quantity_policy"]
    assert projected_risk["confirmation_signal"] == "momentum_continuation"
    assert projected_policy == {
        "sizing_hints": {"liquidity": "normal"},
        "max_quantity": 9,
    }
    for forbidden_key in (
        "signal_decider_alignment",
        "alignment_gate",
        "final_enforcement_decision",
        "requested_quantity",
        "final_quantity",
        "enforcement_source",
    ):
        assert forbidden_key not in projected_risk
    assert {
        key: item.risk_context[key]
        for key in original_risk
    } == original_risk
    assert quantity_policy == original_policy

    clean_item = ExecutionPlanItem(
        id=item.id,
        plan_item_index=0,
        symbol=item.symbol,
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id=item.source_plan_item_id,
        risk_context={
            "confirmation_signal": "momentum_continuation",
            "decider_input": {"score": 6.7},
            "decider_output": {
                "action": "watch",
                "lane": "short-term-decider",
            },
            "market_data": {"spread_bps": "12"},
        },
        quantity_policy={
            "sizing_hints": {"liquidity": "normal"},
            "max_quantity": 9,
        },
    )
    clean_plan = ExecutionPlan(
        id=plan.id,
        external_plan_id=plan.external_plan_id,
        source=plan.source,
        market_date=plan.market_date,
        mode=plan.mode,
        status=plan.status,
        items=[clean_item],
    )
    baseline_candidate = strict_exec_agent._build_agent_candidate(
        plan=clean_plan,
        item=clean_item,
        source_identity=strict_exec_agent.serialize_plan_item_source_identity(
            clean_item
        ),
        valid_until=request.binding.valid_until,
        request_context_digest=request.binding.request_context_digest,
    )
    assert baseline_candidate.candidate_context == request.candidate_context
    assert baseline_candidate.binding.candidate_digest == (
        request.binding.candidate_digest
    )

    engine.dispose()


def test_exec_agent_candidate_projection_removes_alignment_sourced_legacy_max_quantity() -> None:
    item = ExecutionPlanItem(
        id="legacy-alignment-max-item",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        decision_agent_name="short-term-decider",
        risk_context={"confirmation_signal": "momentum_continuation"},
        quantity_policy={
            "sizing_hints": {"liquidity": "normal"},
            "max_quantity": 0,
            "enforcement_source": "core_wait_conflict_alignment_gate",
        },
    )
    plan = ExecutionPlan(
        external_plan_id="legacy-alignment-max-plan",
        market_date=date(2026, 5, 13),
        items=[item],
    )

    candidate_context = strict_exec_agent._agent_candidate_context(
        plan=plan,
        item=item,
    )

    assert candidate_context["quantity_policy"] == {
        "sizing_hints": {"liquidity": "normal"}
    }
    assert item.quantity_policy["max_quantity"] == 0
    assert item.quantity_policy["enforcement_source"] == (
        "core_wait_conflict_alignment_gate"
    )


def test_openclaw_exec_agent_batch_client_sees_all_candidates_and_allocates_different_budgets() -> None:
    adapter = StubBrokerAdapter(
        available_cash=Decimal("800000"),
        holdings=[BrokerAccountHolding(symbol="111111", quantity=3)],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="222222",
                side="buy",
                quantity=4,
                idempotency_key="pending-other-buy",
            )
        ],
    )
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("1000000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    client = FakeBatchExecAgentClient(
        {
            "item-openclaw-batch-a": {
                "action": "approve",
                "confidence": Decimal("0.93"),
                "quantity": 2,
                "final_budget": Decimal("100000"),
                "reference_price": Decimal("50000"),
                "target_price": Decimal("57000"),
                "stop_price": Decimal("47000"),
                "reasons": ["highest ranked candidate"],
                "risk_flags": ["normal_spread", "momentum_continuation"],
                "explanation": {"rank": 1},
            },
            "item-openclaw-batch-b": {
                "action": "approve",
                "confidence": Decimal("0.91"),
                "quantity": 4,
                "final_budget": Decimal("200000"),
                "reference_price": Decimal("50000"),
                "target_price": Decimal("57500"),
                "stop_price": Decimal("47000"),
                "reasons": ["larger portfolio allocation"],
                "risk_flags": ["deep_liquidity", "momentum_continuation"],
                "explanation": {"rank": 2},
            },
        }
    )
    plan = ExecutionPlan(
        id="plan-pk-openclaw-batch",
        external_plan_id="plan-openclaw-batch",
        source="kai-trading-core",
        market_date=datetime(2026, 5, 13, tzinfo=UTC).date(),
        mode="live",
        status="accepted",
        items=[
            ExecutionPlanItem(
                id="item-openclaw-batch-a",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-batch-a",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "decider_input": {"score": 8.9, "rank": 1},
                    "decider_output": {"action": "watch", "confidence": "0.93"},
                    "market_data": {"spread_bps": "10", "volume_score": "0.84"},
                },
            ),
            ExecutionPlanItem(
                id="item-openclaw-batch-b",
                plan_item_index=1,
                symbol="000660",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-batch-b",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "decider_input": {"score": 8.1, "rank": 2},
                    "decider_output": {"action": "watch", "confidence": "0.91"},
                    "market_data": {"spread_bps": "7", "volume_score": "0.91"},
                },
            ),
        ],
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 2
    assert result.rejected_count == 0
    assert len(client.requests) == 1
    assert client.legacy_requests == []
    request = client.requests[0]
    assert [candidate.symbol for candidate in request.candidates] == ["005930", "000660"]
    assert request.candidates[0].decider_input == {"score": 8.9, "rank": 1}
    assert request.candidates[1].decider_output == {"action": "watch", "confidence": "0.91"}
    assert request.candidates[0].market_data == {"spread_bps": "10", "volume_score": "0.84"}
    assert request.account["available_cash"] == "800000.0000"
    assert request.account["holding_count"] == 1
    assert request.portfolio["holdings_by_symbol"] == {"111111": 3}
    assert request.portfolio["open_orders"][0]["symbol"] == "222222"
    assert request.risk_caps["max_symbol_budget"] == "500000.0000"
    assert request.risk_caps["max_total_budget"] == "1000000.0000"
    for candidate in request.candidates:
        assert candidate.binding.version == "short-term-exec-agent-binding-v1"
        assert candidate.binding.plan_id == "plan-openclaw-batch"
        assert candidate.binding.symbol == candidate.symbol
        assert candidate.binding.provider == "kis"
        assert candidate.binding.asset_class == "stock"
        assert candidate.binding.venue == "KRX"
        assert len(candidate.binding.candidate_digest) == 64
        assert candidate.binding.valid_until.tzinfo is not None

    buy_items = {item.symbol: item for item in plan.items if item.intent == "next_open_buy"}
    assert buy_items["005930"].quantity_policy["quantity"] == 2
    assert buy_items["000660"].quantity_policy["quantity"] == 4
    assert buy_items["005930"].risk_context["exec_agent_decision"]["final_budget"] == "100000.0000"
    assert buy_items["000660"].risk_context["exec_agent_decision"]["final_budget"] == "200000.0000"
    assert buy_items["005930"].risk_context["exec_agent_decision"]["approved_notional"] == "100000.0000"
    assert buy_items["000660"].risk_context["exec_agent_decision"]["approved_notional"] == "200000.0000"
    assert [item.intent for item in plan.items].count("max_holding_close_sell") == 2

    engine.dispose()


def test_exec_agent_account_projection_keeps_shared_exposure_without_other_lane_identity() -> None:
    observed_at = datetime.now(UTC)
    same_lane_order = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=1,
        remaining_quantity=1,
        status="open",
        broker_order_id="same-lane-order",
        idempotency_key="same-lane-idempotency",
        limit_price=Decimal("100"),
        payload={
            "decision_agent_name": "short-term-decider",
            "entry_family_key": "short-family",
        },
    )
    other_lane_order = BrokerAccountOpenOrder(
        symbol="005930",
        side="buy",
        quantity=2,
        remaining_quantity=2,
        status="open",
        broker_order_id="signal-order",
        idempotency_key="signal-idempotency",
        limit_price=Decimal("110"),
        payload={
            "decision_agent_name": "signal-decider",
            "entry_family_key": "signal-family",
        },
    )
    item = ExecutionPlanItem(
        id="lane-account-candidate",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        status="pending",
        decision_agent_name="short-term-decider",
        source_plan_item_id="short-family",
    )
    plan = ExecutionPlan(
        external_plan_id="lane-account-plan",
        market_date=date.today(),
        items=[item],
    )
    serialized_orders = [
        strict_exec_agent._serialize_open_order(order)
        for order in (same_lane_order, other_lane_order)
    ]
    account_context = strict_exec_agent._AccountContext(
        available_cash=Decimal("10000"),
        buying_power=Decimal("10000"),
        observed_at=observed_at,
        snapshot_payload={
            "available": True,
            "observed_at": observed_at.isoformat(),
            "available_cash": "10000.0000",
            "open_orders": serialized_orders,
        },
        holdings_by_symbol={"005930": 3},
        open_orders=[same_lane_order, other_lane_order],
        unresolved_buy_reserved_notional=Decimal("320"),
        unresolved_buy_reservation_complete=True,
        holding_quantities_by_decision_lane={
            "005930": {"short-term-decider": 1, "signal-decider": 2},
        },
        holding_lane_attribution_complete_by_symbol={"005930": True},
    )

    request = strict_exec_agent._build_batch_agent_request(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[item],
        account_context=account_context,
        request_started_at=observed_at,
    )

    assert request.portfolio["holdings_by_symbol"] == {"005930": 3}
    assert request.portfolio["holding_quantities_by_decision_lane"] == {
        "005930": {"short-term-decider": 1}
    }
    for projected_orders in (
        request.account["open_orders"],
        request.portfolio["open_orders"],
    ):
        assert len(projected_orders) == 2
        assert projected_orders[0]["decision_agent_name"] == "short-term-decider"
        assert projected_orders[0]["entry_family_key"] == "short-family"
        assert projected_orders[1] == {
            "symbol": "005930",
            "side": "buy",
            "quantity": 2,
            "remaining_quantity": 2,
            "status": "open",
            "limit_price": "110.0000",
        }


def test_account_snapshot_drops_untrusted_broker_open_order_payload() -> None:
    marker = "must-never-leave-the-broker-adapter"
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("100000"),
        observed_at=datetime(2026, 9, 8, 1, tzinfo=UTC),
        source="stub:account_snapshot",
        holdings=[],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=1,
                remaining_quantity=1,
                payload={
                    "CANO": marker,
                    "nested": {"authorization": marker, "api_key": marker},
                    "provider_debug_row": marker,
                },
            )
        ],
    )

    serialized = strict_exec_agent._serialize_account_snapshot(snapshot)

    assert serialized["open_orders"] == [
        {
            "symbol": "005930",
            "side": "buy",
            "quantity": 1,
            "remaining_quantity": 1,
            "status": "open",
            "broker_order_id": None,
            "idempotency_key": None,
            "source_candidate_id": None,
            "limit_price": None,
        }
    ]
    assert marker not in str(serialized)


def test_openclaw_exec_agent_quantity_caps_budget_implied_quantity() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
        exec_agent_max_symbol_budget=Decimal("1000000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    client = FakeExecAgentClient(
        quantity=1,
        final_budget=Decimal("1000000"),
        reference_price=Decimal("50000"),
    )
    plan = ExecutionPlan(
        id="plan-pk-openclaw-cap",
        external_plan_id="plan-openclaw-cap",
        source="kai-trading-core",
        market_date=datetime(2026, 5, 13, tzinfo=UTC).date(),
        mode="live",
        status="accepted",
        items=[
            ExecutionPlanItem(
                id="item-openclaw-cap",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-openclaw-cap",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "decider_output": {"score": 9.1},
                    },
            )
        ],
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 1
    audit = plan.items[0].risk_context["exec_agent_decision"]
    assert audit["quantity"] == 1
    assert audit["agent_approved_quantity_upper_bound"] == 1
    assert audit["agent_contract"]["max_quantity"] == 1
    assert audit["approved_notional"] == "50000.0000"
    assert audit["final_budget"] == "1000000.0000"
    assert plan.items[0].quantity_policy["quantity"] == 1
    monitoring_quantities = [
        item.quantity_policy["quantity"]
        for item in plan.items
        if item.intent in {"target_sell", "stop_sell"}
    ]
    assert monitoring_quantities == [1, 1]

    engine.dispose()


def test_non_kis_short_term_candidate_is_explicitly_unsupported() -> None:
    request = execution_api_service.ExecutionPlanItemRequest(
        symbol="BTC/USD",
        side="buy",
        intent="next_open_buy",
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        decision_agent_name="short-term-decider",
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={"exchange": "ALPACA"},
    )

    assert execution_api_service._initial_plan_item_status(request) == (
        "unsupported_semantics"
    )


def test_non_kis_generated_exit_item_fails_closed() -> None:
    plan = ExecutionPlan(
        id="alpaca-protective-plan",
        external_plan_id="alpaca-protective-plan",
        source="test",
        market_date=date(2026, 5, 13),
        mode="paper",
        status="accepted",
    )
    buy_item = ExecutionPlanItem(
        id="alpaca-entry",
        plan_item_index=0,
        symbol="BTC/USD",
        side="buy",
        intent="next_open_buy",
        status="pending",
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        quantity_policy={"kind": "shares", "quantity": 1},
        risk_context={"exchange": "ALPACA"},
    )

    with pytest.raises(
        strict_exec_agent.PositionIdentityConflictError,
        match="KIS-only",
    ):
        strict_exec_agent._new_monitoring_item(
            plan=plan,
            buy_item=buy_item,
            decision={"idempotency_key": "alpaca-entry-order", "confidence": "0.9"},
            intent="stop_sell",
            quantity=1,
            next_plan_item_index=1,
            trigger_price=Decimal("59000"),
            max_sessions=3,
            exit_rule={"kind": "protective_stop"},
        )


def test_account_context_uses_candidate_exchange_for_kis_us_local_truth() -> None:
    class KisUsAccountAdapter(StubBrokerAdapter):
        def __init__(self) -> None:
            super().__init__(available_cash=Decimal("100000"))
            self.snapshot_exchanges: list[str] = []

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            del symbols, reference_prices
            assert access_token
            self.snapshot_calls += 1
            self.snapshot_exchanges.append(exchange)
            return self.snapshot

    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="kis-key",
        kis_app_secret="kis-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-position-scope-secret-at-least-32-bytes",
    )
    adapter = KisUsAccountAdapter()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="NASD",
    )
    candidate = ExecutionPlanItem(
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="next_open_buy",
        status="pending",
        provider="kis",
        venue="kis-us",
        asset_class="us_equity",
        risk_context={"exchange": "NASD"},
    )

    with Session(engine) as db:
        db.add(
            ExecutionPosition(
                mode="paper",
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="AAPL",
                quantity=2,
                average_price=Decimal("60000"),
                updated_from="test",
            )
        )
        db.commit()

        context = strict_exec_agent._load_account_context(
            settings=settings,
            db=db,
            adapter=adapter,
            candidates=[candidate],
        )

    assert adapter.snapshot_exchanges == ["NASD"]
    assert context.holdings_by_symbol == {"AAPL": 2}
    engine.dispose()


def test_account_context_folds_mixed_kis_domestic_order_routes() -> None:
    class RecordingAdapter(StubBrokerAdapter):
        def __init__(self) -> None:
            super().__init__(available_cash=Decimal("100000"))
            self.snapshot_exchanges: list[str] = []

        def get_account_snapshot(
            self,
            access_token: str,
            *,
            symbols: list[str] | None = None,
            reference_prices: dict[str, Decimal] | None = None,
            exchange: str = "KRX",
        ) -> BrokerAccountSnapshot:
            del symbols, reference_prices
            assert access_token
            self.snapshot_exchanges.append(exchange)
            return self.snapshot

    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_app_key="kis-key",
        kis_app_secret="kis-secret",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "domestic-candidate-route-secret-at-least-32-bytes"
        ),
    )
    adapter = RecordingAdapter()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    candidates = [
        ExecutionPlanItem(
            plan_item_index=index,
            symbol=symbol,
            side="buy",
            intent="next_open_buy",
            status="pending",
            provider="kis",
            venue=exchange,
            asset_class="stock",
            risk_context={"exchange": exchange},
        )
        for index, (symbol, exchange) in enumerate(
            (("005930", "KRX"), ("000660", "NXT"))
        )
    ]

    with Session(engine) as db:
        context = strict_exec_agent._load_account_context(
            settings=settings,
            db=db,
            adapter=adapter,
            candidates=candidates,
        )

    assert adapter.snapshot_exchanges == ["KRX"]
    assert context.available_cash == Decimal("100000")
    assert context.snapshot_payload.get("available") is not False
    engine.dispose()


def test_openclaw_exec_agent_unavailable_fails_closed_when_contract_missing() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    app, engine = build_test_app(adapter)
    settings = app.state.settings.model_copy(update={"exec_agent_enabled": True, "exec_agent_max_account_snapshot_age_seconds": 86400})
    app.state.settings = settings
    client = TestClient(app)
    payload = short_term_plan_payload(exec_agent_decision={"remove": "contract"})
    payload["items"][0]["risk_context"] = {"decider_output": {"score": 6.5}}

    response = client.post("/execution-plans", json=payload)

    assert response.status_code == 201
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "exec_agent_unavailable"

    engine.dispose()


def test_short_term_auto_buy_fails_closed_when_runtime_resumed_without_contract() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    resume_response = client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    assert resume_response.status_code == 200
    assert resume_response.json()["safety"]["automation_enabled"] is True
    assert app.state.settings.automation_enabled is False
    payload = short_term_plan_payload(exec_agent_decision={"remove": "contract"})
    payload["items"][0]["risk_context"] = {"decider_output": {"score": 6.5}}

    response = client.post("/execution-plans", json=payload)

    assert response.status_code == 201
    assert response.json()["exec_agent"]["evaluated_count"] == 1
    assert response.json()["exec_agent"]["approved_count"] == 0
    assert response.json()["exec_agent"]["rejected_count"] == 1
    item = client.get("/execution-plans").json()[0]["items"][0]
    assert item["status"] == "exec_agent_rejected"
    assert item["risk_context"]["exec_agent_decision"]["reason_code"] == "missing_exec_agent_contract"
    assert client.get("/execution-jobs").json() == []
    assert adapter.order_calls == []

    engine.dispose()


def test_max_holding_close_exit_is_monitored_across_days_and_submits_sell() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    client.post("/controls/resume", json={"actor": "ops", "source": "test"})
    response = client.post("/execution-plans", json=short_term_plan_payload())
    assert response.status_code == 201

    adapter.snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("500000"),
        observed_at=datetime(2026, 5, 18, 5, 49, 30, tzinfo=UTC),
        source="stub:account_snapshot",
        holdings=[BrokerAccountHolding(symbol="005930", quantity=5)],
        open_orders=[],
    )
    with app.state.db_session_factory() as session:
        entry_item = session.scalar(
            select(ExecutionPlanItem).where(
                ExecutionPlanItem.side == "buy",
                ExecutionPlanItem.symbol == "005930",
            )
        )
        assert entry_item is not None
        identity = build_position_identity(
            settings=app.state.settings,
            provider="kis",
            exchange="KRX",
        )
        family_identity = broker_orders_service._strategy_entry_family_identity(
            entry_item
        )
        assert family_identity is not None
        command_metadata = {
            broker_orders_service.STRATEGY_ENTRY_FAMILY_METADATA_KEY: family_identity
        }
        session.add(
            ExecutionPosition(
                mode=app.state.settings.runtime_mode,
                provider=identity.provider,
                account_scope=identity.account_scope,
                exchange=identity.exchange,
                symbol="005930",
                quantity=5,
                average_price=Decimal("50000"),
                updated_from="test",
            )
        )
        entry_order = BrokerOrder(
            plan_item=entry_item,
            broker_name="kis",
            mode=app.state.settings.runtime_mode,
            idempotency_key="max-holding-entry-order",
            broker_order_id="max-holding-entry-order",
            symbol="005930",
            side="buy",
            status="filled",
            quantity=5,
            limit_price=Decimal("50000"),
            venue="kis",
            asset_class="stock",
            submitted_at=datetime(2026, 5, 13, 0, 1, tzinfo=UTC),
            request_metadata={
                "exchange": "KRX",
                "execution_scope": execution_scope_payload(
                    identity=identity,
                    venue="kis",
                    asset_class="stock",
                ),
                "metadata": command_metadata,
                "original_command": {"metadata": command_metadata},
            },
        )
        session.add(entry_order)
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=entry_order.id,
                broker_fill_id="max-holding-entry-fill",
                symbol="005930",
                side="buy",
                quantity=5,
                price=Decimal("50000"),
                filled_at=datetime(2026, 5, 13, 0, 2, tzinfo=UTC),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()

    monitoring = client.post(
        "/monitoring/evaluate",
        json={
            "observed_at": "2026-05-18T14:50:00+09:00",
            "prices": [{"symbol": "005930", "price": "50000"}],
        },
    )

    assert monitoring.status_code == 200
    payload = monitoring.json()
    max_exit_items = [item for item in payload["items"] if item["intent"] == "max_holding_close_sell"]
    assert max_exit_items[0]["outcome"] == "submitted", max_exit_items[0]
    assert adapter.order_calls[-1].side == "sell"
    assert adapter.order_calls[-1].quantity == 2

    engine.dispose()


def test_sync_exec_agent_approval_binds_and_removes_policy_only_account_route() -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        quantity_policy={
            "type": "exec_agent_sized",
            "account_reference": "kis:12345678-01",
        },
    )

    strict_exec_agent._apply_approved_decision(
        settings=Settings(
            kis_account_number="12345678",
            kis_account_product_code="01",
            account_drawdown_recovery_scope_secret="route-binding-secret",
        ),
        item=item,
        decision={
            "quantity": 1,
            "idempotency_key": "sync-account-route",
            "sizing_price": "71000",
            "approved_notional": "71000",
            "confidence": "0.7",
        },
    )

    assert "account_reference" not in str(item.risk_context)
    assert "account_reference" not in str(item.quantity_policy)
    assert item.risk_context["execution_account_route"]["version"] == (
        "kis-account-route-v1"
    )


def test_signal_decider_exec_agent_sized_buy_waits_for_local_watch_sizing() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    payload = short_term_plan_payload(
        plan_id="plan-signal-agent-approved",
        source_plan_item_id="signal-agent-approved",
    )
    item_payload = payload["items"][0]
    assert isinstance(item_payload, dict)
    item_payload["decision_agent_name"] = "signal-decider"
    item_payload["quantity_policy"] = {
        "type": "exec_agent_sized",
        "budget_krw": "150000",
        "estimated_reference_price": "50000",
    }

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    response_payload = response.json()
    assert response_payload["exec_agent"]["evaluated_count"] == 0
    buy_item = next(
        item
        for item in response_payload["items"]
        if item["intent"] == "next_open_buy"
    )
    assert buy_item["status"] == "watch_sizing_pending"
    assert buy_item["quantity_policy"]["type"] == "exec_agent_sized"
    assert "kind" not in buy_item["quantity_policy"]
    assert response_payload["execution_jobs"] == []

    engine.dispose()


def test_short_term_exec_agent_does_not_trust_agent_supplied_confirmation() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    payload = short_term_plan_payload(
        exec_agent_decision=strict_exec_decision(
            "0.82",
            context={
                "confirmation_signal": "breakout_confirmed",
                "market_data": {"momentum_confirmation": True},
            },
        )
    )
    item_payload = payload["items"][0]
    assert isinstance(item_payload, dict)
    risk_context = item_payload["risk_context"]
    assert isinstance(risk_context, dict)
    risk_context.pop("confirmation_signal")
    _bind_precomputed_exec_agent_decisions(payload)

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "missing_confirmation_signal"

    engine.dispose()


def test_short_term_exec_agent_uses_only_item_evidence_for_market_quality() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("2000000"),
    )
    client = TestClient(app)
    payload = short_term_plan_payload(
        exec_agent_decision=strict_exec_decision(
            "0.82",
            max_budget="500000",
            context={
                "liquidity_score": "1.0",
                "orderbook_quality": "1.0",
                "spread_bps": "1",
            },
        )
    )
    item_payload = payload["items"][0]
    assert isinstance(item_payload, dict)
    risk_context = item_payload["risk_context"]
    assert isinstance(risk_context, dict)
    risk_context.update(
        {
            "liquidity_score": "0.20",
            "orderbook_quality": "0.20",
            "spread_bps": "120",
        }
    )
    _bind_precomputed_exec_agent_decisions(payload)

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert audit["reason_code"] == "approved"
    assert audit["risk_adjustments"]["liquidity"]["multiplier"] == "0.5000"
    assert audit["risk_adjustments"]["orderbook_quality"]["multiplier"] == "0.6000"
    assert audit["risk_adjustments"]["spread"]["multiplier"] == "0.5000"

    engine.dispose()


def test_short_term_exec_agent_does_not_let_high_confidence_relax_reward_risk() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.91",
                max_budget="100000",
                target_price="53300",
                stop_price="47000",
            )
        ),
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "reward_risk_below_threshold"
    assert audit["reward_risk"] == "1.1000"
    assert audit["required_reward_risk"] == "1.3000"

    engine.dispose()


def test_short_term_exec_agent_does_not_let_tactical_tag_relax_reward_risk() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(
        adapter,
        exec_agent_tactical_scalp_max_stop_pct=Decimal("0.02"),
    )
    client = TestClient(app)

    response = client.post(
        "/execution-plans/preview",
        json=short_term_plan_payload(
            exec_agent_decision=strict_exec_decision(
                "0.70",
                max_budget="150000",
                target_price="51000",
                stop_price="49000",
                special_tactical_scalp=True,
                target_profile="tactical_2pct",
            )
        ),
    )

    assert response.status_code == 200
    buy_item = response.json()["items"][0]
    audit = buy_item["risk_context"]["exec_agent_decision"]
    assert buy_item["status"] == "exec_agent_rejected"
    assert audit["reason_code"] == "reward_risk_below_threshold"
    assert audit["reward_risk"] == "1.0000"
    assert audit["required_reward_risk"] == "1.3000"

    engine.dispose()


def test_exec_agent_batch_response_rejects_partial_candidate_set() -> None:
    valid_until = datetime(2030, 1, 1, tzinfo=UTC)

    def candidate(item_id: str, symbol: str):
        binding = strict_exec_agent.ExecAgentDecisionBinding(
            plan_id="plan-partial-batch",
            item_id=item_id,
            symbol=symbol,
            provider="kis",
            asset_class="stock",
            venue="KRX",
            side="buy",
            intent="next_open_buy",
            candidate_digest=("1" if item_id.endswith("a") else "2") * 64,
            request_context_digest="3" * 64,
            valid_until=valid_until,
        )
        return strict_exec_agent.ExecAgentAllocationCandidate(
            execution_plan_item_id=item_id,
            symbol=symbol,
            side="buy",
            intent="next_open_buy",
            source_identity={},
            candidate_context={},
            binding=binding,
        )

    first = candidate("item-a", "005930")
    second = candidate("item-b", "000660")
    request = strict_exec_agent.ExecAgentBatchSizingRequest(
        plan_id="plan-partial-batch",
        candidates=[first, second],
        account={},
        portfolio={},
        risk_caps={},
    )
    response = strict_exec_agent.ExecAgentBatchSizingResponse(
        decisions=[
            strict_exec_agent.ExecAgentAllocationDecision(
                execution_plan_item_id="item-a",
                binding=first.binding,
                action="reject",
                confidence=Decimal("0.5"),
                reason_code="agent_rejected",
            )
        ]
    )

    with pytest.raises(
        strict_exec_agent._InvalidAgentContract,
        match="missing decisions.*item-b",
    ):
        strict_exec_agent._contracts_from_batch_response(
            response=response,
            request=request,
        )


def test_exec_agent_binding_covers_route_account_portfolio_and_risk_caps() -> None:
    request_started_at = datetime(2026, 5, 13, tzinfo=UTC)
    settings = Settings(
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_account_snapshot_age_seconds=300,
    )
    plan = ExecutionPlan(
        external_plan_id="plan-binding-context",
        market_date=date(2026, 5, 13),
        items=[
            ExecutionPlanItem(
                id="item-binding-context",
                source_plan_item_id="core-binding-context",
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                provider="kis",
                asset_class="stock",
                venue="KRX",
                decision_agent_name="short-term-decider",
                risk_context={
                    "execution_account_route": {
                        "version": "kis-account-route-v1",
                        "configured_route_fingerprint": "1" * 32,
                    },
                    "target_price": "57000",
                    "stop_price": "47000",
                },
            )
        ],
    )
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("800000"),
        buying_power=Decimal("800000"),
        observed_at=datetime(2026, 5, 13, tzinfo=UTC),
        snapshot_payload={
            "available": True,
            "observed_at": "2026-05-13T00:00:00+00:00",
            "available_cash": "800000.0000",
            "buying_power": "800000.0000",
            "holdings": [],
            "open_orders": [],
        },
        holdings_by_symbol={},
        open_orders=[],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
    )

    baseline = strict_exec_agent._build_batch_agent_request(
        settings=settings,
        plan=plan,
        candidates=plan.items,
        account_context=account,
        request_started_at=request_started_at,
    )
    richer_account = replace(
        account,
        available_cash=Decimal("700000"),
        snapshot_payload={
            **account.snapshot_payload,
            "available_cash": "700000.0000",
            "holdings": [{"symbol": "000660", "quantity": 1}],
        },
        holdings_by_symbol={"000660": 1},
    )
    changed_account = strict_exec_agent._build_batch_agent_request(
        settings=settings,
        plan=plan,
        candidates=plan.items,
        account_context=richer_account,
        request_started_at=request_started_at,
    )
    changed_caps = strict_exec_agent._build_batch_agent_request(
        settings=Settings(exec_agent_max_symbol_budget=Decimal("400000")),
        plan=plan,
        candidates=plan.items,
        account_context=account,
        request_started_at=request_started_at,
    )
    plan.items[0].risk_context["execution_account_route"] = {
        "version": "kis-account-route-v1",
        "configured_route_fingerprint": "2" * 32,
    }
    changed_route = strict_exec_agent._build_batch_agent_request(
        settings=settings,
        plan=plan,
        candidates=plan.items,
        account_context=account,
        request_started_at=request_started_at,
    )

    assert baseline.candidates[0].binding.request_context_digest != (
        changed_account.candidates[0].binding.request_context_digest
    )
    assert baseline.candidates[0].binding.request_context_digest != (
        changed_caps.candidates[0].binding.request_context_digest
    )
    assert baseline.candidates[0].binding.candidate_digest != (
        changed_route.candidates[0].binding.candidate_digest
    )


def _exec_lane_feedback_fixture(label: str) -> dict[str, object]:
    return {
        "strategy_learning_by_lane": {
            "short-term-decider": {
                "schema_version": "strategy-lane-learning-v1",
                "status": "ready",
                "consumer_eligible": True,
                "execution_authority": True,
                "decision_lane": "short-term-decider",
                "summary": {"label": label},
                "lessons": [{"lesson": label}],
                "quality_gate": {
                    "status": "ready",
                    "consumer_eligible": True,
                },
            }
        },
        "operational_learning_by_lane": {
            "short-term-decider": {
                "schema_version": "strategy-lane-operational-learning-v1",
                "status": "ready",
                "consumer_eligible": True,
                "execution_authority": True,
                "decision_lane": "short-term-decider",
                "failure_count": 1,
            }
        },
    }


def test_legacy_single_candidate_fallback_reuses_bound_batch_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        exec_agent_max_account_snapshot_age_seconds=300,
        exec_agent_watch_sizing_timeout_seconds=30,
        execution_position_account_scope="legacy-feedback-test",
    )
    item = ExecutionPlanItem(
        id="legacy-feedback-item",
        source_plan_item_id="legacy-feedback-source",
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        provider="kis",
        asset_class="stock",
        venue="KRX",
        decision_agent_name="short-term-decider",
        risk_context={"target_price": "57000", "stop_price": "47000"},
    )
    plan = ExecutionPlan(
        external_plan_id="legacy-feedback-plan",
        market_date=date(2026, 5, 13),
        items=[item],
    )
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("800000"),
        buying_power=Decimal("800000"),
        observed_at=datetime.now(UTC),
        snapshot_payload={"available": True},
        holdings_by_symbol={},
        open_orders=[],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
        execution_identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="KRX",
        ),
    )
    feedback_versions = iter(
        (
            _exec_lane_feedback_fixture("captured-a"),
            _exec_lane_feedback_fixture("later-b"),
        )
    )
    load_count = 0

    def load_feedback(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal load_count
        load_count += 1
        return next(feedback_versions)

    monkeypatch.setattr(
        strict_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        load_feedback,
    )
    batch_request = strict_exec_agent._build_batch_agent_request(
        settings=settings,
        plan=plan,
        candidates=[item],
        account_context=account,
    )

    class LegacyOnlyClient:
        def __init__(self) -> None:
            self.requests: list[strict_exec_agent.ExecAgentSizingRequest] = []

        def size_candidate(
            self,
            request: strict_exec_agent.ExecAgentSizingRequest,
        ) -> strict_exec_agent.ExecAgentSizingResponse:
            self.requests.append(request)
            return strict_exec_agent.ExecAgentSizingResponse(
                action="reject",
                confidence=Decimal("0.5"),
                reason_code="test_reject",
                binding=request.binding,
            )

    legacy_client = LegacyOnlyClient()
    strict_exec_agent._call_batch_sizing_client(
        client=legacy_client,
        request=batch_request,
        settings=settings,
        plan=plan,
        candidates=[item],
        account_context=account,
    )

    assert load_count == 1
    assert len(legacy_client.requests) == 1
    legacy_request = legacy_client.requests[0]
    assert legacy_request.trade_outcome_feedback["summary"] == {
        "label": "captured-a"
    }
    assert legacy_request.binding == batch_request.candidates[0].binding
    assert legacy_request.binding.request_context_digest == (
        batch_request.candidates[0].binding.request_context_digest
    )


def test_direct_sizing_reads_wildcard_exchange_lane_feedback_artifact(
    tmp_path,
) -> None:
    settings = Settings(
        runtime_mode="live",  # type: ignore[arg-type]
        kis_account_number="12345678",
        kis_account_product_code="01",
        execution_position_account_scope="wildcard-feedback-test",
        trade_outcome_feedback_dir=str(tmp_path),
        trade_outcome_feedback_max_age_seconds=3600,
    )
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    lane_feedback = _exec_lane_feedback_fixture("wildcard-artifact")
    execution_scope = {
        "status": "single",
        "provider": identity.provider,
        "account_scope": identity.account_scope,
        "exchange": None,
    }
    for by_lane_key in (
        "strategy_learning_by_lane",
        "operational_learning_by_lane",
    ):
        lane_feedback[by_lane_key]["short-term-decider"][
            "execution_account_scope"
        ] = execution_scope
    payload = attach_artifact_integrity(
        {
            "version": trade_outcome_feedback_service.REPORT_VERSION,
            "schema_version": trade_outcome_feedback_service.REPORT_VERSION,
            "mode": "live",
            "generated_at": datetime.now(UTC).isoformat(),
            "execution_account_scope": execution_scope,
            "quality_gate": {
                "status": "ready",
                "consumer_eligible": True,
            },
            "strategy_learning_by_lane": lane_feedback[
                "strategy_learning_by_lane"
            ],
            "operational_learning_by_lane": lane_feedback[
                "operational_learning_by_lane"
            ],
            "operational_learning_context": {
                "schema_version": "operational-learning-context-v1",
                "status": "ready",
                "consumer_eligible": True,
                "execution_authority": False,
                "scope": execution_scope,
                "allowed_effects": ["delay", "downsize", "reject"],
                "prohibited_effects": ["approve", "increase_size"],
            },
            "prompt_snippet": {},
        }
    )
    (tmp_path / "latest.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("800000"),
        buying_power=Decimal("800000"),
        observed_at=datetime.now(UTC),
        snapshot_payload={"available": True},
        holdings_by_symbol={},
        open_orders=[],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
        execution_identity=identity,
    )

    loaded = strict_exec_agent._strategy_lane_trade_outcome_feedback(
        settings=settings,
        account_context=account,
        decision_lane="short-term-decider",
    )

    if loaded["status"] != "ready":
        pytest.fail(json.dumps(loaded, sort_keys=True))
    assert loaded["summary"] == {"label": "wildcard-artifact"}


def test_direct_sizing_never_loads_global_feedback_without_execution_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_global_load(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("unscoped global feedback must not be loaded")

    monkeypatch.setattr(
        strict_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        forbidden_global_load,
    )
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("800000"),
        buying_power=Decimal("800000"),
        observed_at=datetime.now(UTC),
        snapshot_payload={"available": True},
        holdings_by_symbol={},
        open_orders=[],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
    )

    loaded = strict_exec_agent._strategy_lane_trade_outcome_feedback(
        settings=Settings(),
        account_context=account,
        decision_lane="short-term-decider",
    )

    assert loaded["status"] == "unavailable"
    assert loaded["reason"] == "strategy_lane_learning_unavailable"


@pytest.mark.parametrize("offset_seconds", [-301, 31])
def test_sync_exec_agent_rejects_stale_or_future_account_snapshot(
    offset_seconds: int,
) -> None:
    assert strict_exec_agent._account_snapshot_is_stale(
        settings=Settings(
            exec_agent_enabled=True,
            exec_agent_max_account_snapshot_age_seconds=300,
            account_drawdown_recovery_max_snapshot_future_skew_seconds=30,
        ),
        observed_at=datetime.now(UTC) + timedelta(seconds=offset_seconds),
    )


def test_sync_exec_agent_binding_cannot_extend_a_299_second_old_snapshot() -> None:
    request_started_at = datetime(2026, 5, 13, 0, 0, tzinfo=UTC)
    item = ExecutionPlanItem(
        id="sync-old-snapshot-item",
        source_plan_item_id="core-sync-old-snapshot-item",
        symbol="005930",
        side="buy",
        intent="next_open_buy",
        provider="kis",
        asset_class="stock",
        venue="KRX",
        decision_agent_name="short-term-decider",
        risk_context={"target_price": "57000", "stop_price": "47000"},
    )
    plan = ExecutionPlan(
        external_plan_id="sync-old-snapshot-plan",
        market_date=date(2026, 5, 13),
        items=[item],
    )
    account = strict_exec_agent._AccountContext(
        available_cash=Decimal("800000"),
        buying_power=Decimal("800000"),
        observed_at=request_started_at - timedelta(seconds=299),
        snapshot_payload={"available": True},
        holdings_by_symbol={},
        open_orders=[],
        unresolved_buy_reserved_notional=Decimal("0"),
        unresolved_buy_reservation_complete=True,
    )

    request = strict_exec_agent._build_batch_agent_request(
        settings=Settings(
            exec_agent_max_account_snapshot_age_seconds=300,
            exec_agent_watch_sizing_timeout_seconds=900,
        ),
        plan=plan,
        candidates=[item],
        account_context=account,
        request_started_at=request_started_at,
    )

    assert request.candidates[0].binding.valid_until == (
        request_started_at + timedelta(seconds=1)
    )


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        pytest.param(
            {"candidate_digest": "0" * 64},
            "binding mismatch",
            id="different-candidate-digest",
        ),
        pytest.param(
            {"valid_until": datetime(2020, 1, 1, tzinfo=UTC)},
            "binding expired",
            id="expired",
        ),
        pytest.param(
            {"symbol": "000660"},
            "binding mismatch",
            id="replayed-for-other-symbol",
        ),
        pytest.param(None, "binding is missing", id="missing"),
    ],
)
def test_openclaw_exec_agent_batch_response_requires_exact_current_binding(
    mutation: dict[str, object] | None,
    expected_error: str,
) -> None:
    class MutatingBindingClient(FakeBatchExecAgentClient):
        def size_candidates(self, request):
            response = super().size_candidates(request)
            decision = response.decisions[0]
            decision.binding = (
                None
                if mutation is None
                else decision.binding.model_copy(update=mutation)
            )
            return response

    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    plan = ExecutionPlan(
        id="plan-binding-pk",
        external_plan_id="plan-binding",
        source="kai-trading-core",
        market_date=date(2026, 5, 13),
        mode="live",
        status="accepted",
        items=[
            ExecutionPlanItem(
                id="item-binding",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="core-item-binding",
                risk_context={
                    "confirmation_signal": "momentum_continuation",
                    "decider_output": {"action": "watch"},
                },
            )
        ],
    )
    client = MutatingBindingClient(
        {
            "item-binding": {
                "action": "approve",
                "confidence": Decimal("0.91"),
                "quantity": 2,
                "final_budget": Decimal("100000"),
                "reference_price": Decimal("50000"),
                "target_price": Decimal("57000"),
                "stop_price": Decimal("47000"),
            }
        }
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 0
    assert result.rejected_count == 1
    assert plan.items[0].status == "exec_agent_rejected"
    audit = plan.items[0].risk_context["exec_agent_decision"]
    assert audit["reason_code"] == "invalid_agent_output"
    assert expected_error in audit["reason"]
    engine.dispose()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        pytest.param("missing", "binding is missing", id="missing"),
        pytest.param("expired", "binding expired", id="expired"),
        pytest.param("replayed", "binding mismatch", id="replayed"),
    ],
)
def test_precomputed_exec_agent_contract_requires_current_identity_binding(
    mutation: str,
    expected_error: str,
) -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    payload = short_term_plan_payload()
    item = payload["items"][0]
    assert isinstance(item, dict)
    risk_context = item["risk_context"]
    assert isinstance(risk_context, dict)
    decision = risk_context["exec_agent_decision"]
    assert isinstance(decision, dict)
    binding = decision.get("binding")
    assert isinstance(binding, dict)
    if mutation == "missing":
        decision.pop("binding")
    elif mutation == "expired":
        binding["valid_until"] = "2020-01-01T00:00:00Z"
    else:
        binding["symbol"] = "000660"

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    audit = response.json()["items"][0]["risk_context"]["exec_agent_decision"]
    assert audit["reason_code"] == "invalid_agent_output"
    assert expected_error in audit["reason"]
    engine.dispose()


def test_conflicting_precomputed_exec_agent_contracts_fail_closed() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("500000"))
    app, engine = build_test_app(adapter)
    client = TestClient(app)
    payload = short_term_plan_payload()
    item = payload["items"][0]
    assert isinstance(item, dict)
    risk_context = item["risk_context"]
    assert isinstance(risk_context, dict)
    policy_contract = deepcopy(risk_context["exec_agent_decision"])
    policy_contract["confidence"] = "0.81"
    item["quantity_policy"] = {"exec_agent_decision": policy_contract}

    response = client.post("/execution-plans/preview", json=payload)

    assert response.status_code == 200
    audit = response.json()["items"][0]["risk_context"]["exec_agent_decision"]
    assert audit["reason_code"] == "invalid_agent_output"
    assert "conflicting precomputed" in audit["reason"]
    engine.dispose()


def test_live_exec_agent_rejects_precomputed_contract_without_request_context_binding() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
        execution_position_account_scope="exec-agent-test",
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
        exec_agent_enabled=True,
        exec_agent_max_account_snapshot_age_seconds=86400,
        exec_agent_max_symbol_budget=Decimal("500000"),
        exec_agent_max_total_budget=Decimal("1000000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    precomputed_decision = strict_exec_decision(
        "0.93",
        max_budget="120000",
        context={"source": "precomputed", "contract_id": "durable-contract-1"},
        target_price="57000",
        stop_price="47000",
    )
    precomputed_item_payload = {
        "symbol": "005930",
        "side": "buy",
        "intent": "next_open_buy",
        "time_in_force": None,
        "decision_agent_name": "short-term-decider",
        "source_plan_item_id": "candidate-mixed-precomputed",
        "risk_context": {
            "confirmation_signal": "momentum_continuation",
            "exec_agent_decision": precomputed_decision,
        },
    }
    precomputed_decision["binding"] = (
        strict_exec_agent.build_precomputed_exec_agent_binding(
            plan_id="plan-openclaw-mixed",
            market_date="2026-05-13",
            strategy_run_id=None,
            item_payload=precomputed_item_payload,
            valid_until=datetime(2030, 1, 1, tzinfo=UTC),
        )
    )
    client = FakeBatchExecAgentClient(
        {
            "item-openclaw-mixed-missing": {
                "action": "approve",
                "confidence": Decimal("0.91"),
                "quantity": 4,
                "final_budget": Decimal("200000"),
                "reference_price": Decimal("50000"),
                "target_price": Decimal("57500"),
                "stop_price": Decimal("47000"),
                "reasons": ["sized by live OpenClaw batch"],
                "risk_flags": ["deep_liquidity", "momentum_continuation"],
                "explanation": {"source": "openclaw"},
            },
        }
    )
    plan = ExecutionPlan(
        id="plan-pk-openclaw-mixed",
        external_plan_id="plan-openclaw-mixed",
        source="kai-trading-core",
        market_date=datetime(2026, 5, 13, tzinfo=UTC).date(),
        mode="live",
        status="accepted",
        items=[
            ExecutionPlanItem(
                id="item-openclaw-mixed-precomputed",
                plan_item_index=0,
                symbol="005930",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-mixed-precomputed",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "exec_agent_decision": precomputed_decision,
                    },
            ),
            ExecutionPlanItem(
                id="item-openclaw-mixed-missing",
                plan_item_index=1,
                symbol="000660",
                side="buy",
                intent="next_open_buy",
                status="pending",
                decision_agent_name="short-term-decider",
                source_plan_item_id="candidate-mixed-missing",
                    risk_context={
                        "confirmation_signal": "momentum_continuation",
                        "decider_output": {"score": 8.1},
                    },
            ),
        ],
    )

    with session_factory() as session:
        result = strict_exec_agent.apply_short_term_exec_agent_decisions(
            settings=settings,
            db=session,
            adapter=adapter,
            plan=plan,
            exec_agent_client=client,
        )

    assert result.approved_count == 1
    assert result.rejected_count == 1
    assert len(client.requests) == 1
    assert [candidate.execution_plan_item_id for candidate in client.requests[0].candidates] == [
        "item-openclaw-mixed-missing"
    ]
    assert [candidate.symbol for candidate in client.requests[0].candidates] == ["000660"]
    assert client.legacy_requests == []

    buy_items = {item.symbol: item for item in plan.items if item.intent == "next_open_buy"}
    precomputed_audit = buy_items["005930"].risk_context["exec_agent_decision"]
    missing_audit = buy_items["000660"].risk_context["exec_agent_decision"]
    assert precomputed_audit["reason_code"] == "invalid_agent_output"
    assert "lacks authoritative request context" in precomputed_audit["reason"]
    assert (
        missing_audit["agent_contract"]["context"]["openclaw_agent_id"]
        == strict_exec_agent.OPENCLAW_EXEC_AGENT_ID
    )
    assert missing_audit["final_budget"] == "200000.0000"

    engine.dispose()


def test_exec_agent_local_buy_reservations_are_exact_account_scoped() -> None:
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
    current_identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    foreign_identity = build_position_identity(
        settings=foreign_settings,
        provider="kis",
        exchange="KRX",
    )

    with session_factory() as session:
        for suffix, identity in (("current", current_identity), ("foreign", foreign_identity)):
            session.add(
                BrokerOrder(
                    id=f"{suffix}-open-buy",
                    broker_name="kis",
                    mode="paper",
                    idempotency_key=f"{suffix}-open-buy",
                    broker_order_id=f"{suffix}-open-buy",
                    symbol="005930",
                    side="buy",
                    status="submitted",
                    quantity=1,
                    limit_price=Decimal("100000"),
                    venue="KRX",
                    asset_class="stock",
                    request_metadata={
                        "execution_scope": execution_scope_payload(
                            identity=identity,
                            venue="KRX",
                            asset_class="stock",
                        )
                    },
                )
            )
        session.commit()

        orders = strict_exec_agent._load_local_open_orders(
            settings=settings,
            db=session,
            provider="kis",
            exchange="KRX",
            broker_account_id=None,
        )

    assert [order.payload["broker_order_pk"] for order in orders] == [
        "current-open-buy"
    ]
    engine.dispose()
