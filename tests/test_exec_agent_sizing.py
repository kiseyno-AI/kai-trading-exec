from __future__ import annotations

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
from app.models.runtime import BrokerOrder, ExecutionJob, ExecutionPlan, ExecutionPlanItem, ExecutionPosition
from app.services import exec_agent as strict_exec_agent
from app.services import monitoring as monitoring_service
from app.services import short_term_exec_agent as legacy_exec_agent


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

    def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
        assert access_token
        self.snapshot_calls += 1
        return self.snapshot

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
    runtime_mode: str = "live",
    exec_agent_max_symbol_budget: Decimal = Decimal("300000"),
    exec_agent_max_total_budget: Decimal = Decimal("600000"),
    exec_agent_max_daily_new_buy_notional: Decimal = Decimal("2000000"),
    exec_agent_max_trade_risk_pct: Decimal = Decimal("0.015"),
    exec_agent_stop_loss_pct: Decimal = Decimal("0.10"),
    exec_agent_tactical_scalp_max_stop_pct: Decimal = Decimal("0.015"),
    vol_corr_sizing_enabled: bool = False,
):
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=runtime_mode,  # type: ignore[arg-type]
        kis_app_key="key",
        kis_app_secret="secret",
        kis_account_number="12345678",
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


def short_term_plan_payload(
    *,
    plan_id: str = "plan-short-term-agent-1",
    source_plan_item_id: str = "watcher-candidate-1",
    exec_agent_decision: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "plan_id": plan_id,
        "source": "kai-trading-core",
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


def multi_short_term_plan_payload(
    *,
    plan_id: str = "plan-short-term-agent-multi",
    candidates: list[tuple[str, str, dict[str, object]]],
) -> dict[str, object]:
    return {
        "plan_id": plan_id,
        "source": "kai-trading-core",
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
                "risk_context": {"exec_agent_decision": exec_agent_decision},
            }
            for index, (symbol, source_plan_item_id, exec_agent_decision) in enumerate(candidates, start=1)
        ],
    }


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

    response = client.post(
        "/execution-plans/preview",
        json=multi_short_term_plan_payload(
            candidates=[
                ("005930", "candidate-clean", strict_exec_decision("0.82")),
                (
                    "000660",
                    "candidate-risky",
                    strict_exec_decision(
                        "0.82",
                        target_price="57000",
                        stop_price="45000",
                        context={
                            "liquidity_score": "0.45",
                            "spread_bps": "80",
                            "orderbook_quality": "0.55",
                        },
                    ),
                ),
            ]
        ),
    )

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
    response = client.post("/execution-plans", json=short_term_plan_payload())

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
    assert items_by_intent["max_holding_close_sell"]["risk_context"]["close_exit_date"] == "2026-05-18"

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["completed_job_count"] == 1
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

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["completed_job_count"] == 1
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

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["completed_job_count"] == 0
    assert adapter.order_calls == []
    with app.state.db_session_factory() as session:
        blocked_order = session.scalar(select(BrokerOrder))
        assert blocked_order is not None
        assert blocked_order.status == "rejected"
        assert blocked_order.last_error_code == "account_drawdown_snapshot_unavailable"

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


def test_short_term_exec_agent_approves_existing_holding_add_on_when_guardrails_pass() -> None:
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
            exec_agent_decision=strict_exec_decision("0.82", max_budget="100000")
        ),
    )

    assert response.status_code == 200
    item = response.json()["items"][0]
    audit = item["risk_context"]["exec_agent_decision"]
    assert item["status"] == "next_open_buy_scheduled"
    assert item["quantity_policy"]["quantity"] == 2
    assert audit["reason_code"] == "approved"
    assert audit["existing_holding"] == 3
    assert audit["existing_symbol_exposure"] == "150000.0000"
    assert audit["exposure_after_trade"] == "250000.0000"
    assert audit["max_symbol_exposure"] == "400000.0000"
    assert adapter.order_calls == []

    engine.dispose()


def test_short_term_exec_agent_rejects_existing_holding_add_on_when_exposure_cap_exceeded() -> None:
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
    assert audit["reason_code"] == "symbol_exposure_cap_exceeded"
    assert audit["existing_holding"] == 3
    assert audit["exposure_after_trade"] is None
    assert audit["max_symbol_exposure"] == "100000.0000"
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


def test_short_term_exec_agent_accepts_high_confidence_relaxed_reward_risk() -> None:
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
    assert item["status"] == "next_open_buy_scheduled"
    assert item["quantity_policy"]["quantity"] == 1
    assert audit["reason_code"] == "approved"
    assert audit["reward_risk"] == "1.1000"
    assert audit["required_reward_risk"] == "1.1000"

    engine.dispose()


def test_short_term_exec_agent_accepts_tagged_tactical_two_pct_target() -> None:
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
    payload = response.json()
    buy_item = payload["items"][0]
    target_items = [item for item in payload["items"] if item["intent"] == "target_sell"]
    audit = buy_item["risk_context"]["exec_agent_decision"]
    assert buy_item["status"] == "next_open_buy_scheduled"
    assert audit["reason_code"] == "approved"
    assert audit["reward_risk"] == "1.0000"
    assert audit["required_reward_risk"] == "1.0000"
    assert audit["target_profile"] == "tactical_2pct"
    assert audit["target_tag"] == "target_profile:tactical_2pct"
    assert len(target_items) == 1
    assert target_items[0]["trigger_price"] == "51000.0000"
    exit_rule = target_items[0]["risk_context"]["exit_rule"]
    assert exit_rule["target_profile"] == "tactical_2pct"
    assert exit_rule["target_tag"] == "tactical_2pct:final"
    assert exit_rule["actual_trigger_pct"] == "0.0200"

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

    dispatch = client.post("/execution-jobs/dispatch")

    assert dispatch.status_code == 200
    assert dispatch.json()["blocked"] is True
    assert dispatch.json()["block_reason"] == "paused"
    assert adapter.order_calls == []
    assert client.get("/orders").json() == []

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
    assert request.model == "openai-codex/gpt-5.5"
    assert request.decider_input == {"score": 6.7}
    assert request.account["available_cash"] == "800000.0000"
    audit = plan.items[0].risk_context["exec_agent_decision"]
    assert audit["agent_contract"]["context"]["risk_flags"] == [
        "normal_spread",
        "momentum_continuation",
    ]
    assert audit["final_budget"] == "350000.0000"
    assert plan.items[0].quantity_policy["quantity"] == 4
    assert audit["risk_based_quantity"] == 6
    assert audit["allocation_quantity"] == 4

    engine.dispose()


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

    buy_items = {item.symbol: item for item in plan.items if item.intent == "next_open_buy"}
    assert buy_items["005930"].quantity_policy["quantity"] == 2
    assert buy_items["000660"].quantity_policy["quantity"] == 4
    assert buy_items["005930"].risk_context["exec_agent_decision"]["final_budget"] == "100000.0000"
    assert buy_items["000660"].risk_context["exec_agent_decision"]["final_budget"] == "200000.0000"
    assert buy_items["005930"].risk_context["exec_agent_decision"]["approved_notional"] == "100000.0000"
    assert buy_items["000660"].risk_context["exec_agent_decision"]["approved_notional"] == "200000.0000"
    assert [item.intent for item in plan.items].count("max_holding_close_sell") == 2

    engine.dispose()


def test_openclaw_exec_agent_batch_allocation_preserves_precomputed_contracts() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("800000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
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
                risk_context={"exec_agent_decision": precomputed_decision},
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
                risk_context={"decider_output": {"score": 8.1}},
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
    assert [candidate.execution_plan_item_id for candidate in client.requests[0].candidates] == [
        "item-openclaw-mixed-missing"
    ]
    assert [candidate.symbol for candidate in client.requests[0].candidates] == ["000660"]
    assert client.legacy_requests == []

    buy_items = {item.symbol: item for item in plan.items if item.intent == "next_open_buy"}
    precomputed_audit = buy_items["005930"].risk_context["exec_agent_decision"]
    missing_audit = buy_items["000660"].risk_context["exec_agent_decision"]
    expected_precomputed_contract = strict_exec_agent.ShortTermExecAgentContract.model_validate(
        precomputed_decision
    ).model_dump(mode="json")
    assert precomputed_audit["agent_contract"] == expected_precomputed_contract
    assert precomputed_audit["final_budget"] == "120000.0000"
    assert precomputed_audit["target_price"] == "57000.0000"
    assert precomputed_audit["agent_contract"]["context"] == {
        "confirmation_signal": "momentum_continuation",
        "source": "precomputed",
        "contract_id": "durable-contract-1",
    }
    assert (
        missing_audit["agent_contract"]["context"]["openclaw_agent_id"]
        == strict_exec_agent.OPENCLAW_EXEC_AGENT_ID
    )
    assert missing_audit["final_budget"] == "200000.0000"

    engine.dispose()


def test_openclaw_exec_agent_quantity_caps_budget_implied_quantity() -> None:
    adapter = StubBrokerAdapter(available_cash=Decimal("2000000"))
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",  # type: ignore[arg-type]
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
                risk_context={"decider_output": {"score": 9.1}},
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
        observed_at=datetime(2026, 5, 18, 14, 45, tzinfo=UTC),
        source="stub:account_snapshot",
        holdings=[BrokerAccountHolding(symbol="005930", quantity=5)],
        open_orders=[],
    )
    with app.state.db_session_factory() as session:
        session.add(
            ExecutionPosition(
                mode="live",
                symbol="005930",
                quantity=5,
                average_price=Decimal("50000"),
                updated_from="test",
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
    assert max_exit_items[0]["outcome"] == "submitted"
    assert adapter.order_calls[-1].side == "sell"
    assert adapter.order_calls[-1].quantity == 2

    engine.dispose()
