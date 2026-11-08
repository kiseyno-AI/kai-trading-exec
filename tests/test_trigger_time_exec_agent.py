from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import subprocess
from threading import Event, Thread
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.broker.base import (
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerConfig,
    BrokerCurrentPrice,
    BrokerErrorClassification,
    BrokerIntradayLiquiditySnapshot,
    BrokerLiveMarketContext,
)
from app.api.schemas.execution import MonitoringPriceRequest
from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    ExecutionFill,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPosition,
    MonitoringCheckpoint,
    RuntimeControlState,
)
from app.services import monitoring, trigger_time_exec_agent
from app.services.exec_agent_approval import (
    assess_buy_economics,
    configured_execution_cost_evidence,
)
from app.services.json_compaction import json_payload_hash
from app.services.position_identity import build_position_identity, execution_scope_payload
from app.services.execution_account_route import bind_kis_account_route
from openclaw_contract_test_support import (
    matching_runtime_contract_snapshot,
    verified_runtime_contract_proof,
)
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StubBrokerAdapter,
    _seed_exact_strategy_family_for_sell_items,
    _seed_exact_strategy_family_in_session,
    _use_historical_broker_boundary,
    build_order_placement,
    build_test_app,
    build_token_grant,
)


@pytest.mark.parametrize(
    "malformed_field",
    [
        None,
        "bid",
        "ask",
        "spread",
        "spread_bps",
        "orderbook_depth",
        "orderbook_imbalance",
        "volume",
        "turnover",
        "day_high",
        "day_low",
    ],
)
def test_live_market_quality_reports_malformed_numbers_without_raising(
    malformed_field: str | None,
) -> None:
    fields = {
        "bid": "100",
        "ask": "101",
        "spread": "1",
        "spread_bps": "100",
        "orderbook_depth": "10",
        "orderbook_imbalance": "0.2",
        "volume": "1000",
        "turnover": "100000",
        "day_high": "110",
        "day_low": "90",
    }
    if malformed_field is not None:
        fields[malformed_field] = "not-a-number"

    assert trigger_time_exec_agent._live_execution_quality_invalid_fields(
        **fields,
        require_intraday_range=True,
        session_type="regular",
        normal_ordering_available=True,
    ) == ([] if malformed_field is None else [malformed_field])


@pytest.mark.parametrize("exchange", ["KRX", "KOSPI", "KOSDAQ", "NXT", "SOR"])
def test_live_market_buy_authority_gate_covers_all_domestic_kis_routes(
    exchange: str,
) -> None:
    item = ExecutionPlanItem(
        id=f"domestic-authority-{exchange.lower()}",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue=exchange,
        asset_class="stock",
        risk_context={"exchange": exchange},
    )

    assert trigger_time_exec_agent._live_market_buy_authority_gate_applies(
        settings=Settings(runtime_mode="live"),
        item=item,
        session_type="regular",
    )


def test_live_market_buy_authority_gate_resolves_legacy_krx_and_preserves_us_scope() -> None:
    legacy_krx = ExecutionPlanItem(
        id="legacy-domestic-authority",
        plan_item_index=0,
        symbol="0017J0",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="kis",
        asset_class=None,
        risk_context={},
    )
    kis_us = ExecutionPlanItem(
        id="kis-us-authority",
        plan_item_index=0,
        symbol="AAPL",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_active",
        provider="kis",
        venue="kis",
        asset_class="us_equity",
        risk_context={},
    )

    settings = Settings(runtime_mode="live")
    assert trigger_time_exec_agent._live_market_buy_authority_gate_applies(
        settings=settings,
        item=legacy_krx,
        session_type="regular",
    )
    assert not trigger_time_exec_agent._live_market_buy_authority_gate_applies(
        settings=settings,
        item=kis_us,
        session_type="regular",
    )


_THREAD_COORDINATION_TIMEOUT_SECONDS = 15.0


def _trigger_lane_feedback_fixture() -> dict[str, object]:
    return {
        "execution_authority": False,
        "summary": {"aggregate_audit_only": True},
        "strategy_learning_by_lane": {
            lane: {
                "schema_version": "strategy-lane-learning-v1",
                "status": "ready",
                "consumer_eligible": True,
                "execution_authority": True,
                "decision_lane": lane,
                "summary": {"lane": lane},
                "lessons": [{"lesson": f"{lane}-lesson"}],
                "quality_gate": {
                    "status": "ready",
                    "consumer_eligible": True,
                    "blocked_reasons": [],
                },
            }
            for lane in ("signal-decider", "short-term-decider")
        },
        "operational_learning_by_lane": {
            lane: {
                "schema_version": "strategy-lane-operational-learning-v1",
                "status": "ready",
                "consumer_eligible": True,
                "decision_lane": lane,
                "failure_count": 1,
                "classification_counts": {f"{lane}-failure": 1},
                "allowed_effects": ["delay", "downsize", "reject"],
                "prohibited_effects": ["approve", "increase_size"],
            }
            for lane in ("signal-decider", "short-term-decider")
        },
    }


@pytest.mark.parametrize(
    "decision_lane",
    ("signal-decider", "short-term-decider"),
)
def test_trigger_time_request_uses_only_exact_lane_learning(
    monkeypatch: pytest.MonkeyPatch,
    decision_lane: str,
) -> None:
    item = ExecutionPlanItem(
        id=f"trigger-feedback-{decision_lane}",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_buy_triggered",
        decision_agent_name=decision_lane,
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    ExecutionPlan(
        external_plan_id=f"trigger-feedback-plan-{decision_lane}",
        market_date=date(2026, 10, 26),
        items=[item],
    )
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        lambda *_, **__: _trigger_lane_feedback_fixture(),
    )

    payload = trigger_time_exec_agent._request_payload(
        settings=Settings(runtime_mode="paper"),
        item=item,
        side="buy",
        market_context={},
        account_context={
            "execution_account_scope": {
                "provider": "kis",
                "account_scope": "opaque-account",
            }
        },
        peer_context={},
        intended_quantity=1,
        remaining_quantity=None,
    )

    feedback = payload["trade_outcome_feedback"]
    assert payload["decision_lane"] == decision_lane
    assert feedback["summary"] == {"lane": decision_lane}
    assert feedback["lessons"] == [{"lesson": f"{decision_lane}-lesson"}]
    assert feedback["operational_learning_context"][
        "classification_counts"
    ] == {f"{decision_lane}-failure": 1}
    assert "aggregate_audit_only" not in json.dumps(feedback)


def test_trigger_time_never_uses_global_feedback_without_account_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = ExecutionPlanItem(
        id="trigger-feedback-missing-scope",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_buy_triggered",
        decision_agent_name="short-term-decider",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    ExecutionPlan(
        external_plan_id="trigger-feedback-missing-scope-plan",
        market_date=date(2026, 10, 26),
        items=[item],
    )

    def forbidden_global_load(*_args, **_kwargs):
        raise AssertionError("unscoped global feedback must not be loaded")

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        forbidden_global_load,
    )

    payload = trigger_time_exec_agent._request_payload(
        settings=Settings(runtime_mode="paper"),
        item=item,
        side="buy",
        market_context={},
        account_context={},
        peer_context={},
        intended_quantity=1,
        remaining_quantity=None,
    )

    assert payload["trade_outcome_feedback"]["status"] == "unavailable"
    assert payload["trade_outcome_feedback"]["reason"] == (
        "strategy_lane_learning_unavailable"
    )


@pytest.mark.parametrize("signal_action", ("wait", "reject"))
def test_trigger_time_request_sanitizes_cross_lane_alignment_authority(
    monkeypatch: pytest.MonkeyPatch,
    signal_action: str,
) -> None:
    base_risk_context: dict[str, object] = {
        "exchange": "KRX",
        "target_price": "76000",
        "stop_price": "69000",
        "rationale": "short-term breakout",
        "decider_output": {"action": "buy", "lane": "short-term-decider"},
        "decider_handoff_context": {"lane": "short-term-decider", "score": 8},
    }
    base_quantity_policy: dict[str, object] = {
        "kind": "shares",
        "quantity": 7,
        "max_quantity": 9,
    }
    item = ExecutionPlanItem(
        id=f"trigger-sanitizer-{signal_action}",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_buy_triggered",
        decision_agent_name="short-term-decider",
        risk_context=deepcopy(base_risk_context),
        quantity_policy=deepcopy(base_quantity_policy),
    )
    ExecutionPlan(
        external_plan_id=f"trigger-sanitizer-plan-{signal_action}",
        market_date=date(2026, 10, 26),
        items=[item],
    )
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_latest_trade_outcome_feedback_snippet",
        lambda *_, **__: _trigger_lane_feedback_fixture(),
    )
    request_kwargs = {
        "settings": Settings(runtime_mode="paper"),
        "item": item,
        "side": "buy",
        "market_context": {},
        "account_context": {
            "execution_account_scope": {
                "provider": "kis",
                "account_scope": "opaque-account",
            }
        },
        "peer_context": {},
        "intended_quantity": 7,
        "remaining_quantity": None,
    }
    baseline = trigger_time_exec_agent._request_payload(**request_kwargs)

    item.risk_context = {
        **deepcopy(base_risk_context),
        "signal_decider_alignment": f"{signal_action}_conflict_block",
        "alignment_gate": {
            "signal_decider_action": signal_action,
            "signal_decider_context": {
                "data_quality_warnings": ["signal-only warning"]
            },
        },
        "final_enforcement_decision": "report_only_block_candidate",
        "requested_quantity": 7,
        "final_quantity": 0,
        "enforcement_source": "core_wait_conflict_alignment_gate",
    }
    item.quantity_policy = {
        **deepcopy(base_quantity_policy),
        "final_enforcement_decision": "report_only_block_candidate",
        "requested_quantity": 7,
        "final_quantity": 7,
    }
    contaminated = trigger_time_exec_agent._request_payload(**request_kwargs)

    assert contaminated["signal_context"] == baseline["signal_context"]
    assert json_payload_hash(contaminated) == json_payload_hash(baseline)
    assert item.risk_context["signal_decider_alignment"] == (
        f"{signal_action}_conflict_block"
    )
    assert item.quantity_policy["final_enforcement_decision"] == (
        "report_only_block_candidate"
    )


def _fresh_normal_core_market_context(*, now: datetime | None) -> dict[str, object]:
    observed_at = now or datetime.now(UTC)
    return {
        "market_context_status": "ready",
        "market_context_as_of": observed_at.isoformat(),
        "market_context_fetched_at": observed_at.isoformat(),
        "market_regime_context": {
            "source": "kai-trading-core:/market/snapshot",
            "markets": {
                "KOSPI": {
                    "market": "KOSPI",
                    "market_date": observed_at.astimezone(
                        ZoneInfo("Asia/Seoul")
                    ).date().isoformat(),
                    "observed_at": observed_at.isoformat(),
                    "source": "kai-trading-core:/market/snapshot",
                    "up_count": 650,
                    "down_count": 250,
                    "flat_count": 50,
                    "market_symbol_count": 950,
                    "up_ratio": "0.684211",
                    "advance_decline_ratio": "2.600000",
                }
            },
        },
    }


def _build_trigger_agent_app(
    *,
    monkeypatch: pytest.MonkeyPatch,
    agent_responses: list[dict[str, object]] | None = None,
    broker_adapter: StubBrokerAdapter | None = None,
    settings_overrides: dict[str, object] | None = None,
    account_snapshot_observed_at: datetime | None = None,
):
    captured_requests: list[dict[str, object]] = []
    responses = list(agent_responses or [])
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "load_live_market_regime_context",
        lambda **kwargs: _fresh_normal_core_market_context(now=kwargs.get("now")),
    )
    monkeypatch.setattr(
        monitoring,
        "load_live_market_regime_context",
        lambda **kwargs: (
            _fresh_normal_core_market_context(now=kwargs.get("now"))
            if kwargs["settings"].runtime_mode == "live"
            else {}
        ),
    )

    def fake_agent_request(*, settings: Settings, payload: dict[str, object]) -> dict[str, object]:
        captured_requests.append(payload)
        if not responses:
            raise AssertionError("unexpected trigger-time exec-agent call")
        response = dict(responses.pop(0))
        plan = payload.get("plan")
        signal_context = payload.get("signal_context")
        assert isinstance(plan, dict)
        assert isinstance(signal_context, dict)
        response.setdefault(
            "version",
            trigger_time_exec_agent.TRIGGER_TIME_EXEC_AGENT_VERSION,
        )
        response.setdefault("plan_id", plan["plan_id"])
        response.setdefault(
            "execution_plan_item_id",
            signal_context["execution_plan_item_id"],
        )
        response.setdefault("symbol", signal_context["symbol"])
        provider, model = settings.exec_agent_model.split("/", 1)
        requested_session_key = "trigger-exec-test-requested-session"
        reported_session_id = "trigger-exec-test-session"
        request_sha256 = json_payload_hash(
            trigger_time_exec_agent._json_safe(payload)
        )
        response.setdefault(
            "_openclaw_runtime_provenance",
            {
                "metadata_status": "reported",
                "actual_agent_id": settings.exec_agent_id,
                "actual_provider": provider,
                "actual_model": model,
                "actual_model_ref": settings.exec_agent_model,
                "session_id": reported_session_id,
                "requested_session_key": requested_session_key,
                "request_sha256": request_sha256,
                "runtime_contract": verified_runtime_contract_proof(
                    expected_model_ref=settings.exec_agent_model,
                    requested_session_key=requested_session_key,
                    reported_session_id=reported_session_id,
                    request_sha256=request_sha256,
                ),
            },
        )
        return response

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "_request_exec_agent_decision",
        fake_agent_request,
    )
    adapter = broker_adapter or StubBrokerAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-trigger-agent-1"),
    )
    if account_snapshot_observed_at is not None and adapter.account_snapshot is None:
        adapter.account_snapshot = BrokerAccountSnapshot(
            available_cash=Decimal("1000000.00"),
            observed_at=account_snapshot_observed_at,
            source="stub:account_snapshot",
            total_asset_value=Decimal("10000000"),
            previous_total_asset_value=Decimal("10000000"),
            asset_change_amount=Decimal("0"),
            unrealized_pnl=Decimal("0"),
        )
    settings_payload: dict[str, object] = {
        "database_url": "sqlite:///:memory:",
        "runtime_mode": "paper",
        "kis_app_key": "paper-key",
        "kis_app_secret": "paper-secret",
        "kis_account_number": "12345678",
        "account_drawdown_recovery_scope_secret": (
            "route-binding-secret-for-trigger-tests-32-bytes"
        ),
        "exec_agent_enabled": True,
        "exec_agent_trigger_time_enabled": True,
        "exec_agent_trigger_time_sell_enabled": True,
        "exec_agent_max_position_allocation_pct": Decimal("0.30"),
        "exec_agent_max_symbol_total_exposure_pct": Decimal("0.30"),
    }
    settings_payload.update(settings_overrides or {})
    app, engine = build_test_app(
        settings=Settings(**settings_payload),
        broker_adapter=adapter,
    )
    client = TestClient(app)
    resume = client.post(
        "/controls/resume",
        json={"actor": "ops", "source": "test", "reason": "enable trigger-time agent tests"},
    )
    assert resume.status_code == 200
    if settings_payload["runtime_mode"] == "live":
        observation_at = account_snapshot_observed_at or datetime(
            2026, 6, 5, 0, 30, tzinfo=UTC
        )
        with app.state.db_session_factory() as session:
            state = session.get(RuntimeControlState, "global")
            assert state is not None
            context = dict(state.hourly_risk_off_context or {})
            context.update(
                {
                    "last_market_observation_id": "trigger-agent-fixture",
                    "last_market_observation_market_date": observation_at.astimezone(
                        ZoneInfo("Asia/Seoul")
                    ).date().isoformat(),
                    "last_market_observation_received_at": observation_at.isoformat(),
                    "last_market_observation_source_observed_at": observation_at.isoformat(),
                    "last_market_observation_risk_flags": 0,
                }
            )
            state.hourly_risk_off_context = context
            session.commit()
    return app, engine, client, adapter, captured_requests


def _scoped_execution_position(app, **values: object) -> ExecutionPosition:
    identity = build_position_identity(
        settings=app.state.settings,
        provider="kis",
        exchange="KRX",
    )
    return ExecutionPosition(
        provider=identity.provider,
        account_scope=identity.account_scope,
        exchange=identity.exchange,
        **values,
    )


def _bind_plan_item_to_app_account(app, item: ExecutionPlanItem) -> None:
    risk_context, quantity_policy = bind_kis_account_route(
        settings=app.state.settings,
        provider="kis",
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
    )
    item.provider = "kis"
    item.risk_context = risk_context
    item.quantity_policy = quantity_policy


def test_signal_context_prefers_compact_handoff_digest_and_hashes_legacy_payload() -> None:
    supplied_digest = "ab" * 32
    legacy_handoff = {"nested": {"candidate": "005930", "score": 8}}
    item = ExecutionPlanItem(
        id="trigger-context-digest-item",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        risk_context={
            "decider_handoff_context_digest": supplied_digest,
        },
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    compact_context = trigger_time_exec_agent._signal_context(item=item, side="buy")

    assert compact_context["decider_handoff_context_hash"] == supplied_digest

    item.risk_context = {"decider_handoff_context": legacy_handoff}
    legacy_context = trigger_time_exec_agent._signal_context(item=item, side="buy")

    assert legacy_context["decider_handoff_context_hash"] == json_payload_hash(
        legacy_handoff
    )

    item.risk_context = {"decider_handoff_context_digest": supplied_digest.upper()}
    invalid_context = trigger_time_exec_agent._signal_context(item=item, side="buy")

    assert invalid_context["decider_handoff_context_hash"] is None


def test_peer_context_only_contains_current_account_and_exchange() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="trigger-peer-route-secret-for-tests",
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
    us_risk, us_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "NASD"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    def item(
        *,
        item_id: str,
        symbol: str,
        status: str,
        risk_context: dict[str, object] | None,
        quantity_policy: dict[str, object] | None,
        decision_lane: str | None = "short-term-decider",
    ) -> ExecutionPlanItem:
        return ExecutionPlanItem(
            id=item_id,
            plan_item_index=0,
            symbol=symbol,
            side="buy",
            intent="intraday_watch_buy",
            status=status,
            provider="kis",
            asset_class="stock",
            decision_agent_name=decision_lane,
            risk_context=risk_context,
            quantity_policy=quantity_policy,
        )

    current = item(
        item_id="current-trigger-item",
        symbol="005930",
        status=trigger_time_exec_agent.AGENT_PENDING_STATUS,
        risk_context=current_risk,
        quantity_policy=current_policy,
    )
    current_peer = item(
        item_id="current-trigger-peer",
        symbol="000660",
        status=trigger_time_exec_agent.AGENT_PENDING_STATUS,
        risk_context=current_risk,
        quantity_policy=current_policy,
    )
    opposite_lane_peer = item(
        item_id="opposite-lane-trigger-peer",
        symbol="051910",
        status=trigger_time_exec_agent.AGENT_DELAY_STATUS,
        risk_context=current_risk,
        quantity_policy=current_policy,
        decision_lane="signal-decider",
    )
    foreign_peer = item(
        item_id="foreign-trigger-peer",
        symbol="005930",
        status=trigger_time_exec_agent.AGENT_DELAY_STATUS,
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    )
    other_exchange_peer = item(
        item_id="other-exchange-trigger-peer",
        symbol="AAPL",
        status=trigger_time_exec_agent.AGENT_PENDING_STATUS,
        risk_context=us_risk,
        quantity_policy=us_policy,
    )
    legacy_peer = item(
        item_id="legacy-trigger-peer",
        symbol="035420",
        status=trigger_time_exec_agent.AGENT_PENDING_STATUS,
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    try:
        with session_factory() as db:
            db.add(
                ExecutionPlan(
                    external_plan_id="trigger-peer-scope-plan",
                    source="test",
                    mode="paper",
                    items=[
                        current,
                        current_peer,
                        opposite_lane_peer,
                        foreign_peer,
                        other_exchange_peer,
                        legacy_peer,
                    ],
                )
            )
            db.commit()

            context = trigger_time_exec_agent._peer_context(
                settings=settings,
                db=db,
                item=current,
            )

            assert context["active_candidate_count"] == 1
            assert context["pending_agent_decision_count"] == 1
            assert context["agent_delayed_count"] == 0
            assert [candidate["execution_plan_item_id"] for candidate in context["active_candidates"]] == [
                current_peer.id
            ]
    finally:
        engine.dispose()


def _high_volatility_context(
    *,
    technical_context_as_of: str | None = "2026-06-05",
    override: dict[str, object] | None = None,
) -> dict[str, object]:
    context: dict[str, object] = {
        "short_term_entry_quality": {
            "atr_pct_14": "17.2160",
            "return_5d": "44.5993",
            "trend_state": "range",
        },
        "technical_context_as_of": technical_context_as_of,
    }
    if technical_context_as_of is None:
        context.pop("technical_context_as_of")
    if override is not None:
        context["high_volatility_chase_override"] = override
    return context


def _create_intraday_watch_buy(
    client: TestClient,
    *,
    plan_id: str,
    max_safe_notional_krw: str = "300000",
    risk_context_extra: dict[str, object] | None = None,
    decision_agent_name: str | None = "signal-decider",
    quantity_policy: dict[str, object] | None = None,
) -> str:
    risk_context = {
        "exchange": "KRX",
        "target_price": "72420.00",
        "stop_price": "70000.00",
        "trigger_direction": "at_or_above",
        "decider_output": {"rationale": "trigger-time test candidate"},
    }
    if risk_context_extra:
        risk_context.update(risk_context_extra)
    item: dict[str, object] = {
        "symbol": "005930",
        "side": "buy",
        "intent": "intraday_watch_buy",
        "quantity_policy": quantity_policy
        or {
            "type": "trigger_time_exec_agent_sized",
            "budget_krw": "0",
            "max_safe_notional_krw": max_safe_notional_krw,
        },
        "trigger_price": "71000.00",
        "time_in_force": "day",
        "risk_context": risk_context,
    }
    if decision_agent_name is not None:
        item["decision_agent_name"] = decision_agent_name
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": plan_id,
            "source": "test-fixture",
            "market_date": "2026-06-05",
            "items": [item],
        },
    )
    assert response.status_code == 201
    return response.json()["persisted_execution_item_ids"][0]


def _live_price_payload(
    price: str = "71050.00",
    *,
    day_low: str | None = None,
    day_high: str | None = None,
    turnover: str = "88750000000",
    observed_at: datetime | None = None,
) -> dict[str, object]:
    reference = Decimal(price)
    payload: dict[str, object] = {
        "symbol": "005930",
        "price": price,
        "freshness_source": "kis_exchange_orderbook_time",
        "bid": str(reference - Decimal("5")),
        "ask": str(reference + Decimal("5")),
        "spread": "10.00",
        "spread_bps": "1.41",
        "orderbook_depth": "450000000",
        "orderbook_imbalance": "0.08",
        "volume": "1250000",
        "turnover": turnover,
        "session_type": "regular",
        "normal_ordering_available": True,
        "provenance": {"source": "test_live_market_quality"},
    }
    if observed_at is not None:
        serialized_observed_at = observed_at.isoformat()
        payload["observed_at"] = serialized_observed_at
        payload["provenance"] = {
            "source": "test_live_market_quality",
            "orderbook": {
                "freshness_source": "kis_exchange_orderbook_time",
                "observed_at": serialized_observed_at,
            },
            "field_sources": {
                side: {
                    "freshness_source": "kis_exchange_orderbook_time",
                    "observed_at": serialized_observed_at,
                }
                for side in ("bid", "ask")
            },
        }
    if day_low is not None:
        payload["day_low"] = day_low
    if day_high is not None:
        payload["day_high"] = day_high
    return payload


def _structured_dynamic_gate_context(
    *,
    observed_at: datetime,
) -> dict[str, object]:
    expires_at = datetime(2026, 6, 5, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    codes = [
        "short_term_liquidity_floor_block_delegated_to_exec_watch",
        "short_term_reward_risk_ratio_block_delegated_to_exec_watch",
        "transaction_cost_evidence_pending_live_delegated_to_exec_watch",
    ]
    common = {
        "policy_version": "core_delegated_gate_policy_v1",
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    return {
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
        "execution_feasibility_context": {
            "status": "pending_live_cost_confirmation",
            "expected_slippage_bps": None,
            "expected_slippage_basis": "unavailable",
        },
    }


def test_trigger_time_chase_buy_high_atr_extended_return_vetoes_before_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        settings_overrides={"watch_buy_high_volatility_chase_veto_enabled": True},
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-high-volatility-chase-veto",
        risk_context_extra=_high_volatility_context(),
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high chase with extreme extension",
            "observed_at": "2026-06-05T00:30:00Z",
            "prices": [_live_price_payload(day_low="70000.00", day_high="72500.00")],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    item_payload = payload["items"][0]
    assert item_payload["outcome"] == "blocked"
    assert item_payload["block_reason"] == "short_term_high_volatility_chase_block"
    assert item_payload["risk_decision"]["rule_id"] == "high_volatility_chase_veto"
    assert item_payload["risk_decision"]["return_5d"] == "44.5993"
    assert captured == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        decision = item.risk_context["last_high_volatility_chase_decision"]
        assert decision["outcome"] == "veto"
        assert decision["technical_context"]["status"] == "fresh"
        assert item.risk_context["last_monitoring_event"]["reason"] == "short_term_high_volatility_chase_block"

    engine.dispose()


def test_trigger_time_chase_buy_legacy_high_atr_extended_return_blocks_when_new_veto_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        settings_overrides={"watch_buy_high_volatility_chase_veto_enabled": False},
    )
    _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-legacy-high-atr-ret5-block",
        risk_context_extra=_high_volatility_context(),
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "legacy high ATR extended ret5 block remains active",
            "observed_at": "2026-06-05T00:30:00Z",
            "prices": [_live_price_payload(day_low="70000.00", day_high="72500.00")],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    item_payload = payload["items"][0]
    assert item_payload["outcome"] == "blocked"
    assert item_payload["block_reason"] == "short_term_high_atr_extended_ret5_block"
    assert item_payload["risk_decision"]["outcome"] == "legacy_veto"
    assert item_payload["risk_decision"]["live_veto_enabled"] is False
    assert captured == []
    assert broker_adapter.order_calls == []

    engine.dispose()


@pytest.mark.parametrize("technical_context_as_of", [None, "2026-06-01"])
def test_trigger_time_chase_buy_stale_or_missing_technical_context_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    technical_context_as_of: str | None,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        settings_overrides={"watch_buy_high_volatility_chase_veto_enabled": True},
    )
    _create_intraday_watch_buy(
        client,
        plan_id=f"plan-trigger-agent-stale-tech-{technical_context_as_of or 'missing'}",
        risk_context_extra=_high_volatility_context(technical_context_as_of=technical_context_as_of),
    )

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "stale technical context should fail closed",
            "observed_at": "2026-06-05T00:30:00Z",
            "prices": [_live_price_payload(day_low="70000.00", day_high="71100.00")],
        },
    )

    assert response.status_code == 200
    item_payload = response.json()["items"][0]
    assert item_payload["outcome"] == "blocked"
    assert item_payload["block_reason"] == "stale_or_missing_technical_context_for_chase"
    assert item_payload["risk_decision"]["technical_context"]["status"] in {"missing", "stale"}
    assert captured == []
    assert broker_adapter.order_calls == []

    engine.dispose()


def test_trigger_time_chase_buy_technical_context_age_uses_execution_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        settings_overrides={
            "watch_buy_high_volatility_chase_veto_enabled": True,
            "watch_buy_high_volatility_chase_max_context_age_days": 1,
        },
    )
    _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-stale-current-session-tech",
        risk_context_extra=_high_volatility_context(technical_context_as_of="2026-06-05"),
    )
    price = _live_price_payload(day_low="70000.00", day_high="72500.00")
    price["observed_at"] = "2026-06-09T00:30:00Z"

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "watch item survived into later execution session",
            "observed_at": "2026-06-09T00:30:00Z",
            "prices": [price],
        },
    )

    assert response.status_code == 200
    item_payload = response.json()["items"][0]
    assert item_payload["outcome"] == "blocked"
    assert item_payload["block_reason"] == "stale_or_missing_technical_context_for_chase"
    assert item_payload["risk_decision"]["technical_context"]["status"] == "stale"
    assert item_payload["risk_decision"]["technical_context"]["age_days"] == 4
    assert captured == []
    assert broker_adapter.order_calls == []

    engine.dispose()


def test_high_volatility_chase_rule_does_not_affect_sell_side_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
        settings_overrides={
            "watch_buy_high_volatility_chase_veto_enabled": True,
            "exec_agent_trigger_time_sell_enabled": False,
        },
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-sell-stop-high-volatility-context",
            "source": "test-fixture",
            "market_date": "2026-06-05",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 2},
                    "trigger_price": "70000.00",
                    "time_in_force": "day",
                    "risk_context": _high_volatility_context(),
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    stop_item_id = plan_response.json()["persisted_execution_item_ids"][0]
    _seed_exact_strategy_family_for_sell_items(
        app,
        sell_item_ids=[stop_item_id],
        quantity=2,
        entry_price=Decimal("71000"),
    )
    with app.state.db_session_factory() as session:
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=2,
                average_price=Decimal("71000"),
                updated_from="test",
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "sell stop should remain deterministic",
            "prices": [_live_price_payload(price="69900.00")],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert broker_adapter.order_calls[0].execution_plan_item_id == stop_item_id
    assert broker_adapter.order_calls[0].side == "sell"
    assert captured == []

    engine.dispose()


@pytest.mark.parametrize(
    ("direction", "observed_price", "expected_limit_price"),
    [
        ("at_or_below", "70900.00", Decimal("70905.00")),
        ("at_or_above", "71100.00", Decimal("71105.00")),
    ],
)
def test_trigger_time_buy_agent_is_bypassed_for_persisted_price_line_buy(
    monkeypatch: pytest.MonkeyPatch,
    direction: str,
    observed_price: str,
    expected_limit_price: Decimal,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    response = client.post(
        "/execution-plans",
        json={
            "plan_id": f"plan-trigger-agent-{direction}-entry-line-bypass",
            "source": "test-fixture",
            "market_date": "2026-06-05",
            "items": [
                    {
                            "symbol": "005930",
                            "side": "buy",
                            "intent": "intraday_watch_buy",
                            "decision_agent_name": "signal-decider",
                        "quantity_policy": {"kind": "shares", "quantity": 2},
                    "trigger_price": "71000.00",
                    "chase_limit_price": "71200.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "72420.00",
                        "stop_price": "70000.00",
                        "trigger_direction": direction,
                    },
                }
            ],
        },
    )
    assert response.status_code == 201
    item_id = response.json()["persisted_execution_item_ids"][0]

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "persisted price line should not call trigger-time agent",
            "prices": [_live_price_payload(price=observed_price)],
        },
    )

    assert result.status_code == 200
    payload = result.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["direction"] == direction
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].quantity == 2
    assert broker_adapter.order_calls[0].limit_price == expected_limit_price

    engine.dispose()


@pytest.mark.parametrize(
    "legacy_status",
    (
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
        "agent_rejected",
    ),
)
def test_retired_buy_agent_state_rearms_with_deterministic_sizing(
    monkeypatch: pytest.MonkeyPatch,
    legacy_status: str,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id=f"plan-retired-buy-agent-{legacy_status}",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = legacy_status
        item.quantity_policy = {
            "type": "trigger_time_exec_agent_sized",
            "kind": "trigger_time_exec_agent_sized",
            "max_budget_krw": "200000",
            "max_quantity": 2,
            "quantity": 9,
        }
        item.risk_context = {
            **item.risk_context,
            "last_trigger_time_exec_agent_interaction": {
                "decision": {
                    "decision": "execute_now",
                    "approved_quantity": 9,
                }
            },
        }
        session.commit()

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "retired BUY agent state must rearm deterministically",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )

    assert result.status_code == 200
    payload = result.json()
    assert payload["submitted_order_count"] == 1, json.dumps(payload)
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert 0 < broker_adapter.order_calls[0].quantity < 9
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.quantity_policy["kind"] == "risk_based"
        assert item.quantity_policy["max_quantity"] == 2
        assert "quantity" not in item.quantity_policy
        assert "last_trigger_time_exec_agent_interaction" not in item.risk_context

    engine.dispose()


def test_unrelated_agent_rejection_is_not_rearmed_as_a_buy_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-unrelated-agent-rejection",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = "agent_rejected"
        risk_context = dict(item.risk_context)
        risk_context.pop("last_trigger_time_exec_agent_interaction", None)
        risk_context.pop("trigger_time_exec_agent_interactions", None)
        item.risk_context = risk_context
        session.commit()

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "unrelated rejected authority must remain terminal",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )

    assert result.status_code == 200
    assert result.json()["submitted_order_count"] == 0
    assert captured == []
    assert broker_adapter.order_calls == []
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "agent_rejected"

    engine.dispose()


def test_retired_rejected_buy_is_restored_to_autonomous_symbol_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-retired-rejected-autonomous-membership",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = "agent_rejected"
        item.risk_context = {
            **item.risk_context,
            "last_trigger_time_exec_agent_interaction": {
                "decision": {"decision": "execute_now", "quantity": 2}
            },
        }
        session.commit()

        symbols = monitoring.list_autonomous_monitoring_symbols(
            settings=app.state.settings,
            db=session,
            current_market_date=date(2026, 6, 5),
        )

    assert symbols == ["005930"]
    assert captured == []
    assert broker_adapter.order_calls == []
    engine.dispose()


def test_duplicate_retired_rejected_buys_rearm_only_newest_route_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    older_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-retired-rejected-duplicate-older",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    newer_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-retired-rejected-duplicate-newer",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    with app.state.db_session_factory() as session:
        for item_id in (older_id, newer_id):
            item = session.get(ExecutionPlanItem, item_id)
            assert item is not None
            item.status = "agent_rejected"
            item.risk_context = {
                **item.risk_context,
                "last_trigger_time_exec_agent_interaction": {
                    "decision": {"decision": "execute_now", "quantity": 9}
                },
            }
        session.commit()

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "retired duplicate authority must canonicalize before rearm",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )

    assert result.status_code == 200
    assert result.json()["submitted_order_count"] == 1
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == newer_id
    second_result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "superseded retired duplicate must not reappear next cycle",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )
    assert second_result.status_code == 200
    assert second_result.json()["submitted_order_count"] == 0
    assert len(broker_adapter.order_calls) == 1
    with app.state.db_session_factory() as session:
        older = session.get(ExecutionPlanItem, older_id)
        assert older is not None
        assert older.status == "superseded"
    engine.dispose()


def test_newer_retired_rejected_buy_supersedes_older_active_route_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    older_active_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-active-duplicate-older",
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    newer_rejected_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-retired-rejected-duplicate-newer-than-active",
        quantity_policy={"kind": "shares", "quantity": 2},
    )
    with app.state.db_session_factory() as session:
        newer = session.get(ExecutionPlanItem, newer_rejected_id)
        assert newer is not None
        newer.status = "agent_rejected"
        newer.risk_context = {
            **newer.risk_context,
            "last_trigger_time_exec_agent_interaction": {
                "decision": {"decision": "execute_now", "quantity": 9}
            },
        }
        session.commit()

    first_result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "newest route authority must supersede older active watch",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )
    assert first_result.status_code == 200
    assert first_result.json()["submitted_order_count"] == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == newer_rejected_id

    second_result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "older active duplicate must stay superseded",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )
    assert second_result.status_code == 200
    assert second_result.json()["submitted_order_count"] == 0
    assert len(broker_adapter.order_calls) == 1
    assert captured == []
    with app.state.db_session_factory() as session:
        older = session.get(ExecutionPlanItem, older_active_id)
        assert older is not None
        assert older.status == "superseded"
    engine.dispose()


def test_retired_buy_agent_state_preserves_fixed_share_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-retired-buy-agent-fixed-shares",
        quantity_policy={"kind": "shares", "quantity": 2, "max_quantity": 2},
    )
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        item.status = "agent_approved_pending_submit"
        original_policy = dict(item.quantity_policy)
        item.risk_context = {
            **item.risk_context,
            "last_trigger_time_exec_agent_interaction": {
                "decision": {"decision": "execute_now", "quantity": 9}
            },
        }
        session.commit()

    result = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "fixed share authority must survive retired BUY-agent cleanup",
            "prices": [_live_price_payload(price="71100.00")],
        },
    )

    assert result.status_code == 200
    assert result.json()["submitted_order_count"] == 1
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].quantity == 2
    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.quantity_policy == original_policy
        assert "last_trigger_time_exec_agent_interaction" not in item.risk_context

    engine.dispose()


def test_trigger_time_buy_agent_is_bypassed_for_intraday_pullback_below_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-trigger-agent-pullback-bypass",
        risk_context_extra={"exec_agent_watch_sizing_fallback_allowed": True},
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross above chase persists before pullback",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="70000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0
    assert crossed.json()["items"][0]["block_reason"] in {
        "price_above_chase_limit",
        "watch_buy_intraday_range_chase_block",
    }

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high crossed trigger and pulled back below target buy",
            "prices": [
                _live_price_payload(
                    price="70900.00",
                    day_low="70000.00",
                    day_high="71400.00",
                )
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["triggered_item_count"] == 1
    assert payload["submitted_order_count"] == 1, json.dumps(payload)
    assert payload["items"][0]["outcome"] == "submitted"
    assert payload["items"][0]["trigger_reason"] == "intraday_watch_buy_day_high_cross"
    assert captured == []

    assert len(broker_adapter.order_calls) == 1
    order_call = broker_adapter.order_calls[0]
    assert order_call.execution_plan_item_id == item_id
    assert order_call.side == "buy"
    assert order_call.limit_price == Decimal("70905.00")

    engine.dispose()


def test_stale_trigger_reclaim_blocks_cross_pullback_buy_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-stale-trigger-reclaim-blocked",
        risk_context_extra={"exec_agent_watch_sizing_fallback_allowed": True},
        decision_agent_name="short-term-decider",
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross persists before stale pullback",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high crossed trigger but pulled back too far",
            "prices": [
                _live_price_payload(
                    price="68865.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 0
    assert payload["blocked_item_count"] == 1
    assert payload["items"][0]["outcome"] == "blocked"
    assert payload["items"][0]["block_reason"] == "stale_trigger_reclaim_required"
    assert captured == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_buy_triggered_blocked"
        event = item.risk_context["last_monitoring_event"]
        assert event["event"] == "watch_buy_triggered_blocked"
        assert event["reason"] == "stale_trigger_reclaim_required"
        assert event["details"]["trigger_price"] == "71000.0000"
        assert event["details"]["current_reference_price"] == "68870.0000"
        assert event["details"]["reference_price"] == "68870.0000"
        assert event["details"]["stale_reference_price"] == "68870.0000"
        assert event["details"]["threshold_pct"] == "3.0000"

    board = client.get("/runtime/status")
    assert board.status_code == 200
    blocked_candidates = board.json()["liveness"]["monitoring"]["segments"]["blocked_short_term_candidates"]["items"]
    blocked_item = next(candidate for candidate in blocked_candidates if candidate["symbol"] == "005930")
    assert blocked_item["execution_plan_item_id"] == item_id
    assert blocked_item["blocker_reason"] == "stale_trigger_reclaim_required"

    engine.dispose()


def test_stale_trigger_reclaim_allows_cross_pullback_buy_within_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-stale-trigger-reclaim-within-threshold",
        risk_context_extra={
            "exec_agent_watch_sizing_fallback_allowed": True,
            "stop_price": "68000.00",
        },
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross persists before within-threshold pullback",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "day high crossed trigger and stayed inside stale threshold",
            "prices": [
                _live_price_payload(
                    price="68900.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["submitted_order_count"] == 1
    assert payload["items"][0]["outcome"] == "submitted"
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id
    assert broker_adapter.order_calls[0].limit_price == Decimal("68905.00")

    engine.dispose()


def test_stale_trigger_reclaim_allows_explicit_pullback_strategy_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    item_id = _create_intraday_watch_buy(
        client,
        plan_id="plan-stale-trigger-reclaim-pullback-profile",
        risk_context_extra={
            "exec_agent_watch_sizing_fallback_allowed": True,
            "strategy_profile": "pullback_reclaim",
            "stop_price": "68000.00",
        },
        quantity_policy={"kind": "shares", "quantity": 2},
    )

    crossed = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "live trigger cross persists before pullback-profile entry",
            "prices": [
                _live_price_payload(
                    price="71300.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )
    assert crossed.status_code == 200
    assert crossed.json()["submitted_order_count"] == 0

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "pullback strategy is allowed to buy below old breakout line",
            "prices": [
                _live_price_payload(
                    price="68865.00",
                    day_low="68000.00",
                    day_high="71400.00",
                )
            ],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == item_id

    with app.state.db_session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        check = item.risk_context["last_stale_trigger_reclaim_check"]
        assert check["bypass_reason"] == "pullback_strategy_profile"
        assert check["current_reference_price"] == "68870.0000"

    engine.dispose()


@pytest.mark.parametrize(
    "initial_target_status",
    ["target_sell_monitoring", "target_sell_triggered_blocked"],
)
def test_trigger_time_sell_agent_can_adjust_quantity_limit_and_stop(
    monkeypatch: pytest.MonkeyPatch,
    initial_target_status: str,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "sell",
                "quantity": 2,
                "limit_price": "125500.00",
                "updated_stop_price": "119000.00",
                "reason": "take partial profit and raise stop",
                "risk_notes": ["lock gains"],
                "confidence": "0.80",
                "context_staleness_ok": True,
            }
        ],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-sell-adjust",
            "source": "test-fixture",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "118000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]
    _seed_exact_strategy_family_for_sell_items(
        app,
        sell_item_ids=[target_item_id, stop_item_id],
        quantity=5,
        entry_price=Decimal("120000.0000"),
    )
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert target_item is not None
        target_item.status = initial_target_status
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "target sell with trigger-time agent",
            "prices": [{"symbol": "005930", "price": "125100.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1
    assert len(captured) == 1
    assert captured[0]["signal_context"]["side"] == "sell"
    assert captured[0]["execution_context"]["intended_quantity"] == 5
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == target_item_id
    assert broker_adapter.order_calls[0].quantity == 2
    assert broker_adapter.order_calls[0].limit_price == Decimal("125500.00")

    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "broker_order_submitted"
        assert stop_item.trigger_price == Decimal("119000.0000")
        assert target_item.risk_context["updated_stop_price"] == "119000.0000"

    engine.dispose()


@pytest.mark.parametrize(
    ("agent_status", "stale_agent_context", "expected_recovery_reason"),
    [
        (
            "triggered_pending_agent",
            {
                "last_monitoring_event": {
                    "event": "trigger_time_exec_agent_pending",
                    "recorded_at": "2026-06-05T00:28:00Z",
                },
            },
            "trigger_time_exec_agent_pending_timeout",
        ),
        (
            "agent_delay",
            {
                "last_monitoring_event": {
                    "event": "trigger_time_exec_agent_delay",
                    "recorded_at": "2026-06-05T00:28:00Z",
                },
                "last_trigger_time_exec_agent_interaction": {
                    "recorded_at": "2026-06-05T00:28:00Z",
                    "decision": {
                        "decision": "delay",
                        "valid_until": "2026-06-05T00:29:00Z",
                    },
                },
            },
            "trigger_time_exec_agent_delay_expired",
        ),
        (
            "agent_approved_pending_submit",
            {
                "last_monitoring_event": {
                    "event": "trigger_time_exec_agent_approved",
                    "recorded_at": "2026-06-05T00:28:00Z",
                },
                "last_trigger_time_exec_agent_interaction": {
                    "recorded_at": "2026-06-05T00:28:00Z",
                    "decision": {"decision": "execute_now"},
                },
            },
            "trigger_time_exec_agent_approved_pending_submit_timeout",
        ),
    ],
)
def test_expired_agent_active_target_sell_is_recovered_and_reapproved(
    monkeypatch: pytest.MonkeyPatch,
    agent_status: str,
    stale_agent_context: dict[str, object],
    expected_recovery_reason: str,
) -> None:
    _use_historical_broker_boundary(monkeypatch)
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "sell",
                "quantity": 2,
                "reason": "fresh agent pass approves the recovered target",
                "risk_notes": ["recovered stale durable state"],
                "confidence": "0.80",
                "context_staleness_ok": True,
            }
        ],
        settings_overrides={"exec_agent_trigger_time_timeout_seconds": 90},
        account_snapshot_observed_at=datetime(2026, 6, 5, 0, 30, tzinfo=UTC),
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": f"plan-recover-{agent_status}",
            "source": "test-fixture",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "125000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "118000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]
    _seed_exact_strategy_family_for_sell_items(
        app,
        sell_item_ids=[target_item_id, stop_item_id],
        quantity=5,
        entry_price=Decimal("120000.0000"),
    )
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert target_item is not None
        target_item.status = agent_status
        target_item.risk_context = {
            **dict(target_item.risk_context or {}),
            **stale_agent_context,
        }
        session.add(
            _scoped_execution_position(
                app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime(2026, 6, 5, 0, 29, tzinfo=UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "recover expired trigger-time agent state",
            "observed_at": "2026-06-05T00:30:00Z",
            "prices": [{"symbol": "005930", "price": "125100.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1, json.dumps(response.json())
    assert len(captured) == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == target_item_id
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        assert target_item is not None
        assert target_item.status == "broker_order_submitted"
        recovery_events = [
            event
            for event in target_item.risk_context["monitoring_events"]
            if event.get("reason") == expected_recovery_reason
        ]
        assert recovery_events[-1]["previous_status"] == agent_status
        assert recovery_events[-1]["reason"] == expected_recovery_reason

    engine.dispose()


def test_trigger_time_partial_target_sell_cannot_loosen_existing_protective_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[
            {
                "decision": "execute_now",
                "side": "sell",
                "quantity": 2,
                "updated_stop_price": "60000.00",
                "reason": "take partial profit but widen the remaining-position stop",
                "risk_notes": ["unsafe stop relaxation"],
                "confidence": "0.80",
                "context_staleness_ok": True,
            }
        ],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-sell-stop-relaxation",
            "source": "test-fixture",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 4},
                    "trigger_price": "75000.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "exit_rule": {"kind": "partial_take_profit", "stage": "first"},
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "70000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]
    with app.state.db_session_factory() as session:
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("65000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "partial target may tighten but never loosen the stop",
            "prices": [{"symbol": "005930", "price": "75100.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 0
    assert response.json()["items"][0]["outcome"] == "agent_delay"
    assert len(captured) == 1
    assert broker_adapter.order_calls == []
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "agent_delay"
        assert stop_item.trigger_price == Decimal("70000.0000")
        interaction = target_item.risk_context["last_trigger_time_exec_agent_interaction"]
        assert "would loosen existing protective stop" in interaction["decision"]["reason"]

    engine.dispose()


def test_trigger_time_partial_target_rechecks_concurrently_tightened_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-trigger-agent-concurrent-stop-tightening",
            "source": "test-fixture",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "target_sell",
                    "decision_agent_name": "signal-decider",
                    "quantity_policy": {"kind": "shares", "quantity": 4},
                    "trigger_price": "75000.00",
                    "time_in_force": "day",
                    "risk_context": {
                        "exchange": "KRX",
                        "exit_rule": {"kind": "partial_take_profit", "stage": "first"},
                    },
                },
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "decision_agent_name": "signal-decider",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "70000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                },
            ],
        },
    )
    assert plan_response.status_code == 201
    target_item_id, stop_item_id = plan_response.json()["persisted_execution_item_ids"]
    _seed_exact_strategy_family_for_sell_items(
        app,
        sell_item_ids=[target_item_id, stop_item_id],
        quantity=5,
        entry_price=Decimal("65000.0000"),
    )
    with app.state.db_session_factory() as session:
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("65000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    def tighten_stop_while_agent_runs(
        *, settings: Settings, payload: dict[str, object]
    ) -> dict[str, object]:
        captured.append(payload)
        with app.state.db_session_factory() as competing_session:
            stop_item = competing_session.get(ExecutionPlanItem, stop_item_id)
            assert stop_item is not None
            stop_item.trigger_price = Decimal("72000.0000")
            competing_session.commit()
        plan = payload["plan"]
        signal_context = payload["signal_context"]
        assert isinstance(plan, dict)
        assert isinstance(signal_context, dict)
        provider, model = settings.exec_agent_model.split("/", 1)
        requested_session_key = "trigger-exec-concurrent-stop-requested"
        reported_session_id = "trigger-exec-concurrent-stop-session"
        request_sha256 = json_payload_hash(
            trigger_time_exec_agent._json_safe(payload)
        )
        return {
            "version": trigger_time_exec_agent.TRIGGER_TIME_EXEC_AGENT_VERSION,
            "plan_id": plan["plan_id"],
            "execution_plan_item_id": signal_context["execution_plan_item_id"],
            "symbol": signal_context["symbol"],
            "decision": "execute_now",
            "side": "sell",
            "quantity": 2,
            # This tightens the stop the agent originally observed (70,000),
            # but would loosen the 72,000 stop committed during its request.
            "updated_stop_price": "71000.00",
            "valid_until": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            "reason": "take partial profit and tighten the originally observed stop",
            "risk_notes": ["preserve the latest sibling protection"],
            "confidence": "0.80",
            "context_staleness_ok": True,
            "_openclaw_runtime_provenance": {
                "metadata_status": "reported",
                "actual_agent_id": settings.exec_agent_id,
                "actual_provider": provider,
                "actual_model": model,
                "actual_model_ref": settings.exec_agent_model,
                "session_id": reported_session_id,
                "requested_session_key": requested_session_key,
                "request_sha256": request_sha256,
                "runtime_contract": verified_runtime_contract_proof(
                    expected_model_ref=settings.exec_agent_model,
                    requested_session_key=requested_session_key,
                    reported_session_id=reported_session_id,
                    request_sha256=request_sha256,
                ),
            },
        }

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "_request_exec_agent_decision",
        tighten_stop_while_agent_runs,
    )
    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "revalidate stop after concurrent tightening",
            "prices": [{"symbol": "005930", "price": "75100.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 0
    assert response.json()["items"][0]["outcome"] == "agent_delay"
    assert len(captured) == 1
    assert broker_adapter.order_calls == []
    with app.state.db_session_factory() as session:
        target_item = session.get(ExecutionPlanItem, target_item_id)
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert target_item is not None
        assert stop_item is not None
        assert target_item.status == "agent_delay"
        assert stop_item.trigger_price == Decimal("72000.0000")
        event = target_item.risk_context["last_monitoring_event"]
        assert event["reason"] == "post_agent_protective_stop_relaxation"

    engine.dispose()


def test_trigger_time_agent_always_bypasses_protective_stop_without_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    plan_response = client.post(
        "/execution-plans",
        json={
            "plan_id": "plan-emergency-stop-bypass",
            "source": "test-fixture",
            "items": [
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 5},
                    "trigger_price": "118000.00",
                    "time_in_force": "day",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        },
    )
    assert plan_response.status_code == 201
    stop_item_id = plan_response.json()["persisted_execution_item_ids"][0]
    _seed_exact_strategy_family_for_sell_items(
        app,
        sell_item_ids=[stop_item_id],
        quantity=5,
        entry_price=Decimal("120000.0000"),
    )
    with app.state.db_session_factory() as session:
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "emergency stop should remain deterministic",
            "prices": [{"symbol": "005930", "price": "117900.00"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["submitted_order_count"] == 1
    assert captured == []
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == stop_item_id
    assert broker_adapter.order_calls[0].quantity == 5

    engine.dispose()


def test_runtime_status_exposes_trigger_time_agent_state_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, _broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-agent-counts",
            source="test",
            mode="paper",
            items=[
                ExecutionPlanItem(
                    symbol=symbol,
                    side="buy",
                    intent="intraday_watch_buy",
                    status=status,
                    venue="KIS",
                    asset_class="stock",
                    risk_context={"exchange": "KRX"},
                    quantity_policy={"kind": "shares", "quantity": 1},
                )
                for symbol, status in (
                    ("000001", "watch_monitoring_active"),
                    ("000002", "triggered_pending_agent"),
                    ("000003", "agent_delay"),
                    ("000004", "agent_rejected"),
                    ("000005", "watch_buy_submitted"),
                    ("000006", "watch_sizing_pending"),
                )
            ],
        )
        for item in plan.items:
            _bind_plan_item_to_app_account(app, item)
        session.add(plan)
        session.commit()

    response = client.get("/runtime/status")

    assert response.status_code == 200
    counts = response.json()["liveness"]["monitoring"]["agent_decisions"]
    assert counts == {
        "watching": 1,
        "pending_pre_monitor_sizing": 1,
        "pending_agent_decision": 1,
        "agent_delayed": 1,
        "agent_rejected": 1,
        "agent_approved_submitted": 1,
    }

    with app.state.db_session_factory() as session:
        assert session.scalar(select(BrokerOrder)) is None

    engine.dispose()


def test_monitoring_rearms_partial_protective_stop_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, _broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-partial-protective-stop-coverage",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status="broker_order_submitted",
                    quantity_policy={"kind": "shares", "quantity": 1},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "repair partial protective stop coverage",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 1
    assert coverage["affected_symbols"] == []
    assert coverage["issues"] == []

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_coverage = runtime_status.json()["liveness"]["monitoring"]["protective_stop_coverage"]
    assert runtime_coverage["status"] == "ok"
    assert runtime_coverage["repaired_stop_coverage_count"] == 1

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        stop_items = session.scalars(
            select(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.symbol == "005930",
                ExecutionPlanItem.intent == "stop_sell",
            )
            .order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        checkpoint = session.scalar(select(MonitoringCheckpoint))
        assert stop_item is not None
        assert checkpoint is not None
        assert stop_item.status == "broker_order_submitted"
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 1}
        assert len(stop_items) == 2
        repair_item = stop_items[1]
        assert repair_item.status == "stop_sell_monitoring"
        assert repair_item.quantity_policy == {"kind": "shares", "quantity": 4}
        events = repair_item.risk_context["monitoring_events"]
        assert events[-1]["event"] == "unprotected_position_stop_rearmed"
        assert events[-1]["held_quantity"] == 5
        assert events[-1]["active_stop_quantity"] == 1
        assert events[-1]["missing_quantity"] == 4
        assert events[-1]["rearmed_quantity"] == 4
        assert events[-1]["prior_item_id"] == stop_item_id
        assert checkpoint.status == "running"
        assert checkpoint.details["protective_stop_coverage"]["repaired_stop_coverage_count"] == 1

    engine.dispose()


@pytest.mark.parametrize("stop_status", ["open", "partially_filled"])
def test_monitoring_counts_broker_truth_live_stop_as_protective_coverage(
    monkeypatch: pytest.MonkeyPatch,
    stop_status: str,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id=f"plan-live-stop-coverage-{stop_status}",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status=stop_status,
                    quantity_policy={"kind": "shares", "quantity": 5},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "verify broker truth protective stop coverage",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 0
    assert coverage["affected_symbols"] == []
    assert coverage["issues"] == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.status == stop_status
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 5}
        assert "monitoring_events" not in stop_item.risk_context

    engine.dispose()


def test_monitoring_requires_concrete_share_policy_for_protective_stop_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-position-exit-stop-coverage",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_monitoring",
                    quantity_policy={"kind": "position_exit_from_entry"},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "materialize concrete stop coverage quantity",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 1
    assert coverage["issues"] == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.status == "stop_sell_monitoring"
        assert stop_item.quantity_policy == {"kind": "shares", "quantity": 5}
        events = stop_item.risk_context["monitoring_events"]
        assert events[-1]["event"] == "unprotected_position_stop_rearmed"
        assert events[-1]["active_stop_quantity"] == 0
        assert events[-1]["missing_quantity"] == 5
        assert events[-1]["rearmed_quantity"] == 5

    engine.dispose()


@pytest.mark.parametrize("stop_status", ["broker_order_submitted", "open"])
def test_monitoring_preserves_in_flight_legacy_stop_when_repairing_coverage(
    monkeypatch: pytest.MonkeyPatch,
    stop_status: str,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    legacy_policy = {"kind": "position_exit_from_entry"}
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id=f"plan-in-flight-legacy-stop-coverage-{stop_status}",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status=stop_status,
                    quantity_policy=dict(legacy_policy),
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=5,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "repair legacy in-flight stop coverage",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "ok"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["repaired_stop_coverage_count"] == 1
    assert coverage["issues"] == []
    assert broker_adapter.order_calls == []

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        stop_items = session.scalars(
            select(ExecutionPlanItem)
            .where(
                ExecutionPlanItem.symbol == "005930",
                ExecutionPlanItem.intent == "stop_sell",
            )
            .order_by(ExecutionPlanItem.plan_item_index.asc())
        ).all()
        assert stop_item is not None
        assert stop_item.status == stop_status
        assert stop_item.quantity_policy == legacy_policy
        assert "monitoring_events" not in stop_item.risk_context
        assert len(stop_items) == 2

        repair_item = stop_items[1]
        assert repair_item.status == "stop_sell_monitoring"
        assert repair_item.quantity_policy == {"kind": "shares", "quantity": 5}
        events = repair_item.risk_context["monitoring_events"]
        assert events[-1]["event"] == "unprotected_position_stop_rearmed"
        assert events[-1]["active_stop_quantity"] == 0
        assert events[-1]["missing_quantity"] == 5
        assert events[-1]["rearmed_quantity"] == 5
        assert events[-1]["prior_item_id"] == stop_item_id
        assert events[-1]["prior_status"] == stop_status
        assert events[-1]["prior_quantity_policy"] == legacy_policy
        assert events[-1]["cloned_repair_item"] is True

    engine.dispose()


def test_monitoring_reports_unprotected_position_when_stop_threshold_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-missing-stop-threshold-coverage",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_pending_config",
                    quantity_policy={"kind": "shares", "quantity": 3},
                    trigger_price=None,
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=3,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "missing stop threshold must fail closed",
            "prices": [{"symbol": "005930", "price": "120500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "degraded"
    assert coverage["unprotected_position_count"] == 1
    assert coverage["affected_symbols"] == ["005930"]
    assert coverage["missing_quantities"] == {"005930": 3}
    assert coverage["issues"][0]["outcome"] == "unprotected_position"
    assert coverage["issues"][0]["missing_quantity"] == 3
    assert coverage["issues"][0]["latest_stop_item_id"] == stop_item_id
    assert broker_adapter.order_calls == []

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_coverage = runtime_status.json()["liveness"]["monitoring"]["protective_stop_coverage"]
    assert runtime_coverage["status"] == "degraded"
    assert runtime_coverage["missing_quantities"] == {"005930": 3}

    with app.state.db_session_factory() as session:
        checkpoint = session.scalar(select(MonitoringCheckpoint))
        assert checkpoint is not None
        assert checkpoint.status == "degraded"
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.status == "stop_sell_pending_config"

    engine.dispose()


def test_monitoring_materializes_pending_config_stop_before_crossing_is_missed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-pending-config-stop-materialized",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="005930",
                    side="sell",
                    intent="stop_sell",
                    status="stop_sell_pending_config",
                    quantity_policy={"kind": "shares", "quantity": 2},
                    trigger_price=Decimal("118000.00"),
                    time_in_force="day",
                    risk_context={
                        "exchange": "KRX",
                        "account_reference": "KIS:12345678-01",
                        "deterministic_emergency_stop": True,
                    },
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.flush()
        _seed_exact_strategy_family_in_session(
            settings=app.state.settings,
            session=session,
            sell_items=[plan.items[0]],
            quantity=2,
            entry_price=Decimal("120000.0000"),
        )
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="005930",
                quantity=2,
                average_price=Decimal("120000.0000"),
                updated_from="reconciliation",
                last_reconciled_at=datetime.now(UTC),
                raw_payload={"source": "broker"},
            )
        )
        session.commit()
        stop_item_id = plan.items[0].id

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "cross pending-config stop",
            "prices": [{"symbol": "005930", "price": "117900.00"}],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["protective_stop_coverage"]["status"] == "ok"
    assert payload["protective_stop_coverage"]["unprotected_position_count"] == 0
    assert payload["submitted_order_count"] == 1
    assert len(broker_adapter.order_calls) == 1
    assert broker_adapter.order_calls[0].execution_plan_item_id == stop_item_id
    assert broker_adapter.order_calls[0].quantity == 2

    with app.state.db_session_factory() as session:
        stop_item = session.get(ExecutionPlanItem, stop_item_id)
        assert stop_item is not None
        assert stop_item.status == "broker_order_submitted"
        events = stop_item.risk_context["monitoring_events"]
        assert any(
            event["event"] == "post_entry_stop_monitor_activated"
            and event["reason"] == "repaired_pending_stop_configuration"
            for event in events
        )

    engine.dispose()


def test_monitoring_flags_stale_flat_position_truth_with_broker_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, engine, client, broker_adapter, _captured = _build_trigger_agent_app(
        monkeypatch=monkeypatch,
        agent_responses=[],
    )
    stale_reconciled_at = datetime.now(UTC) - timedelta(hours=2)
    with app.state.db_session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-319660-stale-flat-position",
            source="test",
            mode="paper",
            market_date=datetime(2026, 6, 18, tzinfo=UTC).date(),
            items=[
                ExecutionPlanItem(
                    symbol="319660",
                    side="sell",
                    intent="stop_sell",
                    status="expired",
                    quantity_policy={"kind": "shares", "quantity": 4},
                    trigger_price=Decimal("22100.00"),
                    time_in_force="day",
                    risk_context={"exchange": "KRX"},
                ),
            ],
        )
        _bind_plan_item_to_app_account(app, plan.items[0])
        session.add(plan)
        session.flush()
        session.add(
            BrokerOrder(
                execution_plan_item_id=None,
                broker_name="kis",
                mode="paper",
                idempotency_key="filled-buy-319660",
                symbol="319660",
                side="buy",
                status="filled",
                quantity=4,
                venue="KRX",
                asset_class="stock",
                limit_price=Decimal("23000.00"),
                submitted_at=stale_reconciled_at + timedelta(minutes=5),
                request_metadata={
                    "exchange": "KRX",
                    "execution_scope": execution_scope_payload(
                        identity=build_position_identity(
                            settings=app.state.settings,
                            provider="kis",
                            exchange="KRX",
                        ),
                        venue="KRX",
                        asset_class="stock",
                    ),
                },
            )
        )
        session.add(
                _scoped_execution_position(
                    app,
                mode="paper",
                symbol="319660",
                quantity=0,
                average_price=None,
                updated_from="reconciliation",
                last_reconciled_at=stale_reconciled_at,
                raw_payload={"source": "broker"},
            )
        )
        session.commit()

    response = client.post(
        "/monitoring/evaluate",
        json={
            "actor": "ops",
            "source": "test",
            "reason": "stale flat position must not be globally healthy",
            "prices": [{"symbol": "319660", "price": "22500.00"}],
        },
    )

    assert response.status_code == 200
    coverage = response.json()["protective_stop_coverage"]
    assert coverage["status"] == "degraded"
    assert coverage["unprotected_position_count"] == 0
    assert coverage["position_truth_stale_count"] == 1
    assert coverage["affected_symbols"] == ["319660"]
    issue = coverage["issues"][0]
    assert issue["outcome"] == "position_truth_stale"
    assert issue["position_quantity"] == 0
    assert issue["position_last_reconciled_at"] == stale_reconciled_at.replace(tzinfo=None).isoformat()
    assert "broker_order:buy:filled_unexited" in issue["evidence"]
    assert "sell_monitor:expired" in issue["evidence"]
    assert broker_adapter.order_calls == []

    runtime_status = client.get("/runtime/status")
    assert runtime_status.status_code == 200
    runtime_coverage = runtime_status.json()["liveness"]["monitoring"]["protective_stop_coverage"]
    assert runtime_coverage["status"] == "degraded"
    assert runtime_coverage["position_truth_stale_count"] == 1
    assert runtime_coverage["affected_symbols"] == ["319660"]

    with app.state.db_session_factory() as session:
        checkpoint = session.scalar(select(MonitoringCheckpoint))
        assert checkpoint is not None
        assert checkpoint.status == "degraded"

    engine.dispose()


def test_trigger_time_agent_request_file_exists_only_during_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_paths: list[Path] = []

    def fake_run(command: list[str], *, timeout_seconds: int) -> subprocess.CompletedProcess[str]:
        del timeout_seconds
        assert "--message" not in command
        request_path = Path(command[command.index("--message-file") + 1])
        assert request_path.exists()
        prompt = request_path.read_text(encoding="utf-8")
        assert '"symbol": "005930"' in prompt
        assert "Read /" not in prompt
        request_paths.append(request_path)
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(
                {
                    "result": {
                        "payloads": [
                            {
                                "text": json.dumps(
                                    {
                                        "decision": "delay",
                                        "side": "buy",
                                        "reason": "test",
                                    }
                                )
                            }
                        ]
                    }
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(trigger_time_exec_agent, "_run_exec_agent_command", fake_run)
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "capture_runtime_contract",
        lambda **_: matching_runtime_contract_snapshot(),
    )

    response = trigger_time_exec_agent._request_exec_agent_decision(
        settings=Settings(),
        payload={"symbol": "005930"},
    )

    assert response["decision"] == "delay"
    assert len(request_paths) == 1
    assert not request_paths[0].exists()


def test_timed_out_agent_closes_pipes_without_unbounded_second_communicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Pipe:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class HungProcess:
        pid = 4321
        returncode = None

        def __init__(self) -> None:
            self.stdout = Pipe()
            self.stderr = Pipe()
            self.communicate_calls = 0
            self.wait_calls = 0

        def communicate(self, *, timeout=None):  # noqa: ANN001
            self.communicate_calls += 1
            if self.communicate_calls > 1:
                raise AssertionError("timed-out command must not call communicate again")
            raise subprocess.TimeoutExpired(
                cmd=["hung-agent"],
                timeout=timeout,
                output="partial stdout",
                stderr="partial stderr",
            )

        def wait(self, *, timeout=None):  # noqa: ANN001
            self.wait_calls += 1
            raise subprocess.TimeoutExpired(cmd=["hung-agent"], timeout=timeout)

    process = HungProcess()
    signals: list[int] = []
    monkeypatch.setattr(
        trigger_time_exec_agent.subprocess,
        "Popen",
        lambda *args, **kwargs: process,
    )
    monkeypatch.setattr(
        trigger_time_exec_agent.os,
        "killpg",
        lambda pid, sent_signal: signals.append(sent_signal),
    )

    with pytest.raises(subprocess.TimeoutExpired) as raised:
        trigger_time_exec_agent._run_exec_agent_command(
            ["hung-agent"],
            timeout_seconds=1,
        )

    assert raised.value.output == "partial stdout"
    assert raised.value.stderr == "partial stderr"
    assert process.communicate_calls == 1
    assert process.wait_calls == 2
    assert signals == [
        trigger_time_exec_agent.signal.SIGTERM,
        trigger_time_exec_agent.signal.SIGKILL,
    ]
    assert process.stdout.closed is True
    assert process.stderr.closed is True


def test_trigger_time_cost_model_replaces_stale_slippage_with_fresh_spread() -> None:
    item = ExecutionPlanItem(
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        venue="kis",
        asset_class="stock",
        risk_context={
            "exchange": "KRX",
            "estimated_round_trip_cost_bps": "999",
            "fee_bps": "4",
        },
    )

    evidence = monitoring._post_agent_live_cost_evidence(
        settings=Settings(),
        item=item,
        market_context={"spread_bps": "6"},
    )
    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=evidence,
        require_cost_evidence=True,
    )

    assert assessment.estimated_round_trip_cost_bps == Decimal("56.0000")
    assert assessment.cost_evidence_source == (
        "settings:kis_krx_round_trip_non_spread_cost_bps+live_quote_spread_bps"
    )


def test_live_cost_policy_cannot_be_reduced_by_candidate_or_agent_values() -> None:
    evidence = configured_execution_cost_evidence(
        settings=Settings(kis_krx_round_trip_non_spread_cost_bps=Decimal("50")),
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        live_quote_spread_bps="6",
        require_live_quote_spread=True,
    )
    evidence["estimated_round_trip_cost_bps"] = "0"
    evidence["exec_agent_decision"] = {
        "estimated_round_trip_cost_bps": "0",
        "fee_bps": "0",
    }

    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=evidence,
        require_cost_evidence=True,
    )

    assert assessment.estimated_round_trip_cost_bps == Decimal("56.0000")
    assert assessment.cost_evidence_source == (
        "settings:kis_krx_round_trip_non_spread_cost_bps+live_quote_spread_bps"
    )


def test_live_cost_policy_accepts_explicit_zero_plus_fresh_spread() -> None:
    evidence = configured_execution_cost_evidence(
        settings=Settings(alpaca_us_equity_round_trip_non_spread_cost_bps=Decimal("0")),
        provider="alpaca",
        asset_class="us_equity",
        exchange="NASD",
        live_quote_spread_bps="7.5",
        require_live_quote_spread=True,
    )
    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=evidence,
        require_cost_evidence=True,
    )

    assert assessment.reason is None
    assert assessment.estimated_round_trip_cost_bps == Decimal("7.5000")
    assert assessment.cost_evidence_source == (
        "settings:alpaca_us_equity_round_trip_non_spread_cost_bps+live_quote_spread_bps"
    )


@pytest.mark.parametrize(
    ("provider", "asset_class", "exchange"),
    [
        ("unknown", "stock", "KRX"),
        ("kis", "us_equity", "NASD"),
    ],
)
def test_live_cost_policy_fails_closed_for_unconfigured_route(
    provider: str,
    asset_class: str,
    exchange: str,
) -> None:
    evidence = configured_execution_cost_evidence(
        settings=Settings(kis_us_round_trip_non_spread_cost_bps=None),
        provider=provider,
        asset_class=asset_class,
        exchange=exchange,
        live_quote_spread_bps="5",
        require_live_quote_spread=True,
    )
    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=evidence,
        require_cost_evidence=True,
    )

    assert assessment.reason == "transaction_cost_evidence_missing"
    assert assessment.cost_evidence_source is None


def test_live_cost_policy_fails_closed_without_fresh_spread() -> None:
    evidence = configured_execution_cost_evidence(
        settings=Settings(),
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        live_quote_spread_bps=None,
        require_live_quote_spread=True,
    )
    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=evidence,
        require_cost_evidence=True,
    )

    assert assessment.reason == "transaction_cost_evidence_invalid"


def test_execution_cost_policy_env_override_is_validated() -> None:
    settings = load_settings(
        {"KAI_EXEC_KIS_KRX_ROUND_TRIP_NON_SPREAD_COST_BPS": "75"}
    )
    assert settings.kis_krx_round_trip_non_spread_cost_bps == Decimal("75")

    with pytest.raises(ValidationError):
        load_settings(
            {"KAI_EXEC_KIS_KRX_ROUND_TRIP_NON_SPREAD_COST_BPS": "-0.01"}
        )


@pytest.mark.parametrize(
    ("failure", "expected_reason"),
    [
        ("none", "returned no snapshot"),
        ("stale", "snapshot is stale"),
        ("future", "future clock skew"),
        ("symbol", "symbol identity mismatch"),
        ("source", "lacks trusted exchange-time provenance"),
        ("endpoint", "endpoint identity mismatch"),
        ("range", "day range is invalid"),
        ("error", "refresh failed: RuntimeError"),
    ],
)
def test_trigger_time_liquidity_refresh_fails_closed_on_untrusted_evidence(
    failure: str,
    expected_reason: str,
) -> None:
    reference_at = datetime(2026, 6, 5, 0, 30, tzinfo=UTC)
    snapshot = BrokerIntradayLiquiditySnapshot(
        symbol="OTHER" if failure == "symbol" else "005930",
        exchange="KRX",
        observed_at=(
            reference_at - timedelta(seconds=2)
            if failure == "stale"
            else reference_at + timedelta(seconds=2)
            if failure == "future"
            else reference_at
        ),
        volume=Decimal("1250000"),
        turnover=Decimal("3500000000"),
        day_high=Decimal("70000") if failure == "range" else Decimal("72000"),
        day_low=Decimal("71000") if failure == "range" else Decimal("70000"),
        freshness_source=(
            "transport_received_at_untrusted"
            if failure == "source"
            else "kis_exchange_intraday_minute_time"
        ),
        payload={
            "provider": "kis",
            "endpoint": (
                "/wrong" if failure == "endpoint" else
                "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
            ),
            "tr_id": "FHKST03010200",
        },
    )

    class Adapter:
        provider = "kis"
        calls = 0

        def get_intraday_liquidity_snapshot(self, *_args: object, **_kwargs: object):
            self.calls += 1
            if failure == "error":
                raise RuntimeError("broker unavailable")
            return None if failure == "none" else snapshot

    adapter = Adapter()
    item = ExecutionPlanItem(
        execution_plan_id="plan",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("71000"),
        observed_at=reference_at,
        freshness_source="kis_exchange_orderbook_time",
    )

    _price, reason = monitoring._merge_trigger_time_liquidity_snapshot(
        settings=Settings(
            runtime_mode="live",
            monitoring_max_quote_age_seconds=1,
            monitoring_max_quote_future_skew_seconds=1,
        ),
        adapter=adapter,
        access_token="token",
        item=item,
        price=price,
        reference_at=reference_at,
    )

    assert expected_reason in str(reason)
    assert adapter.calls == 1


def test_trigger_time_liquidity_refresh_retries_one_empty_snapshot_then_uses_fresh_evidence(
) -> None:
    reference_at = datetime(2026, 6, 5, 0, 30, tzinfo=UTC)
    snapshot = BrokerIntradayLiquiditySnapshot(
        symbol="005930",
        exchange="KRX",
        observed_at=reference_at,
        volume=Decimal("1250000"),
        turnover=Decimal("3500000000"),
        day_high=Decimal("72000"),
        day_low=Decimal("70000"),
        freshness_source="kis_exchange_intraday_minute_time",
        payload={
            "provider": "kis",
            "endpoint": (
                "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
            ),
            "tr_id": "FHKST03010200",
            "field_sources": {
                "day_high": {
                    "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-price",
                    "tr_id": "FHKST01010100",
                    "bound_endpoint": (
                        "/uapi/domestic-stock/v1/quotations/"
                        "inquire-time-itemchartprice"
                    ),
                    "bound_tr_id": "FHKST03010200",
                    "binding": (
                        "same_symbol_monotonic_cumulative_totals_and_"
                        "containing_minute_range"
                    ),
                },
                "day_low": {
                    "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-price",
                    "tr_id": "FHKST01010100",
                    "bound_endpoint": (
                        "/uapi/domestic-stock/v1/quotations/"
                        "inquire-time-itemchartprice"
                    ),
                    "bound_tr_id": "FHKST03010200",
                    "binding": (
                        "same_symbol_monotonic_cumulative_totals_and_"
                        "containing_minute_range"
                    ),
                },
            },
        },
    )

    class Adapter:
        provider = "kis"
        calls = 0

        def get_intraday_liquidity_snapshot(self, *_args: object, **_kwargs: object):
            self.calls += 1
            if self.calls == 1:
                raise BrokerAdapterError(
                    "temporary empty intraday response",
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                    code="kis_intraday_liquidity_empty_response",
                )
            return snapshot

    adapter = Adapter()
    item = ExecutionPlanItem(
        execution_plan_id="plan",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("71000"),
        observed_at=reference_at,
        freshness_source="kis_exchange_orderbook_time",
    )

    merged_price, reason = monitoring._merge_trigger_time_liquidity_snapshot(
        settings=Settings(
            runtime_mode="live",
            monitoring_max_quote_age_seconds=1,
            monitoring_max_quote_future_skew_seconds=1,
            kis_get_network_retry_attempts=1,
        ),
        adapter=adapter,
        access_token="token",
        item=item,
        price=price,
        reference_at=reference_at,
    )

    assert reason is None
    assert adapter.calls == 2
    assert merged_price.turnover == Decimal("3500000000")
    assert merged_price.provenance["intraday_liquidity"][
        "refresh_attempt_count"
    ] == 2
    assert merged_price.provenance["field_sources"]["day_high"] == {
        "source": "broker_intraday_liquidity_snapshot",
        "freshness_source": "kis_exchange_intraday_minute_time",
        "observed_at": reference_at.isoformat(),
        "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-price",
        "tr_id": "FHKST01010100",
        "refresh_attempt_count": 2,
        "bound_endpoint": (
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        ),
        "bound_tr_id": "FHKST03010200",
        "binding": (
            "same_symbol_monotonic_cumulative_totals_and_containing_minute_range"
        ),
    }


def test_trigger_time_liquidity_refresh_never_retries_validation_even_if_flagged_retryable(
) -> None:
    reference_at = datetime(2026, 6, 5, 0, 30, tzinfo=UTC)
    snapshot = BrokerIntradayLiquiditySnapshot(
        symbol="005930",
        exchange="KRX",
        observed_at=reference_at,
        volume=Decimal("1250000"),
        turnover=Decimal("3500000000"),
        day_high=Decimal("72000"),
        day_low=Decimal("70000"),
        freshness_source="kis_exchange_intraday_minute_time",
        payload={"provider": "kis"},
    )

    class Adapter:
        provider = "kis"
        calls = 0

        def get_intraday_liquidity_snapshot(self, *_args: object, **_kwargs: object):
            self.calls += 1
            if self.calls == 1:
                raise BrokerAdapterError(
                    "scope mismatch",
                    classification=BrokerErrorClassification.VALIDATION,
                    retryable=True,
                    code="scope_mismatch",
                )
            return snapshot

    adapter = Adapter()
    item = ExecutionPlanItem(
        execution_plan_id="plan",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("71000"),
        observed_at=reference_at,
        freshness_source="kis_exchange_orderbook_time",
    )

    merged_price, reason = monitoring._merge_trigger_time_liquidity_snapshot(
        settings=Settings(runtime_mode="live", kis_get_network_retry_attempts=3),
        adapter=adapter,
        access_token="token",
        item=item,
        price=price,
        reference_at=reference_at,
    )

    assert merged_price is price
    assert adapter.calls == 1
    assert reason == (
        "intraday liquidity refresh failed: scope_mismatch; "
        "attempts=1; retryable=false"
    )


def test_trigger_time_liquidity_refresh_bounds_retry_and_reports_adapter_code(
) -> None:
    reference_at = datetime(2026, 6, 5, 0, 30, tzinfo=UTC)

    class Adapter:
        provider = "kis"
        calls = 0

        def get_intraday_liquidity_snapshot(self, *_args: object, **_kwargs: object):
            self.calls += 1
            raise BrokerAdapterError(
                "temporary empty intraday response",
                classification=BrokerErrorClassification.TRANSIENT,
                retryable=True,
                code="kis_intraday_liquidity_empty_response",
            )

    adapter = Adapter()
    item = ExecutionPlanItem(
        execution_plan_id="plan",
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        provider="kis",
        asset_class="stock",
        risk_context={"exchange": "KRX"},
    )
    price = MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("71000"),
        observed_at=reference_at,
        freshness_source="kis_exchange_orderbook_time",
    )

    merged_price, reason = monitoring._merge_trigger_time_liquidity_snapshot(
        settings=Settings(runtime_mode="live", kis_get_network_retry_attempts=3),
        adapter=adapter,
        access_token="token",
        item=item,
        price=price,
        reference_at=reference_at,
    )

    assert merged_price is price
    assert adapter.calls == 2
    assert reason == (
        "intraday liquidity refresh failed: "
        "kis_intraday_liquidity_empty_response; attempts=2; retryable=true"
    )


def test_account_snapshot_freshness_uses_post_auth_and_fetch_logical_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference_at = datetime(2026, 10, 14, 0, 30, tzinfo=UTC)
    monotonic_ticks = iter((100.0, 102.0, 104.0))
    monkeypatch.setattr(
        trigger_time_exec_agent,
        "_monotonic_seconds",
        lambda: next(monotonic_ticks),
        raising=False,
    )

    def authenticate(**_kwargs: object) -> SimpleNamespace:
        # Consume logical time inside authentication so the regression proves
        # that token-refresh latency is included, not only the snapshot RPC.
        trigger_time_exec_agent._monotonic_seconds()
        return SimpleNamespace(access_token="token")

    monkeypatch.setattr(
        trigger_time_exec_agent,
        "ensure_authenticated_broker_session",
        authenticate,
    )

    class Adapter:
        provider = "kis"

        def get_account_snapshot(
            self,
            _access_token: str,
            **_kwargs: object,
        ) -> BrokerAccountSnapshot:
            return BrokerAccountSnapshot(
                available_cash=Decimal("1000000"),
                buying_power=Decimal("1000000"),
                observed_at=reference_at + timedelta(seconds=3),
                source="stub:post_fetch_account_snapshot",
            )

    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        account_drawdown_recovery_max_snapshot_future_skew_seconds=0,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            payload = trigger_time_exec_agent._load_account_context(
                settings=settings,
                db=session,
                adapter=Adapter(),  # type: ignore[arg-type]
                item=ExecutionPlanItem(
                    execution_plan_id="plan-post-fetch-snapshot",
                    symbol="005930",
                    side="buy",
                    intent="intraday_watch_buy",
                    provider="kis",
                    asset_class="stock",
                    risk_context={"exchange": "KRX"},
                ),
                observed_price=Decimal("71000"),
                observed_at=reference_at,
            )

        assert payload["available"] is True
        assert payload["observed_at"] == (
            reference_at + timedelta(seconds=3)
        ).isoformat()
    finally:
        engine.dispose()


def test_trigger_time_account_context_excludes_foreign_local_open_orders() -> None:
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
                    id=f"trigger-{suffix}-open-buy",
                    broker_name="kis",
                    mode="paper",
                    idempotency_key=f"trigger-{suffix}-open-buy",
                    broker_order_id=f"trigger-{suffix}-open-buy",
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
        payload = trigger_time_exec_agent._account_context_payload(
            settings=settings,
            db=session,
            snapshot=BrokerAccountSnapshot(
                available_cash=Decimal("1000000"),
                buying_power=Decimal("1000000"),
                observed_at=datetime.now(UTC),
                source="stub:account_snapshot",
            ),
            provider="kis",
            exchange="KRX",
        )

    assert payload["execution_account_scope"] == {
        "provider": current_identity.provider,
        "account_scope": current_identity.account_scope,
    }
    assert payload["open_order_count"] == 1
    assert payload["open_orders"][0] == {
        "symbol": "005930",
        "side": "buy",
        "quantity": 1,
        "remaining_quantity": 1,
        "remaining_quantity_indeterminate": False,
        "status": "submitted",
        "limit_price": "100000.0000",
    }
    engine.dispose()


def test_trigger_time_account_context_dedupes_partial_local_order_and_hides_other_lane_identity() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope="trigger-lane-account",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_position_identity(
        settings=settings,
        provider="kis",
        exchange="KRX",
    )
    observed_at = datetime.now(UTC)

    with session_factory() as session:
        short_item = ExecutionPlanItem(
            id="trigger-short-order-item",
            plan_item_index=0,
            symbol="005930",
            side="buy",
            intent="intraday_watch_buy",
            status="broker_order_submitted",
            provider="kis",
            venue="KRX",
            asset_class="stock",
            decision_agent_name="short-term-decider",
            source_plan_item_id="trigger-short-family",
        )
        signal_item = ExecutionPlanItem(
            id="trigger-signal-order-item",
            plan_item_index=1,
            symbol="000660",
            side="buy",
            intent="intraday_watch_buy",
            status="broker_order_submitted",
            provider="kis",
            venue="KRX",
            asset_class="stock",
            decision_agent_name="signal-decider",
            source_plan_item_id="trigger-signal-family",
        )
        plan = ExecutionPlan(
            external_plan_id="trigger-account-lane-plan",
            source="test",
            mode="paper",
            items=[short_item, signal_item],
        )
        short_order = BrokerOrder(
            id="trigger-short-partial-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="trigger-short-partial-order",
            broker_order_id="broker-short-partial",
            execution_plan_item_id=short_item.id,
            symbol="005930",
            side="buy",
            status="submitted",
            quantity=5,
            limit_price=Decimal("100"),
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
        signal_order = BrokerOrder(
            id="trigger-signal-order",
            broker_name="kis",
            mode="paper",
            idempotency_key="trigger-signal-order",
            broker_order_id="broker-signal-order",
            execution_plan_item_id=signal_item.id,
            symbol="000660",
            side="buy",
            status="submitted",
            quantity=2,
            limit_price=Decimal("200"),
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
        session.add_all([plan, short_order, signal_order])
        session.flush()
        session.add(
            ExecutionFill(
                broker_order_pk=short_order.id,
                broker_fill_id="trigger-short-fill",
                idempotency_key="trigger-short-fill",
                symbol="005930",
                side="buy",
                quantity=2,
                price=Decimal("100"),
                filled_at=observed_at - timedelta(seconds=5),
                payload={"fill_time_precision": "exact"},
            )
        )
        session.commit()

        payload = trigger_time_exec_agent._account_context_payload(
            settings=settings,
            db=session,
            snapshot=BrokerAccountSnapshot(
                available_cash=Decimal("10000"),
                buying_power=Decimal("10000"),
                observed_at=observed_at,
                source="stub:account_snapshot",
                open_orders=[
                    BrokerAccountOpenOrder(
                        symbol="005930",
                        side="buy",
                        quantity=5,
                        remaining_quantity=3,
                        status="open",
                        broker_order_id="broker-short-partial",
                        idempotency_key="trigger-short-partial-order",
                        limit_price=Decimal("100"),
                    )
                ],
            ),
            provider="kis",
            exchange="KRX",
            decision_lane="short-term-decider",
            observed_at=observed_at,
        )

    assert payload["open_order_count"] == 2
    same_lane, other_lane = payload["open_orders"]
    assert same_lane["remaining_quantity"] == 3
    assert same_lane["decision_agent_name"] == "short-term-decider"
    assert same_lane["entry_family_key"] == "source:trigger-short-family"
    assert same_lane["broker_order_id"] == "broker-short-partial"
    assert other_lane == {
        "symbol": "000660",
        "side": "buy",
        "quantity": 2,
        "remaining_quantity": 2,
        "remaining_quantity_indeterminate": False,
        "status": "submitted",
        "limit_price": "200.0000",
    }
    engine.dispose()
