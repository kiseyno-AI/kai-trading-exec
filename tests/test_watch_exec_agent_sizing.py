from __future__ import annotations

from copy import deepcopy
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from sqlalchemy import UniqueConstraint, select

from app.api.schemas.execution import (
    ExecutionPlanRequest,
    MonitoringEvaluationRequest,
    MonitoringPriceRequest,
)
from app.broker.base import (
    BrokerAdapterError,
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
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFill,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
    ExecutionPlanItemAudit,
    ExecutionPosition,
)
from app.services import watch_exec_agent_sizing as watch_sizing
from app.services.openclaw_runtime_contract import runtime_contract_proof_is_valid
from openclaw_contract_test_support import (
    matching_runtime_contract_snapshot,
    verified_runtime_contract_proof,
)


from app.services.exec_agent_approval import (
    assess_buy_economics,
    configured_execution_cost_evidence,
    delegated_buy_gate_rejections,
    maximum_entry_price_for_buy_economics,
)
from app.services.json_compaction import json_payload_hash
from app.services.execution_account_route import bind_kis_account_route
from app.services.monitoring import (
    LIVE_WATCH_BUY_VETO_EVENT_TYPE,
    _resolve_watch_buy_order,
    _watch_buy_candidate_budget,
    _watch_buy_exec_agent_sizing_fallback_allowed,
    evaluate_monitoring,
    list_autonomous_monitoring_symbols,
)
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
)
from app.services.risk_sizing import effective_stop_price
from app.services.runtime import (
    RuntimeMetadata,
    build_runtime_daily_board,
    build_runtime_status,
    runtime_execution_decision_metrics,
)
from app.services.runtime_controls import ControlCommand, resume_runtime
from app.services.execution_api import create_execution_plan
from app.services.watch_exec_agent_sizing import (
    _apply_decisions,
    _applicable_decision_ids,
    _account_context_payload,
    _candidate_payload,
    _candidate_universe_payload,
    _chunks,
    _decision_record,
    _quantity_is_shares,
    _sizing_request_payload,
    run_due_watch_exec_agent_sizing,
)


class WatchSizingBrokerStub:
    provider = "kis"

    def __init__(
        self,
        *,
        reconcile_payloads: list[dict[str, object]],
        snapshot_error: Exception | None = None,
        snapshot: BrokerAccountSnapshot | None = None,
    ) -> None:
        self.reconcile_payloads = reconcile_payloads
        self.snapshot_error = snapshot_error
        self.snapshot = snapshot
        self.events: list[str] = []
        self.reconcile_calls: list[str] = []

    def describe(self) -> BrokerConfig:
        return BrokerConfig(provider="kis", mode="paper", configured=True, api_base_url="https://stub.kis.local")

    def authenticate(self) -> BrokerTokenGrant:
        self.events.append("authenticate")
        return BrokerTokenGrant(
            access_token="token",
            token_type="Bearer",
            access_token_expires_at=datetime(2026, 6, 4, 1, 0, tzinfo=UTC),
        )

    def issue_realtime_approval(self, access_token: str) -> BrokerRealtimeApproval:
        return BrokerRealtimeApproval(approval_key="approval", issued_at=datetime(2026, 6, 4, tzinfo=UTC))

    def revoke(self, access_token: str) -> BrokerTokenRevocation:
        return BrokerTokenRevocation(message="revoked")

    def place_order(self, access_token: str, request: BrokerOrderPlacementRequest) -> BrokerOrderPlacement:
        raise NotImplementedError

    def get_current_price(self, access_token: str, *, symbol: str, exchange: str = "KRX") -> BrokerCurrentPrice | None:
        return None

    def get_account_snapshot(
        self,
        access_token: str,
        *,
        symbols: list[str] | None = None,
        reference_prices: dict[str, Decimal] | None = None,
        exchange: str = "KRX",
    ) -> BrokerAccountSnapshot:
        del symbols, reference_prices, exchange
        self.events.append("snapshot")
        if self.snapshot_error is not None:
            raise self.snapshot_error
        if self.snapshot is not None:
            return self.snapshot
        return BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
            observed_at=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
            source="stub:account_snapshot",
            broker_account_id="watch-sizing-test-account",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(self, access_token: str, broker_order_id: str, *, exchange: str = "KRX") -> dict[str, object]:
        self.events.append("reconcile")
        self.reconcile_calls.append(broker_order_id)
        return self.reconcile_payloads[min(len(self.reconcile_calls) - 1, len(self.reconcile_payloads) - 1)]

    def build_headers(self, access_token: str, *, transaction_id: str | None = None, hashkey: str | None = None) -> dict[str, str]:
        return {"authorization": f"Bearer {access_token}"}


def test_retryable_watch_rejection_is_deferred_not_terminal() -> None:
    settings = Settings(runtime_mode="live", exec_agent_min_reward_risk=Decimal("1.8"))
    item = _watch_item(
        risk_context={
            "exchange": "KRX",
            "target_price": "6900",
            "stop_loss_price": "6090",
        },
        trigger_price="6440",
        limit_price="6550",
    )
    item.provider = "kis"
    item.venue = "KRX"
    item.asset_class = "stock"
    assert watch_sizing._retryable_watch_rejection(
        {"reason_code": "trigger_confirmation_and_cost_evidence_missing"},
        settings=settings,
        item=item,
    )
    assert watch_sizing._retryable_watch_rejection(
        {"reason_code": "reward_risk_below_minimum"},
        settings=settings,
        item=item,
    )
    structural_item = _watch_item(
        risk_context={"target_price": "61000", "stop_loss_price": "55000"},
        trigger_price="57300",
        limit_price="58000",
    )
    assert not watch_sizing._retryable_watch_rejection(
        {"reason_code": "reward_risk_below_minimum"},
        settings=settings,
        item=structural_item,
    )


@pytest.mark.parametrize(
    ("symbol", "trigger", "target", "stop", "expected_cap"),
    [
        ("028670", "6630", "7050", "6400", "6657.2237"),
        ("144960", "14650", "16200", "13880", "15297.4503"),
        ("171090", "80100", "84000", "78300", None),
    ],
)
def test_reward_risk_price_watch_cap_uses_full_cost_effective_stop_economics(
    symbol: str,
    trigger: str,
    target: str,
    stop: str,
    expected_cap: str | None,
) -> None:
    item = _watch_item(
        symbol=symbol,
        risk_context={
            "exchange": "KRX",
            "target_price": target,
            "stop_loss_price": stop,
            "trigger_direction": "at_or_above",
        },
        trigger_price=trigger,
        limit_price=trigger,
    )
    item.provider = "kis"
    item.asset_class = "stock"

    cap = watch_sizing._reward_risk_rejection_price_cap(
        settings=Settings(
            runtime_mode="live",
            exec_agent_min_reward_risk=Decimal("1.8"),
            exec_agent_stop_loss_pct=Decimal("0.025"),
            kis_krx_round_trip_non_spread_cost_bps=Decimal("50"),
        ),
        item=item,
    )

    assert (f"{cap:.4f}" if cap is not None else None) == expected_cap


@pytest.mark.parametrize(
    ("runtime_mode", "intent", "expected_minimum"),
    [
        ("paper", "intraday_watch_buy", Decimal("1.30")),
        ("live", "next_open_buy", Decimal("1.30")),
        ("live", "intraday_watch_buy", Decimal("1.80")),
        ("live", "watch_buy", Decimal("1.80")),
        ("live", "trigger_buy", Decimal("1.80")),
        ("live", "chase_buy", Decimal("1.80")),
        ("live", "tactical_buy", Decimal("1.80")),
    ],
)
def test_reward_risk_price_watch_cap_matches_action_envelope_minimum(
    runtime_mode: str,
    intent: str,
    expected_minimum: Decimal,
) -> None:
    settings = Settings(
        runtime_mode=runtime_mode,
        exec_agent_min_reward_risk=Decimal("1.30"),
        exec_agent_stop_loss_pct=Decimal("0.025"),
        kis_krx_round_trip_non_spread_cost_bps=Decimal("50"),
    )
    item = _watch_item(
        risk_context={
            "exchange": "KRX",
            "target_price": "7050",
            "stop_loss_price": "6400",
            "trigger_direction": "at_or_below",
        },
        trigger_price="6700",
        limit_price="6700",
    )
    item.intent = intent
    item.provider = "kis"
    item.asset_class = "stock"

    candidate = _candidate_payload(item)
    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=settings,
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )
    expected_cap = maximum_entry_price_for_buy_economics(
        target_price=Decimal("7050"),
        upstream_stop_price=Decimal("6400"),
        stop_loss_pct=Decimal("0.025"),
        minimum_reward_risk=expected_minimum,
        evidence=configured_execution_cost_evidence(
            settings=settings,
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            live_quote_spread_bps=None,
            require_live_quote_spread=False,
        ),
        require_cost_evidence=runtime_mode == "live",
    )

    assert envelope["minimum_reward_risk"] == f"{expected_minimum:.4f}"
    assert watch_sizing._reward_risk_rejection_price_cap(
        settings=settings,
        item=item,
    ) == expected_cap


@pytest.mark.parametrize(
    ("target", "upstream_stop"),
    [
        ("7050", "6400"),
        ("84000", "78300"),
        ("10.1", "8.08"),
    ],
    ids=[
        "configured-percent-stop",
        "upstream-stop",
        "configured-percent-stop-rounding-boundary",
    ],
)
def test_full_cost_max_entry_is_the_exact_assess_buy_economics_boundary(
    target: str,
    upstream_stop: str,
) -> None:
    settings = Settings(
        runtime_mode="live",
        exec_agent_min_reward_risk=Decimal("1.8"),
        exec_agent_stop_loss_pct=Decimal("0.025"),
        kis_krx_round_trip_non_spread_cost_bps=Decimal("50"),
    )
    cost_evidence = configured_execution_cost_evidence(
        settings=settings,
        provider="kis",
        asset_class="stock",
        exchange="KRX",
        live_quote_spread_bps=None,
        require_live_quote_spread=False,
    )
    cap = maximum_entry_price_for_buy_economics(
        target_price=Decimal(target),
        upstream_stop_price=Decimal(upstream_stop),
        stop_loss_pct=Decimal("0.025"),
        minimum_reward_risk=Decimal("1.8"),
        evidence=cost_evidence,
        require_cost_evidence=True,
    )
    assert cap is not None

    at_cap = assess_buy_economics(
        reference_price=cap,
        target_price=Decimal(target),
        stop_price=effective_stop_price(
            settings=settings,
            entry_price=cap,
            upstream_stop_price=Decimal(upstream_stop),
        ),
        minimum_reward_risk=Decimal("1.8"),
        evidence=cost_evidence,
        require_cost_evidence=True,
    )
    above_cap = cap + Decimal("0.0001")
    above = assess_buy_economics(
        reference_price=above_cap,
        target_price=Decimal(target),
        stop_price=effective_stop_price(
            settings=settings,
            entry_price=above_cap,
            upstream_stop_price=Decimal(upstream_stop),
        ),
        minimum_reward_risk=Decimal("1.8"),
        evidence=cost_evidence,
        require_cost_evidence=True,
    )

    assert at_cap.reason is None
    assert at_cap.net_reward_risk is not None
    assert at_cap.net_reward_risk >= Decimal("1.8000")
    assert above.reason == "net_reward_risk_below_minimum"
    assert above.net_reward_risk == Decimal("1.7999")


def test_freeform_reward_risk_reason_uses_deterministic_defer_disposition() -> None:
    settings = Settings(runtime_mode="live", exec_agent_min_reward_risk=Decimal("1.8"))
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-freeform-reason",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                risk_context={
                    "exchange": "KRX",
                    "target_price": "6900",
                    "stop_loss_price": "6090",
                },
                trigger_price="6440",
                limit_price="6550",
            )
        ],
    )
    plan.items[0].provider = "kis"
    plan.items[0].venue = "kis"
    plan.items[0].asset_class = "stock"

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(
            observed_at="2026-06-04T00:00:00+00:00"
        ),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "reject",
                    "quantity": 0,
                    "approved_notional_krw": 0,
                    "sizing_price": "6550",
                    "confidence": 0.9,
                    "target_profile": "short_swing",
                    "reason_code": "reward_risk_hard_block",
                    "reason": "free-form agent wording",
                }
            ],
        },
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )
    assert summary["deferred"] == 1
    assert summary["rejected"] == 0
    assert plan.items[0].status == watch_sizing.WATCH_SIZING_PENDING_STATUS
    deferred = plan.items[0].risk_context[watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY]
    assert deferred["disposition"] == "defer"
    assert deferred["reason_family"] == "reward_risk"
    assert deferred["expires_at"] == "2026-06-04T06:30:00+00:00"


def test_price_dependent_reward_risk_defer_remains_monitored_after_attempt_cap() -> None:
    settings = Settings(
        runtime_mode="live",
        exec_agent_min_reward_risk=Decimal("1.8"),
        exec_agent_watch_sizing_max_attempts=2,
    )
    item = _watch_item(
        risk_context={
            "exchange": "KRX",
            "target_price": "6900",
            "stop_loss_price": "6090",
            watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                "attempt_count": 1,
                "retry_after": "2026-06-04T00:00:00+00:00",
            },
        },
        trigger_price="6440",
        limit_price="6550",
    )
    item.provider = "kis"
    item.venue = "kis"
    item.asset_class = "stock"
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-defer-cap",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[item],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(
            observed_at="2026-06-04T00:05:00+00:00"
        ),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "reject",
                    "quantity": 0,
                    "approved_notional_krw": 0,
                    "sizing_price": "6550",
                    "confidence": 0.9,
                    "target_profile": "short_swing",
                    "reason_code": "reward_risk_hard_block",
                    "reason": "same dynamic gate after prior defer",
                }
            ],
        },
        now=datetime(2026, 6, 4, 0, 5, tzinfo=UTC),
    )

    assert summary["deferred"] == 1
    assert summary["rejected"] == 0
    assert item.status == watch_sizing.WATCH_SIZING_PENDING_STATUS
    deferred = item.risk_context[watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY]
    assert deferred["disposition"] == "defer"
    assert deferred["reason_family"] == "reward_risk"
    assert deferred["attempt_count"] == 2
    assert deferred["max_attempts"] == 2
    assert deferred["price_watch_only"] is True
    assert deferred["retry_after"] == "2026-06-04T00:10:00+00:00"
    assert deferred["expires_at"] == "2026-06-04T06:30:00+00:00"
    assert item.risk_context["reward_risk_max_entry_price"] == "6515.5805"


@pytest.mark.parametrize(
    ("direction", "trigger_price", "expected_reopened"),
    [
        ("at_or_below", "6700", True),
        ("at_or_above", "6640", False),
    ],
    ids=["pullback-cap-below-trigger", "breakout-cap-below-trigger"],
)
def test_reopens_legacy_price_watch_only_when_cap_matches_direction(
    direction: str,
    trigger_price: str,
    expected_reopened: bool,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        exec_agent_min_reward_risk=Decimal("1.8"),
        exec_agent_watch_sizing_max_attempts=3,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        eligible = _watch_item(
            item_id="price-watch-eligible",
            symbol="028670",
            risk_context={
                "exchange": "KRX",
                "trigger_direction": direction,
                "target_price": "7050",
                "stop_loss_price": "6400",
                "reward_risk_max_entry_price": "6630.0000",
            },
            trigger_price=trigger_price,
            limit_price="6650",
        )
        eligible.provider = "kis"
        eligible.venue = "KRX"
        eligible.asset_class = "stock"
        original_policy = dict(eligible.quantity_policy)
        eligible.status = "exec_agent_rejected"
        eligible.quantity_policy = {
            "kind": "exec_agent_rejected",
            "source": "exec_agent_watch_sizing",
            "reason": "deterministic watch rejection remained unresolved after 3 attempts",
            "reason_code": "deterministic_retry_exhausted",
            "reason_family": "reward_risk",
            "rejection_disposition": "terminal",
            "exec_agent_decision": {
                "source": "openclaw_exec_agent_watch_sizing",
                "action": "reject",
                "decision": "rejected",
                "execution_plan_item_id": eligible.id,
                "symbol": eligible.symbol,
                "quantity": 0,
                "approved_quantity": 0,
                "approved_notional_krw": None,
                "reason_code": "deterministic_retry_exhausted",
                "reason_family": "reward_risk",
                "rejection_disposition": "terminal",
                "reason": "deterministic watch rejection remained unresolved after 3 attempts",
            },
            "previous_policy": original_policy,
        }
        eligible.quantity_policy["short_term_exec_agent_decision"] = dict(
            eligible.quantity_policy["exec_agent_decision"]
        )
        structural = _watch_item(
            item_id="price-watch-structural",
            symbol="041960",
            risk_context={
                "exchange": "KRX",
                "target_price": "12600",
                "stop_loss_price": "11450",
            },
            trigger_price="11900",
            limit_price="12050",
        )
        structural.provider = "kis"
        structural.venue = "KRX"
        structural.asset_class = "stock"
        structural.status = "exec_agent_rejected"
        structural.quantity_policy = dict(eligible.quantity_policy)
        structural.quantity_policy["previous_policy"] = dict(structural.quantity_policy)
        plan = ExecutionPlan(
            external_plan_id="plan-price-watch-recovery",
            source="core-short-term-decider",
            market_date=date(2026, 10, 14),
            mode="live",
            status="accepted",
            items=[eligible, structural],
        )
        session.add(plan)
        session.commit()

        reopened = watch_sizing._reopen_price_watch_terminal_rejections(
            db=session,
            settings=settings,
            market_date=date(2026, 10, 14),
            now=datetime(2026, 10, 14, 1, 0, tzinfo=UTC),
        )
        session.commit()

        assert reopened == (
            ["price-watch-eligible"] if expected_reopened else []
        )
        assert eligible.status == (
            watch_sizing.WATCH_SIZING_PENDING_STATUS
            if expected_reopened
            else "exec_agent_rejected"
        )
        if expected_reopened:
            assert eligible.quantity_policy == original_policy
            deferred = eligible.risk_context[watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY]
            assert deferred["price_watch_only"] is True
            assert deferred["retry_after"] == "2026-10-14T01:05:00+00:00"
            assert deferred["expires_at"] == "2026-10-14T06:30:00+00:00"
            assert eligible.risk_context["reward_risk_max_entry_price"] == "6657.2237"
        assert structural.status == "exec_agent_rejected"
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == eligible.id,
                ExecutionPlanItemAudit.event_type
                == "watch_exec_agent_price_watch_reopened",
            )
        )
        assert (audit is not None) is expected_reopened

    engine.dispose()


def test_decision_validation_rejects_noncanonical_disposition() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-invalid-disposition",
        items=[_watch_item(risk_context={"target_price": "76000", "stop_price": "69000"})],
    )

    with pytest.raises(ValueError, match="rejection_disposition"):
        watch_sizing._validate_exec_agent_decision_payload(
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-1",
                        "symbol": "005930",
                        "action": "reject",
                        "quantity": 0,
                        "reason_code": "reward_risk_hard_block",
                        "reason": "free-form agent wording",
                        "rejection_disposition": "sometimes",
                    }
                ],
            },
            plan=plan,
            expected_item_ids={"watch-item-1"},
        )


@pytest.mark.parametrize(
    "legacy_reason_code",
    [
        "reward_risk_below_minimum",
        "risk_reward_below_minimum",
        "insufficient-reward-to-risk",
        "poor_rr_geometry",
    ],
)
def test_legacy_reward_risk_reason_variants_share_canonical_family(
    legacy_reason_code: str,
) -> None:
    assert watch_sizing._rejection_reason_family([], legacy_reason_code) == "reward_risk"


def test_terminal_entries_without_exposure_complete_plan_as_no_trade() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        execution_position_account_scope="no-trade-test-account",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            entry = _watch_item(
                item_id="entry-1",
                risk_context={"exchange": "KRX", "target_price": "76000", "stop_price": "69000"},
            )
            entry.status = "exec_agent_rejected"
            entry.source_plan_item_id = "core-entry-1"
            entry.provider = "kis"
            entry.venue = "kis"
            entry.asset_class = "stock"
            entry.risk_context, entry.quantity_policy = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=entry.risk_context,
                quantity_policy=entry.quantity_policy,
            )
            target = ExecutionPlanItem(
                id="target-1",
                plan_item_index=1,
                symbol="005930",
                side="sell",
                intent="target_sell",
                status="target_sell_pending_config",
                quantity_policy={
                    "type": "position_exit_from_entry",
                    "linked_entry_item_key": "core-entry-1",
                },
                provider="kis",
                venue="kis",
                asset_class="stock",
            )
            stop = ExecutionPlanItem(
                id="stop-1",
                plan_item_index=2,
                symbol="005930",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={
                    "type": "position_exit_from_entry",
                    "linked_entry_item_key": "core-entry-1",
                },
                provider="kis",
                venue="kis",
                asset_class="stock",
            )
            plan = ExecutionPlan(
                id="plan-1",
                external_plan_id="plan-completed-no-trade",
                source="kai-trading-core",
                mode="live",
                status="accepted",
                items=[entry, target, stop],
            )
            session.add(plan)
            session.flush()
            active_identity = build_position_identity(
                settings=settings,
                provider="kis",
                exchange="KRX",
            )
            session.add(
                ExecutionPosition(
                    id="foreign-account-same-symbol",
                    mode="live",
                    provider="kis",
                    account_scope="f" * 24,
                    exchange="KRX",
                    symbol=entry.symbol,
                    quantity=7,
                    updated_from="test",
                )
            )
            assert active_identity.account_scope != "f" * 24
            session.flush()

            result = watch_sizing.finalize_completed_no_trade_plan(
                db=session,
                settings=settings,
                plan=plan,
            )
            session.commit()

            assert result == {
                "completed": True,
                "entry_count": 1,
                "linked_exit_count": 2,
            }
            assert plan.status == "completed_no_trade"
            assert target.status == "not_applicable"
            assert stop.status == "not_applicable"
            audits = session.scalars(
                select(ExecutionPlanItemAudit).where(
                    ExecutionPlanItemAudit.event_type == "linked_exit_not_applicable"
                )
            ).all()
            assert len(audits) == 2

            exposed_entry = _watch_item(
                item_id="entry-with-order",
                symbol="000660",
                risk_context={"exchange": "KRX"},
            )
            exposed_entry.status = "exec_agent_rejected"
            exposed_entry.source_plan_item_id = "core-entry-with-order"
            exposed_entry.provider = "kis"
            exposed_entry.venue = "kis"
            exposed_entry.asset_class = "stock"
            (
                exposed_entry.risk_context,
                exposed_entry.quantity_policy,
            ) = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=exposed_entry.risk_context,
                quantity_policy=exposed_entry.quantity_policy,
            )
            exposed_exit = ExecutionPlanItem(
                id="exit-with-order",
                plan_item_index=1,
                symbol="000660",
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={
                    "type": "position_exit_from_entry",
                    "linked_entry_item_key": "core-entry-with-order",
                },
                provider="kis",
                venue="kis",
                asset_class="stock",
            )
            exposed_plan = ExecutionPlan(
                id="plan-with-order",
                external_plan_id="plan-with-order",
                source="kai-trading-core",
                mode="live",
                status="accepted",
                items=[exposed_entry, exposed_exit],
            )
            session.add(exposed_plan)
            session.flush()
            session.add(
                BrokerOrder(
                    execution_plan_item_id=exposed_entry.id,
                    broker_name="kis",
                    mode="live",
                    idempotency_key="plan-with-order:entry",
                    broker_order_id="broker-order-1",
                    symbol="000660",
                    side="buy",
                    status="open",
                    quantity=1,
                )
            )
            session.flush()

            exposed_result = watch_sizing.finalize_completed_no_trade_plan(
                db=session,
                settings=settings,
                plan=exposed_plan,
            )
            assert exposed_result["completed"] is False
            assert exposed_plan.status == "accepted"
            assert exposed_exit.status == "stop_sell_pending_config"
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("exposure_kind", "expected_completed"),
    [
        ("unlinked_open_order", False),
        ("unlinked_open_sell", False),
        ("unlinked_unknown_side", False),
        ("unlinked_unknown_order", False),
        ("unlinked_blank_venue", False),
        ("unlinked_whitespace_venue", False),
        ("unlinked_fill", False),
        ("base_quantity_position", False),
        ("safe_local_failure", True),
        ("foreign_provider_order", True),
        ("foreign_exchange_order", True),
    ],
)
def test_no_trade_finalizer_uses_conservative_external_exposure_truth(
    exposure_kind: str,
    expected_completed: bool,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        execution_position_account_scope="no-trade-exposure-account",
        kis_account_number="12345678",
        kis_account_product_code="01",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            entry = _watch_item(
                item_id=f"entry-{exposure_kind}",
                symbol="005930",
                risk_context={"exchange": "KRX"},
            )
            entry.status = "exec_agent_rejected"
            entry.source_plan_item_id = f"core-entry-{exposure_kind}"
            entry.provider = "kis"
            entry.venue = "KRX"
            entry.asset_class = "stock"
            entry.risk_context, entry.quantity_policy = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=entry.risk_context,
                quantity_policy=entry.quantity_policy,
            )
            protective_exit = ExecutionPlanItem(
                id=f"exit-{exposure_kind}",
                plan_item_index=1,
                symbol=entry.symbol,
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={
                    "type": "position_exit_from_entry",
                    "linked_entry_item_key": entry.source_plan_item_id,
                },
                provider="kis",
                venue="KRX",
                asset_class="stock",
            )
            plan = ExecutionPlan(
                id=f"plan-{exposure_kind}",
                external_plan_id=f"plan-{exposure_kind}",
                source="kai-trading-core",
                mode="live",
                status="accepted",
                items=[entry, protective_exit],
            )
            session.add(plan)
            session.flush()

            if exposure_kind == "base_quantity_position":
                identity = build_position_identity(
                    settings=settings,
                    provider="kis",
                    exchange="KRX",
                )
                session.add(
                    ExecutionPosition(
                        mode="live",
                        provider=identity.provider,
                        account_scope=identity.account_scope,
                        exchange=identity.exchange,
                        symbol=entry.symbol,
                        quantity=0,
                        base_quantity=Decimal("0.5"),
                        updated_from="broker_reconciliation",
                    )
                )
            else:
                order = BrokerOrder(
                    execution_plan_item_id=None,
                    broker_name=(
                        "alpaca"
                        if exposure_kind == "foreign_provider_order"
                        else "kis"
                    ),
                    mode="live",
                    idempotency_key=f"unlinked:{exposure_kind}",
                    broker_order_id=(
                        "external-123"
                        if exposure_kind
                        in {
                            "unlinked_open_order",
                            "unlinked_open_sell",
                            "unlinked_unknown_side",
                            "unlinked_unknown_order",
                            "unlinked_blank_venue",
                            "unlinked_whitespace_venue",
                            "foreign_provider_order",
                            "foreign_exchange_order",
                        }
                        else None
                    ),
                    symbol=entry.symbol,
                    side=(
                        "sell"
                        if exposure_kind == "unlinked_open_sell"
                        else "mystery"
                        if exposure_kind == "unlinked_unknown_side"
                        else "buy"
                    ),
                    status=(
                        "open"
                        if exposure_kind
                        in {
                            "unlinked_open_order",
                            "unlinked_open_sell",
                            "unlinked_unknown_side",
                            "unlinked_blank_venue",
                            "unlinked_whitespace_venue",
                            "foreign_provider_order",
                            "foreign_exchange_order",
                        }
                        else "mystery"
                        if exposure_kind == "unlinked_unknown_order"
                        else "failed"
                    ),
                    quantity=1,
                    venue=(
                        "NASDAQ"
                        if exposure_kind == "foreign_exchange_order"
                        else ""
                        if exposure_kind == "unlinked_blank_venue"
                        else "   "
                        if exposure_kind == "unlinked_whitespace_venue"
                        else "KRX"
                    ),
                )
                session.add(order)
                session.flush()
                if exposure_kind == "unlinked_fill":
                    session.add(
                        ExecutionFill(
                            broker_order_pk=order.id,
                            broker_fill_id="unlinked-finalizer-fill",
                            symbol=entry.symbol,
                            side="buy",
                            quantity=1,
                            price=Decimal("71000"),
                            filled_at=datetime(2026, 9, 9, 23, 35, tzinfo=UTC),
                        )
                    )
            session.flush()

            result = watch_sizing.finalize_completed_no_trade_plan(
                db=session,
                settings=settings,
                plan=plan,
            )

            assert result["completed"] is expected_completed
            assert plan.status == (
                "completed_no_trade" if expected_completed else "accepted"
            )
            assert protective_exit.status == (
                "not_applicable"
                if expected_completed
                else "stop_sell_pending_config"
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "scope_case",
    ["missing_binding", "mismatched_binding", "rotated_account"],
)
def test_no_trade_finalizer_requires_proven_execution_scope(
    scope_case: str,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "no-trade-finalizer-scope-secret-at-least-32-bytes"
        ),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            entry = _watch_item(
                item_id=f"entry-scope-{scope_case}",
                symbol="005930",
                risk_context={"exchange": "KRX"},
            )
            entry.status = "exec_agent_rejected"
            entry.source_plan_item_id = f"core-entry-scope-{scope_case}"
            entry.provider = "kis"
            entry.venue = "KRX"
            entry.asset_class = "stock"
            entry.risk_context, entry.quantity_policy = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=entry.risk_context,
                quantity_policy=entry.quantity_policy,
            )
            if scope_case == "missing_binding":
                entry.risk_context.pop("execution_account_route")
                entry.quantity_policy.pop("execution_account_route", None)
            elif scope_case == "mismatched_binding":
                mismatched = dict(entry.risk_context["execution_account_route"])
                mismatched["provider"] = "alpaca"
                entry.risk_context["execution_account_route"] = mismatched
                entry.quantity_policy["execution_account_route"] = mismatched
            call_settings = settings
            if scope_case == "rotated_account":
                call_settings = Settings(
                    database_url="sqlite:///:memory:",
                    runtime_mode="live",
                    kis_account_number="87654321",
                    kis_account_product_code="01",
                    account_drawdown_recovery_scope_secret=(
                        "no-trade-finalizer-scope-secret-at-least-32-bytes"
                    ),
                )
            protective_exit = ExecutionPlanItem(
                id=f"exit-scope-{scope_case}",
                plan_item_index=1,
                symbol=entry.symbol,
                side="sell",
                intent="stop_sell",
                status="stop_sell_pending_config",
                quantity_policy={
                    "type": "position_exit_from_entry",
                    "linked_entry_item_key": entry.source_plan_item_id,
                },
                provider="kis",
                venue="KRX",
                asset_class="stock",
            )
            plan = ExecutionPlan(
                id=f"plan-scope-{scope_case}",
                external_plan_id=f"plan-scope-{scope_case}",
                source="kai-trading-core",
                mode="live",
                status="accepted",
                items=[entry, protective_exit],
            )
            session.add(plan)
            session.flush()

            result = watch_sizing.finalize_completed_no_trade_plan(
                db=session,
                settings=call_settings,
                plan=plan,
            )

            assert result["completed"] is False
            assert plan.status == "accepted"
            assert protective_exit.status == "stop_sell_pending_config"
    finally:
        engine.dispose()


def test_runtime_metrics_expose_no_trade_retry_and_reason_families() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="live")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            current_market_date = datetime.now(ZoneInfo("Asia/Seoul")).date()
            terminal = _watch_item(
                item_id="terminal-entry",
                risk_context={
                    watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                        "reason_family": "stale_defer_must_not_win",
                        "disposition": "defer",
                    },
                },
            )
            terminal.status = "exec_agent_rejected"
            terminal.quantity_policy = {
                "kind": "exec_agent_rejected",
                "source": "exec_agent_watch_sizing",
                "reason_family": "reward_risk",
            }
            deferred = _watch_item(
                item_id="retry-entry",
                symbol="000660",
                risk_context={},
            )
            deferred.risk_context = {
                **deferred.risk_context,
                watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                    "reason_family": "account_snapshot",
                    "disposition": "retry",
                },
            }
            quarantined = _watch_item(
                item_id="quarantined-entry",
                symbol="035420",
                risk_context={},
            )
            quarantined.status = "contract_quarantined"
            session.add_all(
                [
                    ExecutionPlan(
                        id="no-trade-plan",
                        external_plan_id="no-trade-plan",
                        source="kai-trading-core",
                        mode="live",
                        market_date=current_market_date,
                        status="completed_no_trade",
                        items=[terminal],
                    ),
                    ExecutionPlan(
                        id="retry-plan",
                        external_plan_id="retry-plan",
                        source="kai-trading-core",
                        mode="live",
                        market_date=current_market_date,
                        status="accepted",
                        items=[deferred],
                    ),
                    ExecutionPlan(
                        id="quarantined-plan",
                        external_plan_id="quarantined-plan",
                        source="kai-trading-core",
                        mode="live",
                        market_date=current_market_date,
                        status="contract_quarantined",
                        items=[quarantined],
                    ),
                ]
            )
            session.commit()

            expected_metrics = {
                "completed_no_trade_plans": 1,
                "pending_retries": 1,
                "pending_deferrals": 0,
                "terminal_rejections": 1,
                "reason_families": {
                    "account_snapshot": 1,
                    "reward_risk": 1,
                },
                "current_market_date": current_market_date.isoformat(),
                "current_contract_quarantines": 1,
                "current_contract_quarantine_symbols": ["035420"],
                "current_terminal_rejection_symbols": ["005930"],
                "current_pending_retry_symbols": ["000660"],
                "current_pending_deferral_symbols": [],
            }
            assert runtime_execution_decision_metrics(
                settings=settings,
                db=session,
            ) == expected_metrics
            status = build_runtime_status(
                settings=settings,
                runtime=RuntimeMetadata(started_at=datetime(2026, 10, 7, tzinfo=UTC)),
                db=session,
            )
            assert status["execution_decisions"] == expected_metrics
            alerts = {
                alert["kind"]: alert
                for alert in status["actionable_alerts"]["alerts"]
            }
            assert alerts["current_execution_contract_quarantined"]["symbols"] == [
                "035420"
            ]
            assert alerts["current_execution_decisions_rejected"]["symbols"] == [
                "005930"
            ]
            assert alerts["current_execution_decisions_deferred"]["symbols"] == [
                "000660"
            ]
    finally:
        engine.dispose()


def test_watch_exec_agent_defer_expires() -> None:
    now = datetime(2026, 10, 1, 4, 0, tzinfo=UTC)
    item = ExecutionPlanItem(
        risk_context={
            watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                "retry_after": (now + timedelta(minutes=5)).isoformat()
            }
        }
    )
    assert watch_sizing._watch_exec_agent_defer_is_active(item, now=now)
    assert not watch_sizing._watch_exec_agent_defer_is_active(
        item,
        now=now + timedelta(minutes=6),
    )


def test_elapsed_price_watch_is_terminalized_before_agent_or_broker_call(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'elapsed-price-watch.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="20:00",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    item = _watch_item(
        item_id="elapsed-price-watch",
        symbol="028670",
        risk_context={
            "exchange": "KRX",
            "target_price": "7050",
            "stop_loss_price": "6400",
            watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                "reason_code": "reward_risk_hard_block",
                "reason_family": "reward_risk",
                "disposition": "defer",
                "retry_after": "2026-06-04T06:30:00+00:00",
                "expires_at": "2026-06-04T06:30:00+00:00",
                "price_watch_only": True,
            },
        },
        trigger_price="6630",
        limit_price="6650",
    )
    with session_factory() as db:
        db.add(
            ExecutionPlan(
                external_plan_id="elapsed-price-watch-plan",
                source="kai-trading-core",
                market_date=date(2026, 6, 4),
                mode="paper",
                status="accepted",
                items=[item],
            )
        )
        db.commit()

    agent_calls = 0

    def unexpected_agent_call(**_kwargs: object) -> dict[str, object]:
        nonlocal agent_calls
        agent_calls += 1
        raise AssertionError("elapsed price watch must not reach exec-agent")

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        unexpected_agent_call,
    )
    adapter = WatchSizingBrokerStub(reconcile_payloads=[])

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=adapter,
        now=datetime(2026, 6, 4, 6, 31, tzinfo=UTC),
    )

    assert agent_calls == 0
    assert "snapshot" not in adapter.events
    assert result.payload["applied"][0]["price_watch_expired_item_ids"] == [
        item.id
    ]
    with session_factory() as db:
        persisted = db.get(ExecutionPlanItem, item.id)
        assert persisted is not None
        assert persisted.status == "watch_expired"
        assert watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY not in persisted.risk_context
        _assert_canonical_terminal_rejection_policy(
            persisted.quantity_policy,
            reason_code="reward_risk_price_watch_expired",
        )
    engine.dispose()


@pytest.mark.parametrize(
    ("decision_at", "expected_approved", "expected_status"),
    [
        (
            datetime(2026, 6, 4, 6, 29, tzinfo=UTC),
            1,
            "watch_monitoring_active",
        ),
        (
            datetime(2026, 6, 4, 6, 31, tzinfo=UTC),
            0,
            "watch_expired",
        ),
    ],
    ids=["before-krx-close", "after-krx-close"],
)
def test_krx_close_deadline_controls_late_agent_approval(
    decision_at: datetime,
    expected_approved: int,
    expected_status: str,
) -> None:
    settings = Settings(runtime_mode="paper")
    item = _watch_item(
        item_id="elapsed-price-watch-approval",
        symbol="028670",
        risk_context={
            "exchange": "KRX",
            "target_price": "7050",
            "stop_loss_price": "6400",
            watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                "reason_code": "reward_risk_hard_block",
                "reason_family": "reward_risk",
                "disposition": "defer",
                "retry_after": "2026-06-04T06:28:00+00:00",
                "expires_at": "2026-06-04T06:30:00+00:00",
                "price_watch_only": True,
            },
        },
        trigger_price="6630",
        limit_price="6650",
    )
    plan = ExecutionPlan(
        external_plan_id="elapsed-price-watch-approval-plan",
        source="kai-trading-core",
        market_date=date(2026, 6, 4),
        mode="paper",
        status="accepted",
        items=[item],
    )
    request_payload = {"test_scope": "elapsed-price-watch-approval"}

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        request_payload=request_payload,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": plan.external_plan_id,
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=request_payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "action": "approve",
                    "quantity": 10,
                    "approved_notional_krw": "66300",
                    "sizing_price": "6630",
                    "reason_code": "approved",
                    "reason": "late approval must be ignored",
                }
            ],
        },
        account_context=_fresh_account_context(
            observed_at=decision_at.isoformat()
        ),
        expected_item_ids={item.id},
        now=decision_at,
    )

    assert summary["approved"] == expected_approved
    assert summary["rejected"] == 1 - expected_approved
    assert item.status == expected_status
    assert watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY not in item.risk_context
    if expected_status == "watch_expired":
        _assert_canonical_terminal_rejection_policy(
            item.quantity_policy,
            reason_code="reward_risk_price_watch_expired",
        )
    else:
        assert item.quantity_policy["kind"] == "shares"


@pytest.mark.parametrize("exchange", ["KRX", "KOSPI", "KOSDAQ"])
def test_price_watch_deadline_uses_regular_krx_close(exchange: str) -> None:
    settings = Settings(
        market_timezone="Asia/Seoul",
        exec_agent_watch_sizing_end_local_time="20:00",
    )
    item = _watch_item(
        risk_context={"exchange": exchange},
    )
    ExecutionPlan(market_date=date(2026, 6, 4), items=[item])

    assert watch_sizing._price_watch_session_expiry(
        settings=settings,
        item=item,
    ) == datetime(2026, 6, 4, 6, 30, tzinfo=UTC)


@pytest.mark.parametrize("exchange", ["NXT", "SOR", "NASDAQ"])
def test_price_watch_deadline_does_not_apply_to_unsupported_route(
    exchange: str,
) -> None:
    item = _watch_item(risk_context={"exchange": exchange})
    ExecutionPlan(market_date=date(2026, 6, 4), items=[item])

    assert watch_sizing._price_watch_session_expiry(
        settings=Settings(exec_agent_watch_sizing_end_local_time="20:00"),
        item=item,
    ) is None


def _watch_item(
    *,
    item_id: str = "watch-item-1",
    symbol: str = "005930",
    risk_context: dict[str, object] | None,
    trigger_price: str = "71000",
    limit_price: str = "71500",
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol=symbol,
        side="buy",
        intent="intraday_watch_buy",
        status="watch_sizing_pending",
        decision_agent_name="short-term-decider",
        trigger_price=Decimal(trigger_price),
        limit_price=Decimal(limit_price),
        quantity_policy={
            "type": "exec_agent_sized",
            "budget_krw": "1500000",
            "estimated_round_trip_cost_bps": "20",
        },
        risk_context=risk_context,
    )


def _next_open_watch_item(
    *,
    item_id: str = "next-open-item-1",
    symbol: str = "005930",
    estimated_reference_price: str = "71500",
    target_price: str | None = "76000",
    stop_price: str | None = "69000",
) -> ExecutionPlanItem:
    risk_context: dict[str, object] = {
        "exchange": "KRX",
        "estimated_round_trip_cost_bps": "20",
    }
    if target_price is not None:
        risk_context["target_price"] = target_price
    if stop_price is not None:
        risk_context["stop_price"] = stop_price
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol=symbol,
        side="buy",
        intent="next_open_buy",
        status="watch_sizing_pending",
        trigger_price=None,
        limit_price=None,
        time_in_force="day",
        quantity_policy={
            "type": "exec_agent_sized",
            "budget_krw": "150000",
            "estimated_reference_price": estimated_reference_price,
        },
        risk_context=risk_context,
        decision_agent_name="signal-decider",
    )


@pytest.mark.parametrize(
    "decision_source",
    ["openclaw_exec_agent_watch_sizing", "watch_exec_agent_sizing"],
)
def test_canonical_terminal_rejection_removes_all_executable_sizing(
    decision_source: str,
) -> None:
    item = _watch_item(
        item_id="canonical-terminal-reject",
        risk_context={
            "target_price": "76000",
            "stop_price": "69000",
            watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                "disposition": "defer",
                "retry_after": "2026-10-07T15:00:00+00:00",
            },
        },
    )
    previous_policy = deepcopy(item.quantity_policy)
    proposed = {
        "source": decision_source,
        "action": "approve",
        "decision": "approved",
        "execution_plan_item_id": item.id,
        "symbol": item.symbol,
        "quantity": 17,
        "approved_quantity": 17,
        "approved_notional_krw": "1215500.0000",
        "sizing_price": "71500.0000",
        "reference_price": "71500.0000",
        "confidence": 0.91,
        "target_profile": "short_swing",
        "target_tag": "target_profile:short_swing",
        "decided_at": "2026-10-07T12:00:00+00:00",
    }

    final_record, policy = watch_sizing._canonical_terminal_rejection(
        item=item,
        record=proposed,
        previous_policy=previous_policy,
        reason_code="allowed_action_envelope_blocked",
        reason_family="action_envelope",
        reason="deterministic envelope blocked the proposal",
        audit_event_type="watch_exec_agent_sizing_decision",
    )

    assert final_record["source"] == decision_source
    assert final_record["action"] == "reject"
    assert final_record["decision"] == "rejected"
    assert final_record["quantity"] == 0
    assert final_record["approved_quantity"] == 0
    assert final_record["approved_notional_krw"] is None
    assert final_record["sizing_price"] is None
    assert final_record["reference_price"] is None
    assert final_record["confidence"] is None
    assert final_record["target_profile"] is None
    assert final_record["target_tag"] is None
    assert final_record["rejection_disposition"] == "terminal"
    assert set(policy) == {
        "kind",
        "source",
        "reason",
        "reason_code",
        "reason_family",
        "rejection_disposition",
        "exec_agent_decision",
        "short_term_exec_agent_decision",
        "exec_agent_decision_audit",
        "previous_policy",
    }
    assert policy["previous_policy"] == previous_policy
    assert policy["exec_agent_decision"] == policy["short_term_exec_agent_decision"]
    assert policy["exec_agent_decision"]["quantity"] == 0
    assert policy["exec_agent_decision"]["approved_quantity"] == 0
    assert policy["exec_agent_decision"]["approved_notional_krw"] is None
    assert watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY not in item.risk_context


def _assert_canonical_terminal_rejection_policy(
    policy: dict[str, object],
    *,
    reason_code: str,
) -> None:
    assert set(policy) == {
        "kind",
        "source",
        "reason",
        "reason_code",
        "reason_family",
        "rejection_disposition",
        "exec_agent_decision",
        "short_term_exec_agent_decision",
        "exec_agent_decision_audit",
        "previous_policy",
    }
    assert policy["kind"] == "exec_agent_rejected"
    assert policy["source"] == "exec_agent_watch_sizing"
    assert policy["reason_code"] == reason_code
    assert policy["rejection_disposition"] == "terminal"
    decision = policy["exec_agent_decision"]
    assert isinstance(decision, dict)
    assert decision == policy["short_term_exec_agent_decision"]
    assert decision["action"] == "reject"
    assert decision["decision"] == "rejected"
    assert decision["quantity"] == 0
    assert decision["approved_quantity"] == 0
    assert decision["approved_notional_krw"] is None
    assert decision["sizing_price"] is None


def test_next_open_sizing_treats_kind_alias_as_pre_monitor_delegation() -> None:
    item = _next_open_watch_item()
    item.status = "pending"
    item.quantity_policy = {
        "type": "shares",
        "kind": "exec_agent_sized",
        "quantity": 7,
    }

    assert watch_sizing._is_untriggered_next_open_sizing_item(item) is True
    assert watch_sizing._eligible_unsized_item_status(item) is True


def _watch_veto_settings(**overrides) -> Settings:
    return Settings(
        runtime_mode="live",
        automation_enabled=True,
        market_regime_breadth_max_age_seconds=300,
        execution_position_account_scope="watch-sizing-test-route",
        **overrides,
    )


def _fresh_account_context(**overrides: object) -> dict[str, object]:
    context: dict[str, object] = {
        "available": True,
        "buy_sizing_available": True,
        "observed_at": datetime.now(UTC).isoformat(),
        "orderable_cash_krw": "10000000",
        "buying_power_krw": "10000000",
        "holdings": [],
        "open_orders": [],
    }
    context.update(overrides)
    return context


_DIRECT_APPLY_REQUEST_PAYLOAD: dict[str, object] = {
    "test_scope": "direct-watch-apply",
}


def _verified_sizing_runtime(
    settings: Settings | None = None,
    *,
    session_id: str = "test-watch-sizing-session",
    request_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved = settings or Settings()
    provider, model = resolved.exec_agent_model.split("/", 1)
    requested_session_key = f"test-requested-{session_id}"
    bound_request = request_payload or _DIRECT_APPLY_REQUEST_PAYLOAD
    request_sha256 = json_payload_hash(watch_sizing._json_safe(bound_request))
    return {
        "agent_id": resolved.exec_agent_id,
        "configured_agent_id": resolved.exec_agent_id,
        "configured_model_hint": resolved.exec_agent_model,
        "actual_agent_id": resolved.exec_agent_id,
        "actual_provider": provider,
        "actual_model": model,
        "actual_model_ref": resolved.exec_agent_model,
        "session_id": session_id,
        "requested_session_key": requested_session_key,
        "request_sha256": request_sha256,
        "runtime_contract": verified_runtime_contract_proof(
            expected_model_ref=resolved.exec_agent_model,
            requested_session_key=requested_session_key,
            reported_session_id=session_id,
            request_sha256=request_sha256,
        ),
    }


def _watch_veto_plan(
    *,
    settings: Settings,
    risk_context: dict[str, object],
    quantity_policy: dict[str, object] | None = None,
    symbol: str = "005930",
) -> tuple[object, object, str]:
    engine = create_db_engine(Settings(database_url="sqlite:///:memory:", runtime_mode=settings.runtime_mode))
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    session = session_factory()
    plan = ExecutionPlan(
        external_plan_id=f"plan-live-veto-{symbol}",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 8, 18),
        mode=settings.runtime_mode,
        items=[
            _watch_item(
                item_id=f"watch-veto-{symbol}",
                symbol=symbol,
                risk_context={
                    "exchange": "KRX",
                    "target_price": "78000",
                    "stop_price": "69000",
                    **risk_context,
                },
            )
        ],
    )
    item = plan.items[0]
    item.status = "watch_monitoring_active"
    item.quantity_policy = quantity_policy or {"kind": "shares", "quantity": 10}
    session.add(plan)
    session.commit()
    return engine, session, item.id


def _fresh_breadth(**overrides) -> dict[str, object]:
    payload: dict[str, object] = {
        "market": "KRX",
        "market_date": "2026-08-18",
        "observed_at": "2026-08-18T01:00:00+00:00",
        "up_ratio": "0.60",
        "advance_decline_ratio": "1.20",
    }
    payload.update(overrides)
    return payload


def _price(
    *,
    turnover: Decimal | None = Decimal("150000000"),
    volume: Decimal | None = Decimal("10000"),
    day_high: Decimal | None = Decimal("72000"),
    day_low: Decimal | None = Decimal("70000"),
) -> MonitoringPriceRequest:
    observed_at = datetime(2026, 8, 18, 1, 5, tzinfo=UTC)
    return MonitoringPriceRequest(
        symbol="005930",
        price=Decimal("71000"),
        observed_at=observed_at,
        freshness_source="kis_exchange_orderbook_time",
        bid=Decimal("70990"),
        ask=Decimal("71010"),
        turnover=turnover,
        volume=volume,
        day_high=day_high,
        day_low=day_low,
        provenance={
            "field_sources": {
                side: {
                    "freshness_source": "kis_exchange_orderbook_time",
                    "observed_at": observed_at.isoformat(),
                }
                for side in ("bid", "ask")
            }
        },
    )


def _resolve_live_watch_veto(
    *,
    settings: Settings,
    risk_context: dict[str, object],
    price: MonitoringPriceRequest,
    quantity_policy: dict[str, object] | None = None,
    resume_automated_actions: bool = False,
) -> tuple[object, dict[str, object] | None, dict[str, object] | None]:
    engine, session, item_id = _watch_veto_plan(
        settings=settings,
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    try:
        if resume_automated_actions:
            resume_result = resume_runtime(
                settings=settings,
                db=session,
                command=ControlCommand(
                    actor="test",
                    source="test",
                    reason="exercise an explicitly enabled watch-buy path",
                ),
            )
            assert resume_result.outcome in {"applied", "noop"}
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        approval = _resolve_watch_buy_order(
            settings=settings,
            db=session,
            adapter=WatchSizingBrokerStub(reconcile_payloads=[]),
            item=item,
            observed_price=price.price,
            price=price,
        )
        session.flush()
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.execution_plan_item_id == item_id,
                ExecutionPlanItemAudit.event_type == LIVE_WATCH_BUY_VETO_EVENT_TYPE,
            )
        )
        return (
            approval,
            audit.summary if audit is not None else None,
            audit.payload if audit is not None else None,
        )
    finally:
        session.close()
        engine.dispose()


def test_live_watch_buy_rejects_reported_unverified_watch_approval() -> None:
    approval, audit_summary, audit_payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={},
        price=_price(),
        quantity_policy={
            "kind": "shares",
            "quantity": 10,
            "source": "exec_agent_watch_sizing",
            "approval_provenance": {
                "kind": "reported_unverified_exec_agent_watch_sizing",
                "runtime": {"actual_agent_id": None},
            },
        },
    )

    assert approval.quantity is None
    assert approval.reason == "unverified_exec_agent_sizing"
    assert audit_summary is None
    assert audit_payload is None


def test_watch_sizing_treats_decimal_string_share_quantity_as_sized() -> None:
    assert _quantity_is_shares({"kind": "shares", "quantity": "24.00000000"})


def test_live_watch_buy_vetoes_risk_off_breadth_before_submission() -> None:
    approval, summary, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={"market_breadth": _fresh_breadth(up_ratio="0.20", advance_decline_ratio="0.45")},
        price=_price(),
    )

    assert approval.quantity is None
    assert approval.reason == "market_breadth_risk_off"
    assert summary is not None
    assert summary["status"] == "watch_buy_rejected"
    assert payload is not None
    assert payload["skipped_quantity"] == 10
    assert payload["skipped_notional_krw"] == "710000.0000"
    assert payload["market_regime_reason_code"] == "market_breadth_risk_off"


def test_live_watch_buy_stale_breadth_fails_closed_without_override() -> None:
    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={
            "market_breadth": _fresh_breadth(
                market_date="2026-08-17",
                observed_at="2026-08-17T01:00:00+00:00",
            )
        },
        price=_price(),
    )

    assert approval.quantity is None
    assert approval.reason == "market_breadth_stale"
    assert payload is not None
    assert payload["market_regime_reason_code"] == "market_breadth_stale_caution"
    assert payload["override"] is None


def test_live_watch_buy_without_breadth_observation_time_fails_closed() -> None:
    breadth = _fresh_breadth()
    breadth.pop("observed_at")

    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={"market_breadth": breadth},
        price=_price(),
    )

    assert approval.quantity is None
    assert approval.reason == "market_breadth_stale"
    assert payload is not None
    assert payload["market_regime_reason_code"] == "market_breadth_stale_caution"


@pytest.mark.parametrize(
    ("market_date", "observed_at"),
    [
        ("2026-08-18", "2026-08-18T01:00:00"),
        ("2026-08-18", "2026-08-18T01:10:00+00:00"),
        ("2026-08-19", "2026-08-18T01:00:00+00:00"),
    ],
)
def test_live_watch_buy_rejects_untrustworthy_breadth_time_provenance(
    market_date: str,
    observed_at: str,
) -> None:
    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={
            "market_breadth": _fresh_breadth(
                market_date=market_date,
                observed_at=observed_at,
            )
        },
        price=_price(),
    )

    assert approval.quantity is None
    assert approval.reason == "market_breadth_stale"
    assert payload is not None
    assert payload["market_regime_reason_code"] == "market_breadth_stale_caution"


def test_live_watch_buy_downsize_flag_false_blocks_market_guardrail_downsize_reasons() -> None:
    stale_breadth = _fresh_breadth(
        market_date="2026-08-17",
        observed_at="2026-08-17T01:00:00+00:00",
    )
    cases = [
        (
            "market_breadth_risk_off",
            {"market_breadth": _fresh_breadth(up_ratio="0.20", advance_decline_ratio="0.45")},
            "market_breadth_risk_off",
        ),
        (
            "market_breadth_caution",
            {"market_breadth": _fresh_breadth(up_ratio="0.42", advance_decline_ratio="1.10")},
            "market_breadth_caution",
        ),
        (
            "market_breadth_below_normal",
            {"market_breadth": _fresh_breadth(up_ratio="0.48", advance_decline_ratio="1.20")},
            "market_breadth_below_normal",
        ),
        (
            "market_breadth_missing_caution",
            {},
            "market_breadth_missing_caution",
        ),
        (
            "market_breadth_stale_caution",
            {"market_breadth": stale_breadth},
            "market_breadth_stale",
        ),
    ]

    for expected_reason_code, risk_context, expected_approval_reason in cases:
        approval, _, payload = _resolve_live_watch_veto(
            settings=_watch_veto_settings(watch_buy_live_market_regime_downsize_allowed=False),
            risk_context=risk_context,
            price=_price(),
        )

        assert approval.quantity is None, expected_reason_code
        assert approval.reason == expected_approval_reason
        assert payload is not None
        assert payload["market_regime_reason_code"] == expected_reason_code
        assert payload["market_regime_action"] == "downsize"


def test_live_watch_buy_downsize_flag_true_permits_market_guardrail_downsizing_including_stale() -> None:
    stale_breadth = _fresh_breadth(
        market_date="2026-08-17",
        observed_at="2026-08-17T01:00:00+00:00",
    )
    contexts = [
        {"market_breadth": _fresh_breadth(up_ratio="0.20", advance_decline_ratio="0.45")},
        {"market_breadth": _fresh_breadth(up_ratio="0.42", advance_decline_ratio="1.10")},
        {"market_breadth": _fresh_breadth(up_ratio="0.48", advance_decline_ratio="1.20")},
        {},
        {"market_breadth": stale_breadth},
    ]

    for risk_context in contexts:
        approval, _, payload = _resolve_live_watch_veto(
            settings=_watch_veto_settings(watch_buy_live_market_regime_downsize_allowed=True),
            risk_context=risk_context,
            price=_price(),
            resume_automated_actions=True,
        )

        assert approval.quantity is not None
        assert payload is None


def test_live_watch_buy_vetoes_zero_volume_and_turnover() -> None:
    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={"market_breadth": _fresh_breadth()},
        price=_price(volume=Decimal("0"), turnover=Decimal("0")),
    )

    assert approval.quantity is None
    assert approval.reason == "zero_trigger_volume"
    assert payload is not None
    assert payload["reasons"] == ["zero_trigger_volume", "zero_trigger_turnover"]
    assert payload["trigger_quality"]["turnover"] == "0.0000"


def test_live_watch_buy_vetoes_missing_day_high_low() -> None:
    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={"market_breadth": _fresh_breadth()},
        price=_price(day_high=None, day_low=None),
    )

    assert approval.quantity is None
    assert approval.reason == "missing_day_high_low"
    assert payload is not None
    assert payload["trigger_quality"]["day_high"] is None
    assert payload["trigger_quality"]["day_low"] is None


def test_live_watch_buy_vetoes_low_turnover_below_configured_default() -> None:
    approval, _, payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context={"market_breadth": _fresh_breadth()},
        price=_price(turnover=Decimal("14989100")),
    )

    assert approval.quantity is None
    assert approval.reason == "low_trigger_turnover"
    assert payload is not None
    assert payload["trigger_quality"]["min_turnover_krw"] == "100000000.0000"


def test_stale_breadth_override_and_paper_keep_existing_downsize_path() -> None:
    stale_context = {
        "market_breadth": _fresh_breadth(
            market_date="2026-08-17",
            observed_at="2026-08-17T01:00:00+00:00",
        ),
        "live_watch_buy_breadth_override": {
            "enabled": True,
            "actor": "ops",
            "reason": "manual breadth feed outage review accepted this symbol for live submission",
            "recorded_at": "2026-08-18T01:04:00+00:00",
        },
    }
    live_approval, _, live_payload = _resolve_live_watch_veto(
        settings=_watch_veto_settings(),
        risk_context=stale_context,
        price=_price(),
        resume_automated_actions=True,
    )
    assert live_approval.quantity is not None
    assert live_payload is None

    paper_approval, _, paper_payload = _resolve_live_watch_veto(
        settings=Settings(runtime_mode="paper", automation_enabled=True, market_regime_breadth_max_age_seconds=300),
        risk_context={"market_breadth": stale_context["market_breadth"]},
        price=_price(),
        resume_automated_actions=True,
    )
    assert paper_approval.quantity is not None
    assert paper_payload is None


def test_monitoring_cycle_persists_live_veto_status_and_operator_summaries() -> None:
    settings = _watch_veto_settings(database_url="sqlite:///:memory:")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            plan = ExecutionPlan(
                external_plan_id="plan-live-veto-cycle",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 8, 18),
                mode="live",
                items=[
                        _watch_item(
                            item_id="watch-live-cycle-veto",
                            symbol="005930",
                            trigger_price="71010",
                        risk_context={
                            "exchange": "KRX",
                            "target_price": "76000",
                            "stop_price": "69000",
                            "market_breadth": _fresh_breadth(up_ratio="0.20", advance_decline_ratio="0.45"),
                        },
                    )
                ],
            )
            item = plan.items[0]
            item.status = "watch_monitoring_active"
            item.provider = "kis"
            item.venue = "KRX"
            item.asset_class = "stock"
            item.quantity_policy = {"kind": "shares", "quantity": 10}
            item.risk_context, item.quantity_policy = bind_kis_account_route(
                settings=settings,
                provider="kis",
                risk_context=item.risk_context,
                quantity_policy=item.quantity_policy,
            )
            session.add(plan)
            session.commit()

            resume_result = resume_runtime(
                settings=settings,
                db=session,
                command=ControlCommand(
                    actor="test",
                    source="test",
                    reason="exercise live watch-buy veto handling",
                ),
            )
            assert resume_result.outcome == "applied"

            result = evaluate_monitoring(
                settings=settings,
                db=session,
                adapter=WatchSizingBrokerStub(reconcile_payloads=[]),
                command=MonitoringEvaluationRequest(prices=[_price()]),
                now=datetime(2026, 8, 18, 1, 5, tzinfo=UTC),
            )
            session.commit()

            assert result.status_code == 200
            item = session.get(ExecutionPlanItem, "watch-live-cycle-veto")
            assert item is not None
            # A live market veto is recoverable: retain the triggered item so a
            # later healthy observation can re-evaluate it instead of making
            # the market-state block terminal.
            assert item.status == "watch_buy_triggered_blocked"
            audit = session.scalar(
                select(ExecutionPlanItemAudit).where(
                    ExecutionPlanItemAudit.execution_plan_item_id == item.id,
                    ExecutionPlanItemAudit.event_type == LIVE_WATCH_BUY_VETO_EVENT_TYPE,
                )
            )
            assert audit is not None
            assert audit.payload["market_regime_guardrail"]["breadth"]["up_ratio"] == "0.2000"
            audit.occurred_at = datetime.now(UTC)
            session.commit()

            status = build_runtime_status(
                settings=settings,
                runtime=RuntimeMetadata(started_at=datetime(2026, 8, 18, 0, 0, tzinfo=UTC)),
                db=session,
            )
            board = build_runtime_daily_board(
                settings=settings,
                runtime=RuntimeMetadata(started_at=datetime(2026, 8, 18, 0, 0, tzinfo=UTC)),
                db=session,
            )
            assert status["watch_buy_vetoes"] == {"today_count": 1, "today_symbols": ["005930"]}
            assert board["watch_buy_vetoes"] == {"today_count": 1, "today_symbols": ["005930"]}
            assert board["symbols"][0]["latest_plan_item_status"] == "watch_buy_triggered_blocked"
    finally:
        engine.dispose()


def test_issue_231_weekly_candidate_replay_fixture_demonstrates_veto_impact() -> None:
    candidates = [
        {"symbol": "012160", "reason": "market_breadth_risk_off", "turnover": Decimal("32061344"), "closed_pnl": Decimal("-2950")},
        {"symbol": "049430", "reason": "market_breadth_risk_off", "turnover": Decimal("169866250"), "closed_pnl": Decimal("-6900")},
        {"symbol": "010170", "reason": "market_breadth_risk_off", "turnover": Decimal("37723485225"), "closed_pnl": Decimal("-4980")},
        {"symbol": "024840", "reason": "market_breadth_risk_off", "turnover": Decimal("8968735145"), "closed_pnl": Decimal("-1000")},
        {"symbol": "200230", "reason": "market_breadth_risk_off", "turnover": Decimal("24935945"), "closed_pnl": Decimal("-2320")},
        {"symbol": "020120", "reason": "market_breadth_stale_caution", "turnover": Decimal("0"), "closed_pnl": Decimal("0")},
        {"symbol": "000950", "reason": "market_breadth_normal", "turnover": Decimal("14989100"), "closed_pnl": Decimal("-44415")},
    ]

    rule_a_symbols = {
        row["symbol"]
        for row in candidates
        if row["reason"] in {"market_breadth_risk_off", "market_breadth_stale_caution"}
    }
    rule_b_symbols = {
        row["symbol"]
        for row in candidates
        if row["symbol"] in rule_a_symbols or row["turnover"] < Decimal("100000000")
    }
    rule_a_observed_closed_pnl_improvement = -sum(row["closed_pnl"] for row in candidates if row["symbol"] in rule_a_symbols)
    rule_b_observed_closed_pnl_improvement = -sum(row["closed_pnl"] for row in candidates if row["symbol"] in rule_b_symbols)

    assert sorted(rule_a_symbols) == ["010170", "012160", "020120", "024840", "049430", "200230"]
    assert "000950" in rule_b_symbols
    assert rule_a_observed_closed_pnl_improvement == Decimal("18150")
    assert rule_b_observed_closed_pnl_improvement == Decimal("62565")


def _stale_local_order(*, symbol: str = "005930", broker_order_id: str = "stale-open-1") -> BrokerOrder:
    identity = build_position_identity(
        settings=Settings(runtime_mode="paper"),
        provider="kis",
        exchange="KRX",
    )
    return BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key=f"stale:{broker_order_id}",
        broker_order_id=broker_order_id,
        symbol=symbol,
        side="buy",
        status=NormalizedBrokerOrderStatus.OPEN.value,
        quantity=2,
        submitted_at=datetime(2026, 6, 3, 0, 0, tzinfo=UTC),
        last_broker_sync_at=datetime(2026, 6, 3, 0, 0, tzinfo=UTC),
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


def test_chunks_splits_watch_candidates_into_small_batches() -> None:
    items = [{"execution_plan_item_id": f"item-{index}"} for index in range(53)]

    chunks = _chunks(items, 10)

    assert [len(chunk) for chunk in chunks] == [10, 10, 10, 10, 10, 3]


def test_default_watch_sizing_window_covers_nxt_after_session() -> None:
    settings = Settings()

    assert watch_sizing._inside_sizing_window(
        settings=settings,
        now=datetime(2026, 6, 4, 10, 59, tzinfo=UTC),
    )
    assert not watch_sizing._inside_sizing_window(
        settings=settings,
        now=datetime(2026, 6, 4, 11, 1, tzinfo=UTC),
    )


def test_blocked_watch_buy_still_loads_for_exec_agent_sizing(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-blocked.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-blocked-only",
            strategy_run_id="strategy-1",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            items=[
                _watch_item(
                    item_id="watch-item-blocked",
                    symbol="005930",
                    risk_context={"target_price": "76000", "stop_price": "69000"},
                )
            ],
        )
        plan.items[0].status = "watch_buy_triggered_blocked"
        session.add(plan)
        session.commit()

    with session_factory() as session:
        plans = watch_sizing._load_candidate_plans(
            settings=settings,
            db=session,
            market_date=date(2026, 6, 4),
        )

    assert [plan.external_plan_id for plan in plans] == ["plan-blocked-only"]


def test_live_watch_sizing_waits_when_account_snapshot_is_unavailable(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-live-account-unavailable.db'}",
        runtime_mode="live",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-live-account-unavailable",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="live",
                items=[
                    _watch_item(
                        item_id="watch-item-account-unavailable",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("exec-agent must not size live orders without broker account truth")
        ),
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=None,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["blocked"] is True
    assert result.payload["reason"] == "account_context_unavailable"
    assert result.payload["batches_processed"] == 0
    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-account-unavailable")
        assert item is not None
        assert item.status == "watch_sizing_pending"

    engine.dispose()


def test_live_watch_sizing_blocks_when_broker_open_order_truth_is_degraded(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-live-open-orders-degraded.db'}",
        runtime_mode="live",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-live-open-orders-degraded",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="live",
                items=[
                    _watch_item(
                        item_id="watch-item-open-orders-degraded",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("exec-agent must not size live buys from degraded open-order truth")
        ),
    )
    broker = WatchSizingBrokerStub(
        reconcile_payloads=[],
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
                observed_at=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
            source="kis:account_snapshot",
            open_orders=[],
            payload={
                "open_orders_error": {
                    "code": "KIER2570",
                    "message": "open-order inquiry unavailable",
                    "degraded": True,
                }
            },
        ),
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=broker,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["blocked"] is True
    assert result.payload["reason"] == "broker_open_orders_truth_unavailable"
    assert result.payload["batches_processed"] == 0
    account_context = result.payload["account_context"]
    assert account_context["available"] is True
    assert account_context["buy_sizing_available"] is False
    assert account_context["open_orders_truth"] == {
        "available": False,
        "status": "degraded",
        "source": "kis:account_snapshot",
        "observed_at": "2026-06-04T00:00:00+00:00",
        "error_code": "KIER2570",
        "reason": "broker open-order lookup returned degraded truth",
    }
    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-open-orders-degraded")
        assert item is not None
        assert item.status == "watch_sizing_pending"

    engine.dispose()


def test_paper_watch_envelope_warns_but_does_not_block_degraded_open_order_truth() -> None:
    item = _watch_item(
        risk_context={"target_price": "76000", "stop_loss_price": "69000"},
    )
    account_context = _account_context_payload(
        BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
            source="kis:account_snapshot",
            payload={"open_orders_error": {"code": "KIER2570", "degraded": True}},
        )
    )

    payload = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=ExecutionPlan(external_plan_id="plan-paper-degraded-open-orders"),
        candidates=[_candidate_payload(item)],
        account_context=account_context,
        candidate_universe=_candidate_universe_payload([_candidate_payload(item)]),
    )

    envelope = payload["candidates"][0]["allowed_action_envelope"]  # type: ignore[index]
    assert "buy" in envelope["allowed_actions"]
    assert "broker_open_orders_truth_unavailable" in envelope["soft_warnings"]
    assert "broker_open_orders_truth_unavailable" not in envelope["hard_reject_reasons"]


@pytest.mark.parametrize(
    ("snapshot_kwargs", "expected_reason"),
    [
        ({"holdings_complete": False}, "broker_holdings_truth_unavailable"),
        ({"open_orders_complete": False}, "broker_open_orders_truth_unavailable"),
    ],
)
def test_typed_snapshot_completeness_blocks_live_watch_sizing(
    snapshot_kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    account_context = _account_context_payload(
        BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
            source="kis:account_snapshot",
            **snapshot_kwargs,
        ),
        settings=Settings(runtime_mode="live"),
        provider="kis",
        observed_at=datetime.now(UTC),
    )

    assert account_context["available"] is True
    assert account_context["buy_sizing_available"] is False
    assert account_context["buy_sizing_block_reason"] == expected_reason
    assert watch_sizing._live_buy_sizing_block_reason(account_context) == expected_reason


def test_stale_open_order_unknown_broker_status_blocks_watch_sizing_before_agent(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-stale-unknown.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        stale_broker_order_reconciliation_threshold_seconds=60,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-stale-unknown",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id="watch-item-stale",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.add(_stale_local_order(symbol="005930", broker_order_id="stale-unknown-1"))
        session.commit()

    def fail_agent(**_) -> dict[str, object]:
        raise AssertionError("exec-agent should not be called for stale-state blocked symbols")

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fail_agent)
    broker = WatchSizingBrokerStub(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-unknown-1",
                "status": "mystery_status",
                "broker_status": "ZZ99",
                "message": "new broker status",
                "observed_at": datetime(2026, 6, 4, 0, 0, tzinfo=UTC).isoformat(),
                "payload": {"ord_stat": "ZZ99"},
            }
        ]
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=broker,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-stale")
        order = session.scalar(select(BrokerOrder).where(BrokerOrder.broker_order_id == "stale-unknown-1"))
        unknown_event = session.scalar(
            select(BrokerOrderEvent).where(BrokerOrderEvent.event_type == "reconcile_result_unrecognized_status")
        )
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type == "watch_exec_agent_sizing_stale_state_blocked"
            )
        )
        audit_payload = audit.payload if audit is not None else None

    assert broker.events.index("reconcile") < broker.events.index("snapshot")
    assert result.payload["applied"][0]["stale_state_blocked_item_ids"] == ["watch-item-stale"]
    assert item.status == "exec_agent_rejected"
    _assert_canonical_terminal_rejection_policy(
        item.quantity_policy,
        reason_code="stale_order_broker_truth_unconfirmed",
    )
    assert order.status == "stale_terminal_unknown"
    assert order.last_error_classification == "unknown_status"
    assert unknown_event.payload["raw_status_details"] == {
        "status": "mystery_status",
        "broker_status": "ZZ99",
        "message": "new broker status",
        "observed_at": "2026-06-04T00:00:00+00:00",
    }
    assert audit is not None
    assert audit_payload["stale_state_blockers"][0]["classification"] == (
        "stale-terminal-unknown"
    )
    engine.dispose()


def test_stale_open_order_block_survives_account_snapshot_failure(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-stale-snapshot-fail.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        stale_broker_order_reconciliation_threshold_seconds=60,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-stale-snapshot-fail",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id="watch-item-snapshot-fail",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.add(_stale_local_order(symbol="005930", broker_order_id="stale-snapshot-fail-1"))
        session.commit()

    def fail_agent(**_) -> dict[str, object]:
        raise AssertionError("exec-agent should not be called when stale blockers survive snapshot failure")

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fail_agent)
    broker = WatchSizingBrokerStub(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-snapshot-fail-1",
                "status": "mystery_status",
                "broker_status": "ZZ99",
                "message": "new broker status",
                "observed_at": datetime(2026, 6, 4, 0, 0, tzinfo=UTC).isoformat(),
                "payload": {"ord_stat": "ZZ99"},
            }
        ],
        snapshot_error=BrokerAdapterError("account snapshot timeout"),
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=broker,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-snapshot-fail")
        audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type == "watch_exec_agent_sizing_stale_state_blocked"
            )
        )
        audit_payload = audit.payload if audit is not None else None

    assert broker.events.index("reconcile") < broker.events.index("snapshot")
    assert result.payload["applied"][0]["stale_state_blocked_item_ids"] == ["watch-item-snapshot-fail"]
    assert item.status == "exec_agent_rejected"
    _assert_canonical_terminal_rejection_policy(
        item.quantity_policy,
        reason_code="stale_order_broker_truth_unconfirmed",
    )
    assert audit is not None
    assert audit_payload["stale_state_blockers"][0]["classification"] == (
        "stale-terminal-unknown"
    )
    engine.dispose()


def test_stale_open_order_successful_refresh_allows_watch_sizing(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-stale-filled.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        stale_broker_order_reconciliation_threshold_seconds=60,
        exec_agent_watch_sizing_high_conviction_min_notional_krw=Decimal("1400000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-stale-filled",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id="watch-item-refreshed",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.add(_stale_local_order(symbol="005930", broker_order_id="stale-filled-1"))
        session.commit()

    calls: list[list[str]] = []

    def approve_agent(*, payload, **_) -> dict[str, object]:
        calls.append([str(candidate["execution_plan_item_id"]) for candidate in payload["candidates"]])
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-refreshed",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 21,
                    "approved_notional_krw": 1501500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "refreshed_truth",
                    "reason": "stale local order refreshed to terminal broker state",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", approve_agent)
    broker = WatchSizingBrokerStub(
        reconcile_payloads=[
            {
                "broker_order_id": "stale-filled-1",
                "status": "filled",
                "broker_status": "체결",
                "filled_quantity": 2,
                "remaining_quantity": 0,
                "average_fill_price": "71000",
                "observed_at": datetime(2026, 6, 4, 0, 0, tzinfo=UTC).isoformat(),
            }
        ]
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=broker,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-refreshed")
        order = session.scalar(select(BrokerOrder).where(BrokerOrder.broker_order_id == "stale-filled-1"))

    assert broker.events.index("reconcile") < broker.events.index("snapshot")
    assert calls == [["watch-item-refreshed"]]
    assert result.payload["applied"][0]["approved"] == 1
    assert item.status == "watch_monitoring_active"
    assert item.quantity_policy["quantity"] == 20
    assert order.status == "filled"
    assert order.last_error_classification is None
    engine.dispose()


def test_watch_buy_candidate_budget_accepts_max_safe_notional() -> None:
    item = _watch_item(risk_context={})
    item.quantity_policy = {
        "type": "exec_agent_sized",
        "budget_krw": "0",
        "max_safe_notional_krw": 3400457,
    }

    assert _watch_buy_candidate_budget(item) == Decimal("3400457")


def test_candidate_payload_includes_available_decider_flow_short_sector_context() -> None:
    flow_short_sector_context = {
        "status": "available",
        "investor_flow": {
            "status": "available",
            "symbol_flow": {"investor_groups": {"foreign": {"net_value": 1200000}}},
            "market_flow": {"investor_groups": [{"investor_type": "foreign", "net_value": 800000000}]},
        },
        "short_selling": {
            "status": "available",
            "short_balance": {"short_balance_ratio_pct": 0.45},
        },
        "sector_peers": {
            "status": "available",
            "sector_name": "semiconductor",
            "peer_count": 1,
            "peers": [{"symbol": "000660", "change_pct": 0.9}],
        },
    }
    item = _watch_item(
        risk_context={
            "data_quality_warnings": ["No recent news/disclosures were captured."],
            "decider_handoff_context": {
                "decider_input_context": {
                    "pre_decider_enrichment_context": {
                        "schema_version": "pre_decider_enrichment_v1",
                        "status": "available",
                        "market_date": "2026-05-27",
                        "flow_short_sector_context": flow_short_sector_context,
                    }
                }
            },
        }
    )

    payload = _candidate_payload(item)

    assert payload["data_quality_warnings"] == ["No recent news/disclosures were captured."]
    assert payload["decider_flow_short_sector_context"] == {
        "source": (
            "risk_context.decider_handoff_context.decider_input_context."
            "pre_decider_enrichment_context"
        ),
        "schema_version": "pre_decider_enrichment_v1",
        "status": "available",
        "market_date": "2026-05-27",
        "flow_short_sector_context": flow_short_sector_context,
    }


def test_candidate_payload_preserves_unavailable_context_reason_and_source_metadata() -> None:
    item = _watch_item(
        risk_context={
            "decider_handoff_context": {
                "decider_input_context": {
                    "request_payload": {
                        "symbol_day_context": {
                            "pre_decider_enrichment_context": {
                                "schema_version": "pre_decider_enrichment_v1",
                                "status": "partial",
                                "market_date": "2026-05-27",
                                "flow_short_sector_context": {
                                    "status": "partial",
                                    "investor_flow": {
                                        "status": "unavailable",
                                        "reason": "no_stored_pykrx_enrichment_row_for_request_date",
                                        "missing_fields": ["symbol_flow", "market_flow"],
                                        "freshness": {
                                            "source_tables": ["daily_pykrx_enrichment_rows"],
                                            "source_provider": "pykrx",
                                        },
                                    },
                                    "short_selling": {
                                        "status": "unavailable",
                                        "reason": "no_stored_pykrx_short_selling_row_for_request_date",
                                        "freshness": {
                                            "source_tables": ["daily_pykrx_enrichment_rows"],
                                            "source_provider": "pykrx",
                                        },
                                    },
                                    "sector_peers": {
                                        "status": "unavailable",
                                        "reason": "no_stored_pykrx_sector_classification_row_for_request_date",
                                        "freshness": {
                                            "source_tables": ["daily_pykrx_enrichment_rows"],
                                            "source_provider": "pykrx",
                                        },
                                    },
                                },
                            }
                        }
                    }
                }
            },
        }
    )

    payload = _candidate_payload(item)

    decider_context = payload["decider_flow_short_sector_context"]
    assert isinstance(decider_context, dict)
    assert decider_context["source"] == (
        "risk_context.decider_handoff_context.decider_input_context.request_payload."
        "symbol_day_context.pre_decider_enrichment_context"
    )
    flow_context = decider_context["flow_short_sector_context"]
    assert isinstance(flow_context, dict)
    assert flow_context["investor_flow"]["reason"] == "no_stored_pykrx_enrichment_row_for_request_date"
    assert flow_context["investor_flow"]["freshness"]["source_tables"] == ["daily_pykrx_enrichment_rows"]
    assert flow_context["short_selling"]["reason"] == "no_stored_pykrx_short_selling_row_for_request_date"
    assert flow_context["sector_peers"]["reason"] == "no_stored_pykrx_sector_classification_row_for_request_date"


def test_candidate_payload_preserves_direct_core_flow_short_sector_context() -> None:
    flow_short_sector_context = {
        "status": "available",
        "investor_flow": {
            "status": "available",
            "symbol_flow": {
                "investor_groups": {
                    "foreign": {"net_value_krw": 1_250_000_000},
                },
            },
            "market_flow": {
                "investor_groups": [
                    {"name": "foreign", "net_value_krw": 8_500_000_000},
                ],
            },
        },
        "short_selling": {
            "status": "available",
            "short_volume": {"short_volume_ratio_pct": 2.1},
            "short_balance": {"short_balance_ratio_pct": 1.4},
        },
        "sector_peers": {
            "status": "available",
            "relative_state": "leading",
            "sector_name": "Semiconductors",
            "peer_count": 1,
            "peers": [{"symbol": "000660", "change_pct": 1.8}],
        },
        "market_breadth": {
            "status": "available",
            "breadth_state": "constructive",
        },
    }
    item = _watch_item(
        risk_context={"flow_short_sector_context": flow_short_sector_context}
    )

    payload = _candidate_payload(item)

    assert payload["decider_flow_short_sector_context"] == {
        "source": "risk_context",
        "flow_short_sector_context": flow_short_sector_context,
    }


def test_candidate_payload_includes_direct_flow_short_sector_context_blocks() -> None:
    item = _watch_item(
        risk_context={
            "investor_flow_context": {
                "status": "unavailable",
                "reason": "no_stored_pykrx_enrichment_row_for_request_date",
                "source_date": "2026-05-26",
            },
            "short_selling_context": {
                "status": "available",
                "short_volume_ratio_pct": 2.4,
                "source_table": "daily_pykrx_short_selling",
            },
            "sector_peer_context": {
                "status": "available",
                "sector_name": "semiconductor",
                "peer_count": 3,
            },
        }
    )

    payload = _candidate_payload(item)

    decider_context = payload["decider_flow_short_sector_context"]
    assert isinstance(decider_context, dict)
    assert decider_context["source"] == "risk_context"
    flow_context = decider_context["flow_short_sector_context"]
    assert isinstance(flow_context, dict)
    assert flow_context["investor_flow"]["reason"] == "no_stored_pykrx_enrichment_row_for_request_date"
    assert flow_context["short_selling"]["source_table"] == "daily_pykrx_short_selling"
    assert flow_context["sector_peers"]["peer_count"] == 3


def test_candidate_payload_omits_decider_flow_short_sector_context_for_legacy_payloads() -> None:
    item = _watch_item(
        risk_context={
            "data_quality_warnings": ["Investor flow data is missing."],
            "supporting_evidence": ["legacy evidence"],
        }
    )

    payload = _candidate_payload(item)

    assert "decider_flow_short_sector_context" not in payload
    assert payload["data_quality_warnings"] == ["Investor flow data is missing."]
    assert payload["supporting_evidence"] == ["legacy evidence"]


def test_sizing_request_payload_includes_allowed_action_envelope() -> None:
    settings = Settings(runtime_mode="live")
    item = _watch_item(
        risk_context={
            "target_price": "78000",
            "stop_price": "69000",
            "execution_feasibility_context": {"max_safe_notional_krw": "800000"},
        }
    )
    candidate = _candidate_payload(item)

    payload = _sizing_request_payload(
        settings=settings,
        plan=ExecutionPlan(
            external_plan_id="plan-envelope",
            strategy_run_id="strategy-1",
            market_date=date(2026, 6, 4),
        ),
        candidates=[candidate],
        account_context=_fresh_account_context(),
        candidate_universe=_candidate_universe_payload([candidate]),
    )

    envelope = payload["candidates"][0]["allowed_action_envelope"]  # type: ignore[index]
    assert envelope["schema_version"] == "allowed_action_envelope_v1"
    assert "buy" in envelope["allowed_actions"]
    assert envelope["max_quantity"] == 11
    assert envelope["max_notional_krw"] == "800000.0000"


def _delegated_gate_context() -> dict[str, object]:
    observed_at = datetime.now(UTC) - timedelta(hours=1)
    expiry_date = (observed_at + timedelta(days=2)).astimezone(
        ZoneInfo("Asia/Seoul")
    ).date()
    expires_at = datetime(
        expiry_date.year,
        expiry_date.month,
        expiry_date.day,
        15,
        30,
        tzinfo=ZoneInfo("Asia/Seoul"),
    )
    codes = [
        "short_term_liquidity_floor_block_delegated_to_exec_watch",
        "short_term_reward_risk_ratio_block_delegated_to_exec_watch",
        "transaction_cost_evidence_pending_live_delegated_to_exec_watch",
    ]
    return {
        "target_price": "78000",
        "stop_price": "69000",
        "delegated_gate_codes": codes,
        "delegated_gate_confirmation_required": True,
        "delegated_gates": [
            {
                "code": codes[0],
                "observed_value": "2500000000",
                "field_path": "risk_context.validation.short_term_entry_quality.raw_trading_value_krw",
                "comparator": "gte",
                "threshold": "3000000000",
                "policy_version": "core_delegated_gate_policy_v1",
                "observed_at": observed_at.isoformat(),
                "recheck_source": "exec_trigger_time_market_liquidity",
                "expires_at": expires_at.isoformat(),
            },
            {
                "code": codes[1],
                "observed_value": "1.50",
                "field_path": "risk_context.validation.short_term_entry_quality.reward_risk_ratio",
                "comparator": "gte",
                "threshold": "1.80",
                "policy_version": "core_delegated_gate_policy_v1",
                "observed_at": observed_at.isoformat(),
                "recheck_source": "exec_trigger_time_price_geometry_and_cost_model",
                "expires_at": expires_at.isoformat(),
            },
            {
                "code": codes[2],
                "observed_value": None,
                "field_path": "risk_context.execution_feasibility_context.expected_slippage_bps",
                "comparator": "finite_non_negative_with_basis",
                "threshold": "0",
                "policy_version": "core_delegated_gate_policy_v1",
                "observed_at": observed_at.isoformat(),
                "recheck_source": "exec_trigger_time_quote_and_cost_model",
                "expires_at": expires_at.isoformat(),
            },
        ],
        "execution_feasibility_context": {
            "expected_slippage_bps": None,
            "expected_slippage_basis": "unavailable",
        },
    }


def test_sizing_request_payload_blocks_existing_holding_in_allowed_action_envelope() -> None:
    settings = Settings(runtime_mode="live")
    item = _watch_item(risk_context={"target_price": "76000", "stop_price": "69000"})
    candidate = _candidate_payload(item)

    payload = _sizing_request_payload(
        settings=settings,
        plan=ExecutionPlan(external_plan_id="plan-envelope-holding", market_date=date(2026, 6, 4)),
        candidates=[candidate],
        account_context=_fresh_account_context(
            holdings=[{"symbol": "005930", "quantity": 3}],
        ),
        candidate_universe=_candidate_universe_payload([candidate]),
    )

    envelope = payload["candidates"][0]["allowed_action_envelope"]  # type: ignore[index]
    assert envelope["allowed_actions"] == ["hold", "reject"]
    assert "existing_holding" in envelope["hard_reject_reasons"]


def test_watch_sizing_decision_record_tags_tactical_target_profile() -> None:
    item = _watch_item(
        risk_context={
            "target_price": "72420",
            "stop_loss_price": "70290",
        }
    )

    record = _decision_record(
        decision={
            "action": "approve",
            "quantity": 2,
            "approved_notional_krw": 143000,
            "sizing_price": "71000",
            "confidence": 0.68,
            "reason_code": "small_tactical_upside",
            "reason": "2% target with tight stop",
        },
        item=item,
        version="manual-exec-agent-watch-sizing-v1",
    )

    assert record["target_profile"] == "tactical_2pct"
    assert record["target_tag"] == "target_profile:tactical_2pct"


def test_apply_decisions_preserves_triggered_blocked_watch_buy_for_retry() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-blocked-approved",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
                _watch_item(
                    item_id="watch-item-blocked",
                    symbol="005930",
                    risk_context={"target_price": "74000", "stop_price": "70950"},
                ),
        ],
    )
    plan.items[0].status = "watch_buy_triggered_blocked"

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-item-blocked"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-blocked",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 2,
                    "approved_notional_krw": 143000,
                    "sizing_price": "71000",
                    "confidence": 0.68,
                    "target_profile": "tactical_2pct",
                    "reason_code": "small_tactical_upside",
                    "reason": "2% target with tight stop",
                }
            ],
        },
    )

    assert summary["approved"] == 1
    assert plan.items[0].status == "watch_buy_triggered_blocked"
    assert plan.items[0].quantity_policy["quantity"] == 2


def test_paper_intraday_approval_without_contract_proof_is_rejected() -> None:
    plan = ExecutionPlan(
        id="plan-contract-proof-required",
        external_plan_id="plan-contract-proof-required",
        strategy_run_id="strategy-contract-proof-required",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-contract-proof-required",
                risk_context={"target_price": "78000", "stop_price": "69000"},
            )
        ],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-contract-proof-required"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": {
                "actual_agent_id": Settings().exec_agent_id,
                "actual_model_ref": Settings().exec_agent_model,
                "session_id": "missing-contract-proof",
            },
            "decisions": [
                {
                    "execution_plan_item_id": "watch-contract-proof-required",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "71500",
                    "reason_code": "approve_without_contract",
                    "reason": "must fail closed",
                }
            ],
        },
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert plan.items[0].status == "exec_agent_rejected"
    assert plan.items[0].quantity_policy["reason_code"] == (
        "unverified_exec_agent_runtime"
    )


def test_apply_decisions_allows_small_agent_probe_below_min_notional() -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-min-notional",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
                _watch_item(
                    item_id="watch-item-1",
                    symbol="005930",
                    trigger_price="50000",
                    limit_price="50500",
                    risk_context={
                    "target_price": "78000",
                    "stop_price": "49000",
                    watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY: {
                        "reason_family": "reward_risk",
                        "disposition": "defer",
                        "retry_after": "2026-06-03T23:00:00+00:00",
                    },
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "5000000",
                        "liquidity_tier": "high",
                    },
                },
            )
        ],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 3,
                    "approved_notional_krw": 151500,
                    "sizing_price": "50500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe",
                    "reason": "agent returned a small probe",
                }
            ],
        },
    )

    assert summary["approved"] == 1
    assert summary["rejected"] == 0
    assert plan.items[0].status == "watch_monitoring_active"
    policy = plan.items[0].quantity_policy
    assert policy["kind"] == "shares"
    assert policy["quantity"] == 3
    assert policy["sizing_adjustment"]["applied"] is False
    assert policy["sizing_adjustment"]["agent_approved_quantity"] == 3
    assert policy["sizing_adjustment"]["agent_approved_notional"] == "151500.0000"
    assert policy["sizing_adjustment"]["minimum_required_notional"] == "1500000.0000"
    assert policy["sizing_adjustment"]["cap_notional"] == "151500.0000"
    assert policy["sizing_adjustment"]["final_quantity"] == 3
    assert policy["sizing_adjustment"]["final_notional"] == "151500.0000"
    assert policy["sizing_adjustment"]["decision"] == "submit"
    assert policy["sizing_adjustment"]["min_notional_upsize_suppressed"] is True
    assert watch_sizing.WATCH_EXEC_AGENT_DEFER_KEY not in plan.items[0].risk_context


def test_apply_decisions_allows_when_order_lot_minimum_exceeds_agent_cap() -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-min-notional-cap",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                trigger_price="2000",
                limit_price="2030",
                risk_context={
                    "target_price": "2300",
                    "stop_price": "1900",
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "800000",
                        "liquidity_tier": "high",
                    },
                },
            )
        ],
    )
    _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 200,
                    "approved_notional_krw": 406000,
                    "sizing_price": "2030",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe",
                    "reason": "agent returned a small probe",
                }
            ],
        },
    )

    policy = plan.items[0].quantity_policy
    assert plan.items[0].status == "watch_monitoring_active"
    assert policy["kind"] == "shares"
    assert policy["quantity"] == 200
    assert policy["sizing_adjustment"]["applied"] is False
    assert policy["sizing_adjustment"]["agent_approved_quantity"] == 200
    assert policy["sizing_adjustment"]["agent_approved_notional"] == "406000.0000"
    assert policy["sizing_adjustment"]["minimum_required_notional"] == "1500000.0000"
    assert policy["sizing_adjustment"]["cap_notional"] == "406000.0000"
    assert policy["sizing_adjustment"]["final_quantity"] == 200
    assert policy["sizing_adjustment"]["final_notional"] == "406000.0000"
    assert policy["sizing_adjustment"]["decision"] == "submit"
    assert policy["sizing_adjustment"]["min_notional_upsize_suppressed"] is True


def test_apply_decisions_records_vol_corr_cap_details_when_enabled() -> None:
    settings = Settings(runtime_mode="live", vol_corr_sizing_enabled=True)
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-vol-corr-cap",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={
                    "target_price": "78000",
                    "stop_price": "69000",
                    "realized_volatility_pct": "5.0",
                    "max_holding_correlation": "0.82",
                    "correlation_symbol": "000660",
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "5000000",
                        "liquidity_tier": "high",
                    },
                },
            )
        ],
    )
    plan.items[0].quantity_policy = {
        "type": "exec_agent_sized",
        "budget_krw": "5000000",
        "estimated_round_trip_cost_bps": "20",
    }

    _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(
            holdings=[{"symbol": "000660", "quantity": 3}],
        ),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 40,
                    "approved_notional_krw": 2860000,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe",
                    "reason": "agent returned a small probe",
                }
            ],
        },
    )

    policy = plan.items[0].quantity_policy
    assert policy["quantity"] == 24
    assert policy["approved_notional"] == "1716000.0000"
    vol_corr_cap = policy["sizing_adjustment"]["vol_corr_cap"]
    assert vol_corr_cap["baseline_quantity"] == 40
    assert vol_corr_cap["volatility_multiplier"] == "0.7500"
    assert vol_corr_cap["correlation_multiplier"] == "0.8000"
    assert vol_corr_cap["vol_corr_multiplier"] == "0.6000"
    assert vol_corr_cap["vol_corr_cap_reason"] == "high_volatility+high_correlation"
    assert policy["sizing_adjustment"]["min_notional_upsize_suppressed"] is False


def test_apply_decisions_clamps_agent_approval_that_exceeds_cap() -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-approval-cap",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={
                    "target_price": "78000",
                    "stop_price": "69000",
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "800000",
                        "liquidity_tier": "high",
                    },
                },
            )
        ],
    )

    _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 100,
                    "approved_notional_krw": 7150000,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "agent_oversized",
                    "reason": "agent approval exceeds max safe notional",
                }
            ],
        },
    )

    policy = plan.items[0].quantity_policy
    assert plan.items[0].status == "watch_monitoring_active"
    assert policy["kind"] == "shares"
    assert policy["quantity"] == 11
    assert policy["sizing_adjustment"]["final_quantity"] == 11
    assert policy["sizing_adjustment"]["final_notional"] == "786500.0000"
    assert policy["sizing_adjustment"]["decision"] == "submit"
    assert policy["sizing_adjustment"]["agent_quantity"] == 100
    assert policy["sizing_adjustment"]["cap_notional"] == "800000.0000"
    assert policy["allowed_action_envelope"]["max_quantity"] == 11


def test_apply_decisions_rejects_when_allowed_action_envelope_has_no_quantity() -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-envelope-blocked",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(
            orderable_cash_krw="1000",
            buying_power_krw="1000",
        ),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "agent_approval",
                    "reason": "agent approval despite insufficient cash",
                }
            ],
        },
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert plan.items[0].status == "exec_agent_rejected"
    _assert_canonical_terminal_rejection_policy(
        plan.items[0].quantity_policy,
        reason_code="allowed_action_envelope_blocked",
    )


def test_apply_decisions_rejects_when_allowed_action_envelope_has_hard_reject_reason() -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-envelope-hard-reject",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(
            holdings=[{"symbol": "005930", "quantity": 1}],
        ),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "agent_approval",
                    "reason": "agent approval despite existing holding",
                }
            ],
        },
    )

    policy = plan.items[0].quantity_policy
    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert plan.items[0].status == "exec_agent_rejected"
    assert policy["reason_code"] == "allowed_action_envelope_blocked"
    _assert_canonical_terminal_rejection_policy(
        policy,
        reason_code="allowed_action_envelope_blocked",
    )
    assert "existing_holding" in policy["reason"]


def test_apply_decisions_rejects_when_allowed_action_envelope_disallows_buy_downsize(monkeypatch) -> None:
    settings = Settings(runtime_mode="live")
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-envelope-disallows-buy",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    def _blocked_envelope(**_: object) -> dict[str, object]:
        return {
            "schema_version": "allowed_action_envelope_v1",
            "allowed_actions": ["hold", "reject"],
            "max_quantity": 5,
            "max_notional_krw": "357500.0000",
            "hard_reject_reasons": [],
            "soft_warnings": [],
        }

    monkeypatch.setattr(watch_sizing, "_allowed_action_envelope_for_item", _blocked_envelope)

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={"watch-item-1"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "agent_approval",
                    "reason": "agent approval despite disallowed actions",
                }
            ],
        },
    )

    policy = plan.items[0].quantity_policy
    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert plan.items[0].status == "exec_agent_rejected"
    assert policy["reason_code"] == "allowed_action_envelope_blocked"
    _assert_canonical_terminal_rejection_policy(
        policy,
        reason_code="allowed_action_envelope_blocked",
    )
    assert policy["reason"] == "allowed action envelope disallows buy/downsize approval"


@pytest.mark.parametrize(
    "terminal_path",
    ["sizing_skip", "zero_quantity", "agent_terminal"],
)
def test_apply_decisions_canonicalizes_every_terminal_rejection_path(
    terminal_path: str,
    monkeypatch,
) -> None:
    settings = Settings(runtime_mode="live")
    item = _watch_item(
        item_id=f"terminal-{terminal_path}",
        risk_context={"target_price": "76000", "stop_price": "69000"},
    )
    plan = ExecutionPlan(
        id=f"plan-{terminal_path}",
        external_plan_id=f"plan-{terminal_path}",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[item],
    )

    monkeypatch.setattr(
        watch_sizing,
        "_allowed_action_envelope_for_item",
        lambda **_: {
            "allowed_actions": ["buy", "downsize", "reject"],
            "max_quantity": 3,
            "max_notional_krw": "214500.0000",
            "hard_reject_reasons": [],
            "soft_warnings": [],
        },
    )
    if terminal_path in {"sizing_skip", "zero_quantity"}:
        monkeypatch.setattr(
            watch_sizing,
            "_adjust_approved_watch_quantity",
            lambda **_: {
                "decision": "skip" if terminal_path == "sizing_skip" else "submit",
                "reason_code": "minimum_notional_unreachable",
                "reason": "deterministic sizing left no executable quantity",
                "quantity": 0,
                "approved_notional": Decimal("0"),
                "sizing_price": Decimal("71500"),
                "adjustment": {
                    "agent_approved_quantity": 3,
                    "agent_approved_notional": "214500.0000",
                    "final_quantity": 0,
                    "final_notional": "0.0000",
                },
            },
        )

    action = "reject" if terminal_path == "agent_terminal" else "approve"
    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        expected_item_ids={item.id},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "action": action,
                    "quantity": 3,
                    "approved_notional_krw": "214500.0000",
                    "sizing_price": "71500",
                    "confidence": 0.9,
                    "target_profile": "short_swing",
                    "reason_code": "minimum_notional_unreachable",
                    "reason": "proposal must not survive a terminal rejection",
                }
            ],
        },
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert item.status == "exec_agent_rejected"
    expected_reason_code = (
        "allowed_action_envelope_blocked"
        if terminal_path == "zero_quantity"
        else "minimum_notional_unreachable"
    )
    _assert_canonical_terminal_rejection_policy(
        item.quantity_policy,
        reason_code=expected_reason_code,
    )


def test_applicable_decision_ids_only_counts_expected_candidates() -> None:
    decision_ids = _applicable_decision_ids(
        {
            "decisions": [
                {"execution_plan_item_id": "watch-item-1", "action": "approve"},
                {"execution_plan_item_id": "unrelated-item", "action": "approve"},
            ]
        },
        expected_item_ids={"watch-item-1", "watch-item-2"},
    )

    assert decision_ids == {"watch-item-1"}


def test_sizing_request_payload_uses_compact_account_and_candidate_universe() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-compact",
        strategy_run_id="strategy-1",
        market_date=date(2026, 6, 4),
    )
    candidates = [
        _candidate_payload(
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={
                    "target_price": "72420",
                    "confidence_score_10": "7.2",
                    "decider_handoff_context": {
                        "large_unused_blob": "x" * 1000,
                    },
                },
            )
        ),
        _candidate_payload(
            _watch_item(
                item_id="watch-item-2",
                symbol="000660",
                risk_context={"target_price": "128000", "confidence_score_10": "6.8"},
            )
        ),
    ]
    account_context = _account_context_payload(
        BrokerAccountSnapshot(
            available_cash=Decimal("800000"),
            buying_power=Decimal("700000"),
            source="stub:account_snapshot",
            holdings=[BrokerAccountHolding(symbol="035420", quantity=3, average_price=Decimal("180000"))],
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol="009150",
                    side="buy",
                    quantity=2,
                    limit_price=Decimal("150000"),
                )
            ],
        )
    )

    payload = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[candidates[0]],
        account_context=account_context,
        candidate_universe=_candidate_universe_payload(candidates),
    )

    assert payload["account_context"]["orderable_cash_krw"] == "800000"
    assert payload["account_context"]["buying_power_krw"] == "700000"
    assert payload["account_context"]["holding_count"] == 1
    assert payload["candidate_universe"]["candidate_count"] == 2
    assert payload["candidate_universe"]["candidates"][1]["symbol"] == "000660"
    assert payload["candidates"][0]["symbol"] == "005930"
    assert "large_unused_blob" not in str(payload)


def test_apply_decisions_persists_audit_without_rewriting_large_risk_context(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-large-risk.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    large_context = {
        "target_price": "76000",
        "stop_price": "69000",
        "large_exec_agent_input": "x" * 600_000,
    }

    with session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-large-risk",
            strategy_run_id="strategy-1",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            items=[
                _watch_item(
                    item_id="watch-item-large-risk",
                    symbol="005930",
                    risk_context=large_context,
                )
            ],
        )
        session.add(plan)
        session.commit()

    with session_factory() as session:
        plan = watch_sizing._load_plan_for_update(db=session, plan_id=plan.id)
        _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            account_context={
                "available": True,
                "orderable_cash_krw": "10000000",
                "buying_power_krw": "10000000",
            },
            expected_item_ids={"watch-item-large-risk"},
            request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "_runtime_provenance": _verified_sizing_runtime(settings),
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-large-risk",
                        "symbol": "005930",
                        "action": "approve",
                        "quantity": 3,
                        "approved_notional_krw": 214500,
                        "sizing_price": "71500",
                        "confidence": 0.68,
                        "target_profile": "short_swing",
                        "reason_code": "small_probe",
                        "reason": "agent returned a small probe",
                    }
                ],
            },
        )
        session.commit()

    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-large-risk")
        assert item is not None
        assert item.risk_context == large_context
        assert item.status == "watch_monitoring_active"
        assert item.quantity_policy["kind"] == "shares"
        assert item.quantity_policy["quantity"] == 3
        audit_count = session.query(watch_sizing.ExecutionPlanItemAudit).count()
        assert audit_count == 1
        decision_audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type == "watch_exec_agent_sizing_decision"
            )
        )
        assert decision_audit is not None
        assert decision_audit.summary["action"] == "approve"


def test_short_transaction_no_decision_attempts_persist_for_monitoring_fallback(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-no-decision.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_max_attempts=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    large_context = {
        "target_price": "76000",
        "stop_price": "69000",
        "large_exec_agent_input": "x" * 600_000,
    }

    with session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-no-decision",
            strategy_run_id="strategy-1",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            items=[
                _watch_item(
                    item_id="watch-item-no-decision",
                    symbol="005930",
                    risk_context=large_context,
                )
            ],
        )
        session.add(plan)
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        lambda **_: {"version": "manual-exec-agent-watch-sizing-v1", "decisions": []},
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["errors"] == [
        {
            "plan_id": "plan-no-decision",
            "item_ids": ["watch-item-no-decision"],
            "error": "exec-agent did not return applicable decisions after max attempts",
        }
    ]

    with session_factory() as session:
        item = session.get(ExecutionPlanItem, "watch-item-no-decision")
        assert item is not None
        assert item.risk_context == large_context
        assert item.quantity_policy["exec_agent_watch_sizing_attempt_count"] == 2
        assert item.quantity_policy["exec_agent_watch_sizing_max_attempts"] == 2
        assert item.quantity_policy["exec_agent_watch_sizing_fallback_allowed"] is False
        assert item.quantity_policy["exec_agent_watch_sizing_attempts"][-1]["success"] is False
        assert _watch_buy_exec_agent_sizing_fallback_allowed(settings=settings, item=item) is False


def test_concurrent_watch_sizing_cycles_with_large_risk_contexts_do_not_lock(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-contention.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=4,
        exec_agent_watch_sizing_max_batches_per_cycle=10,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    large_blob = "x" * 550_000

    with session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-contention",
            strategy_run_id="strategy-1",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            items=[
                _watch_item(
                    item_id=f"watch-item-{index}",
                    symbol=f"0059{index:02d}",
                    risk_context={
                        "target_price": "76000",
                        "stop_price": "69000",
                        "large_exec_agent_input": large_blob,
                    },
                )
                for index in range(12)
            ],
        )
        session.add(plan)
        session.commit()

    def fake_decisions(*, payload, **_) -> dict[str, object]:
            return {
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": payload["plan_id"],
                "_runtime_provenance": _verified_sizing_runtime(
                    settings,
                    request_payload=payload,
                ),
                "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "reject",
                    "quantity": 0,
                    "approved_notional_krw": 0,
                    "sizing_price": candidate["chase_limit_price"] or candidate["trigger_price"],
                    "confidence": 0.2,
                    "target_profile": "tactical_2pct",
                    "reason_code": "test_reject",
                    "reason": "fixture rejection",
                }
                for candidate in payload["candidates"]
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fake_decisions)

    results: list[object] = []
    errors: list[BaseException] = []

    stop_monitoring = threading.Event()
    monitoring_symbols: list[list[str]] = []

    def run_monitoring_reads() -> None:
        try:
            while not stop_monitoring.is_set():
                with session_factory() as session:
                    monitoring_symbols.append(
                        list_autonomous_monitoring_symbols(
                            settings=settings,
                            db=session,
                            current_market_date=date(2026, 6, 4),
                        )
                    )
        except BaseException as exc:  # pragma: no cover - assertion reports thread failures
            errors.append(exc)

    def run_cycle() -> None:
        try:
            results.append(
                run_due_watch_exec_agent_sizing(
                    settings=settings,
                    session_factory=session_factory,
                    now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
                )
            )
        except BaseException as exc:  # pragma: no cover - assertion reports thread failures
            errors.append(exc)

    monitoring_thread = threading.Thread(target=run_monitoring_reads)
    monitoring_thread.start()
    threads = [threading.Thread(target=run_cycle) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stop_monitoring.set()
    monitoring_thread.join(timeout=2)

    assert errors == []
    assert len(results) == 2
    assert monitoring_symbols
    with session_factory() as session:
        rejected_count = session.query(ExecutionPlanItem).filter(ExecutionPlanItem.status == "exec_agent_rejected").count()
        assert rejected_count == 12
        audit_count = session.query(watch_sizing.ExecutionPlanItemAudit).count()
        assert audit_count == 12


def test_short_transaction_rejects_agent_decision_after_candidate_state_changes(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-candidate-cas.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_max_attempts=1,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    item_id = "watch-candidate-cas"

    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-candidate-cas",
                strategy_run_id="strategy-candidate-cas",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id=item_id,
                        risk_context={
                            "target_price": "76000",
                            "stop_price": "69000",
                        },
                    )
                ],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: {
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
    )

    def approve_stale_candidate(*, payload, **_) -> dict[str, object]:
        candidate = payload["candidates"][0]
        with session_factory() as concurrent:
            item = concurrent.get(ExecutionPlanItem, item_id)
            assert item is not None
            item.risk_context = {
                **dict(item.risk_context or {}),
                "target_price": "77000",
            }
            concurrent.commit()
            return {
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": payload["plan_id"],
                "_runtime_provenance": _verified_sizing_runtime(
                    settings,
                    request_payload=payload,
                ),
                "decisions": [
                {
                    "execution_plan_item_id": item_id,
                    "symbol": candidate["symbol"],
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": "71500",
                    "sizing_price": "71500",
                    "confidence": "0.7",
                    "target_profile": "short_swing",
                    "reason_code": "stale_approval",
                    "reason": "approval for the pre-change candidate",
                }
            ],
        }

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        approve_stale_candidate,
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["applied"] == []
    assert result.payload["errors"][0]["error"].startswith(
        "watch sizing candidate state changed after exec-agent request"
    )
    with session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_sizing_pending"
        assert item.risk_context["target_price"] == "77000"
        assert item.quantity_policy.get("quantity") is None

    engine.dispose()


def test_short_transaction_rechecks_binding_deadline_after_agent_lock_wait(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-binding-deadline.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_timeout_seconds=30,
        exec_agent_watch_sizing_max_attempts=1,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    item_id = "watch-binding-deadline"
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-binding-deadline",
                strategy_run_id="strategy-binding-deadline",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id=item_id,
                        risk_context={
                            "target_price": "76000",
                            "stop_price": "69000",
                        },
                    )
                ],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: {
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
    )

    def approve(*, payload, **_) -> dict[str, object]:
        candidate = payload["candidates"][0]
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": item_id,
                    "symbol": candidate["symbol"],
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": "71500",
                    "sizing_price": "71500",
                    "confidence": "0.7",
                    "target_profile": "short_swing",
                    "reason_code": "late_approval",
                    "reason": "fixture response that waited past its deadline",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", approve)
    clock = iter((100.0, 131.0))
    monkeypatch.setattr(watch_sizing, "monotonic", lambda: next(clock))

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["applied"] == []
    assert result.payload["errors"][0]["error"] == (
        "watch sizing request candidate binding expired before apply"
    )
    with session_factory() as session:
        item = session.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_sizing_pending"
        assert item.quantity_policy.get("quantity") is None

    engine.dispose()


def test_watch_sizing_quarantines_candidates_for_a_different_broker_provider(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-provider.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    kis_item = _watch_item(
        item_id="watch-provider-kis",
        symbol="005930",
        risk_context={"target_price": "76000", "stop_price": "69000"},
    )
    kis_item.provider = "kis"
    alpaca_item = _watch_item(
        item_id="watch-provider-alpaca",
        symbol="AAPL",
        trigger_price="200",
        limit_price="201",
        risk_context={"target_price": "220", "stop_price": "190"},
    )
    alpaca_item.provider = "alpaca"
    alpaca_item.asset_class = "us_equity"
    kis_us_item = _watch_item(
        item_id="watch-provider-kis-us",
        symbol="MSFT",
        trigger_price="400",
        limit_price="401",
        risk_context={
            "exchange": "NASD",
            "target_price": "440",
            "stop_price": "380",
        },
    )
    kis_us_item.provider = "kis"
    kis_us_item.asset_class = "us_equity"
    nxt_item = _watch_item(
        item_id="watch-provider-nxt",
        symbol="000660",
        risk_context={
            "exchange": "NXT",
            "target_price": "220000",
            "stop_price": "190000",
        },
    )
    nxt_item.provider = "kis"
    nxt_item.venue = "NXT"
    sor_item = _watch_item(
        item_id="watch-provider-sor",
        symbol="035420",
        risk_context={
            "exchange": "SOR",
            "target_price": "240000",
            "stop_price": "210000",
        },
    )
    sor_item.provider = "kis"
    sor_item.venue = "SOR"
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-mixed-sizing-providers",
                source="test",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[kis_item, alpaca_item, kis_us_item, nxt_item, sor_item],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: {
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
    )
    requested_ids: list[str] = []

    def fake_decisions(*, payload, **_) -> dict[str, object]:
        requested_ids.extend(
            str(candidate["execution_plan_item_id"])
            for candidate in payload["candidates"]
        )
        candidate = payload["candidates"][0]
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": 71500,
                    "confidence": 0.7,
                    "target_profile": "tactical_2pct",
                    "reason_code": "test_approve",
                    "reason": "provider-matched fixture",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fake_decisions)
    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=WatchSizingBrokerStub(reconcile_payloads=[]),
        now=datetime(2026, 6, 4, 0, 1, tzinfo=UTC),
    )

    assert requested_ids == ["watch-provider-kis"]
    assert any(
        set(summary.get("provider_mismatch_item_ids") or [])
        == {
            "watch-provider-alpaca",
            "watch-provider-kis-us",
            "watch-provider-nxt",
            "watch-provider-sor",
        }
        for summary in result.payload["applied"]
    )
    with session_factory() as session:
        persisted_alpaca = session.get(ExecutionPlanItem, "watch-provider-alpaca")
        assert persisted_alpaca is not None
        assert persisted_alpaca.status == "exec_agent_rejected"
        persisted_kis_us = session.get(ExecutionPlanItem, "watch-provider-kis-us")
        assert persisted_kis_us is not None
        assert persisted_kis_us.status == "exec_agent_rejected"
        persisted_nxt = session.get(ExecutionPlanItem, "watch-provider-nxt")
        assert persisted_nxt is not None
        assert persisted_nxt.status == "exec_agent_rejected"
        persisted_sor = session.get(ExecutionPlanItem, "watch-provider-sor")
        assert persisted_sor is not None
        assert persisted_sor.status == "exec_agent_rejected"
        for persisted in (
            persisted_alpaca,
            persisted_kis_us,
            persisted_nxt,
            persisted_sor,
        ):
            _assert_canonical_terminal_rejection_policy(
                persisted.quantity_policy,
                reason_code="watch_exec_agent_sizing_route_unsupported",
            )
            audit = session.scalar(
                select(ExecutionPlanItemAudit).where(
                    ExecutionPlanItemAudit.execution_plan_item_id == persisted.id,
                    ExecutionPlanItemAudit.event_type
                    == "watch_exec_agent_sizing_route_unsupported",
                )
            )
            assert audit is not None


def test_short_transaction_keeps_each_candidate_bound_to_its_source_plan(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-multi-plan.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=3,
        exec_agent_watch_sizing_max_batches_per_cycle=3,
        exec_agent_watch_sizing_min_notional_krw=Decimal("50000"),
        exec_agent_watch_sizing_high_conviction_min_notional_krw=Decimal("50000"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        for index, symbol in enumerate(("005930", "000660", "035420"), start=1):
            item = _watch_item(
                item_id=f"watch-item-{index}",
                symbol=symbol,
                risk_context={
                    "target_price": "76000",
                    "stop_price": "69000",
                    "execution_feasibility_context": {"max_safe_notional_krw": "800000"},
                },
            )
            item.status = "watch_buy_triggered_blocked"
            session.add(
                ExecutionPlan(
                    external_plan_id=f"plan-{symbol}",
                    strategy_run_id="strategy-1",
                    source="core-short-term-decider",
                    market_date=date(2026, 6, 4),
                    mode="paper",
                    items=[item],
                )
            )
        session.commit()

    calls: list[tuple[str, list[str]]] = []

    def fake_decisions(*, payload, **_) -> dict[str, object]:
        calls.append(
            (
                str(payload["plan_id"]),
                [str(candidate["execution_plan_item_id"]) for candidate in payload["candidates"]],
            )
        )
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": candidate["chase_limit_price"] or candidate["trigger_price"],
                    "confidence": 0.5,
                    "target_profile": "tactical_2pct",
                    "reason_code": "test_approve",
                    "reason": "fixture approval",
                }
                for candidate in payload["candidates"]
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fake_decisions)

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert dict(calls) == {
        "plan-005930": ["watch-item-1"],
        "plan-000660": ["watch-item-2"],
        "plan-035420": ["watch-item-3"],
    }
    assert result.payload["batches_processed"] == 3
    assert sum(summary["approved"] for summary in result.payload["applied"]) == 3

    with session_factory() as session:
        statuses = {
            item.id: item.status
            for item in session.query(ExecutionPlanItem).order_by(ExecutionPlanItem.id).all()
        }

    assert statuses == {
        "watch-item-1": "watch_buy_triggered_blocked",
        "watch-item-2": "watch_buy_triggered_blocked",
        "watch-item-3": "watch_buy_triggered_blocked",
    }


def test_short_transaction_reserves_approved_cash_before_the_next_agent_batch(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-cash-reservation.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=1,
        exec_agent_watch_sizing_max_batches_per_cycle=2,
        exec_agent_watch_sizing_min_notional_krw=Decimal("50000"),
        exec_agent_watch_sizing_high_conviction_min_notional_krw=Decimal("50000"),
        exec_agent_max_position_allocation_pct=Decimal("1"),
        exec_agent_max_symbol_exposure_pct=Decimal("1"),
        exec_agent_max_symbol_total_exposure_pct=Decimal("1"),
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-cash-reservation",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id=f"watch-cash-{index}",
                        symbol=symbol,
                        risk_context={
                            "target_price": "76000",
                            "stop_price": "71000",
                            "execution_feasibility_context": {
                                "max_safe_notional_krw": "100000",
                            },
                        },
                    )
                    for index, symbol in enumerate(("005930", "000660"), start=1)
                ],
            )
        )
        session.commit()

    account_context = {
        "available": True,
        "buy_sizing_available": True,
        "orderable_cash_krw": "100000",
        "buying_power_krw": "100000",
        "holdings": [],
        "open_orders": [],
        "open_orders_truth": {"available": True},
    }
    observed_cash: list[str] = []

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: dict(account_context),
    )

    def fake_decisions(*, payload, **_) -> dict[str, object]:
        observed_cash.append(str(payload["account_context"]["orderable_cash_krw"]))
        candidate = payload["candidates"][0]
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": 71500,
                    "confidence": 0.6,
                    "target_profile": "tactical_2pct",
                    "reason_code": "test_approve",
                    "reason": "fixture approval",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", fake_decisions)

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert observed_cash == ["100000", "28500.0000"], result.payload
    assert sum(summary["approved"] for summary in result.payload["applied"]) == 1
    assert sum(summary["rejected"] for summary in result.payload["applied"]) == 1


def test_short_transaction_prioritizes_highest_edge_candidate_when_batch_budget_is_limited(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-priority.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=1,
        exec_agent_watch_sizing_max_batches_per_cycle=1,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id="plan-priority",
                strategy_run_id="strategy-1",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                items=[
                    _watch_item(
                        item_id="watch-low-edge",
                        symbol="000001",
                        risk_context={
                            "target_price": "72420",
                            "stop_price": "69000",
                            "expected_return_pct": "2.0",
                            "confidence_score_10": "5.0",
                        },
                    ),
                    _watch_item(
                        item_id="watch-high-edge",
                        symbol="999999",
                        risk_context={
                            "target_price": "79520",
                            "stop_price": "69000",
                            "expected_return_pct": "12.0",
                            "confidence_score_10": "8.0",
                            "conviction_score_10": "8.5",
                        },
                    ),
                    _watch_item(
                        item_id="watch-medium-edge",
                        symbol="111111",
                        risk_context={
                            "target_price": "74550",
                            "stop_price": "69000",
                            "expected_return_pct": "5.0",
                            "confidence_score_10": "6.0",
                        },
                    ),
                ],
            )
        )
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: {
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
    )
    observed_symbols: list[str] = []

    def reject_candidate(*, payload, **_) -> dict[str, object]:
        candidate = payload["candidates"][0]
        observed_symbols.append(str(candidate["symbol"]))
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "reject",
                    "quantity": 0,
                    "approved_notional_krw": 0,
                    "sizing_price": candidate["trigger_price"],
                    "confidence": 0.5,
                    "reason_code": "fixture_reject",
                    "reason": "fixture rejection",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", reject_candidate)

    run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert observed_symbols == ["999999"]


def test_short_transaction_supersedes_older_duplicate_symbol_before_sizing(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-dedupe.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=1,
        exec_agent_watch_sizing_max_batches_per_cycle=2,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        older = ExecutionPlan(
            external_plan_id="plan-duplicate-older",
            strategy_run_id="strategy-older",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            accepted_at=datetime(2026, 6, 3, 20, 0, tzinfo=UTC),
            items=[
                _watch_item(
                    item_id="watch-duplicate-older",
                    symbol="005930",
                    risk_context={"target_price": "76000", "stop_price": "69000"},
                ),
                _watch_item(
                    item_id="watch-older-plan-active-sibling",
                    symbol="000660",
                    risk_context={"target_price": "135000", "stop_price": "115000"},
                ),
            ],
        )
        newer = ExecutionPlan(
            external_plan_id="plan-duplicate-newer",
            strategy_run_id="strategy-newer",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            accepted_at=datetime(2026, 6, 3, 21, 0, tzinfo=UTC),
            items=[
                _watch_item(
                    item_id="watch-duplicate-newer",
                    symbol="005930",
                    risk_context={"target_price": "77000", "stop_price": "70000"},
                )
            ],
        )
        session.add_all([older, newer])
        session.commit()

    monkeypatch.setattr(
        watch_sizing,
        "_load_sizing_account_context_short_transaction",
        lambda **_: {
            "available": True,
            "buy_sizing_available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
    )
    observed_item_ids: list[str] = []

    def reject_candidate(*, payload, **_) -> dict[str, object]:
        candidate = payload["candidates"][0]
        observed_item_ids.append(str(candidate["execution_plan_item_id"]))
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": payload["plan_id"],
            "decisions": [
                {
                    "execution_plan_item_id": candidate["execution_plan_item_id"],
                    "symbol": candidate["symbol"],
                    "action": "reject",
                    "quantity": 0,
                    "approved_notional_krw": 0,
                    "sizing_price": candidate["trigger_price"],
                    "confidence": 0.5,
                    "reason_code": "fixture_reject",
                    "reason": "fixture rejection",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", reject_candidate)

    run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert set(observed_item_ids) == {
        "watch-duplicate-newer",
        "watch-older-plan-active-sibling",
    }
    assert "watch-duplicate-older" not in observed_item_ids
    with session_factory() as session:
        older_item = session.get(ExecutionPlanItem, "watch-duplicate-older")
        assert older_item is not None
        assert older_item.status == "superseded"


@pytest.mark.parametrize(
    ("observed_at", "expected_reason"),
    [
        (datetime(2026, 6, 3, 23, 58, 59, tzinfo=UTC), "account_snapshot_stale"),
        (datetime(2026, 6, 4, 0, 0, 31, tzinfo=UTC), "account_snapshot_future_skew"),
    ],
    ids=["stale", "future-skew"],
)
@pytest.mark.parametrize("runtime_mode", ["paper", "live"])
def test_watch_sizing_account_snapshot_freshness_blocks_before_agent_call(
    tmp_path,
    monkeypatch,
    observed_at: datetime,
    expected_reason: str,
    runtime_mode: str,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'watch-sizing-{runtime_mode}-{expected_reason}.db'}",
        runtime_mode=runtime_mode,
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_max_account_snapshot_age_seconds=60,
        account_drawdown_recovery_max_snapshot_future_skew_seconds=5,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        session.add(
            ExecutionPlan(
                external_plan_id=f"plan-{expected_reason}",
                source="core-short-term-decider",
                market_date=date(2026, 6, 4),
                mode=runtime_mode,
                items=[
                    _watch_item(
                        item_id=f"watch-{expected_reason}",
                        symbol="005930",
                        risk_context={"target_price": "76000", "stop_price": "69000"},
                    )
                ],
            )
        )
        session.commit()
    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("stale account truth must not reach the exec-agent")
        ),
    )
    broker = WatchSizingBrokerStub(
        reconcile_payloads=[],
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
            observed_at=observed_at,
            source="stub:account_snapshot",
        ),
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=broker,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["blocked"] is True
    assert result.payload["reason"] == "account_context_unavailable"
    assert result.payload["account_context"]["reason"] == expected_reason
    assert result.payload["batches_processed"] == 0
    engine.dispose()


def test_candidate_payload_maps_legacy_stop_price_to_agent_stop_loss_price() -> None:
    item = _watch_item(
        risk_context={
            "target_price": "76000",
            "stop_price": "69000",
        }
    )

    payload = _candidate_payload(item)

    assert payload["stop_loss_price"] == "69000"


@pytest.mark.parametrize(
    ("risk_context", "expected_reason"),
    [
        pytest.param(
            {"stop_price": "69000"},
            "missing_target_price",
            id="missing-target",
        ),
        pytest.param(
            {"target_price": "71500", "stop_price": "69000"},
            "invalid_target_distance",
            id="target-at-reference",
        ),
        pytest.param(
            {"target_price": "71000", "stop_price": "69000"},
            "invalid_target_distance",
            id="target-below-reference",
        ),
        pytest.param(
            {"target_price": "76000"},
            "missing_stop_price",
            id="missing-stop",
        ),
    ],
)
def test_sizing_request_payload_hard_rejects_incomplete_or_unprofitable_geometry(
    risk_context: dict[str, object],
    expected_reason: str,
) -> None:
    settings = Settings(runtime_mode="live")
    item = _watch_item(risk_context=risk_context)
    candidate = _candidate_payload(item)

    payload = _sizing_request_payload(
        settings=settings,
        plan=ExecutionPlan(
            external_plan_id="plan-envelope-invalid-geometry",
            strategy_run_id="strategy-1",
            market_date=date(2026, 6, 4),
        ),
        candidates=[candidate],
        account_context=_fresh_account_context(),
        candidate_universe=_candidate_universe_payload([candidate]),
    )

    envelope = payload["candidates"][0]["allowed_action_envelope"]  # type: ignore[index]
    assert envelope["allowed_actions"] == ["hold", "reject"]
    assert expected_reason in envelope["hard_reject_reasons"]
    assert expected_reason not in envelope["soft_warnings"]


def test_sizing_envelope_rejects_reward_risk_below_deterministic_floor() -> None:
    item = _watch_item(
        risk_context={
            "target_price": "72700",
            "stop_price": "70500",
            "estimated_round_trip_cost_bps": "20",
        }
    )
    candidate = _candidate_payload(item)

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="paper"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert "reward_risk_below_minimum" in envelope["hard_reject_reasons"]
    assert envelope["allowed_actions"] == ["hold", "reject"]


@pytest.mark.parametrize(
    ("symbol", "entry", "target", "upstream_stop", "expected_effective_stop"),
    [
        ("000020", "10000", "11000", "9000", "9750.0000"),
        ("028670", "6630", "7050", "6400", "6464.2500"),
        ("041960", "11900", "12700", "11450", "11602.5000"),
        ("048910", "5000", "5500", "4700", "4875.0000"),
        ("144960", "14650", "16200", "13880", "14283.7500"),
        ("171090", "80100", "84000", "78300", "78300.0000"),
    ],
)
def test_watch_sizing_envelope_uses_one_server_owned_effective_stop_basis(
    symbol: str,
    entry: str,
    target: str,
    upstream_stop: str,
    expected_effective_stop: str,
) -> None:
    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(
            runtime_mode="paper",
            exec_agent_stop_loss_pct=Decimal("0.025"),
        ),
        candidate={
            "symbol": symbol,
            "provider": "kis",
            "asset_class": "stock",
            "exchange": "KRX",
            "intent": "intraday_watch_buy",
            "chase_limit_price": entry,
            "target_price": target,
            "stop_loss_price": upstream_stop,
        },
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert envelope["upstream_stop_price"] == f"{Decimal(upstream_stop):.4f}"
    assert envelope["effective_stop_price"] == expected_effective_stop
    assert envelope["stop_price"] == expected_effective_stop


def test_approved_watch_sizing_persists_effective_stop_and_updates_stop_child() -> None:
    settings = Settings(
        runtime_mode="paper",
        exec_agent_stop_loss_pct=Decimal("0.025"),
    )
    entry = _watch_item(
        item_id="effective-stop-entry",
        symbol="000020",
        risk_context={
            "exchange": "KRX",
            "target_price": "11000",
            "stop_loss_price": "9000",
        },
        trigger_price="10000",
        limit_price="10000",
    )
    entry.source_plan_item_id = "core-entry-000020"
    stop = ExecutionPlanItem(
        id="effective-stop-child",
        plan_item_index=1,
        symbol="000020",
        side="sell",
        intent="stop_sell",
        status="stop_sell_pending_config",
        trigger_price=Decimal("9000"),
        quantity_policy={
            "type": "position_exit_from_entry",
            "linked_entry_item_key": "core-entry-000020",
        },
    )
    plan = ExecutionPlan(
        id="effective-stop-plan",
        external_plan_id="effective-stop-plan",
        source="core-short-term-decider",
        market_date=date(2026, 10, 14),
        mode="paper",
        items=[entry, stop],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=_fresh_account_context(),
        expected_item_ids={entry.id},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": entry.id,
                    "symbol": entry.symbol,
                    "action": "approve",
                    "quantity": 10,
                    "approved_notional_krw": "100000",
                    "sizing_price": "10000",
                    "confidence": "0.70",
                    "target_profile": "short_swing",
                    "reason_code": "approved",
                    "reason": "server envelope passed",
                }
            ],
        },
        now=datetime(2026, 10, 14, 0, 0, tzinfo=UTC),
    )

    assert summary["approved"] == 1
    assert entry.risk_context["stop_loss_price"] == "9000"
    assert entry.quantity_policy["upstream_stop_price"] == "9000.0000"
    assert entry.quantity_policy["effective_stop_price"] == "9750.0000"
    assert entry.quantity_policy["effective_stop_policy_version"] == (
        "server_effective_stop_v1"
    )
    assert stop.trigger_price == Decimal("9750.0000")


def test_final_watch_quantity_uses_the_same_effective_stop_as_the_envelope() -> None:
    settings = Settings(
        runtime_mode="paper",
        exec_agent_stop_loss_pct=Decimal("0.025"),
    )
    item = _watch_item(
        symbol="000020",
        risk_context={
            "exchange": "KRX",
            "target_price": "11000",
            "stop_loss_price": "9000",
        },
        trigger_price="10000",
        limit_price="10000",
    )
    item.quantity_policy["budget_krw"] = "3000000"
    envelope = watch_sizing._allowed_action_envelope_for_item(
        settings=settings,
        item=item,
        account_context=_fresh_account_context(),
        decision={"sizing_price": "10000"},
    )

    sizing = watch_sizing._adjust_approved_watch_quantity(
        settings=settings,
        item=item,
        decision={
            "quantity": 300,
            "approved_notional_krw": "3000000",
            "sizing_price": "10000",
            "confidence": "0.70",
        },
        account_context=_fresh_account_context(),
        agent_quantity=300,
        allowed_action_envelope=envelope,
    )

    assert envelope["effective_stop_price"] == "9750.0000"
    assert sizing["quantity"] == 300
    assert sizing["approved_notional"] == Decimal("3000000")


def test_live_sizing_envelope_requires_configured_transaction_cost_policy() -> None:
    item = _watch_item(
        risk_context={"target_price": "76000", "stop_price": "69000"}
    )
    candidate = _candidate_payload(item)
    candidate.pop("estimated_round_trip_cost_bps", None)
    candidate["provider"] = "unknown"

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="live"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert "transaction_cost_evidence_missing" in envelope["hard_reject_reasons"]
    assert envelope["allowed_actions"] == ["hold", "reject"]


def test_live_sizing_uses_risk_exchange_when_kis_is_the_broker_venue() -> None:
    item = _watch_item(
        risk_context={
            "exchange": "KRX",
            "target_price": "76000",
            "stop_price": "69000",
        }
    )
    item.provider = "kis"
    item.venue = "kis"
    item.asset_class = "stock"

    candidate = _candidate_payload(item)
    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="live"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert candidate["venue"] == "KIS"
    assert candidate["exchange"] == "KRX"
    assert envelope["estimated_round_trip_cost_bps"] == "50.0000"
    assert "transaction_cost_evidence_missing" not in envelope["hard_reject_reasons"]


@pytest.mark.parametrize(
    "basis",
    [None, "unavailable", "liquidity_tier_estimate", "unknown"],
)
def test_live_sizing_ignores_unverified_pretrigger_slippage_basis(
    basis: str | None,
) -> None:
    feasibility: dict[str, object] = {"expected_slippage_bps": "12"}
    if basis is not None:
        feasibility["expected_slippage_basis"] = basis
    candidate = _candidate_payload(
        _watch_item(
            risk_context={
                "target_price": "78000",
                "stop_price": "69000",
                "execution_feasibility_context": feasibility,
            }
        )
    )
    candidate.pop("estimated_round_trip_cost_bps", None)

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="live"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert "buy" in envelope["allowed_actions"]
    assert envelope["estimated_round_trip_cost_bps"] == "50.0000"
    assert envelope["transaction_cost_evidence_source"] == (
        "settings:kis_krx_round_trip_non_spread_cost_bps"
    )


def test_buy_economics_extreme_decimal_fails_closed_without_raising() -> None:
    assessment = assess_buy_economics(
        reference_price=Decimal("1e999999"),
        target_price=Decimal("2e999999"),
        stop_price=Decimal("0.5e999999"),
        minimum_reward_risk=Decimal("1.30"),
        evidence={"estimated_round_trip_cost_bps": "20"},
        require_cost_evidence=True,
    )

    assert assessment.reason == "economic_value_out_of_range"


def test_buy_economics_ignores_nested_agent_claimed_cost_evidence() -> None:
    assessment = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("110"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence={
            "exec_agent_decision": {
                "context": {"estimated_round_trip_cost_bps": "1"},
                "explanation": {"expected_slippage_bps": "1"},
            }
        },
        require_cost_evidence=True,
    )

    assert assessment.reason == "transaction_cost_evidence_missing"
    assert assessment.cost_evidence_source is None


def test_delegated_gate_validator_ignores_nested_agent_claims() -> None:
    candidate = _candidate_payload(
        _watch_item(
            risk_context={
                "target_price": "76000",
                "stop_price": "69000",
                "exec_agent_decision": {
                    "context": _delegated_gate_context(),
                },
            }
        )
    )

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="paper"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert envelope["delegated_gate_rejections"] == []


def test_sizing_envelope_uses_configured_cost_floor_over_candidate_claim() -> None:
    item = _watch_item(
        risk_context={
            "target_price": "72810",
            "stop_price": "70500",
            "estimated_round_trip_cost_bps": "20",
        }
    )
    candidate = _candidate_payload(item)

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="paper"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert "net_reward_risk_below_minimum" in envelope["hard_reject_reasons"]
    assert Decimal(envelope["gross_reward_risk"]) >= Decimal("1.30")
    assert Decimal(envelope["net_reward_risk"]) < Decimal("1.30")
    assert envelope["estimated_round_trip_cost_bps"] == "50.0000"


def test_watch_sizing_preserves_and_enforces_core_delegated_gate_contract() -> None:
    risk_context = _delegated_gate_context()
    candidate = _candidate_payload(_watch_item(risk_context=risk_context))
    candidate.pop("estimated_round_trip_cost_bps", None)

    assert candidate["delegated_gate_codes"] == _delegated_gate_context()[
        "delegated_gate_codes"
    ]
    assert candidate["delegated_gate_confirmation_required"] is True
    assert candidate["delegated_gates"] == risk_context["delegated_gates"]
    assert candidate["execution_feasibility_context"] == {
        "expected_slippage_bps": None,
        "expected_slippage_basis": "unavailable",
    }
    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="live"),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert envelope["delegated_gate_rejections"] == []
    assert "buy" in envelope["allowed_actions"]
    assert envelope["transaction_cost_evidence_source"] == (
        "settings:kis_krx_round_trip_non_spread_cost_bps"
    )


def test_live_sizing_accepts_zero_quote_spread_cost_evidence() -> None:
    risk_context = _delegated_gate_context()
    risk_context["execution_feasibility_context"] = {
        "expected_slippage_bps": "0",
        "expected_slippage_basis": "quote_spread_bps",
    }
    candidate = _candidate_payload(_watch_item(risk_context=risk_context))
    candidate.pop("estimated_round_trip_cost_bps", None)

    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(
            runtime_mode="live",
            kis_krx_round_trip_non_spread_cost_bps=Decimal("0"),
        ),
        candidate=candidate,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert "buy" in envelope["allowed_actions"]
    assert envelope["estimated_round_trip_cost_bps"] == "0.0000"


@pytest.mark.parametrize(
    ("risk_context", "expected_reason"),
    [
        pytest.param(
            {
                **_delegated_gate_context(),
                "delegated_gate_confirmation_required": False,
            },
            "delegated_gate_confirmation_missing",
            id="confirmation-missing",
        ),
        pytest.param(
            {
                **_delegated_gate_context(),
                "delegated_gate_codes": ["unknown_delegated_gate"],
            },
            "delegated_gate_unknown:unknown_delegated_gate",
            id="unknown-code",
        ),
        pytest.param(
            {
                **_delegated_gate_context(),
                "delegated_gate_codes": [
                    "short_term_buy_quality_score_block_delegated_to_exec_watch"
                ],
            },
            "delegated_gate_unknown:short_term_buy_quality_score_block_delegated_to_exec_watch",
            id="non-recomputable-code",
        ),
        pytest.param(
            {
                **_delegated_gate_context(),
                "delegated_gates": [],
            },
            "delegated_gate_contract_missing",
            id="structured-contract-missing",
        ),
        pytest.param(
            {
                **_delegated_gate_context(),
                "delegated_gates": [
                    {
                        **_delegated_gate_context()["delegated_gates"][0],
                        "field_path": "risk_context.agent_claimed_turnover",
                    },
                    *_delegated_gate_context()["delegated_gates"][1:],
                ],
            },
            "delegated_gate_contract_mismatch:short_term_liquidity_floor_block_delegated_to_exec_watch:field_path",
            id="field-path-mismatch",
        ),
    ],
)
def test_watch_sizing_fails_closed_when_delegated_gate_is_unconfirmed_or_fails(
    risk_context: dict[str, object],
    expected_reason: str,
) -> None:
    envelope = watch_sizing._allowed_action_envelope_for_candidate(
        settings=Settings(runtime_mode="live"),
        candidate=_candidate_payload(_watch_item(risk_context=risk_context)),
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
    )

    assert expected_reason in envelope["hard_reject_reasons"]
    assert envelope["allowed_actions"] == ["hold", "reject"]


def test_dynamic_delegated_gates_resume_only_with_fresh_authoritative_evidence() -> None:
    evaluated_at = datetime.now(UTC)
    economics = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("120"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=configured_execution_cost_evidence(
            settings=Settings(),
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            live_quote_spread_bps="5",
            require_live_quote_spread=True,
        ),
        require_cost_evidence=True,
    )
    live_context = {
        "live_execution_context_available": True,
        "turnover": "3500000000",
        "field_freshness": {
            "turnover": {
                "valid": True,
                "source": "broker_realtime_exchange_time",
                "observed_at": evaluated_at.isoformat(),
                "max_age_seconds": 30,
                "max_future_skew_seconds": 5,
            }
        },
    }

    assert delegated_buy_gate_rejections(
        _delegated_gate_context(),
        evaluated_at=evaluated_at,
        live_market_context=live_context,
        economics=economics,
        require_live_recheck=True,
    ) == []

    live_context["turnover"] = "2500000000"
    assert delegated_buy_gate_rejections(
        _delegated_gate_context(),
        evaluated_at=evaluated_at,
        live_market_context=live_context,
        economics=economics,
        require_live_recheck=True,
    ) == [
        "delegated_gate_failed:short_term_liquidity_floor_block_delegated_to_exec_watch"
    ]


@pytest.mark.parametrize(
    "freshness_update",
    [
        {"source": "transport_received_at_untrusted"},
        {"observed_at": (datetime.now(UTC) - timedelta(minutes=5)).isoformat()},
        {"observed_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat()},
    ],
)
def test_dynamic_liquidity_gate_rejects_untrusted_stale_or_future_turnover(
    freshness_update: dict[str, object],
) -> None:
    evaluated_at = datetime.now(UTC)
    freshness = {
        "valid": True,
        "source": "broker_realtime_exchange_time",
        "observed_at": evaluated_at.isoformat(),
        "max_age_seconds": 30,
        "max_future_skew_seconds": 5,
        **freshness_update,
    }
    live_context = {
        "live_execution_context_available": True,
        "turnover": "3500000000",
        "field_freshness": {"turnover": freshness},
    }
    economics = assess_buy_economics(
        reference_price=Decimal("100"),
        target_price=Decimal("120"),
        stop_price=Decimal("95"),
        minimum_reward_risk=Decimal("1.30"),
        evidence=configured_execution_cost_evidence(
            settings=Settings(),
            provider="kis",
            asset_class="stock",
            exchange="KRX",
            live_quote_spread_bps="5",
            require_live_quote_spread=True,
        ),
        require_cost_evidence=True,
    )

    rejections = delegated_buy_gate_rejections(
        _delegated_gate_context(),
        evaluated_at=evaluated_at,
        live_market_context=live_context,
        economics=economics,
        require_live_recheck=True,
    )

    assert rejections == [
        "delegated_gate_failed:short_term_liquidity_floor_block_delegated_to_exec_watch"
    ]


@pytest.mark.parametrize(
    "target_price",
    [
        pytest.param(None, id="missing-target"),
        pytest.param("71000", id="target-at-reference"),
        pytest.param("70000", id="target-below-reference"),
    ],
)
def test_watch_sizing_decision_record_does_not_tag_invalid_geometry_as_tactical(
    target_price: str | None,
) -> None:
    item = _watch_item(
        risk_context={
            "target_price": target_price,
            "stop_price": "69000",
        }
    )

    record = _decision_record(
        decision={
            "action": "reject",
            "quantity": 2,
            "approved_notional_krw": 142000,
            "sizing_price": "71000",
            "confidence": 0.68,
            "target_profile": "tactical_2pct",
            "reason_code": "invalid_geometry",
            "reason": "invalid target geometry",
        },
        item=item,
        version="manual-exec-agent-watch-sizing-v1",
    )

    assert record["target_profile"] == "standard_breakout"
    assert record["target_tag"] == "target_profile:standard_breakout"


def test_apply_decisions_rejects_partial_response_and_leaves_every_item_pending() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-partial",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "72700", "stop_price": "70950"},
            ),
            _watch_item(
                item_id="watch-item-2",
                symbol="000660",
                risk_context={"target_price": "128000", "stop_price": "69000"},
            ),
        ],
    )

    with pytest.raises(ValueError, match="missing decisions.*watch-item-2"):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="paper"),
            plan=plan,
            account_context={
                "available": True,
                "orderable_cash_krw": "10000000",
                "buying_power_krw": "10000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={"watch-item-1", "watch-item-2"},
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-1",
                        "symbol": "005930",
                        "action": "approve",
                        "quantity": 2,
                        "approved_notional_krw": 143000,
                        "sizing_price": "71000",
                        "confidence": 0.68,
                        "target_profile": "tactical_2pct",
                        "reason_code": "small_tactical_upside",
                        "reason": "2% target with tight stop",
                    }
                ],
            },
        )

    assert plan.items[0].status == "watch_sizing_pending"
    assert plan.items[1].status == "watch_sizing_pending"


def test_apply_decisions_rejects_unknown_response_contract_version() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-invalid-version",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    with pytest.raises(ValueError, match="unsupported exec-agent response version"):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="paper"),
            plan=plan,
            expected_item_ids={"watch-item-1"},
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v999",
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-1",
                        "symbol": "005930",
                        "action": "reject",
                        "quantity": 0,
                    }
                ],
            },
        )


def test_apply_decisions_rejects_item_symbol_mismatch() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-symbol-mismatch",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    with pytest.raises(ValueError, match="symbol mismatch"):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="paper"),
            plan=plan,
            expected_item_ids={"watch-item-1"},
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-1",
                        "symbol": "000660",
                        "action": "reject",
                        "quantity": 0,
                    }
                ],
            },
        )


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("version", " manual-exec-agent-watch-sizing-v1", "unsupported exec-agent response version"),
        ("plan_id", "plan-symbol-mismatch ", "plan_id mismatch"),
        ("execution_plan_item_id", "watch-item-1 ", "unknown execution_plan_item_id"),
        ("symbol", " 005930", "symbol mismatch"),
    ],
)
def test_watch_sizing_authoritative_identity_requires_exact_match(
    field: str,
    value: str,
    expected_error: str,
) -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-symbol-mismatch",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )
    payload: dict[str, object] = {
        "version": "manual-exec-agent-watch-sizing-v1",
        "plan_id": "plan-symbol-mismatch",
        "decisions": [
            {
                "execution_plan_item_id": "watch-item-1",
                "symbol": "005930",
                "action": "reject",
                "quantity": 0,
            }
        ],
    }
    if field in {"version", "plan_id"}:
        payload[field] = value
    else:
        decisions = payload["decisions"]
        assert isinstance(decisions, list) and isinstance(decisions[0], dict)
        decisions[0][field] = value

    with pytest.raises(ValueError, match=expected_error):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="paper"),
            plan=plan,
            expected_item_ids={"watch-item-1"},
            decision_payload=payload,
        )


@pytest.mark.parametrize(
    "payload_text",
    [
        pytest.param('```json\n{"decisions":[]}\n```', id="markdown-fence"),
        pytest.param('result: {"decisions":[]}', id="leading-prose"),
        pytest.param('{"decisions":[]} done', id="trailing-prose"),
        pytest.param('{"version":"v1","version":"v2"}', id="duplicate-top-level"),
        pytest.param('{"decisions":[{"symbol":"005930","symbol":"000660"}]}', id="duplicate-nested"),
        pytest.param('{"confidence":NaN}', id="nan"),
        pytest.param('{"confidence":Infinity}', id="infinity"),
        pytest.param('{"confidence":1e9999}', id="exponent-overflow"),
    ],
)
def test_watch_sizing_parser_rejects_ambiguous_json(payload_text: str) -> None:
    with pytest.raises((ValueError, json.JSONDecodeError)):
        watch_sizing._parse_json_text(payload_text)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_watch_sizing_numeric_helpers_reject_nonfinite_strings(value: str) -> None:
    assert watch_sizing._positive_decimal(value) is None
    assert watch_sizing._nonnegative_decimal(value) is None
    assert watch_sizing._positive_int(value) is None


def test_apply_decisions_rejects_unknown_item_mixed_with_expected_item() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-unknown-item",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={"target_price": "76000", "stop_price": "69000"},
            )
        ],
    )

    with pytest.raises(ValueError, match="unknown execution_plan_item_id"):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="paper"),
            plan=plan,
            expected_item_ids={"watch-item-1"},
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": "plan-unknown-item",
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-1",
                        "symbol": "005930",
                        "action": "reject",
                        "quantity": 0,
                    },
                    {
                        "execution_plan_item_id": "foreign-item",
                        "symbol": "000660",
                        "action": "approve",
                        "quantity": 1,
                    },
                ],
            },
        )


@pytest.mark.parametrize(
    ("available_cash", "expected_approved", "expected_rejected"),
    [
        pytest.param("100000", 1, 1, id="one-share-then-reserved-cash-exhausted"),
        pytest.param("60000", 0, 2, id="cash-below-executable-reference"),
    ],
)
def test_apply_decisions_reserves_cash_between_approvals_in_the_same_response(
    available_cash: str,
    expected_approved: int,
    expected_rejected: int,
) -> None:
    settings = Settings(
        runtime_mode="live",
        exec_agent_max_position_allocation_pct=Decimal("1"),
    )
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-intra-batch-cash-reservation",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
                _watch_item(
                    item_id="watch-item-1",
                    symbol="005930",
                    risk_context={"target_price": "76000", "stop_price": "71400"},
                ),
                _watch_item(
                    item_id="watch-item-2",
                    symbol="000660",
                    risk_context={"target_price": "76000", "stop_price": "71400"},
                ),
        ],
    )
    account_context = _fresh_account_context(
        orderable_cash_krw=available_cash,
        buying_power_krw=available_cash,
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        account_context=account_context,
        expected_item_ids={"watch-item-1", "watch-item-2"},
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "60000",
                    "confidence": 0.60,
                    "target_profile": "short_swing",
                    "reason_code": "cash_probe_one",
                    "reason": "first candidate consumes most available cash",
                },
                {
                    "execution_plan_item_id": "watch-item-2",
                    "symbol": "000660",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": 71500,
                    "sizing_price": "60000",
                    "confidence": 0.60,
                    "target_profile": "short_swing",
                    "reason_code": "cash_probe_two",
                    "reason": "second candidate requests the same remaining cash",
                },
            ],
        },
    )

    assert summary["approved"] == expected_approved
    assert summary["rejected"] == expected_rejected
    assert summary["approved_notional_krw"] == (
        "71500.0000" if expected_approved else "0.0000"
    )
    assert sum(item.status == "watch_monitoring_active" for item in plan.items) == (
        expected_approved
    )
    rejected_items = [
        item for item in plan.items if item.status == "exec_agent_rejected"
    ]
    assert len(rejected_items) == expected_rejected
    for item in rejected_items:
        _assert_canonical_terminal_rejection_policy(
            item.quantity_policy,
            reason_code="allowed_action_envelope_blocked",
        )
    approved_items = [
        item for item in plan.items if item.status == "watch_monitoring_active"
    ]
    assert all(
        item.quantity_policy["sizing_price"] == "71500.0000"
        and item.quantity_policy["approved_notional"] == "71500.0000"
        and item.quantity_policy["sizing_adjustment"]["final_notional"]
        == "71500.0000"
        for item in approved_items
    )
    assert account_context["orderable_cash_krw"] == available_cash


def test_run_due_retries_the_entire_batch_after_a_partial_response(monkeypatch) -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-retry-context",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="live",
        items=[
            _watch_item(
                item_id="watch-item-1",
                symbol="005930",
                risk_context={
                    "target_price": "78000",
                    "stop_price": "69000",
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "5000000",
                        "liquidity_tier": "high",
                    },
                },
            ),
            _watch_item(
                item_id="watch-item-2",
                symbol="005931",
                risk_context={
                    "target_price": "78000",
                    "stop_price": "69000",
                    "execution_feasibility_context": {
                        "max_safe_notional_krw": "800000",
                        "liquidity_tier": "high",
                    },
                },
            ),
        ],
    )
    settings = Settings(
        runtime_mode="live",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_min_notional_krw=Decimal("100000"),
        exec_agent_watch_sizing_high_conviction_min_notional_krw=Decimal("100000"),
    )
    account_context = _fresh_account_context(
        observed_at="2026-06-04T00:00:00+00:00",
    )
    responses = [
        {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "plan-retry-context",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 3,
                    "approved_notional_krw": 214500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe",
                    "reason": "initial response omitted the second candidate",
                }
            ],
        },
        {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "plan-retry-context",
            "_runtime_provenance": _verified_sizing_runtime(settings),
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 3,
                    "approved_notional_krw": 214500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe_retry",
                    "reason": "retry returns the complete batch",
                },
                {
                    "execution_plan_item_id": "watch-item-2",
                    "symbol": "005931",
                    "action": "approve",
                    "quantity": 3,
                    "approved_notional_krw": 214500,
                    "sizing_price": "71500",
                    "confidence": 0.68,
                    "target_profile": "short_swing",
                    "reason_code": "small_probe_retry",
                    "reason": "retry returns the complete batch",
                }
            ],
        },
    ]

    class FakeSession:
        def __init__(self) -> None:
            self.committed = False

        def commit(self) -> None:
            self.committed = True

    db = FakeSession()

    def next_response(*, payload, **_) -> dict[str, object]:
        response = responses.pop(0)
        response["_runtime_provenance"] = _verified_sizing_runtime(
            settings,
            request_payload=payload,
        )
        return response

    monkeypatch.setattr(watch_sizing, "_load_candidate_plans", lambda **_: [plan])
    monkeypatch.setattr(
        watch_sizing,
        "_supersede_duplicate_watch_buys_before_sizing",
        lambda **_: 0,
    )
    monkeypatch.setattr(
        watch_sizing,
        "_reopen_price_watch_terminal_rejections",
        lambda **_: [],
    )
    monkeypatch.setattr(watch_sizing, "_load_sizing_account_context", lambda **_: account_context)
    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        next_response,
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        db=db,  # type: ignore[arg-type]
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["applied"][0]["missing_item_ids"] == []
    assert result.payload["applied"][0]["approved"] == 2
    assert responses == []
    assert db.committed is True
    retry_policy = plan.items[1].quantity_policy
    assert retry_policy["quantity"] == 3
    assert retry_policy["approved_notional"] == "214500.0000"
    assert retry_policy["sizing_adjustment"]["applied"] is False
    assert retry_policy["sizing_adjustment"]["cap_notional"] == "214500.0000"
    assert retry_policy["sizing_adjustment"]["runtime_cap_notional"] == "800000.0000"
    assert retry_policy["sizing_adjustment"]["min_notional_upsize_suppressed"] is False
    assert retry_policy["sizing_adjustment"]["reason"] is None


def test_watch_sizing_request_binding_covers_candidate_account_caps_and_route() -> None:
    request_started_at = datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    item = _watch_item(
        item_id="watch-bound",
        symbol="005930",
        risk_context={
            "target_price": "76000",
            "stop_price": "69000",
            "execution_account_route": {
                "version": "kis-account-route-v1",
                "configured_route_fingerprint": "a" * 32,
            },
        },
    )
    item.provider = "kis"
    item.venue = "kis"
    item.asset_class = "stock"
    plan = ExecutionPlan(
        external_plan_id="plan-watch-bound",
        market_date=date(2026, 6, 4),
        items=[item],
    )
    candidate = _candidate_payload(item)
    account = {
        "available": True,
        "observed_at": "2026-06-04T00:00:00+00:00",
        "orderable_cash_krw": "1000000",
        "holdings": [],
        "open_orders": [],
    }
    baseline = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[candidate],
        account_context=account,
        candidate_universe=_candidate_universe_payload([candidate]),
        request_started_at=request_started_at,
    )
    changed_account = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[candidate],
        account_context={**account, "orderable_cash_krw": "900000"},
        candidate_universe=_candidate_universe_payload([candidate]),
        request_started_at=request_started_at,
    )
    changed_caps = _sizing_request_payload(
        settings=Settings(
            runtime_mode="paper",
            exec_agent_max_symbol_budget=Decimal("900000"),
        ),
        plan=plan,
        candidates=[candidate],
        account_context=account,
        candidate_universe=_candidate_universe_payload([candidate]),
        request_started_at=request_started_at,
    )
    changed_candidate = deepcopy(candidate)
    changed_candidate["target_price"] = "77000"
    changed_economics = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[changed_candidate],
        account_context=account,
        candidate_universe=_candidate_universe_payload([changed_candidate]),
        request_started_at=request_started_at,
    )

    binding = baseline["candidates"][0]["binding"]  # type: ignore[index]
    assert binding["version"] == "watch-exec-agent-binding-v2"
    assert binding["plan_id"] == "plan-watch-bound"
    assert binding["item_id"] == "watch-bound"
    assert binding["provider"] == "kis"
    assert binding["asset_class"] == "stock"
    assert binding["venue"] == "KIS"
    assert binding["exchange"] == "KRX"
    assert binding["side"] == "buy"
    assert binding["intent"] == "intraday_watch_buy"
    assert len(binding["candidate_digest"]) == 64
    assert len(binding["request_context_digest"]) == 64
    assert binding["request_context_digest"] != changed_account["candidates"][0]["binding"]["request_context_digest"]  # type: ignore[index]
    assert binding["request_context_digest"] != changed_caps["candidates"][0]["binding"]["request_context_digest"]  # type: ignore[index]
    assert binding["candidate_digest"] != changed_economics["candidates"][0]["binding"]["candidate_digest"]  # type: ignore[index]


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        pytest.param("missing", "binding is missing", id="missing"),
        pytest.param("missing_exchange", "binding mismatch", id="missing-exchange"),
        pytest.param("mismatch", "binding mismatch", id="mismatch"),
        pytest.param("expired", "binding expired", id="expired"),
        pytest.param("partial", "missing decisions", id="partial"),
    ],
)
def test_watch_sizing_response_requires_exact_current_binding(
    mutation: str,
    expected_error: str,
) -> None:
    items = [
        _watch_item(
            item_id=f"watch-binding-{index}",
            symbol=symbol,
            risk_context={"target_price": "76000", "stop_price": "69000"},
        )
        for index, symbol in enumerate(("005930", "000660"), start=1)
    ]
    plan = ExecutionPlan(external_plan_id="plan-watch-binding", items=items)
    candidates = [_candidate_payload(item) for item in items]
    request = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=candidates,
        account_context={"available": True, "orderable_cash_krw": "1000000"},
        candidate_universe=_candidate_universe_payload(candidates),
    )
    decisions = [
        {
            "execution_plan_item_id": candidate["execution_plan_item_id"],
            "symbol": candidate["symbol"],
            "binding": deepcopy(candidate["binding"]),
            "action": "reject",
            "quantity": 0,
        }
        for candidate in request["candidates"]
    ]
    if mutation == "missing":
        decisions[0].pop("binding")
    elif mutation == "missing_exchange":
        decisions[0]["binding"].pop("exchange")
    elif mutation == "mismatch":
        decisions[0]["binding"]["candidate_digest"] = "0" * 64
    elif mutation == "expired":
        expired = "2020-01-01T00:00:00+00:00"
        request["candidates"][0]["binding"]["valid_until"] = expired
        decisions[0]["binding"]["valid_until"] = expired
    else:
        decisions.pop()

    with pytest.raises(ValueError, match=expected_error):
        watch_sizing._validate_watch_response_bindings(
            request_payload=request,
            decision_payload={"decisions": decisions},
        )


def test_watch_sizing_binding_cannot_extend_a_299_second_old_snapshot() -> None:
    request_started_at = datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    item = _watch_item(
        item_id="watch-old-snapshot-binding",
        risk_context={"target_price": "76000", "stop_price": "69000"},
    )
    plan = ExecutionPlan(external_plan_id="plan-watch-old-snapshot", items=[item])
    candidates = [_candidate_payload(item)]

    request = _sizing_request_payload(
        settings=Settings(
            runtime_mode="live",
            exec_agent_max_account_snapshot_age_seconds=300,
            exec_agent_watch_sizing_timeout_seconds=900,
        ),
        plan=plan,
        candidates=candidates,
        account_context={
            "available": True,
            "buy_sizing_available": True,
            "observed_at": (request_started_at - timedelta(seconds=299)).isoformat(),
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
            "open_orders_truth": {"available": True},
        },
        candidate_universe=_candidate_universe_payload(candidates),
        request_started_at=request_started_at,
    )

    valid_until = datetime.fromisoformat(
        request["candidates"][0]["binding"]["valid_until"]
    )
    assert valid_until == request_started_at + timedelta(seconds=1)


def test_live_watch_sizing_discards_approval_after_account_snapshot_expires() -> None:
    decision_at = datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    item = _watch_item(
        item_id="watch-expired-snapshot-apply",
        risk_context={"target_price": "76000", "stop_price": "69000"},
    )
    plan = ExecutionPlan(external_plan_id="plan-watch-expired-snapshot", items=[item])

    with pytest.raises(ValueError, match="account_snapshot_stale"):
        _apply_decisions(
            db=None,  # type: ignore[arg-type]
            settings=Settings(
                runtime_mode="live",
                exec_agent_max_account_snapshot_age_seconds=300,
            ),
            plan=plan,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": plan.external_plan_id,
                "decisions": [
                    {
                        "execution_plan_item_id": item.id,
                        "symbol": item.symbol,
                        "action": "approve",
                        "quantity": 1,
                        "approved_notional_krw": "71500",
                        "sizing_price": "71500",
                        "reason": "too late",
                    }
                ],
            },
            account_context={
                "available": True,
                "buy_sizing_available": True,
                "observed_at": (decision_at - timedelta(seconds=301)).isoformat(),
                "orderable_cash_krw": "1000000",
                "buying_power_krw": "1000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={item.id},
            now=decision_at,
        )


def test_next_open_candidate_uses_estimated_reference_with_explicit_provenance() -> None:
    plan = ExecutionPlan(
        id="plan-next-open-prompt",
        external_plan_id="plan-next-open-prompt",
        strategy_run_id="strategy-1",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[_next_open_watch_item()],
    )
    candidate = _candidate_payload(plan.items[0])

    assert candidate["intent"] == "next_open_buy"
    assert candidate["estimated_reference_price"] == "71500"
    assert candidate["entry_reference_price"] == "71500"
    assert candidate["entry_reference_source"] == "quantity_policy.estimated_reference_price"

    payload = _sizing_request_payload(
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        candidates=[candidate],
        account_context={
            "available": True,
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
        },
        candidate_universe=_candidate_universe_payload([candidate]),
    )

    assert "next_open_buy" in payload["task"]
    assert "estimated reference" in payload["task"]
    assert "limit ceiling" in payload["task"]
    assert "pending trigger-limit buys" not in payload["task"]


def test_next_open_approval_materializes_one_limit_job_with_agent_provenance() -> None:
    settings = Settings(
        runtime_mode="paper",
        exec_agent_model="openai/gpt-5.6-sol",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="route-binding-secret",
    )
    plan = ExecutionPlan(
        id="plan-next-open-approved",
        external_plan_id="plan-next-open-approved",
        strategy_run_id="strategy-1",
        source="core-signal-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[_next_open_watch_item(item_id="next-open-approved")],
    )
    decision_payload = {
        "version": "manual-exec-agent-watch-sizing-v1",
        "plan_id": plan.external_plan_id,
        "_runtime_provenance": _verified_sizing_runtime(
            settings,
            session_id="session-next-open",
        ),
        "decisions": [
            {
                "execution_plan_item_id": "next-open-approved",
                "symbol": "005930",
                "action": "approve",
                "quantity": 2,
                "approved_notional_krw": "143000",
                "sizing_price": "70000",
                "confidence": 0.71,
                "target_profile": "short_swing",
                "reason_code": "approved",
                "reason": "bounded next-open entry",
            }
        ],
    }
    account_context = {
        "available": True,
        "orderable_cash_krw": "1000000",
        "buying_power_krw": "1000000",
        "holdings": [],
        "open_orders": [],
    }
    plan.items[0].risk_context, plan.items[0].quantity_policy = (
        bind_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context=plan.items[0].risk_context,
            quantity_policy=plan.items[0].quantity_policy,
        )
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        decision_payload=decision_payload,
        request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
        account_context=account_context,
        expected_item_ids={"next-open-approved"},
        now=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
    )
    item = plan.items[0]

    assert summary["approved"] == 1
    assert item.status == "next_open_buy_scheduled"
    assert item.limit_price == Decimal("71500")
    assert item.quantity_policy["kind"] == "shares"
    assert item.quantity_policy["quantity"] == 2
    assert item.quantity_policy["sizing_price"] == "71500.0000"
    assert item.quantity_policy["approved_notional"] == "143000.0000"
    assert "account_reference" not in str(item.quantity_policy)
    assert "account_reference" not in str(item.risk_context)
    assert item.risk_context["execution_account_route"]["version"] == (
        "kis-account-route-v1"
    )
    assert item.quantity_policy["exec_agent_decision"]["runtime_provenance"][
        "actual_model_ref"
    ] == "openai/gpt-5.6-sol"
    assert len(item.jobs) == 1
    assert item.jobs[0].job_type == "next_open_buy"
    assert item.jobs[0].status == "scheduled"
    assert item.jobs[0].scheduled_for == datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    assert Decimal(item.risk_context["stop_price"]) < item.limit_price
    assert item.limit_price < Decimal(item.risk_context["target_price"])

    retry = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=settings,
        plan=plan,
        decision_payload=decision_payload,
        account_context=account_context,
        expected_item_ids={"next-open-approved"},
        now=datetime(2026, 6, 3, 23, 31, tzinfo=UTC),
    )
    assert retry["evaluated"] == 0
    assert len(item.jobs) == 1


def test_signal_next_open_ingress_persists_then_async_sizes_exactly_one_job(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'signal-next-open-async.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_max_attempts=1,
        exec_agent_model="openai/gpt-5.6-sol",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    command = ExecutionPlanRequest.model_validate(
        {
            "plan_id": "signal-next-open-async",
            "source": "test-fixture",
            "market_date": "2026-06-04",
            "items": [
                {
                    "symbol": "005930",
                    "side": "buy",
                    "intent": "next_open_buy",
                    "decision_agent_name": "signal-decider",
                    "quantity_policy": {
                        "type": "exec_agent_sized",
                        "budget_krw": "150000",
                        "estimated_reference_price": "71500",
                    },
                    "risk_context": {
                        "exchange": "KRX",
                        "target_price": "76000",
                        "stop_price": "69000",
                    },
                }
            ],
        }
    )
    with session_factory() as db:
        created = create_execution_plan(
            settings=settings,
            db=db,
            command=command,
        )
        assert created.status_code == 201
        item_id = str(created.payload["persisted_execution_item_ids"][0])
    with session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "watch_sizing_pending"
        assert item.quantity_policy["type"] == "exec_agent_sized"
        assert db.scalars(select(ExecutionJob)).all() == []

    observed_candidates: list[dict[str, object]] = []

    def approve(**kwargs: object) -> dict[str, object]:
        payload = kwargs["payload"]
        assert isinstance(payload, dict)
        candidates = payload["candidates"]
        assert isinstance(candidates, list) and isinstance(candidates[0], dict)
        observed_candidates.extend(candidates)
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "signal-next-open-async",
            "_runtime_provenance": _verified_sizing_runtime(
                settings,
                session_id="signal-next-open-session",
                request_payload=payload,
            ),
            "decisions": [
                {
                    "execution_plan_item_id": item_id,
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 2,
                    "approved_notional_krw": "143000",
                    "sizing_price": "71500",
                    "confidence": "0.7",
                    "target_profile": "short_swing",
                    "reason_code": "approved",
                    "reason": "bounded signal entry",
                }
            ],
        }

    monkeypatch.setattr(watch_sizing, "_request_exec_agent_decisions", approve)
    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=WatchSizingBrokerStub(
            reconcile_payloads=[],
            snapshot=BrokerAccountSnapshot(
                available_cash=Decimal("10000000"),
                buying_power=Decimal("10000000"),
                observed_at=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
                source="stub:account_snapshot",
            ),
        ),
        now=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
    )

    assert result.payload["applied"][0]["approved"] == 1
    assert observed_candidates[0]["entry_reference_price"] == "71500"
    assert observed_candidates[0]["entry_reference_source"] == (
        "quantity_policy.estimated_reference_price"
    )
    with session_factory() as db:
        item = db.get(ExecutionPlanItem, item_id)
        assert item is not None
        assert item.status == "next_open_buy_scheduled"
        assert item.limit_price == Decimal("71500")
        assert item.quantity_policy["approval_provenance"]["kind"] == (
            "verified_exec_agent_watch_sizing"
        )
        jobs = db.scalars(select(ExecutionJob)).all()
        assert len(jobs) == 1
        assert jobs[0].execution_plan_item_id == item.id

    retry = run_due_watch_exec_agent_sizing(
        settings=settings,
        session_factory=session_factory,
        adapter=WatchSizingBrokerStub(reconcile_payloads=[]),
        now=datetime(2026, 6, 3, 23, 31, tzinfo=UTC),
    )
    assert retry.payload["reason"] == "no_unsized_watch_candidates"
    with session_factory() as db:
        assert len(db.scalars(select(ExecutionJob)).all()) == 1
    engine.dispose()


def test_next_open_sizing_quarantines_a_candidate_bound_to_another_account(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'foreign-next-open-binding.db'}"
    original_settings = Settings(
        database_url=database_url,
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-route-binding-secret-for-tests",
    )
    rotated_settings = original_settings.model_copy(
        update={"kis_account_number": "87654321"}
    )
    engine = create_db_engine(original_settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    item = _next_open_watch_item(item_id="foreign-next-open")
    item.status = "pending"
    item.risk_context, item.quantity_policy = bind_kis_account_route(
        settings=original_settings,
        provider="kis",
        risk_context=item.risk_context,
        quantity_policy=item.quantity_policy,
    )
    with session_factory() as db:
        db.add(
            ExecutionPlan(
                external_plan_id="foreign-next-open-plan",
                source="core-signal-decider",
                market_date=date(2026, 6, 4),
                mode="paper",
                status="accepted",
                items=[item],
            )
        )
        db.commit()

    agent_was_called = False

    def unexpected_agent_call(**_kwargs: object) -> dict[str, object]:
        nonlocal agent_was_called
        agent_was_called = True
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "foreign-next-open-plan",
            "decisions": [
                {
                    "execution_plan_item_id": item.id,
                    "symbol": item.symbol,
                    "action": "reject",
                    "reason_code": "test",
                    "reason": "should not be consulted",
                }
            ],
        }

    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions",
        unexpected_agent_call,
    )
    adapter = WatchSizingBrokerStub(reconcile_payloads=[])

    result = run_due_watch_exec_agent_sizing(
        settings=rotated_settings,
        session_factory=session_factory,
        adapter=adapter,
        now=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
    )

    assert agent_was_called is False
    assert "snapshot" not in adapter.events
    assert result.payload["applied"][0]["account_route_mismatch_item_ids"] == [
        item.id
    ]
    with session_factory() as db:
        persisted = db.get(ExecutionPlanItem, item.id)
        persisted_plan = db.scalar(select(ExecutionPlan))
        assert persisted is not None
        assert persisted_plan is not None
        assert persisted.status == "exec_agent_rejected"
        assert persisted.risk_context["watch_exec_agent_account_route_guard"][
            "reason_code"
        ] == "account_route_mismatch"
        _assert_canonical_terminal_rejection_policy(
            persisted.quantity_policy,
            reason_code="account_route_mismatch",
        )
        # The active settings no longer prove the persisted account scope, so
        # the finalizer must not claim that this foreign-route family has no
        # exposure.
        assert persisted_plan.status == "accepted"
        assert result.payload["applied"][0]["no_trade"]["completed"] is False
    engine.dispose()


def test_next_open_approval_at_market_open_is_terminal_without_a_job() -> None:
    plan = ExecutionPlan(
        id="plan-next-open-late",
        external_plan_id="plan-next-open-late",
        strategy_run_id="strategy-1",
        source="core-signal-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[_next_open_watch_item(item_id="next-open-late")],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": plan.external_plan_id,
            "decisions": [
                {
                    "execution_plan_item_id": "next-open-late",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 2,
                    "approved_notional_krw": "143000",
                    "sizing_price": "71500",
                    "reason_code": "approved",
                    "reason": "response arrived at the deadline",
                }
            ],
        },
        account_context={
            "available": True,
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
        },
        expected_item_ids={"next-open-late"},
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert plan.items[0].status == "next_open_buy_expired"
    _assert_canonical_terminal_rejection_policy(
        plan.items[0].quantity_policy,
        reason_code="missed_next_open_window",
    )
    assert plan.items[0].jobs == []


def test_due_next_open_expiry_uses_canonical_terminal_rejection() -> None:
    item = _next_open_watch_item(item_id="next-open-expiry")
    plan = ExecutionPlan(
        id="plan-next-open-expiry",
        external_plan_id="plan-next-open-expiry",
        source="core-signal-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[item],
    )

    summary = watch_sizing._expire_missed_next_open_items(
        db=None,
        plan=plan,
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert summary["rejected"] == 1
    assert summary["missed_next_open_item_ids"] == [item.id]
    assert item.status == "next_open_buy_expired"
    _assert_canonical_terminal_rejection_policy(
        item.quantity_policy,
        reason_code="missed_next_open_window",
    )
    assert item.quantity_policy["exec_agent_decision"]["source"] == (
        "watch_exec_agent_sizing"
    )


def test_next_open_approval_without_runtime_provenance_is_rejected_preopen() -> None:
    plan = ExecutionPlan(
        id="plan-next-open-unverified-runtime",
        external_plan_id="plan-next-open-unverified-runtime",
        strategy_run_id="strategy-1",
        source="core-signal-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[_next_open_watch_item(item_id="next-open-unverified")],
    )

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="paper"),
        plan=plan,
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": plan.external_plan_id,
            "decisions": [
                {
                    "execution_plan_item_id": "next-open-unverified",
                    "symbol": "005930",
                    "action": "approve",
                    "quantity": 1,
                    "approved_notional_krw": "71500",
                    "sizing_price": "71500",
                    "reason_code": "approved",
                    "reason": "missing runtime envelope",
                }
            ],
        },
        account_context={
            "available": True,
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
        },
        expected_item_ids={"next-open-unverified"},
        now=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1


@pytest.mark.parametrize(
    "runtime_provenance",
    [
        None,
        {
            "actual_agent_id": "other-agent",
            "actual_model_ref": "openai/gpt-5.6-sol",
            "session_id": "session-1",
        },
        {
            "actual_agent_id": "exec-agent",
            "actual_model_ref": "other/model",
            "session_id": "session-1",
        },
        {
            "actual_agent_id": "exec-agent",
            "actual_model_ref": "openai/gpt-5.6-sol",
            "session_id": None,
        },
    ],
    ids=["missing", "wrong-agent", "wrong-model", "missing-run"],
)
def test_live_intraday_watch_approval_requires_exact_runtime_provenance(
    runtime_provenance: dict[str, object] | None,
) -> None:
    decided_at = datetime(2026, 6, 4, 0, 0, tzinfo=UTC)
    item = _watch_item(
        item_id="live-intraday-runtime-proof",
        risk_context={"target_price": "78000", "stop_price": "69000"},
    )
    plan = ExecutionPlan(
        external_plan_id="plan-live-intraday-runtime-proof",
        items=[item],
    )
    payload: dict[str, object] = {
        "version": "manual-exec-agent-watch-sizing-v1",
        "plan_id": plan.external_plan_id,
        "decisions": [
            {
                "execution_plan_item_id": item.id,
                "symbol": item.symbol,
                "action": "approve",
                "quantity": 1,
                "approved_notional_krw": "71500",
                "sizing_price": "71500",
                "reason": "must not be trusted",
            }
        ],
    }
    if runtime_provenance is not None:
        payload["_runtime_provenance"] = runtime_provenance

    summary = _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="live"),
        plan=plan,
        decision_payload=payload,
        account_context={
            "available": True,
            "buy_sizing_available": True,
            "observed_at": decided_at.isoformat(),
            "orderable_cash_krw": "1000000",
            "buying_power_krw": "1000000",
            "holdings": [],
            "open_orders": [],
        },
        expected_item_ids={item.id},
        now=decided_at,
    )

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    assert item.status == "exec_agent_rejected"
    assert item.quantity_policy["reason_code"] == "unverified_exec_agent_runtime"
    assert plan.items[0].status == "exec_agent_rejected"
    decision = plan.items[0].quantity_policy["exec_agent_decision"]
    assert decision["reason_code"] == "unverified_exec_agent_runtime"
    assert decision["quantity"] == 0
    assert decision["approved_quantity"] == 0
    assert decision["approved_notional_krw"] is None
    assert decision["sizing_price"] is None
    assert plan.items[0].jobs == []


def test_unverified_approval_keeps_proposed_sizing_only_in_the_audit(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'unverified-approval-audit.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            item = _watch_item(
                item_id="unverified-approval-audit-item",
                risk_context={"target_price": "78000", "stop_price": "69000"},
            )
            plan = ExecutionPlan(
                external_plan_id="unverified-approval-audit-plan",
                source="kai-trading-core",
                mode="paper",
                items=[item],
            )
            db.add(plan)
            db.flush()

            summary = _apply_decisions(
                db=db,
                settings=settings,
                plan=plan,
                decision_payload={
                    "version": "manual-exec-agent-watch-sizing-v1",
                    "plan_id": plan.external_plan_id,
                    "decisions": [
                        {
                            "execution_plan_item_id": item.id,
                            "symbol": item.symbol,
                            "action": "approve",
                            "quantity": 7,
                            "approved_notional_krw": "500500",
                            "sizing_price": "71500",
                            "reason_code": "approved",
                            "reason": "proposal from an unverified runtime",
                        }
                    ],
                },
                account_context={
                    "available": True,
                    "orderable_cash_krw": "1000000",
                    "buying_power_krw": "1000000",
                    "holdings": [],
                    "open_orders": [],
                },
                expected_item_ids={item.id},
                now=datetime(2026, 6, 3, 23, 30, tzinfo=UTC),
            )
            db.flush()

            assert summary["rejected"] == 1
            final = item.quantity_policy["exec_agent_decision"]
            assert final["quantity"] == 0
            assert final["approved_quantity"] == 0
            assert final["approved_notional_krw"] is None
            assert final["sizing_price"] is None
            audit = db.scalar(
                select(ExecutionPlanItemAudit).where(
                    ExecutionPlanItemAudit.execution_plan_item_id == item.id,
                    ExecutionPlanItemAudit.event_type
                    == "watch_exec_agent_sizing_decision",
                )
            )
            assert audit is not None
            assert audit.payload["agent_decision"]["action"] == "approve"
            assert audit.payload["agent_decision"]["quantity"] == 7
            assert audit.payload["agent_decision"]["approved_notional_krw"] == (
                "500500"
            )
            assert audit.payload["final_decision"]["action"] == "reject"
            assert audit.payload["final_decision"]["quantity"] == 0
            assert audit.payload["final_decision"]["approved_quantity"] == 0
    finally:
        engine.dispose()


def test_apply_decisions_audits_the_final_target_geometry_rejection(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-final-audit.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    with session_factory() as session:
        plan = ExecutionPlan(
            external_plan_id="plan-final-audit",
            strategy_run_id="strategy-1",
            source="core-short-term-decider",
            market_date=date(2026, 6, 4),
            mode="paper",
            items=[
                _watch_item(
                    item_id="watch-item-final-audit",
                    symbol="005930",
                    risk_context={"target_price": "71000", "stop_price": "59000"},
                )
            ],
        )
        session.add(plan)
        session.commit()
        plan_id = plan.id

    with session_factory() as session:
        plan = watch_sizing._load_plan_for_update(db=session, plan_id=plan_id)
        summary = _apply_decisions(
            db=session,
            settings=settings,
            plan=plan,
            account_context={
                "available": True,
                "orderable_cash_krw": "10000000",
                "buying_power_krw": "10000000",
                "holdings": [],
                "open_orders": [],
            },
            expected_item_ids={"watch-item-final-audit"},
            request_payload=_DIRECT_APPLY_REQUEST_PAYLOAD,
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
                "_runtime_provenance": _verified_sizing_runtime(
                    settings,
                    session_id="session-final-audit",
                ),
                "decisions": [
                    {
                        "execution_plan_item_id": "watch-item-final-audit",
                        "symbol": "005930",
                        "action": "approve",
                        "quantity": 3,
                        "approved_notional_krw": 214500,
                        "sizing_price": "60000",
                        "confidence": 0.68,
                        "target_profile": "short_swing",
                        "reason_code": "agent_approve",
                        "reason": "agent requested approval",
                    }
                ],
            },
        )
        session.commit()

    assert summary["approved"] == 0
    assert summary["rejected"] == 1
    with session_factory() as session:
        decision_audit = session.scalar(
            select(ExecutionPlanItemAudit).where(
                ExecutionPlanItemAudit.event_type == "watch_exec_agent_sizing_decision"
            )
        )
        assert decision_audit is not None
        assert decision_audit.summary["action"] == "reject"
        assert decision_audit.summary["reason_code"] == "allowed_action_envelope_blocked"
        assert decision_audit.payload["agent_decision"]["action"] == "approve"
        assert decision_audit.payload["final_decision"]["action"] == "reject"
        assert "invalid_target_distance" in decision_audit.payload[
            "deterministic_context"
        ]["allowed_action_envelope"]["hard_reject_reasons"]
        assert decision_audit.payload["deterministic_context"][
            "allowed_action_envelope"
        ]["reference_price"] == "71500.0000"
        assert decision_audit.payload["runtime_provenance"]["configured_model_hint"] == "openai/gpt-5.6-sol"
        assert decision_audit.payload["runtime_provenance"]["actual_model_ref"] == "openai/gpt-5.6-sol"


def test_exec_agent_request_removes_temporary_payload_file(tmp_path, monkeypatch) -> None:
    original_named_temporary_file = watch_sizing.tempfile.NamedTemporaryFile
    captured: dict[str, object] = {}

    def named_temporary_file(*args, **kwargs):
        return original_named_temporary_file(*args, dir=tmp_path, **kwargs)

    monkeypatch.setattr(watch_sizing.tempfile, "NamedTemporaryFile", named_temporary_file)

    def run_exec_agent_command(command, **_):
        captured["command"] = command
        message_path = Path(command[command.index("--message-file") + 1])
        captured["message"] = message_path.read_text(encoding="utf-8")
        return SimpleNamespace(
            stdout=(
                '{"meta":{"agentMeta":{"sessionId":"session-temp-cleanup",'
                '"agentId":"exec-agent","provider":"openai","model":"gpt-5.6-sol"}},"payloads":[{"text":"'
                '{\\"version\\":\\"manual-exec-agent-watch-sizing-v1\\",'
                '\\"plan_id\\":\\"plan-temp-cleanup\\",\\"decisions\\":[]}'
                '"}]}'
            )
        )

    monkeypatch.setattr(watch_sizing, "_run_exec_agent_command", run_exec_agent_command)
    monkeypatch.setattr(
        watch_sizing,
        "capture_runtime_contract",
        lambda **_: matching_runtime_contract_snapshot(),
    )

    response = watch_sizing._request_exec_agent_decisions(
        settings=Settings(runtime_mode="paper"),
        payload={"plan_id": "plan-temp-cleanup", "candidates": []},
    )

    assert response["plan_id"] == "plan-temp-cleanup"
    assert "--message-file" in captured["command"]
    assert "--message" not in captured["command"]
    assert captured["command"][captured["command"].index("--model") + 1] == (
        Settings().exec_agent_model
    )
    assert '"plan_id": "plan-temp-cleanup"' in captured["message"]
    assert '"version":"watch-exec-agent-binding-v2"' in captured["message"]
    assert '"exchange":"..."' in captured["message"]
    assert "Read /" not in captured["message"]
    provenance = response["_runtime_provenance"]
    assert isinstance(provenance, dict)
    assert {
        key: value
        for key, value in provenance.items()
        if key not in {"runtime_contract", "requested_session_key", "request_sha256"}
    } == {
        "agent_id": "exec-agent",
        "configured_agent_id": "exec-agent",
        "configured_model_hint": "openai/gpt-5.6-sol",
        "actual_agent_id": "exec-agent",
        "actual_provider": "openai",
        "actual_model": "gpt-5.6-sol",
        "actual_model_ref": "openai/gpt-5.6-sol",
        "session_id": "session-temp-cleanup",
    }
    assert str(provenance["requested_session_key"]).startswith("watch-sizing-")
    assert runtime_contract_proof_is_valid(
        provenance["runtime_contract"],
        agent_id="exec-agent",
        expected_model_ref=Settings().exec_agent_model,
        requested_session_key=str(provenance["requested_session_key"]),
        reported_session_id="session-temp-cleanup",
        request_sha256=str(provenance["request_sha256"]),
    )
    assert list(tmp_path.iterdir()) == []


def test_watch_sizing_timeout_closes_pipes_without_unbounded_communicate(
    monkeypatch,
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

        def communicate(self, *, timeout=None):
            self.communicate_calls += 1
            if self.communicate_calls > 1:
                raise AssertionError("timed-out command must not communicate again")
            raise watch_sizing.subprocess.TimeoutExpired(
                cmd=["hung-agent"],
                timeout=timeout,
                output="partial stdout",
                stderr="partial stderr",
            )

        def wait(self, *, timeout=None):
            self.wait_calls += 1
            raise watch_sizing.subprocess.TimeoutExpired(
                cmd=["hung-agent"],
                timeout=timeout,
            )

    process = HungProcess()
    signals: list[int] = []
    monkeypatch.setattr(
        watch_sizing.subprocess,
        "Popen",
        lambda *args, **kwargs: process,
    )
    monkeypatch.setattr(
        watch_sizing.os,
        "killpg",
        lambda pid, sent_signal: signals.append(sent_signal),
    )

    with pytest.raises(watch_sizing.subprocess.TimeoutExpired) as raised:
        watch_sizing._run_exec_agent_command(
            ["hung-agent"],
            timeout_seconds=1,
        )

    assert raised.value.output == "partial stdout"
    assert raised.value.stderr == "partial stderr"
    assert process.communicate_calls == 1
    assert process.wait_calls == 2
    assert signals == [watch_sizing.signal.SIGTERM, watch_sizing.signal.SIGKILL]
    assert process.stdout.closed is True
    assert process.stderr.closed is True


def test_watch_sizing_write_reload_locks_and_refreshes_plan_items() -> None:
    statements: list[object] = []
    expected = ExecutionPlan(
        id="plan-lock",
        external_plan_id="external-plan-lock",
        source="core-short-term-decider",
        mode="paper",
    )
    expected_item = ExecutionPlanItem(
        id="item-lock",
        execution_plan_id=expected.id,
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="watch",
    )

    class ScalarResult:
        def __init__(self, values):
            self.values = values

        def one(self):
            return self.values[0]

        def all(self):
            return self.values

    class RecordingSession:
        def scalars(self, statement):
            statements.append(statement)
            if len(statements) == 1:
                return ScalarResult([expected])
            return ScalarResult([expected_item])

    loaded = watch_sizing._load_plan_for_update(
        db=RecordingSession(),
        plan_id="plan-lock",
    )

    assert loaded is expected
    assert loaded.items == [expected_item]
    assert len(statements) == 2
    assert statements[0]._for_update_arg is not None
    assert statements[1]._for_update_arg is not None
    assert statements[1].get_execution_options()["populate_existing"] is True
    assert len(statements[1]._order_by_clauses) == 2


def test_execution_jobs_enforce_one_job_type_per_plan_item() -> None:
    unique_column_sets = {
        tuple(column.name for column in constraint.columns)
        for constraint in ExecutionJob.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert ("execution_plan_item_id", "job_type") in unique_column_sets
