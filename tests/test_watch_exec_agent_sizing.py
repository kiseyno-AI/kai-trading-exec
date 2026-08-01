from __future__ import annotations

import threading
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import select

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
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit
from app.services import watch_exec_agent_sizing as watch_sizing
from app.services.monitoring import (
    _watch_buy_candidate_budget,
    _watch_buy_exec_agent_sizing_fallback_allowed,
    list_autonomous_monitoring_symbols,
)
from app.services.watch_exec_agent_sizing import (
    _apply_decisions,
    _applicable_decision_ids,
    _account_context_payload,
    _candidate_payload,
    _candidate_universe_payload,
    _chunks,
    _decision_record,
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
    ) -> None:
        self.reconcile_payloads = reconcile_payloads
        self.snapshot_error = snapshot_error
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

    def get_account_snapshot(self, access_token: str) -> BrokerAccountSnapshot:
        self.events.append("snapshot")
        if self.snapshot_error is not None:
            raise self.snapshot_error
        return BrokerAccountSnapshot(
            available_cash=Decimal("10000000"),
            buying_power=Decimal("10000000"),
            observed_at=datetime(2026, 6, 4, 0, 1, tzinfo=UTC),
            source="stub:account_snapshot",
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=True, strategy="stub")

    def reconcile_order(self, access_token: str, broker_order_id: str, *, exchange: str = "KRX") -> dict[str, object]:
        self.events.append("reconcile")
        self.reconcile_calls.append(broker_order_id)
        return self.reconcile_payloads[min(len(self.reconcile_calls) - 1, len(self.reconcile_payloads) - 1)]

    def build_headers(self, access_token: str, *, transaction_id: str | None = None, hashkey: str | None = None) -> dict[str, str]:
        return {"authorization": f"Bearer {access_token}"}


def _watch_item(
    *,
    item_id: str = "watch-item-1",
    symbol: str = "005930",
    risk_context: dict[str, object] | None,
) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id=item_id,
        plan_item_index=0,
        symbol=symbol,
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_pending",
        trigger_price=Decimal("71000"),
        limit_price=Decimal("71500"),
        quantity_policy={"type": "exec_agent_sized", "budget_krw": "1500000"},
        risk_context=risk_context,
    )


def _stale_local_order(*, symbol: str = "005930", broker_order_id: str = "stale-open-1") -> BrokerOrder:
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


def test_short_transaction_batches_candidates_across_single_item_plans(tmp_path, monkeypatch) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'watch-sizing-multi-plan.db'}",
        runtime_mode="paper",
        exec_agent_enabled=True,
        exec_agent_watch_sizing_enabled=True,
        exec_agent_watch_sizing_start_local_time="00:00",
        exec_agent_watch_sizing_end_local_time="23:59",
        exec_agent_watch_sizing_batch_size=3,
        exec_agent_watch_sizing_max_batches_per_cycle=1,
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

    calls: list[list[str]] = []

    def fake_decisions(*, payload, **_) -> dict[str, object]:
        calls.append([str(candidate["execution_plan_item_id"]) for candidate in payload["candidates"]])
        return {
            "version": "manual-exec-agent-watch-sizing-v1",
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

    assert calls == [["watch-item-1", "watch-item-2", "watch-item-3"]]
    assert result.payload["batches_processed"] == 1
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

    assert broker.events.index("reconcile") < broker.events.index("snapshot")
    assert result.payload["applied"][0]["stale_state_blocked_item_ids"] == ["watch-item-stale"]
    assert item.status == "exec_agent_rejected"
    assert item.quantity_policy["exec_agent_decision"]["reason_code"] == "stale_order_broker_truth_unconfirmed"
    assert item.quantity_policy["stale_state_blockers"][0]["classification"] == "stale-terminal-unknown"
    assert order.status == "stale_terminal_unknown"
    assert order.last_error_classification == "unknown_status"
    assert unknown_event.payload["raw_status_details"] == {
        "status": "mystery_status",
        "broker_status": "ZZ99",
        "message": "new broker status",
        "observed_at": "2026-06-04T00:00:00+00:00",
    }
    assert audit is not None
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

    assert broker.events.index("reconcile") < broker.events.index("snapshot")
    assert result.payload["applied"][0]["stale_state_blocked_item_ids"] == ["watch-item-snapshot-fail"]
    assert item.status == "exec_agent_rejected"
    assert item.quantity_policy["stale_state_blockers"][0]["classification"] == "stale-terminal-unknown"
    assert audit is not None
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
            "target_price": "76000",
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
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
        candidate_universe=_candidate_universe_payload([candidate]),
    )

    envelope = payload["candidates"][0]["allowed_action_envelope"]  # type: ignore[index]
    assert envelope["schema_version"] == "allowed_action_envelope_v1"
    assert "buy" in envelope["allowed_actions"]
    assert envelope["max_quantity"] == 11
    assert envelope["max_notional_krw"] == "800000.0000"


def test_sizing_request_payload_blocks_existing_holding_in_allowed_action_envelope() -> None:
    settings = Settings(runtime_mode="live")
    item = _watch_item(risk_context={"target_price": "76000", "stop_price": "69000"})
    candidate = _candidate_payload(item)

    payload = _sizing_request_payload(
        settings=settings,
        plan=ExecutionPlan(external_plan_id="plan-envelope-holding", market_date=date(2026, 6, 4)),
        candidates=[candidate],
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [{"symbol": "005930", "quantity": 3}],
            "open_orders": [],
        },
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


def test_apply_decisions_applies_partial_response_and_leaves_missing_item_pending() -> None:
    plan = ExecutionPlan(
        id="plan-1",
        external_plan_id="plan-partial",
        strategy_run_id="strategy-1",
        source="core-short-term-decider",
        market_date=date(2026, 6, 4),
        mode="paper",
        items=[
            _watch_item(item_id="watch-item-1", symbol="005930", risk_context={"target_price": "72420"}),
            _watch_item(item_id="watch-item-2", symbol="000660", risk_context={"target_price": "128000"}),
        ],
    )

    summary = _apply_decisions(
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

    assert summary == {
        "plan_id": "plan-partial",
        "evaluated": 1,
        "approved": 1,
        "rejected": 0,
        "missing": 1,
        "missing_item_ids": ["watch-item-2"],
        "ignored_extra": 0,
    }
    assert plan.items[0].status == "watch_monitoring_active"
    assert plan.items[0].quantity_policy["quantity"] == 2
    assert plan.items[1].status == "watch_monitoring_pending"


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
                risk_context={"target_price": "72420", "stop_price": "69000"},
            ),
        ],
    )
    plan.items[0].status = "watch_buy_triggered_blocked"

    summary = _apply_decisions(
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
        expected_item_ids={"watch-item-blocked"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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


def test_apply_decisions_allows_small_agent_probe_below_min_notional() -> None:
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
                risk_context={
                    "target_price": "76000",
                    "stop_price": "69000",
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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


def test_apply_decisions_allows_when_order_lot_minimum_exceeds_agent_cap() -> None:
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
                risk_context={
                    "target_price": "76000",
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
            "decisions": [
                {
                    "execution_plan_item_id": "watch-item-1",
                    "symbol": "003550",
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
                    "target_price": "76000",
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
    plan.items[0].quantity_policy = {"type": "exec_agent_sized", "budget_krw": "5000000"}

    _apply_decisions(
        db=None,  # type: ignore[arg-type]
        settings=Settings(runtime_mode="live", vol_corr_sizing_enabled=True),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [{"symbol": "000660", "quantity": 3}],
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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
                    "target_price": "76000",
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "1000",
            "buying_power_krw": "1000",
            "holdings": [],
            "open_orders": [],
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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
    assert plan.items[0].quantity_policy["allowed_action_envelope"]["max_quantity"] == 0


def test_apply_decisions_rejects_when_allowed_action_envelope_has_hard_reject_reason() -> None:
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [{"symbol": "005930", "quantity": 1}],
            "open_orders": [],
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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
    assert "existing_holding" in policy["allowed_action_envelope"]["hard_reject_reasons"]
    assert "existing_holding" in policy["reason"]


def test_apply_decisions_rejects_when_allowed_action_envelope_disallows_buy_downsize(monkeypatch) -> None:
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
        settings=Settings(runtime_mode="live"),
        plan=plan,
        account_context={
            "available": True,
            "orderable_cash_krw": "10000000",
            "buying_power_krw": "10000000",
            "holdings": [],
            "open_orders": [],
        },
        expected_item_ids={"watch-item-1"},
        decision_payload={
            "version": "manual-exec-agent-watch-sizing-v1",
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
    assert policy["allowed_action_envelope"]["max_quantity"] == 5
    assert policy["reason"] == "allowed action envelope disallows buy/downsize approval"


def test_run_due_retry_applies_sizing_context_to_missing_decision(monkeypatch) -> None:
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
                    "target_price": "76000",
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
                    "target_price": "76000",
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
    account_context = {
        "available": True,
        "orderable_cash_krw": "10000000",
        "buying_power_krw": "10000000",
    }
    responses = [
        {
            "version": "manual-exec-agent-watch-sizing-v1",
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
            "decisions": [
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
                    "reason": "retry returns the missing candidate",
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

    monkeypatch.setattr(watch_sizing, "_load_candidate_plans", lambda **_: [plan])
    monkeypatch.setattr(watch_sizing, "_load_sizing_account_context", lambda **_: account_context)
    monkeypatch.setattr(
        watch_sizing,
        "_request_exec_agent_decisions_with_retries",
        lambda **_: responses.pop(0),
    )

    result = run_due_watch_exec_agent_sizing(
        settings=settings,
        db=db,  # type: ignore[arg-type]
        now=datetime(2026, 6, 4, 0, 0, tzinfo=UTC),
    )

    assert result.payload["applied"][0]["missing_item_ids"] == ["watch-item-2"]
    assert result.payload["applied"][1]["approved"] == 1
    assert db.committed is True
    retry_policy = plan.items[1].quantity_policy
    assert retry_policy["quantity"] == 3
    assert retry_policy["approved_notional"] == "214500.0000"
    assert retry_policy["sizing_adjustment"]["applied"] is False
    assert retry_policy["sizing_adjustment"]["cap_notional"] == "214500.0000"
    assert retry_policy["sizing_adjustment"]["runtime_cap_notional"] == "800000.0000"
    assert retry_policy["sizing_adjustment"]["min_notional_upsize_suppressed"] is False
    assert retry_policy["sizing_adjustment"]["reason"] is None


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
            decision_payload={
                "version": "manual-exec-agent-watch-sizing-v1",
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
        assert item.quantity_policy["exec_agent_watch_sizing_fallback_allowed"] is True
        assert item.quantity_policy["exec_agent_watch_sizing_attempts"][-1]["success"] is False
        assert _watch_buy_exec_agent_sizing_fallback_allowed(settings=settings, item=item) is True


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
