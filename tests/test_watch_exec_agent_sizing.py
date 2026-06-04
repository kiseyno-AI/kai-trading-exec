from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.broker.base import BrokerAccountHolding, BrokerAccountOpenOrder, BrokerAccountSnapshot
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services.monitoring import _watch_buy_candidate_budget
from app.services.watch_exec_agent_sizing import (
    _apply_decisions,
    _applicable_decision_ids,
    _account_context_payload,
    _candidate_payload,
    _candidate_universe_payload,
    _chunks,
    _decision_record,
    _sizing_request_payload,
)


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


def test_chunks_splits_watch_candidates_into_small_batches() -> None:
    items = [{"execution_plan_item_id": f"item-{index}"} for index in range(53)]

    chunks = _chunks(items, 10)

    assert [len(chunk) for chunk in chunks] == [10, 10, 10, 10, 10, 3]


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
        plan=plan,
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
