from __future__ import annotations

from decimal import Decimal

from app.models.runtime import ExecutionPlanItem
from app.services.watch_exec_agent_sizing import _candidate_payload, _decision_record


def _watch_item(*, risk_context: dict[str, object] | None) -> ExecutionPlanItem:
    return ExecutionPlanItem(
        id="watch-item-1",
        plan_item_index=0,
        symbol="005930",
        side="buy",
        intent="intraday_watch_buy",
        status="watch_monitoring_pending",
        trigger_price=Decimal("71000"),
        limit_price=Decimal("71500"),
        quantity_policy={"type": "exec_agent_sized", "budget_krw": "1500000"},
        risk_context=risk_context,
    )


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
