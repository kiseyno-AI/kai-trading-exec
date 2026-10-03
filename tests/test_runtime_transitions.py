from app.broker.base import NormalizedBrokerOrderStatus
from app.core.config import Settings
from app.models.runtime import ExecutionPlanItem
from app.services.execution_api import _is_runtime_monitored_item
from app.services.monitoring import (
    BLOCKED_MONITORING_STATUSES,
    NON_MONITORABLE_PLAN_ITEM_STATUSES,
    PENDING_DISPATCH_MONITORING_STATUSES,
)
from app.services.monitoring_price_source import _ROUTE_AUTHORITY_STATUSES
from app.services.max_holding_actionability import ACTIVE_MAX_HOLDING_STATUSES
from app.services.portfolio_risk import ACTIVE_MONITORING_STATUSES
from app.services.rejected_order_retries import RETRY_COMPATIBLE_PLAN_ITEM_STATUSES
from app.services.runtime import monitoring_segments
from app.services.runtime_transitions import (
    ACTIVE_SELL_MONITORING_STATUSES,
    ACTIVE_WATCH_MONITORING_STATUSES,
    AGENT_ACTIVE_MONITORING_STATUSES,
    EXECUTION_JOB_STATUSES,
    PLAN_ITEM_STATUSES,
    SELL_ACTIVE_MONITORING_STATUSES,
    SELL_AGENT_ACTIVE_STATUSES,
    SELL_TRIGGERED_BLOCKED_STATUSES,
    SELL_TRIGGERED_PENDING_DISPATCH_STATUSES,
    plan_item_status_for_broker_order_status,
)
from app.services.trigger_time_exec_agent import (
    ACTIVE_PEER_MONITORING_STATUSES,
    AGENT_ACTIVE_STATUSES,
)


def test_open_broker_placements_map_to_existing_non_monitorable_plan_item_statuses() -> None:
    for broker_status in (
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
    ):
        plan_item_status = plan_item_status_for_broker_order_status(broker_status)

        assert plan_item_status in {"open", "partially_filled"}
        assert plan_item_status in NON_MONITORABLE_PLAN_ITEM_STATUSES


def test_cancel_broker_statuses_map_to_explicit_plan_item_statuses() -> None:
    assert (
        plan_item_status_for_broker_order_status(
            NormalizedBrokerOrderStatus.CANCEL_PENDING.value
        )
        == "cancel_pending"
    )
    assert (
        plan_item_status_for_broker_order_status(
            NormalizedBrokerOrderStatus.CANCELED.value
        )
        == "canceled"
    )


def test_plan_item_status_catalogue_covers_runtime_monitoring_and_core_states() -> None:
    emitted_statuses = {
        "core_activation_pending",
        "contract_quarantined",
        "revoked",
        "watch_sizing_pending",
        "target_sell_pending_config",
        "stop_sell_pending_config",
        "max_holding_close_sell_pending_config",
        "target_sell_triggered_blocked",
        "stop_sell_triggered_blocked",
        "max_holding_close_sell_triggered_blocked",
        "max_holding_close_sell_triggered_pending_dispatch",
    }

    assert emitted_statuses <= PLAN_ITEM_STATUSES


def test_execution_job_status_catalogue_covers_core_terminal_states() -> None:
    assert {"revoked", "superseded"} <= EXECUTION_JOB_STATUSES


def test_runtime_monitoring_liveness_excludes_terminal_target_and_stop_sells() -> None:
    for intent in ("target_sell", "stop_sell"):
        for status in ("filled", "expired", "superseded", "cancelled"):
            item = ExecutionPlanItem(side="sell", intent=intent, status=status)

            assert _is_runtime_monitored_item(item) is False


def test_runtime_monitoring_liveness_includes_live_max_holding_sells() -> None:
    for status in (
        "max_holding_close_sell_monitoring",
        "max_holding_close_sell_triggered_blocked",
        "max_holding_close_sell_triggered_pending_dispatch",
        "triggered_pending_agent",
        "agent_delay",
        "agent_approved_pending_submit",
    ):
        item = ExecutionPlanItem(
            side="sell",
            intent="max_holding_close_sell",
            status=status,
        )

        assert _is_runtime_monitored_item(item) is True


def test_sell_monitoring_semantic_subsets_cover_every_sell_intent_consistently() -> None:
    intents = ("target_sell", "stop_sell", "max_holding_close_sell")

    assert SELL_ACTIVE_MONITORING_STATUSES == {
        f"{intent}_monitoring" for intent in intents
    }
    assert SELL_TRIGGERED_BLOCKED_STATUSES == {
        f"{intent}_triggered_blocked" for intent in intents
    }
    assert SELL_TRIGGERED_PENDING_DISPATCH_STATUSES == {
        f"{intent}_triggered_pending_dispatch" for intent in intents
    }
    assert ACTIVE_SELL_MONITORING_STATUSES == (
        SELL_ACTIVE_MONITORING_STATUSES
        | SELL_TRIGGERED_BLOCKED_STATUSES
        | SELL_TRIGGERED_PENDING_DISPATCH_STATUSES
        | SELL_AGENT_ACTIVE_STATUSES
    )
    assert SELL_TRIGGERED_BLOCKED_STATUSES <= BLOCKED_MONITORING_STATUSES
    assert (
        SELL_TRIGGERED_PENDING_DISPATCH_STATUSES
        <= PENDING_DISPATCH_MONITORING_STATUSES
    )


def test_monitoring_consumers_derive_every_canonical_sell_status() -> None:
    assert ACTIVE_SELL_MONITORING_STATUSES <= _ROUTE_AUTHORITY_STATUSES
    assert SELL_TRIGGERED_BLOCKED_STATUSES <= RETRY_COMPATIBLE_PLAN_ITEM_STATUSES
    assert set(ACTIVE_MONITORING_STATUSES) == ACTIVE_SELL_MONITORING_STATUSES
    assert SELL_AGENT_ACTIVE_STATUSES == AGENT_ACTIVE_MONITORING_STATUSES
    assert AGENT_ACTIVE_STATUSES == AGENT_ACTIVE_MONITORING_STATUSES
    assert ACTIVE_PEER_MONITORING_STATUSES == (
        ACTIVE_SELL_MONITORING_STATUSES | ACTIVE_WATCH_MONITORING_STATUSES
    )
    assert set(ACTIVE_MAX_HOLDING_STATUSES) == (
        {"max_holding_close_sell_pending_config"}
        | SELL_AGENT_ACTIVE_STATUSES
        | {
            status
            for status in ACTIVE_SELL_MONITORING_STATUSES
            if status.startswith("max_holding_close_sell_")
        }
    )


def test_runtime_segment_reports_every_canonical_live_sell_status() -> None:
    items = [
        ExecutionPlanItem(
            id=f"sell-{index}",
            symbol=f"{100000 + index:06d}",
            side="sell",
            intent=(
                "max_holding_close_sell"
                if status.startswith("max_holding_close_sell")
                else "stop_sell"
            ),
            status=status,
        )
        for index, status in enumerate(sorted(ACTIVE_SELL_MONITORING_STATUSES))
    ]

    segments = monitoring_segments(
        settings=Settings(database_url="sqlite:///:memory:", runtime_mode="paper"),
        db=None,  # type: ignore[arg-type]
        items=items,
    )

    assert segments["held_position_sell_monitoring"]["item_count"] == len(items)
