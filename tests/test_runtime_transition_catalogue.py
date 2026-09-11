from app.services.runtime_transitions import PLAN_ITEM_STATUSES


def test_plan_item_status_catalogue_covers_every_monitoring_lifecycle_state() -> None:
    expected = {
        "watch_monitoring_pending",
        "watch_monitoring_active",
        "watch_buy_submitted",
        "watch_cancelled",
        "watch_expired",
        "superseded",
        "target_sell_pending_configuration",
        "target_sell_monitoring",
        "target_sell_triggered_pending_dispatch",
        "stop_sell_pending_configuration",
        "stop_sell_monitoring",
        "stop_sell_triggered_pending_dispatch",
        "max_holding_close_sell_monitoring",
        "max_holding_close_sell_triggered_pending_dispatch",
        "triggered_pending_agent",
        "agent_delay",
        "agent_rejected",
        "agent_approved_pending_submit",
    }

    assert expected <= PLAN_ITEM_STATUSES
    assert "missed_next_open_window" not in PLAN_ITEM_STATUSES
    assert "watch_exec_agent_sizing_route_unsupported" not in PLAN_ITEM_STATUSES
    assert "account_route_rejected" not in PLAN_ITEM_STATUSES
