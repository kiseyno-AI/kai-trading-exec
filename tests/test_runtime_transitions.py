from app.broker.base import NormalizedBrokerOrderStatus
from app.services.monitoring import NON_MONITORABLE_PLAN_ITEM_STATUSES
from app.services.runtime_transitions import plan_item_status_for_broker_order_status


def test_open_broker_placements_map_to_existing_non_monitorable_plan_item_statuses() -> None:
    for broker_status in (
        NormalizedBrokerOrderStatus.OPEN.value,
        NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
    ):
        plan_item_status = plan_item_status_for_broker_order_status(broker_status)

        assert plan_item_status in {"open", "partially_filled"}
        assert plan_item_status in NON_MONITORABLE_PLAN_ITEM_STATUSES
