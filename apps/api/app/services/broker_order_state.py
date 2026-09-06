from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Iterable

from sqlalchemy import func, or_

from app.broker.base import NormalizedBrokerOrderStatus
from app.core.config import Settings
from app.models.runtime import BrokerOrder

NONTERMINAL_ORDER_STATUSES = (
    NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
    NormalizedBrokerOrderStatus.SUBMITTED.value,
    NormalizedBrokerOrderStatus.OPEN.value,
    NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
    NormalizedBrokerOrderStatus.CANCEL_PENDING.value,
    NormalizedBrokerOrderStatus.RETRY_PENDING.value,
)
ACTIONABLE_OPEN_ORDER_STATUSES = (
    NormalizedBrokerOrderStatus.SUBMITTED.value,
    NormalizedBrokerOrderStatus.OPEN.value,
    NormalizedBrokerOrderStatus.PARTIALLY_FILLED.value,
)
UNTRUSTED_ACTIVE_ORDER_ERROR_CLASSIFICATIONS = frozenset(
    {
        "missing_broker_order_id",
        "unknown_status",
    }
)


def broker_order_observed_at_expr():
    """Return the lifecycle age anchor used by stale-order decisions.

    A reconciliation heartbeat must not make an old day order young again.
    """
    return func.coalesce(
        BrokerOrder.submitted_at,
        BrokerOrder.created_at,
        BrokerOrder.last_broker_sync_at,
        BrokerOrder.updated_at,
    )


def broker_order_activity_cutoff(
    *,
    settings: Settings,
    now: datetime | None = None,
) -> datetime:
    observed_at = now or datetime.now(UTC)
    return observed_at - timedelta(seconds=settings.stale_broker_order_reconciliation_threshold_seconds)


def active_local_broker_order_filters(
    *,
    settings: Settings,
    statuses: Iterable[str],
    now: datetime | None = None,
) -> list[object]:
    return [
        BrokerOrder.mode == settings.runtime_mode,
        BrokerOrder.status.in_(tuple(statuses)),
        broker_order_observed_at_expr() >= broker_order_activity_cutoff(settings=settings, now=now),
        or_(
            BrokerOrder.last_error_classification.is_(None),
            BrokerOrder.last_error_classification.not_in(
                tuple(UNTRUSTED_ACTIVE_ORDER_ERROR_CLASSIFICATIONS)
            ),
        ),
    ]
