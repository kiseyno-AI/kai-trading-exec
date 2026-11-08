"""A successful inquiry supersedes earlier lookup failures during stale cleanup."""

from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.db.session import create_session_factory
from app.models.runtime import ExecutionFill
from app.services import reconciliation
from test_reconciliation import OBSERVED_AT, ReconciliationStubBroker, _plan_backed_order
from test_reconciliation_concurrency import reconciliation_database


@pytest.mark.parametrize("status", ["open", "filled", "canceled"])
@pytest.mark.parametrize(("prior_code", "prior_classification"), [
    (None, None), ("KIER2570", "broker_rejected"), ("unknown", "unknown_status"),
])
def test_stale_cleanup_keeps_current_success_instead_of_cleared_error(
    reconciliation_database, status, prior_code, prior_classification,
):
    settings, engine = reconciliation_database
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        plan, item, order = _plan_backed_order(
            settings=settings, plan_id="stale-success", side="buy", intent="next_open_buy",
        )
        order.last_error_code = prior_code
        order.last_error_classification = prior_classification
        db.add(plan)
        db.commit()
        observed_at = OBSERVED_AT + timedelta(seconds=60)
        filled_quantity = 2 if status == "filled" else 0
        broker = ReconciliationStubBroker(
            reconcile_payloads=[{
                "broker_order_id": order.broker_order_id, "status": status,
                "filled_quantity": filled_quantity, "average_fill_price": "100",
                "observed_at": observed_at.isoformat(),
            }],
            account_snapshot=BrokerAccountSnapshot(
                observed_at=observed_at, source="kis:account_snapshot",
                holdings_complete=True, open_orders_complete=True,
                holdings=[BrokerAccountHolding(
                    symbol=order.symbol, quantity=2, average_price=Decimal("100"),
                )] if filled_quantity else [],
            ),
        )
        result = reconciliation.reconcile_stale_nonterminal_broker_orders(
            settings=settings, db=db, adapter=broker,
        )
        db.refresh(order)
        db.refresh(item)
        assert order.status == status
        assert item.status == status
        classification = {"open": "live", "filled": "closed", "canceled": "cancelled"}[status]
        assert result["counts"][classification] == 1
        assert order.last_error_code is None
        assert order.last_error_classification is None
        assert (db.scalar(select(func.sum(ExecutionFill.quantity))) or 0) == filled_quantity
        assert broker.order_calls == []
