from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.broker.base import BrokerAccountPosition, BrokerAccountSnapshot
from app.services.broker_raw_holdings import snapshot_with_raw_balance_holdings


def test_raw_holding_overlay_preserves_snapshot_integrity_metadata() -> None:
    observed_at = datetime(2026, 9, 11, 14, 0, tzinfo=UTC)
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=observed_at,
        source="kis:account_snapshot",
        positions=[BrokerAccountPosition(symbol="005930", quantity=2)],
        holdings_complete=False,
        open_orders_complete=False,
        diagnostics={"parse": "incomplete"},
        broker_account_id="raw-account-id",
        payload={
            "balance": {
                "rt_cd": "0",
                "output1": [
                    {
                        "pdno": "005930",
                        "hldg_qty": "2",
                        "ord_psbl_qty": "2",
                        "pchs_avg_pric": "70000",
                        "prpr": "71000",
                        "evlu_amt": "142000",
                    }
                ],
            }
        },
    )

    overlaid = snapshot_with_raw_balance_holdings(
        snapshot,
        as_of=observed_at,
        max_age_seconds=300,
    )

    assert len(overlaid.holdings) == 1
    assert overlaid.positions == snapshot.positions
    assert overlaid.holdings_complete is False
    assert overlaid.open_orders_complete is False
    assert overlaid.diagnostics == snapshot.diagnostics
    assert overlaid.broker_account_id == "raw-account-id"


@pytest.mark.parametrize("value", ["NaN", "sNaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("field", ["hldg_qty", "ord_psbl_qty", "pchs_avg_pric"])
def test_raw_holding_overlay_treats_nonfinite_fields_as_unavailable(field, value) -> None:
    observed_at = datetime(2026, 9, 11, 14, 0, tzinfo=UTC)
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=observed_at,
        source="kis:account_snapshot",
        payload={
            "balance": {
                "rt_cd": "0",
                "output1": [{
                    "pdno": "005930",
                    "hldg_qty": "2",
                    "ord_psbl_qty": "0",
                    "pchs_avg_pric": "70000",
                    field: value,
                }],
            }
        },
    )

    overlaid = snapshot_with_raw_balance_holdings(
        snapshot, as_of=observed_at, max_age_seconds=300,
    )

    if field == "hldg_qty":
        assert overlaid is snapshot
        assert overlaid.holdings == []
    else:
        assert len(overlaid.holdings) == 1
        holding = overlaid.holdings[0]
        assert holding.quantity == 2
        assert holding.sellable_quantity == (None if field == "ord_psbl_qty" else 0)
        assert holding.average_price == (None if field == "pchs_avg_pric" else Decimal("70000"))
