from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import BrokerAdapter
from app.core.config import Settings
from app.services.broker_orders import _domestic_equity_market_date_block


def _adapter(provider: str) -> BrokerAdapter:
    return cast(BrokerAdapter, SimpleNamespace(provider=provider))


def _command(**overrides: object) -> PlaceOrderRequest:
    payload: dict[str, object] = {
        "idempotency_key": "weekend-order-guard",
        "provider": "kis",
        "asset_class": "stock",
        "symbol": "003550",
        "side": "sell",
        "quantity": 2,
        "order_type": "market",
        "exchange": "KRX",
    }
    payload.update(overrides)
    return PlaceOrderRequest.model_validate(payload)


def test_domestic_equity_order_is_blocked_before_broker_submission_on_weekend() -> None:
    result = _domestic_equity_market_date_block(
        settings=Settings(runtime_mode="live", market_timezone="Asia/Seoul"),
        adapter=_adapter("kis"),
        command=_command(),
        now=datetime(2026, 8, 29, 0, 0, tzinfo=UTC),
    )

    assert result == {
        "reason": "domestic_equity_market_closed_weekend",
        "message": "domestic equity order deferred because the market date is a weekend",
        "market_date": "2026-08-29",
        "weekday": "saturday",
        "timezone": "Asia/Seoul",
        "provider": "kis",
        "exchange": "KRX",
    }


def test_domestic_equity_order_is_not_weekend_blocked_on_weekday() -> None:
    assert (
        _domestic_equity_market_date_block(
            settings=Settings(runtime_mode="live", market_timezone="Asia/Seoul"),
            adapter=_adapter("kis"),
            command=_command(),
            now=datetime(2026, 8, 31, 0, 0, tzinfo=UTC),
        )
        is None
    )


def test_weekend_guard_does_not_block_non_domestic_assets() -> None:
    weekend = datetime(2026, 8, 29, 0, 0, tzinfo=UTC)

    assert (
        _domestic_equity_market_date_block(
            settings=Settings(runtime_mode="live", market_timezone="Asia/Seoul"),
            adapter=_adapter("upbit"),
            command=_command(
                provider="upbit",
                asset_class="crypto",
                exchange="UPBIT",
                quantity=None,
                base_quantity=Decimal("0.01"),
            ),
            now=weekend,
        )
        is None
    )
    assert (
        _domestic_equity_market_date_block(
            settings=Settings(runtime_mode="live", market_timezone="Asia/Seoul"),
            adapter=_adapter("kis"),
            command=_command(
                asset_class="us_equity",
                symbol="AAPL",
                exchange="NASD",
                order_type="limit",
                limit_price=Decimal("200"),
            ),
            now=weekend,
        )
        is None
    )
