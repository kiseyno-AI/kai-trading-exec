from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
)
from app.core.config import Settings
from app.services import broker_orders
from app.services.market_regime_guardrails import MarketRegimeGuardrailResult


CHECKED_AT = datetime(2026, 11, 5, 1, 0, tzinfo=UTC)
SOURCE_MARKET_DATE = date(2026, 11, 4)


def _settings() -> Settings:
    return Settings(
        runtime_mode="live",
        automation_enabled=True,
        market_context_enabled=True,
        market_regime_guard_enabled=True,
        market_context_base_url="http://core.test",
    )


def _command(*, quantity: int = 3) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        idempotency_key="final-market-regime",
        provider="kis",
        asset_class="stock",
        symbol="005930",
        side="buy",
        quantity=quantity,
        order_type="limit",
        limit_price=Decimal("70000"),
        time_in_force="day",
        exchange="KRX",
    )


def _plan_item(*, include_stale_sector: bool = False) -> SimpleNamespace:
    risk_context: dict[str, object] = {"market": "KOSPI"}
    if include_stale_sector:
        risk_context["sector_context"] = {
            "sector_name": "persisted-plan-sector",
            "relative_return_pct": "99",
        }
    return SimpleNamespace(
        risk_context=risk_context,
        quantity_policy={},
        source_market_date=SOURCE_MARKET_DATE,
        intent="intraday_watch_buy",
    )


def _snapshot() -> BrokerAccountSnapshot:
    return BrokerAccountSnapshot(
        observed_at=CHECKED_AT,
        source="kis:account_snapshot",
        holdings=[BrokerAccountHolding(symbol="000660", quantity=2)],
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="005930",
                side="buy",
                quantity=1,
                remaining_quantity=1,
            )
        ],
        holdings_complete=True,
        open_orders_complete=True,
    )


def _live_breadth() -> dict[str, object]:
    breadth = {
        "source": "kai-trading-core:/market/snapshot",
        "market": "KOSPI",
        "market_date": "2026-11-05",
        "observed_at": "2026-11-05T00:59:30+00:00",
        "up_count": 500,
        "down_count": 300,
    }
    return {
        "market_context_status": "ready",
        "market_regime_context": {"markets": {"KOSPI": breadth}},
    }


def _live_sector() -> dict[str, object]:
    sector = {
        "source": "kai-trading-core:/market/live-sector-projection",
        "symbol": "005930",
        "market": "KOSPI",
        "market_date": "2026-11-05",
        "source_market_date": "2026-11-04",
        "observed_at": "2026-11-05T00:59:40+00:00",
        "sector_name": "전기전자",
        "relative_return_pct": "1.2",
    }
    return {
        "sector_context_status": "ready",
        "sector_context": sector,
        "sector_identity": {
            "sector_name": "전기전자",
            "market": "KOSPI",
            "source_market_date": "2026-11-04",
        },
        "market_regime_context": {"sector_context": sector},
    }


def test_warm_final_market_regime_sector_context_preloads_candidate_and_account(
    monkeypatch,
) -> None:
    breadth_calls: list[dict[str, object]] = []
    candidate_calls: list[dict[str, object]] = []
    account_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        broker_orders,
        "load_live_market_regime_context",
        lambda **kwargs: breadth_calls.append(kwargs) or _live_breadth(),
    )
    monkeypatch.setattr(
        broker_orders,
        "load_live_sector_regime_context",
        lambda **kwargs: candidate_calls.append(kwargs) or _live_sector(),
    )
    monkeypatch.setattr(
        broker_orders,
        "enrich_account_context_with_live_sector_identity",
        lambda **kwargs: account_calls.append(kwargs) or kwargs["account_context"],
    )

    broker_orders._warm_final_market_regime_sector_context(
        settings=_settings(),
        command=_command(),
        plan_item=_plan_item(),
        snapshot=_snapshot(),
        snapshot_error=None,
        evaluated_at=CHECKED_AT,
    )

    assert len(breadth_calls) == 1
    assert breadth_calls[0]["now"] == CHECKED_AT
    assert breadth_calls[0]["settings"].runtime_mode == "live"
    assert candidate_calls[0]["symbol"] == "005930"
    assert candidate_calls[0]["source_market_date"] == SOURCE_MARKET_DATE
    assert account_calls[0]["cached_only"] is False
    assert account_calls[0]["account_context"]["holdings_complete"] is True
    assert account_calls[0]["account_context"]["open_orders_complete"] is True


def test_final_market_regime_boundary_uses_current_sector_and_account_mapping(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_market_regime_context",
        lambda **_kwargs: _live_breadth(),
    )
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_sector_regime_context",
        lambda **_kwargs: _live_sector(),
    )
    monkeypatch.setattr(
        broker_orders,
        "enrich_account_context_with_live_sector_identity",
        lambda **kwargs: {
            **kwargs["account_context"],
            "sector_mapping_complete": True,
            "sector_mapping_status": "ready",
        },
    )

    def evaluate(**kwargs):
        captured.update(kwargs)
        return MarketRegimeGuardrailResult(
            action="allow",
            posture="normal",
            reason_code="normal_market_regime",
            multiplier=Decimal("1"),
            cap_notional=None,
            audit={"adjusted_quantity": 3},
        )

    monkeypatch.setattr(broker_orders, "evaluate_market_regime_guardrail", evaluate)

    failure = broker_orders._evaluate_final_market_regime_boundary(
        settings=_settings(),
        command=_command(),
        plan_item=_plan_item(),
        market_evidence=None,
        snapshot=_snapshot(),
        snapshot_error=None,
        evaluated_at=CHECKED_AT,
    )

    assert failure is None
    assert captured["context"]["sector_context"]["sector_name"] == "전기전자"
    assert captured["account_context"]["sector_mapping_complete"] is True


def test_final_market_regime_boundary_rejects_ready_but_stale_sector(
    monkeypatch,
) -> None:
    stale_sector = _live_sector()
    stale_sector["sector_context"] = {
        **stale_sector["sector_context"],
        "observed_at": "2026-11-04T00:59:40+00:00",
        "market_date": "2026-11-04",
    }
    stale_sector["market_regime_context"] = {
        "sector_context": stale_sector["sector_context"]
    }
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_market_regime_context",
        lambda **_kwargs: _live_breadth(),
    )
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_sector_regime_context",
        lambda **_kwargs: stale_sector,
    )

    failure = broker_orders._evaluate_final_market_regime_boundary(
        settings=_settings(),
        command=_command(),
        plan_item=_plan_item(),
        market_evidence=None,
        snapshot=_snapshot(),
        snapshot_error=None,
        evaluated_at=CHECKED_AT,
    )

    assert failure is not None
    assert failure[0] == "market_regime_final_evidence_unavailable"


def test_final_market_regime_boundary_retries_when_live_sector_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_market_regime_context",
        lambda **_kwargs: _live_breadth(),
    )
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_sector_regime_context",
        lambda **_kwargs: {
            "sector_context_status": "unavailable",
            "sector_context_reason": "projection_missing",
            "market_regime_context": {},
        },
    )
    monkeypatch.setattr(
        broker_orders,
        "enrich_account_context_with_live_sector_identity",
        lambda **kwargs: {
            **kwargs["account_context"],
            "sector_mapping_complete": False,
            "sector_mapping_status": "incomplete",
        },
    )

    failure = broker_orders._evaluate_final_market_regime_boundary(
        settings=_settings(),
        command=_command(),
        plan_item=_plan_item(include_stale_sector=True),
        market_evidence=None,
        snapshot=_snapshot(),
        snapshot_error=None,
        evaluated_at=CHECKED_AT,
    )

    assert failure is not None
    assert failure[0] == "market_regime_final_evidence_unavailable"
    assert failure[2]["sector_context_status"] == "unavailable"


def test_final_market_regime_boundary_retries_when_account_sector_mapping_is_incomplete(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_market_regime_context",
        lambda **_kwargs: _live_breadth(),
    )
    monkeypatch.setattr(
        broker_orders,
        "load_cached_live_sector_regime_context",
        lambda **_kwargs: _live_sector(),
    )
    monkeypatch.setattr(
        broker_orders,
        "enrich_account_context_with_live_sector_identity",
        lambda **kwargs: {
            **kwargs["account_context"],
            "sector_mapping_complete": False,
            "sector_mapping_status": "incomplete",
            "sector_mapping_missing_symbols": ["000660"],
        },
    )

    failure = broker_orders._evaluate_final_market_regime_boundary(
        settings=_settings(),
        command=_command(),
        plan_item=_plan_item(),
        market_evidence=None,
        snapshot=_snapshot(),
        snapshot_error=None,
        evaluated_at=CHECKED_AT,
    )

    assert failure is not None
    assert failure[0] == "market_regime_final_evidence_unavailable"
    assert failure[2]["sector_mapping_status"] == "incomplete"
    assert failure[2]["sector_mapping_missing_symbols"] == ["000660"]
