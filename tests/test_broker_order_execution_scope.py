from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.api.schemas.execution import ExecutionPlanItemRequest, PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerLiveMarketContext,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder
from app.services import broker_orders as broker_orders_service
from app.services.position_identity import (
    build_position_identity,
    execution_scope_payload,
)


def _command(**overrides: object) -> PlaceOrderRequest:
    values: dict[str, object] = {
        "idempotency_key": "execution-scope-order-1",
        "provider": "kis",
        "venue": "kis",
        "asset_class": "stock",
        "symbol": "005930",
        "side": "buy",
        "quantity": 1,
        "order_type": "limit",
        "time_in_force": "day",
        "exchange": "KRX",
        "limit_price": "71000",
    }
    values.update(overrides)
    return PlaceOrderRequest(**values)  # type: ignore[arg-type]


def _plan_item(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": "plan-item-authority-1",
        "provider": "kis",
        "venue": "kis",
        "asset_class": "us_equity",
        "symbol": "AAPL",
        "side": "buy",
        "intent": "next_open_buy",
        "quantity_policy": {"kind": "shares", "quantity": 1},
        "limit_price": Decimal("100"),
        "trigger_price": None,
        "time_in_force": "day",
        "base_quantity": None,
        "quote_notional": None,
        "quantity_precision": None,
        "price_precision": None,
        "risk_context": {"exchange": "NASD"},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"exchange": "NYSE"}, "exchange"),
        ({"quantity": 7}, "quantity"),
        ({"limit_price": Decimal("150")}, "limit_price"),
        ({"quantity_precision": 4}, "precision"),
        ({"price_precision": 2}, "precision"),
    ],
)
def test_direct_plan_linked_order_must_match_durable_plan_authority(
    override: dict[str, object],
    message: str,
) -> None:
    command_values: dict[str, object] = {
        "execution_plan_item_id": "plan-item-authority-1",
        "venue": "kis",
        "asset_class": "us_equity",
        "exchange": "NASD",
        "symbol": "AAPL",
        "quantity": 1,
        "limit_price": Decimal("100"),
    }
    command_values.update(override)
    command = _command(**command_values)

    error = broker_orders_service._validate_plan_item(
        plan_item=_plan_item(),
        command=command,
    )

    assert error is not None
    assert message in error


@pytest.mark.parametrize(
    "quantity_policy",
    [None, {}, {"kind": "shares"}, {"kind": "shares", "quantity": "invalid"}],
)
def test_plan_linked_order_without_durable_sizing_authority_fails_closed(
    quantity_policy: object,
) -> None:
    item = _plan_item(quantity_policy=quantity_policy)
    command = _command(
        execution_plan_item_id=item.id,
        venue="kis",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        quantity=1,
        limit_price=Decimal("100"),
    )

    error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=command,
    )

    assert error is not None and "sizing authority" in error


def test_invalid_fixed_policy_cannot_fall_back_to_fractional_sizing_fields() -> None:
    item = _plan_item(
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        quantity_policy={"kind": "shares", "quantity": "invalid"},
        base_quantity=Decimal("1"),
        risk_context={"exchange": "US"},
    )
    command = _command(
        execution_plan_item_id=item.id,
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        exchange="US",
        symbol="AAPL",
        quantity=None,
        base_quantity=Decimal("1"),
        limit_price=Decimal("100"),
    )

    error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=command,
    )

    assert error is not None and "sizing authority" in error


def test_trusted_monitoring_can_submit_bounded_partial_sell_with_fresh_price() -> None:
    item = _plan_item(
        side="sell",
        intent="stop_sell",
        quantity_policy={"kind": "shares", "quantity": 10},
        limit_price=Decimal("90"),
        trigger_price=Decimal("90"),
    )
    command = _command(
        execution_plan_item_id=item.id,
        venue="kis",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        side="sell",
        quantity=3,
        limit_price=Decimal("89.50"),
        metadata={"order_purpose": "protective_stop"},
    )
    item.risk_context["monitoring_order_authority"] = {
        "policy_version": 1,
        "source": "monitoring:intraday_target_stop_monitor",
        "command": command.model_dump(mode="json"),
    }

    assert broker_orders_service._validate_plan_item(
        plan_item=item,
        command=command,
        request_source="monitoring:intraday_target_stop_monitor",
    ) is None


def test_monitoring_source_without_persisted_command_authority_cannot_transform_order() -> None:
    item = _plan_item(
        side="sell",
        intent="stop_sell",
        quantity_policy={"kind": "shares", "quantity": 10},
        limit_price=Decimal("90"),
        trigger_price=Decimal("90"),
    )
    command = _command(
        execution_plan_item_id=item.id,
        venue="kis",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        side="sell",
        quantity=3,
        limit_price=Decimal("89.50"),
        metadata={"order_purpose": "protective_stop"},
    )

    error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=command,
        request_source="monitoring:intraday_target_stop_monitor",
    )

    assert error is not None
    assert "quantity" in error


def test_krx_single_price_protective_sell_binds_fresh_executable_bid() -> None:
    observed_at = datetime(2026, 9, 10, 7, 0, tzinfo=UTC)
    logical = _command(
        execution_plan_item_id="single-price-protective-sell",
        provider="kis",
        venue="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="sell",
        quantity=5,
        order_type="market",
        limit_price=None,
        metadata={
            "order_purpose": "protective_stop",
            "route": {
                "action": "submit",
                "exchange": "KRX",
                "session": "krx_after_single_price",
                "ord_dvsn": "07",
                "effective_order_type": "limit",
                "price_source": "single_price_session",
                "next_retry_at": None,
                "reason": "krx_after_hours_single_price_enabled",
            },
        },
    )
    routed = logical.model_copy(update={"order_type": "limit"})
    evidence = broker_orders_service.AutomatedKisSellMarketEvidence(
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=observed_at,
            bid=Decimal("1097"),
            ask=Decimal("1098"),
            normal_ordering_available=True,
            payload={},
            freshness_source="kis_exchange_orderbook_time",
        ),
        is_domestic=True,
    )

    bound = broker_orders_service._bind_automated_kis_sell_limit(
        command=routed,
        evidence=evidence,
    )

    assert bound.order_type == "limit"
    assert bound.limit_price == Decimal("1097")
    assert bound.metadata["route"]["ord_dvsn"] == "07"
    assert bound.metadata["final_exit_price_bound"] == {
        "source": "fresh_executable_bid",
        "limit_price": "1097",
        "time_in_force": "day",
    }


@pytest.mark.parametrize(
    ("route_field", "forged_value"),
    [
        ("action", "defer"),
        ("exchange", "SOR"),
        ("session", "krx_after_close"),
        ("ord_dvsn", "06"),
        ("effective_order_type", "market"),
        ("price_source", "market"),
        ("reason", "caller_supplied"),
    ],
)
def test_forged_domestic_route_cannot_bind_executable_bid(
    route_field: str,
    forged_value: str,
) -> None:
    route = {
        "action": "submit",
        "exchange": "KRX",
        "session": "krx_after_single_price",
        "ord_dvsn": "07",
        "effective_order_type": "limit",
        "price_source": "single_price_session",
        "next_retry_at": None,
        "reason": "krx_after_hours_single_price_enabled",
    }
    route[route_field] = forged_value
    command = _command(
        execution_plan_item_id="forged-single-price-route",
        provider="kis",
        venue="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="sell",
        quantity=5,
        order_type="market",
        limit_price=None,
        metadata={"order_purpose": "protective_stop", "route": route},
    ).model_copy(update={"order_type": "limit"})
    evidence = broker_orders_service.AutomatedKisSellMarketEvidence(
        context=BrokerLiveMarketContext(
            symbol="005930",
            exchange="KRX",
            observed_at=datetime(2026, 9, 10, 7, 0, tzinfo=UTC),
            bid=Decimal("1097"),
            normal_ordering_available=True,
        ),
        is_domestic=True,
    )

    bound = broker_orders_service._bind_automated_kis_sell_limit(
        command=command,
        evidence=evidence,
    )

    assert bound.limit_price is None


def test_fresh_monitoring_sell_retrigger_requires_exact_command_authority() -> None:
    item = _plan_item(
        id="fresh-protective-retrigger",
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol="005930",
        side="sell",
        intent="stop_sell",
        quantity_policy={"kind": "shares", "quantity": 5},
        limit_price=None,
        trigger_price=Decimal("100"),
        risk_context={"exchange": "KRX"},
    )
    command = _command(
        execution_plan_item_id=item.id,
        provider="kis",
        venue="kis",
        asset_class="stock",
        exchange="KRX",
        symbol="005930",
        side="sell",
        quantity=5,
        order_type="market",
        limit_price=None,
        metadata={
            "monitoring_worker": "intraday_target_stop_monitor",
            "order_purpose": "protective_stop",
            "observed_price": "99.0000",
            "route_now": "2026-09-10T01:00:00+00:00",
            "monitoring_sell_trigger_binding": {
                "version": 1,
                "execution_plan_item_id": item.id,
                "symbol": "005930",
                "intent": "stop_sell",
                "trigger_reason": "stop_sell",
                "direction": "at_or_below",
                "trigger_price": "100.0000",
            },
        },
    )
    item.risk_context["monitoring_order_authority"] = {
        "policy_version": 1,
        "source": "monitoring:intraday_target_stop_monitor",
        "command": command.model_dump(mode="json"),
    }

    assert broker_orders_service._fresh_monitoring_sell_retrigger_is_authorized(
        plan_item=item,
        command=command,
        request_source="monitoring:intraday_target_stop_monitor",
    )
    forged_binding = dict(command.metadata or {})
    forged_binding["monitoring_sell_trigger_binding"] = {
        **forged_binding["monitoring_sell_trigger_binding"],
        "trigger_price": "102.0000",
    }
    assert not broker_orders_service._fresh_monitoring_sell_retrigger_is_authorized(
        plan_item=item,
        command=command.model_copy(update={"metadata": forged_binding}),
        request_source="monitoring:intraday_target_stop_monitor",
    )
    assert not broker_orders_service._fresh_monitoring_sell_retrigger_is_authorized(
        plan_item=item,
        command=command.model_copy(update={"quantity": 4}),
        request_source="monitoring:intraday_target_stop_monitor",
    )


@pytest.mark.parametrize("plan_item", [None, _plan_item()])
@pytest.mark.parametrize(
    "caller_purpose",
    ["risk_reduction", "reduce_risk", "risk_reducing", "protective_hedge"],
)
def test_caller_metadata_cannot_reclassify_buy_as_risk_reduction(
    plan_item: SimpleNamespace | None,
    caller_purpose: str,
) -> None:
    command = _command(
        execution_plan_item_id=(plan_item.id if plan_item is not None else None),
        venue="kis",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        quantity=1,
        limit_price=Decimal("100"),
        metadata={"order_purpose": caller_purpose},
    )

    assert broker_orders_service._is_new_buy_order(
        command=command,
        plan_item=plan_item,
    ) is True


@pytest.mark.parametrize(
    "plan_intent",
    [
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
        "stop_sell",
        "target_sell",
        "max_holding_close_sell",
    ],
)
def test_durable_plan_intent_cannot_exempt_a_buy_from_risk_gates(
    plan_intent: str,
) -> None:
    command = _command(execution_plan_item_id="plan-item-authority-1")

    assert broker_orders_service._is_new_buy_order(
        command=command,
        plan_item=_plan_item(intent=plan_intent),
    ) is True


@pytest.mark.parametrize(
    "plan_intent",
    [
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
        "stop_sell",
        "target_sell",
        "max_holding_close_sell",
    ],
)
def test_execution_plan_schema_rejects_buy_with_sell_or_risk_reduction_intent(
    plan_intent: str,
) -> None:
    with pytest.raises(ValueError, match="BUY intent cannot claim risk reduction"):
        ExecutionPlanItemRequest(
            symbol="005930",
            side="buy",
            intent=plan_intent,
            quantity_policy={"kind": "shares", "quantity": 1},
        )


@pytest.mark.parametrize(
    ("plan_item", "expected_purpose"),
    [
        (None, "manual_order"),
        (_plan_item(), "next_open_buy"),
    ],
)
def test_caller_metadata_cannot_change_buy_route_purpose(
    plan_item: SimpleNamespace | None,
    expected_purpose: str,
) -> None:
    command = _command(
        execution_plan_item_id=(plan_item.id if plan_item is not None else None),
        metadata={"order_purpose": "risk_reduction"},
    )

    assert broker_orders_service._order_purpose(
        command=command,
        plan_item=plan_item,
        metadata=dict(command.metadata or {}),
    ) == expected_purpose


def test_direct_plan_linked_order_rejects_intent_and_tif_drift() -> None:
    item = _plan_item(
        asset_class="stock",
        symbol="005930",
        limit_price=Decimal("71000"),
        risk_context={"exchange": "KRX"},
    )
    intent_drift = _command(
        execution_plan_item_id=item.id,
        metadata={"order_purpose": "protective_hedge"},
    )
    tif_drift = _command(
        execution_plan_item_id=item.id,
        time_in_force="ioc",
    )

    intent_error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=intent_drift,
    )
    tif_error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=tif_drift,
    )

    assert intent_error is not None and "intent" in intent_error
    assert tif_error is not None and "time_in_force" in tif_error


@pytest.mark.parametrize(
    ("command_override", "message"),
    [
        ({"base_quantity": Decimal("2")}, "fractional sizing"),
        ({"quantity_precision": 4}, "precision"),
        ({"price_precision": 6}, "precision"),
    ],
)
def test_plan_linked_fractional_order_must_match_persisted_sizing_authority(
    command_override: dict[str, object],
    message: str,
) -> None:
    item = _plan_item(
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        symbol="BTC/USD",
        quantity_policy={"kind": "base_quantity", "quantity": "1.25"},
        limit_price=Decimal("50000"),
        time_in_force="gtc",
        base_quantity=Decimal("1.25"),
        quantity_precision=8,
        price_precision=2,
        risk_context={"exchange": "ALPACA"},
    )
    values: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "provider": "alpaca",
        "venue": "alpaca",
        "asset_class": "crypto",
        "exchange": "ALPACA",
        "symbol": "BTC/USD",
        "side": "buy",
        "quantity": None,
        "base_quantity": Decimal("1.25"),
        "quantity_precision": 8,
        "price_precision": 2,
        "limit_price": Decimal("50000"),
        "time_in_force": "gtc",
    }
    values.update(command_override)
    command = _command(**values)

    error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=command,
    )

    assert error is not None and message in error


@pytest.mark.parametrize(
    ("plan_override", "command_override", "message"),
    [
        ({"venue": None}, {"venue": "kis-us"}, "venue"),
        (
            {
                "asset_class": None,
                "symbol": "AAPL",
                "risk_context": {"exchange": "NASD"},
            },
            {"asset_class": "us_equity", "symbol": "AAPL", "exchange": "NASD"},
            "asset_class",
        ),
        ({"risk_context": {}}, {"exchange": "NXT"}, "exchange"),
    ],
)
def test_legacy_plan_defaults_cannot_authorize_non_default_route_drift(
    plan_override: dict[str, object],
    command_override: dict[str, object],
    message: str,
) -> None:
    plan_values: dict[str, object] = {
        "asset_class": "stock",
        "symbol": "005930",
        "limit_price": Decimal("71000"),
        "risk_context": {"exchange": "KRX"},
    }
    plan_values.update(plan_override)
    item = _plan_item(**plan_values)
    command_values: dict[str, object] = {
        "execution_plan_item_id": item.id,
        "venue": "kis",
    }
    command_values.update(command_override)

    error = broker_orders_service._validate_plan_item(
        plan_item=item,
        command=_command(**command_values),
    )

    assert error is not None and message in error


def test_kis_execution_scope_uses_configured_account_and_routed_order_fields() -> None:
    settings = Settings(
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret=(
            "stable-position-scope-secret-32-bytes"
        ),
    )
    adapter = SimpleNamespace(provider="kis")
    command = _command(venue="kis-us", asset_class="us_equity", exchange="NASD")

    scope = broker_orders_service._canonical_execution_scope(
        settings=settings,
        adapter=adapter,
        access_token="must-not-affect-scope",
        command=command,
    )

    assert scope == execution_scope_payload(
        identity=build_position_identity(
            settings=settings,
            provider="kis",
            exchange="NASD",
        ),
        venue="kis-us",
        asset_class="us_equity",
    )
    assert "12345678" not in str(scope)
    assert "must-not-affect-scope" not in str(scope)


def test_order_metadata_preserves_exact_exchange_for_position_consumers() -> None:
    metadata = broker_orders_service._merge_order_request_metadata(
        base=None,
        command=_command(venue="kis-us", asset_class="us_equity", exchange="nasd"),
        request_source="test",
    )

    assert metadata["exchange"] == "NASD"


def test_scope_binding_rejects_persisted_exchange_conflict() -> None:
    class RecordingSession:
        def add(self, _value: object) -> None:
            raise AssertionError("conflicting exchange must not be written")

    order = BrokerOrder(
        broker_name="kis",
        mode="paper",
        idempotency_key="persisted-exchange-conflict",
        symbol="AAPL",
        side="sell",
        status="submission_pending",
        asset_class="us_equity",
        venue="kis-us",
        request_metadata={"exchange": "NYSE"},
    )
    command = _command(
        venue="kis-us",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
        side="sell",
    )

    with pytest.raises(BrokerAdapterError) as raised:
        broker_orders_service._bind_canonical_execution_scope(
            settings=Settings(runtime_mode="paper"),
            db=RecordingSession(),  # type: ignore[arg-type]
            adapter=SimpleNamespace(provider="kis"),
            access_token="validated",
            order=order,
            command=command,
        )

    assert raised.value.code == "execution_scope_conflict"


def test_alpaca_execution_scope_uses_stable_account_id_across_credential_rotation() -> None:
    class AlpacaIdentityAdapter:
        provider = "alpaca"

        def __init__(self) -> None:
            self.access_tokens: list[str] = []

        def get_broker_account_id(self, access_token: str) -> str:
            self.access_tokens.append(access_token)
            return "stable-alpaca-account-id"

    adapter = AlpacaIdentityAdapter()
    command = _command(
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        exchange="US",
        symbol="SPY",
        quantity=None,
        quote_notional="50",
        order_type="market",
        limit_price=None,
    )

    first_scope = broker_orders_service._canonical_execution_scope(
        settings=Settings(
            runtime_mode="live",
            alpaca_api_key="rotated-key-a",
            alpaca_api_secret="rotated-secret-a",
            account_drawdown_recovery_scope_secret=(
                "stable-position-scope-secret-32-bytes"
            ),
        ),
        adapter=adapter,
        access_token="validated-a",
        command=command,
    )
    second_scope = broker_orders_service._canonical_execution_scope(
        settings=Settings(
            runtime_mode="live",
            alpaca_api_key="rotated-key-b",
            alpaca_api_secret="rotated-secret-b",
            account_drawdown_recovery_scope_secret=(
                "stable-position-scope-secret-32-bytes"
            ),
        ),
        adapter=adapter,
        access_token="validated-b",
        command=command,
    )

    assert first_scope == second_scope
    assert first_scope["provider"] == "alpaca"
    assert first_scope["venue"] == "ALPACA"
    assert first_scope["asset_class"] == "us_equity"
    assert adapter.access_tokens == ["validated-a", "validated-b"]
    assert "stable-alpaca-account-id" not in str(first_scope)
    assert "rotated" not in str(first_scope)


def test_alpaca_live_execution_scope_rejects_malformed_account_id() -> None:
    adapter = SimpleNamespace(
        provider="alpaca",
        get_broker_account_id=lambda _access_token: {"unexpected": "mapping"},
    )
    command = _command(
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
        symbol="BTC/USD",
        quantity=None,
        quote_notional="50",
        order_type="market",
        time_in_force="gtc",
        limit_price=None,
    )

    with pytest.raises(BrokerAdapterError) as raised:
        broker_orders_service._canonical_execution_scope(
            settings=Settings(runtime_mode="live"),
            adapter=adapter,
            access_token="validated",
            command=command,
        )

    assert raised.value.code == "execution_scope_unavailable"


@pytest.mark.parametrize(
    "account_id_loader",
    [
        lambda _access_token: "",
        lambda _access_token: (_ for _ in ()).throw(TimeoutError("unavailable")),
    ],
    ids=("missing", "lookup-error"),
)
def test_alpaca_authenticated_account_lookup_never_falls_back_to_label(
    account_id_loader: object,
) -> None:
    adapter = SimpleNamespace(
        provider="alpaca",
        get_broker_account_id=account_id_loader,
    )

    resolved, account_id = broker_orders_service._load_authenticated_broker_account_id(
        adapter=adapter,
        access_token="validated",
    )

    assert resolved is False
    assert account_id is None


def test_alpaca_canonical_scope_requires_authenticated_account_id_even_with_label() -> None:
    command = _command(
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        exchange="US",
        symbol="SPY",
        quantity=None,
        quote_notional="50",
        order_type="market",
        limit_price=None,
    )

    with pytest.raises(BrokerAdapterError) as raised:
        broker_orders_service._canonical_execution_scope(
            settings=Settings(
                runtime_mode="live",
                execution_position_account_scope="operator-label-is-not-account-truth",
                account_drawdown_recovery_scope_secret=(
                    "stable-position-scope-secret-32-bytes"
                ),
            ),
            adapter=SimpleNamespace(provider="alpaca"),
            access_token="validated",
            command=command,
            broker_account_id=None,
            broker_account_id_resolved=True,
        )

    assert raised.value.code == "execution_scope_unavailable"


def _scoped_order(
    *,
    provider: str,
    account_scope: str,
    venue: str,
    asset_class: str,
    exchange: str,
    symbol: str,
) -> BrokerOrder:
    scope = {
        "policy_version": 1,
        "provider": provider,
        "account_scope": account_scope,
        "venue": venue.upper(),
        "asset_class": asset_class,
        "exchange": exchange.upper(),
    }
    return BrokerOrder(
        broker_name=provider,
        mode="paper",
        idempotency_key=f"{provider}-{symbol}-{exchange}",
        symbol=symbol,
        side="buy",
        status="submission_pending",
        venue=venue,
        asset_class=asset_class,
        request_metadata={"exchange": exchange.upper(), "execution_scope": scope},
    )


@pytest.mark.parametrize(
    ("provider", "candidate_scope", "existing_order"),
    [
        (
            "alpaca",
            {
                "policy_version": 1,
                "provider": "alpaca",
                "account_scope": "a" * 24,
                "venue": "ALPACA",
                "asset_class": "crypto",
                "exchange": "ALPACA",
            },
            _scoped_order(
                provider="alpaca",
                account_scope="a" * 24,
                venue="alpaca",
                asset_class="us_equity",
                exchange="US",
                symbol="SPY",
            ),
        ),
        (
            "kis",
            {
                "policy_version": 1,
                "provider": "kis",
                "account_scope": "b" * 24,
                "venue": "KIS-US",
                "asset_class": "us_equity",
                "exchange": "NYSE",
            },
            _scoped_order(
                provider="kis",
                account_scope="b" * 24,
                venue="kis-us",
                asset_class="us_equity",
                exchange="NASD",
                symbol="AAPL",
            ),
        ),
    ],
    ids=("alpaca-cross-asset", "kis-us-cross-exchange"),
)
def test_buy_capital_scope_includes_same_account_same_currency_routes(
    provider: str,
    candidate_scope: dict[str, object],
    existing_order: BrokerOrder,
) -> None:
    selection = broker_orders_service._select_local_orders_for_execution_scope(
        orders=[existing_order],
        execution_scope=candidate_scope,
        settlement_currency="USD",
    )

    assert selection.indeterminate_order is None
    assert selection.matching_orders == (existing_order,)


def test_sell_inventory_scope_remains_exact_route_only() -> None:
    existing_order = _scoped_order(
        provider="kis",
        account_scope="b" * 24,
        venue="kis-us",
        asset_class="us_equity",
        exchange="NASD",
        symbol="AAPL",
    )
    candidate_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": "b" * 24,
        "venue": "KIS-US",
        "asset_class": "us_equity",
        "exchange": "NYSE",
    }

    selection = broker_orders_service._select_local_orders_for_execution_scope(
        orders=[existing_order],
        execution_scope=candidate_scope,
    )

    assert selection.indeterminate_order is None
    assert selection.matching_orders == ()


@pytest.mark.parametrize(
    "snapshot_completeness",
    [
        {"open_orders_complete": False},
        {"holdings_complete": False},
    ],
)
def test_alpaca_sell_fails_closed_when_snapshot_truth_is_incomplete(
    snapshot_completeness: dict[str, bool],
) -> None:
    class EmptyScalarResult:
        def all(self) -> list[BrokerOrder]:
            return []

    class EmptySession:
        def scalars(self, _query: object) -> EmptyScalarResult:
            return EmptyScalarResult()

    snapshot = BrokerAccountSnapshot(
        broker_account_id="alpaca-account-a",
        observed_at=datetime.now(UTC),
        holdings=[
            BrokerAccountHolding(
                symbol="AAPL",
                quantity=1,
                payload={"base_quantity": "1"},
            )
        ],
        open_orders=[],
        payload={},
        **snapshot_completeness,
    )
    adapter = SimpleNamespace(
        provider="alpaca",
        get_account_snapshot=lambda *_args, **_kwargs: snapshot,
    )
    scope = {
        "policy_version": 1,
        "provider": "alpaca",
        "account_scope": "a" * 24,
        "venue": "ALPACA",
        "asset_class": "us_equity",
        "exchange": "US",
    }

    result = broker_orders_service._evaluate_alpaca_sell_reservation_gate(
        settings=Settings(runtime_mode="paper"),
        db=EmptySession(),  # type: ignore[arg-type]
        adapter=adapter,
        command=_command(
            provider="alpaca",
            venue="alpaca",
            asset_class="us_equity",
            exchange="US",
            symbol="AAPL",
            side="sell",
            quantity=None,
            base_quantity=Decimal("1"),
            limit_price=Decimal("100"),
        ),
        execution_scope=scope,
        session_state=SimpleNamespace(access_token="validated"),
        session_error=None,
    )

    assert result is not None
    assert result.status_code == 409
    assert result.payload["block_reason"] == "sell_position_truth_unavailable"


def test_alpaca_sell_does_not_retry_unscoped_after_internal_type_error() -> None:
    class EmptyScalarResult:
        def all(self) -> list[BrokerOrder]:
            return []

    class EmptySession:
        def scalars(self, _query: object) -> EmptyScalarResult:
            return EmptyScalarResult()

    calls: list[dict[str, object]] = []

    def get_account_snapshot(
        _access_token: str,
        **kwargs: object,
    ) -> BrokerAccountSnapshot:
        calls.append(dict(kwargs))
        if kwargs:
            raise TypeError("internal US snapshot parser failure")
        return BrokerAccountSnapshot(
            broker_account_id="wrong-default-route",
            observed_at=datetime.now(UTC),
        )

    result = broker_orders_service._evaluate_alpaca_sell_reservation_gate(
        settings=Settings(runtime_mode="paper"),
        db=EmptySession(),  # type: ignore[arg-type]
        adapter=SimpleNamespace(
            provider="alpaca",
            get_account_snapshot=get_account_snapshot,
        ),
        command=_command(
            provider="alpaca",
            venue="alpaca",
            asset_class="us_equity",
            exchange="US",
            symbol="AAPL",
            side="sell",
            quantity=None,
            base_quantity=Decimal("1"),
            limit_price=Decimal("100"),
        ),
        execution_scope={
            "policy_version": 1,
            "provider": "alpaca",
            "account_scope": "a" * 24,
            "venue": "ALPACA",
            "asset_class": "us_equity",
            "exchange": "US",
        },
        session_state=SimpleNamespace(access_token="validated"),
        session_error=None,
    )

    assert result is not None
    assert result.payload["block_reason"] == "sell_position_truth_unavailable"
    assert calls == [{"symbols": ["AAPL"], "exchange": "US"}]


@pytest.mark.parametrize(
    ("snapshot", "blocked"),
    [
        (
            BrokerAccountSnapshot(
                holdings=[
                    BrokerAccountHolding(
                        symbol="005930", quantity=1, sellable_quantity=1
                    ),
                    BrokerAccountHolding(
                        symbol="005930", quantity=1, sellable_quantity=1
                    ),
                ],
            ),
            True,
        ),
        (
            BrokerAccountSnapshot(
                holdings=[BrokerAccountHolding(symbol="005930", quantity=1)],
                open_orders_complete=False,
            ),
            True,
        ),
        (
            BrokerAccountSnapshot(
                holdings=[
                    BrokerAccountHolding(
                        symbol="005930", quantity=1, sellable_quantity=1
                    )
                ],
                holdings_complete=False,
                open_orders_complete=False,
            ),
            False,
        ),
    ],
)
def test_kis_sell_uses_one_exact_holding_and_complete_reservation_truth(
    snapshot: BrokerAccountSnapshot,
    blocked: bool,
) -> None:
    class EmptyScalarResult:
        def all(self) -> list[BrokerOrder]:
            return []

    class EmptySession:
        def scalars(self, _query: object) -> EmptyScalarResult:
            return EmptyScalarResult()

        def get(self, _model: object, _key: object) -> None:
            return None

    result = broker_orders_service._evaluate_sell_reservation_gate(
        settings=Settings(runtime_mode="live"),
        db=EmptySession(),  # type: ignore[arg-type]
        adapter=SimpleNamespace(
            provider="kis",
            get_account_snapshot=lambda *_args, **_kwargs: snapshot,
        ),
        command=_command(side="sell", quantity=1),
        request_source="test",
        execution_scope={
            "policy_version": 1,
            "provider": "kis",
            "account_scope": "a" * 24,
            "venue": "KIS",
            "asset_class": "stock",
            "exchange": "KRX",
        },
        session_state=SimpleNamespace(access_token="validated"),
        session_error=None,
    )

    assert (result is not None) is blocked
    if result is not None:
        assert result.payload["block_reason"] == "sell_position_truth_unavailable"


def test_kis_sell_does_not_retry_default_route_after_internal_type_error() -> None:
    class EmptyScalarResult:
        def all(self) -> list[BrokerOrder]:
            return []

    class EmptySession:
        def scalars(self, _query: object) -> EmptyScalarResult:
            return EmptyScalarResult()

    calls: list[dict[str, object]] = []

    def get_account_snapshot(
        _access_token: str,
        **kwargs: object,
    ) -> BrokerAccountSnapshot:
        calls.append(dict(kwargs))
        if kwargs:
            raise TypeError("internal KIS snapshot parser failure")
        return BrokerAccountSnapshot(
            broker_account_id="wrong-default-route",
            observed_at=datetime.now(UTC),
        )

    result = broker_orders_service._evaluate_sell_reservation_gate(
        settings=Settings(runtime_mode="live"),
        db=EmptySession(),  # type: ignore[arg-type]
        adapter=SimpleNamespace(
            provider="kis",
            get_account_snapshot=get_account_snapshot,
        ),
        command=_command(
            side="sell",
            execution_plan_item_id=None,
        ),
        request_source="direct_api",
        execution_scope={
            "policy_version": 1,
            "provider": "kis",
            "account_scope": "a" * 24,
            "venue": "KIS",
            "asset_class": "stock",
            "exchange": "KRX",
        },
        session_state=SimpleNamespace(access_token="validated"),
        session_error=None,
    )

    assert result is not None
    assert result.payload["block_reason"] == "sell_position_truth_unavailable"
    assert calls == [{"symbols": ["005930"], "exchange": "KRX"}]


def test_new_buy_gate_does_not_retry_default_route_after_internal_type_error() -> None:
    calls: list[dict[str, object]] = []

    def get_account_snapshot(
        _access_token: str,
        **kwargs: object,
    ) -> BrokerAccountSnapshot:
        calls.append(dict(kwargs))
        if kwargs:
            raise TypeError("internal KIS account snapshot failure")
        return BrokerAccountSnapshot(
            broker_account_id="wrong-default-route",
            observed_at=datetime.now(UTC),
        )

    decision = broker_orders_service._evaluate_new_buy_account_gate(
        settings=Settings(runtime_mode="paper"),
        db=SimpleNamespace(),  # type: ignore[arg-type]
        adapter=SimpleNamespace(
            provider="kis",
            get_account_snapshot=get_account_snapshot,
        ),
        command=_command(),
        plan_item=None,
        session_state=SimpleNamespace(access_token="validated"),
        execution_scope=None,
    )

    assert decision is not None
    assert decision.allowed is False
    assert "snapshot unavailable" in decision.reason
    assert calls == [{"symbols": ["005930"], "exchange": "KRX"}]


def test_upbit_live_execution_scope_fails_closed_without_stable_scope() -> None:
    command = _command(
        provider="upbit",
        venue="upbit",
        asset_class="crypto",
        exchange="UPBIT",
        symbol="KRW-BTC",
        quantity=None,
        quote_notional="50000",
        order_type="market",
        limit_price=None,
    )

    with pytest.raises(BrokerAdapterError) as raised:
        broker_orders_service._canonical_execution_scope(
            settings=Settings(
                runtime_mode="live",
                upbit_access_key="credential-must-not-be-used-as-scope",
                upbit_secret_key="credential-must-not-be-used-as-scope",
            ),
            adapter=SimpleNamespace(provider="upbit"),
            access_token="validated",
            command=command,
        )

    assert raised.value.classification == BrokerErrorClassification.VALIDATION
    assert raised.value.code == "execution_scope_unavailable"
    assert raised.value.retryable is False
    assert "credential-must-not-be-used-as-scope" not in str(raised.value)


def test_paper_execution_scope_is_deterministic_across_credential_rotation() -> None:
    command = _command(
        provider="upbit",
        venue="upbit",
        asset_class="crypto",
        exchange="UPBIT",
        symbol="KRW-BTC",
        quantity=None,
        quote_notional="50000",
        order_type="market",
        limit_price=None,
    )
    adapter = SimpleNamespace(provider="upbit")

    first = broker_orders_service._canonical_execution_scope(
        settings=Settings(
            runtime_mode="paper",
            upbit_access_key="paper-key-a",
            upbit_secret_key="paper-secret-a",
        ),
        adapter=adapter,
        access_token="paper-token-a",
        command=command,
    )
    second = broker_orders_service._canonical_execution_scope(
        settings=Settings(
            runtime_mode="paper",
            upbit_access_key="paper-key-b",
            upbit_secret_key="paper-secret-b",
        ),
        adapter=adapter,
        access_token="paper-token-b",
        command=command,
    )

    assert first == second
    assert "paper-key" not in str(first)
    assert "paper-secret" not in str(first)
    assert "paper-token" not in str(first)


def test_broker_ack_metadata_cannot_override_canonical_execution_scope() -> None:
    canonical_scope = {
        "policy_version": 1,
        "provider": "kis",
        "account_scope": "a" * 24,
        "venue": "KIS",
        "asset_class": "stock",
        "exchange": "KRX",
    }
    broker_metadata = {
        "endpoint": "/broker/order",
        "execution_scope": {
            "policy_version": 1,
            "provider": "other",
            "account_scope": "b" * 24,
            "venue": "OTHER",
            "asset_class": "other",
            "exchange": "OTHER",
        },
    }

    merged = broker_orders_service._merge_order_request_metadata(
        base=broker_metadata,
        command=_command(),
        request_source="post:/orders",
        canonical_execution_scope=canonical_scope,
    )

    assert merged["execution_scope"] == canonical_scope
    assert merged["endpoint"] == "/broker/order"


def test_retry_metadata_replacement_preserves_canonical_execution_scope() -> None:
    canonical_scope = {
        "policy_version": 1,
        "provider": "alpaca",
        "account_scope": "c" * 24,
        "venue": "ALPACA",
        "asset_class": "crypto",
        "exchange": "ALPACA",
    }
    retry_order = BrokerOrder(
        broker_name="alpaca",
        mode="paper",
        idempotency_key="retry-scope-order",
        symbol="BTC/USD",
        side="buy",
        status="rejected",
        asset_class="crypto",
        venue="alpaca",
        request_metadata={"execution_scope": canonical_scope, "stale": True},
    )

    replacement = broker_orders_service._preserve_canonical_execution_scope(
        request_metadata={"source": "retry", "execution_scope": {"malformed": True}},
        existing_order=retry_order,
    )

    assert replacement == {
        "source": "retry",
        "execution_scope": canonical_scope,
    }


def test_retry_metadata_replacement_carries_malformed_scope_to_final_rejection() -> None:
    retry_order = BrokerOrder(
        broker_name="alpaca",
        mode="live",
        idempotency_key="retry-malformed-scope",
        symbol="BTC/USD",
        side="buy",
        status="rejected",
        asset_class="crypto",
        venue="alpaca",
        request_metadata={"execution_scope": {"account_scope": "not-canonical"}},
    )

    replacement = broker_orders_service._preserve_canonical_execution_scope(
        request_metadata={"source": "retry"},
        existing_order=retry_order,
    )

    assert replacement["execution_scope"] == {"account_scope": "not-canonical"}


def test_retry_scope_conflict_is_a_permanent_preplacement_rejection() -> None:
    class RecordingSession:
        def add(self, _value: object) -> None:
            raise AssertionError("conflicting scope must not be written")

    order = BrokerOrder(
        broker_name="upbit",
        mode="paper",
        idempotency_key="retry-scope-conflict",
        symbol="KRW-BTC",
        side="buy",
        status="submission_pending",
        asset_class="crypto",
        venue="upbit",
        request_metadata={
            "execution_scope": {
                "policy_version": 1,
                "provider": "upbit",
                "account_scope": "d" * 24,
                "venue": "UPBIT",
                "asset_class": "crypto",
                "exchange": "UPBIT",
            }
        },
    )
    command = _command(
        provider="upbit",
        venue="upbit",
        asset_class="crypto",
        exchange="UPBIT",
        symbol="KRW-BTC",
        quantity=None,
        quote_notional="50000",
        order_type="market",
        limit_price=None,
    )

    with pytest.raises(BrokerAdapterError) as raised:
        broker_orders_service._bind_canonical_execution_scope(
            settings=Settings(runtime_mode="paper"),
            db=RecordingSession(),  # type: ignore[arg-type]
            adapter=SimpleNamespace(provider="upbit"),
            access_token="validated",
            order=order,
            command=command,
        )

    assert raised.value.code == "execution_scope_conflict"
    assert raised.value.retryable is False


def test_live_kis_buy_fails_closed_on_typed_incomplete_open_order_snapshot() -> None:
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=datetime.now(UTC),
        open_orders_complete=False,
        payload={},
    )

    decision = broker_orders_service._buy_capital_reservation_decision(
        settings=Settings(runtime_mode="live", account_drawdown_gate_enabled=False),
        snapshot=snapshot,
        command=_command(quantity=1, limit_price=Decimal("71000")),
        plan_item=None,
        local_open_buy_notional=Decimal("0"),
        local_open_buy_unknown_count=0,
        observed_at=datetime.now(UTC),
    )

    assert decision is not None
    assert decision.gate_name == "account_drawdown_snapshot_unavailable"
    assert "open-order notional cannot be verified" in decision.reason


def test_recovery_uses_typed_open_order_completeness_as_canonical_evidence() -> None:
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=datetime.now(UTC),
        total_asset_value=Decimal("1000000"),
        previous_total_asset_value=Decimal("1000000"),
        asset_change_amount=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        open_orders_complete=False,
        payload={},
    )

    reason = broker_orders_service._account_drawdown_recovery_snapshot_incomplete(
        settings=Settings(runtime_mode="live"),
        snapshot=snapshot,
        command=_command(quantity=1, limit_price=Decimal("71000")),
    )

    assert reason == (
        "broker account snapshot lacks reliable open-order truth required for recovery"
    )


def test_live_buy_fails_closed_on_typed_incomplete_holdings_snapshot() -> None:
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=datetime.now(UTC),
        holdings_complete=False,
        payload={},
    )

    decision = broker_orders_service._buy_capital_reservation_decision(
        settings=Settings(runtime_mode="live", account_drawdown_gate_enabled=False),
        snapshot=snapshot,
        command=_command(quantity=1, limit_price=Decimal("71000")),
        plan_item=None,
        local_open_buy_notional=Decimal("0"),
        local_open_buy_unknown_count=0,
        observed_at=datetime.now(UTC),
    )

    assert decision is not None
    assert decision.gate_name == "account_drawdown_snapshot_unavailable"
    assert "holdings cannot be verified" in decision.reason


def test_recovery_requires_typed_complete_holdings_truth() -> None:
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000000"),
        observed_at=datetime.now(UTC),
        total_asset_value=Decimal("1000000"),
        previous_total_asset_value=Decimal("1000000"),
        asset_change_amount=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        holdings_complete=False,
        payload={},
    )

    reason = broker_orders_service._account_drawdown_recovery_snapshot_incomplete(
        settings=Settings(runtime_mode="live"),
        snapshot=snapshot,
        command=_command(quantity=1, limit_price=Decimal("71000")),
    )

    assert reason == (
        "broker account snapshot lacks complete holdings truth required for recovery"
    )
