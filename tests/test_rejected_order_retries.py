from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerConfig,
    BrokerReconnectHandle,
    BrokerTokenGrant,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    BrokerOrderEvent,
    ExecutionFailure,
    ExecutionJob,
    ExecutionPlan,
    ExecutionPlanItem,
)
from app.services import broker_orders as broker_orders_service
from app.services import execution_jobs as execution_jobs_service
from app.services import rejected_order_retries as rejected_order_retries_service
from app.services.execution_account_route import bind_kis_account_route
from app.services.position_identity import (
    build_authenticated_broker_position_identity,
    build_position_identity,
    execution_scope_payload,
)
from app.services.rejected_order_retries import (
    RetryOrderGuardError,
    build_rejected_order_retry_request,
)
from app.services.runtime_controls import ControlCommand, resume_runtime


OBSERVED_AT = datetime(2026, 9, 11, 14, 30, tzinfo=UTC)


def test_retry_snapshot_coverage_uses_typed_completeness_for_every_provider() -> None:
    with pytest.raises(Exception, match="retry_broker_open_orders_incomplete"):
        rejected_order_retries_service._validate_snapshot_open_order_coverage(
            snapshot=BrokerAccountSnapshot(open_orders_complete=False),
            provider="kis",
        )

    with pytest.raises(Exception, match="retry_broker_holdings_incomplete"):
        rejected_order_retries_service._validate_snapshot_holdings_coverage(
            snapshot=BrokerAccountSnapshot(holdings_complete=False),
        )


class RetryScopeAdapter:
    def __init__(
        self,
        *,
        provider: str,
        snapshot: BrokerAccountSnapshot,
        broker_account_id: str | None = None,
    ) -> None:
        self.provider = provider
        self.snapshot = snapshot
        self.broker_account_id = broker_account_id

    def describe(self) -> BrokerConfig:
        return BrokerConfig(
            provider=self.provider,
            mode="paper",
            configured=True,
            api_base_url=f"https://{self.provider}.test",
        )

    def authenticate(self) -> BrokerTokenGrant:
        return BrokerTokenGrant(
            access_token=f"{self.provider}-paper-token",
            token_type="Bearer",
            access_token_expires_at=OBSERVED_AT + timedelta(days=1),
        )

    def reconnect_handle(self) -> BrokerReconnectHandle:
        return BrokerReconnectHandle(supported=False, strategy="test")

    def get_broker_account_id(self, access_token: str) -> str | None:
        assert access_token
        return self.broker_account_id

    def get_account_snapshot(
        self,
        access_token: str,
        **_: object,
    ) -> BrokerAccountSnapshot:
        assert access_token
        return self.snapshot


def _settings(*, provider: str) -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        execution_position_account_scope=f"{provider}-paper-account",
        account_drawdown_recovery_scope_secret=(
            "rejected-retry-account-scope-secret-at-least-32-bytes"
        ),
    )


def _execution_scope(
    *,
    settings: Settings,
    provider: str,
    venue: str,
    asset_class: str,
    exchange: str,
    broker_account_id: str | None,
) -> dict[str, object]:
    identity = (
        build_authenticated_broker_position_identity(
            settings=settings,
            provider=provider,
            exchange=exchange,
            broker_account_id=broker_account_id or "",
        )
        if provider == "alpaca"
        else build_position_identity(
            settings=settings,
            provider=provider,
            exchange=exchange,
        )
    )
    return execution_scope_payload(
        identity=identity,
        venue=venue,
        asset_class=asset_class,
    )


def _persist_retry_case(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    execution_scope: dict[str, object],
) -> tuple[Any, Any, ExecutionJob, BrokerOrder, ExecutionFailure]:
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    db = session_factory()
    plan = ExecutionPlan(
        external_plan_id=f"plan-{command.idempotency_key}",
        source="test",
        mode="paper",
        status="accepted",
        market_date=OBSERVED_AT.date(),
    )
    item = ExecutionPlanItem(
        id=f"retry-item-{command.idempotency_key}"[:36],
        plan=plan,
        plan_item_index=0,
        symbol=command.symbol,
        side=command.side,
        intent="next_open_buy" if command.side == "buy" else "stop_sell",
        status="broker_order_retry_pending",
        provider=command.provider,
        venue=command.venue,
        asset_class=command.asset_class,
        base_quantity=command.base_quantity,
        quote_notional=command.quote_notional,
        quantity_precision=command.quantity_precision,
        price_precision=command.price_precision,
        limit_price=command.limit_price,
        time_in_force=command.time_in_force,
        quantity_policy={
            "kind": "shares" if command.quantity is not None else "fractional",
            "quantity": command.quantity,
            "base_quantity": (
                str(command.base_quantity)
                if command.base_quantity is not None
                else None
            ),
            "quote_notional": (
                str(command.quote_notional)
                if command.quote_notional is not None
                else None
            ),
        },
        risk_context={"exchange": command.exchange},
    )
    order = BrokerOrder(
        plan_item=item,
        broker_name=command.provider,
        mode="paper",
        idempotency_key=command.idempotency_key,
        broker_order_id=f"broker-{command.idempotency_key}",
        symbol=command.symbol,
        side=command.side,
        status="retry_pending",
        quantity=command.quantity,
        base_quantity=command.base_quantity,
        quote_notional=command.quote_notional,
        quantity_precision=command.quantity_precision,
        price_precision=command.price_precision,
        venue=command.venue,
        asset_class=command.asset_class,
        limit_price=command.limit_price,
        submitted_at=OBSERVED_AT,
        request_metadata={
            "source": "test",
            "provider": command.provider,
            "venue": command.venue,
            "asset_class": command.asset_class,
            "exchange": command.exchange,
            "time_in_force": command.time_in_force,
            "sizing": {
                "quantity": command.quantity,
                "base_quantity": (
                    str(command.base_quantity)
                    if command.base_quantity is not None
                    else None
                ),
                "quote_notional": (
                    str(command.quote_notional)
                    if command.quote_notional is not None
                    else None
                ),
                "limit_price": (
                    str(command.limit_price)
                    if command.limit_price is not None
                    else None
                ),
                "quantity_precision": command.quantity_precision,
                "price_precision": command.price_precision,
            },
            "execution_scope": execution_scope,
        },
        events=[
            BrokerOrderEvent(
                event_type="submit_requested",
                normalized_status="submission_pending",
                request_payload=command.model_copy(
                    update={"execution_plan_item_id": item.id}
                ).model_dump(mode="json"),
                payload={"provider": command.provider},
                observed_at=OBSERVED_AT,
            )
        ],
    )
    job = ExecutionJob(
        plan_item=item,
        job_type="broker_order_retry",
        status="running",
        scheduled_for=OBSERVED_AT + timedelta(minutes=1),
        locked_at=OBSERVED_AT + timedelta(minutes=1),
    )
    db.add_all([plan, order, job])
    db.flush()
    failure = ExecutionFailure(
        mode="paper",
        broker_order=order,
        execution_plan_item_id=item.id,
        broker_name=command.provider,
        idempotency_key=command.idempotency_key,
        symbol=command.symbol,
        side=command.side,
        intent=item.intent,
        requested_quantity=command.quantity or 0,
        requested_limit_price=command.limit_price,
        requested_notional=command.quote_notional,
        classification="temporary_market_state",
        broker_error_message="temporary market halt",
        retryable=True,
        status="open",
        first_failed_at=OBSERVED_AT,
        latest_failed_at=OBSERVED_AT,
        retry_decision_status="scheduled",
        retry_decision_reason="temporary_orderability_window",
        retry_next_at=job.scheduled_for,
        retry_deadline_at=OBSERVED_AT + timedelta(hours=1),
        retry_max_attempts=2,
        retry_attempt_count=0,
        retry_job_id=job.id,
    )
    db.add(failure)
    db.commit()
    return engine, db, job, order, failure


def _add_active_duplicate_candidate(
    *,
    db: Any,
    source: PlaceOrderRequest,
    execution_scope: dict[str, object] | None,
    provider: str | None = None,
    venue: str | None = None,
    asset_class: str | None = None,
    exchange: str | None = None,
    status: str = "open",
    candidate_label: str | None = None,
    created_at: datetime | None = None,
) -> BrokerOrder:
    candidate_provider = provider or source.provider
    candidate_venue = venue or source.venue or candidate_provider
    candidate_asset_class = asset_class or source.asset_class
    candidate_exchange = exchange or source.exchange
    metadata: dict[str, object] = {
        "provider": candidate_provider,
        "venue": candidate_venue,
        "asset_class": candidate_asset_class,
        "exchange": candidate_exchange,
    }
    if execution_scope is not None:
        metadata["execution_scope"] = execution_scope
    candidate = BrokerOrder(
        broker_name=candidate_provider,
        mode="paper",
        idempotency_key=(
            f"active-candidate-{candidate_label or source.idempotency_key}"
        ),
        broker_order_id=(
            f"broker-active-candidate-{candidate_label or source.idempotency_key}"
        ),
        symbol=source.symbol,
        side=source.side,
        status=status,
        quantity=source.quantity,
        base_quantity=source.base_quantity,
        quote_notional=source.quote_notional,
        quantity_precision=source.quantity_precision,
        price_precision=source.price_precision,
        venue=candidate_venue,
        asset_class=candidate_asset_class,
        limit_price=source.limit_price,
        submitted_at=OBSERVED_AT + timedelta(seconds=1),
        created_at=created_at or OBSERVED_AT,
        request_metadata=metadata,
    )
    db.add(candidate)
    db.commit()
    return candidate


def _alpaca_buy_retry_case(
    *,
    idempotency_key: str,
) -> tuple[
    Settings,
    Any,
    Any,
    ExecutionJob,
    BrokerOrder,
    ExecutionFailure,
    PlaceOrderRequest,
    dict[str, object],
    RetryScopeAdapter,
]:
    settings = _settings(provider="alpaca")
    account_id = "alpaca-account-a"
    source = PlaceOrderRequest(
        idempotency_key=idempotency_key,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        symbol="BTC/USD",
        side="buy",
        base_quantity=Decimal("0.01"),
        quantity_precision=8,
        price_precision=2,
        order_type="limit",
        limit_price=Decimal("50000"),
        time_in_force="gtc",
        exchange="ALPACA",
    )
    scope = _execution_scope(
        settings=settings,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
        broker_account_id=account_id,
    )
    engine, db, job, order, failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    adapter = RetryScopeAdapter(
        provider="alpaca",
        broker_account_id=account_id,
        snapshot=BrokerAccountSnapshot(
            broker_account_id=account_id,
            available_cash=Decimal("100000"),
            buying_power=Decimal("100000"),
            observed_at=OBSERVED_AT,
            source="test:alpaca-duplicate-scope",
            payload={"open_orders_coverage": "complete"},
        ),
    )
    return settings, engine, db, job, order, failure, source, scope, adapter


def _alpaca_sell_retry_case(
    *,
    idempotency_key: str,
) -> tuple[
    Settings,
    Any,
    Any,
    ExecutionJob,
    BrokerOrder,
    ExecutionFailure,
    PlaceOrderRequest,
    dict[str, object],
    RetryScopeAdapter,
]:
    settings = _settings(provider="alpaca")
    account_id = "alpaca-account-a"
    source = PlaceOrderRequest(
        idempotency_key=idempotency_key,
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        symbol="AAPL",
        side="sell",
        base_quantity=Decimal("1"),
        quantity_precision=4,
        price_precision=2,
        order_type="market",
        time_in_force="day",
        exchange="US",
    )
    scope = _execution_scope(
        settings=settings,
        provider="alpaca",
        venue="alpaca",
        asset_class="us_equity",
        exchange="US",
        broker_account_id=account_id,
    )
    engine, db, job, order, failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    adapter = RetryScopeAdapter(
        provider="alpaca",
        broker_account_id=account_id,
        snapshot=BrokerAccountSnapshot(
            broker_account_id=account_id,
            observed_at=OBSERVED_AT,
            source="test:alpaca-sell",
            payload={"open_orders_coverage": "complete"},
            holdings=[
                BrokerAccountHolding(
                    symbol="AAPL",
                    quantity=0,
                    sellable_quantity=0,
                    payload={"base_quantity": "1"},
                )
            ],
        ),
    )
    return settings, engine, db, job, order, failure, source, scope, adapter


@pytest.mark.parametrize(
    ("side", "base_quantity", "limit_price", "order_type"),
    [
        ("buy", Decimal("0.01234567"), Decimal("62500.125"), "limit"),
        ("sell", Decimal("0.00765432"), None, "market"),
    ],
)
def test_alpaca_fractional_retry_restores_exact_canonical_command(
    side: str,
    base_quantity: Decimal,
    limit_price: Decimal | None,
    order_type: str,
) -> None:
    settings = _settings(provider="alpaca")
    account_id = "alpaca-account-a"
    scope = _execution_scope(
        settings=settings,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
        broker_account_id=account_id,
    )
    source = PlaceOrderRequest(
        idempotency_key=f"alpaca-fractional-{side}",
        execution_plan_item_id=None,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        symbol="BTC/USD",
        side=side,
        base_quantity=base_quantity,
        quantity_precision=8,
        price_precision=3,
        order_type=order_type,
        limit_price=limit_price,
        time_in_force="gtc",
        exchange="ALPACA",
        metadata={"decision_marker": "immutable"},
    )
    engine, db, job, order, _failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    adapter = RetryScopeAdapter(
        provider="alpaca",
        broker_account_id=account_id,
        snapshot=BrokerAccountSnapshot(
            broker_account_id=account_id,
            available_cash=Decimal("100000"),
            buying_power=Decimal("100000"),
            observed_at=OBSERVED_AT,
            source="test:alpaca",
            payload={"open_orders_coverage": "complete"},
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    sellable_quantity=0,
                    payload={"base_quantity": "1.25000000"},
                )
            ],
        ),
    )

    retry = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )

    assert retry.model_dump(exclude={"idempotency_key", "metadata"}) == (
        source.model_copy(update={"execution_plan_item_id": order.execution_plan_item_id})
        .model_dump(exclude={"idempotency_key", "metadata"})
    )
    assert retry.metadata is not None
    assert retry.metadata["decision_marker"] == "immutable"
    assert retry.metadata["retry"]["source_execution_scope"] == scope
    engine.dispose()


@pytest.mark.parametrize(
    ("source", "expected_field", "expected_value"),
    [
        (
            PlaceOrderRequest(
                idempotency_key="upbit-quote-buy",
                provider="upbit",
                venue="upbit",
                asset_class="crypto",
                symbol="KRW-BTC",
                side="buy",
                quote_notional=Decimal("125000.5000"),
                quantity_precision=8,
                price_precision=0,
                order_type="market",
                time_in_force="day",
                exchange="UPBIT",
            ),
            "quote_notional",
            Decimal("125000.5000"),
        ),
        (
            PlaceOrderRequest(
                idempotency_key="upbit-base-buy",
                provider="upbit",
                venue="upbit",
                asset_class="crypto",
                symbol="KRW-ETH",
                side="buy",
                base_quantity=Decimal("0.12345678"),
                quantity_precision=8,
                price_precision=0,
                order_type="limit",
                limit_price=Decimal("4500000"),
                time_in_force="day",
                exchange="UPBIT",
            ),
            "base_quantity",
            Decimal("0.12345678"),
        ),
    ],
)
def test_upbit_retry_preserves_quote_and_base_sizing(
    source: PlaceOrderRequest,
    expected_field: str,
    expected_value: Decimal,
) -> None:
    settings = _settings(provider="upbit")
    scope = _execution_scope(
        settings=settings,
        provider="upbit",
        venue="upbit",
        asset_class="crypto",
        exchange="UPBIT",
        broker_account_id=None,
    )
    engine, db, job, _order, _failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    adapter = RetryScopeAdapter(
        provider="upbit",
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("1000000"),
            buying_power=Decimal("1000000"),
            observed_at=OBSERVED_AT,
            source="test:upbit",
        ),
    )

    retry = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )

    assert getattr(retry, expected_field) == expected_value
    assert retry.provider == "upbit"
    assert retry.venue == "upbit"
    assert retry.asset_class == "crypto"
    assert retry.exchange == "UPBIT"
    assert retry.quantity_precision == 8
    assert retry.price_precision == 0
    engine.dispose()


def test_kis_retry_route_drift_is_quarantined_before_broker_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(provider="kis").model_copy(
        update={"domestic_order_session_routing_enabled": True}
    )
    source = PlaceOrderRequest(
        idempotency_key="kis-nxt-route-drift",
        provider="kis",
        venue="kis",
        asset_class="stock",
        symbol="005930",
        side="sell",
        quantity=1,
        order_type="limit",
        limit_price=Decimal("71000"),
        time_in_force="day",
        exchange="NXT",
    )
    scope = _execution_scope(
        settings=settings,
        provider="kis",
        venue="kis",
        asset_class="stock",
        exchange="NXT",
        broker_account_id=None,
    )
    engine, db, job, order, failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "NXT"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )
    assert risk_context is not None
    assert quantity_policy is not None
    job.plan_item.risk_context = risk_context
    job.plan_item.quantity_policy = quantity_policy
    db.commit()
    adapter = RetryScopeAdapter(
        provider="kis",
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("0"),
            observed_at=OBSERVED_AT,
            source="test:kis-route-drift",
            holdings=[
                BrokerAccountHolding(
                    symbol="005930",
                    quantity=1,
                    sellable_quantity=1,
                    average_price=Decimal("70000"),
                )
            ],
        ),
    )
    retry = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )
    assert broker_orders_service._rejected_order_retry_route_guard(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        command=retry,
        routed_command=retry,
        request_source=f"execution-job:{job.id}",
        observed_at=OBSERVED_AT + timedelta(minutes=2),
    ) is None
    resume_runtime(
        settings=settings,
        db=db,
        command=ControlCommand(
            actor="test",
            source="test",
            reason="exercise rejected-order retry route guard",
        ),
    )
    monkeypatch.setattr(
        broker_orders_service,
        "_route_now",
        lambda _metadata: datetime(2026, 9, 11, 1, 0, tzinfo=UTC),
    )

    result = broker_orders_service.place_broker_order(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        command=retry,
        request_source=f"execution-job:{job.id}",
        now=OBSERVED_AT + timedelta(minutes=2),
    )

    assert result.status_code == 409
    assert result.payload["block_reason"] == "retry_route_authority_changed"
    assert result.payload["manual_reconciliation_required"] is True
    db.refresh(order)
    db.refresh(failure)
    db.refresh(job.plan_item)
    assert order.status == "manual_review_required"
    assert failure.retry_decision_status == "manual_review_required"
    assert job.plan_item.status == "manual_review_required"
    retry_order = db.scalar(
        select(BrokerOrder).where(
            BrokerOrder.idempotency_key == retry.idempotency_key
        )
    )
    assert retry_order is None
    engine.dispose()


@pytest.mark.parametrize("foreign_dimension", ["provider", "account", "route"])
def test_active_duplicate_check_ignores_foreign_execution_scope(
    foreign_dimension: str,
) -> None:
    (
        settings,
        engine,
        db,
        job,
        _order,
        failure,
        source,
        _source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(
        idempotency_key=f"alpaca-foreign-{foreign_dimension}-duplicate"
    )
    candidate_provider = "upbit" if foreign_dimension == "provider" else "alpaca"
    candidate_venue = "upbit" if foreign_dimension == "provider" else "alpaca"
    candidate_asset_class = (
        "us_equity" if foreign_dimension == "route" else "crypto"
    )
    candidate_exchange = (
        "UPBIT"
        if foreign_dimension == "provider"
        else "US"
        if foreign_dimension == "route"
        else "ALPACA"
    )
    foreign_scope = _execution_scope(
        settings=settings,
        provider=candidate_provider,
        venue=candidate_venue,
        asset_class=candidate_asset_class,
        exchange=candidate_exchange,
        broker_account_id=(
            None
            if foreign_dimension == "provider"
            else "alpaca-account-b"
            if foreign_dimension == "account"
            else "alpaca-account-a"
        ),
    )
    _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=foreign_scope,
        provider=candidate_provider,
        venue=candidate_venue,
        asset_class=candidate_asset_class,
        exchange=candidate_exchange,
    )

    retry = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )

    assert retry.provider == "alpaca"
    assert failure.retry_decision_status == "scheduled"
    assert failure.retry_attempt_count == 1
    engine.dispose()


def test_active_duplicate_check_blocks_only_exact_execution_scope() -> None:
    (
        settings,
        engine,
        db,
        job,
        _order,
        failure,
        source,
        source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-exact-duplicate")
    duplicate = _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=source_scope,
    )

    with pytest.raises(RetryOrderGuardError, match="active duplicate order"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.flush()
    db.refresh(failure)
    assert failure.retry_decision_status == "skipped"
    assert failure.retry_decision_reason == "active_duplicate_order_exists"
    assert failure.retry_audit_context["latest_skip"]["duplicate_broker_order_pk"] == (
        duplicate.id
    )
    engine.dispose()


def test_unscoped_candidate_is_not_hidden_by_a_newer_exact_duplicate() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        source,
        source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-mixed-duplicates")
    _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=None,
        candidate_label="unscoped-older",
        created_at=OBSERVED_AT + timedelta(seconds=1),
    )
    _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=source_scope,
        candidate_label="exact-newer",
        created_at=OBSERVED_AT + timedelta(seconds=2),
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_duplicate_order_scope_invalid"
    assert failure.retry_decision_status == "manual_review_required"
    engine.dispose()


def test_prior_retry_pending_order_does_not_block_the_next_bounded_attempt() -> None:
    (
        settings,
        engine,
        db,
        job,
        _order,
        failure,
        source,
        source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-next-bounded-retry")
    _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=source_scope,
        status="retry_pending",
    )

    retry = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )

    assert retry.provider == "alpaca"
    assert failure.retry_attempt_count == 1
    engine.dispose()


def test_existing_retry_idempotency_collision_with_foreign_scope_is_quarantined() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        source,
        _source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-existing-retry-collision")
    foreign_scope = _execution_scope(
        settings=settings,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
        broker_account_id="alpaca-account-b",
    )
    collision = _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=foreign_scope,
    )
    collision.idempotency_key = f"retry:{order.id}:{job.id}"
    db.commit()

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_existing_order_scope_conflict"
    assert failure.retry_decision_status == "manual_review_required"
    engine.dispose()


def test_existing_retry_order_with_exact_scope_and_lineage_is_idempotent() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        _source,
        source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-existing-exact-retry")
    first = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=2),
    )
    retry_order = BrokerOrder(
        execution_plan_item_id=order.execution_plan_item_id,
        broker_name=first.provider,
        mode="paper",
        idempotency_key=first.idempotency_key,
        broker_order_id="broker-existing-exact-retry",
        symbol=first.symbol,
        side=first.side,
        status="submitted",
        quantity=first.quantity,
        base_quantity=first.base_quantity,
        quote_notional=first.quote_notional,
        quantity_precision=first.quantity_precision,
        price_precision=first.price_precision,
        venue=first.venue,
        asset_class=first.asset_class,
        limit_price=first.limit_price,
        submitted_at=OBSERVED_AT + timedelta(minutes=2),
        request_metadata={
            "provider": first.provider,
            "venue": first.venue,
            "asset_class": first.asset_class,
            "exchange": first.exchange,
            "time_in_force": first.time_in_force,
            "sizing": {
                "quantity": first.quantity,
                "base_quantity": str(first.base_quantity),
                "quote_notional": None,
                "limit_price": str(first.limit_price),
                "quantity_precision": first.quantity_precision,
                "price_precision": first.price_precision,
            },
            "execution_scope": source_scope,
            "metadata": first.metadata,
        },
    )
    db.add(retry_order)
    db.commit()

    second = build_rejected_order_retry_request(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job=job,
        triggered_at=OBSERVED_AT + timedelta(minutes=3),
    )

    assert second.idempotency_key == first.idempotency_key
    assert failure.retry_attempt_count == 1
    assert failure.retry_decision_status == "scheduled"
    engine.dispose()


def test_active_alpaca_snapshot_sell_blocks_protective_retry() -> None:
    (
        settings,
        engine,
        db,
        job,
        _order,
        failure,
        _source,
        _source_scope,
        adapter,
    ) = _alpaca_sell_retry_case(idempotency_key="alpaca-active-snapshot-sell")
    adapter.snapshot = replace(
        adapter.snapshot,
        open_orders=[
            BrokerAccountOpenOrder(
                symbol="AAPL",
                side="sell",
                quantity=1,
                remaining_quantity=1,
                broker_order_id="external-active-sell",
            )
        ],
    )

    with pytest.raises(RetryOrderGuardError, match="active duplicate sell order"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    assert failure.retry_attempt_count == 0
    assert failure.retry_order_idempotency_key is None
    engine.dispose()


def test_incomplete_alpaca_sell_open_order_coverage_is_quarantined() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        _source,
        _source_scope,
        adapter,
    ) = _alpaca_sell_retry_case(idempotency_key="alpaca-incomplete-sell-orders")
    adapter.snapshot = replace(
        adapter.snapshot,
        payload={
            "open_orders_coverage": "incomplete",
            "open_orders_error": {"degraded": True},
        },
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_broker_open_orders_incomplete"
    assert failure.retry_decision_status == "manual_review_required"
    assert failure.retry_attempt_count == 0
    assert failure.retry_order_idempotency_key is None
    engine.dispose()


def test_malformed_fractional_snapshot_open_order_is_quarantined() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        source,
        _source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-malformed-open-order")
    adapter.snapshot = BrokerAccountSnapshot(
        broker_account_id="alpaca-account-a",
        available_cash=Decimal("100000"),
        buying_power=Decimal("100000"),
        observed_at=OBSERVED_AT,
        source="test:malformed-fractional-open-order",
        payload={"open_orders_coverage": "complete"},
        open_orders=[
            BrokerAccountOpenOrder(
                symbol=source.symbol,
                side=source.side,
                quantity=0,
                remaining_quantity=0,
                payload={"remaining_base_quantity": "not-a-decimal"},
            )
        ],
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_broker_snapshot_order_invalid"
    assert failure.retry_decision_status == "manual_review_required"
    engine.dispose()


def test_incomplete_alpaca_open_order_coverage_is_quarantined() -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        _source,
        _source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-incomplete-open-orders")
    adapter.snapshot = BrokerAccountSnapshot(
        broker_account_id="alpaca-account-a",
        available_cash=Decimal("100000"),
        buying_power=Decimal("100000"),
        observed_at=OBSERVED_AT,
        source="test:incomplete-open-order-coverage",
        payload={
            "open_orders_coverage": "incomplete",
            "open_orders_error": {"degraded": True},
        },
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_broker_open_orders_incomplete"
    assert failure.retry_decision_status == "manual_review_required"
    engine.dispose()


def test_transient_account_snapshot_failure_keeps_retry_scheduled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        settings,
        engine,
        db,
        job,
        _order,
        failure,
        _source,
        _source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(idempotency_key="alpaca-snapshot-unavailable")

    def _raise_snapshot_error(*_: object, **__: object) -> BrokerAccountSnapshot:
        raise RuntimeError("temporary snapshot outage")

    monkeypatch.setattr(adapter, "get_account_snapshot", _raise_snapshot_error)

    with pytest.raises(RetryOrderGuardError) as error:
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    assert error.value.job_status == "retrying"
    assert error.value.plan_item_status == "broker_order_retry_pending"
    assert failure.retry_decision_status == "scheduled"
    assert failure.retry_next_at is not None
    assert failure.retry_deadline_at is not None
    engine.dispose()


@pytest.mark.parametrize("candidate_shape", ["unscoped", "contradictory"])
def test_ambiguous_same_provider_active_candidate_is_quarantined(
    candidate_shape: str,
) -> None:
    (
        settings,
        engine,
        db,
        job,
        order,
        failure,
        source,
        source_scope,
        adapter,
    ) = _alpaca_buy_retry_case(
        idempotency_key=f"alpaca-{candidate_shape}-duplicate"
    )
    _add_active_duplicate_candidate(
        db=db,
        source=source,
        execution_scope=(None if candidate_shape == "unscoped" else source_scope),
        venue=("other-venue" if candidate_shape == "contradictory" else None),
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_code == "retry_duplicate_order_scope_invalid"
    assert failure.retry_decision_status == "manual_review_required"
    assert failure.retry_next_at is None
    engine.dispose()


@pytest.mark.parametrize("mutation", ["provider", "account", "route", "sizing"])
def test_retry_scope_or_command_mutation_is_quarantined_for_manual_review(
    mutation: str,
) -> None:
    settings = _settings(provider="alpaca")
    source_account_id = "alpaca-account-a"
    source = PlaceOrderRequest(
        idempotency_key=f"alpaca-mutation-{mutation}",
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        symbol="BTC/USD",
        side="sell",
        base_quantity=Decimal("0.01"),
        quantity_precision=8,
        price_precision=2,
        order_type="market",
        time_in_force="gtc",
        exchange="ALPACA",
    )
    scope = _execution_scope(
        settings=settings,
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
        broker_account_id=source_account_id,
    )
    engine, db, job, order, failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope=scope,
    )
    adapter_provider = "alpaca"
    current_account_id = source_account_id
    if mutation == "provider":
        adapter_provider = "upbit"
    elif mutation == "account":
        current_account_id = "alpaca-account-b"
    elif mutation == "route":
        order.venue = "mutated-venue"
    elif mutation == "sizing":
        order.base_quantity = Decimal("0.02")
    db.commit()
    adapter = RetryScopeAdapter(
        provider=adapter_provider,
        broker_account_id=current_account_id,
        snapshot=BrokerAccountSnapshot(
            broker_account_id=current_account_id,
            observed_at=OBSERVED_AT,
            source="test:mutation",
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    sellable_quantity=0,
                    payload={"base_quantity": "1.0"},
                )
            ],
        ),
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert order.last_error_classification == "retry_command_integrity"
    assert failure.retry_decision_status == "manual_review_required"
    assert failure.retry_next_at is None
    events = db.scalars(
        select(BrokerOrderEvent).where(
            BrokerOrderEvent.broker_order_pk == order.id,
            BrokerOrderEvent.event_type == "retry_command_integrity_blocked",
        )
    ).all()
    assert len(events) == 1
    engine.dispose()


def test_retry_without_complete_execution_scope_is_quarantined() -> None:
    settings = _settings(provider="upbit")
    source = PlaceOrderRequest(
        idempotency_key="upbit-missing-scope",
        provider="upbit",
        venue="upbit",
        asset_class="crypto",
        symbol="KRW-BTC",
        side="buy",
        quote_notional=Decimal("50000"),
        quantity_precision=8,
        price_precision=0,
        order_type="market",
        time_in_force="day",
        exchange="UPBIT",
    )
    engine, db, job, order, failure = _persist_retry_case(
        settings=settings,
        command=source,
        execution_scope={},
    )
    adapter = RetryScopeAdapter(
        provider="upbit",
        snapshot=BrokerAccountSnapshot(
            available_cash=Decimal("100000"),
            observed_at=OBSERVED_AT,
            source="test:missing-scope",
        ),
    )

    with pytest.raises(RetryOrderGuardError, match="manual review"):
        build_rejected_order_retry_request(
            settings=settings,
            db=db,
            adapter=adapter,  # type: ignore[arg-type]
            job=job,
            triggered_at=OBSERVED_AT + timedelta(minutes=2),
        )

    db.refresh(order)
    db.refresh(failure)
    assert order.status == "manual_review_required"
    assert failure.retry_decision_reason == "retry_execution_scope_invalid"
    engine.dispose()


def test_unexpected_retry_request_build_failure_never_marks_protective_sell_as_buy_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(provider="alpaca")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    db = session_factory()
    plan = ExecutionPlan(
        external_plan_id="plan-malformed-protective-retry",
        source="test",
        mode="paper",
        status="accepted",
        market_date=OBSERVED_AT.date(),
    )
    item = ExecutionPlanItem(
        plan=plan,
        plan_item_index=0,
        symbol="BTC/USD",
        side="sell",
        intent="stop_sell",
        status="broker_order_retry_dispatching",
        provider="alpaca",
        venue="alpaca",
        asset_class="crypto",
        base_quantity=Decimal("0.01"),
        quantity_policy={"kind": "fractional", "base_quantity": "0.01"},
        time_in_force="gtc",
        risk_context={"exchange": "ALPACA"},
    )
    job = ExecutionJob(
        plan_item=item,
        job_type="broker_order_retry",
        status="running",
        scheduled_for=OBSERVED_AT,
        locked_at=OBSERVED_AT,
        attempts=1,
    )
    db.add_all([plan, job])
    db.commit()
    monkeypatch.setattr(
        execution_jobs_service,
        "build_rejected_order_retry_request",
        lambda **_: (_ for _ in ()).throw(ValueError("malformed retry command")),
    )
    adapter = RetryScopeAdapter(
        provider="alpaca",
        broker_account_id="alpaca-account-a",
        snapshot=BrokerAccountSnapshot(
            broker_account_id="alpaca-account-a",
            observed_at=OBSERVED_AT,
            source="test:unexpected-build-failure",
        ),
    )

    result = execution_jobs_service._dispatch_job(
        settings=settings,
        db=db,
        adapter=adapter,  # type: ignore[arg-type]
        job_id=job.id,
        claim_locked_at=OBSERVED_AT,
    )

    db.refresh(job)
    db.refresh(item)
    assert result == "failed"
    assert job.status == "failed"
    assert item.status == "manual_review_required"
    assert item.status != "next_open_buy_failed"
    engine.dispose()
