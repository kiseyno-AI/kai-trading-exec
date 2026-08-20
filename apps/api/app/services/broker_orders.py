from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.api.schemas.execution import PlaceOrderRequest
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerAdapter,
    BrokerAdapterError,
    BrokerEventSource,
    BrokerOrderPlacementRequest,
    NormalizedBrokerOrderStatus,
)
from app.core.config import Settings
from app.models.runtime import BrokerOrder, BrokerOrderEvent, ExecutionPlan, ExecutionPlanItem
from app.models.runtime import ExecutionFill, ExecutionPosition, new_id
from app.services.account_drawdown_gate import (
    AccountDrawdownDecision,
    account_drawdown_uncertain_decision,
    evaluate_account_drawdown_gate,
    evaluate_us_equity_account_gate,
    record_account_drawdown_block,
)
from app.services.broker_raw_holdings import snapshot_with_raw_balance_holdings
from app.services.broker_auth import ensure_authenticated_broker_session
from app.services.domestic_order_router import RouteDecision, resolve_domestic_order_route
from app.services.execution_failures import record_actionable_execution_failure
from app.services.execution_api import serialize_broker_order
from app.services.fill_idempotency import execution_fill_idempotency_key
from app.services.monitoring_price_source import is_us_regular_session, next_us_regular_open
from app.services.runtime_controls import (
    automated_order_submission_allowed,
    automated_order_submission_block_reason,
    ensure_runtime_control_state,
    runtime_safety_payload,
)
from app.services.runtime_transitions import (
    apply_broker_order_status,
    plan_item_status_for_broker_order_status,
    set_plan_item_status,
)

UPBIT_PAPER_ALLOWED_SYMBOLS = frozenset({"KRW-BTC", "KRW-ETH"})


@dataclass(frozen=True)
class PlaceBrokerOrderResult:
    status_code: int
    payload: dict[str, object]


def place_broker_order(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str = "post:/orders",
    before_success_commit: Callable[[], None] | None = None,
) -> PlaceBrokerOrderResult:
    existing = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    if existing is not None:
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(existing, include_events=True),
            },
        )

    control_state = ensure_runtime_control_state(settings=settings, db=db)
    if not automated_order_submission_allowed(control_state):
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "message": "automated order submission is blocked by runtime safety controls",
                "block_reason": automated_order_submission_block_reason(control_state),
                "safety": runtime_safety_payload(control_state),
            },
        )

    provider_error = _validate_provider_capability(adapter=adapter, command=command)
    if provider_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={
                "duplicate": False,
                "message": provider_error,
                "permanent_rejection": True,
                "rejection_reason": provider_error,
            },
        )

    plan_item = _load_plan_item(
        settings=settings,
        db=db,
        command=command,
    )
    if plan_item is None and command.execution_plan_item_id is not None:
        return PlaceBrokerOrderResult(
            status_code=404,
            payload={
                "message": "execution_plan_item_id was not found",
                "execution_plan_item_id": command.execution_plan_item_id,
            },
        )

    validation_error = _validate_plan_item(plan_item=plan_item, command=command)
    if validation_error is not None:
        return PlaceBrokerOrderResult(
            status_code=422,
            payload={"message": validation_error},
        )

    upbit_guard_error = _validate_upbit_paper_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    )
    if upbit_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": upbit_guard_error,
                "permanent_rejection": True,
                "rejection_reason": upbit_guard_error,
            },
        )

    alpaca_guard_error = _validate_alpaca_crypto_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
        request_source=request_source,
    )
    if alpaca_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": alpaca_guard_error,
                "permanent_rejection": True,
                "rejection_reason": alpaca_guard_error,
            },
        )

    kis_us_guard_error = _validate_kis_us_equity_guardrails(
        settings=settings,
        adapter=adapter,
        command=command,
        plan_item=plan_item,
    )
    if kis_us_guard_error is not None:
        return PlaceBrokerOrderResult(
            status_code=409,
            payload={
                "duplicate": False,
                "message": kis_us_guard_error,
                "permanent_rejection": True,
                "rejection_reason": kis_us_guard_error,
            },
        )

    kis_us_session_route = _kis_us_session_route(command)
    if kis_us_session_route is not None:
        return PlaceBrokerOrderResult(
            status_code=202,
            payload={
                "duplicate": False,
                "message": str(kis_us_session_route["reason"]),
                "deferred": True,
                "block_reason": None,
                "route": kis_us_session_route,
            },
        )

    route_decision = _resolve_route_decision(
        settings=settings,
        command=command,
        plan_item=plan_item,
    )
    if route_decision is not None and route_decision.action != "submit":
        return PlaceBrokerOrderResult(
            status_code=202 if route_decision.action == "defer" else 409,
            payload={
                "duplicate": False,
                "message": route_decision.reason,
                "deferred": route_decision.action == "defer",
                "block_reason": (
                    route_decision.reason
                    if route_decision.action == "block"
                    else None
                ),
                "route": _route_payload(route_decision),
            },
        )
    routed_command = _routed_order_command(
        command=command,
        route_decision=route_decision,
    )

    account_gate_decision = _evaluate_new_buy_account_gate(
        settings=settings,
        db=db,
        adapter=adapter,
        command=routed_command,
        plan_item=plan_item,
    )
    if account_gate_decision is not None and not account_gate_decision.allowed:
        return _persist_account_gate_block(
            settings=settings,
            db=db,
            adapter=adapter,
            command=routed_command,
            request_source=request_source,
            plan_item=plan_item,
            route_decision=route_decision,
            decision=account_gate_decision,
        )

    order = BrokerOrder(
        broker_name=adapter.provider,
        execution_plan_item_id=plan_item.id if plan_item else None,
        mode=settings.runtime_mode,
        idempotency_key=routed_command.idempotency_key,
        symbol=routed_command.symbol,
        side=routed_command.side,
        status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        quantity=routed_command.quantity,
        base_quantity=routed_command.base_quantity,
        quote_notional=routed_command.quote_notional,
        quantity_precision=routed_command.quantity_precision,
        price_precision=routed_command.price_precision,
        venue=routed_command.venue,
        asset_class=routed_command.asset_class,
        limit_price=routed_command.limit_price,
        request_metadata={
            "source": request_source,
            "trusted_order_origin": _trusted_order_origin(request_source),
            "requested_at": datetime.now(UTC).isoformat(),
            "time_in_force": routed_command.time_in_force,
            "provider": routed_command.provider,
            "asset_class": routed_command.asset_class,
            "venue": routed_command.venue,
            "sizing": _sizing_payload(routed_command),
            **({"metadata": routed_command.metadata} if routed_command.metadata is not None else {}),
            **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
        },
    )
    db.add(order)
    request_observed_at = datetime.now(UTC)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        duplicate = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
        if duplicate is None:
            raise
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(duplicate, include_events=True),
            },
        )

    request_event = BrokerOrderEvent(
        broker_order_pk=order.id,
        event_type="submit_requested",
        event_source=BrokerEventSource.SUBMISSION.value,
        normalized_status=NormalizedBrokerOrderStatus.SUBMISSION_PENDING.value,
        request_payload=_order_command_payload(routed_command),
        payload={"provider": adapter.provider},
        observed_at=request_observed_at,
    )
    db.add(request_event)
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="broker_order_submission_pending",
            source="broker_order_service",
            reason=f"broker order submission requested for {routed_command.idempotency_key}",
            payload={"broker_order_pk": order.id, "idempotency_key": order.idempotency_key},
        )

    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="broker_order_service",
            reason=f"ensure active broker session for {command.idempotency_key}",
        )
        placement = adapter.place_order(
            session_state.access_token or "",
            BrokerOrderPlacementRequest(
                idempotency_key=command.idempotency_key,
                execution_plan_item_id=routed_command.execution_plan_item_id,
                symbol=routed_command.symbol,
                side=routed_command.side,
                quantity=routed_command.quantity,
                base_quantity=routed_command.base_quantity,
                quote_notional=routed_command.quote_notional,
                quantity_precision=routed_command.quantity_precision,
                price_precision=routed_command.price_precision,
                provider=routed_command.provider,
                venue=routed_command.venue,
                asset_class=routed_command.asset_class,
                order_type=routed_command.order_type,
                time_in_force=routed_command.time_in_force,
                exchange=routed_command.exchange,
                limit_price=routed_command.limit_price,
                order_division=route_decision.ord_dvsn if route_decision is not None else None,
                route_session=route_decision.session if route_decision is not None else None,
                route_reason=route_decision.reason if route_decision is not None else None,
            ),
        )
    except BrokerAdapterError as exc:
        observed_at = exc.observed_at or datetime.now(UTC)
        event_observed_at = _response_event_observed_at(
            observed_at=observed_at,
            request_observed_at=request_observed_at,
        )
        apply_broker_order_status(order=order, status=exc.normalized_status.value)
        order.last_broker_sync_at = observed_at
        order.request_metadata = exc.request_metadata or order.request_metadata
        order.response_metadata = exc.response_metadata or None
        order.last_error_classification = exc.classification.value
        order.last_error_code = exc.code
        order.last_error_message = str(exc)
        order.last_error_retryable = exc.retryable
        order.raw_broker_payload = exc.response_payload or None
        db.add(
            BrokerOrderEvent(
                broker_order_pk=order.id,
                event_type="submit_failed",
                event_source=BrokerEventSource.SUBMISSION.value,
                normalized_status=order.status,
                broker_status=exc.code,
                error_classification=exc.classification.value,
                retryable=exc.retryable,
                message=str(exc),
                request_payload=exc.request_payload or _order_command_payload(routed_command),
                response_payload=exc.response_payload or None,
                payload={"provider": adapter.provider},
                observed_at=event_observed_at,
            )
        )
        if plan_item is not None:
            set_plan_item_status(
                db=db,
                plan_item=plan_item,
                status=_plan_item_status_for_order_status(order.status),
                source="broker_order_service",
                reason=str(exc),
                payload={
                    "broker_order_pk": order.id,
                    "idempotency_key": order.idempotency_key,
                    "broker_status": exc.code,
                    "event_type": "submit_failed",
                },
            )
        record_actionable_execution_failure(
            settings=settings,
            db=db,
            order=order,
            plan_item=plan_item,
            broker_error_classification=exc.classification.value,
            broker_error_code=exc.code,
            broker_error_message=str(exc),
            retryable=exc.retryable,
            observed_at=observed_at,
            request_source=request_source,
        )
        db.commit()
        db.refresh(order)
        return PlaceBrokerOrderResult(
            status_code=exc.status_code or 503,
            payload={
                "duplicate": False,
                "message": str(exc),
                "order": serialize_broker_order(
                    _load_order_by_idempotency_key(
                        settings=settings,
                        db=db,
                        idempotency_key=command.idempotency_key,
                    )
                    or order,
                    include_events=True,
                ),
            },
        )

    order.broker_order_id = placement.broker_order_id
    apply_broker_order_status(order=order, status=placement.normalized_status.value)
    order.submitted_at = placement.submitted_at
    order.last_broker_sync_at = placement.observed_at
    order.request_metadata = _merge_order_request_metadata(
        base=placement.request_metadata,
        command=routed_command,
        request_source=request_source,
        route_decision=route_decision,
    )
    response_metadata = dict(placement.response_metadata or {})
    if account_gate_decision is not None:
        response_metadata["account_drawdown_gate"] = account_gate_decision.summary
    order.response_metadata = response_metadata
    order.last_error_classification = None
    order.last_error_code = None
    order.last_error_message = None
    order.last_error_retryable = None
    order.raw_broker_payload = placement.response_payload
    event_observed_at = _response_event_observed_at(
        observed_at=placement.observed_at,
        request_observed_at=request_observed_at,
    )
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submit_acknowledged",
            event_source=BrokerEventSource.SUBMISSION.value,
            normalized_status=order.status,
            broker_status=placement.broker_status,
            message=placement.message,
            request_payload=placement.request_payload,
            response_payload=placement.response_payload,
            payload={"provider": adapter.provider},
            observed_at=event_observed_at,
        )
    )
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status=_plan_item_status_for_order_status(order.status),
            source="broker_order_service",
            reason=placement.message,
            payload={
                "broker_order_pk": order.id,
                "broker_order_id": placement.broker_order_id,
                "idempotency_key": order.idempotency_key,
                "event_type": "submit_acknowledged",
            },
        )

    _record_upbit_paper_simulated_fill(
        settings=settings,
        db=db,
        order=order,
        command=routed_command,
        plan_item=plan_item,
        observed_at=event_observed_at + timedelta(microseconds=1),
    )

    if before_success_commit is not None:
        before_success_commit()

    db.commit()
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=201,
        payload={
            "duplicate": False,
            "message": placement.message or "broker order submitted",
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _load_order_by_idempotency_key(
    *,
    settings: Settings,
    db: Session,
    idempotency_key: str,
) -> BrokerOrder | None:
    return db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.idempotency_key == idempotency_key,
        )
        .options(selectinload(BrokerOrder.events))
    )


def _load_plan_item(
    *,
    settings: Settings,
    db: Session,
    command: PlaceOrderRequest,
) -> ExecutionPlanItem | None:
    if command.execution_plan_item_id is None:
        return None
    return db.scalar(
        select(ExecutionPlanItem)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlanItem.id == command.execution_plan_item_id,
            ExecutionPlan.mode == settings.runtime_mode,
        )
    )


def _validate_plan_item(
    *,
    plan_item: ExecutionPlanItem | None,
    command: PlaceOrderRequest,
) -> str | None:
    if plan_item is None:
        return None
    if plan_item.symbol != command.symbol:
        return "execution plan item symbol does not match order symbol"
    if plan_item.side != command.side:
        return "execution plan item side does not match order side"
    if plan_item.provider and plan_item.provider != command.provider:
        return "execution plan item provider does not match order provider"
    if plan_item.asset_class and plan_item.asset_class != command.asset_class:
        return "execution plan item asset_class does not match order asset_class"
    if plan_item.venue and command.venue and plan_item.venue != command.venue:
        return "execution plan item venue does not match order venue"
    return None


def _validate_provider_capability(
    *,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
) -> str | None:
    if command.provider != adapter.provider:
        return (
            f"order provider '{command.provider}' does not match adapter "
            f"provider '{adapter.provider}'"
        )
    if command.provider == "upbit" and command.asset_class != "crypto":
        return "upbit adapter accepts crypto orders only"
    if command.provider == "alpaca" and command.asset_class not in {"crypto", "us_equity"}:
        return "alpaca adapter accepts crypto and US-equity orders only"
    return None


def _validate_upbit_paper_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> str | None:
    if command.provider != "upbit" and adapter.provider != "upbit":
        return None
    if command.provider != "upbit" or adapter.provider != "upbit":
        return "Upbit crypto paper execution requires provider='upbit'"
    if command.asset_class != "crypto":
        return "Upbit crypto paper execution requires asset_class='crypto'"
    if (command.venue or "").lower() != "upbit":
        return "Upbit crypto paper execution requires venue='upbit'"
    if command.exchange != "UPBIT":
        return "Upbit crypto paper execution requires exchange='UPBIT'"
    if command.symbol not in UPBIT_PAPER_ALLOWED_SYMBOLS:
        return "Upbit crypto paper execution is limited to KRW-BTC and KRW-ETH"
    if settings.runtime_mode != "paper":
        return "Upbit execution is paper-only in this runtime"
    config = adapter.describe()
    if config.provider != "upbit" or not config.configured:
        return config.reason or "Upbit provider readiness is uncertain"
    if plan_item is not None:
        if plan_item.provider != "upbit" or plan_item.asset_class != "crypto" or plan_item.venue != "upbit":
            return "execution plan item must explicitly declare Upbit crypto provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and notional is None:
        return "Upbit crypto paper buy requires deterministic quote_notional or limit-price notional"
    if notional is not None and notional > settings.exec_agent_max_symbol_budget:
        return "Upbit crypto paper order exceeds per-symbol execution budget"
    return None


def _validate_alpaca_crypto_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    request_source: str,
) -> str | None:
    if command.provider != "alpaca" and adapter.provider != "alpaca":
        return None
    if command.provider != "alpaca" or adapter.provider != "alpaca":
        return "Alpaca crypto execution requires provider='alpaca'"
    if command.asset_class not in {"crypto", "us_equity"}:
        return "Alpaca execution requires asset_class='crypto' or 'us_equity'"
    expected_exchange = "ALPACA" if command.asset_class == "crypto" else "US"
    if (command.venue or "").lower() != "alpaca" or command.exchange != expected_exchange:
        return f"Alpaca {command.asset_class} execution requires venue='alpaca' and exchange='{expected_exchange}'"
    configured_symbols = (
        settings.alpaca_crypto_allowed_symbols
        if command.asset_class == "crypto"
        else settings.alpaca_us_equity_allowed_symbols
    )
    allowed_symbols = {
        value.strip().upper().replace("-", "/")
        for value in configured_symbols.split(",")
        if value.strip()
    }
    if command.symbol not in allowed_symbols:
        return f"Alpaca {command.asset_class} symbol is not in its configured allowlist"
    if settings.runtime_mode == "mock":
        return "Alpaca execution requires paper or live runtime mode"
    if settings.runtime_mode == "live" and not settings.alpaca_live_trading_enabled:
        return "Alpaca live trading is disabled by configuration"
    order_origin_error = _validate_trusted_order_origin(command=command, request_source=request_source)
    if order_origin_error is not None:
        return order_origin_error
    if (
        command.asset_class == "us_equity"
        and settings.runtime_mode == "live"
        and _is_alpaca_us_equity_strategy_order(request_source)
        and not settings.alpaca_us_equity_strategy_live_enabled
    ):
        return "Alpaca US-equity strategy live trading is disabled by configuration"
    config = adapter.describe()
    if not config.configured:
        return config.reason or "Alpaca provider readiness is uncertain"
    if plan_item is not None and (
        plan_item.provider != "alpaca"
        or plan_item.asset_class != command.asset_class
        or plan_item.venue != "alpaca"
    ):
        return "execution plan item must explicitly declare matching Alpaca provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and notional is None:
        return f"Alpaca {command.asset_class} buys require quote_notional or base_quantity with limit_price"
    max_notional = (
        settings.alpaca_crypto_max_order_notional_usd
        if command.asset_class == "crypto"
        else settings.alpaca_us_equity_max_order_notional_usd
    )
    if notional is not None and notional > max_notional:
        return f"Alpaca {command.asset_class} order exceeds its configured maximum notional"
    return None


def _validate_kis_us_equity_guardrails(
    *,
    settings: Settings,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> str | None:
    if command.provider != "kis" or command.asset_class != "us_equity":
        return None
    if adapter.provider != "kis":
        return "KIS US-equity execution requires provider='kis'"
    if not settings.kis_us_trading_enabled:
        return "KIS US-equity trading is disabled by configuration"
    if (command.venue or "").lower() != "kis":
        return "KIS US-equity execution requires venue='kis'"
    if command.exchange not in {"NASD", "NYSE", "AMEX"}:
        return "KIS US-equity execution requires NASD, NYSE, or AMEX exchange code"
    if command.order_type != "limit" or command.time_in_force != "day":
        return "KIS US-equity execution is limited to day limit orders"
    if command.quantity is None or command.limit_price is None:
        return "KIS US-equity execution requires whole-share quantity and limit_price"
    if plan_item is None:
        return "KIS US-equity execution requires an approved execution plan item"
    if plan_item.provider != "kis" or plan_item.asset_class != "us_equity" or plan_item.venue != "kis":
        return "execution plan item must explicitly declare KIS US-equity provider metadata"
    notional = _order_notional(command=command, plan_item=plan_item)
    if command.side == "buy" and (
        notional is None or notional > settings.kis_us_max_order_notional_usd
    ):
        return "KIS US-equity order exceeds its configured maximum USD notional"
    if settings.runtime_mode == "live":
        if not settings.kis_us_live_trading_enabled:
            return "KIS US-equity live trading is disabled by configuration"
        if settings.kis_us_live_confirmation_token != "ENABLE_KIS_US_LIVE":
            return "KIS US-equity live confirmation token is missing"
        allowed_accounts = {
            value.strip()
            for value in settings.kis_us_account_allowlist.split(",")
            if value.strip()
        }
        if not settings.kis_account_number or settings.kis_account_number not in allowed_accounts:
            return "KIS US-equity live account is not in the configured allowlist"
    return None


def _kis_us_session_route(command: PlaceOrderRequest) -> dict[str, object] | None:
    if command.provider != "kis" or command.asset_class != "us_equity":
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    observed_at = _route_now(metadata)
    local = observed_at.astimezone(ZoneInfo("America/New_York"))
    if is_us_regular_session(local):
        return None
    next_open = next_us_regular_open(observed_at)
    return {
        "action": "defer",
        "exchange": command.exchange,
        "session": "us_closed",
        "ord_dvsn": None,
        "effective_order_type": command.order_type,
        "price_source": "command_limit_price",
        "next_retry_at": next_open.isoformat(),
        "reason": (
            "deferred KIS US-equity order outside New York regular session "
            f"until {next_open.isoformat()}"
        ),
    }


def _validate_trusted_order_origin(*, command: PlaceOrderRequest, request_source: str) -> str | None:
    if not request_source.startswith("alpaca_"):
        return None
    metadata = command.metadata if isinstance(command.metadata, dict) else {}
    expected_origin = request_source.split(":", 1)[0]
    if expected_origin not in {"alpaca_crypto_strategy", "alpaca_us_equity_strategy"}:
        return f"unrecognized Alpaca autonomous order source: {expected_origin}"
    if metadata.get("autonomous_strategy_order") is not True:
        return "Alpaca autonomous strategy orders must carry explicit strategy origin metadata"
    if metadata.get("order_origin") != expected_origin:
        return "Alpaca autonomous strategy order origin metadata does not match request source"
    if command.asset_class == "us_equity" and expected_origin != "alpaca_us_equity_strategy":
        return "Alpaca US-equity strategy orders must use the US-equity strategy origin"
    if command.asset_class == "crypto" and expected_origin != "alpaca_crypto_strategy":
        return "Alpaca crypto strategy orders must use the crypto strategy origin"
    return None


def _is_alpaca_us_equity_strategy_order(request_source: str) -> bool:
    return request_source == "alpaca_us_equity_strategy" or request_source.startswith(
        "alpaca_us_equity_strategy:"
    )


def _evaluate_new_buy_account_gate(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> AccountDrawdownDecision | None:
    if not settings.account_drawdown_gate_enabled:
        return None
    if not _is_new_buy_order(command=command, plan_item=plan_item):
        return None
    if command.provider != "kis" and adapter.provider != "kis":
        return None
    try:
        session_state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            source="account_drawdown_gate",
            reason=f"load broker account snapshot before new buy {command.idempotency_key}",
        )
        if command.asset_class == "us_equity":
            snapshots = [
                (
                    exchange,
                    adapter.get_account_snapshot(
                        session_state.access_token or "",
                        symbols=[command.symbol],
                        exchange=exchange,
                    ),
                )
                for exchange in sorted(("NASD", "NYSE", "AMEX"))
            ]
            snapshot = _merge_us_account_gate_snapshots(snapshots)
        else:
            try:
                snapshot = adapter.get_account_snapshot(
                    session_state.access_token or "",
                    symbols=[command.symbol],
                    exchange=command.exchange or "KRX",
                )
            except TypeError:
                snapshot = adapter.get_account_snapshot(session_state.access_token or "")
    except Exception as exc:
        return account_drawdown_uncertain_decision(
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
            reason=f"broker account snapshot unavailable before new buy: {exc.__class__.__name__}: {exc}",
        )

    observed_at = datetime.now(UTC)
    if command.asset_class == "us_equity":
        notional = _order_notional(command=command, plan_item=plan_item)
        if notional is None:
            return account_drawdown_uncertain_decision(
                order_idempotency_key=command.idempotency_key,
                execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
                reason="KIS US-equity account gate requires deterministic USD order notional",
            )
        return evaluate_us_equity_account_gate(
            settings=settings,
            db=db,
            snapshot=snapshot,
            order_notional=notional,
            order_idempotency_key=command.idempotency_key,
            execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
            now=observed_at,
        )
    return evaluate_account_drawdown_gate(
        settings=settings,
        snapshot=snapshot_with_raw_balance_holdings(
            snapshot,
            as_of=observed_at,
            max_age_seconds=settings.exec_agent_max_account_snapshot_age_seconds,
        ),
        order_idempotency_key=command.idempotency_key,
        execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
        now=observed_at,
    )


def _merge_us_account_gate_snapshots(
    routed_snapshots: list[tuple[str, BrokerAccountSnapshot]],
) -> BrokerAccountSnapshot:
    if len(routed_snapshots) != 3:
        raise ValueError("KIS US-equity account gate requires all three exchange snapshots")
    snapshots = [snapshot for _exchange, snapshot in routed_snapshots]
    observed_at = min(snapshot.observed_at for snapshot in snapshots)
    cash_values = [snapshot.available_cash for snapshot in snapshots if snapshot.available_cash is not None]
    holdings_by_key: dict[tuple[str, str], BrokerAccountHolding] = {}
    for requested_exchange, snapshot in routed_snapshots:
        for holding in snapshot.holdings:
            holding_exchange = _us_holding_exchange(holding.payload) or requested_exchange
            key = (holding_exchange, holding.symbol)
            existing = holdings_by_key.get(key)
            if existing is None:
                holdings_by_key[key] = holding
                continue
            if (
                existing.quantity != holding.quantity
                or existing.average_price != holding.average_price
                or existing.sellable_quantity != holding.sellable_quantity
            ):
                raise ValueError(
                    "conflicting duplicate KIS US holding for "
                    f"{holding_exchange}:{holding.symbol}"
                )
    open_orders_by_key: dict[tuple[str, str], BrokerAccountOpenOrder] = {}
    for requested_exchange, snapshot in routed_snapshots:
        for order in snapshot.open_orders:
            order_exchange = _us_holding_exchange(order.payload) or requested_exchange
            identity = order.broker_order_id or (
                f"{order.symbol}:{order.side}:{order.quantity}:{order.limit_price}"
            )
            key = (order_exchange, identity)
            existing = open_orders_by_key.get(key)
            if existing is None:
                open_orders_by_key[key] = order
                continue
            if existing.remaining_quantity != order.remaining_quantity:
                raise ValueError(
                    "conflicting duplicate KIS US open order for "
                    f"{order_exchange}:{identity}"
                )
    return BrokerAccountSnapshot(
        available_cash=min(cash_values) if cash_values else None,
        observed_at=observed_at,
        source="kis:us_equity_account_gate_aggregate",
        unrealized_pnl=sum(
            (holding.unrealized_pnl or Decimal("0") for holding in holdings_by_key.values()),
            Decimal("0"),
        ),
        holdings=list(holdings_by_key.values()),
        open_orders=list(open_orders_by_key.values()),
        symbol_buying_power={
            symbol: buying_power
            for snapshot in snapshots
            for symbol, buying_power in snapshot.symbol_buying_power.items()
        },
        payload={
            "currency": "USD",
            "account_gate_exchange_coverage": ["AMEX", "NASD", "NYSE"],
            "realized_pnl_coverage": "degraded_local_execution_fills_only",
            "snapshot_sources": [snapshot.source for snapshot in snapshots],
        },
    )


def _us_holding_exchange(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ovrs_excg_cd", "OVRS_EXCG_CD", "exchange", "excg_cd"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            normalized = value.strip().upper()
            return {
                "NAS": "NASD",
                "NASDAQ": "NASD",
                "NYS": "NYSE",
                "AMS": "AMEX",
            }.get(normalized, normalized)
    return None


def _is_new_buy_order(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> bool:
    if command.side != "buy":
        return False
    purpose = _order_purpose(
        command=command,
        plan_item=plan_item,
        metadata=dict(command.metadata or {}),
    )
    if purpose in {"risk_reduction", "reduce_risk", "risk_reducing", "protective_hedge"}:
        return False
    if plan_item is not None and plan_item.intent in {
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
    }:
        return False
    return True


def _persist_account_gate_block(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter,
    command: PlaceOrderRequest,
    request_source: str,
    plan_item: ExecutionPlanItem | None,
    route_decision: RouteDecision | None,
    decision: AccountDrawdownDecision,
) -> PlaceBrokerOrderResult:
    observed_at = datetime.now(UTC)
    order = BrokerOrder(
        broker_name=adapter.provider,
        execution_plan_item_id=plan_item.id if plan_item else None,
        mode=settings.runtime_mode,
        idempotency_key=command.idempotency_key,
        symbol=command.symbol,
        side=command.side,
        status=NormalizedBrokerOrderStatus.REJECTED.value,
        quantity=command.quantity,
        base_quantity=command.base_quantity,
        quote_notional=command.quote_notional,
        quantity_precision=command.quantity_precision,
        price_precision=command.price_precision,
        venue=command.venue,
        asset_class=command.asset_class,
        limit_price=command.limit_price,
        request_metadata={
            "source": request_source,
            "requested_at": observed_at.isoformat(),
            "time_in_force": command.time_in_force,
            "provider": command.provider,
            "asset_class": command.asset_class,
            "venue": command.venue,
            "sizing": _sizing_payload(command),
            "account_drawdown_gate": decision.summary,
            **({"metadata": command.metadata} if command.metadata is not None else {}),
            **({"route": _route_payload(route_decision)} if route_decision is not None else {}),
        },
        response_metadata={"account_drawdown_gate": decision.summary},
        last_error_classification="validation",
        last_error_code=decision.gate_name,
        last_error_message=decision.reason,
        last_error_retryable=False,
        raw_broker_payload=decision.payload,
        last_broker_sync_at=observed_at,
    )
    db.add(order)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        duplicate = _load_order_by_idempotency_key(
            settings=settings,
            db=db,
            idempotency_key=command.idempotency_key,
        )
        if duplicate is None:
            raise
        return PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "order with this idempotency_key already exists",
                "order": serialize_broker_order(duplicate, include_events=True),
            },
        )

    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="submission_blocked",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED.value,
            broker_status=decision.gate_name,
            message=decision.reason,
            request_payload=_order_command_payload(command),
            response_payload=decision.payload,
            payload={
                "provider": adapter.provider,
                "account_drawdown_gate": decision.summary,
                "sells_risk_reduction_allowed": decision.sells_risk_reduction_allowed,
            },
            observed_at=observed_at,
        )
    )
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="watch_buy_triggered_blocked" if plan_item.status.startswith("watch_") else "new_buy_blocked",
            source="account_drawdown_gate",
            reason=decision.reason,
            payload={
                "broker_order_pk": order.id,
                "idempotency_key": order.idempotency_key,
                "gate": decision.summary,
            },
        )
    record_account_drawdown_block(
        db=db,
        execution_plan_item_id=plan_item.id if plan_item is not None else command.execution_plan_item_id,
        decision=decision,
        occurred_at=observed_at,
    )
    db.commit()
    persisted = _load_order_by_idempotency_key(
        settings=settings,
        db=db,
        idempotency_key=command.idempotency_key,
    )
    return PlaceBrokerOrderResult(
        status_code=409,
        payload={
            "duplicate": False,
            "message": "new buy order blocked by account drawdown gate",
            "blocked": True,
            "block_reason": decision.gate_name,
            "account_drawdown_gate": decision.summary,
            "sells_risk_reduction_allowed": decision.sells_risk_reduction_allowed,
            "order": serialize_broker_order(persisted or order, include_events=True),
        },
    )


def _order_command_payload(command: PlaceOrderRequest) -> dict[str, object]:
    return command.model_dump(mode="json")


def _merge_order_request_metadata(
    *,
    base: dict[str, object] | None,
    command: PlaceOrderRequest,
    request_source: str,
    route_decision: RouteDecision | None = None,
) -> dict[str, object]:
    metadata = dict(base or {})
    metadata["source"] = request_source
    metadata["trusted_order_origin"] = _trusted_order_origin(request_source)
    metadata["time_in_force"] = command.time_in_force
    metadata["provider"] = command.provider
    metadata["asset_class"] = command.asset_class
    metadata["venue"] = command.venue
    metadata["sizing"] = _sizing_payload(command)
    if command.metadata is not None:
        metadata["metadata"] = command.metadata
    if route_decision is not None:
        metadata["route"] = _route_payload(route_decision)
    return metadata


def _trusted_order_origin(request_source: str) -> str:
    return request_source.split(":", 1)[0] if request_source else "unknown"


def _resolve_route_decision(
    *,
    settings: Settings,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> RouteDecision | None:
    if command.provider != "kis":
        return None
    if command.asset_class == "us_equity":
        return None
    if not settings.domestic_order_session_routing_enabled:
        return None
    metadata = dict(command.metadata or {})
    if plan_item is not None and isinstance(plan_item.risk_context, dict):
        for key in ("nxt_eligible", "order_purpose", "purpose"):
            if key in plan_item.risk_context and key not in metadata:
                metadata[key] = plan_item.risk_context[key]
    purpose = _order_purpose(command=command, plan_item=plan_item, metadata=metadata)
    return resolve_domestic_order_route(
        settings=settings,
        now=_route_now(metadata),
        symbol=command.symbol,
        side=command.side,
        purpose=purpose,
        desired_order_type=command.order_type,
        limit_price=command.limit_price,
        requested_exchange=command.exchange,
        metadata=metadata,
    )


def _routed_order_command(
    *,
    command: PlaceOrderRequest,
    route_decision: RouteDecision | None,
) -> PlaceOrderRequest:
    if route_decision is None:
        return command
    metadata = dict(command.metadata or {})
    metadata["route"] = _route_payload(route_decision)
    return command.model_copy(
        update={
            "exchange": route_decision.exchange,
            "order_type": route_decision.effective_order_type,
            "limit_price": command.limit_price,
            "metadata": metadata,
        }
    )


def _route_payload(route_decision: RouteDecision | None) -> dict[str, object] | None:
    if route_decision is None:
        return None
    return {
        "action": route_decision.action,
        "exchange": route_decision.exchange,
        "session": route_decision.session,
        "ord_dvsn": route_decision.ord_dvsn,
        "effective_order_type": route_decision.effective_order_type,
        "price_source": route_decision.price_source,
        "next_retry_at": (
            route_decision.next_retry_at.isoformat()
            if route_decision.next_retry_at is not None
            else None
        ),
        "reason": route_decision.reason,
    }


def _sizing_payload(command: PlaceOrderRequest) -> dict[str, object]:
    return {
        "quantity": command.quantity,
        "base_quantity": str(command.base_quantity) if command.base_quantity is not None else None,
        "quote_notional": str(command.quote_notional) if command.quote_notional is not None else None,
        "limit_price": str(command.limit_price) if command.limit_price is not None else None,
        "quantity_precision": command.quantity_precision,
        "price_precision": command.price_precision,
    }


def _record_upbit_paper_simulated_fill(
    *,
    settings: Settings,
    db: Session,
    order: BrokerOrder,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    observed_at: datetime,
) -> None:
    if command.provider != "upbit" or settings.runtime_mode != "paper":
        return
    fill_price = _simulated_fill_price(command=command, plan_item=plan_item)
    if fill_price is None:
        return
    base_quantity = _simulated_base_quantity(command=command, fill_price=fill_price)
    quote_notional = _order_notional(command=command, plan_item=plan_item)
    if base_quantity is None or base_quantity <= 0:
        return

    apply_broker_order_status(order=order, status=NormalizedBrokerOrderStatus.FILLED.value)
    order.last_broker_sync_at = observed_at
    if plan_item is not None:
        set_plan_item_status(
            db=db,
            plan_item=plan_item,
            status="filled",
            source="broker_order_service",
            reason="Upbit paper order filled by local simulator",
            payload={"broker_order_pk": order.id, "idempotency_key": order.idempotency_key},
            occurred_at=observed_at,
        )

    fill = ExecutionFill(
        idempotency_key=execution_fill_idempotency_key(
            broker_order_pk=order.id,
            broker_fill_id=f"paper-fill:{order.idempotency_key}",
            symbol=order.symbol,
            side=order.side,
            filled_at=observed_at,
            price=fill_price,
            quantity=0,
        ),
        broker_order_pk=order.id,
        broker_fill_id=f"paper-fill:{order.idempotency_key}",
        symbol=order.symbol,
        side=order.side,
        quantity=0,
        base_quantity=base_quantity,
        quote_notional=quote_notional,
        price=fill_price,
        filled_at=observed_at,
        payload={
            "provider": "upbit",
            "mode": "paper",
            "simulated": True,
            "asset_class": "crypto",
            "venue": "upbit",
            "base_quantity": str(base_quantity),
            "quote_notional": str(quote_notional) if quote_notional is not None else None,
            "price": str(fill_price),
        },
    )
    db.add(fill)
    db.add(
        BrokerOrderEvent(
            broker_order_pk=order.id,
            event_type="paper_fill_simulated",
            event_source=BrokerEventSource.SYSTEM.value,
            normalized_status=NormalizedBrokerOrderStatus.FILLED.value,
            broker_status="paper_filled",
            message="Upbit paper fill simulated",
            request_payload=_order_command_payload(command),
            response_payload=fill.payload,
            payload={"provider": "upbit", "paper": True, "simulated": True},
            observed_at=observed_at,
        )
    )
    _apply_upbit_paper_position(
        settings=settings,
        db=db,
        symbol=order.symbol,
        side=order.side,
        base_quantity=base_quantity,
        average_price=fill_price,
        observed_at=observed_at,
        fill_payload=fill.payload,
    )


def _apply_upbit_paper_position(
    *,
    settings: Settings,
    db: Session,
    symbol: str,
    side: str,
    base_quantity: Decimal,
    average_price: Decimal,
    observed_at: datetime,
    fill_payload: dict[str, object],
) -> None:
    position = db.scalar(
        select(ExecutionPosition).where(
            ExecutionPosition.mode == settings.runtime_mode,
            ExecutionPosition.symbol == symbol,
        )
    )
    if position is None:
        position = ExecutionPosition(
            id=new_id(),
            mode=settings.runtime_mode,
            symbol=symbol,
            quantity=0,
            base_quantity=Decimal("0"),
            average_price=None,
            updated_from="upbit_paper_simulated_fill",
            last_reconciled_at=observed_at,
            raw_payload={"provider": "upbit", "paper": True, "simulated": True},
        )
        db.add(position)
    current = position.base_quantity or Decimal("0")
    current_average_price = position.average_price
    if side == "buy":
        next_quantity = current + base_quantity
        if current > 0 and current_average_price is not None:
            next_average_price = (
                (current * current_average_price) + (base_quantity * average_price)
            ) / next_quantity
        else:
            next_average_price = average_price
    else:
        next_quantity = max(current - base_quantity, Decimal("0"))
        next_average_price = current_average_price if next_quantity > 0 else None
    position.quantity = 0
    position.base_quantity = next_quantity
    position.average_price = next_average_price
    position.updated_from = "upbit_paper_simulated_fill"
    position.last_reconciled_at = observed_at
    position.raw_payload = {
        "provider": "upbit",
        "paper": True,
        "simulated": True,
        "last_fill": fill_payload,
    }


def _simulated_fill_price(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> Decimal | None:
    if command.limit_price is not None:
        return command.limit_price
    risk_context = plan_item.risk_context if plan_item is not None else None
    if not isinstance(risk_context, dict):
        return None
    for key in ("simulated_fill_price", "paper_fill_price", "reference_price", "estimated_reference_price"):
        value = risk_context.get(key)
        parsed = _positive_decimal(value)
        if parsed is not None:
            return parsed
    provider_metadata = risk_context.get("provider_metadata")
    if isinstance(provider_metadata, dict):
        for key in ("simulated_fill_price", "paper_fill_price", "reference_price"):
            parsed = _positive_decimal(provider_metadata.get(key))
            if parsed is not None:
                return parsed
    return None


def _simulated_base_quantity(
    *,
    command: PlaceOrderRequest,
    fill_price: Decimal,
) -> Decimal | None:
    if command.base_quantity is not None:
        return command.base_quantity
    if command.quote_notional is None:
        return None
    return (command.quote_notional / fill_price).quantize(Decimal("0.000000000001"), rounding=ROUND_DOWN)


def _order_notional(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
) -> Decimal | None:
    if command.quote_notional is not None:
        return command.quote_notional
    price = command.limit_price or _simulated_fill_price(command=command, plan_item=plan_item)
    if price is None:
        return None
    if command.quantity is not None:
        return Decimal(command.quantity) * price
    if command.base_quantity is None:
        return None
    return (command.base_quantity * price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _positive_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value).strip())
    except Exception:
        return None
    if parsed <= 0:
        return None
    return parsed


def _order_purpose(
    *,
    command: PlaceOrderRequest,
    plan_item: ExecutionPlanItem | None,
    metadata: dict[str, object],
) -> str:
    raw_purpose = metadata.get("order_purpose") or metadata.get("purpose")
    if isinstance(raw_purpose, str) and raw_purpose.strip():
        return raw_purpose.strip().lower()
    if plan_item is not None and plan_item.intent == "stop_sell":
        return "protective_stop"
    if plan_item is not None:
        return plan_item.intent
    return "manual_order"


def _route_now(metadata: dict[str, object]) -> datetime:
    raw_value = metadata.get("route_now") or metadata.get("observed_at")
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    return datetime.now(UTC)


def _plan_item_status_for_order_status(order_status: str) -> str:
    return plan_item_status_for_broker_order_status(order_status) or order_status


def _response_event_observed_at(
    *,
    observed_at: datetime,
    request_observed_at: datetime,
) -> datetime:
    minimum = request_observed_at + timedelta(microseconds=1)
    if observed_at >= minimum:
        return observed_at
    return minimum
