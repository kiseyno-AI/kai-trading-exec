from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from app.core.config import Settings
from app.services.nxt_eligibility import is_nxt_eligible_symbol

KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class RouteDecision:
    action: str
    exchange: str
    session: str
    ord_dvsn: str | None
    effective_order_type: str
    price_source: str
    next_retry_at: datetime | None
    reason: str


def resolve_domestic_order_route(
    *,
    settings: Settings,
    now: datetime,
    symbol: str,
    side: str,
    purpose: str,
    desired_order_type: str,
    limit_price: Decimal | None,
    requested_exchange: str,
    metadata: dict[str, object] | None = None,
) -> RouteDecision:
    local_now = _localize(now)
    metadata = metadata or {}
    session = _session_for(local_now)
    nxt_eligible = _is_nxt_eligible(settings=settings, symbol=symbol, metadata=metadata)
    route_preference = settings.nxt_order_route_preference.strip().lower()
    protective = purpose in {"protective_stop", "stop_sell"}

    if session == "krx_regular":
        if _prefer_nxt(route_preference) and nxt_eligible and settings.allow_nxt_main_orders:
            return _submit(
                local_now=local_now,
                exchange="NXT" if route_preference == "nxt" else "SOR",
                session="nxt_main",
                order_type=desired_order_type,
                limit_price=limit_price,
                side=side,
                protective=protective,
                reason="nxt_main_preferred",
            )
        return _submit(
            local_now=local_now,
            exchange="KRX" if requested_exchange != "SOR" else "SOR",
            session="krx_regular",
            order_type=desired_order_type,
            limit_price=limit_price,
            side=side,
            protective=protective,
            reason="krx_regular_session",
        )

    if session == "krx_opening_call_auction":
        return _submit(
            local_now=local_now,
            exchange="KRX",
            session="krx_opening_call_auction",
            order_type=_limit_only_order_type(desired_order_type),
            limit_price=limit_price,
            side=side,
            protective=protective,
            reason="krx_opening_call_auction",
        )

    if session == "nxt_pre":
        if nxt_eligible and settings.allow_nxt_pre_orders:
            return _submit(
                local_now=local_now,
                exchange="NXT",
                session="nxt_pre",
                order_type=_limit_only_order_type(desired_order_type),
                limit_price=limit_price,
                side=side,
                protective=protective,
                reason="nxt_pre_session",
            )
        return _defer_or_block(
            local_now=local_now,
            side=side,
            protective=protective,
            reason="nxt_pre_unavailable_for_symbol_or_policy",
        )

    if session == "krx_preopen_close":
        if _prefer_nxt(route_preference) and nxt_eligible and settings.allow_nxt_pre_orders:
            return _submit(
                local_now=local_now,
                exchange="NXT",
                session="nxt_pre",
                order_type=_limit_only_order_type(desired_order_type),
                limit_price=limit_price,
                side=side,
                protective=protective,
                reason="nxt_pre_preferred",
            )
        if settings.allow_krx_pre_open_offhours_orders:
            return RouteDecision(
                action="submit",
                exchange="KRX",
                session="krx_preopen_close",
                ord_dvsn="05",
                effective_order_type="limit",
                price_source="closing_price_session",
                next_retry_at=None,
                reason="krx_preopen_close_enabled",
            )
        return _defer_or_block(
            local_now=local_now,
            side=side,
            protective=protective,
            reason="krx_preopen_close_disabled",
        )

    if session in {"nxt_after_acceptance", "nxt_after"}:
        if nxt_eligible and settings.allow_nxt_after_orders and _after_hours_side_allowed(settings, side):
            return _submit(
                local_now=local_now,
                exchange="NXT",
                session="nxt_after",
                order_type=_limit_only_order_type(desired_order_type),
                limit_price=limit_price,
                side=side,
                protective=protective,
                reason="nxt_after_session",
            )
        if session == "nxt_after" and settings.allow_krx_after_hours_closing_orders and _after_hours_side_allowed(settings, side):
            return RouteDecision(
                action="submit",
                exchange="KRX",
                session="krx_after_close",
                ord_dvsn="06",
                effective_order_type="limit",
                price_source="closing_price_session",
                next_retry_at=None,
                reason="krx_after_hours_closing_enabled",
            )
        return _defer_or_block(
            local_now=local_now,
            side=side,
            protective=protective,
            reason="after_hours_route_unavailable",
        )

    if session == "krx_after_single_price":
        if nxt_eligible and settings.allow_nxt_after_orders and _after_hours_side_allowed(settings, side):
            return _submit(
                local_now=local_now,
                exchange="NXT",
                session="nxt_after",
                order_type=_limit_only_order_type(desired_order_type),
                limit_price=limit_price,
                side=side,
                protective=protective,
                reason="nxt_after_preferred_for_nxt_symbol",
            )
        if settings.allow_krx_after_hours_single_price_orders and _after_hours_side_allowed(settings, side):
            return RouteDecision(
                action="submit",
                exchange="KRX",
                session="krx_after_single_price",
                ord_dvsn="07",
                effective_order_type="limit",
                price_source="single_price_session",
                next_retry_at=None,
                reason="krx_after_hours_single_price_enabled",
            )
        return _defer_or_block(
            local_now=local_now,
            side=side,
            protective=protective,
            reason="after_single_price_route_unavailable",
        )

    return _defer_or_block(
        local_now=local_now,
        side=side,
        protective=protective,
        reason=f"{session}_not_orderable",
    )


def _session_for(now: datetime) -> str:
    local_time = now.timetz().replace(tzinfo=None)
    if time(8, 0) <= local_time < time(8, 30):
        return "nxt_pre"
    if time(8, 30) <= local_time < time(8, 40):
        return "krx_preopen_close"
    if time(8, 40) <= local_time < time(8, 50):
        return "nxt_pre"
    if time(8, 50) <= local_time < time(9, 0):
        return "krx_opening_call_auction"
    if time(9, 0) <= local_time < time(15, 30):
        return "krx_regular"
    if time(15, 30) <= local_time < time(15, 40):
        return "nxt_after_acceptance"
    if time(15, 40) <= local_time < time(16, 0):
        return "nxt_after"
    if time(16, 0) <= local_time < time(18, 0):
        return "krx_after_single_price"
    if time(18, 0) <= local_time < time(20, 0):
        return "nxt_after"
    return "closed"


def _submit(
    *,
    local_now: datetime,
    exchange: str,
    session: str,
    order_type: str,
    limit_price: Decimal | None,
    side: str,
    protective: bool,
    reason: str,
) -> RouteDecision:
    effective_order_type = _limit_only_order_type(order_type) if session != "krx_regular" else order_type
    if effective_order_type == "limit" and limit_price is None:
        return _defer_or_block(
            local_now=local_now,
            side=side,
            protective=protective,
            reason=f"{reason}:limit_required",
        )
    return RouteDecision(
        action="submit",
        exchange=exchange,
        session=session,
        ord_dvsn=_regular_order_division(effective_order_type),
        effective_order_type=effective_order_type,
        price_source="market" if effective_order_type == "market" else "explicit_limit",
        next_retry_at=None,
        reason=reason,
    )


def _defer_or_block(
    *,
    local_now: datetime,
    side: str,
    protective: bool,
    reason: str,
) -> RouteDecision:
    next_retry_at = _next_regular_session(local_now).astimezone(UTC)
    return RouteDecision(
        action="defer" if protective or side == "sell" else "block",
        exchange="KRX",
        session="closed",
        ord_dvsn=None,
        effective_order_type="limit",
        price_source="none",
        next_retry_at=next_retry_at,
        reason=reason,
    )


def _regular_order_division(order_type: str) -> str:
    return "01" if order_type == "market" else "00"


def _limit_only_order_type(order_type: str) -> str:
    return "limit" if order_type == "market" else order_type


def _localize(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=KST)
    return now.astimezone(KST)


def _next_regular_session(local_now: datetime) -> datetime:
    candidate = local_now.replace(hour=9, minute=0, second=0, microsecond=0)
    if local_now.time() >= time(9, 0):
        candidate += timedelta(days=1)
    return candidate


def _prefer_nxt(route_preference: str) -> bool:
    return route_preference in {"nxt", "sor"}


def _after_hours_side_allowed(settings: Settings, side: str) -> bool:
    return settings.allow_after_hours_sell_orders if side == "sell" else settings.allow_after_hours_buy_orders


def _is_nxt_eligible(
    *,
    settings: Settings,
    symbol: str,
    metadata: dict[str, object],
) -> bool:
    raw_metadata = metadata.get("nxt_eligible")
    if isinstance(raw_metadata, bool):
        return raw_metadata
    return is_nxt_eligible_symbol(settings=settings, symbol=symbol)
