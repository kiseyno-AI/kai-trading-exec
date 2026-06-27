from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot
from app.core.config import Settings
from app.models.runtime import BrokerOrder, ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit


BLOCK_EVENT_TYPE = "account_drawdown_new_buy_blocked"


@dataclass(frozen=True)
class AccountDrawdownDecision:
    allowed: bool
    gate_name: str | None
    reason: str | None
    current_metric: Decimal | None
    threshold: Decimal | None
    baseline_value: Decimal | None
    baseline_timestamp: str | None
    sells_risk_reduction_allowed: bool
    summary: dict[str, object]
    payload: dict[str, object]


def evaluate_account_drawdown_gate(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
    order_idempotency_key: str,
    execution_plan_item_id: str | None,
    now: datetime | None = None,
) -> AccountDrawdownDecision:
    observed_at = now or datetime.now(UTC)
    metrics = _metrics(snapshot)
    baseline_timestamp = snapshot.observed_at.astimezone(UTC).isoformat()
    gates = [
        _daily_total_asset_pct_gate(settings=settings, metrics=metrics),
        _daily_total_asset_abs_gate(settings=settings, metrics=metrics),
        _weekly_total_asset_gate(settings=settings, metrics=metrics),
        _unrealized_abs_gate(settings=settings, metrics=metrics),
        _unrealized_pct_gate(settings=settings, metrics=metrics),
        _severe_holding_gate(settings=settings, snapshot=snapshot),
    ]
    blockers = [gate for gate in gates if gate is not None]
    if blockers:
        gate = blockers[0]
        summary = {
            "gate_name": gate["gate_name"],
            "reason": gate["reason"],
            "current_metric": _decimal_text(gate.get("current_metric")),
            "threshold": _decimal_text(gate.get("threshold")),
            "baseline_value": _decimal_text(gate.get("baseline_value")),
            "baseline_timestamp": gate.get("baseline_timestamp") or baseline_timestamp,
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "sells_risk_reduction_allowed": True,
        }
        return AccountDrawdownDecision(
            allowed=False,
            gate_name=str(gate["gate_name"]),
            reason=str(gate["reason"]),
            current_metric=_decimal_or_none(gate.get("current_metric")),
            threshold=_decimal_or_none(gate.get("threshold")),
            baseline_value=_decimal_or_none(gate.get("baseline_value")),
            baseline_timestamp=str(summary["baseline_timestamp"]),
            sells_risk_reduction_allowed=True,
            summary=summary,
            payload={
                "blocked": True,
                "evaluated_at": observed_at.isoformat(),
                "order_idempotency_key": order_idempotency_key,
                "execution_plan_item_id": execution_plan_item_id,
                "decision": summary,
                "all_blockers": [_json_safe(gate) for gate in blockers],
                "metrics": _json_safe(metrics),
                "raw_balance_payload": snapshot.payload.get("balance") if isinstance(snapshot.payload, dict) else None,
                "holdings": [_holding_payload(holding) for holding in snapshot.holdings],
            },
        )

    return AccountDrawdownDecision(
        allowed=True,
        gate_name=None,
        reason=None,
        current_metric=None,
        threshold=None,
        baseline_value=metrics.get("daily_baseline"),
        baseline_timestamp=baseline_timestamp,
        sells_risk_reduction_allowed=True,
        summary={
            "blocked": False,
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
        },
        payload={
            "blocked": False,
            "evaluated_at": observed_at.isoformat(),
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "metrics": _json_safe(metrics),
        },
    )


def record_account_drawdown_block(
    *,
    db: Session,
    execution_plan_item_id: str | None,
    decision: AccountDrawdownDecision,
    occurred_at: datetime | None = None,
) -> None:
    if execution_plan_item_id is None:
        return
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=execution_plan_item_id,
            event_type=BLOCK_EVENT_TYPE,
            source="broker_order_service",
            summary=decision.summary,
            payload=decision.payload,
            error_message=decision.reason,
            occurred_at=occurred_at or datetime.now(UTC),
        )
    )


def account_drawdown_uncertain_decision(
    *,
    order_idempotency_key: str,
    execution_plan_item_id: str | None,
    reason: str,
    now: datetime | None = None,
) -> AccountDrawdownDecision:
    observed_at = now or datetime.now(UTC)
    summary = {
        "gate_name": "account_drawdown_snapshot_unavailable",
        "reason": reason,
        "current_metric": None,
        "threshold": None,
        "baseline_value": None,
        "baseline_timestamp": observed_at.isoformat(),
        "order_idempotency_key": order_idempotency_key,
        "execution_plan_item_id": execution_plan_item_id,
        "sells_risk_reduction_allowed": True,
    }
    return AccountDrawdownDecision(
        allowed=False,
        gate_name="account_drawdown_snapshot_unavailable",
        reason=reason,
        current_metric=None,
        threshold=None,
        baseline_value=None,
        baseline_timestamp=observed_at.isoformat(),
        sells_risk_reduction_allowed=True,
        summary=summary,
        payload={
            "blocked": True,
            "evaluated_at": observed_at.isoformat(),
            "order_idempotency_key": order_idempotency_key,
            "execution_plan_item_id": execution_plan_item_id,
            "decision": summary,
            "metrics": {},
        },
    )


def latest_account_drawdown_gate_state(
    *,
    settings: Settings,
    db: Session,
) -> dict[str, object]:
    audit = db.scalar(
        select(ExecutionPlanItemAudit)
        .join(ExecutionPlanItemAudit.plan_item)
        .join(ExecutionPlanItem.plan)
        .where(
            ExecutionPlan.mode == settings.runtime_mode,
            ExecutionPlanItemAudit.event_type == BLOCK_EVENT_TYPE,
        )
        .order_by(ExecutionPlanItemAudit.occurred_at.desc())
        .limit(1)
    )
    order = db.scalar(
        select(BrokerOrder)
        .where(
            BrokerOrder.mode == settings.runtime_mode,
            BrokerOrder.response_metadata["account_drawdown_gate"].is_not(None),
        )
        .order_by(BrokerOrder.last_broker_sync_at.desc(), BrokerOrder.created_at.desc())
        .limit(1)
    )
    if audit is None and order is None:
        return {"gated": False, "latest_decision": None}
    if audit is not None and (
        order is None
        or audit.occurred_at >= (order.last_broker_sync_at or order.created_at)
    ):
        return {
            "gated": True,
            "latest_decision": {
                "source": "execution_plan_item_audit",
                "occurred_at": audit.occurred_at.isoformat(),
                "execution_plan_item_id": audit.execution_plan_item_id,
                "summary": audit.summary or {},
                "payload": audit.payload or {},
            },
        }
    return {
        "gated": True,
        "latest_decision": {
            "source": "broker_order",
            "occurred_at": (order.last_broker_sync_at or order.created_at).isoformat(),
            "order_id": order.id,
            "order_idempotency_key": order.idempotency_key,
            "execution_plan_item_id": order.execution_plan_item_id,
            "summary": (order.response_metadata or {}).get("account_drawdown_gate") or {},
            "payload": order.raw_broker_payload or {},
        },
    }


def account_drawdown_gate_report_lines(gate_state: dict[str, object]) -> list[str]:
    latest = gate_state.get("latest_decision")
    if not gate_state.get("gated") or not isinstance(latest, dict):
        return ["Account drawdown gate: not currently gated by the latest audited decision."]
    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    return [
        (
            "Account drawdown gate: gated="
            f"{gate_state.get('gated')}, gate={summary.get('gate_name')}, "
            f"metric={summary.get('current_metric')}, threshold={summary.get('threshold')}, "
            f"baseline={summary.get('baseline_value')}, "
            f"sells/risk-reduction allowed={summary.get('sells_risk_reduction_allowed')}"
        )
    ]


def account_drawdown_gate_prompt_summary(gate_state: dict[str, object]) -> str:
    latest = gate_state.get("latest_decision")
    if not gate_state.get("gated") or not isinstance(latest, dict):
        return "Account drawdown gate is not active in the latest audited decision."
    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    return (
        f"Account drawdown gate active: {summary.get('gate_name')} "
        f"metric={summary.get('current_metric')} threshold={summary.get('threshold')} "
        f"baseline={summary.get('baseline_value')}; sells and risk-reduction remain allowed."
    )


def _field_or_payload_decimal(
    field_value: Decimal | None,
    payload: dict[str, Any],
    keys: tuple[str, ...],
) -> Decimal | None:
    if field_value is not None:
        return field_value
    return _payload_decimal(payload, keys)


def _metrics(snapshot: BrokerAccountSnapshot) -> dict[str, Decimal | None]:
    total_asset = _field_or_payload_decimal(
        snapshot.total_asset_value,
        snapshot.payload,
        ("total_asset_value", "tot_evlu_amt", "nass_amt"),
    )
    daily_baseline = _field_or_payload_decimal(
        snapshot.previous_total_asset_value,
        snapshot.payload,
        ("previous_total_asset_value", "bfdy_tot_asst_evlu_amt"),
    )
    asset_change = _field_or_payload_decimal(
        snapshot.asset_change_amount,
        snapshot.payload,
        ("asset_change_amount", "asst_icdc_amt"),
    )
    unrealized_pnl = _field_or_payload_decimal(
        snapshot.unrealized_pnl,
        snapshot.payload,
        ("unrealized_pnl", "evlu_pfls_smtl_amt"),
    )
    daily_drawdown = None
    daily_drawdown_pct = None
    if total_asset is not None and daily_baseline is not None and daily_baseline > 0:
        daily_drawdown = daily_baseline - total_asset
        daily_drawdown_pct = (daily_drawdown / daily_baseline * Decimal("100")).quantize(Decimal("0.0001"))
    if daily_drawdown is None and asset_change is not None and asset_change < 0:
        daily_drawdown = -asset_change
    unrealized_loss_pct = None
    if unrealized_pnl is not None and total_asset is not None and total_asset > 0 and unrealized_pnl < 0:
        unrealized_loss_pct = ((-unrealized_pnl) / total_asset * Decimal("100")).quantize(Decimal("0.0001"))
    return {
        "total_asset": total_asset,
        "daily_baseline": daily_baseline,
        "daily_drawdown": daily_drawdown,
        "daily_drawdown_pct": daily_drawdown_pct,
        "asset_change_amount": asset_change,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_loss_pct": unrealized_loss_pct,
    }


def _daily_total_asset_pct_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("daily_drawdown_pct")
    threshold = settings.account_daily_total_asset_drawdown_pct
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "daily_total_asset_drawdown_pct",
            "reason": "daily total asset drawdown percent threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("daily_baseline"),
        }
    return None


def _daily_total_asset_abs_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("daily_drawdown")
    threshold = settings.account_daily_total_asset_drawdown_krw
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "daily_total_asset_drawdown_krw",
            "reason": "daily total asset drawdown amount threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("daily_baseline"),
        }
    return None


def _weekly_total_asset_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    baseline = settings.account_weekly_total_asset_baseline_krw
    current_asset = metrics.get("total_asset")
    threshold = settings.account_weekly_total_asset_drawdown_pct
    if baseline <= 0 or current_asset is None or threshold <= 0:
        return None
    drawdown_pct = ((baseline - current_asset) / baseline * Decimal("100")).quantize(Decimal("0.0001"))
    if drawdown_pct >= threshold:
        return {
            "gate_name": "weekly_total_asset_drawdown_pct",
            "reason": "weekly total asset drawdown percent threshold reached",
            "current_metric": drawdown_pct,
            "threshold": threshold,
            "baseline_value": baseline,
            "baseline_timestamp": settings.account_weekly_total_asset_baseline_at,
        }
    return None


def _unrealized_abs_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    unrealized = metrics.get("unrealized_pnl")
    threshold = settings.account_unrealized_loss_abs_krw
    if unrealized is not None and threshold > 0 and unrealized <= -threshold:
        return {
            "gate_name": "unrealized_portfolio_loss_krw",
            "reason": "unrealized portfolio loss amount threshold reached",
            "current_metric": unrealized,
            "threshold": -threshold,
            "baseline_value": metrics.get("total_asset"),
        }
    return None


def _unrealized_pct_gate(
    *,
    settings: Settings,
    metrics: dict[str, Decimal | None],
) -> dict[str, object] | None:
    current = metrics.get("unrealized_loss_pct")
    threshold = settings.account_unrealized_loss_pct
    if current is not None and threshold > 0 and current >= threshold:
        return {
            "gate_name": "unrealized_portfolio_loss_pct",
            "reason": "unrealized portfolio loss percent threshold reached",
            "current_metric": current,
            "threshold": threshold,
            "baseline_value": metrics.get("total_asset"),
        }
    return None


def _severe_holding_gate(
    *,
    settings: Settings,
    snapshot: BrokerAccountSnapshot,
) -> dict[str, object] | None:
    pct_threshold = settings.account_severe_holding_loss_pct
    abs_threshold = settings.account_severe_holding_loss_abs_krw
    for holding in snapshot.holdings:
        holding_loss = -holding.unrealized_pnl if holding.unrealized_pnl is not None else None
        holding_loss_pct = _holding_loss_pct(holding)
        if holding_loss_pct is not None and pct_threshold > 0 and holding_loss_pct >= pct_threshold:
            return {
                "gate_name": "severe_holding_unrealized_loss_pct",
                "reason": "severe holding unrealized loss threshold reached",
                "current_metric": holding_loss_pct,
                "threshold": pct_threshold,
                "baseline_value": holding.average_price,
                "holding_symbol": holding.symbol,
            }
        if holding_loss is not None and abs_threshold > 0 and holding_loss >= abs_threshold:
            return {
                "gate_name": "severe_holding_unrealized_loss_krw",
                "reason": "severe holding unrealized loss amount threshold reached",
                "current_metric": holding.unrealized_pnl,
                "threshold": -abs_threshold,
                "baseline_value": holding.average_price,
                "holding_symbol": holding.symbol,
            }
    return None


def _holding_loss_pct(holding: BrokerAccountHolding) -> Decimal | None:
    if holding.unrealized_pnl_pct is not None:
        value = holding.unrealized_pnl_pct
        return -value if value < 0 else value
    raw = holding.payload
    for key in ("evlu_pfls_rt", "evlu_erng_rt", "profit_loss_pct"):
        parsed = _parse_decimal(raw.get(key))
        if parsed is not None:
            return -parsed if parsed < 0 else parsed
    return None


def _payload_decimal(payload: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        parsed = _parse_decimal(payload.get(key))
        if parsed is not None:
            return parsed
    balance = payload.get("balance")
    if isinstance(balance, dict):
        output2 = balance.get("output2")
        rows = output2 if isinstance(output2, list) else [output2]
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in keys:
                parsed = _parse_decimal(row.get(key))
                if parsed is not None:
                    return parsed
    return None


def _holding_payload(holding: BrokerAccountHolding) -> dict[str, object]:
    return {
        "symbol": holding.symbol,
        "quantity": holding.quantity,
        "average_price": _decimal_text(holding.average_price),
        "unrealized_pnl": _decimal_text(holding.unrealized_pnl),
        "unrealized_pnl_pct": _decimal_text(holding.unrealized_pnl_pct),
        "raw": holding.payload,
    }


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except Exception:
        return None


def _decimal_or_none(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    return _parse_decimal(value)


def _decimal_text(value: object) -> str | None:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None
    return f"{parsed:.4f}"


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
