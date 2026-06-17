from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import Settings, load_settings
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerOrder, ExecutionFill, ExecutionPlanItem

REPORT_VERSION = "trade-outcome-feedback-v1"


@dataclass
class OpenLot:
    quantity: int
    price: Decimal
    filled_at: datetime
    fill: ExecutionFill


def generate_trade_outcome_feedback(
    *,
    settings: Settings,
    db: Session,
    lookback_days: int | None = None,
    as_of: datetime | None = None,
    output_dir: Path | None = None,
) -> dict[str, object]:
    now = _normalize_utc(as_of or datetime.now(UTC))
    days = max(1, int(lookback_days or settings.trade_outcome_feedback_lookback_days))
    started_at = now - timedelta(days=days)
    fills = _load_fills(settings=settings, db=db)
    closed_trades = [
        trade
        for trade in _fifo_closed_trades(fills)
        if _parse_datetime(str((trade.get("sell") or {}).get("filled_at"))) >= started_at
    ]
    summary = _summary(closed_trades)
    lessons = _lessons(closed_trades, max_lessons=settings.trade_outcome_feedback_max_lessons)
    snippet = _snippet(summary=summary, lessons=lessons)
    report_date = now.astimezone(UTC).date()
    report: dict[str, object] = {
        "version": REPORT_VERSION,
        "mode": settings.runtime_mode,
        "generated_at": now.isoformat(),
        "lookback_days": days,
        "started_at": started_at.isoformat(),
        "closed_trade_count": len(closed_trades),
        "summary": summary,
        "closed_trades": closed_trades,
        "lessons": lessons,
        "prompt_snippet": snippet,
    }
    target_dir = output_dir or Path(settings.trade_outcome_feedback_dir)
    _write_artifacts(report=report, report_date=report_date, output_dir=target_dir)
    return report


def load_latest_trade_outcome_feedback_snippet(settings: Settings) -> dict[str, object] | None:
    if not settings.trade_outcome_feedback_enabled:
        return None
    latest_path = Path(settings.trade_outcome_feedback_dir) / "latest.json"
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    snippet = payload.get("prompt_snippet")
    if not isinstance(snippet, dict):
        return None
    return {
        "version": payload.get("version"),
        "generated_at": payload.get("generated_at"),
        "lookback_days": payload.get("lookback_days"),
        "closed_trade_count": payload.get("closed_trade_count"),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        "snippet": snippet,
    }


def _load_fills(*, settings: Settings, db: Session) -> list[ExecutionFill]:
    return list(
        db.scalars(
            select(ExecutionFill)
            .join(ExecutionFill.broker_order)
            .where(
                BrokerOrder.mode == settings.runtime_mode,
            )
            .options(
                selectinload(ExecutionFill.broker_order)
                .selectinload(BrokerOrder.plan_item)
                .selectinload(ExecutionPlanItem.plan)
            )
            .order_by(ExecutionFill.symbol.asc(), ExecutionFill.filled_at.asc(), ExecutionFill.id.asc())
        )
    )


def _fifo_closed_trades(fills: list[ExecutionFill]) -> list[dict[str, object]]:
    lots_by_symbol: dict[str, list[OpenLot]] = {}
    closed: list[dict[str, object]] = []
    for fill in sorted(fills, key=lambda row: (row.symbol, _normalize_utc(row.filled_at), row.id)):
        side = str(fill.side or "").lower()
        if side == "buy":
            lots_by_symbol.setdefault(fill.symbol, []).append(
                OpenLot(
                    quantity=int(fill.quantity),
                    price=Decimal(fill.price),
                    filled_at=_normalize_utc(fill.filled_at),
                    fill=fill,
                )
            )
            continue
        if side != "sell":
            continue
        remaining = int(fill.quantity)
        lots = lots_by_symbol.setdefault(fill.symbol, [])
        while remaining > 0 and lots:
            lot = lots[0]
            matched = min(remaining, lot.quantity)
            closed.append(_closed_trade(lot=lot, sell_fill=fill, quantity=matched))
            lot.quantity -= matched
            remaining -= matched
            if lot.quantity <= 0:
                lots.pop(0)
    return closed


def _closed_trade(*, lot: OpenLot, sell_fill: ExecutionFill, quantity: int) -> dict[str, object]:
    buy_order = lot.fill.broker_order
    sell_order = sell_fill.broker_order
    buy_item = buy_order.plan_item if buy_order is not None else None
    sell_item = sell_order.plan_item if sell_order is not None else None
    sell_price = Decimal(sell_fill.price)
    buy_notional = lot.price * Decimal(quantity)
    sell_notional = sell_price * Decimal(quantity)
    pnl = sell_notional - buy_notional
    return_pct = (pnl / buy_notional * Decimal("100")) if buy_notional > 0 else Decimal("0")
    hold_minutes = int((_normalize_utc(sell_fill.filled_at) - lot.filled_at).total_seconds() // 60)
    source_item = buy_item or sell_item
    return {
        "symbol": sell_fill.symbol,
        "quantity": quantity,
        "buy": _fill_payload(fill=lot.fill, order=buy_order, item=buy_item, quantity=quantity),
        "sell": _fill_payload(fill=sell_fill, order=sell_order, item=sell_item, quantity=quantity),
        "realized_pnl_krw": _decimal_text(pnl),
        "return_pct": _decimal_text(return_pct),
        "hold_minutes": hold_minutes,
        "hold_hours": _decimal_text(Decimal(hold_minutes) / Decimal("60")),
        "source_execution_plan_item_id": source_item.id if source_item is not None else None,
        "decision_run_id": source_item.decision_run_id if source_item is not None else None,
        "decision_agent_name": source_item.decision_agent_name if source_item is not None else None,
        "exec_agent_decision": _exec_agent_decision(source_item),
        "risk_sizing": _risk_sizing_context(source_item),
        "trigger_time": _trigger_time_context(source_item),
        "market_enrichment": _market_enrichment_context(source_item),
        "outcome_bucket": _outcome_bucket(pnl),
    }


def _fill_payload(
    *,
    fill: ExecutionFill,
    order: BrokerOrder | None,
    item: ExecutionPlanItem | None,
    quantity: int,
) -> dict[str, object]:
    return {
        "filled_at": _normalize_utc(fill.filled_at).isoformat(),
        "quantity": quantity,
        "price": _decimal_text(Decimal(fill.price)),
        "broker_fill_id": fill.broker_fill_id,
        "broker_order_id": order.broker_order_id if order is not None else None,
        "execution_plan_item_id": item.id if item is not None else None,
        "intent": item.intent if item is not None else None,
        "source_plan_item_id": item.source_plan_item_id if item is not None else None,
    }


def _summary(closed_trades: list[dict[str, object]]) -> dict[str, object]:
    pnl_values = [_decimal_value(row.get("realized_pnl_krw")) or Decimal("0") for row in closed_trades]
    winners = [value for value in pnl_values if value > 0]
    losers = [value for value in pnl_values if value < 0]
    total = sum(pnl_values, Decimal("0"))
    return {
        "realized_pnl_krw": _decimal_text(total),
        "win_count": len(winners),
        "loss_count": len(losers),
        "win_rate_pct": _decimal_text((Decimal(len(winners)) / Decimal(len(pnl_values)) * Decimal("100")) if pnl_values else Decimal("0")),
        "average_win_krw": _decimal_text(sum(winners, Decimal("0")) / Decimal(len(winners)) if winners else Decimal("0")),
        "average_loss_krw": _decimal_text(sum(losers, Decimal("0")) / Decimal(len(losers)) if losers else Decimal("0")),
        "worst_loss_krw": _decimal_text(min(pnl_values) if pnl_values else Decimal("0")),
    }


def _lessons(closed_trades: list[dict[str, object]], *, max_lessons: int) -> dict[str, object]:
    worst_losses = sorted(
        closed_trades,
        key=lambda row: _decimal_value(row.get("realized_pnl_krw")) or Decimal("0"),
    )[:max_lessons]
    profile_stats: dict[str, list[Decimal]] = {}
    reason_stats: dict[str, list[Decimal]] = {}
    for trade in closed_trades:
        pnl = _decimal_value(trade.get("realized_pnl_krw")) or Decimal("0")
        decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
        profile = str(decision.get("target_profile") or "unknown")
        reason = str(decision.get("reason_code") or "unknown")
        profile_stats.setdefault(profile, []).append(pnl)
        reason_stats.setdefault(reason, []).append(pnl)
    return {
        "worst_losses": [_lesson_trade(row) for row in worst_losses],
        "patterns_to_downsize_or_reject": _negative_patterns(profile_stats, max_lessons=max_lessons),
        "positive_expectancy_patterns": _positive_patterns(profile_stats, max_lessons=max_lessons),
        "negative_reason_codes": _negative_patterns(reason_stats, max_lessons=max_lessons),
    }


def _snippet(*, summary: dict[str, object], lessons: dict[str, object]) -> dict[str, object]:
    hard_gates = []
    for row in lessons.get("patterns_to_downsize_or_reject", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Downsize/reject target_profile={row.get('name')} until evidence improves; avg_pnl={row.get('avg_pnl_krw')} over {row.get('count')} closed trades."
            )
    for row in lessons.get("worst_losses", []):
        if isinstance(row, dict):
            hard_gates.append(
                f"Recent worst loss {row.get('symbol')} {row.get('return_pct')}% ({row.get('realized_pnl_krw')} KRW), profile={row.get('target_profile')}, reason={row.get('reason_code')}."
            )
    return {
        "summary": (
            f"Recent closed trades: pnl={summary.get('realized_pnl_krw')} KRW, "
            f"win_rate={summary.get('win_rate_pct')}%, worst_loss={summary.get('worst_loss_krw')} KRW."
        ),
        "patterns_to_downsize_or_reject": hard_gates[:6],
        "positive_expectancy_patterns": lessons.get("positive_expectancy_patterns", []),
        "required_hard_gates": [
            "Do not increase size for profiles/reason_codes listed as negative without fresh superior live quality.",
            "Prefer delay/reject over execute_now when recent worst-loss conditions resemble the current candidate.",
        ],
    }


def _write_artifacts(*, report: dict[str, object], report_date: date, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_date.isoformat()}.json"
    md_path = output_dir / f"{report_date.isoformat()}.md"
    payload = _json_safe(report)
    json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    json_path.write_text(json_text + "\n", encoding="utf-8")
    (output_dir / "latest.json").write_text(json_text + "\n", encoding="utf-8")
    md_path.write_text(_markdown_report(payload), encoding="utf-8")
    (output_dir / "latest.md").write_text(_markdown_report(payload), encoding="utf-8")


def _markdown_report(report: dict[str, object]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    snippet = report.get("prompt_snippet") if isinstance(report.get("prompt_snippet"), dict) else {}
    lines = [
        f"# Trade Outcome Feedback {str(report.get('generated_at'))[:10]}",
        "",
        f"- Mode: {report.get('mode')}",
        f"- Closed trades: {report.get('closed_trade_count')}",
        f"- Realized P/L: {summary.get('realized_pnl_krw')} KRW",
        f"- Win rate: {summary.get('win_rate_pct')}%",
        f"- Worst loss: {summary.get('worst_loss_krw')} KRW",
        "",
        "## Prompt Snippet",
        "",
        str(snippet.get("summary") or ""),
        "",
        "## Gates",
    ]
    for item in snippet.get("patterns_to_downsize_or_reject", []):
        lines.append(f"- {item}")
    for item in snippet.get("required_hard_gates", []):
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def _negative_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg < 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])))[:max_lessons]


def _positive_patterns(stats: dict[str, list[Decimal]], *, max_lessons: int) -> list[dict[str, object]]:
    rows = []
    for name, values in stats.items():
        total = sum(values, Decimal("0"))
        avg = total / Decimal(len(values))
        if avg > 0:
            rows.append({"name": name, "count": len(values), "avg_pnl_krw": _decimal_text(avg), "total_pnl_krw": _decimal_text(total)})
    return sorted(rows, key=lambda row: Decimal(str(row["avg_pnl_krw"])), reverse=True)[:max_lessons]


def _lesson_trade(trade: dict[str, object]) -> dict[str, object]:
    decision = trade.get("exec_agent_decision") if isinstance(trade.get("exec_agent_decision"), dict) else {}
    return {
        "symbol": trade.get("symbol"),
        "realized_pnl_krw": trade.get("realized_pnl_krw"),
        "return_pct": trade.get("return_pct"),
        "hold_hours": trade.get("hold_hours"),
        "target_profile": decision.get("target_profile"),
        "reason_code": decision.get("reason_code"),
        "decision_run_id": trade.get("decision_run_id"),
    }


def _exec_agent_decision(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    for source in (item.risk_context, item.quantity_policy):
        if isinstance(source, dict):
            for key in ("exec_agent_decision", "short_term_exec_agent_decision", "last_trigger_time_exec_agent_interaction"):
                value = source.get(key)
                if isinstance(value, dict):
                    return _json_safe(value)
    return {}


def _risk_sizing_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None:
        return {}
    payload: dict[str, object] = {}
    for source in (item.quantity_policy, item.risk_context):
        if not isinstance(source, dict):
            continue
        for key in (
            "requested_quantity",
            "approved_quantity",
            "stop_price",
            "effective_stop_price",
            "sizing_adjustment",
            "max_safe_notional_krw",
            "budget_krw",
        ):
            if key in source and key not in payload:
                payload[key] = source[key]
    return _json_safe(payload)


def _trigger_time_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    interaction = item.risk_context.get("last_trigger_time_exec_agent_interaction")
    if not isinstance(interaction, dict):
        return {}
    decision = interaction.get("decision") if isinstance(interaction.get("decision"), dict) else {}
    request = interaction.get("request_payload") if isinstance(interaction.get("request_payload"), dict) else {}
    market = request.get("market_context") if isinstance(request.get("market_context"), dict) else {}
    return _json_safe(
        {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "risk_notes": decision.get("risk_notes"),
            "context_staleness_ok": decision.get("context_staleness_ok"),
            "market_quality_gate": market.get("live_execution_quality_gate"),
        }
    )


def _market_enrichment_context(item: ExecutionPlanItem | None) -> dict[str, object]:
    if item is None or not isinstance(item.risk_context, dict):
        return {}
    payload: dict[str, object] = {}
    for key in (
        "liquidity_tier",
        "trading_value",
        "short_ratio",
        "investor_flow",
        "market_breadth",
        "data_quality_flags",
        "data_quality_warnings",
    ):
        if key in item.risk_context:
            payload[key] = item.risk_context[key]
    return _json_safe(payload)


def _outcome_bucket(pnl: Decimal) -> str:
    if pnl < 0:
        return "loss"
    if pnl > 0:
        return "win"
    return "flat"


def _decimal_value(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: str) -> datetime:
    return _normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _json_safe(value: object) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _normalize_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate exec-agent trade outcome feedback artifacts.")
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    settings = load_settings()
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    with session_factory() as db:
        report = generate_trade_outcome_feedback(
            settings=settings,
            db=db,
            lookback_days=args.lookback_days,
            output_dir=args.output_dir,
        )
    print(json.dumps(_json_safe({"status": "ok", "closed_trade_count": report["closed_trade_count"], "output_dir": str(args.output_dir or settings.trade_outcome_feedback_dir)})))


if __name__ == "__main__":
    main()
