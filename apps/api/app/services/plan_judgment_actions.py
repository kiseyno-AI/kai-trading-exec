from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import Session, undefer

from app.core.config import Settings
from app.models.runtime import ExecutionPlan, ExecutionPlanItem, ExecutionPlanItemAudit

BUY_WATCH_INTENTS = ("intraday_watch_buy", "next_open_buy")
BUY_MUTABLE_STATUSES = (
    "watch_monitoring_active",
    "watch_monitoring_pending",
    "short_term_watcher_pending",
    "short_term_watch_blocked",
    "open",
)
BUY_REVIEWABLE_REJECT_STATUSES = (
    "agent_rejected",
    "exec_agent_rejected",
    "watch_buy_rejected",
    "watch_expired",
)
ADJUSTMENT_KEY = "hourly_plan_action"
SOURCE = "hourly_runtime_check"
MAX_MUTATIONS_PER_RUN = 20


def _database_path(settings: Settings) -> str:
    parsed = urlparse(settings.database_url)
    if parsed.scheme not in {"sqlite", "sqlite+pysqlite"}:
        raise ValueError("hourly plan judgment action currently supports sqlite runtime DB only")
    if settings.database_url.startswith("sqlite:///./"):
        return str(Path(settings.database_url.removeprefix("sqlite:///")).resolve())
    if settings.database_url.startswith("sqlite:///../"):
        return str(Path(settings.database_url.removeprefix("sqlite:///")).resolve())
    if parsed.netloc and parsed.path:
        return unquote(f"/{parsed.netloc}{parsed.path}")
    path = unquote(parsed.path)
    if path.startswith("//"):
        return path[1:]
    if path.startswith("/"):
        return path
    return str(Path(path).resolve())


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(loaded) if isinstance(loaded, dict) else {}
    return {}


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _price_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN))


def _connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(_database_path(settings), timeout=0.2)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 200")
    return conn


def _load_mutable_buy_items(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    after: tuple[str, str, str] | None = None,
) -> list[sqlite3.Row]:
    placeholders_intents = ",".join("?" for _ in BUY_WATCH_INTENTS)
    placeholders_statuses = ",".join("?" for _ in BUY_MUTABLE_STATUSES)
    cursor_clause = ""
    cursor_params: tuple[str, str, str, str, str, str] = ()
    if after is not None:
        symbol, created_at, item_id = after
        cursor_clause = """
              and (
                i.symbol > ?
                or (i.symbol = ? and i.created_at > ?)
                or (i.symbol = ? and i.created_at = ? and i.id > ?)
              )
        """
        cursor_params = (symbol, symbol, created_at, symbol, created_at, item_id)
    return list(
        conn.execute(
            f"""
            select i.id, i.symbol, i.status, i.trigger_price, i.limit_price, i.risk_context, i.created_at
            from execution_plan_items i
            join execution_plans p on p.id = i.execution_plan_id
            where p.mode = ?
              and i.side = 'buy'
              and i.intent in ({placeholders_intents})
              and i.status in ({placeholders_statuses})
              {cursor_clause}
            order by i.symbol asc, i.created_at asc, i.id asc
            limit ?
            """,
            (
                settings.runtime_mode,
                *BUY_WATCH_INTENTS,
                *BUY_MUTABLE_STATUSES,
                *cursor_params,
                MAX_MUTATIONS_PER_RUN,
            ),
        )
    )


def _insert_audit(
    conn: sqlite3.Connection,
    *,
    item_id: str,
    event_type: str,
    summary: dict[str, object],
    payload: dict[str, object],
    now: str,
) -> None:
    conn.execute(
        """
        insert into execution_plan_item_audits (
          id, execution_plan_item_id, event_type, source, summary, payload,
          occurred_at, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            item_id,
            event_type,
            SOURCE,
            json.dumps(summary, ensure_ascii=False),
            json.dumps(payload, ensure_ascii=False),
            now,
            now,
            now,
        ),
    )


def apply_hourly_plan_judgment_action(
    *,
    settings: Settings,
    db: Session | None = None,
    risk_flags: int,
    risk_reasons: list[str],
) -> dict[str, object]:
    if urlparse(settings.database_url).scheme not in {"sqlite", "sqlite+pysqlite"}:
        if db is None:
            raise ValueError("a database session is required for non-sqlite plan judgment actions")
        return _apply_hourly_plan_judgment_action_session(
            db=db,
            settings=settings,
            risk_flags=risk_flags,
            risk_reasons=risk_reasons,
        )
    try:
        with _connect(settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if risk_flags >= 3:
                changed = _expire_buy_watches(conn, settings, risk_flags, risk_reasons)
                message = f"risk_off: 신규/추가매수 watch {changed}건 중단"
                action = "risk_off_expire"
            elif risk_flags >= 1:
                changed = _adjust_buy_watch_prices(conn, settings, risk_flags, risk_reasons)
                message = f"caution: 신규/추가매수 trigger/limit 1% 보수화 {changed}건"
                action = "conservative_price_adjust"
            else:
                restored = _restore_conservative_adjustments(conn, settings)
                changed = restored
                message = f"normal: 보수화 가격선 원복 {restored}건"
                action = "restore_conservative_price_adjust"
            conn.commit()
            return {"status": "applied", "action": action, "message": message, "changed_count": changed}
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            return {"status": "skipped", "action": "db_busy", "message": "skip(db_busy)", "changed_count": 0}
        raise


def _load_mutable_buy_items_session(
    db: Session,
    settings: Settings,
    *,
    after: tuple[str, datetime, str] | None = None,
) -> list[ExecutionPlanItem]:
    filters = [
        ExecutionPlan.mode == settings.runtime_mode,
        ExecutionPlanItem.side == "buy",
        ExecutionPlanItem.intent.in_(BUY_WATCH_INTENTS),
        ExecutionPlanItem.status.in_(BUY_MUTABLE_STATUSES),
    ]
    if after is not None:
        symbol, created_at, item_id = after
        filters.append(
            or_(
                ExecutionPlanItem.symbol > symbol,
                and_(ExecutionPlanItem.symbol == symbol, ExecutionPlanItem.created_at > created_at),
                and_(
                    ExecutionPlanItem.symbol == symbol,
                    ExecutionPlanItem.created_at == created_at,
                    ExecutionPlanItem.id > item_id,
                ),
            )
        )
    return list(
        db.scalars(
            select(ExecutionPlanItem)
            .join(ExecutionPlan, ExecutionPlan.id == ExecutionPlanItem.execution_plan_id)
            .options(undefer(ExecutionPlanItem.risk_context))
            .where(*filters)
            .order_by(ExecutionPlanItem.symbol, ExecutionPlanItem.created_at, ExecutionPlanItem.id)
            .limit(MAX_MUTATIONS_PER_RUN)
        ).all()
    )


def _insert_audit_session(
    db: Session,
    *,
    item: ExecutionPlanItem,
    event_type: str,
    summary: dict[str, object],
    payload: dict[str, object],
    now: datetime,
) -> None:
    db.add(
        ExecutionPlanItemAudit(
            execution_plan_item_id=item.id,
            event_type=event_type,
            source=SOURCE,
            summary=summary,
            payload=payload,
            occurred_at=now,
            created_at=now,
            updated_at=now,
        )
    )


def _expire_buy_watch_item_session(
    db: Session,
    *,
    item: ExecutionPlanItem,
    risk_context: dict[str, object],
    now: datetime,
) -> bool:
    result = db.execute(
        update(ExecutionPlanItem)
        .where(
            ExecutionPlanItem.id == item.id,
            ExecutionPlanItem.status == item.status,
        )
        .values(status="watch_expired", risk_context=risk_context, updated_at=now)
        .execution_options(synchronize_session=False)
    )
    return bool(result.rowcount)


def _apply_hourly_plan_judgment_action_session(
    *,
    db: Session,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> dict[str, object]:
    changed = 0
    now = datetime.now(UTC)
    if risk_flags >= 3:
        action = "risk_off_expire"
        event_type = "hourly_plan_judgment_buy_watch_expired"
        items = _load_mutable_buy_items_session(db, settings)
        for item in items:
            previous = {
                "status": item.status,
                "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
                "limit_price": str(item.limit_price) if item.limit_price is not None else None,
            }
            risk_context = _json_object(item.risk_context)
            risk_context[ADJUSTMENT_KEY] = {
                "action": action,
                "source": SOURCE,
                "risk_flags": risk_flags,
                "risk_reasons": risk_reasons,
                "previous": previous,
                "applied_at": now.isoformat(),
            }
            if not _expire_buy_watch_item_session(db, item=item, risk_context=risk_context, now=now):
                continue
            _insert_audit_session(
                db,
                item=item,
                event_type=event_type,
                summary={"symbol": item.symbol, "action": action, "risk_flags": risk_flags},
                payload={"risk_reasons": risk_reasons, "previous": previous},
                now=now,
            )
            changed += 1
        message = f"risk_off: 신규/추가매수 watch {changed}건 중단"
    elif risk_flags >= 1:
        action = "conservative_price_adjust"
        after: tuple[str, datetime, str] | None = None
        while changed < MAX_MUTATIONS_PER_RUN:
            items = _load_mutable_buy_items_session(db, settings, after=after)
            if not items:
                break
            for item in items:
                risk_context = _json_object(item.risk_context)
                existing = risk_context.get(ADJUSTMENT_KEY)
                if isinstance(existing, dict) and existing.get("action") == action:
                    continue
                trigger_price = _decimal_or_none(item.trigger_price)
                limit_price = _decimal_or_none(item.limit_price)
                if trigger_price is None and limit_price is None:
                    continue
                original = {
                    "trigger_price": str(item.trigger_price) if item.trigger_price is not None else None,
                    "limit_price": str(item.limit_price) if item.limit_price is not None else None,
                }
                adjusted_trigger = _decimal_or_none(_price_text(trigger_price * Decimal("0.99"))) if trigger_price else None
                adjusted_limit = _decimal_or_none(_price_text(limit_price * Decimal("0.99"))) if limit_price else None
                risk_context[ADJUSTMENT_KEY] = {
                    "action": action,
                    "source": SOURCE,
                    "risk_flags": risk_flags,
                    "risk_reasons": risk_reasons,
                    "original": original,
                    "adjusted": {
                        "trigger_price": _price_text(adjusted_trigger),
                        "limit_price": _price_text(adjusted_limit),
                    },
                    "applied_at": now.isoformat(),
                }
                if adjusted_trigger is not None:
                    item.trigger_price = adjusted_trigger
                if adjusted_limit is not None:
                    item.limit_price = adjusted_limit
                item.risk_context = risk_context
                item.updated_at = now
                _insert_audit_session(
                    db,
                    item=item,
                    event_type="hourly_plan_judgment_buy_watch_price_adjusted",
                    summary={"symbol": item.symbol, "action": action, "risk_flags": risk_flags},
                    payload={
                        "risk_reasons": risk_reasons,
                        "original": original,
                        "adjusted": risk_context[ADJUSTMENT_KEY]["adjusted"],
                    },
                    now=now,
                )
                changed += 1
                if changed >= MAX_MUTATIONS_PER_RUN:
                    break
            if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
                break
            last = items[-1]
            after = (last.symbol, last.created_at, last.id)
        message = f"caution: 신규/추가매수 trigger/limit 1% 보수화 {changed}건"
    else:
        action = "restore_conservative_price_adjust"
        after: tuple[str, datetime, str] | None = None
        while changed < MAX_MUTATIONS_PER_RUN:
            items = _load_mutable_buy_items_session(db, settings, after=after)
            if not items:
                break
            for item in items:
                risk_context = _json_object(item.risk_context)
                adjustment = risk_context.get(ADJUSTMENT_KEY)
                if not isinstance(adjustment, dict) or adjustment.get("action") != "conservative_price_adjust":
                    continue
                original = adjustment.get("original") if isinstance(adjustment.get("original"), dict) else {}
                trigger_price = _decimal_or_none(original.get("trigger_price"))
                limit_price = _decimal_or_none(original.get("limit_price"))
                if trigger_price is not None:
                    item.trigger_price = trigger_price
                if limit_price is not None:
                    item.limit_price = limit_price
                risk_context.pop(ADJUSTMENT_KEY, None)
                item.risk_context = risk_context
                item.updated_at = now
                _insert_audit_session(
                    db,
                    item=item,
                    event_type="hourly_plan_judgment_buy_watch_price_restored",
                    summary={"symbol": item.symbol, "action": action},
                    payload={"restored": original},
                    now=now,
                )
                changed += 1
                if changed >= MAX_MUTATIONS_PER_RUN:
                    break
            if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
                break
            last = items[-1]
            after = (last.symbol, last.created_at, last.id)
        message = f"normal: 보수화 가격선 원복 {changed}건"
    db.commit()
    return {"status": "applied", "action": action, "message": message, "changed_count": changed}


def _expire_buy_watches(
    conn: sqlite3.Connection,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    for item in _load_mutable_buy_items(conn, settings):
        previous = {
            "status": item["status"],
            "trigger_price": str(item["trigger_price"]) if item["trigger_price"] is not None else None,
            "limit_price": str(item["limit_price"]) if item["limit_price"] is not None else None,
        }
        risk_context = _json_object(item["risk_context"])
        risk_context[ADJUSTMENT_KEY] = {
            "action": "risk_off_expire",
            "source": SOURCE,
            "risk_flags": risk_flags,
            "risk_reasons": risk_reasons,
            "previous": previous,
            "applied_at": now,
        }
        cursor = conn.execute(
            """
            update execution_plan_items
            set status = 'watch_expired', risk_context = ?, updated_at = ?
            where id = ? and status = ?
            """,
            (json.dumps(risk_context, ensure_ascii=False), now, item["id"], item["status"]),
        )
        if cursor.rowcount > 0:
            _insert_audit(
                conn,
                item_id=item["id"],
                event_type="hourly_plan_judgment_buy_watch_expired",
                summary={"symbol": item["symbol"], "action": "risk_off_expire", "risk_flags": risk_flags},
                payload={"risk_reasons": risk_reasons, "previous": previous},
                now=now,
            )
            changed += 1
    return changed


def _adjust_buy_watch_prices(
    conn: sqlite3.Connection,
    settings: Settings,
    risk_flags: int,
    risk_reasons: list[str],
) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    after: tuple[str, str, str] | None = None
    while changed < MAX_MUTATIONS_PER_RUN:
        items = _load_mutable_buy_items(conn, settings, after=after)
        if not items:
            break
        for item in items:
            risk_context = _json_object(item["risk_context"])
            existing = risk_context.get(ADJUSTMENT_KEY)
            if isinstance(existing, dict) and existing.get("action") == "conservative_price_adjust":
                continue
            trigger_price = _decimal_or_none(item["trigger_price"])
            limit_price = _decimal_or_none(item["limit_price"])
            if trigger_price is None and limit_price is None:
                continue
            original = {
                "trigger_price": str(item["trigger_price"]) if item["trigger_price"] is not None else None,
                "limit_price": str(item["limit_price"]) if item["limit_price"] is not None else None,
            }
            adjusted_trigger = _price_text(trigger_price * Decimal("0.99")) if trigger_price is not None else None
            adjusted_limit = _price_text(limit_price * Decimal("0.99")) if limit_price is not None else None
            risk_context[ADJUSTMENT_KEY] = {
                "action": "conservative_price_adjust",
                "source": SOURCE,
                "risk_flags": risk_flags,
                "risk_reasons": risk_reasons,
                "original": original,
                "adjusted": {"trigger_price": adjusted_trigger, "limit_price": adjusted_limit},
                "applied_at": now,
            }
            conn.execute(
                """
                update execution_plan_items
                set trigger_price = coalesce(?, trigger_price),
                    limit_price = coalesce(?, limit_price),
                    risk_context = ?,
                    updated_at = ?
                where id = ?
                """,
                (adjusted_trigger, adjusted_limit, json.dumps(risk_context, ensure_ascii=False), now, item["id"]),
            )
            _insert_audit(
                conn,
                item_id=item["id"],
                event_type="hourly_plan_judgment_buy_watch_price_adjusted",
                summary={"symbol": item["symbol"], "action": "conservative_price_adjust", "risk_flags": risk_flags},
                payload={
                    "risk_reasons": risk_reasons,
                    "original": original,
                    "adjusted": {"trigger_price": adjusted_trigger, "limit_price": adjusted_limit},
                },
                now=now,
            )
            changed += 1
            if changed >= MAX_MUTATIONS_PER_RUN:
                break
        if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
            break
        after = (items[-1]["symbol"], items[-1]["created_at"], items[-1]["id"])
    return changed


def _restore_conservative_adjustments(conn: sqlite3.Connection, settings: Settings) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    after: tuple[str, str, str] | None = None
    while changed < MAX_MUTATIONS_PER_RUN:
        items = _load_mutable_buy_items(conn, settings, after=after)
        if not items:
            break
        for item in items:
            risk_context = _json_object(item["risk_context"])
            adjustment = risk_context.get(ADJUSTMENT_KEY)
            if not isinstance(adjustment, dict) or adjustment.get("action") != "conservative_price_adjust":
                continue
            original = adjustment.get("original") if isinstance(adjustment.get("original"), dict) else {}
            trigger_price = _price_text(_decimal_or_none(original.get("trigger_price")))
            limit_price = _price_text(_decimal_or_none(original.get("limit_price")))
            risk_context.pop(ADJUSTMENT_KEY, None)
            conn.execute(
                """
                update execution_plan_items
                set trigger_price = coalesce(?, trigger_price),
                    limit_price = coalesce(?, limit_price),
                    risk_context = ?,
                    updated_at = ?
                where id = ?
                """,
                (trigger_price, limit_price, json.dumps(risk_context, ensure_ascii=False), now, item["id"]),
            )
            _insert_audit(
                conn,
                item_id=item["id"],
                event_type="hourly_plan_judgment_buy_watch_price_restored",
                summary={"symbol": item["symbol"], "action": "restore_conservative_price_adjust"},
                payload={"restored": original},
                now=now,
            )
            changed += 1
            if changed >= MAX_MUTATIONS_PER_RUN:
                break
        if changed >= MAX_MUTATIONS_PER_RUN or len(items) < MAX_MUTATIONS_PER_RUN:
            break
        after = (items[-1]["symbol"], items[-1]["created_at"], items[-1]["id"])
    return changed


def _load_reviewable_buy_items(conn: sqlite3.Connection, settings: Settings) -> list[sqlite3.Row]:
    placeholders_intents = ",".join("?" for _ in BUY_WATCH_INTENTS)
    placeholders_statuses = ",".join("?" for _ in BUY_REVIEWABLE_REJECT_STATUSES)
    return list(
        conn.execute(
            f"""
            select i.id, i.symbol, i.status, i.trigger_price, i.limit_price, i.risk_context
            from execution_plan_items i
            join execution_plans p on p.id = i.execution_plan_id
            where p.mode = ?
              and i.side = 'buy'
              and i.intent in ({placeholders_intents})
              and i.status in ({placeholders_statuses})
            order by i.updated_at desc, i.symbol asc, i.id asc
            limit ?
            """,
            (settings.runtime_mode, *BUY_WATCH_INTENTS, *BUY_REVIEWABLE_REJECT_STATUSES, MAX_MUTATIONS_PER_RUN),
        )
    )


def _resume_reviewable_buy_candidates(conn: sqlite3.Connection, settings: Settings) -> int:
    changed = 0
    now = datetime.now(UTC).isoformat()
    for item in _load_reviewable_buy_items(conn, settings):
        previous = {
            "status": item["status"],
            "trigger_price": str(item["trigger_price"]) if item["trigger_price"] is not None else None,
            "limit_price": str(item["limit_price"]) if item["limit_price"] is not None else None,
        }
        risk_context = _json_object(item["risk_context"])
        risk_context[ADJUSTMENT_KEY] = {
            "action": "resume_review",
            "source": SOURCE,
            "previous": previous,
            "applied_at": now,
        }
        cursor = conn.execute(
            """
            update execution_plan_items
            set status = 'watch_monitoring_pending', risk_context = ?, updated_at = ?
            where id = ? and status = ?
            """,
            (json.dumps(risk_context, ensure_ascii=False), now, item["id"], item["status"]),
        )
        if cursor.rowcount <= 0:
            continue
        _insert_audit(
            conn,
            item_id=item["id"],
            event_type="hourly_plan_judgment_buy_watch_resumed",
            summary={"symbol": item["symbol"], "action": "resume_review", "previous_status": item["status"]},
            payload={"previous": previous},
            now=now,
        )
        changed += 1
    return changed
