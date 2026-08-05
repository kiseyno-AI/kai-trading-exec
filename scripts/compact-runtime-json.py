#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

DROP_KEYS = {
    "request_payload",
    "response_payload",
    "decider_handoff_context",
    "decider_input_context",
    "decider_input",
    "decider_output",
    "raw_payload",
    "validation",
    "monitoring_events",
    "trigger_time_exec_agent_interactions",
}


def compact(value):
    if not isinstance(value, dict):
        return value
    result = {}
    for key, child in value.items():
        if key in DROP_KEYS:
            result[f"{key}_omitted"] = True
            continue
        if key == "last_trigger_time_exec_agent_interaction" and isinstance(child, dict):
            result[key] = {
                kept_key: child.get(kept_key)
                for kept_key in (
                    "version",
                    "source",
                    "request_payload_hash",
                    "decision",
                    "model",
                    "agent_id",
                    "decision_timestamp",
                    "previous_monitoring_state",
                    "new_monitoring_state",
                    "recorded_at",
                    "order",
                )
                if kept_key in child
            }
            continue
        if isinstance(child, dict):
            result[key] = compact(child)
        elif isinstance(child, list):
            result[key] = child[:3]
            if len(child) > 3:
                result[f"{key}_truncated_count"] = len(child) - 3
        else:
            result[key] = child if not isinstance(child, str) or len(child) <= 1000 else child[:1000]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Compact oversized execution_plan_items JSON payloads.")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--threshold", type=int, default=100_000, help="Only rewrite rows above this JSON byte size")
    parser.add_argument("--limit", type=int, default=100, help="Maximum rows to inspect")
    parser.add_argument("--write", action="store_true", help="Apply updates; default is dry-run")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser()
    conn = sqlite3.connect(db_path)
    conn.execute("pragma busy_timeout=5000")
    rows = conn.execute(
        """
        select id, symbol, length(risk_context), risk_context
        from execution_plan_items
        where length(risk_context) > ?
        order by length(risk_context) desc
        limit ?
        """,
        [args.threshold, args.limit],
    ).fetchall()
    updated = 0
    for row_id, symbol, before_size, raw_payload in rows:
        try:
            payload = json.loads(raw_payload or "{}")
        except json.JSONDecodeError:
            continue
        compacted = compact(payload)
        encoded = json.dumps(compacted, ensure_ascii=False, separators=(",", ":"))
        print(f"{symbol} {row_id}: {before_size} -> {len(encoded)}")
        if args.write and len(encoded) < int(before_size):
            conn.execute(
                "update execution_plan_items set risk_context = ?, updated_at = datetime('now') where id = ?",
                [encoded, row_id],
            )
            updated += 1
    if args.write:
        conn.commit()
    print(f"{'updated' if args.write else 'dry_run'}={updated} inspected={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
