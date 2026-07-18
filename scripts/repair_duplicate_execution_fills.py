#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path


BACKUP_TABLE = "execution_fill_duplicate_repairs"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Identify and optionally remove duplicate execution_fills rows.",
    )
    parser.add_argument("database", type=Path, help="SQLite database path")
    parser.add_argument("--apply", action="store_true", help="Backup and delete duplicate rows")
    args = parser.parse_args()

    if not args.database.exists():
        raise SystemExit(f"database not found: {args.database}")

    con = sqlite3.connect(args.database)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            select id, broker_order_pk, broker_fill_id, symbol, side, filled_at, price,
                   quantity, payload, created_at, updated_at
            from execution_fills
            order by broker_order_pk, filled_at, id
            """
        ).fetchall()
        groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            groups[_idempotency_key(row)].append(row)

        duplicates = [group for group in groups.values() if len(group) > 1]
        duplicate_rows = sum(len(group) - 1 for group in duplicates)
        print(
            json.dumps(
                {
                    "duplicate_groups": len(duplicates),
                    "duplicate_rows": duplicate_rows,
                    "mode": "apply" if args.apply else "dry-run",
                },
                ensure_ascii=False,
            )
        )
        for group in duplicates:
            keeper = group[0]
            extras = group[1:]
            print(
                json.dumps(
                    {
                        "keeper_id": keeper["id"],
                        "duplicate_ids": [row["id"] for row in extras],
                        "symbol": keeper["symbol"],
                        "side": keeper["side"],
                        "filled_at": keeper["filled_at"],
                        "price": str(keeper["price"]),
                        "quantity": keeper["quantity"],
                    },
                    ensure_ascii=False,
                )
            )

        if not args.apply or duplicate_rows == 0:
            if args.apply:
                _backfill_idempotency_keys(con=con, groups=groups)
                con.commit()
            return 0

        con.execute(
            f"""
            create table if not exists {BACKUP_TABLE} (
                repair_id integer primary key autoincrement,
                repaired_at text not null,
                kept_fill_id text not null,
                duplicate_fill_id text not null,
                duplicate_row_json text not null
            )
            """
        )
        repaired_at = datetime.now(UTC).isoformat()
        for group in duplicates:
            keeper = group[0]
            for duplicate in group[1:]:
                con.execute(
                    f"""
                    insert into {BACKUP_TABLE}
                        (repaired_at, kept_fill_id, duplicate_fill_id, duplicate_row_json)
                    values (?, ?, ?, ?)
                    """,
                    (
                        repaired_at,
                        keeper["id"],
                        duplicate["id"],
                        json.dumps(dict(duplicate), ensure_ascii=False, default=str),
                    ),
                )
                con.execute("delete from execution_fills where id = ?", (duplicate["id"],))
        _backfill_idempotency_keys(con=con, groups=groups)
        con.commit()
        return 0
    finally:
        con.close()


def _idempotency_key(row: sqlite3.Row) -> str:
    broker_fill_id = _optional(row["broker_fill_id"])
    if broker_fill_id:
        parts = ("broker", row["broker_order_pk"], broker_fill_id)
    else:
        parts = (
            "fallback",
            row["broker_order_pk"],
            str(row["symbol"]).strip().upper(),
            str(row["side"]).strip().lower(),
            _timestamp_key(str(row["filled_at"])),
            _decimal_key(row["price"]),
            str(int(row["quantity"])),
        )
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"execution_fill:{digest}"


def _backfill_idempotency_keys(
    *,
    con: sqlite3.Connection,
    groups: dict[str, list[sqlite3.Row]],
) -> None:
    for key, group in groups.items():
        keeper = group[0]
        con.execute(
            "update execution_fills set idempotency_key = ? where id = ?",
            (key, keeper["id"]),
        )


def _optional(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _timestamp_key(value: str) -> str:
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _decimal_key(value: object) -> str:
    return format(Decimal(str(value)).normalize(), "f")


if __name__ == "__main__":
    raise SystemExit(main())
