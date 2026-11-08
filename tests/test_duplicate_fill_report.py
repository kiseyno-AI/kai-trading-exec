import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "optional_columns",
    [(), ("base_quantity",), ("quote_notional",), ("base_quantity", "quote_notional")],
)
def test_duplicate_report_distinguishes_crypto_amounts_and_preserves_legacy_schema(
    tmp_path: Path, optional_columns: tuple[str, ...]
) -> None:
    database = tmp_path / "synthetic-fills.db"
    columns = [
        "id", "broker_order_pk", "broker_fill_id", "symbol", "side",
        "filled_at", "price", "quantity", "payload", "created_at", "updated_at",
        *optional_columns,
    ]
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE execution_fills ("
            + ", ".join(f"{column} TEXT" for column in columns)
            + ")"
        )
        for index, fill_id in enumerate(("first", "second", "repeat")):
            row = [
                fill_id, "order", None, "BTC/USD" if optional_columns else "005930",
                "buy", "2026-11-08T00:00:00+00:00", "60000",
                "0" if optional_columns else ("2" if index == 1 else "1"),
                "{}", None, None,
            ]
            amounts = {
                "base_quantity": ("0.0010", "0.002", "0.001")[index],
                "quote_notional": ("60.0", "120", "60")[index],
            }
            row.extend(amounts[column] for column in optional_columns)
            connection.execute(
                "INSERT INTO execution_fills VALUES ("
                + ",".join("?" for _ in columns) + ")",
                row,
            )

    result = subprocess.run(
        [sys.executable, "scripts/repair_duplicate_execution_fills.py", str(database)],
        capture_output=True,
        text=True,
        check=True,
    )
    report, duplicate = [json.loads(line) for line in result.stdout.splitlines()]

    assert report == {"duplicate_groups": 1, "duplicate_rows": 1, "mode": "dry-run"}
    assert duplicate["keeper_id"] == "first"
    assert duplicate["duplicate_ids"] == ["repeat"]
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0] == 3
