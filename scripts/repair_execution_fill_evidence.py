#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
API_ROOT = REPO_ROOT / "apps" / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.core.config import Settings  # noqa: E402
from app.db.session import create_db_engine, create_session_factory  # noqa: E402
from app.services.execution_fill_repair import (  # noqa: E402
    analyze_fifo_inventory_repairs,
    analyze_execution_fill_repairs,
    apply_fifo_inventory_repairs,
    apply_execution_fill_repairs,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or apply operator-triggered repairs for missing/duplicate fill "
            "evidence or broker-flat FIFO residuals. Duplicate and ambiguous missing "
            "fill rows are isolated or marked for review without deletion/fabrication. "
            "This script only reads/writes local runtime database tables; it does not "
            "call broker APIs or place/cancel/alter orders."
        ),
    )
    parser.add_argument(
        "--database-url",
        help="SQLAlchemy database URL. Defaults to KAI_EXEC_DATABASE_URL or Settings default.",
    )
    parser.add_argument(
        "--runtime-mode",
        choices=("mock", "paper", "live"),
        default=None,
        help="Runtime mode scope. Defaults to KAI_EXEC_RUNTIME_MODE or Settings default.",
    )
    parser.add_argument(
        "--scope",
        choices=("fill-evidence", "fifo-residuals"),
        default="fill-evidence",
        help=(
            "Repair scope. fifo-residuals reports or isolates local FIFO inventory "
            "only when newer broker reconciliation proves the symbol flat."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply proposed repairs. Omit for dry-run reporting.",
    )
    parser.add_argument(
        "--actor",
        help="Operator identity for audit events. Required with --apply.",
    )
    parser.add_argument(
        "--reason",
        help="Operator reason for audit events. Required with --apply.",
    )
    args = parser.parse_args()

    actor = (args.actor or "").strip()
    reason = (args.reason or "").strip()
    if args.apply and (not actor or not reason):
        raise SystemExit("--apply requires --actor and --reason for auditability")

    runtime_mode = args.runtime_mode or os.getenv("KAI_EXEC_RUNTIME_MODE") or "mock"
    if runtime_mode == "live" and not args.database_url:
        raise SystemExit("live fill remediation requires --database-url explicitly")

    settings_kwargs: dict[str, object] = {}
    if args.database_url:
        settings_kwargs["database_url"] = args.database_url
    settings_kwargs["runtime_mode"] = runtime_mode
    settings = Settings(**settings_kwargs)

    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            if args.scope == "fifo-residuals" and args.apply:
                payload = apply_fifo_inventory_repairs(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                    actor=actor,
                    reason=reason,
                )
            elif args.scope == "fifo-residuals":
                payload = analyze_fifo_inventory_repairs(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                )
            elif args.apply:
                payload = apply_execution_fill_repairs(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                    actor=actor,
                    reason=reason,
                )
            else:
                payload = analyze_execution_fill_repairs(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
