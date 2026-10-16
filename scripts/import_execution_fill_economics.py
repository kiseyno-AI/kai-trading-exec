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
from app.services.artifact_integrity import load_strict_json_object  # noqa: E402
from app.services.database_cli_security import (  # noqa: E402
    reject_secret_database_url_argument,
)
from app.services.execution_fill_economics_import import (  # noqa: E402
    analyze_execution_fill_economics_import,
    apply_execution_fill_economics_import,
)


MAX_EVIDENCE_FILE_BYTES = 5 * 1024 * 1024


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate or import exact per-fill broker fee/tax/currency evidence. "
            "Dry-run is the default. Aggregate records, ambiguous identities, and "
            "conflicting existing economics are rejected; no broker API is called."
        )
    )
    parser.add_argument("evidence_file", type=Path)
    parser.add_argument(
        "--database-url",
        help=(
            "Explicit non-secret SQLAlchemy database URL. Supply credential-bearing "
            "URLs through KAI_EXEC_DATABASE_URL so they are not exposed in argv."
        ),
    )
    parser.add_argument(
        "--runtime-mode",
        choices=("mock", "paper", "live"),
        default=None,
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor", help="Operator identity; required with --apply.")
    parser.add_argument("--reason", help="Audit reason; required with --apply.")
    args = parser.parse_args()

    actor = (args.actor or "").strip()
    reason = (args.reason or "").strip()
    if args.apply and (not actor or not reason):
        raise SystemExit("--apply requires --actor and --reason for auditability")
    try:
        size = args.evidence_file.stat().st_size
    except OSError as exc:
        raise SystemExit(f"economics evidence file is unreadable: {exc}") from exc
    if size > MAX_EVIDENCE_FILE_BYTES:
        raise SystemExit("economics evidence file exceeds the 5 MiB safety limit")
    try:
        document = load_strict_json_object(
            args.evidence_file.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise SystemExit(f"economics evidence file is invalid: {exc}") from exc

    runtime_mode = args.runtime_mode or os.getenv("KAI_EXEC_RUNTIME_MODE") or "mock"
    reject_secret_database_url_argument(args.database_url)
    environment_database_url = (os.getenv("KAI_EXEC_DATABASE_URL") or "").strip()
    database_url = args.database_url or environment_database_url or None
    if runtime_mode == "live" and not database_url:
        raise SystemExit(
            "live economics import requires KAI_EXEC_DATABASE_URL or --database-url"
        )
    settings_kwargs: dict[str, object] = {"runtime_mode": runtime_mode}
    if database_url:
        settings_kwargs["database_url"] = database_url
    settings = Settings(**settings_kwargs)

    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            if args.apply:
                payload = apply_execution_fill_economics_import(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                    document=document,
                    actor=actor,
                    reason=reason,
                )
            else:
                payload = analyze_execution_fill_economics_import(
                    db=db,
                    runtime_mode=settings.runtime_mode,
                    document=document,
                )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
