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

from app.broker.kis import KisBrokerAdapter  # noqa: E402
from app.core.config import load_settings  # noqa: E402
from app.db.session import create_db_engine, create_session_factory  # noqa: E402
from app.services.position_truth_remediation import (  # noqa: E402
    analyze_position_truth_remediation,
    apply_position_truth_remediation,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or refresh stale execution position truth through read-only broker "
            "account inquiries. This command never places, amends, or cancels orders."
        )
    )
    parser.add_argument("--database-url", help="Explicit SQLAlchemy database URL.")
    parser.add_argument("--runtime-mode", choices=("mock", "paper", "live"), default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor")
    parser.add_argument("--reason")
    args = parser.parse_args()

    runtime_mode = args.runtime_mode or os.getenv("KAI_EXEC_RUNTIME_MODE") or "mock"
    database_url = args.database_url or os.getenv("KAI_EXEC_DATABASE_URL")
    if runtime_mode == "live" and not args.database_url:
        raise SystemExit("live position remediation requires --database-url explicitly")
    if not database_url:
        raise SystemExit("--database-url or KAI_EXEC_DATABASE_URL is required")
    if args.apply and (not args.actor or not args.reason):
        raise SystemExit("--apply requires --actor and --reason for auditability")

    settings = load_settings().model_copy(
        update={"database_url": database_url, "runtime_mode": runtime_mode}
    )
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            if args.apply:
                payload = apply_position_truth_remediation(
                    settings=settings,
                    db=db,
                    adapter=KisBrokerAdapter(settings=settings),
                    actor=args.actor or "",
                    reason=args.reason or "",
                )
            else:
                payload = analyze_position_truth_remediation(settings=settings, db=db)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
