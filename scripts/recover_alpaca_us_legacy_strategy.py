#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
API_ROOT = REPO_ROOT / "apps" / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.broker.alpaca import AlpacaBrokerAdapter  # noqa: E402
from app.core.config import load_settings  # noqa: E402
from app.db.session import create_db_engine, create_session_factory  # noqa: E402
from app.services.alpaca_legacy_strategy_recovery import (  # noqa: E402
    analyze_alpaca_us_legacy_strategy_exposure,
    apply_alpaca_us_legacy_strategy_exposure,
)
from app.services.database_cli_security import (  # noqa: E402
    reject_secret_database_url_argument,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run the paper Alpaca US legacy-strategy exposure proof, or "
            "apply an exact approved fingerprint as an audited opening-inventory "
            "seed. This command never places, amends, or cancels broker orders."
        )
    )
    parser.add_argument(
        "--database-url",
        help=(
            "Explicit non-secret SQLAlchemy URL. Prefer "
            "KAI_EXEC_DATABASE_URL for credential-bearing URLs."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the exact current proof instead of the default dry-run.",
    )
    parser.add_argument(
        "--approve-fingerprint",
        help="The 64-hex approval_fingerprint emitted by a prior dry-run.",
    )
    parser.add_argument("--actor", help="Operator identity recorded in the audit event.")
    parser.add_argument("--reason", help="Operator reason recorded in the audit event.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    reject_secret_database_url_argument(args.database_url)
    database_url = (
        args.database_url
        or (os.getenv("KAI_EXEC_DATABASE_URL") or "").strip()
        or None
    )
    if not database_url:
        raise SystemExit(
            "KAI_EXEC_DATABASE_URL or --database-url is required; recovery "
            "never guesses a runtime database"
        )
    fingerprint = str(args.approve_fingerprint or "").strip().lower()
    actor = str(args.actor or "").strip()
    reason = str(args.reason or "").strip()
    if args.apply and (not fingerprint or not actor or not reason):
        raise SystemExit(
            "--apply requires --approve-fingerprint, --actor, and --reason"
        )
    if not args.apply and (fingerprint or actor or reason):
        raise SystemExit(
            "approval, actor, and reason arguments are valid only with --apply"
        )

    # Apply the same provider-isolation gates as the paper launcher before
    # Settings validation; a shared .env may otherwise still enable KIS-only
    # coordinators.
    os.environ["KAI_EXEC_RUNTIME_MODE"] = "paper"
    os.environ["KAI_EXEC_RUNTIME_BROKER_PROVIDER"] = "alpaca"
    os.environ["KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED"] = "false"
    os.environ["KAI_EXEC_KIS_US_TRADING_ENABLED"] = "false"
    os.environ["KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED"] = "false"
    os.environ["KAI_EXEC_DATABASE_URL"] = database_url
    settings = load_settings().model_copy(
        update={
            "database_url": database_url,
            "runtime_mode": "paper",
            "runtime_broker_provider": "alpaca",
        }
    )
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    adapter = AlpacaBrokerAdapter(settings=settings)
    try:
        # US-equity authentication validates the account without requiring the
        # account's crypto-trading status. Every later call is read-only.
        grant = adapter.authenticate_us_equity()
        with session_factory() as db:
            if args.apply:
                result = apply_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    access_token=grant.access_token,
                    approved_fingerprint=fingerprint,
                    actor=actor,
                    reason=reason,
                )
            else:
                result = analyze_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                    access_token=grant.access_token,
                )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
