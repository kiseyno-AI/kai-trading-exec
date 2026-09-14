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
from app.services.database_cli_security import (  # noqa: E402
    reject_secret_database_url_argument,
)
from app.services.position_truth_remediation import (  # noqa: E402
    LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES,
    SUPPORTED_POSITION_EXCHANGES,
    analyze_legacy_position_attribution,
    analyze_position_truth_remediation,
    apply_legacy_position_attribution,
    apply_position_truth_remediation,
)
from app.services.position_identity import stable_account_scope_hmac_key  # noqa: E402


def _approved_position_evidence(values: list[str]) -> dict[str, str]:
    approved: dict[str, str] = {}
    for value in values:
        if value.count("=") != 1:
            raise SystemExit("approved position evidence is invalid")
        raw_position_id, raw_fingerprint = value.split("=", maxsplit=1)
        position_id = raw_position_id.strip()
        fingerprint = raw_fingerprint.strip().lower()
        if (
            not position_id
            or position_id in approved
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            raise SystemExit("approved position evidence is invalid")
        approved[position_id] = fingerprint
    return approved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or refresh stale execution position truth through read-only broker "
            "account inquiries. This command never places, amends, or cancels orders."
        )
    )
    parser.add_argument(
        "--database-url",
        help=(
            "Explicit non-secret SQLAlchemy database URL. Supply credential-bearing "
            "URLs through KAI_EXEC_DATABASE_URL so they are not exposed in argv."
        ),
    )
    parser.add_argument("--runtime-mode", choices=("mock", "paper", "live"), default=None)
    parser.add_argument(
        "--legacy-attribution-exchange",
        choices=tuple(
            sorted(
                SUPPORTED_POSITION_EXCHANGES
                | LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
            )
        ),
        help=(
            "Analyze legacy unscoped positions against one exact KIS exchange. "
            "OTCB/OOTC use live all-US holdings evidence and remain quarantined."
        ),
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--approve-position",
        action="append",
        default=[],
        metavar="POSITION_ID=64HEX",
        help="Approve one dry-run evidence fingerprint; repeat for each position.",
    )
    parser.add_argument("--actor")
    parser.add_argument("--reason")
    args = parser.parse_args(argv)

    runtime_mode = args.runtime_mode or os.getenv("KAI_EXEC_RUNTIME_MODE") or "mock"
    quarantine_attribution = (
        args.legacy_attribution_exchange
        in LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
    )
    if quarantine_attribution and runtime_mode != "live":
        raise SystemExit(
            "legacy OTC quarantine attribution requires --runtime-mode live"
        )
    reject_secret_database_url_argument(args.database_url)
    environment_database_url = (os.getenv("KAI_EXEC_DATABASE_URL") or "").strip()
    database_url = args.database_url or environment_database_url or None
    actor = (args.actor or "").strip()
    reason = (args.reason or "").strip()
    approved_evidence = _approved_position_evidence(args.approve_position)
    legacy_apply = bool(args.legacy_attribution_exchange and args.apply)
    if approved_evidence and not legacy_apply:
        raise SystemExit(
            "--approve-position is valid only with legacy attribution --apply"
        )
    if legacy_apply and not approved_evidence:
        raise SystemExit(
            "legacy attribution apply requires at least one --approve-position"
        )
    if runtime_mode == "live" and not database_url:
        raise SystemExit(
            "live position remediation requires KAI_EXEC_DATABASE_URL or --database-url"
        )
    if not database_url:
        raise SystemExit("--database-url or KAI_EXEC_DATABASE_URL is required")
    if args.apply and (not actor or not reason):
        raise SystemExit("--apply requires --actor and --reason for auditability")

    settings = load_settings().model_copy(
        update={"database_url": database_url, "runtime_mode": runtime_mode}
    )
    if quarantine_attribution and stable_account_scope_hmac_key(settings) is None:
        raise SystemExit(
            "legacy OTC quarantine attribution requires a stable account scope secret "
            "of at least 32 bytes"
        )
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            if args.legacy_attribution_exchange and args.apply:
                payload = apply_legacy_position_attribution(
                    settings=settings,
                    db=db,
                    auth_session_factory=session_factory,
                    adapter=KisBrokerAdapter(settings=settings),
                    exchange=args.legacy_attribution_exchange,
                    approved_evidence=approved_evidence,
                    actor=actor,
                    reason=reason,
                )
            elif args.legacy_attribution_exchange:
                payload = analyze_legacy_position_attribution(
                    settings=settings,
                    db=db,
                    auth_session_factory=session_factory,
                    adapter=KisBrokerAdapter(settings=settings),
                    exchange=args.legacy_attribution_exchange,
                )
            elif args.apply:
                payload = apply_position_truth_remediation(
                    settings=settings,
                    db=db,
                    auth_session_factory=session_factory,
                    adapter=KisBrokerAdapter(settings=settings),
                    actor=actor,
                    reason=reason,
                )
            else:
                payload = analyze_position_truth_remediation(settings=settings, db=db)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
