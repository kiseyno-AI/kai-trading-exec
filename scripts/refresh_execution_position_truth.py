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

from app.broker.alpaca import AlpacaBrokerAdapter  # noqa: E402
from app.broker.base import BrokerAdapter, BrokerTokenGrant  # noqa: E402
from app.broker.kis import KisBrokerAdapter  # noqa: E402
from app.core.config import Settings, load_settings  # noqa: E402
from app.db.session import create_db_engine, create_session_factory  # noqa: E402
from app.services.database_cli_security import (  # noqa: E402
    reject_secret_database_url_argument,
)
from app.services.position_truth_remediation import (  # noqa: E402
    ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES,
    LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES,
    SUPPORTED_POSITION_EXCHANGES,
    analyze_legacy_position_attribution,
    analyze_position_truth_remediation,
    apply_legacy_position_attribution,
    apply_position_truth_remediation,
    legacy_attribution_account_scope_source,
)


class _AlpacaPaperUsEquityAttributionAdapter(AlpacaBrokerAdapter):
    """Authenticate the exact US-equity lane used by legacy attribution."""

    def authenticate(self) -> BrokerTokenGrant:
        return self.authenticate_us_equity()


def _legacy_attribution_adapter(settings: Settings) -> BrokerAdapter:
    provider = (
        str(getattr(settings, "runtime_broker_provider", "kis") or "kis")
        .strip()
        .lower()
    )
    if provider == "kis":
        return KisBrokerAdapter(settings=settings)
    if (
        provider == "alpaca"
        and settings.runtime_mode == "paper"
    ):
        return _AlpacaPaperUsEquityAttributionAdapter(settings=settings)
    raise SystemExit(
        "legacy attribution supports KIS routes or the isolated Alpaca paper route"
    )


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
                | ALPACA_PAPER_LEGACY_ATTRIBUTION_EXCHANGES
                | LEGACY_QUARANTINE_ATTRIBUTION_EXCHANGES
            )
        ),
        help=(
            "Analyze legacy unscoped positions against one exact broker route. "
            "US is the isolated Alpaca paper US-equity route; OTCB/OOTC use "
            "live KIS all-US holdings evidence and remain quarantined."
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
    if (
        quarantine_attribution
        and legacy_attribution_account_scope_source(settings=settings) is None
    ):
        raise SystemExit(
            "legacy OTC quarantine attribution requires a stable canonical account "
            "scope from ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET of at least 32 "
            "bytes or EXECUTION_POSITION_ACCOUNT_SCOPE"
        )
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            legacy_adapter = (
                _legacy_attribution_adapter(settings)
                if args.legacy_attribution_exchange
                else None
            )
            if args.legacy_attribution_exchange and args.apply:
                payload = apply_legacy_position_attribution(
                    settings=settings,
                    db=db,
                    auth_session_factory=session_factory,
                    adapter=legacy_adapter,
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
                    adapter=legacy_adapter,
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
