from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import re
from urllib.parse import urlparse

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.models.runtime import ExecutionPosition, RuntimeRemediationEvent


REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE = (
    "reviewed_corporate_action_quarantine"
)
REVIEWED_CORPORATE_ACTION_OUTCOME = "position_corporate_action_quarantined"
REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION = (
    "reviewed_corporate_action_quarantine"
)
REVIEWED_CORPORATE_ACTION_REASON = "corporate_action_nontradeable"
REVIEWED_CORPORATE_ACTION_INVALID_REASON = (
    "corporate_action_quarantine_invalid"
)
REVIEWED_CORPORATE_ACTION_SAFETY_POLICY_VERSION = (
    "reviewed-corporate-action-quarantine-v1"
)
REVIEWED_CORPORATE_ACTION_REVIEW_SOURCE = "operator_reviewed_sec_filings"
REVIEWED_CORPORATE_ACTION_SECURITY_STATUS = (
    "no_longer_publicly_traded_or_outstanding"
)

_SUPPORTED_EXCHANGES = frozenset({"NASD", "NYSE", "AMEX"})
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_REVIEW_EVIDENCE_KEYS = frozenset(
    {
        "source",
        "issuer_name",
        "corporate_action",
        "effective_date",
        "security_status",
        "evidence_refs",
        "reviewed_at",
    }
)
_QUARANTINE_MARKER_KEYS = frozenset(
    {
        "corporate_action_reviewed_at",
        "review_evidence_digest",
        "safety_policy_version",
    }
)
_EXPECTED_SAFETY_INVARIANTS = {
    "broker_actions_allowed": False,
    "collision_scope": "mode_provider_account_symbol_any_exchange",
    "provider": "kis",
    "account_identity_preserved": True,
    "position_quantity_preserved": True,
    "position_average_price_preserved": True,
    "broker_reconciliation_continues": True,
    "quote_strategy_monitoring_allowed": False,
    "route_disposition": REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION,
}


class ReviewedCorporateActionQuarantineState(str, Enum):
    VERIFIED = "verified"
    INVALID_MARKED = "invalid_marked"
    UNRELATED = "unrelated"


def validate_reviewed_corporate_action_quarantine(
    *,
    position: ExecutionPosition,
    event: RuntimeRemediationEvent | None,
) -> ReviewedCorporateActionQuarantineState:
    """Classify a position using immutable audit evidence, never mutable labels alone."""

    sync_context = _position_sync_context(position)
    marked = _has_quarantine_marker(sync_context) or event is not None
    if not marked:
        return ReviewedCorporateActionQuarantineState.UNRELATED
    if not isinstance(sync_context, dict) or event is None:
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    exchange = str(position.exchange or "").strip().upper()
    if not (
        position.provider == "kis"
        and str(position.account_scope or "").strip()
        and exchange in _SUPPORTED_EXCHANGES
        and str(position.symbol or "").strip().upper() == position.symbol
        and sync_context.get("asset_class") == "us_equity"
        and sync_context.get("exchange") == exchange
        and sync_context.get("monitoring_eligible") is False
        and sync_context.get("trade_eligible") is False
        and sync_context.get("manual_review_required") is False
        and sync_context.get("manual_review_reason")
        == REVIEWED_CORPORATE_ACTION_REASON
        and sync_context.get("route_disposition")
        == REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
        and sync_context.get("safety_policy_version")
        == REVIEWED_CORPORATE_ACTION_SAFETY_POLICY_VERSION
    ):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    sync_digest = sync_context.get("review_evidence_digest")
    sync_reviewed_at = _aware_iso_datetime(
        sync_context.get("corporate_action_reviewed_at")
    )
    if not _valid_digest(sync_digest) or sync_reviewed_at is None:
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    if not (
        event.mode == position.mode
        and event.candidate_type == REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE
        and event.candidate_key == position.id
        and event.subject_id == position.id
        and event.outcome == REVIEWED_CORPORATE_ACTION_OUTCOME
    ):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    summary = event.evidence_summary
    if not isinstance(summary, dict):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED
    if not (
        summary.get("mode") == position.mode
        and summary.get("position_id") == position.id
        and summary.get("provider") == position.provider
        and summary.get("account_scope") == position.account_scope
        and summary.get("exchange") == exchange
        and summary.get("symbol") == position.symbol
        and summary.get("asset_class") == "us_equity"
        and summary.get("quantity") == _canonical_decimal(position.quantity)
        and summary.get("base_quantity")
        == _canonical_decimal(position.base_quantity)
        and summary.get("average_price")
        == _canonical_decimal(position.average_price)
        and summary.get("route_disposition")
        == REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
        and summary.get("safety_policy_version")
        == REVIEWED_CORPORATE_ACTION_SAFETY_POLICY_VERSION
        and summary.get("safety_invariants") == _EXPECTED_SAFETY_INVARIANTS
        and summary.get("orders_submitted") == 0
        and summary.get("orders_amended") == 0
        and summary.get("orders_canceled") == 0
    ):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    review_evidence = summary.get("review_evidence")
    review_digest = summary.get("review_evidence_digest")
    reviewed_at = _validated_reviewed_at(review_evidence)
    if not (
        reviewed_at is not None
        and reviewed_at == sync_reviewed_at
        and summary.get("corporate_action_reviewed_at")
        == reviewed_at.isoformat()
        and _valid_digest(review_digest)
        and review_digest == sync_digest
        and review_digest == _payload_digest(review_evidence)
    ):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    snapshot_evidence = summary.get("snapshot_evidence")
    snapshot_digest = summary.get("snapshot_evidence_digest")
    fingerprint = summary.get("evidence_fingerprint")
    if not (
        isinstance(snapshot_evidence, dict)
        and _valid_digest(snapshot_digest)
        and snapshot_digest == _payload_digest(snapshot_evidence)
        and _valid_digest(fingerprint)
        and _valid_digest(summary.get("raw_payload_digest"))
        and isinstance(summary.get("updated_from"), str)
        and str(summary.get("updated_from") or "").strip()
    ):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    fingerprint_material = {
        key: summary.get(key)
        for key in (
            "safety_policy_version",
            "safety_invariants",
            "review_evidence",
            "review_evidence_digest",
            "snapshot_evidence_digest",
            "mode",
            "position_id",
            "provider",
            "account_scope",
            "exchange",
            "symbol",
            "quantity",
            "base_quantity",
            "average_price",
            "updated_from",
            "last_reconciled_at",
            "raw_payload_digest",
        )
    }
    if fingerprint != _payload_digest(fingerprint_material):
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED

    occurred_at = _event_datetime(event.occurred_at)
    if occurred_at is None or reviewed_at > occurred_at:
        return ReviewedCorporateActionQuarantineState.INVALID_MARKED
    return ReviewedCorporateActionQuarantineState.VERIFIED


def load_reviewed_corporate_action_quarantine_state(
    *,
    db: Session,
    position: ExecutionPosition,
    for_update: bool = False,
) -> ReviewedCorporateActionQuarantineState:
    return load_reviewed_corporate_action_quarantine_states(
        db=db,
        positions=[position],
        for_update=for_update,
    )[position.id]


def load_reviewed_corporate_action_quarantine_states(
    *,
    db: Session,
    positions: list[ExecutionPosition],
    for_update: bool = False,
) -> dict[str, ReviewedCorporateActionQuarantineState]:
    if not positions:
        return {}
    position_ids = {position.id for position in positions}
    modes = {position.mode for position in positions}
    statement = (
        select(RuntimeRemediationEvent)
        .where(
            RuntimeRemediationEvent.mode.in_(sorted(modes)),
            RuntimeRemediationEvent.candidate_type
            == REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE,
            or_(
                RuntimeRemediationEvent.candidate_key.in_(sorted(position_ids)),
                RuntimeRemediationEvent.subject_id.in_(sorted(position_ids)),
            ),
        )
        .order_by(
            RuntimeRemediationEvent.occurred_at.desc(),
            RuntimeRemediationEvent.id.desc(),
        )
    )
    if for_update:
        statement = statement.with_for_update()
    events = list(db.scalars(statement).all())
    states: dict[str, ReviewedCorporateActionQuarantineState] = {}
    for position in positions:
        related = [
            event
            for event in events
            if event.mode == position.mode
            and (
                event.candidate_key == position.id
                or event.subject_id == position.id
            )
        ]
        if len(related) > 1:
            states[position.id] = (
                ReviewedCorporateActionQuarantineState.INVALID_MARKED
            )
            continue
        states[position.id] = validate_reviewed_corporate_action_quarantine(
            position=position,
            event=related[0] if related else None,
        )
    return states


def _position_sync_context(position: ExecutionPosition) -> object:
    payload = position.raw_payload
    return payload.get("_position_sync") if isinstance(payload, dict) else None


def _has_quarantine_marker(sync_context: object) -> bool:
    if not isinstance(sync_context, dict):
        return False
    return bool(
        sync_context.get("route_disposition")
        == REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
        or sync_context.get("manual_review_reason")
        in (
            REVIEWED_CORPORATE_ACTION_REASON,
            REVIEWED_CORPORATE_ACTION_INVALID_REASON,
        )
        or any(key in sync_context for key in _QUARANTINE_MARKER_KEYS)
    )


def _validated_reviewed_at(evidence: object) -> datetime | None:
    if not isinstance(evidence, dict) or set(evidence) != _REVIEW_EVIDENCE_KEYS:
        return None
    if not (
        evidence.get("source") == REVIEWED_CORPORATE_ACTION_REVIEW_SOURCE
        and evidence.get("corporate_action") == "dissolution"
        and evidence.get("security_status")
        == REVIEWED_CORPORATE_ACTION_SECURITY_STATUS
        and str(evidence.get("issuer_name") or "").strip()
    ):
        return None
    try:
        effective_date = date.fromisoformat(str(evidence.get("effective_date") or ""))
    except ValueError:
        return None
    reviewed_at = _aware_iso_datetime(evidence.get("reviewed_at"))
    if reviewed_at is None or effective_date > reviewed_at.date():
        return None
    refs = evidence.get("evidence_refs")
    if (
        not isinstance(refs, list)
        or len(refs) < 2
        or not all(isinstance(ref, str) for ref in refs)
        or refs != sorted(refs)
    ):
        return None
    normalized_refs: list[str] = []
    for value in refs:
        ref = str(value or "").strip()
        try:
            parsed = urlparse(ref)
        except ValueError:
            return None
        if (
            parsed.scheme != "https"
            or (parsed.hostname or "").lower() not in {"sec.gov", "www.sec.gov"}
            or not parsed.path.strip("/")
        ):
            return None
        normalized_refs.append(ref)
    if len(set(normalized_refs)) != len(normalized_refs):
        return None
    return reviewed_at


def _aware_iso_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _event_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _canonical_decimal(value: object) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    normalized = parsed.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _payload_digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _SHA256_PATTERN.fullmatch(value) is not None
