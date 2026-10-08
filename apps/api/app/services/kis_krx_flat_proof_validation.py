from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, undefer

from app.core.config import Settings
from app.models.runtime import ExecutionPosition, RuntimeRemediationEvent
from app.services.kis_krx_flat_rebind_contract import (
    KIS_KRX_FLAT_PROOF_GENERIC_SYNC_RAW_FIELDS,
    KIS_KRX_FLAT_PROOF_MAX_AGE_SECONDS,
    KIS_KRX_FLAT_PROOF_REFRESH_CANDIDATE_TYPE,
    KIS_KRX_FLAT_PROOF_REFRESH_EVENT_PROOF_FIELDS,
    KIS_KRX_FLAT_PROOF_REFRESH_OUTCOME,
    KIS_KRX_FLAT_PROOF_SOURCE_CANONICAL,
    KIS_KRX_FLAT_PROOF_SOURCE_GENERIC_SYNC,
    KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY,
    KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE,
    KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME,
    KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS,
    KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION,
    KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS,
    is_lower_hex_sha256,
    kis_krx_flat_absence_proof_digest,
    kis_krx_flat_configured_account_binding_digest,
    kis_krx_flat_evidence_fingerprint,
    kis_krx_flat_proof_refresh_approval_fingerprint,
    kis_krx_flat_proof_refresh_candidate_key,
)
from app.services.position_identity import (
    PositionIdentity,
    PositionIdentityConflictError,
    position_identity_from_columns,
    position_identity_from_payload,
)


_KIS_PERSISTED_DOMESTIC_RESPONSE_FIELDS = frozenset(
    {
        "ctx_area_fk100",
        "ctx_area_nk100",
        "msg1",
        "msg_cd",
        "output1",
        "output2",
        "rt_cd",
    }
)


def load_kis_krx_flat_proof_lineage(
    *,
    settings: Settings,
    db: Session,
    position_id: str,
    symbol: str,
    identity: PositionIdentity,
    for_update: bool = False,
) -> tuple[RuntimeRemediationEvent, dict[str, object]] | None:
    """Validate every persisted generation back to the immutable bind root."""
    statement = (
        select(RuntimeRemediationEvent)
        .options(undefer(RuntimeRemediationEvent.evidence_summary))
        .where(
            RuntimeRemediationEvent.mode == settings.runtime_mode,
            RuntimeRemediationEvent.subject_id == position_id,
            or_(
                and_(
                    RuntimeRemediationEvent.candidate_type
                    == KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE,
                    RuntimeRemediationEvent.outcome
                    == KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME,
                ),
                and_(
                    RuntimeRemediationEvent.candidate_type
                    == KIS_KRX_FLAT_PROOF_REFRESH_CANDIDATE_TYPE,
                    RuntimeRemediationEvent.outcome
                    == KIS_KRX_FLAT_PROOF_REFRESH_OUTCOME,
                ),
            ),
        )
        .order_by(
            RuntimeRemediationEvent.occurred_at.desc(),
            RuntimeRemediationEvent.created_at.desc(),
            RuntimeRemediationEvent.id.desc(),
        )
    )
    if for_update:
        statement = statement.with_for_update()
    events = db.scalars(statement).all()
    if not events or len(events) > 4096:
        return None
    if len({event.id for event in events}) != len(events):
        return None

    for index, event in enumerate(events):
        if not kis_krx_flat_proof_event_core_matches(
            settings=settings,
            event=event,
            position_id=position_id,
            symbol=symbol,
            identity=identity,
        ):
            return None
        is_last = index == len(events) - 1
        if is_last:
            if event.candidate_type != KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE:
                return None
            continue
        if event.candidate_type != KIS_KRX_FLAT_PROOF_REFRESH_CANDIDATE_TYPE:
            return None
        previous = events[index + 1]
        summary = event.evidence_summary
        previous_summary = previous.evidence_summary
        if not isinstance(summary, dict) or not isinstance(previous_summary, dict):
            return None
        occurred_at = event_datetime(event.occurred_at)
        previous_occurred_at = event_datetime(previous.occurred_at)
        superseded_at = aware_iso_datetime(
            summary.get("superseded_reconciliation_observed_at")
        )
        source_chronology_valid = (
            summary.get("source_row_state")
            != KIS_KRX_FLAT_PROOF_SOURCE_GENERIC_SYNC
            or (
                superseded_at is not None
                and previous_occurred_at is not None
                and previous_occurred_at < superseded_at
            )
        )
        if not (
            summary.get("previous_proof_event_id") == previous.id
            and summary.get("previous_proof_candidate_key")
            == previous.candidate_key
            and summary.get("previous_broker_snapshot_observed_at")
            == previous_summary.get("broker_snapshot_observed_at")
            and occurred_at is not None
            and previous_occurred_at is not None
            and previous_occurred_at < occurred_at
            and source_chronology_valid
        ):
            return None
    latest = events[0]
    latest_summary = latest.evidence_summary
    return (
        (latest, latest_summary)
        if isinstance(latest_summary, dict)
        else None
    )


def kis_krx_flat_proof_event_core_matches(
    *,
    settings: Settings,
    event: RuntimeRemediationEvent,
    position_id: str,
    symbol: str,
    identity: PositionIdentity,
) -> bool:
    summary = event.evidence_summary
    if not isinstance(summary, dict):
        return False
    is_bind = (
        event.candidate_type == KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE
        and event.outcome == KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME
    )
    is_refresh = (
        event.candidate_type == KIS_KRX_FLAT_PROOF_REFRESH_CANDIDATE_TYPE
        and event.outcome == KIS_KRX_FLAT_PROOF_REFRESH_OUTCOME
    )
    expected_fields = (
        KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS
        if is_bind
        else KIS_KRX_FLAT_PROOF_REFRESH_EVENT_PROOF_FIELDS
        if is_refresh
        else frozenset()
    )
    observed_at = aware_iso_datetime(summary.get("broker_snapshot_observed_at"))
    occurred_at = event_datetime(event.occurred_at)
    configured_digest = kis_krx_flat_configured_account_binding_digest(
        settings=settings
    )
    absence_digest = kis_krx_flat_absence_proof_digest(
        runtime_mode=settings.runtime_mode,
        identity=identity,
        symbol=symbol,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        configured_account_binding_digest=configured_digest,
    )
    prior_digest = summary.get("prior_position_digest")
    expected_fingerprint = (
        kis_krx_flat_evidence_fingerprint(
            runtime_mode=settings.runtime_mode,
            position_id=position_id,
            prior_position_digest=str(prior_digest),
            identity=identity,
            symbol=symbol,
            snapshot_source="kis:account_snapshot",
            holdings_complete=True,
            open_orders_complete=True,
            holding_absent=True,
            open_order_absent=True,
            absence_proof_digest=absence_digest,
            configured_account_binding_digest=configured_digest,
        )
        if is_lower_hex_sha256(prior_digest)
        else None
    )
    if not (
        event.mode == settings.runtime_mode
        and event.subject_id == position_id
        and set(summary) == expected_fields
        and summary.get("symbol") == symbol
        and summary.get("provider") == identity.provider
        and summary.get("account_scope") == identity.account_scope
        and summary.get("exchange") == identity.exchange
        and summary.get("asset_class") == "equity"
        and summary.get("broker_snapshot_source") == "kis:account_snapshot"
        and observed_at is not None
        and serialize_datetime(observed_at)
        == summary.get("broker_snapshot_observed_at")
        and occurred_at is not None
        and occurred_at >= observed_at
        and occurred_at - observed_at
        <= timedelta(
            seconds=min(
                settings.exec_agent_max_account_snapshot_age_seconds,
                KIS_KRX_FLAT_PROOF_MAX_AGE_SECONDS,
            )
        )
        and summary.get("holdings_complete") is True
        and summary.get("open_orders_complete") is True
        and summary.get("holding_absent") is True
        and summary.get("open_order_absent") is True
        and summary.get("absence_proof_digest") == absence_digest
        and summary.get("configured_account_binding_digest") == configured_digest
        and expected_fingerprint is not None
        and summary.get("evidence_fingerprint") == expected_fingerprint
        and summary.get("safety_policy_version")
        == KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
        and summary.get("historical_account_attributed") is False
        and summary.get("current_flat_truth_bound") is True
        and all(
            type(summary.get(field)) is int and summary[field] == 0
            for field in KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS
        )
    ):
        return False
    if is_bind:
        return event.candidate_key == position_id

    previous_observed_at = aware_iso_datetime(
        summary.get("previous_broker_snapshot_observed_at")
    )
    previous_event_id = summary.get("previous_proof_event_id")
    previous_candidate_key = summary.get("previous_proof_candidate_key")
    source_row_state = summary.get("source_row_state")
    superseded_at = aware_iso_datetime(
        summary.get("superseded_reconciliation_observed_at")
    )
    superseded_digest = summary.get("superseded_payload_digest")
    refresh_fingerprint = summary.get("refresh_approval_fingerprint")
    source_shape_valid = (
        source_row_state == KIS_KRX_FLAT_PROOF_SOURCE_CANONICAL
        and summary.get("superseded_reconciliation_observed_at") is None
        and superseded_digest is None
    ) or (
        source_row_state == KIS_KRX_FLAT_PROOF_SOURCE_GENERIC_SYNC
        and superseded_at is not None
        and previous_observed_at is not None
        and observed_at is not None
        and previous_observed_at < superseded_at < observed_at
        and is_lower_hex_sha256(superseded_digest)
    )
    expected_refresh_fingerprint = (
        kis_krx_flat_proof_refresh_approval_fingerprint(
            runtime_mode=settings.runtime_mode,
            position_id=position_id,
            symbol=symbol,
            identity=identity,
            previous_proof_event_id=previous_event_id,
            previous_proof_candidate_key=previous_candidate_key,
            previous_snapshot_observed_at=str(
                summary.get("previous_broker_snapshot_observed_at")
            ),
            source_row_state=str(source_row_state),
            superseded_reconciliation_observed_at=(
                summary.get("superseded_reconciliation_observed_at")
                if isinstance(
                    summary.get("superseded_reconciliation_observed_at"), str
                )
                else None
            ),
            superseded_payload_digest=(
                superseded_digest
                if isinstance(superseded_digest, str)
                else None
            ),
            absence_proof_digest=absence_digest,
            configured_account_binding_digest=configured_digest,
        )
        if isinstance(previous_event_id, str)
        and previous_event_id
        and isinstance(previous_candidate_key, str)
        and previous_candidate_key
        and previous_observed_at is not None
        and source_shape_valid
        else None
    )
    return bool(
        previous_observed_at is not None
        and observed_at is not None
        and previous_observed_at < observed_at
        and source_shape_valid
        and is_lower_hex_sha256(refresh_fingerprint)
        and refresh_fingerprint == expected_refresh_fingerprint
        and event.candidate_key
        == kis_krx_flat_proof_refresh_candidate_key(
            position_id=position_id,
            snapshot_observed_at=str(summary["broker_snapshot_observed_at"]),
            evidence_fingerprint=str(summary["evidence_fingerprint"]),
            refresh_approval_fingerprint=str(refresh_fingerprint),
        )
    )


def canonical_flat_row_matches_latest_proof(
    *,
    settings: Settings,
    position: ExecutionPosition,
    symbol: str,
    identity: PositionIdentity,
    latest_summary: dict[str, object],
) -> bool:
    raw = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    observed_at = aware_iso_datetime(raw.get("snapshot_observed_at"))
    expected_absence_digest = kis_krx_flat_absence_proof_digest(
        runtime_mode=settings.runtime_mode,
        identity=identity,
        symbol=symbol,
        snapshot_source="kis:account_snapshot",
        holdings_complete=True,
        open_orders_complete=True,
        holding_absent=True,
        open_order_absent=True,
        configured_account_binding_digest=(
            kis_krx_flat_configured_account_binding_digest(settings=settings)
        ),
    )
    try:
        column_identity = position_identity_from_columns(position)
        payload_identity = position_identity_from_payload(raw)
    except PositionIdentityConflictError:
        return False
    return bool(
        position.mode == settings.runtime_mode
        and position.symbol == symbol
        and column_identity == identity
        and payload_identity == identity
        and canonical_decimal(position.quantity) == "0"
        and position.base_quantity
        == KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY
        and position.average_price is None
        and position.updated_from == "reconciliation"
        and set(raw) == KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS
        and raw.get("provider") == identity.provider
        and raw.get("mode") == settings.runtime_mode
        and raw.get("asset_class") == "equity"
        and raw.get("exchange") == identity.exchange
        and raw.get("snapshot_source") == "kis:account_snapshot"
        and observed_at is not None
        and serialize_datetime(observed_at) == raw.get("snapshot_observed_at")
        and serialize_datetime(position.last_reconciled_at)
        == raw.get("snapshot_observed_at")
        and raw.get("holdings_coverage") == "complete"
        and raw.get("open_orders_coverage") == "complete"
        and raw.get("holding_absent") is True
        and raw.get("open_order_absent") is True
        and raw.get("absence_proof_digest") == expected_absence_digest
        and raw.get("configured_account_binding_digest")
        == kis_krx_flat_configured_account_binding_digest(settings=settings)
        and raw.get("historical_account_attributed") is False
        and raw.get("current_flat_truth_bound") is True
        and raw.get("safety_policy_version")
        == KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
        and latest_summary.get("broker_snapshot_observed_at")
        == raw.get("snapshot_observed_at")
        and latest_summary.get("absence_proof_digest")
        == raw.get("absence_proof_digest")
        and latest_summary.get("configured_account_binding_digest")
        == raw.get("configured_account_binding_digest")
    )


def canonical_flat_row_matches_generic_sync_supersede(
    *,
    settings: Settings,
    position: ExecutionPosition,
    symbol: str,
    identity: PositionIdentity,
    latest_event: RuntimeRemediationEvent,
    latest_summary: dict[str, object],
) -> bool:
    raw = position.raw_payload if isinstance(position.raw_payload, dict) else {}
    reconciled_at = aware_datetime(position.last_reconciled_at, assume_utc=True)
    proof_observed_at = aware_iso_datetime(
        latest_summary.get("broker_snapshot_observed_at")
    )
    proof_occurred_at = event_datetime(latest_event.occurred_at)
    try:
        column_identity = position_identity_from_columns(position)
        payload_identity = position_identity_from_payload(raw)
    except PositionIdentityConflictError:
        return False
    return bool(
        position.mode == settings.runtime_mode
        and position.symbol == symbol
        and column_identity == identity
        and payload_identity == identity
        and canonical_decimal(position.quantity) == "0"
        and position.base_quantity
        == KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY
        and position.average_price is None
        and position.updated_from == "reconciliation"
        and set(raw) == KIS_KRX_FLAT_PROOF_GENERIC_SYNC_RAW_FIELDS
        and raw.get("exchange") == "KRX"
        and raw.get("holdings_coverage") == "complete"
        and raw.get("open_orders_coverage") == "complete"
        and raw.get("open_orders_error") is None
        and raw.get("account_number_masked")
        == masked_account_number(settings.kis_account_number)
        and generic_sync_snapshot_payload_proves_symbol_absent(
            raw=raw,
            symbol=symbol,
        )
        and reconciled_at is not None
        and proof_observed_at is not None
        and proof_occurred_at is not None
        and reconciled_at > proof_observed_at
        and reconciled_at > proof_occurred_at
    )


def generic_sync_snapshot_payload_proves_symbol_absent(
    *,
    raw: dict[str, object],
    symbol: str,
) -> bool:
    """Validate the exact successful KIS payload that generic sync persisted.

    This path exists only to recover the narrowly observed proof-overwrite shape.
    It deliberately rejects response-shape drift, malformed rows, duplicate
    identities, and even a zero-quantity target row: recovery requires exact
    target-symbol absence, not merely a parsed zero position.
    """
    balance = raw.get("balance")
    open_orders = raw.get("open_orders")
    if not (
        isinstance(balance, dict)
        and isinstance(open_orders, dict)
        and set(balance) == _KIS_PERSISTED_DOMESTIC_RESPONSE_FIELDS
        and set(open_orders) == _KIS_PERSISTED_DOMESTIC_RESPONSE_FIELDS
        and balance.get("rt_cd") == "0"
        and open_orders.get("rt_cd") == "0"
        and isinstance(balance.get("output1"), list)
        and isinstance(open_orders.get("output1"), list)
    ):
        return False
    normalized_symbol = symbol.strip().upper()
    return _holding_rows_prove_symbol_absent(
        rows=balance["output1"],
        symbol=normalized_symbol,
    ) and _open_order_rows_prove_symbol_absent(
        rows=open_orders["output1"],
        symbol=normalized_symbol,
    )


def _holding_rows_prove_symbol_absent(
    *, rows: list[object], symbol: str
) -> bool:
    identities: set[str] = set()
    for item in rows:
        if not isinstance(item, dict):
            return False
        row_symbol = _nonempty_upper_string(item.get("pdno"))
        quantity = _nonnegative_integer(item.get("hldg_qty"))
        if row_symbol is None or quantity is None:
            return False
        sellable_supplied = "ord_psbl_qty" in item and item.get("ord_psbl_qty") not in (
            None,
            "",
        )
        sellable = (
            _nonnegative_integer(item.get("ord_psbl_qty"))
            if sellable_supplied
            else None
        )
        if sellable_supplied and (sellable is None or sellable > quantity):
            return False
        if row_symbol == symbol or row_symbol in identities:
            return False
        identities.add(row_symbol)
    return True


def _open_order_rows_prove_symbol_absent(
    *, rows: list[object], symbol: str
) -> bool:
    order_ids: set[str] = set()
    for item in rows:
        if not isinstance(item, dict):
            return False
        row_symbol = _nonempty_upper_string(item.get("pdno"))
        order_id = _nonempty_string(item.get("odno")) or _nonempty_string(
            item.get("ord_no")
        )
        quantity = _nonnegative_integer(item.get("ord_qty"))
        if row_symbol is None or order_id is None or quantity is None:
            return False
        remaining: int | None = None
        remaining_supplied = False
        for key in ("rmn_qty", "ord_uncc_qty", "nccs_qty"):
            if key not in item or item.get(key) in (None, ""):
                continue
            remaining_supplied = True
            remaining = _nonnegative_integer(item.get(key))
            if remaining is None:
                return False
            break
        if not remaining_supplied:
            if "tot_ccld_qty" not in item or item.get("tot_ccld_qty") in (
                None,
                "",
            ):
                return False
            filled = _nonnegative_integer(item.get("tot_ccld_qty"))
            if filled is None:
                return False
            remaining = quantity - filled
        side_material = " ".join(
            str(value)
            for value in (
                item.get("sll_buy_dvsn_cd"),
                item.get("sll_buy_dvsn_cd_name"),
                item.get("sll_buy_dvsn_name"),
            )
            if value is not None
        ).lower()
        valid_side = any(
            marker in side_material
            for marker in ("01", "02", "매도", "매수", "sell", "buy")
        )
        if (
            remaining is None
            or remaining < 0
            or remaining > quantity
            or not valid_side
            or row_symbol == symbol
            or order_id in order_ids
        ):
            return False
        order_ids.add(order_id)
    return True


def _nonnegative_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value).replace(",", ""))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0 or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _nonempty_string(value: object) -> str | None:
    normalized = str(value).strip() if isinstance(value, (str, int)) else ""
    return normalized or None


def _nonempty_upper_string(value: object) -> str | None:
    normalized = _nonempty_string(value)
    return normalized.upper() if normalized is not None else None


def generic_sync_payload_digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def kis_krx_flat_proof_timestamp_is_fresh(
    *,
    observed_at: object,
    evaluated_at: object,
    settings: Settings,
) -> bool:
    observed = aware_datetime(observed_at, assume_utc=True)
    evaluated = aware_datetime(evaluated_at, assume_utc=True)
    if observed is None or evaluated is None:
        return False
    max_age_seconds = min(
        settings.exec_agent_max_account_snapshot_age_seconds,
        KIS_KRX_FLAT_PROOF_MAX_AGE_SECONDS,
    )
    return bool(
        observed
        >= evaluated - timedelta(seconds=max_age_seconds)
        and observed
        <= evaluated
        + timedelta(
            seconds=settings.account_drawdown_recovery_max_snapshot_future_skew_seconds
        )
    )


def canonical_decimal(value: object) -> str | None:
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


def aware_datetime(value: object, *, assume_utc: bool = False) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC) if assume_utc else None
    return value.astimezone(UTC)


def aware_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return aware_datetime(parsed)


def event_datetime(value: object) -> datetime | None:
    return aware_datetime(value, assume_utc=True)


def serialize_datetime(value: object) -> str | None:
    parsed = aware_datetime(value, assume_utc=True)
    return parsed.isoformat() if parsed is not None else None


def masked_account_number(account_number: str | None) -> str | None:
    normalized = str(account_number or "").strip()
    if not normalized:
        return None
    if len(normalized) <= 4:
        return normalized
    return f"{'*' * (len(normalized) - 4)}{normalized[-4:]}"
