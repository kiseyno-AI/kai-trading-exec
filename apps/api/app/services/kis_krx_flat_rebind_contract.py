from __future__ import annotations

from decimal import Decimal
import hashlib
import hmac
import json

from app.core.config import Settings
from app.services.position_identity import (
    POSITION_IDENTITY_KEY,
    PositionIdentity,
    PositionIdentityConflictError,
    stable_account_scope_hmac_key,
)


KIS_KRX_LEGACY_FLAT_REBIND_CANDIDATE_TYPE = (
    "kis_krx_legacy_flat_position_rebind"
)
KIS_KRX_LEGACY_FLAT_REBIND_OUTCOME = "current_flat_truth_bound"
KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION = (
    "kis-krx-legacy-flat-position-rebind-v1"
)
KIS_KRX_LEGACY_FLAT_REBIND_CANONICAL_BASE_QUANTITY = Decimal("0")
KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS = (
    "broker_api_mutations",
    "orders_submitted",
    "orders_amended",
    "orders_canceled",
    "fills_created",
    "fills_deleted",
    "pnl_records_mutated",
    "strategy_records_mutated",
)
KIS_KRX_LEGACY_FLAT_REBIND_RAW_PROOF_FIELDS = frozenset(
    {
        "provider",
        "mode",
        "asset_class",
        "exchange",
        "snapshot_source",
        "snapshot_observed_at",
        "holdings_coverage",
        "open_orders_coverage",
        "holding_absent",
        "open_order_absent",
        "absence_proof_digest",
        "configured_account_binding_digest",
        "historical_account_attributed",
        "current_flat_truth_bound",
        "safety_policy_version",
        POSITION_IDENTITY_KEY,
    }
)
KIS_KRX_LEGACY_FLAT_REBIND_EVENT_PROOF_FIELDS = frozenset(
    {
        "symbol",
        "provider",
        "account_scope",
        "exchange",
        "asset_class",
        "broker_snapshot_source",
        "broker_snapshot_observed_at",
        "holdings_complete",
        "open_orders_complete",
        "holding_absent",
        "open_order_absent",
        "absence_proof_digest",
        "configured_account_binding_digest",
        "prior_position_digest",
        "evidence_fingerprint",
        "safety_policy_version",
        "historical_account_attributed",
        "current_flat_truth_bound",
        *KIS_KRX_LEGACY_FLAT_REBIND_ZERO_MUTATION_FIELDS,
    }
)


def kis_krx_flat_configured_account_binding_digest(*, settings: Settings) -> str:
    """Bind proof to configured KIS account/product without changing route scope."""
    account_number = str(settings.kis_account_number or "").strip()
    account_product_code = str(settings.kis_account_product_code or "").strip()
    proof_key = stable_account_scope_hmac_key(settings)
    if proof_key is None:
        app_secret = str(settings.kis_app_secret or "").strip().encode("utf-8")
        proof_key = app_secret if len(app_secret) >= 32 else None
    if not account_number or not account_product_code or proof_key is None:
        raise PositionIdentityConflictError(
            "KIS KRX flat rebind requires an exact keyed configured account proof"
        )
    material = (
        f"{KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION}"
        f"\0configured-kis-account\0{account_number}\0{account_product_code}"
    ).encode("utf-8")
    return hmac.new(proof_key, material, hashlib.sha256).hexdigest()


def kis_krx_flat_absence_proof_digest(
    *,
    runtime_mode: str,
    identity: PositionIdentity,
    symbol: str,
    snapshot_source: str,
    holdings_complete: bool,
    open_orders_complete: bool,
    holding_absent: bool,
    open_order_absent: bool,
    configured_account_binding_digest: str,
) -> str:
    """Hash the exact redacted account/route and target-symbol absence claim."""
    return _canonical_sha256(
        {
            "proof_version": 1,
            "safety_policy_version": (
                KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
            ),
            "runtime_mode": runtime_mode,
            "provider": identity.provider,
            "account_scope": identity.account_scope,
            "exchange": identity.exchange,
            "asset_class": "equity",
            "snapshot_source": snapshot_source,
            "holdings_complete": holdings_complete,
            "open_orders_complete": open_orders_complete,
            "configured_account_binding_digest": configured_account_binding_digest,
            "symbol": symbol,
            "holding_absent": holding_absent,
            "open_order_absent": open_order_absent,
        }
    )


def kis_krx_flat_evidence_material(
    *,
    runtime_mode: str,
    position_id: str,
    prior_position_digest: str,
    identity: PositionIdentity,
    symbol: str,
    snapshot_source: str,
    holdings_complete: bool,
    open_orders_complete: bool,
    holding_absent: bool,
    open_order_absent: bool,
    absence_proof_digest: str,
    configured_account_binding_digest: str,
) -> dict[str, object]:
    """Return the canonical material approved by the operator fingerprint."""
    return {
        "safety_policy_version": (
            KIS_KRX_LEGACY_FLAT_REBIND_SAFETY_POLICY_VERSION
        ),
        "mode": runtime_mode,
        "position_id": position_id,
        "symbol": symbol,
        "prior_position_digest": prior_position_digest,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
        "exchange": identity.exchange,
        "asset_class": "equity",
        "snapshot_source": snapshot_source,
        "holdings_complete": holdings_complete,
        "open_orders_complete": open_orders_complete,
        "holding_absent": holding_absent,
        "open_order_absent": open_order_absent,
        "absence_proof_digest": absence_proof_digest,
        "configured_account_binding_digest": configured_account_binding_digest,
        "historical_account_attributed": False,
        "current_flat_truth_bound": True,
    }


def kis_krx_flat_evidence_fingerprint(
    *,
    runtime_mode: str,
    position_id: str,
    prior_position_digest: str,
    identity: PositionIdentity,
    symbol: str,
    snapshot_source: str,
    holdings_complete: bool,
    open_orders_complete: bool,
    holding_absent: bool,
    open_order_absent: bool,
    absence_proof_digest: str,
    configured_account_binding_digest: str,
) -> str:
    return _canonical_sha256(
        kis_krx_flat_evidence_material(
            runtime_mode=runtime_mode,
            position_id=position_id,
            prior_position_digest=prior_position_digest,
            identity=identity,
            symbol=symbol,
            snapshot_source=snapshot_source,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
            holding_absent=holding_absent,
            open_order_absent=open_order_absent,
            absence_proof_digest=absence_proof_digest,
            configured_account_binding_digest=configured_account_binding_digest,
        )
    )


def is_lower_hex_sha256(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _canonical_sha256(material: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
