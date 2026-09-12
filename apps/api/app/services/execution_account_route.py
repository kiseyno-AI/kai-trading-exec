from __future__ import annotations

from dataclasses import dataclass
import hmac
from hashlib import sha256
from typing import Literal

from app.core.config import Settings
from app.services.position_identity import (
    EXECUTION_SCOPE_POLICY_VERSION,
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
    stable_account_scope_hmac_key,
)

UNSPECIFIED_KIS_ACCOUNT_REFERENCE = "KIS:00000000-00"
KIS_ACCOUNT_ROUTE_BINDING_KEY = "execution_account_route"
KIS_ACCOUNT_ROUTE_BINDING_VERSION = "kis-account-route-v1"
KIS_ACCOUNT_ROUTE_FINGERPRINT_LENGTH = 32
EXECUTION_ACCOUNT_SCOPE_KEY = "execution_account_scope"
_SENSITIVE_ACCOUNT_PAYLOAD_KEYS = frozenset(
    {
        "accountreference",
        "accountid",
        "brokeraccountid",
        "accountno",
        "acctid",
        "acctno",
        "accountnumber",
        "accountproductcode",
        "kisaccountnumber",
        "kisaccountproductcode",
        "cano",
        "acntprdtcd",
        "authorization",
        "proxyauthorization",
        "auth",
        "authentication",
        "bearer",
        "cookie",
        "cookies",
        "credential",
        "credentials",
        "secret",
        "sessioncookie",
        "sessionsecret",
        "sessiontoken",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "authtoken",
        "bearertoken",
        "approvalkey",
        "websocketapprovalkey",
        "xsessionid",
        "appkey",
        "appsecret",
        "apikey",
        "apisecret",
        "clientid",
        "clientsecret",
        "consumerkey",
        "consumersecret",
        "privatekey",
        "secretkey",
        "password",
        "passwd",
        "token",
    }
)
_SENSITIVE_ACCOUNT_PAYLOAD_KEY_SUFFIXES = frozenset(
    {
        "accountreference",
        "accountid",
        "accountno",
        "accountnumber",
        "accountproductcode",
        "authorization",
        "cookie",
        "cookieheader",
        "accesstoken",
        "refreshtoken",
        "authtoken",
        "bearertoken",
        "approvalkey",
        "hashkey",
        "accesskey",
        "apikeyid",
        "appkey",
        "appsecret",
        "apikey",
        "apisecret",
        "clientid",
        "clientsecret",
        "consumerkey",
        "consumersecret",
        "idtoken",
        "privatekey",
        "secretkey",
        "password",
        "passwd",
    }
)


@dataclass(frozen=True)
class AccountRouteMismatch:
    reason_code: str
    provided_route_fingerprint: str | None


@dataclass(frozen=True)
class ExecutionAccountScopeMismatch:
    reason_code: str


@dataclass(frozen=True)
class _ExecutionAccountScopeInspection:
    binding: dict[str, object] | None
    reason_code: str | None


class AccountRouteBindingError(ValueError):
    """Raised when binding would replace or normalize conflicting route authority."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


AccountRouteClassification = Literal["active", "foreign", "indeterminate"]


@dataclass(frozen=True)
class _BoundRouteInspection:
    fingerprint: str | None
    reason_code: str | None


def validate_kis_account_route(
    *,
    settings: Settings,
    provider: str | None,
    risk_context: object,
    quantity_policy: object,
    require_binding: bool = False,
) -> AccountRouteMismatch | None:
    if str(provider or "kis").strip().lower() != "kis":
        return None

    risk_reference = account_reference_from_payload(risk_context)
    policy_reference = account_reference_from_payload(quantity_policy)
    if (
        risk_reference is not None
        and policy_reference is not None
        and risk_reference != policy_reference
    ):
        return AccountRouteMismatch(
            reason_code="account_route_conflict",
            provided_route_fingerprint=_route_fingerprint(settings, risk_reference),
        )

    provided_reference = risk_reference or policy_reference
    expected_reference = configured_kis_account_reference(settings)
    if (
        provided_reference is not None
        and provided_reference != UNSPECIFIED_KIS_ACCOUNT_REFERENCE
        and (expected_reference is None or provided_reference != expected_reference)
    ):
        return AccountRouteMismatch(
            reason_code="account_route_mismatch",
            provided_route_fingerprint=_route_fingerprint(settings, provided_reference),
        )

    bound_route = _inspect_bound_route_bindings(risk_context, quantity_policy)
    if bound_route.reason_code is not None:
        return AccountRouteMismatch(
            reason_code=bound_route.reason_code,
            provided_route_fingerprint=None,
        )
    bound_fingerprint = bound_route.fingerprint
    expected_fingerprint = configured_kis_route_fingerprint(settings)
    route_authority_configured = bool(
        expected_reference
        or str(settings.execution_position_account_scope or "").strip()
    )
    if require_binding and route_authority_configured and expected_fingerprint is None:
        return AccountRouteMismatch(
            reason_code="account_route_binding_unavailable",
            provided_route_fingerprint=None,
        )
    if bound_fingerprint is not None:
        if not _is_valid_route_fingerprint(bound_fingerprint):
            return AccountRouteMismatch(
                reason_code="account_route_binding_invalid",
                provided_route_fingerprint=None,
            )
        if expected_fingerprint is None or not hmac.compare_digest(
            bound_fingerprint,
            expected_fingerprint,
        ):
            return AccountRouteMismatch(
                reason_code="account_route_mismatch",
                provided_route_fingerprint=bound_fingerprint,
            )
        return None
    if require_binding and expected_fingerprint is not None:
        return AccountRouteMismatch(
            reason_code="account_route_binding_missing",
            provided_route_fingerprint=None,
        )
    return None


def configured_kis_account_reference(settings: Settings) -> str | None:
    account_number = str(settings.kis_account_number or "").strip()
    product_code = str(settings.kis_account_product_code or "").strip()
    if not account_number or not product_code:
        return None
    return f"KIS:{account_number}-{product_code}"


def account_reference_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("account_reference")
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    prefix, separator, body = normalized.partition(":")
    if separator and prefix.strip().lower() == "kis":
        return f"KIS:{body.strip()}"
    return normalized


def is_unspecified_kis_account_reference(reference: str | None) -> bool:
    return reference == UNSPECIFIED_KIS_ACCOUNT_REFERENCE


def configured_kis_route_fingerprint(settings: Settings) -> str | None:
    reference = configured_kis_account_reference(settings)
    if reference is None and not str(
        settings.execution_position_account_scope or ""
    ).strip():
        return None
    return _route_fingerprint(
        settings,
        reference or UNSPECIFIED_KIS_ACCOUNT_REFERENCE,
    )


def classify_kis_account_route(
    *,
    settings: Settings,
    provider: str | None,
    risk_context: object,
    quantity_policy: object,
) -> AccountRouteClassification:
    """Classify persisted authority without treating ambiguous legacy data as foreign."""

    if str(provider or "kis").strip().lower() != "kis":
        return "foreign"
    mismatch = validate_kis_account_route(
        settings=settings,
        provider=provider,
        risk_context=risk_context,
        quantity_policy=quantity_policy,
        require_binding=True,
    )
    if mismatch is None:
        return "active"
    expected_fingerprint = configured_kis_route_fingerprint(settings)
    bound_route = _inspect_bound_route_bindings(risk_context, quantity_policy)
    if (
        mismatch.reason_code == "account_route_mismatch"
        and expected_fingerprint is not None
        and mismatch.provided_route_fingerprint is not None
        and bound_route.reason_code is None
        and bound_route.fingerprint is not None
        and _is_valid_route_fingerprint(bound_route.fingerprint)
        and hmac.compare_digest(
            mismatch.provided_route_fingerprint,
            bound_route.fingerprint,
        )
    ):
        return "foreign"
    return "indeterminate"


def bind_kis_account_route(
    *,
    settings: Settings,
    provider: str | None,
    risk_context: object,
    quantity_policy: object,
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    sanitized_risk = sanitize_account_payload(risk_context)
    sanitized_policy = sanitize_account_payload(quantity_policy)
    risk = dict(sanitized_risk) if isinstance(sanitized_risk, dict) else {}
    policy = dict(sanitized_policy) if isinstance(sanitized_policy, dict) else {}
    if str(provider or "kis").strip().lower() != "kis":
        return risk or None, policy or None

    mismatch = validate_kis_account_route(
        settings=settings,
        provider=provider,
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    if mismatch is not None:
        raise AccountRouteBindingError(mismatch.reason_code)

    risk.pop("account_reference", None)
    policy.pop("account_reference", None)
    fingerprint = configured_kis_route_fingerprint(settings)
    if fingerprint is not None:
        risk[KIS_ACCOUNT_ROUTE_BINDING_KEY] = {
            "version": KIS_ACCOUNT_ROUTE_BINDING_VERSION,
            "configured_route_fingerprint": fingerprint,
        }
    return risk or None, policy or None


def kis_account_route_binding_from_payloads(*payloads: object) -> dict[str, str] | None:
    """Return a validated opaque binding that is safe to copy to derived items."""

    fingerprint = _bound_route_fingerprint(*payloads)
    if fingerprint is None or not _is_valid_route_fingerprint(fingerprint):
        return None
    return {
        "version": KIS_ACCOUNT_ROUTE_BINDING_VERSION,
        "configured_route_fingerprint": fingerprint,
    }


def build_execution_account_scope_binding(
    *,
    settings: Settings,
    provider: str,
    exchange: str,
    broker_account_id: str | None = None,
) -> dict[str, object]:
    """Build a persistence-safe account authority for a non-KIS plan item."""

    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == "kis" or not normalized_provider:
        raise PositionIdentityConflictError(
            "non-KIS execution account scope requires an explicit provider"
        )
    if normalized_provider == "alpaca":
        identity = build_authenticated_broker_position_identity(
            settings=settings,
            provider=normalized_provider,
            exchange=exchange,
            broker_account_id=str(broker_account_id or ""),
        )
    elif normalized_provider == "upbit":
        if not str(settings.execution_position_account_scope or "").strip():
            raise PositionIdentityConflictError(
                "Upbit buy authority requires a configured opaque account scope"
            )
        identity = build_position_identity(
            settings=settings,
            provider=normalized_provider,
            exchange=exchange,
        )
    else:
        identity = build_position_identity(
            settings=settings,
            provider=normalized_provider,
            exchange=exchange,
            broker_account_id=broker_account_id,
        )
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": identity.provider,
        "account_scope": identity.account_scope,
    }


def bind_execution_account_scope(
    *,
    provider: str,
    risk_context: object,
    quantity_policy: object,
    binding: dict[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Pin, rather than replace, a non-KIS account authority at admission."""

    sanitized_risk = sanitize_account_payload(risk_context)
    sanitized_policy = sanitize_account_payload(quantity_policy)
    risk = dict(sanitized_risk) if isinstance(sanitized_risk, dict) else {}
    policy = dict(sanitized_policy) if isinstance(sanitized_policy, dict) else {}
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == "kis":
        return risk or None, policy or None

    expected = _validated_execution_account_scope_binding(binding)
    if expected is None or expected["provider"] != normalized_provider:
        raise AccountRouteBindingError("execution_account_scope_invalid")
    existing = _inspect_execution_account_scope_bindings(risk, policy)
    if existing.reason_code is not None:
        raise AccountRouteBindingError(existing.reason_code)
    if existing.binding is not None and not _execution_account_scopes_equal(
        existing.binding,
        expected,
    ):
        raise AccountRouteBindingError("execution_account_scope_mismatch")
    risk[EXECUTION_ACCOUNT_SCOPE_KEY] = dict(expected)
    return risk or None, policy or None


def execution_account_scope_binding_from_payloads(
    *payloads: object,
) -> dict[str, object] | None:
    inspection = _inspect_execution_account_scope_bindings(*payloads)
    if inspection.reason_code is not None or inspection.binding is None:
        return None
    return dict(inspection.binding)


def validate_execution_account_scope(
    *,
    provider: str | None,
    risk_context: object,
    quantity_policy: object,
    current_binding: object,
    require_binding: bool = True,
) -> ExecutionAccountScopeMismatch | None:
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == "kis":
        return None
    expected = _inspect_execution_account_scope_bindings(
        risk_context,
        quantity_policy,
    )
    if expected.reason_code is not None:
        return ExecutionAccountScopeMismatch(expected.reason_code)
    if expected.binding is None:
        return (
            ExecutionAccountScopeMismatch("execution_account_scope_missing")
            if require_binding
            else None
        )
    current = _validated_execution_account_scope_binding(current_binding)
    if current is None:
        return ExecutionAccountScopeMismatch("execution_account_scope_unavailable")
    if (
        expected.binding["provider"] != normalized_provider
        or current["provider"] != normalized_provider
        or not _execution_account_scopes_equal(expected.binding, current)
    ):
        return ExecutionAccountScopeMismatch("execution_account_scope_mismatch")
    return None


def sanitize_account_payload(value: object) -> object:
    """Recursively remove raw account identifiers and authentication secrets."""

    if isinstance(value, dict):
        return {
            key: sanitize_account_payload(item)
            for key, item in value.items()
            if not _is_sensitive_payload_key(key)
        }
    if isinstance(value, list):
        return [sanitize_account_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_account_payload(item) for item in value)
    return value


def redact_account_references(value: object) -> object:
    """Backward-compatible name for :func:`sanitize_account_payload`."""

    return sanitize_account_payload(value)


def _normalized_sensitive_key(value: object) -> str:
    return "".join(
        character
        for character in str(value).strip().casefold()
        if character.isalnum()
    )


def _is_sensitive_payload_key(value: object) -> bool:
    if str(value).strip() == "HASH":
        # KIS returns its request-signing hashkey under this exact field name.
        return True
    normalized = _normalized_sensitive_key(value)
    return normalized in _SENSITIVE_ACCOUNT_PAYLOAD_KEYS or any(
        normalized.endswith(suffix)
        for suffix in _SENSITIVE_ACCOUNT_PAYLOAD_KEY_SUFFIXES
    )


def _bound_route_fingerprint(*payloads: object) -> str | None:
    inspection = _inspect_bound_route_bindings(*payloads)
    return inspection.fingerprint if inspection.reason_code is None else None


def _inspect_bound_route_bindings(*payloads: object) -> _BoundRouteInspection:
    fingerprints: list[str] = []
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        if KIS_ACCOUNT_ROUTE_BINDING_KEY not in payload:
            continue
        binding = payload.get(KIS_ACCOUNT_ROUTE_BINDING_KEY)
        if (
            not isinstance(binding, dict)
            or set(binding)
            != {"version", "configured_route_fingerprint"}
            or binding.get("version") != KIS_ACCOUNT_ROUTE_BINDING_VERSION
        ):
            return _BoundRouteInspection(
                fingerprint=None,
                reason_code="account_route_binding_invalid",
            )
        fingerprint = binding.get("configured_route_fingerprint")
        if not isinstance(fingerprint, str) or not _is_valid_route_fingerprint(
            fingerprint
        ):
            return _BoundRouteInspection(
                fingerprint=None,
                reason_code="account_route_binding_invalid",
            )
        fingerprints.append(fingerprint)
    if not fingerprints:
        return _BoundRouteInspection(fingerprint=None, reason_code=None)
    first = fingerprints[0]
    if any(not hmac.compare_digest(first, fingerprint) for fingerprint in fingerprints[1:]):
        return _BoundRouteInspection(
            fingerprint=None,
            reason_code="account_route_conflict",
        )
    return _BoundRouteInspection(fingerprint=first, reason_code=None)


def _inspect_execution_account_scope_bindings(
    *payloads: object,
) -> _ExecutionAccountScopeInspection:
    bindings: list[dict[str, object]] = []
    for payload in payloads:
        if not isinstance(payload, dict) or EXECUTION_ACCOUNT_SCOPE_KEY not in payload:
            continue
        binding = _validated_execution_account_scope_binding(
            payload.get(EXECUTION_ACCOUNT_SCOPE_KEY)
        )
        if binding is None:
            return _ExecutionAccountScopeInspection(
                binding=None,
                reason_code="execution_account_scope_invalid",
            )
        bindings.append(binding)
    if not bindings:
        return _ExecutionAccountScopeInspection(binding=None, reason_code=None)
    first = bindings[0]
    if any(
        not _execution_account_scopes_equal(first, candidate)
        for candidate in bindings[1:]
    ):
        return _ExecutionAccountScopeInspection(
            binding=None,
            reason_code="execution_account_scope_conflict",
        )
    return _ExecutionAccountScopeInspection(binding=first, reason_code=None)


def _validated_execution_account_scope_binding(
    value: object,
) -> dict[str, object] | None:
    if not isinstance(value, dict) or set(value) != {
        "policy_version",
        "provider",
        "account_scope",
    }:
        return None
    if value.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION:
        return None
    provider = value.get("provider")
    account_scope = value.get("account_scope")
    if (
        not isinstance(provider, str)
        or not provider.strip()
        or provider.strip().lower() == "kis"
        or not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
    ):
        return None
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": provider.strip().lower(),
        "account_scope": account_scope,
    }


def _execution_account_scopes_equal(
    left: dict[str, object],
    right: dict[str, object],
) -> bool:
    return bool(
        left.get("policy_version") == right.get("policy_version")
        and left.get("provider") == right.get("provider")
        and isinstance(left.get("account_scope"), str)
        and isinstance(right.get("account_scope"), str)
        and hmac.compare_digest(
            str(left["account_scope"]),
            str(right["account_scope"]),
        )
    )


def _route_fingerprint(settings: Settings, reference: str) -> str | None:
    configured_scope = str(settings.execution_position_account_scope or "").strip()
    key = stable_account_scope_hmac_key(settings)
    if reference != UNSPECIFIED_KIS_ACCOUNT_REFERENCE and key is not None:
        # A keyed configured KIS account is stronger than an operator label.
        # Otherwise changing credentials from account A to B while retaining
        # the label would leave stale plans authorized for the wrong account.
        message = f"{KIS_ACCOUNT_ROUTE_BINDING_VERSION}\0{reference}".encode(
            "utf-8"
        )
        return hmac.new(key, message, sha256).hexdigest()[
            :KIS_ACCOUNT_ROUTE_FINGERPRINT_LENGTH
        ]
    if configured_scope:
        # The opaque label is a fallback when account material cannot be keyed.
        message = (
            f"{KIS_ACCOUNT_ROUTE_BINDING_VERSION}\0configured-account-scope\0"
            f"{configured_scope}"
        ).encode("utf-8")
        digest = (
            hmac.new(key, message, sha256).hexdigest()
            if key
            else sha256(message).hexdigest()
        )
        return digest[:KIS_ACCOUNT_ROUTE_FINGERPRINT_LENGTH]
    if key is None and str(settings.runtime_mode).strip().lower() != "live":
        # Paper/mock plans remain self-contained for local simulation. Live route
        # identity must never depend on rotating broker credentials.
        secret = (
            settings.account_drawdown_recovery_scope_secret
            or settings.kis_app_secret
            or settings.kis_app_key
        )
        key = secret.encode("utf-8") if secret else None
    if key is None:
        return None
    message = f"{KIS_ACCOUNT_ROUTE_BINDING_VERSION}\0{reference}".encode("utf-8")
    return hmac.new(key, message, sha256).hexdigest()[
        :KIS_ACCOUNT_ROUTE_FINGERPRINT_LENGTH
    ]


def _is_valid_route_fingerprint(value: str) -> bool:
    return len(value) == KIS_ACCOUNT_ROUTE_FINGERPRINT_LENGTH and all(
        character in "0123456789abcdef" for character in value
    )
