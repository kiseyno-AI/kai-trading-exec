from __future__ import annotations

from hashlib import sha256
from math import isfinite
import re


BROKER_ERROR_REDACTION_PREFIX = "broker_error_details_redacted"
BROKER_ERROR_CODE_REDACTION_PREFIX = "broker_error_code_redacted"
_REDACTED_ERROR_PATTERN = re.compile(
    rf"^{BROKER_ERROR_REDACTION_PREFIX}\[sha256=[0-9a-f]{{16}};bytes=[0-9]+\]$"
)

_SENSITIVE_KEYS = frozenset(
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
        "headers",
        "requestheaders",
        "responseheaders",
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
        "jsessionid",
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
_SENSITIVE_KEY_SUFFIXES = frozenset(
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
_SENSITIVE_KEY_WRAPPERS = ("value", "field", "header", "key")
_CODE_KEYS = frozenset(
    {
        "brokerstatus",
        "code",
        "errorcode",
        "msgcd",
    }
)
_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_.-]{0,63}$")
_ALPACA_NUMERIC_CODE_PATTERN = re.compile(r"^[0-9]{6,10}$")
_RT_CD_PATTERN = re.compile(r"^-?[0-9]{1,3}$")
_HTTP_STATUS_PATTERN = re.compile(r"^[1-5][0-9]{2}$")
_CONTENT_TYPE_PATTERN = re.compile(
    r"^[a-z][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*"
    r"(?:;\s*charset=[a-z0-9._-]+)?$",
    re.IGNORECASE,
)
_SAFE_PROVIDERS = frozenset({"alpaca", "kis", "upbit"})
_SAFE_CLASSIFICATIONS = frozenset(
    {
        "account_authority",
        "auth",
        "broker_rejected",
        "network",
        "rate_limit",
        "transient",
        "unknown",
        "validation",
    }
)
_SAFE_STATUSES = frozenset(
    {
        "cancel_pending",
        "canceled",
        "expired",
        "failed",
        "filled",
        "manual_review_required",
        "open",
        "partially_filled",
        "reconcile_window_expired",
        "rejected",
        "retry_pending",
        "stale_terminal_unknown",
        "submission_pending",
        "submitted",
    }
)
_SAFE_INTERNAL_CODES = frozenset(
    {
        "account_route_binding_invalid",
        "account_route_binding_missing",
        "account_route_binding_unavailable",
        "account_route_conflict",
        "account_route_mismatch",
        "alpaca_account_blocked",
        "alpaca_account_not_active",
        "alpaca_crypto_not_active",
        "alpaca_live_trading_disabled",
        "broker_commit_or_exposure_possible",
        "execution_account_scope_conflict",
        "execution_account_scope_invalid",
        "execution_account_scope_mismatch",
        "execution_account_scope_missing",
        "execution_account_scope_unavailable",
        "execution_plan_not_found",
        "execution_scope_conflict",
        "execution_scope_unavailable",
        "market_regime_guard_disabled",
        "missing_exec_agent_contract",
        "persisted_v4_contract_not_proven",
        "revocation_item_binding_mismatch",
        "upbit_live_not_implemented",
        "upbit_live_trading_disabled",
    }
)


def safe_broker_error_message(value: object) -> str:
    """Return a stable, correlation-friendly summary without retaining free text."""

    raw = str(value or "")
    if _REDACTED_ERROR_PATTERN.fullmatch(raw):
        return raw
    encoded = raw.encode("utf-8", errors="replace")
    digest = sha256(encoded).hexdigest()[:16]
    return (
        f"{BROKER_ERROR_REDACTION_PREFIX}"
        f"[sha256={digest};bytes={len(encoded)}]"
    )


def safe_broker_error_code(value: object, *, provider: str | None = None) -> str | None:
    """Keep provider-shaped codes; digest arbitrary code-field echo values."""

    if value is None:
        return None
    raw = str(value).strip()
    normalized_provider = str(provider or "").strip().casefold()
    if raw in _SAFE_INTERNAL_CODES or _CODE_PATTERN.fullmatch(raw) or (
        normalized_provider == "alpaca"
        and _ALPACA_NUMERIC_CODE_PATTERN.fullmatch(raw)
    ):
        return raw
    digest = sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"{BROKER_ERROR_CODE_REDACTION_PREFIX}[sha256={digest}]"


def sanitize_broker_error_payload(value: object) -> object:
    """Reduce an untrusted broker error to safe structured diagnostics.

    Strict broker codes, classifications, and statuses remain useful. External
    correlation/session IDs and all other free text are summarized, while
    credential/account-bearing keys are removed recursively. Ordinary agent
    ``session_id`` provenance is preserved by the separate general payload
    sanitizer and must not be routed through this untrusted-error boundary.
    """

    return _sanitize_error_value(value, key=None)


def _sanitize_error_value(value: object, *, key: object | None) -> object:
    normalized_key = _normalize_key(key) if key is not None else ""
    if isinstance(value, dict):
        sanitized: dict[str, object] = {}
        for raw_key, item in value.items():
            if _normalize_key(raw_key) == "headers" and isinstance(item, dict):
                safe_headers = _safe_response_headers(item)
                if safe_headers:
                    sanitized[str(raw_key)] = safe_headers
                continue
            if _is_sensitive_key(raw_key):
                continue
            sanitized[str(raw_key)] = _sanitize_error_value(item, key=raw_key)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_error_value(item, key=key) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_error_value(item, key=key) for item in value)
    if isinstance(value, str):
        if _REDACTED_ERROR_PATTERN.fullmatch(value):
            return value
        if _is_safe_diagnostic_value(key=normalized_key, value=value):
            return value
        return safe_broker_error_message(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if _is_safe_numeric_diagnostic(key=normalized_key, value=value):
            return value
        return safe_broker_error_message(value)
    return safe_broker_error_message(value)


def _safe_response_headers(headers: dict[object, object]) -> dict[str, object]:
    """Retain only bounded response diagnostics, never auth/session headers."""

    safe: dict[str, object] = {}
    for raw_key, raw_value in headers.items():
        normalized_key = _normalize_key(raw_key)
        if normalized_key == "contenttype" and isinstance(raw_value, str):
            value = raw_value.strip()
            if _CONTENT_TYPE_PATTERN.fullmatch(value):
                safe[str(raw_key).strip().lower()] = value
        elif normalized_key == "retryafter":
            try:
                seconds = int(str(raw_value).strip())
            except (TypeError, ValueError):
                continue
            if 0 <= seconds <= 86_400:
                safe[str(raw_key).strip().lower()] = seconds
    return safe


def _normalize_key(value: object) -> str:
    return "".join(
        character
        for character in str(value).strip().casefold()
        if character.isalnum()
    )


def _is_sensitive_key(value: object) -> bool:
    if str(value).strip() == "HASH":
        return True
    normalized = _normalize_key(value)
    while normalized:
        if normalized in _SENSITIVE_KEYS or any(
            normalized.endswith(suffix) for suffix in _SENSITIVE_KEY_SUFFIXES
        ):
            return True
        wrapper = next(
            (
                candidate
                for candidate in _SENSITIVE_KEY_WRAPPERS
                if normalized.endswith(candidate) and len(normalized) > len(candidate)
            ),
            None,
        )
        if wrapper is None:
            return False
        normalized = normalized[: -len(wrapper)]
    return False


def _is_safe_diagnostic_value(*, key: str, value: str) -> bool:
    normalized_value = value.strip()
    if key in _CODE_KEYS:
        return normalized_value in _SAFE_INTERNAL_CODES or bool(
            _CODE_PATTERN.fullmatch(normalized_value)
        )
    if key == "rtcd":
        return bool(_RT_CD_PATTERN.fullmatch(normalized_value))
    if key == "statuscode":
        return bool(_HTTP_STATUS_PATTERN.fullmatch(normalized_value))
    if key == "contenttype":
        return bool(_CONTENT_TYPE_PATTERN.fullmatch(normalized_value))
    if key == "provider":
        return normalized_value.casefold() in _SAFE_PROVIDERS
    if key in {"classification", "errorclassification"}:
        return normalized_value.casefold() in _SAFE_CLASSIFICATIONS
    if key in {"normalizedstatus", "status"}:
        return normalized_value.casefold() in _SAFE_STATUSES
    # Correlation IDs, session IDs, transaction IDs, free text, and unknown
    # fields are attacker-controlled at this boundary and remain digest-only.
    return False


def _is_safe_numeric_diagnostic(*, key: str, value: int | float) -> bool:
    if isinstance(value, float) and (not isfinite(value) or not value.is_integer()):
        return False
    numeric_value = int(value)
    if key == "statuscode":
        return 100 <= numeric_value <= 599
    if key == "rtcd":
        return -999 <= numeric_value <= 999
    if key == "retryafter":
        return 0 <= numeric_value <= 86_400
    return False
