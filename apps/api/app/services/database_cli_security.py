from __future__ import annotations

import re

from sqlalchemy.engine import make_url


_SENSITIVE_DATABASE_QUERY_KEY_PARTS = (
    "credential",
    "key",
    "pass",
    "secret",
    "token",
)
_SENSITIVE_DATABASE_QUERY_KEYS = frozenset(
    {
        "auth",
        "authentication",
        "authorization",
    }
)
_OPAQUE_CONNECTION_QUERY_KEYS = frozenset(
    {
        "connect_args",
        "connect_string",
        "connection_string",
        "odbc_connect",
    }
)
_EMBEDDED_CREDENTIAL_ASSIGNMENT = re.compile(
    r"(?:^|[;,&\s{])"
    r"(?:api[_ -]?key|access[_ -]?token|credential|passphrase|password|pwd|secret|token)"
    r"\s*(?:=|:)",
    flags=re.IGNORECASE,
)
_EMBEDDED_URI_USERINFO = re.compile(
    r"[a-z][a-z0-9+.-]*://[^/?#\s:@]+:[^/?#\s@]+@",
    flags=re.IGNORECASE,
)


def reject_secret_database_url_argument(value: str | None) -> None:
    """Reject database credentials supplied through process-visible argv."""

    if not value:
        return
    try:
        parsed = make_url(value)
    except Exception as exc:
        raise SystemExit("--database-url is not a valid SQLAlchemy URL") from exc
    unsafe_query = any(
        _database_query_argument_is_sensitive(key=key, value=query_value)
        for key, query_value in parsed.query.items()
    )
    if parsed.password is not None or unsafe_query:
        raise SystemExit(
            "credential-bearing database URLs must be supplied through the "
            "KAI_EXEC_DATABASE_URL environment variable, not argv"
        )


def _database_query_argument_is_sensitive(*, key: object, value: object) -> bool:
    normalized_key = str(key).strip().lower()
    if (
        normalized_key in _SENSITIVE_DATABASE_QUERY_KEYS
        or normalized_key in _OPAQUE_CONNECTION_QUERY_KEYS
        or any(part in normalized_key for part in _SENSITIVE_DATABASE_QUERY_KEY_PARTS)
    ):
        return True
    values = value if isinstance(value, tuple) else (value,)
    return any(
        _query_value_contains_embedded_credentials(item)
        for item in values
    )


def _query_value_contains_embedded_credentials(value: object) -> bool:
    text = str(value or "")
    return bool(
        _EMBEDDED_CREDENTIAL_ASSIGNMENT.search(text)
        or _EMBEDDED_URI_USERINFO.search(text)
    )
