"""Remove raw account identifiers and credential fields from legacy JSON payloads.

Revision ID: 202609090004
Revises: 202609090003

The cleanup is intentionally irreversible: a downgrade must not restore secrets
that were removed from durable audit and broker payloads.
"""

from collections.abc import Sequence
from hashlib import sha256
from math import isfinite
import re

from alembic import op
import sqlalchemy as sa


revision: str = "202609090004"
down_revision: str | None = "202609090003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

REDACTION_BATCH_SIZE = 500

PAYLOAD_COLUMNS: dict[str, tuple[str, ...]] = {
    "runtime_control_state": ("hourly_risk_off_context",),
    "runtime_control_events": ("previous_state", "current_state"),
    "broker_session_events": ("previous_state", "current_state"),
    "execution_plans": ("raw_payload",),
    "execution_plan_items": ("quantity_policy", "risk_context"),
    "execution_plan_item_audits": ("summary", "payload"),
    "broker_orders": (
        "request_metadata",
        "response_metadata",
        "raw_broker_payload",
    ),
    "broker_order_events": ("request_payload", "response_payload", "payload"),
    "execution_failures": ("retry_audit_context", "raw_context"),
    "execution_fills": ("payload",),
    "runtime_remediation_events": ("evidence_summary",),
    "fifo_inventory_isolations": ("audit_payload",),
    "execution_positions": ("raw_payload",),
    "crypto_daily_bars": ("raw_payload",),
    "crypto_strategy_states": ("metrics",),
    "crypto_strategy_risk_states": ("risk_reasons", "metrics"),
    "crypto_strategy_decisions": ("reasons", "metrics", "risk_context", "payload"),
    "monitoring_checkpoints": ("details",),
}

# Scalar text is handled separately so normal decision narratives and successful
# audit messages are not indiscriminately replaced. Conditional rules use the
# third tuple member as an allowlist; ``None`` means any non-null discriminator.
ERROR_TEXT_COLUMNS: dict[
    str,
    tuple[tuple[str, str | None, tuple[str, ...] | None], ...],
] = {
    "broker_sessions": (("last_error", None, None),),
    "broker_session_events": (("message", "outcome", ("rejected",)),),
    "broker_orders": (("last_error_message", None, None),),
    "broker_order_events": (("message", "error_classification", None),),
    "execution_failures": (("broker_error_message", None, None),),
    "execution_jobs": (("last_error", None, None),),
    "execution_plan_item_audits": (("error_message", None, None),),
}

ERROR_PAYLOAD_MESSAGE_COLUMNS = frozenset(
    {
        ("broker_orders", "raw_broker_payload"),
        ("broker_orders", "response_metadata"),
        ("broker_order_events", "response_payload"),
        ("execution_failures", "retry_audit_context"),
        ("execution_failures", "raw_context"),
    }
)
ERROR_PAYLOAD_CONDITION_COLUMNS: dict[str, str] = {
    "broker_orders": "last_error_message",
    "broker_order_events": "error_classification",
}

BROKER_ERROR_REDACTION_PREFIX = "broker_error_details_redacted"
_REDACTED_ERROR_PATTERN = re.compile(
    rf"^{BROKER_ERROR_REDACTION_PREFIX}\[sha256=[0-9a-f]{{16}};bytes=[0-9]+\]$"
)
_ALWAYS_ERROR_TEXT_KEYS = frozenset({"lasterror"})
_BROKER_ERROR_TEXT_KEYS = frozenset(
    {
        "bodydiagnostic",
        "detail",
        "errordescription",
        "errordetail",
        "errormessage",
        "msg1",
        "rawbody",
    }
)
_CODE_KEYS = frozenset(
    {
        "brokerstatus",
        "code",
        "errorcode",
        "msgcd",
    }
)
_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_.-]{0,63}$")
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
        "headers",
        "requestheaders",
        "responseheaders",
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
_SENSITIVE_KEY_WRAPPERS = (
    "value",
    "field",
    "header",
    "key",
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


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    managed_tables = sorted(
        existing_tables.intersection(set(PAYLOAD_COLUMNS) | set(ERROR_TEXT_COLUMNS))
    )
    _lock_payload_writers(bind, table_names=managed_tables)
    for table_name in managed_tables:
        declared_columns = PAYLOAD_COLUMNS.get(table_name, ())
        existing_columns = {
            column["name"] for column in inspector.get_columns(table_name)
        }
        payload_columns = tuple(
            column_name
            for column_name in declared_columns
            if column_name in existing_columns
        )
        if payload_columns:
            condition_column = ERROR_PAYLOAD_CONDITION_COLUMNS.get(table_name)
            scrub_error_payloads = (
                condition_column is None or condition_column in existing_columns
            )
            _redact_table(
                bind,
                table_name=table_name,
                payload_columns=payload_columns,
                scrub_error_payloads=scrub_error_payloads,
                error_condition_column=(
                    condition_column
                    if condition_column in existing_columns
                    else None
                ),
            )
        for column_name, condition_column, condition_values in ERROR_TEXT_COLUMNS.get(
            table_name,
            (),
        ):
            if column_name not in existing_columns or (
                condition_column is not None
                and condition_column not in existing_columns
            ):
                continue
            _redact_error_text_column(
                bind,
                table_name=table_name,
                column_name=column_name,
                condition_column=condition_column,
                condition_values=condition_values,
            )


def downgrade() -> None:
    """Do not restore removed credentials or raw account identifiers."""


def _redact_table(
    bind: sa.engine.Connection,
    *,
    table_name: str,
    payload_columns: tuple[str, ...],
    error_condition_column: str | None = None,
    scrub_error_payloads: bool = True,
) -> None:
    extra_columns = (
        (sa.column(error_condition_column, sa.Text()),)
        if error_condition_column is not None
        and error_condition_column not in payload_columns
        else ()
    )
    table = sa.table(
        table_name,
        sa.column("id", sa.String()),
        *(sa.column(column_name, sa.JSON()) for column_name in payload_columns),
        *extra_columns,
    )
    last_id: object | None = None
    while True:
        statement = (
            sa.select(
                table.c.id,
                *(table.c[column_name] for column_name in payload_columns),
                *(
                    (table.c[error_condition_column],)
                    if error_condition_column is not None
                    and error_condition_column not in payload_columns
                    else ()
                ),
            )
            .order_by(table.c.id.asc())
            .limit(REDACTION_BATCH_SIZE)
        )
        if last_id is not None:
            statement = statement.where(table.c.id > last_id)
        rows = bind.execute(statement).mappings().all()
        if not rows:
            return
        for row in rows:
            updates: dict[str, object] = {}
            for column_name in payload_columns:
                value = row[column_name]
                error_row = (
                    scrub_error_payloads
                    and (
                        error_condition_column is None
                        or row[error_condition_column] is not None
                    )
                )
                sanitized = _sanitize(
                    value,
                    scrub_message=(
                        error_row
                        and (table_name, column_name)
                        in ERROR_PAYLOAD_MESSAGE_COLUMNS
                    ),
                )
                if sanitized != value:
                    updates[column_name] = sanitized
            if updates:
                bind.execute(
                    table.update().where(table.c.id == row["id"]).values(**updates)
                )
        last_id = rows[-1]["id"]


def _redact_error_text_column(
    bind: sa.engine.Connection,
    *,
    table_name: str,
    column_name: str,
    condition_column: str | None,
    condition_values: tuple[str, ...] | None,
) -> None:
    columns = [sa.column("id", sa.String()), sa.column(column_name, sa.Text())]
    if condition_column is not None:
        columns.append(sa.column(condition_column, sa.String()))
    table = sa.table(table_name, *columns)
    last_id: object | None = None
    while True:
        statement = (
            sa.select(table.c.id, table.c[column_name])
            .where(table.c[column_name].is_not(None))
            .order_by(table.c.id.asc())
            .limit(REDACTION_BATCH_SIZE)
        )
        if condition_column is not None:
            if condition_values is None:
                statement = statement.where(table.c[condition_column].is_not(None))
            else:
                statement = statement.where(
                    table.c[condition_column].in_(condition_values)
                )
        if last_id is not None:
            statement = statement.where(table.c.id > last_id)
        rows = bind.execute(statement).mappings().all()
        if not rows:
            return
        for row in rows:
            sanitized = _safe_error_text(row[column_name])
            if sanitized != row[column_name]:
                bind.execute(
                    table.update()
                    .where(table.c.id == row["id"])
                    .values({column_name: sanitized})
                )
        last_id = rows[-1]["id"]


def _lock_payload_writers(
    bind: sa.engine.Connection,
    *,
    table_names: list[str],
) -> None:
    if not table_names:
        return
    if bind.dialect.name == "postgresql":
        # Names come only from the static PAYLOAD_COLUMNS allowlist. Drain every
        # reader and writer, then take all locks in sorted order so an older app
        # transaction cannot reintroduce a secret after this irreversible sweep.
        bind.execute(
            sa.text(
                "LOCK TABLE "
                + ", ".join(table_names)
                + " IN ACCESS EXCLUSIVE MODE"
            )
        )
    elif bind.dialect.name == "sqlite":
        # Alembic opens SQLite transactions as deferred. A no-op write acquires
        # the database-wide RESERVED writer lock before any scan, including in
        # WAL mode, so a late insert cannot escape the irreversible cleanup.
        bind.execute(
            sa.text(f"UPDATE {table_names[0]} SET id = id WHERE 0")
        )


def _sanitize(
    value: object,
    *,
    scrub_message: bool = False,
    parent_key: str | None = None,
) -> object:
    if isinstance(value, dict):
        sanitized: dict[object, object] = {}
        for key, item in value.items():
            if _is_sensitive_key(key):
                continue
            normalized_key = _normalize_key(key)
            if (
                not scrub_message
                and isinstance(item, str)
                and normalized_key in _ALWAYS_ERROR_TEXT_KEYS
            ):
                sanitized[key] = _safe_error_text(item)
            else:
                sanitized[key] = _sanitize(
                    item,
                    scrub_message=scrub_message,
                    parent_key=normalized_key,
                )
        return sanitized
    if isinstance(value, list):
        return [
            _sanitize(
                item,
                scrub_message=scrub_message,
                parent_key=parent_key,
            )
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _sanitize(
                item,
                scrub_message=scrub_message,
                parent_key=parent_key,
            )
            for item in value
        )
    if not scrub_message:
        return value
    normalized_parent_key = parent_key or ""
    if isinstance(value, str):
        if _REDACTED_ERROR_PATTERN.fullmatch(value):
            return value
        if _is_safe_diagnostic_value(key=normalized_parent_key, value=value):
            return value
        return _safe_error_text(value)
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and _is_safe_numeric_diagnostic(
        key=normalized_parent_key,
        value=value,
    ):
        return value
    if isinstance(value, (int, float)):
        return _safe_error_text(value)
    return _safe_error_text(value)


def _safe_error_text(value: object) -> str:
    raw = str(value or "")
    if _REDACTED_ERROR_PATTERN.fullmatch(raw):
        return raw
    encoded = raw.encode("utf-8", errors="replace")
    digest = sha256(encoded).hexdigest()[:16]
    return (
        f"{BROKER_ERROR_REDACTION_PREFIX}"
        f"[sha256={digest};bytes={len(encoded)}]"
    )


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
    return False


def _normalize_key(value: object) -> str:
    return "".join(
        character
        for character in str(value).strip().casefold()
        if character.isalnum()
    )


def _is_sensitive_key(value: object) -> bool:
    if str(value).strip() == "HASH":
        # KIS returns its request-signing hashkey under this exact field name.
        return True
    normalized = _normalize_key(value)
    candidates = [normalized]
    stripped = normalized
    while True:
        wrapper = next(
            (
                candidate
                for candidate in _SENSITIVE_KEY_WRAPPERS
                if stripped.endswith(candidate) and len(stripped) > len(candidate)
            ),
            None,
        )
        if wrapper is None:
            break
        stripped = stripped[: -len(wrapper)]
        candidates.append(stripped)
    return any(
        candidate in _SENSITIVE_KEYS
        or any(
            candidate.endswith(suffix)
            for suffix in _SENSITIVE_KEY_SUFFIXES
        )
        for candidate in candidates
    )
