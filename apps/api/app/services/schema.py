from collections.abc import Mapping
from datetime import UTC, datetime
from time import perf_counter

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import EXEC_OWNED_TABLES


REQUIRED_ALEMBIC_HEADS = ("202609110003",)
EXECUTION_POSITION_SCOPE_REMEDIATION_REASON = (
    "non_flat_execution_position_scope_attribution_required"
)
EXECUTION_POSITION_SCOPE_REMEDIATION = (
    "kis_legacy_position_attribution_dry_run_apply_then_restart"
)

REQUIRED_RUNTIME_COLUMNS = {
    "broker_sessions": ("generation", "refresh_after"),
    "runtime_control_state": (
        "hourly_risk_off_active",
        "hourly_risk_off_epoch",
        "hourly_risk_off_context",
    ),
    "fifo_inventory_isolations": (
        "provider",
        "account_scope",
        "venue",
        "asset_class",
        "exchange",
    ),
    "execution_positions": ("provider", "account_scope", "exchange"),
    "monitoring_checkpoints": ("provider", "account_scope"),
    "crypto_strategy_states": ("account_scope", "strategy_family"),
    "crypto_strategy_risk_states": ("account_scope", "strategy_family"),
    "crypto_strategy_decisions": ("account_scope", "strategy_family"),
    "core_buy_authority_states": (
        "provider",
        "market",
        "currency",
        "runtime_mode",
        "account_scope",
        "fence_active",
        "projection_pending",
        "source_observed_at",
    ),
    "core_buy_authority_observations": (
        "scope_fingerprint",
        "material_fingerprint",
        "source_observed_at",
        "ack_payload",
    ),
}

REQUIRED_UNIQUE_CONSTRAINTS = {
    "execution_jobs": {
        "uq_execution_jobs_plan_item_job_type": (
            "execution_plan_item_id",
            "job_type",
        ),
    },
    "fifo_inventory_isolations": {
        "uq_fifo_inventory_isolations_route_symbol_reconciled": (
            "mode",
            "provider",
            "account_scope",
            "venue",
            "asset_class",
            "exchange",
            "symbol",
            "broker_reconciled_at",
        ),
    },
    "execution_positions": {
        "uq_execution_positions_route_symbol": (
            "mode",
            "provider",
            "account_scope",
            "exchange",
            "symbol",
        ),
    },
    "monitoring_checkpoints": {
        "uq_monitoring_checkpoints_worker_mode_provider_account_scope": (
            "worker_name",
            "mode",
            "provider",
            "account_scope",
        ),
    },
    "crypto_strategy_states": {
        "uq_crypto_strategy_states_scoped_symbol": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
        ),
    },
    "crypto_strategy_risk_states": {
        "uq_crypto_strategy_risk_states_scope": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
        ),
    },
    "crypto_strategy_decisions": {
        "uq_crypto_strategy_decisions_account_family_bar": (
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
            "decision_bar_at",
        ),
    },
    "core_buy_authority_states": {
        "uq_core_buy_authority_states_scope": (
            "provider",
            "market",
            "currency",
            "runtime_mode",
            "account_scope",
        ),
    },
}

REQUIRED_UNIQUE_INDEXES = {
    "fifo_inventory_isolations": {
        "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled": (
            "mode",
            "symbol",
            "broker_reconciled_at",
        ),
    },
    "execution_positions": {
        "uq_execution_positions_legacy_symbol_mode": ("mode", "symbol"),
    },
    "monitoring_checkpoints": {
        "uq_monitoring_checkpoints_legacy_worker_mode": (
            "worker_name",
            "mode",
        ),
    },
    "crypto_strategy_states": {
        "uq_crypto_strategy_states_legacy_mode_provider_version_symbol": (
            "mode",
            "provider",
            "strategy_version",
            "symbol",
        ),
    },
    "crypto_strategy_risk_states": {
        "uq_crypto_strategy_risk_states_legacy_mode_provider_version": (
            "mode",
            "provider",
            "strategy_version",
        ),
    },
    "crypto_strategy_decisions": {
        "uq_crypto_strategy_decisions_legacy_scope_bar": (
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            "decision_bar_at",
        ),
    },
}
FIFO_LEGACY_SCOPE_PREDICATE = (
    "provider IS NULL AND account_scope IS NULL AND venue IS NULL "
    "AND asset_class IS NULL AND exchange IS NULL"
)
POSITION_LEGACY_SCOPE_PREDICATE = (
    "provider IS NULL AND account_scope IS NULL AND exchange IS NULL"
)
MONITORING_LEGACY_SCOPE_PREDICATE = (
    "provider IS NULL AND account_scope IS NULL"
)
CRYPTO_LEGACY_SCOPE_PREDICATE = "account_scope IS NULL AND strategy_family IS NULL"
REQUIRED_UNIQUE_INDEX_PREDICATES = {
    "fifo_inventory_isolations": {
        "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled": (
            FIFO_LEGACY_SCOPE_PREDICATE
        ),
    },
    "execution_positions": {
        "uq_execution_positions_legacy_symbol_mode": (
            POSITION_LEGACY_SCOPE_PREDICATE
        ),
    },
    "monitoring_checkpoints": {
        "uq_monitoring_checkpoints_legacy_worker_mode": (
            MONITORING_LEGACY_SCOPE_PREDICATE
        ),
    },
    "crypto_strategy_states": {
        "uq_crypto_strategy_states_legacy_mode_provider_version_symbol": (
            CRYPTO_LEGACY_SCOPE_PREDICATE
        ),
    },
    "crypto_strategy_risk_states": {
        "uq_crypto_strategy_risk_states_legacy_mode_provider_version": (
            CRYPTO_LEGACY_SCOPE_PREDICATE
        ),
    },
    "crypto_strategy_decisions": {
        "uq_crypto_strategy_decisions_legacy_scope_bar": (
            CRYPTO_LEGACY_SCOPE_PREDICATE
        ),
    },
}

REQUIRED_CHECK_CONSTRAINTS = {
    "fifo_inventory_isolations": {
        "ck_fifo_inventory_isolations_scope_complete": (
            "(provider IS NULL AND account_scope IS NULL AND venue IS NULL "
            "AND asset_class IS NULL AND exchange IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND venue IS NOT NULL AND asset_class IS NOT NULL "
            "AND exchange IS NOT NULL)"
        ),
    },
    "execution_positions": {
        "ck_execution_positions_scope_complete": (
            "(provider IS NULL AND account_scope IS NULL AND exchange IS NULL) "
            "OR (provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND exchange IS NOT NULL)"
        ),
    },
    "monitoring_checkpoints": {
        "ck_monitoring_checkpoints_scope_complete": (
            "(provider IS NULL AND account_scope IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL)"
        ),
    },
    "crypto_strategy_states": {
        "ck_crypto_strategy_states_scope_complete": (
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)"
        ),
    },
    "crypto_strategy_risk_states": {
        "ck_crypto_strategy_risk_states_scope_complete": (
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)"
        ),
    },
    "crypto_strategy_decisions": {
        "ck_crypto_strategy_decisions_scope_complete": (
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)"
        ),
    },
    "core_buy_authority_states": {
        "ck_core_buy_authority_states_krx_scope": (
            "provider = 'kis' AND market = 'KRX' AND currency = 'KRW'"
        ),
    },
}


def check_runtime_schema(db: Session) -> dict[str, object]:
    started = perf_counter()
    try:
        inspector = inspect(db.get_bind())
        existing = set(inspector.get_table_names())
        alembic = _check_alembic_revision(db=db, existing_tables=existing)
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }

    missing = [table for table in EXEC_OWNED_TABLES if table not in existing]
    missing_columns: dict[str, list[str]] = {}
    missing_safety_invariants: dict[str, list[str]] = {}
    blocking_data: dict[str, dict[str, object]] = {}
    try:
        for table, required_columns in REQUIRED_RUNTIME_COLUMNS.items():
            if table not in existing:
                continue
            existing_columns = {
                str(column["name"]) for column in inspector.get_columns(table)
            }
            absent = [
                column for column in required_columns if column not in existing_columns
            ]
            if absent:
                missing_columns[table] = absent
        missing_safety_invariants = _missing_safety_invariants(
            inspector=inspector,
            existing_tables=existing,
        )
        if (
            "execution_positions" in existing
            and "execution_positions" not in missing_columns
        ):
            blocking_position_count = _count_unattributed_non_flat_positions(db)
            if blocking_position_count:
                blocking_data["execution_positions"] = {
                    "status": "blocked",
                    "reason": EXECUTION_POSITION_SCOPE_REMEDIATION_REASON,
                    "blocking_row_count": blocking_position_count,
                    "remediation": EXECUTION_POSITION_SCOPE_REMEDIATION,
                }
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }
    schema_incomplete = bool(
        missing
        or missing_columns
        or missing_safety_invariants
        or alembic["status"] != "ok"
    )
    if schema_incomplete:
        status = "missing"
    elif blocking_data:
        status = "blocked"
    else:
        status = "ok"
    return {
        "status": status,
        "checked_at": datetime.now(UTC).isoformat(),
        "latency_ms": round((perf_counter() - started) * 1000, 3),
        "owned_tables": list(EXEC_OWNED_TABLES),
        "missing_tables": missing,
        "missing_columns": missing_columns,
        "missing_safety_invariants": missing_safety_invariants,
        "blocking_data": blocking_data,
        "alembic": alembic,
    }


def execution_position_readiness(
    runtime_schema: dict[str, object],
) -> dict[str, object] | None:
    if runtime_schema.get("status") != "blocked":
        return None
    blocking_data = runtime_schema.get("blocking_data")
    if not isinstance(blocking_data, dict):
        return None
    position_readiness = blocking_data.get("execution_positions")
    if not isinstance(position_readiness, dict):
        return None
    return {**position_readiness, "restart_required": True}


def _count_unattributed_non_flat_positions(db: Session) -> int:
    count = db.scalar(
        text(
            """
            SELECT COUNT(*)
            FROM execution_positions
            WHERE (quantity <> 0 OR COALESCE(base_quantity, 0) <> 0)
              AND (
                  provider IS NULL
                  OR account_scope IS NULL
                  OR exchange IS NULL
              )
            """
        )
    )
    return int(count or 0)


def _check_alembic_revision(
    *,
    db: Session,
    existing_tables: set[str],
) -> dict[str, object]:
    expected_heads = sorted(REQUIRED_ALEMBIC_HEADS)
    if "alembic_version" not in existing_tables:
        return {
            "status": "missing",
            "expected_heads": expected_heads,
            "current_heads": [],
        }
    current_heads = sorted(
        str(version)
        for version in db.execute(text("SELECT version_num FROM alembic_version"))
        .scalars()
        .all()
    )
    if current_heads == expected_heads:
        status = "ok"
    elif (
        len(current_heads) == 1
        and len(expected_heads) == 1
        and current_heads[0] < expected_heads[0]
    ):
        status = "behind"
    else:
        status = "mismatch"
    return {
        "status": status,
        "expected_heads": expected_heads,
        "current_heads": current_heads,
    }


def _missing_safety_invariants(
    *,
    inspector: object,
    existing_tables: set[str],
) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    inspected_tables = (
        set(REQUIRED_UNIQUE_CONSTRAINTS)
        | set(REQUIRED_UNIQUE_INDEXES)
        | set(REQUIRED_CHECK_CONSTRAINTS)
    )
    for table in sorted(inspected_tables.intersection(existing_tables)):
        table_missing: list[str] = []
        unique_constraints = inspector.get_unique_constraints(table)  # type: ignore[attr-defined]
        for name, columns in REQUIRED_UNIQUE_CONSTRAINTS.get(table, {}).items():
            if not _has_named_columns(
                unique_constraints,
                name=name,
                columns=columns,
            ):
                table_missing.append(name)

        indexes = inspector.get_indexes(table)  # type: ignore[attr-defined]
        for name, columns in REQUIRED_UNIQUE_INDEXES.get(table, {}).items():
            if not _has_named_columns(
                indexes,
                name=name,
                columns=columns,
                require_unique=True,
                predicate=REQUIRED_UNIQUE_INDEX_PREDICATES.get(table, {}).get(name),
            ):
                table_missing.append(name)

        check_constraints = inspector.get_check_constraints(table)  # type: ignore[attr-defined]
        for name, expression in REQUIRED_CHECK_CONSTRAINTS.get(table, {}).items():
            if not _has_check_constraint(
                check_constraints,
                expression=expression,
            ):
                table_missing.append(name)
        if table_missing:
            missing[table] = table_missing
    return missing


def _has_named_columns(
    candidates: list[dict[str, object]],
    *,
    name: str,
    columns: tuple[str, ...],
    require_unique: bool = False,
    predicate: str | None = None,
) -> bool:
    return any(
        candidate.get("name") == name
        and tuple(str(column) for column in candidate.get("column_names") or ())
        == columns
        and (not require_unique or bool(candidate.get("unique")))
        and _has_index_predicate(candidate, predicate=predicate)
        for candidate in candidates
    )


def _has_index_predicate(
    candidate: dict[str, object],
    *,
    predicate: str | None,
) -> bool:
    if predicate is None:
        return True
    dialect_options = candidate.get("dialect_options")
    if not isinstance(dialect_options, Mapping):
        return False
    normalized_predicate = _normalize_check_expression(predicate)
    return any(
        actual is not None
        and _normalize_check_expression(str(actual)) == normalized_predicate
        for actual in (
            dialect_options.get("sqlite_where"),
            dialect_options.get("postgresql_where"),
        )
    )


def _has_check_constraint(
    candidates: list[dict[str, object]],
    *,
    expression: str,
) -> bool:
    normalized_expression = _normalize_check_expression(expression)
    # PostgreSQL truncates long naming-convention-generated identifiers and
    # appends a hash. The exact safety expression is the portable invariant;
    # requiring the rendered name would incorrectly degrade a valid schema.
    return any(
        _normalize_check_expression(str(candidate.get("sqltext") or ""))
        == normalized_expression
        for candidate in candidates
    )


def _normalize_check_expression(value: str) -> str:
    value = _strip_postgresql_noop_text_casts(value)
    normalized: list[str] = []
    index = 0
    inside_literal = False
    while index < len(value):
        character = value[index]
        if character == "'":
            normalized.append(character)
            if inside_literal and index + 1 < len(value) and value[index + 1] == "'":
                normalized.append("'")
                index += 2
                continue
            inside_literal = not inside_literal
        elif inside_literal:
            # SQL string values are case- and whitespace-sensitive. Only the
            # surrounding expression syntax is normalized for comparison.
            normalized.append(character)
        elif not character.isspace() and character not in {'(', ')', '"', '`'}:
            normalized.append(character.casefold())
        index += 1
    return "".join(normalized)


def _strip_postgresql_noop_text_casts(value: str) -> str:
    normalized: list[str] = []
    index = 0
    inside_literal = False
    while index < len(value):
        character = value[index]
        if character == "'":
            normalized.append(character)
            if inside_literal and index + 1 < len(value) and value[index + 1] == "'":
                normalized.append("'")
                index += 2
                continue
            inside_literal = not inside_literal
            index += 1
            continue
        if not inside_literal and value.startswith("::", index):
            type_start = index + 2
            while type_start < len(value) and value[type_start].isspace():
                type_start += 1
            type_end = type_start + len("text")
            next_character = value[type_end : type_end + 1]
            if (
                value[type_start:type_end].casefold() == "text"
                and not (next_character.isalnum() or next_character == "_")
            ):
                index = type_end
                continue
        normalized.append(character)
        index += 1
    return "".join(normalized)
