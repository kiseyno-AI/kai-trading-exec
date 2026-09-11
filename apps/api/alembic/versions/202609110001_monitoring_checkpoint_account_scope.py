"""Scope monitoring checkpoints to an exact broker account.

Revision ID: 202609110001
Revises: 202609090004
"""

from collections.abc import Sequence
import json
import re

from alembic import op
import sqlalchemy as sa


revision: str = "202609110001"
down_revision: str | None = "202609090004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE_NAME = "monitoring_checkpoints"
LEGACY_CONSTRAINT = "uq_monitoring_checkpoints_worker_name_mode"
SCOPED_CONSTRAINT = "uq_monitoring_checkpoints_worker_mode_provider_account_scope"
SCOPE_CHECK = "ck_monitoring_checkpoints_scope_complete"
LEGACY_SCOPE_INDEX = "uq_monitoring_checkpoints_legacy_worker_mode"
LEGACY_SCOPE_PREDICATE = "provider IS NULL AND account_scope IS NULL"
_PROVIDER_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_ACCOUNT_SCOPE_PATTERN = re.compile(r"^[0-9a-f]{24}$")


def upgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("provider", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("account_scope", sa.String(length=128), nullable=True)
        )

    _backfill_exact_execution_scopes(bind)

    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_constraint(LEGACY_CONSTRAINT, type_="unique")
        batch_op.create_check_constraint(
            SCOPE_CHECK,
            "(provider IS NULL AND account_scope IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL)",
        )
        batch_op.create_unique_constraint(
            SCOPED_CONSTRAINT,
            ["worker_name", "mode", "provider", "account_scope"],
        )
        batch_op.create_index(
            LEGACY_SCOPE_INDEX,
            ["worker_name", "mode"],
            unique=True,
            sqlite_where=sa.text(LEGACY_SCOPE_PREDICATE),
            postgresql_where=sa.text(LEGACY_SCOPE_PREDICATE),
        )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_postgresql_table(bind)
    duplicate = bind.execute(
        sa.text(
            f"SELECT worker_name, mode FROM {TABLE_NAME} "
            "GROUP BY worker_name, mode HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if duplicate is not None:
        raise RuntimeError(
            "cannot downgrade: colliding scoped monitoring checkpoints would "
            "violate the legacy one-row constraint"
        )
    attributed = bind.execute(
        sa.text(
            f"SELECT 1 FROM {TABLE_NAME} "
            "WHERE provider IS NOT NULL OR account_scope IS NOT NULL LIMIT 1"
        )
    ).first()
    if attributed is not None:
        raise RuntimeError(
            "cannot downgrade: attributed monitoring checkpoints would lose "
            "their broker account scope"
        )
    with op.batch_alter_table(TABLE_NAME) as batch_op:
        batch_op.drop_index(LEGACY_SCOPE_INDEX)
        batch_op.drop_constraint(SCOPED_CONSTRAINT, type_="unique")
        batch_op.drop_constraint(SCOPE_CHECK, type_="check")
        batch_op.create_unique_constraint(
            LEGACY_CONSTRAINT,
            ["worker_name", "mode"],
        )
        batch_op.drop_column("account_scope")
        batch_op.drop_column("provider")


def _backfill_exact_execution_scopes(bind: sa.engine.Connection) -> None:
    table = sa.Table(TABLE_NAME, sa.MetaData(), autoload_with=bind)
    rows = bind.execute(sa.select(table.c.id, table.c.details)).mappings().all()
    for row in rows:
        scope = _exact_execution_scope(row["details"])
        if scope is None:
            continue
        bind.execute(
            table.update()
            .where(table.c.id == row["id"])
            .values(
                provider=scope["provider"],
                account_scope=scope["account_scope"],
            )
        )


def _exact_execution_scope(details: object) -> dict[str, str] | None:
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except (TypeError, ValueError):
            return None
    if not isinstance(details, dict):
        return None
    scope = details.get("execution_scope")
    if not isinstance(scope, dict) or set(scope) != {
        "policy_version",
        "provider",
        "account_scope",
    }:
        return None
    if type(scope.get("policy_version")) is not int or scope["policy_version"] != 1:
        return None
    provider = scope.get("provider")
    account_scope = scope.get("account_scope")
    if (
        not isinstance(provider, str)
        or _PROVIDER_PATTERN.fullmatch(provider) is None
        or not isinstance(account_scope, str)
        or _ACCOUNT_SCOPE_PATTERN.fullmatch(account_scope) is None
    ):
        return None
    return {"provider": provider, "account_scope": account_scope}


def _lock_postgresql_table(bind: sa.engine.Connection) -> None:
    if bind.dialect.name == "postgresql":
        bind.execute(
            sa.text("LOCK TABLE monitoring_checkpoints IN ACCESS EXCLUSIVE MODE")
        )
