from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "apps"
    / "api"
    / "alembic"
    / "versions"
    / "202609090004_redact_legacy_sensitive_payloads.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load migration {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _payload() -> dict[str, object]:
    return {
        "provider": "kis",
        "execution_scope": {
            "account_scope": "a" * 24,
            "configured_route_fingerprint": "b" * 32,
        },
        "provenance": {"session_id": "agent-session-id"},
        "nested": {
            "CANO": "raw-account",
            "account-number": "raw-account-2",
            "Authorization": "Bearer token",
            "Set-Cookie": "JSESSIONID=set-cookie-secret",
            "Cookie-Header": "session=cookie-header-secret",
            "X-Session-ID": "session-id-secret",
            "auth": "basic-auth-secret",
            "authentication": {"value": "authentication-secret"},
            "credentials": {"password": "credential-secret"},
            "cookie": "session=cookie-secret",
            "session_token": "session-token-secret",
            "session_secret": "session-secret",
            "bearer": "bare-bearer-secret",
            "secret": "generic-secret",
            "access_token": "token",
            "app-secret": "secret",
            "HASH": "kis-hashkey",
            "hashkey": "kis-hashkey-2",
            "token": "generic-token",
            "kis_app_secret": "kis-secret",
            "alpaca_api_secret": "alpaca-secret",
            "upbit_secret_key": "upbit-secret",
            "APCA-API-KEY-ID": "alpaca-key-id",
            "oauth_client_id": "oauth-client",
            "provider_consumer_key": "consumer-key",
            "provider_id_token": "identity-token",
            "provider_account_id": "provider-account",
            "hash": "safe-content-hash",
            "safe_metric": "kept",
        },
        "rows": [
            {"client_secret": "secret-2", "symbol": "005930"},
            {"quantity": 2},
        ],
    }


def _assert_sanitized(value: object, *, scrub_error_fields: bool = False) -> None:
    if scrub_error_fields:
        assert isinstance(value, dict)
        assert value["provider"] == "kis"
        redacted_values = (
            value["execution_scope"]["account_scope"],
            value["execution_scope"]["configured_route_fingerprint"],
            value["provenance"]["session_id"],
            value["nested"]["hash"],
            value["nested"]["safe_metric"],
            value["rows"][0]["symbol"],
            value["rows"][1]["quantity"],
        )
        assert all(
            isinstance(item, str)
            and item.startswith("broker_error_details_redacted[")
            for item in redacted_values
        )
        return
    assert value == {
        "provider": "kis",
        "execution_scope": {
            "account_scope": "a" * 24,
            "configured_route_fingerprint": "b" * 32,
        },
        "provenance": {"session_id": "agent-session-id"},
        "nested": {"hash": "safe-content-hash", "safe_metric": "kept"},
        "rows": [{"symbol": "005930"}, {"quantity": 2}],
    }


def test_upgrade_redacts_every_declared_runtime_payload_column() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    tables: dict[str, sa.Table] = {}
    for table_name, payload_columns in migration.PAYLOAD_COLUMNS.items():
        tables[table_name] = sa.Table(
            table_name,
            metadata,
            sa.Column("id", sa.String(36), primary_key=True),
            *(sa.Column(column_name, sa.JSON()) for column_name in payload_columns),
        )
    metadata.create_all(engine)

    with engine.begin() as connection:
        for index, table in enumerate(tables.values()):
            connection.execute(
                table.insert().values(
                    id=f"row-{index}",
                    **{
                        column_name: _payload()
                        for column_name in migration.PAYLOAD_COLUMNS[table.name]
                    },
                )
            )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    with engine.connect() as connection:
        for table_name, table in tables.items():
            row = connection.execute(sa.select(table)).mappings().one()
            for column_name in migration.PAYLOAD_COLUMNS[table_name]:
                _assert_sanitized(
                    row[column_name],
                    scrub_error_fields=table_name == "execution_failures",
                )
    engine.dispose()


def test_upgrade_is_idempotent_and_skips_tables_or_columns_not_present() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    table = sa.Table(
        "broker_orders",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("raw_broker_payload", sa.JSON()),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(
            table.insert().values(id="order-1", raw_broker_payload=_payload())
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()
            migration.upgrade()

    with engine.connect() as connection:
        stored = connection.scalar(
            sa.select(table.c.raw_broker_payload).where(table.c.id == "order-1")
        )
    _assert_sanitized(stored)
    engine.dispose()


def test_downgrade_does_not_restore_removed_secrets() -> None:
    migration = _load_migration()
    assert migration.downgrade() is None


def test_upgrade_locks_all_existing_postgres_tables_before_rewriting_rows() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="postgresql")
    inspector = MagicMock()
    inspector.get_table_names.return_value = [
        "broker_orders",
        "execution_plan_items",
    ]
    inspector.get_columns.side_effect = lambda table_name: [
        {"name": "id"},
        {
            "name": (
                "raw_broker_payload"
                if table_name == "broker_orders"
                else "risk_context"
            )
        },
    ]
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind

    with (
        patch.object(migration, "op", migration_op),
        patch.object(migration.sa, "inspect", return_value=inspector),
        patch.object(migration, "_redact_table") as redact_table,
    ):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "LOCK TABLE broker_orders, execution_plan_items "
        "IN ACCESS EXCLUSIVE MODE"
    )
    assert redact_table.call_count == 2


def test_upgrade_claims_a_sqlite_writer_fence_before_rewriting_rows() -> None:
    migration = _load_migration()
    bind = MagicMock()
    bind.dialect = SimpleNamespace(name="sqlite")
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["broker_orders"]
    inspector.get_columns.return_value = [
        {"name": "id"},
        {"name": "raw_broker_payload"},
    ]
    migration_op = MagicMock()
    migration_op.get_bind.return_value = bind

    with (
        patch.object(migration, "op", migration_op),
        patch.object(migration.sa, "inspect", return_value=inspector),
        patch.object(migration, "_redact_table") as redact_table,
    ):
        migration.upgrade()

    assert str(bind.execute.call_args_list[0].args[0]) == (
        "UPDATE broker_orders SET id = id WHERE 0"
    )
    redact_table.assert_called_once()


def test_upgrade_scrubs_legacy_broker_error_text_without_deleting_provenance() -> None:
    migration = _load_migration()
    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    metadata = sa.MetaData()
    broker_sessions = sa.Table(
        "broker_sessions",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("last_error", sa.Text()),
    )
    broker_session_events = sa.Table(
        "broker_session_events",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("outcome", sa.String(16)),
        sa.Column("message", sa.Text()),
        sa.Column("current_state", sa.JSON()),
    )
    broker_orders = sa.Table(
        "broker_orders",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("request_metadata", sa.JSON()),
        sa.Column("response_metadata", sa.JSON()),
        sa.Column("raw_broker_payload", sa.JSON()),
        sa.Column("last_error_message", sa.Text()),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(
            broker_sessions.insert().values(
                id="kis:paper",
                last_error="Authorization: Bearer legacy-secret-marker",
            )
        )
        connection.execute(
            broker_session_events.insert(),
            [
                {
                    "id": "failed",
                    "outcome": "rejected",
                    "message": "CANO=12345678 token=legacy-secret-marker",
                    "current_state": {
                        "last_error": "token=legacy-secret-marker",
                        "provenance": {"session_id": "agent-session-id"},
                    },
                },
                {
                    "id": "success",
                    "outcome": "applied",
                    "message": "broker access token issued",
                    "current_state": {
                        "provenance": {"session_id": "agent-session-id"}
                    },
                },
            ],
        )
        connection.execute(
            broker_orders.insert().values(
                id="order-1",
                request_metadata={
                    "provenance": {"session_id": "agent-session-id"}
                },
                response_metadata={
                    "provider": "alpaca",
                    "request_id": "legacy-secret-marker",
                },
                raw_broker_payload={
                    "code": "AUTH_ECHO",
                    "message": "token=legacy-secret-marker",
                    "detail": 9876543210987654,
                    "unknown": 9876543210987654,
                },
                last_error_message="token=legacy-secret-marker",
            )
        )
        operations = Operations(MigrationContext.configure(connection))
        with patch.object(migration, "op", operations):
            migration.upgrade()

    with engine.connect() as connection:
        session_error = connection.scalar(sa.select(broker_sessions.c.last_error))
        events = {
            row.id: row
            for row in connection.execute(
                sa.select(broker_session_events).order_by(broker_session_events.c.id)
            )
        }
        broker_order = connection.execute(sa.select(broker_orders)).mappings().one()

    assert "legacy-secret-marker" not in session_error
    assert session_error.startswith("broker_error_details_redacted[")
    assert "legacy-secret-marker" not in events["failed"].message
    assert events["failed"].current_state == {
        "last_error": events["failed"].current_state["last_error"],
        "provenance": {"session_id": "agent-session-id"},
    }
    assert events["failed"].current_state["last_error"].startswith(
        "broker_error_details_redacted["
    )
    assert events["success"].message == "broker access token issued"
    assert events["success"].current_state == {
        "provenance": {"session_id": "agent-session-id"}
    }
    assert broker_order["request_metadata"] == {
        "provenance": {"session_id": "agent-session-id"}
    }
    assert "legacy-secret-marker" not in repr(
        {
            "response_metadata": broker_order["response_metadata"],
            "raw_broker_payload": broker_order["raw_broker_payload"],
            "last_error_message": broker_order["last_error_message"],
        }
    )
    assert "9876543210987654" not in repr(broker_order["raw_broker_payload"])
    assert broker_order["raw_broker_payload"]["code"] == "AUTH_ECHO"
    engine.dispose()


def test_error_payload_scrub_matches_runtime_strict_diagnostic_allowlist() -> None:
    migration = _load_migration()
    payload = {
        "provider": "alpaca",
        "status_code": 401,
        "classification": "auth",
        "error_classification": "account_authority",
        "status": "rejected",
        "code": "AUTH_ECHO",
        "error_code": "execution_account_scope_mismatch",
        "rt_cd": "-1",
        "error": "Authorization: Bearer legacy-error-secret",
        "renamed_authorization_value": "Bearer legacy-authorization-secret",
        "nested": {
            "client_secret_value": "legacy-client-secret",
            "arbitrary_string": "legacy-free-text",
            "arbitrary_number": 987654321,
            "retry_after": 60,
        },
    }

    sanitized = migration._sanitize(payload, scrub_message=True)

    assert sanitized["provider"] == "alpaca"
    assert sanitized["status_code"] == 401
    assert sanitized["classification"] == "auth"
    assert sanitized["error_classification"] == "account_authority"
    assert sanitized["status"] == "rejected"
    assert sanitized["code"] == "AUTH_ECHO"
    assert sanitized["error_code"] == "execution_account_scope_mismatch"
    assert sanitized["rt_cd"] == "-1"
    assert "renamed_authorization_value" not in sanitized
    assert "client_secret_value" not in sanitized["nested"]
    for key in ("error",):
        assert sanitized[key].startswith("broker_error_details_redacted[")
    for key in ("arbitrary_string", "arbitrary_number"):
        assert sanitized["nested"][key].startswith(
            "broker_error_details_redacted["
        )
    assert sanitized["nested"]["retry_after"] == 60
    assert "legacy-" not in repr(sanitized)


def test_non_error_payload_keeps_business_values_but_removes_renamed_secrets() -> None:
    migration = _load_migration()
    payload = {
        "provider": "alpaca",
        "symbol": "AAPL",
        "filled_quantity": 2,
        "message": "ordinary successful fill",
        "authorization_value": "Bearer legacy-success-secret",
        "nested": {"client_secret_value": "legacy-client-secret", "price": "50.25"},
    }

    sanitized = migration._sanitize(payload, scrub_message=False)

    assert sanitized == {
        "provider": "alpaca",
        "symbol": "AAPL",
        "filled_quantity": 2,
        "message": "ordinary successful fill",
        "nested": {"price": "50.25"},
    }
