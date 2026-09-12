from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.runtime import MonitoringCheckpoint
from app.services.position_identity import EXECUTION_SCOPE_POLICY_VERSION


def exact_monitoring_execution_scope(
    value: object,
    *,
    provider: str,
) -> dict[str, object] | None:
    if (
        not isinstance(value, dict)
        or set(value) != {"policy_version", "provider", "account_scope"}
        or type(value.get("policy_version")) is not int
        or value.get("policy_version") != EXECUTION_SCOPE_POLICY_VERSION
        or value.get("provider") != provider
    ):
        return None
    account_scope = value.get("account_scope")
    if (
        not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
    ):
        return None
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": provider,
        "account_scope": account_scope,
    }


def upsert_scoped_monitoring_checkpoint(
    *,
    db: Session,
    worker_name: str,
    mode: str,
    status: str,
    observed_at: datetime,
    details: dict[str, object],
    succeeded: bool,
    provider: str,
) -> MonitoringCheckpoint | None:
    if (
        provider == "alpaca"
        and details.get("execution_scope_source")
        != "authenticated_broker_account"
    ):
        return None
    execution_scope = exact_monitoring_execution_scope(
        details.get("execution_scope"),
        provider=provider,
    )
    if execution_scope is None:
        return None
    account_scope = str(execution_scope["account_scope"])
    checkpoint = db.scalar(
        select(MonitoringCheckpoint).where(
            MonitoringCheckpoint.worker_name == worker_name,
            MonitoringCheckpoint.mode == mode,
            MonitoringCheckpoint.provider == provider,
            MonitoringCheckpoint.account_scope == account_scope,
        )
    )
    if checkpoint is None:
        checkpoint = MonitoringCheckpoint(
            worker_name=worker_name,
            mode=mode,
            provider=provider,
            account_scope=account_scope,
        )
        db.add(checkpoint)
    checkpoint.status = status
    checkpoint.last_heartbeat_at = observed_at
    if succeeded:
        checkpoint.last_success_at = observed_at
    checkpoint.details = {**details, "execution_scope": execution_scope}
    return checkpoint
