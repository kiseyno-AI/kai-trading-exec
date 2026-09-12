from datetime import UTC, datetime
import threading
from time import perf_counter

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.session import ping_database
from app.services.position_identity import stable_account_scope_hmac_key
from app.services.schema import check_runtime_schema
from app.services.runtime import RuntimeMetadata, liveness_payload


_database_healthcheck_lock = threading.Lock()
_STABLE_SCOPE_SECRET_SETTING = (
    "KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET"
)
_CONFIGURED_SCOPE_SETTING = "KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE"


def check_database(db: Session) -> dict[str, object]:
    started = perf_counter()
    if not _database_healthcheck_lock.acquire(blocking=False):
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": "HealthCheckInProgress",
        }
    try:
        ping_database(db)
    except SQLAlchemyError as exc:
        return {
            "status": "error",
            "checked_at": datetime.now(UTC).isoformat(),
            "latency_ms": round((perf_counter() - started) * 1000, 3),
            "error": exc.__class__.__name__,
        }
    finally:
        _database_healthcheck_lock.release()
    return {
        "status": "ok",
        "checked_at": datetime.now(UTC).isoformat(),
        "latency_ms": round((perf_counter() - started) * 1000, 3),
    }


def check_execution_account_scope(settings: Settings) -> dict[str, object]:
    if settings.runtime_mode != "live":
        return {"status": "ok", "providers": [], "source": "not_applicable"}

    providers = _configured_live_broker_providers(settings)
    stable_secret_value = str(
        settings.account_drawdown_recovery_scope_secret or ""
    ).strip()
    stable_secret = stable_account_scope_hmac_key(settings) is not None
    configured_scope = bool(
        str(settings.execution_position_account_scope or "").strip()
    )
    missing: list[str] = []
    missing_secret = False
    missing_label = False
    provider_sources: dict[str, str] = {}
    for provider in providers:
        if provider == "alpaca":
            # Alpaca exposes a broker-owned account ID. Position identity uses
            # that stronger material and therefore always needs a stable HMAC
            # key in live mode; an operator label cannot replace it.
            if not stable_secret:
                missing.append(provider)
                missing_secret = True
            else:
                provider_sources[provider] = "stable_hmac_secret"
        elif provider == "upbit":
            if not configured_scope:
                missing.append(provider)
                missing_label = True
            else:
                provider_sources[provider] = "configured_route_label"
        elif configured_scope:
            provider_sources[provider] = "configured_route_label"
        elif stable_secret:
            provider_sources[provider] = "stable_hmac_secret"
        else:
            missing.append(provider)
            missing_secret = True
            missing_label = True
    if missing:
        required_settings: list[str] = []
        if missing_secret:
            required_settings.append(_STABLE_SCOPE_SECRET_SETTING)
        if missing_label:
            required_settings.append(_CONFIGURED_SCOPE_SETTING)
        result: dict[str, object] = {
            "status": "error",
            "providers": missing,
            "reason": (
                "live execution account identity lacks a stable opaque scope; "
                "configure the required setting before broker orders or position "
                "reconciliation"
            ),
            "required_settings": required_settings,
        }
        if stable_secret_value and not stable_secret and missing_secret:
            result["invalid_settings"] = [_STABLE_SCOPE_SECRET_SETTING]
        return result

    sources = set(provider_sources.values())
    if not providers:
        source = "not_applicable"
    elif len(sources) == 1:
        source = next(iter(sources))
    else:
        source = "provider_specific"
    result = {"status": "ok", "providers": providers, "source": source}
    if len(sources) > 1:
        result["provider_sources"] = provider_sources
    return result


def _configured_live_broker_providers(settings: Settings) -> list[str]:
    providers: list[str] = []
    if all(
        str(value or "").strip()
        for value in (
            settings.kis_app_key,
            settings.kis_app_secret,
            settings.kis_account_number,
            settings.kis_account_product_code,
        )
    ):
        providers.append("kis")
    if settings.alpaca_live_trading_enabled and all(
        str(value or "").strip()
        for value in (settings.alpaca_api_key, settings.alpaca_api_secret)
    ):
        providers.append("alpaca")
    if settings.upbit_live_trading_enabled and all(
        str(value or "").strip()
        for value in (settings.upbit_access_key, settings.upbit_secret_key)
    ):
        providers.append("upbit")
    return providers


def build_health_payload(
    *,
    settings: Settings,
    runtime: RuntimeMetadata,
    db: Session,
    runtime_startup_gate: dict[str, object] | None = None,
) -> dict[str, object]:
    database = check_database(db)
    schema = check_runtime_schema(db) if database["status"] == "ok" else {
        "status": "skipped",
        "reason": "database dependency is not healthy",
    }
    dependencies = {
        "database": database,
        "runtime_schema": schema,
        "execution_account_scope": check_execution_account_scope(settings),
        "automated_runtime_startup": _runtime_startup_dependency(
            runtime_startup_gate
        ),
    }
    status = "ok" if all(dep["status"] == "ok" for dep in dependencies.values()) else "degraded"
    return {
        "status": status,
        "service": settings.service_name,
        "environment": settings.environment,
        "mode": settings.runtime_mode,
        "version": "0.1.0",
        "checked_at": datetime.now(UTC).isoformat(),
        "dependencies": dependencies,
        "liveness": liveness_payload(runtime),
    }


def _runtime_startup_dependency(
    runtime_startup_gate: dict[str, object] | None,
) -> dict[str, object]:
    if not isinstance(runtime_startup_gate, dict):
        return {
            "status": "ok",
            "startup_status": "not_reported",
            "reason": None,
        }
    startup_status = str(runtime_startup_gate.get("status") or "unknown")
    return {
        **runtime_startup_gate,
        "status": "blocked" if startup_status == "blocked" else "ok",
        "startup_status": startup_status,
    }
