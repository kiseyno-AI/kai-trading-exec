from __future__ import annotations

import hmac
from dataclasses import dataclass
from hashlib import sha256

from app.core.config import Settings


POSITION_IDENTITY_KEY = "_position_identity"
POSITION_IDENTITY_VERSION = "execution-position-route-v2"
EXECUTION_SCOPE_POLICY_VERSION = 1
ACCOUNT_SCOPE_HMAC_MIN_KEY_BYTES = 32


class PositionIdentityConflictError(RuntimeError):
    """Raised before position truth can cross a broker/account/market route."""


@dataclass(frozen=True)
class PositionIdentity:
    provider: str
    account_scope: str
    exchange: str

    def as_payload(self) -> dict[str, str]:
        return {
            "version": POSITION_IDENTITY_VERSION,
            "provider": self.provider,
            "account_scope": self.account_scope,
            "exchange": self.exchange,
        }


def build_position_identity(
    *,
    settings: Settings,
    provider: str,
    exchange: str,
    broker_account_id: str | None = None,
) -> PositionIdentity:
    normalized_provider = str(provider or "unknown").strip().lower() or "unknown"
    normalized_exchange = str(exchange or "unknown").strip().upper() or "UNKNOWN"
    account_material, scope_key = _stable_account_material(
        settings=settings,
        provider=normalized_provider,
        broker_account_id=broker_account_id,
    )
    message = (
        f"{POSITION_IDENTITY_VERSION}\0{normalized_provider}\0{account_material}"
    ).encode("utf-8")
    account_scope = (
        hmac.new(scope_key, message, sha256).hexdigest()[:24]
        if scope_key is not None
        else sha256(message).hexdigest()[:24]
    )
    return PositionIdentity(
        provider=normalized_provider,
        account_scope=account_scope,
        exchange=normalized_exchange,
    )


def build_authenticated_broker_position_identity(
    *,
    settings: Settings,
    provider: str,
    exchange: str,
    broker_account_id: str,
) -> PositionIdentity:
    """Derive an opaque identity from broker-authenticated account truth.

    This explicit authority path rejects a missing account id and mirrors the
    broker-id precedence used by :func:`build_position_identity`.  It pins a
    pending non-KIS buy to the exact account observed at plan admission, so
    changing credentials cannot silently redirect that buy.
    """

    normalized_provider = str(provider or "").strip().lower()
    normalized_exchange = str(exchange or "unknown").strip().upper() or "UNKNOWN"
    normalized_account_id = str(broker_account_id or "").strip()
    if not normalized_provider or not normalized_account_id:
        raise PositionIdentityConflictError(
            "authenticated broker account identity is unavailable"
        )

    scope_key = stable_account_scope_hmac_key(settings)
    if scope_key is None and settings.runtime_mode != "live":
        paper_secret = str(
            settings.account_drawdown_recovery_scope_secret
            or _paper_broker_scope_secret(
                settings=settings,
                provider=normalized_provider,
            )
        ).strip()
        scope_key = paper_secret.encode("utf-8") if paper_secret else None
    if scope_key is None:
        raise PositionIdentityConflictError(
            "authenticated broker account identity requires a stable scope secret"
        )

    account_material = f"broker-account-id\0{normalized_account_id}"
    message = (
        f"{POSITION_IDENTITY_VERSION}\0{normalized_provider}\0{account_material}"
    ).encode("utf-8")
    return PositionIdentity(
        provider=normalized_provider,
        account_scope=hmac.new(scope_key, message, sha256).hexdigest()[:24],
        exchange=normalized_exchange,
    )


def position_identity_from_payload(payload: object) -> PositionIdentity | None:
    if not isinstance(payload, dict) or POSITION_IDENTITY_KEY not in payload:
        return None
    raw_identity = payload.get(POSITION_IDENTITY_KEY)
    if not isinstance(raw_identity, dict) or set(raw_identity) != {
        "version",
        "provider",
        "account_scope",
        "exchange",
    }:
        raise PositionIdentityConflictError("stored position identity is malformed")
    if raw_identity.get("version") != POSITION_IDENTITY_VERSION:
        raise PositionIdentityConflictError(
            "stored position identity version is unsupported"
        )
    provider = raw_identity.get("provider")
    account_scope = raw_identity.get("account_scope")
    exchange = raw_identity.get("exchange")
    if (
        not isinstance(provider, str)
        or not provider
        or not isinstance(account_scope, str)
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
        or not isinstance(exchange, str)
        or not exchange
    ):
        raise PositionIdentityConflictError("stored position identity is invalid")
    return PositionIdentity(
        provider=provider.strip().lower(),
        account_scope=account_scope,
        exchange=exchange.strip().upper(),
    )


def position_identity_from_columns(position: object) -> PositionIdentity | None:
    provider = getattr(position, "provider", None)
    account_scope = getattr(position, "account_scope", None)
    exchange = getattr(position, "exchange", None)
    values = (provider, account_scope, exchange)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise PositionIdentityConflictError(
            "execution position route columns are only partially populated"
        )
    return position_identity_from_payload(
        {
            POSITION_IDENTITY_KEY: {
                "version": POSITION_IDENTITY_VERSION,
                "provider": provider,
                "account_scope": account_scope,
                "exchange": exchange,
            }
        }
    )


def payload_with_position_identity(
    payload: object,
    *,
    identity: PositionIdentity,
) -> dict[str, object]:
    result = dict(payload) if isinstance(payload, dict) else {}
    result[POSITION_IDENTITY_KEY] = identity.as_payload()
    return result


def execution_scope_payload(
    *,
    identity: PositionIdentity,
    venue: str,
    asset_class: str,
) -> dict[str, object]:
    provider = str(identity.provider or "").strip().lower()
    account_scope = str(identity.account_scope or "").strip()
    normalized_venue = str(venue or "").strip().upper()
    normalized_asset_class = str(asset_class or "").strip().lower()
    normalized_exchange = str(identity.exchange or "").strip().upper()
    if (
        not provider
        or len(account_scope) != 24
        or any(character not in "0123456789abcdef" for character in account_scope)
        or not normalized_venue
        or not normalized_asset_class
        or not normalized_exchange
    ):
        raise PositionIdentityConflictError(
            "execution scope requires an opaque account and complete order route"
        )
    return {
        "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
        "provider": provider,
        "account_scope": account_scope,
        "venue": normalized_venue,
        "asset_class": normalized_asset_class,
        "exchange": normalized_exchange,
    }


def stable_account_scope_hmac_key(settings: Settings) -> bytes | None:
    value = str(settings.account_drawdown_recovery_scope_secret or "").strip()
    if not value:
        return None
    encoded = value.encode("utf-8")
    if len(encoded) < ACCOUNT_SCOPE_HMAC_MIN_KEY_BYTES:
        return None
    return encoded


def _stable_account_material(
    *,
    settings: Settings,
    provider: str,
    broker_account_id: str | None,
) -> tuple[str, bytes | None]:
    normalized_broker_account_id = str(broker_account_id or "").strip()
    raw_account_material = (
        f"broker-account-id\0{normalized_broker_account_id}"
        if normalized_broker_account_id
        else None
    )

    if raw_account_material is None and provider == "kis":
        account_number = str(settings.kis_account_number or "").strip()
        product_code = str(settings.kis_account_product_code or "").strip()
        if account_number and product_code:
            raw_account_material = (
                f"configured-kis-account\0{account_number}\0{product_code}"
            )

    configured_scope = str(settings.execution_position_account_scope or "").strip()
    stable_scope_secret = str(
        settings.account_drawdown_recovery_scope_secret or ""
    ).strip()
    stable_scope_key = stable_account_scope_hmac_key(settings)
    if (
        stable_scope_key is None
        and stable_scope_secret
        and settings.runtime_mode != "live"
    ):
        stable_scope_key = stable_scope_secret.encode("utf-8")
    if raw_account_material is not None and (
        provider == "alpaca"
        or (provider == "kis" and stable_scope_key is not None)
    ):
        # Authenticated Alpaca truth and a keyed configured KIS account are
        # stronger than an operator label. The label remains a fallback only
        # when raw account material cannot be keyed safely; otherwise an A -> B
        # account switch could look unchanged.
        if stable_scope_key is not None:
            return raw_account_material, stable_scope_key
        if provider == "alpaca" and settings.runtime_mode != "live":
            paper_scope_secret = _paper_broker_scope_secret(
                settings=settings,
                provider=provider,
            )
            if paper_scope_secret:
                return raw_account_material, paper_scope_secret.encode("utf-8")
        raise PositionIdentityConflictError(
            "authenticated Alpaca account identity requires "
            "ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET of at least 32 bytes"
        )
    if configured_scope:
        # The operator-supplied opaque label is canonical only where no stronger
        # authenticated account identity is available.
        return f"configured-account-scope\0{configured_scope}", stable_scope_key
    if raw_account_material is not None and stable_scope_key is not None:
        return raw_account_material, stable_scope_key
    if raw_account_material is not None and settings.runtime_mode == "live":
        raise PositionIdentityConflictError(
            "live raw broker account identity requires "
            "ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET of at least 32 bytes or "
            "EXECUTION_POSITION_ACCOUNT_SCOPE"
        )
    if raw_account_material is not None:
        paper_scope_secret = _paper_broker_scope_secret(
            settings=settings,
            provider=provider,
        )
        if paper_scope_secret:
            return raw_account_material, paper_scope_secret.encode("utf-8")

    if settings.runtime_mode == "live":
        raise PositionIdentityConflictError(
            "live position reconciliation requires a stable broker account ID "
            "or EXECUTION_POSITION_ACCOUNT_SCOPE"
        )

    # Paper/mock adapters may not expose a real account object. Their position
    # truth is scoped to one provider and runtime mode, independent of credentials.
    return f"simulated-provider-route\0{settings.runtime_mode}", None


def _paper_broker_scope_secret(*, settings: Settings, provider: str) -> str:
    if provider == "kis":
        return str(settings.kis_app_secret or settings.kis_app_key or "").strip()
    if provider == "alpaca":
        return str(
            settings.alpaca_api_secret or settings.alpaca_api_key or ""
        ).strip()
    if provider == "upbit":
        return str(settings.upbit_secret_key or settings.upbit_access_key or "").strip()
    return ""
