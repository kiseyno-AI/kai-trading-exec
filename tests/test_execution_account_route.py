from __future__ import annotations

from copy import deepcopy
import hmac
from hashlib import sha256

import pytest

from app.core.config import Settings
from app.services.execution_account_route import (
    AccountRouteBindingError,
    bind_execution_account_scope,
    bind_kis_account_route,
    build_execution_account_scope_binding,
    classify_kis_account_route,
    configured_kis_route_fingerprint,
    redact_account_references,
    sanitize_account_payload,
    validate_execution_account_scope,
    validate_kis_account_route,
)
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    build_position_identity,
)


def _settings() -> Settings:
    return Settings(
        runtime_mode="paper",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="stable-route-secret",
    )


def test_redaction_removes_case_insensitive_account_aliases_recursively() -> None:
    payload = {
        "Account_ID": "raw-account",
        "nested": [
            {
                "BROKER_ACCOUNT_ID": "broker-raw",
                "account_no": "12345678",
                "AcCt_Id": "acct-id",
                "ACCT_NO": "acct-no",
                "account_scope": "a" * 24,
                "marker": "preserved",
            }
        ],
    }

    redacted = sanitize_account_payload(payload)

    assert redacted == {
        "nested": [
            {
                "account_scope": "a" * 24,
                "marker": "preserved",
            }
        ]
    }


def test_redaction_removes_nested_credential_keys_without_losing_opaque_scope() -> None:
    payload = {
        "Authorization": "Bearer authorization-secret",
        "Set-Cookie": "JSESSIONID=set-cookie-secret",
        "Cookie-Header": "session=cookie-header-secret",
        "X-Session-ID": "session-id-secret",
        "provenance": {"session_id": "agent-session-id"},
        "auth": "basic-auth-secret",
        "authentication": {"method": "bearer", "value": "auth-secret"},
        "credentials": {"username": "operator", "password": "credential-secret"},
        "cookie": "session=cookie-secret",
        "session_token": "session-token-secret",
        "session_secret": "session-secret",
        "bearer": "bare-bearer-secret",
        "secret": "generic-secret",
        "nested": [
            {
                "access_token": "access-token-secret",
                "refresh_token": "refresh-token-secret",
                "appkey": "application-key-secret",
                "appsecret": "application-secret",
                "api_key": "api-key-secret",
                "api_secret": "api-secret",
                "HASH": "kis-hashkey-secret",
                "hashkey": "kis-hashkey-secret-2",
                "token": "generic-token-secret",
                "kis_app_secret": "kis-secret",
                "alpaca_api_secret": "alpaca-secret",
                "upbit_access_key": "upbit-access",
                "upbit_secret_key": "upbit-secret",
                "APCA-API-KEY-ID": "alpaca-key-id",
                "oauth_client_id": "oauth-client",
                "provider_consumer_key": "consumer-key",
                "provider_id_token": "identity-token",
                "provider_account_id": "provider-account",
                "account_number": "87654321",
                "account_scope": "a" * 24,
                "hash": "safe-content-hash",
                "market_data": {"symbol": "005930", "price": "71000"},
            }
        ],
        "tuple_payload": (
            {"access_token": "tuple-secret", "safe": "kept"},
        ),
    }

    redacted = redact_account_references(payload)

    assert redacted == {
        "provenance": {"session_id": "agent-session-id"},
        "nested": [
            {
                "account_scope": "a" * 24,
                "hash": "safe-content-hash",
                "market_data": {"symbol": "005930", "price": "71000"},
            }
        ],
        "tuple_payload": ({"safe": "kept"},),
    }


@pytest.mark.parametrize(
    "malformed_binding",
    [
        "not-an-object",
        {},
        {"version": "unknown", "configured_route_fingerprint": "0" * 32},
        {"version": "kis-account-route-v1"},
        {
            "version": "kis-account-route-v1",
            "configured_route_fingerprint": "0" * 32,
            "unexpected": True,
        },
    ],
)
def test_any_malformed_route_binding_fails_even_with_valid_sibling(
    malformed_binding: object,
) -> None:
    settings = _settings()
    valid_risk, _policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={},
        quantity_policy={},
    )

    mismatch = validate_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context=valid_risk,
        quantity_policy={"execution_account_route": malformed_binding},
        require_binding=True,
    )

    assert mismatch is not None
    assert mismatch.reason_code == "account_route_binding_invalid"


def test_conflicting_valid_route_bindings_fail_as_route_conflict() -> None:
    settings = _settings()
    valid_risk, _policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={},
        quantity_policy={},
    )
    conflicting_policy = deepcopy(valid_risk)
    conflicting_policy["execution_account_route"]["configured_route_fingerprint"] = (
        "f" * 32
    )

    mismatch = validate_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context=valid_risk,
        quantity_policy=conflicting_policy,
        require_binding=True,
    )

    assert mismatch is not None
    assert mismatch.reason_code == "account_route_conflict"


def test_binding_refuses_to_rebind_an_existing_foreign_account_route() -> None:
    original_settings = _settings()
    rotated_settings = original_settings.model_copy(
        update={"kis_account_number": "87654321"}
    )
    original_risk, original_policy = bind_kis_account_route(
        settings=original_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "exec_agent_sized"},
    )

    with pytest.raises(AccountRouteBindingError) as raised:
        bind_kis_account_route(
            settings=rotated_settings,
            provider="kis",
            risk_context=original_risk,
            quantity_policy=original_policy,
        )

    assert raised.value.reason_code == "account_route_mismatch"
    assert original_risk is not None
    assert original_risk["execution_account_route"][
        "configured_route_fingerprint"
    ] == configured_kis_route_fingerprint(original_settings)


def test_keyed_kis_route_fingerprint_prefers_account_over_operator_label() -> None:
    shared = {
        "runtime_mode": "live",
        "kis_account_product_code": "01",
        "account_drawdown_recovery_scope_secret": (
            "stable-kis-route-scope-secret-at-least-32-bytes"
        ),
        "execution_position_account_scope": "primary-kis-label",
    }
    account_a = Settings(**shared, kis_account_number="11111111")
    account_b = Settings(**shared, kis_account_number="22222222")
    relabeled_a = account_a.model_copy(
        update={"execution_position_account_scope": "renamed-kis-label"}
    )

    assert configured_kis_route_fingerprint(account_a) != (
        configured_kis_route_fingerprint(account_b)
    )
    assert configured_kis_route_fingerprint(account_a) == (
        configured_kis_route_fingerprint(relabeled_a)
    )


def test_only_an_opaque_valid_binding_can_prove_a_foreign_route() -> None:
    settings = _settings()
    foreign_settings = settings.model_copy(update={"kis_account_number": "87654321"})
    foreign_risk, foreign_policy = bind_kis_account_route(
        settings=foreign_settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    assert classify_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context=foreign_risk,
        quantity_policy=foreign_policy,
    ) == "foreign"
    assert classify_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"account_reference": "KIS:87654321-01"},
        quantity_policy=None,
    ) == "indeterminate"


def test_opaque_execution_scope_is_sufficient_route_binding_authority() -> None:
    settings = Settings(
        runtime_mode="paper",
        execution_position_account_scope="opaque-current-account",
    )

    risk_context, quantity_policy = bind_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={"exchange": "KRX"},
        quantity_policy={"kind": "shares", "quantity": 1},
    )

    assert configured_kis_route_fingerprint(settings) is not None
    assert (
        validate_kis_account_route(
            settings=settings,
            provider="kis",
            risk_context=risk_context,
            quantity_policy=quantity_policy,
            require_binding=True,
        )
        is None
    )


def test_upbit_buy_scope_uses_configured_opaque_authority_and_detects_rotation() -> None:
    secret = "upbit-account-authority-secret-at-least-32-bytes"
    admitted_settings = Settings(
        runtime_mode="live",
        execution_position_account_scope="opaque-upbit-account-a",
        account_drawdown_recovery_scope_secret=secret,
    )
    admitted_binding = build_execution_account_scope_binding(
        settings=admitted_settings,
        provider="upbit",
        exchange="UPBIT",
    )
    risk_context, quantity_policy = bind_execution_account_scope(
        provider="upbit",
        risk_context={"exchange": "UPBIT"},
        quantity_policy={"kind": "quote_notional", "amount": "10000"},
        binding=admitted_binding,
    )

    expected_scope = hmac.new(
        secret.encode("utf-8"),
        (
            "execution-position-route-v2\0upbit\0"
            "configured-account-scope\0opaque-upbit-account-a"
        ).encode("utf-8"),
        sha256,
    ).hexdigest()[:24]
    assert admitted_binding == {
        "policy_version": 1,
        "provider": "upbit",
        "account_scope": expected_scope,
    }

    rotated_binding = build_execution_account_scope_binding(
        settings=admitted_settings.model_copy(
            update={"execution_position_account_scope": "opaque-upbit-account-b"}
        ),
        provider="upbit",
        exchange="UPBIT",
    )
    mismatch = validate_execution_account_scope(
        provider="upbit",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
        current_binding=rotated_binding,
        require_binding=True,
    )

    assert mismatch is not None
    assert mismatch.reason_code == "execution_account_scope_mismatch"


def test_upbit_buy_scope_rejects_simulated_provider_fallback() -> None:
    with pytest.raises(PositionIdentityConflictError):
        build_execution_account_scope_binding(
            settings=Settings(
                runtime_mode="paper",
                upbit_access_key="paper-upbit-access",
                upbit_secret_key="paper-upbit-secret",
            ),
            provider="upbit",
            exchange="UPBIT",
        )


def test_alpaca_authenticated_account_scope_overrides_operator_label() -> None:
    settings = Settings(
        runtime_mode="paper",
        execution_position_account_scope="operator-display-label",
        account_drawdown_recovery_scope_secret=(
            "alpaca-account-authority-secret-at-least-32-bytes"
        ),
    )

    authenticated = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="authenticated-account-a",
    )
    reconciled = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
        broker_account_id="authenticated-account-a",
    )
    label_only = build_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="ALPACA",
    )

    assert reconciled == authenticated
    assert reconciled.account_scope != label_only.account_scope

def test_live_route_fingerprint_rejects_a_short_hmac_key() -> None:
    settings = Settings(
        runtime_mode="live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        account_drawdown_recovery_scope_secret="too-short",
    )

    assert configured_kis_route_fingerprint(settings) is None
    mismatch = validate_kis_account_route(
        settings=settings,
        provider="kis",
        risk_context={},
        quantity_policy={},
        require_binding=True,
    )
    assert mismatch is not None
    assert mismatch.reason_code == "account_route_binding_unavailable"
