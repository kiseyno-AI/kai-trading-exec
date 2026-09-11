from app.broker.error_safety import (
    safe_broker_error_code,
    sanitize_broker_error_payload,
)


def test_known_internal_error_code_remains_actionable() -> None:
    assert safe_broker_error_code("execution_scope_unavailable") == (
        "execution_scope_unavailable"
    )
    assert sanitize_broker_error_payload(
        {"code": "execution_scope_unavailable"}
    ) == {"code": "execution_scope_unavailable"}
    assert sanitize_broker_error_payload(
        {
            "code": "execution_account_scope_mismatch",
            "classification": "account_authority",
        }
    ) == {
        "code": "execution_account_scope_mismatch",
        "classification": "account_authority",
    }


def test_arbitrary_lowercase_error_code_is_digest_only() -> None:
    secret_like_value = "account-secret-marker"

    direct = safe_broker_error_code(secret_like_value)
    nested = sanitize_broker_error_payload({"code": secret_like_value})

    assert direct is not None
    assert direct.startswith("broker_error_code_redacted[sha256=")
    assert nested["code"].startswith("broker_error_details_redacted[sha256=")
    assert secret_like_value not in direct
    assert secret_like_value not in str(nested)


def test_wrapped_sensitive_field_names_are_removed_recursively() -> None:
    sanitized = sanitize_broker_error_payload(
        {
            "renamed_authorization_value": "Bearer secret-marker",
            "headers": {"x-session": "secret-marker"},
            "request_headers": {"x-session": "secret-marker"},
            "responseHeaders": {"x-session": "secret-marker"},
            "nested": {
                "client_secret_value": "secret-marker",
                "status": "rejected",
            },
            "market_value": "50.25",
        }
    )

    assert sanitized["nested"] == {"status": "rejected"}
    assert "market_value" in sanitized
    assert "headers" not in sanitized
    assert "request_headers" not in sanitized
    assert "responseHeaders" not in sanitized
