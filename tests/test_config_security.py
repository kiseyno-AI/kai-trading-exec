from __future__ import annotations

from app.core.config import Settings


def test_default_exec_agent_model_matches_the_deployed_runtime_identity() -> None:
    assert Settings().exec_agent_model == "openai/gpt-5.6-sol"


def test_settings_repr_omits_credentials_and_account_identifiers() -> None:
    secrets = {
        "database_url": "postgresql://operator:database-marker@localhost/runtime",
        "api_bearer_token": "api-bearer-marker",
        "account_drawdown_recovery_scope_secret": "scope-secret-marker",
        "execution_position_account_scope": "position-scope-marker",
        "market_context_bearer_token": "market-bearer-marker",
        "kis_app_key": "kis-app-key-marker",
        "kis_app_secret": "kis-app-secret-marker",
        "kis_account_number": "kis-account-number-marker",
        "kis_us_account_allowlist": "kis-us-account-allowlist-marker",
        "kis_hts_id": "kis-hts-id-marker",
        "kis_us_live_confirmation_token": "kis-confirmation-marker",
        "upbit_access_key": "upbit-access-marker",
        "upbit_secret_key": "upbit-secret-marker",
        "alpaca_api_key": "alpaca-key-marker",
        "alpaca_api_secret": "alpaca-secret-marker",
    }

    rendered = repr(Settings(**secrets))

    for marker in secrets.values():
        assert marker not in rendered
