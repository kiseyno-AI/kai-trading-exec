from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.schemas.execution import ExecutionPlanRequest
from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.kis import KisBrokerAdapter
from app.broker.upbit import UpbitBrokerAdapter
from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import create_app
from app.services.execution_api import _build_execution_plan


ROOT = Path(__file__).parents[1]


def test_runtime_broker_provider_is_normalized_and_allowlisted() -> None:
    assert load_settings(
        {"KAI_EXEC_RUNTIME_BROKER_PROVIDER": "  ALPACA  "}
    ).runtime_broker_provider == "alpaca"

    with pytest.raises(ValidationError, match="runtime_broker_provider"):
        load_settings({"KAI_EXEC_RUNTIME_BROKER_PROVIDER": "unknown"})


@pytest.mark.parametrize("provider", ["alpaca", "upbit"])
def test_kis_only_autonomous_coordinator_rejects_non_kis_provider(
    provider: str,
) -> None:
    with pytest.raises(
        ValidationError,
        match="autonomous runtime currently supports only the KIS provider",
    ):
        Settings(
            runtime_broker_provider=provider,
            autonomous_runtime_enabled=True,
        )


@pytest.mark.parametrize("runtime_provider", ["alpaca", "upbit"])
def test_omitted_plan_provider_keeps_legacy_kis_authority(
    runtime_provider: str,
) -> None:
    plan = _build_execution_plan(
        settings=Settings(
            runtime_mode="paper",
            runtime_broker_provider=runtime_provider,
        ),
        command=ExecutionPlanRequest(
            plan_id=f"implicit-kis-{runtime_provider}",
            source="legacy-krx-client",
            items=[
                {
                    "symbol": "005930",
                    "side": "sell",
                    "intent": "stop_sell",
                    "quantity_policy": {"kind": "shares", "quantity": 1},
                    "trigger_price": "69000",
                    "risk_context": {"exchange": "KRX"},
                }
            ],
        ),
    )

    assert plan.items[0].provider == "kis"


@pytest.mark.parametrize(
    (
        "provider",
        "adapter_type",
        "expected_exchanges",
        "expected_status",
        "expected_source",
        "expected_reason",
    ),
    [
        (
            "kis",
            KisBrokerAdapter,
            ["AMEX", "KOSDAQ", "KOSPI", "KRX", "NASD", "NXT", "NYSE", "SOR"],
            "available",
            "configured_account_scope",
            None,
        ),
        (
            "alpaca",
            AlpacaBrokerAdapter,
            ["ALPACA", "US"],
            "unavailable",
            None,
            "authenticated_account_identity_unavailable",
        ),
        (
            "upbit",
            UpbitBrokerAdapter,
            ["UPBIT"],
            "available",
            "configured_account_scope",
            None,
        ),
    ],
)
def test_default_app_factory_drives_status_and_daily_board_provider(
    provider: str,
    adapter_type: type[object],
    expected_exchanges: list[str],
    expected_status: str,
    expected_source: str | None,
    expected_reason: str | None,
) -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
        runtime_broker_provider=provider,
        execution_position_account_scope=f"{provider}-runtime-test-account",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
    )

    adapter = app.state.broker_adapter_factory(settings)
    assert isinstance(adapter, adapter_type)
    assert adapter.provider == provider

    with TestClient(app) as client:
        status_response = client.get("/runtime/status")
        board_response = client.get("/runtime/daily-board")

    assert status_response.status_code == 200
    assert board_response.status_code == 200
    for payload in (status_response.json(), board_response.json()["runtime"]):
        assert payload["execution_scope"] == {
            "provider": provider,
            "status": expected_status,
            "source": expected_source,
            "reason": expected_reason,
            "exchanges": expected_exchanges,
        }
        if provider != "kis":
            assert payload["account_drawdown_gate"] == {
                "available": False,
                "enabled": settings.account_drawdown_gate_enabled,
                "gated": None,
                "provider": provider,
                "latest_decision": None,
                "active_recovery_scopes": {},
                "reason": "provider_scoped_gate_state_unavailable",
            }
    if provider != "kis":
        assert board_response.json()["account_drawdown_gate"] == {
            "available": False,
            "enabled": settings.account_drawdown_gate_enabled,
            "gated": None,
            "provider": provider,
            "latest_decision": None,
            "active_recovery_scopes": {},
            "reason": "provider_scoped_gate_state_unavailable",
        }
    engine.dispose()


@pytest.mark.parametrize(
    ("launcher", "provider"),
    [
        ("start-api.sh", "kis"),
        ("start-api-no-migrate.sh", "kis"),
        ("start-kis-us-paper.sh", "kis"),
        ("start-alpaca-paper.sh", "alpaca"),
    ],
)
def test_launchers_force_runtime_provider_after_environment_files(
    launcher: str,
    provider: str,
) -> None:
    script = (ROOT / launcher).read_text()
    forced_export = f'export KAI_EXEC_RUNTIME_BROKER_PROVIDER="{provider}"'

    assert script.count(forced_export) == 1
    assert script.index(forced_export) > script.rindex("source ./.env")
    if "source ./.env.paper" in script:
        assert script.index(forced_export) > script.rindex("source ./.env.paper")
