from decimal import Decimal

from fastapi.testclient import TestClient

from app.broker.base import (
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerCurrentPrice,
)
from app.core.config import Settings
from app.models.runtime import ExecutionPlanItem, ExecutionPosition
from test_broker_auth import (
    KIS_PAPER_API_BASE_URL,
    StubBrokerAdapter,
    build_order_placement,
    build_test_app,
    build_token_grant,
    wait_for,
)


class SymbolScopedQuoteErrorAdapter(StubBrokerAdapter):
    def __init__(
        self,
        *,
        current_price_errors: dict[str, Exception],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.current_price_errors = current_price_errors

    def get_current_price(
        self,
        access_token: str,
        *,
        symbol: str,
        exchange: str = "KRX",
    ) -> BrokerCurrentPrice | None:
        symbol_error = self.current_price_errors.get(symbol)
        if symbol_error is not None:
            self.current_price_calls.append((symbol, exchange))
            raise symbol_error
        return super().get_current_price(access_token, symbol=symbol, exchange=exchange)


def test_autonomous_monitoring_keeps_partial_prices_when_one_live_quote_fails() -> None:
    broker_adapter = SymbolScopedQuoteErrorAdapter(
        config=BrokerConfig(
            provider="kis",
            mode="paper",
            configured=True,
            api_base_url=KIS_PAPER_API_BASE_URL,
        ),
        token_grant=build_token_grant(),
        order_placement=build_order_placement(broker_order_id="kis-auto-monitor-partial"),
        current_prices={"005930": Decimal("125100.00")},
        current_price_errors={
            "000660": BrokerAdapterError(
                "EGW00201",
                status_code=429,
                classification=BrokerErrorClassification.RATE_LIMIT,
                retryable=True,
            )
        },
    )
    app, engine = build_test_app(
        settings=Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="paper-key",
            kis_app_secret="paper-secret",
            kis_account_number="12345678",
            dispatch_loop_interval_seconds=60,
            monitoring_loop_interval_seconds=0.05,
        ),
        broker_adapter=broker_adapter,
        autonomous_runtime_enabled=True,
    )
    with TestClient(app) as client:
        resume = client.post(
            "/controls/resume",
            json={"actor": "ops", "source": "console", "reason": "enable autonomous runtime"},
        )
        assert resume.status_code == 200

        plan_response = client.post(
            "/execution-plans",
            json={
                "plan_id": "plan-autonomous-monitoring-partial-quotes",
                "source": "kai-trading-core",
                "items": [
                    {
                        "symbol": "005930",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "130000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    },
                    {
                        "symbol": "000660",
                        "side": "sell",
                        "intent": "target_sell",
                        "quantity_policy": {"kind": "shares", "quantity": 1},
                        "trigger_price": "90000.00",
                        "time_in_force": "day",
                        "risk_context": {"exchange": "KRX"},
                    },
                ],
            },
        )
        assert plan_response.status_code == 201
        first_item_id, second_item_id = plan_response.json()["persisted_execution_item_ids"]

        with app.state.db_session_factory() as session:
            session.add_all(
                [
                    ExecutionPosition(
                        mode="paper",
                        symbol="005930",
                        quantity=1,
                        average_price=Decimal("120000.0000"),
                        updated_from="reconciliation",
                        raw_payload={"source": "broker"},
                    ),
                    ExecutionPosition(
                        mode="paper",
                        symbol="000660",
                        quantity=1,
                        average_price=Decimal("88000.0000"),
                        updated_from="reconciliation",
                        raw_payload={"source": "broker"},
                    ),
                ]
            )
            session.commit()

        wait_for(
            lambda: (
                client.get("/runtime/status")
                .json()["liveness"]["autonomous"]["monitoring"]["price_source"]["last_price_count"]
                == 1
            )
        )

        runtime_status = client.get("/runtime/status")
        assert runtime_status.status_code == 200
        autonomous = runtime_status.json()["liveness"]["autonomous"]["monitoring"]
        price_source = autonomous["price_source"]
        diagnostics = price_source["diagnostics"]

        assert autonomous["last_cycle"]["price_count"] == 1
        assert autonomous["last_cycle"]["evaluated_item_count"] == 1
        assert price_source["status"] == "degraded"
        assert price_source["last_symbol_count"] == 2
        assert price_source["last_price_count"] == 1
        assert diagnostics["requested_symbol_count"] == 2
        assert diagnostics["priced_symbol_count"] == 1
        assert diagnostics["missing_symbol_count"] == 1
        assert diagnostics["missing_symbols"] == ["000660"]
        assert diagnostics["quote_failure_count"] == 1
        assert diagnostics["quote_failures"] == [{"symbol": "000660", "error": "EGW00201"}]
        assert ("005930", "KRX") in broker_adapter.current_price_calls
        assert ("000660", "KRX") in broker_adapter.current_price_calls
        assert broker_adapter.order_calls == []

        with app.state.db_session_factory() as session:
            first_item = session.get(ExecutionPlanItem, first_item_id)
            second_item = session.get(ExecutionPlanItem, second_item_id)
            assert first_item is not None
            assert second_item is not None
            assert first_item.status == "target_sell_monitoring"
            assert second_item.status == "target_sell_monitoring"

    engine.dispose()
