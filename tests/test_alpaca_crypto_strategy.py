from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from threading import Barrier, Lock

import pytest

import app.services.alpaca_crypto_strategy as strategy_module
from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountOpenOrder,
    BrokerAccountSnapshot,
    BrokerLiveMarketContext,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    CryptoDailyBar,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
    ExecutionFill,
)
from app.services.alpaca_crypto_strategy import (
    AlpacaCryptoStrategyService,
    _asset_increments,
    _daily_rebalance_order_key,
    _latest_closed_bar_is_fresh,
    _order_key,
    _protective_exit_order_key,
    _quote_health,
)
from app.services.broker_orders import PlaceBrokerOrderResult
from app.services.crypto_trend_strategy import TrendFeatures
from app.services.position_identity import (
    build_authenticated_broker_position_identity,
    execution_scope_payload,
)


class FakeStrategyAdapter:
    def __init__(
        self,
        *,
        observed_at: datetime,
        holdings: list[BrokerAccountHolding] | None = None,
        open_orders: list[BrokerAccountOpenOrder] | None = None,
        latest_bar_age_days: int = 1,
        snapshot_payload: dict[str, object] | None = None,
        broker_account_id: str = "account-a",
        total_asset_value: Decimal = Decimal("100000"),
        previous_total_asset_value: Decimal | None = Decimal("100000"),
        holdings_complete: bool = True,
        open_orders_complete: bool = True,
    ) -> None:
        self.observed_at = observed_at
        self.holdings = holdings or []
        self.open_orders = open_orders or []
        self.latest_bar_age_days = latest_bar_age_days
        self.snapshot_payload = snapshot_payload or {"open_orders_coverage": "complete"}
        self.broker_account_id = broker_account_id
        self.total_asset_value = total_asset_value
        self.previous_total_asset_value = previous_total_asset_value
        self.holdings_complete = holdings_complete
        self.open_orders_complete = open_orders_complete

    def get_account_snapshot(self, *_args, **_kwargs) -> BrokerAccountSnapshot:  # noqa: ANN002, ANN003
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            buying_power=Decimal("1000"),
            total_asset_value=self.total_asset_value,
            previous_total_asset_value=self.previous_total_asset_value,
            observed_at=self.observed_at,
            source="test",
            holdings=self.holdings,
            open_orders=self.open_orders,
            payload=self.snapshot_payload,
            broker_account_id=self.broker_account_id,
            holdings_complete=self.holdings_complete,
            open_orders_complete=self.open_orders_complete,
        )

    def get_crypto_daily_bars(self, *_args, symbols: list[str], **_kwargs):  # noqa: ANN002, ANN003, ANN201
        latest_date = self.observed_at.date() - timedelta(
            days=self.latest_bar_age_days
        )
        start = latest_date - timedelta(days=229)
        return {
            symbol: [
                {
                    "t": datetime.combine(start + timedelta(days=offset), datetime.min.time(), tzinfo=UTC).isoformat(),
                    "o": str(Decimal("100") + offset - Decimal("0.5")),
                    "h": str(Decimal("100") + offset + Decimal("1")),
                    "l": str(Decimal("100") + offset - Decimal("1")),
                    "c": str(Decimal("100") + offset + Decimal(offset % 3) / Decimal("10")),
                    "v": "10",
                }
                for offset in range(230)
            ]
            for symbol in symbols
        }

    def list_active_crypto_assets(self) -> list[dict[str, object]]:
        return [
            {"symbol": "BTC/USD", "min_trade_increment": "0.00000001"},
            {"symbol": "ETH/USD", "min_trade_increment": "0.0001"},
        ]


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "runtime_mode": "paper",
        "database_url": "sqlite:///:memory:",
        "account_drawdown_recovery_scope_secret": "s" * 32,
        "alpaca_crypto_strategy_enabled": True,
        "alpaca_crypto_allowed_symbols": "BTC/USD,ETH/USD",
        "alpaca_crypto_strategy_decision_hour_utc": 0,
        "alpaca_crypto_strategy_decision_minute_utc": 10,
        "alpaca_crypto_strategy_sleeve_usd": Decimal("1000"),
        "alpaca_crypto_max_order_notional_usd": Decimal("100"),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _execution_scopes(
    settings: Settings,
    *,
    broker_account_id: str = "account-a",
    asset_class: str = "crypto",
    exchange: str = "ALPACA",
) -> tuple[dict[str, object], dict[str, object]]:
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange=exchange,
        broker_account_id=broker_account_id,
    )
    route_scope = execution_scope_payload(
        identity=identity,
        venue="alpaca",
        asset_class=asset_class,
    )
    coordinator_scope = {
        key: route_scope[key]
        for key in ("policy_version", "provider", "account_scope")
    }
    return coordinator_scope, route_scope


def _quotes(observed_at: datetime) -> dict[str, BrokerLiveMarketContext]:
    return {
        "BTC/USD": BrokerLiveMarketContext(
            symbol="BTC/USD",
            exchange="ALPACA",
            observed_at=observed_at,
            bid=Decimal("329.9"),
            ask=Decimal("330.1"),
            spread=Decimal("0.2"),
            spread_bps=Decimal("6.06"),
            freshness_source="alpaca_exchange_quote_time",
        ),
        "ETH/USD": BrokerLiveMarketContext(
            symbol="ETH/USD",
            exchange="ALPACA",
            observed_at=observed_at,
            bid=Decimal("329.8"),
            ask=Decimal("330.2"),
            spread=Decimal("0.4"),
            spread_bps=Decimal("12.12"),
            freshness_source="alpaca_exchange_quote_time",
        ),
    }


def _seed_protected_strategy_position(
    *,
    session_factory,
    settings: Settings,
    observed_at: datetime,
    base_quantity: Decimal = Decimal("0.25"),
) -> tuple[dict[str, object], dict[str, object]]:
    coordinator_scope, route_scope = _execution_scopes(settings)
    with session_factory() as db:
        state = CryptoStrategyState(
            mode=settings.runtime_mode,
            provider="alpaca",
            account_scope=str(coordinator_scope["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            strategy_owned_base_quantity=base_quantity,
            high_water_price=Decimal("120"),
            protective_stop_price=Decimal("100"),
            last_action="buy",
            last_action_at=observed_at - timedelta(minutes=10),
            metrics={"quantity_increment": "0.00000001"},
        )
        buy = BrokerOrder(
            broker_name="alpaca",
            mode=settings.runtime_mode,
            idempotency_key="seed-protected-strategy-buy",
            broker_order_id="seed-protected-strategy-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=base_quantity,
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=10),
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                },
            },
        )
        risk = CryptoStrategyRiskState(
            mode=settings.runtime_mode,
            provider="alpaca",
            account_scope=str(coordinator_scope["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            daily_baseline_equity=Decimal("100000"),
            daily_baseline_at=observed_at,
            weekly_baseline_equity=Decimal("100000"),
            weekly_baseline_at=datetime.combine(
                observed_at.date() - timedelta(days=observed_at.weekday()),
                datetime.min.time(),
                tzinfo=UTC,
            ),
            peak_equity=Decimal("100000"),
            peak_equity_at=observed_at,
            current_equity=Decimal("100000"),
            current_equity_at=observed_at,
            risk_status="healthy",
            metrics={"weekly_baseline_source": "durable_test_history"},
        )
        db.add_all([state, buy, risk])
        db.flush()
        db.add(
            ExecutionFill(
                broker_order_pk=buy.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=base_quantity,
                quote_notional=base_quantity * Decimal("100"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=9),
            )
        )
        db.commit()
    return coordinator_scope, route_scope


def test_daily_cycle_persists_signals_stops_and_submits_once_per_closed_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 17, 0, 20, tzinfo=UTC)
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(settings=settings, session_factory=session_factory)
    submitted: list[str] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command.idempotency_key)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "message": "paper order accepted",
                "order": {"id": f"local-{len(submitted)}", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    adapter = FakeStrategyAdapter(observed_at=observed_at)

    first = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )
    second = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at + timedelta(seconds=15)),
        observed_at=observed_at + timedelta(seconds=15),
        execution_scope=_execution_scopes(settings)[0],
    )

    assert first["daily_decision"]["status"] == "processed"  # type: ignore[index]
    assert second["daily_decision"]["status"] == "already_processed"  # type: ignore[index]
    assert len(submitted) == 2
    assert len(set(submitted)) == 2
    with session_factory() as db:
        assert db.query(CryptoStrategyDecision).count() == 2
        assert db.query(CryptoDailyBar).count() == 460
        states = db.query(CryptoStrategyState).all()
        assert len(states) == 2
        assert all(state.eligible for state in states)
        assert all(state.high_water_price is not None for state in states)
        assert all(state.protective_stop_price is not None for state in states)


def test_quote_health_fails_closed_for_stale_or_wide_quotes() -> None:
    observed_at = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    health = _quote_health(
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=datetime(2026, 8, 16, 0, 58),
                bid=Decimal("100"),
                ask=Decimal("102"),
                spread=Decimal("2"),
                spread_bps=Decimal("198.02"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        symbols=("BTC/USD",),
        observed_at=observed_at,
        max_age_seconds=60,
        max_spread_bps=Decimal("100"),
    )

    assert health["blocked_symbols"] == ["BTC/USD"]
    assert health["symbols"]["BTC/USD"]["reasons"] == [  # type: ignore[index]
        "stale_quote",
        "spread_too_wide",
    ]


@pytest.mark.parametrize(
    ("freshness_source", "time_offset", "expected_reason"),
    [
        ("transport_received_at_untrusted", timedelta(0), "untrusted_quote_timestamp"),
        ("alpaca_exchange_quote_time", timedelta(minutes=5), "future_quote_timestamp"),
    ],
)
def test_quote_health_requires_exchange_provenance_and_rejects_future_time(
    freshness_source: str,
    time_offset: timedelta,
    expected_reason: str,
) -> None:
    observed_at = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    quote = BrokerLiveMarketContext(
        symbol="BTC/USD",
        exchange="ALPACA",
        observed_at=observed_at + time_offset,
        bid=Decimal("100"),
        ask=Decimal("101"),
        spread=Decimal("1"),
        spread_bps=Decimal("99.5"),
        freshness_source=freshness_source,
    )

    health = _quote_health(
        quotes={"BTC/USD": quote},
        symbols=("BTC/USD",),
        observed_at=observed_at,
        max_age_seconds=60,
        max_spread_bps=Decimal("100"),
    )

    assert health["blocked_symbols"] == ["BTC/USD"]
    assert expected_reason in health["symbols"]["BTC/USD"]["reasons"]  # type: ignore[index]
    if expected_reason == "future_quote_timestamp":
        assert health["symbols"]["BTC/USD"]["age_seconds"] == "-300.0"  # type: ignore[index]


def test_quote_health_allows_small_exchange_clock_skew() -> None:
    observed_at = datetime(2026, 8, 16, 1, 0, tzinfo=UTC)
    quote = BrokerLiveMarketContext(
        symbol="BTC/USD",
        exchange="ALPACA",
        observed_at=observed_at + timedelta(seconds=3),
        bid=Decimal("100"),
        ask=Decimal("101"),
        spread=Decimal("1"),
        spread_bps=Decimal("99.5"),
        freshness_source="alpaca_exchange_quote_time",
    )

    health = _quote_health(
        quotes={"BTC/USD": quote},
        symbols=("BTC/USD",),
        observed_at=observed_at,
        max_age_seconds=60,
        max_spread_bps=Decimal("100"),
    )

    assert health["blocked_symbols"] == []
    assert health["symbols"]["BTC/USD"]["age_seconds"] == "-3.0"  # type: ignore[index]


@pytest.mark.parametrize(
    ("freshness_source", "time_offset"),
    [
        ("transport_received_at_untrusted", timedelta(0)),
        ("alpaca_exchange_quote_time", timedelta(minutes=5)),
    ],
)
def test_untrusted_or_future_quote_never_reaches_entry_order(
    monkeypatch: pytest.MonkeyPatch,
    freshness_source: str,
    time_offset: timedelta,
) -> None:
    observed_at = datetime(2026, 8, 17, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted: list[str] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command.idempotency_key)
        return PlaceBrokerOrderResult(status_code=201, payload={"duplicate": False})

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    quote = BrokerLiveMarketContext(
        symbol="BTC/USD",
        exchange="ALPACA",
        observed_at=observed_at + time_offset,
        bid=Decimal("329.9"),
        ask=Decimal("330.1"),
        spread=Decimal("0.2"),
        spread_bps=Decimal("6.06"),
        freshness_source=freshness_source,
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(observed_at=observed_at),  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": quote},
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result["status"] == "degraded"
    assert "quotes_not_actionable" in result["reasons"]
    assert submitted == []


def test_live_strategy_requires_separate_live_gate() -> None:
    observed_at = datetime(2026, 8, 16, 0, 20, tzinfo=UTC)
    settings = _settings(runtime_mode="live", alpaca_crypto_strategy_live_enabled=False)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=create_session_factory(engine),
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(observed_at=observed_at),  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result == {
        "enabled": True,
        "status": "blocked",
        "reason": "alpaca_crypto_strategy_live_enabled_is_false",
    }


def test_protective_exit_dedupes_strategy_fills_and_preserves_manual_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(settings=settings, session_factory=session_factory)
    submitted: list[tuple[Decimal, dict[str, object], str]] = []

    with session_factory() as db:
        state = CryptoStrategyState(
            mode="paper",
            provider="alpaca",
            account_scope=str(_execution_scopes(settings)[0]["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            high_water_price=Decimal("120"),
            protective_stop_price=Decimal("100"),
            metrics={"quantity_increment": "0.00000001"},
        )
        strategy_order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="strategy-buy",
            broker_order_id="strategy-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": _execution_scopes(settings)[1],
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": _execution_scopes(settings)[1],
                },
            },
        )
        manual_order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="manual-buy",
            broker_order_id="manual-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.75"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "post:/orders",
                "exchange": "ALPACA",
                "execution_scope": _execution_scopes(settings)[1],
            },
        )
        db.add_all([state, strategy_order, manual_order])
        db.flush()
        db.add_all([
            ExecutionFill(
                broker_order_pk=strategy_order.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.25"),
                quote_notional=Decimal("25"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=5),
            ),
            ExecutionFill(
                broker_order_pk=strategy_order.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.25"),
                quote_notional=Decimal("25"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=5),
            ),
            ExecutionFill(
                broker_order_pk=manual_order.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.75"),
                quote_notional=Decimal("75"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=4),
            ),
        ])
        db.commit()

    def fake_place_broker_order(*, command, request_source, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append((command.base_quantity, command.metadata, request_source))
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "message": "paper order accepted", "order": {"status": "submitted"}},
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "1.0"},
            )
        ],
    )

    result = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result["protective_actions"][0]["quantity"] == "0.25000000"  # type: ignore[index]
    assert submitted == [
        (
            Decimal("0.25000000"),
                {
                    "strategy_version": "crypto_trend_v1",
                    "strategy_family": "crypto:ALPACA",
                    "order_origin": "alpaca_crypto_strategy",
                    "expected_execution_scope": _execution_scopes(settings)[1],
                    "autonomous_strategy_order": True,
                "decision_type": "protective_exit",
                "protective_stop_price": "100.000000000000",
                "trigger_bid": "99",
                    "risk_reduction": True,
                    "strategy_owned_base_quantity": "0.250000000000",
                    "broker_base_quantity": "1.0",
                    "active_protective_base_quantity": "0",
                    "desired_protective_base_quantity": "0.25000000",
                },
            "alpaca_crypto_strategy:protective_exit",
        )
    ]
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.strategy_owned_base_quantity == Decimal("0.250000000000")
        assert db.query(ExecutionFill).count() == 3


def test_protective_exit_excludes_conflicting_duplicate_strategy_fill_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(settings=settings, session_factory=session_factory)
    submitted: list[tuple[Decimal, dict[str, object], str]] = []

    with session_factory() as db:
        state = CryptoStrategyState(
            mode="paper",
            provider="alpaca",
            account_scope=str(_execution_scopes(settings)[0]["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            high_water_price=Decimal("120"),
            protective_stop_price=Decimal("100"),
            metrics={"quantity_increment": "0.00000001"},
        )
        strategy_order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="strategy-conflicting-buy",
            broker_order_id="strategy-conflicting-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": _execution_scopes(settings)[1],
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": _execution_scopes(settings)[1],
                },
            },
        )
        db.add_all([state, strategy_order])
        db.flush()
        db.add_all([
            ExecutionFill(
                broker_order_pk=strategy_order.id,
                broker_fill_id="strategy-conflicting-fill",
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.25"),
                quote_notional=Decimal("25"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=5),
            ),
            ExecutionFill(
                broker_order_pk=strategy_order.id,
                broker_fill_id="strategy-conflicting-fill",
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.30"),
                quote_notional=Decimal("30"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=5),
            ),
        ])
        db.commit()

    def fake_place_broker_order(*, command, request_source, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append((command.base_quantity, command.metadata, request_source))
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "message": "paper order accepted", "order": {"status": "submitted"}},
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "1.0"},
            )
        ],
    )

    result = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result["protective_actions"] == []
    assert submitted == []
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.strategy_owned_base_quantity == Decimal("0E-12")
        assert db.query(ExecutionFill).count() == 2


def test_force_exit_caps_crypto_sell_to_strategy_owned_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 17, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(settings=settings, session_factory=session_factory)
    submitted: list[Decimal] = []

    with session_factory() as db:
        strategy_order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="strategy-buy",
            broker_order_id="strategy-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": _execution_scopes(settings)[1],
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": _execution_scopes(settings)[1],
                },
            },
        )
        db.add(strategy_order)
        db.flush()
        db.add(
            ExecutionFill(
                broker_order_pk=strategy_order.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.25"),
                quote_notional=Decimal("25"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(days=1),
            )
        )
        db.commit()

    def ineligible_symbol(
        symbol: str,
        _bars,
        *,
        as_of,
        annualization_days,
    ):  # noqa: ANN001, ANN201
        assert annualization_days == Decimal("365")
        return TrendFeatures(
            symbol=symbol,
            as_of_date=as_of.date(),
            closed_bar_count=200,
            latest_bar_date=date(2026, 8, 16),
            latest_close=Decimal("100"),
            ema50=Decimal("90"),
            ema200=Decimal("110"),
            total_return_90d=Decimal("-0.01"),
            atr14=Decimal("2"),
            realized_volatility_30d=Decimal("0.20"),
            eligible=False,
            reasons=("close_not_above_ema200",),
        )

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command.base_quantity)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "message": "paper order accepted", "order": {"status": "submitted"}},
        )

    monkeypatch.setattr(strategy_module, "evaluate_symbol", ineligible_symbol)
    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "0.10"},
            )
        ],
    )

    result = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("100"),
                ask=Decimal("101"),
                spread=Decimal("1"),
                spread_bps=Decimal("99"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    action = result["daily_decision"]["actions"][0]  # type: ignore[index]
    assert action["action"] == "sell"
    assert action["requested_base_quantity"] == "0.10"
    assert action["strategy_owned_base_quantity"] == "0.10"
    assert action["broker_base_quantity"] == "0.10"
    assert submitted == [Decimal("0.10")]
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.strategy_owned_base_quantity == Decimal("0.100000000000")


def test_account_rotation_quarantines_prior_and_legacy_strategy_state_and_fills(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 17, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    scope_a, route_a = _execution_scopes(settings, broker_account_id="account-a")
    scope_b, _route_b = _execution_scopes(settings, broker_account_id="account-b")
    submitted: list[str] = []

    with session_factory() as db:
        prior_state = CryptoStrategyState(
            mode="paper",
            provider="alpaca",
            account_scope=str(scope_a["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            strategy_owned_base_quantity=Decimal("0.25"),
            high_water_price=Decimal("120"),
            protective_stop_price=Decimal("100"),
            metrics={"quantity_increment": "0.00000001"},
        )
        prior_risk = CryptoStrategyRiskState(
            mode="paper",
            provider="alpaca",
            account_scope=str(scope_a["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            daily_baseline_equity=Decimal("100000"),
            daily_baseline_at=observed_at,
            weekly_baseline_equity=Decimal("100000"),
            weekly_baseline_at=observed_at,
            peak_equity=Decimal("100000"),
            peak_equity_at=observed_at,
            current_equity=Decimal("100000"),
            current_equity_at=observed_at,
            risk_status="healthy",
        )
        prior_order = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="account-a-strategy-buy",
            broker_order_id="account-a-strategy-buy",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": route_a,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_a,
                },
            },
        )
        db.add_all([prior_state, prior_risk, prior_order])
        db.flush()
        db.add(
            ExecutionFill(
                broker_order_pk=prior_order.id,
                symbol="BTC/USD",
                side="buy",
                quantity=0,
                base_quantity=Decimal("0.25"),
                quote_notional=Decimal("25"),
                price=Decimal("100"),
                filled_at=observed_at - timedelta(minutes=1),
            )
        )
        db.commit()

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command.idempotency_key)
        return PlaceBrokerOrderResult(status_code=201, payload={"duplicate": False})

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        broker_account_id="account-b",
        total_asset_value=Decimal("50000"),
        previous_total_asset_value=Decimal("50000"),
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "1"},
            )
        ],
    )
    result = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=scope_b,
    )

    assert result["status"] == "healthy"
    assert result["protective_actions"] == []
    assert submitted == []
    with session_factory() as db:
        current_state = db.query(CryptoStrategyState).filter_by(
            account_scope=scope_b["account_scope"],
            strategy_family="crypto:ALPACA",
            symbol="BTC/USD",
        ).one()
        assert current_state.strategy_owned_base_quantity == Decimal("0E-12")
        assert current_state.protective_stop_price is None
        current_risk = db.query(CryptoStrategyRiskState).filter_by(
            account_scope=scope_b["account_scope"],
            strategy_family="crypto:ALPACA",
        ).one()
        assert current_risk.daily_baseline_equity == Decimal("50000.0000")
        assert current_risk.risk_status == "healthy"
        assert db.query(CryptoStrategyState).count() == 2
        assert db.query(CryptoStrategyRiskState).count() == 2


@pytest.mark.parametrize(
    ("legacy_kind", "configured_version", "legacy_version"),
    [
        pytest.param(
            "state",
            "crypto_trend_v1",
            "crypto_trend_v1",
            id="state-current-version",
        ),
        pytest.param(
            "state",
            "crypto_trend_v2",
            "crypto_trend_v1",
            id="state-prior-version",
        ),
        pytest.param(
            "risk",
            "crypto_trend_v2",
            "crypto_trend_v1",
            id="risk-prior-version",
        ),
        pytest.param(
            "decision",
            "crypto_trend_v2",
            "crypto_trend_v1",
            id="decision-prior-version",
        ),
        pytest.param(
            "order",
            "crypto_trend_v1",
            None,
            id="active-order",
        ),
        pytest.param(
            "terminal_order",
            "crypto_trend_v1",
            None,
            id="terminal-order",
        ),
    ],
)
def test_legacy_unscoped_strategy_authority_allows_only_scoped_protection(
    monkeypatch: pytest.MonkeyPatch,
    legacy_kind: str,
    configured_version: str,
    legacy_version: str | None,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(
        alpaca_crypto_allowed_symbols="BTC/USD",
        alpaca_crypto_strategy_version=configured_version,
        alpaca_crypto_strategy_decision_minute_utc=0,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    coordinator_scope, _ = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    if legacy_kind == "state":
        assert legacy_version is not None
        legacy = CryptoStrategyState(
            mode="paper",
            provider="alpaca",
            account_scope=None,
            strategy_family=None,
            strategy_version=legacy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            strategy_owned_base_quantity=Decimal("1"),
        )
    elif legacy_kind == "risk":
        assert legacy_version is not None
        legacy = CryptoStrategyRiskState(
            mode="paper",
            provider="alpaca",
            account_scope=None,
            strategy_family=None,
            strategy_version=legacy_version,
            risk_status="healthy",
        )
    elif legacy_kind == "decision":
        assert legacy_version is not None
        legacy = CryptoStrategyDecision(
            mode="paper",
            provider="alpaca",
            account_scope=None,
            strategy_family=None,
            strategy_version=legacy_version,
            symbol="BTC/USD",
            decision_bar_at=observed_at - timedelta(days=1),
            decided_at=observed_at - timedelta(days=1),
            action="hold",
            decision_status="no_action",
            eligible=False,
            target_fraction=Decimal("0"),
        )
    else:
        legacy = BrokerOrder(
            broker_name="alpaca",
            mode="paper",
            idempotency_key="legacy-unscoped-strategy-buy",
            broker_order_id="legacy-unscoped-strategy-buy",
            symbol="BTC/USD",
            side="buy",
            status="canceled" if legacy_kind == "terminal_order" else "open",
            base_quantity=Decimal("1"),
            venue="alpaca",
            asset_class="crypto",
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
            },
        )
    with session_factory() as db:
        db.add(legacy)
        db.commit()

    submitted = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "message": "paper order accepted",
                "order": {"id": "protective-order", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99.9"),
                ask=Decimal("100.0"),
                spread=Decimal("0.1"),
                spread_bps=Decimal("10.01"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "legacy_strategy_scope_requires_manual_review"
    assert result["protective_actions"][0]["action"] == "protective_sell"  # type: ignore[index]
    assert [command.side for command in submitted] == ["sell"]
    with session_factory() as db:
        assert db.query(CryptoStrategyState).count() == 1 + int(legacy_kind == "state")
        assert db.query(CryptoStrategyRiskState).count() == 1 + int(legacy_kind == "risk")
        assert db.query(CryptoStrategyDecision).count() == int(
            legacy_kind == "decision"
        )


def test_strategy_revalidates_coordinator_scope_against_own_snapshot() -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    scope_a, _route_a = _execution_scopes(settings, broker_account_id="account-a")

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            broker_account_id="account-b",
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=scope_a,
    )

    assert result == {
        "enabled": True,
        "status": "blocked",
        "reason": "authenticated_execution_scope_mismatch",
    }
    with session_factory() as db:
        assert db.query(CryptoStrategyState).count() == 0
        assert db.query(CryptoStrategyRiskState).count() == 0


@pytest.mark.parametrize(
    ("holdings", "holdings_complete", "open_orders_complete", "reason"),
    [
        (
            [
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.1"},
                )
            ],
            False,
            True,
            "broker_holdings_truth_unavailable",
        ),
        (
            [
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.1"},
                ),
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.2"},
                ),
            ],
            True,
            True,
            "duplicate_broker_holding_identity",
        ),
    ],
)
def test_strategy_blocks_incomplete_or_duplicate_inventory_truth(
    holdings: list[BrokerAccountHolding],
    holdings_complete: bool,
    open_orders_complete: bool,
    reason: str,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=holdings,
            holdings_complete=holdings_complete,
            open_orders_complete=open_orders_complete,
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result == {
        "enabled": True,
        "status": "blocked",
        "reason": reason,
    }
    with session_factory() as db:
        assert db.query(CryptoStrategyState).count() == 0
        assert db.query(CryptoStrategyRiskState).count() == 0


def test_strategy_blocks_daily_decision_when_open_order_truth_is_incomplete() -> None:
    observed_at = datetime(2026, 8, 16, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=create_session_factory(engine),
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            open_orders_complete=False,
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert result["status"] == "degraded"
    assert result["daily_decision"] == {
        "status": "blocked",
        "reason": "broker_open_orders_truth_unavailable",
    }


def test_asset_increments_reject_duplicate_symbol_instead_of_last_wins() -> None:
    assert _asset_increments(
        [
            {"symbol": "BTCUSD", "min_trade_increment": "0.000001"},
            {"symbol": "BTC/USD", "min_trade_increment": "0.01"},
            {"symbol": "ETH/USD", "min_trade_increment": "0.0001"},
        ]
    ) == {"ETH/USD": Decimal("0.0001")}


def test_market_closed_revalidates_current_account_before_scoped_success() -> None:
    observed_at = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="SPY")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
        asset_class="us_equity",
        exchange="US",
        time_in_force="day",
        order_origin="alpaca_us_equity_strategy",
    )
    stale_scope, _stale_route = _execution_scopes(
        settings,
        broker_account_id="account-a",
        asset_class="us_equity",
        exchange="US",
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            broker_account_id="account-b",
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={},
        observed_at=observed_at,
        execution_scope=stale_scope,
        market_open=False,
    )

    assert result == {
        "enabled": True,
        "status": "blocked",
        "reason": "authenticated_execution_scope_mismatch",
    }


def test_same_account_and_symbol_are_isolated_between_strategy_families() -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    adapter = FakeStrategyAdapter(observed_at=observed_at)
    crypto = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    us_equity = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
        asset_class="us_equity",
        exchange="US",
        time_in_force="day",
        order_origin="alpaca_us_equity_strategy",
    )
    assert crypto.annualization_days == Decimal("365")
    assert us_equity.annualization_days == Decimal("252")

    crypto.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )
    us_equity.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at),
        observed_at=observed_at,
        execution_scope=_execution_scopes(
            settings,
            asset_class="us_equity",
            exchange="US",
        )[0],
    )

    with session_factory() as db:
        assert {
            state.strategy_family for state in db.query(CryptoStrategyState).all()
        } == {"crypto:ALPACA", "us_equity:US"}
        assert {
            state.strategy_family
            for state in db.query(CryptoStrategyRiskState).all()
        } == {"crypto:ALPACA", "us_equity:US"}


def test_order_key_isolated_by_account_and_strategy_family() -> None:
    common = {
        "version": "crypto_trend_v1",
        "decision_date": date(2026, 8, 16),
        "symbol": "BTC/USD",
        "action": "daily-buy",
    }

    account_a_crypto = _order_key(
        **common,
        account_scope="a" * 24,
        strategy_family="crypto:ALPACA",
    )
    account_b_crypto = _order_key(
        **common,
        account_scope="b" * 24,
        strategy_family="crypto:ALPACA",
    )
    account_a_equity = _order_key(
        **common,
        account_scope="a" * 24,
        strategy_family="us_equity:US",
    )

    assert len({account_a_crypto, account_b_crypto, account_a_equity}) == 3
    assert all(len(value) <= 128 for value in (
        account_a_crypto,
        account_b_crypto,
        account_a_equity,
    ))


def test_protective_exit_key_advances_only_after_a_terminal_attempt() -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    _coordinator_scope, route_scope = _execution_scopes(settings)

    with session_factory() as db:
        canceled = BrokerOrder(
            id="protective-canceled-1",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="protective-key-1",
            broker_order_id="protective-broker-1",
            symbol="BTC/USD",
            side="sell",
            status="canceled",
            base_quantity=Decimal("1"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=2),
            request_metadata={
                "source": "alpaca_crypto_strategy:protective_exit",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                    "decision_type": "protective_exit",
                },
            },
        )
        db.add(canceled)
        db.flush()

        next_key = _protective_exit_order_key(
            settings=settings,
            db=db,
            symbol="BTC/USD",
            observed_at=observed_at,
            order_origin="alpaca_crypto_strategy",
            strategy_family="crypto:ALPACA",
            route_scope=strategy_module.FifoRouteScope(
                provider="alpaca",
                account_scope=str(route_scope["account_scope"]),
                venue="alpaca",
                asset_class="crypto",
                exchange="ALPACA",
            ),
        )
        assert next_key != canceled.idempotency_key
        assert next_key.endswith(sha256(canceled.id.encode("utf-8")).hexdigest()[:16])

        active = BrokerOrder(
            id="protective-open-2",
            broker_name="alpaca",
            mode="paper",
            idempotency_key=next_key,
            broker_order_id="protective-broker-2",
            symbol="BTC/USD",
            side="sell",
            status="open",
            base_quantity=Decimal("0.6"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=1),
            request_metadata={
                "source": "alpaca_crypto_strategy:protective_exit",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                    "decision_type": "protective_exit",
                },
            },
        )
        db.add(active)
        db.flush()

        assert _protective_exit_order_key(
            settings=settings,
            db=db,
            symbol="BTC/USD",
            observed_at=observed_at,
            order_origin="alpaca_crypto_strategy",
            strategy_family="crypto:ALPACA",
            route_scope=strategy_module.FifoRouteScope(
                provider="alpaca",
                account_scope=str(route_scope["account_scope"]),
                venue="alpaca",
                asset_class="crypto",
                exchange="ALPACA",
            ),
        ) == next_key

        active.status = "expired"
        db.flush()
        third_key = _protective_exit_order_key(
            settings=settings,
            db=db,
            symbol="BTC/USD",
            observed_at=observed_at,
            order_origin="alpaca_crypto_strategy",
            strategy_family="crypto:ALPACA",
            route_scope=strategy_module.FifoRouteScope(
                provider="alpaca",
                account_scope=str(route_scope["account_scope"]),
                venue="alpaca",
                asset_class="crypto",
                exchange="ALPACA",
            ),
        )
        assert third_key not in {canceled.idempotency_key, next_key}


def test_terminal_partial_protective_exit_rearms_for_exact_remaining_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    coordinator_scope, route_scope = _execution_scopes(settings)
    old_key = _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=observed_at.date(),
        symbol="BTC/USD",
        action="protective-sell",
        account_scope=str(coordinator_scope["account_scope"]),
        strategy_family="crypto:ALPACA",
    )
    submitted: list[object] = []

    with session_factory() as db:
        state = CryptoStrategyState(
            mode="paper",
            provider="alpaca",
            account_scope=str(coordinator_scope["account_scope"]),
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            eligible=True,
            target_fraction=Decimal("0.40"),
            strategy_owned_base_quantity=Decimal("1"),
            high_water_price=Decimal("120"),
            protective_stop_price=Decimal("100"),
            last_action="buy",
            last_action_at=observed_at - timedelta(minutes=10),
            metrics={"quantity_increment": "0.00000001"},
        )
        buy = BrokerOrder(
            id="strategy-buy-one",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="strategy-buy-one",
            broker_order_id="strategy-buy-one",
            symbol="BTC/USD",
            side="buy",
            status="filled",
            base_quantity=Decimal("1"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=10),
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                },
            },
        )
        canceled_sell = BrokerOrder(
            id="protective-partial-canceled",
            broker_name="alpaca",
            mode="paper",
            idempotency_key=old_key,
            broker_order_id="protective-old-broker-id",
            symbol="BTC/USD",
            side="sell",
            status="canceled",
            base_quantity=Decimal("1"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=5),
            request_metadata={
                "source": "alpaca_crypto_strategy:protective_exit",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                    "decision_type": "protective_exit",
                },
            },
        )
        db.add_all([state, buy, canceled_sell])
        db.flush()
        db.add_all(
            [
                ExecutionFill(
                    broker_order_pk=buy.id,
                    symbol="BTC/USD",
                    side="buy",
                    quantity=0,
                    base_quantity=Decimal("1"),
                    quote_notional=Decimal("100"),
                    price=Decimal("100"),
                    filled_at=observed_at - timedelta(minutes=9),
                ),
                ExecutionFill(
                    broker_order_pk=canceled_sell.id,
                    symbol="BTC/USD",
                    side="sell",
                    quantity=0,
                    base_quantity=Decimal("0.4"),
                    quote_notional=Decimal("40"),
                    price=Decimal("100"),
                    filled_at=observed_at - timedelta(minutes=4),
                ),
            ]
        )
        db.commit()

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "message": "paper order accepted",
                "order": {"id": "protective-new", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.6"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert len(submitted) == 1
    command = submitted[0]
    assert command.base_quantity == Decimal("0.60000000")
    assert command.idempotency_key != old_key
    assert result["protective_actions"] == [
        {
            "symbol": "BTC/USD",
            "action": "protective_sell",
            "quantity": "0.6",
            "stop_price": "100.000000000000",
            "trigger_bid": "99",
            "order": {
                "status_code": 201,
                "duplicate": False,
                "message": "paper order accepted",
                "order_id": "protective-new",
                "broker_order_id": None,
                "status": "submitted",
            },
        }
    ]
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.strategy_owned_base_quantity == Decimal("0.600000000000")
        assert state.last_action == "protective_sell"


@pytest.mark.parametrize(
    ("status_code", "payload", "expected_action"),
    [
        (
            409,
            {
                "duplicate": False,
                "block_reason": "runtime_paused",
                "message": "blocked",
                "order": {"status": "rejected"},
            },
            "protective_sell_blocked",
        ),
        (
            422,
            {
                "duplicate": False,
                "rejection_reason": "invalid_order",
                "message": "rejected",
                "order": {"status": "rejected"},
            },
            "protective_sell_blocked",
        ),
        (
            503,
            {"duplicate": False, "message": "broker unavailable"},
            "protective_sell_failed",
        ),
        (
            200,
            {
                "duplicate": True,
                "message": "terminal duplicate",
                "order": {"status": "canceled"},
            },
            "protective_sell_blocked",
        ),
    ],
)
def test_unacknowledged_protective_order_degrades_without_recording_success(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    payload: dict[str, object],
    expected_action: str,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **_kwargs: PlaceBrokerOrderResult(
            status_code=status_code,
            payload=payload,
        ),
    )
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    action = result["protective_actions"][0]
    assert action["action"] == expected_action
    assert action["reason"]
    assert action["status_code"] == status_code
    assert result["status"] == "degraded"
    assert "protection_blocked" in result["reasons"]
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.last_action == "buy"
        assert state.last_action_at == observed_at.replace(tzinfo=None) - timedelta(minutes=10)


def test_retryable_protective_auth_failure_rearms_with_same_scoped_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    commands: list[object] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        commands.append(command)
        if len(commands) == 1:
            return PlaceBrokerOrderResult(
                status_code=503,
                payload={
                    "duplicate": False,
                    "block_reason": "broker_authentication_failed",
                    "order": {"status": "retry_pending"},
                },
            )
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "order": {"status": "submitted"},
            },
        )

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        fake_place_broker_order,
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "0.25"},
            )
        ],
    )
    quote = BrokerLiveMarketContext(
        symbol="BTC/USD",
        exchange="ALPACA",
        observed_at=observed_at,
        bid=Decimal("99"),
        ask=Decimal("100"),
        spread=Decimal("1"),
        spread_bps=Decimal("100"),
        freshness_source="alpaca_exchange_quote_time",
    )

    first = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": quote},
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )
    second = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": quote},
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert first["status"] == "degraded"
    assert first["protective_actions"][0]["action"] == "protective_sell_failed"  # type: ignore[index]
    assert second["protective_actions"][0]["action"] == "protective_sell"  # type: ignore[index]
    assert len(commands) == 2
    assert commands[0].idempotency_key == commands[1].idempotency_key  # type: ignore[union-attr]
    assert commands[0].metadata["expected_execution_scope"] == route_scope  # type: ignore[union-attr]
    assert commands[1].metadata["expected_execution_scope"] == route_scope  # type: ignore[union-attr]


def test_active_duplicate_protective_order_is_a_successful_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **_kwargs: PlaceBrokerOrderResult(
            status_code=200,
            payload={
                "duplicate": True,
                "message": "active duplicate",
                "order": {"id": "protective-active", "status": "open"},
            },
        ),
    )
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert result["protective_actions"][0]["action"] == "protective_sell"
    assert result["status"] == "healthy"
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert state.last_action == "protective_sell"


def test_untrusted_quote_reports_protection_blocked_without_broker_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted: list[object] = []

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs) or PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "order": {"status": "submitted"}},
        ),
    )
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="transport_received_at_untrusted",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert submitted == []
    assert result["protective_actions"] == [
        {
            "symbol": "BTC/USD",
            "action": "protective_sell_blocked",
            "reason": "quote_not_actionable",
        }
    ]
    assert result["status"] == "degraded"
    assert result["reasons"] == ["quotes_not_actionable", "protection_blocked"]


@pytest.mark.parametrize(
    ("open_side", "expected_action", "expected_status"),
    [
        ("buy", "protective_sell_blocked", "degraded"),
        ("sell", "protective_sell_active", "healthy"),
    ],
)
def test_only_active_sell_open_order_counts_as_existing_protection(
    monkeypatch: pytest.MonkeyPatch,
    open_side: str,
    expected_action: str,
    expected_status: str,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    if open_side == "sell":
        with session_factory() as db:
            db.add(
                BrokerOrder(
                    broker_name="alpaca",
                    mode="paper",
                    idempotency_key="verified-protective-open",
                    broker_order_id="open-sell",
                    symbol="BTC/USD",
                    side="sell",
                    status="open",
                    base_quantity=Decimal("0.25"),
                    venue="alpaca",
                    asset_class="crypto",
                    submitted_at=observed_at - timedelta(minutes=1),
                    request_metadata={
                        "source": "alpaca_crypto_strategy:protective_exit",
                        "exchange": "ALPACA",
                        "execution_scope": route_scope,
                        "metadata": {
                            "strategy_family": "crypto:ALPACA",
                            "expected_execution_scope": route_scope,
                            "decision_type": "protective_exit",
                            "protective_stop_price": "100",
                            "trigger_bid": "99",
                        },
                    },
                )
            )
            db.commit()
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted: list[object] = []

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs) or PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "order": {"status": "submitted"}},
        ),
    )
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol="BTC/USD",
                    side=open_side,
                    quantity=0,
                    status="open",
                    broker_order_id=f"open-{open_side}",
                    idempotency_key=(
                        "verified-protective-open" if open_side == "sell" else None
                    ),
                    payload={
                        "base_quantity": "0.25",
                        "remaining_base_quantity": "0.25",
                    },
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert submitted == []
    assert result["protective_actions"][0]["action"] == expected_action
    assert result["status"] == expected_status
    if open_side == "buy":
        assert result["protective_actions"][0]["reason"] == "conflicting_open_buy"
        assert "protection_blocked" in result["reasons"]


@pytest.mark.parametrize(
    ("asset_class", "latest_bar_date", "observed_at", "max_age_days", "expected"),
    [
        (
            "crypto",
            date(2026, 9, 6),
            datetime(2026, 9, 8, 0, 20, tzinfo=UTC),
            1,
            False,
        ),
        (
            "us_equity",
            date(2026, 9, 4),
            datetime(2026, 9, 8, 14, 0, tzinfo=UTC),
            4,
            True,
        ),
        (
            "us_equity",
            date(2026, 9, 3),
            datetime(2026, 9, 8, 14, 0, tzinfo=UTC),
            4,
            False,
        ),
    ],
)
def test_latest_closed_bar_freshness_is_asset_and_session_aware(
    asset_class: str,
    latest_bar_date: date,
    observed_at: datetime,
    max_age_days: int,
    expected: bool,
) -> None:
    assert _latest_closed_bar_is_fresh(
        latest_bar_date=latest_bar_date,
        observed_at=observed_at,
        asset_class=asset_class,
        max_age_days=max_age_days,
    ) is expected


def test_stale_daily_bars_block_entries_but_protective_exit_still_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted_sides: list[str] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted_sides.append(command.side)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "message": "accepted",
                "order": {"id": f"order-{len(submitted_sides)}", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            latest_bar_age_days=10,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert submitted_sides == ["sell"]
    assert result["protective_actions"][0]["action"] == "protective_sell"
    assert result["daily_decision"]["status"] == "blocked"
    assert result["daily_decision"]["reason"] == "daily_bars_stale"
    assert result["status"] == "degraded"
    assert "daily_bars_stale" in result["reasons"]


@pytest.mark.parametrize(
    "expected_reason",
    ["closed_daily_bars_unavailable", "symbols_do_not_share_latest_closed_bar"],
)
def test_daily_bar_integrity_blocks_degrade_the_cycle(
    monkeypatch: pytest.MonkeyPatch,
    expected_reason: str,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 20, tzinfo=UTC)
    allowed_symbols = (
        "BTC/USD"
        if expected_reason == "closed_daily_bars_unavailable"
        else "BTC/USD,ETH/USD"
    )
    settings = _settings(alpaca_crypto_allowed_symbols=allowed_symbols)
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    adapter = FakeStrategyAdapter(observed_at=observed_at)
    original_get_bars = adapter.get_crypto_daily_bars

    def integrity_broken_bars(*args, **kwargs):  # noqa: ANN002, ANN003, ANN201
        bars = original_get_bars(*args, **kwargs)
        if expected_reason == "closed_daily_bars_unavailable":
            bars["BTC/USD"] = []
        else:
            bars["ETH/USD"] = bars["ETH/USD"][:-1]
        return bars

    adapter.get_crypto_daily_bars = integrity_broken_bars  # type: ignore[method-assign]
    submitted: list[object] = []
    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs),
    )

    result = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={
            symbol: _quotes(observed_at)[symbol]
            for symbol in allowed_symbols.split(",")
        },
        observed_at=observed_at,
        execution_scope=_execution_scopes(settings)[0],
    )

    assert submitted == []
    assert result["daily_decision"]["status"] == "blocked"
    assert result["daily_decision"]["reason"] == expected_reason
    assert result["status"] == "degraded"
    assert expected_reason in result["reasons"]


@pytest.mark.parametrize(
    ("metrics", "expected_reason"),
    [
        ({}, "trade_increment_unavailable"),
        ({"quantity_increment": "1"}, "below_trade_increment"),
    ],
)
def test_unorderable_protective_quantity_degrades_instead_of_false_healthy(
    monkeypatch: pytest.MonkeyPatch,
    metrics: dict[str, object],
    expected_reason: str,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        state.metrics = metrics
        db.commit()
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted: list[object] = []
    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs),
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert submitted == []
    assert result["protective_actions"] == [
        {
            "symbol": "BTC/USD",
            "action": "protective_sell_blocked",
            "reason": expected_reason,
        }
    ]
    assert result["status"] == "degraded"
    assert "protection_blocked" in result["reasons"]


@pytest.mark.parametrize(
    ("scenario", "open_remaining", "expected_quantity"),
    [
        ("partial_protective", Decimal("0.01"), Decimal("0.99")),
        ("unrelated_take_profit", Decimal("1"), Decimal("1")),
    ],
)
def test_open_sell_only_offsets_verified_protective_remaining_quantity(
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    open_remaining: Decimal,
    expected_quantity: Decimal,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
        base_quantity=Decimal("1"),
    )
    open_key = (
        "partial-protective-open"
        if scenario == "partial_protective"
        else "unrelated-take-profit-open"
    )
    with session_factory() as db:
        db.add(
            BrokerOrder(
                id=f"local-{open_key}",
                broker_name="alpaca",
                mode="paper",
                idempotency_key=open_key,
                broker_order_id=f"broker-{open_key}",
                symbol="BTC/USD",
                side="sell",
                status="open",
                base_quantity=open_remaining,
                venue="alpaca",
                asset_class="crypto",
                limit_price=(
                    Decimal("150")
                    if scenario == "unrelated_take_profit"
                    else None
                ),
                submitted_at=observed_at - timedelta(minutes=1),
                request_metadata=(
                    {
                        "source": "alpaca_crypto_strategy:protective_exit",
                        "exchange": "ALPACA",
                        "execution_scope": route_scope,
                        "metadata": {
                            "strategy_family": "crypto:ALPACA",
                            "expected_execution_scope": route_scope,
                            "decision_type": "protective_exit",
                            "protective_stop_price": "100",
                            "trigger_bid": "99",
                        },
                    }
                    if scenario == "partial_protective"
                    else {
                        "source": "post:/orders",
                        "exchange": "ALPACA",
                        "execution_scope": route_scope,
                    }
                ),
            )
        )
        db.commit()

    submitted: list[object] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted.append(command)
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "order": {"id": "supplemental-protection", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "1"},
                )
            ],
            open_orders=[
                BrokerAccountOpenOrder(
                    symbol="BTC/USD",
                    side="sell",
                    quantity=0,
                    status="open",
                    broker_order_id=f"broker-{open_key}",
                    idempotency_key=open_key,
                    limit_price=(
                        Decimal("150")
                        if scenario == "unrelated_take_profit"
                        else None
                    ),
                    payload={
                        "base_quantity": format(open_remaining, "f"),
                        "remaining_base_quantity": format(open_remaining, "f"),
                    },
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert len(submitted) == 1
    assert submitted[0].base_quantity == expected_quantity
    if scenario == "partial_protective":
        assert submitted[0].idempotency_key != open_key
    assert result["protective_actions"][0]["action"] == "protective_sell"


def test_incomplete_open_order_coverage_blocks_protection_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    submitted: list[object] = []
    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs),
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
            snapshot_payload={
                "open_orders_coverage": "incomplete",
                "open_orders_error": {
                    "code": "alpaca_open_orders_page_limit_reached",
                    "degraded": True,
                },
            },
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert submitted == []
    assert result["protective_actions"] == [
        {
            "symbol": "BTC/USD",
            "action": "protective_sell_blocked",
            "reason": "open_orders_coverage_degraded",
        }
    ]
    assert result["status"] == "degraded"
    assert "protection_blocked" in result["reasons"]


def test_transient_force_exit_retries_same_bar_before_marking_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    with session_factory() as db:
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        state.protective_stop_price = Decimal("50")
        db.commit()
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted_keys: list[str] = []

    def ineligible_symbol(
        symbol: str,
        _bars,
        *,
        as_of,
        annualization_days,
    ):  # noqa: ANN001, ANN201
        assert annualization_days == Decimal("365")
        return TrendFeatures(
            symbol=symbol,
            as_of_date=as_of.date(),
            closed_bar_count=230,
            latest_bar_date=observed_at.date() - timedelta(days=1),
            latest_close=Decimal("100"),
            ema50=Decimal("90"),
            ema200=Decimal("110"),
            total_return_90d=Decimal("-0.01"),
            atr14=Decimal("2"),
            realized_volatility_30d=Decimal("0.20"),
            eligible=False,
            reasons=("close_not_above_ema200",),
        )

    def flaky_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        submitted_keys.append(command.idempotency_key)
        if len(submitted_keys) == 1:
            return PlaceBrokerOrderResult(
                status_code=503,
                payload={"duplicate": False, "message": "broker unavailable"},
            )
        return PlaceBrokerOrderResult(
            status_code=201,
            payload={
                "duplicate": False,
                "message": "accepted",
                "order": {"id": "force-exit-retry", "status": "submitted"},
            },
        )

    monkeypatch.setattr(strategy_module, "evaluate_symbol", ineligible_symbol)
    monkeypatch.setattr(strategy_module, "place_broker_order", flaky_place_broker_order)
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "0.25"},
            )
        ],
    )
    quote = BrokerLiveMarketContext(
        symbol="BTC/USD",
        exchange="ALPACA",
        observed_at=observed_at,
        bid=Decimal("200"),
        ask=Decimal("201"),
        spread=Decimal("1"),
        spread_bps=Decimal("49.9"),
        freshness_source="alpaca_exchange_quote_time",
    )

    first = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": quote},
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )
    assert first["daily_decision"]["actions"][0]["status"] == "retryable"
    assert first["status"] == "degraded"
    assert "rebalance_retryable_failure" in first["reasons"]
    with session_factory() as db:
        decision = db.query(CryptoStrategyDecision).filter_by(symbol="BTC/USD").one()
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert decision.decision_status == "retryable"
        assert state.last_processed_bar_at is None
        assert state.last_action == "buy"

    second = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": quote},
        observed_at=observed_at + timedelta(seconds=15),
        execution_scope=coordinator_scope,
    )

    assert submitted_keys == [submitted_keys[0], submitted_keys[0]]
    assert second["daily_decision"]["actions"][0]["status"] == "submitted"
    assert second["status"] == "healthy"
    with session_factory() as db:
        decision = db.query(CryptoStrategyDecision).filter_by(symbol="BTC/USD").one()
        state = db.query(CryptoStrategyState).filter_by(symbol="BTC/USD").one()
        assert decision.decision_status == "submitted"
        assert state.last_processed_bar_at is not None
        assert state.last_action == "sell"


def test_retryable_daily_decision_advances_after_terminal_local_attempt() -> None:
    observed_at = datetime(2026, 9, 8, 0, 20, tzinfo=UTC)
    decision_bar_at = datetime(2026, 9, 7, 0, 0, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, route_scope = _execution_scopes(settings)
    route = strategy_module.FifoRouteScope(
        provider="alpaca",
        account_scope=str(coordinator_scope["account_scope"]),
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
    )
    old_key = _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=decision_bar_at.date(),
        symbol="BTC/USD",
        action="daily-sell",
        account_scope=route.account_scope,
        strategy_family="crypto:ALPACA",
    )

    with session_factory() as db:
        decision = CryptoStrategyDecision(
            mode="paper",
            provider="alpaca",
            account_scope=route.account_scope,
            strategy_family="crypto:ALPACA",
            strategy_version=settings.alpaca_crypto_strategy_version,
            symbol="BTC/USD",
            decision_bar_at=decision_bar_at,
            decided_at=observed_at,
            action="sell",
            decision_status="retryable",
            eligible=False,
            target_fraction=Decimal("0"),
            idempotency_key=old_key,
        )
        failed_order = BrokerOrder(
            id="failed-daily-attempt-one",
            broker_name="alpaca",
            mode="paper",
            idempotency_key=old_key,
            symbol="BTC/USD",
            side="sell",
            status="failed",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at - timedelta(minutes=1),
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                    "decision_type": "daily_target_rebalance",
                    "decision_bar_at": decision_bar_at.isoformat(),
                },
            },
        )
        db.add_all([decision, failed_order])
        db.flush()

        retry_key = _daily_rebalance_order_key(
            settings=settings,
            db=db,
            symbol="BTC/USD",
            decision_date=decision_bar_at.date(),
            decision_bar_at=decision_bar_at,
            action="daily-sell",
            order_origin="alpaca_crypto_strategy",
            strategy_family="crypto:ALPACA",
            route_scope=route,
        )
        assert retry_key != old_key
        assert retry_key.endswith(
            sha256(failed_order.id.encode("utf-8")).hexdigest()[:16]
        )

        retry_pending = BrokerOrder(
            id="retry-pending-daily-attempt-two",
            broker_name="alpaca",
            mode="paper",
            idempotency_key=retry_key,
            symbol="BTC/USD",
            side="sell",
            status="retry_pending",
            base_quantity=Decimal("0.25"),
            venue="alpaca",
            asset_class="crypto",
            submitted_at=observed_at,
            request_metadata={
                "source": "alpaca_crypto_strategy:daily_rebalance",
                "exchange": "ALPACA",
                "execution_scope": route_scope,
                "metadata": {
                    "strategy_family": "crypto:ALPACA",
                    "expected_execution_scope": route_scope,
                    "decision_type": "daily_target_rebalance",
                    "decision_bar_at": decision_bar_at.isoformat(),
                },
            },
        )
        decision.idempotency_key = retry_key
        db.add(retry_pending)
        db.flush()

        assert _daily_rebalance_order_key(
            settings=settings,
            db=db,
            symbol="BTC/USD",
            decision_date=decision_bar_at.date(),
            decision_bar_at=decision_bar_at,
            action="daily-sell",
            order_origin="alpaca_crypto_strategy",
            strategy_family="crypto:ALPACA",
            route_scope=route,
        ) == retry_key


def test_cold_start_uses_previous_equity_and_blocks_weekly_warmup_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 9, 9, 0, 20, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    submitted: list[object] = []

    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs) or PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "order": {"status": "submitted"}},
        ),
    )
    adapter = FakeStrategyAdapter(
        observed_at=observed_at,
        total_asset_value=Decimal("900"),
        previous_total_asset_value=Decimal("1000"),
    )
    coordinator_scope = _execution_scopes(settings)[0]

    first = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": _quotes(observed_at)["BTC/USD"]},
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )
    second = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes={"BTC/USD": _quotes(observed_at + timedelta(minutes=1))["BTC/USD"]},
        observed_at=observed_at + timedelta(minutes=1),
        execution_scope=coordinator_scope,
    )

    assert submitted == []
    assert first["status"] == "risk_blocked"
    assert second["status"] == "risk_blocked"
    assert "daily_drawdown_limit" in first["reasons"]
    assert "weekly_baseline_warmup" in first["reasons"]
    assert first["daily_decision"]["actions"][0]["status"] == "blocked_risk"
    assert second["daily_decision"]["actions"][0]["status"] == "blocked_risk"
    with session_factory() as db:
        risk = db.query(CryptoStrategyRiskState).one()
        state = db.query(CryptoStrategyState).one()
        assert risk.daily_baseline_equity == Decimal("1000.0000")
        assert risk.weekly_baseline_equity is None
        assert risk.peak_equity == Decimal("1000.0000")
        assert state.last_processed_bar_at is None


def test_weekly_baseline_warmup_keeps_protective_sell_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 9, 9, 0, 5, tzinfo=UTC)
    settings = _settings(alpaca_crypto_allowed_symbols="BTC/USD")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, _route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    with session_factory() as db:
        db.query(CryptoStrategyRiskState).delete()
        db.commit()
    submitted: list[object] = []
    monkeypatch.setattr(
        strategy_module,
        "place_broker_order",
        lambda **kwargs: submitted.append(kwargs["command"]) or PlaceBrokerOrderResult(
            status_code=201,
            payload={"duplicate": False, "order": {"status": "submitted"}},
        ),
    )
    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )

    result = service.run_cycle(
        adapter=FakeStrategyAdapter(
            observed_at=observed_at,
            holdings=[
                BrokerAccountHolding(
                    symbol="BTC/USD",
                    quantity=0,
                    payload={"base_quantity": "0.25"},
                )
            ],
        ),  # type: ignore[arg-type]
        access_token="test",
        quotes={
            "BTC/USD": BrokerLiveMarketContext(
                symbol="BTC/USD",
                exchange="ALPACA",
                observed_at=observed_at,
                bid=Decimal("99"),
                ask=Decimal("100"),
                spread=Decimal("1"),
                spread_bps=Decimal("100"),
                freshness_source="alpaca_exchange_quote_time",
            )
        },
        observed_at=observed_at,
        execution_scope=coordinator_scope,
    )

    assert result["status"] == "risk_blocked"
    assert "weekly_baseline_warmup" in result["reasons"]
    assert result["protective_actions"][0]["action"] == "protective_sell"  # type: ignore[index]
    assert len(submitted) == 1
    assert submitted[0].side == "sell"  # type: ignore[union-attr]


def test_concurrent_protective_cycles_share_one_terminal_generation_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    observed_at = datetime(2026, 9, 8, 0, 5, tzinfo=UTC)
    database_path = (tmp_path / "protective-concurrency.db").resolve()
    settings = _settings(
        database_url=f"sqlite:///{database_path}",
        alpaca_crypto_allowed_symbols="BTC/USD",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    coordinator_scope, route_scope = _seed_protected_strategy_position(
        session_factory=session_factory,
        settings=settings,
        observed_at=observed_at,
    )
    route = strategy_module.FifoRouteScope(
        provider="alpaca",
        account_scope=str(route_scope["account_scope"]),
        venue="alpaca",
        asset_class="crypto",
        exchange="ALPACA",
    )
    prior_key = _order_key(
        version=settings.alpaca_crypto_strategy_version,
        decision_date=observed_at.date(),
        symbol="BTC/USD",
        action="protective-sell",
        account_scope=route.account_scope,
        strategy_family="crypto:ALPACA",
    )
    with session_factory() as db:
        db.add(
            BrokerOrder(
                id="terminal-protective-generation",
                broker_name="alpaca",
                mode="paper",
                idempotency_key=prior_key,
                broker_order_id="terminal-protective-generation",
                symbol="BTC/USD",
                side="sell",
                status="canceled",
                base_quantity=Decimal("0.25"),
                venue="alpaca",
                asset_class="crypto",
                submitted_at=observed_at - timedelta(minutes=1),
                request_metadata={
                    "source": "alpaca_crypto_strategy:protective_exit",
                    "exchange": "ALPACA",
                    "execution_scope": route_scope,
                    "metadata": {
                        "strategy_family": "crypto:ALPACA",
                        "expected_execution_scope": route_scope,
                        "decision_type": "protective_exit",
                    },
                },
            )
        )
        db.commit()

    service = AlpacaCryptoStrategyService(
        settings=settings,
        session_factory=session_factory,
    )
    snapshot = BrokerAccountSnapshot(
        available_cash=Decimal("1000"),
        total_asset_value=Decimal("100000"),
        observed_at=observed_at,
        broker_account_id="account-a",
        payload={"open_orders_coverage": "complete"},
        holdings=[
            BrokerAccountHolding(
                symbol="BTC/USD",
                quantity=0,
                payload={"base_quantity": "0.25"},
            )
        ],
    )
    quotes = {
        "BTC/USD": BrokerLiveMarketContext(
            symbol="BTC/USD",
            exchange="ALPACA",
            observed_at=observed_at,
            bid=Decimal("99"),
            ask=Decimal("100"),
            spread=Decimal("1"),
            spread_bps=Decimal("100"),
            freshness_source="alpaca_exchange_quote_time",
        )
    }
    barrier = Barrier(2)
    lock = Lock()
    broker_keys: set[str] = set()
    accepted_keys: list[str] = []

    def fake_place_broker_order(*, command, **_kwargs):  # noqa: ANN003, ANN201
        barrier.wait(timeout=5)
        with lock:
            duplicate = command.idempotency_key in broker_keys
            broker_keys.add(command.idempotency_key)
            if not duplicate:
                accepted_keys.append(command.idempotency_key)
        return PlaceBrokerOrderResult(
            status_code=200 if duplicate else 201,
            payload={
                "duplicate": duplicate,
                "order": {
                    "id": "one-protective-order",
                    "status": "open" if duplicate else "submitted",
                },
            },
        )

    monkeypatch.setattr(strategy_module, "place_broker_order", fake_place_broker_order)

    def run_protective_cycle() -> list[dict[str, object]]:
        with session_factory() as db:
            state = db.query(CryptoStrategyState).filter_by(
                account_scope=coordinator_scope["account_scope"],
                strategy_family="crypto:ALPACA",
                symbol="BTC/USD",
            ).one()
            return service._run_protective_exits(
                db=db,
                adapter=FakeStrategyAdapter(observed_at=observed_at),  # type: ignore[arg-type]
                snapshot=snapshot,
                quotes=quotes,
                quote_health={"blocked_symbols": []},
                states={"BTC/USD": state},
                observed_at=observed_at,
                route_scope=route,
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: run_protective_cycle(), range(2)))

    assert len(broker_keys) == 1
    assert len(accepted_keys) == 1
    assert accepted_keys[0] != prior_key
    assert [actions[0]["action"] for actions in results] == [
        "protective_sell",
        "protective_sell",
    ]
