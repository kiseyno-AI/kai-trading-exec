from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

import app.services.alpaca_crypto_strategy as strategy_module
from app.broker.base import BrokerAccountHolding, BrokerAccountSnapshot, BrokerLiveMarketContext
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    CryptoDailyBar,
    CryptoStrategyDecision,
    CryptoStrategyState,
    ExecutionFill,
)
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService, _quote_health
from app.services.broker_orders import PlaceBrokerOrderResult
from app.services.crypto_trend_strategy import TrendFeatures


class FakeStrategyAdapter:
    def __init__(
        self,
        *,
        observed_at: datetime,
        holdings: list[BrokerAccountHolding] | None = None,
    ) -> None:
        self.observed_at = observed_at
        self.holdings = holdings or []

    def get_account_snapshot(self, *_args, **_kwargs) -> BrokerAccountSnapshot:  # noqa: ANN002, ANN003
        return BrokerAccountSnapshot(
            available_cash=Decimal("1000"),
            buying_power=Decimal("1000"),
            total_asset_value=Decimal("100000"),
            previous_total_asset_value=Decimal("100000"),
            observed_at=self.observed_at,
            source="test",
            holdings=self.holdings,
        )

    def get_crypto_daily_bars(self, *_args, symbols: list[str], **_kwargs):  # noqa: ANN002, ANN003, ANN201
        start = self.observed_at.date() - timedelta(days=230)
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
        "alpaca_crypto_strategy_enabled": True,
        "alpaca_crypto_allowed_symbols": "BTC/USD,ETH/USD",
        "alpaca_crypto_strategy_decision_hour_utc": 0,
        "alpaca_crypto_strategy_decision_minute_utc": 10,
        "alpaca_crypto_strategy_sleeve_usd": Decimal("1000"),
        "alpaca_crypto_max_order_notional_usd": Decimal("100"),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


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
        ),
        "ETH/USD": BrokerLiveMarketContext(
            symbol="ETH/USD",
            exchange="ALPACA",
            observed_at=observed_at,
            bid=Decimal("329.8"),
            ask=Decimal("330.2"),
            spread=Decimal("0.4"),
            spread_bps=Decimal("12.12"),
        ),
    }


def test_daily_cycle_persists_signals_stops_and_submits_once_per_closed_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_at = datetime(2026, 8, 16, 0, 20, tzinfo=UTC)
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
    )
    second = service.run_cycle(
        adapter=adapter,  # type: ignore[arg-type]
        access_token="test",
        quotes=_quotes(observed_at + timedelta(seconds=15)),
        observed_at=observed_at + timedelta(seconds=15),
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
            request_metadata={"source": "alpaca_crypto_strategy:daily_rebalance"},
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
            request_metadata={"source": "post:/orders"},
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
            )
        },
        observed_at=observed_at,
    )

    assert result["protective_actions"][0]["quantity"] == "0.25000000"  # type: ignore[index]
    assert submitted == [
        (
            Decimal("0.25000000"),
            {
                "strategy_version": "crypto_trend_v1",
                "order_origin": "alpaca_crypto_strategy",
                "autonomous_strategy_order": True,
                "decision_type": "protective_exit",
                "protective_stop_price": "100.000000000000",
                "trigger_bid": "99",
                "risk_reduction": True,
                "strategy_owned_base_quantity": "0.250000000000",
                "broker_base_quantity": "1.0",
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
            request_metadata={"source": "alpaca_crypto_strategy:daily_rebalance"},
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
            )
        },
        observed_at=observed_at,
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
            request_metadata={"source": "alpaca_crypto_strategy:daily_rebalance"},
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

    def ineligible_symbol(symbol: str, _bars, *, as_of):  # noqa: ANN001, ANN201
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
            )
        },
        observed_at=observed_at,
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
