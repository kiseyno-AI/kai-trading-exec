from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models import EXEC_OWNED_TABLES
from app.models.runtime import (
    CryptoDailyBar,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
)


def test_crypto_strategy_tables_are_exec_owned_and_persist_decimal_state() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    inspector = inspect(engine)
    expected_tables = {
        "crypto_daily_bars",
        "crypto_strategy_states",
        "crypto_strategy_risk_states",
        "crypto_strategy_decisions",
    }
    bar_open_at = datetime(2026, 8, 15, tzinfo=UTC)
    bar_close_at = bar_open_at + timedelta(days=1)

    assert expected_tables.issubset(set(inspector.get_table_names()))
    assert expected_tables.issubset(set(EXEC_OWNED_TABLES))
    assert "updated_at" not in {
        column["name"] for column in inspector.get_columns("crypto_strategy_decisions")
    }

    with session_factory() as db:
        db.add_all(
            [
                CryptoDailyBar(
                    provider="alpaca",
                    symbol="BTC/USD",
                    timeframe="1Day",
                    bar_open_at=bar_open_at,
                    bar_close_at=bar_close_at,
                    open_price=Decimal("63000.125"),
                    high_price=Decimal("64500.5"),
                    low_price=Decimal("62500.25"),
                    close_price=Decimal("64000.375"),
                    volume=Decimal("123.456789"),
                    trade_count=321,
                    vwap=Decimal("63650.75"),
                    source="alpaca-rest",
                ),
                CryptoStrategyState(
                    mode="paper",
                    provider="alpaca",
                    strategy_version="crypto_trend_v1",
                    symbol="BTC/USD",
                    last_processed_bar_at=bar_close_at,
                    eligible=True,
                    target_fraction=Decimal("0.40000000"),
                    strategy_owned_base_quantity=Decimal("0.12345678"),
                    high_water_price=Decimal("64500.5"),
                    protective_stop_price=Decimal("58050.45"),
                    last_action="buy",
                    last_action_at=bar_close_at,
                    metrics={"ema_200": "60000"},
                ),
                CryptoStrategyRiskState(
                    mode="paper",
                    provider="alpaca",
                    strategy_version="crypto_trend_v1",
                    daily_baseline_equity=Decimal("100000"),
                    daily_baseline_at=bar_open_at,
                    weekly_baseline_equity=Decimal("101000"),
                    weekly_baseline_at=bar_open_at,
                    peak_equity=Decimal("102000"),
                    peak_equity_at=bar_open_at,
                    current_equity=Decimal("100500"),
                    current_equity_at=bar_close_at,
                    risk_status="healthy",
                    risk_reasons=[],
                ),
                CryptoStrategyDecision(
                    mode="paper",
                    provider="alpaca",
                    strategy_version="crypto_trend_v1",
                    symbol="BTC/USD",
                    decision_bar_at=bar_close_at,
                    decided_at=bar_close_at + timedelta(minutes=10),
                    action="buy",
                    decision_status="planned",
                    eligible=True,
                    target_fraction=Decimal("0.40000000"),
                    reference_price=Decimal("64000.375"),
                    requested_quote_notional=Decimal("25"),
                    high_water_price=Decimal("64500.5"),
                    protective_stop_price=Decimal("58050.45"),
                    idempotency_key="ctv1-20260816-btc-buy-1",
                    reasons=["trend_eligible"],
                    metrics={"momentum_90d": "0.12"},
                ),
            ]
        )
        db.commit()

        state = db.query(CryptoStrategyState).one()
        decision = db.query(CryptoStrategyDecision).one()
        assert state.target_fraction == Decimal("0.40000000")
        assert state.strategy_owned_base_quantity == Decimal("0.123456780000")
        assert state.metrics == {"ema_200": "60000"}
        assert decision.requested_quote_notional == Decimal("25.0000")
        assert decision.reasons == ["trend_eligible"]


def test_crypto_strategy_decision_is_unique_per_scope_and_decision_bar() -> None:
    settings = Settings(database_url="sqlite:///:memory:", runtime_mode="paper")
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    decision_bar_at = datetime(2026, 8, 16, tzinfo=UTC)

    def decision(action: str) -> CryptoStrategyDecision:
        return CryptoStrategyDecision(
            mode="paper",
            provider="alpaca",
            strategy_version="crypto_trend_v1",
            symbol="ETH/USD",
            decision_bar_at=decision_bar_at,
            action=action,
            decision_status="planned",
            eligible=True,
            target_fraction=Decimal("0.20"),
        )

    with session_factory() as db:
        db.add(decision("buy"))
        db.commit()
        db.add(decision("hold"))
        with pytest.raises(IntegrityError):
            db.commit()
