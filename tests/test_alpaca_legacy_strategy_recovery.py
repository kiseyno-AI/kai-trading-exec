from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import subprocess
import sys

import pytest

from app.broker.base import (
    BrokerAccountHolding,
    BrokerAccountSnapshot,
    BrokerLiveMarketContext,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    BrokerOrder,
    CryptoStrategyDecision,
    CryptoStrategyRiskState,
    CryptoStrategyState,
    ExecutionFill,
    ExecutionPosition,
    RuntimeRemediationEvent,
)
from app.services.alpaca_legacy_strategy_recovery import (
    analyze_alpaca_us_legacy_strategy_exposure,
    apply_alpaca_us_legacy_strategy_exposure,
)
from app.services.alpaca_crypto_strategy import AlpacaCryptoStrategyService
from app.services.position_identity import (
    PositionIdentityConflictError,
    build_authenticated_broker_position_identity,
    execution_scope_payload,
)


NOW = datetime(2026, 10, 3, 19, 4, 9, tzinfo=UTC)
ACCOUNT_ID = "paper-account-a"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _settings() -> Settings:
    return Settings(
        runtime_mode="paper",
        runtime_broker_provider="alpaca",
        database_url="sqlite:///:memory:",
        account_drawdown_recovery_scope_secret="s" * 32,
        alpaca_us_equity_allowed_symbols="SPY,QQQ",
        alpaca_us_equity_strategy_version="us_equity_trend_v1",
    )


class RecoveryReadAdapter:
    def __init__(self, snapshot: BrokerAccountSnapshot) -> None:
        self.snapshot = snapshot
        self.snapshot_calls: list[tuple[tuple[str, ...], str]] = []
        self.reconcile_calls: list[tuple[str, str | None, str]] = []
        self.mutation_calls = 0
        self.proofs: dict[str, dict[str, object]] = {}

    def get_account_snapshot(
        self,
        *_args,
        symbols: list[str] | None = None,
        exchange: str = "ALPACA",
        **_kwargs,
    ) -> BrokerAccountSnapshot:  # noqa: ANN002, ANN003
        self.snapshot_calls.append((tuple(symbols or ()), exchange))
        return self.snapshot

    def reconcile_order(
        self,
        _access_token: str,
        broker_order_id: str,
        *,
        expected_symbol: str | None = None,
        exchange: str = "ALPACA",
        **_kwargs,
    ) -> dict[str, object]:
        self.reconcile_calls.append(
            (broker_order_id, expected_symbol, exchange)
        )
        return self.proofs[broker_order_id]

    def place_order(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        self.mutation_calls += 1
        raise AssertionError("legacy recovery must not place orders")

    def cancel_order(self, *_args, **_kwargs):  # noqa: ANN002, ANN003, ANN201
        self.mutation_calls += 1
        raise AssertionError("legacy recovery must not cancel orders")

    def list_active_crypto_assets(self) -> list[dict[str, object]]:
        return [
            {"symbol": "QQQ", "min_trade_increment": "0.000000001"},
            {"symbol": "SPY", "min_trade_increment": "0.000000001"},
        ]

    def get_crypto_daily_bars(
        self,
        _access_token: str,
        *,
        symbols: list[str],
        **_kwargs,
    ) -> dict[str, list[dict[str, object]]]:
        start = NOW.date() - timedelta(days=230)
        return {
            symbol: [
                {
                    "t": datetime.combine(
                        start + timedelta(days=offset),
                        datetime.min.time(),
                        tzinfo=UTC,
                    ).isoformat(),
                    "o": str(Decimal("100") + offset - Decimal("0.5")),
                    "h": str(Decimal("100") + offset + Decimal("1")),
                    "l": str(Decimal("100") + offset - Decimal("1")),
                    "c": str(
                        Decimal("100")
                        + offset
                        + Decimal(offset % 3) / Decimal("10")
                    ),
                    "v": "1000",
                }
                for offset in range(230)
            ]
            for symbol in symbols
        }


def _snapshot() -> BrokerAccountSnapshot:
    return BrokerAccountSnapshot(
        broker_account_id=ACCOUNT_ID,
        observed_at=NOW,
        source="alpaca:paper:us_equity",
        available_cash=Decimal("1000"),
        total_asset_value=Decimal("100000"),
        previous_total_asset_value=Decimal("100000"),
        holdings=[
            BrokerAccountHolding(
                symbol="QQQ",
                quantity=0,
                average_price=Decimal("712.95"),
                payload={"base_quantity": "0.140248264"},
            ),
            BrokerAccountHolding(
                symbol="SPY",
                quantity=0,
                average_price=Decimal("766.242213"),
                payload={"base_quantity": "0.131786004"},
            ),
        ],
        open_orders=[],
        holdings_complete=True,
        open_orders_complete=True,
        payload={
            "provider": "alpaca",
            "mode": "paper",
            "asset_class": "us_equity",
            "holdings_coverage": "complete",
            "open_orders_coverage": "complete",
        },
    )


def _session_with_legacy_exposure():
    settings = _settings()
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="US",
        broker_account_id=ACCOUNT_ID,
    )
    with session_factory() as db:
        db.add_all(
            [
                ExecutionPosition(
                    id="position-qqq",
                    mode="paper",
                    provider="alpaca",
                    account_scope=identity.account_scope,
                    exchange="US",
                    symbol="QQQ",
                    quantity=0,
                    base_quantity=Decimal("0.140248264"),
                    average_price=Decimal("712.95"),
                    updated_from="broker_reconciliation",
                    last_reconciled_at=NOW,
                ),
                ExecutionPosition(
                    id="position-spy",
                    mode="paper",
                    provider="alpaca",
                    account_scope=identity.account_scope,
                    exchange="US",
                    symbol="SPY",
                    quantity=0,
                    base_quantity=Decimal("0.131786004"),
                    average_price=Decimal("766.2422"),
                    updated_from="broker_reconciliation",
                    last_reconciled_at=NOW,
                ),
                CryptoStrategyState(
                    id="legacy-state-qqq",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="us_equity_trend_v1",
                    symbol="QQQ",
                    eligible=True,
                    target_fraction=Decimal("0.5"),
                    strategy_owned_base_quantity=Decimal("0"),
                ),
                CryptoStrategyState(
                    id="legacy-state-spy",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="us_equity_trend_v1",
                    symbol="SPY",
                    eligible=True,
                    target_fraction=Decimal("0.5"),
                    strategy_owned_base_quantity=Decimal("0"),
                ),
                CryptoStrategyRiskState(
                    id="legacy-risk-us",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="us_equity_trend_v1",
                    risk_status="healthy",
                ),
                CryptoStrategyRiskState(
                    id="legacy-risk-crypto-decoy",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="crypto_trend_v1",
                    risk_status="healthy",
                ),
                CryptoStrategyDecision(
                    id="legacy-decision-qqq",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="us_equity_trend_v1",
                    symbol="QQQ",
                    decision_bar_at=NOW - timedelta(days=45),
                    decided_at=NOW - timedelta(days=45),
                    action="buy",
                    decision_status="submitted",
                    eligible=True,
                    target_fraction=Decimal("0.5"),
                    idempotency_key="usequitytrendv1-qqq-dailybuy",
                ),
                CryptoStrategyDecision(
                    id="legacy-decision-spy",
                    mode="paper",
                    provider="alpaca",
                    strategy_version="us_equity_trend_v1",
                    symbol="SPY",
                    decision_bar_at=NOW - timedelta(days=45),
                    decided_at=NOW - timedelta(days=45),
                    action="buy",
                    decision_status="submitted",
                    eligible=True,
                    target_fraction=Decimal("0.5"),
                    idempotency_key="usequitytrendv1-spy-dailybuy",
                ),
            ]
        )
        strategy_qqq = BrokerOrder(
            id="order-qqq",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="usequitytrendv1-qqq-dailybuy",
            broker_order_id="broker-qqq",
            symbol="QQQ",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.140248264"),
            venue="alpaca",
            asset_class="us_equity",
            request_metadata={"source": "alpaca_crypto_strategy:daily_rebalance"},
        )
        strategy_spy = BrokerOrder(
            id="order-spy-strategy",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="usequitytrendv1-spy-dailybuy",
            broker_order_id="broker-spy-strategy",
            symbol="SPY",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.130499130"),
            venue="alpaca",
            asset_class="us_equity",
            request_metadata={"source": "alpaca_crypto_strategy:daily_rebalance"},
        )
        manual_spy = BrokerOrder(
            id="order-spy-manual",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="manual-spy-validation",
            broker_order_id="broker-spy-manual",
            symbol="SPY",
            side="buy",
            status="filled",
            base_quantity=Decimal("0.001286874"),
            venue="alpaca",
            asset_class="us_equity",
            request_metadata={"source": "post:/orders"},
        )
        current_origin_legacy = BrokerOrder(
            id="order-current-origin-legacy",
            broker_name="alpaca",
            mode="paper",
            idempotency_key="current-origin-legacy-terminal",
            broker_order_id="broker-current-origin-legacy",
            symbol="QQQ",
            side="buy",
            status="canceled",
            base_quantity=Decimal("0.01"),
            venue="alpaca",
            asset_class="us_equity",
            request_metadata={
                "source": "alpaca_us_equity_strategy:daily_rebalance"
            },
        )
        db.add_all(
            [
                strategy_qqq,
                strategy_spy,
                manual_spy,
                current_origin_legacy,
            ]
        )
        db.flush()
        db.add_all(
            [
                ExecutionFill(
                    id="fill-qqq",
                    broker_order_pk=strategy_qqq.id,
                    symbol="QQQ",
                    side="buy",
                    quantity=0,
                    base_quantity=Decimal("0.140248264"),
                    quote_notional=Decimal("100"),
                    price=Decimal("712.95"),
                    filled_at=NOW - timedelta(days=45),
                ),
                ExecutionFill(
                    id="fill-spy-strategy",
                    broker_order_pk=strategy_spy.id,
                    symbol="SPY",
                    side="buy",
                    quantity=0,
                    base_quantity=Decimal("0.130499130"),
                    quote_notional=Decimal("100"),
                    price=Decimal("766.212"),
                    filled_at=NOW - timedelta(days=45),
                ),
                ExecutionFill(
                    id="fill-spy-manual",
                    broker_order_pk=manual_spy.id,
                    symbol="SPY",
                    side="buy",
                    quantity=0,
                    base_quantity=Decimal("0.001286874"),
                    quote_notional=Decimal("0.99"),
                    price=Decimal("769.306"),
                    filled_at=NOW - timedelta(days=45),
                ),
            ]
        )
        db.commit()
    return settings, engine, session_factory, identity.account_scope


def _adapter() -> RecoveryReadAdapter:
    adapter = RecoveryReadAdapter(_snapshot())
    for broker_order_id, symbol, key, quantity in (
        (
            "broker-qqq",
            "QQQ",
            "usequitytrendv1-qqq-dailybuy",
            "0.140248264",
        ),
        (
            "broker-spy-strategy",
            "SPY",
            "usequitytrendv1-spy-dailybuy",
            "0.130499130",
        ),
        (
            "broker-spy-manual",
            "SPY",
            "manual-spy-validation",
            "0.001286874",
        ),
    ):
        adapter.proofs[broker_order_id] = {
            "provider": "alpaca",
            "broker_order_id": broker_order_id,
            "symbol": symbol,
            "side": "buy",
            "status": "filled",
            "filled_base_quantity": quantity,
            "payload": {
                "id": broker_order_id,
                "symbol": symbol,
                "side": "buy",
                "status": "filled",
                "client_order_id": key,
                "filled_qty": quantity,
            },
        }
    return adapter


def _runtime_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={
            "alpaca_crypto_strategy_enabled": True,
            "alpaca_crypto_allowed_symbols": "SPY,QQQ",
            "alpaca_crypto_strategy_version": (
                settings.alpaca_us_equity_strategy_version
            ),
            "alpaca_crypto_strategy_decision_hour_utc": 23,
            "alpaca_crypto_strategy_decision_minute_utc": 59,
        }
    )


def _coordinator_scope(settings: Settings) -> dict[str, object]:
    identity = build_authenticated_broker_position_identity(
        settings=settings,
        provider="alpaca",
        exchange="US",
        broker_account_id=ACCOUNT_ID,
    )
    route = execution_scope_payload(
        identity=identity,
        venue="alpaca",
        asset_class="us_equity",
    )
    return {
        key: route[key]
        for key in ("policy_version", "provider", "account_scope")
    }


def _quotes(
    observed_at: datetime = NOW,
) -> dict[str, BrokerLiveMarketContext]:
    return {
        symbol: BrokerLiveMarketContext(
            symbol=symbol,
            exchange="US",
            observed_at=observed_at,
            bid=Decimal("699" if symbol == "QQQ" else "750"),
            ask=Decimal("700" if symbol == "QQQ" else "751"),
            spread=Decimal("1"),
            spread_bps=Decimal("14"),
            freshness_source="alpaca_exchange_quote_time",
        )
        for symbol in ("QQQ", "SPY")
    }


def _row_material(row: object) -> dict[str, object]:
    table = row.__class__.__table__  # type: ignore[attr-defined]
    return {
        column.name: deepcopy(getattr(row, column.name))
        for column in table.columns
    }


def _legacy_graph_material(db) -> dict[str, list[dict[str, object]]]:  # noqa: ANN001
    return {
        "states": [
            _row_material(row)
            for row in db.query(CryptoStrategyState)
            .filter(CryptoStrategyState.account_scope.is_(None))
            .order_by(CryptoStrategyState.id)
            .all()
        ],
        "risks": [
            _row_material(row)
            for row in db.query(CryptoStrategyRiskState)
            .filter(CryptoStrategyRiskState.account_scope.is_(None))
            .order_by(CryptoStrategyRiskState.id)
            .all()
        ],
        "decisions": [
            _row_material(row)
            for row in db.query(CryptoStrategyDecision)
            .filter(CryptoStrategyDecision.account_scope.is_(None))
            .order_by(CryptoStrategyDecision.id)
            .all()
        ],
        "orders": [
            _row_material(row)
            for row in db.query(BrokerOrder).order_by(BrokerOrder.id).all()
        ],
        "fills": [
            _row_material(row)
            for row in db.query(ExecutionFill).order_by(ExecutionFill.id).all()
        ],
        "positions": [
            _row_material(row)
            for row in db.query(ExecutionPosition)
            .order_by(ExecutionPosition.id)
            .all()
        ],
    }


def test_dry_run_proves_exact_strategy_and_manual_opening_inventory_without_broker_mutation() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    try:
        with session_factory() as db:
            result = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            repeated = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            assert repeated["approval_fingerprint"] == result[
                "approval_fingerprint"
            ]
            assert list(db.new) == []
            assert list(db.dirty) == []
            assert list(db.deleted) == []
            assert db.query(CryptoStrategyState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ).count() == 0
            assert db.query(RuntimeRemediationEvent).count() == 0

        assert result["mode"] == "dry-run"
        assert result["eligible"] is True
        assert result["provider"] == "alpaca"
        assert result["asset_class"] == "us_equity"
        assert result["exchange"] == "US"
        assert result["strategy_family"] == "us_equity:US"
        assert result["account_scope"] == account_scope
        assert len(str(result["approval_fingerprint"])) == 64
        assert result["opening_inventory"] == {
            "QQQ": {
                "broker_base_quantity": "0.140248264",
                "strategy_base_quantity": "0.140248264",
                "manual_base_quantity": "0",
            },
            "SPY": {
                "broker_base_quantity": "0.131786004",
                "strategy_base_quantity": "0.130499130",
                "manual_base_quantity": "0.001286874",
            },
        }
        assert result["legacy_risk_state_ids"] == ["legacy-risk-us"]
        assert result["authority_order_ids"] == [
            "order-current-origin-legacy"
        ]
        assert adapter.snapshot_calls == [
            (("QQQ", "SPY"), "US"),
            (("QQQ", "SPY"), "US"),
        ]
        assert sorted(adapter.reconcile_calls) == [
            ("broker-qqq", "QQQ", "US"),
            ("broker-qqq", "QQQ", "US"),
            ("broker-spy-manual", "SPY", "US"),
            ("broker-spy-manual", "SPY", "US"),
            ("broker-spy-strategy", "SPY", "US"),
            ("broker-spy-strategy", "SPY", "US"),
        ]
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_exact_fingerprint_apply_seeds_audited_scoped_inventory_and_keeps_legacy_rows_immutable() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            legacy_before = _legacy_graph_material(db)
            applied = apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="restore proven paper US strategy opening inventory",
                now=NOW,
            )

        assert applied["mode"] == "apply"
        assert applied["status"] == "applied"
        assert applied["approval_fingerprint"] == dry_run[
            "approval_fingerprint"
        ]
        assert applied["broker_mutations"] == 0
        assert adapter.mutation_calls == 0
        with session_factory() as db:
            legacy_after = _legacy_graph_material(db)
            assert legacy_after == legacy_before
            scoped = {
                row.symbol: row
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    mode="paper",
                    provider="alpaca",
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                    strategy_version="us_equity_trend_v1",
                )
                .all()
            }
            assert set(scoped) == {"QQQ", "SPY"}
            assert scoped["QQQ"].strategy_owned_base_quantity == Decimal(
                "0.140248264000"
            )
            assert scoped["SPY"].strategy_owned_base_quantity == Decimal(
                "0.130499130000"
            )
            for state in scoped.values():
                assert state.eligible is False
                assert state.target_fraction == Decimal("0E-8")
                assert state.last_processed_bar_at is None
                assert state.high_water_price is None
                assert state.protective_stop_price is None
                assert state.last_action == "opening_inventory_seeded"
                marker = state.metrics["legacy_strategy_recovery"]
                assert marker["policy_version"].endswith("-v1")
                assert marker["approval_fingerprint"] == dry_run[
                    "approval_fingerprint"
                ]
            event = db.query(RuntimeRemediationEvent).filter_by(
                mode="paper",
                candidate_type="alpaca_us_legacy_strategy_exposure",
                outcome="opening_inventory_seeded",
                subject_id=account_scope,
            ).one()
            assert event.actor == "operator@example.com"
            assert event.reason == (
                "restore proven paper US strategy opening inventory"
            )
            assert event.evidence_summary["approval_fingerprint"] == dry_run[
                "approval_fingerprint"
            ]
            assert event.evidence_summary["opening_inventory"] == dry_run[
                "opening_inventory"
            ]
            assert event.evidence_summary["broker_mutations"] == 0
            assert event.evidence_summary["legacy_rows_mutated"] == 0
            assert db.query(CryptoStrategyRiskState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ).count() == 0
            assert db.query(CryptoStrategyDecision).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ).count() == 0
    finally:
        engine.dispose()


def test_apply_rejects_stale_fingerprint_without_partial_seed() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            legacy = db.get(CryptoStrategyState, "legacy-state-spy")
            assert legacy is not None
            legacy.target_fraction = Decimal("0.49")
            db.commit()

            with pytest.raises(
                PositionIdentityConflictError,
                match="no longer matches current truth",
            ):
                apply_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    approved_fingerprint=str(
                        dry_run["approval_fingerprint"]
                    ),
                    actor="operator@example.com",
                    reason="stale evidence must fail closed",
                    now=NOW,
                )

            assert db.in_transaction() is False
            assert db.query(CryptoStrategyState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ).count() == 0
            assert db.query(RuntimeRemediationEvent).filter_by(
                candidate_type="alpaca_us_legacy_strategy_exposure",
                outcome="opening_inventory_seeded",
            ).count() == 0
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_dry_run_rejects_broker_order_side_mismatch() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    adapter.proofs["broker-spy-strategy"] = {
        **adapter.proofs["broker-spy-strategy"],
        "side": "sell",
        "payload": {
            **adapter.proofs["broker-spy-strategy"]["payload"],  # type: ignore[index]
            "side": "sell",
        },
    }
    try:
        with session_factory() as db:
            with pytest.raises(
                PositionIdentityConflictError,
                match="does not match local evidence",
            ):
                analyze_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    now=NOW,
                )
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_dry_run_rejects_strategy_decision_action_order_side_mismatch() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    try:
        with session_factory() as db:
            decision = db.get(
                CryptoStrategyDecision,
                "legacy-decision-qqq",
            )
            assert decision is not None
            decision.action = "sell"
            db.commit()
            with pytest.raises(
                PositionIdentityConflictError,
                match="decision side",
            ):
                analyze_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    now=NOW,
                )
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_dry_run_rejects_non_alpaca_runtime_before_any_broker_read() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    settings = settings.model_copy(
        update={"runtime_broker_provider": "kis"}
    )
    adapter = _adapter()
    try:
        with session_factory() as db:
            with pytest.raises(
                PositionIdentityConflictError,
                match="paper Alpaca runtime",
            ):
                analyze_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    now=NOW,
                )
        assert adapter.snapshot_calls == []
        assert adapter.reconcile_calls == []
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_dry_run_rejects_sub_quantum_opening_inventory_instead_of_rounding() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    overprecise = Decimal("0.1402482641")
    qqq, spy = adapter.snapshot.holdings
    adapter.snapshot = replace(
        adapter.snapshot,
        holdings=[
            replace(qqq, payload={"base_quantity": str(overprecise)}),
            spy,
        ],
    )
    adapter.proofs["broker-qqq"] = {
        **adapter.proofs["broker-qqq"],
        "filled_base_quantity": str(overprecise),
        "payload": {
            **adapter.proofs["broker-qqq"]["payload"],  # type: ignore[index]
            "filled_qty": str(overprecise),
        },
    }
    try:
        with session_factory() as db:
            position = db.get(ExecutionPosition, "position-qqq")
            order = db.get(BrokerOrder, "order-qqq")
            fill = db.get(ExecutionFill, "fill-qqq")
            assert position is not None and order is not None and fill is not None
            position.base_quantity = overprecise
            order.base_quantity = overprecise
            fill.base_quantity = overprecise
            db.commit()
            with pytest.raises(
                PositionIdentityConflictError,
                match="quantity precision",
            ):
                analyze_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    now=NOW,
                )
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_apply_refetches_under_order_serialization_and_rolls_back_lock_window_drift() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    stable_snapshot = adapter.snapshot
    qqq, spy = stable_snapshot.holdings
    drifted_snapshot = replace(
        stable_snapshot,
        holdings=[
            qqq,
            replace(
                spy,
                payload={"base_quantity": "0.131786005"},
            ),
        ],
    )
    snapshots = [stable_snapshot, stable_snapshot, drifted_snapshot]

    def sequenced_snapshot(
        _access_token: str,
        *,
        symbols: list[str] | None = None,
        exchange: str = "ALPACA",
        **_kwargs,
    ) -> BrokerAccountSnapshot:
        adapter.snapshot_calls.append((tuple(symbols or ()), exchange))
        return snapshots[min(len(adapter.snapshot_calls) - 1, 2)]

    adapter.get_account_snapshot = sequenced_snapshot  # type: ignore[method-assign]
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            with pytest.raises(PositionIdentityConflictError):
                apply_alpaca_us_legacy_strategy_exposure(
                    settings=settings,
                    db=db,
                    adapter=adapter,  # type: ignore[arg-type]
                    access_token="authenticated-paper-token",
                    approved_fingerprint=str(
                        dry_run["approval_fingerprint"]
                    ),
                    actor="operator@example.com",
                    reason="lock-window evidence must stay exact",
                    now=NOW,
                )
            assert db.in_transaction() is False
            assert db.query(CryptoStrategyState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ).count() == 0
            assert db.query(RuntimeRemediationEvent).filter_by(
                candidate_type="alpaca_us_legacy_strategy_exposure",
                outcome="opening_inventory_seeded",
            ).count() == 0
        assert adapter.snapshot_calls == [
            (("QQQ", "SPY"), "US"),
            (("QQQ", "SPY"), "US"),
            (("QQQ", "SPY"), "US"),
        ]
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_recovery_cli_exposes_dry_run_and_exact_fingerprint_apply() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "recover_alpaca_us_legacy_strategy.py"),
            "--help",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "dry-run" in result.stdout
    assert "--apply" in result.stdout
    assert "--approve-fingerprint" in result.stdout


def test_runtime_accepts_exact_opening_inventory_seed_without_absorbing_manual_quantity() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )

        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )

        assert result.get("reason") != (
            "legacy_strategy_scope_requires_manual_review"
        )
        assert result.get("reason") != (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert result["daily_decision"]["status"] == "not_due"
        with session_factory() as db:
            scoped = {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            }
            assert scoped == {
                "QQQ": Decimal("0.140248264000"),
                "SPY": Decimal("0.130499130000"),
            }
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_runtime_accepts_omitted_zero_holding_after_scoped_full_sell() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    cycle_at = NOW + timedelta(minutes=1)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )
            identity = build_authenticated_broker_position_identity(
                settings=settings,
                provider="alpaca",
                exchange="US",
                broker_account_id=ACCOUNT_ID,
            )
            scope = execution_scope_payload(
                identity=identity,
                venue="alpaca",
                asset_class="us_equity",
            )
            sell = BrokerOrder(
                id="order-qqq-post-seed-full-sell",
                broker_name="alpaca",
                mode="paper",
                idempotency_key="qqq-post-seed-full-sell",
                broker_order_id="broker-qqq-post-seed-full-sell",
                symbol="QQQ",
                side="sell",
                status="filled",
                base_quantity=Decimal("0.140248264"),
                venue="alpaca",
                asset_class="us_equity",
                request_metadata={
                    "source": "alpaca_us_equity_strategy:protective_exit",
                    "exchange": "US",
                    "execution_scope": scope,
                    "metadata": {
                        "strategy_family": "us_equity:US",
                        "expected_execution_scope": scope,
                    },
                },
            )
            db.add(sell)
            db.flush()
            db.add(
                ExecutionFill(
                    id="fill-qqq-post-seed-full-sell",
                    broker_order_pk=sell.id,
                    symbol="QQQ",
                    side="sell",
                    quantity=0,
                    base_quantity=Decimal("0.140248264"),
                    quote_notional=Decimal("98"),
                    price=Decimal("699"),
                    filled_at=NOW + timedelta(seconds=30),
                )
            )
            db.commit()

        spy = next(
            holding
            for holding in adapter.snapshot.holdings
            if holding.symbol == "SPY"
        )
        adapter.snapshot = replace(
            adapter.snapshot,
            observed_at=cycle_at,
            holdings=[spy],
        )
        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(cycle_at),
            observed_at=cycle_at,
            execution_scope=_coordinator_scope(settings),
        )

        assert result.get("reason") != (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert adapter.mutation_calls == 0
        with session_factory() as db:
            assert {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            } == {
                "QQQ": Decimal("0E-12"),
                "SPY": Decimal("0.130499130000"),
            }
    finally:
        engine.dispose()


def test_runtime_recovery_rejects_unexpected_broker_holding_symbol() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )

        adapter.snapshot = replace(
            adapter.snapshot,
            holdings=[
                *adapter.snapshot.holdings,
                BrokerAccountHolding(
                    symbol="IWM",
                    quantity=0,
                    average_price=Decimal("240"),
                    payload={"base_quantity": "0.1"},
                ),
            ],
        )
        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )

        assert result["status"] == "blocked"
        assert result["reason"] == (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("delta", [Decimal("-0.000000001"), Decimal("0.000000001")])
def test_runtime_conservation_drift_blocks_before_protective_or_daily_orders(
    delta: Decimal,
) -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )
            qqq_state = db.query(CryptoStrategyState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
                symbol="QQQ",
            ).one()
            qqq_state.high_water_price = Decimal("800")
            qqq_state.protective_stop_price = Decimal("710")
            db.commit()
            original_quantities = {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            }
            order_count = db.query(BrokerOrder).count()
            decision_count = db.query(CryptoStrategyDecision).count()

        qqq, spy = adapter.snapshot.holdings
        adapter.snapshot = replace(
            adapter.snapshot,
            holdings=[
                replace(
                    qqq,
                    payload={
                        "base_quantity": str(
                            Decimal("0.140248264") + delta
                        )
                    },
                ),
                spy,
            ],
        )
        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )

        assert result["status"] == "blocked"
        assert result["reason"] == (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert result["protective_actions"] == []
        assert result["daily_decision"] == {
            "status": "blocked",
            "reason": "legacy_strategy_opening_inventory_conservation_drift",
        }
        assert adapter.mutation_calls == 0
        with session_factory() as db:
            assert db.query(BrokerOrder).count() == order_count
            assert db.query(CryptoStrategyDecision).count() == decision_count
            assert {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            } == original_quantities
    finally:
        engine.dispose()


def test_due_decision_preserves_recovery_marker_and_authority_on_next_cycle() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings).model_copy(
        update={
            "alpaca_crypto_strategy_decision_hour_utc": 19,
            "alpaca_crypto_strategy_decision_minute_utc": 0,
        }
    )
    service = AlpacaCryptoStrategyService(
        settings=runtime_settings,
        session_factory=session_factory,
        asset_class="us_equity",
        exchange="US",
        time_in_force="day",
        bar_source="alpaca_stock_bars_v2_iex",
        order_origin="alpaca_us_equity_strategy",
    )
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )

        first = service.run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(NOW),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )
        assert first.get("reason") != (
            "legacy_strategy_scope_requires_manual_review"
        )
        assert first["daily_decision"]["status"] in {"processed", "partial"}
        with session_factory() as db:
            for state in db.query(CryptoStrategyState).filter_by(
                account_scope=account_scope,
                strategy_family="us_equity:US",
            ):
                assert "legacy_strategy_recovery" in state.metrics

        second_at = NOW + timedelta(minutes=1)
        adapter.snapshot = replace(adapter.snapshot, observed_at=second_at)
        second = service.run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(second_at),
            observed_at=second_at,
            execution_scope=_coordinator_scope(settings),
        )
        assert second.get("reason") != (
            "legacy_strategy_scope_requires_manual_review"
        )
        assert second.get("reason") != (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        with session_factory() as db:
            assert {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            } == {
                "QQQ": Decimal("0.140248264000"),
                "SPY": Decimal("0.130499130000"),
            }
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()


def test_runtime_rejects_mutated_opening_inventory_event_without_rewriting_seed() -> None:
    settings, engine, session_factory, account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )
            event = db.query(RuntimeRemediationEvent).filter_by(
                candidate_type="alpaca_us_legacy_strategy_exposure",
                outcome="opening_inventory_seeded",
                subject_id=account_scope,
            ).one()
            evidence = deepcopy(event.evidence_summary)
            evidence["opening_inventory"]["SPY"][
                "strategy_base_quantity"
            ] = "0.130599130"
            evidence["opening_inventory"]["SPY"][
                "manual_base_quantity"
            ] = "0.001186874"
            event.evidence_summary = evidence
            db.commit()
            original_quantities = {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            }

        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )
        assert result["status"] == "blocked"
        assert result["reason"] == (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert adapter.mutation_calls == 0
        with session_factory() as db:
            assert {
                row.symbol: row.strategy_owned_base_quantity
                for row in db.query(CryptoStrategyState)
                .filter_by(
                    account_scope=account_scope,
                    strategy_family="us_equity:US",
                )
                .all()
            } == original_quantities
    finally:
        engine.dispose()


def test_runtime_recovery_requires_fresh_broker_snapshot_before_any_order() -> None:
    settings, engine, session_factory, _account_scope = (
        _session_with_legacy_exposure()
    )
    adapter = _adapter()
    runtime_settings = _runtime_settings(settings)
    try:
        with session_factory() as db:
            dry_run = analyze_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                now=NOW,
            )
            apply_alpaca_us_legacy_strategy_exposure(
                settings=settings,
                db=db,
                adapter=adapter,  # type: ignore[arg-type]
                access_token="authenticated-paper-token",
                approved_fingerprint=str(dry_run["approval_fingerprint"]),
                actor="operator@example.com",
                reason="seed proven opening inventory",
                now=NOW,
            )

        adapter.snapshot = replace(
            adapter.snapshot,
            observed_at=NOW - timedelta(seconds=301),
        )
        result = AlpacaCryptoStrategyService(
            settings=runtime_settings,
            session_factory=session_factory,
            asset_class="us_equity",
            exchange="US",
            time_in_force="day",
            bar_source="alpaca_stock_bars_v2_iex",
            order_origin="alpaca_us_equity_strategy",
        ).run_cycle(
            adapter=adapter,  # type: ignore[arg-type]
            access_token="authenticated-paper-token",
            quotes=_quotes(),
            observed_at=NOW,
            execution_scope=_coordinator_scope(settings),
        )
        assert result["status"] == "blocked"
        assert result["reason"] == (
            "legacy_strategy_opening_inventory_conservation_drift"
        )
        assert result["protective_actions"] == []
        assert adapter.mutation_calls == 0
    finally:
        engine.dispose()
