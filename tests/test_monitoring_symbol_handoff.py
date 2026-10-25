import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from urllib.error import URLError

import pytest
from sqlalchemy import inspect

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import ExecutionPlan, ExecutionPlanItem
from app.services import monitoring


class _CoreMonitoringResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def _settings() -> Settings:
    return Settings(
        market_context_enabled=True,
        market_context_base_url="http://core.test",
        market_context_cache_ttl_seconds=60,
    )


def _observation_item(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "monitoring_statuses": ["signal_wait_observation_only"],
        "exec_monitoring_active": False,
        "exec_agent_approved": False,
        "planned_intents": [],
        "tactical_actions": [],
        "handoff_intents": [],
        "trigger_price": None,
        "chase_limit_price": None,
        "exec_runtime_exclusion_reason": "observation_only_no_order_authority",
    }


def _observation_payload(
    *,
    now: datetime,
    symbols: list[str] | None = None,
) -> dict[str, object]:
    symbols = symbols or ["032080"]
    return {
        "market_date": "2026-10-26",
        "source_coverage": {"membership_complete": True},
        "freshness": {
            "market_date": "2026-10-26",
            "generated_at": now.isoformat(),
        },
        "realtime_subscription": {
            "desired_symbols": list(symbols),
            "observation_only_symbols": list(symbols),
            "max_subscriptions": 40,
        },
        "items": [_observation_item(symbol) for symbol in symbols],
    }


def test_core_monitoring_symbol_read_uses_cached_handoff(monkeypatch) -> None:
    monitoring._CORE_DESIRED_MONITORING_SYMBOL_CACHE.clear()
    requested_urls: list[str] = []

    def fake_urlopen(request, *, timeout):
        requested_urls.append(request.full_url)
        assert timeout == 3.0
        return _CoreMonitoringResponse(
            {"realtime_subscription": {"desired_symbols": ["005930", "000660"]}}
        )

    monkeypatch.setattr(monitoring, "urlopen", fake_urlopen)

    symbols = monitoring._load_core_desired_monitoring_symbols(settings=_settings())

    assert symbols == ["005930", "000660"]
    assert requested_urls == ["http://core.test/execution/runtime/monitored-symbols?refresh=false"]


def test_core_observation_only_handoff_requires_fresh_same_day_non_actionable_items(
    monkeypatch,
) -> None:
    monitoring._CORE_DESIRED_MONITORING_SYMBOL_CACHE.clear()
    now = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    payload = _observation_payload(now=now)
    payload["freshness"].update(
        generated_at=(now - timedelta(seconds=120)).isoformat(),
        status="stale",
    )
    payload["realtime_subscription"]["desired_symbols"] = [
        "035420",
        "032080",
        "005930",
    ]
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: _CoreMonitoringResponse(payload),
    )

    handoff = monitoring._load_core_monitoring_symbol_handoff(
        settings=_settings(),
        observed_at=now,
    )

    assert list(handoff.desired_symbols) == ["035420", "032080", "005930"]
    assert list(handoff.observation_only_symbols) == ["032080"]


def test_environment_priority_never_becomes_observation_only_authority(
    monkeypatch,
) -> None:
    now = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    monkeypatch.setenv("KAI_EXEC_MONITORING_SYMBOL_PRIORITY", "111111,222222")
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: _CoreMonitoringResponse(
            _observation_payload(now=now)
        ),
    )

    handoff = monitoring._load_core_monitoring_symbol_handoff(
        settings=_settings(),
        observed_at=now,
    )

    assert list(handoff.desired_symbols) == ["111111", "222222"]
    assert list(handoff.observation_only_symbols) == ["032080"]


@pytest.mark.parametrize(
    ("mutation", "expected_desired"),
    [
        (
            lambda payload, now: payload["freshness"].update(
                generated_at=(now - timedelta(seconds=301)).isoformat()
            ),
            ["032080"],
        ),
        (
            lambda payload, now: payload["freshness"].update(
                generated_at=(now + timedelta(microseconds=1)).isoformat()
            ),
            ["032080"],
        ),
        (
            lambda payload, now: payload.update(market_date="2026-10-25"),
            ["032080"],
        ),
        (
            lambda payload, now: payload["source_coverage"].update(
                membership_complete=False
            ),
            ["032080"],
        ),
        (
            lambda payload, now: payload["realtime_subscription"].update(
                observation_only_symbols=["005930"]
            ),
            ["032080"],
        ),
        (
            lambda payload, now: payload["items"][0].update(
                monitoring_statuses=["watch_monitoring_active"]
            ),
            ["032080"],
        ),
        (
            lambda payload, now: payload["realtime_subscription"].update(
                observation_only_symbols=["１２３４５６"]
            ),
            ["032080"],
        ),
    ],
)
def test_core_observation_only_handoff_fails_closed_on_invalid_contract(
    monkeypatch,
    mutation,
    expected_desired,
) -> None:
    now = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    payload = _observation_payload(now=now)
    mutation(payload, now)
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: _CoreMonitoringResponse(payload),
    )

    handoff = monitoring._load_core_monitoring_symbol_handoff(
        settings=_settings(),
        observed_at=now,
    )

    assert list(handoff.desired_symbols) == expected_desired
    assert list(handoff.observation_only_symbols) == []


def test_core_observation_only_handoff_loader_failure_keeps_priority_only(
    monkeypatch,
) -> None:
    monitoring._CORE_DESIRED_MONITORING_SYMBOL_CACHE["http://core.test"] = (
        100.0,
        ("032080",),
    )
    monkeypatch.setattr(monitoring.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("offline")),
    )

    handoff = monitoring._load_core_monitoring_symbol_handoff(
        settings=_settings(),
        observed_at=datetime(2026, 10, 26, 0, 1, tzinfo=UTC),
    )

    assert list(handoff.desired_symbols) == ["032080"]
    assert list(handoff.observation_only_symbols) == []


def test_core_observation_universe_is_not_rejected_by_quote_batch_size(
    monkeypatch,
) -> None:
    now = datetime(2026, 10, 26, 0, 1, tzinfo=UTC)
    symbols = [
        "012030",
        "032080",
        "033560",
        "070590",
        "079940",
        "086060",
        "089890",
        "323350",
        "398120",
    ]
    payload = _observation_payload(now=now, symbols=symbols)
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: _CoreMonitoringResponse(payload),
    )
    settings = _settings().model_copy(
        update={"monitoring_max_symbols_per_cycle": 6}
    )

    handoff = monitoring._load_core_monitoring_symbol_handoff(
        settings=settings,
        observed_at=now,
    )

    assert list(handoff.observation_only_symbols) == symbols


def test_core_monitoring_symbol_read_uses_recent_success_on_transient_failure(monkeypatch) -> None:
    monitoring._CORE_DESIRED_MONITORING_SYMBOL_CACHE.clear()
    settings = _settings()
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: _CoreMonitoringResponse(
            {"realtime_subscription": {"desired_symbols": ["035420", "005930"]}}
        ),
    )
    assert monitoring._load_core_desired_monitoring_symbols(settings=settings) == ["035420", "005930"]

    def fail_urlopen(*_args, **_kwargs):
        raise URLError("temporary core outage")

    monkeypatch.setattr(monitoring, "urlopen", fail_urlopen)

    assert monitoring._load_core_desired_monitoring_symbols(settings=settings) == ["035420", "005930"]


def test_core_monitoring_symbol_read_drops_expired_fallback(monkeypatch) -> None:
    settings = _settings()
    monitoring._CORE_DESIRED_MONITORING_SYMBOL_CACHE["http://core.test"] = (
        10.0,
        ("005930",),
    )
    monkeypatch.setattr(monitoring.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(
        monitoring,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("core unavailable")),
    )

    assert monitoring._load_core_desired_monitoring_symbols(settings=settings) == []


def test_core_symbols_prioritize_without_filtering_exec_local_watch_plans(monkeypatch, tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-priority.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=3,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    monkeypatch.setattr(
        monitoring,
        "_load_core_desired_monitoring_symbols",
        lambda **_kwargs: ["000660"],
    )

    try:
        with session_factory() as session:
            session.add(
                ExecutionPlan(
                    external_plan_id="core-monitoring-priority",
                    source="kai-trading-core",
                    market_date=date(2026, 8, 14),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            plan_item_index=index,
                            symbol=symbol,
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                        )
                        for index, symbol in enumerate(["005930", "000660", "035420"])
                    ],
                )
            )
            session.commit()

            symbols = monitoring.list_autonomous_monitoring_symbols(
                settings=settings,
                db=session,
                current_market_date=date(2026, 8, 14),
            )

        assert symbols == ["000660", "005930", "035420"]
    finally:
        engine.dispose()


def test_file_backed_monitoring_loaders_include_watch_buy_risk_context_for_safe_cap(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'monitoring-safe-cap.db'}",
        runtime_mode="paper",
        monitoring_max_symbols_per_cycle=3,
        monitoring_expire_prior_market_watch_buys_enabled=False,
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as session:
            session.add(
                ExecutionPlan(
                    external_plan_id="safe-cap-plan",
                    source="kai-trading-core",
                    market_date=date(2026, 8, 14),
                    mode="paper",
                    status="accepted",
                    items=[
                        ExecutionPlanItem(
                            id="safe-cap-watch-buy",
                            plan_item_index=0,
                            symbol="005930",
                            side="buy",
                            intent="intraday_watch_buy",
                            status="watch_monitoring_active",
                            quantity_policy={"kind": "shares", "quantity": 10},
                            trigger_price=Decimal("100.0000"),
                            risk_context={
                                "execution_feasibility_context": {
                                    "budget_exceeds_safe_notional": True,
                                    "max_safe_quantity": 3,
                                }
                            },
                        )
                    ],
                )
            )
            session.commit()

        loader_cases = (
            {},
            {"symbols": {"005930"}},
            {"symbols": {"005930"}, "exclude_pending_config": True},
        )
        for kwargs in loader_cases:
            with session_factory() as session:
                items = monitoring._load_monitored_items(settings=settings, db=session, **kwargs)

                assert [item.id for item in items] == ["safe-cap-watch-buy"]
                item = items[0]
                assert "risk_context" not in inspect(item).unloaded
                assert monitoring._watch_buy_safe_quantity_cap(item) == 3
    finally:
        engine.dispose()
