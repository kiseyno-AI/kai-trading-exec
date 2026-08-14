import json
from datetime import date
from urllib.error import URLError

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


def test_core_monitoring_symbol_read_requests_refresh(monkeypatch) -> None:
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
    assert requested_urls == ["http://core.test/execution/runtime/monitored-symbols?refresh=true"]


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
